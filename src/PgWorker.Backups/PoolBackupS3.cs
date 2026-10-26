using Shared.Core;

namespace PgWorker.Backups;

/// <summary>
/// Пул-клиент IBackupS3 (t36, arch/19 §5): write-цели (список роли источника),
/// read-цели (все ноды пула, порядок endpoints); цель — существующий BackupS3
/// (endpoint+креды одного адреса), кэш per-цели. Перебор всегда С ПЕРВОЙ цели
/// («первый доступный по списку»); каждая попытка несёт per-попыточный таймаут
/// S3-клиента цели — «быстрый отказ/успех, не молчание» (R3, arch/23 §7).
/// Транспортный отказ цели открывает circuit-breaker: цель исключается из
/// перебора на окно восстановления (arch/19 §5 «Исключение отказавшей цели»),
/// истечение окна или успешный ответ возвращают её в перебор.
/// Семантика: put/delete — write-список с перебором (исчерпание → маркер);
/// точечные GET — read-список до найденного (NotFound → следующая, исходная
/// ошибка наружу); листинги — первая доступная (пустой список = успех);
/// BucketExists — любая доступная.
/// </summary>
public sealed class PoolBackupS3 : IBackupS3, IAsyncDisposable
{
    // Окно исключения отказавшей цели (arch/19 §5): закрывает тыл
    // ретрай-циклам источников (приёмник WAL — 30-35 с) — мёртвая цель не
    // зондируется чаще её собственного цикла; полокна (≈22 с) + следующий
    // тик укладывают возвращение поднявшейся ноды в минуту.
    internal static readonly TimeSpan DefaultBreakerWindow = TimeSpan.FromSeconds(45);

    private readonly IReadOnlyList<string> _writeTargets;
    private readonly IReadOnlyList<string> _readTargets;
    private readonly Func<string, IBackupS3> _targetFactory;
    private readonly Dictionary<string, IBackupS3> _cache = new(StringComparer.Ordinal);
    private readonly TimeSpan _breakerWindow;
    private readonly TimeProvider _clock;
    private readonly object _breakerGate = new();
    private readonly Dictionary<string, (DateTimeOffset Until, string Error)> _broken = new(StringComparer.Ordinal);

    public PoolBackupS3(
        IReadOnlyList<string> writeTargets,
        IReadOnlyList<string> readTargets,
        string accessKey, string secretKey,
        string bucket, string? caBundlePath,
        TimeSpan? requestTimeout = null,
        TimeSpan? breakerWindow = null,
        TimeProvider? clock = null,
        Func<string, IBackupS3>? targetFactory = null)
    {
        _writeTargets = writeTargets;
        _readTargets = readTargets;
        // Прод: uri => new BackupS3(new S3ClientOptions(uri, bucket, access,
        // secret, CaBundlePath: caBundlePath), requestTimeout); инъекция —
        // юнит-тесты (FakeTargetS3 с управляемым отказом).
        _targetFactory = targetFactory ?? (uri => new BackupS3(
            new S3ClientOptions(uri, bucket, accessKey, secretKey, CaBundlePath: caBundlePath),
            requestTimeout));
        _breakerWindow = breakerWindow ?? DefaultBreakerWindow;
        _clock = clock ?? TimeProvider.System;
    }

    // ── Circuit-breaker цели (arch/19 §5): транспортный отказ исключает цель
    // из перебора на окно; NotFound — не отказ (сервер отвечает — цель жива
    // для union-семантики чтения); успех возвращает цель сразу.

    // Попытка цели под breaker-семантикой: цель в окне → сохранённая причина
    // (участвует в маркере исчерпания как исходная ошибка); после вызова —
    // успех закрывает окно, транспортный отказ открывает.
    private async Task<Result<T>> AttemptAsync<T>(string uri, Func<Task<Result<T>>> call)
    {
        if (IsExcluded(uri, out var saved))
            return Result<T>.Failed(new ApplicationException(saved));
        var result = await call();
        if (result.IsSuccess)
        {
            NoteSuccess(uri);
            return result;
        }

        var message = result.Error!.Message;
        if (!IsNotFound(message))
            NoteFailure(uri, message);
        return result;
    }

    // Не-генерик-перегрузка: put/delete — Result без значения.
    private async Task<Result> AttemptAsync(string uri, Func<Task<Result>> call)
    {
        if (IsExcluded(uri, out var saved))
            return Result.Failed(new ApplicationException(saved));
        var result = await call();
        if (result.IsSuccess)
        {
            NoteSuccess(uri);
            return result;
        }

        var message = result.Error!.Message;
        if (!IsNotFound(message))
            NoteFailure(uri, message);
        return result;
    }

    private bool IsExcluded(string uri, out string error)
    {
        lock (_breakerGate)
        {
            if (_broken.TryGetValue(uri, out var broken)
                && broken.Until > _clock.GetUtcNow())
            {
                error = broken.Error;
                return true;
            }

            error = "";
            return false;
        }
    }

    private void NoteFailure(string uri, string error)
    {
        lock (_breakerGate)
            _broken[uri] = (_clock.GetUtcNow() + _breakerWindow, error);
    }

    private void NoteSuccess(string uri)
    {
        lock (_breakerGate)
            _broken.Remove(uri);
    }

    public async Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
    {
        // Любая доступная: write-цели по приоритету, затем read.
        Result<bool> last = default;
        foreach (var uri in _writeTargets.Concat(_readTargets))
        {
            var result = await AttemptAsync(uri, () => Target(uri).BucketExistsAsync(ct));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<bool>.Failed(new ApplicationException(
            $"{S3PoolMarkers.Exhausted}: bucket-exists — all pool nodes are unavailable: {last.Error!.Message}"));
    }

    public async Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await FirstAliveAsync(
            uri => Target(uri).ListWalAsync(cluster, shard, maxKeysPerTest, ct),
            "list-wal");

    /// <summary>Union-листинг wal/ по ВСЕМ read-целям (t36, arch/19 §5 «где
    /// данные есть»): дедуп по имени объекта. Для сверки цепочки контроля:
    /// первая доступная нода может отставать (лаг mesh) — её соло-листинг даёт
    /// ЛОЖНУЮ дыру и переводит шард в WAL_CHAIN_ERROR (прецедент AC4: n1 после рестарта
    /// догоняет mesh — 002 отсутствует только на ней). Успех — если ответил
    /// хотя бы один узел; исчерпание — маркер.</summary>
    public async Task<Result<IReadOnlyList<WalObject>>> ListWalUnionAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await ListUnionAsync(cluster, shard, "wal/", maxKeysPerTest, ct);

    /// <summary>Union-листинг ПРОИЗВОЛЬНОГО префикса по ВСЕМ read-целям (t02,
    /// arch/19 §5 «где данные есть»): полный бэкап заливается на первую живую
    /// ноду момента заливки — набор full/&lt;id&gt;/pg_wal/ и wal/-дубли
    /// размещены по нодам; соло-листинг первой живой цели даёт ложное «пусто»
    /// (verify FAILED «set contains no pg_wal WAL segments») и ложный gap
    /// (drill). Дедуп по имени; успех — если ответил хотя бы один узел;
    /// исчерпание — маркер.</summary>
    public async Task<Result<IReadOnlyList<WalObject>>> ListUnionAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        var merged = new Dictionary<string, WalObject>(StringComparer.Ordinal);
        Result<IReadOnlyList<WalObject>> last = default;
        var anySuccess = false;
        foreach (var uri in _readTargets)
        {
            var result = await AttemptAsync(uri, () => Target(uri).ListAsync(cluster, shard, prefix, maxKeysPerTest, ct));
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            anySuccess = true;
            foreach (var o in result.Value)
                merged.TryAdd(o.Name, o);
        }

        if (!anySuccess)
            return Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException(
                $"{S3PoolMarkers.Exhausted}: list-union {prefix} — all pool nodes are unavailable: {last.Error!.Message}"));
        return Result<IReadOnlyList<WalObject>>.Success([.. merged.Values]);
    }

    public async Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await FirstAliveAsync(
            uri => Target(uri).ListPrefixAsync(prefix, maxKeysPerTest, ct),
            $"list {prefix}");

    // Запись: попытка на текущей, отказ → следующая; исчерпание → маркер.
    public async Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
    {
        Result last = default;
        foreach (var uri in _writeTargets)
        {
            var result = await AttemptAsync(uri, () => Target(uri).PutObjectAsync(key, data, sha256, ct));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result.Failed(new ApplicationException(
            $"{S3PoolMarkers.Exhausted}: put {key} — all list nodes are unavailable: {last.Error!.Message}"));
    }

    // Удаление — write-семантика put (первая доступная нода, mesh разносит
    // tombstone — arch/19 §5); исчерпание → маркер.
    public async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        Result last = default;
        foreach (var uri in _writeTargets)
        {
            var result = await AttemptAsync(uri, () => Target(uri).DeleteKeysAsync(keys, ct));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result.Failed(new ApplicationException(
            $"{S3PoolMarkers.Exhausted}: delete — all list nodes are unavailable: {last.Error!.Message}"));
    }

    public async Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await FirstAliveAsync(
            uri => Target(uri).ListAsync(cluster, shard, prefix, maxKeysPerTest, ct),
            $"list {prefix}");

    // Точечное чтение: NotFound/транспортный отказ → следующая; успех — оттуда;
    // исчерпание → последняя исходная ошибка (не маркер — причина важнее).
    public async Task<Result<string>> GetObjectAsync(
        string cluster, string shard, string key, CancellationToken ct = default)
        => await FirstFoundAsync(
            uri => Target(uri).GetObjectAsync(cluster, shard, key, ct), $"get {key}");

    public async Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await FirstAliveAsync(
            uri => Target(uri).ListFullsAsync(cluster, shard, maxKeysPerTest, ct),
            "list-fulls");

    public async Task<Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default)
        => await FirstFoundAsync(
            uri => Target(uri).DownloadTextAsync(cluster, shard, objectKey, ct),
            $"get {objectKey}");

    // Запись файла: write-семантика put (перебор write-списка); исчерпание → маркер.
    public async Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
    {
        Result last = default;
        foreach (var uri in _writeTargets)
        {
            var result = await AttemptAsync(uri, () => Target(uri).PutFileAsync(key, filePath, ct));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result.Failed(new ApplicationException(
            $"{S3PoolMarkers.Exhausted}: put-file {key} — all list nodes are unavailable: {last.Error!.Message}"));
    }

    // Файловый GET: перебор read-списка до найденного (как GetObjectAsync);
    // исчерпание — последняя исходная ошибка (не маркер — причина важнее).
    public async Task<Result> GetObjectToFileAsync(
        string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
    {
        Result last = default;
        foreach (var uri in _readTargets)
        {
            var result = await AttemptAsync(uri, () => Target(uri)
                .GetObjectToFileAsync(cluster, shard, key, destFilePath, ct));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last;
    }

    // Листинги: первая доступная read-цель (успех = пустой список тоже успех);
    // исчерпание — маркер (алертная грань «пул недоступен»).
    private async Task<Result<T>> FirstAliveAsync<T>(
        Func<string, Task<Result<T>>> call, string what)
    {
        Result<T> last = default;
        foreach (var uri in _readTargets)
        {
            var result = await AttemptAsync(uri, () => call(uri));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<T>.Failed(new ApplicationException(
            $"{S3PoolMarkers.Exhausted}: {what} — all pool nodes are unavailable: {last.Error!.Message}"));
    }

    // Точечные GET: любой отказ (NotFound/транспорт) → следующая; успех — оттуда;
    // исчерпание — ПОСЛЕДНЯЯ исходная ошибка (текст причины).
    private async Task<Result<string>> FirstFoundAsync(
        Func<string, Task<Result<string>>> call, string what)
    {
        Result<string> last = default;
        foreach (var uri in _readTargets)
        {
            var result = await AttemptAsync(uri, () => call(uri));
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last;
    }

    // Кэш per-цели: один клиент на endpoint (переиспользование соединений).
    private IBackupS3 Target(string uri)
    {
        if (_cache.TryGetValue(uri, out var cached))
            return cached;
        var created = _targetFactory(uri);
        _cache[uri] = created;
        return created;
    }

    internal static bool IsNotFound(string message)
        => message.Contains("not found", StringComparison.OrdinalIgnoreCase);

    public async ValueTask DisposeAsync()
    {
        foreach (var target in _cache.Values)
            if (target is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        _cache.Clear();
    }
}
