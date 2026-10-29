using System.Security.Cryptography;
using EtcdWorker.Core;
using EtcdWorker.Etcd;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Перенос EtcdSnapshotSink (arch/25 §4): цель — живой резолв
/// etcd-backup кластера, bucket etcd-backups; статус/ретенция/CatchUp/пульс —
/// механика 1:1. Путь ОДИН, без сравнения с прошлым состоянием — каждый снятый
/// слепок уезжает новой парой: put etcd/&lt;имя&gt;.db (серверная SHA256-проверка
/// транспорта) → put .meta.json → ретенция N пар (EtcdExportRetention) →
/// статус-ключ OK с семантикой покрытия: last_uploaded_unix = метка
/// ПОКРЫВАЕМОГО слепка (taken из имени, не now — фактическое время заливки
/// фиксируется только meta.uploaded_unix; инвариант IsBehind); last_object —
/// объект этой выгрузки, реально лежащий в S3; last_sha256 — наблюдаемость, в
/// решении о put не участвует. Любая неудача — статус FAILED + error (поля
/// последнего успеха сохраняются) и Result.Failed: S3-транспорт всегда
/// транзиент, догоняет доводка CatchUpAsync. Все шаги идемпотентны (put поверх;
/// коллизия имён в одну секунду — новее поверх). Единый бюджет timeoutSec —
/// на всю попытку ExportAsync (linked-CTS CancelAfter: puts+ретенция+статус
/// суммарно; транзиент не тормозит снятие).</summary>
public sealed class SnapshotExporter(
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    EtcdBackupTargetResolver resolver, // живой резолв цели etcd-backup (bucket/endpoint'ы/креды) — на каждую попытку
    Func<EtcdBackupTarget, IEtcdSnapshotS3> clientFactory, // S3-клиент цели, per-попытку (IAsyncDisposable)
    int retentionObjects,
    int timeoutSec,
    string instanceId,
    int intervalMin,
    Action? pulse = null, // пульс активности цикла выгрузки: S3-шаги — «пульс конечного вызова» (arch/14 §6)
    TimeSpan? pulsePeriod = null)
{
    // Период пульса — полокна проверки watchdog (инъекция от хоста;
    // дефолт — полокна при дефолтных опциях).
    private TimeSpan PulsePeriod => pulsePeriod ?? TimeSpan.FromSeconds(10);

    public async Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
    {
        // Живой резолв цели (arch/25 §4) и прошлый статус — ОДИН раз на попытку;
        // Failed-резолв → статус FAILED с маркер-текстом ошибки (поля
        // последнего успеха сохраняются).
        var target = await resolver.ResolveAsync(ct);
        var lastStatus = await ReadStatusAsync(ct);
        if (!target.IsSuccess)
            return await FailAsync(
                lastStatus.IsSuccess ? lastStatus.Value : null,
                target.Error!, ct);
        if (!lastStatus.IsSuccess)
            return await FailAsync(lastStatus.Value, lastStatus.Error!, ct);

        // S3-клиент — per-попытку, из резолва цели.
        await using var s3 = clientFactory(target.Value);

        // Шаги под бюджетом таймаута: единый linked-токен на попытку.
        using var step = CancellationTokenSource.CreateLinkedTokenSource(ct);
        step.CancelAfter(TimeSpan.FromSeconds(timeoutSec));

        var sha = Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

        // Метки: taken — метка ПОКРЫВАЕМОГО слепка (из имени;
        // нераспознанное имя — now), uploaded — фактическое время put'а
        // (живёт только в meta.uploaded_unix, НЕ в статус-ключе).
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var taken = EtcdSnapshotStatus.TakenUnixFromName(snapshotFileName) ?? now;

        var objKey = $"{EtcdExportRetention.Prefix}{snapshotFileName}"; // etcd/snapshot-<id>.db
        var put = await PulseAsync(token => s3.PutObjectAsync(objKey, data, sha, token), step.Token);
        if (!put.IsSuccess)
            return await FailAsync(lastStatus.Value, put.Error!, ct);

        var meta = EtcdSnapshotMetaJson.Serialize(
            new EtcdSnapshotMeta(sha, data.Length, revision, taken, now, instanceId));
        // Мета-ключ — по id БЕЗ ".db": канон etcd/snapshot-<id>.meta.json.
        // НЕ $"{objKey}.meta.json": ключ snapshot-<id>.db.meta.json ретенция
        // (EtcdExportRetention.Select) разобрал бы как мету с id snapshot-<id>.db
        // без пары .db.db → сирота → снос первым же ретенционным проходом.
        var metaPut = await PulseAsync(
            token => s3.PutObjectAsync(
                $"{EtcdExportRetention.Prefix}{snapshotFileName[..^".db".Length]}.meta.json",
                System.Text.Encoding.UTF8.GetBytes(meta), null, token),
            step.Token);
        if (!metaPut.IsSuccess)
            return await FailAsync(lastStatus.Value, metaPut.Error!, ct);

        // Ретенция: неудача — транзиент (следующий слепок пересоберёт); снятие
        // не откатываем, но статус честно FAILED — доводка повторит.
        var retention = await ApplyRetentionAsync(s3, step.Token);
        if (!retention.IsSuccess)
            return await FailAsync(lastStatus.Value, retention.Error!, ct);

        // Статус OK с семантикой ПОКРЫТИЯ: last_uploaded_unix = taken (не now) —
        // инвариант IsBehind без рваных сравнений.
        var statusPut = await PutStatusAsync(EtcdSnapshotStatusJson.Ok(
            taken, objKey, sha, data.Length, intervalMin), step.Token);
        if (!statusPut.IsSuccess)
            return Result.Failed(statusPut.Error!); // статус не записан — устареет (алерт stale)

        return Result.Success();
    }

    /// <summary>Доводка отстающей выгрузки: отставание — по СТАТУС-ключу
    /// (EtcdSnapshotStatus.IsBehind: FAILED, либо ключа нет, либо новейший
    /// локальный снят позже подтверждённого покрытия), а не по факту доводки.
    /// Возврат: true — выгрузка отстаёт (в т.ч. FAILED при ПУСТОМ томе:
    /// догонять нечего, но короткий сон важнее полного интервала — сигнал
    /// оператору); false — здорова (безделье). При отставании новейший
    /// локальный слепок (если есть) перечитывается и уезжает тем же конвейером
    /// ExportAsync (резолв цели + S3-клиент per-попытку) — успешный проход
    /// продвигает last_uploaded_unix и закрывает отставание; промежуточные
    /// невыгруженные слепки — нижние ревизии того же ряда, восстановление
    /// интересует только последний. Ошибка re-export → Failed (короткий сон).</summary>
    public async Task<Result<bool>> CatchUpAsync(string snapshotsDir, CancellationToken ct)
    {
        var last = await ReadStatusAsync(ct);
        if (!last.IsSuccess)
            return Result<bool>.Failed(last.Error!);
        var latest = LatestLocalFile(snapshotsDir);
        if (!EtcdSnapshotStatus.IsBehind(
                last.Value, EtcdSnapshotStatus.TakenUnixFromName(Path.GetFileName(latest ?? ""))))
            return Result<bool>.Success(false); // здорова — безделье

        if (latest is null)
            return Result<bool>.Success(true); // отстаёт (FAILED), догонять нечего

        var data = await File.ReadAllBytesAsync(latest, ct);
        var exported = await ExportAsync(Path.GetFileName(latest), data, revision: null, ct);
        return exported.IsSuccess
            ? Result<bool>.Success(true)
            : Result<bool>.Failed(exported.Error!);
    }

    /// <summary>Чтение статус-ключа (failover по endpoints, паттерн WalStatusWriter);
    /// null-значение = ключа ещё нет.</summary>
    public async Task<Result<EtcdSnapshotStatus?>> ReadStatusAsync(CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in etcdEndpoints)
        {
            var got = await etcd.GetAsync(endpoint, EtcdSnapshotStatusJson.Key, ct);
            if (!got.IsSuccess)
            {
                last = got; // отказ — следующий endpoint
                continue;
            }

            return got.Value is not { } kv
                ? Result<EtcdSnapshotStatus?>.Success(null)
                : Result<EtcdSnapshotStatus?>.Success(EtcdSnapshotStatusJson.Parse(kv.Value));
        }

        return Result<EtcdSnapshotStatus?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Ретенция: list префикса → чистый отбор → batch-delete. Клиент —
    // per-попытку из ExportAsync (тот же резолв цели).
    private async Task<Result> ApplyRetentionAsync(IEtcdSnapshotS3 s3, CancellationToken ct)
    {
        var listed = await PulseAsync(token => s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct: token), ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!);
        var stale = EtcdExportRetention.Select(listed.Value, retentionObjects);
        return stale.Count == 0
            ? Result.Success()
            : await PulseAsync(token => s3.DeleteKeysAsync(stale, token), ct);
    }

    // Неудача: статус FAILED + error, поля последнего успеха сохраняются.
    // Запись статуса — под ВНЕШНИМ токеном (не под бюджетом попытки): бюджет
    // мог исчерпаться на упавшем S3-put, а FAILED обязан доехать до etcd —
    // это быстрый локальный put, не S3-вызов. Отказ записи статуса не
    // маскирует исходную ошибку (алерт stale заметит).
    private async Task<Result> FailAsync(EtcdSnapshotStatus? lastOk, Exception error, CancellationToken ct)
    {
        await PutStatusAsync(EtcdSnapshotStatusJson.Failed(lastOk, error.Message, intervalMin), ct);
        return Result.Failed(error);
    }

    private async Task<Result> PutStatusAsync(string json, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in etcdEndpoints)
        {
            var put = await etcd.PutAsync(endpoint, EtcdSnapshotStatusJson.Key, json, lease: null, ct);
            if (put.IsSuccess)
                return Result.Success();
            last = put;
        }

        return Result.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Новейший локальный слепок (имя — таймстемп, Ordinal-сортировка = время).
    public static string? LatestLocalFile(string dir)
        => Directory.Exists(dir)
            ? Directory.GetFiles(dir, "snapshot-*.db").OrderByDescending(f => f, StringComparer.Ordinal).FirstOrDefault()
            : null;

    /// <summary>S3-вызов с пульсом активности цикла выгрузки (arch/14 §6
    /// «пульс конечного внешнего вызова»): пер-попыточный таймаут несёт
    /// S3-клиент, бюджет попытки — linked-CTS ExportAsync; пульс живёт ровно
    /// вокруг вызова. Пульс — канал активности цикла, поэтому Action, а не
    /// ILoopProgress.</summary>
    private async Task<Result<T>> PulseAsync<T>(
        Func<CancellationToken, Task<Result<T>>> call, CancellationToken ct)
    {
        using var timer = pulse is null
            ? null
            : new Timer(_ => pulse(), null, TimeSpan.Zero, PulsePeriod);
        return await call(ct);
    }

    // Негенерик-перегрузка: вызовы без значения (put/delete → Result).
    private async Task<Result> PulseAsync(
        Func<CancellationToken, Task<Result>> call, CancellationToken ct)
    {
        using var timer = pulse is null
            ? null
            : new Timer(_ => pulse(), null, TimeSpan.Zero, PulsePeriod);
        return await call(ct);
    }
}
