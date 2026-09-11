using System.Collections.Concurrent;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core;

namespace PgWorker.IntegrationTests.Backups;

// Фейковый SQL-слой: слоты в памяти, LSN управляется тестом (AAA-Act).
public sealed class FakeWalSqlExecutor : IWalSqlExecutor
{
    public ConcurrentDictionary<string, bool> Slots { get; } = new();

    public (string Lsn, int Tli) Current { get; set; } = ("0/1000000", 1);

    public Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(Slots.TryGetValue(slot, out var alive) && alive));

    public Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        Slots[slot] = true;
        return Task.FromResult(Result.Success());
    }

    public Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
        => Task.FromResult(Result<(string, int)>.Success(Current));
}

// Фейковый S3 (поверхность IBackupS3): объекты в памяти, стартовое наполнение —
// тестом. t06: PrefixObjects — объекты произвольных префиксов (полные ключи),
// DeletedKeys — журнал удалений, FailNextDelete — сбой для transient-сценариев.
// t04: PrefixedObjects — ключи относительно <C>/<X>/ (сид verify-наборов),
// Contents — содержимое для GetObjectAsync, Fails/FailsGetObject — транспорт-сбои.
public sealed class FakeBackupS3 : IBackupS3
{
    public List<(string Cluster, string Shard, string Name)> Objects { get; } = [];

    // Объекты произвольных ключей (list/delete через ListPrefixAsync/DeleteKeysAsync).
    public List<(string Key, long SizeBytes)> PrefixObjects { get; } = [];

    public List<string> DeletedKeys { get; } = [];

    // Один сбой batch-delete (transient-сценарий AC3): следующий вызов падает.
    public bool FailNextDelete { get; set; }

    // Объекты с ключом после <C>/<X>/ (наборы full/<id>/pg_wal/…): сид тестов
    // verify. Содержимое history — в Contents. Голые wal-имена остаются в
    // Objects (совместимость с тестами t03): ListAsync("wal/") их ОБЪЕДИНЯЕТ с
    // PrefixedObjects-ключами на "wal/" — единая картина wal/-префикса для CheckRange.
    public List<(string Cluster, string Shard, string Key)> PrefixedObjects { get; } = [];

    public Dictionary<(string Cluster, string Shard, string Key), string> Contents { get; } = [];

    public bool Fails { get; set; }           // транспорт-сбой чтений S3 (transient-тест list)
    public bool FailsGetObject { get; set; }  // падает только GET (transient-тест GET history)

    // t05: полные шарда (id) — DR-поиск ListFullsAsync по fake-S3.
    public List<(string Cluster, string Shard, string Id)> Fulls { get; } = [];

    // t05: тексты маленьких объектов по objectKey внутри префикса шарда
    // ("full/<id>/backup_label", "full/<id>/backup_manifest").
    public Dictionary<string, string> Texts { get; } = new(StringComparer.Ordinal);

    // Один сбой list (transient-сценарий «S3 недоступен»): list не падает в статус.
    public bool FailList { get; set; }

    public DateTimeOffset LastModified { get; set; } = DateTimeOffset.UtcNow;

    public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
        => Task.FromResult(Fails
            ? Result<bool>.Failed(new ApplicationException("s3 down"))
            : Result<bool>.Success(true));

    public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => ListAsync(cluster, shard, "wal/", maxKeysPerTest, ct);

    public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        if (Fails)
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("s3 down")));
        var prefixed = PrefixedObjects
            .Where(o => o.Cluster == cluster && o.Shard == shard
                        && o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(o => new WalObject(o.Key[(o.Key.LastIndexOf('/') + 1)..], LastModified));
        // wal/-префикс пополняется Objects (голые имена wal-сегментов/history);
        // дубликаты имён схлопываются — порядок для CheckRange не важен (сортирует).
        var walNames = prefix == "wal/"
            ? Objects.Where(o => o.Cluster == cluster && o.Shard == shard)
                .Select(o => new WalObject(o.Name, LastModified))
            : [];
        var merged = prefixed.Concat(walNames)
            .GroupBy(o => o.Name, StringComparer.Ordinal)
            .Select(g => g.First())
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
            (IReadOnlyList<WalObject>)merged));
    }

    public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default)
        => Task.FromResult(Fails || FailsGetObject || !Contents.TryGetValue((cluster, shard, key), out var content)
            ? Result<string>.Failed(new ApplicationException("s3 get failed"))
            : Result<string>.Success(content));

    public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        var walKeys = Objects.Select(o =>
            new S3ObjectInfo($"{o.Cluster}/{o.Shard}/wal/{o.Name}", 16, LastModified));
        var prefixKeys = PrefixObjects.Select(o => new S3ObjectInfo(o.Key, o.SizeBytes, LastModified));
        var all = walKeys.Concat(prefixKeys)
            .Where(o => o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
            (IReadOnlyList<S3ObjectInfo>)all));
    }

    public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        // Transient-сбой по требованию теста («S3 недоступен» — статус не меняем).
        if (FailList)
            return Task.FromResult(Result<IReadOnlyList<string>>.Failed(
                new ApplicationException("fake S3 list failure (FailList)")));

        var fromFulls = Fulls.Where(f => f.Cluster == cluster && f.Shard == shard)
            .Select(f => f.Id);
        var fromPrefixObjects = PrefixObjects
            .Select(o => o.Key)
            .Select(key => key.StartsWith($"{cluster}/{shard}/full/", StringComparison.Ordinal)
                ? key[$"{cluster}/{shard}/full/".Length..].Split('/')[0]
                : "")
            .Where(id => id.Length > 0);
        var ids = fromFulls.Concat(fromPrefixObjects)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)ids));
    }

    public Task<Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default)
    {
        var key = $"{cluster}/{shard}/{objectKey}";
        return Task.FromResult(Texts.TryGetValue(key, out var text)
            ? Result<string>.Success(text)
            : Result<string>.Failed(new ApplicationException($"fake S3 get {key}: not found")));
    }

    public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        // Transient-сбой по требованию теста (статусы etcd не меняем — AC3).
        if (FailNextDelete)
        {
            FailNextDelete = false;
            return Task.FromResult(Result.Failed(
                new ApplicationException("fake S3 delete failure (FailNextDelete)")));
        }

        foreach (var key in keys)
        {
            DeletedKeys.Add(key);
            Objects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/wal/{o.Name}" == key);
            PrefixObjects.RemoveAll(o => o.Key == key);
            PrefixedObjects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/{o.Key}" == key);
        }

        return Task.FromResult(Result.Success());
    }
}
