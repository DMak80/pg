using System.Security.Cryptography;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Maintenance;

namespace PgWorker.Backups.EtcdExport;

/// <summary>S3-sink снапшотов etcd (t08, spec §3.6): путь ОДИН, без сравнения
/// с прошлым состоянием — каждый снятый слепок уезжает новой парой (§2.7):
/// put etcd/&lt;имя&gt;.db (серверная SHA256-проверка транспорта) → put
/// .meta.json → ретенция N пар (EtcdExportRetention) → статус-ключ OK с
/// семантикой покрытия: last_uploaded_unix = метка ПОКРЫВАЕМОГО слепка (taken
/// из имени, не now — фактическое время заливки фиксируется только
/// meta.uploaded_unix; инвариант IsBehind §3.5 п.1); last_object — объект
/// этой выгрузки, реально лежащий в S3; last_sha256 — наблюдаемость, в
/// решении о put не участвует. Любая неудача — статус FAILED + error (поля
/// последнего успеха сохраняются) и Result.Failed: S3-транспорт всегда
/// транзиент, догоняет доводка SnapshotLoop (CatchUpAsync). Все шаги
/// идемпотентны (put поверх; коллизия имён в одну секунду — новее поверх).
/// Единый бюджет timeoutSec — на всю попытку ExportAsync (linked-CTS
/// CancelAfter: puts+ретенция+статус суммарно; транзиент не тормозит снятие).</summary>
public sealed class EtcdSnapshotSink(
    IBackupS3 s3,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    int retentionObjects,
    int timeoutSec,
    string instanceId,
    int intervalMin) : ISnapshotSink
{
    public async Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
    {
        // Шаги под бюджетом таймаута: единый linked-токен на попытку.
        using var step = CancellationTokenSource.CreateLinkedTokenSource(ct);
        step.CancelAfter(TimeSpan.FromSeconds(timeoutSec));

        var sha = Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

        // Прошлый статус — ТОЛЬКО для FAILED-ветки (перенос полей последнего
        // успеха, §3.6); в решении о put не участвует: каждый слепок уезжает
        // новой парой (§2.7 — ветви сравнения sha с прошлым состоянием нет).
        var last = await ReadStatusAsync(step.Token);
        if (!last.IsSuccess)
            return await FailAsync(last.Value, last.Error!, ct);

        // Метки (§3.1/§3.2): taken — метка ПОКРЫВАЕМОГО слепка (из имени;
        // нераспознанное имя — now), uploaded — фактическое время put'а
        // (живёт только в meta.uploaded_unix, НЕ в статус-ключе).
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var taken = EtcdSnapshotStatus.TakenUnixFromName(snapshotFileName) ?? now;

        var objKey = $"{EtcdExportRetention.Prefix}{snapshotFileName}"; // etcd/snapshot-<id>.db
        var put = await s3.PutObjectAsync(objKey, data, sha, step.Token);
        if (!put.IsSuccess)
            return await FailAsync(last.Value, put.Error!, ct);

        var meta = EtcdSnapshotMetaJson.Serialize(
            new EtcdSnapshotMeta(sha, data.Length, revision, taken, now, instanceId));
        // Мета-ключ — по id БЕЗ ".db": канон §3.2 etcd/snapshot-<id>.meta.json.
        // НЕ $"{objKey}.meta.json": ключ snapshot-<id>.db.meta.json ретенция
        // (EtcdExportRetention.Select) разобрал бы как мету с id snapshot-<id>.db
        // без пары .db.db → сирота → снос первым же ретенционным проходом.
        var metaPut = await s3.PutObjectAsync(
            $"{EtcdExportRetention.Prefix}{snapshotFileName[..^".db".Length]}.meta.json",
            System.Text.Encoding.UTF8.GetBytes(meta), null, step.Token);
        if (!metaPut.IsSuccess)
            return await FailAsync(last.Value, metaPut.Error!, ct);

        // Ретенция: неудача — транзиент (следующий слепок пересоберёт); снятие
        // не откатываем, но статус честно FAILED — доводка повторит.
        var retention = await ApplyRetentionAsync(step.Token);
        if (!retention.IsSuccess)
            return await FailAsync(last.Value, retention.Error!, ct);

        // Статус OK с семантикой ПОКРЫТИЯ: last_uploaded_unix = taken (не now) —
        // инвариант IsBehind без рваных сравнений (§3.6).
        var statusPut = await PutStatusAsync(EtcdSnapshotStatusJson.Ok(
            taken, objKey, sha, data.Length, intervalMin), step.Token);
        if (!statusPut.IsSuccess)
            return Result.Failed(statusPut.Error!); // статус не записан — устареет (алерт stale)

        return Result.Success();
    }

    /// <summary>Доводка отстающей выгрузки (spec §3.5 п.1/п.3): отставание — по
    /// СТАТУС-ключу (EtcdSnapshotStatus.IsBehind: FAILED, либо ключа нет, либо
    /// новейший локальный снят позже подтверждённого покрытия), а не по факту
    /// доводки. Возврат: true — выгрузка отстаёт (в т.ч. FAILED при ПУСТОМ томе:
    /// догонять нечего, но короткий сон RetryIntervalSec важнее полного
    /// интервала — сигнал оператору); false — здорова (безделье). При отставании
    /// новейший локальный слепок (если есть) перечитывается и уезжает тем же
    /// конвейером — успешный проход продвигает last_uploaded_unix и закрывает
    /// отставание (§3.5 п.1); промежуточные невыгруженные слепки — нижние
    /// ревизии того же ряда, восстановление интересует только
    /// последний. Ошибка re-export → Failed (за SnapshotLoop — короткий сон).</summary>
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
        Result<Kv?>? last = null;
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

        return Result<EtcdSnapshotStatus?>.Failed(last?.Error
            ?? new ApplicationException("нет живых endpoints etcd"));
    }

    // Ретенция: list префикса → чистый отбор → batch-delete.
    private async Task<Result> ApplyRetentionAsync(CancellationToken ct)
    {
        var listed = await s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct: ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!);
        var stale = EtcdExportRetention.Select(listed.Value, retentionObjects);
        return stale.Count == 0
            ? Result.Success()
            : await s3.DeleteKeysAsync(stale, ct);
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
        Result? last = null;
        foreach (var endpoint in etcdEndpoints)
        {
            var put = await etcd.PutAsync(endpoint, EtcdSnapshotStatusJson.Key, json, lease: null, ct);
            if (put.IsSuccess)
                return Result.Success();
            last = put;
        }

        return Result.Failed(last?.Error ?? new ApplicationException("нет живых endpoints etcd"));
    }

    // Новейший локальный слепок (имя — таймстемп, Ordinal-сортировка = время).
    internal static string? LatestLocalFile(string dir)
        => Directory.Exists(dir)
            ? Directory.GetFiles(dir, "snapshot-*.db").OrderByDescending(f => f, StringComparer.Ordinal).FirstOrDefault()
            : null;
}
