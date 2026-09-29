using System.Text.RegularExpressions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using PgWorker.Core;
using PgWorker.Etcd.Parsing;
using Shared.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Гварды сирот: имена канонические (иначе 404 без похода в etcd), префикс в
// реестре OBSERVED (нет → 404; DELETING → 409); подсистема включена (503).
internal static partial class OrphanGuards
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardPattern();

    // Чтение записи реестра для префикса: имена → 404 без etcd; выключено →
    // 503; сбой всех endpoints → 503 (EtcdWriteUnavailableException);
    // префикса нет → 404; DELETING → 409.
    public static async Task<Result<OrphanEntry>> FindObservedAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string shard,
        Func<BackupsRuntimeOptions?> runtime, CancellationToken ct)
    {
        if (!ClusterPattern().IsMatch(cluster) || !ShardPattern().IsMatch(shard))
            return Result<OrphanEntry>.Failed(
                new OrphanNotFoundException($"{cluster}/{shard}"));
        if (runtime() is null)
            return Result<OrphanEntry>.Failed(new BackupsDisabledException());

        var prefix = $"{cluster}/{shard}";
        var kv = await EtcdFailover.CallAsync(endpoints,
            e => gateway.GetAsync(e, OrphanRegistry.Key, ct));
        if (!kv.IsSuccess)
            return Result<OrphanEntry>.Failed(kv.Error!);

        var registry = kv.Value is { } some ? OrphanRegistry.Parse(some.Value) : null;
        var entry = registry?.Orphans.FirstOrDefault(e => e.Prefix == prefix);
        if (entry is null)
            return Result<OrphanEntry>.Failed(new OrphanNotFoundException(prefix));
        if (entry.State == OrphanState.Deleting)
            return Result<OrphanEntry>.Failed(new OrphanDeletingException(prefix));
        return Result<OrphanEntry>.Success(entry);
    }
}

// POST /api/backups/orphans/{c}/{x}/hold и DELETE ... (t04, arch/19 §4):
// hold-флаг сироты «до разбора». Put делает ТОЛЬКО API (атомарный put/del
// одного ключа, без read-modify-write); повторный hold идемпотентен (put
// поверх), unhold идемпотентен (del, ключа нет — тоже успех). Реестр не пишет.
public sealed class OrphanHoldHandler(
    IEtcdGateway gateway, string[] endpoints, TimeProvider time,
    Func<BackupsRuntimeOptions?> runtime)
{
    // Put hold-ключа {"set_unix":now,"set_by":<requestedBy>} — 204.
    public async Task<Result> SetAsync(string cluster, string shard, string requestedBy, CancellationToken ct)
    {
        var guard = await OrphanGuards.FindObservedAsync(gateway, endpoints, cluster, shard, runtime, ct);
        if (!guard.IsSuccess)
            return Result.Failed(guard.Error!);

        var now = time.GetUtcNow().ToUnixTimeSeconds();
        return await EtcdFailover.CallAsync(endpoints,
            e => gateway.PutAsync(e, OrphanRegistry.HoldKey(guard.Value.Prefix),
                OrphanRegistry.HoldToJson(now, requestedBy), null, ct));
    }

    // Del hold-ключа — 204 идемпотентен (ключа нет — тоже успех). Реестр НЕ
    // читаем: у unhold по контракту только 503 (spec §3.4).
    public async Task<Result> RemoveAsync(string cluster, string shard, CancellationToken ct)
    {
        if (runtime() is null)
            return Result.Failed(new BackupsDisabledException());

        return await EtcdFailover.CallAsync(endpoints,
            e => gateway.DeleteAsync(e, OrphanRegistry.HoldKey($"{cluster}/{shard}"), prefix: false, ct));
    }
}

// POST /api/backups/orphans/{c}/{x}/delete (t04, arch/19 §4): заявка явного
// удаления сироты — единственный путь удалить защищённую (hold/валидный
// полный): минует оба гварда отбора. Повторная заявка — put поверх (не 409:
// заявка — состояние, не эксклюзивный клэйм). Исполняет sweeper ближайшим
// проходом; заявка и запись гасятся вместе.
public sealed class OrphanDeleteHandler(
    IEtcdGateway gateway, string[] endpoints, TimeProvider time,
    Func<BackupsRuntimeOptions?> runtime)
{
    // Put ключа заявки {"requested_unix":now,"requested_by":<requestedBy>} — 202 + DTO.
    public async Task<Result<OrphanDeleteRequestedDto>> RequestAsync(
        string cluster, string shard, OrphanDeleteRequest? body, string requestedBy, CancellationToken ct)
    {
        var guard = await OrphanGuards.FindObservedAsync(gateway, endpoints, cluster, shard, runtime, ct);
        if (!guard.IsSuccess)
            return Result<OrphanDeleteRequestedDto>.Failed(guard.Error!);
        var prefix = guard.Value.Prefix;

        // confirm обязан совпадать с префиксом (осознанная потеря DR-источника).
        if (body?.Confirm != prefix)
            return Result<OrphanDeleteRequestedDto>.Failed(
                new OrphanConfirmMismatchException(prefix, body?.Confirm ?? ""));

        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var put = await EtcdFailover.CallAsync(endpoints,
            e => gateway.PutAsync(e, OrphanRegistry.DeleteKey(prefix),
                OrphanRegistry.DeleteToJson(now, requestedBy), null, ct));
        if (!put.IsSuccess)
            return Result<OrphanDeleteRequestedDto>.Failed(put.Error!);

        return Result<OrphanDeleteRequestedDto>.Success(
            new OrphanDeleteRequestedDto(prefix, now, requestedBy));
    }
}

/// <summary>Тело заявки удаления сироты: {"confirm":"<C>/<X>"}.</summary>
public sealed record OrphanDeleteRequest(string? Confirm);

/// <summary>Ответ 202: заявка принята, sweeper исполнит ближайшим проходом
/// (Minimal API отдаёт camelCase {"prefix","requestedUnix","requestedBy"}).</summary>
public sealed record OrphanDeleteRequestedDto(string Prefix, long RequestedUnix, string RequestedBy);
