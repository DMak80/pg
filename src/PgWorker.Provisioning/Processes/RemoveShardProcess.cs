using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// RemoveShardProcess — демонтаж ОТДЕЛЬНОГО шарда Active-кластера по маркеру
/// shards/&lt;X&gt;/state=TO_REMOVE (t06 spec §5.3; arch/14 §5 H; эталон remove-shard.sh
/// + DeprovisioningProcess scoped-to-shard). Guard'ы G1–G7 в S1 перед любым
/// разрушающим действием; порядок «сначала docker, потом etcd» — мёртвые ключи
/// при сбое безвредны (маркер стоит, повторный тик продолжает). Кластер живёт.
/// </summary>
public sealed class RemoveShardProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "remove-shard";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"remove-shard {cluster}/{shardName}: клэйм не наш (или потерян) — мутации запрещены"));

        var shard = snap.Shards.FirstOrDefault(s => s.Name == shardName);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // уже демонтирован

        // S0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // G1 (R6): свежее чтение config ДО guard'ов — NOT_INITIALIZED/TO_REMOVE
        // безопасно прекращает демонтаж шарда (provisioning поднимет declared-шард
        // как обычный / deprovisioning кластера снесёт всё сам).
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // S1: guard'ы G2–G7 (§4.4) — над снапшотом тика; провал = last_error
        // с причиной + InProgress (маркер-состояние живёт, повтор тиком).
        var blocked = CheckGuards(snap, shard);
        if (blocked is { } guardId)
        {
            await journal.WritePhaseAsync(cluster, Op, $"blocked-{guardId}", claims.InstanceId,
                GuardReason(snap, shard, guardId), ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // G5 (заявки /pgworker/moves/<C>/ с to/old_shard == X) — единственный
        // guard с чтением вне снапшота; саморазрешающийся: MoveProcess отклонит
        // заявку перманентно (§5.5) и удалит её — следующий тик пройдёт guard.
        var movesRef = await MovesReferenceShardAsync(cluster, shardName, ct);
        if (!movesRef.IsSuccess)
            return await FailAsync(cluster, movesRef.Error!, "guards", ct);
        if (movesRef.Value)
        {
            await journal.WritePhaseAsync(cluster, Op, "blocked-G5", claims.InstanceId,
                "есть заявки переездов, ссылающиеся на шард — дождитесь их разбора", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // S2: REMOVING → RemoveNode каждой ноды (404 = ок) + сироты шарда.
        var removed = await RemoveNodesAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            return await FailAsync(cluster, removed.Error!, "removing-nodes", ct);

        // S3: guard docker-объектов нет → чистка etcd.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return await FailAsync(cluster, objects.Error!, "listing-objects", ct);
        if (objects.Value.Any(name => name.StartsWith($"pgw-{cluster}-{shardName}-", StringComparison.Ordinal)))
            return await Finish(cluster, "removing-nodes", ProcessOutcome.InProgress, ct);

        var cleaned = await CleanKeysAsync(cluster, shardName, ct);
        if (!cleaned.IsSuccess)
            return await FailAsync(cluster, cleaned.Error!, "cleaning-keys", ct);

        // S4: снапшот P12 (точка изменения) + done. Кластер продолжает жить.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    // Guard'ы G2–G4/G6/G7 — чистые функции над снапшотом тика; null = прошли.
    internal static string? CheckGuards(ClusterSnapshot snap, ShardSpec shard)
    {
        if (shard.Replicas <= 0 && shard.Nodes.Count == 0)
            return "G2";
        if (snap.Routing.Any(r => r.Owner == shard.Name))
            return "G3";
        if (snap.Routing.Any(r => r.Status is not null
                && (r.MoveSource == shard.Name || r.MoveTarget == shard.Name)))
            return "G4";
        if (shard.Nodes.Any(n => n.State == NodeState.Quarantined))
            return "G6";
        if (snap.Shards.Count <= 1)
            return "G7";
        return null;
    }

    // Человекочитаемые причины блокировки (тексты §4.4 — переводятся в last_error).
    internal static string GuardReason(ClusterSnapshot snap, ShardSpec shard, string guardId) => guardId switch
    {
        "G2" => "шард не заявлен — нечего демонтировать",
        "G3" => $"на шарде {snap.Routing.Count(r => r.Owner == shard.Name)} бакетов (routing) — " +
                "сначала явно перевезите их (заявки /pgworker/moves/, UI переездов — t07)",
        "G4" => "незавершённый переезд бакета — завершите/отмените",
        "G5" => "есть заявки переездов, ссылающиеся на шард — дождитесь их разбора",
        "G6" => "шард в карантине после эвакуации — сначала разбор данных (t05 runbook)",
        "G7" => "нельзя снять последний шард — для полного демонтажа удалите кластер",
        _ => $"guard {guardId}",
    };

    // G5: живые заявки переездов кластера ссылаются на шард (to ИЛИ old_shard).
    private async Task<Result<bool>> MovesReferenceShardAsync(string cluster, string shardName, CancellationToken ct)
    {
        var moves = await RangeAsync($"/pgworker/moves/{cluster}/", ct);
        if (!moves.IsSuccess)
            return Result<bool>.Failed(moves.Error!);

        foreach (var kv in moves.Value)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.TryGetProperty("to", out var to) && to.ValueKind == JsonValueKind.String
                    && to.GetString() == shardName)
                    return Result<bool>.Success(true);
                if (root.TryGetProperty("old_shard", out var old) && old.ValueKind == JsonValueKind.String
                    && old.GetString() == shardName)
                    return Result<bool>.Success(true);
            }
            catch (JsonException)
            {
                // битая заявка — разберёт MoveProcess (t01: логируется, не молчит);
                // демонтажу она не мешает
            }
        }

        return Result<bool>.Success(false);
    }

    // S2: REMOVING + RemoveNode заявленных нод, затем сироты префикса шарда.
    private async Task<Result> RemoveNodesAsync(string cluster, ShardSpec shard, CancellationToken ct)
    {
        foreach (var node in shard.Nodes)
        {
            if (node.State != NodeState.Removing)
            {
                var marked = await PutAsync(
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state", "REMOVING", ct);
                if (!marked.IsSuccess)
                    return marked;
            }

            var removed = await driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        // Сироты шарда: имя с префиксом pgw-<C>-<X>-, nodes-ключа нет.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return objects;

        var known = shard.Nodes
            .Select(n => $"pgw-{cluster}-{shard.Name}-{n.Name}")
            .ToHashSet();
        var prefix = $"pgw-{cluster}-{shard.Name}-";
        foreach (var orphan in objects.Value.Where(name =>
                     name.StartsWith(prefix, StringComparison.Ordinal) && !known.Contains(name)).ToList())
        {
            var tail = orphan[($"pgw-{cluster}-").Length..].Split('-');
            if (tail.Length < 2)
                continue; // чужое имя с нашим префиксом — не трогаем вслепую

            var nodeName = tail[^1];
            var removed = await driver.RemoveNodeAsync(cluster, shard.Name, nodeName, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        return Result.Success();
    }

    // S3: чистка etcd — всё про шард; остальные шарды не затронуты.
    private async Task<Result> CleanKeysAsync(string cluster, string shardName, CancellationToken ct)
    {
        var scope = $"{cluster}-{shardName}";

        // Префикс шарда целиком (state/replicas/nodes/dsn/master — всё).
        var delShard = await DeleteAsync($"/clusters/{cluster}/shards/{shardName}/", prefix: true, ct);
        if (!delShard.IsSuccess)
            return delShard;

        // Точечные заявки ресурсов (даже если scope ещё жив) + префикс scope.
        foreach (var request in (string[])["request_cpu", "request_mem", "request_disk"])
        {
            var del = await DeleteAsync($"/service/{scope}/{request}", prefix: false, ct);
            if (!del.IsSuccess)
                return del;
        }

        var delScope = await DeleteAsync($"/service/{scope}/", prefix: true, ct);
        if (!delScope.IsSuccess)
            return delScope;

        // portalloc: точечная фильтрация записей "<X>/<n>" из JSON (Д10 — ключ общий
        // на кластер, read-modify-write под клэймом безопасен). Сбой чтения ≠
        // «ключа нет»: молчаливый пропуск фильтрации оставил бы записи шарда в
        // portalloc навсегда (шард исчез из /clusters/ — повторного тика не будет) —
        // возвращаем ошибку, ретрай следующим тиком доведёт чистку.
        var ports = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (ports is { IsSuccess: false })
            return ports;
        if (ports is { IsSuccess: true, Value: not null })
        {
            var parsed = Portalloc.Parse(cluster, ports.Value.Value);
            if (parsed.IsSuccess)
            {
                var kept = parsed.Value
                    .Where(p => !p.Key.StartsWith($"{shardName}/", StringComparison.Ordinal))
                    .ToDictionary(p => p.Key, p => p.Value);
                var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(kept), ct);
                if (!put.IsSuccess)
                    return put;
            }
        }

        // Журнал эвакуации не переживает демонтаж шарда.
        return await DeleteAsync($"/pgworker/evacuations/{cluster}/{shardName}", prefix: false, ct);
    }

    // G1 (R6): свежее чтение config — NOT_INITIALIZED/TO_REMOVE прекращает демонтаж.
    private async Task<bool> ClusterStateChangedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess || config.Value is not { } kv)
            return false;
        return kv.Value.Contains("\"NOT_INITIALIZED\"") || kv.Value.Contains("\"TO_REMOVE\"");
    }

    private async Task<Result<ProcessOutcome>> Finish(
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteAsync(string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, prefix, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
