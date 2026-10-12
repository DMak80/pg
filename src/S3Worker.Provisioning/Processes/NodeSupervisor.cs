using System.Text.Json;
using System.Text.Json.Serialization;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Надзор (arch/24 §5 C): health-проба каждой ноды по host:port из portalloc;
/// снесённый контейнер (положительное свидетельство docker-инспекции, S7) и
/// расхождение лимитов (автоконверге) → пересоздание на том же placed_on
/// (volume сохраняется), state=PROVISIONING — в RUNNING переводит следующий
/// тик по health; молчание дольше NodeDeadSec → UNREACHABLE, БЕЗ деструктива
/// (S7: флап ≠ смерть; трек first_seen в work/&lt;C&gt; — WriteSupervisionAsync,
/// S6: фазовые записи трек не затирают); периодический сбор usage
/// (HealthIntervalSec) → nodes/node&lt;k&gt;/usage (snake_case, arch/23 §2.1);
/// слепой inspect — ошибка тика, пересозданий вслепую нет. Одно пересоздание
/// за тик; ноды NOT_INITIALIZED/TO_REMOVE/REMOVING чужих процессов не трогаются.
/// </summary>
public sealed class NodeSupervisor(
    IEtcdGateway gateway,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IOwnS3NodeClient nodeClient,
    SecretProfileStore secrets,
    S3ProvisioningOptions options,
    PortAllocHealer healer,
    TimeProvider? clock = null) // clock — тестовый порог NodeDeadSec (FixedTimeProvider)
{
    private const string Op = "supervise";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Креды профиля: без них env пересозданной ноды не собрать —
        // пересоздания невозможны (diagnostic в supervision-треке).
        if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var nodeSecrets, out var secretDiagnostic))
        {
            var existingTrack = await journal.ReadUnreachableAsync(cluster, ct);
            var skip = await journal.WriteSupervisionAsync(
                cluster, claims.InstanceId, existingTrack.Value ?? new Dictionary<string, long>(),
                $"профиль секретов недоступен: {secretDiagnostic} — пересоздание отложено", ct);
            return skip.IsSuccess ? Result.Success() : skip;
        }

        var recreated = false;
        var warnings = new List<string>();
        var unreachable = new Dictionary<string, long>(
            (await journal.ReadUnreachableAsync(cluster, ct)).Value ?? new Dictionary<string, long>());

        foreach (var nodeSnap in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            var node = nodeSnap.Node;
            var state = await S3ProcessCommon.ReadNodeStateAsync(gateway, endpoints, cluster, node, ct);
            if (!state.IsSuccess)
                return state.Error!;
            var stateValue = state.Value;

            // Ноды демонтажа и незапущенные заявки — не трогаем никогда.
            // PROVISIONING чужого процесса (provisioning A) не пересоздаётся и
            // лимитами не сверяется, но надзор переводит её в RUNNING по зрячей
            // пробе (arch/24 §5 C).
            if (stateValue is S3NodeStates.ToRemove or S3NodeStates.Removing or S3NodeStates.NotInitialized)
                continue;
            var isForeignProvisioning = stateValue == S3NodeStates.Provisioning;
            var supervisable = !isForeignProvisioning;

            // Лестница E9 ДО деструктива: адрес ноды из portalloc (или реконструкция).
            var address = await ResolveAddressAsync(cluster, node, ct);
            if (!address.IsSuccess)
                return address.Error!;

            // Docker-факт: объект есть? Слепой inspect — ошибка тика (пересозданий вслепую нет).
            var live = await driver.InspectNodeAsync(cluster, node, ct);
            if (!live.IsSuccess)
                return live.Error!;

            if (live.Value is null)
            {
                // Снесённый контейнер (положительное свидетельство) → пересоздание.
                if (!supervisable)
                    continue; // PROVISIONING чужого процесса — чужая ответственность
                if (recreated)
                    continue; // одно пересоздание за тик
                var rebuilt = await RecreateAsync(snap, node, address.Value, nodeSecrets!, "container missing", ct);
                if (!rebuilt.IsSuccess)
                    return rebuilt.Error!;
                recreated = true;
                unreachable.Remove(node);
                continue;
            }

            // Автоконверге лимитов: docker-факт (cpu/mem) vs декларация; disk —
            // декларация, не сверяется (arch/23 §2). Одно пересоздание за тик.
            if (supervisable)
            {
                var liveLimits = await driver.NodeResourcesAsync(cluster, node, ct);
                if (!liveLimits.IsSuccess)
                    return liveLimits.Error!;
                var declared = S3ProcessCommon.ParseResources(nodeSnap.Resources);
                if (!LimitsMatch(liveLimits.Value, declared))
                {
                    if (!recreated)
                    {
                        warnings.Add($"лимиты контейнера {node} расходятся с декларацией — пересоздание");
                        var rebuilt = await RecreateAsync(snap, node, address.Value, nodeSecrets!, "limits drift", ct);
                        if (!rebuilt.IsSuccess)
                            return rebuilt.Error!;
                        recreated = true;
                        unreachable.Remove(node);
                        continue;
                    }

                    warnings.Add($"лимиты {node} расходятся — отложено до следующего тика");
                    continue;
                }
            }

            // Health-проба платформенной health-грани (arch/23 §5) по адресу portalloc.
            var health = await nodeClient.HealthAsync(address.Value.Host, address.Value.S3Port, ct);
            if (health.IsSuccess && health.Value)
            {
                // Успешное зондирование — счётчик молчания ноды стартует заново;
                // PROVISIONING → RUNNING (завершение чужого/своего provisioning).
                unreachable.Remove(node);
                if (stateValue != S3NodeStates.Running)
                {
                    var running = await S3ProcessCommon.WriteNodeStateAsync(
                        gateway, endpoints, cluster, node, S3NodeStates.Running, ct);
                    if (!running.IsSuccess)
                        return running;
                }

                // Периодический usage-сбор (HealthIntervalSec-гейт по updated_unix):
                // ошибка usage — транзиент, тик не фейлит (риск §15.3: грань/диск).
                var usage = await CollectUsageAsync(
                    cluster, node, address.Value, nodeSecrets!, snap, ct);
                if (!usage.IsSuccess)
                    warnings.Add($"usage {node}: {usage.Error!.Message}");
                continue;
            }

            // Docker-факт прочитан (inspect успешен выше) — проба зрячая: любой
            // отказ пробы = положительное свидетельство молчания ноды. Слепота
            // воркера — только недоступность docker-факта (ошибка тика выше).

            // Нода молчит: трек first_seen — только для supervisable
            // (PROVISIONING грузится — бюджет молчания надзора не стартует).
            if (!supervisable)
            {
                warnings.Add($"PROVISIONING {node} молчит — трек надзора заморожен");
                continue;
            }

            var firstSeen = unreachable.TryGetValue(node, out var seen) ? seen : NowUnix();
            unreachable[node] = firstSeen;
            if (_clock.GetUtcNow().ToUnixTimeSeconds() - firstSeen > options.NodeDeadSec)
            {
                // UNREACHABLE — маркировка БЕЗ деструктива (arch/24 §5 C: флап ≠ смерть);
                // восстановит проба следующего тика либо пересоздание по docker-факту.
                var dead = await S3ProcessCommon.WriteNodeStateAsync(
                    gateway, endpoints, cluster, node, S3NodeStates.Unreachable, ct);
                if (!dead.IsSuccess)
                    return dead;
            }
        }

        // Стационарная запись надзора (трек first_seen + warning-и тика).
        return await journal.WriteSupervisionAsync(
            cluster, claims.InstanceId, unreachable,
            warnings.Count > 0 ? string.Join("; ", warnings) : null, ct);
    }

    private long NowUnix() => _clock.GetUtcNow().ToUnixTimeSeconds();

    private static bool LimitsMatch(Shared.Docker.NodeLimits? live, (decimal? Cpu, long? MemBytes)? declared)
    {
        if (declared is null)
            return true;
        return NullableDecimalEquals(live?.NanoCpus / 1_000_000_000m, declared.Value.Cpu)
               && NullableLongEquals(live?.MemoryBytes, declared.Value.MemBytes);
    }

    private static bool NullableDecimalEquals(decimal? a, decimal? b) => (a, b) switch
    {
        (null, null) => true,
        ({ } x, { } y) => x == y,
        _ => false,
    };

    private static bool NullableLongEquals(long? a, long? b) => (a, b) switch
    {
        (null, null) => true,
        ({ } x, { } y) => x == y,
        _ => false,
    };

    // Адрес ноды: portalloc-запись; нет записи — лестница E9 (реконструкция из
    // inspect живого контейнера, put-if-absent под locks/portalloc).
    private async Task<Result<NodeAddress>> ResolveAddressAsync(string cluster, string node, CancellationToken ct)
    {
        var current = await GetAsync(S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!current.IsSuccess)
            return Result<NodeAddress>.Failed(current.Error!);
        if (current.Value is { } kv)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && doc.RootElement.TryGetProperty(node, out var entry))
                {
                    return Result<NodeAddress>.Success(new NodeAddress(
                        entry.GetProperty("host").GetString()!,
                        entry.GetProperty("s3").GetInt32()));
                }
            }
            catch (JsonException ex)
            {
                return Result<NodeAddress>.Failed(ex);
            }
        }

        var healed = await healer.HealNodeAsync(cluster, node, ct);
        if (!healed.IsSuccess)
            return Result<NodeAddress>.Failed(healed.Error!);

        // После реконструкции адрес — из inspect; перечитываем запись (S5).
        var reread = await GetAsync(S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!reread.IsSuccess)
            return Result<NodeAddress>.Failed(reread.Error!);
        if (reread.Value is not { } fresh)
            return Result<NodeAddress>.Failed(new ApplicationException(
                $"supervise {cluster}/{node}: адрес реконструирован, записи portalloc нет"));

        try
        {
            var alloc = S3ProcessCommon.ParsePortAlloc(fresh.Value);
            return alloc.TryGetValue(node, out var addr)
                ? Result<NodeAddress>.Success(addr)
                : Result<NodeAddress>.Failed(new ApplicationException(
                    $"supervise {cluster}/{node}: записи ноды нет в portalloc после реконструкции"));
        }
        catch (JsonException ex)
        {
            return Result<NodeAddress>.Failed(ex);
        }
    }

    // Usage-сбор (arch/24 §5 C): ключа нет или updated_unix старше
    // HealthIntervalSec → проба грани → put snake_case (arch/23 §2.1).
    // Ошибка грани — Failed (тик только warning-ит; не фейлит).
    private async Task<Result> CollectUsageAsync(
        string cluster, string node, NodeAddress address,
        OwnS3NodeSecrets nodeSecrets, S3ClusterSnapshot snap, CancellationToken ct)
    {
        var usageKey = S3ProcessCommon.NodeUsageKey(cluster, node);
        var current = await GetAsync(usageKey, ct);
        if (!current.IsSuccess)
            return current.Error!;
        if (current.Value is { } kv)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.TryGetProperty("updated_unix", out var updated)
                    && NowUnix() - updated.GetInt64() < options.HealthIntervalSec)
                    return Result.Success(); // свежий usage — не собираем
            }
            catch (JsonException)
            {
                // битый ключ — пересоберём (перезапись ниже)
            }
        }

        var fact = await nodeClient.GetUsageAsync(address.Host, address.S3Port, nodeSecrets, ct);
        if (!fact.IsSuccess)
            return fact.Error!;

        var value = JsonSerializer.Serialize(new UsagePayload(fact.Value!.UsedBytes, fact.Value.TotalBytes, NowUnix()));
        return await PutAsync(usageKey, value, ct);
    }

    // Пересоздание ноды на том же placed_on (volume жив — EnsureNode не трогает
    // том данных), env профиля, порт/лимиты из portalloc/декларации.
    private async Task<Result> RecreateAsync(
        S3ClusterSnapshot snap, string node, NodeAddress address,
        OwnS3NodeSecrets nodeSecrets, string reason, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var nodeSnap = snap.Nodes.FirstOrDefault(n => n.Node == node);
        var limits = S3ProcessCommon.ParseResources(nodeSnap?.Resources);

        // Контейнер мог остаться живым (лимиты) — сначала снос; volume не трогаем.
        var removed = await driver.RemoveNodeAsync(cluster, node, removeVolume: false, ct);
        if (!removed.IsSuccess)
            return removed;

        var env = new Dictionary<string, string>(nodeSecrets.ToContainerEnv())
        {
            ["OWNS3_DATA_DIR"] = "/data",
        };
        var ensured = await driver.EnsureNodeAsync(
            new S3NodeSpec(
                cluster, node, address.Host, address.S3Port, options.OwnS3Image, env,
                limits?.Cpu, limits?.MemBytes),
            options.ContourNetwork, ct);
        if (!ensured.IsSuccess)
            return ensured;

        var state = await S3ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, node, S3NodeStates.Provisioning, ct);
        if (!state.IsSuccess)
            return state;
        return await journal.WritePhaseAsync(cluster, Op, $"recreated-{reason}", claims.InstanceId, null, ct);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    // usage-ключ (arch/23 §2.1 — snake_case).
    private sealed record UsagePayload(
        [property: JsonPropertyName("used_bytes")] long UsedBytes,
        [property: JsonPropertyName("total_bytes")] long TotalBytes,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix);
}
