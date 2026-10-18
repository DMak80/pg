using System.Text.Json;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Rolling-ротация секретов нод (arch/23 §6, arch/24 §4): заявка
/// /s3worker/rotations/&lt;C&gt; — ЭКСКЛЮЗИВНАЯ ветка тика (надзор/D/E этим тиком
/// не идут — дисциплина «одно пересоздание за тик»). Пересоздание — по одной
/// ноде за тик с env ТЕКУЩЕГО профиля кластера (config.secret_profile читается
/// свежим снапшотом — последовательные ротации работают); volume не трогается
/// (данные объектов живы). Wait-семантика: пересозданная нода обязана ожить
/// (RUNNING) до перехода к следующей; невалидный профиль — journal-отказ
/// (постоянный; оператор чинит env). Значения секретов не попадают в журнал.
/// </summary>
public sealed class SecretRotator(
    IEtcdGateway gateway,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    SecretProfileStore secrets,
    PkiStore pki,
    InstanceEnvComposer envComposer,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options)
{
    private const string Op = "rotate-secrets";

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Заявка ротации: нет — ветка не активна (no-op).
        var ticket = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.RotationKey(cluster), ct);
        if (!ticket.IsSuccess)
            return ticket.Error!;
        if (ticket.Value is null)
            return Result.Success();

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Профиль ТЕКУЩИЙ (свежий снапшот config): невалиден — journal-отказ,
        // заявка жива (оператор чинит env; пересозданий нет).
        if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var nodeSecrets, out var diagnostic))
        {
            var reject = await journal.WritePhaseAsync(
                cluster, Op, "invalid-secret-profile", claims.InstanceId, diagnostic, ct);
            return reject.IsSuccess ? Result.Success() : reject;
        }

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;

        foreach (var node in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            var matches = await NodeEnvMatchesAsync(cluster, node, nodeSecrets!, ct);
            if (!matches.IsSuccess)
                return matches.Error!;

            if (matches.Value)
            {
                // Нода на env профиля, но ещё не ожила после пересоздания:
                // health-проба по advertised-адресу (канон P) → RUNNING.
                // НЕ ожила — ждём (следующая не трогается: rolling, не
                // параллельный снос). Оживление — сам ротатор: заявка —
                // ЭКСКЛЮЗИВНАЯ ветка тика, надзор этим тиком не идёт.
                if (node.State != S3NodeStates.Running)
                {
                    var probe = await ResolveAddressAsync(cluster, node.Node, ct);
                    if (!probe.IsSuccess)
                        return probe.Error!;
                    var health = await nodeClient.HealthAsync(
                        options.AdvertisedHost ?? probe.Value.Host, probe.Value.S3Port, ct);
                    if (!health.IsSuccess || !health.Value)
                        return await journal.WritePhaseAsync(
                            cluster, Op, "waiting-node", claims.InstanceId, null, ct);
                    var running = await S3ProcessCommon.WriteNodeStateAsync(
                        gateway, endpoints, cluster, node.Node, S3NodeStates.Running, ct);
                    if (!running.IsSuccess)
                        return running;
                }

                continue;
            }

            // Первая нева нода: пересоздание с env профиля (volume жив).
            var removed = await driver.RemoveNodeAsync(cluster, node.Node, removeVolume: false, ct);
            if (!removed.IsSuccess)
                return removed;
            var address = await ResolveAddressAsync(cluster, node.Node, ct);
            if (!address.IsSuccess)
                return address.Error!;

            // PKI-материал переиспользуется из etcd (ключи переживают
            // пересоздание, arch/24 §4); отказ — отложенный тик (диагностика).
            var clusterPki = await pki.EnsureClusterPkiAsync(cluster, ct);
            if (!clusterPki.IsSuccess)
                return await journal.WritePhaseAsync(
                    cluster, Op, "waiting-pki", claims.InstanceId, clusterPki.Error!.Message, ct);
            var nodePki = await pki.EnsureNodePkiAsync(cluster, node.Node, clusterPki.Value.Ca, ct);
            if (!nodePki.IsSuccess)
                return await journal.WritePhaseAsync(
                    cluster, Op, "waiting-pki", claims.InstanceId, nodePki.Error!.Message, ct);

            var env = envComposer.Compose(
                nodeSecrets!, nodePki.Value, clusterPki.Value.Ca.Ca,
                $"http://{options.AdvertisedHost ?? address.Value.Host}:{address.Value.S3Port}");
            var resources = S3ProcessCommon.ParseResources(node.Resources);
            var ensured = await driver.EnsureNodeAsync(
                new S3NodeSpec(
                    cluster, node.Node, address.Value.Host, address.Value.S3Port,
                    options.OwnS3Image, env, resources?.Cpu, resources?.MemBytes),
                options.ContourNetwork, ct);
            if (!ensured.IsSuccess)
                return ensured;
            var marking = await S3ProcessCommon.WriteNodeStateAsync(
                gateway, endpoints, cluster, node.Node, S3NodeStates.Provisioning, ct);
            if (!marking.IsSuccess)
                return marking;

            // Одна нода за тик: следующая — когда эта дойдёт до RUNNING.
            return await journal.WritePhaseAsync(
                cluster, Op, $"rotated-{node.Node}", claims.InstanceId, null, ct);
        }

        // Все ноды приведены: заявка гасится (одна txn) + journal done.
        var del = await DeleteKeyAsync(S3ProcessCommon.RotationKey(cluster), ct);
        if (!del.IsSuccess)
            return del;
        return await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
    }

    // Env-сверка БЕЗ значений секретов: OWNS3_ROOT_USER равен профилю (имя —
    // не секрет) и состав ключей совпадает (число OWNS3_ACCESS_KEYS__<i>__*).
    private async Task<Result<bool>> NodeEnvMatchesAsync(
        string cluster, S3NodeSnapshot node, OwnS3NodeSecrets expected, CancellationToken ct)
    {
        var env = await driver.NodeEnvAsync(cluster, node.Node, ct);
        if (!env.IsSuccess)
            return Result<bool>.Failed(env.Error!);
        if (env.Value is null)
            return Result<bool>.Success(false); // объекта нет — ротация (пере)создаст

        if (!env.Value.TryGetValue("OWNS3_ROOT_USER", out var actualUser)
            || !string.Equals(actualUser, expected.RootUser, StringComparison.Ordinal))
            return Result<bool>.Success(false);

        // Индекс — ВТОРОЙ сегмент: OWNS3_ACCESS_KEYS__<i>__<FIELD>.Split("__")
        // = [OWNS3_ACCESS_KEYS, <i>, <FIELD>]; [2] — имя поля, не индекс.
        var actualKeyIndexes = env.Value.Keys
            .Where(k => k.StartsWith("OWNS3_ACCESS_KEYS__", StringComparison.Ordinal))
            .Select(k => k.Split("__")[1])
            .ToHashSet(StringComparer.Ordinal);
        var expectedKeyIndexes = expected.AccessKeys
            .Select((_, i) => i.ToString(System.Globalization.CultureInfo.InvariantCulture))
            .ToHashSet(StringComparer.Ordinal);
        return Result<bool>.Success(actualKeyIndexes.SetEquals(expectedKeyIndexes));
    }

    // Адрес ноды: portalloc (ротация не меняет закрепления).
    private async Task<Result<NodeAddress>> ResolveAddressAsync(string cluster, string node, CancellationToken ct)
    {
        var current = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!current.IsSuccess)
            return Result<NodeAddress>.Failed(current.Error!);
        if (current.Value is not { } kv)
            return Result<NodeAddress>.Failed(new ApplicationException(
                $"rotate-secrets {cluster}/{node}: no portalloc entry"));
        try
        {
            var alloc = S3ProcessCommon.ParsePortAlloc(kv.Value);
            return alloc.TryGetValue(node, out var addr)
                ? Result<NodeAddress>.Success(addr)
                : Result<NodeAddress>.Failed(new ApplicationException(
                    $"rotate-secrets {cluster}/{node}: node not in portalloc"));
        }
        catch (JsonException ex)
        {
            return Result<NodeAddress>.Failed(ex);
        }
    }

    private async Task<Result> DeleteKeyAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
