using System.Diagnostics.Metrics;
using System.Text.Json;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Процесс G — MeshGovernance (arch/24 §5 G, spec §3.5): приведение фактического
/// mesh инстансов к декларациям links + denylist. Шаги тика (порядок фиксирован):
/// 1) deny-доведение — полная перезапись PUT /replication/denylist всем RUNNING
/// (дешёвая, идемпотентная — не «одна за тик»); 2) сверка состава — желаемые
/// target-записи инстанса X = соседи по links минус denylist минус
/// несуществующие/не-RUNNING (deny-инстанс — всегда ∅); расхождение → ОДНА
/// структурная операция за тик (первая по Ordinal): недостающая пара → join,
/// лишняя запись → delete-target на каждой стороне пары. Битые links/denylist
/// (parseError снапшота) — диагностика в journal, битая часть не применяется;
/// отсутствующий links — пустой граф (полный разрыв — валидная декларация).
/// 0 RUNNING — no-op (idle). Все операции под клэймом &lt;C&gt;.
/// Та же логика — фаза P4 provisioning'а (arch/24 §5: P4 — та же механика, что G).
/// </summary>
public sealed class MeshGovernanceProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    SecretProfileStore secrets,
    PkiStore pki,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options,
    Meter meter)
{
    public const string Op = "mesh-governance";

    private readonly Counter<long> _join = meter.CreateCounter<long>("s3worker_mesh_join_total");
    private readonly Counter<long> _delete = meter.CreateCounter<long>("s3worker_mesh_delete_total");
    private readonly Counter<long> _deny = meter.CreateCounter<long>("s3worker_mesh_deny_total");

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Креды профиля: без них админ-грань недостижима — диагностический skip
        // (по образцу надзора; оператор чинит env — тик повторит).
        if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var nodeSecrets, out var secretDiagnostic))
            return await SkipAsync(cluster, $"профиль секретов недоступен: {secretDiagnostic}", ct);

        // Адреса инстансов (portalloc, формат arch/23 §8 — словарь по именам).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Fail(cluster, addresses.Error!, "portalloc", ct);

        var running = snap.Nodes
            .Where(n => n.State == S3NodeStates.Running)
            .Select(n => n.Node)
            .ToHashSet(StringComparer.Ordinal);
        if (running.Count == 0)
            return await SkipAsync(cluster, null, ct);

        // Битые декларации (parseError снапшота при наличии ключа) — не применяем.
        var linksError = snap.ParseError?.Contains("/links: битый JSON", StringComparison.Ordinal) == true;
        var denyError = snap.ParseError?.Contains("/denylist: битый JSON", StringComparison.Ordinal) == true;
        var warnings = new List<string>();
        if (linksError)
            warnings.Add("links: битый JSON — граф не применяется (диагностика в parseError)");
        if (denyError)
            warnings.Add("denylist: битый JSON — отзовы не применяются");

        // PKI: ленивый выпуск CA+оркестратора (перед deny-доведением).
        var clusterPki = await pki.EnsureClusterPkiAsync(cluster, ct);
        if (!clusterPki.IsSuccess)
            return Fail(cluster, clusterPki.Error!, "pki", ct);
        var orchestrator = clusterPki.Value.Orchestrator;

        // Шаг 1: deny-доведение — всем RUNNING сразу (идемпотентно).
        var denyPushed = false;
        if (!denyError)
        {
            var denyPush = await PushDenyListAsync(
                cluster, snap, running, addresses.Value, nodeSecrets!, orchestrator, warnings, ct);
            if (!denyPush.IsSuccess)
                return denyPush.Error!;
            denyPushed = denyPush.Value;
        }

        // Шаг 2: сверка состава — одна структурная операция за тик. Битый links
        // (parseError) — состав недостоверен: сверка НЕ выполняется вовсе (ни
        // join, ни снос — битая часть не применяется, §3.3); отсутствующий links —
        // пустой граф (полный разрыв — валидная декларация).
        var phase = denyPushed ? "deny-pushed" : "idle";
        if (!linksError)
        {
            var links = snap.Links ?? [];
            var denySet = denyError ? [] : snap.DenyList ?? [];
            var existing = snap.Nodes.Select(n => n.Node).ToHashSet(StringComparer.Ordinal);

            // Желаемые и фактические составы RUNNING-инстансов.
            var mismatches = new List<MeshMismatch>();
            foreach (var x in running.OrderBy(n => n, StringComparer.Ordinal))
            {
                var desired = DesiredNeighbors(x, links, denySet, existing, running);
                var status = await nodeClient.GetReplicationStatusAsync(
                    Host(addresses.Value, x), Port(addresses.Value, x), nodeSecrets!, ct);
                if (!status.IsSuccess)
                {
                    warnings.Add($"status {x}: {status.Error!.Message} — транзиент");
                    continue;
                }

                var actual = status.Value!.NeighborNames;
                foreach (var y in desired.Where(d => !actual.Contains(d, StringComparer.Ordinal)).OrderBy(d => d, StringComparer.Ordinal))
                    mismatches.Add(new MeshMismatch(MeshOp.Join, x, y));
                foreach (var y in actual.Where(a => !desired.Contains(a, StringComparer.Ordinal)).OrderBy(a => a, StringComparer.Ordinal))
                    mismatches.Add(new MeshMismatch(MeshOp.Delete, x, y));
            }

            // Первая по Ordinal операция: join до delete-пар (детерминизм тика).
            var operation = mismatches
                .OrderBy(m => m.Op)
                .ThenBy(m => m.X, StringComparer.Ordinal)
                .ThenBy(m => m.Y, StringComparer.Ordinal)
                .FirstOrDefault();
            if (operation is not null)
            {
                var outcome = await ApplyAsync(
                    cluster, operation, running, addresses.Value, nodeSecrets!, warnings, ct);
                if (!outcome.IsSuccess)
                    return outcome.Error!;
                phase = outcome.Value;
            }
        }

        // Стационарная запись тика (фаза + warning-и).
        var written = await journal.WritePhaseAsync(
            cluster, Op, phase, claims.InstanceId,
            warnings.Count > 0 ? string.Join("; ", warnings) : null, ct);
        return written.IsSuccess ? Result.Success() : written;
    }

    // Желаемые target-записи X (arch/24 §5 G п.2): соседи по links минус denylist
    // минус несуществующие/не-RUNNING; deny-инстанс — полная изоляция (∅).
    internal static HashSet<string> DesiredNeighbors(
        string x, IReadOnlyList<S3ReplicationLink> links, IReadOnlyList<string> denyList,
        HashSet<string> existing, HashSet<string> running)
    {
        if (denyList.Contains(x, StringComparer.Ordinal))
            return [];
        var denied = denyList.ToHashSet(StringComparer.Ordinal);
        return links
            .Where(l => string.Equals(l.A, x, StringComparison.Ordinal)
                        || string.Equals(l.B, x, StringComparison.Ordinal))
            .Select(l => string.Equals(l.A, x, StringComparison.Ordinal) ? l.B : l.A)
            .Where(y => existing.Contains(y) && running.Contains(y) && !denied.Contains(y))
            .ToHashSet(StringComparer.Ordinal);
    }

    // Deny-перезапись: entries = имена denylist × publicKeyPem из nodes/<I>/pki
    // (имени нет в pki — пропуск + warning); недоступный инстанс — transient
    // (тик повторит всем), etcd-отказ — Failed.
    private async Task<Result<bool>> PushDenyListAsync(
        string cluster, S3ClusterSnapshot snap, HashSet<string> running,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        OwnS3NodeSecrets nodeSecrets, IssuedPkiMaterial orchestrator,
        List<string> warnings, CancellationToken ct)
    {
        var denyList = snap.ParseError?.Contains("/denylist: битый JSON", StringComparison.Ordinal) == true
            ? []
            : snap.DenyList ?? [];
        var entries = new List<(string Name, string PublicKeyPem)>();
        foreach (var name in denyList.OrderBy(n => n, StringComparer.Ordinal))
        {
            var material = await pki.TryReadNodePkiAsync(cluster, name, ct);
            if (!material.IsSuccess)
            {
                // etcd/битость — не транзиент состава; journal-before-выход.
                var denyPkiFail = Fail(cluster, material.Error!, "deny-pki", ct);
                return Result<bool>.Failed(denyPkiFail.Error!);
            }
            if (material.Value is not { } issued)
            {
                warnings.Add($"deny {name}: материала pki нет — имя пропущено");
                continue;
            }

            entries.Add((name, issued.Certificate.PublicKeyPem));
        }

        var pushedAny = false;
        foreach (var x in running.OrderBy(n => n, StringComparer.Ordinal))
        {
            // DenyList-имя — перезапись не шлётся (arch/24 §5 G п.1): собственный
            // список отзывов отозванному не нужен, изоляция — записи у соседей.
            if (denyList.Contains(x, StringComparer.Ordinal))
                continue;
            if (!addresses.TryGetValue(x, out var xAddress))
            {
                warnings.Add($"deny {x}: адреса нет в portalloc — транзиент");
                continue;
            }

            var push = await nodeClient.PutDenyListAsync(
                options.AdvertisedHost ?? xAddress.Host, xAddress.S3Port, entries, orchestrator, ct);
            if (push.IsSuccess && push.Value)
            {
                pushedAny = true;
                _deny.Add(1, new KeyValuePair<string, object?>("result", "accepted"));
            }
            else if (push.IsSuccess && !push.Value)
            {
                // 403 — trust не принят: transient, тик повторит (§3.4).
                pushedAny = true; // фаза deny-pushed честно отражает попытку доведения
                warnings.Add($"deny {x}: trust не принят (403) — ретрай тиком");
                _deny.Add(1, new KeyValuePair<string, object?>("result", "transient"));
            }
            else
            {
                warnings.Add($"deny {x}: {push.Error!.Message} — транзиент");
                _deny.Add(1, new KeyValuePair<string, object?>("result", "transient"));
            }
        }

        return Result<bool>.Success(pushedAny);
    }

    // Одна структурная операция mesh (arch/24 §5 G п.2).
    private async Task<Result<string>> ApplyAsync(
        string cluster, MeshMismatch mismatch, HashSet<string> running,
        IReadOnlyDictionary<string, NodeAddress> addresses, OwnS3NodeSecrets nodeSecrets,
        List<string> warnings, CancellationToken ct)
    {
        if (mismatch.Op == MeshOp.Join)
        {
            // endpoint b — из portalloc (AdvertisedHost ?? host + s3-порт — то же
            // правило, что endpoints; §3.5 граница G).
            if (!addresses.TryGetValue(mismatch.Y, out var bAddress))
            {
                warnings.Add($"join {mismatch.X}→{mismatch.Y}: адреса нет в portalloc — транзиент");
                _join.Add(1, new KeyValuePair<string, object?>("result", "transient"));
                return Result<string>.Success("waiting-join");
            }

            var endpoint = $"http://{options.AdvertisedHost ?? bAddress.Host}:{bAddress.S3Port}";
            var join = await nodeClient.AdminJoinAsync(
                Host(addresses, mismatch.X), Port(addresses, mismatch.X), nodeSecrets, endpoint, ct);
            if (!join.IsSuccess)
            {
                _join.Add(1, new KeyValuePair<string, object?>("result", "transient"));
                return await WaitingAsync(cluster, $"join {mismatch.X}→{mismatch.Y}: {join.Error!.Message}", ct);
            }

            switch (join.Value!.Kind)
            {
                case OwnS3JoinKind.Joined:
                    _join.Add(1, new KeyValuePair<string, object?>("result", "accepted"));
                    return Result<string>.Success($"joined-{mismatch.X}-{mismatch.Y}");
                case OwnS3JoinKind.Conflict:
                    // Имя числится с другим ключом → delete конфликтной стороны,
                    // повтор следующим тиком (§3.5).
                    var deleted = await nodeClient.DeleteTargetAsync(
                        Host(addresses, mismatch.X), Port(addresses, mismatch.X), nodeSecrets, mismatch.Y, ct);
                    _join.Add(1, new KeyValuePair<string, object?>("result", "conflict"));
                    _delete.Add(1, new KeyValuePair<string, object?>("result", deleted.IsSuccess && deleted.Value ? "cleaned" : "transient"));
                    warnings.Add($"join {mismatch.X}→{mismatch.Y}: конфликт имени — target снесён, повтор следующим тиком");
                    return Result<string>.Success("cleaned-targets");
                default: // Refused / Transient
                    _join.Add(1, new KeyValuePair<string, object?>("result", "transient"));
                    return await WaitingAsync(cluster, $"join {mismatch.X}→{mismatch.Y}: отказ/недоступен — ретрай тиком", ct);
            }
        }

        // Delete-пара: снос лишней записи на КАЖДОЙ стороне (курсор сохраняется —
        // owns3/07 §2); сторона не RUNNING — снос её записей следующим тиком.
        var cleaned = 0;
        foreach (var (side, name) in new[] { (mismatch.X, mismatch.Y), (mismatch.Y, mismatch.X) })
        {
            if (!running.Contains(side) || !addresses.ContainsKey(side))
                continue;
            var removed = await nodeClient.DeleteTargetAsync(
                Host(addresses, side), Port(addresses, side), nodeSecrets, name, ct);
            if (removed.IsSuccess && removed.Value)
                cleaned++;
            else if (!removed.IsSuccess)
                warnings.Add($"delete-target {side}/{name}: {removed.Error!.Message} — транзиент");
        }

        _delete.Add(1, new KeyValuePair<string, object?>("result", cleaned > 0 ? "cleaned" : "transient"));
        return Result<string>.Success("cleaned-targets");
    }

    // Диагностический skip (профиль/0 RUNNING): Success, journal-фаза.
    private async Task<Result> SkipAsync(string cluster, string? warning, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, "idle", claims.InstanceId, warning, ct);
        return written.IsSuccess ? Result.Success() : written;
    }

    // waiting-фаза (transient операции): Success — тик продолжит ретраем.
    private async Task<Result<string>> WaitingAsync(string cluster, string warning, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, "waiting-join", claims.InstanceId, warning, ct);
        return written.IsSuccess
            ? Result<string>.Success("waiting-join")
            : Result<string>.Failed(written.Error!);
    }

    private Result Fail(string cluster, Exception error, string phase, CancellationToken ct)
    {
        journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    // Advertised host пробы/подключения (как endpoints — AdvertisedHost ?? host).
    private string Host(IReadOnlyDictionary<string, NodeAddress> addresses, string node)
        => addresses.TryGetValue(node, out var addr)
            ? options.AdvertisedHost ?? addr.Host
            : node;

    private int Port(IReadOnlyDictionary<string, NodeAddress> addresses, string node)
        => addresses.TryGetValue(node, out var addr) ? addr.S3Port : 9000;

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        try
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                S3ProcessCommon.ParsePortAlloc(kv.Value));
        }
        catch (JsonException ex)
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(ex);
        }
    }

    internal enum MeshOp
    {
        Join,
        Delete,
    }

    // Расхождение состава: недостающая пара (Join) или лишняя запись (Delete).
    internal sealed record MeshMismatch(MeshOp Op, string X, string Y);
}
