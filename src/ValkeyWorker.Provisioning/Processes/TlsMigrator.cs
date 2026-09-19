using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// TlsMigrator (t06, arch/21 §5 T): авто-миграция plain→TLS — первый шаг
/// Active-ветки (до надзора C; образец kafka M). Детект — чистая функция
/// NeedsMigration (ca_pem/ca_key в etcd отсутствуют ИЛИ args живого контейнера
/// без --tls-port). Фазы: T0 journal-before-manipulations + снапшот «до»;
/// T1 ensure CA+кредов (txn put-if-absent) + перечитка config (гонка
/// TO_REMOVE — abort); T2 пересоздание контейнера с каноническими TLS-args
/// (серт в volume, порт/лимиты те же — portalloc не меняется); T3 PING по TLS
/// (бюджет NodeBootSec, цикл 100 мс) → state=RUNNING, снапшот «после», journal
/// done. Идемпотентность по факту: канонический кластер → NotNeeded;
/// отработавший миграцию кластер неотличим от поднятого канонически.
/// Вызывается только держателем клэйма &lt;C&gt;; nodes=1 (node1).
/// </summary>
public sealed class TlsMigrator(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IClusterSecretEnsurer secrets,
    NodeTlsProvisioner tlsProvisioner,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    TimeProvider? clock = null)
{
    public const string Op = "migrate-tls";

    private const string PhaseDone = "done";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    /// <summary>Итог тика миграции: NotNeeded — кластер уже канонический.</summary>
    public enum MigrationOutcome
    {
        NotNeeded,
        InProgress,
    }

    /// <summary>
    /// Чистый детект премиграционного кластера (arch/21 §5 T): ca_pem/ca_key в
    /// etcd отсутствуют ИЛИ живой контейнер собран без --tls-port (старый канон).
    /// </summary>
    public static bool NeedsMigration(ValkeyClusterSnapshot snap, IReadOnlyList<string>? liveNodeArgs)
        => snap.CaPem is null || snap.CaKey is null || liveNodeArgs?.Contains("--tls-port") != true;

    public async Task<Result<MigrationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/21 §6).
        if (!claims.IsMine(cluster))
            return Result<MigrationOutcome>.Failed(new ApplicationException(
                $"migrate-tls {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Детект: etcd-ключи + args живого контейнера (nodes=1 — node1; best-effort
        // инспект: контейнера нет → args нет — детект по etcd-ключам остаётся).
        var liveArgsResult = await ReadLiveNodeArgsAsync(cluster, snap, ct);
        if (!liveArgsResult.IsSuccess)
            return Result<MigrationOutcome>.Failed(liveArgsResult.Error!);
        var liveArgs = liveArgsResult.Value;
        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result<MigrationOutcome>.Failed(journalState.Error!);

        // NeedsMigration=false возможно уже после T2 (args TLS), но T3 (готовность)
        // мог не завершиться. Канонический итог фиксирует journal done (или его
        // отсутствие — кластер сразу поднят новым кодом): только он даёт NotNeeded.
        var done = !NeedsMigration(snap, liveArgs)
                   && (journalState.Value is null
                       || journalState.Value.Op != Op
                       || journalState.Value.Phase == PhaseDone);
        if (done)
            return Result<MigrationOutcome>.Success(MigrationOutcome.NotNeeded);

        // T0: journal-before-manipulations + снапшот «до».
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<MigrationOutcome>.Failed(started.Error!);
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return await FailAsync(cluster, before.Error!, "snapshot-before", ct);
        }

        // T1: ensure CA (+ креды добором той же txn) + re-read.
        var ensured = await secrets.EnsureAsync(cluster, ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "ensured-ca", ct);
        snap = snap with
        {
            AdminPassword = snap.AdminPassword ?? ensured.Value.AdminPassword,
            AppPassword = snap.AppPassword ?? ensured.Value.AppPassword,
            CaPem = ensured.Value.CaPem,
            CaKey = ensured.Value.CaKey,
        };
        var ensuredPhase = await journal.WritePhaseAsync(cluster, Op, "ensured-ca", claims.InstanceId, null, ct);
        if (!ensuredPhase.IsSuccess)
            return Result<MigrationOutcome>.Failed(ensuredPhase.Error!);

        // Гонка «панель пишет TO_REMOVE посреди миграции» — перечитывание перед T2.
        if (await ConfigRemovedAsync(cluster, ct))
            return await AbortAsync(cluster);

        // T2: пересоздание контейнера с каноническими TLS-args (серты в volume,
        // порт/лимиты те же — portalloc не меняется); уже-TLS контейнер не трогаем.
        var nodeArgs = await driver.NodeArgsAsync(cluster, "node1", ct);
        if (!nodeArgs.IsSuccess)
            return await FailAsync(cluster, nodeArgs.Error!, "recreated", ct);
        if (nodeArgs.Value is not { } args || !args.Contains("--tls-port"))
        {
            var recreated = await RecreateNodeAsync(snap, ensured.Value, ct);
            if (!recreated.IsSuccess)
                return await FailAsync(cluster, recreated.Error!, "recreated", ct);
        }

        // T3: PING по TLS → state=RUNNING; бюджет NodeBootSec (цикл 100 мс).
        var boot = await AwaitBootAsync(snap, ensured.Value, ct);
        if (!boot.IsSuccess)
            return await FailAsync(cluster, boot.Error!, "boot-timeout", ct);
        var running = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "RUNNING", ct);
        if (!running.IsSuccess)
            return Result<MigrationOutcome>.Failed(running.Error!);

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return await FailAsync(cluster, after.Error!, "snapshot-after", ct);
        }

        // journal done ⇒ следующий тик: NeedsMigration=false → NotNeeded.
        var finished = await journal.WritePhaseAsync(cluster, Op, PhaseDone, claims.InstanceId, null, ct);
        return finished.IsSuccess
            ? Result<MigrationOutcome>.Success(MigrationOutcome.InProgress)
            : Result<MigrationOutcome>.Failed(finished.Error!);
    }

    // T2: RecreateNodeAsync — RemoveNode → EnsureNodeTls → EnsureNode с
    // каноническими TLS-args; лимиты — из декларации; порт — из portalloc.
    private async Task<Result> RecreateNodeAsync(
        ValkeyClusterSnapshot snap, ValkeyCredentials creds, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return addresses.Error!;
        if (!addresses.Value.TryGetValue("node1", out var address))
            return Result.Failed(new ApplicationException(
                $"migrate-tls {cluster}: node1 не закреплён в portalloc"));

        var nodeSnap = snap.Nodes.GetValueOrDefault("node1");
        var limits = ProcessCommon.ParseResources(nodeSnap?.Resources);
        var args = NodeArgsBuilder.Build(
            snap.Config?.MaxmemoryBytes ?? 0, snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
            creds.AdminPassword, creds.AppPassword);

        var removed = await driver.RemoveNodeAsync(cluster, "node1", ct);
        if (!removed.IsSuccess)
            return removed;

        var tls = await tlsProvisioner.EnsureNodeTlsAsync(
            cluster, "node1", address.Host, options.AdvertisedClientHost ?? address.Host,
            creds.CaPem, creds.CaKey, ct);
        if (!tls.IsSuccess)
            return tls;

        var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
            cluster, "node1", address.Host, address.ClientPort, options.NodeImage, args,
            limits?.Cpu, limits?.MemBytes,
            TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)), ct);
        if (!ensured.IsSuccess)
            return ensured;

        var state = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "PROVISIONING", ct);
        if (!state.IsSuccess)
            return state;

        var phase = await journal.WritePhaseAsync(cluster, Op, "recreated", claims.InstanceId, null, ct);
        return phase.IsSuccess ? Result.Success() : phase;
    }

    // T3: PING по TLS admin-кредом — транзиент-толерантный цикл в бюджете
    // NodeBootSec (образец V4/AwaitBootAsync).
    private async Task<Result> AwaitBootAsync(
        ValkeyClusterSnapshot snap, ValkeyCredentials creds, CancellationToken ct)
    {
        var addresses = await ReadPortAllocAsync(snap.Cluster, ct);
        if (!addresses.IsSuccess)
            return addresses.Error!;
        if (!addresses.Value.TryGetValue("node1", out var address))
            return Result.Failed(new ApplicationException(
                $"migrate-tls {snap.Cluster}: node1 не закреплён в portalloc"));

        var endpoint = new ValkeyEndpoint(
            options.AdvertisedClientHost ?? address.Host, address.ClientPort,
            "admin", creds.AdminPassword, creds.CaPem);
        var startedAt = _clock.GetUtcNow();
        var budget = TimeSpan.FromSeconds(options.NodeBootSec);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var ping = await valkey.PingAsync(endpoint, ct);
            if (ping.IsSuccess)
                return Result.Success();
            if (_clock.GetUtcNow() - startedAt > budget)
                return Result.Failed(new TimeoutException(
                    $"migrate-tls {snap.Cluster}/node1: нода не отвечает по TLS {budget.TotalSeconds:F0} c " +
                    $"({ping.Error!.Message})"));
            await Task.Delay(100, ct);
        }
    }

    // args живой ноды (детект контейнера старого канона); null — объекта нет.
    private async Task<Result<IReadOnlyList<string>?>> ReadLiveNodeArgsAsync(
        string cluster, ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        if (snap.Nodes.Count == 0)
            return Result<IReadOnlyList<string>?>.Success(null);
        return await driver.NodeArgsAsync(cluster, "node1", ct);
    }

    // config.state=TO_REMOVE посреди миграции — безопасная остановка (образец V3).
    private async Task<bool> ConfigRemovedAsync(string cluster, CancellationToken ct)
    {
        var read = await GetWithFailoverAsync(ProcessCommon.ConfigKey(cluster), ct);
        if (!read.IsSuccess)
            return false; // транспортный сбой — перечитку сделает следующий тик
        return read.Value is { } kv
               && TryReadState(kv.Value) is "TO_REMOVE";
    }

    private static string? TryReadState(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("state", out var state)
                   && state.ValueKind == JsonValueKind.String
                ? state.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null; // битый config — не TO_REMOVE (парсер уже отметил ошибку)
        }
    }

    private async Task<Result<MigrationOutcome>> AbortAsync(string cluster)
    {
        var aborted = await journal.WritePhaseAsync(
            cluster, Op, "aborted-state-changed", claims.InstanceId, null, CancellationToken.None);
        return aborted.IsSuccess
            ? Result<MigrationOutcome>.Success(MigrationOutcome.InProgress)
            : Result<MigrationOutcome>.Failed(aborted.Error!);
    }

    private async Task<Result<MigrationOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return written.IsSuccess
            ? Result<MigrationOutcome>.Failed(error)
            : Result<MigrationOutcome>.Failed(written.Error!);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetWithFailoverAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
            ProcessCommon.ParsePortAlloc(kv.Value));
    }

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
