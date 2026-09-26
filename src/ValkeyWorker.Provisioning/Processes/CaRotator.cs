using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// CaRotator (t07, arch/21 §5 K): ротация per-cluster CA и серверного серта
/// по заявке /valkeyworker/ca_rotations/&lt;C&gt; — окно двойного доверия без
/// остановки обслуживания. Фазы: P (staging ca_next_* put-if-absent) →
/// D (ca_pem = bundle OLD+NEW — перечитавшие дискавери доверяют обоим) →
/// R (пересоздание node1 с сертом от NEW; факт-детект IsValidTar — без
/// in-memory-треков, nodes=1) → C (атомарный txn: ca_pem/ca_key ← NEW,
/// del staging, del заявки) → K4 (снапшот + done). Эксклюзивный второй шаг
/// Active-ветки: окно открыто ⇒ InProgress ⇒ надзор/конвергер/ротация
/// кредов в тике не идут (решение пользователя, spec §2.4). Ждущие исходы
/// (waiting-*) вентиль НЕ блокируют. Вызывается только держателем клэйма
/// &lt;C&gt;; состояние — только в etcd. Отказ etcd/docker между фазами —
/// Failed c last_error в journal (spec §5).
/// </summary>
public sealed class CaRotator(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    NodeTlsProvisioner tlsProvisioner,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    TimeProvider? clock = null)
{
    public const string Op = "rotate-ca";

    private const string PhaseDone = "done";
    private const string PhaseCommitted = "committed";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    /// <summary>Итог тика: NotNeeded — no-op ветки; Waiting — заявка жива,
    /// окно НЕ открыто (ветка продолжается); InProgress — окно открыто/
    /// доигрывается (вентиль блокирует C/D/E).</summary>
    public enum RotationOutcome
    {
        NotNeeded,
        Waiting,
        InProgress,
    }

    public async Task<Result<RotationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result<RotationOutcome>.Failed(new ApplicationException(
                $"rotate-ca {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // K0.1: заявка + journal.
        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result<RotationOutcome>.Failed(ticket.Error!);
        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result<RotationOutcome>.Failed(journalState.Error!);

        // K0.2: хвост после коммита (заявка снята, done не записан) —
        // идемпотентный финал K4 без мутаций staging/ноды.
        var afterCommit = journalState.Value is { Op: Op } j && j.Phase == PhaseCommitted;
        if (afterCommit && ticket.Value is null)
            return await FinishAsync(cluster, ct);

        // K0.3: окно уже открыто? — доигрывание P→D→R→C БЕЗ ждущих проверок.
        var windowOpen = await WindowOpenAsync(cluster, journalState.Value, ct);
        if (windowOpen.IsSuccess && windowOpen.Value)
        {
            // Аномалия-защита: окно открыто, а канона нет — внешняя порча;
            // Failed = ретрай тиком (самокоррекции нет).
            if (snap.CaPem is null || snap.CaKey is null)
                return Result<RotationOutcome>.Failed(new ApplicationException(
                    $"rotate-ca {cluster}: окно открыто, но ca_pem/ca_key отсутствуют — внешняя порча, ретрай тиком"));
            return await PhasesAsync(snap, ct);
        }
        if (!windowOpen.IsSuccess)
            return Result<RotationOutcome>.Failed(windowOpen.Error!);

        // K0.4: заявки нет и хвоста/окна нет — no-op ветки.
        if (ticket.Value is null)
            return Result<RotationOutcome>.Success(RotationOutcome.NotNeeded);

        // K0.5: ждущие причины (исход Waiting, БЕЗ мутаций; journal-запись фазы).
        if (snap.Endpoints is null || snap.AdminPassword is null || snap.AppPassword is null
            || snap.CaPem is null || snap.CaKey is null)
            return await WaitAsync(cluster, "waiting-cluster", ct);
        var passwordAlive = await PasswordRotationAliveAsync(cluster, journalState.Value, ct);
        if (!passwordAlive.IsSuccess)
            return Result<RotationOutcome>.Failed(passwordAlive.Error!);
        if (passwordAlive.Value)
            return await WaitAsync(cluster, "waiting-password-rotation", ct);

        // K0.6: перечитка config — TO_REMOVE: демонтаж B всё почистит.
        var removed = await ConfigRemovedAsync(cluster, ct);
        if (!removed.IsSuccess)
            return Result<RotationOutcome>.Failed(removed.Error!);
        if (removed.Value)
            return await AbortAsync(cluster);

        return await PhasesAsync(snap, ct);
    }

    // Фазы P→D→(R→C→K4 — Task 3): доигрывание по факту.
    private async Task<Result<RotationOutcome>> PhasesAsync(
        ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // P: staging НОВОЙ CA — одна генерация на жизнь ротации.
        var staging = await EnsureStagingAsync(cluster, ct);
        if (!staging.IsSuccess)
            return Result<RotationOutcome>.Failed(staging.Error!);
        var (nextKey, nextPem) = staging.Value;

        // D: bundle OLD+NEW в точке дискавери ДО замены серта ноды.
        var caPem = snap.CaPem!;
        if (!caPem.Contains(nextPem))
        {
            var markedD = await journal.WritePhaseAsync(cluster, Op, "phase-d", claims.InstanceId, null, ct);
            if (!markedD.IsSuccess)
                return Result<RotationOutcome>.Failed(markedD.Error!);
            var bundlePut = await TxnAsync(TxnRequest.Of(
                [TxnCompare.ValueEqual(CaPemKey(cluster), caPem)],
                [new TxnOp.Put(CaPemKey(cluster), caPem + "\n" + nextPem, null)]), ct);
            if (!bundlePut.IsSuccess)
                return await FailAsync(cluster, bundlePut.Error!, "phase-d", ct);
            if (!bundlePut.Value.Succeeded)
                return await FailAsync(cluster, new ApplicationException(
                    $"rotate-ca {cluster}: ca_pem изменился с момента чтения (внешняя запись?) — ретрай тиком"), "phase-d", ct);
        }

        // R→C→K4: Task 3 (ReplayNodeCommitAsync); до его появления — окно живо.
        return await ReplayNodeCommitAsync(snap, nextPem, nextKey, ct);
    }

    // R: пересоздание node1 с перевыпуском серта от NEW (лечение ЛЮБОГО
    // состояния ноды; преф-чека живости нет — nodes=1, persistence off,
    // кеш восполним; spec §5 R) → C: атомарный коммит → K4: финал.
    private async Task<Result<RotationOutcome>> ReplayNodeCommitAsync(
        ValkeyClusterSnapshot snap, string nextPem, string nextKey, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // R.1: факт-детект — валидный NEW-серт в TLS-volume ⇒ R завершён.
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<RotationOutcome>.Failed(addresses.Error!);
        if (!addresses.Value.TryGetValue("node1", out var address))
            return Result<RotationOutcome>.Failed(new ApplicationException(
                $"rotate-ca {cluster}: node1 не закреплён в portalloc"));
        var archive = await driver.GetTlsArchiveAsync(cluster, address.Host, options.NodeImage, ct);
        if (!archive.IsSuccess)
            return await FailAsync(cluster, archive.Error!, "phase-r", ct);
        if (archive.Value is { } tar && NodeTlsProvisioner.IsValidTar(tar,
                options.AdvertisedClientHost ?? address.Host, nextPem, _clock))
            return await CommitAsync(cluster, nextPem, nextKey, ct);

        // R.2: гонка TO_REMOVE перед пересозданием — abort.
        var removed = await ConfigRemovedAsync(cluster, ct);
        if (!removed.IsSuccess)
            return Result<RotationOutcome>.Failed(removed.Error!);
        if (removed.Value)
            return await AbortAsync(cluster);

        // R.3: серт/ca.pem volume = NEW (НЕ bundle: --tls-auth-clients no),
        // journal phase-r, RemoveNode → EnsureNode (порт/лимиты прежние —
        // порт из portalloc, лимиты из декларации resources ноды).
        var tls = await tlsProvisioner.EnsureNodeTlsAsync(
            cluster, "node1", address.Host, options.AdvertisedClientHost ?? address.Host,
            nextPem, nextKey, ct);
        if (!tls.IsSuccess)
            return await FailAsync(cluster, tls.Error!, "phase-r", ct);
        var markedR = await journal.WritePhaseAsync(cluster, Op, "phase-r", claims.InstanceId, null, ct);
        if (!markedR.IsSuccess)
            return Result<RotationOutcome>.Failed(markedR.Error!);

        var nodeSnap = snap.Nodes.GetValueOrDefault("node1");
        var limits = ProcessCommon.ParseResources(nodeSnap?.Resources);
        var args = NodeArgsBuilder.Build(
            snap.Config?.MaxmemoryBytes ?? 0, snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
            snap.AdminPassword!, snap.AppPassword!);
        var removedNode = await driver.RemoveNodeAsync(cluster, "node1", ct);
        if (!removedNode.IsSuccess)
            return await FailAsync(cluster, removedNode.Error!, "phase-r/node1", ct);
        var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
            cluster, "node1", address.Host, address.ClientPort, options.NodeImage, args,
            limits?.Cpu, limits?.MemBytes,
            TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)), ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "phase-r/node1", ct);
        var provisioning = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "PROVISIONING", ct);
        if (!provisioning.IsSuccess)
            return Result<RotationOutcome>.Failed(provisioning.Error!);
        var markedRNode = await journal.WritePhaseAsync(
            cluster, Op, "phase-r/node1", claims.InstanceId, null, ct);
        if (!markedRNode.IsSuccess)
            return Result<RotationOutcome>.Failed(markedRNode.Error!);

        // R.4: AwaitBoot — PING по TLS с якорем nextPem (серт уже NEW);
        // бюджет NodeBootSec, цикл 100 мс (порт TlsMigrator.AwaitBootAsync).
        var boot = await AwaitBootAsync(address, snap.AdminPassword!, nextPem, ct);
        if (!boot.IsSuccess)
            return await FailAsync(cluster, boot.Error!, "boot-timeout", ct);
        var running = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "RUNNING", ct);
        if (!running.IsSuccess)
            return Result<RotationOutcome>.Failed(running.Error!);

        return await CommitAsync(cluster, nextPem, nextKey, ct);
    }

    // C: атомарный коммит ОДНОЙ txn (compare по staging-ключу — гонка
    // параллельной ротации закрыта) → K4.
    private async Task<Result<RotationOutcome>> CommitAsync(
        string cluster, string nextPem, string nextKey, CancellationToken ct)
    {
        var markedC = await journal.WritePhaseAsync(cluster, Op, PhaseCommitted, claims.InstanceId, null, ct);
        if (!markedC.IsSuccess)
            return Result<RotationOutcome>.Failed(markedC.Error!);
        var commit = await TxnAsync(TxnRequest.Of(
            [TxnCompare.ValueEqual(NextKeyKey(cluster), nextKey)],
            [
                new TxnOp.Put(CaPemKey(cluster), nextPem, null),
                new TxnOp.Put(CaKeyKey(cluster), nextKey, null),
                new TxnOp.Delete(NextPemKey(cluster), Prefix: false),
                new TxnOp.Delete(NextKeyKey(cluster), Prefix: false),
                new TxnOp.Delete(TicketKey(cluster), Prefix: false),
            ]), ct);
        if (!commit.IsSuccess)
            return await FailAsync(cluster, commit.Error!, PhaseCommitted, ct);
        if (!commit.Value.Succeeded)
            return await FailAsync(cluster, new ApplicationException(
                $"rotate-ca {cluster}: ca_next_key изменился с момента чтения (параллельная ротация?) — ретрай тиком"), PhaseCommitted, ct);

        return await FinishAsync(cluster, ct);
    }

    // PING по TLS с якорем NEW: транзиент-толерантный цикл в бюджете NodeBootSec.
    private async Task<Result> AwaitBootAsync(
        NodeAddress address, string adminPassword, string nextPem, CancellationToken ct)
    {
        var endpoint = new ValkeyEndpoint(
            options.AdvertisedClientHost ?? address.Host, address.ClientPort,
            "admin", adminPassword, nextPem);
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
                    $"rotate-ca нода не отвечает по TLS (якорь NEW) {budget.TotalSeconds:F0} c " +
                    $"({ping.Error!.Message})"));
            await Task.Delay(100, ct);
        }
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
            ProcessCommon.ParsePortAlloc(kv.Value));
    }

    // Окно открыто: staging есть ИЛИ journal rotate-ca вне {done, waiting-*}.
    private async Task<Result<bool>> WindowOpenAsync(string cluster, WorkState? journalState, CancellationToken ct)
    {
        var nextKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!nextKey.IsSuccess)
            return Result<bool>.Failed(nextKey.Error!);
        var nextPem = await GetAsync(NextPemKey(cluster), ct);
        if (!nextPem.IsSuccess)
            return Result<bool>.Failed(nextPem.Error!);
        if (nextKey.Value is not null || nextPem.Value is not null)
            return Result<bool>.Success(true);
        return Result<bool>.Success(journalState is { Op: Op } j
            && j.Phase != PhaseDone
            && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal));
    }

    // Живая ротация креда (spec §5 K0.5): заявка rotations ИЛИ стейт
    // work/<C>/rotation с фазой e1-pending|e1-added|e2-committed.
    private async Task<Result<bool>> PasswordRotationAliveAsync(string cluster, WorkState? journalState, CancellationToken ct)
    {
        if (journalState is { Op: "rotate" } r && r.Phase != PhaseDone)
            return Result<bool>.Success(true);
        var passwordTicket = await GetAsync($"/valkeyworker/rotations/{cluster}", ct);
        if (!passwordTicket.IsSuccess)
            return Result<bool>.Failed(passwordTicket.Error!);
        if (passwordTicket.Value is not null)
            return Result<bool>.Success(true);
        var state = await GetAsync(ProcessCommon.RotationStateKey(cluster), ct);
        if (!state.IsSuccess)
            return Result<bool>.Failed(state.Error!);
        if (state.Value is not { } kv)
            return Result<bool>.Success(false);
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var phase = doc.RootElement.TryGetProperty("phase", out var p)
                        && p.ValueKind == JsonValueKind.String ? p.GetString() : null;
            return Result<bool>.Success(phase is "e1-pending" or "e1-added" or "e2-committed");
        }
        catch (JsonException)
        {
            return Result<bool>.Success(false); // битый стейт — не «живая ротация»
        }
    }

    // Фаза P: чтение staging; отсутствующая — генерация + txn put-if-absent;
    // проигрыш compare — re-read (чужая staging валидна, образец kafka).
    // Отказы (journal/txn) — FailStagingAsync: last_error в journal (spec §5).
    private async Task<Result<(string Key, string Pem)>> EnsureStagingAsync(string cluster, CancellationToken ct)
    {
        var key = await GetAsync(NextKeyKey(cluster), ct);
        if (!key.IsSuccess)
            return Result<(string, string)>.Failed(key.Error!);
        var pem = await GetAsync(NextPemKey(cluster), ct);
        if (!pem.IsSuccess)
            return Result<(string, string)>.Failed(pem.Error!);
        if (key.Value is { } existingKey && pem.Value is { } existingPem)
            return Result<(string, string)>.Success((existingKey.Value, existingPem.Value));

        var markedP = await journal.WritePhaseAsync(cluster, Op, "phase-p", claims.InstanceId, null, ct);
        if (!markedP.IsSuccess)
            return Result<(string, string)>.Failed(markedP.Error!);

        var generated = ValkeyPki.GenerateCa(cluster);
        var txn = await TxnAsync(TxnRequest.Of(
            [TxnCompare.NotExists(NextKeyKey(cluster)), TxnCompare.NotExists(NextPemKey(cluster))],
            [
                new TxnOp.Put(NextKeyKey(cluster), generated.CaKeyPem, null),
                new TxnOp.Put(NextPemKey(cluster), generated.CaPem, null),
            ]), ct);
        if (!txn.IsSuccess)
            return await FailStagingAsync(cluster, txn.Error!);

        var finalKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!finalKey.IsSuccess || finalKey.Value is null)
            return await FailStagingAsync(cluster, finalKey.Error
                ?? new ApplicationException($"rotate-ca {cluster}: ca_next_key не читается после txn"));
        var finalPem = await GetAsync(NextPemKey(cluster), ct);
        if (!finalPem.IsSuccess || finalPem.Value is null)
            return await FailStagingAsync(cluster, finalPem.Error
                ?? new ApplicationException($"rotate-ca {cluster}: ca_next_pem не читается после txn"));
        return Result<(string, string)>.Success((finalKey.Value!.Value, finalPem.Value!.Value));
    }

    // FailAsync для фазы P (стадия staging): last_error в journal, затем Failed.
    private async Task<Result<(string Key, string Pem)>> FailStagingAsync(string cluster, Exception error)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, "phase-p", claims.InstanceId, error.Message, CancellationToken.None);
        return written.IsSuccess
            ? Result<(string, string)>.Failed(error)
            : Result<(string, string)>.Failed(written.Error!);
    }

    // Финал K4: снапшот «после» + journal done (идемпотентно).
    private async Task<Result<RotationOutcome>> FinishAsync(string cluster, CancellationToken ct)
    {
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result<RotationOutcome>.Failed(after.Error!);
        }
        var done = await journal.WritePhaseAsync(cluster, Op, PhaseDone, claims.InstanceId, null, ct);
        return done.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.InProgress)
            : Result<RotationOutcome>.Failed(done.Error!);
    }

    // Ждущий исход: journal-запись + Waiting (без мутаций; заявка жива).
    private async Task<Result<RotationOutcome>> WaitAsync(string cluster, string phase, CancellationToken ct)
    {
        var waiting = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return waiting.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.Waiting)
            : Result<RotationOutcome>.Failed(waiting.Error!);
    }

    // config.state=TO_REMOVE — безопасная остановка (порт TlsMigrator.AbortAsync).
    private async Task<Result<RotationOutcome>> AbortAsync(string cluster)
    {
        var aborted = await journal.WritePhaseAsync(
            cluster, Op, "aborted-state-changed", claims.InstanceId, null, CancellationToken.None);
        return aborted.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.Waiting)
            : Result<RotationOutcome>.Failed(aborted.Error!);
    }

    private async Task<Result<bool>> ConfigRemovedAsync(string cluster, CancellationToken ct)
    {
        var read = await GetAsync(ProcessCommon.ConfigKey(cluster), ct);
        if (!read.IsSuccess)
            return Result<bool>.Failed(read.Error!);
        return Result<bool>.Success(read.Value is { } kv && TryReadState(kv.Value) is "TO_REMOVE");
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

    // Отказ фазы — last_error в journal (spec §5 «Отказ etcd/docker между
    // фазами: Failed c last_error в journal»), затем Failed (ретрай тиком).
    private async Task<Result<RotationOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return written.IsSuccess
            ? Result<RotationOutcome>.Failed(error)
            : Result<RotationOutcome>.Failed(written.Error!);
    }

    private static string TicketKey(string cluster) => $"/valkeyworker/ca_rotations/{cluster}";

    private static string NextKeyKey(string cluster) => $"/valkey/clusters/{cluster}/ca_next_key";

    private static string NextPemKey(string cluster) => $"/valkey/clusters/{cluster}/ca_next_pem";

    private static string CaPemKey(string cluster) => $"/valkey/clusters/{cluster}/ca_pem";

    private static string CaKeyKey(string cluster) => $"/valkey/clusters/{cluster}/ca_key";

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
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

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
