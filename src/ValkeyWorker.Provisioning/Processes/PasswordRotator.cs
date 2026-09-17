using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// PasswordRotator (arch/21 §5 E): заявка /valkeyworker/rotations/&lt;C&gt;
/// ({"role":"app"|"admin",…}) — окно двух паролей без рестартов:
/// E1 ACL SETUSER &lt;role&gt; &gt;NEW (оба пароля валидны);
/// E2 ОДНА txn: [compare value(&lt;role&gt;_password)==OLD][put NEW; del заявки];
/// E3 ACL SETUSER &lt;role&gt; &lt;OLD (старый пароль удалён).
/// Стейт доигрывания (фаза + OLD/NEW) — в отдельном ключе work/&lt;C&gt;/rotation:
/// журнал work/&lt;C&gt; надзор перезаписывает каждый тик, а NEW, добавленный на
/// ноду в E1, обязан доигрываться ТЕМ ЖЕ значением (свежая генерация на
/// доигрывании оставила бы на ноде валидный «осиротевший» пароль); E3
/// доигрывается по фазе e2-committed даже без заявки (краш между E2 и E3 —
/// без стейта OLD остался бы валидным навсегда). Ротация admin не трогает
/// app и наоборот. Битая заявка — мусор: del с journal.
/// </summary>
public sealed class PasswordRotator(
    IEtcdGateway gateway,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    IValkeyConnection valkey,
    Func<string>? generator = null) // генератор NEW (дефолт — канон 32 симв)
{
    private const string Op = "rotate";
    private const string PhaseE1 = "e1-added";
    private const string PhaseE2 = "e2-committed";

    // Payload стейта доигрывания (ключ work/<C>/rotation, arch/20 §3 — camelCase).
    private sealed record RotationState(
        [property: JsonPropertyName("phase")] string Phase,
        [property: JsonPropertyName("role")] string Role,
        [property: JsonPropertyName("old")] string Old,
        [property: JsonPropertyName("new")] string New,
        [property: JsonPropertyName("requested_by")] string? RequestedBy);

    public async Task<Result> TickAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var claimed = ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Стейт доигрывания живёт до E3 (переживает тик надзора — отдельный ключ).
        var stateRead = await ReadStateAsync(cluster, ct);
        if (!stateRead.IsSuccess)
            return stateRead.Error!;
        var state = stateRead.Value;

        // Заявка ротации (панель ставит txn version==0).
        var request = await ReadRequestAsync(cluster, ct);
        if (!request.IsSuccess)
            return request.Error!;
        var (role, requestedBy) = request.Value ?? ("", null);

        if (state is null)
        {
            // Заявок нет, стейта нет — пустой шаг Active-ветки.
            if (request.Value is null)
                return Result.Success();

            // Битая заявка (role не app|admin) — мусор: del с journal.
            if (role is not ("app" or "admin"))
            {
                var cleanup = await DeleteWithFailoverAsync(ProcessCommon.RotationKey(cluster), ct);
                if (!cleanup.IsSuccess)
                    return cleanup.Error!;
                return await journal.WritePhaseAsync(
                    cluster, Op, "invalid-request", claims.InstanceId, $"role={role}", ct);
            }

            var started = await StartRotationAsync(snap, cluster, role, requestedBy, ct);
            if (!started.IsSuccess)
                return started;
            state = started.Value;
        }

        // Доигрывание по стейту (в т.ч. сразу после старта).
        return await ResumeAsync(snap, cluster, state, ct);
    }

    // E1: генерация NEW, добавление на ноду, запись стейта — точка невозврата.
    private async Task<Result<RotationState>> StartRotationAsync(
        ValkeyClusterSnapshot snap, string cluster, string role, string? requestedBy, CancellationToken ct)
    {
        // Active-кластер: endpoints + admin-кред обязательны.
        if (snap.Endpoints is null || snap.AdminUser is null || snap.AdminPassword is null)
            return Result<RotationState>.Failed(new ApplicationException(
                $"rotate {cluster}: нет endpoints/admin-креда — ротация невозможна"));

        // OLD — текущее значение etcd (failover); NEW — генерация.
        var oldPasswordKey = $"/valkey/clusters/{cluster}/{role}_password";
        var oldRead = await GetWithFailoverAsync(oldPasswordKey, ct);
        if (!oldRead.IsSuccess)
            return Result<RotationState>.Failed(oldRead.Error!);
        var old = oldRead.Value?.Value
                  ?? throw new ApplicationException($"rotate {cluster}: {role}_password отсутствует");
        var newP = (generator ?? ValkeyPasswordGenerator.Generate)();

        var endpoint = ValkeyEndpointOf(snap);
        var e1 = await valkey.AclSetUserAsync(endpoint, [role, $">{newP}"], ct);
        if (!e1.IsSuccess)
            return Result<RotationState>.Failed(e1.Error!);

        // Стейт ДО E2: отказ после E1 → следующий тик доигрывает с ТЕМ ЖЕ NEW.
        var state = new RotationState(PhaseE1, role, old, newP, requestedBy);
        var saved = await WriteStateAsync(cluster, state, ct);
        if (!saved.IsSuccess)
            return Result<RotationState>.Failed(saved.Error!);

        var phase = await journal.WritePhaseAsync(cluster, Op, PhaseE1, claims.InstanceId, null, ct);
        return phase.IsSuccess
            ? Result<RotationState>.Success(state)
            : Result<RotationState>.Failed(phase.Error!);
    }

    // E2 (фаза e1-added) → E3 (фаза e2-committed) по стейту; E3 — даже без заявки.
    private async Task<Result> ResumeAsync(
        ValkeyClusterSnapshot snap, string cluster, RotationState state, CancellationToken ct)
    {
        var role = state.Role;
        var oldPasswordKey = $"/valkey/clusters/{cluster}/{role}_password";

        if (state.Phase == PhaseE1)
        {
            // Active-креды обязательны и на доигрывании.
            if (snap.AdminUser is null || snap.AdminPassword is null)
                return Result.Failed(new ApplicationException(
                    $"rotate {cluster}: нет admin-креда — доигрывание ротации невозможно"));

            // E2: ОДНА txn [compare value==OLD][put NEW; del заявки].
            var e2 = await TxnWithFailoverAsync(TxnRequest.Of(
            [
                TxnCompare.ValueEqual(oldPasswordKey, state.Old),
            ], [
                new TxnOp.Put(oldPasswordKey, state.New, null),
                new TxnOp.Delete(ProcessCommon.RotationKey(cluster), Prefix: false),
            ]), ct);
            if (!e2.IsSuccess)
                return e2.Error!;
            if (!e2.Value.Succeeded)
            {
                // OLD уже сменился (параллельная ротация) — повтор тика перечитает.
                return Result.Failed(new ApplicationException(
                    $"rotate {cluster}: {oldPasswordKey} изменился под нами — ретрай тиком"));
            }

            var committed = new RotationState(PhaseE2, role, state.Old, state.New, state.RequestedBy);
            var saved = await WriteStateAsync(cluster, committed, ct);
            if (!saved.IsSuccess)
                return saved.Error!;
            var e2Phase = await journal.WritePhaseAsync(cluster, Op, PhaseE2, claims.InstanceId, null, ct);
            if (!e2Phase.IsSuccess)
                return e2Phase;

            state = committed;
        }

        // E3: <OLD — удаление старого пароля (NEW остаётся единственным).
        var endpoint = ValkeyEndpointOf(snap);
        var e3 = await valkey.AclSetUserAsync(endpoint, [role, $"<{state.Old}"], ct);
        if (!e3.IsSuccess)
            return e3.Error!;

        // Стейт исчерпан (доигрывание завершено) + journal done.
        var cleared = await DeleteStateAsync(cluster, ct);
        if (!cleared.IsSuccess)
            return cleared.Error!;
        return await journal.WritePhaseAsync(
            cluster, Op, "done", claims.InstanceId, $"role={role} by={state.RequestedBy}", ct);
    }

    private static ValkeyEndpoint ValkeyEndpointOf(ValkeyClusterSnapshot snap)
    {
        var (host, port) = ProcessCommon.ParseEndpoint(snap.Endpoints!);
        return new ValkeyEndpoint(host, port, snap.AdminUser!, snap.AdminPassword!);
    }

    // ── стейт доигрывания (work/<C>/rotation, failover по endpoints) ──

    private static readonly JsonSerializerOptions StateJson = new()
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    private async Task<Result<RotationState?>> ReadStateAsync(string cluster, CancellationToken ct)
    {
        var read = await GetWithFailoverAsync(ProcessCommon.RotationStateKey(cluster), ct);
        if (!read.IsSuccess)
            return Result<RotationState?>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<RotationState?>.Success(null);
        try
        {
            return Result<RotationState?>.Success(JsonSerializer.Deserialize<RotationState>(kv.Value, StateJson));
        }
        catch (JsonException e)
        {
            return Result<RotationState?>.Failed(new ApplicationException(
                $"битый стейт ротации {ProcessCommon.RotationStateKey(cluster)}: {e.Message}", e));
        }
    }

    private async Task<Result> WriteStateAsync(string cluster, RotationState state, CancellationToken ct)
        => await PutWithFailoverAsync(
            ProcessCommon.RotationStateKey(cluster), JsonSerializer.Serialize(state, StateJson), ct);

    private async Task<Result> DeleteStateAsync(string cluster, CancellationToken ct)
        => await DeleteWithFailoverAsync(ProcessCommon.RotationStateKey(cluster), ct);

    // ── заявка ротации (панель) ──

    // (role, requested_by) заявки; null — заявки нет.
    private async Task<Result<(string Role, string? RequestedBy)?>> ReadRequestAsync(
        string cluster, CancellationToken ct)
    {
        var read = await GetWithFailoverAsync(ProcessCommon.RotationKey(cluster), ct);
        if (!read.IsSuccess)
            return Result<(string, string?)?>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<(string, string?)?>.Success(null);
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var role = doc.RootElement.TryGetProperty("role", out var r) && r.ValueKind == JsonValueKind.String
                ? r.GetString()
                : null;
            var by = doc.RootElement.TryGetProperty("requested_by", out var b) && b.ValueKind == JsonValueKind.String
                ? b.GetString()
                : null;
            return Result<(string, string?)?>.Success((role ?? "", by));
        }
        catch (JsonException ex)
        {
            return Result<(string, string?)?>.Failed(ex);
        }
    }

    // ── etcd-примитивы (failover по endpoints) ──

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
        => await ProvisioningProcess.GetWithFailoverAsync(gateway, endpoints, key, ct);

    private async Task<Result> PutWithFailoverAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteWithFailoverAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest req, CancellationToken ct)
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
