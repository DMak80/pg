using System.Text.Json;
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
/// Journal-фазы между E1–E3: отказ → повтор тика доигрывает. Ротация admin не
/// трогает app и наоборот. Битая заявка — мусор: del с journal.
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

    public async Task<Result> TickAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var claimed = ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Заявка ротации (панель ставит txn version==0).
        var request = await ReadRequestAsync(cluster, ct);
        if (!request.IsSuccess)
            return request.Error!;
        if (request.Value is not { } requestPair)
            return Result.Success(); // заявок нет — пустой шаг Active-ветки
        var role = requestPair.Role;
        var requestedBy = requestPair.RequestedBy;

        // Битая заявка (role не app|admin) — мусор: del с journal.
        if (role is not ("app" or "admin"))
        {
            var cleanup = await DeleteRequestAsync(cluster, ct);
            if (!cleanup.IsSuccess)
                return cleanup.Error!;
            return await journal.WritePhaseAsync(
                cluster, Op, "invalid-request", claims.InstanceId, $"role={role}", ct);
        }

        // Active-кластер: endpoints + admin-кред обязательны.
        if (snap.Endpoints is null || snap.AdminUser is null || snap.AdminPassword is null)
            return Result.Failed(new ApplicationException(
                $"rotate {cluster}: нет endpoints/admin-креда — ротация невозможна"));

        var first = snap.Endpoints.Split(',', StringSplitOptions.TrimEntries)[0];
        var separator = first.LastIndexOf(':');
        var endpoint = new ValkeyEndpoint(
            first[..separator], int.Parse(first[(separator + 1)..]),
            snap.AdminUser, snap.AdminPassword);

        // OLD — текущее значение etcd; NEW — генерация.
        var oldPasswordKey = $"/valkey/clusters/{cluster}/{role}_password";
        var oldRead = await gateway.GetAsync(gatewayEndpoint(), oldPasswordKey, ct);
        if (!oldRead.IsSuccess)
            return oldRead.Error!;
        var oldPassword = oldRead.Value?.Value
            ?? throw new ApplicationException($"rotate {cluster}: {role}_password отсутствует");
        var newPassword = (generator ?? ValkeyPasswordGenerator.Generate)();

        // Фаза из журнала: отказ между E1–E3 → повтор тика доигрывает.
        var state = await journal.ReadAsync(cluster, ct);
        var phase = state.Value?.Op == Op ? state.Value?.Phase : null;

        // E1: >NEW — оба пароля валидны, клиенты работают со OLD.
        if (phase is null or "started")
        {
            var e1 = await valkey.AclSetUserAsync(endpoint, [role, $">{newPassword}"], ct);
            if (!e1.IsSuccess)
                return e1.Error!;
            var e1Phase = await journal.WritePhaseAsync(
                cluster, Op, "e1-added", claims.InstanceId, null, ct);
            if (!e1Phase.IsSuccess)
                return e1Phase;
        }

        // E2: ОДНА txn [compare value==OLD][put NEW; del заявки].
        var e2 = await gateway.TxnAsync(gatewayEndpoint(), TxnRequest.Of(
        [
            TxnCompare.ValueEqual(oldPasswordKey, oldPassword),
        ], [
            new TxnOp.Put(oldPasswordKey, newPassword, null),
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

        var e2Phase = await journal.WritePhaseAsync(
            cluster, Op, "e2-committed", claims.InstanceId, null, ct);
        if (!e2Phase.IsSuccess)
            return e2Phase;

        // E3: <OLD — удаление старого пароля (NEW остаётся единственным).
        var e3 = await valkey.AclSetUserAsync(endpoint, [role, $"<{oldPassword}"], ct);
        if (!e3.IsSuccess)
            return e3.Error!;

        return await journal.WritePhaseAsync(
            cluster, Op, "done", claims.InstanceId, $"role={role} by={requestedBy}", ct);

        string gatewayEndpoint() => endpoints[0];
    }

    // (role, requested_by) заявки; null — заявки нет.
    private async Task<Result<(string Role, string? RequestedBy)?>> ReadRequestAsync(
        string cluster, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var read = await gateway.GetAsync(endpoint, ProcessCommon.RotationKey(cluster), ct);
            if (!read.IsSuccess)
            {
                last = read;
                continue;
            }

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

        return Result<(string, string?)?>.Failed(last!.Error!);
    }

    private async Task<Result> DeleteRequestAsync(string cluster, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, ProcessCommon.RotationKey(cluster), prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last!;
    }
}
