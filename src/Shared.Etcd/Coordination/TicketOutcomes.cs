using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Аудит заявки из payload (t10): возраст — из requested_unix (канон §9.8-протокола,
// переживает рестарты), не из mod_revision etcd.
public sealed record TicketRequestAudit(long RequestedUnix, string? RequestedBy);

// Исходы заявок воркеров (t10, arch/15 §4 / arch/20 §3): пишет только воркер,
// перезаписывается каждым новым исходом, чистится демонтажом X2.
public static class TicketOutcomes
{
    public const string KindPasswordApp = "password-app";
    public const string KindPasswordAdmin = "password-admin";
    public const string KindCa = "ca";
    public const string KindRebalance = "rebalance";

    public static string Key(string workerPrefix, string cluster)
        => $"{workerPrefix}/ticket_outcomes/{cluster}";

    // null — битый JSON или нет requested_unix: возраст считается нулём,
    // снятие НЕ выполняется (параноидальный отказ от снятия).
    public static TicketRequestAudit? ParseAudit(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("requested_unix", out var unix)
                || unix.ValueKind != JsonValueKind.Number
                || !unix.TryGetInt64(out var requested))
                return null;
            var by = root.TryGetProperty("requested_by", out var b)
                     && b.ValueKind == JsonValueKind.String ? b.GetString() : null;
            return new TicketRequestAudit(requested, by);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string OutcomeJson(
        string kind, string outcome, string? reason,
        long requestedUnix, string? requestedBy, long finishedUnix)
        => JsonSerializer.Serialize(new TicketOutcomeValue(kind, outcome, reason, requestedUnix, requestedBy, finishedUnix), Json);

    private sealed record TicketOutcomeValue(
        [property: JsonPropertyName("kind")] string Kind,
        [property: JsonPropertyName("outcome")] string Outcome,
        [property: JsonPropertyName("reason")] string? Reason,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string? RequestedBy,
        [property: JsonPropertyName("finished_unix")] long FinishedUnix);
}

// Возрастная экспирация не-начатой заявки (t10, arch/16 §5 / arch/21 §5).
// Гвард экспирации (spec §3.1) — тройной предикат «не начато»:
//  1) заявка жива (ticketPayload != null — за вызывающим);
//  2) окно ротации не открыто (staging ca_next_* отсутствует — за вызывающим:
//     вызов ТОЛЬКО из дооконной ветки процесса);
//  3) mutationLive=false — журнал/стейт/прогресс процесса не в незавершённой
//     мутационной фазе (журнал роли H / rotate-ca K / стейт E / прогресс
//     balance I; сюда же вызывающий передаёт true при живом окне чужой
//     ротации — точка принципиально не экспирационная).
// Waiting-точка процесса сама по себе «не начато» НЕ доказывает.
public sealed class TicketExpirator(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> TryExpireAsync(
        WorkJournal journal, string cluster, string op, string instanceId,
        string outcomeKey, string kind, string ticketKey, string ticketPayload,
        string waitingReason, int timeoutSec, long nowUnix, bool mutationLive,
        CancellationToken ct)
    {
        // Предикат 3 гварда: незавершённая мутация жива — снятие запрещено
        // при любом возрасте (spec §3.1/§5).
        if (mutationLive)
            return Result<bool>.Success(false);
        var audit = TicketOutcomes.ParseAudit(ticketPayload);
        if (audit is null)
            return Result<bool>.Success(false); // битый payload — NOT expiry
        var age = nowUnix - audit.RequestedUnix;
        if (age <= timeoutSec)
            return Result<bool>.Success(false); // обычный waiting

        // journal-before-manipulations: терминальная фаза expired закрывает
        // фазовую серию и считает worker_operation_total{result=expired}
        // (подписка WorkJournal.PhaseWritten → WorkerMetricsInstrumentation).
        // Фаза — ВСЕГДА непрефиксованная "expired" (role-префиксы вида admin:
        // сломали бы совпадение с FinalPhases метрики); различие ролей —
        // в kind исхода и reason.
        var expired = await journal.WritePhaseAsync(
            cluster, op, "expired", instanceId,
            $"ticket age={age}s reason={waitingReason}", ct);
        if (!expired.IsSuccess)
            return Result<bool>.Failed(expired.Error!);

        // ОДНА txn [compare Exists][del заявку][put исход]: атомарна; проигрыш
        // compare (заявки уже нет) — no-op-успех; краш до txn — повтор тиком.
        var txn = await TxnAsync(TxnRequest.Of(
            [TxnCompare.Exists(ticketKey)],
            [
                new TxnOp.Delete(ticketKey, Prefix: false),
                new TxnOp.Put(outcomeKey, TicketOutcomes.OutcomeJson(
                    kind, "expired", waitingReason, audit.RequestedUnix, audit.RequestedBy, nowUnix), null),
            ]), ct);
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        return Result<bool>.Success(true);
    }

    // Финал операции (H/K/E/I): успешная заявка гасит expired-алерт панели
    // перезаписью исхода. Идемпотентный put — безопасен ДО снятия заявки/журнала
    // (порядок вызова — за вызывающим, см. задачи-потребители).
    public async Task<Result> WriteDoneAsync(
        string outcomeKey, string kind, TicketRequestAudit? audit, long nowUnix, CancellationToken ct)
    {
        var value = TicketOutcomes.OutcomeJson(
            kind, "done", null, audit?.RequestedUnix ?? nowUnix, audit?.RequestedBy, nowUnix);
        var put = await PutAsync(outcomeKey, value, ct);
        return put;
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

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
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
}
