using System.Text.Json;
using Microsoft.Extensions.Logging;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

using ValkeyWorker.Core.Writing;
using ValkeyWorker.Provisioning.Processes;
namespace ValkeyWorker.App.Api.Operations;

// Ответ успешной отмены (204 без тела — панель тело не читает; DTO по образцу
// воркерских дублей, осознанно).
public sealed record ValkeyRotationCancelledDto(string Cluster);

// Ротация уже в исполнении (мутационная фаза журнала/стейта) — 409 «Rotation
// rejected» (arch/02 §11.2-7/8: начатая ротация доедет — остановка невозможна).
public sealed class ValkeyRotationInProgressException(string cluster, string op)
    : Exception($"{op} rotation of cluster {cluster} is already executing — cancellation window closed");

// Заявка ротации не найдена (отмена) — 404; идемпотентность повтора
// отсутствует осознанно (образец §9.7.5).
public sealed class ValkeyRotationNotFoundException(string cluster, string op)
    : Exception($"{op} rotation request for {cluster} not found");

// Базовый класс отмен живых заявок ротаций (arch/02 §11.2-7/8, u17; D13):
// del ключа заявки выполняет ТОЛЬКО воркер; гвард «ротация не начата» — по
// work-журналу /valkeyworker/work/<C> И по стейту доигрывания work/<C>/rotation
// (E-ротация пишет стейт e1-pending ДО SETUSER — точка невозврата: слепой к
// стейту гвард пропускал бы отмену начавшейся ротации; arch/02 §11.2). del —
// условный txn с compare: значение заявки + mod_revision журнала и стейта
// (0 = ключа нет; клэйм между гвардом и txn меняет mod_revision → txn не
// сойдётся → перечитанный гвард отвечает 409). Исход каждой отмены — строка
// в host-лог воркера (аудит: оператор из заголовка X-Requested-By, панель
// шлёт username сессии).
public abstract class CancelValkeyRotationHandlerBase(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger logger)
{
    protected abstract string Op { get; }

    protected abstract string TicketKey(string cluster);

    // Ключ стейта доигрывания E-ротации (null — у ротации его нет: CA живёт
    // только журналом; стейт ставится в P? — нет: CA пишет журнал phase-p до
    // staging, журнального compare достаточно).
    protected abstract string? StateKey(string cluster);

    // Мутационная фаза = «ротация начата» (гвард отмены, решение A3):
    // предикаты — ТОЧНО из ротаторов Provisioning (единый источник фаз).
    protected abstract bool MutationLive(WorkState? journal);

    // Мутация по стейту доигрывания (значение поля "phase" стейта); дефолт —
    // стейта нет (null) → не живая.
    protected virtual bool StateMutationLive(string? statePhase) => false;

    public async Task<Result<ValkeyRotationCancelledDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое, иначе 404.
        if (!ValkeyLimits.ClusterPattern().IsMatch(cluster))
            return Result<ValkeyRotationCancelledDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Гвард п.1: живая мутационная фаза журнала ИЛИ стейта доигрывания →
        // 409 (до чтения заявки — начатая ротация «доедет»).
        var journal = await ReadJournalAsync(cluster, ct);
        if (!journal.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(journal.Error!);
        var stateKey = StateKey(cluster);
        // Result — структура: nullable-обёртка для «стейта нет» (CA).
        Result<(string? State, long ModRevision)>? state = null;
        if (stateKey is not null)
            state = await ReadStatePhaseAsync(cluster, stateKey, ct);
        if (state is { IsSuccess: false } failed)
            return Result<ValkeyRotationCancelledDto>.Failed(failed.Error!);
        if (MutationLive(journal.Value.State)
            || (state is { Value: { } stateRead } && StateMutationLive(stateRead.State)))
            return Blocked(cluster, requestedBy);

        // Заявка отсутствует → 404.
        var key = TicketKey(cluster);
        var ticket = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(ticket.Error!);
        var value = ticket.Value?.Value;
        if (value is null)
            return NotFound(cluster, requestedBy);

        // Del условным txn: compare по значению заявки + mod_revision журнала
        // и стейта (0 = ключа нет; клэйм-запись делает compare несходящимся).
        var txn = await TxnDeleteAsync(key, value, JournalKey(cluster), journal.Value.ModRevision,
            stateKey, state is { IsSuccess: true, Value: { } st } ? st.ModRevision : 0, ct);
        if (!txn.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(txn.Error!);
        if (txn.Value.Succeeded)
            return Cancelled(cluster, requestedBy);

        // Compare не сошёлся — гонка с воркером: ключ исчез → 404; значение
        // изменилось или журнал/стейт ушли вперёд (клэйм!) → повторить гвард
        // один раз и решить заново (409 при начавшейся ротации, иначе txn).
        var reread = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!reread.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(reread.Error!);
        if (reread.Value?.Value is not { } rereadValue)
            return NotFound(cluster, requestedBy);

        var journalAgain = await ReadJournalAsync(cluster, ct);
        if (!journalAgain.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(journalAgain.Error!);
        Result<(string? State, long ModRevision)>? stateAgain = null;
        if (stateKey is not null)
            stateAgain = await ReadStatePhaseAsync(cluster, stateKey, ct);
        if (stateAgain is { IsSuccess: false } failedAgain)
            return Result<ValkeyRotationCancelledDto>.Failed(failedAgain.Error!);
        if (MutationLive(journalAgain.Value.State)
            || (stateAgain is { Value: { } stateReadAgain } && StateMutationLive(stateReadAgain.State)))
            return Blocked(cluster, requestedBy);

        var txnAgain = await TxnDeleteAsync(key, rereadValue, JournalKey(cluster), journalAgain.Value.ModRevision,
            stateKey, stateAgain is { IsSuccess: true, Value: { } stAgain } ? stAgain.ModRevision : 0, ct);
        if (!txnAgain.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(txnAgain.Error!);
        if (txnAgain.Value.Succeeded)
            return Cancelled(cluster, requestedBy);

        // Второй compare не сошёлся (воркер пишет журнал/стейт каждый тик) —
        // не угадываем: 503 «write unavailable», повторная попытка оператора.
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: compare lost twice — not cancelled (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<ValkeyRotationCancelledDto>.Failed(new EtcdWriteUnavailableException());
    }

    // Условный del: compare значение заявки + mod_revision журнала (+ стейта,
    // когда ротация его ведёт); modRevision 0 = ключа нет (etcd-семантика).
    private async Task<Result<TxnResult>> TxnDeleteAsync(
        string key, string value, string journalKey, long journalModRevision,
        string? stateKey, long stateModRevision, CancellationToken ct)
    {
        var compares = new List<TxnCompare>
        {
            TxnCompare.ValueEqual(key, value),
            TxnCompare.ModRevisionEqual(journalKey, journalModRevision),
        };
        if (stateKey is not null)
            compares.Add(TxnCompare.ModRevisionEqual(stateKey, stateModRevision));

        return await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(compares, [new TxnOp.Delete(key, Prefix: false)]),
            ct));
    }

    // Исходы с аудитом в host-лог (arch/02 §11.2): оператор/исход/кластер.
    private Result<ValkeyRotationCancelledDto> Cancelled(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: cancelled (ticket deleted; operator {Operator})",
            Op, cluster, requestedBy);
        return Result<ValkeyRotationCancelledDto>.Success(new ValkeyRotationCancelledDto(cluster));
    }

    private Result<ValkeyRotationCancelledDto> Blocked(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: rejected — rotation already executing (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<ValkeyRotationCancelledDto>.Failed(
            new ValkeyRotationInProgressException(cluster, Op));
    }

    private Result<ValkeyRotationCancelledDto> NotFound(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: ticket not found (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<ValkeyRotationCancelledDto>.Failed(
            new ValkeyRotationNotFoundException(cluster, Op));
    }

    private string JournalKey(string cluster) => $"/valkeyworker/work/{cluster}";

    // Чтение work-журнала кластера: разбор + mod_revision для compare. Битый
    // JSON — журнал «не читается» (толерантно: гвард не блокирует отмену —
    // домен воркера, не трогаем), mod_revision для txn берётся по факту.
    private async Task<Result<(WorkState? State, long ModRevision)>> ReadJournalAsync(
        string cluster, CancellationToken ct)
    {
        var key = JournalKey(cluster);
        var raw = await ValkeyEtcdFailover.CallAsync(
            endpoints, endpoint => gateway.GetAsync(endpoint, key, ct));
        if (!raw.IsSuccess)
            return Result<(WorkState?, long)>.Failed(raw.Error!);
        if (raw.Value is not { } kv)
            return Result<(WorkState?, long)>.Success((null, 0));
        try
        {
            var state = JsonSerializer.Deserialize<WorkState>(kv.Value);
            return Result<(WorkState?, long)>.Success((state, (long)kv.ModRevision));
        }
        catch (JsonException)
        {
            return Result<(WorkState?, long)>.Success((null, (long)kv.ModRevision));
        }
    }

    // Чтение стейта доигрывания: фаза (поле "phase") + mod_revision. Битый
    // JSON — фаза null (гвард не блокирует отмену по мусору), mod_rev по факту.
    private async Task<Result<(string? State, long ModRevision)>> ReadStatePhaseAsync(
        string cluster, string stateKey, CancellationToken ct)
    {
        var raw = await ValkeyEtcdFailover.CallAsync(
            endpoints, endpoint => gateway.GetAsync(endpoint, stateKey, ct));
        if (!raw.IsSuccess)
            return Result<(string?, long)>.Failed(raw.Error!);
        if (raw.Value is not { } kv)
            return Result<(string?, long)>.Success((null, 0));
        return Result<(string?, long)>.Success(
            (PasswordRotator.StatePhaseOf(kv.Value), (long)kv.ModRevision));
    }
}

// Отмена живой заявки ротации пароля (ключ /valkeyworker/rotations/<C> — ОДИН
// на обе роли app|admin; гвард — ТОЧНО предикаты PasswordRotator: журнал
// e1-added/e2-committed ИЛИ стейт e1-pending|e1-added|e2-committed; done/
// expired/waiting-* — не начата).
public sealed class CancelPasswordRotationHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger<CancelPasswordRotationHandler> logger)
    : CancelValkeyRotationHandlerBase(gateway, endpoints, logger)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/valkeyworker/rotations/{cluster}";

    // Стейт доигрывания E-ротации (arch/21 §5 E) — участвует в гварде и txn.
    protected override string? StateKey(string cluster)
        => $"/valkeyworker/work/{cluster}/rotation";

    protected override bool MutationLive(WorkState? journal)
        => PasswordRotator.MutationLive(journal);

    protected override bool StateMutationLive(string? statePhase)
        => PasswordRotator.StateMutationLive(statePhase);
}

// Отмена живой заявки ротации CA (ключ /valkeyworker/ca_rotations/<C>; гвард —
// ТОЧНО предикат MutationLive CaRotator: op=rotate-ca вне {done, waiting-*}).
public sealed class CancelValkeyCaRotationHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger<CancelValkeyCaRotationHandler> logger)
    : CancelValkeyRotationHandlerBase(gateway, endpoints, logger)
{
    protected override string Op => "rotate-ca";

    protected override string TicketKey(string cluster) => $"/valkeyworker/ca_rotations/{cluster}";

    // CA-ротация стейта доигрывания не ведёт (журнального compare достаточно:
    // phase-p пишется в журнал до постановки staging).
    protected override string? StateKey(string cluster) => null;

    protected override bool MutationLive(WorkState? journal)
        => CaRotator.MutationLive(journal);
}
