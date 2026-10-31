using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

using KafkaWorker.Core.Writing;
namespace KafkaWorker.App.Api.Operations;

// Ответ успешной отмены (204 без тела — панель тело не читает; DTO по образцу
// воркерских дублей, осознанно).
public sealed record KafkaRotationCancelledDto(string Cluster);

// Ротация уже в исполнении (мутационная фаза журнала) — 409 «Rotation rejected»
// (arch/02 §10.2-18/19/20: начатая ротация доедет — остановка невозможна).
public sealed class KafkaRotationInProgressException(string cluster, string op)
    : Exception($"{op} rotation of cluster {cluster} is already executing — cancellation window closed");

// Заявка ротации не найдена (отмена) — 404; идемпотентность повтора
// отсутствует осознанно (образец §9.7.5 / KafkaRebalanceNotFoundException).
public sealed class KafkaRotationNotFoundException(string cluster, string op)
    : Exception($"{op} rotation request for {cluster} not found");

// Базовый класс отмен живых заявок ротаций (arch/02 §10.2-18/19/20, u17; D13):
// del ключа заявки выполняет ТОЛЬКО воркер; гвард «ротация не начата» — по
// work-журналу /kafkaworker/work/<C>; del — условный txn с ДВУМЯ compare:
// значение заявки (чужая/перезаписанная заявка не удаляется) И mod_revision
// журнала, снятые при гварде. Второй compare закрывает окно TOCTOU с клэймом
// ротатора: клэйм (первая журнальная запись мутационной фазы) между гвардом и
// txn меняет mod_revision журнала → txn не сойдётся → перечитанный гвард
// отвечает 409 (arch/02 §10.2: гонка с клэймом ловится отказом txn). Исход
// каждой отмены — строка в host-лог воркера (аудит: оператор из заголовка
// X-Requested-By, панель шлёт username сессии).
public abstract class CancelKafkaRotationHandlerBase(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger logger)
{
    protected abstract string Op { get; }

    protected abstract string TicketKey(string cluster);

    // Мутационная фаза = «ротация начата» (гвард отмены, решение A3):
    // предикаты — ТОЧНО из ротаторов Provisioning (единый источник фаз).
    protected abstract bool MutationLive(WorkState? journal);

    // Ключ work-журнала кластера (та же запись, что у ротатора).
    private string JournalKey(string cluster) => $"/kafkaworker/work/{cluster}";

    public async Task<Result<KafkaRotationCancelledDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaRotationCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));

        // Гвард п.1: живая мутационная фаза журнала → 409 (до чтения заявки —
        // начатая ротация «доедет», отменять нечего даже при живом ключе).
        var journal = await ReadJournalAsync(cluster, ct);
        if (!journal.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(journal.Error!);
        if (MutationLive(journal.Value.State))
            return Blocked(cluster, requestedBy);

        // Заявка отсутствует → 404.
        var key = TicketKey(cluster);
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(ticket.Error!);
        var value = ticket.Value;
        if (value is null)
            return NotFound(cluster, requestedBy);

        // Del условным txn: compare по значению заявки + mod_revision журнала
        // (0 = журнала нет; в etcd mod_revision отсутствующего ключа = 0 —
        // клэйм-запись журнала делает compare несходящимся).
        var txn = await TxnDeleteAsync(key, value, JournalKey(cluster), journal.Value.ModRevision, ct);
        if (!txn.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(txn.Error!);
        if (txn.Value.Succeeded)
            return Cancelled(cluster, requestedBy);

        // Compare не сошёлся — гонка с воркером: ключ исчез (исполнено/снято
        // воркером) → 404; значение изменилось или журнал ушёл вперёд (клэйм!)
        // → повторить гвард один раз и решить заново (409 при начавшейся
        // ротации, иначе повторный txn по свежим сравнениям).
        var reread = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!reread.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(reread.Error!);
        if (reread.Value is null)
            return NotFound(cluster, requestedBy);

        var journalAgain = await ReadJournalAsync(cluster, ct);
        if (!journalAgain.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(journalAgain.Error!);
        if (MutationLive(journalAgain.Value.State))
            return Blocked(cluster, requestedBy);

        var txnAgain = await TxnDeleteAsync(
            key, reread.Value, JournalKey(cluster), journalAgain.Value.ModRevision, ct);
        if (!txnAgain.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(txnAgain.Error!);
        if (txnAgain.Value.Succeeded)
            return Cancelled(cluster, requestedBy);

        // Второй compare не сошёлся (воркер пишет журнал/заявку каждый тик) —
        // не угадываем: 503 «write unavailable», повторная попытка оператора.
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: compare lost twice — not cancelled (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<KafkaRotationCancelledDto>.Failed(new EtcdWriteUnavailableException());
    }

    // Условный del: обе веткиtxn одного вида — compare [value, journal modRev].
    private async Task<Result<TxnResult>> TxnDeleteAsync(
        string key, string value, string journalKey, long journalModRevision, CancellationToken ct)
        => await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [
                    TxnCompare.ValueEqual(key, value),
                    TxnCompare.ModRevisionEqual(journalKey, journalModRevision),
                ],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct));

    // Исходы с аудитом в host-лог (arch/02 §10.2): оператор/исход/кластер.
    private Result<KafkaRotationCancelledDto> Cancelled(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: cancelled (ticket deleted; operator {Operator})",
            Op, cluster, requestedBy);
        return Result<KafkaRotationCancelledDto>.Success(new KafkaRotationCancelledDto(cluster));
    }

    private Result<KafkaRotationCancelledDto> Blocked(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: rejected — rotation already executing (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<KafkaRotationCancelledDto>.Failed(
            new KafkaRotationInProgressException(cluster, Op));
    }

    private Result<KafkaRotationCancelledDto> NotFound(string cluster, string requestedBy)
    {
        logger.LogInformation(
            "DELETE {Op} rotation cancel of {Cluster}: ticket not found (operator {Operator})",
            Op, cluster, requestedBy);
        return Result<KafkaRotationCancelledDto>.Failed(
            new KafkaRotationNotFoundException(cluster, Op));
    }

    // Чтение work-журнала кластера: сырое значение + mod_revision для compare.
    // Битый JSON — журнал «не читается» (толерантно: гвард не блокирует отмену
    // — домен воркера, не трогаем), mod_revision для txn берётся по факту.
    private async Task<Result<(WorkState? State, long ModRevision)>> ReadJournalAsync(
        string cluster, CancellationToken ct)
    {
        var key = JournalKey(cluster);
        var raw = await EtcdFailover.CallAsync(
            endpoints, endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!raw.IsSuccess)
            return Result<(WorkState?, long)>.Failed(raw.Error!);
        var kv = raw.Value.FirstOrDefault(k => k.Key == key);
        if (kv is null)
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
}

// Мутационные фазы пароль-ротации — предикат AppMutationLive PasswordRotator
// (фаза роли: app — legacy-имена без префикса; done/expired/waiting-* — не
// начата). Роли раздельны: заявка роли блокируется только фазой ЭТОЙ роли
// (журнал один на кластер, роли идут по порядку App→Admin).
public sealed class CancelAppRotationHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger<CancelAppRotationHandler> logger)
    : CancelKafkaRotationHandlerBase(gateway, endpoints, logger)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/kafkaworker/rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => PasswordRotator.AppMutationLive(journal);
}

// Отмена заявки admin-ротации (ключ admin_rotations/<C>; фазы роли admin —
// те же имена с префиксом «admin:» — AdminMutationLive PasswordRotator).
public sealed class CancelAdminRotationHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger<CancelAdminRotationHandler> logger)
    : CancelKafkaRotationHandlerBase(gateway, endpoints, logger)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/kafkaworker/admin_rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => PasswordRotator.AdminMutationLive(journal);
}

// Отмена заявки CA-ротации (ключ ca_rotations/<C>); гвард — ТОЧНО предикат
// MutationLive CaRotator: op=rotate-ca в любой фазе вне {done, waiting-*}
// (phase-p/d/r/committed/c и будущие мутационные фазы — начата).
public sealed class CancelCaRotationHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    ILogger<CancelCaRotationHandler> logger)
    : CancelKafkaRotationHandlerBase(gateway, endpoints, logger)
{
    protected override string Op => "rotate-ca";

    protected override string TicketKey(string cluster) => $"/kafkaworker/ca_rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => CaRotator.MutationLive(journal);
}
