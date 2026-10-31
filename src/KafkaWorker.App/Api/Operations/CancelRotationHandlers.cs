using System.Text.Json;
using KafkaWorker.Core;
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
// work-журналу /kafkaworker/work/<C>; del — txn compare по значению (гонка с
// параллельным клэймом/исполнением воркера ловится отказом txn и решается
// повторным чтением — 404/409 по факту).
public abstract class CancelKafkaRotationHandlerBase(IEtcdGateway gateway, string[] endpoints)
{
    protected abstract string Op { get; }

    protected abstract string TicketKey(string cluster);

    // Мутационная фаза = «ротация начата» (гвард отмены, решение A3):
    // константы — точные фазы PasswordRotator/CaRotator (KafkaWorker.Provisioning).
    protected abstract bool MutationLive(WorkState? journal);

    public async Task<Result<KafkaRotationCancelledDto>> HandleAsync(
        string cluster, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaRotationCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));

        // Гвард п.1: живая мутационная фаза журнала → 409 (до чтения заявки —
        // начатая ротация «доедет», отменять нечего даже при живом ключе).
        var journal = await ReadJournalAsync(cluster, ct);
        if (!journal.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(journal.Error!);
        if (MutationLive(journal.Value))
            return Result<KafkaRotationCancelledDto>.Failed(
                new KafkaRotationInProgressException(cluster, Op));

        // Заявка отсутствует → 404.
        var key = TicketKey(cluster);
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(ticket.Error!);
        var value = ticket.Value;
        if (value is null)
            return Result<KafkaRotationCancelledDto>.Failed(
                new KafkaRotationNotFoundException(cluster, Op));

        // Del условным txn: compare по значению (чужая/перезаписанная заявка
        // не удаляется — compare не сойдётся).
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(key, value)],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(txn.Error!);
        if (txn.Value.Succeeded)
            return Result<KafkaRotationCancelledDto>.Success(new KafkaRotationCancelledDto(cluster));

        // Compare не сошёлся — гонка с воркером: ключ исчез (исполнено/снято
        // воркером) → 404; значение изменилось → повторить гвард один раз
        // и решить заново (409 при начавшейся ротации, иначе повторный txn).
        var reread = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!reread.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(reread.Error!);
        if (reread.Value is null)
            return Result<KafkaRotationCancelledDto>.Failed(
                new KafkaRotationNotFoundException(cluster, Op));

        var journalAgain = await ReadJournalAsync(cluster, ct);
        if (!journalAgain.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(journalAgain.Error!);
        if (MutationLive(journalAgain.Value))
            return Result<KafkaRotationCancelledDto>.Failed(
                new KafkaRotationInProgressException(cluster, Op));

        var txnAgain = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(key, reread.Value)],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct));
        if (!txnAgain.IsSuccess)
            return Result<KafkaRotationCancelledDto>.Failed(txnAgain.Error!);
        return txnAgain.Value.Succeeded
            ? Result<KafkaRotationCancelledDto>.Success(new KafkaRotationCancelledDto(cluster))
            : Result<KafkaRotationCancelledDto>.Failed(new EtcdWriteUnavailableException());
    }

    // Чтение work-журнала кластера; битый JSON — журнал «не читается»
    // (толерантно: гвард не блокирует отмену — домен воркера, не трогаем).
    private async Task<Result<WorkState?>> ReadJournalAsync(string cluster, CancellationToken ct)
    {
        var raw = await KafkaApiHelpers.ReadKeyAsync(
            gateway, endpoints, $"/kafkaworker/work/{cluster}", ct);
        if (!raw.IsSuccess)
            return Result<WorkState?>.Failed(raw.Error!);
        if (raw.Value is not { } body)
            return Result<WorkState?>.Success(null);
        try
        {
            return Result<WorkState?>.Success(JsonSerializer.Deserialize<WorkState>(body));
        }
        catch (JsonException)
        {
            return Result<WorkState?>.Success(null);
        }
    }
}

// Мутационные фазы пароль-ротации — ТОЧНО предикат RoleMutationLive
// PasswordRotator (фаза роли: app — legacy-имена без префикса; done/expired/
// waiting-* — не начата). Роли раздельны: заявка роли блокируется только
// фазой ЭТОЙ роли (журнал один на кластер, роли идут по порядку App→Admin).
public sealed class CancelAppRotationHandler(IEtcdGateway gateway, string[] endpoints)
    : CancelKafkaRotationHandlerBase(gateway, endpoints)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/kafkaworker/rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => journal is { Op: "rotate" } j
           && j.Phase is "phase-a" or "rotated-commit" or "phase-c";
}

// Отмена заявки admin-ротации (ключ admin_rotations/<C>; фазы роли admin —
// те же имена с префиксом «admin:» — RoleMutationLive PasswordRotator).
public sealed class CancelAdminRotationHandler(IEtcdGateway gateway, string[] endpoints)
    : CancelKafkaRotationHandlerBase(gateway, endpoints)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/kafkaworker/admin_rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => journal is { Op: "rotate" } j
           && j.Phase is "admin:phase-a" or "admin:rotated-commit" or "admin:phase-c";
}

// Отмена заявки CA-ротации (ключ ca_rotations/<C); гвард — ТОЧНО предикат
// CaMutationLive CaRotator: op=rotate-ca в любой фазе вне {done, waiting-*}
// (phase-p/d/r/committed/c и будущие мутационные фазы — начата).
public sealed class CancelCaRotationHandler(IEtcdGateway gateway, string[] endpoints)
    : CancelKafkaRotationHandlerBase(gateway, endpoints)
{
    protected override string Op => "rotate-ca";

    protected override string TicketKey(string cluster) => $"/kafkaworker/ca_rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => journal is { Op: "rotate-ca" } j
           && j.Phase != "done"
           && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal);
}
