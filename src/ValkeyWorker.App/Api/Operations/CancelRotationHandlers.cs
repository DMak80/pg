using System.Text.Json;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

using ValkeyWorker.Core.Writing;
namespace ValkeyWorker.App.Api.Operations;

// Ответ успешной отмены (204 без тела — панель тело не читает; DTO по образцу
// воркерских дублей, осознанно).
public sealed record ValkeyRotationCancelledDto(string Cluster);

// Ротация уже в исполнении (мутационная фаза журнала) — 409 «Rotation rejected»
// (arch/02 §11.2-7/8: начатая ротация доедет — остановка невозможна).
public sealed class ValkeyRotationInProgressException(string cluster, string op)
    : Exception($"{op} rotation of cluster {cluster} is already executing — cancellation window closed");

// Заявка ротации не найдена (отмена) — 404; идемпотентность повтора
// отсутствует осознанно (образец §9.7.5).
public sealed class ValkeyRotationNotFoundException(string cluster, string op)
    : Exception($"{op} rotation request for {cluster} not found");

// Базовый класс отмен живых заявок ротаций (arch/02 §11.2-7/8, u17; D13):
// del ключа заявки выполняет ТОЛЬКО воркер; гвард «ротация не начата» — по
// work-журналу /valkeyworker/work/<C>; del — txn compare по значению (гонка с
// параллельным клэймом/исполнением воркера ловится отказом txn и решается
// повторным чтением — 404/409 по факту).
public abstract class CancelValkeyRotationHandlerBase(IEtcdGateway gateway, string[] endpoints)
{
    protected abstract string Op { get; }

    protected abstract string TicketKey(string cluster);

    // Мутационная фаза = «ротация начата» (гвард отмены, решение A3):
    // константы — точные предикаты PasswordRotator/CaRotator (ValkeyWorker.Provisioning).
    protected abstract bool MutationLive(WorkState? journal);

    public async Task<Result<ValkeyRotationCancelledDto>> HandleAsync(
        string cluster, CancellationToken ct)
    {
        // Имя каноническое, иначе 404.
        if (!ValkeyLimits.ClusterPattern().IsMatch(cluster))
            return Result<ValkeyRotationCancelledDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Гвард п.1: живая мутационная фаза журнала → 409 (до чтения заявки).
        var journal = await ReadJournalAsync(cluster, ct);
        if (!journal.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(journal.Error!);
        if (MutationLive(journal.Value))
            return Result<ValkeyRotationCancelledDto>.Failed(
                new ValkeyRotationInProgressException(cluster, Op));

        // Заявка отсутствует → 404.
        var key = TicketKey(cluster);
        var ticket = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(ticket.Error!);
        var value = ticket.Value?.Value;
        if (value is null)
            return Result<ValkeyRotationCancelledDto>.Failed(
                new ValkeyRotationNotFoundException(cluster, Op));

        // Del условным txn: compare по значению (чужая/перезаписанная заявка
        // не удаляется — compare не сойдётся).
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(key, value)],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct));
        if (!txn.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(txn.Error!);
        if (txn.Value.Succeeded)
            return Result<ValkeyRotationCancelledDto>.Success(new ValkeyRotationCancelledDto(cluster));

        // Compare не сошёлся — гонка с воркером: ключ исчез → 404; значение
        // изменилось → повторить гвард один раз и решить заново.
        var reread = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!reread.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(reread.Error!);
        if (reread.Value?.Value is not { } rereadValue)
            return Result<ValkeyRotationCancelledDto>.Failed(
                new ValkeyRotationNotFoundException(cluster, Op));

        var journalAgain = await ReadJournalAsync(cluster, ct);
        if (!journalAgain.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(journalAgain.Error!);
        if (MutationLive(journalAgain.Value))
            return Result<ValkeyRotationCancelledDto>.Failed(
                new ValkeyRotationInProgressException(cluster, Op));

        var txnAgain = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(key, rereadValue)],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct));
        if (!txnAgain.IsSuccess)
            return Result<ValkeyRotationCancelledDto>.Failed(txnAgain.Error!);
        return txnAgain.Value.Succeeded
            ? Result<ValkeyRotationCancelledDto>.Success(new ValkeyRotationCancelledDto(cluster))
            : Result<ValkeyRotationCancelledDto>.Failed(new EtcdWriteUnavailableException());
    }

    // Чтение work-журнала кластера; битый JSON — журнал «не читается»
    // (толерантно: гвард не блокирует отмену — домен воркера, не трогаем).
    private async Task<Result<WorkState?>> ReadJournalAsync(string cluster, CancellationToken ct)
    {
        var raw = await ValkeyApiHelpers.ReadKeyAsync(
            gateway, endpoints, $"/valkeyworker/work/{cluster}", ct);
        if (!raw.IsSuccess)
            return Result<WorkState?>.Failed(raw.Error!);
        if (raw.Value?.Value is not { } body)
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

// Отмена живой заявки ротации пароля (ключ /valkeyworker/rotations/<C> — ОДИН
// на обе роли app|admin; гвард — ТОЧНО предикат PasswordMutationLive
// CaRotator/PasswordRotator: op=rotate в фазе e1-added/e2-committed —
// e1-pending в журнал не пишется; done/expired/waiting-* — не начата).
public sealed class CancelPasswordRotationHandler(IEtcdGateway gateway, string[] endpoints)
    : CancelValkeyRotationHandlerBase(gateway, endpoints)
{
    protected override string Op => "rotate";

    protected override string TicketKey(string cluster) => $"/valkeyworker/rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => journal is { Op: "rotate" } j
           && j.Phase is "e1-added" or "e2-committed";
}

// Отмена живой заявки ротации CA (ключ /valkeyworker/ca_rotations/<C>; гвард —
// ТОЧНО предикат CaMutationLive CaRotator: op=rotate-ca вне {done, waiting-*}).
public sealed class CancelValkeyCaRotationHandler(IEtcdGateway gateway, string[] endpoints)
    : CancelValkeyRotationHandlerBase(gateway, endpoints)
{
    protected override string Op => "rotate-ca";

    protected override string TicketKey(string cluster) => $"/valkeyworker/ca_rotations/{cluster}";

    protected override bool MutationLive(WorkState? journal)
        => journal is { Op: "rotate-ca" } j
           && j.Phase != "done"
           && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal);
}
