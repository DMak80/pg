using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Удаление valkey-кластера через API воркера: перевод config.state в TO_REMOVE,
// остальные поля сохранены (spec §4.7). Guard «отсутствия state» исполняется
// чтением (Active ⇒ State==null) + txn compare mod_revision прочитанного —
// проигрыш compare значит параллельный перевод, повтор запроса идемпотентен
// (уже TO_REMOVE → успех без записи). Успех = 202: демонтаж исполняет процесс B.
public sealed class DeleteClusterHandler(IEtcdGateway gateway, string[] endpoints)
{
    public const string ToRemoveState = "TO_REMOVE"; // канон config.state (arch/20 §2)

    public async Task<Result> HandleAsync(string cluster, CancellationToken ct)
    {
        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result.Failed(config.Error);
        if (config.Value is null)
            return Result.Failed(new ValkeyClusterNotFoundException(cluster));

        // Идемпотентность: уже TO_REMOVE → успех без записи.
        if (config.Value.State == ToRemoveState)
            return Result.Success();

        // RMW-txn: compare mod_revision == прочитанного → put с state=TO_REMOVE.
        var key = ValkeyApiHelpers.ConfigKey(cluster);
        var updated = config.Value with { State = ToRemoveState };
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, config.Revision!.Value)],
                [new TxnOp.Put(key, updated.Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result.Failed(new ValkeyConcurrentWriteException(key));

        return Result.Success();
    }
}
