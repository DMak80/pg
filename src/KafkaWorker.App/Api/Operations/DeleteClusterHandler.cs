using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Удаление kafka-кластера через API воркера: перевод config.state в TO_REMOVE
// с сохранением остальных полей (arch/02 §10.2-2). Порт панельного
// DeleteKafkaClusterCommandHandler (он уже читал config напрямую у etcd).
// Идемпотентен: уже TO_REMOVE → 204 без записи. Без ретраев — повтор = новый DELETE.
public sealed class DeleteClusterHandler(IEtcdGateway gateway, string[] endpoints)
{
    public const string ToRemoveState = "TO_REMOVE"; // канон config.state (arch/02 §10.2-2)

    public async Task<Result> HandleAsync(string cluster, CancellationToken ct)
    {
        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result.Failed(config.Error);
        if (config.Value is null)
            return Result.Failed(new KafkaClusterNotFoundException(cluster));

        // Идемпотентность: уже TO_REMOVE → 204 без записи.
        if (config.Value.State == ToRemoveState)
            return Result.Success();

        // PUT config с state=TO_REMOVE, остальные поля сохранены.
        var updated = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint, KafkaApiHelpers.ConfigKey(cluster), config.Value.WithState(ToRemoveState).Serialize(), null, ct));
        return updated.IsSuccess ? Result.Success() : Result.Failed(updated.Error!);
    }
}
