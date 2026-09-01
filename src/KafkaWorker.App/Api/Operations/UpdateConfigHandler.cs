using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 200 PUT /api/kafka/clusters/{c}/config (arch/03 §7.2; панель отвечает
// Ok(DTO) — код/тело 1:1, чек 50 шаг 7).
public sealed record KafkaConfigUpdatedDto(
    string Cluster, int ReplicationFactor, int MinInSyncReplicas,
    int DefaultPartitions, long DefaultRetentionMs);

// Изменение default-конфигов через API воркера (arch/02 §10.2-3): RMW-txn по
// mod_revision прочитанного config. Порт панельного UpdateKafkaConfigCommandHandler;
// проигрыш compare → KafkaConcurrentWriteException = 503 (retry клиентом).
// Применение значений — converge dynamic broker configs воркера (arch/16 §5 E).
public sealed class UpdateConfigHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<KafkaConfigUpdatedDto>> HandleAsync(
        string cluster, KafkaConfigUpdateRequest request, CancellationToken ct)
    {
        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaConfigUpdatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaConfigUpdatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaConfigUpdatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Валидация на эффективных значениях (minISR ≤ RF, границы §10.3).
        var errors = KafkaCreateValidator.ValidateUpdate(
            request, config.Value.ReplicationFactor, config.Value.MinInSyncReplicas);
        if (errors.Count > 0)
            return Result<KafkaConfigUpdatedDto>.Failed(new KafkaValidationException(errors));

        // RMW-txn: compare mod_revision == прочитанной + put канонического JSON
        // (state сохраняется — его нет у Active).
        var updated = config.Value.With(request);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(KafkaApiHelpers.ConfigKey(cluster), config.Revision!.Value)],
                [new TxnOp.Put(KafkaApiHelpers.ConfigKey(cluster), updated.Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaConfigUpdatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaConfigUpdatedDto>.Failed(
                new KafkaConcurrentWriteException(KafkaApiHelpers.ConfigKey(cluster)));

        return Result<KafkaConfigUpdatedDto>.Success(new KafkaConfigUpdatedDto(
            cluster, updated.ReplicationFactor, updated.MinInSyncReplicas,
            updated.DefaultPartitions, updated.DefaultRetentionMs));
    }
}
