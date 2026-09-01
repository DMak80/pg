using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters (arch/03 §7.2; DTO панельный 1:1 — дубль осознан, t08).
public sealed record KafkaClusterCreatedDto(
    string Name,
    string State,
    int Brokers,
    int ReplicationFactor,
    int MinInSyncReplicas,
    int DefaultPartitions,
    long DefaultRetentionMs,
    string Cpu,
    string MemGi,
    string DiskGi);

// Создание kafka-кластера через API воркера (task etcd-via-worker-api): порт
// панельного CreateKafkaClusterCommandHandler с единственной заменой — активный
// endpoint не из снапшота панели, а свой список с failover. Клэйм имени →
// пакет PUT → компенсация при сбое (arch/02 §10.2-1). Без ретраев: повтор =
// новый POST от клиента.
public sealed class CreateClusterHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaClusterCreatedDto>> HandleAsync(
        CreateKafkaClusterRequest request, CancellationToken ct)
    {
        // 1) Валидация (сервер — источник истины, arch/02 §10.3).
        var errors = KafkaCreateValidator.Validate(request);
        if (errors.Count > 0)
            return Result<KafkaClusterCreatedDto>.Failed(new KafkaValidationException(errors));

        // 2) Клэйм имени: compare NotExists + put config NOT_INITIALIZED.
        var plan = KafkaClusterCreatePlan.Build(request, time.GetUtcNow().ToUnixTimeSeconds());
        var claim = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(plan.ConfigKey)],
                [new TxnOp.Put(plan.ConfigKey, plan.ConfigValue, null)]),
            ct));
        if (!claim.IsSuccess)
            return Result<KafkaClusterCreatedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<KafkaClusterCreatedDto>.Failed(new KafkaClusterAlreadyExistsException(request.Name!));

        // 3) Пакет PUT brokers/<k>/{state,resources}; сбой → компенсация
        //    префиксом (arch/02 §10.2-1 п.3; повтор создания — 409 на клэйме).
        foreach (var put in plan.Puts)
        {
            var putResult = await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, put.Key, put.Value, null, ct));
            if (putResult.IsSuccess)
                continue;

            await EtcdFailover.CallAsync(endpoints, endpoint => gateway.DeleteAsync(
                endpoint, $"/kafka/clusters/{request.Name}/", prefix: true, ct));
            return Result<KafkaClusterCreatedDto>.Failed(putResult.Error!);
        }

        return Result<KafkaClusterCreatedDto>.Success(new KafkaClusterCreatedDto(
            request.Name!, KafkaClusterCreatePlan.NotInitialized,
            request.Brokers ?? KafkaLimits.DefBrokers,
            request.ReplicationFactor ?? KafkaLimits.DefRf,
            request.MinInSyncReplicas ?? KafkaLimits.DefMinIsr,
            request.DefaultPartitions ?? KafkaLimits.DefPartitions,
            request.DefaultRetentionMs ?? KafkaLimits.DefRetentionMs,
            plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk));
    }
}
