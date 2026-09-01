using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Ответ 201 POST /api/clusters (arch/02 §9.1; DTO панельный 1:1 — дубль осознан, t08).
public sealed record ClusterCreatedDto(
    string Name,
    string DbName,
    bool Sharded,
    int BucketsCount,
    int ShardsTotal,
    int Replicas,
    string RequestCpu,
    string RequestMem,
    string RequestDisk,
    string State);

// Создание кластера через API воркера (task etcd-via-worker-api): порт панельного
// CreateClusterCommandHandler с единственной заменой — активный endpoint не из
// снапшота панели, а свой список с failover (все недоступны → 503).
// Клэйм имени → пакет PUT → компенсация при сбое (arch/02 §9.2). Без ретраев:
// повтор = новый POST от клиента.
public sealed class CreateClusterHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<ClusterCreatedDto>> HandleAsync(CreateClusterRequest command, CancellationToken ct)
    {
        var request = command;

        // 0) Нормализация: sharded=false → 1/1; отсутствует = true (arch/02 §9.3).
        //    ДО Validate — валидатор и план работают с каноническим запросом.
        request = request.Normalize();

        // 1) Валидация (сервер — источник истины, spec t12 §2)
        var errors = CreateClusterValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ClusterCreatedDto>.Failed(new CreateClusterValidationException(errors));

        // 2) Клэйм имени: compare NotExists + put config (атомарная уникальность, arch/02 §9.2)
        var plan = ClusterCreatePlan.Build(request, time.GetUtcNow().ToUnixTimeSeconds());
        var claim = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(plan.ConfigKey)], [new TxnOp.Put(plan.ConfigKey, plan.ConfigValue, null)]),
            ct));
        if (!claim.IsSuccess)
            return Result<ClusterCreatedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ClusterCreatedDto>.Failed(new ClusterAlreadyExistsException(request.Name));

        // 3) Пакет PUT (без txn: max-txn-ops=128 не вмещает 2N+ ключей — arch/02 §9.2)
        foreach (var put in plan.Puts)
        {
            var putResult = await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, put.Key, put.Value, null, ct));
            if (putResult.IsSuccess)
                continue;

            await CompensateAsync(plan, ct);
            return Result<ClusterCreatedDto>.Failed(putResult.Error!);
        }

        return Result<ClusterCreatedDto>.Success(new ClusterCreatedDto(
            request.Name, request.Name, request.Sharded!.Value, request.Buckets, request.Shards,
            request.Replicas, plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk,
            ClusterCreatePlan.NotInitialized));
    }

    // Компенсация best-effort: префикс кластера целиком + точечные request_*
    // (пространство Patroni не трогаем — arch/02 §9.2). Ошибка компенсации не
    // маскирует исходную: частичный кластер безопасен (повтор создания → 409).
    private async Task CompensateAsync(ClusterCreatePlan plan, CancellationToken ct)
    {
        await EtcdFailover.CallAsync(endpoints, endpoint => gateway.DeleteAsync(
            endpoint, $"/clusters/{plan.ConfigKey.Split('/')[2]}/", prefix: true, ct));
        foreach (var key in plan.RequestKeys)
            await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.DeleteAsync(endpoint, key, prefix: false, ct));
    }
}
