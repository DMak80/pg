using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда создания кластера — первая мутация панели (arch/01 §1; spec t12 §3.5);
// вторая — перевод в DELETING (DeleteClusterCommand, arch/02 §9.4).
public sealed record CreateClusterCommand(CreateClusterRequest Request) : ICommand<ClusterCreatedDto>;

// Ответ 201 POST /api/clusters (arch/03 §1.1).
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

// Валидация не прошла: 400 с errors по полям (arch/03 §1.1).
public sealed class CreateClusterValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры создания кластера некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Клэйм-txn не сошёлся: имя занято (arch/02 §9.2) — 409.
public sealed class ClusterAlreadyExistsException(string name)
    : Exception($"кластер {name} уже существует (config-ключ присутствует)");

// Нет снапшота/активного endpoint'а — писать некуда (spec t12 §8.12) — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)");

// Клэйм имени → пакет PUT → компенсация при сбое (arch/02 §9.2). Без ретраев:
// повтор = новый POST от пользователя (spec t12 §8.13).
[InjectAsScoped]
public sealed class CreateClusterCommandHandler(
    ISnapshotStore store,
    IEtcdGateway gateway,
    TimeProvider time) : ICommandHandler<CreateClusterCommand, ClusterCreatedDto>
{
    public async ValueTask<Result<ClusterCreatedDto>> Handle(CreateClusterCommand command, CancellationToken ct)
    {
        var request = command.Request;

        // 0) Нормализация: sharded=false → 1/1; отсутствует = true (arch/02 §9.3).
        //    ДО Validate — валидатор и план работают с каноническим запросом.
        request = request.Normalize();

        // 1) Валидация (сервер — источник истины, spec t12 §2)
        var errors = CreateClusterValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ClusterCreatedDto>.Failed(new CreateClusterValidationException(errors));

        // 2) Активный endpoint из снапшота — его выбирает/ротирует refresher (spec t12 §8.12)
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ClusterCreatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Клэйм имени: compare version==0 + put config (атомарная уникальность, arch/02 §9.2)
        var plan = ClusterCreatePlan.Build(request, time.GetUtcNow().ToUnixTimeSeconds());
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(plan.ConfigKey, 0)], [new KvPut(plan.ConfigKey, plan.ConfigValue)], ct);
        if (!claim.IsSuccess)
            return Result<ClusterCreatedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ClusterCreatedDto>.Failed(new ClusterAlreadyExistsException(request.Name));

        // 4) Пакет PUT (без txn: max-txn-ops=128 не вмещает 2N+ ключей — arch/02 §9.2)
        foreach (var put in plan.Puts)
        {
            var putResult = await gateway.PutAsync(endpoint, put.Key, put.Value, ct);
            if (putResult.IsSuccess)
                continue;

            await CompensateAsync(endpoint, plan, ct);
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
    private async Task CompensateAsync(string endpoint, ClusterCreatePlan plan, CancellationToken ct)
    {
        await gateway.DeleteAsync(endpoint, $"/clusters/{plan.ConfigKey.Split('/')[2]}/", prefix: true, ct);
        foreach (var key in plan.RequestKeys)
            await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
    }
}
