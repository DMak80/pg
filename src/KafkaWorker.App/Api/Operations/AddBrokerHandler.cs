using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters/{c}/brokers (arch/03 §7.2; дубль осознан).
public sealed record KafkaBrokerAddedDto(
    string Cluster, string Name, string Cpu, string MemGi, string DiskGi, string State);

// Добавление брокера через API воркера (arch/02 §10.2-4): имя генерит сервер
// broker<max+1> (≤9) по фактическим брокерам префикса; клэйм-txn version==0 на
// state-ключ + put resources; сбой resources → компенсация точечным del state.
// Порт панельного AddKafkaBrokerCommandHandler (guards на прямых чтениях etcd).
public sealed class AddBrokerHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<KafkaBrokerAddedDto>> HandleAsync(
        string cluster, AddKafkaBrokerRequest request, CancellationToken ct)
    {
        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaBrokerAddedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaBrokerAddedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Имя по фактическим брокерам префикса (прямое чтение — не снапшот), ≤ 9.
        var brokers = await KafkaApiHelpers.ReadBrokerNamesAsync(gateway, endpoints, cluster, ct);
        if (!brokers.IsSuccess)
            return Result<KafkaBrokerAddedDto>.Failed(brokers.Error!);
        var next = (brokers.Value.Count == 0 ? 0 : brokers.Value.Max()) + 1;
        if (next > KafkaLimits.MaxBrokers)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaBrokerLimitException());
        var name = $"broker{next}";

        // Валидация ресурсов (границы §10.3; дефолты 2/2/20).
        var cpu = KafkaClusterCreatePlan.Canonical(request.Cpu ?? KafkaLimits.DefCpu);
        var memGi = request.MemGi ?? KafkaLimits.DefMemGi;
        var diskGi = request.DiskGi ?? KafkaLimits.DefDiskGi;
        var errors = new List<KafkaWorker.Core.Writing.ValidationError>();
        if ((request.Cpu ?? KafkaLimits.DefCpu) < KafkaLimits.MinCpu
            || (request.Cpu ?? KafkaLimits.DefCpu) > KafkaLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {KafkaLimits.MinCpu}..{KafkaLimits.MaxCpu} ядер"));
        if (memGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("memGi", $"memGi: {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        if (diskGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        if (errors.Count > 0)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaValidationException(errors));

        // Клэйм-txn: compare NotExists(brokers/<b>/state) + put NOT_INITIALIZED.
        var stateKey = KafkaApiHelpers.BrokerKey(cluster, name, "state");
        var claim = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(stateKey)],
                [new TxnOp.Put(stateKey, "NOT_INITIALIZED", null)]),
            ct));
        if (!claim.IsSuccess)
            return Result<KafkaBrokerAddedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaBrokerNameTakenException(name));

        // PUT resources; сбой → компенсация точечным del state (§9.5-паттерн).
        var resourcesPut = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint,
            KafkaApiHelpers.BrokerKey(cluster, name, "resources"),
            JsonSerializer.Serialize(new ResourcesJson(cpu, $"{memGi}Gi", $"{diskGi}Gi")),
            null, ct));
        if (!resourcesPut.IsSuccess)
        {
            await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.DeleteAsync(endpoint, stateKey, prefix: false, ct));
            return Result<KafkaBrokerAddedDto>.Failed(resourcesPut.Error!);
        }

        return Result<KafkaBrokerAddedDto>.Success(new KafkaBrokerAddedDto(
            cluster, name, cpu, $"{memGi}Gi", $"{diskGi}Gi", "NOT_INITIALIZED"));
    }
}
