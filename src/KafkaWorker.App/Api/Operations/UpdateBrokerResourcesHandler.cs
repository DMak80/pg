using System.Text.RegularExpressions;
using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 200 PUT /api/kafka/clusters/{c}/brokers/{b}/resources (t06, spec §4.2).
public sealed record KafkaBrokerResourcesDto(
    string Cluster, string Broker, string Cpu, string MemGi, string DiskGi);

// Изменение ресурсов существующего брокера — мутация №15 (t06, adminpanel/02
// §10.2-15): guard'ы по прямым чтениям etcd → канонизация → put ключа целиком.
// Применение — автоматическое: NodeRegenerator воркера (arch/16 §5 J) сверяет
// лимиты живого контейнера и rolling-ит по одному за тик; disk — инфо-поле.
// Порт DeleteBrokerHandler (guard'ы) + UpdateConfigHandler (DTO-ответ).
public sealed partial class UpdateBrokerResourcesHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Имя брокера каноническое (иначе 404 — воркер такие не создаёт).
    [GeneratedRegex("^broker[1-9]$")]
    private static partial Regex BrokerPattern();

    public async Task<Result<KafkaBrokerResourcesDto>> HandleAsync(
        string cluster, string broker, KafkaResourcesUpdateRequest request, CancellationToken ct)
    {
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (!BrokerPattern().IsMatch(broker))
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaBrokerResourcesDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaBrokerResourcesDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Текущая декларация ресурсов (эффективные значения = new ?? current).
        var resourcesKey = KafkaApiHelpers.BrokerKey(cluster, broker, "resources");
        var currentJson = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, resourcesKey, ct);
        if (!currentJson.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(currentJson.Error!);
        if (currentJson.Value is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));
        var current = BrokerResourcesJson.TryParse(currentJson.Value);
        if (current is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new InvalidKafkaConfigException(cluster));

        // Брокер в демонтаже — ресурсы менять незачем (409).
        var state = await KafkaApiHelpers.ReadKeyAsync(
            gateway, endpoints, KafkaApiHelpers.BrokerKey(cluster, broker, "state"), ct);
        if (!state.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(state.Error!);
        if (state.Value is "TO_REMOVE" or "REMOVING")
            return Result<KafkaBrokerResourcesDto>.Failed(
                new KafkaBrokerRemovalInProgressException(cluster, broker));

        // Валидация §10.3 на эффективных значениях.
        var errors = KafkaResourcesUpdateValidator.Validate(request, current);
        if (errors.Count > 0)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaValidationException(errors));

        // Каноническая перезапись целиком (ключ плоский — RMW не нужен);
        // идемпотентность: повтор — та же запись.
        var plan = new KafkaResourcesUpdatePlan(
            request.Cpu ?? current.Cpu, request.MemGi ?? current.MemGi, request.DiskGi ?? current.DiskGi);
        var put = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint, resourcesKey, plan.CanonicalJson, null, ct));
        if (!put.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(put.Error!);

        return Result<KafkaBrokerResourcesDto>.Success(new KafkaBrokerResourcesDto(
            cluster, broker,
            KafkaClusterCreatePlan.Canonical(plan.Cpu), $"{plan.MemGi}Gi", $"{plan.DiskGi}Gi"));
    }
}
