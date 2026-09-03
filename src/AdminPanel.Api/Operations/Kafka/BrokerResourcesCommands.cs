using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== 15. Изменение ресурсов брокера (t06, 02 §10.2-15): декларация в etcd
// через API воркера; применяет NodeRegenerator (rolling, автоконверге) =====

public sealed record UpdateKafkaBrokerResourcesCommand(
    string Cluster, string Broker, KafkaBrokerResourcesRequestDto Request)
    : ICommand<KafkaBrokerResourcesUpdatedDto>;

public sealed record KafkaBrokerResourcesRequestDto(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record KafkaBrokerResourcesUpdatedDto(
    string Cluster, string Broker, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class UpdateKafkaBrokerResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateKafkaBrokerResourcesCommand, KafkaBrokerResourcesUpdatedDto>
{
    public async ValueTask<Result<KafkaBrokerResourcesUpdatedDto>> Handle(
        UpdateKafkaBrokerResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaBrokerResourcesUpdatedDto>(
            api, "kafkaworker", HttpMethod.Put,
            $"/api/kafka/clusters/{command.Cluster}/brokers/{command.Broker}/resources",
            body: command.Request, requestedBy: null, ct);
}
