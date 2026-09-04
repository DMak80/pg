using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== Kafka-мутации — прокси в API KafkaWorker (task etcd-via-worker-api,
// arch/16 §1.1): панель не пишет в etcd; воркер выполняет клэймы/RMW/компенсации
// 1:1 (arch/02 §10.2). Оператор (requestedBy) передают мутации с аудитом:
// rotate (8), desired (7), create-topic (9), delete-topic (10) — заголовком
// X-Requested-By (spec §3.7); остальные шлют null (заголовок не ставится). =====

// ===== 1. Создание кластера (arch/02 §10.2-1) =====

public sealed record CreateKafkaClusterCommand(CreateKafkaClusterRequest Request)
    : ICommand<KafkaClusterCreatedDto>;

// Ответ 201 POST /api/kafka/clusters (arch/03 §7.2).
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

[InjectAsScoped]
public sealed class CreateKafkaClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateKafkaClusterCommand, KafkaClusterCreatedDto>
{
    public async ValueTask<Result<KafkaClusterCreatedDto>> Handle(
        CreateKafkaClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaClusterCreatedDto>(
            api, "kafkaworker", HttpMethod.Post, "/api/kafka/clusters",
            command.Request, requestedBy: null, ct);
}

// ===== 2. Удаление кластера — config.state=TO_REMOVE (arch/02 §10.2-2) =====

public sealed record DeleteKafkaClusterCommand(string Cluster) : ICommand<KafkaClusterDeletedDto>;

public sealed record KafkaClusterDeletedDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteKafkaClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteKafkaClusterCommand, KafkaClusterDeletedDto>
{
    public async ValueTask<Result<KafkaClusterDeletedDto>> Handle(
        DeleteKafkaClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaClusterDeletedDto>(
            api, "kafkaworker", HttpMethod.Delete, $"/api/kafka/clusters/{command.Cluster}",
            body: null, requestedBy: null, ct);
}

// ===== 3. Изменение default-конфигов (arch/02 §10.2-3) =====

public sealed record UpdateKafkaConfigCommand(string Cluster, KafkaConfigUpdateRequest Request)
    : ICommand<KafkaConfigUpdatedDto>;

public sealed record KafkaConfigUpdatedDto(
    string Cluster, int ReplicationFactor, int MinInSyncReplicas,
    int DefaultPartitions, long DefaultRetentionMs);

[InjectAsScoped]
public sealed class UpdateKafkaConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateKafkaConfigCommand, KafkaConfigUpdatedDto>
{
    public async ValueTask<Result<KafkaConfigUpdatedDto>> Handle(
        UpdateKafkaConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaConfigUpdatedDto>(
            api, "kafkaworker", HttpMethod.Put, $"/api/kafka/clusters/{command.Cluster}/config",
            command.Request, requestedBy: null, ct);
}

// ===== 4. Добавление брокера (arch/02 §10.2-4) =====

public sealed record AddKafkaBrokerCommand(string Cluster, AddKafkaBrokerRequest Request)
    : ICommand<KafkaBrokerAddedDto>;

public sealed record KafkaBrokerAddedDto(
    string Cluster, string Name, string Cpu, string MemGi, string DiskGi, string State);

[InjectAsScoped]
public sealed class AddKafkaBrokerCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AddKafkaBrokerCommand, KafkaBrokerAddedDto>
{
    public async ValueTask<Result<KafkaBrokerAddedDto>> Handle(
        AddKafkaBrokerCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaBrokerAddedDto>(
            api, "kafkaworker", HttpMethod.Post, $"/api/kafka/clusters/{command.Cluster}/brokers",
            command.Request, requestedBy: null, ct);
}

// ===== 5. Удаление брокера — маркер TO_REMOVE (arch/02 §10.2-5) =====

public sealed record RemoveKafkaBrokerCommand(string Cluster, string Broker)
    : ICommand<KafkaBrokerRemovedDto>;

public sealed record KafkaBrokerRemovedDto(string Cluster, string Broker);

[InjectAsScoped]
public sealed class RemoveKafkaBrokerCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RemoveKafkaBrokerCommand, KafkaBrokerRemovedDto>
{
    public async ValueTask<Result<KafkaBrokerRemovedDto>> Handle(
        RemoveKafkaBrokerCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaBrokerRemovedDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/brokers/{command.Broker}",
            body: null, requestedBy: null, ct);
}

// ===== 8. Ротация app-пароля — клэйм-txn заявки (arch/02 §10.2-8) =====

public sealed record RotateKafkaPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaPasswordRotatedDto>;

public sealed record KafkaPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateKafkaPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateKafkaPasswordCommand, KafkaPasswordRotatedDto>
{
    public async ValueTask<Result<KafkaPasswordRotatedDto>> Handle(
        RotateKafkaPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaPasswordRotatedDto>(
            api, "kafkaworker", HttpMethod.Post,
            $"/api/kafka/clusters/{command.Cluster}/app-password/rotate",
            body: null, command.RequestedBy, ct);
}

// ===== 16. Ротация admin-пароля (adminpanel/02 §10.2-16, t03) =====

public sealed record RotateKafkaAdminPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaAdminPasswordRotatedDto>;

public sealed record KafkaAdminPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateKafkaAdminPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateKafkaAdminPasswordCommand, KafkaAdminPasswordRotatedDto>
{
    public async ValueTask<Result<KafkaAdminPasswordRotatedDto>> Handle(
        RotateKafkaAdminPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaAdminPasswordRotatedDto>(
            api, "kafkaworker", HttpMethod.Post,
            $"/api/kafka/clusters/{command.Cluster}/admin-password/rotate",
            body: null, command.RequestedBy, ct);
}

// ===== 7. Конфиг-заявка топика — desired (arch/02 §10.2-7; arch/15 §3) =====

public sealed record UpsertTopicDesiredCommand(
    string Cluster, string Topic, TopicDesiredRequest Request, string RequestedBy)
    : ICommand<KafkaTopicDesiredDto>;

// Ответ 200 PUT /api/kafka/clusters/{c}/topics/{t} (arch/03 §7.2).
public sealed record KafkaTopicDesiredDto(
    string Cluster, string Topic, int? Partitions, long? RetentionMs, int? MinInSyncReplicas);

[InjectAsScoped]
public sealed class UpsertTopicDesiredCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpsertTopicDesiredCommand, KafkaTopicDesiredDto>
{
    public async ValueTask<Result<KafkaTopicDesiredDto>> Handle(
        UpsertTopicDesiredCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaTopicDesiredDto>(
            api, "kafkaworker", HttpMethod.Put,
            $"/api/kafka/clusters/{command.Cluster}/topics/{command.Topic}",
            command.Request, command.RequestedBy, ct);
}

// ===== 8. Отмена конфиг-заявки — desired=null (arch/02 §10.2-8) =====

public sealed record CancelTopicDesiredCommand(string Cluster, string Topic)
    : ICommand<KafkaTopicDesiredCancelledDto>;

public sealed record KafkaTopicDesiredCancelledDto(string Cluster, string Topic);

[InjectAsScoped]
public sealed class CancelTopicDesiredCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelTopicDesiredCommand, KafkaTopicDesiredCancelledDto>
{
    public async ValueTask<Result<KafkaTopicDesiredCancelledDto>> Handle(
        CancelTopicDesiredCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaTopicDesiredCancelledDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/topics/{command.Topic}/desired",
            body: null, requestedBy: null, ct);
}

// ===== 9. Создание топика — клэйм-txn desired.create (arch/02 §10.2-9) =====

public sealed record CreateKafkaTopicCommand(string Cluster, CreateTopicRequest Request, string RequestedBy)
    : ICommand<KafkaTopicCreatedDto>;

// Ответ 201 POST /api/kafka/clusters/{c}/topics (arch/03 §7.2).
public sealed record KafkaTopicCreatedDto(string Cluster, string Topic, int Partitions, int ReplicationFactor);

[InjectAsScoped]
public sealed class CreateKafkaTopicCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateKafkaTopicCommand, KafkaTopicCreatedDto>
{
    public async ValueTask<Result<KafkaTopicCreatedDto>> Handle(
        CreateKafkaTopicCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaTopicCreatedDto>(
            api, "kafkaworker", HttpMethod.Post, $"/api/kafka/clusters/{command.Cluster}/topics",
            command.Request, command.RequestedBy, ct);
}

// ===== 10. Удаление топика — клэйм-txn desired.delete (arch/02 §10.2-10) =====

public sealed record DeleteKafkaTopicCommand(string Cluster, string Topic, string RequestedBy)
    : ICommand<KafkaTopicDeletedDto>;

public sealed record KafkaTopicDeletedDto(string Cluster, string Topic);

[InjectAsScoped]
public sealed class DeleteKafkaTopicCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteKafkaTopicCommand, KafkaTopicDeletedDto>
{
    public async ValueTask<Result<KafkaTopicDeletedDto>> Handle(
        DeleteKafkaTopicCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaTopicDeletedDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/topics/{command.Topic}",
            body: null, command.RequestedBy, ct);
}

// ===== 11–12. Отмена lifecycle-заявок — del ключа (arch/02 §10.2-11/12) =====

public sealed record CancelTopicLifecycleCommand(string Cluster, string Topic, string Op)
    : ICommand<KafkaTopicLifecycleCancelledDto>;

public sealed record KafkaTopicLifecycleCancelledDto(string Cluster, string Topic, string Op);

[InjectAsScoped]
public sealed class CancelTopicLifecycleCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelTopicLifecycleCommand, KafkaTopicLifecycleCancelledDto>
{
    public async ValueTask<Result<KafkaTopicLifecycleCancelledDto>> Handle(
        CancelTopicLifecycleCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaTopicLifecycleCancelledDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/topics/{command.Topic}/desired.{command.Op}",
            body: null, requestedBy: null, ct);
}
