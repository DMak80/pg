using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// ===== DTO топиковых мутаций (арх-канон arch/03 §7.2; дубль панельных осознан, t08) =====

// Ответ 200 PUT /api/kafka/clusters/{c}/topics/{t}.
public sealed record KafkaTopicDesiredDto(
    string Cluster, string Topic, int? Partitions, long? RetentionMs, int? MinInSyncReplicas);

// Ответ 204 DELETE /api/kafka/clusters/{c}/topics/{t}/desired (тело не читается).
public sealed record KafkaTopicDesiredCancelledDto(string Cluster, string Topic);

// Ответ 201 POST /api/kafka/clusters/{c}/topics.
public sealed record KafkaTopicCreatedDto(string Cluster, string Topic, int Partitions, int ReplicationFactor);

// Ответ 204 DELETE /api/kafka/clusters/{c}/topics/{t} (тело не читается).
public sealed record KafkaTopicDeletedDto(string Cluster, string Topic);

// Ответ 204 отмены lifecycle-заявок (тело не читается).
public sealed record KafkaTopicLifecycleCancelledDto(string Cluster, string Topic, string Op);

// ===== 6. Конфиг-заявка топика — desired RMW (arch/02 §10.2-6) =====

// Порт панельного UpsertTopicDesiredCommandHandler (task etcd-via-worker-api):
// guards по прямым чтениям etcd; desired_by — заголовок X-Requested-By (панель
// шлёт оператора), fallback "api" (у панели ClaimsPrincipal; значения etcd не
// меняются, spec §3.7).
public sealed class UpdateTopicDesiredHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaTopicDesiredDto>> HandleAsync(
        string cluster, string topic, TopicDesiredRequest request, string requestedBy, CancellationToken ct)
    {
        // Имя топика каноническое и не internal (arch/15 §3) — иначе 404.
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicDesiredDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicDesiredDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicDesiredDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicDesiredDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Ключ топика напрямую (снапшотов нет): нет/missing/битый.
        var key = KafkaApiHelpers.TopicKey(cluster, topic);
        var read = await KafkaApiHelpers.ReadTopicKeyAsync(gateway, endpoints, key, ct);
        if (read.Error is not null)
            return Result<KafkaTopicDesiredDto>.Failed(read.Error);
        if (read.Json is null)
            return Result<KafkaTopicDesiredDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));
        if (read.Json.Missing)
            return Result<KafkaTopicDesiredDto>.Failed(
                new KafkaTopicNotFoundException(cluster, topic, "топик отсутствует в кластере"));

        // Валидация против факта (partitions — только увеличение, §3.2).
        var errors = KafkaTopicDesiredPlan.Validate(request, read.Json.Partitions);
        if (errors.Count > 0)
            return Result<KafkaTopicDesiredDto>.Failed(new KafkaValidationException(errors));

        // RMW-txn: compare mod_revision + put с desired (факт не трогаем).
        var updated = read.Json.WithDesired(
            KafkaTopicDesiredPlan.Build(request),
            time.GetUtcNow().ToUnixTimeSeconds(),
            requestedBy);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, read.Revision!.Value)],
                [new TxnOp.Put(key, updated.Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaTopicDesiredDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicDesiredDto>.Failed(new KafkaConcurrentWriteException(key));

        return Result<KafkaTopicDesiredDto>.Success(new KafkaTopicDesiredDto(
            cluster, topic, request.Partitions, request.RetentionMs, request.MinInSyncReplicas));
    }
}

// ===== 7. Отмена конфиг-заявки — desired=null RMW (arch/02 §10.2-7) =====

// Порт панельного CancelTopicDesiredCommandHandler: RMW без desired-полей
// (факт сохранён); заявки нет → 404.
public sealed class DeleteDesiredHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<KafkaTopicDesiredCancelledDto>> HandleAsync(
        string cluster, string topic, CancellationToken ct)
    {
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicDesiredCancelledDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        var key = KafkaApiHelpers.TopicKey(cluster, topic);
        var read = await KafkaApiHelpers.ReadTopicKeyAsync(gateway, endpoints, key, ct);
        if (read.Error is not null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(read.Error);
        if (read.Json is null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));
        if (read.Json.Desired is null)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(
                new KafkaTopicDesiredNotFoundException(cluster, topic));

        // RMW-txn: compare mod_revision + put без desired-полей (факт сохранён).
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, read.Revision!.Value)],
                [new TxnOp.Put(key, read.Json.WithoutDesired().Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicDesiredCancelledDto>.Failed(new KafkaConcurrentWriteException(key));

        return Result<KafkaTopicDesiredCancelledDto>.Success(new(cluster, topic));
    }
}

// ===== 9. Создание топика — клэйм-txn desired.create (arch/02 §10.2-9) =====

// Порт панельного CreateKafkaTopicCommandHandler: имя генерит клиент, guards
// по свежему ключу топика (есть не-missing → 409; missing с desired → 409;
// обе lifecycle-заявки свободны); requested_by — X-Requested-By, fallback "api".
public sealed class CreateTopicHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaTopicCreatedDto>> HandleAsync(
        string cluster, CreateTopicRequest request, string requestedBy, CancellationToken ct)
    {
        var topic = request.Name ?? "";

        // Имя каноническое (404 при мусоре — как мутации 6–7).
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicCreatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicCreatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Guards по свежему ключу топика: есть и не missing → 409; missing с
        // живым desired → 409; обе lifecycle-заявки отсутствуют (§10.2-9).
        var key = KafkaApiHelpers.TopicKey(cluster, topic);
        var read = await KafkaApiHelpers.ReadTopicKeyAsync(gateway, endpoints, key, ct);
        if (read.Error is not null)
            return Result<KafkaTopicCreatedDto>.Failed(read.Error);
        if (read.Json is not null && !read.Json.Missing)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaTopicExistsException(cluster, topic));
        if (read.Json is { Missing: true, Desired: not null })
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaDesiredPendingException(cluster, topic));

        foreach (var op in new[] { "create", "delete" })
        {
            var ticket = await KafkaApiHelpers.ReadKeyAsync(
                gateway, endpoints, KafkaApiHelpers.LifecycleKey(cluster, topic, op), ct);
            if (!ticket.IsSuccess)
                return Result<KafkaTopicCreatedDto>.Failed(ticket.Error!);
            if (ticket.Value is not null)
                return Result<KafkaTopicCreatedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, op));
        }

        var errors = KafkaTopicCreateValidator.Validate(request, config.Value);
        if (errors.Count > 0)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaValidationException(errors));

        // Клэйм-txn: compare NotExists(desired.create) + put (порт §9.8).
        var ticketKey = KafkaApiHelpers.LifecycleKey(cluster, topic, "create");
        var plan = KafkaTopicCreatePlan.Build(
            request, config.Value, time.GetUtcNow().ToUnixTimeSeconds(), requestedBy);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(ticketKey)],
                [new TxnOp.Put(ticketKey, plan.Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaTopicCreatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, "create"));

        return Result<KafkaTopicCreatedDto>.Success(new(
            cluster, topic, plan.Partitions, plan.ReplicationFactor));
    }
}

// ===== 10. Удаление топика — клэйм-txn desired.delete (arch/02 §10.2-10) =====

// Порт панельного DeleteKafkaTopicCommandHandler: топик существует и не missing
// (404), живой desired/create → 409; клэйм-txn + идемпотентность (живая
// delete-заявка → 204 без записи).
public sealed class DeleteTopicHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaTopicDeletedDto>> HandleAsync(
        string cluster, string topic, string requestedBy, CancellationToken ct)
    {
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicDeletedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicDeletedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Топик должен существовать и не быть missing (404), живой desired — 409.
        var read = await KafkaApiHelpers.ReadTopicKeyAsync(
            gateway, endpoints, KafkaApiHelpers.TopicKey(cluster, topic), ct);
        if (read.Error is not null)
            return Result<KafkaTopicDeletedDto>.Failed(read.Error);
        if (read.Json is null || read.Json.Missing)
            return Result<KafkaTopicDeletedDto>.Failed(
                new KafkaTopicNotFoundException(cluster, topic, "топик отсутствует в кластере"));
        if (read.Json.Desired is not null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaDesiredPendingException(cluster, topic));

        var createTicket = await KafkaApiHelpers.ReadKeyAsync(
            gateway, endpoints, KafkaApiHelpers.LifecycleKey(cluster, topic, "create"), ct);
        if (!createTicket.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(createTicket.Error!);
        if (createTicket.Value is not null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, "create"));

        // Клэйм-txn + идемпотентность: живая delete-заявка → 204 без записи.
        var ticketKey = KafkaApiHelpers.LifecycleKey(cluster, topic, "delete");
        var existing = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, ticketKey, ct);
        if (!existing.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(existing.Error!);
        if (existing.Value is not null)
            return Result<KafkaTopicDeletedDto>.Success(new(cluster, topic));

        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(ticketKey)],
                [new TxnOp.Put(ticketKey, new TopicLifecycleDeleteJson(
                    time.GetUtcNow().ToUnixTimeSeconds(), requestedBy).Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicDeletedDto>.Success(new(cluster, topic)); // гонка постановки — уже стоит

        return Result<KafkaTopicDeletedDto>.Success(new(cluster, topic));
    }
}

// ===== 11–12. Отмена lifecycle-заявок — del ключа (arch/02 §10.2-11/12) =====

// Порт панельного CancelTopicLifecycleCommandHandler (общий для create/delete):
// 404 если заявки нет; del ключа заявки (окно отмены — до тика воркера).
public sealed class CancelLifecycleHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<KafkaTopicLifecycleCancelledDto>> HandleAsync(
        string cluster, string topic, string op, CancellationToken ct)
    {
        if (op is not ("create" or "delete"))
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(
                new KafkaLifecycleNotFoundException(cluster, topic, op));

        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // 404 если заявки нет; del ключа заявки.
        var ticketKey = KafkaApiHelpers.LifecycleKey(cluster, topic, op);
        var range = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, ticketKey, ct);
        if (!range.IsSuccess)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(range.Error!);
        if (range.Value is null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(
                new KafkaLifecycleNotFoundException(cluster, topic, op));

        var deleted = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.DeleteAsync(endpoint, ticketKey, prefix: false, ct));
        if (!deleted.IsSuccess)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(deleted.Error!);

        return Result<KafkaTopicLifecycleCancelledDto>.Success(new(cluster, topic, op));
    }
}
