using System.Text.RegularExpressions;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== Общие отказы kafka-мутаций (порт текстов pg-команд) =====

// Кластер не найден (config-ключа нет / имя неканоническое) — 404.
public sealed class KafkaClusterNotFoundException(string cluster)
    : Exception($"kafka-кластер {cluster} не найден");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class KafkaClusterNotActiveException(string cluster, string state)
    : Exception($"kafka-кластер {cluster} не Active (state={state}) — операция отклонена");

// Битый config в etcd — 503.
public sealed class InvalidKafkaConfigException(string cluster)
    : Exception($"config kafka-кластера {cluster} не читается (битый JSON)");

// Валидация: 400 с errors по полям.
public sealed class KafkaValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-compare проигран (конкурентная запись) — повтор запроса клиентом.
public sealed class KafkaConcurrentWriteException(string key)
    : Exception($"{key} изменился с момента чтения — повторите запрос");

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

public sealed class KafkaClusterAlreadyExistsException(string name)
    : Exception($"kafka-кластер {name} уже существует");

[InjectAsScoped]
public sealed class CreateKafkaClusterCommandHandler(
    ISnapshotStore store,
    IEtcdGateway gateway,
    TimeProvider time) : ICommandHandler<CreateKafkaClusterCommand, KafkaClusterCreatedDto>
{
    public async ValueTask<Result<KafkaClusterCreatedDto>> Handle(
        CreateKafkaClusterCommand command, CancellationToken ct)
    {
        var request = command.Request;

        // 1) Валидация (сервер — источник истины, arch/02 §10.3).
        var errors = KafkaCreateValidator.Validate(request);
        if (errors.Count > 0)
            return Result<KafkaClusterCreatedDto>.Failed(new KafkaValidationException(errors));

        // 2) Активный endpoint kafka-снапшота.
        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaClusterCreatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Клэйм имени: txn compare version(config)==0 + put config NOT_INITIALIZED.
        var plan = KafkaClusterCreatePlan.Build(request, time.GetUtcNow().ToUnixTimeSeconds());
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(plan.ConfigKey, 0)], [new KvPut(plan.ConfigKey, plan.ConfigValue)], ct);
        if (!claim.IsSuccess)
            return Result<KafkaClusterCreatedDto>.Failed(new EtcdWriteUnavailableException());
        if (!claim.Value.Succeeded)
            return Result<KafkaClusterCreatedDto>.Failed(new KafkaClusterAlreadyExistsException(request.Name!));

        // 4) Пакет PUT brokers/<k>/{state,resources}; сбой → компенсация префиксом
        //    (arch/02 §10.2-1 п.3; повтор создания — 409 на клэйме).
        foreach (var put in plan.Puts)
        {
            var putResult = await gateway.PutAsync(endpoint, put.Key, put.Value, ct);
            if (putResult.IsSuccess)
                continue;

            await gateway.DeleteAsync(endpoint, $"/kafka/clusters/{request.Name}/", prefix: true, ct);
            return Result<KafkaClusterCreatedDto>.Failed(new EtcdWriteUnavailableException());
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

// ===== 2. Удаление кластера — config.state=TO_REMOVE (arch/02 §10.2-2) =====

public sealed record DeleteKafkaClusterCommand(string Cluster) : ICommand<KafkaClusterDeletedDto>;

public sealed record KafkaClusterDeletedDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteKafkaClusterCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<DeleteKafkaClusterCommand, KafkaClusterDeletedDto>
{
    public async ValueTask<Result<KafkaClusterDeletedDto>> Handle(
        DeleteKafkaClusterCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaClusterDeletedDto>.Failed(new EtcdWriteUnavailableException());

        // Config напрямую (снапшот отстаёт): нет/битый/имя неканоническое → 404/503.
        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaClusterDeletedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaClusterDeletedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        // Идемпотентность: уже TO_REMOVE → 204 без записи (arch/02 §10.2-2).
        if (config.Value.State == "TO_REMOVE")
            return Result<KafkaClusterDeletedDto>.Success(new KafkaClusterDeletedDto(cluster));

        // PUT config с state=TO_REMOVE, остальные поля сохранены.
        var updated = await gateway.PutAsync(
            endpoint, KafkaCommandHelpers.ConfigKey(cluster), config.Value.WithState("TO_REMOVE").Serialize(), ct);
        if (!updated.IsSuccess)
            return Result<KafkaClusterDeletedDto>.Failed(new EtcdWriteUnavailableException());

        return Result<KafkaClusterDeletedDto>.Success(new KafkaClusterDeletedDto(cluster));
    }
}

// ===== 3. Изменение default-конфигов — RMW-txn по mod_revision (arch/02 §10.2-3) =====

public sealed record UpdateKafkaConfigCommand(string Cluster, KafkaConfigUpdateRequest Request)
    : ICommand<KafkaConfigUpdatedDto>;

public sealed record KafkaConfigUpdatedDto(
    string Cluster, int ReplicationFactor, int MinInSyncReplicas,
    int DefaultPartitions, long DefaultRetentionMs);

[InjectAsScoped]
public sealed class UpdateKafkaConfigCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<UpdateKafkaConfigCommand, KafkaConfigUpdatedDto>
{
    public async ValueTask<Result<KafkaConfigUpdatedDto>> Handle(
        UpdateKafkaConfigCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaConfigUpdatedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaConfigUpdatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaConfigUpdatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaConfigUpdatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Валидация на эффективных значениях (minISR ≤ RF, границы §10.3).
        var errors = KafkaCreateValidator.ValidateUpdate(
            command.Request, config.Value.ReplicationFactor, config.Value.MinInSyncReplicas);
        if (errors.Count > 0)
            return Result<KafkaConfigUpdatedDto>.Failed(new KafkaValidationException(errors));

        // RMW-txn: compare mod_revision == прочитанной + put канонического JSON
        // (state сохраняется — его нет у Active).
        var updated = config.Value.With(command.Request);
        var txn = await gateway.TxnAsync(
            endpoint,
            [TxnCompare.ByModRevision(KafkaCommandHelpers.ConfigKey(cluster), config.Revision!.Value)],
            [new KvPut(KafkaCommandHelpers.ConfigKey(cluster), updated.Serialize())],
            ct);
        if (!txn.IsSuccess)
            return Result<KafkaConfigUpdatedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<KafkaConfigUpdatedDto>.Failed(new KafkaConcurrentWriteException(KafkaCommandHelpers.ConfigKey(cluster)));

        return Result<KafkaConfigUpdatedDto>.Success(new KafkaConfigUpdatedDto(
            cluster, updated.ReplicationFactor, updated.MinInSyncReplicas,
            updated.DefaultPartitions, updated.DefaultRetentionMs));
    }
}

// ===== 4. Добавление брокера — клэйм-txn version(state)==0 (arch/02 §10.2-4) =====

public sealed record AddKafkaBrokerCommand(string Cluster, AddKafkaBrokerRequest Request)
    : ICommand<KafkaBrokerAddedDto>;

public sealed record KafkaBrokerAddedDto(
    string Cluster, string Name, string Cpu, string MemGi, string DiskGi, string State);

public sealed class KafkaBrokerNameTakenException(string name)
    : Exception($"брокер {name} уже заявлен (state-ключ присутствует)");

public sealed class KafkaBrokerLimitException()
    : Exception("достигнут предел 9 брокеров");

[InjectAsScoped]
public sealed class AddKafkaBrokerCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<AddKafkaBrokerCommand, KafkaBrokerAddedDto>
{
    public async ValueTask<Result<KafkaBrokerAddedDto>> Handle(
        AddKafkaBrokerCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;
        var request = command.Request;

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaBrokerAddedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaBrokerAddedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaBrokerAddedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Имя генерит сервер: broker<max+1> по фактическим брокерам префикса
        // (снапшот отстаёт — читаем диапазон напрямую), ≤ 9.
        var brokers = await KafkaCommandHelpers.ReadBrokerNamesAsync(gateway, endpoint, cluster, ct);
        if (!brokers.IsSuccess)
            return Result<KafkaBrokerAddedDto>.Failed(new EtcdWriteUnavailableException());
        var next = (brokers.Value.Count == 0 ? 0 : brokers.Value.Max()) + 1;
        if (next > KafkaLimits.MaxBrokers)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaBrokerLimitException());
        var name = $"broker{next}";

        // Валидация ресурсов (границы §10.3; дефолты 2/2/20).
        var cpu = KafkaClusterCreatePlan.Canonical(request.Cpu ?? KafkaLimits.DefCpu);
        var memGi = request.MemGi ?? KafkaLimits.DefMemGi;
        var diskGi = request.DiskGi ?? KafkaLimits.DefDiskGi;
        var errors = new List<ValidationError>();
        if ((request.Cpu ?? KafkaLimits.DefCpu) < KafkaLimits.MinCpu
            || (request.Cpu ?? KafkaLimits.DefCpu) > KafkaLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {KafkaLimits.MinCpu}..{KafkaLimits.MaxCpu} ядер"));
        if (memGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("memGi", $"memGi: {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        if (diskGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        if (errors.Count > 0)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaValidationException(errors));

        // Клэйм-txn: compare version(brokers/<b>/state)==0 + put NOT_INITIALIZED.
        var stateKey = KafkaCommandHelpers.BrokerKey(cluster, name, "state");
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(stateKey, 0)],
            [new KvPut(stateKey, "NOT_INITIALIZED")], ct);
        if (!claim.IsSuccess)
            return Result<KafkaBrokerAddedDto>.Failed(new EtcdWriteUnavailableException());
        if (!claim.Value.Succeeded)
            return Result<KafkaBrokerAddedDto>.Failed(new KafkaBrokerNameTakenException(name));

        // PUT resources; сбой → компенсация точечным del state (образец pg §9.5).
        var resourcesPut = await gateway.PutAsync(
            endpoint,
            KafkaCommandHelpers.BrokerKey(cluster, name, "resources"),
            System.Text.Json.JsonSerializer.Serialize(
                new ResourcesJson(cpu, $"{memGi}Gi", $"{diskGi}Gi")), ct);
        if (!resourcesPut.IsSuccess)
        {
            await gateway.DeleteAsync(endpoint, stateKey, prefix: false, ct);
            return Result<KafkaBrokerAddedDto>.Failed(new EtcdWriteUnavailableException());
        }

        return Result<KafkaBrokerAddedDto>.Success(new KafkaBrokerAddedDto(
            cluster, name, cpu, $"{memGi}Gi", $"{diskGi}Gi", "NOT_INITIALIZED"));
    }
}

// ===== 5. Удаление брокера — маркер TO_REMOVE (arch/02 §10.2-5) =====

public sealed record RemoveKafkaBrokerCommand(string Cluster, string Broker)
    : ICommand<KafkaBrokerRemovedDto>;

public sealed record KafkaBrokerRemovedDto(string Cluster, string Broker);

public sealed class KafkaBrokerNotFoundException(string cluster, string broker)
    : Exception($"брокер {broker} kafka-кластера {cluster} не найден");

public sealed class KafkaBrokerIsControllerException(string cluster, string broker)
    : Exception($"брокер {broker} — controller-нода кластера {cluster}, демонтаж запрещён (роль фиксируется навсегда)");

public sealed class KafkaLastBrokerException(string cluster)
    : Exception($"нельзя снять последний брокер кластера {cluster}");

[InjectAsScoped]
public sealed partial class RemoveKafkaBrokerCommandHandler(
    ISnapshotStore store,
    IEtcdGateway gateway) : ICommandHandler<RemoveKafkaBrokerCommand, KafkaBrokerRemovedDto>
{
    public async ValueTask<Result<KafkaBrokerRemovedDto>> Handle(
        RemoveKafkaBrokerCommand command, CancellationToken ct)
    {
        var (cluster, broker) = (command.Cluster, command.Broker);

        // Имя брокера каноническое (иначе 404 — панель такие не создаёт).
        if (!BrokerPattern().IsMatch(broker))
            return Result<KafkaBrokerRemovedDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaBrokerRemovedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaBrokerRemovedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaBrokerRemovedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaBrokerRemovedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Guard'ы по свежим данным префикса (роль/число живых брокеров; guard
        // «на брокере есть партиции» панель НЕ проверяет — фактических реплик в
        // etcd нет, авторитетная проверка — воркер, arch/02 §10.2-5).
        var role = await KafkaCommandHelpers.ReadKeyAsync(gateway, endpoint, KafkaCommandHelpers.BrokerKey(cluster, broker, "role"), ct);
        if (!role.IsSuccess)
            return Result<KafkaBrokerRemovedDto>.Failed(new EtcdWriteUnavailableException());
        if (role.Value == "controller")
            return Result<KafkaBrokerRemovedDto>.Failed(new KafkaBrokerIsControllerException(cluster, broker));

        var brokers = await KafkaCommandHelpers.ReadBrokerNamesAsync(gateway, endpoint, cluster, ct);
        if (!brokers.IsSuccess)
            return Result<KafkaBrokerRemovedDto>.Failed(new EtcdWriteUnavailableException());
        if (!brokers.Value.Contains(ParseBrokerId(broker)))
            return Result<KafkaBrokerRemovedDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));
        if (brokers.Value.Count <= 1)
            return Result<KafkaBrokerRemovedDto>.Failed(new KafkaLastBrokerException(cluster));

        // Маркер one-way; идемпотентен (повтор PUT того же значения безвреден).
        var marked = await gateway.PutAsync(
            endpoint, KafkaCommandHelpers.BrokerKey(cluster, broker, "state"), "TO_REMOVE", ct);
        if (!marked.IsSuccess)
            return Result<KafkaBrokerRemovedDto>.Failed(new EtcdWriteUnavailableException());

        return Result<KafkaBrokerRemovedDto>.Success(new KafkaBrokerRemovedDto(cluster, broker));
    }

    [GeneratedRegex("^broker[1-9]$")]
    private static partial Regex BrokerPattern();

    private static int ParseBrokerId(string broker)
        => int.TryParse(broker["broker".Length..], out var id) ? id : 0;
}

// ===== 6. Ротация app-пароля — клэйм-txn заявки (arch/02 §10.2-8) =====

public sealed record RotateKafkaPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaPasswordRotatedDto>;

public sealed record KafkaPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

public sealed class KafkaRotationAlreadyRequestedException(string cluster)
    : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения");

[InjectAsScoped]
public sealed class RotateKafkaPasswordCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<RotateKafkaPasswordCommand, KafkaPasswordRotatedDto>
{
    public async ValueTask<Result<KafkaPasswordRotatedDto>> Handle(
        RotateKafkaPasswordCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (панель не перезаписывает; после исполнения ключ исчезает).
        var key = $"/kafkaworker/rotations/{cluster}";
        var ticket = await KafkaCommandHelpers.ReadKeyAsync(gateway, endpoint, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());
        if (ticket.Value is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare version==0 + put (pg §9.8 один в один).
        var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var txn = await gateway.TxnAsync(
            endpoint, [new TxnCompare(key, 0)],
            [new KvPut(key, new KafkaRotationTicketJson(requestedUnix, command.RequestedBy).Serialize())], ct);
        if (!txn.IsSuccess)
            return Result<KafkaPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        return Result<KafkaPasswordRotatedDto>.Success(
            new KafkaPasswordRotatedDto(cluster, requestedUnix, command.RequestedBy));
    }
}

// ===== Общие хелперы чтения (config/ключи напрямую у etcd — снапшот отстаёт) =====

internal static class KafkaCommandHelpers
{
    // Чтение config-ключа с revision: (значение, mod_revision) — для RMW-мутаций.
    internal sealed record ConfigRead(KafkaConfigJson? Value, long? Revision, Exception? Error);

    internal static async Task<ConfigRead> ReadConfigAsync(
        IEtcdGateway gateway, string endpoint, string cluster, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, ConfigKey(cluster), ct);
        if (!range.IsSuccess)
            return new ConfigRead(null, null, new EtcdWriteUnavailableException());

        var kv = range.Value.FirstOrDefault(k => k.Key == ConfigKey(cluster));
        if (kv is null)
            return new ConfigRead(null, null, null);
        var config = KafkaConfigJson.TryParse(kv.Value);
        return config is null
            ? new ConfigRead(null, null, new InvalidKafkaConfigException(cluster))
            : new ConfigRead(config, (long)kv.ModRevision, null);
    }

    internal static async Task<Result<IReadOnlyList<int>>> ReadBrokerNamesAsync(
        IEtcdGateway gateway, string endpoint, string cluster, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, $"/kafka/clusters/{cluster}/brokers/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<int>>.Failed(range.Error!);

        var ids = new HashSet<int>();
        foreach (var kv in range.Value)
        {
            // /kafka/clusters/<C>/brokers/broker<k>/{state,role,resources}:
            // ["", "kafka", "clusters", <C>, "brokers", "broker<k>", leaf].
            var segments = kv.Key.Split('/');
            if (segments.Length == 7 && segments[5].StartsWith("broker", StringComparison.Ordinal)
                && int.TryParse(segments[5]["broker".Length..], out var id))
                ids.Add(id);
        }

        return Result<IReadOnlyList<int>>.Success((IReadOnlyList<int>)ids.OrderBy(i => i).ToList());
    }

    internal static async Task<Result<string?>> ReadKeyAsync(
        IEtcdGateway gateway, string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    // Активный endpoint — из общего снапшота панели (pg-цикл выбирает/ротирует
    // его в Etcd.ActiveEndpoint по тем же EtcdOptions; kafka-мутации пишут туда же,
    // отдельного выбора у kafka-снапшота нет — симметрия pg-команд).
    internal static string? ActiveEndpoint(ISnapshotStore store)
        => store.Current?.Etcd.ActiveEndpoint;

    internal static string ConfigKey(string cluster) => $"/kafka/clusters/{cluster}/config";

    internal static string BrokerKey(string cluster, string broker, string leaf)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/{leaf}";
}
