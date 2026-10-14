using System.Text.Json;
using System.Text.Json.Serialization;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// Ответ 201 POST /api/s3/clusters (arch/24 §1.1): instances — имена созданных
// инстансов (0 инстансов — валидная декларация без развёртывания, arch/23 §2).
public sealed record S3ClusterCreatedDto(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("nodes")] int Nodes,
    [property: JsonPropertyName("secretProfile")] string SecretProfile,
    [property: JsonPropertyName("instances")] IReadOnlyList<string> Instances);

// Создание S3-кластера через API воркера: валидация (400) → профиль (422) →
// клэйм-txn NotExists(config) + пакет (config c nodes=len(instances) +
// instances × {placement, resources, state=NOT_INITIALIZED}; 0 инстансов —
// только config; 409) → сбой midway — компенсация del --prefix (§9.1-образец).
// Создание клэйм-lease НЕ берёт — это заявка.
public sealed class CreateClusterHandler(
    IEtcdGateway gateway, string[] endpoints, SecretProfileStore secrets, TimeProvider time)
{
    public async Task<Result<S3ClusterCreatedDto>> HandleAsync(CreateS3ClusterRequest request, CancellationToken ct)
    {
        // 1) Валидация (сервер — источник истины).
        var errors = S3ClusterValidator.Validate(request);
        if (errors.Count > 0)
            return Result<S3ClusterCreatedDto>.Failed(new S3ValidationException(errors));

        var name = request.Name!;
        var instances = request.Instances ?? [];
        var profile = request.SecretProfile!.Trim();

        // 2) Профиль секретов — по факту env воркера (422, перечень имён).
        if (!secrets.ProfileNames.Contains(profile, StringComparer.Ordinal))
            return Result<S3ClusterCreatedDto>.Failed(new S3SecretProfileUnavailableException(secrets.ProfileNames));

        // 3) Коллизия имён инстансов в кластере (409): txn-сравнение ведёт только
        // config, состав проверяем фактическим чтением (гонка закрывается
        // атомарностью клэйм-txn имени кластера).
        var existing = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, name, ct);
        if (!existing.IsSuccess)
            return Result<S3ClusterCreatedDto>.Failed(existing.Error!);
        var collision = instances.FirstOrDefault(i => existing.Value.Contains(i.Name!, StringComparer.Ordinal));
        if (collision is not null)
            return Result<S3ClusterCreatedDto>.Failed(new S3NodeAlreadyExistsException(name, collision.Name!));

        // 4) Клэйм имени одной txn: NotExists config + put config + пакет инстансов.
        var created = time.GetUtcNow().ToUnixTimeSeconds();
        var configValue = S3ProcessCommon.SerializeCanonicalConfig(
            S3Limits.Platform, instances.Count, profile, created, "NOT_INITIALIZED");
        var configKey = S3ApiHelpers.ConfigKey(name);
        var ops = new List<TxnOp> { new TxnOp.Put(configKey, configValue, null) };
        foreach (var instance in instances)
        {
            ops.Add(new TxnOp.Put(
                S3ApiHelpers.NodeKey(name, instance.Name!, "placement"), instance.Placement!.ToJson(), null));
            ops.Add(new TxnOp.Put(
                S3ApiHelpers.NodeKey(name, instance.Name!, "resources"),
                S3Limits.ResourcesJson(instance.Resources ?? request.Resources ?? new(null, null, null)), null));
            ops.Add(new TxnOp.Put(
                S3ApiHelpers.NodeKey(name, instance.Name!, "state"), "NOT_INITIALIZED", null));
        }

        var claim = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(configKey)], ops),
            ct));
        if (!claim.IsSuccess)
            return Result<S3ClusterCreatedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<S3ClusterCreatedDto>.Failed(new S3ClusterAlreadyExistsException(name));

        return Result<S3ClusterCreatedDto>.Success(
            new S3ClusterCreatedDto(name, "NOT_INITIALIZED", instances.Count, profile,
                instances.Select(i => i.Name!).ToList()));
    }
}

// Валидатор создания (чистая функция; arch/24 §1.1: name/platform/instances 0..9
// c именами паттерна кластера и обязательным placement; ресурсы — per-instance
// с fallback на общий; профиль — 422 в хендлере по факту env).
public static class S3ClusterValidator
{
    public static List<ValidationError> Validate(CreateS3ClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Name is null || !S3Limits.ClusterPattern().IsMatch(request.Name))
            errors.Add(new("name", "name: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)"));

        if (!string.Equals(request.Platform, S3Limits.Platform, StringComparison.Ordinal))
            errors.Add(new("platform", $"platform: только \"{S3Limits.Platform}\" (arch/23 §2)"));

        var instances = request.Instances ?? [];
        if (instances.Count > S3Limits.MaxNodes)
            errors.Add(new("instances", $"instances: 0..{S3Limits.MaxNodes} (арх.23 §2)"));

        // Имена: паттерн + не резерв + уникальность в теле; placement обязателен
        // каждому; ресурсы — per-instance (диапазоны — прежние).
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < instances.Count; i++)
        {
            var instance = instances[i];
            if (instance.Name is null || !S3Limits.InstanceNamePattern().IsMatch(instance.Name))
                errors.Add(new($"instances[{i}].name", "name: [a-z][a-z0-9_]{0,62} (паттерн кластера)"));
            else if (string.Equals(instance.Name, S3Limits.ReservedInstanceName, StringComparison.Ordinal))
                errors.Add(new($"instances[{i}].name", $"«{S3Limits.ReservedInstanceName}» зарезервировано (PKI оркестратора)"));
            else if (!seen.Add(instance.Name))
                errors.Add(new($"instances[{i}].name", $"дубль имени «{instance.Name}» в теле"));
            if (instance.Placement is null || instance.Placement.IsEmpty)
                errors.Add(new($"instances[{i}].placement", "placement: минимум один атрибут (dockerHost|swarmNode|swarmLabels)"));
            ValidateResources(instance.Resources, errors, $"instances[{i}].");
        }

        ValidateResources(request.Resources, errors);
        if (string.IsNullOrWhiteSpace(request.SecretProfile))
            errors.Add(new("secretProfile", "secretProfile: непустое имя env-профиля (arch/23 §6)"));

        return errors;
    }

    // Ресурсы (создание/мутация): cpu 0.01..64, memGi/diskGi 1..65536.
    internal static void ValidateResources(S3ResourcesDto? resources, List<ValidationError> errors, string fieldPrefix = "")
    {
        var cpu = resources?.Cpu ?? 1m;
        if (cpu is < S3Limits.MinCpu or > S3Limits.MaxCpu)
            errors.Add(new($"{fieldPrefix}cpu", $"cpu: {S3Limits.MinCpu}..{S3Limits.MaxCpu} ядер"));
        var memGi = resources?.MemGi ?? S3Limits.MinGiB;
        if (memGi is < S3Limits.MinGiB or > S3Limits.MaxGiB)
            errors.Add(new($"{fieldPrefix}memGi", $"memGi: {S3Limits.MinGiB}..{S3Limits.MaxGiB}"));
        var diskGi = resources?.DiskGi ?? 10;
        if (diskGi is < S3Limits.MinGiB or > S3Limits.MaxGiB)
            errors.Add(new($"{fieldPrefix}diskGi", $"diskGi: {S3Limits.MinGiB}..{S3Limits.MaxGiB}"));
    }
}
