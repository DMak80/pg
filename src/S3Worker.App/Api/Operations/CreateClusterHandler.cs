using System.Text.Json;
using System.Text.Json.Serialization;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// Ответ 201 POST /api/s3/clusters (arch/24 §1.1).
public sealed record S3ClusterCreatedDto(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("nodes")] int Nodes,
    [property: JsonPropertyName("secretProfile")] string SecretProfile);

// Создание S3-кластера через API воркера: валидация (400) → профиль (422) →
// клэйм-txn put-if-absent config (state=NOT_INITIALIZED; 409) → ОДНА txn
// пакета nodes/node<k>/{placement,resources,state} → сбой midway — компенсация
// del --prefix (§9.1-образец). Создание клэйм-lease НЕ берёт — это заявка.
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
        var nodes = request.Nodes!.Value;
        var profile = request.SecretProfile!.Trim();

        // 2) Профиль секретов — по факту env воркера (422, перечень имён).
        if (!secrets.ProfileNames.Contains(profile, StringComparer.Ordinal))
            return Result<S3ClusterCreatedDto>.Failed(new S3SecretProfileUnavailableException(secrets.ProfileNames));

        // 3) Клэйм имени одной txn: NotExists config + put config + пакет нод.
        var created = time.GetUtcNow().ToUnixTimeSeconds();
        var configValue = S3ProcessCommon.SerializeCanonicalConfig(
            S3Limits.Platform, nodes, profile, created, "NOT_INITIALIZED");
        var configKey = S3ApiHelpers.ConfigKey(name);
        var ops = new List<TxnOp> { new TxnOp.Put(configKey, configValue, null) };
        for (var k = 1; k <= nodes; k++)
        {
            var node = $"node{k}";
            var placement = request.Placements![k - 1];
            ops.Add(new TxnOp.Put(S3ApiHelpers.NodeKey(name, node, "placement"), placement.ToJson(), null));
            ops.Add(new TxnOp.Put(S3ApiHelpers.NodeKey(name, node, "resources"), S3Limits.ResourcesJson(request.Resources ?? new(null, null, null)), null));
            ops.Add(new TxnOp.Put(S3ApiHelpers.NodeKey(name, node, "state"), "NOT_INITIALIZED", null));
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
            new S3ClusterCreatedDto(name, "NOT_INITIALIZED", nodes, profile));
    }
}

// Валидатор создания (чистая функция; spec §6: name/platform/nodes/placements/
// resources; профиль — 422 в хендлере по факту env).
public static class S3ClusterValidator
{
    public static List<ValidationError> Validate(CreateS3ClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Name is null || !S3Limits.ClusterPattern().IsMatch(request.Name))
            errors.Add(new("name", "name: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)"));

        if (!string.Equals(request.Platform, S3Limits.Platform, StringComparison.Ordinal))
            errors.Add(new("platform", $"platform: только \"{S3Limits.Platform}\" (arch/23 §2)"));

        var nodes = request.Nodes ?? S3Limits.MinNodes;
        if (nodes is < S3Limits.MinNodes or > S3Limits.MaxNodes)
            errors.Add(new("nodes", $"nodes: {S3Limits.MinNodes}..{S3Limits.MaxNodes}"));

        var placements = request.Placements;
        if (placements is null || placements.Count != nodes)
        {
            errors.Add(new("placements", $"placements: ровно {nodes} (по одному на ноду)"));
        }
        else
        {
            for (var i = 0; i < placements.Count; i++)
            {
                if (placements[i].IsEmpty)
                    errors.Add(new("placements", $"placements[{i}]: минимум один атрибут (dockerHost|swarmNode|swarmLabels)"));
            }
        }

        ValidateResources(request.Resources, errors);
        if (string.IsNullOrWhiteSpace(request.SecretProfile))
            errors.Add(new("secretProfile", "secretProfile: непустое имя env-профиля (arch/23 §6)"));

        return errors;
    }

    // Ресурсы (создание/мутация): cpu 0.01..64, memGi/diskGi 1..65536.
    internal static void ValidateResources(S3ResourcesDto? resources, List<ValidationError> errors)
    {
        var cpu = resources?.Cpu ?? 1m;
        if (cpu is < S3Limits.MinCpu or > S3Limits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {S3Limits.MinCpu}..{S3Limits.MaxCpu} ядер"));
        var memGi = resources?.MemGi ?? S3Limits.MinGiB;
        if (memGi is < S3Limits.MinGiB or > S3Limits.MaxGiB)
            errors.Add(new("memGi", $"memGi: {S3Limits.MinGiB}..{S3Limits.MaxGiB}"));
        var diskGi = resources?.DiskGi ?? 10;
        if (diskGi is < S3Limits.MinGiB or > S3Limits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: {S3Limits.MinGiB}..{S3Limits.MaxGiB}"));
    }
}
