namespace PgWorker.Core.Writing;

// Тело POST /api/clusters/{cluster}/shards (arch/02 §9.5): replicas с дефолтом 2
// обрабатывает handler (JSON-биндинг даёт 0 при отсутствии поля).
// Перенос из AdminPanel.Etcd.Writing (task etcd-via-worker-api), значения 1:1.
public sealed record AddShardRequest(int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk);

// Границы — те же, что создания кластера (arch/02 §9.3; константы кода).
public static class AddShardValidator
{
    public static IReadOnlyList<ValidationError> Validate(AddShardRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Replicas is < CreateClusterLimits.MinReplicas or > CreateClusterLimits.MaxReplicas)
            errors.Add(new("replicas",
                $"реплики: целое {CreateClusterLimits.MinReplicas}..{CreateClusterLimits.MaxReplicas}"));
        if (request.RequestCpu < CreateClusterLimits.MinCpu || request.RequestCpu > CreateClusterLimits.MaxCpu)
            errors.Add(new("requestCpu",
                $"CPU (ядра): {CreateClusterLimits.MinCpu}..{CreateClusterLimits.MaxCpu}"));
        if (request.RequestMem is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestMem",
                $"память (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        if (request.RequestDisk is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestDisk",
                $"диск (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        return errors;
    }
}

// План ключей одного add-shard (arch/02 §9.5): чистая функция — вызывается
// ТОЛЬКО после валидатора (образец ClusterCreatePlan). KvPut → нейтральный PlanPut.
public sealed record ShardScalePlan(
    string ReplicasKey,
    string ReplicasValue,
    IReadOnlyList<PlanPut> Puts,        // nodes state × R + request_* (пакет PUT после клэйма)
    IReadOnlyList<string> RequestKeys,  // компенсация: точечные del
    string CanonicalCpu,
    string CanonicalMem,
    string CanonicalDisk)
{
    public const string NotInitialized = "NOT_INITIALIZED";

    public static ShardScalePlan Build(string cluster, string shard, AddShardRequest request)
    {
        var cpu = CreateClusterValidator.CanonicalCpu(request.RequestCpu);
        var mem = CreateClusterValidator.CanonicalGiB(request.RequestMem);
        var disk = CreateClusterValidator.CanonicalGiB(request.RequestDisk);

        var puts = new List<PlanPut>();
        for (var r = 0; r < request.Replicas; r++)
            puts.Add(new(
                $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{(char)('a' + r)}/state",
                NotInitialized));

        var requestKeys = new List<string>();
        foreach (var (leaf, value) in new[] { ("request_cpu", cpu), ("request_mem", mem), ("request_disk", disk) })
        {
            var key = $"/service/{cluster}-{shard}/{leaf}";
            puts.Add(new(key, value));
            requestKeys.Add(key);
        }

        puts.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        return new ShardScalePlan(
            $"/clusters/{cluster}/shards/{shard}/replicas",
            request.Replicas.ToString(),
            puts,
            requestKeys,
            cpu, mem, disk);
    }
}
