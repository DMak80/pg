using System.Globalization;
using System.Text.RegularExpressions;

namespace AdminPanel.Etcd.Writing;

// Тело POST /api/clusters (arch/03 §1.1): биндится Minimal API как JSON.
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk);

// Ошибка валидации одного поля (ProblemDetails errors, arch/03 §1.1).
public sealed record ValidationError(string Field, string Message);

// Границы создания кластера — arch/02 §9.3; константы кода, не конфиг (spec t12 §8.15).
public static partial class CreateClusterLimits
{
    // Без дефиса: scope <C>-<X> и ScopeMatcher однозначны; dbname = <C> (spec t12 §8.5).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    public static partial Regex NamePattern();

    public const int MinBuckets = 1;
    public const int MaxBuckets = 8192;
    public const int MinShards = 1;
    public const int MaxShards = 128;
    public const int MinReplicas = 1;   // 1 = только мастер <X>a (spec t12 §8.4)
    public const int MaxReplicas = 26;  // буквы нод a..z
    public const decimal MinCpu = 0.01m;
    public const decimal MaxCpu = 64m;
    public const int MinGiB = 1;
    public const int MaxGiB = 65536;
}

// Чистая функция валидации: сервер — источник истины (spec t12 §2).
public static class CreateClusterValidator
{
    public static IReadOnlyList<ValidationError> Validate(CreateClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (!CreateClusterLimits.NamePattern().IsMatch(request.Name ?? ""))
            errors.Add(new("name", "имя: ^[a-z][a-z0-9_]{0,62}$ (без дефиса)"));
        if (request.Buckets is < CreateClusterLimits.MinBuckets or > CreateClusterLimits.MaxBuckets)
            errors.Add(new("buckets", $"бакеты: целое {CreateClusterLimits.MinBuckets}..{CreateClusterLimits.MaxBuckets}"));
        if (request.Shards is < CreateClusterLimits.MinShards or > CreateClusterLimits.MaxShards
            || request.Shards > request.Buckets)
            errors.Add(new("shards", $"шарды: целое {CreateClusterLimits.MinShards}..{CreateClusterLimits.MaxShards} и не больше бакетов"));
        if (request.Replicas is < CreateClusterLimits.MinReplicas or > CreateClusterLimits.MaxReplicas)
            errors.Add(new("replicas", $"реплики: целое {CreateClusterLimits.MinReplicas}..{CreateClusterLimits.MaxReplicas}"));
        if (request.RequestCpu < CreateClusterLimits.MinCpu || request.RequestCpu > CreateClusterLimits.MaxCpu)
            errors.Add(new("requestCpu", $"CPU (ядра): {CreateClusterLimits.MinCpu}..{CreateClusterLimits.MaxCpu}"));
        if (request.RequestMem is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestMem", $"память (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        if (request.RequestDisk is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestDisk", $"диск (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        return errors;
    }

    // Канонические строки etcd (arch/02 §9.1): cpu invariant-десятичное без хвостовых нулей.
    public static string CanonicalCpu(decimal cpu)
        => cpu.ToString("0.########", CultureInfo.InvariantCulture);

    public static string CanonicalGiB(int gib)
        => $"{gib}Gi";
}
