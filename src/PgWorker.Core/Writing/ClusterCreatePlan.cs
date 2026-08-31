using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Core.Writing;

// План ключей одного создания (arch/02 §9.1): чистая функция запрос+время → ключи.
// Вызывается ТОЛЬКО после CreateClusterValidator (невалидный запрос здесь не проверяется).
// Перенос из AdminPanel.Etcd.Writing (task etcd-via-worker-api): KvPut → нейтральный
// PlanPut (клиентские txn-типы строит хендлер API, план от них не зависит).
public sealed record ClusterCreatePlan(
    string ConfigKey,
    string ConfigValue,
    IReadOnlyList<PlanPut> Puts,         // всё кроме config (пакет PUT после клэйма)
    IReadOnlyList<string> RequestKeys,   // компенсация: точечные del (пространство Patroni не трогаем)
    string CanonicalCpu,
    string CanonicalMem,
    string CanonicalDisk)
{
    public const string NotInitialized = "NOT_INITIALIZED";

    public static ClusterCreatePlan Build(CreateClusterRequest request, long nowUnix)
    {
        var cpu = CreateClusterValidator.CanonicalCpu(request.RequestCpu);
        var mem = CreateClusterValidator.CanonicalGiB(request.RequestMem);
        var disk = CreateClusterValidator.CanonicalGiB(request.RequestDisk);

        var config = new ConfigJson(request.Buckets, request.Name, nowUnix, NotInitialized);
        var puts = new List<PlanPut>();
        var requestKeys = new List<string>();

        for (var s = 0; s < request.Shards; s++)
        {
            var shard = $"shard{s + 1}";
            puts.Add(new($"/clusters/{request.Name}/shards/{shard}/replicas",
                request.Replicas.ToString()));
            for (var r = 0; r < request.Replicas; r++)
                puts.Add(new(
                    $"/clusters/{request.Name}/shards/{shard}/nodes/{shard}{(char)('a' + r)}/state",
                    NotInitialized));

            // Заявка ресурсов на КАЖДУЮ ноду scope (arch/02 §9.1)
            foreach (var (leaf, value) in new[]
                     {
                         ("request_cpu", cpu), ("request_mem", mem), ("request_disk", disk),
                     })
            {
                var key = $"/service/{request.Name}-{shard}/{leaf}";
                puts.Add(new(key, value));
                requestKeys.Add(key);
            }
        }

        for (var i = 0; i < request.Buckets; i++)
        {
            // владелец — непрерывный блок: канон arch/02 §9.1.1
            var owner = $"shard{OwnerShard(i, request.Buckets, request.Shards)}";
            puts.Add(new($"/clusters/{request.Name}/buckets/routing/bucket_{i}", owner));
            puts.Add(new(
                $"/clusters/{request.Name}/buckets/status/bucket_{i}",
                JsonSerializer.Serialize(new StatusJson($"bucket_{i}", NotInitialized, owner, nowUnix))));
        }

        puts.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key)); // детерминированный порядок
        return new ClusterCreatePlan(
            $"/clusters/{request.Name}/config",
            JsonSerializer.Serialize(config),
            puts,
            requestKeys,
            cpu,
            mem,
            disk);
    }

    // Распределение бакетов непрерывными блоками — «бакет к ближайшему центру
    // отрезка» (arch/02 §9.1.1): floor((2·i+1)·S/(2·N)); канон 10×3 → 3+4+3
    // (остаток — среднему шарду). Целочисленно, без float:
    // max (2·8191+1)·128 = 2 097 024 — переполнение int исключено.
    public static int OwnerShard(int bucket, int buckets, int shards)
        => (2 * bucket + 1) * shards / (2 * buckets) + 1;

    // config-JSON: имена полей — канон init-cluster.sh (snake_case).
    private sealed record ConfigJson(
        [property: JsonPropertyName("buckets")] int Buckets,
        [property: JsonPropertyName("dbname")] string DbName,
        [property: JsonPropertyName("created_unix")] long CreatedUnix,
        [property: JsonPropertyName("state")] string State);

    // Статус-ключ бакета: без target/started_unix/phase — это не переезд (arch/02 §2.1).
    private sealed record StatusJson(
        [property: JsonPropertyName("bucket")] string Bucket,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("owner")] string Owner,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix);
}
