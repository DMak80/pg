using PgWorker.Core.Writing;

namespace PgWorker.Core.Seed;

// Демо-сид pg-контура (arch/14 §1.1.1, task etcd-via-worker-api): перенос
// dev-stand/adminpanel/seed.sh 1:1. Чистые (key, value)-пары: порядок/значения
// каноничны, txn не нужен (скрипт писал плоскими put). Динамические части —
// только now-смещения статусов и created_unix (fixed-константы скрипта — как есть),
// чтобы seeded-аномалии были живыми (StaleMoveSeconds=600 и т.п.).
public sealed record PostgresDemoSeedPlan(long NowUnix)
{
    public IReadOnlyList<PlanPut> Puts { get; } = Build(NowUnix);

    private static IReadOnlyList<PlanPut> Build(long now)
    {
        List<PlanPut> puts =
        [
            // Config (Active-канон: state-поля нет — кластер проинициализирован).
            new PlanPut("/clusters/demo/config",
                $"{{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":{now}}}"),

            // Шарды: dsn/replicas/master (master статично; эмуляторы стенда
            // перепишут с lease — здесь только декларация).
            new PlanPut("/clusters/demo/shards/s1/dsn", "host=s1a,s1b port=5432 dbname=demo user=postgres"),
            new PlanPut("/clusters/demo/shards/s1/replicas", "1"),
            new PlanPut("/clusters/demo/shards/s1/master", "s1a:5432"),
            new PlanPut("/clusters/demo/shards/s2/dsn", "host=s2a,s2b port=5432 dbname=demo user=postgres"),
            new PlanPut("/clusters/demo/shards/s2/replicas", "1"),
            new PlanPut("/clusters/demo/shards/s2/master", "s2a:5432"),

            // Очередь заявок PgWorker (arch/02 §2.3.1): bucket_13 (принадлежит
            // s2) — «увезти на s1»; fixed-unix скрипта.
            new PlanPut("/pgworker/moves/demo/bucket_13",
                """{"op":"move","to":"s1","requested_unix":1755850000,"requested_by":"ops"}"""),

            // Статусы переездов: bucket_3 свежий (SYNCING); 7/11 протухшие
            // (порог StaleMoveSeconds=600); у bucket_7 — last_error.
            new PlanPut("/clusters/demo/buckets/status/bucket_3",
                $$"""{"bucket":"bucket_3","state":"SYNCING","owner":"s1","target":"s2","started_unix":{{now - 120}},"updated_unix":{{now - 60}},"phase":"copy"}"""),
            new PlanPut("/clusters/demo/buckets/status/bucket_7",
                $$"""{"bucket":"bucket_7","state":"ABORTING","owner":"s2","target":"s1","started_unix":{{now - 1000}},"updated_unix":{{now - 900}},"phase":"cleanup","last_error":"receiver went away"}"""),
            new PlanPut("/clusters/demo/buckets/status/bucket_11",
                $$"""{"bucket":"bucket_11","state":"FROZEN","owner":"s1","target":"s2","started_unix":{{now - 7400}},"updated_unix":{{now - 7200}},"phase":"cutover-wait"}"""),

            // Журнал самолечений routing (restore-heal).
            new PlanPut("/clusters/demo/heals/bucket_5",
                $$"""{"bucket":"bucket_5","was":"s2","now":"s1","reason":"restore-heal","ts":{{now - 86400}}}"""),

            // HA-DCS: два скопа; статично (в full-стенде эмуляторы перепишут
            // members/leader/optime с lease).
            new PlanPut("/service/demo-s1/optime/leader", "738273634528"),
            new PlanPut("/service/demo-s1/initialize", "738273612345678"),
            new PlanPut("/service/demo-s1/config", """{"ttl":5,"loop_wait":2,"retry_timeout":3}"""),
            new PlanPut("/service/demo-s2/optime/leader", "738273634001"),
            new PlanPut("/service/demo-s2/initialize", "738273611234567"),
            new PlanPut("/service/demo-s2/config", """{"ttl":5,"loop_wait":2,"retry_timeout":3}"""),

            // Стендовая топология (в full эмуляторы перепишут реальными IP).
            new PlanPut("/cluster/nodes/s1a", "172.28.0.11"),
            new PlanPut("/cluster/nodes/s1b", "172.28.0.12"),
            new PlanPut("/cluster/nodes/s2a", "172.28.0.21"),
            new PlanPut("/cluster/nodes/s2b", "172.28.0.22"),
        ];

        // Routing 16 бакетов фикс-раскладкой EtcdSeed: s1=10, s2=6.
        foreach (var b in new[] { 0, 2, 3, 4, 6, 8, 10, 11, 12, 14 })
            puts.Add(new PlanPut($"/clusters/demo/buckets/routing/bucket_{b}", "s1"));
        foreach (var b in new[] { 1, 5, 7, 9, 13, 15 })
            puts.Add(new PlanPut($"/clusters/demo/buckets/routing/bucket_{b}", "s2"));

        // HA-скопы: leader + members (master/replica) — цикл по образцу скрипта.
        foreach (var s in new[] { "s1", "s2" })
        {
            var (a, b) = ($"{s}a", $"{s}b");
            puts.Add(new PlanPut($"/service/demo-{s}/leader", $$"""{"name":"{{a}}"}"""));
            puts.Add(new PlanPut($"/service/demo-{s}/members/{a}",
                $$"""{"name":"{{a}}","conn_url":"{{a}}:5432","role":"master","state":"running","timeline":1,"lag":0}"""));
            puts.Add(new PlanPut($"/service/demo-{s}/members/{b}",
                $$"""{"name":"{{b}}","conn_url":"{{b}}:5432","role":"replica","state":"streaming","timeline":1,"lag":0}"""));
        }

        return puts;
    }
}
