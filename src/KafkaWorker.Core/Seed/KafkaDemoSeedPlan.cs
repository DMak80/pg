using KafkaWorker.Core.Writing;

namespace KafkaWorker.Core.Seed;

// Демо-сид kafka-домена (arch/16 §1.1.1, task etcd-via-worker-api): перенос
// dev-stand/adminpanel/kafka-seed.sh 1:1. РОВНО 2 кластера: events — Active
// (3 брокера RUNNING controller + endpoints + app-креды + topics-архетипы +
// lifecycle-заявки + живые ротация/ребалансировка/reassignment-прогресс);
// pending — NOT_INITIALIZED (config с state + 3 брокера-заявки). TO_REMOVE сид
// НЕ сеет (его создаёт чек 50 шагом DELETE). Чистые (key, value)-пары: txn не
// нужен (скрипт писал плоскими put). Фиксированные unix скрипта — константами;
// now — ТОЛЬКО ротация (как в скрипте: requested_unix=$(date +%s)).
public sealed record KafkaDemoSeedPlan(long NowUnix)
{
    public IReadOnlyList<PlanPut> Puts { get; } = Build(NowUnix);

    private static IReadOnlyList<PlanPut> Build(long now)
    {
        List<PlanPut> puts =
        [
            // --- events: Active (state снят — семантика arch/15 §2.1) ---
            new PlanPut("/kafka/clusters/events/config",
                """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}"""),
            new PlanPut("/kafka/clusters/events/endpoints",
                "host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003"),
            new PlanPut("/kafka/clusters/events/app_user", "app"),
            new PlanPut("/kafka/clusters/events/app_password", "SeEdPaSsWoRd0123456789AbCdEf"),

            // topics: факт без заявки / desired / missing (архетипы arch/15 §3).
            new PlanPut("/kafka/clusters/events/topics/orders",
                """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000","min.insync.replicas":"2"},"synced_unix":1750000100,"missing":false}"""),
            new PlanPut("/kafka/clusters/events/topics/payments",
                """{"partitions":6,"replication_factor":3,"configs":{"retention.ms":"604800000"},"desired":{"partitions":12},"desired_unix":1750000010,"desired_by":"ops","synced_unix":1750000110,"missing":false}"""),
            new PlanPut("/kafka/clusters/events/topics/ghost",
                """{"partitions":3,"replication_factor":1,"configs":{"retention.ms":"604800000"},"desired":{"configs":{"retention.ms":"86400000"}},"desired_unix":1750000200,"desired_by":"admin","synced_unix":1750000300,"missing":true}"""),

            // lifecycle-заявки (t01, arch/15 §3.1): create без факт-ключа +
            // delete на живой orders.
            new PlanPut("/kafka/clusters/events/topics/audit/desired.create",
                """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"86400000"},"requested_unix":1756501200,"requested_by":"seed"}"""),
            new PlanPut("/kafka/clusters/events/topics/orders/desired.delete",
                """{"requested_unix":1756501300,"requested_by":"seed"}"""),

            // Живая заявка ротации (чистится только исполнением/удалением — A10);
            // единственное динамическое время (образец скрипта).
            new PlanPut("/kafkaworker/rotations/events",
                $$"""{"requested_unix":{{now}},"requested_by":"seed"}"""),

            // Ребалансировка (t02): живая заявка + drain-прогресс — парсер/UI/
            // алерты видны без живого воркера (арх/15 §4).
            new PlanPut("/kafkaworker/rebalances/events",
                """{"requested_unix":1756500123,"requested_by":"seed"}"""),
            new PlanPut("/kafkaworker/reassignments/events",
                """{"mode":"drain","drain_broker":"broker2","partitions_total":6,"partitions_remaining":3,"submitted_unix":1756500130,"updated_unix":1756500135,"instance":"seed"}"""),

            // --- pending: заявка NOT_INITIALIZED ---
            new PlanPut("/kafka/clusters/pending/config",
                """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500900,"state":"NOT_INITIALIZED"}"""),
        ];

        // Брокеры: events RUNNING/controller (resources 2/4/40) + pending
        // NOT_INITIALIZED (resources 2/2/20) — циклы скрипта.
        for (var k = 1; k <= 3; k++)
        {
            puts.Add(new PlanPut($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING"));
            puts.Add(new PlanPut($"/kafka/clusters/events/brokers/broker{k}/role", "controller"));
            puts.Add(new PlanPut($"/kafka/clusters/events/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"4Gi","disk":"40Gi"}"""));
            puts.Add(new PlanPut($"/kafka/clusters/pending/brokers/broker{k}/state", "NOT_INITIALIZED"));
            puts.Add(new PlanPut($"/kafka/clusters/pending/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"2Gi","disk":"20Gi"}"""));
        }

        return puts;
    }
}
