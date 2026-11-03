# E. ClusterConfigConverger

Уровень 4. Конвергенция dynamic broker configs и ACL роли app без
рестартов. Функция — [02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §5;
ACL-канон — [04-security.md](../04-security.md) §2.

Active-ветка, лёгкий: describe dynamic broker configs (по одному брокеру) vs
`config.{default_*}` → при отличии AlterBrokerConfigs на всех брокерах
(идемпотентный Set; применяется **без рестартов**). Маппинг полей заявки →
Kafka-конфиги: `default_retention_ms`→`log.retention.ms`,
`default_partitions`→`num.partitions`,
`replication_factor`→`default.replication.factor`,
`min_insync_replicas`→`min.insync.replicas`. Фактические топики не
трогаются (только desired-заявками, [D](d-topic-sync.md)).

**ACL-converge (тот же лёгкий шаг тика)**: DescribeAcls → diff к
каноническому ACL-плану роли app ([04-security.md](../04-security.md) §2) →
CreateAcls/DeleteAcls (идемпотентная сходимость; admin/inter — super.users,
ACL для них не нужны). Авторизатор StandardAuthorizer включён env нод
([01-placement.md](../01-placement.md) §2); deny-by-default — app-клиенты
получают доступ только после первого converge (секунды после подъёма;
снапшот ACL живёт в KRaft-метаданных — повторные converge почти всегда
no-op).
