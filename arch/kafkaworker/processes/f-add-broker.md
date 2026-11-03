# F. AddBrokerProcess

Уровень 4. Подъём broker-only ноды в Active-кластере. Функция —
[03-broker-scaling.md](../03-broker-scaling.md) §1; модель —
[01-placement.md](../01-placement.md).

`brokers/<b>/state=NOT_INITIALIZED` у Active-кластера: план (host/порт;
`role=broker`) (добор портов — под глобальным portalloc-клэймом
[01-placement.md](../01-placement.md) §1; не взял → journal
waiting-portalloc-lock) → контейнер (env: `QUORUM_VOTERS` уже зафиксирован —
нода подключается к кворуму) → ждать появления в DescribeCluster → RMW
`endpoints` (добавить адрес) → `state=RUNNING`. Уже RUNNING → no-op.
