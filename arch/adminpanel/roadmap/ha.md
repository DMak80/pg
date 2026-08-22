# Трек: ha (HA-кластеры, live-пробы, HA-алерты)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) §2.2, §6,
[../03-panels.md](../03-panels.md) §5 (SQL-каталог).

## Задачи

- `t06-ha-api` ← `t04-etcd-api` — HA-модель, live-пробы, HA-алерты.
  `AdminPanel.Probes`: (а) Patroni REST `:8008` `GET /cluster` по каждому
  member-хосту scope'а (timeout, тег Application Name); (б) SQL-проба Npgsql
  по DSN из etcd + пароль из `AdminPanel:Probes:Password`,
  `TargetSessionAttributes=ReadWrite`, `default_transaction_read_only=on`,
  запросы каталога 03 §5 (pg_stat_replication, pg_replication_slots,
  pg_stat_subscription, inventory `bucket_%`, pg_is_in_recovery);
  пробы — отдельный редкий тик, результат обогащает снапшот, ошибки не ронят
  etcd-данные (`Probes[]`). Связка scope→(cluster,shard) по префиксу
  `<C>-`. Эндпоинты `GET /api/ha`, `GET /api/ha/{scope}`. Алерты:
  `shard-no-leader`, `ha-member-not-streaming`, `replica-lag-high`,
  `slot-lag-high`, `slot-wal-lost`, `slot-invalidation-risk`,
  `sync-standby-missing` (P8), `inventory-mismatch` (P21/P23), `probe-failed`.
  Unit: парсеры Patroni-JSON, SQL-результатов, AlertEngine-сценарии;
  integration: Testcontainers postgres:18 (+ etcd с сидом) → пробы живые,
  алерты на лагах/слотах воспроизводятся.
