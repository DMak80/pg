# Трек: sharding (инспекция кластеров/шардов/бакетов)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) §2.1,
[../03-panels.md](../03-panels.md) (DTO, алерты).

## Задачи

- `t05-sharding-api` ← `t04-etcd-api` — API шардирования. Эндпоинты
  `GET /api/clusters`, `GET /api/clusters/{cluster}` (config, shards
  c dsn/replicas/master+leaseAlive, buckets c фильтрами `?owner=&state=`,
  heals; `?state=` принимает ACTIVE тоже). Алерты шардирования (03 §4):
  `shard-no-master` (P11), `move-stale`, `move-frozen-long`, `move-aborting`
  (P7), `move-flipped-status-stuck`, `bucket-lost` / `bucket-no-routing` /
  `bucket-out-of-range` (P18/P23). Парсинг DSN (multi-host) и возраста
  переездов из `updated_unix`. Unit: AlertEngine-сценарии (протухший lease,
  зависший FROZEN, routing в никуда, дыра карты), мапперы DTO; integration:
  сид аномалий в Testcontainers-etcd → API отдаёт их, алерты на месте.
