# Трек: stand (dev-стенд, e2e, поставка)

Контекст: [../04-local-stand.md](../04-local-stand.md).

## Задачи

- `t10-dev-stand` ← `t06-ha-api` — собственный docker dev-стенд + e2e.
  `dev-stand/`: compose (профили quick/full по доке 04), `seed.sh`
  (идемпотентный сид контроль-плейна: кластер demo, 16 бакетов, статусы
  переездов-аномалий, heals, два `/service/`-scope'а), PG-шарды s1a/s1b,
  s2a/s2b (postgres:18, реплики, `wal_level=logical`, trust), patroni-эмуляторы
  `hc*` (REST `:8008` `/cluster`,`/primary`,`/replica`; master-lease
  `/clusters/demo/shards/X/master` TTL 5 c; регистрация members/nodes).
  Скрипты проверок `checks/00-up.sh`, `10-smoke-api.sh`, `20-alerts.sh`,
  `30-failover.sh` (stop мастера → `shard-no-master` → promote → алерт гаснет),
  `40-live-probes.sh`, `90-down.sh` — стиль `../pg/arch/stand/checks`.
  Результат: полный e2e-прогон против работающей панели зелёный.
