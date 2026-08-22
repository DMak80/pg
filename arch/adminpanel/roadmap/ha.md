# Трек: ha (HA-кластеры, live-пробы, HA-алерты)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) §2.2, §6,
[../03-panels.md](../03-panels.md) §5 (SQL-каталог).

## Задачи

- `t90-fix-probe-enrich-flaky` — стабилизация флакающего
  `EtcdSnapshotIntegrationTests.Refresher_EnrichesSnapshot_FromProbeState`
  (занесён при мерже t07: полный integration-прогон падает на
  inventory-mismatch «лишний bucket_0», изолированный запуск и прогон на
  момент t06 — зелёные; порядок тестов в коллекции общего etcd-контейнера
  влияет на сид → isolation/idempotent-seed fix).
