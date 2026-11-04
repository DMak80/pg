# Runbook — эксплуатация PgWorker

Документ переработан и перенесён в `arch/` (канон):

- общие процедуры — [arch/operations/README.md](../arch/operations/README.md):
  вторые инстансы воркеров, внешние образы и registry, серт API воркера,
  запуск docker-E2E серий;
- потеря WAL-слота — [arch/backups/09-operability.md](../arch/backups/09-operability.md) §7;
- PgTune-параметры — [arch/pgworker/09-pgtune.md](../arch/pgworker/09-pgtune.md) §9;
- watchdog воркера — [arch/pgworker/08-operability.md](../arch/pgworker/08-operability.md) §3;
- HA-etcd контур и восстановление etcd —
  [arch/etcdworker/06-operability.md](../arch/etcdworker/06-operability.md) §7–§8;
- TLS/CA Valkey — [arch/valkeyworker/06-operability.md](../arch/valkeyworker/06-operability.md) §7;
- REST-TLS Patroni и бэкап `deploy/tls` —
  [arch/pgworker/08-operability.md](../arch/pgworker/08-operability.md) §7.

Правила тестовых прогонов и изоляции окружений перенесены в канон:
[arch/testing/README.md](../arch/testing/README.md) ([01-e2e-launch.md](../arch/testing/01-e2e-launch.md),
[02-e2e-isolation.md](../arch/testing/02-e2e-isolation.md)).
