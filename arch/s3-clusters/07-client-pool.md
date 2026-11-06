# S3-кластеры: клиентская грань пула

Уровень 3. Как клиенты адресуются к пулу и что входит в их контракт.
Дискавери-ключи — [02-keys.md](02-keys.md) §1 (`endpoints`, `secrets`).

## 1. Пул = набор обычных S3-серверов

- **Запись** — перебор «любой доступный» с per-попыточным таймаутом PUT
  (health-проба недоступность не доказывает — факт 8
  `../../../OwnS3/docs/s3-replication.md`; быстрый отказ/успех, не молчание —
  R3 `../../../OwnS3/docs/owns3-replication-requirements.md`).
- **Чтение/восстановление** — по приоритетам per-cluster (страховка на лаг
  доставки).
- **Синхронизация между серверами** — не забота клиентов: это mesh-репликация
  платформы ([05-platform.md](05-platform.md) §2) и зонд
  ([03-observation-statuses.md](03-observation-statuses.md)).
- **Layout объектов** — [backups/06-s3-layout.md](../backups/06-s3-layout.md) без изменений.

Канон фиксирует границу; реализация — PgWorker, правки контракта —
[backups/README.md](../backups/README.md)/[pgworker/04-security.md](../pgworker/04-security.md).

## 2. Реализация (фиксируется)

- **PgWorker — читатель** `/s3/clusters/<C>/{config,nodes/<I>/state,endpoints,secrets}`
  (ключ `secrets` — креды `backups`, read-write,
  [backups/08-secrets.md](../backups/08-secrets.md)) плюс `/s3worker/portalloc/<C>` для
  резолва имён инстансов в адреса; **в `/s3/` PgWorker НЕ пишет**.
- **Приоритеты записи per-PG-кластер** живут в policy бэкапов
  ([backups/05-etcd-contract.md](../backups/05-etcd-contract.md)) ссылками на имена инстансов
  (`write_master`/`write_replica` — по роли источника, не по роли ноды);
  чтение — все ноды пула без приоритетов (порядок `endpoints`-ключа).
- **EtcdNode** (агенты узлов etcd-контура) читает `config`/`endpoints`/
  `secrets` (ключ `backups`) etcd-backup кластера — как клиент пула
  ([etcdnode/02-snapshots.md](../etcdnode/02-snapshots.md) §1).
