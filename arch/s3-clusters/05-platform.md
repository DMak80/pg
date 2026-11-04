# S3-кластеры: платформа инстансов

Уровень 3. Что за платформа обслуживает пул, какие bucket'ы живут на
инстансе и как устроена mesh-репликация. Модель размещения —
[04-placement.md](04-placement.md); секреты и PKI —
[06-secrets.md](06-secrets.md).

## 1. Платформа

**owns3** — единственная платформа пула:

- образ `owns3:dev` — внешний, собирается репозиторием `../OwnS3`
  (`deploy/build-image.sh` там); в registry не кладётся
  ([s3worker/01-placement.md](../s3worker/01-placement.md) §2);
- инстанс — ownS3 single-drive (`OWNS3_DATA_DIR=/data`);
- health — `/healthz`
  ([owns3/05-access-config.md](../../../OwnS3/arch/owns3/05-access-config.md) §5);
- занятость диска — админ-грань `GET /admin/storage` (там же);
- удаления ownS3 — физические (versioning отсутствует): ретенция
  [backups/05-etcd-contract.md](../backups/05-etcd-contract.md) работает напрямую, чистки версий не
  требуется.

## 2. Bucket'ы и mesh-репликация

- **bucket per-install** ([backups/06-s3-layout.md](../backups/06-s3-layout.md)) — на каждом
  инстансе, единое имя на всех инстансах кластера (клиент с одним комплектом
  кредов работает с любым инстансом); создание/поддержание — обязанность
  оркестратора (идемпотентный `CreateBucket` admin-кредом,
  [s3worker/02-cluster-lifecycle.md](../s3worker/02-cluster-lifecycle.md) §5).
- **etcd-backup кластер** (`etcd_backup=true`, [02-keys.md](02-keys.md) §1):
  на каждом инстансе ДОПОЛНИТЕЛЬНО фиксированный etcd-слепковый bucket —
  имя-константа `etcd-backups`, одинаковое на всех установках (создаёт
  S3Worker тем же идемпотентным `CreateBucket`; потребитель — EtcdWorker,
  [etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md) §1 — имя не
  конфигурируется).
- **mesh-репликация** — внутренняя механика ownS3
  ([owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md);
  конфиг per-install — R6 `../../../OwnS3/docs/owns3-replication-requirements.md`):
  состав mesh инстансов кластера доводит S3Worker по декларации `links`
  (PKI — [06-secrets.md](06-secrets.md) §2; доводка —
  [s3worker/05-mesh-governance.md](../s3worker/05-mesh-governance.md)).
  Локальный volume инстанса — НЕ копия: переживание отказа — только у
  инстансов, связанных в `links`.

## 3. Адресация портов

S3-порт контейнера `9000` → host-порт из portalloc воркера
(`/s3worker/portalloc/<C>`, [08-coordination.md](08-coordination.md) §1);
advertised-адрес = advertised host + этот порт — попадает в `endpoints`
([02-keys.md](02-keys.md) §1) и в `OWNS3_REPLICATION_ADVERTISED_ENDPOINT`
([06-secrets.md](06-secrets.md) §2).
