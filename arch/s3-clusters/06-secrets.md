# S3-кластеры: секреты (контрактная часть)

Уровень 3. Два независимых контура секретов кластера — оба **per-cluster в
etcd**, оба переживают пересоздание контейнеров и смерть воркера:
**креды клиентской S3-грани** (ключ `secrets`) и **PKI репликации**
(`pki/ca`, `pki/orchestrator`, `nodes/<I>/pki`). PKI репликации клиентские
креды НЕ заменяет. Форматы ключей и примеры — [02-keys.md](02-keys.md);
исполнение воркером — [s3worker/04-security.md](../s3worker/04-security.md).

## 1. Креды клиентской грани

- Креды клиентской S3-грани (root-пара админ-грани + статические access
  keys) генерирует S3Worker при provisioning кластера и хранит в ключе
  `/s3/clusters/<C>/secrets` ([02-keys.md](02-keys.md) §2): пишет только
  воркер, ленивый txn put-if-absent (проигрыш гонки → re-read, первый
  записавший — истина); ротация — замена целиком, txn по `mod_revision`
  ([08-coordination.md](08-coordination.md) §1).
- **Состав**: root-пара (`root_user` — детерминированное имя
  `s3w-<C>-root`, не секрет; `root_password` — генерируется) + access keys
  `backups` (read-write — джобы/агенты бэкапов PgWorker) и `panel` (read-only
  — грань «Хранилище бэкапов» панели). Генерация значений — криптостойкие
  32 симв [A-Za-z0-9] (алфавит без спецсимволов — безопасно для env
  контейнера, JSON-значений и SigV4).
- **etcd-backup кластер** (`config.etcd_backup=true`, [02-keys.md](02-keys.md) §1):
  состав тот же, формат един для всех кластеров, но ключ `backups` НЕ
  генерируется — подставляется из спец-ключа бэкапа etcd: env-пары воркера
  `S3W_ETCD_BACKUP_ACCESSKEY` / `S3W_ETCD_BACKUP_SECRETKEY` (полупара —
  fail-fast старта воркера, [s3worker/08-operability.md](../s3worker/08-operability.md) §5;
  создание кластера с признаком без полной пары — 422,
  [s3worker/07-api.md](../s3worker/07-api.md) §3). Root-пара и ключ `panel`
  генерируются как у всех. Потребитель спец-ключа — etcdworker: читает
  значение из secrets-ключа однотипно с остальными клиентами.
- **Доставка в инстансы** — env контейнера (значения из secrets-ключа):
  `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` + `OWNS3_ACCESS_KEYS__<i>__*`
  (формат — прецедент
  [owns3/05-access-config.md](../../../OwnS3/arch/owns3/05-access-config.md) §2);
  env — транспорт доставки значений, не хранилище (источник правды — etcd).
- **Экспозиция**: парсеры панели разбирают secrets для внутреннего
  S3-клиента грани «Хранилище бэкапов» (ключ `panel`); в UI/API значения не
  отдаются — только имена/политики ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §12).
  PgWorker читает ключ `backups` ([07-client-pool.md](07-client-pool.md) §2).
- **Отсутствие/битость/неполнота** secrets-ключа существующего кластера —
  отказ фазы/диагностика процессов воркера (ретраи тиком), НЕ тихая
  перегенерация: живые инстансы с прежним env разошлись бы с ключом.
- **Ротация** — заявка `/s3worker/rotations/<C>`
  ([08-coordination.md](08-coordination.md) §1): генерация новых root+ключей
  (etcd-backup — спец-ключ пере-подставляется из env) — однократно на заявку
  (journal-маркер: повторный перевыпуск каждым тиком обнулял бы rolling) →
  замена secrets целиком (txn по `mod_revision`) → rolling-пересоздание
  инстансов по одному за тик с новым env; env-сверка контейнера — по ПОЛНЫМ
  значениям (root-пара + каждый
  `OWNS3_ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}` + отсутствие лишних
  индексов): состав при перевыпуске неизменен, значения новые — сверки по
  одному составу недостаточно; рестарт-семантика платформ: ownS3 —
  переконфигурация env + рестарт
  ([owns3/05-access-config.md](../../../OwnS3/arch/owns3/05-access-config.md) §2).

## 2. PKI репликации

- **CA кластера и выданные им сертификаты инстансов генерирует S3Worker и
  хранит в etcd** (ключи `pki/ca`, `pki/orchestrator`, `nodes/<I>/pki`,
  [02-keys.md](02-keys.md) §1) — ключи переживают provision/переезд
  контейнера: контейнер пересоздаётся с тем же сертификатом (том и env
  пересоздаются, материал в etcd). Криптомеханика — канон owns3/07 §2
  (ECDSA P-256, peer-сертификаты + trust-anchor CA + join): `CreateCa`/
  `IssueServer` клиентской копии peer-PKI ownS3
  (`S3Worker.Core/OwnS3/Protocol/PeerAuth`).
- **Выпуск** — лениво, по мере надобности (первый инстанс / deny-доведение),
  txn put-if-absent: CA (имя = `<C>`), сертификат оркестратора (имя
  `orchestrator`, для до-join контура `PUT /replication/denylist`,
  owns3/07 §6), сертификаты инстансов (имя = `<I>`). Сертификаты —
  **бессрочные** (`expiresAtUnixMs=0`): отзыва через deny-list достаточно,
  плановая ротация ключей/CA — вне (roadmap, owns3/07 §12).
- **Доставка в инстансы** — env контейнера (значения, не пути — прецедент
  owns3/07 §11): `OWNS3_REPLICATION_CERT` (JSON сертификата инстанса),
  `OWNS3_REPLICATION_PRIVATE_KEY` (PEM), `OWNS3_REPLICATION_TRUST_CA`
  (trust-anchor JSON `pki/ca`: `{"name","publicKeyPem"}`),
  `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` (`scheme://host:port` — advertised
  host + S3-порт инстанса из `/s3worker/portalloc/<C>`).
- **Состав mesh НЕ передаётся env** (env-seed — legacy-only, owns3/07 §2):
  доводка состава — админ-гранью/join
  ([s3worker/05-mesh-governance.md](../s3worker/05-mesh-governance.md)).
- **Приватные ключи** CA/инстансов читает только воркер (панель управление
  ключами не показывает — полностью скрыто бэком); `denylist` кластера
  ([02-keys.md](02-keys.md) §1) — операция отзыва: имя = ключ инстанса в
  `PUT /replication/denylist` всех инстансов.
