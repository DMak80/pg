# S3Worker: безопасность и ротации

Уровень 3. Функция «безопасность»: креды клиентской S3-грани, репликационный
PKI, доставка секретов в инстансы и ротация кредов. Контракт секретов —
[s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md) (единственный источник истины по составу и
форматам ключей); полный автомат ротации —
[H. SecretRotator](processes/h-credentials-rotator.md); сетевая модель и TLS
установки — [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md).

## 1. Креды клиентской S3-грани

Per-cluster ключ `/s3/clusters/<C>/secrets` ([s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md)):
root-пара админ-грани + статические access keys `backups` (read-write) и
`panel` (read-only). Генерирует воркер при provisioning (txn
put-if-absent); etcd-backup кластеру ключ `backups` подставляется из
спец-ключа env воркера `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}`, root-пара и
`panel` генерируются как у всех.

Источник для процессов — `ClusterSecretsStore`:

- **Ensure** — выпуск/идемпотентное чтение (provisioning);
- **TryRead** — надзор C, G, F, add-node, ротатор H; отсутствие, битость или
  неполнота ключа — journal-отказ с диагностикой БЕЗ значений (ретраи
  тиком), НЕ тихая перегенерация: живые инстансы с прежним env разошлись бы
  с ключом;
- **Rotate** — генерация новых значений и замена ключа целиком (txn по
  `mod_revision`; проигрыш — отложенный тик).

Значения секретов никогда не попадают в журнал и диагностику.

## 2. Репликационный PKI

CA кластера, сертификат оркестратора и сертификаты инстансов выпускает
воркер (криптомеханика — owns3/07 §2, ECDSA P-256) и хранит в
`/s3/clusters/<C>/pki/*` и `nodes/<I>/pki` ([s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md));
материал переживает provisioning и переезд контейнера. Выпуск — ленивый, по
мере надобности (первый инстанс, deny-доведение), txn put-if-absent.
Сертификаты **бессрочные**: отзыв компрометации — deny-list
([05-mesh-governance.md](05-mesh-governance.md)), плановая ротация ключей/CA
— вне штатных операций ([roadmap/s3.md](../roadmap/s3.md)). Приватные ключи
читает только воркер; панель управление ключами не показывает.

## 3. Доставка в инстансы

Env контейнера инстанса при создании (значения — из secrets-ключа и PKI):

- `OWNS3_ROOT_USER` / `OWNS3_ROOT_PASSWORD`;
- `OWNS3_ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}` (по числу ключей);
- `OWNS3_REPLICATION_CERT`, `OWNS3_REPLICATION_PRIVATE_KEY`,
  `OWNS3_REPLICATION_TRUST_CA`, `OWNS3_REPLICATION_ADVERTISED_ENDPOINT`;
- `OWNS3_DATA_DIR=/data`,
  `OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS` (из
  `S3Worker:OwnS3:ReplicationReconcileIntervalSec`; не задана/0 — переменная
  не ставится: прод-дефолт OwnS3 3 ч).

Состав mesh в env НЕ передаётся — его доводит G
([05-mesh-governance.md](05-mesh-governance.md)). Единая точка сборки env —
`InstanceEnvComposer`; env — транспорт доставки значений, не хранилище
(источник правды — etcd). Ротация кредов пересобирает env целиком.

## 4. Ротация клиентских кредов (H)

Заявка `/s3worker/rotations/<C>` ([07-api.md](07-api.md) §2): генерация новых
root+ключей и замена `secrets` целиком (etcd-backup — спец-ключ
пере-подставляется из env) — **однократно на заявку** (journal-маркер:
повторный перевыпуск каждым тиком обнулял бы rolling) → rolling-пересоздание
инстансов по одному за тик с новым env (volume не трогается, PKI и
`root_user` переиспользуются) → del заявки. Env-сверка контейнера — по
ПОЛНЫМ значениям (root-пара + каждый
`OWNS3_ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}` + отсутствие лишних
индексов): состав при перевыпуске неизменен, значения новые — сверки по
одному составу недостаточно. Ротация — эксклюзивная ветка тика (надзор,
add/remove-node в том тике не идут; [08-operability.md](08-operability.md)
§1).

Отказ между шагами безопасен: ключ заменён атомарно, инстансы доводятся
следующими тиками по journal-фазе; инстанс, ещё не пересозданный, работает
со старыми значениями (клиенты, перечитавшие etcd, — с новыми: расхождение
закрывает rolling). Ротация ключей репликации/CA — вне (roadmap,
owns3/07 §12); отзыв — deny-list ([05-mesh-governance.md](05-mesh-governance.md)).

## 5. Границы доверия

- **etcd** — хранилище per-cluster-секретов и PKI (как pg/kafka/valkey):
  компрометация etcd = зона доверия контроль-плейна; доступ — только из
  закрытой сети установки.
- **Per-install env-секреты** — только TLS воркера: серверный серт API
  (etcd-ключ `/workers/api_tls/s3worker`, пишет панель; env
  `S3W_API_TLS_*` — бутстрап-фоллбек, [07-api.md](07-api.md) §4) и
  транспортные секреты docker/SSH ([01-placement.md](01-placement.md) §1).
- **Репликационный транспорт** инстансов — mTLS на peer-PKI (§2); клиентская
  S3-грань — SigV4 по статическим ключам (§1).
