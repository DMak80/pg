# KafkaWorker: безопасность и ротации

Уровень 3. Функция «безопасность»: per-cluster PKI, SASL/ACL, секреты кластера
и ротации. Полные автоматы — [H](processes/h-password-rotator.md),
[K](processes/k-ca-rotator.md), [M](processes/m-security-migrator.md); сетевая
модель и TLS установки — [13-network-security.md](../13-network-security.md).

## 1. Per-cluster CA и сертификаты брокеров

**Per-cluster CA** (генерирует воркер при provisioning/миграции; ensure txn
put-if-absent — единый механизм с кредами):

- `ca_key` — self-signed CA (RSA-2048, CN=`kfw-<C>-ca-<отпечаток>`, срок
  10 лет; subject уникален на генерацию — в бандле OLD+NEW openssl-клиенты
  берут первый якорь по subject, одинаковые CN поколений ломали верификацию
  сертов второй генерации), приватный ключ PEM PKCS#8; подписывает
  сертификаты нод. Панель не читает.
- `ca_pem` — публичный сертификат CA; точка дискавери truststore
  ([kafka-clusters/05-client-discovery.md](../kafka-clusters/05-client-discovery.md)). В окне ротации —
  bundle OLD+NEW (PEM-конкатенация), после коммита — только NEW.

**Сертификаты нод**: генерируются воркером при сборке env
(`CertificateRequest` .NET), CN=`broker<k>`, SAN: `DNS:broker<k>` (INTERNAL) и
`DNS:<AdvertisedClientHost>` либо `IP:<адрес>` (CLIENT); один серт покрывает
оба SASL_SSL-listener'а. Срок 10 лет, RSA-2048; клиентская аутентификация на
listener'ах — `none` (принципалы из SASL/PLAIN).

## 2. Роли принципалов и authorization

Authorizer — `StandardAuthorizer` (KRaft), `allow.everyone.if.no.acl.found=
false` — deny-by-default. ACL живут в KRaft-метаданных (тома) и переживают
пересоздания контейнеров.

| Принципал | Роль | Права |
|---|---|---|
| `User:admin` | администратор | `super.users` — воркер (AdminClient + миниклиент [06](06-rebalancing.md) §1), панель (пробы), сид |
| `User:inter` | inter-broker | `super.users` — репликация и internal-топики не зависят от ACL |
| `User:app` | приложения | ACL (converge [E](processes/e-config-converger.md)): TOPIC `*` {READ,WRITE,DESCRIBE}, GROUP `*` {READ,DESCRIBE}, TRANSACTIONAL_ID `*` {WRITE,DESCRIBE} |

**Клиентские подключения воркера**: все Kafka-контакты — `admin` + `ca_pem`
по CLIENT endpoints из etcd (SASL_SSL, [kafka-clusters/05-client-discovery.md](../kafka-clusters/05-client-discovery.md));
AdminClient и миниклиент reassignment используют один канал и одни креды.
Пробы панели — `admin`, приложения — `app` + `ca_pem`.

## 3. Секреты кластера

Per-cluster, в etcd, генерирует воркер (ensure txn put-if-absent):

- **app** — `app_user`=`"app"`, `app_password` (32 симв `[A-Za-z0-9]`);
  ротация — [H](processes/h-password-rotator.md).
- **admin** — `admin_user`=`"admin"`, `admin_password`; super.user; ротация —
  [H](processes/h-password-rotator.md).
- **CA** — `ca_key` + `ca_pem`; ротация — [K](processes/k-ca-rotator.md).
- **inter-broker** — детерминированный пароль из имени кластера
  ([01-placement.md](01-placement.md) §3), вне etcd, super.user.

Env-секреты per-install — TLS HTTP API ([07-api.md](07-api.md) §2):
`KFW_API_TLS_{CERT,KEY}` — бутстрап-фоллбек серверного серта (приоритет —
etcd-ключ `/workers/api_tls/kafkaworker`, пишет панель); 
`KFW_API_TLS_CLIENT_CA` — валидатор клиентов, только env.

## 4. Ротации и миграция

- **Пароли app/admin (H)** — без окна недоступности: окно двух кредов
  OLD+NEW на брокерах, атомарный коммит в etcd, затем снятие OLD. Ротация
  admin не трогает app-кред и inter; окно «часть брокеров знает только NEW»
  невозможно. Точный автомат — [H](processes/h-password-rotator.md).
- **CA и серты (K)** — окно двойного доверия P→D→R→C: клиенты успевают
  перечитать bundle, OLD-ключ уничтожается перезаписью в коммите. Точный
  автомат — [K](processes/k-ca-rotator.md).
- **Премиграционные кластеры (M)** — нет CA/admin-креда или брокеры без
  SASL_SSL-env: Kafka не допускает смешанные security-протоколы inter-broker,
  поэтому миграция — полный рестарт кластера разом (окно ~1–3 мин), а не
  rolling. Точный автомат — [M](processes/m-security-migrator.md).
- Отказ между фазами безопасен: оба креда валидны (H) либо повтор продолжает
  по journal-фазе (K); guard'ы, приоритеты и waiting-фазы — в процессах,
  общая политика экспирации — [08-operability.md](08-operability.md) §3.
