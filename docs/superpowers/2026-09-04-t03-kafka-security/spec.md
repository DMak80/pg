# t03-kafka-security — безопасность Kafka-домена: TLS, ACL, разделение кредов, mTLS API

- **Дата**: 2026-09-04
- **Задача**: `t03-kafka-security` (roadmap `arch/roadmap/kafkaworker.md`)
- **Канон**: контракт etcd — `arch/15-kafka-clusters.md`; воркер —
  `arch/16-kafkaworker.md`; панельная проекция —
  `arch/adminpanel/02-etcd-contract.md` (глава 10, §2.3.2). Арх-канон
  обновлён этой задачей ДО кода (arch/-first); спека описывает отражение
  контракта в коде.

## 1. Цель

Закрыть четыре дыры безопасности Kafka-домена:

1. **Транспорт Kafka**: CLIENT и INTERNAL listeners переводятся с
   SASL_PLAINTEXT на **SASL_SSL** (TLS, per-cluster CA). CONTROLLER
   остаётся PLAINTEXT — KRaft-кворум живёт только внутри закрытой
   per-cluster сети `kfw-net-<C>` (решение интервью).
2. **Authorization (ACL)**: включается StandardAuthorizer (KRaft),
   deny-by-default; принципал `User:app` получает минимальные права
   (READ/WRITE/DESCRIBE топиков, группы, transactional id);
   `admin`/`inter` — super.users.
3. **Разделение кредов**: вместо одного per-cluster SASL-креда `app`,
   которым работали воркер, панель И приложения, появляются роли:
   `admin_user`/`admin_password` (воркер/панель/CLI) и
   `app_user`/`app_password` (только приложения); обе ротируются
   (заявки панели, протокол фаз A/B/C).
4. **Транспорт HTTP API KafkaWorker**: голый `X-Api-Key` (env
   `KFW_API_KEY`, отключён по умолчанию) заменяется на **mTLS-only**
   (клиентские сертификаты per-install API-CA; `KFW_API_KEY` удаляется).
   Вся HTTP-грань (вкл. `/healthz`) — только TLS; advertise-URL в
   `/kafkaworker/api/<id>` становится `https://`.

Существующие кластеры (поднятые на SASL_PLAINTEXT) **мигрируются
автоматически** — converge-процессом SecurityMigrator (полный рестарт
кластера разом, окно ~1–3 мин; техника — §7 M).

## 2. Принципы

- **arch/-first**: контракт (15/16/adminpanel-02) уже обновлён этой
  задачей; код ниже — исполнение контракта, не его источник.
- **Per-cluster изоляция секретов**: CA и креды каждого кластера — свои;
  компрометация одного кластера не открывает остальные.
- **etcd — хранилище per-cluster-секретов контроль-плейна**: `app`/`admin`
  креды и `ca_key`/`ca_pem` живут в etcd по образцу существующих секретов
  (ensure txn put-if-absent, ротация txn-коммитом). Единственное осознанное
  исключение — per-install TLS-секреты HTTP API воркера: транспортная
  граница API не может жить в etcd, потому что etcd-клиент сам ходит по
  HTTP (бутстрап-парадокс).
- **Один канон, без веток**: после t03 НЕТ «старого формата» — новые
  кластера поднимаются сразу в новом каноне, старые мигрируются M.
  Дискавери-контракт (15 §5) меняется один раз (breaking change для
  TLS-неготовых клиентов — заявляется релизом).
- **Идемпотентность и takeover**: все новые операции перепроверяют факт;
  состояние — etcd + тома; смерть инстанса — takeover клэймом ≤ TTL.
- **Deny по умолчанию**: `allow.everyone.if.no.acl.found=false`;
  принципал наименьших привилегий `app` не может администрировать.
- **Русский для документации, английский для идентификаторов**;
  TreatWarningsAsErrors, .NET 10, тесты — AAA-комментарии.

## 3. Решения интервью (зафиксированы с пользователем)

| # | Развилка | Решение |
|---|---|---|
| 1 | Охват TLS | CLIENT + INTERNAL → SASL_SSL; CONTROLLER — PLAINTEXT в закрытой сети кворума |
| 2 | PKI | Per-cluster CA: ключ CA — секрет etcd (`ca_key`), публичный серт — точка дискавери (`ca_pem`); серты нод генерит воркер |
| 3 | Креды/ACL | `admin` в etcd + StandardAuthorizer: admin/inter — super.users; app — ACL минимум; воркер/панель/CLI ходят как admin |
| 4 | HTTP API | mTLS-only per-install API-CA; `X-Api-Key`/`KFW_API_KEY` удаляем; `/healthz` за тем же TLS |
| 5 | Существующие кластеры | Converge-миграция всех (rolling по одному невозможен технически — смешанные inter-broker протоколы роняют ISR ниже minISR; поэтому полный рестарт кластера разом) |
| 6 | Ротации | Ротация admin-пароля — в скоупе (фазы A/B/C); ротация CA/сертов — roadmap (серты долгоживущие, 10 лет) |

## 4. Правки канона arch/ (уже внесены в этом worktree)

### 4.1. `arch/15-kafka-clusters.md` (контракт etcd)

- **§2 таблица ключей кластера**: `app_user`/`app_password` — роль
  приложений (ACL); НОВЫЕ ключи `admin_user` (`"admin"`, ensure воркера),
  `admin_password` (32 симв, ensure + ротация; панель читает для проб),
  `ca_pem` (PEM публичного CA-серта, точка дискавери),
  `ca_key` (PEM PKCS#8 приватный ключ CA; панель НЕ читает).
- **§2.1 примеры**: канонические значения `admin_user`/`admin_password`
  и формат PEM-ключей (`\n`-переносы одной строкой).
- **§4 координация**: НОВЫЙ ключ `/kafkaworker/admin_rotations/<C>` —
  заявка ротации admin-пароля (клэйм-txn панели, исполнение H, del
  воркером/панелью-отмена); панель читает из `/kafkaworker/` также
  `admin_rotations/`.
- **§5 клиентский дискавери**: добавлен шаг чтения `ca_pem` → TLS-доверие;
  протокол фиксируется каноном: `security.protocol=SASL_SSL` +
  `sasl.mechanisms=PLAIN` (без per-cluster флага безопасности).
- **§6 сбои**: Active-кластер без `ca_pem`/`admin_password` —
  critical-алерт `kafka-security-missing`; битый PEM — parseError +
  warning `kafka-key-malformed`.

### 4.2. `arch/16-kafkaworker.md` (воркер)

- **Преамбула**: «одиннадцать процессов» (+ SecurityMigrator M);
  H переименован в PasswordRotator (роли app|admin); из границ исключены
  TLS/ACL, добавлена ротация CA как roadmap.
- **§1.1 HTTP API**: mTLS-only; дискавери-ключ `url` — `https://`;
  env-секреты `KFW_API_TLS_{CERT,KEY,CLIENT_CA}` (+`_PATH`-варианты);
  `KafkaWorker:Api:Tls:AllowInsecureHttp` (default false, только WAF);
  `KFW_API_KEY` удалён.
- **§2.1 listeners**: INTERNAL/CLIENT — SASL_SSL (PLAIN поверх TLS),
  CONTROLLER — PLAINTEXT; JAAS-пользователи по ролям admin/app (окна
  ротации `user_<name>`+`user_<name>2`); per-install-секреты — только TLS
  API.
- **§2.2 env-таблица**: `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=
  CONTROLLER:PLAINTEXT,INTERNAL:SASL_SSL,CLIENT:SASL_SSL`; НОВЫЕ env:
  `KAFKA_SSL_KEYSTORE_TYPE=CERTIFICATE_CHAIN/KEY PEM-пара`,
  `KAFKA_SSL_TRUSTSTORE_TYPE=PEM` + `…_CERTIFICATES=ca_pem`,
  `KAFKA_AUTHORIZER_CLASS_NAME=…StandardAuthorizer`,
  `KAFKA_SUPER_USERS=User:admin;User:inter`,
  `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`; JAAS INTERNAL/CLIENT
  несут `user_admin`[+2] и `user_app`[+2].
- **§2.3 (новый) PKI, TLS и authorization**: per-cluster CA (RSA-2048,
  CN=`kfw-<C>-ca`, 10 лет), серты нод (CN=`broker<k>`, SAN: DNS-имя ноды
  + advertised CLIENT host/IP, 10 лет, PEM в env, клиентская
  аутентификация listener'ов — none, принципалы из SASL); таблица
  принципалов admin/inter/app и ACL-план роли app.
- **§2.4 CLI**: `--command-config` — `security.protocol=SASL_SSL`,
  JAAS admin-креда, PEM-truststore `ca_pem` в файл `/tmp/kfw-ca.pem`
  (`ssl.truststore.type=PEM` + `ssl.truststore.location`).
- **§3.1/§3.2**: читаемые/пишемые ключи — admin/CA-ключи, `admin_rotations`.
- **§4 секреты**: группы app/admin/CA/inter; per-install — только TLS API.
- **§5**: классификация тика (M — до всех Active-шагов); K2 — ensure
  CA+admin+app; E — + ACL-converge (DescribeAcls → diff → Create/Delete);
  H — обобщён на app|admin; НОВЫЙ **M. SecurityMigrator** (детект:
  нет `ca_pem`/`ca_key`/`admin_password` ИЛИ env контейнеров без
  `KAFKA_SSL_TRUSTSTORE_TYPE`; фазы M0–M4: guard'ы → ensure → полный
  рестарт всех брокеров разом → ждём готовности → ACL-converge;
  идемпотентность по факту).
- **§8 конфигурация**: `KafkaWorker:Api:Tls {...}`, `https://`
  advertise, env-секреты TLS.
- **§9 риски**: +R8 (SAN vs смена advertised-host), +R9 (окно миграции
  M — отказ TLS-неготовых клиентов), +R10 (ca_key в etcd — зона доверия
  контроль-плейна, ротация CA — roadmap).

### 4.3. `arch/adminpanel/02-etcd-contract.md` (панель)

- **§2.3.2**: URL KafkaWorker-API — `https://`; панель аутентифицируется
  клиентским сертом (`AdminPanel:Workers:KafkaTls {...}`, env
  `KFW_PANEL_TLS_*`); `/healthz`-поллер — тем же сертом; X-Api-Key для
  KafkaWorker удалён (PgWorker-ключ не трогается).
- **§10.1**: панель читает `admin_user`/`admin_password`/`ca_pem` (internal
  стор, пробы SASL_SSL); `app_*` из чтения проб убраны.
- **§10.2**: мутация **№16** «Ротация admin-пароля»
  `POST /api/kafka/clusters/{c}/admin-password/rotate` (клэйм-txn
  `/kafkaworker/admin_rotations/<C>`; отказы 404/409/503 — как у №8).

## 5. Структура и компоненты (отражение в коде)

### 5.1. Воркер — KafkaWorker.Core

- **`Templates/ClusterPki.cs`** (новый): генерация self-signed CA
  (RSA-2048, CN=`kfw-<C>-ca`, BasicConstraints CA, NotAfter +10 лет;
  `CertificateRequest` .NET, без внешних инструментов) →
  `(caPem, caKeyPem PKCS#8)`; выпуск серта ноды (CN=`broker<k>`, SAN
  `DNS:broker<k>` + `DNS:<AdvertisedClientHost>` либо `IP:…`,
  EKU ServerAuth, 10 лет), подпись ключом CA; парсинг/валидация PEM из
  etcd (`X509Certificate2`).
- **`Templates/NodeEnvBuilder.cs`** (расширение): `NodeEnvSpec` получает
  `AdminPasswords` (окно ротации admin), `CaPem`, `BrokerCertPem`,
  `BrokerKeyPem`; таблица env — новый канон 16 §2.2 (SASL_SSL map,
  SSL-PEM пару, authorizer, super.users, deny-by-default, JAAS
  `user_admin`[+2]/`user_app`[+2] на INTERNAL и CLIENT). Детерминизм R3
  сохраняется: серт ноды генерируется один раз на сборку env-набора
  кластера (кешируется в рамках тика процесса), всё остальное — чистые
  функции от входа.

### 5.2. Воркер — KafkaWorker.Provisioning

- **`Processes/ClusterSecretEnsurer.cs`** (переименование/обобщение
  `AppSecretEnsurer`): ensure `admin_user`/`admin_password`/`ca_pem`/
  `ca_key`/`app_user`/`app_password` одной txn put-if-absent по
  отсутствующим; CA генерирует `ClusterPki` (не из фиксированного
  сида — случайный); re-read при проигрыше. Вызывается из K2
  provisioning и M1 миграции.
- **`Kafka/KafkaAdminClient.cs` + `IKafkaAdminClient.cs`**: фабрика
  принимает TLS-параметры (`SecurityProtocol=SaslSsl`, креда admin,
  доверие CA: `Set("ssl.ca.pem", caPem)` — librdkafka ≥1.5, входит в
  Confluent.Kafka 2.14.2); НОВЫЕ операции `DescribeAclsAsync` /
  `CreateAclsAsync` / `DeleteAclsAsync` (шов для ACL-converge).
  Все процессы воркера подключаются как **admin** по CLIENT endpoints
  из etcd (SASL_SSL + доверие `ca_pem`; INTERNAL advertised — docker-DNS,
  недостижим из процесса воркера вне сети `kfw-net-<C>` — INTERNAL остаётся
  транспортом reassign-CLI, §2.4; канон 16 §2.3 синхронизирован).
- **`Processes/ClusterConfigConverger.cs`**: шаг **ACL-converge** после
  конфиг-converge — план роли `User:app` (TOPIC `*` {READ, WRITE,
  DESCRIBE}; GROUP `*` {READ, DESCRIBE}; TRANSACTIONAL_ID `*` {WRITE,
  DESCRIBE}; все LITERAL): DescribeAcls → diff (создать недостающие,
  удалить лишние у `User:app`) → Create/DeleteAcls. Чистые функции
  плана — отдельный тип (юнит-тесты).
- **`Processes/ReassignCli.cs`**: `BuildAdminProperties` — SASL_SSL +
  JAAS admin + PEM CA; `BuildExecCommand` пишет `/tmp/kfw-ca.pem` тем же
  printf-механизмом (CA-PEM не содержит апострофов и `\` — безопасно;
  переносы кодируются `printf '%s\n'`).
- **`Processes/PasswordRotator.cs`** (обобщение `AppPasswordRotator`):
  параметр роли (app|admin): ключ заявки `rotations`/`admin_rotations`,
  ключ пароля `app_password`/`admin_password`, JAAS-пользователи
  `user_app`/`user_admin` (+`2` в окне). Фазы A/B/C — без изменений
  механики. Обрабатывает по одной заявке за тик (app раньше admin —
  детерминированный порядок).
- **`Processes/SecurityMigrator.cs`** (новый, 16 §5 M): детект
  премиграционного кластера (снапшот-поля CA/admin + docker-inspect env
  живых контейнеров на `KAFKA_SSL_TRUSTSTORE_TYPE`); M0 guard'ы (живые
  ротации/reassignment/regens → journal-waiting; снапшот P12 «до»),
  M1 ensure секретов, M2 пересоздание ВСЕХ контейнеров брокеров разом
  (volume сохраняется; порты/сеть/roles из portalloc — без изменений),
  M3 ожидание готовности (DescribeCluster с admin-кредом по CLIENT
  endpoints из etcd — SASL_SSL, бюджет `BrokerBootSec`), M4 стартовый
  ACL-converge + снапшот P12 «после». Вызывается из KafkaClusterProcesses
  в начале Active-ветки.

### 5.3. Воркер — KafkaWorker.App

- **`Options.cs`**: `ApiOptions` — минус `ApiKey`, плюс
  `TlsOptions { ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath,
  ClientCaPem|ClientCaPath, AllowInsecureHttp=false }` (env-биндинг
  `KFW_API_TLS_*` / `KFW_API_TLS_*_PATH`).
- **`Program.cs`**: Kestrel — `ListenAnyIP(8080)` c `UseHttps(серт)` и
  `ClientCertificateMode.Required` + валидация цепочки против ClientCA
  (fail-fast при старте: серт+ключ читаются и совпадают, ClientCA задан,
  `AdvertiseUrl` начинается с `https://`). `AllowInsecureHttp=true` —
  без TLS (только WAF-фикстуры; warning-лог при старте). ApiKeyMiddleware
  удаляется; DI PasswordRotator/SecurityMigrator/ClusterSecretEnsurer.
- **`Api/ApiKeyMiddleware.cs`** — удаляется (заменён mTLS).
- **`Api/Operations/RotateAdminPasswordHandler.cs`** (новый, порт
  `RotateAppPasswordHandler`): `POST /api/kafka/clusters/{c}/
  admin-password/rotate` — клэйм-txn `version(admin_rotations/<C>)==0` +
  put заявки; 404/409/503 по контракту adminpanel/02 §10.2 №16.
- Healthz: без изменений логики (тот же порт, TLS сверху).
- Комментарии в Program.cs/Options.cs про «env-секретов per-install нет»
  заменяются на «только TLS API».

### 5.4. Панель — AdminPanel.*

- **`Etcd/Workers/WorkerApiOptions.cs`**: минус `KafkaApiKey`, плюс
  `KafkaTlsOptions { ClientCertPem|Path, ClientKeyPem|Path,
  ServerCaPem|Path }` (env `KFW_PANEL_TLS_*`).
- **`Etcd/Workers/WorkerApiGateway.cs`**: HttpClient `"workers"`
  конфигурируется клиентским сертом (по заданным настройкам) и доверием
  ServerCA (для pg-URL http:// без изменений — серт применяется только к
  TLS-запросам); заголовок `X-Api-Key` для kafkaworker не шлётся.
  `WorkerHealthPoller` — тем же клиентом (без правок логики).
- **`Etcd/KafkaSecretsStore.cs` + `KafkaSnapshotRefresher.cs`**:
  `KafkaClusterSecrets { Cluster, AdminUser, AdminPassword, CaPem }`;
  чтение ключей `admin_user`/`admin_password`/`ca_pem` (app-креды панель
  больше не читает); парсер `KafkaParser` — новые ключи в internal-стор
  (в `KafkaClusterInfo`/UI/API не выносятся), `ca_key` не читается.
- **`Probes/Kafka/KafkaClientCache.cs` + `ConfluentKafkaProbeClient.cs`**:
  конфиг `SecurityProtocol=SaslSsl` + `Set("ssl.ca.pem", caPem)`;
  ключ кэша расширяется caPem (смена CA → пересоздание клиентов);
  креда — admin. Пробы ходят как admin (super.user — все Describe/List/
  оффсеты разрешены).
- **`Api/Operations/RotateAdminPasswordCommand.cs`** (порт
  `RotateAppPasswordCommand`) — мутация №16; UI (React kafka-домен):
  кнопка «Ротация admin-пароля» в деталях кластера рядом с app-ротацией,
  модалка-предупреждение о rolling-рестартах; очередь заявок
  `KafkaAdminRotationTicket` в kafka-снапшоте + warning-алерт
  `kafka-admin-rotation-pending` (порт `kafka-rotation-pending`).

### 5.5. Стенд и поставка

- **`deploy/docker-compose.yml`** (kafkaworker): volume `kfw-api-tls`
  (ro, `/tls`), env `KFW_API_TLS_CERT_PATH=/tls/server.crt`,
  `KFW_API_TLS_KEY_PATH=/tls/server.key`,
  `KFW_API_TLS_CLIENT_CA_PATH=/tls/ca.pem`,
  `KafkaWorker__Api__AdvertiseUrl=https://host.docker.internal:8081`;
  `KFW_API_KEY` удаляется. `.env.example` — соответствующие комментарии.
- **`deploy/tls/gen.sh`** (новый): openssl-генерация per-install
  API-пакета в `deploy/tls/` (gitignored): CA, серверный серт воркера
  (SAN: `localhost`, `host.docker.internal`, `kafkaworker`), клиентский
  серт панели. Вызывается оператором/стендом; серты живут вне git.
- **`docker/KafkaWorker.Dockerfile`**: HEALTHCHECK — mTLS-вызов (`/healthz`
  за клиентской аутентификацией, §1.4): клиентская пара
  `healthcheck.crt/key` из `deploy/tls/gen.sh` —
  `curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key https://localhost:8080/healthz`.
- **`dev-stand/adminpanel/`**: `00-up.sh` генерирует TLS-пакет
  (переиспользует gen.sh) в `dev-stand/adminpanel/tls/` (gitignored) и
  прокидывает воркеру (deploy compose с env-путями) и панели
  (`ADMINPANEL__WORKERS__KAFKATLS__*_PATH`); README-стенда +
  `adminpanel.appsettings.json` — новые настройки. Чеки `checks/` —
  https-вызовы с `--cacert`/клиентским сертом.
- **`dev-stand/seed.sh`** — вызовы API воркера становятся mTLS
  (`curl --cert/--key/--cacert`).

### 5.6. Библиотека дискавери (вне репо — зафиксировать в канон)

Дискавери-контракт 15 §5 уже несёт `ca_pem` + SASL_SSL. Библиотека
`t05-kafka-discovery-lib` (репозиторий Puzzle) обновляется отдельной
задачей того же релиза (иначе приложения не подключатся после M);
в её спеку заносится: чтение `ca_pem`, `SASL_SSL/PLAIN`,
hot-reload кредов уже есть.

## 6. Фазы реализации (скелет для plan-фазы)

1. **Ф1 PKI-ядро**: `ClusterPki` + `NodeEnvBuilder` (новый env-канон) +
   юнит-тесты (SAN-правила, PEM-валидность, JAAS-наборы ролей/окон
   ротации, super.users/authorizer env, чистые функции).
2. **Ф2 Контракт/секреты**: `ClusterSecretEnsurer` (ensure CA+admin+app),
   парсеры воркера/панели (новые ключи, unknownKeys-толерантность,
   `kafka-security-missing`), `KafkaClusterSecrets` панели.
3. **Ф3 Транспорт воркера**: AdminClient TLS+admin-кред во всех
   процессах; `ReassignCli` SASL_SSL-command-config; ACL-converge в E;
   provisioning K2/K3/K5 на новом env; интеграционный тест поднятия
   TLS-кластера (фикстура генерирует серты через `ClusterPki`).
4. **Ф4 Ротации**: PasswordRotator (app|admin), мутация №16 (воркер) +
   команда/UI/алерт панели; интеграционный тест ротации admin (фазы,
   окно двух кредов, доступность приложения app-кредом на всём окне).
5. **Ф5 mTLS API**: Kestrel-конфигурация, Options/env, удаление
   ApiKeyMiddleware; панель WorkerApiGateway/поллер TLS; Dockerfile/
   deploy/стенд-скрипты; WAF-тесты на AllowInsecureHttp.
6. **Ф6 Миграция**: `SecurityMigrator` + интеграционный тест
   «PLAINTEXT-кластер → M → SASL_SSL-кластер» (эндпоинты и данные
   сохраняются, app-кред проходит ACL-проверки, admin-кред управляет);
   e2e-маркер Release.
7. **Ф7 Полировка**: доки (README стенда, deploy), прогон полного
   набора, roadmap-чистка (§9).

## 7. Ограничения и out of scope

- **Ротация CA и серверных сертов** — roadmap (новая запись, см. §9);
  серты — 10 лет, expire-мониторинг не строим.
- **TLS на CONTROLLER-listener** — не делается (закрытая сеть кворума;
  реальный контур при необходимости — roadmap).
- **mTLS PgWorker API** — не трогаем (X-Api-Key остаётся; отдельная
  задача при желании).
- **Тюнинг TLS** (шифры/протоколы, TLS 1.3-only) — не настраиваем,
  дефолты Kafka/JVM и .NET Kestrel.
- **Kerberos/OAUTHBEARER и прочие механизмы** — только PLAIN поверх TLS.
- **Kafka-exporter/метрики** — t04.
- Окно миграции M — объявленный простой кластера (~1–3 мин);
  rolling-вариант без даунтайма отвергнут технически (смешанные
  inter-broker протоколы невозможны) и по цене (вечный расщеплённый
  INTERNAL-порт у мигрированных).

## 8. Критерии приёмки

1. **Юнит**: `ClusterPki` (CA/серт, SAN: docker-DNS + advertised
   DNS/IP, PEM round-trip), `NodeEnvBuilder` (полный новый env-набор по
   16 §2.2, JAAS-набор admin[+2]/app[+2]/inter, окна ротаций обеих
   ролей), ACL-план/диф, миграционный детект, `ReassignCli`
   properties (SASL_SSL + truststore), Options-биндинг TLS.
2. **Интеграционные (docker)**: новый кластер поднимается SASL_SSL
   (endpoints из etcd, приложение-клиент с `ca_pem`+app-кредом
   производит/потребляет); **ACL**: app-кред получает отказ на
   CreateTopics/DescribeCluster-админ-операции, admin-кред — выполняет;
   ротация admin (окно A/B/C — app-клиент работает непрерывно);
   миграция PLAINTEXT→SASL_SSL (данные/эндпоинты живы); mTLS API
   (клиент без серта — отказ, с сертом — 200; `/healthz` за TLS);
   portalloc/t91-тесты не сломаны.
3. **E2E Release (мерж-гейт)**: свежий Release, полный прогон
   `KafkaWorker.IntegrationTests` (docker); маркер-кейс
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release
   --filter FullyQualifiedName~Provisioning_TlsClusterUp` (новый кейс
   в ProvisioningTests — поднятие TLS-кластера с проверкой produser/
   consumer через ca_pem); pg-маркер `Scale_AddEmptyShard` — зелёный
   (общие слои не задеты регрессией).
4. **Стенд**: `dev-stand/adminpanel/checks/00-up.sh` поднимает полную
   систему с mTLS (панель видит живой /healthz воркера по https,
   kafka-пробы зелёные через admin+CA, сиды наливаются); чек kafka
   проходит на TLS-кластере.
5. **Контракт**: значения в etcd соответствуют каноническим примерам
   15 §2.1 (вкл. новые ключи); парсеры не падают на неизвестных ключах.
6. **Код**: TreatWarningsAsErrors чисто; тесты с AAA-комментариями;
   нигде не осталось `X-Api-Key`/`KFW_API_KEY` для KafkaWorker и
   `SASL_PLAINTEXT` в env брокеров/клиентов воркера.

## 9. Roadmap-чистка (мерж-гейт)

- Запись `t03-kafka-security` удаляется из `arch/roadmap/kafkaworker.md`
  тем же коммитом мержа (вкл. `←`-зависимости).
- Добавляется новая запись: «`tNN-kafka-ca-rotation` — ротация per-cluster
  CA и серверных сертификатов (окно двойного доверия, rolling;
  отложено из t03)» с зависимостью от текущего канона безопасности.

## 10. Открытые вопросы

Нет — все принципиальные развилки закрыты интервью (§3); детали
имплементации (именование тестовых классов, точечные сигнатуры)
решаются в plan-фазе.
