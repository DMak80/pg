# t03-docker-tls-ssh — транспортная безопасность PgWorker: TLS/SSH к Engine API, RBAC/docker-группы, mTLS HTTP API

- **Дата**: 2026-09-05
- **Задача**: `t03-docker-tls-ssh` (roadmap `arch/roadmap/pgworker.md`)
- **Канон**: воркер — `arch/14-pgworker.md` (§1.1, §2.2–2.3); сеть —
  `arch/13-network-security.md` §2; панельная проекция —
  `arch/adminpanel/02-etcd-contract.md` (§2.3.1/§2.3.2); метрики —
  `arch/18-metrics.md` §5.2. Арх-канон обновлён этой задачей ДО кода
  (arch/-first); спека описывает отражение контракта в коде.
- **Прецедент**: kafka-часть t03 (`t03-kafka-security`, мерж 2026-09-04,
  `docs/superpowers/2026-09-04-t03-kafka-security/`) уже реализовала mTLS API
  для KafkaWorker и per-install TLS-пакет стенда (`deploy/tls/`,
  `kfw-install-ca`, `TlsEndpoints`/`WorkerTlsHandler`). Эта задача
  переиспользует готовые механизмы и переносит их на pg-домен и docker-транспорт.

## 1. Цель

Закрыть четыре транспортные дыры безопасности pg-домена:

1. **Транспорт к Docker Engine API**: сейчас PgWorker ходит в демоны по
   plaintext `tcp://host:2375` или `unix:///var/run/docker.sock` в доверенной
   сети. Добавляются защищённые схемы транспорта: `tcp://` + **mTLS**
   (per-install docker-CA, серверные серты демонов, клиентский серт воркера)
   и **SSH-туннели** `ssh://user@host` (key-аутентификация, локальный форвардинг
   к daemon-порту на loopback удалённого хоста).
2. **RBAC/docker-группы**: канон хостов (Docker CE тонкого RBAC не имеет) —
   unix-socket по группе `docker`, контейнер воркера с `group_add`/выделенным
   пользователем, запрет `:2375` наружу, firewall-правила; фиксируется в
   arch/14 §2.2.1 и поставке (compose-комментарий/README).
3. **Транспортная безопасность HTTP API PgWorker** (arch/14 §1.1): голый
   `X-Api-Key` (env `PGW_API_KEY`, отключён по умолчанию) заменяется на
   **mTLS-only** по образцу KafkaWorker: вся грань порта `:8080` (вкл.
   `/healthz` и `/metrics`) — только TLS; клиенты аутентифицируются клиентскими
   сертами per-install API-CA; `PGW_API_KEY` удаляется; advertise-URL в
   `/pgworker/api/<id>` становится `https://`.
4. **Отдельные креды панели и сида**: мутации панели и вызовы стендового сида
   аутентифицируются РАЗНЫМИ клиентскими сертами (`panel.crt`, `seed.crt`)
   одной клиентской CA — различимость в журналах сервера, независимый отзыв;
   плюс `prometheus.crt` (скрейп за mTLS) и `healthcheck.crt` (docker-HEALTHCHECK).

Существующие стенды/развёртывания мигрируются **тем же релизом** (панель и
воркер обновляются атомарно; pg-домен не имеет полевых кластеров, чей
контроль-плейн живёт отдельно — миграционного процесса уровня
SecurityMigrator не требуется, вся миграция = конфигурация поставки).

## 2. Принципы

- **arch/-first**: контракт (arch/14/13/18, adminpanel/02) уже обновлён этой
  задачей; код ниже — исполнение контракта, не его источник.
- **Один паттерн на оба воркера**: mTLS API PgWorker — 1:1 механика
  KafkaWorker (`TlsEndpoints`/`WorkerTlsHandler`/`MtlsApiTests`), включая
  fail-fast старта, `AllowInsecureHttp=false` (только WAF-тесты), PEM/PATH-дуализм
  env-секретов. Дубли кода PgWorker.App ↔ KafkaWorker.App — осознанные
  (прецедент `PgWorker.Docker`/`KafkaWorker.Docker`); унификация — в `t08-unify-
  adminpanel-duplicates` (заметка дописывается в roadmap, §9).
- **Единая per-install API-пакета** (решение О1, принято): оба воркера
  обслуживаются одной CA `kfw-install-ca` — один клиентский серт панели валиден
  на обоих API, один ServerCA в доверии панели, одна ротация пакета на релиз.
- **Docker-транспорт — отдельный пер-install docker-CA**: docker-хосты — другое
  доверенное множество (много хостов, операторские демоны), изоляция доверия
  от API-пакеты (компрометация одного не открывает другое).
- **etcd-секреты не затрагиваются**: per-cluster секреты pg-домена, etcd-транспорт
  и Patroni-контур — вне задачи (различные каналы; их TLS — отдельные roadmap-пункты).
- **Идемпотентность и transient-толерантность**: отказ туннеля/докера —
  transient (healthz-пинг красный, тики повторяют); никакой новой машины
  состояний не вводится.
- **Русский для документации, английский для идентификаторов**;
  TreatWarningsAsErrors, .NET 10, тесты — AAA-комментарии; порты docker в
  тестах — динамические; зачистка контейнеров/сетей после каждой серии.

## 3. Решения

Зафиксированы из roadmap/канона/прецедента t03-kafka; развилки О1–О4 решены
пользователем (принятые решения — §10).

| # | Развилка | Решение |
|---|---|---|
| 1 | HTTP API PgWorker | mTLS-only по прецеденту KafkaWorker (§1.1 arch/14): `PGW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]`, `AllowInsecureHttp` (default false, только WAF), `X-Api-Key`/`PGW_API_KEY` удалён, вся грань `:8080` (вкл. `/healthz`, `/metrics`) за TLS |
| 2 | Per-install API-пакета | ОДНА CA `kfw-install-ca` на оба воркера (расширение существующего `deploy/tls/gen.sh`): серверный серт `pgserver.crt` (SAN: `pgworker`, `localhost`, `host.docker.internal`, `127.0.0.1`), клиентские `panel.crt` (уже есть), `seed.crt`, `prometheus.crt`, `healthcheck.crt` (уже есть) — О1 |
| 3 | Docker-транспорт | tcp:// + mTLS (per-install docker-CA, `--tlsverify` на демонах) и `ssh://`-туннели; plaintext tcp остаётся для dev/тестов с warning-логом (О2) |
| 4 | SSH-механика | Worker-managed туннель: `Renci.SshNet` + `ForwardedPortLocal(127.0.0.1:0 → 127.0.0.1:2376)`, Engine-клиент ходит по выделенному локальному порту как обычный tcp:// (+TLS если задан) — переиспользует существующий tcp-путь целиком; key-аутентификация, fingerprint-pin опционален (TOFU+warning без него) — О3 |
| 5 | RBAC/docker-группы | Канон + поставка: arch/14 §2.2.1, firewall-матрица arch/13 §2, compose-комментарий `group_add`, README-инструкция по daemon-флагам `--tlsverify`; authz-плагины — вне скоупа |
| 6 | Метрики за mTLS | Скрейп по https с клиентским сертом (`prometheus.yml` tls_config) — заодно чинит сломанный t03-kafka scrape (джоба `kafkaworker:8080` сейчас plain против mTLS-грани; чек 65 на полном стенде красный) — О4 |
| 7 | E2E-транспорт | Фикстура генерирует per-install TLS-пакет (C# `CertificateRequest`, паттерн `TestPki`/`ClusterPki`) и стартует хосты по https (прецедент Provisioning_TlsClusterUp — фикстурный TLS — доменный стиль) |

## 4. Правки канона arch/ (уже внесены в этом worktree)

### 4.1. `arch/14-pgworker.md` (воркер)

- **Преамбула**: из «Границ» исключены «TLS к Docker API/SSH-туннели»;
  добавлен указатель на t03 (§1.1, §2.2–2.3) и новая граница «тонкий RBAC
  Engine API (authz-плагины)».
- **§1.1 HTTP API**: mTLS-only (env `PGW_API_TLS_{CERT,KEY,CLIENT_CA}`[+_PATH],
  единая пакета с KafkaWorker, отдельные клиентские серты панели и сида,
  `X-Api-Key`/`PGW_API_KEY` удалён, вся грань за TLS вкл. `/metrics`, скрейп
  Prometheus по mTLS); дискавери-ключ `url` — `https://`.
- **§1.1.1 сид**: транспорт — общий mTLS-канал, отдельный клиентский серт
  `seed.crt`.
- **§2.2**: endpoint-схемы хостов — `unix://` / `tcp://`(+TLS) / `ssh://`;
  НОВЫЙ §2.2.1 «Транспорт к Engine API: TLS, SSH-туннели, RBAC» — daemon-флаги
  `--tlsverify`, per-install docker-CA (`deploy/tls/gen-docker.sh`),
  клиентский серт `pgworker-docker.crt`, SSH-механика (ForwardedPortLocal,
  key, fingerprint-pin/TOFU, transient-отказ), RBAC/docker-группы (группа
  docker, `group_add`, запрет `:2375`, root-эквивалентность socket-доступа).
- **§2.3 Swarm**: `SwarmManager` — те же схемы транспорта.
- **§3.3**: value `/pgworker/api/<id>` — `https://`.
- **§4 Секреты**: новая группа 4 per-install TLS/транспорт:
  `PGW_API_TLS_*`, `PGW_DOCKER_TLS_*`, `PGW_DOCKER_SSH_KEY[_PATH]`,
  `PGW_DOCKER_SSH_FINGERPRINT`; `PGW_API_KEY` исключён.
- **§8 Конфигурация**: `PgWorker:Docker { Tls {…}, Ssh {…} }`,
  `PgWorker:Api { Tls {…} }`; advertise https-валидация.
- **§9 Риски**: R13 (SAN vs advertise-хост), R14 (SSH TOFU без pin),
  R15 (plaintext tcp остаётся для dev/тестов — канон прода + warning).

### 4.2. `arch/13-network-security.md` (матрица)

- **§2**: две строки — «PgWorker → docker-хосты (Engine API) 2376/22»
  (mTLS/ssh, `:2375` запрещён) и «панель / Prometheus → воркеры 8080»
  (HTTPS mTLS, клиентские серты `panel.crt`/`seed.crt`/`prometheus.crt`).

### 4.3. `arch/adminpanel/02-etcd-contract.md` (панель)

- **§2.3.1**: `/pgworker/api/<id>` — `url` `https://`; mTLS клиентским сертом;
  `X-Api-Key`/`PGW_API_KEY` удалён; единая пакета (§2.3.2).
- **§2.3.2**: переименование `AdminPanel:Workers:KafkaTls` →
  `AdminPanel:Workers:WorkerTls` (env `KFW_PANEL_TLS_*` →
  `WORKERS_PANEL_TLS_*` тем же релизом); ЕДИНАЯ CA на оба воркера;
  `X-Api-Key` удалён для ОБОИХ воркеров.

### 4.4. `arch/18-metrics.md` (скрейп)

- **§5.2**: джобы воркеров — `scheme: https` + `tls_config`
  (ca/`prometheus.crt`/key из TLS-пакета, контейнер прометеуса монтирует пакет
  ro); джоба `adminpanel` — http (панель без TLS, вне скоупа).

## 5. Структура и компоненты (отражение в коде)

### 5.1. Docker-транспорт — PgWorker.Docker

- **`Engine/DockerTlsOptions.cs`** (новый): `CaPem|CaPath, ClientCertPem|
  ClientCertPath, ClientKeyPem|ClientKeyPath` (секция `PgWorker:Docker:Tls`;
  env-биндинг `PGW_DOCKER_TLS_{CA,CERT,KEY}` + `_PATH` — таблица bindings по
  образцу `WorkerTlsHandler.EnvBindings`). Загрузка PEM с файловым fallback,
  PFX round-trip (паттерн `WorkerTlsHandler.Build` — macOS SslStream).
- **`Engine/DockerEngineFactory.cs`** (расширение): `Create(endpoint, hostAlias,
  DockerTlsOptions?)` — для `tcp://` при заданном TLS: `SocketsHttpHandler.
  SslOptions.ClientCertificates` (клиентский серт) +
  `RemoteCertificateValidationCallback` — цепочка против CA
  (`X509ChainTrustMode.CustomRootTrust`, `RevocationMode.NoCheck` — паттерн
  валидации `WorkerTlsHandler.ValidateChain`). Частичная TLS-конфигурация
  (например, CA без клиентского серта) → `ApplicationException` fail-fast при
  старте фабрики. unix:// — TLS игнорирует; tcp:// без TLS — как сейчас +
  warning-лог (R15). API-версия/все операции движка — без изменений.
- **`Engine/SshTunnelOptions.cs`** (новый): `KeyPem|KeyPath,
  RemoteDaemonHost="127.0.0.1", RemoteDaemonPort=2376, FingerprintSha256?,
  KeepAliveSec=15, ConnectTimeoutSec=10` (секция `PgWorker:Docker:Ssh`; env
  `PGW_DOCKER_SSH_KEY[_PATH]`, `PGW_DOCKER_SSH_FINGERPRINT`).
- **`Engine/SshHostConnection.cs`** (новый): на `ssh://[user@]host[:port]` —
  одна `SshClient` (key из PEM, RSA/Ed25519; user из endpoint; `HostKeyReceived`
  — сверка SHA-256 fingerprint: задан → строго, не задан → accept + warning
  единожды на хост) + `ForwardedPortLocal("127.0.0.1", 0 →
  RemoteDaemonHost:RemoteDaemonPort)`; фактический bound-порт отдаётся фабрике,
  которая строит штатный `DockerEngine` на `tcp://127.0.0.1:<bound>` (+TLS
  поверх, если задан — daemon на daemon-порту канонически с `--tlsverify`).
  Keepalive SSH-сессии; разрыв канала/сессии → reconnect с бэкоффом
  (следующие тики healthz/надзора честно видят хост недоступным — transient);
  `DisposeAsync` — останов форварда + disconnect. Штатные вызовы движка
  (Ping/ListContainers/Create/Exec/…) не знают о туннеле.
- **Интеграционная точка** (существующий шов, без новых слоёв):
  `DockerEngineFactory` — singleton DI (Program.cs) — получает
  `DockerTlsOptions`/`SshTunnelOptions` конструктором и расширяется: схема
  `ssh://` → создание/переиспользование `SshHostConnection` (кэш по endpoint,
  `DisposeAsync` фабрики закрывает туннели) и делегирование в
  `Create("tcp://127.0.0.1:<bound>", hostAlias)`; схема `tcp://` → TLS-handler;
  схема `unix://` — как сейчас. Драйверы (`PlainClusterDriver`/`SwarmClusterDriver`)
  и `HostEndpoint(name, endpoint)` НЕ меняются — endpoint-строки уже текут
  из конфига; HostAlias/portalloc/BusyPorts — без изменений (имя хоста из
  конфига). Парсинг схем (`unix|tcp|ssh`, дефолты порта, user@host) — чистая
  функция в фабрике (юнит-тесты). Зависимость `Renci.SshNet` — пин в
  `Directory.Packages.props`, ProjectReference только у `PgWorker.Docker`.

### 5.2. mTLS HTTP API — PgWorker.App

- **`Api/ApiTlsEndpoints.cs`** (новый, копия `KafkaWorker.App/Api/
  TlsEndpoints.cs` с ренеймом): env-биндинги `PGW_API_TLS_{CERT,KEY,CLIENT_CA}`
  + `_PATH` → `PgWorker:Api:Tls:*`; `ConfigureMtls(builder)` — Kestrel
  `ListenAnyIP(port)` с `UseHttps` (`ServerCertificate`+`ClientCertificateMode.
  RequireCertificate`+`ClientCertificateValidation` = цепочка против ClientCA);
  серты живут всё приложение (без using — хендшейк-валидация). Отличие от
  kafka: порт парсится из `ASPNETCORE_URLS`/`urls` конфига, если задан (E2E
  поднимает хост-процесс на свободном порту; kafka-жёсткий `8080` не
  переиспользуется), иначе 8080.
- **`Options.cs`**: `ApiOptions` — минус `ApiKey`, плюс `TlsOptions {
  ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath, ClientCaPem|
  ClientCaPath, AllowInsecureHttp=false }`; `DockerOptions` — плюс `Tls`,
  `Ssh`. Валидации AddOptions: `AdvertiseUrl` начинается с `https://` (или
  `AllowInsecureHttp`); частичный `Docker:Tls` — ошибка старта.
- **`Program.cs`**: `ApiTlsEndpoints.ApplyEnvOverrides` + `ConfigureMtls`
  до Build(); fail-fast: серт+ключ читаются и совпадают, ClientCA задан;
  warning-лог при `AllowInsecureHttp=true`. `ApiKeyMiddleware` из пайплайна
  удаляется; комментарий «X-Api-Key …» заменён на mTLS-канон.
- **`Api/ApiKeyMiddleware.cs`** — удаляется (заменён mTLS); `PGW_API_KEY`
  исключён из `SecretsFromEnv`/appsettings/доки.
- Healthz/metrics: логика без изменений (TLS сверху, как kafka).

### 5.3. Панель — AdminPanel.*

- **`Etcd/Workers/WorkerApiOptions.cs`**: `KafkaTlsOptions` →
  `WorkerTlsOptions` (переименование, семантика одна — единый клиентский серт
  панели на оба API); `PgApiKey` удаляется; свойство `KafkaTls` → `WorkerTls`.
- **`Etcd/Workers/WorkerTlsHandler.cs`**: таблица env —
  `WORKERS_PANEL_TLS_{CERT,KEY,SERVER_CA}` + `_PATH` →
  `AdminPanel:Workers:WorkerTls:*`; логика Build/ValidateChain — без изменений.
- **`Etcd/Workers/WorkerApiGateway.cs`**: `ApiKeyOf`/`X-Api-Key` удаляются
  полностью (pgworker и kafkaworker — только mTLS клиентским сертом handler'а);
  `X-Requested-By` сохраняется; failover по живым lease-ключам — без изменений.
- **`Etcd/Workers/WorkerHealthPoller.cs`**: без изменений логики (тот же
  named-client c TLS-handler'ом; pg-эндпоинты теперь https).
- Тесты панели: `WorkerTlsHandlerTests` — переименование/новые env-ключи;
  юнит на отсутствие `X-Api-Key` в исходящих запросах (fake-handler перехват).

### 5.4. Поставка и стенд

- **`deploy/tls/gen.sh`** (восстановить в git — сейчас скрипт НЕ закоммичен:
  `deploy/tls/.gitignore` вида `*` проглотил его в e14ba9c; README и
  `00-up.sh` на него ссылаются): `.gitignore` → `*`, `!.gitignore`, `!gen.sh`,
  `!gen-docker.sh`; выпущенные файлы остаются вне git. Расширение: `pgserver.
  crt/key` (CN=`pgworker`, SAN: `pgworker`, `localhost`, `host.docker.internal`,
  `127.0.0.1` — R13), `seed.crt/key`, `prometheus.crt/key` (клиентские серты
  той же `kfw-install-ca`); идемпотентность (перегенерация только при
  отсутствии ca.pem) — как сейчас.
- **`deploy/tls/gen-docker.sh`** (новый): per-install docker-CA
  (`CN=pgw-docker-ca`) + клиентская пара `pgworker-docker.crt/key` +
  серверный серт демона по аргументу (`gen-docker.sh <host-dns|ip>` — SAN по
  аргументу). Канон использования — arch/14 §2.2.1.
- **`deploy/docker-compose.yml`** (pgworker): volume `pgw-api-tls:/tls:ro`,
  env `PGW_API_TLS_{CERT,KEY,CLIENT_CA}_PATH=/tls/pgserver.crt|/tls/pgserver.key|
  /tls/ca.pem`, `PgWorker__Api__AdvertiseUrl: https://host.docker.internal:8080`,
  `PGW_API_KEY` удалён; комментарий RBAC (`group_add: ["<gid docker>"]` —
  пример в комментариях, не включён по умолчанию — текущие стенды работают
  root'ом контейнера).
- **`deploy/.env.example`**: `PGW_API_KEY` удалён, TLS-секреты —
  задокументированы (пути из volume; PEM — альтернатива).
- **`docker/PgWorker.Dockerfile`**: HEALTHCHECK — mTLS-вызов (`curl -sf
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key
  https://localhost:8080/healthz`) — 1:1 с KafkaWorker.Dockerfile.
- **`dev-stand/adminpanel/docker-compose.yml`**: панель —
  `ADMINPANEL__WORKERS__WORKERTLS__*_PATH` (переименование), монтирование
  пакета уже есть; прометеус — mount `../../deploy/tls:/tls:ro` +
  env-пути tls_config.
- **`dev-stand/adminpanel/metrics/prometheus/prometheus.yml`**: джобы
  `pgworker`/`kafkaworker` — `scheme: https` + `tls_config` (О4: чинит
  сломанный kafka-скрейп t03).
- **`dev-stand/adminpanel/checks/`**: `00-up.sh` (healthz pgworker → https +
  certs), `05-seed.sh` (seed-вызов → `https://localhost:8080` с
  `seed.crt`), `20-alerts.sh`, `65-metrics.sh` (воркер- curls → https +
  `--cacert/--cert/--key`); `dev-stand/seed.sh` — https + `seed.crt`.
- Чеки kafka (05-seed kafka, 57) уже https — не трогаются.

### 5.5. Тесты

- **Юниты** (`PgWorker.UnitTests`): парсинг endpoint-схем
  (unix/tcp/ssh, дефолты порта, user@host), сборка TLS-handler'а
  (клиентский серт подан, callback установлен — паттерн
  `WorkerTlsHandlerTests`), fail-fast частичной TLS-конфигурации,
  `SshTunnelOptions`-биндинг + fingerprint-семантика (строго/TOFU),
  `ApiTlsEndpoints.ApplyEnvOverrides` (PEM и _PATH), валидации
  AdvertiseUrl-https, plan-функции туннеля без сети (target-вычисление).
- **Интеграция** (`PgWorker.IntegrationTests`, docker):
  - **TLS к Engine API**: контейнер `nginx:alpine` (stream proxy: `listen
    <port> ssl` → `unix:/var/run/docker.sock`, серты фикстуры, sock-mount)
    через testcontainers → `DockerEngineFactory` на
    `tcp://localhost:<mapped>` + TLS-конфиг → Ping/ListContainers/create/delete
    одноразового контейнера; без TLS-конфига — plaintext путь не сломан
    (существующие DockerDriverTests).
  - **SSH-туннель**: sshd-контейнер (key-аутентификация, фикстурный keypair;
    внутри — socat `TCP-LISTEN:<daemon-port>,fork → unix:/var/run/docker.sock`)
    → endpoint `ssh://testuser@localhost:<mapped>` → Ping/ListContainers
    через туннель; fingerprint-pin задан → подключается; неверный pin → отказ.
  - **mTLS API**: порт `MtlsApiTests` (реальный сокет, самоподписанный пакет
    фикстуры): без клиентского серта — отказ хендшейка; с сертом — 200;
    `/healthz` за TLS; `AllowInsecureHttp=true` — WAF-фабрики (in-memory)
    работают; ModuleInitializer выставляет `PgWorker__Api__Tls__AllowInsecureHttp`
    до первого `Program.Main` (паттерн 6a7c6e1).
  - Существующие API-фабрики (`PgWorkerApiFactory`, pg-`MetricsApiFactory`)
    — `AllowInsecureHttp=true` в конфиге фабрик.
- **E2E** (`E2eFixture`): фикстура генерирует per-install пакет
  (`CertificateRequest`, CN=`pgw-e2e-ca`) → PEM в env
  (`PGW_API_TLS_CERT/KEY/CLIENT_CA` — PEM-дуализм env освобождает от файлов)
  + `ASPNETCORE_URLS=https://127.0.0.1:<port>` + advertise
  `https://127.0.0.1:<port>`; пробы готовности и клиентские вызовы —
  HttpClient с клиентским сертом и CA-доверием фикстуры. Маркер-кейс
  `Scale_AddEmptyShard` остаётся гейтом (теперь за mTLS).

## 6. Фазы реализации (скелет для plan-фазы)

1. **Ф1 Docker-транспорт**: `DockerTlsOptions`/`SshTunnelOptions`/парсинг
   схем/TLS-handler/`SshHostConnection` (+`Renci.SshNet` в packages);
   юнит-тесты транспортного слоя; интеграционные TLS-proxy и SSH-tunnel кейсы.
2. **Ф2 mTLS API воркера**: `ApiTlsEndpoints`, Options/валидации, Program.cs,
   удаление `ApiKeyMiddleware`/`PGW_API_KEY`; MtlsApiTests-порт + WAF-фабрики.
3. **Ф3 Панель**: `WorkerTls` переименование, env-ключи, gateway без X-Api-Key;
   юниты панели.
4. **Ф4 Поставка**: gen.sh (восстановление в git + pg/seed/prometheus-серты),
   gen-docker.sh, deploy-compose, Dockerfile HEALTHCHECK, .env.example.
5. **Ф5 Стенд**: stand-compose (панель env, прометеус mount), prometheus.yml
   (https+tls_config), чеки 00/05/20/65 + seed.sh → https; полный стенд
   00-up.sh зелёный на mTLS (вкл. починенный kafka-скрейп).
6. **Ф6 E2E**: фикстурный TLS-пакет, https-хосты; маркер `Scale_AddEmptyShard`
   + полный E2eFixture на свежем Release.
7. **Ф7 Полировка**: README стенда/deploy (настройка хостов: daemon-флаги,
   gen-docker.sh, group_add), прогоны серий с зачисткой контейнеров/сетей,
   roadmap-чистка (§9).

## 7. Ограничения и out of scope

- **TLS etcd** (клиент воркера/панели → etcd) — вне задачи; etcd-транспорт
  остаётся http в закрытой сети (отдельный roadmap-пункт при желании).
- **KafkaWorker.Docker** (TLS/ssh к Engine API у kafka-воркера) — не трогаем
  (kafka стоит на одном хосте через unix-socket; симметрия — по потребности).
- **Ротация CA/сертов** — перегенерация per-install пакета оператором;
  автоматическая ротация/двойное доверие — roadmap (t07-kafka-ca-rotation
  покрывает kafka-часть; per-install пакета — та же механика при необходимости).
- **Тонкий RBAC Engine API** (authz-плагины, split-привилегии Docker EE) —
  граница зафиксирована в arch/14 (преамбула §Границы); Docker CE даёт только
  coarse-grained docker-группу.
- **TLS панели** (UI :5050) — вне скоупа (стенд — http по cookie-логину).
- **SSH для etcd-трафика/Patroni** — вне скоупа (etcd endpoints прямо в сети;
  туннели — только docker-домен).
- **Парольная аутентификация SSH** — не поддерживаем (только key; пароль в
  env-секрете хуже key-пары).
- **Тюнинг TLS** (шифры/TLS 1.3-only) — дефолты .NET Kestrel/SslStream.
- Plaintext `tcp://` Engine API остаётся рабочим (тесты/локальные стенды) —
  не убираем (R15), канон прода — защищённые схемы.

## 8. Критерии приёмки

1. **Юнит**: парсинг endpoint-схем и дефолтов; TLS-handler (серт подан,
   callback/CA); fail-fast частичной конфигурации; fingerprint-семантика
   (строго/TOFU+warning); `ApiTlsEndpoints` env-биндинги; валидации
   https-advertise; отсутствие `X-Api-Key` в gateway панели.
2. **Интеграционные (docker)**: TLS к Engine API (nginx-proxy + серты —
   Ping/ListContainers/create/delete); SSH-туннель (sshd+socat — Ping/
   ListContainers; неверный pin → отказ); mTLS API (без серта — отказ
   хендшейка, с сертом — 200, `/healthz` за TLS, `AllowInsecureHttp` — WAF);
   portalloc/t90/e2e-профиль тесты не сломаны (plaintext/unix путь).
3. **E2E Release (мерж-гейт)**: свежий Release; маркер
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
   -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` — зелёный
   (хосты воркера на https с фикстурным mTLS-пакетом); полный E2eFixture 8/8.
4. **Стенд**: `dev-stand/adminpanel/checks/00-up.sh` поднимает полную систему:
   pgworker на mTLS (`/healthz` по https с сертами), панель ходит в оба API
   клиентским сертом (мутации pg-домена работают), сид наливается по https,
   чек 65 — все scrape-джобы up ВКЛЮЧАЯ kafka (чинит gap t03-kafka), чеки
   05/20 зелёные.
5. **Поставка**: `deploy/tls/gen.sh` и `gen-docker.sh` в git, воспроизводят
   полный пакет (вкл. `pgserver/seed/prometheus` + docker-CA) с нуля;
   Dockerfile-HEALTHCHECK pgworker зелёный; `PGW_API_KEY` нигде не осталось
   (compose/.env.example/код/доки).
6. **Код**: TreatWarningsAsErrors чисто; тесты с AAA-комментариями; порты в
   тестах — динамические; зачистка контейнеров/сетей docker после каждой серии;
   нигде не осталось `ApiKeyMiddleware`/`PGW_API_KEY`/`X-Api-Key` для
   PgWorker и `KafkaTls`-именования в панели.

## 9. Roadmap-чистка (мерж-гейт)

- Запись `t03-docker-tls-ssh` удаляется из `arch/roadmap/pgworker.md` тем же
  коммитом мержа (вкл. `←`-зависимости других пунктов, если появятся).
- В `t08-unify-adminpanel-duplicates` (arch/roadmap/pgworker.md) дописывается
  третья группа дублей: `ApiTlsEndpoints`/TlsEndpoints (PgWorker.App ↔
  KafkaWorker.App) и TLS-валидационные хелперы — унификация тем же проходом.

## 10. Принятые решения (интервью 2026-09-05)

Все четыре развилки закрыты пользователем — приняты рекомендованные варианты;
§3/§5 и правки канона arch/ написаны под них (дополнительных правок не
требуют).

| # | Развилка | Принято |
|---|---|---|
| О1 | Per-install API-пакета | **Единая CA `kfw-install-ca` на оба воркера** — расширить `deploy/tls/gen.sh` (pgserver/seed/prometheus-серты); один клиентский серт панели на оба API, одна ротация пакета на релиз |
| О2 | Plaintext tcp:// к Engine API | **Разрешён + warning-лог** — dev/тесты (testcontainers, локальные стенды) не ломаются; канон прода — только 2376+mTLS или ssh (arch/14 §2.2.1, R15) |
| О3 | SSH-механика | **Worker-managed туннель**: `Renci.SshNet` + `ForwardedPortLocal` → daemon `127.0.0.1:2376` (+TLS); переиспользует tcp-путь 1:1; демон на daemon-порту — канон с `--tlsverify` |
| О4 | Скрейп метрик за mTLS | **Обе джобы (pgworker + kafkaworker) тем же релизом** — prometheus.yml (https + tls_config), mount пакета в прометеус, чек 65; попутно закрывает сломанный t03-kafka scrape |

Открытых вопросов не осталось — детали имплементации (именование тестовых
классов, точечные сигнатуры, конфиги тест-контейнеров) решаются в plan-фазе.
