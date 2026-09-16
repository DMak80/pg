# Spec: сервис ValkeyWorker (t02-valkey-worker)

- **Дата**: 2026-09-16
- **Roadmap**: [`arch/roadmap/valkey.md`](../../../arch/roadmap/valkey.md), тег `t02-valkey-worker` (снимается тем же коммитом мержа в `main` — мерж-гейт, §10)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t02-valkey-worker` (ветка `feat-t02-valkey-worker`)
- **Тип**: новая функциональность — фоновый сервис-оркестратор (исполнительная сторона декларативного контракта)
- **Канон** (готов, задача t01): [`arch/20-valkey-clusters.md`](../../../arch/20-valkey-clusters.md) (контракт etcd `/valkey/` + `/valkeyworker/`), [`arch/21-valkeyworker.md`](../../../arch/21-valkeyworker.md) (оркестратор). **Образец реализации** — KafkaWorker (`src/KafkaWorker.*`: решётка сборок, циклы, процессы, API, тесты, поставка). Принципы синхронизации — [`arch/17-synchronization-principles.md`](../../../arch/17-synchronization-principles.md).

## 1. Цель

Реализовать сервис **ValkeyWorker** (.NET 10, фоновый хост с mTLS HTTP-гранью
`:8080`) — исполнительную сторону контракта arch/20–21:

1. **Пять процессов** (arch/21 §5): Provisioning (A, V0–V5, готовность =
   PING с admin-кредом), Deprovisioning (B, X0–X3, порядок «сначала docker,
   потом etcd»), NodeSupervisor (C: пересоздание снесённого контейнера,
   UNREACHABLE по NodeDeadSec с соблюдением S7 «слепая проба — никаких
   действий» и «одно пересоздание за тик», кеш неприкосновенен на
   etcd-уровне, лестница E9 portalloc-самолечения), ConfigConverger (D:
   `CONFIG SET maxmemory/maxmemory-policy` без рестартов + идемпотентный
   converge ACL-плана), PasswordRotator (E: окно двух паролей E1→E2→E3 без
   рестартов).
2. **Координация** `/valkeyworker/` — переиспользование `Shared.Etcd`
   (`ClaimStore`/`PortAllocLock`/`WorkJournal`, `keyPrefix="/valkeyworker"`;
   префикс — уже параметр конструктора, нового кода координации не пишется).
3. **HTTP API** (mTLS-грань, arch/21 §1.1) — полный набор мутаций
   valkey-домена (§4.7; решение пользователя) + `POST /api/seed/demo` за
   флагом + `POST /api/restart`; воркер — единственный писатель `/valkey/` и
   `/valkeyworker/`.
4. **Docker-поставка**: `docker/ValkeyWorker.Dockerfile` + сервис
   `valkeyworker` в `deploy/docker-compose.yml` (хост-порт API 8082 —
   продолжение ряда pg 8080 / kfw 8081); образ нод `valkey/valkey:9.1.2` —
   строка в `dev-stand/images/images.txt` + зеркалирование в
   `192.168.0.1:5000` (мульти-арх, ранбук `docs/runbook.md`).
5. **Тесты**: юниты (TDD, фейки etcd/docker/RESP), интеграционные
   (testcontainers-etcd + локальный docker-хост, реальный образ ноды),
   docker-E2E свежего Release (изоляция по `docs/e2e-isolation.md`,
   телеметрия по `docs/e2e-launch.md`).
6. **Мерж-гейт** (§10): roadmap-правки — снять тег `t02-valkey-worker` и
   зависимость `t05-valkey-metrics ← t02-valkey-worker`; добавить новый
   пункт унификации docker-движков (§10.2).

### 1.1. Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Пин версии образа `valkey/valkey` | **9.1.2** — текущий стабильный мажор (новый домен рождается на актуальной линейке, как KafkaWorker на `apache/kafka:4.0.0`); `Images:Node` default + строка в `images.txt` |
| Объём HTTP API в t02 | **Полный набор мутаций** по сигнатурам-образцу KafkaWorker (create/delete, конфиг-мутации, resources, ротации app/admin, seed, restart) — детальные контракты-доки эндпоинтов остаются за t03 (там панель) |
| Клиент воркера к Valkey-нодам | **Собственный RESP-миниклиент** в `ValkeyWorker.Core` (~150–250 строк: AUTH, PING, CONFIG GET/SET, ACL LIST/ACL SETUSER): ноль новых внешних зависимостей, полный контроль ACL-синтаксиса (окно двух паролей), лёгкие фейки для юнитов; мультиплексирование воркеру не нужно |
| Docker-движок | **Своя копия** kafkaworker-движка в `ValkeyWorker.Docker` (Pg/Kfw-копии существенно разошлись — 492 строки diff, унификация нетривиальна); техдолг трёх копий — новый roadmap-пункт `t07-unify-docker-engine` (§10.2). PgWorker/KafkaWorker код не трогаем — их E2E в мерж-гейте не нужен |
| Стендовая интеграция | **Только deploy-сервис** (deploy/docker-compose.yml): полный dev-стенд (`dev-stand/adminpanel/checks/00-up.sh`, чеки) — t03 вместе с панелью valkey-домена |
| Применение мутации `resources` (пробел канона, §1.2) | **Автоконверге в надзоре C**: тик сверяет лимиты контейнера (inspect: cpu/mem) с `nodes/node1/resources` — расхождение → пересоздание (одно за тик, дисциплина надзора; кеш восполним). Arch-first: дополнение arch/21 §5 C — ДО кода, в рамках t02 |

Остальные решения — буква канона arch/20–21 (не оспорены): порты 17000–17999;
контейнер `vwk-<C>-node<k>`, без volume (`--save "" --appendonly no`), без
per-cluster сети; ACL при старте аргументами командной строки; advertised-правило;
снапшоты префиксов `/valkey/`+`/valkeyworker/`; конфиг-форма arch/21 §8.

### 1.2. Проверка канона (arch-first)

При изучении найден **один пробел** канона: ключ `nodes/node<k>/resources` —
заявка панели (arch/20 §2), но процесса применения ИЗМЕНЕНИЯ лимитов контейнера
в arch/21 нет (у KafkaWorker это процесс NodeRegenerator J; надзор Valkey —
только реактивный). Решение пользователя: **автоконверге в надзоре C** (§4.5 C)
— arch/21 §5 C дополняется явной фразой (сверка лимитов inspect vs
`resources` → пересоздание, одно за тик) в рамках t02, ДО реализации надзора
(фаза 1 §7). Остальное в `arch/20`/`arch/21` противоречий и пробелов не
содержит — канон полон для реализации (процессы, фазы, ключи, txn-протоколы,
конфиг-форма, риски R1–R7); иных правок arch/ в этой задаче нет (кроме
roadmap §10). Новый пробел при реализации — остановиться, обновить arch/ ДО
кода (базовые правила п.1), с явной фиксацией.

## 2. Принципы

1. **arch-first**: код зеркалит канон arch/20–21; расхождения = брак.
   Детали, которые канон оставил t02 (точные флаги контейнера, сигнатуры
   эндпоинтов, сид-набор), фиксируются этим spec и планом — не «по вкусу».
2. **Образец-канон — переносить буквально**: где механика совпадает с
   KafkaWorker (классификация тика, циклы, клэймы, journal, portalloc+lock,
   advertised-правило, health, WAF-тесты API, поставка) — код/структура
   переносится 1:1 с заменой домена; отличия (один контейнер, нет
   inter-node сети, RESP вместо AdminClient, ACL runtime вместо JAAS,
   persistence off) — упрощения фиксируются явно.
3. **Переиспользование `Shared.*`** (унификация t08/t09): координация
   (`Shared.Etcd`: ClaimStore/PortAllocLock/WorkJournal/SnapshotJob/EtcdGateway),
   планировщики (`Shared.Core`: PlacementPlanner, PortAllocator,
   Writing/PlanPut, ValidationError), TLS-env (`Shared.Tls/TlsEnv`), метрики
   (`Shared.Metrics`: AddAppMetrics + WorkerMetricsInstrumentation), Retry
   (Polly jitter), HealthChecks (HealthCheckAbstract), Result. Новый код —
   только доменно-специфичный.
4. **Идемпотентность каждого шага; journal-before-manipulations; операции
   только под живым клэймом `<C>`; takeover ≤ TTL 15 с + тик** (arch/17,
   arch/21 §6). Атомарность etcd — txn с compare (`mod_revision` config,
   `version==0` клэймы/заявки, RMW endpoints).
5. **Кеш восполним — данные не дороже доступности** (arch/20 преамбула):
   persistence off, надзор пересоздаёт контейнер свободно (холодный старт —
   документированное поведение), деструктив только по положительному
   свидетельству (S7); кеш-ключи домена надзор не чистит никогда.
6. **Тесты — по канонам проекта** (AGENTS.md): динамические порты
   (`FreePortWindow` вне стендовых зон / `assignRandomHostPort: true`), guid
   RunTag в именах, полный teardown при любом исходе + **ассерт чистоты**
   (`docs/e2e-isolation.md`), NodeBootSec в интеграционных фикстурах ≤ 100 с,
   зачистка контейнеров/сетей после каждой серии, E2E-телеметрия
   (`docs/e2e-launch.md`: артефакты в `/tmp/pgw-e2e-artifacts-<guid>/`,
   `[PHASE]`-строки, MarkFailed — стоп без удаления). Перезапуск упавших
   тестов без анализа логов — запрещён.
7. **Язык**: документация/комментарии — русские; идентификаторы — английские;
   `.NET 10`, `Nullable=enable`, `TreatWarningsAsErrors=true`, пакеты —
   только через `Directory.Packages.props` (новых внешних пакетов нет).

## 3. Структура решения (сборки и компоненты)

Новые проекты в `src/` (решётка — зеркало KafkaWorker), все добавляются в
`src/PgWorker.slnx`:

| Сборка | Содержимое | Прообраз |
|---|---|---|
| `src/ValkeyWorker.App` | `Program.cs` (DI-композиция, fail-fast, KeyPrefix-литерал `"/valkeyworker"`), `Options.cs`, `EtcdConnectCallback`, `HealthState`, `Api/` (TlsEndpoints-паттерн, WorkerApiCertReader, ApiModule + Operations/*), `Loops/` (KeepaliveLoop, SnapshotLoop, ReconcileLoop, ValkeyClusterProcesses, ValkeyClusterClassifier), `HealthChecks/` | `KafkaWorker.App` |
| `src/ValkeyWorker.Core` | `Model/ValkeyDomain.cs` (снапшот-модель кластера), `Model/ValkeyPasswordGenerator.cs` (32 симв `[A-Za-z0-9]`), `Valkey/ValkeyConnection.cs` + `IValkeyConnection` (RESP-миниклиент), `Writing/ValkeyWriting.cs` | `KafkaWorker.Core` |
| `src/ValkeyWorker.Etcd` | `Parsing/ValkeySnapshotParser.cs` (толерантный парсер `/valkey/clusters/`) | `KafkaWorker.Etcd` |
| `src/ValkeyWorker.Provisioning` | `Processes/*` (Provisioning, Deprovisioning, NodeSupervisor, ConfigConverger, PasswordRotator, ClusterSecretEnsurer, PortAllocIndex, PortAllocHealer, NodeArgsBuilder, ProcessCommon), зависимость от `ValkeyWorker.Docker` | `KafkaWorker.Provisioning` |
| `src/ValkeyWorker.Docker` | `Engine/` (DockerEngine, DockerEngineFactory, IDockerEngine), `Drivers/` (PlainClusterDriver, SwarmClusterDriver, IClusterDriver, HostEndpoint) — копия kafkaworker-движка | `KafkaWorker.Docker` |
| `src/tests/ValkeyWorker.UnitTests` | юниты с фейками (FakeEtcd/IClusterDriver/IValkeyConnection, FixedTimeProvider) | `KafkaWorker.UnitTests` |
| `src/tests/ValkeyWorker.IntegrationTests` | Etcd-группа (координация с префиксом `/valkeyworker` — смоук), Valkey-группа (реальные контейнеры), Api-группа (WAF in-memory), E2E-фикстура | `KafkaWorker.IntegrationTests` |

Ссылки: App → все; Provisioning → Core/Etcd/Docker/Shared.*; Etcd → Shared.Etcd;
Core → Shared.Core. Никаких зависимостей на `PgWorker.*`/`KafkaWorker.*`.

## 4. Компоненты (детали реализации)

### 4.1. Конфигурация (`ValkeyWorkerOptions`)

Форма — arch/21 §8 (буква):

```
ValkeyWorker:Etcd { Endpoints[] }
ValkeyWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                      PortRange{From=17000,To=17999}, Images{Node="valkey/valkey:9.1.2"} }
ValkeyWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000 }
ValkeyWorker:Thresholds { NodeBootSec=120, NodeDeadSec=90 }   // тесты: NodeBootSec ≤ 100
ValkeyWorker:Parallelism { MaxClusters=4 }
ValkeyWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
ValkeyWorker:AdvertisedClientHost=null   // стенды — host.docker.internal
ValkeyWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false,
                   Tls { ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath,
                         ClientCaPem|ClientCaPath, AllowInsecureHttp=false } }
// env-секреты: VWK_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH] (арх/21 §4)
```

Fail-fast при старте (порт KafkaWorker): пустые `Etcd:Endpoints`; пустой
`Api:AdvertiseUrl`; AdvertiseUrl не `https://` при `AllowInsecureHttp=false`;
`Docker:Mode=Swarm` без `SwarmManager`; `Mode=Plain` без `Hosts`.

### 4.2. Точка входа `Program.cs`

Порт `KafkaWorker.App/Program.cs` (DI-композиция в том же порядке):
- etcd-клиент: `AddHttpClient("etcd")` + `EtcdConnectCallback.CreateHandler`
  (SocketsHttpHandler, PooledConnectionLifetime, IPv4-first — паттерн arch/21 §7);
- серверный серт API: `WorkerApiCertReader.ReadAsync(endpoints,
  "valkeyworker")` — ключ `/workers/api_tls/valkeyworker` (пишет панель,
  adminpanel/02 §9.9), приоритет etcd > env (`VWK_API_TLS_*`, TlsEnv);
  thumbprint применённого серта — в `/valkeyworker/api/<id>`;
- координация: `ClaimStore("/valkeyworker", …, advertiseUrl, thumbprint)`,
  `PortAllocLock("/valkeyworker", …, claimStore.InstanceId)`,
  `WorkJournal("/valkeyworker", …)` — литерал префикса в одном месте;
- `SnapshotJob` (префиксы снапшота `/valkey/` + `/valkeyworker/`);
- метрики-каркас: `AddAppMetrics("ValkeyWorker", …)` +
  `WorkerMetricsInstrumentation` с подпиской на `WorkJournal.PhaseWritten`
  (воркер-паттерн arch/18 §2.2: циклы/клэймы/фазы/операции/снапшоты;
  доменный коллектор INFO — t05, НЕ входит);
- драйвер docker по режиму (Plain: Hosts; Swarm: manager);
- процессы A–E + вспомогательные (§4.5–4.6), циклы (Keepalive → Snapshot →
  Reconcile), health-чеки (`valkeyworker`, `reconcile-loop`, `keepalive-loop`,
  `snapshot-loop`);
- грань: `/metrics`, `/healthz`, `/api` на одном mTLS-Kestrel `:8080`.

### 4.3. RESP-миниклиент (`IValkeyConnection`/`ValkeyConnection`, Core)

Транспорт — TCP + протокол RESP (как сервер отвечает; команды клиента —
inline/массив, разбор — по типу кадра `+ - : $ *`). Возможности (ровно
потребности процессов, ничего сверх):

| Метод | Команды | Потребитель |
|---|---|---|
| `ConnectAsync+AuthAsync` | `AUTH admin <pwd>` | все пробы воркера |
| `PingAsync` | `PING` → PONG | готовность V4, надзор C |
| `ConfigGetAsync`/`ConfigSetAsync` | `CONFIG GET maxmemory/maxmemory-policy` / `CONFIG SET …` | Converger D |
| `AclListAsync` | `ACL LIST` | Converger D (ACL-план) |
| `AclSetUserAsync` | `ACL SETUSER <user> [>pwd \| <pwd] [права]` | provisioning-аргументы не через соединение; Rotator E1/E3; Converger D |

Таймаут короткий (секунды — это пробы/команды, не ожидания), одна команда =
одно соединение или короткоживущий пул — деталь плана; ретраи — Polly jitter
поверх оркестрации (не внутри клиента). Фейк `IValkeyConnection` для юнитов —
in-memory модель пользователя/паролей/конфига (оба пароля в окне ротации).

### 4.4. Парсер снапшота (`ValkeySnapshotParser`, Etcd)

Вход `Range("/valkey/clusters/")` → `ValkeyClusterSnapshot { Config
{Nodes, MaxmemoryBytes, MaxmemoryPolicy, CreatedUnix, State?},
Nodes[node<k> → {State?, Resources?}], Endpoints?, AppUser/AppPassword?,
AdminUser/AdminPassword?, UnknownKeys, ParseErrors }`.

- **Приёмочный критерий** — канонические примеры arch/20 §2.1 парсятся
  дословно: заявка `{"nodes":1,"maxmemory_bytes":536870912,
  "maxmemory_policy":"allkeys-lru","created_unix":1756500000,
  "state":"NOT_INITIALIZED"}`; Active-конфиг без `state`; `endpoints`
  `"host.docker.internal:17001"`; `app_user`/`admin_user` строки; пароли —
  32 симв `[A-Za-z0-9]`.
- Толерантность (arch/20 §5): битый JSON → parseError-запись без исключения;
  неизвестный ключ → счётчик `unknownKeys`; незнакомое `state` → Active-ветка
  с raw-строкой; пустой `endpoints` → отсутствует; неполные креды → null-поля
  (без исключения).
- Формат `maxmemory_policy` — 8 значений канона (валидация в API, парсер
  пропускает незнакомые как raw — система развивается).

### 4.5. Процессы (Provisioning; arch/21 §5 A–E)

Все — только под живым клэймом `<C>` (ClaimStore), journal-before-manipulations
(WorkJournal), снапшот-делегат «до/после» (SnapshotJob) в provisioning и
deprovisioning. Идемпотентность: каждый шаг перепроверяет факт.

**A. ProvisioningProcess (V0–V5)** — по arch/21 §5 A дословно:
- V0 claim + journal(op=provision), снапшот «до»;
- V1 план: PlacementPlanner (группы node1..N, хосты из драйвера) + порт
  через PortAllocator под глобальным `PortAllocLock` (занятость =
  docker-публикации ∪ portalloc чужих кластеров через PortAllocIndex; не
  взял клэйм → journal `waiting-portalloc-lock`, InProgress, следующий тик);
  journal phase=planned;
- V2 ensure секретов admin+app (`ClusterSecretEnsurer`: txn put-if-absent
  `admin_user="admin"`/`admin_password`/`app_user="app"`/`app_password`;
  проигрыш txn → re-read существующих);
- V3 контейнер `vwk-<C>-node<k>` (`NodeArgsBuilder`, §4.5.1) + лимиты из
  `resources` + публикация клиентского host-порта (контейнерный 6379) +
  `state=PROVISIONING`; существующий (re-run) — сверка (args/лимиты/порт) и
  пропуск;
- V4 ждать готовности: PING с admin-кредом → PONG (бюджет NodeBootSec,
  транзиент-толерантный цикл) → `state=RUNNING`;
- V5 put `endpoints` (advertised-хост:клиентский порт); config: txn (compare
  `mod_revision`) → put канонического JSON **без** `state`; снапшот «после»;
  journal done.
- Гонка «TO_REMOVE посреди provisioning»: перечитывание config перед фазами —
  смена state безопасно прекращает процесс.

**4.5.1. Аргументы контейнера (`NodeArgsBuilder`)** — канон arch/21 §2:
команда `valkey-server` + флаги: `--user default off`, `--user admin on
><пароль> ~* +@all`, `--user app on ><пароль> ~* +@read +@write`,
`--maxmemory <bytes> --maxmemory-policy <policy>`, `--save ""`,
`--appendonly no`. Детерминизм: аргументы собираются ТОЛЬКО из etcd-факта
(декларация + креды) → пересоздание контейнера собирает актуальные пароли.
Restart-политика `unless-stopped`. Точная сборка командной строки docker
(порядок флагов, экранирование пустого `--save ""`) — план с юнит-тестом.

**B. DeprovisioningProcess (X0–X3)** — claim+journal+снапшот «до»; X1 docker:
`rm -f vwk-<C>-*` (404 = ок; томов нет); X2 etcd: `del --prefix
/valkey/clusters/<C>/` + del `/valkeyworker/{claims,work,portalloc,rotations}/<C>*`
(очистка координации ВКЛЮЧАЯ заявки ротаций); X3 снапшот «после», клэйм снят
явно (del + revoke lease).

**C. NodeSupervisor** — сверка декларации с docker-фактом + PING (admin-кред):
- снесённый контейнер (docker-факт) → пересоздание (NodeArgsBuilder, актуальные
  креды/декларация), `state=PROVISIONING`; в RUNNING — следующий тик по PING;
- **автоконверге лимитов** (решение по пробелу §1.2; дополнение arch/21 §5 C):
  тик сверяет лимиты живого контейнера (inspect: NanoCpus/Memory) с
  `nodes/node<k>/resources` (cpu/mem; disk — инфо, не сверяется) — расхождение
  → пересоздание с лимитами декларации (docker не меняет лимиты живого
  контейнера; кеш восполним — пересоздание дёшево), `state=PROVISIONING`;
  дисциплина та же — одно пересоздание за тик (по любой причине); слепой
  inspect (docker-хост молчит) — ошибка тика, пересозданий вслепую нет (порт
  слепоты надзора);
- молчание дольше NodeDeadSec (трек first_seen в `work/<C>` —
  `WriteSupervisionAsync` WorkJournal; счётчик стартует только по УСПЕШНОМУ
  зондированию) → `state=UNREACHABLE` + пересоздание (тома нет — данных не
  жалко); **слепая проба — никаких действий, трек заморожен** (S7);
- одно пересоздание за тик; ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` чужих
  процессов не трогаем;
- лестница E9: нода без записи portalloc → до деструктива — реконструкция из
  inspect живого контейнера (published-порт + host, put-if-absent под
  `locks/portalloc`, проигрыш → re-read) → только потом новая аллокация
  (PortAllocHealer);
- `endpoints` сходится к portalloc-канону тиком надзора (расхождение → RMW).

**D. ConfigConverger** — лёгкий шаг Active-ветки: `CONFIG GET
maxmemory`/`maxmemory-policy` vs `config.{maxmemory_bytes,maxmemory_policy}`
→ `CONFIG SET` при отличии (маппинг `maxmemory_bytes`→`maxmemory`,
`maxmemory_policy`→`maxmemory-policy`); ACL-план: `ACL LIST` vs канон
(admin/app, `default off`) → идемпотентный `ACL SETUSER`-converge;
`maxmemory_bytes ≥ mem`-лимита → journal-warning (ответственность оператора,
R3 arch/21).

**E. PasswordRotator** — заявка `/valkeyworker/rotations/<C>`
(`{"role":"app"|"admin",…}`, txn `version==0` ставит API); NEW = 32 симв:

```
E1 ACL SETUSER <role> >NEW    — OLD+NEW валидны, клиенты работают со OLD
E2 ОДНА txn: [compare value(<role>_password)==OLD][put NEW; del заявки]
E3 ACL SETUSER <role> <OLD    — удаление старого пароля
```

Journal-фазы между E1–E3 (отказ → повтор тика доигрывает); пересоздание
контейнера в окне безопасно (аргументы из etcd; после E2 собирается с NEW);
ротация admin не трогает app и наоборот; битая заявка — мусор: del с journal.
Без снапшотов P12 (порт KafkaWorker: точки изменений — только
provisioning/deprovisioning).

### 4.6. Циклы (App/Loops)

- **KeepaliveLoop** (первым): продление lease клэймов/лидера/instance/api
  (5 с); ставит `/valkeyworker/instances/<id>` + `/valkeyworker/api/<id>`
  `{"url","instance","since_unix","cert_thumbprint"?}`.
- **SnapshotLoop**: под лидером `/valkeyworker/leader` — регулярные снапшоты
  контроль-плейна `/valkey/`+`/valkeyworker/` (SnapshotJob, retention).
- **ReconcileLoop** (тик ScanIntervalSec=5): Range `/valkey/clusters/` →
  ValkeySnapshotParser → классификация тика (`ValkeyClusterClassifier`:
  `config.state=NOT_INITIALIZED` → A; `TO_REMOVE` → B; отсутствие state →
  Active: C → D → E; parseError-кластеры — пропуск с логом; parallelism
  MaxClusters=4; ErrorDelayMs на ошибку тика).

### 4.7. HTTP API (mTLS-грань; полный набор — решение пользователя)

Транспорт: тот же Kestrel `:8080`, mTLS per-install API-CA
(`AllowInsecureHttp=true` — только WAF-тесты); всё под префиксом `/api`.
Сигнатуры — порт KafkaWorker-образца с заменой домена; детальные
контракты-доки — t03. Guards читают etcd напрямую (без снапшота панели).

| Метод и путь | Действие | Коды |
|---|---|---|
| `POST /api/valkey/clusters` | создание: валидация → txn put-if-absent `config` (`state=NOT_INITIALIZED`, `nodes=1`, `created_unix`, `maxmemory_*`), `nodes/node1/state=NOT_INITIALIZED`, `nodes/node1/resources` | 201; 409 уже есть; 400 валидация |
| `DELETE /api/valkey/clusters/{c}` | `config.state=TO_REMOVE` (txn compare отсутствия state; повтор — идемпотентен) | 202; 404 нет |
| `PUT /api/valkey/clusters/{c}/config` | конфиг-мутация `maxmemory_bytes`/`maxmemory_policy` (converge D применит) | 200; 404; 400 |
| `PUT /api/valkey/clusters/{c}/nodes/{node}/resources` | лимиты cpu/mem (в v1 node1) | 200; 404; 400 |
| `POST /api/valkey/clusters/{c}/password/rotate` | заявка ротации `{"role":"app"\|"admin"}` → `/valkeyworker/rotations/<C>` txn `version==0` | 202; 404; 409 заявка стоит |
| `POST /api/seed/demo` | демо-кластер `demo` (флаг `EnableSeedEndpoint`, default false; идемпотентен: живой config → 200 `{"seeded":false}`) | 200/201 |
| `POST /api/restart` | graceful self-stop (применение серта/конфига рестартом) | 202 |

Валидации API (инварианты канона): имя `^[a-z][a-z0-9_]{0,62}$`; `nodes=1`
(иное — 400, реплики — roadmap); `maxmemory_policy` ∈ 8 значений канона
(дефолт `allkeys-lru`); **`maxmemory_bytes < mem`-лимит** из `resources`
(R3 — иначе OOM-килл; 400 при create; при раздельных мутациях — сверка с
текущими полями); форматы cpu/mem/disk как pg §9.3.

Seed-набор `demo`: `nodes=1`, `maxmemory_bytes=536870912`,
`maxmemory_policy=allkeys-lru`, `resources {"cpu":"1","mem":"1Gi","disk":"10Gi"}`
(512MiB < 1Gi — инвариант соблюдён).

### 4.8. Health и наблюдаемость

`/healthz` по канону честного health (t09): последнее состояние цикла, не
первый сбой; структура всегда (catch-all → Degraded с данными секций).
Секции: `etcd-reachable`, `docker-hosts`, `loops-alive`, `claims`,
`snapshot-freshness`. Логи: claim/takeover, фазы (journal), rebuild, converge.
Diag-ключи: `/valkeyworker/work/<C>`, `nodes/node<k>/state`.

## 5. Docker-поставка и образы

1. **`docker/ValkeyWorker.Dockerfile`** — порт `docker/KafkaWorker.Dockerfile`:
   multi-stage `sdk:10.0` publish `ValkeyWorker.App` → `aspnet:10.0`, curl
   для HEALTHCHECK (`/healthz` по mTLS с сертами из `/tls`), `EXPOSE 8080`.
2. **`deploy/docker-compose.yml`** — сервис `valkeyworker` по образцу
   `kafkaworker`: build `docker/ValkeyWorker.Dockerfile`, image
   `valkeyworker:dev` (в registry НЕ класть — локальный), `restart:
   unless-stopped`, ports `"${VWK_API_HOST_PORT:-8082}:8080"`, volumes
   `/var/run/docker.sock`, `vw-snapshots:/snapshots`, `vw-api-tls:/tls:ro`,
   `extra_hosts` `local:host-gateway` + `host.docker.internal:host-gateway`,
   env: `ValkeyWorker__Etcd__Endpoints__0`, `ValkeyWorker__AdvertisedClientHost`
   (default `host.docker.internal`), `ValkeyWorker__Api__AdvertiseUrl`
   (default `https://host.docker.internal:8082`), `VWK_API_TLS_{CERT,KEY,
   CLIENT_CA}_PATH=/tls/…`, `ValkeyWorker__Api__EnableSeedEndpoint`.
   Томов данных нод НЕТ (persistence off — томов у домена не бывает).
3. **Образ нод**: строка `valkey/valkey:9.1.2` в
   `dev-stand/images/images.txt` + зеркалирование мульти-арх
   (`mirror-image.sh`) в `192.168.0.1:5000` (правила — `docs/runbook.md`);
   перед стендом/docker-сериями — `pull-images.sh`. `Images:Node` default в
   appsettings — `valkey/valkey:9.1.2` (без префикса registry — образ уже в
   локальном docker, как `apache/kafka:4.0.0` у Kfw).
4. **appsettings.json** (App): дерево §4.1 с дефолтами; env-оверрайды
   `ValkeyWorker__*`.

## 6. Тесты

### 6.1. Юниты (`src/tests/ValkeyWorker.UnitTests`)

TDD (сначала тест — фазы плана привязаны к ним): RESP-клиент (парсер кадров,
AUTH/PING/CONFIG/ACL — на эфемерном TCP-сервере-заглушке), NodeArgsBuilder
(канонический набор флагов, экранирование), ValkeySnapshotParser
(канонические примеры §2.1 — приёмочные; толерантность §5 — все 6 строк
таблицы), валидации API (инвариант maxmemory < mem, 8 значений policy, имя,
nodes=1), ProvisioningProcess/DeprovisioningProcess/NodeSupervisor/
ConfigConverger/PasswordRotator (фазы, идемпотентность re-run, гонка
TO_REMOVE, слепая проба S7, E9-лестница, окно двух паролей E1–E3 — на фейках
FakeEtcd/IClusterDriver/IValkeyConnection + FixedTimeProvider),
классификатор, TlsEnv-биндинги, health-агрегат, EtcdConnectCallback.
Комментарии тестов — AAA-нотацией.

### 6.2. Интеграционные (`src/tests/ValkeyWorker.IntegrationTests`)

- **Valkey-группа** (фикстура — порт `KafkaClusterFixture`): testcontainers
  etcd (порт динамический) + ЛОКАЛЬНЫЙ docker-хост (воркер — хост-процесс);
  `AdvertisedClientHost="localhost"`; окно портов — копия `FreePortWindow`
  (поиск 21000+, стендовая зона для valkey-копии расширяется до 18000:
  15xxx pg / 16xxx kfw / 17xxx valkey-стенды; зонд на рантайме — литералов
  `:17000` в expects нет); `RunTag`-guid в именах кластеров/контейнеров;
  `NodeBootSec ≤ 100`; teardown демонтирует ВСЁ созданное + ассерт чистоты
  (ни контейнера `vwk-*` тега, ни ключа префикса). Сценарии: provisioning
  реального `valkey/valkey:9.1.2` (контейнер → PING → endpoints → config без
  state); ACL-матрица (default off; app — read/write, без админ-команд;
  admin — +@all); deprovisioning (контейнер удалён, etcd-префикс пуст,
  portalloc снят); converge (`CONFIG SET maxmemory` без рестарта контейнера);
  автоконверге лимитов (PUT resources → пересоздание с новыми лимитами);
  ротация (в окне оба пароля валидны, после E3 OLD отвергнут, заявка
  удалена); надзор (снос контейнера → пересоздание с теми же кредами;
  stop-контейнер → UNREACHABLE-путь с бюджетом).
- **Api-группа** (WAF in-memory, `AllowInsecureHttp=true` — только для
  тестов): мутации create/delete/config/resources/rotate (201/202/404/409/400
  семантика), seed-идемпотентность, restart-202, mtls-грань (отказ без
  клиентского серта), серт из `/workers/api_tls/valkeyworker` при старте.
- **Etcd-группа**: смоук координации с префиксом `/valkeyworker` (claim/
  takeover/portalloc-lock против реального etcd; глубокие протоколы уже
  покрыты `Shared.Etcd.UnitTests`).

### 6.3. Docker-E2E (`ValkeyWorker.IntegrationTests` E2E-фикстура)

По `docs/e2e-isolation.md` + `docs/e2e-launch.md`: собственный контур
(guid во всех именах): тестовая docker-сеть + etcd-контейнер + контейнер
`valkeyworker:dev` (Release-сборка фикстурой; `PGW_TEST_E2E_NOBUILD`-аналог
— только бисект), API-порт динамический; сценарий: создание кластера через
API → контейнер `vwk-<C>-node1` (host-порт из динамического окна) → PING →
проверка дискавери-ключей (`endpoints`, креды, config без state) →
RESP-проверка с app-кредом → удаление → чистота. Телеметрия: артефакты
`/tmp/pgw-e2e-artifacts-<guid>/` (docker-логи/inspect + host.log до
удаления), `[PHASE]`-строки для фаз > 60 с, MarkFailed → стоп без удаления.
Параллельные прогоны не мешают (guid + динамические порты). После каждой
серии — зачистка (контейнеры И сети — сети теста создаёт фикстура сама).

## 7. Фазы (порядок исполнения)

1. **Arch-правка (arch-first, §1.2)**: дополнение arch/21 §5 C автоконверге
   лимитов (inspect vs `resources` → пересоздание, одно за тик, слепой
   inspect — никаких действий) — ДО реализации надзора. Коммит в ветке.
2. **Каркас**: 5 сборок + 2 тест-проекта в slnx, Options + fail-fast,
   Program-DI (координация Shared с prefix `/valkeyworker`, SnapshotJob,
   метрики-каркас, health), EtcdConnectCallback, TlsEnv. Чистая сборка
   (TreatWarningsAsErrors), пустые циклы живы.
3. **Etcd-слой**: ValkeySnapshotParser (TDD: канонические примеры §2.1 +
   толерантность §5) + ValkeyWriting + координационный смоук.
4. **Docker-слой**: копия Engine/Drivers из KafkaWorker.Docker (замена
   namespace, без правок Kfw), PortAllocIndex/PortAllocHealer-каркас.
5. **RESP-клиент** (TDD: протокол + команды).
6. **Процессы** (TDD, по одному): ClusterSecretEnsurer → NodeArgsBuilder →
   Provisioning A → Deprovisioning B → NodeSupervisor C (+E9 +автоконверге
   лимитов) → ConfigConverger D → PasswordRotator E; классификатор;
   ReconcileLoop.
7. **HTTP API**: mtls-грань (WorkerApiCertReader + TlsEndpoints-паттерн),
   Operations-хендлеры полного набора §4.7 + seed + restart; WAF-тесты.
8. **Поставка**: Dockerfile, deploy-compose-сервис (8082), appsettings,
   `images.txt` + зеркалирование `valkey/valkey:9.1.2` + `pull-images.sh`.
9. **Интеграция**: Valkey-группа фикстуры + сценарии §6.2; зачистка серий.
10. **E2E**: фикстура Release + сценарий §6.3; Release-прогон зелёный.
11. **Мерж-гейт** (§10): roadmap-правки тем же коммитом.

Каждая фаза заканчивается чистой сборкой + зелёными тестами своей области;
серии тестов — с зачисткой контейнеров/сетей между ними (AGENTS.md).

## 8. Ограничения

- **Не входит** (границы arch/21 + решения §1.1): TLS клиентских подключений
  (`t06-valkey-tls`); реплики/sentinel/cluster-топологии (nodes=1 всегда);
  persistence RDB/AOF (off по канону); коллектор доменных метрик INFO и
  дашборд (`t05-valkey-metrics` — в t02 только каркас воркер-метрик);
  панель valkey-домена и контракты-доки эндпоинтов (`t03`); клиентская
  библиотека Puzzle (`t04`); полный dev-стенд/00-up.sh (`t03`); вынос
  docker-движка в Shared (`t07-unify-docker-engine`, §10.2).
- **Код PgWorker/KafkaWorker/AdminPanel не трогается** (кроме общих файлов
  поставки: `deploy/docker-compose.yml` — новый сервис, `images.txt`,
  `src/PgWorker.slnx`, `Directory.Build.props` при необходимости). Их E2E
  в мерж-гейте не требуется; mерж-гейт — свежий Release docker-E2E серии
  ValkeyWorker (кейс-маркер: provisioning→discovery→deprovisioning §6.3).
- `arch/20` не меняется; `arch/21` — единственная правка: дополнение §5 C
  автоконверге лимитов (§1.2, фаза 1); прочее — буква канона. Новый пробел,
  найденный в реализации, — сначала arch/, потом код, с явной фиксацией.
- Локально собираемые образы (`valkeyworker:dev`) в registry
  `192.168.0.1:5000` НЕ класть; туда — только внешний `valkey/valkey:9.1.2`.
- Тесты: никаких хардкод-портов/диапазонов (литералы `:17000` в expects
  запрещены), NodeBootSec ≤ 100, полный teardown + ассерт чистоты, никакие
  sleep > 30 с.

## 9. Метрики успеха (функциональные критерии)

1. Создание кластера через API (или seed) на поднятом контуре → ≤ тиков
   Reconcile: контейнер `vwk-<C>-node1` (образ 9.1.2, persistence off, ACL,
   maxmemory из декларации, лимиты resources), PING отвечает с admin-креда,
   `endpoints`/`nodes/node1/state=RUNNING` в etcd, `config` без `state`.
2. App-кред читает/пишет ключи и получает отказ на админ-команду; admin —
   `+@all`; `default` отключён; оба креда — 32 симв, в API не отдаются.
3. Конфиг-мутация `maxmemory_bytes`/`policy` применяется `CONFIG SET` без
   рестарта контейнера (converge ≤ 2 тиков); мутация `resources` —
   пересозданием контейнера с новыми лимитами (автоконверге надзора,
   одно за тик, ≤ пары тиков).
4. Ротация app: между E1 и E2 валидны оба пароля; после E3 OLD отвергнут,
   NEW работает, заявка удалена; app-ротация не трогает admin-кред.
5. Снос контейнера вручную → надзор пересоздаёт (те же креды/порт из
   portalloc) → RUNNING по PING; etcd-ключи домена не тронуты.
6. Удаление кластера (TO_REMOVE) → контейнера нет, etcd-префиксы
   `/valkey/clusters/<C>/` и координация `<C>` чисты, portalloc снят.
7. `/healthz` отдаёт структуру с секциями §4.8 при живом/деградированном
   контуре; `/metrics` экспонирует Meter `ValkeyWorker`.
8. После teardown любого сценария — ни контейнера, ни сети, ни ключа его
   guid-префикса (ассерт чистоты).

## 10. Roadmap-правки (мерж-гейт)

1. **Снять тег** `t02-valkey-worker`: удалить пункт из
   `arch/roadmap/valkey.md` и зависимость `← t02-valkey-worker` из
   `t05-valkey-metrics` — тем же мерж-коммитом (правила
   `arch/roadmap/README.md`; сам, без команды).
2. **Добавить пункт `t07-unify-docker-engine`** в `arch/roadmap/pgworker.md`
   (линия унификации t08/t09; следующий свободный номер тега — t07,
   проверено по активным тегам): три копии DockerEngine/ClusterDriver
   (PgWorker.Docker — SSH-туннели/TLS-docker, KafkaWorker.Docker,
   ValkeyWorker.Docker — копия kfw) разошлись; унификация в общую
   Shared-сборку с сохранением Pg-специфики (SSH/TLS) как опций; мерж-гейт
   той задачи — полный E2E Pg+Kfw+Valkey. Добавляется в рамках t02 (долг
   зафиксирован сразу), исполняется отдельно.

## 11. Критерии приёмки

1. Существуют `src/ValkeyWorker.{App,Core,Etcd,Provisioning,Docker}` +
   `src/tests/ValkeyWorker.{Unit,Integration}Tests` (в slnx); сборка
   `dotnet build` чистая (TreatWarningsAsErrors), `dotnet test` юнитов и
   интеграции зелёный (после зачистки серий — ни остаточных контейнеров
   `vwk-*`, ни сетей).
2. Реализованы и покрыты тестами: процессы A–E с фазами канона (V0–V5,
   X0–X3, S7/E9-надзор, converge D, окно двух паролей E1–E3), координация
   `/valkeyworker/` на Shared.Etcd (префикс-параметр), парсер с
   каноническими примерами arch/20 §2.1 и толерантностью §5.
3. HTTP API §4.7: полный набор мутаций на mTLS-грани `:8080`, серверный серт
   из `/workers/api_tls/valkeyworker` с env-фоллбеком `VWK_API_TLS_*`,
   валидации §4.7 (инвариант `maxmemory_bytes < mem`), seed за флагом,
   дискавери-ключ `/valkeyworker/api/<id>` с thumbprint.
4. Поставка: `docker/ValkeyWorker.Dockerfile`; сервис `valkeyworker` в
   `deploy/docker-compose.yml` (хост-порт 8082, volumes `vw-snapshots`/
   `vw-api-tls`, env `VWK_*`/`ValkeyWorker__*`); `valkey/valkey:9.1.2` в
   `images.txt` и зеркалирован мульти-арх в `192.168.0.1:5000`
   (`pull-images.sh` тянет).
5. Тесты соответствуют канонам: динамические порты (FreePortWindow/
   GetMappedPublicPort, литералов `:17000` в expects нет), guid-изоляция,
   teardown при любом исходе + ассерт чистоты, NodeBootSec ≤ 100,
   E2E-телеметрия (артефакты/[PHASE]/MarkFailed) — §6.
6. Docker-E2E на свежем Release зелёный (сценарий §6.3: API → контейнер →
   дискавери → RESP-проба app-кредом → удаление → чистота).
7. Код PgWorker/KafkaWorker/AdminPanel не изменён (кроме общих файлов
   поставки §8); `arch/20` не изменён; `arch/21` — только дополнение §5 C
   (автоконверге лимитов, §1.2).
8. Мутация `resources` применяется автоконверге надзора: изменение лимита →
   пересоздание контейнера с новыми лимитами ≤ пары тиков (одно за тик),
   покрыто интеграционным тестом.
9. Мерж-коммит: тег `t02-valkey-worker` снят (пункт + `← t02` из t05),
   пункт `t07-unify-docker-engine` добавлен в `arch/roadmap/pgworker.md`
   (§10).
