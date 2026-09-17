# Spec: valkey-домен AdminPanel (t03-valkey-panel)

- **Дата**: 2026-09-17
- **Roadmap**: [`arch/roadmap/valkey.md`](../../../arch/roadmap/valkey.md), тег `t03-valkey-panel` (снимается тем же коммитом мержа в `main` — мерж-гейт, §10)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t03-valkey-panel` (ветка `feat-t03-valkey-panel`)
- **Тип**: новая функциональность — домен панели: etcd-инспекция `/valkey/`, REST API + React-панель, live-пробы, алерты, операции (мутации через API ValkeyWorker), стендовая интеграция
- **Канон**: [`arch/20-valkey-clusters.md`](../../../arch/20-valkey-clusters.md) (ключи `/valkey/`+`/valkeyworker/`), [`arch/21-valkeyworker.md`](../../../arch/21-valkeyworker.md) (исполнитель; мутации — его HTTP API §1.1). Панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §11 (чтение+мутации) + §2.3.3 (дискавери API), [`arch/adminpanel/03-panels.md`](../../../arch/adminpanel/03-panels.md) §8 (REST/UI/алерты), [`arch/adminpanel/04-local-stand.md`](../../../arch/adminpanel/04-local-stand.md) §1/§2.4/§3 (стенд) — **дописаны этой задачей до кода** (§1.2). **Образец реализации** — Kafka-домен панели (`KafkaSnapshot*`, `KafkaParser`, `KafkaQuery`, `Operations/Kafka/*`, `pages/kafka-cluster/`): домен valkey проще (standalone `nodes=1`, нет топиков/ребалансов/lifecycle-заявок) — механика переносится 1:1 с усечением.
- **Живой исполнитель** (слит в main, t02): `src/ValkeyWorker.*` — HTTP API на mTLS `:8080` (`create`/`delete`/`config`/`resources`/`rotate app|admin`/`seed`/`restart`), ставит lease-ключ `/valkeyworker/api/<id>`. Спецификация воркера — [`docs/superpowers/2026-09-16-t02-valkey-worker/spec.md`](../2026-09-16-t02-valkey-worker/spec.md).

## 1. Цель

Реализовать **четвёртый домен AdminPanel** — valkey (разделяемый кеш, канон arch/20):

1. **Инспекция etcd**: домен-снапшот `ValkeySnapshot` (отдельный от pg/kafka; свой refresher, тик 3 с, теми же `EtcdOptions`) — кластеры/ноды/состояния/endpoints/заявки ротаций + дискавери API воркера + целевой серт `/workers/api_tls/valkeyworker` (02 §11.1, §2.3.3).
2. **REST API**: `GET /api/valkey/clusters[/{c}]` (инспекция) + **5 мутаций-прокси** в API ValkeyWorker (02 §11.2): создание/удаление кластера, конфиг-мутация `maxmemory_*`, ресурсы ноды, заявка ротации пароля app|admin.
3. **React-панель**: страница списка `ValkeyClusters` + страница деталей `ValkeyClusterDetails` (вкладка «Нода», модалы создания/конфига/ресурсов/ротаций/удаления) — по образцу kafka-домена; пункт меню, роуты `/valkey`, `/valkey/:cluster`.
4. **Live-пробы**: тик 15 с, RESP `AUTH admin + PING` по `endpoints` из etcd (креды — internal-стор, наружу не отдаются) → `live` в DTO ноды (только факт живости; INFO-телеметрия — t05).
5. **Алерты**: `ValkeyAlertEngine` — 8 kinds (03 §8.4): not-initialized/to-remove (info), node-not-running/endpoints-missing (critical), rotation-pending (info), key-malformed (warning), worker-api-unreachable (critical)/worker-unhealthy (warning) по `/valkeyworker/api/`.
6. **Грань «Воркеры»**: valkeyworker — третья карточка (инстансы/health/серт/перезапуск; `WorkerApiGateway`, `WorkerCertService`, `WorkerHealthPoller` расширяются списком воркеров).
7. **Сводки**: `GET /api/alerts` объединяет три движка (kind `valkey-*`); `GET /api/overview` — valkey-сводка (`clustersTotal`, `clustersCritical`).
8. **Стенд**: сервис `valkeyworker` в `dev-stand/adminpanel/docker-compose.yml` (профиль `valkey`), полный подъём `00-up.sh`, valkey-сид в `05-seed.sh` (демо-кластер `demo` — контейнер `vwk-demo-node1` живёт), чек `51-valkey-api.sh` (полный цикл через панель→воркер), чистка в `90-down.sh` (04 §1/§2.4/§3).
9. **Мерж-гейт** (§10): roadmap-правка — снять тег `t03-valkey-panel`.

### 1.1. Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Клиент live-пробы valkey | **Собственный RESP-миниклиент** в `AdminPanel.Probes` (порт `ValkeyWorker.Core.ValkeyConnection`: AUTH+PING; ~100–150 строк, ноль новых внешних пакетов; лёгкий фейк для юнитов) |
| Объём live-пробы | **Только PING-живость** (`live` да/нет + probeError в DTO ноды); INFO-телеметрия (used_memory/evicted/clients) — территория `t05-valkey-metrics` (arch/18) |
| Стендовая интеграция | **Полный контур + сид**: сервис в compose-профиле `valkey` входит в полный `00-up.sh`; `05-seed.sh` наливает демо-кластер через `POST /api/seed/demo` живого воркера (контейнер живёт — панель показывает живые данные); чек `51-valkey-api.sh` — полный цикл; `90-down.sh` чистит (04 §2.4) |
| Отмена заявки ротации из панели | **Без отмены**: UI/API отмену не даёт (заявка исполняется тиками за секунды, окно «передумать» мало; зависшая заявка — runbook). Противоречие канона закрыто: arch/20 §3 — «del — только воркером; панель заявку не пишет и не снимает» (§1.2) |
| Перечень arch-правок | **Полный** (§1.2): adminpanel/02 §11+§2.3.3+§9.9, 03 §8, 04 стенд, 01 упоминания, arch/21 §1.1, arch/20 §3 |

Остальное — буква канона arch/20–21 и панельных проекций 02 §11/03 §8 (не оспорены):
`nodes=1` всегда (v1); панель etcd valkey-домена **только читает** (мутации — через API
воркера); `app_user`/`app_password` панель не читает вовсе (парсер пропускает молча —
02 §11.1); `admin_*` — только для проб, в UI/API не отдаются; из `/valkeyworker/` панель
читает только `rotations/` и `api/`; валидации и дефолты 02 §11.3 (512 MiB, `allkeys-lru`,
1/1Gi/10Gi, инвариант R3 `maxmemoryBytes < memGi`).

### 1.2. Arch-правки (arch-first — внесены ДО кода, отдельным коммитом ветки)

| Файл | Правка |
|---|---|
| `arch/adminpanel/02-etcd-contract.md` | преамбула: домен Valkey §11; **§2.3.3** «дискавери API ValkeyWorker» (lease-ключи `/valkeyworker/api/`, mTLS той же пакеты, health-поллер); **§9.9**: valkeyworker в перечне воркеров сертов + ключ `/workers/api_tls/valkeyworker`; **§11** «Valkey (чтение + мутации)»: §11.1 читаемые ключи (модель/правила секреров), §11.2 таблица 5 мутаций (протоколы исполнителя = живой код t02), §11.3 валидация |
| `arch/adminpanel/03-panels.md` | §3 таблица панелей: Воркеры + ValkeyWorker; §3.7 грань «Воркеры» — три карточки; **§8** «Valkey: панель и REST API» (§8.1 эндпоинты, §8.2 DTO, §8.3 UI + форма §8.3.1, §8.4 каталог алертов); бывший §8 «Версионирование контракта» → §9 |
| `arch/adminpanel/04-local-stand.md` | §1 таблица профилей: `kafka` (фактическое состояние — был вне таблицы) и `valkey` (t03); **§2.4** сервис `valkeyworker` (сертификаты/env/порт-диапазон 17000–17999 + валkey-сид — заявка, доигрываемая воркером); §3: `00-up.sh` `--profile valkey`, чек `51-valkey-api.sh`, `90-down.sh` чистка демо-контейнера |
| `arch/adminpanel/01-architecture.md` | схема и правила потоков: мутации → API PgWorker/KafkaWorker/**ValkeyWorker**; ключи дискавери трёх воркеров |
| `arch/21-valkeyworker.md` §1.1 | «Детальные контракты эндпоинтов — t03» → ссылка на adminpanel/02 §11.2 (зафиксированы) |
| `arch/20-valkey-clusters.md` §3 | «Панель читает только `rotations/`» → «`rotations/` и `api/`»; снятие заявки ротации — только воркером (отмены из панели нет — решение §1.1); строка таблицы `rotations` согласована |

`arch/roadmap/valkey.md` этой фазой НЕ меняется (тег снимается мерж-гейтом §10).
Новый пробел, найденный при реализации, — сначала arch/, потом код, с явной фиксацией.

## 2. Принципы

1. **arch-first**: код зеркалит arch/20–21 + панельные проекции 02 §11 / 03 §8 / 04 §2.4; расхождения = брак. Сигнатуры мутаций — живой код воркера t02 (§1.2 таблица 02 §11.2 сверена с `src/ValkeyWorker.App/Api/`).
2. **Образец — Kafka-домен, переносить буквально**: где механика совпадает (домен-снапшот+refresher, secrets-стор, alert-engine, инспекция+прокси-мутации, health-поллер, UI-структура страниц, стенд-сервис) — код/структура переносится 1:1 с заменой домена; отличия valkey (одна нода, нет топиков/ребалансов/CA/lifecycle, PING вместо DescribeCluster, ротация без рестартов) — усечения фиксируются явно.
3. **Панель — читатель и прокси**: в etcd valkey-домена панель НЕ пишет ничего (как pg/kafka-домены; единственные панельные записи панели остаются §9/§9.9 общего контракта). Все мутации — `WorkerApiGateway` → HTTP API ValkeyWorker; коды маппятся 1:1 (ProblemDetails как есть; недоступность API → собственный 503).
4. **Секреты наружу не выходят**: `admin_password` живёт только в internal-сторе (для проб); `app_*` не читаются вовсе; в DTO/UI/API — никогда.
5. **Толерантность парсеров** (arch/20 §5): битый JSON → parseError-запись без исключения; неизвестный ключ → `unknownKeys`; незнакомое `state` → Active-ветка с raw-строкой; пустой `endpoints` → отсутствует; неполные креды → кластер без пробы. Транспортный провал любого KV-чтения роняет тик refresher'а (неполный снапшот хуже прежнего).
6. **Тесты — по канонам проекта** (AGENTS.md): динамические порты, guid-изоляция, полный teardown + ассерт чистоты, таймауты ≤ 100 с, зачистка контейнеров/сетей после каждой серии; комментарии тестов — AAA-нотацией.
7. **Язык**: документация/комментарии — русские; идентификаторы — английские; `.NET 10`, `Nullable=enable`, `TreatWarningsAsErrors=true`, новых внешних пакетов нет (RESP-миниклиент — свой).

## 3. Структура решения

Новых сборок нет — домен встраивается в существующие проекты панели (решётка Kafka-домена):

| Сборка | Новые файлы | Образец |
|---|---|---|
| `src/AdminPanel.Core` | `Valkey/ValkeySnapshot.cs` (модель домена), `Valkey/ValkeyAlerting/{ValkeyAlertEngine,ValkeyAlertsOptions}.cs` | `Kafka/KafkaSnapshot.cs`, `Kafka/KafkaAlerting/*` |
| `src/AdminPanel.Etcd` | `ValkeySnapshotRefresher.cs`, `ValkeySnapshotStore.cs`, `ValkeySecretsStore.cs`, `Parsing/ValkeyParser.cs`, `ValkeyPanelOptions.cs`; правки: `Workers/WorkerApiGateway.cs` (+`valkeyworker`), `Workers/WorkerCertService.cs` (+`valkeyworker`), `Workers/WorkerHealthPoller.cs` (+valkey-инстансы), `Workers/KafkaWorkerHealthStore.cs`-аналог — `Workers/ValkeyWorkerHealthStore.cs`, `ModuleExtensions.cs` (`AddValkey()`) | `KafkaSnapshotRefresher.cs`, `KafkaSecretsStore.cs`, `Parsing/KafkaParser.cs`, `KafkaPanelOptions.cs` |
| `src/AdminPanel.Probes` | `Valkey/{IValkeyProbeClient,ValkeyConnection,ValkeyProbeStore,ValkeyProbeLoop}.cs`; `ProbesOptions` + секция `Valkey` | `Kafka/*` (петля/стор/клиент) + `ValkeyWorker.Core/Valkey/ValkeyConnection.cs` (RESP) |
| `src/AdminPanel.Api` | `Inspection/ValkeyQuery.cs`; `Operations/Valkey/{ValkeyOperationsModule,ValkeyCommands,ValkeyRequests}.cs`; правки: `Inspection/OverviewQuery.cs` (+valkey-сводка), `Inspection/AlertsQuery.cs` (+valkey-движок), `Program.cs` (регистрации) | `Inspection/KafkaQuery.cs`, `Operations/Kafka/*` |
| `frontend/src` | `pages/ValkeyClustersPage.tsx`, `pages/valkey-cluster/{ValkeyClusterDetailsPage,CreateValkeyClusterModal,DeleteValkeyClusterButton,EditClusterConfigModal,EditNodeResourcesModal,RotatePasswordButton}.tsx`, `App.tsx` (роуты+меню), `api/{dto,queries}.ts` (+valkey-типы) | `pages/KafkaClustersPage.tsx`, `pages/kafka-cluster/*` |
| `dev-stand/adminpanel` | `docker-compose.yml` (сервис `valkeyworker`, профиль `valkey`), `checks/{00-up.sh,05-seed.sh,51-valkey-api.sh,90-down.sh}` | сервис `kafkaworker`, чек `50-kafka-api.sh` |
| `src/tests/AdminPanel.UnitTests` | `ValkeyParserTests.cs`, `ValkeyModelTests.cs`, `ValkeyRefresherTests.cs`, `ValkeyAlertRulesTests.cs`, `ProbesValkey/*` (RESP-клиент + петля), `Operations/Valkey/*` (прокси-команды) | `Kafka*Tests.cs`, `ProbesKafka/*` |
| `src/tests/AdminPanel.IntegrationTests` | `ValkeyApiTests.cs`, `ValkeySnapshotIntegrationTests.cs` (etcd testcontainers: refresher+инспекция по сиду) | `InspectionApiTests`, `EtcdSnapshotIntegrationTests` |

Ссылки между сборками не меняются (Core ← Etcd ← Probes/Api — уже есть). Код
`ValkeyWorker.*`/`KafkaWorker.*`/`PgWorker.*` **не трогается** (RESP-миниклиент панели —
своя копия по образцу, ссылок на сборки воркеров нет).

## 4. Компоненты (детали реализации)

### 4.1. Модель домена (`AdminPanel.Core/Valkey/ValkeySnapshot.cs`)

```csharp
// Домен-снапшот Valkey (arch/02 §11.1): отдельный от EtcdSnapshot/KafkaSnapshot —
// своя механика тика, теми же настройками endpoints. Immutable; refresher строит
// новый и атомарно заменяет в ValkeySnapshotStore.
public sealed record ValkeySnapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<ValkeyClusterInfo> Clusters,
    IReadOnlyList<ValkeyRotationTicket> Rotations,     // /valkeyworker/rotations/ (arch/20 §3)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,     // живые ключи /valkeyworker/api/ (arch/02 §2.3.3)
    IReadOnlyList<WorkerHealth> WorkerHealth,          // опрос /healthz живых инстансов
    IReadOnlyList<ValkeyProbeResult> Probes,           // live-PING пробы (§4.6)
    IReadOnlyList<Alert> Alerts,                       // ValkeyAlertEngine (03 §8.4)
    IReadOnlyList<KeyParseError> ParseErrors,          // битые JSON valkey-ключей (arch/20 §5)
    int UnknownKeyCount,
    WorkerApiCert? WorkerApiCert = null);              // целевой серт /workers/api_tls/valkeyworker (02 §9.9)

// Кластер /valkey/clusters/<C>/ (arch/20 §2): config + state + факт (nodes/endpoints).
public sealed record ValkeyClusterInfo(
    string Name,
    ValkeyClusterState State,                // Active|NotInitialized|ToRemove; отсутствие state = Active
    int Nodes,                               // всегда 1 (v1)
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long? CreatedUnix,
    string? Endpoints,                       // null/пусто — воркер не дописал (алерт у Active)
    IReadOnlyList<ValkeyNodeInfo> NodesList, // node1 (v1 — один элемент)
    ValkeyRotationTicket? Rotation);         // живая заявка ротации (джойн по кластеру)

// Нода node<k>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|UNREACHABLE|
// REMOVING|TO_REMOVE; толерантно к новым); Live — из PING-пробы (null — проба молчит).
public sealed record ValkeyNodeInfo(
    string Name, string? State,
    decimal? Cpu, int? MemGi, int? DiskGi,   // заявка resources (парсинг "4Gi" → int)
    bool? Live, string? ProbeError);

// Заявка ротации /valkeyworker/rotations/<C> (arch/20 §3): role app|admin + аудит.
public sealed record ValkeyRotationTicket(
    string Cluster, string Role, long RequestedUnix, string? RequestedBy);

// Результат live-пробы кластера (§4.6): одна нода в v1.
public sealed record ValkeyProbeResult(
    string Cluster, string Node, bool Live, long CheckedUnix, string? Error);
```

`ValkeyClusterState` — enum `Active|NotInitialized|ToRemove` + `Parse(raw)` толерантно
(незнакомое → Active — arch/20 §5). Маппинг `state` — по образцу `KafkaClusterStates`.

### 4.2. Парсер (`AdminPanel.Etcd/Parsing/ValkeyParser.cs`)

Вход `Range("/valkey/clusters/")` → `(Clusters, UnknownKeyCount, ParseErrors)`; отдельные
функции `ParseRotations` (`Range("/valkeyworker/rotations/")`). Приёмочный критерий —
канонические примеры arch/20 §2.1 дословно:

- `config`-заявка `{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}`;
- Active-config **без** поля `state`; `TO_REMOVE` — с полем;
- `endpoints` `"host.docker.internal:17001"`; пустой/пробельный → отсутствует (arch/20 §5);
- `resources` `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` (`cpu` — decimal invariant; `mem`/`disk` — `<int>Gi` → `MemGi`/`DiskGi`, неканонический суффикс → поле null, не ошибка);
- `app_user`/`app_password` — **пропуск молча** (без `unknownKeys`: ожидаемые игнорируемые — 02 §11.1);
- `admin_user`/`admin_password` — не в модель кластера: их извлекает refresher в secrets-стор (§4.4; парсер их тоже пропускает молча);
- толерантность — все 6 строк таблицы arch/20 §5 (битый JSON → parseError; незнакомое `state` → Active; неполные креды → кластер просто без пробы);
- ротация `{"role":"app","requested_unix":…,"requested_by":…}`: `role` — raw-строка (толерантно, не только app|admin), битый JSON → parseError-запись, ключ не трогаем.

Фикстуры — `tests/AdminPanel.UnitTests/EtcdFixtures/` (`.json`, по образцу kafka).

### 4.3. Refresher и сторы (`AdminPanel.Etcd`)

- **`ValkeySnapshotRefresher`** — порт `KafkaSnapshotRefresher` 1:1: `BackgroundService`,
  тик `AdminPanel:Valkey:RefreshIntervalSeconds` (default 3), range по четырём префиксам
  (`/valkey/clusters/`, `/valkeyworker/rotations/`, `/valkeyworker/api/`, точечный ключ
  `/workers/api_tls/valkeyworker`) на активном endpoint (sticky + failover по кругу);
  транспортный провал любого чтения роняет тик (`FailTick`: прежние данные,
  `EtcdReachable=false`, счётчик+1, алерты пересчитываются); первый тик сразу.
  Сборка: парсеры → креды в `ValkeySecretsStore` (§4.4) → мердж live-проб (из
  `IValkeyProbeReader`, как `MergeRuntime` kafka: `Live`/`ProbeError` в `NodesList`) →
  `ValkeySnapshot` → `ValkeyAlertEngine.Evaluate(built, previous)` (двухаргументная —
  §4.7; live-данные уже в снапшоте) → атомарная замена в `ValkeySnapshotStore`.
- **`ValkeyPanelOptions`** (`AdminPanel:Valkey { RefreshIntervalSeconds=3 }`) — по образцу `KafkaPanelOptions`.
- Регистрация — `ModuleExtensions.AddValkey()` (стор/refresher/hosted-service), вызывается из `Program.cs` рядом с `AddKafka()`.

### 4.4. Секреты проб (`ValkeySecretsStore`) и дискавери-правило

- `IValkeySecretsStore` — internal-словарь `Cluster → (AdminUser, AdminPassword)`;
  заполняет refresher из тех же KV `/valkey/clusters/` (парсинг сегментов ключа, как
  `ReadSecrets` kafka): полный набор `admin_user`+`admin_password` → запись; частичный —
  пропуск без ошибки (ensure воркера в процессе). В модель/UI/API не попадает никогда.
- Цель пробы — `endpoints` кластера из снапшота (при nodes=1 — единственный адрес),
  прогон через `HostMapResolver` (порядок 02 §6: адрес из etcd → override `HostMap` →
  прямое подключение) — на стенде `host.docker.internal` резолвится панелью в докере.

### 4.5. Расширения общей инфраструктуры воркеров

- **`WorkerApiGateway.ResolveEndpoints`**: + case `"valkeyworker" => valkeyStore.Current?.WorkerEndpoints` (DI: `IValkeySnapshotStore`). Мутации и рестарт идут по тем же правилам failover/broadcast.
- **`WorkerCertService`**: `worker is not ("pgworker" or "kafkaworker" or "valkeyworker")` — три воркера во всех трёх валидациях (ключ `/workers/api_tls/valkeyworker`; 02 §9.9 — та же механика generate/upload/delete + проверка «не затрагивает исходящие»).
- **`WorkerHealthPoller.RunOnceAsync`**: + valkey-блок (по образцу kafka-блока): endpoints из `IValkeySnapshotStore`, результат в `IValkeyWorkerHealthStore`; тот же клиентский серт, тот же интервал.
- **`GetWorkersQuery`** (WorkersModule): третья строка `new("valkeyworker", …)` — карточка на грани «Воркеры» появляется сама (фронт рендерит по данным).

### 4.6. RESP-миниклиент и петля проб (`AdminPanel.Probes/Valkey`)

- **`ValkeyConnection`** (`IValkeyProbeClient`) — копия паттерна
  `ValkeyWorker.Core/Valkey/ValkeyConnection.cs` в namespace панели: TCP + RESP,
  команды ровно две: `AUTH <admin_user> <admin_password>` + `PING` → bool
  (PONG). Одна проба = одно короткоживущее соединение, таймаут
  `AdminPanel:Probes:Valkey:TimeoutSec` (default 3). Ретраев внутри нет — следующий
  тик и есть ретрай (симметрия refresher'а).
- **`ValkeyProbeLoop`** — `BackgroundService`, тик `AdminPanel:Probes:Valkey:IntervalSec`
  (default 15, `Enabled=true`): по снапшоту `ValkeySnapshot` для каждого Active-кластера
  с `endpoints` и полным набором кредов в `ValkeySecretsStore` — PING; результат
  `ValkeyProbeResult` → `ValkeyProbeStore` (per-cluster последняя проба). Ошибка/нет
  кредов → `Live=false`+`Error` / кластер без пробы (`live=null` в DTO). Пробы не
  блокируют KV-тик и наоборот (переносится из стора успешным тиком — симметрия kafka).
- **`ProbesOptions.Valkey`**: `{ Enabled=true, IntervalSec=15, TimeoutSec=3 }`.
- Фейк `IValkeyProbeClient` для юнитов: in-memory (флаг «отвечает», счётчик AUTH-кредов).

### 4.7. Алерты (`ValkeyAlertEngine`, 03 §8.4)

Чистая функция `(ValkeySnapshot next, ValkeySnapshot? prev) → Alert[]`; sinceUnix — по
стабильному `id = kind:target` (механика pg/kafka); сортировка severity → kind → target.
Пороги — `AdminPanel:ValkeyAlerts` (`ValkeyAlertsOptions.FreshProvisioningSeconds=60`).
Каталог (все 8):

| kind | severity | Условие |
|---|---|---|
| `valkey-cluster-not-initialized` | info | state=NOT_INITIALIZED |
| `valkey-cluster-to-remove` | info | state=TO_REMOVE |
| `valkey-node-not-running` | critical | Active-кластер, нода state ∉ {RUNNING}, кроме fresh-PROVISIONING (< 60 с; детекция по prev-снапшоту — порт `IsFreshProvisioning` kafka: PROVISIONING наблюдался и тик назад) |
| `valkey-endpoints-missing` | critical | Active без `endpoints` (arch/20 §5) |
| `valkey-rotation-pending` | info | живая заявка `/valkeyworker/rotations/<C>` |
| `valkey-key-malformed` | warning | parseError-запись valkey-ключей (arch/20 §5) |
| `worker-api-unreachable` | critical | `WorkerEndpoints` пуст (target `valkeyworker`; формулировка/Hint/Remedy — по образцу kafka-варианта, 03 §8.4) |
| `worker-unhealthy` | warning | живой ключ, `/healthz` ≠ 200 (target `valkeyworker/<id>`) |

Hint/Remedy — по образцу kafka-алертов (runbook/движок), тексты — русские.

### 4.8. Инспекция API (`Inspection/ValkeyQuery.cs`)

- `GET /api/valkey/clusters` → `ValkeyClusterSummaryDto[]` (03 §8.2): из снапшота;
  `nodesRunning` — по `NodesList[].State == "RUNNING"`; `rotationPending` — живая заявка.
- `GET /api/valkey/clusters/{cluster}` → `ValkeyClusterDto` (404 — кластера нет в
  снапшоте); джойн live-проб (`NodesList.Live`) и ротации.
- **`OverviewQuery`**: `OverviewDto` + поле `Valkey` (`OverviewValkeyDto` —
  `ClustersTotal`, `ClustersCritical`: critical-алерты kinds
  `valkey-node-not-running`/`valkey-endpoints-missing`; null до первого тика) — порт
  `MapKafka`.
- **`AlertsQuery`**: объединение движков pg+kafka+valkey (kind уже различает `valkey-*`).

### 4.9. Мутации (`Operations/Valkey`)

Пять команд-прокси (панель валидацию NOT делает — сервер-воркер источник истины; фронт
дублирует для UX). Общий Error-хендлер — общий с pg/kafka-модулем (503 панели при
`WorkerApiUnavailableException`; ProblemDetails воркера как есть; `X-Requested-By` —
username сессии). Тела запросов и ответы — DTO воркера t02 (03 §8.2):

| Эндпоинт панели | Прокси в API воркера | Успех | DTO ответа |
|---|---|---|---|
| `POST /api/valkey/clusters` | `POST /api/valkey/clusters` | 201 | `ValkeyClusterCreatedDto` |
| `DELETE /api/valkey/clusters/{c}` | `DELETE …/clusters/{c}` | **202** (демонтаж асинхронный — процесс B) | — |
| `PUT /api/valkey/clusters/{c}/config` | `PUT …/clusters/{c}/config` | 200 | `ValkeyConfigUpdatedDto` |
| `PUT /api/valkey/clusters/{c}/nodes/{node}/resources` | `PUT …/nodes/{node}/resources` | 200 | `ValkeyResourcesUpdatedDto` |
| `POST /api/valkey/clusters/{c}/password/rotate` | `POST …/password/rotate` | 202 | `ValkeyPasswordRotatedDto` |

Отказы воркера маппятся 1:1 (400/404/409/503). Ротация — тело `{role: "app"|"admin"}`
(400 при ином — валидирует воркер). Успех не дёргает refresher принудительно: следующий
тик (3 с) подхватывает новые ключи (02 §4).

### 4.10. Фронтенд

- **Роуты/меню** (`App.tsx`, layout): `/valkey` → `ValkeyClustersPage`, `/valkey/:cluster` → `ValkeyClusterDetailsPage`; пункт «Valkey» (после Kafka).
- **`ValkeyClustersPage`**: таблица имя / state-бейдж / нода running / endpoints (сокращённо) / maxmemory (человекочитаемо MiB/GiB) + policy / бейдж ротации (флаг `rotationPending` — 03 §8.2; role и возраст — в шапке деталей кластера); кнопка «Создать кластер» → `CreateValkeyClusterModal` (поля 03 §8.3.1: имя, maxmemory MiB def 512, policy select из 8 канонических def `allkeys-lru`, ресурсы 1/1/10; клиентская валидация-зеркало 02 §11.3 вкл. инвариант maxmemory < mem; ProblemDetails в теле формы; блокировка двойного клика).
- **`ValkeyClusterDetailsPage`**: шапка — state-бейджи, endpoints, бейдж живой заявки ротации (role + возраст — из `ValkeyClusterDto.rotation`), кнопки: «Изменить конфиг» (`EditClusterConfigModal`: maxmemory/policy; предупреждение R3), «Сменить app-пароль» / «Сменить admin-пароль» (`RotatePasswordButton` c `role`, общий компонент: предупреждение «после применения подключения со старым паролем отвергаются до перечитывания кредов — окно двух паролей»; 409 «уже запрошена» — текстом), «Удалить кластер» (`DeleteValkeyClusterButton`: красная, подтверждение; при TO_REMOVE все кнопки мутаций скрыты; `canMutate` = Active); вкладка **Нода** — name/state/resources/live(+probeError) + «Изменить ресурсы» (`EditNodeResourcesModal`: cpu/mem/disk; подпись «применяется пересозданием контейнера — кеш восполним; disk — инфо-поле»).
- **`api/dto.ts`/`api/queries.ts`**: valkey-типы (03 §8.2) + `useValkeyClusters`/`useValkeyCluster` (polling как у kafka-запросов). `WorkersPage` не меняется (карточка приезжает из `GET /api/workers`).

### 4.11. Стенд (`dev-stand/adminpanel`)

- **Сервис `valkeyworker`** (compose, профиль `valkey`) — порт сервиса `kafkaworker`:
  build `docker/ValkeyWorker.Dockerfile`, image `valkeyworker:dev`, сеть стенда,
  `restart: unless-stopped`, volumes `/var/run/docker.sock`, `vw-snapshots`,
  `../../deploy/tls:/tls:ro` (общая стендовая пакета per-install API-CA — та же, что у
  kfw/панели; если SAN серверного серта не покрывает `valkeyworker` — расширить
  `deploy/tls/gen.sh` SAN-списком, деталь фазы, проверяется чеком 51), `extra_hosts
  local:host-gateway`; env: `ValkeyWorker__Etcd__Endpoints__0=http://etcd:2379`,
  `ValkeyWorker__AdvertisedClientHost=host.docker.internal`,
  `ValkeyWorker__Api__AdvertiseUrl=https://valkeyworker:8080`,
  `VWK_API_TLS_{CERT,KEY,CLIENT_CA}_PATH=/tls/…`,
  `ValkeyWorker__Api__EnableSeedEndpoint=true`. API-порт на хост НЕ публикуется
  (панель — по compose-DNS; отличие от kfw: его 8082 хост-порт больше не нужен для
  valkey — чеки ходят через панель). Хост-порты нод — portalloc 17000–17999: на одном
  docker-хосте один valkey-контур (стенд ИЛИ deploy) — зафиксировано в 04 §2.4.
- **`00-up.sh`**: `--profile valkey` в подъёме + wait-on-healthy valkeyworker
  (heartbeat: живой ключ `/valkeyworker/api/<id>` ≤ 15 с + тик).
- **`05-seed.sh`**: + блок valkey — `POST /api/seed/demo` живого воркера (stand: curl с
  клиентским сертом из `deploy/tls`; идемпотентен); затем wait: кластер `demo` Active
  (config без state) ≤ бюджета тиков воркера (NodeBootSec-граница, не хардкод
  ожидания вечностью — таймаут с внятной ошибкой).
- **`checks/51-valkey-api.sh`** (bash+jq, стиль `50-kafka-api.sh`): login → панель
  видит сид `demo` (список/детали/нода RUNNING/endpoints) и `live=true` (PING-проба);
  полный цикл с RunTag-именем: create (201) → NOT_INITIALIZED → RUNNING ≤ бюджета →
  конфиг-мутация (maxmemory; converge ≤ пары тиков — проверить по панельным данным) →
  resources (пересоздание контейнера: PROVISIONING → RUNNING) → ротации app+admin (202,
  заявка видна бейджем, исполняется: ключ исчезает) → delete (202) → контейнера
  `vwk-<tag>-*` и ключей `/valkey/clusters/<tag>/` нет (чистота); 409-ветки (дубль
  имени; двойная ротация). Финал — демо-контур остаётся (сид живёт).
- **`90-down.sh`**: + остановка valkeyworker и удаление демо-контейнера
  `vwk-demo-node1` (тому/сети — compose down как обычно).

## 5. Тесты

### 5.1. Юниты (`src/tests/AdminPanel.UnitTests`)

- **`ValkeyParserTests`**: канонические примеры arch/20 §2.1 (приёмочные) + толерантность
  — все строки таблицы §5 + пропуск `app_*`/`admin_*` молча + `resources`-форматы
  (`"4Gi"` → 4; неканонический суффикс → null) + ротации (role raw, битый JSON).
- **`ValkeyModelTests`**: state-маппинг (незнакомое → Active), мердж live-проб.
- **`ValkeyRefresherTests`**: тик на фикстурах (все префиксы), провал одного чтения →
  отказный тик (прежние данные, счётчик), креды → secrets-стор (полный/частичный набор),
  мердж проб, WorkerApiCert-парсинг.
- **`ValkeyAlertRulesTests`**: все 8 kinds вкл. fresh-PROVISIONING-окно и гашение;
  sinceUnix по стабильному id.
- **`ProbesValkey/ValkeyConnectionTests`**: RESP-клиент против эфемерного
  TCP-сервера-заглушки (PONG/ошибка AUTH/таймаут).
- **`ProbesValkey/ValkeyProbeLoopTests`**: петля на фейке клиента — live по кредам,
  кластер без кредов/без endpoints не пробится, ошибка → `Live=false`+Error.
- **`Operations/Valkey`**: прокси-команды (маппинг кодов 1:1, 503 при недоступности,
  `X-Requested-By`), `ValkeyQuery`-мапперы (summary/details/overview).
- Комментарии — AAA-нотацией.

### 5.2. Интеграционные (`src/tests/AdminPanel.IntegrationTests`)

- **`ValkeySnapshotIntegrationTests`**: etcd (Testcontainers, динамический порт) + сид
  ключей канонических примеров → refresher `RefreshOnceAsync` → снапшот/секреты/ротации;
  битые ключи → parseError-записи без исключения.
- **`ValkeyApiTests`**: `WebApplicationFactory` на реальном etcd-контейнере: инспекция
  (список/детали/404), `/api/workers` содержит valkeyworker-карточку; мутации —
  сквозной путь панели против подменённого HttpMessageHandler (заглушка API воркера:
  коды/тела/ProblemDetails) — маппинг команд дополнительно покрыт юнитами §5.1.
- Изоляция: свой etcd-контейнер на класс, guid-префиксы ключей, teardown + ассерт
  чистоты (ни контейнера, ни ключа префикса). Таймауты ≤ 100 с.

### 5.3. Стендовый чек (E2E-гейт)

`checks/51-valkey-api.sh` (§4.11) на свежем образе панели (Release-сборка
`adminpanel:dev` инкрементальная no-op) — мерж-гейт задачи. Docker-E2E PgWorker
(`Scale_AddEmptyShard`) НЕ требуется — код воркеров не трогается (AGENTS.md правило
про воркер-код: `AdminPanel.*` — не воркеры).

## 6. Фазы (порядок исполнения)

1. **Arch-правки** (§1.2) — уже внесены; отдельный коммит ветки «arch: valkey-домен
   панели (t03)».
2. **Модель + парсер** (TDD): `ValkeySnapshot.cs` + `ValkeyParser` + фикстуры
   (канонические примеры §2.1, толерантность §5).
3. **Refresher + сторы**: `ValkeySnapshotRefresher`/`ValkeySnapshotStore`/
   `ValkeySecretsStore`/`AddValkey()`; юниты тика.
4. **Инфраструктура воркеров**: `WorkerApiGateway`/`WorkerCertService`/
   `WorkerHealthPoller` (+`ValkeyWorkerHealthStore`)/`GetWorkersQuery` — valkeyworker.
5. **Алерты** (TDD): `ValkeyAlertEngine` + опции; `AlertsQuery`/`OverviewQuery` —
   сводные.
6. **Live-пробы** (TDD): RESP-миниклиент → `ValkeyProbeLoop`/`ValkeyProbeStore`/
   `IValkeyProbeReader` → мердж в refresher.
7. **REST API**: `ValkeyQuery` (инспекция) + `ValkeyOperationsModule` (5 мутаций);
   интеграционные `ValkeyApiTests`.
8. **Фронтенд**: dto/queries → список → детали+вкладка Нода → модалы; роуты/меню.
9. **Стенд**: compose-сервис + серты (gen.sh SAN при необходимости) + `00-up.sh` +
   `05-seed.sh` + `51-valkey-api.sh` + `90-down.sh`.
10. **Мерж-гейт** (§10): roadmap-правка тем же коммитом.

Каждая фаза — чистая сборка (`TreatWarningsAsErrors`) + зелёные тесты своей области;
тестовые серии с docker-контуром — с зачисткой после каждой (AGENTS.md).

## 7. Ограничения

- **Не входит**: клиентская библиотека Puzzle (`t04-valkey-discovery-lib`); коллектор
  INFO-метрик и Grafana-дашборд (`t05-valkey-metrics` — панельная проба только
  PING-живость); TLS клиентских подключений и ключ `ca_pem` (`t06-valkey-tls`);
  топологии nodes>1/реплики/sentinel (roadmap); отмена заявки ротации (решение §1.1);
  INFO/CONFIG GET в панельной пробе.
- **Код воркеров** (`src/{PgWorker,KafkaWorker,ValkeyWorker}.*`) и `Shared.*` не
  трогается; RESP-миниклиент — своя копия в панели (ссылок на сборки воркеров нет).
  Правки `WorkerApiGateway`/`WorkerCertService`/`WorkerHealthPoller`/`GetWorkersQuery` —
  файлы `AdminPanel.*` (расширение списка воркеров, kafka-поведение не меняется).
- **`app_password` панель не читает** (02 §11.1); `admin_password` — только internal-стор
  проб; в DTO/UI/API креды не отдаются никогда.
- Панель в etcd valkey-домена не пишет ничего (мутации — только API воркера);
  `/valkeyworker/` читает только `rotations/` + `api/`.
- Локально собираемый `adminpanel:dev`/`valkeyworker:dev` в registry
  `192.168.0.1:5000` НЕ класть.
- Тесты: никаких хардкод-портов (динамические/testcontainers), никаких sleep > 30 с,
  полный teardown + ассерт чистоты.

## 8. Метрики успеха (функциональные критерии)

1. Стенд (полный `00-up.sh`): панель `/valkey` показывает демо-кластер `demo` — Active,
   нода RUNNING, endpoints, `live=true` (PING-проба с admin-креда из etcd).
2. Создание кластера из UI → 201 → бейдж «не инициализирован» → ≤ пары тиков воркера
   RUNNING; конфиг-мутация применяется без рестарта контейнера (converge D); мутация
   ресурсов — пересозданием контейнера (автоконверге C); обе видны в UI ≤ ~10 с.
3. Ротации app и admin: 202, бейдж заявки в UI, заявка исполняется (ключ исчезает,
   креды в etcd обновлены); 409 на дубль.
4. Удаление кластера: 202 → демонтаж воркером → кластер исчезает из UI; ни контейнера,
   ни ключей.
5. Алерты: все 8 kinds зажигаются/гаснут на сконструированных состояниях (стоп-контейнер
   → node-not-running; del endpoints → endpoints-missing; битый ключ → key-malformed;
   остановка воркера → worker-api-unreachable; и т.д.).
6. Грань «Воркеры»: карточка ValkeyWorker (инстансы/health/серт-applied/перезапуск).
7. `GET /api/overview` — valkey-сводка; `GET /api/alerts` — алерты трёх доменов.
8. После teardown — ни контейнеров `vwk-*` тега, ни ключей guid-префиксов (ассерт).

## 9. Roadmap-правки (мерж-гейт)

Снять тег `t03-valkey-panel`: удалить пункт из `arch/roadmap/valkey.md` — тем же
мерж-коммитом (правила `arch/roadmap/README.md`; сам, без команды). Стрелок
`← t03-valkey-panel` у остальных пунктов нет (проверено по текущему файлу).

## 10. Критерии приёмки

1. Существуют: `AdminPanel.Core/Valkey/`, `AdminPanel.Etcd/{ValkeySnapshotRefresher,
   ValkeySnapshotStore, ValkeySecretsStore, Parsing/ValkeyParser}`,
   `AdminPanel.Probes/Valkey/`, `AdminPanel.Api/{Inspection/ValkeyQuery,
   Operations/Valkey}`; сборка `dotnet build` чистая (TreatWarningsAsErrors);
   `dotnet test` юнитов и интеграции зелёный (после зачистки серий — ни остаточных
   контейнеров, ни сетей).
2. Инспекция: refresher читает ровно `/valkey/clusters/` + `/valkeyworker/{rotations,
   api}/` + `/workers/api_tls/valkeyworker`; канонические примеры arch/20 §2.1 —
   приёмочные тесты парсера; толерантность §5 покрыта (все строки таблицы).
3. REST API 03 §8.1: инспекция + 5 мутаций-прокси 02 §11.2 (коды 1:1 с воркером,
   202 у delete/rotate); креды в ответах отсутствуют.
4. Live-проба: PING с admin-креда (RESP-миниклиент панели), `live` в DTO ноды,
   креды наружу не отдаются; INFO-телеметрии нет.
5. Алерты 03 §8.4: все 8 kinds реализованы и покрыты; `/api/alerts` и `/api/overview`
   включают valkey-домен.
6. Грань «Воркеры»: карточка valkeyworker (серт `/workers/api_tls/valkeyworker`,
   generate/upload/delete/restart, health-поллер).
7. Фронтенд: `/valkey` + `/valkey/:cluster` (список, детали, вкладка Нода, 5 модалов
   мутаций по 03 §8.3); валидация-зеркало 02 §11.3 вкл. инвариант R3.
8. Стенд: сервис `valkeyworker` (профиль `valkey`) в полном `00-up.sh`; `05-seed.sh`
   наливает `demo` (контейнер `vwk-demo-node1` живёт); чек `51-valkey-api.sh` проходит
   на свежем образе панели; `90-down.sh` чистит демо-контур.
9. Arch-правки §1.2 в ветке ДО кода (порядок коммитов фиксирует arch-first); канон
   и код не расходятся.
10. Мерж-коммит: тег `t03-valkey-panel` снят из `arch/roadmap/valkey.md` (§9).
