# t02-valkey-worker — план реализации (сервис ValkeyWorker)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — реализация по задачам, шаги
> отмечаются чекбоксами (`- [ ]`).

**Цель:** фоновый сервис ValkeyWorker (.NET 10, mTLS-грань `:8080`) —
исполнительная сторона контракта arch/20–21: пять процессов (Provisioning A,
Deprovisioning B, NodeSupervisor C, ConfigConverger D, PasswordRotator E),
координация `/valkeyworker/` на `Shared.Etcd`, полный набор HTTP-мутаций
valkey-домена, docker-поставка (образ нод `valkey/valkey:9.1.2`, deploy-сервис
на хост-порту 8082), тесты (юниты TDD + интеграция + docker-E2E), roadmap-правки
мерж-гейта.

**Архитектура:** решётка сборок — зеркало KafkaWorker (`ValkeyWorker.{App,Core,
Etcd,Provisioning,Docker}` + tests). ReconcileLoop тиком 5 с парсит `/valkey/clusters/`
и классификация: `NOT_INITIALIZED` → A, `TO_REMOVE` → B, Active → C → D → E;
всё под lease-клэймами ClaimStore, journal-before-manipulations, portalloc под
глобальным lock. Команды нодам — собственный RESP-миниклиент (TCP,
AUTH/PING/CONFIG/ACL, ноль пакетов). Docker-движок — упрощённая копия
kafkaworker-движка (без volume, без per-cluster сети, args контейнера = флаги
`valkey-server`).

**Технологии:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`),
ASP.NET Core Minimal API (mTLS-Kestrel), etcd v3 HTTP JSON gateway, Docker
Engine API через HttpClient, testcontainers (etcd), FluentAssertions + xUnit v3,
TLS через `Shared.Tls/TlsEnv`, метрики через `Shared.Metrics`.

**Spec:** `docs/superpowers/2026-09-16-t02-valkey-worker/spec.md` — план
реализует его фазы 1–11 (§7) в том же порядке (задачи 1–17 = фазы); аргументация
от spec, исполнители читают оба.

## Глобальные ограничения (из spec §2/§8; действуют для каждой задачи)

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`
  (сборка 0 warnings); пакетов НОВЫХ нет — все из `src/Directory.Packages.props`
  (Confluent.Kafka НЕ подключается: RESP-клиент свой).
- Все новые проекты — в `src/PgWorker.slnx`; зависимостей на `PgWorker.*`/
  `KafkaWorker.*`/`AdminPanel.*` нет (только `Shared.*`).
- Порты docker в тестах — только динамические: `assignRandomHostPort: true` +
  `GetMappedPublicPort`, либо рантайм-зонд `FreePortWindow` (21000+; стендовая
  зона для valkey-копии 15000–18000). Литералов `:17000` в expects нет.
  `NodeBootSec` в интеграционных фикстурах ≤ 100 с; sleep > 30 с в тестах нет.
- Каждый интеграционный/E2E тест полностью чистит за собой: teardown в
  `DisposeAsync` при любом исходе + ассерт чистоты (docs/e2e-isolation.md);
  после КАЖДОЙ docker-серии — зачистка контейнеров/сетей прогона
  (`docker rm -f` своих по guid-тегу, затем `docker network prune -f`).
- E2E-телеметрия (docs/e2e-launch.md): артефакты `/tmp/pgw-e2e-artifacts-<guid>/`
  (docker-логи/inspect + логи воркера до удаления), `[PHASE]`-строки для фаз
  > 60 с, `MarkFailed()` → teardown останавливает, но не удаляет. Перезапуск
  упавших тестов без анализа логов запрещён.
- Код PgWorker/KafkaWorker/AdminPanel не трогается (кроме общих файлов
  поставки по spec §8: `deploy/docker-compose.yml` — новый сервис,
  `dev-stand/images/images.txt`, `src/PgWorker.slnx`, при необходимости
  `src/Directory.Build.props` — без смены поведения существующих сборок).
  `arch/20` не меняется; `arch/21` — только правка Task 1 (§5 C автоконверге
  лимитов).
- Локально собираемые образы (`valkeyworker:dev`) в registry `192.168.0.1:5000`
  НЕ класть; туда — только внешний `valkey/valkey:9.1.2`.
- Язык: документация/комментарии — русские, идентификаторы — английские;
  тесты — AAA-комментариями (Arrange/Act/Assert).
- Новый пробел канона при реализации → СТОП, сначала правка arch/ с явной
  фиксацией, потом код (базовые правила п.1; spec §1.2).
- Рабочая директория shell сбрасывается между вызовами: в каждой команде
  `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-valkey-worker && …`.

## Соглашения плана

- Каждая задача: **Вход** (предусловие), **Действие** (файлы), **Выход** (что
  готово), **Проверка** (команда/критерий), **Spec** (закрываемое требование);
  внутри — bite-size TDD-шаги.
- Команды тестов: `DOTNET_CLI_UI_LANGUAGE=en dotnet test … -c Debug` (юниты)
  / `-c Release` (E2E); сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build
  src/PgWorker.slnx -c Debug`.
- «Порт <файл-образец> 1:1 с заменой X→Y» = скопировать файл из KafkaWorker,
  заменить namespace/имена по таблице замен (ниже), сохранить структуру и
  комментарии (переведя доменные упоминания kafka→valkey); это разрешено
  spec §2 п.2 («переносить буквально») и НЕ плейсхолдер — образец лежит в репо.
- Таблица замен (действует во всех «портах»):
  `KafkaWorker`→`ValkeyWorker`; `kafkaworker`→`valkeyworker`; `Kafka`→`Valkey`
  (в типах); `kfw-`→`vwk-`; `/kafka/`→`/valkey/`; `/kafkaworker`→`/valkeyworker`;
  `KFW_API_TLS_`→`VWK_API_TLS_`; `apache/kafka:4.0.0`→`valkey/valkey:9.1.2`;
  порт-дефолты `16000/16999`→`17000/17999`; `BrokerBootSec`→`NodeBootSec`;
  `broker<k>`→`node<k>`; контейнерный порт 9094→6379.
- Коммит-шаги: `git add <точные пути> && git commit -m "<conventional>"`;
  работает ветка `feat-t02-valkey-worker` (worktree уже существует).

## Файловая карта (итоговая структура новых файлов)

```
src/ValkeyWorker.Core/            Model/ValkeyDomain.cs, Model/ValkeyPasswordGenerator.cs,
                                  Valkey/IValkeyConnection.cs, Valkey/ValkeyConnection.cs,
                                  Writing/ValkeyWriting.cs
src/ValkeyWorker.Etcd/            Parsing/ValkeySnapshotParser.cs
src/ValkeyWorker.Docker/          Engine/IDockerEngine.cs, Engine/DockerEngine.cs,
                                  Engine/DockerEngineFactory.cs, Drivers/ClusterDriver.cs
src/ValkeyWorker.Provisioning/    Processes/{ProvisioningProcess, DeprovisioningProcess,
                                  NodeSupervisor, ConfigConverger, PasswordRotator,
                                  ClusterSecretEnsurer, PortAllocIndex, PortAllocHealer,
                                  NodeArgsBuilder, ProcessCommon}.cs
src/ValkeyWorker.App/             Program.cs, Options.cs, HealthState.cs,
                                  EtcdConnectCallback.cs, appsettings.json,
                                  Api/{TlsEndpoints, WorkerApiCertReader, ApiModule}.cs,
                                  Api/Operations/{ValkeyLimits, ValkeyExceptions,
                                  ValkeyApiHelpers, CreateClusterHandler, DeleteClusterHandler,
                                  UpdateConfigHandler, UpdateResourcesHandler,
                                  RotatePasswordHandler, SeedDemoHandler, RestartHandler}.cs,
                                  Loops/{KeepaliveLoop, SnapshotLoop, ReconcileLoop,
                                  ValkeyClusterClassifier, ValkeyClusterProcesses}.cs,
                                  HealthChecks/{ValkeyWorkerHealth, ServiceProbes}.cs
src/tests/ValkeyWorker.UnitTests/ App/{EtcdConnectCallbackTests, TlsEnvBindingsTests,
                                  HealthTests, ValkeyClusterClassifierTests}.cs,
                                  Core/{ValkeyConnectionTests, ValkeyPasswordGeneratorTests}.cs,
                                  Etcd/ValkeySnapshotParserTests.cs,
                                  Writing/ValkeyWritingTests.cs,
                                  Api/ValkeyValidationTests.cs,
                                  Provisioning/{NodeArgsBuilderTests, ClusterSecretEnsurerTests,
                                  ProvisioningProcessTests, DeprovisioningProcessTests,
                                  NodeSupervisorTests, ConfigConvergerTests, PasswordRotatorTests,
                                  PortAllocIndexTests, PortAllocHealerTests}.cs,
                                  Fakes/{Fakes, FakeDriver, FakeValkeyConnection,
                                  FixedTimeProvider}.cs
src/tests/ValkeyWorker.IntegrationTests/
                                  Valkey/{ValkeyClusterFixture, FreePortWindow,
                                  ProvisioningTests, AclMatrixTests, DeprovisioningTests,
                                  ConvergeTests, ResourcesAutorecreateTests, RotationTests,
                                  SupervisionTests}.cs,
                                  Etcd/CoordinationSmokeTests.cs,
                                  Api/{ValkeyApiFactory, MetricsApiFactory,
                                  ClusterMutationsApiTests, RotateApiTests, SeedApiTests,
                                  RestartApiTests, MtlsApiTests, WorkerApiCertStartupTests,
                                  MetricsApiTests}.cs,
                                  E2e/{ValkeyE2eEnvironment, ValkeyE2eLifecycleTests}.cs
docker/ValkeyWorker.Dockerfile    (новый)
deploy/docker-compose.yml         (+ сервис valkeyworker, + volumes vw-*)
dev-stand/images/images.txt       (+ строка valkey/valkey:9.1.2)
arch/21-valkeyworker.md           (правка §5 C — Task 1)
arch/roadmap/{valkey,pgworker}.md (мерж-гейт — Task 17)
```

Ответственность: Core — домен/RESP/Writing (без etcd); Etcd — парсер снапшота;
Docker — Engine API + драйверы; Provisioning — процессы A–E; App — DI, циклы,
API-грань; тесты зеркалят сборки.

---

### Task 1: Arch-правка — автоконверге лимитов в arch/21 §5 C (arch-first; spec фаза 1)

**Files:**
- Modify: `arch/21-valkeyworker.md` (раздел «### C. NodeSupervisor (надзор)»)

**Interfaces:** — (документ; фиксирует поведение, реализуемое в Task 10).

**Вход:** spec одобрен (гейт user-review пройден); worktree `feat-t02-valkey-worker`
(ветка создана, spec-каталог в рабочем дереве).

**Действие:** в подсписок раздела «C. NodeSupervisor (надзор)» после пункта
«Снесённый контейнер …» добавить пункт «Автоконверге лимитов» (решение
пользователя по пробелу §1.2 spec):

```markdown
- **Автоконверге лимитов `resources`** (применение изменений заявки): тик
  сверяет лимиты живого контейнера (inspect: NanoCpus/Memory) с
  `nodes/node<k>/resources` (cpu/mem; disk — инфо-поле, не сверяется) —
  расхождение → пересоздание контейнера с лимитами декларации (docker не
  меняет лимиты живого контейнера; кеш восполним — пересоздание дёшево),
  `state=PROVISIONING`. Дисциплина та же — **одно пересоздание за тик**
  (по любой причине); слепой inspect (docker-хост молчит) — ошибка тика,
  пересозданий вслепую нет.
```

В заголовок раздела и пункт «Лестница E9» не лезть; остальные разделы arch/21
и весь arch/20 не менять.

**Выход:** канон фиксирует применение мутации `resources`; дальнейшие задачи
аргументируются от него.

**Проверка:** `git diff arch/21-valkeyworker.md` — один добавленный пункт в §5 C.

**Spec:** §1.2 (arch-first, решение пользователя), §7 фаза 1.

- [ ] **Шаг 1. Внести правку** в `arch/21-valkeyworker.md` текстом выше.
- [ ] **Шаг 2. Коммит**:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-valkey-worker
git add arch/21-valkeyworker.md docs/superpowers/2026-09-16-t02-valkey-worker/
git commit -m "docs(arch): t02 — автоконверге лимитов resources в надзоре C (arch/21 §5 C) + spec"
```

---

### Task 2: Каркас — 7 проектов в slnx, Options, Program (живые пустые циклы), health, TLS-env (spec фаза 2)

**Files:**
- Create: `src/ValkeyWorker.Core/ValkeyWorker.Core.csproj` (+ `Model/ValkeyDomain.cs` — пока только `record NodeLimits(decimal? CpuCores, long? MemoryBytes);` с док-комментарием)
- Create: `src/ValkeyWorker.Etcd/ValkeyWorker.Etcd.csproj`
- Create: `src/ValkeyWorker.Docker/ValkeyWorker.Docker.csproj`
- Create: `src/ValkeyWorker.Provisioning/ValkeyWorker.Provisioning.csproj`
- Create: `src/ValkeyWorker.App/{ValkeyWorker.App.csproj, Program.cs, Options.cs, HealthState.cs, EtcdConnectCallback.cs, appsettings.json, Loops/KeepaliveLoop.cs, Loops/SnapshotLoop.cs, Loops/ReconcileLoop.cs, Loops/ValkeyClusterProcesses.cs, Loops/ValkeyClusterClassifier.cs, HealthChecks/ServiceProbes.cs, HealthChecks/ValkeyWorkerHealth.cs, Api/TlsEndpoints.cs, Api/WorkerApiCertReader.cs}`
- Create: `src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj` (+ `App/EtcdConnectCallbackTests.cs`, `App/TlsEnvBindingsTests.cs`, `App/HealthTests.cs`)
- Create: `src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj` (+ `AssemblyParallelism.cs` — копия kfw: `[assembly: CollectionBehavior(DisableTestParallelization = true)]`)
- Modify: `src/PgWorker.slnx` (папка `/valkey/` с 5 сборками + 2 проекта в `/tests/`)

**Interfaces (Produces, для задач 3–13):**
- `namespace ValkeyWorker.App`: `sealed class ValkeyWorkerOptions` — дерево §4.1 spec:
  `Etcd { string[] Endpoints }`; `Docker { string Mode="Plain", DockerHostOptions[] Hosts, string? SwarmManager, PortRangeOptions PortRange{From=17000,To=17999}, DockerImagesOptions Images{Node="valkey/valkey:9.1.2"} }`; `Loops { int ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000 }`; `Thresholds { int NodeBootSec=120, int NodeDeadSec=90 }`; `Parallelism { int MaxClusters=4 }`; `Snapshots { string Dir="/snapshots", int RetentionFiles=10, int MaintenanceIntervalMin=60 }`; `string? AdvertisedClientHost`; `Api { string AdvertiseUrl="", TlsOptions Tls, bool EnableSeedEndpoint=false }`; `Shared.Metrics.MetricsOptions Metrics` (базовый класс Shared.Metrics, без подкласса — коллектор t05 не входит).
  Примечание (осознанное решение плана): `Snapshots.MaintenanceIntervalMin=60`
  — перенос механики снапшотов образца kfw (конструктор `SnapshotJob` из
  Shared.Etcd требует интервал maintenance-прохода retention); в spec §4.1 и
  arch/21 §8 поле не указано (там только Dir/RetentionFiles) — дефолт 60 мин
  как у kfw, транспорентен для домена.
- `Loops`: `KeepaliveLoop`/`SnapshotLoop`/`ReconcileLoop : BackgroundService` — порты kfw 1:1 (`KeepaliveLoop(ClaimStore claimStore, ValkeyWorkerOptions opts, TimeProvider clock, ILogger<KeepaliveLoop> logger)`; продление lease клэймов/лидера/instance + `instances/<id>` + `api/<id>` `{"url","instance","since_unix","cert_thumbprint"?}`; SnapshotLoop — под лидером `/valkeyworker/leader`, префиксы `/valkey/`+`/valkeyworker/`); `ValkeyClusterProcesses` — пока интерфейс `public interface IValkeyClusterProcesses { Task TickAsync(CancellationToken ct); }` + пустая реализация (наполняется в Task 12); `ReconcileLoop` тикает `TickAsync` c `ScanIntervalSec`/`ErrorDelayMs` (порт тела kfw-цикла).
- `EtcdConnectCallback`, `TlsEndpoints`, `WorkerApiCertReader`, `HealthState`, `ServiceProbes`, `ValkeyWorkerHealth` — порты kfw 1:1 по таблице замен (серверный серт из ключа `/workers/api_tls/valkeyworker`, env-префикс `VWK_API_TLS_`; health-секции `etcd-reachable`, `docker-hosts`, `loops-alive`, `claims`, `snapshot-freshness`).
- `const string KeyPrefix = "/valkeyworker"` — единственный литерал в `Program.cs`.

**Вход:** Task 1 закоммичен.

**Действие:**
1. csproj — копии kfw по таблице замен (App: Worker-sdk, FrameworkReference
   AspNetCore, InternalsVisibleTo обоим тест-проектам, ссылки на
   Core/Etcd/Docker/Provisioning/Shared.Metrics/Shared.Tls; Core → Shared.Core;
   Etcd → Core + Shared.Etcd; Docker → Core; Provisioning → Core/Etcd/Docker +
   пакет `Microsoft.Extensions.Logging.Abstractions`; IntegrationTests — пакетный
   набор kfw (FluentAssertions, Microsoft.AspNetCore.Mvc.Testing,
   Microsoft.NET.Test.Sdk, Testcontainers, xunit.v3, coverlet) + ProjectReference
   на все 5 сборок; UnitTests — xunit/FluentAssertions без Testcontainers).
   Никаких версий пакетов в csproj (централизованное CPM).
2. slnx: `<Folder Name="/valkey/">` с 5 проектами; в `/tests/` добавить оба
   тест-проекта.
3. `Options.cs` — дерево выше (порт `src/KafkaWorker.App/Options.cs` с
   удалением kafka-специфики: TopicSync/Reassign/Metrics-подкласс).
4. `Program.cs`-каркас: Configure Options; `AddAppMetrics("ValkeyWorker", …)` +
   `WorkerMetricsInstrumentation` с подпиской на `WorkJournal.PhaseWritten`
   (порт Program.cs kfw:36–47); `AddHttpClient("etcd").
   ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler)`;
   fail-fast `ValidateOnStart` (3 правила: пустые Endpoints; пустой
   AdvertiseUrl; AdvertiseUrl не `https://` при `!AllowInsecureHttp` — ещё два
   docker-правила в фабрике драйвера, Task 4); `WorkerApiCertReader.ReadAsync(
   endpoints, "valkeyworker", CancellationToken.None)`; `TlsEndpoints.
   ApplyEnvOverrides` + `ConfigureMtls(builder, port: 8080, managedCert)`;
   thumbprint применённого серта; координация `ClaimStore`/`PortAllocLock`/
   `WorkJournal` c `KeyPrefix="/valkeyworker"`; `SnapshotJob` (префиксы
   снапшота `/valkey/` + `/valkeyworker/` — по фактической сигнатуре конструктора
   Shared.Etcd, свериться с использованием в PgWorker); циклы + health-чеки
   (`valkeyworker`, `reconcile-loop`, `keepalive-loop`, `snapshot-loop`);
   `MapAppMetrics/MapHealthChecks("/healthz")`; `public partial class Program;`
   для WAF. Процессы НЕ регистрируются (Task 12).
5. `appsettings.json` — дерево §4.1 с дефолтами (`ValkeyWorker:Docker:Hosts:
   [{"Name":"local","Endpoint":"unix:///var/run/docker.sock"}]`,
   `Images:Node = "valkey/valkey:9.1.2"`).
6. Юниты: `EtcdConnectCallbackTests`, `TlsEnvBindingsTests`, `HealthTests` —
   порты kfw-тестов 1:1.

**Выход:** решение собирается чисто; `ValkeyWorker.App` стартует с живыми
Keepalive/Snapshot/Reconcile (пустым) и `/healthz`/`/metrics` на mTLS-грани.

**Проверка:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-valkey-worker
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug
```
Обе зелёные, 0 warnings; существующие сборки PgWorker/KafkaWorker не сломаны.

**Spec:** §3 (структура решения), §4.1–§4.2, §4.8, §7 фаза 2.

- [ ] **Шаг 1.** Создать 7 csproj + slnx-правка; `dotnet build` зелёный (пустые сборки).
- [ ] **Шаг 2.** `Options.cs` + `appsettings.json`; `ValkeyDomain.cs` с `NodeLimits`.
- [ ] **Шаг 3.** Порты `EtcdConnectCallback/TlsEndpoints/WorkerApiCertReader/HealthState/ServiceProbes/ValkeyWorkerHealth` + юнит-тесты; `dotnet test src/tests/ValkeyWorker.UnitTests` зелёный.
- [ ] **Шаг 4.** `Program.cs`-каркас + циклы-заглушки; `dotnet build` зелёный.
- [ ] **Шаг 5. Коммит**:
```bash
git add src/ValkeyWorker.Core src/ValkeyWorker.Etcd src/ValkeyWorker.Docker \
  src/ValkeyWorker.Provisioning src/ValkeyWorker.App src/tests/ValkeyWorker.UnitTests \
  src/tests/ValkeyWorker.IntegrationTests src/PgWorker.slnx
git commit -m "feat(valkey): каркас ValkeyWorker — решётка сборок, Options, Program, циклы-заглушки, health, TLS-env (t02)"
```

---

### Task 3: Etcd-слой — домен-модель, парсер снапшота, Writing + координационный смоук (spec фаза 3)

**Files:**
- Modify: `src/ValkeyWorker.Core/Model/ValkeyDomain.cs` (дополнить моделью кластера)
- Create: `src/ValkeyWorker.Etcd/Parsing/ValkeySnapshotParser.cs`
- Create: `src/ValkeyWorker.Core/Writing/ValkeyWriting.cs`
- Create: `src/tests/ValkeyWorker.IntegrationTests/Etcd/CoordinationSmokeTests.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Etcd/ValkeySnapshotParserTests.cs`, `Writing/ValkeyWritingTests.cs`

**Interfaces (Produces, для задач 6–13):**
```csharp
namespace ValkeyWorker.Core.Model;

// raw-строки заявки как в etcd: cpu — "2"/"0.5", mem/disk — "1Gi"/"10Gi"
// (канон pg §9.3: только суффикс Gi). Парсер хранит raw (толерантность
// arch/20 §5); парсинг в лимиты — ProcessCommon (Task 7).
public sealed record ValkeyResources(string? Cpu, string? Mem, string? Disk);

public sealed record ValkeyClusterConfig(
    int Nodes, long MaxmemoryBytes, string MaxmemoryPolicy, long CreatedUnix, string? State);

public sealed record ValkeyNodeSnapshot(string Node, string? State, ValkeyResources? Resources);

public sealed record ValkeyClusterSnapshot(
    string Cluster,
    ValkeyClusterConfig? Config,                       // null = config-ключа нет/битый (→ ParseErrors)
    IReadOnlyDictionary<string, ValkeyNodeSnapshot> Nodes,
    string? Endpoints,
    string? AppUser, string? AppPassword,              // неполный набор кредов → null-поля независимо
    string? AdminUser, string? AdminPassword,
    IReadOnlyList<string> UnknownKeys,
    IReadOnlyList<string> ParseErrors);

public sealed record ParsedValkeySnapshot(
    IReadOnlyList<ValkeyClusterSnapshot> Clusters,
    IReadOnlyList<string> UnknownKeys, IReadOnlyList<string> ParseErrors);

namespace ValkeyWorker.Etcd.Parsing;
public static class ValkeySnapshotParser
{
    public static Result<ParsedValkeySnapshot> Parse(IReadOnlyList<Shared.Etcd.Client.Kv> kvs);
}
```
`ValkeyWriting` (Core): `static string ConfigJson(int nodes, long maxmemoryBytes,
string policy, long createdUnix)` — канонический JSON БЕЗ `state` (camelCase:
`{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru",
"created_unix":1756500000}`); `static string EndpointsValue(string host, int
port)` → `"<host>:<port>"`; `static string ResourcesJson(decimal cpu, int memGi,
int diskGi)` → `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` (канон pg §9.3, для API
Task 13); `static readonly IReadOnlySet<string> KnownPolicies` — 8 значений
канона (`allkeys-lru, allkeys-lfu, volatile-lru, volatile-lfu, allkeys-random,
volatile-random, volatile-ttl, noeviction`).

**Вход:** Task 2 закоммичен.

**Действие:** TDD.
1. Приёмочные тесты парсера — канонические примеры arch/20 §2.1 ДОСЛОВНО
   (каждый — отдельный [Fact]): заявка `config` с `state=NOT_INITIALIZED`;
   Active-конфиг без `state`; `config` с `state=TO_REMOVE`; `endpoints` =
   `"host.docker.internal:17001"` (значение-строка читается как данные ключа);
   `app_user="app"`/`admin_user="admin"`/пароли 32 симв; `nodes/node1/state=
   NOT_INITIALIZED`; `nodes/node1/resources={"cpu":"1","mem":"1Gi","disk":"10Gi"}`.
2. Толерантность — все 6 строк таблицы arch/20 §5 (Fact на каждую): битый JSON
   `config` → parseError-запись, без исключения; неизвестный ключ → счётчик
   `unknownKeys`; незнакомое `state` (напр. `"DRAFT"`) → Active-ветка с
   raw-строкой; пустой/пробельный `endpoints` → Endpoints == null; неполные
   креды (app_user без пароля) → AppPassword == null (пароль null независимо
   от юзера); `nodes=2` в config — парсер пропускает как есть (валидация — API).
3. `ValkeyWritingTests`: ConfigJson выдаёт дословно канонический JSON §2.1
   Active-вида; креды в JSON конфига НЕ попадают; EndpointsValue — формат;
   ResourcesJson — канон pg §9.3 (cpu decimal-строкой инвариантной культурой,
   mem/disk только `<n>Gi`).
4. Реализация: сегменты ключа `/valkey/clusters/<C>/…` — свитч по хвосту
   (`config`, `endpoints`, `app_user`, `app_password`, `admin_user`,
   `admin_password`, `nodes/<k>/state`, `nodes/<k>/resources`; прочее →
   unknownKeys); JSON через `JsonDocument` в Try-паттерне.
5. Координационный смоук `CoordinationSmokeTests` (интеграционный, порт
   `src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs` +
   `PortAllocLockRaceTests.cs` в урезанном виде, ОДНО отличие — prefix
   `/valkeyworker`): testcontainers-etcd (динамический порт); claim/takeover
   (второй store забирает после `ReleaseClusterAsync` первого);
   portalloc-lock взаимоисключение (второй `TryAcquireAsync` false при живом
   первом). Глубокие протоколы уже покрыты `Shared.Etcd.UnitTests` — смоук
   только на префикс. После серии — зачистка (dispose контейнера фикстурой).

**Выход:** ReconcileLoop-источник и API-гарды имеют типизированную модель;
канон JSON-записи единый; координация `/valkeyworker` подтверждена на реальном etcd.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ValkeySnapshotParser|FullyQualifiedName~ValkeyWriting"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~CoordinationSmoke"
docker ps -a --format '{{.Names}}' | grep -c etcd || true   # 0 остаточных
```
Все зелёные; после смоука контейнер etcd dispose-нут фикстурой.

**Spec:** §4.4, §6.1 (парсер), §6.2 (Etcd-группа), §7 фаза 3.

- [ ] **Шаг 1.** Приёмочные тесты §2.1 (failing) → реализация парсера → зелёные.
- [ ] **Шаг 2.** Толерантность §5 — 6 кейсов (failing) → зелёные.
- [ ] **Шаг 3.** `ValkeyWriting` + тесты (вкл. ResourcesJson канона pg §9.3).
- [ ] **Шаг 4.** Координационный смоук на `/valkeyworker` → зелёный (зачистка серии).
- [ ] **Шаг 5. Коммит**:
```bash
git add src/ValkeyWorker.Core src/ValkeyWorker.Etcd src/tests/ValkeyWorker.UnitTests src/tests/ValkeyWorker.IntegrationTests
git commit -m "feat(valkey): домен-модель, парсер /valkey/ (канон §2.1 + толерантность §5), Writing + смоук координации (t02)"
```

---

### Task 4: Docker-движок — копия kfw-движка в ValkeyWorker.Docker (spec фаза 4)

**Files:**
- Create: `src/ValkeyWorker.Docker/Engine/IDockerEngine.cs`, `Engine/DockerEngine.cs`, `Engine/DockerEngineFactory.cs`
- Create: `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`
- Modify: `src/ValkeyWorker.App/Program.cs` (DI-фабрика драйвера)

Примечание по файлам: у kfw отдельного файла фабрики НЕТ — класс
`DockerEngineFactory` объявлен внутри `src/KafkaWorker.Docker/Engine/DockerEngine.cs`
(строка ~14). Копия — `{IDockerEngine.cs, DockerEngine.cs}`; выделение фабрики
у ValkeyWorker в отдельный файл `Engine/DockerEngineFactory.cs` — допустимое
упрощение структуры (поведение то же, using в Program.cs чище).

**Interfaces (Produces, для задач 7–10):**
```csharp
namespace ValkeyWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

// Спецификация ноды: Args готовит NodeArgsBuilder (Task 6); драйвер только размещает.
public sealed record ValkeyNodeSpec(
    string Cluster, string NodeName, string Host, int ClientHostPort,
    string Image, IReadOnlyList<string> Args,
    decimal? CpuCores, long? MemoryBytes);

// E9-реконструкция portalloc: Host — хост размещения, ClientHostPort — published host-порт.
public sealed record NodeEndpointInspection(string Host, int ClientHostPort);

public interface IClusterDriver
{
    Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct);          // HostInfo — Shared.Core.Planning
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);
    Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct);
    Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct); // без removeVolume — томов нет
    Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct); // автоконверге C
    Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(string cluster, string nodeName, CancellationToken ct); // E9
    // Cmd (args) живого контейнера; null = объекта нет — сверка V3 (image+args+порт+лимиты).
    Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(string cluster, string nodeName, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);
}
```
`PlainClusterDriver`: `public const int ClientContainerPort = 6379;`
(контейнерный порт → выделенный host-порт); `internal static string NodeName(
string cluster, string node) => $"vwk-{cluster}-{node}";`. `SwarmClusterDriver`
— порт kfw с теми же упрощениями. `NodeLimits` — из Task 2 (`ValkeyWorker.Core.Model`).

**Вход:** Task 3 закоммичен.

**Действие:**
1. `Engine/*` — копия `src/KafkaWorker.Docker/Engine/{IDockerEngine.cs,
   DockerEngine.cs}` по таблице замен (фабрика — внутри DockerEngine.cs у
   kfw; в копе выделена в `DockerEngineFactory.cs`). Отличия копии:
   - `ContainerSpec` — упрощённый: `record ContainerSpec(string Image,
     IReadOnlyList<string> Cmd, IReadOnlyList<PortMap> Ports, string Hostname,
     double? CpuCores, long? MemoryBytes, string? Label)` — БЕЗ volume-полей,
     БЕЗ сети (`Network`/`NetworkAliases` не передаются — контейнер живёт в
     сети запуска воркера, arch/21 §2); `Cmd` — обязательный (флаги
     `valkey-server`, включая пустой аргумент `--save ""` — передаётся
     элементом `""` массива: экранирование решается массивом Engine API, не
     строкой); в `CreateContainerAsync` копии — `RestartPolicy
     {"Name":"unless-stopped"}` в HostConfig (arch/21 §2).
   - Методы `Exec*`, `RemoveVolumeAsync`, `VolumeExistsAsync` — НЕ переносить
     (RESP вместо exec; тома у домена нет); вместо env-инспекций kfw
     (`InspectContainerEnv*`/`InspectServiceEnv*`) — Cmd-инспекции для сверки
     args V3: `InspectContainerCmdAsync` (Config.Cmd контейнера; null = нет)
     и `InspectServiceCmdAsync` (swarm: TaskTemplate.ContainerSpec.Cmd) — по
     образцу env-методов kfw; остальное (списки, инспекции ресурсов и
     endpoint'а, busy-порты, swarm-методы, сети `Ensure/DeleteNetworkAsync`) —
     переносить (движок общий).
2. `Drivers/ClusterDriver.cs` — по интерфейсу выше: `GetHostsAsync` (фильтр
   префикса `vwk-`), `GetBusyPortsAsync`, `EnsureNodeAsync` (идемпотентность по
   имени: существующий контейнер → без пересоздания; решение о сверке/замене —
   у процессов V3), `RemoveNodeAsync` (stop+rm на всех хостах, 404 = ок;
   сетей/томов нет), `NodeResourcesAsync` (перебор хостов → inspect),
   `InspectNodeEndpointAsync` (published-порт 6379 + host), `NodeArgsAsync`
   (перебор хостов → Cmd), `ListNodeObjectsAsync` (`vwk-<C>-`).
3. `Program.cs`: `DockerEngineFactory` + выбор драйвера Plain/Swarm с
   fail-fast (порт блока Program.cs kfw:179–197): Swarm без `SwarmManager` →
   ApplicationException; Plain без Hosts → ApplicationException.

**Выход:** драйвер управляет контейнерами `vwk-<C>-node<k>` (порт 6379 →
динамический host-порт, лимиты cpu/mem, restart unless-stopped), отдаёт Cmd для
сверки V3; подключён в DI.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
```
Зелёная (0 warnings). Функциональность драйвера подтверждается интеграцией Task 15.

**Spec:** §3 (сборка Docker), §4.5 V3/C (размещение, сверка args, E9), §7 фаза 4, решение §1.1 (своя копия движка).

- [ ] **Шаг 1.** Копия `Engine/*` с упрощениями + Cmd-инспекции; `dotnet build` зелёный.
- [ ] **Шаг 2.** `Drivers/ClusterDriver.cs` по интерфейсу; `dotnet build` зелёный.
- [ ] **Шаг 3.** DI-фабрика в `Program.cs`; `dotnet build` зелёный.
- [ ] **Шаг 4. Коммит**:
```bash
git add src/ValkeyWorker.Docker src/ValkeyWorker.App
git commit -m "feat(valkey): docker-движок ValkeyWorker — копия kfw без volume/сетей/exec, Cmd-args + инспекция Cmd (t02)"
```

---

### Task 5: RESP-миниклиент + генератор паролей (TDD; spec фаза 5)

**Files:**
- Create: `src/ValkeyWorker.Core/Valkey/IValkeyConnection.cs`, `Valkey/ValkeyConnection.cs`
- Create: `src/ValkeyWorker.Core/Model/ValkeyPasswordGenerator.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Core/ValkeyConnectionTests.cs`, `Core/ValkeyPasswordGeneratorTests.cs`

**Interfaces (Produces, для задач 7/10/11):**
```csharp
namespace ValkeyWorker.Core.Valkey;

// Точка подключения + креды (admin-пробы воркера / converge).
public sealed record ValkeyEndpoint(string Host, int Port, string User, string Password);

// RESP-миниклиент (spec §4.3): одна команда = одно короткоживущее TCP-соединение
// (пробы/команды тиковые, мультиплексирование не нужно); таймаут — секунды;
// ретраи — Polly снаружи (оркестрация, не клиент). Ошибка сети/протокола →
// Result.Failed (S7: слепая проба — не исключение).
public interface IValkeyConnection
{
    Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(ValkeyEndpoint ep, string parameter, CancellationToken ct);
    Task<Result> ConfigSetAsync(ValkeyEndpoint ep, string parameter, string value, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct);
    Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct);
}
```
AUTH выполняется внутри каждой команды (после connect — `AUTH <user> <password>`,
ожидание `+OK`); конструктор `ValkeyConnection(TimeSpan connectAndCommandTimeout)`
(дефолт 5 с — это пробы, не ожидания). `ValkeyPasswordGenerator.Generate()` →
32 симв `[A-Za-z0-9]` через `RandomNumberGenerator`.

**Вход:** Task 2 закоммичен.

**Действие:** TDD на эфемерном TCP-сервере-заглушке (`TcpListener` на
`127.0.0.1:0`, фактический порт — литералов нет), отвечающем захардкоженными
RESP-кадрами:
1. Парсер кадров: `+PONG\r\n` → успех; `-ERR …` → Failed с сообщением;
   `:1`; `$5\r\nhello\r\n` и `$-1\r\n` (null); `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`;
   кадры, приходящие кусками (запись в два захода) — клиент дочитывает.
2. Фрейминг запроса: заглушка читает байты и тест ассертит протокол
   `*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$8\r\n<пароль>\r\n` (массив RESP).
3. Команды: PING → `+PONG`; AUTH не-ОК (`-WRONGPASS`) → Failed; `CONFIG GET
   maxmemory` → array → словарь `{"maxmemory":"536870912"}`; `CONFIG SET` →
   `+OK`; `ACL LIST` → массив строк; `ACL SETUSER app >pwd ~* +@read +@write`
   → `+OK`; таймаут (заглушка молчит) → Failed.
4. `ValkeyPasswordGenerator`: длина 32, `[A-Za-z0-9]`, два вызова разные;
   на 1000 генераций встречаются символы всех трёх классов.
5. Реализация: `TcpClient` + `NetworkStream`, буферное чтение до полного
   кадра, рекурсивный разбор array; запись — единый кадр-массив; `using` на
   соединение в каждом методе.

**Выход:** клиент покрывает все команды процессов A–E; генератор готов для
ClusterSecretEnsurer (Task 7).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyConnection|FullyQualifiedName~ValkeyPasswordGenerator"
```
Зелёные. Реальная нода — в Task 15.

**Spec:** §4.3, §7 фаза 5, решение §1.1 (собственный RESP-миниклиент).

- [ ] **Шаг 1.** Тесты парсера кадров (failing) → реализация → зелёные.
- [ ] **Шаг 2.** Тесты фрейминга + команд (failing) → реализация → зелёные.
- [ ] **Шаг 3.** Тест таймаута/ошибок → зелёный.
- [ ] **Шаг 4.** `ValkeyPasswordGenerator` + тесты.
- [ ] **Шаг 5. Коммит**:
```bash
git add src/ValkeyWorker.Core src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): RESP-миниклиент + генератор паролей (TDD, t02)"
```

---

### Task 6: NodeArgsBuilder (TDD) — флаги valkey-server

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/NodeArgsBuilder.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeArgsBuilderTests.cs`

**Interfaces (Produces, для задач 8/10):**
```csharp
namespace ValkeyWorker.Provisioning.Processes;

public static class NodeArgsBuilder
{
    // Канон arch/21 §2: ACL при старте + maxmemory + persistence off.
    // Детерминизм: аргументы ТОЛЬКО из etcd-факта (декларация + креды) →
    // пересоздание контейнера собирает актуальные пароли.
    public static IReadOnlyList<string> Build(
        long maxmemoryBytes, string maxmemoryPolicy, string adminPassword, string appPassword);
}
```

**Вход:** Tasks 3–5 закоммичены.

**Действие:** TDD — приёмник канонического набора:
```csharp
// AAA: канонический набор флагов valkey-server из декларации и кредов (arch/21 §2)
[Fact]
public void Build_канонический_набор_флагов()
{
    // Arrange
    long maxmemory = 536870912;
    var policy = "allkeys-lru";

    // Act
    var args = NodeArgsBuilder.Build(maxmemory, policy, "ADMINPWD32SYMBOLSxxxxxxxxxxx", "APPPWD32SYMBOLSxxxxxxxxxxxxx");

    // Assert — порядок флагов канонический (детерминизм сверки V3)
    args.Should().Equal(
        "valkey-server",
        "--user", "default", "off",
        "--user", "admin", "on", ">ADMINPWD32SYMBOLSxxxxxxxxxxx", "~*", "+@all",
        "--user", "app", "on", ">APPPWD32SYMBOLSxxxxxxxxxxxxx", "~*", "+@read", "+@write",
        "--maxmemory", "536870912",
        "--maxmemory-policy", "allkeys-lru",
        "--save", "",
        "--appendonly", "no");
}
```
Плюс тесты: `--save ""` — РОВНО один пустой элемент (не отсутствие и не
`\"\"`-литерал); разные пароли/maxmemory → разные args.

**Выход:** сборка аргументов контейнера детерминирована и покрыта — args
участвуют в сверке V3 (Task 8) и автоконверге (Task 10).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeArgsBuilder"
```
Зелёные.

**Spec:** §4.5.1, §6.1, §7 фаза 6.

- [ ] **Шаг 1.** Тест канонического набора (failing).
- [ ] **Шаг 2.** Реализация → зелёный.
- [ ] **Шаг 3. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): NodeArgsBuilder — флаги valkey-server из декларации/кредов (TDD, t02)"
```

---

### Task 7: Фейки тестов + ClusterSecretEnsurer + PortAllocIndex/PortAllocHealer (TDD)

Пометка трассировки фаз: PortAllocIndex/PortAllocHealer — «каркас» фазы 4
spec §7; реализуются здесь (до первого использования — ProvisioningProcess в
Task 8, фаза 6), чтобы задача 8 была чисто процессной.

**Files:**
- Create: `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` (FakeEtcd), `Fakes/FixedTimeProvider.cs`, `Fakes/FakeDriver.cs`, `Fakes/FakeValkeyConnection.cs`
- Create: `src/ValkeyWorker.Provisioning/Processes/{ClusterSecretEnsurer.cs, PortAllocIndex.cs, PortAllocHealer.cs, ProcessCommon.cs}`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/{ClusterSecretEnsurerTests.cs, PortAllocIndexTests.cs, PortAllocHealerTests.cs}`

**Interfaces (Produces, для задач 8–12):**
```csharp
// Фейки: FakeEtcd/FixedTimeProvider — порты KafkaWorker.UnitTests/Provisioning 1:1
// (FakeEtcd — txn с compare, Range/Put/Delete по префиксу; FixedTimeProvider — ручное время).
// FakeDriver : ValkeyWorker.Docker.Drivers.IClusterDriver — in-memory контейнеры
//   {имя → (host, hostPort, cpu, mem, args, image)}; поддержка подмены args/image/лимитов
//   живого контейнера (кейсы сверки V3: «существующий с иными args → пересоздание»),
//   настраиваемые занятые порты, флаг «движок недоступен» (слепой inspect → Result.Failed).
// FakeValkeyConnection : ValkeyWorker.Core.Valkey.IValkeyConnection — in-memory модель
//   ноды: пользователи{user → множество паролей}, конфиг{param → value}; AUTH проверяет
//   пару; PING требует валидную пару; CONFIG/ACL по образцу; переключатель «молчит».

public sealed record ValkeyCredentials(string AdminPassword, string AppPassword);

public interface IClusterSecretEnsurer
{
    // ensure: txn put-if-absent admin_user="admin"/admin_password/app_user="app"/app_password
    // (генерация ValkeyPasswordGenerator); проигрыш txn → re-read существующих.
    Task<Result<ValkeyCredentials>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class PortAllocIndex(IEtcdGateway gateway, string[] endpoints,
    Microsoft.Extensions.Logging.ILogger<PortAllocIndex> logger)
{
    // Занятость portalloc ВСЕХ кластеров /valkeyworker/portalloc/<C> (union);
    // битый JSON → игнор с логом.
    public Task<Result<IReadOnlySet<int>>> ForeignAllocatedPortsAsync(string cluster, CancellationToken ct);
}

public sealed class PortAllocHealer(IEtcdGateway gateway, string[] endpoints, IClusterDriver driver,
    ClaimStore claims, WorkJournal journal, PortAllocLock portLock, PortAllocIndex index,
    ValkeyProvisioningOptions options)
{
    // E9-лестница: нода без записи portalloc → реконструкция из inspect живого
    // контейнера (published-порт + host) put-if-absent под locks/portalloc;
    // проигрыш → re-read. Контейнера нет / inspect недоступен → Failed (вслепую нет).
    public Task<Result<int>> HealNodePortAsync(string cluster, string node, CancellationToken ct);
}

// Общие опции процессов (порт ProvisioningOptions kfw по таблице замен):
public sealed record ValkeyProvisioningOptions(
    int PortRangeFrom, int PortRangeTo, int NodeBootSec, int NodeDeadSec,
    string? AdvertisedClientHost, string NodeImage);
```
`ProcessCommon` — порт kfw (хелперы чтения/записи состояний нод, claimed-check)
+ парсер строк ресурсов канона pg §9.3 в лимиты: `static (decimal? Cpu, long? MemBytes)?
ParseResources(ValkeyResources? r)` — cpu: decimal-строка инвариантной культуры
(`"2"`, `"0.5"`; БЕЗ суффиксов), mem: `"<n>Gi"` → `n × 2^30`; disk не парсится
(инфо); незнакомый формат → null (процесс не ставит лимит — валидация API
гарантирует канон на входе, толерантность чтения сохраняется).

**Вход:** Tasks 4–5 закоммичены (драйвер, IValkeyConnection).

**Действие:** TDD.
1. Перенести фейки (FakeEtcd/FixedTimeProvider — копии; FakeDriver/FakeValkey —
   новые по интерфейсам Tasks 4–5).
2. `ClusterSecretEnsurerTests`: первое ensure → обе пары сгенерированы
   (32 симв `[A-Za-z0-9]`) через txn put-if-absent; конкурентный сценарий
   (FakeEtcd уже содержит чужие пароли) → re-read, чужие возвращены; частичные
   креды (есть app, нет admin) → дозаполняются только отсутствующие.
3. `PortAllocIndexTests`: два portalloc-кластера → объединение портов; битый
   JSON → игнор.
4. `PortAllocHealerTests`: нода без portalloc, живой контейнер (FakeDriver) →
   реконструкция put-if-absent (порт из inspect); put-if-absent проигран
   (FakeEtcd-конкурент) → re-read чужого значения; inspect недоступен →
   Failed; контейнера нет → Failed (E9 не выдумывает порт).
5. Реализация по образцам kfw (без сертификатных/CA/AdminClient-частей).

**Выход:** строительные блоки процессов готовы и покрыты фейками (каркас
PortAlloc* — до первого использования в Task 8).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterSecretEnsurer|FullyQualifiedName~PortAlloc"
```
Зелёные.

**Spec:** §4.5 V2/C (E9), §6.1 (фейки), §7 фаза 4 (каркас PortAlloc*; используются фазой 6 — Task 8).

- [ ] **Шаг 1.** Фейки (FakeEtcd/FixedTimeProvider — копии; FakeDriver с подменой args/FakeValkey — новые).
- [ ] **Шаг 2.** ClusterSecretEnsurer TDD.
- [ ] **Шаг 3.** PortAllocIndex TDD.
- [ ] **Шаг 4.** PortAllocHealer TDD.
- [ ] **Шаг 5. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): фейки + ClusterSecretEnsurer + PortAllocIndex/Healer (TDD, t02)"
```

---

### Task 8: ProvisioningProcess A (V0–V5, TDD)

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`

**Interfaces (Produces, для задач 10/12):**
```csharp
public sealed class ProvisioningProcess(
    IEtcdGateway gateway, string[] endpoints, IClusterDriver driver,
    ClaimStore claims, WorkJournal journal, PortAllocLock portLock, PortAllocIndex portIndex,
    IClusterSecretEnsurer secrets, IValkeyConnection valkey, ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    // Один тик машины V0–V5; идемпотентен; только под живым клэймом <C>.
    public Task<Result> TickAsync(ValkeyClusterSnapshot snapshot, CancellationToken ct);
}
```

**Вход:** Task 7 закоммичен.

**Действие:** фазовая машина V0–V5 (arch/21 §5 A дословно):
- V0 claim + journal(op=provision) + снапшот «до» (snapshot-делегат);
- V1 план: PlacementPlanner (группы node1..N, hosts из `driver.GetHostsAsync`)
  + PortAllocator под глобальным `PortAllocLock`; занятость =
  `driver.GetBusyPortsAsync` ∪ `portIndex.ForeignAllocatedPortsAsync`; не взял
  lock → journal `waiting-portalloc-lock`, Result успех (InProgress — следующий
  тик); запись `/valkeyworker/portalloc/<C>` `{"node1":{"host":"<h>","client":<p>}}`;
  journal phase=planned;
- V2 `secrets.EnsureAsync`;
- V3 контейнер: `NodeArgsBuilder.Build` → `driver.EnsureNodeAsync(new
  ValkeyNodeSpec(…))` с лимитами из `nodes/node1/resources` (cpu/mem через
  ProcessCommon-парсер; disk — инфо); put `nodes/node1/state=PROVISIONING`;
  существующий контейнер (re-run) → **сверка image + args + порт + лимитов**
  (spec §4.5 A V3 «сверка (args/лимиты/порт)»; args — `driver.NodeArgsAsync`,
  лимиты — `NodeResourcesAsync`, порт — `InspectNodeEndpointAsync` vs
  portalloc-запись; пароли/maxmemory/persistence живут в args — детерминизм
  §4.5.1): полное совпадение → пропуск; любое расхождение → пересоздание
  (remove + ensure) с каноническими параметрами;
- V4 готовность: цикл `valkey.PingAsync` с admin-кредом до PONG в бюджет
  NodeBootSec (транзиент-толерантно: ошибка соединения не прерывает, только
  бюджет) → put `nodes/node1/state=RUNNING`;
- V5 put `endpoints` = advertised-хост:клиентский порт
  (`AdvertisedClientHost ?? host размещения`); config: txn compare
  `mod_revision` (config начала тика) → put `ValkeyWriting.ConfigJson(…)` БЕЗ
  `state`; снапшот «после»; journal done;
- Гонка TO_REMOVE: перечитывание config перед V3 и V5 — `state=TO_REMOVE` →
  прекращение (journal `aborted-state-changed`);
- Journal-фазы: `provision → planned → ensured-secrets → container → running →
  done` (journal-before-manipulations).

Тесты (фейки + FixedTimeProvider):
1. Полный прогон V0→V5: клэйм взят; порт из окна (не занят); контейнер создан
   (FakeDriver: имя `vwk-<C>-node1`, образ `valkey/valkey:9.1.2`, лимиты из
   resources, args канонические); PING → PONG (FakeValkey); `endpoints`
   записаны; config перезаписан без `state`; state=RUNNING; journal `done`;
   снапшот-делегат вызван дважды («до»/«после»).
2. Идемпотентность re-run: второй тик на готовом кластере — существующий
   контейнер совпадает по image/args/порту/лимитам → без изменений.
3. **Сверка V3 — иные args**: существующий контейнер с подменёнными args
   (FakeDriver; напр. другой `--maxmemory`) → пересоздан с каноническими args
   (FakeDriver фиксирует вторую сборку).
4. PortAllocLock занят (FakeEtcd держит чужой lease) → journal
   `waiting-portalloc-lock`, контейнера нет.
5. Гонка TO_REMOVE между V1 и V3 → процесс прекращён, контейнера нет.
6. V4 бюджет исчерпан (FakeValkey молчит, FixedTimeProvider) → Result.Failed,
   journal `boot-timeout`, state остаётся PROVISIONING.
7. Чужой клэйм (claims/<C> занят другим instance) → никаких записей.

**Выход:** процесс A реализует arch/21 §5 A (сверка V3 — image+args+порт+лимиты).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ProvisioningProcessTests"
```
Зелёные (7 сценариев).

**Spec:** §4.5 A (V3-сверка), §4.5.1 (детерминизм args), §6.1, §7 фаза 6, §9.1.

- [ ] **Шаг 1.** Тест полного прогона (failing).
- [ ] **Шаг 2.** Реализация V0–V5 (вкл. сверку V3) → зелёный.
- [ ] **Шаг 3.** Тесты идемпотентности/args-расхождения/лок-ожидания/гонки/бюджета/чужого-клэйма → зелёные.
- [ ] **Шаг 4. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): ProvisioningProcess V0–V5 со сверкой image/args/порт/лимиты (TDD, t02)"
```

---

### Task 9: DeprovisioningProcess B (X0–X3, TDD)

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`

**Interfaces (Produces, для задачи 12):**
```csharp
public sealed class DeprovisioningProcess(
    IEtcdGateway gateway, string[] endpoints, IClusterDriver driver,
    ClaimStore claims, WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    public Task<Result> TickAsync(ValkeyClusterSnapshot snapshot, CancellationToken ct);
}
```

**Вход:** Task 8 закоммичен.

**Действие:** X0–X3 (arch/21 §5 B):
- X0 claim + journal(op=deprovision) + снапшот «до»;
- X1 docker: `driver.ListNodeObjectsAsync(<C>)` → `RemoveNodeAsync` каждый
  (404 = ок); ПОРЯДОК «сначала docker, потом etcd»; томов нет;
- X2 etcd: `del --prefix /valkey/clusters/<C>/` + del `/valkeyworker/claims/<C>`
  + `/valkeyworker/work/<C>` + `/valkeyworker/portalloc/<C>` +
  `/valkeyworker/rotations/<C>` (очистка координации ВКЛЮЧАЯ заявки ротаций);
- X3 снапшот «после»; клэйм снят явно (del + revoke lease —
  `claims.ReleaseClusterAsync`).

Тесты:
1. Полный прогон: контейнеры `vwk-<C>-*` удалены ДО чистки etcd (порядок —
   журналом вызовов FakeDriver); все префиксы etcd пусты; клэйм снят;
   journal `deprovision/done`.
2. 404 на docker (контейнеров нет) → успех, etcd-чистка выполнена.
3. Ошибка docker-хоста в X1 → Result.Failed, etcd-префикс ЦЕЛ (порядок!).

**Выход:** процесс B готов; демонтаж упорядочен.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~DeprovisioningProcessTests"
```
Зелёные.

**Spec:** §4.5 B, §7 фаза 6, §9.6.

- [ ] **Шаг 1.** Тесты (failing) → реализация → зелёные (3 сценария).
- [ ] **Шаг 2. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): DeprovisioningProcess X0–X3, docker-до-etcd (TDD, t02)"
```

---

### Task 10: NodeSupervisor C — надзор + E9 + автоконверге лимитов + S7 (TDD)

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/NodeSupervisor.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`

**Interfaces (Produces, для задачи 12):**
```csharp
public sealed class NodeSupervisor(
    IEtcdGateway gateway, string[] endpoints, IClusterDriver driver,
    ClaimStore claims, WorkJournal journal, IValkeyConnection valkey,
    ValkeyProvisioningOptions options, PortAllocHealer healer)
{
    public Task<Result> TickAsync(ValkeyClusterSnapshot snapshot, CancellationToken ct);
}
```

**Вход:** Tasks 7–8 закоммичены (healer, args, секреты через ensure).

**Действие:** надзор (arch/21 §5 C + правка Task 1):
1. Снесённый контейнер (docker-факт: объекта нет — положительное
   свидетельство) → пересоздание через `EnsureNodeAsync` (NodeArgsBuilder:
   актуальные креды etcd + декларация) + `state=PROVISIONING`; в RUNNING —
   следующий тик по PING.
2. Автоконверге лимитов: живой контейнер → `NodeResourcesAsync` (inspect) vs
   `nodes/node1/resources` (cpu/mem; disk не сверяется) — расхождение →
   пересоздание с лимитами декларации, `state=PROVISIONING`. Слепой inspect
   (`Result.Failed`) → ошибка тика, пересозданий нет.
3. PING-проба admin-кредом; трек `first_seen` через
   `journal.WriteSupervisionAsync` (счётчик стартует по УСПЕШНОМУ
   зондированию); молчание дольше NodeDeadSec → `state=UNREACHABLE` +
   пересоздание (тома нет — данных не жалко). Слепая проба (ошибка
   соединения) → НИКАКИХ действий, трек заморожен (S7).
4. Одно пересоздание за тик (по любой причине).
5. E9: нода без записи portalloc → `healer.HealNodePortAsync` ДО деструктива.
6. Кеш-ключи домена надзор НЕ чистит никогда.
7. Ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` — не трогаем (чужие процессы).
8. `endpoints` сходится к portalloc-канону: расхождение → RMW.

Тесты:
1. Снос контейнера → пересоздан (те же креды/порт — FakeDriver фиксирует args
   второй сборки), state=PROVISIONING; следующий тик PING → RUNNING.
2. Автоконверге: ресурсы в декларации cpu 2 при контейнере cpu 1 →
   пересоздание с cpu=2, ОДНО за тик; совпадающие лимиты → пусто.
3. Слепой inspect (FakeDriver fail) → Failed, контейнер жив, state не меняется.
4. UNREACHABLE: PING не отвечает NodeDeadSec+1 (FixedTimeProvider) при живом
   контейнере → state=UNREACHABLE + пересоздание; затем PING → RUNNING.
5. S7: PING-ошибка соединения при треке first_seen → трек не двигается,
   пересоздания нет, state без изменений.
6. E9: portalloc-ключ удалён → реконструкция из inspect; деструктива не было.
7. endpoints-расхождение (порт ≠ portalloc) → RMW к канону.

**Выход:** надзор C реализует arch/21 §5 C с автоконверге (правка Task 1).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeSupervisorTests"
```
Зелёные (7 сценариев).

**Spec:** §4.5 C (вкл. автоконверге), §6.1, §7 фаза 6, §9.5/§9.8.

- [ ] **Шаг 1.** Тесты сноса/автоконверге (failing) → реализация → зелёные.
- [ ] **Шаг 2.** Тесты S7/UNREACHABLE/E9/endpoints-RMW → зелёные.
- [ ] **Шаг 3. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): NodeSupervisor — надзор/E9/автоконверге лимитов/S7 (TDD, t02)"
```

---

### Task 11: ConfigConverger D + PasswordRotator E (TDD)

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/{ConfigConverger.cs, PasswordRotator.cs}`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/{ConfigConvergerTests.cs, PasswordRotatorTests.cs}`

**Interfaces (Produces, для задачи 12):**
```csharp
public sealed class ConfigConverger(IValkeyConnection valkey, IEtcdGateway gateway, WorkJournal journal)
{
    // Active-ветка: endpoints+admin-кред из снапшота.
    public Task<Result> TickAsync(ValkeyClusterSnapshot snapshot, CancellationToken ct);
}

public sealed class PasswordRotator(
    IEtcdGateway gateway, string[] endpoints, ClaimStore claims, WorkJournal journal,
    IValkeyConnection valkey, ValkeyPasswordGenerator generator)
{
    // Заявка /valkeyworker/rotations/<C> {"role":"app"|"admin",…} (E1–E3).
    public Task<Result> TickAsync(ValkeyClusterSnapshot snapshot, CancellationToken ct);
}
```

**Вход:** Tasks 5, 8 закоммичены.

**Действие:**
1. `ConfigConverger`: `ConfigGetAsync("maxmemory")`/`("maxmemory-policy")` vs
   `config.{maxmemory_bytes,maxmemory_policy}` (маппинг `maxmemory_bytes`→
   `maxmemory`, `maxmemory_policy`→`maxmemory-policy`) → `ConfigSetAsync` при
   отличии (без рестартов); ACL-план: `AclListAsync` vs канон (admin:
   `~* +@all`; app: `~* +@read +@write`; `default off`) → идемпотентный
   `AclSetUserAsync`-converge (пароли в ACL LIST не видны — сверка прав,
   пароли — только окно E); `maxmemory_bytes ≥ mem`-лимита (из
   `nodes/node1/resources`, ProcessCommon-парсер) → journal-warning
   (`warning-maxmemory-mem`, R3 — ответственность оператора); ошибка
   соединения → Failed (тик повторится).
2. `PasswordRotator` E1–E3 (arch/21 §5 E): заявка из etcd (`role` app|admin);
   NEW = `generator.Generate()`; фазы по journal (отказ → повтор тика доигрывает):
   E1 `ACL SETUSER <role> >NEW`; E2 ОДНА txn [compare
   value(`/valkey/clusters/<C>/<role>_password`)==OLD][put NEW; del заявки];
   E3 `ACL SETUSER <role> <OLD`. Ротация admin не трогает app и наоборот;
   пересоздание контейнера в окне безопасно (аргументы из etcd; после E2 — NEW);
   битая заявка (role не app|admin) → мусор: del с journal. Без снапшотов P12
   (точки изменений — только provisioning/deprovisioning).

Тесты:
- Converger: расхождение maxmemory → CONFIG SET вызван; совпадение → пусто;
  ACL-падение прав app (FakeValkey: app без +@read) → SETUSER-восстановление;
  `maxmemory_bytes ≥ mem` → journal-warning; слепая нода → Failed.
- Rotator: полный E1→E3 — после E1 ОБА пароля валидны (FakeValkey), после E2
  etcd содержит NEW и заявка удалена, после E3 OLD отвергнут/NEW работает;
  отказ между E1/E2 (journal на фазе e1, тик перезапущен) → доигрывает с E2;
  ротация app не трогает admin-пароль; битая заявка → del + journal.

**Выход:** Active-ветка D и ротация E готовы.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ConfigConverger|FullyQualifiedName~PasswordRotator"
```
Зелёные.

**Spec:** §4.5 D/E, §7 фаза 6, §9.3/§9.4.

- [ ] **Шаг 1.** ConfigConverger TDD.
- [ ] **Шаг 2.** PasswordRotator TDD (окно двух паролей).
- [ ] **Шаг 3. Коммит**:
```bash
git add src/ValkeyWorker.Provisioning src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): ConfigConverger D + PasswordRotator E — окно двух паролей (TDD, t02)"
```

---

### Task 12: Классификатор + ValkeyClusterProcesses + финальная DI-композиция Program.cs

**Files:**
- Modify: `src/ValkeyWorker.App/Loops/{ValkeyClusterClassifier.cs, ValkeyClusterProcesses.cs, ReconcileLoop.cs}` (наполнение), `src/ValkeyWorker.App/Program.cs` (DI процессов)
- Test: `src/tests/ValkeyWorker.UnitTests/App/ValkeyClusterClassifierTests.cs`

**Interfaces (Produces):**
```csharp
public enum ValkeyClusterKind { Provision, Deprovision, Active, Skip }

public static class ValkeyClusterClassifier
{
    // NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; отсутствие state → Active;
    // Config == null (битый) → Skip; незнакомое state → Active (raw, arch/20 §5).
    public static ValkeyClusterKind Classify(ValkeyClusterSnapshot snapshot);
}

public sealed class ValkeyClusterProcesses(/* процессы A–E + gateway/endpoints/parser/parallelism */)
    : IValkeyClusterProcesses
{
    // Range /valkey/clusters/ → ValkeySnapshotParser → классификация →
    // Provision/Deprovision/Active(C→D→E); Skip — лог; parallelism MaxClusters=4;
    // ошибки кластера не роняют тик.
    public Task TickAsync(CancellationToken ct);
}
```

**Вход:** Tasks 8–11 закоммичены.

**Действие:**
1. `ValkeyClusterClassifierTests` (юниты, AAA): 4 ветки (NOT_INITIALIZED /
   TO_REMOVE / отсутствие state / битый Config=null; + raw-state → Active).
2. Наполнить `ValkeyClusterProcesses` (порт `KafkaClusterProcesses` без
   kafka-специфики: нет reassign/topic-sync/security-migrator/backoff —
   валю-туннель C→D→E) и `ReconcileLoop` (порт kfw: тик ScanIntervalSec,
   ErrorDelayMs после ошибки).
3. `Program.cs`: зарегистрировать `IValkeyConnection → ValkeyConnection`,
   `IClusterSecretEnsurer`, `PortAllocIndex`, `PortAllocHealer`, процессы
   A–E, `IValkeyClusterProcesses → ValkeyClusterProcesses`; хелпер
   `ToProvisioningOptions(opts)` → `new ValkeyProvisioningOptions(
   opts.Docker.PortRange.From, opts.Docker.PortRange.To,
   opts.Thresholds.NodeBootSec, opts.Thresholds.NodeDeadSec,
   opts.AdvertisedClientHost, opts.Docker.Images.Node)`; SnapshotDelegate
   для A/B.
4. Смоук-проверки юнитов: `HealthTests`-паттерн — Degraded при недоступном
   etcd, циклы живы.

**Выход:** воркер функционально полон (цикл водит процессы A–E).

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug
```
Обе зелёные (все юниты).

**Spec:** §4.5/§4.6, §7 фаза 6 (классификатор, ReconcileLoop).

- [ ] **Шаг 1.** Классификатор TDD.
- [ ] **Шаг 2.** ValkeyClusterProcesses + ReconcileLoop; все юниты зелёные.
- [ ] **Шаг 3.** Program.cs DI-финал; build зелёный.
- [ ] **Шаг 4. Коммит**:
```bash
git add src/ValkeyWorker.App src/tests/ValkeyWorker.UnitTests
git commit -m "feat(valkey): классификатор + ValkeyClusterProcesses + DI процессов A–E (t02)"
```

---

### Task 13: HTTP API — полный набор мутаций §4.7 + seed + restart (юниты валидаций + WAF-тесты + метрики-смоук)

**Files:**
- Create: `src/ValkeyWorker.App/Api/Operations/{ValkeyLimits.cs, ValkeyExceptions.cs, ValkeyApiHelpers.cs, CreateClusterHandler.cs, DeleteClusterHandler.cs, UpdateConfigHandler.cs, UpdateResourcesHandler.cs, RotatePasswordHandler.cs, SeedDemoHandler.cs, RestartHandler.cs}`
- Modify: `src/ValkeyWorker.App/Api/ApiModule.cs` (маппинг 7 эндпоинтов), `src/ValkeyWorker.App/Program.cs` (DI хендлеров)
- Test: `src/tests/ValkeyWorker.UnitTests/Api/ValkeyValidationTests.cs`
- Test: `src/tests/ValkeyWorker.IntegrationTests/Api/{ValkeyApiFactory.cs, MetricsApiFactory.cs, ClusterMutationsApiTests.cs, RotateApiTests.cs, SeedApiTests.cs, RestartApiTests.cs, MtlsApiTests.cs, WorkerApiCertStartupTests.cs, MetricsApiTests.cs}`

**Interfaces (Produces):**
```csharp
// DTO (JSON camelCase; ресурсы — по образцу kfw AddBrokerHandler: decimal ядра + int GiB):
public sealed record CreateValkeyClusterRequest(string? Name, int? Nodes, long? MaxmemoryBytes,
    string? MaxmemoryPolicy, ValkeyResourcesUpdateRequest? Resources);
public sealed record ValkeyResourcesUpdateRequest(decimal? Cpu = null, int? MemGi = null, int? DiskGi = null);
public sealed record ValkeyConfigUpdateRequest(long? MaxmemoryBytes = null, string? MaxmemoryPolicy = null);
public sealed record RotateValkeyPasswordRequest(string? Role);

// Исключения (порт KafkaExceptions по таблице замен):
// ValkeyValidationException(errors), ValkeyClusterNotFoundException,
// ValkeyClusterAlreadyExistsException, ValkeyClusterNotActiveException,
// ValkeyRotationAlreadyRequestedException, EtcdWriteUnavailableException,
// InvalidValkeyConfigException, WorkerApiNotFoundException(seed выключен).
```
Эндпоинты (ApiModule — порт kfw-модуля; guards читают etcd напрямую, без
снапшота панели):
- `POST /api/valkey/clusters` → 201 (txn put-if-absent `config` c
  `state=NOT_INITIALIZED`, `nodes=1`, `created_unix`, `maxmemory_*`;
  `nodes/node1/state=NOT_INITIALIZED`; `nodes/node1/resources` = 
  `ValkeyWriting.ResourcesJson(cpu, memGi, diskGi)`); 409 уже есть; 400 валидация;
- `DELETE /api/valkey/clusters/{cluster}` → 202 (txn compare отсутствия
  `state` → put `state=TO_REMOVE`; повтор идемпотентен); 404 нет;
- `PUT /api/valkey/clusters/{cluster}/config` → 200 (мутация
  `maxmemory_bytes`/`maxmemory_policy`; converge D применит); 404; 400;
- `PUT /api/valkey/clusters/{cluster}/nodes/{node}/resources` → 200
  (v1 node1); 404; 400;
- `POST /api/valkey/clusters/{cluster}/password/rotate` → 202 (заявка
  `/valkeyworker/rotations/<C>` txn `version==0`: `role`, `requested_unix`,
  `requested_by` из `X-Requested-By` fallback `"api"`); 404; 409 заявка стоит;
- `POST /api/seed/demo` → 200/201 (идемпотентен: живой `config` кластера
  `demo` → `{"seeded":false}`; флаг `EnableSeedEndpoint`, выключен → 404);
- `POST /api/restart` → 202 graceful self-stop (порт RestartHandler kfw).

Валидации (`ValkeyLimits` + `ValkeyApiHelpers`; форматы — канон pg §9.3,
`arch/adminpanel/02-etcd-contract.md` ~514–515, по образцу kfw
`AddBrokerHandler.cs:41-50`): имя `^[a-z][a-z0-9_]{0,62}$`; `nodes==1` (иное
400 — реплики roadmap); `maxmemory_policy` ∈ 8 значений канона (дефолт
`allkeys-lru`); `maxmemory_bytes` > 0; **`Cpu` — десятичные ядра 0.01–64**
(DTO decimal; в etcd канонической строкой инвариантной культуры: `"0.5"`,
`"2"` — БЕЗ суффикса `m`); **`MemGi`/`DiskGi` — целые GiB 1–65536; в etcd
ТОЛЬКО `"<n>Gi"`** (парсер панели t03 по образцу `KafkaParser.ParseGi`
принимает исключительно суффикс `Gi` — иные суффиксы стали бы parseError
читателя, не допускать); **инвариант `maxmemory_bytes < MemGi × 2^30`**
(400 при create; при раздельных мутациях — сверка с текущими полями etcd);
seed-набор `demo`: nodes=1, `maxmemory_bytes=536870912`, `allkeys-lru`,
cpu=1/mem=1Gi/disk=10Gi (512MiB < 1Gi — инвариант соблюдён).

**Вход:** Task 12 закоммичен.

**Действие:**
1. `ValkeyLimits`/`ValkeyApiHelpers` + юнит-тесты валидаций
   (`ValkeyValidationTests`: имя/nodes/policy/maxmemory/границы Cpu-MemGi-DiskGi/
   инвариант maxmemory < MemGi×2^30 — на чистых функциях, AAA).
2. Хендлеры (порт kfw-хендлеров с валю-логикой) + ProblemDetails-маппинг в
   ApiModule (порт kfw: 400 с `errors`-словарём, 404, 409, 503) + DI.
3. WAF-тесты (`ValkeyApiFactory` — порт `KafkaApiFactory`: in-memory host,
   `AllowInsecureHttp=true` ТОЛЬКО тесты, Testcontainers-etcd на динамическом
   порту; последовательная коллекция): create 201/409/400 (все правила,
   вкл. инвариант и 8 значений policy); delete 202/404/идемпотентность;
   config 200/404/400; resources 200/404/400; rotate 202/409/404; seed
   201/200-seeded:false/404-флаг-off; restart 202 (хост жив после ответа);
   **секретность ответов** (spec §9.2): тела ответов create/seed/rotate/config
   не содержат паролей — сериализованный JSON не имеет ключей `*password*`
   и не содержит 32-символьных значений `[A-Za-z0-9]{32}` (ассерт по строке
   ответа); mtls: без клиентского серта при включённом TLS — отказ на
   хендшейке (порт MtlsApiTests); серверный серт из
   `/workers/api_tls/valkeyworker` при старте с env-фоллбеком `VWK_API_TLS_*`
   (порт WorkerApiCertStartupTests).
4. Метрики-смоук (`MetricsApiFactory` + `MetricsApiTests` — порт kfw):
   `/metrics` экспонирует имена Meter `ValkeyWorker` (spec §9.7).
5. После WAF-серий — dispose etcd-контейнеров фикстурами + зачистка серии.

**Выход:** полный набор мутаций §4.7 на mTLS-грани; валидации покрыты юнитами
и WAF-тестами; `/metrics` несёт `ValkeyWorker`.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyValidation"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Api|FullyQualifiedName~Metrics"
docker ps -a --format '{{.Names}}' | grep -E 'vwk-|etcd' || true
docker network prune -f
```
Все зелёные; остаточных контейнеров нет.

**Spec:** §4.7, §6.1 (валидации API), §6.2 (Api-группа), §7 фаза 7, §9.2 (креды не отдаются), §9.7 (Meter), §11.3.

- [ ] **Шаг 1.** `ValkeyLimits`/`ValkeyApiHelpers` + юнит-тесты валидаций → зелёные.
- [ ] **Шаг 2.** Хендлеры create/delete + ApiModule + DI; WAF create/delete (вкл. ассерт секретности тел) → зелёные.
- [ ] **Шаг 3.** Хендлеры config/resources/rotate/seed/restart; WAF → зелёные (зачистка серии).
- [ ] **Шаг 4.** WAF mtls/cert-startup + метрики-смоук (`MetricsApiTests`) → зелёные (зачистка серии).
- [ ] **Шаг 5. Коммит**:
```bash
git add src/ValkeyWorker.App src/tests/ValkeyWorker.UnitTests src/tests/ValkeyWorker.IntegrationTests
git commit -m "feat(valkey): HTTP API полного набора мутаций + seed/restart, валидации pg §9.3 + WAF + метрики (t02)"
```

---

### Task 14: Поставка — Dockerfile, deploy-compose (8082), images.txt + зеркалирование

**Files:**
- Create: `docker/ValkeyWorker.Dockerfile`
- Modify: `deploy/docker-compose.yml` (+ сервис `valkeyworker`, + volumes `vw-snapshots`/`vw-api-tls`)
- Modify: `dev-stand/images/images.txt` (+ `valkey/valkey:9.1.2` по алфавиту)

**Interfaces:** — (инфраструктура).

**Вход:** Task 13 закоммичен (App несёт полную функциональность).

**Действие:**
1. `docker/ValkeyWorker.Dockerfile` — копия `docker/KafkaWorker.Dockerfile`
   с заменой `KafkaWorker.App`→`ValkeyWorker.App` (multi-stage `sdk:10.0`
   publish → `aspnet:10.0`, curl для HEALTHCHECK `/healthz` по mTLS с `/tls`,
   `ENV ASPNETCORE_HTTP_PORTS=8080`, `EXPOSE 8080`, ENTRYPOINT
   `["dotnet","ValkeyWorker.App.dll"]`).
2. `deploy/docker-compose.yml` — сервис `valkeyworker` по образцу `kafkaworker`
   (вставка после сервиса `kafkaworker`; в корневые `volumes:` добавить
   `vw-snapshots:` и `vw-api-tls:`):
```yaml
  valkeyworker:
    build:
      context: ..
      dockerfile: docker/ValkeyWorker.Dockerfile
    image: valkeyworker:dev          # локальный, в registry НЕ класть
    restart: unless-stopped
    ports:
      - "${VWK_API_HOST_PORT:-8082}:8080"   # ряд pg 8080 / kfw 8081 / vwk 8082
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - vw-snapshots:/snapshots
      - vw-api-tls:/tls:ro
    extra_hosts:
      - "local:host-gateway"
      - "host.docker.internal:host-gateway"
    environment:
      ValkeyWorker__Etcd__Endpoints__0: ${VWK_ETCD_ENDPOINT:-http://localhost:2379}
      ValkeyWorker__AdvertisedClientHost: ${VWK_ADVERTISED_CLIENT_HOST:-host.docker.internal}
      ValkeyWorker__Api__AdvertiseUrl: ${VWK_API_ADVERTISE_URL:-https://host.docker.internal:8082}
      VWK_API_TLS_CERT_PATH: /tls/server.crt
      VWK_API_TLS_KEY_PATH: /tls/server.key
      VWK_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
      ValkeyWorker__Api__EnableSeedEndpoint: ${VWK_API_ENABLE_SEED:-false}
```
3. `images.txt`: добавить строку `valkey/valkey:9.1.2` (алфавитный порядок);
   зеркалировать мульти-арх: `dev-stand/images/mirror-image.sh
   valkey/valkey:9.1.2`; проверить `docker pull 192.168.0.1:5000/valkey/valkey:9.1.2`
   и что `dev-stand/images/pull-images.sh` тянет её. `valkeyworker:dev` в
   images.txt НЕ вносить (запрещено). Перед docker-серияями Tasks 15–16 —
   `pull-images.sh`.
4. Локальная проверка сборки образа (из корня worktree):
   `docker build -f docker/ValkeyWorker.Dockerfile -t valkeyworker:dev .`;
   валидность compose: `docker compose -f deploy/docker-compose.yml config -q`.

**Выход:** образ собирается; сервис описан; образ нод зеркалирован в локальный
registry.

**Проверка:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-valkey-worker
docker build -f docker/ValkeyWorker.Dockerfile -t valkeyworker:dev . && \
  docker image inspect valkeyworker:dev --format '{{.Id}}'
docker compose -f deploy/docker-compose.yml config -q && echo COMPOSE_OK
docker pull 192.168.0.1:5000/valkey/valkey:9.1.2
grep -c 'valkey/valkey:9.1.2' dev-stand/images/images.txt   # 1
```
Все успешны.

**Spec:** §5 (поставка и образы), §7 фаза 8, §11.4.

- [ ] **Шаг 1.** Dockerfile + сборка образа успешна.
- [ ] **Шаг 2.** Compose-сервис + volumes; `config -q` валиден.
- [ ] **Шаг 3.** images.txt + `mirror-image.sh` + проверка pull.
- [ ] **Шаг 4. Коммит**:
```bash
git add docker/ValkeyWorker.Dockerfile deploy/docker-compose.yml dev-stand/images/images.txt
git commit -m "feat(valkey): поставка ValkeyWorker — Dockerfile, deploy-сервис :8082, valkey/valkey:9.1.2 в registry (t02)"
```

---

### Task 15: Интеграционные тесты — Valkey-группа (реальные ноды)

**Files:**
- Create: `src/tests/ValkeyWorker.IntegrationTests/Valkey/{ValkeyClusterFixture.cs, FreePortWindow.cs, ProvisioningTests.cs, AclMatrixTests.cs, DeprovisioningTests.cs, ConvergeTests.cs, ResourcesAutorecreateTests.cs, RotationTests.cs, SupervisionTests.cs}`

**Interfaces:** `ValkeyClusterFixture : IAsyncLifetime` — порт
`KafkaClusterFixture`: etcd testcontainers `quay.io/coreos/etcd:v3.5.21`
(`WithPortBinding(2379, assignRandomHostPort: true)` + wait-проба
`/v3/maintenance/status`); `PlainClusterDriver` на `unix:///var/run/docker.sock`
(host `local`); `AdvertisedClientHost="localhost"`; `RunTag =
Guid.NewGuid().ToString("N")[..8]`; `Cluster(name) => $"{name}{RunTag}"` c
регистрацией на демонтаж в `DisposeAsync`; `Options = new(FreePortWindow.Find()…
NodeBootSec: 100, NodeDeadSec: 90, "localhost", "valkey/valkey:9.1.2")`;
`SeedClusterAsync(cluster)` (config NOT_INITIALIZED + `nodes/node1/state` +
`nodes/node1/resources` канона pg §9.3); `SnapshotAsync(cluster)`; риги
`ProvisionRigAsync/SuperviseRigAsync/ConvergeRigAsync/RotateRigAsync` по
образцу kfw-ригов (все координационные типы с `keyPrefix:"/valkeyworker"`);
teardown: демонтаж всех `vwk-<C>-*` прогона + dispose etcd + ассерт чистоты
(ни контейнера `vwk-*` тега, ни ключа префикса). `FreePortWindow` — копия kfw
с `StandZoneFrom=15000`, `StandZoneTo=18000` (15xxx pg / 16xxx kfw / 17xxx
valkey-стенды), `SearchFrom=21000`, `Size=64`, `Step=128`.

**Вход:** Tasks 8–14 закоммичены; `dev-stand/images/pull-images.sh` выполнен
(образ нод в локальном docker).

**Действие:** сценарии (все с `RunTag`-именами; NodeBootSec ≤ 100; teardown
при любом исходе):
1. `ProvisioningTests`: сид → тики ProvisioningProcess → контейнер
   `vwk-<C>-node1` (image `valkey/valkey:9.1.2`; Cmd содержит `--save ""` и
   `--appendonly no` — проверка `docker inspect` .Config.Cmd); PING
   admin-кредом → PONG; `endpoints` = `localhost:<фактический порт>`;
   `nodes/node1/state=RUNNING`; config без `state`; креды 32 симв.
2. `AclMatrixTests`: `default` без пароля — AUTH-отказ; app — SET/GET
   работает, `CONFIG GET`/`ACL LIST` — отказ (без админ-команд); admin —
   `+@all` (PING, CONFIG SET).
3. `DeprovisioningTests`: TO_REMOVE-сид → DeprovisioningProcess → контейнера
   нет; `/valkey/clusters/<C>/` пуст; `/valkeyworker/{claims,work,portalloc,
   rotations}/<C>*` пусты.
4. `ConvergeTests`: мутация config `maxmemory_bytes`/policy в etcd →
   ConfigConverger → `CONFIG GET maxmemory` = новое значение; контейнер НЕ
   пересоздан (Id до/после равен).
5. `ResourcesAutorecreateTests`: PUT resources (cpu=2, mem=2Gi) → тик
   надзора → контейнер пересоздан с новыми лимитами (inspect NanoCpus/Memory),
   одно пересоздание за тик, затем RUNNING по PING.
6. `RotationTests`: заявка role=app → тики ротатора → в окне после E1 оба
   пароля валидны (побы обоим), после E3 OLD отвергнут/NEW работает, заявка
   удалена; admin-кред не тронут (PING прежним admin-паролем).
7. `SupervisionTests`: `docker rm -f vwk-<C>-node1` → тик надзора → пересоздан
   с теми же кредами (AUTH прежним admin-паролем) и портом (portalloc),
   RUNNING; `docker stop` → UNREACHABLE-путь с коротким NodeDeadSec рига
   (напр. 5 с) → пересоздание.

**Выход:** метрики успеха §9.1–§9.8 (кроме E2E-специфики) покрыты на реальных
контейнерах.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Valkey"
# ЗАЧИСТКА СЕРИИ (обязательна после финальной строки прогона):
docker ps -a --format '{{.Names}}' | grep "vwk-" | xargs -r docker rm -f
docker network prune -f
docker ps -a --format '{{.Names}}' | grep -c "vwk-" || true   # 0
```
Тесты зелёные; остатков нет.

**Spec:** §6.2 (Valkey-группа), §7 фаза 9, §11.5.

- [ ] **Шаг 1.** Фикстура + FreePortWindow (зона до 18000) + provisioning-смоук → зелёный (+ зачистка).
- [ ] **Шаг 2.** ACL-матрица + deprovisioning → зелёные (+ зачистка).
- [ ] **Шаг 3.** Converge + автоконверге лимитов → зелёные (+ зачистка).
- [ ] **Шаг 4.** Ротация + надзор → зелёные (+ зачистка).
- [ ] **Шаг 5. Коммит**:
```bash
git add src/tests/ValkeyWorker.IntegrationTests
git commit -m "test(valkey): интеграция — provisioning/ACL/deprovisioning/converge/автоконверге/ротация/надзор (t02)"
```

---

### Task 16: Docker-E2E — Release-фикстура, сценарий API → контейнер → дискавери → RESP → чистота

**Files:**
- Create: `src/tests/ValkeyWorker.IntegrationTests/E2e/{ValkeyE2eEnvironment.cs, ValkeyE2eLifecycleTests.cs}`

Примечание по тегу образа (сознательное отклонение от буквы spec §6.3
`valkeyworker:dev`): E2E собирает образ в тег **`valkeyworker:e2e`** из того
же `docker/ValkeyWorker.Dockerfile` — изоляция свежего Release-артефакта
прогона от локального `valkeyworker:dev` дев-стенда (параллельный прогон не
затирает и не зависит от dev-образа; `PGW_TEST_E2E_NOBUILD=1` бисектит именно
e2e-тег). Локальный `valkeyworker:dev` в registry не попадает (как и e2e).

**Interfaces:** `ValkeyE2eEnvironment : IAsyncLifetime` — порт
`src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (guid-контур,
teardown-порядок, ассерт чистоты, retry старта окружения, телеметрия):
- `runId = Guid.NewGuid().ToString("N")`; сеть `vwk-en-{runId}` (создаёт
  фикстура); etcd-контейнер `vwk-ee-{runId}` (динамический порт,
  wait-стратегия `/v3/maintenance/status`); воркер — контейнер
  `vwk-ew-{runId}` образа `valkeyworker:e2e` (собирается фикстурой:
  `docker build -f docker/ValkeyWorker.Dockerfile -t valkeyworker:e2e .`
  из корня worktree; Dockerfile публикует Release); API-порт
  `assignRandomHostPort: true` (в контейнер 8080).
- Воркер-контейнер: монтирование docker.sock; env:
  `ValkeyWorker__Etcd__Endpoints__0=http://host.docker.internal:<фактический
  порт etcd>`, `AdvertisedClientHost=host.docker.internal`,
  `AdvertiseUrl=https://host.docker.internal:<apiPort>`,
  `ValkeyWorker__Docker__PortRange__From/To` — окно FreePortWindow (21000+),
  TLS-серты: тест генерирует self-signed CA + server + client в
  `/tmp/pgw-e2e-artifacts-<runId>/tls`, монтирует как `/tls:ro`, env
  `VWK_API_TLS_{CERT,KEY,CLIENT_CA}_PATH` (порт MtlsApiTests kfw);
  extra_hosts `host.docker.internal:host-gateway`.
- Телеметрия: артефакты `/tmp/pgw-e2e-artifacts-<runId>/`; docker-логи +
  inspect ВСЕХ контейнеров (вкл. воркер и `vwk-<C>-node1`) снимаются ДО
  удаления; `MarkFailed()` → teardown `docker stop` без удаления +
  `README-cleanup.txt` (own-only по runId); ожидания-фазы > 60 с → строка
  `[PHASE] …` в журнал теста + немедленный сбор логов (slow-phase).
- Гейт запуска: `PGW_TEST_DOCKER=1` (нет — Skip, паттерн PgWorker E2E);
  `PGW_TEST_E2E_NOBUILD=1` — пропустить сборку образа (только бисект).
- Сценарий `ValkeyE2eLifecycleTests.Lifecycle_ProvisionToClean` (кейс-маркер
  мерж-гейта):
  1. `[PHASE] wait-worker` — `/healthz` по mTLS готов (бюджет ≤ 30 с);
  2. `POST /api/valkey/clusters` (name `e2e{runId[..8]}`, nodes=1, 512MiB,
     allkeys-lru, resources cpu=1/mem=1Gi/disk=10Gi) → 201;
  3. `[PHASE] wait-provision` — поллинг etcd `nodes/node1/state=RUNNING` +
     контейнер `vwk-<C>-node1` жив (бюджет ≤ 100 с);
  4. PING admin-кредом по endpoints из etcd (host-порт — фактический из
     `docker inspect`, литералов `:17xxx` в expects нет);
  5. RESP app-кредом: SET/GET тестового ключа; `CONFIG GET` — отказ (ACL);
  6. дискавери-ключи: `endpoints`/креды/config без `state`;
     `/valkeyworker/api/<id>` содержит url и cert_thumbprint;
  7. `DELETE /api/valkey/clusters/{c}` → 202 → wait: контейнера нет, префиксы
     etcd чисты, portalloc снят;
  8. teardown + ассерт чистоты: ни контейнера/сети/ключа guid-префикса.

**Вход:** Tasks 14–15 закоммичены; `pull-images.sh` выполнен; docker жив.

**Действие:** реализация по интерфейсу; Debug-смоук, затем Release-прогон;
после серий — зачистка.

**Выход:** E2E зелёный на свежем Release; сценарий = кейс-маркер мерж-гейта.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"
# зачистка серии:
docker ps -a --format '{{.Names}}' | grep "vwk-" | xargs -r docker rm -f
docker network prune -f
```
Зелёный; `[PHASE]`-строки в журнале; артефакты в `/tmp/pgw-e2e-artifacts-<guid>/`.
При падении — разбор по артефактам; перезапуск только после полного анализа
(docs/e2e-launch.md §4).

**Spec:** §6.3, §7 фаза 10, §11.6.

- [ ] **Шаг 1.** `ValkeyE2eEnvironment` (контур/телеметрия/MarkFailed/ретрай старта; тег `valkeyworker:e2e`).
- [ ] **Шаг 2.** Сценарий Lifecycle_ProvisionToClean; Debug-прогон → зелёный (+ зачистка).
- [ ] **Шаг 3.** Release-прогон зелёный (+ зачистка; сверить `[PHASE]`-журнал и артефакты).
- [ ] **Шаг 4. Коммит**:
```bash
git add src/tests/ValkeyWorker.IntegrationTests
git commit -m "test(valkey): docker-E2E Release — lifecycle provision→discovery→clean (t02)"
```

---

### Task 17: Мерж-гейт — roadmap-правки тем же мерж-коммитом (spec фаза 11)

**Files:**
- Modify: `arch/roadmap/valkey.md` (снять пункт `t02-valkey-worker`; из `t05-valkey-metrics` убрать `← t02-valkey-worker`)
- Modify: `arch/roadmap/pgworker.md` (добавить пункт `t07-unify-docker-engine`)

**Interfaces:** — (roadmap).

**Вход:** Tasks 1–16 закоммичены; мерж в `main` готовится (правки входят в
мерж-коммит, НЕ отдельным — правила arch/roadmap/README.md).

**Действие:**
1. `arch/roadmap/valkey.md`: удалить блок `- **t02-valkey-worker** — сервис
   ValkeyWorker …` целиком; в `t05-valkey-metrics` убрать `
   ← t02-valkey-worker` (остаётся `- **t05-valkey-metrics** — телеметрия …`).
   Никаких пометок «сделано» (история — в git).
2. `arch/roadmap/pgworker.md` — добавить пункт (следующий свободный номер
   t07, проверен по активным тегам):
```markdown
- **`t07-unify-docker-engine`** — унификация docker-движков: три копии
  DockerEngine/ClusterDriver (PgWorker.Docker — SSH-туннели/TLS-docker;
  KafkaWorker.Docker; ValkeyWorker.Docker — копия kfw, t02) разошлись
  (diff kfw/pg ~492 строк). Вынос в общую Shared-сборку с сохранением
  Pg-специфики (SSH/TLS) как опций. Мерж-гейт: полный docker-E2E
  Pg+Kfw+Valkey (все три домена).
```
3. Финальные контрольные прогоны перед мержем (каждая docker-серия — с
   зачисткой после финальной строки):
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --filter "FullyQualifiedName~CoordinationSmoke"
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Api|FullyQualifiedName~Metrics"
docker ps -a --format '{{.Names}}' | grep "vwk-" | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Valkey"
docker ps -a --format '{{.Names}}' | grep "vwk-" | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"
docker ps -a --format '{{.Names}}' | grep "vwk-" | xargs -r docker rm -f; docker network prune -f
```
4. Мерж-коммит в `main` включает roadmap-правки (dev-flow завершение ветки;
   коммит/пуш — по команде пользователя).

**Выход:** roadmap актуален: тег t02 снят, долг t07 зафиксирован.

**Проверка:** `grep -rn "t02-valkey-worker" arch/roadmap/` — пусто;
`grep -n "t07-unify-docker-engine" arch/roadmap/pgworker.md` — одна строка;
контрольные прогоны зелёные; после серий хост чист
(`docker ps -a | grep -c "vwk-"` → 0).

**Spec:** §10 (мерж-гейт), §11.9.

- [ ] **Шаг 1.** Правки valkey.md (снять тег + зависимость t05).
- [ ] **Шаг 2.** Правка pgworker.md (добавить t07).
- [ ] **Шаг 3.** Контрольные прогоны Release с зачистками (build → юниты → смоук → Api/метрики → Valkey → E2E).
- [ ] **Шаг 4.** Мерж-коммит с roadmap-правками (по команде пользователя).

---

## Self-review плана

- **Покрытие spec:** §1 п.1 (процессы A–E) → Tasks 8–11; п.2 (координация
  Shared.Etcd, prefix `/valkeyworker`) → Task 2 + смоук Task 3; п.3 (HTTP API
  полный набор + seed + restart) → Task 13; п.4 (поставка) → Task 14; п.5
  (тесты) → Tasks 3/5/6/7/8/9/10/11/12/13/15/16; п.6 (мерж-гейт) → Task 17;
  §1.2 (arch-правка первой) → Task 1; §4.1–§4.2 → Task 2; §4.3 → Task 5;
  §4.4 → Task 3; §4.5 A/B/C/D/E → Tasks 8/9/10/11; §4.5.1 → Task 6; §4.6 →
  Task 12; §4.7 → Task 13 (форматы ресурсов — канон pg §9.3: Cpu decimal
  0.01–64 без суффикса m, MemGi/DiskGi int 1–65536, в etcd только `<n>Gi`);
  §4.8 → Task 2; §5 → Task 14; §6.1 → Tasks 3/5/6/7/8/9/10/11/12/13
  (валидации API — юниты Task 13); §6.2 → Tasks 3 (Etcd) / 13 (Api+метрики) /
  15 (Valkey); §6.3 → Task 16; §7 фазы 1–11 → Tasks 1–17 по порядку
  (PortAlloc*-каркас фазы 4 — Task 7, до использования в Task 8 фазы 6);
  §8 ограничения — Глобальные ограничения (вкл. допустимость
  `src/Directory.Build.props`); §9 метрики → приёмочные тесты Tasks 13
  (§9.2 секретность, §9.7 Meter)/15/16; §10 → Task 17; §11 критерии —
  проверки задач.
- **Плейсхолдеры:** TBD/TODO нет; каждое «порт <файла> 1:1» ссылается на
  существующий файл репозитория с таблицей замен (разрешено spec §2 п.2).
- **Типы:** `NodeLimits` (Task 2) → драйвер Task 4; `ValkeyNodeSpec` +
  `IClusterDriver` c `NodeArgsAsync` (Task 4) → Tasks 7/8/10; `IValkeyConnection` +
  `ValkeyEndpoint` (Task 5) → фейк Task 7, Tasks 8/10/11;
  `ValkeyClusterSnapshot`/`ParsedValkeySnapshot` (Task 3) → Tasks 8–12;
  `NodeArgsBuilder.Build` (Task 6) → Tasks 8/10 (сверка V3 — image+args+порт+
  лимиты); `ValkeyProvisioningOptions`/`IClusterSecretEnsurer`/`PortAllocIndex`/
  `PortAllocHealer` (Task 7) → Tasks 8/10/12; DTO Task 13 (`Cpu` decimal,
  `MemGi`/`DiskGi` int — образец kfw) согласованы с `ValkeyWriting.ResourcesJson`
  и ProcessCommon-парсером (Task 7); эндпоинты/коды Task 13 согласованы с §4.7;
  префикс `/valkeyworker` един во всех задачах; тег E2E-образа
  `valkeyworker:e2e` — зафиксированное сознательное отклонение от §6.3
  (изоляция от dev-образа, тот же Dockerfile).
