# t32-s3-cluster-provisioning — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Рабочий сервис S3Worker — оркестратор кластеров S3-хранилища бэкапов (платформа ownS3): provisioning по декларации, надзор, изменение состава, HTTP API (mTLS), поставка в deploy-контур и стенд, docker-E2E серия; плюс точечное расширение админ-грани ownS3 дисковыми полями (источник `usage`).

**Architecture:** 5 новых проектов `src/S3Worker.{Core,Etcd,Docker,Provisioning,App}` в solution-папке `/s3/` по образцу ValkeyWorker/KafkaWorker (те же слои, координация Shared.Etcd на префиксе `/s3worker`, каркас healthz/metrics/mTLS-API/LoopWatchdog). Процессы A–E — идемпотентные машины состояний по arch/24 §5 с клэймами/journal/portalloc по arch/17. Всё значимое состояние — в etcd; секреты нод — только env-профили воркера.

**Tech Stack:** .NET 10 (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions + Testcontainers, AWSSDK.S3 (bucket-операции), etcd HTTP JSON gateway `/v3/*`, Docker Engine API через `Shared.Docker`.

**Spec:** [`docs/superpowers/2026-10-11-t32-s3-cluster-provisioning/spec.md`](spec.md) — план аргументирует от спеки; исполнители читают спеку и план вместе. Каноны: [`arch/23-s3-clusters.md`](../../../arch/23-s3-clusters.md), [`arch/24-s3worker.md`](../../../arch/24-s3worker.md), [`arch/17-synchronization-principles.md`](../../../arch/17-synchronization-principles.md), [`arch/21-valkeyworker.md`](../../../arch/21-valkeyworker.md) (образец слоёв), [`arch/owns3/05-access-config.md`](../../../arch/owns3/05-access-config.md).

**Рабочая директория всех команд** — корень worktree `/Users/demakaev/ZCodeProject/worktrees/t32-s3-cluster-provisioning` (в командах — относительные пути от него; сборка/тесты — через `src/PgWorker.slnx`).

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — сборка без единого предупреждения (spec §14.12); версии пакетов — только через `src/Directory.Packages.props` (AWSSDK.S3 там уже есть).
- Источник истины — arch/23 + arch/24 (+ owns3/05 §7 после Task 1); расхождение кода с каноном — блокирующее замечание. Канонические примеры значений arch/23 §2.1 — критерий приёмки парсеров.
- Префиксы etcd: домен `/s3/`, координация `/s3worker/`; пишет оба префикса ТОЛЬКО S3Worker (spec §14.1). Ключи координации — arch/23 §8 один в один: `/s3worker/{leader,claims/<C>,work/<C>,portalloc/<C>,locks/portalloc,instances/<id>,api/<id>,rotations/<C>}`; lease TTL 15 с.
- Идемпотентность каждого шага по факту; journal-before-manipulations (`/s3worker/work/<C>`); мутации `/s3/` — только под живым клэймом `<C>` (S5); деструктив — только по положительному свидетельству docker-инспекции (S7); фазовые записи журнала не затирают накопленные треки (S6 — для S3Worker в журнале нет доменных треков надзора, но WritePhaseAsync каркаса уже сохраняет семантику).
- Поллинг-инвариант: таймаут одиночного внешнего вызова (etcd/docker/пробы нод/S3) = половина окна watchdog (7.5 с при дефолтах); никаких блокирующих ожиданий длиннее 30 с в коде агента/тестов; `LoopWatchdog` на всех циклах.
- Секреты: значения `S3W_NODE_SECRETS__*` никогда не попадают в etcd/журнал/лог/метрики/тексты исключений (spec §8, §14.8). Диагностика профилей — только имена.
- Имена docker-объектов детерминированы: контейнер `s3w-<C>-node<k>`, volume `s3w-<C>-node<k>-data`; контейнерный S3-порт 9000; restart `unless-stopped`; label `s3worker` (arch/24 §2).
- E2E/интеграционные: динамические порты (`assignRandomHostPort`/`GetMappedPublicPort`/зонд свободных портов — литералов-портов в expects нет); `NodeBootSec` ≤ 100 с; полный teardown при любом исходе + ассерт чистоты; телеметрия в `/tmp/pgw-e2e-artifacts-<guid>/` ДО удаления; `[PHASE]`-метки для фаз > 60 с; перезапуск упавших тестов без анализа логов — запрещён (docs/e2e-isolation.md, docs/e2e-launch.md).
- Образ `s3worker` — publish на хосте + runtime-COPY (запрет `COPY src/`/sdk-стадий в образы для E2E/стенда, AGENTS.md); локально собираемые образы в registry `192.168.0.1:5000` не кладутся.
- Язык: комментарии/документация — русские, идентификаторы — английские. Тесты — с комментариями по AAA (Arrange/Act/Assert).
- Каждый Task заканчивается коммитом в feature-ветке worktree (в main не мержим; коммит-пуш — по гейтам dev-flow).

## Файловая карта (создаваемое/изменяемое)

Новые проекты (solution-папка `/s3/` в `src/PgWorker.slnx`):

- `src/S3Worker.Core/` — `Model/S3Domain.cs`, `OwnS3/IOwnS3NodeClient.cs`, `OwnS3/OwnS3NodeClient.cs`, `OwnS3/SigV4GetSigner.cs`
- `src/S3Worker.Etcd/` — `Parsing/S3SnapshotParser.cs`
- `src/S3Worker.Docker/` — `S3Worker.Docker.csproj`, `Drivers/ClusterDriver.cs`
- `src/S3Worker.Provisioning/` — `Processes/S3ProcessCommon.cs`, `Processes/S3Planning.cs`, `Processes/PlacementResolver.cs`, `Processes/PortAllocIndex.cs`, `Processes/PortAllocHealer.cs`, `SecretProfileStore.cs`, `Processes/ProvisioningProcess.cs`, `Processes/DeprovisioningProcess.cs`, `Processes/NodeSupervisor.cs`, `Processes/AddNodeProcess.cs`, `Processes/RemoveNodeProcess.cs`, `Processes/SecretRotator.cs`
- `src/S3Worker.App/` — `Program.cs`, `Options.cs`, `appsettings.json`, `EtcdConnectCallback.cs`, `HealthState.cs`, `LoopsVitality.cs`, `Loops/{ReconcileLoop,KeepaliveLoop,SnapshotLoop,S3ClusterClassifier,S3ClusterProcesses}.cs`, `Api/{ApiModule,TlsEndpoints,WorkerApiCertReader}.cs`, `Api/Operations/{S3Api,CreateClusterHandler,DeleteClusterHandler,AddNodeHandler,DeleteNodeHandler,UpdateNodeResourcesHandler,PostRotationHandler,SyncProbeHandler,SeedDemoHandler}.cs`, `HealthChecks/{ServiceProbes,S3WorkerHealth}.cs`
- `src/tests/S3Worker.UnitTests/`, `src/tests/S3Worker.IntegrationTests/` (+ `E2e/`)

Точечные правки существующего:

- `arch/owns3/05-access-config.md` §7 (Task 1, arch-first)
- `src/OwnS3.App/Handlers/AdminHandlers.cs` + `src/tests/OwnS3.IntegrationTests/Api/AdminScenarios.cs` (Task 2)
- `src/Shared.Docker/Engine/IDockerEngine.cs` + `Engine/DockerEngine.cs` (Task 6: labels swarm-нод)
- `docker/S3Worker.Dockerfile` (new), `deploy/docker-compose.yml`, `deploy/.env.example`, `deploy/tls/gen.sh` (Task 16)
- `deploy/docker-compose.contour.yml`, `dev-stand/adminpanel/checks/00-up.sh`, `dev-stand/adminpanel/checks/75-s3worker.sh` (new) (Task 17)

Порядок задач = порядок исполнения; зависимости указаны в поле «Вход» каждой задачи. Порядок 3–15 допускает перестановку внутри слоя при сохранении «юнити раньше интеграции»; Task 2 строго до Task 8 (usage-клиент читает формат грани); arch-правка §2.2 (Шаг 12.1) — строго до кода процессов D/E (внутри Task 12).

---

### Task 1: Arch-правка owns3/05 §7 — дисковые поля GET /admin/storage

**Spec:** §2.1 (правка — первый шаг реализации, до кода грани), §1 п.4, §12 фаза 2, §14.2.

**Вход:** ветка t32 чистая; спека прочитана; правила arch-first (AGENTS.base.md §1).

**Files:**
- Modify: `arch/owns3/05-access-config.md` (§7 «Админ-грань (erasure-режим)», пункт `GET /admin/storage`)

**Interfaces:**
- Produces: канон формата ответа `GET /admin/storage` с дисковыми полями (используют Task 2 — код грани, Task 8 — usage-клиент, тесты AdminScenarios).

**Действие:**

- [ ] Шаг 1.1. В §7 после абзаца «GET /admin/storage — статус: …» дополнить пункт (тон документа — канонический, без истории задачи):

```markdown
- **Дисковые поля** (источник — статистика ФС корней режима, тот же, что у
  метрик `ownS3_disk_*` §5): `usedBytes`/`totalBytes` (long, camelCase).
  single-режим — верхнего уровня по тому данных (`OWNS3_DATA_DIR`-корень);
  EC-режим — верхнего уровня (сумма по дискам пула) и в разрезе каждого
  диска (`sets[].drives[].usedBytes/totalBytes` по пути диска). Корень/диск
  недоступны для подсчёта (DriveInfo-ошибка) — соответствующие поля
  `null` (грань не падает; наблюдаемость деградации — метрики §5).
  Существующие поля (`mode`/`sets`/`heal`) не меняются — грань расширяется
  обратной совместимостью.
```

- [ ] Шаг 1.2. Проверить, что формулировка не конфликтует с §5 (метрики `ownS3_disk_used_bytes`/`ownS3_disk_total_bytes` — тот же источник) и с чек-строкой про `47-owns3-ec.sh` (грань остаётся единой). Заголовок §7 не трогать.

**Выход:** канон грани описывает дисковые поля; код грани (Task 2) получает источник истины.

**Проверка:** `git diff arch/owns3/05-access-config.md` — одна точечная правка §7; повторное чтение §7 целиком не противоречит arch/23 §5 («занятость диска — админ-грань GET /admin/storage»).

- [ ] Шаг 1.3. Коммит: `git add arch/owns3/05-access-config.md && git commit -m "t32: arch-first — owns3/05 §7 дисковые поля GET /admin/storage (usedBytes/totalBytes: single — верхний уровень, EC — по сетам/дискам)"`

---

### Task 2: Админ-грань ownS3 — код дисковых полей + тесты

**Spec:** §2.1, §11 «OwnS3», §14.2.

**Вход:** Task 1 смержён в ветку (канон описывает поля).

**Files:**
- Modify: `src/OwnS3.App/Handlers/AdminHandlers.cs`
- Test: `src/tests/OwnS3.IntegrationTests/Api/AdminScenarios.cs` (расширение существующего файла: кейсы single + EC)

**Interfaces:**
- Consumes: `StorageModeInfo.DataRoots` (см. `src/OwnS3.App/Pipeline/VolumeCleanupService.cs` — образец подсчёта `DriveInfo`), `ErasureStatusProvider.GetStatus()` (`Sets[].Drives[].Path`).
- Produces: ответ `GET /admin/storage` — JSON camelCase: single `{mode, usedBytes, totalBytes, sets, heal}`; EC `{mode, usedBytes, totalBytes, sets:[{…, drives:[{position, path, diskId, online, usedBytes, totalBytes}]}], heal}`. Недоступный диск — `null`-поля.

**Действие:**

- [ ] Шаг 2.1. Падающие тесты (AAA, xunit.v3 + FluentAssertions — стиль файла). В `AdminScenarios`:

```csharp
// Single: дисковые поля верхнего уровня присутствуют и согласованы с томом.
[Fact]
public async Task Storage_SingleMode_DiskFieldsMatchDataRoot()
{
    // Arrange — single-фабрика коллекции; root-креды
    var client = New SingleModeEdge factory client (по образцу класса SingleModeEdge);

    // Act
    var response = await client.SendSignedAsync("GET", "/admin/storage", credentials: root);
    var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(...));

    // Assert — поля есть; согласованы с фактическим томом того же корня
    json.RootElement.GetProperty("usedBytes").GetInt64().Should().BeInRange(0, long.MaxValue);
    json.RootElement.GetProperty("totalBytes").GetInt64().Should().BeGreaterThan(0);
    json.RootElement.GetProperty("usedBytes").GetInt64()
        .Should().BeLessThanOrEqualTo(json.RootElement.GetProperty("totalBytes").GetInt64());
    // согласованность: totalBytes == DriveInfo того же корня данных фабрики
    var root = <DataRoot из конфигурации single-фабрики>;
    new DriveInfo(Path.GetFullPath(root)).TotalSize.Should().Be(json.RootElement.GetProperty("totalBytes").GetInt64());
}
```

EC-кейс (в основной класс): после существующих ассертов `Storage_RootKey_200_EcSchema` — новый `[Fact] Storage_EcDiskFields_PerDriveAndSum`: каждый `drives[i].usedBytes/totalBytes` не-null и `used ≤ total`; верхнего уровня `usedBytes == Σ drives[].usedBytes`, `totalBytes == Σ drives[].totalBytes` (EC-фабрика: 4 диска-подкаталога одного физического тома — суммы по 4 записям одного `DriveInfo`).

- [ ] Шаг 2.2. Прогнать — ожидаются FAIL (полей нет):

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName~AdminScenarios"`
Expected: новые кейсы FAIL (`usedBytes` property not found), прежние — PASS.

- [ ] Шаг 2.3. Реализация в `AdminHandlers.HandleStorageAsync` (не трогая `HandleHealAsync`):

```csharp
// single: подсчёт по mode.DataRoots (образец — VolumeCleanupService.UpdateDiskGauges);
// EC: per-drive DriveInfo(drive.Path) + суммы пула; IOException/UnauthorizedAccess
// от DriveInfo → long? null (грань не падает — канон 05 §7).
private static (long? Used, long? Total) DiskOf(string root)
{
    try { var d = new DriveInfo(root); return (d.TotalSize - d.TotalFreeSpace, d.TotalSize); }
    catch (Exception) { return (null, null); }  // корень недоступен — поля null
}
```
single-ветка: анонимный объект получает `usedBytes`/`totalBytes` из `DiskOf(mode.DataRoots.Single())` (или агрегата, если корней несколько); EC-ветка: каждый `drives`-элемент получает `usedBytes`/`totalBytes` из `DiskOf(d.Path)`; верхний уровень — сумма не-null значений (все null → null). camelCase сохранён `JsonOptions`.

- [ ] Шаг 2.4. Прогон зелёный:

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName~AdminScenarios"`
Expected: PASS (все, вкл. прежние — обратная совместимость).

- [ ] Шаг 2.5. Регрессия OwnS3 целиком (грань тронута — прогнать всю серию; это НЕ docker-E2E):

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName!~E2e"`
Expected: PASS без предупреждений (TreatWarningsAsErrors).

**Выход:** `GET /admin/storage` отвечает дисковыми полями в обоих режимах; обратная совместимость подтверждена тестами.

**Проверка:** зелёные команды Шагов 2.4–2.5.

- [ ] Шаг 2.6. Коммит: `git add -A src/OwnS3.App/Handlers/AdminHandlers.cs src/tests/OwnS3.IntegrationTests/Api/AdminScenarios.cs && git commit -m "t32: админ-грань ownS3 — дисковые поля usedBytes/totalBytes GET /admin/storage (single/EC, owns3/05 §7)"`

---

### Task 3: Каркас проектов S3Worker.* — slnx, Options, Program, mTLS-грань

**Spec:** §4 (таблица проектов), §6 (грань API), §12 фаза 3, §14.12.

**Вход:** Tasks 1–2 в ветке (не блокируют, но держим порядок фаз); образцы `src/ValkeyWorker.App/*` прочитаны исполнителем.

**Files:**
- Create: `src/S3Worker.Core/S3Worker.Core.csproj`, `src/S3Worker.Etcd/S3Worker.Etcd.csproj`, `src/S3Worker.Docker/S3Worker.Docker.csproj`, `src/S3Worker.Provisioning/S3Worker.Provisioning.csproj`, `src/S3Worker.App/S3Worker.App.csproj`
- Create: `src/S3Worker.App/Options.cs`, `src/S3Worker.App/Program.cs`, `src/S3Worker.App/appsettings.json`, `src/S3Worker.App/EtcdConnectCallback.cs`, `src/S3Worker.App/HealthState.cs`, `src/S3Worker.App/Api/TlsEndpoints.cs`, `src/S3Worker.App/Api/WorkerApiCertReader.cs`, `src/S3Worker.App/DockerEnvBindings.cs`
- Modify: `src/PgWorker.slnx` (папка `/s3/`)
- Test: `src/tests/S3Worker.UnitTests/*` (csproj + `App/OptionsValidationTests.cs`, `App/TlsEnvBindingsTests.cs`, `App/DockerEnvBindingsTests.cs`, `App/EtcdConnectCallbackTests.cs`)

**Interfaces:**
- Consumes: `Shared.{Core,Etcd,Tls,Metrics,Docker}`; каркас `Shared.Core.Hosting` (ILoopProgress, LoopWatchdog, WatchdogConfigGuard, PulsingDelay); `TlsEnv.ApplyEnvOverrides`, `TlsMaterial` (Shared.Tls).
- Produces (для последующих задач):
  - `S3WorkerOptions` (полное дерево, arch/24 §8 — см. Шаг 3.3);
  - `TlsEndpoints.ApplyEnvOverrides/ConfigureMtls(builder, port: 8080, managedCert)` с env-биндингами `S3W_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]`;
  - `WorkerApiCertReader.ReadAsync(endpoints, "s3worker", ct)` — ключ `/workers/api_tls/s3worker` (env-фоллбек; прототип: `ValkeyWorker.App/Api/WorkerApiCertReader.cs`, замена префикса valkeyworker→s3worker);
  - `HealthState`, `EtcdConnectCallback.CreateHandler` (порт `ValkeyWorker.App/EtcdConnectCallback.cs`: PooledConnectionLifetime + IPv4-first; юнит-тест — клон `EtcdConnectCallbackTests` с заменой литерала `VWK_`→нет — у S3 env нет, тест на handler-поведение);
  - `public partial class Program;` (точка входа для WebApplicationFactory-тестов).

**Действие:**

- [ ] Шаг 3.1. csproj-файлы по образцам: `S3Worker.App.csproj` — копия `ValkeyWorker.App.csproj` с заменами имён проектов и `InternalsVisibleTo Include="S3Worker.UnitTests"/"S3Worker.IntegrationTests"`; `S3Worker.Core.csproj` — `Sdk="Microsoft.NET.Sdk"` + `ProjectReference: Shared.Core` + `PackageReference AWSSDK.S3` + `Using Include="Shared.Core"`; `S3Worker.Etcd.csproj` — ссылки на Shared.Etcd + S3Worker.Core; `S3Worker.Docker.csproj` — Shared.Docker + S3Worker.Core (+ `Using Shared.Docker`); `S3Worker.Provisioning.csproj` — копия `ValkeyWorker.Provisioning.csproj` с заменой Valkey→S3 (вкл. `Microsoft.Extensions.Logging.Abstractions` для ILogger) + ссылка на Shared.Etcd. Тестовые csproj — копии `ValkeyWorker.{UnitTests,IntegrationTests}.csproj` с заменой ссылок; IntegrationTests вкл. `Testcontainers` и `Microsoft.AspNetCore.Mvc.Testing`.

- [ ] Шаг 3.2. slnx: в `src/PgWorker.slnx` после `</Folder>` папки `/valkey/`:

```xml
    <Folder Name="/s3/">
        <Project Path="S3Worker.Core/S3Worker.Core.csproj" />
        <Project Path="S3Worker.Etcd/S3Worker.Etcd.csproj" />
        <Project Path="S3Worker.Docker/S3Worker.Docker.csproj" />
        <Project Path="S3Worker.Provisioning/S3Worker.Provisioning.csproj" />
        <Project Path="S3Worker.App/S3Worker.App.csproj" />
    </Folder>
```
и в `/tests/` — два тестовых проекта.

- [ ] Шаг 3.3. `Options.cs` — дерево arch/24 §8 точно (doc-комментарии — из канона):

```csharp
public sealed class S3WorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();                 // Endpoints[]
    public string Bucket { get; set; } = "";                       // per-install bucket, arch/19 §5
    public DockerOptions Docker { get; set; } = new();
    public LoopsOptions Loops { get; set; } = new();
    public ThresholdsOptions Thresholds { get; set; } = new();
    public ParallelismOptions Parallelism { get; set; } = new();   // MaxClusters=4
    public SnapshotOptions Snapshots { get; set; } = new();        // Dir=/snapshots, RetentionFiles=10, MaintenanceIntervalMin=60
    public ApiOptions Api { get; set; } = new();                   // AdvertiseUrl, EnableSeedEndpoint=false, Tls
}
public sealed class DockerOptions
{
    public string Mode { get; set; } = "Plain";                    // Plain|Swarm
    public DockerHostOptions[] Hosts { get; set; } = [];           // {Name, Endpoint}
    public string? SwarmManager { get; set; }
    public PortRangeOptions PortRange { get; set; } = new();       // From=18000, To=18999 (ряд pg/kafka/valkey; пример arch/23 §2.1 — порты 18001+)
    public DockerImagesOptions Images { get; set; } = new();       // OwnS3="owns3:dev"
    public Shared.Docker.DockerTlsOptions? Tls { get; set; }       // env S3W_DOCKER_TLS_*
    public Shared.Docker.SshTunnelOptions? Ssh { get; set; }       // env S3W_DOCKER_SSH_*
    public string? ContourNetwork { get; set; }                    // стенд/E2E: ноды в сети контура
    public string? AdvertisedHost { get; set; }                    // single-host/стенд; multi-host — имена хостов
}
public sealed class ThresholdsOptions
{
    public int NodeBootSec { get; set; } = 100;                    // ≤100 (спека §12.2)
    public int NodeDeadSec { get; set; } = 90;
    public int HealthIntervalSec { get; set; } = 15;               // период сбора usage
}
```
`LoopsOptions`/`ApiOptions`/`TlsOptions` — порты `ValkeyWorker.App/Options.cs` (Watchdog `Shared.Core.Hosting.WatchdogOptions`, `ErrorDelayMs=2000`, `SnapshotIntervalMin=360`).

- [ ] Шаг 3.4. `DockerEnvBindings.cs` — порт `PgWorker.App/DockerEnvBindings.cs` с заменами: `S3W_DOCKER_TLS_{CA,CERT,KEY}[_PATH] → S3Worker:Docker:Tls:*`, `S3W_DOCKER_SSH_KEY[_PATH] → S3Worker:Docker:Ssh:Key*`, `S3W_DOCKER_SSH_FINGERPRINT → S3Worker:Docker:Ssh:FingerprintSha256` (REST-TLS-блока НЕТ — он pg-специфичен; getEnv-инъекция для юнитов сохраняется).

- [ ] Шаг 3.5. Юниты (сначала тесты): `OptionsValidationTests` — валидации Program переносятся в тестируемый вид: проверить через `ValidateDataAnnotations`-паттерн нельзя — поэтому тестируем чистые предикаты; фактические fail-fast-проверки Program.cs вынести в `internal static class S3StartupChecks` (internal → InternalsVisibleTo): `EtcdEndpoints`, `AdvertiseUrl` (непустой; https:// либо AllowInsecureHttp), `DockerTopology` (Swarm→SwarmManager обязателен; Plain→Hosts непусты; Hosts без дубликатов имён), `Thresholds` (NodeBootSec/NodeDeadSec/HealthIntervalSec > 0; NodeBootSec ≤ 100), `Bucket` (непустое, `^[a-z0-9][a-z0-9_.-]{0,62}$`), `PortRange` (0 < From < To ≤ 65535). `TlsEnvBindingsTests` — env-биндинги `S3W_API_TLS_*[_PATH]` переносятся в конфиг-дерево (порт `TlsEnvBindingsTests` valkey). `DockerEnvBindingsTests` — TLS/SSH-биндинги `S3W_DOCKER_*`. Прогнать: FAIL (типов нет) → реализовать → PASS.

- [ ] Шаг 3.6. `Program.cs` — минимальный каркас (порт `ValkeyWorker.App/Program.cs` с заменами Valkey→S3; порядок сохранён):
  - `Configure<S3WorkerOptions>`; `HealthState` + `ILoopProgress`;
  - `AddAppMetrics("S3Worker", …)` → Meter `s3worker`; `WorkerMetricsInstrumentation` + подписка `WorkJournal.PhaseWritten += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase)` (WorkJournal появится в Task 4 — в этом шаге НЕ регистрируется, блок закомментирован TODO-маркером Task 4; вместо этого — сборка без координации);
  - `AddHttpClient("etcd", timeout 7.5 c) + EtcdConnectCallback.CreateHandler`;
  - `ValidateOnStart` через `S3StartupChecks` (все проверки Шага 3.5);
  - `WorkerApiCertReader.ReadAsync(etcdEndpoints, "s3worker")`;
  - `TlsEndpoints.ApplyEnvOverrides` + `ConfigureMtls(builder, 8080, managedCert)`; thumbprint серта;
  - `DockerEnvBindings.Apply*EnvOverrides`;
  - драйвер docker (Plain/Swarm `PlainClusterDriver`/`SwarmClusterDriver` — реализация Task 6; в этом шаге — только DI-фабрика DockerEngineFactory и fail-fast топологии из `S3StartupChecks.DockerTopology`);
  - `app.MapAppMetrics(); app.MapHealthChecks("/healthz");` + `MapWorkerApi()` — пустой (эндпоинты Task 14);
  - `await app.RunAsync(); public partial class Program;`.
  Секции, которым нужны типы из Tasks 4–14 (ClaimStore, WorkJournal, процессы, хендлеры, Loops), в этом шаге в Program НЕ пишутся — Program наращивается инкрементально задачами 4, 7–14 (каждая задача правит свой блок DI в Program.cs).

- [ ] Шаг 3.7. `appsettings.json` — зеркало `ValkeyWorker.App/appsettings.json` с деревом `S3Worker` (Etcd default `http://localhost:2379`; Docker: Plain, Hosts=[{local, unix:///var/run/docker.sock}], PortRange 18000–18999, Images.OwnS3="owns3:dev"; Thresholds 100/90/15; Api: AdvertiseUrl="", EnableSeedEndpoint=false).

- [ ] Шаг 3.8. Сборка + юниты зелёные:

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
Expected: Build succeeded, 0 Warning(s) (TreatWarningsAsErrors — любой warning = ошибка).
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Release`
Expected: PASS (Options/TlsEnv/DockerEnv/EtcdConnectCallback-юниты).

**Выход:** 5 проектов собираются в slnx; опции/fail-fast/mTLS-каркас/healthz/metrics на месте; точка входа тестов `Program`.

**Проверка:** команды Шага 3.8 зелёные.

- [ ] Шаг 3.9. Коммит: `git add -A src/PgWorker.slnx src/S3Worker.* src/tests/S3Worker.UnitTests && git commit -m "t32: каркас S3Worker — проекты /s3/ в slnx, Options (arch/24 §8), Program с fail-fast+mTLS-гранью, healthz/metrics (s3worker_*)"`

---

### Task 4: Координация Shared.Etcd + циклы (Keepalive/Snapshot/Reconcile) + классификатор

**Spec:** §3 (паттерны arch/17 S1/S5/S6), §5 (классификация тика), §9 (healthz/метрики/дискавери), §12 фаза 3.

**Вход:** Task 3 (Program, Options, HealthState). Образцы: `ValkeyWorker.App/Loops/*`, `Shared.Etcd/Coordination/*`.

**Files:**
- Create: `src/S3Worker.App/Loops/KeepaliveLoop.cs`, `Loops/SnapshotLoop.cs`, `Loops/ReconcileLoop.cs`, `Loops/S3ClusterClassifier.cs`, `Loops/S3ClusterProcesses.cs`, `LoopsVitality.cs`, `HealthChecks/ServiceProbes.cs`, `HealthChecks/S3WorkerHealth.cs`
- Modify: `src/S3Worker.App/Program.cs` (блоки координации/циклов/health-чеков; раскоммент PhaseWritten-seam)
- Test: `src/tests/S3Worker.UnitTests/App/S3ClusterClassifierTests.cs`, `App/LoopsVitalityTests.cs`, `App/HealthTests.cs`; `src/tests/S3Worker.IntegrationTests/Etcd/S3EtcdFixture.cs`, `Etcd/CoordinationSmokeTests.cs` (spec §11 «Координация»)

**Interfaces:**
- Consumes: `ClaimStore(keyPrefix:"/s3worker", …)`, `PortAllocLock("/s3worker", …)`, `WorkJournal("/s3worker", …)` — конструкторы без изменений (Shared.Etcd), `SnapshotJob` (Shared.Etcd.Maintenance), `Shared.Core.Hosting.LoopWatchdog` + `WatchdogConfigGuard`.
- Produces:
  - `enum S3ClusterKind { Provision, Deprovision, Active, Skip }`;
  - `static S3ClusterKind S3ClusterClassifier.Classify(S3ClusterSnapshot snap)` (S3ClusterSnapshot — Task 5; в этом шаге интерфейс заносится как forward-dep: если Task 5 ещё не сделан — выполнить Task 5 ПЕРЕД этим шагом, порядок 4↔5 свободен, но классификатору нужна модель);
  - `IS3ClusterProcesses` — диспетчер ветки тика (полная реализация Processes — Task 10–13; здесь интерфейс + no-op-реализация, ReconcileLoop тикает и логирует);
  - keepalive-луп поддерживает lease клэймов/лидера/api-ключа `/s3worker/api/<id>` (`Api:AdvertiseUrl` + thumbprint — см. ClaimStore-конструктор).

**Действие:**

- [ ] Шаг 4.1. (порядок) Выполнить Task 5 (модель+парсер), если ещё не выполнен — классификатор зависит от `S3ClusterSnapshot`.
- [ ] Шаг 4.2. Юнит-кейсы классификатора (AAA): `Config==null → Skip`; `State==null → Active`; `NOT_INITIALIZED → Provision`; `TO_REMOVE → Deprovision`; незнакомое `"SOMETHING"` → `Active`. Прогнать FAIL→реализовать (порт `ValkeyClusterClassifier` с заменой типа)→PASS.

- [ ] Шаг 4.3. Loops — порты `ValkeyWorker.App/Loops/{KeepaliveLoop,SnapshotLoop,ReconcileLoop}.cs` (замены: ValkeyWorkerOptions→S3WorkerOptions, ValkeyClusterSnapshot→S3ClusterSnapshot, парсер `S3SnapshotParser`, `IS3ClusterProcesses`); `ReconcileLoop`: тик = Range `/s3/` → `S3SnapshotParser.Parse` → по кластерам `Classify` → dispatch в `IS3ClusterProcesses` (с ограничением `Parallelism.MaxClusters`, паттерн valkey); счётчик unknownKeys → `Meter.CreateCounter<long>("s3worker_unknown_keys_total")` инкрементом `snapshot.UnknownKeys.Count` (spec §9). `S3ClusterProcesses` (в этом шаге): классификация → no-op с Debug-логом (процессы подселяются Tasks 10–13). `LoopsVitality` — порт `ValkeyWorker.App/LoopsVitality.cs`.

- [ ] Шаг 4.4. Health: `ServiceProbes` (etcd-reachable, docker-hosts ping через DockerEngineFactory-движки) и `S3WorkerHealth` (агрегатор: секции `etcd-reachable`, `docker-hosts`, `loops-alive`, `claims`, `snapshot-freshness`, `watchdog` — порт `ValkeyWorkerHealth`); регистрация HealthChecks + `AddLoopWatchdog` + `WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold` в Program (порт блока ValkeyWorker Program.cs:306–320).

- [ ] Шаг 4.5. Интеграционные (spec §11 «Координация»): `S3EtcdFixture` — порт `ValkeyEtcdFixture` (etcd-testcontainer `quay.io/coreos/etcd:v3.5.21`, динамический порт, префикс теста, teardown-очистка своих ключей). `CoordinationSmokeTests` (AAA): два ClaimStore на один etcd — клэйм `<C>` получает ровно один; PortAllocLock — взаимоисключение; WorkJournal `WritePhaseAsync` перезаписывает фазу, сохраняя структуру; api-ключ keepalive жив (Range `/s3worker/api/` непуст после Start+tick).

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~CoordinationSmoke"`
Expected: PASS; после прогона `docker ps -a | grep etcd` — пусто (фикстура за собой убрала).

**Выход:** воркер-каркас тикает: keepalive держит lease, reconcile читает `/s3/` и классифицирует, snapshot-луп лидера, healthz честный, watchdog вооружён.

**Проверка:** Шаг 4.2-юниты и Шаг 4.5-интеграция зелёные; сборка slnx без предупреждений.

- [ ] Шаг 4.6. Коммит: `git add -A && git commit -m "t32: координация /s3worker + циклы воркера (клэймы/portalloc-lock/journal, Reconcile 5 c, классификатор, healthz, watchdog)"`

---

### Task 5: Модель домена + S3SnapshotParser (чтение /s3/)

**Spec:** §4 (S3Worker.Etcd), §11 юниты «S3SnapshotParser», §14.1 (канонические примеры §2.1 — критерий приёмки), arch/23 §2/§2.1/§9.

**Вход:** Task 3 (проекты). Образец: `ValkeyWorker.Etcd/Parsing/ValkeySnapshotParser.cs`.

**Files:**
- Create: `src/S3Worker.Core/Model/S3Domain.cs`, `src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs`
- Test: `src/tests/S3Worker.UnitTests/Etcd/S3SnapshotParserTests.cs`

**Interfaces:**
- Produces:

```csharp
namespace S3Worker.Core.Model;
public sealed record S3ClusterConfig(string? Platform, int? Nodes, string? SecretProfile,
    long? CreatedUnix, string? State);           // State: NOT_INITIALIZED|TO_REMOVE|null=Active|raw
public sealed record S3NodePlacement(string? DockerHost, string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);
public sealed record S3NodeResources(string? Cpu, string? Mem, string? Disk);   // "2"|"4Gi"|"200Gi"
public sealed record S3NodeUsage(long UsedBytes, long TotalBytes, long UpdatedUnix);
public sealed record S3NodeSnapshot(string Node, string? State, string? PlacedOn,
    S3NodePlacement? Placement, S3NodeResources? Resources, S3NodeUsage? Usage, string? ParseError);
public sealed record S3ClusterSnapshot(string Cluster, S3ClusterConfig? Config,
    IReadOnlyList<S3NodeSnapshot> Nodes, string? Endpoints, string? ParseError);
public sealed record S3DomainSnapshot(IReadOnlyList<S3ClusterSnapshot> Clusters,
    IReadOnlyList<string> UnknownKeys);
public static class S3NodeStates
{
    public const string NotInitialized = "NOT_INITIALIZED"; public const string Provisioning = "PROVISIONING";
    public const string Running = "RUNNING"; public const string Unreachable = "UNREACHABLE";
    public const string Removing = "REMOVING"; public const string ToRemove = "TO_REMOVE";
}
```

**Действие:**

- [ ] Шаг 5.1. Юнит-тесты на канонических примерах arch/23 §2.1 (значения — один в один из канона в тест-данные; AAA):
  - `config` заявки: `{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800,"state":"NOT_INITIALIZED"}` → Config{Platform="owns3", Nodes=3, SecretProfile="backups-main", CreatedUnix=1760156800, State="NOT_INITIALIZED"};
  - `config` Active: то же без `state` → State==null;
  - `config` + `"state":"TO_REMOVE"`; `placement` plain/swarm/swarm_labels/комбинация; `resources` `{"cpu":"2","mem":"4Gi","disk":"200Gi"}`; `state:"RUNNING"`; `placed_on:"h1"`; `usage` пример §2.1; `endpoints:"h1:18001,h2:18002,h3:18003"`;
  - unknownKeys: ключ `/s3/clusters/c/unknown-t33` (и `/s3/whatever`) → в `UnknownKeys`, кластер не пострадал;
  - битый JSON `config` → snapshot.ParseError != null, тик не падает (Parse возвращает Success с ParseError);
  - битый `usage`/`placement` → node.ParseError, кластер жив;
  - `state`-семантика: отсутствие = Active (классификатор — Task 4).

Run: `dotnet test src/tests/S3Worker.UnitTests -c Release --filter FullyQualifiedName~S3SnapshotParser` → FAIL → реализация → PASS.

- [ ] Шаг 5.2. `S3SnapshotParser.Parse(IReadOnlyList<Kv> kvs) → Result<S3DomainSnapshot>`: разбор `/s3/clusters/<C>/{config,nodes/node<k>/{placement,resources,state,placed_on,usage},endpoints}` (ключи §3 `connectivity`/`sync` — НЕ читаем: t33, идут в UnknownKeys); толерантность: JsonException → ParseError; узлы сортируются по номеру `node<k>` (Ordinal); лишние/неизвестные — UnknownKeys.

**Выход:** чтение домена `/s3/` с канонической точностью и толерантностью.

**Проверка:** Шаг 5.1 зелёный; сборка без предупреждений.

- [ ] Шаг 5.3. Коммит: `git add -A && git commit -m "t32: модель S3-домена + S3SnapshotParser (канонические примеры arch/23 §2.1, unknownKeys/битый-JSON-толерантность)"`

---

### Task 6: Docker-слой — ClusterDriver + labels swarm-нод (Shared.Docker)

**Spec:** §4 (S3Worker.Docker), §7 (docker-механика), §12 фаза 5.

**Вход:** Task 3. Образцы: `ValkeyWorker.Docker/Drivers/ClusterDriver.cs` (Plain/Swarm), `Shared.Docker/Engine/*`.

**Files:**
- Modify: `src/Shared.Docker/Engine/IDockerEngine.cs` (record `DockerSwarmNode`), `src/Shared.Docker/Engine/DockerEngine.cs` (парсинг labels)
- Create: `src/S3Worker.Docker/Drivers/ClusterDriver.cs`
- Test: `src/tests/Shared.Docker.UnitTests` (кейс парсинга labels — в существующий файл ListNodes-тестов), `src/tests/S3Worker.UnitTests/Docker/ClusterDriverTests.cs` (фейковый IDockerEngine)

**Interfaces:**
- Produces:

```csharp
namespace S3Worker.Docker.Drivers;
public sealed record HostEndpoint(string Name, string Endpoint);
public sealed record S3NodeSpec(string Cluster, string NodeName, string Host, int S3HostPort,
    string Image, IReadOnlyDictionary<string, string> Env, decimal? CpuCores, long? MemoryBytes);
public sealed record S3NodeInspection(string Host, int S3HostPort, bool Running = true);
// Атрибуты хоста для PlacementResolver (arch/23 §4): plain — Name из Hosts[], Labels пуст;
// swarm — Name=hostname, Labels=node.labels.*.
public sealed record S3HostAttributes(string Name, int UsedSlots, IReadOnlyDictionary<string, string> Labels);
public interface IS3ClusterDriver
{
    int S3ContainerPort { get; }   // 9000
    Task<Result<IReadOnlyList<S3HostAttributes>>> GetHostsAsync(CancellationToken ct);
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);
    Task<Result> EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct);
    Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct);
    Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct);
    Task<Result<S3NodeInspection?>> InspectNodeAsync(string cluster, string node, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(string cluster, string node, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct); // контейнеры+тома (сироты X2)
}
```
- `DockerSwarmNode` получает `IReadOnlyDictionary<string, string> Labels` (default — пустой) + парсинг `Spec.Labels` в ListNodesAsync — юнит Shared.Docker.UnitTests на JSON-ответе `/nodes` с labels.

**Действие:**

- [ ] Шаг 6.1. Shared.Docker: расширить `DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks, IReadOnlyDictionary<string,string> Labels)`; парсинг `node.Spec.Labels` (для swarm-селекторов `swarm_labels`, arch/23 §4). Юнит-тест (AAA): JSON `/nodes` c `Spec.Labels {"geo":"dc1"}` → Labels отражены; без labels → пустой словарь. FAIL→реализация→PASS. Прогон всей Shared.Docker.UnitTests + Shared.Etcd/Core.UnitTests (общая сборка тронута):

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests src/tests/Shared.Etcd.UnitTests src/tests/Shared.Core.UnitTests -c Release` → PASS (Shared.Docker тронут — все Shared-юнит-серии одной командой).

- [ ] Шаг 6.2. Юнит-кейсы ClusterDriver на фейковом `IDockerEngine` (порт стиля `ClusterDriverEnvTests`/`ClusterDriverLimitsTests` valkey; AAA):
  - `EnsureNodeAsync`: контейнер `s3w-<C>-node<k>`, ports (9000→host), env `OWNS3_*`, `RestartPolicy="unless-stopped"`, `Label/LabelKey="s3worker"`, `VolumeName="s3w-<C>-node<k>-data"`+`VolumeDest="/data"`, лимиты CpuCores/MemoryBytes переданы; `contourNetwork!=null` → `Network=contourNetwork`; null → Network не задан;
  - идемпотентность: существующий контейнер с теми же параметрами — повторный Ensure не пересоздаёт (сверка имени; 409 = успех движка);
  - `RemoveNodeAsync(removeVolume:true)` — RemoveContainer + DeleteVolume; 404 → успех;
  - `InspectNodeAsync`: контейнера нет → `null` (положительное свидетельство S7); есть → Host+порт из PortBindings+Running;
  - `NodeEnvAsync`: env контейнера (ротация/сверки) — по имени через перебор хостов (plain) / InspectServiceEnvAsync (swarm);
  - `ListNodeObjectsAsync`: имена контейнеров `s3w-<C>-*` и томов `s3w-<C>-*-data` (перечисление через ListContainersAsync(namePrefix)/ListVolumes — проверить фактические возможности движка: если листинга томов нет — перебор детерминированных имён `node1..node9` через VolumeExistsAsync).

Run FAIL → реализация `PlainClusterDriver`/`SwarmClusterDriver` (порты ValkeyWorker-драйверов с заменами: без ACL-args (env вместо Args), volume-механика EnsureVolume/VolumeName, swarm: CreateService с NodeConstraint по hostname из GET /nodes) → PASS.

- [ ] Шаг 6.3. Program.cs: DI `IS3ClusterDriver` по Mode (порт блока ValkeyWorker Program.cs:116–138 с S3-опциями; fail-fast топологии уже в S3StartupChecks).

**Выход:** драйвер умеет весь docker-слой домена: placement-атрибуты, занятость портов, ensure/remove нод с volume, инспекции S7, env-сверка, сироты.

**Проверка:** юниты Task 6 + Shared.Docker.UnitTests зелёные; сборка slnx чистая.

- [ ] Шаг 6.4. Коммит: `git add -A && git commit -m "t32: S3Worker.Docker ClusterDriver (s3w-<C>-node<k> + volume, S7-инспекции) + swarm labels в Shared.Docker"`

---

### Task 7: PlacementResolver + PortAllocIndex + PortAllocHealer

**Spec:** §5 P1, §7 (placement/portalloc), §11 юниты «Placement-резолвер», «Порт-аллокатор», §12 фаза 5; arch/23 §4; arch/17 S5/E9.

**Вход:** Tasks 5–6. Образцы: `ValkeyWorker.Provisioning/Processes/{PortAllocIndex,PortAllocHealer}.cs`, `Shared.Core/Planning/PortAllocator.cs`, `KfwPlanning.cs`.

**Files:**
- Create: `src/S3Worker.Provisioning/Processes/S3Planning.cs`, `Processes/PlacementResolver.cs`, `Processes/PortAllocIndex.cs`, `Processes/PortAllocHealer.cs`
- Test: `src/tests/S3Worker.UnitTests/Provisioning/{PlacementResolverTests,PortAllocIndexTests,PortAllocHealerTests}.cs`

**Interfaces:**
- Produces:
  - `S3Planning` (по образцу KfwPlanning): `Group(cluster, nodes)`, `PortsOf(NodeAddress a) => [a.S3Port]`, `HostOf`, `MakeAddress(host, port) => new NodeAddress(host, port)`, `KeyOf(p) => p.Node`; `NodeAddress` — S3Worker.Core `record NodeAddress(string Host, int S3Port)`;
  - `static Result<string> PlacementResolver.Resolve(S3NodePlacement selector, IReadOnlyList<S3HostAttributes> hosts, out IReadOnlyList<string> candidates)` — предикат равенства (конъюнкция всех указанных атрибутов: `docker_host`→`Name` (plain), `swarm_node`→`Name` (swarm hostname), `swarm_labels`→подмножество `Labels`), кандидаты → `OrderBy(UsedSlots).ThenBy(Name, Ordinal)`, первый; ноль кандидатов → Failed (transient-фейл фазы P1);
  - `PortAllocIndex.ForeignAllocatedPortsAsync(exceptCluster)` — busy-порты чужих `/s3worker/portalloc/<C>` (поле `"s3"`; битые чужие ключи — Warning+skip);
  - `PortAllocHealer.HealNodeAsync(cluster, node)` — E9-лестница: запись portalloc есть → ок; нет → `InspectNodeAsync` живого контейнера → реконструкция (put-if-absent под `locks/portalloc`); объекта нет → новая аллокация вызывающим (S7);
  - формат portalloc (arch/23 §8): `{"node1":{"host":"h1","s3":18001},"node2":{"host":"h2","s3":18002}}` — `S3ProcessCommon.ParsePortAlloc/SerializePortAlloc` (см. Task 9 — S3ProcessCommon создаётся здесь, без процессов).

**Действие:**

- [ ] Шаг 7.1. Юниты PlacementResolver (AAA):
  - plain-селектор `{"docker_host":"h1"}` при хостах h1/h2 → h1;
  - ноль кандидатов (нет такого имени) → Failed, candidates пуст;
  - least-loaded: h1(3 слота)/h2(1 слот) → h2; tie 1/1 → лексикографически меньший (Ordinal);
  - swarm: `swarm_node:"n1"`; `swarm_labels {"geo":"dc1"}` → хост с меткой; `{"geo":"dc1","tier":"s3"}` — конъюнкция: хост без `tier` не кандидат;
  - идемпотентность повторных проходов: тот же селектор/хосты → тот же хост.

- [ ] Шаг 7.2. Юниты PortAllocIndex: чужие записи с `"s3"`-портом собираются; свой exceptCluster исключён; битый JSON чужого ключа — skip без падения.

- [ ] Шаг 7.3. Юниты PortAllocHealer (порт `PortAllocHealerTests` valkey — E9-лестница, AAA): утерян журнал → реконструкция из inspect (порт+host → put-if-absent под клэймом); контейнера нет → Failed «новая аллокация» (вызывающий делает её); проигрыш put-if-absent (чужая запись) → re-read, использование чужой.

Run: FAIL → реализации (`PlacementResolver` — чистая функция; `PortAllocIndex` — порт valkey c полем `s3`; `PortAllocHealer` — порт valkey с заменой driver-метода `InspectNodeAsync`) → PASS.

**Выход:** P1-механика размещения и порт-закрепления готова к процессам.

**Проверка:** три юнит-файла зелёные.

- [ ] Шаг 7.4. Коммит: `git add -A && git commit -m "t32: PlacementResolver (arch/23 §4) + PortAllocIndex/Healer (t90/E9, формат portalloc s3)"`

---

### Task 8: Клиент ownS3-ноды — health / bucket / usage (+SigV4)

**Spec:** §4 (S3Worker.Core — IOwnS3NodeClient), §5 C (usage), §11 юниты «клиент ownS3», §12 фаза 6; owns3/05 §3/§5/§7 (после Task 1 — формат грани). Зависимость: Task 2 (дисковые поля) выполнен.

**Вход:** Tasks 3, 5 (NodeAddress, secrets-тип появится в Task 9 — интерфейс клиента принимает их: perform Task 9 ПЕРЕД реализацией клиента, либо определить `OwnS3NodeSecrets` в Task 9 и в этом Task только потребить; порядок 8↔9 свободен, но сборка требует оба).

**Files:**
- Create: `src/S3Worker.Core/OwnS3/IOwnS3NodeClient.cs`, `OwnS3/OwnS3NodeClient.cs`, `OwnS3/SigV4GetSigner.cs`
- Test: `src/tests/S3Worker.UnitTests/OwnS3/{OwnS3NodeClientTests,SigV4GetSignerTests}.cs` (моки через кастомный HttpMessageHandler)

**Interfaces:**
- Produces:

```csharp
public sealed record OwnS3NodeUsage(long UsedBytes, long TotalBytes);
public interface IOwnS3NodeClient
{
    // GET /healthz (без подписи): 200 → true; таймаут/5xx/404 → false. HttpClient "owns3-node", timeout 7.5 c.
    Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct);
    // exists-проба: GetBucketLocation → 200=true, 404=false (AWSSDK, root-креды профиля, ForcePathStyle).
    Task<Result<bool>> BucketExistsAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);
    // идемпотентно: exists → Success(skip); PutBucket; 409 BucketAlreadyOwnedByYou → Success (owns3/02 CreateBucket).
    Task<Result> EnsureBucketAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);
    // SigV4 GET /admin/storage root-кредом → usedBytes/totalBytes (Task 1-формат); null-поля → Failed (транзиент).
    Task<Result<OwnS3NodeUsage>> GetUsageAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);
}
```

**Действие:**

- [ ] Шаг 8.1. Юниты SigV4GetSigner (AAA): детерминированный вектор (фиксированные дата/креды/путь) → точный `Authorization: AWS4-HMAC-SHA256 Credential=…/…, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=<hex>`; сверка сигнатуры с независимо посчитанным значением в тесте (захардкоженный ожидаемый hex — рассчитывается тестом через те же примитивы, но отдельным кодом — паттерн «двойная реализация»: в тесте подпись пересчитывается прямо в теле теста через HMAC-цепочку).
- [ ] Шаг 8.2. Юниты OwnS3NodeClient на мок-handler (AAA):
  - health: 200 → true; 500/timeout → false (транзиент, не исключение наружу кроме transport);
  - EnsureBucket: первый вызов GetBucketLocation 404 → PutBucket 200 → Success; повторный exists=true → PutBucket не вызван;
  - EnsureBucket: PutBucket 409 → Success (идемпотентность);
  - usage: 200 `{"mode":"XL Single","usedBytes":10,"totalBytes":20,…}` → OwnS3NodeUsage(10,20); 403/500 → Failed; null-поля → Failed;
  - контракты таймаутов: HttpClient "owns3-node" с Timeout 7.5 c (assert конфигурации).
  Run FAIL → реализация → PASS.
- [ ] Шаг 8.3. Реализация: `SigV4GetSigner` — ручная заголовочная подпись GET без payload (canonical request: GET, canonical URI, пустой query, заголовки host/x-amz-content-sha256/x-amz-date, `x-amz-content-sha256` = SHA256 пустого тела; stringToSign/derivedKey по стандарту SigV4 — то же ядро, что `OwnS3.Protocol/Auth/SigV4Core`, но клиентская сторона в S3Worker.Core БЕЗ ссылки на OwnS3.Protocol). `OwnS3NodeClient`: health — именованный HttpClient; bucket — `AmazonS3Client` per-node (`AmazonS3Config{ServiceURL="http://host:port", ForcePathStyle=true}` + BasicAWSCredentials root-пары; таймауты клиента 7.5 с, retries off — ретраи делает тик); usage — HttpClient + SigV4GetSigner, парсинг camelCase полей.

**Выход:** клиент ноды закрывает health/bucket/usage-грани ownS3 с таймаут-контрактами поллинг-инварианта.

**Проверка:** юниты Task 8 зелёные; сборка чистая.

- [ ] Шаг 8.4. Коммит: `git add -A && git commit -m "t32: IOwnS3NodeClient — health/bucket/usage ownS3-ноды (AWSSDK + SigV4 /admin/storage, таймауты 7.5 c)"`

---

### Task 9: SecretProfileStore (env-профили S3W_NODE_SECRETS__*) + S3ProcessCommon

**Spec:** §4 (Provisioning: SecretProfileStore), §8 (секреты), §12 фаза 3/6; arch/23 §6, arch/24 §4.

**Вход:** Task 3 (+5 для типов). Прецедент формата env: `OWNS3_ACCESS_KEYS__<i>__*` (deploy/docker-compose.yml сервис owns3).

**Files:**
- Create: `src/S3Worker.Provisioning/SecretProfileStore.cs`, `Processes/S3ProcessCommon.cs`
- Modify: `src/S3Worker.App/Program.cs` (DI SecretProfileStore + fail-fast неполных пар)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/{SecretProfileStoreTests,S3ProcessCommonTests}.cs`

**Interfaces:**
- Produces:

```csharp
namespace S3Worker.Provisioning;
public sealed record OwnS3AccessKey(string AccessKey, string SecretKey, string Policy); // read-write|read-only
public sealed record OwnS3NodeSecrets(string RootUser, string RootPassword,
    IReadOnlyList<OwnS3AccessKey> AccessKeys)
{ public IReadOnlyDictionary<string, string> ToContainerEnv(); }  // OWNS3_ROOT_*, OWNS3_ACCESS_KEYS__<i>__*
public sealed class SecretProfileStore(Func<string?, string?>? getenv = null)
{
    public IReadOnlyList<string> ProfileNames { get; }             // для 422-перечня — только имена
    public bool TryGet(string profile, out OwnS3NodeSecrets? secrets, out string? diagnostic); // diagnostic — БЕЗ значений
    public IReadOnlyList<string> ValidateEnvPairs();               // полупары → fail-fast старта
}
```
- `S3ProcessCommon` (порт `ValkeyWorker.Provisioning/ProcessCommon.cs`): ключи `ConfigKey/EndpointsKey/NodeStateKey/PlacedOnKey/UsageKey/PlacementKey/ResourcesKey(cluster,node)`, `PortAllocKey/RotationKey(cluster)` (`/s3worker/…`), `ReadNodeStateAsync/WriteNodeStateAsync` (failover), `EnsureClaimed`, `ParseResources` (cpu decimal; mem/disk `<n>Gi` → bytes; disk — декларация), `ParsePortAlloc/SerializePortAlloc` (формат `{"node<k>":{"host":"h","s3":<port>}}`), `S3ProvisioningOptions`-record (`PortRangeFrom/To, NodeBootSec, NodeDeadSec, HealthIntervalSec, AdvertisedHost, OwnS3Image, Bucket`).

**Действие:**

- [ ] Шаг 9.1. Юниты SecretProfileStore (AAA; env через getenv-инъекцию):
  - полный профиль (`ROOT_USER`+`ROOT_PASSWORD`+ключи 0..1, policy read-write/read-only) → TryGet ok; `ToContainerEnv()` = `OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD/OWNS3_ACCESS_KEYS__0__{ACCESSKEY,SECRETKEY,POLICY}/__1__…`;
  - профиль только с root-парой (без access keys) → валиден (ключи опциональны);
  - отсутствие профиля → TryGet false, diagnostic «профиль не найден; доступны: …» (имена, без значений);
  - полупара (есть `__ROOT_USER`, нет `__ROOT_PASSWORD`) → ValidateEnvPairs возвращает имя профиля → Program fail-fast;
  - access key без SECRETKEY/POLICY → профиль невалиден (diagnostic «неполный профиль», permanent P1);
  - policy вне `read-write|read-only` → невалиден;
  - `ToString()`/диагностики не содержат значений секретов (assert: diagnostic не содержит подстроку пары).
- [ ] Шаг 9.2. Юниты S3ProcessCommon: ParseResources канон/мусор; ParsePortAlloc/SerializePortAlloc roundtrip формата `{"node1":{"host":"h1","s3":18001}}`; ключи-пути точны (`/s3/clusters/c/nodes/node2/state`, `/s3worker/portalloc/c`, `/s3worker/rotations/c`).
  Run FAIL → реализация → PASS.
- [ ] Шаг 9.3. Program: DI `SecretProfileStore`; fail-fast: `foreach (var p in store.ValidateEnvPairs()) throw new ApplicationException($"S3W_NODE_SECRETS__{p}: неполная секретная пара")` (арх/24 §8 «неполные секретные пары»).

**Выход:** профили секретов читаются из env с диагностиками без утечек; общие хелперы процессов готовы.

**Проверка:** юниты зелёные; сборка чистая.

- [ ] Шаг 9.4. Коммит: `git add -A && git commit -m "t32: SecretProfileStore (S3W_NODE_SECRETS__*, fail-fast полупар, диагностики без значений) + S3ProcessCommon"`

---

### Task 10: ProvisioningProcess A (P0–P5)

**Spec:** §5 A, §12 фаза 7, arch/24 §5 A; паттерны arch/17 S5/S6.

**Вход:** Tasks 4–9 (координация, модель, драйвер, placement/portalloc, клиент, секреты). Образец: `KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` (K1-структура claim→plan→ensure→wait→commit).

**Files:**
- Create: `src/S3Worker.Provisioning/Processes/ProvisioningProcess.cs`
- Modify: `src/S3Worker.App/Program.cs` (DI), `Loops/S3ClusterProcesses.cs` (ветка Provision)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/ProvisioningProcessTests.cs`

**Interfaces:**
- Consumes: ClaimStore/WorkJournal/PortAllocLock/PortAllocIndex/IS3ClusterDriver/IOwnS3NodeClient/SecretProfileStore/S3SnapshotParser-снапшоты/S3Planning+`Shared.Core.Planning.PortAllocator`.
- Produces: `Task<Result> RunAsync(S3ClusterSnapshot snap, CancellationToken ct)` — исполняет заявку до Active или безопасного отказа; `ProcessOutcome`-семантика как у valkey (InProgress/Done — через journal-фазу).

**Действие (фазы — один в один arch/24 §5 A):**

- [ ] Шаг 10.1. Юнит-скелет тестов (AAA; фейки: InMemoryEtcdGateway-обёртка на `IEtcdGateway`-моках по образцу `ProvisioningProcessTests` valkey/kafka — `Fakes.cs` юнит-проекта):
  - P0: клэйм не взяли → никаких записей; взяли → journal `{"op":"provision","phase":"started"}`;
  - гонка TO_REMOVE посреди: перечитка config перед фазами — state стал TO_REMOVE → безопасная остановка (journal `aborted-state-changed`, без docker-мутаций);
  - P1: ноль кандидатов placement → transient Failed + journal с перечнем кандидатов (пустой); несколько кандидатов → journal-запись с перечнем кандидатов, выбран least-loaded (tie-break по имени — детерминизм, arch/23 §4); порт-клэйм не взяли → journal `waiting-portalloc-lock`, без аллокаций; неполный профиль → permanent Failed c диагностикой (без значений);
  - P1-план: селектор 2 нод → 2 размещения → `placed_on` записан; portalloc закреплён (`{"node1":{"host":"h1","s3":P1},"node2":…}`), повторный прогон — порты стабильны (идемпотентность);
  - P2: EnsureNodeAsync вызван с volume-спекой (env `OWNS3_*`+`OWNS3_DATA_DIR=/data`, порты 9000→host, лимиты); health не отвечает в бюджет → InProgress (ретрай тиком), без eternal-блока (бюджет NodeBootSec);
  - P3: bucket есть → EnsureBucket skip; нет → создан root-кредом; ошибка 5xx → transient;
  - P5: `endpoints = "h1:18001,h2:18002"` (AdvertisedHost при заданном, иначе host); config переписан каноническим JSON без `state` по txn compare mod_revision; journal `done`.
  Run FAIL.
- [ ] Шаг 10.2. Реализация `ProvisioningProcess` (ctor: gateway, endpoints, driver, claims, journal, portAllocLock, portAllocIndex, placement-данные, secrets, nodeClient, options, snapshotDelegate, progress; порт структуры KafkaWorker `ProvisioningProcess`: `RunAsync → P0 claim/journal/перечитка → PlanAsync (placement всех нод + PortAllocator под locks/portalloc; занятость = GetBusyPortsAsync ∪ ForeignAllocatedPortsAsync; валидация профиля; journal phase=planned с перечнем кандидатов каждой ноды — arch/23 §4 требует запись при выборе из нескольких, при нуле — пустой перечень в transient-фейле) → EnsureNodesAsync (volume+контейнер+state=PROVISIONING) → AwaitBootAsync (health IOwnS3NodeClient, бюджет NodeBootSec, ILoopProgress-пульс) → EnsureBucketsAsync (P3) → P4 no-op (комментарий-маркер t35) → CommitAsync (endpoints put + config txn mod_revision, канонический JSON arch/23 §2.1 Active-пример) → FinishAsync done`). Канонический JSON config: `{"platform":"owns3","nodes":N,"secret_profile":"<p>","created_unix":T}` (без state, поля в порядке канона).
- [ ] Шаг 10.3. Run PASS: `dotnet test src/tests/S3Worker.UnitTests -c Release --filter FullyQualifiedName~ProvisioningProcessTests`.
- [ ] Шаг 10.4. Подключение в `S3ClusterProcesses` (ветка `Provision`) + Program DI.

**Выход:** заявка NOT_INITIALIZED доезжает до Active идемпотентно, с journal-фазами и takeover-безопасностью.

**Проверка:** юниты зелёные; сборка чистая.

- [ ] Шаг 10.5. Коммит: `git add -A && git commit -m "t32: ProvisioningProcess P0–P5 (placement/portalloc/нода/bucket/endpoints/снятие state, идемпотентность по факту)"`

---

### Task 11: NodeSupervisor C (health/пересоздание/UNREACHABLE/usage/resources-rolling)

**Spec:** §5 C, §12 фаза 7, arch/24 §5 C; arch/17 S7.

**Вход:** Task 10 (+все его зависимости). Образец: `ValkeyWorker.Provisioning/Processes/NodeSupervisor.cs`.

**Files:**
- Create: `src/S3Worker.Provisioning/Processes/NodeSupervisor.cs`
- Modify: Program DI, S3ClusterProcesses (Active-ветка)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/NodeSupervisorTests.cs`

**Interfaces:**
- Produces: `Task<Result> RunAsync(S3ClusterSnapshot snap, CancellationToken ct)` — надзорный проход; ничего не возвращает кроме исхода.

**Действие:**

- [ ] Шаг 11.1. Юниты (AAA; фиксированный TimeProvider — `FixedTimeProvider` юнит-проекта):
  - контейнер снесён (InspectNodeAsync → null — положительное свидетельство S7) → EnsureNode на том же `placed_on` (volume жив — EnsureNodeAsync не трогает volume) + state=PROVISIONING; **одно пересоздание за тик** (вторая снесённая нода — следующим тиком);
  - health ок + state=PROVISIONING → RUNNING;
  - health молчит дольше NodeDeadSec (последний успех по state/времени probes; трек в journal `WriteSupervisionAsync`-поле — S6: фазовые записи трек не затирают) → UNREACHABLE; деструктива НЕТ;
  - слепой inspect (драйвер Failed) → никаких действий;
  - usage: ключа нет или `updated_unix` старше HealthIntervalSec → GetUsageAsync → put `nodes/node<k>/usage` `{"used_bytes":N,"total_bytes":N,"updated_unix":T}` (snake_case — канон arch/23 §2.1!); usage-ошибка — транзиент, тик не фейлит (риск §15.3);
  - лимиты контейнера ≠ resources.cpu/mem → пересоздание с новыми лимитами (rolling-мутация PUT resources) — в бюджет «одно пересоздание за тик»; disk — не сверяется (декларация);
  - ноды NOT_INITIALIZED/TO_REMOVE/REMOVING надзор не трогает.
  Run FAIL.
- [ ] Шаг 11.2. Реализация: порт `ValkeyWorker NodeSupervisor` с заменами: проба `IOwnS3NodeClient.HealthAsync(host, port)`; unreachable-трек — journal supervision (каркас `WriteSupervisionAsync/ReadSupervisionStateAsync` Shared.Etcd уже несёт); usage-сбор с HealthIntervalSec-гейтом; ресурсы-автоконверге через `NodeResourcesAsync` (docker-факт) vs `ParseResources(snap)`; «одно пересоздание за тик» — счётчик в проходе.
- [ ] Шаг 11.3. Run PASS: `dotnet test src/tests/S3Worker.UnitTests -c Release --filter FullyQualifiedName~NodeSupervisorTests`; подключение в Active-ветку (первой — arch/24 §5).

**Выход:** надзор: самовосстановление снесённых, маркировка молчащих, периодический usage, rolling-ресурсы.

**Проверка:** юниты зелёные.

- [ ] Шаг 11.4. Коммит: `git add -A && git commit -m "t32: NodeSupervisor — health/пересоздание (S7, одно за тик)/UNREACHABLE/usage (snake_case arch/23)/rolling-ресурсы"`

---

### Task 12: Arch-правка arch/23 §2 (актуализация config.nodes) + DeprovisioningProcess B + AddNodeProcess D + RemoveNodeProcess E

**Spec:** §2.2 (arch-правка — первым шагом, до кода D/E), §5 B/D/E (D/E — RMW endpoints + актуализация `config.nodes`, txn по `mod_revision`), §12 фаза 7, arch/24 §5 B/D/E.

**Вход:** Tasks 10–11. Образцы: `ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs`, `KafkaWorker.Provisioning/Processes/AddBrokerProcess/RemoveBrokerProcess.cs`.

**Files:**
- Modify: `arch/23-s3-clusters.md` (§2 — arch-правка первого шага)
- Create: `src/S3Worker.Provisioning/Processes/DeprovisioningProcess.cs`, `Processes/AddNodeProcess.cs`, `Processes/RemoveNodeProcess.cs`
- Modify: Program DI, S3ClusterProcesses
- Test: `src/tests/S3Worker.UnitTests/Provisioning/{DeprovisioningProcessTests,AddNodeProcessTests,RemoveNodeProcessTests}.cs`

**Interfaces:**
- Produces: `RunAsync(snap, ct)` у каждого; завершающее окно D/E — RMW `endpoints` + актуализация `config.nodes` до фактического состава (txn по `mod_revision`, идемпотентно по факту — spec §2.2).

**Действие:**

- [ ] Шаг 12.1. **Arch-правка** `arch/23-s3-clusters.md` §2 (arch-first, до кода D/E; spec §2.2):
  - графа «Пишет» ключа `config`: формулировку воркера дополнить — «воркер (снимает `state` после инициализации, актуализирует `nodes` при изменении состава (D/E), txn по `mod_revision`)»;
  - примечание поля `nodes` той же правкой согласовать: «плановое число нод при создании (заявка панели), после D/E — поддерживается воркером равным фактическому составу» (предел 1..9 и рекомендация 2–3 не меняются).
  Проверка: `git diff arch/23-s3-clusters.md` — точечная правка §2; текст не противоречит §8 (координация) и arch/24 §3.
- [ ] Шаг 12.2. Юниты Deprovisioning (AAA):
  - X0 claim+journal `op=deprovision`; X1: всем нодам `REMOVING`;
  - X2: docker-снос на всех хостах (ListNodeObjects → Remove контейнеры и тома; 404=ок) + del `/s3worker/portalloc/<C>`; порядок «сначала docker, потом etcd» (assert: del etcd-префикса только после успешного docker-шага; docker-фаза Failed → etcd не тронут, ретрай тиком);
  - X3: del `--prefix /s3/clusters/<C>/` + del `/s3worker/{work,portalloc,rotations}/<C>`; journal done; клэйм снят явно.
- [ ] Шаг 12.3. Юниты AddNode (AAA): заявка `nodes/node<max+1>/state=NOT_INITIALIZED` у Active-кластера → mini-A (placement+portalloc довыделение под locks/portalloc+volume+контейнер+bucket+health→RUNNING) + завершающее окно: RMW `endpoints` (txn compare value==прочитанное; новая нода добавлена, прежние на месте) + актуализация `config.nodes` до фактического числа нод (txn по `mod_revision`, canonical JSON — spec §2.2); прочие ноды не тронуты; идемпотентность: `nodes` уже равен фактическому составу → config-txn не выполняется (no-op).
- [ ] Шаг 12.4. Юниты RemoveNode (AAA):
  - маркер TO_REMOVE → REMOVING → снос контейнера+volume по `placed_on` → del ключей ноды `{placement,resources,state,placed_on,usage}` + исключение из portalloc-записи (RMW под locks/portalloc) → завершающее окно: RMW `endpoints` + актуализация `config.nodes` до фактического числа (txn по `mod_revision`, spec §2.2; повторный прогон при актуальном `nodes` — no-op) → done;
  - гвард «последняя нода» → отказ фазы (journal, без мутаций);
  - гвард «мёртвые остальные»: живых RUNNING-нод кроме выводимой нет → отказ фазы (вывод не рвёт последнюю точку записи пула — arch/24 §5 E);
  - гварды идут ДО деструктива.
  Run FAIL → реализации (порты kafka-образцов с S3-спецификой: гварды считаются по `snap.Nodes[].State`) → PASS.
- [ ] Шаг 12.5. Подключение: Deprovision-ветка классификатора; Active-ветка: C → D → E.

**Выход:** изменение состава и полный демонтаж с чистыми etcd/docker; `config.nodes` следует фактическому составу по канону (arch/23 §2 после правки Шага 12.1).

**Проверка:** три юнит-файла зелёные; arch-дифф Шага 12.1 — единственная arch-правка домена в задаче после Tasks 1/2.

- [ ] Шаг 12.6. Коммит: `git add -A && git commit -m "t32: arch/23 §2 актуализация config.nodes (D/E, txn mod_revision) + Deprovision X0–X3 (docker→etcd) + AddNode D + RemoveNode E (гварды, RMW endpoints + nodes)"`

---

### Task 13: SecretRotator (rolling-ротация) — ветка заявки rotations

**Spec:** §5 «Ротация секретов», §8, §12 фаза 7; arch/23 §6, arch/24 §4.

**Вход:** Task 11 (EnsureNode-механика надзора).

**Files:**
- Create: `src/S3Worker.Provisioning/Processes/SecretRotator.cs`
- Modify: Program DI, S3ClusterProcesses (живая заявка ротации → ЭКСКЛЮЗИВНАЯ ветка тика — надзор/D/E этим тиком не идут: дисциплина «одно пересоздание за тик»)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/SecretRotatorTests.cs`

**Interfaces:**
- Produces: `Task<Result> RunAsync(S3ClusterSnapshot snap, CancellationToken ct)`; заявка `/s3worker/rotations/<C>` `{"requested_unix":T,"requested_by":"…"}`.

**Действие:**

- [ ] Шаг 13.1. Юниты (AAA):
  - заявки нет → no-op (ветка не активна);
  - заявка жива + профиль невалиден → journal-отказ (постоянный, без пересозданий), заявка жива (оператор чинит env);
  - заявка жива: первая нода с env ≠ текущего профиля (сверка `NodeEnvAsync` по OWNS3_ROOT_USER и составу ключей — БЕЗ значений секретов) → RemoveNode+EnsureNode (volume не трогается) с env профиля; **одна нода за тик**;
  - все ноды приведены → del заявки (одна txn) + journal done;
  - профиль сменился у кластера — rotator читает `config.secret_profile` свежим снапшотом (несколько последовательных ротаций работают);
  - значения секретов не появляются в journal-фазах (assert по записанным фазам).
- [ ] Шаг 13.2. Реализация: цикл по нодам кластера (Ordinal): env-сверка (дешёвая: наличие OWNS3_ROOT_USER == профилю, число ключей == профилю; полная идентичность значений не требуется — сверка по пользователю/составу), пересоздание через EnsureNode с `ToContainerEnv()` профиля; state=PROVISIONING → следующий тик надзора вернёт RUNNING (ротация не ждёт health — но после последней ноды заявка гасится; UNREACHABLE-ну — надзор лечит).
  Wait-семантика: пересозданная нода должна ожить до перехода к следующей (rolling, не параллельный снос): фаза «ждать health ноды k» — через state: нода не RUNNING → тик НЕ пересоздаёт следующую (только ожидание; таймаута нет — надзор в следующих тиках восстановит/UNREACHABLE). Зафиксировать юнитом: две ноды, обе с чужим env → тик 1 пересоздал node1; тик 2 (node1 не RUNNING) — НЕ трогает node2.
  Run PASS.
- [ ] Шаг 13.3. Подключение: в `S3ClusterProcesses` Active-ветка начинается с проверки заявки ротации (Range `/s3worker/rotations/<C>`): живая → только SecretRotator.

**Выход:** ротация секретов — rolling по одной ноде за тик с доводкой health, заявка гасится по завершении.

**Проверка:** юниты зелёные.

- [ ] Шаг 13.4. Коммит: `git add -A && git commit -m "t32: SecretRotator — rolling-пересоздание нод с env профиля (одна за тик, volume жив), del заявки по завершении"`

---

### Task 14: HTTP API — 7 эндпоинтов + seed + валидации

**Spec:** §6 (таблица + валидации), §12 фаза 8, arch/24 §1.1.

**Вход:** Tasks 9 (SecretProfileStore для 422), 4 (каркас ApiModule). Образец: `ValkeyWorker.App/Api/*`.

**Files:**
- Create: `src/S3Worker.App/Api/ApiModule.cs`, `Api/Operations/S3Api.cs` (DTO+исключения+валидатор), `Api/Operations/CreateClusterHandler.cs`, `DeleteClusterHandler.cs`, `AddNodeHandler.cs`, `DeleteNodeHandler.cs`, `UpdateNodeResourcesHandler.cs`, `PostRotationHandler.cs`, `SyncProbeHandler.cs`, `SeedDemoHandler.cs`, `Api/S3ApiMetrics.cs`
- Modify: Program.cs (DI хендлеров + S3ApiMetrics, MapWorkerApi)
- Test: `src/tests/S3Worker.UnitTests/Api/S3ValidationTests.cs`, `src/tests/S3Worker.UnitTests/Api/S3ApiMetricsTests.cs`

**Interfaces:**
- Produces (таблица §6 точно):

| Метод+путь | Коды |
|---|---|
| `POST /api/s3/clusters` | 201 / 409 / 400 / 422 / 503 |
| `DELETE /api/s3/clusters/{c}` | 202 (перевод TO_REMOVE) / 204 (already) / 404 / 503 |
| `POST /api/s3/clusters/{c}/nodes` | 201 / 400 / 409 (не Active / nodes≥9) / 404 / 503 |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | 202 / 204 (already TO_REMOVE) / 404 / 409 (последняя нода) / 503 |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | 200 / 400 / 404 / 503 |
| `POST /api/s3/clusters/{c}/rotations` | 201 / 200 (повтор — идемпотентен) / 404 / 503 |
| `POST /api/s3/clusters/{c}/sync-probe` | 202 always (no-op заглушка t33) |
| `POST /api/seed/demo` | за `EnableSeedEndpoint=false`; 201/200 идемпотентно / 404 (флаг off) |

- `S3ApiMetrics.Track(string operation, int statusCode)` — метрика `s3worker_api_operations_total{operation,code}` (Шаг 14.4; инкремент — map-обработчики ApiModule, проверка — Task 15.2).
- DTO (camelCase JSON): `CreateS3ClusterRequest { name, platform, nodes, secretProfile, placements: [{ dockerHost?, swarmNode?, swarmLabels? }], resources: { cpu, memGi, diskGi } }`; `AddNodeRequest { placement, resources }`; `UpdateResourcesRequest { cpu?, memGi?, diskGi? }`.
- Исключения: `S3ValidationException(errors)` → 400; `S3SecretProfileUnavailableException(names)` → 422; `S3ClusterAlreadyExistsException`/`S3ClusterNotActiveException`/`S3RotationAlreadyRequestedException`/`S3LastNodeException` → 409; `S3ClusterNotFoundException`/`S3NodeNotFoundException` → 404; `EtcdWriteUnavailableException` → 503.
- Валидатор `S3ClusterValidator`: name `^[a-z][a-z0-9_]{0,62}$`; platform == `"owns3"`; nodes 1..9; placements.Length == nodes, каждый непустой (dockerHost|swarmNode|swarmLabels — минимум один) — 400; cpu 0.01..64 (decimal), memGi/diskGi 1..65536 — 400; secretProfile непустое → в SecretProfileStore (422 с перечнем ProfileNames, без значений).

**Действие:**

- [ ] Шаг 14.1. Юниты валидатора (AAA): каждый лимит — граничные кейсы (name 63 символа ок/64 нет; platform "minio" → 400; nodes 1/9 ок, 0/10 нет; cpu 0.01/64 ок, 0.005/65 нет; memGi 1/65536; placements 2 из nodes=3 → 400; пустой placement → 400; профиль отсутствует → 422-исключение с именами). Run FAIL → `S3Api.cs` → PASS.
- [ ] Шаг 14.2. Хендлеры (порт `CreateClusterHandler` valkey + kafka-паттерны):
  - Create: txn `[compare version(/s3/clusters/<C>/config)==0][put config][put nodes/node<k>/{placement,resources,state=NOT_INITIALIZED} ×N]` (одна txn; config-значение — канонический JSON §2.1 заявки: `{"platform":"owns3","nodes":N,"secret_profile":"<p>","created_unix":T,"state":"NOT_INITIALIZED"}`); проигрыш compare → 409; сбой midway → компенсация `del --prefix /s3/clusters/<C>/` (§9.1-образец adminpanel/02); создание клэйм-txn'ом НЕ берёт lease — это заявка (клэйм возьмёт процесс).
  - DeleteCluster: GET config → нет → 404; state==TO_REMOVE → 204; иначе RMW txn `[compare mod_revision==r][put config канонический JSON+"state":"TO_REMOVE"]` → 202.
  - AddNode: гварды (config есть/state нет → иначе 409; фактических нод <9 → иначе 409); put node<max+1> ключей (txn compare version(новых ключей)==0) → 201.
  - DeleteNode: нода есть; already TO_REMOVE → 204; нод >1 → иначе 409 (S3LastNodeException); put state=TO_REMOVE → 202.
  - UpdateResources: RMW resources-ключа (validate cpu/memGi/diskGi) → 200 (применит надзор C rolling'ом; disk — декларация).
  - PostRotation: txn `[compare version(/s3worker/rotations/<C>)==0][put {"requested_unix":T,"requested_by":<X-Requested-By|"api">}]`; повтор → 200 идемпотентно; кластера нет → 404.
  - SyncProbe: `Results.Accepted()` безусловно (заглушка t33; комментарий-маркер).
  - SeedDemo: флаг off → 404; on → создать кластер `demo` (nodes=2, профиль `backups-main`, placement `{"docker_host":"local"}×2`, resources cpu=1/memGi=1/diskGi=10) через CreateClusterHandler-логику; живой demo → 200.
- [ ] Шаг 14.3. `ApiModule.MapWorkerApi()` — маппинг путей/кодов по таблице (порт ApiModule valkey: ProblemDetails + `errors.<field>`-массивы для 400; `RequestedBy(http)` — заголовок X-Requested-By); каждый эндпоинт в финале инкрементирует счётчик Шага 14.4 фактическим кодом ответа.
- [ ] Шаг 14.4. **Счётчик операций API** (spec §9 «операции API» — у ValkeyWorker его нет, портом каркаса не приносится): `S3ApiMetrics` — обёртка над `Counter<long>` Meter `S3Worker`: `Track(string operation, int statusCode)` → `s3worker_api_operations_total{operation,code}`; operation ∈ `create-cluster | delete-cluster | add-node | delete-node | update-node-resources | post-rotation | sync-probe | seed-demo`; регистрация в Program (`builder.Services.AddSingleton(sp => new S3ApiMetrics(sp.GetRequiredService<Meter>(), "s3worker_api_operations_total"))`); инкремент в map-обработчиках ApiModule (успех и ошибки — один и тот же хук в конце обработки). Экспозиция — общий `/metrics` (Task 3, AddAppMetrics). Юнит: `S3ApiMetricsTests` — инкремент фиксируется MeterListener'ом с тегами operation/code (паттерн Meter-проверок — `src/tests/Shared.Metrics.UnitTests`); проверка в контуре — Task 15.2 (метрика в `/metrics` после серии вызовов).
- [ ] Шаг 14.5. Сборка+юниты: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/S3Worker.UnitTests -c Release` — зелёные.

**Выход:** все мутации S3-домена принимаются mTLS-гранью с каноническими кодами и компенсациями.

**Проверка:** юниты валидаций зелёные; сборка чистая.

- [ ] Шаг 14.6. Коммит: `git add -A && git commit -m "t32: HTTP API S3Worker — 7 эндпоинтов (клэйм-txn/компенсация/гварды), валидации 400/422, sync-probe 202-заглушка, seed за флагом, счётчик s3worker_api_operations_total"`

---

### Task 15: Интеграционные тесты — API (mTLS/TestPki), реальный контур процессов

**Spec:** §11 интеграционные, §12 фаза 7–8, §14.4.

**Вход:** Tasks 4, 14. Образцы: `ValkeyWorker.IntegrationTests/Api/{TestPki,ValkeyApiFactory,ClusterMutationsApiTests,MtlsApiTests}.cs`, `Valkey/{ValkeyClusterFixture,ProvisioningTests,SupervisionTests}.cs`.

**Files:**
- Create: `src/tests/S3Worker.IntegrationTests/Api/TestPki.cs` (порт), `Api/S3ApiFactory.cs`, `Api/ClusterMutationsApiTests.cs`, `Api/MtlsApiTests.cs`, `S3/S3ClusterFixture.cs`, `S3/ProvisioningTests.cs`, `S3/SupervisionTests.cs`, `S3/FreePortWindow.cs` (порт)
- Modify: — ( fixture уже в Task 4: S3EtcdFixture)

**Interfaces:**
- Consumes: Program (public partial), S3EtcdFixture; docker-гейт `PGW_TEST_DOCKER=1` для `S3/`-классов.

**Действие:**

- [ ] Шаг 15.1. `TestPki` + `S3ApiFactory` (порты valkey: собственная CA+server+client серты; WebApplicationFactory: env etcd → S3EtcdFixture-endpoint, `AllowInsecureHttp=false`, AdvertiseUrl https://localhost). `MtlsApiTests`: без клиентского серта — TLS-отказ; с валидным — 2xx/4xx по маршруту.
- [ ] Шаг 15.2. `ClusterMutationsApiTests` (AAA; против S3EtcdFixture): create 201 → ключи `/s3/clusters/<C>/config`(state=NOT_INITIALIZED)+nodes×3 — точные значения канона §2.1; повтор create → 409; невалидные name/platform/nodes/placements/resources → 400 (+errors.<field>); несуществующий профиль → 422 с перечнем; delete → 202 + state=TO_REMOVE; повтор delete → 204; 404 на чужое имя; add-node 201 (node<max+1>), при nodes=9 → 409; delete-node последней → 409; rotations → 201, повтор → 200; sync-probe → 202 и ключи `connectivity`/`sync` НЕ появились (Range-ассерт); компенсация: инъекция сбоя пакета PUT (мок-gateway/битый endpoint) → префикс кластера пуст (del --prefix сработал); после серии вызовов `GET /metrics` (mTLS-клиентом TestPki) содержит `s3worker_api_operations_total{operation="create-cluster",code="201"}` ≥ 1 (Task 14.4).
- [ ] Шаг 15.3. Реальный контур `S3/` (гейт `PGW_TEST_DOCKER=1`, Skip иначе — паттерн Valkey/классов): `S3ClusterFixture` — etcd-testcontainer + docker-движок хоста + сборка образа `owns3:e2e-it` (publish OwnS3.App на хосте → docker build -f docker/OwnS3.Dockerfile узким контекстом — Образец OwnS3.IntegrationTests/E2e; кэш docker — образ пересобирается при изменениях) + `FreePortWindow`; кластеры `s3w-t32it-<guid>`; teardown: del своего etcd-префикса + docker rm свои контейнеры/тома (фильтр по имени) при любом исходе. Тесты (AAA): Provisioning — заявка напрямую в etcd (как панель через API) → RunAsync процессов циклом тиков ≤ NodeBootSec → config без state, ноды RUNNING, endpoints, контейнер+volume существуют, bucket существует (S3-клиентом), `/healthz` ноды 200; Supervision — `docker rm -f` контейнера → тик надзора → пересоздан на том же placed_on, volume сохранён (метка-объект в bucket жива), usage-ключ обновился.
- [ ] Шаг 15.4. Прогоны (каждая серия — зачистка после: контейнеры/тома `s3w-*`, сети не создаём):

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.Api"`
Expected: PASS.
Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.S3"`
Expected: PASS; `docker ps -a --filter name=s3w- -q | wc -l` → 0, `docker volume ls -q --filter name=s3w- | wc -l` → 0.

**Выход:** API-контур и процессы подтверждены против реальных etcd/docker/ownS3.

**Проверка:** обе команды зелёные + чистота docker после серий.

- [ ] Шаг 15.5. Коммит: `git add -A && git commit -m "t32: интеграционные S3Worker — mTLS-API/TestPki (клэйм-txn, компенсация, 400/409/422) + реальный контур процессов (docker-гейт, зачистка)"`

---

### Task 16: Поставка — Dockerfile, deploy compose ×2, .env.example, gen.sh

**Spec:** §4 (поставка), §10 deploy, §12 фаза 9.

**Вход:** Tasks 3–14 (воркер собирается); образцы: `docker/OwnS3.Dockerfile` (publish-COPY-паттерн), `deploy/docker-compose.yml` (анкоры), `deploy/tls/gen.sh`.

**Files:**
- Create: `docker/S3Worker.Dockerfile`
- Modify: `deploy/docker-compose.yml`, `deploy/.env.example`, `deploy/tls/gen.sh`

**Interfaces:**
- Produces: образ `s3worker:dev` (publish на хосте + compose build с `ARG S3W_PUBLISH_DIR: artifacts/s3worker/publish`); сервисы `s3worker`/`s3worker-2`; env-профиль демо в `.env.example`; SAN серверного серта покрывает `s3worker`/`s3worker-2`.

**Действие:**

- [ ] Шаг 16.1. `docker/S3Worker.Dockerfile` — по канону E2E-образов (структура OwnS3.Dockerfile + healthcheck ValkeyWorker.Dockerfile):

```dockerfile
# syntax=docker/dockerfile:1
# Образ S3Worker (arch/24 §8): dotnet publish НА ХОСТЕ, в контейнер — ТОЛЬКО
# publish-вывод (runtime-слой; канон E2E-образов AGENTS.md — без sdk-стадии).
# CLI: dotnet publish src/S3Worker.App -c Release -o artifacts/s3worker/publish
#      docker build -f docker/S3Worker.Dockerfile -t s3worker:dev artifacts/s3worker/publish
# Compose: context .., args { S3W_PUBLISH_DIR: artifacts/s3worker/publish }
ARG S3W_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG S3W_PUBLISH_DIR
WORKDIR /app
COPY ${S3W_PUBLISH_DIR} ./
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
# /healthz за mTLS (arch/24 §1.1): клиентская пара healthcheck из TLS-пакета.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "S3Worker.App.dll"]
```

- [ ] Шаг 16.2. `deploy/docker-compose.yml` — по образцу valkeyworker (анкоры + volumes pgw-api-tls общий):

```yaml
# S3Worker (arch/24 §2/§8): docker.sock, том снапшотов P12, общий TLS-volume
# pgw-api-tls (server-серт kfw/vwk-ряда с SAN s3worker*; до ключа
# /workers/api_tls/s3worker от панели t34 — env-фоллбек S3W_API_TLS_*).
x-s3worker-env: &s3worker-env
  S3Worker__Etcd__Endpoints__0: ${S3W_ETCD_ENDPOINT_0:-http://host.docker.internal:2379}
  S3Worker__Etcd__Endpoints__1: ${S3W_ETCD_ENDPOINT_1:-http://host.docker.internal:2381}
  S3Worker__Etcd__Endpoints__2: ${S3W_ETCD_ENDPOINT_2:-http://host.docker.internal:2383}
  S3Worker__Docker__AdvertisedHost: ${S3W_ADVERTISED_HOST:-host.docker.internal}
  S3Worker__Bucket: ${S3W_BUCKET:?задайте bucket per-install}
  S3Worker__Api__EnableSeedEndpoint: ${S3W_API_ENABLE_SEED:-false}
  S3W_API_TLS_CERT_PATH: /tls/server.crt
  S3W_API_TLS_KEY_PATH: /tls/server.key
  S3W_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
  # Демо-профиль секретов нод (arch/23 §6): в .env; значения НЕ в git.
  S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER:?задайте секрет}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_PASSWORD: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_PASSWORD:?задайте секрет}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__ACCESSKEY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__ACCESSKEY:-}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__SECRETKEY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__SECRETKEY:-}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__POLICY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__POLICY:-}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__ACCESSKEY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__ACCESSKEY:-}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__SECRETKEY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__SECRETKEY:-}
  S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__POLICY: ${S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__POLICY:-}

x-s3worker-base: &s3worker-base
  build:
    context: ..
    dockerfile: docker/S3Worker.Dockerfile
    args:
      S3W_PUBLISH_DIR: artifacts/s3worker/publish
  image: s3worker:dev
  restart: unless-stopped
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    - s3w-snapshots:/snapshots
    - pgw-api-tls:/tls:ro
  extra_hosts:
    - "local:host-gateway"
    - "host.docker.internal:host-gateway"
```
сервисы: `s3worker` (порт `"${S3W_API_HOST_PORT:-8086}:8080"` — продолжение ряда 8080/8081/8082, AdvertiseUrl `https://host.docker.internal:${S3W_API_HOST_PORT:-8086}`), `s3worker-2` (`"${S3W_API_HOST_PORT2:-8089}:8080"`, AdvertiseUrl по порту); volume `s3w-snapshots` в volumes-список.

- [ ] Шаг 16.3. `deploy/.env.example` — секция S3Worker (по тону соседних секций; вкл. `S3W_BUCKET`, `S3W_API_HOST_PORT(_2)`, `S3W_ETCD_ENDPOINT_0..2`, `S3W_ADVERTISED_HOST`, `S3W_API_ENABLE_SEED`, полный демо-профиль `S3W_NODE_SECRETS__BACKUPS-MAIN__*` со стендовыми примерами и комментарием «прод — per-install секрет; значения никогда не в git/etcd»).

- [ ] Шаг 16.4. `deploy/tls/gen.sh`: расширить `SERVER_SAN` — добавить `DNS:s3worker,DNS:s3worker-2` и маркер регенерации (grep `DNS:s3worker` по образцу существующего блока kafkaworker-2); строку «✓ TLS-пакет …» дополнить.

- [ ] Шаг 16.5. Проверка сборки образа:

Run: `dotnet publish src/S3Worker.App/S3Worker.App.csproj -c Release -o artifacts/s3worker/publish --nologo && docker build -q -f docker/S3Worker.Dockerfile -t s3worker:dev artifacts/s3worker/publish`
Expected: оба шага успешны; `docker image inspect s3worker:dev` — размер runtime-слоя (без sdk).

**Выход:** поставка s3worker:dev ×2 инстанса контурная; секреты/этcd/advertise — через env.

**Проверка:** Шаг 16.5 зелёный; `docker compose -f deploy/docker-compose.yml config` валидирует анкоры (без подъёма).

- [ ] Шаг 16.6. Коммит: `git add docker/S3Worker.Dockerfile deploy/ && git commit -m "t32: поставка S3Worker — publish-COPY Dockerfile, deploy compose s3worker×2 (8086/8089, общий TLS-volume), .env.example, gen.sh SAN"`

---

### Task 17: Стенд — contour-override, 00-up.sh, чек 75-s3worker.sh

**Spec:** §10 dev-stand/checks, §12 фаза 10, §14.10.

**Вход:** Task 16. Образцы: `deploy/docker-compose.contour.yml`, `dev-stand/adminpanel/checks/00-up.sh`, `51-valkey-api.sh`.

**Files:**
- Modify: `deploy/docker-compose.contour.yml`, `dev-stand/adminpanel/checks/00-up.sh`
- Create: `dev-stand/adminpanel/checks/75-s3worker.sh`

**Действие:**

- [ ] Шаг 17.1. `deploy/docker-compose.contour.yml`: добавить сервисы в стендовую сеть (по образцу valkeyworker): `s3worker: { networks: { default: !reset null, contour: { aliases: [s3worker] } } }`, `s3worker-2: { networks: { default: !reset null, contour: { aliases: [s3worker-2] } } }`.
- [ ] Шаг 17.2. `00-up.sh`: (а) publish-фаза с `[PHASE]`-меткой рядом с OwnS3-фазой: `dotnet publish src/S3Worker.App … -o artifacts/s3worker/publish`; (б) строка подъёма deploy-контура: `… up -d --build --force-recreate pgworker pgworker-2 owns3 s3worker s3worker-2`; (в) после готовности — healthz×2 (mTLS healthcheck-парой, порты `${S3W_API_HOST_PORT:-8086}`/`${S3W_API_HOST_PORT2:-8089}`, цикл ≤60 с как у pgworker) + ключи `/s3worker/api/` ≥2 через `ect get --prefix --keys-only`; (г) экспорт `S3W_API_HOST_PORT(_2)` в шапке по образцу PGW_*.
- [ ] Шаг 17.3. Чек `75-s3worker.sh` (порт стиля `51-valkey-api.sh`; RunTag-имена; trap-чистка; set -euo pipefail):
  - Arrange: демо-профиль секретов уже в `deploy/.env` (00-up.sh: если `S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER` нет в .env — дописать стендовые значения по образцу OWNS3_*-блока);
  - healthz/metrics: `curl -fsS --cacert … --cert healthcheck.* https://localhost:${S3W_API_HOST_PORT}/healthz` + `/metrics` содержит `s3worker_`;
  - демо-кластер через API (mTLS healthcheck-парой): `POST /api/s3/clusters {"name":"s3w75<tag>","platform":"owns3","nodes":1,"secretProfile":"backups-main","placements":[{"dockerHost":"local"}],"resources":{"cpu":0.5,"memGi":1,"diskGi":5}}` → 201 → ожидание (цикл ≤ NodeBootSec+тик-запас): etcd `config` без state, `nodes/node1/state=RUNNING`, `endpoints` формат `host:port`, `placed_on=local`;
  - валидации: невалидный name → 400; неизвестный профиль → 422; sync-probe → 202;
  - снос: `DELETE /api/s3/clusters/s3w75<tag>` → 202 → ожидание: etcd-префикс кластера пуст, `docker ps -a | grep s3w-s3w75` пуст, volume убран;
  - финал: зачистка за собой (assert чистоты) — «❌/✅»-вывод по стилю чеков.
- [ ] Шаг 17.4. Прогон чека на живом стенде:

Run: `bash dev-stand/adminpanel/checks/75-s3worker.sh`
Expected: зелёный; повторный прогон идемпотентен; после — `docker ps -a --filter name=s3w- -q | wc -l` → 0.

**Выход:** полный стенд поднимает s3worker×2; чек s3worker зелёный и самоочищается.

**Проверка:** Шаг 17.4; также `bash dev-stand/adminpanel/checks/00-up.sh` отрабатывает до конца (s3worker-фазы в журнале с `[PHASE]`).

- [ ] Шаг 17.5. Коммит: `git add deploy/docker-compose.contour.yml dev-stand/ && git commit -m "t32: стенд — s3worker×2 в контуре 00-up.sh ([PHASE] publish, healthz/api-ключи) + чек 75-s3worker (демо-кластер через API, зачистка)"`

---

### Task 18: E2E — фикстура S3E2eEnvironment + сценарии 1–3

**Spec:** §11 E2E (фикстура + сценарии 1–3), §12 фаза 11; docs/e2e-isolation.md + docs/e2e-launch.md.

**Вход:** Tasks 15–17 (стабильные процессы/поставка). Эталон: `ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs` (исполнитель читает его целиком).

**Files:**
- Create: `src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`, `E2e/S3E2eLifecycleTests.cs`
- Test: он же (сценарии в `S3E2eLifecycleTests`).

**Interfaces:**
- Produces: `S3E2eEnvironment` — `StartAsync(slug)` (одиночный) и `StartTwoAsync(slug)` (двухинстансный, Task 19): `ApiBaseUrl`, `Api1/Api2*`, `EtcdEndpoint`, `Gateway` (EtcdGateway на контейнерный etcd), `ClusterTag` (первые 8 guid), `NetName`, `ClientPem/ClientKeyPem` (mTLS-клиент), `RunDockerAsync`, `MarkFailed`, `WriteAsync(artifacts)`; сборка образов: `s3worker:e2e-<runId>` (publish S3Worker.App на хосте → `docker build -f docker/S3Worker.Dockerfile` УЗКИМ контекстом артефактов → тег per-прогон), `owns3:dev` (publish OwnS3.App → `docker build -f docker/OwnS3.Dockerfile` — свежесть каждого прогона, S8); гейт `PGW_TEST_DOCKER=1`, лазейка `PGW_TEST_E2E_NOBUILD=1` (только s3worker-образ; owns3:dev обязателен — отсутствие = fail-fast с командой сборки).

**Действие:**

- [ ] Шаг 18.1. Фикстура — порт ValkeyE2eEnvironment с заменами: имена `s3w-en-/s3w-ee-/s3w-ew(1|2)-<runId>`; TLS-пакет (CA/server/client — GenerateCa/IssueCert-порты); env воркера: `S3Worker__Etcd__Endpoints__0=http://host.docker.internal:<etcdPort>`, `S3Worker__Docker__AdvertisedHost=host.docker.internal`, `S3Worker__Api__AdvertiseUrl=https://host.docker.internal:<apiPort>`, `S3Worker__Docker__PortRange__{From,To}` из `FreePortWindow.Find()` (порт юнит-фикстуры Valkey/FreePortWindow.cs — копия), `S3Worker__Docker__Images__OwnS3=owns3:dev`, `S3Worker__Bucket=pgworker-backups`, `S3W_NODE_SECRETS__BACKUPS-MAIN__{ROOT_USER,ROOT_PASSWORD}` (+access keys), `S3Worker__Loops__ScanIntervalSec=1`, `S3Worker__Thresholds__{NodeBootSec=100,NodeDeadSec=15,HealthIntervalSec=3}`, `S3W_API_TLS_*` из пакета; bind: docker.sock + tlsDir; extraHost host-gateway; retry-3 против network-гонок; `README-cleanup.txt` (docker rm контейнеров/томов `s3w-<tag>`, сеть); телеметрия-метод: docker logs/inspect всех контейнеров окружения + списки `docker ps/volume ls` в `/tmp/pgw-e2e-artifacts-<runId>/` ДО удаления; teardown: `_failed` → стоп без удаления (MarkFailed), иначе полный демонтаж + **ассерт чистоты** (исключение при остатках: контейнеры `s3w-<runId>*`, тома `s3w-*<runId>*`, сеть, ключи etcd `/s3/`,`/s3worker/` пусты).
- [ ] Шаг 18.2. `S3E2eLifecycleTests` (AAA; `[Collection]`-сериализация по slug; каждый тест — свой контур StartAsync + await DisposeAsync):
  - **Сценарий 1 Provisioning**: `POST /api/s3/clusters` (nodes=2, placements docker_host=local ×2, profile backups-main, ресурсы минимальные) → 201; ожидание ≤ 100 c + тики: etcd `config` без `state` (канонический JSON Active-примера), обе ноды `RUNNING`, `placed_on=local`, `endpoints="host.docker.internal:P1,host.docker.internal:P2"` (формат §2.1); docker: контейнеры `s3w-<C>-node{1,2}` + volumes `-data` существуют; bucket на каждой ноде — проверка S3-клиентом (AWSSDK ListBuckets root-кредом на оба host-порта); `/healthz` нод 200.
  - **Сценарий 2 Идемпотентность**: после Active — 3 последовательных тика (сон 3×ScanInterval): значения `portalloc`/`endpoints`/`config.mod_revision` стабильны (после снятия state новых записей нет; допускается только usage/надзор), контейнеры не пересозданы (docker Id неизменен).
  - **Сценарий 3 AddNode**: node2→node3: `POST /nodes` → 201 → `RUNNING` ≤ бюджета; `endpoints` RMW: 3 адреса, прежние 2 на месте; `config.nodes` актуализирован до 3 (txn по mod_revision, spec §2.2); volume node3 существует; bucket на node3 создан.
  Маркировка `MarkFailed()` при любом ассерт-провале до DisposeAsync.
- [ ] Шаг 18.3. Прогон (после — зачистка серий по AGENTS.md):

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.E2e.S3E2eLifecycleTests"`
Expected: PASS; артефакты в /tmp/pgw-e2e-artifacts-<guid>/ (если падение — разбор по ним, перезапуск без анализа запрещён); после серии: `docker ps -a --filter name=s3w- -q | wc -l` = 0, `docker volume ls -q --filter name=s3w- | wc -l` = 0, `docker network ls | grep -c s3w-en-` = 0.

**Выход:** изолированная E2E-фикстура S3Worker + зелёные сценарии provisioning/идемпотентность/add-node.

**Проверка:** Шаг 18.3 зелёный + чистота.

- [ ] Шаг 18.4. Коммит: `git add -A src/tests/S3Worker.IntegrationTests/E2e && git commit -m "t32: E2E S3Worker — фикстура (guid-изоляция, publish-образы, телеметрия, ассерт чистоты) + сценарии provisioning/идемпотентность/add-node"`

---

### Task 19: E2E — сценарии 4–10 + мерж-гейт (полный прогон серии)

**Spec:** §11 сценарии 4–10, §12 фаза 11, §14.11 (мерж-гейт: полный E2E-прогон S3Worker на свежем Release).

**Вход:** Task 18 (фикстура). 

**Files:**
- Create: `src/tests/S3Worker.IntegrationTests/E2e/S3E2eScaleTests.cs` (сценарии 4–5), `E2e/S3E2eSupervisionTests.cs` (6), `E2e/S3E2eTakeoverTests.cs` (7), `E2e/S3E2eRotationTests.cs` (8–9), `E2e/S3E2eApiTests.cs` (10)

**Действие:**

- [ ] Шаг 19.1. **Сценарий 4 RemoveNode** (ScaleTests): кластер N=2 → `DELETE …/nodes/node2` → 202 → снос контейнера+volume node2, del ключей ноды, portalloc-запись без node2, `endpoints` = 1 адрес, `config.nodes` актуализирован до 1 (spec §2.2); последняя нода: `DELETE …/nodes/node1` → 409; докомплект: повтор delete node2 (уже нет) → 404.
- [ ] Шаг 19.2. **Сценарий 5 Deprovision** (ScaleTests): Active N=2 → `DELETE /api/s3/clusters/<C>` → 202 → X0–X3: etcd-префикс `/s3/clusters/<C>/` пуст, `/s3worker/{work,portalloc,rotations}/<C>` нет, контейнеров/томов кластера нет; повтор DELETE → 404. Ассерт чистоты фикстуры покрывает остатки.
- [ ] Шаг 19.3. **Сценарий 6 Надзор** (SupervisionTests): Active N=1 → положить тест-объект в bucket (AWSSDK put); `docker rm -f s3w-<C>-node1` → тик: контейнер пересоздан на том же `placed_on`, `PROVISIONING` → `RUNNING`, тест-объект читается (volume сохранён); `docker pause <контейнер>` → health молчит → `UNREACHABLE` по NodeDeadSec=15 (env-оверрайд фикстуры) — ждать ≤ 40 с, деструктива нет (контейнер существует, volume есть, state=UNREACHABLE); `usage`-ключ обновляется (updated_unix растёт за HealthIntervalSec=3 + 2 тика; поля used/total > 0 — Task 2-грань); `docker unpause` → `RUNNING` возвращается.
- [ ] Шаг 19.4. **Сценарий 7 Takeover** (TakeoverTests; порт ValkeyE2eTakeoverTests): `StartTwoAsync` → заявка N=2 → дождаться P2 (journal phase содержит ensure/ноды PROVISIONING, контейнер node1 есть) → kill держателя клэйма (`docker rm -f <holder>` по резолву claims.instance → api-ключ → порт → Api1|Api2): второй инстанс подхватывает ≤ TTL 15 с + тик (суммарный бюджет ≤ 40 с): процесс доигран до Active вторым (journal-фаза прогрессирует, endpoints/config финальны); двойной контроллер невозможен — после takeover клэйм один (claims-ключ содержит instance выжившего).
- [ ] Шаг 19.5. **Сценарий 8 Ротация** (RotationTests). Механика: ротация пересоздаёт ноды с env ТЕКУЩЕГО профиля кластера (значения читаются из env воркера — оператор обновляет env и перезапускает воркер, затем подаёт заявку). Две фазы теста: (а) контур с профилем `BACKUPS-MAIN` → Active N=2 → `POST /rotations` → rolling: node1 пересоздана, node2 следующим тиком; `OWNS3_ROOT_USER` контейнеров совпадает с env воркера (assert по docker-env инспекции — сравнение в коде теста, БЕЗ печати значений в лог); health обеих нод жив; заявка удалена по завершении; (б) смена значений: перезапустить контейнер воркера (`docker rm -f` + новый контейнер того же имени/сети/env, где `S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER/PASSWORD` заменены на вторую пару) → новая заявка ротации → ноды пересозданы с новыми root-кредами: GetBucketLocation НОВОЙ root-парой → 200, СТАРОЙ → 403.
- [ ] Шаг 19.6. **Сценарий 9 sync-probe** (RotationTests-файлом): `POST …/sync-probe` → 202; sleep 2×тик; Range `/s3/clusters/<C>/` — ключей `connectivity`/`sync` нет.
- [ ] Шаг 19.7. **Сценарий 10 API-валидации** (ApiTests): 400 — name/platform/nodes/placement/resources (по кейсу на каждый); 404 — кластер/нода (`add-node` на чужое имя — 404); 409 — create-повтор на живом имени и delete последней ноды кластера N=1 (гварды `nodes<9` при add-node дополнительно закрыты юнитами Task 14 и интеграционными Task 15 — подъём кластера N=9 в E2E неоправданно долог); rotation-повтор → 200; 422 — несуществующий secretProfile с перечнем имён в detail, значения секретов в теле ответа отсутствуют.
- [ ] Шаг 19.8. Полный прогон серии (мерж-гейт, свежий Release — автосборка фикстуры; до — `docker network prune -f` страховочно; после каждой серии — чистота):

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.E2e"`
Expected: ALL PASS; чистота: `docker ps -a --filter name=s3w- -q | wc -l` = 0; `docker volume ls -q --filter name=s3w- | wc -l` = 0; `docker network ls | grep -c s3w-en-` = 0; осиротевших сетей нет (`docker network ls | grep -c s3w-` только ожидаемые 0).

- [ ] Шаг 19.9. Регрессия мерж-гейта соседних доменов (код Shared.Docker тронут — Task 6): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Scale_AddEmptyShard"` → PASS (мерж-гейт AGENTS.md); юниты всех серий: `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName!~IntegrationTests"` → PASS. Зачистка после серий (контейнеры/тома/сети) — по финальной строке каждого прогона.

**Выход:** полная E2E-серия S3Worker зелёная на свежем Release; соседние домены не сломаны.

**Проверка:** Шаги 19.8–19.9 зелёные + чистота контуров.

- [ ] Шаг 19.10. Коммит: `git add -A && git commit -m "t32: E2E S3Worker — сценарии remove/deprovision/надзор/takeover/ротация/sync-probe/API-валидации; мерж-гейт серии зелёный"`

---

## Самопроверка плана (выполнена автором)

1. **Покрытие спеки по разделам:** §2.1/фаза 2 → Tasks 1–2; §2.2/фаза 7 arch-правка → Task 12 Шаг 12.1 (первый шаг задачи, до кода D/E) + формулировки D/E в Шагах 12.3–12.4 и E2E-сценариях 3/4; §4 компоненты → Tasks 3–9 (все строки таблицы проектов); §5 процессы A/B/C/D/E/ротация/F-заглушка → Tasks 10–14 (F — SyncProbeHandler 202); §6 API (7 эндпоинтов + валидации + seed-флаг) → Task 14; §7 docker-механика → Tasks 6–7 (placement/portalloc/advertised/имена/ContourNetwork; journal с перечнем кандидатов — Шаги 10.1–10.2); §8 секреты → Tasks 9, 13 (+E2E 19.5); §9 наблюдаемость → Tasks 3–4 (healthz/метрики/unknownKeys/PhaseWritten-seam) + Task 14.4 (счётчик `s3worker_api_operations_total`, проверки Task 15.2); §10 стенд → Tasks 16–17; §11 тесты → Tasks 5,7,8,9,10–12,14,15,18,19 (все перечисленные группы); §12 фазы 2–11 → Tasks 1–19 по порядку (зависимости: фаза 2 → до фазы 6/Task 8; arch-правка §2.2 → до кода D/E внутри Task 12); §13 границы — план не выходит (домен ownS3 — только §2.1-расширение; домен arch/23 — только §2.2-правка; mesh/зонд/панель/квоты — не реализуются, P4 no-op помечен); §14 критерии 1–12 → закрыты задачами (1→Tasks 1,5,12; 2→Tasks 1–2; 3→Tasks 10,18; 4→Tasks 10–13, 18–19; 5→Task 19.4; 6→Tasks 12, 18–19; 7→Tasks 2, 11, 19.3; 8→Tasks 9, 13, 19.5; 9→Tasks 14, 19.7; 10→Tasks 16–17; 11→Tasks 18–19; 12→все сборочные шаги + NodeBootSec≤100).
2. **Placeholder-скан:** шагов вида «TBD/реализовать позже/добавить обработку» нет; шаблонные файлы — порты конкретных файлов-образцов с точными заменами (это указание действия, не заглушка); все сигнатуры/форматы/кодовые точкиnamed.
3. **Консистентность типов:** `NodeAddress(Host, S3Port)` и формат portalloc `"s3"` едины (Tasks 7, 10, 12); `OwnS3NodeSecrets`/`ToContainerEnv()` — Tasks 8/9/10/13; `S3ClusterSnapshot` — Tasks 5/10–14; диапазон 18000–18999 — Options/P2-примеры/E2E; `s3worker_*`-Meter — Task 3/4; имя bucket per-install — Options.Bucket + env S3W_BUCKET; usage-ключ snake_case (etcd-канон) vs грани camelCase — маппинг в Task 11 явно.

## Открытые допущения (не вопросы — зафиксированы решениями плана, источник указан)

- Диапазон `PortRange` 18000–18999 и хост-порты API 8086/8089 — продолжение канонических рядов (примеры arch/23 §2.1: порты 18001+; ряд 8080–8085 занят pg/kfw/vwk).
- Живая заявка ротации — эксклюзивная ветка тика (надзор/D/E ждут) — дисциплина «одно пересоздание за тик» (arch/24 §5 C).
- Seed-эндпоинт реализован за флагом по паттерну pg (спека §6: «включается конфигом»); E2E и чек пользуются прямым POST, не сидом.
- E2E-тег образа s3worker — `e2e-<runId>` (per-прогон, изоляция параллельных сборок), owns3-образ — `owns3:dev` (буква спеки §11).
