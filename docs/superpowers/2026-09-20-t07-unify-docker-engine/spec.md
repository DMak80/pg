# t07-unify-docker-engine — унификация docker-движков трёх воркеров (spec)

- **Дата**: 2026-09-20
- **Roadmap**: `arch/roadmap/pgworker.md`, тег `t07-unify-docker-engine` (снимается тем же коммитом мержа в `main` — мерж-гейт, §11)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/t07-unify-docker-engine`
- **Тип**: структурный рефакторинг (дедупликация Engine-слоя docker-клиента с выравниванием семантики). Внешние контракты (etcd-ключи, HTTP API, имена docker-объектов, образы, конфигурация) не меняются; осознанные изменения поведения движка — только канон-суперсет §7.
- **Прецеденты**: t08 (`docs/superpowers/2026-09-14-t08-unify-adminpanel-duplicates/` — паттерн `Shared.*`-сборок) и t09 (`docs/superpowers/2026-09-15-t09-unify-worker-duplicates/` — дедупликация Pg↔Kfw по тому же паттерну: as-is-перенос, параметризация, union-API, вливание тестов).

## 1. Цель

Устранить три копии docker-движка (тонкого клиента Docker Engine API):
`PgWorker.Docker/Engine`, `KafkaWorker.Docker/Engine`, `ValkeyWorker.Docker/Engine`
— 3737 строк движка+моделей, ядро побайтово идентично, копии разошлись
набором методов, спеками и семантическими деталями (карта §2). Движок
выносится в новую общую сборку
**`Shared.Docker`** по паттерну t08/t09; Pg-специфика транспорта (SSH-туннели,
TLS к Engine API) становится опциями фабрики, доступными всем трём доменам.
Копии движка в трёх воркерах удаляются (мёртвый код не оставляем).

Доменные `ClusterDriver`'ы (Plain/Swarm каждого воркера) **не переносятся**:
они доменные адаптации (имена объектов `pgw-`/`kfw-`/`vwk-`, env-сборка,
per-cluster сети, TLS-volume механика, бэкапы, усыновление) — переключаются
на общий движок и остаются в своих сборках (решение пользователя §8.1).

Мерж-гейт — полный docker-E2E всех трёх доменов (Pg+Kfw+Valkey) на свежем
Release (§11).

## 2. Исходное состояние (проверено по коду worktree)

### 2.1. Карта копий

| Файл | Строк | Примечание |
|---|---|---|
| `src/PgWorker.Docker/Engine/DockerEngine.cs` | 978 | фабрика (SSH+TLS) + `DockerTlsMaterial` + `DockerHttpException` + движок |
| `src/PgWorker.Docker/Engine/IDockerEngine.cs` | 118 | интерфейс + модели (`DockerContainerInspect`, `ContainerSpec` с Tmpfs/ExtraHosts/RestartPolicy) |
| `src/PgWorker.Docker/Engine/EndpointScheme.cs` | 49 | разбор `unix://|tcp://|ssh://` (pg-only) |
| `src/PgWorker.Docker/Engine/SshHostConnection.cs` | 127 | SSH-сессия + локальный форвардинг + reconnect (pg-only) |
| `src/PgWorker.Docker/Engine/SshTunnelOptions.cs` | 80 | модель + env-биндинги `PGW_DOCKER_SSH_*` (pg-only) |
| `src/PgWorker.Docker/Engine/DockerTlsOptions.cs` | 47 | модель + env-биндинги `PGW_DOCKER_TLS_*` (pg-only) |
| `src/KafkaWorker.Docker/Engine/DockerEngine.cs` | 974 | фабрика unix/tcp (без EndpointScheme) + движок |
| `src/KafkaWorker.Docker/Engine/IDockerEngine.cs` | 128 | + kfw-методы (env/resources инспекции, `DeleteNetwork`, `VolumeExists`) |
| `src/ValkeyWorker.Docker/Engine/DockerEngine.cs` | 957 | движок (volume-archive через helper, Detach-exec с поллингом) |
| `src/ValkeyWorker.Docker/Engine/DockerEngineFactory.cs` | 48 | фабрика unix/tcp (отдельный файл) |
| `src/ValkeyWorker.Docker/Engine/IDockerEngine.cs` | 136 | + vwk-методы (volume-archive, Cmd-инспекции) |
| `src/ValkeyWorker.Docker/Engine/TarArchive.cs` | 95 | мини-ustar tar (TLS-серты в volume) |
| **Итого три копии** | **5717 строк** | движок+модели 3737 (переносятся), доменные Drivers 1980 (не переносятся) |

Drivers (доменные, НЕ переносятся, переключаются на общий движок):
`PgWorker.Docker/Drivers/ClusterDriver.cs` (931: `IClusterDriver`,
`PlainClusterDriver`, `SwarmClusterDriver`, `BackupJobsCleaner`) +
`NodeMatcher.cs` (85) + `BackupAgentNames.cs` (13);
`KafkaWorker.Docker/Drivers/ClusterDriver.cs` (493);
`ValkeyWorker.Docker/Drivers/ClusterDriver.cs` (458).

Diff-масштаб расхождения (wc по `diff`): DockerEngine kfw↔pg — 568 строк,
vwk↔kfw — 892, pg↔vwk — 985; ClusterDriver kfw↔pg — 1125 (доменная логика).

### 2.2. Ядро идентично, расхождения — по четырём осям

Ядро реализации (Json-опции, `SendAsync`/`GetAsync`/`PostAsync`, `Demux`
raw-stream, `TaskFilter`, `TryGetNodesAsync`/`TryGetTasksAsync`/
`TryGetServicePublishedPortAsync`, `CollectSwarmPortsAsync`,
`BuildContainerBody`/`BuildServiceBody` в основе, идемпотентные catch-блоки
404/409, `BusyPortsAsync`) — одно и то же во всех трёх копиях.

**(а) Фабрика/транспорт.**
pg — `EndpointScheme.Parse` + SSH-туннели (кэш `SshHostConnection` по
endpoint, reconnect с бэкоффом, `IAsyncDisposable` фабрики) + TLS
(`DockerTlsMaterial` поверх `Shared.Tls`; plaintext-tcp — warning-лог).
kfw/vwk — только `unix://`/`tcp://`, парсинг `StartsWith("unix://")` без
`EndpointScheme`; не `IAsyncDisposable`.

**(б) Набор методов интерфейса** (полная union-таблица — §4.2):
общие у всех трёх (13) + pg-only (`InspectContainerAsync`,
`GetContainerLogsAsync`), pg/kfw (`RemoveVolumeAsync` — у vwk
`DeleteVolumeAsync`; `ExecAsync` — vwk не использует exec, RESP-транспорт),
kfw-only (`VolumeExistsAsync`,
`DeleteNetworkAsync`, `InspectContainerEnvAsync`, `InspectServiceEnvAsync`,
`InspectContainerResourcesAsync`, `InspectServiceResourcesAsync`,
`InspectNodeEndpointAsync`), vwk-only (`EnsureVolumeAsync`,
`DeleteVolumeAsync`, `PutVolumeArchiveAsync`, `GetVolumeArchiveAsync`,
`InspectContainerCmdAsync`, `InspectServiceCmdAsync` + дубли kfw-инспекций).

**(в) `ContainerSpec`** — три разные: pg (Env + VolumeName/VolumeDest +
Network/Aliases + Tmpfs + ExtraHosts + RestartPolicy + Cmd со сбросом
ENTRYPOINT образа), kfw (Env + Volume + Network/Aliases + Cmd без сброса),
vwk (обязательный Cmd + Binds + RestartPolicy; без Env/volume-данных/Network).

**(г) Семантические расхождения** (решение — канон-суперсет, §7):
`StartContainerAsync` ловит 304 только pg; `CreateContainerAsync` при
404 «No such image» делает pull+retry kfw и vwk (pg — нет); `ExecAsync` при
exit!=0 включает stdout в сообщение только kfw; label-ключ контейнера/сервиса —
литерал `"pgworker"` захардкожен во ВСЕХ трёх копиях (vwk/kfw-контейнеры
маркируются «pgworker» — наследие копирования; читателей label в коде нет).

### 2.3. Доменные «хвосты» внутри движка

- `InspectNodeEndpointAsync`: kfw хардкодит `"9094/tcp"` + парсинг env
  `KAFKA_ADVERTISED_LISTENERS`; vwk хардкодит `"6379/tcp"` + `State.Running`.
- `NodeLimits`: kfw — `(long NanoCpus, long MemoryBytes)`
  (`KafkaWorker.Core/Planning/NodeRegenPlanner.cs`), vwk — `(decimal?
  CpuCores, long? MemoryBytes)` (`ValkeyWorker.Core/Model/ValkeyDomain.cs`).
- Docker-факт endpoint/лимитов нужен всем трём — обобщается нейтрально (§4.6).

### 2.4. Потребители движка

- `Program.cs` ×3: DI-регистрация `DockerEngineFactory` (pg — с
  `docker.Tls`/`docker.Ssh` из `PgWorkerOptions.Docker`; kfw/vwk — `new
  DockerEngineFactory()` без опций) + `PlainClusterDriver`/`SwarmClusterDriver`.
- `PgWorker.Backups` (`BackupProcess`, `BackupVerifyProcess`): через
  `IClusterDriver.EngineFor(host)` → `CreateContainerAsync` (Cmd-спеки джобов
  с Entrypoint-сбросом), `GetContainerLogsAsync`, `ListContainersAsync`,
  `RemoveContainerAsync`, `RemoveVolumeAsync`.
- Тесты: юниты движка — `PgWorker.UnitTests/Docker/` (`DockerEngineTests` 325
  строк — FakeHandler без docker; `DockerEngineExecTests`;
  `EndpointSchemeTests`; `DockerTlsOptionsTests`; `SshTunnelOptionsTests`) и
  `ValkeyWorker.UnitTests/Docker/` (`ContainerSpecTests`, `TarArchiveTests`);
  docker-интеграции — `PgWorker.IntegrationTests/Docker/` (7 файлов, вкл.
  `SshTunnelEngineTests`, `TlsEngineProxyTests` + PKI `EngineProxyTestPki`,
  гейт `DockerTrait` по `PGW_TEST_DOCKER=1`); драйвер-тесты —
  `ClusterDriverTests` (pg, 831), `SwarmClusterDriverTlsTests` (vwk) и
  процессные; E2E — `PgWorker.IntegrationTests/E2e` (E2eFixture),
  `KafkaWorker.IntegrationTests` (Kafka-серия), `ValkeyWorker.IntegrationTests/E2e`.
- `SSH.NET` — пакет только у `PgWorker.Docker`; `Shared.Tls` — там же.

### 2.5. Инфраструктура Shared-сборок

`src/Shared.{Core,Etcd,Tls,Metrics}` (t08), в `PgWorker.slnx` — папка
`/common/`; паттерн: общие сборки не зависят от доменных, глобальные
`<Using>` на уровне csproj потребителей закрывают замены namespace,
`Directory.Packages.props` — централизованное версионирование. `Result`/
`HostInfo`/`Planning` — уже в `Shared.Core` (t09).

## 3. Принципы

1. **arch-first**: структурная фиксация «общий движок Shared.Docker» в
   канонах трёх воркеров — ДО кода (фаза A, §10).
2. **Паттерн `Shared.*`** (t08/t09): новая сборка `Shared.Docker` в `/common/`
   slnx; не зависит от доменных сборок; доменные движки удаляются полностью.
3. **Граница — только Engine-слой**: `ClusterDriver`'ы доменные, остаются
   (§8.1); их общий скелет (перебор хостов, swarm-обёртка) НЕ обобщается —
   расхождения доменные и осознанные.
4. **Канон-суперсет**: одно поведение для всех трёх доменов — самое надёжное
   из имеющихся (§7); никаких per-domain ветвей в общем коде движка.
5. **Нейтральность движка**: движок не знает доменных деталей — порт
   инспекции и label-ключ параметризованы, advertised-чтение kafka — в
   kfw-драйвере (§8.2/§8.3).
6. **Внешние контракты нетронуты**: имена docker-объектов (`pgw-`/`kfw-`/
   `vwk-`), сети, env-наборы, portalloc, etcd, HTTP API, образы, appsettings
   — без изменений.
7. **`TreatWarningsAsErrors=true`**: полный `dotnet build` после каждой фазы.
8. **Язык**: комментарии/доки — русский; идентификаторы — английские.

## 4. Решение: сборка `Shared.Docker`

### 4.1. Состав (`src/Shared.Docker/`)

```
Engine/IDockerEngine.cs        — union-интерфейс + все модели (§4.2/§4.3)
Engine/DockerEngine.cs         — реализация (база — pg-копия; +kfw/vwk-методы;
                                 канон-суперсет §4.5; helper-механика vwk)
Engine/DockerEngineFactory.cs  — фабрика pg-канона: EndpointScheme + TLS +
                                 SSH-кэш туннелей; IAsyncDisposable
Engine/EndpointScheme.cs       — as-is из pg
Engine/SshHostConnection.cs    — as-is из pg
Engine/DockerTlsOptions.cs     — модель (поля PEM) БЕЗ pg-env-биндингов (§4.7)
Engine/SshTunnelOptions.cs     — модель БЕЗ pg-env-биндингов (§4.7)
Engine/TarArchive.cs           — as-is из vwk
Shared.Docker.csproj           — см. зависимости ниже
```

Канон реализации — pg-копия (самая полная: фабрика с транспортами, инспект
контейнера, логи); kfw/vwk-методы вливаются в тот же класс. `DockerHttpException`
— в `IDockerEngine.cs` (как сегодня в копиях).

Зависимости: `Shared.Core` (Result), `Shared.Tls` (TlsMaterial/TlsChain),
пакеты `SSH.NET`, `Microsoft.Extensions.Logging.Abstractions`. Новых версий
пакетов нет — `SSH.NET` уже в `Directory.Packages.props` (использует
`PgWorker.Docker`); запись остаётся, потребитель — `Shared.Docker`.

### 4.2. Интерфейс `IDockerEngine` — union методов

Идемпотентность (общий контракт, дословно из копий): 404 на удалении =
успех; 409 «already exists» на создании = успех; 304 start = успех.

| Метод | Происхождение | Изменения при объединении |
|---|---|---|
| `PingAsync` | все | — |
| `ListContainersAsync(namePrefix, all)` | все | — |
| `InspectContainerAsync(id)` → `DockerContainerInspect` | pg | — |
| `GetContainerLogsAsync(idOrName, tail)` | pg | — |
| `CreateContainerAsync(spec, name)` | все | + pull-fallback (404 «No such image» → `PullImageAsync` → retry) — суперсет из kfw/vwk |
| `StartContainerAsync(idOrName)` | все | + catch 304 — суперсет из pg |
| `StopContainerAsync(idOrName, timeoutSec)` | все | — (304/404 — уже у всех) |
| `RemoveContainerAsync(idOrName, force)` | все | — |
| `ExecAsync(containerId, cmd)` | pg/kfw | ошибка exit!=0 включает stderr И stdout — суперсет из kfw |
| `EnsureNetworkAsync(name)` | все | — |
| `DeleteNetworkAsync(name)` | kfw/vwk | — |
| `RemoveVolumeAsync(name)` | pg/kfw | — (vwk-домен использует `DeleteVolumeAsync`) |
| `VolumeExistsAsync(name)` | kfw | — |
| `EnsureVolumeAsync(name)` | vwk | — |
| `DeleteVolumeAsync(name)` | vwk | — |
| `PutVolumeArchiveAsync(name, tar, image)` | vwk | helper-механика (create+exec-запись+finally-clean) as-is |
| `GetVolumeArchiveAsync(name, image)` | vwk | helper + GET container-archive + переупаковка as-is |
| `ListNodesAsync` | все | — |
| `CreateServiceAsync(spec)` | все | — |
| `RemoveServiceAsync(name)` | все | — |
| `ListServicesAsync(namePrefix)` | все | — |
| `ListTasksAsync(serviceName)` | все | `DockerTask` — union c `ContainerId` (pg/kfw уже имеют) |
| `BusyPortsAsync` | все | — |
| `InspectContainerResourcesAsync(name)` → `NodeLimits?` | kfw/vwk | kfw-реализация (JsonElement, чтение `NanoCpus`/`NanoCPUs` обоих) |
| `InspectServiceResourcesAsync(name)` | kfw/vwk | — |
| `InspectContainerEnvAsync(idOrName)` | kfw | — |
| `InspectServiceEnvAsync(name)` | kfw | — |
| `InspectContainerCmdAsync(idOrName)` | vwk | — |
| `InspectServiceCmdAsync(name)` | vwk | — |
| `InspectNodeEndpointAsync(name, containerPort)` → `DockerNodeEndpoint?` | kfw/vwk | **порт — параметр** (kfw: 9094, vwk: 6379); swarm-фолбэк as-is; advertised-чтение уходит в kfw-драйвер (§4.6) |
| `PullImageAsync(imageName)` | pg (internal) | internal; путь pull-fallback + интеграции-тесты |

### 4.3. Модели (все — в `IDockerEngine.cs`, namespace `Shared.Docker`)

| Модель | Канон | Изменения |
|---|---|---|
| `DockerContainer(Id, Names, State, Image)` | все | — |
| `DockerContainerInspect(Id, Hostname, Aliases, Env, Ports, Running?, ExitCode?, StartedAtUnix?)` | pg | — (kfw/vwk получают доступ) |
| `DockerSwarmNode(Id, Hostname, State, RunningTasks)` | все | — |
| `DockerTask(Id, NodeId, State, Host?, PublishedPort?, ContainerId?)` | pg/kfw | vwk-копия без `ContainerId` — union с ним |
| `PortMap(ContainerPort, HostPort)` | все | — |
| `NodeLimits(long NanoCpus, long MemoryBytes)` | kfw (docker-факт) | см. §4.6 |
| `DockerNodeEndpoint(int ClientHostPort, bool Running, string? TaskHost = null)` | union kfw/vwk | см. §4.6 |
| `ServiceSpec(Name, Template, NodeConstraint)` | все | — |
| `ContainerSpec` | супер-спека | §4.4 |
| `DockerHttpException(method, path, statusCode, body)` | все | — |

### 4.4. `ContainerSpec` — супер-спека с флагом `ResetEntrypoint`

Надмножество полей трёх копий; домены передают своё, отсутствующее — null:

```csharp
public sealed record ContainerSpec(
    string Image,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    IReadOnlyDictionary<string, string>? Env = null,     // pg/kfw (vwk: null)
    IReadOnlyList<string>? Cmd = null,                   // vwk — всегда; pg-джобы
    bool ResetEntrypoint = false,                        // pg-семантика: при Cmd
                                                         // сбросить ENTRYPOINT образа
    string? VolumeName = null, string? VolumeDest = null,// pg/kfw data-volume
    IReadOnlyList<string>? Binds = null,                 // vwk TLS-volume "vol:/path"
    string? Network = null, IReadOnlyList<string>? NetworkAliases = null,
    IReadOnlyDictionary<string, string>? Tmpfs = null,   // pg-джобы бэкапов
    IReadOnlyList<string>? ExtraHosts = null,            // pg-джобы бэкапов
    double? CpuCores = null, long? MemoryBytes = null,
    string? RestartPolicy = null,                        // default "unless-stopped"
    string? Label = null, string? LabelKey = null);      // label-пара; ключ —
                                                         // домен: pgworker/
                                                         // kafkaworker/valkeyworker
```

- `BuildContainerBody`: `Env` null → поле не пишется (vwk-контейнеры не
  меняются); `Cmd` + `ResetEntrypoint=true` → `Entrypoint=[]` (сегодняшнее
  поведение pg-путей); `Cmd` + `ResetEntrypoint=false` → Cmd аргументами
  entrypoint (сегодняшнее поведение kfw-тестов/vwk); `LabelKey`/`Label` оба
  заданы → `Labels = { [LabelKey] = Label }`; оба null → без Labels.
- `BuildServiceBody` (swarm) — аналогично union (Env/Cmd/Binds→Mounts/
  VolumeName→Mounts/Label-пара/лимиты), канон pg-копии + vwk-ветки.
- Перенос потребителей: pg — `Cmd`-пути (`WalStreamProcess`/`BackupProcess`/
  verify/restore-джобы, тестовые alpine) добавляют `ResetEntrypoint: true`;
  pg-драйвер `BuildSpec` — Label-пара `("pgworker", cluster)`; kfw/vwk —
  Label-пара своего домена. Точный перечень точек `Cmd`-сброса — плану (grep
  `Cmd:` по `PgWorker.*`).

### 4.5. Фабрика: транспорты как опции (доступные всем доменам)

Канон — pg-копия `DockerEngineFactory` as-is: ctor
`(DockerTlsOptions? tls = null, SshTunnelOptions? ssh = null, ILogger?
logger = null, ILoggerFactory? loggerFactory = null)`; `Create(endpoint,
hostAlias?)` — `unix://` (ConnectCallback) | `tcp://` (+TLS при конфигурации,
plaintext — warning-лог) | `ssh://` (туннель → `tcp://127.0.0.1:<bound>`),
кэш туннелей + `EnsureConnected`, `IAsyncDisposable`, API v1.44.

- Регистрации kfw/vwk `new DockerEngineFactory()` остаются валидными
  (опции optional). **Следствие унификации**: kfw/vwk получают схему
  `ssh://`/`tcp://+TLS` в `Hosts[].Endpoint` без своего кода (конфиг-модель
  endpoint — строка, дополнительно не расширяется).
- kfw/vwk-фабрики (парсинг `StartsWith("unix://")`) удаляются; их поведение
  покрывается `EndpointScheme` (для `tcp://` — базовый адрес `http://host:port`,
  pg-канон).

### 4.6. Нейтрализация доменных хвостов

1. **`InspectNodeEndpointAsync(name, containerPort)`**: движок возвращает
   `DockerNodeEndpoint(ClientHostPort, Running, TaskHost?)` — порт-биндинг
   `"<containerPort>/tcp"` + `State.Running` из инспекта, swarm-фолбэк по
   running-таску as-is (kfw-механика). Чтение advertised-пары из env
   `KAFKA_ADVERTISED_LISTENERS` (парсер `ReadAdvertisedClient`) **переезжает
   в kfw-драйвер**: `NodeEndpointInspection.AdvertisedClient` заполняется из
   `NodeEnvAsync` (движковый `InspectContainerEnvAsync` уже есть). Доменные
   записи инспекции (`NodeEndpointInspection` kfw/vwk) не меняются.
2. **`NodeLimits(long NanoCpus, long MemoryBytes)`** — docker-факт, живёт в
   `Shared.Docker`. kfw: доменная запись `NodeLimits` в
   `KafkaWorker.Core/Planning/NodeRegenPlanner.cs` удаляется, потребители
   переключаются на `Shared.Docker.NodeLimits` (поля идентичны — замена типа).
   vwk: доменная `NodeLimits(decimal? CpuCores, long? MemoryBytes)` остаётся;
   vwk-драйвер конвертирует (`nanoCpus → cores = nanoCpus / 1e9`).
3. **label-ключ** — параметр `ContainerSpec.LabelKey` (§4.4); домены передают
   свой (`pgworker`/`kafkaworker`/`valkeyworker`). Helper-контейнеры
   volume-архива (vwk-механика) создаются движком без label — как сегодня.

### 4.7. Опции моделей vs env-биндинги

`DockerTlsOptions`/`SshTunnelOptions` — модели (PEM-поля, fail-fast фабрики)
переносятся в `Shared.Docker` **без** массивов `EnvBindings`/`ApplyEnvOverrides`:
env-имена `PGW_DOCKER_TLS_*`/`PGW_DOCKER_SSH_*` — pg-специфика. Pg-часть
(`EnvBindings` + `ApplyEnvOverrides`, ~30 строк на обе опции) переносится в
`PgWorker.App` (один файл `DockerEnvBindings.cs`, вызовы в `Program.cs:35–36`
сохраняются); юнит-тесты env-маппинга (`DockerTlsOptionsTests`,
`SshTunnelOptionsTests`) следуют за кодом (§6).

### 4.8. Целевая структура ссылок

```
/common/   Shared.Docker (НОВЫЙ)   → Shared.Core, Shared.Tls, SSH.NET,
                                    Microsoft.Extensions.Logging.Abstractions
           (+ существующие Shared.Core/Etcd/Tls/Metrics — без изменений)
/docker/   PgWorker.Docker         → теряет Engine/* (6 файлов); Drivers/* остаются;
                                    csproj: +Shared.Docker, −SSH.NET, −Shared.Tls
/kafka/    KafkaWorker.Docker      → теряет Engine/* (2 файла); Drivers/* остаются;
                                    csproj: +Shared.Docker
/valkey/   ValkeyWorker.Docker     → теряет Engine/* (4 файла); Drivers/* остаются;
                                    csproj: +Shared.Docker
/tests/    Shared.Docker.UnitTests (НОВЫЙ) — юниты движка (§6)
```

- `PgWorker.slnx`: `/common/` + `Shared.Docker/Shared.Docker.csproj`,
  `/tests/` + `Shared.Docker.UnitTests`; csproj доменных Docker-сборок —
  глобальный `<Using Include="Shared.Docker"/>` (механика t08/t09, гасит
  замены namespace в драйверах/бэкапах/тестах).
- `Program.cs` ×3 — без структурных правок (pg-фабрика уже с опциями;
  kfw/vwk — `new DockerEngineFactory()` валиден); `PgWorker.App` —
  env-биндинги §4.7.
- `PgWorker.Backups` — транзитивно через `PgWorker.Docker` + using.

## 5. Что НЕ входит в скоуп (осознанно)

- **`ClusterDriver`'ы, `NodeMatcher`, `BackupAgentNames`, `BackupJobsCleaner`,
  `HostEndpoint`** — доменные, остаются на месте (§8.1); их скелетное сходство
  (перебор хостов, Plain/Swarm-пара) не обобщается.
- **Фейки тестов** (`FakeEtcd`-аналогов здесь нет; FakeHandler-движка —
  переносится, §6) и процессные тесты — не трогаются, кроме замены namespace.
- **Конфиг-модели** (`PgWorkerOptions.Docker` и аналоги kfw/vwk): типы полей
  Tls/Ssh станут `Shared.Docker.*` — иначе без изменений; kfw/vwk-конфиги НЕ
  получают новых секций (SSH/TLS-опции доступны через фабрику, но секций
  конфига для них не добавляется — по требованию).
- **Новые NuGet-пакеты** — не добавляются; версия `SSH.NET` не меняется.
- **etcd/HTTP/docker-объекты/образы/deploy/dev-stand** — нетронуты.
- **Панель** (`AdminPanel.*`) — не трогается (движок не виден снаружи).

## 6. Перенос тестов

**Новый `src/tests/Shared.Docker.UnitTests`** (csproj по образцу
`Shared.Core.UnitTests`; `Shared.Docker` даёт ему `InternalsVisibleTo` —
`PullImageAsync`/`BuildContainerBody`/`Demux`/`CreateHandler` internal, как
сегодня в копиях):

- из `PgWorker.UnitTests/Docker/`: `DockerEngineTests` (FakeHandler-юниты:
  пинг/списки/идемпотентные catch/инспект/логи/Demux/BusyPorts),
  `DockerEngineExecTests`, `EndpointSchemeTests` — as-is (namespace);
- из `ValkeyWorker.UnitTests/Docker/`: `TarArchiveTests`,
  `ContainerSpecTests` → тесты `BuildContainerBody` супер-спеки;
- **новые кейсы суперсета** (регресс-выравнивания §7): start-304 = успех;
  create-404 «No such image» → pull → retry (по call-логу FakeHandler);
  exec exit!=0 → сообщение содержит stdout; `Cmd`+`ResetEntrypoint` →
  `Entrypoint=[]`, без флага — Cmd как есть; `LabelKey`/`Label`-пара;
  `InspectNodeEndpointAsync` с параметром порта (9094/6379 → соответствующий
  биндинг); `Env=null` → тело без Env.
- env-маппинг-тесты (`DockerTlsOptionsTests`, `SshTunnelOptionsTests`)
  следуют за кодом §4.7 — в `PgWorker.UnitTests` (pg-специфика), модельные
  ассерты (частичная конфигурация → fail-fast фабрики) — в
  `Shared.Docker.UnitTests`.

**Остаются по месту** (замена namespace при необходимости):

- доменные драйвер-тесты: `ClusterDriverTests` (pg), `NodeMatcherTests`,
  `BackupAgentNamesTests`, `BackupJobsCleanerTests`, `SwarmClusterDriverTlsTests`
  (vwk), процессные обоих воркеров — индикаторы регрессии доменной семантики;
- docker-интеграции `PgWorker.IntegrationTests/Docker/` (7 файлов, вкл.
  `SshTunnelEngineTests`/`TlsEngineProxyTests`/`EngineProxyTestPki`) —
  прецедент t09 (интеграционные по месту): PKI-фикстуры и `DockerTrait`-гейт
  уже там; тесты переключаются на `Shared.Docker` и расширяются кейсами
  суперсета против живого docker (pull-fallback на отсутствующем образе);
- Kafka/Valkey-интеграции и E2E — индикаторы гейта (§11), не переносятся.

Правила тестов AGENTS.md (динамические порты `assignRandomHostPort: true`,
полный teardown с ассертом чистоты, зачистка контейнеров/сетей между
сериями, BrokerBootSec ≤ 100 с, телеметрия `docs/e2e-launch.md`) — без
изменений; `Shared.Docker.UnitTests` — чистые юниты без docker (FakeHandler).

## 7. Осознанные изменения поведения (канон-суперсет, решение §8.2)

1. **kfw/vwk: `StartContainerAsync` — 304 already-started = успех** (сегодня
   kfw/vwk бросают исключение). Идемпотентность супервиза;pg-семантика.
2. **pg: `CreateContainerAsync` при 404 «No such image» — pull образа и
   повтор create** (сегодня kfw и vwk уже имеют — суперсет фактически меняет
   поведение только pg). Надёжность первого запуска на чистом
   хосте; `PullImageAsync` против локального registry (канон образов —
   `192.168.0.1:5000`, локально-собираемые образы туда не кладутся — на них
   fallback просто не сработает, поведение не хуже сегодняшнего).
3. **pg/vwk: ошибка `ExecAsync` (exit!=0) включает stdout** (сегодня только
   kfw) — диагностика (Kafka-CLI/утилиты печатают stack trace в stdout).
4. **kfw/vwk: label-ключ контейнеров/сервисов — доменный**
   (`kafkaworker`/`valkeyworker`; сегодня — наследие-литерал `pgworker`).
   Читателей label нет; docker-inspect-косметика. pg — без изменений.
5. **kfw: advertised-пара endpoint-инспекции читается в драйвере** (значение
   `NodeEndpointInspection.AdvertisedClient` — то же, источник —
   `NodeEnvAsync` вместо движкового env-парсинга; функционально идентично).
6. **vwk: `DockerTask` инспекции несёт `ContainerId`** (движковый факт;
   драйвер не использует — безвредное расширение).

Иных изменений поведения нет: имена объектов, env, сети, порты, etcd, API —
нетронуты. Все шесть пунктов — внутренняя механика движка/диагностика,
на внешние контракты не влияют.

## 8. Решения пользователя (AskUserQuestion, 2026-09-20)

1. **Граница унификации**: «Только Engine-слой» — `Shared.Docker` выносит
   движок+фабрику+модели+TarArchive (union методов); `ClusterDriver`'ы
   доменные, переключаются на общий движок, копии движка удаляются.
2. **Семантика объединения**: «Канон-суперсет» — единое поведение для всех
   трёх доменов: 304-catch, pull-fallback, exec-stdout, label-ключ домена.
3. **Доменные хвосты**: «Нейтральный движок» — endpoint-инспекция с
   параметром `containerPort`, advertised-чтение — в kfw-драйвере;
   `NodeLimits(long, long)` — docker-факт в Shared, доменные конверсии у
   потребителей.
4. **`ContainerSpec`**: «Супер-спека + флаг» — надмножество полей, различие
   семантики Cmd — явный `ResetEntrypoint` (pg-пути передают true).
5. **arch-правка**: «Правки в 14/16/21» — фиксация общего движка в канонах
   трёх воркеров + отражение суперсета (без нового arch-документа).

## 9. Риски и меры

| Риск | Мера |
|---|---|
| Регрессия идемпотентности/транспорта при вливании трёх копий в один движок | Ядро as-is (pg-канон); union-таблица §4.2 — построчная сверка в плане; перенесённые юниты + новые кейсы суперсета (§6); docker-интеграции pg (DockerTrait-гейт) |
| Регрессия pg-бэкапов (Cmd-спеки джобов с Entrypoint-сбросом) | `ResetEntrypoint: true` во всех Cmd-путях (grep-сверка в плане); юнит-кейсы Body-билдера; E2E бэкапов в полном E2eFixture |
| Регрессия vwk TLS-volume (helper-механика + tar) | Перенос as-is; `TarArchiveTests` + `SwarmClusterDriverTlsTests`; vwk-E2E в гейте |
| Pull-fallback тянет образ из Docker Hub вместо registry | Канон образов — `images.txt` + `pull-images.sh` (runbook); fallback повторяет ровно kfw/vwk-механику; e2e/интеграции прогоняются после `pull-images.sh` |
| Опечатка в LabelKey/порте инспекции у доменов | Константы домена в одном месте (драйвер/NodeEnvBuilder); юнит-кейсы §4.6 |
| `TreatWarningsAsErrors` на осиротевших using | Полный build после каждой фазы (принцип 7) |
| Осколки копий после переноса | Grep-гейт (§12.1) на фазе чистки |
| Образы воркеров не собираются после смены ссылок | Фаза G: сборка трёх образов + compose config валиден |
| Расхождение vwk-конверсии лимитов (decimal?) | Юнит-кейс конверсии nanoCpus↔cores в vwk-домене; инспекции-тесты драйвера |

## 10. Фазы (скелет для плана)

Каждая фаза — зелёный build+unit своего контура; порядок обязателен.

- **Фаза A — arch-first**: `arch/14-pgworker.md` §2.2 (после «per-host
  connection»): клиент Engine API — общая сборка `Shared.Docker` (унификация
  t07: три копии движка объединены; SSH/TLS — опции фабрики для всех
  доменов; create при отсутствии образа — pull+retry). §2.2.1 — ссылка на
  реализацию транспорта в `Shared.Docker`. `arch/16-kafkaworker.md` §2.5:
  общий `Shared.Docker` (порт t07) + строка суперсета (start 304-идемпотентен,
  pull-fallback, exec-диагностика с stdout, label `kafkaworker`).
  `arch/21-valkeyworker.md` §2: аналогичная строка (label `valkeyworker`,
  TLS-volume-транспорт — методы `Shared.Docker`). Сверка живых упоминаний
  переносимых типов вне `docs/superpowers/`.
- **Фаза B — каркас `Shared.Docker`**: csproj + slnx (/common/, /tests/);
  перенос ядра (IDockerEngine union, DockerEngine, фабрика, EndpointScheme,
  SshHostConnection, опции-модели, TarArchive); канон-суперсет §7;
  нейтрализация §4.6; `Shared.Docker.UnitTests` — переносы + новые кейсы
  (§6). Домены ещё на своих копиях.
- **Фаза C — переключение PgWorker**: `PgWorker.Docker/Drivers` + csproj
  (`+Shared.Docker`, `−SSH.NET/−Shared.Tls`, Using); `ResetEntrypoint: true`
  во всех Cmd-путях (Backups/Drivers/тесты); `PgWorker.App` — env-биндинги
  §4.7 (Program.cs), Options-типы; `PgWorker.Backups` — Using; удалить
  `PgWorker.Docker/Engine/*` (6 файлов); правка тестов pg (юниты движка уже
  в B — локальные файлы удалить; интеграции — namespace).
- **Фаза D — переключение KafkaWorker**: Drivers + csproj + Using; замена
  доменного `NodeLimits` на `Shared.Docker.NodeLimits` (потребители
  NodeRegenPlanner/регенератор); advertised-чтение — в драйвер (перенос
  `ReadAdvertisedClient`, заполнение `NodeEndpointInspection.AdvertisedClient`
  из `NodeEnvAsync`); label `kafkaworker`; удалить `KafkaWorker.Docker/Engine/*`
  (2 файла); правка тестов kfw.
- **Фаза E — переключение ValkeyWorker**: Drivers + csproj + Using;
  NodeLimits-конверсия в драйвере; label `valkeyworker`; удалить
  `ValkeyWorker.Docker/Engine/*` (4 файла, вкл. отдельную фабрику); правка
  тестов vwk.
- **Фаза F — чистка**: grep-гейт остатков (§12.1); сверка slnx/CPM;
  ревизия using/неймспейсов в затронутых тестах.
- **Фаза G — верификация и гейты** (полный состав — §11): build Release
  0 warnings → все юниты (с зачисткой между сериями) → интеграции ×3
  (docker, зачистка серий) → полный docker-E2E Pg+Kfw+Valkey на свежем
  Release → сборка трёх образов + compose-config. Телеметрия — по
  `docs/e2e-launch.md`; упавший сценарий не перезапускается без анализа
  логов.

## 11. Мерж-гейт (roadmap + AGENTS.md)

Ветеринарная последовательность фазы G, все пункты обязательны:

1. `dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
2. Все юнит-серии (вкл. `Shared.Docker.UnitTests`) — зелёные; зачистка
   контейнеров/сетей/томов после каждой серии (AGENTS.md).
3. Интеграционные docker-серии: `PgWorker.IntegrationTests` (полные, вкл.
   Docker-серию `PGW_TEST_DOCKER=1`), `KafkaWorker.IntegrationTests`
   (полные), `ValkeyWorker.IntegrationTests` (полные); зачистка между
   сериями; `AdminPanel` — дым (не тронут).
4. **Полный docker-E2E всех трёх доменов на свежем Release** (roadmap-гейт
   задачи):
   - PgWorker: полный `E2eFixture` — `DOTNET_CLI_UI_LANGUAGE=en
     PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release
     --filter FullyQualifiedName~E2e` (E2eFixture собирает Release сам;
     `PGW_TEST_E2E_NOBUILD=1` — только бисект; точный фильтр серии план
     сверяет по именам классов `E2e*Scenarios`);
   - KafkaWorker: полный docker-прогон Kafka-серии (живые брокеры,
     `KafkaClusterFixture`) — это и есть docker-E2E kfw-домена;
   - ValkeyWorker: полный E2E `ValkeyE2eLifecycleTests` + vwk-интеграции.
   Перед сериями — `dev-stand/images/pull-images.sh`; между сериями —
   зачистка (дождаться финальной строки прогона, `docker network prune -f`
   страховочно); телеметрия E2E по постоянным правилам.
5. Сборка docker-образов `pgworker:dev`, `kafkaworker:dev`, `valkeyworker:dev`;
   `deploy/docker-compose.yml` config валиден. Локально-собираемые образы в
   registry НЕ кладутся.
6. Тем же мерж-коммитом в `main`: удалить пункт `t07-unify-docker-engine` из
   `arch/roadmap/pgworker.md` (и из `←`-зависимостей, если упомянут);
   история задачи — `docs/superpowers/2026-09-20-t07-unify-docker-engine/`.

## 12. Критерии приёмки

1. **Дедупликация**: в `src/` ровно одна копия движка — grep-гейт
   `grep -rn "class DockerEngine\b|interface IDockerEngine|class
   DockerEngineFactory|class SshHostConnection|record EndpointScheme|class
   TarArchive|class DockerHttpException|record ContainerSpec|record
   NodeLimits"` по `src/` находит определения только в `Shared.Docker`
   (допустимо: `NodeLimits`-упоминания kfw-потребителей как использования);
   каталоги `*/Docker/Engine/` трёх воркеров не существуют.
2. `dotnet build src/PgWorker.slnx -c Release` — зелёный, 0 warnings.
3. Все серии §11 (2–4) зелёные, вкл. полный docker-E2E трёх доменов.
4. Изменения поведения ограничены §7 (шесть пунктов); внешний контракт
   (имена объектов/env/etcd/API) — без изменений: интеграции проходят без
   правок ожиданий, кроме осознанных правок §7 (label, advertised-источник).
5. Три образа собираются; deploy/dev-stand конфигурации не менялись.
6. arch/14 §2.2/§2.2.1, arch/16 §2.5, arch/21 §2 обновлены (фаза A);
   roadmap-тег снят тем же коммитом (§11.6).
7. `SSH.NET`-ссылка — только у `Shared.Docker` (csproj доменов чисты).

## 13. Open questions

Нет: пять проектных развилок закрыты пользователем (§8). Детали,
остающиеся плану (не развилки): точный перечень Cmd-путей pg с
`ResetEntrypoint` (grep в фазе C), порядок вливания kfw/vwk-методов в класс
движка (фаза B), имена тест-кейсов суперсета.
