# t07-unify-docker-engine — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Три копии docker-движка (`PgWorker.Docker/Engine`, `KafkaWorker.Docker/Engine`, `ValkeyWorker.Docker/Engine`) объединяются в одну общую сборку `Shared.Docker` (union-методы, канон-суперсет семантики, SSH/TLS как опции фабрики); копии в доменах удаляются, доменные `ClusterDriver`'ы остаются и переключаются на общий движок.

**Architecture:** Новая сборка `src/Shared.Docker` (папка `/common/` slnx, паттерн t08/t09) не зависит от доменных сборок. Канон реализации — pg-копия (самая полная); kfw/vwk-методы вливаются в тот же класс движка; четыре семантических расхождения решаются канон-суперсетом (spec §7). Pg-специфика env-биндингов (`PGW_DOCKER_*`) переезжает в `PgWorker.App`. Внешние контракты (etcd/HTTP/имена docker-объектов/образы/конфиги) не меняются.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), CPM (`Directory.Packages.props`), xunit.v3 + FluentAssertions, SSH.NET (версия не меняется).

**Spec:** `docs/superpowers/2026-09-20-t07-unify-docker-engine/spec.md` — план аргументируется от spec; исполнитель читает spec и план вместе.

## Глобальные ограничения (spec §3 + AGENTS.md, действуют на каждую задачу)

- `TreatWarningsAsErrors=true`: после каждой фазы — полный `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release`, 0 warnings.
- Порты docker-контейнеров в тестах — только динамические (`assignRandomHostPort: true` / зонд свободных портов); никаких литералов вида `:16000`.
- После КАЖДОЙ docker-серии (интеграции/E2E) — зачистка (дождаться финальной строки прогона): остаточные контейнеры/тома `pgw-*|kfw-*|vwk-*` доменов + `docker network prune -f` (страховочно; сети per-cluster ryuk не подбирает).
- `BrokerBootSec` интеграционных фикстур ≤ 100 с.
- E2E-изоляция — `docs/e2e-isolation.md`; телеметрия E2E — `docs/e2e-launch.md` (упавший сценарий НЕ перезапускается без анализа логов и согласия пользователя).
- Внешние образы — из локального registry `192.168.0.1:5000`; перед docker-сериями — `dev-stand/images/pull-images.sh`. Локально-собираемые образы (`pgworker:dev` и др.) в registry НЕ класть.
- Новых NuGet-пакетов нет; `SSH.NET 2026.0.0` уже в `Directory.Packages.props` — запись остаётся, потребитель становится `Shared.Docker`.
- Язык: комментарии/доки — русский, идентификаторы — английские.
- Внешние контракты нетронуты: имена объектов (`pgw-`/`kfw-`/`vwk-`), env-наборы, portalloc, etcd-ключи, HTTP API, appsettings, deploy/dev-stand — без изменений. Изменения поведения — только шесть пунктов spec §7.
- Работа — только в worktree `/Users/demakaev/ZCodeProject/worktrees/t07-unify-docker-engine` (ветка `t07-unify-docker-engine`). Коммиты в feature-ветке — после каждой задачи; мерж в `main` — по отдельному приказу пользователя.

## Карта файлов

```
CREATE  src/Shared.Docker/Shared.Docker.csproj                    (задача 2)
CREATE  src/Shared.Docker/Engine/EndpointScheme.cs                (задача 2, as-is pg)
CREATE  src/Shared.Docker/Engine/SshHostConnection.cs             (задача 2, as-is pg)
CREATE  src/Shared.Docker/Engine/DockerTlsOptions.cs              (задача 2, модель без env-биндингов)
CREATE  src/Shared.Docker/Engine/SshTunnelOptions.cs              (задача 2, модель без env-биндингов)
CREATE  src/Shared.Docker/Engine/TarArchive.cs                    (задача 2, as-is vwk)
CREATE  src/Shared.Docker/Engine/IDockerEngine.cs                 (задача 3, union-контракт)
CREATE  src/Shared.Docker/Engine/DockerEngine.cs                  (задача 3, pg-канон + вливание kfw/vwk)
CREATE  src/Shared.Docker/Engine/DockerEngineFactory.cs           (задача 3, as-is pg)
CREATE  src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj (задача 2)
CREATE  src/tests/Shared.Docker.UnitTests/Engine/*.cs             (задачи 2–3: переносы + суперсет-кейсы)
CREATE  src/PgWorker.App/DockerEnvBindings.cs                     (задача 4, pg env-биндинги)
MODIFY  src/PgWorker.slnx                                         (задача 2: /common/ + /tests/)
MODIFY  arch/14-pgworker.md, arch/16-kafkaworker.md, arch/21-valkeyworker.md (задача 1)
MODIFY  src/PgWorker.Docker/{PgWorker.Docker.csproj, Drivers/ClusterDriver.cs}      (задача 4)
MODIFY  src/PgWorker.App/{Program.cs, Options.cs, PgWorker.App.csproj, HealthChecks/ServiceProbes.cs} (задача 4)
MODIFY  src/PgWorker.Backups/{csproj, WalStreamProcess.cs, Job/BackupJobSpec.cs, Job/VerifyJobSpec.cs, Restore/RestoreJobSpec.cs, Process/BackupProcess.cs, Process/BackupVerifyProcess.cs} (задача 4)
MODIFY  src/tests/PgWorker.UnitTests/*, src/tests/PgWorker.IntegrationTests/*       (задача 4, правки по месту)
DELETE  src/PgWorker.Docker/Engine/ (6 файлов: DockerEngine, IDockerEngine, EndpointScheme, SshHostConnection, SshTunnelOptions, DockerTlsOptions)  (задача 4)
MODIFY  src/KafkaWorker.Docker/{csproj, Drivers/ClusterDriver.cs}                   (задача 5)
MODIFY  src/KafkaWorker.Core/{csproj, Planning/NodeRegenPlanner.cs}                 (задача 5)
MODIFY  src/KafkaWorker.App/{Program.cs, HealthChecks/ServiceProbes.cs, csproj}     (задача 5)
MODIFY  src/tests/KafkaWorker.UnitTests/*, src/tests/KafkaWorker.IntegrationTests/* (задача 5)
DELETE  src/KafkaWorker.Docker/Engine/ (2 файла: DockerEngine, IDockerEngine)       (задача 5)
MODIFY  src/ValkeyWorker.Docker/{csproj, Drivers/ClusterDriver.cs}                  (задача 6)
MODIFY  src/ValkeyWorker.App/{Program.cs, HealthChecks/ServiceProbes.cs, csproj}    (задача 6)
MODIFY  src/ValkeyWorker.Provisioning/{csproj, Processes/NodeTlsProvisioner.cs}     (задача 6)
MODIFY  src/tests/ValkeyWorker.UnitTests/*, src/tests/ValkeyWorker.IntegrationTests/* (задача 6)
DELETE  src/ValkeyWorker.Docker/Engine/ (4 файла: DockerEngine, DockerEngineFactory, IDockerEngine, TarArchive) (задача 6)
MODIFY  arch/roadmap/pgworker.md                                                     (задача 9, мерж-коммит)
```

Примечание: счётчики файлов Engine согласованы со spec §4.8/§10 и фактическим каталогом worktree: pg — 6, kfw — 2, vwk — 4. Удаляются фактические каталоги; критерий приёмки §12.1 — «каталоги `*/Docker/Engine/` не существуют».

---

### Задача 1: arch-first — фиксация общего движка в канонах (Фаза A)

**Связь со spec:** §3.1 (arch-first), §10-A, §12.6.
**Вход (предусловие):** worktree на ветке `t07-unify-docker-engine`, код не тронут; spec одобрен.
**Выход (что готово):** каноны arch/14 §2.2/§2.2.1, arch/16 §2.5, arch/21 §2 фиксируют общий движок `Shared.Docker` и суперсет-семантику; устаревших упоминаний переносимых типов вне `docs/superpowers/` нет. Коммит arch-only.

- [ ] **Шаг 1.1: arch/14 §2.2** — в `arch/14-pgworker.md` §2.2 после строки «Каждый хост — свой клиент Docker Engine API (per-host connection).» (~строка 325) добавить абзац:

```markdown
  Клиент Engine API — общая сборка трёх воркеров `Shared.Docker`
  (унификация t07: три копии движка объединены; SSH/TLS — опции фабрики,
  доступные всем доменам; create при отсутствии образа — pull+retry).
```

- [ ] **Шаг 1.2: arch/14 §2.2.1** — в конец раздела §2.2.1 (перед `### 2.3. Режим Swarm`, ~строка 381) добавить:

```markdown
- **Реализация транспорта** (unix / tcp+TLS / ssh-туннель с кэшем и
  reconnect, docker-PKI-материал) — общая сборка `Shared.Docker` (t07);
  env-секреты `PGW_DOCKER_TLS_*`/`PGW_DOCKER_SSH_*` — pg-слой `PgWorker.App`
  (`DockerEnvBindings`).
```

- [ ] **Шаг 1.3: arch/16 §2.5** — в `arch/16-kafkaworker.md` §2.5 в конец раздела (после абзаца про per-cluster сети `kfw-net-<C>`, перед `## 3. Контракт etcd`) добавить:

```markdown
Клиент Engine API — общий `Shared.Docker` (порт t07-унификации движка;
канон-суперсет семантики: start 304-идемпотентен, create при отсутствии
образа — pull+retry, exec-ошибка включает stdout, label
контейнеров/сервисов — `kafkaworker`).
```

- [ ] **Шаг 1.4: arch/21 §2** — в `arch/21-valkeyworker.md` §2 в конец раздела (перед следующим `##`) добавить:

```markdown
Клиент Engine API — общий `Shared.Docker` (порт t07-унификации движка;
канон-суперсет: start 304-идемпотентен, create при отсутствии образа —
pull+retry, exec-ошибка включает stdout, label контейнеров/сервисов —
`valkeyworker`); TLS-volume-транспорт (helper-контейнер, tar-архив) —
методы `Shared.Docker` (`EnsureVolumeAsync`/`PutVolumeArchiveAsync`/
`GetVolumeArchiveAsync`/`DeleteVolumeAsync`).
```

- [ ] **Шаг 1.5: сверка живых упоминаний**. Действие: выполнить grep и поправить устаревшие упоминания переносимых типов (namespace `*.Docker.Engine`, «копия kfw-движка» и т.п.) в живых документах `arch/`, `docs/` (НЕ трогать `docs/superpowers/**` — история задач):

```bash
grep -rn "Docker\.Engine\|DockerEngine" arch/ docs/ deploy/ dev-stand/ --include="*.md" --include="*.yaml" --include="*.yml" | grep -v "docs/superpowers"
```

  Ожидание: упоминания канонических схем/транспорта остаются корректными; конкретные ссылки на доменные движки/фабрики либо отсутствуют, либо дополняются словом «общий `Shared.Docker`».

- [ ] **Шаг 1.6: проверка и коммит**. Проверка: `git diff --stat` — только `arch/14-pgworker.md`, `arch/16-kafkaworker.md`, `arch/21-valkeyworker.md` (+ возможные doc-правки шага 1.5). Коммит:

```bash
git add arch/ docs/
git commit -m "arch(t07): общий docker-движок Shared.Docker в канонах 14/16/21 (arch-first)"
```

---

### Задача 2: каркас `Shared.Docker` — csproj, slnx, вспомогательные типы и их юниты (Фаза B, часть 1)

**Связь со spec:** §4.1 (состав), §4.7 (опции-модели без env-биндингов), §4.8 (структура ссылок), §6 (переносы EndpointSchemeTests/TarArchiveTests/модельных тестов), §10-B.
**Вход (предусловие):** задача 1 закоммичена.
**Выход (что готово):** сборка `Shared.Docker` в slnx (`/common/`) собирается; вспомогательные типы перенесены; тест-проект `Shared.Docker.UnitTests` в slnx (`/tests/`) зелёный. Домены ещё на своих копиях (полный build решения зелёный).

- [ ] **Шаг 2.1: csproj сборки** — создать `src/Shared.Docker/Shared.Docker.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <InternalsVisibleTo Include="Shared.Docker.UnitTests"/>
        <InternalsVisibleTo Include="PgWorker.IntegrationTests"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
    </ItemGroup>

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Logging.Abstractions"/>
        <PackageReference Include="SSH.NET"/>
    </ItemGroup>

</Project>
```

  (`InternalsVisibleTo PgWorker.IntegrationTests` — `PullImageAsync`/`CreateHandler` используются интеграциями `DockerDriverTests`/`ExecDriverTests`, остаются в PgWorker-проекте; `Shared.Docker.UnitTests` — перенесённые юниты движка.)

- [ ] **Шаг 2.2: slnx** — в `src/PgWorker.slnx`: в `<Folder Name="/common/">` добавить `<Project Path="Shared.Docker/Shared.Docker.csproj" />`; в `<Folder Name="/tests/">` добавить `<Project Path="tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj" />`.

- [ ] **Шаг 2.3: тест-проект** — создать `src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj` по образцу `src/tests/Shared.Core.UnitTests/Shared.Core.UnitTests.csproj` (те же PackageReference: coverlet.collector/FluentAssertions/Microsoft.NET.Test.Sdk/xunit.runner.visualstudio/xunit.v3; `Using Include="Xunit"`; ProjectReference `..\..\Shared.Docker\Shared.Docker.csproj`).

- [ ] **Шаг 2.4: перенос as-is** — скопировать с заменой namespace на `Shared.Docker` (файлы доменных копий НЕ трогать — они удаляются в задачах 4–6):
  - `src/PgWorker.Docker/Engine/EndpointScheme.cs` → `src/Shared.Docker/Engine/EndpointScheme.cs`;
  - `src/PgWorker.Docker/Engine/SshHostConnection.cs` → `src/Shared.Docker/Engine/SshHostConnection.cs`;
  - `src/ValkeyWorker.Docker/Engine/TarArchive.cs` → `src/Shared.Docker/Engine/TarArchive.cs`.
  As-is: только `namespace` и `using` (убрать `using PgWorker.Core`/`ValkeyWorker.Core`, добавить `using Shared.Core` при необходимости) — комментарии сохранить.

- [ ] **Шаг 2.5: опции-модели без env-биндингов (§4.7)** — создать:
  - `src/Shared.Docker/Engine/DockerTlsOptions.cs`: класс `DockerTlsOptions` из pg-копии, НО без `EnvBindings` и `ApplyEnvOverrides` (и `using Microsoft.Extensions.Configuration`); PEM-поля `CaPem/CaPath/ClientCertPem/ClientCertPath/ClientKeyPem/ClientKeyPath` + doc-комментарии как в оригинале.
  - `src/Shared.Docker/Engine/SshTunnelOptions.cs`: класс `SshTunnelOptions` из pg-копии без `EnvBindings`/`ApplyEnvOverrides`; сохранить поля (`KeyPem/KeyPath/RemoteDaemonHost/RemoteDaemonPort/FingerprintSha256/KeepAliveSec/ConnectTimeoutSec`) и методы `TunnelTarget()` + `static DecideHostKeyTrust(...)` as-is.

- [ ] **Шаг 2.6: переносы юнитов** — в `src/tests/Shared.Docker.UnitTests/Engine/`:
  - `EndpointSchemeTests.cs` — as-is из `src/tests/PgWorker.UnitTests/Docker/EndpointSchemeTests.cs` (namespace → `Shared.Docker.UnitTests.Engine`, убрать using домена);
  - `TarArchiveTests.cs` — as-is из `src/tests/ValkeyWorker.UnitTests/Docker/TarArchiveTests.cs` (3 кейса: Build_Read_RoundTrip, Build_KeyHeaderMode0600, Read_TrailingNullBlocks_Terminate);
  - `SshTunnelOptionsModelTests.cs` — модельные кейсы из `src/tests/PgWorker.UnitTests/Docker/SshTunnelOptionsTests.cs` (без env-теста): `DecideHostKeyTrust_ExpectedPinSet_StrictComparison`, `DecideHostKeyTrust_NoPin_TofuAcceptWithWarning`, `KeyMaterial_PemOrPathFallback`, `TunnelTarget_DefaultsAndCustom_Validated`, `TunnelTarget_Invalid_FailFast` — as-is, namespace новый. (env-тест `ApplyEnvOverrides_SshKeysMapped` остаётся в PgWorker.UnitTests до задачи 4.)

- [ ] **Шаг 2.7: проверка сборки и юнитов**. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj -c Release
```

  Ожидание: build 0 warnings (все проекты решения, домены ещё на копиях); тесты зелёные.

- [ ] **Шаг 2.8: коммит**:

```bash
git add src/Shared.Docker src/tests/Shared.Docker.UnitTests src/PgWorker.slnx
git commit -m "feat(t07): каркас Shared.Docker — csproj/slnx + транспортные и модельные типы + юниты"
```

---

### Задача 3: union-интерфейс, движок-канон-суперсет, фабрика, юниты движка (Фаза B, часть 2)

**Связь со spec:** §4.1 (канон pg), §4.2 (union-таблица методов), §4.3 (модели), §4.4 (ContainerSpec-супер-спека + ResetEntrypoint), §4.5 (фабрика as-is), §4.6 (нейтрализация), §6 (переносы DockerEngineTests/Exec/ContainerSpec + новые кейсы суперсета), §7 (шесть изменений поведения), §10-B.
**Вход (предусловие):** задача 2 закоммичена, `Shared.Docker` собирается.
**Выход (что готово):** полный движок `Shared.Docker.Engine.DockerEngine` (union, канон-суперсет) + фабрика + `IDockerEngine`-контракт; юниты движка (перенесённые + новые суперсет-кейсы) зелёные; домены ещё на своих копиях.

**Interfaces (Produces — для задач 4–6):** namespace `Shared.Docker`: `IDockerEngine` (union ниже), `sealed class DockerEngine : IDockerEngine` (internal-члены: `CreateHandler` — на фабрике, `PullImageAsync`, `static BuildContainerBody`, `static BuildServiceBody`, `Demux`), `class DockerEngineFactory(DockerTlsOptions? tls = null, SshTunnelOptions? ssh = null, ILogger? logger = null, ILoggerFactory? loggerFactory = null) : IAsyncDisposable` с методом `IDockerEngine Create(string endpoint, string? hostAlias = null)`, модели `DockerContainer/DockerContainerInspect/DockerSwarmNode/DockerTask/PortMap/NodeLimits/DockerNodeEndpoint/ServiceSpec/ContainerSpec/DockerHttpException`.

- [ ] **Шаг 3.1: union-контракт** — создать `src/Shared.Docker/Engine/IDockerEngine.cs` (namespace `Shared.Docker`; doc-комментарии — по образцу копий, идемпотентность в шапке интерфейса). Состав методов — точно по таблице spec §4.2 (все сигнатуры уже существуют в копиях, кроме параметризованного endpoint):

```csharp
using Shared.Core;

namespace Shared.Docker;

// Тонкий клиент Docker Engine API: только нужные endpoints поверх HttpClient.
// Общий движок трёх воркеров (t07): union методов pg/kfw/vwk, канон-суперсет
// семантики. Идемпотентность: 404 на удаление = успех (объекта уже нет);
// 409 "already exists" на create = успех; 304 start = успех; create при
// 404 "No such image" — pull образа и повтор.
public interface IDockerEngine : IAsyncDisposable
{
    Task<Result> PingAsync(CancellationToken ct);
    Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct);
    Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct);
    Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct);
    Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct);
    Task<Result> StartContainerAsync(string idOrName, CancellationToken ct);
    Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);
    Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct);
    Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct);
    Task<Result> EnsureNetworkAsync(string name, CancellationToken ct);
    Task<Result> DeleteNetworkAsync(string name, CancellationToken ct);
    Task<Result> RemoveVolumeAsync(string name, CancellationToken ct);
    Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct);
    Task<Result> EnsureVolumeAsync(string name, CancellationToken ct);
    Task<Result> DeleteVolumeAsync(string name, CancellationToken ct);
    Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct);
    Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct);
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct);
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct);
    Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct);
    Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct);
    Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct);
    // containerPort — контейнерный порт клиентского listener'а (kfw: 9094, vwk: 6379).
    Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct);
}
```

  `PullImageAsync` — internal-метод класса `DockerEngine` (НЕ интерфейса), как в копиях.
  Модели в том же файле (§4.3):

```csharp
public sealed record DockerContainer(string Id, string[] Names, string State, string Image);

public sealed record DockerContainerInspect(
    string Id, string Hostname, string[] Aliases, string[] Env, PortMap[] Ports,
    bool? Running = null, int? ExitCode = null, long? StartedAtUnix = null);

public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);

// union: vwk-копия без ContainerId — объединена с pg/kfw-вариантом (§7.6).
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort,
    string? ContainerId = null);

public sealed record PortMap(int ContainerPort, int HostPort);

// docker-факт лимитов (0 = без лимита); доменные конверсии — у потребителей (§4.6.2).
public sealed record NodeLimits(long NanoCpus, long MemoryBytes);

// union kfw/vwk; advertised-пара ушла в kfw-драйвер (§4.6.1).
public sealed record DockerNodeEndpoint(int ClientHostPort, bool Running, string? TaskHost = null);

public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint);

public sealed record DockerHttpException(string method, string path, int statusCode, string body);
```

- [ ] **Шаг 3.2: ContainerSpec — супер-спека** — в том же файле, дословно по spec §4.4:

```csharp
// Супер-спека трёх доменов (t07): домены передают своё, отсутствующее — null.
// ResetEntrypoint=true (pg-семантика): при заданном Cmd сбросить ENTRYPOINT
// образа (Entrypoint=[]) — Cmd выполняется как есть, а не аргументами
// образного entrypoint; false — Cmd как аргументы entrypoint (kfw/vwk).
public sealed record ContainerSpec(
    string Image,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    IReadOnlyDictionary<string, string>? Env = null,
    IReadOnlyList<string>? Cmd = null,
    bool ResetEntrypoint = false,
    string? VolumeName = null, string? VolumeDest = null,
    IReadOnlyList<string>? Binds = null,
    string? Network = null, IReadOnlyList<string>? NetworkAliases = null,
    IReadOnlyDictionary<string, string>? Tmpfs = null,
    IReadOnlyList<string>? ExtraHosts = null,
    double? CpuCores = null, long? MemoryBytes = null,
    string? RestartPolicy = null,
    string? Label = null, string? LabelKey = null);
```

- [ ] **Шаг 3.3: движок — перенос канона и вливание** — создать `src/Shared.Docker/Engine/DockerEngine.cs`:
  1. Взять pg-копию `src/PgWorker.Docker/Engine/DockerEngine.cs` целиком (namespace `Shared.Docker`, `using Shared.Core`; doc-комментарии сохранить), НО вынести в отдельный файл `DockerEngineFactory.cs` часть фабрики (см. шаг 3.4). В копии pg фабрика и движок в одном файле — при переносе: класс `DockerEngineFactory` + `internal sealed class DockerTlsMaterial` (материал поверх `Shared.Tls`) + `CreateHandler`/SSH-кэш идут в `DockerEngineFactory.cs`; `DockerEngine` + `DockerHttpException` (если не в IDockerEngine.cs — по §4.1 исключение кладём в `IDockerEngine.cs`) — в `DockerEngine.cs`.
  2. Влить из kfw-копии (`src/KafkaWorker.Docker/Engine/DockerEngine.cs`): `VolumeExistsAsync`, `DeleteNetworkAsync`, `InspectContainerEnvAsync`, `InspectServiceEnvAsync`, `InspectContainerResourcesAsync` (чтение `NanoCpus`/`NanoCPUs` обоих написаний; возврат `NodeLimits(long, long)` — конверсий нет), `InspectServiceResourcesAsync`, `InspectNodeEndpointAsync` (см. п. 4), pull-fallback в `CreateContainerAsync` (см. шаг 3.5).
  3. Влить из vwk-копии (`src/ValkeyWorker.Docker/Engine/DockerEngine.cs`): `EnsureVolumeAsync`, `DeleteVolumeAsync`, `PutVolumeArchiveAsync` (helper-механика: create helper-контейнера без label + exec-запись tar + finally-clean — as-is), `GetVolumeArchiveAsync` (helper + GET `/containers/<helper>/archive` + переупаковка as-is), `InspectContainerCmdAsync`, `InspectServiceCmdAsync`, Detach-exec-поллинг-механика (если отличается от pg exec-пути — сохранить vwk-вариант в volume-archive-методах), ветви `BuildContainerBody`/`BuildServiceBody` (см. шаг 3.5).
  4. `InspectNodeEndpointAsync(string name, int containerPort, ...)`: kfw-механика, параметризованная портом: порт-биндинг `$"{containerPort}/tcp"` из инспекта + `State.Running` (vwk-семантика) + swarm-фолбэк по running-таску as-is (kfw-механика; порт и хост — из одного вызова `ListTasksAsync`). Возвращает `DockerNodeEndpoint(ClientHostPort, Running, TaskHost)`. Чтение advertised из env (`ReadAdvertisedClient`) в движок НЕ переносится (уезжает в kfw-драйвер, задача 5).
- [ ] **Шаг 3.4: фабрика as-is** — `src/Shared.Docker/Engine/DockerEngineFactory.cs`: pg-реализация фабрики as-is (namespace `Shared.Docker`): ctor `(DockerTlsOptions? tls = null, SshTunnelOptions? ssh = null, ILogger? logger = null, ILoggerFactory? loggerFactory = null)`, `Create(endpoint, hostAlias?)` — `unix://` (ConnectCallback) | `tcp://` (+TLS при конфигурации, plaintext-tcp — warning-лог) | `ssh://` (туннель → `tcp://127.0.0.1:<bound>`), `EndpointScheme.Parse`, кэш `SshHostConnection` + reconnect/`EnsureConnected`, `IAsyncDisposable`, API v1.44. Изменений кода нет, кроме namespace.
- [ ] **Шаг 3.5: канон-суперсет в движке (§7)** — точечные правки слитого кода:
  - `StartContainerAsync`: catch 304 → успех (pg-вариант; kfw/vwk его не имели — §7.1);
  - `CreateContainerAsync`: 404 «No such image» → `await PullImageAsync(spec.Image, ct)` → повтор create (kfw-вариант; §7.2);
  - `ExecAsync`: exit != 0 → сообщение содержит stderr И stdout (kfw-вариант; §7.3);
  - `BuildContainerBody`: `Env == null` → поле Env в тело НЕ пишется (vwk-контейнеры не меняются); `Cmd` + `ResetEntrypoint=true` → `"Entrypoint" = []`; `Cmd` без флага → Cmd как аргументы (без Entrypoint-ключа); `LabelKey != null && Label != null` → `Labels = { [LabelKey] = Label }`; оба null → без Labels; задан только ОДИН из пары (Label без LabelKey или наоборот) → без Labels — неполная пара не пишется вовсе (домены всегда передают пару; смешанный случай встречается только в переносимых тестах — см. шаг 3.6); заменяет хардкод `"pgworker"` — §7.4; `VolumeName/VolumeDest`, `Binds` (vwk `vol:/path`), `Tmpfs`, `ExtraHosts`, `RestartPolicy` (null → `"unless-stopped"` — канон копий), `Network/NetworkAliases`, `CpuCores→NanoCPUs`/`MemoryBytes` — union-ветки из трёх копий;
  - `BuildServiceBody`: pg-канон + vwk-ветки: `Binds` → `Mounts`, `VolumeName/VolumeDest` → `Mounts`, Label-пара (`LabelKey`/`Label` — семантика та же, что в `BuildContainerBody`, вкл. смешанный случай), лимиты (`TaskTemplate.Resources.Limits`), `Env == null` → поле не пишется, `Cmd` + `ResetEntrypoint` → `Entrypoint=[]` (union-семантика как в `BuildContainerBody`).
- [ ] **Шаг 3.6: переносы юнитов движка** — в `src/tests/Shared.Docker.UnitTests/Engine/` (namespace `Shared.Docker.UnitTests.Engine`; FakeHandler переносится как helper):
  - `DockerEngineTests.cs` — из `src/tests/PgWorker.UnitTests/Docker/DockerEngineTests.cs` (FakeHandler-юниты: Factory_UnixEndpoint_HasConnectCallback_TcpHasNot, Ping, ListContainers, CreateContainer×3, RemoveContainer_404, StopContainer, BusyPorts, ListServices, CreateService×2). Ассерты/механика — as-is; НО спеки `ContainerSpec` конструируются в тестах по СТАРОЙ позиционной сигнатуре (DockerEngineTests.cs:103–112, :146–149 — позиционные VolumeName/VolumeDest/Ports/Hostname/CpuCores/MemoryBytes/Label) — при переносе переписать ВСЕ конструкции спеков на именованные аргументы супер-спеки (как в ContainerSpecTests ниже); `Label: "shop"` без `LabelKey` (DockerEngineTests.cs:112) — либо дополнить парой `LabelKey: "pgworker"`, либо опустить Label вовсе: в этих кейсах Labels не ассертуется, оба варианта семантически эквивалентны (без Labels / с Labels — поле никем не проверяется);
  - `DockerEngineFactoryTlsTests.cs` — фабричные кейсы из `src/tests/PgWorker.UnitTests/Docker/DockerTlsOptionsTests.cs`: `Factory_TcpWithTls_ClientCertAndChainCallbackSet`, `Factory_PartialTlsConfig_FailFast`, `Factory_NoTls_PlainTcpHandlerWithoutSslOptions`, `Factory_UnixEndpoint_TlsIgnored`;
  - `DockerEngineExecTests.cs` — из `src/tests/PgWorker.UnitTests/Docker/DockerEngineExecTests.cs` ТОЛЬКО движковые кейсы `ExecAsync_ReturnsStdout`, `ExecAsync_NonZeroExit_Fails` (PlainDriver-кейсы остаются в PgWorker.UnitTests — задача 4; в pg-копии файл потом усекается); конструкции спеков — именованные аргументы (та же оговорка);
  - `ContainerSpecTests.cs` — переработка `src/tests/ValkeyWorker.UnitTests/Docker/ContainerSpecTests.cs` в тесты супер-спеки: `BuildContainerBody_NodeSpec_DefaultUnlessStopped`, `BuildContainerBody_HelperSpec_NoRestart` (as-is по смыслу; спеки — через именованные аргументы супер-спеки) + новые кейсы шага 3.7.
- [ ] **Шаг 3.7: новые кейсы суперсета (§6, §7)** — добавить в `src/tests/Shared.Docker.UnitTests/Engine/`:
  - `DockerEngineTests`: `StartContainer_304AlreadyStarted_ReturnsSuccess` (FakeHandler → 304);
  - `DockerEngineTests`: `CreateContainer_404NoSuchImage_PullsAndRetries` — FakeHandler со сценарным списком ответов (при необходимости расширить FakeHandler call-логом: последовательность `POST /containers/create` → 404 `{"message":"No such image: …"}`, `POST /images/create` → 201, `POST /containers/create` → 201); ассерт: три вызова в логе, итог — успех;
  - `DockerEngineExecTests`: `ExecAsync_NonZeroExit_MessageContainsStdoutAndStderr` (демпд stdout и stderr в ответе exec/json; сообщение Failed содержит оба);
  - `ContainerSpecTests`: `BuildContainerBody_CmdWithResetEntrypoint_SetsEmptyEntrypoint`; `BuildContainerBody_CmdWithoutFlag_CmdAsArguments` (нет ключа Entrypoint); `BuildContainerBody_LabelPair_SetsLabels`; `BuildContainerBody_NoLabel_NoLabelsField`; `BuildContainerBody_LabelWithoutLabelKey_NoLabelsField` (смешанный случай: задан только Label — Labels не пишутся; LabelKey без Label — тот же исход — параметризованный кейс); `BuildContainerBody_EnvNull_EnvFieldOmitted`;
  - `DockerEngineTests` (или отдельный `InspectNodeEndpointTests`): `InspectNodeEndpoint_PortParameter_ResolvesBinding` — инспект-ответ с биндингами `9094/tcp` и `6379/tcp`: вызов с `containerPort: 9094` возвращает порт 9094-биндинга, с `containerPort: 6379` — порт 6379-биндинга; `Running` — из `State.Running`.
- [ ] **Шаг 3.8: проверка**. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj -c Release
```

  Ожидание: build 0 warnings; все юниты `Shared.Docker.UnitTests` зелёные (перенесённые + новые). Юниты доменов не тронуты и зелёные (копии ещё на месте).

- [ ] **Шаг 3.9: коммит**:

```bash
git add src/Shared.Docker src/tests/Shared.Docker.UnitTests
git commit -m "feat(t07): Shared.Docker — union-движок (канон-суперсет §7), фабрика, юниты суперсета"
```

---

### Задача 4: переключение PgWorker на `Shared.Docker`, удаление pg-копии (Фаза C)

**Связь со spec:** §4.4 (перенос потребителей Cmd/Label), §4.7 (env-биндинги в PgWorker.App), §4.8 (csproj pg), §6 (правки тестов pg), §10-C, §12.7 (SSH.NET только у Shared.Docker).
**Вход (предусловие):** задачи 1–3 закоммичены, `Shared.Docker` зелёная.
**Выход (что готово):** pg-домен (Docker-сборка, Backups, App, тесты) компилируется против `Shared.Docker`; каталог `src/PgWorker.Docker/Engine/` не существует; `SSH.NET`/`Shared.Tls` ушли из `PgWorker.Docker.csproj`; build Release 0 warnings; юниты pg + docker-интеграции pg зелёные.

- [ ] **Шаг 4.1: csproj `PgWorker.Docker`** — `src/PgWorker.Docker/PgWorker.Docker.csproj`: добавить `<ProjectReference Include="..\Shared.Docker\Shared.Docker.csproj"/>`; удалить `<PackageReference Include="SSH.NET"/>` и `<ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>`; проверить `<PackageReference Include="Microsoft.Extensions.Configuration"/>` (~строка 13): после удаления Engine/ и переезда env-биндингов в `PgWorker.App` (§4.7) живых потребителей `ConfigurationManager`/`IConfiguration` в `PgWorker.Docker` не остаётся (`grep -rn "ConfigurationManager\|IConfiguration" src/PgWorker.Docker/` → пусто) — ссылку удалить; если grep непуст — оставить и зафиксировать в журнале фазы. Добавить `<Using Include="Shared.Docker"/>` в ItemGroup Using. `InternalsVisibleTo` (UnitTests/IntegrationTests) — без изменений (internal-члены драйвера).
- [ ] **Шаг 4.2: драйвер** — `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (метод `BuildSpec`, ~строка 617): `new ContainerSpec(...)` — позиционные аргументы superClass (Image, Ports, Hostname, далее именованные); `Label: topology.Cluster` → `LabelKey: "pgworker", Label: topology.Cluster`. Удалить `using PgWorker.Docker.Engine` (глобальный Using закрывает). Константу label-ключа — `internal const string LabelKey = "pgworker";` в `ClusterDriver` (одно место домена, §9).
- [ ] **Шаг 4.3: Cmd-пути с Entrypoint-сбросом (grep-сверка выполнена планом)** — точный перечень всех `Cmd:`-спеков pg (найдены grep'ом `grep -rn "Cmd:" src/PgWorker.Backups src/PgWorker.Docker src/tests/PgWorker.IntegrationTests src/tests/PgWorker.UnitTests`):
  - `src/PgWorker.Backups/WalStreamProcess.cs` (~266, `Cmd: WalAgentCommand.Build()`) → добавить `ResetEntrypoint: true`;
  - `src/PgWorker.Backups/Job/VerifyJobSpec.cs` (~38, `Cmd: VerifyJobCommand.Build()`) → `ResetEntrypoint: true`;
  - `src/PgWorker.Backups/Restore/RestoreJobSpec.cs` (~46, `Cmd: RestoreJobCommand.Build()`) → `ResetEntrypoint: true`;
  - `src/PgWorker.Backups/Job/BackupJobSpec.cs` (~39) — `Cmd: null` — НЕ трогать (сброс не нужен);
  - `src/tests/PgWorker.IntegrationTests/Docker/BackupEngineTests.cs` (~66, alpine `sh -c …`) → `ResetEntrypoint: true`;
  - `src/tests/PgWorker.IntegrationTests/Docker/TlsEngineProxyTests.cs` (~76, `["sleep", "5"]`) → `ResetEntrypoint: true`.
  Во всех перечисленных спеках также заменить `Label: cluster` на `LabelKey: "pgworker", Label: cluster`. Аргументы спеков — по сигнатуре супер-спеки (задача 3.2; `Env`/`VolumeName`/`VolumeDest` — именованные).
- [ ] **Шаг 4.4: env-биндинги в `PgWorker.App` (§4.7)** — создать `src/PgWorker.App/DockerEnvBindings.cs` (namespace `PgWorker.App`): перенести из pg-копий `EnvBindings`-массивы + `ApplyEnvOverrides` (те же env-имена `PGW_DOCKER_TLS_*`/`PGW_DOCKER_SSH_*` и те же конфиг-ключи `PgWorker:Docker:Tls:*`/`PgWorker:Docker:Ssh:*`) в один файл — два статических класса или один `internal static class DockerEnvBindings` с методами `ApplyTlsEnvOverrides(ConfigurationManager, Func<string,string?>? getenv = null)` и `ApplySshEnvOverrides(...)`. В `src/PgWorker.App/Program.cs` строки 35–36 заменить на вызовы новых методов (место вызова — до всего остального — сохраняется). `src/PgWorker.App/PgWorker.App.csproj`: добавить `<Using Include="Shared.Docker"/>` (типы `DockerTlsOptions`/`SshTunnelOptions` полей `Options.cs` теперь из Shared — transitively через PgWorker.Docker). `PgWorker.App` KEEPS свой `ProjectReference Shared.Tls` (используется `Api/ApiTlsEndpoints.cs` — не трогать).
- [ ] **Шаг 4.5: Backups и тестовые csproj** — `<Using Include="Shared.Docker"/>` добавить: `src/PgWorker.Backups/PgWorker.Backups.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` (сборка приходит транзитивно через `PgWorker.Docker`). Явные `using PgWorker.Docker.Engine;` в файлах (`WalStreamProcess`, `BackupProcess`, `BackupVerifyProcess`, джоб-спеки, `FakeBackupEngine`, `StubScaleDriver`, docker-интеграции, HealthChecks) — удалить.
- [ ] **Шаг 4.6: удалить pg-копию движка** — `git rm -r src/PgWorker.Docker/Engine/` (6 файлов: DockerEngine.cs, IDockerEngine.cs, EndpointScheme.cs, SshHostConnection.cs, SshTunnelOptions.cs, DockerTlsOptions.cs).
- [ ] **Шаг 4.7: юниты pg**:
  - удалить перенесённые в B: `src/tests/PgWorker.UnitTests/Docker/DockerEngineTests.cs`, `EndpointSchemeTests.cs`;
  - `DockerEngineExecTests.cs` — оставить только `PlainDriver_ExecNode_*` кейсы (движковые удалены — перенесены);
  - `DockerTlsOptionsTests.cs` — удалить (фабричные кейсы перенесены в задачу 3; env-кейс `ApplyEnvOverrides_DockerTlsKeysMapped` переехал — см. следующий пункт);
  - `SshTunnelOptionsTests.cs` — удалить (модельные перенесены, env-кейс — в новый файл);
  - создать `src/tests/PgWorker.UnitTests/Docker/DockerEnvBindingsTests.cs`: два env-маппинг-кейса as-is по смыслу (`ApplyTlsEnvOverrides_DockerTlsKeysMapped`, `ApplySshEnvOverrides_SshKeysMapped`) против `PgWorker.App.DockerEnvBindings` (getenv-инъекция, как в исходных тестах); если `PgWorker.UnitTests` не ссылается на `PgWorker.App` — ссылка уже есть (ProjectReference в csproj присутствует).
- [ ] **Шаг 4.8: интеграционные тесты pg** — namespace-правки (удаление `using PgWorker.Docker.Engine` при глобальном Using): `Docker/DockerDriverTests.cs`, `ExecDriverTests.cs`, `BackupEngineTests.cs`, `SshTunnelEngineTests.cs`, `TlsEngineProxyTests.cs`, `Backups/FakeBackupEngine.cs`, `Backups/WalStreamProcessTests.cs`, `Backups/BackupVerifyProcessTests.cs`, `Backups/RestoreProcessTests.cs`, `Backups/BackupSelfHealTests.cs`, `Etcd/StubScaleDriver.cs`, `Etcd/ShardScaleContractTests.cs`. Спеки `BackupEngineTests.cs:66`/`TlsEngineProxyTests.cs:76` — уже поправлены в шаге 4.3.
- [ ] **Шаг 4.8a: интеграционный кейс суперсета против живого docker (§6)** — в `src/tests/PgWorker.IntegrationTests/Docker/` (файл `DockerDriverTests.cs` или новый `EngineSupersetTests.cs`, паттерн `DockerTrait`-гейта и динамических портов существующих docker-интеграций): кейс `CreateContainer_MissingImage_PullsAndRetries` — create контейнера по несуществующему имени образа (например `<registry>/pgw-t07-no-such-image:missing`, уникальный гарантированно-отсутствующий тег) → движок делает pull (закончится ошибкой pull для несуществующего образа — ассерт: итог Failed с ошибкой pull, НО в docker-логах/поведении виден именно pull-fallback, а не голый 404 create; альтернатива по факту FakeHandler-семантики — позитивный вариант: образ из локального registry `192.168.0.1:5000`, заранее не запуulled на хосте (уникальный тег alpine: запушить нельзя — локальный registry только для зеркалируемых; тогда негативный вариант — канонический). Итоговая форма теста — негативная: pull-fallback вызывается (виден в ошибке «pull access denied/No such image» от pull, не от create), create не падает молча. Полный teardown контейнера — в `finally`.
- [ ] **Шаг 4.9: проверка сборки и юнитов**. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
```

  Ожидание: 0 warnings; юниты pg зелёные.

- [ ] **Шаг 4.10: docker-интеграции pg (контур фазы)**. Перед серией: `dev-stand/images/pull-images.sh`. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter FullyQualifiedName~Docker
```

  Ожидание: серия зелёная (FakeHandler-часть — юнитами; docker-файлы: BackupEngineTests, DockerDriverTests, ExecDriverTests, SshTunnelEngineTests, TlsEngineProxyTests). После финальной строки — зачистка (стандартный блок, см. «Блок зачистки» в задаче 8).

- [ ] **Шаг 4.11: коммит**:

```bash
git add -A
git commit -m "refactor(t07): PgWorker на Shared.Docker — драйвер/бэкапы/env-биндинги, копия движка удалена"
```

---

### Задача 5: переключение KafkaWorker, advertised-чтение в драйвер, удаление kfw-копии (Фаза D)

**Связь со spec:** §4.6.1 (advertised → драйвер), §4.6.2 (NodeLimits в Shared), §4.8 (csproj kfw), §7.4/§7.5 (label kafkaworker, advertised-источник), §10-D.
**Вход (предусловие):** задача 4 закоммичена, pg-контур зелёной.
**Выход (что готово):** kfw-домен компилируется против `Shared.Docker`; `KafkaWorker.Core` использует `Shared.Docker.NodeLimits`; advertised-пара читается драйвером из `NodeEnvAsync`; каталог `src/KafkaWorker.Docker/Engine/` не существует; build + юниты kfw зелёные.

- [ ] **Шаг 5.1: csproj** — `src/KafkaWorker.Docker/KafkaWorker.Docker.csproj`: +`<ProjectReference Include="..\Shared.Docker\Shared.Docker.csproj"/>`, +`<Using Include="Shared.Docker"/>`. `src/KafkaWorker.Core/KafkaWorker.Core.csproj`: +`<ProjectReference Include="..\Shared.Docker\Shared.Docker.csproj"/>`, +`<Using Include="Shared.Docker"/>` (транзитивно подтянет SSH.NET — осознанно, §4.6.2: потребители NodeLimits переключаются на Shared-тип).
- [ ] **Шаг 5.2: `NodeLimits`** — `src/KafkaWorker.Core/Planning/NodeRegenPlanner.cs`: удалить строку `public sealed record NodeLimits(long NanoCpus, long MemoryBytes);` (тип приходит из `Shared.Docker`, поля идентичны — замена прозрачна для `NeedsRegen`, `NodeRegenerator`, тестов `NodeRegenPlannerTests`/`NodeRegeneratorTests`/`Fakes.cs`).
- [ ] **Шаг 5.3: драйвер kfw** — `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs`:
  - обе спеки (`new ContainerSpec(...)` ~167 и ~402): Label-пара → `LabelKey: "kafkaworker", Label: spec.Cluster`; аргументы — по супер-спеке (Env, VolumeName/VolumeDest — именованные). Константа `internal const string LabelKey = "kafkaworker";`;
  - `InspectNodeEndpointAsync` драйвера (Plain ~310–324, Swarm ~449–462): движковый вызов теперь `engine.InspectNodeEndpointAsync(name, ClientContainerPort /* 9094 */, ct)`; `NodeEndpointInspection(host, found.ClientHostPort, advertised)` — advertised НЕ из движка: перенести приватный парсер `ReadAdvertisedClient` (kfw-движок, ~строка 329: разбор env `KAFKA_ADVERTISED_LISTENERS` → пара CLIENT) в драйвер и заполнять `AdvertisedClient` значением из `NodeEnvAsync(cluster, nodeName)` (движковый `InspectContainerEnvAsync` под капотом; логика чтения env у драйверного `NodeEnvAsync` уже есть). **AdvertisedClient заполняется из `NodeEnvAsync` ТОЛЬКО в Plain-ветке; в Swarm-ветке остаётся `null` — как сегодня** (движковый swarm-фолбэк env шаблона не читал: `DockerNodeEndpoint(PublishedPort, null, Host)` — «env шаблона не читаем — сверка advertised — plain-only», kfw-движок :308; swarm-драйвер пробрасывал null, :461; хотя swarm-`NodeEnvAsync` реально возвращает env сервиса, включение его в advertised было бы седьмым изменением поведения вне §7 — не делаем). Доменная запись `NodeEndpointInspection` (Host, ClientHostPort, AdvertisedClient) — без изменений. Значение AdvertisedClient в Plain — то же (§7.5: источник меняется, значение нет).
- [ ] **Шаг 5.4: App и фабрика** — `src/KafkaWorker.App/Program.cs`: `new DockerEngineFactory()` валиден (опции optional); проверить using (глобальный Using добавить в `src/KafkaWorker.App/KafkaWorker.App.csproj`); `src/KafkaWorker.App/HealthChecks/ServiceProbes.cs` — убрать `using KafkaWorker.Docker.Engine`.
- [ ] **Шаг 5.5: удалить kfw-копию** — `git rm src/KafkaWorker.Docker/Engine/DockerEngine.cs src/KafkaWorker.Docker/Engine/IDockerEngine.cs`.
- [ ] **Шаг 5.6: тесты kfw** — `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj`, `src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`: +`<Using Include="Shared.Docker"/>`; файлы `App/HealthTests.cs`, `Kafka/KafkaClusterFixture.cs`, `Kafka/KafkaClientChurnTests.cs` — убрать using доменного движка. Если тесты фикстур напрямую инстанцируют kfw-фабрику — заменить на `Shared.Docker.DockerEngineFactory` (API идентичен: `Create(endpoint, hostAlias?)`).
- [ ] **Шаг 5.7: проверка**. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release
```

  Ожидание: 0 warnings; юниты kfw (вкл. Planning) зелёные. (Полные kfw-интеграции — фаза G.)

- [ ] **Шаг 5.8: коммит**:

```bash
git add -A
git commit -m "refactor(t07): KafkaWorker на Shared.Docker — NodeLimits из Shared, advertised в драйвере, копия удалена"
```

---

### Задача 6: переключение ValkeyWorker, конверсия лимитов, удаление vwk-копии (Фаза E)

**Связь со spec:** §4.6.2 (vwk-конверсия nanoCpus→cores), §4.8 (csproj vwk), §7.4 (label valkeyworker), §10-E.
**Вход (предусловие):** задача 5 закоммичена.
**Выход (что готово):** vwk-домен компилируется против `Shared.Docker`; драйвер конвертирует лимиты; каталог `src/ValkeyWorker.Docker/Engine/` не существует; build + юниты vwk зелёные.

- [ ] **Шаг 6.1: csproj** — `src/ValkeyWorker.Docker/ValkeyWorker.Docker.csproj`: +ProjectReference `Shared.Docker`, +Using. `<Using Include="Shared.Docker"/>` также: `src/ValkeyWorker.App/ValkeyWorker.App.csproj`, `src/ValkeyWorker.Provisioning/ValkeyWorker.Provisioning.csproj`, `src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj`, `src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj`.
- [ ] **Шаг 6.2: драйвер vwk** — `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`:
  - спеки (~164 и ~368): супер-спека — `Cmd` (обязательный, БЕЗ `ResetEntrypoint` — vwk-args это аргументы `docker-entrypoint.sh`, флаг false по умолчанию), `Binds` (TLS-volume `vol:/path`), `Env` не передаётся (null), `RestartPolicy`; Label-пара → `LabelKey: "valkeyworker", Label: spec.Cluster`; константа `internal const string LabelKey = "valkeyworker";`;
  - `NodeResourcesAsync` (~225): движок возвращает `Shared.Docker.NodeLimits(long NanoCpus, long MemoryBytes)` → конвертация в доменный `ValkeyWorker.Core NodeLimits(decimal? CpuCores, long? MemoryBytes)`: `cores = nanoCpus == 0 ? null : (decimal)nanoCpus / 1_000_000_000m` (0 = без лимита → null; сверить с текущим поведением vwk-движка §«0 = без лимита» — сохранить семантику значений, которые сегодня получает `NodeSupervisor.LimitsMatch`/`ProvisioningProcess.LimitsMatch`);
  - инспекции endpoint/Cmd: движковый `InspectNodeEndpointAsync(name, 6379, ct)` (порт 6379 — константа домена);
  - helper-контейнеры volume-archive — код движка (Shared), спеки helper'ов БЕЗ Label/LabelKey — как сегодня.
- [ ] **Шаг 6.3: App/Provisioning** — `src/ValkeyWorker.App/Program.cs` (`new DockerEngineFactory()` валиден), `HealthChecks/ServiceProbes.cs`, `src/ValkeyWorker.Provisioning/Processes/NodeTlsProvisioner.cs` — убрать using доменного движка.
- [ ] **Шаг 6.4: удалить vwk-копию** — `git rm -r src/ValkeyWorker.Docker/Engine/` (4 файла: DockerEngine.cs, DockerEngineFactory.cs, IDockerEngine.cs, TarArchive.cs).
- [ ] **Шаг 6.5: тесты vwk** — удалить перенесённые в задачу 2/3: `src/tests/ValkeyWorker.UnitTests/Docker/TarArchiveTests.cs`, `ContainerSpecTests.cs`. Остались по месту (namespace-правки): `SwarmClusterDriverTlsTests.cs` (в т.ч. фабрика из Shared), `NodeTlsProvisionerTests.cs`, `ProvisioningProcessTests.cs`, `App/HealthTests.cs`, интеграционные `ValkeyClusterFixture.cs`, `ProvisioningTests.cs`, `TlsMigrationTests.cs`, `Api/MetricsDomainTests.cs`.
- [ ] **Шаг 6.6: юнит-кейс конверсии лимитов (§9)** — в `src/tests/ValkeyWorker.UnitTests/Docker/` (или рядом с драйвер-тестами) добавить кейс драйверной конверсии через `NodeResourcesAsync` с Fake-движком (паттерн существующих драйвер-тестов vwk): движковый `NodeLimits(1_500_000_000, 2_000_000_000)` → доменный `NodeLimits(CpuCores: 1.5m, MemoryBytes: 2_000_000_000)`; `NodeLimits(0, 0)` → cores null («0 = без лимита»); ожидаемые значения сверить с текущими ассертами `ResourcesAutorecreateTests`/`SupervisionTests`.
- [ ] **Шаг 6.7: проверка**. Проверка:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj -c Release
```

  Ожидание: 0 warnings; юниты vwk зелёные.

- [ ] **Шаг 6.8: коммит**:

```bash
git add -A
git commit -m "refactor(t07): ValkeyWorker на Shared.Docker — конверсия лимитов в драйвере, копия удалена"
```

---

### Задача 7: чистка — grep-гейты остатков, сверка slnx/CPM (Фаза F)

**Связь со spec:** §3.7 (мёртвый код не оставляем), §9 (осколки копий), §10-F, §12.1, §12.7.
**Вход (предусловие):** задачи 2–6 закоммичены, все три домена на `Shared.Docker`.
**Выход (что готово):** в `src/` ровно одна копия движка; осиротевших using/namespace нет; slnx и CPM консистентны. Коммит чистки (если есть правки).

- [ ] **Шаг 7.1: grep-гейт дедупликации (§12.1)**. Проверка:

```bash
grep -rEn "class DockerEngine\b|interface IDockerEngine|class DockerEngineFactory|class SshHostConnection|record EndpointScheme|class TarArchive|class DockerHttpException|record ContainerSpec|record NodeLimits" src/ --include="*.cs"
```

  Ожидание: определения ТОЛЬКО в `src/Shared.Docker/` (для `NodeLimits` допустимы: `ValkeyWorker.Core/Model/ValkeyDomain.cs` — доменная запись осталась по §4.6.2; kfw-потребители — использования, не определения). Плюс:

```bash
test ! -d src/PgWorker.Docker/Engine && test ! -d src/KafkaWorker.Docker/Engine && test ! -d src/ValkeyWorker.Docker/Engine && echo OK
```

- [ ] **Шаг 7.2: grep-гейт namespace-остатков**. Проверка:

```bash
grep -rn "PgWorker\.Docker\.Engine\|KafkaWorker\.Docker\.Engine\|ValkeyWorker\.Docker\.Engine" src/ --include="*.cs" --include="*.csproj"
```

  Ожидание: 0 совпадений.

- [ ] **Шаг 7.3: grep-гейт зависимостей (§12.7)**. Проверка:

```bash
grep -rln "SSH.NET" src/ --include="*.csproj"
```

  Ожидание: только `src/Shared.Docker/Shared.Docker.csproj` (запись версии — в `Directory.Packages.props`, без изменений). Также `grep -rn "Shared.Tls" src/PgWorker.Docker/` → пусто (осталась у `Shared.Docker` и `PgWorker.App` — для `ApiTlsEndpoints`).

- [ ] **Шаг 7.4: сверка slnx/CPM** — `src/PgWorker.slnx`: `Shared.Docker` в `/common/`, `Shared.Docker.UnitTests` в `/tests/`; ни один csproj не содержит `Version=` на PackageReference (CPM, `EnablePackageVersionOverride=false`); контроль шага 4.1 — в `PgWorker.Docker` нет кода `ConfigurationManager`/`IConfiguration` при отсутствии ссылки (или ссылка оставлена осознанно — тогда потребитель виден grep'ом). Проверка:

```bash
grep -rn 'PackageReference.*Version=' src/ --include="*.csproj"
grep -A2 'PackageReference Include="Microsoft.Extensions.Configuration"' src/PgWorker.Docker/PgWorker.Docker.csproj || echo "ссылки нет — OK (шаг 4.1)"
grep -rn "ConfigurationManager\|IConfiguration" src/PgWorker.Docker/ || echo "потребителей нет — OK"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```

  Ожидание: первый grep пуст; связка ссылка/потребители консистентна; build 0 warnings.

- [ ] **Шаг 7.5: коммит** (только если были правки):

```bash
git add -A && git commit -m "chore(t07): чистка остатков после унификации движка"
```

---

### Задача 8: верификация и мерж-гейт — серии, docker-E2E трёх доменов, образы (Фаза G)

**Связь со spec:** §10-G, §11.1–11.5, §12.2–12.5.
**Вход (предусловие):** задачи 1–7 закоммичены; grep-гейты чисты.
**Выход (что готово):** все серии §11 зелёные: build Release 0 warnings → юнит-серии → интеграции ×3 (docker) → полный docker-E2E Pg+Kfw+Valkey на свежем Release → три образа собираются, compose-config валиден.

**Блок зачистки (после КАЖДОЙ docker-серии — дождаться финальной строки прогона):**

```bash
docker ps -aq --filter name=pgw- --filter name=kfw- --filter name=vwk- | xargs -r docker rm -f
docker volume ls -q | grep -E '^(pgw-|kfw-|vwk-)' | xargs -r docker volume rm || true
docker network prune -f
docker system df   # контрольный взгляд: без накоплений
```

  Контейнеры/сети поднятого dev-станда не трогаем (фильтры только по префиксам доменов; `network prune` удаляет лишь неиспользуемые сети).

- [ ] **Шаг 8.1: подготовка образов** — `dev-stand/images/pull-images.sh` (внешние образы из `192.168.0.1:5000`).
- [ ] **Шаг 8.2: build Release**. Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 errors, 0 warnings.
- [ ] **Шаг 8.3: юнит-серии** (по очереди, после каждой — финальная строка; юниты docker не поднимают, зачистка — контрольная):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Core.UnitTests/Shared.Core.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Etcd.UnitTests/Shared.Etcd.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release   # дым: панель не тронута
```

  Ожидание: все зелёные. Таймаут/зависание — каноны AGENTS.base.md §11 (анализ логов, не перезапуск).

- [ ] **Шаг 8.4: интеграционные docker-серии ×3** (между сериями — блок зачистки; BrokerBootSec ≤ 100 с):
  1. PgWorker полные (вкл. Docker-серию): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` → зачистка;
  2. KafkaWorker полные (живые брокеры, `KafkaClusterFixture`): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj -c Release` → зачистка;
  3. ValkeyWorker полные: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj -c Release` → зачистка;
  4. AdminPanel дым: build + юниты (8.3) достаточно, конфигурации не менялись.
  Ожидание: все серии зелёные. Упавший сценарий: `MarkFailed()`-телеметрия по `docs/e2e-launch.md` (логи в `/tmp/pgw-e2e-artifacts-<guid>/`), разбор по логам, перезапуск — только после анализа причин (AGENTS.md).

- [ ] **Шаг 8.5: docker-E2E на свежем Release (roadmap-гейт задачи, §11.4)**:
  1. PgWorker — полный `E2eFixture` (собирает Release сам):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2e
```

  (фильтр покрывает классы `E2e*Scenarios` + `E2eAutoBuildTests`; кейс-маркер `Scale_AddEmptyShard` входит в `E2eScaleScenarios`) → зачистка;
  2. KafkaWorker — docker-E2E kfw-домена = полный docker-прогон Kafka-серии (шаг 8.4.2; если 8.4.2 уже зелёный на финальном коде — отдельный повтор не нужен, зафиксировать в журнале фазы);
  3. ValkeyWorker — E2E: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj -c Release --filter FullyQualifiedName~ValkeyE2e` (полный `ValkeyE2eLifecycleTests`; vwk-интеграции — шаг 8.4.3) → зачистка.
  Ожидание: все зелёные; `docker network ls | grep -cE 'kfw-net|pgw.*net'` после зачистки — 0 (осиротевших сетей нет).

- [ ] **Шаг 8.6: образы и compose (§11.5)**. Проверка:

```bash
docker compose -f deploy/docker-compose.yml build pgworker kafkaworker valkeyworker
docker compose -f deploy/docker-compose.yml config >/dev/null && echo CONFIG_OK
```

  Ожидание: `pgworker:dev`, `kafkaworker:dev`, `valkeyworker:dev` собираются; config валиден. Образы в registry `192.168.0.1:5000` НЕ пушить.
- [ ] **Шаг 8.7: коммит фазы** (если остались незакоммиченные правки по итогам серий):

```bash
git add -A && git commit -m "test(t07): мерж-гейт — серии юнитов/интеграций/E2E трёх доменов зелёные"
```

---

### Задача 9: мерж-коммит — снятие roadmap-тега (закрытие задачи)

**Связь со spec:** §11.6, §12.6; AGENTS.md «Roadmap — только несделанные задачи».
**Вход (предусловие):** задача 8 полностью зелёная; мерж в `main` приказан пользователем отдельным приказом (без приказа — стоп).
**Выход (что готово):** мерж-коммит в `main` включает удаление пункта `t07-unify-docker-engine` из `arch/roadmap/pgworker.md` (строки ~18–23) и из `←`-зависимостей других пунктов (если упомянут; проверка: `grep -rn "t07-unify-docker-engine" arch/roadmap/` → 0). История задачи — `docs/superpowers/2026-09-20-t07-unify-docker-engine/`.

- [ ] **Шаг 9.1:** перед мержем: `grep -rn "t07-unify-docker-engine" arch/ roadmap docs/ 2>/dev/null` — фиксируем все места; правка `arch/roadmap/pgworker.md` готовится в мерж-коммит (НЕ отдельным).
- [ ] **Шаг 9.2:** мерж в `main` по приказу пользователя; тем же коммитом — удаление roadmap-пункта. Никаких пометок «закрыта/реализована» — только удаление тега.

---

## Чек-лист приёмки (spec §12 — сверка перед мержем)

1. Дедупликация: grep §12.1 — определения движковых типов только в `Shared.Docker`; каталоги `*/Docker/Engine/` трёх воркеров не существуют (задачи 4–7).
2. `dotnet build src/PgWorker.slnx -c Release` — 0 warnings (задачи 2–8).
3. Все серии §11(2–4) зелёные, вкл. полный docker-E2E трёх доменов (задача 8).
4. Изменения поведения — только шесть пунктов §7; интеграции прошли без правок ожиданий, кроме осознанных (label-ключ, advertised-источник) — задачи 4–6.
5. Три образа собираются; deploy/dev-stand конфигурации не менялись (задача 8.6, `git diff --stat deploy/ dev-stand/` — пусто).
6. arch/14 §2.2/§2.2.1, arch/16 §2.5, arch/21 §2 обновлены (задача 1); roadmap-тег снят мерж-коммитом (задача 9).
7. `SSH.NET` — только у `Shared.Docker` (задача 7.3).

## Примечания исполнителю

- Порядок задач обязателен (arch → каркас → движок → pg → kfw → vwk → чистка → гейты): каждый следующий шаг опирается на предыдущий, домены переключаются по одному, чтобы падение сборки локализовалось в одном домене.
- Переносы «as-is» означают: код и комментарии копируются без изменений, кроме namespace/using. Любая поведенческая правка — только из списка §7 (шаг 3.5). Исключение — переносимые ТЕСТЫ: конструкции `ContainerSpec` по старой позиционной сигнатуре переписываются на именованные аргументы супер-спеки (шаг 3.6) — это необходимость компиляции, не поведенческая правка.
- `TreatWarningsAsErrors`: осиротевший `using` — ошибка компиляции; при правке файла удаляйте using доменного движка сразу.
- Связывание типов в потребителях — через глобальный `<Using Include="Shared.Docker"/>` в csproj ПОТРЕБИТЕЛЯ (Using в csproj не транзитивен): полный список csproj с Using — шаги 4.1/4.5, 5.1/5.4/5.6, 6.1.
- Если интеграционный/E2E-прогон упал: телеметрия и разбор по `docs/e2e-launch.md`; перезапуск упавших тестов без анализа логов и согласия пользователя ЗАПРЕЩЁН (AGENTS.md).
