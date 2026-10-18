# t31-vwk-contour-network — план реализации (контурный режим сетей VWK; S3W fail-fast Swarm; стенд = ядро + панель)

> **Для исполнителей (агентных):** план исполняется по шагам (checkbox-трекинг ниже).
> Каждый ШАГ исполняет кодер (сильная модель); механическая часть шага выделена в
> ПОДПУНКТЫ — самодостаточные задания для слабой модели (coder-flash, без контекста
> проекта: получает только текст подпункта и путь к файлу). Не выделенное в подпункты
> (ревью результатов подпунктов, исправления, отладка, скрипты сценариев, документация)
> делает кодер сам.

**Цель:** опция `ValkeyWorker:Docker:ContourNetwork` (контурный режим сетей vwk-нод по
образцу PgWorker/S3Worker), fail-fast старта `Mode=Swarm`+`ContourNetwork` для
ValkeyWorker и S3Worker, стенд `00-up.sh` поднимает только ядро и панель (без
сидирования ресурсов), новый ручной чек `52-valkey-contour.sh`.

**Архитектура:** опция приезжает в App-Options → нормализуется/валидируется в
Program.cs (fail-fast ApplicationException по канону PgWorker) → прокладывается срезом
`ValkeyProvisioningOptions.ContourNetwork` в 4 процесса → доезжает параметром
`EnsureNodeAsync(spec, contourNetwork, ct)` до docker-драйвера → `ContainerSpec.Network`
только при создании нового контейнера (идемпотентность прежняя). Advertised-правило и
portalloc НЕ меняются. Стендовые compose задают сеть константой стендовой сети;
deploy-шаблон — через общий `PGW_CONTOUR_NETWORK` (пусто = прод).

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit +
FluentAssertions, docker compose, bash-чеки стенда.

**Spec:** `docs/superpowers/2026-10-18-t31-vwk-contour-network/spec.md` (работа в
worktree; план аргументирует от spec — исполнители читают оба).

**Корень исполнения (worktree):** `/Users/demakaev/ZCodeProject/worktrees/feat-t31-vwk-contour-network` —
все пути ниже относительны этого корня. Все команды выполнять из него.

## Глобальные ограничения

- ⛔ **ПРИОРИТЕТ №1: E2E не меняется ВООБЩЕ.** Запрещено править/создавать/удалять:
  файлы каталогов `src/tests/*/IntegrationTests/E2e/`, env-словари E2E-окружений,
  `src/tests/PgWorker.E2ePrepare/**`, скрипты/команды/фильтры запуска E2E-серий;
  проверка контурного режима в E2E НЕ добавляется. Ни один шаг плана не запускает
  и не модифицирует E2E. Проверка кода воркеров — юнит/интеграционные тесты (не
  E2E) и сборка.
- Arch-правки (§3.1 spec: `arch/21`, `arch/24`, `arch/adminpanel/04`, плюс
  `arch/roadmap/valkey.md`) уже лежат в worktree незакоммиченными — НЕ описывать их
  как шаги кода; закоммитить вместе с задачей (Шаг 10).
- Bash-чеки стенда `05-seed.sh` и `10`–`90` НЕ менять; `05-seed.sh` не расширяется
  ветками `pg`/`s3` и не удаляется. Новые проверки — только новым скриптом
  `52-valkey-contour.sh`.
- KafkaWorker — вне скоупа. PgWorker (код/deploy/arch) — эталон, не трогаем.
- Никакого сидирования ресурсов на стенде: `00-up.sh` ничего не наливает; ресурсы —
  только вручную через панель.
- Комментарии/документация — по-русски; идентификаторы — английские; тесты — с
  AAA-комментариями.
- Локально собираемые образы в registry не класть; `owns3:dev` — внешний образ.
- Никаких новых штатных операций сверх arch/21, arch/24, arch/adminpanel/04.
- Docker-зачистка после КАЖДОЙ тестовой серии; между сериями не запускать ничего
  поверх незачищенной (каноны AGENTS.md).

## Review Focus (классы отказов, не покрытые спецификацией напрямую)

1. **Повторный ensure ноды при заданной сети не пересоздаёт существующий контейнер**
   (сеть применяется только к новому) — тест
   `Plain_EnsureNode_ExistingContainer_NotRecreated_WithContourNetwork` (Шаг 1).
2. **`Mode` регистронезависим** (`"swarm"` тоже fail-fast) — Theory-кейсы
   `"swarm"` в тестах guard (Шаги 2, 3).
3. **Пустая/пробельная сеть = прежнее поведение** (`Network == null`, guard не
   кидает) — тесты `Plain_EnsureNode_NoContourNetwork_NetworkNull` (Шаг 1) и
   `OtherCombinations_DoNotThrow` (Шаг 2).
4. **Смена сигнатуры `EnsureNodeAsync` не ломает интеграционные VWK-тесты**
   (передан `null` → контейнер в default bridge, поведение прежнее) — прогон
   Шага 8.
5. **Прод без `PGW_CONTOUR_NETWORK`**: интерполяция пустая → env пуст →
   default bridge — config-гейт Шага 4 (grep пустого значения без env-файла).

---

### Шаг 1. ValkeyWorker: контурный режим сети нод (опция → срез → драйвер → тесты)

**Вход:** worktree чист по коду (только arch-правки незакоммичены); образцы изучены:
`S3Worker.App/Options.cs` (свойство `ContourNetwork`), `S3Worker.Docker/Drivers/ClusterDriver.cs`
(`EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct)`,
`Network: contourNetwork` в `ContainerSpec`), `ContainerSpec` уже имеет
`string? Network = null` (`src/Shared.Docker/Engine/IDockerEngine.cs`).

**Файлы:**
- Modify: `src/ValkeyWorker.App/Options.cs` (свойство `DockerOptions.ContourNetwork`)
- Modify: `src/ValkeyWorker.Provisioning/Processes/ProcessCommon.cs` (параметр среза)
- Modify: `src/ValkeyWorker.App/Program.cs` (`ToProvisioningOptions` — прокладка+нормализация)
- Modify: `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс, Plain, Swarm)
- Modify: `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs`, `NodeSupervisor.cs`, `CaRotator.cs`, `TlsMigrator.cs` (4 call-site)
- Modify: `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` (фейк-драйвер)
- Modify: `src/tests/ValkeyWorker.IntegrationTests/Valkey/CaRotationTests.cs`, `TlsMigrationTests.cs` (вызовы; интеграционные, НЕ E2e-каталог)
- Modify/Create-тесты: `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverEnvTests.cs`

**Interfaces (производит):**
- `ValkeyProvisioningOptions(..., int RotationTicketTimeoutSec = 3600, string? ContourNetwork = null)`
- `IClusterDriver.EnsureNodeAsync(ValkeyNodeSpec spec, string? contourNetwork, CancellationToken ct)`
- `DockerOptions.ContourNetwork` (`string?`)

**Действие (кодер):** выполняет подпункты 1.1–1.11, ревьюит каждую правку после
исполнения (сверка с образцами S3Worker: сигнатура, `Network: contourNetwork`,
комментарии), затем запускает проверку. Идемпотентность Plain (`existing.Value.Any(c
=> c.Names.Contains(name))` → return БЕЗ create) — НЕ менять: сеть доезжает только в
`ContainerSpec` нового контейнера. Комментарий интерфейса обновить (сеть контура —
только новому контейнеру; null — default bridge; воркер сеть не создаёт/не удаляет).

**Выход:** контурная сеть опцией доезжает до `ContainerSpec.Network` нового контейнера
VWK-ноды; все проекты VWK компилируются; юнит-тесты сети зелёные.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj -c Release
```
Ожидание: сборка без warning-ошибок (`TreatWarningsAsErrors=true`), все тесты PASS, в
т.ч. новые `Plain_EnsureNode_ContourNetwork_SetsNetwork`,
`Plain_EnsureNode_NoContourNetwork_NetworkNull`,
`Plain_EnsureNode_ExistingContainer_NotRecreated_WithContourNetwork`.

**Связь со spec:** §3.2 (кроме fail-fast валидации — она в Шаге 2), критерий №2,
фаза 2.

**Подпункты (слабой модели):**

- **1.1.** В файле `src/ValkeyWorker.App/Options.cs`, в классе `DockerOptions`
  (объявлен как `public sealed class DockerOptions`), после свойства
  `public DockerImagesOptions Images { get; set; } = new();` добавь свойство с
  XML-комментарием:
  ```csharp
      /// <summary>Стенд/E2E: ноды создаются в сети контура поставки; сеть воркером
      /// не создаётся/не удаляется; пусто — default bridge docker-хоста запуска.
      /// Не поддерживается в Mode=Swarm — fail-fast старта (arch/21 §2).</summary>
      public string? ContourNetwork { get; set; }
  ```

- **1.2.** В файле `src/ValkeyWorker.Provisioning/Processes/ProcessCommon.cs` найди
  объявление записи (строка ~24):
  ```csharp
  public sealed record ValkeyProvisioningOptions(
      int PortRangeFrom,
      int PortRangeTo,
      int NodeBootSec,
      int NodeDeadSec,
      string? AdvertisedClientHost,
      string NodeImage,
      int RotationTicketTimeoutSec = 3600)
  ```
  Замени закрывающую строку `int RotationTicketTimeoutSec = 3600)` на две строки:
  ```csharp
      int RotationTicketTimeoutSec = 3600,
      string? ContourNetwork = null)
  ```
  Статическое свойство `Default` ниже не меняй (без нового аргумента = null).

- **1.3.** В файле `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`:
  1) В интерфейсе `IClusterDriver` замени строку (строка ~59)
     `Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct);` на:
     ```csharp
         // Идемпотентно создать ноду (plain: контейнер vwk-<C>-node<k>; swarm:
         // сервис с constraint node.id==<id>, publish mode=host). contourNetwork —
         // сеть контура поставки/сценария: применяется ТОЛЬКО к новому контейнеру
         // plain-режима (null — default bridge docker-хоста запуска); воркер сеть
         // не создаёт и не удаляет.
         Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, string? contourNetwork, CancellationToken ct);
     ```
  2) В классе `PlainClusterDriver` замени сигнатуру (строка ~154)
     `public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)`
     на
     `public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, string? contourNetwork, CancellationToken ct)`.
     В теле этого же метода найди инициализатор `var containerSpec = new ContainerSpec(`
     и строку `Env: spec.Env);` (последний аргумент) — замени её на две строки:
     ```csharp
                 // TLS-материал — env контейнера (VALKEY_TLS_{CERT,KEY,CA}).
                 Env: spec.Env,
                 // Сеть контура: null — default bridge; только при создании
                 // (существующий контейнер не трогается).
                 Network: contourNetwork);
     ```
     (исходный комментарий `// TLS-материал — env контейнера (VALKEY_TLS_{CERT,KEY,CA}).`
     перед `Env: spec.Env` уже есть — не дублируй его, просто добавь запятую после
     `Env: spec.Env` и строки `Network: contourNetwork);` с комментарием.)
  3) В классе `SwarmClusterDriver` замени сигнатуру (строка ~357)
     `public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)`
     на
     `public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, string? contourNetwork, CancellationToken ct)`
     и первой строкой тела метода (перед `return await Result.FromAsync(...)`) вставь
     комментарий:
     ```csharp
             // contourNetwork не используется: сеть overlay назначает сам Swarm
             // (Mode=Swarm + ContourNetwork отсечён fail-fast старта, arch/21 §2).
     ```

- **1.4.** В файле `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs`
  найди вызов (~строка 331):
  ```csharp
              var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
                  cluster, node, address.Host, address.ClientPort, options.NodeImage, cmd,
                  limits?.Cpu, limits?.MemBytes,
                  Env: env), ct);
  ```
  Замени последнюю строку на: `Env: env), options.ContourNetwork, ct);`

- **1.5.** В файле `src/ValkeyWorker.Provisioning/Processes/NodeSupervisor.cs` найди
  вызов (~строка 334), заканчивающийся строкой
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(snap.CaPem!, snap.CaKey!, node, advertised)), ct);` —
  замени её на
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(snap.CaPem!, snap.CaKey!, node, advertised)), options.ContourNetwork, ct);`

- **1.6.** В файле `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs` найди
  вызов (~строка 210), заканчивающийся строкой
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(nextPem, nextKey, "node1", advertised)), ct);` —
  замени её на
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(nextPem, nextKey, "node1", advertised)), options.ContourNetwork, ct);`

- **1.7.** В файле `src/ValkeyWorker.Provisioning/Processes/TlsMigrator.cs` найди
  вызов (~строка 182), заканчивающийся строкой
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(
      creds.CaPem, creds.CaKey, "node1", options.AdvertisedClientHost ?? address.Host)), ct);` —
  замени её на
  `Env: NodeTlsProvisioner.BuildNodeTlsEnv(
      creds.CaPem, creds.CaKey, "node1", options.AdvertisedClientHost ?? address.Host)), options.ContourNetwork, ct);`

- **1.8.** В файле `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` найди метод
  (строка ~285) `public Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)`
  и замени его сигнатуру на
  `public Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, string? contourNetwork, CancellationToken ct)`.
  Тело метода не меняй.

- **1.9.** В файлах интеграционных тестов (НЕ трогай больше ничего в них):
  1) `src/tests/ValkeyWorker.IntegrationTests/Valkey/CaRotationTests.cs` — два
     вызова `fx.Driver.EnsureNodeAsync(new(...), ct)`: первый (~строка 41)
     заканчивается `1m, 1024L * 1024 * 1024), ct));` → замени на
     `1m, 1024L * 1024 * 1024), null, ct));`; второй (~строка 175) заканчивается
     `Env: NodeTlsProvisioner.BuildNodeTlsEnv(
         nextPem, nextKey, "node1", ValkeyClusterFixture.AdvertisedClientHost)), ct);` →
     добавь `null, ` перед `ct);` (итог: `...AdvertisedClientHost)), null, ct);`).
  2) `src/tests/ValkeyWorker.IntegrationTests/Valkey/TlsMigrationTests.cs` — два
     вызова: первый (~строка 42) заканчивается `1m, 1024L * 1024 * 1024),
            ct);` → замени на `1m, 1024L * 1024 * 1024),
            null, ct);`; второй (~строка 114) заканчивается
     `1m, 1024L * 1024 * 1024),
            ct)).IsSuccess.Should().BeTrue();` → вставь `null, ` той же правкой
     (итог: `1m, 1024L * 1024 * 1024),
            null, ct)).IsSuccess.Should().BeTrue();`).

- **1.10.** В файле `src/ValkeyWorker.App/Program.cs` найди функцию (внизу файла):
  ```csharp
  static ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions ToProvisioningOptions(ValkeyWorkerOptions opts) => new(
      opts.Docker.PortRange.From,
      opts.Docker.PortRange.To,
      opts.Thresholds.NodeBootSec,
      opts.Thresholds.NodeDeadSec,
      opts.AdvertisedClientHost,
      opts.Docker.Images.Node,
      opts.Thresholds.RotationTicketTimeoutSec);
  ```
  Последнюю строку `opts.Thresholds.RotationTicketTimeoutSec);` замени на:
  ```csharp
      opts.Thresholds.RotationTicketTimeoutSec,
      // Контурная сеть нод (arch/21 §2): пусто → null — прежнее поведение.
      opts.Docker.ContourNetwork is { Length: > 0 } ? opts.Docker.ContourNetwork : null);
  ```

- **1.11.** В файле `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverEnvTests.cs`:
  1) В существующем тесте `Plain_EnsureNode_EnvReachesContainerSpec` замени строку
     `var result = await driver.EnsureNodeAsync(Spec(SampleEnv), CancellationToken.None);`
     на `var result = await driver.EnsureNodeAsync(Spec(SampleEnv), null, CancellationToken.None);`
  2) В существующем тесте `Swarm_EnsureNode_EnvReachesServiceTemplate` замени строку
     `var result = await driver.EnsureNodeAsync(Spec(SampleEnv), CancellationToken.None);`
     на `var result = await driver.EnsureNodeAsync(Spec(SampleEnv), null, CancellationToken.None);`
  3) После теста `Plain_EnsureNode_EnvReachesContainerSpec` добавь три новых теста:
     ```csharp
         // AAA: Plain — контур-сеть (стенд/сценарий) доезжает до Network спецификации
         // нового контейнера.
         [Fact]
         public async Task Plain_EnsureNode_ContourNetwork_SetsNetwork()
         {
             // Arrange — контейнера нет; движок-дублёр запоминает spec создания.
             var engine = new StubEngine();
             var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

             // Act
             var result = await driver.EnsureNodeAsync(Spec(SampleEnv), "contour", CancellationToken.None);

             // Assert
             result.IsSuccess.Should().BeTrue();
             engine.CreatedSpec!.Network.Should().Be("contour");
         }

         // AAA: Plain — контурная сеть не задана (прод/умолчание) — прежнее
         // поведение: Network=null (default bridge docker-хоста запуска).
         [Fact]
         public async Task Plain_EnsureNode_NoContourNetwork_NetworkNull()
         {
             // Arrange
             var engine = new StubEngine();
             var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

             // Act
             var result = await driver.EnsureNodeAsync(Spec(SampleEnv), null, CancellationToken.None);

             // Assert
             result.IsSuccess.Should().BeTrue();
             engine.CreatedSpec!.Network.Should().BeNull();
         }

         // AAA: Plain — идемпотентность: существующий контейнер НЕ пересоздаётся
         // (контурная сеть применяется только к новому контейнеру).
         [Fact]
         public async Task Plain_EnsureNode_ExistingContainer_NotRecreated_WithContourNetwork()
         {
             // Arrange — контейнер vwk-c1-node1 уже есть (running).
             var engine = new StubEngine
             {
                 Containers = [new DockerContainer("id1", ["vwk-c1-node1"], "running", "valkey/valkey:9.1.2")],
             };
             var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

             // Act
             var result = await driver.EnsureNodeAsync(Spec(SampleEnv), "contour", CancellationToken.None);

             // Assert — create не вызван (CreatedSpec остался null).
             result.IsSuccess.Should().BeTrue();
             engine.CreatedSpec.Should().BeNull();
         }
     ```

---

### Шаг 2. ValkeyWorker: fail-fast валидации старта (Swarm + ContourNetwork)

**Вход:** Шаг 1 принят (юниты VWK зелёные). Образец текста исключения — PgWorker
(`src/PgWorker.App/Program.cs`): `"PgWorker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers"`.

**Файлы:**
- Modify: `src/ValkeyWorker.App/Options.cs` (guard-метод на `DockerOptions`)
- Modify: `src/ValkeyWorker.App/Program.cs` (вызов guard в фабрике драйвера)
- Create: `src/tests/ValkeyWorker.UnitTests/App/OptionsContourNetworkTests.cs`

**Interfaces (производит):** `DockerOptions.EnsureContourNetworkMode()` — `void`,
кидает `ApplicationException` при `Mode`==Swarm (регистронезависимо) и непустой
(не whitespace) `ContourNetwork`.

**Действие (кодер):** выполняет подпункты 2.1–2.3, ревьюит (текст исключения — точно
по образцу PgWorker с префиксом `ValkeyWorker:`; место вызова — DI-фабрика
`IClusterDriver` рядом с существующими ApplicationException-проверками SwarmManager/Hosts),
запускает проверку.

**Выход:** старт воркера с `Mode=Swarm` + непустой `ValkeyWorker:Docker:ContourNetwork`
падает fail-fast с диагностическим сообщением; юнит-тест guard зелёный.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj -c Release --filter FullyQualifiedName~OptionsContourNetworkTests
```
Ожидание: PASS (все тесты нового файла), затем полный прогон юнитов VWK без
регрессий.

**Связь со spec:** §3.2 (валидация fail-fast), §2 (принцип Swarm), критерий №4 (VWK),
фаза 2.

**Подпункты (слабой модели):**

- **2.1.** В файле `src/ValkeyWorker.App/Options.cs`, в классе `DockerOptions`,
  после свойства `public DockerImagesOptions Images { get; set; } = new();` (и
  после добавленного ранее свойства `ContourNetwork`, если оно добавлено последним)
  добавь метод:
  ```csharp
      /// <summary>Fail-fast старта (arch/21 §2): ContourNetwork не поддерживается
      /// в Mode=Swarm — сеть overlay назначает сам Swarm (канон PgWorker).</summary>
      public void EnsureContourNetworkMode()
      {
          if (!string.IsNullOrWhiteSpace(ContourNetwork)
              && string.Equals(Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
              throw new ApplicationException(
                  "ValkeyWorker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
      }
  ```

- **2.2.** В файле `src/ValkeyWorker.App/Program.cs` найди фабрику драйвера — блок,
  начинающийся со строки `builder.Services.AddSingleton<IClusterDriver>(sp =>`; в
  нём строки:
  ```csharp
      var docker = sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Docker;
      var factory = sp.GetRequiredService<DockerEngineFactory>();
      if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
  ```
  Между строкой `var factory = ...` и `if (string.Equals(docker.Mode, "Swarm"...`
  вставь:
  ```csharp
      // Контурная сеть (arch/21 §2): Mode=Swarm + ContourNetwork — ошибка
      // конфигурации (сеть overlay назначает сам Swarm) — fail-fast.
      docker.EnsureContourNetworkMode();
  ```

- **2.3.** Создай файл `src/tests/ValkeyWorker.UnitTests/App/OptionsContourNetworkTests.cs`
  с содержимым:
  ```csharp
  using FluentAssertions;
  using ValkeyWorker.App;
  using Xunit;

  namespace ValkeyWorker.UnitTests.App;

  // Fail-fast старта (arch/21 §2): ContourNetwork не поддерживается в Mode=Swarm —
  // сеть overlay назначает сам Swarm (канон PgWorker).
  public class OptionsContourNetworkTests
  {
      [Theory]
      [InlineData("Swarm")]
      [InlineData("swarm")]
      public void Swarm_WithContourNetwork_Throws(string mode)
      {
          // Arrange — swarm (в любом регистре) + контурная сеть: ошибка конфигурации.
          var docker = new DockerOptions { Mode = mode, ContourNetwork = "adminpanel-stand-net" };

          // Act
          var act = () => docker.EnsureContourNetworkMode();

          // Assert
          act.Should().Throw<ApplicationException>().WithMessage(
              "ValkeyWorker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
      }

      [Theory]
      [InlineData("Swarm", null)]
      [InlineData("Swarm", "")]
      [InlineData("Swarm", "   ")]
      [InlineData("Plain", "adminpanel-stand-net")]
      public void OtherCombinations_DoNotThrow(string mode, string? contourNetwork)
      {
          // Arrange — пустая сеть в Swarm и любая в Plain — валидные комбинации.
          var docker = new DockerOptions { Mode = mode, ContourNetwork = contourNetwork };

          // Act/Assert
          docker.EnsureContourNetworkMode();
      }
  }
  ```

---

### Шаг 3. S3Worker: fail-fast валидации старта (Swarm + ContourNetwork)

**Вход:** Шаг 2 принят. Механизм валидаций S3W — чистые методы
`src/S3Worker.App/S3StartupChecks.cs` (`internal static partial class`,
`InternalsVisibleTo S3Worker.UnitTests` уже в csproj), Program применяет их в
`.Validate(...).ValidateOnStart()`; в DI-фабрике `IS3ClusterDriver` комментарий
«fail-fast на противоречивой конфигурации уже в S3StartupChecks.DockerTopology».
Существующие тесты валидаций — `src/tests/S3Worker.UnitTests/App/OptionsValidationTests.cs`.

**Файлы:**
- Modify: `src/S3Worker.App/S3StartupChecks.cs` (guard-метод)
- Modify: `src/S3Worker.App/Program.cs` (вызов в фабрике `IS3ClusterDriver`)
- Modify: `src/tests/S3Worker.UnitTests/App/OptionsValidationTests.cs` (тест)

**Interfaces (производит):** `S3StartupChecks.EnsureContourNetworkMode(DockerOptions docker)`
— `void`, кидает `ApplicationException` при Mode==Swarm (регистронезависимо) и
непустой `Docker.ContourNetwork` (`string?`, уже существует в
`S3Worker.App/Options.cs`).

**Действие (кодер):** выполняет подпункты 3.1–3.3, ревьюит (текст — по образцу
PgWorker с префиксом `S3Worker:`; вызов — в фабрике драйвера рядом с
`var factory = ...`, до `if (Swarm)`), запускает проверку.

**Выход:** старт S3Worker с `Mode=Swarm` + непустой `S3Worker:Docker:ContourNetwork`
падает fail-fast; юнит-тест зелёный; юниты S3W без регрессий.

**Проверка:**
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests/S3Worker.UnitTests.csproj -c Release
```
Ожидание: все PASS, включая новые `ContourNetwork_SwarmMode_FailFast`,
`ContourNetwork_PlainOrEmpty_NoFailFast`.

**Связь со spec:** §3.3, §2 (принцип Swarm), критерий №4 (S3W), фаза 3.

**Подпункты (слабой модели):**

- **3.1.** В файле `src/S3Worker.App/S3StartupChecks.cs`, после метода
  `public static bool PortRange(S3WorkerOptions o)` (конец класса), добавь метод:
  ```csharp
      // Контурная сеть (arch/24 §2): не поддерживается в Mode=Swarm — сеть
      // overlay назначает сам Swarm (fail-fast старта, канон PgWorker).
      public static void EnsureContourNetworkMode(DockerOptions docker)
      {
          if (!string.IsNullOrWhiteSpace(docker.ContourNetwork)
              && string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
              throw new ApplicationException(
                  "S3Worker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
      }
  ```

- **3.2.** В файле `src/S3Worker.App/Program.cs` найди фабрику — блок
  `builder.Services.AddSingleton<S3Worker.Docker.Drivers.IS3ClusterDriver>(sp =>`; в
  нём строки:
  ```csharp
      var docker = sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Docker;
      var factory = sp.GetRequiredService<DockerEngineFactory>();
      if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
  ```
  Между `var factory = ...` и `if (string.Equals(docker.Mode, "Swarm"...` вставь:
  ```csharp
      // Контурная сеть (arch/24 §2): Mode=Swarm + ContourNetwork — ошибка
      // конфигурации (сеть overlay назначает сам Swarm) — fail-fast старта.
      S3StartupChecks.EnsureContourNetworkMode(docker);
  ```

- **3.3.** В файле `src/tests/S3Worker.UnitTests/App/OptionsValidationTests.cs`, в
  конец класса `OptionsValidationTests` (после последнего теста
  `PortRange_OrderAndBounds`) добавь два теста:
  ```csharp
      [Theory]
      [InlineData("Swarm")]
      [InlineData("swarm")]
      public void ContourNetwork_SwarmMode_FailFast(string mode)
      {
          // Arrange — swarm (в любом регистре) + контурная сеть: ошибка конфигурации.
          var o = Valid();
          o.Docker.Mode = mode;
          o.Docker.ContourNetwork = "adminpanel-stand-net";

          // Act
          var act = () => S3StartupChecks.EnsureContourNetworkMode(o.Docker);

          // Assert
          act.Should().Throw<ApplicationException>().WithMessage(
              "S3Worker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
      }

      [Fact]
      public void ContourNetwork_PlainOrEmpty_NoFailFast()
      {
          // Arrange — Plain с сетью и Swarm без сети: валидные комбинации.
          var plain = Valid();
          plain.Docker.ContourNetwork = "adminpanel-stand-net";
          var swarmEmpty = Valid();
          swarmEmpty.Docker.Mode = "Swarm";
          swarmEmpty.Docker.SwarmManager = "tcp://mgr:2376";
          swarmEmpty.Docker.ContourNetwork = "";

          // Act/Assert
          S3StartupChecks.EnsureContourNetworkMode(plain.Docker);
          S3StartupChecks.EnsureContourNetworkMode(swarmEmpty.Docker);
      }
  ```

---

### Шаг 4. Deploy и стендовый compose: env контурной сети VWK

**Вход:** Шаги 1–3 приняты. В `deploy/docker-compose.yml` у якоря `x-s3worker-env`
уже есть образец: `S3Worker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}` с
комментарием; у `x-valkeyworker-env` (строки ~165–177) такой строки нет. В
`dev-stand/adminpanel/docker-compose.yml` сервисы `valkeyworker` / `valkeyworker-2`
(профиль `valkey`) env-блоков с ContourNetwork не имеют.

**Файлы:**
- Modify: `deploy/docker-compose.yml` (якорь `x-valkeyworker-env`)
- Modify: `dev-stand/adminpanel/docker-compose.yml` (сервисы `valkeyworker`, `valkeyworker-2`)

**Действие (кодер):** выполняет подпункты 4.1–4.2, ревьюит (симметрия со строкой
S3Worker в том же файле; значение стендового compose — константа
`adminpanel-stand-net`, БЕЗ интерполяции deploy-`.env`), запускает проверку.

**Выход:** deploy-шаблон прокидывает `ValkeyWorker__Docker__ContourNetwork` из
общего `PGW_CONTOUR_NETWORK` (пусто в проде); стендовые сервисы VWK задают
`adminpanel-stand-net` константой.

**Проверка:**
```bash
# deploy: env-переменная имеет приоритет над .env (интерполяция не зависит от
# значения PGW_CONTOUR_NETWORK в .env). Пререквизит: deploy/.env существует —
# файл несёт остальные обязательные :?-секреты (PGW_PG_SUPERUSER_PASSWORD,
# S3W_BUCKET, OWNS3_* …), без него compose config падает на интерполяции
# (создаётся 00-up.sh Шага 9; на этом хосте стенд поднимался).
cd deploy && PGW_CONTOUR_NETWORK=adminpanel-stand-net docker compose -f docker-compose.yml --env-file .env config \
  | grep -i 'adminpanel-stand-net'
# ожидание: ValkeyWorker__Docker__ContourNetwork: adminpanel-stand-net

# прод-гейт (Review Focus №5): литерал интерполяции с пустым дефолтом ":-" (прод сеть не указывает)
cd deploy && docker compose -f docker-compose.yml config --no-interpolate \
  | grep -n 'ValkeyWorker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}'
# ожидание: строка найдена (пустой дефолт); интерполяция прод-конфига даёт пустое значение

# стендовый compose: оба сервиса несут константу
cd dev-stand/adminpanel && docker compose --profile valkey config | grep -c 'ValkeyWorker__Docker__ContourNetwork: adminpanel-stand-net'
# ожидание: 2
```

**Связь со spec:** §3.4, фаза 4; ограничение №6 (прод не указывает сеть).

**Подпункты (слабой модели):**

- **4.1.** В файле `deploy/docker-compose.yml`, в блоке `x-valkeyworker-env:
  &valkeyworker-env` (строки ~165–177) найди строку
  `  ValkeyWorker__AdvertisedClientHost: ${VWK_ADVERTISED_CLIENT_HOST:-host.docker.internal}`
  и сразу ПОСЛЕ неё вставь:
  ```yaml
    # Сеть контура (симметрия PgWorker/S3Worker, arch/21 §2): задана → контейнеры
    # vwk-нод создаются в ней одной (стенд — adminpanel-stand-net); пусто —
    # прод-режим (default bridge docker-хоста запуска).
    ValkeyWorker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}
  ```

- **4.2.** В файле `dev-stand/adminpanel/docker-compose.yml` — в ОБОИХ сервисах
  (`valkeyworker` и `valkeyworker-2`, профиль `valkey`): в блоке `environment:`
  каждого найди строку `ValkeyWorker__AdvertisedClientHost: host.docker.internal`
  и сразу после неё вставь две строки:
  ```yaml
        # Контурный режим сетей (arch/21 §2): ноды vwk-* — в стендовой сети контура.
        ValkeyWorker__Docker__ContourNetwork: adminpanel-stand-net
  ```
  (обе вставки с одинаковой степенью отступа, как у соседних env-строк сервиса).

---

### Шаг 5. 00-up.sh: стенд = ядро + панель, без сидирования

**Вход:** Шаг 4 принят. Текущий `dev-stand/adminpanel/checks/00-up.sh` (528 строк)
содержит наливку демо/сидов/owns3-фаз. Канон нового поведения —
`arch/adminpanel/04-local-stand.md` §1/§2.2/§3 (строка таблицы `00-up.sh`: ядро +
панель на пустом контуре) — УЖЕ в worktree.

**Файлы:**
- Modify: `dev-stand/adminpanel/checks/00-up.sh`

**Действие (кодер сам — правка требует понимания связей скрипта; подпунктов нет):**
1. **Шапка** (комментарий строки 2–11): переписать по-новому: подъём = ГАРАНТИРОВАННАЯ
   зачистка контура (90-down.sh -v) → publish-фазы образов ([PHASE]: SdGenerator,
   wal-агент, S3Worker; остальные — compose build) → стендовый compose
   (профили kafka + valkey + metrics) → deploy-контур contour-override
   (воркеры, БЕЗ owns3) → wait-on-healthy (etcd-кворум, healthz/lease-ключи
   воркеров ×2, панель, прометей). Никакие ресурсы не создаются и никакие сиды
   не наливаются — ресурсы создаёт оператор вручную через панель.
2. **Удалить ЦЕЛИКОМ** (без переезда в другие скрипты), по маркерам:
   - fail-fast проверку образа `owns3:dev` (блок `docker image inspect owns3:dev …`
     с echo «image owns3:dev missing»);
   - блок healthz ownS3 `:9010` (чтение `OWNS3_PORT`, цикл healthz, строка
     «owns3 alive»);
   - блок bucket etcd-слепков (чтение `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD`,
     `docker run … minio/mc … mc mb … pgworker-etcd-snapshots`, строка
     «bucket pgworker-etcd-snapshots ready»);
   - весь блок «1c) Канонический demo» — от комментария `# 1c)` до строки
     `echo "  demo: Active, nodes RUNNING, bucket status keys removed (shard1/shard2 × a/b)"`
     включительно: POST `/api/clusters`, весь wait-гейт конвергенции
     (`demo_ready`/`conv_*`/детектор стагнации/`conv_fail_diag` и связанные
     функции `patroni_rest_port`/`demo_*`), [PHASE]-строки этого блока;
   - весь блок «7c-2) Сид пула бэкапов» — от `# 7c-2)` до строки
     `echo "  backups pool: node1 RUNNING, endpoints=…"` включительно;
   - весь блок «7d) valkey-сид» — от `# 7d)` до строки
     `"$PWD/checks/05-seed.sh" valkey` включительно.
3. **Профиль `full` убрать из подъёма**: строка `echo ">>> bringing up the stand…"`
   и цикл `docker compose --profile full --profile kafka --profile valkey
   --profile metrics up -d --build` → без `--profile full` (в echo тоже).
4. **deploy-контур без owns3**: строка `… up -d --build --force-recreate pgworker
   pgworker-2 owns3 s3worker s3worker-2` → `… pgworker pgworker-2 s3worker
   s3worker-2`; комментарий блока — без упоминания owns3.
5. **Сопроводительные комментарии**: в блоке 1b убрать фразу про «demo создаётся
   ниже (1c, POST /api/clusters)»; комментарий блока PGW_BACKUP_S3_PROFILES —
   убрать упоминание «сида пула (00-up.sh)» (профиль остаётся: env-механика
   сервисов ядра); комментарии шапки про «bucket-сид ownS3 → POST /api/clusters»
   исчезают с шапкой (п.1). Блоки deploy/.env-механики (PGW_METRICS_NETWORK/
   PGW_SCRAPE_NETWORK, S3W_BUCKET + S3W_NODE_SECRETS, PGW_BACKUP_S3_PROFILES,
   порты, pgw-s3-profiles.env/s3w-node-secrets.env, удаление
   PgWorker__Loops__ScanIntervalSec) — ОСТАЮТСЯ как есть.
6. **Финальную строку** заменить на:
   `echo "✓ stand is up (core + panel; resources are created manually via the panel)"`.
7. Нумерация/комментарии оставшихся блоков (7, 7b, 7c, 8, 9) — можно оставить
   как есть, если ссылки на удалённые блоки в них не упоминаются (в 7b ссылка
   «их ждут панель (WorkerEndpoints) и чек 51» — валидна, остаётся).

**Выход:** `00-up.sh` поднимает только ядро и панель; наливка/ сиды / owns3-фазы /
профиль full из скрипта удалены; сохранённые фазы (зачистка, publish, TLS, .env,
etcd-кворум, healthz/lease ×2 воркеров, панель, прометей) не тронуты.

**Проверка:**
```bash
bash -n dev-stand/adminpanel/checks/00-up.sh   # синтаксис
grep -nE 'owns3:dev|owns3 did not come alive|pgworker-etcd-snapshots|demo_ready|conv_mark|conv_fail_diag|POST /api/clusters|POST /api/s3/clusters|05-seed|--profile full|OWNS3_PORT|OWNS3_ROOT' dev-stand/adminpanel/checks/00-up.sh || echo "OK: seeding gone"
grep -n 'core + panel' dev-stand/adminpanel/checks/00-up.sh   # новая финальная строка
grep -cE 'PGW_METRICS_NETWORK|S3W_NODE_SECRETS|pgw-api-tls|90-down' dev-stand/adminpanel/checks/00-up.sh  # сохранённые фазы на месте (>=4)
```
Ожидание: `OK: seeding gone`; финальная строка найдена; счётчик сохранённых фаз ≥ 4.
(Запуск стенда — Шаг 9; в этом шаге скрипт не выполняется.)

**Связь со spec:** §3.5 (00-up.sh), §1 п.3, критерий №5, фаза 5; ограничение №2.

---

### Шаг 6. README стенда: ядро + панель, ресурсы вручную

**Вход:** Шаг 5 принят; канон — `arch/adminpanel/04-local-stand.md` (правленый, в
worktree): §1 (профили, «00-up.sh поднимает ТОЛЬКО ядро и панель», env грани
«Хранилище бэкапов» → `http://s3w-backups-node1:9000`), §2.2 (ресурсы — только
вручную через панель; 05-seed.sh — не часть штатного подъёма), §2.4 (валkey-сид —
вручную/чеком 51), §3 (таблица чеков, вкл. `52-valkey-contour.sh`), §5 (быстрый старт).

**Файлы:**
- Modify: `dev-stand/adminpanel/README.md`

**Действие (кодер сам — редактура текста по канону arch; подпунктов нет).** Сверяясь
с `arch/adminpanel/04-local-stand.md`, отразить:
1. **Быстрый старт**: стенд = ядро + панель на пустом контуре (одна сеть
   `adminpanel-stand-net`, один etcd-кластер as-etcd-1/2/3); ресурсы (PG-кластеры,
   S3-пулы, valkey/kafka демо) создаются вручную через панель; сиды чеков —
   `checks/05-seed.sh` вручную.
2. **Профили**: строка `full` — as-minio поднимается вручную (в полный подъём
   `00-up.sh` НЕ входит); строки kafka/valkey/metrics — «входит в 00-up.sh»
   остаются.
3. **«Хранилище бэкапов»**: bucket `pgworker-backups` больше не создаётся
   `00-up.sh` (mc-налив удалён); грань панели до создания пула через панель —
   configured с health-отказом (endpoint `s3w-backups-node1:9000` отвечает после
   провижининга пула); standalone-`owns3` — ручной подъём при необходимости
   (проверка — чек 46 после ручного подъёма).
4. **demo-кластер**: абзац «demo-кластер — канонический: 00-up.sh создаёт…» —
   заменить: кластеры создаются вручную через панель (API воркеров); стенд задаёт
   `PGW_CONTOUR_NETWORK`=`adminpanel-stand-net` (режим сети контура движков
   pg/vwk/s3w — ноды в одной сети со всем контуром).
5. **«Сиды через API воркеров»**: убрать «00-up.sh зовёт 05-seed.sh valkey…»;
   `05-seed.sh` — не часть штатного подъёма (зовут чеки 50/51 и ручные прогоны);
   остальное про идемпотентность/совместимость — остаётся.
6. **E2E-раздел**: «демо создаёт 00-up (POST /api/clusters); сиды — внутри…» —
   заменить: полный прогон чеков `10`–`60` — после ручного создания ресурсов через
   панель (демо-кластер/пул) и/или сидов `05-seed.sh`; порядок и «повторный прогон
   с чистого состояния» — остаются.
7. **Новый чек**: добавить строку-описание `checks/52-valkey-contour.sh` (ручная
   приёмка контурного режима сетей ValkeyWorker: создание кластера через панель →
   контейнер в `adminpanel-stand-net`, endpoints advertised host:port → удаление,
   чистота) — рядом со строкой чека 51/по стилю списка чеков README.
8. Существующие описания чеков 05/10–90 не менять.

**Выход:** README зеркалит новый порядок стенда; упоминаний «00-up.sh создаёт
demo/наливает/зовёт 05-seed.sh» не осталось.

**Проверка:**
```bash
grep -nE '00-up\.sh (создаёт|зовёт)|bucket .* создаёт `?00-up' dev-stand/adminpanel/README.md || echo "OK: stale claims gone"
grep -c '52-valkey-contour' dev-stand/adminpanel/README.md   # ожидание: >= 1
grep -c 'вручную через панель' dev-stand/adminpanel/README.md  # ожидание: >= 2
```

**Связь со spec:** §3.5 (README), фаза 5.

---

### Шаг 7. Новый чек 52-valkey-contour.sh (ручная приёмка контурного режима)

**Вход:** Шаги 4–6 приняты (стендовый compose задаёт VWK
`adminpanel-stand-net`; 00-up поднимает ядро+панель). Паттерны — чек
`checks/51-valkey-api.sh` (логин cookie, `api()`/`code()`-хелперы, etcd-хелперы
`etcd_key`/`etcd_has` через `docker compose exec -T etcd1 etcdctl`, RunTag-имя,
wait-циклы, бюджеты, чистота после delete) и строка канона в
`arch/adminpanel/04-local-stand.md` §3.

**Файлы:**
- Create: `dev-stand/adminpanel/checks/52-valkey-contour.sh`

**Действие (кодер сам — сценарная логика приёмки; подпунктов нет).** Написать
самодостаточный самоочищающийся bash-чек (исполняемый, `set -euo pipefail`,
`cd "$(dirname "$0")/.."`) по сценарию:
1. Гварды: панель жива (`$BASE/api/healthz`, цикл ≤ 60 c); ядро живо — lease-ключи
   valkeyworker ×2 (`etcd get /valkeyworker/api/ --prefix --keys-only | grep -c .` ≥ 2).
2. Логин (`POST /api/auth/login`, admin/admin, cookie JAR; хелперы `api()`/`code()`
   по образцу 51).
3. Создание valkey-кластера с УНИКАЛЬНЫМ именем через панель: TAG=`v52$(date +%s)`,
   `POST /api/valkey/clusters` (тело — как create в 51: maxmemory/policy/resources),
   ожидание 201.
4. Wait: кластер Active + нода RUNNING через панель (`GET /api/valkey/clusters/$TAG`,
   jq `.state == "ACTIVE" and .nodesList[0].state == "RUNNING"`; бюджет — тики
   воркера, по образцу 51: ≤ 150 × 2 c).
5. Ассерты контурного режима:
   - `docker inspect vwk-$TAG-node1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}'`
     содержит `adminpanel-stand-net`;
   - `etcd_key "/valkey/clusters/$TAG/endpoints"` — непусто, формат
     `host:port` (portalloc; `*:*` шаблон, значение вида `host.docker.internal:17xxx`
     — без хардкода порта, только формат).
6. Teardown при ЛЮБОМ исходе (`trap … EXIT` после создания кластера): `DELETE
   /api/valkey/clusters/$TAG` (если создан), wait исчезновения контейнера
   `vwk-$TAG-*` и ключей `/valkey/clusters/$TAG/` (по образцу 51: цикл ≤ 150 × 2 c).
7. Финальные ассерты чистоты (после wait): ни контейнера, ни ключей; успех-строка
   `✓ 52-valkey-contour: contour network mode green (node in adminpanel-stand-net, advertised host:port, no traces)`.
Чек НЕ вызывает 00-up/05-seed и никем автоматически не вызывается (ручная приёмка).
Комментарии — по-русски, ассерты — по канонам чеков.

**Выход:** новый скрипт существует, синтаксически валиден, соответствует канону
таблицы чеков arch/adminpanel/04 §3.

**Проверка:**
```bash
bash -n dev-stand/adminpanel/checks/52-valkey-contour.sh && echo "OK: syntax"
test -x dev-stand/adminpanel/checks/52-valkey-contour.sh || chmod +x dev-stand/adminpanel/checks/52-valkey-contour.sh
grep -c 'adminpanel-stand-net' dev-stand/adminpanel/checks/52-valkey-contour.sh  # ожидание: >= 1
```
(Реальный прогон — Шаг 9.)

**Связь со spec:** §3.6, критерии №3/№5 (частично), фаза 5; ограничение №3.

---

### Шаг 8. Прогоны: сборка + юниты + интеграция VWK/S3W (НЕ E2E) с зачисткой серий

**Вход:** Шаги 1–7 приняты (код скомпилирован на юнит-прогонах; стенд-файлы
подготовлены, но не запускались).

**Действие (кодер сам):**
1. Полная сборка решения (гейт `TreatWarningsAsErrors`):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release`.
2. Юнит-серии (по очереди, после каждой — финальная строка прогона):
   VWK UnitTests → S3W UnitTests.
3. Интеграционные серии (docker; ПО ОЧЕРЕДИ, не параллельно; после КАЖДОЙ —
   финальная строка и зачистка контуров):
   `ValkeyWorker.IntegrationTests` → `S3Worker.IntegrationTests`.
4. Зачистка после каждой серии: остаточные контейнеры/тома/сети серий
   (`docker ps -a --format '{{.Names}}' | grep -E '^(vwk-|s3w-|etcd|owns3)'`,
   `docker network ls | grep -cE '(vwk|s3w|kfw)-.*net'` при нуле контейнеров =
   осиротевшие → `docker network prune -f`; каноны AGENTS.md). Стенд не поднимать
   до конца всех серий (порт-коллизии).
5. Правила запуска: команды в фоне, контроль старта по логам ≤ 15 c, проверка
   отсутствия зависания каждые 2 мин; зависание — разбор по логам, не ожидание.
6. E2E-серии НЕ запускать (ограничение №1 spec).

**Выход:** сборка чистая; юниты VWK/S3W зелёные; интеграция VWK/S3W зелёная
(вкл. исправленные сигнатурой `CaRotationTests`/`TlsMigrationTests`); тестовые
контуры зачищены.

**Проверка:**
```bash
# Переменная PGW_TEST_DOCKER принудительно снята (env -u) — E2e-гейт внутри IntegrationTests-проектов остаётся закрытым (⛔ Ограничение №1 spec).
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
env -u PGW_TEST_DOCKER DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests/ValkeyWorker.UnitTests.csproj -c Release
env -u PGW_TEST_DOCKER DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests/S3Worker.UnitTests.csproj -c Release
env -u PGW_TEST_DOCKER DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj -c Release
env -u PGW_TEST_DOCKER DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests/S3Worker.IntegrationTests.csproj -c Release
# после каждой серии — зачистка; в конце:
docker ps -a --format '{{.Names}}' | grep -E '^(vwk-|s3w-|kfw-|pgw-)' || echo "OK: no leftover engine containers"
```
Ожидание: build — без ошибок/warnings; тесты — все PASS; финальная строка — OK.

**Связь со spec:** фаза 6 (юниты → интеграция), критерий №7 (частично: сборка,
юниты/интеграция), ограничение №1 (E2E не трогаем и не запускаем).

---

### Шаг 9. Ручная приёмка стенда: 00-up.sh (ядро + пустой контур) и чек 52

**Вход:** Шаг 8 принят; docker-хост свободен от тестовых серий; внешние образы
стенда presence — по ранбуку (`dev-stand/images/pull-images.sh` при необходимости).

**Действие (кодер сам):**
1. `cd dev-stand/adminpanel && checks/00-up.sh` — в фоне, мониторинг по
   phase-логу/выводу (правила: старт-контроль ≤ 15 c, проверки каждые 2 мин;
   [PHASE]-метки; отказ — разбор по логам, БЕЗ перезапуска до анализа).
2. После успеха — ассерты критерия №5 (пустой контур ядра):
   - нет контейнеров ресурсов: `docker ps -a --format '{{.Names}}' | grep -E 'owns3|as-minio|^pgw-|^vwk-|^s3w-'` → пусто;
   - ядро живо: etcd×3, панель, kafkaworker×2, valkeyworker×2 (as-valkeyworker*),
     pgworker×2, s3worker×2 (deploy-*), prometheus/grafana/alertmanager/sd-generator;
   - в etcd НЕТ ключей ресурсов: `docker compose exec -T etcd1 etcdctl
     --endpoints=http://localhost:2379 get / --prefix --keys-only | grep -E
     '^/(clusters/|valkey/clusters/|s3/clusters/)'` → пусто (только lease-ключи
     воркеров `/pgworker/api/`, `/kafkaworker/…`, `/valkeyworker/…`,
     `/s3worker/api/`);
   - панель: `curl -fsS http://localhost:5050/api/healthz` → 200;
   - финальная строка прогона содержит «core + panel».
3. `checks/52-valkey-contour.sh` — прогон (фон, мониторинг); ожидание — зелёный
   (контейнер `vwk-v52*` в `adminpanel-stand-net`, endpoints `host:port`, после —
   ни контейнера, ни ключей).
4. Разбор стенда: `checks/90-down.sh -v`; ассерт чистоты (`docker ps -a | grep -E
   '^(as-|deploy-|vwk-|pgw-|s3w-|kfw-)'` → пусто; осиротевшие сети → prune).
   Если чек/подъём упали — teardown останавливает, но сохраняет артефакты для
   разбора (каноны телеметрии); перезапуск — только после полного анализа логов.

**Выход:** стенд поднимается ядром+панелью на пустом контуре; чек 52 зелёный;
стенд разобран и зачищен.

**Проверка:** команды п.2–3 выше — все ассерты зелёные; `52-valkey-contour.sh`
завершается строкой успеха; после `90-down.sh -v` контейнеров/сетей стенда нет.

**Связь со spec:** критерии №3 и №5, фаза 6 (ручная приёмка стенда), §3.6.

---

### Шаг 10. Diff-гейт E2E и коммит задачи

**Вход:** Шаги 1–9 приняты.

**Действие (кодер сам):**
1. Гейт ограничения №1 (в diff НЕТ файлов E2E):
   ```bash
   git diff --name-only "$(git merge-base main HEAD)" -- . | grep -iE 'e2e|e2eprepare' && echo "FAIL: E2E files touched" || echo "OK: no E2E files"
   git status --porcelain | grep -iE 'e2e|e2eprepare' && echo "FAIL: untracked E2E files" || echo "OK: no untracked E2E"
   git status --short
   ```
   При FAIL — остановиться и разобраться (правка/создание E2E-файлов недопустима).
2. Гейт ограничения №3 (стенд-чеки не менялись):
   ```bash
   git diff --name-only "$(git merge-base main HEAD)" -- dev-stand/adminpanel/checks/ \
     | grep -vE 'checks/(00-up|52-valkey-contour)\.sh$' && echo "FAIL: forbidden check touched" || echo "OK: only 00-up.sh and 52 are changed"
   ```
3. Коммит ВСЕХ изменений задачи в feature-ветке (arch-правки §3.1 spec, включая
   `arch/roadmap/valkey.md`, + код + тесты + compose + стенд + план/spec):
   ```bash
   git add -A && git commit -m "feat(vwk): contour network mode; s3w swarm fail-fast; stand = core+panel without seeding (t31)"
   ```
   Мерж в main / пуш — ТОЛЬКО по явной просьбе пользователя (не делать).

**Выход:** изменения задачи закоммичены в feature-ветке; гейты E2E/чеков зелёные.

**Проверка:** обе grep-строки п.1–2 выводят `OK`; `git status --short` пуст после
коммита; `git log -1 --oneline` — коммит задачи.

**Связь со spec:** критерии №1, №6; ограничение №1.

---

## Само-ревью плана (выполнено)

- **Покрытие spec:** §3.2 → Шаги 1–2; §3.3 → Шаг 3; §3.4 → Шаг 4; §3.5 → Шаги 5–6;
  §3.6 → Шаг 7; §3.7 — не требует кода (следствия приняты); фаза 6 → Шаги 8–9;
  ограничения/критерии → Шаги 8–10 (гейты). Arch (§3.1) уже в worktree — кодом не
  описывается, коммитится Шагом 10. Пробелов нет.
- **E2E-запрет:** ни один шаг/подпункт/проверка не правит и не запускает
  E2E-файлы/словари/серии/E2ePrepare; интеграционные правки VWK (`Valkey/`-каталог)
  — не E2e-каталог, требуются компиляцией смены сигнатуры; Шаг 10 гейтит diff.
- **Типы/сигнатуры:** `EnsureNodeAsync(ValkeyNodeSpec, string?, CancellationToken)`
  единообразна во всех правках (интерфейс, Plain, Swarm, 4 процесса, фейк,
  интеграционные, тесты); `ValkeyProvisioningOptions.ContourNetwork` (string?,
  опциональный, в конце записи — все существующие конструкторы-вызовы без него
  валидны); guard-имена `EnsureContourNetworkMode` (S3W static/S3StartupChecks,
  VWK instance/DockerOptions) не конфликтуют.
- **Пропорция:** подпункты несут готовый код только там, где он однозначен
  (механика по образцам); сценарные скрипты (00-up, 52, README) — спецификации
  для сильной модели.
