# pgtune-provision — план реализации (PGTune: расчёт параметров PG и применение при provision нод)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** параметры `postgresql.conf` нод кластеров PgWorker рассчитываются алгоритмом PGTune (ядро `PgTune.Calculate` — чистая функция 1:1 `docs/pgtune-calculation-spec.md`) от характеристик ноды — память/CPU из заявок `/service/<scope>/request_{mem,cpu}`, остальные входы — опции `PgWorker:Pgtune` — и применяются при создании нод (bootstrap Patroni DCS в `SPILO_CONFIGURATION`) вместо сегодняшних жёстких констант; doorman-бюджет синхронизируется: `serverConnections = max(10, max_connections − 5)`. Пересчёт — на каждый EnsureNode-путь, БЕЗ фиксации в etcd (решение пользователя; etcd-контракт не меняется вообще).

**Архитектура:** ядро в `PgWorker.Core/Tuning/PgTune.cs` (без I/O, без NuGet, единица — KB двоичные) → конфигурация `PgtuneOptions`/`PgtuneSettings` (fail-fast старта) → фабрика `PgtuneInputsFactory` (Provisioning, от `NodeResources`) → применение в `SpiloEnvBuilder` (merge: PGTune-вывод ∪ PgWorker-канон, канон перезаписывает; `ExcludeParams` вырезается здесь) и `DoormanConfigBuilder` (бюджет от рассчитанного `max_connections`) → проводка: `IClusterDriver.EnsureNodeAsync` получает `PgTuneResult? tuning`, 4 процесса вычисляют тюнинг per-shard один раз до цикла нод → интеграционные сценарии + docker-E2E гейт.

**Тех-стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), ядро — только BCL; xUnit + FluentAssertions, тесты с AAA-комментариями; новых пакетов нет (`Directory.Packages.props` не меняется).

**Spec:** `docs/superpowers/2026-09-12-pgtune-provision/spec.md` (одобрен). Норматив алгоритма: `docs/pgtune-calculation-spec.md` (§2 входы, §4 формулы, §5 формат, §6 псевдокод, §7 контрольные примеры — тест-векторы один-в-один). Канон: `arch/14-pgworker.md`, `arch/12-bucket-pitfalls.md` (P15). Рабочая директория — worktree `/Users/demakaev/ZCodeProject/worktrees/feat-pgtune-provision`.

## Глобальные ограничения (из spec §2/§6 + AGENTS.md)

- Ядро — чистая функция: без I/O, без NuGet, внутренняя единица KB (двоичные), все `floor`/порядок правил — по §1/§8 спецификации алгоритма; контрольные примеры §7 — обязательные тест-векторы.
- Канон PgWorker перекрывает PGTune: merge = PGTune-вывод ∪ канон-ключи (`wal_level: logical`, `hot_standby`, `sync_replication_slots`, `max_slot_wal_keep_size`, `max_wal_senders: "10"`, `max_replication_slots`, `wal_keep_size`, `checkpoint_timeout` + лог-блок); из канона убираются `max_connections`, `shared_buffers`, `effective_cache_size`, `checkpoint_completion_target`, `random_page_cost` (несёт PGTune).
- `wal_level=minimal`/`max_wal_senders=0` (desktop) не применяются никогда: `DbType=desktop` отвергается валидацией старта.
- Параметры, не выведенные для входа (cpuNum не задан → параллельные/autovacuum/io_workers), в YAML не пишутся вовсе (никаких суррогатных дефолтов).
- Параметры НЕ фиксируются в etcd: новых ключей нет, txn-записей нет, etcd-канала переопределения нет; панель не затрагивается. Переопределение — только через `PgWorker:Pgtune`.
- ExcludeParams применяется в `SpiloEnvBuilder` (spec §4.2), не в фабрике: `Calculate` всегда даёт полный вывод.
- Значения — строки в инвариантной культуре; YAML-значения параметров — в кавычках (текущий стиль `max_connections: "60"`).
- Тесты: AAA-комментарии; docker-порты — только динамические; E2E — по `docs/e2e-isolation.md` (guid-имена, own-only чистка, ассерт чистоты, полный teardown при любом исходе); таймауты фикстур ≤ 100 с.
- После КАЖДОЙ тестовой серии — зачистка: если dev-стенд поднят, фильтрованная форма `docker rm -f $(docker ps -aq --filter "name=pgw-") 2>/dev/null; docker network prune -f` (не трогает `as-*`/`adminpanel`), иначе `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`; следующая серия — только после `docker ps -aq` без своих остатков.
- Все команды `dotnet` — с `DOTNET_CLI_UI_LANGUAGE=en`.
- Язык: комментарии/документация — русский, идентификаторы — английский.
- Коммиты в feature-ветке — свободно, по задачам; правки канона arch/ (Задача 5) коммятся ОДНИМ коммитом с проводкой (Задача 6) — spec §5 п.7.

## Карта файлов

| Файл | Действие | Ответственность |
|---|---|---|
| `src/PgWorker.Core/Tuning/PgTune.cs` | Create | ядро: enums, `PgTuneInput/PgTuneParameter/PgTuneResult`, `PgTune.Calculate/ToPostgresqlConf/ToAlterSystem`, `KnownParameterNames` |
| `src/tests/PgWorker.UnitTests/Tuning/PgTuneTests.cs` | Create | тест-векторы §7 + правила/форматы/валидация |
| `src/PgWorker.App/Options.cs` | Modify | `PgtuneOptions` + `IsValid()` + `ToRuntime()`, поле в `PgWorkerOptions` |
| `src/PgWorker.App/Program.cs` | Modify | `.Validate(o => o.Pgtune.IsValid())`; DI фабрики; exclude в драйверы; фабрика в 4 процесса |
| `src/PgWorker.App/appsettings.json` | Modify | секция `PgWorker:Pgtune` |
| `src/PgWorker.Provisioning/Processes/PgtuneSettings.cs` | Create | runtime-record (паттерн `PlacementOptions`) |
| `src/PgWorker.Provisioning/Processes/PgtuneInputsFactory.cs` | Create | `Create(NodeResources?)` → `PgTuneResult` |
| `src/tests/PgWorker.UnitTests/Provisioning/PgtuneInputsFactoryTests.cs` | Create | floor/fallback/детерминизм + канал «опции → вход ядра → вывод» |
| `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` | Modify | `SpiloEnvBuilder.Build(+tuning,+exclude)` merge; `DoormanConfigBuilder.Build(dbname, serverConnections)` + `ServerConnections(tuning)` |
| `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs` | Modify | merge/exclude/doorman-кейсы |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | Modify | `IClusterDriver.EnsureNodeAsync(+tuning)`, Plain/Swarm, `BuildSpec(+tuning)`, exclude в конструкторах |
| `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` | Modify | P2.1: тюнинг per-shard |
| `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` | Modify | 3 сайта EnsureNode: тюнинг от актуальных заявок |
| `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` | Modify | A3: тюнинг per-shard |
| `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` | Modify | репарация: `Create(null)` |
| `arch/14-pgworker.md` | Modify | §2.1, §5 A P2.1, §5 G A3, §5 C, §5 J, §8 |
| `arch/12-bucket-pitfalls.md` | Modify | P15: вычисляемый бюджет doorman |
| `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`, `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`, `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` | Modify | сигнатуры стабов EnsureNodeAsync |
| `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`, `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs` | Modify | колл-сайты EnsureNodeAsync |
| `src/tests/PgWorker.IntegrationTests/E2e/E2ePgtuneScenarios.cs` | Create | интеграционные сценарии |

---

### Задача 1: Ядро `PgTune.cs` — TDD (тест-векторы §7 первыми, затем реализация)

**Files:**
- Create: `src/PgWorker.Core/Tuning/PgTune.cs`
- Test: `src/tests/PgWorker.UnitTests/Tuning/PgTuneTests.cs`

**Interfaces:**
- Consumes: только BCL (без I/O, без NuGet).
- Produces (для задач 2–6):
  - `enum PgTuneOsType { Linux, Windows, Mac }`, `enum PgTuneDbType { Web, Oltp, Dw, Desktop, Mixed }`, `enum PgTuneHdType { Ssd, San, Hdd, Nvme }`, `enum PgTuneDbSize { LessRam, MidRam, GreaterRam }`, `enum PgTuneMemoryUnit { KB, MB, GB, TB }`;
  - `sealed record PgTuneInput(int DbVersion, PgTuneOsType OsType, PgTuneDbType DbType, long TotalMemory, PgTuneMemoryUnit TotalMemoryUnit, int? CpuNum, int? ConnectionNum, PgTuneHdType HdType, PgTuneDbSize DbSize);`
  - `sealed record PgTuneParameter(string Name, string Value);` — значение строкой §5.1;
  - `sealed record PgTuneResult(IReadOnlyList<PgTuneParameter> Parameters, IReadOnlyList<string> Warnings)` + индексатор `public string? this[string name]` (значение параметра по имени, null если нет; нужен `BuildSpec` для doorman — spec §4.4 использует `tuning["max_connections"]`);
  - `static class PgTune` с `Calculate(PgTuneInput): PgTuneResult`, `ToPostgresqlConf(PgTuneInput, PgTuneResult): string`, `ToAlterSystem(PgTuneInput, PgTuneResult): string` и `public static IReadOnlyCollection<string> KnownParameterNames` (25 имён из §5.2 — для валидации `ExcludeParams` в Задаче 2).

**Решение по границе KB-канала (выводится из spec, зафиксировать в комментарии ядра):** §2 спецификации алгоритма ограничивает `totalMemory ≤ 999999` в человеко-единицах инструмента (MB/GB/TB — ограничение входа PGTune-калькулятора). KB в `PgTuneMemoryUnit` — внутренний канал фабрики (spec.md §4.3: `TotalMemoryKb = floor(MemoryBytes/1024)`, дефолт 8 GiB = 8 388 608 KB > 999 999), а spec.md §6 требует, чтобы память > 100 GB давала только предупреждение и не блокировала расчёт. Следовательно: для `unit = KB` применяется нижняя граница ≥ 1 (иначе расчёт невозможен), верхний числовой кап §2 НЕ применяется (кап — свойство человеко-единиц); для `MB` — ≥ 512, для `GB`/`TB` — ≥ 1, верхний кап `≤ 999999` — для всех трёх. Альтернативное прочтение (кап 999 999 на число KB) отвергает дефолт 8 GiB самого spec.md — несостоятельно.

- [x] **Шаг 1: скелет + падающие тесты (red)**

  - Вход: worktree `feat-pgtune-provision`, ветка чиста (только spec/plan); `PgWorker.Core` собирается.
  - Действие:
    - Создать `src/PgWorker.Core/Tuning/PgTune.cs`: enums/records (сигнатуры выше) + `static class PgTune`, методы бросают `NotImplementedException`; `KnownParameterNames` — сразу полный список 25 имён §5.2.
    - Создать `src/tests/PgWorker.UnitTests/Tuning/PgTuneTests.cs` (AAA-комментарии) — полный набор векторов:
      1. **Пример 1** (§7): вход `(15, Linux, Web, 4, GB, 4, 300, Ssd, MidRam)` — построчное равенство блока «заголовок + параметры» (`# DB Version: 15` … `max_parallel_maintenance_workers = 2`); `Warnings` равен `["WARNING", "wal_compression = lz4 requires PostgreSQL", "to be compiled with --with-lz4"]` (тексты §4.21 — правило нормативно, в тексте примера опущено «для компактности»).
      2. **Пример 2**: вход `(18, Linux, Dw, 64, GB, 16, null, Nvme, MidRam)` — точные строки (`work_mem = 149796kB`, `huge_pages = try`, `random_page_cost = 4`, `io_method = io_uring`, без `jit`/`io_workers`); `Warnings` = lz4-пара + пустая строка + liburing-пара + пустая строка + тройка cost-DW (`"...on NVMe drives are left at defaults"`, `"to avoid catastrophic index scan selections"`, `"Monitor query planner behavior and adjust random_page_cost if necessary"`).
      3. **Пример 3**: вход `(18, Windows, Desktop, 8, GB, null, null, Hdd, MidRam)` — точные строки (`work_mem = 15603kB`, `effective_io_concurrency` отсутствует, `jit` отсутствует, `wal_level = minimal`, `max_wal_senders = 0`, `io_method = worker`); `Warnings` = lz4-пара.
      4. **Пример 4 (ALTER SYSTEM)**: вход примера 1 → точные строки `ALTER SYSTEM SET max_connections = '300';` … (значения всегда в одинарных кавычках, комментарии `--`).
      5. **formatValue** (§5.1): `1048576 → "1GB"`, `262144 → "256MB"`, `149796 → "149796kB"`, `2096128 → "2047MB"`; кратность GB проверяется первой (кратное GB не выводится в MB).
      6. **Порядок правил `wal_buffers`** (§4.8): кап 16MB до «округления вверх» (вход, дающий 3% от SB в диапазоне (14336; 16384) → 16384), минимум 32 — последним (маленькая RAM → `"32kB"`).
      7. **Порядок правил `random_page_cost`** (§4.10): `less_ram` сильнее типа диска (less_ram+hdd → 1.1); hdd → 4; dw+nvme (mid_ram) → 4; web+ssd+mid_ram → 1.1.
      8. **cpuNum не задан**: параллельные, `autovacuum_max_workers`, `io_workers` отсутствуют; `parallel_for_work_mem = 8` — через `work_mem` примера 3 (15603kB).
      9. **Windows-капы** (§4.5/§4.13): PG17/Windows/большая RAM → `maintenance_work_mem = 2047MB` (2096128 KB); `work_mem` кап 2096128; (`autovacuum_work_mem` на Windows ≤ 17 не выводится — недостижимо ≥ 2GB, §4.18 примечание).
      10. **dbSize-коррекции `work_mem`** (§4.13): `less_ram` ×1.3, `greater_ram` ×0.9 (входы с предсказуемым базовым значением).
      11. **Минимум `work_mem` 4096** (`"4MB"`).
      12. **Валидация границ** (§2): `dbVersion` вне 10..18, `totalMemory` ≤ 0, `unit=MB && totalMemory < 512`, `totalMemory > 999999` (для MB/GB/TB), `cpuNum` задан и < 1, `connectionNum` задан и < 20 → `ArgumentException` до расчёта; для `unit=KB` — кап не применяется (см. решение выше), KB = 8 388 608 проходит.
      13. **Индексатор**: `result["max_connections"] == "300"`, отсутствующее имя → null.
    - Заголовок входа в конфиге: `# Total Memory (RAM): <totalMemory> <unit>` (по образцу §5.2: «4 GB»); `dbSize` в заголовок не включается (§8.7 нюанса).
  - Выход: тесты компилируются и падают (`NotImplementedException`) — контракт ядра зафиксирован векторами.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter FullyQualifiedName~PgTuneTests` — сборка ок, все тесты красные.
  - Spec: §2 принцип 1, §4.1 ядро, §5 фаза 1 (TDD).

- [x] **Шаг 2: реализация до зелёного**

  - Вход: шаг 1 выполнен (векторы красные).
  - Действие: реализовать `Calculate` строго по §4.1–§4.21 и псевдокоду §6 спецификации алгоритма:
    - порядок вычислений и зависимостей (§4: `shared_buffers` → `huge_pages`/`wal_buffers`/`work_mem`/`autovacuum_work_mem`; `maintenance_work_mem` → `autovacuum_work_mem`; параллельные → `work_mem`);
    - целочисленные деления с `floor`; `work_mem` — вещественное деление, затем `floor` с множителем типа БД, коррекция `dbSize` (новый `floor`), минимум, кап Windows;
    - `parallel_for_work_mem` = `max_worker_processes` (= `cpuNum` при cpuNum ≥ 4), иначе 8;
    - `formatValue` §5.1; порядок вывода §5.2 (null-параметры пропускаются; `max_wal_senders = 0` — не «отсутствующий»); два режима вывода §5.3; warning-правила §4.21 (включая `''`-разделители и первый элемент `WARNING`);
    - валидация границ §2 — `ArgumentException` до расчёта (включая решение по KB выше);
    - значения-строки — инвариантная культура (`0.9`, `1.1`, `4`); `ToAlterSystem` — значения в одинарных кавычках, комментарии `--`.
  - Выход: ядро воспроизводит контрольные примеры §7 один-в-один.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter FullyQualifiedName~PgTuneTests` — зелёный; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 ошибок/предупреждений (TreatWarningsAsErrors).
  - Spec: §4.1, §2 принцип 1/2, §6 ограничение 1, AC §7 п.1.

- [x] **Шаг 3: коммит**

  - Действие: `git add src/PgWorker.Core/Tuning/PgTune.cs src/tests/PgWorker.UnitTests/Tuning/PgTuneTests.cs && git commit -m "feat(core): pgtune — ядро расчёта параметров PG (тест-векторы спецификации алгоритма)"`.
  - Проверка: `git log --oneline -1` содержит новый коммит.
  - Spec: §5 фаза 1.

---

### Задача 2: Конфигурация — `PgtuneOptions` + валидация старта + `PgtuneSettings` + appsettings.json

**Files:**
- Modify: `src/PgWorker.App/Options.cs`, `src/PgWorker.App/Program.cs`, `src/PgWorker.App/appsettings.json`
- Create: `src/PgWorker.Provisioning/Processes/PgtuneSettings.cs`

**Interfaces:**
- Consumes: `PgTune.KnownParameterNames` (Задача 1).
- Produces (для задач 3, 6):
  - `PgWorkerOptions.Pgtune: PgtuneOptions`;
  - `PgtuneOptions`: `int DbVersion = 18`, `string DbType = "oltp"`, `string HdType = "ssd"`, `string DbSize = "mid_ram"`, `int Connections = 60`, `long DefaultTotalMemoryBytes = 8589934592`, `string[] ExcludeParams = ["io_method", "io_workers"]`; метод `bool IsValid()`;
  - `PgtuneOptions.ToRuntime(): PgtuneSettings` (паттерн `MovesOptions.ToRuntime`/`PlacementOptions`);
  - `PgtuneSettings` (record в `PgWorker.Provisioning/Processes/PgtuneSettings.cs` — App не тянет Provisioning-типов в домен, Provisioning не зависит от App): `record PgtuneSettings(int DbVersion, string DbType, string HdType, string DbSize, int Connections, long DefaultTotalMemoryBytes, IReadOnlySet<string> ExcludeParams)` — строки домена как есть (маппинг в enum — фабрика, Задача 3).

- [x] **Шаг 1: PgtuneOptions + IsValid + ToRuntime + PgtuneSettings + appsettings**

  - Вход: Задача 1 завершена (ядро зелёное).
  - Действие:
    - `Options.cs`: класс `PgtuneOptions` (поля/дефолты выше, XML-комментарии по-русски: назначение каждого поля, P15-связка `Connections`, exclude-обоснование io_uring) + поле `public PgtuneOptions Pgtune { get; set; } = new();` в `PgWorkerOptions`;
    - `IsValid()` (образец `BackupsOptions.IsValid`): `DbType ∈ {web, oltp, dw, mixed}` (регистронезависимо; `desktop` запрещён — несовместим с P3); `Connections` 20…999999; `DbVersion` 10…18; `HdType ∈ {ssd, san, hdd, nvme}`; `DbSize ∈ {less_ram, mid_ram, greater_ram}`; `DefaultTotalMemoryBytes ≥ 536870912` (512MiB — граница §2 спецификации алгоритма); `ExcludeParams` — каждое имя ∈ `PgTune.KnownParameterNames`;
    - `ToRuntime()`: `new PgtuneSettings(DbVersion, DbType, HdType, DbSize, Connections, DefaultTotalMemoryBytes, new HashSet<string>(ExcludeParams, StringComparer.Ordinal))`;
    - `PgtuneSettings.cs`: record (см. Interfaces) с докой «runtime-склейка PgWorker:Pgtune (spec §4.2); Provisioning не зависит от PgWorker.App»;
    - `appsettings.json`: секция `PgWorker:Pgtune` с дефолтами (DbVersion 18, DbType "oltp", HdType "ssd", DbSize "mid_ram", Connections 60, DefaultTotalMemoryBytes 8589934592, ExcludeParams ["io_method","io_workers"]).
  - Выход: конфигурация читается и валидируется; runtime-склейка готова.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 ошибок/предупреждений.
  - Spec: §4.2, §5 фаза 2.

- [x] **Шаг 2: fail-fast валидация в Program.cs**

  - Вход: шаг 1 выполнен.
  - Действие: в цепочку `AddOptions<PgWorkerOptions>()` перед `.ValidateOnStart()` добавить `.Validate(o => o.Pgtune.IsValid(), "<сообщение с перечнем правил PgWorker:Pgtune и указанием на desktop-запрет/границы>")` (образец — `.Validate(o => o.Backups.IsValid(), ...)`).
  - Выход: невалидные опции роняют старт воркера.
  - Проверка: build Release зелёный; существующий юнит-набор зелёный: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`.
  - Spec: §4.2 (fail-fast старта), §2 принцип 5.

- [x] **Шаг 3: коммит**

  - Действие: `git add src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/PgWorker.App/appsettings.json src/PgWorker.Provisioning/Processes/PgtuneSettings.cs && git commit -m "feat(app): pgtune — конфигурация PgWorker:Pgtune с fail-fast валидацией старта"`.
  - Проверка: `git log --oneline -1` содержит новый коммит.
  - Spec: §5 фаза 2.

---

### Задача 3: Фабрика входов `PgtuneInputsFactory` + юнит

**Files:**
- Create: `src/PgWorker.Provisioning/Processes/PgtuneInputsFactory.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/PgtuneInputsFactoryTests.cs`

**Interfaces:**
- Consumes: `PgtuneSettings` (Задача 2), `PgTune`/`PgTuneResult` (Задача 1), `NodeResources(double? CpuCores, long? MemoryBytes)` (`PgWorker.Core/Model/Domain.cs`).
- Produces (для Задачи 6): `sealed class PgtuneInputsFactory(PgtuneSettings settings, ILogger<PgtuneInputsFactory> log)` с `public PgTuneResult Create(NodeResources? resources)` — синхронный, без side-effect'ов кроме warning-лога; вызывается держателем клэйма `<C>` (контекст процессов).

- [ ] **Шаг 1: юнит-тесты фабрики (AAA)**

  - Вход: Задача 2 завершена (`PgtuneSettings` существует).
  - Действие: создать `PgtuneInputsFactoryTests.cs` (фабрика без etcd, логгер — `NullLogger<PgtuneInputsFactory>.Instance`, дефолтные `PgtuneSettings`):
    1. `floor(MemoryBytes/1024)`: `NodeResources(4, 4294967296)` → вход ядра 4 194 304 KB → `shared_buffers == "1GB"`, `max_connections == "60"` (из настроек);
    2. fallback при `resources == null` и при `NodeResources(null, null)`: `DefaultTotalMemoryBytes/1024` = 8 388 608 KB → `shared_buffers == "2GB"`, `effective_cache_size == "6GB"`;
    3. `floor(CpuCores)`: `CpuCores = 4.7` → cpuNum 4, параллельные выведены (`max_worker_processes == "4"`; §4.0/§4.12 спецификации алгоритма — набор параллельных непуст только при cpuNum ≥ 4); `CpuCores = 2.7` → floor 2, но cpuNum < 4 — параллельные параметры ОТСУТСТВУЮТ (опционально: `work_mem == "30840kB"` — значение по формуле при `parallel_for_work_mem = 8` для входа oltp/8GiB/Connections=60/mid_ram; фиксирует использование дефолта 8, а не cpuNum); `CpuCores = 0.5` (floor 0 < 1) → cpuNum не задан (параллельные/`autovacuum_max_workers`/`io_workers` отсутствуют);
    4. детерминизм: два вызова с одинаковым входом → равные результаты (records equality);
    5. предупреждения расчёта попадают в warning-лог (например, память < 256MB — расчёт не блокируется, spec §6).
    6. канал «опции воркера → вход ядра → вывод» (AC §7 п.3 — «изменение опций воркера подхватывается следующим созданием контейнера»): фабрика с НЕ-дефолтными `PgtuneSettings` — `Connections = 40` → `max_connections == "40"` (комплементарно: `DoormanConfigBuilder.ServerConnections` даёт 35 — фиксируется в Задаче 4); `DbType = "dw"` → `default_statistics_target == "500"` и `random_page_cost == "4"` (dw+ssd+mid_ram, §4.10 правило 3);
  - Выход: контракт фабрики зафиксирован (красные).
  - Проверка: `dotnet test ... --filter FullyQualifiedName~PgtuneInputsFactoryTests` — компиляция ок, тесты падают (типа нет).
  - Spec: §4.3, §4.6 (PgtuneInputsFactoryTests).

- [ ] **Шаг 2: реализация фабрики**

  - Вход: шаг 1 выполнен.
  - Действие: реализовать `Create(NodeResources? resources)`:
    - `TotalMemory = floor((resources?.MemoryBytes ?? settings.DefaultTotalMemoryBytes) / 1024)`, `TotalMemoryUnit = KB`;
    - `CpuNum`: `resources.CpuCores` отсутствует или `floor(CpuCores) < 1` → null; иначе `floor(CpuCores)`;
    - `ConnectionNum = settings.Connections`; `DbVersion = settings.DbVersion`; `OsType = Linux` (контейнеры);
    - `DbType`/`HdType`/`DbSize`: явный маппинг строк настроек в enum (case-insensitive; `"less_ram"→LessRam` и т.п. — `Enum.TryParse` имена с подчёркиванием не разрулит; незнакомая строка → `InvalidOperationException` — fail-fast расчёта; валидация старта уже отсекла мусор);
    - `PgTune.Calculate(...)`; исключение НЕ глотается — уходит вверх (фейл фазы тика, транзиент-ретрай с бэкоффом, spec §4.3);
    - `result.Warnings` → warning-лог воркера (`log.LogWarning`, не блокируют) → вернуть `PgTuneResult`.
  - Выход: фабрика готова к внедрению в процессы.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter FullyQualifiedName~PgtuneInputsFactoryTests` — зелёный; полный юнит-набор зелёный; build Release — 0 warnings.
  - Spec: §4.3, §5 фаза 3, AC §7 п.4.

- [x] **Шаг 3: коммит**

  - Действие: `git add src/PgWorker.Provisioning/Processes/PgtuneInputsFactory.cs src/tests/PgWorker.UnitTests/Provisioning/PgtuneInputsFactoryTests.cs && git commit -m "feat(provisioning): pgtune — фабрика входов от актуальных заявок ресурсов"`.
  - Проверка: `git log --oneline -1` содержит новый коммит.
  - Spec: §5 фаза 3.

---

### Задача 4: Применение — `SpiloEnvBuilder` merge + `DoormanConfigBuilder` sync + юнит

**Files:**
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`
- Modify (call-site компиляции): `src/PgWorker.Docker/Drivers/ClusterDriver.cs` — только строка `DoormanConfigBuilder.Build(topology.Cluster)` → `Build(topology.Cluster, 55)` (в Задаче 6 заменяется на вычисление от tuning)
- Test: `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`

**Interfaces:**
- Consumes: `PgTuneResult` (Задача 1).
- Produces (для Задачи 6):
  - `SpiloEnvBuilder.Build(ShardTopology, EtcdEndpoints, InstallSecrets, PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)`;
  - `DoormanConfigBuilder.Build(string dbname, int serverConnections)` — `max_db_connections`/`default_pool_size` = `serverConnections`;
  - `DoormanConfigBuilder.ServerConnections(PgTuneResult tuning): int` = `max(10, int.Parse(tuning["max_connections"], InvariantCulture) − 5)`; `tuning["max_connections"]` отсутствует/нечисло → `ApplicationException` (параметр выводится всегда — §4.0 спецификации алгоритма; фейл сборки спеки, не тихий дефолт).

**Решение по доставке ExcludeParams (выводится из spec):** spec §4.2 фиксирует применение exclude на этапе сборки YAML (SpiloEnvBuilder), а §4.5 фиксирует сигнатуру `EnsureNodeAsync` только аргументом `tuning` — per-call канала для exclude нет. Поэтому exclude доставляется параметром конструктора драйвера (как `enableDoorman`): опциональный параметр `IReadOnlySet<string>? pgtuneExclude = null` у `PlainClusterDriver`/`SwarmClusterDriver` (Задача 6), `BuildSpec` передаёт его пятым аргументом `SpiloEnvBuilder.Build`. Опциональные параметры сохраняют shape вызова §4.4 (`Build(topology, etcd, secrets, tuning)`).

- [x] **Шаг 1: merge в SpiloEnvBuilder + doorman-сигнатура**

  - Вход: Задачи 1–3 завершены.
  - Действие:
    - `SpiloEnvBuilder.Build(..., PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)`:
      - `tuning == null` → прежний хардкод-набор (текущий raw string без изменений — тесты драйвера, изолированные пути);
      - иначе `bootstrap.dcs.postgresql.parameters` строится программно: упорядоченный список пар — сначала PGTune-параметры в порядке §5.2 (минус `ExcludeParams`), затем PgWorker-канон «поверх» с перезаписью по имени без дубликатов (позиция первого вхождения сохраняется, новые ключи — в конец): `wal_level: logical`, `hot_standby: "on"`, `sync_replication_slots: "on"`, `max_slot_wal_keep_size: "16GB"`, `max_wal_senders: "10"`, `max_replication_slots: "10"`, `wal_keep_size: "2048MB"`, `checkpoint_timeout: "15min"` + лог-блок (`logging_collector: "on"`, `log_directory: "log"`, `log_filename: "postgresql-%Y-%m-%d.log"`, `log_rotation_age: "1d"`, `log_rotation_size: "100MB"`) — значения и кавычки точно как в текущем raw string; PGTune-значения — YAML-строками в кавычках (`name: "value"`, отступ как у текущего блока parameters);
      - остальной YAML-каркас (тайминги, callbacks, env-ключи) — без изменений; `PatroniTimings` не затрагиваются.
    - `DoormanConfigBuilder`: `Build(string dbname, int serverConnections)` — `max_db_connections`/`default_pool_size` = `serverConnections`; добавить `ServerConnections(PgTuneResult)` (см. Interfaces);
    - call-site в `PlainClusterDriver.BuildSpec`: `DoormanConfigBuilder.Build(topology.Cluster, 55)` (временный литерал 55 — текущее поведение; в Задаче 6 заменяется).
  - Выход: билдеры умеют merge/exclude/синхронизированный doorman; прежние пути (tuning=null) не изменены.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 ошибок/предупреждений; существующие тесты `NodeConfigBuildersTests` зелёные.
  - Spec: §4.4, §2 принцип 3.

- [x] **Шаг 2: юнит-кейсы NodeConfigBuildersTests (AAA)**

  - Вход: шаг 1 выполнен.
  - Действие: дополнить `NodeConfigBuildersTests.cs`:
    1. **merge PGTune ∪ канон**: tuning от входа (например, oltp/8GiB/4cpu/60/ssd/mid_ram) → SPILO_CONFIGURATION несёт `max_connections: "60"`, `shared_buffers: "2GB"`, `effective_cache_size: "6GB"`, `checkpoint_completion_target: "0.9"` (всё — из PGTune) И канон поверх: `wal_level: logical` (сохраняется даже при dbType=dw — канон перезаписывает), `max_wal_senders: "10"`, `sync_replication_slots: "on"`, `hot_standby: "on"`, `wal_keep_size: "2048MB"`, `checkpoint_timeout: "15min"`, лог-блок; порядок PGTune-параметров = §5.2;
    2. **exclude**: `excludeParams = {io_method, io_workers}` (dbVersion 18, cpuNum ≥ 18, не-io_uring-вход) → `io_method`/`io_workers` в YAML отсутствуют (параметр не пишется вовсе, никаких пустых значений);
    3. **doorman**: `Build("shop", 55)` содержит `max_db_connections = 55` и `default_pool_size = 55`; `ServerConnections`: tuning с max_connections 60 → 55, 40 → 35, 14 → 10 (пол);
    4. **tuning == null** → прежний хардкод-набор (покрыто существующими тестами — убедиться, что не сломаны).
  - Выход: контракт merge/exclude/doorman зафиксирован тестами.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter FullyQualifiedName~NodeConfigBuildersTests` — зелёный; полный юнит-набор зелёный.
  - Spec: §4.4, §4.6 (NodeConfigBuildersTests), AC §7 п.2.

- [x] **Шаг 3: коммит**

  - Действие: `git add src/PgWorker.Core/Templates/NodeConfigBuilders.cs src/PgWorker.Docker/Drivers/ClusterDriver.cs src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs && git commit -m "feat(core): pgtune — merge PGTune+канон в SpiloEnvBuilder, doorman-бюджет от рассчитанного max_connections"`.
  - Проверка: `git log --oneline -1` содержит новый коммит.
  - Spec: §5 фаза 4.

---

### Задача 5: Канон arch/ — правки ПЕРЕД проводкой (один коммит с Задачей 6)

**Files:**
- Modify: `arch/14-pgworker.md`, `arch/12-bucket-pitfalls.md`

**Interfaces:**
- Produces: синхронизированный канон — источник правды для Задачи 6 (принцип arch-first, AGENTS.base §1).
- etcd-контракт не меняется вообще (новых ключей нет; панель не затрагивается).

- [x] **Шаг 1: правки arch/14-pgworker.md**

  - Вход: Задача 4 завершена (код применения знает точные имена/поведение).
  - Действие:
    - **§2.1**: параметры PG в `bootstrap.dcs` — из PGTune (`PgTune.Calculate`, ядро по `docs/pgtune-calculation-spec.md`), merge с каноном PgWorker (канон-ключи перезаписывают: `wal_level=logical`, walsenders/slots=10, `wal_keep_size`, `checkpoint_timeout`, лог-блок); константы `shared_buffers`/`effective_cache_size`/`random_page_cost`/`checkpoint_completion_target`/`max_connections` из канона убраны (несёт PGTune); параметры пересчитываются при КАЖДОМ EnsureNode-пути от актуальных `request_*` — БЕЗ фиксации в etcd (осознанный риск дрейфа конфига внутри шарда при изменении заявок — консистентность на операторе); конвергенция pg-параметров работающих нод — out of scope (отдельная задача roadmap);
    - **§5 A P2.1**: перед EnsureNode — вычисление тюнинга per-shard (`PgtuneInputsFactory.Create` от уже прочитанных заявок ресурсов), один результат на все ноды прохода;
    - **§5 G A3**: то же для AddShard;
    - **§5 C** (надзор): пересоздание снесённой ноды/rebuild — тюнинг от актуальных `request_*` на момент пересоздания;
    - **§5 J** (усыновление): репарация нод — `resources = null`, тюнинг от дефолтов опций;
    - **§8**: конфигурация `PgWorker:Pgtune { DbVersion=18, DbType="oltp" (desktop запрещён), HdType="ssd", DbSize="mid_ram", Connections=60, DefaultTotalMemoryBytes=8589934592, ExcludeParams=["io_method","io_workers"] }` + fail-fast валидация старта; переопределение параметров — только через эти опции, etcd-канала нет.
  - Выход: канон описывает новое поведение параметров/конфигурации.
  - Проверка: `git diff arch/14-pgworker.md` — все шесть мест правлены; в §2.1 не осталось хардкода `max_connections=60`/`shared_buffers=2GB` как канона.
  - Spec: §3 (arch/14), §5 фаза 7 (п.7: тем же коммитом, что код).

- [x] **Шаг 2: правки arch/12-bucket-pitfalls.md (P15)**

  - Действие: P15 — бюджет соединений синхронизируется от PGTune: doorman-бюджет вычисляемый `serverConnections = max(10, max_connections − 5)` (60 = 55 + 2 админ/mover + 3 reserved сохраняется при дефолте); `max_connections` — вход PGTune (константа опции `PgWorker:Pgtune:Connections`, default 60), не хардкод.
  - Выход: P15 согласован с кодом.
  - Проверка: `git diff arch/12-bucket-pitfalls.md` отражает вычисляемый бюджет.
  - Spec: §3 (arch/12 P15).

- [x] **Шаг 3: правки НЕ коммитить отдельно**

  - Действие: arch-дифф остаётся в рабочем дереве до Задачи 6 — коммитится одним коммитом с проводкой (spec §5 п.7 «тем же коммитом, что код»).
  - Проверка: `git status` показывает модифицированные `arch/14-pgworker.md`, `arch/12-bucket-pitfalls.md` (незакоммичены).
  - Spec: §5 п.7, принцип arch-first.

---

### Задача 6: Проводка — сигнатура `EnsureNodeAsync`, драйверы, 4 процесса, DI, стабы

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs`, `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs`, `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`, `src/PgWorker.Provisioning/Processes/AddShardProcess.cs`, `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs`, `src/PgWorker.App/Program.cs`
- Modify (стабы/колл-сайты): `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`, `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`, `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`, `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`, `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs`

**Interfaces:**
- Consumes: `PgTuneResult` (З.1), `PgtuneSettings` (З.2), `PgtuneInputsFactory` (З.3), `SpiloEnvBuilder.Build(+tuning,+exclude)` / `DoormanConfigBuilder.Build(dbname, serverConnections)` / `ServerConnections` (З.4).
- Produces:
  - `IClusterDriver.EnsureNodeAsync(ShardTopology, string nodeName, NodeAddress, InstallSecrets, EtcdEndpoints, NodeResources? resources, PgTuneResult? tuning, CancellationToken ct)` — новый аргумент перед `ct`;
  - `PlainClusterDriver(..., string? advertisedHost = null, IReadOnlySet<string>? pgtuneExclude = null)`; `SwarmClusterDriver(..., string nodeImage = "...", IReadOnlySet<string>? pgtuneExclude = null)` (и внутренний template-драйвер в Swarm `EnsureNodeAsync` прокидывает exclude);
  - `BuildSpec(..., NodeResources? resources, PgTuneResult? tuning)` (internal): env из `SpiloEnvBuilder.Build(topology, etcd, secrets, tuning, pgtuneExclude)`; при `enableDoorman`: `serverConnections = tuning is null ? 55 : DoormanConfigBuilder.ServerConnections(tuning)`;
  - процессы: ctor += `PgtuneInputsFactory pgtune`; `EnsureNodesAsync` (Provisioning/AddShard) += `PgTuneResult tuning`.

- [x] **Шаг 1: драйвер — интерфейс и обе реализации**

  - Вход: Задачи 1–5 завершены; arch-дифф в рабочем дереве.
  - Действие:
    - `IClusterDriver.EnsureNodeAsync` — добавить `PgTuneResult? tuning` перед `ct`; обновить doc-комментарий (tuning — рассчитанный per-shard PGTune-вывод; null → прежний хардкод-набор);
    - `PlainClusterDriver`: ctor += `pgtuneExclude`; `EnsureNodeAsync` прокидывает tuning в `BuildSpec`; `BuildSpec(topology, nodeName, addr, secrets, etcd, resources, tuning)`: env — `SpiloEnvBuilder.Build(topology, etcd, secrets, tuning, pgtuneExclude)`; doorman-бюджет от tuning (см. Interfaces);
    - `SwarmClusterDriver`: то же; template `new PlainClusterDriver([], new DockerEngineFactory(), enableDoorman, nodeImage, pgtuneExclude)`.
  - Выход: драйвер несёт тюнинг в SPILO_CONFIGURATION и doorman-конфиг.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — остаётся красным ТОЛЬКО из-за вызовов/стабов `EnsureNodeAsync` (фиксируются шагом 3); ошибки в самих драйверах отсутствуют.
  - Spec: §4.4, §4.5, §2 принцип 4.

- [x] **Шаг 2: 4 процесса — расчёт тюнинга per-shard до цикла нод**

  - Действие (ctor каждого += `PgtuneInputsFactory pgtune`):
    - `ProvisioningProcess` P2.1: в per-shard лямбде `Parallel.ForEachAsync` после `ReadShardResourcesAsync` (~строка 131) — `var tuning = pgtune.Create(resources);` один раз на шард; `EnsureNodesAsync(..., resources, tuning, ...)` → `EnsureNodeAsync(..., resources, tuning, ct)`;
    - `NodeSupervisor` (3 сайта EnsureNodeAsync: пересоздание снесённой ~246, recreate-маркер ~428, rebuild ~578): `var tuning = pgtune.Create(resources);` сразу после соответствующего `ReadShardResourcesAsync` (на сайте с ленивой загрузкой ~196–220 — после загрузки, один расчёт на шард);
    - `AddShardProcess` A3: после `ReadShardResourcesAsync` (~114) — tuning per-shard, прокинуть в `EnsureNodesAsync` → `EnsureNodeAsync`;
    - `AdoptionProcess` (репарация ~411): `resources` остаётся `null`; `var tuning = pgtune.Create(null);` — один раз до цикла шардов (результат от дефолтов константен).
  - Выход: все EnsureNode-пути несут рассчитанный тюнинг; сбой расчёта = фейл фазы тика (исключение уходит в существующий контур Result/бэкофф).
  - Проверка: build (после шага 3) зелёный.
  - Spec: §4.5, §2 принцип 4/5.

- [x] **Шаг 3: DI (Program.cs) + стабы и колл-сайты тестов**

  - Действие:
    - `Program.cs`: `.Validate` уже стоит (З.2); добавить singleton `PgtuneInputsFactory` (`PgtuneSettings` = `opts.Pgtune.ToRuntime()`, логгер из DI); в фабрики драйверов (~175/~183) передать `pgtuneExclude: new HashSet<string>(docker.Pgtune.ExcludeParams, StringComparer.Ordinal)`; в регистрации `ProvisioningProcess` (~224), `NodeSupervisor` (~246), `AdoptionProcess` (~276), `AddShardProcess` (~323) добавить аргумент-фабрику;
    - стабы `EnsureNodeAsync` в 4 файлах — добавить параметр `PgTuneResult? tuning` перед `ct` (тела не меняют поведение; `StubScaleDriver` может запоминать tuning для ассертов, если потребуется Задаче 7);
    - прямые вызовы в `ClusterDriverTests.cs`/`DockerDriverTests.cs` — добавить `tuning: null`; в `ClusterDriverTests` кейс инспекции env (~538) дополнить (опционально) проверкой env с tuning — минимум фиксация компиляции.
  - Выход: весь solution собирается; юнит-набор зелёный.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 ошибок/предупреждений; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release` — зелёный.
  - Spec: §4.5 (проводка + список стабов).

- [x] **Шаг 4: коммит (arch + проводка одним коммитом)**

  - Действие: `git add arch/14-pgworker.md arch/12-bucket-pitfalls.md src/PgWorker.Docker/Drivers/ClusterDriver.cs src/PgWorker.Provisioning/Processes/ src/PgWorker.App/Program.cs src/tests/ && git commit -m "feat(provisioning): pgtune — проводка EnsureNodeAsync/процессов + канон arch (14 §2.1/§5/§8, 12 P15)"`.
  - Проверка: `git status` — arch-файлы закоммичены вместе с кодом проводки.
  - Spec: §5 фаза 5 + п.7.

---

### Задача 7: Интеграционные сценарии (docker, изоляция по docs/e2e-isolation.md)

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2ePgtuneScenarios.cs`

**Interfaces:**
- Consumes: паттерн `E2eEnvironment.StartAsync("tag")` / `E2eFixture.WaitForAsync` / `DockerTrait.SkipIfUnavailable()` (как `E2eScaleScenarios`); guid-тег окружения во всех именах; динамические хост-порты; собственные etcd-префиксы/бакеты сценария.
- Produces: 3 сценария spec §4.6.

- [x] **Шаг 1: сценарий provision → env нод**

  - Вход: Задача 6 завершена (код проводки зелёный); docker доступен.
  - Действие: класс `E2ePgtuneScenarios` (по образцу `E2eScaleScenarios`; teardown окружения при любом исходе — `await using var fx`; ассерт чистоты после teardown по docs/e2e-isolation.md):
    - provision кластера (уникальное имя `pgtune{tag}`); дождаться Active;
    - AAA Assert: `SPILO_CONFIGURATION` контейнеров нод несёт рассчитанные `max_connections`/`shared_buffers` (значения от поданных `request_*` сценария), doorman-синхронизация: `DOORMAN_CONFIG` содержит `max_db_connections = 55` (при дефолтных Connections=60); SPILO_CONFIGURATION идентичен у всех нод одного прохода (env-ключи `PGW_NODE_HOST`/`PGW_NODE_NAME` вне сравнения).
  - Выход: сценарий фиксирует AC §7 п.2.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter FullyQualifiedName~E2ePgtuneScenarios` — сценарий зелёный; после прогона — зачистка (см. Глобальные ограничения) и `docker network prune -f`.
  - Spec: §4.6 (интеграционные), AC §7 п.2.

- [x] **Шаг 2: сценарий пересчёта от актуальных request_***

  - Действие: provision кластера с `request_mem = 4Gi` → env нод: `shared_buffers: "1GB"`; удалить контейнер ноды; перезаписать `/service/<scope>/request_mem` на 8Gi; дождаться пересоздания надзором (бюджеты — из конфигурации окружения сценария, как в соседних E2E; итерации ожидания ≤ 30 c); AAA Assert: env пересозданного контейнера рассчитан от НОВОЙ заявки (`shared_buffers: "2GB"`) — env отличается от прежнего; никаких новых ключей в etcd не появилось (etcd-контракт неизменен).
  - Выход: AC §7 п.3 (пересчёт от актуальных заявок).
  - Проверка: как шаг 1 (оба сценария в фильтре зелёные); зачистка после серии.
  - Spec: §4.6, §6 (осознанный пересчёт), AC §7 п.3.

- [x] **Шаг 3: сценарий отсутствия request_mem → дефолты**

  - Действие: provision кластера БЕЗ `request_mem` → AAA Assert: env нод рассчитан от `DefaultTotalMemoryBytes` (8GiB → `shared_buffers: "2GB"`, `effective_cache_size: "6GB"`, `max_connections: "60"`); при `request_cpu < 1` (или отсутствии) — параллельные параметры в YAML отсутствуют.
  - Выход: AC §7 п.4.
  - Проверка: полный фильтр `--filter FullyQualifiedName~E2ePgtuneScenarios` зелёный; зачистка контейнеров/сетей серии; финальная проверка чистоты — ассертами сценария + `docker ps -aq` без pgw-остатков.
  - Spec: §4.6, AC §7 п.4, §7 п.5 (teardown).

---

### Задача 8: Docker-E2E гейт на свежем Release + зачистка + приёмка

**Files:** изменений кода нет — прогон и сверка.

- [ ] **Шаг 1: docker-E2E на свежем Release (обязателен — меняется PgWorker.Provisioning)**

  - Вход: Задачи 1–7 завершены; docker доступен; внешние образы в локальном registry (`dev-stand/images/pull-images.sh` при необходимости).
  - Действие: из worktree выполнить:
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
    ```
    (E2eFixture сам собирает свежий Release; `PGW_TEST_E2E_NOBUILD` НЕ использовать.)
  - Выход: мерж-гейт кейс-маркер зелёный на свежем бинаре.
  - Проверка: финальная строка прогона — passed/failed счётчики с 0 failed.
  - Spec: §4.6 (docker-E2E обязателен), AC §7 п.5.

- [ ] **Шаг 2: зачистка контейнеров/сетей после каждой серии**

  - Действие: после КАЖДОЙ серии (юниты → интеграция → E2E): дождаться финальной строки прогона, затем `docker rm -f $(docker ps -aq --filter "name=pgw-") 2>/dev/null; docker network prune -f` (если dev-стенд поднят; иначе `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`); проверить `docker ps -aq` — только контейнеры стенда (`as-*`/`adminpanel`), если стенд поднят; `docker network ls | grep -c kfw-net` — 0 осиротевших.
  - Выход: отсутствие остаточных контейнеров/сетей перед следующим прогоном.
  - Spec: AGENTS.md (зачистка), AC §7 п.5.

- [ ] **Шаг 3: финальная сверка критериев приёмки (spec §7)**

  - Действие: прогнать полный юнит-набор и сверить пункты:
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
    ```
    - AC1: контрольные примеры 1–4 один-в-один (`PgTuneTests` зелёные);
    - AC2/AC3/AC4: покрыты `NodeConfigBuildersTests` + `PgtuneInputsFactoryTests` (включая канал «опции → вход ядра → вывод» — пункт 6 Задачи 3) + интеграционными сценариями (Задача 7 — пересчёт от актуальных заявок);
    - AC5: юнит/интеграционные зелёные; docker-E2E `Scale_AddEmptyShard` на свежем Release зелёный; teardown чистит полностью;
    - AC6: arch-правки в коммите проводки (Задача 6).
    При расхождении — вернуться в соответствующую задачу, не «латать» поверх.
  - Выход: все критерии §7 выполнены; ветка готова к ревью.
  - Проверка: полный юнит-прогон зелёный; `git log --oneline` — коммиты задач 1–6; `git status` чист.
  - Spec: §7 (критерии приёмки), §5 фазы 1–7.
