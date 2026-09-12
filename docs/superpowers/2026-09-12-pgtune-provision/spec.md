# Spec: PGTune — расчёт параметров PostgreSQL и применение при provision нод PgWorker

Дата: 2026-09-12
Основание: `docs/pgtune-calculation-spec.md` (нормативная спецификация алгоритма: входы §2, формулы §4, формат вывода §5, псевдокод §6, контрольные примеры §7, нюансы §8).
Канон: `arch/14-pgworker.md` (provisioning, §2.1 Spilo env, §3 etcd-контракт), `arch/12-bucket-pitfalls.md` (P15), `arch/11-bucket-sharding.md` (§4).

Решения пользователя (зафиксированы вопросами, 2026-09-12):
1. Параметры НЕ фиксируются в etcd — пересчёт при каждом EnsureNode-пути от актуальных `request_*` (осознанный риск дрейфа конфига внутри шарда — §6).
2. `io_method`/`io_workers` — exclude по умолчанию (io_uring требует проверки сборки Spilo).
3. Doorman-бюджет синхронизируется: `serverConnections = max(10, max_connections − 5)`.
4. Дефолт `DbType = oltp`.

## 1. Цель

Параметры `postgresql.conf` нод кластеров PgWorker рассчитываются алгоритмом PGTune (по `docs/pgtune-calculation-spec.md`) из характеристик ноды — память и CPU из заявок ресурсов `/service/<scope>/request_{mem,cpu}`, остальные входы — конфигурация воркера — и применяются при создании нод (bootstrap Patroni DCS), вместо сегодняшних жёстких констант `SPILO_CONFIGURATION`.

Сегодня (`arch/14` §2.1, `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`): `bootstrap.dcs.postgresql.parameters` — хардкод (`max_connections=60` P15, `shared_buffers=2GB`, `effective_cache_size=6GB`, `random_page_cost=1.1`, `checkpoint_completion_target=0.9`). Задача: рассчитывать их PGTune-ом, сохранив инварианты PgWorker:

- **P3** (`arch/12`): `wal_level=logical` и репликационный контур переездов бакетов (`max_wal_senders=10`, `max_replication_slots=10`, `sync_replication_slots`, `max_slot_wal_keep_size`, `wal_keep_size`) — канон PgWorker, из PGTune не берётся никогда (вывод PGTune `wal_level=minimal`/`max_wal_senders=0` для `desktop` несовместим с M0-префлайтом переездов и физрепликацией шарда);
- **P15** (`arch/12`): бюджет соединений `max_connections = doorman-бюджет + 5` (60 = 55 + 2 админ/mover + 3 reserved) — сохраняется синхронизацией doorman-конфига от рассчитанного `max_connections`.

Алгоритм общий: ядро принимает все 9 входов явно; в PgWorker часть входов подставляется константами конфигурации (`PgWorker:Pgtune`).

## 2. Принципы

1. **Ядро — чистая функция 1:1 спецификации.** Без I/O, без NuGet-зависимостей, внутренняя единица — KB двоичные, все `floor`/порядок правил — по §1 и §8 спецификации алгоритма. Контрольные примеры §7 — обязательные тест-векторы (один-в-один).
2. **Ядро общее, PgWorker-специфика — на границе.** Все входы — поля `PgTuneInput`; константы (dbVersion, dbType, hdType, dbSize, connections, fallback-память) живут только в опциях воркера и фабрике входов.
3. **Канон PgWorker перекрывает PGTune.** Merge параметров: PGTune-вывод ∪ PgWorker-канон; канон-ключи перезаписывают (дубликатов в YAML нет).
4. **Пересчёт на каждый EnsureNode-путь (решение пользователя).** Параметры — детерминированная функция от `(request_mem, request_cpu, PgWorker:Pgtune)`; никакого хранимого состояния: без новых etcd-ключей, без txn-записей, без ручного переопределения через etcd. Расчёт выполняется один раз на шард за EnsureNode-проход — все ноды одного прохода получают одинаковый результат. Изменение заявок/опций подхватывается следующим созданием контейнера (новый шард, rebuild, пересоздание).
5. **Fail-fast на границах.** Границы входов (§2 спецификации алгоритма) валидируются до расчёта; невалидные опции — fail-fast старта; невалидный расчёт — journal-фейл фазы, не тихий пропуск.

## 3. Изменение канона (arch/) — до кода

- `arch/14-pgworker.md`:
  - §2.1: параметры PG в `bootstrap.dcs` — из PGTune (`PgTune.Calculate`), merge с каноном PgWorker; константы `shared_buffers`/`effective_cache_size`/`random_page_cost`/`checkpoint_completion_target`/`max_connections` из канона убираются (несёт PGTune); параметры пересчитываются при каждом EnsureNode-пути (без фиксации);
  - §5 A P2.1, §5 G A3, §5 C, §5 J: вычисление тюнинга перед `EnsureNode` (per-shard, от актуальных заявок ресурсов);
  - §8: конфигурация `PgWorker:Pgtune` + валидация старта.
- `arch/12-bucket-pitfalls.md` (P15): бюджет doorman — вычисляемый (`serverConnections = max(10, max_connections − 5)`), `max_connections` — вход PGTune (константа опции `PgWorker:Pgtune:Connections`, default 60).
- **etcd-контракт не меняется вообще** (новых ключей нет; панель не затрагивается).

## 4. Структура/компоненты

### 4.1. Ядро алгоритма — `src/PgWorker.Core/Tuning/PgTune.cs` (новое)

```
enum PgTuneOsType { Linux, Windows, Mac }
enum PgTuneDbType { Web, Oltp, Dw, Desktop, Mixed }
enum PgTuneHdType { Ssd, San, Hdd, Nvme }
enum PgTuneDbSize { LessRam, MidRam, GreaterRam }
enum PgTuneMemoryUnit { KB, MB, GB, TB }

sealed record PgTuneInput(int DbVersion, PgTuneOsType OsType, PgTuneDbType DbType,
    long TotalMemory, PgTuneMemoryUnit TotalMemoryUnit,
    int? CpuNum, int? ConnectionNum, PgTuneHdType HdType, PgTuneDbSize DbSize);

sealed record PgTuneParameter(string Name, string Value);   // значение строкой §5
sealed record PgTuneResult(IReadOnlyList<PgTuneParameter> Parameters,  // порядок §5.2, null-параметры пропущены
    IReadOnlyList<string> Warnings);                        // правила §4.21

static class PgTune
{
    public static PgTuneResult Calculate(PgTuneInput input);          // §4 спецификации алгоритма
    public static string ToPostgresqlConf(PgTuneInput, PgTuneResult); // §5.2 (тест-векторы)
    public static string ToAlterSystem(PgTuneInput, PgTuneResult);    // §5.3 (тест-векторы)
}
```

- `Calculate` воспроизводит формулы §4.1–§4.21 и псевдокод §6 спецификации алгоритма: порядок вычислений (`shared_buffers` → `huge_pages`/`wal_buffers`/`work_mem`/`autovacuum_work_mem`; `maintenance_work_mem` → `autovacuum_work_mem`; параллельные → `work_mem`), `formatValue` (§5.1: кратные GB → GB, кратные MB → MB, иначе kB; `2096128 → 2047MB`), порядок правил `random_page_cost`/`wal_buffers`, капы Windows (2GB−1MB), `parallel_for_work_mem` (= `max_worker_processes` при cpuNum ≥ 4, иначе 8), вывод `max_wal_senders = 0` только с `wal_level=minimal`.
- Значения — строки в инвариантной культуре (`0.9`, `1.1`, `4`); память — `formatValue`.
- Валидация границ входа (§2 спецификации алгоритма) — `ArgumentException` до расчёта.

### 4.2. Конфигурация — `PgWorkerOptions.Pgtune` (`src/PgWorker.App/Options.cs`, appsettings.json)

```
sealed class PgtuneOptions {
    int DbVersion = 18;                    // PG-версия образа pgworker-node (Spilo-18)
    string DbType = "oltp";                // web|oltp|dw|mixed|desktop
    string HdType = "ssd";                 // ssd|san|hdd|nvme
    string DbSize = "mid_ram";             // less_ram|mid_ram|greater_ram
    int Connections = 60;                  // вход connectionNum (P15: doorman = Connections − 5)
    long DefaultTotalMemoryBytes = 8589934592;  // fallback при отсутствии request_mem (8GiB)
    string[] ExcludeParams = ["io_method", "io_workers"];  // не применять (см. ниже)
}
```

- Валидация (fail-fast старта, образец `BackupsOptions.IsValid`): `DbType ∈ {web,oltp,dw,mixed,desktop}`, `desktop` запрещён (несовместим с P3); `Connections` 20…999999; `DbVersion` 10…18; `HdType`/`DbSize` из домена; `DefaultTotalMemoryBytes` ≥ 512MiB (граница §2 спецификации алгоритма); `ExcludeParams` — известные имена вывода.
- `ExcludeParams` применяется на этапе сборки YAML (`SpiloEnvBuilder`), не в фабрике входов: фабрика/`Calculate` всегда дают полный вывод. Дефолт `[io_method, io_workers]` (решение пользователя): `io_method=io_uring` требует сборки PG с `--with-liburing` (для образа pgworker-node/Spilo-18 не проверено); отсутствие параметра → PG18 default `io_method=worker` — совпадает с расчётом для не-Linux и безопасно. После проверки сборки оператор убирает из exclude.
- **Переопределение параметров оператором — только через опции** (`Connections`, `DbType`, `HdType`, `DbSize`, `DefaultTotalMemoryBytes`, `ExcludeParams`); ручного etcd-канала переопределения нет (решение пользователя).
- Runtime-склейка: `PgtuneOptions` → `PgtuneSettings` (record, провайдится DI в `Program.cs`; паттерн `MovesOptions.ToRuntime`/`PlacementOptions`) — PgWorker.Provisioning не зависит от PgWorker.App.

### 4.3. Фабрика входов — `src/PgWorker.Provisioning/Processes/PgtuneInputsFactory.cs` (новое)

```
sealed class PgtuneInputsFactory(PgtuneSettings settings, ILogger<PgtuneInputsFactory> log)
{
    /// Пересчёт тюнинга шарда от актуальных заявок ресурсов (решение пользователя:
    /// без фиксации в etcd). Синхронный, без side-effect'ов кроме warning-лога.
    PgTuneResult Create(NodeResources? resources);
}
```

- `TotalMemoryKb = floor(MemoryBytes / 1024)` из `resources.MemoryBytes`; `MemoryBytes` отсутствует/нечитаем (NodeResourcesParser даёт null) → `DefaultTotalMemoryBytes / 1024`.
- `CpuNum = floor(CpuCores)`; отсутствует или `< 1` → cpuNum не задан (алгоритм честно пропускает параллельные/autovacuum/io_workers).
- `DbVersion`/`DbType`/`HdType`/`DbSize`/`ConnectionNum` — константы `PgtuneSettings`; `OsType = Linux` (контейнеры).
- `PgTune.Calculate` → warnings в warning-лог воркера (не блокируют) → `PgTuneResult`.
- Сбой расчёта (исключение) = фейл фазы тика (транзиент-ретрай с бэкоффом), не тихий пропуск. Вызывается только держателем клэйма `<C>` (контекст процессов).

### 4.4. Применение — `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`

- `SpiloEnvBuilder.Build(topology, etcd, secrets, PgTuneResult? tuning)`:
  - `tuning == null` → прежний хардкод-набор (тесты драйвера, изолированные пути);
  - иначе `bootstrap.dcs.postgresql.parameters` = ordered dictionary: сначала PGTune-параметры (порядок §5.2, минус `ExcludeParams`), затем PgWorker-канон поверх (перезапись без дубликатов): `wal_level: logical`, `hot_standby: "on"`, `sync_replication_slots: "on"`, `max_slot_wal_keep_size: "16GB"`, `max_wal_senders: "10"`, `max_replication_slots: "10"`, `wal_keep_size: "2048MB"`, `checkpoint_timeout: "15min"` + лог-блок (`logging_collector`, `log_directory`, …) — как сегодня. Значения — YAML-строками в кавычках (текущий стиль: `max_connections: "60"`).
  - Из канона убираются `max_connections`, `shared_buffers`, `effective_cache_size`, `checkpoint_completion_target`, `random_page_cost` (несёт PGTune).
- `DoormanConfigBuilder.Build(dbname)` → `Build(dbname, int serverConnections)`: `max_db_connections`/`default_pool_size` = `serverConnections`. Вызов в `ClusterDriver.BuildSpec` (при `enableDoorman`): `serverConnections = max(10, int.Parse(tuning["max_connections"]) − 5)` — синхронизация от рассчитанного в памяти результата (55 при 60 — инвариант P15; параметр `max_connections` выводится всегда, отсутствие значения невозможно).
- Тайминги Patroni (`PatroniTimings`) не затрагиваются.

### 4.5. Проводка в процессы и драйвер

- `IClusterDriver.EnsureNodeAsync(..., NodeResources? resources, PgTuneResult? tuning, CancellationToken ct)` — новый аргумент перед `ct`; `BuildSpec` несёт его в `SpiloEnvBuilder`/`DoormanConfigBuilder`.
- Точки вызова (все вычисляют тюнинг per-shard один раз, до цикла нод шарда — фабрика от уже прочитанных `resources`):
  - `ProvisioningProcess` P2.1 (`EnsureNodesAsync`; resources уже читает `ReadShardResourcesAsync`);
  - `NodeSupervisor` (пересоздание снесённой ноды, rebuild; resources уже читает `ReadShardResourcesAsync`);
  - `AddShardProcess` A3;
  - `AdoptionProcess` (репарация нод; `resources` остаётся `null` — тюнинг от дефолтов опций).
- Обновить сигнатуры stub'ов: `PgWorker.UnitTests/Provisioning/Fakes.cs`, `PgWorker.UnitTests/Backups/BackupProcessTests.cs`, `PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`, `PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`.

### 4.6. Тесты

- **Юнит `PgTuneTests`** (`src/tests/PgWorker.UnitTests/Tuning/`, AAA-комментарии): контрольные примеры 1–3 спецификации алгоритма — точные строки `postgresql.conf` (параметры + заголовок входа); пример 4 — `ALTER SYSTEM`; предупреждения по правилам §4.21 (в текстах примеров опущены «для компактности» — правило нормативно: пример 1 — lz4; пример 2 — lz4 + liburing + DW/NVMe cost; пример 3 — lz4); `formatValue` (кратные GB/MB/kB, `2096128 → 2047MB`); порядок правил (`wal_buffers`, `random_page_cost`); `cpuNum` не задан (параллельные/autovacuum отсутствуют, `parallel_for_work_mem=8`); Windows-капы; `dbSize`-коррекции `work_mem`; минимум `work_mem` 4MB.
- **Юнит `PgtuneInputsFactoryTests`** (без etcd): `floor(MemoryBytes/1024)`; fallback `DefaultTotalMemoryBytes` при null/нечитаемом `request_mem`; `floor(CpuCores)` и `0.5` → cpuNum не задан; одинаковый результат при одинаковых входах (детерминизм).
- **Юнит `NodeConfigBuildersTests`**: merge PGTune+канон (`wal_level: logical` сохраняется при dbType=dw; `max_wal_senders: "10"`); exclude `io_method` не пишет параметр; doorman 55 при Connections=60, 35 при Connections=40, пол 10.
- **Интеграционные (`PgWorker.IntegrationTests`)**: provision кластера → `SPILO_CONFIGURATION` контейнеров нод несёт рассчитанные `max_connections`/`shared_buffers` и совпадает между нодами одного прохода; удаление контейнера ноды → надзор пересоздаёт → env пересозданной ноды пересчитан от АКТУАЛЬНЫХ `request_*` (после перезаписи заявки env нового контейнера отличается); отсутствие `request_mem` → расчёт от `DefaultTotalMemoryBytes`. Isolation/teardown — по `docs/e2e-isolation.md` (динамические порты, own-only чистка, ассерт чистоты).
- **Docker-E2E на Release обязателен** (меняется `PgWorker.Provisioning`): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`.

## 5. Фазы

1. **Ядро** `PgTune.cs` (TDD: тест-векторы контрольных примеров §7 — первыми, затем реализация до зелёного).
2. **Конфигурация**: `PgtuneOptions` + валидация + `PgtuneSettings` + appsettings.json.
3. **Фабрика входов** `PgtuneInputsFactory` + юнит.
4. **Применение**: `SpiloEnvBuilder` merge + `DoormanConfigBuilder` sync + юнит.
5. **Проводка**: сигнатура `IClusterDriver.EnsureNodeAsync`, 4 процесса, stub'ы.
6. **Интеграционные + E2E** на Release; зачистка контейнеров/сетей после каждой серии.
7. **Канон**: правки `arch/14-pgworker.md` и `arch/12-bucket-pitfalls.md` (P15) тем же коммитом, что код.

## 6. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; ядро PGTune — только BCL, без NuGet.
- **Осознанное решение (2026-09-12): параметры не фиксируются** — пересчёт при каждом EnsureNode-пути от актуальных заявок. Следствие: после изменения `request_mem`/`request_cpu` пересозданная/rebuild-нода шарда получит конфиг, рассчитанный от новых заявок, а ноды, поднятые ранее, продолжат работать на прежнем (конфиг PG живёт в данных/DCS) — возможный дрейф конфига внутри шарда. Консистентность при изменении заявок живого кластера — ответственность оператора (пересоздать все ноды шарда либо не менять заявки у работающего шарда). Переопределение параметров — только через `PgWorker:Pgtune`/`ExcludeParams`, etcd-канала переопределения нет.
- **Конвергенция pg-параметров работающих нод — out of scope**: `max_connections`/`shared_buffers` требуют рестарта PostgreSQL; параметры применяются только при bootstrap (создание контейнера); автоматическое выравнивание (PATCH /config + pending_restart) — отдельная задача roadmap.
- `wal_level=minimal`/`max_wal_senders=0` (desktop-ветка PGTune) не применяются никогда; `DbType=desktop` отвергается валидацией старта.
- Параметры, не выведенные для данного входа (cpuNum не задан → параллельные, autovacuum, io_workers), в YAML не пишутся вовсе (никаких суррогатных дефолтов).
- PGTune-значения применяются буквально (воспроизводимость эталона); известная особенность `max_worker_processes = cpuNum` при включённых parallel workers — поведение PGTune как есть; переопределение — через `ExcludeParams`/опции.
- Изменение `PgtuneOptions` (Connections/DbType/…) подхватывается следующим EnsureNode-путём (новый шард/rebuild), работающие ноды не трогаются.
- Заголовок входов («# DB Version: …») в `SPILO_CONFIGURATION` не пишется — YAML несёт только `parameters`.
- `totalMemory` > 100GB / < 256MB — предупреждение PGTune (warning-лог воркера), расчёт не блокируется.

## 7. Критерии приёмки

1. Реализация воспроизводит контрольные примеры 1–4 `docs/pgtune-calculation-spec.md` один-в-один (юнит-тесты сверяют строки конфига и предупреждения).
2. Provision нового кластера: каждая нода несёт `SPILO_CONFIGURATION` с merge(PGTune ∪ канон P3); параметры нод одного прохода совпадают; doorman-бюджет = `max_connections − 5` (синхронизирован от рассчитанного результата).
3. Пересчёт от актуальных заявок (решение пользователя): пересоздание/rebuild ноды после изменения `request_*` даёт конфиг от новых значений; изменение опций воркера подхватывается следующим созданием контейнера; никакого состояния в etcd не появляется (etcd-контракт неизменен).
4. Отсутствие/нечитаемость `request_mem` → расчёт от `DefaultTotalMemoryBytes`; `request_cpu < 1` → параллельные параметры отсутствуют.
5. Юнит/интеграционные зелёные; docker-E2E `Scale_AddEmptyShard` на свежем Release зелёный; teardown чистит за сценарием полностью.
6. `arch/14-pgworker.md` и `arch/12-bucket-pitfalls.md` (P15) синхронизированы с кодом.
