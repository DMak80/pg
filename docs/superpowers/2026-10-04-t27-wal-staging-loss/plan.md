# t27-wal-staging-loss — план реализации (Фаза 3 dev-flow)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** собственный WAL-приёмник (физическая репликация, подтверждение слота по факту доставки в S3) + двойная архивация (мастер и sync-standby пишут в один префикс `wal/`) — RPO=0 подтверждённых транзакций при потере любой одной машины; staging-том упраздняется.

**Архитектура:** новый консольный проект `src/PgWorker.WalReceiver` (entrypoint образа `pgworker-wal`), реюз математики `WalFileName`/`WalChain`/`IBackupS3` из `PgWorker.Backups`; оркестрация `WalStreamProcess` держит per-node контейнеры-агенты `pgw-backup-wal-<C>-<X>-<N>` на мастере и sync-standby; прогресс — только объекты S3, etcd-ключ `wal` пишет воркер (расширяется полем `agents`).

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), Npgsql 10.0.3 (`Npgsql.Replication`), AWSSDK.S3 3.7.511.8, Docker (testcontainers 4.14.0), etcd, MinIO.

**Spec:** [`docs/superpowers/2026-10-04-t27-wal-staging-loss/spec.md`](spec.md) — план аргументируется от спеки; исполнители читают оба документа. Канон — `arch/19-backups.md` (arch-first правки уже в worktree, не закоммичены).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/fix-t27-wal-staging-loss`, ветка `fix-t27-wal-staging-loss`. Все пути ниже — от корня worktree.

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — 0 warnings в каждой сборке.
- Централизованное версионирование: `src/Directory.Packages.props` (Npgsql 10.0.3, AWSSDK.S3 3.7.511.8, Testcontainers 4.14.0 — уже есть; НОВЫХ пакетов не добавлять).
- Тесты docker: порты ТОЛЬКО динамические (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`), никаких литералов вида `:16000`; таймауты фикстур ≤ 100 с.
- Каждый интеграционный/E2E тест: свой guid-контур, полный teardown при любом исходе, ассерт чистоты (`docs/e2e-isolation.md`); после КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (`docker network prune -f` страховочно).
- Телеметрия E2E по канону `docs/e2e-launch.md` (артефакты teardown, `[PHASE]`, без перезапуска упавших).
- Образы: локально собираемые (`pgworker-wal:dev`/`:e2e`) в registry 192.168.0.1:5000 НЕ класть; новые внешние — строка в `dev-stand/images/images.txt` + mirror.
- MULTI-HOST: любые гарантии — только от реплицированных S3/etcd; локальный буфер сегмента — кэш без durability.
- Язык: комментарии/документация — русский; идентификаторы — английский; тесты — AAA-комментарии.
- Решение по restart-политике (контролёр флоу): **`no`** — exited = permanent-факт, пересоздаёт только супервиз воркера со свежими env; spec НЕ правится, канон правится Task 0.
- Мерж-гейт: E2E на свежем Release + кейс-маркер `Scale_AddEmptyShard`; коммиты в feature-ветке — свободно.

---

### Task 0: arch-first — restart-политика `no` в каноне arch/19 §3

**Files:**
- Modify: `arch/19-backups.md` (правка уже модифицированного в worktree файла, раздел §3)

**Interfaces:**
- Consumes: решение контролёра флоу (restart-политика `no`).
- Produces: канон, согласованный со spec §3.3; все последующие задачи зеркалят его.

- [ ] **Step 1: Правка канона**

В `arch/19-backups.md` §3, абзац «Агент», заменить фразу:

```
S3-креды и DSN — env
контейнера (§7); restart-политика `unless-stopped` (docker поднимает
агента после смерти/ребута — воркер тиком сверяет).
```

на:

```
S3-креды и DSN — env
контейнера (§7); restart-политика `no`: переподключение к источнику —
внутри приёмника (transient-цикл), exited-контейнер — только permanent
(несовместимый segment size, невалидные env/креды) — супервиз тика воркера
пересоздаёт агента со свежими env; ребут docker-хоста покрывается тем же
супервизом (exited-агент пересоздаётся первым тиком после появления хоста).
```

- [ ] **Step 2: Проверка**

Run: `grep -c "unless-stopped" arch/19-backups.md`
Expected: `0`
Run: `grep -n "restart-политика \`no\`" arch/19-backups.md`
Expected: 1 совпадение в §3.

- [ ] **Step 3: Commit**

```bash
git add arch/19-backups.md
git commit -m "arch(t27): §3 канона — restart-политика wal-агента no: exited=permanent, пересоздаёт супервиз воркера со свежими env (согласование со spec t27 §3.3)"
```

**Связь со spec:** §2 arch-first; §3.3 п.5.

---

### Task 1: Каркас `PgWorker.WalReceiver` — проект, env-контракт, stdout-маркеры

**Files:**
- Create: `src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj`
- Create: `src/PgWorker.WalReceiver/WalReceiverOptions.cs`
- Create: `src/PgWorker.WalReceiver/WalReceiverMarkers.cs`
- Create: `src/PgWorker.WalReceiver/Program.cs` (заглушка — врезка ядра в Task 5)
- Modify: `src/PgWorker.slnx` (в `<Folder Name="/backups/">`)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverOptionsTests.cs`, `src/tests/PgWorker.UnitTests/Backups/WalReceiverMarkersTests.cs`

**Interfaces:**
- Consumes: —
- Produces (для Tasks 2–6):
  - `namespace PgWorker.WalReceiver`; ProjectReference на `PgWorker.Backups`.
  - `public sealed record WalReceiverOptions(string PgHost, int PgPort, string PgUser, string PgPassword, string PgDbname, string Slot, string Cluster, string Shard, string S3Endpoint, string? S3Region, string S3Bucket, string S3AccessKey, string S3SecretKey, bool S3PathStyle)`
  - `public static class WalReceiverEnv { public static WalReceiverOptions? Parse(Func<string,string?> env, out List<string> errors); }` — null при невалидном env; обязательные непустые: `PG_HOST, PG_PORT (1..65535), PG_USER, PG_PASSWORD, PG_DBNAME, SLOT, CLUSTER, SHARD, S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY`; опциональные: `S3_REGION`; `S3_PATHSTYLE` — bool, default `true`, принимаются `true/false/1/0`.
  - `public static class WalReceiverMarkers` — однострочные JSON в stdout:
    - `Starting()` → `{"phase":"starting"}`
    - `Streaming(ulong walStart)` → `{"phase":"streaming","wal_start":"<hex X/Y>"}`
    - `Delivering(string segment)` → `{"phase":"delivering","segment":"..."}`
    - `Heartbeat(ulong confirmedLsn)` → `{"heartbeat":"<hex X/Y>"}`
    - `Result(bool ok, string? error, string? lastDelivered)` → `{"ok":true,...}` / `{"ok":false,"error":"..."}` (без секретов!)
    - LSN в маркерах — формат PG `X/Y` (hex, uppercase ok).
  - Program: `int Main()` — парсит env; невалидные → `Result(false, joined errors, null)` + return 5. Вызов ядра — Task 5.

- [ ] **Step 1: Проект + slnx**

`src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj` (по образцу `PgWorker.Backups.csproj`, без PackageReference — Npgsql/AWSSDK.S3 транзитивно от Backups):

```xml
<Project Sdk="Microsoft.NET.Sdk">
    <!-- Приёмник WAL (t27, arch/19 §3): физическая репликация Npgsql + доставка
         сегментов в S3 (IBackupS3 из Backups). Отдельный образ pgworker-wal (§2). -->
    <PropertyGroup>
        <OutputType>Exe</OutputType>
        <TargetFramework>net10.0</TargetFramework>
        <ImplicitUsings>enable</ImplicitUsings>
    </PropertyGroup>
    <ItemGroup>
        <ProjectReference Include="..\PgWorker.Backups\PgWorker.Backups.csproj"/>
    </ItemGroup>
</Project>
```

В `src/PgWorker.slnx` внутрь `<Folder Name="/backups/">` добавить:
```xml
        <Project Path="PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" />
```

- [ ] **Step 2: Пишу failing-тесты env-парсинга (AAA)**

`WalReceiverOptionsTests.cs`: кейсы — полный валидный набор парсится (все поля на месте); отсутствие каждого обязательного ключа → null + ошибка с именем ключа; `PG_PORT=abc`/`0`/`70000` → ошибка; `S3_PATHSTYLE=false/0/1/true` → bool; отсутствие `S3_PATHSTYLE` → `true`; отсутствие `S3_REGION` → null. Секрет (`PG_PASSWORD`, `S3_SECRET_KEY`) не попадает в текст ошибок.

- [ ] **Step 3: Run — красные**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WalReceiverOptions"`
Expected: FAIL (типы не существуют — ошибка компиляции теста).

- [ ] **Step 4: Реализация WalReceiverOptions + WalReceiverMarkers + Program-заглушка**

`WalReceiverOptions.cs`: record + static class `WalReceiverEnv { public static WalReceiverOptions? Parse(Func<string,string?> env, out List<string> errors); }` (record и парсер — раздельные типы: одно имя для record и static class конфликтует в C#). `WalReceiverMarkers.cs`: методы выше, `System.Text.Json` сериализация, LSN-хелпер `LsnText(ulong)` → `$"{v >> 32:X}/{v & 0xFFFFFFFF:X}"`. `Program.cs`: parse → ошибки → маркер Result + `return 5`; успех → `return 0` (заглушка до Task 5).

- [ ] **Step 5: Run — зелёные + маркеры**

Тесты `WalReceiverMarkersTests.cs`: `Starting()` даёт ровно `{"phase":"starting"}`; `Result(false,"...")` не содержит секретов; `Heartbeat(0x1234_0000_0000)` → `{"heartbeat":"1234/0"}`.
Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WalReceiver"`
Expected: PASS, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.WalReceiver src/PgWorker.slnx src/tests/PgWorker.UnitTests/Backups/WalReceiverOptionsTests.cs src/tests/PgWorker.UnitTests/Backups/WalReceiverMarkersTests.cs
git commit -m "feat(wal): t27 Ф1 — каркас PgWorker.WalReceiver: env-контракт (без секретов в argv/логах) + stdout-JSON-маркеры протокола"
```

**Связь со spec:** §3.1 (env-контракт, маркеры), Ф1.

---

### Task 2: `SegmentAssembler` — математика сегментов от LSN

**Files:**
- Create: `src/PgWorker.WalReceiver/SegmentAssembler.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverSegmentAssemblerTests.cs`

**Interfaces:**
- Consumes: `WalFileName` (PgWorker.Backups, `SegmentBytes=16MiB`).
- Produces (для Task 4):
  - `public readonly record struct XLogChunk(ulong WalStart, ReadOnlyMemory<byte> Data);`
  - `public sealed record ClosedSegment(uint Tli, string Name, byte[] Data, ulong EndLsn);` — `Name` = 24-hex имя, `EndLsn` = LSN конца сегмента.
  - `public sealed class SegmentAssembler(ulong startLsn)`: `public IReadOnlyList<ClosedSegment> Append(XLogChunk chunk);` `public ulong LastAppendedLsn { get; }`.

Механика (канон arch/19 §3, спека §3.1 п.3):
- `segId = lsn / 0x1000000` (16 MiB); `log = segId / 0x100`; `seg = segId % 0x100` — сверять имена с `WalFileName.FromLsn`-математикой (юнит-связка).
- Старт с произвольной позиции: байты до `alignUp(startLsn)` (первой границы сегмента) ОТБРАСЫВАЮТСЯ — неполный головной сегмент не выгружается (PG-сегмент обязан быть ровно 16 MiB; «цепочка закрепится первым upload» — первым ПОЛНЫМ сегментом).
- Буфер накапливается от границы; чанк может пересекать границы — сплит на 0..N закрытых сегментов; каждый закрытый обязан набрать ровно `WalFileName.SegmentBytes` байт (иначе — internal-ошибка `ApplicationException`, permanent-маркер).
- Контроль непрерывности: `chunk.WalStart` обязан равняться `LastAppendedLsn` (первый — `startLsn`); разрыв → `ApplicationException` (протокол физической репликации дыр не даёт — защита от багов транспорта).
- TLI сегмента: из long page header первого page сегмента — LE `uint32` по смещению 4; валидация заголовка: `xlp_pageaddr` (LE `uint64` @ смещение 8) == LSN границы сегмента (самопроверка без хрупких WAL-магик). TLI запоминается как текущий; сегмент нового TLI триггерит `.history`-ветку (Task 4).

- [ ] **Step 1: Failing-тесты (AAA)** — минимум:
  1. Один сегмент по кускам: 4 чанка по 4 MiB от границы → 0..3 закрытых (последний чанк закрывает), имя `000000010000000000000000`-типа сверено с `WalFileName.FromLsn(1, lsnText)`, `EndLsn = start + 16MiB`.
  2. Чанк пересекает границу: один чанк 20 MiB → 1 закрытый + буфер 4 MiB; следующий чанк добивает → второй закрытый.
  3. Старт с середины сегмента (`startLsn = граница + 5 MiB`): первые 5 MiB отброшены, первый закрытый — СЛЕДУЮЩИЙ полный сегмент, его имя — от границы (не от startLsn).
  4. TLI: синтетический page header (info/magic не важны — проверяется только pageaddr) в начале сегмента с `tli=2` → `ClosedSegment.Tli == 2`; следующий сегмент без new header-проверки наследует tli.
  5. Разрыв `chunk.WalStart != LastAppendedLsn` → `ApplicationException`.
  6. Недобор: попытка закрыть сегмент < SegmentBytes внутренним API — недостижимо извне (проверяется через 1+3).

- [ ] **Step 2: Run красные** — `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~SegmentAssembler` → FAIL (компиляция).

- [ ] **Step 3: Реализация** по механике выше.

- [ ] **Step 4: Run зелёные** — тот же фильтр → PASS, 0 warnings.

- [ ] **Step 5: Commit** — `feat(wal): t27 Ф1 — SegmentAssembler: границы/имена сегментов от LSN, TLI из long page header, отбрасывание неполного головного сегмента`.

**Связь со spec:** §3.1 п.1/п.3 (сверка математики с `WalFileName`), §5 (`.partial` не появляется).

---

### Task 3: Резолв старта — хвост S3 + `restart_lsn` слота

**Files:**
- Create: `src/PgWorker.WalReceiver/S3TailResolver.cs`
- Create: `src/PgWorker.WalReceiver/ISlotPositionReader.cs` (интерфейс)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverTailTests.cs`

**Interfaces:**
- Consumes: `IBackupS3.ListWalAsync`, `WalChain.Check`, `WalFileName`.
- Produces (для Task 4):
  - `public static class S3TailResolver { public static async Task<Result<ulong?>> ResolveTailAsync(IBackupS3 s3, string cluster, string shard, CancellationToken ct); }` — list → min-объект как chain_start → `WalChain.Check(start, names)`: непрерывна → `EndLsn(LastSegment)`; дыра → `EndLsn(chain.LastSegment)` (старт от конца непрерывной части; повторная доставка выше дыры идемпотентна по имени); пустой список → `null`.
  - `public static ulong EndLsn(WalFileName segment)` — `(ulong)(segment.Log * 256 + segment.Seg + 1) << 24`.
  - `public interface ISlotPositionReader { Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct); }` (реализация Npgsql — Task 5).

- [ ] **Step 1: Failing-тесты (AAA)**: фейк `IBackupS3` (локальный мини-фейк в тесте: `ListWalAsync` от захардкоженного списка `WalObject`) —
  1. сегменты `...0001..0005` → tail = конец `0005`;
  2. дыра `0001,0002,0004` → tail = конец `0002`;
  3. пусто → null;
  4. TLI-переход с history в списке — chain непрерывен, tail = конец последнего;
  5. `EndLsn` согласован с `WalFileName`: `EndLsn(parse("000000010000000000000002"))` → `FromLsn`-обратно даёт `...0003`.
- [ ] **Step 2: Run красные.**
- [ ] **Step 3: Реализация.**
- [ ] **Step 4: Run зелёные.**
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф1 — резолв стартовой позиции от хвоста S3 (WalChain) + интерфейс чтения restart_lsn слота`.

**Связь со spec:** §3.1 п.2 (резолв хвоста, пустой префикс — от restart_lsn).

---

### Task 4: `WalReceiverCore` — главный цикл, feedback-инвариант, backpressure

**Files:**
- Create: `src/PgWorker.WalReceiver/IXLogReplicationSource.cs`
- Create: `src/PgWorker.WalReceiver/WalReceiverCore.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverCoreTests.cs`

**Interfaces:**
- Consumes: Task 1 (`WalReceiverOptions`, маркеры), Task 2 (`XLogChunk`, `SegmentAssembler`, `ClosedSegment`), Task 3 (`S3TailResolver`, `ISlotPositionReader`), `IBackupS3.PutObjectAsync` (sha256-hex параметром — серверная сверка).
- Produces (для Task 5):
  - `public interface IXLogReplicationSource : IAsyncDisposable`:
    ```csharp
    Task<Result> OpenAsync(CancellationToken ct);                       // подключение + сверка wal_segment_size == WalFileName.SegmentBytes (mismatch → Failed permanent)
    Task<Result> StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct);
    IAsyncEnumerable<XLogChunk> Stream { get; }                          // после успешного Start
    Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct); // standby status: write=flush=applied одной LSN
    Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct); // null = рантайм не экспонирует TIMELINE_HISTORY
    ```
  - `public sealed class WalReceiverCore` : `public static async Task<int> RunAsync(WalReceiverOptions o, IXLogReplicationSource source, ISlotPositionReader slots, IBackupS3 s3, TextWriter stdout, CancellationToken ct)` — коды: `0` чистая отмена; `3` segment-size mismatch; `4` слот отсутствует; `6` невосстановимая ошибка протокола (разрыв LSN).

Механика цикла:
1. Маркер `Starting()`.
2. `OpenAsync` — Failed «segment size» → `Result(false,...)` + return 3.
3. Tail: `S3TailResolver.ResolveTailAsync` — транспортный сбой → ретрай каждые 5 c (transient, процесс жив). `null` (пустой префикс) → `slots.ReadRestartLsnAsync(slot)`; слот отсутствует → return 4. Стартовая LSN — явно в `StartReplicationAsync`.
4. `await foreach (var chunk in source.Stream.WithCancellation(ct))`:
   - `assembler.Append(chunk)` → для каждого закрытого сегмента: маркер `Delivering(name)`; put-цикл `s3.PutObjectAsync($"{cluster}/{shard}/wal/{name}", data, sha256Hex)` — сбой → ретрай 5 c (НЕ двигаем enumerator — backpressure: сервер ждёт подтверждения, TCP-окно закрывается, ничего не теряется; слот удерживает). Успех put → `source.SetConfirmedAsync(endLsn)` — ЕДИНСТВЕННОЕ место подтверждения (инвариант: write=flush=applied = конец последнего доставленного в S3 закрытого сегмента, никогда дальше).
   - Сегмент нового TLI (первый с `Tli != prevTli`): `ReadTimelineHistoryAsync(tli)` → не null → put `wal/{tli:x8}.history`; null → маркер `{"phase":"streaming","history_missing":"<tli>"}` и продолжение (fallback-контроль воркера — Task 13).
   - Маркер `Streaming(assembler.LastAppendedLsn)` — не чаще 1/5 c.
5. Обрыв источника (exception из Stream/Start) — transient: `Starting()`, пауза 5 c, переподключение (п.2), tail-резолв заново. Процесс НЕ завершается.
6. `ct` → маркер `Result(true, null, last)` + return 0.

- [ ] **Step 1: Failing-тесты (AAA)** на `FakeXLogSource` (in-test: программируемая очередь чанков, счётчик `MoveNextCalls`, `ConfirmedLsn`, флаг `ThrowOnStream`) + мини-фейк `IBackupS3` (put в словарь; флаг `FailNextPuts:int`):
  1. **Feedback-инвариант**: 3 сегмента живым S3 → `ConfirmedLsn` после цикла = `EndLsn(сегмента 3)`; в процессе — никогда не опережает последний успешный put (фейк пишет журнал подтверждений).
  2. **Backpressure**: `FailNextPuts = 1` на сегменте 2 → после его закрытия `MoveNextCalls` замирает (новые чанки не запрашиваются), `ConfirmedLsn == EndLsn(сегмент 1)`; «починка» S3 (сброс флага по таймеру фейка) → put успешен → поток продолжился, доставка 2–3, `ConfirmedLsn == EndLsn(3)`.
  3. **Рестарт**: тот же фейк-S3 с объектами 1–2 → новый core → `StartReplicationAsync` получил `EndLsn(2)` (дозасылка от хвоста).
  4. **TLI-ветка**: `ReadTimelineHistoryAsync → byte[]{...}` → put `wal/00000002.history` в фейк-S3; `null` → маркер `history_missing`, put истории не было.
  5. **Permanent**: Open-сбой segment-size → return 3 + result-JSON c error; слот-ридер «слота нет» при пустом S3 → return 4.
  6. **Reconnect**: `ThrowOnStream` после сегмента 1 → `OpenCalls >= 2`, цикл продолжился, сегмент 2 доставлен.
- [ ] **Step 2: Run красные.**
- [ ] **Step 3: Реализация** `IXLogReplicationSource.cs` + `WalReceiverCore.cs` (механика выше).
- [ ] **Step 4: Run зелёные** — PASS, 0 warnings.
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф1 — главный цикл приёмника: подтверждение ТОЛЬКО после успешного S3-put, backpressure, TLI/.history, transient-переподключение`.

**Связь со spec:** §3.1 п.1–5 (весь контракт приёмника), §2 (durability от S3), §6 AC1-ядро.

---

### Task 5: Npgsql-реализации + Program

**Files:**
- Create: `src/PgWorker.WalReceiver/NpgsqlReplication.cs` (реализация `IXLogReplicationSource` — класс `NpgsqlXLogSource`)
- Create: `src/PgWorker.WalReceiver/NpgsqlSlotPositionReader.cs`
- Modify: `src/PgWorker.WalReceiver/Program.cs` (врезка `WalReceiverCore.RunAsync`)

**Interfaces:**
- Consumes: Task 4 (`IXLogReplicationSource`, `RunAsync`), Task 1 (`WalReceiverEnv.Parse`, маркеры).
- Produces: рабочий entrypoint (для Task 6 интеграций и Task 7 образа).

Детали реализации:
- `NpgsqlXLogSource(WalReceiverOptions o)`:
  - SQL-коннект (обычный `NpgsqlConnection`): `$"Host={o.PgHost};Port={o.PgPort};Username={o.PgUser};Password={o.PgPassword};Database={o.PgDbname};SSL Mode=Require;Trust Server Certificate=true;Timeout=10"` → `SHOW wal_segment_size` (строка вида `16MB`) → parse → сверка с `WalFileName.SegmentBytes`: расхождение → `Result.Failed` с текстом «wal_segment_size ... ≠ 16 MiB» (permanent-ветка п.3).
  - Replication-коннект: `new NpgsqlReplicationConnection(...)` c `Replication=database` в connstring (walsender-режим Npgsql; слот физический задаётся типом). `StartReplicationAsync(new PhysicalReplicationSlot(o.Slot), new NpgsqlLogSequenceNumber(startLsn), cancellationToken: ct)`.
  - `Stream`: `await foreach (var msg in replication)` → `yield return new XLogChunk((ulong)msg.WalStart, msg.Data)` (конверсия `NpgsqlLogSequenceNumber` ↔ `ulong` — по фактическому API Npgsql 10; свериться с XML-doc пакета в `~/.nuget/packages/npgsql/10.0.3`).
  - `SetConfirmedAsync`: `replication.SetReplicationStatus(...)` + `SendStatusAsync(ct)` — write=flush=applied = переданная LSN (точную сигнатуру сверить по API Npgsql; семантика фиксирована: одна LSN на все три поля; серверные keepalive Npgsql отвечает последним выставленным статусом — потому подтверждение ставится ТОЛЬКО после put).
  - `ReadTimelineHistoryAsync`: попробовать публичный API Npgsql для `TIMELINE_HISTORY`; отсутствует → `Task.FromResult(Result<byte[]?>.Success(null))` — fallback Task 13.
- `NpgsqlSlotPositionReader`: SQL `SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name=$1` → парсинг `X/Y` → `ulong`; пустой результат → `Result<ulong>.Failed("слот <slot> отсутствует")`.
- `Program.cs`: `WalReceiverEnv.Parse(Environment.GetEnvironmentVariable)` → невалид → маркер + return 5; иначе `return await WalReceiverCore.RunAsync(o, new NpgsqlXLogSource(o), new NpgsqlSlotPositionReader(o), new BackupS3(ToRuntime(o)), Console.Out, ctSource.Token)` — `Console.CancelKeyPress`/`AppDomain.ProcessExit` → отмена; `BackupS3` собрать из `BackupsRuntimeOptions` с полями S3 приёмника (конструктор существует).
- [ ] **Step 1: Реализация** (по механике; API-сверка по XML-doc Npgsql 10.0.3).
- [ ] **Step 2: Сборка 0 warnings** — `dotnet build src/PgWorker.WalReceiver -c Debug` → PASS.
- [ ] **Step 3: Юниты Task 4 остаются зелёными** (фейки не задеты) — `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~WalReceiver`.
- [ ] **Step 4: Commit** — `feat(wal): t27 Ф1 — Npgsql-транспорт репликации (START_REPLICATION PHYSICAL, status по доставке) + Program-врезка`.

**Связь со spec:** §3.1 (механика приёмника), §7 (риск Npgsql-VERDICT API — спайк в Task 6).

---

### Task 6 (Ф2): Интеграции приёмника — docker PG + MinIO

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/Backups/OwnPostgres.cs` (фикстура PG с TLS)
- Create: `src/tests/PgWorker.IntegrationTests/Backups/WalReceiverIntegrationTests.cs`
- Test: тот же файл (сценарии ниже)

**Interfaces:**
- Consumes: `WalReceiverCore.RunAsync`, `NpgsqlXLogSource`, `NpgsqlSlotPositionReader`, `OwnMinio` (существующая фикстура), `BackupS3`, `WalChain`.
- Produces: AC1/AC6-доказательства in-process; вердикт TIMELINE_HISTORY (решает условность Task 13); доверие к транспорту для Task 7+.

Фикстура `OwnPostgres` (канон e2e-isolation: guid, динамический порт, teardown, ассерт чистоты):
- `postgres:17-alpine` (образ уже в images.txt), `POSTGRES_PASSWORD`, `WithPortBinding(5432, assignRandomHostPort: true)`, wait `pg_isready -U postgres` ≤ 45 c.
- TLS (sslmode=require приёмника): серты self-signed генерятся кодом фикстуры (`System.Security.Cryptography.X509Certificates.CertificateRequest`, RSA-2048, CN=localhost, PEM в temp-каталог: `server.crt`/`server.key`); после старта — `docker exec` (root): `cp` серты в `/var/lib/postgresql/tls/`, `chown postgres`, `chmod 600 server.key`; затем SQL от postgres: `ALTER SYSTEM SET ssl='on'; ALTER SYSTEM SET ssl_cert_file='/var/lib/postgresql/tls/server.crt'; ALTER SYSTEM SET ssl_key_file='/var/lib/postgresql/tls/server.key'; SELECT pg_reload_conf();` — проверка `SELECT ssl_is_used()`-недоступна без коннекта; фейковая проверка: подключение с `SSL Mode=Require` из теста проходит (приёмник Trust Server Certificate).
- Слот: `SELECT pg_create_physical_replication_slot('pgw_bkp_it', true)` (immediate+reserved).
- Опции конструирования приёмника: `WalReceiverOptions` с `Slot="pgw_bkp_it"`, `Cluster/Shared` = guid-значения, `S3Endpoint` = published-порт MinIO.
- `IAsyncDisposable`: контейнер + temp-серты; ассерт чистоты: `docker ps -a --filter name=<guid-имя>` пуст.

Сценарии (каждый Fact — свой `OwnPostgres` + свой `OwnMinio`; AAA-комментарии; `DockerTrait.SkipIfUnavailable()`):

- [ ] **Step 1: Тест `Доставка_сегментов_и_непрерывность`** — старт core в Task-обёртке (`CancellationTokenSource`); INSERT-нагрузка + `pg_switch_wal` (30×8MiB — образец `GenerateWalAsync` из E2eBackupScenarios, упростить до ~6 переключений) → wait ≤ 60 c: ≥ 2 сегмента в MinIO (`ListWalAsync`), все без `.partial`, `WalChain.Check(min, names).IsContinuous`; `restart_lsn ≤ EndLsn(last)` — SQL-зонд `pg_replication_slots` (**AC1**). Остановка core (cancel) — return 0.
- [ ] **Step 2: Тест `Рестарт_дозасылает_от_хвоста`** — сегменты 1–2 доставлены; core отменён; ещё WAL (2 переключения); новый core → wait: новые сегменты в MinIO, цепочка непрерывна, дыр НЕТ, `restart_lsn ≤ хвоста`.
- [ ] **Step 3: Тест `Обрыв_S3_backpressure_восстановление` (AC6)** — core жив; `docker pause` MinIO (testcontainers `PauseAsync()`); выждать **60 c** (бюджет спеки «≥1 мин»; в журнале теста строка `[PHASE] pause-wait 60s`); SQL-зонд: `restart_lsn` ≤ хвоста (НЕ ушёл за недоставленное); `docker unpause`; wait ≤ 90 c: доигралось, `WalChain` непрерывен, `restart_lsn ≤ нового хвоста` (AC1 после восстановления).
- [ ] **Step 4: Тест `Permanent_segment_size_mismatch`** — отдельный Fact: `OwnPostgres` c `POSTGRES_INITDB_ARGS=--wal-segsize=32` (initdb принимает 32MB) → `RunAsync` → return `3`, маркер result содержит «wal_segment_size».
- [ ] **Step 5: Вердикт TIMELINE_HISTORY** — в Step 1 временно залогировать (stdout приёмника в тесте): факт вызова `ReadTimelineHistoryAsync` → null/byte[]; результат зафиксировать в комментарии теста и сообщении коммита (управляет условностью Task 13). TLI-переход в Ф2 не воспроизводится (single-node) — путь закрыт юнитом Task 4 и E2E Task 14 (AC5).
- [ ] **Step 6: Run** — `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter FullyQualifiedName~WalReceiverIntegration` → PASS; teardown чист (ассерты фикстур); после серии: `docker ps -a --filter name=pgw-` пуст, `docker network prune -f`.
- [ ] **Step 7: Commit** — `feat(wal): t27 Ф2 — интеграции приёмника: AC1 (restart_lsn ≤ доставленного хвоста), AC6 (S3-обрыв backpressure), рестарт-дозасылка, permanent mismatch; вердикт TIMELINE_HISTORY=<результат>`.

**Связь со spec:** Ф2, AC1, AC6, §7-риск.

---

### Task 7 (Ф3): Образ `pgworker-wal` + registry + compose

**Files:**
- Create: `docker/PgWorker.Wal.Dockerfile`
- Modify: `dev-stand/images/images.txt` (+1 строка)
- Modify: `deploy/docker-compose.yml` (build-сервис)

**Interfaces:**
- Consumes: Task 5 (публикуемый бинарь).
- Produces: образ `pgworker-wal:dev` (стенд/деплой) и `pgworker-wal:e2e` (Task 14); база в registry.

- [ ] **Step 1: Dockerfile** (по образцу `docker/PgWorker.Dockerfile`, база — runtime, без shell-обёрток):

```dockerfile
# syntax=docker/dockerfile:1
# Образ WAL-агента-приёмника (t27, arch/19 §2/§3): опубликованный бинарь
# PgWorker.WalReceiver, ENTRYPOINT без shell. Контейнерам агентов воркер
# передаёт параметры ТОЛЬКО env (§7). Сборка (контекст — корень репо):
#   docker build -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -o /app --nologo

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=publish /app ./
ENTRYPOINT ["dotnet", "PgWorker.WalReceiver.dll"]
```

- [ ] **Step 2: images.txt** — добавить строку `mcr.microsoft.com/dotnet/runtime:10.0` (алфавитный порядок списка); зеркалировать по ранбуку: `dev-stand/images/mirror-image.sh mcr.microsoft.com/dotnet/runtime:10.0` (запуск на машине со стендом; если зеркалирование недоступно в текущем окружении — зафиксировать в коммите как required-шаг стендa).
- [ ] **Step 3: compose** — в `deploy/docker-compose.yml` рядом с `pgworker-backup` (профиль build):

```yaml
  # Образ WAL-агента-приёмника (t27, arch/19 §3): только сборка — сервиса нет,
  # агентов запускает PgWorker как long-running контейнеры per-node.
  pgworker-wal:
    build:
      context: ..
      dockerfile: docker/PgWorker.Wal.Dockerfile
    image: pgworker-wal:dev
    profiles: ["build"]
    entrypoint: ["true"]
```

- [ ] **Step 4: Проверка сборки** — из корня worktree: `docker build -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev .` → успех; `docker run --rm pgworker-wal:dev` → выход 5 с маркером result (env пуст) — НЕ оставляет контейнеров.
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — образ pgworker-wal (dotnet/runtime, ENTRYPOINT приёмник) + зеркало базы + compose build-сервис`.

**Связь со spec:** §3.2.

---

### Task 8 (Ф3): Конфиг `Wal:AgentImage`

**Files:**
- Modify: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions` + `WalAgentImage`)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsWalOptions.AgentImage`, `ToRuntime()`, `IsValid()`)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (новый/расширить существующий при наличии)

**Interfaces:**
- Consumes: —
- Produces: `BackupsRuntimeOptions.WalAgentImage` (default `"pgworker-wal:dev"`) — используют Task 12 (ContainerSpec), env `PgWorker__Backups__Wal__AgentImage`.

- [ ] **Step 1: Failing-тест** — `BackupsOptions.IsValid()`: `Enabled=true` + `Wal.AgentImage=""` → false; дефолт `Wal.AgentImage == "pgworker-wal:dev"`; `ToRuntime()` переносит `WalAgentImage`.
- [ ] **Step 2: Run красный.**
- [ ] **Step 3: Реализация**: record-поле `string WalAgentImage = "pgworker-wal:dev"` (в конец `BackupsRuntimeOptions`, именованные аргументы по всему коду уже), `BackupsWalOptions { public string AgentImage { get; set; } = "pgworker-wal:dev"; }`, в `ToRuntime()`: `WalAgentImage: Wal.AgentImage`, в `IsValid()` — `&& !string.IsNullOrWhiteSpace(Wal.AgentImage)` под веткой Enabled.
- [ ] **Step 4: Run зелёный** + весь юнит-проект (`dotnet test src/tests/PgWorker.UnitTests -c Debug`) — позиционные вызовы `new BackupsRuntimeOptions(...)` сломаются? — нет, конструктор с default в конце; компиляция это подтвердит.
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — опция Wal:AgentImage (дефолт pgworker-wal:dev, fail-fast Enabled)`.

**Связь со spec:** §3.3 п.5, канон §9.

---

### Task 9 (Ф3): Поле `agents` в etcd-ключе wal

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` (`WalAgentState`, `WalStreamState.Agents`)
- Modify: `src/PgWorker.Backups/WalStatusWriter.cs` (сериализация/парсинг)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalStatusWriterTests.cs` (расширить), `src/tests/AdminPanel.UnitTests/` — тест панельного парсера с `agents`

**Interfaces:**
- Consumes: —
- Produces (для Task 12): `public enum WalAgentPresence { Running, Exited, Absent }`; `public sealed record WalAgentState(string Node, WalAgentPresence State);`; `WalStreamState` + хвостовой параметр `IReadOnlyList<WalAgentState>? Agents = null`; JSON: `"agents":[{"node":"<n>","state":"running|exited|absent"}]` — опционально (не пишется при null).

- [ ] **Step 1: Failing-тесты (AAA)**: writer roundtrip с `Agents` → JSON содержит `"agents":[{"node":"shard1a","state":"running"}]`; без `Agents` → поля нет; парсинг старого JSON (без agents) → `Agents == null`, `parseErrors` пуст (**AC7**); парсинг с agents → список на месте; панельный `AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs)` на ключе с `agents` → `Errors` пуст (AC7, юнит в AdminPanel.UnitTests по образцу существующих парсер-тестов).
- [ ] **Step 2: Run красный.**
- [ ] **Step 3: Реализация**: `WalStreamState` — добавить позиционный параметр в конец record; `WalStatusWriter.ToJson`/`Parse` — поле `agents` (string[] состояния — маппинг `running/exited/absent` ↔ enum); `with { Agents = ... }` в Task 12.
- [ ] **Step 4: Run зелёный**: юниты воркера + панели: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~WalStatus; dotnet test src/tests/AdminPanel.UnitTests -c Debug`.
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — опциональное поле agents в wal-ключе (супервиз-факты per-node), обратно совместимо (AC7)`.

**Связь со spec:** §3.5, AC7.

---

### Task 10 (Ф3): `BackupAgentNames` per-node + драйверы

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/BackupAgentNames.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (Plain + Swarm + `IClusterDriver`)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`
- Test: `src/tests/PgWorker.UnitTests/Docker/BackupAgentNamesTests.cs`, `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`

**Interfaces:**
- Consumes: —
- Produces (для Task 12):
  - `BackupAgentNames.Container(string cluster, string shard, string node)` → `pgw-backup-wal-<C>-<X>-<N>`; `Volume(...)` — УДАЛИТЬ; `Prefix(cluster)` без изменений.
  - `IClusterDriver.EnsureBackupAgentAsync(string cluster, string shard, string node, ContainerSpec spec, string host, CancellationToken ct)` — +`node`.
  - Шард-матчинг контейнера: хвост после префикса == `<X>` (старый формат) ИЛИ начинается с `<X>-` (per-node) — применяется в `AgentShardOf` (Plain) и `AgentShardOfSwarm`.
  - `RemoveBackupAgentsAsync` — поведение volume: оставить существующий идемпотентный `RemoveVolumeAsync($"{agent}-staging")` (404 = успех): у per-node агентов тома нет → no-op; старые `-staging`-тома сносятся этим же вызовом (миграция §3.6 — тому отдельный механизм не нужен).

- [ ] **Step 1: Failing-тесты**: `Container("c","s","n1")` → `pgw-backup-wal-c-s-n1`; юниты `EnsureBackupAgent`: спека с `VolumeName: null` принимается (валидация удалена); spec per-node имя контейнера; `ClusterDriverTests`: тест с `VolumeName: BackupAgentNames.Volume(...)` удалить/заменить на `VolumeName: null` + `VolumeDest: null`; `RestartPolicy` в тестах-спеках — по Task 12 (`no`).
- [ ] **Step 2: Run красный.**
- [ ] **Step 3: Реализация**: правки `BackupAgentNames` (удалить `Volume`), сигнатура `EnsureBackupAgentAsync` (+`node`, имя через `Container(cluster, shard, node)`), удалить валидацию VolumeName; `AgentShardOf`-матчинг; Swarm-драйвер — синхронная смена сигнатуры; `StubScaleDriver` — per-node имена в `EnsureBackupAgentAsync`/`BackupAgentNamesOf` (матчинг как в драйвере).
- [ ] **Step 4: Run зелёный** — юниты + компиляция интеграций (`dotnet build src/tests/PgWorker.IntegrationTests -c Debug`; WalStreamProcessTests упадут компиляцией — это Task 12, временно пометить? НЕТ: задачи Task 10–12 делать одним подмножеством с общим зелёным состоянием; порядок внутри — Task 10 → 11 → 12, прогон тестов WalStreamProcessTests переносится в Step Task 12. Checkpoint: `dotnet build src/PgWorker.slnx -c Debug` обязан быть зелёным в конце Task 12).
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — per-node имена wal-агентов, EnsureBackupAgentAsync(cluster,shard,node), валидация staging-тома удалена`.

**Связь со spec:** §3.4.

---

### Task 11 (Ф3): `ShardEndpoints.ResolveSyncStandbyAsync`

**Files:**
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/ShardEndpointsSyncTests.cs` (или расширить WalStreamProcessTests — по месту)

**Interfaces:**
- Consumes: `ShardProbe.GetClusterAsync` (Patroni `/cluster`).
- Produces (для Task 12): `public Task<Result<NodeAddress?>> ResolveSyncStandbyAsync(string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)` — перебор нод → member `role=="replica" && state=="running" && Sync==true` && имя в portalloc шарда → его адрес; не найден → `Success(null)` (БЕЗ fallback на мастера); `ResolveBackupSourceAsync` переписать как `sync ?? ResolveMasterAsync` (DRY, поведение не меняется).

- [ ] **Step 1: Failing-тест (AAA)**: интеграционный с `HttpListener` на свободном порту (зонд порта `TcpListener(0)`-паттерн), отдающим JSON `patroni-cluster-sync.json`-формата; portalloc-сид с `patroni=<порт>` → резолв возвращает адрес `s1b`; без sync-члена (JSON без `"sync":true`) → `null`; сервер недоступен → `null` (не Failed).
- [ ] **Step 2: Run красный.** **Step 3: Реализация.** **Step 4: Run зелёный** (`--filter FullyQualifiedName~ShardEndpointsSync`).
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — резолв sync-standby (Patroni sync-статус, без fallback) + DRY ResolveBackupSourceAsync`.

**Связь со spec:** §3.3 п.2.

---

### Task 12 (Ф3): `WalStreamProcess` — два источника, per-node супервиз, миграция; удаление WalAgentCommand

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs`
- Delete: `src/PgWorker.Backups/WalAgentCommand.cs`
- Delete: `src/tests/PgWorker.UnitTests/Backups/WalAgentCommandTests.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` (`FakeWalSqlExecutor` per-DSN)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (обновить + новые)

**Interfaces:**
- Consumes: Task 8 (`WalAgentImage`), Task 9 (`WalStreamState.Agents`), Task 10 (per-node драйвер), Task 11 (`ResolveSyncStandbyAsync`).
- Produces: финальная оркестрация (AC3-интеграционный контур, миграция AC7-сценарий, agents-факты).

Правки `WalStreamProcess` (порядок шагов тика — по spec §3.3):

1. **Гварды** — без изменений (restore-гвард, backup_password, клэйм, Active, QUARANTINED, Enabled=false).
2. **Два источника**: мастер — как сейчас (`ResolveMasterAsync` + `ResolveMasterRef`); sync — `shards.ResolveSyncStandbyAsync(...)` → `ResolveNodeRef(shard, addresses, addr)` (обобщить `ResolveMasterRef`: пара `(nodeRef, pgHost=alias, pgPort=5432)` для ноды из portalloc; object — host:pg-port). Sync-источник отсутствует → только мастер (второй агент поднимется тиком при появлении). Дедупликация: если sync-адрес == мастер-адрес → один источник.
3. **Слоты per-instance**: для КАЖДОГО источника свой `adminDsn` (`ShardEndpoints.AdminDsn(sourceAddr, ...)`) и зонд `SlotExistsAsync`; правило «слот исчез»: Break только если слота нет НИ на одном источнике при живой цепочке (живой слот на любой ноде — не BROKEN; recreateSlot на мастере); ensure — на каждом источнике, где нет.
4. **Контроль** `ControlDueAsync` — без изменений (list/ratchet/дыра/BROKEN/lag), пишет ключ как раньше (Agents=null — факты приходят шагом 5).
5. **Ensure per-node агентов** `EnsureAgentsAsync(cluster, shard, sources, options, slot, password, wal, ct)`:
   - desired-имена: `BackupAgentNames.Container(cluster, shard, src.Node)` per источник.
   - листинг драйвера; для каждого desired: существующий running с этим именем → skip; иначе (нет/exited/чужое имя) → `RemoveBackupAgentsAsync(cluster, shard)` (сносит ВСЕ агенты шарда, включая старый формат без суффикса ноды и его `-staging`-том — миграция §3.6 тем же механизмом) → создать все desired.
   - `ContainerSpec`: `Image: options.WalAgentImage`, `Env` — новый контракт §3.1: `PG_HOST/PG_PORT/PG_USER=backup_exec/PG_PASSWORD/PG_DBNAME=postgres/SLOT/CLUSTER/SHARD/S3_ENDPOINT(options.AgentS3Endpoint)/S3_REGION?(если задан)/S3_BUCKET/S3_ACCESS_KEY/S3_SECRET_KEY/S3_PATHSTYLE(options.S3PathStyle.ToString().ToLowerInvariant())`; `Cmd: null`, `ResetEntrypoint: false` (ENTRYPOINT образа), `VolumeName: null`, `VolumeDest: null`, `Ports: []`, `RestartPolicy: "no"`, сеть/лимиты — как сейчас.
   - Хост агента = docker-хост источника (`masterAddr.Host` per источник).
   - agents-факты: из листинга — per источник `running/exited/absent`; после супервиза, если wal-ключ существует (прочитан ранее или перечитан), `WriteIfChangedAsync(wal with { Agents = facts })` — идемпотентно.
6. `BreakAsync`/`StopShardAsync`/`StopAllAsync` — без изменений (префиксная чистка уже корректна).

`FakeWalSqlExecutor`: слоты per-инстансу — `ConcurrentDictionary<string, HashSet<string>> SlotsByDsn` (ключ — adminDsn, значение — слоты); `SlotExists/EnsureSlot` работают по паре (dsn, slot); `ContainsKey("pgw_bkp_c1_shard1")`-ассерты тестов заменить на «слот есть на DSN мастера/обоих DSN» (тесты ниже).

- [ ] **Step 1: Обновить failing-тесты WalStreamProcessTests** — существующие: имена агентов `pgw-backup-wal-c1-shard1-shard1a` (мастер-only контур: single-нода без sync → один агент); `EnsuredAgentSpecs`: `VolumeName` null, `RestartPolicy == "no"`, `Image == options.WalAgentImage`. Новые (AAA):
  1. **AC3-контур**: сид 2 ноды (`shard1a`,`shard1b`) + `HttpListener`-Patroni (sync на `shard1b`; паттерн Task 11) → тик → ДВА ensured-агента per-node; слот ensured на ОБОИХ DSN; ключ `wal` (при объектах в S3) содержит `agents` с двумя нодами.
  2. **Миграция §3.6 / AC7-сценарий**: сид `BackupAgentObjects` старым именем `pgw-backup-wal-c1-shard1` (state running) → тик → старый удалён (`RemovedBackupAgents`), per-node поднят; повторный тик — идемпотентен.
  3. **Смена мастера**: первый тик (мастер `shard1a`) → второй тик с portalloc/master-ключом на `shard1b` → агент `-shard1a` снесён, `-shard1b` поднят.
  4. **Слот исчез — правило «всех нод»**: двухнодовый контур; слот жив только на sync-ноде → тик НЕ пишет BROKEN (ensure на мастере); слот исчез на обеих при живой цепочке (объекты в fake-S3 + ключ ACTIVE) → BROKEN + recreateSlot.
  5. **Sync появился**: первый тик single-нода → один агент; второй тик с sync- Patrons → второй агент поднят, первый жив (не пересоздан).
- [ ] **Step 2: Run красный** (`--filter FullyQualifiedName~WalStreamProcess`).
- [ ] **Step 3: Реализация** правок `WalStreamProcess` + `FakeBackupDeps` + удаление `WalAgentCommand.cs`/`WalAgentCommandTests.cs`.
- [ ] **Step 4: Run зелёный**: `dotnet build src/PgWorker.slnx -c Debug` (0 warnings) + `dotnet test src/tests/PgWorker.UnitTests -c Debug` (полный — упавшие ссылки на WalAgentCommand удалены) + `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalStreamProcess|FullyQualifiedName~ShardEndpointsSync"`. После серии — зачистка (`docker ps -a --filter name=pgw-`, `docker network prune -f`).
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — WalStreamProcess: два источника (мастер+sync), per-node агенты без volume, супервиз/миграция со старого формата, agents-факты в ключе; WalAgentCommand (pg_receivewal/bash) удалён`.

**Связь со spec:** §3.3, §3.4, §3.6, AC3, AC7-сценарий миграции.

---

### Task 13 (Ф3, условная): history-fallback воркером (docker-exec + put)

> Условие: Task 6 Step 5 зафиксировал, что Npgsql НЕ экспонирует `TIMELINE_HISTORY` (маркер `history_missing`). Если API есть — задача пропускается (отметить в коммите Task 14).

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` (`ControlDueAsync`)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (+1 Fact)

**Interfaces:**
- Consumes: `driver.ExecNodeAsync(cluster, shard, node, cmd, ct)` (существует), `s3.PutObjectAsync`.
- Produces: объект `wal/<tli>.history` при TLI-переходе (AC5-механика).

Механика: в `ControlDueAsync` после list — найти TLI-переходы без history-объекта: множество TLI сегментов из `objects`, для каждого `tli > chainStart.Tli` без `wal/{tli:x8}.history` в списке → `ExecNodeAsync` на источнике (первый живой из sources): `["sh","-c","base64 -w0 /home/postgres/pgdata/pgroot/pg_wal/{tli:x8}.history"]` (Spilo-layout; base64 — бинарная безопасность) → decode → `PutObjectAsync($"{cluster}/{shard}/wal/{tli:x8}.history", bytes, sha256)`; exec-сбой/файла нет → transient (следующий контроль повторит; счётчик не вводим — YAGNI).

- [ ] **Step 1: Failing-тест**: fake-S3 с сегментами `000000010000000000000001`, `000000020000000000000001` (TLI-переход), history НЕТ; StubScaleDriver.ExecNodeAsync → base64 синтетики `("1\t0/1000000\t...\n")` → тик → history-объект в fake-S3; повторный тик — идемпотентен (exec не повторяется: счётчик вызовов).
- [ ] **Step 2: красный → Step 3: реализация → Step 4: зелёный** (`--filter FullyQualifiedName~WalStreamProcess`).
- [ ] **Step 5: Commit** — `feat(wal): t27 Ф3 — history-fallback: контроль докладывает <tli>.history docker-exec ноды при TLI-переходе без объекта (канон §3)`.

**Связь со spec:** §3.1 п.3 fallback, §7-риск, AC5.

---

### Task 14 (Ф4): E2E — инфраструктура, раскарантин, новые сценарии

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (`WalImage`, `EnsureWalImageAsync`)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs` (раскарантин + 4 новых Fact; хелпер `StartWalHostAsync` + `PgWorker__Backups__Wal__AgentImage`)

**Interfaces:**
- Consumes: Task 7 (Dockerfile), Task 12 (оркестрация), готовая инфраструктура E2eEnvironment/E2eFixture (изоляция, телеметрия, teardown).
- Produces: AC2/AC3/AC4/AC5/AC8-доказательства.

Общие для сценариев: `E2eEnvironment.StartAsync("<slug>", withMinio: true)` (свой guid-контур), `SeedClusterAsync`, `StartWalHostAsync` (+`["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage`); нагрузка — `GenerateWalAsync` (существует); хостовые `BackupS3`-клиенты — паттерн существующего теста; все wait-бюджеты ≤ 300 c; teardown фикстуры — готовый; после серии — зачистка контейнеров/сетей.

- [ ] **Step 1: Инфраструктура** — `E2eEnvironment`: `public const string WalImage = "pgworker-wal:e2e";` + `EnsureWalImageAsync` (docker build `-f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:e2e <root>`, один раз на процесс — образец `EnsureJobImageAsync`); вызов рядом с ним (в ветке `withMinio`); `StartWalHostAsync` — env `PgWorker__Backups__Wal__AgentImage`.
  Проверка: `dotnet build src/tests/PgWorker.IntegrationTests -c Debug`.
- [ ] **Step 2: Раскарантин `WalStream_UploadsSegmentsContinuously` (AC8)** — удалить `Assert.Skip("t27-wal-staging-loss: ...")` и комментарий карантина; ассерт 4 (агент running) — обновить под per-node: `docker ps --filter name=pgw-backup-wal-<C>-shard1-` → есть `running`-агент мастера (имя ноды из `MasterPgAsync`); добавить ассерт: ключ `wal` содержит `agents` (≥1 запись, state=running). Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter FullyQualifiedName~WalStream_UploadsSegmentsContinuously` → PASS.
- [ ] **Step 3: `WalStream_AgentRecreate_UnderLoad_NoGap` (AC2)** — нагрузка фоном (Task-цикл INSERT+switch ≥ 2 мин); `docker rm -f <агент мастера>`; wait ≤ 120 c: агент пересоздан (docker ps: имя то же, контейнер новый), ≥1 новый сегмент после рескрета; `WalChain.Check(chainStart, list).IsContinuous`; ключ `wal` ∈ {ACTIVE, DEGRADED} и возвращается в ACTIVE ≤ 120 c; `BROKEN` НЕ возникал (поллинг ключа в цикле нагрузки: видеть BROKEN → fail).
- [ ] **Step 4: `WalStream_DualArchiving_TwoAgents_OnePrefix` (AC3-E2E)** — кластер 2 ноды/шард (дефолт `SyncStrict=true` — sync-standby появится сам); wait ≤ 300 c: ДВА running-агента (`pgw-backup-wal-<C>-shard1-<мастерНода>` и `-<syncНода>`); нагрузка; wait: сегменты растут, `WalChain` непрерывен (union одного префикса), ключ `ACTIVE`, `agents` — обе ноды; SQL-зонды: слот `pgw_bkp_<C>_shard1` существует на ОБОИХ нодах (2 Npgsql-коннекта к published-портам обеих нод из portalloc).
- [ ] **Step 5: `WalStream_MasterDown_ReplicaAgentContinues` (AC4)** — нагрузка; docker stop контейнера мастера (pgw-<C>-shard1-<n>); Patroni promote реплики (ждём смены primary пробами `/primary` — хелпер `MasterPgAsync`); wait ≤ 300 c: агент реплики продолжает доставку (новые сегменты после смерти мастера; promote создаёт WAL), цепочка непрерывна (`WalChain.CheckWithRestart`), ключ не `BROKEN`; после promote воркер пересоздаёт агентов на новых ролях (агент новой мастера-ноды running ≤ 300 c).
- [ ] **Step 6: `WalStream_Promote_TliHistory_ChainGlued` (AC5)** — тот же контур шага 5 (отдельный Fact, свой guid): после promote наблюдается TLI ≥ 2; wait: объект `wal/<tli>.history` в S3 (приёмник или fallback Task 13); `WalChain.CheckWithRestart(chainStart, names).IsContinuous`; ключ `ACTIVE`.
- [ ] **Step 7: Полная E2E-серия бэкапов + зачистка** — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~E2eBackupScenarios"` → PASS; телеметрия по канону (артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `[PHASE]`-строки — готовая инфраструктура); `docker ps -a --filter name=pgw-` пуст, `docker network prune -f`, `docker volume ls -q --filter name=pgw-` пуст.
- [ ] **Step 8: Commit** — `feat(wal): t27 Ф4 — E2E: раскарантин WalStream_UploadsSegmentsContinuously (AC8), пересоздание агента без дыры (AC2), двойная архивация (AC3), смерть мастера (AC4), promote/TLI+.history (AC5)`.

**Связь со spec:** Ф4, AC2–AC5, AC8.

---

### Task 15 (Ф5): Мерж-гейт

**Files:**
- Modify: `arch/roadmap/reliability.md` (удалить пункт `t27-wal-staging-loss` + все `← t27`-упоминания)
- Modify: `arch/roadmap/reliability-report.md` (обновить сводки по факту сделанного)

**Interfaces:**
- Consumes: все задачи.
- Produces: зелёный мерж-гейт (AC9).

- [ ] **Step 1: Свежий Release, полный билд** — `dotnet build src/PgWorker.slnx -c Release` → 0 warnings, 0 errors.
- [ ] **Step 2: Юниты (Release)** — `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName!~IntegrationTests"` → все зелёные; зачистка не нужна (docker нет), но проверить `docker ps -a --filter name=pgw-` пуст.
- [ ] **Step 3: Интеграции (Release)** — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release` → зелёные; после: `docker network prune -f`, `docker ps -a --filter name=pgw-` пуст, тома pgw-* пусты.
- [ ] **Step 4: E2E на свежем Release + кейс-маркер** — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` → PASS; полная E2E-серия PgWorker (`E2eFixture`-сценарии, затронутые изменениями воркера: backup/scale/supervisor минимум) → PASS; зачистка между сериями (см. Global Constraints).
- [ ] **Step 5: Roadmap** — удалить тег `t27-wal-staging-loss` из `arch/roadmap/reliability.md` (списки + `←`-зависимости) и обновить `reliability-report.md` — ТЕМ ЖЕ коммитом.
- [ ] **Step 6: Финальный прогон без остатков** — `docker ps -a --filter name=pgw-` пусто; `docker network ls | grep pgw-` пусто; `docker volume ls -q | grep pgw-` пусто; etcd-префиксы тестов удалены (внутри фикстур).
- [ ] **Step 7: Commit** — `chore(wal): t27 Ф5 — мерж-гейт: серии Release зелёные, кейс-маркер, roadmap-тег снят, reliability-report обновлён`.

**Связь со spec:** Ф5, AC9.

---

## Порядок исполнения и зависимости

```
Task 0 (канон) → Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 (Ф2)
                                                   ↘ Task 7, 8, 9, 10, 11 → Task 12 → (Task 13 условная) → Task 14 (Ф4) → Task 15 (Ф5)
```

Task 7–11 взаимно независимы (после Task 5); Task 12 стягивает их все; Task 14 требует 7+12; Task 13 — только между 12 и 14, по вердикту Task 6.

## Контроль покрытия критериев приёмки

| AC | Закрывается |
|---|---|
| AC1 (инвариант подтверждения) | Task 4 (юнит), Task 6 Steps 1/3 (SQL-зонд, до/после паузы S3) |
| AC2 (пересоздание без потери) | Task 14 Step 3 |
| AC3 (двойная архивация) | Task 12 Step 1 (интеграции: 2 агента, слоты на обеих нодах, agents-поле), Task 14 Step 4 |
| AC4 (потеря источника) | Task 14 Step 5 |
| AC5 (failover/TLI) | Task 14 Step 6 (+ Task 4 TLI-юнит, Task 13 fallback при необходимости) |
| AC6 (S3-обрыв) | Task 6 Step 3 |
| AC7 (совместимость/миграция) | Task 9 (парсеры с/без agents), Task 12 Step 1 (миграция старого формата) |
| AC8 (карантин) | Task 14 Step 2 |
| AC9 (регресс-гейт) | Task 15 |

## Риски исполнения (из spec §7)

- **Npgsql-API** (`SetReplicationStatus`/auto-keepalive/`TIMELINE_HISTORY`/LSN-конверсии): сверять по XML-doc пакета 10.0.3; поведение keepalive-ответов фиксируется интеграцией AC1 (Task 6) — если `restart_lsn` уходит без подтверждения, остановиться и разбираться по логам (НЕ увеличивать ожидания молча).
- **Флэки E2E от параллельной доставки двух агентов**: list-ассерты — только через `WalChain` (union), поллинг до согласованности ключ×list (паттерн существующего теста).
- **E2E бюджет**: фазы > 60 c телеметрируются автоматически; упавший сценарий — `MarkFailed()`, артефакты, разбор без перезапуска.
- **Образ dotnet/runtime** отсутствует локально — до стендов/E2E выполнить зеркалирование (Task 7 Step 2).
