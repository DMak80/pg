# t24-walreceiver-tli-jump — план реализации

> **Для исполнителя:** план исполняет субагент coder (сильная модель) — ВЕСЬ план
> сам, включая подпункты (режим executor="coder"; coder-flash не используется).
> Подпункты — самодостаточные задания по общим правилам; coder выполняет их как
> обычные инструкции. Пути — от корня worktree
> `/Users/demakaev/ZCodeProject/worktrees/feat-t24-walreceiver-tli-jump/`.
> Обязателен к прочтению: `AGENTS.base.md` (корень монорепо) и `AGENTS.md` проекта.
>
> **Состояние исполнения:** шаги 1–12 ВЫПОЛНЕНЫ и закоммичены (ceea95f3 …
> 8336e0d6, 12 коммитов); в ветке WIP-коммит 668a039f (E2E t24b со СТАРЫМИ
> гоночными ассертами — подлежит ПЕРЕРАБОТКЕ на ассерты финальной сходимости,
> НЕ удалению). Оставшаяся работа — шаги 13–16.

**Цель:** приёмник WAL переживает 58P01 после фейловера PG досыпкой старого
таймлайна (RPO=0) с fallback-прыжком на текущий таймлайн (осознанный RPO-зазор:
маркер → DEGRADED+`gap` в etcd → форсированный пересъём полного → панельный
алерт `backup-wal-tli-gap`), канон arch/19 правится ДО кода (arch-first).
E2E — детерминированные кейсы сходимости: t24a (досыпка) и t24b (сходимость
после индуцированного среза сегментов) + зелёный `ChainGlued`; ассерты E2E —
ТОЛЬКО финальная сходимость системы (spec §2, §3.7, §6 п.15).

**Архитектура:** двухшаговая tli-процедура внутри приёмника (IDENTIFY_SYSTEM →
досыпка с явным TIMELINE на СВЕЖЕМ соединении → прыжок от границы сегмента
xlogpos), сигнал о зазоре — stdout-маркер `tli_jump` → персистентная фиксация
контролем воркера в поле `gap` ключа `wal` (etcd — источник истины, переживает
рестарты), снятие — покрытием COMPLETED-полным; планировщик полных форсирует
пересъём по живому непокрытому `gap`; панель читает `gap` толерантно и
алертит critical. Известное ограничение канала (spec §3.2, принято):
пересоздание агентов надзором после фейловера может стирать лог с маркером до
чтения контроля — метка есть грань НАБЛЮДАЕМОСТИ, не корректности; E2E от
метки не зависит: кейсы ассертируют финальную сходимость при любом пути
(прыжок с зазором + пересъём / досыпка без зазора / зазор не возник).

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), Npgsql
10.0.3 (перегрузка `PhysicalReplicationConnection.StartReplication(slot, lsn,
ct, timeline)` и `ReplicationConnection.IdentifySystem(ct)` — подтверждены по
XML-докам пакета; Npgsql-обмен ленив — транспорт форсирует первый шаг чтения в
`StartReplicationAsync`, чтобы 58P01 выходил из вызова старта — коммит 8336e0d6),
xunit + FluentAssertions, testcontainers/docker E2E.

**Спецификация:** `docs/superpowers/2026-10-22-t24-walreceiver-tli-jump/spec.md`
(ОБНОВЛЁННАЯ версия — исполнитель читает spec и план вместе; при конфликте
приоритет у решений spec §6, выписанных ниже в шагах).

## Глобальные ограничения

- Канон — `arch/19-backups.md`; правки канона ДО кода (arch-first, AC8).
- Инвариант подтверждения-по-доставке (t27) НЕ меняется: SetConfirmed — только
  после успешного S3-put, в т.ч. в досыпке и прыжке.
- Layout S3 (`wal/<segment>`, `wal/<tli:x8>.history`), имя слота, правила
  непрерывности/`CheckWithRestart`/BROKEN/ретенции, механика джобов
  полных/verify/restore/drill, механика restore-промоута (t05 AC4),
  KafkaWorker/ValkeyWorker, env-контракт агента — БЕЗ изменений.
- Матч 58P01 — по `PostgresException.SqlState == "58P01"`, НЕ по тексту.
- 58P01 — transient-механика ВНУТРИ агента: без новых permanent-кодов выхода.
- Каждый шаг идемпотентен (arch/17); `gap` в etcd — WriteIfChanged.
- `.history` TLI 1 не существует и не запрашивается (история — только при
  T сервера > TLI хвоста ≥ 1).
- **E2E проверяет сходимость, а не воспроизведение ошибки** (spec §2, §6
  п.15): ассерты обоих кейсов — консистентный итог (доставка завершилась и
  догнала, цепь в S3 цела/склеена через TLI-границу, COMPLETED-полный после
  индуцирования, кластер Active, живого `gap`-поля нет); промежуточные события
  (факт 58P01, факт прыжка, stdout-маркеры `tli_jump`/`tli_backfill`, метка
  `gap` в моменте, timing «прыжок → контроль») в E2E НЕ ассертируются — они
  гоночные (пересоздание агентов надзором — свойство среды, §3.2);
  stdout-маркеры остаются в телеметрии прогона как диагностика. Несходимость в
  бюджет — сигнал к расследованию по телеметрии (без перезапуска до анализа;
  расширение бюджетов не допускается).
- Ветки механики (58P01/прыжок/маркеры/метка) покрываются юнитами (Ф1/Ф2).
- Метка `gap` — грань наблюдаемости, не условие корректности: архивация
  восстанавливается приёмником (§3.1) независимо от того, успел ли контроль
  прочитать маркер; незакрытый зазор вскрывается drill/restore-валидацией.
- Язык: комментарии/документация — русский, идентификаторы — английские.
- Комментарии тестов — по AAA.
- Команды тестов: с env-префиксом при команде
  (`DOTNET_CLI_UI_LANGUAGE=en ...`), вывод длинных прогонов — целиком в файл
  (`> /tmp/…log 2>&1`), читать из файла.
- Таймауты ожиданий агента ≤ 30 с на итерацию; тесты docker — только
  динамические порты; зачистка контейнеров/сетей между сериями.
- R3 «не молчание»: каждая сбой-ветка агента (включая отказ старта стрима при
  58P01 в любой конфигурации) пишет диагностику в stderr.

## Review Focus (проверить в ревью, тесты закреплены в шагах)

1. **Цикл «снятие ↔ повторная установка» gap**, пока маркер `tli_jump` жив в
   хвосте лога: гейт установки = «не покрыт И профиль наблюдаем» (шаг 8,
   юниты `TliGap_CoveredByFull_Lifted`, `TliJumpMarker_CoveredByFull_NotSet`).
2. **Старт досыпки/прыжка на погашенном соединении**: отказ 58P01 гасит
   replication-соединение транспорта (`KillReplicationAsync`) — процедура
   ОБЯЗАНА открывать свежее `OpenAsync` перед КАЖДЫМ своим стартом (досыпка,
   прыжок); страховка от регресса — юнит-ассерт `OpenCalls` растёт при
   tli-процедуре (шаг 5) и инвариант закрытий `Disposed >= Opened - 1`.
3. **Утечка walsender'ов** от `IdentifySystemAsync` и процедурных
   `OpenAsync` (короткоживящие соединения): закрытий ≥ открытий − 1 (шаг 2,
   юнит `IdentifySystem_ConnectionClosed`; шаг 5 `TliProcedure_DoesNotLeakConnections`).
4. **Досыпка/прыжок подтверждают слот только после put** — тот же инвариант
   RPO в новых ветках (шаг 5: процедура использует штатный контур приёма).
5. **Гоночные ассерты в E2E** (spec §3.2/§6 п.15): в t24a/t24b НЕ должно быть
   ассертов на 58P01/прыжок/stdout-маркеры/метку `gap` в моменте/timing
   «прыжок → контроль»; только 5 финальных инвариантов сходимости (шаг 13);
   юнит `TliGap_HeldWithoutMarker` — удержание живого gap (наблюдаемость).

Плюс backwards-compat парсеров: ключ `wal` без `gap` читается без parseErrors
и в панели, и в воркере (шаги 7, 10 — юниты).

---

## Шаг 1. Ф0 — правки канона (arch-first, ДО кода)

**Статус:** ВЫПОЛНЕН — коммит `ceea95f3`.

**Вход:** spec одобрен; worktree чист (изменений кода нет).

**Действие** (сильная модель сама — документация, не для слабой модели): внести
правки по spec §8 (9 пунктов: arch/19 §3 «58P01 tli-переход» + `.history`
дополнение + правила непрерывности; §4 ключ `wal` с полем `gap`; §2 сигнал
планировщика; §3.5/§3.6 видимость зазора; §10 риск; adminpanel/02 толерантное
чтение `gap`; adminpanel/03 строка алерта `backup-wal-tli-gap`). Тексты — в
стиле окружающих абзацев канона.

**Выход:** канон описывает механику 58P01-переживания, поле `gap`, сигнал
планировщика, видимость в restore/drill и панельный алерт.

**Проверка:** оба файла против spec §8 пункт-к-пункту; `git diff --stat arch/`
— только эти файлы.

**Связь со spec:** §4 Ф0, §8 (AC8).

Подпунктов нет (документация).

---

## Шаг 2. Ф1 — контракт и транспорт: `WalStartRefused`, `IdentifySystemAsync`, `timeline`

**Статус:** ВЫПОЛНЕН — коммит `420328da` (сопутствующее уточнение реализации:
Npgsql-обмен ленивый — транспорт форсирует первый шаг чтения в
`StartReplicationAsync`, чтобы 58P01 выходил из вызова старта; вошло в коммит
шага 12 `8336e0d6`).

**Вход:** шаг 1 слит (канон описывает механику).

**Действие:** расширить контракт приёмника и Npgsql-транспорт; обновить сигнатуры
фейков юнитов. Прод-код — подпунктами 2.1–2.5. Затем coder сам пишет юниты
(перечень ниже) по образцу `WalReceiverSourceConnectionTests`.

**Выход:** `IXLogReplicationSource` умеет `IdentifySystemAsync` и явный
`timeline` в старте; 58P01 выходит наружу типизированным `WalStartRefused`;
все реализации/фейки компилируются.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
(0 warnings) и
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalReceiverSourceConnectionTests"` — зелёные, включая новые:
- `StartRefused_58P01_ReturnsTypedMarker` — фейк-соединение бросает
  `new PostgresException("requested WAL segment … has already been removed", "ERROR", "58P01", "")`
  из `StartReplication` → `StartReplicationAsync(..., timeline: null, ...)`
  возвращает Failed, `Error` — `WalStartRefused`, `Error.Message` содержит текст
  исходной ошибки;
- `StartFailed_OtherSqlState_PlainApplicationException` — тот же бросок с
  SqlState `"42P01"` → `Error` НЕ `WalStartRefused`;
- `IdentifySystem_ConnectionClosed` — `IdentifySystemAsync` на фейке с
  счётчиками (без предварительного `OpenAsync` источника): после вызова
  `Opened == Disposed` (короткоживящее соединение гасится; инвариант
  «закрытий ≥ открытий − 1»);
- `IdentifySystem_Failed_ReturnsFailed` — фейк с `FailOpen = true` (без
  предварительного основного `OpenAsync` источника):
  `IdentifySystemAsync` → Failed (transient-семантика ядра).

**Связь со spec:** §3.1 (распознавание 58P01, §6 п.9; IDENTIFY_SYSTEM; явный
TIMELINE), §9 (риск «Npgsql-перегрузка»).

### Подпункт 2.1. Новый тип `WalStartRefused`

В файле `src/PgWorker.WalReceiver/IXLogReplicationSource.cs` перед объявлением
интерфейса `IXLogReplicationSource` добавь класс:

```csharp
/// <summary>Отказ START_REPLICATION по 58P01 (сервер не может отдать WAL со
/// стартовой позиции: запрошенный сегмент срезан/не существует на запрошенном
/// таймлайне) — типизированный маркер наружу из транспорта; текст исходной
/// ошибки Postgres — в Message (диагностика stderr). transient-механика
/// tli-процедуры приёмника; матч по SqlState, не по тексту. Транспорт гасит
/// соединение при этом отказе — старт tli-процедуры требует свежего
/// OpenAsync.</summary>
public sealed class WalStartRefused(string serverError)
    : ApplicationException($"START_REPLICATION refused (58P01): {serverError}");
```

### Подпункт 2.2. Расширение контракта `IXLogReplicationSource`

В файле `src/PgWorker.WalReceiver/IXLogReplicationSource.cs`:

1. Добавь record сразу после класса `WalStartRefused`:

```csharp
/// <summary>Идентификация сервера репликации (IDENTIFY_SYSTEM): текущий
/// таймлайн сервера и позиция записи WAL.</summary>
public sealed record ServerIdentity(uint Tli, ulong XLogPos);
```

2. В интерфейсе `IXLogReplicationSource` замени метод

```csharp
Task<Result> StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct);
```

на

```csharp
/// <summary>START_REPLICATION SLOT <slot> PHYSICAL <startLsn>; timeline —
/// явный TIMELINE команды (null — без TIMELINE, как раньше). Отказ 58P01 —
/// наружу как WalStartRefused (соединение при отказе гасится: следующий
/// старт требует OpenAsync).</summary>
Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct);

/// <summary>IDENTIFY_SYSTEM по короткоживущему replication-соединению
/// (образец ReadTimelineHistoryAsync: стрим-коннект после failed-старта уже
/// погашен).</summary>
Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct);
```

3. Дополни doc-comment интерфейса (сводку семантики) фразой о 58P01 →
   `WalStartRefused` и явном TIMELINE (по образцу существующего текста).

### Подпункт 2.3. `IReplicationConnection` + прод-обёртка

В файле `src/PgWorker.WalReceiver/NpgsqlReplication.cs`:

1. В интерфейс `IReplicationConnection` добавь метод (после `Open`):

```csharp
/// <summary>IDENTIFY_SYSTEM: (Timeline, XLogPos) сервера.</summary>
Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct);
```

2. В интерфейсе `IReplicationConnection` замени сигнатуру

```csharp
IAsyncEnumerable<XLogDataMessage> StartReplication(
    PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn, CancellationToken ct);
```

на

```csharp
IAsyncEnumerable<XLogDataMessage> StartReplication(
    PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn,
    CancellationToken ct, uint? timeline);
```

3. В классе `NpgsqlPhysicalReplicationConnection` реализуй оба метода:

```csharp
public async Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct)
{
    var identity = await _inner.IdentifySystem(ct);
    return (identity.Timeline, (ulong)identity.XLogPos);
}

public IAsyncEnumerable<XLogDataMessage> StartReplication(
    PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn,
    CancellationToken ct, uint? timeline)
    => timeline is { } tli
        ? _inner.StartReplication(slot, startLsn, ct, tli)
        : _inner.StartReplication(slot, startLsn, ct);
```

(перегрузка Npgsql `StartReplication(slot, lsn, ct, timeline)` — «streaming
starts on timeline tli», сервер отдаёт исторический таймлайн до точки
переключения; вызов без timeline — существующая перегрузка.)

### Подпункт 2.4. `NpgsqlXLogSource`: 58P01, timeline, IDENTIFY_SYSTEM

В файле `src/PgWorker.WalReceiver/NpgsqlReplication.cs`, класс `NpgsqlXLogSource`:

1. Замени тело `StartReplicationAsync` (сигнатура нового контракта со
   значением-параметром `uint? timeline`):

```csharp
public async Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct)
{
    try
    {
        var replication = _replication ?? throw new InvalidOperationException("OpenAsync not called");
        var physicalSlot = new PhysicalReplicationSlot(slot);
        _messages = replication.StartReplication(
            physicalSlot, (NpgsqlLogSequenceNumber)startLsn, ct, timeline);
        return Result.Success();
    }
    catch (Npgsql.PostgresException e) when (e.SqlState == "58P01")
    {
        // Отказ старта по 58P01: типизированный маркер наружу (матч по SqlState,
        // не по тексту) — tli-процедура ядра; соединение гасим как при любом
        // сбое старта (не переиспользуем полуживой walsender; процедуре нужен
        // свежий OpenAsync перед её стартом).
        await KillReplicationAsync();
        return Result.Failed(new WalStartRefused(e.MessageText));
    }
    catch (Exception e)
    {
        // Стрим не начался — соединение непригодно к переиспользованию:
        // гасим (следующий цикл OpenAsync создаст свежее), не оставляя
        // источнику полуживой walsender.
        await KillReplicationAsync();
        return Result.Failed(new ApplicationException($"START_REPLICATION {slot}: {e.Message}", e));
    }
}
```

2. Добавь метод (после `ReadTimelineHistoryAsync`):

```csharp
public async Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
{
    // IDENTIFY_SYSTEM — replication-команда: короткоживущее соединение по
    // образцу ReadTimelineHistoryAsync (стрим-коннект после failed-старта
    // уже погашен); каждая попытка живёт ровно до своего Dispose.
    try
    {
        await using var connection = NewConnection();
        await connection.Open(ct);
        var (tli, xlogPos) = await connection.IdentifySystem(ct);
        return Result<ServerIdentity>.Success(new ServerIdentity(tli, xlogPos));
    }
    catch (Exception e)
    {
        return Result<ServerIdentity>.Failed(new ApplicationException(
            $"IDENTIFY_SYSTEM {_o.PgHost}:{_o.PgPort}: {e.Message}", e));
    }
}
```

### Подпункт 2.5. Фейки юнитов — новые сигнатуры и точки программирования

1. Файл `src/tests/PgWorker.UnitTests/Backups/WalReceiverSourceConnectionTests.cs`,
   класс `FakeReplicationConnection`: замени сигнатуру
   `StartReplication(PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber
   startLsn, CancellationToken ct)` на
   `StartReplication(PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber
   startLsn, CancellationToken ct, uint? timeline)` (тело прежнее — бросает при
   `FailStartReplication`, иначе пустой поток); добавь поля/флаги и метод:

```csharp
// Точки программирования: флаг отказа Open (сбой открытия короткоживящего
// Identify-соединения), исключение из StartReplication (например,
// PostgresException с SqlState 58P01), результат IdentifySystem.
public bool FailOpen;
public Exception? StartReplicationException;
public (uint Tli, ulong XLogPos) IdentifyResult = (1, 0);

public Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct)
    => Task.FromResult(IdentifyResult);
```

В `Open(CancellationToken ct)` после `Opened++` добавь
`if (FailOpen) throw new IOException("fake: open failed");`.
В теле `StartReplication` первым делом:
`if (StartReplicationException is { } ex) throw ex;` (до существующей проверки
`FailStartReplication`). Тесты настройки: 58P01 → `StartReplicationException =
new PostgresException("requested WAL segment has already been removed",
"ERROR", "58P01", "")`; другой SqlState → тот же конструктор с `"42P01"`.

2. Файл `src/tests/PgWorker.UnitTests/Backups/WalReceiverCoreTests.cs`, класс
   `FakeXLogSource`: сигнатуру
   `StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct)`
   замени на `(string slot, ulong startLsn, uint? timeline, CancellationToken
   ct)` (тело прежнее; сохраняй также последний `timeline` в поле `uint?
   LastTimeline`); добавь метод:

```csharp
public Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
    => Task.FromResult(Result<ServerIdentity>.Success(new ServerIdentity(1, 0)));
```

(программируемость `Identity`/отказов — шаг 5).

3. Файл `src/PgWorker.WalReceiver/WalReceiverCore.cs`: единственный вызов
   `source.StartReplicationAsync(o.Slot, tail.Value, ct)` приведи к
   `source.StartReplicationAsync(o.Slot, tail.Value, null, ct)` (параметр
   `timeline: null` — «без TIMELINE», прежнее поведение).

---

## Шаг 3. Ф1 — резолв хвоста: позиция + имя сегмента

**Статус:** ВЫПОЛНЕН — коммит `7a421d94`.

**Вход:** шаг 2 слит.

**Действие:** резолвер возвращает пару (позиция, имя хвостового сегмента) —
tli-процедуре нужен TLI из имени. Прод-код — подпункты 3.1–3.2; тесты
`WalReceiverTailTests` правит coder (ассерты `.Position`, значения те же).

**Выход:** `S3TailResolver.ResolveTailAsync` возвращает `Result<WalTail?>`;
ядро хранит `tailSegment`.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalReceiverTailTests"` — зелёные (все 5 старых
кейсов на новом типе + у непрерывной цепочки `SegmentName` == имя последнего
сегмента); сборка решения 0 warnings.

**Связь со spec:** §3.1 (вход tli-процедуры `tailSegment`).

### Подпункт 3.1. `WalTail` в `S3TailResolver`

В файле `src/PgWorker.WalReceiver/S3TailResolver.cs`:

1. Перед классом `S3TailResolver` добавь:

```csharp
/// <summary>Хвост S3: стартовая LSN (конец последнего сегмента непрерывной
/// части цепочки) + имя хвостового сегмента (TLI из имени — вход tli-процедуры
/// при 58P01).</summary>
public sealed record WalTail(ulong Position, string SegmentName);
```

2. Сигнатуру `ResolveTailAsync` смени на
   `public static async Task<Result<WalTail?>> ResolveTailAsync(...)` (параметры
   те же); возвраты: `Result<WalTail?>.Success(null)` — пустой префикс/цепочка
   не закреплена (как сегодня); `Result<WalTail?>.Success(new
   WalTail(EndLsn(last), last.Name))` — вместо текущего `Success(EndLsn(last))`
   (значение `last` — `chain.LastSegment`).

### Подпункт 3.2. `WalReceiverCore` — резолв с именем

В файле `src/PgWorker.WalReceiver/WalReceiverCore.cs`, блок «2. Стартовая
позиция» замени так, чтобы резолв давал и имя (семантика веток прежняя:
транспортный сбой — ретрай, пустой префикс — старт от restart_lsn слота,
слота нет — код 4):

```csharp
WalTail? walTail = null;
while (walTail is null)
{
    var resolved = await S3TailResolver.ResolveTailAsync(s3, o.Cluster, o.Shard, ct);
    if (resolved.IsSuccess && resolved.Value is { } position)
    {
        walTail = position;
        break;
    }

    if (resolved.IsSuccess)
        break; // пустой префикс — старт от слота

    await Task.Delay(retry, ct);
}

ulong startAt;
string? tailSegment;
if (walTail is { } tailInfo)
{
    startAt = tailInfo.Position;
    tailSegment = tailInfo.SegmentName;
}
else
{
    var restart = await slots.ReadRestartLsnAsync(o.Slot, ct);
    if (!restart.IsSuccess)
    {
        await Emit(WalReceiverMarkers.Result(false, restart.Error!.Message, lastDelivered));
        return 4;
    }

    startAt = restart.Value;
    tailSegment = null;
}
```

а последующий старт (подпункт 2.5 п.3) использует `startAt`:
`source.StartReplicationAsync(o.Slot, startAt, null, ct)`; строку `var
assembler = new SegmentAssembler(tail.Value);` замени на
`var assembler = new SegmentAssembler(startAt);` (после врезки шага 5 `startAt`
может быть обновлён процедурой).

---

## Шаг 4. Ф1 — маркеры `tli_backfill` / `tli_jump`

**Статус:** ВЫПОЛНЕН — коммит `55f14038`.

**Вход:** шаг 3 слит.

**Действие:** подпункт 4.1 (прод-код); тесты `WalReceiverMarkersTests` coder
дописывает. Формат JSON — System.Text.Json сериализует параметры
primary-конструктора в порядке объявления, вычисляемое свойство `Phase` — ПОСЛЕ
них (фактический порядок проекта: у `StreamingMarker`/`DeliveringMarker`
`"phase"` — последнее поле). Порядок полей маркеров НЕФУНКЦИОНАЛЕН: контроль
воркера парсит по `Contains("\"phase\":\"tli_jump\"")` +
JSON-десериализации; в E2E маркеры — телеметрия (не ассерты). Тесты — по
образцу проекта (Contain-ассерты каждого поля), БЕЗ точного равенства строк.

**Выход:** маркеры в stdout-протоколе воркера.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalReceiverMarkersTests"` — зелёные; новые кейсы
Contain-ассертами:
- backfill: `"phase":"tli_backfill"`, `"from_segment":"000000010000000000000003"`,
  `"to_tli":2`;
- jump: `"phase":"tli_jump"`,
  `"from_segment":"000000010000000000000003"`, `"from_tli":1`, `"to_tli":2`,
  `"to_segment":"000000020000000000000007"`.

**Связь со spec:** §3.1 п.3/п.4 (форматы маркеров), §2 (протокол stdout).

### Подпункт 4.1. Маркеры в `WalReceiverMarkers`

В файле `src/PgWorker.WalReceiver/WalReceiverMarkers.cs`:

1. После `PoolExhausted()` добавь фабрики:

```csharp
/// <summary>tli-процедура (58P01): досыпка старого таймлайна от хвостового
/// сегмента до точки переключения — зазора нет (RPO=0).</summary>
public static string TliBackfill(string fromSegment, uint toTli)
    => ToJson(new TliBackfillMarker(fromSegment, toTli));

/// <summary>tli-процедура: прыжок на текущий таймлайн сервера — сегменты
/// (from_segment, to_segment] не будут доставлены (осознанный RPO-зазор);
/// контроль воркера фиксирует поле gap по этому маркеру при from_tli != to_tli.</summary>
public static string TliJump(string fromSegment, uint fromTli, uint toTli, string toSegment)
    => ToJson(new TliJumpMarker(fromSegment, fromTli, toTli, toSegment));
```

2. Внизу класса (рядом с остальными private-records) добавь — по образцу
   `StreamingMarker`/`DeliveringMarker` (вычисляемое `Phase` в теле записи;
   в JSON `"phase"` идёт после полей данных — как у всех multi-field маркеров
   протокола):

```csharp
private sealed record TliBackfillMarker(
    [property: JsonPropertyName("from_segment")] string FromSegment,
    [property: JsonPropertyName("to_tli")] uint ToTli)
{
    [property: JsonPropertyName("phase")]
    public string Phase => "tli_backfill";
}

private sealed record TliJumpMarker(
    [property: JsonPropertyName("from_segment")] string FromSegment,
    [property: JsonPropertyName("from_tli")] uint FromTli,
    [property: JsonPropertyName("to_tli")] uint ToTli,
    [property: JsonPropertyName("to_segment")] string ToSegment)
{
    [property: JsonPropertyName("phase")]
    public string Phase => "tli_jump";
}
```

---

## Шаг 5. Ф1 — tli-процедура ядра (досыпка / прыжок)

**Статус:** ВЫПОЛНЕН — коммит `00651f55`. Перечень юнитов ниже нормализован по
AC2 (spec §7): юниты — покрытие веток механики (промежуточные события);
итог — E2E (§3.7); перед мерж-гейтом (шаг 15) сверить фактический набор с
перечнем — недостающие кейсы дописать мелким коммитом (см. «Проверку» шага 15).

**Вход:** шаги 2–4 слиты.

**Действие:** подпункты 5.1 (процедура), 5.2 (врезка в `RunAsync`), 5.3
(программируемость `FakeXLogSource`). Юниты всех веток §3.1 coder пишет сам
(перечень ниже; тела — по образцу существующих кейсов `WalReceiverCoreTests`).

Ключевой инвариант процедуры: отказ 58P01 ГАСИТ replication-соединение
транспорта — каждый старт процедуры (досыпка, прыжок) выполняется на СВЕЖЕМ
соединении (`OpenAsync` непосредственно перед стартом). Страховка от регресса
«процедура стартует без open» — юнит-ассерты `OpenCalls` (см. тесты) и
инвариант утечки соединений; прод-вердикт — интеграция шага 6 и E2E шагов 12–13.

**Выход:** при 58P01 агент досыпает старый таймлайн или прыгает; маркеры и
stderr-диагностика на месте; счётчики соединений не растут.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalReceiverCoreTests"` — зелёные, включая:

- `TliBackfill_ClosesGap_NoRpoLoss` (AC1-ветка; AC2-часть «подтверждение только
  после put») — в S3 сегменты TLI1 №0–1; старт без TIMELINE от `EndLsn(1)` →
  `WalStartRefused`; `Identity=(Tli:2, XLogPos:…)` → маркер `tli_backfill`,
  `.history` в S3, старт `timeline:1` от того же хвоста → доставка сегмента
  TLI1 №2 → конец потока → переподключение от выросшего хвоста → старт без
  TIMELINE успешен → сегмент TLI2 №3 доставлен. Ассерты: маркер
  `tli_backfill` в output, `tli_jump` НЕТ, `wal/00000002.history` в S3, код 0;
  `OpenCalls >= 2` (стартовый open цикла + процедурный open перед досыпкой —
  процедура НЕ стартует на погашенном соединении); `ConfirmJournal` строго
  после put каждого сегмента (инвариант RPO действует и в ветке досыпки).
- `TliBackfillRefused_FallsBackToJump` (AC2-ядро: TLI-граничный прыжок) —
  отказ и без TIMELINE, и с `timeline:1`; `Identity=(2, XLogPos: 3*SegmentBytes
  + 5MiB)` → маркеры `tli_backfill` и `tli_jump`; `.history` в S3 (put до
  досыпки); старт прыжка ровно от `3*SegmentBytes` (граница сегмента) с
  `timeline:2` → стрим продолжается; `to_segment` ==
  `000000020000000000000003`, `from_tli:1`, `to_tli:2` в output;
  `OpenCalls >= 3` (стартовый + open перед досыпкой + open перед прыжком —
  отказ досыпки гасит соединение); `ConfirmJournal` — подтверждение сегментов
  прыжка только после их put.
- `TliJump_SameTli_NoBackfillNoHistory` (AC2/AC4: внутри-TLI прыжок) —
  `Identity=(1, …)`, отказ без TIMELINE → сразу прыжок (`timeline:1` от
  границы) — НЕТ `tli_backfill`, НЕТ put `.history`, маркер `tli_jump` с
  `from_tli == to_tli == 1`.
- `TliProcedure_RePromoteDuringProcedure_Converges` (AC2: гонки повторного
  промоута, §3.1 п.5) — первый цикл: отказ 58P01 без TIMELINE, `Identity=(2,…)`,
  прыжок `timeline:2` → снова `WalStartRefused` (сервер успел промоутнуться на
  T=3) → `Success(null)` → retry-цикл; второй цикл: `Identity=(3,…)` →
  `.history(3)` put → досыпка `timeline:1` успех → доставка продолжается.
  Ассерты: две итерации процедуры (два маркера `tli_backfill`), маркер
  `tli_jump` первого цикла, конвергенция без счётчиков (доставка растёт,
  код 0 по отмене).
- `TliProcedure_IdentifyFails_PlainRetry` — отказ старта +
  `IdentifySystemAsync` → Failed: ретраи (`OpenCalls` растёт), ни одного
  маркера `tli_backfill`/`tli_jump` (IDENTIFY не выполнен), код 0 по отмене.
- `TliProcedure_HistoryMissing_StillBackfills` — `Identity=(2, …)`, history →
  null → маркер `history_missing` + досыпка продолжается штатно.
- `TliProcedure_OpenFails_PlainRetry` — отказ старта 58P01, процедурный
  `OpenAsync` → Failed (точка отказа `OpenFailuresRemaining`): обычный
  retry-цикл, ПРЫЖКА НЕТ (в `StartLog` нет записей с `Timeline != null`,
  нет маркера `tli_jump`); маркер `tli_backfill` при этом ЕСТЬ — по дословному
  коду процедуры он пишется ДО `OpenAsync`, его присутствие — норма (ассерт
  отсутствия tli_*-маркеров здесь НЕ применяется); код 0 по отмене.
- `WalStartRefused_EmptyS3Tail_PlainRetry` — пустой S3, старт от
  restart_lsn, отказ → ретраи БЕЗ tli-маркеров (процедура неприменима:
  `tailSegment == null`; поведение — по образцу сбой-веток §3.1/§9: transient
  retry-цикл).
- `TliProcedure_DoesNotLeakConnections` (на `FakeReplicationConnection` через
  `NpgsqlXLogSource`): серия «open → refuse → identify → open →
  backfill-refuse → open → jump» ×3 — `Disposed >= Opened - 1` с учётом ВСЕХ
  открытий (процедурных включительно; каждое соединение живёт ровно до своей
  замены).

**Связь со spec:** §3.1 п.1–5 (§6 п.1/2/7/8/14/17), AC1 (юнит-часть), AC2
(юниты — ветки механики), AC4, §9 риски 1/3.

### Подпункт 5.1. Метод `TliProcedureAsync` в `WalReceiverCore`

В файле `src/PgWorker.WalReceiver/WalReceiverCore.cs` добавь private-метод
(после `RunAsync`, перед `PutWithRetryAsync`):

```csharp
/// <summary>Tli-процедура (58P01, arch/19 §3): досыпка старого таймлайна от
/// хвоста S3 до точки переключения (CopyDone → переподключение от выросшего
/// хвоста — зазора нет, RPO=0) или прыжок-fallback на текущий таймлайн от
/// границы сегмента xlogpos (сегменты (tail .. старт] в S3 не появятся —
/// осознанный RPO-зазор, маркер tli_jump). Возвращает Success(стартовая LSN) —
/// старт выполнен, приём продолжается штатным контуром (assembler → put →
/// confirm; подтверждение слота ТОЛЬКО после успешного put — инвариант RPO не
/// меняется); Success(null) — процедура не стартовала (transient: обычный
/// retry-цикл). КАЖДЫЙ старт процедуры выполняется на СВЕЖЕМ replication-
/// соединении: отказ 58P01 гасит прежнее (транспорт), поэтому перед досыпкой
/// и перед прыжком — OpenAsync. Идемпотентна: гонки повторного промоута
/// конвергентны — каждая итерация начинается IDENTIFY_SYSTEM, счётчиков
/// попыток нет; старт с историческим TIMELINE безопасен (сервер отдаёт его
/// до конца ветки).</summary>
private static async Task<Result<ulong?>> TliProcedureAsync(
    WalReceiverOptions o, IXLogReplicationSource source, IBackupS3 s3, TextWriter stdout,
    ulong tail, string tailSegment, TimeSpan retry, CancellationToken ct)
{
    if (WalFileName.TryParse(tailSegment) is not { } tailFile)
        return Result<ulong?>.Success(null); // имя не парсится — retry (защитно)

    // 1. IDENTIFY_SYSTEM: сбой — transient (retry-цикл).
    var identity = await source.IdentifySystemAsync(ct);
    if (!identity.IsSuccess)
    {
        await Console.Error.WriteLineAsync(
            $"tli_procedure: identify failed: {identity.Error!.Message.Split('\n')[0]}");
        return Result<ulong?>.Success(null);
    }
    var (serverTli, xlogPos) = identity.Value;

    // 2. .history нового TLI — ДО досыпки (канон §3: загружается обязательно;
    //    переход в потоке не наблюдается — prevTli сессии инициализируется
    //    заново первым сегментом). TLI 1 истории не имеет — не запрашивается.
    if (serverTli > tailFile.Tli)
    {
        var history = await source.ReadTimelineHistoryAsync(serverTli, ct);
        if (history is { IsSuccess: true } && history.Value is not null)
        {
            await PutWithRetryAsync(
                s3, stdout, o, $"{o.Cluster}/{o.Shard}/wal/{serverTli:x8}.history",
                history.Value,
                Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(history.Value)),
                retry, ct);
        }
        else
        {
            await stdout.WriteLineAsync(WalReceiverMarkers.HistoryMissing(serverTli));
        }
    }

    // 3. Досыпка (только при T сервера > TLI хвоста): стрим старого таймлайна
    //    от хвоста до точки переключения — байты удерживаются слотом на ноде;
    //    повторный 58P01 (удержание потеряно) — прыжок ниже.
    if (serverTli > tailFile.Tli)
    {
        await stdout.WriteLineAsync(WalReceiverMarkers.TliBackfill(tailSegment, serverTli));
        await Console.Error.WriteLineAsync(
            $"tli_procedure: backfill segment {tailSegment} (tli {tailFile.Tli}) up to tli {serverTli} switch point");
        var openedBackfill = await source.OpenAsync(ct);
        if (!openedBackfill.IsSuccess)
        {
            // Отказ 58P01 погасил стрим-соединение — процедуре нужно свежее;
            // неудача открытия — transient (retry-цикл).
            await Console.Error.WriteLineAsync(
                $"tli_procedure: open failed: {openedBackfill.Error!.Message.Split('\n')[0]}");
            return Result<ulong?>.Success(null);
        }

        var backfill = await source.StartReplicationAsync(o.Slot, tail, tailFile.Tli, ct);
        if (backfill.IsSuccess)
            return Result<ulong?>.Success(tail);
        if (backfill.Error is not WalStartRefused)
        {
            await Console.Error.WriteLineAsync(
                $"tli_procedure: backfill start failed: {backfill.Error!.Message.Split('\n')[0]}");
            return Result<ulong?>.Success(null); // transient
        }

        await Console.Error.WriteLineAsync(
            $"tli_procedure: backfill refused (58P01) — old-tli segments recycled, jumping");
    }

    // 4. Прыжок-fallback (и единственный путь при T == TLI хвоста — зазор
    //    внутри одного таймлайна): старт от ГРАНИЦЫ сегмента, содержащего
    //    xlogpos — текущий записываемый сегмент существует всегда, сборщик
    //    доставит его целиком (старт ровно на границе — головного отброса нет).
    //    Отказ досыпки погасил соединение — прыжок на свежем.
    var segmentStart = xlogPos / (ulong)WalFileName.SegmentBytes
                     * (ulong)WalFileName.SegmentBytes;
    var segId = (uint)(segmentStart / (ulong)WalFileName.SegmentBytes);
    var toSegment = new WalFileName(
        serverTli, segId / WalFileName.SegsPerLog, segId % WalFileName.SegsPerLog).Name;
    await stdout.WriteLineAsync(
        WalReceiverMarkers.TliJump(tailSegment, tailFile.Tli, serverTli, toSegment));
    await Console.Error.WriteLineAsync(
        $"tli_procedure: jump {tailSegment} (tli {tailFile.Tli}) -> {toSegment} (tli {serverTli})");
    var openedJump = await source.OpenAsync(ct);
    if (!openedJump.IsSuccess)
    {
        await Console.Error.WriteLineAsync(
            $"tli_procedure: open failed: {openedJump.Error!.Message.Split('\n')[0]}");
        return Result<ulong?>.Success(null); // transient
    }

    var jump = await source.StartReplicationAsync(o.Slot, segmentStart, serverTli, ct);
    if (jump.IsSuccess)
        return Result<ulong?>.Success(segmentStart);

    await Console.Error.WriteLineAsync(
        $"tli_procedure: jump start failed: {jump.Error!.Message.Split('\n')[0]}");
    return Result<ulong?>.Success(null); // включая повторный 58P01 — новая итерация процедуры
}
```

### Подпункт 5.2. Врезка в `RunAsync`

В файле `src/PgWorker.WalReceiver/WalReceiverCore.cs` замени блок «3.
START_REPLICATION с явной стартовой позицией» (резолв шага 3 оставил там
`source.StartReplicationAsync(o.Slot, startAt, null, ct)` + ветку неуспеха со
`start_failed`/delay/continue) на:

```csharp
// 3. START_REPLICATION с явной стартовой позицией; отказ 58P01 — tli-процедура
//    (досыпка старого таймлайна / прыжок на текущий) вместо retry. Процедура
//    стартовала — приём продолжается от её позиции; не стартовала — обычный
//    transient-цикл. tailSegment == null (пустой префикс — старт от
//    restart_lsn слота) — процедура неприменима (вход не определён): retry-цикл.
//    Диагностика любой неуспешной ветки старта — start_failed в stderr (R3
//    «не молчание»: текст WalStartRefused несёт исходную ошибку сервера).
var started = await source.StartReplicationAsync(o.Slot, startAt, null, ct);
if (!started.IsSuccess)
{
    var resumed = started.Error is WalStartRefused && tailSegment is not null
        ? await TliProcedureAsync(o, source, s3, stdout, startAt, tailSegment, retry, ct)
        : Result<ulong?>.Success(null);
    if (resumed is { IsSuccess: true, Value: { } procedureStart })
    {
        startAt = procedureStart;
    }
    else
    {
        await Console.Error.WriteLineAsync(
            $"start_failed: {started.Error!.Message.Split('\n')[0]}");
        await Task.Delay(retry, ct);
        continue;
    }
}
```

(блок «4. Приём» ниже уже использует `new SegmentAssembler(startAt)` — из
подпункта 3.2; `uint prevTli = 0;` остаётся как есть — первая сессия после
процедуры инициализирует TLI первым сегментом, переход не наблюдается).

### Подпункт 5.3. Программируемость `FakeXLogSource`

В файле `src/tests/PgWorker.UnitTests/Backups/WalReceiverCoreTests.cs`, класс
`FakeXLogSource` (после полей `StartCalls`/`StartLsn`):

```csharp
// tli-механика (58P01): политика старта (startLsn, timeline) → результат;
// null — успех (прежнее поведение). Identify — Identity или Failed при null.
// OpenFailuresRemaining — программный отказ процедурного open (счётчик
// оставшихся открытий, которые должны упасть).
public Func<ulong, uint?, Result>? StartPolicy;
public readonly List<(ulong Lsn, uint? Timeline)> StartLog = [];
public ServerIdentity? Identity = new(1, 0);
public int OpenFailuresRemaining;
```

- `StartReplicationAsync(string slot, ulong startLsn, uint? timeline,
  CancellationToken ct)`: добавляй `(startLsn, timeline)` в `StartLog`;
  `if (StartPolicy is { } policy) return policy(startLsn, timeline);` — затем
  прежнее тело (счётчики/`FirstStartLsn`).
- `IdentifySystemAsync` верни `Identity is { } id
  ? Result<ServerIdentity>.Success(id)
  : Result<ServerIdentity>.Failed(new ApplicationException("identify unavailable"))`.
- `OpenAsync` верни Failed при `OpenFailuresRemaining > 0` (декрементируя
  счётчик) — точка отказа процедурного открытия.

(отказ 58P01 в политике — `Result.Failed(new WalStartRefused("fake: segment removed"))`).

---

## Шаг 6. Ф1 — интеграция на живом PG: IDENTIFY_SYSTEM + явный TIMELINE

**Статус:** ВЫПОЛНЕН — коммит `49b51859` (формулировка ниже нормализована:
порог сегментов — от фактической генерации, а не «≥3»).

**Вход:** шаг 5 слит.

**Действие** (coder сам — тест на живом контуре): в файл
`src/tests/PgWorker.IntegrationTests/Backups/WalReceiverIntegrationTests.cs`
добавь факт `TliStart_IdentifyAndExplicitTimeline_StreamsFromTail` по образцу
`SegmentDeliveryAndContinuity`: Arrange — своё окружение `OwnPostgres` +
`OwnS3`, `GenerateWalAsync(adminDsn, Switches)`; доставка ядра
`WalReceiverCore` (старт без TIMELINE) накатывает сегменты — остановить
отменой. Порог ожидания листинга — от ФАКТИЧЕСКОЙ генерации: дождаться ≥1
закрытого сегмента в `wal/` (число сегментов не фиксировать жёстко — приёмник
стартует от `restart_lsn` слота и доставляет только ЗАКРЫТЫЕ сегменты выше
него; «≥3 сегментов при 3 переключениях» недостижимо — незакрытый хвост не
выгружается). Act — прямой транспорт: `source.OpenAsync` →
`IdentifySystemAsync` → ассерты `Tli == 1`, `XLogPos > 0`; хвост S3
(`ListSegmentsAsync`) → `source.StartReplicationAsync(slot, tail, timeline: 1,
ct)` → первый чанк `Stream` (с отменой по бюджету ≤30 с). Assert —
`chunk.WalStart == tail` (перегрузка с timeline стартует ровно от LSN —
поведение риса §9 «не иначе»), данные чанка не пусты, утечки нет
(DisposeAsync source).

**Выход:** перегрузка с timeline проверена на живом PG (вердикт до E2E).

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalReceiverIntegrationTests"` — зелёные; после серии — зачистка контейнеров/сетей теста
(канон AGENTS.md).

**Связь со spec:** §4 Ф1, §9 (риск «Npgsql-перегрузка»).

Подпунктов нет (окружение теста требует знания контура — сильная модель).

---

## Шаг 7. Ф2 — модель и писатель: поле `gap` ключа `wal`

**Статус:** ВЫПОЛНЕН — коммит `e8d3b504`.

**Вход:** шаги 1–6 слиты.

**Действие:** подпункты 7.1–7.3; юниты `WalStatusWriterTests` coder дописывает:
roundtrip с `Gap` (JSON содержит `"gap":{"from":…,"to":…,"tli":…}`), `Gap ==
null` → в JSON поля `gap` нет, чтение ключа без `gap` → `Gap == null`.

**Выход:** `WalStreamState.Gap` сквозь модель/JSON/etcd; backwards-compat.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalStatusWriterTests"` — зелёные.

**Связь со spec:** §3.4, §6 п.10.

### Подпункт 7.1. `WalGapState` в модели

В файле `src/PgWorker.Etcd/Parsing/BackupsModel.cs` перед record
`WalStreamState` добавь:

```csharp
/// <summary>Границы RPO-зазора tli-прыжка приёмника (58P01): сегменты
/// (From .. To] не будут доставлены в S3; пишет контроль воркера по маркеру
/// tli_jump приёмника, снимает покрытием — COMPLETED-полный с
/// wal_start_segment ≥ To (сравнение имён CompareOrdinal).</summary>
public sealed record WalGapState(string From, string To, uint Tli);
```

и в `WalStreamState` добавь ПОСЛЕ `Agents` последний опциональный параметр
(в doc-comment — «t24 (arch/19 §4): опциональный RPO-зазор tli-прыжка; null у
старых ключей — обратная совместимость»):

```csharp
WalGapState? Gap = null);
```

### Подпункт 7.2. Писатель `WalStatusWriter`

В файле `src/PgWorker.Backups/WalStatusWriter.cs`:

1. В `ToJson` добавь аргумент в `WalStatusPayload(...)` после `state.Agents...`:
   `Gap: state.Gap is { } gap ? new GapPayload(gap.From, gap.To, gap.Tli) : null`.
2. В `Parse` — в конструктор `WalStreamState(...)` последним аргументом:
   `payload.Gap is { From: { Length: > 0 }, To: { Length: > 0 }, Tli: not null } g
   ? new WalGapState(g.From, g.To, g.Tli.Value) : null`
   (частично битое поле → Gap=null — ключ читается, деградации нет).
3. Рядом с `WalAgentPayload` добавь private-record:

```csharp
private sealed record GapPayload(
    [property: JsonPropertyName("from")] string From,
    [property: JsonPropertyName("to")] string To,
    [property: JsonPropertyName("tli")] uint? Tli);
```

и поле в `WalStatusPayload` последним:
`[property: JsonPropertyName("gap")] GapPayload? Gap` (null не пишется —
`WhenWritingNull` уже в опциях `Json`).

### Подпункт 7.3. Перенос `Gap` при мутациях записи

В файле `src/PgWorker.Backups/WalStreamProcess.cs` найди конструктор записи в
`ControlDueAsync` — `var next = new WalStreamState(state, slot, masterRef,
start.Name, lastUploadedName, lastUploadedName, lastUploadedUnix, lag, error);`
— и добавь именованный аргумент `Gap: wal?.Gap` (временно: живой `gap` ключа
не теряется при проходах контроля; следующий шаг — tli-gap-блок контроля —
заменит выражение на вычисляемое `Gap: gap`). Мутации через `wal with { … }`
(BreakAsync/StopAllAsync/StopShardAsync/EnsureAgentsAsync) `Gap` уже
переносят — не трогай.

---

## Шаг 8. Ф2 — контроль воркера: чтение маркера, фиксация/снятие `gap`

**Статус:** ВЫПОЛНЕН — коммит `bc12ed6a`. Перечень юнитов ниже нормализован по
AC3 (spec §7); канал маркера — со свойством среды (spec §3.2, принято):
пересоздание агентов надзором может стирать лог до чтения — метка есть грань
наблюдаемости, не корректности; E2E от метки не зависит (ассерты финальные).

**Вход:** шаг 7 слит.

**Действие:** подпункты 8.1–8.3; интеграционные кейсы `WalStreamProcessTests`
coder пишет сам (перечень ниже, по образцу класса; `driver.BackupAgentLogTail`
программирует лог мастер-агента).

**Выход:** контроль фиксирует/держит/снимает `gap` (WriteIfChanged), journal
`wal-tli-gap/<X>`, хвост лога агента 200 строк; внутри-TLI маркер игнорируется.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalStreamProcessTests"` — зелёные, включая:

- `TliJumpMarker_WritesDegradedGap` (AC3: фиксация по маркеру) — сид: полный
  COMPLETED (wal_start ниже), wal-ключ ACTIVE, цепочка сегментов в
  `FakeBackupS3`, лог агента содержит
  `{"from_segment":"000000010000000000000003","from_tli":1,"to_tli":2,"to_segment":"000000020000000000000007","phase":"tli_jump"}`
  (порядок полей — как сериализует приёмник, парсер находил по
  `"phase":"tli_jump"`; в сиде порядок полей значения не имеет) → тик: ключ
  DEGRADED, `Error` начинается с `tli-gap:` и содержит
  `000000010000000000000003`, `000000020000000000000007` и `1→2`,
  `Gap == {From=…000003, To=…000007, Tli=2}`, journal-фаза
  `wal-tli-gap/shard1`.
- `TliJumpGap_CoveredByFull_Lifted` (AC3: снятие покрытием) — wal-ключ с живым
  `Gap` + полный COMPLETED с `wal_start_segment ≥ gap.to` → тик: ключ без
  `Gap`, state ACTIVE (при живом прогрессе), `error` без `tli-gap`.
- `TliJumpSameTli_NoGap` (AC3/AC4: внутри-TLI игнор) — маркер
  `from_tli == to_tli == 1` → ключ БЕЗ `Gap` (state по общим правилам
  lag/тишины).
- `TliGap_WriteIfChanged_Idempotent` (AC3: идемпотентность) — два тика с тем
  же маркером: значение ключа стабильно (второй тик не переписывает —
  journal-фаза одна).
- `TliGap_ProfileGone_Lifted` — wal-ключ с `Gap`, листинг БЕЗ сегментов ниже
  `gap.to` (срезаны) → `Gap` снят.
- `TliGap_HeldWithoutMarker` — wal-ключ с `Gap`, лог агента пустой → ключ
  остаётся DEGRADED, `error` построен из полей записи и содержит `1→2`
  (`Gap` перенесён; текст стабилен между тиками — WriteIfChanged).
- `TliJumpMarker_CoveredByFull_NotSet` — маркер в логе, но полный с
  `wal_start ≥ to_segment` уже есть → `Gap` НЕ ставится (нет цикла
  снятие↔установка).

**Связь со spec:** §3.2 (включая «известное ограничение канала»), §6
п.3/4/5/6/13, AC3, AC4; §9 риски (хвост 200, ложный tli-gap при
restore-промоуте, стирание маркера пересозданием).

### Подпункт 8.1. `StubScaleDriver`: программируемый лог агента

В файле `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` добавь
public-поле `public string BackupAgentLogTail = "";` и замени тело
`BackupAgentLogsAsync` на `=> Task.FromResult(Result<string>.Success(BackupAgentLogTail));`
(семантика остальных вызовов не меняется).

### Подпункт 8.2. Парсер маркера `tli_jump` и TLI-хелпер в `WalStreamProcess`

В файле `src/PgWorker.Backups/WalStreamProcess.cs`:

1. В конец класса (перед `S3PulseAsync`) добавь private-тип, парсер и хелпер:

```csharp
// t24 (arch/19 §3/§4): маркер прыжка из лога приёмника (stdout-JSON,
// WalReceiverMarkers.TliJump); парсится последняя строка с "phase":"tli_jump".
// Имена сегментов валидируются WalFileName — чужие/битые строки не маркер.
private sealed record TliJumpLogMarker(
    [property: System.Text.Json.Serialization.JsonPropertyName("from_segment")] string? FromSegment,
    [property: System.Text.Json.Serialization.JsonPropertyName("from_tli")] uint FromTli,
    [property: System.Text.Json.Serialization.JsonPropertyName("to_tli")] uint ToTli,
    [property: System.Text.Json.Serialization.JsonPropertyName("to_segment")] string? ToSegment);

internal static (string FromSegment, uint FromTli, uint ToTli, string ToSegment)? ParseTliJumpMarker(
    string logTail)
{
    var line = logTail
        .Split('\n')
        .LastOrDefault(l => l.Contains("\"phase\":\"tli_jump\"", StringComparison.Ordinal))
        ?.Trim();
    if (line is null)
        return null;
    try
    {
        var marker = System.Text.Json.JsonSerializer.Deserialize<TliJumpLogMarker>(line);
        return marker is { FromSegment: { Length: > 0 }, ToSegment: { Length: > 0 } } m
               && WalFileName.TryParse(m.FromSegment) is not null
               && WalFileName.TryParse(m.ToSegment) is not null
            ? (m.FromSegment, m.FromTli, m.ToTli, m.ToSegment)
            : null;
    }
    catch (System.Text.Json.JsonException)
    {
        return null; // чужая/битая строка — не маркер
    }
}

// TLI из 24-hex имени сегмента (первые 8 символов); битое имя — 0 (текст
// tli-gap строится только из валидированных имён).
internal static uint TliOfSegment(string segmentName)
    => WalFileName.TryParse(segmentName) is { } f ? f.Tli : 0;
```

2. Оба существующих вызова `driver.BackupAgentLogsAsync(cluster,
   BackupAgentNames.Container(cluster, shard, masterRef), 50, ct)` (ветки
   pool_exhausted) — замени литерал `50` на `200`; в класс добавь private-const
   `private const int AgentLogTail = 200;` и используй её в этих и новом вызове
   (окно видимости однократного маркера ≥ 15 мин при streaming 1/5 c).

### Подпункт 8.3. Tli-gap-блок в `ControlDueAsync`

В файле `src/PgWorker.Backups/WalStreamProcess.cs`, в `ControlDueAsync`:

1. Сразу после строки `Observe(cluster, shard, lag, now - lastUploadedUnix.Value);`
   вставь:

```csharp
// t24 (arch/19 §3/§4): tli-зазор — маркер tli_jump приёмника в хвосте лога
// мастер-агента. Персистентная фиксация в ключе wal (переживает рестарты
// воркера/агентов — не зависит от лога контейнера); снятие — покрытием
// (COMPLETED-полный с wal_start ≥ gap.to) либо исчезновением профиля зазора
// из листинга (ретенция срезала нижележащие TLI). Установка гейтится теми же
// предикатами — цикл «снятие ↔ повторная установка» при живом в окне маркере
// исключён. Внутри-TLI прыжок (from_tli == to_tli) — обычная дыра (BROKEN
// путь контроля по объектам), gap НЕ ставится. Пропуск чтения (маркер ушёл
// из хвоста ИЛИ пересоздание контейнера надзором стёрло лог — свойство среды,
// spec §3.2) — не критичен: метка — грань наблюдаемости, не корректности;
// незакрытый зазор вскрывают drill/restore.
static bool CoveredByFull(IReadOnlyList<FullBackupState> fulls, string toSegment)
    => fulls.Any(f => f.State == FullBackupStatus.Completed
        && f.WalStartSegment is { Length: > 0 } walStart
        && string.CompareOrdinal(walStart, toSegment) >= 0);
bool ProfileObserved(string toSegment)
    => objects.Select(o => WalFileName.TryParse(o.Name)).OfType<WalFileName>()
        .Any(s => string.CompareOrdinal(s.Name, toSegment) < 0);
var shardFulls = shardBackups?.Full ?? [];

WalGapState? gap = wal?.Gap;
if (gap is not null && (CoveredByFull(shardFulls, gap.To) || !ProfileObserved(gap.To)))
    gap = null; // заживление покрытием / профиль зазора срезан ретенцией

if (gap is null)
{
    var agentLogTail = await driver.BackupAgentLogsAsync(cluster,
        BackupAgentNames.Container(cluster, shard, masterRef), AgentLogTail, ct);
    if (agentLogTail.IsSuccess
        && ParseTliJumpMarker(agentLogTail.Value) is { } marker
        && marker.FromTli != marker.ToTli
        && !CoveredByFull(shardFulls, marker.ToSegment)
        && ProfileObserved(marker.ToSegment))
    {
        gap = new WalGapState(marker.FromSegment, marker.ToSegment, marker.ToTli);
        await journal.WritePhaseAsync(cluster, Op, $"wal-tli-gap/{shard}", claims.InstanceId,
            $"tli jump {marker.FromTli}->{marker.ToTli}: segments ({marker.FromSegment}, {marker.ToSegment}] " +
            "will not be delivered — the worker re-runs the full backup", ct);
    }
}
```

2. Блок вычисления деградаций (существующий `string? error = null; var state =
   WalStreamStatus.Active; …`) дополни ПЕРВЫМ приоритетом — gap жив.
   Текст error строится ВСЕГДА из полей записи (from_tli = TLI хвостового
   сегмента `gap.From` — первые 8 hex имени, через `TliOfSegment`; имена
   валидированы парсером/писателем) — шаблон един для установки и удержания,
   WriteIfChanged стабилен. Сразу после объявления `error`/`state` вставь:

```csharp
if (gap is not null)
{
    state = WalStreamStatus.Degraded;
    error = $"tli-gap: сегменты ({gap.From}, {gap.To}] не будут доставлены — приёмник пережил " +
            $"58P01 tli-переход {TliOfSegment(gap.From)}→{gap.Tli}; воркер переснимает полный";
}
else if (lag > options.WalLagMaxSegments)
```

— т.е. существующие ветки `if (lag > …)` / `else if (now - lastUploadedUnix >
…)` превращаются в `else if`-цепочку после gap-ветки (их тела не меняются;
последующие проверки `pool_exhausted` остаются как есть).

3. В конструкторе `next = new WalStreamState(...)` замени временный аргумент
   подпункта 7.3 `Gap: wal?.Gap` на `Gap: gap`.

---

## Шаг 9. Ф2 — планировщик: форсированный пересъём по `gap`

**Статус:** ВЫПОЛНЕН — коммит `ad9088aa`.

**Вход:** шаг 8 слит.

**Действие:** подпункты 9.1–9.2; юниты `BackupPlannerTests` coder дописывает:
`IsDue_WalGapUncovered_True` (свежий валидный полный, `walGapUncovered: true`
→ true), `IsDue_WalGapCovered_FreshFull_NotDue` (`walGapUncovered: false` +
свежий полный → false — покрытие отключает сигнал, шторма пересъёмов нет);
существующие кейсы `HasActive`/`BackoffPassed` остаются зелёными — инварианты
одного активного и бэкоффа серии FAILED не сломаны (AC3).

**Выход:** живой непокрытый `gap` форсирует планирование полного; HasActive и
бэкофф работают как всегда.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupPlannerTests"` — зелёные (AC3-часть про планировщик).

**Связь со spec:** §3.3, §4 Ф2, AC3.

### Подпункт 9.1. `BackupPlanner.IsDue`

В файле `src/PgWorker.Backups/Process/BackupPlanner.cs` в методе
`IsDue` (сигнатура `… long? lastRestoreFinishedUnix = null, bool
walChainBroken = false)`) добавь последний параметр `bool walGapUncovered = false`
(doc-comment: «t24 (arch/19 §2): живой непокрытый tli-gap wal-ключа — пересъём
безусловно, симметрично BROKEN; правило покрытия и бэкофф — как у
walChainBroken») и первую строку тела замени на:

```csharp
if (walChainBroken || walGapUncovered)
    return true;
```

### Подпункт 9.2. `BackupProcess` — сигнал от `Gap`

В файле `src/PgWorker.Backups/Process/BackupProcess.cs` после вычисления
`breakCovered` (блок G3 перед `BackupPlanner.IsDue(...)`) добавь:

```csharp
// t24 (arch/19 §2): живой непокрытый tli-gap — форс-пересъём (симметрично
// walChainBroken; покрывший COMPLETED-полный ждёт снятия метки контролем).
var gapBoundary = shardBackups?.Wal is { Gap: { } liveGap } ? liveGap.To : null;
var gapCovered = gapBoundary is { Length: > 0 } gapTo
    && fulls.Any(f => f.State == FullBackupStatus.Completed
        && f.WalStartSegment is { Length: > 0 } walStart
        && string.CompareOrdinal(walStart, gapTo) >= 0);
```

и вызов `BackupPlanner.IsDue(fulls, fullMaxAgeSec, nowUnix, lastRestoreFinished,`
`walChainBroken: brokenBoundary is not null && !breakCovered)` дополни аргументом
`walGapUncovered: gapBoundary is not null && !gapCovered`.

---

## Шаг 10. Ф3 — панель: модель и толерантный парсер `gap`

**Статус:** ВЫПОЛНЕН — коммит `1dcf3e17`.

**Вход:** шаг 7 слит (формат ключа зафиксирован).

**Действие:** подпункты 10.1–10.2; кейсы `BackupsParserTests`
(AdminPanel.UnitTests) coder дописывает: ключ wal с `"gap":{...}` → `Gap`
прочитан; ключ без `gap` → `Gap == null` и НЕТ parseErrors; `"gap":"junk"`
(не объект) и `gap` без обязательных полей → `Gap == null`, parseError НЕ
добавляется.

**Выход:** панель видит зазор; старые/битые ключи читаются без ошибок.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupsParserTests"` — зелёные (обе сборки тестов: AdminPanel и PgWorker).

**Связь со spec:** §3.5, AC5.

### Подпункт 10.1. `WalGapInfo` в `BackupsInfo`

В файле `src/AdminPanel.Core/BackupsInfo.cs` перед record `WalStreamInfo`
добавь:

```csharp
/// <summary>Границы RPO-зазора tli-прыжка приёмника из поля gap wal-ключа
/// (архивный формат arch/19 §4): сегменты (From .. To] невосстановимы из
/// бэкапов.</summary>
public sealed record WalGapInfo(string From, string To, uint Tli);
```

и в `WalStreamInfo` добавь последний опциональный параметр
`WalGapInfo? Gap = null` (doc-comment: «t24: опционально — старые/битые ключи
поля не несут»).

### Подпункт 10.2. Чтение `gap` в `BackupsParser`

В файле `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`, ветка ключа wal
(`segments.Length == 6 && segments[5] == "wal"`): после вычисления
`state`/`unix` и до конструктора `WalStreamInfo` добавь (хелперы
`String`/`Long` класса принимают `JsonElement`):

```csharp
// t24: gap — опциональное поле (толерантно: отсутствует/битое → null,
// parseError не пишется — backwards-compat правила канона arch/19 §4).
WalGapInfo? gap = null;
if (root.TryGetProperty("gap", out var gapEl) && gapEl.ValueKind == JsonValueKind.Object)
{
    var gapFrom = String(gapEl, "from");
    var gapTo = String(gapEl, "to");
    var gapTli = Long(gapEl, "tli");
    if (gapFrom is not null && gapTo is not null && gapTli is not null)
        gap = new WalGapInfo(gapFrom, gapTo, (uint)gapTli.Value);
}
```

и передай `gap` последним аргументом в `new WalStreamInfo(...)`.

---

## Шаг 11. Ф3 — правило алерта `backup-wal-tli-gap`

**Статус:** ВЫПОЛНЕН — коммит `974e6e62`.

**Вход:** шаг 10 слит.

**Действие:** подпункт 11.1; кейсы `BackupWalTliGapRuleTests` coder пишет по
образцу `BackupChainBrokenRuleTests` (TestSnapshots.FullCluster/Healthy):
`Gap_ActiveCluster_Critical` (Gap живой живого Active-кластера → один alert,
critical, kind `backup-wal-tli-gap`, Remedy WorkerAuto, Message содержит
`From`/`To` и «re-runs the full backup»), `NoGap_Silent` (wal DEGRADED без
Gap → молчит — домен существующих правил), `Gap_DeadCluster_Silent`,
`Gap_ClusterWithoutShard_Silent` (шард не в кластере — молчит).

**Выход:** critical-алерт на живой `gap` живого Active-кластера; снимается
снятием поля (заживление полным).

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupWalTliGapRuleTests"` — зелёные.

**Связь со spec:** §3.5, §6 п.11, AC5.

### Подпункт 11.1. Новый файл `BackupWalTliGapRule`

Создай файл `src/AdminPanel.Core/Alerting/Rules/BackupWalTliGapRule.cs` —
структура 1:1 с `BackupChainBrokenRule.cs` (namespace/атрибут
`[InjectAsSingleton(typeof(IAlertRule))]`/метод `Evaluate`), отличия:

```csharp
// backup-wal-tli-gap (critical, arch/19 §3/§4): живое поле gap wal-ключа шарда
// живого Active-кластера — RPO-зазор tli-прыжка приёмника (58P01 пережит,
// сегменты между from и to невосстановимы из бэкапов); воркер сам переснимает
// полный (планировщик arch/19 §2 реагирует на gap), метка снимается
// заживлением. DEGRADED без gap — домен wal-chain-broken/лагов (не трогается).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupWalTliGapRule : IAlertRule
{
    public const string KindName = "backup-wal-tli-gap";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { Gap: { } gap })
                continue;

            // только живой Active-кластер (образец BackupChainBrokenRule)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"WAL tli-gap for shard {shard} of cluster {backups.Cluster}: segments " +
                $"({gap.From} .. {gap.To}] are unrestorable from backups (RPO event, tli ->{gap.Tli}) " +
                $"— the worker re-runs the full backup",
                new Dictionary<string, string>
                {
                    ["state"] = wal.State.ToString().ToUpperInvariant(),
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                    ["gap_from"] = gap.From,
                    ["gap_to"] = gap.To,
                    ["gap_tli"] = $"{gap.Tli}",
                },
                null,
                "data between the gap boundaries cannot be restored from backups (RPO event): " +
                "check the data-loss window against your recovery requirements",
                AlertRemedy.WorkerAuto,
                "the worker re-runs the full backup; the alert clears when the covering full " +
                "backup completes and the gap field is lifted by the worker");
        }
    }
}
```

---

## Шаг 12. Ф4 — E2E `WalStream_TliBackfill_ClosesGap_NoRpoLoss` (t24a, главный путь)

**Статус:** ВЫПОЛНЕН — коммит `8336e0d6` (кейс зелёный; в тот же коммит вошло
сопутствующее уточнение транспорта: Npgsql-обмен ленив — первый шаг чтения
форсируется в `StartReplicationAsync`, 58P01 выходит из вызова старта).
Формулировки ниже нормализованы по обновлённому §3.7: ассерты — финальная
сходимость, БЕЗ маркерных проверок; фактические гоночные ассерты маркеров
(если присутствуют в закоммиченном коде теста) убираются в шаге 13 п.1.

**Вход:** шаги 1–11 слиты.

**Действие** (coder сам — E2E-код на живом контуре): класс
`src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamTliScenarios.cs` —
скелет и хелперы скопировать из `E2eWalStreamPromoteScenarios` (E2eEnvironment
со своей сетью/etcd/ownS3, `StartWalScenarioAsync`, `StartWalHostAsync`,
`SeedClusterAsync`, `MasterPgAsync`, `PrimaryTimelineAsync`, `SwitchWalsAsync`,
`GenerateWalAsync`, `BackgroundWalLoadAsync`, `WalScenarioDiagDumpAsync`,
`[PHASE]`-метки фаз, catch → `Fx.MarkFailed()` + дамп, finally → отмена
нагрузки; per-method окружение; полный teardown; ассерт чистоты — по канону
`docs/e2e-isolation.md`). Тест `WalStream_TliBackfill_ClosesGap_NoRpoLoss`
(slug `wal-t24a`, кластер `shopt24a{ClusterTag}`), фазы:

1. Подъём контура и обоих агентов running (WaitAsync 300 s, образец
   `bothAgents`).
2. `GenerateWalAsync(adminDsn, ct)` — база цепочки TLI 1; дождаться ≥2
   сегментов TLI 1 в листинге S3 (`backupS3.ListWalAsync`, бюджет 300 s).
3. `[PHASE]` пауза S3: `Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct)`.
4. Ещё `GenerateWalAsync` — сегменты закрыты, доставки нет (слот держит);
   выдержка ~10 с (приёмник уходит в put-ретрай — backpressure).
5. `docker stop` текущего мастера (`MasterPgAsync` → имя контейнера
   `pgw-{cluster}-shard1-{master}`), дождаться нового primary
   (`MasterPgAsync`) и `tli >= 2` (`PrimaryTimelineAsync`, бюджет 120 s).
6. `SwitchWalsAsync(newMasterDsn, 2, ct)` — TLI-граница в закрытом сегменте.
7. `[PHASE]` возврат S3: `docker start` ownS3-ноды.
8. Ассерты сходимости (E2ePhase.WaitAsync, бюджет 300 s, прогресс-делегат
   листинга — образец `walListingProgress` из ChainGlued): ключ `wal` ACTIVE,
   `Gap == null`, BROKEN нет, цепочка непрерывна —
   `WalChain.CheckWithRestart(chainStart, names).IsContinuous` от
   `chain_start` ключа по листингу `wal/`, `.history` нового TLI в S3.
   Промежуточные события (маркер `tli_backfill` в логах, факт 58P01) НЕ
   ассертируются; логи агентов снимаются в телеметрию прогона (артефакты) как
   диагностика.

**Выход:** главный путь (AC1) детерминированно сходится и зелёный.

**Проверка:** запуск по канону E2E (команда — в шаге 14):
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStream_TliBackfill_ClosesGap_NoRpoLoss"` — PASS; артефакты прогона без фаз-молчаний >60 с; после — зачистка контейнеров/сетей теста.

**Связь со spec:** §3.7 кейс t24a, AC1, AC7 (телеметрия).

Подпунктов нет (E2E требует знания контура — сильная модель).

---

## Шаг 13. Ф4 — переработка E2E t24b на ассерты финальной сходимости

**Статус:** К ИСПОЛНЕНИЮ (первый из оставшихся).

**Вход:** шаги 1–12 выполнены (ceea95f3…8336e0d6); в ветке WIP-коммит
`668a039f` — E2E `WalStream_TliJump_GapMarked_FullRetaken` со СТАРЫМИ
гоночными ассертами (факт 58P01, stdout-маркеры, метка `gap` в моменте,
тайминги «прыжок → контроль»). Spec обновлён: ассерты — ТОЛЬКО финальная
сходимость (§2, §3.7, §6 п.15) — WIP ПЕРЕРАБАТЫВАЕТСЯ, не удаляется.

**Действие** (coder сам; состав WIP сверен по `git show 668a039f` — изменён
один файл `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamTliScenarios.cs`,
+272 строки; хелперы `ParseWalStart`/`SwitchWithRecordsAsync` — t24b-эксклюзивны,
сохраняются):

1. **Нормализация t24a (шаг 12):** убрать из фактического кода
   `WalStream_TliBackfill_ClosesGap_NoRpoLoss` гоночные ассерты stdout-маркеров
   (`"phase":"tli_backfill"` есть / `"phase":"tli_jump"` нет в логах агентов),
   если они присутствуют в закоммиченном коде: маркеры — телеметрия, не гейт
   (§3.7); снятие логов агентов в артефакты прогона остаётся (диагностика).
   Финальные ассерты t24a (шаг 12 п.8) не меняются.
2. **Переработка t24b** — `WalStream_TliJump_GapMarked_FullRetaken` (slug
   `wal-t24b`, кластер `shopt24b{ClusterTag}`), сохранить из WIP сценарий
   индуцирования:
   - фазы 1–5 как в t24a: подъём контура и обоих агентов → база цепочки TLI 1 →
     `[PHASE]` пауза S3 (`docker stop` ownS3-ноды) → записная генерация
     недоставленного WAL (хелпер WIP `SwitchWithRecordsAsync` — INSERT +
     `pg_switch_wal`; на idle-базе пустое переключение НЕ материализует
     сегмент — нужна запись в каждом) → `docker stop` мастера → promote
     (новый primary, `tli >= 2`).
   - `[PHASE]` срез удержания на новом мастере (§6 п.16): drop слота
     (`PgWorker.Backups.BackupNames.Slot(cluster, "shard1")` — имя длинного
     кластера усекается sha1, использовать метод проекта; SQL
     `SELECT pg_drop_replication_slot(@slot)`) + записные `pg_switch_wal`
     (`SwitchWithRecordsAsync(newMasterDsn, 160, ct)`) суммарно за
     `wal_keep_size`=2048 MB / 128 сегментов (160 — с запасом); ensure воркера
     пересоздаст слот уже на высокой позиции; `[PHASE]`-метки начала/конца
     фазы.
   - `[PHASE]` возврат S3 (`docker start` ownS3-ноды).
   - контрольная INSERT-вставка на новом мастере — маркер «догона» (механика
     единая, inline-команды Npgsql в тесте, БЕЗ `SwitchWithRecordsAsync` —
     хелпер не возвращает LSN вставки): (1) `INSERT INTO wal_load(payload)
     VALUES ('r')`; (2) `controlLsn = SELECT pg_current_wal_lsn()` — позиция
     контрольной записи; (3) `SELECT pg_switch_wal()` — закрыть сегмент
     контрольной записи (он будет доставлен приёмником; LSN снимать ДО
     переключения — после него позиция ушла бы в следующий незакрытый сегмент
     и ассерт «догона» был бы ложно-недостижим).
3. **Удалить из WIP-кода** (гоночная механика старой семантики): снос старых
   агентов шарда перед возвратом S3; ожидания пересоздания агентов воркером
   (docker logs `--since`-механика); ВСЕ промежуточные ассерты и ожидания —
   факт 58P01, наличие/отсутствие stdout-маркеров `tli_jump`/`tli_backfill` в
   логах, метка `gap`-поля в моменте (DEGRADED-этап), тайминги
   «прыжок → контроль».
4. **Ассерты — 5 финальных инвариантов сходимости** (§3.7; итог при любом
   пути — прыжок с зазором + пересъём / досыпка без зазора / зазор не возник;
   общий бюджет ожидания 900 с, `[PHASE]`-отчёты; прогресс-делегат листинга
   S3 + ключ wal + ключи full):
   1. кластер Active (ключ config жив, без поля `state`-артефакта демонтажа;
      шард `dsn` жив — образец `ProvisionedAsync`);
   2. ключ `wal`: state `ACTIVE` (не BROKEN), `Gap == null` (не возник или
      снят покрытием; живой незакрытый `gap` при сошедшемся итоге = несходимость);
   3. цепочка `wal/` в S3 цела/склеена через TLI-границу:
      `WalChain.CheckWithRestart(chainStart, names).IsContinuous` от
      `chain_start` ключа;
   4. каталог бэкапов здоров: существует COMPLETED-полный с
      `started_unix` после момента завершения индукции (фиксация unix-метки
      после `SwitchWithRecordsAsync(160)`) и `verify.state != FAILED`
      (null/PENDING/OK допустимы — «verify ≠ FAILED»); хелпер WIP
      `ParseWalStart` — в прогресс-диагностике полных (не гейт);
   5. доставка догнала: последний сегмент непрерывной цепи (по листингу S3)
      ≥ сегмента контрольной вставки — `control = WalFileName.FromLsn(tli,
      controlLsn)`, где `tli = PrimaryTimelineAsync(...)` (снимается при
      проверке), `controlLsn` — зафиксированный на фазе контрольной вставки
      ДО переключения (см. выше); ассерт
      `string.CompareOrdinal(lastChainSegment.Name, control.Name) >= 0`
      (сравнение имён == сравнение (TLI, log, seg) — покрывает и TLI, и
      позицию).
5. Развернуть `WalScenarioDiagDumpAsync`-дамп в catch (MarkFailed-семантика —
   без изменений, по образцу t24a); несходимость в бюджет — расследование по
   артефактам, перезапуск запрещён до анализа (канон `docs/e2e-launch.md`).
6. Коммит одним: `test(e2e): t24a/t24b — ассерты финальной сходимости
   (маркеры/промежуточные события — телеметрия, не гейт; t24b: индуцированный
   срез, 5 инвариантов)`.

**Выход:** оба кейса детерминированные (сходимость), без гоночных ассертов;
WIP-механика индукции сохранена.

**Проверка:** двумя отдельными командами (канон):
1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` → 0 warnings;
2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalReceiverCoreTests"` → зелёные
   (tli-юниты не задеты);
плюс в фактическом коде t24a/t24b нет ассертов на `tli_jump`/`tli_backfill`/
`58P01`/`Gap != null`-в-моменте (визуальная сверка — grep по коду кейсов);
живой прогон обоих кейсов — шаг 14.

**Связь со spec:** §2 (принцип сходимости), §3.7 (оба кейса, 5 инвариантов),
§6 п.15/п.16, AC1/AC2/AC3 (E2E-часть).

Подпунктов нет (переработка E2E по точному составу WIP — сильная модель).

---

## Шаг 14. Ф4 — зелёная E2E-серия WalStream (t24a + t24b + ChainGlued) на свежем Release

**Статус:** К ИСПОЛНЕНИЮ.

**Вход:** шаг 13 слит (оба кейса переработаны на сходимость).

**Действие** (coder сам; сборка и тесты — ДВЕ ОТДЕЛЬНЫЕ команды, канон
AGENTS.md): статическая подготовка → сборка → прогон:

1. `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
3. `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStream_Promote_TliHistory_ChainGlued"` — PASS (AC6:
   устранение эпизодического фейла механикой шага 5; код теста НЕ меняется).
4. Оба новых кейса:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStream_TliBackfill_ClosesGap_NoRpoLoss|FullyQualifiedName~WalStream_TliJump_GapMarked_FullRetaken"` — PASS
   (AC1 + AC2/AC3 E2E-часть: сходимость; записная генерация 128+ сегментов/2 ГБ
   WAL в t24b — `[PHASE]`-телеметрия фазы, несходимость в бюджет —
   расследование по артефактам, расширение бюджетов не допускается).
5. Серия WalStream-сценариев вместе:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2eWalStream"` — все классы PASS
   (в `E2eWalStreamTliScenarios` оба кейса: t24a + t24b).
6. Зачистка контейнеров/томов/сетей серии (страховочный гейт; E2E-трейты
   чистят сами — проверить остатки `docker ps -a | grep pgw-` и
   `docker network ls | grep -c 'pgw-\|kfw-net'` = 0 при нуле контейнеров).

Вывод каждой команды — целиком в файл `/tmp/t24-e2e-<n>.log` (читать из файла);
все фазы >60 с — с `[PHASE]`-отчётом; упавший сценарий — MarkFailed-семантика:
разбор по артефактам БЕЗ перезапуска (канон `docs/e2e-launch.md`).

**Выход:** AC1/AC6 + E2E-часть AC2/AC3; серия E2E WalStream зелёная на свежем
бинаре.

**Проверка:** финальные строки логов 3–5 — PASS/Passed; п.6 — 0 остатков.

**Связь со spec:** §3.7 (все три кейса), §4 Ф4, AC1/AC2/AC3 (E2E)/AC6.

Подпунктов нет.

---

## Шаг 15. Ф5 — мерж-гейт: полные серии на свежем Release

**Статус:** К ИСПОЛНЕНИЮ.

**Вход:** шаги 1–14 слиты.

**Действие** (coder сам): полный регресс перед roadmap-чисткой (сборка уже
свежая — после шага 14; если код менялся — пересобрать отдельной командой):

1. Сверка юнит-покрытия AC2/AC3 с фактическим набором (перечни шагов 5 и
   8/9): юниты покрывают ветки механики (промежуточные события); недостающие
   кейсы (в частности `TliProcedure_RePromote…` — гонки повторного промоута)
   дописать мелким коммитом ДО серий и прогнать их фильтром класса.
2. Юниты:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~UnitTests"` → лог `/tmp/t24-units.log`.
3. Интеграции + E2E (одна команда, PGW_TEST_DOCKER=1):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"` → лог `/tmp/t24-integration.log`
   (E2E-объём задачи — t24a + t24b + ChainGlued; E2ePrepare перед серией —
   шаг 14; дождаться финальной строки; серии длинные — контроль по правилам
   AGENTS.base п.13: стартовал/не висит, проверка каждые 2 мин по логу).
4. Кейс-маркер:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~Scale_AddEmptyShard"` → PASS.
5. После КАЖДОЙ серии — зачистка контейнеров/томов/сетей (страховочный гейт:
   остатков `pgw-*` нет, `docker network prune -f` при осиротевших
   `kfw-net-*`/`pgw-*-net`).

Критерий: все серии зелёные, 0 warnings (TreatWarningsAsErrors), 0 остатков
контейнеров/сетей/ключей между сериями; телеметрия E2E по канону.

**Выход:** AC7 выполнен.

**Проверка:** финальные строки логов — Passed; `docker ps -a | grep -c pgw-` == 0.

**Связь со spec:** §4 Ф5, AC2/AC3 (юнит-контрольная точка), AC7.

Подпунктов нет.

---

## Шаг 16. Ф5 — roadmap-чистка и коммит мержа

**Статус:** К ИСПОЛНЕНИЮ.

**Вход:** шаг 15 зелёный.

**Действие** (coder сам — документация): тем же коммитом (правила
`arch/roadmap/README.md` — слита → тег удалён сам, без вопросов):

1. `arch/roadmap/backup.md` — удалить пункт `t24-walreceiver-tli-jump`
   (строки пункта целиком); проверить `grep -rn "t24" arch/roadmap/` — из
   `←`-зависимостей других пунктов упоминания удалить.
2. `arch/roadmap/reliability.md` — в пункте `t29-flaky-tests` удалить подпункт
   3 (`WalStream_Promote_TliHistory_ChainGlued` — класс больше не флакает:
   устранён механикой tli-процедуры приёмника, закреплён зелёными E2E t24a /
   t24b / ChainGlued; нумерацию остальных подпунктов не менять).
3. `arch/roadmap/reliability-report.md` — сводка характеристики D: дополнить
   абзац «Ядро добротное» фразой о переживания 58P01 tli-перехода приёмником:
   досыпка старого таймлайна до точки переключения (RPO=0); fallback-прыжок —
   осознанный RPO-зазор с полем `gap` в wal-ключе (грань наблюдаемости: канал
   метки ограничен пересозданием агентов надзором — принятые границы
   arch/19 §3/§3.2), форс-пересъёмом полного и панельным алертом
   `backup-wal-tli-gap`; незакрытый зазор вскрывается drill/restore-валидацией;
   устойчивость подтверждена детерминированными E2E сходимости — t24a (досыпка)
   и t24b (индуцированный срез: drop слота + записные переключения за
   `wal_keep_size`=2048 MB/128 сегментов → финальные инварианты).
4. Коммит в feature-ветке worktree (сообщение:
   `t24: wal receiver survives tli jump on 58P01 (backfill + gap-marked jump fallback)`).

**Выход:** roadmap без t24; история — в git и docs/superpowers/.

**Проверка:** `grep -rn "t24-walreceiver-tli-jump" arch/` — пусто (кроме
`docs/superpowers/`); `git status` — чистый после коммита.

**Связь со spec:** §4 Ф5, §6 п.16 (согласование формулировок индукции),
AC7/AC8 (финальная часть).

Подпунктов нет.

---

## Чек-лист самопроверки исполнителя (перед возвратом)

1. AC1–AC8 закрыты (семантика E2E — сходимость, §6 п.15):
   - AC1 (досыпка: E2E t24a + юниты) — шаги 5, 12 (выполнены) + нормализация
     ассертов шагом 13 п.1;
   - AC2 (прыжок: юниты веток механики + E2E t24b — финальная сходимость
     без промежуточных ассертов) — шаг 5 (выполнен; контрольная точка — шаг 15
     п.1) + шаги 13/14;
   - AC3 (метка и заживление: юниты WalStreamProcess/BackupPlanner + E2E t24b
     инвариант «живого `gap` нет при финальном ACTIVE») — шаги 8, 9
     (выполнены) + шаги 13/14;
   - AC4 (внутри-TLI) — шаг 5 `TliJump_SameTli…` + шаг 8
     `TliJumpSameTli_NoGap` (выполнены);
   - AC5 (панель) — шаги 10, 11 (выполнены);
   - AC6 (ChainGlued) — шаг 14;
   - AC7 (регресс-гейт) — шаг 15;
   - AC8 (канон до кода) — шаг 1 (выполнен, ceea95f3).
2. В t24a/t24b НЕТ ассертов на промежуточные события (58P01, прыжок,
   stdout-маркеры, метка `gap` в моменте, timing) — только финальные
   инварианты сходимости (шаги 12–14); stdout-маркеры — телеметрия артефактов.
3. Инвариант подтверждения-по-доставке не нарушен (досыпка и прыжок идут
   штатным контуром приёма — шаг 5).
4. Каждый старт tli-процедуры — на свежем `OpenAsync` (отказ 58P01 гасит
   соединение; шаг 5 — юнит-ассерты `OpenCalls` + инвариант утечки).
5. Ни в одном оставшемся шаге нет выбора «как сделать» — все имена/форматы/
   тексты заданы; отклонение от плана = остановка и вопрос через main-агент.
6. Все временные ряды: 0 warnings, комментарии AAA в тестах, русский язык
   комментариев, идентификаторы английские.
