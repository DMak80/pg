# t38-owns3-result-migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Унификация внутренних исходов операций ownS3 на Result-парадигму монорепо (`Result`/`Result<T>` из `src/Shared.Core/Result.cs`): подключить `Shared.Core` ко всем четырём проектам `OwnS3.*` и перевести на `Result` все внутренние сигнатуры (транспорт репликации, стор `IObjectStore`/`IXlDrive`/`IReplicaStore`, `ReplicaApplier`, `ReplicationProber`, хендлеры App) — без изменения HTTP-контракта, JSON-моделей и дисковых форматов.

**Architecture:** Чисто внутренняя миграция по принципам spec: arch-first (канон в `arch/owns3/` до кода), затем кодовые срезы «интерфейс → реализации → потребители» в порядке, при котором каждый коммит собирается и зелёный (`TreatWarningsAsErrors=true` не прощает частичных состояний). Success несёт терминальный исход (включая не-ошибочные варианты: Not-Modified, отсутствие записи, tombstone/404, `reachable=false` с ответом), Failed — только технический отказ (транспорт, кворум EC, 429, ошибки каталога S3-операций). Ошибки стора перестают лететь исключениями: маппинг в S3-ошибки выполняет App-транслятор; catch-ветки стора в `S3Middleware` демонтируются.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), `Shared.Core` (`Result`/`Result<T>` — readonly record struct с `Match`/`Bind`/`Map`/`Throw`), xUnit v3 + FluentAssertions, Testcontainers (docker-E2E `PGW_TEST_DOCKER=1`).

**Spec:** [`docs/superpowers/2026-10-13-t38-owns3-result-migration/spec.md`](spec.md) — план аргументируется от spec; исполнители читают оба документа.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/refactor-t38-owns3-result-migration` (ветка `refactor-t38-owns3-result-migration`). Все пути ниже — от корня worktree; все команды — из корня worktree.

## Global Constraints

- **Arch-first (spec §2.1, критерий 1):** Task 0 (правки `arch/`) коммитится РАНЬШЕ любого кодового коммита; arch-текст соответствует реализованной конвенции дословно.
- **Внешние контракты неизменны (spec §2.2, критерий 4):** 21 операция S3-API, replication/admin-грани, JSON-модели ответов (`rttMs` — по-прежнему nullable-поле JSON `ProbeOutcome`), дисковые форматы (`xl.meta`, `.owns3.sys`) — байт-в-байт. Интеграционные сценарии `src/tests/OwnS3.IntegrationTests/Api/` проходят БЕЗ правок HTTP-ожиданий.
- **Семантика бинарного Result (spec §2.3):** Success — терминальный исход, который удалось узнать (включая 304, отсутствие записи, tombstone/404 фетча, `reachable=false` с HTTP-ответом); Failed — только технический отказ (`ObjectStoreException`, `XlInvalidArgumentException`, `ReplicationChecksumMismatchException`, `PeerRejectedException`, `UnknownReplicationTargetException`, обрыв/таймаут/429, кворум EC).
- **Protocol вне области (spec §2.5/§6):** `OwnS3.Protocol` получает `ProjectReference` на `Shared.Core`, но БЕЗ появления Result-сигнатур; `S3ProtocolException` (включая `BadDigest`/`InvalidRequest` из `HashingBodyStream`) остаётся исключением.
- **`OperationCanceledException` по внешнему ct — НЕ исход операции (spec §6):** пробрасывается, как сегодня. ЗАПРЕЩЕНО заворачивать в `Result.From`/`FromAsync` код, где возможна внешняя отмена, без отдельного проброса OCE (см. паттерн Step 5.2).
- **`PutResult`/`CompleteResult`/`DeletedKeyResult` — доменные типы-значения успеха (spec §3):** остаются типами; «унификация» = сигнатуры становятся `Task<Result<PutResult>>` и т.п. `ObjectContent` не меняется по форме (`NotModified`/`Range` — семантика чтения, не конвенция исходов).
- **Счётчики исходов репликации — семантика канона 07 §10 «как сегодня», байт-в-байт (spec §4.4):** `ReplicaApplyOutcome` — четырёхзначный enum `{ Applied, Skipped, ConflictApplied, ConflictSkipped }`: пара `ConflictApplied`/`ConflictSkipped` выражает флаг конфликта канона 07 §10 ПОВЕРХ исхода применения. Инкременты фетчера: `Applied → applied`; `Skipped → skipped`; `ConflictApplied → conflict И applied`; `ConflictSkipped → conflict И skipped`. Inbox-предфильтр (`NotificationInbox.HandleObjectPut`) через enum не проходит и остаётся на conflict + skipped, как сегодня. Изменение semantics метрик НЕ вводится; глава 07 §10 не меняется.
- **Язык:** комментарии и документация — по-русски; идентификаторы — на английском. Тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- **`TreatWarningsAsErrors=true`** — каждый коммит собирается чисто; `Nullable=enable`.
- **Вне задачи (spec §6):** потребители OwnS3 по HTTP (`S3Worker`, `AdminPanel`, бэкапы PgWorker), `OwnS3NodeClient` воркеров, новые внешние пакеты (только `ProjectReference`), поведение операций.
- **Запуск команд:** все dotnet-прогоны — с `DOTNET_CLI_UI_LANGUAGE=en`; docker-E2E — `PGW_TEST_DOCKER=1`, конфигурация Release, E2eFixture собирает образ сам (инкрементально); после КАЖДОЙ тестовой серии — зачистка контейнеров/сетей и контрольные grep-ы (правила AGENTS.md, телеметрия — `docs/e2e-launch.md`).
- **Мерж-гейт:** roadmap-запись `t38-owns3-result-migration` удаляется из `arch/roadmap/owns3.md` **мерж-коммитом** задачи (канон AGENTS.md: тем же коммитом, сам, без вопросов).

## Карта миграции сигнатур (сводно; детали — в задачах)

| Интерфейс/класс | Было | Стало |
|---|---|---|
| `IObjectStore` (21 операция) | void/`Task<T>` + исключения | `Task<Result>`/`Task<Result<T>>` (Task 3) |
| `IXlDrive.TryReadMeta` | `XlMetaRecord?` | `Result<XlMetaLookup>` (Task 2) |
| `IXlDrive.BucketExists` | `bool` + throw Insufficient* (EC) | `Result<bool>` (Task 2: EC без read-кворума во всех сетах → `Failed(InsufficientReadQuorum)` — канон 06 §7) |
| `IXlDrive.EnumerateObjects` | `IXlObjectCursor` + throw Insufficient* (EC-предиктат) | `Result<IXlObjectCursor>` (Task 2: онлайн < k в любом сете → `Failed(InsufficientReadQuorum)`; курсорные XlIntegrityException — вне перечня, остаются исключениями курсора) |
| `IXlDrive` void/mутации | void + исключения | `Result`/`Task<Result>` (Task 2) |
| `RangeResolver.Resolve` | `AppliedByteRange?` + throw InvalidRange | `Result<AppliedByteRange?>` (Task 3) |
| `ListWalker.Walk` | `ListPage` + throw InvalidArgument | `Result<ListPage>` (Task 3) |
| `XlPathEncoder.EncodeSegments` | throw XlInvalidArgumentException | остаётся исключением (чистая функция кодирования; конвертация в `Failed` на границе XlObjectStore/ReplicaStore — Tasks 3–4) |
| `IReplicaStore` | исключения + `out nextMarker` | `Result`/`Result<T>`, маркер в значении (Task 4) |
| `IReplicationTransport.SendNotificationsAsync` | `Task<long?>` | `Task<Result<long>>` (Task 5) |
| `IReplicationTransport.FetchObjectAsync` | `Task<FetchResult>` (DTO) | `Task<Result<FetchOutcome>>` (вариантная запись; Task 5) |
| `IReplicationTransport.PingAsync` | `Task<PingResult>` (`RttMs?`) | `Task<Result<PingResult>>`, `RttMs` не-null (Task 5) |
| `ReplicaApplier.ApplyObjectAsync` | `Task<ReplicaApplyResult>` (два bool) | `Task<Result<ReplicaApplyOutcome>>` — enum из 4 значений `Applied/Skipped/ConflictApplied/ConflictSkipped` (Task 6) |
| `ReplicationProber.ProbeAsync` | `Task<ProbeReport?>` | `Task<Result<ProbeReport>>` (Task 6) |
| `S3Middleware` | catch `ObjectStoreException`/`XlInvalidArgumentException`/`ObjectStoreUnavailableException` | ветки демонтированы; маппинг — `S3ResultTranslator` (Task 7) |
| Демонтируемые DTO | `FetchResult`, `ObjectAttributesResult`, `ReplicaApplyResult`, `ObjectStoreUnavailableException` | — (Tasks 3, 5, 6, 7) |

---

## Task 0: Arch-first — конвенция исходов в `arch/owns3/` (фаза 0 spec)

**Files:**
- Modify: `arch/owns3/01-overview.md` (§3 — границы зависимостей; §4 — таблица ключевых решений)
- Modify: `arch/owns3/06-erasure.md` (§8 — конец раздела, после абзаца «Бакеты»)
- Modify: `arch/owns3/07-replication.md` (§11 — абзац «Исполнение в App»)
- Modify (только при фактическом месте): `arch/owns3/05-access-config.md`

**Interfaces:**
- Consumes: ничего (канон описывается до кода).
- Produces: каноническое описание Result-конвенции — код Tasks 1–8 зеркалит его дословно.

- [ ] **Step 0.1: `01-overview.md` §3 — границы зависимостей**

В `arch/owns3/01-overview.md` §3 заменить последний абзац раздела (сейчас: «Границы зависимостей: `Protocol` и `Storage` не зависят от `App` и друг от друга; `Replication` — от `Storage` (применение/чтение) и `Protocol` (подпись исходящих); `App` — от всех трёх; общие библиотеки `Shared.*` — по мере надобности.») на:

```markdown
Границы зависимостей: `Protocol` и `Storage` не зависят от `App` и друг
от друга; `Replication` — от `Storage` (применение/чтение) и `Protocol`
(подпись исходящих); `App` — от всех трёх. Все четыре проекта ссылаются
на `Shared.Core` — конвенция исходов внутренних операций монорепо
(§4); для `Protocol` ссылка служебная: Result-сигнатур протокол не
получает, его исключения (`S3ProtocolException`) — область протокола.
```

- [ ] **Step 0.2: `01-overview.md` §4 — ключевое решение**

В таблицу «Ключевые решения» §4 добавить строку (последней строкой таблицы, после строки «два режима хранения…»):

```markdown
| конвенция исходов внутренних операций — `Result`/`Result<T>` (`Shared.Core`) | Success несёт терминальный исход, включая не-ошибочные терминальные варианты (Not-Modified, отсутствие записи, tombstone/404 соседа у фетча, `reachable=false` ping-пробы с полученным ответом); Failed — только технический отказ (транспорт не ответил, кворумная недоступность EC, 429 «не приняли», ошибки каталога S3-операций, checksum-mismatch фетча). Ошибки стора возвращаются в `Result.Error` (не летят исключениями через границу интерфейса); маппинг в S3-ошибки каталога выполняет App при трансляции исхода в HTTP-ответ. Внешние HTTP-грани и дисковые форматы от конвенции не зависят | единая конвенция монорепо (прецедент воркеров) |
```

- [ ] **Step 0.3: `06-erasure.md` §8 — строка о доставке исходов**

В `arch/owns3/06-erasure.md` §8 после абзаца «**Бакеты**: Create/Delete — …» (конец раздела §8) добавить абзац:

```markdown
**Доставка исходов**: кворумные отказы записи/чтения (503
`Insufficient*`) и исходы операций стора доставляются вызывающему слою
по Result-конвенции главы 01 §4 — без летящих исключений через границу
`IXlDrive`: отказ (включая кворумный) — `Result.Failed` с доменным
кодом, терминальное знание (в т.ч. «записи нет») — `Result.Success`.
```

- [ ] **Step 0.4: `07-replication.md` §11 — строка в «Исполнение в App»**

В `arch/owns3/07-replication.md` §11 в абзац «**Исполнение в App** (глава 01 §3 — App стыкует): …» добавить последним предложением (после «Путь записи клиента не содержит синхронных сетевых вызовов (принцип 1).»):

```markdown
Внутренние исходы транспорта/notifier/фетчера/prober — Result-конвенция
главы 01 §4: 429/обрыв — отказ (бэкофф), ACK/404/tombstone — терминальные
значения фетчера и notifier'а.
```

- [ ] **Step 0.5: `05-access-config.md` — только фактическое место (решение user-review)**

Поискать фактическое место, описывающее исходы ВНУТРЕННИХ интерфейсов (не HTTP-грани):

Run: `grep -n "исход\|исключени\|Exception\|Result" arch/owns3/05-access-config.md`
Решение: правка ТОЛЬКО если найдено место, описывающее исходы внутренних интерфейсов стора/транспорта; в найденное место добавить ссылку на конвенцию главы 01 §4 той же формулировкой, что и §4.7 spec. Правки «под набор» (описание HTTP-граней, auth, env) НЕ делаются. Если места нет — шаг пропускается без правки.

- [ ] **Step 0.6: Проверка правок**

Run: `git diff --stat arch/`
Expected: 3–4 изменённых файла: `arch/owns3/01-overview.md`, `06-erasure.md`, `07-replication.md` (+ `05-access-config.md` только при фактическом месте Step 0.5).

Run: `grep -c "Result-конвенция главы 01 §4" arch/owns3/06-erasure.md arch/owns3/07-replication.md`
Expected: `06-erasure.md:1` и `07-replication.md:1`.

- [ ] **Step 0.7: Commit (arch-first — до любого кода)**

```bash
git add arch/owns3/
git commit -m "t38: arch-first — конвенция исходов внутренних операций ownS3 на Result/Result<T> из Shared.Core: owns3/01 §3 (все четыре проекта ссылаются на Shared.Core; Protocol — служебная ссылка без Result-сигнатур) + §4 (ключевое решение: Success — терминальный исход, включая не-ошибочные варианты; Failed — только технический отказ; ошибки стора в Result.Error, маппинг в S3-ошибки — App); owns3/06 §8 (кворумные отказы и исходы стора — по Result-конвенции, без летящих исключений через IXlDrive); owns3/07 §11 (исходы транспорта/notifier/фетчера/prober — Result-конвенция: 429/обрыв — отказ, ACK/404/tombstone — терминальные значения)"
```

---

## Task 1: Подключение Shared.Core + доменные типы-исходы (фазы 1.1–1.2 spec)

**Files:**
- Modify: `src/OwnS3.Protocol/OwnS3.Protocol.csproj`, `src/OwnS3.Storage/OwnS3.Storage.csproj`, `src/OwnS3.Replication/OwnS3.Replication.csproj`, `src/OwnS3.App/OwnS3.App.csproj`
- Create: `src/OwnS3.Storage/Domain/Outcomes.cs`
- Create: `src/OwnS3.Replication/Transport/FetchOutcome.cs`
- Create: `src/OwnS3.Replication/Transport/ReplicationErrors.cs`
- Create: `src/OwnS3.Replication/Lww/ReplicaApplyOutcome.cs`

**Interfaces:**
- Consumes: `Result`/`Result<T>` из `src/Shared.Core/Result.cs` (уже существует); доменные типы `XlMetaRecord`, `ReplicaKeyState`, `ReplicationListEntry`, `ObjectMetadata`, `ObjectAttributes` (все уже существуют).
- Produces (для Tasks 2–6, exact-имена):
  - `OwnS3.Storage`: `XlMetaLookup` (`Found(XlMetaRecord Record)` | `Missing()`), `ReplicaKeyLookup` (`Found(ReplicaKeyState State)` | `Missing()`), `GetAttributesOutcome` (`NotModified(ObjectMetadata Metadata)` | `Full(ObjectAttributes Attributes)`), `ReplicaListingPage(IReadOnlyList<ReplicationListEntry> Entries, string? NextMarker)`.
  - `OwnS3.Replication.Transport`: `FetchOutcome` (`Fetched(...)` | `Tombstone(long DeleteTimeUnixMs)` | `KeyNotFound()`), `PeerRejectedException`, `UnknownReplicationTargetException`.
  - `OwnS3.Replication.Lww`: `enum ReplicaApplyOutcome { Applied, Skipped, ConflictApplied, ConflictSkipped }`.

- [ ] **Step 1.1: ProjectReference на Shared.Core во все четыре csproj**

`ProjectReference`-группа уже есть в `OwnS3.Replication.csproj` (Protocol + Storage) и `OwnS3.App.csproj` — туда добавляется СТРОКА в существующую группу (в Replication — после `..\OwnS3.Storage\OwnS3.Storage.csproj`; в App — после `..\OwnS3.Replication\OwnS3.Replication.csproj`). Новая `<ItemGroup>` создаётся ТОЛЬКО в `OwnS3.Protocol.csproj` и `OwnS3.Storage.csproj` (группы ProjectReference там нет — только `PackageReference`/`InternalsVisibleTo`):

```xml
    <ItemGroup>
        <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
    </ItemGroup>
```

Дублирующих `ProjectReference`-групп не создавать (дубль ItemGroup — брак сборки). Границы главы 01 §3 сохраняются: Protocol/Storage не зависят друг от друга и от App.

- [ ] **Step 1.2: Доменные типы-исходы Storage — файл `src/OwnS3.Storage/Domain/Outcomes.cs`**

```csharp
namespace OwnS3.Storage;

// Терминальные варианты одного исхода (конвенция главы 01 §4): Success несёт
// терминальное знание, включая не-ошибочные варианты; Failed — только
// технический отказ. Стиль вариантных записей — по прецеденту UploadVisibility.

/// <summary>Исход поиска xl-меты: запись есть | записи нет (обе — терминальный
/// Success; nullable-маркер устранён).</summary>
public abstract record XlMetaLookup
{
    public sealed record Found(XlMetaRecord Record) : XlMetaLookup;

    public sealed record Missing() : XlMetaLookup;
}

/// <summary>Состояние ключа репликации для LWW (глава 07 §7): запись есть |
/// записи нет (обе — терминальный Success).</summary>
public abstract record ReplicaKeyLookup
{
    public sealed record Found(ReplicaKeyState State) : ReplicaKeyLookup;

    public sealed record Missing() : ReplicaKeyLookup;
}

/// <summary>Исход GetObjectAttributes (глава 02): полные атрибуты |
/// Not-Modified (Metadata несёт ETag/LastModified для заголовков 304).
/// DTO ObjectAttributesResult демонтируется.</summary>
public abstract record GetAttributesOutcome
{
    public sealed record NotModified(ObjectMetadata Metadata) : GetAttributesOutcome;

    public sealed record Full(ObjectAttributes Attributes) : GetAttributesOutcome;
}

/// <summary>Страница листинга replication-грани (глава 07 §6): маркер
/// NextMarker уходит из out-параметра в состав значения (null — конец).</summary>
public sealed record ReplicaListingPage(IReadOnlyList<ReplicationListEntry> Entries,
    string? NextMarker);
```

- [ ] **Step 1.3: Типы транспорта — файл `src/OwnS3.Replication/Transport/FetchOutcome.cs`**

```csharp
namespace OwnS3.Replication.Transport;

// Исход фетча реплики (глава 07 §6) — терминальные варианты одного исхода
// (конвенция главы 01 §4): объект | tombstone | ключ неизвестен; транспортный
// отказ — Result.Failed (не вариант значения). DTO FetchResult демонтируется.
public abstract record FetchOutcome
{
    /// <summary>200-объект: тело + заголовки x-owns3-repl-*/x-amz-meta-*.</summary>
    public sealed record Fetched(Stream Body, long ContentLength, long ModTimeUnixMs,
        string ContentSha256, string ETag, string ContentType,
        IReadOnlyDictionary<string, string> UserMetadata) : FetchOutcome;

    /// <summary>200-tombstone: ключ удалён у источника, тела нет.</summary>
    public sealed record Tombstone(long DeleteTimeUnixMs) : FetchOutcome;

    /// <summary>404: ключ неизвестен — состояние приёмника не меняется.</summary>
    public sealed record KeyNotFound() : FetchOutcome;
}
```

- [ ] **Step 1.4: Типизированные ошибки транспорта — файл `src/OwnS3.Replication/Transport/ReplicationErrors.cs`**

```csharp
namespace OwnS3.Replication.Transport;

// Новые типизированные ошибки транспорта (конвенция главы 01 §4): кладутся в
// Result.Error, не летят исключениями через границу IReplicationTransport.

/// <summary>Сосед не принял рассылку: 429 (приёмный буфер переполнен),
/// не-200 ответ или обрыв транспорта — вызывающий бэкоффит (глава 07 §8).</summary>
public sealed class PeerRejectedException(string message) : Exception(message);

/// <summary>Probe: имя цели не входит в состав targets снапшота — маркер 400
/// для хендлера (глава 07 §2).</summary>
public sealed class UnknownReplicationTargetException(string message) : Exception(message);
```

- [ ] **Step 1.5: Enum исхода применения — файл `src/OwnS3.Replication/Lww/ReplicaApplyOutcome.cs`**

```csharp
namespace OwnS3.Replication.Lww;

// Исход LWW-применения реплики (глава 07 §7/§10) — терминальные Success-варианты;
// двухбулевый ReplicaApplyResult(Applied, Conflict) демонтируется. Четыре значения
// кодируют ровно то, что сегодня несут два bool (Applied, Conflict): пара
// ConflictApplied/ConflictSkipped выражает флаг конфликта канона 07 §10 ПОВЕРХ
// исхода применения — у фетчика конфликт сосуществует с applied/skipped,
// инкременты байт-в-байт как сегодня (см. Task 6).
public enum ReplicaApplyOutcome
{
    /// <summary>IncomingWins без конфликта — реплика применена.</summary>
    Applied,

    /// <summary>Identical/LocalWins без конфликта — пропуск без применения.</summary>
    Skipped,

    /// <summary>Равные modTime, разный sha — тай-брейк по sha, входящая победила
    /// и применена: фетчер инкрементирует conflict И applied (канон 07 §10).</summary>
    ConflictApplied,

    /// <summary>Равные modTime, разный sha — тай-брейк по sha в пользу локальной,
    /// входящая не применена: фетчер инкрементирует conflict И skipped.</summary>
    ConflictSkipped,
}
```

- [ ] **Step 1.6: Сборка (все четыре проекта + тесты компилируются, поведение не изменилось)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug`
Expected: BUILD SUCCEEDED, 0 warnings (TreatWarningsAsErrors). Новые типы нигде ещё не потребляются — только добавления.

- [ ] **Step 1.7: Commit**

```bash
git add src/OwnS3.Protocol/OwnS3.Protocol.csproj src/OwnS3.Storage/OwnS3.Storage.csproj src/OwnS3.Replication/OwnS3.Replication.csproj src/OwnS3.App/OwnS3.App.csproj src/OwnS3.Storage/Domain/Outcomes.cs src/OwnS3.Replication/Transport/FetchOutcome.cs src/OwnS3.Replication/Transport/ReplicationErrors.cs src/OwnS3.Replication/Lww/ReplicaApplyOutcome.cs
git commit -m "t38: подключить Shared.Core ко всем четырём проектам OwnS3.* (ProjectReference, Protocol — служебная ссылка без Result-сигнатур) + доменные типы-исходы конвенции: XlMetaLookup/ReplicaKeyLookup (Found|Missing), GetAttributesOutcome (NotModified|Full), ReplicaListingPage (маркер в значении), FetchOutcome (Fetched|Tombstone|KeyNotFound), PeerRejectedException/UnknownReplicationTargetException, ReplicaApplyOutcome (4 значения Applied/Skipped/ConflictApplied/ConflictSkipped — пара Conflict* выражает флаг конфликта канона 07 §10 поверх исхода применения; счётчики байт-в-байт как сегодня)"
```

---

## Task 2: `IXlDrive` → Result (драйвы размещения; фаза 1.3 spec)

**Files:**
- Modify: `src/OwnS3.Storage/Drives/IXlDrive.cs`
- Modify: `src/OwnS3.Storage/Drives/SingleXlDrive.cs`
- Modify: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs`
- Modify: `src/OwnS3.Storage/XlObjectStore.cs`, `XlObjectStore.Buckets.cs`, `XlObjectStore.Objects.cs`, `XlObjectStore.Copy.cs`, `XlObjectStore.Multipart.cs` (временные мосты — см. Step 2.4)
- Modify: `src/OwnS3.Storage/ReplicaStore.cs` (временные мосты)
- Modify: `src/OwnS3.Storage/Erasure/ErasureHeal.cs` (адаптация к новым сигнатурам — потребляет `ErasureXlDrive` напрямую)
- Modify: `src/OwnS3.App/Pipeline/VolumeCleanupService.cs` (`RunCleanupAsync` → `Task<Result>`)
- Modify: `src/OwnS3.App/Program.cs` (fail-fast `Initialize()` → `.Throw()` — см. Step 2.6)
- Test: `src/tests/OwnS3.UnitTests/Storage/ErasureXlDriveBucketsListTests.cs`, `ErasureXlDriveMultipartTests.cs`, `ErasureXlDriveReadTests.cs`, `ErasureXlDriveWriteTests.cs`, `ErasureHealTests.cs`, `ErasureCleanupTests.cs`, `ReplicaStoreTests.cs`, `src/tests/OwnS3.UnitTests/Replication/ReplicaApplierTests.cs` — полный пофайловый перевод компиляционных мест Step 2.7 (аудит: перечислены ВСЕ файлы юнитов с прямыми вызовами переводимых IXlDrive-членов; `VolumeCleanupServiceTests`/`XlVolumeTests`/`MultipartJournalsTests`/`MultipartCleanupTests` прямых вызовов мигрируемых сигнатур НЕ содержат — XlVolume/MultipartJournals-статика вне миграции), новые кейсы Missing/кворум

**Interfaces:**
- Consumes: `XlMetaLookup` (Task 1).
- Produces (для Tasks 3–4): новые сигнатуры `IXlDrive` (таблица Step 2.1); `Result<PutResult> Commit(...)`, `Result<CompleteResult> CommitMultipart(...)`.

- [ ] **Step 2.1: Новые сигнатуры `IXlDrive`**

Перевести интерфейс `src/OwnS3.Storage/Drives/IXlDrive.cs` к сигнатурам (изменившиеся — помечены `// ←`):

```csharp
public interface IXlDrive
{
    // ── жизненный цикл тома ──
    Result Initialize();                                                    // ←
    Task<Result> RunCleanupAsync(CancellationToken ct);                     // ←
    bool CheckHealth();                                                     // touch-проба: без доменных бросков (сверено)

    // ── бакеты ──
    Result<bool> BucketExists(string bucket);                               // ← (EC: НЕТ сетов с read-кворумом →
                                                                            // Failed(InsufficientReadQuorum) — «503, не „бакета нет“»,
                                                                            // канон 06 §7; финализация по факту кода — spec §4.2)
    Result CreateBucket(string bucket, DateTimeOffset? creationDate = null); // ←
    Result DeleteBucket(string bucket);                                     // ← (BucketNotEmpty → Failed(ObjectStoreException))
    Result<IReadOnlyList<BucketEntry>> ListBuckets();                       // ← (EC-консенсус: кворумный отказ → Failed)

    // ── объекты ──
    Result<XlMetaLookup> TryReadMeta(string bucket, string key);            // ← (Found | Missing)
    IXlStaging CreateStaging(string bucket, string key);                    // без изменений: каталог tmp без IO-семантики исхода;
                                                                            // XlInvalidArgumentException из XlPathEncoder («Key too long»)
                                                                            // конвертируется в Failed НА ГРАНИЦЕ XlObjectStore/ReplicaStore
                                                                            // (Step 3.2/4.2) — чистая функция кодирования остаётся исключением
    Task<Result<PartDigest>> WritePartAsync(IXlStaging staging, int partNumber,
        Stream body, long contentLength, CancellationToken ct);             // ←
    Result CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey,
        XlMetaRecord srcMeta);                                              // ←
    Result WriteMeta(IXlStaging staging, XlMetaRecord record);              // ←
    Result<PutResult> Commit(string bucket, string key, XlMetaRecord record,
        IXlStaging staging);                                                // ←
    Result<Stream> OpenData(string bucket, string key, XlMetaRecord meta,
        long offset, long length);                                          // ←
    Result VerifyFullChecksum(string bucket, string key, XlMetaRecord meta); // ←
    Result Delete(string bucket, string key, XlMetaRecord meta);            // ←
    Result<IReadOnlyList<(int Number, long Size)>> EnumerateParts(string bucket,
        string key, XlMetaRecord meta);                                     // ←

    // ── листинги ──
    Result<IXlObjectCursor> EnumerateObjects(string bucket);                // ← (EC-предиктат: онлайн < k хотя бы
                                                                            // в одном сете → Failed(InsufficientReadQuorum) — «неполный
                                                                            // листинг опаснее недоступности», канон 06 §7; курсорные
                                                                            // XlIntegrityException битой структуры — вне перечня миграции,
                                                                            // остаются исключениями КУРСОРА (ReadCurrentMeta), не драйва)

    // ── multipart-журналы ──
    Result<List<MultipartJournals.UploadJournalEntry>> ReadUploads(string bucket, string key); // ←
    Result WriteUploads(string bucket, string key,
        List<MultipartJournals.UploadJournalEntry> uploads);                // ←
    Result<IReadOnlyList<string>> ListUploadKeyDirs();                      // ←
    Result<List<MultipartJournals.UploadJournalEntry>> ReadUploadsByKeyDir(string keyDirPath); // ←
    bool UploadDirExists(string bucket, string key, string uploadId);       // «проверили и знаем»: доменных бросков нет (Directory.Exists — сверено)
    Result CreateUploadDir(string bucket, string key, string uploadId,
        XlMetaRecord uploadRecord);                                         // ←
    Result<XlMetaRecord> ReadUploadMeta(string bucket, string key, string uploadId); // ← (нет каталога → Failed(ObjectStoreException(NoSuchUpload)))
    Result TrashUpload(string bucket, string key, string uploadId);         // ←
    Result<List<MultipartJournals.PartJournalEntry>> ReadParts(string bucket,
        string key, string uploadId);                                       // ←
    IXlStaging CreateUploadStaging(string bucket, string key, string uploadId); // без изменений (конвертация на границе — как CreateStaging)
    Result CommitUploadPart(IXlStaging staging, int partNumber, string etagHex,
        long size, DateTimeOffset modTime);                                 // ←
    Result<CompleteResult> CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord);          // ←
}
```

Обновить doc-комментарии интерфейса (semantics Missing/Failed — по конвенции главы 01 §4).

**Аудит не-Result членов IXlDrive на доменные `ObjectStoreException`-броски** (каждый бросок — либо Result, либо явная оговорка): `EnumerateObjects` — переводится (выше); `CreateStaging`/`CreateUploadStaging` — единственный бросок `XlInvalidArgumentException` из `XlPathEncoder` («Key too long») — конвертация в `Failed` НА ГРАНИЦЕ слоя операции (Steps 3.2.6, 4.1), через границу интерфейса доменный код не летит; `IXlObjectCursor.ReadCurrentMeta` — `XlIntegrityException` битой структуры: вне перечня миграции (spec §1 — курсор не входит в список интерфейсов), остаётся исключением курсора; `CheckHealth`/`UploadDirExists` — доменных бросков нет (сверено). Других доменных бросков в не-Result членах НЕТ — контрольные grep Steps 2.3/3.2/8.4 сходятся к нулю.

- [ ] **Step 2.2: Правила перевода реализаций (`SingleXlDrive`, `ErasureXlDrive`)**

Механические правила для обоих драйвов:

1. Тело метода, которое сегодня `throw new ObjectStoreException(code, msg)` / `throw new XlInvalidArgumentException(msg)` → `return Result.Failed(new ObjectStoreException(code, msg))` / `Result.Failed(new XlInvalidArgumentException(msg))` (тип-носитель кода не меняется — он кладётся в `Error`, перестаёт лететь). Исключение — вызовы `XlPathEncoder` внутри Result-методов: они гасятся обёрткой `Result.From`/`FromAsync` (пп. 3, 6) в `Failed`; внутри НЕ-Result-методов (`CreateStaging`/`CreateUploadStaging`) — остаются летящими до границы слоя операции (Step 3.2/4.2).
2. `XlMetaRecord? TryReadMeta` → `Result<XlMetaLookup>`: `null` → `new XlMetaLookup.Missing()`; запись → `new XlMetaLookup.Found(meta)`. Кворумная недоступность EC (сегодня `throw ObjectStoreException(Insufficient*)`) → `Result.Failed(...)`. `bool BucketExists` → `Result<bool>`: Single — `Result<bool>.Success(Directory.Exists(...))`; EC — кворумная ветка (сегодня `throw InsufficientReadQuorum` в ErasureXlDrive.BucketExists) → `Result<bool>.Failed(new ObjectStoreException(InsufficientReadQuorum))` (комментарий «503, не „бакета нет“» сохраняется). `IXlObjectCursor EnumerateObjects` → `Result<IXlObjectCursor>`: EC-предиктат (сегодня `throw InsufficientReadQuorum` в ErasureXlDrive.EnumerateObjects — онлайн < k в любом сете) → `Result<IXlObjectCursor>.Failed(...)`; Single — `Success(new SingleObjectCursor(...))` без изменений. Оставшиеся bool-методы (`CheckHealth`, `UploadDirExists`) доменных исключений не бросают (сверено) — остаются bool.
3. Методы с телом-возвратом (`Commit` → `PutResult`, `CommitMultipart` → `CompleteResult`, читающие журналы): успешный путь → `Result<T>.Success(value)`; существующие в них `throw` → `Failed`. Внутренние `catch (IOException …)`/EC-кворум-ветки, которые сегодня перебрасывают/транслируют, — возвращают `Result.Failed` с тем же типом исключения в `Error` (`Result.From`/`FromAsync` уместны, когда в методе невозможна отмена — см. п. 5).
4. `OpenData`: конкурентное удаление (сегодня `FileNotFoundException` из файловой системы) и IO-отказы → `Result<Stream>.Failed(ex)`; успех → `Result<Stream>.Success(stream)`.
5. НЕ превращать в `Failed` отмену по внешнему ct: `OperationCanceledException` в async-методах (`WritePartAsync`) пробрасывается как сегодня (catch-фильтры `when (ct.IsCancellationRequested)` — rethrow); при использовании `Result.FromAsync` — явный `catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }` ДО catch-all.
6. Броски `XlPathEncoder` («Key too long after encoding») внутри переведённых Result-методов драйва гасятся обёрткой в `Failed` (пп. 1, 3) — HTTP-исход не меняется: транслятор Task 7 маппит `XlInvalidArgumentException` → 400 InvalidArgument, как сегодня catch-ветка middleware.

- [ ] **Step 2.3: Перевод `SingleXlDrive.cs` и `ErasureXlDrive.cs` по правилам Step 2.2**

Оба файла переводятся полностью за один шаг (ErasureXlDrive — все `throw new ObjectStoreException(Insufficient*)`/`BucketNotEmpty`/`NoSuchUpload`/`InvalidPart` → `Failed`; SingleXlDrive — `BucketNotEmpty`, `NoSuchUpload`, `InvalidPart`). Комментарии при бросаниях сохраняются, переезжая к возврату `Failed`.

Run (контроль полноты — броски остались ТОЛЬКО внутри не-Result staging-методов через XlPathEncoder, которых в драйвах нет: CreateStaging сам не бросает явно): `grep -c "throw new ObjectStoreException\|throw new XlInvalidArgumentException" src/OwnS3.Storage/Drives/SingleXlDrive.cs src/OwnS3.Storage/Drives/ErasureXlDrive.cs`
Expected: `0` и `0`.

- [ ] **Step 2.4: Временные мосты в потребителях-слоях выше (`XlObjectStore.*`, `ReplicaStore`) — публичные контракты `IObjectStore`/`IReplicaStore` пока НЕ меняются**

Цель — компилируемость и неизменное поведение до Tasks 3–4 (исключения сейчас ловит `S3Middleware` — сохраняем). Идиома моста — готовый `Throw()` у `Result`:

- мутации драйва: `Drive.CreateBucket(bucket)` → `Drive.CreateBucket(bucket).Throw();` (бросит `Error` — прежнее поведение);
- `BucketExists`: `bool exists = Drive.BucketExists(bucket).Throw().Value;` — кворумный отказ бросается, как сегодня;
- `TryReadMeta`: мост через общий приватный хелпер в каждом классе-потребителе (единый вид):

```csharp
    // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
    private static XlMetaRecord? UnwrapMeta(Result<XlMetaLookup> lookup)
        => lookup.IsSuccess
            ? lookup.Value is XlMetaLookup.Found f ? f.Record : null
            : throw lookup.Error!;
```

- значение-возвраты — идиома `.Throw().Value`: `Drive.Commit(...)` → `Drive.Commit(...).Throw().Value` (возвращает `PutResult`); `Result<T>.Throw()` возвращает `Result<T>`, а НЕ `T` — `var x = result.Throw();` без `.Value` молча даёт `x` тип `Result<T>` (для record struct срабатывает Deconstruct при кортеж-распаковке!) — всегда `.Throw().Value` для значений.

Каждый мост помечать строкой-комментарием `// мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore`.

- [ ] **Step 2.5: `ErasureHeal.cs` — адаптация к новым сигнатурам (потребляет `ErasureXlDrive` напрямую)**

`src/OwnS3.Storage/Erasure/ErasureHeal.cs` (точные места: `ListBuckets` вызывается в `RunPassAsync` БЕЗ локального catch — отказ логируют вызывающие: `RequestPass` (Task.Run-catch) и hosted-сервис `ErasureHealHostedService.RunPassSafeAsync`; пер-объектное гашение — существующий catch в `HealBucketAsync`):
- `RunPassAsync`: `var buckets = drive.ListBuckets().Throw().Value;` (финальная внутренняя идиома heal — не мост: кворумный отказ пробрасывается вызывающим → LogError, как сегодня);
- `HealBucketAsync`: `drive.EnumerateObjects(bucket)` теперь `Result<IXlObjectCursor>` — существующий `catch (ObjectStoreException)` (строки ~79–84) заменяется на `IsFailed`-ветку с тем же исходом (пропуск бакета проходом — семантика как сегодня);
- `HealObject` (внутри обхода): `drive.TryReadMeta(bucket, key)` → pattern-match `Found → реконструкция/сверка`, `Missing → не в консенсусе — не трогать, пропуск объекта` (как сегодня `meta?.Ec is null → return` — реконструкция при Missing нереализуема: нет эталона `meta.Ec`), `Failed → пропуск объекта heal-прохода с логом` (сегодня — исключение наверх; heal-проход гасит пер-объектные отказы — прецедент существующих catch в `HealBucketAsync`/`HealObject`).

Логика heal не меняется — только форма потребления исходов.

- [ ] **Step 2.6: `VolumeCleanupService.cs` — `RunCleanupAsync` вернул `Task<Result>`; `Program.cs` — сохранение fail-fast `Initialize()`**

1. В `src/OwnS3.App/Pipeline/VolumeCleanupService.cs`, метод `RunPassAsync`: заменить блок `try { await drive.RunCleanupAsync(ct); UpdateDiskGauges(); } catch (Exception ex) { logger.LogError(...); return; }` на потребление Result:

```csharp
        var cleanup = await drive.RunCleanupAsync(ct);
        if (cleanup.IsFailed)
        {
            logger.LogError(cleanup.Error, "Проход чистки данных упал");
            return;
        }
        UpdateDiskGauges();
```

2. В `src/OwnS3.App/Program.cs` (~строка 405, стартовый fail-fast блок «невалидный конфиг/том = диагностика + ненулевой exit»): statement-вызов `drive.Initialize();` после перевода метода на `Result` перестаёт бросать — catch не срабатывает, `Environment.Exit(1)` не выполняется, приложение стартует с невалидным томом (тихое изменение поведения). Минимальная правка — мост ВНУТРИ существующего try (бросок `Error` ловится тем же catch — лог/exit байт-в-байт как сегодня):

```csharp
    drive = app.Services.GetRequiredService<IXlDrive>();
    drive.Initialize().Throw(); // мост t38 (постоянный): fail-fast по отказу тома — как сегодня
```

- [ ] **Step 2.7: Тесты драйвов — перевод ассертов + компиляционные места + новые терминальные кейсы (полный пофайловый аудит)**

Аудит проведён по ВСЕМ юнит-файлам (grep прямых вызовов переводимых IXlDrive-членов); файлы без прямых вызовов мигрируемых сигнатур (`VolumeCleanupServiceTests`, `XlVolumeTests`, `MultipartJournalsTests`, `MultipartCleanupTests` — там XlVolume/`MultipartJournals`-статика, не трогаемые) правок не требуют. Общее правило: statement-вызовы Result-методов (`_drive.CreateBucket(...)`, `WriteMeta`, `Commit`, `TrashUpload`, `WriteUploads`, `CreateUploadDir`, `RunCleanupAsync` без потребления результата) компилируются без правок — переводятся ТОЛЬКО потребления значений, ассерты и хелперы с типами возврата.

Ассерты бросаний (доменные) — паттерн:

```csharp
// Было:
// await act.Should().ThrowAsync<ObjectStoreException>()
//     .Where(e => e.Code == ObjectStoreErrorCode.InsufficientReadQuorum);
// Стало (AAA):
// Act
var result = await act();
// Assert
result.IsFailed.Should().BeTrue();
result.Error.Should().BeOfType<ObjectStoreException>()
    .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);
```

Добавить новые кейсы в `ErasureXlDriveReadTests.cs` — по образцу конструктора тест-класса этого файла (`new ErasurePool(_roots, new ErasureTopology(4, 4, 1), TimeProvider.System)` + `new ErasureXlDrive(_pool, defaultParity: 1, TimeProvider.System, NullLogger<ErasureXlDrive>.Instance)`):

```csharp
[Fact]
public void TryReadMeta_ОтсутствующаяЗапись_ТерминальныйSuccessMissing()
{
    // Arrange — пул/драйв по образцу конструктора тест-класса (EC 4+1)
    // Act
    var lookup = _drive.TryReadMeta("b", "k");
    // Assert — отсутствие записи — терминальный Success (конвенция 01 §4)
    lookup.IsSuccess.Should().BeTrue();
    lookup.Value.Should().BeOfType<XlMetaLookup.Missing>();
}
```

и кворумный отказ (отключение >parity дисков → `IsFailed` + `ObjectStoreException(InsufficientRead/WriteQuorum)`) — по образцу существующих деградационных тестов файла. Аналогичный Missing-кейс для `SingleXlDrive` — в `ReplicaStoreTests.cs` (фикстура файла создаёт `SingleXlDrive` поверх temp-тома — единственный single-драйвовый контекст юнитов; `XlVolumeTests` драйва не создаёт — там только `XlVolume`).

Компиляционные места и value-ассерты — ПОФАЙЛОВО (ломаются сменой сигнатур или падают в рантайме; переводятся ЗДЕСЬ, иначе Step 2.8 не собирается/не зелёный):

- `ErasureCleanupTests.cs`: `var digest = await _drive.WritePartAsync(...)` (~47) → `var digest = (await _drive.WritePartAsync(...)).Throw().Value;` (поля `digest.ETagHex/.Sha256Hex/.Length` в record-инициализации ~48–51); остальные вызовы — statement (без правок);
- `ErasureHealTests.cs`: Put-хелпер (~44–55): digest → `(await …).Throw().Value`; `return _drive.TryReadMeta("b", key)!;` (~55) → `return _drive.TryReadMeta("b", key).Throw().Value is XlMetaLookup.Found f ? f.Record : throw new InvalidOperationException("запись не найдена после Put");`; `using var stream = _drive.OpenData(...)` (~82, ~106, ~277) → `using var stream = _drive.OpenData(...).Throw().Value;` (`meta!`-распаковки ~276–277 — вместе с value-паттерном TryReadMeta); `_drive.ReadParts(...).Should().ContainSingle()` (~177) → `… .Throw().Value.Should().ContainSingle()`; `_drive.ListUploadKeyDirs().Single()` (~342) → `… .Throw().Value.Single()`; digest d1/d2 (~250–256) — как digest; `CommitMultipart` в `Task.Run` (~271) — Func-возврат Result компилируется (без правок);
- `ErasureXlDriveBucketsListTests.cs`: Put-хелпер digest (~40–41); `var meta = _drive.TryReadMeta(...)` (~62, ~79, ~209) → value-паттерн `var meta = _drive.TryReadMeta(...).Throw().Value is XlMetaLookup.Found f ? f.Record : null;`; null-ассерт `_drive.TryReadMeta("b", "del").Should().BeNull();` (~71) → `_drive.TryReadMeta("b", "del").Should().Match<Result<XlMetaLookup>>(r => r.IsSuccess && r.Value is XlMetaLookup.Missing);` (компилируется, но падает в рантайме — BeNull на Result всегда false); `var buckets = _drive.ListBuckets();` (~150, `.Select` на ~153) → `var buckets = _drive.ListBuckets().Throw().Value;`; EnumerateObjects-тесты — по абзацу выше (~158–217);
- `ErasureXlDriveMultipartTests.cs`: digest (~56–58, ~136, ~430–432) — `(await …).Throw().Value`; TryReadMeta meta/`meta!` (~95, ~158, ~241, ~272, ~312) — value-паттерн; `using var stream/body = _drive.OpenData(...)` (~100, ~273, ~314) → `.Throw().Value`; `_drive.ReadUploads(...)` (~107 `.Should().BeEmpty()`, ~181, ~203–206 `merged` + `WriteUploads(b, "keep", merged)`, ~363) → `.Throw().Value`; `_drive.ReadParts(...)` (~388, ~435) → `.Throw().Value`; `_drive.ListUploadKeyDirs().Single()` (~401) и `_drive.ReadUploadsByKeyDir(keyDirPath)` (~405) → `.Throw().Value`;
- `ErasureXlDriveReadTests.cs`: Put-хелпер (~40–48): digest + `return _drive.TryReadMeta(bucket, key)!;` — паттерн ErasureHealTests; `using var stream = _drive.OpenData(...)` (~73, ~89, ~106, ~126) → `.Throw().Value`; null-ассерт TryReadMeta (~139) — Missing-ассерт (паттерн BucketsList ~71); `_drive.EnumerateParts(...)` (~238) → `.Throw().Value`; `VerifyFullChecksum`-ассерты (~182 `NotThrow`, ~197 `Throw<XlIntegrityException>`) — СОХРАНЯЮТСЯ без правок: `XlIntegrityException` — вне перечня миграции (бросок остаётся), `NotThrow` на Result-возврате валиден и семантически верен;
- `ErasureXlDriveWriteTests.cs`: Put-хелпер (~39–46): digest + `return _drive.Commit(bucket, key, record, staging);` (хелпер возвращает `PutResult`) → `return _drive.Commit(...).Throw().Value;`; digest (~178–181);
- `ReplicaStoreTests.cs`: `fixture.Drive.ListBuckets().Single(e => e.Name == "bf").CreationDate…` (~142) → `fixture.Drive.ListBuckets().Throw().Value.Single(e => e.Name == "bf")…`; `Drive.BucketExists("bempty").Should().BeFalse();` (~161) → `.Value.Should().BeFalse()`;
- `ReplicaApplierTests.cs` (~201): `Drive.BucketExists(bucket).Should().BeFalse();` → `.Value.Should().BeFalse()` (bool теперь в `.Value`).

- [ ] **Step 2.8: Прогоны (срез собирается и зелёный)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests.Storage"`
Expected: PASS.

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS — HTTP-контракт не изменился (мосты бросают прежние исключения в прежних местах; Range/листинги ещё не переведены — Task 3).

Зачистка серий (страховочный гейт): `docker ps -a --format '{{.Names}}' | grep -c os3` → `0`; `docker network ls --format '{{.Name}}' | grep -c os3` → `0` (сироты — own-only `docker network rm`, массовое скопление от прерванных прогонов — `docker network prune -f`).

- [ ] **Step 2.9: Commit**

```bash
git add src/OwnS3.Storage/ src/OwnS3.App/Pipeline/VolumeCleanupService.cs src/OwnS3.App/Program.cs src/tests/OwnS3.UnitTests/Storage/ src/tests/OwnS3.UnitTests/Replication/ReplicaApplierTests.cs
git commit -m "t38: IXlDrive на Result/Result<T>: TryReadMeta → Result<XlMetaLookup> (Found|Missing, nullable-маркер устранён), BucketExists → Result<bool> и EnumerateObjects → Result<IXlObjectCursor> (EC-кворум: InsufficientReadQuorum → Failed — «503, не бакета нет/неполный листинг опаснее недоступности», канон 06 §7), мутации/журналы/Commit/CommitMultipart/OpenData — Result-сигнатуры; кворумные отказы EC и BucketNotEmpty — Failed(ObjectStoreException) вместо летящих исключений; SingleXlDrive/ErasureXlDrive переведены (броски XlPathEncoder в Result-методах гасятся в Failed); ErasureHeal потребляет исходы (HealBucketAsync: catch → IsFailed; Missing — пропуск; логика heal без изменений); VolumeCleanupService — Failed → LogError; Program.cs — сохранение fail-fast старта (Initialize().Throw() в стартовый catch: LogCritical + Environment.Exit(1) — байт-в-байт как сегодня); XlObjectStore/ReplicaStore — временные Throw()-мосты (демонтаж в задачах IObjectStore/IReplicaStore); тесты — Result-ассерты, переводы компиляции (BucketExists-bool в ReplicaStoreTests/ReplicaApplierTests, EnumerateObjects-курсор/кворум в ErasureXlDriveBucketsListTests) + кейсы Success(Missing)/кворум-Failed"
```

---

## Task 3: `IObjectStore` → Result + `XlObjectStore` + бросающие хелперы Storage + хендлерные мосты (фаза 1.3 + вход фазы 3)

**Files:**
- Modify: `src/OwnS3.Storage/IObjectStore.cs`
- Modify: `src/OwnS3.Storage/XlObjectStore.cs`, `XlObjectStore.Buckets.cs`, `XlObjectStore.Objects.cs`, `XlObjectStore.Copy.cs`, `XlObjectStore.List.cs`, `XlObjectStore.Multipart.cs` (перевод + демонтаж мостов Task 2)
- Modify: `src/OwnS3.Storage/RangeResolver.cs` (→ `Result<AppliedByteRange?>`)
- Modify: `src/OwnS3.Storage/ListWalker.cs` (→ `Result<ListPage>`)
- Modify: `src/OwnS3.Storage/Domain/DomainTypes.cs` (демонтаж `ObjectAttributesResult`)
- Modify (временные мосты): `src/OwnS3.App/Handlers/ObjectHandlers.cs`, `MultipartHandlers.cs`, `BucketHandlers.cs`, `ListHandlers.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs`, `XlObjectStoreObjectTests.cs`, `XlObjectStoreCopyTests.cs`, `XlObjectStoreListTests.cs`, `XlObjectStoreMultipartTests.cs`, `CommitResultEventMetadataTests.cs` (namespace `OwnS3.UnitTests.Storage` — НЕ подпадает под glob `XlObjectStore*Tests.cs`), `RangeResolverTests.cs`

**Interfaces:**
- Consumes: новые сигнатуры `IXlDrive` (Task 2), `GetAttributesOutcome` (Task 1).
- Produces (для Tasks 5–7): сигнатуры `IObjectStore` из таблицы Step 3.1; `Result<AppliedByteRange?> RangeResolver.Resolve(ByteRange? range, long size)`; `Result<ListPage> ListWalker.Walk(string bucket, ListQuery query)`.

- [ ] **Step 3.1: Новые сигнатуры `IObjectStore`**

```csharp
public interface IObjectStore
{
    // бакеты
    Task<Result> CreateBucketAsync(string bucket, CancellationToken ct);                // BucketAlreadyOwnedByYou → Failed
    Task<Result> DeleteBucketAsync(string bucket, CancellationToken ct);                // NoSuchBucket/BucketNotEmpty → Failed
    Task<Result<bool>> BucketExistsAsync(string bucket, CancellationToken ct);
    Task<Result<IReadOnlyList<BucketEntry>>> ListBucketsAsync(CancellationToken ct);

    // объекты
    Task<Result<PutResult>> PutObjectAsync(string bucket, string key, Stream body,
        long contentLength, ObjectUploadMetadata metadata, CancellationToken ct);
    Task<Result<ObjectContent>> GetObjectAsync(string bucket, string key,
        ObjectReadOptions options, CancellationToken ct);                               // NotModified/Range — в ObjectContent (форма не меняется)
    Task<Result<ObjectContent>> HeadObjectAsync(string bucket, string key,
        ObjectReadOptions? options, CancellationToken ct);
    Task<Result> DeleteObjectAsync(string bucket, string key, CancellationToken ct);    // идемпотентен (204-семантика)
    Task<Result<IReadOnlyList<DeletedKeyResult>>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct);                  // per-key состав — значение; отказ всей операции → Failed
    Task<Result<PutResult>> CopyObjectAsync(CopyRequest request, CancellationToken ct);
    Task<Result<GetAttributesOutcome>> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct);                            // NotModified|Full — терминальные Success

    // листинги
    Task<Result<ListPage>> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct); // битый continuation-token → Failed(XlInvalidArgumentException) — 400

    // multipart
    Task<Result<string>> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct);
    Task<Result<PutResult>> UploadPartAsync(string bucket, string key, string uploadId,
        int partNumber, Stream body, long contentLength, CancellationToken ct);
    Task<Result<CompleteResult>> CompleteMultipartUploadAsync(string bucket, string key,
        string uploadId, IReadOnlyList<PartEtag> parts, CancellationToken ct);
    Task<Result> AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct);
    Task<Result<PartsPage>> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct);
    Task<Result<UploadsPage>> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct);
}
```

- [ ] **Step 3.2: Перевод `XlObjectStore.*` (демонтаж мостов Task 2) + бросающие хелперы**

Правила для всех partial-файлов:

1. Демонтаж всех `// мост t38` Step 2.4: вызовы драйва теперь отдаются наверх как Result — цепочки через `Bind`/`BindAsync` либо явные `if (res.IsFailed) return Result<…>.Failed(res.Error!);`. Пример `EnsureBucket` (XlObjectStore.cs) — кворумный отказ `BucketExists` пробрасывается как `Failed`, отсутствие бакета — доменный `NoSuchBucket`:

```csharp
    // Отсутствие бакета — доменный исход NoSuchBucket (Failed по конвенции 01 §4);
    // кворумная недоступность EC — Failed(InsufficientReadQuorum) («503, не „бакета нет“», канон 06 §7)
    internal Result EnsureBucket(string bucket)
    {
        var exists = Drive.BucketExists(bucket);
        return exists.IsFailed
            ? Result.Failed(exists.Error!)
            : exists.Value
                ? Result.Success()
                : Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchBucket));
    }
```

2. Все `throw new ObjectStoreException(...)` / `throw new XlInvalidArgumentException(...)` в XlObjectStore.* → `return Result.Failed(...)` / `Result<T>.Failed(...)` (типы-носители не меняются).
3. Внутренние `try/catch` с перебросом (например, XlObjectStore.Multipart.cs:113 `throw;`) → возврат `Failed` с исходной ошибкой.
4. **`RangeResolver.Resolve` → `Result<AppliedByteRange?>`** (`src/OwnS3.Storage/RangeResolver.cs`): `null`-возврат «Range не задан» → `Result<AppliedByteRange?>.Success(null)`; три `throw ObjectStoreException(InvalidRange)` → `Result<AppliedByteRange?>.Failed(new ObjectStoreException(ObjectStoreErrorCode.InvalidRange))`. Вызовы в `XlObjectStore.Objects.cs` (GetObject/Head): `var resolved = RangeResolver.Resolve(options.Range, meta.Size); if (resolved.IsFailed) return Result<ObjectContent>.Failed(resolved.Error!); AppliedByteRange? range = resolved.Value;` — 416 InvalidRange приходит как Failed (транслятор Task 7 маппит по коду — HTTP-исход тот же).
5. **`ListWalker.Walk` → `Result<ListPage>`** (`src/OwnS3.Storage/ListWalker.cs`): `catch (FormatException)` с `throw new XlInvalidArgumentException("Invalid continuation token")` → `return Result<ListPage>.Failed(new XlInvalidArgumentException("Invalid continuation token"))`; `drive.EnumerateObjects(bucket)` (курсор, строка ~43) теперь `Result<IXlObjectCursor>` — `IsFailed`-исход пробрасывается как `Result<ListPage>.Failed(...)` (EC-кворум листинга: сегодня бросок летит из Walk в ListObjectsAsync → middleware → 503; мост Task 3 сохраняет то же поведение, финально — транслятор); остальное тело листинга без изменений — `XlIntegrityException` курсора гасится уже существующим в Walker пропуском/логом. Вызов в `XlObjectStore.List.cs`: `var page = walker.Walk(bucket, query); return page;` (ListObjectsAsync возвращает `Result<ListPage>` — проброс как есть).
6. **`CreateStaging`/`CreateUploadStaging` — конвертация летящего `XlInvalidArgumentException` на границе XlObjectStore** (методы драйва остались не-Result по Step 2.1; исключение — из `XlPathEncoder.EncodeSegments` «Key too long after encoding»). Во ВСЕХ местах вызова в XlObjectStore — `XlObjectStore.Objects.cs` (PutObject), `XlObjectStore.Copy.cs` (CopyObject), `XlObjectStore.Multipart.cs` (UploadPart через `CreateUploadStaging`) — обернуть вызов:

```csharp
    IXlStaging staging;
    try
    {
        staging = Drive.CreateStaging(bucket, key);
    }
    catch (XlInvalidArgumentException ex)
    {
        // Ключ не представим xl-путём (сегмент >255 байт, канон 04 §1) — доменный 400
        return Result<PutResult>.Failed(ex);
    }
```

(тип возврата — по сигнатуре операции; `using var staging` при этом сохраняется после блока).
7. `Task.Yield()`-контракты и троттлинги тела — без изменений; `OperationCanceledException` по ct не гасится (в async-методах без catch-all; `Result.FromAsync` НЕ использовать там, где возможна отмена).

Run (контроль полноты по ВСЕМУ Storage — летящие доменные исключения остаются ТОЛЬКО в XlPathEncoder, конвертируемом на границе): `grep -rn "throw new ObjectStoreException\|throw new XlInvalidArgumentException" src/OwnS3.Storage/ | grep -v "XlPathEncoder.cs" | grep -v "ReplicaStore.cs"`
Expected: пусто (ReplicaStore — Task 4; XlPathEncoder — осознанное исключение, его единственный бросок «Key too long after encoding» конвертируется на границе слоёв операций п. 6 и Step 4.2).

- [ ] **Step 3.3: Демонтаж DTO `ObjectAttributesResult`**

В `src/OwnS3.Storage/Domain/DomainTypes.cs` удалить record `ObjectAttributesResult` (строки с комментарием «Исход GetObjectAttributes (P10)…») — исход теперь `Result<GetAttributesOutcome>`; в `XlObjectStore.Objects.cs` (`GetObjectAttributesAsync`) вернуть `Result<GetAttributesOutcome>.Success(new GetAttributesOutcome.Full(attrs))` / `Success(new GetAttributesOutcome.NotModified(meta))` на тех же ветках, где сегодня заполняются `Attributes`/`NotModifiedMetadata`.

- [ ] **Step 3.4: Временные мосты в S3-хендлерах (Object/Multipart/Bucket/List) — HTTP-поведение не меняется**

Правило: `Store.XxxAsync` — Task-сигнатуры; extension `Throw()` в `Shared.Core` определён ТОЛЬКО для `ValueTask<Result*>` — для Task его НЕТ. Мост для async-вызовов — ТОЛЬКО скобочная форма: void-операции `(await Store.XxxAsync(...)).Throw();` (бросит `Error` — прежний путь в catch-ветки `S3Middleware`), значимые — `(await Store.XxxAsync(...)).Throw().Value` (идиома Step 2.4: `Result<T>.Throw()` возвращает `Result<T>`, не `T`; применяется уже к Result ПОСЛЕ await). Примеры:

```csharp
// ObjectHandlers.PutObjectHandler.HandleAsync:
var result = (await Store.PutObjectAsync(context.Bucket, context.Key, body, contentLength, metadata, ct))
    .Throw().Value; // мост t38: демонтаж в задаче App-финала

// GetObjectAttributesHandler.HandleAsync:
var outcome = (await Store.GetObjectAttributesAsync(context.Bucket, context.Key, attributes,
    maxParts == OperationValidation.DefaultPagingLimit ? null : maxParts,
    partNumberMarker == 0 ? null : partNumberMarker, conditions, ct)).Throw().Value;
if (outcome is GetAttributesOutcome.NotModified nm) { /* прежняя ветка NotModifiedMetadata */ }
else { var attrs = ((GetAttributesOutcome.Full)outcome).Attributes; /* прежняя ветка */ }
```

Drain-семантика PutObject временно: `catch → DrainBodyAsync → throw` заменяется на:

```csharp
            var put = await Store.PutObjectAsync(context.Bucket, context.Key, body, contentLength, metadata, ct);
            if (put.IsFailed)
            {
                await DrainBodyAsync(body, ct); // тело дочитывается до проброса исхода (мост t38)
                put.Throw();                    // бросает Error; отдельный return недостижим
                return;                         // (CS1997: в async Task — statement-throw, не return-throw)
            }
```

— тело по-прежнему дочитывается до проброса исхода (мост бросает `Error`), а `S3ProtocolException` из обёрток тела (BadDigest/InvalidRequest) перекрывает исход стора, как сегодня.

Каждый мост — комментарий `// мост t38: демонтаж в задаче App-финала`.

Statement-операции хендлеров (сегодня результат отбрасывается — после Task 3 отказ молча проглатывался бы: `DeleteObjectAsync`-Failed → ложный 204, `CreateBucketAsync`/`DeleteBucketAsync`/`AbortMultipartUploadAsync`-Failed → пропуск доменной ошибки) — обязательный мост той же скобочной формы, поимённо: `ObjectHandlers.cs` (~168, `DeleteObjectHandler`), `BucketHandlers.cs` (~61 Create, ~81 Delete), `MultipartHandlers.cs` (~116 Abort) → `(await Store.XxxAsync(...)).Throw();`.

- [ ] **Step 3.5: Тесты `XlObjectStore*Tests`/`RangeResolverTests` — Result-ассерты**

Перевести ассерты в `XlObjectStoreBucketTests.cs`, `XlObjectStoreObjectTests.cs`, `XlObjectStoreCopyTests.cs`, `XlObjectStoreListTests.cs`, `XlObjectStoreMultipartTests.cs`, `RangeResolverTests.cs` по паттерну Step 2.7. Семантические кейсы добавить.

`XlObjectStoreObjectTests.cs` — по образцу существующей фикстуры файла (`StoreFixture` с `new XlObjectStore(Drive, new FixedTimeProvider(TestVectors.FixedTime), …)`, тесты — `IClassFixture<StoreFixture>`):

```csharp
[Fact]
public async Task GetObject_ОтсутствующийКлюч_FailedСКодомNoSuchKey()
{
    // Arrange (fixture.Store — бакет "b" создан в InitializeAsync фикстуры)
    // Act
    var result = await fixture.Store.GetObjectAsync("b", "missing",
        new ObjectReadOptions(null, null), TestContext.Current.CancellationToken);
    // Assert — ошибка каталога операции — Failed с кодом (преобразование Missing→NoSuchKey на уровне S3-операции)
    result.IsFailed.Should().BeTrue();
    result.Error.Should().BeOfType<ObjectStoreException>()
        .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchKey);
}
```

`RangeResolverTests.cs` (перевод ВСЕХ ассертов файла):
- Throw-ассерты (~56, ~67) — по синхронному паттерну: `var result = RangeResolver.Resolve(...); result.IsFailed.Should().BeTrue(); result.Error.Should().BeOfType<ObjectStoreException>().Which.Code.Should().Be(ObjectStoreErrorCode.InvalidRange);`
- value-ассерты суффиксных кейсов (~36, ~44, ~46 — `.Should().Be(new AppliedByteRange(...))`) → `RangeResolver.Resolve(...).Throw().Value.Should().Be(new AppliedByteRange(...));` (компилируются, но падают в рантайме — `Be` на `Result<T>` всегда false);
- NoRange-кейс (~75 — `.Should().BeNull()`) → `RangeResolver.Resolve(null, size).Value.Should().BeNull();` («Range не задан» — терминальный `Success(null)`, Step 3.2.4).

```csharp
[Fact]
public void Resolve_ПустойОбъект_FailedInvalidRange()
{
    // Act
    var result = RangeResolver.Resolve(new ByteRange(0, null), size: 0);
    // Assert
    result.IsFailed.Should().BeTrue();
    result.Error.Should().BeOfType<ObjectStoreException>()
        .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidRange);
}

[Fact]
public void Resolve_RangeНеЗадан_SuccessNull_ДиапазонНеПрименяется()
{
    // Act
    var result = RangeResolver.Resolve(null, size: 10);
    // Assert — «Range не задан» — терминальное знание, не отказ
    result.IsSuccess.Should().BeTrue();
    result.Value.Should().BeNull();
}
```

`XlObjectStoreListTests.cs` — кейс битого continuation-token → `IsFailed` + `Error` типа `XlInvalidArgumentException` (замена/дубль существующего бросающего ассерта, если есть).

`XlObjectStoreBucketTests.cs` — bool-ассерты `BucketExistsAsync` (тест `BucketExists_TrueForRealBucket_FalseForSysDirAndMissing`, ~строки 105–113: `(await _store.BucketExistsAsync(...)).Should().BeTrue/BeFalse()`) перестают компилироваться (`await` теперь даёт `Result<bool>`) — перевести: `(await _store.BucketExistsAsync("b", CancellationToken.None)).Value.Should().BeTrue();` (и симметрично `.Value.Should().BeFalse()` для `.owns3.sys`/`missing`).

`CommitResultEventMetadataTests.cs` (фикстурный `Store` — 7 value-потребляющих вызовов; ассерты полей `PutResult`/`CompleteResult` после `.Throw().Value` НЕ меняются):
- `var result = await Store.PutObjectAsync(...)` (~28, ~68) → `var result = (await Store.PutObjectAsync(...)).Throw().Value;` (далее `result.ETag/.LastModified/.Size/.ContentSha256Hex` — как сегодня);
- `var uploadId = await Store.CreateMultipartUploadAsync(...)` (~43) → `… .Throw().Value;` (`uploadId` — string);
- `var e1/e2 = await Store.UploadPartAsync(...)` (~47, ~49) → `… .Throw().Value;` (далее `e1.ETag`/`e2.ETag` в manifest ~53–54);
- `var result = await Store.CompleteMultipartUploadAsync(...)` (~53) → `… .Throw().Value;`;
- `var result = await Store.CopyObjectAsync(...)` (~72) → `… .Throw().Value;`.

- [ ] **Step 3.6: Прогоны**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests.Storage"`
Expected: PASS.

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS без правок HTTP-ожиданий (критерий 4); `ErrorFormatScenarios`/`FailFastScenarios` — прежние 400/416 (мосты бросают те же исключения).

Зачистка серий — как в Step 2.8.

- [ ] **Step 3.7: Commit**

```bash
git add src/OwnS3.Storage/ src/OwnS3.App/Handlers/ src/tests/OwnS3.UnitTests/Storage/
git commit -m "t38: IObjectStore на Result/Result<T> (21 операция): ошибки каталога главы 02 — Failed(ObjectStoreException), GetObjectAttributes → Result<GetAttributesOutcome> (NotModified|Full — терминальные Success, DTO ObjectAttributesResult демонтирован), PutResult/CompleteResult/DeletedKeyResult — значения успеха; XlObjectStore.* переведён, мосты Task 2 демонтированы; бросающие хелперы Storage: RangeResolver.Resolve → Result<AppliedByteRange?> (InvalidRange → Failed — 416 сохранён), ListWalker.Walk → Result<ListPage> (битый continuation-token → Failed(XlInvalidArgument) — 400 сохранён), XlPathEncoder-броски CreateStaging конвертируются в Failed на границе XlObjectStore; S3-хендлеры — временные Throw()-мосты (HTTP-поведение не изменилось, демонтаж в задаче App-финала); drain-семантика PutObject сохранена; тесты стора/RangeResolver — Result-ассерты"
```

---

## Task 4: `IReplicaStore` → Result + потребители (фаза 1.3)

**Files:**
- Modify: `src/OwnS3.Storage/IReplicaStore.cs`
- Modify: `src/OwnS3.Storage/ReplicaStore.cs` (перевод + демонтаж мостов Task 2)
- Modify: `src/OwnS3.Replication/Lww/ReplicaApplier.cs` (адаптация вызовов стора; собственные сигнатуры — Task 6)
- Modify: `src/OwnS3.Replication/Inbound/NotificationInbox.cs`, `src/OwnS3.Replication/Reconcile/ReconcileRunner.cs` (адаптация)
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs`, `ReplicationAdminHandlers.cs` (fetch-грань/листинг/sync-report на новых сигнатурах)
- Test: `src/tests/OwnS3.UnitTests/Storage/ReplicaStoreTests.cs`, `src/tests/OwnS3.UnitTests/Replication/NotificationInboxTests.cs` (строка ~138 — компиляция `ReadKeyState`), `src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs` (строка ~192 — компиляция `ListBuckets` фикстурного `ReplicaStore`)

**Interfaces:**
- Consumes: `ReplicaKeyLookup`, `ReplicaListingPage` (Task 1), новые сигнатуры `IXlDrive` (Task 2).
- Produces (для Tasks 5–7):

```csharp
public interface IReplicaStore
{
    Task<Result> ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct);
        // ReplicationChecksumMismatchException и staging-снос — Failed
    Result DeleteObject(string bucket, string key);                       // идемпотентно
    Result<ReplicaKeyLookup> ReadKeyState(string bucket, string key);    // Found | Missing
    Result<ReplicaListingPage> ListReplicationEntries(string bucket, string? marker, int maxKeys);
        // NextMarker — в значении (out-параметр устранён)
    Result<(ObjectMetadata Metadata, Stream Body)> OpenObjectForReplication(string bucket, string key);
        // NoSuchKey → Failed(ObjectStoreException(NoSuchKey))
    Result ApplyBucketCreate(string bucket, DateTimeOffset creationDate);
    Result ApplyBucketDelete(string bucket);                              // BucketNotEmpty → Failed(ObjectStoreException)
    Result<IReadOnlyList<BucketEntry>> ListBuckets();
}
```

- [ ] **Step 4.1: Перевод сигнатур + `ReplicaStore`**

`IReplicaStore.cs` — к сигнатурам блока Produces. `ReplicaStore.cs`:
- `ApplyObjectAsync`: sha-мисматч → `Result.Failed(new ReplicationChecksumMismatchException(...))` (staging сносится в `finally`, как сегодня); `EnsureBucket`-ветки — по `Drive.*` Result-цепочке (`Bind`); `drive.CreateStaging(bucket, key)` — не-Result метод драйва: летящий `XlInvalidArgumentException` («Key too long») конвертируется на границе — как Step 3.2.6 (symmetric: `try { staging = drive.CreateStaging(...); } catch (XlInvalidArgumentException ex) { return Result.Failed(ex); }`); тело метода (WritePart/WriteMeta/Commit — уже Result-методы драйва) — цепочкой `Bind`;
- `ReadKeyState` → `Result<ReplicaKeyLookup>`: мета есть → `Found(new ReplicaKeyState(...))`; нет → `Missing()`; кворумный отказ драйва — `Failed` пробросом;
- `ListReplicationEntries` → `Result<ReplicaListingPage>` (внутренняя логика листинга без изменений; `nextMarker` локальная переменная уходит в конструктор значения; `drive.EnumerateObjects(bucket)` (строка ~61) теперь `Result<IXlObjectCursor>` — `IsFailed`-исход (EC-кворум) пробрасывается как `Result<ReplicaListingPage>.Failed(...)`, сегодня бросок летит в middleware → 503 — мост до Task 7, финально — helper);
- `OpenObjectForReplication`: нет меты → `Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey))`;
- `ApplyBucketDelete`/`ApplyBucketCreate`/`DeleteObject`/`ListBuckets` — мосты Task 2 демонтируются, `Drive.DeleteBucket(...)`-ошибки отдаются наверх (`BucketNotEmpty` — `Failed`).
- Демонтаж всех `// мост t38` из Task 2 в этом файле.

- [ ] **Step 4.2: `ReplicaApplier` — адаптация вызовов стора (сигнатуры аплаера — Task 6)**

- `ApplyObjectCoreAsync`/`ApplyDelete`: чтение состояния — общий паттерн (те же мосты, что Step 2.4):

```csharp
    // мост t38: демонтаж в задаче перевода ReplicaApplier
    private static ReplicaKeyState? UnwrapKeyState(Result<ReplicaKeyLookup> lookup)
        => lookup.IsSuccess
            ? lookup.Value is ReplicaKeyLookup.Found f ? f.State : null
            : throw lookup.Error!;
```

логика LWW-сравнения не меняется;
- `_store.ApplyObjectAsync(...)` (в локальной `ApplyAsync`) — мост скобочной формы `(await _store.ApplyObjectAsync(...)).Throw();` (Task-сигнатура — идиома Замечаний п.1(б); демонтаж в Task 6 вместе с переводом сигнатуры самого аплаера; мисматч продолжает лететь исключением до Task 6 — его ловит ObjectFetcher, см. Step 5.4);
- `ApplyDelete`: `_store.DeleteObject(...).Throw()` — мост;
- `ApplyBucketCreate`: `.Throw()` — мост;
- `ApplyBucketDelete` + `RetryTick`: заменить `catch (ObjectStoreException ex) when (ex.Code == BucketNotEmpty)` на исход:

```csharp
        var delete = _store.ApplyBucketDelete(bucket);
        if (delete.IsFailed && delete.Error is ObjectStoreException
            {
                Code: ObjectStoreErrorCode.BucketNotEmpty
            })
            EnqueueBucketRetry(bucket, nextAttempt: 0);
        else if (delete.IsFailed)
            delete.Throw(); // мост t38: прочие отказы — прежний путь исключения (демонтаж в Task 6)
```

(и симметрично в `RetryTick`; механика отложенных ретраев главы 07 §7 не меняется).

- [ ] **Step 4.3: `NotificationInbox` и `ReconcileRunner`**

- `NotificationInbox.HandleObjectPut`: `store.ReadKeyState(e.Bucket, e.Key)` → `switch` по `Found/Missing/Failed` (`Failed` — мост `.Throw()`; поведение прежнее); остальное без изменений;
- `ReconcileRunner.RunCoreAsync`: `store.ReadKeyState(bucket, entry.Key)` — как выше; вызовы `transport.ListingBucketsAsync`/`ListAsync` — ещё старые (транспорт — Task 5), не трогать.

- [ ] **Step 4.4: `ReplicationHandlers` + `ReplicationAdminHandlers` — потребление новых сигнатур**

- `HandleObjectFetchAsync` (fetch-грань, ReplicationHandlers.cs): `ReadKeyState` → `Failed → мост .Throw()` (кворумный отказ чтения/длинный ключ — НЕ «Missing»: техотказ не должен превращаться в tombstone/404 — принцип 3 spec; переходно его ловит живой до Task 7 middleware catch → 503 + XML Insufficient*Quorum / 400, как сегодня); `Found → открыть и отдать 200`; `Missing → tombstone/404` (та же логика). `OpenObjectForReplication` → `Result<(Metadata, Body)>`: `Success → 200-отдача`; `Failed` — ЕДИНЫЙ мост `.Throw()` (бросает, как сегодня): `NoSuchKey`-гонка удаления → 404 + XML-тело NoSuchKey (канон 03 §5), кворумные `Insufficient*` → 503 + XML, прочие → catch-all 500 — всё байт-в-байт как сегодня через middleware-ветки (пустой 404 НЕ вводится). Демонтаж мостов и явная запись тех же ответов — Task 7 (Step 7.3.5, единый helper на все грани):

```csharp
        var stateResult = store.ReadKeyState(bucket, key);
        if (stateResult.IsFailed)
            stateResult.Throw(); // мост t38: техотказ чтения — как сегодня (middleware → 503/400 + XML); демонтаж в Task 7
        if (stateResult.Value is ReplicaKeyLookup.Found found)
        {
            var opened = store.OpenObjectForReplication(bucket, key);
            // мост t38: Failed (вкл. NoSuchKey-гонку удаления, кворумные Insufficient*)
            // пробрасывается как сегодня — middleware-ветки отдают 404/503 + XML (03 §5);
            // демонтаж в задаче App-финала (Step 7.3.5) — единый helper записи тех же ответов
            var (metadata, body) = opened.Throw().Value; // идиома .Throw().Value (Step 2.4)
            // … прежняя отдача 200 + заголовки x-owns3-repl-* (без изменений)
        }
```

`HandleListingAsync`: `store.ListBuckets()` / `store.ListReplicationEntries(...)` → `Failed`-ветка — мост `.Throw()` (кворумные отказы листинга сегодня доходят до middleware catch → 503 + XML; сохраняется до Task 7 — Step 7.3.5); успех — прежние JSON-ответы (без изменений). `ReplicationAdminHandlers.HandleSyncReportAsync`: `ListReplicationEntries` — тот же мост `.Throw()` (переходно), `nextMarker` — из значения страницы.

- [ ] **Step 4.5: Тесты `ReplicaStoreTests`/`NotificationInboxTests`/`ReconcileRunnerTests` — Result-ассерты и компиляция**

`ReplicaStoreTests.cs` (полный перевод — value-потребления, сигнатура без out, ассерты):
- `ReadKeyState` отсутствующего ключа → `IsSuccess && Value is ReplicaKeyLookup.Missing` (новый кейс);
- `ApplyObjectAsync` sha-мисматч → `IsFailed` + `Error` типа `ReplicationChecksumMismatchException` (замена существующего `ThrowAsync`-ассерта); ключ длиннее xl-пути (сегмент >255 байт) → `IsFailed` + `Error` типа `XlInvalidArgumentException` (граничная конвертация Step 4.1);
- `var opened = Store.OpenObjectForReplication("b", "k"); using (opened.Body)` (~68–69) → `var (metadata, body) = Store.OpenObjectForReplication("b", "k").Throw().Value; using (body)` (заголовки — из `metadata`);
- `Store.ListReplicationEntries(bucket, null, 2, out var nextMarker)` / `…, nextMarker, 2, out var next2` (~113, ~123) — out-параметр устранён: `var page1 = Store.ListReplicationEntries(bucket, null, 2).Throw().Value; var nextMarker = page1.NextMarker;` (и симметрично `page2`/`next2`; ассерты записей — по `page1.Entries`/`page2.Entries`);
- `var act = () => Store.ApplyBucketDelete("bfull");` + Throw-ассерт `BucketNotEmpty` (~158) → Result-ассерт (`IsFailed` + `Error` типа `ObjectStoreException` с кодом `BucketNotEmpty`); statement-вызовы `ApplyBucketDelete` (~157, ~162) — без правок.

Компиляция `NotificationInboxTests.cs` (~строка 138): `_fixture.Store.ReadKeyState(bucket, key)!.Size.Should().Be(body.Length);` → value-паттерн:

```csharp
        // Assert — ключ не перезаписан (размер исходного тела)
        var state = _fixture.Store.ReadKeyState(bucket, key).Throw().Value;
        (state is ReplicaKeyLookup.Found f ? f.State.Size : -1).Should().Be(body.Length);
```

Компиляция `ReconcileRunnerTests.cs` (~строка 192; фикстура содержит `public ReplicaStore Store`): `_fixture.Store.ListBuckets().Should().ContainSingle(b => b.Name == "new-bucket").Which.CreationDate…` → `_fixture.Store.ListBuckets().Throw().Value.Should().ContainSingle(b => b.Name == "new-bucket").Which.CreationDate…`. Statement-вызовы аплаера (`ApplyBucketCreate`/`ApplyObjectAsync`, ~115–123) — без правок; `FakeReconcileTransport` — Task 5 (Step 5.7).

- [ ] **Step 4.6: Прогоны**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests"`
Expected: PASS (вся сборка юнитов: Storage + Replication).

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS без правок HTTP-ожиданий (в т.ч. `ReplicationScenarios`, `ReplicationAdminScenarios`, `ReplicationEventsScenarios`).

Зачистка серий — как в Step 2.8.

- [ ] **Step 4.7: Commit**

```bash
git add src/OwnS3.Storage/IReplicaStore.cs src/OwnS3.Storage/ReplicaStore.cs src/OwnS3.Replication/ src/OwnS3.App/Handlers/ReplicationHandlers.cs src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs src/tests/OwnS3.UnitTests/Storage/ReplicaStoreTests.cs src/tests/OwnS3.UnitTests/Replication/NotificationInboxTests.cs src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs
git commit -m "t38: IReplicaStore на Result/Result<T>: ReadKeyState → Result<ReplicaKeyLookup> (Found|Missing), ListReplicationEntries → Result<ReplicaListingPage> (NextMarker в значении, out-параметр устранён), OpenObjectForReplication/ApplyBucketDelete — Result (NoSuchKey/BucketNotEmpty/ReplicationChecksumMismatch — Failed); ReplicaStore переведён (мосты Task 2 демонтированы; CreateStaging-броски XlPathEncoder конвертированы в Failed на границе); потребители (ReplicaApplier/NotificationInbox/ReconcileRunner/ReplicationHandlers/ReplicationAdminHandlers) — на новых сигнатурах, LWW-логика/механика ретраев не изменилась; fetch-грань: Failed (вкл. NoSuchKey-гонку) пробрасывается мостом как сегодня — 404 + XML-тело от middleware catch (пустой 404 не вводится)"
```

---

## Task 5: Транспорт репликации → Result (фаза 2.1/2.3 spec)

**Files:**
- Modify: `src/OwnS3.Replication/Transport/IReplicationTransport.cs`
- Modify: `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs`
- Modify: `src/OwnS3.Replication/Transport/ReplicationDto.cs` (демонтаж `FetchResult`)
- Modify: `src/OwnS3.Replication/Notifier/NotificationDispatcher.cs`, `Fetching/ObjectFetcher.cs`, `Reconcile/ReconcileRunner.cs`, `ReplicationProber.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/NotificationDispatcherTests.cs`, `ObjectFetcherTests.cs`, `ReconcileRunnerTests.cs`, `ReplicationProberTests.cs`, `src/tests/OwnS3.UnitTests/SigV4SignerTests.cs` (единственное юнит-покрытие РЕАЛЬНОГО транспорта `ReplicationHttpClient` — 6 вызовов; namespace корневой — вне фильтра `OwnS3.UnitTests.Replication`)

**Interfaces:**
- Consumes: `FetchOutcome`, `PeerRejectedException` (Task 1).
- Produces (для Tasks 6–7):

```csharp
// PingResult: RttMs — НЕ nullable (нет ответа — нет Success; конвенция 01 §4)
public sealed record PingResult(bool Reachable, long RttMs);

public interface IReplicationTransport
{
    // 200 → Success(acceptedToSeqNo); 429/HTTP-отказ/обрыв → Failed(PeerRejectedException)
    Task<Result<long>> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch,
        CancellationToken ct);

    // 200-объект | 200-tombstone | 404 — терминальные Success-варианты FetchOutcome;
    // транспортный отказ → Failed; DTO FetchResult демонтирован
    Task<Result<FetchOutcome>> FetchObjectAsync(TargetRecord target, string bucket, string key,
        CancellationToken ct);

    Task<Result<ReplicationListingResponse>> ListAsync(TargetRecord target, string bucket,
        string? marker, int maxKeys, CancellationToken ct);

    Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ListingBucketsAsync(
        TargetRecord target, CancellationToken ct);

    // Любой полученный HTTP-ответ → Success(PingResult(reachable, rttMs));
    // обрыв/per-target-таймаут → Failed; внешняя отмена ct — проброс
    Task<Result<PingResult>> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct);
}
```

- [ ] **Step 5.1: `ReplicationHttpClient` — перевод реализации**

Правила по методам:

1. `SendNotificationsAsync`: 200 → `Result<long>.Success(accepted.AcceptedToSeqNo)`; 429 → `Result<long>.Failed(new PeerRejectedException($"сосед не принял батч: 429 (target={target.Name})"))`; прочие не-2xx (сегодня `EnsureSuccessStatusCode`) и обрыв → `Failed(new PeerRejectedException(...))` — обёртка `try/catch` с фильтром отмены:

```csharp
        try
        {
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                return Result<long>.Failed(new PeerRejectedException(
                    $"сосед не принял батч: 429 (target={target.Name})"));
            if (!response.IsSuccessStatusCode)
                return Result<long>.Failed(new PeerRejectedException(
                    $"HTTP {(int)response.StatusCode} от {target.Name}"));
            var accepted = JsonSerializer.Deserialize<NotificationAcceptedResponse>(
                await response.Content.ReadAsByteArrayAsync(ct), ReplicationTransportJson.Options);
            return Result<long>.Success(accepted?.AcceptedToSeqNo ?? 0);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции (graceful stop)
        }
        catch (Exception ex)
        {
            return Result<long>.Failed(new PeerRejectedException($"транспорт рассылки к {target.Name}: {ex.Message}"));
        }
```

2. `FetchObjectAsync`: 404 → `Success(new FetchOutcome.KeyNotFound())`; tombstone-заголовок → `Success(new FetchOutcome.Tombstone(tombstone))`; 200-объект → `Success(new FetchOutcome.Fetched(body, …))` (те же заголовки/парсинг); не-2xx/обрыв → `Failed(PeerRejectedException)` (по паттерну п.1); OCE по внешнему ct — `throw`.
3. `ListAsync`/`ListingBucketsAsync`: `EnsureSuccessStatusCode` → `Failed(PeerRejectedException)`; десериализация — успех (значения как сегодня).
4. `PingAsync`: ответ (любой код) → `Result<PingResult>.Success(new PingResult(statusCode == OK, rttMs))`; per-target-таймаут (`OperationCanceledException` при `!ct.IsCancellationRequested`) и `HttpRequestException` → `Failed(new PeerRejectedException(...))` — nullable-`RttMs` демонтирован; внешний ct — проброс.

- [ ] **Step 5.2: Демонтаж DTO `FetchResult`**

В `src/OwnS3.Replication/Transport/ReplicationDto.cs` удалить record `FetchResult` (вместе с комментарием «Исход фетча…»); его роль — `FetchOutcome` (Task 1).

Run: `grep -rn "\bFetchResult\b" src/ --include="*.cs"`
Expected: `0` совпадений (упоминания `FetchOutcome` подстрокой не считаются).

- [ ] **Step 5.3: `NotificationDispatcher` — `Result<long>`**

В `RunLoopAsync`: заменить блок `long? accepted; try { … } catch { accepted = null; }` на:

```csharp
            var send = await _transport.SendNotificationsAsync(_target,
                new NotificationBatchRequest(_originHostId, batch), ct);
            if (send is { IsSuccess: true, Value: var ack })
            {
                // ACK приёма: watermark двигается (только вперёд), персист дебаунсом (§5)
                watermark = Math.Max(watermark, ack);
                _cursors.Advance(_target.Name, ack);
                Online = true;
                _metrics.Notified(_target.Name, "accepted");
                backoff = _backoffBase;
                continue;
            }

            // 429/ошибка: приёмник не принял — бэкофф, повтор от watermark (at-least-once)
            Online = false;
            _metrics.Notified(_target.Name, "backoff");
            _metrics.Retried(_target.Name);
            _logger.LogInformation("Сосед {Target} не принял уведомления ({Error}) — бэкофф {Backoff}",
                _target.Name, send.Error!.Message, backoff);
            await Task.Delay(backoff, ct);
            backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2,
                TimeSpan.FromSeconds(_options.BackoffMaxSec).Ticks));
```

OCE по ct — пробрасывается транспортом (SendNotificationsAsync не гасит), `RunAsync`-catch наверху сохраняется. Механика §8 не меняется.

- [ ] **Step 5.4: `ObjectFetcher.TryFetchOneAsync` — `Result<FetchOutcome>` (catch-блоки СОХРАНЯЮТСЯ до Task 6)**

Заменить тело `try` на матчинг терминальных вариантов (транзит `Failed` — повтор):

```csharp
        var fetch = await _transport.FetchObjectAsync(_source, task.Bucket, task.Key, ct);
        if (fetch.IsFailed)
            return HandleFetchFailure(_source.Name, task, fetch.Error!); // Failed → повтор по бэкоффу (глава 07 §7)
        switch (fetch.Value)
        {
            case FetchOutcome.Tombstone t:
                // Ключ удалён у источника — tombstone-применение (глава 07 §7)
                _applier.ApplyDelete(task.Bucket, task.Key, t.DeleteTimeUnixMs);
                _counters.Applied(_source.Name);
                return false;
            case FetchOutcome.KeyNotFound:
                // Ключ неизвестен — состояние приёмника не меняется (skip без повтора)
                return false;
            case FetchOutcome.Fetched f:
                await using (f.Body)
                {
                    Stream body = _options.MaxRateBytes > 0
                        ? new ThrottledStream(f.Body, _options.MaxRateBytes)
                        : f.Body;
                    var applyResult = await _applier.ApplyObjectAsync(task.Bucket, task.Key, body,
                        contentLength: f.ContentLength,
                        new ReplicaObjectMetadata(
                            DateTimeOffset.FromUnixTimeMilliseconds(f.ModTimeUnixMs),
                            f.ETag.Trim('"'), f.ContentSha256, f.ContentType, f.UserMetadata),
                        expectedSha256: f.ContentSha256, ct);
                    // счётчики исходов — по результату СВОЕГО применения (перевод аплаера —
                    // следующая задача; здесь мост: applyResult — прежний тип до Task 6)
                    _counters.Fetched(_source.Name);
                    if (applyResult.Conflict) _counters.Conflict(_source.Name);
                    if (applyResult.Applied) _counters.Applied(_source.Name);
                    else _counters.Skipped(_source.Name);
                }
                return false;
        }
        return false; // недостижимо (варианты закрыты)
```

`HandleFetchFailure` — новый приватный метод (единая точка повтора):

```csharp
    // Отказ фетча/применения: sha-мисматч и техотказы — повтор по бэкоффу (глава 07 §7).
    private bool HandleFetchFailure(string sourceName, FetchTask task, Exception error)
    {
        _logger.LogWarning(error, "Фетч {Source}/{Bucket}/{Key} не удался — повтор по бэкоффу",
            sourceName, task.Bucket, task.Key);
        return true;
    }
```

**Переходное состояние (аплаер ещё на мостах Task 4 — до Task 6):** существующие catch-блоки `ReplicationChecksumMismatchException` и `Exception` в `TryFetchOneAsync` СОХРАНЯЮТСЯ с маршрутизацией в новый helper — мисматч-исключение из моста `Throw()` аплаера не должно убивать воркер (`WorkerLoopAsync`/`DrainAsync`/`RunAsync` ловят только OCE), а тест `FetchChecksumMismatch_RequeuedNotApplied` (мисматч рождается в реальном аплаере/сторе) обязан оставаться зелёным:

```csharp
        catch (ReplicationChecksumMismatchException ex)
        {
            // мост t38: аплаер ещё бросает (до Task 6) — маршрутизуем в единый helper повтора
            return HandleFetchFailure(_source.Name, task, ex);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // graceful stop — не ошибка фетча
        }
        catch (Exception ex)
        {
            // мост t38: техотказ применения/чтения тела (до Task 6) — повтор
            return HandleFetchFailure(_source.Name, task, ex);
        }
```

Демонтаж обоих catch-блоков — Step 6.3.1 (вместе с переводом аплаера, где checksum-mismatch становится `Error`-исходом применения).

- [ ] **Step 5.5: `ReconcileRunner` — листинги на Result**

В `RunCoreAsync` оба ретрай-цикла (`ListingBucketsAsync`, `ListAsync`): вместо `try { … break; } catch (Exception ex) { LogWarning; Delay; }` — потребление исхода:

```csharp
                var listResult = await transport.ListingBucketsAsync(target, ct);
                if (listResult.IsSuccess)
                {
                    neighborBuckets = listResult.Value;
                    break;
                }
                logger.LogWarning(listResult.Error, "Reconcile {Source}: сосед недоступен — ретрай через {Backoff}",
                    sourceName, backoff);
                await Task.Delay(backoff, ct);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(60).Ticks));
```

(симметрично для `ListAsync`; OCE-фильтры `when (ct.IsCancellationRequested) → throw` сохраняются). Бэкофф-механика §9 не меняется.

- [ ] **Step 5.6: `ReplicationProber.PingQuietAsync` — `Result<PingResult>`**

```csharp
    private async Task<ProbeOutcome> PingQuietAsync(TargetRecord target, CancellationToken ct)
    {
        var ping = await transport.PingAsync(target,
            TimeSpan.FromMilliseconds(options.ProbeTimeoutMs), ct);
        // Обрыв/таймаут пер-цели — исход false без rttMs (глава 07 §2); внешняя
        // отмена ct пробрасывается транспортом наверх
        return ping is { IsSuccess: true, Value: var p }
            ? new ProbeOutcome(target.Name, p.Reachable, p.RttMs)
            : new ProbeOutcome(target.Name, false, null);
    }
```

(`ProbeOutcome` — JSON-модель admin-грани — НЕ меняется: `RttMs` остаётся nullable-полем ответа.) Сигнатура `ProbeAsync → Task<ProbeReport?>` здесь пока не меняется (Task 6).

- [ ] **Step 5.7: Фейки тестов — новые сигнатуры**

`FakeTransport` (NotificationDispatcherTests), `FakeFetchTransport` (ObjectFetcherTests), `FakeReconcileTransport` (ReconcileRunnerTests), `FakeProbeTransport` (ReplicationProberTests): привести к новым сигнатурам; сценарные исходы фейков теперь строят `Result`/`FetchOutcome`:

```csharp
// FakeFetchTransport (пример):
// 404 → Result<FetchOutcome>.Success(new FetchOutcome.KeyNotFound());
// tombstone → Success(new FetchOutcome.Tombstone(ms));
// сетевой сбой → Result<FetchOutcome>.Failed(new PeerRejectedException("источник offline (тест)"));
// 429 нотификаций → Result<long>.Failed(new PeerRejectedException("429 (тест)"));
// ping-ответ → Result<PingResult>.Success(new PingResult(reachable, rtt));
// ping-обрыв → Result<PingResult>.Failed(new PeerRejectedException("обрыв (тест)"));
```

- [ ] **Step 5.8: Тесты Dispatcher/Fetcher/Reconcile — семантика Result (критерий 5)**

Существующие сценарии 429/обрыв/404/tombstone продолжают покрывать механику (фейки возвращают те же исходы в новой форме). Добавить/скорректировать ассерты семантики:

- Dispatcher: `429 → Failed → Online=false + счётчик backoff + повтор от watermark` (существующий тест на 429 — ассерт `Online.Should().BeFalse()` остаётся);
- Fetcher: `404 → skip без повтора` (очередь пустеет, retry-коллекция пуста — как сегодня), `tombstone → ApplyDelete`, `Failed(PeerRejected) → повтор` (бэкофф); `FetchChecksumMismatch_RequeuedNotApplied` — зелёный (мисматч из реального аплаера ловится переходным catch-блоком Step 5.4);
- ReconcileRunner: сосед недоступен → ретрай (существующий тест с фейк-отказом).

`SigV4SignerTests.cs` — единственное юнит-покрытие РЕАЛЬНОГО `ReplicationHttpClient` (6 вызовов через `new ReplicationHttpClient(new HttpClient(handler) …)`; контрольный grep `FetchResult` файл не ловит — везде `var`). Перевод по вариантам `FetchOutcome`:

- 200-объект (~85–95): `var result = await transport.FetchObjectAsync(...)` → `fetch.IsSuccess.Should().BeTrue(); var fetched = fetch.Value.Should().BeOfType<FetchOutcome.Fetched>().Which;` — далее ассерты полей варианта: `fetched.Body.Should().NotBeNull(); fetched.ModTimeUnixMs.Should().Be(1760000000000); fetched.ContentSha256/ETag/ContentType/UserMetadata` — как сегодня (тело — потоковое, `fetched.Body`);
- tombstone (~115–122): `fetch.Value.Should().BeOfType<FetchOutcome.Tombstone>().Which.DeleteTimeUnixMs.Should().Be(1760001111000);` (тела нет — вариант без Body);
- 404 (~131–138): `fetch.Value.Should().BeOfType<FetchOutcome.KeyNotFound>();` (терминальный Success — состояние приёмника не меняется);
- `SendNotifications_ParsesAcceptedToSeqNo` (~156–160): `var accepted = await transport.SendNotificationsAsync(...)` → `(await transport.SendNotificationsAsync(...)).Throw().Value.Should().Be(42);`;
- `SendNotifications_429_ReturnsNull` (~172–177 — переименовать `…_ReturnsFailed`): nullable-маркер демонтирован → `var send = await …; send.IsFailed.Should().BeTrue(); send.Error.Should().BeOfType<PeerRejectedException>();` — ровно семантика критерия 5 (429 → Failed → бэкофф вызывающего);
- листинг бакетов (~195–199): `var buckets = await transport.ListingBucketsAsync(...)` → `(await …).Throw().Value.Should().ContainSingle().Which.Name.Should().Be("b1");` (+ `buckets[0].CreationUnixMs` — по значению).

Тесты используют AAA-комментарии; имена исходов — `Result<FetchOutcome>.Success(...)`/`Failed(...)`.

- [ ] **Step 5.9: Прогоны**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests.Replication"`
Expected: PASS (включая `FetchChecksumMismatch_RequeuedNotApplied` — переходные catch-блоки Step 5.4 на месте).

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS (ReplicationScenarios/ReplicationEventsScenarios/ReplicationAdminScenarios — HTTP-контракт грани не изменился).

Зачистка серий — как в Step 2.8.

- [ ] **Step 5.10: Commit**

```bash
git add src/OwnS3.Replication/ src/tests/OwnS3.UnitTests/Replication/ src/tests/OwnS3.UnitTests/SigV4SignerTests.cs
git commit -m "t38: IReplicationTransport на Result/Result<T>: SendNotificationsAsync → Result<long> (429/не-200/обрыв → Failed(PeerRejectedException), nullable-маркер long? устранён), FetchObjectAsync → Result<FetchOutcome> (Fetched|Tombstone|KeyNotFound — терминальные Success, DTO FetchResult демонтирован), ListAsync/ListingBucketsAsync → Result, PingAsync → Result<PingResult> (любой HTTP-ответ — Success, RttMs не-null, обрыв/таймаут — Failed); потребители (NotificationDispatcher/ObjectFetcher/ReconcileRunner/ReplicationProber) — на Match-исходах, бэкоффы/повторы/счётчики без изменений; переходные catch-блоки фетчера (мисматч/техотказ аплаера до его перевода в Task 6) маршрутизированы в HandleFetchFailure — воркер не падает; OCE внешнего ct — проброс; фейки тестов — новые сигнатуры"
```

---

## Task 6: `ReplicaApplier` + `ReplicationProber` (фаза 2.2 spec)

**Files:**
- Modify: `src/OwnS3.Replication/Lww/ReplicaApplier.cs`
- Modify: `src/OwnS3.Replication/ReplicationProber.cs`
- Modify: `src/OwnS3.Replication/Fetching/ObjectFetcher.cs` (счётчики по outcome; демонтаж переходных catch Step 5.4)
- Modify: `src/OwnS3.Replication/Inbound/NotificationInbox.cs` (ApplyDelete/ApplyBucket* → Result)
- Modify: `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` (`HandleProbeAsync` — 400 по `Failed(Unknown…)`; `HandleSyncReportAsync` — демонтаж моста Task 4 при наличии)
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (демонтаж оставшихся мостов Task 4)
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicaApplierTests.cs`, `ReplicationProberTests.cs`, `ObjectFetcherTests.cs`

**Interfaces:**
- Consumes: `ReplicaApplyOutcome` (Task 1 — 4 значения), `UnknownReplicationTargetException` (Task 1), `Result`-сигнатуры `IReplicaStore` (Task 4) и `IReplicationTransport` (Task 5).
- Produces (для Task 7):
  - `Task<Result<ReplicaApplyOutcome>> ApplyObjectAsync(...)`; `Result ApplyDelete(string bucket, string key, long deleteTimeUnixMs)`; `Result ApplyBucketCreate(string bucket, long creationDateUnixMs)`; `Result ApplyBucketDelete(string bucket)`.
  - `Task<Result<ProbeReport>> ProbeAsync(string? targetName, CancellationToken ct)` — `Failed(UnknownReplicationTargetException)` = «имя не из состава».

- [ ] **Step 6.1: `ReplicaApplier` → Result-сигнатуры (демонтаж `ReplicaApplyResult` и мостов Task 4)**

1. Удалить вложенный `readonly record struct ReplicaApplyResult(bool Applied, bool Conflict)`; сигнатуры:

```csharp
    public async Task<Result<ReplicaApplyOutcome>> ApplyObjectAsync(string bucket, string key, Stream body,
        long contentLength, ReplicaObjectMetadata metadata, string expectedSha256, CancellationToken ct)
        // IncomingWins+применено без конфликта → Success(Applied);
        // Identical/LocalWins без конфликта → Success(Skipped);
        // конфликтная победа incoming (равные mod, sha-тай-брейк) → Success(ConflictApplied)
        // — применено после конфликта; конфликтный LocalWins (тай-брейк в пользу
        // локальной, входящая не применена) → Success(ConflictSkipped); событие
        // ConflictDetected — только лог, как сегодня; технический отказ — Failed

    public Result ApplyDelete(string bucket, string key, long deleteTimeUnixMs)
    public Result ApplyBucketCreate(string bucket, long creationDateUnixMs)
    public Result ApplyBucketDelete(string bucket)   // BucketNotEmpty → внутренний RetryQueue (как сегодня), не Failed
```

2. Точки перевода:
   - «знаний нет — входящее применяется свободно» → `Success(ReplicaApplyOutcome.Applied)`;
   - `outcome == IncomingWins ? ApplyAsync(conflict) : Skip(conflict)`: `ApplyAsync` возвращает `Result<ReplicaApplyOutcome>.Success(conflict ? ReplicaApplyOutcome.ConflictApplied : ReplicaApplyOutcome.Applied)` при победе incoming (конфликтная победа — `ConflictApplied`, тоже применено); skip-ветка возвращает `Success(conflict ? ReplicaApplyOutcome.ConflictSkipped : ReplicaApplyOutcome.Skipped)` (конфликтный LocalWins — `ConflictSkipped`); `ConflictDetected`-событие и лог — без изменений;
   - демонтаж моста `_store.ApplyObjectAsync(...).Throw()`: проброс `Failed` стора наружу (`if (apply.IsFailed) return Result<ReplicaApplyOutcome>.Failed(apply.Error!);`) — checksum-mismatch теперь приходит фетчеру как `Error`, а не исключение;
   - `ApplyDelete`/`ApplyBucketCreate`: `_store.…` Result-возврат напрямую (демонтаж `Throw()`-мостов Task 4);
   - `ApplyBucketDelete`: Step 4.2-паттерн уже готов — прочие (не BucketNotEmpty) `Failed` теперь возвращаются (не `Throw()`); `RetryTick` — симметрично.

- [ ] **Step 6.2: `ReplicationProber.ProbeAsync` → `Task<Result<ProbeReport>>`**

```csharp
    public async Task<Result<ProbeReport>> ProbeAsync(string? targetName, CancellationToken ct)
    {
        var snapshot = targets.Snapshot;
        if (targetName is not null && snapshot.All(t => t.Name != targetName))
            return Result<ProbeReport>.Failed(new UnknownReplicationTargetException(
                $"цель probe не входит в состав targets: {targetName}"));
        IReadOnlyList<TargetRecord> selected = targetName is null
            ? snapshot
            : snapshot.Where(t => t.Name == targetName).ToList();
        var outcomes = await Task.WhenAll(selected.Select(t => PingQuietAsync(t, ct)));
        return Result<ProbeReport>.Success(new ProbeReport(selected.Count > 0, outcomes));
    }
```

`ProbeReport`/`ProbeOutcome` (JSON-модель ответа admin-грани) — НЕ меняются.

- [ ] **Step 6.3: Потребители (счётчики — семантика канона 07 §10 «как сегодня», байт-в-байт)**

1. `ObjectFetcher.TryFetchOneAsync` (демонтаж моста Step 5.4 И переходных catch-блоков): ветка `FetchOutcome.Fetched` →

```csharp
                    var apply = await _applier.ApplyObjectAsync(task.Bucket, task.Key, body,
                        contentLength: f.ContentLength,
                        new ReplicaObjectMetadata(
                            DateTimeOffset.FromUnixTimeMilliseconds(f.ModTimeUnixMs),
                            f.ETag.Trim('"'), f.ContentSha256, f.ContentType, f.UserMetadata),
                        expectedSha256: f.ContentSha256, ct);
                    if (apply.IsFailed)
                        return HandleFetchFailure(_source.Name, task, apply.Error!); // checksum-mismatch и техотказы применения — повтор (глава 07 §7); fetched НЕ инкрементируется — байт-в-байт как сегодня (сегодня мисматч бросается из ApplyObjectAsync ДО строки счётчика)
                    _counters.Fetched(_source.Name);
                    // Инкременты — по исходу, семантика «как сегодня» (канон 07 §10):
                    // пара ConflictApplied/ConflictSkipped несёт флаг конфликта ПОВЕРХ
                    // исхода применения — конфликт сосуществует с applied/skipped
                    switch (apply.Value)
                    {
                        case ReplicaApplyOutcome.Applied:
                            _counters.Applied(_source.Name);
                            break;
                        case ReplicaApplyOutcome.Skipped:
                            _counters.Skipped(_source.Name);
                            break;
                        case ReplicaApplyOutcome.ConflictApplied:
                            _counters.Conflict(_source.Name);
                            _counters.Applied(_source.Name);
                            break;
                        case ReplicaApplyOutcome.ConflictSkipped:
                            _counters.Conflict(_source.Name);
                            _counters.Skipped(_source.Name);
                            break;
                    }
```

Здесь же ДЕМОНТИРОВАТЬ переходные catch-блоки Step 5.4 (`ReplicationChecksumMismatchException` и catch-all): мисматч и техотказы применения теперь приходят как `Error`-исход `apply.IsFailed` и маршрутизируются в `HandleFetchFailure` из Result-ветки; catch `OperationCanceledException when (ct.IsCancellationRequested)` — сохранить (rethrow).

ветка `Tombstone`: `_applier.ApplyDelete(...)` — `IsFailed → HandleFetchFailure`, иначе `_counters.Applied` + `false`.

2. `NotificationInbox.Handle`: `ApplyDelete`/`ApplyBucketCreate`/`ApplyBucketDelete` теперь `Result` — `IsFailed`-ветка: проброс `.Throw()` (мост t38, помечен комментарием): переходно техотказ применения (кворумные `Insufficient*` — достижимы из любой мутации драйва) ловит живой до Task 7 middleware catch → 503 + XML, как сегодня; замена на трансляцию — Task 7 (Step 7.3.5, ветка notifications). Механика 429/дедуп не меняется. **Inbox-предфильтр `HandleObjectPut` НЕ трогается**: конфликтный skip предфильтра остаётся `conflict` + `skipped` (как сегодня — семантика канона §10 сохраняется; предфильтр через `ReplicaApplyOutcome` не проходит, spec §4.4);
3. `ReconcileRunner`: `applier.ApplyBucketCreate(name, creationUnixMs)` → `if (r.IsFailed) logger.LogWarning(r.Error, …);` (проход не роняет — прецедент ретрай-механики §9);
4. `ReplicationAdminHandlers.HandleProbeAsync`:

```csharp
        var probe = await prober.ProbeAsync(string.IsNullOrEmpty(targetName) ? null : targetName,
            context.RequestAborted);
        if (probe.IsFailed && probe.Error is UnknownReplicationTargetException)
        {
            // Имя не из состава — 400 с пояснением (глава 07 §2)
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(
                new { error = $"unknown replication target: {targetName}" }, context.RequestAborted);
            return;
        }
        probe.Throw(); // прочие отказы — прежний путь (catch-all → 500)
        var report = probe.Value;
        // … прежняя сборка ProbeResponse (JSON-модель не меняется)
```

5. Мосты `// мост t38` из Task 4 в `ReplicationHandlers`/`ReplicationAdminHandlers` (fetch-грань/листинг/sync-report) и проброс inbox (п.2) в этой задаче НЕ демонтируются — переходно их ловит живой до Task 7 middleware catch (`ObjectStoreException` → S3-код по каталогу: `NoSuchKey` → 404, `Insufficient*Quorum` → 503, …), т.е. тела/коды ответов байт-в-байт как сегодня. Демонтаж ВСЕХ этих мостов — Task 7 (Step 7.3.5): явная запись тех же ответов через единый helper `S3ResultTranslator.WriteAsync` на КАЖДОЙ грани (кворумные отказы достижимы из каждой — BucketExists/ListBuckets/TryReadMeta/OpenData/мутации EC-драйва); «окончательных» пробросов в catch-all для стор-отказов НЕ остаётся.

- [ ] **Step 6.4: Тесты `ReplicaApplierTests`/`ReplicationProberTests`/`ObjectFetcherTests`**

- `ReplicaApplierTests`: ассерты `ReplicaApplyResult` → `Result<ReplicaApplyOutcome>` (`IsSuccess` + `Value.Should().Be(ReplicaApplyOutcome.Applied/Skipped/ConflictApplied/ConflictSkipped)` — по всем четырём исходам, кейсы обеих конфликтных ветвей: конфликтная победа incoming → `ConflictApplied`; конфликтный LocalWins → `ConflictSkipped`); кейс техотказа применения (фейк-стор с Failed) → `IsFailed` (замена `ThrowAsync<ReplicationChecksumMismatchException>` на `Error.Should().BeOfType<ReplicationChecksumMismatchException>()`);
- `ReplicationProberTests`: `null`-возврат → `IsFailed` + `Error.Should().BeOfType<UnknownReplicationTargetException>()`; успешный probe → `IsSuccess` + прежние ассерты отчёта; `FakeProbeTransport` — уже на новых сигнатурах (Task 5);
- `ObjectFetcherTests`: счётчики по исходам применения «как сегодня», байт-в-байт: `ConflictApplied → conflict И applied` (оба); `ConflictSkipped → conflict И skipped` (оба); `Applied → applied`; `Skipped → skipped`; при `Failed` применения (мисматч) `fetched` НЕ инкрементируется (как сегодня — счётчик стоит после успешного применения); `FetchChecksumMismatch_RequeuedNotApplied` — зелёный на новом пути (мисматч как `Error` применения, без catch-блоков).

- [ ] **Step 6.5: Прогоны**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests"`
Expected: PASS.

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS без правок HTTP-ожиданий (probe: неизвестное имя — по-прежнему 400; `MetricsReplicationScenarios` — applied_total как сегодня).

Зачистка серий — как в Step 2.8.

- [ ] **Step 6.6: Commit**

```bash
git add src/OwnS3.Replication/ src/OwnS3.App/Handlers/ src/tests/OwnS3.UnitTests/Replication/
git commit -m "t38: ReplicaApplier на Result: ApplyObjectAsync → Result<ReplicaApplyOutcome> (4 значения Applied/Skipped/ConflictApplied/ConflictSkipped — терминальные Success; пара Conflict* выражает флаг конфликта канона 07 §10 поверх исхода применения; двухбулевый ReplicaApplyResult демонтирован), ApplyDelete/ApplyBucketCreate/Delete → Result (BucketNotEmpty — внутренний RetryQueue как раньше); счётчики фетчера — семантика «как сегодня» байт-в-байт: ConflictApplied → conflict+applied, ConflictSkipped → conflict+skipped; демонтаж переходных catch-блоков фетчера (мисматч — Error-исход применения); inbox-предфильтр не меняется (conflict+skipped); ReplicationProber.ProbeAsync → Result<ProbeReport> (неизвестное имя — Failed(UnknownReplicationTargetException) → 400 хендлера); механика бэкоффов/ретраев без изменений; ProbeOutcome/JSON-модели ответов не менялись"
```

---

## Task 7: App-финал — транслятор исходов + демонтаж catch-веток `S3Middleware` (фаза 3.1–3.2 spec)

**Files:**
- Create: `src/OwnS3.App/Pipeline/S3ResultTranslator.cs`
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs`
- Modify: `src/OwnS3.App/Pipeline/RequestContext.cs` (поле `HostId`)
- Modify: `src/OwnS3.App/Handlers/OperationHandlerBase.cs` (helper записи ошибки)
- Modify: `src/OwnS3.App/Handlers/ObjectHandlers.cs`, `MultipartHandlers.cs`, `BucketHandlers.cs`, `ListHandlers.cs` (демонтаж Throw-мостов → Match)
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (демонтаж мостов Task 4/6: fetch/listing/notifications — явная запись ответов через helper; `ILogger` в ctor — см. Step 7.3.5)
- Modify: `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` (sync-report — сигнатуры `requestId`/`errorHostId`, helper)
- Modify: `src/OwnS3.App/Program.cs` (DI: `ILogger<ReplicationHandlers>` в ctor `ReplicationHandlers`)
- Modify: `src/OwnS3.Storage/ObjectStoreException.cs` (удаление `ObjectStoreUnavailableException`)
- Test: `src/tests/OwnS3.UnitTests/S3ResultTranslatorTests.cs` (новый)

**Interfaces:**
- Consumes: `Result`-сигнатуры `IObjectStore` (Task 3), `PeerRejectedException`/`UnknownReplicationTargetException` (Task 1).
- Produces:

```csharp
// src/OwnS3.App/Pipeline/S3ResultTranslator.cs
public static class S3ResultTranslator
{
    // Failed → S3Error: ObjectStoreException — по каталогу (перенос MapStorageCode
    // из S3Middleware, канон 03 §5 + коды главы 02); XlInvalidArgumentException — 400;
    // прочие Error — 500 InternalError. hostId — СЫРОЙ nullable OwnS3Options.HostId
    // (как middleware-ветки сегодня: null → пустой <HostId></HostId>; БЕЗ MachineName-фолбэка).
    public static S3Error Translate(Exception error, string resource, string requestId, string? hostId);

    // Запись ошибки в HTTP-ответ (статус по каталогу + XML-тело канона 03 §5;
    // HEAD — без тела; отмена записи — RequestAborted). Единый helper ВСЕХ граней:
    // S3-хендлеры (через WriteFailedAsync) и replication/admin-грани (Step 7.3.5) —
    // компенсация демонтированных middleware-веток, тело/код байт-в-байт как сегодня
    // (Resource — context.Request.Path, как в middleware-ветках).
    public static Task WriteAsync(HttpContext context, Exception error, string requestId, string? hostId);
}
```

и в `OperationHandlerBase`:

```csharp
    // Запись исхода Failed хендлера в HTTP (формат S3-ошибок — канон 03 §5; HEAD — без тела)
    protected static Task WriteFailedAsync(S3HandlerContext context, Result result);
```

- [ ] **Step 7.1: `S3ResultTranslator` — транслятор исходов**

```csharp
using Microsoft.AspNetCore.Http;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Pipeline;

// Трансляция исходов Result хендлеров в S3-ошибки (конвенция главы 01 §4; формат —
// канон 03 §5 + коды каталога главы 02): ObjectStoreException — доменный код,
// XlInvalidArgumentException — 400 InvalidArgument, прочие Error — 500 InternalError.
public static class S3ResultTranslator
{
    public static S3Error Translate(Exception error, string resource, string requestId, string? hostId) =>
        error switch
        {
            ObjectStoreException ex => new S3Error(MapStorageCode(ex.Code),
                Resource: resource, RequestId: requestId, HostId: hostId),
            XlInvalidArgumentException ex => new S3Error(S3ErrorCode.InvalidArgument,
                Resource: resource, RequestId: requestId, HostId: hostId,
                MessageOverride: ex.Message),
            _ => new S3Error(S3ErrorCode.InternalError,
                Resource: resource, RequestId: requestId, HostId: hostId),
        };

    // Запись ошибки в HTTP-ответ — единый helper всех граней (компенсация
    // демонтированных middleware-веток): статус по каталогу, XML-тело канона
    // 03 §5, Resource — Request.Path (как в middleware-ветках), HEAD — без тела;
    // hostId — сырой nullable (null → пустой <HostId></HostId>, как сегодня).
    public static async Task WriteAsync(HttpContext context, Exception error,
        string requestId, string? hostId)
    {
        var s3Error = Translate(error, context.Request.Path, requestId, hostId);
        context.Response.StatusCode = s3Error.Info.HttpStatus;
        if (HttpMethods.IsHead(context.Request.Method))
            return; // HEAD-ошибки — без тела (глава 03 §5)
        context.Response.ContentType = "application/xml";
        await context.Response.WriteAsync(S3ErrorXmlWriter.Write(s3Error), context.RequestAborted);
    }

    // Маппинг доменных кодов Storage в каталог S3-ошибок (имена синхронны) —
    // перенесён из S3Middleware без изменений.
    private static S3ErrorCode MapStorageCode(ObjectStoreErrorCode code) => code switch
    {
        ObjectStoreErrorCode.NoSuchBucket => S3ErrorCode.NoSuchBucket,
        ObjectStoreErrorCode.NoSuchKey => S3ErrorCode.NoSuchKey,
        ObjectStoreErrorCode.BucketAlreadyOwnedByYou => S3ErrorCode.BucketAlreadyOwnedByYou,
        ObjectStoreErrorCode.BucketNotEmpty => S3ErrorCode.BucketNotEmpty,
        ObjectStoreErrorCode.InvalidPart => S3ErrorCode.InvalidPart,
        ObjectStoreErrorCode.InvalidPartOrder => S3ErrorCode.InvalidPartOrder,
        ObjectStoreErrorCode.NoSuchUpload => S3ErrorCode.NoSuchUpload,
        ObjectStoreErrorCode.InvalidRange => S3ErrorCode.InvalidRange,
        ObjectStoreErrorCode.PreconditionFailed => S3ErrorCode.PreconditionFailed,
        ObjectStoreErrorCode.NotModified => S3ErrorCode.NotModified,
        ObjectStoreErrorCode.EntityTooLarge => S3ErrorCode.EntityTooLarge,
        ObjectStoreErrorCode.InsufficientReadQuorum => S3ErrorCode.InsufficientReadQuorum,
        ObjectStoreErrorCode.InsufficientWriteQuorum => S3ErrorCode.InsufficientWriteQuorum,
        _ => S3ErrorCode.InternalError,
    };
}
```

- [ ] **Step 7.2: `HostId` (nullable) в `RequestContext`, `Context` в `S3HandlerContext`, helper `WriteFailedAsync`**

1. В `src/OwnS3.App/Pipeline/RequestContext.cs` добавить поле (после `Model`):

```csharp
    // Сырой nullable OwnS3Options.HostId (как middleware-ветки писали в S3Error):
    // null → пустой <HostId></HostId> в XML-ошибке — байт-в-байт как сегодня;
    // MachineName-фолбэк — только originHostId репликации (DI, x-owns3-repl-origin).
    public required string? HostId { get; init; }
```

и заполнить в `S3Middleware.InvokeAsync` (контекст создаётся ЗДЕСЬ, строки ~268–274 — блок `await DispatchAsync(context, new RequestContext { … })`; НЕ в DispatchAsync): `HostId = options.Value.HostId` (минимальная правка; БЕЗ `?? Environment.MachineName` — иначе тело XML-ошибки меняется, принцип 2).

2. В `src/OwnS3.App/Handlers/OperationHandlerBase.cs` открыть HttpContext из контекста (сегодня публичен только `Http` типа `HttpRequest` — `_http` приватен):

```csharp
    // HttpContext хендлера — для WriteFailedAsync (S3ResultTranslator.WriteAsync
    // принимает HttpContext: Response + Request.Path + RequestAborted).
    public HttpContext Context => _http;
```

3. Helper записи исхода (там же):

```csharp
    // Запись исхода Failed хендлера в HTTP-ответ — делегация единому helper
    // транслятора (формат — канон 03 §5; HEAD — без тела; Resource — Request.Path,
    // как в демонтированных middleware-ветках, НЕ Model.RawPath: RawTarget несёт
    // путь «как прислал клиент», middleware писал частично декодированный Path —
    // тело XML-ошибки меняется для %-кодированных путей, байт-в-байт важнее).
    // Отмена записи — RequestAborted (тот же токен, что middleware передаёт хендлерам).
    protected static Task WriteFailedAsync(S3HandlerContext context, Result result)
        => S3ResultTranslator.WriteAsync(context.Context, result.Error!,
            context.Request.RequestId, context.Request.HostId);
```

- [ ] **Step 7.3: `S3Middleware` — демонтаж catch-веток стора**

1. Удалить catch-блоки `catch (XlInvalidArgumentException ex)`, `catch (ObjectStoreException ex)`, `catch (ObjectStoreUnavailableException)` (строки ~277–295) — их работу выполняет транслятор в хендлерах.
2. Удалить приватный `MapStorageCode` из S3Middleware (перенесён в транслятор) и `using`, ставшие неиспользуемыми.
3. Сохранить: `catch (S3ProtocolException ex)` (Protocol — вне области) и catch-all `Exception → 500` (страховка).
4. Удалить класс `ObjectStoreUnavailableException` из `src/OwnS3.Storage/ObjectStoreException.cs` (мёртвая заглушка t36 — тип никто не бросает; критерий 3). В комментарии шапки `ObjectStoreErrorCode` убрать упоминание служебного кода «для этого — ObjectStoreUnavailableException».
5. **Репликационные/admin-грани — компенсация демонтажа middleware-веток единым helper `S3ResultTranslator.WriteAsync` (Step 7.1).** Кворумные отказы (`Insufficient*Quorum` → 503 + XML, канон 06 §7 «503, не „бакета нет“») достижимы из КАЖДОЙ грани (ErasureXlDrive: BucketExists/ListBuckets/TryReadMeta/OpenData/мутации), поэтому ВСЕ мосты `Throw()` из Steps 4.4/6.3 демонтируются на явную запись тех же ответов (helper сам маппит код: `NoSuchKey` → 404, `Insufficient*` → 503, `XlInvalidArgument` → 400, прочие → 500 — эквивалент демонтированной пары «доменный catch + catch-all», тело/код байт-в-байт):

- `HandleObjectFetchAsync` (fetch-грань) — расширить сигнатуру `…(HttpContext, string bucket, string key, string requestId, string? errorHostId)` (вызов из replication-ветки middleware: `await replicationHandlers.HandleObjectFetchAsync(context, replPath.Bucket, replPath.Key, requestId, options.Value.HostId);` — там `requestId` локальная переменная, `options` доступен; `errorHostId` — СЫРОЙ nullable, эквивалент middleware-веток: null → пустой `<HostId></HostId>`; MachineName-фолбэк DI-параметра `hostId` — только заголовок `x-owns3-repl-origin` 200-ответов, НЕ ошибки):

```csharp
        var stateResult = store.ReadKeyState(bucket, key);
        if (stateResult.IsFailed)
            return await S3ResultTranslator.WriteAsync(context, stateResult.Error!, requestId, errorHostId);
        // … Found/Missing — прежняя логика (200-отдача / tombstone / пустой 404 — как сегодня)

        // внутри ветки Found:
        var opened = store.OpenObjectForReplication(bucket, key);
        if (opened.IsFailed)
            return await S3ResultTranslator.WriteAsync(context, opened.Error!, requestId, errorHostId);
        // NoSuchKey-гонка удаления → 404 + XML NoSuchKey; кворумные Insufficient* → 503 + XML;
        // прочие → 500 — всё как сегодня через middleware-ветки (канон 03 §5, принцип 2 spec)
```

- `HandleListingAsync` — расширить сигнатуру `…(HttpContext, string requestId, string? errorHostId)`: `store.ListBuckets()` и `store.ListReplicationEntries(...)`: `IsFailed → return await S3ResultTranslator.WriteAsync(context, …Error!, requestId, errorHostId);` (сегодня кворумный отказ листинга → middleware → 503 + XML; успех — прежние JSON-ответы);
- `ReplicationAdminHandlers.HandleSyncReportAsync` — расширить сигнатуру `…(HttpContext, string requestId, string? errorHostId)` (ctor НЕ менять): `ListReplicationEntries` `IsFailed → WriteAsync` (503 + XML как сегодня);
- `HandleNotificationsAsync` — расширить сигнатуру `…(HttpContext, string sourceName, string requestId, string? errorHostId)`: техотказы применения inbox (мост Step 6.3.2 — `Throw()` из `ApplyDelete`/`ApplyBucket*` при самодостаточных событиях; остаётся в NotificationInbox НАВСЕГДА — см. Замечания п.1) ловятся локально и транслируются тем же helper; доменные — без LogError (как сегодня middleware-ветки), не-доменные — с LogError (наблюдаемость catch-all). Для лога добавить `ILogger<ReplicationHandlers> logger` последним параметром primary-конструктора (DI в `Program.cs` — минимальная правка):

```csharp
        long? accepted;
        try
        {
            accepted = inbox.Handle(sourceName, batch);
        }
        catch (Exception ex) when (ex is ObjectStoreException or XlInvalidArgumentException)
        {
            // Доменный техотказ применения — трансляция как сегодня (middleware-ветка
            // без LogError): кворум → 503 + XML, длинный ключ → 400 + XML
            return await S3ResultTranslator.WriteAsync(context, ex, requestId, errorHostId);
        }
        catch (Exception ex)
        {
            // Не-доменный — LogError (эквивалент catch-all middleware: сегодня логируется)
            // + 500 InternalError с XML-телом (тот же исход)
            logger.LogError(ex, "Приём уведомлений от {Source} упал", sourceName);
            return await S3ResultTranslator.WriteAsync(context, ex, requestId, errorHostId);
        }
```

- демонтированные мосты пометить: удалить все `// мост t38` в `ReplicationHandlers.cs`/`ReplicationAdminHandlers.cs` (контроль — Замечания для исполнителей п.1);
- `AdminHandlers.HandleStorageAsync`/`HandleHealAsync` — стор-вызовов не делают (`ErasureStatusProvider`/`RequestPass` — вне мигрируемых сигнатур): правок не требуют.
- `requestId`/`errorHostId` для `HandleListingAsync`/`HandleNotificationsAsync`/`HandleSyncReportAsync` — передать параметрами из соответствующих веток `S3Middleware` по тому же образцу, что fetch-грань (в ветках `requestId` локальная переменная, `options.Value.HostId` доступен).

Run: `grep -rn "ObjectStoreUnavailableException" src/` → `0` совпадений.

- [ ] **Step 7.4: Демонтаж Throw-мостов в S3-хендлерах → Match (drain-семантика сохранена)**

Правило для каждого вызова стора в `ObjectHandlers`/`MultipartHandlers`/`BucketHandlers`/`ListHandlers`:

```csharp
// Общий вид (пример HeadObjectHandler):
// Act
var read = await Store.HeadObjectAsync(context.Bucket, context.Key, options, ct);
if (read.IsFailed)
    return await WriteFailedAsync(context, read);
var content = read.Value;
// … прежний Respond без изменений
```

- `PutObjectHandler` (drain-семантика, spec §4.5): отказ стора — теперь `Failed`, тело дочитывается до записи ответа:

```csharp
            // Act: отказ Storage (в т.ч. ДО чтения тела — NoSuchBucket) не должен
            // скрывать сверки целостности конвейера — тело дочитывается и при отказе
            // (drain-семантика t36: BadDigest/InvalidRequest/чанковая подпись
            // наблюдаются до записи ответа; S3ProtocolException из обёрток тела
            // перекрывает исход Storage — пробрасывается в S3Middleware)
            var put = await Store.PutObjectAsync(context.Bucket, context.Key, body, contentLength, metadata, ct);
            if (put.IsFailed)
            {
                await DrainBodyAsync(body, ct);
                return await WriteFailedAsync(context, put);
            }
            var result = put.Value;
            // … прежние событие репликации + Respond без изменений
```

- `GetObjectAttributesHandler`: `Failed → WriteFailedAsync`; `GetAttributesOutcome.NotModified → 304-ветка` / `Full → прежняя сборка GetObjectAttributesOutput` (без изменений);
- `BucketHandlers`/`ListHandlers`/`MultipartHandlers`: тот же паттерн (`BucketExistsAsync` → `Result<bool>`: `if (exists.IsFailed) WriteFailed; else if (!exists.Value) { прежняя 404-ветка } else { … }` — по фактическому коду хендлеров, HTTP-исходы те же; `ListObjectsAsync`-`Failed` (битый continuation-token) → `WriteFailedAsync` → 400 — как сегодня через catch-ветку);
- Служебные `EnsureObjectKey`/`UploadMetadata`/`ReadOptions` и пр. — не трогаются.

- [ ] **Step 7.5: Юнит-тесты транслятора — новый файл `src/tests/OwnS3.UnitTests/S3ResultTranslatorTests.cs`**

```csharp
using OwnS3.App.Pipeline;
using OwnS3.Protocol.Errors;
using OwnS3.Storage;

namespace OwnS3.UnitTests;

// Трансляция Result.Failed в S3-ошибки (конвенция главы 01 §4, каталог 03 §5).
public sealed class S3ResultTranslatorTests
{
    [Theory]
    [InlineData(ObjectStoreErrorCode.NoSuchBucket, S3ErrorCode.NoSuchBucket)]
    [InlineData(ObjectStoreErrorCode.NoSuchKey, S3ErrorCode.NoSuchKey)]
    [InlineData(ObjectStoreErrorCode.BucketNotEmpty, S3ErrorCode.BucketNotEmpty)]
    [InlineData(ObjectStoreErrorCode.InvalidRange, S3ErrorCode.InvalidRange)]
    [InlineData(ObjectStoreErrorCode.InsufficientReadQuorum, S3ErrorCode.InsufficientReadQuorum)]
    [InlineData(ObjectStoreErrorCode.PreconditionFailed, S3ErrorCode.PreconditionFailed)]
    public void Translate_ОшибкиКаталога_S3КодПоИмени(ObjectStoreErrorCode code, S3ErrorCode expected)
    {
        // Act
        var error = S3ResultTranslator.Translate(new ObjectStoreException(code), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(expected);
        error.Info.HttpStatus.Should().BePositive();
    }

    [Fact]
    public void Translate_XlInvalidArgument_400InvalidArgument()
    {
        // Act
        var error = S3ResultTranslator.Translate(new XlInvalidArgumentException("лимит сегмента"), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(S3ErrorCode.InvalidArgument);
        error.Info.HttpStatus.Should().Be(400);
    }

    [Fact]
    public void Translate_ПрочаяОшибка_500InternalError()
    {
        // Act
        var error = S3ResultTranslator.Translate(new InvalidOperationException("x"), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(S3ErrorCode.InternalError);
        error.Info.HttpStatus.Should().Be(500);
    }
}
```

(Члены подтверждены: `S3Error.Code`/`S3Error.Info.HttpStatus` — `src/OwnS3.Protocol/Xml/S3Error.cs`; `S3ErrorCode` — enum `src/OwnS3.Protocol/Errors/S3ErrorCode.cs`. Тест использует `error.Code.Should().Be(expected)` — сравнение enum.)

- [ ] **Step 7.6: Прогоны (регресс контракта — критерий 4)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests"`
Expected: PASS.

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS — все Api-сценарии (Access/Auth/BodyIntegrity/Erasure*/ErrorFormat/FailFast/Multipart/ObjectStorage/Routing/Replication*) БЕЗ правок HTTP-ожиданий. Особо: `ErrorFormatScenarios` (формат S3-ошибок: 400 InvalidArgument битого continuation-token, 416 InvalidRange — теперь через транслятор), `FailFastScenarios` (drain), `ReplicationAdminScenarios` (probe 400).

Зачистка серий — как в Step 2.8.

- [ ] **Step 7.7: Commit**

```bash
git add src/OwnS3.App/ src/OwnS3.Storage/ObjectStoreException.cs src/tests/OwnS3.UnitTests/S3ResultTranslatorTests.cs
git commit -m "t38: App-финал Result-миграции: S3ResultTranslator (Failed → S3Error по каталогу 03 §5: ObjectStoreException — доменный код, XlInvalidArgumentException — 400, прочие — 500; MapStorageCode перенесён из S3Middleware) + WriteAsync — единый helper записи ошибок всех граней (статус+XML, Resource из Request.Path, HEAD без тела, hostId — сырой nullable как middleware) + Context в S3HandlerContext + WriteFailedAsync хендлеров (делегация helper'у; RequestContext.HostId — string?); S3Middleware — демонтаж catch-веток ObjectStoreException/XlInvalidArgumentException/ObjectStoreUnavailableException (сохранены S3ProtocolException и catch-all → 500); мёртвая заглушка ObjectStoreUnavailableException удалена; S3-хендлеры — Match вместо мостов, drain-семантика PutObject сохранена; репликационные/admin-грани — демонтаж мостов на WriteAsync с requestId+errorHostId из middleware (fetch: NoSuchKey-гонка → 404+XML, кворумные Insufficient* → 503+XML; listing/sync-report — тот же helper; notifications — try/catch inbox: доменные без лога, прочие с LogError; ILogger в ctor ReplicationHandlers); юнит-тесты транслятора; HTTP-контракт не изменился (регресс Api-сценариев: 400/416/503 через транслятор)"
```

---

## Task 8: Полные прогоны и зачистка серий (фаза 4 spec; мерж-гейт)

**Files:** изменений нет — только прогоны и зачистка. Если прогон вскрывает дефект — правка отдельным коммитом с повтором задетой серии.

- [ ] **Step 8.1: Полные юниты**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests"`
Expected: PASS — вся сборка (критерий 6).

Зачистка после серии: `docker ps -a --format '{{.Names}}' | grep -c os3` → `0`; `docker network ls --format '{{.Name}}' | grep -cE 'os3|owns3'` → `0`.

- [ ] **Step 8.2: Полные интеграции (Debug)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests"`
Expected: PASS; E2e-классы без `PGW_TEST_DOCKER=1` — Skip (норма).

Зачистка — как в Step 8.1.

- [ ] **Step 8.3: Полная docker-E2E серия OwnS3 на свежем Release (мерж-гейт — критерий 7)**

E2eFixture соберёт Release-образ сам (инкрементально — секунды; телеметрия `[PHASE]` по канонам `docs/e2e-launch.md`):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e"
```

Expected: PASS — вся E2e-серия (AwsSdk*, Mc*, Erasure, HealthMetrics, Tls*, Replication/* — включая Probe/Convergence/Membership). Изоляция: guid-контур на сценарий, полный teardown + ассерты чистоты — уже в фикстурах; после финальной строки прогона — контрольный grep (Step 8.1) и зачистка при остатках (own-only `docker network rm` / при массовом скоплении от прерванных прогонов — `docker network prune -f`).

- [ ] **Step 8.4: Контрольные проверки критериев приёмки**

Run (демонтированные типы отсутствуют — критерии 2/3): `grep -rn "ObjectStoreUnavailableException" src/ --include="*.cs" | wc -l` → `0`; `grep -rn "\bFetchResult\b\|\bObjectAttributesResult\b\|\bReplicaApplyResult\b" src/ --include="*.cs" | wc -l` → `0`.

Run (летящие доменные исключения Storage — только XlPathEncoder с конвертацией на границе — критерии 2/4): `grep -rn "throw new ObjectStoreException\|throw new XlInvalidArgumentException" src/OwnS3.Storage/ --include="*.cs" | grep -v "XlPathEncoder.cs" | wc -l` → `0`.

Run (catch-ветки middleware — критерий 3): `grep -n "catch (ObjectStoreException\|catch (XlInvalidArgumentException\|catch (ObjectStoreUnavailableException" src/OwnS3.App/Pipeline/S3Middleware.cs` → пусто; `grep -n "catch (S3ProtocolException" src/OwnS3.App/Pipeline/S3Middleware.cs` → `1`.

Run (fail-fast старта сохранён): `grep -n "Initialize().Throw()" src/OwnS3.App/Program.cs` → `1` (отказ тома по-прежнему бросает в стартовый catch → LogCritical + `Environment.Exit(1)` — поведение «невалидный конфиг/том = диагностика + ненулевой exit» байт-в-байт).

Run (arch-first — критерий 1; merge-base от main, т.к. `git log --reverse` на ветке с унаследованной историей вернул бы древнейшие коммиты репозитория):

```bash
first=$(git log --format=%H --reverse $(git merge-base HEAD main)..HEAD | head -1)
git show --stat "$first"
```

Expected: первый коммит задачной ветки меняет ТОЛЬКО `arch/owns3/*` (Task 0).

- [ ] **Step 8.5: Итоговое резюме фазы execute**

Сформировать (в ответе агента, не отдельным файлом) таблицу: критерии приёмки 1–7 (spec §7) → чем закрыты (Task/Step/прогон). Мерж-гейт (Фаза 8 dev-flow): по явному приказу «мерж» — мерж в `main` + пуш; **мерж-коммит удаляет roadmap-запись `t38-owns3-result-migration` из `arch/roadmap/owns3.md`** (канон AGENTS.md: из списков и из `←`-зависимостей; сам, без вопросов).

---

## Соответствие план ↔ spec (self-review)

| Требование spec | Чем закрывается |
|---|---|
| §4.7 arch-правки (01 §3/§4, 06 §8, 07 §11, 05 — по факту) | Task 0 (Steps 0.1–0.5), коммит раньше кода (критерий 1; проверка — merge-base, Step 8.4) |
| §4.6 Shared.Core во все 4 csproj | Task 1 Step 1.1 |
| §4.1 общая семантика (Success-терминалы / Failed-техотказы) | Все задачи; контрольные кейсы — Steps 2.7, 3.5, 4.5, 5.8, 6.4 (критерий 5) |
| Аудит тестов компиляции — ПОЛНЫЙ (grep каждого переводимого члена Steps 2.1/3.1/4.1/5.1/6.1 + демонтируемых типов по ВСЕМУ src/tests: OwnS3.UnitTests + OwnS3.IntegrationTests + соседние сборки) | Task 2 — 8 файлов (Step 2.7 пофайлово, вкл. ErasureHealTests/ErasureCleanupTests); Task 3 — XlObjectStore*Tests (5) + CommitResultEventMetadataTests (7 вызовов фикстурного Store) + RangeResolverTests (Step 3.5); Task 4 — ReplicaStoreTests (value/out-параметр/ассерты) + NotificationInboxTests (~138) + ReconcileRunnerTests (~192, фикстурный ReplicaStore) (Step 4.5); Task 5 — 4 фейка + SigV4SignerTests (единственное покрытие реального ReplicationHttpClient: FetchOutcome-варианты, 429 → Failed(PeerRejected) — критерий 5; Steps 5.7–5.8); Task 6 — ReplicaApplierTests/Prober/Fetcher (Step 6.4). Вне миграции (проверено): VolumeCleanupServiceTests/XlVolumeTests/MultipartJournalsTests/MultipartCleanupTests (XlVolume/MultipartJournals-статика); OwnS3.IntegrationTests (Api + E2e) — все S3-вызовы через IAmazonS3/mc/HTTP (совпадения имён PutObjectAsync и пр. — НЕ OwnS3.Storage), единственное упоминание мигрируемых типов — комментарий в E2e/Replication/ConvergenceScenarios.cs:220; PgWorker/S3Worker/AdminPanel/прочие сборки — OwnS3NodeClient/BackupS3 и чужие PingAsync/ProbeAsync/ListAsync (вне задачи, spec §6) |
| §4.2 `IObjectStore` (21 операция) | Task 3 (Step 3.1) |
| §4.2 `IXlDrive` (TryReadMeta→Lookup, мутации→Result, BucketExists→`Result<bool>` и EnumerateObjects→`Result<IXlObjectCursor>` — EC-кворум → Failed(Insufficient*); CheckHealth/UploadDirExists — bool, бросков нет, сверено; CreateStaging — XlPathEncoder-бросок конвертируется на границе; курсорные XlIntegrityException — вне перечня) | Task 2 (Steps 2.1–2.2, аудит не-Result членов — Step 2.1) |
| §4.2 `IReplicaStore` (7 сигнатур + маркер в значении) | Task 4 (Step 4.1) |
| §4.2 `BucketMetaStore` | Перевода НЕ требует: не реализует мигрируемые интерфейсы, доменных бросков нет (мета `bucket.json` поверх `XlVolume`); используется внутри `SingleXlDrive` — покрывается переводом Task 2 |
| §4.2 `XlDiskJanitor` (чистки) | Перевода НЕ требует: статические вызовы `MultipartJournals.*` поверх корней тома, мигрируемых сигнатур не вызывает (проверено grep); вызывается реализациями драйвов внутри их Result-методов |
| Аудит statement-потреблений прод-кода (после void→Result statement-вызов компилируется, но отказ молча проглатывается) | Греп всех statement-вызовов переводимых void-Result членов по `src/OwnS3.*` вне Files задач: `Program.cs:405 Initialize()` — fail-fast, мост Step 2.6.2 (постоянный); хендлерные statement-операции (`ObjectHandlers:~168`, `BucketHandlers:~61/~81`, `MultipartHandlers:~116`) — обязательный мост Step 3.4 (демонтаж Task 7 → WriteFailedAsync); inbox-применения — Result-ветки Step 6.3.2; фоновые циклы (VolumeCleanupService/ErasureHeal/Coordinator) — Result-потребление Steps 2.5–2.6 (Coordinator стор напрямую не зовёт — конструкторы неизменны). Других statement-потреблений с поведением-по-исключению НЕТ |
| §4.2 демонтаж `ObjectAttributesResult`; `PutResult`/`CompleteResult`/`DeletedKeyResult` — значения | Steps 3.1, 3.3 |
| §4.2 «Реализации … переводятся вместе с интерфейсами» (все бросающие места Storage — без остаточных летящих исключений после демонтажа catch) | Steps 3.2 (RangeResolver/ListWalker/XlPathEncoder-граница), 4.1 (ReplicaStore), контрольные grep — Steps 3.2/8.4 |
| §4.3 транспорт: `Result<long>`, `FetchOutcome`, `Result<PingResult>` (RttMs не-null), листинги | Task 5 (Steps 5.1–5.2) |
| §4.3 `PeerRejectedException`/`UnknownReplicationTargetException` | Task 1 (Step 1.4); потребители — Tasks 5–6 |
| §4.3 Dispatcher/Fetcher/Prober — потребление исходов | Steps 5.3, 5.4, 5.6, 6.2 |
| §4.4 `ReplicaApplier` → `Result<ReplicaApplyOutcome>` — 4 значения `{ Applied, Skipped, ConflictApplied, ConflictSkipped }`; демонтаж `ReplicaApplyResult`; счётчики «как сегодня» байт-в-байт (пара Conflict* — флаг конфликта канона 07 §10 поверх исхода применения; ConflictApplied → conflict+applied, ConflictSkipped → conflict+skipped) | Task 6 (Step 1.5 — enum; Steps 6.1, 6.3.1: switch из 4 case; inbox-предфильтр не меняется — conflict+skipped) |
| §4.5 транслятор + демонтаж catch-веток + удаление `ObjectStoreUnavailableException`; репликационные/admin-грани — компенсация единым helper `S3ResultTranslator.WriteAsync`: ВСЕ стор-отказы граней (fetch: stateResult/opened; listing; sync-report; notifications) — статус+XML по каталогу байт-в-байт (NoSuchKey → 404, кворумные Insufficient* → 503, XlInvalidArgument → 400, прочие → 500); «окончательных» пробросов стор-отказов в catch-all не остаётся; WriteFailedAsync — Resource из Request.Path (не RawPath) | Task 7 (Steps 7.1–7.3, все грани — Step 7.3.5) |
| §4.5 drain-семантика PutObject | Steps 3.4 (мост) → 7.4 (финал) |
| §4.5 хендлеры/replication-хендлеры/hosted-сервисы — новые сигнатуры, логика не меняется | Tasks 3–7; VolumeCleanupService — Step 2.6; Coordinator — конструкторы неизменны |
| §5 фаза 4: юниты → интеграции → docker-E2E Release + зачистка серий | Task 8 |
| §6 ограничения (Protocol вне области; OCE — не исход; TreatWarningsAsErrors; multi-host) | Global Constraints + правила Steps 2.2.5, 5.1.1 |
| §7 критерии 1–7 | Task 8.4 (контрольные команды) + таблица выше |

## Замечания для исполнителей

1. **Мосты `Throw()`** — переходная идиома Tasks 2–6 для сохранения компилируемости/поведения по срезам; каждый помечен `// мост t38`. После Task 7 мосты демонтированы ВЕЗДЕ, КРОМЕ осознанных ПОСТОЯННЫХ: `NotificationInbox.cs` (пробросы применения/предфильтра — компенсация try/catch `HandleNotificationsAsync` + helper, Step 7.3.5) и `Program.cs` (fail-fast `Initialize().Throw()` — Step 2.6.2: бросок в стартовый catch с `Environment.Exit(1)` — легитимная исключительная точка). Контроль после Task 7: `grep -rn "мост t38" src/ --include="*.cs" | grep -v "NotificationInbox.cs" | grep -v "Program.cs"` → пусто. Переходные catch-блоки `ObjectFetcher` (Step 5.4) — мосты ровно до Step 6.3.1. ДВЕ идиомы: (а) значения — ТОЛЬКО `.Throw().Value`: `Result<T>.Throw()` возвращает `Result<T>` (не `T`); `var x = r.Throw();` без `.Value` даёт `x` тип `Result<T>` и при кортеж-распаковке срабатывает `Deconstruct` — тихий брак; (б) Task-сигнатуры — extension `Throw()` определён в `Shared.Core` ТОЛЬКО для `ValueTask<Result*>` (для Task — CS1061): async-мост — ТОЛЬКО скобочная форма `(await …).Throw()` / `(await …).Throw().Value`; `return x.Throw();` в async-Task-методе — CS1997 (statement-throw + `return;`). (в) statement-потребления: после смены void→Result statement-вызов компилируется, но отказ МОЛЧА проглатывается — каждое прод-место, где поведение опирается на исключение (fail-fast/retry-по-catch/фоновые циклы), обязано иметь явный мост или Result-ветку (аудит — self-review «Аудит statement-потреблений»).
2. **`Result.FromAsync` вслепую НЕ применять** — глотает `OperationCanceledException`; там, где возможна внешняя отмена, — явный catch-фильтр `when (ct.IsCancellationRequested) → throw` (паттерн Step 5.1.1).
3. **Стиль вариантных записей** — `abstract record` + `sealed`-наследники (прецедент `UploadVisibility`); паттерн-матчинг `is Type Variant v` / `switch` — единообразно в рамках файла.
4. **Комментарии при переводах** — сохраняются и переезжают к возвратам `Failed`/`Success` (история обоснований не теряется).
5. **Летящие доменные исключения в Storage после Task 4 — только `XlPathEncoder.EncodeSegments`** (чистая функция кодирования; конвертация в `Failed` на границе слоя операции — Steps 3.2.6, 4.1). Всё остальное — `Result.Error`; контрольный grep — Steps 3.2/8.4.
6. **Счётчики исходов репликации не менять**: `ReplicaApplyOutcome` — 4 значения; инкременты фетчера байт-в-байт как сегодня (`ConflictApplied → conflict И applied`, `ConflictSkipped → conflict И skipped`); конфликтный skip inbox-предфильтра — `conflict` + `skipped`; канон 07 §10 не меняется.
7. После КАЖДОЙ тестовой серии (шаги «Прогоны») — дожидаться финальной строки и зачищать контуры (правила AGENTS.md: зачистка серий, осиротевшие сети).
