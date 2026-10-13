# t38-owns3-result-migration — унификация исходов операций ownS3 на Result-парадигму

Спецификация задачи dev-flow. Постановка — тег `t38-owns3-result-migration`
в [`arch/roadmap/owns3.md`](../../../arch/roadmap/owns3.md). Задача чисто
внутренняя: HTTP-контракт S3-API, admin/replication-грани и дисковые форматы
(`xl.meta`, `.owns3.sys`) не меняются.

## 1. Цель

Проекты `src/OwnS3.*` сегодня не ссылаются на `Shared.Core` и живут на
собственной конвенции исходов операций:

- специализированные DTO транспорта/стора, дискриминируемые по месту:
  `FetchResult` (транспорт), `ObjectAttributesResult`,
  `DeletedKeyResult`, `ReplicaApplyResult` (стор/репликация);
- nullable-маркеры дискретных вариантов: `SendNotificationsAsync →
  Task<long?>` (null = 429/отказ → бэкофф), `ProbeAsync →
  Task<ProbeReport?>` (null = «имя не из состава» → 400),
  `TryReadMeta → XlMetaRecord?` / `ReadKeyState → ReplicaKeyState?`
  (null = «записи нет»);
- ошибки S3-операций стора ходят исключениями `ObjectStoreException` /
  `XlInvalidArgumentException` / `ObjectStoreUnavailableException`,
  которые ловит `S3Middleware`.

Задача — привести внутренние исходы OwnS3 к Result-парадигме монорепо
(`Result`/`Result<T>` из `src/Shared.Core/Result.cs`), как в воркерах
(прецеденты: `src/PgWorker.Backups/BackupS3.cs` — интерфейс S3-клиента
целиком на `Task<Result>/Task<Result<T>>`;
`src/S3Worker.Core/OwnS3/OwnS3NodeClient.cs`):
подключить `Shared.Core` ко всем четырём проектам OwnS3 и перевести на
`Result` все внутренние сигнатуры: транспорт репликации, стор
(`IObjectStore`, `IXlDrive`, `IReplicaStore`), пайплайн/хендлеры App.

## 2. Принципы

1. **Arch-first**: конвенция исходов сначала фиксируется в `arch/owns3/`
   (фаза 0), затем код зеркалит её. Источник истины один.
2. **Внешние контракты неизменны**: 21 операция S3-API, replication-грани,
   admin-грани, JSON-модели ответов, дисковые форматы — байт-в-байт как
   сегодня; миграция не меняет ни одного HTTP-кода, тела или заголовка.
   Интеграционные/E2E-сценарии проходят без правок ожиданий.
3. **Единая семантика бинарного Result** (решение user-review, согласовано):
   - **Success несёт терминальный исход, который удалось узнать**, включая
     «не-ошибочные» терминальные варианты: Not-Modified (304),
     отсутствие записи (`TryReadMeta`/`ReadKeyState`), tombstone и 404
     соседа у фетча, `reachable=false` у ping-пробы с полученным HTTP-ответом.
     Прецедент монорепо: `Result<bool> HealthAsync` — «узнали ответ» =
     Success, «не смогли узнать» = Failed.
   - **Failed — только технический отказ**: транспорт не ответил
     (обрыв/таймаут/429 «не приняли»), кворумная недоступность EC,
     ошибки S3-операций (`ObjectStoreException`: `NoSuchKey`,
     `NoSuchBucket`, `BucketNotEmpty`, `PreconditionFailed`,
     `InvalidRange`, …), `XlInvalidArgumentException`,
     checksum-mismatch фетча.
4. **Полный перевод** (решение user-review): все внутренние интерфейсы —
   `IObjectStore`, `IReplicationTransport`, `IXlDrive`, `IReplicaStore`,
   `ReplicaApplier`, `ReplicationProber` — и все хендлеры App. Ошибки
   стора перестают быть летящими исключениями: возвращаются в
   `Result.Error`; маппинг в S3-ошибки (канон главы 03 §5, каталог глав 02)
   выполняет App при трансляции исхода в HTTP-ответ.
5. **Shared.Core — во все четыре проекта** (решение user-review):
   `ProjectReference` на `Shared.Core` добавляется в `OwnS3.Protocol`,
   `OwnS3.Storage`, `OwnS3.Replication`, `OwnS3.App` — по букве roadmap,
   даже если Protocol не получит Result-сигнатур (его исключения
   `S3ProtocolException` — область протокола, вне перечисленной зоны
   «транспорт репликации, стор, пайплайн/хендлеры»; они остаются
   исключениями).
6. **Слои кодифицируют свою семантику**: низкий слой сообщает «как есть»
   (отсутствие записи — `Success(Missing)`), слой S3-операции
   (`XlObjectStore`) превращает его в ошибку операции
   (`Failed(ObjectStoreException(NoSuchKey))`) — терминальность
   интерпретируется на уровне, которому она принадлежит.

## 3. Решения (согласованы с пользователем)

| Развилка | Решение |
|---|---|
| Охват миграции стора | **Полный перевод**: все внутренние интерфейсы и хендлеры; ошибки операций — в `Result.Error`, маппинг в S3-ошибки — в App |
| Терминальные «не-ошибочные» исходы (404 фетча, tombstone, 304, «записи нет») | **В Success** вариантами значения; Failed — только технические отказы (обрыв, кворум, 429, ошибки S3-операций) |
| Подключение Shared.Core | **Во все четыре проекта** OwnS3.*, включая Protocol (ссылка без появления Result-сигнатур в Protocol) |
| Arch-правки | **Точечно по факту**: центральное описание конвенции — глава 01 (§3 границы зависимостей, §4 ключевое решение); строки в 06 §8 и 07 §11 там, где главы говорят об исходах внутренних интерфейсов; в 05 — только если найдётся фактическое место, правка «под набор» не делается |
| `PutResult`/`CompleteResult`/`DeletedKeyResult` | Остаются доменными типами-значениями успеха (прецедент `Result<OwnS3NodeUsage>`); «унификация» = сигнатуры становятся `Task<Result<PutResult>>` и т.п.; дискриминаций в них нет — `PutResult` несёт только данные успеха, per-key состав `DeletedKeyResult` — часть контракта XML-ответа DeleteObjects (глава 02) |
| `ObjectContent` | Не меняется по форме: `NotModified`/`Range` — семантика чтения из главы 02 (conditional/Range-запросы), а не «конвенция исходов» (в перечне roadmap отсутствует); сигнатура — `Task<Result<ObjectContent>>` |
| `ReplicaApplyOutcome` | **Четырёхзначный enum** `{ Applied, Skipped, ConflictApplied, ConflictSkipped }` (решение user-review по замечанию ревью): пара `ConflictApplied`/`ConflictSkipped` выражает флаг конфликта канона 07 §10 поверх исхода применения; глава 07 §10 не меняется |

## 4. Структура/компоненты

### 4.1. Общая семантика исходов

Внутренняя операция OwnS3 возвращает `Result` / `Result<T>`:

- **успех/терминальный исход** — `Result.Success()/Result<T>.Success(value)`;
  терминальные варианты одного исхода (объект|tombstone|404; полные
  атрибуты|Not-Modified; запись есть|записи нет) — вариантные записи
  (abstract record с sealed-вариантами; прецедент кодовой базы —
  `UploadVisibility.All/Owned`);
- **отказ** — `Result.Failed(error)`: `ObjectStoreException` (с кодом для
  каталога S3-ошибок), `XlInvalidArgumentException`,
  `ReplicationChecksumMismatchException` и новые типизированные ошибки
  транспорта (ниже). Носитель кода ошибки остаётся исключительным типом,
  но перестаёт лететь: кладётся в `Error`.

### 4.2. Стор: `src/OwnS3.Storage`

| Сигнатура (`IObjectStore`) | Стало |
|---|---|
| `CreateBucketAsync` / `DeleteBucketAsync` / `DeleteObjectAsync` / `AbortMultipartUploadAsync` | `Task<Result>` (ошибки каталога главы 02 — `Failed(ObjectStoreException)`) |
| `BucketExistsAsync` | `Task<Result<bool>>` |
| `ListBucketsAsync` / `ListObjectsAsync` / `ListPartsAsync` / `ListMultipartUploadsAsync` | `Task<Result<…>>` (страницы — значения) |
| `PutObjectAsync` / `CopyObjectAsync` / `UploadPartAsync` | `Task<Result<PutResult>>` |
| `CompleteMultipartUploadAsync` | `Task<Result<CompleteResult>>` |
| `GetObjectAsync` / `HeadObjectAsync` | `Task<Result<ObjectContent>>` |
| `GetObjectAttributesAsync` | `Task<Result<GetAttributesOutcome>>` — вариантная запись: `NotModified(ObjectMetadata)` \| `Full(ObjectAttributes)`; DTO `ObjectAttributesResult` демонтируется |
| `DeleteObjectsAsync` | `Task<Result<IReadOnlyList<DeletedKeyResult>>>` — отказ всей операции → `Failed`; per-key состав — значение |
| `CreateMultipartUploadAsync` | `Task<Result<string>>` |

`IXlDrive` (синхронный контракт «где и как лежат байты и меты»; реализации
`SingleXlDrive`/`ErasureXlDrive`):

- void-методы жизненного цикла (`Initialize`, `RunCleanupAsync`,
  бакетные/объектные мутации, multipart-журналы, `TrashUpload`, …) —
  `Result`/`Task<Result>`; технические отказы диска (IO, кворум EC) —
  `Failed`;
- `TryReadMeta(string, string)` → `Result<XlMetaLookup>` — вариантная
  запись `Found(XlMetaRecord Record)` \| `Missing()`; nullable-маркер
  устранён (принцип 3: отсутствие записи — терминальный Success);
- `BucketExists`/`CheckHealth`/`UploadDirExists` (bool-ответы) — остаются
  bool-значениями там, где отвечают «проверили и знаем»; методы, чей отказ
  возможен до ответа, — `Result<bool>` (финализация по факту кода — план);
- `Commit` → `Result<PutResult>`, `CommitMultipart` → `Result<CompleteResult>`.

`IReplicaStore` (применение/чтение репликации):

- `ApplyObjectAsync`/`DeleteObject`/`ApplyBucketCreate`/`ApplyBucketDelete` —
  `Task<Result>`/`Result`; `ReplicationChecksumMismatchException` и
  `BucketNotEmpty` — `Failed`;
- `ReadKeyState` → `Result<ReplicaKeyLookup>` — `Found(ReplicaKeyState)` \|
  `Missing()`;
- `ListReplicationEntries` → `Result<ReplicationListingPage>` (маркер
  `NextMarker` уходит из out-параметра в состав значения);
- `OpenObjectForReplication` → `Result<(ObjectMetadata Metadata, Stream Body)>`;
- `ListBuckets` → `Result<IReadOnlyList<BucketEntry>>`.

Реализации (`XlObjectStore.*`, `ReplicaStore`, `SingleXlDrive`,
`ErasureXlDrive`, `BucketMetaStore`, чистки) переводятся вместе с
интерфейсами: бросание `ObjectStoreException` внутри стора заменяется
возвратом `Failed` (внутренние исключения гасятся на месте своего
возникновения либо пробрасываются в `Error` через `Result.From`/`FromAsync`).

### 4.3. Транспорт репликации: `src/OwnS3.Replication`

| Сигнатура (`IReplicationTransport`) | Стало |
|---|---|
| `SendNotificationsAsync` | `Task<Result<long>>`: 200 → `Success(acceptedToSeqNo)`; 429/HTTP-отказ/обрыв → `Failed(new PeerRejectedException(...))`; nullable-маркер `long?` устранён |
| `FetchObjectAsync` | `Task<Result<FetchOutcome>>` — вариантная запись: `Fetched(Stream Body, long ContentLength, long ModTimeUnixMs, string ContentSha256, string ETag, string ContentType, IReadOnlyDictionary<string,string> UserMetadata)` \| `Tombstone(long DeleteTimeUnixMs)` \| `KeyNotFound()`; DTO `FetchResult` демонтируется; транспортный отказ → `Failed` |
| `ListAsync` / `ListingBucketsAsync` | `Task<Result<ReplicationListingResponse>>` / `Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>>` |
| `PingAsync` | `Task<Result<PingResult>>`: любой полученный HTTP-ответ → `Success(PingResult(bool Reachable, long RttMs))` — `RttMs` перестаёт быть nullable (нет ответа — нет Success); обрыв/таймаут → `Failed`; внешняя отмена ct — проброс, как сегодня |

Новые типизированные ошибки (в `OwnS3.Replication`): `PeerRejectedException`
(сосед не принял: 429/не-200/обрыв), `UnknownReplicationTargetException`
(probe: имя не из состава). Потребители:

- `NotificationDispatcher`: `Success(ack)` → watermark/`Online=true`;
  `Failed` → `Online=false`, бэкофф (механика главы 07 §8 не меняется);
- `ObjectFetcher`: `Success(Fetched)` → применение; `Success(Tombstone)` →
  `ApplyDelete`; `Success(KeyNotFound)` → skip без повтора;
  `Failed` (в т.ч. checksum-mismatch по типу `Error`) → повтор по бэкоффу
  (глава 07 §7);
- `ReplicationProber.ProbeAsync` → `Task<Result<ProbeReport>>`:
  неизвестное имя → `Failed(UnknownReplicationTargetException)` → хендлер
  400; `ProbeOutcome` (JSON-модель ответа admin-грани) не меняется —
  `rttMs` остаётся nullable-полем JSON (отсутствует при `Failed`).

### 4.4. LWW-слой: `ReplicaApplier`

`ApplyObjectAsync` → `Task<Result<ReplicaApplyOutcome>>` — enum
`{ Applied, Skipped, ConflictApplied, ConflictSkipped }` (все четыре —
терминальные Success-исходы); `ApplyDelete` → `Result`. Двухбулевый
`ReplicaApplyResult(Applied, Conflict)` демонтируется. Технический отказ
применения → `Failed`.

Счётчики `fetched/applied/skipped/conflict` (глава 07 §10) инкрементируются
фетчером по исходу — семантика «как сегодня», байт-в-байт:
`Applied` → applied; `Skipped` → skipped; `ConflictApplied` →
conflict И applied; `ConflictSkipped` → conflict И skipped. Пара
`ConflictApplied`/`ConflictSkipped` выражает флаг конфликта канона 07 §10
поверх исхода применения: у фетчера конфликт сосуществует с applied/skipped,
и четырёхзначный enum кодирует ровно то, что сегодня несут два bool
`(Applied, Conflict)`; глава 07 §10 при этом НЕ меняется. Inbox-предфильтр
(`NotificationInbox.HandleObjectPut`) через `ReplicaApplyOutcome` не
проходит и остаётся на conflict + skipped, как сегодня.

### 4.5. Пайплайн/хендлеры: `src/OwnS3.App`

- **Трансляция исходов**: единый helper App (например,
  `S3ResultTranslator`) — `Result`/`Result<T>` хендлера → HTTP: `Failed`
  с `ObjectStoreException` → S3-ошибка по каталогу (глава 03 §5 +
  коды глав 02), `XlInvalidArgumentException` → 400, прочие `Error` →
  500 `InternalError`. Формат S3-ошибок — канон 03 §5, без изменений.
- **`S3Middleware`**: ветки catch `ObjectStoreException` /
  `XlInvalidArgumentException` / `ObjectStoreUnavailableException`
  демонтируются; остаются catch `S3ProtocolException` (Protocol — вне
  области) и catch-all `Exception` → 500 как страховка. Мёртвая заглушка
  `ObjectStoreUnavailableException` (t36: тип никто не бросает) удаляется.
- **Хендлеры** (`ObjectHandlers`, `MultipartHandlers`, `BucketHandlers`,
  `ListHandlers`): каждый вызов стора — `Match`/`Bind`; drain-семантика
  PutObject (дочитывание тела при отказе стора) сохраняется: отказ
  теперь `Failed`, тело дочитывается до записи ответа.
- **Replication-хендлеры**: `HandleProbeAsync` — `Failed(UnknownReplicationTargetException)`
  → 400; ping/приёмники — по §4.3.
- **Hosted-сервисы** (`ReplicationCoordinator`, `ErasureHealHostedService`,
  `VolumeCleanupService`, reconcile-циклы): потребляют новые сигнатуры,
  логика повторов/бэкоффов не меняется.

### 4.6. Подключение Shared.Core

`ProjectReference` на `../Shared.Core/Shared.Core.csproj` — в
`OwnS3.Protocol.csproj`, `OwnS3.Storage.csproj`,
`OwnS3.Replication.csproj`, `OwnS3.App.csproj`. Версионирование внешних
пакетов не затрагивается (ProjectReference, новых внешних пакетов нет).
Границы зависимостей главы 01 §3 сохраняются: `Protocol` и `Storage`
не зависят друг от друга и от `App`.

### 4.7. Arch-first правки (фаза 0, до кода)

| Документ | Правка |
|---|---|
| `arch/owns3/01-overview.md` §3 | Границы зависимостей: все четыре проекта OwnS3 ссылаются на `Shared.Core` (конвенция исходов монорепо) |
| `arch/owns3/01-overview.md` §4 | Ключевое решение: «конвенция исходов внутренних операций — `Result`/`Result<T>` (Shared.Core): Success несёт терминальный исход, включая не-ошибочные терминальные варианты (Not-Modified, отсутствие записи, tombstone/404 соседа, reachable=false с полученным ответом); Failed — технический отказ (транспорт, кворум, 429, ошибки каталога операций). Внешние HTTP-грани и дисковые форматы от конвенции не зависят» |
| `arch/owns3/06-erasure.md` §8 | Строка: кворумные отказы записи/чтения (503 `Insufficient*`) и исходы операций стора доставляются вызывающему слою по Result-конвенции главы 01 §4 (без летящих исключений через границу интерфейса) |
| `arch/owns3/07-replication.md` §11 | Строка в «Исполнение в App»: внутренние исходы транспорта/notifier/фетчера/prober — Result-конвенция главы 01 §4: 429/обрыв — отказ (бэкофф), ACK/404/tombstone — терминальные значения фетчера и notifier'а |
| `arch/owns3/05-access-config.md` | Правка только при фактическом месте, описывающем исходы внутренних интерфейсов (по решению user-review; правка «под набор» не делается) |

## 5. Фазы

### Фаза 0 — arch-first

Правки по таблице §4.7; канон описывает конвенцию до её появления в коде.

### Фаза 1 — подключение и стор (`src/OwnS3.Storage`)

1. `ProjectReference` Shared.Core во все четыре csproj (§4.6).
2. Доменные типы-исходы: `XlMetaLookup`, `ReplicaKeyLookup`,
   `GetAttributesOutcome`, `ReplicaListingPage` (§4.2); демонтаж
   `ObjectAttributesResult`.
3. Перевод `IObjectStore`/`IXlDrive`/`IReplicaStore` + реализаций
   (`XlObjectStore.*`, `ReplicaStore`, `SingleXlDrive`, `ErasureXlDrive`)
   на Result-сигнатуры.
4. Юнит-тесты Storage рядом с изменениями: исходы операций (успех /
   ошибка каталога / терминальные варианты), отсутствие записи =
   `Success(Missing)`, кворумные отказы EC — `Failed`.

### Фаза 2 — репликация (`src/OwnS3.Replication`)

1. Транспорт: `SendNotificationsAsync → Result<long>`,
   `FetchObjectAsync → Result<FetchOutcome>`, `PingAsync →
   Result<PingResult>`, листинги; `FetchResult` демонтирован; типы
   `PeerRejectedException`/`UnknownReplicationTargetException`.
2. `ReplicaApplier` → `Result<ReplicaApplyOutcome>` (четыре значения,
   §4.4); `ReplicationProber` → `Result<ProbeReport>`.
3. Потребители: `NotificationDispatcher`, `ObjectFetcher`,
   `NotificationInbox`, `ReconcileRunner` — на Match/Bind; логика
   бэкоффов/повторов/счётчиков без изменений (семантика счётчиков — §4.4).
4. Юнит-тесты Replication: 429 → `Failed` → бэкофф; фетч
   объект/tombstone/404 → применение/delete/skip; probe — один сосед,
   неизвестное имя → `Failed`; фейк `IReplicationTransport` — на новых
   сигнатурах.

### Фаза 3 — пайплайн/хендлеры (`src/OwnS3.App`)

1. Транслятор исходов + демонтаж catch-веток `S3Middleware` (§4.5).
2. Хендлеры операций, replication- и admin-хендлеры — на новых сигнатурах;
   drain-семантика PutObject сохранена.
3. Интеграционные сценарии (`src/tests/OwnS3.IntegrationTests/Api/`) —
   прогон без правок HTTP-ожиданий (регресс контракта); при необходимости
   правки только внутренних механик фикстур.

### Фаза 4 — полные прогоны

Юниты → интеграции (`-c Debug`); docker-E2E серии OwnS3 на свежем Release
(`PGW_TEST_DOCKER=1`, инкрементальная сборка E2eFixture); после каждой
серии — зачистка контейнеров/сетей по канонам AGENTS.md; телеметрия
прогона — `docs/e2e-launch.md`.

## 6. Ограничения и границы

- **Вне задачи**: изменения HTTP-контракта S3-API/replication/admin-граней,
  JSON-моделей ответов, дисковых форматов (`xl.meta`, `.owns3.sys`),
  поведения операций; сигнатуры `OwnS3.Protocol` (SigV4/XML/`S3ProtocolException`
  остаются исключениями — Protocol вне области); правки потребителей
  OwnS3 по HTTP (`S3Worker`, `AdminPanel`, бэкапы PgWorker);
  новые внешние пакеты; `OwnS3NodeClient` воркеров (уже на Result).
- Отмена (`OperationCanceledException` по внешнему ct) — не исход
  операции: пробрасывается, как сегодня (graceful stop).
- `TreatWarningsAsErrors=true` — сборка чистая; `LangVersion=latest`,
  `Nullable=enable`.
- Multi-host: конвенция исходов не меняет сетевую модель и durability
  (глава 07 §12) — отказы лишь меняют форму доставки вызывающему.
- Roadmap-запись `t38` удаляется из `arch/roadmap/owns3.md` мерж-коммитом
  задачи (канон AGENTS.md).

## 7. Критерии приёмки

1. Arch-правки (§4.7) закоммичены раньше кодовых (arch-first) и
   соответствуют реализованной конвенции дословно.
2. Все внутренние сигнатуры OwnS3 (`IObjectStore`, `IXlDrive`,
   `IReplicaStore`, `IReplicationTransport`, `ReplicaApplier`,
   `ReplicationProber`, хендлеры) — на `Result`/`Result<T>`;
   nullable-маркеры (`long?`, `ProbeReport?`, `XlMetaRecord?`,
   `ReplicaKeyState?`) и DTO-дискриминации (`FetchResult`,
   `ObjectAttributesResult`, `ReplicaApplyResult`) устранены;
   `PutResult`/`CompleteResult`/`DeletedKeyResult` — значения успеха в
   Result-сигнатурах.
3. `S3Middleware` не содержит catch-веток `ObjectStoreException` /
   `XlInvalidArgumentException` / `ObjectStoreUnavailableException`;
   маппинг ошибок — транслятором App; catch `S3ProtocolException` и
   catch-all → 500 сохранены.
4. HTTP-контракт неизменен: интеграционные сценарии Api проходят без
   правок HTTP-ожиданий; `ProbeOutcome`/JSON-модели ответов — без
   изменений (`rttMs` — по-прежнему nullable-поле JSON).
5. Семантика Result (принцип 3) подтверждена тестами: 404/tombstone
   фетча — терминальные Success (skip без повтора), 429/обрыв — Failed
   (бэкофф), `TryReadMeta`/`ReadKeyState` отсутствие записи —
   Success(Missing), ошибки каталога S3-операций — Failed с кодом;
   исходы применения — `Applied` → applied, `Skipped` → skipped,
   `ConflictApplied` → conflict + applied, `ConflictSkipped` →
   conflict + skipped (метрики главы 07 §10 байт-в-байт,
   `ownS3_replication_conflicts_total` не занижается).
6. Серии зелёные: `OwnS3.UnitTests`, `OwnS3.IntegrationTests` — `-c Debug`;
   docker-E2E серии OwnS3 — на свежем Release (`PGW_TEST_DOCKER=1`);
   между сериями — зачистка контуров (ассерты чистоты teardown).
7. Мерж-гейт задачи: код трогает `src/OwnS3.*` — docker-E2E на свежем
   Release обязателен (правило AGENTS.md); полный прогон E2e-серий OwnS3.
