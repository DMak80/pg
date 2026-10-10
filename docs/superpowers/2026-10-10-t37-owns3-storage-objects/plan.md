# t37-owns3-storage-objects: план реализации (rev.6: BCL-only без unsafe/P-Invoke; CopyObject на net10 — всегда fallback-копирование, вариант A)

> **Для исполняющих агентов:** ОБЯЗАТЕЛЬНЫЙ саб-скилл: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять по задачам; шаги отмечаются чекбоксами (`- [ ]`). Каждый шаг несёт Вход/Действие/Выход/Проверку/Связь со spec.

**Цель:** single-drive xl-хранение ownS3 (канон `arch/owns3/04-storage.md`) и реализация 15 операций контракта `IObjectStore` (7 объектных + 3 листинга + 5 бакетных); multipart (7 методов) остаётся заглушечным до t38.

**Архитектура:** `OwnS3.Storage` — чистая библиотека (без ASP.NET): `XlVolume` (том, чистки) + `XlObjectStore` (реализация `IObjectStore`) + кодирование путей/метафайл/листинг/условия как отдельные классы; коммит-поинт любой записи — атомарная замена `xl.meta` (rename). `OwnS3.App` подставляет `XlObjectStore` в DI, инициализирует том fail-fast до Kestrel, фоновые чистки 15 мин, `/healthz` с пробой тома.

**Технологии:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions, `TimeProvider` (тесты — фиксированное время), IncrementalHash (MD5/SHA-256), `FileStream.Flush(flushToDisk: true)` — только стандартные API .NET (см. ограничение 9); CopyObject на net10 — побайтовое копирование (хардлинк — точка включения на net11, P11).

**Спека:** `docs/superpowers/2026-10-10-t37-owns3-storage-objects/spec.md` (план аргументирует от спеки; исполнители читают оба документа).

**Рабочая директория всех команд:** корень worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t37-owns3-storage-objects` (ветка `feat-t37-owns3-storage-objects`, коммитить свободно). Все пути ниже — от корня worktree.

---

## Глобальные ограничения (действуют в каждой задаче)

1. Сборка Release — 0 warnings (`TreatWarningsAsErrors=true`, `src/Directory.Build.props`); MD5 — только `IncrementalHash.CreateHash(HashAlgorithmName.MD5)` / `MD5.HashData` (прецедент `src/OwnS3.App/Handlers/HashingBodyStream.cs`, паттерн зелёный в t36).
2. Комментарии/документация — по-русски; идентификаторы — на английском. Тесты — AAA-нотация в комментариях (`// Arrange`, `// Act`, `// Assert`).
3. `OwnS3.Protocol` НЕ трогаем (границы t37, спека §9); `arch/*` уже содержит правки спеки §3 — новых arch-правок план не делает (решение пользователя: порядок листинга — канон 02 §3 «лексикографический по UTF-8 байтам» буквально, без правок; conditional в GetObjectAttributes — решением координатора включается в t37, спека не противоречит).
4. Никаких docker-контейнеров и хост-портов: юниты — temp-каталог, интеграция — in-memory WAF. Никаких sleep-ожиданий дольше 30 с: детерминированное время через фиксированный `TimeProvider` и `File.SetLastWriteTimeUtc`.
5. `ObjectStoreErrorCode`/`ObjectStoreException` — без изменений (спека §4.1).
6. Каждая задача заканчивается зелёным прогоном своих тестов и коммитом; тесты запускаются с `DOTNET_CLI_UI_LANGUAGE=en`.
7. Чистки тестовой среды: temp-тома удаляются в teardown тестов (`finally`/`Dispose`); docker не используется — контейнерной зачистки нет.
8. **Изоляция интеграционных кейсов:** том на класс (`IClassFixture`) НЕ достаточен — xUnit рандомизирует порядок кейсов внутри класса. Правило: мутационные кейсы работают на уникальном имени бакета СВОЕГО кейса (`b-<суффикс>`), немутационные — на никогда не создаваемом имени `bucket`; порядок кейсов внутри класса тогда неважен.
9. **Только стандартные API .NET (BCL) — приказ пользователя:** запрещён unsafe в любом виде (ключевое слово `unsafe`, блоки, `AllowUnsafeBlocks` в csproj, `LibraryImport` — генератор эмитит unsafe) и P/Invoke в любом виде (`DllImport`, вызовы libc/native). Следствие: fsync каталогов НЕ выполняется — стандартного BCL-API не существует (см. примечание Task 3).

## Проектные решения плана (в рамках формата спеки, применяются всеми задачами)

- **P1. dataDir-имя = versionId записи.** Формат `xl.meta` (спека §4.2) не хранит отдельного поля dataDir; имя каталога данных `<uuid>/part.1` := `VersionId` записи (оба — UUID v4 «новый на каждую запись», канон 04 §1/§2). Orphan-чистка: Guid-подкаталоги каталога объекта ≠ versionId текущего `xl.meta` (или при отсутствии/битости `xl.meta` — все Guid-подкаталоги, см. Task 3).
- **P2. `XlInvalidArgumentException(message) : Exception`** — доменный 400-исход Storage (лимит сегмента 255 байт, невалидный continuation-token). `InvalidArgument` в `ObjectStoreErrorCode` отсутствует и не добавляется (ограничение 5); App ловит по типу и маппит в `S3ErrorCode.InvalidArgument`.
- **P3. `XlIntegrityException(message) : Exception`** — невосстановимая порча (битые `xl.meta`+`xl.meta.bkp`, checksum-mismatch при чтении, несоответствие фактической длины тела заявленной). Попадает в catch-all `S3Middleware` → 500 `InternalError` с диагностикой в лог.
- **P4. Файловый IO в Storage — синхронный** (локальный том, надёжность и простота); методы контракта — async-обёртки (`Task.FromResult`/`ValueTask`). fsync-дисциплина (спека §4.3 п.2) в рамках BCL: `FileStream.Flush(flushToDisk: true)` для КАЖДОГО записываемого файла (`part.1` и временного файла `xl.meta` до его rename); fsync КАТАЛОГОВ не выполняется — стандартного BCL-API нет, P/Invoke запрещён (ограничение 9, примечание Task 3); вместо него в коммит-цикле записи — diagnostic-warning в лог (Task 6 п.4).
- **P5. Время.** Все метки времени — через `TimeProvider` (мс, UTC). Тесты: существующий `FixedTimeProvider` (`src/tests/OwnS3.UnitTests/TestVectors.cs`) задаёт «сейчас» записи → modTime детерминирован; возраст `.trash`/orphan в тестах чисток — `File.SetLastWriteTimeUtc(...)` в прошлое.
- **P6. Транзитные заглушки `XlObjectStore`.** Класс появляется в задаче 4 со всеми методами; нереализованные на момент задачи методы бросают `ObjectStoreUnavailableException` (UploadPart — drain + бросок, семантика NotWired); каждая следующая задача заменяет свою группу методов на реализацию. После задачи 8 заглушечными остаются только multipart-методы (до t38).
- **P7. Файлы Storage** — в корне `src/OwnS3.Storage/` (соответствие таблице спеки §4.1); `XlObjectStore` — partial: `XlObjectStore.cs` (ctor/хелперы), `XlObjectStore.Buckets.cs`, `XlObjectStore.Objects.cs`, `XlObjectStore.Copy.cs`, `XlObjectStore.List.cs`, `XlObjectStore.Multipart.cs`. Тесты — `src/tests/OwnS3.UnitTests/Storage/`.
- **P8. Конвенция ETag.** `xl.meta` хранит hex-значение БЕЗ кавычек; доменные значения, идущие в HTTP-заголовок/XML — В КАВЫЧКАХ: `PutResult.ETag`, `ObjectMetadata.ETag`, `ObjectAttributes.ETag`, `ListEntry.ETag` (Contents/ETag листинга — как у Get/Head, канон 02 §3); `ConditionalEvaluator.ETagMatches` сравнивает нормализованно (кавычки снимаются с обеих сторон).
- **P9. Байтовый порядок ключей.** Единый порядок сортировки/сравнения (листинги, ListBuckets, маркеры) — строгий лексикографический по UTF-8 байтам (канон 02 §3, решение пользователя). Сравнение реализует `Utf8ByteOrder` (создаётся в Task 1; побайтовое сравнение UTF-8; string Ordinal НЕ подходит — расходится с байтовым на символах вне BMP: UTF-16 ставит сурогаты D800–DFFF ниже U+E000–U+FFFF). Все места: сортировка детей, emit-фильтры, marker/continuation, сортировка ListBuckets — только через `Utf8ByteOrder`.
- **P10. Conditional в GetObjectAttributes (решение координатора).** GetObjectAttributes обрабатывает conditional-заголовки по таблице канона 02 §1 как GET-семантика (`ifModifiedSinceApplies: true`): провал `If-Match`/`If-Unmodified-Since` → бросок `PreconditionFailed` (412); попадание `If-None-Match`/`If-Modified-Since` → исход Not-Modified обёрткой `ObjectAttributesResult` (флагом с метаданными, не броском — 304 обязан нести заголовки `ETag`/`Last-Modified`, философия §5.3). Контракт `IObjectStore.GetObjectAttributesAsync` расширяется параметром `ObjectConditions?` (доменная правка уровня §5, разрешена решением координатора).
- **P11. CopyObject на net10 — всегда fallback-копирование (вариант A, решение пользователя).** `File.CreateHardLink` отсутствует в net10.0 (API .NET 11 Preview), P/Invoke/unsafe запрещены (ограничение 9) — выбран вариант A: на net10 линк постоянно «неудачен», работает fallback-ветка (побайтовое копирование `part.1` + fsync). Семантика Q2 «жёсткая ссылка + fallback при неудаче link» сохранена: на net10 link всегда неудачен; спека и каноны НЕ меняются (arch про CopyObject требует атомарный коммит и ETag-семантику — механика данных не предписана). Точка включения хардлинка — единственный метод `TryCreateHardLink` (Task 8): при переходе монорепо на net11 включается одной правкой в нём. Цена (осознанная): CopyObject большого объекта — полный IO-проход.

---

### Task 0: контрольная arch-сверка (фаза 0 спеки)

**Файлы:** только чтение; правок нет (правки канона уже в worktree: `git status` — `M arch/owns3/02-operations.md`, `M arch/owns3/04-storage.md`).

- [ ] **Шаг 1. Сверить пять правок §3 спеки с каноном 04**
  - Вход: спека §3; канон `arch/owns3/04-storage.md` (+ `02-operations.md` для п.2).
  - Действие: проверить по тексту канона: (1) спецслучаи кодирования (§1: `.`/`..` → `%2E`/`%2E%2E`; пустой сегмент → `%`; `__XLDIR__`-экранирование завершающего `_` → `%5F`; порядок декодирования — суффикс-проверка на сыром сегменте, затем percent-декод); (2) `bucket.json` (`createdAt`, unix-мс) — источник `CreationDate` ListBuckets; (3) цели `.trash/` — имена `<guid>`; (4) Delete при вложенных ключах: `xl.meta` и dataDir в `.trash/` ПО ОТДЕЛЬНОСТИ, опустевший каталог объекта удаляется; (5) новый ключ при живом каталоге-префиксе — схема перезаписи.
  - Выход: подтверждение, что все пять пунктов дословно присутствуют в каноне.
  - Проверка: любое расхождение — СТОП и эскалация координатору (arch-first правка), не «додумывать». Совпадение — переход к шагу 2.
  - Связь со spec: §3 (arch-first правки), §7 фаза 0.

- [ ] **Шаг 2. Сверить сверку SHA-256**
  - Вход: канон 04 §2.
  - Действие: убедиться, что канон фиксирует «сверка при каждом чтении данных» (правки не требуется — спека §3, последний абзац).
  - Выход: зафиксировано в журнале выполнения; коммита нет.
  - Проверка: текст канона содержит формулировку сверка при каждом чтении.
  - Связь со spec: §3, §4.4 п.5.

---

### Task 1: XlPathEncoder + Utf8ByteOrder — кодирование ключа и байтовый порядок (фаза 1)

**Файлы:**
- Create: `src/OwnS3.Storage/XlPathEncoder.cs`, `src/OwnS3.Storage/XlInvalidArgumentException.cs`, `src/OwnS3.Storage/Utf8ByteOrder.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlPathEncoderTests.cs`, `src/tests/OwnS3.UnitTests/Storage/Utf8ByteOrderTests.cs`

**Interfaces (производит):**
```csharp
namespace OwnS3.Storage;

// Доменный InvalidArgument-исход Storage (канон 04 §1 лимит сегмента; канон 02 §3
// невалидный continuation-token): InvalidArgument в ObjectStoreErrorCode отсутствует
// (перечень t37 закрыт спекой), App маппит по типу в S3 InvalidArgument 400.
public sealed class XlInvalidArgumentException(string message) : Exception(message);

public static class XlPathEncoder
{
    // Ключ → кодированные сегменты пути (без bucket), спецслучаи канона 04 §1.
    // Бросает XlInvalidArgumentException("Key too long after encoding") при
    // кодированном сегменте (вкл. приклеенный маркер) длиннее 255 байт.
    public static string[] EncodeSegments(string key);

    // Кодированный относительный путь (сегменты через '/').
    public static string EncodePath(string key);

    // Обратное преобразование (сегменты каталога → ключ); маркер каталога
    // раскрывается в завершающий '/'.
    public static string DecodeSegments(IReadOnlyList<string> segments);
}

// Строгий байтовый порядок UTF-8 (канон 02 §3, решение пользователя; P9):
// побайтовое сравнение UTF-8 представления. string Ordinal НЕ подходит —
// расходится с байтовым порядком на символах вне BMP (сурогаты D800-DFFF
// в UTF-16 ниже U+E000-U+FFFF, в UTF-8 — выше). Единый компаратор сортировки
// детей листинга, emit-фильтров, marker/continuation и ListBuckets.
public static class Utf8ByteOrder
{
    public static int Compare(string left, string right);       // побайтово UTF-8
    public static int CompareBytes(ReadOnlySpan<byte> l, ReadOnlySpan<byte> r);
    public static bool StartsWith(string value, string prefix); // байтовый префикс
}
```

**Алгоритм кодирования (зафиксирован, канон 04 §1; порядок шагов обязателен):**
1. Разбить ключ по `/` на сегменты (пустые сегменты значимы: `a//b` → `["a","","b"]`).
2. Если ключ оканчивается на `/` — последний (пустой) сегмент-маркер ОТБРАСЫВАЕТСЯ и запоминается флаг `hasDirMarker`; кодируются остальные сегменты. Каждый сегмент: `Uri.EscapeDataString(segment)` (RFC 3986: вне `A-Za-z0-9-._~` → `%XX`, UTF-8), затем спецслучаи на результат: `""` → `"%"`; `"."` → `"%2E"`; `".."` → `"%2E%2E"`; результат, оканчивающийся на `__XLDIR__` (литеральный) — заменить ПОСЛЕДНИЙ символ `_` на `"%5F"`.
3. При `hasDirMarker` — к последнему уже КОДИРОВАННОМУ сегменту приклеивается суффикс `__XLDIR__` (экранирование литерального маркера уже выполнено на шаге 2 — приклеенный маркер остаётся чистым; по образцу `encodeDirObject` референса: `prefix/` → `prefix__XLDIR__`).
4. Проверка: длина каждого кодированного сегмента в байтах UTF-8 ≤ 255 — ПОСЛЕ приклейки маркера (маркер удлиняет сегмент); иначе бросок `XlInvalidArgumentException("Key too long after encoding")`.

Проверка порядка на векторах: `dir/` → шаг 2: `["dir"]` → шаг 3: `["dir__XLDIR__"]` ✓ (НЕ `dir__XLDIR_%5F`); `a//` → сегменты `["a","",""]`, маркер: кодируем `["a",""]` → `["a","%"]` → приклейка → `["a","%__XLDIR__"]` ✓; литеральный `x__XLDIR__` (без завершающего `/`) → шаг 2 экранирует → `x__XLDIR_%5F` ✓; составной `x__XLDIR__/` → `["x__XLDIR_%5F"]` + маркер → `x__XLDIR_%5F__XLDIR__` (декод: снять суффикс → percent-декод → `x__XLDIR__` + `/` — roundtrip сходится).

**Алгоритм декодирования (порядок обязателен — симметричен кодированию, «обратное преобразование однозначно», канон 04 §1):**
1. Суффикс-проверка `__XLDIR__` на СЫРОМ сегменте: маркер каталога — снять суффикс, сегмент даст часть ключа с завершающим `/`.
2. Спецслучай на сыром сегменте: сегмент, РАВНЫЙ одиночному `"%"`, — пустая строка. ВАЖНО: применять ДО percent-декода — `Uri.UnescapeDataString("%")` возвращает `"%"` (невалидный escape не декодируется), буквальное применение сломало бы roundtrip `a//b`. Сырой одиночный `%` возникает только из пустого сегмента (литеральный `%` кодируется как `%25`, канон 04 §1).
3. Percent-декод остатка: `Uri.UnescapeDataString`.
4. Ключ = join `/` между частями сегментов.

**Эталонные векторы (обязательные в тестах, roundtrip encode→decode = исходный ключ):**

| Ключ | Путь |
|---|---|
| `a/b/c` | `a/b/c` |
| `dir/` | `dir__XLDIR__` |
| `a//b` | `a/%/b` |
| `a//` | `a/%__XLDIR__` |
| `.` | `%2E` |
| `..` | `%2E%2E` |
| `100%` | `100%25` |
| `lit%2E` | `lit%252E` |
| `x__XLDIR__` (литеральный) | `x__XLDIR_%5F` |
| `x__XLDIR__/` (литерал + маркер) | `x__XLDIR_%5F__XLDIR__` |
| `café` | `caf%C3%A9` |

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: интерфейсы Task 1 определены (ещё не реализованы).
  - Действие: создать `XlPathEncoderTests`: `[Theory]`/`[InlineData(key, path)]` по таблице векторов — три ассерта на вектор (Encode == path; Decode == key; roundtrip); прямой кейс декода `DecodeSegments(["a","%","b"]) == "a//b"` (фиксация спецслучая `%`); кейс `new string('a', 300)` → `XlInvalidArgumentException`; кейсы коллизий `a/%/b` ≠ `a/%25/b`. Создать `Utf8ByteOrderTests`: `Compare_ByteOrderNotUtf16` — `"\uFFFF"` vs `"\U0001F600"` (😀): байтово `EF BF BF` < `F0 9F 98 80` → 😀 БОЛЬШЕ U+FFFF (string Ordinal дал бы обратное — фиксирует P9); ASCII/диакритика (`Z` < `a` < `~` < `Á`). Шаблон кейса:

```csharp
[Theory]
[InlineData("dir/", "dir__XLDIR__")]
[InlineData("a//b", "a/%/b")]
public void Encode_KeyToPath_CanonicalVectors(string key, string expectedPath)
{
    // Arrange / Act
    var path = XlPathEncoder.EncodePath(key);
    // Assert: вектор канона 04 §1 (спецслучаи кодирования)
    path.Should().Be(expectedPath);
    XlPathEncoder.DecodeSegments(path.Split('/')).Should().Be(key); // roundtrip
}
```

  - Выход: файлы тестов с красными кейсами.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlPathEncoderTests|FullyQualifiedName~Utf8ByteOrderTests"` → ошибка компиляции (типы не найдены).
  - Связь со spec: §4.1 (XlPathEncoder), §10.3 (раскладка), канон 02 §3 (порядок, P9).

- [ ] **Шаг 2. Реализовать**
  - Вход: красные тесты шага 1.
  - Действие: `XlInvalidArgumentException.cs`, `XlPathEncoder.cs` (порядок шагов кодирования/декодирования обязателен), `Utf8ByteOrder.cs`.
  - Выход: кодирование и байтовый компаратор готовы.
  - Проверка: та же команда → все кейсы PASS.
  - Связь со spec: §4.1, канон 04 §1, P9.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон шага 2.
  - Действие: `git add -A && git commit -m "feat(owns3-storage): XlPathEncoder — кодирование ключа в xl-путь по канону 04 (спецслучаи, лимит 255 байт) + Utf8ByteOrder"`.
  - Выход: коммит в feature-ветке.
  - Проверка: `git log -1 --stat` — три новых файла.
  - Связь со spec: фаза 1 §7.

---

### Task 2: XlMetaFile — побайтовый формат xl.meta (фаза 1)

**Файлы:**
- Create: `src/OwnS3.Storage/XlMetaFile.cs`, `src/OwnS3.Storage/XlIntegrityException.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlMetaFileTests.cs`

**Interfaces (производит):**
```csharp
namespace OwnS3.Storage;

// Невосстановимая порча xl-данных: битые xl.meta+xl.meta.bkp, checksum-mismatch
// при чтении, несоответствие фактической длины тела. App-конвейер отдаёт 500
// InternalError (catch-all), диагностика — в лог.
public sealed class XlIntegrityException(string message) : Exception(message);

// Запись-версия unversioned-объекта (формат — спека §4.2 / канон 04 §2).
public sealed record XlMetaRecord(Guid VersionId, long Size, DateTimeOffset ModTime, string ETag,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata,
    IReadOnlyDictionary<string, string> Headers, string ContentSha256)
{
    // Имя каталога данных = versionId (решение P1).
    public string DataDirName => VersionId.ToString("N");
}

public static class XlMetaFile
{
    // Строгая десериализация одного файла; битая структура/magic/версия — XlIntegrityException.
    public static XlMetaRecord ReadFile(string path);

    // Чтение каталога объекта: xl.meta → при порче фолбэк xl.meta.bkp (fromBackup=true);
    // отсутствие обоих файлов — FileNotFoundException (вызывающий транслирует в NoSuchKey);
    // оба битые — XlIntegrityException.
    public static XlMetaRecord Read(string objectDir, out bool fromBackup);

    // Запись xl.meta в каталог: существующий xl.meta копируется в xl.meta.bkp →
    // новый пишется во временный файл (FileStream, flushToDisk: true — fsync ДО
    // rename, спека §4.3 п.2) → атомарный rename поверх (канон 04 §2).
    public static void Write(string objectDir, XlMetaRecord record);
}
```

**Бинарный формат (little-endian, дословно спека §4.2):**
```
magic "OWS3" (4 ASCII) | uint8 formatVersion=1 | uint16 recordVersion=1 |
16B versionId | int64 size | uint64 modTimeUnixMs |
string etag | string contentType |
uint16 userMetadataCount | (string key, string value)* |
uint16 headersCount | (string name, string value)* |
string contentSha256
```
`string` = `uint16 byteLength + UTF-8 bytes`. Длины писать вручную (`BinaryPrimitives`/`MemoryStream` + `BinaryWriter` для чисел; 7-bit-префикс `BinaryWriter.Write(string)` НЕ подходит). Чтение: любое несоответствие (обрыв данных, magic ≠ `OWS3`, formatVersion ≠ 1, recordVersion ≠ 1, остаточные байты) → `XlIntegrityException`. `ETag` в записи — hex без кавычек (P8). `Write`: временный файл `xl.meta.tmp` создаётся/пишется через `FileStream` с `Flush(flushToDisk: true)` и закрывается ДО `File.Move(tmp, xl.meta, overwrite: true)` (fsync tmp-файла до rename; fsync каталога после rename не выполняется — примечание Task 3).

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: интерфейс Task 2 определён.
  - Действие: `XlMetaFileTests` (temp-каталог `Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid())`, удаление в `finally`): `SerializeDeserialize_Roundtrip` (все поля: userMetadata 2 пары, headers 1 пара, etag hex без кавычек, sha256 64 hex); `WriteRead_ThroughFile_Roundtrip`; `Write_Overwrite_CreatesBkpAndReplaces` (v1, v2 → xl.meta = v2, xl.meta.bkp = v1); `Read_CorruptedMain_FallsBackToBkp` (Write v1, Write v2, затереть `xl.meta` мусором → Read = v1, `fromBackup=true`); `Read_BothCorrupted_ThrowsIntegrity`; `ReadFile_BadMagic_ThrowsIntegrity`; `ReadFile_UnknownFormatVersion_ThrowsIntegrity`; `ReadFile_TruncatedPayload_ThrowsIntegrity`.
  - Выход: файл теста.
  - Проверка: прогон с фильтром `~XlMetaFileTests` → ошибка компиляции.
  - Связь со spec: §4.1 (XlMetaFile), §4.2 (формат), §4.4 п.1 (bkp-фолбэк).

- [ ] **Шаг 2. Реализовать**
  - Вход: красный тест.
  - Действие: `XlMetaFile.cs` + `XlIntegrityException.cs` по формату (Write — с fsync временного файла до rename; только BCL — ограничение 9).
  - Выход: метафайл готов.
  - Проверка: фильтр `~XlMetaFileTests` → PASS.
  - Связь со spec: §4.2, §4.3 п.2.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): XlMetaFile — бинарный формат xl.meta, bkp-фолбэк, атомарная замена с fsync tmp-файла"` (после `git add -A`).
  - Выход: коммит.
  - Проверка: `git log -1` — сообщение на месте.
  - Связь со spec: фаза 1 §7.

---

### Task 3: XlVolume — том, volume.json, чистки (фаза 1)

> **Примечание (глобальное, ограничение 9 — приказ пользователя): fsync каталогов НЕ выполняется.** Стандартного BCL-API fsync каталога не существует, P/Invoke и unsafe запрещены. Канон 04 / спека §4.3 п.2 допускают best-effort: платформа/механизм без fsync-каталога → warning в лог, не отказ. Следствие для надёжности: fsync ФАЙЛОВ (`part.1`, tmp-файл `xl.meta` через `FileStream.Flush(flushToDisk: true)`) выполняется полностью; durability каталогных метаданных — на усмотрение ОС. Компонент `DirectoryFsync` из ранних ревизий плана УДАЛЁН.

**Файлы:**
- Create: `src/OwnS3.Storage/XlVolume.cs`
- Modify: `src/OwnS3.Storage/OwnS3.Storage.csproj` — добавить `<PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />` (версия 10.0.9 уже в `src/Directory.Packages.props`)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlVolumeTests.cs`

**Interfaces (производит):**
```csharp
namespace OwnS3.Storage;

// Том xl (канон 04 §1/§3/§6). Один экземпляр на процесс; после Initialize —
// служебная структура готова, чистки старта выполнены.
public sealed class XlVolume(string root, TimeProvider timeProvider, ILogger? logger = null)
{
    public string Root { get; }               // корень тома
    public string SysDir { get; }             // <root>/.owns3.sys
    public string TmpDir { get; }             // <sys>/tmp
    public string MultipartDir { get; }       // <sys>/multipart
    public string TrashDir { get; }           // <sys>/.trash
    public string BucketsMetaDir { get; }     // <sys>/buckets
    public string ConfigDir { get; }          // <sys>/config
    public bool Initialized { get; }

    // Создание на пустом томе ИЛИ валидация существующего volume.json
    // (отсутствие при непустом томе / чужой magic / версия > 1 / режим не
    // "XL Single" — исключение fail-fast, канон 04 §3). Затем старт-чистки §6:
    // tmp/* — безусловно; .trash/* и orphan-dataDir старше 1 ч (mtime).
    public void Initialize();

    // Фоновый проход (те же пороги .trash/orphan 1 ч; multipart — t38).
    public Task RunCleanupAsync(CancellationToken ct);

    // volume.json валиден + touch-проба записи в tmp (создать+удалить файл).
    public bool CheckHealth();

    // rename пути в .trash/<guid> (создаёт .trash при отсутствии).
    internal void MoveToTrash(string path);
}
```

**Детали:**
- `volume.json`: JSON `{"magic":"OWNS3-VOL","formatVersion":1,"volumeId":"<uuid N>","mode":"XL Single"}`; `File.WriteAllText` + `JsonSerializer` (BCL, пакет не нужен).
- **Orphan-чистка (старт и фон — один и тот же код)**: рекурсивный обход поддеревьев бакетов (каталоги 1-го уровня тома, кроме `.owns3.sys`); в КАЖДОМ каталоге поддерева подкаталоги, чьё имя парсится `Guid.TryParse(..., "N")` и чей mtime старше 1 ч → `MoveToTrash`, ЗА ИСКЛЮЧЕНИЕМ текущего dataDir — `VersionId.ToString("N")` читаемого `xl.meta` ЭТОГО каталога, если `xl.meta` есть и валиден. Отсутствие или битость `xl.meta` каталога НЕ блокирует чистку его Guid-подкаталогов (orphan после краша Delete между двумя rename: `xl.meta` уже в `.trash`, dataDir-каталог остался — чистится по возрасту). Битый `xl.meta` — `logger.LogWarning`, не трогать сам файл. Не-Guid подкаталоги — сегменты вложенных ключей, не трогать.
- **Порог 1 ч** = `timeProvider.GetUtcNow() - File.GetLastWriteTimeUtc(path) > TimeSpan.FromHours(1)`.
- `RunCleanupAsync` — синхронная реализация в `Task.FromResult`-обёртке.

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: интерфейс Task 3 определён.
  - Действие: `XlVolumeTests` (temp-том, `finally`-удаление, `TimeProvider.System`):
    - `Initialize_EmptyRoot_CreatesSysLayout` — существуют `.owns3.sys/{tmp,multipart,.trash,buckets,config}` и валидный `volume.json`;
    - `Initialize_SecondRun_ValidatesAndKeeps`;
    - `Initialize_ForeignMagic_Throws`; `Initialize_IncompatibleVersion_Throws` (formatVersion 2 в файле); `Initialize_WrongMode_Throws`; `Initialize_MissingVolumeJsonOnDirtyVolume_Throws`;
    - `Initialize_ClearsTmpUnconditionally`;
    - `Initialize_RemovesAgedTrash_KeepsFresh` (`File.SetLastWriteTimeUtc`: −2 ч удалён, −1 мин остался);
    - `Initialize_KeepsCurrentDataDir_MovesAgedOrphan` — раскладка руками: `b/k/xl.meta` (запись VersionId V через `XlMetaFile.Write`), `b/k/<V>/part.1`, лишний `b/k/<other-guid>/part.1` mtime −2 ч → после Initialize лишний в `.trash/`, `<V>` на месте;
    - `Cleanup_RemovesGuidDataDir_WithoutXlMeta` — каталог `b/k/<guid>/part.1` mtime −2 ч, `xl.meta` у `b/k` ОТСУТСТВУЕТ (краш Delete между rename) → `RunCleanupAsync` → Guid-каталог в `.trash/`;
    - `Cleanup_KeepsNonGuidSubdirs` — `b/k/nested-key/xl.meta` не тронут;
    - `RunCleanup_SameThresholds` (пороги `.trash`/orphan без Initialize);
    - `CheckHealth_TrueOnValidVolume`; `CheckHealth_FalseWhenTmpMissing`.
  - Выход: файл теста.
  - Проверка: фильтр `~XlVolumeTests` → ошибка компиляции.
  - Связь со spec: §4.1 (XlVolume), §4.7 (пороги/старт/фон), §2.2 (мусор только tmp/.trash/orphan — всё чистится).

- [ ] **Шаг 2. Реализовать**
  - Вход: красный тест.
  - Действие: `XlVolume.cs` по Interfaces/Детали выше (только BCL — ограничение 9), PackageReference в csproj.
  - Выход: том готов.
  - Проверка: весь `OwnS3.UnitTests` без фильтра → PASS (регрессий нет).
  - Связь со spec: §4.7.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): XlVolume — volume.json fail-fast, старт/фоновые чистки (orphan вкл. без xl.meta)"`.
  - Выход: коммит.
  - Проверка: `git log -1 --stat`.
  - Связь со spec: фаза 1 §7.

---

### Task 4: BucketMetaStore + бакетные операции XlObjectStore (фаза 2)

**Файлы:**
- Create: `src/OwnS3.Storage/BucketMetaStore.cs`, `src/OwnS3.Storage/XlObjectStore.cs`, `src/OwnS3.Storage/XlObjectStore.Buckets.cs`, `src/OwnS3.Storage/XlObjectStore.Multipart.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs`

**Interfaces (производит; задачи 6–9 используют; использует `Utf8ByteOrder` из Task 1):**
```csharp
namespace OwnS3.Storage;

public sealed class BucketMetaStore(XlVolume volume, TimeProvider timeProvider)
{
    // .owns3.sys/buckets/<bucket>/bucket.json {"createdAt": <unix-мс>}; возвращает createdAt.
    public DateTimeOffset CreateBucketMeta(string bucket);
    public DateTimeOffset? TryReadCreationDate(string bucket); // null — файла нет
}

// Реализация IObjectStore (том обязан быть Initialized до первого вызова).
public sealed partial class XlObjectStore(XlVolume volume, TimeProvider timeProvider,
    ILogger<XlObjectStore> logger) : IObjectStore
{
    // Общие хелперы (XlObjectStore.cs):
    //   string BucketRoot(string bucket) — Path.Combine(volume.Root, bucket);
    //   bool BucketExists(string bucket) — Directory.Exists && bucket != ".owns3.sys";
    //   string ObjectDir(string bucket, string key) — BucketRoot + XlPathEncoder.EncodePath(key);
    //   void EnsureBucket(string bucket) — бросок ObjectStoreException(NoSuchBucket).
}
```

**Бакетные методы (спека §4.6):**
- `CreateBucketAsync`: существует → `BucketAlreadyOwnedByYou`; иначе `Directory.CreateDirectory(BucketRoot)` + `BucketMetaStore.CreateBucketMeta` (каталог бакета первым — видимость листинга = наличие каталога).
- `DeleteBucketAsync`: нет → `NoSuchBucket`; в поддереве есть `xl.meta` (`Directory.EnumerateFiles(b, "xl.meta", AllDirectories).Any()`) → `BucketNotEmpty`; иначе `MoveToTrash(BucketRoot)` + `MoveToTrash(BucketsMetaDir/<bucket>)` (проверка живых multipart-загрузок — t38, комментарий-маркер).
- `BucketExistsAsync`: каталог существует и не служебный.
- `ListBucketsAsync`: каталоги 1-го уровня кроме `.owns3.sys`, ОТСОРТИРОВАНО по имени (`Utf8ByteOrder` — создан в Task 1; сортировка в Storage обязательна: порядок `EnumerateDirectories` ФС не гарантирован; спека §4.6 «по алфавиту»); `CreationDate` из `bucket.json` (файла нет → `DateTimeOffset.FromUnixTimeMilliseconds(0)`).
- `XlObjectStore.Multipart.cs`: 7 multipart-методов — заглушки семантики NotWired (все → `throw new ObjectStoreUnavailableException()`; `UploadPartAsync` — сначала drain тела, код перенести из `NotWiredObjectStore.DrainAsync`); объектные методы ДО задач 6–9 — транзитно `throw new ObjectStoreUnavailableException()` (P6), каждый с комментарием `// t37: реализация в Task N плана`.

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: интерфейс Task 4 определён; `Utf8ByteOrder` доступен (Task 1).
  - Действие: `XlObjectStoreBucketTests` (фикстура: temp-том + `Initialize()` + `new XlObjectStore(volume, timeProvider, NullLogger<XlObjectStore>.Instance)`):
    - `CreateBucket_CreatesDirAndMeta_CreationDateFromBucketJson` (createdAt ≈ now фиксированного TimeProvider);
    - `CreateBucket_Existing_BucketAlreadyOwnedByYou`;
    - `DeleteBucket_Missing_NoSuchBucket`;
    - `DeleteBucket_Empty_Succeeds_RemovesDirAndMeta`;
    - `DeleteBucket_WithObject_BucketNotEmpty` (объект собрать руками: `ObjectDir` + `XlMetaFile.Write`);
    - `BucketExists_True/False` (`False` для `.owns3.sys`);
    - `ListBuckets_ExcludesSysDir_SortedByName_ReturnsCreationDates` — создать бакеты в НЕ-алфавитном порядке создания (`z-bucket` первым, `a-bucket` вторым) → результат отсортирован по имени;
    - `MultipartMethods_ThrowUnavailable` (все 7; `UploadPartAsync` — drain: `CountingStream`-паттерн из `NotWiredObjectStoreTests`).
  - Выход: файл теста.
  - Проверка: фильтр `~XlObjectStoreBucketTests` → ошибка компиляции.
  - Связь со spec: §4.6 (бакеты), §4.1 (XlObjectStore), §5.4 (multipart-заглушки переезжают).

- [ ] **Шаг 2. Реализовать**
  - Вход: красный тест.
  - Действие: четыре файла по спецификации выше (включая сортировку ListBucketsAsync через `Utf8ByteOrder`).
  - Выход: бакетный контур Storage готов.
  - Проверка: весь юнит-проект → PASS.
  - Связь со spec: §4.6.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): бакетные операции XlObjectStore + BucketMetaStore (ListBuckets отсортирован); multipart-заглушки"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 2 §7.

---

### Task 5: контрактные правки §5 + ConditionalEvaluator + RangeResolver (фаза 3, чистая логика)

**Файлы:**
- Modify: `src/OwnS3.Storage/Domain/DomainTypes.cs`, `src/OwnS3.Storage/IObjectStore.cs`, `src/OwnS3.Storage/NotWiredObjectStore.cs`
- Modify: `src/OwnS3.App/Handlers/ObjectHandlers.cs` (ReadOptions/GetObjectHandler/HeadObjectHandler/GetObjectAttributesHandler), `src/OwnS3.App/Pipeline/S3Middleware.cs`
- Modify: `src/tests/OwnS3.UnitTests/NotWiredObjectStoreTests.cs` (сигнатуры HeadObject/GetObjectAttributes)
- Create: `src/OwnS3.Storage/ConditionalEvaluator.cs`, `src/OwnS3.Storage/RangeResolver.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/ConditionalEvaluatorTests.cs`, `src/tests/OwnS3.UnitTests/Storage/RangeResolverTests.cs`

**Interfaces (производит):**
```csharp
// DomainTypes.cs — правки спеки §5 + P10:
public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range, string? IfRange);

public sealed record AppliedByteRange(long Start, long End, long Total);

// NotModified=true: Body=Stream.Null, Metadata заполнен (ETag/LastModified для 304).
// Range!=null: применённый диапазон; Body — Stream части объекта (у Head — Stream.Null).
public sealed record ObjectContent(ObjectMetadata Metadata, Stream Body, bool NotModified, AppliedByteRange? Range);

// Исход GetObjectAttributes (P10): NotModifiedMetadata != null → 304 (Metadata несёт
// ETag/LastModified для заголовков); иначе Attributes заполнены.
public sealed record ObjectAttributesResult(ObjectAttributes? Attributes, ObjectMetadata? NotModifiedMetadata);

// IObjectStore.cs (спека §5.2 + P10):
Task<ObjectContent> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options, CancellationToken ct);
Task<ObjectAttributesResult> GetObjectAttributesAsync(string bucket, string key,
    IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
    ObjectConditions? conditions, CancellationToken ct);

public enum ConditionalOutcome { Proceed, NotModified, PreconditionFailed }

public static class ConditionalEvaluator
{
    // Таблица канона 02 §1 (даты уже спарсены App; невалидные отброшены):
    // If-Match решает единолично; далее If-None-Match; далее If-Modified-Since
    // (только read-семантика: Get/Head/GetObjectAttributes); далее If-Unmodified-Since.
    public static ConditionalOutcome Evaluate(ObjectConditions? conditions, string etag,
        DateTimeOffset lastModified, bool ifModifiedSinceApplies);

    // Значение заголовка (возможен список через запятую, кавычки нормализуются,
    // «*» — любой) против etag объекта; Ordinal.
    public static bool ETagMatches(string headerValue, string etag);
}

public static class RangeResolver
{
    // null → полный объект (Range отсутствует или If-Range не совпал/дата).
    // Невалидный/вне размера/пустой объект → ObjectStoreException(InvalidRange).
    // Суффикс: ByteRange(null, negativeN) — N последних байт; N >= size → весь объект.
    public static AppliedByteRange? Resolve(ByteRange? range, string? ifRange, string etag, long size);
}
```

**Точное тело `Evaluate` (реализовать дословно):**
```csharp
public static ConditionalOutcome Evaluate(ObjectConditions? c, string etag,
    DateTimeOffset lastModified, bool ifModifiedSinceApplies)
{
    if (c is null) return ConditionalOutcome.Proceed;
    if (c.IfMatch is not null)
        return ETagMatches(c.IfMatch, etag) ? ConditionalOutcome.Proceed
                                            : ConditionalOutcome.PreconditionFailed;
    if (c.IfNoneMatch is not null)
        return ETagMatches(c.IfNoneMatch, etag) ? ConditionalOutcome.NotModified
                                                : ConditionalOutcome.Proceed;
    // modTime хранится в мс — сравнение с HTTP-датами по секундам
    var trimmed = new DateTimeOffset(lastModified.Ticks - lastModified.Ticks % TimeSpan.TicksPerSecond,
        lastModified.Offset);
    if (ifModifiedSinceApplies && c.IfModifiedSince is not null && trimmed <= c.IfModifiedSince)
        return ConditionalOutcome.NotModified;
    if (c.IfUnmodifiedSince is not null && trimmed > c.IfUnmodifiedSince)
        return ConditionalOutcome.PreconditionFailed;
    return ConditionalOutcome.Proceed;
}
```

**Точное тело `Resolve`:**
```csharp
public static AppliedByteRange? Resolve(ByteRange? range, string? ifRange, string etag, long size)
{
    if (ifRange is not null && !ConditionalEvaluator.ETagMatches(ifRange, etag))
        return null;                       // If-Range не совпал (или дата) → 200 полным
    if (range is null) return null;
    if (size == 0) throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
    if (range.Start is { } start)
    {
        if (start >= size) throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
        var end = range.End is null || range.End >= size ? size - 1 : range.End.Value;
        return new AppliedByteRange(start, end, size);
    }
    var suffix = -range.End!.Value;        // ParseRange кодирует bytes=-N как End=-N
    if (suffix <= 0) throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
    return suffix >= size ? new AppliedByteRange(0, size - 1, size)
                          : new AppliedByteRange(size - suffix, size - 1, size);
}
```

**Правки App (компиляция + активация 206/304/Attributes-conditional; живой эффект — после Task 10):**
1. `ObjectHandlers.ReadOptions`: добавить `var ifRange = context.Http.Headers["If-Range"].FirstOrDefault();` → `new ObjectReadOptions(conditions, range, ifRange.Length > 0 ? ifRange : null)`.
2. `GetObjectHandler`: после `Store.GetObjectAsync`:
   - `content.NotModified` → `StatusCode = 304`, заголовки `ETag`/`LastModified`, тело НЕ пишется (Body — Stream.Null; ContentLength не трогать — 304 без тела), return;
   - `content.Range is { } r` → `StatusCode = 206`, `Headers.ContentRange = $"bytes {r.Start}-{r.End}/{r.Total}"`, `ContentLength = r.End - r.Start + 1`, тело — как обычно;
   - иначе текущее поведение 200.
3. `HeadObjectHandler`: `await Store.HeadObjectAsync(bucket, key, options, ct)` → те же ветки 304/206/200 без тела (206: `ContentRange` + `ContentLength = r.End - r.Start + 1`; 304: только ETag/Last-Modified).
4. `GetObjectAttributesHandler` (P10): `var conditions = ReadOptions(context).Conditions;` → передача последним аргументом `GetObjectAttributesAsync`; ответ: `if (result.NotModifiedMetadata is { } nm) { StatusCode = 304; Headers.ETag = nm.ETag; Headers.LastModified = nm.LastModified.ToString("R"); return; }`; остальной код использует `result.Attributes!`.
5. `S3Middleware`: `catch (XlInvalidArgumentException ex)` → `WriteErrorAsync(new S3Error(S3ErrorCode.InvalidArgument, ..., MessageOverride: ex.Message))` — ПЕРЕД `catch (ObjectStoreException)`.
6. `NotWiredObjectStore`: `HeadObjectAsync(bucket, key, options, ct)` → `Throw<ObjectContent>()`; `GetObjectAttributesAsync(..., conditions, ct)` → `Throw<ObjectAttributesResult>()`; тест `NotWiredObjectStoreTests.BodylessMethods` — вызовы с `null!` для новых параметров.

- [ ] **Шаг 1. Написать падающие тесты**
  - Вход: interfaces Task 5 определены.
  - Действие: `ConditionalEvaluatorTests` (If-Match совпал/нет/`*`/кавычки/список; If-None-Match совпал → NotModified / не совпал → Proceed; приоритет If-Match над If-None-Match: оба заданы, оба совпали → Proceed — «остальные по нему не меняют исход»; If-Modified-Since: менялся позже → Proceed, не менялся → NotModified; `ifModifiedSinceApplies=false` игнорирует If-Modified-Since; If-Unmodified-Since: менялся после → PreconditionFailed; мс-обрезка: modTime 12:00:00.500 vs If-Modified-Since 12:00:00 → NotModified) и `RangeResolverTests` (`a-b`; `a-`; `-N`; end за размером обрезан; суффикс ≥ size → весь; start ≥ size → InvalidRange; size=0 c Range → InvalidRange; If-Range совпал → диапазон; If-Range не совпал → null; If-Range-дата → null).
  - Выход: два файла тестов.
  - Проверка: фильтры `~ConditionalEvaluatorTests|~RangeResolverTests` → ошибка компиляции.
  - Связь со spec: §4.1 (ConditionalEvaluator/RangeResolver), §4.4, §5.1–5.3, P10.

- [ ] **Шаг 2. Реализовать правки**
  - Вход: красные тесты.
  - Действие: правки домена/Storage/App по спискам «Interfaces» и «Правки App» (вкл. сигнатуру/обёртку GetObjectAttributes и его 304-ветку).
  - Выход: контракт чтения и Attributes обновлён; evaluator/resolver готовы.
  - Проверка: весь `OwnS3.UnitTests` → PASS; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App/OwnS3.App.csproj -c Release` → 0 warnings (интеграционные не меняются: NotWired ещё в DI, исходы прежние).
  - Связь со spec: §5, P10.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёные прогоны.
  - Действие: `git commit -m "feat(owns3): контрактные правки чтения (NotModified/Range/IfRange, Attributes-conditional) + ConditionalEvaluator + RangeResolver + 206/304 в хендлерах"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 3 §7.

---

### Task 6: XlObjectStore — Put/Delete/DeleteObjects, коммит-цикл (фаза 3)

**Файлы:**
- Create: `src/OwnS3.Storage/XlObjectStore.Objects.cs` (partial: PutObjectAsync/DeleteObjectAsync/DeleteObjectsAsync + приватные хелперы записи)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreObjectTests.cs` (фикстура-хелпер `StoreFixture`: temp-том + `FixedTimeProvider(TestVectors.FixedTime)` + store; Dispose — удаление тома)

**Interfaces (производит, используют Task 7–9):**
```csharp
// XlObjectStore.Objects.cs — приватные хелперы:
//   Task<PutResult> CommitStagedObjectAsync(string bucket, string key, XlMetaRecord record,
//       string stagingDir, CancellationToken ct)
//     — коммит-схема канона 04 §4 (новый ключ: один rename; перезапись: rename
//     dataDir → замена xl.meta → старый dataDir в .trash).
//   static async Task<(string ETagHex, string Sha256Hex)> CopyBodyToFileAsync(
//       Stream body, string filePath, long expectedLength)
//     — копирование с IncrementalHash MD5+SHA256 и счётчиком; фактическая длина ≠
//     expectedLength → XlIntegrityException (остаточный случай, спека §4.3 п.1).
```

**`PutObjectAsync` — последовательность (спека §4.3, реализовать точно):**
1. `EnsureBucket` → NoSuchBucket.
2. `staging = Path.Combine(volume.TmpDir, Guid.NewGuid().ToString("N"))`; `versionId = Guid.NewGuid()`; `dataDir = staging/<versionId "N">`; `part.1 = dataDir/part.1`.
3. Копирование тела → `(etag, sha256)`; `xl.meta` = `XlMetaRecord(versionId, actualLength, now, etag, metadata.ContentType, metadata.UserMetadata, EmptyHeaders, sha256)` (ETag — hex без кавычек, P8); `XlMetaFile.Write(staging, record)` (fsync tmp-файла внутри Write). fsync файлов: `part.1` — `FileStream.Flush(flushToDisk: true)` при записи (чистый BCL, ограничение 9). fsync КАТАЛОГОВ (`dataDir`, `staging`) не выполняется (примечание Task 3).
4. Коммит `CommitStagedObjectAsync`: `target = ObjectDir(bucket, key)`:
   - `target` НЕ существует → `Directory.CreateDirectory(Path.GetDirectoryName(target))` → `Directory.Move(staging, target)` (единый rename = коммит);
   - `target` существует → старый `XlMetaFile.Read(target, out _)` (нет файла — нет старого) → `Directory.Move(dataDir, target/<versionId "N">)` → `XlMetaFile.Write(target, record)` (bkp→tmp→rename, КОММИТ) → старый `target/<oldVersionId "N">` существует → `volume.MoveToTrash(...)`.
   - После коммита (место, где ранее планировался fsync каталога — приказ пользователя, примечание Task 3): `logger.LogWarning("fsync каталога объекта не выполняется (только BCL, приказ пользователя): {Target}", target)` — diagnostic-warning для наблюдаемости.
5. `finally`: `staging` существует → `Directory.Delete(staging, recursive: true)` (при любом отказе — спека §4.3 п.4).
6. Возврат `PutResult('"' + etag + '"')` (кавычки — P8).

**`DeleteObjectAsync`** (спека §4.3, канон 04 §4 п.5): `EnsureBucket`; `xl.meta` нет → успех (идемпотентность); `MoveToTrash(xl.meta)`; `MoveToTrash(dataDir)` — ПО ОТДЕЛЬНОСТИ, цели `.trash/<guid>`; опустевший `ObjectDir` (нет записей) → `Directory.Delete`; каталоги-префиксы не трогать.

**`DeleteObjectsAsync`**: `EnsureBucket`; по ключу: `try DeleteObjectAsync → DeletedKeyResult(key, true, null, null)`, `catch (ObjectStoreException ex) → (key, false, ex.Code.ToString(), ex.Message)`; `quiet` — Storage игнорирует (фильтрует App).

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: `XlObjectStore` c заглушками объектов (P6), хелперы Task 1–4 доступны.
  - Действие: `XlObjectStoreObjectTests` (AAA):
    - `PutGet_Roundtrip_ContentMetadataEtag` — PUT "hello", `ObjectUploadMetadata("text/plain", {"k":"v"})` → PutResult.ETag = md5("hello") hex В КАВЫЧКАХ (Get-часть кейса дописывается в Task 7);
    - `Put_DiskLayout_CanonicalPaths` — `<том>/b/k/xl.meta`, `<том>/b/k/<uuid>/part.1`, `.owns3.sys/{tmp,multipart,.trash,buckets,config}`, `volume.json` (критерий §10.3);
    - `Put_MissingBucket_NoSuchBucket`;
    - `Put_LengthMismatch_ThrowsIntegrity_StagingRemoved`;
    - `Put_BodyThrowsMidway_ObjectInvisible_TmpEmpty` — стрим, кидающий на 2-м чтении → исключение, tmp пуст, объект невидим (проверка через раскладку; Get-ассерт — Task 7) (критерий §10.4);
    - `Put_CommitVisibility_Immediate` — после PUT раскладка `xl.meta` + dataDir на месте;
    - `Put_Overwrite_LastContentWins_OldDataDirOrphaned` — PUT v1, PUT v2: в `ObjectDir` два uuid-каталога; старому `File.SetLastWriteTimeUtc(-2ч)` + `RunCleanupAsync` → остался ровно один (критерий §10.6);
    - `Put_NewKeyUnderLivePrefix_OverwriteScheme` — PUT `a/b`, затем PUT `a` → оба живы;
    - `Delete_MissingKey_IdempotentSuccess`; `Delete_RemovesObject`; `Delete_KeepsNestedSibling` (`a` и `a/b`: удалить `a` → `a/b` жив; критерий §10.5); `Delete_DirKey`;
    - `Delete_LeavesNoXlMeta_DataDirCleanedByVolume` — краш-имитация: удалить только `xl.meta` (dataDir остался) → `RunCleanupAsync` (mtime −2 ч) убирает dataDir;
    - `DeleteObjects_Mixed_PerKeyResults`; `DeleteObjects_MissingBucket_NoSuchBucket`;
    - `ParallelPut_SameKey_EveryCommitAtomic` — 4 параллельных PUT `Task.WhenAll` → раскладка консистентна: `xl.meta` читается, указывает ровно на один dataDir, orphan-каталоги допустимы;
    - `Put_KeyTooLongAfterEncoding_ThrowsInvalidArgument`;
    - `Put_SpecialKeys_Roundtrip` (`"a//b"`, `"."`, `"100%"`, `"lit%2E"`, `"x__XLDIR__"`, `"dir/"` — PUT и раскладка по кодированному пути; Get-часть — Task 7).
  - Выход: файл теста.
  - Проверка: фильтр `~XlObjectStoreObjectTests` → падение (методы-заглушки).
  - Связь со spec: §4.3 (жизненный цикл записи), §4.1 (XlObjectStore), §10.3–10.6.

- [ ] **Шаг 2. Реализовать**
  - Вход: красный тест.
  - Действие: `XlObjectStore.Objects.cs` (Get/Head/Attributes — остаются транзитными заглушками до Task 7/8); только BCL — ограничение 9.
  - Выход: коммит-цикл записи готов.
  - Проверка: весь юнит-проект → PASS.
  - Связь со spec: §4.3.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): PutObject/DeleteObject(s) — коммит-поинт rename xl.meta, staging, .trash-механика"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 3 §7.

---

### Task 7: XlObjectStore — Get/Head: conditional, Range, checksum (фаза 3)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Objects.cs` (+GetObjectAsync/HeadObjectAsync)
- Test: дополнение `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreObjectTests.cs`

**`GetObjectAsync(bucket, key, options, ct)` — последовательность (спека §4.4):**
1. `EnsureBucket`; `XlMetaFile.Read` каталога объекта (нет — `NoSuchKey`); `fromBackup` → `logger.LogWarning`.
2. `ConditionalEvaluator.Evaluate(options.Conditions, meta.ETag, meta.ModTime, ifModifiedSinceApplies: true)`: `PreconditionFailed` → бросок `ObjectStoreException(PreconditionFailed)`; `NotModified` → `new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null)`. Сравнение — с ETag всего объекта (P8: нормализация кавычек в `ETagMatches`).
3. `RangeResolver.Resolve(options.Range, options.IfRange, meta.ETag, meta.Size)` → `InvalidRange` бросок / null / диапазон.
4. **Checksum до отдачи**: полный проход SHA-256 по `<dataDir>/part.1`; ≠ `meta.ContentSha256` → `XlIntegrityException("checksum mismatch ...")` (клиенту байты не отдаются).
5. Body: Range == null → `new FileStream(part.1, FileMode.Open, FileAccess.Read, FileShare.Read)`; Range → тот же FileStream + `Seek(start, Begin)`. Возврат `ObjectContent(meta, body, false, range)`.

**`HeadObjectAsync(bucket, key, options, ct)`**: шаги 1–3 те же (`ifModifiedSinceApplies: true`); шаг 4 ПРОПУЩЕН (HEAD данных не читает — сверка не выполняется); Body = `Stream.Null`.

Хелпер `ToMetadata(string key, XlMetaRecord record) → ObjectMetadata(key, '"' + record.ETag + '"', record.Size, record.ModTime, record.ContentType, record.UserMetadata)` (ETag — в кавычках, P8).

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: Put-контур Task 6 готов; Get/Head — заглушки.
  - Действие: дополнить `XlObjectStoreObjectTests` (и дописать Get-ассерты кейсов Task 6):
    - `Get_Missing_NoSuchKey`;
    - `Get_Roundtrip_FullObject` (тело/метаданные/ETag в кавычках);
    - `Get_Range206_ReturnsSlice_TotalIsFullSize` — "abcdef", Range (2,3) → тело "cd", `AppliedByteRange(2,3,6)`;
    - `Get_RangeChecksummed_FullObjectHash_TamperedOutsideRange_ThrowsIntegrity` — порча байта 0 при Range 4-5 → `XlIntegrityException` ДО открытия тела (критерий §10.2);
    - `Get_TamperedData_ThrowsIntegrityBeforeBody`;
    - `Get_Conditional_IfMatch_Ok/Failed412`; `IfNoneMatch_Match_NotModifiedFlag` (Metadata заполнен, Body — Stream.Null); `IfModifiedSince_NotModified`; `IfUnmodifiedSince_Failed412`; `IfMatch_PriorityOverIfNoneMatch`;
    - `Get_Range_Suffix/EndClamped/StartOutOfBounds416/EmptyObject416/EmptyObjectFull200`;
    - `Get_IfRange_Match206/Mismatch200/DateForm200`;
    - `Head_ReturnsMetadata_WithoutReadingData` — порча part.1 → Head НЕ бросает; `Head_Range_AppliedByteRange`; `Head_IfNoneMatch_NotModified`;
    - `Get_SpecialKeys_Roundtrip` (Get по исходным ключам Task 6).
  - Выход: расширенный тест-класс.
  - Проверка: фильтр `~XlObjectStoreObjectTests` → падают Get/Head-кейсы.
  - Связь со spec: §4.4 (conditional/Range/checksum), §11 Q1.

- [ ] **Шаг 2. Реализовать**
  - Вход: красные кейсы.
  - Действие: Get/Head по последовательности выше.
  - Выход: чтение готово.
  - Проверка: весь юнит-проект → PASS.
  - Связь со spec: §4.4.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): Get/Head — conditional-таблица, Range/If-Range, полная SHA-256 сверка до отдачи"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 3 §7.

---

### Task 8: XlObjectStore — CopyObject + GetObjectAttributes (с conditional) (фаза 4)

**Файлы:**
- Create: `src/OwnS3.Storage/XlObjectStore.Copy.cs` (partial)
- Test: дополнение `XlObjectStoreObjectTests.cs` (или отдельный `XlObjectStoreCopyTests.cs` с той же фикстурой)

**`CopyObjectAsync(CopyRequest, ct)` (спека §4.3 Copy + решение Q2, реализация Q2 — fallback-ветка на net10, P11/вариант A):**
1. Источник: `EnsureBucket(SourceBucket)` → NoSuchBucket; `XlMetaFile.Read` → NoSuchKey.
2. Conditional источника: `Evaluate(request.SourceConditions, src.ETag, src.ModTime, ifModifiedSinceApplies: false)`; исход `NotModified` ИЛИ `PreconditionFailed` → бросок `PreconditionFailed` (copy — не GET/HEAD: If-None-Match-попадание = 412).
3. `src.Size > 5 ГБ` → `ObjectStoreException(EntityTooLarge)` (App не валидирует copy по длине).
4. Приёмник: `EnsureBucket(DestBucket)`. Staging: `tmp/<guid>/<newUuid>/part.1` — выделенный метод:

```csharp
// Единственная точка включения хардлинка: File.CreateHardLink — API .NET 11
// (в net10.0 отсутствует; P/Invoke запрещён ограничением 9). При переходе
// монорепо на net11 включается одной правкой здесь (P11, вариант A).
private static bool TryCreateHardLink(string sourcePath, string destPath) => false;
```

   Вызов: `if (!TryCreateHardLink(srcPart, stagedPart)) { побайтовое копирование srcPart → stagedPart; FileStream.Flush(flushToDisk: true); logger.LogWarning("хардлинк недоступен (нет BCL-API в net10.0) — побайтовое копирование"); }`. На net10 основная ветка — всегда fallback-копирование; семантика Q2 сохранена (link «постоянно неудачен»), ограничение 9 соблюдено, спека/каноны не меняются (arch требует атомарный коммит и ETag-семантику, механика данных не предписана).
5. `xl.meta`: `COPY` → ContentType/UserMetadata источника; `REPLACE` → `request.NewMetadata`; etag/sha256/Size — наследованы; VersionId новый; ModTime = now; коммит — `CommitStagedObjectAsync` (включая src == dest). Возврат `PutResult('"'+ src.ETag + '"')` (кавычки, P8).

Тестовый хук (внутренний): `internal Func<string, string, bool>? HardLinkProbe;` — при заданном подменяет `TryCreateHardLink` (проверка fallback-семантики и линк-ветки без реального хардлинка на net10).

**`GetObjectAttributesAsync(bucket, key, attributes, maxParts, partNumberMarker, conditions, ct)` (P10 — conditional по таблице канона 02 §1, GET-семантика):**
1. `EnsureBucket`; `XlMetaFile.Read` → NoSuchKey.
2. `ConditionalEvaluator.Evaluate(conditions, meta.ETag, meta.ModTime, ifModifiedSinceApplies: true)`: `PreconditionFailed` → бросок `ObjectStoreException(PreconditionFailed)` (412); `NotModified` → `new ObjectAttributesResult(null, ToMetadata(key, meta))` (304 с заголовками ETag/Last-Modified — их ставит хендлер, Task 5 п.4).
3. Успех: `ObjectPartsAttributes(PartsCount: 1, PartNumberMarker: partNumberMarker ?? 0, NextPartNumberMarker: null, MaxParts: maxParts ?? 1000, IsTruncated: false, Parts: [(1, meta.Size)])`; возврат `ObjectAttributesResult(new ObjectAttributes('"'+meta.ETag+'"', meta.Size, "STANDARD", parts), null)` — **ETag В КАВЫЧКАХ** (P8: хендлер ставит значение прямо в HTTP-заголовок `ETag`); фильтрацию по запрошенным делает App.

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: Put/Get/Head готовы (Task 6–7); сигнатура Attributes-контракта из Task 5.
  - Действие: кейсы:
    - `Copy_ContentAndInheritedMetadata_CopyDirective` (ContentType/UserMetadata источника; ETag копии == ETag источника — в кавычках);
    - `Copy_ReplaceDirective_UsesNewMetadata`;
    - `Copy_SurvivesSourceDeletion` — copy → удалить источник → Get копии без ошибок целостности (на net10 копия имеет собственные байты — ассерт не требует реального хардлинка; кейс сохраняется и для будущей net11-механики);
    - `Copy_DefaultNet10_AlwaysFallback_ByteCopy` — БЕЗ хука: дефолт net10 → побайтовое копирование, копия валидна, содержимое совпадает с источником (P11/вариант A);
    - `Copy_HardLinkProbeTrue_TakesLinkBranch` — `HardLinkProbe = (s, d) => { File.Copy(s, d); return true; }` (имитация успешного линка): линк-ветка отработала без собственного копирования, объект валиден (фиксация ветвления для будущей net11-механики);
    - `Copy_SourceConditions_Fail412` (If-Match не совпал; и If-None-Match совпал → 412, не 304);
    - `Copy_MissingSource_NoSuchKey`; `Copy_MissingDestBucket_NoSuchBucket`;
    - `Copy_SourceTooLarge_EntityTooLarge` — `XlMetaFile.Write` поверх xl.meta живого объекта с `Size = 5 ГБ + 1`;
    - `Copy_SameKey_MetadataRewrite` (src == dest, REPLACE);
    - `Attributes_ETagQuoted_SizeStorageClass_PartsSyntheticOne` (ETag в кавычках; Parts=[(1,size)], PartsCount=1, MaxParts=1000/переданный);
    - `Attributes_Conditional_IfMatchFailed_Throws412`; `Attributes_Conditional_IfNoneMatchMatch_ReturnsNotModifiedMetadata` (Attributes == null, NotModifiedMetadata.ETag в кавычках); `Attributes_Conditional_IfModifiedSince_NotModified`; `Attributes_Conditional_IfUnmodifiedSince_Failed412` (P10);
    - `Attributes_Missing_NoSuchKey`; `Attributes_MissingBucket_NoSuchBucket`.
  - Выход: тест-кейсы.
  - Проверка: фильтр `~XlObjectStoreObjectTests` → падают Copy/Attributes-кейсы.
  - Связь со spec: §4.3 (Copy), §4.1, §11 Q2 (реализация — fallback-ветка на net10, P11/вариант A), P10 (conditional Attributes).

- [ ] **Шаг 2. Реализовать**
  - Вход: красные кейсы.
  - Действие: `XlObjectStore.Copy.cs` по спецификации (conditional Attributes + ETag в кавычках).
  - Выход: Copy/Attributes готовы.
  - Проверка: весь юнит-проект → PASS.
  - Связь со spec: §4.3, фаза 4 §7, P10.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): CopyObject (fallback-копирование на net10, точка включения хардлинка для net11, COPY/REPLACE) + GetObjectAttributes (conditional 412/304, ETag quoted, синтетическая 1 часть)"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 4 §7.

---

### Task 9: ListWalker — листинги v1/V2/Versions, ленивый k-way merge (фаза 5)

**Файлы:**
- Create: `src/OwnS3.Storage/ListWalker.cs` (`Utf8ByteOrder` уже существует из Task 1)
- Modify: `src/OwnS3.Storage/XlObjectStore.List.cs` (partial: `ListObjectsAsync` = `EnsureBucket` + `new ListWalker(volume, logger).Walk(...)`)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreListTests.cs`

**Interfaces:**
```csharp
namespace OwnS3.Storage;

// Обход bucket-дерева ленивым k-way merge «следующих ключей поддеревьев»
// (спека §4.5). Порядок эмитов — строгий байтовый порядок полных ключей
// (Utf8ByteOrder из Task 1). Логгер — от XlObjectStore (warning при битом xl.meta).
internal sealed class ListWalker(XlVolume volume, ILogger? logger = null)
{
    public ListPage Walk(string bucket, ListQuery query);
}

// Внутренние типы (файл ListWalker.cs):
internal interface IKeyCursor
{
    string? PeekKey { get; }   // минимальный ещё не эмитнутый ключ поддерева; null — исчерпан
    void MoveNext();
}
internal sealed class DirectoryCursor : IKeyCursor
{
    // Курсор каталога с ключом-префиксом P ("" для корня бакета):
    //  1) собственный ключ P — если в каталоге есть xl.meta; P минимален в своём
    //     поддереве (любой другой ключ поддерева длиннее с тем же началом);
    //  2) далее дети: EnumerateDirectories → декодировать имена сегментов →
    //     лениво создать DirectoryCursor(P + имя + "/") для каждого;
    //     дети сливаются k-way merge по PeekKey (Utf8ByteOrder) — по ПОЛНЫМ
    //     ключам, не по именам: ребёнок "!z" даёт ключи P+"!z..." — раньше
    //     ребёнка "m" с P+"/m..." (байт '!'(0x21) < '/'(0x2F)).
    // PeekKey = минимум из [собственный ключ, PeekKey живых детей]; MoveNext
    // продвигает владельца текущего минимума.
}
```

**Алгоритм `Walk` (реализовать точно; решение пользователя major-2):**
1. `maxKeys = query.MaxKeys ?? 1000`; `maxKeys == 0` → `ListPage([], [], IsTruncated: false, null, null, 0)`.
2. **Маркер:** `after` = V2: декодированный `continuation-token` (`Base64Url.DecodeFromString` → UTF-8; битый → `XlInvalidArgumentException("Invalid continuation token")`) **??** `startAfter` (токен выигрывает); V1: `marker`; Versions: `marker` (хендлер t36 уже кладёт туда `key-marker`). `null`, если источник не задан.
3. Курсор корня = `DirectoryCursor("")`; цикл `PeekKey`:
   - `key == null` → конец; `after != null && Utf8ByteOrder.Compare(key, after) <= 0` → `MoveNext()`, продолжить (строго после маркера);
   - префикс: `!Utf8ByteOrder.StartsWith(key, prefix)` → `MoveNext()`, продолжить (корректность — фильтром; спуск по сегментам префикса — только оптимизация, опциональна по spec §9);
   - эмит: `rest = key[prefix.Length..]`; при `delimiter != null` и `rest` содержит delimiter — `commonPrefix = prefix + rest[..idx + delimiter.Length]`, дедуп против последнего эмитнутого префикса, эмит в CommonPrefixes; иначе — чтение `xl.meta` объекта (`XlMetaFile.Read`; при `XlIntegrityException` — `logger?.LogWarning(...)`, ключ пропускается — листинг не роняет один битый объект) и эмит `ListEntry(key, '"'+meta.ETag+'"', meta.Size, meta.ModTime)` (ETag в кавычках — P8, Contents/ETag как у Get/Head). Любой эмит увеличивает счётчик; при `count == maxKeys` → `IsTruncated = true`, стоп (`lastEmitted` = последний эмит — ключ или common prefix).
4. `NextMarker` (v1) = `lastEmitted` — только при `delimiter != null` (без delimiter клиент продолжает по последнему `Contents/Key`); `NextContinuationToken` (V2) = `Base64Url.EncodeToString(UTF8(lastEmitted))`.
5. `KeyCount = Contents.Count + CommonPrefixes.Count`; Variant (V1/V2/Versions) на обход НЕ влияет — обёртки добавляет App.

**Skip-оптимизация (необязательная; исправленный инвариант, ревью M-2):** при перечислении детей уровня (отсортированы по `Utf8ByteOrder(имя)`) ребёнок C пропускается, ТОЛЬКО если существует следующий брат R, лениво вычисленный `minKey(R) <= after` — И — имя C НЕ является байтовым префиксом имени R. Обоснование: если имя C не префикс R, существует позиция различия внутри имени C — любой ключ поддерева C меньше `minKey(R)` на этом байте → все ключи C ≤ after, skip корректен. Если имя C — байтовый префикс имени R (например, дети `m` и `m!z`), инвариант ЛОЖЕН: при after=`m!z` ключи `m0`, `mz` > after, но `minKey(R)=m!z ≤ after` — поддерево C пропускалось бы ошибочно; в этом случае C НЕ пропускается (emit-фильтр п.3 корректно отсечёт лишнее). Emit-фильтр и skip используют одну семантику сравнения (`Utf8ByteOrder`, полные ключи).

- [ ] **Шаг 1. Написать падающий тест**
  - Вход: Put-контур готов (фикстура с PUT-хелпером); `Utf8ByteOrder` и его тесты — из Task 1.
  - Действие: `XlObjectStoreListTests`:
    - `Order_StrictUtf8ByteLexicographic` — ключи `a`, `a!z`, `a/m`, `ab`, `b`, `Z`, `~`, `Á`, `😀` → точный порядок: `Z`(0x5A) < `a`(0x61) < `a!z`(0x21) < `a/m`(0x2F) < `ab`(0x62) < `b` < `~`(0x7E) < `Á`(0xC3 0x81) < `😀`; ассерт `ListEntry.ETag` — в кавычках (P8);
    - `Marker_AfterInsideMixedShape` — after=`a` → `["a!z","a/m","ab",...]`; after=`a!z` → `["a/m","ab",...]`; after=`a/m` → `["ab",...]`;
    - `Marker_SkipSiblingPrefixTrap` (контрпример ревью M-2) — ключи `m`, `m!z`, `m0`, `mz`; after=`m!z` → эмит ровно `m0`, `mz` (ни `m` < after, ни `m!z` == after);
    - `List_BrokenXlMeta_SkippedWithWarning` — объект с мусорным `xl.meta`+`xl.meta.bkp` не появляется в Contents и не роняет обход;
    - `NestedKeys_BothListed` (`a` + `a/b`); `DirKey_ListsAsKeyWithTrailingSlash` (`dir/`);
    - `Prefix_Filter`; `Prefix_PartialLastSegment` ("photos/2" матчит "photos/2", "photos/2x", "photos/2020/x" — байтовый префикс);
    - `Delimiter_CommonPrefixes_Dedup_CountedInMaxKeys` ("a/1","a/2","b" → CP ["a/"], Contents ["b"]; maxKeys=1 → только CP, truncated);
    - `Delimiter_DirKeyFoldsIntoItself` (ключ "photos/" + delimiter "/" → CP ["photos/"]);
    - `Marker_StrictlyAfter_SkipsSubtree` (after "a/x": "a/w" нет, "a/y" есть, "b" есть);
    - `ContinuationToken_Paging_Deterministic` (3 ключа, maxKeys=2 → токен → страница 2; повторный вызов — тот же результат);
    - `ContinuationToken_Invalid_InvalidArgument`; `StartAfter_IgnoredWhenTokenPresent`;
    - `MaxKeys_Zero_EmptyNotTruncated`;
    - `NextMarker_V1_OnlyWithDelimiter`;
    - `KeyCount_V2_SumsContentsAndPrefixes`;
    - `Versions_SameWalkAsV1`;
    - `MissingBucket_NoSuchBucket`; `EmptyBucket_EmptyPage`; `SpecialKeys_ListedDecoded` ("a//b", "100%").
  - Выход: файл тестов.
  - Проверка: фильтр `~XlObjectStoreListTests` → ошибка компиляции.
  - Связь со spec: §4.5 (листинги), §4.1 (ListWalker), канон 02 §3, решение пользователя major-2, ревью M-2.

- [ ] **Шаг 2. Реализовать**
  - Вход: красные тесты.
  - Действие: `ListWalker.cs` (k-way merge курсоров, эмит с чтением xl.meta, исправленный skip; logger — через конструктор), `XlObjectStore.List.cs` (передаёт `logger`).
  - Выход: листинги готовы.
  - Проверка: весь юнит-проект → PASS.
  - Связь со spec: §4.5.

- [ ] **Шаг 3. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "feat(owns3-storage): ListWalker — k-way merge, строгий UTF-8 байтовый порядок, ListEntry ETag quoted, префикс-безопасный skip"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 5 §7.

---

### Task 10: App-стыковка — DI, fail-fast, чистки, метрики диска, healthz, изоляция и переработка сценариев (фаза 6)

**Файлы:**
- Modify: `src/OwnS3.App/Program.cs`, `src/OwnS3.App/Pipeline/OwnS3Metrics.cs`
- Create: `src/OwnS3.App/Pipeline/VolumeCleanupService.cs`
- Delete: `src/OwnS3.Storage/NotWiredObjectStore.cs`, `src/tests/OwnS3.UnitTests/NotWiredObjectStoreTests.cs`
- Modify (изоляция): `src/tests/OwnS3.IntegrationTests/Api/OwnS3AppFactory.cs` (temp-том + Dispose), и классы `AccessScenarios.cs`, `AuthScenarios.cs`, `BodyIntegrityScenarios.cs`, `MetricsHealthScenarios.cs`, `RoutingScenarios.cs` — перевод на `IClassFixture<OwnS3AppFactory>` + разводка имён бакетов ПО КЕЙСАМ; `ErrorFormatScenarios.cs`/`FailFastScenarios.cs` остаются в `OwnS3TestCollection`

**Реализация:**
1. **`Program.cs` — DI ДО Build, Initialize ПОСЛЕ Build.** Вместо строки `builder.Services.AddSingleton<IObjectStore, NotWiredObjectStore>();` (до `builder.Build()`):

```csharp
// Домен: реестр учёток + том и объектный слой t37 (xl-хранение, канон 04).
builder.Services.AddSingleton<AccessKeyRegistry>();
builder.Services.AddSingleton(sp => new XlVolume(
    sp.GetRequiredService<IOptions<OwnS3Options>>().Value.DataDir,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<XlVolume>>()));
builder.Services.AddSingleton<IObjectStore>(sp => new XlObjectStore(
    sp.GetRequiredService<XlVolume>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<XlObjectStore>>()));
builder.Services.AddSingleton<BucketMetaStore>();
builder.Services.AddHostedService<VolumeCleanupService>();
builder.Services.AddSingleton<OwnS3Metrics>();
```

   После `var app = builder.Build();` (рядом с чтением `options`), ДО `app.RunAsync()`:

```csharp
// Том данных (arch/owns3/04 §3/§6): fail-fast до старта Kestrel — невалидный
// том = диагностика + ненулевой exit (спека §6.1). XlVolume уже в DI — берём
// тот же инстанс, который получат XlObjectStore и VolumeCleanupService.
var volume = app.Services.GetRequiredService<XlVolume>();
try { volume.Initialize(); }
catch (Exception ex)
{
    app.Logger.LogCritical(ex, "Том ownS3 {DataDir} невалиден — отказ старта", options.DataDir);
    Environment.Exit(1);
}
```

   `/healthz`: `app.MapGet("/healthz", () => volume.CheckHealth() ? Results.Ok() : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));` (замыкание на `volume`).
2. **`VolumeCleanupService : BackgroundService`** (ctor: `XlVolume volume, OwnS3Metrics metrics, ILogger<VolumeCleanupService> logger`): в `ExecuteAsync` СНАЧАЛА немедленный проход (старт-чистки уже сделаны `Initialize` — повтор безвреден; главное — метрики диска заполнены сразу, без 15-мин ожидания), затем `PeriodicTimer(TimeSpan.FromMinutes(15))`. Проход: stopwatch → `volume.RunCleanupAsync(ct)` → дисковые метрики (`DriveInfo(volume.Root)` — BCL: `TotalSize`, `TotalFreeSpace`; used = total − free; недоступен `DriveInfo` → warning, значения не трогать) → `logger.LogInformation("[CLEANUP] durationMs={Ms}", ...)`; исключение тика — лог Error, сервис живёт.
3. **`OwnS3Metrics`**: поля `internal long DiskUsedBytes/DiskTotalBytes` (Interlocked) + `meter.CreateObservableGauge<long>("ownS3.disk.used.bytes", () => DiskUsedBytes, unit: "By")` и `"ownS3.disk.total.bytes"` (финальные имена `ownS3_disk_used_bytes`/`ownS3_disk_total_bytes` — прецедент `ownS3_requests_total`).
4. **`OwnS3AppFactory`**: поле `public string TempVolumeDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-waf-" + Guid.NewGuid().ToString("N"));`; конфиг `["OwnS3:DataDir"] = TempVolumeDir`; `protected override void Dispose(bool)` → `base` + `Directory.Delete(TempVolumeDir, true)` в try/catch.
5. Удаление `NotWiredObjectStore.cs` + `NotWiredObjectStoreTests.cs`.
6. **Изоляция существующих классов (дух spec §6.5; правило ограничения 8):** двухуровневая:
   - том на класс: `IClassFixture<OwnS3AppFactory>` (снять `[Collection(OwnS3TestCollection.Name)]`) для `AccessScenarios`, `AuthScenarios`, `BodyIntegrityScenarios`, `MetricsHealthScenarios`, `RoutingScenarios`; в collection остаются только `ErrorFormatScenarios`/`FailFastScenarios` (их кейсы завершаются в конвейере ДО хендлера — 403/400/ValidateOnStart, том не мутируют и от содержимого не зависят);
   - имена бакетов ПО КЕЙСАМ (xUnit рандомизирует порядок кейсов внутри класса — том на класс НЕ достаточен): каждый МУТАЦИОННЫЙ кейс работает на уникальном имени бакета своего кейса (`b-create`, `b-admin`, `b-del`, …), НЕМУТАЦИОННЫЕ кейсы — на имени `bucket`, которое в этом классе никогда не создаётся; порядок кейсов тогда неважен, все ожидания детерминированы.

**Таблица смен исходов существующих кейсов (после изоляции; имена тестов обновить `ReachesStub → ReachesStorage`):**

| Кейс (файл) | Было | Стало |
|---|---|---|
| `RoutingScenarios` theory «15 строк t36» | 500 | строка `PUT /bucket` — мутационная — ВЫНОСИТСЯ из theory в отдельный кейс `CreateBucket_Admin_200` (бакет `b-create` → 200); остальные 14 строк немутационные, бакет `bucket` никогда не создаётся: `GET /` → 200 (статус не зависит от содержимого); `DELETE /bucket` → 404; `HEAD /bucket` → 404; `GET /bucket?prefix=x`/`?list-type=2`/`?versions` → 404 NoSuchBucket; `GET /bucket?uploads` → 500 (multipart-заглушка); `GET/HEAD/DELETE /bucket/key` → 404; `DELETE/GET /bucket/key?uploadId=u` → 500; `POST /bucket/key?uploads` → 500; `GET /bucket/key?attributes` → 404 |
| `RoutingScenarios.PutObject_ReachesObjectStore` | 500 | 404 (PUT /bucket/key — бакета нет) |
| `RoutingScenarios.CopyObject_...` | 500 | 404 (dest-бакета нет) |
| `RoutingScenarios.UploadPart_And_UploadPartCopy_...` | 500 | 500 (multipart-заглушки — не меняются) |
| `RoutingScenarios.DeleteObjects_...` | 500 | 404 (POST /bucket?delete — бакета нет) |
| `RoutingScenarios.CompleteMultipartUpload_...` | 500 | 500 (multipart) |
| `RoutingScenarios.EncodedKey_*` | 500+InternalError | 404+NoSuchBucket |
| `AccessScenarios.Admin_CreateBucket_...` | 500 | 200 — бакет `b-admin` (уникальное имя: не влияет на кейсы класса) |
| `AccessScenarios.Reader_GetObject/Writer_PutObject_...` | 500 | 404 NoSuchBucket (бакет `bucket` в классе не создаётся) |
| `AccessScenarios.Reader_MultipartListings_...` | 500 | 500 (multipart) |
| `AuthScenarios` ReachesStub-кейсы | 500 | 404 (GET /bucket/key и presigned GET — бакета нет) |
| `BodyIntegrityScenarios.PutObject_CorrectSha256_...` | 500 | 404 NoSuchBucket (сверка прошла — дошло до Storage) |
| `BodyIntegrityScenarios.ChunkedPut_ValidChain_...` | 500 | 404 NoSuchBucket (аналогично) |
| `BodyIntegrityScenarios.DeleteObjects_CorrectContentMd5_...` | 500 | 404 NoSuchBucket (POST /bucket?delete, MD5 верен — дошло до Storage) |
| `BodyIntegrityScenarios` негативные (BadDigest/InvalidRequest/порча) | 400 | не меняются (отказ в конвейере до Storage) |
| `MetricsHealthScenarios.StructuredLog/Metrics` | status=500, code="500" | status=404, code="404" |

**Новые тесты метрик диска (критерий §10.6):**
- юнит `src/tests/OwnS3.UnitTests/Storage/VolumeCleanupServiceTests.cs`: `StartAsync_ImmediatePass_UpdatesDiskGauges` — temp-том + `XlVolume.Initialize()` + `new OwnS3Metrics(new Meter("test"))` + сервис; `await svc.StartAsync(ct); await svc.StopAsync(ct);` → `metrics.DiskUsedBytes > 0 && metrics.DiskTotalBytes > 0` (первый проход выполняется до первого await — `StartAsync` возвращается после него);
- интеграционный кейс в `MetricsHealthScenarios`: `Metrics_ContainsDiskGaugeSeries` — GET `/metrics` → тело содержит `ownS3_disk_used_bytes` и `ownS3_disk_total_bytes` (Prometheus-экспортёр OTel — pull: значения собираются в момент scrape; немедленный проход сервиса уже заполнил gauge при старте хоста).

- [ ] **Шаг 1. Реализовать кодовые пункты 1–5**
  - Вход: Storage готов (Task 1–9); App ещё на NotWired.
  - Действие: Program/VolumeCleanupService/OwnS3Metrics/фабрика/удаление NotWired по пунктам 1–5 (DI-регистрация строго ДО `builder.Build()`, Initialize — после Build, до RunAsync).
  - Выход: App работает на реальном Storage; NotWired удалён.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App/OwnS3.App.csproj -c Release` → 0 warnings; юнит-проект (вкл. `VolumeCleanupServiceTests`) → PASS.
  - Связь со spec: §6.1–6.5, §4.7 (фон/метрики), §5.4.

- [ ] **Шаг 2. Прогнать интеграционные — ожидаемо красные**
  - Вход: код шага 1; сценарии ещё не переведены/не обновлены.
  - Действие: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`.
  - Выход: список падений.
  - Проверка: фактические падения соответствуют таблице исходов (расхождение сверх таблицы — СТОП, разбор по канону 02).
  - Связь со spec: §6.6 (переработка RoutingScenarios).

- [ ] **Шаг 3. Перевести классы на IClassFixture, развести имена бакетов по кейсам, обновить ожидания**
  - Вход: таблица исходов + правило имён п.6.
  - Действие: снять `[Collection]` и добавить `IClassFixture<OwnS3AppFactory>` у Access/Auth/BodyIntegrity/MetricsHealth/Routing; вынести мутационные кейсы на уникальные имена (`PUT /b-create`, `Admin PUT /b-admin`, …); немутационные оставить на `bucket`; обновить ассерты/имена по таблице; ErrorFormat/FailFast не трогать.
  - Выход: сценарии детерминированы независимо от порядка кейсов.
  - Проверка: юниты + интеграция полностью → PASS.
  - Связь со spec: §6.5, §8 (изоляция сценариев), ограничение 8.

- [ ] **Шаг 4. Коммит**
  - Вход: зелёные прогоны.
  - Действие: `git commit -m "feat(owns3-app): XlObjectStore в DI, fail-fast тома, VolumeCleanupService (немедленный старт + 15 мин), метрики диска, healthz по тому; NotWired удалён; изоляция том+имена по кейсам, реальные исходы сценариев"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 6 §7.

---

### Task 11: интеграционные сценарии полного HTTP-цикла (фаза 6)

**Файлы:**
- Create: `src/tests/OwnS3.IntegrationTests/Api/ObjectStorageScenarios.cs` (`IClassFixture<OwnS3AppFactory>` — свой temp-том, полный teardown фабрикой)
- Create: `src/tests/OwnS3.IntegrationTests/Api/HealthzVolumeScenarios.cs` (`IClassFixture`)

**Правило имён (ограничение 8):** каждый кейс `ObjectStorageScenarios` работает на уникальном имени бакета своего кейса (`b-cycle`, `b-range`, `b-cond`, `b-head`, `b-attrs`, `b-err`, `b-409`, `b-nonempty`, `b-ow`, `b-nested`, `b-list`; multipart-кейс — на `bucket`, никогда не создаваемом) — порядок кейсов внутри класса неважен.

**`ObjectStorageScenarios` (подписанный HTTP-цикл, спека §8; AAA; ключи с unicode/спецсимволами хотя бы в одном кейсе):**
- `FullCycle_PutHeadGetListCopyAttributesDeleteDeleteObjects` (бакет `b-cycle`) — создать бакет (admin) → PUT "hello" (+`x-amz-meta-a: b`, Content-Type) → HEAD (заголовки без тела) → GET (тело/ETag в кавычках/Last-Modified/user-meta) → List v1/V2/Versions (XML: Contents, KeyCount, CommonPrefixes) → Copy → GetObjectAttributes (`x-amz-object-attributes: ETag,ObjectSize,ObjectParts`; заголовок ETag — quoted) → Delete → DeleteObjects (XML `Delete`/`DeleteResult`, Quiet-кейс) → HeadBucket 200 → DeleteBucket 204;
- `Range_Get_206ContentRange_416InvalidRange` (`b-range`) — 206 + `Content-Range: bytes 2-3/6`, тело "cd"; `bytes=100-` → 416; пустой объект+Range → 416;
- `Conditional_Get_304WithoutBody_WithHeaders_412` (`b-cond`) — If-None-Match совпал → 304, заголовки ETag/Last-Modified, тела нет; If-Match не совпал → 412; If-Range: совпал → 206, не совпал → 200;
- `Head_Range_206WithoutBody` (`b-head`) — HEAD + Range: 206, `Content-Range`, Content-Length диапазона, тела нет;
- `Attributes_Conditional_304_412` (`b-attrs`) — GetObjectAttributes с If-None-Match совпал → 304 с ETag/Last-Modified без тела; If-Match не совпал → 412 (P10);
- `Errors_NoSuchBucket_NoSuchKey_XmlCodes` (бакет `bucket` не создаётся / ключ в `b-err`) — GET несозданного бакета → 404 `NoSuchBucket`; созданный бакет, ключа нет → `NoSuchKey`;
- `CreateBucket_Twice_409BucketAlreadyOwnedByYou` (`b-409`); `DeleteBucket_NonEmpty_409BucketNotEmpty` (`b-nonempty`); `DeleteBucket_Missing_404NoSuchBucket` (несозданное имя);
- `PutObject_Overwrite_EtagChanges_LastContentWins` (`b-ow`);
- `NestedKeys_PutGetDelete` (`b-nested`) — `a` и `a/b` оба читаются; DELETE `a` → `a/b` жив;
- `Listings_ByteOrder_Paging_Delimiter` (`b-list`) — ключи `a`, `a!z`, `a/m`, `ab` → строгий байтовый порядок (решение пользователя); `logs/x1..x4` + `root`: delimiter `/` → CP `logs/` + Contents `root`; V2 maxKeys=1 → NextContinuationToken → вторая страница; v1 marker;
- `Multipart_Operations_Still500` (бакет `bucket`, никогда не создаётся) — CreateMultipartUpload/UploadPart → 500 InternalError (критерий §10.8).
- `HealthzVolumeScenarios`: `Healthz_200OnValidVolume`; `Healthz_503WhenVolumeUnavailable` — удалить `factory.TempVolumeDir` → GET /healthz → 503 (фабрика удалит остальное в Dispose).

- [ ] **Шаг 1. Написать сценарии**
  - Вход: App на реальном Storage (Task 10).
  - Действие: два файла сценариев по перечню (правило имён бакетов — обязательное).
  - Выход: сценарии полного цикла.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release` → PASS; падение — диагностика по канонам 02/04, ослабление ассертов только с обоснованием.
  - Связь со spec: §8, §10.1–10.8, P10.

- [ ] **Шаг 2. Коммит**
  - Вход: зелёный прогон.
  - Действие: `git commit -m "test(owns3): интеграционные сценарии полного HTTP-цикла (байтовый порядок, Attributes-conditional) + healthz по тому"`.
  - Выход: коммит.
  - Проверка: `git log -1`.
  - Связь со spec: фаза 6 §7.

---

### Task 12: мерж-гейт (фаза 7)

- [ ] **Шаг 1. Полная сборка Release без warnings**
  - Вход: все задачи 0–11 закрыты.
  - Действие: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App/OwnS3.App.csproj -c Release` (тянет Protocol+Storage+Shared.Metrics).
  - Выход: артефакт сборки.
  - Проверка: 0 Error / 0 Warning (`TreatWarningsAsErrors` — любой warning = ошибка); в коде задач нет `unsafe`/`LibraryImport`/`DllImport` (ограничение 9).
  - Связь со spec: §7 фаза 7, §10.9.

- [ ] **Шаг 2. Юниты**
  - Вход: сборка чистая.
  - Действие: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release`.
  - Выход: отчёт прогона.
  - Проверка: все зелёные; t36-регрессий нет.
  - Связь со spec: §8.

- [ ] **Шаг 3. Интеграция**
  - Вход: юниты зелёные.
  - Действие: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`.
  - Выход: отчёт прогона.
  - Проверка: все зелёные.
  - Связь со spec: §8.

- [ ] **Шаг 4. Сверка критериев приёмки**
  - Вход: зелёные прогоны.
  - Действие: пройти чеклист спеки §10 п.1–10; каждый пункт — ссылкой на покрывающий тест (п.1 → T11 FullCycle; п.2 → T7/T11 conditional/Range/checksum + T8 Attributes-conditional; п.3 → T6 DiskLayout; п.4 → T6/T7; п.5 → T6/T9; п.6 → T3/T10 + кейсы метрик диска; п.7 → T11 Healthz; п.8 → T10/T11 multipart-500; п.9 → шаги 1–3; п.10 → границы не тронуты).
  - Выход: заполненный чеклист.
  - Проверка: ни один пункт не остался без теста; расхождение — назад в соответствующую задачу.
  - Связь со spec: §10.

- [ ] **Шаг 5. Границы и финальный коммит**
  - Вход: чеклист закрыт.
  - Действие: убедиться, что docker-контур НЕ поднимался (E2E — t39; спека §7.7); закоммитить остатки (если есть).
  - Выход: финальное состояние ветки.
  - Проверка: `git status` чистый; `git log` — задачи 1–11 коммитами.
  - Связь со spec: §7 фаза 7, §9.

- [ ] **Шаг 6. Отчёт**
  - Вход: ветка готова.
  - Действие: отчёт main-агенту; напоминание: мерж-гейт t37 при слитии в main снимает тег `t37-owns3-storage-objects` из `arch/roadmap/owns3.md` тем же коммитом (правило AGENTS.md roadmap).
  - Выход: сводка.
  - Проверка: отчёт содержит статус всех задач и результаты прогонов.
  - Связь со spec: §12.

---

## Саморевью плана rev.6 (выполнено при написании)

- **Решение пользователя (конфликт Task 8, вариант A):** `File.CreateHardLink` отсутствует в net10.0 (API .NET 11 Preview), P/Invoke/unsafe запрещены ограничением 9 — выбран вариант A: на net10 всегда fallback-ветка. Новое решение P11; Task 8 п.4 переписан: выделенный `TryCreateHardLink` (всегда `false` на net10, комментарий-точка включения для net11, сниппет приведён), основная ветка — побайтовое копирование `part.1` + `Flush(flushToDisk: true)` + warning «хардлинк недоступен»; обоснование (семантика Q2 сохранена, arch не меняется, ограничение 9 соблюдено) и цена (полный IO-проход на CopyObject) — в P11 и тексте Task 8. Тесты: `Copy_DefaultNet10_AlwaysFallback_ByteCopy` (дефолт, без хука) и `Copy_HardLinkProbeTrue_TakesLinkBranch` (имитация линка через хук); `Copy_SurvivesSourceDeletion` — без требования реального хардлинка; `HardLinkProbe` — тест-хюк как было. Коммит-сообщение Task 8 и «Технологии» шапки синхронизированы.

- **Приказ пользователя (BCL-only):** новое глобальное ограничение 9 (запрет unsafe/`AllowUnsafeBlocks`/`LibraryImport`/P-Invoke). Компонент `DirectoryFsync` и его сниппет/шаги УДАЛЕНЫ из Task 3 (заголовок/Files/Interfaces/шаг 2/коммит-сообщение очищены); вместо него — глобальное примечание Task 3 (fsync каталогов не выполняется: BCL-API нет, канон 04 / спека §4.3 п.2 допускают best-effort). Task 6: п.3 — fsync только файлов (`Flush(flushToDisk: true)`), fsync каталогов staging/dataDir убран; п.4 — место бывшего fsync каталога коммит-цикла (единственное упомянутое приказом место) — diagnostic-warning в лог. Task 2 — ссылка «fsync каталога после rename не выполняется (примечание Task 3)». P4 и «Технологии» шапки переписаны под BCL-only. Task 12 шаг 1 — проверка отсутствия `unsafe`/`LibraryImport`/`DllImport` в коде задач.
- **Проверка остаточных упоминаний:** `DriveInfo`, `Base64Url`, `FileStream.Flush(flushToDisk: true)` — BCL, разрешены; других P/Invoke/unsafe-механик в плане нет; `File.CreateHardLink` из механики исключён (P11 — точка включения на net11).
- **Контроль прежних замечаний (ревью-1/2/3):** порядок кодирования Task 1 (маркер после экранирования, 255 после приклейки), `Utf8ByteOrder` в Task 1 (зависимости T1→T4→T6/T9), logger `ListWalker(XlVolume, ILogger?)`, DI до Build/Initialize после (Task 10 п.1), ListBuckets сортировка (Task 4), формула маркеров (Task 9 п.2), orphan без xl.meta (Task 3), метрики диска (Task 10), ETag кавычки P8 (вкл. ListEntry/Attributes), байтовый порядок P9, изоляция том+имена по кейсам (ограничение 8, Task 10/11), skip-инвариант префикса (Task 9), спецслучай `%` декодирования (Task 1), fsync tmp-файла xl.meta (Task 2/P4), Attributes-conditional P10 (Task 5/8/11) — все сохранены.
- **Покрытие спеки:** фазы §7.0–7.7 → Task 0–12; критерии §10.1–10.10 — все со ссылками (Task 12 шаг 4).
- **Типы:** консистентность `ObjectContent`/`AppliedByteRange`/`ObjectReadOptions`/`ObjectAttributesResult`/`ToMetadata(string, XlMetaRecord)`/`Utf8ByteOrder`/`ListWalker(XlVolume, ILogger?)` проверена по всем задачам.
- **Плейсхолдеры:** отсутствуют.
