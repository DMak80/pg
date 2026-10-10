# t38-owns3-multipart: план реализации (rev.6: ревью 5 Фазы 4 — правится шапка XlObjectStoreBucketTests (grep-гейт достижим), EntityTooLarge-спуф через x-amz-decoded-content-length, сопутствующие комментарии XlVolume/Routing/Access/ObjectStorage; ранее rev.2–5 — скетч MultipartBodyStream, NoSuchUpload при битом parts.json, зачистка заглушек, List→Delete, пересчёты ассертов, посев 5 ГБ через xl.meta, AllUploads §9.9, непустое тело CreateUploadAsync, CP-пагинация без дублей, удаление заглушечных кейсов t36)

> **Для исполняющих агентов:** ОБЯЗАТЕЛЬНЫЙ саб-скилл: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять по задачам; шаги отмечаются чекбоксами (`- [ ]`). Каждый шаг несёт Вход/Действие/Выход/Проверку/Связь со spec.

**Цель:** полный цикл S3 Multipart Upload API — 7 операций (`CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`) на диске по канону `arch/owns3/04-storage.md` §5; многочастевое тело — первый класс чтения (checksum/Range/Attributes/Copy); заглушечные 500 исчезают.

**Архитектура:** `OwnS3.Storage` — чистая библиотека (без ASP.NET): новый `MultipartJournals` (записи/пути/атомарные JSON-журналы `uploads.json`/`parts.json`/`attempt.json`), `MultipartBodyStream` (составной поток чтения), реализация 7 методов в `XlObjectStore.Multipart.cs` поверх существующих `XlMetaFile`/`_commitLock`/`XlVolume`; чистка брошенных загрузок 24 ч — в `XlVolume` (старт + фон). `OwnS3.App` — передача инициатора/видимости в хендлерах, `LastModified` из `PutResult`; интеграционные HTTP-сценарии.

**Технологии:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions, `TimeProvider` (тесты — фиксированное время / `File.SetLastWriteTimeUtc`), `IncrementalHash` (MD5/SHA-256), только стандартные BCL API (без unsafe/P/Invoke — унаследованный запрет t37), fsync файлов — `Flush(flushToDisk: true)`, каталогов — нет (diagnostic-warning).

**Спека:** `docs/superpowers/2026-10-10-t38-owns3-multipart/spec.md` (план аргументирует от спеки; исполнители читают оба документа).

**Рабочая директория всех команд:** корень worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t38-owns3-multipart` (ветка `feat-t38-owns3-multipart`, коммитить свободно). Все пути ниже — от корня worktree.

---

## Глобальные ограничения (действуют в каждой задаче)

1. Сборка Release — 0 warnings (`TreatWarningsAsErrors=true`, `src/Directory.Build.props`); MD5 — только `IncrementalHash.CreateHash(HashAlgorithmName.MD5)` / `MD5.HashData` (зелёный прецедент t36/t37).
2. Комментарии/документация — по-русски; идентификаторы — на английском. Тесты — AAA-нотация в комментариях (`// Arrange`, `// Act`, `// Assert`).
3. `OwnS3.Protocol` НЕ трогаем (спека §8); роутинг/подпись/матрица прав/формат `xl.meta` (recordVersion 1) — без изменений. `arch/*` уже содержит правки спеки §3 (лежат в worktree: `git status` — `M arch/owns3/02-operations.md`, `M arch/owns3/04-storage.md`, `M arch/owns3/05-access-config.md`) — план новых arch-правок не делает; расхождение кода с каноном — блокирующее (СТОП и эскалация).
4. Никаких docker-контейнеров и хост-портов: юниты — temp-каталог, интеграция — in-memory WAF (`OwnS3AppFactory`). Никаких sleep-ожиданий: детерминированное время через `FixedTimeProvider` (`src/tests/OwnS3.UnitTests/TestVectors.cs`) и `File.SetLastWriteTimeUtc`.
5. `ObjectStoreErrorCode`/`ObjectStoreException` — без изменений (перечень закрыт; `InvalidArgument`-исходы Storage — через существующий `XlInvalidArgumentException`). `ObjectStoreUnavailableException` остаётся в коде (маппинг 500 в `S3Middleware`), источников больше нет.
6. Каждая задача заканчивается зелёным прогоном своих тестов и коммитом; тесты/сборка — с `DOTNET_CLI_UI_LANGUAGE=en`. Команды (паттерн t37): юниты — `dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter …`; интеграция — `dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release …`; сборка — `dotnet build src/OwnS3.App/OwnS3.App.csproj -c Release` (тянет Protocol+Storage) и/или build тест-проектов.
7. Изоляция тестов: юниты — temp-том на класс (`StoreFixture`, полный teardown в `DisposeAsync`); интеграция — том на WAF-фабрику, каждый мутационный кейс — уникальный бакет СВОЕГО кейса (`b-mp-<суффикс>`), немутационные — на никогда не создаваемом `bucket`.
8. E2E/docker не поднимается (граница t39); реальными клиентами (AWS SDK/`mc`) не проверяем.

## Проектные решения плана (в рамках формата спеки, применяются всеми задачами)

- **М1. Журналы — camelCase-JSON, атомарная запись.** Новый `internal static class MultipartJournals` (`src/OwnS3.Storage/MultipartJournals.cs`): записи `UploadJournalEntry(UploadId, Bucket, Key, InitiatedMs, AccessKey)`, `PartJournalEntry(PartNumber, ETag, Size, ModTimeMs)`, `AttemptMarker(DataDir)`; sha-путь `KeyDir(multipartRoot, bucket, key)` = `<root>/<sha256(UTF-8("<bucket>/<key>")) hex lowercase>`; запись любого журнала — tmp-файл рядом (`<name>.tmp`, `Flush(flushToDisk: true)`) → `File.Move(overwrite: true)`. Чтение: файла нет → пустой список; битый JSON → `JsonException` наружу (политику решает вызывающий). `parts.json` хранит etag hex БЕЗ кавычек.
- **М2. Инвариант разрешения загрузки** (спека §4.2): загрузка «жива» ⇔ в `uploads.json` есть запись с `(UploadId, Bucket, Key)` ∧ каталог `<uploadID>/` существует. Иначе — `NoSuchUpload` (в т.ч. битый `uploads.json`: warning в лог + `NoSuchUpload`). «Призрачная» запись (каталог утрачен) загрузкой не является — не блокирует `DeleteBucket`, чистится по возрасту.
- **М3. Все мутации журналов — под `_commitLock`** (тот же единственный лок процесса, что и коммиты t37): Create (добавление записи), UploadPart (rename части + upsert `parts.json` — согласованная пара), Complete (шаги 3–9), Abort. Тело части пишется в `part.N.tmp` БЕЗ лока (долго), fsync — до входа в лок. Чистка `XlVolume` пишет журналы БЕЗ лока — осознанная гонка: порог 24 ч делает коллизию с живой загрузкой невозможной по построению, а последствие гонки с Complete — «призрачная» запись, безвредная по М2, убираемая следующим проходом.
- **М4. `xl.meta` загрузки** — запись формата `XlMetaFile` t37: `VersionId` = `Guid.Parse(uploadId)`, `Size` = 0, `ETag` = `""`, `ContentSha256` = `""` (заполняются на Complete), `ModTime` = время инициации, `ContentType`/`UserMetadata` — из CreateMultipartUpload. Пишется ДО добавления записи в `uploads.json` (каталог без записи — невидим).
- **М5. Пороги/лимиты** (константы в `XlObjectStore.Multipart.cs`): минимальная часть (кроме последней) `MinPartSize = 5L * 1024 * 1024` (5 МиБ); максимальный размер части/копии `MaxPartSize = 5L * 1024 * 1024 * 1024` (5 ГБ — совпадает с `MaxCopySize` t37). Брошенная загрузка — `AbandonedUploadThreshold = TimeSpan.FromHours(24)` в `XlVolume`.
- **М6. Составной ETag** (канон 02 §1): `"<N>-<md5hex(concat этагов манифеста)>`, где concat — hex-строки без кавычек и разделителей в порядке манифеста, md5 — от ASCII-байтов этой строки (`MD5.HashData(Encoding.ASCII.GetBytes(concat))`). В `xl.meta` — без внешних кавычек (как обычный etag, конвенция P8 t37); в `CompleteResult.ETag` — в кавычках.
- **М7. Идемпотентность Complete через `attempt.json`**: каталог данных сборки (`dataDir`-uuid, он же будущий `VersionId` записи) создаётся в целевом каталоге объекта; маркер `{dataDir}` в каталоге загрузки. Битый/несовпадающий маркер → трактуется как отсутствующий (новая попытка; старый недособранный dataDir — orphan, чистится порогом 1 ч механизмом t37). `CompletePreCommitProbe` — тест-хук сбоя между переносом частей и коммитом (по образцу `HardLinkProbe` t37).
- **М8. Многочастевое тело — единая механика чтения.** Части закоммиченного объекта = перечисление `part.N` в dataDir (номер из имени, размер из файла), по возрастанию; простой PUT — ровно `part.1` (частный случай, «синтетическая одна часть» `GetObjectAttributes` получается автоматически). Полное тело — `MultipartBodyStream` (ленивые `FileStream`, `FileShare.Read`); срез Range — тот же класс с параметрами `(startOffset, limit)`. Checksum-сверка — SHA-256 последовательного прохода по всем файлам ДО отдачи байтов (200 и 206 — как t37).
- **М9. Видимость загрузок** (канон 05 §3): доменный тип `UploadVisibility` (`DomainTypes.cs`): `AllUploads | OwnedBy(string AccessKey)`; `ListParts`/`ListMultipartUploads` получают его параметром (контрактная правка §5 спеки); чужая загрузка для `OwnedBy` → `NoSuchUpload` (ListParts) / фильтр списка (ListMultipartUploads). Владелец — `AccessKey` записи `uploads.json`.
- **М10. Сортировка ListMultipartUploads**: лексикографическая по UTF-8 байтам ключа (`Utf8ByteOrder.Compare`), затем по `InitiatedMs`, затем по `UploadId` (стабильный полный порядок). Маркер-пара `(key-marker, upload-id-marker)`: записи с ключом < key-marker (байтово) пропускаются; при ключе == key-marker и заданном upload-id-marker — выдача начинается строго после записи-маркера В ПОРЯДКЕ ВЫДАЧИ; маркерная запись не найдена — ключи > key-marker (байтово), а при заданном `delimiter` дополнительно пропускаются ключи с префиксом key-marker (`Utf8ByteOrder.StartsWith`): маркер-CP (NextKeyMarker = сам префикс, см. ниже) продолжает выдачу СТРОГО после префикса — без повторного свёртывания уже выданного CommonPrefix. Свёртка `delimiter` → `CommonPrefixes` (по образцу `ListWalker`: вхождение delimiter в ключе ПОСЛЕ prefix, префикс = ключ до delimiter включительно; дедуп по порядку; CommonPrefixes считаются в `max-uploads`; `NextKeyMarker` при усечении на CP — сам префикс, `NextUploadIdMarker` — пусто).

---

### Task 0: контрольная arch-сверка (фаза 0 спеки)

**Файлы:** только чтение; правок нет (правки канонов уже в worktree: `git status` — `M arch/owns3/02-operations.md`, `M arch/owns3/04-storage.md`, `M arch/owns3/05-access-config.md`).

- [ ] **Шаг 1. Сверить пять правок §3 спеки с канонами**
  - Вход: спека §3; `arch/owns3/04-storage.md` §5, `arch/owns3/05-access-config.md` §3, `arch/owns3/02-operations.md` §1/§2/§5.
  - Действие: сверить по тексту канонов: (1) журнал `parts.json` — записи `{partNumber, etag hex-MD5 без кавычек, size, modTime}` в `<uploadID>/`, атомарное обновление вместе с заменой файла части; (2) записи `uploads.json` содержат исходные `bucket`/`key`; (3) маркер `attempt.json` с `dataDir` текущей сборки; (4) ListParts по чужому uploadId для read-only → 404 `NoSuchUpload` (фильтр видимости); (5) ETag CopyObject наследуется от источника (для multipart — составной `N-md5`).
  - Выход: подтверждение, что все пункты присутствуют в канонах дословно.
  - Проверка: любое расхождение — СТОП и эскалация координатору (arch-first правка), не «додумывать».
  - Связь со spec: §3, §6 фаза 0.

- [ ] **Шаг 2. Сверить механику операций с каноном 02 §5**
  - Вход: `arch/owns3/02-operations.md` §5 (семь операций), §1 (ETag).
  - Действие: убедиться в соответствии спеке: составной ETag `<N>-<md5(concat part-ETag'ов)>`; `InvalidPart` — несовпадение ETag/номера, несуществующая часть, часть < 5 МиБ не последняя; `InvalidPartOrder` — порядок манифеста (App, готово); повторный Complete после успеха и повторный Abort → `NoSuchUpload`; UploadPartCopy: `InvalidArgument` на невалидный/выходящий за размер диапазон, `EntityTooLarge` > 5 ГБ.
  - Выход: зафиксировано в журнале выполнения; коммита нет.
  - Проверка: тексты канонов содержат все формулировки.
  - Связь со spec: §4.2, §6 фаза 0.

---

### Task 1: MultipartJournals — записи, sha-пути, атомарные журналы (фаза 1)

**Файлы:**
- Create: `src/OwnS3.Storage/MultipartJournals.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/MultipartJournalsTests.cs`

**Interfaces (производит):**
```csharp
namespace OwnS3.Storage;

// internal — доступен OwnS3.UnitTests (InternalsVisibleTo). Журналы канона 04 §5:
// camelCase-JSON, атомарная запись tmp+fsync+rename (по образцу XlMetaFile.Write).
internal static class MultipartJournals
{
    // Запись журнала активных загрузок (uploads.json, канон 04 §5).
    internal sealed record UploadJournalEntry(string UploadId, string Bucket, string Key,
        long InitiatedMs, string AccessKey);

    // Запись журнала частей (parts.json, канон 04 §5; etag — hex-MD5 БЕЗ кавычек).
    internal sealed record PartJournalEntry(int PartNumber, string ETag, long Size, long ModTimeMs);

    // Маркер попытки Complete (attempt.json): dataDir текущей сборки.
    internal sealed record AttemptMarker(string DataDir);

    // <multipartRoot>/<sha256(UTF-8("<bucket>/<key>")) hex lowercase>.
    public static string KeyDir(string multipartRoot, string bucket, string key);

    public static string UploadsJsonPath(string keyDir);                 // <keyDir>/uploads.json
    public static string UploadDirPath(string keyDir, string uploadId);  // <keyDir>/<uploadId>
    public static string PartsJsonPath(string uploadDir);                // <uploadDir>/parts.json
    public static string AttemptJsonPath(string uploadDir);              // <uploadDir>/attempt.json

    public static string PartFileName(int partNumber);                   // "part.N"
    public static string PartTmpFileName(int partNumber);                // "part.N.tmp"

    // Чтение: файла нет → пустой список; пустой файл → пустой список;
    // JsonException — наружу (политику решает вызывающий: операции — warning
    // + NoSuchUpload; чистка — mtime-прокси).
    public static List<UploadJournalEntry> ReadUploads(string path);
    public static List<PartJournalEntry> ReadParts(string path);
    public static AttemptMarker? ReadAttempt(string uploadDir);          // нет/битый → null

    // Атомарная запись: <name>.tmp (Flush(flushToDisk: true)) → rename поверх.
    public static void WriteUploads(string path, IReadOnlyList<UploadJournalEntry> entries);
    public static void WriteParts(string path, IReadOnlyList<PartJournalEntry> entries);
    public static void WriteAttempt(string uploadDir, AttemptMarker marker);
}
```

- [ ] **Шаг 1. Написать failing-тесты roundtrip и путей**

`src/tests/OwnS3.UnitTests/Storage/MultipartJournalsTests.cs` (temp-каталог на класс, `IDisposable`-teardown):

```csharp
using System.Security.Cryptography;
using System.Text.Json;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Журналы multipart-контура: sha-пути, roundtrip uploads/parts/attempt,
// атомарность (tmp не остаётся), отсутствие файла = пустой список.
public class MultipartJournalsTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "owns3-mj-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог при любом исходе
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void KeyDir_IsSha256HexOfSlashJoinedBucketKey()
    {
        // Arrange
        var expected = Convert.ToHexString(SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes("bucket/key with space"))).ToLowerInvariant();

        // Act
        var dir = MultipartJournals.KeyDir(_root, "bucket", "key with space");

        // Assert: hex lowercase sha256, каталог внутри multipart-корня
        dir.Should().Be(Path.Combine(_root, expected));
    }

    [Fact]
    public void UploadsRoundtrip_WritesCamelCaseJsonAtomically()
    {
        // Arrange
        var keyDir = Path.Combine(_root, "abc");
        Directory.CreateDirectory(keyDir);
        var entries = new List<MultipartJournals.UploadJournalEntry>
        {
            new("id1", "b", "k", 1_728_000_000_000, "writer"),
        };

        // Act
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), entries);
        var read = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));

        // Assert: roundtrip; camelCase-поля; tmp-файл не остался
        read.Should().ContainSingle().Which.Should().Match<MultipartJournals.UploadJournalEntry>(e =>
            e.UploadId == "id1" && e.Bucket == "b" && e.Key == "k"
            && e.InitiatedMs == 1_728_000_000_000 && e.AccessKey == "writer");
        File.Exists(Path.Combine(keyDir, "uploads.json.tmp")).Should().BeFalse();
        JsonDocument.Parse(File.ReadAllText(MultipartJournals.UploadsJsonPath(keyDir)))
            .RootElement[0].EnumerateObject().Select(p => p.Name)
            .Should().BeEquivalentTo(["uploadId", "bucket", "key", "initiatedMs", "accessKey"]);
    }

    // Аналогично (по образцу шага выше): PartsRoundtrip_CamelCaseSortedByCaller —
    // [{partNumber, etag, size, modTimeMs}]; AttemptRoundtrip_DataDirPreserved —
    // {dataDir}; Read_MissingFile_EmptyList — uploads/parts; ReadAttempt_Missing_Null;
    // Read_EmptyFile_EmptyList; Read_BrokenJson_Throws (JsonException наружу).
}
```

- [ ] **Шаг 2. Прогнать тесты — убедиться в отказе компиляции**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~MultipartJournalsTests"`
Expected: ошибка компиляции — `MultipartJournals` не существует.

- [ ] **Шаг 3. Реализовать MultipartJournals**

`src/OwnS3.Storage/MultipartJournals.cs` — код по Interfaces-блоку выше; детали:

```csharp
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace OwnS3.Storage;

// Журналы multipart-контура (канон 04 §5): camelCase-JSON; атомарная запись
// tmp + fsync + rename (по образцу XlMetaFile.Write). internal — для
// XlVolume/XlObjectStore и юнит-тестов (InternalsVisibleTo).
internal static class MultipartJournals
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    internal sealed record UploadJournalEntry(string UploadId, string Bucket, string Key,
        long InitiatedMs, string AccessKey);

    internal sealed record PartJournalEntry(int PartNumber, string ETag, long Size, long ModTimeMs);

    internal sealed record AttemptMarker(string DataDir);

    // Каталог ключа: sha256 UTF-8("<bucket>/<key>"), hex lowercase (канон 04 §1/§5).
    public static string KeyDir(string multipartRoot, string bucket, string key) =>
        Path.Combine(multipartRoot, Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(bucket + "/" + key))).ToLowerInvariant());

    public static string UploadsJsonPath(string keyDir) => Path.Combine(keyDir, "uploads.json");
    public static string UploadDirPath(string keyDir, string uploadId) => Path.Combine(keyDir, uploadId);
    public static string PartsJsonPath(string uploadDir) => Path.Combine(uploadDir, "parts.json");
    public static string AttemptJsonPath(string uploadDir) => Path.Combine(uploadDir, "attempt.json");

    public static string PartFileName(int partNumber) => $"part.{partNumber}";
    public static string PartTmpFileName(int partNumber) => $"part.{partNumber}.tmp";

    public static List<UploadJournalEntry> ReadUploads(string path) => ReadList<MultipartJournals.UploadJournalEntry>(path);

    public static List<PartJournalEntry> ReadParts(string path) => ReadList<MultipartJournals.PartJournalEntry>(path);

    public static AttemptMarker? ReadAttempt(string uploadDir)
    {
        try
        {
            return JsonSerializer.Deserialize<AttemptMarker>(File.ReadAllText(AttemptJsonPath(uploadDir)), JsonOptions);
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            return null; // битый/утраченный маркер — новой попытке не мешает (М7)
        }
    }

    public static void WriteUploads(string path, IReadOnlyList<UploadJournalEntry> entries) =>
        WriteJsonAtomic(path, entries);

    public static void WriteParts(string path, IReadOnlyList<PartJournalEntry> entries) =>
        WriteJsonAtomic(path, entries);

    public static void WriteAttempt(string uploadDir, AttemptMarker marker) =>
        WriteJsonAtomic(AttemptJsonPath(uploadDir), marker);

    // Чтение списка: файла нет ИЛИ пустой → []; JsonException — наружу.
    private static List<T> ReadList<T>(string path)
    {
        if (!File.Exists(path))
            return [];
        var json = File.ReadAllText(path);
        return json.Length == 0 ? [] : JsonSerializer.Deserialize<List<T>>(json, JsonOptions) ?? [];
    }

    // Атомарная запись: <name>.tmp (fsync) → rename поверх. Каталог создаёт вызывающий.
    private static void WriteJsonAtomic<T>(string path, T value)
    {
        var tmp = path + ".tmp";
        using (var stream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions);
            stream.Write(bytes, 0, bytes.Length);
            stream.Flush(flushToDisk: true);
        }
        File.Move(tmp, path, overwrite: true);
    }
}
```

- [ ] **Шаг 4. Прогнать тесты — зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~MultipartJournalsTests"`
Expected: PASS (все кейсы класса).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/MultipartJournals.cs src/tests/OwnS3.UnitTests/Storage/MultipartJournalsTests.cs
git commit -m "feat(owns3): MultipartJournals — sha-пути и атомарные журналы uploads/parts/attempt (t38, фаза 1)"
```

---

### Task 2: чистки XlVolume — брошенные загрузки 24 ч, опустевшие sha-каталоги (фаза 1)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlVolume.cs` (метод `CleanupAbandonedUploads`, вызовы из `Initialize` и `RunCleanupAsync`)
- Test: `src/tests/OwnS3.UnitTests/Storage/MultipartCleanupTests.cs`

**Interfaces (производит):**
```csharp
// XlVolume: константа и метод (частные); поведение — старт и каждый фоновый проход
// чистят брошенные загрузки порогом 24 ч от initiation (запись) / mtime (прокси).
private static readonly TimeSpan AbandonedUploadThreshold = TimeSpan.FromHours(24);
private void CleanupAbandonedUploads(); // вызывается из Initialize() и RunCleanupAsync()
```

**Алгоритм `CleanupAbandonedUploads` (зафиксирован):**
1. `MultipartDir` не существует → выход.
2. Для каждого `<sha256>/`-каталога: прочитать `uploads.json` (нет файла → пусто; `JsonException` → warning в лог и работа дальше только по mtime-прокси).
3. Записи: `nowMs - InitiatedMs > 24 ч` → каталог загрузки `UploadDirPath(keyDir, UploadId)` (существует) → `MoveToTrash`; запись исключается из журнала. Прочие записи — остаются.
4. Каталоги загрузок БЕЗ оставшейся записи (журнал бит/утрачен/запись удалена): `timeProvider.GetUtcNow() - File.GetLastWriteTimeUtc(dir) > 24 ч` → `MoveToTrash`.
5. Журнал изменился (шаг 3 удалял записи) → перезапись `WriteUploads` (атомарно).
6. Журнал пуст И каталогов загрузок нет → удалить `<sha256>/` целиком (в нём только `uploads.json`).

- [ ] **Шаг 1. Написать failing-тесты**

`src/tests/OwnS3.UnitTests/Storage/MultipartCleanupTests.cs` — паттерн `XlVolumeTests`: том на класс, `TimeProvider.System`, возраст — `File.SetLastWriteTimeUtc` в прошлое; initiation-возраст — записью `InitiatedMs` в прошлое. Наполнение макета загрузок — руками через `MultipartJournals` (внутренний API):

```csharp
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Чистка брошенных загрузок (канон 04 §5–6): 24 ч от initiation-записи, mtime-прокси
// при битом журнале, опустевшие sha-каталоги удаляются; живые загрузки не трогаются.
public class MultipartCleanupTests : IDisposable
{
    private static readonly long NowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private readonly XlVolume _volume = new(
        Path.Combine(Path.GetTempPath(), "owns3-mc-" + Guid.NewGuid().ToString("N")), TimeProvider.System);
    private string Root => _volume.Root;

    public void Dispose()
    {
        // Teardown: temp-том при любом исходе
        if (Directory.Exists(Root))
            Directory.Delete(Root, recursive: true);
    }

    private string SeedUpload(string bucket, string key, string uploadId, long initiatedMs)
    {
        // Макет живой загрузки: каталог + xl.meta-заглушка + запись в журнале
        var keyDir = MultipartJournals.KeyDir(_volume.MultipartDir, bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        Directory.CreateDirectory(uploadDir);
        File.WriteAllText(Path.Combine(uploadDir, "part.1"), "data");
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key, initiatedMs, "writer"));
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
        return keyDir;
    }

    [Fact]
    public async Task RunCleanup_AbandonedByInitiation_TrashedAndRecordRemoved()
    {
        // Arrange: запись инициирована 25 ч назад; mtime каталога — свежий
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u1", NowMs - TimeSpan.FromHours(25).TotalMilliseconds);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));

        // Assert: запись удалена; каталог загрузки ушёл в .trash; sha-каталог удалён
        uploads.Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, "u1")).Should().BeFalse();
        Directory.Exists(keyDir).Should().BeFalse();
    }

    // Аналогично: RunCleanup_FreshUpload_Kept (запись моложе порога — на месте);
    // RunCleanup_BrokenUploadsJson_MtimeProxyTrashes (журнал перезаписан мусором,
    // mtime каталога в прошлом → каталог в .trash, битый файл удалён вместе с sha-каталогом);
    // RunCleanup_EmptyJournalNoDirs_RemovesShaDir; RunCleanup_GhostRecordWithoutDir_RemovedFromJournal
    // (запись есть, каталога нет — запись чистится по возрасту);
    // Initialize_RunsAbandonedCleanup (старт-чистка — как RunCleanup, на Initialize).
}
```

- [ ] **Шаг 2. Прогнать — отказ (метод не существует / каталоги не чистятся)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~MultipartCleanupTests"`
Expected: FAIL.

- [ ] **Шаг 3. Реализовать чистку в XlVolume**

В `src/OwnS3.Storage/XlVolume.cs`: константа `AbandonedUploadThreshold = TimeSpan.FromHours(24)`; метод `CleanupAbandonedUploads()` по алгоритму выше (использует `MultipartJournals`, `MoveToTrash`, `timeProvider`, `logger`); вызов добавить в `Initialize()` после `CleanAged()` и в `RunCleanupAsync()` после `CleanAged()`. Комментарий о гоночном окне — М3 (чистка вне `_commitLock`: порог 24 ч; «призрачная» запись безвредна по М2). Сопутствующая правка комментария: комментарий метода `RunCleanupAsync` (строки ~71–74, «Фоновый проход: ТОЛЬКО возрастные .trash и orphan-dataDir … multipart — t38») обновить — фон чистит и брошенные загрузки порогом 24 ч (убрать «multipart — t38», перечислить три чистки). Код скелета:

```csharp
// Брошенные загрузки — 24 ч от initiation (канон 04 §5–6): чистится стартом и
// каждым фоновым проходом. Без _commitLock XlObjectStore: порог 24 ч исключает
// коллизию с живой загрузкой; гонка с Complete может оставить «призрачную» запись
// (каталога уже нет) — она не разрешается как загрузка и уходит следующим проходом.
private void CleanupAbandonedUploads()
{
    if (!Directory.Exists(MultipartDir))
        return;
    var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
    foreach (var keyDir in Directory.EnumerateDirectories(MultipartDir))
    {
        var uploadsPath = MultipartJournals.UploadsJsonPath(keyDir);
        List<MultipartJournals.UploadJournalEntry> uploads;
        try
        {
            uploads = MultipartJournals.ReadUploads(uploadsPath);
        }
        catch (JsonException ex)
        {
            // Битый журнал (спека §4.1): warning; записи недоступны — только mtime-прокси
            logger?.LogWarning(ex, "Битый uploads.json в {KeyDir}: записи не читаются, каталоги чистятся по mtime", keyDir);
            uploads = [];
        }
        var kept = new List<MultipartJournals.UploadJournalEntry>();
        foreach (var entry in uploads)
        {
            if (nowMs - entry.InitiatedMs > AbandonedUploadThreshold.TotalMilliseconds)
                TrashIfExists(MultipartJournals.UploadDirPath(keyDir, entry.UploadId));
            else
                kept.Add(entry);
        }
        // Каталоги без живой записи (журнал бит/утрачен): mtime-прокси
        foreach (var uploadDir in Directory.EnumerateDirectories(keyDir))
            if (kept.All(e => e.UploadId != Path.GetFileName(uploadDir))
                && IsAged(uploadDir, AbandonedUploadThreshold))
                MoveToTrash(uploadDir);
        if (kept.Count != uploads.Count)
            MultipartJournals.WriteUploads(uploadsPath, kept);
        if (kept.Count == 0 && !Directory.EnumerateDirectories(keyDir).Any())
            Directory.Delete(keyDir, recursive: true); // остался только uploads.json
    }
}

private void TrashIfExists(string path)
{
    if (Directory.Exists(path))
        MoveToTrash(path);
}

// Обобщение существующего IsAged(path) на произвольный порог:
private bool IsAged(string path, TimeSpan threshold) =>
    timeProvider.GetUtcNow() - File.GetLastWriteTimeUtc(path) > threshold;
```

Существующий `IsAged(string)` заменить вызовом `IsAged(path, AgeThreshold)` (1 ч).

- [ ] **Шаг 4. Прогнать тесты чисток + регресс XlVolume**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~MultipartCleanupTests|FullyQualifiedName~XlVolumeTests"`
Expected: PASS (новые + существующие чистки не регрессировали).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlVolume.cs src/tests/OwnS3.UnitTests/Storage/MultipartCleanupTests.cs
git commit -m "feat(owns3): чистка брошенных multipart-загрузок 24 ч в XlVolume — старт+фон (t38, фаза 1)"
```

---

### Task 3: контракт Create + CreateMultipartUpload + AbortMultipartUpload (фаза 2)

**Файлы:**
- Modify: `src/OwnS3.Storage/IObjectStore.cs` (сигнатура Create — контрактная правка §5.1)
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs` (реализация Create/Abort, `ResolveUpload`-хелпер)
- Modify: `src/OwnS3.App/Handlers/MultipartHandlers.cs` (Create-хендлер: инициатор)
- Modify: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs` (удаление заглушечных multipart-кейсов t36 — иначе контрактная правка ломает компиляцию юнит-проекта)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (новый класс)

**Interfaces (производит):**
```csharp
// IObjectStore (правка §5.1 спеки):
Task<string> CreateMultipartUploadAsync(string bucket, string key,
    ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct);

// XlObjectStore.Multipart.cs (internal, переиспользуют Task 4–7):
internal const long MinPartSize = 5L * 1024 * 1024;               // 5 МиБ, кроме последней
internal const long MaxPartSize = 5L * 1024 * 1024 * 1024;        // 5 ГБ

// Разрешение загрузки (М2): запись (uploadId, bucket, key) ∧ каталог существует;
// visibility != null && запись чужая → NoSuchUpload. Битый журнал — warning + NoSuchUpload.
internal MultipartJournals.UploadJournalEntry ResolveUploadOrThrow(
    string bucket, string key, string uploadId, UploadVisibility? visibility);

// Чтение журнала ключа с warning-переводом JsonException → NoSuchUpload.
internal List<MultipartJournals.UploadJournalEntry> ReadUploadsOrThrow(string keyDir);
```

- [ ] **Шаг 1. Написать failing-тесты Create/Abort**

`src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` — на `StoreFixture` (том+store на класс, бакет `b`, `FixedTimeProvider`):

```csharp
using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Multipart-цикл XlObjectStore (канон 02 §5, 04 §5): раскладка, исходы,
// last-writer-wins, идемпотентность Complete/Abort, многочастевое чтение.
public class XlObjectStoreMultipartTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private static readonly ObjectUploadMetadata Meta =
        new("text/plain", new Dictionary<string, string> { ["k"] = "v" });

    private XlObjectStore Store => fixture.Store;
    private XlVolume Volume => fixture.Volume;
    private string Root => fixture.Root;

    // Минимальный валидный манифест Complete: N-1 частей по 5 МиБ + последняя произвольная.
    internal static byte[] PartBytes(int size, byte fill) =>
        Enumerable.Repeat(fill, size).ToArray();

    [Fact]
    public async Task Create_WritesLayoutAndJournal()
    {
        // Act
        var uploadId = await Store.CreateMultipartUploadAsync("b", "mp-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert: раскладка канона 04 §5 (uploadId — UUID N; xl.meta загрузки; запись журнала)
        Guid.TryParseExact(uploadId, "N", out _).Should().BeTrue();
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "mp-key");
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeTrue();
        File.Exists(Path.Combine(MultipartJournals.UploadDirPath(keyDir, uploadId), "xl.meta")).Should().BeTrue();
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
            .Should().ContainSingle().Which.Should().Match<MultipartJournals.UploadJournalEntry>(e =>
                e.UploadId == uploadId && e.Bucket == "b" && e.Key == "mp-key"
                && e.AccessKey == "writer");
        // xl.meta загрузки: метаданные будущего объекта, Size/ETag/Sha — пустые (М4)
        var record = XlMetaFile.Read(MultipartJournals.UploadDirPath(keyDir, uploadId), out _);
        record.ContentType.Should().Be("text/plain");
        record.UserMetadata.Should().ContainKey("k").WhoseValue.Should().Be("v");
        record.Size.Should().Be(0);
        record.ETag.Should().BeEmpty();
    }

    [Fact]
    public async Task Create_MissingBucket_NoSuchBucket()
    {
        // Act
        var act = async () => await Store.CreateMultipartUploadAsync("nob", "k", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task Abort_TrashesDirAndRecord_SecondAbort_NoSuchUpload()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "abort-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act
        await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId, TestContext.Current.CancellationToken);
        var second = async () => await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId,
            TestContext.Current.CancellationToken);

        // Assert: повторный Abort — NoSuchUpload (канон 02 §5); журнал пуст; каталог в .trash
        await second.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "abort-key");
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeFalse();
    }

    // Аналогично: Abort_UnknownUpload_NoSuchUpload; Create_TwoUploadsSameKey_BothListedInJournal;
    // BrokenUploadsJson_CreateThenUploadPart_NoSuchUpload (журнал перезаписан мусором:
    // UploadPart → NoSuchUpload — кейс закрыт в Task 4, здесь — Create не падает).
}
```

- [ ] **Шаг 2. Прогнать — отказ (сигнатура/реализация)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: ошибка компиляции — у `CreateMultipartUploadAsync` нет параметра `initiatorAccessKey`.

- [ ] **Шаг 3. Контракт + реализация Create/Abort + App-инициатор**

1. `src/OwnS3.Storage/IObjectStore.cs` — сигнатура Create по Interfaces-блоку (параметр `initiatorAccessKey` до `ct`).
2. `src/OwnS3.Storage/XlObjectStore.Multipart.cs` — заменить заглушку Create/Abort реализацией; удалить `ThrowUnavailable`-использование у них (заглушки остальных методов остаются до своих задач); константы `MinPartSize`/`MaxPartSize`; хелперы `ReadUploadsOrThrow`/`ResolveUploadOrThrow`:

```csharp
// CreateMultipartUpload (канон 02 §5/04 §5): каталог загрузки + xl.meta загрузки
// ДО записи в журнале (каталог без записи — невидим); запись журнала — под _commitLock.
public Task<string> CreateMultipartUploadAsync(string bucket, string key,
    ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct)
{
    EnsureBucket(bucket);
    var uploadId = Guid.NewGuid().ToString("N"); // формат N (канон 04 §5)
    var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
    var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
    Directory.CreateDirectory(uploadDir);
    var record = new XlMetaRecord(Guid.Parse(uploadId), 0, timeProvider.GetUtcNow(), "",
        metadata.ContentType, metadata.UserMetadata, EmptyHeaders, ""); // М4
    XlMetaFile.Write(uploadDir, record);
    lock (_commitLock)
    {
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key,
            timeProvider.GetUtcNow().ToUnixTimeMilliseconds(), initiatorAccessKey));
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
    }
    return Task.FromResult(uploadId);
}

// Abort (канон 02 §5/04 §5): под _commitLock — разрешение, удаление записи, каталог в .trash.
public Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct)
{
    EnsureBucket(bucket);
    var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
    lock (_commitLock)
    {
        var uploads = ReadUploadsOrThrow(keyDir);
        var entry = uploads.FirstOrDefault(e =>
            e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        if (!Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)))
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload); // М2: призрачная запись
        uploads.Remove(entry);
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
        volume.MoveToTrash(MultipartJournals.UploadDirPath(keyDir, uploadId));
    }
    return Task.CompletedTask;
}

// Журнал ключа: битый JSON — warning + NoSuchUpload (М2); файла нет — пусто.
internal List<MultipartJournals.UploadJournalEntry> ReadUploadsOrThrow(string keyDir)
{
    try
    {
        return MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
    }
    catch (JsonException ex)
    {
        _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: загрузки ключа недоступны", keyDir);
        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    }
}

// Разрешение загрузки: запись ∧ каталог (М2); чужая для OwnedBy — NoSuchUpload (М9).
internal MultipartJournals.UploadJournalEntry ResolveUploadOrThrow(
    string bucket, string key, string uploadId, UploadVisibility? visibility)
{
    var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
    var uploads = ReadUploadsOrThrow(keyDir);
    var entry = uploads.FirstOrDefault(e =>
        e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
        ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    if (!Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)))
        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    if (visibility is UploadVisibility.Owned(var owner) && entry.AccessKey != owner)
        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    return entry;
}
```

Ссылка на `UploadVisibility` требует его определения — на этом шаге он ещё не создан (Task 5). Поэтому в Task 3 `ResolveUploadOrThrow` вводится с параметром `UploadVisibility?` — тип создаётся ЗДЕСЬ минимально в `Domain/DomainTypes.cs` (М9), а использование в контракте List — Task 5. Так компиляция едина на каждом шаге.

3. `src/OwnS3.Storage/Domain/DomainTypes.cs` — добавить (М9):

```csharp
/// <summary>Видимость загрузок для ListParts/ListMultipartUploads (канон 05 §3):
/// все — read-write/admin; только свои — read-only (чужая = несуществующая).</summary>
public abstract record UploadVisibility
{
    public static readonly UploadVisibility AllUploads = new All();
    public static UploadVisibility OwnedBy(string accessKey) => new Owned(accessKey);
    public sealed record All : UploadVisibility;
    public sealed record Owned(string AccessKey) : UploadVisibility;
}
```

4. `src/OwnS3.App/Handlers/MultipartHandlers.cs` — `CreateMultipartUploadHandler`: вызов с инициатором:

```csharp
var uploadId = await Store.CreateMultipartUploadAsync(context.Bucket, context.Key, metadata,
    context.Request.Identity!.AccessKey, ct);
```

5. `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs` — удалить заглушечные multipart-кейсы t36 ЦЕЛИКОМ (существующий файл вызывает методы по старым сигнатурам — контрактная правка этого шага ломает его компиляцию, а семантика заглушек исчезает по §4.6.3/§9.1):
   - кейс `MultipartMethods_ThrowUnavailable` + фабрику `MultipartInvocations` (ассертят `ObjectStoreUnavailableException` от заглушек всех методов; вызовы `CreateMultipartUploadAsync(..., default)` без `initiatorAccessKey` и `ListPartsAsync(..., null, null, default)` без `visibility`);
   - кейс `UploadPart_DrainsBodyBeforeThrowing` + приватный `CountingStream` (drain-семантика удалена вместе с заглушкой UploadPart: после этого шага живой UploadPart отвечает `NoSuchUpload` до чтения тела);
   - комментарий шапки класса (строка ~8: «…сортировка ListBuckets, multipart-заглушки (NotWired-семантика).») — убрать перечисление «multipart-заглушки (NotWired-семантика)»: это единственный остаток вхождения паттерна grep-гейта Task 10 по `src/tests/OwnS3.UnitTests/Storage/`;
   - реальные исходы всех 7 методов покрыты кейсами `XlObjectStoreMultipartTests` (`Create_MissingBucket_NoSuchBucket`, `Abort_UnknownUpload_NoSuchUpload`, `UploadPart_UnknownUpload_NoSuchUpload`, `ListParts_UnknownUpload_NoSuchUpload` и далее по задачам) — дублировать их здесь не нужно.

- [ ] **Шаг 4. Прогнать тесты + сборка всего решения**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: PASS.
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreBucketTests"`
Expected: PASS (заглушечные кейсы удалены, бакетные — не тронуты).
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App/OwnS3.App.csproj -c Release`
Expected: 0 warnings, 0 errors (App скомпилирован с новой сигнатурой).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/IObjectStore.cs src/OwnS3.Storage/XlObjectStore.Multipart.cs \
        src/OwnS3.Storage/Domain/DomainTypes.cs src/OwnS3.App/Handlers/MultipartHandlers.cs \
        src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs \
        src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): CreateMultipartUpload/Abort на диске + UploadVisibility-тип + инициатор в App; удалены заглушечные multipart-кейсы t36 из XlObjectStoreBucketTests (t38, фаза 2)"
```

---

### Task 4: UploadPart — механика части last-writer-wins (фаза 2)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (использует):** `ResolveUploadOrThrow` (Task 3), `MultipartJournals` (Task 1), `_commitLock` (t37).

**Interfaces (производит):**
```csharp
// Чтение журнала частей загрузки (М2/спека §4.1): битый parts.json — warning в лог
// + NoSuchUpload — единообразно для UploadPart/ListParts/Complete. Файла нет — пусто.
internal List<MultipartJournals.PartJournalEntry> ReadPartsOrThrow(string uploadDir);
```

- [ ] **Шаг 1. Написать failing-тесты UploadPart**

```csharp
[Fact]
public async Task UploadPart_WritesPartFileAndJournal_EtagQuoted()
{
    // Arrange
    var uploadId = await Store.CreateMultipartUploadAsync("b", "up-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var payload = Encoding.UTF8.GetBytes("part-one");

    // Act
    var result = await Store.UploadPartAsync("b", "up-key", uploadId, 1,
        new MemoryStream(payload), payload.Length, TestContext.Current.CancellationToken);

    // Assert: ETag = md5(payload) hex В КАВЫЧКАХ (P8); part.1 на диске; запись parts.json
    var md5 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(payload)).ToLowerInvariant();
    result.ETag.Should().Be("\"" + md5 + "\"");
    result.LastModified.Should().Be(TestVectors.FixedTime);
    var uploadDir = MultipartJournals.UploadDirPath(
        MultipartJournals.KeyDir(Volume.MultipartDir, "b", "up-key"), uploadId);
    File.Exists(Path.Combine(uploadDir, "part.1")).Should().BeTrue();
    MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
        .Should().ContainSingle().Which.Should().Match<MultipartJournals.PartJournalEntry>(p =>
            p.PartNumber == 1 && p.ETag == md5 && p.Size == payload.Length);
    // tmp-файл не остался
    File.Exists(Path.Combine(uploadDir, MultipartJournals.PartTmpFileName(1))).Should().BeFalse();
}

[Fact]
public async Task UploadPart_SamePartNumber_LastWriterWins()
{
    // Arrange
    var uploadId = await Store.CreateMultipartUploadAsync("b", "lw-key", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act: две записи одного номера (первая больше)
    var first = Encoding.UTF8.GetBytes("first-version-of-part");
    var second = Encoding.UTF8.GetBytes("2nd");
    await Store.UploadPartAsync("b", "lw-key", uploadId, 1, new MemoryStream(first),
        first.Length, TestContext.Current.CancellationToken);
    await Store.UploadPartAsync("b", "lw-key", uploadId, 1, new MemoryStream(second),
        second.Length, TestContext.Current.CancellationToken);

    // Assert: файл и журнал — от последней записи (согласованная пара)
    var uploadDir = MultipartJournals.UploadDirPath(
        MultipartJournals.KeyDir(Volume.MultipartDir, "b", "lw-key"), uploadId);
    new FileInfo(Path.Combine(uploadDir, "part.1")).Length.Should().Be(second.Length);
    MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
        .Should().ContainSingle().Which.Size.Should().Be(second.Length);
}

// Аналогично: UploadPart_UnknownUpload_NoSuchUpload (несуществующий uploadId);
// UploadPart_AfterAbort_NoSuchUpload; UploadPart_LengthMismatch_XlIntegrity
// (contentLength=10, тело 3 байта → XlIntegrityException; tmp и журнал не изменились);
// UploadPart_TwoParts_JournalSortedByNumber (части 2, затем 1 — журнал [1,2]);
// UploadPart_BrokenPartsJson_NoSuchUpload (parts.json перезаписан мусором при живой
// записи → NoSuchUpload — единообразно с Complete/ListParts, спека §4.1).
```

- [ ] **Шаг 2. Прогнать — отказ (заглушка 500-типа)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.UploadPart"`
Expected: FAIL (ObjectStoreUnavailableException).

- [ ] **Шаг 3. Реализовать UploadPart**

`src/OwnS3.Storage/XlObjectStore.Multipart.cs` — заменить заглушку (и её `DrainAsync`):

```csharp
// UploadPart (канон 02 §5/04 §5): тело → part.N.tmp БЕЗ лока (fsync), затем под
// _commitLock — согласованная пара rename части + upsert parts.json (М3).
public async Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
    Stream body, long contentLength, CancellationToken ct)
{
    EnsureBucket(bucket);
    ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
    var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
    var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
    var tmpPath = Path.Combine(uploadDir, MultipartJournals.PartTmpFileName(partNumber));
    // Тело: MD5-инкремент + счётчик + fsync (длина ≠ contentLength → XlIntegrityException,
    // остаточный случай — конвейер уже проверил тело)
    string etagHex;
    long total;
    using (var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5))
    using (var file = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
    {
        var buffer = new byte[64 * 1024];
        total = 0;
        int read;
        while ((read = await body.ReadAsync(buffer, ct)) > 0)
        {
            file.Write(buffer, 0, read);
            md5.AppendData(buffer, 0, read);
            total += read;
        }
        file.Flush(flushToDisk: true);
        etagHex = Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant();
    }
    if (total != contentLength)
        throw new XlIntegrityException($"Фактическая длина части {total} не совпадает с заявленной {contentLength}");
    var modTime = timeProvider.GetUtcNow();
    await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
    lock (_commitLock)
    {
        File.Move(tmpPath, Path.Combine(uploadDir, MultipartJournals.PartFileName(partNumber)),
            overwrite: true);
        var parts = ReadPartsOrThrow(uploadDir);
        parts.RemoveAll(p => p.PartNumber == partNumber);
        parts.Add(new MultipartJournals.PartJournalEntry(partNumber, etagHex, total,
            modTime.ToUnixTimeMilliseconds()));
        parts.Sort((left, right) => left.PartNumber.CompareTo(right.PartNumber));
        MultipartJournals.WriteParts(MultipartJournals.PartsJsonPath(uploadDir), parts);
    }
    return new PutResult('"' + etagHex + '"', modTime);
}

// Журнал частей загрузки: битый parts.json (JsonException) — warning + загрузка
// недоступна (NoSuchUpload) — единообразно с ListParts/Complete (спека §4.1: потеря
// целостности журнала — загрузка недоступна). Файла нет — пустой список.
internal List<MultipartJournals.PartJournalEntry> ReadPartsOrThrow(string uploadDir)
{
    try
    {
        return MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
    }
    catch (JsonException ex)
    {
        _logger.LogWarning(ex, "Битый parts.json загрузки в {UploadDir}: загрузка недоступна", uploadDir);
        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    }
}
```

Примечание: конкурентные чтения `parts.json` в этом же локе — согласованы (М3). Битый `parts.json` → `NoSuchUpload` (через `ReadPartsOrThrow`) — то же исход, что у ListParts/Complete: загрузка с потерянным журналом частей недоступна целиком (спека §4.1), клиенту единая семантика «NoSuchUpload», каталог чистится по возрасту 24 ч.

- [ ] **Шаг 4. Прогнать тесты**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: PASS.

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlObjectStore.Multipart.cs src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): UploadPart — часть+журнал согласованными rename под _commitLock (t38, фаза 2)"
```

---

### Task 5: ListParts + ListMultipartUploads + DeleteBucket-гейт (фаза 2)

**Файлы:**
- Modify: `src/OwnS3.Storage/IObjectStore.cs` (контрактные правки §5.2/§5.3: `ListPartsAsync` + `UploadVisibility`; `UploadsQuery` + поле)
- Modify: `src/OwnS3.Storage/Domain/DomainTypes.cs` (`UploadsQuery` + `UploadVisibility Visibility`)
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs` (оба листинга)
- Modify: `src/OwnS3.Storage/XlObjectStore.Buckets.cs` (DeleteBucket — проверка живых загрузок)
- Modify: `src/OwnS3.App/Handlers/MultipartHandlers.cs` (видимость из policy в ListParts/ListMultipartUploads)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (производит):**
```csharp
// IObjectStore (правки §5.2–§5.3 спеки):
Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
    int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct);
Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct);

// DomainTypes: UploadsQuery расширяется (последний параметр — до закрывающей скобки):
public sealed record UploadsQuery(string? Prefix, string? Delimiter, string? KeyMarker,
    string? UploadIdMarker, int? MaxUploads, string? EncodingType, UploadVisibility Visibility);
```

- [ ] **Шаг 1. Написать failing-тесты листингов и DeleteBucket**

```csharp
[Fact]
public async Task ListParts_PaginationAndMarker()
{
    // Arrange: части 1,2,3 — журнал по возрастанию
    var uploadId = await Store.CreateMultipartUploadAsync("b", "lp-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    for (var n = 1; n <= 3; n++)
    {
        var bytes = Encoding.UTF8.GetBytes("p" + n);
        await Store.UploadPartAsync("b", "lp-key", uploadId, n, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);
    }

    // Act: страница maxParts=2, затем продолжение за маркером
    var page1 = await Store.ListPartsAsync("b", "lp-key", uploadId, 2, null,
        UploadVisibility.AllUploads, TestContext.Current.CancellationToken);
    var page2 = await Store.ListPartsAsync("b", "lp-key", uploadId, 2, page1.NextPartNumberMarker,
        UploadVisibility.AllUploads, TestContext.Current.CancellationToken);

    // Assert: усечение, маркер = последний выданный номер; ETag в кавычках; порядок номеров
    page1.Parts.Select(p => p.PartNumber).Should().Equal(1, 2);
    page1.IsTruncated.Should().BeTrue();
    page1.NextPartNumberMarker.Should().Be(2);
    page2.Parts.Select(p => p.PartNumber).Should().Equal(3);
    page2.IsTruncated.Should().BeFalse();
    page1.Parts[0].ETag.Should().StartWith("\"").And.EndWith("\"");
    page1.Parts[0].LastModified.Should().Be(TestVectors.FixedTime);
}

[Fact]
public async Task ListParts_ForeignUploadForReadOnly_NoSuchUpload()
{
    // Arrange: загрузка владельца writer
    var uploadId = await Store.CreateMultipartUploadAsync("b", "ro-key", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act: read-only чужой доступ
    var act = async () => await Store.ListPartsAsync("b", "ro-key", uploadId, null, null,
        UploadVisibility.OwnedBy("reader"), TestContext.Current.CancellationToken);

    // Assert: неотличимо от несуществующей (канон 05 §3, Q2 спеки)
    await act.Should().ThrowAsync<ObjectStoreException>()
        .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
}

[Fact]
public async Task ListMultipartUploads_FiltersMarkersAndVisibility()
{
    // Arrange: ключи a/x (ДВЕ загрузки writer), a/y (writer + чужая admin), zzz;
    // чужой бакет «other» (создаётся явно — фикстура создаёт только "b") — не попадает
    await Store.CreateBucketAsync("other", TestContext.Current.CancellationToken);
    var u1 = await Store.CreateMultipartUploadAsync("b", "a/x", Meta, "writer",
        TestContext.Current.CancellationToken);
    var u1b = await Store.CreateMultipartUploadAsync("b", "a/x", Meta, "writer",
        TestContext.Current.CancellationToken);
    var u2 = await Store.CreateMultipartUploadAsync("b", "a/y", Meta, "writer",
        TestContext.Current.CancellationToken);
    var foreign = await Store.CreateMultipartUploadAsync("b", "a/y", Meta, "admin",
        TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("b", "zzz", Meta, "writer",
        TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("other", "a/x", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act 1: prefix "a/" + read-only(writer) видит только свои
    var page = await Store.ListMultipartUploadsAsync("b", new UploadsQuery("a/", null, null, null,
        null, null, UploadVisibility.OwnedBy("writer")), TestContext.Current.CancellationToken);

    // Assert 1: свои загрузки своих ключей; чужая (admin) отфильтрована; порядок ключей байтовый.
    // Выдача — ТРИ записи: обе загрузки a/x (ключ повторяется) + a/y. Порядок ключей
    // детерминирован (М10: байтовая сортировка); НЕ детерминирован только порядок
    // u1/u1b внутри a/x (равные initiatedMs при FixedTimeProvider, доопределение по
    // uploadId) — их ассерт множеством, без зависимости от порядка
    page.Uploads.Select(u => u.Key).Should().Equal("a/x", "a/x", "a/y");
    page.Uploads.Where(u => u.Key == "a/x").Select(u => u.UploadId)
        .Should().Contain(u1).And.Contain(u1b);
    page.Uploads.Select(u => u.UploadId).Should().NotContain(foreign); // фильтр видимости (М9)
    page.CommonPrefixes.Should().BeEmpty();

    // Act 2: та же выборка под read-write/admin — ВСЕ загрузки бакета (§9.9, позитивная ветка)
    var allPage = await Store.ListMultipartUploadsAsync("b", new UploadsQuery("a/", null, null, null,
        null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

    // Assert 2: четыре записи — обе a/x, обе a/y (u2 + чужая foreign); порядок ключей
    // детерминирован, порядок одно-ключевых загрузок — нет (см. Assert 1)
    allPage.Uploads.Should().HaveCount(4);
    allPage.Uploads.Select(u => u.Key).Should().Equal("a/x", "a/x", "a/y", "a/y");
    allPage.Uploads.Select(u => u.UploadId).Should().Contain(foreign); // admin-загрузка видна (§9.9)
}

[Fact]
public async Task ListMultipartUploads_Delimiter_CommonPrefixesCountedInMax()
{
    // Arrange: ключи a/1, a/2, c (delimiter="/")
    await Store.CreateMultipartUploadAsync("b", "a/1", Meta, "writer", TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("b", "a/2", Meta, "writer", TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("b", "c", Meta, "writer", TestContext.Current.CancellationToken);

    // Act
    var page = await Store.ListMultipartUploadsAsync("b", new UploadsQuery(null, "/", null, null,
        2, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

    // Assert: CP "a/" + ключ "c" = 2 позиции; усечение; NextKeyMarker — сам префикс
    page.CommonPrefixes.Select(p => p.Prefix).Should().Equal("a/");
    page.Uploads.Select(u => u.Key).Should().Equal("c");
    page.IsTruncated.Should().BeFalse();
}

[Fact]
public async Task ListMultipartUploads_CpMarkerPagination_NoDuplicatePrefixes()
{
    // Arrange: ключи a/1, a/2, c; delimiter "/"; страница из ОДНОЙ позиции — первая
    // страница отдаёт только CP "a/" и усекается с NextKeyMarker = сам префикс
    await Store.CreateMultipartUploadAsync("b", "a/1", Meta, "writer", TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("b", "a/2", Meta, "writer", TestContext.Current.CancellationToken);
    await Store.CreateMultipartUploadAsync("b", "c", Meta, "writer", TestContext.Current.CancellationToken);

    // Act: страница 1 → продолжение по маркеру-CP (без upload-id-marker)
    var page1 = await Store.ListMultipartUploadsAsync("b", new UploadsQuery(null, "/", null, null,
        1, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);
    var page2 = await Store.ListMultipartUploadsAsync("b", new UploadsQuery(null, "/",
        page1.NextKeyMarker, page1.NextUploadIdMarker, 1, null, UploadVisibility.AllUploads),
        TestContext.Current.CancellationToken);

    // Assert: страница 1 = CP "a/" + усечение, NextKeyMarker = "a/"; страница 2 —
    // БЕЗ дубля CP (ключи префикса "a/" пропущены префикс-фильтром М10), продолжение
    // ключом "c", полное
    page1.CommonPrefixes.Select(p => p.Prefix).Should().Equal("a/");
    page1.Uploads.Should().BeEmpty();
    page1.IsTruncated.Should().BeTrue();
    page1.NextKeyMarker.Should().Be("a/");
    page2.CommonPrefixes.Should().BeEmpty();
    page2.Uploads.Select(u => u.Key).Should().Equal("c");
    page2.IsTruncated.Should().BeFalse();
}

[Fact]
public async Task DeleteBucket_LiveUpload_BucketNotEmpty_AfterAbort_Ok()
{
    // Arrange
    await Store.CreateBucketAsync("bb", TestContext.Current.CancellationToken);
    var uploadId = await Store.CreateMultipartUploadAsync("bb", "k", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act / Assert: живая загрузка блокирует удаление
    var del = async () => await Store.DeleteBucketAsync("bb", TestContext.Current.CancellationToken);
    await del.Should().ThrowAsync<ObjectStoreException>()
        .Where(e => e.Code == ObjectStoreErrorCode.BucketNotEmpty);
    await Store.AbortMultipartUploadAsync("bb", "k", uploadId, TestContext.Current.CancellationToken);
    await del.Should().NotThrowAsync();
}

// Аналогично: ListParts_UnknownUpload_NoSuchUpload; ListParts_BrokenPartsJson_NoSuchUpload
// (parts.json перезаписан мусором при живой записи → NoSuchUpload, спека §4.1);
// ListMultipartUploads_KeyMarkerAndUploadIdMarker_ContinuesAfterPair;
// ListMultipartUploads_BrokenJournal_SkippedWithWarning (ключ с битым журналом
// пропускается, остальные ключи листятся).
```

- [ ] **Шаг 2. Прогнать — отказ компиляции (сигнатуры)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: ошибка компиляции — `ListPartsAsync` без `visibility`, `UploadsQuery` без `Visibility`.

- [ ] **Шаг 3. Реализовать листинги, DeleteBucket-гейт, App-видимость**

1. `DomainTypes.cs` — расширить `UploadsQuery` полем `UploadVisibility Visibility` (последним).
2. `IObjectStore.cs` — `ListPartsAsync` + параметр `UploadVisibility visibility` перед `ct`.
3. `XlObjectStore.Multipart.cs` — реализации (заглушки заменить):

```csharp
// ListParts (канон 02 §5): части из parts.json по возрастанию; marker — строго после;
// maxParts null = 1000; битый parts.json при живой записи → NoSuchUpload (спека §4.1,
// ReadPartsOrThrow из Task 4 — единый исход с UploadPart/Complete).
public Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
    int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct)
{
    EnsureBucket(bucket);
    ResolveUploadOrThrow(bucket, key, uploadId, visibility);
    var uploadDir = MultipartJournals.UploadDirPath(
        MultipartJournals.KeyDir(volume.MultipartDir, bucket, key), uploadId);
    var parts = ReadPartsOrThrow(uploadDir);
    var marker = partNumberMarker ?? 0;
    var selected = parts.Where(p => p.PartNumber > marker)
        .OrderBy(p => p.PartNumber)
        .Take(maxParts ?? 1000)
        .Select(p => new PartEntry(p.PartNumber, '"' + p.ETag + '"', p.Size,
            DateTimeOffset.FromUnixTimeMilliseconds(p.ModTimeMs)))
        .ToList();
    var truncated = parts.Count(p => p.PartNumber > marker) > selected.Count;
    return Task.FromResult(new PartsPage(selected, truncated,
        truncated ? selected[^1].PartNumber : null));
}

// ListMultipartUploads (канон 02 §5, М10): сбор всех записей бакета из sha-каталогов,
// сортировка ключ(UTF-8 байты)→initiatedMs→uploadId, фильтры prefix/маркер-пара,
// свёртка delimiter, max-uploads с подсчётом CommonPrefixes.
public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct)
{
    EnsureBucket(bucket);
    var all = new List<MultipartJournals.UploadJournalEntry>();
    if (Directory.Exists(volume.MultipartDir))
        foreach (var keyDir in Directory.EnumerateDirectories(volume.MultipartDir))
        {
            try
            {
                all.AddRange(MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
                    .Where(e => e.Bucket == bucket));
            }
            catch (JsonException ex)
            {
                // Битый журнал одного ключа не валит листинг бакета (спека §4.1)
                _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: ключ пропущен", keyDir);
            }
        }
    // Видимость (М9): read-only — только свои
    all = all.Where(e => query.Visibility is not UploadVisibility.Owned(var owner)
                         || e.AccessKey == owner).ToList();
    // Сортировка М10
    all.Sort((l, r) =>
    {
        var byKey = Utf8ByteOrder.Compare(l.Key, r.Key);
        if (byKey != 0) return byKey;
        var byTime = l.InitiatedMs.CompareTo(r.InitiatedMs);
        return byTime != 0 ? byTime : string.CompareOrdinal(l.UploadId, r.UploadId);
    });
    // prefix
    if (query.Prefix is not null)
        all = all.Where(e => Utf8ByteOrder.StartsWith(e.Key, query.Prefix)).ToList();
    // маркер-пара (М10): строго после записи-маркера в порядке выдачи; маркер-CP
    // (NextKeyMarker = префикс) — при delimiter ключи этого префикса пропускаются
    // ЦЕЛИКОМ: уже выданный CommonPrefix не свёртывается повторно (без дублей)
    if (query.KeyMarker is not null)
    {
        var markerIndex = all.FindIndex(e => Utf8ByteOrder.Compare(e.Key, query.KeyMarker) == 0
            && query.UploadIdMarker is not null && e.UploadId == query.UploadIdMarker);
        all = markerIndex >= 0
            ? all.Skip(markerIndex + 1).ToList()
            : all.Where(e => Utf8ByteOrder.Compare(e.Key, query.KeyMarker) > 0
                && (query.Delimiter is null
                    || !Utf8ByteOrder.StartsWith(e.Key, query.KeyMarker))).ToList();
    }
    // Свёртка delimiter + пагинация (uploads + CP вместе ≤ max-uploads, М10)
    var maxUploads = query.MaxUploads ?? 1000;
    var uploads = new List<UploadEntry>();
    var prefixes = new List<CommonPrefixEntry>();
    bool truncated = false;
    string? nextKey = null, nextUploadId = null;
    foreach (var e in all)
    {
        string? commonPrefix = null;
        if (query.Delimiter is not null)
        {
            var idx = e.Key.IndexOf(query.Delimiter, query.Prefix?.Length ?? 0, StringComparison.Ordinal);
            if (idx >= 0)
                commonPrefix = e.Key[..(idx + query.Delimiter.Length)];
        }
        var isSeenPrefix = commonPrefix is not null
            && prefixes.Any(p => p.Prefix == commonPrefix) ? (bool?)true : null;
        if (uploads.Count + prefixes.Count >= maxUploads
            && (commonPrefix is null || isSeenPrefix != true))
        {
            truncated = true; // осталась невыданная позиция
            break;
        }
        if (commonPrefix is null)
        {
            uploads.Add(new UploadEntry(e.Key, e.UploadId,
                DateTimeOffset.FromUnixTimeMilliseconds(e.InitiatedMs)));
            nextKey = e.Key; nextUploadId = e.UploadId;
        }
        else if (isSeenPrefix != true)
        {
            prefixes.Add(new CommonPrefixEntry(commonPrefix));
            nextKey = commonPrefix; nextUploadId = null;
        }
    }
    return Task.FromResult(new UploadsPage(uploads, prefixes, truncated,
        truncated ? nextKey : null, truncated ? nextUploadId : null));
}
```

Примечание к маркеру: при заданном `KeyMarker` без `UploadIdMarker` — выдача со всех ключей строго после key-marker (`markerIndex == -1` при `UploadIdMarker == null` — записи ключа key-marker отфильтруются условием `Compare > 0`); если key-marker — ранее выданный CommonPrefix (а `delimiter` задан), ключи с этим префиксом пропускаются целиком — двухстраничная пагинация через `NextKeyMarker`-CP не выдаёт дублей (тест ниже). Пропуск по префиксу применяется только при заданном `delimiter`: без него CP-семантики нет и маркер-ключ работает обычным образом.

4. `XlObjectStore.Buckets.cs` — `DeleteBucketAsync`: после существующей проверки `xl.meta` добавить (заменив комментарий «t38: …»):

```csharp
// Живые multipart-загрузки бакета блокируют удаление (канон 02 §1/§5): активная
// запись = запись с существующим каталогом загрузки (М2)
if (Directory.Exists(volume.MultipartDir))
    foreach (var keyDir in Directory.EnumerateDirectories(volume.MultipartDir))
    {
        List<MultipartJournals.UploadJournalEntry> uploads;
        try { uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)); }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый uploads.json в {KeyDir} при DeleteBucket: пропущен", keyDir);
            continue;
        }
        if (uploads.Any(e => e.Bucket == bucket
                && Directory.Exists(MultipartJournals.UploadDirPath(keyDir, e.UploadId))))
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
    }
```

5. `MultipartHandlers.cs` — в `ListPartsHandler` и `ListMultipartUploadsHandler` вычислять видимость и передавать:

```csharp
// Видимость загрузок (канон 05 §3): read-only — только свои
var visibility = context.Request.Identity!.Policy == AccessPolicy.ReadOnly
    ? UploadVisibility.OwnedBy(context.Request.Identity.AccessKey)
    : UploadVisibility.AllUploads;
```

(`ListPartsAsync` — параметр; `UploadsQuery(..., visibility)` — последний аргумент; подключить `using OwnS3.App.Access;` при необходимости.)

- [ ] **Шаг 4. Прогнать тесты Storage-класса и сборку**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests|FullyQualifiedName~XlObjectStoreBucketTests"`
Expected: PASS.
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`
Expected: 0 warnings.

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/IObjectStore.cs src/OwnS3.Storage/Domain/DomainTypes.cs \
        src/OwnS3.Storage/XlObjectStore.Multipart.cs src/OwnS3.Storage/XlObjectStore.Buckets.cs \
        src/OwnS3.App/Handlers/MultipartHandlers.cs \
        src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): ListParts/ListMultipartUploads с видимостью + DeleteBucket-гейт живых загрузок (t38, фаза 2)"
```

---

### Task 6: Complete — сверка манифеста (InvalidPart-исходы) (фаза 3)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (использует):** `ResolveUploadOrThrow`, `MinPartSize` (Task 3), `ReadPartsOrThrow` (Task 4), `MultipartJournals` (Task 1).

- [ ] **Шаг 1. Написать failing-тесты сверки манифеста**

```csharp
[Fact]
public async Task Complete_WrongEtag_InvalidPart()
{
    // Arrange: одна часть загружена, манифест с чужим ETag
    var uploadId = await Store.CreateMultipartUploadAsync("b", "ci-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var bytes = Encoding.UTF8.GetBytes("part");
    var put = await Store.UploadPartAsync("b", "ci-key", uploadId, 1, new MemoryStream(bytes),
        bytes.Length, TestContext.Current.CancellationToken);

    // Act
    var act = async () => await Store.CompleteMultipartUploadAsync("b", "ci-key", uploadId,
        [new PartEtag(1, "\"deadbeef\"")], TestContext.Current.CancellationToken);

    // Assert
    await act.Should().ThrowAsync<ObjectStoreException>()
        .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);
}

// Аналогично: Complete_MissingPartNumber_InvalidPart (манифест ссылается на номер 2,
// загружена только 1); Complete_SmallPartNotLast_InvalidPart (две части по 3 байта —
// первая < 5 МиБ); Complete_SmallLastPart_Allowed (первая 5 МиБ, последняя 3 байта —
// проходит дальше, отказ ожидаем только по другим причинам — здесь кейс закрыт в Task 7
// успешной сборкой);
// Complete_EtagWithoutQuotes_Matches (манифест прислал ETag без кавычек / в UPPER —
// нормализация кавычек + lowercase, сверка проходит в Task 7);
// Complete_UnknownUpload_NoSuchUpload.
```

- [ ] **Шаг 2. Прогнать — отказ (заглушка)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.Complete"`
Expected: FAIL (ObjectStoreUnavailableException).

- [ ] **Шаг 3. Реализовать сверку манифеста**

`XlObjectStore.Multipart.cs` — сверка как внутренний метод (сама сборка — Task 7; на этом шаге `CompleteMultipartUploadAsync` после успешной сверки бросает `NotImplementedException`-заглушку НЕ вводим: метод целиком реализуется в Task 7, здесь вводим ТОЛЬКО метод сверки и используем его в полном методе Task 7; чтобы шаг был независимо зелёным, метод сверки тестируется через публичный Complete, который на этом шаге бросает `ObjectStoreException(InvalidPart)` при провале сверки и `ObjectStoreUnavailableException` при успехе сверки — временная граница, удаляемая Task 7):

```csharp
// Сверка манифеста Complete с parts.json (канон 02 §5): каждая пара манифеста —
// номер существует ∧ ETag совпал (кавычки снимаются, hex в lowercase); каждая
// часть кроме последней ≥ MinPartSize. Провал — InvalidPart.
internal static void ValidateManifestAgainstJournal(IReadOnlyList<PartEtag> manifest,
    List<MultipartJournals.PartJournalEntry> journal)
{
    var byNumber = journal.ToDictionary(p => p.PartNumber);
    for (var i = 0; i < manifest.Count; i++)
    {
        var (number, etagRaw) = manifest[i];
        if (!byNumber.TryGetValue(number, out var uploaded))
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
        var expected = etagRaw.Trim('"').ToLowerInvariant();
        if (!string.Equals(uploaded.ETag, expected, StringComparison.Ordinal))
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
        if (i < manifest.Count - 1 && uploaded.Size < MinPartSize)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
    }
}
```

`CompleteMultipartUploadAsync` на этом шаге: `EnsureBucket` → под `_commitLock`: `ResolveUploadOrThrow` → `ReadPartsOrThrow(uploadDir)` (битый журнал → warning + `NoSuchUpload`, Task 4) → `ValidateManifestAgainstJournal` → `throw new ObjectStoreUnavailableException(); // сборка — следующая задача`.

- [ ] **Шаг 4. Прогнать тесты сверки**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.Complete"`
Expected: FAIL-кейсы InvalidPart — PASS; кейс отсутствия отказа (успешная сборка) — ещё не пишем (Task 7).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlObjectStore.Multipart.cs src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): сверка манифеста Complete — InvalidPart по ETag/номеру/размеру 5 МиБ (t38, фаза 3)"
```

---

### Task 7: Complete — попытка/перенос/коммит/зачистка/идемпотентность (фаза 3)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (производит):**
```csharp
// Тест-хук сбоя между переносом частей и коммитом (по образцу HardLinkProbe t37):
// задан → вызывается ПОСЛЕ переноса всех частей и ДО записи xl.meta объекта;
// исключение хука эмулирует крах процесса до коммит-поинта.
internal Action? CompletePreCommitProbe;
```

**Алгоритм CompleteMultipartUploadAsync (шаги 1–9 спеки §4.2, весь под `_commitLock`, М3/М7):**
1. `EnsureBucket`; `ResolveUploadOrThrow` (без видимости).
2. `ReadPartsOrThrow(uploadDir)` (битый `parts.json` → warning + `NoSuchUpload`, Task 4); `ValidateManifestAgainstJournal` (Task 6).
3. `attempt.json`: `ReadAttempt` != null → dataDir-имя из маркера; null → `Guid.NewGuid().ToString("N")` + `WriteAttempt`. dataDir-путь = `Path.Combine(ObjectDir(bucket, key), dataDirName)`; `Directory.CreateDirectory` (создаёт префиксы).
4. Перенос: для каждого номера манифеста: `dataDir/part.N` нет → `File.Move(uploadDir/part.N, dataDir/part.N)` (источник отсутствует → `InvalidPart`); перенесённое не трогать.
5. `CompletePreCommitProbe?.Invoke()` — точка сбоя.
6. Составной ETag (М6): `manifest.Count + "-" + md5(concat etag-hex)`; SHA-256 — последовательный `IncrementalHash` по `dataDir/part.N` в порядке манифеста.
7. `xl.meta` объекта: `XlMetaFile.Read(uploadDir)` (метаданные загрузки) → новая запись: `VersionId` = dataDir-uuid попытки, `Size` = сумма размеров частей манифеста (из журнала), `ETag` = составной, `ModTime` = now, `ContentType`/`UserMetadata` — из загрузки, `Headers` = EmptyHeaders, `ContentSha256` = вычисленный.
8. КОММИТ: `XlMetaFile.Write(ObjectDir(bucket, key), record)` — атомарная замена (bkp→tmp→rename); для нового ключа — та же атомарная установка (Write создаёт каталоги).
9. После коммита: старый dataDir целевого объекта (из прежнего `xl.meta`, если был и ≠ новому) → `volume.MoveToTrash`; удаление записи загрузки из `uploads.json` (перезапись); `volume.MoveToTrash(uploadDir)` — целиком (остатки частей не из манифеста, журналы, `xl.meta` загрузки, `attempt.json`). Возврат `CompleteResult('"' + составной + '"')`.

- [ ] **Шаг 1. Написать failing-тесты сборки**

```csharp
internal static async Task<string> UploadBigPartAsync(XlObjectStore store, string bucket, string key,
    string uploadId, int partNumber, int size, byte fill)
{
    var bytes = PartBytes(size, fill);
    var result = await store.UploadPartAsync(bucket, key, uploadId, partNumber,
        new MemoryStream(bytes), bytes.Length, TestContext.Current.CancellationToken);
    return result.ETag.Trim('"');
}

[Fact]
public async Task Complete_CommitsObject_CompositeEtagAndLayout()
{
    // Arrange: 2 части: 5 МиБ + 3 байта
    var uploadId = await Store.CreateMultipartUploadAsync("b", "cc-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var etag1 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var etag2 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 2, 3, 0xBB);
    var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };

    // Act
    var result = await Store.CompleteMultipartUploadAsync("b", "cc-key", uploadId, manifest,
        TestContext.Current.CancellationToken);

    // Assert: составной ETag «2-md5(concat)» В КАВЫЧКАХ; объект виден Get/Head/List;
    // xl.meta: Size/ETag/VersionId; загрузка зачищена (запись удалена, каталог в .trash)
    var concat = etag1 + etag2;
    var composite = "2-" + Convert.ToHexString(
        System.Security.Cryptography.MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
    result.ETag.Should().Be("\"" + composite + "\"");
    var head = await Store.HeadObjectAsync("b", "cc-key", null, TestContext.Current.CancellationToken);
    head.Metadata.ETag.Should().Be("\"" + composite + "\"");
    head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    head.Metadata.ContentType.Should().Be("text/plain");
    var listed = await Store.ListObjectsAsync("b", new ListQuery(null, null, null, null, null, null, null, false, ListVariant.V1),
        TestContext.Current.CancellationToken);
    listed.Contents.Any(e => e.Key == "cc-key").Should().BeTrue();
    var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "cc-key");
    MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
    Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeFalse();
}

[Fact]
public async Task Complete_CrashBeforeCommit_IdempotentRetryReusesParts()
{
    // Arrange: загрузка 5 МиБ + 3 байта; хук сбоя после переноса, до коммита
    var uploadId = await Store.CreateMultipartUploadAsync("b", "cr-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var etag1 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var etag2 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 2, 3, 0xBB);
    var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };
    Store.CompletePreCommitProbe = () => throw new IOException("crash before commit");

    // Act: сбойная попытка
    var crashed = async () => await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId,
        manifest, TestContext.Current.CancellationToken);
    await crashed.Should().ThrowAsync<IOException>();
    Store.CompletePreCommitProbe = null;

    // Assert до повтора: объект НЕ виден; загрузка жива (ListParts работает)
    var missing = async () => await Store.GetObjectAsync("b", "cr-key",
        new ObjectReadOptions(null, null, null), TestContext.Current.CancellationToken);
    await missing.Should().ThrowAsync<ObjectStoreException>().Where(e => e.Code == ObjectStoreErrorCode.NoSuchKey);
    var parts = await Store.ListPartsAsync("b", "cr-key", uploadId, null, null,
        UploadVisibility.AllUploads, TestContext.Current.CancellationToken);
    parts.Parts.Should().HaveCount(2);

    // Act: повторный Complete — идемпотентен (доиспользование переноса, тот же dataDir)
    var result = await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId, manifest,
        TestContext.Current.CancellationToken);
    var head = await Store.HeadObjectAsync("b", "cr-key", null, TestContext.Current.CancellationToken);
    head.Metadata.ETag.Should().Be(result.ETag);
    // dataDir попытки переиспользован: в каталоге объекта ровно один Guid-dataDir
    var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cr-key"));
    Directory.EnumerateDirectories(objectDir).Count().Should().Be(1);
}

// Аналогично: Complete_AfterSuccess_SecondComplete_NoSuchUpload;
// Complete_ExtraPartsNotInManifest_TrashedWithUploadDir (часть 3 загружена, манифест
// 1-2 → после Complete каталог загрузки с part.3 в .trash, объект = 2 части);
// Complete_NewObjectDir_PrefixesCreated (ключ «deep/nested/key»);
// Complete_OverwriteExistingObject_OldDataDirTrashed (простой PUT до, Complete поверх:
// старый dataDir в .trash, xl.meta новый);
// Complete_DoseUploadAfterCrash_Allowed (после сбойного Complete дозагрузить часть 2
// заново и собрать).
```

- [ ] **Шаг 2. Прогнать — отказ (ObjectStoreUnavailable)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.Complete"`
Expected: новые кейсы FAIL.

- [ ] **Шаг 3. Реализовать сборку по алгоритму**

`XlObjectStore.Multipart.cs` — полный `CompleteMultipartUploadAsync` (шаги 1–9 выше); удалить временную заглушку Task 6; код ключевых фрагментов:

```csharp
internal Action? CompletePreCommitProbe; // точка сбоя: после переноса, до коммита

public Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
    IReadOnlyList<PartEtag> parts, CancellationToken ct)
{
    EnsureBucket(bucket);
    var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
    var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
    lock (_commitLock)
    {
        // 1–2. Разрешение + сверка (ReadPartsOrThrow — Task 4: битый журнал → NoSuchUpload)
        ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
        var journal = ReadPartsOrThrow(uploadDir);
        ValidateManifestAgainstJournal(parts, journal);
        // 3. Попытка (М7): attempt.json — dataDir текущей сборки
        var dataDirName = MultipartJournals.ReadAttempt(uploadDir)?.DataDir
                          ?? Guid.NewGuid().ToString("N");
        MultipartJournals.WriteAttempt(uploadDir, new MultipartJournals.AttemptMarker(dataDirName));
        var target = ObjectDir(bucket, key);
        var dataDir = Path.Combine(target, dataDirName);
        Directory.CreateDirectory(dataDir); // создаёт и каталоги-префиксы
        // 4. Перенос частей (идемпотентность: перенесённое не трогать)
        foreach (var (number, _) in parts)
        {
            var dest = Path.Combine(dataDir, MultipartJournals.PartFileName(number));
            if (File.Exists(dest))
                continue;
            var src = Path.Combine(uploadDir, MultipartJournals.PartFileName(number));
            if (!File.Exists(src))
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
            File.Move(src, dest);
        }
        // 5. Точка сбоя (тест-хук)
        CompletePreCommitProbe?.Invoke();
        // 6. Составной ETag (М6) + SHA-256 полным проходом по dataDir
        var journalByNumber = journal.ToDictionary(p => p.PartNumber);
        var concat = string.Concat(parts.Select(p => journalByNumber[p.PartNumber].ETag));
        var compositeEtag = parts.Count + "-" + Convert.ToHexString(
            MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
        string sha256Hex;
        using (var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            var buffer = new byte[64 * 1024];
            foreach (var (number, _) in parts)
                using (var stream = new FileStream(
                    Path.Combine(dataDir, MultipartJournals.PartFileName(number)),
                    FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int read;
                    while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                        sha.AppendData(buffer, 0, read);
                }
            sha256Hex = Convert.ToHexString(sha.GetHashAndReset()).ToLowerInvariant();
        }
        // 7–8. xl.meta объекта и КОММИТ (единственная точка видимости)
        var uploadMeta = XlMetaFile.Read(uploadDir, out _);
        var size = parts.Sum(p => journalByNumber[p.PartNumber].Size);
        var record = new XlMetaRecord(Guid.Parse(dataDirName), size, timeProvider.GetUtcNow(),
            compositeEtag, uploadMeta.ContentType, uploadMeta.UserMetadata, EmptyHeaders, sha256Hex);
        var oldDataDir = ReadCurrentDataDirOrNull(target);
        XlMetaFile.Write(target, record);
        // 9. Зачистка после коммита
        if (oldDataDir is not null && oldDataDir != dataDirName)
        {
            var oldPath = Path.Combine(target, oldDataDir);
            if (Directory.Exists(oldPath))
                volume.MoveToTrash(oldPath);
        }
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.RemoveAll(e => e.UploadId == uploadId && e.Bucket == bucket && e.Key == key);
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
        volume.MoveToTrash(uploadDir);
        return Task.FromResult(new CompleteResult('"' + compositeEtag + '"'));
    }
}
```


- [ ] **Шаг 4. Прогнать весь Storage-класс**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: PASS (все кейсы, включая Task 3–6).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlObjectStore.Multipart.cs src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): Complete — попытка attempt.json, перенос rename'ами, составной ETag, коммит xl.meta, зачистка (t38, фаза 3)"
```

---

### Task 8: многочастевое чтение — MultipartBodyStream, checksum, Range, Attributes (фаза 4)

**Файлы:**
- Create: `src/OwnS3.Storage/MultipartBodyStream.cs`
- Modify: `src/OwnS3.Storage/XlObjectStore.Objects.cs` (GetObject: тело/сверка по частям; VerifyChecksum — по списку файлов)
- Modify: `src/OwnS3.Storage/XlObjectStore.Copy.cs` (GetObjectAttributes: реальные части)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (производит):**
```csharp
namespace OwnS3.Storage;

// Составное тело объекта: последовательность part.N (ленивое открытие, FileShare.Read).
// Полный объект: (files, 0, totalLength); срез Range: (files, startOffset, sliceLength).
// files — пути файлов частей в порядке байтов.
public sealed class MultipartBodyStream : Stream
{
    public MultipartBodyStream(IReadOnlyList<string> partPaths, long startOffset, long length);
    // Stream: CanRead=true, CanSeek=false, Length=length; Dispose закрывает текущий FileStream.
}

// XlObjectStore.Objects.cs (internal, переиспользуют Task 9–10):
// Перечисление частей данных записи: (номер, путь, размер) по возрастанию номеров.
internal static List<(int Number, string Path, long Size)> EnumerateDataParts(string objectDir, string dataDirName);
```

- [ ] **Шаг 1. Написать failing-тесты многочастевого чтения**

```csharp
[Fact]
public async Task Get_MultipartObject_BodyIsConcatOfParts()
{
    // Arrange: объект из частей 5 МиБ (0xAA) + «abc»
    var uploadId = await Store.CreateMultipartUploadAsync("b", "read-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "read-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var e2 = await UploadBigPartAsync(Store, "b", "read-key", uploadId, 2, 3, (byte)'c');
    var result = await Store.CompleteMultipartUploadAsync("b", "read-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

    // Act
    var content = await Store.GetObjectAsync("b", "read-key", new ObjectReadOptions(null, null, null),
        TestContext.Current.CancellationToken);
    using var body = content.Body;
    var bytes = new byte[body.Length];
    await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

    // Assert: тело = конкатенация; хвост — «ccc»; conditional по составному ETag:
    // If-None-Match совпал → 304-семантика (NotModified-обёртка)
    bytes.Length.Should().Be(5 * 1024 * 1024 + 3);
    bytes[^3..].Should().Equal("ccc"u8.ToArray());
    var notModified = await Store.GetObjectAsync("b", "read-key",
        new ObjectReadOptions(new ObjectConditions(null, result.ETag, null, null), null, null),
        TestContext.Current.CancellationToken);
    notModified.NotModified.Should().BeTrue();
}

[Fact]
public async Task Get_MultipartObject_RangeAcrossPartBoundary_206()
{
    // Arrange: части 5 МиБ + 3 байта; срез (5*1024*1024 - 2, 5*1024*1024 + 2) — через стык
    var uploadId = await Store.CreateMultipartUploadAsync("b", "rng-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "rng-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var e2 = await UploadBigPartAsync(Store, "b", "rng-key", uploadId, 2, 3, (byte)'c');
    await Store.CompleteMultipartUploadAsync("b", "rng-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

    // Act
    var start = 5 * 1024 * 1024 - 2;
    var content = await Store.GetObjectAsync("b", "rng-key",
        new ObjectReadOptions(null, new ByteRange(start, start + 4), null),
        TestContext.Current.CancellationToken);
    using var body = content.Body;
    var bytes = new byte[body.Length];
    await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

    // Assert: 206, срез = 2 байта хвоста первой части + 3 второй; сверка пройдена ДО байтов
    content.Range.Should().NotBeNull();
    bytes.Should().Equal([(byte)0xAA, (byte)0xAA, (byte)'c', (byte)'c', (byte)'c']);
}

[Fact]
public async Task Get_MultipartObject_RangeStartsExactlyAtPartBoundary_206()
{
    // Arrange: части 5 МиБ + «abc»; срез начинается РОВНО на границе частей —
    // регресс-кейс скетча MultipartBodyStream (skip == size части обязан
    // переводить старт в следующую часть с позиции 0, а не в первую часть)
    var uploadId = await Store.CreateMultipartUploadAsync("b", "bnd-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "bnd-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var e2 = await UploadBigPartAsync(Store, "b", "bnd-key", uploadId, 2, 3, (byte)'c');
    await Store.CompleteMultipartUploadAsync("b", "bnd-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

    // Act
    var start = 5 * 1024 * 1024;
    var content = await Store.GetObjectAsync("b", "bnd-key",
        new ObjectReadOptions(null, new ByteRange(start, start + 2), null),
        TestContext.Current.CancellationToken);
    using var body = content.Body;
    var bytes = new byte[body.Length];
    await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

    // Assert: 206; срез = ТОЛЬКО вторая часть («ccc»), ни байта первой
    content.Range.Should().NotBeNull();
    bytes.Should().Equal("ccc"u8.ToArray());
}

[Fact]
public async Task Get_MultipartObject_CorruptedPart_500BeforeBytes()
{
    // Arrange: порча байта в part.1 закоммиченного объекта
    var uploadId = await Store.CreateMultipartUploadAsync("b", "cor-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "cor-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var e2 = await UploadBigPartAsync(Store, "b", "cor-key", uploadId, 2, 3, (byte)'c');
    await Store.CompleteMultipartUploadAsync("b", "cor-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);
    var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cor-key"));
    var dataDir = Directory.EnumerateDirectories(objectDir).Single();
    var partPath = Path.Combine(objectDir, dataDir, "part.1");
    using (var fs = new FileStream(partPath, FileMode.Open, FileAccess.Write, FileShare.None))
    {
        fs.Seek(100, SeekOrigin.Begin);
        fs.WriteByte(0x00);
    }

    // Act / Assert: сверка по конкатенации ДО отдачи — XlIntegrityException (500), байтов нет
    var act = async () => await Store.GetObjectAsync("b", "cor-key",
        new ObjectReadOptions(null, null, null), TestContext.Current.CancellationToken);
    await act.Should().ThrowAsync<XlIntegrityException>();
}

[Fact]
public async Task GetObjectAttributes_MultipartObject_RealPartsWithPagination()
{
    // Arrange: 3 части (5 МиБ, 5 МиБ, 3 байта)
    var uploadId = await Store.CreateMultipartUploadAsync("b", "attr-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 1, 5 * 1024 * 1024, 0x01);
    var e2 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 2, 5 * 1024 * 1024, 0x02);
    var e3 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 3, 3, 0x03);
    var complete = await Store.CompleteMultipartUploadAsync("b", "attr-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2), new PartEtag(3, e3)], TestContext.Current.CancellationToken);

    // Act: страница maxParts=2, продолжение
    var page1 = await Store.GetObjectAttributesAsync("b", "attr-key",
        [ObjectAttributeName.ObjectParts], 2, null, null, TestContext.Current.CancellationToken);
    var page2 = await Store.GetObjectAttributesAsync("b", "attr-key",
        [ObjectAttributeName.ObjectParts], 2, page1.Attributes!.Parts!.NextPartNumberMarker, null,
        TestContext.Current.CancellationToken);

    // Assert: реальные части/размеры; PartsCount=3; составной ETag
    page1.Attributes!.Parts!.PartsCount.Should().Be(3);
    page1.Attributes.Parts.IsTruncated.Should().BeTrue();
    page1.Attributes.Parts.Parts.Should().Equal([(1, 5L * 1024 * 1024), (2, 5L * 1024 * 1024)]);
    page2.Attributes!.Parts!.Parts.Should().Equal([(3, 3L)]);
    page1.Attributes.ETag.Should().Be(complete.ETag);
}

// Аналогично: GetObjectAttributes_SimplePut_SyntheticSinglePart (регрессия: часть (1, size));
// Get_MultipartObject_Range_IfRangeMismatch_Full200 (If-Range чужой ETag → 200 полным телом).
```

- [ ] **Шаг 2. Прогнать — отказ**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.Get|FullyQualifiedName~XlObjectStoreMultipartTests.GetObjectAttributes"`
Expected: FAIL (хардкод part.1: тело обрезано/сверка по part.1; атрибуты — синтетическая часть).

- [ ] **Шаг 3. Реализовать MultipartBodyStream + чтение по частям**

1. `src/OwnS3.Storage/MultipartBodyStream.cs`:

```csharp
namespace OwnS3.Storage;

// Составное тело данных (канон 04 §1: порядок part.N = порядок байтов): лениво
// открывает файлы частей (FileShare.Read), переходит между ними при чтении.
// Полный объект: startOffset=0, length=Size; срез Range — смещение/длина среза.
public sealed class MultipartBodyStream : Stream
{
    private readonly IReadOnlyList<string> _partPaths;
    private FileStream? _current;
    private int _currentIndex;      // стартовая часть — вычислена конструктором
    private int _openedIndex = -1;  // индекс открытого файла; -1 — пока не открывали
    private long _positionInCurrent;
    private long _remaining;

    public MultipartBodyStream(IReadOnlyList<string> partPaths, long startOffset, long length)
    {
        _partPaths = partPaths;
        _remaining = length;
        // Стартовая часть и смещение В ней: startOffset ∈ [0, суммарный размер].
        // ВАЖНО: skip == size части — старт со СЛЕДУЮЩЕЙ части с позиции 0
        // (диапазон, начинающийся ровно на границе частей).
        var skip = startOffset;
        var index = 0;
        while (index < partPaths.Count)
        {
            var size = new FileInfo(partPaths[index]).Length;
            if (skip < size)
                break;                        // старт внутри части index
            skip -= size;                     // старт за этой частью (вкл. ровно на границе)
            index++;
        }
        _currentIndex = index;                // первое чтение откроет ровно эту часть
        _positionInCurrent = skip;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _remaining;
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_remaining <= 0)
            return 0;
        EnsureCurrentOpen();
        var read = _current!.Read(buffer, offset, (int)Math.Min(count, _remaining));
        if (read == 0)
            throw new EndOfStreamException("часть данных короче ожидаемого (изменилась на диске?)");
        _remaining -= read;
        return read;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_remaining <= 0)
            return 0;
        EnsureCurrentOpen();
        var read = await _current!.ReadAsync(buffer[..(int)Math.Min(buffer.Length, _remaining)], cancellationToken);
        if (read == 0)
            throw new EndOfStreamException("часть данных короче ожидаемого (изменилась на диске?)");
        _remaining -= read;
        return read;
    }

    // Ленивое открытие стартовой части и переход к следующей при исчерпании текущей.
    // _openedIndex = -1 → ничего не открыто: первое открытие — РОВНО _currentIndex,
    // вычисленный конструктором (без инкремента); далее — последовательный переход.
    private void EnsureCurrentOpen()
    {
        if (_current is not null && _current.Position < _current.Length)
            return;
        _current?.Dispose();
        var next = _openedIndex < 0 ? _currentIndex : _openedIndex + 1;
        if (next >= _partPaths.Count)
            throw new EndOfStreamException("данные объекта исчерпаны раньше длины");
        _current = new FileStream(_partPaths[next], FileMode.Open, FileAccess.Read, FileShare.Read);
        if (_positionInCurrent > 0)
        {
            _current.Seek(_positionInCurrent, SeekOrigin.Begin);
            _positionInCurrent = 0;
        }
        _openedIndex = next;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _current?.Dispose();
        base.Dispose(disposing);
    }
}
```

2. `XlObjectStore.Objects.cs`:
   - `EnumerateDataParts(objectDir, dataDirName)`: `Directory.EnumerateFiles(dataDir, "part.*")`, парсинг номера (`int.TryParse` суффикса после `part.`; не-числовые — skip), сортировка по номеру, размер из `FileInfo`; пусто → `XlIntegrityException` (запись без данных — порча).
   - `GetObjectAsync` шаги 4–5 заменить: `var parts = EnumerateDataParts(...)`; `VerifyChecksum(parts paths, meta)` — последовательный проход SHA-256 по всем файлам; тело: `range is null` → `new MultipartBodyStream(paths, 0, meta.Size)`; иначе → `new MultipartBodyStream(paths, range.Start, range.End - range.Start + 1)`. Старый `RangeSliceStream` удалить (частный случай покрывается `MultipartBodyStream`).
   - `VerifyChecksum(IReadOnlyList<string> partPaths, XlMetaRecord meta)` — `IncrementalHash` по файлам циклом.
3. `XlObjectStore.Copy.cs` — `GetObjectAttributesAsync` п.3: вместо синтетической части — `EnumerateDataParts`:

```csharp
// Части — реальные файлы dataDir (канон 02 §5 GetObjectAttributes; простой PUT — part.1)
var dataParts = EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName);
var marker = partNumberMarker ?? 0;
var selected = dataParts.Where(p => p.Number > marker)
    .Take(maxParts ?? 1000)
    .Select(p => (p.Number, p.Size))
    .ToList();
var truncated = dataParts.Count(p => p.Number > marker) > selected.Count;
var parts = new ObjectPartsAttributes(dataParts.Count, marker,
    truncated ? selected[^1].Number : null, maxParts ?? 1000, truncated, selected);
```

- [ ] **Шаг 4. Прогнать Storage-класс + регресс объектных тестов**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests|FullyQualifiedName~XlObjectStoreObjectTests|FullyQualifiedName~XlObjectStoreCopyTests"`
Expected: PASS (простой PUT через единую multipart-механику не регрессировал).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/MultipartBodyStream.cs src/OwnS3.Storage/XlObjectStore.Objects.cs \
        src/OwnS3.Storage/XlObjectStore.Copy.cs \
        src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): многочастевое чтение — MultipartBodyStream, checksum по конкатенации, Range через стыки, реальные части Attributes (t38, фаза 4)"
```

---

### Task 9: CopyObject multipart-источника (фаза 4)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Copy.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (использует):** `EnumerateDataParts` (Task 8), `CopyPart`/`HardLinkProbe` (t37).

- [ ] **Шаг 1. Написать failing-тест**

```csharp
[Fact]
public async Task Copy_MultipartSource_InheritsCompositeEtagAndParts()
{
    // Arrange: multipart-объект (5 МиБ + 3 байта)
    var uploadId = await Store.CreateMultipartUploadAsync("b", "cpsrc-key", Meta, "writer",
        TestContext.Current.CancellationToken);
    var e1 = await UploadBigPartAsync(Store, "b", "cpsrc-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
    var e2 = await UploadBigPartAsync(Store, "b", "cpsrc-key", uploadId, 2, 3, (byte)'c');
    var complete = await Store.CompleteMultipartUploadAsync("b", "cpsrc-key", uploadId,
        [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

    // Act
    var copy = await Store.CopyObjectAsync(new CopyRequest("b", "cpsrc-key", "b", "cpdst-key",
        ReplaceMetadata: false, NewMetadata: null, SourceConditions: null),
        TestContext.Current.CancellationToken);

    // Assert: составной ETag/Size наследованы (arch-правка §3.5/Q3); тело копии читается;
    // dataDir копии содержит обе части
    copy.ETag.Should().Be(complete.ETag);
    var dst = await Store.HeadObjectAsync("b", "cpdst-key", null, TestContext.Current.CancellationToken);
    dst.Metadata.ETag.Should().Be(complete.ETag);
    dst.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    var dstObjectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cpdst-key"));
    var dstDataDir = Directory.EnumerateDirectories(dstObjectDir).Single();
    Directory.EnumerateFiles(dstDataDir).Select(Path.GetFileName).OrderBy(n => n)
        .Should().Equal(["part.1", "part.2"]);
    var content = await Store.GetObjectAsync("b", "cpdst-key",
        new ObjectReadOptions(null, new ByteRange(5 * 1024 * 1024, 5 * 1024 * 1024 + 2), null),
        TestContext.Current.CancellationToken);
    using var body = content.Body;
    var tail = new byte[body.Length];
    await body.ReadExactlyAsync(tail, TestContext.Current.CancellationToken);
    tail.Should().Equal("ccc"u8.ToArray());
}
```

- [ ] **Шаг 2. Прогнать — отказ (копируется только part.1)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.Copy_MultipartSource"`
Expected: FAIL (в dataDir копии только part.1; чтение хвоста падает).

- [ ] **Шаг 3. Реализовать пофайловый перенос частей**

`XlObjectStore.Copy.cs`, шаг 4 `CopyObjectAsync` — заменить одиночный `CopyPart(srcPart, destPart)` на цикл по частям источника:

```csharp
// Пофайловый перенос частей источника (простой PUT — ровно part.1; multipart — все):
// хардлинк-точка t37 на каждую часть; etag/sha256/Size наследуются из записи
var srcDataDir = Path.Combine(ObjectDir(request.SourceBucket, request.SourceKey), src.DataDirName);
foreach (var (_, partPath, _) in EnumerateDataParts(
             ObjectDir(request.SourceBucket, request.SourceKey), src.DataDirName))
    CopyPart(partPath, Path.Combine(dataDir, Path.GetFileName(partPath)));
```

(строку `var srcPart = ...; var destPart = ...; CopyPart(srcPart, destPart);` удалить; остальное — метаданные/наследование ETag/sha — уже корректно из t37).

- [ ] **Шаг 4. Прогнать Storage-класс + регресс Copy-тестов**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests|FullyQualifiedName~XlObjectStoreCopyTests"`
Expected: PASS.

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlObjectStore.Copy.cs src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): CopyObject multipart-источника — пофайловый перенос частей, наследование составного ETag (t38, фаза 4)"
```

---

### Task 10: UploadPartCopy (фаза 5)

**Файлы:**
- Modify: `src/OwnS3.Storage/XlObjectStore.Multipart.cs`
- Modify: `src/OwnS3.App/Handlers/MultipartHandlers.cs` (`CopyPartResult.LastModified` из `PutResult`)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` (дополнение)

**Interfaces (использует):** `ResolveUploadOrThrow`, `MaxPartSize` (Task 3), `EnumerateDataParts` (Task 8), conditional `ifModifiedSinceApplies: false` (t37), `ConditionalEvaluator`.

**Алгоритм UploadPartCopyAsync (спека §4.2):**
1. `EnsureBucket(request.DestBucket)`; `ResolveUploadOrThrow(DestBucket, DestKey, UploadId, null)` → `NoSuchUpload`.
2. Источник: `EnsureBucket(request.SourceBucket)`; `XlMetaFile.Read(ObjectDir(src))` → `FileNotFoundException` → `NoSuchKey`.
3. Conditional источника: `ConditionalEvaluator.Evaluate(SourceConditions, src.ETag, src.ModTime, ifModifiedSinceApplies: false)`; не `Proceed` → `PreconditionFailed` (412).
4. Резолв диапазона: `null` → `(0, src.Size - 1)`; иначе `(Start, End)`: `Start >= src.Size || End >= src.Size` → `XlInvalidArgumentException` (InvalidArgument 400; `start > end` уже отсечён App-парсером). Длина = `End - Start + 1`; `> MaxPartSize` → `ObjectStoreException(EntityTooLarge)`.
5. Срез источника: `EnumerateDataParts` источника → маппинг `(start, length)` на файлы: последовательное чтение с `Seek` в каждом файле (`FileShare.Read`), ровно `length` байтов → запись в `part.N.tmp` приёмника (MD5-инкремент + счётчик + fsync; недобор данных → `XlIntegrityException`).
6. Под `_commitLock`: rename `part.N.tmp` → `part.N` + upsert `parts.json` (механика UploadPart).
7. Возврат `PutResult('"' + md5hex + '"', modTime)`.

- [ ] **Шаг 1. Написать failing-тесты**

```csharp
[Fact]
public async Task UploadPartCopy_WholeObject_EtagIsRangeMd5()
{
    // Arrange: источник «hello» (простой PUT); живая загрузка приёмника
    await Store.PutObjectAsync("b", "upc-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
        5, Meta, TestContext.Current.CancellationToken);
    var uploadId = await Store.CreateMultipartUploadAsync("b", "upc-dst", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act
    var result = await Store.UploadPartCopyAsync(new PartCopyRequest("b", "upc-src", "b", "upc-dst",
        uploadId, 1, null, null), TestContext.Current.CancellationToken);

    // Assert: ETag = md5("hello"); часть в журнале приёмника; источник не изменился
    result.ETag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\"");
    var uploadDir = MultipartJournals.UploadDirPath(
        MultipartJournals.KeyDir(Volume.MultipartDir, "b", "upc-dst"), uploadId);
    MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
        .Should().ContainSingle().Which.Size.Should().Be(5);
    var src = await Store.GetObjectAsync("b", "upc-src", new ObjectReadOptions(null, null, null),
        TestContext.Current.CancellationToken);
    using var reader = new StreamReader(src.Body);
    (await reader.ReadToEndAsync(TestContext.Current.CancellationToken)).Should().Be("hello");
}

// Аналогично: UploadPartCopy_RangeFromMultipartSource (источник — multipart-объект,
// диапазон через стык частей; ETag части = md5 среза; часть приёмника собирается
// Complete'ом в читаемый объект); UploadPartCopy_RangeBeyondSource_InvalidArgument
// (start = size → XlInvalidArgumentException);
// UploadPartCopy_SourceConditionFailed_PreconditionFailed (If-Match чужой → 412);
// UploadPartCopy_UnknownUpload_NoSuchUpload; UploadPartCopy_MissingSource_NoSuchKey.

[Fact]
public async Task UploadPartCopy_RangeOver5Gb_EntityTooLarge()
{
    // Arrange: источник-заготовка «5 ГБ + 1» — ПРЯМОЙ посев xl.meta (Size), без записи
    // 5 ГБ на диск: ветка EntityTooLarge срабатывает по src.Size ДО чтения данных
    // (шаг 4 алгоритма — резолв диапазона/лимит; срез шага 5 не открывается). part.1-
    // заглушка — формальная валидность раскладки, данные никогда не читаются
    var size = 5L * 1024 * 1024 * 1024 + 1;
    var versionId = Guid.NewGuid();
    var srcDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("big-src"));
    var dataDir = Path.Combine(srcDir, versionId.ToString("N"));
    Directory.CreateDirectory(dataDir);
    File.WriteAllText(Path.Combine(dataDir, "part.1"), "x");
    XlMetaFile.Write(srcDir, new XlMetaRecord(versionId, size, TestVectors.FixedTime,
        "big-src-etag", "application/octet-stream",
        new Dictionary<string, string>(), new Dictionary<string, string>(), "big-src-sha"));
    var uploadId = await Store.CreateMultipartUploadAsync("b", "big-dst", Meta, "writer",
        TestContext.Current.CancellationToken);

    // Act: диапазон длиной 5 ГБ + 1 (> MaxPartSize) — отказ ДО чтения данных
    var act = async () => await Store.UploadPartCopyAsync(new PartCopyRequest("b", "big-src", "b", "big-dst",
        uploadId, 1, new ByteRange(0, 5L * 1024 * 1024 * 1024), null), TestContext.Current.CancellationToken);

    // Assert
    await act.Should().ThrowAsync<ObjectStoreException>()
        .Where(e => e.Code == ObjectStoreErrorCode.EntityTooLarge);
}
```

- [ ] **Шаг 2. Прогнать — отказ (заглушка)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests.UploadPartCopy"`
Expected: FAIL (ObjectStoreUnavailableException).

- [ ] **Шаг 3. Реализовать UploadPartCopyAsync + App-LastModified**

1. `XlObjectStore.Multipart.cs` — по алгоритму выше; ключевой фрагмент чтения среза:

```csharp
// Срез источника по границам частей (спека §4.3): маппинг offset → части,
// последовательное чтение ровно length байтов с записью в часть приёмника.
private static async Task<(string ETagHex, long Total)> CopySliceToFileAsync(
    string sourceObjectDir, string sourceDataDirName, long start, long length, string destPath)
{
    using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
    long copied = 0;
    using (var file = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None))
    {
        var buffer = new byte[64 * 1024];
        foreach (var (_, partPath, partSize) in EnumerateDataParts(sourceObjectDir, sourceDataDirName))
        {
            if (start >= partSize)
            {
                start -= partSize; // диапазон начинается дальше этой части
                continue;
            }
            using var src = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            src.Seek(start, SeekOrigin.Begin);
            start = 0;
            while (copied < length)
            {
                var want = (int)Math.Min(buffer.Length, length - copied);
                var read = await src.ReadAsync(buffer.AsMemory(0, want));
                if (read == 0)
                    break; // часть исчерпана — следующая
                file.Write(buffer, 0, read);
                md5.AppendData(buffer, 0, read);
                copied += read;
            }
            if (copied >= length)
                break;
        }
        file.Flush(flushToDisk: true);
    }
    if (copied != length)
        throw new XlIntegrityException($"Срез источника короче заявленного: {copied} из {length}");
    return (Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant(), copied);
}
```

Коммит части — тот же блок `lock (_commitLock) { File.Move(...); upsert parts.json; }`, что в UploadPart (вынести в общий private-метод `CommitPartFile(uploadDir, partNumber, tmpPath, etagHex, size, modTime)` и использовать в обоих; чтение журнала в нём — через `ReadPartsOrThrow`).

2. `MultipartHandlers.cs` — `UploadPartCopyHandler`: `LastModified = S3HandlerContext.FormatDate(result.LastModified)` вместо `DateTimeOffset.UtcNow`.

3. Зачистка заглушечных следов (спека §4.6.3; все 7 методов реальны — источников заглушек больше нет):
   - удалить ставший неиспользуемым `private static Task<T> ThrowUnavailable<T>()` из `XlObjectStore.Multipart.cs`;
   - заменить заголовочный комментарий файла `XlObjectStore.Multipart.cs` («Multipart-методы XlObjectStore — заглушки до t38 … App маппит в 500 InternalError») на описание реальных 7 операций (канон 02 §5, журналы — канон 04 §5);
   - `DrainAsync` уже удалён Task 4; `ObjectStoreUnavailableException` остаётся в `ObjectStoreException.cs` (маппинг 500 в `S3Middleware`, спека §4.6.4) — источников в Storage нет.

- [ ] **Шаг 4. Прогнать Storage-класс + гейт отсутствия заглушечных следов**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter "FullyQualifiedName~XlObjectStoreMultipartTests"`
Expected: PASS (все).
Run: `grep -rn "ThrowUnavailable\|DrainAsync\|NotWired\|заглушк\|ObjectStoreUnavailableException" src/OwnS3.Storage/XlObjectStore.Multipart.cs src/tests/OwnS3.UnitTests/Storage/`
Expected: пусто (заглушечных следов нет ни в реализации, ни в юнит-тестах — кейсы t36 удалены Task 3; сборка зелёная подтверждает отсутствие неиспользуемого кода при `TreatWarningsAsErrors`).

- [ ] **Шаг 5. Коммит**

```bash
git add src/OwnS3.Storage/XlObjectStore.Multipart.cs src/OwnS3.App/Handlers/MultipartHandlers.cs \
        src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs
git commit -m "feat(owns3): UploadPartCopy — срез источника по границам частей, лимиты/conditional, LastModified из modTime части; зачистка заглушечных следов ThrowUnavailable/заголовка (t38, фаза 5)"
```

---

### Task 11: App-стыковка — переделка старых кейсов + интеграционные HTTP-сценарии (фаза 6)

**Файлы:**
- Modify: `src/tests/OwnS3.IntegrationTests/Api/RoutingScenarios.cs` (multipart-строки TheoryData + два Fact-кейса → реальные исходы)
- Modify: `src/tests/OwnS3.IntegrationTests/Api/AccessScenarios.cs` (`Reader_MultipartListings_PassWithoutOwnerFilter` → реальный фильтр)
- Modify: `src/tests/OwnS3.IntegrationTests/Api/ObjectStorageScenarios.cs` (`Multipart_Operations_Still500` → реальные исходы)
- Create: `src/tests/OwnS3.IntegrationTests/Api/MultipartScenarios.cs`

**Interfaces (использует):** `OwnS3AppFactory` (roles: Reader/Writer/Admin), `OwnS3TestClient.SendSignedAsync`, `RoutingScenarios.ErrorXmlAsync`, XML-модели t36 (`InitiateMultipartUploadResult` и т.д. — парсинг `XDocument`).

- [ ] **Шаг 1. Переделать заглушечные кейсы (реальные исходы)**

1. `RoutingScenarios.StorageOperations` — строки 29/33/34/35 заменить (бакет `bucket` не существует; до Storage доходит, заглушек больше нет):

```csharp
{ "GET",     "/bucket?uploads",              HttpStatusCode.NotFound,             "NoSuchBucket" },
{ "DELETE",  "/bucket/key?uploadId=u",       HttpStatusCode.NotFound,             "NoSuchBucket" },
{ "GET",     "/bucket/key?uploadId=u",       HttpStatusCode.NotFound,             "NoSuchBucket" },
{ "POST",    "/bucket/key?uploads",          HttpStatusCode.NotFound,             "NoSuchBucket" },
```

Комментарий «multipart-заглушки — 500 (граница t38)» заменить на «multipart — реальные исходы Storage (t38)».
2. `UploadPart_And_UploadPartCopy_ReachStorage`: ожидания → 404 `NoSuchBucket` (оба); ассерт-комментарий «multipart-заглушки — не меняются (до t38)» заменить на реальные исходы.
3. `CompleteMultipartUpload_ReachesStorage`: ожидание → 404 `NoSuchBucket`; ассерт-комментарий «multipart-заглушка (до t38)» — так же.
4. `AccessScenarios.Reader_MultipartListings_PassWithoutOwnerFilter` → переименовать в `Reader_MultipartListings_PassRights_ReachStorage`; ожидания → 404 `NoSuchBucket` (право есть; фильтр «своих» к несуществующему бакету не применяется); комментарий кейса «read-only допуск … без фильтра "своих" — фильтр появляется с данными загрузок (t38…)» и ассерт-комментарий «multipart-заглушки — 500 (до t38)» переписать под реальную семантику (право пройдено, запрос дошёл до Storage).
5. `ObjectStorageScenarios.Multipart_Operations_Still500` → переименовать в `Multipart_Operations_ReachStorage`: оба запроса → 404 `NoSuchBucket`; комментарий кейса «…до Storage дело не доходит, заглушки multipart-методов отвечают 500 InternalError…» заменить (бакет не создан — 404 NoSuchBucket от Storage).
6. Сопутствующие комментарии шапок (тексты описывают исчезающее заглушечное состояние): шапка класса `RoutingScenarios` (строка ~10: «multipart — 500 заглушки до t38») — заменить упоминание на реальные исходы multipart-операций; шапка класса `ObjectStorageScenarios` (строка ~15: «multipart — 500 до t38») — так же.

- [ ] **Шаг 2. Написать MultipartScenarios (полный HTTP-цикл, спека §7)**

`src/tests/OwnS3.IntegrationTests/Api/MultipartScenarios.cs` — `IClassFixture<OwnS3AppFactory>`; правило изоляции: каждый мутационный кейс — уникальный бакет `b-mp-<суффикс>` (создаёт admin); AA-нотация. Каркас и ключевые кейсы:

```csharp
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace OwnS3.IntegrationTests.Api;

// Полный HTTP-цикл multipart (глава 02 §5) через подписанные запросы: 7 операций,
// составной ETag, Range через стыки, права/видимость, DeleteBucket-гейт, ошибки.
public sealed class MultipartScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static readonly byte[] FiveMiB = new byte[5 * 1024 * 1024]; // нули

    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    // CreateMultipartUpload: тело канонически отсутствует, но отправляем ОДИН байт —
    // пустое body НЕ создаёт request.Content у тест-клиента, и Content-Type/
    // x-amz-meta-* не дошли бы до сервера (при этом попадая в подпись — 403).
    // Payload по умолчанию UNSIGNED-PAYLOAD, хендлер POST ?uploads тело игнорирует.
    private static async Task<string> CreateUploadAsync(OwnS3TestClient client, string bucket, string key,
        Dictionary<string, string>? headers = null, OwnS3TestClient.Credentials? credentials = null)
    {
        var response = await client.SendSignedAsync("POST", $"/{bucket}/{key}?uploads",
            headers: headers, credentials: credentials, body: " "u8.ToArray());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        return xml.Root!.Element(S3Ns + "UploadId")!.Value;
    }

    private static async Task<string> UploadPartAsync(OwnS3TestClient client, string bucket, string key,
        string uploadId, int partNumber, byte[] body)
    {
        var response = await client.SendSignedAsync("PUT", $"/{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId}",
            body: body, payloadString: Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return response.Headers.ETag!.Tag; // «"hex"»
    }

    [Fact]
    public async Task FullCycle_CreatePartsListCompleteGetHeadDelete()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-cycle", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-cycle", "mp-obj",
            new Dictionary<string, string> { ["Content-Type"] = "text/plain", ["x-amz-meta-k"] = "v" });

        // Act: UploadPart ×3 (перезагрузка №2), ListParts (пагинация), Complete
        var e1 = await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 1, FiveMiB);
        await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 2, new byte[10]); // перезагрузка
        var tail = "abc"u8.ToArray();
        var e2 = await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 2, tail);
        var list = await client.SendSignedAsync("GET", $"/b-mp-cycle/mp-obj?uploadId={uploadId}&max-parts=1");
        var listXml = XDocument.Parse(await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var nextMarker = listXml.Root!.Element(S3Ns + "NextPartNumberMarker")!.Value;
        var list2 = await client.SendSignedAsync("GET",
            $"/b-mp-cycle/mp-obj?uploadId={uploadId}&part-number-marker={nextMarker}");
        var list2Xml = XDocument.Parse(await list2.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        list2Xml.Root!.Elements(S3Ns + "Part").Single().Element(S3Ns + "PartNumber")!.Value
            .Should().Be("2"); // перезагруженная часть — последняя версия в журнале
        var manifest = $"""<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="{S3Ns}"><Part><PartNumber>1</PartNumber><ETag>{e1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{e2}</ETag></Part></CompleteMultipartUpload>""";
        var complete = await client.SendSignedAsync("POST", $"/b-mp-cycle/mp-obj?uploadId={uploadId}",
            body: Encoding.UTF8.GetBytes(manifest));

        // Assert: составной ETag; GET-тело; Head-метаданные; List; повторный Complete — 404;
        // Delete; повторный Delete — 204; GET после Delete — 404 (полный цикл спеки §7)
        var completeXml = XDocument.Parse(await complete.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var hex1 = e1.Trim('"');
        var hex2 = e2.Trim('"');
        var expectedComposite = "2-" + Convert.ToHexString(MD5.HashData(
            Encoding.ASCII.GetBytes(hex1 + hex2))).ToLowerInvariant();
        completeXml.Root!.Element(S3Ns + "ETag")!.Value.Should().Be("\"" + expectedComposite + "\"");
        list.StatusCode.Should().Be(HttpStatusCode.OK);
        listXml.Root!.Element(S3Ns + "IsTruncated")!.Value.Should().Be("true");
        var get = await client.SendSignedAsync("GET", "/b-mp-cycle/mp-obj");
        (await get.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Length
            .Should().Be(FiveMiB.Length + 3);
        get.Headers.ETag!.Tag.Should().Be("\"" + expectedComposite + "\"");
        var head = await client.SendSignedAsync("HEAD", "/b-mp-cycle/mp-obj");
        head.StatusCode.Should().Be(HttpStatusCode.OK);
        head.Content.Headers.ContentType!.ToString().Should().StartWith("text/plain");
        // List: собранный объект листится как обычный (листинг — по xl.meta, ETag составной)
        var objects = await client.SendSignedAsync("GET", "/b-mp-cycle?list-type=2&prefix=mp-obj");
        objects.StatusCode.Should().Be(HttpStatusCode.OK);
        var objXml = XDocument.Parse(await objects.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        objXml.Root!.Element(S3Ns + "KeyCount")!.Value.Should().Be("1");
        objXml.Root!.Elements(S3Ns + "Contents").Single().Element(S3Ns + "Key")!.Value
            .Should().Be("mp-obj");
        objXml.Root!.Elements(S3Ns + "Contents").Single().Element(S3Ns + "ETag")!.Value
            .Should().Be("\"" + expectedComposite + "\"");
        // Повторный Complete завершённой загрузки — 404 NoSuchUpload (объект ещё жив:
        // исход именно «загрузка не существует», канон 02 §5)
        var again = await client.SendSignedAsync("POST", $"/b-mp-cycle/mp-obj?uploadId={uploadId}",
            body: Encoding.UTF8.GetBytes(manifest));
        again.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(again)).Should().Be("NoSuchUpload");
        // Delete multipart-объекта — как простого PUT (спека §1): 204 + идемпотентный повтор
        var del = await client.SendSignedAsync("DELETE", "/b-mp-cycle/mp-obj");
        del.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var delAgain = await client.SendSignedAsync("DELETE", "/b-mp-cycle/mp-obj");
        delAgain.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var getGone = await client.SendSignedAsync("GET", "/b-mp-cycle/mp-obj");
        getGone.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(getGone)).Should().Be("NoSuchKey");
    }

    // Кейсы того же класса (каждый — свой бакет b-mp-*):
    // Range206_AcrossPartBoundary (GET с bytes=5242878-5242882 → 206, тело = стык);
    // Conditional_IfMatchCompositeEtag (If-Match составной → 200; чужой → 412);
    // Attributes_RealParts (x-amz-object-attributes=ObjectParts → 3 части, пагинация);
    // UploadPartCopy_WholeAndRange (источник-PUT: весь объект и диапазон → Complete → Get);
    // Copy_MultipartSource_InheritsCompositeEtag (CopyObject → ETag копии = источника);
    // Abort_Sequence (Abort → 204; повторный → 404 NoSuchUpload; Complete/UploadPart/ListParts
    //   после → 404 NoSuchUpload);
    // InvalidPart_Cases (чужой ETag / дыра в номерах / первая < 5 МиБ → 400 InvalidPart);
    // InvalidPartOrder_App (манифест 2,1 → 400 InvalidPartOrder — валидация App t36);
    // EntityTooLarge_Part (UploadPart: НЕпустое тело (байт) + заголовок
    //   x-amz-decoded-content-length=5368709121 — ОБЫЧНЫЙ request-заголовок: уходит
    //   на провод и корректно входит в подпись (спуф content-length через
    //   Content.Headers ненадёжен — HttpClient может пересчитать его по факту);
    //   ObjectHandlers.ResolveContentLength читает decoded-заголовок первым,
    //   ValidateObjectSize отказывает ДО чтения тела → 400 EntityTooLarge);
    // NoSuchUpload_UnknownId (несуществующий uploadId на UploadPart/ListParts/Complete/Abort);
    // ReaderRights_CreateForbidden_ListingsFiltered (reader: POST ?uploads → 403 AccessDenied;
    //   admin создаёт бакет b-mp-ro, writer создаёт в нём загрузку: reader
    //   ListMultipartUploads → пусто, reader ListParts чужой uploadId → 404 NoSuchUpload;
    //   writer видит свою);
    // DeleteBucket_LiveUpload409_AfterAbort204 (admin создаёт бакет, writer — загрузку;
    //   DELETE бакета → 409 BucketNotEmpty; Abort → DELETE → 204);
    // ListMultipartUploads_Xml (ключи + CommonPrefixes по delimiter, маркеры).

    [Fact]
    public async Task UploadPart_ChunkedSignature_PartStored()
    {
        // Arrange: чанковая подпись тела части (полный паттерн ChunkedPut
        // BodyIntegrityScenarios — seed из подписи, тело собирает signer)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-chunked", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-chunked", "part-obj");
        var date = OwnS3AppFactory.HostTime;
        var payload = OwnS3.Protocol.Auth.PayloadHashModeClassifier.StreamingValue;
        var host = client.Http.BaseAddress!.Authority;
        var pathAndQuery = $"/b-mp-chunked/part-obj?partNumber=1&uploadId={uploadId}";
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "5"),
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT",
            "/b-mp-chunked/part-obj", "partNumber=1&uploadId=" + uploadId, headers, payload, date, "us-east-1");
        var body = Encoding.UTF8.GetBytes("hello");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, body, signature, date, "us-east-1");
        using var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Put, pathAndQuery);
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", "5");
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: сверка конвейера прошла; часть реально в журнале загрузки (ListParts)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Headers.ETag!.Tag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\""); // md5("hello")

        // Assert: часть реально в журнале загрузки (ListParts — одна часть)
        var list = await client.SendSignedAsync("GET", $"/b-mp-chunked/part-obj?uploadId={uploadId}");
        list.StatusCode.Should().Be(HttpStatusCode.OK);
        XDocument.Parse(await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Root!.Elements(S3Ns + "Part").Should().ContainSingle();
    }
}
```

- [ ] **Шаг 3. Прогнать интеграционные сценарии**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~MultipartScenarios"`
Expected: PASS (все кейсы; temp-том WAF-фабрики удаляется её Dispose).

- [ ] **Шаг 4. Прогнать ВСЕ интеграционные (регрессии t36/t37)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`
Expected: PASS (`RoutingScenarios`, `AccessScenarios`, `ObjectStorageScenarios`, `BodyIntegrityScenarios`, `ErrorFormatScenarios`, `AuthScenarios`, `FailFastScenarios`, `HealthzVolumeScenarios`, `MetricsHealthScenarios`, `MultipartScenarios`).

- [ ] **Шаг 5. Коммит**

```bash
git add src/tests/OwnS3.IntegrationTests/Api/RoutingScenarios.cs \
        src/tests/OwnS3.IntegrationTests/Api/AccessScenarios.cs \
        src/tests/OwnS3.IntegrationTests/Api/ObjectStorageScenarios.cs \
        src/tests/OwnS3.IntegrationTests/Api/MultipartScenarios.cs
git commit -m "test(owns3): интеграционные HTTP-сценарии multipart + переделка заглушечных кейсов на реальные исходы (t38, фаза 6)"
```

---

### Task 12: мерж-гейт — сборка Release, полные прогоны, чистки (фаза 7)

**Файлы:** без новых правок кода (только прогоны; правки — исключительно по фактам найденных дефектов, отдельными коммитами с обоснованием).

- [ ] **Шаг 1. Полная сборка Release**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`
Expected: 0 warnings, 0 errors (`TreatWarningsAsErrors`).

- [ ] **Шаг 2. Полный прогон юнитов**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release`
Expected: PASS, 0 упавших.

- [ ] **Шаг 3. Полный прогон интеграции (после финальной строки шага 2)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release`
Expected: PASS, 0 упавших. docker не использовался — контейнерной зачистки нет; temp-тома снесены teardown-ами тестов.

- [ ] **Шаг 4. Сверка критериев приёмки спеки**

  - Вход: спека §9 (12 пунктов).
  - Действие: пройтись по пунктам 1–12, каждому сопоставить зелёный тест (юнит/интеграция) или границу (п.12 — E2E не выполнялся, t39).
  - Выход: таблица «критерий → тест» в журнале выполнения.
  - Проверка: пункт без теста — СТОП, добавить тест (правкой соответствующей задачи) до закрытия.
  - Связь со spec: §9.

- [ ] **Шаг 5. Финальный коммит изменений (если были) и сводка**

```bash
git status --short   # ожидается: чисто (arch-правки §3 закоммичены спекой ранее)
git log --oneline -12
```

  - Выход: ветка `feat-t38-owns3-multipart` готова к ревью/мерж-гейту пользователя.
  - Проверка: `git status` чист; все серии зелёные.
  - Связь со spec: §6 фаза 7.

Примечание (исполняется на мерже в main, не в worktree): мерж-коммит снимает тег `t38-owns3-multipart` из `arch/roadmap/owns3.md` (правило AGENTS: слитая задача удаляется из roadmap тем же коммитом).

---

## Self-review плана (rev.6: правки ревью 5 Фазы 4 внесены)

1. **Покрытие спеки:** §4.1 раскладка → Task 1/3 (ассерты путей); §4.2 семь операций → Task 3 (Create/Abort), 4 (UploadPart), 5 (ListParts/ListMultipartUploads), 6–7 (Complete), 10 (UploadPartCopy); §4.3 чтение → Task 8/9; §4.4 чистки → Task 2; §4.5 видимость/права → Task 3 (тип), 5 (листинги), 11 (HTTP); §4.6 App-правки → Task 3/5 (инициатор/видимость), 10 (LastModified; п.3 шага 3 — зачистка заглушечных следов `ThrowUnavailable`/заголовка файла, grep-гейт в шаге 4), 11 (HTTP-исходы); §5 контракты → Task 3/5; §6 фазы 0–7 → Task 0–12; §7 тесты → все задачи, вкл. полный цикл Create→…→Get→Head→List→Delete (Task 11, с идемпотентным повтором Delete и GET 404 после), Range на стыке частей и РОВНО на границе (Task 8), EntityTooLarge части (Task 11, спуф Content-Length) и диапазона UploadPartCopy > 5 ГБ (Task 10, посев xl.meta с Size 5 ГБ+1 — без 5 ГБ IO, правило ≤ 30 с), чанковую подпись UploadPart (Task 11, паттерн ChunkedPut), видимость листингов в ОБЕИХ ветках — OwnedBy-фильтр чужой загрузки и AllUploads-позитив «4 записи включая admin'скую» (Task 5, §9.9); CP-маркерная пагинация ListMultipartUploads без дублей префиксов (Task 5, тест ListMultipartUploads_CpMarkerPagination_NoDuplicatePrefixes); §9 критерии → Task 12 шаг 4. Пробелов нет.
2. **Плейсхолдеры:** шаги с пометкой «Аналогично» содержат полный образец кода и точный перечень кейсов — это расширение образца, а не отложенное решение; иных заглушек нет.
3. **Компиляция юнит-проекта на каждом шаге:** существующие заглушечные кейсы t36 в `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreBucketTests.cs` (`MultipartMethods_ThrowUnavailable` + `MultipartInvocations` + `UploadPart_DrainsBodyBeforeThrowing` + `CountingStream`) удаляются Task 3 шагом 3 п.5 — единственный тест-файл вне нового класса, вызывающий multipart-методы IObjectStore (проверено grep по src/tests); их сигнатуры ломаются контрактными правками Task 3/5, семантика исчезает по §4.6.3/§9.1. grep-гейт Task 10 шага 4 контролирует отсутствие заглушечных следов в Storage-коде И юнит-тестах; контрольный grep по worktree подтверждает полноту зачистки области гейта (XlObjectStore.Multipart.cs: шапка+броски — Task 3/4/5/6/7/10; XlObjectStoreBucketTests.cs: шапка ~8 и кейсы 133–189 — Task 3 п.5). Вне области гейта остаются: ObjectStoreException.cs (тип ObjectStoreUnavailableException — §4.6.4, без изменений) и интеграционные комментарии (правятся Task 11 п.1–6). Сопутствующие комментарии синхронизированы: XlVolume.RunCleanupAsync (Task 2), шапки/кейсы Routing/Access/ObjectStorage (Task 11), EntityTooLarge-спуф — через x-amz-decoded-content-length (обычный request-заголовок, входит в подпись).
4. **Типы/сигнатуры:** `CreateMultipartUploadAsync(..., initiatorAccessKey, ct)` — Task 3, используется Task 4–10 тестами; `ListPartsAsync(..., visibility, ct)` и `UploadsQuery(..., Visibility)` — Task 5, используется Task 7/8 тестами; `ReadPartsOrThrow(uploadDir)` — Task 4 (первое использование — UploadPart), переиспользуется Task 5 (ListParts), 6–7 (Complete): битый `parts.json` → warning + `NoSuchUpload` единообразно (спека §4.1; замечание ревью №5 — план приведён к spec, правка spec не требуется); `EnumerateDataParts` — Task 8, используется Task 9/10; `CompletePreCommitProbe` — Task 7. Конфликт имени `PartEntry` доменный/журнальный разрешён: журнальный — `MultipartJournals.PartJournalEntry` (вложен в internal-класс). `UploadVisibility` вводится Task 3 (раньше первого использования в сигнатуре `ResolveUploadOrThrow`). `MultipartBodyStream`: стартовый индекс/смещение вычисляет конструктор (вкл. случай startOffset ровно на границе части — старт со следующей части с позиции 0), первое открытие — ровно по вычисленному индексу без инкремента (`_openedIndex`), переходы последовательны (замечание ревью №1; покрыто тестами стыка `RangeAcrossPartBoundary` и границы `RangeStartsExactlyAtPartBoundary`).
