# План t39-owns3-e2e: приёмочная грань ownS3 + снятие If-Range и UploadPartCopy

> **Для исполнителей:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (рекомендуется)
> или superpowers:executing-plans — выполнять по задаче за раз. Шаги — чекбоксы (`- [ ]`).

**Цель:** приёмочная грань ownS3 — docker-E2E серия реальными клиентами (AWS SDK .NET, `mc`), dev-стенд-сервис `owns3` в докере, health/метрики в докер-контуре; плюс полное снятие с ownS3 граней, недостижимых целевыми клиентами: `If-Range` (заголовок чтения) и серверной операции `UploadPartCopy` (набор операций — 21; решения пользователя Q1/Q2, spec §9.1–9.2).

**Архитектура:** серия `src/tests/OwnS3.IntegrationTests/E2e` — per-Fact окружение `OwnS3E2eEnvironment` (своя сеть `os3-en-{guid}`, контейнер `os3-es-{guid}`, том `os3-vd-{guid}`, динамический порт, wait-стратегия `/healthz`, телеметрия до удалений, MarkFailed → stop-без-удаления, ассерт чистоты); образ `owns3:e2e`/`owns3:dev` — publish-COPY (`docker/OwnS3.Dockerfile`, `ARG OWNS3_PUBLISH_DIR`); стенд-сервис в `deploy/docker-compose.yml` (env-секреты из `deploy/.env`, порт `${OWNS3_HOST_PORT:-9010}:9000`, подъём в `00-up.sh`, чек `46-owns3.sh`); зачистка — `RangeResolver.Resolve(range, size)`, `ObjectReadOptions(Conditions, Range)`, без чтения `If-Range`; изъятие UploadPartCopy целиком (маршрут/enum/хендлер/валидация/матрица/DI/presigned/XML-тип/домен/Storage) — остаточные запросы матчатся как `UploadPart` (заголовок `x-amz-copy-source` игнорируется, канон 02 §5).

**Стек:** .NET 10 (`TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions, Testcontainers 4.14.0, AWSSDK.S3 3.7.511.8 (версии централизованы в `src/Directory.Packages.props`), docker CLI, `minio/mc:RELEASE.2025-08-13T08-35-41Z` (уже в `dev-stand/images/images.txt`).

**Spec:** `docs/superpowers/2026-10-10-t39-owns3-e2e/spec.md` (v3, утверждён на повторном гейте user-review; включает решения Q1 — снятие If-Range, Q2 — снятие UploadPartCopy). Исполнители читают spec и план вместе.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t39-owns3-e2e`, ветка `feat-t39-owns3-e2e`. Все команды — из корня worktree, если не указано иное.

## Глобальные ограничения (spec §2, §7; AGENTS.md; AGENTS.base.md §13)

- Код сервиса `src/OwnS3.{Protocol,Storage,App}` правится ТОЛЬКО в части зачистки If-Range/UploadPartCopy (spec §4.6, §7 — решения пользователя Q1/Q2, расширившие scope). Иной дефект, вскрытый E2E, — отдельный пункт с обоснованием против канона, не «подгонка теста».
- Каноны уже поправлены спекой v3 в worktree (6 файлов, §3.1–3.4): счёт операций 21, контракты If-Range/UploadPartCopy изъяты. Arch-first соблюдён: код зачищается ПОСЛЕ канона; правки канона коммитятся ВМЕСТЕ с зачисткой кода (Задача 1).
- E2E-образы: `dotnet publish` на хосте, в контейнер — только publish-вывод; никаких `COPY src/` и sdk-стадий; теги `owns3:*` в registry `192.168.0.1:5000` НЕ кладутся; новых строк `images.txt` нет.
- Изоляция: per-Fact окружение, guid (`runId`) во всех именах; только динамические порты (зонд `TcpListener :0`); teardown при любом исходе; ассерт чистоты; own-only чистка (голые префиксы `os3-*` в фильтрах удаления запрещены — только по `{runId}`).
- Телеметрия (`docs/e2e-launch.md`): `[PHASE]`-метки с таймингом; docker logs/inspect ДО удалений; упавший прогон — контейнер остановлен, не удалён (`MarkFailed`), чистка вручную по `README-cleanup.txt`; перезапуск упавших тестов без анализа логов запрещён.
- Гейты: `PGW_TEST_DOCKER=1` — docker-серия (без неё Facts — Skip); `PGW_TEST_E2E_NOBUILD=1` — пропуск сборки образа (только бисект).
- Таймауты: подъём инстанса ≤ 30 с (wait-стратегия); любой полл/ожидание ≤ 30 с с конечным бюджетом; никаких sleep в ожидание готовности.
- Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings; документация/комментарии — русские, идентификаторы — английские; тесты — AAA-нотация, ассерты FluentAssertions.
- Коммиты — свободно (feature-ветка); мерж в `main` — только по явной команде пользователя.
- После КАЖДОЙ тестовой серии — дождаться финальной строки прогона и зачистить остатки перед следующей серией.

## Факты AWSSDK.S3 3.7.511.8, определяющие код плана

Проверены 2026-10-10 по пакетам из NuGet-кеша (XML-дока + компиляционная проба тестового проекта против `AWSSDK.S3 3.7.511.8`). Исполнитель НЕ «угадывает» имена членов SDK — ниже проверенные факты:

1. **Flexible checksums — per-client**: `AmazonS3Config.RequestChecksumCalculation` (наследуется от `Amazon.Runtime.ClientConfig`), enum `Amazon.Runtime.RequestChecksumCalculation { WHEN_REQUIRED, WHEN_SUPPORTED }` (дефолт SDK — `WHEN_SUPPORTED`). Глобальной статики `Amazon.Util.AWSConfigs.RequestChecksumCalculation` в AWSSDK.Core 3.7.501.1 НЕТ (компиляция — ошибка; в XML-доке отсутствует). План реализует режимы единственным компилируемым способом — per-client настройкой в фабрике (Задача 7); прочие элементы спеки (последовательная коллекция §4.7, оба режима, отдельный WHEN_SUPPORTED-факт) — по букве спеки.
2. **If-Range недоступен** (решение Q1, spec §9.1 — снята полностью): в `GetObjectRequest` свойства If-Range нет (полный список свойств: `EtagToMatch`/`EtagToNotMatch`/`ModifiedSinceDate(Utc)`/`UnmodifiedSinceDate(Utc)`/`ByteRange`/`PartNumber`/…).
3. **UploadPartCopy недоступен** (решение Q2, spec §9.2 — снята полностью): типа `UploadPartCopyRequest` и метода `UploadPartCopyAsync` в пакете НЕТ (компиляция — CS0246; 121 вхождение «UploadPartCopy» в XML-доке — только тексты ссылок); `mc` операцию не использует.
4. **Имена API**: CreateBucket → `PutBucketRequest`/`PutBucketAsync`; HEAD-объект → `GetObjectMetadataRequest`/`GetObjectMetadataAsync`; `GetObjectRequest.ByteRange` — тип `Amazon.S3.Model.ByteRange` (НЕ строка): `new ByteRange("bytes=10-20")`.
5. `GetObjectAttributesRequest.ObjectAttributes` — `List<Amazon.S3.ObjectAttributes>` (enum `ObjectAttributes.ETag/ObjectSize/StorageClass/ObjectParts`).
6. `CompleteMultipartUploadRequest.PartETags` — `List<PartETag>`; конструктор `new PartETag(int partNumber, string eTag)`.
7. `AmazonS3Exception` несёт `ErrorCode` (string) и `StatusCode`.
8. **ETag-форма**: `PutObjectResponse.ETag`/`GetObjectResponse.ETag`/`GetObjectMetadataResponse.ETag` — БЕЗ кавычек; conditional-заголовки (`EtagToMatch`/`EtagToNotMatch`) — строка С кавычками; `CompleteMultipartUploadResponse.ETag` — с кавычками (сравнение через `Trim('"')`).

---

### Задача 1: Зачистка If-Range и UploadPartCopy в коде и тестах (spec §4.6, §5 фаза 1)

**Вход:** worktree с arch-правками спеки v3 (§3.1–3.4: каноны 01/02/03/05, входная точка `22-owns3.md`, roadmap-шапка — счёт 21, контракты изъяты; правки уже на диске, коммитятся вместе с кодом в шаге 1.27). В коде: If-Range-механика распределена по 8 файлам (`IfRange`/`If-Range`/локальные `ifRange` — все вхождения закрывает grep-гейт шага 1.24); UploadPartCopy/CopyPartResult — вхождения в 11 файлах `src/OwnS3.*` и 7 тестовых файлах (якоря ниже; сверено grep'ом).

**Files (Modify; точные якоря в шагах):**
- If-Range: `src/OwnS3.Storage/Domain/DomainTypes.cs`, `src/OwnS3.Storage/RangeResolver.cs`, `src/OwnS3.Storage/XlObjectStore.Objects.cs`, `src/OwnS3.App/Handlers/ObjectHandlers.cs`, `src/tests/OwnS3.UnitTests/Storage/RangeResolverTests.cs`, `XlObjectStoreObjectTests.cs`, `XlObjectStoreMultipartTests.cs`, `src/tests/OwnS3.IntegrationTests/Api/ObjectStorageScenarios.cs`
- UploadPartCopy: `src/OwnS3.App/Routing/S3Operation.cs`, `S3Router.cs`, `src/OwnS3.App/Handlers/MultipartHandlers.cs`, `OperationValidation.cs`, `src/OwnS3.App/Access/OperationAccessMatrix.cs`, `src/OwnS3.App/Program.cs`, `src/OwnS3.Protocol/Auth/S3PresignedOperations.cs`, `src/OwnS3.Protocol/Xml/S3XmlModels.cs`, `src/OwnS3.Storage/IObjectStore.cs`, `XlObjectStore.Multipart.cs`, `MultipartJournals.cs`, `Domain/DomainTypes.cs` (тип `PartCopyRequest`), тесты: `XlObjectStoreMultipartTests.cs`, `S3RouterTests.cs`, `HandlersValidationTests.cs`, `OperationAccessMatrixTests.cs`, `S3XmlTests.cs`, `Api/MultipartScenarios.cs`, `Api/RoutingScenarios.cs`.

**Interfaces (Produces):** `ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range)`; `RangeResolver.Resolve(ByteRange? range, long size)`; `IObjectStore` без `UploadPartCopyAsync`; enum `S3Operation` без `UploadPartCopy`; набор операций — 21.

**Часть A — If-Range:**

- [ ] **Шаг 1.1. Контрольная сверка канона (spec §5 фаза 0):**

```bash
grep -n "If-Range" arch/owns3/02-operations.md
grep -n "21 операци\|UploadPartCopy" arch/22-owns3.md arch/owns3/01-overview.md arch/owns3/02-operations.md arch/owns3/03-protocol.md arch/owns3/05-access-config.md arch/roadmap/owns3.md | head
```
Критерий: 02 §1 — формулировка «не поддерживается/игнорируется», If-Range отсутствует в заголовках GetObject; счёт «21 операция» на месте, контракт UploadPartCopy из канонов изъят (вхождения UploadPartCopy — только в фиксациях снятия в 02). Расхождение — СТОП, чинить канон до кода (arch-first).

- [ ] **Шаг 1.2. `Domain/DomainTypes.cs:21` — убрать поле `IfRange`:**

```csharp
// БЫЛО:
public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range, string? IfRange);
// СТАЛО:
public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range);
```

- [ ] **Шаг 1.3. `RangeResolver.cs` — заменить файл целиком:**

```csharp
namespace OwnS3.Storage;

// Резолв одиночного Range канона 02 §1: a-b, a-, -N; обрезка end по размеру,
// суффикс длиннее объекта, пустой объект. If-Range не поддерживается
// (канон 02 §1): заголовок игнорируется — ответ определяется только Range.
public static class RangeResolver
{
    public static AppliedByteRange? Resolve(ByteRange? range, long size)
    {
        if (range is null)
            return null;
        if (size == 0)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
        if (range.Start is { } start)
        {
            if (start >= size)
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
            var end = range.End is null || range.End >= size ? size - 1 : range.End.Value;
            return new AppliedByteRange(start, end, size);
        }
        var suffix = -range.End!.Value;        // ParseRange кодирует bytes=-N как End=-N
        if (suffix <= 0)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
        return suffix >= size ? new AppliedByteRange(0, size - 1, size)
                              : new AppliedByteRange(size - suffix, size - 1, size);
    }
}
```

Ветвь `ConditionalEvaluator.ETagMatches(ifRange, …)` и зависимость резолва от `ConditionalEvaluator` удаляются; сам `ConditionalEvaluator` остаётся (conditional-семантики канона 02 §1 без изменений).

- [ ] **Шаг 1.4. `XlObjectStore.Objects.cs` — два вызова + комментарии:**

```csharp
// Строка ~189, комментарий — БЫЛО «…Range/If-Range, полная» → СТАЛО «…Range, полная»
// Строка ~225, БЫЛО:
//        // 3. Range/If-Range (сверка — с ETag всего объекта, P8)
//        var range = RangeResolver.Resolve(options.Range, options.IfRange, meta.ETag, meta.Size);
// СТАЛО:
//        // 3. Range (If-Range не поддерживается — канон 02 §1)
//        var range = RangeResolver.Resolve(options.Range, meta.Size);
// Строка ~250, БЫЛО:
//        var range = RangeResolver.Resolve(options?.Range, options?.IfRange, meta.ETag, meta.Size);
// СТАЛО:
//        var range = RangeResolver.Resolve(options?.Range, meta.Size);
```

- [ ] **Шаг 1.5. `ObjectHandlers.cs` — убрать чтение заголовка:** комментарий ~321 «conditional + Range + If-Range …» → «conditional + Range …»; строки ~336–337:

```csharp
// БЫЛО:
//     var ifRange = context.Http.Headers["If-Range"].FirstOrDefault();
//     return new ObjectReadOptions(conditions, range, ifRange is { Length: > 0 } ? ifRange : null);
// СТАЛО:
//     return new ObjectReadOptions(conditions, range);
```

- [ ] **Шаг 1.6. `RangeResolverTests.cs`:** сигнатуры оставшихся кейсов → `Resolve(range, size)` (строки 15, 26, 28, 37, 45, 47, 55, 66, 77); удалить методы `IfRange_Match_AppliesRange` (80–88), `IfRange_Mismatch_FullObject` (90–95), `IfRange_DateForm_TreatedAsMismatch` (97–105), `IfRange_Mismatch_MakesEvenInvalidRange_Harmless` (107–114); удалить константу `Etag` (строка 9) и «If-Range» из шапки-комментария (5–6).

- [ ] **Шаг 1.7. `XlObjectStoreObjectTests.cs`:** удалить `Get_IfRange_Match206` (304–320), `Get_IfRange_Mismatch200` (322–337), `Get_IfRange_DateForm200` (339–355); прочие `ObjectReadOptions(...)` с тремя аргументами сократить до двух (компилятор перечислит).

- [ ] **Шаг 1.8. `XlObjectStoreMultipartTests.cs` (If-Range-часть):** удалить `Get_MultipartObject_Range_IfRangeMismatch_Full200` (879–899).

- [ ] **Шаг 1.9. `Api/ObjectStorageScenarios.cs`:** в conditional-факте удалить If-Range-блок целиком (строки 209–219: два `SendSignedAsync` с `If-Range` и три ассерта). Conditional-ассерты выше (304/412) остаются.

**Часть B — UploadPartCopy:**

- [ ] **Шаг 1.10. `Routing/S3Operation.cs`** — удалить строку `    UploadPartCopy,` из enum (строка 25) и поправить комментарий шапки (строка 3): «22 операции» → «21 операция».

- [ ] **Шаг 1.11. `Routing/S3Router.cs:77`** — удалить строку маршрута:

```csharp
// БЫЛО:
//            "PUT" when hasPartNumber && hasUploadId && copySource => new S3Route(S3Operation.UploadPartCopy, path.Bucket, path.Key, null),
//            "PUT" when hasPartNumber && hasUploadId => new S3Route(S3Operation.UploadPart, path.Bucket, path.Key, null),
// СТАЛО (остаётся ТОЛЬКО вторая строка):
//            "PUT" when hasPartNumber && hasUploadId => new S3Route(S3Operation.UploadPart, path.Bucket, path.Key, null),
```
Запрос `PUT ?partNumber&uploadId` + `x-amz-copy-source` после этого матчится как `UploadPart` (заголовок игнорируется — новая формулировка канона 02 §5). Локальная переменная `copySource` остаётся (маршрут CopyObject, строка ~79). Там же поправить комментарий шапки файла (строка 5): «Роутер 22 операций» → «Роутер 21 операции».

- [ ] **Шаг 1.12. `Handlers/MultipartHandlers.cs:72–106`** — удалить класс `UploadPartCopyHandler` целиком (от строки 72 до начала `CompleteMultipartUploadHandler` ~107), включая `CopyPartResult`-ответ. Там же поправить шапку-комментарий файла (строки 12–14, перечень операций): убрать `UploadPartCopy` из перечня («…CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads…») — иначе его поймает grep-гейт 1.24.

- [ ] **Шаг 1.13. `Handlers/OperationValidation.cs`** — удалить `ParseCopySourceRange` (строки ~95–110: комментарий «x-amz-copy-source-range … глава 02 UploadPartCopy» + метод). Использовался только `UploadPartCopyHandler`. `ParseCopySource` (80–94) ОСТАЁТСЯ — используется CopyObject (его copy-условия `x-amz-copy-source-if-*` на объектном endpoint — без изменений, spec §4.6).

- [ ] **Шаг 1.14. `Access/OperationAccessMatrix.cs:24`** — из списка строки матрицы multipart удалить `S3Operation.UploadPartCopy,`:

```csharp
// БЫЛО:
//            S3Operation.CreateMultipartUpload, S3Operation.UploadPart, S3Operation.UploadPartCopy,
// СТАЛО:
//            S3Operation.CreateMultipartUpload, S3Operation.UploadPart,
```

- [ ] **Шаг 1.15. `Program.cs` + `Pipeline/S3Middleware.cs`** — удалить DI-регистрации `UploadPartCopyHandler` (строки 70 и 110) и комментарий строки 52 «22 хендлера протокольного контура (spec §3.4.1)» → «21 хендлер протокольного контура». Там же `Pipeline/S3Middleware.cs`: комментарий строки 29 «(22 хендлера протокольного контура)» → «(21 хендлер…)»; комментарий строки 61 «Шаг 3: роутер 22 операций» → «Шаг 3: роутер 21 операции».

- [ ] **Шаг 1.16. `Protocol/Auth/S3PresignedOperations.cs`** — удалить строку `"UploadPartCopy",` из presigned-разрешённых (набор — канон 03 §2) и поправить комментарий шапки (строки 3–4): «ровно 10» → «ровно 9».

- [ ] **Шаг 1.17. `Protocol/Xml/S3XmlModels.cs:252–253`** — удалить тип `CopyPartResult` (атрибут `[XmlRoot("CopyPartResult", …)]` + объявление класса + тело; XML-схема изъята из канона 03 §4).

- [ ] **Шаг 1.18. `Storage/IObjectStore.cs`** — удалить строку контракта `Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct);` и поправить комментарий шапки (строка 3): «Контракт 22 операций» → «Контракт 21 операции».

- [ ] **Шаг 1.19. `Storage/XlObjectStore.Multipart.cs`** — удалить метод `UploadPartCopyAsync` (строки ~168–230, включая комментарий-шапку «UploadPartCopy (канон 02 §5, спека §4.2)…») и приватный хелпер `CopySliceToFileAsync` (~231 и далее; использовался только копированием — других вызовов нет).

- [ ] **Шаг 1.20. `Storage/MultipartJournals.cs:50–52`** — комментарий `PartTmpFileNameUnique` без UploadPartCopy:

```csharp
// БЫЛО:
//    // Уникальный tmp на попытку записи части (ревью t38): параллельные
//    // UploadPart/UploadPartCopy одного partNumber пишут в РАЗНЫЕ tmp-файлы —
// СТАЛО:
//    // Уникальный tmp на попытку записи части (ревью t38): параллельные
//    // UploadPart одного partNumber пишут в РАЗНЫЕ tmp-файлы —
```
(метод `PartTmpFileNameUnique` остаётся — параллельные UploadPart одного partNumber).

- [ ] **Шаг 1.21. `Storage/Domain/DomainTypes.cs:37–39`** — удалить тип `PartCopyRequest` (после шага 1.18 не используется нигде, кроме удаляемых тестов; «полное изъятие механики, не оставить мёртвым» — spec §4.6):

```csharp
// УДАЛИТЬ:
//public sealed record PartCopyRequest(string SourceBucket, string SourceKey,
//    string DestBucket, string DestKey, string UploadId, int PartNumber,
//    ByteRange? SourceRange, ObjectConditions? SourceConditions);
```
(`CopyRequest` выше в файле ОСТАЁТСЯ — CopyObject.)

- [ ] **Шаг 1.22. Тесты UploadPartCopy:**
  - `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` — удалить 7 методов: `UploadPartCopy_WholeObject_EtagIsRangeMd5` (1006), `UploadPartCopy_RangeFromMultipartSource_PartAssemblesToObject` (1032), `UploadPartCopy_RangeBeyondSource_InvalidArgument` (1064), `UploadPartCopy_SourceConditionFailed_PreconditionFailed` (1081), `UploadPartCopy_UnknownUpload_NoSuchUpload` (1100), `UploadPartCopy_MissingSource_NoSuchKey` (1116), `UploadPartCopy_RangeOver5Gb_EntityTooLarge` (1132–~1160).
  - `src/tests/OwnS3.UnitTests/S3RouterTests.cs:38` — удалить Theory-строку `{ "PUT", "/b/k", "partNumber=1&uploadId=x", true,  S3Operation.UploadPartCopy },`.
  - `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` — комментарий строки 11 «partNumber (UploadPart/UploadPartCopy)» → «partNumber (UploadPart)»; удалить тест `ParseCopySourceRange_Valid_AndInvalid` (267–279; метод уходит шагом 1.13).
  - `src/tests/OwnS3.UnitTests/OperationAccessMatrixTests.cs:24` — актуализировать ожидаемый список (убрать `S3Operation.UploadPartCopy,`).
  - `src/tests/OwnS3.UnitTests/S3XmlTests.cs` — удалить `Serialize_CopyPartResult_MatchesSample` (198–204).
  - `src/tests/OwnS3.IntegrationTests/Api/MultipartScenarios.cs` — удалить Fact `UploadPartCopy_WholeAndRange` (215 и далее, целиком).
  - `src/tests/OwnS3.IntegrationTests/Api/RoutingScenarios.cs` — Fact `UploadPart_And_UploadPartCopy_ReachStorage` (102–122): удалить uploadPartCopy-Act (запрос с `x-amz-copy-source`) и его два ассерта; переименовать в `UploadPart_ReachesStorage`; комментарий шапки файла (строка 8) «Роутинг 22 операций» → «Роутинг 21 операции»:

```csharp
    [Fact]
    public async Task UploadPart_ReachesStorage()
    {
        // Arrange
        var client = NewClient();

        // Act
        var uploadPart = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            body: Encoding.UTF8.GetBytes("part"));

        // Assert: бакет не существует — реальный исход Storage (404 NoSuchBucket)
        uploadPart.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(uploadPart)).Should().Be("NoSuchBucket");
    }
```

  Комментарии-счётчики в прочих тестовых шапках (канон 02 — «21 операция»): `S3RouterTests.cs` строки 5 («Таблица маршрутов 22 операций») и 76 («вне набора 22 операций»), `OperationAccessMatrixTests.cs` строка 6 («3 роли × 22 операции») — заменить 22 → 21.

- [ ] **Шаг 1.23. Presigned-Theory `Api/AuthScenarios`** — прогнать; если Theory падает от сужения presigned-набора (шаг 1.16) — актуализировать ожидаемый список по канону 03 §2 (spec §4.6, оговорка). Прямых вхождений UploadPartCopy в файле нет (сверено grep'ом) — ожидается зелёный без правок.

**Часть C — гейты:**

- [ ] **Шаг 1.24. Grep-гейт (spec §4.6, расширенный шаблон):**

```bash
grep -rin "ifrange\|if-range\|uploadpartcopy\|copypartresult" src/OwnS3.Protocol src/OwnS3.Storage \
  src/OwnS3.App src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests --include="*.cs"
grep -rn "22 операц\|22 хендлера" src/OwnS3.Protocol src/OwnS3.Storage \
  src/OwnS3.App src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests --include="*.cs"
```
Критерий: обе команды — пусто (0 строк; первая — регистронезависимо; вторая ловит устаревающие счётчики «22 операции/хендлера» в комментариях — канон 02 «21 операция»).

- [ ] **Шаг 1.25. Сборка и прогоны без docker:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build
```
Критерий: build — 0 warnings; оба прогона зелёные (conditional-семантики, CopyObject и прочие multipart-операции не регрессировали — критерий spec §8 п.9; отдельные тесты «игнорирования» снятых граней не заводятся — механики нет, spec §4.6).

- [ ] **Шаг 1.26. Сверка остаточной семантики маршрута (руководство, не тест):** запрос `PUT /b/k?partNumber&uploadId` с заголовком `x-amz-copy-source` теперь маршрутизируется как `UploadPart` (строка 78 S3Router) — заголовок не читается вовсе; это ровно канон 02 §5. Проверка фактом: в `S3Router` не осталось ветки с `hasPartNumber && hasUploadId && copySource`.

- [ ] **Шаг 1.27. Коммит (код + канон-правки спеки v3 одним коммитом):**

```bash
git add -A
git commit -m "feat(t39): снять If-Range и UploadPartCopy с ownS3 полностью — 21 операция (канон 02/03, решения Q1/Q2; grep-гейт)"
```

**Выход:** обе грани изъяты (механика, контракты, покрытие); сборка чистая; юниты+интеграция зелёные.
**Проверка:** шаги 1.24–1.25.
**Связь со spec:** §1 п.4, §3.3–3.4, §4.6 (обе таблицы целиком + PartCopyRequest по принципу «полного изъятия»), §5 фаза 1, §8 п.9/п.12.

---

### Задача 2: Docker-образ `docker/OwnS3.Dockerfile` + env-маппинг канона 05 §4 + ручная CLI-проверка (spec §4.1, §5 фаза 2)

**Вход:** Задача 1 слита (969aa93e, все проверки зелёные); прецедент `docker/Metrics.SdGenerator.Dockerfile`; `mcr.microsoft.com/dotnet/aspnet:10.0` в `images.txt`. На шаге 2.2 Фазы 6 вскрыт дефект вне плана: OwnS3.App не читает канонические env-имена `OWNS3_*` (arch/owns3/05 §4) — стандартный .NET-провайдер ждёт `OwnS3__Root__User` и т.п., трансляции в коде нет → в докере с каноническими env сервис падает на fail-fast валидации (`OptionsValidationException`, `src/OwnS3.App/Program.cs`). Тесты t36–t38 задавали секцию `OwnS3:*` in-memory (`OwnS3AppFactory.cs`) — дефект не был виден до первого докер-запуска. Фикс — шаг 2.2a (одобрен пользователем: вариант 1 — env-маппинг в коде; основание — base-rules §1 «arch — источник истины, код обязан зеркалить канон» и spec §7 «иной фикс — отдельным пунктом плана с обоснованием»).

**Files:**
- Create: `docker/OwnS3.Dockerfile`
- Modify: `src/OwnS3.App/Program.cs` (шаг 2.2a — трансляция env-имён канона 05 §4; arch-first: канон уже требует эти имена, код приводится к канону, канон не меняется)

**Interfaces (Produces):** образ `owns3:e2e` (CLI-режим: publish → `docker build … artifacts/e2e/owns3`) — использует Задача 5; compose-режим (`context: ..`, `args.OWNS3_PUBLISH_DIR: artifacts/owns3/publish`) — использует Задача 3.

- [ ] **Шаг 2.1. Создать `docker/OwnS3.Dockerfile`** — дословно из spec §4.1:

```dockerfile
# syntax=docker/dockerfile:1
# Образ ownS3 (arch/owns3/01 §5, 05 §6): dotnet publish НА ХОСТЕ, в контейнер —
# ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников; канон E2E-образов).
# CLI (узкий контекст — каталог артефактов):
#   dotnet publish src/OwnS3.App/OwnS3.App.csproj -c Release -o artifacts/e2e/owns3
#   docker build -f docker/OwnS3.Dockerfile -t owns3:e2e artifacts/e2e/owns3
# Compose (контекст — корень репо, ARG указывает путь вывода):
#   build: { context: .., dockerfile: docker/OwnS3.Dockerfile,
#            args: { OWNS3_PUBLISH_DIR: artifacts/owns3/publish } }
ARG OWNS3_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG OWNS3_PUBLISH_DIR
WORKDIR /app
COPY ${OWNS3_PUBLISH_DIR} ./
# Порт S3-грани — OwnS3:Server:Port (дефолт 9000; HTTP-only, path-style).
EXPOSE 9000
ENTRYPOINT ["dotnet", "OwnS3.App.dll"]
```

(В файл НЕ добавлять: `ASPNETCORE_HTTP_PORTS` — Program сам слушает 9000 через `app.Urls`; HEALTHCHECK — curl в aspnet-образе отсутствует, готовность — внешние пробы.)

- [ ] **Шаг 2.2a. Env-маппинг канона 05 §4 (фикс дефекта Фазы 6) — ДО запуска контейнера шага 2.2.**

**Действие:** в `src/OwnS3.App/Program.cs` сразу после `var builder = WebApplication.CreateBuilder(args);` и ДО `builder.Services.Configure<OwnS3Options>(…)` добавить трансляцию канонических env-имён (arch/owns3/05 §4) в конфиг-ключи `OwnS3:*` — env приоритетнее appsettings:

```csharp
// Env-имена канона arch/owns3/05 §4 (OWNS3_*): стандартный .NET-провайдер
// ждёт OwnS3__Root__User — транслируем канонические имена в ключи OwnS3:*
// (env приоритетнее appsettings; t39, фикс дефекта первого докер-запуска).
foreach (var (env, key) in new[]
         {
             ("OWNS3_ROOT_USER", "OwnS3:Root:User"),
             ("OWNS3_ROOT_PASSWORD", "OwnS3:Root:Password"),
             ("OWNS3_SERVER_PORT", "OwnS3:Server:Port"),
             ("OWNS3_DATA_DIR", "OwnS3:DataDir"),
             ("OWNS3_HOST_ID", "OwnS3:HostId"),
         })
{
    var value = Environment.GetEnvironmentVariable(env);
    if (!string.IsNullOrEmpty(value))
        builder.Configuration[key] = value;
}
// Массив статических ключей OWNS3_ACCESS_KEYS__<i>__* (глава 05 §2:
// индексная нумерация массива в env — конвенция .NET): сканируем до
// первого пропуска индекса.
for (var i = 0; ; i++)
{
    var access = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__ACCESSKEY");
    var secret = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__SECRETKEY");
    if (string.IsNullOrEmpty(access) && string.IsNullOrEmpty(secret))
        break;
    builder.Configuration[$"OwnS3:AccessKeys:{i}:AccessKey"] = access;
    builder.Configuration[$"OwnS3:AccessKeys:{i}:SecretKey"] = secret;
    var policy = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__POLICY");
    if (!string.IsNullOrEmpty(policy))
        builder.Configuration[$"OwnS3:AccessKeys:{i}:Policy"] = policy;
}
```

После правки — пересобрать publish-вывод и образ (команды шага 2.2 выполняют это заново; publish инкрементален). Существующие Api-тесты (задают `OwnS3:*` in-memory) не затронуты: трансляция только добавляет источник конфигурации из env, при отсутствии `OWNS3_*` поведение прежнее.

**Выход:** сервис в докере с каноническими env `OWNS3_*` проходит fail-fast валидацию и поднимается; env-имена канона 05 §4 читаются кодом (код зеркалит канон).

**Проверка:** прогон шага 2.2 (docker run с `-e OWNS3_ROOT_USER/-e OWNS3_ROOT_PASSWORD`) → `/healthz` 200 и в `docker logs os3-manual-check` нет `OptionsValidationException`; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings; юниты+интеграция OwnS3 зелёные (регрессии in-memory-конфигурации нет):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build
```

**Связь со spec:** §7 (отдельный пункт плана с обоснованием против канона — arch-first: канон 05 §4 уже требует имена `OWNS3_*`, код приводится к канону, канон не меняется); предусловие критерия §8 п.7 (стенд-сервис: compose передаёт `OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD`) и §8 п.1 (E2E-окружение задаёт `OWNS3_*`-env контейнеру).

- [ ] **Шаг 2.2. Ручная CLI-проверка образа** (до интеграции в тесты; порт 19099 — только этот одноразовый ручной прогон, тесты используют динамические):

```bash
echo ">>> [PHASE] publish OwnS3.App ($(date +%H:%M:%S))"
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/OwnS3.App/OwnS3.App.csproj \
  -c Release -o artifacts/e2e/owns3 --nologo
echo ">>> [PHASE] publish готов ($(date +%H:%M:%S))"
docker build -f docker/OwnS3.Dockerfile -t owns3:e2e artifacts/e2e/owns3
docker volume create os3-manual-check >/dev/null
docker run -d --name os3-manual-check -p 127.0.0.1:19099:9000 \
  -e OWNS3_ROOT_USER=os3manualroot -e OWNS3_ROOT_PASSWORD=os3manual-pwd \
  -e OWNS3_HOST_ID=os3-manual -v os3-manual-check:/data owns3:e2e
for i in $(seq 1 30); do curl -sf http://localhost:19099/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://localhost:19099/healthz && echo " HEALTHZ-OK"
docker rm -f os3-manual-check && docker volume rm -f os3-manual-check
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: publish/build успешны; `HEALTHZ-OK`; списки пусты после зачистки.

- [ ] **Шаг 2.3. Коммит:**

```bash
git add docker/OwnS3.Dockerfile src/OwnS3.App/Program.cs
git commit -m "feat(t39): docker/OwnS3.Dockerfile (publish-COPY, CLI+compose) + env-маппинг канона 05 §4 (OWNS3_* → OwnS3:*, фикс докер-старта)"
```

**Выход:** рецепт образа в репо; код читает канонические env канона 05 §4; доказано — контейнер из publish-вывода стартует с `OWNS3_*` и отвечает `/healthz`.
**Проверка:** шаги 2.2a–2.2.
**Связь со spec:** §4.1, §5 фаза 2, §7 (фикс 2.2a), §8 п.7/п.8.

---

### Задача 3: Dev-стенд-сервис owns3 (compose + env-шаблон + подъём 00-up.sh) (spec §4.2, §5 фаза 3)

**Вход:** Задача 2 (Dockerfile); `deploy/.env` — per-install, в `.gitignore` (строки 28–30 `.gitignore`).

**Files:**
- Modify: `deploy/docker-compose.yml` (сервис `owns3` после `valkeyworker-2` ~строка 254; `owns3-data` в `volumes:` ~строка 286)
- Modify: `deploy/.env.example` (секция `OWNS3_*` в конец)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (publish-блок после SdGenerator ~строка 51; `owns3` в deploy-up ~строка 92; healthz-гейт после ~строки 97)

**Interfaces (Produces):** сервис `owns3` deploy-проекта: порт `${OWNS3_HOST_PORT:-9010}:9000`, том `owns3-data:/data`, env `OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD` из `deploy/.env` (`:?`-fail-fast); Задача 4 стучит на `http://localhost:${OWNS3_HOST_PORT:-9010}`.

- [ ] **Шаг 3.1. Сервис в `deploy/docker-compose.yml`** — дословно из spec §4.2 (после `valkeyworker-2`, перед `pgworker-backup`; в корневой `volumes:` добавить `  owns3-data:`):

```yaml
  # ownS3 (arch/owns3/01, 05 §6): целевое S3-хранилище бэкапов; после
  # перевода клиентов (t41) вытесняет as-minio. До того — живёт в контуре
  # стенда параллельно (бэкапы остаются на as-minio). standalone-сервис:
  # без etcd, без docker.sock; один процесс, один том.
  owns3:
    build:
      context: ..
      dockerfile: docker/OwnS3.Dockerfile
      args:
        OWNS3_PUBLISH_DIR: artifacts/owns3/publish
    image: owns3:dev
    restart: unless-stopped
    ports:
      # 9000/9001 хоста заняты as-minio полного стенда — ownS3 продолжает
      # портовой ряд (канон 05 §6); внутри контейнера канонический 9000.
      - "${OWNS3_HOST_PORT:-9010}:9000"
    environment:
      OWNS3_ROOT_USER: ${OWNS3_ROOT_USER:?задайте секрет}
      OWNS3_ROOT_PASSWORD: ${OWNS3_ROOT_PASSWORD:?задайте секрет}
    volumes:
      - owns3-data:/data
```

- [ ] **Шаг 3.2. Секция в `deploy/.env.example`** — дословно из spec §4.2 (в конец файла):

```
# ownS3 (arch/owns3/05): root-пара обязательна (fail-fast старта);
# статические ключи — опционально (пусто = только root).
OWNS3_ROOT_USER=owns3-local-root
OWNS3_ROOT_PASSWORD=owns3-local-root-pwd
#OWNS3_HOST_PORT=9010
#OWNS3_ACCESS_KEYS__0__ACCESSKEY=stands-writer
#OWNS3_ACCESS_KEYS__0__SECRETKEY=…
#OWNS3_ACCESS_KEYS__0__POLICY=read-write
```

- [ ] **Шаг 3.3. `00-up.sh`: publish-блок** — сразу после `echo ">>> [PHASE] publish SdGenerator готов …"` (строка ~51):

```bash
# ownS3 (t39, arch/owns3/05 §6): publish НА ХОСТЕ → compose build пакует только
# вывод (канон E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish OwnS3.App ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/OwnS3.App/OwnS3.App.csproj" \
  -c Release -o "$ROOT/artifacts/owns3/publish" --nologo \
  || { echo "❌ publish OwnS3 не удался"; exit 1; }
echo ">>> [PHASE] publish OwnS3 готов ($(date +%H:%M:%S))"
```

- [ ] **Шаг 3.4. `00-up.sh`: сервис в deploy-up + healthz-гейт.** В вызове (строка ~92) добавить `owns3`:

```bash
# БЫЛО:
  if ( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 2>&1 | tail -2 ); then
# СТАЛО:
  if ( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 owns3 2>&1 | tail -2 ); then
```

После блока `[ "$pg_up_ok" = 1 ] || …` (строка ~97), перед `echo ">>> поднимаю стенд…"`, вставить (бюджет 60 c, полл 2 c — spec §4.2):

```bash
# ownS3 (t39): готовность — healthz-проба хост-публикации (порт синхронен с
# deploy/.env, дефолт 9010; бэкапы остаются на as-minio).
OWNS3_PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ROOT/deploy/.env" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
OWNS3_PORT="${OWNS3_PORT:-9010}"
for i in $(seq 1 30); do curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null 2>&1 && break; sleep 2; done
curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null \
  || { echo "❌ owns3 не ожил за 60 c (:${OWNS3_PORT}/healthz; docker compose -p deploy logs owns3)"; exit 1; }
echo "  owns3 жив (http://localhost:${OWNS3_PORT}/healthz)"
```

- [ ] **Шаг 3.5. Частичная проверка (deploy-контур, без полного стенда):**

```bash
bash -n dev-stand/adminpanel/checks/00-up.sh
[ -f deploy/.env ] || cp deploy/.env.example deploy/.env
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/OwnS3.App/OwnS3.App.csproj \
  -c Release -o artifacts/owns3/publish --nologo
( cd deploy && docker compose --env-file .env up -d --build owns3 )
for i in $(seq 1 30); do curl -sf http://localhost:9010/healthz >/dev/null 2>&1 && break; sleep 2; done
curl -sf http://localhost:9010/healthz && echo " OWNS3-UP-OK"
```
Критерий: `bash -n` без ошибок; `OWNS3-UP-OK`. Сервис НЕ снимать — нужен Задаче 4 (после неё — зачистка `docker compose rm -sf owns3 && docker volume rm -f deploy_owns3-data`).

- [ ] **Шаг 3.6. Коммит:**

```bash
git add deploy/docker-compose.yml deploy/.env.example dev-stand/adminpanel/checks/00-up.sh
git commit -m "feat(t39): стенд-сервис owns3 — compose, секция OWNS3_* env-шаблона, publish и healthz-гейт в 00-up.sh"
```

**Выход:** ownS3 поднимается deploy-compose-билдом из publish-артефактов; `00-up.sh` поднимает и дожидается.
**Проверка:** шаг 3.5.
**Связь со spec:** §4.2, §5 фаза 3, §8 п.7/п.10.

---

### Задача 4: Фикс trailing-slash (дефект Фазы 6) + smoke-чек `46-owns3.sh` (spec §4.2; §7)

**Вход:** Задача 3 (сервис owns3 поднят локально, порт 9010, root-креды в `deploy/.env`); прецеденты `45-backups-storage.sh`, mc-паттерн `00-up.sh`. На Фазе 6 вскрыт второй дефект вне плана (доказан телеметрией упавших прогонов, с точностью до запроса): `S3PathParser` (`src/OwnS3.Protocol/Requests/S3PathParser.cs:14–22`) трактует путь `/{bucket}/` (trailing slash, пустой ключ) как объектный запрос с `Key=""` → bucket-level запросы со trailing slash маршрутизируются в объектные операции → 400 `InvalidArgument`. Проявления: AWSSDK .NET `PutBucketAsync` шлёт `PUT /{bucket}/` (лог: `operation=PutObject, key=, status=400`) — блокирует Задачи 6–9 и мерж-гейт 10.3; mc alias-probe `GET /probe-*/?location=` — тот же 400 — блокирует шаг 4.2. Канон 02/03 случай `/{bucket}/` не описывает; референс S3/MinIO трактует его как bucket-level. Фикс — шаг 4.1a, выполняется ПЕРВЫМ в задаче, до прогона 4.2 (одобрен пользователем: вариант 1 — та же схема, что шаг 2.2a; arch-first + принцип Q2 «код согласно AWSSDK.S3»).

**Files:**
- Create: `dev-stand/adminpanel/checks/46-owns3.sh` (`chmod +x`)
- Modify: `arch/owns3/03-protocol.md` §6 (шаг 4.1a — формулировка trailing slash, арх-правка первой)
- Modify: `src/OwnS3.Protocol/Requests/S3PathParser.cs` (шаг 4.1a)
- Modify: `src/tests/OwnS3.UnitTests/S3PathParserTests.cs`, `S3RouterTests.cs` (шаг 4.1a — юнит-кейсы)

- [ ] **Шаг 4.1. Скрипт чека** (полный текст; mc через хост-публикацию — ephemeral `docker run` с `--add-host host.docker.internal:host-gateway`, без `--network`, прецедент формы — ephemeral run из `00-up.sh`; бакет уникален ts-суффиксом; чистка — trap при любом исходе):

```bash
#!/usr/bin/env bash
# Smoke-чек стендового ownS3 (t39, arch/owns3/05 §5/§6): /healthz, /metrics
# (каталожные серии), mc-smoke через хост-публикацию. Стендовые бэкапы
# остаются на as-minio: чек НЕ трогает PGW_BACKUP_S3_* и бакет
# pgworker-backups. Временный бакет уникален (ts-суффикс); чистка — trap.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"
ENV_FILE="$ROOT/deploy/.env"
PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
PORT="${PORT:-9010}"
ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
[ -n "$ROOT_USER" ] && [ -n "$ROOT_PASSWORD" ] \
  || { echo "❌ deploy/.env: нет OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD"; exit 1; }
BASE="http://localhost:${PORT}"
MC="minio/mc:RELEASE.2025-08-13T08-35-41Z"
BUCKET="os3check-$(date +%s)"
# mc ходит через ХОСТ-ПУБЛИКАЦИЮ (канон 05 §6, spec §4.2 п.3): ephemeral
# контейнер mc резолвит host.docker.internal в шлюз docker-хоста и стучит
# на публикуемый порт owns3 — тот же путь, что внешний клиент стенда.
MC_TARGET="http://host.docker.internal:${PORT}"

# 1) healthz — 200 без подписи
curl -sf "$BASE/healthz" >/dev/null || { echo "❌ $BASE/healthz не 200"; exit 1; }
echo "  healthz: 200"

# 2) /metrics — каталожные серии канона 05 §5
METRICS="$(curl -sf "$BASE/metrics")"
echo "$METRICS" | grep -q 'ownS3_requests_total{' \
  || { echo "❌ /metrics: нет ownS3_requests_total{"; exit 1; }
echo "$METRICS" | grep -q 'ownS3_disk_total_bytes' \
  || { echo "❌ /metrics: нет ownS3_disk_total_bytes"; exit 1; }
echo "  metrics: ownS3_requests_total{…} / ownS3_disk_total_bytes — на месте"

# 3) mc-smoke ЧЕРЕЗ ХОСТ-ПУБЛИКАЦИЮ: alias → mb → cp(128 КиБ /dev/urandom)
#    → ls → cat(sha256 == источник) → stat → скачивание → rm → ls пуст → rb;
#    чистка — trap при любом исходе (mc rb --force).
mc_run() { docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh "$MC" -c "$1"; }
cleanup() {
  mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null 2>&1 \
          && mc rb --force c/$BUCKET >/dev/null 2>&1" || true
}
trap cleanup EXIT
mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null \
         && mc mb c/$BUCKET >/dev/null \
         && head -c 131072 /dev/urandom > /tmp/os3check.bin \
         && SRC=\$(sha256sum /tmp/os3check.bin | cut -d' ' -f1) \
         && mc cp /tmp/os3check.bin c/$BUCKET/obj.bin >/dev/null \
         && mc ls c/$BUCKET | grep -q obj.bin \
         && GOT=\$(mc cat c/$BUCKET/obj.bin | sha256sum | cut -d' ' -f1) \
         && mc stat c/$BUCKET/obj.bin | grep -E 'Size|ETag' >/dev/null \
         && mc cp c/$BUCKET/obj.bin /tmp/os3check-out.bin >/dev/null \
         && cmp -s /tmp/os3check.bin /tmp/os3check-out.bin \
         && mc rm c/$BUCKET/obj.bin >/dev/null \
         && [ -z \"\$(mc ls c/$BUCKET 2>/dev/null)\" ] \
         && mc rb c/$BUCKET >/dev/null \
         && [ \"\$SRC\" = \"\$GOT\" ]" \
  || { echo "❌ mc-smoke через хост-публикацию :${PORT} не прошёл"; exit 1; }
trap - EXIT
echo "  mc (host.docker.internal:${PORT}): mb→cp→ls→cat(sha256=источник)→stat→скачивание(cmp ok)→rm→rb"

echo "✓ owns3-чек зелёный (healthz + metrics + mc-smoke, :${PORT})"
```

- [ ] **Шаг 4.1a. Фикс trailing-slash `/{bucket}/` (arch-first, до прогона 4.2).**

**Действие (по порядку):**

(1) Arch-правка ПЕРВОЙ — `arch/owns3/03-protocol.md` §6, пункт «**Path-style only**» (после абзаца «бакет всегда в пути…»), добавить формулировку:

```
- Путь `/{bucket}/` (trailing slash, пустой ключ) — bucket-level запрос:
  пустой ключ трактуется как его отсутствие (прецедент S3/MinIO; реальные
  клиенты — AWS SDK .NET `CreateBucket`, mc probe — шлют именно так).
  Ключи с внутренними пустыми сегментами (`a//b`) — прежняя семантика
  (пустые сегменты сохраняются); подпись не затрагивается — canonical URI
  строится из raw-target.
```

(2) Код — `src/OwnS3.Protocol/Requests/S3PathParser.cs`: дополнить шапку-комментарий (строки 3–4) формой `/{bucket}/`, и после строки `var rawKey = path[(slash + 1)..];` (строка 19) вставить:

```csharp
        // /{bucket}/ (trailing slash, пустой ключ) — bucket-level запрос
        // (канон 03 §6): пустой ключ = его отсутствие; внутренние пустые
        // сегменты (a//b) не затрагиваются — rawKey непустой.
        if (rawKey.Length == 0)
            return new S3Path(bucket, null);
```

Итог метода `Parse` (для сверки):

```csharp
    public static S3Path Parse(string rawPath)
    {
        // Срезается только ведущий '/': хвостовой слэш — часть ключа S3
        // (кроме вырожденного /{bucket}/ — bucket-level, ниже).
        var path = rawPath.StartsWith('/') ? rawPath[1..] : rawPath;
        if (path.Length == 0)
            return new S3Path(null, null);

        var slash = path.IndexOf('/');
        if (slash < 0)
            return new S3Path(path, null);

        var bucket = path[..slash];
        var rawKey = path[(slash + 1)..];
        // /{bucket}/ (trailing slash, пустой ключ) — bucket-level запрос
        // (канон 03 §6): пустой ключ = его отсутствие; внутренние пустые
        // сегменты (a//b) не затрагиваются — rawKey непустой.
        if (rawKey.Length == 0)
            return new S3Path(bucket, null);
        // Декодирование по сегментам: пустые сегменты сохраняются (ключи со слэшами).
        var decodedKey = string.Join("/", rawKey.Split('/').Select(Uri.UnescapeDataString));
        return new S3Path(bucket, decodedKey);
    }
```

(3) Пересборка стенд-сервиса (ДО интеграционной проверки): контейнер стенда поднят шагом 3.5 compose-билдом из ПРЕ-фикс артефактов `artifacts/owns3/publish` — пересобирается фиксом (прецедент: 2.2a оговаривал пересборку своего контейнера):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/OwnS3.App/OwnS3.App.csproj \
  -c Release -o artifacts/owns3/publish --nologo
( cd deploy && docker compose --env-file .env up -d --build --force-recreate owns3 )
for i in $(seq 1 30); do curl -sf http://localhost:9010/healthz >/dev/null 2>&1 && break; sleep 2; done
curl -sf http://localhost:9010/healthz && echo " OWNS3-REBUILT-OK"
```
Критерий: `OWNS3-REBUILT-OK` (стенд-контейнер несёт фикс; инкрементальный publish — секунды).

(4) Юнит-кейсы — `src/tests/OwnS3.UnitTests/S3PathParserTests.cs`: шапку-комментарий файла дополнить формой `/{bucket}/` (bucket-level, канон 03 §6); добавить два Fact'а (стиль файла):

```csharp
    [Fact]
    public void Parse_BucketTrailingSlash_BucketLevel_NoKey()
    {
        // Arrange / Act: /{bucket}/ — bucket-level (канон 03 §6: пустой ключ
        // трактуется как его отсутствие; AWS SDK CreateBucket шлёт PUT /{bucket}/)
        var path = S3PathParser.Parse("/b/");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().BeNull();
    }

    [Fact]
    public void Parse_InnerEmptySegments_PreservedInKey()
    {
        // Arrange / Act: a//b — внутренние пустые сегменты сохраняются
        var path = S3PathParser.Parse("/b/a//b");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().Be("a//b");
    }
```

(существующий `Parse_BucketOnly` `("/b")` — прежнее поведение, остаётся зелёным без правок).

`src/tests/OwnS3.UnitTests/S3RouterTests.cs` — в Theory-таблицу `Routes` добавить строки (bucket-операции через trailing slash):

```csharp
        // /{bucket}/ — bucket-level (канон 03 §6): маршрутизация как без слэша
        { "PUT",     "/b/",               "",         false, S3Operation.CreateBucket },
        { "GET",     "/b/",               "",         false, S3Operation.ListObjects },
        { "HEAD",    "/b/",               "",         false, S3Operation.HeadBucket },
        { "GET",     "/b/",               "location", false, S3Operation.GetBucketLocation },
```

(строка `{ "GET", "/b/k", "", false, S3Operation.GetObject }` — объектный путь без trailing slash — прежнее поведение, уже в таблице).

(5) Коммит фикса (arch + код + тесты одним коммитом — прецедент шага 1.27):

```bash
git add arch/owns3/03-protocol.md src/OwnS3.Protocol/Requests/S3PathParser.cs \
        src/tests/OwnS3.UnitTests/S3PathParserTests.cs src/tests/OwnS3.UnitTests/S3RouterTests.cs
git commit -m "fix(t39): /{bucket}/ — bucket-level (канон 03 §6, прецедент S3/MinIO) — S3PathParser: пустой ключ = отсутствие; юнит-кейсы"
```

**Выход:** bucket-level запросы с trailing slash (`PUT /{bucket}/` SDK, `GET /probe-*/?location=` mc) маршрутизируются в bucket-операции, а не в объектные с пустым ключом; разблокированы шаг 4.2 (mc-smoke) и Задачи 6–9 (SDK PutBucketAsync).

**Проверка:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --no-build \
  --filter "FullyQualifiedName~S3PathParserTests|FullyQualifiedName~S3RouterTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build
```
Критерий: build 0 warnings; целевые кейсы и полные прогоны зелёные (регрессий парсера/роутера/подписи нет — подпись не затрагивается, canonical URI строится из raw-target). Интеграционно: повтор шага 4.2 на ПЕРЕСОБРАННОМ контейнере (п.3 — mc-smoke проходит, alias-probe больше не получает 400). SDK-сторона подтверждается при прогоне Задачи 6 — отдельного действия здесь не требует: E2E-образ строится из текущего исходника, фикс подтянется автоматически.

**Связь со spec:** §7 (отдельный пункт плана с обоснованием: канон 02/03 случай не описывал — arch-first правка 03 §6 первой, код зеркалит канон); принцип решений Q1/Q2 «код согласно AWSSDK.S3/реальным клиентам»; предусловие критериев §8 п.2 (SDK), п.4 (mc), п.5 (метрики после SDK-трафика).

- [ ] **Шаг 4.2. Исполняемость, синтаксис, прогон, зачистка стенда:**

```bash
chmod +x dev-stand/adminpanel/checks/46-owns3.sh
bash -n dev-stand/adminpanel/checks/46-owns3.sh
bash dev-stand/adminpanel/checks/46-owns3.sh
( cd deploy && docker compose --env-file .env rm -sf owns3 ) && docker volume rm -f deploy_owns3-data
```
Критерий: финальная строка `✓ owns3-чек зелёный …`; временного бакета не осталось (trap + rb); частичный контур разобран.

- [ ] **Шаг 4.3. Коммит:**

```bash
git add dev-stand/adminpanel/checks/46-owns3.sh
git commit -m "feat(t39): чек 46-owns3.sh — healthz/metrics/mc-smoke стендового ownS3 с самоочисткой"
```

**Выход:** trailing-slash `/{bucket}/` — bucket-level (арх+код+тесты, шаг 4.1a); стендовый smoke-чек в наборе чеков; зелёный на частично поднятом контуре.
**Проверка:** шаги 4.1a (юниты/build) и 4.2 (прогон чека).
**Связь со spec:** §4.2 (чек), §7 (фикс 4.1a), §8 п.4/п.7 (предусловия фикса — mc-критерий п.4).

---

### Задача 5: E2E-инфраструктура — окружение, гейт, коллекция, кейс-маркер Healthz (spec §4.3/§4.7, §5 фаза 4)

**Вход:** Задачи 1–2; прецеденты: `KafkaE2eEnvironment.cs` (структура/teardown/RunProcessAsync), `OwnMinio.cs` (wait-стратегия S3-сервиса), `E2eEnvironment.cs` PgWorker (статическая сборка с семафором, `NoMsBuildReuseEnv`), `DockerTrait.cs` PgWorker (Skip-гейт), `E2eRestoreScenarios.RunScenarioAsync` (MarkFailed-обёртка), `E2ePatroniFileSdScenarios` (VolumeBuilder/WithVolumeMount).

**Files:**
- Modify: `src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/DockerTrait.cs`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eCollection.cs`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eEnvironment.cs`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs`
- Create (временно, шаг 5.7 → удалить в 5.8): `src/tests/OwnS3.IntegrationTests/E2e/TeardownProbeScenarios.cs`

**Interfaces:**
- Consumes: образ `owns3:e2e` (Задача 2); `minio/mc:RELEASE.2025-08-13T08-35-41Z`.
- Produces (для Задач 6–9): `OwnS3E2eEnvironment.StartAsync(slug, ct)` / `RunScenarioAsync(slug, body, ct)`; свойства `Slug`, `ArtifactsDir`, `NetName`, `ContainerName`, `S3BaseUrl` (`http://localhost:{динамический порт}`), `S3InternalUrl` (`http://os3-es-{runId}:9000`), `RunTag` (8 hex), `RunId`; константы `RootUser/RootPassword/WriterAccessKey/WriterSecretKey/ReaderAccessKey/ReaderSecretKey/AdminAccessKey/AdminSecretKey`, `McImage`; методы `RunDockerAsync(args, ct)`, `MarkFailed()`, `CollectDiagnosticsAsync(mark)`; коллекция `OwnS3E2eCollection.Name` (spec §4.7).

- [ ] **Шаг 5.1. Пакеты в csproj** — в первый ItemGroup с PackageReference добавить (версии централизованы — НЕ дублировать):

```xml
        <PackageReference Include="AWSSDK.S3"/>
        <PackageReference Include="Testcontainers"/>
```

- [ ] **Шаг 5.2. `E2e/DockerTrait.cs`** (гейт docker-серии, прецедент PgWorker):

```csharp
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

// Гейт docker-серии ownS3 (прецедент PgWorker DockerTrait): без
// PGW_TEST_DOCKER=1 тесты пропускаются — CI без docker остаётся зелёным
// (единая env docker-серий монорепо).
public static class DockerTrait
{
    public const string EnvVar = "PGW_TEST_DOCKER";

    public static void SkipIfUnavailable()
    {
        if (Environment.GetEnvironmentVariable(EnvVar) != "1")
            Assert.Skip($"docker-тесты выключены (установите {EnvVar}=1 для включения)");
    }
}
```

- [ ] **Шаг 5.3. `E2e/OwnS3E2eCollection.cs`** (последовательность E2E-классов — по букве spec §4.7):

```csharp
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>Последовательная коллекция ВСЕХ E2E-классов ownS3 (spec §4.7):
/// DisableParallelization выводит её из параллельного пула xUnit. In-memory
/// Api-сценарии AWS SDK не используют — параллельность коллекций не
/// конфликтует.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OwnS3E2eCollection
{
    public const string Name = "owns3-e2e";
}
```

- [ ] **Шаг 5.4. `E2e/OwnS3E2eEnvironment.cs`** — полный файл:

```csharp
using System.Diagnostics;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using DotNet.Testcontainers.Volumes;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E окружение ownS3 (t39, docs/e2e-isolation.md +
/// docs/e2e-launch.md; модель KafkaE2eEnvironment — компактный standalone):
/// своя сеть os3-en-{runId}, свой контейнер os3-es-{runId} (образ owns3:e2e),
/// свой том os3-vd-{runId}. Готовность — wait-стратегия GET /healthz
/// (внутренний 9000, бюджет 30 с). Телеметрия: ВСЕ фазы — [PHASE]-метки с
/// таймингом в Console + UTC-записи в phases.log (ArtifactsDir); docker
/// logs/inspect ДО удалений (причины: "failed" — падение сценария,
/// "teardown", "slow-phase-*" — любая фаза дольше 60 с; к сборочным фазам
/// publish/build docker-диагностика неприменима — контейнера ещё нет, их
/// полные логи — process-build-*.log в ArtifactsDir). MarkFailed() → teardown
/// ОСТАНАВЛИВАЕТ контейнер, но не удаляет (README-cleanup.txt; own-only по
/// runId). Retry старта ×3 против гонок сети. Гейты: PGW_TEST_DOCKER=1
/// (Skip — DockerTrait), PGW_TEST_E2E_NOBUILD=1 — бисект.
/// </summary>
public sealed class OwnS3E2eEnvironment : IAsyncDisposable
{
    private const string ServiceImage = "owns3:e2e";

    /// <summary>Образ mc (клиент оператора; запинен, в images.txt).</summary>
    public const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";

    // Литералы e2e-секретов (одноразовый контур; роли — клиентские кейсы).
    public const string RootUser = "os3e2e-root";
    public const string RootPassword = "os3e2e-root-secret";
    public const string WriterAccessKey = "os3e2e-writer";
    public const string WriterSecretKey = "os3e2e-writer-secret";
    public const string ReaderAccessKey = "os3e2e-reader";
    public const string ReaderSecretKey = "os3e2e-reader-secret";
    public const string AdminAccessKey = "os3e2e-admin";
    public const string AdminSecretKey = "os3e2e-admin-secret";

    // Статическая сборка образа — раз на процесс (прецедент E2eEnvironment
    // PgWorker): publish+build платит только первый Fact серии.
    private static readonly SemaphoreSlim StaticGate = new(1, 1);
    private static bool _staticReady;

    // Env вложенных dotnet-сборок: build-server/nodeReuse держат stdout-пайп
    // после завершения publish (e2e-факт) — реюз отключаем (канон E2eFixture).
    private static readonly IReadOnlyDictionary<string, string> NoMsBuildReuseEnv =
        new Dictionary<string, string>
        {
            ["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1",
            ["MSBUILDDISABLENODEREUSE"] = "1",
        };

    private readonly INetwork _net;
    private readonly IContainer _service;
    private readonly string _runId;
    private readonly string _containerName;
    private readonly string _volumeName;
    private readonly int _hostPort;
    private bool _failed;

    private OwnS3E2eEnvironment(string slug, string runId, INetwork net,
        string containerName, string volumeName, IContainer service,
        string artifactsDir, int hostPort)
    {
        Slug = slug;
        _runId = runId;
        _net = net;
        _containerName = containerName;
        _volumeName = volumeName;
        _service = service;
        ArtifactsDir = artifactsDir;
        _hostPort = hostPort;
    }

    public string Slug { get; }

    /// <summary>Артефакты телеметрии прогона (логи/inspect/phases.log).</summary>
    public string ArtifactsDir { get; }

    /// <summary>Docker-имя сети окружения (os3-en-{runId}).</summary>
    public string NetName => $"os3-en-{_runId}";

    /// <summary>Docker-имя контейнера сервиса (os3-es-{runId}).</summary>
    public string ContainerName => _containerName;

    /// <summary>S3-грань с хоста (SDK/HttpClient): динамический порт.</summary>
    public string S3BaseUrl => $"http://localhost:{_hostPort}";

    /// <summary>S3-грань изнутри docker-сети (mc: docker run --network).</summary>
    public string S3InternalUrl => $"http://{_containerName}:9000";

    /// <summary>Тег прогона (8 hex) — компонент имён бакетов сценариев.</summary>
    public string RunTag => _runId[..8];

    /// <summary>Полный guid прогона (ассерт чистоты, README-cleanup).</summary>
    public string RunId => _runId;

    public void MarkFailed() => _failed = true;

    /// <summary>Подъём окружения: статическая сборка образа → сеть → том →
    /// контейнер (wait /healthz ≤ 30 с); 3 попытки против распознанных гонок —
    /// подъём заново ЦЕЛИКОМ (полное удаление попытки).</summary>
    public static async Task<OwnS3E2eEnvironment> StartAsync(
        string slug, CancellationToken ct = default)
    {
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            throw new InvalidOperationException(
                $"E2E требует {DockerTrait.EnvVar}=1 (иначе Skip в тесте DockerTrait'ом)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(artifactsDir);

        // Статический ассет процесса: owns3:e2e (publish на хосте + CLI-build).
        await BuildImageOnceAsync(artifactsDir, ct);

        for (var attempt = 1; ; attempt++)
        {
            var net = new NetworkBuilder().WithName($"os3-en-{runId}").Build();
            var volumeName = $"os3-vd-{runId}";
            var volume = new VolumeBuilder().WithName(volumeName).Build();
            var containerName = $"os3-es-{runId}";
            var hostPort = FreePort();
            var service = new ContainerBuilder(ServiceImage)
                .WithName(containerName)
                .WithNetwork(net)
                .WithPortBinding(hostPort, 9000)
                .WithVolumeMount(volumeName, "/data")
                .WithEnvironment("OWNS3_ROOT_USER", RootUser)
                .WithEnvironment("OWNS3_ROOT_PASSWORD", RootPassword)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", WriterAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", WriterSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write")
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__ACCESSKEY", ReaderAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__SECRETKEY", ReaderSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__POLICY", "read-only")
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__ACCESSKEY", AdminAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__SECRETKEY", AdminSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__POLICY", "admin")
                .WithEnvironment("OWNS3_HOST_ID", $"os3-e2e-{runId[..8]}")
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(
                        request => request.ForPort(9000).ForPath("/healthz"),
                        wait => wait.WithTimeout(TimeSpan.FromSeconds(30))))
                .Build();

            async Task RemoveAttemptAsync()
            {
                await SafeStopAsync(service, remove: true);
                await TryDockerAsync(["volume", "rm", "-f", volumeName]);
                try { await net.DeleteAsync(ct); } catch { /* уже нет */ }
            }

            try
            {
                var sw = Stopwatch.StartNew();
                await net.CreateAsync(ct);
                await volume.CreateAsync(ct);
                await service.StartAsync(ct);
                var env = new OwnS3E2eEnvironment(slug, runId, net, containerName,
                    volumeName, service, artifactsDir, hostPort);
                await env.LogPhaseAsync("start", sw);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по runId):\n"
                    + $"docker rm -f {containerName}\n"
                    + $"docker volume rm -f {volumeName}\n"
                    + $"docker network rm {env.NetName}\n", ct);
                return env;
            }
            catch (Exception ex) when (attempt < 3
                && (ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("bind", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("port", StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"[PHASE] start: попытка {attempt} не удалась ({ex.Message.Split('\n')[0]}) — ретрай с чистого листа");
                await RemoveAttemptAsync();
                continue;
            }
            catch
            {
                await RemoveAttemptAsync();
                throw;
            }
        }
    }

    /// <summary>Сценарий под политикой телеметрии (docs/e2e-launch.md §3):
    /// исключение тела → телеметрия "failed" ДО остановки → MarkFailed →
    /// rethrow; teardown — DisposeAsync при любом исходе. Упавший прогон
    /// оставляет контейнер остановленным.</summary>
    public static async Task RunScenarioAsync(
        string slug, Func<OwnS3E2eEnvironment, Task> body, CancellationToken ct = default)
    {
        var fx = await StartAsync(slug, ct);
        try
        {
            await body(fx);
        }
        catch
        {
            try { await fx.CollectDiagnosticsAsync("failed"); }
            catch { /* телеметрия — лучшее усилие */ }
            fx.MarkFailed();
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    /// <summary>docker-CLI: stdout; ненулевой exit/таймаут — исключение с
    /// выводом процесса (бюджет 2 мин — зависший процесс умирает).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>Телеметрия ДО любых удалений (docs/e2e-launch.md §1).</summary>
    public async Task CollectDiagnosticsAsync(string mark)
    {
        await ShellToFileAsync($"logs --tail 2000 {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.log"));
        await ShellToFileAsync($"inspect {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.json"));
    }

    /// <summary>Финал фазы (docs/e2e-launch.md §2): [PHASE]-метка с таймингом
    /// в Console + UTC-запись в phases.log; фаза > 60 с — немедленный сбор
    /// docker-диагностики (slow-phase-*) независимо от исхода фазы.</summary>
    private async Task LogPhaseAsync(string name, Stopwatch sw)
    {
        sw.Stop();
        var line = $"phase {name}: ok, {sw.Elapsed.TotalSeconds:F1}s";
        Console.WriteLine($"[PHASE] {line}");
        await File.AppendAllTextAsync(Path.Combine(ArtifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} {line}\n");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
        {
            try { await CollectDiagnosticsAsync($"slow-phase-{name}-{(int)sw.Elapsed.TotalSeconds}s"); }
            catch { /* телеметрия — лучшее усилие */ }
        }
    }

    /// <summary>Полный teardown при любом исходе: телеметрия → dispose
    /// контейнера → volume rm с ретраями (демон освобождает асинхронно,
    /// бюджет ~30 с; превышение — громкий фейл ассерта) → сеть → ассерт
    /// чистоты по runId (own-only).</summary>
    public async ValueTask DisposeAsync()
    {
        var sw = Stopwatch.StartNew();
        try { await CollectDiagnosticsAsync("teardown"); }
        catch { /* телеметрия не маскирует исход сценария */ }

        if (_failed)
        {
            // docs/e2e-launch.md §3: контейнер ОСТАНОВИТЬ, не удалить (том/
            // сеть остаются для разбора; чистка — README-cleanup.txt вручную).
            await SafeStopAsync(_service, remove: false);
            await LogPhaseAsync("teardown", sw);
            return;
        }

        await SafeStopAsync(_service, remove: true);

        // volume: ретраи до ~30 с (полл 2 с).
        for (var i = 0; ; i++)
        {
            var left = await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None);
            if (string.IsNullOrWhiteSpace(left) || i >= 15)
                break;
            await TryDockerAsync(["volume", "rm", "-f", _volumeName]);
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        try { await _net.DeleteAsync(); } catch { /* уже нет */ }

        // Ассерт чистоты: ни контейнера, ни тома, ни сети с runId прогона.
        var containers = await RunDockerOrNullAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={_runId}"],
            CancellationToken.None);
        containers.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal))
            .ToList()
            .Should().BeEmpty($"teardown убрал все контейнеры прогона {_runId}");
        (await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось тома {_volumeName}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={NetName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось сети {NetName}");

        // Фаза teardown — с таймингом и slow-сбором > 60 c (спец §6:
        // [PHASE] на publish/build/старт/teardown). После удалений docker-
        // диагностика недоступна — LogPhaseAsync глотает её отказ (контейнер
        // уже удалён), метка и UTC-запись пишутся всегда.
        await LogPhaseAsync("teardown", sw);
    }

    // ── Статическая сборка образа (раз на процесс): publish на хосте +
    // docker build CLI-режимом (узкий контекст артефактов; канон
    // E2E-образов). Фазы — с UTC-метками в phases.log окружения и
    // [PHASE]-метками в Console; docker-диагностика к сборочным фазам
    // НЕ применима (контейнера ещё нет) — «тихая фаза» исключена метками
    // и полными логами процессов в ArtifactsDir. ──

    private static async Task BuildImageOnceAsync(string artifactsDir, CancellationToken ct)
    {
        if (_staticReady || Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_staticReady)
                return;
            var root = FindRepoRoot();
            var outDir = Path.Combine(root, "artifacts", "e2e", "owns3");
            var sw = Stopwatch.StartNew();
            PhaseStatic(artifactsDir, "build owns3:e2e: publish (host) — старт");
            await RunProcessAsync("dotnet",
                ["publish", Path.Combine(root, "src", "OwnS3.App", "OwnS3.App.csproj"),
                 "-c", "Release", "-o", outDir, "--nologo"],
                ct, TimeSpan.FromMinutes(10), env: NoMsBuildReuseEnv,
                logFile: Path.Combine(artifactsDir, "process-build-publish.log"));
            PhaseStatic(artifactsDir,
                $"build owns3:e2e: publish готов за {sw.Elapsed.TotalSeconds:F0} c — docker build…");
            await RunProcessAsync("docker",
                ["build", "-f", Path.Combine(root, "docker", "OwnS3.Dockerfile"),
                 "-t", ServiceImage, outDir],
                ct, TimeSpan.FromSeconds(120),
                logFile: Path.Combine(artifactsDir, "process-build-docker.log"));
            PhaseStatic(artifactsDir,
                $"build owns3:e2e: готов за {sw.Elapsed.TotalSeconds:F0} c (publish+build)");
            _staticReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // [PHASE]-метка сборочной фазы: Console + phases.log (UTC) окружения.
    private static void PhaseStatic(string artifactsDir, string message)
    {
        Console.WriteLine($"[PHASE] {message}");
        File.AppendAllText(Path.Combine(artifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} {message}\n");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "OwnS3.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("корень репо (docker/OwnS3.Dockerfile) не найден");
    }

    // ── Процесс-хелперы (канон RunProcessAsync: таймаут убивает дерево,
    //    вывод — в исключение, полный лог — в logFile) ──

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout,
        IReadOnlyDictionary<string, string>? env = null, string? logFile = null)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        if (env is not null)
            foreach (var (key, value) in env)
                psi.Environment[key] = value;
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, output + error, ct);
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); }
            catch { /* процесс мог уже выйти */ }
            string drained = "";
            try
            {
                drained = string.Join("", await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5)));
            }
            catch (TimeoutException)
            {
                // пайп завис — вывод недоступен
            }
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, drained, CancellationToken.None);
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит; хвост:\n{drained}");
        }
    }

    private async Task TryDockerAsync(IReadOnlyList<string> args)
    {
        try { await RunDockerAsync(args, CancellationToken.None); }
        catch { /* чистка — лучшее усилие */ }
    }

    private async Task<string> RunDockerOrNullAsync(IReadOnlyList<string> args, CancellationToken ct)
    {
        try { return await RunDockerAsync(args, ct); }
        catch { return ""; }
    }

    private async Task ShellToFileAsync(string args, string path)
    {
        try
        {
            var output = await RunDockerAsync(args.Split(' '), CancellationToken.None);
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    private static async Task SafeStopAsync(IContainer container, bool remove)
    {
        try { await container.StopAsync(); }
        catch { /* уже остановлен/не стартовал */ }
        if (remove)
        {
            try { await container.DisposeAsync(); }
            catch { /* уже удалён */ }
        }
    }

    // Зонд свободного порта (динамические порты везде; прецедент FreePort).
    private static int FreePort()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
```

- [ ] **Шаг 5.5. `E2e/HealthMetricsScenarios.cs`** — первый живой тест серии (кейс-маркер spec §5 фаза 4; Metrics-факт добавит Задача 9):

```csharp
namespace OwnS3.IntegrationTests.E2e;

// Health/метрики ownS3 в докер-контуре (t39, arch/owns3/05 §5): healthz —
// доступность без аутентификации (том жив); metrics-факт — Задача 9 плана.
[Collection(OwnS3E2eCollection.Name)]
public sealed class HealthMetricsScenarios
{
    [Fact]
    public async Task Healthz_200_NoSignature()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: служебный путь вне S3-конвейера (подпись не нужна), том жив
        await OwnS3E2eEnvironment.RunScenarioAsync("hm1", async fx =>
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

            // Act
            var response = await http.GetAsync($"{fx.S3BaseUrl}/healthz", ct);

            // Assert
            response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
                "healthz не требует подписи и отвечает 200 на живом томе (канон 05 §5)");
        }, ct);
    }
}
```

- [ ] **Шаг 5.6. Зелёный цикл кейс-маркера:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.Healthz_200_NoSignature"
docker ps -a --filter name=os3- --format '{{.Names}}'
docker volume ls --filter name=os3- --format '{{.Name}}'
docker network ls --filter name=os3- --format '{{.Name}}'
```
Критерий: PASS (первый прогон платит publish+build — `[PHASE]`-метки в выводе); все три списка пусты.

- [ ] **Шаг 5.7. Проверка stop-режима MarkFailed (принудительное падение).** Временный файл `E2e/TeardownProbeScenarios.cs`:

```csharp
namespace OwnS3.IntegrationTests.E2e;

// ВРЕМЕННАЯ проба teardown (шаг 5.7 плана t39): принудительное падение
// доказывает stop-без-удаления (MarkFailed, канон e2e-launch §3) и
// README-cleanup.txt. Файл удаляется шагом 5.8.
[Collection(OwnS3E2eCollection.Name)]
public sealed class TeardownProbeScenarios
{
    [Fact]
    public async Task TeardownProbe_ForcedFailure()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3E2eEnvironment.RunScenarioAsync("probe", fx =>
        {
            true.Should().BeFalse("принудительное падение пробы teardown");
            return Task.CompletedTask;
        }, ct);
    }
}
```

Прогон (ожидаемо красный):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.TeardownProbe" || true
docker ps -a --filter name=os3-es- --format '{{.Names}} {{.Status}}'
ls /tmp/pgw-e2e-artifacts-*/README-cleanup.txt | tail -1
```
Критерий: FAILED; контейнер `os3-es-{guid}` в статусе `Exited`; том/сеть живы; README-cleanup.txt существует.

- [ ] **Шаг 5.8. Ручная чистка по README + удаление пробы + повторный зелёный:**

```bash
docker rm -f $(docker ps -aq --filter name=os3-es-) 2>/dev/null || true
docker volume rm -f $(docker volume ls -q --filter name=os3-vd-) 2>/dev/null || true
docker network rm $(docker network ls --format '{{.Name}}' --filter name=os3-en-) 2>/dev/null || true
rm src/tests/OwnS3.IntegrationTests/E2e/TeardownProbeScenarios.cs
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.Healthz_200_NoSignature"
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: чистка прошла; проба удалена; healthz-маркер снова PASS; чистота.

- [ ] **Шаг 5.9. Коммит:**

```bash
git add src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj src/tests/OwnS3.IntegrationTests/E2e/
git commit -m "feat(t39): OwnS3E2eEnvironment — изолированное E2E-окружение ownS3 (сеть+контейнер+том, телеметрия, ассерт чистоты) + кейс-маркер Healthz"
```

**Выход:** воспроизводимое окружение серии; зелёный Healthz-маркер; stop-режим MarkFailed доказан.
**Проверка:** шаги 5.6–5.8.
**Связь со spec:** §4.3 (весь), §4.7, §5 фаза 4, §8 п.1/п.6.

---

### Задача 6: Фабрика клиентов SDK + `AwsSdkSimpleScenarios` (8 Facts) (spec §4.4, §4.5 класс 1)

**Вход:** Задача 5 (окружение, коллекция, гейт).

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eClients.cs`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/AwsSdkSimpleScenarios.cs`

**Interfaces:**
- Consumes: `OwnS3E2eEnvironment` (креды/URL), `RunScenarioAsync`, `OwnS3E2eCollection`.
- Produces (для Задач 7–9): `OwnS3Role { Root, Writer, Reader, Admin }`; `OwnS3ChecksumMode { WhenRequired, WhenSupported }`; `OwnS3E2eClients.CreateS3(fx, role, checksum=WhenRequired)` → `IAmazonS3`; `OwnS3E2eETag.Quote/Md5Hex/Multipart`; `OwnS3E2eBytes.Deterministic(long, int)`.

- [ ] **Шаг 6.1. `E2e/OwnS3E2eClients.cs`** — полный файл:

```csharp
using System.Security.Cryptography;
using Amazon.Runtime;
using Amazon.S3;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>Роль клиента окружения (литералы e2e-секретов; матрица 05 §3).</summary>
public enum OwnS3Role { Root, Writer, Reader, Admin }

/// <summary>Режим flexible checksums клиента (канон arch/owns3/03 §3).
/// Per-client: AmazonS3Config.RequestChecksumCalculation — глобальной статики
/// AWSConfigs.RequestChecksumCalculation в AWSSDK.Core 3.7.501.1 нет
/// (проверено компиляцией; единственный компилируемый способ настройки).</summary>
public enum OwnS3ChecksumMode { WhenRequired, WhenSupported }

/// <summary>Реальные клиенты E2E-серии: фабрика AWS SDK .NET (никаких
/// собственных подписантов) + хелперы ETag/данных.</summary>
public static class OwnS3E2eClients
{
    /// <summary>Клиент AWS SDK .NET против ownS3-окружения: path-style,
    /// us-east-1, таймаут 10 c; режим checksum — параметром (основной —
    /// WhenRequired: режим, в который обязан быть настроен клиент бэкапов,
    /// канон 03 §3 — иначе свежий SDK шлёт неподдерживаемый
    /// STREAMING-UNSIGNED-PAYLOAD-TRAILER → 400).</summary>
    public static IAmazonS3 CreateS3(OwnS3E2eEnvironment fx, OwnS3Role role,
        OwnS3ChecksumMode checksum = OwnS3ChecksumMode.WhenRequired)
    {
        var (access, secret) = role switch
        {
            OwnS3Role.Root => (OwnS3E2eEnvironment.RootUser, OwnS3E2eEnvironment.RootPassword),
            OwnS3Role.Writer => (OwnS3E2eEnvironment.WriterAccessKey, OwnS3E2eEnvironment.WriterSecretKey),
            OwnS3Role.Reader => (OwnS3E2eEnvironment.ReaderAccessKey, OwnS3E2eEnvironment.ReaderSecretKey),
            OwnS3Role.Admin => (OwnS3E2eEnvironment.AdminAccessKey, OwnS3E2eEnvironment.AdminSecretKey),
            _ => throw new ArgumentOutOfRangeException(nameof(role)),
        };
        var config = new AmazonS3Config
        {
            ServiceURL = fx.S3BaseUrl,
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
            Timeout = TimeSpan.FromSeconds(10),
            RequestChecksumCalculation = checksum == OwnS3ChecksumMode.WhenRequired
                ? RequestChecksumCalculation.WHEN_REQUIRED
                : RequestChecksumCalculation.WHEN_SUPPORTED,
        };
        return new AmazonS3Client(new BasicAWSCredentials(access, secret), config);
    }
}

/// <summary>ETag-семантика (канон arch/owns3/02 §1): MD5 простого PUT;
/// составной multipart-ETag «N-md5(concat БАЙТОВ hex-ETag'ов частей)».
/// Сверки тест считает сам.</summary>
public static class OwnS3E2eETag
{
    /// <summary>Значение для conditional-заголовков SDK (ETag — в кавычках).</summary>
    public static string Quote(string etag) => $"\"{etag.Trim('"')}\"";

    public static string Md5Hex(byte[] body)
        => Convert.ToHexString(MD5.HashData(body)).ToLowerInvariant();

    public static string Multipart(int parts, IReadOnlyList<string> partEtags)
    {
        var concat = new List<byte>();
        foreach (var etag in partEtags)
            concat.AddRange(Convert.FromHexString(etag.Trim('"')));
        return $"{parts}-{Md5Hex([.. concat])}";
    }
}

/// <summary>Детерминированные тела тестов (seed; размеры КиБ–МиБ).</summary>
public static class OwnS3E2eBytes
{
    public static byte[] Deterministic(long length, int seed)
    {
        var data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }
}
```

- [ ] **Шаг 6.2. `E2e/AwsSdkSimpleScenarios.cs`** — полный файл (8 Facts; If-Range-кейсов нет — поддержка снята, Задача 1):

```csharp
using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 1 (spec §4.5): AWS SDK .NET — простые операции реальным клиентом
// (PUT/GET/HEAD, conditional, Range, Copy, Delete/batch, листинги, оба
// режима checksum, матрица ролей). Данные детерминированы (seed); AAA.
[Collection(OwnS3E2eCollection.Name)]
public sealed class AwsSdkSimpleScenarios
{
    [Fact]
    public async Task Buckets_Lifecycle_ByAdmin()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: полный жизненный цикл бакета admin-клиентом SDK: создание,
        // заголовок, список, локация, повторное создание → 409, удаление
        // пустого → 204, удаление несуществующего → 404
        await OwnS3E2eEnvironment.RunScenarioAsync("s1", async fx =>
        {
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            var bucket = $"os3e2e{fx.RunTag}lc";

            // Act / Assert
            await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            (await s3.HeadBucketAsync(new HeadBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.OK);
            (await s3.ListBucketsAsync(ct)).Buckets.Should().Contain(b => b.BucketName == bucket);
            (await s3.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = bucket }, ct))
                .Location.Value.Should().BeEmpty("us-east-1 кодируется пустой Location (канон 02)");

            // Act / Assert: повторное создание уже своего бакета → 409
            (await s3.Invoking(x => x.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "BucketAlreadyOwnedByYou" && e.StatusCode == HttpStatusCode.Conflict);

            // Act / Assert: удаление пустого → 204; несуществующего → 404
            (await s3.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchBucket" && e.StatusCode == HttpStatusCode.NotFound);
        }, ct);
    }

    [Fact]
    public async Task PutGet_Roundtrip_Metadata_And_Etag()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: PUT (256 КиБ, ключ с UTF-8/слэшами, ContentType, 2 метаданные)
        // → GET возвращает байты/ETag-MD5/ContentType/метаданные; HEAD —
        // Size/ETag/LastModified; ListObjectsV2 видит ключ
        await OwnS3E2eEnvironment.RunScenarioAsync("s2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}rt";
            var key = "backups/os3-e2e/файл+1.dat";
            var body = OwnS3E2eBytes.Deterministic(256 * 1024, 42);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: PUT (MetadataCollection SDK сама ставит префикс x-amz-meta-)
            var put = new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
                ContentType = "application/x-os3-e2e",
            };
            put.Metadata.Add("src", "e2e");
            put.Metadata.Add("run", fx.RunTag);
            var putResp = await s3.PutObjectAsync(put, ct);

            // Assert: ETag = MD5 тела (канон 02 §1)
            putResp.HttpStatusCode.Should().Be(HttpStatusCode.OK);
            putResp.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));

            // Act / Assert: GET — байты/ETag/ContentType/метаданные
            using var get = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body, "GET возвращает байты PUT без искажений");
            get.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            get.ContentType.Should().Be("application/x-os3-e2e");
            get.Metadata["src"].Should().Be("e2e");
            get.Metadata["run"].Should().Be(fx.RunTag);

            // Act / Assert: HEAD — размер/ETag/LastModified; листинг видит ключ
            var head = await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct);
            head.ContentLength.Should().Be(body.Length);
            head.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            head.LastModified.Should().BeAfter(DateTimeOffset.UtcNow.AddDays(-1));
            (await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket }, ct))
                .S3Objects.Should().Contain(o => o.Key == key);
        }, ct);
    }

    [Fact]
    public async Task Conditional_Requests()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: conditional GET — If-Match верный 200 / чужой 412;
        // If-None-Match верный 304; If-Modified-Since будущее 304;
        // If-Unmodified-Since прошлое 412 (канон 02 §1)
        await OwnS3E2eEnvironment.RunScenarioAsync("s3", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}cd";
            var key = "cond.bin";
            var body = OwnS3E2eBytes.Deterministic(64 * 1024, 7);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var etag = (await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct)).ETag!.Trim('"');

            // Act / Assert: If-Match верный → 200
            (await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToMatch = OwnS3E2eETag.Quote(etag),
            }, ct)).HttpStatusCode.Should().Be(HttpStatusCode.OK);

            // Act / Assert: If-Match чужой → 412 PreconditionFailed
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToMatch = "\"deadbeef\"",
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed
                            && e.ErrorCode == "PreconditionFailed");

            // Act / Assert: If-None-Match верный → 304 (SDK бросает исключение)
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToNotMatch = OwnS3E2eETag.Quote(etag),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotModified);

            // Act / Assert: If-Modified-Since (будущее) → 304
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, ModifiedSinceDateUtc = DateTime.UtcNow.AddDays(1),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotModified);

            // Act / Assert: If-Unmodified-Since (прошлое) → 412
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key,
                UnmodifiedSinceDateUtc = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
        }, ct);
    }

    [Fact]
    public async Task Range_Requests()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Range — закрытый bytes=10-20 (206 + срез), открытый bytes=N-,
        // суффикс -5, end за размером → 206 с обрезкой. If-Range-кейсов нет:
        // поддержка снята с ownS3 (spec §3.3/§4.6), SDK заголовок не шлёт
        await OwnS3E2eEnvironment.RunScenarioAsync("s4", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}rg";
            var key = "range.bin";
            var body = OwnS3E2eBytes.Deterministic(100_000, 11);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            async Task<byte[]> RangeAsync(string range)
            {
                using var resp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucket, Key = key, ByteRange = new ByteRange(range),
                }, ct);
                resp.HttpStatusCode.Should().Be(HttpStatusCode.PartialContent,
                    $"диапазон {range} обязан отвечать 206");
                using var ms = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(ms, ct);
                return ms.ToArray();
            }

            // Act / Assert
            (await RangeAsync("bytes=10-20")).Should().Equal(body[10..21]);
            (await RangeAsync("bytes=10-")).Should().Equal(body[10..]);
            (await RangeAsync("bytes=-5")).Should().Equal(body[^5..]);
            (await RangeAsync("bytes=99990-999999")).Should().Equal(body[99990..],
                "end за размером обрезается до конца объекта");
        }, ct);
    }

    [Fact]
    public async Task CopyObject_Copy()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: CopyObject COPY наследует тело (ETag = MD5 того же тела) и
        // метаданные; REPLACE меняет ContentType/metadata; DeleteObject
        // идемпотентен (204); GET удалённого → NoSuchKey 404
        await OwnS3E2eEnvironment.RunScenarioAsync("s5", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}cp";
            var src = "copy-src.bin";
            var body = OwnS3E2eBytes.Deterministic(128 * 1024, 21);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var put = new PutObjectRequest
            {
                BucketName = bucket, Key = src, InputStream = new MemoryStream(body),
                ContentType = "text/plain",
            };
            put.Metadata.Add("gen", "1");
            await s3.PutObjectAsync(put, ct);

            // Act / Assert: COPY — байты и ETag наследуются
            await s3.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = bucket, SourceKey = src,
                DestinationBucket = bucket, DestinationKey = "copy-dst.bin",
            }, ct);
            using var copy = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct);
            using var ms = new MemoryStream();
            await copy.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            copy.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            copy.Metadata["gen"].Should().Be("1", "COPY наследует метаданные");

            // Act / Assert: REPLACE — новые ContentType/metadata
            var replace = new CopyObjectRequest
            {
                SourceBucket = bucket, SourceKey = src,
                DestinationBucket = bucket, DestinationKey = "copy-rep.bin",
                MetadataDirective = S3MetadataDirective.REPLACE,
                ContentType = "application/json",
            };
            replace.Metadata.Add("gen", "2");
            await s3.CopyObjectAsync(replace, ct);
            using var rep = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = "copy-rep.bin" }, ct);
            rep.ContentType.Should().Be("application/json");
            rep.Metadata["gen"].Should().Be("2");

            // Act / Assert: DeleteObject → 204; GET → 404 NoSuchKey; повтор → 204
            (await s3.DeleteObjectAsync(
                new DeleteObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.GetObjectAsync(
                    new GetObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchKey" && e.StatusCode == HttpStatusCode.NotFound);
            (await s3.DeleteObjectAsync(
                new DeleteObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent, "удаление идемпотентно");
        }, ct);
    }

    [Fact]
    public async Task DeleteObjects_Batch_And_Listings()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: 12 ключей → DeleteObjects 3 → перечислены; delimiter →
        // CommonPrefixes; пагинация max-keys=5 до исчерпания;
        // GetObjectAttributes → ETag/ObjectSize/StorageClass
        await OwnS3E2eEnvironment.RunScenarioAsync("s6", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}ls";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            for (var i = 0; i < 12; i++)
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket, Key = $"list/prefix{i % 3}/obj{i}",
                    InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1024, i)),
                }, ct);

            // Act / Assert: батч-удаление 3 ключей — перечислены в ответе
            var batch = await s3.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = new List<KeyVersion>
                {
                    new() { Key = "list/prefix0/obj0" },
                    new() { Key = "list/prefix1/obj1" },
                    new() { Key = "list/prefix2/obj2" },
                },
            }, ct);
            batch.DeletedObjects.Select(d => d.Key).Should()
                .Contain(new[] { "list/prefix0/obj0", "list/prefix1/obj1", "list/prefix2/obj2" });

            // Act / Assert: delimiter без Prefix — свёртка по ПЕРВОМУ "/"
            // после префикса (канон 02 §3, ListWalker): все 9 ключей живут
            // под "list/" → единственный CommonPrefix, объектов нет
            var root = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket, Delimiter = "/",
            }, ct);
            root.CommonPrefixes.Select(p => p.Prefix).Should().BeEquivalentTo("list/");
            root.S3Objects.Should().BeEmpty("все ключи свернуты в префикс list/");

            // Act / Assert: Prefix="list/" — следующий уровень префиксов
            var nested = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket, Delimiter = "/", Prefix = "list/",
            }, ct);
            nested.CommonPrefixes.Select(p => p.Prefix).Should()
                .BeEquivalentTo("list/prefix0/", "list/prefix1/", "list/prefix2/");

            // Act / Assert: пагинация max-keys=5 до исчерпания (9 ключей)
            var seen = new List<string>();
            string? token = null;
            while (true)
            {
                var page = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = bucket, MaxKeys = 5, ContinuationToken = token,
                }, ct);
                seen.AddRange(page.S3Objects.Select(o => o.Key));
                if (!page.IsTruncated)
                    break;
                page.NextContinuationToken.Should().NotBeNull("truncated страница несёт токен");
                token = page.NextContinuationToken;
            }
            seen.Should().HaveCount(9, "12 создано − 3 удалено пакетом");

            // Act / Assert: GetObjectAttributes — ETag/ObjectSize/StorageClass
            var body = OwnS3E2eBytes.Deterministic(4096, 99);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "attr.bin", InputStream = new MemoryStream(body),
            }, ct);
            var attrs = await s3.GetObjectAttributesAsync(new GetObjectAttributesRequest
            {
                BucketName = bucket, Key = "attr.bin",
                ObjectAttributes = new List<ObjectAttributes>
                    { ObjectAttributes.ETag, ObjectAttributes.ObjectSize, ObjectAttributes.StorageClass },
            }, ct);
            attrs.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            attrs.ObjectSize.Should().Be(body.Length);
            attrs.StorageClass.Value.Should().Be("STANDARD");
        }, ct);
    }

    [Fact]
    public async Task ChecksumTrailer_PutGet_WhenSupported()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: приёмка канона 03 §3 реальным SDK в режиме WHEN_SUPPORTED:
        // SDK сам использует чанковую подпись с трейлерами
        // (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER + x-amz-checksum-*);
        // ETag-правило не меняется (MD5 тела)
        await OwnS3E2eEnvironment.RunScenarioAsync("s7", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer,
                OwnS3ChecksumMode.WhenSupported);
            var bucket = $"os3e2e{fx.RunTag}cs";
            var key = "trailer.bin";
            var body = OwnS3E2eBytes.Deterministic(512 * 1024, 5);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: PUT — SDK вычисляет чексуммы сам (трейлерный чанковый режим)
            var put = await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            // Assert
            put.HttpStatusCode.Should().Be(HttpStatusCode.OK,
                "трейлерный режим WHEN_SUPPORTED обязан приниматься ownS3 (канон 03 §3)");
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            get.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body),
                "трейлеры не меняют ETag-правило простого PUT");
        }, ct);
    }

    [Fact]
    public async Task ReaderRole_Denied_On_Write()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: статические ключи работают в докер-контуре (матрица 05 §3):
        // reader пишет → 403 AccessDenied; читает writer-объект → 200
        await OwnS3E2eEnvironment.RunScenarioAsync("s8", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var writer = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            using var reader = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Reader);
            var bucket = $"os3e2e{fx.RunTag}ro";
            var key = "protected.bin";
            var body = OwnS3E2eBytes.Deterministic(2048, 13);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await writer.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            // Act / Assert: reader PUT → 403
            (await reader.Invoking(x => x.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "denied.bin", InputStream = new MemoryStream(body),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "AccessDenied" && e.StatusCode == HttpStatusCode.Forbidden);

            // Act / Assert: reader GET writer-объекта → 200
            using var get = await reader.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
        }, ct);
    }
}
```

- [ ] **Шаг 6.3. Прогон класса:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.AwsSdkSimpleScenarios"
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: 8/8 PASS; чистота. Вскрыт дефект СЕРВИСА — СТОП: отдельный пункт с обоснованием против канона (spec §7), не «подгонка теста».

- [ ] **Шаг 6.4. Коммит:**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eClients.cs src/tests/OwnS3.IntegrationTests/E2e/AwsSdkSimpleScenarios.cs
git commit -m "feat(t39): AwsSdkSimpleScenarios — 8 E2E-фактов AWS SDK + фабрика клиентов (per-client checksum)"
```

**Выход:** реальный SDK прошёл простые операции против контейнера ownS3; оба режима checksum доказаны.
**Проверка:** шаг 6.3.
**Связь со spec:** §4.4 (фабрика; режимы — per-client, единственный компилируемый API), §4.5 класс 1 (пп. 1–8), §8 п.2/п.3.

---

### Задача 7: `AwsSdkMultipartScenarios` (5 Facts) (spec §4.5 класс 2)

**Вход:** Задача 6 (фабрика, `OwnS3E2eETag.Multipart`); лимит не-последней части — 5 МиБ (канон 02). `UploadPartCopy`-кейсов нет: операция снята с ownS3 (Задача 1, spec §3.4/§9.2).

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/AwsSdkMultipartScenarios.cs`

- [ ] **Шаг 7.1. Файл целиком:**

```csharp
using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 2 (spec §4.5): multipart реальным SDK — полный цикл со сборкой
// составного ETag, Range через стыки частей, Abort-семантика, перезагрузка
// части (last-writer-wins), DeleteBucket-гейт живой загрузки.
[Collection(OwnS3E2eCollection.Name)]
public sealed class AwsSdkMultipartScenarios
{
    private const int MiB = 1024 * 1024;

    [Fact]
    public async Task Multipart_FullCycle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Initiate → UploadPart×3 (2×5 МиБ + 1 МиБ хвост) → ListParts →
        // Complete (составной ETag «3-md5(concat)» по формуле канона 02 §1) →
        // GET = конкатенация частей; HEAD Size; ObjectParts — реальные 3 части
        await OwnS3E2eEnvironment.RunScenarioAsync("m1", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mp";
            var key = "multipart.bin";
            var part1 = OwnS3E2eBytes.Deterministic(5L * MiB, 101);
            var part2 = OwnS3E2eBytes.Deterministic(5L * MiB, 102);
            var part3 = OwnS3E2eBytes.Deterministic(1L * MiB, 103);
            var body = [.. part1, .. part2, .. part3];
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: полный цикл
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var partEtags = new List<string>();
            foreach (var (i, part) in new[] { (1, part1), (2, part2), (3, part3) })
            {
                partEtags.Add((await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = i,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"'));
            }

            // Assert: ListParts до Complete — 3 записи с номерами/размерами
            var parts = await s3.ListPartsAsync(new ListPartsRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct);
            parts.Parts.Should().HaveCount(3);
            parts.Parts.Select(p => p.PartNumber).Should().ContainInOrder(1, 2, 3);
            parts.Parts.Select(p => p.Size).Should()
                .ContainInOrder(part1.Length, part2.Length, part3.Length);

            // Act: Complete → составной ETag
            var completed = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = partEtags.Select((e, idx) => new PartETag(idx + 1, e)).ToList(),
            }, ct);
            completed.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Multipart(3, partEtags),
                "составной ETag = «3-md5(concat бинарных ETag'ов частей)» (канон 02 §1)");

            // Assert: GET = конкатенация; HEAD Size; ObjectParts = 3
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            (await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct))
                .ContentLength.Should().Be(body.Length);
            var attrs = await s3.GetObjectAttributesAsync(new GetObjectAttributesRequest
            {
                BucketName = bucket, Key = key,
                ObjectAttributes = new List<ObjectAttributes> { ObjectAttributes.ObjectParts },
            }, ct);
            attrs.ObjectParts.Should().NotBeNull();
            attrs.ObjectParts!.Parts.Should().HaveCount(3, "атрибуты несут реальные части");
        }, ct);
    }

    [Fact]
    public async Task Multipart_Range_Over_Part_Boundaries()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: bytes через стык частей (граница 5 МиБ) → 206, точный срез по
        // границам частей (механика t38 — реальным клиентом)
        await OwnS3E2eEnvironment.RunScenarioAsync("m2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mr";
            var key = "mp-range.bin";
            var part1 = OwnS3E2eBytes.Deterministic(5L * MiB, 201);
            var part2 = OwnS3E2eBytes.Deterministic(5L * MiB, 202);
            var body = [.. part1, .. part2];
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var etags = new List<string>();
            foreach (var (i, part) in new[] { (1, part1), (2, part2) })
            {
                etags.Add((await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = i,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"'));
            }
            await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = etags.Select((e, idx) => new PartETag(idx + 1, e)).ToList(),
            }, ct);

            // Act: диапазон через стык частей 1|2
            using var get = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, ByteRange = new ByteRange("bytes=5242860-5242930"),
            }, ct);

            // Assert
            get.HttpStatusCode.Should().Be(HttpStatusCode.PartialContent);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body[5242860..5242931]);
        }, ct);
    }

    [Fact]
    public async Task Multipart_Abort_Semantics()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Abort → 204; повторный Abort → NoSuchUpload 404; Complete
        // после Abort → NoSuchUpload; ListParts после Abort → NoSuchUpload
        await OwnS3E2eEnvironment.RunScenarioAsync("m3", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}ma";
            var key = "mp-abort.bin";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var etag = (await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = 1,
                InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1 * MiB, 301)),
            }, ct)).ETag!;

            // Act / Assert: Abort → 204; повторный → NoSuchUpload 404
            (await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload" && e.StatusCode == HttpStatusCode.NotFound);

            // Act / Assert: Complete после Abort → NoSuchUpload
            (await s3.Invoking(x => x.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = [new(1, etag.Trim('"'))],
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload");

            // Act / Assert: ListParts после Abort → NoSuchUpload
            (await s3.Invoking(x => x.ListPartsAsync(new ListPartsRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload");
        }, ct);
    }

    [Fact]
    public async Task Multipart_PartReupload_LastWriterWins()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: повторная загрузка того же partNumber другим телом (5 МиБ —
        // лимит не-последней части) → Complete собирает ПОСЛЕДНЮЮ версию
        await OwnS3E2eEnvironment.RunScenarioAsync("m4", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mw";
            var key = "mp-reupload.bin";
            var stale = OwnS3E2eBytes.Deterministic(5L * MiB, 401);
            var fresh = OwnS3E2eBytes.Deterministic(5L * MiB, 402);
            var tail = OwnS3E2eBytes.Deterministic(1 * MiB, 403);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            async Task<string> UploadAsync(int number, byte[] part) => (await s3.UploadPartAsync(
                new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = number,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"');

            // Act: part1 дважды (stale → fresh), part2 — хвост
            _ = await UploadAsync(1, stale);
            var etag1 = await UploadAsync(1, fresh);
            var etag2 = await UploadAsync(2, tail);
            var completed = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = [new(1, etag1), new(2, etag2)],
            }, ct);

            // Assert: объект = fresh + tail; составной ETag по последним частям
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal([.. fresh, .. tail], "побеждает последняя версия части");
            completed.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Multipart(2, [etag1, etag2]));
        }, ct);
    }

    [Fact]
    public async Task DeleteBucket_BlockedByLiveUpload()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: гейт t38 — DeleteBucket при живой multipart-загрузке → 409
        // BucketNotEmpty; после Abort → 204
        await OwnS3E2eEnvironment.RunScenarioAsync("m5", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mg";
            var key = "mp-live.bin";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = 1,
                InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1 * MiB, 501)),
            }, ct);

            // Act / Assert: живая загрузка блокирует удаление бакета
            (await admin.Invoking(x => x.DeleteBucketAsync(
                    new DeleteBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "BucketNotEmpty" && e.StatusCode == HttpStatusCode.Conflict);

            // Act / Assert: Abort → бакет пуст → удаление 204
            await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct);
            (await admin.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
        }, ct);
    }
}
```

- [ ] **Шаг 7.2. Прогон класса:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.AwsSdkMultipartScenarios"
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: 5/5 PASS; чистота.

- [ ] **Шаг 7.3. Коммит:**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/AwsSdkMultipartScenarios.cs
git commit -m "feat(t39): AwsSdkMultipartScenarios — multipart-цикл AWS SDK (составной ETag, Range через стыки, Abort, reupload, гейт DeleteBucket)"
```

**Выход:** multipart реальным SDK доказан (5 Facts — весь состав класса 2 спеки v3).
**Проверка:** шаг 7.2.
**Связь со spec:** §4.5 класс 2 (все 5 кейсов), §8 п.2/п.3.

---

### Задача 8: `McScenarios` — mc как второй реальный клиент (spec §4.5 класс 3)

**Вход:** Задача 5 (окружение: `S3InternalUrl`, `NetName`, `McImage`, `RunDockerAsync`); прецедент `E2eBackupJobsScenarios.McLsAsync` (ephemeral `docker run --rm --network … --entrypoint /bin/sh`).

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/McScenarios.cs`

- [ ] **Шаг 8.1. Файл целиком** (mc ходит ИЗНУТРИ docker-сети окружения на `S3InternalUrl`; вывод mc — в диагностику ассертов; парсинг минимальный, маркеры):

```csharp
using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 3 (spec §4.5): mc — второй реальный клиент (оператор; SigV4 с
// собственной канонизацией). Ephemeral docker run в сети окружения
// (прецедент E2eBackupJobsScenarios); образ запинен. Формат вывода mc
// версионно-зависим — сверки по маркерам/счётчикам, без хрупких колонок.
[Collection(OwnS3E2eCollection.Name)]
public sealed class McScenarios
{
    // mc-скрипт: alias → операции; вывод маркеров; exit-код mc = код скрипта.
    private static Task<string> McAsync(OwnS3E2eEnvironment fx, string script, CancellationToken ct)
        => fx.RunDockerAsync(
        [
            "run", "--rm", "--network", fx.NetName, "--entrypoint", "/bin/sh",
            OwnS3E2eEnvironment.McImage,
            "-c", $"mc alias set t {fx.S3InternalUrl} '{OwnS3E2eEnvironment.RootUser}' " +
                  $"'{OwnS3E2eEnvironment.RootPassword}' >/dev/null && {script}",
        ], ct);

    [Fact]
    public async Task Mc_Bucket_Object_Lifecycle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: alias → mb → cp (128 КиБ /dev/urandom внутри mc-контейнера) →
        // ls видит → cat|sha256 == источник → stat → скачивание (cmp) → rm →
        // ls пуст → rb — полный жизненный цикл оператора
        await OwnS3E2eEnvironment.RunScenarioAsync("c1", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}mc";

            // Act: один sh-сценарий; маркер несёт обе sha256
            var output = await McAsync(fx, $"""
                mc mb t/{bucket} \
                  && head -c 131072 /dev/urandom > /tmp/os3e2e.bin \
                  && SRC=$(sha256sum /tmp/os3e2e.bin | cut -d' ' -f1) \
                  && mc cp /tmp/os3e2e.bin t/{bucket}/obj.bin >/dev/null \
                  && mc ls t/{bucket} | grep -q obj.bin \
                  && GOT=$(mc cat t/{bucket}/obj.bin | sha256sum | cut -d' ' -f1) \
                  && mc stat t/{bucket}/obj.bin | grep -E 'Size|ETag' >/dev/null \
                  && mc cp t/{bucket}/obj.bin /tmp/os3e2e-out.bin >/dev/null \
                  && cmp -s /tmp/os3e2e.bin /tmp/os3e2e-out.bin \
                  && mc rm t/{bucket}/obj.bin >/dev/null \
                  && [ -z "$(mc ls t/{bucket} 2>/dev/null)" ] \
                  && mc rb t/{bucket} >/dev/null \
                  && echo "OS3E2E_SHA $SRC $GOT"
                """, ct);

            // Assert: roundtrip байтов сходится (sha256 источник == cat)
            output.Should().MatchRegex(@"OS3E2E_SHA [0-9a-f]{64} [0-9a-f]{64}",
                "mc cat обязан вернуть те же байты, что cp; вывод: {0}", output);
            var shas = output.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Where(t => t.Length == 64 && t.All(char.IsAsciiHexDigit)).ToList();
            shas.Should().HaveCountGreaterThanOrEqualTo(2);
            shas[0].Should().Be(shas[1], "sha256(mc cat) == sha256(источник)");
        }, ct);
    }

    [Fact]
    public async Task Mc_Recursive_Listing()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: набор ключей с префиксами → ls --recursive перечисляет все;
        // ls <bucket>/prefix фильтрует (путь листинга цепочек бэкапов)
        await OwnS3E2eEnvironment.RunScenarioAsync("c2", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}ml";

            // Act
            var output = await McAsync(fx, $"""
                mc mb t/{bucket} \
                  && mc cp /etc/hostname t/{bucket}/demo/s1/full/base.tar >/dev/null \
                  && mc cp /etc/hostname t/{bucket}/demo/s1/wal/00001 >/dev/null \
                  && mc cp /etc/hostname t/{bucket}/demo/s2/full/base.tar >/dev/null \
                  && echo "OS3E2E_ALL $(mc ls --recursive t/{bucket} | wc -l | tr -d ' ')" \
                  && echo "OS3E2E_PREFIX $(mc ls t/{bucket}/demo/s1 | wc -l | tr -d ' ')"
                """, ct);

            // Assert: 3 объекта рекурсивно; префикс demo/s1 — 2 записи
            output.Should().Contain("OS3E2E_ALL 3",
                "рекурсивный листинг видит все ключи; вывод: {0}", output);
            output.Should().Contain("OS3E2E_PREFIX 2",
                "листинг по префиксу фильтрует; вывод: {0}", output);
        }, ct);
    }

    [Fact]
    public async Task Mc_Alias_Region_Protocol()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: mc alias set с --api S3v4 — сквозная проверка заголовочной
        // SigV4-подписи вторым независимым клиентом (собственная канонизация)
        await OwnS3E2eEnvironment.RunScenarioAsync("c3", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}mv";

            // Act: alias с явным протоколом + полный мини-цикл
            var output = await fx.RunDockerAsync(
            [
                "run", "--rm", "--network", fx.NetName, "--entrypoint", "/bin/sh",
                OwnS3E2eEnvironment.McImage,
                "-c", $"mc alias set --api S3v4 t {fx.S3InternalUrl} " +
                      $"'{OwnS3E2eEnvironment.RootUser}' '{OwnS3E2eEnvironment.RootPassword}' >/dev/null " +
                      $"&& mc mb t/{bucket} >/dev/null " +
                      $"&& mc cp /etc/hostname t/{bucket}/probe >/dev/null " +
                      $"&& mc cat t/{bucket}/probe | wc -c | tr -d ' '",
            ], ct);

            // Assert: копия читается (непустой счётчик байтов)
            int.TryParse(output.Trim(), out var bytes).Should().BeTrue("вывод mc: {0}", output);
            bytes.Should().BeGreaterThan(0, "mc (--api S3v4) проходит подпись ownS3 end-to-end");
        }, ct);
    }
}
```

- [ ] **Шаг 8.2. Прогон класса:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.McScenarios"
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: 3/3 PASS; чистота (ephemeral mc-контейнеры `--rm` исчезают сами).

- [ ] **Шаг 8.3. Коммит:**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/McScenarios.cs
git commit -m "feat(t39): McScenarios — mc как второй реальный клиент (lifecycle, recursive-листинг, S3v4-подпись)"
```

**Выход:** SigV4-подпись подтверждена вторым независимым клиентом; roundtrip байтов сходится.
**Проверка:** шаг 8.2.
**Связь со spec:** §4.4 (mc), §4.5 класс 3, §8 п.4.

---

### Задача 9: `HealthMetricsScenarios.Metrics_Prometheus_Series_AfterTraffic` (spec §4.5 класс 4 п. 2)

**Вход:** Задачи 5–6. Поведение `/metrics` — OTel Prometheus-экспортёр с периодическим reader (серии появляются ближайшим тиком; фактическая форма лейблов `ownS3_requests_total{otel_scope_name=…,code=…,operation=…}` — ассерты по отдельным подстрокам, без хрупкой склейки полной серии).

**Files:**
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs` (добавить Fact + хелпер)

- [ ] **Шаг 9.1. Добавить в класс `HealthMetricsScenarios`:**

```csharp
    [Fact]
    public async Task Metrics_Prometheus_Series_AfterTraffic()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: SDK-трафик (PUT/GET/LIST) → /metrics несёт каталожные серии
        // канона 05 §5 (requests_total с operation/code, duration-гистограмма,
        // дисковые gauge); OTel reader периодический — полл (0.5 c, бюджет 30 c)
        await OwnS3E2eEnvironment.RunScenarioAsync("hm2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}hm";
            await admin.PutBucketAsync(new Amazon.S3.Model.PutBucketRequest { BucketName = bucket }, ct);
            var body = OwnS3E2eBytes.Deterministic(4096, 77);
            await s3.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
            {
                BucketName = bucket, Key = "metrics.bin", InputStream = new MemoryStream(body),
            }, ct);
            using var get = await s3.GetObjectAsync(
                new Amazon.S3.Model.GetObjectRequest { BucketName = bucket, Key = "metrics.bin" }, ct);
            await s3.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
                { BucketName = bucket }, ct);

            // Act: полл /metrics (plain HttpClient, без подписи) до всех паттернов
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            var deadline = Environment.TickCount64 + 30_000;
            string metrics = "";
            while (true)
            {
                metrics = await http.GetStringAsync($"{fx.S3BaseUrl}/metrics", ct);
                var hasAll = metrics.Contains("ownS3_requests_total", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"PutObject\"", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"GetObject\"", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"ListObjectsV2\"", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_request_duration_seconds_bucket", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_disk_used_bytes", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_disk_total_bytes", StringComparison.Ordinal);
                if (hasAll || Environment.TickCount64 >= deadline)
                    break;
                await Task.Delay(500, ct);
            }

            // Assert: серии и лейблы (форма лейблов OTel — порядок переменный,
            // поэтому отдельные подстроки)
            metrics.Should().Contain("ownS3_requests_total");
            metrics.Should().Contain("operation=\"PutObject\"", "PUT был; вывод:\n{0}", metrics);
            metrics.Should().Contain("operation=\"GetObject\"");
            metrics.Should().Contain("operation=\"ListObjectsV2\"");
            metrics.Should().Contain("code=\"200\"", "все запросы сценария успешны");
            metrics.Should().Contain("ownS3_request_duration_seconds_bucket");
            metrics.Should().Contain("ownS3_disk_used_bytes");
            metrics.Should().Contain("ownS3_disk_total_bytes");

            // Assert: gauge-значения (used > 0 после PUT; total >= used)
            var used = ParseGauge(metrics, "ownS3_disk_used_bytes");
            var total = ParseGauge(metrics, "ownS3_disk_total_bytes");
            used.Should().BeGreaterThan(0, "после PUT том занят (снимок: {0})", metrics);
            total.Should().BeGreaterThanOrEqualTo(used);
        }, ct);
    }

    // Gauge-строка Prometheus: «имя значение» (учёт экспоненциальной записи).
    private static double ParseGauge(string metrics, string name)
    {
        var line = metrics.Split('\n')
            .FirstOrDefault(l => l.StartsWith(name + " ", StringComparison.Ordinal))
            ?? throw new Xunit.Sdk.XunitException($"gauge {name} не найден в /metrics");
        return double.Parse(line[(name.Length + 1)..].Trim(),
            System.Globalization.CultureInfo.InvariantCulture);
    }
```

- [ ] **Шаг 9.2. Прогон класса:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.HealthMetricsScenarios"
docker ps -a --filter name=os3- --format '{{.Names}}'
```
Критерий: 2/2 PASS (Healthz + Metrics); чистота.

- [ ] **Шаг 9.3. Коммит:**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs
git commit -m "feat(t39): Metrics_Prometheus_Series_AfterTraffic — серии ownS3_* в докер-контуре после SDK-трафика"
```

**Выход:** health/метрики подтверждены в docker-контуре E2E (буква roadmap-пункта 3).
**Проверка:** шаг 9.2.
**Связь со spec:** §4.5 класс 4, §8 п.5.

---

### Задача 10: Мерж-гейт (полный) + roadmap/arch-чистка t39 (spec §5 фаза 6, §8)

**Вход:** Задачи 1–9 завершены и закоммичены.

**Files:**
- Modify: `arch/roadmap/owns3.md`, `arch/22-owns3.md`, `arch/owns3/01-overview.md`, `arch/owns3/03-protocol.md` (чистка упоминаний t39)

- [ ] **Шаг 10.1. Сборка и прогоны без docker:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release 2>&1 | tail -3
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build
```
Критерий: build 0 warnings; юниты и интеграция зелёные (E2e-факты без `PGW_TEST_DOCKER` — Skip).

- [ ] **Шаг 10.2. Grep-гейт §4.6 (расширенный шаблон):**

```bash
grep -rin "ifrange\|if-range\|uploadpartcopy\|copypartresult" src/OwnS3.Protocol src/OwnS3.Storage \
  src/OwnS3.App src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests --include="*.cs" | wc -l
```
Критерий: `0`.

- [ ] **Шаг 10.3. Docker-E2E серия целиком на свежем Release + чистота хоста:**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e"
docker ps -a --filter name=os3- --format '{{.Names}}'
docker volume ls --filter name=os3- --format '{{.Name}}'
docker network ls --filter name=os3- --format '{{.Name}}'
```
Критерий: 18/18 Facts PASS (8 simple + 5 multipart + 3 mc + 2 health/metrics); автосборка publish+образ с `[PHASE]`-метками; все три списка пусты. При падении — разбор по `/tmp/pgw-e2e-artifacts-{guid}/` (docker-логи, phases.log), перезапуск запрещён до формулировки причин (канон e2e-launch §4).

- [ ] **Шаг 10.4. Бисект-гейт `PGW_TEST_E2E_NOBUILD=1`:**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 PGW_TEST_E2E_NOBUILD=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.Healthz_200_NoSignature"
```
Критерий: PASS; сборка пропущена (нет `[PHASE] owns3:e2e publish`).

- [ ] **Шаг 10.5. Стенд-гейт: полный подъём + чек:**

```bash
bash dev-stand/adminpanel/checks/00-up.sh
bash dev-stand/adminpanel/checks/46-owns3.sh
```
Критерий: стенд поднялся (в журнале — `[PHASE] publish OwnS3.App`, «owns3 жив»); чек 46 — `✓ owns3-чек зелёный`. Стендовые бэкапы остались на as-minio: `grep PGW_BACKUP_S3_ENDPOINT deploy/.env` → `as-minio`/`:9000` не изменился.

- [ ] **Шаг 10.6. Правка spec.md — механизм flexible checksums по факту SDK** (задача-документ, не arch-канон; подтверждение ревью Фазы 4: формулировки §4.4/§4.5/§7/§9.8 описывают несуществующий в AWSSDK.Core 3.7.501.1 механизм — глобальной статики `Amazon.Util.AWSConfigs.RequestChecksumCalculation` нет, проверено компиляцией; реализация плана — per-client `AmazonS3Config.RequestChecksumCalculation`). Точечные замены:

1. §4.4, абзац «**Flexible checksums** …» (со строки «`Amazon.Util.AWSConfigs.RequestChecksumCalculation` — глобальная статика SDK.» до «…параллельность коллекций не конфликтует.») заменить на:

```
- **Flexible checksums** (канон 03 §3): настройка per-client —
  `AmazonS3Config.RequestChecksumCalculation` (enum
  `Amazon.Runtime.RequestChecksumCalculation`: `WHEN_REQUIRED`/`WHEN_SUPPORTED`;
  глобальной статики `AWSConfigs.RequestChecksumCalculation` в AWSSDK.Core
  3.7.501.1 нет — проверено по пакету). Фабрика принимает режим: основные
  Facts создают клиента в `WHEN_REQUIRED` (режим, в который обязан быть
  настроен клиент бэкапов: без него свежий SDK шлёт неподдерживаемый
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER` → 400), отдельный Fact — в
  `WHEN_SUPPORTED` (SDK сам использует чанковый режим с подписанными
  трейлерами). E2E-классы последовательны одной коллекцией
  (консервативная мера; глобального состояния серия не имеет). In-memory
  `Api`-сценарии AWS SDK не используют.
```

2. §4.5, класс 1 п.7: «перевод `AWSConfigs.RequestChecksumCalculation = WHEN_SUPPORTED` →» заменить на «клиент фабрики в режиме `WHEN_SUPPORTED` →».
3. §7, пункт «**Глобальная `AWSConfigs.RequestChecksumCalculation`**: …» заменить на: «- **Flexible checksums per-client** (`AmazonS3Config.RequestChecksumCalculation`): фабрика задаёт режим на каждом клиенте; env `AWS_REQUEST_CHECKSUM_CALCULATION` (статический кеш SDK) серией не используется — влияние окружения исключено.»
4. §9, п.8: «…глобальная статика `AWSConfigs.RequestChecksumCalculation` не терпит параллельных переключателей; изоляция окружений при этом полная (per-class).» заменить на: «…настройка checksum — per-client (`AmazonS3Config.RequestChecksumCalculation`; глобальной статики в AWSSDK.Core 3.7.501.1 нет), последовательность — консервативная мера; изоляция окружений полная (per-class).»
5. §6 «Тестовая стратегия»: «Сами E2E-классы последовательны (одна коллекция — изоляция глобальной `AWSConfigs`; окружения при этом независимы).» заменить на: «Сами E2E-классы последовательны (одна коллекция — последовательный прогон, консервативная мера против глобального состояния SDK; окружения при этом независимы).» — формулировка «изоляция глобальной AWSConfigs» ложна (статики нет).
6. Блок «Правки ревью спеки (решения пользователя)» (§0-шапка, после пункта Q2) — добавить пункт, снимающий расхождение spec ↔ фактический канон (третья контрактовая правка вне перечня §3 и декларации не-целей «иные контрактовые изменения — вне t39»):

```
- **Trailing slash `/{bucket}/` — bucket-level** (внесено шагом 4.1a плана,
  решение пользователя по схеме Q1/Q2 — фикс дефекта Фазы 6): правка
  03 §6 — пустой ключ трактуется как его отсутствие (прецедент S3/MinIO;
  AWS SDK CreateBucket и mc probe шлют именно так).
```

Контроль:

```bash
grep -n "AWSConfigs.RequestChecksumCalculation" docs/superpowers/2026-10-10-t39-owns3-e2e/spec.md
grep -cn "bucket-level" docs/superpowers/2026-10-10-t39-owns3-e2e/spec.md
```
Критерий: первая команда — совпадения ТОЛЬКО в негативных формулировках («глобальной статики `AWSConfigs.RequestChecksumCalculation` … нет — проверено»); утверждений о существовании/использовании глобальной статики не осталось. Вторая — ≥ 1 (пункт о trailing slash в блоке «Правки ревью спеки» на месте — spec больше не расходится с фактическим каноном 03 §6).

- [ ] **Шаг 10.7. Roadmap/arch-чистка тега t39 (мерж-гейт — тем же мерж-коммитом, правило AGENTS.md):**

1. `arch/roadmap/owns3.md`: удалить пункт `**t39-owns3-e2e**` целиком; у `t40`/`t41`/`t42` снять зависимость `← t39-owns3-e2e`; шапка — без живой ссылки на t39; внутри пункта t40 (~строка 67) фразу «в начальной версии t36–t39 не реализуется» переписать без адресации («в начальной версии не реализуется»).
2. `arch/22-owns3.md` (входная точка): упоминание «после приёмки t39» → безадресная форма («после приёмочной грани клиентами»); отдельное упоминание ~строки 47 «новые задачи после t39 (миграция с MinIO, TLS)» → «новые задачи после приёмочной грани (миграция с MinIO, TLS)».
3. `arch/owns3/01-overview.md` §1: «целевая замена MinIO подсистемы бэкапов после приёмки t39» → «…после приёмочной грани клиентами».
4. `arch/owns3/03-protocol.md` §3 (~строка 214): «это требуется учесть при приёмке t39 (настройка клиентов бэкапов и `mc`)» → «это требуется учесть при приёмке клиентами (настройка клиентов бэкапов и `mc`)».

Контроль:

```bash
grep -rn "t39" arch/
```
Критерий: пусто (история — в git и `docs/superpowers/`; фиксации снятия If-Range/UploadPartCopy в 02 — безадресные, остаются).

- [ ] **Шаг 10.8. Финальный коммит:**

```bash
git add docs/superpowers/2026-10-10-t39-owns3-e2e/spec.md \
        arch/roadmap/owns3.md arch/22-owns3.md arch/owns3/01-overview.md arch/owns3/03-protocol.md
git commit -m "docs(t39): мерж-гейт — spec per-client checksum (факт SDK), снятие тега t39 с roadmap и упоминаний в канонах owns3"
```

**Выход:** все критерии приёмки spec §8 подтверждены прогонами; spec приведён к факту SDK; ветка готова к мерж-гейту dev-flow (мерж в `main` — по явной команде пользователя).
**Проверка:** шаги 10.1–10.7.
**Связь со spec:** §5 фаза 6, §8 п.1–п.12; дополнение ревью Фазы 4 (механизм checksum).

---

## Порядок исполнения и зависимости

```
Задача 1 (зачистка If-Range + UploadPartCopy; канон-правки уже в worktree)
   ──► Задача 2 (Dockerfile) ──► Задача 5 (E2E-инфра) ──► Задача 6 (SDK simple)
                 │                                            ├─► Задача 7 (multipart)
                 │                                            ├─► Задача 8 (mc)
                 │                                            └─► Задача 9 (metrics)
                 └─► Задача 3 (стенд) ──► Задача 4 (фикс 4.1a trailing-slash + чек 46)
Все ──► Задача 10 (мерж-гейт + roadmap/arch-чистка)
```

## Сводка критериев приёмки (spec §8) по задачам

| Критерий spec | Задача/шаг |
|---|---|
| 1. E2E-серия зелёная, автосборка, NOBUILD | 5.6, 10.3, 10.4 |
| 2. AWS SDK простые+multipart (21 операция, без If-Range/UploadPartCopy) | 6.3, 7.2 |
| 3. Оба режима flexible checksums (per-client — факт SDK) | 6.3 (WHEN_SUPPORTED-факт; остальные — WHEN_REQUIRED) |
| 4. mc-клиент, sha256 roundtrip | 4.1a (trailing-slash), 8.2 |
| 5. healthz/metrics в докер-контуре | 5.6, 9.2 |
| 6. Изоляция/телеметрия/чистота, MarkFailed | 5.6–5.8, 10.3 |
| 7. Стенд-сервис, порт 9010, чек 46 | 2.2a (env-маппинг), 3.5, 4.2, 10.5 |
| 8. Образ publish-COPY, без registry | 2.2 |
| 9. If-Range и UploadPartCopy сняты полностью (канон 21, grep-гейт, нерегрессии) | 1.1, 1.24–1.25, 10.2 |
| 10. Секция OWNS3_* в .env.example | 3.2 |
| 11. Канон 05 §6 ↔ реализация; чистка t39 (вкл. 22-owns3.md) | 10.7 (10.6 — spec per-client) |
| 12. Сборка чистая, юниты/интеграция зелёные, t36–t38 без изъятых кейсов | 1.25, 10.1 |

## Саморевью (выполнено автором плана)

1. **Покрытие spec v3:** §3.1–3.4 (arch-правки, 6 файлов) — в worktree, Задача 1.1 сверяет, шаг 1.27 коммитит вместе с кодом; §4.1→З2; §4.2→З3/З4; §4.3→З5; §4.4→З6 (per-client checksum — единственный компилируемый API; коллекция §4.7 — по букве в З5.3); §4.5 класс 1→З6, класс 2 (5 кейсов)→З7, класс 3→З8, класс 4→З5.5/З9; §4.6 обе таблицы→З1 (шаги 1.2–1.9 If-Range, 1.10–1.23 UploadPartCopy; `PartCopyRequest` — по принципу «полного изъятия механики», шаг 1.21); §4.7→З5; §5 фазы 0–6→З1.1/З1/З2/З3–З4/З5/З6–З9/З10; §8 п.1–12 — таблица; spec-правка per-client (дополнение ревью)→З10.6; roadmap/arch-гейт → З10.7 (вкл. `arch/22-owns3.md` и упоминание t39 внутри пункта t40 roadmap).
2. **Плейсхолдеры:** отсутствуют — каждый шаг содержит код/команды/критерий; блокированных позиций нет (Q2 решён, класс 2 — ровно 5 Facts).
3. **Типы/сигнатуры:** имена членов SDK сверены с пакетом (шапка «Факты»); `ObjectReadOptions(Conditions, Range)` консистентен в З1.2–1.9; `RangeResolver.Resolve(ByteRange?, long)` — З1.3/1.4; `IObjectStore` без `UploadPartCopyAsync` — З1.18/1.19; остаточная маршрутизация `PUT ?partNumber&uploadId`+`x-amz-copy-source` → `UploadPart` (З1.11, сверка 1.26); API окружения З5 используется в З6–З9 без расхождений; `OwnS3E2eCollection.Name` — З5.3 и `[Collection]` всех классов.
4. **Ревью Фазы 4 (CHANGES_REQUESTED) — все позиции закрыты:** (1) CommonPrefixes по семантике ListWalker «первый delimiter после prefix» — двухзапросный ассерт в З6.2; (2) `RunDockerOrNullAsync(args, ct)` — сигнатура и вызовы согласованы; (3) mc-smoke чека 46 — через хост-публикацию `host.docker.internal:${PORT}` (канон 05 §6, spec §4.2 п.3); (4) фазовая телеметрия — phases.log (UTC) для всех фаз вкл. сборочные, `LogPhaseAsync` с slow-phase-диагностикой > 60 с, причина `failed` в RunScenarioAsync; для сборочных фаз docker-диагностика неприменима (контейнера нет) — обосновано, их полные логи в `ArtifactsDir/process-build-*.log`; (5–6) шапки-комментарии MultipartHandlers (12–14) и S3PresignedOperations («ровно 9») включены в шаги зачистки — grep-гейт 1.24 чист; (7) счёт If-Range-вхождений убран из «Входа» Задачи 1 (гейт 1.24 — исчерпывающая проверка); дополнение — шаг 10.6 приводит spec к per-client факту SDK.
5. **Повторное ревью Фазы 4 (6 low, тексто-ссылочные) — закрыты:** (1) преамбула 4.1 синхронна телу (хост-публикация, `--add-host host-gateway`, без `--network`); (2) устаревающие счётчики «22» (10 строк — S3Operation:3, S3Router:5, S3Middleware:29/:61, IObjectStore:3, RoutingScenarios:8, S3RouterTests:5/:76, OperationAccessMatrixTests:6; Program:52 — в 1.15) переписываются на «21» в шагах 1.10/1.11/1.15/1.18/1.22 + контроль-гейт 1.24 расширен шаблоном `22 операц|22 хендлера`; (3) шаг 10.7 дополнен правками `arch/22-owns3.md:47` и упоминания t39 внутри пункта t40 roadmap (~:67); (4) в 10.6 добавлена пятая замена (spec §6 — «изоляция глобальной AWSConfigs» → консервативная мера); (5) `DisposeAsync` обёрнут в `Stopwatch` + `LogPhaseAsync("teardown", sw)` в обеих ветках (код; doc-комментарий уже декларирует все фазы); (6) ссылки критерия §8 п.11 исправлены на 10.6+10.7 (таблица и саморевью).
6. **Фаза 6, дефект вне плана (шаг 2.2) — закрыт шагом 2.2a:** OwnS3.App не читал канонические env `OWNS3_*` (арх 05 §4) — .NET-провайдер ждёт `OwnS3__*`, в докере сервис падал на fail-fast (тесты t36–t38 задавали конфиг in-memory, дефект не был виден). Фикс (одобрен пользователем, вариант 1): трансляция env → ключи `OwnS3:*` в Program.cs (~35 строк, env приоритетнее appsettings, все имена 05 §4 вкл. `OWNS3_ACCESS_KEYS__<i>__*`); arch-first — код приводится к канону, канон не меняется; основание — base-rules §1 + spec §7. Коммит — шаг 2.3 (Program.cs добавлен в git add).
7. **Фаза 6, второй дефект вне плана (trailing slash) — закрыт шагом 4.1a:** `S3PathParser` трактовал `/{bucket}/` как объектный запрос с `Key=""` → 400 на `PUT /{bucket}/` (AWSSDK `PutBucketAsync`) и `GET /probe-*/?location=` (mc alias-probe) — блокировал 4.2 и Задачи 6–9. Фикс (одобрен пользователем, вариант 1 — схема 2.2a): arch-first правка `arch/owns3/03-protocol.md` §6 ПЕРВОЙ («`/{bucket}/` — bucket-level: пустой ключ = его отсутствие, прецедент S3/MinIO; `a//b` — прежняя семантика; подпись не затрагивается — canonical URI из raw-target») + одна ветка в `S3PathParser.Parse` (`rawKey.Length == 0 → new S3Path(bucket, null)`) + юнит-кейсы парсера/роутера; коммит arch+код+тесты вместе (шаг 4.1a п.5). Вставлен ДО первого заблокированного шага (4.2), нумерация не тронута. Контрольное ревью 4.1a (3 замечания) закрыто: (a) в 4.1a добавлена пересборка стенд-сервиса (п.3: publish → `compose up -d --build --force-recreate owns3` → healthz-гейт — контейнер 3.5 собран из пре-фикс артефактов); (b) решение пользователя зафиксировано в spec шестой заменой шага 10.6 (пункт в блоке «Правки ревью спеки» — «03 §6: `/{bucket}/` — bucket-level, внесено шагом 4.1a, схема Q1/Q2»); (c) форвард-ссылка на 6.3 убрана из критерия 4.1a («SDK-сторона подтверждается при прогоне Задачи 6 — E2E-образ строится из текущего исходника»), якорь вставки уточнён (строка 19), шапка S3PathParserTests.cs дополняется формой `/{bucket}/`.
