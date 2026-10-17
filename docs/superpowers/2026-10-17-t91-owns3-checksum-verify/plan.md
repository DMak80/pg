# t91-owns3-checksum-verify: заголовочная checksum-верификация PutObject + `/s3/health/cluster` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ownS3 сверяет заголовок `x-amz-checksum-{crc32,crc32c,sha1,sha256}` на PutObject с фактически прочитанным телом (несовпадение → 400 `BadDigest`, формат/мульти → 400 `InvalidArgument`, объект не коммитится) и отдаёт публичный кластер-health `GET /s3/health/cluster` (200/503 + JSON drives-счётчики camelCase) для ClusterOk-пробы панели.

**Architecture:** Верификация встраивается в существующий конвейер без второго прохода по телу: чистый парсер `OperationValidation.ParseChecksumHeader` (отказ формата/мульти — до чтения тела), вычисление ровно одного выбранного алгоритма — в точках `AppendData` обёртки `HashingBodyStream`, сверка — в её EOF-точке `VerifyHashes` (порядок: sha256-подпись → checksum → MD5); чанковые режимы не затронуты (авторитет — трейлер). Байтовая конвенция CRC — общий helper `ChecksumConventions` в `OwnS3.Protocol`, общий для трейлерного (`AwsChunkedReader`) и заголовочного путей; типы: crc32 — `System.IO.Hashing.Crc32`, crc32c — собственный `OwnS3.Protocol.Auth.Crc32C` (big-endian). Кластер-health — шаг 0 `S3Middleware` (префикс `/s3/health/` до роутинга 21 операции) + `MapGet` + хендлер `AdminHandlers.HandleClusterHealthAsync` (источники — те же, что у `/admin/storage`).

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit.v3 + FluentAssertions (AAA-комментарии), `System.IO.Hashing` 10.0.12 (только `Crc32`; в 10.x тип `Crc32C` отсутствует — используется собственный `OwnS3.Protocol.Auth.Crc32C`), AWS SDK .NET (`AWSSDK.S3` 3.7.511.8) в E2E, testcontainers-контур `OwnS3E2eEnvironment` (сборка Release в фикстуре).

**Spec:** `docs/superpowers/2026-10-17-t91-owns3-checksum-verify/spec.md`. Канон: `arch/owns3/02-operations.md` (PutObject, заголовки), `arch/owns3/03-protocol.md` §3/§5, `arch/owns3/05-access-config.md` §5, `arch/owns3/06-erasure.md` (write-кворум); контракт панели — `arch/adminpanel/02-etcd-contract.md` §2.5. Arch-правки фазы 0 УЖЕ внесены в worktree некоммиченными — фиксируются первым коммитом этой задачи.

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`**; версионирование пакетов — централизованное (`src/Directory.Packages.props`); НОВЫХ пакетов/версий не добавляется: `System.IO.Hashing` уже подключён в `OwnS3.Protocol` (транзитивно доступен тестам), в `OwnS3.App` добавляется ссылка `<PackageReference Include="System.IO.Hashing"/>` БЕЗ версии (CPM).
- Тексты ошибок — фиксированы спекой §3.1 дословно: несовпадение `BadDigest` / `The checksum you specified did not match the calculated checksum.` (тот же текст, что у трейлерной сверки `AwsChunkedReader.Finish`); формат — `InvalidArgument` / `Value for x-amz-checksum-<algorithm> header is invalid.` (`<algorithm>` — фактическое wire-имя: crc32/crc32c/sha1/sha256); мульти — `InvalidArgument` / `Invalid checksum provided.`
- CRC-типы и байтовая конвенция (закреплена зелёным тестом `AwsChunkedReaderTests.Checksums_KnownAnswerVectors_Rfc3720`): crc32 — `System.IO.Hashing.Crc32`, отдаёт little-endian → big-endian разворотом (эталон «123456789» → 0xCBF43926 → `y/Q5Jg==`); crc32c — **собственный `OwnS3.Protocol.Auth.Crc32C`** (в System.IO.Hashing 10.x тип `Crc32C` отсутствует — удалён апстримом; реализация отдаёт big-endian по построению, общий с трейлерным путём `AwsChunkedReader`; эталон RFC 3720 → 0xE3069283 → `4waSgw==`). Если E2E SDK вскроет расхождение crc32c-пути с эталоном SDK — минимальная синхронная правка ОБЕИХ путей одной задачей (общая точка), с юнит-вектором; не откладывается (spec §3.2 «Риск CRC-конвенции»).
- Регресс-бюджет = 0: запросы без checksum-заголовков идут прежним путём кода (парсер возвращает null до создания хэшей); `UploadPart`/`CopyObject`/`DeleteObjects` checksum-заголовки не читают (спека §3.2, §6).
- Drain-семантика t36 сохраняется: отказ Storage до чтения тела не скрывает сверку; формат/мульти-отказ происходит до чтения тела (сразу 400) (spec §3.2).
- Новых env/конфигурации, внешних docker-образов, метрик-серий, изменений xl-формата и контрактов Storage — НЕТ (spec §6). Локальные образы в registry `192.168.0.1:5000` не кладутся.
- Тесты: AAA-комментарии по-русски; идентификаторы по-английски. Команды сборки/тестов — ТОЛЬКО в фоне (`run_in_background: true`), результат забирать по task_id; любые ожидания ≤ 30 с (AGENTS.base §4/§12).
- E2E-изоляция — канон `docs/e2e-isolation.md`: контур `OwnS3E2eEnvironment`/`OwnS3EcE2eEnvironment` поднимает и зачищает всё сам (динамические порты, guid-имена, ассерт чистоты); НОВЫХ docker-механик задача не вводит. После каждой docker-серии — финальная строка прогона и страховочная проверка остатков (`docker ps -a`, `docker network ls | grep os3-en`), при необходимости `docker network prune -f`. Перезапуск упавших тестов без анализа логов запрещён (AGENTS.md, телеметрия).
- Прогоны серий — по одной, не поверх друг друга и не поверх поднятого dev-стенда (runbook «Запуск docker-E2E серий»).
- Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t91-owns3-checksum-verify`. Коммиты — в feature-ветке свободно; мерж/пуш — только по явной просьбе пользователя.
- Юнит-прогон: `dotnet test src/tests/OwnS3.UnitTests -c Debug …`; WAF Api-прогон: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~OwnS3.IntegrationTests.Api …`; docker-E2E: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter …` (сборка Release — в фикстуре, инкрементальный no-op).

---

### Task 1: Коммит канона (фаза 0 — arch-правки уже в worktree)

**Вход:** worktree `feat-t91-owns3-checksum-verify`; `git status` показывает изменённые `arch/owns3/02-operations.md`, `arch/owns3/03-protocol.md`, `arch/owns3/05-access-config.md` и непtracked `docs/superpowers/2026-10-17-t91-owns3-checksum-verify/` (spec.md; plan.md появится этим шагом). Код не тронут.

**Действие/Файлы:** закоммитить arch-правки и документацию задачи как первый коммит (правки канона — часть задачи, arch-first уже исполнен спекой).

**Выход:** канон t91 зафиксирован в истории ветки; рабочее дерево чистое от arch/docs-изменений.

**Проверка:** `git -C <worktree> status --short` → пусто (после коммита); `git -C <worktree> log --oneline -1` → коммит на месте.

**Spec:** §2 arch-first, §3.1/§5.1 (таблицы правок), §4 фаза 0.

- [ ] **Шаг 1.1. Коммит канона и документации**

  - Вход: spec/plan написаны; arch-правки в дереве (проверить `git diff --stat arch/owns3/`).
  - Действие:
    ```bash
    cd /Users/demakaev/ZCodeProject/worktrees/feat-t91-owns3-checksum-verify
    git add arch/owns3/02-operations.md arch/owns3/03-protocol.md arch/owns3/05-access-config.md \
        docs/superpowers/2026-10-17-t91-owns3-checksum-verify/
    git commit -m "arch(owns3): канон t91 — заголовочные x-amz-checksum-* на PutObject (02/03) + кластер-health /s3/health/cluster (05 §5)"
    ```
  - Выход: первый коммит задачи; код ещё не менялся.
  - Проверка: `git status --short` (после коммита) — пустой вывод; `git show --stat HEAD` — три arch-файла + docs.
  - Spec: §2 «Arch-first», §4 фаза 0 «Исполнено этой спекой».

---

### Task 2: Общий helper CRC-конвенции `ChecksumConventions` (Protocol)

**Вход:** коммит Task 1 в истории; `AwsChunkedReader.Finish` содержит локальный `Reverse(_crc32!.GetCurrentHash())` (src/OwnS3.Protocol/Auth/AwsChunkedReader.cs:295, приватный `Reverse` — строки 320–324).

**Действие/Файлы:**
- Create: `src/OwnS3.Protocol/Auth/ChecksumConventions.cs`.
- Modify: `src/OwnS3.Protocol/Auth/AwsChunkedReader.cs` — crc32-ветка `Finish` переключается на helper, локальный `Reverse` удаляется.

**Выход:** единая точка байтовой конвенции crc32 (big-endian), используемая трейлерным путём; App (Task 4) будет звать тот же helper.

**Проверка:** `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~AwsChunkedReaderTests` (фон) → PASS без изменений исходов (векторы RFC 3720 и вектор 6 — регресс-сетка миграции).

**Spec:** §3.2 «Байтовая конвенция CRC — та же, что у трейлерной сверки … вынести в общий helper Protocol↔App»; §7 критерий 8.

**Interfaces (Produces):**
```csharp
namespace OwnS3.Protocol.Auth;
// big-endian байты crc32 от инкрементального состояния System.IO.Hashing.Crc32.
public static byte[] Crc32BigEndian(System.IO.Hashing.Crc32 crc);
```

- [ ] **Шаг 2.1. Helper**

  - Вход: Task 1 закоммичен.
  - Действие: создать `src/OwnS3.Protocol/Auth/ChecksumConventions.cs`:
    ```csharp
    using System.IO.Hashing;

    namespace OwnS3.Protocol.Auth;

    // Байтовая конвенция AWS контрольных сумм x-amz-checksum-* (глава 03 §3):
    // crc32 — big-endian (BCL Crc32 отдаёт little-endian — разворот); crc32c —
    // OwnS3.Protocol.Auth.Crc32C, big-endian по построению (в System.IO.Hashing
    // 10.x тип Crc32C отсутствует — своя реализация). Единая точка конвенции для
    // трейлерной (AwsChunkedReader) и заголовочной (App.HashingBodyStream) сверки
    // — пути не расходятся; фиксация — known-answer-векторы RFC 3720 и вектор 6
    // (AwsChunkedReaderTests) + литеральные векторы HashingBodyStream-тестов.
    public static class ChecksumConventions
    {
        // big-endian байты crc32 от инкрементального состояния BCL.
        public static byte[] Crc32BigEndian(Crc32 crc)
        {
            var hash = crc.GetCurrentHash();
            Array.Reverse(hash);
            return hash;
        }
    }
    ```
  - Выход: новый файл Protocol; поведение системы не изменилось (helper ещё никем не зовётся).
  - Проверка: `dotnet build src/OwnS3.Protocol/OwnS3.Protocol.csproj -c Debug` (фон) → 0 errors/warnings.
  - Spec: §3.2 (конвенция CRC), §3.1 таблица «риск CRC».

- [ ] **Шаг 2.2. Миграция AwsChunkedReader на helper**

  - Вход: helper создан (2.1).
  - Действие: в `src/OwnS3.Protocol/Auth/AwsChunkedReader.cs`:
    - строка ~295: `"x-amz-checksum-crc32" => Convert.ToHexString(Reverse(_crc32!.GetCurrentHash())),` → `"x-amz-checksum-crc32" => Convert.ToHexString(ChecksumConventions.Crc32BigEndian(_crc32!)),`;
    - удалить приватный `private static byte[] Reverse(byte[] bytes) { … }` (строки ~320–324) — единственный потребитель заменён.
  - Выход: трейлерный crc32-путь считает конвенцию в общем helper.
  - Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~AwsChunkedReaderTests` (фон) → PASS (все кейсы, включая `Checksums_KnownAnswerVectors_Rfc3720` и `Read_TrailerMode_Crc32Vector6_Ok`).
  - Spec: §7 критерий 8 (идентичность путей — общая точка конвенции).

- [ ] **Шаг 2.3. Коммит**

  - Вход: шаг 2.2 зелёный.
  - Действие:
    ```bash
    git add src/OwnS3.Protocol/Auth/ChecksumConventions.cs src/OwnS3.Protocol/Auth/AwsChunkedReader.cs
    git commit -m "feat(owns3): общий helper CRC-конвенции ChecksumConventions — трейлерный путь AwsChunkedReader переведён на него"
    ```
  - Выход: коммит в ветке.
  - Проверка: `git status --short` → пусто.
  - Spec: §3.2 (общий helper).

---

### Task 3: Парсер `ParseChecksumHeader` (TDD)

**Вход:** Task 2 закоммичен; `src/OwnS3.App/Handlers/OperationValidation.cs` — чистые статические валидации; юнит-файл `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` существует (обычный xUnit-класс без конструктора).

**Действие/Файлы:**
- Modify: `src/OwnS3.App/Handlers/OperationValidation.cs` — `enum ChecksumAlgorithm`, `record ChecksumExpectation`, `ParseChecksumHeader(IHeaderDictionary)`; `using Microsoft.AspNetCore.Http;`.
- Test: `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` — дописать секцию парсера.

**Выход:** чистая, юнит-тестируемая функция разбора семейства `x-amz-checksum-*` с каноническими отказами; используется Task 5 (PutObjectHandler).

**Проверка:** `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~HandlersValidationTests` (фон) → PASS (старые + новые кейсы).

**Spec:** §3.2 (таблица, компонент `OperationValidation.cs`); §3.1 тексты; §7 критерии 3/4.

**Interfaces (Produces):**
```csharp
namespace OwnS3.App.Handlers;

public enum ChecksumAlgorithm { Crc32, Crc32C, Sha1, Sha256 }

// Ожидание клиента: ровно один известный checksum-заголовок + его base64-значение.
public sealed record ChecksumExpectation(ChecksumAlgorithm Algorithm, string ExpectedBase64);

public static partial class OperationValidation   // существующий класс дополняется
{
    // null — заголовков семейства нет ИЛИ единственный — неизвестный алгоритм (игнор);
    // исключение S3ProtocolException(InvalidArgument) — более одного заголовка
    //   семейства («Invalid checksum provided.») либо форматно-невалидное значение
    //   («Value for x-amz-checksum-<algorithm> header is invalid.»);
    // x-amz-sdk-checksum-algorithm не читается (не входит в семейство).
    public static ChecksumExpectation? ParseChecksumHeader(IHeaderDictionary headers);
}
```

- [ ] **Шаг 3.1. Тесты парсера (красный)**

  - Вход: Task 2 закоммичен; `OperationValidation.ParseChecksumHeader` не существует.
  - Действие: дописать в конец `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` (в using-блок файла добавить `using Microsoft.AspNetCore.Http;`):
    ```csharp
    // — x-amz-checksum-* PutObject: парсер ровно одного заголовка семейства —

    private static IHeaderDictionary Headers(params (string Name, string Value)[] pairs)
    {
        var headers = new HeaderDictionary();
        foreach (var (name, value) in pairs)
            headers[name] = value;
        return headers;
    }

    // Валидное значение каждого алгоритма → ожидание с тем же алгоритмом и значением.
    [Theory]
    [InlineData("x-amz-checksum-crc32",  "crc32",  "Ox7nCg==")]                  // вектор 6: crc32(TestVectors.PutBody)
    [InlineData("x-amz-checksum-crc32c", "crc32c", "4waSgw==")]                  // эталон RFC 3720 (big-endian)
    [InlineData("x-amz-checksum-sha1",   "sha1",   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")] // sha1("")
    [InlineData("x-amz-checksum-sha256", "sha256", "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3GSuDM=")] // sha256("")
    public void ParseChecksumHeader_SingleKnown_ReturnsExpectation(string header, string algorithm, string value)
    {
        // Arrange / Act
        var expectation = OperationValidation.ParseChecksumHeader(Headers((header, value)));

        // Assert: алгоритм и base64-значение распознаны (CRC-конвенция — big-endian)
        expectation.Should().NotBeNull();
        expectation!.Algorithm.ToString().ToLowerInvariant().Should().Be(algorithm);
        expectation.ExpectedBase64.Should().Be(value);
    }

    // Не-base64 → InvalidArgument (каждый алгоритм); base64 неверной длины
    // (4/4/20/32 нарушены) → InvalidArgument (каждый алгоритм).
    [Theory]
    [InlineData("x-amz-checksum-crc32",  "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-crc32c", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha1",   "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha256", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-crc32",  "AAAA")]        // 2 байта вместо 4
    [InlineData("x-amz-checksum-crc32c", "AAAAAAAA")]    // 6 байт вместо 4
    [InlineData("x-amz-checksum-sha1",   "Ox7nCg==")]    // 4 байта вместо 20
    [InlineData("x-amz-checksum-sha256", "AAAA")]        // 2 байта вместо 32
    public void ParseChecksumHeader_InvalidFormat_ThrowsInvalidArgument(string header, string value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseChecksumHeader(Headers((header, value)));

        // Assert: формат-отказ с фактическим именем алгоритма в сообщении
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        act.Should().Throw<S3ProtocolException>()
            .Which.Message.Should().Be($"Value for {header} header is invalid.");
    }

    // Два заголовка семейства → InvalidArgument «Invalid checksum provided.» —
    // включая пару известный+неизвестный (семейство, а не известность).
    [Theory]
    [InlineData("x-amz-checksum-sha256", "x-amz-checksum-crc32")]
    [InlineData("x-amz-checksum-sha256", "x-amz-checksum-crc64nvme")]
    public void ParseChecksumHeader_MultipleFamilyHeaders_ThrowsInvalidArgument(string first, string second)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseChecksumHeader(
            Headers((first, "Ox7nCg=="), (second, "Ox7nCg==")));

        // Assert: текст ErrInvalidChecksum референса
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        act.Should().Throw<S3ProtocolException>()
            .Which.Message.Should().Be("Invalid checksum provided.");
    }

    // Единственный НЕизвестный алгоритм → null (игнор); пустой набор → null;
    // x-amz-sdk-checksum-algorithm на исход не влияет (не семейство).
    [Fact]
    public void ParseChecksumHeader_UnknownSingleOrNullSet_ReturnsNull()
    {
        // Arrange / Act / Assert: единственный crc64nvme — молчаливый игнор
        OperationValidation.ParseChecksumHeader(Headers(("x-amz-checksum-crc64nvme", "Ox7nCg==")))
            .Should().BeNull();
        // пустой набор
        OperationValidation.ParseChecksumHeader(Headers()).Should().BeNull();
        // sdk-подсказка не считается заголовком семейства и не мешает известному
        var expectation = OperationValidation.ParseChecksumHeader(
            Headers(("x-amz-sdk-checksum-algorithm", "SHA256"), ("x-amz-checksum-crc32", "Ox7nCg==")));
        expectation!.Algorithm.Should().Be(ChecksumAlgorithm.Crc32);
        expectation.ExpectedBase64.Should().Be("Ox7nCg==");
    }
    ```
  - Выход: новые кейсы не компилируются/падают — парсера нет.
  - Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~HandlersValidationTests` (фон) → FAIL/CE «ParseChecksumHeader/ChecksumExpectation/ChecksumAlgorithm не существует» — это ожидаемый красный.
  - Spec: §3.2 (правила парсера), §3.1 (тексты), §7 критерий 3/4.

- [ ] **Шаг 3.2. Реализация парсера**

  - Вход: красные тесты 3.1.
  - Действие: в `src/OwnS3.App/Handlers/OperationValidation.cs` — добавить `using Microsoft.AspNetCore.Http;` и в конец класса:
    ```csharp
    // Wire-имена алгоритмов набора x-amz-checksum-* (симметрия трейлерам, глава 03 §3).
    private static readonly IReadOnlyDictionary<string, ChecksumAlgorithm> KnownChecksumAlgorithms =
        new Dictionary<string, ChecksumAlgorithm>(StringComparer.Ordinal)
        {
            ["crc32"] = ChecksumAlgorithm.Crc32,
            ["crc32c"] = ChecksumAlgorithm.Crc32C,
            ["sha1"] = ChecksumAlgorithm.Sha1,
            ["sha256"] = ChecksumAlgorithm.Sha256,
        };

    // x-amz-checksum-<alg> (глава 02 PutObject): опционален, не более одного
    // на запрос. Возвращает null при отсутствии ИЛИ единственном неизвестном
    // алгоритме (игнор); более одного заголовка семейства (включая неизвестные
    // алгоритмы) → InvalidArgument «Invalid checksum provided.»; известный —
    // валидация base64 и длины по алгоритму (crc32/crc32c — 4 байта, sha1 — 20,
    // sha256 — 32) → ChecksumExpectation, иначе InvalidArgument с фактическим
    // именем алгоритма. x-amz-sdk-checksum-algorithm не читается.
    public static ChecksumExpectation? ParseChecksumHeader(IHeaderDictionary headers)
    {
        const string prefix = "x-amz-checksum-";
        string? knownWireName = null, knownValue = null;
        var familyCount = 0;
        foreach (var (name, values) in headers)
        {
            if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            familyCount++;
            var wireName = name[prefix.Length..].ToLowerInvariant();
            if (KnownChecksumAlgorithms.ContainsKey(wireName))
            {
                knownWireName = wireName;
                knownValue = values.ToString();
            }
        }

        if (familyCount == 0)
            return null;
        if (familyCount > 1)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Invalid checksum provided.");
        if (knownWireName is null)
            return null;   // единственный заголовок семейства — неизвестный алгоритм: игнор

        var expectedLength = knownWireName is "crc32" or "crc32c" ? 4
            : knownWireName == "sha1" ? 20 : 32;
        Span<byte> decoded = stackalloc byte[32];
        if (!Convert.TryFromBase64String(knownValue, decoded, out var written) || written != expectedLength)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument,
                $"Value for x-amz-checksum-{knownWireName} header is invalid.");
        return new ChecksumExpectation(KnownChecksumAlgorithms[knownWireName], knownValue);
    }
    ```
    И в тот же файл (namespace-уровень, рядом с классом):
    ```csharp
    // Алгоритм заголовочной контрольной суммы x-amz-checksum-<alg> (глава 02).
    public enum ChecksumAlgorithm { Crc32, Crc32C, Sha1, Sha256 }

    // Ожидание клиента: ровно один известный checksum-заголовок + его base64-значение.
    public sealed record ChecksumExpectation(ChecksumAlgorithm Algorithm, string ExpectedBase64);
    ```
  - Выход: парсер реализован по контракту §3.2.
  - Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~HandlersValidationTests` (фон) → PASS (все кейсы 3.1 + прежние).
  - Spec: §3.2 таблица (компонент OperationValidation.cs), §3.1 тексты, §7 критерий 3/4.

- [ ] **Шаг 3.3. Коммит**

  - Вход: 3.2 зелёный.
  - Действие:
    ```bash
    git add src/OwnS3.App/Handlers/OperationValidation.cs src/tests/OwnS3.UnitTests/HandlersValidationTests.cs
    git commit -m "feat(owns3): парсер x-amz-checksum-* — ChecksumExpectation + ParseChecksumHeader (один заголовок семейства, base64/длина по алгоритму, InvalidArgument-тексты канона)"
    ```
  - Выход: коммит.
  - Проверка: `git status --short` → пусто.
  - Spec: §3.2.

---

### Task 4: Сверка в `HashingBodyStream` (TDD)

**Вход:** Task 3 закоммичен (`ChecksumExpectation` доступен); `HashingBodyStream` считает SHA-256 (всегда) и MD5 (при Content-MD5), сверяя их в `VerifyHashes()`; `src/OwnS3.App/OwnS3.App.csproj` НЕ ссылается на `System.IO.Hashing`.

**Действие/Файлы:**
- Modify: `src/OwnS3.App/OwnS3.App.csproj` — `<PackageReference Include="System.IO.Hashing"/>` (без версии, CPM; нужен для `Crc32` — `Crc32C` берётся из `OwnS3.Protocol`, пакет для него не нужен).
- Modify: `src/OwnS3.App/Handlers/HashingBodyStream.cs` — аргумент `checksum`, вычисление выбранного алгоритма в точках `AppendData`, сверка в `VerifyHashes` между sha256-hex и MD5.
- Test: Create `src/tests/OwnS3.UnitTests/App/HashingBodyStreamChecksumTests.cs`.

**Выход:** обёртка тела вычисляет ровно один выбранный алгоритм поверх единственного прохода чтения и отдаёт `BadDigest` с каноническим текстом при несовпадении; существующие вызовы (`ObjectHandlers`, `MultipartHandlers`) не меняются (аргумент опционален).

**Проверка:** `dotnet test src/tests/OwnS3.UnitTests -c Debug` (фон) → PASS (новые векторы + все прежние).

**Spec:** §3.2 (компонент HashingBodyStream.cs), §2 «Ровно один хэш на запрос», §7 критерии 1/2/8.

**Interfaces (Produces):**
```csharp
// Новый конструкторский аргумент (остальные три — прежние, значение по умолчанию
// сохраняет все существующие вызовы):
public HashingBodyStream(Stream inner, string? expectedSha256Hex, string? expectedMd5Base64,
    ChecksumExpectation? checksum = null);
// Поведение: Sha256 → переиспользуется существующий _sha256 (второго инстанса нет);
// Crc32 → System.IO.Hashing Crc32 + ChecksumConventions.Crc32BigEndian;
// Crc32C → OwnS3.Protocol.Auth.Crc32C (собственный тип, big-endian по построению;
//          в System.IO.Hashing 10.x тип Crc32C отсутствует);
// Sha1 → IncrementalHash(SHA1).
// VerifyHashes: sha256-hex (InvalidRequest) → checksum (BadDigest) → MD5 (BadDigest).
```

- [ ] **Шаг 4.1. Юнит-векторы (красный)**

  - Вход: Task 3 в истории; тест-файла нет.
  - Действие: создать `src/tests/OwnS3.UnitTests/App/HashingBodyStreamChecksumTests.cs`:
    ```csharp
    using System.IO.Hashing;
    using System.Security.Cryptography;
    using System.Text;
    using OwnS3.App.Handlers;
    using OwnS3.Protocol.Auth;
    using OwnS3.Protocol.Errors;

    namespace OwnS3.UnitTests.App;

    // Сверка заголовочной контрольной суммы в HashingBodyStream (глава 02 PutObject):
    // вычисление ровно выбранного алгоритма за один проход, сверка на EOF в порядке
    // sha256-подпись → checksum → MD5. Векторы: crc32/crc32c — литеральные эталоны
    // «123456789» (ISO-HDLC 0xCBF43926 big-endian / RFC 3720 0xE3069283); sha1/sha256 —
    // BCL (тестируется сборка и конвенция, не BCL); crc32-вектор 6 трейлерного пути —
    // та же константа, что у AwsChunkedReader (идентичность путей, критерий 8).
    // Типы: crc32 — System.IO.Hashing.Crc32 (little-endian → helper разворота),
    // crc32c — OwnS3.Protocol.Auth.Crc32C (big-endian по построению).
    public sealed class HashingBodyStreamChecksumTests
    {
        private static readonly byte[] Data = Encoding.ASCII.GetBytes("123456789");

        // Эталонная сумма по алгоритму; CRC-конвенция — та же, что у трейлерного
        // пути: crc32 — big-endian (общий helper), crc32c — OwnS3.Protocol.Auth.Crc32C.
        private static string ChecksumOf(ChecksumAlgorithm algorithm, byte[] data)
        {
            switch (algorithm)
            {
                case ChecksumAlgorithm.Crc32:
                    var crc32 = new Crc32();
                    crc32.Append(data);
                    return Convert.ToBase64String(ChecksumConventions.Crc32BigEndian(crc32));
                case ChecksumAlgorithm.Crc32C:
                    var crc32c = new Crc32C();
                    crc32c.Append(data);
                    return Convert.ToBase64String(crc32c.GetCurrentHash());
                case ChecksumAlgorithm.Sha1:
                    return Convert.ToBase64String(SHA1.HashData(data));
                default:
                    return Convert.ToBase64String(SHA256.HashData(data));
            }
        }

        // Чтение до EOF — сверка выполняется на нулевом Read (конструктор 3 аргумента
        // + checksum). Дочитывание обязательным элементом кейса.
        private static void ReadAll(ChecksumAlgorithm algorithm, string expected)
        {
            using var stream = new HashingBodyStream(new MemoryStream(Data), null, null,
                new ChecksumExpectation(algorithm, expected));
            var buffer = new byte[8];
            while (stream.Read(buffer, 0, buffer.Length) > 0) { }
        }

        // AAA: корректная сумма каждого алгоритма — сверка проходит (EOF без исключения);
        // crc32/crc32c сверены против ЛИТЕРАЛЬНЫХ эталонов (фиксация конвенции).
        [Theory]
        [InlineData(ChecksumAlgorithm.Crc32,  "y/Q5Jg==")] // 0xCBF43926 big-endian
        [InlineData(ChecksumAlgorithm.Crc32C, "4waSgw==")] // 0xE3069283
        public void Verify_CorrectChecksum_Passes(ChecksumAlgorithm algorithm, string expected)
        {
            // Arrange / Act / Assert
            ChecksumOf(algorithm, Data).Should().Be(expected, "независимый эталон конвенции");
            var act = () => ReadAll(algorithm, expected);
            act.Should().NotThrow();
        }

        // AAA: sha1/sha256 — BCL-эталон (конвенция sha — нативный порядок байтов BCL).
        [Theory]
        [InlineData(ChecksumAlgorithm.Sha1)]
        [InlineData(ChecksumAlgorithm.Sha256)]
        public void Verify_CorrectShaChecksum_Passes(ChecksumAlgorithm algorithm)
        {
            // Arrange / Act / Assert
            var act = () => ReadAll(algorithm, ChecksumOf(algorithm, Data));
            act.Should().NotThrow();
        }

        // AAA: битое значение (валидный base64 корректной длины, чужая сумма) — BadDigest
        // с текстом трейлерной сверки.
        [Theory]
        [InlineData(ChecksumAlgorithm.Crc32)]
        [InlineData(ChecksumAlgorithm.Crc32C)]
        [InlineData(ChecksumAlgorithm.Sha1)]
        [InlineData(ChecksumAlgorithm.Sha256)]
        public void Verify_CorruptedChecksum_ThrowsBadDigest(ChecksumAlgorithm algorithm)
        {
            // Arrange: сумма ДРУГОГО тела той же длины
            var wrong = ChecksumOf(algorithm, "other"u8.ToArray());

            // Act
            var act = () => ReadAll(algorithm, wrong);

            // Assert
            act.Should().Throw<S3ProtocolException>()
                .Which.Code.Should().Be(S3ErrorCode.BadDigest);
            act.Should().Throw<S3ProtocolException>()
                .Which.Message.Should().Be("The checksum you specified did not match the calculated checksum.");
        }

        // AAA: пустое тело + корректная сумма пустоты — сверка проходит на первом же Read.
        [Theory]
        [InlineData(ChecksumAlgorithm.Crc32,  "AAAAAA==")]
        [InlineData(ChecksumAlgorithm.Crc32C, "AAAAAA==")]
        [InlineData(ChecksumAlgorithm.Sha1,   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")]
        [InlineData(ChecksumAlgorithm.Sha256, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3GSuDM=")]
        public void Verify_EmptyBody_EmptyChecksum_Passes(ChecksumAlgorithm algorithm, string emptySum)
        {
            // Arrange
            using var stream = new HashingBodyStream(new MemoryStream(), null, null,
                new ChecksumExpectation(algorithm, emptySum));

            // Act / Assert: первый Read — 0 → VerifyHashes без исключения
            stream.Read(new byte[1], 0, 1).Should().Be(0);
        }

        // AAA: порядок сверк — битая sha256-подпись И корректный checksum → InvalidRequest
        // (подпись первее).
        [Fact]
        public void Verify_Sha256SignatureCheckedFirst()
        {
            // Arrange: подпись от другого тела (64 hex), checksum корректный
            var wrongPayload = Convert.ToHexString(SHA256.HashData("other"u8.ToArray())).ToLowerInvariant();
            var correctCrc32 = "y/Q5Jg==";

            // Act
            var act = () =>
            {
                using var stream = new HashingBodyStream(new MemoryStream(Data), wrongPayload, null,
                    new ChecksumExpectation(ChecksumAlgorithm.Crc32, correctCrc32));
                var buffer = new byte[8];
                while (stream.Read(buffer, 0, buffer.Length) > 0) { }
            };

            // Assert: InvalidRequest перекрывает — сверка подписи стоит первой
            act.Should().Throw<S3ProtocolException>()
                .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
        }
    }
    ```
  - Выход: тесты не компилируются — у `HashingBodyStream` нет 4-го аргумента.
  - Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~HashingBodyStreamChecksumTests` (фон) → CE/FAIL «no constructor takes 4 arguments» — ожидаемый красный.
  - Spec: §3.2 (HashingBodyStream), §7 критерий 1/2/8, §3.1 (BadDigest-текст).

- [ ] **Шаг 4.2. Реализация сверки в потоке**

  - Вход: красные тесты 4.1.
  - Действие: `src/OwnS3.App/OwnS3.App.csproj` — добавить в ItemGroup с ProjectReference:
    ```xml
    <PackageReference Include="System.IO.Hashing"/>
    ```
    `src/OwnS3.App/Handlers/HashingBodyStream.cs` — добавить `using System.IO.Hashing;` и `using OwnS3.Protocol.Auth;`; поля и конструктор:
    ```csharp
    private readonly ChecksumExpectation? _checksum;
    private readonly Crc32? _checksumCrc32;          // алгоритм Crc32 (System.IO.Hashing)
    private readonly Crc32C? _checksumCrc32c;        // алгоритм Crc32C (OwnS3.Protocol.Auth)
    private readonly IncrementalHash? _checksumSha1; // алгоритм Sha1 (Sha256 — существующий _sha256)

    public HashingBodyStream(Stream inner, string? expectedSha256Hex, string? expectedMd5Base64,
        ChecksumExpectation? checksum = null)
    {
        _inner = inner;
        _expectedSha256Hex = expectedSha256Hex;
        _expectedMd5Base64 = expectedMd5Base64;
        _md5 = expectedMd5Base64 is null ? null : IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        // Ровно один инкрементальный хэш поверх прохода чтения; Sha256 — переиспользование
        // уже существующего _sha256 (второй инстанс не создаётся).
        _checksum = checksum;
        if (checksum is not null)
            switch (checksum.Algorithm)
            {
                case ChecksumAlgorithm.Crc32: _checksumCrc32 = new(); break;
                case ChecksumAlgorithm.Crc32C: _checksumCrc32c = new(); break;
                case ChecksumAlgorithm.Sha1: _checksumSha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1); break;
            }
    }
    ```
    В обоих `Read`/`ReadAsync` после `_md5?.AppendData(...)`:
    ```csharp
    _checksumCrc32?.Append(buffer.AsSpan(offset, read));
    _checksumCrc32c?.Append(buffer.AsSpan(offset, read));
    _checksumSha1?.AppendData(buffer, offset, read);
    ```
    В `VerifyHashes()` — блок checksum МЕЖДУ sha256-hex-сверкой и MD5-сверкой:
    ```csharp
    if (_checksum is not null)
    {
        var actualChecksum = _checksum.Algorithm switch
        {
            ChecksumAlgorithm.Sha256 => Convert.ToBase64String(_sha256.GetCurrentHash()),
            ChecksumAlgorithm.Crc32 => Convert.ToBase64String(
                ChecksumConventions.Crc32BigEndian(_checksumCrc32!)),
            ChecksumAlgorithm.Crc32C => Convert.ToBase64String(_checksumCrc32c!.GetCurrentHash()),
            _ => Convert.ToBase64String(_checksumSha1!.GetCurrentHash()),
        };
        if (!string.Equals(actualChecksum, _checksum.ExpectedBase64, StringComparison.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.BadDigest,
                "The checksum you specified did not match the calculated checksum.");
    }
    ```
    В `Dispose(bool disposing)` — `_checksumSha1?.Dispose();` рядом с базовой очисткой (`Crc32`/`Crc32C` не IDisposable).
  - Выход: сверка реализована; существующие 3-аргументные вызовы (`ObjectHandlers` ×2, `MultipartHandlers` ×1) компилируются без правок.
  - Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug` (фон) → PASS: новые векторы + ВСЕ прежние юниты (регресс-сетка: AwsChunkedReaderTests, HandlersValidationTests и пр.).
  - Spec: §3.2 (компонент HashingBodyStream), §2 «Ровно один хэш на запрос», «Сверка в существующем конвейере», §7 критерий 1/2/8.

- [ ] **Шаг 4.3. Коммит**

  - Вход: 4.2 зелёный (полный юнит-прогон).
  - Действие:
    ```bash
    git add src/OwnS3.App/OwnS3.App.csproj src/OwnS3.App/Handlers/HashingBodyStream.cs \
        src/tests/OwnS3.UnitTests/App/HashingBodyStreamChecksumTests.cs
    git commit -m "feat(owns3): сверка заголовочного x-amz-checksum-* в HashingBodyStream — один выбранный алгоритм за проход, BadDigest на EOF, порядок sha256→checksum→MD5"
    ```
  - Выход: коммит.
  - Проверка: `git status --short` → пусто.
  - Spec: §3.2/§3.3 (юнит-векторы).

---

### Task 5: Подключение в `PutObjectHandler` + WAF-сценарии (а)–(и2)

**Вход:** Tasks 2–4 в истории; `PutObjectHandler` строит тело (ObjectHandlers.cs:36–41) без чтения checksum-заголовков; WAF-серия `BodyIntegrityScenariosBase` (+single-класс `BodyIntegrityScenarios`, +EC-наследник `BodyIntegrityEcScenarios` в ErasureContractScenarios.cs) уже параметризована t40 — новые кейсы базы автоматически исполняются в обеих параметризациях; `ReplicationAppFactory : OwnS3AppFactory` (ReplicationScenarios.cs:14) задаёт mesh-соседей с недоступным endpoint — успешные клиентские put ставят события object-put в outbox-журнал (образец чтения — `ReplicationEventsScenarios.ReadOutbox`, формат v1 `OutboxEntryCodec`).

**Действие/Файлы:**
- Modify: `src/OwnS3.App/Handlers/ObjectHandlers.cs` (`PutObjectHandler.HandleAsync`, строки ~35–41).
- Modify: `src/tests/OwnS3.IntegrationTests/Api/BodyIntegrityScenarios.cs` — кейсы (а)–(и) в `BodyIntegrityScenariosBase` + класс (и2) на `ReplicationAppFactory`.

**Выход:** PutObject сверяет заголовочные checksum в НЕ-чанковых режимах; чанковые и прочие операции — прежнее поведение; WAF-покрытие в single и EC; событие репликации не отправляется при непрошедшей сверке (ассерт outbox).

**Проверка:** `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~BodyIntegrity` (фон) → PASS в обеих параметризациях + репликационный класс (фильтр матчит `BodyIntegrityScenarios`, `BodyIntegrityEcScenarios`, `BodyIntegrityReplicationScenarios`).

**Spec:** §3.2 (PutObjectHandler, прочие хендлеры), §3.3 (кейсы (а)–(з)), §7 критерии 1–7 (в т.ч. критерий 2 — «событие репликации не отправлялось»).

- [ ] **Шаг 5.1. Вызов парсера в PutObjectHandler**

  - Вход: Task 4 закоммичен.
  - Действие: в `src/OwnS3.App/Handlers/ObjectHandlers.cs` заменить блок построения тела (строки ~35–41):
    ```csharp
    // Тело: чанковый режим → AwsChunkedReader (трейлер — авторитет, заголовочные
    // x-amz-checksum-* игнорируются); hex-sha256/Content-MD5/checksum → HashingBodyStream.
    Stream body = context.Http.Body;
    ChecksumExpectation? checksum = null;
    if (context.TryGetChunkedContext(out var chunked))
        body = new AwsChunkedReader(body, chunked!);
    else
        checksum = OperationValidation.ParseChecksumHeader(context.Http.Headers);
    body = new HashingBodyStream(body,
        Sha256HexOrNull(context),
        context.Http.Headers.ContentMD5.ToString() is { Length: > 0 } md5 ? md5 : null,
        checksum);
    ```
    Отказ формата/мульти бросается парсером ДО `Store.PutObjectAsync` — тело не читается (сразу 400).
  - Выход: конвейер PutObject подключён; чанковая ветка не вызывает парсер.
  - Проверка: `dotnet build src/OwnS3.App/OwnS3.App.csproj -c Debug` (фон) → 0 errors.
  - Spec: §3.2 (ObjectHandlers.cs), §3.1 таблица режимов (чанковые — игнор заголовков).

- [ ] **Шаг 5.2. WAF-кейсы (а)–(и2)**

  - Вход: 5.1 собран.
  - Действие: дописать в `BodyIntegrityScenariosBase` (файл `src/tests/OwnS3.IntegrationTests/Api/BodyIntegrityScenarios.cs`; в using-блок файла добавить `using OwnS3.Replication;` и `using OwnS3.Replication.Outbox;` — для кейса (и2); уже есть: `System.IO.Hashing`, `System.Security.Cryptography`, `OwnS3.Protocol.Auth`) хелпер и кейсы:
    ```csharp
    // Контрольная сумма тела по алгоритму (конвенция: crc32 — big-endian от BCL
    // Crc32; crc32c — OwnS3.Protocol.Auth.Crc32C, big-endian).
    private static string ChecksumOf(string algorithm, byte[] data) => algorithm switch
    {
        "crc32" => Convert.ToBase64String(Crc32.Hash(data).Reverse().ToArray()),
        "crc32c" => Convert.ToBase64String(Crc32C.Hash(data)),
        "sha1" => Convert.ToBase64String(SHA1.HashData(data)),
        "sha256" => Convert.ToBase64String(SHA256.HashData(data)),
        _ => throw new ArgumentOutOfRangeException(algorithm),
    };

    // (а) корректный checksum при UNSIGNED-PAYLOAD — единственная серверная сверка
    // тела прошла, запрос дошёл до Storage (NoSuchBucket-шаблон серии).
    [Theory]
    [InlineData("crc32")]
    [InlineData("crc32c")]
    [InlineData("sha1")]
    [InlineData("sha256")]
    public async Task PutObject_CorrectChecksum_UnsignedPayload_ReachesStorage(string algorithm)
    {
        // Arrange: UNSIGNED-PAYLOAD (дефолт клиента) + корректная сумма тела
        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = ChecksumOf(algorithm, Body) },
            body: Body);

        // Assert: сверка прошла — Storage отказал по бакету
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (б) порча checksum → 400 BadDigest (каждый алгоритм).
    [Theory]
    [InlineData("crc32")]
    [InlineData("crc32c")]
    [InlineData("sha1")]
    [InlineData("sha256")]
    public async Task PutObject_CorruptedChecksum_400BadDigest(string algorithm)
    {
        // Arrange: сумма ДРУГОГО тела
        var wrong = ChecksumOf(algorithm, "other body"u8.ToArray());

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = wrong },
            body: Body);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("BadDigest");
    }

    // (в) форматно-невалидный (не base64 / неверная длина) → 400 InvalidArgument.
    [Theory]
    [InlineData("x-amz-checksum-crc32", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha256", "AAAA")]
    public async Task PutObject_InvalidChecksumFormat_400InvalidArgument(string header, string value)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [header] = value }, body: Body);

        // Assert: отказ ДО чтения тела — Storage не звался, канонический код
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidArgument");
    }

    // (г) hex-sha256 подпись + sha256-checksum — обе сверки независимо:
    // г1 — обе корректны; г2 — подпись корректна, checksum битой; г3 — подпись битая,
    // checksum корректен (InvalidRequest первее).
    [Fact]
    public async Task PutObject_Sha256SignatureAndChecksum_IndependentChecks()
    {
        var client = NewClient();
        var payload = Convert.ToHexString(SHA256.HashData(Body)).ToLowerInvariant();
        var goodChecksum = Convert.ToBase64String(SHA256.HashData(Body));
        var badChecksum = Convert.ToBase64String(SHA256.HashData("other body"u8.ToArray()));

        // Act / Assert (г1): обе сверки прошли — до Storage
        (await client.SendSignedAsync("PUT", "/bucket/key", payloadString: payload,
                headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = goodChecksum }, body: Body))
            .StatusCode.Should().Be(HttpStatusCode.NotFound);

        // Act / Assert (г2): подпись корректна, checksum битой → BadDigest
        var corrupted = await client.SendSignedAsync("PUT", "/bucket/key", payloadString: payload,
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = badChecksum }, body: Body);
        corrupted.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(corrupted)).Should().Be("BadDigest");

        // Act / Assert (г3): подпись битая, checksum корректен → InvalidRequest (порядок)
        var brokenSignature = Convert.ToHexString(SHA256.HashData("other body"u8.ToArray())).ToLowerInvariant();
        var invalidRequest = await client.SendSignedAsync("PUT", "/bucket/key", payloadString: brokenSignature,
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = goodChecksum }, body: Body);
        invalidRequest.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(invalidRequest)).Should().Be("InvalidRequest");
    }

    // (д) два checksum-заголовка → 400 InvalidArgument.
    [Fact]
    public async Task PutObject_TwoChecksumHeaders_400InvalidArgument()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string>
            {
                ["x-amz-checksum-sha256"] = Convert.ToBase64String(SHA256.HashData(Body)),
                ["x-amz-checksum-crc32"] = ChecksumOf("crc32", Body),
            }, body: Body);

        // Assert: «Invalid checksum provided.» (семейство, включая разные алгоритмы)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidArgument");
    }

    // (е) пустое тело + корректная сумма пустоты — сверка проходит на EOF первого Read.
    [Theory]
    [InlineData("crc32",  "AAAAAA==")]
    [InlineData("crc32c", "AAAAAA==")]
    [InlineData("sha1",   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")]
    [InlineData("sha256", "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3GSuDM=")]
    public async Task PutObject_EmptyBody_EmptyChecksum_ReachesStorage(string algorithm, string emptySum)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = emptySum },
            body: []);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (ж) чанковый trailer-режим с заголовочным checksum: заголовок игнорируется
    // (авторитет — трейлер); валидный трейлер + БИТОЙ заголовок → запрос доходит до Storage.
    [Fact]
    public async Task ChunkedPut_TrailerMode_HeaderChecksumIgnored()
    {
        // Arrange: чанковое тело с ВАЛИДНЫМ трейлером crc32 + битой заголовочной суммой
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
        var payload = PayloadHashModeClassifier.StreamingTrailerValue;
        var host = client.Http.BaseAddress!.Authority;
        var goodTrailer = Crc32.Hash(Body).Reverse().ToArray();
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", Body.Length.ToString()),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
            ("x-amz-checksum-crc32", "AAAAAA=="), // битая заголовочная — обязана игнорироваться
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT", "/bucket/key",
            string.Empty, headers, payload, date, "us-east-1");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, Body, signature, date,
            "us-east-1", trailerName: "x-amz-checksum-crc32", trailerChecksum: goodTrailer);

        using var request = new HttpRequestMessage(HttpMethod.Put, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("x-amz-trailer", "x-amz-checksum-crc32");
        request.Headers.TryAddWithoutValidation("x-amz-checksum-crc32", "AAAAAA==");
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-checksum-crc32", "x-amz-content-sha256", "x-amz-date",
                 "x-amz-decoded-content-length", "x-amz-trailer"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", Body.Length.ToString());
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: трейлерная сверка прошла, заголовок проигнорирован — до Storage
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (з) UploadPart с checksum-заголовком — прежнее поведение (заголовок не читается).
    [Fact]
    public async Task UploadPart_ChecksumHeader_Ignored()
    {
        // Arrange: битая сумма на part-запросе — не 400, прежний исход Storage
        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            headers: new Dictionary<string, string>
            {
                ["x-amz-checksum-sha256"] = Convert.ToBase64String(SHA256.HashData("other"u8.ToArray())),
            }, body: Encoding.UTF8.GetBytes("part"));

        // Assert: NoSuchBucket от Storage — хендлер части checksum-заголовки не читает
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (и) коммит-семантика на живом бакете: корректный checksum → 200 + ETag hex-MD5
    // (не зависит от алгоритма); битой → 400 BadDigest и объект НЕ закоммичен
    // (GetObject → NoSuchKey; событие репликации — ассерт кейса (и2) ниже).
    [Fact]
    public async Task PutObject_Checksum_CommitSemanticsAndEtag()
    {
        // Arrange: бакет кейса (admin)
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/b-ci-chk", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var md5Hex = Convert.ToHexString(MD5.HashData(Body)).ToLowerInvariant();

        // Act / Assert: crc32-put → 200, ETag = MD5
        var put = await client.SendSignedAsync("PUT", "/b-ci-chk/ok.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = ChecksumOf("crc32", Body) },
            body: Body);
        put.StatusCode.Should().Be(HttpStatusCode.OK);
        put.Headers.ETag!.Tag.Should().Be($"\"{md5Hex}\"", "ETag — hex-MD5, от алгоритма checksum не зависит");

        // Act / Assert: sha256-put того же тела — тот же ETag (независимость)
        var put2 = await client.SendSignedAsync("PUT", "/b-ci-chk/ok2.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = ChecksumOf("sha256", Body) },
            body: Body);
        put2.Headers.ETag!.Tag.Should().Be($"\"{md5Hex}\"");

        // Act / Assert: битая сумма → 400 BadDigest, объекта нет (NoSuchKey)
        var broken = await client.SendSignedAsync("PUT", "/b-ci-chk/broken.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = ChecksumOf("crc32", "other"u8.ToArray()) },
            body: Body);
        broken.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(broken)).Should().Be("BadDigest");
        var get = await client.SendSignedAsync("GET", "/b-ci-chk/broken.bin");
        get.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(get)).Should().Be("NoSuchKey");
    }
    ```
    И в конец ФАЙЛА `BodyIntegrityScenarios.cs` (namespace-уровень, рядом с single-классом `BodyIntegrityScenarios`) — класс кейса (и2):
    ```csharp
    // (и2) критерий 2 «событие репликации не отправлялось»: ассерт на фабрике
    // С соседями (ReplicationAppFactory — успешные put ставят события object-put
    // в outbox; на базовой фабрике без соседей журнал не ведётся вовсе — там ассерт
    // пустоты был бы тривиально-зелёным, см. ReplicationEventsScenarios.
    // NoNeighbors_NoOutboxDirectory). Успешный checksum-put → событие есть;
    // битой → события нет (сверка до коммит-поинта).
    public sealed class BodyIntegrityReplicationScenarios(ReplicationAppFactory factory)
        : IClassFixture<ReplicationAppFactory>
    {
        private OwnS3TestClient NewClient() => new(factory.CreateClient());

        private string OutboxDir => Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication", "outbox");

        // Декод всех событий журнала (бинарный формат v1, глава 07 §4) — образец
        // ReplicationEventsScenarios.ReadOutbox.
        private static List<ReplicationEvent> ReadOutbox(string outboxDir)
        {
            var events = new List<ReplicationEvent>();
            if (!Directory.Exists(outboxDir))
                return events;
            foreach (var segment in Directory.GetFiles(outboxDir, "seg-*.log").OrderBy(f => f))
            {
                var bytes = File.ReadAllBytes(segment);
                var offset = 0;
                while (offset < bytes.Length)
                {
                    if (!OutboxEntryCodec.TryDecode(bytes, offset, out var e, out offset))
                        break;
                    events.Add(e);
                }
            }
            return events;
        }

        private static string Crc32Of(byte[] data) =>
            Convert.ToBase64String(System.IO.Hashing.Crc32.Hash(data).Reverse().ToArray());

        [Fact]
        public async Task PutObject_BadChecksum_NoReplicationEvent()
        {
            // Arrange: сосед задан (клиентские put ставляют события); бакет кейса
            var client = NewClient();
            (await client.SendSignedAsync("PUT", "/b-ci-repl", credentials: OwnS3TestClient.Admin()))
                .StatusCode.Should().Be(HttpStatusCode.OK);
            var body = Encoding.UTF8.GetBytes("hello world");

            // Act: успешный put с корректным crc32 → 200
            (await client.SendSignedAsync("PUT", "/b-ci-repl/ok.bin",
                headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = Crc32Of(body) },
                body: body)).StatusCode.Should().Be(HttpStatusCode.OK);

            // Assert: коммит-поинт пройден — событие object-put поставлено
            ReadOutbox(OutboxDir).Should().Contain(e =>
                e.Type == ReplicationEventType.ObjectPut && e.Bucket == "b-ci-repl" && e.Key == "ok.bin");

            // Act: битой put → 400 BadDigest
            var broken = await client.SendSignedAsync("PUT", "/b-ci-repl/broken.bin",
                headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = Crc32Of("other"u8.ToArray()) },
                body: body);
            broken.StatusCode.Should().Be(HttpStatusCode.BadRequest);
            (await RoutingScenarios.ErrorXmlAsync(broken)).Should().Be("BadDigest");

            // Assert: сверка ДО коммит-поинта — события для broken.bin НЕТ
            ReadOutbox(OutboxDir).Should().NotContain(e =>
                e.Type == ReplicationEventType.ObjectPut && e.Bucket == "b-ci-repl" && e.Key == "broken.bin");
        }
    }
    ```
  - Выход: кейсы (а)–(и) в базе (single+EC автоматически) + класс (и2) на репликационной фабрике.
  - Проверка: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~BodyIntegrity` (фон) → PASS (single + EC + репликационный класс, включая прежние кейсы серии).
  - Spec: §3.3 (интеграционные WAF, кейсы (а)–(з)), §7 критерии 1/2/3/5/6/7 (критерий 2 — событие репликации: кейс (и2)).

- [ ] **Шаг 5.3. Коммит**

  - Вход: 5.2 зелёный во всех трёх классах серии.
  - Действие:
    ```bash
    git add src/OwnS3.App/Handlers/ObjectHandlers.cs src/tests/OwnS3.IntegrationTests/Api/BodyIntegrityScenarios.cs
    git commit -m "feat(owns3): PutObject сверяет заголовочный x-amz-checksum-* (не-чанковые режимы) + WAF-кейсы (а)–(и2) single/EC/outbox-событийность"
    ```
  - Выход: коммит.
  - Проверка: `git status --short` → пусто.
  - Spec: §3.2/§3.3, §7 критерий 2.

---

### Task 6: E2E реальными клиентами AWS SDK (все четыре алгоритма)

**Вход:** Task 5 закоммичен; E2E-класс `AwsSdkSimpleScenarios` (коллекция `OwnS3E2eCollection`, текущие using: `System.Net; Amazon.S3; Amazon.S3.Model; FluentAssertions;`) использует `OwnS3E2eEnvironment.RunScenarioAsync(<slug>, …)` с последними slug s1–s10; клиент-фабрика `OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer)` — режим `WhenRequired` (заголовки SDK НЕ шлёт — значение задаём свойством запроса).

**Действие/Файлы:**
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/AwsSdkSimpleScenarios.cs` — using-блок + метод `ChecksumHeader_Put_VerifyAndReject` (slug `s11`).

**Выход:** SDK-приёмка всех четырёх алгоритмов (валидный → 200 + объект читается + ETag-MD5; битый → `AmazonS3Exception` 400/`BadDigest`); маркер-кейс мерж-гейта.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~ChecksumHeader` (фон, docker доступен) → PASS.

**Spec:** §3.3 (E2E), §4 фаза 4, §7 критерии 1/2/8; риск CRC-конвенции §3.2.

- [ ] **Шаг 6.1. E2E-сценарий s11**

  - Вход: Task 5 в истории; docker на хосте доступен (иначе Skip-гейт).
  - Действие: в `src/tests/OwnS3.IntegrationTests/E2e/AwsSdkSimpleScenarios.cs` ДОБАВИТЬ в using-блок (обязательно все три — `Crc32C` в System.IO.Hashing 10.x отсутствует, это собственный тип Protocol; `SHA1/SHA256` требуют System.Security.Cryptography, ImplicitUsings его не даёт):
    ```csharp
    using System.IO.Hashing;             // Crc32
    using System.Security.Cryptography;  // SHA1/SHA256
    using OwnS3.Protocol.Auth;           // Crc32C (в System.IO.Hashing 10.x типа нет)
    ```
    и дописать метод в `AwsSdkSimpleScenarios`:
    ```csharp
    [Fact]
    public async Task ChecksumHeader_Put_VerifyAndReject()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: заголовочная checksum-верификация реальным SDK (канон 02 PutObject):
        // ВСЕ ЧЕТЫРЕ алгоритма — свойство запроса шлёт x-amz-checksum-<alg>;
        // валидное значение → 200 + объект читается + ETag-MD5; битое → 400
        // BadDigest от сервера. Битые значения — валидный base64 корректной длины
        // (проверяется несовпадение суммы, не формат).
        await OwnS3E2eEnvironment.RunScenarioAsync("s11", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}hc";
            var body = OwnS3E2eBytes.Deterministic(256 * 1024, 31);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Конвенция теста = конвенция сервера: crc32 big-endian (BCL + Reverse),
            // crc32c — OwnS3.Protocol.Auth.Crc32C (big-endian).
            string Sum(string algorithm) => algorithm switch
            {
                "crc32" => Convert.ToBase64String(Crc32.Hash(body).Reverse().ToArray()),
                "crc32c" => Convert.ToBase64String(Crc32C.Hash(body)),
                "sha1" => Convert.ToBase64String(SHA1.HashData(body)),
                _ => Convert.ToBase64String(SHA256.HashData(body)),
            };
            string WrongSum(string algorithm) => algorithm is "crc32" or "crc32c"
                ? "AAAAAA=="                       // 4 байта, чужое значение
                : Convert.ToBase64String(new byte[algorithm == "sha1" ? 20 : 32]);

            Task<PutObjectResponse> PutAsync(string algorithm, string checksum, string key) =>
                s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
                    ChecksumCRC32 = algorithm == "crc32" ? checksum : null,
                    ChecksumCRC32C = algorithm == "crc32c" ? checksum : null,
                    ChecksumSHA1 = algorithm == "sha1" ? checksum : null,
                    ChecksumSHA256 = algorithm == "sha256" ? checksum : null,
                }, ct);

            foreach (var algorithm in new[] { "crc32", "crc32c", "sha1", "sha256" })
            {
                // Act: валидная сумма
                var put = await PutAsync(algorithm, Sum(algorithm), $"chk-{algorithm}.bin");

                // Assert: 200 + объект читается + ETag hex-MD5 (не зависит от алгоритма)
                put.HttpStatusCode.Should().Be(HttpStatusCode.OK, "алгоритм {0}", algorithm);
                using var get = await s3.GetObjectAsync(
                    new GetObjectRequest { BucketName = bucket, Key = $"chk-{algorithm}.bin" }, ct);
                using var ms = new MemoryStream();
                await get.ResponseStream.CopyToAsync(ms, ct);
                ms.ToArray().Should().Equal(body);
                get.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));

                // Act: битая сумма (валидный base64 корректной длины)
                var rejected = await s3.Invoking(x => PutAsync(algorithm, WrongSum(algorithm), $"bad-{algorithm}.bin"))
                    .Should().ThrowAsync<AmazonS3Exception>();

                // Assert: HTTP 400, код BadDigest в теле ошибки; объект не закоммичен
                rejected.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest, "алгоритм {0}", algorithm);
                rejected.Which.ErrorCode.Should().Be("BadDigest");
                (await s3.Invoking(x => x.GetObjectMetadataAsync(
                        new GetObjectMetadataRequest { BucketName = bucket, Key = $"bad-{algorithm}.bin" }, ct))
                    .Should().ThrowAsync<AmazonS3Exception>())
                    .Which.StatusCode.Should().Be(HttpStatusCode.NotFound, "битой put не коммитит объект");
            }
        }, ct);
    }
    ```
  - Выход: SDK-кейс всех четырёх алгоритмов; фильтр `~ChecksumHeader` уникален (не задевает `ChecksumTrailer_PutGet_WhenSupported`).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~ChecksumHeader` (фон) → PASS; дождаться финальной строки; остатков контура нет (`docker ps -a | grep os3-es` → пусто после teardown'а окружения; при падении — разбор по артефактам `/tmp/pgw-e2e-artifacts-<runId>/`, перезапуск без анализа запрещён).
  - Spec: §3.3 E2E, §7 критерии 1/2/6/8; при расхождении crc32c с SDK — синхронная правка общей точки конвенции обоих путей (Global Constraints), с юнит-вектором, той же задачей.

- [ ] **Шаг 6.2. Коммит**

  - Вход: 6.1 зелёный.
  - Действие:
    ```bash
    git add src/tests/OwnS3.IntegrationTests/E2e/AwsSdkSimpleScenarios.cs
    git commit -m "test(owns3): E2E SDK-приёмка заголовочных checksum — 4 алгоритма валид/битый (маркер ChecksumHeader), сценарий s11"
    ```
  - Выход: коммит.
  - Проверка: `git status --short` → пусто.
  - Spec: §3.3, §4 фаза 4.

---

### Task 7: Кластер-health `/s3/health/cluster`

**Вход:** Task 6 закоммичен; `S3Middleware` шаг 0 пропускает `/healthz` и metrics; `Program.cs` мапит `/healthz` через `drive.CheckHealth()`; `AdminHandlers` имеет `JsonOptions` (camelCase) и образец `HandleStorageAsync` (источники `StorageModeInfo`/`ErasureStatusProvider`); EC-топология фабрики 4/1 → write-кворум сетов = 3 (двух офлайн-дисков достаточно для write-деградации); `OwnS3EcAppFactory`/`OwnS3EcDegrAppFactory` — образцы фабрик/коллекций.

**Действие/Файлы:**
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` — шаг 0: пропуск префикса `/s3/health/`.
- Modify: `src/OwnS3.App/Program.cs` — `app.MapGet("/s3/health/cluster", …)` рядом с `/healthz`.
- Modify: `src/OwnS3.App/Handlers/AdminHandlers.cs` — `HandleClusterHealthAsync(HttpContext)`.
- Test: Create `src/tests/OwnS3.IntegrationTests/Api/ClusterHealthScenarios.cs` (single), Create `src/tests/OwnS3.IntegrationTests/Api/ClusterHealthEcScenarios.cs` (EC), Modify `src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs` (кейс `hm3`).
- Modify: `dev-stand/adminpanel/checks/46-owns3.sh` — проба cluster-health (опция спеки §5.2).

**Выход:** публичный GET `/s3/health/cluster` — 200/503 + JSON `healthyDrives`/`offlineDrives`/`totalDrives` (camelCase, long), без `healingDrives`; перехват до роутинга 21 операции (анонимный запрос — не 403); чек стенда видит эндпоинт.

**Проверка:** `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~ClusterHealth` (фон) → PASS (single + EC); E2E — шаг 7.4.

**Spec:** §5.1 (контракт), §5.2 (код и тесты), §7 критерии 9/10/11.

**Interfaces (Produces):**
```csharp
namespace OwnS3.App.Handlers;
// GET /s3/health/cluster: single — условия /healthz (touch-проба IXlDrive);
// EC — все ErasureStatusProvider.Sets[*].Writable; счётчики drives — по
// Sets[*].Drives[*].Online. Пишет 200/503 + JSON-тело camelCase.
public static async Task HandleClusterHealthAsync(HttpContext context);
```

- [ ] **Шаг 7.1. WAF-тесты single (красный)**

  - Вход: Task 6 в истории; эндпоинта нет (запрос упадёт в path-style матч конвейера — красный).
  - Действие: создать `src/tests/OwnS3.IntegrationTests/Api/ClusterHealthScenarios.cs`:
    ```csharp
    using System.Net;
    using System.Text.Json;

    namespace OwnS3.IntegrationTests.Api;

    // /s3/health/cluster (канон 05 §5, критерии §9–§11): публичная проба
    // (без подписи) готовности хранилища обслуживать запись — single-режим.
    // 200/503 + JSON drives-счётчики camelCase; шаг 0 конвейера — до роутинга
    // 21 операции (анонимный запрос не получает 403). Изоляция 503-кейса —
    // tmp-каталог тома с восстановлением (образец HealthzVolumeScenarios).
    public sealed class ClusterHealthScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
    {
        private static async Task<JsonDocument> JsonAsync(HttpResponseMessage response)
        {
            var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            return JsonDocument.Parse(body);
        }

        [Fact]
        public async Task ClusterHealth_200_JsonFields_NoSignature()
        {
            // Arrange / Act: анонимный GET (никаких Authorization-заголовков)
            var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                TestContext.Current.CancellationToken);

            // Assert: 200 (не 403 — путь перехвачен до роутинга), поля camelCase
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            using var json = await JsonAsync(response);
            json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(1);
            json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(0);
            json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(1);
            json.RootElement.TryGetProperty("healingDrives", out _).Should().BeFalse(
                "heal-статус — в /admin/storage, не в кластер-health");
        }

        [Fact]
        public async Task ClusterHealth_503_JsonFields_WhenVolumeUnavailable()
        {
            // Arrange: прогрев (ленивый старт хоста — как HealthzVolumeScenarios),
            // затем ломаем том удалением tmp-каталога
            (await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken))
                .StatusCode.Should().Be(HttpStatusCode.OK);
            var tmpDir = Path.Combine(factory.TempVolumeDir, ".owns3.sys", "tmp");
            if (Directory.Exists(tmpDir))
                Directory.Delete(tmpDir, recursive: true);
            try
            {
                // Act
                var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                    TestContext.Current.CancellationToken);

                // Assert: 503 + счётчики отказа (0/1/1)
                response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
                using var json = await JsonAsync(response);
                json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(0);
                json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(1);
                json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(1);
            }
            finally
            {
                // Teardown: восстановить структуру для соседних кейсов класса
                Directory.CreateDirectory(tmpDir);
            }
        }
    }
    ```
  - Выход: кейсы красные (эндпоинт ещё не перехватывается: путь уходит в роутер → 400/403).
  - Проверка: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~ClusterHealthScenarios` (фон) → FAIL — ожидаемый красный.
  - Spec: §5.2 (WAF single, HealthzVolumeScenarios-образец), §7 критерии 9/10/11.

- [ ] **Шаг 7.2. Реализация эндпоинта (зелёный 7.1)**

  - Вход: красные тесты 7.1.
  - Действие:
    1. `src/OwnS3.App/Pipeline/S3Middleware.cs`, шаг 0 (строки ~39–44) — дополнить условие:
       ```csharp
       // Шаг 0: служебные пути — точное совпадение, запрос уходит в endpoint'ы.
       var path = context.Request.Path;
       if (path == "/healthz"
           || path.StartsWithSegments("/s3/health/")   // кластер-health: до роутинга 21 операции (канон 05 §5)
           || (metricsOptions.Enabled && path == metricsOptions.Path))
       ```
    2. `src/OwnS3.App/Handlers/AdminHandlers.cs` — добавить `using OwnS3.Storage;` в using-блок (сейчас есть только `OwnS3.Storage.Erasure`) и метод в конец класса:
       ```csharp
       // GET /s3/health/cluster (канон 05 §5): готовность хранилища обслуживать
       // запись — single: touch-проба тома (условия /healthz); EC: все сеты
       // writable (write-кворум, глава 06). Публичная проба наблюдения — без
       // подписи (прецедент /healthz; потребитель — ClusterOk-проба панели,
       // adminpanel/02 §2.5). Тело — JSON drives-счётчики camelCase (long);
       // healingDrives не отдаётся (heal-проход не per-drive — /admin/storage).
       public static async Task HandleClusterHealthAsync(HttpContext context)
       {
           var mode = context.RequestServices.GetRequiredService<StorageModeInfo>();
           long healthy, offline, total;
           bool writable;
           if (mode.IsErasure)
           {
               var status = context.RequestServices.GetRequiredService<ErasureStatusProvider>().GetStatus();
               var drives = status.Sets.SelectMany(set => set.Drives).ToList();
               total = drives.Count;
               healthy = drives.Count(d => d.Online);
               offline = total - healthy;
               writable = status.Sets.All(set => set.Writable);
           }
           else
           {
               var drive = context.RequestServices.GetRequiredService<IXlDrive>();
               writable = drive.CheckHealth();
               total = 1;
               healthy = writable ? 1 : 0;
               offline = 1 - healthy;
           }

           context.Response.StatusCode = writable
               ? StatusCodes.Status200OK
               : StatusCodes.Status503ServiceUnavailable;
           context.Response.ContentType = "application/json";
           await context.Response.WriteAsync(JsonSerializer.Serialize(new
           {
               healthyDrives = healthy,
               offlineDrives = offline,
               totalDrives = total,
           }, JsonOptions));
       }
       ```
    3. `src/OwnS3.App/Program.cs` — рядом с `app.MapGet("/healthz", …)` (строки ~499–503):
       ```csharp
       // Кластер-health (канон 05 §5): готовность записи; перехват пути — шаг 0
       // S3Middleware (до роутинга 21 операций); без подписи.
       app.MapGet("/s3/health/cluster", Handlers.AdminHandlers.HandleClusterHealthAsync);
       ```
  - Выход: эндпоинт отвечает 200/503 + JSON; шаг 0 изолирует префикс.
  - Проверка: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~ClusterHealthScenarios` (фон) → PASS.
  - Spec: §5.1/§5.2 (S3Middleware шаг 0, Program.MapGet, AdminHandlers).

- [ ] **Шаг 7.3. WAF-тесты EC**

  - Вход: 7.2 зелёный.
  - Действие: создать `src/tests/OwnS3.IntegrationTests/Api/ClusterHealthEcScenarios.cs`:
    ```csharp
    using System.Net;
    using System.Text.Json;
    using Microsoft.Extensions.DependencyInjection;
    using OwnS3.Storage;

    namespace OwnS3.IntegrationTests.Api;

    // /s3/health/cluster в EC-топологии 4/1 (канон 05 §5, глава 06): 200 при всех
    // writable-сетах; write-деградация сета (в 4/1 write-кворум = 3 → двух
    // офлайн-дисков достаточно) → 503, offlineDrives > 0. Своя фабрика-коллекция —
    // деградация не влияет на соседние EC-серии (образец OwnS3EcDegrAppFactory).
    [Collection(ClusterHealthEcCollection.Name)]
    public sealed class ClusterHealthEcScenarios(OwnS3EcHealthAppFactory factory)
    {
        private OwnS3TestClient NewClient() => new(factory.CreateClient());
        private ErasureXlDrive Drive => factory.Services.GetRequiredService<ErasureXlDrive>();
        private string DiskDir(int index) => factory.DataDirs[index];

        // Перевод диска в offline с копией каталога (возврат — RestoreDisk);
        // образец ErasureDegradationScenarios.
        private string TakeDiskOffline(int index)
        {
            var backup = Path.Combine(Path.GetTempPath(), "owns3-ch-backup-" + Guid.NewGuid().ToString("N"));
            CopyDirectory(DiskDir(index), backup);
            Directory.Delete(DiskDir(index), recursive: true);
            Drive.RefreshOnline();
            return backup;
        }

        private void RestoreDisk(int index, string? backup)
        {
            if (backup is null)
                return;
            if (!Directory.Exists(DiskDir(index)))
                Directory.Move(backup, DiskDir(index));
            else if (Directory.Exists(backup))
                Directory.Delete(backup, recursive: true);
            Drive.RefreshOnline();
        }

        private static void CopyDirectory(string source, string dest)
        {
            Directory.CreateDirectory(dest);
            foreach (var file in Directory.EnumerateFiles(source))
                File.Copy(file, Path.Combine(dest, Path.GetFileName(file)));
            foreach (var dir in Directory.EnumerateDirectories(source))
                CopyDirectory(dir, Path.Combine(dest, Path.GetFileName(dir)));
        }

        private static async Task<JsonDocument> JsonAsync(HttpResponseMessage response)
        {
            var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            return JsonDocument.Parse(body);
        }

        [Fact]
        public async Task ClusterHealth_Ec_200_AllSetsWritable()
        {
            // Arrange / Act: здоровый пул 4/1 — анонимная проба
            var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                TestContext.Current.CancellationToken);

            // Assert: 200 + счётчики по всем дискам всех сетов
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            using var json = await JsonAsync(response);
            json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(4);
            json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(0);
            json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(4);
        }

        [Fact]
        public async Task ClusterHealth_Ec_503_WhenWriteQuorumLost()
        {
            // Arrange: прогрев хоста, затем диски 2 и 3 offline (онлайн 2 < write-кворума 3)
            (await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken))
                .StatusCode.Should().Be(HttpStatusCode.OK);
            var backup2 = TakeDiskOffline(2);
            var backup3 = TakeDiskOffline(3);
            try
            {
                // Act
                var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                    TestContext.Current.CancellationToken);

                // Assert: write-деградация сета → 503, офлайн-диски видны
                response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
                using var json = await JsonAsync(response);
                json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(2);
                json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(2);
                json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(4);
            }
            finally
            {
                RestoreDisk(3, backup3);
                RestoreDisk(2, backup2);
            }
        }
    }

    // Фабрика health-серии: СВОЙ 4-дисковый EC-том на коллекцию.
    public sealed class OwnS3EcHealthAppFactory : OwnS3EcAppFactory;

    [CollectionDefinition(Name)]
    public sealed class ClusterHealthEcCollection : ICollectionFixture<OwnS3EcHealthAppFactory>
    {
        public const string Name = "OwnS3EcHealth";
    }
    ```
  - Выход: EC-кейсы 200/503 в собственной коллекции.
  - Проверка: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~ClusterHealth` (фон) → PASS (single + EC вместе).
  - Spec: §5.2 (WAF EC), §7 критерий 9/10.

- [ ] **Шаг 7.4. E2E hm3 + чек стенда 46**

  - Вход: 7.3 зелёный.
  - Действие:
    1. В `src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs` дописать кейс:
       ```csharp
       [Fact]
       public async Task ClusterHealth_200_JsonFields()
       {
           DockerTrait.SkipIfUnavailable();
           var ct = TestContext.Current.CancellationToken;

           // AAA: кластер-health в докер-контуре (канон 05 §5): анонимный GET —
           // 200 (шаг 0 конвейера, не 403) + JSON-поля camelCase (панельная проба
           // ClusterOk adminpanel/02 §2.5 парсит эти имена)
           await OwnS3E2eEnvironment.RunScenarioAsync("hm3", async fx =>
           {
               using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

               // Act
               var response = await http.GetAsync($"{fx.S3BaseUrl}/s3/health/cluster", ct);

               // Assert: single-контур — 1 живой диск; healingDrives отсутствует
               response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
               using var json = System.Text.Json.JsonDocument.Parse(
                   await response.Content.ReadAsStringAsync(ct));
               json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(1);
               json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(0);
               json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(1);
               json.RootElement.TryGetProperty("healingDrives", out _).Should().BeFalse();
           }, ct);
       }
       ```
    2. В `dev-stand/adminpanel/checks/46-owns3.sh` после блока `# 1) healthz` (и до `# 2) mc-smoke…`) вставить:
       ```bash
       # 1b) кластер-health — 200 без подписи + JSON drives-поля (канон 05 §5;
       #     потребитель — ClusterOk-проба панели, adminpanel/02 §2.5)
       CLUSTER="$(curl -sf "$BASE/s3/health/cluster")" \
         || { echo "❌ $BASE/s3/health/cluster не 200"; exit 1; }
       echo "$CLUSTER" | grep -q '"healthyDrives"' \
         || { echo "❌ /s3/health/cluster: нет healthyDrives"; exit 1; }
       echo "  cluster-health: 200 + healthyDrives"
       ```
       (изменение — только проба нового эндпоинта; mc-smoke и метрики чека не трогаются.)
  - Выход: E2E-покрытие против контейнера; стендовый чек видит cluster-пробу.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~ClusterHealth_200_JsonFields` (фон) → PASS; bash-синтаксис чека — `bash -n dev-stand/adminpanel/checks/46-owns3.sh` → пусто (прогон чека целиком — на поднятом стенде, вне этого плана).
  - Spec: §5.2 (E2E HealthMetricsScenarios; чек 46 — опция спеки).

- [ ] **Шаг 7.5. Коммит**

  - Вход: 7.2–7.4 зелёные.
  - Действие:
    ```bash
    git add src/OwnS3.App/Pipeline/S3Middleware.cs src/OwnS3.App/Program.cs \
        src/OwnS3.App/Handlers/AdminHandlers.cs \
        src/tests/OwnS3.IntegrationTests/Api/ClusterHealthScenarios.cs \
        src/tests/OwnS3.IntegrationTests/Api/ClusterHealthEcScenarios.cs \
        src/tests/OwnS3.IntegrationTests/E2e/HealthMetricsScenarios.cs \
        dev-stand/adminpanel/checks/46-owns3.sh
    git commit -m "feat(owns3): кластер-health GET /s3/health/cluster — 200/503 + JSON drives-счётчики (шаг 0 конвейера, ClusterOk-проба панели); WAF single/EC + E2E hm3 + чек 46"
    ```
  - Выход: коммит.
  - Проверка: `git status --short` → пусто.
  - Spec: §5 целиком.

---

### Task 8: Мерж-гейт — полные прогоны

**Вход:** Tasks 1–7 закоммичены; рабочее дерево чистое; docker на хосте доступен; dev-стенд не поднят (серии не поверх стенда).

**Действие/Файлы:** изменений кода нет (кроме возможных фиксатов по результатам прогонов — тогда соответствующая задача дорабатывается и перекоммичивается). Прогоны — по одному, каждый до финальной строки, между сериями — проверка остатков docker.

**Выход:** зелёные полные прогоны: юниты OwnS3, WAF Api OwnS3, полный docker-E2E OwnS3 на свежем Release; страховочная зачистка сетей.

**Проверка:** итоговые команды и ожидания — в шагах ниже; все — фон, каждая серия дожидается финальной строки.

**Spec:** §4 фаза 6, §7 критерий 12.

- [ ] **Шаг 8.1. Полные юниты + WAF Api**

  - Вход: дерево чистое.
  - Действие: запустить (фон, по очереди):
    ```bash
    dotnet test src/tests/OwnS3.UnitTests -c Debug
    dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~OwnS3.IntegrationTests.Api
    ```
  - Выход: полные серии без фильтров-подмножеств.
  - Проверка: оба фоновых прогона → 0 failed (существующие тесты — без изменений исходов, критерий 12).
  - Spec: §7 критерий 12.

- [ ] **Шаг 8.2. Полный docker-E2E OwnS3 на свежем Release**

  - Вход: 8.1 зелёный.
  - Действие: запустить (фон):
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests \
        -c Release --filter FullyQualifiedName~OwnS3.IntegrationTests.E2e
    ```
    (E2E собирает Release сам — инкрементальный no-op; `PGW_TEST_E2E_NOBUILD` не ставить: нужен СВЕЖИЙ Release.)
  - Выход: полный E2E-прогон ownS3 (все сценарии, включая новые `s11`/`hm3` и регресс flexible-checksums `s7`).
  - Проверка: прогон → 0 failed, 0 skipped-по-гейту (docker=1); после финальной строки — `docker ps -a | grep -c os3-es` → 0, `docker network ls | grep os3-en` → пусто (остатки — по README-cleanup артефактов, затем `docker network prune -f` как страховка). Падение — разбор по логам/артефактам БЕЗ перезапуска (AGENTS.md телеметрия).
  - Spec: §4 фаза 6 («полный E2eFixture на свежем Release — обязателен»), §7 критерий 12.

- [ ] **Шаг 8.3. Маркер-кейс мерж-гейта**

  - Вход: 8.2 зелёный.
  - Действие: запустить (фон) кейс-маркер задачи (быстрая проверка мерж-гейта по образцу `Scale_AddEmptyShard`):
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests \
        -c Release --filter FullyQualifiedName~ChecksumHeader
    ```
  - Выход: маркер зафиксирован для будущих гейтов, затрагивающих ownS3.
  - Проверка: PASS (1 тест `AwsSdkSimpleScenarios.ChecksumHeader_Put_VerifyAndReject`); после — проверка остатков как в 8.2.
  - Spec: §3.3 («Маркер-кейс мерж-гейта — один из новых AwsSdk-сценариев»), §4 фаза 6.

- [ ] **Шаг 8.4. Финал ветки: статус и напоминание мерж-гейта**

  - Вход: 8.1–8.3 зелёные.
  - Действие: сводка для main-агента: `git log --oneline main..HEAD` (все коммиты задачи), `git status --short` (пусто). Мерж-коммит (по отдельной явной команде пользователя) обязан включить удаление тега `t91-owns3-checksum-verify` из `arch/roadmap/owns3.md` (правило AGENTS.md: тег снимается тем же коммитом, из списков и `←`-зависимостей; без пометок «закрыта»).
  - Выход: ветка готова к ревью/мержу.
  - Проверка: `git status --short` → пусто; список коммитов соответствует задачам 1–7.
  - Spec: §8 (решение 1 — тег `t91` снимается мерж-коммитом целиком).

---

## Self-Review (исполнен автором плана; для исполнителя — справка соответствия)

- **Покрытие спеки:** §3.1 канон → Task 1; §3.2 парсер → Task 3; поток → Task 4; PutObjectHandler/прочие хендлеры → Task 5 (шаг 5.1; «прочие без изменений» — регресс-кейсы (ж)/(з) + полный прогон 8.1); пакеты → Task 4 (csproj); drain-семантика → сохранена конструктивно (сверка в EOF обёртки, парсер до тела); §3.3 юниты → Tasks 3.1/4.1; WAF (а)–(з) → 5.2 (+коммит-семантика (и) под критерии 1/2/6); E2E → Task 6; §5 health → Task 7; §4 фазы → порядок задач 1–8; §7 критерии 1–8 → Tasks 4–6 (критерий 2 «событие репликации не отправлялось» — ассерт outbox кейса (и2), Task 5.2), 9–11 → Task 7, 12 → Task 8.
- **Типы/сигнатуры:** `ChecksumExpectation(ChecksumAlgorithm Algorithm, string ExpectedBase64)` определена в Task 3 и использована в Tasks 4–5 без переименований; конструктор `HashingBodyStream(…, ChecksumExpectation? checksum = null)` — Task 4 (производит), Tasks 5 (потребляет); `ChecksumConventions.Crc32BigEndian(Crc32)` — Task 2 (производит), Tasks 2/4 (потребляют). CRC-типы: crc32 — `System.IO.Hashing.Crc32`; crc32c — `OwnS3.Protocol.Auth.Crc32C` (собственный: в System.IO.Hashing 10.x тип отсутствует, big-endian по построению — общий с трейлерным путём `AwsChunkedReader`).
- **Заполненность:** код всех шагов приведён целиком; заполнители («TBD», «аналогично Task N») отсутствуют.
- **Допущения (явные):** (1) EC 503-кейс — «офлайн-дисков достаточно для write-деградации» = ДВА офлайн-диска в 4/1: write-кворум `ErasureQuorum.Write(k=3, m=1) = 3`, один офлайн оставляет 3 онлайн ≥ кворума → сет writable; двух достаточно (факт кода `ErasureStatusProvider`/`ErasureQuorum`). (2) Presigned PutObject с checksum-заголовком отдельного тест-кейса не имеет — §3.3/§7 спеки его не требуют; поведение то же, что у заголовочной подписи: хендлер читает заголовки независимо от схемы подписи (таблица режимов §3.1 — следствие одной точки парсинга). (3) Ассерт критерия 2 выполнен на `ReplicationAppFactory` (кейс (и2)), а не в кейсе (и): на базовой фабрике без соседей outbox-журнал не ведётся вовсе (`ReplicationEventsScenarios.NoNeighbors_NoOutboxDirectory`) — «пустота outbox» там не проверяема содержательно; на репликационной фабрике положительный контроль (успешный put → событие есть) отличает «битой put не ставит событие» от «события не ставятся никогда».
