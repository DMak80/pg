# owns3-denylist-replace — план реализации

> **Для agentic-воркеров:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять по задаче за раз.
> Шаги — чекбоксы (`- [ ]`) для трекинга.

**Цель:** метод `PUT /replication/denylist` — идемпотентная полная перезапись
deny-list репликации ownS3 одним вызовом, с авторизацией исключительно по
сертификату вызывающего (до-join контур replication-грани).

**Архитектура:** переиспользование до-join контура join (те же примитивы:
`PeerCertificate.FromJson` → `PeerCertificateAuthority.Verify` →
`DenyList.IsDenied` → `PeerRequestVerifier`), новый тонкий сервис
`DenyListReplaceService` (входящая сторона, без транспорта) + ветка в
peer-конвейере `S3Middleware` ДО резолва пира + хендлер в `ReplicationHandlers`.
Область действия deny-list не расширяется: перезапись решает только будущие
join, существующие линки/курсоры/targets не трогает.

**Стек:** .NET 10, C# (`Nullable=enable`, **`TreatWarningsAsErrors=true`**),
xUnit v3 + FluentAssertions, Testcontainers (docker-E2E).

**Спека:** [`docs/superpowers/2026-10-14-owns3-denylist-replace/spec.md`](spec.md)
— план аргументирует от спеки; исполнители читают обе. Канон контракта —
[`arch/owns3/07-replication.md`](../../../arch/owns3/07-replication.md)
(§2 «Полная перезапись deny-list», §6 `PUT /replication/denylist` + таблица
операций, §10 лог-события) — правки уже внесены arch-first в spec-фазе.

**WORKTREE:** весь код/тесты/коммиты — только в
`/Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace`
(ветка `feat-owns3-denylist-replace`). Все пути ниже — от корня worktree.

---

## Global Constraints

- **Arch-first уже исполнен** в spec-фазе (`arch/owns3/07-replication.md`
  §2/§6/§10 обновлены); код зеркалит канон, расхождение — блокирующее
  замечание ревью. Фаза кода начинается со сверки (Task 0).
- **Формат ошибок — стиль join-грани**: отказы доверия/подписи — XML
  (`S3ErrorXmlWriter`, `AccessDenied`/коды каталога подписи), 400
  `AuthorizationHeaderMalformed` — XML; malformed заголовка сертификата/тела —
  400 JSON `{error}`.
- **Порядок проверок §4.2 spec**: конфигурация → сертификат → доверие →
  подпись → тело. Тело НЕ разбирается при отказе доверия.
- **Успех — 202 Accepted без тела**; `revokedAtUnixMs` каждой записи ставит
  сервер (момент применения); пустой `entries: []` — снятие всех отзывов.
- **Существующие члены `DenyList` (`Add`/`Remove`/`Snapshot`/`IsDenied`) и
  admin-операции deny-list — без изменений** (принцип 4 spec).
- .NET 10, `TreatWarningsAsErrors` — сборка без ворнингов; комментарии
  по-русски, идентификаторы по-английски; тесты — AAA-нотация (комментарии
  Arrange/Act/Assert).
- Коммиты в feature-ветке — свободно, после каждого зелёного цикла; стиль по
  образцу истории (`t39: …`): префикс `owns3-denylist-replace: …`.
- Команды тестов — только в фоне (`run_in_background: true`), фиксировать
  task_id; таймауты ожидания — не более 30 секунд; после КАЖДОЙ тестовой
  серии (юниты → интеграции → docker-E2E) — зачистка контейнеров/сетей.
- docker-E2E: каноны [`docs/e2e-isolation.md`](../../../docs/e2e-isolation.md)
  / [`docs/e2e-launch.md`](../../../docs/e2e-launch.md) — уже реализованы в
  `OwnS3MeshE2eEnvironment` (guid-изоляция, полный teardown при любом исходе,
  ассерт чистоты, динамические порты, `[PHASE]`-телеметрия); новые кейсы
  пишутся поверх этой инфраструктуры, ничего своего не поднимают.
- Ручная зачистка между сериями — страховочный гейт (основной механизм —
  самоочистка тестов): остаточных контейнеров `os3m-*` и сетей `os3m-en-*`
  после серии быть не должно.

---

## Карта файлов

| Файл | Действие | Ответственность |
|---|---|---|
| `src/OwnS3.Replication/DenyList.cs` | Modify | `Replace(...)` + валидация записей; существующие члены не меняются |
| `src/OwnS3.Replication/Transport/ReplicationErrors.cs` | Modify | `DenyReplaceRefusedException`, `DenyReplaceBadRequestException`, `DenyListReplaced` (рядом с join-исключениями) |
| `src/OwnS3.Replication/DenyListReplaceService.cs` | Create | входящая сторона перезаписи (шаги 1–9 §4.2, без хоста) |
| `src/OwnS3.App/Pipeline/S3Middleware.cs` | Modify | ветка `PUT /replication/denylist` в peer-конвейере (до резолва пира) |
| `src/OwnS3.App/Handlers/ReplicationHandlers.cs` | Modify | `HandleDenyReplaceAsync` + DI-параметр сервиса |
| `src/OwnS3.App/Program.cs` | Modify | регистрация `DenyListReplaceService` рядом с `JoinPeerService` |
| `src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs` | Modify | кейсы `Replace` |
| `src/tests/OwnS3.UnitTests/Replication/DenyListReplaceServiceTests.cs` | Create | контур доверия/подписи/тела без хоста |
| `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs` | Modify | api-кейсы метода + хелперы |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs` | Modify | docker-E2E кейсы + хелпер edge-вызова |

Прецеденты (читать исполнителю соответствующей задачи):
`JoinPeerService.cs` (до-join контур), `ReplicationTargets.Replace/Validate`
(идемпотентная замена + валидация до персиста), `HandleJoinAsync` в
`ReplicationHandlers.cs` (маппинг исходов), `Api/PeerAuthScenarios.cs`
(`SendPeerOnAsync`, `PeerAuthMaterial`, собственные фабрики кейсов),
`E2e/Replication/PeerAuthScenarios.cs` (`SendEdgeJoinAsync`,
`AdminDenyListAsync`, `MeshPeerAuthMaterial`).

---

### Task 0: Сверка arch-канона (фаза 0 spec)

**Файлы:** читать `arch/owns3/07-replication.md` (без правок).

**Вход:** spec одобрен; arch-правки spec-фазы слиты в ветку.

**Действие:** прочитать и сверить буквы канона, на которые опирается код:
- §2 п. «Полная перезапись deny-list» — контракт метода (тело, валидация,
  202/400/403, до-join контур, локальность/оркестратор);
- §6 п. `PUT /replication/denylist` + строка `denylist replace` в таблице
  операций;
- §10 «Лог-события» — «…снятие отзыва и полная перезапись deny-list (число
  записей)».

**Выход:** уверенность, что код ниже зеркалит канон; расхождение канона с
plan/spec — стоп и вопрос пользователю (не выбирать прочтение самому).

**Проверка:** тексты трёх пунктов канона читаны; противоречий с §4 spec нет.

**Связь со spec:** §5 фаза 0.

- [ ] **Шаг 0.1:** прочитать указанные пункты `arch/owns3/07-replication.md`,
  зафиксировать соответствие каждому исходу кода в плане (таблица §4.2 spec ↔
  §6 канона).

---

### Task 1: `DenyList.Replace` — юниты (TDD)

**Файлы:**
- Modify: `src/OwnS3.Replication/DenyList.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs`

**Interfaces (produces, для Task 2):**
```csharp
// DenyList.cs — новый член (существующие Add/Remove/Snapshot/IsDenied не меняются):
public void Replace(IReadOnlyList<(string Name, string PublicKeyPem)> entries)
// валидация ДО персиста; невалидное → ArgumentException, снапшот не тронут;
// успех: полный обмен словаря под _lock, все записи с
// revokedAtUnixMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds(); персист tmp+rename.
```

**Вход:** Task 0 сверен; переиспользуются существующие: фикстура `DenyListFixture`
(объявлена в `src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs`) и
`FixedTimeProvider` (`src/tests/OwnS3.UnitTests/TestVectors.cs`).

**Действие:** RED → GREEN цикл по кейсам фазы 1 spec.

**Выход:** `DenyList.Replace` с валидацией и персистом; зелёные юниты.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~DenyListTests` — зелёный.

**Связь со spec:** §4.3 (контракт `Replace`), §5 фаза 1, критерии 1/2/4/5 (часть).

- [ ] **Шаг 1.1: написать failing-тесты** — добавить в конец `DenyListTests.cs`
  (в класс `DenyListTests`; `using System.Security.Cryptography;` в верх файла):

```csharp
// ── Replace: полная перезапись (PUT /replication/denylist, глава 07 §2) ──

// Валидный PEM публичного ключа ECDSA P-256 (валидация Replace — ImportFromPem;
// константы KeyA/KeyB — намеренно битые, для негативных кейсов).
private static string ValidPem()
{
    using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    return key.ExportSubjectPublicKeyInfoPem();
}

[Fact]
public void Replace_SwapsList_PersistsAndReloads()
{
    // Arrange — отзыв n1 (битый PEM Add-у не мешает); перезапись одним n2
    var denyList = Create();
    denyList.Add("n1", KeyA);
    var pem2 = ValidPem();

    // Act
    denyList.Replace([("n2", pem2)]);

    // Assert — полный обмен: старой записи нет, новая есть (имя и ключ);
    // персист: перечитанный с того же файла видит то же
    denyList.Snapshot.Should().ContainSingle()
        .Which.Should().BeEquivalentTo(new DenyEntry("n2", pem2, At.ToUnixTimeMilliseconds()));
    denyList.IsDenied("n1", KeyA).Should().BeFalse();
    denyList.IsDenied("n2", "другой-ключ").Should().BeTrue();
    Create().Snapshot.Should().ContainSingle().Which.Name.Should().Be("n2");
}

[Fact]
public void Replace_EmptyList_ClearsAll_FileIsEmptyArray()
{
    // Arrange — один отзыв
    var denyList = Create();
    denyList.Add("n1", KeyA);

    // Act — пустой entries: снятие всех отзывов
    denyList.Replace([]);

    // Assert — список пуст; файл — литерал [] (формат на диске не меняется)
    denyList.Snapshot.Should().BeEmpty();
    denyList.IsDenied("n1", KeyA).Should().BeFalse();
    File.ReadAllText(_fixture.FilePath).Should().Be("[]");
    Create().Snapshot.Should().BeEmpty();
}

[Fact]
public void Replace_SetsRevokedAtToApplicationTime()
{
    // Arrange — перезапись в At+5мин (провайдер с другим «сейчас»)
    var later = At.AddMinutes(5);
    var denyList = Create(new FixedTimeProvider(later));
    var pem = ValidPem();

    // Act
    denyList.Replace([("n1", pem)]);

    // Assert — время каждой записи = момент применения перезаписи (серверное)
    denyList.Snapshot.Should().ContainSingle()
        .Which.RevokedAtUnixMs.Should().Be(later.ToUnixTimeMilliseconds());
}

[Fact]
public void Replace_IdempotentSamePayload_UpdatesTime()
{
    // Arrange — первая перезапись в At
    var pem = ValidPem();
    Create().Replace([("n1", pem)]);

    // Act — повторная перезапись тем же составом позже: успешная замена
    var later = At.AddMinutes(5);
    var reloaded = Create(new FixedTimeProvider(later));
    reloaded.Replace([("n1", pem)]);

    // Assert — одна запись, время от последнего применения
    reloaded.Snapshot.Should().ContainSingle()
        .Which.RevokedAtUnixMs.Should().Be(later.ToUnixTimeMilliseconds());
}

[Fact]
public void Replace_PersistAtomic_NoTmpLeft()
{
    // Arrange
    var denyList = Create();

    // Act
    denyList.Replace([("n1", ValidPem())]);

    // Assert — tmp+rename: файла .tmp не осталось, основной файл заменён
    File.Exists(_fixture.FilePath + ".tmp").Should().BeFalse();
    File.Exists(_fixture.FilePath).Should().BeTrue();
}

[Theory]
[InlineData("name-empty")]      // пустое имя
[InlineData("pem-empty")]       // пустой publicKeyPem
[InlineData("pem-broken")]      // PEM не импортируется как ECDSA P-256
[InlineData("name-duplicate")]  // дубли имён
[InlineData("key-duplicate")]   // дубли ключей
public void Replace_InvalidEntries_Throws_SnapshotAndFileUntouched(string kind)
{
    // Arrange — действующий отзыв keep; невалидный состав по виду кейса
    var denyList = Create();
    denyList.Add("keep", KeyA);
    var pemA = ValidPem();
    var pemB = ValidPem();
    var entries = kind switch
    {
        "name-empty" => [("", pemA)],
        "pem-empty" => [("n1", "")],
        "pem-broken" => [("n1", KeyA)],
        "name-duplicate" => [("n1", pemA), ("n1", pemB)],
        _ => [("n1", pemA), ("n2", pemA)],
    };

    // Act
    var act = () => denyList.Replace(entries);

    // Assert — ArgumentException ДО персиста: снапшот и файл не тронуты
    act.Should().Throw<ArgumentException>();
    denyList.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    Create().Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
}

[Fact]
public void Replace_CoexistsWithAddRemoveUnderLock()
{
    // Arrange — смешанная последовательность операций одного инстанса
    var pem = ValidPem();
    var denyList = Create();

    // Act
    denyList.Add("n1", KeyA);
    denyList.Replace([("n2", pem)]);
    denyList.Add("n3", KeyB);
    var removed = denyList.Remove("n2");

    // Assert — операции Replace/Add/Remove/IsDenied согласованы под одним локом;
    // персист отражает финальный состав
    removed.Should().BeTrue();
    denyList.IsDenied("n2", pem).Should().BeFalse();
    denyList.IsDenied("n3", "x").Should().BeTrue();
    denyList.Snapshot.Select(e => e.Name).Should().BeEquivalentTo("n3");
    Create().Snapshot.Select(e => e.Name).Should().BeEquivalentTo("n3");
}
```

- [ ] **Шаг 1.2: прогнать — убедиться в FAIL** (в фоне, task_id зафиксировать):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~DenyListTests`
  Ожидание: новые кейсы падают (нет метода `Replace` — ошибка компиляции CS1061).

- [ ] **Шаг 1.3: минимальная реализация** — в `src/OwnS3.Replication/DenyList.cs`
  добавить после `Remove` (существующие члены не трогать):

```csharp
// Полная перезапись deny-list (PUT /replication/denylist, глава 07 §2):
// идемпотентная замена всего списка; валидация ДО персиста — невалидный состав
// бросает ArgumentException, снапшот не тронут (прецедент ReplicationTargets.Replace);
// каждая запись получает revokedAtUnixMs = момент применения перезаписи.
public void Replace(IReadOnlyList<(string Name, string PublicKeyPem)> entries)
{
    ValidateEntries(entries);
    lock (_lock)
    {
        var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        _entries.Clear();
        foreach (var (name, publicKeyPem) in entries)
            _entries[name] = new DenyEntry(name, publicKeyPem, now);
        Persist();
    }
}

// Валидация записей перезаписи (глава 07 §2): непустые name/publicKeyPem, PEM
// публичного ключа ECDSA P-256, уникальность имён и ключей (образец
// ReplicationTargets.Validate — текст ошибок по-русски, имя параметра entries).
private static void ValidateEntries(IReadOnlyList<(string Name, string PublicKeyPem)> entries)
{
    var nameDuplicates = DuplicatesOf(entries.Select(e => e.Name).ToList());
    if (nameDuplicates.Count > 0)
        throw new ArgumentException(
            $"Дубликаты имён записей deny-list: {string.Join(",", nameDuplicates)}", nameof(entries));
    var keyDuplicates = DuplicatesOf(entries.Select(e => e.PublicKeyPem).ToList());
    if (keyDuplicates.Count > 0)
        throw new ArgumentException(
            $"Дубликаты публичных ключей deny-list: {string.Join(",", keyDuplicates)}", nameof(entries));
    foreach (var (name, publicKeyPem) in entries)
    {
        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(publicKeyPem))
            throw new ArgumentException(
                $"Запись deny-list {name}: name и publicKeyPem обязательны", nameof(entries));
        try
        {
            using var key = System.Security.Cryptography.ECDsa.Create();
            key.ImportFromPem(publicKeyPem); // SUBJECT PUBLIC KEY ECDSA P-256
        }
        catch (Exception ex)
        {
            throw new ArgumentException(
                $"Запись deny-list {name}: невалидный PEM публичного ключа ECDSA P-256 — {ex.Message}",
                nameof(entries));
        }
    }
}

private static List<string> DuplicatesOf(List<string> values) =>
    values.GroupBy(v => v, StringComparer.Ordinal).Where(g => g.Count() > 1)
        .Select(g => g.Key).ToList();
```

- [ ] **Шаг 1.4: прогнать — убедиться в PASS** (той же командой; в фоне).
  Ожидание: весь `DenyListTests` зелёный (старые кейсы не тронуты).

- [ ] **Шаг 1.5: коммит**
```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace add src/OwnS3.Replication/DenyList.cs src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace commit -m "owns3-denylist-replace: DenyList.Replace — идемпотентная полная перезапись deny-list (валидация до персиста, tmp+rename) + юниты"
```

---

### Task 2: исключения + `DenyListReplaceService` — юниты (TDD)

**Файлы:**
- Modify: `src/OwnS3.Replication/Transport/ReplicationErrors.cs`
- Create: `src/OwnS3.Replication/DenyListReplaceService.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/DenyListReplaceServiceTests.cs` (новый)

**Interfaces:**
- Consumes: `DenyList.Replace` (Task 1); `PeerIdentity(Name, Certificate, Key,
  CertificateJson)`; `PeerTrustAnchor.FromJson`; `PeerCertificate.FromJson`;
  `PeerCertificateAuthority.CreateCa/IssueServer/Verify`; `PeerRequestSigner.Sign`;
  `PeerRequestVerifier.Verify(model, publicKey) : PeerVerifyResult`
  (`Fail(S3ErrorCode Code, string? Detail)`); `S3RequestModel`
  (`Method, RawPath, RawQuery, Headers, Host, OpenBody`);
  `S3HeaderCollection.FromPairs`; `SigV4Core.HexSha256`; `Result<T>`
  (`Failed/Success/IsSuccess/Value/Error`).
- Produces (для Task 3):
```csharp
// Transport/ReplicationErrors.cs:
public sealed class DenyReplaceRefusedException(string message, S3ErrorCode? errorCode = null)
    : Exception(message) { public S3ErrorCode? ErrorCode { get; } }
public sealed class DenyReplaceBadRequestException(string message) : Exception(message);
public sealed record DenyListReplaced(int Count);

// DenyListReplaceService.cs:
public sealed class DenyListReplaceService(
    PeerIdentity? identity, PeerTrustAnchor? trustAnchor, DenyList denyList,
    TimeProvider timeProvider, ILogger<DenyListReplaceService> logger)
{
    public Result<DenyListReplaced> HandleIncoming(S3RequestModel model,
        string certificateBase64, ReadOnlyMemory<byte> rawBody);
}
```

**Вход:** Task 1 слит (зелёный). Прецедент тестов —
`src/tests/OwnS3.UnitTests/Replication/JoinPeerServiceTests.cs`
(мини-PKI хелперы `NewCa`/`MakeIdentity`/`CertBase64`, fake-модель запроса).

**Действие:** RED → GREEN: сначала исключения+сервис-скелет (компиляция
тестов требует типы), затем тесты, затем полная реализация сервиса.

**Выход:** сервис входящей перезаписи, тестируемый без хоста; все исходы §4.2
шагов 1–9 покрыты юнитами.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~DenyListReplaceServiceTests` — зелёный.

**Связь со spec:** §4.4 (сервис), §4.2 (порядок/исходы), §5 фаза 2.

- [ ] **Шаг 2.1: типы ошибок** — в `src/OwnS3.Replication/Transport/ReplicationErrors.cs`
  добавить после join-исключений (перед `PeerJoined`):

```csharp
// ── owns3-denylist-replace: ошибки входящей перезаписи deny-list
// (глава 07 §2/§6, конвенция 01 §4) ──

/// <summary>Перезапись deny-list отказана — маркер 403: конфигурация (ручной
/// режим состава), доверие (CA/истёк/deny-list вызывающего) или подпись запроса.
/// ErrorCode — исходный код каталога подписи для отказов верификации; грань
/// отдаст errorCode ?? AccessDenied (AuthorizationHeaderMalformed → 400).</summary>
public sealed class DenyReplaceRefusedException(string message, S3ErrorCode? errorCode = null)
    : Exception(message)
{
    public S3ErrorCode? ErrorCode { get; } = errorCode;
}

/// <summary>Тело перезаписи malformed/невалидно (не-JSON, нет entries, пустые
/// поля, битый PEM, дубли имён/ключей) — маркер 400 JSON {error}; список
/// не менялся.</summary>
public sealed class DenyReplaceBadRequestException(string message) : Exception(message);

/// <summary>Исход входящей перезаписи: применённый состав (число записей —
/// лог/тесты, глава 07 §10).</summary>
public sealed record DenyListReplaced(int Count);
```

- [ ] **Шаг 2.2: failing-тесты** — создать
  `src/tests/OwnS3.UnitTests/Replication/DenyListReplaceServiceTests.cs`:

```csharp
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Входящая перезапись deny-list (глава 07 §2/§6, PUT /replication/denylist):
// до-join контур по прецеденту JoinPeerServiceTests — реальные мини-PKI
// материалы, подпись PeerRequestSigner, fake-модель запроса; порядок проверок
// §4.2 (конфигурация → сертификат → доверие → подпись → тело).
public sealed class DenyListReplaceServiceFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public string DenyPath => Path.Combine(Dir, "denylist.json");

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class DenyListReplaceServiceTests : IDisposable
{
    private readonly DenyListReplaceServiceFixture _fixture = new();

    // Фиксированное «сейчас» (skew ±15 мин верификатора — от At).
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    // ── материал: CA + identity вызывающего/принимающего (как JoinPeerServiceTests) ──

    private static (PeerTrustAnchor Ca, string CaPrivatePem) NewCa(string name = "owns3-ca")
    {
        var (json, pem) = PeerCertificateAuthority.CreateCa(name);
        return (PeerTrustAnchor.FromJson(json), pem);
    }

    private static PeerIdentity MakeIdentity(string name, PeerTrustAnchor ca, string caPem,
        DateTimeOffset? at = null, int expiresDays = PeerCertificateAuthority.DefaultExpiresDays)
    {
        var (cert, pem) = PeerCertificateAuthority.IssueServer(name, ca, caPem, at ?? At, expiresDays);
        var key = ECDsa.Create();
        key.ImportFromPem(pem);
        return new PeerIdentity(cert.Name, cert, key, cert.ToJson());
    }

    private static string CertBase64(PeerIdentity side) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(side.CertificateJson));

    // Валидный PEM записи списка (ECDSA P-256).
    private static string ValidPem()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return key.ExportSubjectPublicKeyInfoPem();
    }

    // Тело перезаписи {entries:[{name, publicKeyPem}]} (camelCase, §4.1).
    private static string BodyOf(params (string Name, string PublicKeyPem)[] entries) =>
        JsonSerializer.Serialize(new
        {
            entries = entries.Select(e => new { name = e.Name, publicKeyPem = e.PublicKeyPem }).ToList(),
        });

    // Подписанная модель PUT /replication/denylist (по образцу SignedJoinModel).
    private static S3RequestModel SignedModel(ECDsa signingKey, string callerName, string body,
        DateTimeOffset? at = null)
    {
        var date = at ?? At;
        var payloadHash = SigV4Core.HexSha256(Encoding.UTF8.GetBytes(body));
        var (auth, peerDate, _) = PeerRequestSigner.Sign(signingKey, callerName, "PUT",
            "/replication/denylist", "",
            [("host", "neighbor:9000"),
                ("x-owns3-peer-date", PeerDateOf(date)),
                ("x-owns3-peer-content-sha256", payloadHash)],
            payloadHash, date);
        return new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/replication/denylist",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", auth),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(body)),
        };
    }

    private static string PeerDateOf(DateTimeOffset at) =>
        at.ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    private DenyListReplaceService Service(PeerIdentity? identity, PeerTrustAnchor? trust,
        DenyList deny, TimeProvider? tp = null) =>
        new(identity, trust, deny, tp ?? new FixedTimeProvider(At),
            NullLogger<DenyListReplaceService>.Instance);

    private static ReadOnlyMemory<byte> BodyBytes(string body) =>
        new(Encoding.UTF8.GetBytes(body));

    // ── валидный вызов ──

    [Fact]
    public void Valid_ReplaceApplied_CountReturned()
    {
        // Arrange — принимающий (mesh-a) с CA; вызывающий-оркестратор (operator)
        // с сертификатом того же CA (не числится соседом — неважно для метода)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(me, ca, deny, tp);
        var (name, pem) = ("peer-b", ValidPem());
        var (name2, pem2) = ("peer-c", ValidPem());

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, BodyOf((name, pem), (name2, pem2))),
            CertBase64(caller), BodyBytes(BodyOf((name, pem), (name2, pem2))));

        // Assert — Success; состав = тело; count = 2; время — серверное (At)
        result.IsSuccess.Should().BeTrue();
        result.Value.Count.Should().Be(2);
        deny.Snapshot.Select(e => e.Name).Should().BeEquivalentTo(name, name2);
        deny.Snapshot.Should().OnlyContain(e => e.RevokedAtUnixMs == At.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Valid_EmptyEntries_ClearsList()
    {
        // Arrange — действующий отзыв; вызывающий не отозван
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("old", ValidPem());
        var service = Service(me, ca, deny, tp);

        // Act — пустой entries: снятие всех отзывов
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — Success; список пуст
        result.IsSuccess.Should().BeTrue();
        result.Value.Count.Should().Be(0);
        deny.Snapshot.Should().BeEmpty();
    }

    // ── конфигурация ──

    [Fact]
    public void NoIdentity_Refused()
    {
        // Arrange — identity принимающего не задан (ручной режим) — как join: 403
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(null, ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── доверие сертификату ──

    [Fact]
    public void NoTrust_Refused()
    {
        // Arrange — trust-anchor не задан (ручной режим состава)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), null, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── доверие сертификату ──

    [Fact]
    public void OtherCaCert_Refused()
    {
        // Arrange — сертификат вызывающего от чужого CA
        var (ca, caPem) = NewCa();
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var caller = MakeIdentity("operator", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    [Fact]
    public void ExpiredCert_Refused()
    {
        // Arrange — истёкший сертификат вызывающего (выпуск 2 дня назад, срок 1)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem, at: At.AddDays(-2), expiresDays: 1);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    [Fact]
    public void UnparsableCert_Refused()
    {
        // Arrange — заголовок сертификата — base64 мусора (не JSON)
        var (ca, caPem) = NewCa();
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(ECDsa.Create(), "operator", BodyOf()),
            Convert.ToBase64String(Encoding.UTF8.GetBytes("не-сертификат")), BodyBytes(BodyOf()));

        // Assert — FromJson бросает → отказ доверия (403), не 400
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── deny-list вызывающего ──

    [Fact]
    public void DeniedCallerByName_Refused()
    {
        // Arrange — вызывающий отозван по имени (ключ другой)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("operator", "-----BEGIN PUBLIC KEY-----\nOTHER\n-----END PUBLIC KEY-----\n");
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — отозванный не переписывает список отзывов
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("operator");
    }

    [Fact]
    public void DeniedCallerByKey_Refused()
    {
        // Arrange — вызывающий отозван по публичному ключу (имя другое)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("другое-имя", caller.Certificate.PublicKeyPem);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("другое-имя");
    }

    // ── подпись запроса ──

    [Fact]
    public void SignatureByOtherKey_RefusedWithCatalogCode()
    {
        // Arrange — запрос подписан ЧУЖИМ ключом (сертификат при этом валидный)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var stranger = MakeIdentity("stranger", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(stranger.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — код каталога подписи (грань отдаст 403 SignatureDoesNotMatch)
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void SkewedDate_RefusedWithCatalogCode()
    {
        // Arrange — дата подписи At+16 минут (допуск ±15)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, BodyOf(), at: At.AddMinutes(16)),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.RequestTimeTooSkewed);
    }

    [Fact]
    public void MalformedAuthorization_RefusedWith400Code()
    {
        // Arrange — Authorization валидного префикса OWNS3-PEER-, но мусорная
        // структура (без Credential): Verify → AuthorizationHeaderMalformed
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);
        var model = new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/replication/denylist",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", "OWNS3-PEER-мусор"),
                ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", SigV4Core.HexSha256(Encoding.UTF8.GetBytes(BodyOf()))),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(BodyOf())),
        };

        // Act
        var result = service.HandleIncoming(model, CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — код AuthorizationHeaderMalformed (грань отдаст 400 XML)
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.AuthorizationHeaderMalformed);
    }

    // ── порядок: доверие ДО тела (принцип 7) ──

    [Fact]
    public void GarbageBody_UntrustedCert_RefusedNotBadRequest()
    {
        // Arrange — мусорное тело + сертификат чужого CA
        var (ca, caPem) = NewCa();
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var caller = MakeIdentity("operator", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, "{мусор не json"),
            CertBase64(caller), BodyBytes("{мусор не json"));

        // Assert — отказ доверия (403), тело не разбирается и не рулит кодом
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── тело (после доверия) ──

    [Fact]
    public void MalformedJson_BadRequest_ListUntouched()
    {
        // Arrange — действующий отзыв; тело — не-JSON
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("keep", ValidPem());
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, "{мусор"),
            CertBase64(caller), BodyBytes("{мусор"));

        // Assert — 400-тип; Replace НЕ вызван (снапшот/файл прежние)
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    }

    [Fact]
    public void EntriesMissing_BadRequest()
    {
        // Arrange — корректный JSON без поля entries
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, """{"noEntries":true}"""),
            CertBase64(caller), BodyBytes("""{"noEntries":true}"""));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().BeEmpty();
    }

    [Theory]
    [InlineData("dup-names")]
    [InlineData("dup-keys")]
    [InlineData("broken-pem")]
    [InlineData("empty-fields")]
    public void InvalidEntries_BadRequest_ListUntouched(string kind)
    {
        // Arrange — действующий отзыв; невалидный состав по виду кейса
        // (валидация Replace → ArgumentException → 400-тип)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("keep", ValidPem());
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);
        var pemA = ValidPem();
        var pemB = ValidPem();
        var body = kind switch
        {
            "dup-names" => BodyOf(("n1", pemA), ("n1", pemB)),
            "dup-keys" => BodyOf(("n1", pemA), ("n2", pemA)),
            "broken-pem" => BodyOf(("n1", "-----BEGIN PUBLIC KEY-----\nZZZ\n-----END PUBLIC KEY-----\n")),
            _ => JsonSerializer.Serialize(new
            {
                entries = new object[] { new { name = "", publicKeyPem = "" } },
            }),
        };

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, body),
            CertBase64(caller), BodyBytes(body));

        // Assert — 400-тип; снапшот/файл не тронуты (валидация ДО персиста)
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
        new DenyList(_fixture.DenyPath, tp).Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    }
}
```

- [ ] **Шаг 2.3: скелет сервиса** — создать
  `src/OwnS3.Replication/DenyListReplaceService.cs` (компиляция тестов
  требует тип; полная реализация = скелет сразу):

```csharp
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Входящая сторона полной перезаписи deny-list (глава 07 §2/§6, PUT
// /replication/denylist): до-join контур — те же примитивы, что у входящего
// join (JoinPeerService.HandleIncoming): сертификат вызывающего →
// PeerCertificateAuthority.Verify против trust-anchor → DenyList.IsDenied →
// PeerRequestVerifier (ключ сертификата) → тело → DenyList.Replace. Проверки
// доверия — ДО разбора тела: malformed-тело от недоверенного источника не
// рулит кодом ответа. Тестируем без хоста (прецедент JoinPeerService);
// собственного транспорта нет — метод только входящий.
public sealed class DenyListReplaceService(
    PeerIdentity? identity,
    PeerTrustAnchor? trustAnchor,
    DenyList denyList,
    TimeProvider timeProvider,
    ILogger<DenyListReplaceService> logger)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    // Тело перезаписи (глава 07 §2): {entries:[{name, publicKeyPem}]};
    // revokedAtUnixMs ставит сервер — в теле не передаётся.
    private sealed record DenyReplaceEntry(string? Name, string? PublicKeyPem);

    private sealed record DenyReplaceBody(IReadOnlyList<DenyReplaceEntry>? Entries);

    // Шаги 1–9 §4.2: конфигурация → сертификат → доверие → подпись → тело.
    public Result<DenyListReplaced> HandleIncoming(S3RequestModel model, string certificateBase64,
        ReadOnlyMemory<byte> rawBody)
    {
        // 1. Ручной режим состава (identity не нужен для проверки — семантика
        //    та же, что у join: без trust-anchor перезапись недоступна).
        if (identity is null || trustAnchor is null)
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                "перезапись deny-list отказана: identity и/или trust-anchor не заданы (ручной режим состава)"));

        // 2. Сертификат вызывающего.
        PeerCertificate cert;
        try
        {
            cert = PeerCertificate.FromJson(
                Encoding.UTF8.GetString(Convert.FromBase64String(certificateBase64)));
        }
        catch (Exception ex)
        {
            logger.LogWarning("deny-list replace: сертификат вызывающего не читается — {Message}", ex.Message);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: сертификат вызывающего не читается — {ex.Message}"));
        }

        // 3. Доверие: подписан CA trust-anchor ∧ не истёк.
        if (!PeerCertificateAuthority.Verify(cert, trustAnchor, timeProvider.GetUtcNow()))
        {
            logger.LogWarning(
                "deny-list replace: сертификат вызывающего {Name} не проходит верификацию trust-anchor (CA/срок)",
                cert.Name);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: сертификат вызывающего {cert.Name} не проходит верификацию trust-anchor (CA/срок)"));
        }

        // 4. Вызывающий не отозван (имя ИЛИ ключ) — отозванный не переписывает
        //    список отзывов.
        if (denyList.IsDenied(cert.Name, cert.PublicKeyPem))
        {
            logger.LogWarning("deny-list replace: вызывающий {Name} отозван (deny-list)", cert.Name);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: вызывающий {cert.Name} отозван (deny-list)"));
        }

        // 5. Подпись запроса — публичным ключом сертификата; ErrorCode зеркалит
        //    каталог подписи (грань отдаст errorCode ?? AccessDenied).
        using var callerKey = ECDsa.Create();
        try
        {
            callerKey.ImportFromPem(cert.PublicKeyPem);
        }
        catch (Exception ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: публичный ключ сертификата вызывающего не импортируется — {ex.Message}"));
        }
        var verify = new PeerRequestVerifier(timeProvider).Verify(model, callerKey);
        if (verify is not PeerVerifyResult.Ok)
        {
            var code = verify is PeerVerifyResult.Fail fail ? (S3ErrorCode?)fail.Code : null;
            logger.LogWarning("deny-list replace: подпись запроса {Name} не проходит ({Code})",
                cert.Name, code);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: подпись запроса {cert.Name} не проходит верификацию", code));
        }

        // 6. Тело — ПОСЛЕ доверия/подписи.
        DenyReplaceBody? body;
        try
        {
            body = JsonSerializer.Deserialize<DenyReplaceBody>(rawBody.Span, Json);
        }
        catch (JsonException ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(
                $"malformed тело перезаписи deny-list — {ex.Message}"));
        }
        if (body?.Entries is null)
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(
                "тело перезаписи deny-list: ожидается {entries:[{name, publicKeyPem}]}"));

        // 7–8. Валидация записей — внутри Replace (ArgumentException до персиста)
        //      → 400-тип с пояснением; снапшот не тронут.
        var entries = body.Entries
            .Select(e => (e.Name ?? string.Empty, e.PublicKeyPem ?? string.Empty))
            .ToList();
        try
        {
            denyList.Replace(entries);
        }
        catch (ArgumentException ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(ex.Message));
        }

        // 9. Успех: лог-событие §10 (число записей); исход — count.
        logger.LogInformation("Полная перезапись deny-list: {Count} записей (вызывающий {Caller})",
            entries.Count, cert.Name);
        return Result<DenyListReplaced>.Success(new DenyListReplaced(entries.Count));
    }
}
```

- [ ] **Шаг 2.4: прогнать тесты** (в фоне):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~DenyListReplaceServiceTests`
  Ожидание при первом прогоне после 2.1+2.2 — FAIL компиляции до 2.3; после
  2.3 — все кейсы PASS. Если какой-то кейс падает — чинить реализацию (не тест),
  кроме случая, когда тест противоречит §4.2 spec (тогда — вопрос пользователю).

- [ ] **Шаг 2.5: полный юнит-прогон** (в фоне):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Release`
  Ожидание: весь проект зелёный (регресс). Серия юнитов не создаёт
  docker-ресурсов — зачистка не нужна.

- [ ] **Шаг 2.6: коммит**
```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace add src/OwnS3.Replication/Transport/ReplicationErrors.cs src/OwnS3.Replication/DenyListReplaceService.cs src/tests/OwnS3.UnitTests/Replication/DenyListReplaceServiceTests.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace commit -m "owns3-denylist-replace: DenyListReplaceService — входящая перезапись по сертификату вызывающего (до-join контур) + ошибки + юниты"
```

---

### Task 3: грань — маршрут + хендлер + DI + api-интеграции (TDD)

**Файлы:**
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (peer-ветка `/replication/*`, после join-под-ветки, до `CredentialOf`)
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (ctor + `HandleDenyReplaceAsync`)
- Modify: `src/OwnS3.App/Program.cs` (DI, рядом с `JoinPeerService` — строки ~241–255)
- Test: `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs`

**Interfaces:**
- Consumes: `DenyListReplaceService.HandleIncoming` (Task 2);
  `S3ModelFactory.Create(context)`; `S3ErrorXmlWriter.Write(S3Error)`;
  `PeerAuthAppFactory`/`PeerAuthMaterial`/`SendPeerOnAsync`/`SendJoinOnAsync`
  (уже в тестовом файле).
- Produces: HTTP-контракт `PUT /replication/denylist` (§4.1/§4.2).

**Вход:** Task 2 слит. Устройство peer-ветки: `OWNS3-PEER-*` → join-под-ветка →
(новое: denylist-под-ветка) → `CredentialOf` → резолв пира. SigV4-ветка грани
не обслуживает метод: admin/root → 403 `AccessDenied` (A13, шаг 5 ветки),
replication-ключ соседа → `DispatchReplicationAsync` → 400 `InvalidArgument`
(«Unknown replication resource») — оба пути уже существуют в коде, api-кейсы
закрепляют как регресс (Task 4).

**Действие:** RED (api-кейсы падают: PUT уходит в peer-ветку → `CredentialOf`
→ резолв → 403) → реализация (middleware + хендлер + DI) → GREEN.

**Выход:** рабочий HTTP-метод на replication-грани; api-кейсы контура
доверия/тела зелёные.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~Api.PeerAuthScenarios` — зелёный.

**Связь со spec:** §4.1, §4.5, §5 фаза 3, критерии 1–5.

ВНИМАНИЕ (изоляция кейсов): deny-list живёт в temp-томе фабрики; кейсы,
меняющие список, — ТОЛЬКО на собственных фабриках (`using var factory = new
PeerAuthAppFactory();`), как join-кейсы. Кейсы, не меняющие список
(missing header, malformed, SigV4-изоляция) — можно на общей `Client`.

- [ ] **Шаг 3.1: failing api-кейсы** — в
  `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs`, в класс
  `PeerAuthScenarios` (после секции «удаление / deny-list / статус»):

```csharp
// ── PUT /replication/denylist: полная перезапись (глава 07 §2/§6) ──

// Тело перезаписи {entries:[{name, publicKeyPem}]} (camelCase).
private static string DenyBody(params (string Name, string PublicKeyPem)[] entries) =>
    JsonSerializer.Serialize(new
    {
        entries = entries.Select(e => new { name = e.Name, publicKeyPem = e.PublicKeyPem }).ToList(),
    }, Json);

// PUT /replication/denylist peer-подписью вызывающего; вызывающий по умолчанию —
// Own (сертификат подписан CA фабрики, не отозван; не обязан числиться соседом);
// certificate — подмена сертификата вызывающего (кейсы чужого CA).
private static Task<HttpResponseMessage> SendDenyReplaceOnAsync(OwnS3TestClient client,
    string body, ECDsa? signingKey = null, PeerCertificate? certificate = null,
    DateTimeOffset? at = null) =>
    SendPeerOnAsync(client, "PUT", "/replication/denylist", body,
        signingKey ?? PeerAuthMaterial.Own.Key, "owns3-own",
        certificate is null ? CertBase64Of(PeerAuthMaterial.Own.Certificate) : CertBase64Of(certificate),
        at);

[Fact]
public async Task DenyReplace_ValidCert_202_AdminListShowsNewEntries()
{
    // Arrange — СОБСТВЕННАЯ фабрика (кейс меняет deny-list); список из двух записей
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    var body = DenyBody(
        ("peer-x", PeerAuthMaterial.PeerXCert.PublicKeyPem),
        ("peer-y", PeerAuthMaterial.PeerYCert.PublicKeyPem));

    // Act — перезапись сертификатом owns3-own (подписан CA, не отозван)
    var response = await SendDenyReplaceOnAsync(client, body);

    // Assert — 202 без тела; состав заменён целиком с серверным revokedAtUnixMs
    response.StatusCode.Should().Be(HttpStatusCode.Accepted);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().BeEmpty();
    var list = await client.SendSignedAsync("GET", "/admin/replication/denylist",
        credentials: OwnS3TestClient.Admin());
    var entries = JsonDocument.Parse(
        await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement
        .GetProperty("entries");
    entries.GetArrayLength().Should().Be(2);
    entries.Should().Contain(e => e.GetProperty("name").GetString() == "peer-x"
        && e.GetProperty("publicKeyPem").GetString() == PeerAuthMaterial.PeerXCert.PublicKeyPem
        && e.GetProperty("revokedAtUnixMs").GetInt64() > 0);

    // Assert — следующий join от отозванного имени → 403 (критерий 1)
    (await SendJoinOnAsync(client, "http://peer-x:9000")).StatusCode
        .Should().Be(HttpStatusCode.Forbidden);
}

[Fact]
public async Task DenyReplace_EmptyEntries_202_AllowJoinAgain()
{
    // Arrange — СОБСТВЕННАЯ фабрика: список с peer-x блокирует join
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    (await SendDenyReplaceOnAsync(client,
        DenyBody(("peer-x", PeerAuthMaterial.PeerXCert.PublicKeyPem)))).StatusCode
        .Should().Be(HttpStatusCode.Accepted);
    (await SendJoinOnAsync(client, "http://peer-x:9000")).StatusCode
        .Should().Be(HttpStatusCode.Forbidden);

    // Act — перезапись пустым списком: снятие всех отзывов
    var response = await SendDenyReplaceOnAsync(client, DenyBody());

    // Assert — 202; список пуст; ранее отозванный пир проходит join (критерий 5)
    response.StatusCode.Should().Be(HttpStatusCode.Accepted);
    var cleared = await client.SendSignedAsync("GET", "/admin/replication/denylist",
        credentials: OwnS3TestClient.Admin());
    (await cleared.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("\"entries\":[]");
    (await SendJoinOnAsync(client, "http://peer-x:9000")).StatusCode
        .Should().Be(HttpStatusCode.OK);
}

[Fact]
public async Task DenyReplace_RevokedCaller_403_ListUnchanged()
{
    // Arrange — СОБСТВЕННАЯ фабрика: список, содержащий самого вызывающего
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    var bodyWithOwn = DenyBody(("owns3-own", PeerAuthMaterial.Own.Certificate.PublicKeyPem));
    (await SendDenyReplaceOnAsync(client, bodyWithOwn)).StatusCode
        .Should().Be(HttpStatusCode.Accepted);

    // Act — повторный вызов от отозванного вызывающего
    var response = await SendDenyReplaceOnAsync(client,
        DenyBody(("peer-x", PeerAuthMaterial.PeerXCert.PublicKeyPem)));

    // Assert — 403; список не изменился (отозванный не переписывает список)
    response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("AccessDenied");
    var list = await client.SendSignedAsync("GET", "/admin/replication/denylist",
        credentials: OwnS3TestClient.Admin());
    var entries = JsonDocument.Parse(
        await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement
        .GetProperty("entries");
    entries.GetArrayLength().Should().Be(1);
    entries[0].GetProperty("name").GetString().Should().Be("owns3-own");
}

[Fact]
public async Task DenyReplace_CertNotFromTrustAnchor_403()
{
    // Arrange — СОБСТВЕННАЯ фабрика; вызывающий с сертификатом чужого CA (peer-y)
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());

    // Act
    var response = await SendDenyReplaceOnAsync(client, DenyBody(),
        certificate: PeerAuthMaterial.PeerYCert);

    // Assert — 403 XML AccessDenied; список не менялся (критерий 3)
    response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("AccessDenied");
}

[Fact]
public async Task DenyReplace_NoTrustAnchor_403_ManualMode()
{
    // Arrange — фабрика без trust-anchor (ручной режим состава)
    using var factory = new PeerAuthNoTrustAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());

    // Act
    var response = await SendDenyReplaceOnAsync(client, DenyBody());

    // Assert — 403 (как join; критерий 6)
    response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
}

[Fact]
public async Task DenyReplace_MissingCertHeader_400()
{
    // Act — PUT без заголовка X-OwnS3-Peer-Certificate: прямой вызов
    // SendPeerOnAsync с certificateBase64: null (общая фабрика: список не меняется)
    var response = await SendPeerOnAsync(Client, "PUT", "/replication/denylist",
        DenyBody(), PeerAuthMaterial.Own.Key, "owns3-own", null);

    // Assert — 400 JSON с пояснением (как join)
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("X-OwnS3-Peer-Certificate");
}

[Fact]
public async Task DenyReplace_MalformedBody_400_ListUntouched()
{
    // Arrange — СОБСТВЕННАЯ фабрика; действующий отзыв
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    (await SendDenyReplaceOnAsync(client,
        DenyBody(("keep", PeerAuthMaterial.PeerXCert.PublicKeyPem)))).StatusCode
        .Should().Be(HttpStatusCode.Accepted);

    // Act — тело не-JSON
    var response = await SendDenyReplaceOnAsync(client, "{мусор");

    // Assert — 400 JSON; список прежний (критерий 4)
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("error");
    var list = await client.SendSignedAsync("GET", "/admin/replication/denylist",
        credentials: OwnS3TestClient.Admin());
    (await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("keep");
}

[Fact]
public async Task DenyReplace_InvalidEntries_400_ListUntouched()
{
    // Arrange — СОБСТВЕННАЯ фабрика; невалидный состав: дубли имён
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    var body = DenyBody(("dup", PeerAuthMaterial.PeerXCert.PublicKeyPem),
        ("dup", PeerAuthMaterial.PeerYCert.PublicKeyPem));

    // Act
    var response = await SendDenyReplaceOnAsync(client, body);

    // Assert — 400 с пояснением; список не менялся (валидация до персиста)
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("error");
}

[Fact]
public async Task DenyReplace_BrokenPemEntry_400()
{
    // Arrange — СОБСТВЕННАЯ фабрика; запись с битым PEM
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    var body = DenyBody(("broken", "-----BEGIN PUBLIC KEY-----\nZZZ\n-----END PUBLIC KEY-----\n"));

    // Act
    var response = await SendDenyReplaceOnAsync(client, body);

    // Assert — 400 с пояснением
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("error");
}

[Fact]
public async Task DenyReplace_MalformedAuthorization_400()
{
    // Arrange — СОБСТВЕННАЯ фабрика; мусорная структура OWNS3-PEER-Authorization
    // (валидный префикс схемы) при валидном сертификате и теле
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    using var request = new HttpRequestMessage(HttpMethod.Put, "/replication/denylist");
    request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(DenyBody()));
    request.Headers.TryAddWithoutValidation("Authorization", "OWNS3-PEER-мусор");
    request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate",
        CertBase64Of(PeerAuthMaterial.Own.Certificate));

    // Act
    var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

    // Assert — 400 AuthorizationHeaderMalformed XML (та же пара исходов, что join)
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("AuthorizationHeaderMalformed");
}

[Fact]
public async Task DenyReplace_GarbageBody_UntrustedCert_403Not400()
{
    // Arrange — СОБСТВЕННАЯ фабрика; мусорное тело + сертификат чужого CA:
    // порядок проверок — доверие ДО тела (принцип 7, критерий 4)
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());

    // Act
    var response = await SendDenyReplaceOnAsync(client, "{мусор",
        certificate: PeerAuthMaterial.PeerYCert);

    // Assert — отказ доверия (403), не 400 тела
    response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("AccessDenied");
}
```

- [ ] **Шаг 3.2: прогнать — RED** (в фоне):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~Api.PeerAuthScenarios`
  Ожидание: новые DenyReplace-кейсы падают (маршрута нет: peer-ветка после
  join-под-ветки зовёт `CredentialOf`/резолв → 403/400 не те).

- [ ] **Шаг 3.3: ветка в S3Middleware** — в
  `src/OwnS3.App/Pipeline/S3Middleware.cs`, в peer-ветке `/replication/*`
  СРАЗУ после join-под-ветки (`if (HttpMethods.IsPost(method) && ... "/replication/join") {...}`)
  и ДО `PeerRequestVerifier.CredentialOf`:

```csharp
// Deny-replace-под-ветка (глава 07 §2/§6): PUT /replication/denylist —
// второй до-join запрос грани: вызывающий — оркестратор, не обязан числиться
// соседом; аутентификация по сертификату вызывающего (как у join), ДО
// резолва пира по составу.
if (HttpMethods.IsPut(method) && context.Request.Path.Value == "/replication/denylist")
{
    await replicationHandlers.HandleDenyReplaceAsync(context, requestId, options.Value.HostId);
    return;
}
```

- [ ] **Шаг 3.4: хендлер** — в
  `src/OwnS3.App/Handlers/ReplicationHandlers.cs`:
  1) расширить primary-ctor (после `JoinPeerService joinService`):
     `DenyListReplaceService denyReplaceService`;
  2) после `HandleJoinAsync` добавить:

```csharp
// PUT /replication/denylist (глава 07 §2/§6): входящая полная перезапись
// deny-list — до-join аутентификация по сертификату X-OwnS3-Peer-Certificate
// и подписи запроса (DenyListReplaceService.HandleIncoming); успех → 202 без
// тела; отказ доверия/подписи → 403 XML кода каталога (malformed Authorization
// → 400 XML — та же пара исходов, что у join-ветки); malformed/невалидное
// тело → 400 JSON {error}; нет заголовка сертификата → 400 JSON.
public async Task HandleDenyReplaceAsync(HttpContext context, string requestId, string? errorHostId)
{
    // Модель — та же сборка (верификатор читает только заголовки/путь; хэш
    // тела — из заголовка схемы); тело читается целиком в память ДО сервиса.
    var model = S3ModelFactory.Create(context);
    var certificateHeader = context.Request.Headers["X-OwnS3-Peer-Certificate"].FirstOrDefault();
    if (string.IsNullOrEmpty(certificateHeader))
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsJsonAsync(new { error = "X-OwnS3-Peer-Certificate header required" },
            context.RequestAborted);
        return;
    }
    using var buffer = new MemoryStream();
    await context.Request.Body.CopyToAsync(buffer, context.RequestAborted);
    var outcome = denyReplaceService.HandleIncoming(model, certificateHeader, buffer.ToArray());
    if (outcome.IsSuccess)
    {
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        return;
    }
    switch (outcome.Error)
    {
        case DenyReplaceRefusedException refused:
            // Отказ подписи — код каталога (SignatureDoesNotMatch/RequestTimeTooSkewed/…);
            // прочие отказы доверия — AccessDenied; malformed Authorization — 400
            // (выравнивание с join-веткой грани).
            var code = refused.ErrorCode ?? OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied;
            context.Response.StatusCode =
                code == OwnS3.Protocol.Errors.S3ErrorCode.AuthorizationHeaderMalformed
                    ? StatusCodes.Status400BadRequest
                    : StatusCodes.Status403Forbidden;
            context.Response.ContentType = "application/xml";
            await context.Response.WriteAsync(OwnS3.Protocol.Xml.S3ErrorXmlWriter.Write(
                new OwnS3.Protocol.Xml.S3Error(code, Resource: context.Request.Path,
                    RequestId: requestId, HostId: errorHostId, MessageOverride: refused.Message)),
                context.RequestAborted);
            return;
        case DenyReplaceBadRequestException bad:
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = bad.Message }, context.RequestAborted);
            return;
        default:
            // Неожиданный тип отказа — прежний путь catch-all middleware (500 XML).
            throw outcome.Error!;
    }
}
```

- [ ] **Шаг 3.5: DI** — в `src/OwnS3.App/Program.cs`:
  1) сразу после регистрации `JoinPeerService` (заодно с теми же
     `peerIdentity`/`peerTrust` замыканиями):

```csharp
// Входящая перезапись deny-list (глава 07 §2): тот же identity/trust-материал,
// что join (до-join контур replication-грани).
builder.Services.AddSingleton(sp => new OwnS3.Replication.DenyListReplaceService(
    peerIdentity, peerTrust,
    sp.GetRequiredService<OwnS3.Replication.DenyList>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<OwnS3.Replication.DenyListReplaceService>>()));
```

  2) в регистрацию `ReplicationHandlers` добавить аргумент
     `sp.GetRequiredService<OwnS3.Replication.DenyListReplaceService>()`
     (после `JoinPeerService`).

- [ ] **Шаг 3.6: сборка + прогон — GREEN** (в фоне):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~Api.PeerAuthScenarios`
  Ожидание: все кейсы (включая старые) зелёные; ворнингов нет
  (`TreatWarningsAsErrors` — сборка упадёт при любом).

- [ ] **Шаг 3.7: коммит**
```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace add src/OwnS3.App/Pipeline/S3Middleware.cs src/OwnS3.App/Handlers/ReplicationHandlers.cs src/OwnS3.App/Program.cs src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace commit -m "owns3-denylist-replace: PUT /replication/denylist — ветка peer-конвейера, хендлер, DI + api-интеграции"
```

---

### Task 4: регресс-изоляция — SigV4-пути и admin-операции

**Файлы:**
- Test: `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs` (добавить 2 кейса)

**Interfaces:** Consumes: `EnsureKeyTargetOnAsync`, `ReplicationCreds`,
`client.SendSignedAsync` (уже в файле); HTTP-контракт Task 3.

**Вход:** Task 3 слит (метод живой).

**Действие:** добавить регресс-кейсы закрытости SigV4-пути (оба уже
гарантируются существующим кодом граней — кейсы закрепляют контракт):
- admin/root SigV4 → SigV4-ветка replication-грани: политика `admin` ≠
  `replication` → 403 `AccessDenied` (A13-изоляция);
- replication-ключ числящегося соседа → валидная SigV4, `sourceName`
  резолвится → `DispatchReplicationAsync`: PUT не входит в пост-join
  диспетчеризацию → 400 `InvalidArgument`.

**Выход:** SigV4-изоляция метода закреплена тестами; существующие
deny-list-операции подтверждены зелёными без правок ожиданий.

**Проверка:** прогон `Api.PeerAuthScenarios` целиком + соседних
`ReplicationAdminScenarios` — зелёные.

**Связь со spec:** §4.1 (SigV4-ветка не обслуживает метод), §5 фаза 3
(регресс-кейсы), критерии 6/8.

- [ ] **Шаг 4.1: кейсы** — в `Api/PeerAuthScenarios.cs` (секция deny-replace):

```csharp
[Fact]
public async Task DenyReplace_SigV4Admin_403AccessDenied()
{
    // Arrange — СОБСТВЕННАЯ фабрика; admin-креды SigV4 (кейс списка не меняет)
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());

    // Act — PUT /replication/denylist по SigV4 от admin (A13: политика грани
    // строго replication)
    var response = await client.SendSignedAsync("PUT", "/replication/denylist",
        payloadString: PayloadHash(DenyBody()), body: Encoding.UTF8.GetBytes(DenyBody()),
        credentials: OwnS3TestClient.Admin());

    // Assert — 403 AccessDenied (критерий 8)
    response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("AccessDenied");
}

[Fact]
public async Task DenyReplace_ReplicationKeyNeighbor_400InvalidArgument()
{
    // Arrange — СОБСТВЕННАЯ фабрика; состав с legacy-соседом peer-a
    // (replication-креды числятся в targets)
    using var factory = new PeerAuthAppFactory();
    var client = new OwnS3TestClient(factory.CreateClient());
    await EnsureKeyTargetOnAsync(client);

    // Act — PUT по SigV4 репликационным ключом числящегося соседа (PUT не
    // входит в пост-join диспетчеризацию грани)
    var response = await client.SendSignedAsync("PUT", "/replication/denylist",
        payloadString: PayloadHash(DenyBody()), body: Encoding.UTF8.GetBytes(DenyBody()),
        credentials: ReplicationCreds);

    // Assert — 400 InvalidArgument «Unknown replication resource» (критерий 8)
    response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
        .Should().Contain("InvalidArgument");
}
```

- [ ] **Шаг 4.2: прогон класса + соседних админ-сценариев** (в фоне):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName~Api.PeerAuthScenarios|FullyQualifiedName~Api.ReplicationAdminScenarios"`
  Ожидание: всё зелёное; в частности существующие
  `DenyRemove_AllowsJoinAgain`, `DeleteTarget_KeyRevoke_BlocksJoin_202`,
  `DenyList_Get_ListsEntries` не изменили ожиданий (принцип 4 spec).
  После серии — зачистки не требует (in-memory хосты), но убедиться, что
  docker-контейнеров не поднималось.

- [ ] **Шаг 4.3: коммит**
```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace add src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace commit -m "owns3-denylist-replace: регресс-изоляция SigV4-путей к PUT /replication/denylist (admin 403 A13, replication-сосед 400)"
```

---

### Task 5: docker-E2E + полные прогоны + зачистка

**Файлы:**
- Test: `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs` (добавить хелпер + 2 кейса)

**Interfaces:** Consumes: `OwnS3MeshE2eEnvironment.RunScenarioAsync(slug,
nodes, scenario, fullMeshTargets, ct, peerAuth)`; `MeshPeerAuthMaterial`
(`Create(n, caName)`, `Nodes[i].Cert/.CertJson/.PrivatePem`, `CertNameOf(i)`);
`MeshClients.*` (`AdminClient`, `CreateBucketAsync`, `PutObjectAsync`,
`GetObjectOrNullAsync`, `PollUntilAsync`); `AdminJoinAsync`,
`AdminDenyListAsync` (уже в файле); `PeerRequestSigner.Sign`.

**Вход:** Tasks 1–4 слиты; docker доступен; образ `owns3:e2e` соберётся сам
(`OwnS3E2eImage.BuildOnceAsync` — publish на хосте + runtime-COPY, канон
AGENTS.md; `PGW_TEST_E2E_NOBUILD=1` не использовать).

**Действие:** два E2E-кейса (живая mesh-механика перезаписи), затем полные
прогоны сериями с зачисткой после каждой.

**Выход:** E2E-подтверждение критериев 5/7 (join блокируется/разрешается
перезаписью; действующая репликация не рвётся) + зелёные полные прогоны.

**Проверка:** docker-E2E серия `E2e.Replication.PeerAuthScenarios` зелёная;
затем весь `E2e.Replication` зелёный; после каждой серии — остаточных
`os3m-*` контейнеров/сетей нет.

**Связь со spec:** §5 фаза 4, критерии 5/7/9.

- [ ] **Шаг 5.1: E2E-хелпер + кейсы** — в
  `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs`,
  в класс `PeerAuthScenarios` (рядом с `SendEdgeJoinAsync`):

```csharp
/// <summary>Прямой PUT /replication/denylist на replication-грань узла с хоста
/// (peer-подпись §4.2 вручную — по образцу SendEdgeJoinAsync; вызывающий —
/// сертификат из материала mesh, подписанный CA, не обязан числиться соседом;
/// дата — настоящее время контейнера).</summary>
private static async Task<HttpResponseMessage> SendEdgeDenyReplaceAsync(OwnS3MeshE2eEnvironment env,
    int node, ECDsa signingKey, string credential, string certificateBase64,
    IReadOnlyList<(string Name, string PublicKeyPem)> entries, CancellationToken ct)
{
    // Arrange — тело и заголовки схемы OWNS3-PEER (§4.2)
    var body = JsonSerializer.Serialize(new
    {
        entries = entries.Select(e => new { name = e.Name, publicKeyPem = e.PublicKeyPem }).ToList(),
    });
    var payloadHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body)))
        .ToLowerInvariant();
    var at = DateTimeOffset.UtcNow;
    var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
    var host = new Uri(env.NodeHostUrl(node)).Authority;
    var (authorization, _, _) = PeerRequestSigner.Sign(signingKey, credential, "PUT",
        "/replication/denylist", "",
        new List<(string, string)>
        {
            ("host", host),
            ("x-owns3-peer-date", peerDate),
            ("x-owns3-peer-content-sha256", payloadHash),
        }, payloadHash, at);

    // Act — запрос на replication-грань
    using var request = new HttpRequestMessage(HttpMethod.Put,
        $"{env.NodeHostUrl(node)}/replication/denylist");
    request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
    request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
    request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
    request.Headers.TryAddWithoutValidation("Authorization", authorization);
    request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate", certificateBase64);
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
    return await http.SendAsync(request, ct);
}

// ── 6. полная перезапись deny-list (PUT /replication/denylist, глава 07 §2) ──

[Fact]
public async Task DenyReplaceFull_BlocksJoinThenEmptyListAllows()
{
    DockerTrait.SkipIfUnavailable();
    // 2 ноды с identity+CA; вызывающий перезаписи — сертификат узла-0
    // (оркестратор, не обязан числиться соседом узла-1). Очистку списка
    // выполняет узел-1 СВОИМ сертификатом: после первой перезаписи узел-0
    // отозван на узле-1 — по §4.2 п.5 отозванный не переписывает список
    // (иначе 403); узел-1 не отозван у самого себя, серт того же CA.
    var auth = MeshPeerAuthMaterial.Create(2, "os3m-denyfull-ca");
    await OwnS3MeshE2eEnvironment.RunScenarioAsync("denyfull", 2, async env =>
    {
        var ct = TestContext.Current.CancellationToken;
        using var orchestratorKey = ECDsa.Create();
        orchestratorKey.ImportFromPem(auth.Nodes[0].PrivatePem);
        var orchestratorCert = Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[0].CertJson));
        using var node1Key = ECDsa.Create();
        node1Key.ImportFromPem(auth.Nodes[1].PrivatePem);
        var node1Cert = Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[1].CertJson));

        // Act — перезапись на узле-1 списка, содержащего узел-0
        using var put = await SendEdgeDenyReplaceAsync(env, 1, orchestratorKey,
            auth.CertNameOf(0), orchestratorCert,
            [(auth.CertNameOf(0), auth.Nodes[0].Cert.PublicKeyPem)], ct);

        // Assert — 202; admin-грань узла-1 видит запись (имя + ключ)
        put.StatusCode.Should().Be(HttpStatusCode.Accepted);
        (await AdminDenyListAsync(env, 1, ct)).EnumerateArray().Should().ContainSingle(e =>
            e.GetProperty("name").GetString() == auth.CertNameOf(0)
            && e.GetProperty("publicKeyPem").GetString() == auth.Nodes[0].Cert.PublicKeyPem
            && e.GetProperty("revokedAtUnixMs").GetInt64() > 0);

        // Assert — join узла-0 → узла-1 отказан (deny-list решает будущие join)
        using (var denied = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
            denied.StatusCode.Should().Be(HttpStatusCode.Forbidden);

        // Act — перезапись пустым списком (снятие всех отзывов одним вызовом);
        // вызывающий — узел-1 (узел-0 отозван и получил бы 403)
        using var clear = await SendEdgeDenyReplaceAsync(env, 1, node1Key,
            auth.CertNameOf(1), node1Cert, [], ct);

        // Assert — 202; список пуст; join снова проходит
        clear.StatusCode.Should().Be(HttpStatusCode.Accepted);
        (await AdminDenyListAsync(env, 1, ct)).GetArrayLength().Should().Be(0);
        using (var rejoined = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
            rejoined.StatusCode.Should().Be(HttpStatusCode.OK);
    }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
}

[Fact]
public async Task DenyReplace_DoesNotBreakActiveLinks()
{
    DockerTrait.SkipIfUnavailable();
    // 3 ноды: join (0,1) и (0,2); перезапись на узле-0 списка без узла-1
    // (и с узлом-2) — действующие линки продолжают доставлять
    var auth = MeshPeerAuthMaterial.Create(3, "os3m-denyactive-ca");
    await OwnS3MeshE2eEnvironment.RunScenarioAsync("denyactive", 3, async env =>
    {
        var ct = TestContext.Current.CancellationToken;
        var bucket = $"da-{env.RunTag}";
        await MeshClients.CreateBucketAsync(env, 0, bucket, ct);

        // Arrange — активная mesh 0→1, 0→2; k1 доставлен обоим
        foreach (var b in new[] { 1, 2 })
        {
            using var join = await AdminJoinAsync(env, 0, env.NodeInternalUrl(b), ct);
            join.StatusCode.Should().Be(HttpStatusCode.OK, $"join пары (0,{b})");
        }
        var k1 = "denyactive-k1"u8.ToArray();
        await MeshClients.PutObjectAsync(env, 0, bucket, "k1", k1, ct);
        await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) is { } on1
                    && on1.SequenceEqual(k1)
                    && await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "k1", ct) is { } on2
                    && on2.SequenceEqual(k1),
            "k1 до узлов 1 и 2 до перезаписи");

        // Act — перезапись deny-list узла-0: список БЕЗ узла-1 (линк с ним —
        // действующий) и С узлом-2 (даже отозванный в списке — линк жив)
        using var orchestratorKey = ECDsa.Create();
        orchestratorKey.ImportFromPem(auth.Nodes[0].PrivatePem);
        using var put = await SendEdgeDenyReplaceAsync(env, 0, orchestratorKey,
            auth.CertNameOf(0), Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[0].CertJson)),
            [(auth.CertNameOf(2), auth.Nodes[2].Cert.PublicKeyPem)], ct);

        // Assert — 202; доставка продолжает работать по обоим линкам (критерий 7)
        put.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var k2 = "denyactive-k2"u8.ToArray();
        await MeshClients.PutObjectAsync(env, 0, bucket, "k2", k2, ct);
        await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k2", ct) is { } on1
                    && on1.SequenceEqual(k2),
            "k2 до узла-1 после перезаписи (список без него — линк не тронут)");
        await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "k2", ct) is { } on2
                    && on2.SequenceEqual(k2),
            "k2 до узла-2 после перезаписи (узел в списке — действующий линк всё равно жив)");

        // Assert — состав targets узла-0 не тронут перезаписью (оба соседа на месте)
        (await StatusNeighborsAsync(env, 0, ct)).EnumerateArray().Should().HaveCount(2);
    }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
}
```

- [ ] **Шаг 5.2: сборка проекта интеграций** (в фоне; ловит ворнинги до
  долгого docker-прогона):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/OwnS3.IntegrationTests -c Release`
  Ожидание: Build succeeded, 0 Warning(s)/Error(s).

- [ ] **Шаг 5.3: docker-E2E серия PeerAuthScenarios** (в фоне, задача долгая —
  контроль по task_id каждые ≤30 с; фазы дольше 60 с сопровождаются `[PHASE]`
  в журнале — окружение делает само):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build --filter FullyQualifiedName~E2e.Replication.PeerAuthScenarios`
  Ожидание: все кейсы класса зелёные (включая новые 2). Упавший сценарий —
  разбор по логам `/tmp/pgw-e2e-artifacts-<runId>/` БЕЗ перезапуска (канон
  docs/e2e-launch.md); контейнеры упавшего окружения оставлены для разбора
  (README-cleanup.txt) — после разбора удалить вручную.

- [ ] **Шаг 5.4: зачистка после серии** (страховочный гейт; основной —
  самоочистка окружения, ассерт чистоты уже в тесте):
```bash
leftovers=$(docker ps -aq --filter "name=os3m-"); [ -n "$leftovers" ] && docker rm -f $leftovers; \
nets=$(docker network ls --filter "name=os3m-en-" --format "{{.Name}}"); [ -n "$nets" ] && docker network rm $nets; \
echo "cleanup done: containers=[$leftovers] nets=[$nets]"
```
  Ожидание: обе переменные пусты (или перечислено и удалено то, что нашлось).

- [ ] **Шаг 5.5: полная replication-E2E серия** (регресс соседних сценариев;
  в фоне; зачистка после — той же командой шага 5.4):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build --filter FullyQualifiedName~E2e.Replication`
  Ожидание: вся серия зелёная.

- [ ] **Шаг 5.6: финальные полные прогоны (серия за серией, зачистка после
  каждой docker-серии)**:
  1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` —
     0 warnings (TreatWarningsAsErrors), включает OwnS3.* и оба тест-проекта.
  2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~OwnS3.UnitTests` — зелёный.
  3. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~OwnS3.IntegrationTests&FullyQualifiedName!~E2e"` —
     зелёный (api-серии; docker-E2E выключены — скип).
  4. Зачистка (шаг 5.4) — после шагов 5.3/5.5 достаточно; здесь убедиться,
     что остатков нет.

- [ ] **Шаг 5.7: коммит**
```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace add src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-owns3-denylist-replace commit -m "owns3-denylist-replace: docker-E2E — перезапись блокирует/разрешает join, действующие линки не рвутся"
```

---

## Self-Review (трассировка spec → задачи)

| Требование spec | Задача/шаг |
|---|---|
| §4.3 `DenyList.Replace` (валидация до персиста, tmp+rename, лок) | Task 1 |
| §4.4 сервис `DenyListReplaceService` (ctor/Result/исключения) | Task 2 |
| §4.2 п.1 (ручной режим → 403) | Task 2 (NoIdentity/NoTrust), Task 3 (api NoTrustAnchor) |
| §4.2 п.2 (нет заголовка серта → 400 JSON) | Task 3 (хендлер; api MissingCertHeader) |
| §4.2 п.3 (серт не парсится → 403) | Task 2 (UnparsableCert) |
| §4.2 п.4 (не тот CA/истёк → 403) | Task 2 (OtherCa/Expired), Task 3 (api) |
| §4.2 п.5 (вызывающий в deny → 403, список не менялся) | Task 2 (ByName/ByKey), Task 3 (api RevokedCaller) |
| §4.2 п.6 (подпись: 403 коды каталога / 400 malformed) | Task 2 (Mismatch/Skew/MalformedAuth), Task 3 (api MalformedAuthorization) |
| §4.2 п.7 (тело не-JSON/нет entries → 400) | Task 2 (MalformedJson/EntriesMissing), Task 3 (api MalformedBody) |
| §4.2 п.8 (записи невалидны → 400 до персиста) | Task 1 (Theory), Task 2 (Theory), Task 3 (api InvalidEntries/BrokenPem) |
| §4.2 п.9 (202, tmp+rename, серверное время) | Task 1 (персист/время), Task 3 (202 api), Task 5 (E2E 202) |
| §4.1 маршрут (peer-ветка до резолва) | Task 3 (middleware) |
| §4.1 SigV4: admin → 403 A13; replication-сосед → 400 | Task 4 |
| §4.5 хендлер в `ReplicationHandlers`, DI в Program | Task 3 |
| §4.6 лог-события (перезапись/отказы) | Task 2 (LogInformation/LogWarning) |
| §4.7 arch-зеркалирование | Task 0 (сверка) + Tasks 1–5 |
| §5 фаза 4 E2E: блок join/снятие; линк не рвётся | Task 5 (оба кейса) |
| Критерий 9 (сборка без ворнингов, полные прогоны, teardown) | Task 5 (шаги 5.6, 5.3–5.5) |
| Границы §6 (не вводится/не меняется) |никакие файлы вне карты не правятся; admin-операции и join не трогаются; регресс — Tasks 4–5 |

Известные осознанные упрощения плана (не пробелы): тесты Task 2/3 не
проверяют отсутствие файла denylist.json в кейсе «Replace НЕ вызван» при
пустом исходном списке — вместо этого фиксируется неизменность снапшота при
действующей записи (сильнее и стабильнее);Credential вызывающего не обязан
совпадать с cert.Name (нюанс §4.2) — покрыто неявно всеми кейсами
(SendPeerOnAsync передаёт credential отдельно от сертификата).
