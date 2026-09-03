# t91-kafka-portalloc-race — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Устранить гонку параллельного выделения клиентских портов брокеров KafkaWorker — глобальный клэйм `/kafkaworker/locks/portalloc` (порт t90) + чтение занятости portalloc соседей в обеих секциях довыделения (ProvisioningProcess K1, AddBrokerProcess).

**Architecture:** Буквальный порт `src/PgWorker.Etcd/Coordination/PortAllocLock.cs` в `KafkaWorker.Etcd.Coordination` (txn `version==0` + put-with-lease TTL 15 с, локальный busy-гейт параллельных тиков, release del-under-ValueEqual + revoke) + новый `PortAllocIndex` (busy из `/kafkaworker/portalloc/*` чужих кластеров). Секции довыделения K1/EnsurePorts — целиком под клэймом с ранними выходами до лока; «не взял» → журнальная фаза `waiting-portalloc-lock` + Result.Success без мутаций. Контракты arch/15 §4 и arch/16 §2.1/§3.2/§5 A/F обновляются ДО кода (arch-first).

**Tech Stack:** .NET 10, C# LangVersion=latest, Nullable=enable, TreatWarningsAsErrors=true; xUnit + FluentAssertions; Testcontainers (интеграционный race-тест на реальном etcd).

**Spec:** `docs/superpowers/2026-09-03-kafka-portalloc-race/spec.md` (в этой же ветке; план аргументируется от spec — исполнитель читает оба).

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; версионирование пакетов централизованное (`Directory.Packages.props`) — НОВЫХ пакетов нет.
- Язык: документация/комментарии — русский; идентификаторы — английские.
- Тесты: docker-порты динамические (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`) — никаких литералов хост-портов; интеграционный race-тест не поднимает брокеров (BrokerBootSec не участвует, бюджет ретрая 10 с).
- Тесты — комментарии по нотации AAA (// Arrange / // Act / // Assert).
- Работа строго в worktree `feat-kafka-portalloc-race`; коммит после каждой задачи; пуш — только по требованию владельца репозитория.
- Эталоны для порта: `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`, `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs`, `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (TxnFault), `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs`, `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs` (EtcdCollection) + `PortAllocLockRaceTests.cs` (всё уже в main, читать при исполнении).

---

### Task 1: Контракты arch/15 §4 + arch/16 §2.1/§3.2/§5 A/F (arch-first)

**Вход:** spec §3.4 (точный состав правок), файлы `arch/15-kafka-clusters.md`, `arch/16-kafkaworker.md` в актуальном main.
**Действие:** правка четырёх разделов двух контрактов по тексту spec §3.4 — ДО любого кода.
**Выход:** контракты описывают глобальный клэйм и полную карту занятости.
**Проверка:** grep новых строк; визуальный diff.
**Связь со spec:** §3.4 п.1–5, §4 фаза 1.

**Files:**
- Modify: `arch/15-kafka-clusters.md` (§4, таблица координации)
- Modify: `arch/16-kafkaworker.md` (§2.1 пункт «Placement/порты»; §3.2 таблица; §5 A шаг K1; §5 F первый абзац)

- [ ] **Step 1: arch/15 §4 — новая строка таблицы координации**

В `arch/15-kafka-clusters.md` §4, в таблице сразу ПОСЛЕ строки `| /kafkaworker/portalloc/<C> | ... |` добавить строку:

```markdown
| `/kafkaworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** (t91, arch/16 §2.1): взаимоисключение секции довыделения клиентских портов «чтение занятости → выбор портов → запись `/kafkaworker/portalloc/<C>`» (provision K1 / add-broker) — пер-кластерные клэймы кросс-кластерную гонку не закрывают. Value: `{"instance":"<id>","since_unix":…}`. Захват txn `version==0` + put-with-lease; освобождение по завершении секции (del + revoke lease), смерть держателя — TTL. Не взял → InProgress (следующий тик). Без keepalive: секция короткая (единицы секунд ≪ TTL). |
```

Перечень «Панель читает из `/kafkaworker/` только `rotations/`, `rebalances/`, `reassignments/`, `regens/`» под таблицей НЕ менять (`locks/` панелью не читается).

- [ ] **Step 2: arch/16 §2.1 — полная карта занятости + клэйм**

В `arch/16-kafkaworker.md` §2.1 найти пункт, начинающийся словами `- **Placement/порты**: анти-аффинити нод по docker-хостам (порт` — заменить его целиком на:

```markdown
- **Placement/порты**: анти-аффинити нод по docker-хостам (порт
  PlacementPlanner PgWorker); порт-аллокатор из диапазона `16000–16999`
  (**1 клиентский порт на ноду**), закрепление в
  `/kafkaworker/portalloc/<C>`; лимиты контейнера из `resources` (cpu/mem;
  disk-заявка — инфо, квоты томов — roadmap). Занятость для довыделения =
  docker-публикации ∪ записи portalloc ВСЕХ чужих кластеров
  (`/kafkaworker/portalloc/*`, кроме своего — свой переиспользуется как
  закрепление; закрывает кросс-кластерную коллизию, включая окно «portalloc
  записан, контейнеры ещё не созданы»). **Глобальный portalloc-клэйм** (t91):
  довыделение новых портов (недобор нод, не переиспользование закреплений) —
  глобально взаимоисключающая секция «чтение занятости → выбор портов →
  запись», выполняется только держателем `/kafkaworker/locks/portalloc`
  (arch/15 §4; txn `version==0` + put-with-lease TTL 15 с). Не взял →
  InProgress (следующий тик ~5 с); смерть держателя гасит lease ≤ 15 с —
  takeover без оператора. Полностью закреплённый portalloc (rebuild, ранний
  выход без записи) клэйма не требует. Касается всех точек довыделения:
  provision K1, add-broker (§5 A/F).
```

- [ ] **Step 3: arch/16 §3.2 — строка пишемого ключа**

В §3.2 (таблица «Пишемые ключи») сразу ПОСЛЕ строки `| /kafkaworker/regens/<C> | ... |` добавить:

```markdown
| `/kafkaworker/locks/portalloc` | захват секции довыделения портов (t91, §2.1) | `{"instance":"<id>","since_unix":…}` — lease TTL 15 с, txn `version==0` + put-with-lease; del + revoke lease по завершении секции (arch/15 §4) |
```

- [ ] **Step 4: arch/16 §5 A (K1) и §5 F — пометки клэйма**

В §5 A (блок кода фаз A/K0–K6) строку фазы K1

```text
K1 план: placement + порт-аллокация (закрепление portalloc);
```

заменить на:

```text
K1 план: placement + порт-аллокация (закрепление portalloc) — под
   глобальным portalloc-клэймом /kafkaworker/locks/portalloc (§2.1):
   занят = docker ∪ portalloc чужих кластеров; не взял клэйм → journal
   waiting-portalloc-lock (InProgress, следующий тик);
```

В §5 F первый абзац (начинается `` `brokers/<b>/state=NOT_INITIALIZED` у Active-кластера: план (host/порт;``) — после слов «план (host/порт; `role=broker`)» вставить «(добор портов — под глобальным portalloc-клэймом §2.1; не взял → journal waiting-portalloc-lock)».

- [ ] **Step 5: Проверка правок**

Run: `git diff --stat` и `git diff arch/`
Expected: изменены ровно 2 файла; в diff видны все 5 правок (arch/15 ×1, arch/16 ×4).

- [ ] **Step 6: Commit**

```bash
git add arch/15-kafka-clusters.md arch/16-kafkaworker.md
git commit -m "t91: контракт arch/15 §4 + arch/16 §2.1/§3.2/§5 A/F — глобальный portalloc-клэйм /kafkaworker/locks/portalloc и карта занятости docker ∪ portalloc чужих кластеров (arch-first, по spec §3.4)"
```

---

### Task 2: PortAllocLock — класс, FakeEtcd.TxNFault, юнит-тесты, DI

**Вход:** эталон `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`; эталон хука `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (TxnFault, строка ~32 и вставка в TxnAsync); `ClaimStore` (`InstanceId`, паттерн leased-ключа); `Fakes.FakeEtcd` (KafkaWorker.UnitTests/Provisioning); spec §3.1.
**Действие:** TDD — юнит-тесты (порт набора t90) → класс-порт с kafka-префиксом → доработка фейка (TxnFault по эталону PgWorker) → DI-регистрация.
**Выход:** `KafkaWorker.Etcd.Coordination.PortAllocLock` + `PortLockBusyException`; зелёные юнит-тесты; зарегистрированный DI-синглтон.
**Проверка:** `dotnet test` фильтром по PortAllocLockTests.
**Связь со spec:** §3.1, §4 фаза 2, §7 критерий 2/5.

**Files:**
- Create: `src/KafkaWorker.Etcd/Coordination/PortAllocLock.cs`
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` (FakeEtcd — хук TxnFault)
- Test: `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs`
- Modify: `src/KafkaWorker.App/Program.cs` (DI-регистрация PortAllocLock)

**Interfaces:**
- Consumes: `KafkaWorker.Core.Result<T>`, `KafkaWorker.Etcd.Client.IEtcdGateway` (`LeaseGrantAsync(endpoint, ttlSec, ct) → Result<long>`, `LeaseRevokeAsync(endpoint, lease, ct) → Result`, `TxnAsync(endpoint, TxnRequest, ct) → Result<TxnResult>`), `TxnRequest.Of(compare[], ops[])`, `TxnCompare.NotExists(key)`, `TxnCompare.ValueEqual(key, value)`, `TxnOp.Put(key, value, lease)`, `TxnOp.Delete(key, Prefix: false)`.
- Produces (для задач 4–6): `PortAllocLock(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)`; `const string PortAllocLock.Key = "/kafkaworker/locks/portalloc"`; `Task<Result<bool>> TryAcquireAsync(CancellationToken ct)`; `Task ReleaseAsync()`; `PortLockBusyException`.

- [ ] **Step 1: Написать юнит-тесты (падают — класса нет)**

Создать `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs`:

```csharp
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.Etcd;

// PortAllocLock (t91, arch/15 §4 / arch/16 §2.1): глобальный portalloc-клэйм —
// взаимоисключение секции довыделения портов между кластерами/инстансами
// (порт набора t90 PgWorker).
public class PortAllocLockTests
{
    private const string Ep = "http://etcd:2379";

    // AAA: первый захват проходит и пишет ключ с instance держателя;
    // второй (другой инстанс) получает false — не ошибка, не перезаписывает.
    [Fact]
    public async Task TryAcquire_SecondInstance_GetsFalse()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(CancellationToken.None);
        var secondAcquired = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        firstAcquired.IsSuccess.Should().BeTrue();
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.IsSuccess.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-1");
    }

    // AAA: release (del + revoke) освобождает — повторный захват другим инстансом
    // проходит; повторный ReleaseAsync — no-op.
    [Fact]
    public async Task Release_AllowsTakeover_AndIsIdempotent()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");
        (await first.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        await first.ReleaseAsync();
        await first.ReleaseAsync(); // повтор — no-op
        var reclaimed = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        reclaimed.Value.Should().BeTrue();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA (ревью-блокер t90, порт): повторный TryAcquire тем же объектом при живом
    // захвате — false, НЕ true: клэйм-объект DI-синглтон, ReconcileLoop тикает
    // кластеры ПАРАЛЛЕЛЬНО (MaxClusters) — параллельные тики одного инстанса
    // обязаны взаимоисключаться. «Занят» — не ошибка: waiting-portalloc-lock,
    // следующий тик.
    [Fact]
    public async Task TryAcquire_AlreadyHeldBySameObject_ReturnsFalse()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await locks.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        var again = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        again.IsSuccess.Should().BeTrue();
        again.Value.Should().BeFalse(); // держит параллельный тик этого же инстанса
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-1"); // ключ держателя не тронут
    }

    // AAA (регрессия busy-гэта): два TryAcquireAsync на ОДНОМ объекте — первый
    // true, второй false; после ReleaseAsync первого — захват снова проходит.
    [Fact]
    public async Task SameObject_SecondTickBlockedUntilRelease()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");

        // Act
        var first = await portLock.TryAcquireAsync(CancellationToken.None);
        var second = await portLock.TryAcquireAsync(CancellationToken.None);
        await portLock.ReleaseAsync();
        var reclaimed = await portLock.TryAcquireAsync(CancellationToken.None);
        await portLock.ReleaseAsync();

        // Assert
        first.Value.Should().BeTrue();
        second.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
    }

    // AAA: лок перехвачен (lease истёк, ключ перезаписан чужим value) —
    // release НЕ удаляет чужой ключ (del под compare ValueEqual).
    [Fact]
    public async Task Release_AfterTakeover_DoesNotDeleteForeignKey()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var mine = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await mine.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        // имитация истечения TTL и перехвата: ключ перезаписан чужим value
        etcd.Seed(PortAllocLock.Key, """{"instance":"inst-2","since_unix":1}""");

        // Act
        await mine.ReleaseAsync();

        // Assert: чужой ключ жив — del под ValueEqual(наш value) не сошёлся
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA: сбой etcd на txn → Result.Failed (процесс пойдёт в обычный бэкофф,
    // не InProgress-тихо).
    [Fact]
    public async Task TryAcquire_EtcdTxnFailure_ReturnsFailed()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd
        {
            TxnFault = _ => Result<KafkaWorker.Etcd.Client.TxnResult>.Failed(
                new ApplicationException("etcd: connection refused")),
        };
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");

        // Act
        var acquired = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        acquired.IsSuccess.Should().BeFalse();
    }
}
```

- [ ] **Step 2: Прогнать тесты — убедиться в ошибке компиляции**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~PortAllocLockTests"`
Expected: FAIL — `PortAllocLock`/`TxnFault` не существуют (ошибка компиляции проекта тестов).

- [ ] **Step 3: Доработать FakeEtcd — хук TxnFault (порт 1:1 эталона PgWorker)**

В `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`, в классе `FakeEtcd` рядом с существующими хуками (`OnTxnBeforeCompare`) добавить свойство — тип ТОЧЕЧНО как у эталона `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (строка ~32):

```csharp
        // Сбой-инъекция txn (t91: ошибка захвата PortAllocLock → Result.Failed;
        // порт TxnFault PgWorker): запрос → готовый Failed ДО compare.
        public Func<TxnRequest, Result<TxnResult>>? TxnFault { get; set; }
```

И в самом начале метода `TxnAsync` (до `lock (_gate)`):

```csharp
            if (TxnFault?.Invoke(req) is { } failed)
                return Task.FromResult(failed);
```

- [ ] **Step 4: Создать PortAllocLock (порт 1:1 с PgWorker)**

Создать `src/KafkaWorker.Etcd/Coordination/PortAllocLock.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Etcd.Coordination;

/// <summary>
/// Глобальный portalloc-клэйм (t91, arch/15 §4 / arch/16 §2.1; порт PortAllocLock
/// PgWorker t90): взаимоисключение секции довыделения портов «чтение занятости →
/// выбор портов → запись portalloc» — пер-кластерные клэймы кросс-кластерную гонку
/// не закрывают (два параллельно сеемых кластера читают /kafkaworker/portalloc/*
/// до первой записи соседа и выбирают одинаковые порты). Захват — txn version==0 +
/// put-with-lease TTL 15 с (паттерн /kafkaworker/leader); keepalive не нужен —
/// секция короткая (единицы секунд ≪ TTL). Освобождение — явное: del под compare
/// ValueEqual(наш value; lease истёк и лок перехвачен — чужой ключ не трогаем) +
/// revoke lease. «Занят» (чужим инстансом или параллельным тиком этого же — объект
/// в DI один на процесс) — не ошибка: вызывающий возвращает InProgress
/// (waiting-portalloc-lock), следующий тик (~5 с) повторяет; смерть держателя гасит
/// TTL ≤ 15 с — takeover без оператора.
/// </summary>
public sealed class PortAllocLock(
    string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)
{
    public const string Key = "/kafkaworker/locks/portalloc";
    private const int TtlSec = 15;

    private readonly object _sync = new();
    private long? _lease;
    private string? _payload; // наш value — compare «чужой-не-трогаем» при release

    /// <summary>Захват: true — держим; false — занят (другим инстансом либо
    /// параллельным тиком ЭТОГО инстанса — клэйм-объект DI-синглтон; НЕ ошибка).</summary>
    public async Task<Result<bool>> TryAcquireAsync(CancellationToken ct)
    {
        // t90 (порт): объект — DI-синглтон, ReconcileLoop тикает кластеры
        // ПАРАЛЛЕЛЬНО (MaxClusters) — для второго тика того же инстанса клэйм
        // «занят» так же, как для чужого: пока тик A держит секцию (поля
        // _lease/_payload гасятся только в ReleaseAsync), тик B получает false →
        // PortLockBusyException → waiting-portalloc-lock → следующий тик.
        // Локальная проверка ДО etcd-раунда, а не reentrant-true: (1) второй
        // конкурент НЕ входит в секцию concurrently — иначе обе секции читают
        // busy и пишут portalloc (сама гонка t91); (2) не тратим grant+txn на
        // заведомо занятый клэйм; (3) _lease/_payload пишет ровно один держатель
        // инстанса — перехват по истёкшему TTL не перезапишет поля, и release
        // зависшего тика не удалит чужой живой ключ под ValueEqual.
        lock (_sync)
        {
            if (_lease is not null)
                return Result<bool>.Success(false); // держит параллельный тик — ждём следующим тиком
        }

        var grant = await WithFailoverAsync(endpoint => gateway.LeaseGrantAsync(endpoint, TtlSec, ct));
        if (!grant.IsSuccess)
            return Result<bool>.Failed(grant.Error!);

        var payload = JsonSerializer.Serialize(new LockPayload(instanceId, Now()));
        var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(Key)],
                [new TxnOp.Put(Key, payload, grant.Value)]),
            ct));
        if (!txn.IsSuccess)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Failed(txn.Error!);
        }

        if (!txn.Value.Succeeded)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Success(false); // занят другим инстансом — не ошибка
        }

        lock (_sync)
        {
            _lease = grant.Value;
            _payload = payload;
        }

        return Result<bool>.Success(true);
    }

    /// <summary>Освобождение: del под compare ValueEqual(наш value) + revoke lease.
    /// Отказ del — best-effort (ключ гаснет по TTL); повтор/без захвата — no-op.</summary>
    public async Task ReleaseAsync()
    {
        long lease;
        string payload;
        lock (_sync)
        {
            if (_lease is not { } l)
                return;
            lease = l;
            payload = _payload!;
            _lease = null;
            _payload = null;
        }

        // Чужой лок не трогаем: compare не сошёлся (lease истёк, ключ перезаписан) → del не выполнится.
        _ = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(Key, payload)],
                [new TxnOp.Delete(Key, Prefix: false)]),
            CancellationToken.None));
        await RevokeSilentlyAsync(lease);
    }

    private async Task RevokeSilentlyAsync(long lease)
    {
        try
        {
            await WithFailoverAsync(endpoint => gateway.LeaseRevokeAsync(endpoint, lease, CancellationToken.None));
        }
        catch
        {
            // best-effort: истечёт по TTL
        }
    }

    private long Now() => clock.GetUtcNow().ToUnixTimeSeconds();

    // Failover по endpoints: первый успешный ответ выигрывает (паттерн ClaimStore).
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // Value ключа /kafkaworker/locks/portalloc (arch/15 §4).
    private sealed record LockPayload(
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix);
}

/// <summary>Сигнал «глобальный portalloc-клэйм занят другим инстансом» (t91):
/// НЕ фейл — без бэкоффа; процесс возвращает InProgress (waiting-portalloc-lock),
/// следующий тик повторяет. Маркер-тип для ветки обработки рядом с FailAsync.</summary>
public sealed class PortLockBusyException() : Exception(
    $"{PortAllocLock.Key}: занят другим инстансом — повторить следующим тиком");
```

- [ ] **Step 5: Прогнать тесты — зелёные**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~PortAllocLockTests"`
Expected: PASS, 6 тестов.

- [ ] **Step 6: DI-регистрация в Program.cs**

В `src/KafkaWorker.App/Program.cs` сразу ПОСЛЕ регистрации `ClaimStore` (`builder.Services.AddSingleton(sp => new ClaimStore(...)` — блок с `IEtcdGateway`/endpoints) добавить:

```csharp
// t91: глобальный portalloc-клэйм (arch/15 §4 / arch/16 §2.1) — DI-синглтон,
// InstanceId единый с ClaimStore (сквозная диагностика держателя).
builder.Services.AddSingleton(sp => new PortAllocLock(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
```

(проверить `using KafkaWorker.Etcd.Coordination;` в шапке файла — уже есть для ClaimStore/WorkJournal).

- [ ] **Step 7: Сборка решения**

Run: `dotnet build`
Expected: 0 ошибок, 0 warnings (TreatWarningsAsErrors).

- [ ] **Step 8: Commit**

```bash
git add src/KafkaWorker.Etcd/Coordination/PortAllocLock.cs src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs
git commit -m "t91: PortAllocLock — глобальный клэйм /kafkaworker/locks/portalloc (порт t90: txn version==0 + put-with-lease TTL 15 с, локальный busy-гейт параллельных тиков, release под ValueEqual + revoke) + DI-регистрация; FakeEtcd.TxNFault (порт эталона); юнит-тесты (порт набора t90)"
```

---

### Task 3: PortAllocIndex — чтение занятости portalloc соседей

**Вход:** эталон `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs`; формат kafka-записи `{"broker<k>":{"host":"h","client":16001}}` (arch/15 §4); spec §3.2, §6 (принцип «чужой мусор не роняет наш provision»).
**Действие:** TDD — тесты (включая валидный JSON без обязательных полей → skip) → класс.
**Выход:** `KafkaWorker.Provisioning.Processes.PortAllocIndex.ReadBusyAsync(exceptCluster, ct)`.
**Проверка:** `dotnet test` фильтром по PortAllocIndexTests.
**Связь со spec:** §3.2, §4 фаза 3, §7 критерий 4.

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/PortAllocIndex.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocIndexTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway.RangeAsync(endpoint, prefix, ct) → Result<IReadOnlyList<Kv>>` (`Kv(Key, Value, ModRevision)`); `KafkaWorker.Core.Result<T>`.
- Produces (для задач 4–5): `PortAllocIndex(IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)`; `Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(string exceptCluster, CancellationToken ct)`.

- [ ] **Step 1: Написать юнит-тесты (падают — класса нет)**

Создать `src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocIndexTests.cs`:

```csharp
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// PortAllocIndex (t91, arch/16 §2.1): busy = клиентские порты записей
// /kafkaworker/portalloc/* ЧУЖИХ кластеров; свой — исключается (закрепление,
// не занятость); чужой мусор любой формы (битый JSON, JSON без обязательных
// полей) — skip без ошибки (порт PgWorker-индекса, spec §3.2/§6).
public class PortAllocIndexTests
{
    private const string Ep = "http://etcd:2379";

    private static PortAllocIndex NewIndex(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

    // AAA: записи чужих кластеров дают busy-кортежи (host, client) всех их нод.
    [Fact]
    public async Task ReadBusy_ForeignClusters_AreBusy()
    {
        // Arrange: два соседа по docker-хосту.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001}}""");
        etcd.Seed("/kafkaworker/portalloc/shop2",
            """{"broker1":{"host":"h2","client":16000}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)>
        {
            ("h1", 16000), ("h1", 16001), ("h2", 16000),
        });
    }

    // AAA: свой кластер исключается — его portalloc переиспользуется аллокатором
    // как закрепление, а не занятость.
    [Fact]
    public async Task ReadBusy_OwnCluster_Excluded()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: пусто — единственная запись принадлежит исключённому кластеру.
        busy.Value.Should().BeEmpty();
    }

    // AAA: битый JSON соседа — Warning + skip ключа: чужой мусор не роняет наш
    // provision (принцип PgWorker-индекса).
    [Fact]
    public async Task ReadBusy_MalformedNeighbour_Skipped()
    {
        // Arrange: один живой сосед, один битый.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16000}}""");
        etcd.Seed("/kafkaworker/portalloc/broken", "{not-json");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: битый пропущен, валидный учтён.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)> { ("h1", 16000) });
    }

    // AAA (ревью t91): валидный JSON соседа БЕЗ обязательных полей (host/client) —
    // skip ключа без ошибки: как и битый JSON, это чужой мусор, не повод ронять
    // наш тик в Failed (spec §3.2/§6 — эталон PgWorker не роняет чтение ни на
    // каком мусоре).
    [Fact]
    public async Task ReadBusy_ValidJsonMissingFields_Skipped()
    {
        // Arrange: сосед без поля host; сосед без поля client; валидный сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/nohost", """{"broker1":{"client":16000}}""");
        etcd.Seed("/kafkaworker/portalloc/noclient", """{"broker1":{"host":"h1"}}""");
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16005}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: оба неполных ключа пропущены, валидный учтён.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)> { ("h1", 16005) });
    }
}
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~PortAllocIndexTests"`
Expected: FAIL — `PortAllocIndex` не существует.

- [ ] **Step 3: Создать PortAllocIndex**

Создать `src/KafkaWorker.Provisioning/Processes/PortAllocIndex.cs`:

```csharp
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Индекс занятости портов из etcd (t91, arch/16 §2.1): busy = docker-публикации
/// (добавляет вызывающий) ∪ записи portalloc ВСЕХ чужих кластеров. Свои записи
/// исключает вызывающий (exceptCluster) — свой portalloc переиспользуется
/// аллокатором как закрепление, а не занятость. Чужой мусор любой формы — битый
/// JSON ИЛИ валидный JSON без обязательных полей host/client — Warning-лог + skip
/// ключа: чужой мусор не роняет наш provision (порт PortAllocIndex PgWorker,
/// spec §3.2/§6).
/// </summary>
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/kafkaworker/portalloc/";

    /// <summary>Клиентский порт каждой записи каждого ЧУЖОГО /kafkaworker/portalloc/&lt;C&gt;.</summary>
    public async Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(
        string exceptCluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, Prefix, ct));
        if (!range.IsSuccess)
            return Result<IReadOnlySet<(string Host, int Port)>>.Failed(range.Error!);

        var busy = new HashSet<(string, int)>();
        foreach (var kv in range.Value)
        {
            var cluster = kv.Key.Split('/')[^1];
            if (cluster == exceptCluster)
                continue;

            // Формат arch/15 §4: {"broker<k>":{"host":"h","client":16001}}.
            // Фильтр catch — все формы чужого мусора: JsonException (битый JSON),
            // KeyNotFoundException (нет обязательного поля), InvalidOperationException
            // (поле не того типа — GetString/GetInt32 на несоответствующем узле).
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                foreach (var node in doc.RootElement.EnumerateObject())
                    busy.Add((node.Value.GetProperty("host").GetString()!,
                        node.Value.GetProperty("client").GetInt32()));
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                // Не наш ключ — не наша ответственность: лог + skip.
                logger.LogWarning("битый portalloc соседа {Cluster}: {Error}", cluster, ex.Message);
            }
        }

        return Result<IReadOnlySet<(string Host, int Port)>>.Success(busy);
    }

    // Failover-обёртка: первый успешный endpoint выигрывает (паттерн процессов).
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

- [ ] **Step 4: Прогнать тесты — зелёные**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~PortAllocIndexTests"`
Expected: PASS, 4 теста.

- [ ] **Step 5: Commit**

```bash
git add src/KafkaWorker.Provisioning/Processes/PortAllocIndex.cs src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocIndexTests.cs
git commit -m "t91: PortAllocIndex — busy из /kafkaworker/portalloc/* чужих кластеров (кроме своего; чужой мусор любой формы — битый JSON и JSON без полей host/client — Warning + skip); юнит-тесты"
```

---

### Task 4: ProvisioningProcess — секция K1 под клэймом + DI

**Вход:** Tasks 2–3 (`PortAllocLock`, `PortAllocIndex`); текущий `ProvisioningProcess.PlanAsync`/`RunAsync`; spec §3.3 п.1.
**Действие:** пред-выход до лока → клэйм с try/finally → busy = docker ∪ foreign → ветка `waiting-portalloc-lock` в RunAsync; ctor +2 параметра; DI-регистрация ProvisioningProcess (+ синглтон PortAllocIndex) и тесты-rig обновить.
**Выход:** K1 довыделяет порты только держателем клэйма; занят → InProgress без мутаций; сборка KafkaWorker.App зелёная.
**Проверка:** юнит-тесты (новые + все существующие ProvisioningProcessTests зелёные); `dotnet build`.
**Связь со spec:** §3.3 п.1, §3.5 инвариант, §4 фаза 4, §7 критерии 3–4.

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` (ctor, `RunAsync`, `PlanAsync`)
- Modify: `src/KafkaWorker.App/Program.cs` (регистрация ProvisioningProcess + синглтон PortAllocIndex + using)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (Rig/NewRig + новые тесты)

**Interfaces:**
- Consumes: `PortAllocLock.TryAcquireAsync/ReleaseAsync`, `PortLockBusyException`, `PortAllocIndex.ReadBusyAsync` (задачи 2–3); DI-регистрация `PortAllocLock` (задача 2, Step 6).
- Produces: `ProvisioningProcess(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal journal, PortAllocLock portLock, PortAllocIndex portAlloc, IAppSecretEnsurer appSecret, IKafkaAdminClientFactory adminFactory, IClusterConfigConverger converger, ProvisioningOptions options, Func<CancellationToken, Task<Result>>? snapshot = null)` — сигнатура для задачи 5/DI; DI-синглтон `PortAllocIndex` (для задачи 5).

- [ ] **Step 1: Обновить тестовый Rig (новые зависимости)**

В `src/tests/KafkaWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`:
1) `Rig` расширить полями `PortAllocLock PortLock, PortAllocIndex PortAlloc;`
2) в `NewRig` после `var journal = ...` добавить:

```csharp
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId);
        var portAllocIndex = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
```

3) создание процесса привести к новой сигнатуре:

```csharp
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, claims, journal, portLock, portAllocIndex,
            new AppSecretEnsurer(etcd, [Ep]),
            new FakeAdminFactory(admin),
            converger,
            new ProvisioningOptions(16000, 16999, brokerBootSec, 90, null, "apache/kafka:4.0.0"),
            snapshot: ct =>
            {
                snapshotPoints.Add($"n{snapshotPoints.Count}");
                return ValueTask.FromResult(Result.Success()).AsTask();
            });
```

4) return Rig — добавить `portLock, portAllocIndex`; 5) в `Run_NotClaimed_RefusesMutations` — тот же набор из двух строк перед `new ProvisioningProcess(...)` и те же два аргумента; 6) в шапку файла добавить `using Microsoft.Extensions.Logging.Abstractions;`.

- [ ] **Step 2: Написать новые тесты (падают — поведения нет)**

Добавить в `ProvisioningProcessTests` три теста:

```csharp
    // AAA (t91): клэйм занят чужим инстансом при НЕДОБОРЕ портов → журнальная
    // фаза waiting-portalloc-lock, успех тика (InProgress), никаких мутаций
    // portalloc и docker.
    [Fact]
    public async Task Run_PortLockBusy_WaitingPhase_NoMutations()
    {
        // Arrange: клэйм /kafkaworker/locks/portalloc держит «другой инстанс».
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);
        rig.Etcd.Seed("/kafkaworker/locks/portalloc", """{"instance":"inst-2","since_unix":1}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress-семантика — успех без мутаций (следующий тик повторит).
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-portalloc-lock");
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/portalloc/events");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA (t91): чужая portalloc-запись — занятость (окно «сосед записал,
    // контейнеры ещё не созданы» закрыто): выделенные порты обходят соседские.
    [Fact]
    public async Task Run_ForeignPortAllocRecord_PortIsNotReused()
    {
        // Arrange: сосед shop1 закрепил h1:16000, его контейнеров ещё нет
        // (docker-busy пуст — окно K1→K3 гонки t91).
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);
        rig.Etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16000}}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: полный прогон; порты events начинаются с 16001 — 16000 занят соседом.
        result.IsSuccess.Should().BeTrue();
        var portAlloc = rig.Etcd.Store["/kafkaworker/portalloc/events"].Value;
        portAlloc.Should().NotContain("\"client\":16000");
        portAlloc.Should().Contain("\"client\":16001").And.Contain("\"client\":16002").And.Contain("\"client\":16003");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16001,h1:16002,h1:16003");
    }

    // AAA (t91): всё закреплено (rebuild) — пред-выход ДО клэйма: занятый чужим
    // клэйм не мешает довести provisioning до done, portalloc не перезаписывается.
    [Fact]
    public async Task Run_AllPinnedEarlyExit_IgnoresPortLock()
    {
        // Arrange: portalloc полный (3/3), клэйм занят чужим инстансом.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);
        rig.Etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001},"broker3":{"host":"h1","client":16002}}""");
        rig.Etcd.Seed("/kafkaworker/locks/portalloc", """{"instance":"inst-2","since_unix":1}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: полный прогон до done (не waiting-portalloc-lock) — клэйм не брался,
        // закрепления переиспользованы как есть.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value
            .Should().Contain("\"client\":16000"); // закрепление не тронуто
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }
```

- [ ] **Step 3: Прогнать — failing-state TDD**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: FAIL — ошибка компиляции проекта тестов (Rig уже на новой сигнатуре, процесс — на старой). Это ожидаемый failing-state TDD; после Step 4 `Run_AllPinnedEarlyExit_IgnoresPortLock` обязан остаться зелёным (регрессионный якорь: пред-выход не ломает путь «всё закреплено»), `Run_PortLockBusy_...` и `Run_ForeignPortAllocRecord_...` — стать зелёными.

- [ ] **Step 4: Правка ProvisioningProcess**

В `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs`:

а) ctor — после `WorkJournal journal,` вставить `PortAllocLock portLock, PortAllocIndex portAlloc,`:

```csharp
public sealed class ProvisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    IAppSecretEnsurer appSecret,
    IKafkaAdminClientFactory adminFactory,
    IClusterConfigConverger converger,
    ProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
```

б) `RunAsync` — заменить блок обработки `planned`:

```csharp
        // K1: план placement + порты, закрепление portalloc, фиксация ролей.
        var planned = await PlanAsync(snap, ct);
        if (!planned.IsSuccess)
        {
            // t91: клэйм занят — не фейл, InProgress (следующий тик ~5 с);
            // сбой захвата — обычный фейл (бэкофф).
            if (planned.Error is PortLockBusyException)
                return await FinishAsync(cluster, "waiting-portalloc-lock", ct);
            return Fail(cluster, planned.Error!, "planning");
        }
```

в) `PlanAsync` — заменить тело между чтением pinned и txn на новую структуру (пред-выход + клэйм + foreign):

```csharp
    // K1: placement → порт-аллокация → закрепление /kafkaworker/portalloc/<C>
    // (txn compare version==0; конкурент закрепил первым → берём его) + роли.
    // t91 (arch/16 §2.1): довыделение портов — под глобальным клэймом
    // /kafkaworker/locks/portalloc: без него два параллельно сеемых кластера
    // читают занятость до первой записи соседа и выбирают одинаковые порты.
    // Занятость = docker-публикации ∪ portalloc ЧУЖИХ кластеров (свой —
    // закрепление, переиспользуется аллокатором); «не взял» — не ошибка:
    // waiting-portalloc-lock, следующий тик повторяет.
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanAsync(
        KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        var wanted = snap.Brokers.Select(b => b.Name).ToList();

        // Ранний пред-выход ДО клэйма (порт t90): всё закреплено —
        // переиспользование без записи; тики waiting-brokers (K4) не
        // соперничают за глобальный клэйм.
        if (wanted.All(existing.ContainsKey))
            return await PlannedAsync(existing, cluster, ct);

        // t91: захват глобального клэйма; сбой — обычный фейл (бэкофф),
        // занят — PortLockBusyException → тик-ретрай.
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
        try
        {
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ReadBusyAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreign.Error!);
            var busy = new HashSet<(string Host, int Port)>(foreign.Value);
            foreach (var p in dockerBusy.Value)
                busy.Add(p);

            var plan = PlacementPlanner.Plan(wanted, hosts.Value);
            var allocated = PortAllocator.Allocate(plan, existing, busy, options.PortFrom, options.PortTo);
            if (!allocated.IsSuccess)
                return allocated;

            foreach (var (node, addr) in allocated.Value)
                existing[node] = addr;

            // Создание ключа — только если нет (compare version==0); проигрыш → re-read.
            var key = PortAllocKey(cluster);
            var serialized = SerializePortAlloc(existing);
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, serialized, null)]), ct);
            if (!txn.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(reread.Error!);
                existing = new Dictionary<string, NodeAddress>(reread.Value);
            }

            // Фиксация ролей (put только при отличии; роль навсегда, arch/15 §2) —
            // ВСЕГДА, независимо от исхода txn (порт семантики исходного кода:
            // ключ мог быть записан до t91 без ролей). Внутри секции, до release.
            foreach (var broker in snap.Brokers)
            {
                if (RolesFor(snap.Config.Brokers).GetValueOrDefault(broker.Name) is { } role && broker.Role != role)
                {
                    var put = await PutAsync(RoleKey(cluster, broker.Name), role, ct);
                    if (!put.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
                }
            }

            return await PlannedAsync(existing, cluster, ct);
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // Журнал planned + результат секции (порт PlannedAsync t90 — внутри try,
    // до release; клэйм короткий).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlannedAsync(
        Dictionary<string, NodeAddress> existing, string cluster, CancellationToken ct)
    {
        var planned = await journal.WriteAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
        return planned.IsSuccess
            ? Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing)
            : Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(planned.Error!);
    }
```

Ветка txn-проигрыша оставлена как в исходном коде (re-read и продолжение) — идемпотентность сохранена; отличие от оригинала только в обёртке клэйма и foreign-busy.

- [ ] **Step 5: Прогнать тесты — все зелёные**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: PASS — 7 существующих + 3 новых = 10.

- [ ] **Step 6: DI в Program.cs — регистрация ProvisioningProcess + синглтон PortAllocIndex**

Изменение ctor процесса (Step 4) ломает регистрацию в `src/KafkaWorker.App/Program.cs` — править В ЭТОЙ ЖЕ задаче (коммит Task 4 обязан собираться). В регистрации `ProvisioningProcess` после `sp.GetRequiredService<WorkJournal>(),` добавить `sp.GetRequiredService<PortAllocLock>(), sp.GetRequiredService<PortAllocIndex>(),` (`PortAllocLock` зарегистрирован в Task 2 Step 6). Рядом с той регистрацией добавить синглтон индекса (потребуется задаче 5):

```csharp
builder.Services.AddSingleton(sp => new PortAllocIndex(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ILogger<PortAllocIndex>>()));
```

Для `ILogger<PortAllocIndex>` добавить в шапку Program.cs `using Microsoft.Extensions.Logging;` (сейчас отсутствует; `using KafkaWorker.Provisioning.Processes;` уже есть):

```csharp
using Microsoft.Extensions.Logging;
```

- [ ] **Step 7: Сборка решения**

Run: `dotnet build`
Expected: 0 ошибок, 0 warnings (TreatWarningsAsErrors) — включая KafkaWorker.App.

- [ ] **Step 8: Commit**

```bash
git add src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "t91: ProvisioningProcess K1 — секция довыделения под глобальным portalloc-клэймом (busy = docker ∪ portalloc чужих; пред-выход «всё закреплено» до клэйма; занят → waiting-portalloc-lock/InProgress без мутаций) + DI (регистрация процесса + синглтон PortAllocIndex); юнит-тесты"
```

---

### Task 5: AddBrokerProcess — секция добора под клэймом

**Вход:** Tasks 2–4 (PortAllocLock, PortAllocIndex, DI-синглтоны обоих, новая сигнатура ProvisioningProcess); текущий `AddBrokerProcess.EnsurePortsAsync`/`RunAsync`; spec §3.3 п.2.
**Действие:** зеркально Task 4 — ранний выход до лока (уже есть `missing.Count == 0`), клэйм с try/finally вокруг добора, foreign-busy, ветка `waiting-portalloc-lock`.
**Выход:** AddBroker добирает порты только держателем клэйма.
**Проверка:** юнит-тесты AddBrokerProcessTests (новые + существующие).
**Связь со spec:** §3.3 п.2, §3.5, §4 фаза 5, §7 критерии 3–4.

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs` (ctor, `RunAsync`, `EnsurePortsAsync`)
- Modify: `src/KafkaWorker.App/Program.cs` (регистрация AddBrokerProcess)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/AddBrokerProcessTests.cs` (Rig/NewRig + новые тесты)

**Interfaces:**
- Consumes: `PortAllocLock`, `PortAllocIndex` (задачи 2–3; DI-синглтоны зарегистрированы в Task 2 Step 6 и Task 4 Step 6).
- Produces: `AddBrokerProcess(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal journal, PortAllocLock portLock, PortAllocIndex portAlloc, IKafkaAdminClientFactory adminFactory, ProvisioningOptions options)`.

- [ ] **Step 1: DI в Program.cs — регистрация AddBrokerProcess**

В `src/KafkaWorker.App/Program.cs` в регистрации `AddBrokerProcess` после `sp.GetRequiredService<WorkJournal>(),` добавить `sp.GetRequiredService<PortAllocLock>(), sp.GetRequiredService<PortAllocIndex>(),` (оба синглтона уже зарегистрированы — Task 2 Step 6 и Task 4 Step 6; `using Microsoft.Extensions.Logging;` добавлен в Task 4 Step 6). Другие правки Program.cs в этой задаче НЕ требуются.

- [ ] **Step 2: Обновить тестовый Rig AddBrokerProcessTests**

В `NewRig` после `var journal = ...`:

```csharp
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId);
        var portAllocIndex = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
```

`new AddBrokerProcess(etcd, [Ep], driver, claims, journal, portLock, portAllocIndex, new FakeAdminFactory(admin), new ProvisioningOptions(16000, 16999, 600, 90, null, "apache/kafka:4.0.0"));` — в `NewRig` и в `Run_NotClaimed_Refuses`; в шапку — `using Microsoft.Extensions.Logging.Abstractions;`.

- [ ] **Step 3: Написать новые тесты**

Добавить в `AddBrokerProcessTests`:

```csharp
    // AAA (t91): клэйм занят чужим инстансом при недоборе портов broker4 →
    // журнальная фаза waiting-portalloc-lock, успех тика, без мутаций.
    [Fact]
    public async Task Run_PortLockBusy_WaitingPhase_NoMutations()
    {
        // Arrange: Active-кластер + заявка broker4; клэйм держит «другой инстанс».
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafkaworker/locks/portalloc", """{"instance":"inst-2","since_unix":1}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress — успех без мутаций portalloc/docker/endpoints.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-portalloc-lock");
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value.Should().NotContain("broker4");
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }

    // AAA (t91): чужая portalloc-запись — занятость добора: broker4 не получает
    // порт, закреплённый соседом (окно «сосед записал, контейнеров нет»).
    [Fact]
    public async Task Run_ForeignPortAllocRecord_PortIsNotReused()
    {
        // Arrange: сосед shop1 закрепил h1:16003; docker-busy пуст.
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16003}}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker4 — следующий свободный после своих и соседа (16004).
        result.IsSuccess.Should().BeTrue();
        var spec = rig.Driver.Ensured.Single(s => s.NodeName == "broker4");
        spec.ClientHostPort.Should().Be(16004);
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value
            .Should().Contain("\"broker4\":{\"host\":\"h1\",\"client\":16004}");
    }
```

- [ ] **Step 4: Прогнать — падают на новом поведении (и на сигнатуре до правки)**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~AddBrokerProcessTests"`
Expected: FAIL (компиляция — пока ctor старый; после правки кода — зелёные).

- [ ] **Step 5: Правка AddBrokerProcess**

В `src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs`:

а) ctor — после `WorkJournal journal,` вставить `PortAllocLock portLock, PortAllocIndex portAlloc,`.

б) `RunAsync` — заменить блок обработки `ports`:

```csharp
        // План: адреса из portalloc + добор портов для новых брокеров (RMW).
        var ports = await EnsurePortsAsync(snap, pending, ct);
        if (!ports.IsSuccess)
        {
            // t91: клэйм занят — не фейл, InProgress (следующий тик ~5 с).
            if (ports.Error is PortLockBusyException)
            {
                await journal.WriteAsync(cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);
                return Result.Success();
            }

            return Fail(cluster, ports.Error!, "planning");
        }
        var addresses = ports.Value;
```

в) `EnsurePortsAsync` — блок `if (missing.Count > 0) { ... }` заменить на (захват клэйма + foreign):

```csharp
        if (missing.Count > 0)
        {
            // t91 (arch/16 §2.1): добор портов — под глобальным клэймом
            // /kafkaworker/locks/portalloc; сбой — обычный фейл, занят —
            // PortLockBusyException → тик-ретрай.
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
            if (!acquired.Value)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
            try
            {
                var hosts = await driver.GetHostsAsync(ct);
                if (!hosts.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
                var dockerBusy = await driver.GetBusyPortsAsync(ct);
                if (!dockerBusy.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
                var foreign = await portAlloc.ReadBusyAsync(cluster, ct);
                if (!foreign.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreign.Error!);

                // План — ТОЛЬКО новые ноды (порт PgWorker AddShard): собственные порты
                // живых контейнеров числятся в busy и при полном плане аллокатор счёл бы
                // их «занятыми» и перевыделил (рассинхрон portalloc/endpoints/контейнера).
                // Закреплённые адреса исключаются из кандидатов явно; t91: плюс
                // занятость portalloc ЧУЖИХ кластеров (окно «сосед записал,
                // контейнеров ещё нет»).
                var plan = PlacementPlanner.Plan(missing, hosts.Value);
                var taken = new HashSet<(string Host, int Port)>(dockerBusy.Value);
                foreach (var p in foreign.Value)
                    taken.Add(p);
                foreach (var addr in addresses.Values)
                    taken.Add((addr.Host, addr.ClientPort));
                var allocated = PortAllocator.Allocate(
                    plan, addresses, taken, options.PortFrom, options.PortTo);
                if (!allocated.IsSuccess)
                    return allocated;
                foreach (var (node, addr) in allocated.Value)
                    addresses[node] = addr;

                var serialized = SerializePortAlloc(addresses);
                var key = PortAllocKey(cluster);
                var txn = await TxnAsync(
                    TxnRequest.Of(
                        [TxnCompare.ModRevisionEqual(key, current.Value.Revision ?? 0)],
                        [new TxnOp.Put(key, serialized, null)]),
                    ct);
                if (!txn.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(txn.Error!);
                if (!txn.Value.Succeeded)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new ApplicationException(
                        $"portalloc {key} изменился с момента чтения — ретрай тиком"));
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }
```

- [ ] **Step 6: Прогнать тесты — все зелёные**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~AddBrokerProcessTests"`
Expected: PASS — 5 существующих + 2 новых = 7.

- [ ] **Step 7: Сборка (DI Program.cs — регистрация AddBrokerProcess)**

Run: `dotnet build`
Expected: 0 ошибок, 0 warnings.

- [ ] **Step 8: Commit**

```bash
git add src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/Provisioning/AddBrokerProcessTests.cs
git commit -m "t91: AddBrokerProcess — добор портов под глобальным portalloc-клэймом (taken = docker ∪ foreign ∪ свои закрепления; занят → waiting-portalloc-lock/InProgress) + DI-регистрация; юнит-тесты"
```

---

### Task 6: Интеграционный race-тест на реальном etcd

**Вход:** Tasks 2–5; лёгкая `EtcdFixture` (`src/tests/KafkaWorker.IntegrationTests/Etcd/ApiEtcdFixture.cs`, etcd-only, `assignRandomHostPort`); эталон коллекции `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs:104` (`EtcdCollection`); эталон `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`; spec §4 фаза 6.
**Действие:** объявить xUnit-коллекцию etcd-фикстуры (порт PgWorker — БЕЗ коллекции тест-класс не привяжется к фикстуре) → race-тест — две параллельные мини-секции «busy из префикса → Allocate → Put» под клэймом → непересекающиеся порты.
**Выход:** интеграционное подтверждение инварианта §3.5 на реальном txn-примитиве etcd.
**Проверка:** `dotnet test` фильтром по PortAllocLockRaceTests (требуется docker).
**Связь со spec:** §4 фаза 6, §7 критерий 6.

**Files:**
- Modify: `src/tests/KafkaWorker.IntegrationTests/Etcd/ApiEtcdFixture.cs` (добавить `EtcdCollection` в конец файла)
- Test: `src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs` (новый; фикстура уже существует)

**Interfaces:**
- Consumes: `EtcdFixture` (`Endpoint`, `Gateway` — `KafkaWorker.Etcd.Client.EtcdGateway`); новая `EtcdCollection` (`Name = "kafka-etcd"`, `ICollectionFixture<EtcdFixture>`); `PortAllocLock` (задача 2); `KafkaWorker.Core.Planning.PortAllocator/PlacementPlan/NodePlacement`; `KafkaWorker.Core.Model.NodeAddress(Host, ClientPort)`; `IEtcdGateway.RangeAsync/GetAsync/PutAsync`.

- [ ] **Step 1: Объявить коллекцию etcd-фикстуры (порт EtcdCollection PgWorker)**

В `src/tests/KafkaWorker.IntegrationTests/Etcd/ApiEtcdFixture.cs` В КОНЕЦ файла (после класса `EtcdFixture`) добавить — порт `EtcdCollection` из `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs:104` (у KafkaWorker.IntegrationTests такой коллекции нет; без неё xUnit не привяжет фикстуру к тест-классу):

```csharp
// Один etcd-контейнер на etcd-only тест-классы сборки (t91; порт EtcdCollection
// PgWorker): ключи тестов не пересекаются, контейнер поднимается один.
[CollectionDefinition(Name)]
public sealed class EtcdCollection : ICollectionFixture<EtcdFixture>
{
    public const string Name = "kafka-etcd";
}
```

- [ ] **Step 2: Написать race-тест**

Создать `src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`:

```csharp
using System.Text;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using Xunit;

namespace KafkaWorker.IntegrationTests.Etcd;

// t91: гонка ПАРАЛЛЕЛЬНОГО provisioning на реальном etcd — две критические
// секции «busy из префикса portalloc → Allocate → put portalloc» под глобальным
// клэймом дают НЕПЕРЕСЕКАЮЩИЕСЯ порты; без клэйма+индекса обе читали бы пустой
// префикс (класс гонки t90: «port is already allocated»; порт PgWorker-теста).
[Collection(EtcdCollection.Name)]
public class PortAllocLockRaceTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Критическая секция довыделения (мини-K1): под PortAllocLock читает busy из
    // префикса portalloc (кроме своего кластера — PortAllocIndex-паттерн),
    // аллоцирует порт, пишет ключ. Ретрай-цикл «пока не acquired» имитирует тики
    // (~200 мс) с бюджетом 10 с.
    private async Task<Result> CriticalSectionAsync(PortAllocLock portLock, string cluster, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return acquired;
            if (!acquired.Value)
            {
                await Task.Delay(200, ct); // «занят другим» — следующий тик
                continue;
            }

            try
            {
                // busy = portalloc-записи ВСЕХ чужих кластеров (как PortAllocIndex):
                // кортежи (host из записи, client) — именно так их сравнивает
                // PortAllocator по placement.Host.
                var range = await Gateway.RangeAsync(Endpoint, "/kafkaworker/portalloc/", ct);
                if (!range.IsSuccess)
                    return range;
                var busy = new HashSet<(string, int)>();
                foreach (var kv in range.Value)
                {
                    if (kv.Key.EndsWith($"/{cluster}", StringComparison.Ordinal))
                        continue;
                    foreach (var (host, port) in ParseEntries(kv.Value))
                        busy.Add((host, port));
                }

                // Аллокация одного брокера (1 клиентский порт); диапазон — значения
                // в etcd, не host-биндинги: литералы допустимы (AGENTS.md — про
                // хост-порты docker).
                var plan = new PlacementPlan([new NodePlacement("broker1", "h1")]);
                var allocated = PortAllocator.Allocate(
                    plan, new Dictionary<string, NodeAddress>(), busy, 16000, 16100);
                if (!allocated.IsSuccess)
                    return allocated;
                var put = await Gateway.PutAsync(
                    Endpoint, $"/kafkaworker/portalloc/{cluster}", Serialize(allocated.Value), null, ct);
                if (!put.IsSuccess)
                    return put;
                return Result.Success();
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }

        return Result.Failed(new ApplicationException("порт-клэйм не освобождался 10 с — гонка/дедлок"));
    }

    // Формат arch/15 §4: {"broker<k>":{"host":"h","client":P}} — пары (host, client).
    private static IEnumerable<(string Host, int Port)> ParseEntries(string json)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            yield return (node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
    }

    private static string Serialize(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        var sb = new StringBuilder("{");
        foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            sb.Append($"\"{node}\":{{\"host\":\"{addr.Host}\",\"client\":{addr.ClientPort}}},");
        return (sb.Length > 1 ? sb.ToString()[..^1] : "{") + "}";
    }

    // AAA: две параллельные секции (барьер одновременного старта) — порты двух
    // кластеров не пересекаются; ключ клэйма исчезает после release обеих.
    [Fact]
    public async Task ParallelSections_AllocateDisjointPorts()
    {
        // Arrange — «два инстанса» с независимыми клэймами
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task1 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(first, "events1", ct); });
        var task2 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(second, "events2", ct); });

        // Act — одновременный старт
        start.SetResult();
        var results = await Task.WhenAll(task1, task2);

        // Assert: обе секции дошли до конца
        results.Should().OnlyContain(r => r.IsSuccess);

        // Порты двух кластеров НЕ пересекаются (без клэйма обе получили бы 16000)
        var firstPorts = ParseEntries(
            (await Gateway.GetAsync(Endpoint, "/kafkaworker/portalloc/events1", ct)).Value!.Value)
            .Select(e => e.Port).ToList();
        var secondPorts = ParseEntries(
            (await Gateway.GetAsync(Endpoint, "/kafkaworker/portalloc/events2", ct)).Value!.Value)
            .Select(e => e.Port).ToList();
        firstPorts.Should().NotBeEmpty();
        secondPorts.Should().NotBeEmpty();
        firstPorts.Intersect(secondPorts).Should().BeEmpty(
            "клэйм сериализует секции — повторная видит запись соседа в busy");

        // Ключ клэйма исчез после release обеих секций
        var lockKey = await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct);
        lockKey.Value.Should().BeNull();
    }

    // AAA: захват/занятость/release на реальном txn-примитиве etcd.
    [Fact]
    public async Task TryAcquire_MutualExclusionAndRelease()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(ct);
        var secondAcquired = await second.TryAcquireAsync(ct);
        await first.ReleaseAsync();
        var reclaimed = await second.TryAcquireAsync(ct);
        await second.ReleaseAsync();

        // Assert
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
        (await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct)).Value.Should().BeNull();
    }
}
```

- [ ] **Step 3: Прогнать (docker поднимет etcd-контейнер с динамическим портом)**

Run: `dotnet test src/tests/KafkaWorker.IntegrationTests --filter "FullyQualifiedName~PortAllocLockRaceTests"`
Expected: PASS, 2 теста (фикстура привязана коллекцией — контейнер один на класс).

- [ ] **Step 4: Commit**

```bash
git add src/tests/KafkaWorker.IntegrationTests/Etcd/ApiEtcdFixture.cs src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs
git commit -m "t91: интеграционный race-тест на реальном etcd (EtcdCollection-привязка фикстуры, динамический порт) — параллельные секции довыделения под клэймом дают непересекающиеся порты"
```

---

### Task 7: Финальный прогон решения

**Вход:** все задачи 1–6 слиты в ветку.
**Действие:** полный build + test решения; визуальная проверка отсутствия посторонних диффов.
**Выход:** зелёное решение.
**Проверка:** вывод команд.
**Связь со spec:** §4 фаза 8, §7 критерий 7.

- [ ] **Step 1: Полная сборка и тесты**

Run: `dotnet build && dotnet test`
Expected: build 0 warnings/0 ошибок; все тесты PASS (включая новые 17: 6 PortAllocLockTests + 4 PortAllocIndexTests + 3 ProvisioningProcessTests + 2 AddBrokerProcessTests + 2 PortAllocLockRaceTests).

- [ ] **Step 2: Ревизия объёма ветки**

Run: `git diff main --stat`
Expected: arch/ ×2 (Task 1); roadmap-файл НЕ меняется в ветке (мерж-гейт — Task 8); docs/superpowers (spec+plan); src ×5 (PortAllocLock.cs, PortAllocIndex.cs, ProvisioningProcess.cs, AddBrokerProcess.cs, Program.cs); тесты ×7 (Fakes.cs, PortAllocLockTests.cs, PortAllocIndexTests.cs, ProvisioningProcessTests.cs, AddBrokerProcessTests.cs, ApiEtcdFixture.cs, PortAllocLockRaceTests.cs).

- [ ] **Step 3: Commit (если остались несобранные хвосты) и готовность к ревью**

```bash
git status --short
```

Expected: чистое дерево (всё закоммичено задачами 1–6; docs/superpowers/spec+plan — закоммитить отдельным шагом, если ещё не были):

```bash
git add docs/superpowers/2026-09-03-kafka-portalloc-race/
git commit -m "t91: spec+plan (docs/superpowers)"
```

---

### Task 8: Мерж-гейт roadmap — ВЫПОЛНЯЕТСЯ ПРИ СЛИЯНИИ В main (не в рабочей ветке!)

**Вход:** ветка прошла ревью и готова к слиянию в main.
**Действие:** тем же merge-коммитом (или коммитом слияния в main) удалить пункт `t91-kafka-portalloc-race` из `arch/roadmap/kafkaworker.md` (строки с «**`t91-kafka-portalloc-race`**» по «...Зависимостей нет.»); `←`-ссылок на t91 в других пунктах roadmap НЕТ (проверено на дату плана) — удалять больше нечего.
**Выход:** roadmap без тега t91.
**Проверка:** `grep -rn "t91" arch/roadmap/` — пусто.
**Связь со spec:** §3.4 п.6, §4 фаза 7, §7 критерий 8.

- [ ] **Step 1 (в merge-коммите): удалить пункт t91 из roadmap**

Run (после слияния, в main): `grep -n "t91" arch/roadmap/kafkaworker.md`
Expected: одна запись-пункт; удалить его целиком и закоммитить тем же коммитом слияния (`docs(roadmap): тег t91-kafka-portalloc-race удалён — задача слита (мерж-гейт)` — прецедент a9df637).

- [ ] **Step 2: контроль**

Run: `grep -rn "t91" arch/roadmap/ || echo OK`
Expected: `OK`.

---

## Self-Review плана (выполнен автором, включая правки по обоим раундам ревью Фазы 4)

- **Покрытие spec:** §3.1 → Task 2; §3.2 → Task 3 (включая расширенный catch по ревью); §3.3 п.1 → Task 4; §3.3 п.2 → Task 5; §3.4 → Tasks 1 и 8; §3.5 инвариант → Tasks 4–6; фазы §4 1–8 → Tasks 1–8 (фаза 7 = Task 8); критерии §7 1–8 → задачи 1/2/4/5/6/7/8. Групп нет.
- **Плейсхолдеры:** отсутствуют — каждый шаг содержит полный код/текст/команду.
- **Консистентность типов:** `PortAllocLock.TryAcquireAsync/ReleaseAsync`, `PortAllocIndex.ReadBusyAsync(string, CancellationToken)`, ctor-сигнатуры процессов совпадают между задачами 2–5; `NodeAddress(string Host, int ClientPort)`, `PlacementPlan([NodePlacement(Node, Host)])` сверены с кодом; тип `FakeEtcd.TxnFault` — `Func<TxnRequest, Result<TxnResult>>?` — сверен с эталоном PgWorker (Fakes.cs:32) и лямбдой теста Task 2 Step 1.
- **Привязка etcd-фикстуры:** Task 6 Step 1 объявляет `EtcdCollection` (`[CollectionDefinition(Name)]`, `Name = "kafka-etcd"`, `ICollectionFixture<EtcdFixture>`) в ApiEtcdFixture.cs — порт `EtcdCollection` PgWorker (EtcdFixture.cs:104); тест-класс помечен `[Collection(EtcdCollection.Name)]` — без коллекции xUnit v3 не создаёт фикстуру для ctor-параметра.
- **Сборка каждого коммита:** Task 4 включает DI-правку Program.cs (регистрация ProvisioningProcess + синглтон PortAllocIndex + using) и build-шаг — коммит задачи собирается; Task 5 правит только регистрацию AddBrokerProcess.
- **Арифметика тестов (сверена grep'ом по фактическим файлам):** ProvisioningProcessTests — 7 существующих [Fact] + 3 новых = 10 (Task 4 Step 5); AddBrokerProcessTests — 5 существующих + 2 новых = 7 (Task 5 Step 6); новых всего 17 = 6 (PortAllocLock) + 4 (PortAllocIndex) + 3 (Provisioning) + 2 (AddBroker) + 2 (race); затронутых тест-файлов 7 (включая ApiEtcdFixture.cs с коллекцией).
