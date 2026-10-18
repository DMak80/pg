# План: фикс флейка AC12 (часть А) + параллельность по классам (часть Б) + прод-фикс фабрики AdminClient (часть В) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Части А/Б исполнены (AC12 стабилен — оба пути Act2 валидны; параллельность по классам с per-class окружением; полный прогон 72/73 за 5 м 13 с). Часть В (текущая, фаза A4/A5): устранить прод-дефект `KafkaAdminClientFactory` — dispose нативного клиента под висящей admin-операцией (`ERR__DESTROYED`, единственный красный полного прогона) — строго arch-first: правка `arch/16` §6 → фикс `KafkaAdminClient.cs` (счётчик активных операций + отложенный dispose с бюджетом дрейна) → юнит-покрытие → повторная приёмка: юниты зелёные, полный параллельный прогон 73/73, `destroyed = 0`.

**Architecture:** Адаптер `KafkaAdminClient` ведёт Interlocked-счётчик активных операций в каркасе `RunAsync`; замена (Unhealthy в `Create`) и вытеснение (`EvictIdle`) удаляют запись из кеша мгновенно (раздача не блокируется), а dispose заменяемого исполняется в фоне, когда счётчик его операций достигнет нуля (continuation последней операции), с верхним бюджетом `requestTimeout + 30 с` — по исчерпании принудительный dispose + warning. `Dispose()` фабрики — тот же дрейн по каждому клиенту (детерминированность остановки без вечных ожиданий). Гарантии сети не меняются: пометка Unhealthy и пересоздание немедленны, backoff-пины не тронуты, `DisposeAsync` адаптера — no-op.

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), Interlocked/TCS-дрейн, xunit.v3 (юниты AAA, `InternalsVisibleTo KafkaWorker.UnitTests` уже настроен), Confluent.Kafka 2.15.1.

**Spec:** `docs/superpowers/2026-10-17-kfw-rotation-timeout-flake/spec.md` (ревизия с частью В: §6.4, фазы A4/A5, §8/§9). План аргументируется от спеки — исполнитель читает ОБА документа.

## Global Constraints

- Область правок (spec §8): тестовая сборка `src/tests/KafkaWorker.IntegrationTests/**`, прод-фабрика `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (+ юнит-тесты `src/tests/KafkaWorker.UnitTests/**`), одна строка `Confluent.Kafka` в `Directory.Packages.props`, arch-first-правка `arch/16-kafkaworker.md` §6. ИНОГО прод-кода и `arch/**` не трогать.
- **Порядок arch-first обязателен (spec §8):** СНАЧАЛА `arch/16` §6, ЗАТЕМ код — нарушение порядка = сломанный контракт (AGENTS.base §1).
- Прод-фикс — production-ready (AGENTS.base §9): без «зависших» dispose (бюджет дрейна с диагностикой-warning), без ухудшения поведения при обрывах сети (замена немедленна, backoff-пины ≥1000 мс не меняются, DisposeAsync адаптера — no-op), Create не блокируется дрейном.
- Юнит-тесты — AAA-разметка, комментарии по-русски; идентификаторы — по-английски.
- Сборка Release — 0 warnings. Все команды — ТОЛЬКО в фоне (`run_in_background: true`), task_id фиксировать; sleep/ожидания ≤ 30 с; наблюдение тестов: старт ≤ 15 с, контроль каждые 2 мин.
- Перезапуск упавших тестов «для выяснения» ЗАПРЕЩЁН: падение — стоп, улики ДО зачистки (лог, `docker ps -a | grep kfw-`, `docker network ls | grep kfw-net`, `docker logs` контейнеров), разбор, повтор — только с согласия пользователя (spec §8, §9.7).
- Зачистка после КАЖДОЙ серии — точечно только `kfw-*` (сети через `while IFS= read -r n; do docker network rm "$n"; done`); `docker network prune -f` ЗАПРЕЩЁН. Логи `/tmp/pgw-kfw-rotto-1..5.log` не перезаписывать (5 — A3-прогон); новый номер — следующий свободный по `ls /tmp/pgw-kfw-rotto-*`.
- Коммиты — в feature-ветку `fix-kfw-rotation-timeout-flake`. Мерж/пуш — только по явному требованию пользователя.
- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/fix-kfw-rotation-timeout-flake` (далее `<WT>`).
- Контрольные команды — BSD-совместимые: `grep -cF`/`grep -c` без `\|`-альтернатив; python-разбор TRX с namespace-константой (прецеденты — ранее в плане).

---

## Статус исполнения (фиксация фактов, код не переписывается)

### Часть А (AC12) + часть Б (параллельность) — ИСПОЛНЕНЫ

| Фаза | Суть | Статус / факты | Коммит(ы) |
|---|---|---|---|
| A0/A1 | диагностика H1–H3; правка AC12 рев.1 (§5.1–5.4) | ВЫПОЛНЕНО | `0622199c`, `5474c7b3`, `3df9e280`, `a8c7bd59`, `d089ecc0` |
| B1–B6 | параллельность: runner.json 5, per-class коллекции/фикстуры/окна портов, ClaimStoreTests, EtcdCollection, Confluent.Kafka 2.15.1 (§6.3, §6.4) | ВЫПОЛНЕНО | `70f76802`, `daa2ca60`, `e59273a2`, `69c3e8e0`, `d872447a`, `470a163d` |
| B7.1–B7.2 | сборка solution 0 warnings; юниты 343/343 (§9.1 для той ревизии) | ВЫПОЛНЕНО | — |
| B7.3 | изолированный прогон — УПАЛ по H5 (лог `rotto-4`); зачистка 0/0 | ЗАКРЫТО правкой A2 | — |
| A2 | Act2: ветвление живой/вырожденный путь + различительная валидация RF=1 + PollMs=250/TickIntervalMs=1000 (§5.2, §5.6) | ВЫПОЛНЕНО | `a4d4c125` |
| A3 | изолированный приём: 3/3 зелёные за 1 м 39 с; различимость пути: `degenerate=1 live=0` (обе формы валидны) | ВЫПОЛНЕНО | — |
| B7R.1–B7R.6 | полный параллельный прогон: **72/73 за 5 м 13 с**; 21 пара перекрытий классов (TRX, python с NS); маркеры: found 2 = 0, sigabrt = 0, **destroyed = 1**; зачистка 0/0; diff-область чистая; сверка AC12 по обоим путям — пройдена | ВЫПОЛНЕНО С ОСТАТКОМ: единственный красный — `ReassignmentTests.Reassign_Повторная_Подача_Безопасна` [24 с]: «Kafka AdminClient (localhost:22024-22026): Broker handle destroyed without termination» в UpAsync — ВНУТРИ одного класса, порты собственного окна; в изолированном прогоне зелёный; диагноз (дважды: problem-solver + кодер) — прод-дефект фабрики (dispose заменяемого под висящей операцией, §6.4) → часть В | — |

AC12 (часть А) закрыт: изолированно 3/3, в полном прогоне зелёный, путь различим. Дальнейшие задачи — только часть В.

---

## Task A4: Прод-фикс фабрики AdminClient — arch-first (spec §6.4; фаза A4 §7)

**Files:**
- Modify: `<WT>/arch/16-kafkaworker.md` (§6 «Надёжность», пункт «AdminClient-кэш (t05)», ~строки 830–836)
- Modify: `<WT>/src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (фабрика `Create` ~строки 74–77, `EvictIdle` ~строки 90–100, `Dispose` ~строки 105–117; адаптер `KafkaAdminClient`: поля ~строки 168–184, `RunAsync` ~строки 396–424, `DisposeNative` ~строки 233–243)
- Modify: `<WT>/src/tests/KafkaWorker.UnitTests/Kafka/KafkaAdminClientFactoryTests.cs` (новые тесты дрейна)

**Interfaces:**
- Consumes: `KafkaAdminClient(bootstrap, user, password, caPem, requestTimeout, ILogger?, Action? onFailed)`; `Entry(KafkaAdminClient)`; `InternalsVisibleTo KafkaWorker.UnitTests` (уже в csproj — internal-seam доступен юнитам).
- Produces (точные сигнатуры):
  - адаптер: `internal static TimeSpan DrainGrace(TimeSpan requestTimeout)`; `internal Task DisposeWhenIdleAsync(TimeSpan budget, CancellationToken ct)`; `internal Task<Result> RunUnderOperationAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)` (тестовый шов — каркас `RunAsync` без построения нативного клиента); `internal int ActiveOperations { get; }`; `internal int DisposeNativeCount` (тестовое наблюдение диспозов).
  - фабрика: `internal void Dispose(TimeSpan grace)`; публичный `Dispose()` → `Dispose(DrainGrace(requestTimeout))`.

**Вход (предусловие):** ветка `fix-kfw-rotation-timeout-flake` на `a4d4c125`, рабочий каталог чист (кроме каталога задачи); логи-улики в `/tmp` не удалять.

**Действие:** строго по шагам: (A4.1) arch/16 §6 — канон диспоза заменяемого «дрейн с бюджетом»; (A4.2) код: счётчик активных операций в каркасе `RunAsync`, запись удаляется из кеша мгновенно, dispose заменяемого — фоном по нулю операций с бюджетом `requestTimeout+30 с` → принудительный + warning; `Dispose()` фабрики — тот же дрейн; (A4.3) юнит-тесты дрейна (AAA, по-русски) + сверка существующих 8 тестов фабрики на непротиворечие; (A4.4) сборка 0 warnings + все юниты зелёные + коммит.

**Выход:** dispose нативного клиента под висящей операцией исключён по построению; замена/раздача не блокируются; остановка детерминирована.

**Проверка:** шаги A4.1–A4.4 (каждый со своей командой/критерием).

**Связь со spec:** §6.4 (дефект, arch-first, дизайн фикса, юнит-покрытие); §4 п.8 (нативные ресурсы не уничтожаются под работой); §7 фаза A4; §8 (production-ready, порядок arch-first); §9.1/§9.6.

- [ ] **Step A4.1: arch-first — правка arch/16 §6 (ПЕРВОЙ, до кода)**

В `<WT>/arch/16-kafkaworker.md` §6 заменить пункт:

```markdown
- **AdminClient-кэш (t05)**: фабрика — sharable-кэш per
  (bootstrap, user, password); Create — «получить клиент ключа», DisposeAsync
  адаптера — no-op (владение у кэша); пины librdkafka
  `reconnect/retry.backoff.ms ≥ 1000` (+ rdkafka-лог на Debug); Failed
  операции → unhealthy-инвалидация → пересоздание (Dispose заменяемого —
  в фоне); неактивные ключи вытесняются; остановка — детерминированный
  Dispose (кэш = IDisposable DI-синглтон). Инцидент-класс: t11 (панель).
```

на:

```markdown
- **AdminClient-кэш (t05)**: фабрика — sharable-кэш per
  (bootstrap, user, password); Create — «получить клиент ключа», DisposeAsync
  адаптера — no-op (владение у кэша); пины librdkafka
  `reconnect/retry.backoff.ms ≥ 1000` (+ rdkafka-лог на Debug); Failed
  операции → unhealthy-инвалидация → пересоздание (замена немедленна, запись
  удаляется из кэша мгновенно; Dispose заменяемого — в фоне ПОСЛЕ завершения
  его висящих операций: дрейн по счётчику активных операций адаптера, бюджет
  requestTimeout + хвост, по исчерпании — принудительный dispose с warning;
  dispose под висящей операцией запрещён — нативный ERR__DESTROYED; раздача
  новых клиентов дрейном не блокируется); неактивные ключи вытесняются
  (dispose вытесняемого — тот же дрейн); остановка — детерминированный
  Dispose (кэш = IDisposable DI-синглтон; тот же дрейн с бюджетом по каждому
  клиенту — вечного ожидания нет). Инцидент-класс: t11 (панель).
```

- [ ] **Step A4.2: Фикс KafkaAdminClient.cs**

**(а) Адаптер — поля и шов наблюдения.** После блока `_clientGate` (~строки 171–175) добавить:

```csharp
    // Счётчик активных операций и дрейн-dispose (arch/16 §6): dispose нативного
    // клиента под висящей операцией запрещён (ERR__DESTROYED) — заменяемый
    // клиент диспозится фоном по нулю операций (continuation последней).
    private int _activeOperations;

    // Ожидание «операций нет» (ставится DisposeWhenIdleAsync; завершается
    // EndOperation при нуле). RunContinuationsAsynchronously — continuation
    // дрейна не выполняется в потоке операции.
    private TaskCompletionSource? _drainTcs;

    // Тестовое наблюдение (юниты дрейна; InternalsVisibleTo): сколько раз
    // исполнялся DisposeNative и сколько операций висит сейчас.
    internal int DisposeNativeCount;

    internal int ActiveOperations => Volatile.Read(ref _activeOperations);
```

**(б) Счётчик в каркасе RunAsync.** Заменить оба каркаса (строки 396–424):

```csharp
    private async Task<Result> RunAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            return await action(EnsureClient());
        }
        catch (Exception e)
        {
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
            return Result.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
    }

    private async Task<Result<T>> RunAsync<T>(Func<IAdminClient, Task<T>> action, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            return Result<T>.Success(await action(EnsureClient()));
        }
        catch (Exception e)
        {
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
            return Result<T>.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
    }
```

на:

```csharp
    // Общий каркас: клиент создаётся один раз; исключения → Result.Failed
    // (+ unhealthy-пометка записи кэша, кроме отмены host'а). Операция
    // стоит на счётчике всё время выполнения (arch/16 §6): дрейн-dispose
    // заменяемого ждёт нуля — dispose под висящей операцией исключён.
    private async Task<Result> RunAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
    {
        BeginOperation();
        try
        {
            ct.ThrowIfCancellationRequested();
            return await action(EnsureClient());
        }
        catch (Exception e)
        {
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
            return Result.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
        finally
        {
            EndOperation();
        }
    }

    private async Task<Result<T>> RunAsync<T>(Func<IAdminClient, Task<T>> action, CancellationToken ct)
    {
        BeginOperation();
        try
        {
            ct.ThrowIfCancellationRequested();
            return Result<T>.Success(await action(EnsureClient()));
        }
        catch (Exception e)
        {
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
            return Result<T>.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
        finally
        {
            EndOperation();
        }
    }

    // Счётчик операций: инкремент до действия, декремент в finally —
    // исключение/отмена тоже освобождают слот. Ноль при поставленном
    // ожидании дрейна завершает его (continuation последней операции).
    private void BeginOperation() => Interlocked.Increment(ref _activeOperations);

    private void EndOperation()
    {
        if (Interlocked.Decrement(ref _activeOperations) != 0)
            return;

        TaskCompletionSource? waiter;
        lock (_clientGate)
        {
            waiter = _drainTcs;
            _drainTcs = null;
        }

        waiter?.TrySetResult();
    }
```

**(в) DisposeNative — счётчик наблюдения.** Заменить (строки 233–243):

```csharp
    internal void DisposeNative()
    {
        lock (_clientGate)
        {
            _client?.Dispose();
            _client = null;
        }
    }
```

на:

```csharp
    internal void DisposeNative()
    {
        lock (_clientGate)
        {
            DisposeNativeCount++;
            _client?.Dispose();
            _client = null;
        }
    }
```

**(г) Дрейн-dispose.** Сразу после `DisposeNative` добавить:

```csharp
    // Dispose после завершения висящих операций (arch/16 §6): операций нет —
    // немедленно; есть — фоном ждём нуля (continuation последней) с бюджетом;
    // бюджет исчерпан — принудительный dispose + warning («не ждать вечно»,
    // диагностика вместо зависания). Гарантии сети не меняются: замена и
    // раздача клиентов уже произошли (запись удалена из кеша ДО этого вызова).
    internal async Task DisposeWhenIdleAsync(TimeSpan budget, CancellationToken ct)
    {
        Task? idle;
        lock (_clientGate)
        {
            if (_activeOperations == 0)
            {
                idle = null; // операций нет — dispose сразу (ниже, вне гейта)
            }
            else
            {
                _drainTcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                idle = _drainTcs.Task;
            }
        }

        if (idle is null)
        {
            DisposeNative();
            return;
        }

        var finished = await Task.WhenAny(idle, Task.Delay(budget, ct)).ConfigureAwait(false);
        if (finished != idle)
            log?.LogWarning(
                "Kafka AdminClient ({Bootstrap}): бюджет дрейна dispose {Budget:F0} с исчерпан при {Active} активных операциях — принудительный dispose",
                bootstrap, budget.TotalSeconds, Volatile.Read(ref _activeOperations));

        DisposeNative(); // дрейн дождался либо бюджет — dispose в любом случае
    }

    // Бюджет дрейна dispose (arch/16 §6): операции сами ограничены
    // requestTimeout (передаётся в каждый Options.RequestTimeout), хвост —
    // на завершение continuation'ов/выход poll-потоков.
    internal static TimeSpan DrainGrace(TimeSpan requestTimeout)
        => requestTimeout + TimeSpan.FromSeconds(30);

    // Тестовый шов (юниты дрейна): действие в каркасе RunAsync — счётчик
    // операций и unhealthy-пометка работают как в проде; нативный клиент
    // не строится (действие решает само, звать ли EnsureClient).
    internal Task<Result> RunUnderOperationAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
        => RunAsync(action, ct);
```

**(д) Фабрика — замена и вытеснение через дрейн.** В `Create` заменить (строки 74–75):

```csharp
            if (_entries.Remove(key, out var replaced))
                Task.Run(replaced.Client.DisposeNative); // заменяемый — фон (Dispose ждёт poll-поток)
```

на:

```csharp
            if (_entries.Remove(key, out var replaced))
            {
                // Замена немедленна (запись удалена из кеша); dispose
                // заменяемого — фоном ПОСЛЕ его висящих операций (arch/16 §6,
                // ERR__DESTROYED под операцией запрещён).
                Task.Run(() => replaced.Client.DisposeWhenIdleAsync(
                    DrainGrace(requestTimeout), CancellationToken.None));
            }
```

В `EvictIdle` заменить (строки 99–100):

```csharp
        foreach (var entry in evicted)
            Task.Run(entry.Client.DisposeNative);
```

на:

```csharp
        foreach (var entry in evicted)
            Task.Run(() => entry.Client.DisposeWhenIdleAsync(
                DrainGrace(requestTimeout), CancellationToken.None));
```

**(е) Dispose фабрики — дрейн с бюджетом.** Заменить (строки 105–117):

```csharp
    public void Dispose()
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        foreach (var entry in removed)
            entry.Client.DisposeNative();
    }
```

на:

```csharp
    public void Dispose()
        => Dispose(DrainGrace(requestTimeout));

    // Остановка host'а — детерминированно: тот же дрейн с бюджетом по
    // каждому клиенту (arch/16 §6) — без операций немедленно, с операциями
    // не дольше бюджета (записей немного — параллельно, WaitAll).
    internal void Dispose(TimeSpan grace)
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        Task.WaitAll(removed.Select(
            e => e.Client.DisposeWhenIdleAsync(grace, CancellationToken.None)).ToArray());
    }
```

(Клиенты с висящими операциями после бюджета диспозятся принудительно — с warning; клиенты без операций — мгновенно.)

- [ ] **Step A4.3: Юнит-тесты дрейна (AAA, комментарии по-русски)**

Добавить в `<WT>/src/tests/KafkaWorker.UnitTests/Kafka/KafkaAdminClientFactoryTests.cs` (вверху файла добавить `using System.Diagnostics;`):

```csharp
    // ===== Дрейн-dispose заменяемого (arch/16 §6): dispose под висящей
    // операцией запрещён (ERR__DESTROYED) — тесты через шов
    // RunUnderOperationAsync (нативный клиент не строится). =====

    // AAA: замена Unhealthy при висящей операции НЕ диспозит клиент — dispose
    // ждёт завершения операции (дрейн).
    [Fact]
    public async Task UnhealthyReplace_DoesNotDisposeUnderActiveOperation()
    {
        // Arrange: клиент в кэше; операция висит (TCS управляет завершением).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(_ => gate.Task, CancellationToken.None);
        first.ActiveOperations.Should().Be(1, "операция на счётчике");

        // Act: пометка Failed + замена (фоновый дрейн обязан ждать нуля).
        first.NotifyFailed();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: замена мгновенна (новый клиент), dispose НЕ исполнился —
        // операция первого всё ещё висит.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
        first.DisposeNativeCount.Should().Be(0, "dispose под висящей операцией запрещён");

        // Cleanup: отпустить операцию — фоновый дрейн завершится сам.
        gate.TrySetResult(Result.Success());
        await operation;
    }

    // AAA: дрейн исполняет dispose по завершении последней операции
    // (continuation), не дожидаясь бюджета.
    [Fact]
    public async Task DisposeWhenIdle_WaitsForLastOperation_ThenDisposes()
    {
        // Arrange: клиент (вне фабричной замены — наблюдаем детерминированно);
        // дрейн запущен при висящей операции.
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(3));
        var gate = new TaskCompletionSource<Result>();
        var operation = client.RunUnderOperationAsync(_ => gate.Task, CancellationToken.None);
        var drain = client.DisposeWhenIdleAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        await Task.Delay(200);
        drain.IsCompleted.Should().BeFalse("дрейн ждёт висящую операцию");

        // Act: операция завершается.
        gate.SetResult(Result.Success());
        await operation;
        await drain;

        // Assert: dispose исполнен по нулю операций — без исчерпания бюджета.
        client.DisposeNativeCount.Should().Be(1);
        client.ActiveOperations.Should().Be(0);
    }

    // AAA: бюджет дрейна — по исчерпании принудительный dispose (не ждать
    // вечно), операция при этом остаётся при своём исходе.
    [Fact]
    public async Task DisposeWhenIdle_BudgetExhausted_ForcesDispose()
    {
        // Arrange: операция висит дольше бюджета.
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(3));
        var gate = new TaskCompletionSource<Result>();
        var operation = client.RunUnderOperationAsync(_ => gate.Task, CancellationToken.None);

        // Act: дрейн с коротким бюджетом.
        var drain = client.DisposeWhenIdleAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);
        await drain;

        // Assert: принудительный dispose по бюджету; операция не сорвана
        // исключением — завершится своим исходом.
        client.DisposeNativeCount.Should().Be(1, "бюджет исчерпан — принудительный dispose");

        // Cleanup.
        gate.TrySetResult(Result.Success());
        var done = await operation;
        done.IsSuccess.Should().BeTrue("операция завершилась своим исходом, не ERR__DESTROYED");
    }

    // AAA: Create не блокируется дрейном заменяемого (раздача новых клиентов
    // жива, пока старый дрейнит).
    [Fact]
    public async Task Create_NotBlockedByDrainOfReplaced()
    {
        // Arrange: заменяемый клиент с висящей операцией (дрейн ждёт).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(_ => gate.Task, CancellationToken.None);
        first.NotifyFailed();
        factory.Create("h:9092", "app", "pw", caPem: null); // замена запустила фоновый дрейн

        // Act: новые Create по ДРУГОМУ ключу и по ключу заменённого — замер.
        var sw = Stopwatch.StartNew();
        var fresh = factory.Create("h2:9092", "app", "pw", caPem: null);
        var again = factory.Create("h2:9092", "app", "pw", caPem: null);
        sw.Stop();

        // Assert: раздача мгновенна (не ждёт дрейна), reuse нового ключа жив.
        sw.ElapsedMilliseconds.Should().BeLessThan(3000, "Create не блокируется дрейном");
        again.Should().BeSameAs(fresh);

        // Cleanup.
        gate.TrySetResult(Result.Success());
        await operation;
    }

    // AAA: Dispose фабрики при висящей операции дожидается её завершения
    // (дрейн с позитивным исходом — без принуждения) и детерминированно
    // возвращается.
    [Fact]
    public async Task FactoryDispose_DrainsActiveOperation_Deterministically()
    {
        // Arrange: клиент с операцией, освобождающейся через 200 мс.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(
            async _ =>
            {
                await Task.Delay(200);
                return await gate.Task;
            }, CancellationToken.None);

        // Act: остановка фабрики (внутренний seam — короткий бюджет теста).
        var disposeTask = Task.Run(() => factory.Dispose(TimeSpan.FromSeconds(5)));
        gate.TrySetResult(Result.Success());
        var finished = disposeTask.Wait(TimeSpan.FromSeconds(5));

        // Assert: Dispose вернулся (не висит) — дождался операции, без
        // принуждения бюджет не потребовался.
        finished.Should().BeTrue("Dispose детерминирован: дрейн с бюджетом, не вечное ожидание");
        first.DisposeNativeCount.Should().Be(1, "клиент задиспожен по завершении операции");
        await operation;
    }
```

Затем — сверка существующих тестов файла на непротиворечие (по коду, без правок): `Create_AfterFailure_CreatesFreshClient` (замена без операций → фоновый дрейн мгновенно диспозит — ассерты NotSame/CreatedClients не затронуты), `Dispose_ThenCreate_BuildsFreshClient` (Dispose без операций → мгновенный дрейн), `Create_EvictsIdleEntries` (вытеснение без операций → мгновенный дрейн) — остаются зелёными; остальные тесты механику диспоза не касаются. Противоречий нет.

- [ ] **Step A4.4: Сборка solution 0 warnings + ВСЕ юниты зелёные + коммит (в фоне, task_id; старт тестов ≤15 с, контроль каждые 2 мин)**

```bash
cd <WT> && DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --no-build
```

Ожидание: `Build succeeded`, `0 Warning(s)`; юниты — все зелёные, включая 5 новых тестов дрейна и 8 прежних тестов фабрики. Падение — стоп, разбор (не перезапуск без согласия). Коммит:

```bash
cd <WT> && git add arch/16-kafkaworker.md src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs src/tests/KafkaWorker.UnitTests/Kafka/KafkaAdminClientFactoryTests.cs && git commit -m "fix(kfw): дрейн-dispose заменяемого AdminClient — arch/16 §6 + счётчик операций + бюджет (spec 6.4)"
```

---

## Task A5: Повторная приёмка — изолированный + полный параллельный прогон (spec §9; фаза A5 §7)

**Files:** — (прогоны; артефакты — логи + TRX)

**Вход (предусловие):** Task A4 слит; образы на хосте; dev-стенд НЕ поднят параллельно (поднят — вопрос через main-агента); `docker network ls --format '{{.Name}}' | grep -c '^kfw-net'` → 0; `ls /tmp/pgw-kfw-rotto-*` — выбрать следующий свободный номер `<n>` (1–5 заняты).

**Действие:** изолированный прогон `RotationTimeoutTests`; полный параллельный прогон 73/73 с TRX; свидетельство перекрытия; маркеры found2=0/destroyed=0/sigabrt=0; точечная зачистка; контроль diff-области (теперь вкл. `KafkaAdminClient.cs`, `arch/16-kafkaworker.md`); сводка B3.

**Выход:** §9 п.1–п.6 закрыты с доказательствами.

**Проверка:** шаги ниже; падение — стоп, улики ДО зачистки, разбор, повтор только с согласия (§9.7).

**Связь со spec:** §9 п.1 (юниты вкл. новые), п.2 (подтверждение в составе полного — закрыто A3/B7R, изолированный повтор — страховка объёма), п.3 (73/73 + destroyed=0), п.4, п.5 (закрыто построчной сверкой B7R.6 — не дублируется), п.6; §7 фазы A5/B3.

- [ ] **Step A5.1: Изолированный прогон RotationTimeoutTests (в фоне, task_id; старт ≤15 с, контроль каждые 2 мин)**

```bash
cd <WT> && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~RotationTimeoutTests" 2>&1 | tee /tmp/pgw-kfw-rotto-<n>.log
```

Ожидание: 3/3 зелёные (ориентир по A3: ~1–2 мин).

- [ ] **Step A5.2: Зачистка (точечно, только kfw-*)**

`docker ps -a --format '{{.Names}}' | grep -c 'kfw-'` → 0; при остатке сетей: `docker network ls --format '{{.Name}}' | grep '^kfw-net' | while IFS= read -r n; do docker network rm "$n"; done`, контроль `grep -c '^kfw-net'` → 0. `docker network prune -f` НЕ использовать.

- [ ] **Step A5.3: Полный параллельный прогон + TRX (в фоне, task_id; старт ≤15 с, контроль каждые 2 мин; затяжные фазы >5 мин — анализ docker-логов без остановки)**

```bash
cd <WT> && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --logger "trx;LogFileName=pgw-kfw-full-par-a5.trx" --results-directory /tmp/pgw-kfw-trx 2>&1 | tee /tmp/pgw-kfw-full-par-a5.log
```

Ожидание: **73/73 зелёные** (ориентир по B7R.1: ~5–6 мин). До финальной строки — никаких новых запусков.

- [ ] **Step A5.4: Маркеры среды (§9 п.3 — destroyed теперь обязан быть 0)**

```bash
grep -c "found 2" /tmp/pgw-kfw-full-par-a5.log; grep -cF "Broker handle destroyed" /tmp/pgw-kfw-full-par-a5.log; grep -ci "sigabrt" /tmp/pgw-kfw-full-par-a5.log
```

Ожидание: `0`, `0`, `0` (BSD grep: `-cF` без альтернатив).

- [ ] **Step A5.5: Свидетельство параллельности — перекрытие классов по TRX (python с NS-константой)**

```bash
python3 - <<'PY'
import re, xml.etree.ElementTree as ET
from datetime import datetime

def ts(x):
    return datetime.fromisoformat(re.sub(r'\.(\d{6})\d*', r'.\1', x))

root = ET.parse('/tmp/pgw-kfw-trx/pgw-kfw-full-par-a5.trx').getroot()
NS = '{http://microsoft.com/schemas/VisualStudio/TeamTest/2010}'
tests = []
for ut in root.iter(NS + 'UnitTestResult'):
    s, e = ut.get('startTime'), ut.get('endTime')
    if s and e:
        cls = ut.get('testName').rsplit('.', 1)[0]
        tests.append((ts(s), ts(e), cls))
tests.sort()
overlapped = []
for i, (s1, e1, c1) in enumerate(tests):
    for s2, e2, c2 in tests[i + 1:]:
        if s2 >= e1:
            break
        if c2 != c1:
            overlapped.append((c1, c2))
            break
pairs = sorted(set(overlapped))
print('пар перекрывающихся по времени разных классов:', len(pairs))
for p in pairs[:10]:
    print('  ', p[0], '<->', p[1])
assert pairs, 'перекрытия классов нет — параллельность не доказана'
PY
```

Ожидание: ≥ 1 перекрывающаяся пара (ориентир по B7R.2: ~21).

- [ ] **Step A5.6: Зачистка после серии (точечно, только kfw-*)** — как Step A5.2.

- [ ] **Step A5.7: Контроль области изменений (§9 п.6 — расширенная область части В)**

```bash
cd <WT> && git status --porcelain && git diff main...HEAD --stat
```

Ожидание: изменения ТОЛЬКО в `src/tests/KafkaWorker.IntegrationTests/**`, `src/tests/KafkaWorker.UnitTests/**`, `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs`, `arch/16-kafkaworker.md`, `src/Directory.Packages.props` (одна строка) и `docs/superpowers/2026-10-17-kfw-rotation-timeout-flake/`. Иного прод-кода и `arch/**` — нет.

- [ ] **Step A5.8: Сводка (фаза B3, §7)**

Журнал исполнения: коммиты A1/B1–B6/A2/A4; фактология приёмок (A3: 3/3, degenerate=1/live=0; B7R.1: 72/73, destroyed=1 → часть В; A5: юниты вкл. дрейн-тесты, 73/73, маркеры 0/0/0, перекрытия, зачистки 0/0, diff-область). Мерж/пуш — только по явному требованию пользователя.

---

## Self-Review (выполнен автором плана)

1. **Покрытие spec (ревизия с частью В):** §6.4 arch-first → Step A4.1 (точная замена пункта §6 arch/16: «дрейн с бюджетом», смысл «замена немедленна, dispose не в горячем пути» сохранён, вытеснение и остановка — тот же дрейн); §6.4 дизайн фикса → Step A4.2 (счётчик Interlocked в каркасе RunAsync обоих арностей; запись удаляется мгновенно; dispose заменяемого фоном по нулю с continuation; бюджет `DrainGrace = requestTimeout + 30 с` — значение из плана по букве спеки «requestTimeout + хвост»; принудительный dispose + warning; `Dispose()` фабрики — тот же дрейн; Unhealthy/пересоздание немедленны, DisposeAsync no-op, пины не тронуты); §6.4 юнит-покрытие → Step A4.3 (все 5 заявленных спекой поведений: не под операцией / по завершению / бюджет / Create не блокируется / Dispose фабрики; шов `RunUnderOperationAsync` без натива; сверка 8 существующих тестов — непротиворечие отмечено); §7 фаза A4 → Task A4, A5 → Task A5; §9 п.1 → A4.4+A5.3-состав, п.2 → A3 (закрыто) + A5.1-страховка, п.3 → A5.3–A5.5 (destroyed=0 — прямой критерий фикса), п.4 → A5.2/A5.6, п.5 → закрыто B7R.6 (не дублируется), п.6 → A5.7 (расширенная область), п.7 → Global Constraints. Пробелов нет.
2. **Точность кода:** старые фрагменты сверены с текущим `KafkaAdminClient.cs` (Create :74–75, EvictIdle :99–100, Dispose :105–117, RunAsync :396–424, DisposeNative :233–243); `InternalsVisibleTo KafkaWorker.UnitTests` подтверждён в csproj (internal-seam легален); `log` — поле адаптера (warning без нового параметра); вложенность lock исключена (`DisposeNative` зовётся вне `_clientGate`); `Task.WhenAny` с `Task.Delay(budget, ct)` — при отмене токена даёт принудительный dispose (детерминированность остановки).
3. **Плейсхолдеры:** нет; единственная параметризация — `<n>` номера лога с процедурой выбора.
4. **Типы/сигнатуры:** `DrainGrace(TimeSpan) → TimeSpan`, `DisposeWhenIdleAsync(TimeSpan, CancellationToken) → Task`, `RunUnderOperationAsync(Func<IAdminClient, Task<Result>>, CancellationToken) → Task<Result>`, `ActiveOperations`/`DisposeNativeCount` (internal), фабрика `Dispose()` → `Dispose(TimeSpan)` — все использования в A4.2/д/е и A4.3 согласованы; `IKafkaAdminClient` не меняется.
5. **Незапланированных решений нет:** дизайн — из spec §6.4 (выбор «отложенный dispose» обоснован спекой); значение хвоста бюджета 30 с — деталь плана, разрешённая спекой («конкретное значение — план»).
