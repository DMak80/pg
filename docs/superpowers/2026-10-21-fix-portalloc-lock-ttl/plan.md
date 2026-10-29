# План: пер-попыточный таймаут docker-транспорта — фикс пробоя TTL 15 с глобального portalloc-лока

> **Для исполняющих агентов:** план исполняется по шагам; REQUIRED SUB-SKILL:
> superpowers:subagent-driven-development или superpowers:executing-plans.
> Шаги отмечены чекбоксами (`- [ ]`).
>
> **Режим исполнения (executor="coder"):** план исполняет субагент coder —
> сильная модель, работает ОДИН, субагентов не создаёт. Подпункты для слабой
> модели (блоки «ПОДПУНКТЫ ДЛЯ СЛАБОЙ МОДЕЛИ» в конце шагов) для coder —
> обычные инструкции: делать точно по их тексту, без отсебятины.

**Цель:** задать пер-попыточный таймаут `HttpClient` (7.5 с) в
`DockerEngineFactory.Create` — устранить молчание docker-транспорта до
дефолтных 100 с, из-за которого зависший docker-вызов внутри секции
довыделения удерживает глобальный portalloc-лок дольше TTL 15 с (гонка
коллизий портов) и валит воркер watchdog-ом (ложный «тихий» вызов > 30 с).

**Архитектура:** одна точка — `Shared.Docker/Engine/DockerEngineFactory.cs`:
internal-константа `CallTimeout` (7.5 с) + применение `Timeout` в единственном
создании docker-`HttpClient` + internal-перегрузка `Create` с настраиваемым
таймаутом как тестовое окно. Публичный API, `PortAllocLock`, процессы воркеров,
`arch/**`, `docs/**` — без изменений. Тиковая модель сама превращает
таймаут-исключение в transient-фейл (`Result.FromAsync` → `Result.Failed` →
повтор тиком; идемпотентность docker-вызовов — канон).

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions.

**Spec:** `docs/superpowers/2026-10-21-fix-portalloc-lock-ttl/spec.md`
(исполнитель читает spec и этот план вместе; все решения — из spec Р1–Р8,
развилок у исполнителя нет).

## Глобальные ограничения (из spec §2 и AGENTS.md)

- Правки ТОЛЬКО в двух файлах: `src/Shared.Docker/Engine/DockerEngineFactory.cs`
  и новом `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineFactoryCallTimeoutTests.cs`
  (критерий приёмки spec §7.6).
- `PortAllocLock` (TTL 15 с, без keepalive), процессы воркеров, `arch/**`,
  `docs/**` — НЕ трогать.
- Публичный API фабрики неизменен: `Create(string endpoint, string? hostAlias = null)`
  сигнатура и поведение; `CallTimeout` и перегрузка с таймаутом — internal
  (`InternalsVisibleTo("Shared.Docker.UnitTests")` уже объявлен в
  `src/Shared.Docker/Shared.Docker.csproj:4`).
- Комментарии новых строк — по-русски; идентификаторы — английские; тесты — с
  AAA-комментариями (Arrange/Act/Assert).
- `TreatWarningsAsErrors=true` — сборка без единого предупреждения.
- Новых NuGet-пакетов нет (централизованное версионирование
  `Directory.Packages.props` не трогаем).
- Тесты короткие: без долгих ожиданий (функциональный тест — таймаут 300 мс,
  ассерт elapsed ≤ 3 с); порты — только динамические (порт 0 +
  `listener.LocalEndpoint`, без литералов).
- Команды сборки/тестов — по канону проекта: env-префикс при самой команде
  (`DOTNET_CLI_UI_LANGUAGE=en ...`), вывод длинных команд — в файл лога
  (`> /tmp/<имя>.log 2>&1`), читать лог из файла; запуск/мониторинг — по
  базовым правилам AGENTS.base.md §4/§13 (фон, task_id, контроль старта и
  хода, ожидания ≤ 30 с на цикл проверки).

## Review Focus

Классы входов/отказов, подразумеваемые spec, и чем закрыт каждый:

1. **Молчащий docker-демон** (соединение принято, ответа нет) — вызов движка
   обязан умереть таймаутом транспорта, а не молчать 100 с: функциональный тест
   `Create_SilentServer_PingFailsByTransportTimeout` (Шаг 2).
2. **Невнимательная правка константы** (значение уползло от канона «полокна
   watchdog» 7.5 с) — структурный тест `CallTimeout_CanonValue_75Seconds`
   (Шаг 2).
3. **Regression публичного пути**: plaintext tcp без TLS не сломан
   перестройкой `Create` — существующий `Factory_NoTls_PlainTcpHandlerWithoutSslOptions`
   (полный прогон `Shared.Docker.UnitTests`, Шаг 3); unix/ssh/tls-ветки —
   код-ревью Шага 1 (тело перенесено без изменений, поменяна только строка
   создания `HttpClient`).
4. **Пробой TTL лока на живом контуре** — не пинается юнитами (нужен реальный
   etcd+docker+второй инстанс); закрыт самим механизмом (7.5 с ≪ 15 с —
   арифметика spec §5) и E2E-гейтом `Scale_AddEmptyShard` на свежем Release
   (Шаг 4: provisioning-путь довыделения портов проходит через фабрику).
5. **Долгие вызовы движка** (`PullImageAsync`, logs, stop) под единым
   таймаутом 7.5 с — осознанное решение spec Р8/§5 (идемпотентность +
   transient-фейл); отдельных тестов НЕ добавлять (выход за scope Р6).

---

## Шаг 1. Прод-код: константа `CallTimeout` + применение в `Create` + internal-перегрузка

**Вход:** worktree `fix-portalloc-lock-ttl` чист (кроме `docs/` этой задачи);
spec одобрен; файл `src/Shared.Docker/Engine/DockerEngineFactory.cs` в
исходном состоянии (метод `Create` — строки 85–99, `HttpClient` создаётся в
одной строке 97).

**Действие (сильная модель):** выполнить подпункты 1.1 и 1.2 (последовательно,
в одном порядке), затем сделать ревью результата: (а) тело internal-перегрузки
— дословно прежнее тело `Create`, изменена ТОЛЬКО инициализация `HttpClient`
(добавлен `Timeout = callTimeout`, init-объект разложен на несколько строк);
(б) публичная обёртка — `public virtual`, сигнатура прежняя; (в) константа —
`internal static readonly`, комментарий дословно из подпункта; (г) никаких
иных правок файла. Исправить расхождения, если подпункты выполнены неточно.

**Выход:** фабрика задаёт пер-попыточный таймаут каждому созданному
docker-`HttpClient`; `DockerEngineFactory.CallTimeout` доступен тестам через
`InternalsVisibleTo`.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/Shared.Docker/Shared.Docker.csproj -c Debug --nologo` —
успех, 0 ошибок, 0 предупреждений (`TreatWarningsAsErrors=true`).

**Связь со spec:** §3.1 (а, б, в), §4 фаза 1; решения Р1 (способ фикса), Р2
(7.5 с), Р7 (internal-перегрузка как тестовое окно), Р8 (единый таймаут на все
вызовы).

**ПОДПУНКТЫ ДЛЯ СЛАБОЙ МОДЕЛИ** (выполнять по порядку; файл один и тот же):

**1.1. Добавить константу `CallTimeout` в класс `DockerEngineFactory`.**

Файл: `src/Shared.Docker/Engine/DockerEngineFactory.cs`. Место: в классе
`DockerEngineFactory`, сразу после закрывающей скобки метода `CreateHandler`
(метод заканчивается строками `        return sockets;` и `    }`) и ПЕРЕД
комментарием `    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).`.
Вставь между ними (одна пустая строка до и после):

```csharp
    // Поллинг-инвариант (arch/14 §6): одиночный docker-вызов не молчит дольше
    // окна проверки watchdog — пер-попыточный таймаут = половина окна (7.5 с при
    // дефолтах), симметрия etcd/patroni (AddHttpClient воркеров). Зависший вызов →
    // исключение → transient-фейл тика, а не молчание до дефолтных 100 с HttpClient.
    // Гарантия для глобального portalloc-лока (arch/15 §4: без keepalive, секция
    // ≪ TTL 15 с): любой одиночный docker-чтение секции довыделения умирает
    // таймаутом задолго до истечения lease.
    internal static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(7.5);
```

Текст комментария — дословно как выше, не перефразировать.

**1.2. Разделить `Create` на публичную обёртку и internal-перегрузку.**

Файл: `src/Shared.Docker/Engine/DockerEngineFactory.cs`. Найди метод
`Create`, начинающийся с комментария `    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).`
и заканчивающийся строками `        return new DockerEngine(httpClient, hostAlias);`
и `    }`. Замени ВЕСЬ этот блок (комментарий + метод целиком) на:

```csharp
    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).
    public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
        => Create(endpoint, hostAlias, CallTimeout);

    // Тестовое окно: internal-перегрузка с настраиваемым пер-попыточным
    // таймаутом; публичный контракт Create(endpoint, hostAlias) неизменен.
    internal IDockerEngine Create(string endpoint, string? hostAlias, TimeSpan callTimeout)
    {
        var scheme = EndpointScheme.Parse(endpoint);
        if (scheme.Scheme == EndpointScheme.Ssh)
            endpoint = $"tcp://127.0.0.1:{TunnelFor(endpoint, scheme).BoundPort}";

        var parsedEndpoint = EndpointScheme.Parse(endpoint);
        var baseAddress = scheme.Scheme == EndpointScheme.Unix
            ? "http://localhost" // фиктивный хост: соединение уходит в unix-сокет через ConnectCallback
            : (_tls is not null
                ? $"https://{parsedEndpoint.Host}:{parsedEndpoint.Port}"
                : $"http://{parsedEndpoint.Host}:{parsedEndpoint.Port}"); // HttpClient не понимает tcp://
        var httpClient = new HttpClient(CreateHandler(endpoint))
        {
            BaseAddress = new Uri(baseAddress),
            Timeout = callTimeout,
        };
        return new DockerEngine(httpClient, hostAlias);
    }
```

Тело internal-перегрузки — дословно прежнее тело `Create` (ветки ssh/unix/tls
и вычисление `baseAddress` не менять), отличие только в инициализации
`HttpClient`: вместо одной строки с `BaseAddress` — init-объект с
`BaseAddress` и `Timeout = callTimeout`. Больше в файле ничего не менять.

- [x] Шаг 1 выполнен: сборка проекта чистая, ревью сильной моделью пройдено.

## Шаг 2. Юнит-тесты: `DockerEngineFactoryCallTimeoutTests` (структурный + функциональный)

**Вход:** Шаг 1 завершён (константа и internal-перегрузка существуют, сборка
чистая).

**Действие (сильная модель):** выполнить подпункт 2.1 (создать тест-файл
дословно), затем ревью: файл в `Engine/`, namespace и using — как у соседних
тестов (`DockerEngineFactoryTlsTests.cs`), оба теста `[Fact]`, AAA-комментарии,
никаких литералов портов (порт 0 + `LocalEndpoint`), teardown listener — в
`finally`. Исправить расхождения.

**Выход:** два зелёных юнит-теста, фиксирующих канон-значение константы и
проброс таймаута транспорта в вызовы движка.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj -c Debug --filter FullyQualifiedName~DockerEngineFactoryCallTimeoutTests` —
2 passed, 0 failed; суммарное время тестов — единицы секунд.

**Связь со spec:** §3.3 (А, Б), §4 фаза 2; решения Р6 (ровно два юнита, без
интеграционных docker-тестов), Р7 (300 мс через internal-перегрузку, без
рефлексии и «спящих 7.5 с»).

**ПОДПУНКТЫ ДЛЯ СЛАБОЙ МОДЕЛИ:**

**2.1. Создать новый тест-файл целиком.**

Создай файл `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineFactoryCallTimeoutTests.cs`
(каталог `Engine/` уже существует) с содержимым ДОСЛОВНО:

```csharp
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// Пер-попыточный таймаут docker-транспорта (arch/14 §6, arch/15 §4): константа
// канона «полокна проверки watchdog» и проброс таймаута HttpClient в вызовы
// движка — зависший docker-демон даёт transient-фейл, а не молчание до 100 с
// (окно гонки довыделения против TTL 15 с глобального portalloc-лока).
public class DockerEngineFactoryCallTimeoutTests
{
    [Fact]
    public void CallTimeout_CanonValue_75Seconds()
    {
        // Arrange / Act / Assert: канон — половина окна проверки watchdog (30 с),
        // симметрия etcd/patroni (7.5 с); защита от невнимательной правки.
        DockerEngineFactory.CallTimeout.Should().Be(TimeSpan.FromSeconds(7.5));
    }

    [Fact]
    public async Task Create_SilentServer_PingFailsByTransportTimeout()
    {
        // Arrange: молчащий TCP-сервер (принимает соединения, не отвечает) на
        // динамическом порту; движок с таймаутом 300 мс через internal-перегрузку.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var factory = new DockerEngineFactory();
            await using var engine = factory.Create(
                $"tcp://127.0.0.1:{port}", "h", TimeSpan.FromMilliseconds(300));
            var sw = Stopwatch.StartNew();

            // Act: одиночный вызов движка против молчащего сервера.
            var result = await engine.PingAsync(CancellationToken.None);
            sw.Stop();

            // Assert: таймаут транспорта пробрасывается в вызов — Failed быстро,
            // а не молчание до дефолтных 100 с HttpClient.
            result.IsSuccess.Should().BeFalse();
            sw.Elapsed.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(3));
        }
        finally
        {
            // Teardown: остановить listener при любом исходе.
            listener.Stop();
        }
    }
}
```

Больше ничего в тест-проекте не менять.

- [x] Шаг 2 выполнен: оба теста зелёные, ревью сильной моделью пройдено.

## Шаг 3. Локальная верификация: сборка Debug+Release решения, полный прогон Shared.Docker.UnitTests, коммит

**Вход:** Шаги 1–2 завершены.

**Действие (сильная модель; субпунктов нет — запуск команд и анализ вывода):**

1. Сборка Debug: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug --nologo > /tmp/pgw-fix-ttl-build-debug.log 2>&1` —
   прочитать лог из файла: успех, 0 предупреждений.
2. Сборка Release: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-fix-ttl-build-release.log 2>&1` —
   успех, 0 предупреждений (критерий приёмки spec §7.4).
3. Полный прогон юнитов транспорта:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests/Shared.Docker.UnitTests.csproj -c Debug > /tmp/pgw-fix-ttl-unittests.log 2>&1` —
   все тесты зелёные, включая существующие TLS/endpoint-тесты (регрессий
   перестройки `Create` нет; `dotnet test` сам пересоберёт — это локальный
   цикл, не E2E-гейт).
4. Падение/предупреждение — диагностировать по логам и коду, исправить
   (правки только в двух файлах плана), повторить с пункта 1.
5. Коммит в feature-ветке worktree (стиль репозитория —
   `тип(scope): суть — детали; Верификация: ...`):

```bash
git add src/Shared.Docker/Engine/DockerEngineFactory.cs src/tests/Shared.Docker.UnitTests/Engine/DockerEngineFactoryCallTimeoutTests.cs
git commit -m "fix(docker): пер-попыточный таймаут HttpClient в DockerEngineFactory.Create — CallTimeout 7.5 с (полокна проверки watchdog, симметрия etcd/patroni, arch/14 §6): зависший docker-вызов умирает таймаутом → transient-фейл тика вместо молчания до 100 с; устраняет окно гонки довыделения против TTL 15 с глобального portalloc-лока (arch/15 §4) и ложный снос воркера watchdog. Публичный API неизменен; тестовое окно — internal-перегрузка Create(endpoint, hostAlias, callTimeout). Верификация: сборка slnx Debug+Release без предупреждений; Shared.Docker.UnitTests зелёные (структурный константа 7.5 с + функциональный молчащий TCP-сервер 300 мс → PingAsync Failed ≤ 3 с)"
```

**Выход:** решение собирается в Debug и Release без предупреждений; юниты
транспорта зелёные; изменения закоммичены.

**Проверка:** логи трёх команд (читать из файлов) — все зелёные; `git log -1
--stat` — коммит содержит ровно два файла плана.

**Связь со spec:** §4 фаза 3, §7 критерии 3–4.

- [x] Шаг 3 выполнен.

## Шаг 4. Мерж-гейт E2E: статподготовка → сборка Release → `Scale_AddEmptyShard` → зачистка

**Вход:** Шаг 3 завершён (коммит есть). Задача трогает код воркеров
(`Shared.Docker` в поставке всех воркеров) — docker-E2E на свежем Release
обязателен (AGENTS.md). Команды — ОТДЕЛЬНЫЕ, строго в этом порядке; полный
E2eFixture НЕ требуется (транспорт не меняет provisioning/portalloc/moves —
spec Р6).

**Действие (сильная модель; субпунктов нет — запуск команд, мониторинг,
зачистка; каждую команду — в фон с фиксацией task_id, вывод в файл, мониторинг
по канону AGENTS.base.md §13: контроль старта по логу, проверки хода, без
пассивного ожидания):**

1. Статподготовка E2E (одним процессом, ДО тестов):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/pgw-fix-ttl-e2eprepare.log 2>&1` —
   успех: штамп готовности в конце лога (без него ВСЕ E2E-классы падают пачкой
   «статика E2E не подготовлена»).
2. Сборка Release (ОТДЕЛЬНОЙ командой; её вывод — часть гейта, свежесть
   бинаря видна здесь):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-fix-ttl-e2e-build.log 2>&1` —
   успех, 0 предупреждений.
3. Тесты — ТОЛЬКО `--no-build`, env-префикс при самой команде:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/pgw-fix-ttl-e2e-scale.log 2>&1` —
   PASS. Упавший сценарий — разбор ПО ЛОГАМ прогона (docker-логи/inspect и
   host.log снимаются teardown-ом в `/tmp/pgw-e2e-artifacts-*`), перезапуск
   упавших тестов без анализа логов и формулировки причин — запрещён
   (AGENTS.md, телеметрия E2E).
4. Зачистка после серии (страховочный гейт): проверить остатки —
   `docker ps -aq --filter name=pgw-` и `docker network ls | grep kfw-net`;
   при наличии остатков: `docker ps -aq --filter name=pgw- | xargs docker rm -f`,
   затем `docker network prune -f` (канон AGENTS.md: осиротевшие `kfw-net-*`
   исчерпывают пул подсетей docker). Убедиться, что после зачистки обе
   проверки пусты.

**Выход:** E2E-гейт зелёный на свежем Release-бинаре; контейнеры/сети серии
зачищены.

**Проверка:** логи `/tmp/pgw-fix-ttl-e2eprepare.log`, `/tmp/pgw-fix-ttl-e2e-build.log`,
`/tmp/pgw-fix-ttl-e2e-scale.log` — все успешны (в scale-логе — passed);
проверки остатков пусты.

**Связь со spec:** §4 фаза 4, §7 критерий 5; Р6 (маркер `Scale_AddEmptyShard`,
без полного E2eFixture).

- [x] Шаг 4 выполнен.

## Шаг 5. Приёмочная самопроверка: дифф, охват, критерии spec §7

**Вход:** Шаги 1–4 завершены.

**Действие (сильная модель; субпунктов нет):** прогнать по очереди и сверить
с критериями приёмки spec §7 (все семь):

1. `git diff main...HEAD --stat` (и `git status --short`) — правки ТОЛЬКО в
   `src/Shared.Docker/Engine/DockerEngineFactory.cs` и
   `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineFactoryCallTimeoutTests.cs`
   (критерий 6; плюс docs-файлы этой задачи — допустимо, это не код).
2. `grep -rn "new HttpClient" src --include="*.cs" | grep -v obj/ | grep -v bin/ | grep -v tests/` —
   единственное docker-создание в `DockerEngineFactory` (строка с
   `Timeout = callTimeout`); прочие (`WorkerApiCertReader` 3 с,
   `S3CaHttpClientFactory`) — вне зоны docker, не тронуты (критерий 1, Р4).
3. Сверка кода: публичный `Create(string, string?)` — `public virtual`,
   сигнатура прежняя; `CallTimeout` и перегрузка — internal (критерий 2).
4. Оба юнит-теста зелёные (Шаг 2), сборка Release чистая (Шаги 3–4),
   E2E-гейт зелёный (Шаг 4) — критерии 3–5.
5. Комментарии новых строк — по-русски, тесты — AAA (критерий 7; видно из
   кода Шагов 1–2).
   Расхождение любого пункта — вернуться к соответствующему шагу, исправить,
   повторить верификацию.

**Выход:** все критерии приёмки spec §7 подтверждены; задача готова к
мерж-гейту.

**Проверка:** чек-лист выше пройден полностью без расхождений.

**Связь со spec:** §7 целиком.

- [x] Шаг 5 выполнен.

---

## Итог отчёта исполнителя

После Шага 5 исполнитель возвращает: список выполненных шагов, результаты
проверок (сборка/юниты/E2E — зелёные, со ссылками на лог-файлы), хеш коммита,
результат приёмочной самопроверки (Шаг 5) и список файлов диффа.
