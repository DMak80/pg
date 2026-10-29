# Spec: пер-попыточный таймаут docker-транспорта — пробой TTL 15 с глобального portalloc-лока

**Дата:** 2026-10-21 · **Источник:** ревью проекта `.my/review_project.md`, находка 🟠 #1 (Major, Л1+Л-arch) · **Worktree:** `fix-portalloc-lock-ttl`

---

## 1. Цель

Устранить окно гонки довыделения портов, создаваемое молчанием docker-транспорта:
сегодня единая фабрика docker-движков создаёт `HttpClient` без `Timeout` (действует
дефолт 100 с), при этом глобальный portalloc-лок имеет TTL 15 с без keepalive и
рассчитан на инвариант «секция короткая (единицы секунд ≪ TTL)» (arch/15 §4).
Зависший docker-вызов (чтение занятости) внутри секции довыделения удерживает лок
дольше TTL → второй инстанс легитимно перехватывает лок по истёкшему lease →
параллельные секции читают занятость до записей друг друга и выбирают одни и те же
порты — воспроизводимый класс отказа «port is already allocated» (контейнеры в
`Created`, provisioning в retry-цикле). Побочный эффект того же молчания — ложный
снос воркера watchdog: тихий вызов > 30 с при живом цикле нарушает инвариант
поллинга arch/14 §6.

Механизм фикса: задать пер-попыточный таймаут `HttpClient` в
`DockerEngineFactory.Create` — симметрично существующим таймаутам транспортов
etcd/patroni (7.5 с, полокна проверки watchdog). Одно изменение в `Shared.Docker`
закрывает всех четырёх воркеров (PgWorker/KafkaWorker/S3Worker/ValkeyWorker) и их
health-пробы: docker-`HttpClient` в прод-коде создаётся только в этой фабрике.

### Сценарий отказа (закрывается фиксом)

Перегруженный/полумёртвый docker-демон на одном хосте при живом etcd во время
посева/довыделения → docker-чтение занятости молчит до 100 с → lease лока истекает
на 15-й секунде → второй инстанс берёт лок → обе секции выбирают пересекающиеся
порты (compare mod_revision в RMW защищает только собственный per-cluster ключ,
union-чтения занятости до записей друг друга не защищены) → коллизия портов,
контейнеры в `Created`, provisioning в retry-цикле. Дополнительно: молчание > 30 с
→ graceful-рестарт воркера посреди тика (шторм рестартов при устойчивой деградации).

---

## 2. Принципы

1. **arch/-first соблюдён без правки arch/**: канон уже содержит требование —
   arch/14 §6 (инвариант поллинга долгих фаз: «ни один одиночный блокирующий вызов
   внутри фазы reconcile (docker create/start контейнеров нод/wal-агентов/джобов,
   SQL/HTTP-пробы, etcd-операции) не имеет права молчать дольше порога сноса —
   медленный daemon делает одиночный вызов длиннее любого наперёд заданного
   числа») и arch/14 §6 (пульс конечного внешнего вызова: «клиенту задаётся
   КОНЕЧНЫЙ пер-попыточный таймаут короче окна проверки»). Фикс приводит код в
   соответствие с каноном; контракт (arch/14 §2.2/§2.2.1, §5 G, §6; arch/15 §4;
   arch/16 §2.1) не меняется — каталог `arch/` не трогаем.
2. **Симметрия таймаутов**: etcd/patroni — 7.5 с (`AddHttpClient("etcd"/"patroni",
   c => c.Timeout = TimeSpan.FromSeconds(7.5))` в Program.cs всех воркеров,
   полокна проверки watchdog); S3 — 10 с; SSH-connect — 10 с. Docker получает
   7.5 с — тот же паттерн «полокна проверки watchdog», тот же прецедент
   реализации (комментарий-канон у константы, как `SqlTimeoutSec = 7` у
   `NpgsqlWalSqlExecutor` со ссылкой на arch/14 §6).
3. **Тиковая модель самовосстанавливается**: таймаут-исключение (`HttpClient`
   бросает `TaskCanceledException`) → `Result.FromAsync` превращает его в
   `Result.Failed` → обычный transient-фейл тика → повтор следующим тиком
   (идемпотентность docker-вызовов движка уже канон: 304 already-started /
   404 already-removed / 409 already-created; pull докачивается слоями).
   Отдельная классификация docker-таймаутов не вводится — семантика та же, что у
   таймаутов etcd.
4. **Минимальный дифф**: одна константа + применение в `Create` + тестовое окно.
   Публичный API фабрики не меняется; процессы воркеров, `PortAllocLock` (TTL,
   отсутствие keepalive, release-семантика) не трогаются.
5. **Язык**: комментарии в коде — по-русски, идентификаторы — английские;
   `TreatWarningsAsErrors=true` — сборка без предупреждений.

---

## 3. Структура / компоненты

### 3.1. Прод-код: `src/Shared.Docker/Engine/DockerEngineFactory.cs`

**(а) Константа пер-попыточного таймаута** (поле класса фабрики):

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

**(б) Применение** в `Create` (стр. 97, единственное создание `HttpClient`):

```csharp
var httpClient = new HttpClient(CreateHandler(endpoint))
{
    BaseAddress = new Uri(baseAddress),
    Timeout = CallTimeout,
};
```

**(в) Тестовое окно** (public-контракт неизменен; internal-перегрузка для
функционального юнит-теста с уменьшенным таймаутом — существующий
`InternalsVisibleTo("Shared.Docker.UnitTests")` уже разрешает доступ):

```csharp
public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
    => Create(endpoint, hostAlias, CallTimeout);

internal IDockerEngine Create(string endpoint, string? hostAlias, TimeSpan callTimeout)
{
    // ... существующее тело Create (scheme/tunnel/baseAddress) ...
    var httpClient = new HttpClient(CreateHandler(endpoint))
    {
        BaseAddress = new Uri(baseAddress),
        Timeout = callTimeout,
    };
    return new DockerEngine(httpClient, hostAlias);
}
```

`HttpClient.Timeout` применяется движком `SendAsync`-ом к КАЖДОМУ вызову
(листинг/инспект/busy-порты/create/start/stop/remove/exec/logs/pull) — это и есть
пер-попыточный таймаут; стриминговых ответов движок не читает
(`ReadAsStringAsync`/`ReadAsByteArrayAsync` — полные тела).

### 3.2. Охват (почему достаточно одной точки)

Docker-`HttpClient` в прод-коде создаётся только в `DockerEngineFactory.Create`
(греп `new HttpClient` по `src` без obj/bin: единственное docker-создание —
стр. 97 фабрики). Через фабрику движки строят:

- `PgWorker.Docker/Drivers/ClusterDriver.cs`, `KafkaWorker.Docker/…`,
  `S3Worker.Docker/…`, `ValkeyWorker.Docker/…` — все docker-операции воркеров;
- `HealthChecks/ServiceProbes.cs` всех воркеров — health-пробы тоже получают
  короткий таймаут (желательно: зонд и не должен молчать).

Секции глобального portalloc-лока с docker-чтениями внутри (закрываются фиксом у
всех): PgWorker `ProvisioningProcess.PlanPortsAsync` (`GetBusyPortsAsync`,
`GetHostsAsync` между `TryAcquire`/`Release`), KafkaWorker
`PortAllocHealer.ReallocateAsync` (+ RMW-секция `ReconstructAsync`), S3Worker
`ProvisioningProcess`/`AddNodeProcess`, ValkeyWorker `ProvisioningProcess`.

### 3.3. Тесты: `src/tests/Shared.Docker.UnitTests/Engine/`

**(А) Структурный** — константа канона защищена от невнимательной правки:

```
DockerEngineFactory.CallTimeout.Should().Be(TimeSpan.FromSeconds(7.5));
```

**(Б) Функциональный, быстрый** (нет реального docker, нет долгих ожиданий):
локальный молчащий TCP-сервер (`TcpListener` на `127.0.0.1` с портом из
`listener.LocalEndpoint` — фактический, без литералов), принимающий соединения и
не отвечающий; `factory.Create("tcp://127.0.0.1:<port>", "h",
TimeSpan.FromMilliseconds(300))` (internal-перегрузка); `PingAsync` → ассерты:
`IsSuccess == false`, elapsed ≤ 3 с (механизм: таймаут транспорта пробрасывается
в вызовы, а не дефолтные 100 с). Teardown: остановить listener (try/finally).
Комментарий теста — AAA-нотация.

Имя тест-класса: `DockerEngineFactoryCallTimeoutTests` (новый файл в
`Engine/`); оба теста — `[Fact]`.

### 3.4. Что НЕ меняется

- `PortAllocLock`: TTL 15 с, отсутствие keepalive, release-семантика — канон
  arch/15 §4 / arch/14 §5 G сохраняется как есть.
- Процессы воркеров (ProvisioningProcess/PortAllocHealer/драйверы) — не трогаем.
- `arch/**`, `docs/**` — не трогаем (обоснование в «Принципах» и «Принятых
  решениях» Р3).
- Публичный API `DockerEngineFactory.Create(string, string?)` и все интерфейсы.

---

## 4. Фазы

1. **Код**: константа `CallTimeout` + применение в `Create` + internal-перегрузка
   (`src/Shared.Docker/Engine/DockerEngineFactory.cs`).
2. **Юнит-тесты**: новый `DockerEngineFactoryCallTimeoutTests` (тесты А и Б).
3. **Верификация локальная**: сборка `src/PgWorker.slnx` Debug+Release без
   предупреждений; прогон `Shared.Docker.UnitTests` (+ смежные юниты воркеров не
   требуются — их фейки движков не идут через фабрику).
4. **Мерж-гейт E2E** (правило AGENTS.md: задача трогает код воркеров —
   `Shared.Docker` в поставке всех воркеров): двумя ОТДЕЛЬНЫМИ командами —
   (1) статподготовка `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
   src/tests/PgWorker.E2ePrepare -c Release`; (2) сборка `DOTNET_CLI_UI_LANGUAGE=en
   dotnet build src/PgWorker.slnx -c Release --nologo`; (3) тесты с `--no-build`:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c
   Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`.
   Полный прогон E2eFixture не требуется: транспорт не меняет
   provisioning/portalloc/moves-процессы. После серии — зачистка контейнеров/сетей
   по канону E2E.

---

## 5. Ограничения и риски

- **Единый таймаут на все вызовы движка** (включая `PullImageAsync`,
  `GetContainerLogsAsync`, `StopContainerAsync?t=N`, exec-цепочки): выбрано
  осознанно (канон §6 «пер-попыточный таймаут» без пер-вызовных исключений —
  симметрия etcd/patroni/S3, у которых один таймаут на клиенте). Следствия при
  превышении: исключение → transient-фейл → повтор тиком; docker-операции
  идемпотентны (pull докачивается слоями локального registry; stop продолжается
  на демоне, повторный stop → 304; create → 409). Отдельные таймауты для
  pull/logs НЕ вводим.
- **Предельная арифметика секции лока**: два ПОСЛЕДОВАТЕЛЬНЫХ docker-чтения
  (`GetHostsAsync` → `GetBusyPortsAsync`) в худшем патологическом сочетании
  «первое медленное-но-успешное ≈ 7.5 с + второе зависшее 7.5 с» дают ≈ 15 с =
  TTL. Реалистические профили далеки от предела (первый фейл прерывает секцию
  `return Failed` → `finally Release`; успех при таймауте соседа требует
  патологического сочетания), а watchdog-инвариант закрыт с запасом (7.5 ≪ 30).
  Запас при необходимости даётся уменьшением константы — одной правки достаточно.
- **Не закрывается данным фиксом** (вне скоупа, найдено ревью отдельно):
  `NpgsqlMoveSqlExecutor` без таймаутов (ревью #2) — отдельная задача.

---

## 6. Принятые решения (закрытые развилки)

Вопрос способа фикса был задан пользователю через `AskUserQuestion` — ответ не
получен; решения ниже приняты по лучшему суждению в соответствии с рекомендацией
ревью #1, канонами `arch/` и правилами AGENTS.md. Каждое зафиксировано явно;
у исполнителя плана выбора не остаётся.

**Р1. Способ фикса: пер-попыточный таймаут `HttpClient` в фабрике** (против
keepalive лока / выноса docker-чтений из-под лока / комбинации). Обоснование:
рекомендация ревью («самое дешёвое, закрывает и коллизию, и ложный снос»);
единая точка `Shared.Docker` покрывает 4 воркеров и пробы; keepalive менял бы
контракт лока (канон «без keepalive» — осознанный, arch/15 §4) и не закрывал
ложный снос watchdog; вынос чтений — перестройка канонизированных секций всех
воркеров с сохранением проблемы молчания. TTL лока не меняем, keepalive не вводим.

**Р2. Значение таймаута: 7.5 с** (`TimeSpan.FromSeconds(7.5)`, константа
`CallTimeout`). Обоснование: симметрия с etcd/patroni («полокна проверки
watchdog», прецедент Program.cs:129–133 всех воркеров), рекомендация ревью.
Альтернатива 10 с (S3/SSH) отклонена: меньше запас до TTL 15 с лока, канон
etcd/patroni ближе к docker-вызовам reconcile-тика.

**Р3. `arch/` не трогаем.** Фикс приводит код в соответствие с уже существующим
каноном arch/14 §6 (инвариант поллинга прямо называет docker-вызовы и требует
пер-попыточный таймаут короче окна проверки); контракт arch/14 §2.2/§2.2.1
(транспорт), §5 G (клэйм), arch/15 §4 (порт-аллокатор) не меняется. Значение
живёт константой в коде с канон-комментарием — прецедент `NpgsqlWalSqlExecutor`
(`SqlTimeoutSec = 7` со ссылкой на arch/14 §6).

**Р4. Охват: только `DockerEngineFactory`.** Docker — единственный транспорт без
таймаута (etcd/patroni 7.5 с, S3 10 с, SSH-connect 10 с — проверено грепом);
docker-`HttpClient` создаётся только в фабрике. ServiceProbes получают таймаут
как желательное следствие. Иные `HttpClient` (cert-reader 3 с, S3CA-фабрика AWS
SDK) уже вне зоны docker и не трогаются.

**Р5. Поведение при таймауте: transient-фейл тика без новой классификации.**
`TaskCanceledException` → `Result.Failed` → бэкофф/повтор тиком — семантика
симметрична etcd-таймаутам; отдельный тип исключения и обработка не вводятся.

**Р6. Тесты: два юнита + E2E-минимум.** Юнит-структурный (константа 7.5 с) и
юнит-функциональный (молчащий TCP-сервер + internal-перегрузка 300 мс — быстрый,
без docker). Интеграционные docker-тесты не добавляем (механизм тот же,
testcontainers-медленность не нужна). E2E по AGENTS.md: свежий Release,
`Scale_AddEmptyShard` (маркер); полный E2eFixture — нет (не меняем
provisioning/portalloc/moves-процессы).

**Р7. Тестовое окно: internal-перегрузка `Create(endpoint, hostAlias,
callTimeout)`.** Публичный контракт неизменен; `InternalsVisibleTo` уже объявлен
для `Shared.Docker.UnitTests`. Рефлексию и «спящие 7.5 с» тесты не используем
(правило коротких тестов).

**Р8. Единый таймаут на ВСЕ вызовы движка** (pull/logs/stop/exec включительно),
без пер-вызовных исключений — см. «Ограничения и риски»; отдельные таймауты не
вводим, configurable-опцию (env) не добавляем (домашняя среда, канон-константа).

---

## 7. Критерии приёмки

1. `DockerEngineFactory.Create` создаёт `HttpClient` с
   `Timeout = DockerEngineFactory.CallTimeout` (7.5 с); единственное создание
   docker-`HttpClient` в прод-коде (греп `new HttpClient`).
2. Публичный API фабрики не изменился: `Create(string endpoint, string?
   hostAlias = null)` сигнатура и поведение сохранены; `CallTimeout` и
   internal-перегрузка — internal.
3. Оба юнит-теста зелёные: структурный (константа 7.5 с) и функциональный
   (молчащий сервер: `PingAsync` → `Failed`, elapsed ≤ 3 с при таймауте 300 мс;
   teardown listener в `finally`).
4. Сборка `src/PgWorker.slnx` Release без предупреждений
   (`TreatWarningsAsErrors=true`).
5. E2E-гейт зелёный: `PgWorker.E2ePrepare` отработал; сборка Release; `dotnet
   test … --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` с
   `PGW_TEST_DOCKER=1` — pass; после серии контейнеры/сети зачищены.
6. `PortAllocLock`, процессы воркеров, `arch/**`, `docs/**` — без изменений
   (`git diff` подтверждает: правки только в `DockerEngineFactory.cs` и новом
   тест-файле).
7. Комментарии новых строк кода — по-русски, тесты — AAA-нотация.
