# Спецификация t04-etcd-api — API инспекции etcd и каркас AlertEngine

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины: `arch/roadmap/etcd.md`
(пункт `t04-etcd-api`), `arch/03-panels.md` (ГЛАВНЫЙ документ задачи: список
эндпоинтов §1, DTO §2, каталог алертов §4, polling §3), `arch/02-etcd-contract.md`
§4 п.4–5 (порядок тика: сборка снапшота → AlertEngine → атомарная замена),
`arch/01-architecture.md` §1–2 (направление зависимостей, состав проектов,
CQRS queries), §6 (настройки). Фактическое состояние кода — t03
(`src/AdminPanel.Core`, `src/AdminPanel.Etcd`, `src/AdminPanel.Api`): модель
снапшота, `SnapshotStore`, `SnapshotRefresher` (успешный тик и `FailTick`
с `Alerts = []`), auth-модуль с default-deny guard, query-ветка CQRS
(`MeQuery` — образец).

## 1. Цель

Три read-only API-эндпоинта инспекции etcd из снапшота — `GET /api/overview`
(сводка с полями-заглушками шардирования), `GET /api/etcd/status`,
`GET /api/alerts` (query-параметры `?severity=`, `?kind=`) — и каркас
`AlertEngine` в `AdminPanel.Core`: чистая оценка «правила на снапшоте →
`Alert[]`» со стабильными id `kind:target` и `sinceUnix` сравнением с прошлым
снапшотом, с etcd-частью каталога алертов (03 §4): `etcd-unreachable`,
`etcd-no-quorum`, `etcd-endpoint-down`, `etcd-alarm`, `snapshot-stale`,
`cluster-incomplete`, `key-malformed` — 7 из 24 kind'ов; остальные (шардирование
— t05, HA/пробы — t06) добавляются своими правилами в каркас без его правки.
Эндпоинты — через `IQuery`-хендлеры и диспетчер `IHandler` (паттерн t02
`MeQuery`), маппинг снапшот→DTO — чистыми функциями. Все эндпоинты под
default-deny guard (уже есть, без правок auth). `SnapshotRefresher`
интегрируется с `AlertEngine`: алерты вычисляются на каждом тике, включая
отказный. Тесты: unit на правила/каркас/мапперы/хендлеры + integration-смоук
API (401 без cookie, 503 без снапшота, 200 с фикстурным снапшотом, путь
данных против Testcontainers-etcd).

Новых NuGet-пакетов нет: CQRS, attribute-DI, Result — Infrastructure; JSON и
Minimal API — shared framework; Testcontainers уже подключён (t03).

## 2. Принципы

- Источник истины — `arch/`; всё, что arch/ не оговаривает, решено минимальным
  способом и зафиксировано в §3. Расхождение с arch/ запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде — русские. Тексты `message`
  алертов — русские (человекочитаемые, видны в UI-таблице алертов; arch язык
  сообщений не фиксирует — панель русскоязычная по составу документации).
- Тесты — xunit v3 + FluentAssertions, комментарии по нотации AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- Паттерны t01–t03 обязательны: attribute-DI (`[InjectAs*]`), query-ветка CQRS
  (`IQuery<T>`/`IQueryHandler`/`IHandler.HandleQuery`), `Result`-монада,
  модульная композиция, unit-тесты без хоста (грабля статического кеша сборок —
  t03 §3.15), integration — один Program-хост на процесс (t02 §14).
- API не ходит в etcd на запрос: только чтение `ISnapshotStore` (arch/01 §1).
- Мутации `arch/01–04` запрещены; из `arch/roadmap/` меняется только
  `etcd.md` — удаление пункта по деливераблу §14.

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **`GET /api/alerts` входит в t04** — roadmap t04 перечисляет его явно
   («Эндпоинты: … `GET /api/alerts` (с query-параметрами)»). Вопрос задания
   координатора «возможно, он в t05/t06» — проверено: нет. Эндпоинт отдаёт
   **все** алерты текущего снапшота; t05/t06 расширяют только набор правил
   `AlertEngine`, эндпоинт и DTO не меняются.
2. **Каркас правил: один класс на kind.** «Каркас» из roadmap =
   `IAlertRule { string Kind; IEnumerable<Alert> Evaluate(EtcdSnapshot,
   AlertContext) }` + `AlertEngine(IEnumerable<IAlertRule>)`, прогоняющий
   правила и владеющий общей механикой (стабильные id, `sinceUnix`,
   сортировка). Правила регистрируются attribute-DI в Core
   (`[InjectAsSingleton(typeof(IAlertRule))]`) — t05/t06 добавляют классы
   правил без правок двигателя. Альтернатива «один статический класс со
   switch по kind» отвергнута: при 24 kind'а это класс-переросток и правки
   чужих задач в одном файле.
3. **`AlertEngine` — чистая функция без знания настроек.** Core не ссылается
   на `EtcdOptions` (живёт в `AdminPanel.Etcd`; направление зависимостей
   arch/01 §1 — только `Etcd → Core`). Параметры оценки передаются явно:
   `Evaluate(EtcdSnapshot snapshot, EtcdSnapshot? previous, DateTimeOffset
   nowUtc, double refreshIntervalSeconds)`; `refreshIntervalSeconds` нужен
   правилу `snapshot-stale` (порог 3×`RefreshInterval`, 03 §4), вызывающий
   (refresher) берёт его из `EtcdOptions` с тем же fallback `<= 0 → 3`, что и
   тик (t03 §3.3).
4. **Семантика `sinceUnix`** (03 §2: «сравнивает с прошлым снапшотом по
   стабильному `id` — присутствует с»): алерт с id, который был в
   `previous.Alerts` → `SinceUnix` переносится из предыдущего; id новый →
   `SinceUnix = nowUtc.ToUnixTimeSeconds()` (момент первого наблюдения);
   `previous == null` (первый тик после старта панели) → `SinceUnix = null` —
   алерт мог присутствовать до старта панели, время появления неизвестно
   (обоснование nullable-поля в 03 §2). История отдельно не хранится (03 §2).
5. **`AlertEngine` вызывается и на отказном тике.** В t03 `FailTick` строит
   снапшот с `Alerts = []` — для t03 было безразлично, но `etcd-unreachable`
   (порог `ConsecutiveFailures ≥ 2`) вычисляется именно на отказных тиках:
   при переносе прежних алертов он бы не вспыхнул никогда. Правка
   `SnapshotRefresher`: оба пути тика (успешная сборка и `FailTick`) после
   построения снапшота вычисляют `alertEngine.Evaluate(новый, previous,
   now, intervalSeconds)` и заменяют снапшот с заполненными `Alerts`
   (`snapshot with { Alerts = … }`). На отказном тике данные прежние, но
   Etcd-часть свежая (`ConsecutiveFailures` растёт — t03 §3.9), поэтому
   `etcd-unreachable` появляется ровно со второго отказа, `snapshot-stale` —
   по мере старения `BuiltAtUtc`; data-алерты (`cluster-incomplete`,
   `key-malformed`) пересчитываются по прежним данным (id стабильны,
   `sinceUnix` переносится — «присутствует с» не рвётся).
6. **Пороги t04 — константы правил, секция `AdminPanel:Alerts` не заводится.**
   Задействованные пороги заданы в 03 §4 буквально: «≥ 2 тиков»
   (`UnreachableThreshold = 2`) и «старше 3×RefreshInterval»
   (`StaleIntervalsMultiplier = 3`). Ключи секции `AdminPanel:Alerts`
   (arch/01 §6: `StaleMoveSeconds`, `FrozenSeconds`, `ReplicaLagBytes`)
   нужны только алертам t05/t06 — POCO `AlertsOptions` заводит t05 со своим
   первым порогом (YAGNI: пустой конфиг-класс сейчас не нужен никому).
7. **Таргеты и стабильные id** (03 §2 `id = kind:target`; один kind может
   давать несколько алертов — «на каждый endpoint/alarm/ключ»):

   | kind | target | id (пример) |
   |---|---|---|
   | `etcd-unreachable` | `"etcd"` | `etcd-unreachable:etcd` |
   | `etcd-no-quorum` | `"etcd"` | `etcd-no-quorum:etcd` |
   | `etcd-endpoint-down` | URL endpoint'а | `etcd-endpoint-down:http://etcd2:2379` |
   | `etcd-alarm` | `"{memberId}:{type}"` | `etcd-alarm:13829092748572458721:nospace` |
   | `snapshot-stale` | `"snapshot"` | `snapshot-stale:snapshot` |
   | `cluster-incomplete` | имя кластера | `cluster-incomplete:demo` |
   | `key-malformed` | ключ | `key-malformed:/clusters/demo/config` |

   `memberId` — decimal-строка (как парсит gateway t03, `AllowReadingFromString`),
   `type` — строчное имя: `nospace` / `corrupt` / `unknown` (неизвестный код
   alarm — толерантность к будущим типам etcd, 02 §2.4 «NOSPACE-потомки»).
8. **Условия правил — по фактической модели t03** (03 §4 «источник» ↔ поля):
   `etcd-unreachable`: `Etcd.ConsecutiveFailures >= 2`; `etcd-no-quorum`:
   `Etcd.QuorumSuspected` (эвристика t03 §3.11 — raft-признаки уже выведены
   в поле; отдельного разбора `status.errors` не требуется, ошибки живых
   endpoints идут в `details.errors`); `etcd-endpoint-down`: `!Reachable` на
   каждый endpoint из `Etcd.Endpoints`; `etcd-alarm`: каждый элемент
   `Etcd.Alarms`; `snapshot-stale`: `nowUtc - BuiltAtUtc > 3×interval`;
   `cluster-incomplete`: `ClusterInfo.Incomplete` (computed-свойство t03) на
   каждый кластер; `key-malformed`: каждая запись `ParseErrors` (t03 §3.4).
9. **`details` — плоский словарь `string → string`** (тип уже в Core),
   camelCase-ключи, строковые значения: `consecutiveFailures`, `errors`
   (join `"; "`), `memberId`, `alarmType`, `ageSeconds`, `thresholdSeconds`,
`builtAtUnix`, `dbname` (`"missing"`/значение), `reason`. Полный состав —
§4.3; UI показывает их в деталях (03 §7 Alerts-панель).
10. **Сортировка алертов** не оговорена arch: детерминированный порядок —
    severity по убыванию (Critical → Warning → Info), затем `Kind`, затем
    `Target` (Ordinal). Фиксируется в `AlertEngine` — стабильно для UI-ленты
    и тестов; фильтрация — на стороне эндпоинта (§3.14).
11. **DTO-представление чисел и enum'ов** (JSON camelCase, arch/03 преамбула):
    `memberId`/`leaderMemberId` — **decimal-строки** (значения uint64 member-id
    превышают 2^53 — JS-число теряет точность; etcd gateway сам отдаёт int64
    строками — t03 §3.17); `raftTerm` — number (реальные raft-термы малы);
    `severity` — строчные строки `"critical"|"warning"|"info"` (формат
    query-параметра 03 §1); `alarmType` — `"nospace"|"corrupt"`; `state` в
    заглушке переездов — строка `"SYNCING"|"FROZEN"|"ABORTING"` (канон
    статус-ключей 02 §2.1).
12. **Отсутствие снапшота → 503 ProblemDetails.** До первого тика
    `ISnapshotStore.Current == null` (t03 §3.13 — «потребители показывают
    загрузку»); частичный DTO без данных невозможен (нет даже `BuiltAtUtc`).
    Хендлеры трёх query возвращают `Result<T>.Failed(new
    SnapshotNotReadyException())`, эндпоинт маппит отказ в
    `Results.Problem(statusCode: 503, title: "Snapshot not ready")` — фронт
    повторит по polling (2/5/15 с). Имена прочих отказов — 500 (как
    `MeQuery`-паттерн t02).
13. **Валидация query-параметров `/api/alerts`**: `?severity=` принимает
    строго `critical|warning|info` (или отсутствует); иное значение → 400
    ProblemDetails (опечатка фронта ловится сразу, а не пустым списком).
    `?kind=` — свободная строка точного сравнения; неизвестный kind → 200 `[]`
    (kind'ы эволюционируют между задачами — 400 был бы ложным сбоем).
    Валидация — в эндпоинте до `HandleQuery` (query получает уже
    `AlertSeverity?` + `string? kind`).
14. **`EtcdStatusDto.isLeader` и `.active`** (03 §2): `active` =
    `Etcd.ActiveEndpoint == endpoint.Url` (метка активного, 02 §4 п.1);
    `isLeader` = `member.Id == leaderId`, где `leaderId` — `LeaderMemberId`
    первого живого endpoint с валидным `leader > 0`, иначе первого любого с
    не-null (02 §2.4 «IsLeader по совпадению id со статусом leader»); нет
    валидного leaderId ни у одного — все `isLeader = false`.
15. **`OverviewDto` — контракт полный, кластерная часть — заглушки-пустышки.**
    По roadmap t04 «сводка без шардирования — поля-заглушки»: под-DTO
    `clusters[]` и `activeMoves[]` заводятся сейчас с полным составом полей
    по 03 §2 (фронтенд t07 типизирует сразу), но маппер t04 возвращает пустые
    списки — наполнение в t05 (данные в снапшоте уже есть). Реальны в t04:
    `alertsCritical`/`alertsWarning` (по `Alerts` — только etcd-виды, расширится
    в t05/t06 автоматически), `etcd{reachable, endpointsOk, endpointsTotal}`,
    `snapshotAgeMs` (мс от `BuiltAtUtc` до `nowUtc`, `>= 0`), `stale` — та же
    формула, что у алерта `snapshot-stale`: `age > 3×RefreshIntervalSeconds`
    (единственная формула свежести — бейдж Overview и алерт не разойдутся).
16. **Integration-фабрика `"api"`: hosted-сервисы отключаются, store
    подменяется.** Refresher в фабрике (Endpoints пуст) каждые 3 c строил бы
    отказный снапшот и перезатирал тестовые — гонки. Решение:
    `ConfigureTestServices` → `services.RemoveAll<IHostedService>()` (hosted
    в решении один — `SnapshotRefresher`; singleton-регистрация самого типа
    остаётся, `EtcdHealthCheck` резолвится как раньше) + `Replace(ISnapshotStore
    → TestSnapshotStore)` — тестовая реализация с управляемым `Current`
    (settable). Тесты коллекции `"api"` сериализованы xunit'ом — гонок между
    ними нет; `AuthTests`/`HealthzTests` от отключения hosted не зависят
    (guard/cookie/self-чек — без refresher'а).
17. **Смоук «против Testcontainers-etcd» без второго Program-хоста**
    (roadmap t04 «integration-смоук API против Testcontainers-etcd»):
    поднять контейнер (`EtcdContainerFixture`, t03) → `EtcdTestHarness.
    NewRefresher` (реальный gateway/refresher/AlertEngine) делает тик против
    живого etcd → полученный снапшот кладётся в `TestSnapshotStore` фабрики →
    `GET /api/*` через WAF-клиент отдаёт реальные данные. Так проверяется
    полный путь данных etcd→gateway→парсеры→AlertEngine→store→query→DTO→HTTP;
    единственная склейка — перенос ссылки снапшота между store (безTransport'а
    внутрь хоста — ограничение одного Program-хоста на процесс, t02 §14).
18. **`SnapshotRefresherTests`/`EtcdTestHarness` обновляются под новый
    конструктор** refresher'а (`+ IAlertEngine`): харнессы передают
    `new AlertEngine(SevenEtcdRules())` (тестовый helper со списком правил
    t04); ассерты существующих тестов не меняются (на чистых фикстурах
    `Alerts` пуст), добавляются новые кейсы на заполнение `Alerts` (§10).
19. **Файлы запросов: query + DTO + handler + статический mapper в одном
    файле** (паттерн `MeQuery.cs`), mapper — отдельный статический класс в том
    же файле: unit-тесты мапперов напрямую, handler тонкий (store → mapper).

## 4. Каркас AlertEngine (AdminPanel.Core/Alerting/)

### 4.1. Контракты

```csharp
// IAlertRule.cs
namespace AdminPanel.Core.Alerting;

// Правило каталога алертов (03 §4): один kind, чистая оценка снапшота.
// Каркас: t05/t06 добавляют свои правила — новыми классами, без правки AlertEngine.
public interface IAlertRule
{
    // Kind каталога, напр. "etcd-unreachable" (03 §4).
    string Kind { get; }

    // Алерты правила по текущему снапшоту (0..N; SinceUnix проставляет AlertEngine).
    IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context);
}

// Параметры оценки, не входящие в снапшот: прошлый снапшот (sinceUnix),
// текущее время и период тика (порог snapshot-stale). Core не знает настроек (§3.3).
public sealed record AlertContext(
    EtcdSnapshot? Previous,
    DateTimeOffset NowUtc,
    double RefreshIntervalSeconds);
```

```csharp
// IAlertEngine.cs
namespace AdminPanel.Core.Alerting;

// Чистая функция Snapshot → Alert[] (arch/01 §2): правила + общая механика.
public interface IAlertEngine
{
    IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot,
        EtcdSnapshot? previous,
        DateTimeOffset nowUtc,
        double refreshIntervalSeconds);
}
```

```csharp
// AlertEngine.cs
namespace AdminPanel.Core.Alerting;

// Каркас: прогон правил → стабильные id → sinceUnix из прошлого снапшота → сортировка.
[InjectAsSingleton(typeof(IAlertEngine))]
public sealed class AlertEngine(IEnumerable<IAlertRule> rules) : IAlertEngine
{
    public IReadOnlyList<Alert> Evaluate(...)
    {
        // 1) context = new AlertContext(previous, nowUtc, refreshIntervalSeconds);
        // 2) все правила → алерты (Id = $"{Kind}:{Target}" проставляет правило;
        //    SinceUnix здесь: previous==null → null; был в previous → прежний;
        //    новый → nowUtc.ToUnixTimeSeconds()) (§3.4);
        // 3) сортировка: Severity по убыванию, Kind, Target (Ordinal) (§3.10);
        // 4) вернуть IReadOnlyList<Alert>.
    }
}
```

Регистрация правил: `[InjectAsSingleton(typeof(IAlertRule))]` на каждом
классе правила — `AddCore()` (AutoRegistration Core-сборки, уже вызывается в
`Program.cs`) регистрирует и сами типы, и `IAlertRule`; DI отдаёт
`IEnumerable<IAlertRule>` в `AlertEngine` автоматически. Дубликаты `Kind`
между правилами невозможны по построению (класс на kind), контроль — unit-тест
на уникальность kind'ов в списке правил t04.

### 4.2. Правила t04 (AdminPanel.Core/Alerting/Rules/)

| Файл / класс | Kind | Severity | Условие (по модели t03) |
|---|---|---|---|
| `EtcdUnreachableRule` | `etcd-unreachable` | Critical | `Etcd.ConsecutiveFailures >= 2` (константа `Threshold = 2`, 03 §4) |
| `EtcdNoQuorumRule` | `etcd-no-quorum` | Critical | `Etcd.QuorumSuspected` (raft-эвристика t03 §3.11) |
| `EtcdEndpointDownRule` | `etcd-endpoint-down` | Warning | `!Reachable` — по одному алерту на endpoint |
| `EtcdAlarmRule` | `etcd-alarm` | Critical | по одному алерту на каждый `Etcd.Alarms` |
| `SnapshotStaleRule` | `snapshot-stale` | Warning | `NowUtc - BuiltAtUtc > 3 × RefreshIntervalSeconds` (константа `Multiplier = 3`) |
| `ClusterIncompleteRule` | `cluster-incomplete` | Warning | `ClusterInfo.Incomplete` — по одному на кластер |
| `KeyMalformedRule` | `key-malformed` | Warning | по одному алерту на `ParseErrors` |

Все правила — stateless-классы, регистрация `[InjectAsSingleton(typeof(IAlertRule))]`,
условие ложно → пустой `Enumerable.Empty<Alert>()`.

### 4.3. Сообщения и details (фиксируются; ключи camelCase)

| Kind | Message (рус.) | Details |
|---|---|---|
| `etcd-unreachable` | `etcd недоступен: {n} подряд неудачных тика` | `consecutiveFailures` |
| `etcd-no-quorum` | `подозрение на отсутствие кворума etcd (raft без лидера)` | `errors` — join `"; "` всех `Endpoints[].Errors` |
| `etcd-endpoint-down` | `endpoint etcd недоступен: {url}` | `errors` — join ошибок endpoint'а |
| `etcd-alarm` | `тревога etcd {NOSPACE\|CORRUPT} на member {memberId}` | `memberId`, `alarmType` (`nospace`/`corrupt`/`unknown`) |
| `snapshot-stale` | `снапшот устарел: возраст {age} c при пороге {t} c` | `ageSeconds`, `thresholdSeconds`, `builtAtUnix` |
| `cluster-incomplete` | `кластер {name} без config-ключа (incomplete)` | `dbname` (`"missing"` или значение) |
| `key-malformed` | `ключ не разобран: {key}` | `reason` |

`EtcdAlarmType` → строка: `NoSpace → "nospace"`, `Corrupt → "corrupt"`,
иное → `"unknown"`; имя для message — верхний регистр строки
(`"NOSPACE"`). Форматирование чисел — инвариантная культура.

## 5. Интеграция в SnapshotRefresher

1. Конструктор: `+ IAlertEngine alertEngine` (DI; singleton — двигается
   вместе с refresher'ом).
2. Интервал для оценки: то же значение, что у тика — `RefreshIntervalSeconds`
   c fallback `<= 0 → 3` (вычисляется один раз, используется и таймером, и
   оценкой — refactor локальной переменной `ExecuteAsync` в приватное поле/
   метод).
3. Успешный тик: `previous = store.Current` (уже берётся) → сборка через
   `SnapshotBuilder.Build` (без изменений) → `snapshot with { Alerts =
   alertEngine.Evaluate(snapshot, previous, now, interval) }` → `store.Replace`
   (порядок arch/02 §4 п.4–5: сборка → AlertEngine → атомарная замена).
4. `FailTick`: тот же прогон — построенный отказный снапшот (данные прежние,
   Etcd-часть свежая, t03 §3.9) получает `Alerts` от `alertEngine.Evaluate`
   перед `Replace` (§3.5). `Probes` в FailTick остаются `[]` (тема t06).
5. `SnapshotBuilder` не меняется: алерты — пост-фактум оценки, не часть
   склейки тика (его unit-тесты не затрагиваются).

## 6. API-эндпоинты (AdminPanel.Api/Inspection/)

### 6.1. `InspectionModule.cs`

```csharp
// Композиция эндпоинтов инспекции etcd (arch/03 §1; guard уже закрыл /api/*).
public static class InspectionModule
{
    // GET /api/overview, /api/etcd/status, /api/alerts (03 §1).
    public static IEndpointRouteBuilder MapInspectionApi(this IEndpointRouteBuilder endpoints);

    // Снапшот ещё не собран (t03 §3.13) → 503 ProblemDetails (§3.12).
    public sealed class SnapshotNotReadyException : Exception;
}
```

Общий вид эндпоинта (паттерн `me` из t02):

```csharp
endpoints.MapGet("/api/overview", async (IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleQuery<OverviewQuery, OverviewDto>(new OverviewQuery(), ct);
    return result.IsSuccess
        ? Results.Ok(result.Value)
        : Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Snapshot not ready", detail: result.Error!.Message);
});
```

`/api/alerts`: `[FromQuery] string? severity, string? kind`; severity
валидируется по набору `{"critical","warning","info"}` (§3.13), маппится в
`AlertSeverity?`; невалидный → 400 ProblemDetails
(`Results.Problem(statusCode: 400, title: "Invalid severity", detail: …)`).

`Program.cs`: `+ app.MapInspectionApi();` после `app.MapAuthApi();` — единственная
правка точки входа (регистрация хендлеров — уже работающий `AddApi()` +
AutoRegistration).

### 6.2. DTO (03 §2; camelCase JSON, §3.11)

```csharp
// OverviewQuery.cs
public sealed record OverviewQuery : IQuery<OverviewDto>;

public sealed record OverviewDto(
    int AlertsCritical,
    int AlertsWarning,
    OverviewEtcdDto Etcd,
    IReadOnlyList<OverviewClusterDto> Clusters,     // t04: [] (t05 наполнит)
    IReadOnlyList<OverviewMoveDto> ActiveMoves,     // t04: [] (t05 наполнит)
    long SnapshotAgeMs,
    bool Stale);

public sealed record OverviewEtcdDto(bool Reachable, int EndpointsOk, int EndpointsTotal);

// Заглушки контракта t05 (03 §2): поля полные, значения — в t04 всегда пусто.
public sealed record OverviewClusterDto(
    string Name, int Shards, int Buckets, int ActiveMoves, int MasterlessShards);
public sealed record OverviewMoveDto(
    string Cluster, int Bucket, string State, string? Owner, string? Target, long? UpdatedUnix);

// Маппер: снапшот → сводку; чистая функция (unit напрямую).
public static class OverviewMapper
{
    public static OverviewDto Map(EtcdSnapshot snapshot, DateTimeOffset nowUtc, double refreshIntervalSeconds);
}

// Хендлер: store.Current → 503-отказ или mapper.
[InjectAsScoped]
public sealed class OverviewQueryHandler(ISnapshotStore store, TimeProvider time,
    IOptions<EtcdOptions> etcdOptions) : IQueryHandler<OverviewQuery, OverviewDto>;
```

Формулы маппера: `AlertsCritical = snapshot.Alerts.Count(a => a.Severity ==
AlertSeverity.Critical)` (аналогично Warning); `EndpointsOk = Etcd.Endpoints.
Count(e => e.Reachable)`, `EndpointsTotal = Etcd.Endpoints.Count`;
`SnapshotAgeMs = max(0, (long)Math.Round((nowUtc − BuiltAtUtc).TotalMilliseconds))`;
`Stale = (nowUtc − BuiltAtUtc) > TimeSpan.FromSeconds(3 ×
refreshIntervalSeconds)`; `Clusters = []`, `ActiveMoves = []`.

```csharp
// EtcdStatusQuery.cs
public sealed record EtcdStatusQuery : IQuery<EtcdStatusDto>;

public sealed record EtcdStatusDto(
    IReadOnlyList<EtcdEndpointDto> Endpoints,
    IReadOnlyList<EtcdMemberDto> Members,
    IReadOnlyList<EtcdAlarmDto> Alarms,
    bool QuorumSuspected,
    DateTimeOffset LastRefreshUtc);

public sealed record EtcdEndpointDto(
    string Url, bool Reachable, double? LatencyMs, string? Version,
    long? DbSizeBytes, string? LeaderMemberId, ulong? RaftTerm,
    IReadOnlyList<string> Errors, bool Active);

// id/leaderMemberId — decimal-строки (§3.11): uint64 сверх 2^53.
public sealed record EtcdMemberDto(
    string Id, string? Name, IReadOnlyList<string> PeerUrls,
    IReadOnlyList<string> ClientUrls, bool IsLeader);

public sealed record EtcdAlarmDto(string MemberId, string Type);

public static class EtcdStatusMapper
{
    // leaderId — первый живой endpoint с LeaderMemberId > 0, иначе первый
    // любой не-null; нет — все IsLeader=false (§3.14).
    public static EtcdStatusDto Map(EtcdStatus etcd);
}

[InjectAsScoped]
public sealed class EtcdStatusQueryHandler(ISnapshotStore store)
    : IQueryHandler<EtcdStatusQuery, EtcdStatusDto>;
```

```csharp
// AlertsQuery.cs
public sealed record AlertsQuery(AlertSeverity? Severity, string? Kind)
    : IQuery<IReadOnlyList<AlertDto>>;

// severity — строчная строка, details как есть, sinceUnix nullable (§3.4).
public sealed record AlertDto(
    string Id, string Severity, string Kind, string Target, string Message,
    IReadOnlyDictionary<string, string>? Details, long? SinceUnix);

public static class AlertsMapper
{
    // Map — Core→DTO (severity: Critical→"critical", Warning→"warning", Info→"info");
    // ApplyFilters — фильтры severity/kind до маппинга.
    public static IReadOnlyList<AlertDto> Map(IReadOnlyList<Alert> alerts);
    public static IReadOnlyList<Alert> ApplyFilters(
        IReadOnlyList<Alert> alerts, AlertSeverity? severity, string? kind);
}

[InjectAsScoped]
public sealed class AlertsQueryHandler(ISnapshotStore store)
    : IQueryHandler<AlertsQuery, IReadOnlyList<AlertDto>>;
```

### 6.3. Сводка контракта HTTP (сверка с arch/03 §1)

| Метод+путь | Auth | Успех | Отказ |
|---|---|---|---|
| `GET /api/overview` | cookie | 200 `OverviewDto` | 401 без cookie (guard); 503 ProblemDetails до первого тика |
| `GET /api/etcd/status` | cookie | 200 `EtcdStatusDto` | аналогично |
| `GET /api/alerts` | cookie | 200 `AlertDto[]` (все алерты; `?severity=critical\|warning\|info`, `?kind=` — фильтры) | 401; 503; 400 при невалидном `severity` |

Проблемные ответы — `application/problem+json` (паттерн t02). Опечатки path
под `/api/*` без cookie → 401 (default-deny), с cookie → 404 (маршрутизатор) —
существующее поведение guard'а, не меняется.

## 7. Состав изменений (дерево файлов)

```
src/AdminPanel.Core/
├── Alerting/
│   ├── IAlertEngine.cs                 [новый] IAlertEngine, IAlertRule, AlertContext
│   ├── AlertEngine.cs                  [новый] каркас: правила → id/sinceUnix → сортировка
│   └── Rules/
│       ├── EtcdUnreachableRule.cs      [новый] critical, ConsecutiveFailures >= 2
│       ├── EtcdNoQuorumRule.cs         [новый] critical, QuorumSuspected
│       ├── EtcdEndpointDownRule.cs     [новый] warning, по одному на упавший endpoint
│       ├── EtcdAlarmRule.cs            [новый] critical, по одному на alarm
│       ├── SnapshotStaleRule.cs        [новый] warning, BuiltAtUtc > 3×interval
│       ├── ClusterIncompleteRule.cs    [новый] warning, по одному на incomplete-кластер
│       └── KeyMalformedRule.cs         [новый] warning, по одному на ParseError
└── (остальное — без изменений)
src/AdminPanel.Etcd/
└── SnapshotRefresher.cs                [правка] + IAlertEngine: Alerts на обоих путях
                                               тика; interval-поле для оценки (§5)
src/AdminPanel.Api/
├── Program.cs                          [правка] + app.MapInspectionApi()
└── Inspection/
    ├── InspectionModule.cs             [новый] MapInspectionApi, severity-валидация,
    │                                           SnapshotNotReadyException, 503-маппинг
    ├── OverviewQuery.cs                [новый] query+dto+handler+OverviewMapper
    ├── EtcdStatusQuery.cs              [новый] query+dto+handler+EtcdStatusMapper
    └── AlertsQuery.cs                  [новый] query+dto+handler+AlertsMapper
src/tests/AdminPanel.UnitTests/
├── AlertEngineTests.cs                 [новый] каркас + все 7 правил + sinceUnix + сортировка
├── InspectionMappersTests.cs           [новый] Overview/EtcdStatus/Alerts-мапперы
├── InspectionQueryHandlerTests.cs      [новый] 503-отказ без снапшота, фильтры alerts
├── SnapshotRefresherTests.cs           [правка] харнесс: + AlertEngine-аргумент;
│                                               + кейсы Alerts на обоих путях тика
├── EtcdHealthCheckTests.cs             [без правок] конструирует refresher через
│                                               RefresherTestHarness.New — аргумент
│                                               добавляется внутри харнесса
└── TestSnapshots.cs                    [новый] helper сборки EtcdSnapshot-фикстур для тестов
src/tests/AdminPanel.IntegrationTests/
├── AuthTests.cs                        [правка] фабрика: RemoveAll<IHostedService> +
│                                               Replace(ISnapshotStore → TestSnapshotStore)
├── EtcdSnapshotIntegrationTests.cs     [правка] EtcdTestHarness.NewRefresher: + AlertEngine;
│                                               + ассерты Alerts (чистый сид → [];
│                                               отказ ×2 → etcd-unreachable)
└── InspectionApiTests.cs               [новый] 401/503/200/400/фильтры/живой etcd (§9)
arch/roadmap/etcd.md                    [правка] удалить пункт t04-etcd-api (§14)
```

`appsettings*.json`, `Directory.Packages.props`, `Directory.Build.props`,
`.slnx`, `AdminPanel.Infrastructure`, `AdminPanel.Probes` — без изменений.

## 8. Настройки

Новых настроек нет: `AdminPanel:Alerts` не заводится (§3.6, появится в t05);
`EtcdOptions.RefreshIntervalSeconds` используется повторно (порог
`snapshot-stale` и формула `Overview.stale`); auth-настройки — без изменений.

## 9. Integration-тесты (src/tests/AdminPanel.IntegrationTests/)

Коллекция `"api"` (существующая `AuthWebFactory`, правится по §3.16):
`RemoveAll<IHostedService>()`, `TestSnapshotStore` (внутри
`InspectionApiTests.cs`; `Current` settable, `Replace` пишет туда же),
аксессор `factory.Snapshot { get; set; }`.

### 9.1. `InspectionApiTests` [Collection("api")] — HTTP-контракт

- `Endpoints_WithoutCookie_Return401` — `/api/overview`, `/api/etcd/status`,
  `/api/alerts` без cookie → 401 (default-deny guard; смоук guard'а для
  новых путей).
- `Endpoints_NoSnapshot_Return503ProblemDetails` — `factory.Snapshot = null` →
  все три → 503 `application/problem+json`, `title: "Snapshot not ready"`.
- `Overview_WithSnapshot_ReturnsDto` — фикстурный снапшот (1 живой + 1 мёртвый
  endpoint, 1 critical + 2 warning алерта): `alertsCritical=1`,
  `alertsWarning=2`, `etcd.endpointsOk=1`, `endpointsTotal=2`,
  `snapshotAgeMs >= 0`, `stale=false` (свежий BuiltAtUtc), `clusters=[]`,
  `activeMoves=[]`.
- `Overview_StaleSnapshot_StaleTrue` — BuiltAtUtc на 4×interval в прошлом →
  `stale=true`, возраст в `snapshotAgeMs`.
- `EtcdStatus_WithSnapshot_ReturnsEndpointsMembersAlarms` — поля endpoints
  (включая `active` только у активного URL), members c `isLeader=true` у
  совпадающего id (decimal-строки), alarms, `quorumSuspected`, `lastRefreshUtc`.
- `Alerts_WithSnapshot_ReturnAllSorted` — все алерты, сортировка severity
  desc, severity — строчные строки, `sinceUnix` на месте.
- `Alerts_SeverityFilter_ReturnsOnlyMatching` — `?severity=critical` → только
  critical; `?severity=warning` → warning.
- `Alerts_KindFilter_ReturnsOnlyMatching` — `?kind=etcd-endpoint-down`.
- `Alerts_UnknownKind_ReturnsEmpty200` — `?kind=nope` → 200 `[]`.
- `Alerts_InvalidSeverity_Returns400ProblemDetails` — `?severity=bogus` →
  400 `application/problem+json`.
- `Alerts_BothFilters_Combine` — `?severity=warning&kind=key-malformed`.

Фикстурный снапшот — из `TestSnapshots`-подобного helper'а (копия подхода в
integration-сборке — сборки тестов друг на друга не ссылаются, прецедент
`FixedTimeProvider`).

### 9.2. `InspectionEtcdApiTests` [Collection("api"), IClassFixture<EtcdContainerFixture>]

Путь данных «живой etcd → API» (§3.17): harness-харнесс t03 + AlertEngine
(правка `EtcdTestHarness.NewRefresher`: `+ IAlertEngine`):

- `LiveEtcd_OverviewEtcdStatusAlerts_ReflectRealSnapshot` — тик против
  контейнера с сидом demo → снапшот в `factory.Snapshot` → `/api/etcd/status`
  200 (version `3.5.21`, member `test`, `endpointsOk=1/1`), `/api/overview`
  (`etcd.reachable=true`), `/api/alerts` → `[]` (чистый сид).
- `LiveEtcd_SeededAnomalies_ProduceAlerts` — сид аномалий `kv/put`-ом: битое
  значение статус-ключа (`/clusters/demo/buckets/status/bucket_1` = `"not
  json"` → `key-malformed`) и ключи кластера без config
  (`/clusters/ghost/shards/g1/dsn` → `cluster-incomplete:ghost`) → повторный
  тик → `/api/alerts` содержит оба kind'а, `sinceUnix=null` (первое
  наблюдение — §3.4).

### 9.3. Правки существующих

- `AuthTests.cs` — фабрика (§3.16); сами auth-тесты не меняются.
- `EtcdSnapshotIntegrationTests.cs` — харнесс-аргумент; в
  `Refresher_RefreshOnce_BuildsExpectedSnapshot` ассерт `Alerts` остаётся
  `BeEmpty` (чистый сид); в `EtcdFailureTests` добавляется ассерт: после двух
  отказных тиков `store.Current.Alerts` содержит `etcd-unreachable:etcd`
  (critical).

## 10. Unit-тесты (src/tests/AdminPanel.UnitTests/)

Хелпер `TestSnapshots` (новый): сборка `EtcdSnapshot`-фикстур (healthy-базис:
reachable + 2 endpoints живы + свежий `BuiltAtUtc` + полный кластер; модификации
через `with`), переиспользуется тестами правил/мапперов/хендлеров.

### 10.1. `AlertEngineTests`

Каркас:

- `Evaluate_HealthySnapshot_NoAlerts` — все условия здоровы → `[]`.
- `Evaluate_CollectsAllRules` — снимок с 4 проблемами (failures, битый ключ,
  ghost-кластер, alarm) → алерты всех соответствующих kind'ов.
- `Evaluate_Ids_AreKindColonTarget` — формат id у каждого алерта.
- `Evaluate_Sorts_SeverityDescThenKindThenTarget` — critical раньше warning,
  внутри уровня — по kind/target (Ordinal).
- `Evaluate_RuleKinds_Unique` — у правил t04 kind'ы не дублируются (защита
  каркаса от copy-paste t05/t06).

Правила (по §4.2; time через `FixedTimeProvider`):

- `Unreachable_AtThresholdTwo_Critical` — `ConsecutiveFailures=1` → нет,
  `=2` → critical с target `etcd` и `details.consecutiveFailures`.
- `NoQuorum_WhenSuspected_CriticalWithErrors` — `QuorumSuspected=true`,
  ошибки endpoints в `details.errors`.
- `EndpointDown_PerFailedEndpoint_Warning` — 1 из 3 упал → один алерт,
  target = URL, message содержит URL.
- `EndpointDown_AllAlive_NoAlert`.
- `Alarm_PerAlarm_CriticalWithMemberIdType` — NoSpace + Corrupt → два алерта,
  target `"{memberId}:nospace"` / `":corrupt"`.
- `SnapshotStale_AfterThreeIntervals_Warning` — возраст `3×interval + 1 c` →
  есть (details `ageSeconds`/`thresholdSeconds`/`builtAtUnix`); `2×interval` →
  нет.
- `ClusterIncomplete_OnlyIncompleteClusters` — ghost (без config) → warning с
  target=имя; полный demo → нет.
- `KeyMalformed_PerParseError` — две записи `ParseErrors` → два алерта,
  target = ключ, `details.reason`.

sinceUnix (§3.4):

- `SinceUnix_CarriedFromPrevious` — id был в `previous` с `SinceUnix=T` → тот
  же `T`.
- `SinceUnix_NewAlert_GetsCurrentUnix` — id новый → `nowUtc.ToUnixTimeSeconds()`.
- `SinceUnix_NullOnFirstTick` — `previous=null` → все `SinceUnix=null`.
- `SinceUnix_DisappearedAlert_NotResurrected` — алерт был, условия больше нет
  → в новом списке отсутствует (история не хранится).

### 10.2. `SnapshotRefresherTests` (правки + новые)

- Харнесс `RefresherTestHarness.New`: `+ AlertEngine` (список правил t04).
- `Refresh_AlertsStoredOnSuccessTick` — фикстура с одним битым ключом →
  `store.Current.Alerts` содержит `key-malformed:…`.
- `Refresh_AlertsComputedOnFailTick` — успешный тик, затем 2 отказных →
  `Alerts` содержит `etcd-unreachable:etcd`; `cluster-incomplete` из прежних
  данных сохраняется с прежним `SinceUnix` (перенос).
- Существующие кейсы — без изменений ассертов.

### 10.3. `InspectionMappersTests`

- `OverviewMapper_CountsEtcdAndAlerts` — critical/warning/ok/total, возраст,
  `stale=false`.
- `OverviewMapper_StaleByTripleInterval_True` — порог 3×interval.
- `OverviewMapper_NegativeAgeClampedToZero` — BuiltAtUtc в будущем (скачок
  часов) → `snapshotAgeMs=0`.
- `OverviewMapper_ClusterStubs_Empty` — `clusters`/`activeMoves` пусты (t04).
- `EtcdStatusMapper_ActiveFlag_OnlyForActiveEndpoint`.
- `EtcdStatusMapper_IsLeader_ByLeaderMemberIdOfAliveEndpoint` — id строкой,
  fallback на неживой endpoint с leader, отсутствие leader → все false.
- `EtcdStatusMapper_MapsAlarmsQuorumLastRefresh`.
- `AlertsMapper_SeverityLowercaseStrings` — Critical→`"critical"` и т.д.
- `AlertsMapper_PassesDetailsAndSinceUnix`.
- `AlertsMapper_Filters_SeverityKindBoth`.

### 10.4. `InspectionQueryHandlerTests`

- Хендлеры через `new SnapshotStore()` (без DI): `OverviewQueryHandler` /
  `EtcdStatusQueryHandler` / `AlertsQueryHandler`.
- `Handle_NoSnapshot_ReturnsFailedSnapshotNotReady` — `Current=null` →
  `Result.Failed`, error — `SnapshotNotReadyException` (эндпоинт-маппинг 503 —
  integration §9.1).
- `Handle_WithSnapshot_ReturnsDto` — положили снапшот → успех, DTO на месте.
- `AlertsHandler_AppliesFilters` — severity/kind/оба/null.

## 11. Ограничения (что НЕ делается)

- Алерты шардирования (shard-no-master, move-*, bucket-*) — t05; HA/проба
  (shard-no-leader, ha-*, replica/slot/inventory, probe-failed) — t06. Каркас
  принимает их как новые `IAlertRule`-классы — правки `AlertEngine` не
  потребуются.
- Эндпоинты `/api/clusters*`, `/api/ha*` — t05/t06; `OverviewDto` наполнение
  кластерной части — t05; HA-сводка Overview — по arch/03 §3 (t09 доливает).
- `AlertsOptions`/`AdminPanel:Alerts` — t05 (§3.6); никаких настроек в t04.
- История/хранение алертов, дедупликация по времени, mute/ack — нет (03 §2
  «без хранения истории»); watch/push — нет (02 §5).
- Фронтенд, OpenAPI-документация вручную — нет (OpenAPI уже подключён,
  новые GET попадают автоматически).
- Кластеризация/collapse массовых `key-malformed` — нет: по одному на ключ
  (их единицы; порог шума — пересмотр с данными эксплуатации).
- Мутации `arch/01–04` запрещены; roadmap — только удаление пункта t04 (§14).

## 12. Пакеты

Новых пакетов нет. Используемое уже в решении: `Microsoft.Extensions.*`
(Options/DI/Hosting), ASP.NET Core Minimal API, xunit.v3 + FluentAssertions,
`Testcontainers` (t03), `Microsoft.AspNetCore.Mvc.Testing` (t02). CPM не
меняется.

## 13. Настройки тестовых проектов

Без изменений: ссылки проектов уже расставлены (UnitTests → Core/Etcd/Api;
IntegrationTests → Core/Etcd/Api + Testcontainers). Новых фикстур-файлов нет
(снапшот-фикстуры собирает `TestSnapshots`-код, а не JSON).

## 14. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t04-etcd-api` из `arch/roadmap/etcd.md`.
Зависимости `← t04-etcd-api` в `arch/roadmap/sharding.md` (t05),
`arch/roadmap/ha.md` (t06), `arch/roadmap/frontend.md` (t07) НЕ трогаются —
по указанию координатора и прецеденту t03 (зависимость чистится
задачей-владельцем). Правка выполняется в ветке задачи до мержа — попадает в
мерж-коммит.

## 15. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings
   (`TreatWarningsAsErrors=true` не подавлен).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные (нужен Docker:
   Testcontainers-etcd; unit — без Docker).
3. Unit: все 7 правил t04, механика каркаса (id/sinceUnix/сортировка),
   мапперы, 503-отказ хендлеров покрыты (§10).
4. Integration: 401 без cookie на новых эндпоинтах; 503 без снапшота;
   200-DTO с фикстурным снапшотом; 400/фильтры `/api/alerts`; путь данных
   живой etcd → refresher → AlertEngine → API (§9).
5. `SnapshotRefresher` вычисляет `Alerts` на обоих путях тика: на фикстуре с
   аномалиями — data-алерты в снапшоте; 2 отказных тика → `etcd-unreachable`
   (§10.2, §9.3).
6. Auth не ослаблен: новые эндпоинты под guard (401-смоук §9.1); правок
   `AuthModule` нет.
7. `grep PackageReference` по csproj: изменений нет (§12).
8. Панель по-прежнему не пишет в etcd (ревью: новых вызовов gateway нет;
   `kv/put` — только тесты).
9. Пункт `t04-etcd-api` отсутствует в `arch/roadmap/etcd.md`; зависимости
   `← t04-etcd-api` в sharding/ha/frontend сохранены; других мутаций `arch/`
   нет.
10. Все решения §3 не противоречат arch/01 §1–2/§6, arch/02 §4, arch/03
    (проверка на ревью).

## 16. Риски и заметки

- **Правка конструктора `SnapshotRefresher`** ломает компиляцию существующих
  харнессов (unit `RefresherTestHarness.New`, integration
  `EtcdTestHarness.NewRefresher`) — правки точечные (один аргумент), это
  осознанная цена DI-интеграции; альтернатива (статический вызов без
  интерфейса) лишала бы t05/t06 подмены правил в тестах.
- **Соревнование тестов за `TestSnapshotStore`**: коллекция `"api"`
  сериализована xunit'ом; тесты обязаны ставить `factory.Snapshot` в Arrange
  каждый раз (не полагаться на порядок) — фиксируется в §9.1.
- **`sinceUnix` на первом тике = null** — сознательное решение (§3.4):
  фронтенд t07 обязан рендерить «—» для null; иначе видно ложное «с сейчас».
- **Кластерная заглушка Overview** — поля присутствуют, списки пусты:
  фронтенд t07 не должен рассчитывать на заполненность до t05 (бейдж
  «данных нет» — его забота).
- **`stale` и `snapshot-stale` дублируют формулу** 3×interval в двух местах
  (правило и OverviewMapper) — оба покрыты тестами на одинаковый порог;
  вынос в общий хелпер Core возможен, но создаёт coupling маппера Api к
  правилу — оставлено простым (две строки).
- **Живой-etcd смок зависит от Docker** — как и все integration (CI-нотис
  t03); при недоступном Docker падает вся integration-сборка, не только t04.
- **decimal-строки id в DTO** — фронтенд t07 сравнивает их как строки
  (сопоставление member↔leaderId в `EtcdStatusMapper` уже сделано на стороне
  API — фронту только отображение).
- **`RemoveAll<IHostedService>`** убирает и будущие hosted-сервисы из
  тестового хоста — при появлении новых (t06 пробы) фабрика потребует
  точечного отключения только refresher'а; фиксируется как заметка в коде
  фабрики.
