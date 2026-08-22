# Спецификация t06-ha-api — HA-инспекция, live-пробы и HA-алерты

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/ha.md` (пункт `t06-ha-api` — объём),
`arch/03-panels.md` (ГЛАВНЫЙ документ задачи: эндпоинты §1, DTO §2,
каталог алертов §4 — 9 kind'ов t06, SQL-каталог пробы §5),
`arch/02-etcd-contract.md` §2.2 (ключи `/service/`), §3 (модель снапшота:
`HaScope`/`HaMember`/`ShardRuntime`), §4 (отдельный тик проб, «пробы
вносятся из их последнего результата»), §6 (live-пробы: Patroni REST
`:8008/cluster`, SQL Npgsql, HostMap-порядок, пароль из настроек),
§7 (вырожденные случаи), `arch/01-architecture.md` §1 (направление
зависимостей, «пробы пишут в снапшот отдельным тиком»), §6 (секции
настроек `AdminPanel:Probes` и `AdminPanel:Alerts`), §8 (Patroni REST
недоступен), `arch/04-local-stand.md` §1/§2.3 (HostMap-пример стенда).
Фактическое состояние кода — t05 (`src/`): `SnapshotRefresher` +
`SnapshotBuilder` + `SnapshotStore`/`ISnapshotStore` (Etcd),
`AlertEngine`-каркас + 15 правил (t04+t05), `AlertsOptions` без
HA-порогов (завещано t05 §3.11), `InspectionModule` (overview/etcd/
clusters/alerts), `ShardRuntimeDto`-контракт и маппер runtime (t05 §3.14,
данных нет), `HaMember` с probe-полями (t03, всегда null), модуль
`AdminPanel.Probes` — пустой каркас `ModuleExtensions.AddProbes()`,
integration-фабрика `"api"` с `TestSnapshotStore` и
`RemoveAll<IHostedService>`.

## 1. Цель

Третья зона инспекции — **HA** (Patroni DCS + live-состояние нод) и
живые пробы, обогащающие снапшот (данные `/service/` уже парсятся t03):

1. **Модуль `AdminPanel.Probes`** (сейчас пуст): фоновый оркестратор
   (тик 15 c) + две пробы —
   - **PatroniRestProbe**: `GET http://<host>:8008/cluster` (timeout 3 c)
     по каждому member-хосту каждого matched-скопа, параллельно;
   - **SqlProbe**: Npgsql по разобранному DSN шарда (host'ы через
     `HostMap`, пароль из `AdminPanel:Probes:Password`,
     `TargetSessionAttributes=ReadWrite`,
     `default_transaction_read_only=on`), read-only запросы каталога
     03 §5 (pg_stat_replication, pg_replication_slots,
     pg_stat_subscription, инвентарь `bucket_%`, pg_is_in_recovery);
   - результат — `ProbeState` в отдельном сторе; refresher вносит его в
     следующий снапшот (arch/02 §4): `HaMember`-поля (timeline, лаг,
     probe-статус), `ShardInfo.Runtime`, `EtcdSnapshot.Probes`.
2. **Эндпоинты** `GET /api/ha` (сводный список скопов) и
   `GET /api/ha/{scope}` (детали: leader, members+runtime, optime, raw
   config) — последние незакрытые строки таблицы arch/03 §1.
3. **9 правил HA-алертов** (03 §4): `shard-no-leader`,
   `ha-member-not-streaming`, `replica-lag-high`, `slot-lag-high`,
   `slot-wal-lost`, `slot-invalidation-risk`, `sync-standby-missing`,
   `inventory-mismatch`, `probe-failed` — новыми классами `IAlertRule`
   **без правок `AlertEngine`/`IAlertEngine`/`AlertContext`**
   (обещание t04 §3.2, прецедент t05 §5).
4. **Пороги** `AdminPanel:Alerts`: `ReplicaLagBytes` (16 МБ, arch/01 §6;
   заводится t06 — обещание t05 §3.11) и `SlotSafeWalSizeBytes` (§3.8).
5. **Отключаемость проб** (`PatroniEnabled`/`SqlEnabled`, arch/02 §6):
   выключенная проба не выполняется, её поля в снапшоте отсутствуют
   (runtime null, probe-поля null), её алерты не вычисляются
   («SQL-алерты — только при включённых пробах», 03 §4).

Тесты: unit — парсер Patroni `/cluster`-JSON, HostMap-резолвер,
построитель SQL-строки (мержинг пароля/маппинг хостов), слияние
probe-состояния со снапшотом, оркестратор на фейках, 9 правил на
фикстурах (пороги/границы/отключённость); integration — Patroni-проба
против локального HTTP-стаба (с HostMap e2e), SQL-проба против
Testcontainers postgres:18, HA-эндпоинты через фабрику `"api"`,
живой путь «etcd-сид → refresher с probe-стором → AlertEngine → API».

Пакеты: `Npgsql` 10.0.3 — версия референса `../Puzzle`
(`src/Directory.Packages.props`, CPM; §12).

## 2. Принципы

- Источник истины — `arch/`; всё, что arch/ не оговаривает, решено
  минимальным production-ready способом и зафиксировано в §3.
  Расхождение с arch/ запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде — русские. Тексты
  `message` алертов — русские (прецедент t04 §2, t05 §4.3).
- Тесты — xunit v3 + FluentAssertions, комментарии по AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- Паттерны t01–t05 обязательны: attribute-DI, query-ветка CQRS,
  `Result`-монада, файл запроса = query + DTO + статический mapper +
  handler, unit — без хоста, integration — один Program-хост на
  процесс, коллекция `"api"` сериализована.
- API не ходит в etcd/PG на запрос: только чтение `ISnapshotStore`.
  Пробы — тоже не на запросах: фоновый тик (arch/01 §1).
- Направление зависимостей (arch/01 §1): `Etcd → Core`, `Probes →
  Core`, `Core → Infrastructure`. Probes не ссылается на Etcd-модуль
  и наоборот; стык — типы Core (§3.2).
- Панель остаётся read-only: к PG — только SELECT из
  `pg_catalog`/`pg_stat_*` (архивный каталог 03 §5, тексты не
  меняются), к etcd — только чтение (пробы ничего не пишут в DCS).
- Мутации `arch/01–04` запрещены; из `arch/roadmap/` меняется только
  `ha.md` — удаление пункта t06 (§14).

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **Слияние проб со снапшотом делает refresher, а не оркестратор**
   (arch/02 §4 п.3 «пробы вносятся из их последнего результата»,
   §4 «результат обогащает следующий снапшот»). Пробы пишут только
   своё состояние — `IProbeStateStore` (Core); `SnapshotRefresher`
   на каждом успешном тике применяет чистый `ProbeEnricher.Apply(
   снапшот, probeState)` перед алертами. Единственный писатель
   `ISnapshotStore` — refresher (arch/01 §1); «пробы пишут в него же»
   (arch/01 §1) понимается как «их результат попадает в снапшот
   refresher'ом», т.к. arch/02 §4 задаёт механику явно. Цена —
   свежесть проб отстаёт от KV-данных не более чем на один тик
   refresher'а (≤ 3 c): приемлемо, проба сама редкая (15 c).
2. **Стык модулей — типы Core.** `Etcd` не может ссылаться на
   `Probes` и наоборот (arch/01 §1), поэтому в Core заводятся:
   `ProbeState`-модель + `IProbeStateStore` (интерфейс стора проб,
   impl — в Probes) + `ISnapshotReader { EtcdSnapshot? Current }`
   (доступ к текущему снапшоту для модулей вне Etcd; orchestrator
   читает из него цели проб). В Etcd: `ISnapshotStore :
   ISnapshotReader` (наследование интерфейса; `SnapshotStore`
   регистрируется под обоими интерфейсами —
   `[InjectAsSingleton(typeof(ISnapshotStore), typeof(ISnapshotReader))]`;
   механика мульти-интерфейсов InjectAs уже поддержана каркасом).
   Существующие потребители `ISnapshotStore` не меняются.
3. **Цели проб — только matched-скопы и шарды известных кластеров.**
   Unmatched-скоп — «чужой service в общем etcd — норма» (arch/02 §7):
   отображается как есть, без проб и без алертов; HostMap его адресов
   не знает. Шарды без `DsnHosts` (пустой dsn) SQL-пробой не трогаются.
4. **Patroni-проба — пер-member, из ответа берётся своя запись.**
   arch/02 §6.1: запрос `GET http://<host>:8008/cluster` делается «по
   каждому member-хосту scope'а» — каждый member опрашивается через
   собственный REST (своя нода знает своё состояние лучше всех).
   Ответ `/cluster` — вид всего кластера; из него берётся запись с
   `name == <member>` (REST-представление этого члена). Нет такой
   записи (переименованная нода) — ошибка пробы «member не найден в
   ответе /cluster». Отказ REST ноды — ошибка пробы этого member'а;
   соседние member'ы того же скопа продолжают опрашиваться своими
   эндпоинтами (избыточность «по каждому member-хосту» и есть
   устойчивость к смерти одного REST).
5. **Правила обогащения `HaMember`** (arch/01 §8: «etcd-часть HA
   остаётся»): при успехе пробы поля REST перекрывают DCS —
   `Role`/`State` из ответа (REST — фактическое состояние, arch/02
   §6.1), `Timeline`/`LagBytes` из ответа, `ProbeAtUtc` = время пробы,
   `ProbeError = null`; при ошибке — DCS `Role`/`State` остаются,
   `Timeline`/`LagBytes = null` (не показываем протухшие лаги),
   `ProbeAtUtc` = время попытки (когда наблюдали ошибку),
   `ProbeError` = текст. Члены без результата пробы (проба выключена
   или тик ещё не было) — не трогаются (поля null, как t03).
6. **SQL-строка строится из разобранных полей `ShardInfo`**
   (`DsnHosts`/`Port`/`DbName`/`User` — DSN уже разобран DsnParser t03;
   повторный парсинг libpq-строки в Probes не нужен и нарушил бы
   направление зависимостей). Каждый `host:port` (порт — `Port ?? 5432`)
   прогоняется через `HostMap` (точное совпадение ключа, arch/02 §6),
   хосты передаются эндпоинт-синтаксисом Npgsql `h1:p1,h2:p2` (явный
   порт у каждого — после маппинга порты могут различаться). Далее:
   `Password` из `AdminPanel:Probes:Password` (пусто — ключ не
   ставится: стенд trust, arch/04 §5), `Username = ShardInfo.User`
   (null — не ставится), `Database = DbName` (null — не ставится),
   `TargetSessionAttributes=ReadWrite`, `Application Name=adminpanel`,
   `Timeout`/`CommandTimeout` = `TimeoutSeconds` (statement_timeout,
   arch/02 §6.2), `Options=-c default_transaction_read_only=on`
   (двойная защита от записи). Один пароль на все кластеры — по
   контракту arch/01 §6 (`Password`, единственное число).
7. **Отказ SQL-пробы — целиком на шард.** Любая ошибка (подключение,
   любой из 5 запросов) → `ShardRuntime { Error = текст, списки пустые,
   IsInRecovery = null }` + `ProbeResult(ok:false)`. Частичные
   результаты не отдаются: полусобранный runtime вводил бы в заблуждение
   (лаги слотов без инвентаря и т.п.), а каталог 03 §5 — атомарный
   набор. Транзакция не нужна: все запросы — одиночные SELECT catalogs.
8. **Пороги алертов.** `slot-lag-high` использует тот же
   `ReplicaLagBytes` (16 МБ): каталог пишет «лаг слота > порога», не
   именуя отдельного ключа, а arch/01 §6 определяет единственный
   лаг-порог. Для `slot-invalidation-risk` («safe_wal_size < порога»)
   заводится `SlotSafeWalSizeBytes = 1 GiB`: семантика порога иная
   (остаток WAL до среза, а не лаг; предупреждать заранее — P4 «ДО
   среза»), место порогов по 03 §4 — секция `AdminPanel:Alerts`.
   Фолбэк `<= 0` → каталогный дефолт (константы правил), как t05 §3.11.
9. **`probe-failed` — severity `info`** по каталогу 03 §4. Сводка
   arch/01 §8 называет его «warning» — конфликт разрешён в пользу
   каталога (t04-прецедент: severity/условия — канон 03 §4; arch/01 —
   обзор). Алерт выдаётся на каждый неуспешный `ProbeResult` (kind
   patroni/sql), т.е. максимум один алерт на цель на тик.
10. **`shard-no-leader` — только для matched-скопов.** Каталог: «HA-scope
    без `leader`-ключа»; unmatched-скоп не алертится вовсе (arch/02 §7 —
    чужой service не наша зона ответственности). Условие —
    `LeaderName == null` (нет ключа `/service/<scope>/leader`).
11. **`inventory-mismatch` — сверка только по ACTIVE-бакетам.**
    Ожидаемые схемы шарда = `{bucket_<id> | routing owner == шард И
    State == Active}`; фактические = `Runtime.BucketSchemas`. Бакеты в
    переезде (SYNCING/FROZEN/ABORTING) исключены с обеих сторон: схема
    на шаре-приёмнике в момент копирования — норма (P21/P23 ищут именно
    «тихие» расхождения устоявшейся карты, а не артефакты переезда).
    Окно в секунды после flip (статус снят атомарно с flip, схема-источник
    удаляется следом в том же прогоне move-bucket.sh) может дать
    короткоживущий алерт — принято: он отражает реальное (пусть
    краткое) расхождение и гаснет следующим тиком.
12. **`sync-standby-missing` — по букве каталога**, без carve-outs:
    `Runtime` есть и без ошибки, `IsInRecovery == false` (попали на
    мастера; `TargetSessionAttributes=ReadWrite` это обеспечивает) и
    нет standby с `SyncState in ("sync","quorum")` → warning. Шард без
    физических реплик честно алертится (P8: переезды невозможны);
    легитимные одиночные шарды эксплуатируются с `SqlEnabled=false`.
13. **`ha-member-not-streaming` — ожидание по роли**: `master` →
    `running`, `replica` → `streaming`, прочие роли Patroni (напр.
    `sync_standby`) не проверяются (нет каталожного ожидания).
    Проверяются только члены с успешной пробой (`ProbeError == null` и
    `ProbeAtUtc != null`): ошибки проб кормят `probe-failed`, а не этот
    kind; без проб (выключены/не было тика) правило молчит (03 §4).
14. **Идентификация целей.** `ProbeResult.Target`: patroni —
    `"<scope>/<member>"`, sql — `"<cluster>/<shard>"`; `Kind`:
    `"patroni"` / `"sql"`. Алерт `probe-failed` получает target
    `"<kind>:<target>"` (напр. `patroni:demo-s1/s1a`) — стабильный
    уникальный `id` даже при теоретическом пересечении имён
    scope/кластеров. Target'ы slot-алертов —
    `"<cluster>/<shard>/<slot_name>"` (уникальность per-slot).
15. **Оркестратор**: тик `IntervalSeconds` (default 15, `<= 0` → 15 +
    LogWarning — прецедент t03 §3.3), первый тик сразу (прецедент t03
    §7.2). За тик: чтение `ISnapshotReader.Current` (null — пустой
    тик, запись пустого `ProbeState`), сбор целей, все пробы параллельно
    (`Task.WhenAll`; таймаут каждой ограничен `TimeoutSeconds`),
    сборка `ProbeState` и одна атомарная замена в `IProbeStateStore`.
    Ошибка отдельной цели ловится в её зону (`ProbeResult(ok:false)`),
    тик не падает. `PatroniEnabled=false` → patroni-цели не выполняются
    и в `ProbeState.Members` пусто; `SqlEnabled=false` → аналогично для
    `Runtimes`; оба выключены → фоновый цикл не стартует вовсе (один
    LogInformation при старте; hosted-сервис остаётся зарегистрированным).
16. **Фабрика `"api"` не меняется.** `RemoveAll<IHostedService>` (t04
    §3.16) отключает и новый оркестратор проб — это корректно:
    снапшот фабрики под контролем теста (`TestSnapshotStore`), а
    оркестратор не пишет в `ISnapshotStore` (§3.1) и в фабричных
    тестах не нужен. Заметка t04 §16 («потребуется точечное отключение
    refresher'а») закрывается: точечное отключение не требуется.
    Оркестратор тестируется прямой конструкцией (как refresher в
    `EtcdTestHarness`).
17. **`GET /api/ha` — сводный DTO по UI-таблице** (03 §3), прецедент
    `ClusterSummaryDto` (t05 §3.2): `HaScopeSummaryDto { scope, cluster,
    shard, matched, leaderName, membersTotal, membersHealthy, lagMaxBytes }`.
    `membersHealthy` — члены с `State in ("running","streaming")`
    (по модели после слияния — DCS или REST, источник не различается);
    `lagMaxBytes` = max `LagBytes` членов (null, если ни у кого нет).
    Порядок — как в снапшоте (парсер отдаёт по Scope Ordinal, t03).
18. **`GET /api/ha/{scope}` — 404 для неизвестного scope**
    (`ScopeNotFoundException`, прецедент t05 §3.10); DTO — по arch/03 §2
    дословно (§6.2). `optimeLeader` — number: LSN-величины < 2^53
    (правило decimal-строк t04 §3.11 — только для uint64 member-id).
    `rawConfig` — строка-как-есть (raw JSON, arch/02 §2.2).
19. **HA-сводка Overview не добавляется**: `OverviewDto` (03 §2) полей
    HA не содержит; прецедент t05 §11 — фронтенду HA-список даёт
    `GET /api/ha`. Без правки `OverviewQuery`.
20. **HostMap-контракт**: ключ и значение — полные `host:port`
    (arch/02 §6 «точное совпадение»); нет совпадения — адрес
    без изменений; пуст по умолчанию (прод, arch/01 §6). Стендовые
    значения (arch/04 §2.3) в `appsettings.Development.json` НЕ
    пишутся: стенд — t10, значения появятся его коммитом вместе с
    самим стендом (в t06 стенда нет, маппить нечего).
21. **Числовой разбор SQL**: `pg_wal_lsn_diff`/`safe_wal_size` —
    numeric; читаются как decimal и приводятся к long в C# (разности
    LSN целочисленны, величины < 2^53). `client_addr` (inet) — через
    значение-объект + `ToString()` (не типизированное чтение). Lag в
    Patroni-JSON — number (толерантно к absent/null).
22. **HTTP-тег пробы**: roadmap упоминает «тег Application Name» для
    Patroni-пробы; Application Name — параметр SQL-строки (§3.6), для
    HTTP эквивалент — заголовок `User-Agent: AdminPanel` на каждый
    запрос (идентификация панели в access-логах patroni).

## 4. Live-пробы (модуль AdminPanel.Probes + стык Core/Etcd)

### 4.1. Состояние проб (Core)

```csharp
// Core/ProbeState.cs — состояние последнего тика проб (arch/02 §4, §6).
// Пишет только ProbeOrchestrator; читает SnapshotRefresher (§3.1).

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки.
public sealed record HaMemberProbe(
    string? Role, string? State, long? Timeline, long? LagBytes,
    DateTimeOffset AtUtc, string? Error);

// Состояние одного тика проб: цели — по matched-скопам и шардам кластеров.
public sealed record ProbeState(
    DateTimeOffset AtUtc,
    IReadOnlyList<ProbeResult> Probes,                    // все попытки, ok и error
    IReadOnlyDictionary<string, HaMemberProbe> Members,   // ключ "<scope>/<member>"
    IReadOnlyDictionary<string, ShardRuntime> Runtimes); // ключ "<cluster>/<shard>"

// Стор состояния проб: атомарная замена ссылки (зеркалит ISnapshotStore).
public interface IProbeStateStore
{
    ProbeState? Current { get; }
    void Replace(ProbeState state);
}
```

`ProbeResult` (t03) используется как есть: `Kind` = `"patroni"`/`"sql"`,
`Target` — §3.14, `LatencyMs` — замер `Stopwatch.GetTimestamp()`
(прецедент `SnapshotRefresher.StatusOfAsync`).

### 4.2. Слияние со снапшотом (Core/ProbeEnricher.cs)

Чистая функция; вызывается refresher'ом сразу после `SnapshotBuilder.Build`
(успешный тик) — правила §3.5, перенос §3.1:

```csharp
// Внесение результатов проб в свежий снапшот (arch/02 §4 п.3): члены HA
// обогащаются REST-полями, шардам ставится Runtime, Probes — последним тиком.
// state null (тиков не было) — снапшот без изменений (Probes уже пусты).
public static class ProbeEnricher
{
    public static EtcdSnapshot Apply(EtcdSnapshot snapshot, ProbeState? state);
}
```

Лишние ключи состояния (скоп/шард исчез из etcd между тиками)
игнорируются — lookup по ключам текущего снапшота.

### 4.3. Доступ к снапшоту (Core + Etcd)

```csharp
// Core/ISnapshotReader.cs — чтение текущего снапшота модулями вне Etcd
// (направление зависимостей arch/01 §1: Probes → Core, не → Etcd).
public interface ISnapshotReader
{
    EtcdSnapshot? Current { get; }
}

// Etcd/SnapshotStore.cs [правка — только атрибут]:
// ISnapshotStore наследует ISnapshotReader; регистрация под обоими интерфейсами.
[InjectAsSingleton(typeof(ISnapshotStore), typeof(ISnapshotReader))]
public sealed class SnapshotStore : ISnapshotStore { /* код без правок */ }
```

`ISnapshotStore` (`Etcd`) становится `: ISnapshotReader`; реализация не
меняется (`Current` уже есть). Отказный тик refresher'а дополнительно
сохраняет `previous?.Probes ?? []` (сейчас там `[]` — тика проб это
касается: Probes — часть снапшота, сохраняется как HaScopes/Clusters).

### 4.4. Настройки (Probes/ProbesOptions.cs)

```csharp
// [Config]-POCO live-проб: секция AdminPanel:Probes (arch/01 §6, arch/02 §6).
// Имена ключей — с суффиксом Seconds по прецеденту EtcdOptions (t03 §3.3).
[Config("AdminPanel:Probes")]
public class ProbesOptions
{
    // Patroni REST :8008/cluster — включена по умолчанию (arch/02 §6.1).
    public bool PatroniEnabled { get; set; } = true;

    // SQL-проба Npgsql — включена по умолчанию; в проде — на усмотрение (arch/02 §6.2).
    public bool SqlEnabled { get; set; } = true;

    // Тик оркестратора (arch/02 §4). <= 0 — fallback 15 c с LogWarning.
    public double IntervalSeconds { get; set; } = 15;

    // Таймаут одной пробы: HTTP-запрос / connection+command SQL (arch/01 §6).
    // <= 0 — fallback 3 c.
    public double TimeoutSeconds { get; set; } = 3;

    // Пароль SQL-проб (в DSN из etcd пароля нет никогда — arch/02 §2.1).
    // Пусто — ключ не попадает в строку подключения (стенд trust).
    public string Password { get; set; } = "";

    // «etcd-адрес ноды host:port» → «адрес, достижимый с хоста панели»
    // (arch/02 §6): точное совпадение ключа, иначе адрес без изменений.
    public Dictionary<string, string> HostMap { get; set; } = [];
}
```

### 4.5. HostMap-резолвер (Probes/HostMapResolver.cs)

```csharp
// Разрешение адреса цели пробы: адрес из etcd → override при точном
// совпадении host:port → прямое подключение (arch/02 §6, §6.2).
// Чистая функция — unit-тестируется без сети.
public static class HostMapResolver
{
    // "s1a" + 8008 + карта {"s1a:8008": "127.0.0.1:8011"} → "127.0.0.1:8011";
    // без совпадения → "s1a:8008". Возвращает полный "host:port".
    public static string Resolve(IReadOnlyDictionary<string, string> hostMap, string host, int port);
}
```

### 4.6. Patroni-проба (Probes/PatroniRestProbe.cs)

```csharp
// Распаренный член ответа GET /cluster (Patroni-формат, arch/02 §6.1;
// толерантно: поля отсутствуют/строчные числа — NumberHandling как EtcdGateway).
public sealed record PatroniClusterMember(
    string? Name, string? Role, string? State, long? Timeline, long? LagBytes);

public static class PatroniClusterParser
{
    // JSON {"members":[{name,role,state,timeline,lag,…},…]} → список; ошибки
    // JSON → исключение (ловится пробом → ProbeResult error).
    public static IReadOnlyList<PatroniClusterMember> Parse(string json);
}

public sealed record PatroniMemberResult(HaMemberProbe Enrichment, ProbeResult Result);

// Проба одного члена HA-скопа: GET http://<host>:8008/cluster.
public interface IPatroniRestProbe
{
    Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct);
}

// Реализация: typed HttpClient "patroni" (ModuleExtensions, таймаут из
// ProbesOptions.TimeoutSeconds — паттерн EtcdGateway/Etcd ModuleExtensions),
// заголовок User-Agent: AdminPanel (§3.22). URL — Resolve(HostMap, member.Host, 8008).
// Из ответа берётся запись name == member.Name (§3.4); латентность — Stopwatch.
[InjectAsSingleton(typeof(IPatroniRestProbe))]
public sealed class PatroniRestProbe : IPatroniRestProbe { … }
```

### 4.7. SQL-проба (Probes/SqlProbe.cs)

```csharp
public sealed record SqlShardResult(ShardRuntime Runtime, ProbeResult Result);

// Проба одного шарда: 5 запросов каталога 03 §5 одним подключением.
public interface ISqlProbe
{
    Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct);
}

// Реализация: NpgsqlConnectionStringBuilder (§3.6) → new NpgsqlConnection(cs)
// (открывается на тик; пул Npgsql переиспользует соединения сам).
// Чтение колонок: numeric → decimal → long (§3.21); inet → ToString.
[InjectAsSingleton(typeof(ISqlProbe))]
public sealed class SqlProbe : ISqlProbe { … }
```

SQL-тексты — дословно из arch/03 §5 (инвариант документа); порядок
выполнения: pg_is_in_recovery → pg_stat_replication →
pg_replication_slots → pg_stat_subscription → инвентарь `bucket_%`.
Каждый запрос — `CommandText`-константа в `SqlProbe` (без DDL/DML).

### 4.8. Оркестратор (Probes/ProbeOrchestrator.cs)

```csharp
// Фоновый тик проб (arch/02 §4 «отдельный тик Probes.Interval»): цели из
// текущего снапшота, все пробы параллельно, состояние — в IProbeStateStore.
// Пробы не блокируют тик KV (refresher берёт состояние готовым).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class ProbeOrchestrator(
    ISnapshotReader snapshotReader,
    IProbeStateStore stateStore,
    IPatroniRestProbe patroniProbe,
    ISqlProbe sqlProbe,
    IOptions<ProbesOptions> options,
    TimeProvider time,
    ILogger<ProbeOrchestrator> logger) : BackgroundService
{
    // ExecuteAsync: оба вида выключены → LogInformation + выход (§3.15);
    // иначе первый тик сразу и PeriodicTimer(IntervalSeconds) — прецедент t03.
    // RunOnceAsync — публичное ядро тика для тестов (прецедент RefreshOnceAsync):
    // 1) snapshot == null → пустой ProbeState; 2) цели (§3.3) → Task.WhenAll;
    // 3) ошибки per-цели → ProbeResult(ok:false); 4) одна замена в стор.
}
```

### 4.9. Стор (Probes/ProbeResultsStore.cs)

```csharp
// Хранилище состояния проб: volatile-замена ссылки (зеркалит SnapshotStore).
[InjectAsSingleton(typeof(IProbeStateStore))]
public sealed class ProbeResultsStore : IProbeStateStore { … }
```

### 4.10. ModuleExtensions (Probes) [правка]

`AddProbes()` после `AutoRegistration(Assembly)` добавляет именованный
HttpClient (порядок и паттерн — Etcd `ModuleExtensions`):

```csharp
services.AddHttpClient<PatroniRestProbe>(PatroniRestProbe.HttpClientName)
    .ConfigureHttpClient((sp, client) =>
    {
        // TimeoutSeconds <= 0 → 3 c + LogWarning (§4.4, прецедент EtcdOptions).
        client.Timeout = TimeSpan.FromSeconds(seconds);
    });
```

## 5. Правила HA-алертов (Core/Alerting/Rules/)

Механика — t04/t05 без правок: stateless-классы
`[InjectAsSingleton(typeof(IAlertRule))]`, автоскан `AddCore()`, id
`kind:target`, `sinceUnix`/сортировка — `AlertEngine`. «SQL-алерты —
только при включённых пробах» выполняется само: выключенная проба не
даёт данных (`Runtime` null / probe-поля null / `Probes` пуст), правила
на них молчат; etcd-часть (`shard-no-leader`) вычисляется всегда.

### 5.1. Таблица правил (условия — по модели после слияния)

| Класс | Kind | Severity | Условие | Target | По одному на |
|---|---|---|---|---|---|
| `ShardNoLeaderRule` | `shard-no-leader` | Critical | `Matched && LeaderName == null` (нет `/service/<s>/leader`, §3.10) | `{scope}` | matched-скоп |
| `HaMemberNotStreamingRule` | `ha-member-not-streaming` | Warning | проба успешна (`ProbeAtUtc != null && ProbeError == null`) и фактический `State !=` ожидания по роли (§3.13) | `{scope}/{member}` | член |
| `ReplicaLagHighRule` | `replica-lag-high` | Warning | проба успешна и `LagBytes > ReplicaLagBytes` | `{scope}/{member}` | член |
| `SlotLagHighRule` | `slot-lag-high` | Warning | `Runtime` без ошибки и `slot.LagBytes > ReplicaLagBytes` (§3.8) | `{cluster}/{shard}/{slot}` | слот |
| `SlotWalLostRule` | `slot-wal-lost` | Critical | `Runtime` без ошибки и `slot.WalStatus == "lost"` (P4) | `{cluster}/{shard}/{slot}` | слот |
| `SlotInvalidationRiskRule` | `slot-invalidation-risk` | Warning | `Runtime` без ошибки и `slot.SafeWalSizeBytes` есть и `< SlotSafeWalSizeBytes` (P4, ДО среза; null — skip) | `{cluster}/{shard}/{slot}` | слот |
| `SyncStandbyMissingRule` | `sync-standby-missing` | Warning | `Runtime` без ошибки, `IsInRecovery == false`, нет standby с `SyncState in ("sync","quorum")` (P8, §3.12) | `{cluster}/{shard}` | шард |
| `InventoryMismatchRule` | `inventory-mismatch` | Warning | `Runtime` без ошибки и симметрическая разность «ACTIVE-routing шарда» × `BucketSchemas` непуста (§3.11) | `{cluster}/{shard}` | шард |
| `ProbeFailedRule` | `probe-failed` | Info | `snapshot.Probes` c `Ok == false` (§3.9, §3.14) | `{kind}:{target}` | неудавшаяся проба |

Конструкторы: `ReplicaLagHighRule`/`SlotLagHighRule`/
`SlotInvalidationRiskRule` принимают `IOptions<AlertsOptions>`; остальные
беспараметрические. «Runtime без ошибки» = `Runtime != null &&
Runtime.Error == null`.

### 5.2. Сообщения и details (фиксируются; ключи camelCase, инвариант)

| Kind | Message (рус.) | Details |
|---|---|---|
| `shard-no-leader` | `HA-scope {scope} без leader-ключа (шард {cluster}/{shard} без лидера)` | `scope`, `cluster`, `shard` |
| `ha-member-not-streaming` | `член {member} scope {scope} в состоянии {state} (роль {role}, ожидалось {expected})` | `scope`, `member`, `role`, `state`, `expected` |
| `replica-lag-high` | `лаг члена {member} scope {scope} — {lag} байт, порог {threshold} байт` | `lagBytes`, `thresholdBytes` |
| `slot-lag-high` | `лаг слота {slot} шарда {cluster}/{shard} — {lag} байт, порог {threshold} байт` | `lagBytes`, `thresholdBytes` |
| `slot-wal-lost` | `слот {slot} шарда {cluster}/{shard}: wal_status=lost — WAL срезан, источник догонит только пересозданием (P4)` | `walStatus` |
| `slot-invalidation-risk` | `слоту {slot} шарда {cluster}/{shard} осталось {size} байт WAL до среза (порог {threshold} байт, P4)` | `safeWalSizeBytes`, `thresholdBytes` |
| `sync-standby-missing` | `у мастера шарда {cluster}/{shard} нет sync-standby (sync_state sync/quorum) — предусловие переездов не выполнено (P8)` | `standbiesTotal` |
| `inventory-mismatch` | `инвентарь схем шарда {cluster}/{shard} не совпадает с routing: отсутствуют [{missing}], лишние [{extra}]` | `missing`, `extra` (строки через запятую; отсутствующая сторона — пустая строка) |
| `probe-failed` | `проба {kind} по {target} не удалась: {error}` | `kind`, `target`, `error` |

Числа в details — инвариантные строки (`InvariantCulture`, прецедент
t05 §4.3); отсутствующие nullable-поля в details не попадают.

### 5.3. Настройки (Core/Alerting/AlertsOptions.cs) [правка]

```csharp
[Config("AdminPanel:Alerts")]
public class AlertsOptions
{
    // … StaleMoveSeconds = 600, FrozenSeconds = 60 — без правок (t05 §4.5) …

    // replica-lag-high и slot-lag-high: порог лага в байтах (arch/01 §6;
    // каталог 03 §4). <= 0 — дефолт каталога.
    public long ReplicaLagBytes { get; set; } = 16 * 1024 * 1024;

    // slot-invalidation-risk: остаток safe_wal_size ниже порога — риск
    // среза слота (03 §4; §3.8). <= 0 — дефолт 1 GiB.
    public long SlotSafeWalSizeBytes { get; set; } = 1024L * 1024 * 1024;
}
```

Константы правил: `ReplicaLagHighRule.DefaultBytes = 16 * 1024 * 1024`,
`SlotInvalidationRiskRule.DefaultBytes = 1024L * 1024 * 1024` (фолбэк
`<= 0`, прецедент t05 §3.11). `appsettings.json`: секция `AdminPanel:Alerts`
дополняется обоими ключами с дефолтами (самодокументирование, прецедент t05 §4.5).

## 6. API-эндпоинты (AdminPanel.Api/Inspection/)

### 6.1. InspectionModule.cs [правка]

```csharp
// GET /api/ha — сводный список HA-скопов (arch/03 §1).
endpoints.MapGet("/api/ha", … HandleQuery<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>);

// GET /api/ha/{scope} — детали скопа (arch/03 §1); ScopeNotFoundException → 404,
// прочий отказ → 503 — маппинг как у /api/clusters/{cluster} (t05 §6.1).
endpoints.MapGet("/api/ha/{scope}", (string scope, …) =>
    // … HandleQuery<HaScopeDetailsQuery, HaScopeDto>(new(scope)) →
    // успех 200; ScopeNotFoundException → Problem 404 "Scope not found";
    // прочее → 503 "Snapshot not ready".
```

`ScopeNotFoundException` — публичный тип модуля (рядом с
`ClusterNotFoundException`). Auth-guard уже закрыл `/api/*`.

### 6.2. HaQuery.cs [новый: query + DTO + mapper + handler]

```csharp
public sealed record HaScopesQuery : IQuery<IReadOnlyList<HaScopeSummaryDto>>;

// Сводный список — UI-таблица HA (03 §3; §3.17): агрегаты по скопу.
public sealed record HaScopeSummaryDto(
    string Scope, string? Cluster, string? Shard, bool Matched,
    string? LeaderName, int MembersTotal, int MembersHealthy, long? LagMaxBytes);

public sealed record HaScopeDetailsQuery(string Scope) : IQuery<HaScopeDto>;

// Детали — arch/03 §2 HaScopeDto дословно (§3.18). Initialized модели
// (t03, /initialize) в DTO не входит — его нет в контракте 03 §2.
public sealed record HaScopeDto(
    string Scope, string? Cluster, string? Shard, bool Matched,
    string? LeaderName, long? OptimeLeader,
    IReadOnlyList<HaMemberDto> Members, string? RawConfig);

public sealed record HaMemberDto(
    string Name, string Host, int? Port, string? Role, string? State,
    long? Timeline, long? LagBytes, DateTimeOffset? ProbeAtUtc, string? ProbeError);

public static class HaMappers
{
    // Чистые функции: сводка (membersHealthy/lagMax — §3.17) и детали
    // (прямой перенос полей модели); порядок — как в снапшоте.
    public static IReadOnlyList<HaScopeSummaryDto> MapSummaries(IReadOnlyList<HaScope> scopes);
    public static HaScopeDto MapDetails(HaScope scope);
}

[InjectAsScoped]
public sealed class HaScopesQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>;
// null-снапшот → Failed(SnapshotNotReadyException) → 503 (t04 §3.12).

[InjectAsScoped]
public sealed class HaScopeDetailsQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopeDetailsQuery, HaScopeDto>;
// null-снапшот → 503; scope не найден → Failed(ScopeNotFoundException) → 404 (§3.18).
```

`runtime` в `ShardDto` наполняется автоматически: маппер t05 уже
проецирует `ShardInfo.Runtime`, который после t06 не null при включённой
SQL-пробе — правок `ClusterDetailsQuery.cs` нет, данные появляются.

### 6.3. Сводка контракта HTTP (сверка с arch/03 §1)

| Метод+путь | Auth | Успех | Отказ |
|---|---|---|---|
| `GET /api/ha` | cookie | 200 `HaScopeSummaryDto[]` | 401 без cookie; 503 ProblemDetails до первого тика |
| `GET /api/ha/{scope}` | cookie | 200 `HaScopeDto` | 401; 503; **404** ProblemDetails `Scope not found` для неизвестного имени |

Проблемные ответы — `application/problem+json` (паттерн t02/t04/t05).

## 7. Состав изменений (дерево файлов)

```
src/AdminPanel.Core/
├── ISnapshotReader.cs                        [новый] чтение снапшота вне Etcd (§4.3)
├── ProbeState.cs                             [новый] ProbeState/HaMemberProbe/IProbeStateStore (§4.1)
├── ProbeEnricher.cs                          [новый] слияние состояния проб со снапшотом (§4.2)
└── Alerting/
    ├── AlertsOptions.cs                      [правка] + ReplicaLagBytes, SlotSafeWalSizeBytes (§5.3)
    └── Rules/
        ├── ShardNoLeaderRule.cs              [новый] critical, нет leader-ключа
        ├── HaMemberNotStreamingRule.cs       [новый] warning, member не running/streaming
        ├── ReplicaLagHighRule.cs             [новый] warning, лаг члена > ReplicaLagBytes
        ├── SlotLagHighRule.cs                [новый] warning, лаг слота > ReplicaLagBytes
        ├── SlotWalLostRule.cs                [новый] critical, wal_status=lost
        ├── SlotInvalidationRiskRule.cs       [новый] warning, safe_wal_size < порога
        ├── SyncStandbyMissingRule.cs         [новый] warning, нет sync/quorum standby
        ├── InventoryMismatchRule.cs          [новый] warning, схемы bucket_% ≠ routing
        └── ProbeFailedRule.cs                [новый] info, неудавшиеся пробы
src/AdminPanel.Etcd/
├── SnapshotStore.cs                          [правка] ISnapshotStore: ISnapshotReader;
│                                             атрибут += typeof(ISnapshotReader) (§4.3)
├── SnapshotRefresher.cs                      [правка] ctor += IProbeStateStore;
│                                             ProbeEnricher.Apply на успехе; FailTick
│                                             сохраняет previous?.Probes (§3.1, §4.3)
└── SnapshotBuilder.cs                        [без правок] — слияние в ProbeEnricher
src/AdminPanel.Probes/
├── AdminPanel.Probes.csproj                  [правка] + Npgsql, Microsoft.Extensions.Http,
│                                             Microsoft.Extensions.Hosting.Abstractions (§12)
├── ModuleExtensions.cs                       [правка] AddHttpClient "patroni" (§4.10)
├── ProbesOptions.cs                          [новый] [Config("AdminPanel:Probes")] (§4.4)
├── HostMapResolver.cs                        [новый] резолвер адресов проб (§4.5)
├── PatroniRestProbe.cs                       [новый] IPatroniRestProbe + PatroniClusterParser (§4.6)
├── SqlProbe.cs                               [новый] ISqlProbe + построитель строки (§4.7)
├── ProbeOrchestrator.cs                      [новый] BackgroundService-тик проб (§4.8)
└── ProbeResultsStore.cs                      [новый] стор состояния проб (§4.9)
src/AdminPanel.Api/
├── appsettings.json                          [правка] + AdminPanel:Probes (дефолты §4.4),
│                                             AdminPanel:Alerts += ReplicaLagBytes/SlotSafeWalSizeBytes
└── Inspection/
    ├── InspectionModule.cs                   [правка] + 2 маршрута HA, ScopeNotFoundException,
    │                                         404/503-маппинг (§6.1)
    └── HaQuery.cs                            [новый] query+dto+mapper+handler (§6.2)
src/tests/AdminPanel.UnitTests/
├── AdminPanel.UnitTests.csproj               [правка] + None ProbesFixtures\**\*.json
├── ProbesFixtures/patroni-cluster.json       [новый] реальный фрагмент Patroni /cluster
│                                             (pg-report §4: pg2/replica/streaming/timeline/lag)
├── AlertTestRules.cs                         [правка] + 9 правил t06 (пороговые — с Options)
├── TestSnapshots.cs                          [правка] + HA-фикстуры: скопы (matched/unmatched,
│                                             leader/без, члены с пробой/ошибкой), ShardRuntime
├── HaAlertRulesTests.cs                      [новый] 9 правил на фикстурах (§10.1)
├── HaMappersTests.cs                         [новый] сводка/детали/хендлеры 503/404 (§10.2)
├── PatroniClusterParserTests.cs              [новый] парсинг /cluster JSON (§10.3)
├── HostMapResolverTests.cs                   [новый] точное совпадение/identity (§10.4)
├── SqlConnectionFactoryTests.cs              [новый] построение Npgsql-строки (§10.5)
├── ProbeEnricherTests.cs                     [новый] слияние/null/лишние ключи (§10.6)
├── ProbeOrchestratorTests.cs                 [новый] оркестратор на фейках (§10.7)
└── SnapshotRefresherTests.cs                 [правка] ctor + enrich/preserve-кейсы (§10.8)
src/tests/AdminPanel.IntegrationTests/
├── EtcdTestHarness.cs                        [правка] NewRefresher += стор проб (§9.4)
├── EtcdSnapshotIntegrationTests.cs           [правка] + enrich-тест против живого etcd (§9.4)
├── InspectionProbeApiTests.cs                [новый] живой путь: снапшот с пробами →
│                                             /api/ha, /api/clusters runtime, /api/alerts (§9.3)
├── HaApiTests.cs                             [новый] HTTP-контракт /api/ha* (§9.2)
├── PatroniRestProbeTests.cs                  [новый] HTTP-стаб + HostMap e2e (§9.5)
├── PostgresFixture.cs                        [новый] Testcontainers postgres:18, wal_level=logical (§9.6)
└── SqlProbeIntegrationTests.cs               [новый] SQL-проба против живого PG (§9.6)
arch/roadmap/ha.md                            [правка] удалить пункт t06-ha-api (§14)
```

`Program.cs`, `AlertEngine.cs`, `IAlertEngine.cs`, `AlertContext.cs`,
парсеры Etcd, `AdminPanel.slnx`, `Directory.Packages.props` (кроме
Npgsql-версии), фабрика `"api"` — без изменений.

## 8. Интеграция и настройки

- DI: правила/опции Core — автоскан `AddCore()`; `ProbesOptions`,
  пробы, оркестратор, стор — автоскан `AddProbes()` (вызван в
  `Program.cs` с t01); HttpClient "patroni" — §4.10. Ручных регистраций нет.
- `ISnapshotReader` резолвится к тому же singleton-экземпляру
  `SnapshotStore`, что и `ISnapshotStore` (фабричная регистрация
  каркаса ведёт оба дескриптора к одному типу).
- `appsettings.json` (§7): `"Probes": { "PatroniEnabled": true,
  "SqlEnabled": true, "IntervalSeconds": 15, "TimeoutSeconds": 3,
  "Password": "", "HostMap": {} }` внутри `AdminPanel` —
  самодокументирование контракта (прецедент t05 §4.5; пустой Password —
  не секрет). `appsettings.Development.json` не меняется (§3.20).
- OpenAPI: новые GET попадают в схему автоматически (t04 §11).
- Порядок старта: оркестратор проб может тикнуть раньше первого тика
  refresher'а — `ISnapshotReader.Current == null` → пустой тик (§3.15),
  данные появятся со вторым тиком проб.

## 9. Integration-тесты (src/tests/AdminPanel.IntegrationTests/)

### 9.1. Принципы

Коллекция `"api"` — без правок фабрики (§3.16); контейнерные пробы —
прямая конструкция (прецедент `EtcdTestHarness`). Docker нужен (CI-нотис
t03): etcd- и postgres-контейнеры; при недоступности Docker падает
integration-сборка целиком — существующее поведение.

### 9.2. `HaApiTests` [Collection("api")]

HA-фикстура `InspectionSnapshots.Ha(builtAt)` (расширение файла
`InspectionApiTests.cs`): снапшот `Clustered`-типа + скопы: `demo-s1`
(matched, leader `s1a`, optime; члены: `s1a` — проба ок master/running/
timeline 1/lag 0; `s1b` — проба ок replica/streaming/lag 17 МБ;
API-фикстура алертов по HA не добавляет — alerts в снапшоте руками,
как `Fixture`) и `other-scope` (unmatched, без leader; 1 член: DCS
state `stopped`, проба упала — `probeError "connection refused"`,
timeline/lag null — §3.5). Тесты:

- `Ha_WithoutCookie_Return401` — оба пути без cookie → 401.
- `Ha_NoSnapshot_Return503ProblemDetails` — оба → 503 `Snapshot not ready`.
- `Ha_WithSnapshot_ReturnSummaries` — 2 записи; matched/unmatched,
  leaderName/null, membersTotal 2/1, membersHealthy 2/0 (DCS `stopped`
  у члена unmatched), lagMaxBytes 17 МБ/null; порядок по Scope Ordinal
  (`demo-s1` < `other-scope`).
- `HaDetails_ReturnsMembersWithProbeFields` — `demo-s1`: cluster/shard,
  leaderName, optimeLeader (number), rawConfig (строка),
  members[1]: name/host/port/role/state/timeline/lagBytes/probeAtUtc
  (ISO)/probeError(null).
- `HaDetails_MemberProbeError_Visible` — `other-scope`: role/state из
  DCS, timeline/lag null, probeError не пуст (§3.5).
- `HaDetails_UnknownScope_Return404ProblemDetails` — `?scope=nope` →
  404 `Scope not found`.

### 9.3. `InspectionProbeApiTests` [Collection("api") + EtcdContainerFixture]

Живой путь «etcd-сид → refresher (+ состояние проб) → API» (клейка
переносом снапшота в `TestSnapshotStore`, прецедент t04 §3.17):

- `LiveEtcd_ProbeStateEnriches_HaAndClusterApi` — Arrange: сид demo;
  `SettableProbeStateStore` (тестовый double в файле харнесса) с
  `Members` для `demo-s1/s1a`, `demo-s1/s1b` (streaming, lag) и
  `Runtimes` для `demo/s1`, `demo/s2` (standbies sync/quorum, слоты,
  схемы 16/16); `EtcdTestHarness.NewRefresher(store, probeStore,
  fixture.Endpoint)` → `RefreshOnceAsync` → снапшот в фабрику.
  Act: `GET /api/ha/demo-s1` (timeline/lag/probeAtUtc не null),
  `GET /api/ha` (2 скопа сида: demo-s1, demo-s2), `GET /api/clusters/demo`
  (`shards[].runtime` не null: standbiesSync, bucketSchemas 16),
  `GET /api/alerts?kind=probe-failed` → `[]`.
- `LiveEtcd_FailedProbe_ProducesProbeFailedAlert` — префилл с ошибкой
  (`patroni:demo-s1/s1a` connect refused) → `/api/alerts` содержит
  `probe-failed:patroni:demo-s1/s1a` (info) и `ha-member-not-streaming`
  НЕ содержит (ошибка — не «не стримит», §3.13).

### 9.4. Правки `EtcdTestHarness`/`EtcdSnapshotIntegrationTests`

- `NewRefresher(ISnapshotStore store, IProbeStateStore? probes = null,
  params string[] endpoints)` — новый параметр; по умолчанию пустой
  стор (`SettableProbeStateStore` — double с settable `Current`,
  прецедент `TestSnapshotStore`; живёт в файле харнесса).
  `SnapshotRefresher`-конструктор получает стор явно.
- Существующие ассерты без правок: пустой стор → `Probes` пуст,
  HA-правила молчат, список move-алертов неизменен.
- Новый тест `Refresher_EnrichesSnapshot_FromProbeState`: префилл →
  `RefreshOnceAsync` → `store.Current.HaScopes[…].Members[…]`
  обогащены, `Clusters[…].Shards[…].Runtime` проставлен,
  `Probes` = список стора, `Alerts` содержит `sync-standby-missing`
  (если у фикстурного runtime нет sync-standby) — точный состав
  фиксируется в тесте.

### 9.5. `PatroniRestProbeTests` (прямая конструкция)

Стаб — `HttpListener` на `127.0.0.1:<random>`: любой `GET` → тело
`patroni-cluster.json` (200, application/json); запись access-лога не
нужна. Проба конструируется с `HttpClient` и `ProbesOptions.HostMap =
{ "s1a:8008": "127.0.0.1:<stub-port>" }`:

- `Probe_MapsHostAndParsesSelfEntry` — member `s1a` → enrichment
  role/state/timeline/lag из своей записи; `ProbeResult.Ok`, latency
  > 0, target `demo-s1/s1a`, kind `patroni`.
- `Probe_AnotherMember_PicksOwnEntry` — member `s1b` → своя запись
  (другой lag/role).
- `Probe_UnmappedHost_DirectConnection` — HostMap без записи: probe на
  `127.0.0.1:<stub>` через member host `127.0.0.1` → ok (маппинг не
  обязателен, identity-ветка §3.20).
- `Probe_DeadPort_ReturnsError` — HostMap на закрытый порт →
  `Enrichment.Error` не пуст, `Result.Ok == false`, latency есть.
- `Probe_MemberMissingInResponse_Error` — стаб-ответ без записи
  member'а → ошибка «не найден в ответе /cluster» (§3.4).

### 9.6. `PostgresFixture` + `SqlProbeIntegrationTests` (Testcontainers)

`PostgresFixture` (IClassFixture; паттерн `EtcdContainerFixture`):
generic-контейнер `postgres:18`, env `POSTGRES_HOST_AUTH_METHOD=trust`
(паттерн стенда arch/04 §2.3), command `-c wal_level=logical` (слоты
нужны живыми), случайный host-порт; готовность — ретрай-подключение
Npgsql (30×1 c). Тесты (`SqlProbe` с `HostMap {"pg:5432" →
"127.0.0.1:<mapped>"}`; `ShardInfo` с `DsnHosts=["pg"]`):

- `SqlProbe_ReadsCatalogFromLivePostgres` — Arrange: схемы
  `bucket_0..bucket_15`, слот `pg_create_logical_replication_slot(
  't06_slot','pgoutput')`; Act: проба; Assert: `IsInRecovery == false`,
  `BucketSchemas` — 16 схем, слот в `Slots` (active, wal_status),
  `Standbies` пуст (реплик нет), `Error == null`; `ProbeResult.Ok`.
- `SqlProbe_GeneratesWal_SlotLagGrows` — вставка строк (WAL) → повторная
  проба: `LagBytes` слота > 0 (подтверждённого flush нет).
- `SqlProbe_WrongPassword_ErrorRuntime` — проба с неверным Password →
  `Runtime.Error` не пуст, списки пустые, `IsInRecovery == null`,
  `ProbeResult.Ok == false` (§3.7).
- `AlertRules_OnLiveRuntime` — снапшот с живым `Runtime` (без реплик,
  16/16 схем) через `AlertEngine` с правилами t06: есть
  `sync-standby-missing:demo/s1`, нет `inventory-mismatch`; после
  `DROP SCHEMA bucket_15` и повторной пробы — появляется
  `inventory-mismatch:demo/s1` (extra/missing по стороне diff).
- `AlertRules_SlotLagReproduced_LowThreshold` — после генерации WAL
  (лаг слота > 0) `AlertEngine` с `ReplicaLagBytes = 1` выдаёт
  `slot-lag-high:demo/s1/t06_slot` — воспроизведение лаг-алерта на
  живых данных (roadmap; порог занижен, чтобы не гнать 16 МБ WAL).

### 9.7. `InspectionEtcdApiTests` [правка — смоук]

`LiveEtcd_InspectionEndpoints_ReflectRealSnapshot` дополняется: `GET
/api/ha` → 2 скопа (`demo-s1`, `demo-s2`, оба matched, leader `s1a`/
`s2a`), `GET /api/ha/demo-s1` → members 2, timeline null (пробы в этом
тесте не wired — §3.1: обогащение только через стор). Прочие ассерты
без правок.

## 10. Unit-тесты (src/tests/AdminPanel.UnitTests/)

Время — `FixedTimeProvider` (unit) / управляемое время фабрики
(integration); пороговые правила — с `Options.Create(new AlertsOptions
{ … })` для проверки границ.

### 10.1. `HaAlertRulesTests` [новый]

- `ShardNoLeader_MatchedNoLeader_Critical` — matched-скоп без leader →
  Critical, target `demo-s1`, details cluster/shard; с leader → нет.
- `ShardNoLeader_UnmatchedNoLeader_Ignored` — unmatched без leader →
  нет алерта (§3.10).
- `HaMemberNotStreaming_ReplicaNotStreaming_Warning` — replica
  `starting` с успешной пробой → Warning, details role/state/expected;
  replica `streaming` → нет; master `running` → нет.
- `HaMemberNotStreaming_MasterNotRunning_Warning` — master `stopped` →
  Warning.
- `HaMemberNotStreaming_UnknownRole_Skipped` — роль `sync_standby` →
  нет (§3.13).
- `HaMemberNotStreaming_ProbeErrorOrMissing_Skipped` — ошибка пробы /
  проба не проводилась → нет (для ошибки — `probe-failed`).
- `ReplicaLagHigh_AboveThreshold_Warning` — lag 16 МБ + 1 байт при
  дефолте → Warning, details lagBytes/thresholdBytes; ровно порог → нет;
  custom `ReplicaLagBytes = 100` → 101 есть / 100 нет.
- `ReplicaLagHigh_MasterZeroLag_NoAlert`; `ReplicaLagHigh_NoProbe_Silent`.
- `SlotLagHigh_AboveThreshold_Warning` — слот с lag > порога →
  Warning, target `demo/s1/move_bucket_3` (§3.14).
- `SlotWalLost_LostSlot_Critical` — `wal_status='lost'` → Critical;
  `active`/`unreserved` → нет.
- `SlotInvalidationRisk_BelowThreshold_Warning` — safe_wal_size 1 GiB −
  1 → Warning; null safe_wal_size (нет max_slot_wal_keep_size) → нет;
  custom порог из Options.
- `SlotRules_RequireErrorFreeRuntime` — `Runtime.Error != null` или
  `Runtime == null` → все slot-правила молчат (пробы выключены/ошибка).
- `SyncStandbyMissing_MasterWithoutSync_Warning` — IsInRecovery false,
  standbies только `async` → Warning, details standbiesTotal;
  с `sync` или `quorum` → нет; IsInRecovery true → нет; runtime error → нет.
- `InventoryMismatch_MissingAndExtraSchemas_Warning` — routing ждёт
  `bucket_0..2` на s1, фактически `bucket_0,bucket_1,bucket_9` →
  Warning, details missing=`bucket_2`, extra=`bucket_9`.
- `InventoryMismatch_MovingBucketExcluded` — бакет в SYNCING на
  target-шарде не создаёт extra (§3.11); полное совпадение → нет.
- `ProbeFailed_EachFailedResult_Info` — 1 patroni + 1 sql failure →
  2 Info, id `probe-failed:patroni:demo-s1/s1a`,
  `probe-failed:sql:demo/s1`, details kind/error; ok-результаты → нет.
- `HaRules_FullEngine_Scenario` — сквозной снапшот «нет лидера +
  реплика не стримит + слот lost + нет sync-standby + проба падала»
  через полный `AlertEngine(AlertTestRules.All())`: детерминированная
  сортировка (Critical: shard-no-leader, slot-wal-lost → Warning:
  ha-member-not-streaming, sync-standby-missing → Info: probe-failed),
  `sinceUnix` переносится от previous.

### 10.2. `HaMappersTests` [новый]

- `MapSummaries_CountsAndFlags` — membersHealthy по state
  running/streaming; lagMaxBytes = max; unmatched: cluster/shard null,
  matched=false.
- `MapSummaries_EmptyLag_NullLagMaxBytes`.
- `MapDetails_FullTransfer` — все поля модели → DTO, включая
  probeError/probeAtUtc, rawConfig, optimeLeader.
- Хендлеры напрямую: `HaScopesQueryHandler` — 503 без снапшота /
  сводки со снапшотом; `HaScopeDetailsQueryHandler` — 503; 404
  (`ScopeNotFoundException`) для неизвестного; 200 для существующего.

### 10.3. `PatroniClusterParserTests` [новый]

Фикстура `ProbesFixtures/patroni-cluster.json` — реальный фрагмент
Patroni (pg-report §4: `{"name":"pg2","host":"10.0.0.12","port":5432,
"role":"replica","state":"streaming","timeline":1,"lag":0}` + мастер;
формат ответа: `{"members":[…]}`):

- `Parse_FullFixture_AllMembers` — 2+ записи, поля на месте.
- `Parse_Tolerant_MissingFieldsAndNulls` — без timeline, lag null,
  строковые числа (AllowReadingFromString) — толерантно (arch/02 §8).
- `Parse_BrokenJson_Throws` — мусор → JsonException (ловит проба).

### 10.4. `HostMapResolverTests` [новый]

- `Resolve_ExactMatch_Overrides` — `"s1a:8008"` → `"127.0.0.1:8011"`.
- `Resolve_NoMatch_Identity` — `"s1a:8008"`.
- `Resolve_EmptyMap_Identity`; `Resolve_DifferentPort_NotMatched` —
  карта знает `s1a:5432`, спрашивают `s1a:8008` → identity (порт —
  часть ключа).

### 10.5. `SqlConnectionFactoryTests` [новый]

Построение строки (чистая часть `SqlProbe`, отдельная внутренняя
функция/статик для тестируемости без сервера):

- `Build_MapsHostsPerEndpoint` — DsnHosts `[s1a,s1b]`, Port 5432,
  HostMap `s1a:5432 → 127.0.0.1:5433` → Host = `127.0.0.1:5433,s1b:5432`
  (явный порт у каждого эндпоинта, §3.6).
- `Build_MergesPassword` — Password задан → в строке; пуст → ключа нет.
- `Build_ReadOnlyAndSessionAttributes` — `TargetSessionAttributes=
  ReadWrite`, `Options=-c default_transaction_read_only=on`,
  `Application Name=adminpanel`, Timeout/CommandTimeout из
  TimeoutSeconds (§3.6).
- `Build_NullUserAndDb_Omitted` — user/dbname отсутствуют → ключей нет.

### 10.6. `ProbeEnricherTests` [новый]

- `Apply_NullState_NoChange` — снапшот без изменений, Probes пуст.
- `Apply_EnrichesMembers` — успех: role/state/timeline/lag перекрыты,
  probeAtUtc поставлен, probeError null; ошибка: DCS role/state
  остались, timeline/lag null, probeError есть (§3.5).
- `Apply_SetsRuntimeAndProbes` — runtime по ключу кластер/шард;
  Probes = список состояния.
- `Apply_StaleTargetsIgnored` — состояние ссылается на исчезнувший
  скоп/шард — снапшот валиден, лишние ключи не падают (§4.2).
- `Apply_EnabledFlagsImplicit` — в состоянии только sql-части →
  members не тронуты, runtime стоит (аналог для patroni).

### 10.7. `ProbeOrchestratorTests` [новый]

Оркестратор с фейковыми `IPatroniRestProbe`/`ISqlProbe` (записывают
вызовы) и `SettableProbeStateStore`:

- `RunOnce_BuildsTargetsFromSnapshot` — matched-скоп + шард →
  вызваны все члены/шард; unmatched-скоп и шард без dsn — нет (§3.3).
- `RunOnce_ParallelBothKinds_WritesState` — состояние содержит members
  + runtimes + probes, одна замена.
- `RunOnce_DisabledKind_Skipped` — `PatroniEnabled=false` → patroni
  не вызван, Members пуст (аналог sql).
- `RunOnce_BothDisabled_Noop` — ничего не вызывается, состояние пусто.
- `RunOnce_NoSnapshot_EmptyState`.
- `RunOnce_ProbeThrows_CapturedAsFailedResult` — фейк бросает →
  ProbeResult(ok:false), тик не падает (§3.15).

### 10.8. Правки `SnapshotRefresherTests`

- Конструктор харнесса: refresher получает стор проб (пустой по
  умолчанию) — существующие кейсы без правок ассертов.
- `Refresh_EnrichesFromProbeState` — префилл → обогащённый снапшот
  (members/runtime/Probes) — зеркалит integration-кейс на unit-фикстурах.
- `Refresh_FailTick_PreservesProbes` — протухший probe-в-снапшоте не
  теряется на отказном тике (§4.3).

## 11. Ограничения (что НЕ делается)

- Аутентификация Patroni REST (Bearer-токен) не поддерживается:
  стенд-эмуляторы и текущий прод `../pg` отдают `/cluster` открыто;
  при появлении токена — отдельная roadmap-задача (arch/ не оговаривает).
- Per-кластерные SQL-пароли — нет: контракт `AdminPanel:Probes:Password`
  один (arch/01 §6).
- История проб/алертов, метрики, графики — нет (arch/01 §9); срез
  `Probes[]` — только последний тик.
- Patroni-эндпоинты кроме `GET /cluster` (`/patroni`, `/config`,
  switchover) — не используются; панель не мутирует Patroni (arch/01 §9).
- Свои healthz-чеки проб — нет: отказы проб видны через
  `probe-failed` и UI-поля (arch/01 §8; YAGNI).
- Watch/push, live-переподключения — нет (arch/02 §5).
- HA-сводка в `GET /api/overview` — нет (§3.19); фронтенд HA-панелей —
  t09; dev-стенд + e2e (включая живые эмуляторы `hc*`) — t10;
  «Стендовая топология» — уже есть в модели (t03), не трогается.
- Ручной DSN-парсинг в Probes — нет (§3.6); тексты SQL 03 §5 —
  дословно (инвариант документа).
- Мутации `arch/01–04` запрещены; roadmap — только удаление пункта
  t06 (§14).

## 12. Пакеты

- `Directory.Packages.props`: `+ <PackageVersion Include="Npgsql"
  Version="10.0.3" />` — версия референса `../Puzzle`
  (`src/Directory.Packages.props`, строка Npgsql; CPM-правило arch/01 §2).
- `AdminPanel.Probes.csproj`: `+ PackageReference Npgsql`,
  `Microsoft.Extensions.Hosting.Abstractions` (BackgroundService),
  `Microsoft.Extensions.Http` (AddHttpClient) — версии уже в CPM
  (10.0.9); `Microsoft.Extensions.Options` — транзитивно
  (через Hosting.Abstractions/Core).
- Тестовые проекты: без новых PackageReference — Npgsql доступен
  транзитивно (UnitTests/IntegrationTests → Api → Probes); FluentAssertions/
  Testcontainers уже есть. Хостинг-зависимости Probes не тянут лишнего:
  Hosting.Abstractions/Http — лёгкие абстракции (те же, что у Etcd).

## 13. Настройки тестовых проектов

- `AdminPanel.UnitTests.csproj`: `+ <None Include="ProbesFixtures\**\*.json"
  CopyToOutputDirectory="PreserveNewest"/>` (паттерн EtcdFixtures);
  загрузчик фикстуры — чтение файла в тесте (без общего хелпера:
  формат простой текст/JSON, не kvs).
- `AdminPanel.IntegrationTests.csproj`: без правок.

## 14. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t06-ha-api` из
`arch/roadmap/ha.md` (файл остаётся с шапкой трека; других пунктов
нет — удаляется только пункт списка; прецедент t05 §14). Зависимости
`← t06-ha-api` (`arch/roadmap/stand.md` t10, `arch/roadmap/frontend.md`
t09) НЕ трогаются — их чистит задача-владелец. Правка выполняется в
ветке задачи до мержа.

## 15. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings
   (`TreatWarningsAsErrors=true` не подавлен).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные (Docker:
   Testcontainers etcd + postgres:18; unit — без Docker).
3. Unit: 9 правил (пороги/границы, отключённость проб, matched-only,
   error-члены, null safe_wal_size, moving-exclusion), парсер
   Patroni-JSON (толерантность), HostMap-резолвер, построитель
   SQL-строки (маппинг/пароль/read-only), ProbeEnricher,
   оркестратор на фейках, мапперы/хендлеры HA — покрыты (§10).
4. Integration: 401/503/200/404 HA-эндпоинтов; Patroni-проба против
   HTTP-стаба (HostMap e2e, ошибки); SQL-проба против живого
   postgres:18 (слоты/лаги/инвентарь/ошибки, алерты на живом
   runtime); живой путь etcd → refresher(+пробы) → AlertEngine →
   `/api/ha*`, `/api/clusters/{c}` (runtime), `/api/alerts` (§9).
5. `AlertEngine`/`IAlertEngine`/`AlertContext`/`SnapshotBuilder`/
   `Program.cs`/фабрика `"api"` не изменялись (diff пуст) — правило
   t04 §3.2 выполнено.
6. Отключаемость: `PatroniEnabled=false`/`SqlEnabled=false` →
   соответствующие поля снапшота null/пусты, HA-алерты проб молчат
   (unit §10.1/§10.7) — требование 03 §4 «SQL-алерты — только при
   включённых пробах» соблюдено; `shard-no-leader` живёт всегда.
7. Панель по-прежнему не пишет в etcd и PG: SQL — только SELECT
   каталога 03 §5 + `default_transaction_read_only=on`; `kv/put` —
   только тесты.
8. `grep PackageReference` по csproj: добавлен только Npgsql в
   AdminPanel.Probes (+ Host/Http к Probes, §12); CPM += Npgsql 10.0.3.
9. Пункт `t06-ha-api` отсутствует в `arch/roadmap/ha.md`; `← t06-ha-api`
   в stand.md/frontend.md сохранён; других мутаций `arch/` нет.
10. Все решения §3 не противоречат arch/01 §1/§6/§8, arch/02 §2.2/
    §3/§4/§6/§7, arch/03 §1/§2/§4/§5 (проверка на ревью).

## 16. Риски и заметки

- **Свежесть проб отстаёт от KV на ≤ 1 тик refresher'а** (§3.1) — цена
  схемы «единственный писатель снапшота» (arch/01 §1); при интервале
  проб 15 c и тике KV 3 c задержка несущественна.
- **Ломка ассертов существующих тестов не планируется**: пустой стор
  проб по умолчанию в харнессах сохраняет текущие списки алертов;
  правки `EtcdTestHarness`/`SnapshotRefresherTests` — только конструктор
  и новые кейсы (отличие от t05 §3.15, где сид сам рождал алерты).
- **`RemoveAll<IHostedService>` гасит и оркестратор** в фабрике `"api"`
  — осознанно (§3.16): управляемые снапшоты важнее живых тиков в
  HTTP-тестах; живые проб-тесты — прямая конструкция.
- **postgres:18 vs версии стенда** — тот же образ, что arch/04 §2.3 и
  `../pg` (pg-report §6); wal_level=logical нужен только ради живых
  слотов в тесте (prod-бакетный слой требует его и так).
- **Тест repro лага > 16 МБ** не разыгрывается в integration (долго
  гнать WAL): пороги покрыты unit-границами (§10.1), integration
  воспроизводит лаг-алерт на живом слоте с заниженным порогом
  `ReplicaLagBytes = 1` (§9.6).
- **`HttpListener`-стаб** — кроссплатформенный managed-сервер без
  зависимостей; альтернатива (встроенный Kestrel в тесте) отвергнута —
  тяжелее, а нужна одна rote-отдача JSON.
- **Один Password на всё** — осознанное следствие arch/01 §6; при
  мульти-кластерных секретах — roadmap-задача (не текущая).
- **`User-Agent: AdminPanel`** вместо несуществующего для HTTP «тега
  Application Name» (§3.22) — идентификация панели в логах эмуляторов/
  Patroni; SQL-строка несёт настоящий `Application Name=adminpanel`.
- **Проб-ошибка не гасит etcd-данные** (arch/02 §6 «ошибка пробы не
  роняет данные из etcd») — гарантировано структурно: enrichment
  аддитивный, отказный путь сохраняет прежние кластеры/скопы (t03 §3.9).
- **Конфликт severities `probe-failed`** (info каталога vs warning
  обзора arch/01 §8) разрешён каталогом (§3.9) — зафиксировано здесь,
  правка arch/ не требуется (архивный каталог — канон).
