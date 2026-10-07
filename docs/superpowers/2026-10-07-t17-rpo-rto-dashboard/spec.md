# t17-rpo-rto-dashboard — spec (Фаза 1, dev-flow)

RPO/RTO-числа оператору: не только пороговые алерты («полный просрочен»/
«цепочка рвётся»), а сами значения — возраст последнего валидного полного,
лаг WAL, сводный RPO-потенциал «потеряем ≈ N», фактическая длительность
последнего failover/rebuild/drill/restore — per-shard, в AdminPanel, плюс
длительности failover/rebuild Prometheus-сериями. Roadmap:
`arch/roadmap/reliability.md`, трек P3/N. Зависимость `t14-backup-metrics-export`
смержена (словарь arch/18 §2.7 — канон для новых серий).

## 1. Цель и контекст

### 1.1. Проблема

Надёжностные характеристики (D/R из `arch/roadmap/reliability-report.md`)
видны оператору только как бинаризированные пороговые алерты панели
(`backup-full-stale`, `wal-stream-lag`, …). Сами числа нигде не показываются:

- **RPO-данные уже в снапшоте панели** (`/pgworker/backups/*`, arch/19 §4):
  `ClusterBackupsInfo` несёт `ShardLastCompletedUnix` (парсер уже фильтрует
  verify=FAILED), `WalStreamInfo` (State/LastUploadedUnix/LagSegments),
  `DrillInfo`, `RestoreOperationInfo`, `Policy.FullMaxAgeSec` — но нигде не
  вычисляются «возраст валидного полного» / «лаг WAL» / «что потеряем сейчас»
  одним числом.
- **RTO-факты (failover/rebuild) не фиксируются вовсе**: надзор
  (NodeSupervisor) держит in-memory трек недоступности и персистит его полем
  `unreachable` в `/pgworker/work/<C>`, но моменты «лидер ожил сменой» /
  «нода пересоздана и работает» теряются — фактические длительности
  восстановления известны только из E2E-прогонов, не из эксплуатации.
- Длительности drill/restore вычислимы из существующих ключей, но числом
  оператору не показываются.

### 1.2. Решение (кратко)

1. **БЕЗ новых ключей**: work-ключ `/pgworker/work/<C>` расширяется
   аддитивными полями `last_failover` / `last_rebuild` — последний факт
   каждого вида `{shard, node, cause, detected_unix, resolved_unix?,
   duration_sec?}` (§3.1). Пишет `NodeSupervisor` в существующих точках
   переходов надзора; открытое событие переживает takeover (ключ в etcd).
   Истории/кольца НЕТ — только последний факт каждого вида.
2. **Панель — вычислитель чисел**: чистая функция `ReliabilityCalculator`
   над уже существующим снапшотом (RPO-факты уже там) + расширенный парсер
   work-ключа (§3.4): возраст валидного полного + id, лаг WAL, сводный
   RPO-потенциал; последние failover/rebuild/drill/restore с длительностями
   и ongoing-бейджами.
3. **Грань «Надёжность»**: read-only страница `/reliability` (таблица
   кластер×шард: RPO-блок + RTO-блок) + сводная карточка на Overview
   (клиентская агрегация); API `GET /api/reliability` (§3.5).
4. **Prometheus**: длительности последнего failover/rebuild — сериями
   `pgworker_ha_failover_duration_seconds` / `pgworker_ha_rebuild_duration_seconds`
   (расширение словаря arch/18 §2.7, паттерн t14; §3.3). RPO-числа в
   Prometheus НЕ экспортируются — уже покрыты сериями t14
   (`pgworker_backup_full_age_seconds`, `pgworker_backup_wal_*`).

### 1.3. Охват и границы

**Входит**: PG-шарды (`/clusters/<C>`) — RPO/RTO-числа в панели, RTO-факты
в work-ключе, две Prometheus-серии + панель Grafana.

**НЕ входит (осознанно)**:

- **Kafka/Valkey** — RPO Kafka держится RF×minISR по зафиксированному решению
  2026-09-28 (arch/16 §6), числовых бэкап-RPO там нет; Valkey — кеш by
  design. Домены в грань не входят. WorkJournal — общий класс Pg/Kfw
  (Shared.Etcd), но новые поля PgWorker-доменные: Kfw-журналы их не пишут
  (null опускается сериализацией).
- **Новые etcd-ключи** — отменено пользователем: вариант
  `/pgworker/ha-events` отклонён; всё живёт в существующем work-ключе.
  Следствие: чистки Deprovisioning D2 / remove-shard (S-процессы) НЕ
  меняются — work-ключ удаляется целиком, как сейчас.
- **История HA-событий/кольцо** — нет (следствие решения п.1): хранится
  последний факт каждого вида; накопительная история — Prometheus-канон
  (серии t17 + counters t14).
- **Новые алерт-kinds** — не заводятся: пороговые правила уже есть, грань
  показывает ЧИСЛА с индикацией по существующим порогам. Новая
  Prometheus-группа алертов для RTO-серий не создаётся (флапливо на
  фиксированном пороге длительности; числа — для оператора и дашборда).
- **Мутации** — грань строго read-only (панель — немой наблюдатель).
- **RPO в Prometheus** — серии t14 уже покрывают; дублирование не вводится.

## 2. Принципы

- **arch-first**: правки `arch/14` §3.3/§5 C, `arch/18` §2.7,
  `arch/adminpanel/02` §2.3.1/§3, `arch/adminpanel/03` — ДО кода, теми же
  коммитами, что и соответствующий код (§4, фаза 1).
- **Надзор — единственный писатель** фактов в `/pgworker/work/<C>` под
  клэймом `<C>` (как и весь work-ключ); панель — только читатель.
  RMW-гонок нет (один писатель — держатель клэйма).
- **Наблюдатель не влияет на надзор**: запись фактов — строго после основных
  переходов надзора; сбой записи — warning-лог, тик надзора не роняет
  (потеря наблюдаемости ≠ потеря данных). Пороги/бюджеты надзора
  (`NodeDeadSec`, ускорение failover, rebuild-механика) НЕ меняются.
- **Факт — состояние в etcd, не в памяти**: открытое событие (без
  `resolved_unix`) живёт в ключе; takeover-инстанс продолжает его от
  сохранённого `detected_unix`. `detected_unix` уже фиксируется существующим
  треком недоступности (поле `unreachable`): первый тик недоступности —
  источник детекции; НОВОГО трекинг-механизма не заводим.
- **Факты переживают чужие записи**: фазовые записи процессов
  (`WritePhaseAsync`) обязаны переносить `last_failover`/`last_rebuild`,
  как уже переносят `unreachable` (урок t09 — иначе каждый тик
  provisioning/moves стирает факты).
- **Обратная совместимость схемы**: старые work-ключи (без новых полей)
  читаются панелью и воркером как «фактов нет»; новые поля — nullable,
  сериализация опускает null (канон `WorkState`).
- **Числа — чистые функции над снапшотом**: вычислитель RPO/RTO —
  pure-функция в `AdminPanel.Core` (юнит-покрытие без etcd), по образцу
  `AlertEngine`/`BackupFullStaleRule`.
- **Толерантный читатель**: панель молчит при отсутствии полей/ключей
  (подсистема надзора старой версии, шарды без событий) и при битых JSON
  (parseError-паттерн существующих парсеров).
- **Панель ВСЕГДА в докере** (AGENTS.md); новых сервисов/портов/джоб
  Prometheus нет — грань на существующем снапшот-тике, серии — в
  существующий `/metrics` PgWorker.

## 3. Структура и компоненты

### 3.1. Контракт etcd: расширение `/pgworker/work/<C>` (arch/14 §3.3 — правка первой фазой)

К существующей схеме work-ключа аддитивно два поля (null опускается):

```
"last_failover": {"shard":"<X>","node":"<n>","cause":"accelerated"|"elections",
                  "detected_unix":<unix>,"resolved_unix":<unix>?,"duration_sec":<n>?}
"last_rebuild":  {"shard":"<X>","node":"<n>","cause":"auto-dead"|"operator-recreate",
                  "detected_unix":<unix>,"resolved_unix":<unix>?,"duration_sec":<n>?}
```

Семантика (длительность — от ПЕРВОГО тика недоступности до
работоспособности; окно детекции входит — это честный RTO «от недоступности
до работоспособности»):

- **`kind=failover`** — недоступность ЛИДЕРА HA-scope, завершившаяся сменой
  лидера:
  - `detected_unix` — первый тик недоступности лидера (существующий трек
    `unreachable`, ключ `"<X>/<n>"`); takeover продолжает от сохранённого
    значения;
  - `cause` ∈ `accelerated` (надзор применил ускорение: failover-маркер +
    удаление leader-ключа — `AccelerateDeadLeaderFailoverAsync`) |
    `elections` (промоушен ждёт Patroni: нет
    `SupportsRunningInspection`/живого кандидата — ускорение не применено);
  - `resolved_unix` — первый тик, где `/service/<C>-<X>/leader` указывает на
    ДРУГУЮ ноду; `duration_sec = resolved_unix − detected_unix`;
  - транзиентный флап (лидер ожил, лидерство сохранил) — факта failover НЕ
    БЫЛО: открытая запись удаляется без фиксации;
  - graceful switchover ЖИВОГО лидера (soft-path RecreateMarkedNodes) — не
    событие (недоступности нет).
- **`kind=rebuild`** — пересоздание ноды:
  - `cause` ∈ `auto-dead` (надзор: недоступна дольше `NodeDeadSec` при живом
    кворуме — rebuild-ветка `SuperviseShardAsync`) | `operator-recreate`
    (маркер TO_RECREATE — `RecreateMarkedNodesAsync`);
  - `detected_unix` — первый тик недоступности из того же трека (для
    operator-recreate ЖИВОЙ ноды трека нет — момент начала исполнения
    маркера, пересоздание с нуля без окна детекции);
  - `resolved_unix` — первый тик, где пересозданная нода жива пробой
    (переход state → RUNNING в alive-ветке надзора);
    `duration_sec = resolved_unix − detected_unix` (включает окно детекции
    для `auto-dead`).
- **Открытое событие** (`resolved_unix`/`duration_sec` отсутствуют) —
  идёт сейчас: панель показывает ongoing-бейдж с тикающей длительностью
  `now − detected_unix`. Воркер растущий duration НЕ пишет.
- **Хранится ПОСЛЕДНИЙ факт каждого вида**: новое событие того же вида
  перезаписывает поле (без истории). Одновременные failover+rebuild на
  разных шардах — каждое поле независимо; на ОДНОМ шарде возможны оба факта
  (умерший лидер: failover — смена лидера, rebuild — пересоздание ноды).
- **Запись**: `NodeSupervisor` — put всего ключа в существующих тиках надзора
  (`WriteSupervisionAsync` — расширенная сигнатура, §3.2); открытие факта —
  в тике детекции/исполнения, закрытие — в тике разрешения. Факты
  переносятся фазовыми записями (`WritePhaseAsync` — carry-forward,
  принцип §2).
- **Гварды (границы надзора — как у самого надзора, новых доменов не
  появляется)**: усыновлённые `object`-ноды не rebuild'ятся — фактов не
  порождают (только существующий journal); QUARANTINED/REMOVING — вне проб,
  фактов нет; шард без dsn — домен AddShardProcess, надзор его не пишет;
  TO_REMOVE — демонтируемое, фактов нет.
- **Ушедший шард**: факт последнего события может пережить удаление шарда из
  декларации (ключ кластерный) — панель отображает факты ТОЛЬКО для шардов
  живой декларации; надзор не чистит историю осознанно (последний факт,
  не журнал).
- **Эвакуация** (весь шард мёртв): открытый failover-факт остаётся открытым
  до смены лидера или deprovision (удаления ключа D2) — эвакуация видна
  своей гранью, осознанное ограничение.

### 3.2. PgWorker: фиксация фактов (`NodeSupervisor` + `WorkJournal`)

**`WorkJournal`** (`src/Shared.Etcd/Coordination/WorkJournal.cs`):

- `WorkState` расширяется полями `LastFailover`/`LastRebuild` (nullable-
  record `HaSupervisionFact(Shard, Node, Cause, DetectedUnix, ResolvedUnix?,
  DurationSec?)`; сериализация snake_case-именами, null опускается).
- `WritePhaseAsync`: carry-forward расширяется — фазовая запись без явных
  фактов сохраняет существующие из ключа (как `unreachable` сегодня).
- `WriteSupervisionAsync`: сигнатура расширяется факторами (обязательные,
  как `unreachable` — надзор единственный писатель этих полей).
- `ReadUnreachableAsync` → `ReadSupervisionStateAsync` (или парный
  `ReadFactsAsync`): чтение трека + фактов одним чтением (сейчас два чтения
  ключа не нужно — один `ReadAsync`).
- KafkaWorker: общий класс, поля nullable — kfw-журналы меняются только
  переносом (там фактов нет, сериализация опускает).

**`NodeSupervisor`** (`src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`)
— точки встраивания (код уже содержит все факты):

- чтение в начале `TickAsync` (тик надзора): трек `unreachable` + факты
  `last_failover`/`last_rebuild` из ключа (takeover-продолжение);
- **открытие failover**: dead-ветка `SuperviseShardAsync`, `isLeader` —
  после успешного ускорения `AccelerateDeadLeaderFailoverAsync` →
  `cause=accelerated`; та же ветка без применения ускорения (guard'ы
  `SupportsRunningInspection`/кандидат) → `cause=elections`;
  `detected_unix` — `track["<X>/<n>"]` (уже записан `track.TryAdd`);
- **закрытие failover**: тик, где спарсенный `leader` scope ≠ node открытого
  факта (лидер сменился) → resolved+duration; флап-оживание (нода жива,
  лидер тот же) → открытая запись удаляется без фиксации;
- **открытие rebuild**: rebuild-ветка `SuperviseShardAsync` (`auto-dead`,
  detected из трека) и исполнение маркера в `RecreateMarkedNodesAsync`
  (`operator-recreate`, detected из трека либо момент исполнения);
- **закрытие rebuild**: alive-ветка (переход state → RUNNING) для ноды
  открытого rebuild-факта;
- запись фактов — в финальном `WriteSupervisionAsync` тика (после всех
  переходов — принцип §2); ошибка — warning-лог через существующий
  `lastError`-механизм тика, тик не роняет;
- чистые переходы фактов (открытие/закрытие/сброс/флап) — отдельный
  маленький чистый класс `HaFactState` (Provisioning; без etcd) —
  юнит-тестируемость без фикстур; NodeSupervisor делегирует ему решения
  «открыть/закрыть/сбросить».

**Механики надзора не меняются**: никаких новых проб, порогов, условий;
вставка — наблюдатель рядом с переходами.

### 3.3. Prometheus: RTO-серии (arch/18 §2.7 — расширение словаря)

Две серии в словаре §2.7 (заголовок секции расширяется: «Бэкап-домен и
RTO-длительности PgWorker»):

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `pgworker_ha_failover_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО failover (resolved − detected, §3.1); открытого события/факта нет — серия не эмитится |
| `pgworker_ha_rebuild_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО rebuild; факта нет — серия не эмитится |

- Пишутся тиком надзора: единый марк-метод `WorkerMetricsInstrumentation`
  (`src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`) по образцу
  `BackupFullAge` — набор шардов тика перезатирает стейт кластера ЦЕЛИКОМ
  (ушедшие шарды серии не копят; шард без факта — серия шарда исчезает).
  Источник — факты WorkState, которые надзор только что записал.
  Наблюдатель пассивный (try/catch, lock — канон файла), подключение —
  ctor-параметр NodeSupervisor в `Program.cs` (как `fullAgeObserver`
  BackupProcess'а).
- RPO-числа в Prometheus НЕ экспортируются (серии t14 покрывают); новых
  алертов НЕ заводим (§1.3).
- Grafana: панель «RTO-длительности» в существующий
  `metrics/grafana/dashboards/backups.json` (failover/rebuild рядом с
  drill/restore-длительностями — тема восстановимости).

### 3.4. Панель: модель, парсер, вычислитель (`AdminPanel.Core` / `AdminPanel.Etcd`)

- **Парсер** (`WorkJournalParser` — расширяется): `last_failover`/
  `last_rebuild` → `HaSupervisionInfo(Shard, Node, Cause, DetectedUnix,
  ResolvedUnix?, DurationSec?)` (панельный дубль воркерной модели —
  осознанный, канон бэкапных моделей); `WorkJournalInfo` расширяется двумя
  nullable-полями. Толерантность: полей нет — null (старые ключи читаются);
  битый JSON — parseError (существующий паттерн); незнакомые поля — игнор.
  Панель work-ключ УЖЕ читает (adminpanel/02 §2.3.1, `PgWorkerWork` в
  снапшоте) — новый префикс/чтение не заводятся.
- **Вычислитель `ReliabilityCalculator`** (чистая функция, Core; по образцу
  `AlertEngine`-правил — вход `EtcdSnapshot`, выход per-cluster/per-shard):
  - **RPO-блок**:
    - `fullAgeSec` — `now − ShardLastCompletedUnix` (валидный = COMPLETED
      verify≠FAILED — фильтр уже в парсере, та же семантика свежести, что у
      планировщика arch/19 §2 и `backup-full-stale`); валидного нет → null
      («никогда»); шарда нет в словаре → mode=`off` (подсистема не включена,
      секция молчит);
    - `fullId` — id этого полного (из `ShardsFulls`: max `FinishedUnix`
      среди COMPLETED verify≠FAILED; клик — переход к деталям грани бэкапов);
    - `walLagSegments` — `LagSegments`; `walAgeSec` — `now −
      LastUploadedUnix`;
    - `rpoPotentialSec` + `mode` — сводное «потеряем при полной гибели
      шарда сейчас»: wal `ACTIVE|DEGRADED` → `walAgeSec`, mode=`wal`;
      `BROKEN|STOPPED`/wal-ключа нет → `fullAgeSec`, mode=`full`; префикс
      бэкапов пуст → mode=`off`. Индикация — по существующим порогам
      (`thresholdFullAgeSec` = `Policy.FullMaxAgeSec ?? 86400` — дефолт
      `BackupFullStaleRule`).
  - **RTO-блок** (последний факт каждого вида):
    - `lastFailover {shard, node, cause, detectedUnix, resolvedUnix?,
      durationSec?, ongoing}` / `lastRebuild {…}` — из `PgWorkerWork`
      (кластер → факты; отображение только для шардов живой декларации);
      ongoing = resolved отсутствует → длительность `now − detectedUnix`
      (пересчитывается каждым тиком снапшота — «тикающая» на глазах);
    - `lastDrill {state, durationSec|ongoing, finishedUnix, error?}` — из
      `DrillInfo` (`FinishedUnix − StartedUnix`; RUNNING → ongoing от
      `StartedUnix`);
    - `lastRestore {state, durationSec|ongoing, finishedUnix}` — из
      `RestoreOperationInfo` (`FinishedUnix − RequestedUnix` — от ЗАЯВКИ,
      включая очередь: честный операторский RTO; активная → ongoing от
      `RequestedUnix`).

### 3.5. API (arch/adminpanel/03 — правка)

- **`GET /api/reliability`** (read-only, cookie-auth; по образцу `HaQuery`):
  `ReliabilityDto {clusters:[{cluster, shards:[{shard, declared, rpo{
  mode, fullAgeSec?, fullId?, walLagSegments?, walAgeSec?,
  rpoPotentialSec?, thresholdFullAgeSec}, rto{lastFailover?, lastRebuild?,
  lastDrill?, lastRestore?}}]}]}`. Всегда 200: пустые секции = подсистема
  выключена / надзор старой версии (толерантность §2); 503 — только при
  отсутствии снапшота (существующий `SnapshotNotReadyException`-паттерн).
  Хендлер — `ISnapshotStore` → `ReliabilityCalculator` → DTO-маппер
  (чистые функции, файл `src/AdminPanel.Api/Inspection/ReliabilityQuery.cs`).

### 3.6. UI (React+Mantine, по образцу `BackupsStoragePage`)

- **Страница «Надёжность»** (`/reliability`, роут+навигация между HA и
  Алертами): таблица кластер×шард — RPO-колонки (возраст валидного полного
  с цветовой индикацией по `thresholdFullAgeSec` + id, лаг WAL сегм.,
  возраст WAL-хвоста, «потеряем ≈ N» с индикацией mode) + RTO-колонки
  (последний failover: длительность/когда/cause/node; последний rebuild:
  аналогично; drill: итог+длительность; restore: итог+длительность);
  ongoing-значения — бейдж «идёт» с тикающей длительностью; форм ввода нет;
  polling — общий переключатель (2/5/15/off).
- **Overview-карточка «Надёжность»**: worst `rpoPotentialSec` по Active-
  кластерам + число шардов mode=`full` («RPO держится только полным») и
  mode=`off` при включённой подсистеме; длительность последнего failover
  установки. Клиентская агрегация `GET /api/reliability` (по образцу
  HA/Backups-агрегаций — `OverviewDto` не расширяется).

## 4. Фазы (для plan-фазы)

1. **arch-правки** (ДО кода, частью работы задачи):
   - `arch/14` §3.3 — таблица work-ключа: аддитивные поля `last_failover`/
     `last_rebuild`, семантика, перенос фазовыми записями;
   - `arch/14` §5 C — фиксация фактов надзором: точки, cause-словарь,
     detected/resolved-семантика, гварды границ;
   - `arch/18` §2.7 — две RTO-серии (расширение заголовка секции и таблицы);
   - `arch/adminpanel/02` §2.3.1 — строка `/pgworker/work/<C>`: новые поля
     читаются панелью; §3 — `WorkJournalInfo`-расширение;
   - `arch/adminpanel/03` — §1 эндпоинт `GET /api/reliability`; §3 страница
     «Надёжность», карточка Overview, пункт навигации.
2. **Воркер**: `WorkState`/`WritePhaseAsync`-carry-forward/
   `WriteSupervisionAsync` (Shared.Etcd); `HaFactState` + встройка в
   `NodeSupervisor` (Provisioning); юниты (открытие/закрытие обоих видов,
   cause-различение, флап-без-фиксации, takeover по открытому факту,
   carry-forward, усыновлённые/карантин/TO_REMOVE не пишутся).
3. **Метрики**: марк-метод + ObservableGauge-серии в
   `WorkerMetricsInstrumentation`; подключение в `Program.cs`; канон-тест
   словаря (имена серий в `/metrics`) + юнит стейта (перезапись набора,
   null→исчезновение); панель Grafana в `backups.json`.
4. **Панель Core/Etcd**: `HaSupervisionInfo` + парсер +
   `ReliabilityCalculator`; юниты (валидный полный c verify-фильтром,
   mode=wal/full/off, BROKEN→full, ongoing-факты, битые JSON, старые ключи
   без полей).
5. **Панель Api**: `ReliabilityQuery` + DTO-мапперы; интеграционные тесты
   refresher'а (ключи в реальном etcd → снапшот → `/api/reliability`-смоук).
6. **frontend**: api-типы, страница, карточка Overview, роут/навигация.
7. **Приёмка** (§6) + практики: при необходимости — строка в
   `docs/adminpanel/02-etcd-snapshot.md` по правилам INDEX.

## 5. Ограничения и риски

| # | Риск/ограничение | Митигация |
|---|---|---|
| R1 | Фазовая запись процесса стирает факты (каждый тик — put ключа) | carry-forward в `WritePhaseAsync` (как `unreachable`, урок t09); юнит-тест на перенос |
| R2 | Воркер упал в окне события | открытое событие персистится в ключе; takeover продолжает от `detected_unix` |
| R3 | Транзиентный флап лидера → ложные failover-факты | фиксация ТОЛЬКО по факту смены leader; флап-оживание — запись удаляется без фиксации; юнит |
| R4 | Смена лидера в окне рестарта воркера без открытого факта | событие не изобретается (честное «нет данных»); открытие — только от живого трека |
| R5 | Тик надзора утяжелён | записи фактов — в уже существующем финальном put тика (`WriteSupervisionAsync`); НЕТ дополнительных etcd-операций |
| R6 | 3-с тик панели: длительности с точностью тика | длительности фактов фиксирует ВОРКЕР (detected/resolved из тиков надзора), панель только отображает; ongoing тикает на глазах — осознанно |
| R7 | Shared.Etcd — общий с Kfw класс | поля nullable, kfw не пишет их (null опускается); kfw-тесты не меняются; канон `unreachable`-переноса уже общий |
| R8 | E2E-бюджеты: тронуты PgWorker.Provisioning/Shared.Etcd | механики надзора не меняются; мерж-гейт — docker-E2E кейс-маркер Scale_AddEmptyShard на свежем Release (AGENTS.md); полная серия — по решению ревью |
| R9 | Кардинальность серий | по одной серии на (cluster, shard), перезапись набора тика — ушедшие шарды не копят |
| R10 | Факт переживает удаление шарда из декларации | панель фильтрует по живой декларации (кластер×шард таблицы); ключ кластерный, чистки D2 достаточно |

## 6. Критерии приёмки

1. Сборка Release 0 warnings; юниты и интеграции затронутых проектов
   (PgWorker, AdminPanel, Shared) зелёные — серии по отдельности, зачистка
   контейнеров/сетей между сериями (AGENTS.md).
2. Воркер: юнит-покрытие фактов — открытие/закрытие failover
   (accelerated/elections) и rebuild (auto-dead/operator-recreate),
   detected из трека недоступности, флап-оживание без фиксации, takeover
   по открытому факту, carry-forward `WritePhaseAsync`, усыновлённые/
   QUARANTINED/REMOVING/TO_REMOVE не пишутся.
3. Метрики: `/metrics` PgWorker содержит
   `pgworker_ha_failover_duration_seconds{cluster,shard}` и
   `pgworker_ha_rebuild_duration_seconds{cluster,shard}` (канон-тест
   словаря по образцу t14); юнит стейта: перезапись набора кластера,
   null-факт → серия исчезает.
4. Панель: юниты `ReliabilityCalculator` — валидный полный (verify=FAILED
   не свежесть), mode=wal/full/off, BROKEN→full, ongoing-факты, битые JSON,
   старые work-ключи без полей. Интеграция: refresher с реальным etcd
   подхватывает расширенный work-ключ; `/api/reliability` отдаёт числа
   (смоук по образцу существующих Inspection-тестов).
5. UI: страница «Надёжность» рендерится на стенде (full-профиль) с числами
   живого стенда; ongoing-бейдж тикает; карточка Overview показывает сводку;
   форм ввода нет.
6. Мерж-гейт: docker-E2E кейс-маркер
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
   зелёный на свежем Release.
7. Мерж-гейт трека reliability (roadmap README): тег t17 снят из
   `arch/roadmap/reliability.md` (список + `←`-зависимости) мерж-коммитом;
   в `reliability-report.md` тем же коммитом — «RPO/RTO-чисел оператору
   нет (`t17`)» убран из открытых разрывов §N, строка перенесена в «Сделано
   в рамках трека», сводка характеристики N обновлена.
8. Исторических атрибуций в arch/docs не оставляем (правило AGENTS.md):
   правки arch/ описывают состояние, без «в рамках t17».

## 7. Принятые решения (жёсткие — из ответов пользователя)

1. **RTO-источник — расширение work-ключа** `/pgworker/work/<C>` полями
   `last_failover`/`last_rebuild` (последний факт каждого вида, без
   истории/кольца); вариант нового ключа `/pgworker/ha-events` ОТМЕНЁН
   пользователем. Пишет NodeSupervisor в существующих точках
   (AccelerateDeadLeaderFailoverAsync, rebuild-ветка SuperviseShardAsync,
   RecreateMarkedNodesAsync); открытое событие переживает takeover.
   Следствие: чистки D2/S3 не меняются (новых ключей нет).
2. **UI**: отдельная read-only страница «Надёжность» (`/reliability`,
   таблица кластер×шард: RPO-блок + RTO-блок) + сводная карточка на
   Overview по образцу клиентских агрегаций HA/Backups.
3. **Prometheus**: панель + экспорт — длительности failover/rebuild сериями
   `pgworker_ha_failover_duration_seconds`/`pgworker_ha_rebuild_duration_seconds`
   (расширение словаря arch/18 §2.7 + панель Grafana по канону t14; без
   новой группы алертов); RPO-числа в Prometheus НЕ экспортировать —
   покрыты сериями t14.
4. **Семантика длительности**: от ПЕРВОГО тика недоступности (окно детекции
   входит, NodeDeadSec=90) до работоспособности (RUNNING / смена
   leader-ключа); detected_unix уже фиксируется треком надзора.
5. **Состав — расширенный**: RPO-блок (возраст валидного полного + id, лаг
   WAL, сводный RPO-потенциал «потеряем ≈ N» с индикацией источника);
   RTO-блок (последний failover/rebuild + длительности последнего
   drill/restore из снапшота с cause/итогом + ongoing-бейдж «идёт сейчас»
   с тикающей длительностью); БЕЗ истории событий (следствие п.1).
