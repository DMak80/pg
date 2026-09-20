# t05-valkey-metrics — телеметрия Valkey-домена

Spec Фазы 1 (dev-flow). Постановка — `arch/roadmap/valkey.md` (тег `t05-valkey-metrics`).
Канон-образец — `arch/18-metrics.md` (словарь §2, воркер-паттерн §2.2, коллектор §4,
стенд §5). Worktree: `feat-t05-valkey-metrics`.

## 1. Цель и контекст

Ввести телеметрию Valkey-домена тем же единым каркасом, что у PgWorker/KafkaWorker:

1. **Воркер-паттерн §2.2 у ValkeyWorker уже работает** (сделано в t02): `AddAppMetrics("ValkeyWorker")`,
   `WorkerMetricsInstrumentation` + подписка на `WorkJournal.PhaseWritten`, `app.MapAppMetrics()`,
   `/metrics` на mTLS-Kestrel `:8080`. Эта часть постановки НЕ переделывается — только
   проверяется тестами.
2. **Добавить коллектор доменных метрик Valkey-нод** — структурное зеркало коллектора
   Kafka (§4): тиковый hosted-сервис, INFO через redis-пробу (RESP-миниклиент воркера),
   самонаблюдение коллектора.
3. **Пополнить словарь arch/18 §2** секцией valkey-домена; дашборд `dashboards/valkey.json`;
   конфиг `ValkeyWorker:Metrics` (CollectIntervalSec).

Контракт etcd (`arch/20`) НЕ меняется: телеметрия — наблюдаемая грань, коллектор читает
существующие ключи (`/valkey/clusters/`, `/valkeyworker/portalloc/`) read-only.

Канон, который меняется: `arch/18-metrics.md` (словарь/коллектор/стенд/конфиг) и
`arch/21-valkeyworker.md` (два места упоминания t05-коллектора — §7 и блок «Границы»
преамбулы; см. §6). Порядок arch/-first: сначала arch/, затем код.

Решения пользователя (зафиксированы при уточнении):

- Словарь — **расширенный** набор из INFO + **полная секция Replication** (15 серий).
- Алерты — **Stalled + 2 доменных** (ValkeyEvictionsGrowing, ValkeyMemoryNearMax).
- Подход — **зеркало Kafka** (без рефакторинга Shared.Metrics, без сбора в циклах).

## 2. Принципы

- **Пассивный наблюдатель** (arch/18): сбор никогда не влияет на поведение воркера —
  ошибки коллектора не роняют тик/хост, серии — ObservableGauge поверх стейта.
- **Зеркало паттернов**: `ValkeyMetricsState`/`ValkeyMetricsCollector` — структурные копии
  `KafkaMetricsState`/`KafkaMetricsCollector` (включая консервативный LastSuccess и
  материализацию измерений под lock).
- **Все доменные серии — gauge** (не counter): persistence off и пересоздания контейнера
  сбрасывают кумулятивные счётчики Valkey в 0 — counter-тип ломал бы `increase()`/`rate()`;
  gauge-снапшоты за тик честны и после сброса. PromQL-динамика — через `rate()`/`increase()`
  по gauge-серям (для доменного словаря это каноническое решение, фиксируется в arch/18 §2).
- **Одна INFO-проба на ноду за тик** (аналог «одного AdminClient-коннекта за тик» §4):
  команда `INFO all` — все секции одним кадром; короткоживущий TCP с таймаутом клиента (5 c).
- **Сбор вне клэймов** (read-only, §4): параллелен любым процессам A–E, безопасен.
- **Консервативный LastSuccess**: `valkey_collector_last_success_timestamp_seconds`
  обновляется только при полном успехе всех кластеров тика (зеркало Kafka; пустой
  список кластеров = успех).
- **Без backoff-механизма** (отличие от Kafka, фиксируется в arch/18 §4): RESP-миниклиент —
  лёгкая короткоживущая TCP-проба (не тяжёлый AdminClient); лежачая нода стоит один
  connect-таймаут за тик, отдельный backoff-стейт не нужен (YAGNI).
- Язык: документация/комментарии — русские; идентификаторы — английские (AGENTS.base §7).

## 3. Словарь метрик valkey-домена (канон → arch/18 §2, новая секция)

Имена — финальные Prometheus-формата (dot-нотация Meter → подчёркивания при экспорте;
фактические имена фиксирует интеграционный тест, риск M3). Лейблы конечны:
`cluster` (доменное имя кластера), `node` (`node<k>`), для role — `role` ∈ {master,slave}.
Лейблы `job`/`instance` назначает Prometheus по scrape-джобе (§5.2 паттерн).

| Имя (финальное) | Тип | Лейблы | Источник INFO | Смысл |
|---|---|---|---|---|
| `valkey_memory_used_bytes` | gauge | cluster, node | Memory.used_memory | потребление памяти нодой, байты |
| `valkey_memory_max_bytes` | gauge | cluster, node | Memory.maxmemory | предел памяти ноды (maxmemory), байты |
| `valkey_connected_clients` | gauge | cluster, node | Clients.connected_clients | подключённые клиенты |
| `valkey_blocked_clients` | gauge | cluster, node | Clients.blocked_clients | заблокированные клиенты (BLPOP и т.п.) |
| `valkey_evicted_keys` | gauge | cluster, node | Stats.evicted_keys | кумулятив выселенных ключей (сброс при рестарте — gauge) |
| `valkey_expired_keys` | gauge | cluster, node | Stats.expired_keys | кумулятив истёкших ключей |
| `valkey_keyspace_hits` | gauge | cluster, node | Stats.keyspace_hits | кумулятив попаданий (hit-rate = hits/(hits+misses)) |
| `valkey_keyspace_misses` | gauge | cluster, node | Stats.keyspace_misses | кумулятив промахов |
| `valkey_instantaneous_ops_per_sec` | gauge | cluster, node | Stats.instantaneous_ops_per_sec | операции/сек (готовая скорость, без rate()) |
| `valkey_total_connections_received` | gauge | cluster, node | Stats.total_connections_received | кумулятив принятых соединений |
| `valkey_rejected_connections` | gauge | cluster, node | Stats.rejected_connections | кумулятив отклонённых соединений (maxclients) |
| `valkey_total_commands_processed` | gauge | cluster, node | Stats.total_commands_processed | кумулятив обработанных команд |
| `valkey_role` | gauge | cluster, node, role | Replication.role | 1 в серии с `role="master|slave"` (enum-паттерн) |
| `valkey_connected_slaves` | gauge | cluster, node | Replication.connected_slaves | число реплик (v1 standalone — всегда 0; заготовка под будущие топологии) |
| `valkey_collector_last_success_timestamp_seconds` | gauge | — | самонаблюдение | unix-время последнего успешного тика коллектора |

Поле INFO отсутствует/нечислово в ответе ноды → конкретная серия не эмитится (консервативно,
без нулей-фантомов); сам сбор кластера при этом успешен.

Воркер-паттерн §2.2 (циклы/клэймы/фазы/операции/снапшоты) — общий словарь, у ValkeyWorker
уже работает с t02; фактический `process`-словарь valkey-журнала: `provision`,
`deprovision`, `rotate`, плюс `supervise` (подавляемый, SuppressedOps каркаса) и
вспомогательный `healing-portalloc` (факт фиксирует интеграционный тест по образцу §2.2;
в t05 не меняется).

## 4. Компоненты (код)

Все новые файлы — в существующих проектах; новые проекты/сборки НЕ создаются.

### 4.1. `IValkeyConnection` + `ValkeyConnection` (ValkeyWorker.Core/Valkey)

- Новый метод `InfoAllAsync(ValkeyEndpoint ep, CancellationToken ct)` →
  `Result<IReadOnlyDictionary<string, string>>`: команда `INFO all` (bulk-строка) →
  плоский словарь `ключ → значение` (строки `k:v`, `\r\n`-разделители; заголовки секций
  `# Name` и пустые строки пропускаются). Словарь хранит всё как строки, включая
  нечисловые (напр. `db0` из Keyspace) — коллектор берёт только числовые поля перечня §3.
- Парсер INFO — чистая функция из bulk-строки (строки `k:v`, `\r\n`-разделители,
  `# section`-заголовки, пустые строки, комментарии) — internal для юнит-тестов
  (паттерн `Resp`-парсера).
- Каркас `ExecuteAsync` (connect → AUTH → команда → проекция) переиспользуется как есть.

### 4.2. `ValkeyMetricsState` (ValkeyWorker.App, новый файл)

Зеркало `KafkaMetricsState`: конструктор регистрирует 15 ObservableGauge в сервисном
`Meter`; стейт под lock (`Dictionary<(string Cluster, string Node), …>` + `_lastSuccess`);
материализация `.ToArray()` под lock (урок Ф7-4); `UpdateCluster(cluster, значения)`
затирает предыдущие записи кластера (ушедшие ноды не копятся); `MarkSuccess(at)` — только
при полном успехе тика; `DebugSnapshot()` для юнит-тестов (InternalsVisibleTo уже есть
по паттерну). Серия `valkey_role` хранится как `(cluster, node, role) → 1`.

### 4.3. `ValkeyMetricsCollector` (ValkeyWorker.App, новый файл)

Зеркало `KafkaMetricsCollector` (hosted-сервис `BackgroundService`):

- Тик `ValkeyWorker:Metrics:CollectIntervalSec` (default 30; `<=0` → 30 + warning-лог).
- Источник кластеров: Range `/valkey/clusters/` с failover по endpoints →
  `ValkeySnapshotParser.Parse` (тот же делегат-паттерн, что у Kafka-коллектора в
  Program.cs). Ошибка чтения → пропуск тика (LastSuccess не двигается).
- Фильтр кластеров: только Active (`Config.State == null` — невыполненные заявки не
  трогаем, зеркало ревью Ф4-6) и полные дискавери-поля: `AdminUser`, `AdminPassword`
  обязательны.
- Адреса нод: отдельный Range `/valkeyworker/portalloc/` (формат значения
  `{"node<k>":{"host":"h","client":17001}}`, канон `NodeAddress`). Нода кластера без
  portalloc-записи — пропуск (лестница E9 — забота надзора C, коллектор только читает).
- Хост пробы: `AdvertisedClientHost ?? portalloc.host` (правило advertised §2 arch/21,
  тот же резолв, что NodeSupervisor/ConfigConverger).
- Сбор ноды: одна `InfoAllAsync` с admin-кредом; извлечение полей словаря §3 →
  `state.UpdateCluster(...)`. Ошибка ноды (Failed/timeout) → тик кластера неуспешен
  (warning-лог, зеркало Kafka), остальные кластеры тика собираются.
- LastSuccess: только при полном успехе всех (cluster, node)-проб тика.
- `CollectOnceAsync` публичен для юнит-тестов (паттерн Kafka).

### 4.4. Program.cs и опции (ValkeyWorker.App)

- `Options.cs`: `Shared.Metrics.MetricsOptions Metrics` → заменяется на
  `ValkeyWorkerMetricsOptions : Shared.Metrics.MetricsOptions` c `int CollectIntervalSec = 30`
  (зеркало `KafkaWorkerMetricsOptions`); комментарий «коллектор — t05, не входит» убирается.
- `appsettings.json`: секция `"Metrics"` дополняется `"CollectIntervalSec": 30`.
- `Program.cs`: DI `ValkeyMetricsState` + `AddHostedService(ValkeyMetricsCollector)`
  после циклов (место — зеркало Kafka: сразу после ReconcileLoop-блока).

### 4.5. Тестовый фейк

Юнит-фейки `IValkeyConnection` — в тестовых проектах (интерфейс мал; отдельный
shared-фейк не заводим).

## 5. Стенд мониторинга (dev-stand/adminpanel/metrics)

### 5.1. prometheus.yml

Новая scrape-джоба (зеркало `kafkaworker`): `job_name: valkeyworker`, `scheme: https`,
тот же `tls_config` (per-install API-CA + клиентский серт скрейпера из TLS-пакета стенда),
static-таргет `valkeyworker:8080` (сеть стенда; сервис `valkeyworker` в compose профиля
`valkey`, полный подъём `checks/00-up.sh` его поднимает).

### 5.2. rules.yml — группа `valkey` (3 алерта)

| Алерт | expr | for | severity |
|---|---|---|---|
| `ValkeyCollectorStalled` | `time() - valkey_collector_last_success_timestamp_seconds > 300` | 0m | warning |
| `ValkeyEvictionsGrowing` | `rate(valkey_evicted_keys[5m]) > 0` | 5m | warning |
| `ValkeyMemoryNearMax` | `(valkey_memory_used_bytes / valkey_memory_max_bytes > 0.9) and (valkey_memory_max_bytes > 0)` | 10m | warning |

`and valkey_memory_max_bytes > 0` отсекает безлимитный maxmemory=0 (деление дало бы
+Inf → ложный алерт; по канону домена maxmemory задаётся всегда, случай теоретический —
но запрос честный). Runbook-ссылки в annotations — на arch/18 §4/§2-valkey (паттерн
kafka-группы).

### 5.3. dashboards/valkey.json

Uid `valkey`, стиль — точный паттерн `kafka.json` (schemaVersion 41, refresh 30s, now-1h).
Панели:
- timeseries «Memory used vs max» — `valkey_memory_used_bytes` + `valkey_memory_max_bytes`
  (legend `{{cluster}}/{{node}}`), + stat «Memory pressure» —
  `valkey_memory_used_bytes / valkey_memory_max_bytes and valkey_memory_max_bytes > 0`;
- stat «Keyspace hit rate» — `valkey_keyspace_hits / clamp_min(valkey_keyspace_hits + valkey_keyspace_misses,1)`;
- timeseries «Evictions (rate 5m)» — `rate(valkey_evicted_keys[5m])`;
- timeseries «ops/sec» — `valkey_instantaneous_ops_per_sec`;
- timeseries «Clients» — `valkey_connected_clients` + `valkey_blocked_clients`;
- timeseries «Connections (rate)» — `rate(valkey_total_connections_received[5m])` +
  `rate(valkey_rejected_connections[5m])`;
- stat «Connected slaves» — `valkey_connected_slaves`;
- stat «Collector staleness, s (alert > 300)» — `time() - valkey_collector_last_success_timestamp_seconds`.

Провижинер Grafana уже грузит весь каталог `dashboards/` — новых записей в provisioning не надо.

### 5.4. checks/65-metrics.sh

Дополнения (паттерн существующих шагов):
- Гарантия живости: `docker start as-valkeyworker` + ожидание `/healthz` (mTLS-кертом
  healthcheck, как as-kafkaworker шаг 0) — чек обязан проходить после любой предыстории.
- Серия `valkey_collector_last_success_timestamp_seconds` в TSDB (retry-цикл): пустой
  домен = консервативный успех тика, серия появится при живом воркере (отличие от
  kafka-серии, остающейся условной).
- Число алертов в шаге rules: `>= 8` → `>= 11` (8 существующих + 3 valkey).
- Дашборды: `>= 3` → `>= 4`.
- Scrape-джоба `valkeyworker` попадает в общий all-up-гейт (шаг 2) автоматически.

## 6. Обновление канона (arch/-first, ПЕРВЫМ шагом реализации)

1. `arch/18-metrics.md`:
   - §2: новая подсекция «2.6. Valkey-домен (коллектор ValkeyWorker §4)» — таблица §3
     этого спека + фиксация gauge-семантики кумулятивов (сбросы при рестарте контейнера)
     и консервативного отсутствия серии при отсутствии поля INFO;
   - §2.2: процесс-словарь valkey-журнала (`provision`, `deprovision`, `rotate`,
     `supervise`, `healing-portalloc`) — строкой в существующий перечень;
   - §4: раздел переименовать в «Коллекторы доменных метрик (Kafka, Valkey)», добавить
     valkey-подраздел: тик, источник кластеров/адресов (portalloc + AdvertisedClientHost),
     `INFO all` одной пробой на ноду, консервативный LastSuccess, фиксация «без backoff»
     с обоснованием (лёгкая TCP-проба против AdminClient);
   - §5.2: строка джобы `valkeyworker` в таблицу scrape-таргетов;
   - §5.3: `dashboards/valkey.json` в перечень;
   - §6: тесты valkey-коллектора (unit/integration) в канон уровня;
   - §8: `ValkeyWorker:Metrics { CollectIntervalSec=30 }` в конфиг-блок;
   - §9: новая строка-риск НЕ добавляется — принцип кардинальности уже зафиксирован M1
     (лейблы конечны; valkey-лейблы cluster/node — реальные сущности домена).
2. `arch/21-valkeyworker.md` — ДВА места упоминания t05-коллектора (ревью Ф4):
   - **§7 «Наблюдаемость»**: фраза «коллектор доменных метрик INFO и дашборд — t05
     (вне t01)» заменяется ссылкой на реализацию (arch/18 §4) — упоминание t05 уходит.
   - **Преамбула, блок «Границы (что НЕ входит)»**: пункт «коллектор доменных метрик INFO
     и дашборд (t05, arch/18 §2.2/§4-паттерн)» УДАЛЯЕТСЯ. После t05 коллектор — внутренний
     hosted-сервис ValkeyWorker: утверждение «не входит» становится ложным, а канон не
     вправе содержать ложь после задачи (arch/ — источник истины; прецедент t08 — канон
     правили под факт кода). Наблюдаемость воркера уже описана в §7 (ссылка на arch/18);
     дашборд — артефакт стека мониторинга arch/18 §5, не грань воркера — отдельной строки
     в «Границах» не требует. Пункты про панель (t03) и библиотеку Puzzle (t04) — внешние
     артефакты, остаются в «Границах» как есть.
   - **НЕ трогать**: упоминание «урок инцидента t05» в §2 (kfw-net, исчерпание подсетей
     docker) — это ДРУГОЙ t05 (нумерация инцидента мерж-гейта 2026-09-05), к тегу задачи
     `t05-valkey-metrics` отношения не имеет.
3. `arch/roadmap/valkey.md`: пункт `t05-valkey-metrics` удаляется тем же коммитом мержа
   (правило roadmap: слитая задача снимает тег; «Порядок» остаётся про остальные задачи).

## 7. Фазы реализации (для плана Фазы 2)

- **Ф1 Канон**: правки arch/18 (§2.6/§2.2/§4/§5/§6/§8), arch/21 (§7 + блок «Границы»
  преамбулы; упоминание инцидента kfw-net в §2 не трогается) — arch-only коммит.
- **Ф2 RESP INFO**: `InfoAllAsync` + парсер INFO в ValkeyConnection (TDD: юнит-парсер).
- **Ф3 Коллектор**: `ValkeyMetricsState` + `ValkeyMetricsCollector` + опции + DI
  (TDD: юнит-тесты коллектора/стейта по образцу `KafkaMetricsCollectorTests`).
- **Ф4 Интеграционные тесты**: фиксация фактических имён словаря в `/metrics`
  (`MetricsApiTests`); живой коллектор против docker-ноды valkey (fixture
  `ValkeyClusterFixture`/`RespProbe`-паттерн): provisioning кластера → INFO-сбор →
  серии в стейте/экспорте.
- **Ф5 Стенд**: prometheus.yml + rules.yml + dashboards/valkey.json + checks/65-metrics.sh.
- **Ф6 Мерж-гейт**: полный прогон юнитов/интеграций; docker-E2E ValkeyWorker на свежем
  Release (`ValkeyE2eLifecycleTests`; правило AGENTS.md для задач, трогающих код воркеров);
  чек 65 на стенде; снятие тега t05 из roadmap.

## 8. Ограничения

- Контракт etcd (`arch/20`) и AdminPanel НЕ трогаются: панель живёт на etcd-снапшоте,
  метрики в ней не отображаются (границы arch/18).
- Shared.Metrics НЕ рефакторится (решение пользователя: зеркало, не обобщение).
- Репликация — только чтение секции Replication INFO (`role`, `connected_slaves`);
  управление репликами вне домена v1 (arch/21 границы).
- TLS к нодам — t06-valkey-tls; коллектор ходит plain-RESP в доверенной docker-сети
  (как все пробы воркера).
- Кардинальность: серии только по существующим (cluster, node) из portalloc; ушедшие
  ноды затираются UpdateCluster, домен v1 — nodes=1.

## 9. Тестирование

- **Unit (ValkeyWorker.UnitTests)**:
  - парсер INFO: секции/`\r\n`/пустые строки/нечисловые значения/битый ввод;
  - коллектор (fake `IValkeyConnection`): успешный сбор обновляет стейт; ошибка ноды —
    тик жив, LastSuccess не двигается; пустой домен — успех; не-Active/без кредов —
    пропуск без пробы; нода без portalloc — пропуск; `<=0`-интервал — дефолт 30;
  - стейт: UpdateCluster затирает ушедшие ноды; DebugSnapshot-значения.
  - Комментарии тестов — AAA (правило AGENTS.md пользователя).
- **Integration (ValkeyWorker.IntegrationTests)**:
  - `/metrics` содержит канонические имена §3 (фиксация фактических экспортированных
    имён против словаря, риск M3; `otel_scope_name="ValkeyWorker"`);
  - коллектор против реальной valkey-ноды (docker): INFO-сбор живой ноды → серии
    словаря со значениями; остановленная нода → LastSuccess стоит, тик не падает.
- **E2E/стенд**: чек 65 (§5.4); docker-E2E Release-серия ValkeyWorker — мерж-гейт.
- Интеграционные таймауты короткие; контейнеры/сети — полный teardown при любом исходе
  (правила AGENTS.md/AGENTS.base §11).

## 10. Критерии приёмки

1. `arch/18` содержит §2.6 (15 серий §3), valkey-подраздел §4, джобу/дашборд/конфиг;
   `arch/21` чист от устаревших упоминаний t05-коллектора: §7 ссылается на arch/18 §4,
   блок «Границы» без пункта про коллектор (упоминание инцидента kfw-net в §2 остаётся —
   другая сущность); тег t05-valkey-metrics снят с roadmap (тем же коммитом мержа).
2. `GET /metrics` живого ValkeyWorker (стенд) отдаёт все 15 канонических имён §3 при
   живом демо-кластере; без кластеров — `valkey_collector_last_success_timestamp_seconds`.
3. Prometheus: джоба `valkeyworker` up в полном стенде; TSDB содержит valkey-серии;
   Grafana показывает дашборд valkey (4 дашборда); rules — 11 алертов, симуляция
   `ValkeyCollectorStalled` доступна через остановку демо-ноды (ручная проверка).
4. Юниты/интеграции ValkeyWorker + Shared.Metrics зелёные; docker-E2E
   ValkeyWorker на свежем Release зелёный (мерж-гейт).
5. Чек `65-metrics.sh` проходит после полного `00-up.sh` и после частичной
   предыстории (гарантия живости valkeyworker внутри чека).
6. Код воркера не изменил поведение процессов A–E: сбор — только новые файлы
   `ValkeyMetrics*`, точечные вставки DI/опций (дифф Program.cs — регистрация
   коллектора; Options.cs — subclass).
