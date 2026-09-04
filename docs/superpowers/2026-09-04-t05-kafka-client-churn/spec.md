# t05-kafka-client-churn — AdminClient не churn'ится и не жжёт CPU на недоступных кластерах KafkaWorker

- Дата: 2026-09-04
- Roadmap: `arch/roadmap/kafkaworker.md`, тег `t05-kafka-client-churn` (исчезает из roadmap тем же коммитом мержа)
- Каноны: `arch/16-kafkaworker.md` (воркер), `arch/15-kafka-clusters.md` (контракт etcd),
  `arch/17-synchronization-principles.md` (E9 — лестница самолечения portalloc, **уже
  слита в main коммитом 4fb24b7; здесь ссылка, не переопределение**), `arch/18-metrics.md`
- Образец паттерна: t11-kafka-probe-churn в панели (коммит `0e59744`, 2026-09-02) —
  кэш `KafkaClientCache`, пины librdkafka ≥1000 мс, backoff 15→60→300 с, чек 58
- Worktree: `feat-t05-kafka-client-churn`

## 1. Цель и проблема

Инцидент as-kafkaworker 2026-09-04 (~100% ядра весь аптайм, cgroup ~52 core-мин
за 50 мин, 2/3 system time). Разбор на живом стенде показал:

1. **Churn клиентов**: каждый kafka-доменный шаг создаёт новый Confluent
   AdminClient — `KafkaAdminClientFactory.Create()` вызывается на каждый тик из
   supervise, converger'а, reassigner'а, TopicSync (несколько раз за тик),
   provisioning (K4-ожидание) и коллектора метрик; на лежащем кластере это
   4–6 rd_kafka-инстансов/мин, каждый с `rdk:main` + брокерными нативными
   потоками + 2 managed LongRunning.
2. **Reconnect-шторм**: дефолтные `reconnect.backoff.ms=100`/`retry.backoff.ms=100`
   при мгновенном connection-refusal дают 468 «3/3 brokers are down» за 3 мин
   (~1000 строк лога/мин).
3. **Зависший поток**: LongRunning-поток Confluent.Kafka в состоянии `Running`
   без syscalls (804/800 тиков за 8 с) крутит 100% ядра весь аптайм —Dispose
   недоступного клиента не завершает poll-цикл (дефект Confluent.Kafka;
   следствие churn'а: каждый тик оставляет такой поток).
4. **Тупик portalloc**: seed-кластер с `endpoints host.docker.internal:16001–16003`
   (refused), portalloc пуст при объявленных брокерах → supervise падает
   «broker broker1 не закреплён в portalloc» каждый тик вечно — утеря журнала
   аллокаций трактуется как «нехилируемо» (ошибка E9, arch/17).

Тот же класс инцидента, что t11 в панели (0e59744), но защиты t11 на воркер не
переносились. Коллектор t04 не причина (добавляет лишь 1 клиент/30 с при
`Metrics.Enabled=true`), но включается в общее решение.

**Цель**: недоступный кластер Kafka не стоит воркеру ни CPU, ни лог-шторма, ни
роста потоков; утерянный portalloc самолечится лестницей E9 без оператора;
живые кластеры работают как раньше (латентность операций не регрессирует).

### 1.1. Декомпозиция (6 пунктов roadmap → компоненты спеки)

| # | Пункт roadmap | Компонент здесь |
|---|---|---|
| 1 | Кэш AdminClient per `(bootstrap,user,password)`, инвалидация при смене кредов/endpoints | §3.1 кэширующая `KafkaAdminClientFactory` |
| 2 | Пины librdkafka ≥1000 мс в `AdminClientConfig` | §3.1 (профиль BackoffMs=1000/MaxMs=10000, rdkafka-лог → Debug) |
| 3 | Детерминированный `await using` во всех путях, не финализаторы GC | §3.1 (владение у кэша; Dispose — фон-вытеснение и shutdown) |
| 4 | Экспоненциальный backoff недоступного кластера в kafka-шагах (15→60→300 с, сброс при успехе) | §3.2 `KafkaClusterBackoff` + гейты supervise/Active/коллектора |
| 5 | Самолечение утерянного portalloc (E9, arch/17): лестница трёх веток | §3.3 `PortAllocHealer` + §3.4 грань инспекции драйвера |
| 6 | Согласие стенда + чек CPU/threads по образцу 58-го | §6 операционные меры, §5.4 чек 66 |

## 2. Принципы

- **arch/-first**: правки arch/16 (§5 C — лестница+гейт пробы; §5 Active-ветка —
  backoff kafka-шагов; §6 — кэш клиентов) делает execute по плану; arch/17 не
  переопределяется (E9 уже канонизирован), arch/15 не меняется — лестница
  использует существующие ключи (`portalloc/<C>`, `locks/portalloc`, `endpoints`),
  новых ключей и форматов нет.
- **Паттерн t11 — адаптация, не копия**: кэш per ключ + пины librdkafka +
  фоновый Dispose заменяемых + backoff-шкала 15→60→300 (сброс при успехе,
  чистка исчезнувших). Отличия от панели продиктованы устройством воркера:
  ключом владеет фабрика (процессы не меняются), политика skip'а шагов —
  централизованная (ActiveAsync), backoff-проба надзора возвращает «слепую
  пробу» (существующая семантика null).
- **Seam `IKafkaAdminClient` неприкосновенен**: сигнатуры операций и
  `Result`-семантика не меняются; меняется контракт владения у
  `IKafkaAdminClientFactory` (§3.1). Юнит-тесты процессов на fake'ах остаются
  валидными.
- **Флап ≠ смерть (arch/17 S7)**: backoff не меняет `brokers/<b>/state` и не
  стартует/не исполняет бюджет молчания — слепая проба остаётся слепой пробой
  (unreachable-трек заморожен, чистка только по исчезновению из декларации).
- **Данные неприкосновенны**: лестница E9 никогда не трогает тома; живой
  контейнер при реконструкции portalloc не пересоздаётся (EnsureNode
  идемпотентен по имени).
- **Клэймы и первый-записавший (arch/17 S5)**: все записи portalloc — только
  под глобальным клэймом `/kafkaworker/locks/portalloc`; проигрыш txn → re-read
  и догоняем (первый записавший — истина); отсутствие docker-объекта —
  положительное свидетельство смерти (S7).
- **Динамические порты в тестах**: никаких литералов `:16000`; закрытые порты —
  заведомо свободные (зонд), окна публикаций — `FreePortWindow` (AGENTS.md).
- **Язык**: документация/комментарии — русский, идентификаторы — английские.

## 3. Структура / компоненты

### 3.1. Кэширующая фабрика `KafkaAdminClientFactory` (переписывание)

Файл: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (фабрика;
адаптер `KafkaAdminClient` почти без изменений — см. ниже).

**Новый контракт владения `IKafkaAdminClientFactory`** (фиксируется в
`IKafkaAdminClient.cs`):

```csharp
/// <summary>Фабрика клиентов по bootstrap+кредам (SASL/PLAIN, arch/15 §5).
/// t05: ШАРЕНАЯ — возвращает кэшированный адаптер per (bootstrap,user,password);
/// Create — не «новый клиент», а «получить клиент ключа». DisposeAsync
/// возвращённого адаптера — no-op (владение у фабрики-кэша); реальный Dispose —
/// вытеснение из кэша (фон) и остановка host'а. Смена endpoints/кредов — другой
/// ключ → другой клиент (инвалидация по построению).</summary>
public interface IKafkaAdminClientFactory
{
    IKafkaAdminClient Create(string bootstrap, string user, string password);
}
```

Механика:

- Кэш: словарь `(string Bootstrap, string User, string Password) → Entry`
  под lock (паттерн `KafkaClientCache` t11). `Entry` — адаптер + `LastUsedUtc`
  + `Unhealthy`-флаг.
- **Пины librdkafka** в `AdminClientConfig` всех создаваемых клиентов
  (константы фабрики, не конфиг — YAGNI):
  `RetryBackoffMs = 1000`, `ReconnectBackoffMs = 1000`,
  `ReconnectBackoffMaxMs = 10000` + `SetLogHandler` → `LogDebug("rdkafka: …")`
  (reconnect-шторм «3/3 brokers are down» уходит с Info на Debug).
- **Unhealthy-инвалидация**: адаптер `KafkaAdminClient` получает внутренний
  колбэк/флаг — `RunAsync` при исходе `Result.Failed` (любая операция) помечает
  свою запись `Unhealthy`, кроме отмены (`OperationCanceledException` при
  `ct.IsCancellationRequested` — остановка host'а клиента не инвалидирует).
  Следующий `Create` по этому ключу видит `Unhealthy` → создаёт свежий клиент,
  старый Dispose — **в фоне** (`Task.Run`; Dispose недоступного клиента может
  ждать poll-поток — не в горячем пути тика, урок t11). Транзиентные таймауты
  живого кластера дают редкие пересоздания — не churn (частоту контакта с
  лежащим кластером давит §3.2).
- **Вытеснение по неактивности**: при каждом `Create` — проход по кэшу:
  `LastUsedUtc` старше `IdleEvictAfterMin = 10` → фоновый Dispose. Кластер
  удалён/креды сменены — ключ перестаёт запрашиваться, нативные потоки не
  копятся. Без таймеров (чистка срабатывает с тиками, которых у воркера всегда
  много).
- **Shutdown**: фабрика реализует `IDisposable` (DI-синглтон уже есть) —
  детерминированный Dispose всех живых клиентов при остановке host'а.
- **Счётчик `CreatedClients`** (public, как t11) — метрика churn'а для
  интеграционных тестов.
- Потокобезопасность: `IAdminClient` Confluent допускает параллельные вызовы;
  разные кластеры — разные ключи/клиенты; один кластер — последовательные шаги
  Active + возможный параллельный тик коллектора (тот же клиент, это допустимо).

Процессы (supervise/provisioning/converger/reassigner/topicSync/ротация/
add-remove/коллектор) **не меняются**: их `await using var admin =
adminFactory.Create(...)` остаётся и теперь детерминированно no-op'ит Dispose —
«клиент на тик» исчезает без каскадного рефакторинга и без финализаторов GC
(ни один путь не оставляет клиент без владельца: владелец всегда кэш).

Отвергнутая альтернатива — убрать `await using` из всех процессов: диф по
10+ файлам и тестам при той же семантике; no-op dispose на seam'е закрывает
пункты 1–3 roadmap минимально.

### 3.2. Трекер недоступности кластера `KafkaClusterBackoff`

Файл: `src/KafkaWorker.Provisioning/Kafka/KafkaClusterBackoff.cs`.
DI-синглтон; в памяти инстанса (без etcd — политика частоты проб, не
состояние кластера); ctor принимает `TimeProvider` (тестируемость, паттерн
PartitionReassignerProcess).

```csharp
public sealed class KafkaClusterBackoff(TimeProvider clock)
{
    // Окно блокировки kafka-контакта после N-й подряд неудачи (t11-шкала):
    // 1-я → 15 с, 2-я → 60 с, 3-я и далее → 300 с. Сброс при успехе.
    internal static TimeSpan BackoffAfter(int consecutiveFailures);

    public bool IsBlocked(string cluster);              // окно активно?
    public void RecordFailure(string cluster, string error); // +1 неудача, раздвинуть окно
    public void RecordSuccess(string cluster);          // сброс
    public void ForgetMissing(IReadOnlySet<string> liveClusters); // чистка исчезнувших
}
```

Писатели (ровно два, оба — первые kafka-контакты конвейера):

1. **`NodeSupervisor.DescribeAliveAsync`** — гейт ДО похода в сеть: окно
   активно → сразу `null` (слепая проба без сети и без клиента); проба
   выполнялась и упала → `RecordFailure`; успешна → `RecordSuccess`.
2. **`KafkaMetricsCollector.TryCollectClusterAsync`** — тот же гейт (skip
   кластера в backoff — не создаёт клиент), фейл сбора → `RecordFailure`,
   успех → `RecordSuccess`.

Гейт шагов Active — **`KafkaClusterProcesses.ActiveAsync`**: после
`supervisor.RunAsync` (docker-часть надзора отработала всегда) —
`if (backoff.IsBlocked(cluster)) return Result.Success();` — kafka-шаги
E (converger) → I (reassigner) → G/F (remove/add) → H (ротация) → J
(регенерация) → D (TopicSync) пропускаются до истечения окна. Тик — успех
(не ошибка): лежащий кластер не долбится каждые 5–15 с и не засоряет
journal/лог.

Границы гейта:

- **Docker-часть надзора не гейтится** (снесённый контейнер пересоздаётся и
  при лежащем кластере — self-healing не зависит от kafka).
- **Provisioning (K0–K6) и Deprovisioning не гейтятся**: стартовый подъём —
  ожидаемые неудачи DescribeCluster внутри бюджета `BrokerBootSec`
  (транзиент-толерантный цикл); гейт удлинял бы старт до 300-с окон.
  С кэшем+пинами K4-ожидание уже не churn'ит (один клиент на ключ).
- **Backoff не пишет в etcd и не меняет `brokers/<b>/state`** — чистая
  политика частоты (флап ≠ смерть, S7); unreachable-трек заморожен как при
  слепой пробе (S6: трек не затирается).
- На каждый тик `ReconcileLoop` — `ForgetMissing` по множеству кластеров
  снапшота (состояние исчезнувших кластеров не копится, паттерн t11).

`KafkaWorker:Loops`/`Thresholds` НЕ расширяются: шкала — семантическая
константа компонента (как `BackoffAfter` t11), управляемость не требуется.

### 3.3. Лестница самолечения portalloc `PortAllocHealer` (E9)

Файл: `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs`.
Точка вызова — начало `NodeSupervisor.RunAsync`, сразу после чтения portalloc:
брокеры `Supervisable` без адреса в portalloc (сегодня это путь вечного
«broker <b> не закреплён в portalloc» при пересоздании) → лестница per broker.
Ранняя точка обязательна: `RecreateAsync` сносит контейнер ДО `EnsureNodeAsync`,
поэтому ветка 2 (живой контейнер) из EnsureNode недостижима — решать надо до
любых деструктивных действий. `EnsureNodeAsync`-Fail «не закреплён» остаётся
страховкой (недостижим при корректном потоке). Канон — arch/17 E9 (ссылка;
здесь — привязка к исполнению) + arch/16 §5 C (обновляет execute).

Два инварианта надзора поверх лестницы (ревью Ф7):

- **Перевод PROVISIONING→RUNNING — по трём фактам**: контейнер жив, зрячая
  проба видит брокера в кластере, И advertised-адрес брокера
  (`AdvertisedClientHost ?? host:port` из portalloc) уже присутствует в
  `/kafka/clusters/<C>/endpoints` — владелец процесса (add-broker F)
  пишет endpoints ДО RUNNING; без третьего факта supervise чужой процесс
  не «догоняет» (иначе add-broker-брокер получает RUNNING до своего
  endpoints-RMW → pending пуст → адрес навсегда вне bootstrap-списка).
- **Сходимость endpoints (шаг надзора, после лестницы)**: фактический
  `endpoints` сверяется с каноном «адреса всех брокеров, у которых есть
  закрепление в portalloc и state не TO_REMOVE/REMOVING»; расхождение →
  RMW (txn mod_revision; put если ключа нет). Закрывает недоехавший
  RMW ветки 3 после сбоя EnsureNode/endpoints (следующий тик — ветка 1,
  «ретрай тиком» без сходимости не исполнялся бы); для add-broker в
  полёте безопасен: адрес канонический, RMW владельца идемпотентен
  («добавить если нет»), порядок «endpoints до RUNNING» сохраняется.

Три ветки, по одной на брокера:

1. **Адрес есть в portalloc** → используется (нынешний путь; advertise
   стабилен — rebuild по закреплению).
2. **Журнала нет/неполон, контейнер есть** (положительная инспекция §3.4) →
   **реконструкция**: под глобальным клэймом `locks/portalloc`
   (`PortAllocLock.TryAcquireAsync`; не взял → journal-фаза
   `waiting-portalloc-lock`, InProgress, следующий тик) → запись portalloc
   put-if-absent (txn `version==0`; проигрыш → re-read и догоняем — первый
   записавший истина, S5) из инспекции: `host` — хост размещения контейнера,
   `client` — published host-порт CLIENT (9094). Контрольная сверка:
   клиентский порт в env `KAFKA_ADVERTISED_LISTENERS` контейнера ==
   published; расхождение → journal-warning (реконструкция по PortBindings —
   канон). **Контейнер не трогаем** (жив — данные неприкосновенны; EnsureNode
   идемпотентен по имени и пропустит); последующие пересоздания (молчание)
   пойдут по восстановленному закреплению.
3. **Нет ни журнала, ни контейнера** → брокер мёртв по S7-свидетельству →
   **новая аллокация** под тем же глобальным клэймом: занятость =
   docker-публикации (`GetBusyPortsAsync`) ∪ portalloc чужих (`PortAllocIndex`)
   ∪ свои закрепления → `PlacementPlanner.Plan` + `PortAllocator.Allocate`
   (паттерн `AddBrokerProcess.EnsurePortsAsync`: plan только для недобора,
   RMW portalloc txn compare `mod_revision`) → пересоздание контейнера по
   новому адресу (`EnsureNodeAsync`, `state=PROVISIONING` — та же семантика,
   что пересоздание снесённого контейнера в supervise) → **RMW `endpoints`**: пересборка
   advertise-адресов всех брокеров из восстановленного portalloc
   (`AdvertisedClientHost ?? host:port`), txn compare `mod_revision`
   (ключа нет → put). Клиенты перечитают дискавери тиком (arch/15 §5).

Свойства:

- **journal-before-manipulations**: фаза `healing-portalloc` (+ итог ветки
  `reconstructed` / `reallocated`) перед мутациями.
- **Идемпотентность/takeover**: повтор после сбоя между шагами безопасен —
  закреплённый адрес делает следующую итерацию веткой 1; эксклюзия — клэйм
  `<C>` (supervise под ним), кросс-кластерные гонки — глобальный клэйм
  (S5/t90/t91).
- Порядок «сначала portalloc, потом контейнер, потом endpoints» — фиксированный
  (S5): endPoint-читатели видят согласованные адреса.
- Диапазон портов — из `ProvisioningOptions` (как везде), никаких литералов.

AddBroker/Provisioning не меняются: их план-секции уже довыделяют порты под
клэймом (t91) — тупик существовал только в supervise (чтение без самолечения).

### 3.4. Грань инспекции endpoint'а в docker-драйвере

`IDockerEngine` (Engine) + `IClusterDriver` (Drivers), по образцу
`InspectContainerResourcesAsync` (404 → null; plain — перебор хостов):

```csharp
// IClusterDriver: инспекция размещения брокера для реконструкции portalloc
// (E9): null = docker-объекта нет (положительное свидетельство смерти, S7).
Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct);

public sealed record NodeEndpointInspection(
    string Host,            // хост размещения (имя из Docker:Hosts / хост таска)
    int ClientHostPort,     // published host-порт CLIENT-порта контейнера (9094)
    string? AdvertisedClient); // env KAFKA_ADVERTISED_LISTENERS → CLIENT (контроль)
```

- plain: `GET /containers/<name>/json` — `HostConfig.PortBindings[9094]` →
  host-порт, хост — на чьём движке найден; env — из `Config.Env`.
- swarm: published-порт и хост running-таска (`DockerTask.Host/PublishedPort`);
  env шаблона сервиса НЕ читаем — контрольная сверка advertised
  (journal-warning при расхождении с published) — **plain-only**, в swarm
  `AdvertisedClient = null` (поле nullable; отсутствие сверки — только
  отсутствие warning, адрес восстанавливается по PortBindings/таску).
  404/объекта нет → `null`.
- Ошибка инспекции (docker-хост молчит) → `Result.Failed` — надзор не решает
  вслепую (порт слепоты C), следующий тик повторит.

### 3.5. Точки интеграции (сводка)

| Точка | Изменение |
|---|---|
| `KafkaAdminClientFactory` (Program.cs DI) | кэширующая, `IDisposable` |
| `NodeSupervisor` | гейт пробы backoff'ом; лестница E9 для безадресных Supervisable-брокеров (вход — после чтения portalloc) |
| `KafkaClusterProcesses.ActiveAsync` | skip kafka-шагов E–J при `IsBlocked` |
| `KafkaMetricsCollector` | гейт сбора кластера backoff'ом |
| `ReconcileLoop.TickAsync` | `backoff.ForgetMissing(кластеры снапшота)` |
| `IClusterDriver`/`IDockerEngine` + оба драйвера | `InspectNodeEndpointAsync` |
| `PortAllocHealer` | новый компонент §3.3 |

## 4. Данные и контракт etcd

**Новых ключей и форматов нет** (arch/15 §4 не меняется). Лестница пишет
существующие ключи существующими паттернами:

| Ключ | Паттерн записи в лестнице |
|---|---|
| `/kafkaworker/portalloc/<C>` | ветка 2 — put-if-absent (`version==0`); ветка 3 — RMW (`mod_revision`) |
| `/kafkaworker/locks/portalloc` | оба веточных мутанта — под `PortAllocLock` (txn `version==0` + lease, t90/t91) |
| `/kafka/clusters/<C>/endpoints` | ветка 3 — RMW (`mod_revision`; put если ключа нет) |
| `/kafkaworker/work/<C>` | фазы `healing-portalloc` / `waiting-portalloc-lock` (существующий формат журнала) |

Правки arch/ (исполняет execute по плану; здесь — требуемые изменения):

- **arch/16 §5 C (надзор)**: ссылка на E9-лестницу (портalloc → реконструкция
  из inspect → новая аллокация + RMW endpoints); kafka-проба гейтится
  backoff'ом недоступного кластера (окно активно → слепая проба без сети).
- **arch/16 §5 C (надзор, дополнение ревью Ф7)**: два контракта фиксов —
  «перевод PROVISIONING→RUNNING — по трём фактам (контейнер жив, проба
  видит, advertised-адрес в endpoints); endpoints сходится к
  portalloc-канону тиком» (вносится Task 9 плана вместе с кодом фиксов).
- **arch/16 §5 (введение Active-ветки)**: kafka-шаги Active (E–J) пропускаются
  на время backoff недоступного кластера (15→60→300 с, сброс при успехе);
  docker-надзор и provisioning не гейтятся.
- **arch/16 §6 (надёжность)**: пункт «AdminClient — кэш per
  (bootstrap,user,password)»: sharable-фабрика, пины librdkafka ≥1000 мс,
  unhealthy-инвалидация, вытеснение неактивных, фоновый Dispose заменяемых.
- **arch/17**: канон E9 НЕ переопределяется — единственная правка: строка
  реестра ошибок «Закрыта» `E9: планируется t05` → `t05 (2026-09-…)` (дата
  фактического мержа; правка — тем же коммитом, Task 8 плана).
- **arch/15**: без изменений (форматы ключей прежние).

## 5. Фазы

Фазирование исполнения (детализация — в плане; порядок устойчив к ревью):

- **Ф1 — кэш+пины (§3.1)**: фабрика, юниты кэша; прогон юнитов процессов
  (контракт не изменился — зелёные без правок).
- **Ф2 — backoff (§3.2)**: трекер, гейты supervise/Active/коллектора,
  `ForgetMissing`; юниты шкалы/гейтов.
- **Ф3 — лестница E9 (§3.3–3.4)**: грань драйвера, healer, интеграция в
  supervise; юниты трёх веток + гонки.
- **Ф4 — интеграционные тесты (§7)**: churn на закрытых портах; подъём кластера
  после утери portalloc.
- **Ф5 — стенд (§6)**: согласие сида, чек 66, прогон приёмки на живом стенде.
- **Ф6 — arch/-правки + roadmap-гейт**: arch/16 §5 C/§5/§6 по §4; удаление
  тега `t05-kafka-client-churn` из `arch/roadmap/kafkaworker.md` тем же
  коммитом; E2E-гейт (§7).

## 6. Операционные меры (стенд)

1. **Согласие стенда** (операционная мера №6 roadmap, до/вместе с t05):
   рассинхрон инцидентного кластера (portalloc пуст при объявленных брокерах,
   endpoints refused) устраняется пересевом kafka-части `dev-stand/adminpanel/
   checks/05-seed.sh` либо очисткой конфига кластера из etcd. После Ф3 этот
   класс рассинхрона самолечится — мера разовая.
2. **Чек 66** (`dev-stand/adminpanel/checks/66-kafka-worker-churn.sh`, по
   образцу 58-го): репро инцидента на живом стенде — Active-кластер с
   endpoints на заведомо закрытые порты (префикс портов вне зон стенда и
   PgWorker-тестов, как `CHURN_PORTS` 58-го; предусловие — порты свободны) +
   brokers declared без portalloc. Ассерты за окно `CHURN_MINUTES` (по
   умолчанию 5; приёмка t05 — 15): (1) CPU контейнера `as-kafkaworker`
   ≤ 10% (бюджет чека; приёмка ≤5% ядра); (2) rdkafka-строк в логе ≤ 1/мин
   (после фикса — 0: лог на Debug); (3) число потоков процесса стабильно
   (±10, как 58-й); (4) лестница отработала: journal-фаза healing/portalloc
   восстановлен, кластер не в вечном «не закреплён в portalloc»; cleanup —
   del сида.

## 7. Критерии приёмки

Юнит-тесты (KafkaWorker.UnitTests):

1. **Кэш**: повторный `Create` того же ключа → тот же инстанс (счётчик
   `CreatedClients` не растёт); смена bootstrap/кредов → новый клиент,
   счётчик +1; unhealthy-пометка при Failed операции → следующий Create
   пересоздаёт; отмена (OCE) не инвалидирует; вытеснение по неактивности
   (TimeProvider сдвиг); Dispose при shutdown — детерминированный.
2. **Backoff**: `BackoffAfter` = 15/60/300/300…; `IsBlocked` до/после окна;
   `RecordSuccess` сбрасывает; `ForgetMissing` чистит исчезнувшие И сбрасывает
   счётчик (новая неудача после чистки — снова 15-с окно, не 60/300); гейт
   supervise — окно активно → проба не зовётся (фейк-фабрика фиксирует ноль
   вызовов) → probeBlind; writer-путь коллектора — фейл сбора → `IsBlocked`
  true, успех → false; гейт ActiveAsync (skip E–J) и коллектор —
   интеграционно в §7.5 (ActiveAsync — internal-агрегатор без швов: все
   зависимости, кроме converger'а, — конкретные классы; делать 9 интерфейсов
   ради одного if — отказ, churn-интеграция исполняет ActiveAsync
   хост-процессом).
3. **Лестница (3 ветки)**:
   - ветка 1: адрес в portalloc → пересоздание по закреплению, записей нет;
   - ветка 2: portalloc пуст, контейнеры живы, инспекция отдаёт (host,
     client-порт, advertised) → put-if-absent под клэймом (фейк etcd/txn),
     контейнеры НЕ пересозданы, брокеры остаются RUNNING, `version==0`
     проигрыш → re-read;
   - ветка 3: инспекция null → новая аллокация (фейк занятости: первый
     свободный), recreate, endpoints RMW = пересобранный список; клэйм занят
     → `waiting-portalloc-lock` (InProgress, без мутаций);
   - S7-инверсия: ошибка инспекции (docker молчит) → никаких мутаций, фейл
     тика;
   - гонка add-broker (ревью Ф7-1): PROVISIONING-брокер с живым контейнером
     и зрячей пробой, но БЕЗ адреса в endpoints → остаётся PROVISIONING;
     адрес появился (endpoints-RMW владельца) → следующий тик переводит
     RUNNING;
   - сходимость endpoints (ревью Ф7-4): расхождение endpoints с
     portalloc-адресами → RMW повторяется тиком; совпадение → ноль записей.
4. **Конфиг клиентов**: пины backoff присутствуют в AdminClientConfig
   (строится фабрикой; assert по свойствам конфига — без сети).

Интеграционные (KafkaWorker.IntegrationTests, динамические порта,
`KafkaClusterFixture`/`FreePortWindow` — литералов `:16000` нет):

5. **Churn на закрытых портах** (порт `KafkaProbeClosedPortsTests` t11): сид
   Active-кластера с endpoints на заведомо закрытые порты → воркер-цикл
   (хост-процесс фикстуры) за виртуальные ~4 мин: `CreatedClients` ≤ 2
   (первый клиент + не более одного пересоздания), а не 5–7/тик; поток
   стабилен (замер Thread count до/после); лог без rdkafka-шторма. Тот же
   файл — гейт Active-ветки: риг `KafkaClusterProcesses` (процессы
   фикстуры, как ProvisioningTests) на кластере в активном backoff-окне
   (без brokers-декларации — docker-часть надзора безвредна) → тик
   `ActiveAsync` = Success, `CreatedClients` не растёт (skip E–J).
6. **Подъём после утери portalloc** (образец ProvisioningTests): живой кластер
   фикстуры → del `/kafkaworker/portalloc/<C>` → тик надзора → portalloc
   восстановлен инспекцией живых контейнеров (ветка 2: прежние порты,
   пересозданий нет, брокеры RUNNING); затем del portalloc + снос контейнеров
   брокеров → тики воркера → новая аллокация + пересоздание + endpoints RMW
   (ветка 3) → DescribeCluster отвечает, брокеры RUNNING.

Стенд (приёмка roadmap):

7. Лежащий кластер ≥ 15 мин: CPU as-kafkaworker ≤ 5% ядра; ≤ 1 rdkafka-строки/
   мин; число потоков стабильно (чек 66 с `CHURN_MINUTES=15`).

Мерж-гейт (AGENTS.md — задача трогает код воркера и provisioning/portalloc):

8. `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
   -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` на свежем
   Release; полный прогон E2eFixture (изменение provisioning/portalloc-процессов);
   интеграционные серии KafkaWorker зелёные; чек 66 на стенде зелёный;
   roadmap-тег удалён тем же коммитом.

## 8. Ограничения / вне scope

- TLS/ACL и HTTP-транспорт API — t03-kafka-security; метрики — без новых серий
  (каркас t04 достаточен; `CreatedClients` — внутренний счётчик для тестов).
- Backoff не персистится в etcd и не виден панели (внутренняя политика
  частоты; внешнее состояние кластера — states/journal, как раньше).
- Гейт provisioning/deprovisioning backoff'ом — нет (бюджет `BrokerBootSec`);
  коллектор может пропускать лежащие кластеры — `KafkaCollectorStalled`
  поведение прежнее (консервативная свежесть, t04).
- PgWorker не трогается (лестница воркер-агностична по построению arch/17 —
  перенос в PgWorker отдельной задачей roadmap, если понадобится).
- Ротация пароля: старый клиент ключа OLD вытесняется по неактивности —
  отдельной invalidation-логики не требуется (другой ключ = другой клиент).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | no-op `DisposeAsync` на seam'е: будущий код может решить, что Dispose реален | контракт зафикирован в `IKafkaAdminClientFactory` + arch/16 §6; юнит-кэша проверяет sharability |
| R2 | Unhealthy-инвалидация на транзиентных таймаутах живого кластера → лишние пересоздания | пересоздание ≤ частоты операций (не тик), Dispose фон; чек 66 следит за CreatedClients/потоками |
| R3 | Гейт Active скрывает реальную аварию (кластер лежит, шаги тихо skip'аются) | supervise-проба остаётся источником backoff-факта: первая же проба после окна выполняется; journal/health циклов живые (`loops-alive`); слепая проба ≠ молчание (states не трогаем) |
| R4 | Лестница ветки 3 конфликтует с параллельным add-broker (оба аллоцируют) | глобальный клэйм `locks/portalloc` — обе секции взаимоисключены (S5); «не взял» → waiting, следующий тик |
| R5 | Реконструкция из inspect при «плавающем» published-порту (docker переназначил) | PortBindings — фактический канон; сверка с advertised env → journal-warning при расхождении |
| R6 | IAdminClient потокобезопасность при параллельном коллекторе | документированная потокобезопасность Confluent admin-операций; кластеры изолированы ключами; чек 66 следит за потоками |
| R7 | Deprecation-поведение Confluent Dispose (зависший поток) остаётся при вытеснении | Dispose только в фоне (Task.Run) — тик не блокируется; счётчик потоков в чеке 66 |

## 10. Открытые вопросы

Не блокируют написание плана — решения приняты по канонам и могут быть
переопределены ревью:

1. **Гейт коллектора метрик backoff'ом** (§3.2): принято «да» (единая политика
   kafka-контакта). Альтернатива — оставить коллектор долбить каждые 30 с
   (с кэшем это дёшево, но лог-warning'и сбора остаются).
2. **Базовая ступень шкалы = 15 с** (не ScanIntervalSec=5): принято по t11 —
   первое окно равно «естественному» интервалу kafka-циклов (TopicSync/
   Reassign = 15 с).
3. **Имя чека = 66**: следующий свободный номер после 65-metrics.
4. **`IdleEvictAfterMin = 10`**: константа без конфигурации (YAGNI; вытеснение
   не критично для корректности — только для гигиены ресурсов).
