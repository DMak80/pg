# t91-kafka-portalloc-race — глобальный portalloc-клэйм KafkaWorker (устранение гонки параллельного выделения портов)

- Дата: 2026-09-03
- Roadmap: `arch/roadmap/kafkaworker.md`, тег `t91-kafka-portalloc-race` (удаляется из
  roadmap тем же коммитом слияния — мерж-гейт проекта)
- Контракт (источник истины): `arch/15-kafka-clusters.md` §4, `arch/16-kafkaworker.md`
  §2.1 / §3.2 / §5 A / §5 F — обновляются по этому spec (arch/-first: сначала контракт,
  затем код)
- Эталон: t90 `PortAllocLock` PgWorker (мерж 21ca8b4) —
  `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`, контракт `arch/14-pgworker.md`
  §2.4/§3.3, тесты `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs` +
  `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`
- Worktree: `feat-kafka-portalloc-race`

## 1. Цель

Устранить гонку ПАРАЛЛЕЛЬНОГО выделения клиентских портов брокеров у KafkaWorker —
тот же класс гонки, что был у PgWorker до t90: два кластера KafkaWorker на одном
docker-хосте, засеянные одновременно (provisioning K1 / add-broker), читают занятость
ДО первой записи друг друга → одинаковые порты → контейнеры второго кластера падают
с «port is already allocated».

Отягчающее отличие от PgWorker на момент t90: у KafkaWorker чтение portalloc соседей
отсутствует ВООБЩЕ — `ProvisioningProcess.PlanAsync` читает занятость как
docker-биндинги (`driver.GetBusyPortsAsync`), `AddBrokerProcess.EnsurePortsAsync` —
как docker-биндинги ∪ записи СВОЕГО `/kafkaworker/portalloc/<C>`; префикс
`/kafkaworker/portalloc/*` чужих кластеров никто не читает. Поэтому одного клэйма
мало: кластер A записывает portalloc в K1, но контейнеры (docker-публикации портов)
создаются позже, в K3 — в этом окне кластер B, даже взяв клэйм после release A,
всё равно не видит портов A. Фиксу нужны ОБА компонента:

1. **Глобальный клэйм `/kafkaworker/locks/portalloc`** — сериализация секций
   довыделения между кластерами/инстансами (порт t90).
2. **Чтение занятости соседей** — под клэймом busy = docker-публикации ∪ записи
   portalloc ВСЕХ чужих кластеров (новый `PortAllocIndex`, аналог
   `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs` для kafka-формата записи);
   свой portalloc — не занятость, а закрепление (переиспользуется аллокатором).

Механизм (решения пользователя 2026-09-03, зафиксированы):

| Вопрос | Решение |
|---|---|
| Состав фикса | Клэйм + чтение portalloc соседей (полный фикс; «только клэйм» и «клэйм до конца K3» отклонены — оставляют окно K1→K3 / требуют долгой секции с keepalive) |
| Механизм клэйма | Буквальный порт t90: txn `version==0` + put-with-lease TTL 15 с, паттерн `/kafkaworker/leader`; без keepalive (секция короткая) |
| Охват | Обе точки довыделения KafkaWorker: ProvisioningProcess (K1 `PlanAsync`) и AddBrokerProcess (`EnsurePortsAsync`) |
| Формат portalloc / модель аллокации | Не меняются: `{"broker<k>":{"host":"h","client":16001}}`, «первый свободный» из `PortRange` |
| Тесты | Юнит (FakeEtcd) + интеграционный race-тест на реальном etcd (`EtcdFixture`, непересекающиеся порты); e2e с двумя kafka-кластерами — не требуется |

Ссылки на код (текущее состояние, ветка `feat-kafka-portalloc-race`):

- `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` — K1 `PlanAsync`
  (строки ~139–194): чтение pinned → docker-busy → allocate → txn put-if-absent.
- `src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs` — `EnsurePortsAsync`
  (строки ~92–150): чтение pinned → docker-busy ∪ свои закрепления → allocate →
  txn RMW по mod_revision.
- `src/KafkaWorker.App/Loops/ReconcileLoop.cs` — кластеры тикаются ПАРАЛЛЕЛЬНО
  (`SemaphoreSlim MaxClusters`) → локальный busy-гейт параллельных тиков своего
  инстанса обязателен (ревью-блокер t90).

## 2. Принципы

- **arch/-first**: правки контрактов `arch/15` §4 и `arch/16` §2.1/§3.2/§5 A/§5 F
  описаны в этом spec (§3.4) и применяются к файлам `arch/` в фазе execute ДО кода;
  код — отражение контракта.
- **Минимальность**: один новый ключ координации, два новых класса (`PortAllocLock`,
  `PortAllocIndex`); формат `/kafkaworker/portalloc/<C>`, модель «первый свободный»,
  placement и роли — без изменений; закрепления (rebuild, надзор, ротация,
  регенерация) работают как раньше.
- **Переиспользование примитива**: захват/освобождение — паттерн
  `ClaimStore.TryPutLeasedKeyAsync` (txn `NotExists` + put-with-lease TTL 15 с);
  отличие — короткая секция без keepalive и с явным release по завершении
  (порт `PortAllocLock` PgWorker 1:1).
- **Осознанный дубль кода**: `KafkaWorker.Etcd.Coordination.PortAllocLock` — копия
  PgWorker-класса со своим префиксом (`/kafkaworker/locks/portalloc`); дубли между
  воркерами — прецедент проекта (AGENTS.md, унификация — roadmap).
- **Тиковая модель**: не взял клэйм — не ошибка, а InProgress (журнальная фаза
  `waiting-portalloc-lock`, без мутаций); следующий тик (~5 с, `ScanIntervalSec`)
  повторяет. Никаких внутритиковых поллов и ожиданий захвата.
- **Ранние выходы вне лока**: полностью закреплённый portalloc (rebuild,
  переиспользование без записи) клэйма не требует — тики `waiting-brokers` (K4) не
  соперничают за глобальный клэйм (порт пред-выхода t90).
- **Панель не затронута**: новый подпрефикс `/kafkaworker/locks/` панелью не
  читается и не пишется (arch/15 §4: панель читает из `/kafkaworker/` только
  `rotations/`, `rebalances/`, `reassignments/`, `regens/` — перечень не меняется).

## 3. Структура / компоненты

### 3.1. PortAllocLock (новый класс, порт t90)

Расположение: `src/KafkaWorker.Etcd/Coordination/PortAllocLock.cs` (рядом с
`ClaimStore` — тот же примитив leased-ключа).

```csharp
public sealed class PortAllocLock(
    string[] endpoints, IEtcdGateway gateway, TimeProvider clock,
    string instanceId)
{
    public const string Key = "/kafkaworker/locks/portalloc";

    // Захват: локальный busy-гейт (DI-синглтон, параллельные тики своего
    // инстанса) → lease TTL 15 с → txn NotExists(Key) + put-with-lease.
    // true — держим; false — занят (другим инстансом ИЛИ параллельным тиком
    // этого же — НЕ ошибка); ошибка etcd → Result.Failed.
    public Task<Result<bool>> TryAcquireAsync(CancellationToken ct);

    // Освобождение: del под compare ValueEqual(наш value; lease истёк и лок
    // перехвачен — чужой ключ не трогаем) + revoke lease (best-effort).
    // Повторный вызов / без захвата — no-op.
    public Task ReleaseAsync();
}

// Маркер «клэйм занят — не фейл»: процесс возвращает InProgress
// (waiting-portalloc-lock), следующий тик повторяет.
public sealed class PortLockBusyException() : Exception(
    $"{PortAllocLock.Key}: занят другим инстансом — повторить следующим тиком");
```

Контракт ключа (arch/15 §4; полный аналог arch/14 §3.3):

- Ключ: `/kafkaworker/locks/portalloc`, lease TTL 15 с.
- Value: `{"instance":"<id>","since_unix":<unix>}` — `instance` = `ClaimStore.InstanceId`
  держателя (единый с клэймами/журналом — сквозная диагностика).
- Захват: txn `version==0` + put-with-lease (паттерн `/kafkaworker/leader`).
- Локальный busy-гейт: проверка живого захвата ДО etcd-раунда (поля
  `_lease`/`_payload` под `lock (_sync)`); второй конкурент того же инстанса
  получает `false` — НЕ reentrant-`true` (иначе обе секции concurrently читают busy
  и пишут portalloc — сама гонка t91; аргументация — комментарий ревью-блокера t90
  в `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`).
- Освобождение: явное по завершении секции — del под compare `ValueEqual(наш
  value)` + revoke lease. Смерть держателя → ключ гасит TTL ≤ 15 с → takeover
  следующим тиком без оператора.
- Без keepalive: критическая секция — единицы секунд ≪ TTL 15 с.
- Failover-обёртка по endpoints — паттерн `ClaimStore` (первый успешный выигрывает).

### 3.2. PortAllocIndex (новый класс, чтение занятости соседей)

Расположение: `src/KafkaWorker.Provisioning/Processes/PortAllocIndex.cs` (рядом с
потребителями; порт `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs`).

```csharp
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/kafkaworker/portalloc/";

    // Все клиентские порты каждой записи каждого ЧУЖОГО
    // /kafkaworker/portalloc/<C> — свой кластер исключает caller
    // (параметр exceptCluster): свой portalloc переиспользуется
    // аллокатором как закрепление, а не занятость.
    public Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(
        string exceptCluster, CancellationToken ct);
}
```

- Формат kafka-записи: `{"broker<k>":{"host":"h","client":16001}}` — ОДИН порт на
  ноду (упрощение тройки pg/patroni/doorman); парсинг — `JsonDocument`-цикл, как в
  `ReadPortAllocAsync` процессов.
- Битый JSON соседа — Warning-лог + skip ключа: чужой мусор не роняет наш
  provision (принцип PgWorker-индекса).
- Контракт занятости (arch/16 §2.1): **busy для довыделения = docker-публикации
  (чужие И своих соседей по кластеру: дубликат внутри кластера — такой же конфликт)
  ∪ portalloc чужих кластеров**; свой portalloc — закрепление.

### 3.3. Точки интеграции (два процесса)

Единый паттерн t90: лок покрывает всю секцию работы с кросс-кластерной картой
занятости — от чтения busy до записи portalloc включительно; ранние выходы
«менять нечего» — вне лока.

1. **ProvisioningProcess.PlanAsync** (K1, `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs`):
   - до лока: чтение pinned (`ReadPortAllocAsync`); ранний пред-выход «всё
     закреплено» (`wanted.All(existing.ContainsKey)` → журнал `planned` + возврат
     existing, БЕЗ чтений hosts/busy и БЕЗ txn-записи — переиспользование
     закреплений не соперничает за клэйм; тики waiting-brokers K4 идут мимо лока);
   - не взял лок (сбой захвата → обычный фейл-бэкофф; занят → `PortLockBusyException`)
     → `FinishAsync(cluster, "waiting-portalloc-lock")` → Result.Success
     (InProgress-семантика тика, аналог `waiting-brokers`), БЕЗ мутаций
     `/kafkaworker/portalloc/*`;
   - под локом (try/finally `ReleaseAsync`): `GetHostsAsync` → `GetBusyPortsAsync`
     (docker) → `PortAllocIndex.ReadBusyAsync` (чужие кластеры) → busy = docker ∪
     foreign → `PlacementPlanner.Plan` + `PortAllocator.Allocate(plan, existing,
     busy, PortFrom, PortTo)` → txn put-if-absent (`NotExists`; проигрыш → re-read
     соседа — идемпотентность сохранена) → фиксация ролей → журнал `planned`;
   - фиксация ролей (`brokers/<k>/role`) и журнал `planned` — внутри секции, до
     release (порт t90: `PlannedAsync` в try-блоке; секция короткая, поведение
     относительно текущего кода не меняется).
2. **AddBrokerProcess.EnsurePortsAsync** (`src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs`):
   - до лока: чтение pinned; ранний выход «недобора нет» (`missing.Count == 0` —
     уже существует, без клэйма и записи);
   - не взял лок → журнал `waiting-portalloc-lock` + Result.Success, без мутаций;
   - под локом (try/finally): `GetHostsAsync` → `GetBusyPortsAsync` →
     `ReadBusyAsync` (чужие) → taken = docker ∪ foreign ∪ СВОИ закреплённые адреса
     (свои живые ноды — не кандидаты для новых: дубликат внутри кластера — тоже
     конфликт; семантика текущего кода сохранена, добавлен foreign) →
     `PlacementPlanner.Plan(missing, …)` + `PortAllocator.Allocate` → txn RMW по
     `mod_revision` (проигрыш → ошибка-ретрай тиком, как сейчас);
   - фиксация ролей broker-only — как сейчас.

DI: регистрация в `src/KafkaWorker.App/Program.cs` —

```csharp
builder.Services.AddSingleton(sp => new PortAllocLock(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
```

`PortAllocLock` и `PortAllocIndex` прокидываются ctor-параметрами в
ProvisioningProcess и AddBrokerProcess (регистрации в Program.cs обновить).

### 3.4. Отражение в контракте (arch/-first; применяется в фазе execute)

1. `arch/15-kafka-clusters.md` §4 — новая строка таблицы координации:

   | `/kafkaworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** (t91, arch/16 §2.1): взаимоисключение секции довыделения клиентских портов «чтение занятости → выбор портов → запись `/kafkaworker/portalloc/<C>`» (provision K1 / add-broker) — пер-кластерные клэймы кросс-кластерную гонку не закрывают. Value: `{"instance":"<id>","since_unix":…}`. Захват txn `version==0` + put-with-lease; освобождение по завершении секции (del + revoke lease), смерть держателя — TTL. Не взял → InProgress (следующий тик). Без keepalive: секция короткая (единицы секунд ≪ TTL). |

   Перечень «панель читает только rotations/rebalances/reassignments/regens» не
   меняется — `locks/` панелью не читается.

2. `arch/16-kafkaworker.md` §2.1 (пункт «Placement/порты») — дополнение по образцу
   arch/14 §2.4 п.2:
   - занятость для довыделения = docker-публикации ∪ записи portalloc ВСЕХ чужих
     кластеров (`/kafkaworker/portalloc/*`, кроме своего — свой переиспользуется
     как закрепление; закрывает кросс-кластерную коллизию, включая окно «portalloc
     записан, контейнеры ещё не созданы»);
   - **глобальный portalloc-клэйм** (t91): довыделение новых портов (недобор
     нод, не переиспользование закреплений) — глобально взаимоисключающая секция
     «чтение занятости → выбор портов → запись», выполняется только держателем
     `/kafkaworker/locks/portalloc` (arch/15 §4; txn `version==0` + put-with-lease
     TTL 15 с). Не взял → InProgress (следующий тик ~5 с); смерть держателя гасит
     lease ≤ 15 с — takeover без оператора. Полностью закреплённый portalloc
     (rebuild, ранний выход без записи) клэйма не требует. Касается всех точек
     довыделения: provision K1, add-broker (§5 A/F).
3. `arch/16-kafkaworker.md` §3.2 (пишемые ключи) — строка
   `/kafkaworker/locks/portalloc` (когда: захват секции довыделения; значение —
   как arch/15 §4).
4. `arch/16-kafkaworker.md` §5 A (K1) — пометка «порт-аллокация под глобальным
   portalloc-клэймом (§2.1); не взял → journal waiting-portalloc-lock».
5. `arch/16-kafkaworker.md` §5 F (AddBroker) — пометка «добор портов под
   глобальным portalloc-клэймом (§2.1); занят → journal waiting-portalloc-lock».
6. `arch/roadmap/kafkaworker.md` — тег `t91-kafka-portalloc-race` удаляется тем же
   коммитом слияния ветки в main (мерж-гейт: из списка и из `←`-зависимостей;
   зависимостей у t91 нет).

### 3.5. Инвариант (для ревью)

**Любое чтение кросс-кластерной занятости с последующей записью
`/kafkaworker/portalloc/<C>` происходит под глобальным portalloc-клэймом, и эта
занятость включает записи portalloc чужих кластеров.** Взаимоблокировок нет: лок
один, вложенности нет; кластерный клэйм `<C>` удерживается независимо и не порождает
циклов ожидания (лока не берётся внутри чужой критической секции). Записи portalloc
вне секций довыделения не появляются: RemoveBroker (RMW-фильтрация — только удаляет
записи, под пер-кластерным клэймом) и Deprovisioning (del ключа) конфликтов портов
не создают — без лока, как у PgWorker.

## 4. Фазы реализации

1. **Контракт** — правки `arch/15` §4 и `arch/16` §2.1/§3.2/§5 A/§5 F по §3.4
   этого spec (arch/-first: до кода).
2. **PortAllocLock** — класс (порт t90 1:1, префикс `/kafkaworker/locks/portalloc`)
   + DI-регистрация; доработка `FakeEtcd`
   (`src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`): опциональный хук
   `TxnFault` (порт PgWorker-фейка) для теста «ошибка etcd → Failed»; юнит-тесты
   `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs` — порт набора t90:
   захват ок / второй инстанс false / release → повторный захват ок / повторный
   ReleaseAsync no-op / повторный TryAcquire тем же объектом false (busy-гейт
   параллельных тиков) / takeover не удаляет чужой ключ (ValueEqual) / сбой txn →
   Failed.
3. **PortAllocIndex** — класс + юнит-тесты: свой кластер исключается / чужие записи
   дают busy-кортежи (host, client) / битый JSON соседа — skip без ошибки.
4. **Интеграция в ProvisioningProcess** — пред-выход до лока, секция под локом,
   busy = docker ∪ foreign, `waiting-portalloc-lock`; юнит-тесты: клэйм занят
   (FakeEtcd с засеянным `/kafkaworker/locks/portalloc`) → фаза
   `waiting-portalloc-lock`, мутаций portalloc нет, драйвер-фэйк не звался; клэйм
   взят → обычный путь K1 (включая ранний выход «всё закреплено» без записи);
   allocation учитывает чужую запись соседа (засеянный
   `/kafkaworker/portalloc/<другой C>` → выделенный порт не совпадает).
5. **Интеграция в AddBrokerProcess** — те же инварианты: занят → журнал
   `waiting-portalloc-lock` без мутаций; взят → добор с учётом foreign-busy.
6. **Интеграционный race-тест** — `src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`
   на существующей лёгкой `EtcdFixture` (etcd-only, `assignRandomHostPort: true`;
   порт t90-теста): две параллельные мини-секции «ReadBusy(префикс portalloc минус
   свой кластер) → PortAllocator.Allocate(1 нода, 1 порт) → Put» под клэймом
   (ретрай-цикл «занят → пауза 200 мс», бюджет 10 с; барьер одновременного старта)
   → порты двух кластеров НЕ пересекаются; ключ лока исчезает после release обеих.
   Диапазон портов в тесте — значения в etcd (не host-биндинги), литералы
   допустимы.
7. **Roadmap-гейт** — удалить тег `t91-kafka-portalloc-race` из
   `arch/roadmap/kafkaworker.md` тем же коммитом, что и слияние ветки (§3.4 п.6).
8. **Прогон** — `dotnet build` + `dotnet test` всего решения
   (TreatWarningsAsErrors=true), устранение замечаний.

## 5. Ограничения и не-цели

- PgWorker не трогается (t90 уже в main); общая библиотека клэймов не выделяется —
  осознанный дубль (унификация — roadmap).
- Формат `/kafkaworker/portalloc/<C>` и модель «первый свободный» не меняются
  (порты не фрагментируются, wrap-логика не появляется); placement, роли KRaft,
  env-генерация — без изменений.
- Лок один глобальный (не per-host/per-диапазон): multi-host сериализация
  избыточна, но редка и коротка — per-host локи YAGNI (порт решения t90).
- Лок удерживается только на секцию планирования портов; ensure app-секрета (K2),
  создание контейнеров (K3), ожидание готовности (K4), converge (K5) — вне лока.
- RemoveBroker (portalloc-фильтрация), Deprovisioning (del ключа), NodeSupervisor /
  NodeRegenerator / AppPasswordRotator (только читают закрепления) — не берут лок:
  не довыделяют портов.
- Поведение панели не меняется; e2e-стенд с двумя реальными kafka-кластерами,
  админ-UI — вне scope.
- Массовый параллельный посев N кластеров растягивается на N тиков (~5 с на
  кластер) — осознанный компромисс t90.

## 6. Риски

- **Двойное владение** (держатель завис > TTL 15 с, лок перехвачен, старый
  дописал): окно теории; секция ≪ TTL, страховка — docker-busy в карте занятости
  (контейнеры первого кластера видны второму) и идемпотентность txn
  put-if-absent/RMW.
- **Шум журнала** `waiting-portalloc-lock` при параллельном посеве —
  наблюдаемость (не ошибка), гаснет первым успешным тиком.
- **Отказ etcd при release** — лок живёт до TTL 15 с, конкурент ждёт 1–3 тика:
  деградация доступна, корректность сохранена.
- **Битый portalloc соседа** — Warning + skip (чужой мусор не роняет наш provision);
  фактическая занятость docker остаётся второй линией защиты.

## 7. Критерии приёмки

1. `arch/15` §4 содержит строку `/kafkaworker/locks/portalloc`; `arch/16` §2.1
   описывает глобальный клэйм и полную карту занятости (docker ∪ portalloc чужих
   кластеров); §3.2/§5 A/§5 F отражают клэйм; правки применены в ветке ДО кода
   (arch/-first).
2. `PortAllocLock` реализует захват txn `version==0` + put-with-lease TTL 15 с,
   локальный busy-гейт параллельных тиков своего инстанса (повторный TryAcquire —
   false) и освобождение del-under-`ValueEqual` + revoke lease.
3. ProvisioningProcess (K1) и AddBrokerProcess выполняют довыделение портов только
   под клэймом; ранние выходы «закреплено/недобора нет» — до лока; «не взял» →
   журнальная фаза `waiting-portalloc-lock` + Result.Success, без мутаций
   `/kafkaworker/portalloc/*`.
4. Под клэймом карта занятости включает записи `/kafkaworker/portalloc/*` чужих
   кластеров (`PortAllocIndex`): кластер, сеемый после записи соседа, но ДО
   создания его контейнеров, получает непересекающиеся порты (окно K1→K3 закрыто).
5. Юнит-тесты: захват/занятость/release/ошибки лока (AAA-комментарии); инварианты
   двух процессов при занятом клэйме; allocation с учётом чужой portalloc-записи.
6. Интеграционный race-тест (EtcdFixture, реальный etcd, динамический порт): две
   параллельные критические секции дают непересекающиеся порты; ключ лока исчезает
   после release обеих.
7. `dotnet build` и `dotnet test` решения зелёные (TreatWarningsAsErrors=true);
   хост-порты в тестах динамические (Testcontainers `assignRandomHostPort`) —
   литералов вида `:16000` в expects нет.
8. Тег `t91-kafka-portalloc-race` удалён из `arch/roadmap/kafkaworker.md` тем же
   коммитом слияния (мерж-гейт проекта).
