# Спецификация: fix-provision-portalloc-alerts — самолечение provision (порты/бэкофф) и честный алертинг панели

Дата: 2026-09-01. Фаза dev-flow: spec. Источники: `arch/14-pgworker.md`
(канон — обновлён этой задачей: §2.4 занятость портов portalloc всех
кластеров, §5 A P1 усыновление фактических портов + P2.1 сверка портов
контейнера + бэкофф ретраев, §3.3 поля ретраев `/pgworker/work/<C>`,
§8 пороги), `arch/adminpanel/02-etcd-contract.md` (§2.3.1 панель читает
`/pgworker/work/` + опрос `/healthz`, §3 модель `WorkJournalInfo`/
`WorkerHealth`, §4 тик), `arch/adminpanel/03-panels.md` §4 (эскалация
`cluster-not-initialized`, новые kind `provision-stuck`/`worker-unhealthy`),
`arch/09-troubleshooting.md` §11 (сироты `pgw-*`), диагностика живого
стенда 2026-09-01 (main-агент; не переоткрывалась). Расширение задачи
(2026-09-01, директива пользователя «воркер — хозяин: смотрит что запущено
реально, сопоставляет, чинит и провижинит — ВСЁ САМ»): arch/14 дополнен
(§5 автономный reconcile, P1-перепланирование занятых, P2.2 identity-проба +
лечение HA-scope, §5 J AD2'-инвариант Active, §8 комментарий PatroniBootSec,
§9 R11/R12); источники — факты живой верификации после 2182372 (воркер
healthy, бэкофф работает, adoption-фильтр работает; вскрыты Д1–Д3).

---

## 1. Цель

Дефект живого стенда: кластеры `canon10`, `smoke` вечно
`state=NOT_INITIALIZED`, ноды `PROVISIONING` ~2 часа, Patroni обоих кластеров
полностью здоров. Корень: portalloc в etcd (canon10=15014–15019,
smoke=15014–15017 — коллизия!) расходится с фактическими docker-портами
контейнеров (canon10=15004–15009, smoke=15000–15003); старый portalloc v1
исчез (create_revision новых ключей 578/651 моложе клэймов 575; вероятный
источник — пересев чеков `15-cluster-create.sh` без чистки portalloc или
гонка параллельных окон). После потери `PlanPortsAsync` построил новый план
из docker-busy, `EnsureNodeAsync` идемпотентен только по имени (контейнеры
не пересозданы), `WaitPatroniAsync` бьёт в мёртвые порты 18014+ → бюджет
600 с → вечный фейл каждым тиком (234 одинаковых отказа за 10 минут). Панель
всё это время молчит о главном: только info `cluster-not-initialized` без
эскалации, `last_error` из `/pgworker/work/*` не читает вовсе,
`/healthz=503` воркера (docker unhealthy 96 проб) невидим.

Закрыть девять дыр (план пользователя 2026-09-01 + расширение «воркер —
хозяин», та же дата):

- **A. Усыновление фактических портов в provision** — `PlanPortsAsync`
  (`src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs:194`) при
  пустом/неполном/расходящемся portalloc инспектирует живые канонические
  контейнеры кластера (`driver.InspectNodesAsync`, паттерн AD2 из
  `AdoptionProcess.cs`) и берёт каноном фактические порты контейнеров, а не
  новые. После фикса живой стенд самолечится тиками воркера.
- **B. Сверка портов в `EnsureNodeAsync`** — идемпотентность по имени
  (`src/PgWorker.Docker/Drivers/ClusterDriver.cs:124-129`) дополняется
  сверкой фактических public-биндингов контейнера с планом; расхождение →
  пересоздание контейнера (для PROVISIONING-нод безопасно, данных нет).
- **C. Аллокатор портов видит соседей** — busy-множество
  (`src/PgWorker.Core/Planning/PortAllocator.cs`, вызовы в
  `ProvisioningProcess.PlanPortsAsync` и `AddShardProcess.PlanShardPortsAsync`)
  пополняется всеми записями `/pgworker/portalloc/*` из etcd, а не только
  docker-публикациями (закрывает кросс-кластерную коллизию smoke/canon10;
  сужает roadmap `t90-portalloc-parallel-race` до параллельной гонки).
- **D. Честный алертинг панели** — (а) эскалация `cluster-not-initialized`
  по возрасту до Warning; (б) новое правило `provision-stuck` по
  `/pgworker/work/<C>` (`last_error` + возраст серии фейлов) — снапшот панели
  учится читать work-ключи; (в) `worker-unhealthy` — панель опрашивает
  `/healthz` живых инстансов воркера (уже ходит в его API) и алертит
  degraded до истечения lease-ключей.
- **E. Бэкофф ретраев зависшего provision** — вместо 234 одинаковых фейлов
  за 10 минут: экспоненциальная задержка ретраев (5 с → 60 с, кап),
  переносимая в `/pgworker/work/<C>` (переживает рестарт воркера, видна
  панели), сброс при прогрессе; исчерпание бюджета Patroni сбрасывает трекер
  бюджета — каждая новая попытка получает полный бюджет.
- **F. Уборка черепков `pgw-solo-*`** — процедура диагностики и ручной
  уборки контейнеров без etcd-деклараций документируется (arch/09 §11);
  чек `15-cluster-create.sh` чистит и `/pgworker/portalloc/smoke` при
  пересеве. Руками стенд не трогаем (только по приказу).
- **G. Автономный reconcile «воркер — хозяин» (Д1–Д3, расширение)** —
  живая верификация после 2182372 вскрыла три дыры автономии:
  - **Д1 — коллизия портов при Ensure = вечный цикл**: portalloc кластеров
    пересекался (наследие инцидента); create контейнера падал на занятом
    чужим host-порте, а «закреплено и переиспользуется» (wanted ⊆ existing)
    не давал перепланировать → вечный фейл-цикл; контейнеры шарда исчезали
    (stop+rm прошёл, create упал), а probes по коллизионным портам попадали
    в ЧУЖОЙ Patroni → фальш-RUNNING/фальш-dsn. Требование: занятость
    запланированного порта чужим фактом (docker-биндинг минус свои
    контейнеры ∪ portalloc соседей) → нода перепланируется на свободные,
    portalloc/dsn обновляются, контейнер создаётся в том же тике; проба
    обязана идентифицировать НАШУ ноду (REST `/patroni`: scope+name) —
    чужой ответ ≠ success.
  - **Д2 — фальш-Active не репарируется**: smoke стал Active с dsn на чужие
    порты и вечно unreachable; AdoptionProcess сверяет только недостающие
    ноды, не portalloc/dsn с фактом. Требование: инвариант каждый тик для
    Active — portalloc = факт живых канонических контейнеров (merge как в
    P1, тот же фильтр кластера), dsn пересобирается из фактического
    portalloc; расхождение — репарация с журналом. Никаких вечных
    «unreachable».
  - **Д3 — Patroni-ломка не лечится**: после утраты данных (volume пуст)
    ноды вечно «waiting for leader to bootstrap» при живом initialize-ключе.
    Требование: WaitPatroni при исчерпании бюджета лидера И доказанной
    утрате данных ВСЕХ нод scope (docker-exec `test -f PG_VERSION`:
    Present/Absent/Unknown) → чистка HA-scope (initialize/leader/sync/
    optime//members/; request_* не трогаем) → Patroni бутстрапится заново;
    данные есть хоть у одной ноды → НЕ лечить, журнал-фейл «разбор
    оператора»; Unknown → ждать. Одна чистка на scope за бюджет; всё в
    журнал (arch/14 R11).

Зачем: без этого воркер — не хозяин (живые контейнеры игнорирует, вечный
ретрай без прогресса, фальш-ACTIVE и мёртвые HA-scope не репарирует), а
панель — не наблюдатель (реальная проблема невидима, оператор приучён к
молчанию панели).

Ключевой критерий: **живой стенд НЕ перезапускается** — после деплоя фикса
воркер самолечит canon10/smoke тиками (пересоздание контейнеров не
требуется: после merge портов Patroni-пробы сходятся на фактических портах —
контейнеры не трогаются вовсе).

---

## 2. Принципы

1. **Факт над записью (в PROVISIONING-фазе).** Живой канонический контейнер
   `pgw-<C>-<X>-<n>` — положительное свидетельство; portalloc — лишь след
   плана. При расхождении каноном становятся фактические порты контейнера
   (данные уже в volume, Patroni жив — нулевой даунтайм). Отступление от
   AD2-канона усыновления (`AdoptionProcess` существующие записи не
   перезаписывает) осознанное: там Active-кластеры с внешними нодами, здесь —
   собственный провижининг без данных. Перезаписываются только записи без
   `object`.
2. **План побеждает контейнер (когда факта нет).** Если контейнер канонической
   ноды существует с чужими портами, но инспекция его не опознала (swarm,
   неоднозначность), `EnsureNodeAsync` пересоздаёт контейнер по плану — фаза
   PROVISIONING, данных нет, volume сохраняется.
3. **Занятость = docker ∪ portalloc всех кластеров.** Порт считается busy,
   если он опубликован живым контейнером ИЛИ записан в чей-то
   `/pgworker/portalloc/*` (кроме своего — свой переиспользуется как
   закрепление). Битый JSON соседа — skip ключа с journal-заметкой, не роняет
   provision (чужой мусор не блокирует наш подъём).
4. **Алерт = реальная проблема.** Эскалации и новые kind отражают то, что
   воркер уже знает (`last_error`, `/healthz`): текст ошибки — оператору,
   движитель — `WorkerAuto`/`OperatorRunbook` по каталогу 03 §4.1. Вечный
   алерт класса WorkerAuto = дефект воркера (фиксится пунктами A–E).
5. **Бэкофф — в журнале, не в памяти.** Серия фейлов живёт в
   `/pgworker/work/<C>` (переживает рестарт, видна панели и оператору);
   сбрасывается успехом, а не временем. Бэкофф никогда не блокирует
   самолечение: новая конфигурация (деплой фикса) чинится первой же
   попыткой после истечения текущей задержки (кап 60 с).
6. **arch/-first.** Контракты (arch/14, arch/adminpanel/02/03, arch/09)
   обновлены до кода в этом же ветвлении; код ниже повторяет контракт, а не
   наоборот.
7. Общие базовые правила: .NET 10, `Nullable=enable`,
   `TreatWarningsAsErrors=true`, centralized packages; тесты — AAA-нотация
   в комментариях; документация — русский, идентификаторы — английские.

---

## 3. Структура / компоненты

### 3.1. PgWorker: `PlanPortsAsync` — усыновление фактических портов (пункт A)

`src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs`,
`PlanPortsAsync` (P1). Новый порядок:

```
1. existing = ReadPortAllocAsync(<C>)                       // как сейчас
2. wanted = все ключи "<shard>/<node>" декларации           // как сейчас
3. НОВОЕ — усыновление факта (ВЫПОЛНЯЕТСЯ ВСЕГДА, не только при неполном
   portalloc — расхождение нельзя узнать без инспекции, а полный portalloc
   может быть расходящимся: потерян и выделен заново; ровно состояние живого
   стенда canon10/smoke):
   names = wanted.Select(w => w.Split('/')[1])              // имена нод
   discovered = driver.InspectNodesAsync(names)
   по каждой находке (key = "<shard>/<node>" из wanted по имени ноды):
     - guard: DiscoveredNode.Object == $"pgw-{cluster}-{shard}-{node}"
       (канонический контейнер нашего провижининга; чужой/неоднозначный
        матчинг — skip c journal-заметкой "adopt-skipped")
     - guard: Pg > 0 && Patroni > 0 (частичная публикация не канон)
     - guard: запись existing не имеет object (object-записи не трогаем)
     - факт = NodeAddress(Host, NodePorts(Pg, Patroni, Doorman)) БЕЗ object:
       записи нет → добавить (changed); запись есть и равна факту → не пишем;
       запись есть и расходится → перезаписать фактом (changed)
4. ранний выход (идемпотентность): wanted ⊆ existing И после merge
   ничего не изменилось → Planned БЕЗ записи portalloc
5. wanted ⊆ existing, но merge изменил записи → записать portalloc и Planned
6. остался недобор (ноды без контейнера и без записи) → прежний путь:
   hosts + busy (busy = docker ∪ portalloc соседей, п. 3.3) +
   PlacementPlanner + PortAllocator.Allocate + merge
7. запись portalloc:
   - ключа не было → txn compare NotExists + put (как сейчас)
   - ключ был (частичный/расходящийся) → put (read-modify-write под
     клэймом — паттерн AddShard A2 / AdoptionProcess AD2)
```

Механика инспекции — существующий `PlainClusterDriver.InspectNodesAsync`
(матчит hostname/alias; наши контейнеры несут `Hostname=nodeName`,
`NetworkAliases=[nodeName, pgw-…]` из `BuildSpec`) → `NodeMatcher.Match`.
Swarm-драйвер возвращает пусто — merge тихо пропускает, работает п. B
(ограничение §5). InspectNodesAsync вызывается КАЖДЫЙ тик provision (стоимость
— list+inspect на docker-хост; тики provision живут только пока кластер
NOT_INITIALIZED); для здорового сходящегося кластера шаг 4 даёт выход без
записи — лишних мутаций etcd нет.

`AddShardProcess.PlanShardPortsAsync` НЕ усыновляет (домен — Active-кластер,
там усыновлением занимается `AdoptionProcess`; см. ограничения §5).

### 3.2. PgWorker: `EnsureNodeAsync` — сверка портов контейнера (пункт B)

`src/PgWorker.Docker/Drivers/ClusterDriver.cs`,
`PlainClusterDriver.EnsureNodeAsync` (:108). Существующий блок
идемпотентности (:124-129 «контейнер есть → return») заменяется:

```
существующий контейнер по имени найден (ListContainers all:true):
  если addr.Object != null → return (усыновлённые не трогаем, R9)
  inspect = engine.InspectContainerAsync(container.Id)
  ожидаемые биндинги: 5432→addr.Ports.Pg, 8008→addr.Ports.Patroni,
                      6432→addr.Ports.Doorman (только enableDoorman)
  фактические = inspect.Ports (ContainerPort→HostPort)
  расхождение = хоть один ожидаемый биндинг отсутствует ИЛИ ведёт на другой
                HostPort
  расхождение → StopContainer(name, 10) + RemoveContainerAsync(name, force)
                → продолжить обычным путём (CreateContainer + Start);
                volume НЕ трогаем (данные PROVISIONING-фазы не важны,
                но и не вредны — Patroni поднимется с теми же данными)
  совпадение → return (как сейчас)
```

Безопасность вызывавших путей (проверено по коду): `ProvisioningProcess`
и `AddShardProcess` зовут `EnsureNodeAsync` только для нод
`state != RUNNING` (PROVISIONING/NOT_INITIALIZED — данных нет);
`NodeSupervisor` — только после `RemoveNodeAsync` (rebuild/recreate) или
при отсутствии контейнера (сверка декларации). Живые RUNNING-контейнеры
через `EnsureNodeAsync` не проходят — снести данные невозможно. Swarm-режим
сверки не получает (ограничение §5).

Вместе с A: в здоровом сценарии merge (3.1) выравнивает portalloc до факта →
сверка (3.2) сходится → контейнеры вообще не пересоздаются. B — вторая линия
обороны для сценариев, где инспекция не нашла контейнер (swarm, кривые алиасы
алиасы): контейнер пересоздаётся по плану.

### 3.3. PgWorker: busy-множество из etcd portalloc (пункт C)

Новый DI-сервис `PortAllocIndex` (namespace
`PgWorker.Provisioning.Endpoints`, рядом с `ShardEndpoints`; зависимости
`IEtcdGateway` + endpoints, failover-перебор — паттерн процессов):

```
Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(
    string exceptCluster, CancellationToken ct)
  range /pgworker/portalloc/ → по каждому kv:
    leaf = key без префикса; leaf == exceptCluster → skip
    Portalloc.Parse: битый JSON → journal-заметка ("битый portalloc соседа
      <leaf>: …"), skip ключа (не роняет provision)
    каждая запись → добавить (host, pg), (host, patroni), (host, doorman)
      (doorman=0 у object-нод — не добавлять, 0 не бывает публичным)
```

Вызовы: `ProvisioningProcess.PlanPortsAsync` и
`AddShardProcess.PlanShardPortsAsync` — вместо голого
`driver.GetBusyPortsAsync`:

```
busy = dockerBusy ∪ PortAllocIndex.ReadBusyAsync(<C>)
```

Свой portalloc НЕ входит в busy (он — `existing`, переиспользуется
аллокатором); записи соседей закрывают последовательную кросс-кластерную
коллизию (второй кластер видит закрепления первого). Гонка параллельных
окон (оба читают до первой записи) остаётся — roadmap
`t90-portalloc-parallel-race` (сужен этой задачей до параллельной гонки,
см. arch/roadmap/pgworker.md).

### 3.4. Панель: чтение work-журналов + 3 алертных правила (пункт D)

**D1. Снапшот читает `/pgworker/work/`.**
`src/AdminPanel.Etcd/SnapshotRefresher.cs`: новый range
`Prefixes.PgWorkerWork = "/pgworker/work/"` (транспортный провал роняет тик —
как остальные KV). Новый парсер `WorkJournalParser`
(`src/AdminPanel.Etcd/Parsing/`): чистая функция `IReadOnlyList<Kv> →
WorkJournalInfo[]` + ошибки; толерантность — битый JSON → ParseError-запись
(алерт `key-malformed`), ключ не свой — skip. Модель
(`src/AdminPanel.Core/`):

```csharp
sealed record WorkJournalInfo(
    string Cluster, string Op, string Phase, string Instance,
    long UpdatedUnix, string? LastError,
    int? FailCount, long? FailFirstUnix, long? RetryNotBeforeUnix);
```

`EtcdSnapshot` получает `IReadOnlyList<WorkJournalInfo> PgWorkerWork`
(после `PgWorkerEndpoints`); `SnapshotBuilder.Build` — новый параметр;
`FailTick` переносит `previous?.PgWorkerWork ?? []`. `Cluster` — leaf ключа.

**D2. Эскалация `cluster-not-initialized` (а).**
`ClusterNotInitializedRule`: baseline возраста =
`cluster.CreatedUnix ?? sinceUnix(prev-алерта) ?? now`; при
`now − baseline > AlertsOptions.NotInitializedWarnSec` (default 900 c —
заведомо больше PatroniBootSec=600) severity Warning, иначе Info (как
сейчас); id/Target не меняются (эскалация не мутирует id — `SinceUnix`
 — AlertEngine переносится). `AlertsOptions` += `NotInitializedWarnSec=900`.

**D3. Новое правило `provision-stuck` (б).**
`Rules/ProvisionStuckRule` (kind `provision-stuck`, severity Warning,
Remedy `WorkerAuto`): для каждой `WorkJournalInfo` с
`LastError != null` и `now − FailFirstUnix > AlertsOptions.ProvisionStuckSec`
(default 300 c): Message несёт `LastError` (обрезка до разумной длины),
Details: `fail_count`, `op`, `phase`, `updated_unix`,
`retry_not_before_unix`. Id `provision-stuck:<C>`. Кластер при этом обычно
`NOT_INITIALIZED` — но правило не фильтрует по state (журнал живёт и при
прочих op; правило смотрит `op=="provision"` — см. §3.5: поля серии пишутся
только provision-процессом). Возраст серии, а не последней записи: серия
фейлов живёт в журнале с первого фейла до успеха (§3.5) — миганий нет.
`AlertsOptions` += `ProvisionStuckSec=300`.

**D4. `worker-unhealthy` (в) — опрос `/healthz`.**
Новые файлы в `src/AdminPanel.Etcd/Workers/`:

- `WorkerHealthStore` (`IWorkerHealthStore`: `Current`, `Replace` — паттерн
  `ProbeResultsStore`): `IReadOnlyList<WorkerHealth>`;
- `WorkerHealthPoller` (`BackgroundService`, `IHostedService`): раз в
  `WorkerApiOptions.HealthIntervalSec` (default 15) по каждому живому
  `PgWorkerEndpoints` (из `ISnapshotStore.Current`): `GET <url>/healthz`
  (HttpClient-фабрика `"workers"`, без `X-Api-Key` — middleware воркера
  `/healthz` не трогает); 200 → Healthy, 503 → Degraded, сетевой сбой/таймаут
  → Unreachable. Ядро тика `RunOnceAsync` — публично для тестов (прецедент
  `RefreshOnceAsync`/`ProbeOrchestrator.RunOnceAsync`).
- модель `WorkerHealth`/`WorkerHealthStatus` — в `AdminPanel.Core`
  (контракт 02 §3).

`SnapshotRefresher` вносит `WorkerHealthStore.Current` в снапшот после
сборки (образец `ProbeEnricher.Apply`); `FailTick` — прежние значения.
Опции: `WorkerApiOptions` += `HealthEnabled=true`, `HealthIntervalSec=15`
(секция `AdminPanel:Workers`).

`Rules/WorkerUnhealthyRule` (kind `worker-unhealthy`, severity Warning,
Remedy `OperatorRunbook`): по каждому `WorkerHealth.Status != Healthy` →
Alert target `pgworker/<instanceId>`, Message с Detail (у Degraded —
строка-причина из health-секций воркера, у Unreachable — сетевая ошибка).
Hint: lease жив, но процесс нездоров; docker-healthcheck гасит контейнер →
lease-ключ вот-вот исчезнет — тогда эстафета уходит `worker-api-unreachable` (critical).
Инстанс Healthy → алерта нет.

KafkaWorker в опрос не входит (домен-снапшот отдельный; симметрия —
тривиальное расширение позже, см. ограничения §5).

UI: новых экранов нет — kind появляются в существующем списке алертов
(severity-бейджи Info/Warning/Critical уже умеют); фронтенд не меняется.

### 3.5. PgWorker: бэкофф ретраев provision (пункт E)

**E1. Формат журнала.** `WorkState`
(`src/PgWorker.Etcd/Coordination/WorkJournal.cs`) += optional-поля
`fail_count` (int?), `fail_first_unix` (long?),
`retry_not_before_unix` (long?) — JSON-имена
`fail_count`/`fail_first_unix`/`retry_not_before_unix` (контракт arch/14
§3.3). Семантика серии: пишутся фейлом, ПЕРЕНОСЯТСЯ записями фаз внутри
серии (InProgress-фазы провижининга не стирают контекст неудачи), сбрасываются
`Done` (и началом другого op). `WritePhaseAsync` получает необязательный
параметр-контекст серии; отдельный метод не нужен.

**E2. ProvisioningProcess.** В начале `TickAsync` (после клэйм-гварда и
guard'а декларации, до P0): `journal.ReadAsync` → если
`op=="provision" && RetryNotBeforeUnix > now` → `return InProgress` БЕЗ
записи журнала (skip тика; снапшот панели и так несёт серию). Прочитанный
контекст (`fail_count`, `fail_first_unix`) передаётся во все
`WritePhaseAsync` этого тика (перенос серии). `FailAsync`:

```
n        = серия жива (прочитанный в начале тика fail_count != null,
           op тот же) ? past_fail_count + 1 : 1
delay    = min(ProvisionRetryBaseSec · 2^(n−1), ProvisionRetryMaxSec)   // 5,10,20,40,60,60…
first    = n == 1 ? now : past_fail_first_unix
пишем:   last_error=ex.Message, fail_count=n, fail_first_unix=first,
         retry_not_before_unix=now+delay
```

Счётчик считается «подряд идущим» без разбора текста ошибки (простота >
точность; transient-ошибки, чинящиеся следующим тиком, серию не продолжают —
успех/`Done` сбрасывает всё, смена op начинает новую серию). `Finish`
(success/Done) пишет фазы без серии (сброс).

**E3. Сброс трекера бюджета Patroni.** `WaitPatroniAsync` (:298): при
возврате бюджет-фейла — `_patroniWaitSince.TryRemove(scope)` ДО возврата
ошибки (сегодня запись живёт вечно → каждый следующий тик фейлится
мгновенно, без нового бюджета). Симметрично в `AddShardProcess` (:287) —
тот же скопированный паттерн. Backoff-skip (E2) при этом НЕ применяется к
AddShard (см. ограничения §5).

**E4. Опции.** `ThresholdsOptions` += `ProvisionRetryBaseSec=5`,
`ProvisionRetryMaxSec=60` (appsettings + arch/14 §8). `PlacementOptions`
(IClusterProcess.cs) += эти два поля, проброс в `ProvisioningProcess` из
`Program.cs` (:142).

### 3.6. Черепки pgw-solo-* и пересев чеков (пункт F)

- `arch/09-troubleshooting.md` §11 (добавлен этой задачей): диагностика
  (docker ps + сверка деклараций etcd), процедура ручной уборки (по одной
  команде на имя, только по приказу оператора), предупреждение о
  running-кластерах. Код НЕ трогает `pgw-solo-*` (без деклараций воркер их
  не видит — Deprovisioning D1 чистит сироты только в рамках
  TO_REMOVE-кластера).
- `dev-stand/adminpanel/checks/15-cluster-create.sh`: блок чистки (строки
  ~20-23) дополняется `ect del /pgworker/portalloc/smoke` — пересев чека
  больше не оставляет порт-закреплений прошлого прогона (деклараций нет —
  portalloc уже никем не читается, а новый прогон с фиксами A/B/C корректен и без
  чистки; чистка убирает мусор и исторические коллизии).
- `arch/roadmap/pgworker.md` `t90` — сужен этой задачей (см. §3.3).

### 3.7. Автономный reconcile «воркер — хозяин» (пункт G: Д1–Д3)

Директива: воркер СНАЧАЛА смотрит что запущено реально и сопоставляет,
ПОТОМ чинит/достраивает и провижинит. Сам. Всегда.

**Д1 — перепланирование занятых портов (provision, P1).**
`ProvisioningProcess.PlanPortsAsync` после adoption-merge (§3.1) добавляет
шаг сверки занятости:

```
dockerBusy = driver.GetBusyPortsAsync()                  // все публикации хоста
selfFact   = порты фактических контейнеров СВОЕГО кластера (adoption-находки)
foreign    = (dockerBusy − selfFact) ∪ PortAllocIndex.ReadBusyAsync(<C>)
для каждой записи existing, НЕ подтверждённой фактом своего контейнера:
   пересечение её портов (pg/patroni/doorman) с foreign → запись снимается
   (detach; object-записи не трогаем) — changed=true
далее прежняя логика: полный && !changed → выход без записи; иначе
PortAllocator.Allocate недобора (переиспользует валидные, выделяет свободные)
→ CommitPortAlloc → EnsureNode создаёт контейнер В ТОМ ЖЕ ТИКЕ
```

Гонка «порт заняли между P1 и create» → честный фейл тика → следующий тик
видит занятость в docker-факте → перепланирует (самолечение ≤ 2 тиков).
Detach-логика — чистая функция `PortPlanConvergence.DetachColliding(existing,
selfFact, foreign)` в `PgWorker.Core.Planning` (переиспользуется Д2).
dsn строится далее из обновлённого portalloc (P2.5 уже так делает).

**Д1б — probe-идентификация своей ноды (P2.2).** `ShardProbe` +=
`IdentifyAsync(NodeAddress) → Result<NodeIdentity?>` (GET `/patroni` —
Patroni отдаёт `{"scope":…,"name":…,"state":…,"role":…}`; битый JSON/сбой →
null). `WaitPatroniAsync` заменяет `IsAliveAsync` на идентификацию: нода
жива ⟺ `identity.Scope == "<C>-<X>" && identity.Name == <n>`. Чужой ответ по
коллизионному порту = не наша нода → ожидание → бюджет → фейл (и Д1 уже
перепланировал порты). Надзор (`NodeSupervisor`) не меняется: после Д1
коллизии канонических нод невозможны (граница — §5).

**Д2 — инвариант адресов Active (AdoptionProcess, каждый тик).** В
`AdoptionProcess.TickAsync` после AD1-чтения portalloc — блок «AD2'»:

```
кандидаты = ноды всех dsn-шардов (nodes-ключи ∪ HA-members)
инспекция driver.InspectNodesAsync (тот же фильтр: каноническое имя объекта
  pgw-<C>-<X>-<n>, pg>0, patroni>0)
  факт ≠ запись (без object) → перезапись (changed); записи без контейнера,
  чьи порты заняты чужим фактом → detach + переаллокация (как Д1)
changed → put portalloc + journal phase="repaired-portalloc"
dsn-инвариант: для каждого dsn-шарду пересобрать multi-host dsn из
  фактического portalloc (ноды по именам, креды как P2.5) → расхождение
  с ключом → put + journal phase="repaired-dsn"; dsn пересобирается только
  для канонических (без object) нод — object-шарды: dsn операторский факт
  (R9-симметрия), не трогаем
0 находок → тихий skip; transport-провал инспекции → transient (не ронять тик)
```

Фальш-Active (dsn на чужие порты) самолечится первым же тиком с живой
docker-картиной; фальш-UNREACHABLE уходят вместе с репарацией адресов.

**Д3 — лечение HA-scope при доказанной утрате данных (P2.2, бюджет-ветка).**
При исчерпании бюджета лидера (`PatroniBootSec`, он же бюджет Д3 — arch/14
§8) вместо безусловного фейла:

```
для каждой ноды scope: presence = driver.NodeDataPresenceAsync(C, X, n)
   (docker-exec: sh -c 'test -f <PGDATA>/PG_VERSION && echo present
    || echo absent'; stdout-parse; транспорт-сбой → Unknown)
все Absent  → ResetScopeAsync: del /service/<scope>/{initialize,leader,sync},
               del prefix /service/<scope>/{optime/,members/}   — request_*
               НЕ трогаем (декларации панели); journal phase="reset-scope";
               трекер бюджета сброшен (E3) → InProgress: Patroni бутстрапится
               заново, новый бюджет
хоть одна Present → FailAsync «<scope>: данные есть (ноды …), лидера нет —
               разбор оператора: чистка scope уничтожила бы данные» (панель:
               provision-stuck несёт текст оператору)
любой Unknown  → НЕ лечить: InProgress-ожидание (docker недоступен — не
               доказательство утраты)
```

`NodeDataPresenceAsync` — новый метод `IClusterDriver` (enum `DataPresence
{ Present, Absent, Unknown }`); PGDATA-путь —
`/home/postgres/pgdata/pgroot/data/PG_VERSION` (volume-корень Spilo, arch/14
§2.1). Один reset на scope за бюджет (после чистки бюджет начинается
заново); каждая чистка — в журнал. `AddShardProcess` Д3-лечения не получает
(его ноды всегда без данных; граница — §5).
---

## 4. Фазы

Исполнение (план-фаза детализирует шаги; порядок зависимостей):

1. **Ф1 — контракты** (сделано в spec-фазе): arch/14 (§2.4, §5 A, §3.3, §8),
   arch/adminpanel/02 (§2.3.1, §3, §4), arch/adminpanel/03 §4,
   arch/09 §11, roadmap t90.
2. **Ф2 — PgWorker provision/порты** (A, B, C): `PortAllocIndex` + вызовы в
   PlanPortsAsync/PlanShardPortsAsync; merge-усыновление в PlanPortsAsync;
   сверка портов в PlainClusterDriver.EnsureNodeAsync. Юнит-тесты (§6).
2-А. **Ф2-А — автономный reconcile** (G: Д1–Д3, расширение): перепланирование
   занятых портов (PortPlanConvergence + P1); probe-идентификация ноды
   (ShardProbe.IdentifyAsync + WaitPatroni); AD2'-инвариант адресов Active
   (portalloc/dsn = факт, AdoptionProcess); NodeDataPresenceAsync + лечение
   HA-scope при доказанной утрате данных. Юнит-тесты (§6).
3. **Ф3 — PgWorker бэкофф** (E): WorkState-поля + перенос серии;
   backoff-skip; FailAsync-счётчик; сброс трекера WaitPatroni (оба процесса);
   опции. Юнит-тесты.
4. **Ф4 — панель** (D): WorkJournalParser + PgWorkerWork в снапшоте;
   эскалация cluster-not-initialized; ProvisionStuckRule; WorkerHealthStore/
   Poller + WorkerUnhealthyRule; опции AlertsOptions/WorkerApiOptions.
   Юнит + фикстуры.
5. **Ф5 — стендовые мелочи** (F): чек 15 — чистка portalloc; контроль
   arch/09 §11 (текст уже в Ф1).
6. **Ф6 — интеграция/сборка**: полный прогон тестов, сборка
   (`TreatWarningsAsErrors`), self-review против контрактов.
7. **Ф7 — верификация на живом стенде** (после деплоя фикс-образа отдельным
   приказом; стенд НЕ перезапускается): наблюдение тиков воркера —
   canon10/smoke → ACTIVE за ожидаемое время (порядка минут: merge портов →
   Patroni-пробы по фактическим портам → SQL-фазы → снятие status-ключей);
   проверка алертов панели (§7).

---

## 5. Ограничения

- **Стенд не трогаем руками** (docker/etcd — только чтение); самолечение
  проверяется тиками воркера после деплоя фикса (деплой — отдельный шаг по
  приказу пользователя). `pgw-solo-*` не удаляются вовсе.
- **Распределение усыновления/репарации.** Provision — merge-усыновление +
  перепланирование занятых (§3.1/§3.7 Д1); Active — инвариант адресов в
  `AdoptionProcess` (§3.7 Д2: тот же фильтр, dsn пересобирается). object-
  записи (внешние ноды) не перезаписываются нигде. `AddShardProcess` не
  усыновляет, не получает backoff-skip и Д3-лечения (его ноды всегда без
  данных; вечные фейлы не наблюдались — механика переносится при
  необходимости отдельно).
- **Д3-границы**: чистка HA-scope ТОЛЬКО при Absent у ВСЕХ нод scope (Unknown
  и Present — не лечим); request_* не трогаются; один reset на scope за
  бюджет; надзор (`NodeSupervisor`) пробами не идентифицирует (после Д1
  коллизии канонических нод невозможны) и Д3 не выполняет.
- **Swarm**: `InspectNodesAsync` — заглушка (пусто) → усыновление A не
  работает, работает B-ветка пересоздания по плану; сверка портов в
  `SwarmClusterDriver.EnsureNodeAsync` не реализуется (MVP, стенд plain;
  отметить в коде комментарием-«swarm: инспект тасков»).
- **Гонка параллельных окон provisioning** (два кластера читают пустой
  префикс одновременно) остаётся — roadmap `t90` (глобальный txn/курсор).
- **`worker-unhealthy` — только PgWorker** (KafkaWorker-симметрия —
  тривиальное расширение позже, отдельной задачей не заводится: механика
  общая, поводы не наблюдались).
- **Backoff не различает тексты ошибок** (серия = подряд идущие фейлы);
  перманентные ошибки (порт-диапазон исчерпан) получают ту же задержку —
  панель уже алертит `provision-stuck` с текстом, оператор видит причину.
- **Не входит** (побочное, вне scope): info `probe-failed` по живым
  canon-нодам (панель бьёт по IP 172.20.x из `/service/*/members`,
  недоступным из сети панели — отдельная roadmap-задача: HostMap/mapping
  адресов проб); пересев чеков как источник потери portalloc лечится
  симптомно (чистка чека + устойчивость A/B/C), ревизию всех чеков не делаем.
- **Пробы Patroni** (`WaitPatroniAsync`) используют `topology.Nodes[…]` из
  portalloc — после merge A бьют по фактическим портам; отдельных
  «фоллбэков на docker» в пробах не появляется (источник адресов один —
  portalloc, инвариант arch/14 §3.1 сохраняется).

---

## 6. Тесты

Юнит (`tests/PgWorker.UnitTests/`, AAA-нотация в комментариях):

- `Planning/PortAllocatorTests` (существующие): +
  «закреплённый порт из busy соседа не переиспользуется при отсутствии в
  existing» (дубль-страховка контракта C на уровне аллокатора: busy-union
  передаётся вызывателем).
- `Provisioning/PlanPortsAdoptionTests` (новые, через моки
  `IClusterDriver`/`IEtcdGateway` — прецеденты моков процессов в
  существующих тестах `Provisioning/`): пустой portalloc + живой контейнер →
  запись из факта (без object); частичный → merge (существующая запись без
  object при расхождении перезаписывается фактом; с object — нет); ПОЛНЫЙ
  расходящийся portalloc → перезапись фактом (сценарий canon10); полный
  portalloc, совпадающий с фактом → запись НЕ перезаписывается (version
  ключа не растёт — идемпотентность); чужой Object-контейнер → skip +
  journal; ключ существовал → put (не txn).
- `Etcd/PortAllocIndexTests`: busy из двух соседей (3 порта на запись),
  exceptCluster исключается, битый JSON соседа → skip без Failed.
- `Docker/EnsureNodePortDriftTests` (fake `IDockerEngine` — прецедент
  `NodeMatcherTests`): контейнер с совпадающими портами → не трогается;
  расхождение 5432 → stop+rm+create; отсутствие биндинга → recreate;
  `addr.Object != null` → не трогается; enableDoorman=false не сверяет 6432.
- `Etcd/WorkJournalTests`: серия полей пишется/переносится/сбрасывается
  (Done), чтение старого формата (без полей) → null-поля.
- `Provisioning/ProvisionBackoffTests`: первый фейл → retry_not_before =
  now+base; n-й → min(base·2^(n−1), cap); skip при RetryNotBeforeUnix > now
  (мок etcd с журналом; журнал не пишется — verify); фаза прогресса после
  фейла → счётчик продолжается, Done → сброс; сброс `_patroniWaitSince` при
  бюджет-фейле (наблюдаемо: следующий вызов ждёт полный бюджет, не фейлится
  мгновенно).
- Д1 (автономный reconcile, расширение): `Planning/PortPlanConvergenceTests`
  (чистая функция): коллизионная запись (не факт своя, порт в foreign) →
  detach; запись == факт своего контейнера → живёт (docker-busy своих нод не
  «занимает» их же закрепления); object-запись → не трогается; полный
  portalloc без коллизий → changed=false. Процессные
  (`ProvisioningProcessTests`): полный portalloc, одна нода без контейнера, её
  порт занят docker-фактом соседа → тик перепланировал ТОЛЬКО её
  (portalloc перезаписан, EnsureNode в том же тике — EnsuredNodes содержит
  ноду), чужие записи не тронуты; гонка «порт заняли после P1» не
  моделируется (документировано поведением ≤2 тиков).
- Д1б: `ShardProbeTests` (существующие): IdentifyAsync — JSON `/patroni` со
  scope/name → NodeIdentity; битый JSON → null. Процессные: WaitPatroni при
  ответе ЧУЖОГО scope по план-порту → InProgress-ожидание (не RUNNING);
  фальш-RUNNING исключён.
- Д2: `AdoptionProcessTests` (существующие): Active + факт контейнеров
  расходится с portalloc → записи перезаписаны, journal phase=
  "repaired-portalloc"; dsn-ключ не соответствует фактическому portalloc →
  пересобран, phase="repaired-dsn"; 0 находок → skip без мутаций;
  transport-провал инспекции → тик не роняется.
- Д3: `Docker/NodeDataPresenceTests` (fake `IDockerEngine`/ExecResult):
  stdout "present" → Present, "absent" → Absent, исключение → Unknown.
  Процессные: бюджет исчерпан + все ноды Absent → ключи
  /service/<scope>/{initialize,leader,sync,optime/*,members/*} удалены,
  request_* живы, journal phase="reset-scope", исход InProgress; хоть одна
  Present → фейл с текстом «разбор оператора», ключи целы; Unknown →
  InProgress, ключи целы.
- Панель (`tests/AdminPanel.UnitTests/`): `WorkJournalParserTests` (реальные
  фрагменты значений + битый JSON → ParseError; фикстуры
  `EtcdFixtures/work-*.json`); `ClusterNotInitializedTests` (молодой → Info,
  CreatedUnix/возраст > 900 → Warning, отсутствует CreatedUnix → fallback
  sinceUnix); `ProvisionStuckRuleTests` (LastError+возраст > 300 → Warning c
  текстом ошибки в Message; FailFirstUnix null/свежий → нет алерта);
  `WorkerUnhealthyRuleTests` (Degraded/Unreachable → Warning per-instance,
  Healthy → пусто); `WorkerHealthPollerTests` (fake HttpMessageHandler:
  200/503/сетевая ошибка → статусы; пустые endpoints → пустой список);
  `SnapshotRefresherTests` (существующие): range work-ключей в тике,
  FailTick переносит.

Интеграционные (`tests/PgWorker.IntegrationTests/`,
`tests/AdminPanel.IntegrationTests/`) — прецеденты живы (обязательны):
`EtcdContractTests.WorkJournal_RoundTrip_AgainstRealEtcd` (EtcdFixture,
Testcontainers) расширяется round-trip'ом `RetrySeries` через реальный
etcd; `EtcdSnapshotIntegrationTests` (EtcdContainerFixture) — сид
`/pgworker/work/*` (валидный + битый JSON) → `RefreshOnceAsync` →
`PgWorkerWork` в снапшоте + `ParseError` на битый ключ. Прогоны —
`DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`.

---

## 7. Критерии приёмки

1. **Живой стенд самолечится** (Ф7, без перезапуска стенда): после деплоя
   фикса canon10 и smoke за конечное число тиков доходят до ACTIVE: portalloc
   = фактические порты контейнеров (проверка чтением etcd), ноды RUNNING,
   `status/bucket_*` сняты, dsn записаны; контейнеры НЕ пересоздавались
   (docker ps — те же контейнеры, uptime не сброшен).
2. **Коллизия закрыта**: новый кластер, созданный при живом canon10 (e2e или
   руками на стенде по приказу), получает порты вне записей
   `/pgworker/portalloc/*` соседей.
3. **Бэкофф**: зависший provision (повтор воспроизведения на стенде не
   требуется — юнит-тесты) пишет серию в `/pgworker/work/<C>`: частота фейлов
   падает с «каждый тик» до не чаще одной в 60 с; после успеха серия сброшена.
4. **Панель**: `cluster-not-initialized` старше 900 с — Warning; при живом
   `last_error` provision — `provision-stuck` (Warning) с текстом ошибки;
   degraded-воркер (healthz 503 при живом lease) — `worker-unhealthy`
   (Warning, target pgworker/<instance>); всё гаснет при выздоровлении.
5. Тесты (§6) зелёные; сборка без warnings (`TreatWarningsAsErrors`);
   контракты arch/ синхронны коду (ревью-гейт).
6. Документ: arch/09 §11 отвечает на вопрос «что такое pgw-solo-* Created и
   как их убирать» (руками — только по приказу).
7. Roadmap t90 не содержит закрытой части (последовательная коллизия),
   мерж-гейт соблюдён.
8. **Д1 (сам починил)**: юнит-сценарий «порт плана занят чужим» → воркер
   перепланировал ноду и создал контейнер В ТОТ ЖЕ тик, без оператора; на
   живом стенде (после деплоя, по приказу) пересекающиеся portalloc
   canon10/smoke сходятся: каждый кластер на своих свободных портах, вечный
   фейл-цикл «port is already allocated» исчез за ≤ 2 тика.
9. **Д2 (сам починил)**: фальш-Active (dsn/portalloc на чужие порты)
   репарируется первым тиком с живой docker-картиной: portalloc/dsn = факт,
   journal repaired-*, unreachable уходит; без оператора.
10. **Д3 (сам починил, безопасно)**: scope без лидера + все ноды без данных →
    чистка scope и повторный bootstrap (юнит-критерий: ключи удалены, request_*
    живы, InProgress, reset в журнале); данные есть хоть у одной ноды → чистки
    НЕТ (ключи целы), фейл с текстом для оператора; Unknown → чистки нет.

---

## 8. Принятые решения (сводка, с обоснованиями)

1. **Усыновление с перезаписью существующих записей** (отступление от
   AD2-канона AdoptionProcess): сценарий стенда — записи ЕСТЬ, но битые;
   merge «только отсутствующие» дефект не чинит. Guard'ы: только
   канонические контейнеры (имя), pg/patroni > 0, перезапись только записей
   без object.
2. **Сверка портов — в драйвере** (`PlainClusterDriver.EnsureNodeAsync`), а
   не в процессах: единая точка для provision/add-shard; надзорные вызовы
   безопасны (после Remove или при отсутствии контейнера); volume
   сохраняется.
3. **Busy = docker ∪ portalloc соседей** через отдельный `PortAllocIndex`:
   один источник правды для ProvisioningProcess и AddShardProcess (дефект
   общий); битые соседние ключи не роняют provision (skip + заметка).
4. **Бэкофф в `/pgworker/work/<C>`** (etcd), не in-memory: переживает
   рестарт воркера (деплой фикса = рестарт), виден панели (provision-stuck
   читает те же поля) и оператору. Формат расширен optional-полями —
   обратная совместимость без миграции.
5. **Опрос `/healthz` панелью** (вариант «панель опрашивает» из вилки
   пользователя) вместо «воркер пишет health в lease-ключ»: health-логика
   уже реализована в воркере (`PgWorkerHealth`, секции degraded), сеть
   панель→воркер уже существует (WorkerApiGateway ходит в API), новый
   писатель etcd-ключей и расширение lease-семантики не нужны; middleware
   воркера `/healthz` не закрывает api-key — панель зовёт без ключа.
6. **Эскалация по `created_unix`** (fallback — возраст алерта):
   не зависит от рестартов панели; порог 900 c > PatroniBootSec 600 c —
   здоровый провижининг не эскалируется.
7. **`provision-stuck` по возрасту серии** (`fail_first_unix`), а не
   `updated_unix`: InProgress-фазы обновляют журнал каждый тик — возраст
   последней записи ничего не значит; серия живёт в журнале до успеха
   (переносится фазами) — алерт не мигает.
8. **Сброс трекера бюджета Patroni при фейле**: без него новая попытка
   фейлится мгновенно (трекер вечен) — 234 фейла/10 мин; с ним цикл
   «бюджет 600 c → фейл → бэкофф → новый бюджет» даёт воркеру шанс
   самолечиться каждой попыткой, а панели — стабильную серию для алерта.
9. **KafkaWorker вне опроса healthz**: домен-снапшот отдельный, поводов нет;
   расширение тривиально (механика WorkerHealthPoller общая).
10. **Занятость = docker-факт минус свои контейнеры** (Д1): живой контейнер
    СВОЕЙ ноды занимает свой порт по определению; без вычитания selfFact
    перепланирование сносило бы здоровые закрепления. busy для аллокации —
    прежний (docker ∪ соседи): своя нода без контейнера обязана обходить и
    СВОИ прежние записи только через detach-условие «не подтверждено фактом».
11. **Identity через `/patroni`** (Д1б): endpoint отдаёт scope+name —
    глобально уникальная пара (scope `<C>-<X>`), достаточно для вывода
    «наша/чужая»; `/cluster` не содержит scope (имена нод шаблонные и
    совпадают между кластерами — ловили ложные матчи).
12. **Д3 через FailAsync при живых данных** (а не тихий last_error):
    серия+бэкофф → панельный provision-stuck показывает текст оператору без
    новых правил панели; чистка при живых данных запрещена контрактом
    (arch/14 R11).

## 9. Риски и митигации

| # | Риск | Митигация |
|---|---|---|
| R1 | Усыновление A перезаписывает portalloc «правильными» записями при ложном docker-матче (контейнер-тёзка) | Guard'ы: только каноническое имя объекта, pg+patroni > 0, запись без object; неоднозначность матчится skip'ом в NodeMatcher; под клэймом |
| R2 | Сверка B пересоздаёт контейнер с занятым портом (порт плана занял чужой сервис) → CreateContainer фейлится | Честный journal.last_error + provision-stuck в панели; идемпотентность: следующий тик повторит (порт освободится или оператор вмешается); лучше вечного ожидания Patroni на мёртвом порте |
| R3 | Бэкофф задерживает подъём кластера после реального исправления причины | Кап 60 с — задержка первой попытки после деплоя ≤ 60 с; счётчик сбрасывается первым успехом; skip не пишет журнал (шум не растёт) |
| R4 | Панель роняет тик из-за нового range `/pgworker/work/` при transport-фейле | Тот же контракт, что остальные KV (неполный снапшот хуже прежнего); FailTick хранит прежние PgWorkerWork |
| R5 | Опрос healthz создаёт нагрузку/фолс-негативы (панель за NAT) | Интервал 15 с, timeout общий с workers-клиентом; Unreachable при живом lease — Warning (не Critical), Hint ведёт оператора к runbook; HealthEnabled=false выключает |
| R6 | Поля серии в WorkState ломают старых читателей | Все поля optional (JSON-serialization WhenWritingNull); старые значения без полей читаются как null-серия |
