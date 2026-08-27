# Спецификация t12-cluster-create — создание кластера из UI

Дата: 2026-08-23. Фаза dev-flow: spec. Задача выдана пользователем напрямую
(автономный режим: все проектные решения приняты исполнителем и зафиксированы
в §8). Источники истины: `arch/02-etcd-contract.md` §2.1–2.2 (ключи, lease),
**§9** (контракт записи — внесён этой задачей до написания кода),
`arch/03-panels.md` §1–4 (эндпоинты, DTO, панели, алерты — уточнён этой
задачей), `arch/01-architecture.md` §1–2, §9 (write-path — уточнён).
Паттерн записи — референс `../pg arch/scripts/init-cluster.sh` (регистрация
кластера в etcd последним пакетом); CQRS-команды — референс
`../Puzzle docs/01.03-cqrs.md` (обрезанный под панель, без DB-контекстов).

Фактическое состояние кода: read-only панель t01–t11 — `IEtcdGateway` без
методов записи (комментарий «панель не пишет» устаревает этой задачей),
CQRS только queries (`IQuery`/`IQueryHandler`/`IHandler.HandleQuery`),
`ClustersParser`/`ServiceParser` не знают `state` в config, `NOT_INITIALIZED`
в статусах, `nodes/` и `request_*`; фронтенд — страницы t08/t09, mutations
используются только для login/logout. Roadmap-тег: `t12-cluster-create`
(`arch/roadmap/sharding.md`).

## 1. Цель

Во вкладке «Кластеры» добавить создание нового кластера: форма (уникальное
имя; кол-во бакетов; кол-во шардов ≤ бакетов; кол-во реплик, минимум 1 —
мастер, стандартно 2; заявка ресурсов **на каждую ноду** — cpu/mem/disk) и
запись по этим параметрам структуры кластера в etcd: контроль-плейн
`/clusters/<C>/…` с состояниями `NOT_INITIALIZED` (бакеты и плановые ноды) и
заявки `/service/<C>-shard<k>/request_{cpu,mem,disk}`. Заявленные ресурсы
отображаются там, где они семантически значимы: в карточке шарда (лимиты его
нод) и в деталях HA-scope. Поднятие нод/Patroni/схем — **вне scope**
(будущий provisioning читает записанные ключи).

Не входит (границы): инициализация нод и схем `bucket_*`, Patroni-ключи
(`leader/members/optime/initialize/config`), изменение/удаление кластера,
dsn/master-ключи, аудит-журнал операций, история создании.

## 2. Принципы

- **arch/-first**: контракт записи зафиксирован в `arch/02 §9`, REST/UI — в
  `arch/03 §1–4`; код не может противоречить им (расхождение = правка arch тем
  же PR).
- **Единственная мутация**: панель остаётся read-only по отношению ко всему,
  кроме собственного создания кластера; writer-код изолирован в команде
  `CreateClusterCommand` и минимальных методах `IEtcdGateway`.
- **Сервер — источник истины валидации** (arch/02 §9.3); фронт дублирует
  правила только для быстрой ошибки у поля.
- **Корректность не зависит от свежести снапшота**: уникальность имени —
  txn-клэйм `version == 0` в etcd, а не чтение снапшота; снапшот используется
  только для выбора активного endpoint'а записи.
- Подходы — из референсов: команда/диспетчер — Puzzle CQRS (без
  DB-текстов/Context — их место занимает etcd-txn); раскладка бакетов
  round-robin и «ключи последним пакетом» — `init-cluster.sh`.
- Идентификаторы английские; комментарии и тексты UI русские; тесты — с
  комментариями по AAA.
- YAGNI: без редактирования/удаления кластеров, без истории/аудита, без
  новых секций конфигурации (границы валидации — константы кода).

## 3. Структура и компоненты

### 3.1. Контракт etcd (arch/02 §9 — уже в arch, здесь кратко)

Успешное `POST /api/clusters` пишет (`<C>` имя, `N` бакетов, `S` шардов,
`R` реплик, `T` unix):

```
/clusters/<C>/config                              {"buckets":N,"dbname":"<C>",
                                                    "created_unix":T,
                                                    "state":"NOT_INITIALIZED"}
/clusters/<C>/shards/shard<k>/replicas            "<R>"
/clusters/<C>/shards/shard<k>/nodes/<n>/state     "NOT_INITIALIZED"  (n: shard1a, shard1b…)
/clusters/<C>/buckets/routing/bucket_<i>          "shard<k>"          (round-robin, i=0..N-1)
/clusters/<C>/buckets/status/bucket_<i>           {"bucket":"bucket_<i>","state":"NOT_INITIALIZED",
                                                    "owner":"shard<k>","updated_unix":T}
/service/<C>-shard<k>/request_cpu                 "2" | "0.5"         (ядра, invariant)
/service/<C>-shard<k>/request_mem                 "8Gi"
/service/<C>-shard<k>/request_disk                "100Gi"
```

Протокол записи: (1) txn-клэйм имени — compare `version(config)==0` + put
config → compare не сошёлся = 409; (2) пакет обычных PUT остальных ключей
(etcd `max-txn-ops`=128 не вмещает 2N+ ключей); (3) сбой посередине —
компенсация `del --prefix /clusters/<C>/` + точечные `del` своих
`request_*`-ключей; неудачная компенсация оставляет частичный кластер
(безопасно: повторное создание откажет на клэйме, добор/очистка — runbook).
НЕ пишутся: `dsn`, `master`, Patroni-ключи, heals.

Валидация (arch/02 §9.3): `name` `^[a-z][a-z0-9_]{0,62}$` (без дефиса —
однозначность scope-матчинга), `buckets` 1..8192, `shards` 1..128 и ≤
`buckets`, `replicas` 1..26 (default 2), `requestCpu` 0.01..64 (ядра,
десятичное), `requestMem`/`requestDisk` 1..65536 (GiB, целые).

### 3.2. Модель Core (`AdminPanel.Core`)

- `ClusterInfo` + поле `ClusterState State` (новый enum `ClusterState {
  Active, NotInitialized }`; отсутствие `state` в config = `Active` —
  кластеры `init-cluster.sh`).
- `ShardInfo` + поле `IReadOnlyList<NodeInfo> Nodes` (пусто у старых);
  новый record `NodeInfo(string Name, string? State)` — `State` raw-строка
  (толерантно к будущим состояниям provisioning'а).
- `BucketState` + член `NotInitialized`.
- `HaScope` + `string? RequestCpu, RequestMem, RequestDisk` (raw-значения
  ключей `request_*`).

### 3.3. Слой Etcd (`AdminPanel.Etcd`)

- `IEtcdGateway` + минимальные write-методы (те же endpoint-аргументы и
  `Result`, что у чтения):
  - `TxnAsync(endpoint, compares, puts, ct)` → `Result<TxnResult>`
    (`TxnCompare(string Key, long Version)`, `KvPut(string Key, string
    Value)`, `TxnResult(bool Succeeded)`); тело `POST /v3/kv/txn`
    `{"compare":[{"key":b64,"version":0}],"success":[{"request_put":{…}}]}`,
    ответ `{"succeeded":…}`;
  - `PutAsync(endpoint, key, value, ct)` → `Result`;
  - `DeleteAsync(endpoint, keyOrPrefix, prefix, ct)` → `Result`
    (`POST /v3/kv/delete-range`, `range_end` — уже существующий
    `PrefixEnd`).
- Парсер `ClustersParser`: `config.state == "NOT_INITIALIZED"` →
  `ClusterState.NotInitialized`; статус-ключ `state:"NOT_INITIALIZED"` →
  `BucketState.NotInitialized` (иные неизвестные state — по-прежнему
  KeyParseError); новый лист `shards/<X>/nodes/<n>/state` → `NodeInfo`
  (отсортированы по имени).
- Парсер `ServiceParser`: листья `request_cpu|request_mem|request_disk` →
  поля HaScope (raw-строки; пустое значение = null).
- Новый `ClusterCreatePlan` (чистая функция, `AdminPanel.Etcd.Writing`):
  валидированный запрос + `nowUnix` → (а) конфиг-JSON клэйма, (б)
  детерминированный список остальных `(key, value)`, (в) список
  компенсационных `request_*`-ключей. Границы/регексы валидации —
  `CreateClusterLimits` + `CreateClusterValidator` (чистая функция →
  `IReadOnlyList<(string Field, string Error)>`) там же; canonical-строки:
  cpu — `decimal.ToString(CultureInfo.InvariantCulture)` без хвостовых
  нулей, mem/disk — `$"{n}Gi"`.

### 3.4. CQRS-команды (`AdminPanel.Infrastructure`, по Puzzle 01.03)

- Маркеры `ICommand<T>` + `ICommandHandler<in TC, TR>` (форма как у
  `IQueryHandler` — только `Handle`; `GetContext`/DB-контексты из Puzzle не
  копируются: панель без БД, транзакция — etcd-txn клэйма).
- `IHandler` + `HandleCommand<C,T>` (тот же Activity + scope-план, что у
  `HandleQuery`).

### 3.5. Команда и эндпоинт (`AdminPanel.Api`)

- `CreateClusterCommand(CreateClusterRequest Request) : ICommand<
  ClusterCreatedDto>`; рекорд `CreateClusterRequest(string Name, int Buckets,
  int Shards, int Replicas, decimal RequestCpu, int RequestMem, int
  RequestDisk)`; `ClusterCreatedDto` — по arch/03 §1.1 (канонические строки
  ресурсов, `state:"NOT_INITIALIZED"`).
- `CreateClusterCommandHandler(ISnapshotStore store, IEtcdGateway gateway)`
  `[InjectAsScoped]`: (1) `CreateClusterValidator` → отказ
  `CreateClusterValidationException(Errors)`; (2) активный endpoint:
  `store.Current?.Etcd.ActiveEndpoint`; нет снапшота/endpoint'а → отказ
  `EtcdWriteUnavailableException`; (3) txn-клэйм → `succeeded == false` →
  `ClusterAlreadyExistsException(Name)`; (4) PUT по плану; первый сбой →
  компенсация (§3.1) → отказ исходной ошибки; (5) `ClusterCreatedDto`.
  Никаких ретраев (как у refresher'а — повтор = новый POST от пользователя).
- Новый `OperationsModule.MapOperationsApi()` (InspectionModule остаётся
  read-only): `POST /api/clusters` → 201 + `ClusterCreatedDto`; маппинг
  отказов: validation → 400 ProblemDetails (`errors` по полям), already
  exists → 409, прочее (вкл. недоступность etcd) → 503. Auth-guard `/api/*`
  уже закрывает эндпоинт (401 без cookie). Подключение — в `Program.cs`
  рядом с `MapInspectionApi()`.

### 3.6. Отражение в чтении (DTO-мапперы)

- `BucketStates`: `Name/TryParse` + `NOT_INITIALIZED`.
- `ClusterSummaryDto` + `NotInitialized`; `ClusterDto` + `State`
  (строчный канон `ACTIVE|NOT_INITIALIZED`); `ShardDto` + `Nodes[]` +
  `Requests{Cpu,Mem,Disk}?` (join `snapshot.HaScopes` по `"<C>-<X>"`,
  matched); `HaScopeDto` + `Requests{Cpu,Mem,Disk}?`; `OverviewDto`.
  clusters[] + `notInitialized` (для NOT_INITIALIZED кластеров
  `masterlessShards = 0` — без мастера это ожидаемо, не деградация).
- `activeMoves` (сводка + Overview) считать только
  `SYNCING|FROZEN|ABORTING` (сейчас: `!= Active` — NOT_INITIALIZED
  ошибочно попал бы в «переезды»; arch/03 §2).
- Фильтр `?state=` на `/api/clusters/{c}` принимает `NOT_INITIALIZED`.

### 3.7. Алерты (`AlertEngine`)

- Новое правило `ClusterNotInitializedRule` → kind `cluster-not-initialized`,
  severity info, target `<C>`: кластер в `NOT_INITIALIZED`.
- `MoveStaleRule`: скип `BucketState.NotInitialized` (не переезд).
- `ShardNoLeaderRule`: скип scope'ов, чей кластер в `NOT_INITIALIZED`.
- Без правок (проверено по коду): `ShardNoMasterRule` (dsn пуст — скип),
  `MoveFrozenLongRule`/`MoveAbortingRule` (точные состояния),
  `MoveFlippedStatusStuckRule` (требует `target`, у NOT_INITIALIZED его нет),
  `BucketNoRoutingRule`/`BucketOutOfRangeRule` (работают и дают сигнал
  частичного создания).

### 3.8. Фронтенд (`frontend/src`)

- `api/dto.ts`: `CreateClusterRequestDto`, `ClusterCreatedDto`,
  `ClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED'`, `NodeDto`,
  `ShardRequestsDto`, `notInitialized` в `ClusterSummaryDto`,
  `state` в `ClusterDto`, `requests` в `ShardDto`/`HaScopeDto`,
  `notInitialized` в `OverviewClusterDto`, `BucketStateName` +
  `'NOT_INITIALIZED'`.
- `api/queries.ts`: `createCluster(request)` (POST через `apiFetch`) и
  `queryKeys` без изменений (мутация инвалидирует `['clusters']`).
- Новый `pages/clusters/ClusterCreateModal.tsx`: Mantine Modal + форма;
  поля с клиентской валидацией-зеркалом (§3.1); дефолты: бакеты 16, шарды 2,
  реплики 2, cpu 2, mem 8 GiB, disk 100 GiB; группа полей подписана «на
  каждую ноду»; отправка — `useMutation(createCluster)`: успех → закрыть,
  `invalidateQueries({queryKey:['clusters']})`; ошибка — текст ProblemDetails
  (409 «имя занято», 400 — по полям, 503 — «etcd недоступен»); кнопка
  заблокирована на время мутации.
- `ClustersPage`: кнопка «Создать кластер» в шапке (рядом с Title) +
  модалка; строка списка — серый бейдж «не инициализирован».
- `ClusterDetailsPage`/`ShardsTab`: столбец «Ресурсы на ноду»
  (`2 CPU · 8Gi · 100Gi`, `—` если нет заявки); блок «Ноды» — имя + бейдж
  состояния (`NOT_INITIALIZED` серым).
- `BucketsTab`/`BucketStateBadge`: `NOT_INITIALIZED` — серый бейдж; в
  подсветке не-ACTIVE участвует, но без жёлто-красной окраски.
- `MovesTab`: фильтр строго `SYNCING|FROZEN|ABORTING` (NOT_INITIALIZED —
  не переезд).
- `HaScopeDetailsPage`: блок «Заявленные ресурсы нод» (cpu/mem/disk, при
  наличии `requests`).
- `OverviewPage`: кластеры с `notInitialized` — приглушённый текст, бейдж.

### 3.9. dev-stand

Новый `dev-stand/checks/15-cluster-create.sh` (паттерн `10-smoke-api.sh`):
очистка префиксов smoke-кластера (`etcdctl del /clusters/smoke --prefix`,
`del /service/smoke-shard1/request_cpu` и т.д.), логин → `POST
/api/clusters` (smoke, 4 бакета, 2 шарда, 2 реплики) → 201; проверка ключей
в etcd (config/routing/status/nodes/request_*); повторный POST → 409;
`GET /api/clusters` содержит smoke с бейджем. Сид `seed.sh` не меняется.

### 3.10. Тесты

- Unit: валидатор (каждое правило + канонизация строк), план (полный набор
  ключей/значений, round-robin, имена нод a..z, компенсационный список),
  парсеры (config.state, статус NOT_INITIALIZED, nodes-лист, request_*,
  старые кластеры без новых полей), правила алертов (кластер NotInitialized:
  move-stale/shard-no-leader молчат, cluster-not-initialized горит),
  мапперы DTO, диспётчер `HandleCommand`; хендлер с фейковым gateway
  (клэйм прошёл, PUT падает на k-м ключе → вызваны компенсационные delete
  префикса и точечные request_*).
- Integration (Testcontainers etcd + `WebApplicationFactory`): POST создание
  → 201 и точный набор ключей в etcd (через gateway range); повтор → 409;
  невалидные тела → 400 (по каждому правилу); без auth → 401; после
  создания снапшот подхватывает кластер (поллинг `/api/clusters` ≤ ~10 c).

## 4. Фазы исполнения (для plan)

1. **Infrastructure**: `ICommand`/`ICommandHandler`/`HandleCommand` (+unit).
2. **Core+парсеры**: модель §3.2, парсеры, `BucketStates` (+unit, фикстуры).
3. **Etcd write API + план**: gateway-методы, `CreateClusterLimits`/
   `Validator`/`ClusterCreatePlan` (+unit).
4. **Команда**: `CreateClusterCommand` + хендлер (+unit, мок gateway:
   клэйм/успех/409/сбой-компенсация).
5. **API**: `OperationsModule`, DTO чтения §3.6 (+integration §3.10).
6. **Алерты**: правило + подавления (+unit).
7. **Фронтенд**: §3.8 (порядок: dto/queries → modal → списки/детали/HA).
8. **dev-stand**: `15-cluster-create.sh`; прогон `checks/` целиком.

## 5. Ограничения

- Только создание: без edit/delete кластера, без изменения констант после
  создания (P18 — buckets/dbname навсегда), без снятия NOT_INITIALIZED
  (это сделает будущий provisioning).
- Панель не поднимает ноды и не создаёт схемы — записывается заявка.
- Не трогаем: сид `seed.sh`, Patroni-эмуляторы, пробы, auth-механику,
  конфигурационные секции (новых `AdminPanel:*` опций нет).
- `TreatWarningsAsErrors=true`, .NET 10, CPM — как везде; новые зависимости
  не нужны.

## 6. Критерии приёмки

1. Форма на панели «Кластеры» создаёт кластер; все параметры из задачи
   присутствуют; уникальность имени проверяется (409 на повтор, атомарно —
   гонка двух POST не создаёт дубль).
2. В etcd ровно набор ключей §3.1 (включая `request_cpu/mem/disk` в
   `/service/<C>-shard<k>/`); значения — канонические строки.
3. Бакеты: routing round-robin + status `NOT_INITIALIZED`; ноды: `<shard><a..>`
   со state `NOT_INITIALIZED`; config содержит `state:"NOT_INITIALIZED"`.
4. `shards > buckets` и прочие нарушения §3.1 → 400 с указанием поля.
5. Новый кластер появляется в списке/деталях через обычный polling (без
   перезапуска), с бейджем «не инициализирован»; заявки ресурсов видны в
   карточке шарда и деталях HA-scope.
6. Для нового кластера нет critical/warning-шумов (shard-no-leader/
   shard-no-master/move-* молчат); горит info `cluster-not-initialized`.
7. Старые кластеры (без новых полей) отображаются и алертятся как раньше
   (регрессий нет — все тесты t01–t11 зелёные).
8. Сбой записи после клэйма приводит к компенсации (префикс `/clusters/<C>/`
   и свои `request_*` удалены) — покрыто unit-тестом хендлера; неудачная
   компенсация оставляет строго частичный кластер с config — задокументировано
   (§3.1, §7).
9. `dotnet build`/`dotnet test` без варнингов и зелёные; `cd frontend &&
   npm run build` проходит; dev-stand `checks/00..15` зелёные.

## 7. Риски

- **max-txn-ops/объём записи**: клэйм — одна txn; пакет — последовательные
  PUT (8192 бакетов = ~16k PUT; практические N ≤ сотен — приемлемо; лимит
  8192 защищает от абсурдных N).
- **Частичная запись**: компенсация best-effort; остаток безопасен (409 на
  повтор), ручная очистка — runbook (как `init-cluster.sh`).
- **Имя занято «остатком»**: видно в UI как incomplete/NOT_INITIALIZED
  кластер — оператор видит причину 409.

## 8. Принятые решения (автономно, по контексту задачи)

1. **Панель получает единственную мутацию** — правка принципа «read-only
   навсегда» (arch/01 §9, README): задача пользователя прямо требует запись;
   все прочие мутации остаются запрещёнными.
2. **dbname = имя кластера**: отдельного поля в задаче нет; `init-cluster.sh`
   допускает любое dbname, но панель создаёт заявку, где БД ещё не существует
   — естественный выбор `dbname = <C>` (константа с создания, P18).
3. **Имена шардов `shard1..shardS`**: соответствуют примеру
   `init-cluster.sh`; predictable для будущего provisioning.
4. **Имена нод `<shard><буква a..z>`** (`shard1a` — мастер): конвенция стенда
   `../pg` (`s1a`,`s1b` — Patroni member names); отсюда потолок replicas=26.
5. **Имя кластера без дефиса** (`^[a-z][a-z0-9_]{0,62}$`): иначе
   `ScopeMatcher` (scope `<C>-<X>`, префиксный разбор) даёт коллизии
   (`demo` vs `demo-old`); совместимо с `valid_dbname` из `pg`.
6. **Формат request_***: cpu — десятичные ядра invariant-строкой, mem/disk —
   `"<целое>Gi"`: строки-квантити в духе Kubernetes (будущие лимиты нод),
   читаемы человеком, тривиально валидируются; парсинг обратно не нужен —
   отображение raw.
7. **request_* лежат в `/service/<scope>/`** (по требованию задачи) и
   означают заявку на **каждую** ноду scope; отображение — карточка шарда
   (семантика «лимиты нод шарда») + детали HA-scope (пространство, где ключи
   физически живут).
8. **Клэйм-txn + пакет PUT + компенсация**, а не одна txn: etcd
   `max-txn-ops` (128) не вместит 2N+ ключей; паттерн «ключи последним
   пакетом» проверен `init-cluster.sh`.
9. **CQRS-команда по Puzzle, но без DB-слоя**: `GetContext`/IDbContext/
   транзакция БД не копируются — их роль выполняет txn-клэйм etcd; панель
   остаётся без собственной БД.
10. **Состояние NOT_INITIALIZED трёхуровневое**: кластер (`config.state`),
    бакет (status-ключ), нода (`nodes/<n>/state`) — каждое меняется будущим
    provisioning'ом независимо (ноды раньше схем и т.п.); отсутствие полей у
    старых кластеров = ACTIVE.
11. **Алерты**: новый info `cluster-not-initialized`; подавлены
    `shard-no-leader` (у не поднятого кластера нет лидера — это не авария) и
    move-* для NOT_INITIALIZED (не переезды); `shard-no-master` безопасен
    автоматически (dsn не пишется).
12. **Активный endpoint записи — из снапшота** (`EtcdStatus.ActiveEndpoint`,
    его выбирает/ротирует refresher); писать «на все endpoints» не нужно —
    raft реплицирует; нет снапшота → 503.
13. **Без force-refresh и без ретраев команды**: следующий тик refresher'а
    (3 c) подхватывает ключи; повтор мутации — за пользователем (как
    перезапуск скрипта в `pg`).
14. **Overview `masterlessShards=0` для NOT_INITIALIZED** — «без мастера» у
    заявленного кластера не деградация; кластер помечен и приглушён в UI.
15. **Новые правила валидации — константы кода** (`CreateClusterLimits`), не
    конфигурация: доменные границы, настройки не нужны.
16. **Ops-код в новом `OperationsModule`** (не в InspectionModule): чтение и
    запись остаются в разных модулях композиции; handler — в Api, план/
    валидатор — в AdminPanel.Etcd (рядом с gateway), модель — в Core.
