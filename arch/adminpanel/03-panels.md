# 03. Панели и REST API

Спецификация UI-панелей и HTTP-контракта. Всё read-only, кроме мутаций:
`POST /api/clusters` (создание кластера, 02 §9), `DELETE /api/clusters/{name}`
(перевод в TO_REMOVE, 02 §9.4), `POST /api/clusters/{cluster}/shards`
(добавление шарда, 02 §9.5), `DELETE /api/clusters/{cluster}/shards/{shard}`
(маркер демонтажа шарда, 02 §9.6), `POST /api/clusters/{cluster}/moves`
(заявки на переезды бакетов, 02 §9.7), `POST /api/clusters/{cluster}/app-password/rotate`
(заявка ротации app-пароля, 02 §9.8), а также мутация пересоздания ноды
`POST /api/ha/{scope}/nodes/{node}/recreate` (маркеры TO_RECREATE/recreate —
зафиксирована кодом): GET-эндпоинты и POST login/logout не
мутируют инспектируемые системы (кластерные мутации пишут только ключи своих
операций). Отдельный модуль **Kafka** (`/api/kafka/*` — §7): свои GET и 8
мутаций декларативной модели (протоколы — 02 §10). JSON, camelCase,
`ProblemDetails` для ошибок.
Все эндпоинты, кроме `login` и `healthz`, требуют cookie-сессию (401 без неё).

## 1. Список эндпоинтов

| Метод+путь | Назначение |
|---|---|
| `POST /api/auth/login` | тело `{username,password}` → 204+cookie \| 401 (rate-limit 5/мин) |
| `POST /api/auth/logout` | погасить сессию → 204 |
| `GET /api/auth/me` | `{username}` \| 401 |
| `GET /api/healthz` | живость самой панели (без auth): `{status:"ok"}` |
| `GET /api/overview` | дашборд: сводка etcd+кластеров+алертов, `snapshotAgeMs` |
| `GET /api/etcd/status` | endpoints, members, leader, alarms, reachable, версия |
| `GET /api/clusters` | список кластеров (сводный) |
| `POST /api/clusters` | создание кластера (02 §9): тело `CreateClusterRequestDto` → 201+`ClusterCreatedDto` \| 400 (валидация) \| 409 (имя занято) \| 503 (etcd/снапшот) |
| `GET /api/clusters/{cluster}` | детали: config, шарды, бакеты, heals (всё сразу; N ≤ тысяч — грид фильтруется на клиенте) |
| `POST /api/clusters/{cluster}/shards` | добавить шард Active-кластеру (02 §9.5): тело `AddShardRequestDto` → 201+`ShardAddedDto` \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/clusters/{cluster}/shards/{shard}` | маркер демонтажа шарда `TO_REMOVE` (02 §9.6): 204 \| 404 \| 409 \| 503 |
| `POST /api/clusters/{cluster}/moves` | заявки на переезды бакетов (02 §9.7): тело `MoveBucketsRequestDto` → 201+`MovesQueuedDto` \| 400 \| 404 \| 409 \| 503 |
| `POST /api/clusters/{cluster}/app-password/rotate` | заявка ротации app-пароля кластера (02 §9.8): без тела → 201+`AppPasswordRotatedDto` \| 404 \| 409 \| 503 |
| `GET /api/ha` | список HA-scope'ов (сводный) |
| `GET /api/ha/{scope}` | детали scope: leader, members+runtime, optime, raw config, request_* |
| `GET /api/alerts` | все алерты; query `?severity=critical|warning|info`, `?kind=` |

Дополнительно к квери-параметрам: `?owner=&state=` на `/api/clusters/{c}`
возвращают отфильтрованный `buckets` (удобно для детальной страницы; по
умолчанию — все). `state` принимает и `NOT_INITIALIZED` (02 §2.1).

### 1.1. Контракт `POST /api/clusters`

Тело `CreateClusterRequestDto` (валидация — 02 §9.3; все ограничения —
ProblemDetails 400 с деталями по полям):

```text
CreateClusterRequestDto: name, sharded (bool, опционально: отсутствует/null
                          = true — обратная совместимость), buckets, shards
                          (передаются ТОЛЬКО при sharded=true; при
                          sharded=false не требуются — сервер нормализует
                          в 1/1, 02 §9.3), replicas,
                          requestCpu (число ядер, десятичное),
                          requestMem (GiB, целое), requestDisk (GiB, целое)
```

Ответ 201 (кластер записан в etcd, состояние NOT_INITIALIZED; снапшот
подхватит на следующем тике):

```text
ClusterCreatedDto: name, dbname, sharded (bool), bucketsCount, shardsTotal,
                    replicas, requestCpu, requestMem, requestDisk (строки-каноны
                    02 §9.1), state:"NOT_INITIALIZED"
```

Нешардированная БД (`sharded=false`): в etcd пишется вырожденная структура
1 бакет × 1 шард (02 §9.1), ответ возвращает `bucketsCount=1`,
`shardsTotal=1`, `sharded=false`.

Отказы: 409 `Cluster already exists` (клэйм-txn не сошёлся — имя занято);
503 (нет снапшота/активного endpoint'а, etcd-ошибка записи). Компенсация
частичной записи — 02 §9.2.

### 1.2. Контракт `DELETE /api/clusters/{name}`

Перевод кластера в состояние удаления (протокол — 02 §9.4): панель не
удаляет ключи, а пишет `config.state="TO_REMOVE"`; снятие нод и очистка —
внешний оркестратор/runbook. Идемпотентен: повторный DELETE кластера уже
в `TO_REMOVE` — тоже 204.

Успех — 204 (без тела). Отказы: 404 `Cluster not found` (config-ключа нет
или имя неканоническое — 02 §9.3); 503 (нет снапшота/активного endpoint'а,
etcd-ошибка, битый config). Пока config занят, имя не создаётся повторно
(409 на POST — 02 §9.2).

### 1.3. Контракт `POST /api/clusters/{cluster}/shards`

Добавление шарда Active-кластеру (протокол — 02 §9.5): панель дописывает
декларацию нового шарда (replicas + nodes/NOT_INITIALIZED + request_*),
подъём выполняет PgWorker. Шард стартует ПУСТЫМ — routing/status не пишутся,
перераспределения бакетов нет (явные переезды — 02 §9.7).

Тело `AddShardRequestDto` (валидация — 02 §9.3, те же границы, что создания
кластера; все ограничения — ProblemDetails 400 с деталями по полям):

```text
AddShardRequestDto: replicas (целое 1..26, дефолт 2 — отсутствие поля = 2),
                    requestCpu (десятичные ядра 0.01..64),
                    requestMem (GiB, целое 1..65536),
                    requestDisk (GiB, целое 1..65536)
```

Имя шарда генерирует сервер: `shard<max+1>` по числовым суффиксам
существующих шардов (02 §9.5); свободного ввода нет.

Ответ 201 (декларация записана в etcd, шард `NOT_INITIALIZED`; PgWorker
поднимет ноды — снапшот подхватит на следующем тике):

```text
ShardAddedDto:  cluster, name (сгенерированное shard<k>), replicas,
                requestCpu, requestMem, requestDisk (строки-каноны 02 §9.1),
                state:"NOT_INITIALIZED"
```

Отказы: 404 `Cluster not found` (config-ключа нет или имя кластера
неканоническое); 409 (кластер не Active — NOT_INITIALIZED «дождитесь
инициализации» / TO_REMOVE «кластер удаляется»; клэйм-txn имени не сошёлся —
конкурентный POST занял имя; достигнут предел 128 шардов); 400 (валидация
полей); 503 (нет снапшота/активного endpoint'а, etcd-ошибка чтения/записи,
битый config). Компенсация частичной записи — 02 §9.5.

### 1.4. Контракт `DELETE /api/clusters/{cluster}/shards/{shard}`

Маркер демонтажа шарда (протокол — 02 §9.6): панель не удаляет ключи, а
ставит `shards/<X>/state="TO_REMOVE"`; снятие нод и очистку выполняет
PgWorker (guard'ы G1–G7; до демонтажа шард виден с бейджем «к удалению»).
Идемпотентен: повторный DELETE шарда уже в `TO_REMOVE` — тоже 204.

Успех — 204 (без тела). Отказы: 404 (кластер или шард не найдены, имена
неканонические); 409 (кластер не Active; серверные пред-проверки guard'ов —
бакеты на шарде, незавершённый переезд, последний шард, карантин — тексты
02 §9.6); 503 (нет снапшота/активного endpoint'а, etcd-ошибка, битый config,
снапшот отстаёт — повтор запроса). Гонки между пред-проверкой и flip'ом
переезда ловят guard'ы PgWorker G3/G4 — маркер останется, демонтаж подождёт.

### 1.5. Контракт `POST /api/clusters/{cluster}/moves`

Заявки на переезды бакетов (протокол — 02 §9.7): панель ставит очередь ключей
`/pgworker/moves/<C>/bucket_<i>`; выполнение — PgWorker (старейшая заявка
кластера, одна за раз — переезды последовательны по построению контракта).
Повтор после частичного сбоя идемпотентен: уже стоящие идентичные заявки
возвращаются в `skipped` без перезаписи.

Тело `MoveBucketsRequestDto` (валидация — 02 §9.7; ограничения — ProblemDetails
400/409 с деталями):

```text
MoveBucketsRequestDto: from (шард-источник), to (шард-приёмник),
                        buckets (непустой массив уникальных int — какие бакеты
                        источника везти; порядок обработки = по возрастанию id)
```

Ответ 201 (заявки записаны в etcd; PgWorker начнёт со старейшей — вкладка
«Переезды» покажет очередь):

```text
MovesQueuedDto: cluster, from, to, queued[int[]] (поставлены сейчас),
                 skipped[int[]] (идентичные уже стояли)
```

Отказы: 400 (`buckets` пуст/дубликаты, `from == to`); 404 (кластер или шард
не найдены, имена неканонические); 409 (кластер не Active; нешардированная
БД; приёмник TO_REMOVE; бакет не на источнике / в незавершённом переезде /
не ACTIVE; на бакете уже стоит иная заявка; конкурентная заявка — txn-клэйм
не сошёлся); 503 (нет снапшота/активного endpoint'а, etcd-ошибка чтения/
записи, битый config). Сбой посередине — без компенсации (02 §9.7 п.5):
повтор досдаст остаток.

### 1.6. Контракт `POST /api/clusters/{cluster}/app-password/rotate`

Заявка ротации per-cluster app-пароля (протокол — 02 §9.8): панель ставит
ключ `/pgworker/rotations/<C>` (txn-клэйм); выполнение — PgWorker
(AppPasswordRotator): ALTER ROLE на всех поднятых шардах и атомарная замена
`/clusters/<C>/app_password`. Сам пароль панель не знает и не показывает.

Тело не требуется (пустое/отсутствующее; посторонние поля игнорируются).

Ответ 201 (заявка в etcd; применение асинхронно — секунды при живых шардах):

```text
AppPasswordRotatedDto: cluster, requestedUnix, requestedBy
```

Отказы: 404 (кластер не найден, имя неканоническое); 409 (кластер не Active;
ротация уже запрошена — живая заявка; конкурентный POST по txn-клэйму);
503 (нет снапшота/активного endpoint'а, etcd-ошибка чтения/записи, битый
config). Повтор после успеха валиден (заявки уже нет). UI-модалка
предупреждает о разрыве подключений со старым паролем до перечитывания
кредов клиентами.

## 2. DTO (ключевые поля)

```text
OverviewDto:  alertsCritical, alertsWarning, etcd{reachable, endpointsOk, endpointsTotal},
              clusters[{name, shards, buckets, activeMoves, masterlessShards,
              notInitialized(bool)}],
              activeMoves[{cluster,bucket,state,owner,target,updatedUnix}],
              snapshotAgeMs, stale(bool)
EtcdStatusDto: endpoints[{url, reachable, latencyMs, version, dbSizeBytes,
              leaderMemberId, raftTerm, errors[], active}], members[{id, name,
              peerUrls, clientUrls, isLeader}], alarms[{memberId, type}],
              quorumSuspected, lastRefreshUtc
ClusterDto:   name, dbname, bucketsCount, createdUnix, incomplete(bool),
              state(ACTIVE|NOT_INITIALIZED|TO_REMOVE), sharded(bool), shards[ShardDto],
              buckets[BucketDto], pendingMoves[MoveTicketDto] — очередь заявок
              /pgworker/moves/<C>/ (02 §2.3.1; джойн по кластеру, сортировка
              по requestedUnix), heals[HealDto],
              standNodes[{name,address}] — стендовый топо-реестр снапшота
              (02 §2.3; поле глобально для всех кластеров, обычно пусто;
              UI-блок «Стендовая топология» рисуется при наличии)
ClusterSummaryDto: name, dbname, bucketsCount, incomplete(bool),
              notInitialized(bool), toRemove(bool), shardsTotal, shardsWithMaster,
              activeMoves
ShardDto:     name, state(ACTIVE|TO_REMOVE — маркер демонтажа 02 §2.1/§9.6,
              отсутствие ключа = ACTIVE), dsn, hosts[], replicasDeclared,
              masterAddress, masterLeaseAlive(bool), nodes[{name, state}],
              requests{cpu, mem, disk}?(nullable) — заявка на ноду из
              HaScope `<C>-<X>` (02 §2.2 request_*), null у старых кластеров,
              runtime{standbiesSync, slotsLagMaxBytes,
              walStatusLost[], subscriptions[], bucketSchemas[], error}(nullable)
BucketDto:    id, owner, state(ACTIVE|SYNCING|FROZEN|ABORTING|NOT_INITIALIZED),
              move{owner,target,startedUnix,updatedUnix,phase,lastError}? ,
              ageSec (для не-ACTIVE)
HealDto:      bucket, was, now, reason, tsUnix
HaScopeDto:   scope, cluster?, shard?, matched(bool), leaderName, optimeLeader,
              members[{name, host, port, role, state, timeline, lagBytes,
              probeAtUtc, probeError}], rawConfig,
              requests{cpu, mem, disk}?(nullable) — заявка на ноду (02 §9.1)
AlertDto:     id, severity, kind, target, message, details{...}, sinceUnix
AddShardRequestDto: replicas (целое 1..26, дефолт 2), requestCpu (десятичные
              ядра 0.01..64), requestMem/requestDisk (GiB, целые 1..65536) —
              тело POST шардов (§1.3, валидация 02 §9.3)
ShardAddedDto: cluster, name (сгенерированное shard<k>), replicas, requestCpu,
              requestMem, requestDisk (строки-каноны 02 §9.1),
              state:"NOT_INITIALIZED" — ответ 201 (§1.3)
MoveBucketsRequestDto: from, to, buckets[int[]] — тело POST заявок на переезды
              (§1.5, валидация 02 §9.7)
MovesQueuedDto: cluster, from, to, queued[int[]], skipped[int[]] — ответ 201 (§1.5)
AppPasswordRotatedDto: cluster, requestedUnix, requestedBy — ответ 201 заявки
              ротации app-пароля (§1.6, протокол 02 §9.8; значение пароля
              панели неизвестно — только факт заявки)
MoveTicketDto: bucketId(int? — null у неканонического leaf'а), bucket(raw-leaf),
              op(move|rollback|finalize|abort), to, requestedUnix, requestedBy —
              строка очереди заявок кластера (ClusterDto.pendingMoves)
```

`sinceUnix` алерта: `AlertEngine` сравнивает с прошлым снапшотом по
стабильному `id` (`kind:target`) — «присутствует с»; живёт в снапшоте, без
хранения истории.

`sharded` в `ClusterDto` — вычисляемое поле отображения: `false` ⟺ ровно
1 бакет и не более 1 шарда (`bucketsCount==1 && shards ≤ 1`). Признак «тип
БД» в etcd не хранится (02 §9.1: нешардированная пишется вырожденной 1×1),
поэтому осознанно созданный шардированный кластер 1×1 отображается как
нешардированный — для UI различие несущественно (таблица из одного бакета
на единственном шарде не информативна). Единственный потребитель поля —
решение «показывать ли вкладку Бакеты» (§3).

`masterlessShards` кластера в NOT_INITIALIZED всегда 0: «без мастера» у ещё
не поднятого кластера — ожидаемое состояние, не деградация (кластер помечен
`notInitialized`, UI показывает серым).

`activeMoves` (сводка кластера и Overview) считает только
`SYNCING|FROZEN|ABORTING`: `NOT_INITIALIZED` — не переезд, а начальное
состояние бакета (02 §9).

## 3. Панели UI

| Панель | Что показывает |
|---|---|
| **Login** | форма логин/пароль; ошибка 401 |
| **Overview** | бейдж stale; карточки: etcd (reachable, endpoints ok/total; alarms — в ленте алертов и на панели etcd), кластеры (шарды/бакеты/переезды), активные переезды списком, лента алертов (critical/warning); сводка HA: скольки scope'ов без лидера (клиентская агрегация `GET /api/ha` — `OverviewDto` HA-полей не содержит) |
| **etcd** | таблица endpoints (reachable, latency, версия, raftTerm, ошибки, метка «активный»), members (+лидер), alarms; `lastRefreshUtc` |
| **Clusters** | список: имя, dbname, N, шард мастеровых/всего, активные переезды, пометки (incomplete, not-initialized, «к удалению» при `toRemove`); кнопка «Создать кластер» → модальная форма (§3.1) |
| **Cluster details** | вкладки: Шарды (dsn, replicas, master+leaseAlive, sync-standby, лаг слотов; ноды: имя+state; заявка ресурсов на ноду cpu/mem/disk; колонка действий — кнопка «Убрать шард» (красная, per-row; диалог со счётчиком бакетов шарда, дизейбл при N>0 с пояснением «сначала перевезите бакеты», серверный 409 — текстом ProblemDetails); бейдж «к удалению» у шарда state=TO_REMOVE; кнопка «Добавить шард» в заголовке вкладки — модальная форма §3.2: реплики/CPU/память/диск, без имени — генерируется; подпись «Шард стартует пустым — перераспределение бакетов выполняется отдельными явными переездами»; кнопки скрыты, когда кластер не Active — симметрия с «Удалить кластер»), Бакеты (грид id×owner×state, фильтр по owner/state, подсветка не-ACTIVE, возраст; вкладка скрыта при `sharded=false` — нешардированная БД 1×1 без карты бакетов, 02 §9.1; кнопка «Перенести бакеты» в заголовке вкладки (только Active && sharded — canScale) — модальная форма §3.3), Переезды (только не-ACTIVE, кроме NOT_INITIALIZED: phase, updated, last_error; блок «Очередь заявок» — `pendingMoves` по возрастанию requestedUnix: бакет, op, to, возраст заявки, кем поставлена; исчезновение заявки без смены routing/status = отвергнута PgWorker'ом), Heals (журнал), «Стендовая топология» (блок по `standNodes` деталей — реестр `/cluster/nodes/`, скрыт при пустом); шапка: бейдж TO_REMOVE, кнопка «Сменить app-пароль» (только Active; → `POST /api/clusters/{cluster}/app-password/rotate` — 02 §9.8; модальное подтверждение с предупреждением «после применения подключения со старым паролем отвергаются до перечитывания кредов приложением — выполняйте в тихое окно»; 409 «уже запрошена» — текстом) и кнопка «Удалить кластер» (красная, с подтверждением; → `DELETE /api/clusters/{name}` — 02 §9.4; при `state=TO_REMOVE` обе кнопки скрыты — обратного перехода нет) |
| **HA** | список scope'ов: scope, cluster/shard, лидер, члены (роль/состояние), лаг max, пометка unmatched |
| **HA details** | leader, optime, таблица members: name/role/state/timeline/lag/probe-статус; блок «Заявленные ресурсы нод» (request_*, при наличии); raw config (свернуто) |
| **Alerts** | таблица всех алертов: severity-цвет, kind, target, message, since; фильтр по severity |

### 3.1. Форма «Создать кластер» (формы данных: эта + добавление шарда §3.2 + перенос бакетов §3.3)

Модальный диалог (Mantine Modal + TextInput/NumberInput) с кнопки «Создать
кластер» на панели Clusters. Поля: имя; бакеты; шарды (≤ бакетов); реплики
(дефолт 2, минимум 1 — только мастер); группа «Ресурсы нод (заявка, на каждую
ноду)»: CPU (ядра, шаг 0.1), память (GiB), диск (GiB). Клиентская валидация —
зеркало 02 §9.3 (быстрая ошибка у поля); серверная — источник истины.
Отправка — POST `/api/clusters`; успех → закрыть форму, инвалидировать
`clusters`-запросы (список обновится, новый кластер — с бейджем
«не инициализирован»); ошибка — ProblemDetails в теле формы (409 — «имя
занято», 400 — по полям, 503 — «etcd недоступен»). Двойной клик защищён
блокировкой кнопки на время мутации.

### 3.2. Форма «Добавить шард» (t06)

Модальный диалог с кнопки «Добавить шард» в заголовке вкладки Шарды на
Cluster details (только Active-кластер). Поля: реплики (дефолт 2, минимум 1 —
только мастер), группа «Ресурсы нод (заявка, на каждую ноду)»: CPU (ядра,
шаг 0.1), память (GiB), диск (GiB); поля имени НЕТ — имя генерирует сервер
(`shard<max+1>`, 02 §9.5). Подпись в форме: «Шард стартует пустым —
перераспределение бакетов выполняется отдельными явными переездами (UI
переездов — 02 §9.7)». Клиентская валидация — зеркало 02 §9.3; серверная —
источник истины. Отправка — POST `/api/clusters/{cluster}/shards` (§1.3);
успех → закрыть форму, инвалидировать `clusters`-запросы и детали кластера
(новый шард появится с нодами NOT_INITIALIZED → PROVISIONING → RUNNING по
мере подъёма PgWorker); ошибка — ProblemDetails в теле формы (409 — «кластер
не Active или имя занято», 400 — по полям, 503 — «etcd недоступен»). Двойной
клик защищён блокировкой кнопки на время мутации.

### 3.3. Форма «Перенести бакеты» (заявки на переезды)

Модальный диалог с кнопки «Перенести бакеты» в заголовке вкладки Бакеты на
Cluster details (только Active-кластер с `sharded=true` — canScale). Поля:
Select «Шард-источник» (все шарды, у каждого — счётчик его бакетов по
routing; шард в TO_REMOVE допустим — эвакуация перед демонтажем), Select
«Шард-приёмник» (все шарды кроме источника, не TO_REMOVE), чекбокс-список
бакетов выбранного источника (id, state; активны для выбора только ACTIVE;
бакеты с уже стоящей заявкой — disabled с бейджем «в очереди»; кнопки
«выбрать все» / «снять»). Подпись: «Переезды выполняются последовательно,
по одному бакету за раз (обрабатывает PgWorker); порядок — по возрастанию
id». Клиентская валидация — зеркало 02 §9.7 (непустой выбор, from ≠ to);
серверная — источник истины. Отправка — POST `/api/clusters/{cluster}/moves`
(§1.5); успех → сводка-Alert в открытой форме («поставлено в очередь: N,
уже стояли: M» при непустом `skipped`) с кнопкой «Готово» — закрывает форму
(тостов нет — notification-библиотека в проект не входит); инвалидация
`clusters`-запросов и деталей кластера — сразу (очередь заявок появится на
вкладке «Переезды» со следующего тика, переезды начнутся асинхронно);
ошибка — ProblemDetails в теле формы
(409 — guard'ы 02 §9.7, 400 — по полям, 503 — «etcd недоступен»). Двойной
клик защищён блокировкой кнопки на время мутации.

Общие элементы: переключатель интервала polling (2/5/15 с/off, default 5 с,
выбор сохраняется в localStorage), тёмная тема, авто-logout при 401
(redirect на /login), stale-бейдж в шапке layout'а — по `snapshotAgeMs`/`stale`
ответа `/api/overview`, опрашиваемого с текущим polling-интервалом (при
недоступности данных — «нет данных»), счётчики critical/warning у пункта
«Алерты» в навигации (клиентский подсчёт по ответу `/api/alerts`, опрашиваемому
с тем же интервалом; скрыты при нуле/ошибке). Форм ввода четыре: логин,
создание кластера (§3.1), добавление шарда (§3.2) и перенос бакетов (§3.3) —
всё остальное панель немая по отношению к данным.

## 4. Каталог алертов (`AlertEngine`)

Чистая функция `Snapshot → Alert[]`; severity: `critical` (прод горит),
`warning` (деградация/риск), `info` (заметка). Пороги — `AdminPanel:Alerts`.

| kind | severity | Условие | Источник |
|---|---|---|---|
| `etcd-unreachable` | critical | `consecutiveFailures ≥ 2` тиков | refresher |
| `etcd-no-quorum` | critical | raft-признаки отсутствия лидера / `status.errors` | `/v3/maintenance/status` |
| `etcd-endpoint-down` | warning | endpoint из настроек недоступен | status по endpoints |
| `etcd-alarm` | critical | есть alarms (NOSPACE и др.) | `/v3/maintenance/alarm` |
| `snapshot-stale` | warning | `BuiltAtUtc` старше `3×RefreshInterval` | refresher |
| `shard-no-master` | critical | `dsn` есть, `master`-ключа нет (P11: протухший lease) | `/clusters/…/master` |
| `shard-no-leader` | critical | HA-scope без `leader`-ключа, **кроме scope'ов кластера в NOT_INITIALIZED** (ноды ещё не подняты — 02 §9) | `/service/…/leader` |
| `cluster-not-initialized` | info | кластер в `NOT_INITIALIZED` (заявлен, ноды не подняты) — заметка, пока provisioning не переведёт в ACTIVE | config.state |
| `move-stale` | warning | status-ключ не-ACTIVE (кроме NOT_INITIALIZED) дольше `StaleMoveSeconds` (600 c) | `…/buckets/status/*` |
| `move-frozen-long` | critical | `FROZEN` дольше `FrozenSeconds` (60 c) — cutover обязан быть секундами | `…/buckets/status/*` |
| `move-aborting` | warning | `ABORTING` (незавершённая уборка, P7) | `…/buckets/status/*` |
| `move-flipped-status-stuck` | warning | status есть, routing уже = target (P7) | routing+status |
| `bucket-lost` | critical | routing → несуществующий шард (P23-а) | routing × shards |
| `bucket-no-routing` | warning | бакет из `0..N-1` без routing-ключа («дыра» карты) | routing × config |
| `bucket-out-of-range` | warning | routing-ключ с `N ≥ buckets` (P18) | routing × config |
| `cluster-incomplete` | warning | префикс `/clusters/<C>` без `config` | парсер |
| `key-malformed` | warning | ключ не разобран | парсер |
| `ha-member-not-streaming` | warning | Patroni-проба: member не `running/streaming` | Patroni REST |
| `replica-lag-high` | warning | лаг реплики > `ReplicaLagBytes` (16 МБ) | Patroni REST |
| `slot-lag-high` / `slot-wal-lost` | warning / critical | лаг слота > порога / `wal_status='lost'` (P4) | SQL-проба |
| `slot-invalidation-risk` | warning | `safe_wal_size` < порога (P4, ДО среза) | SQL-проба |
| `sync-standby-missing` | warning | у мастера нет `sync_state IN ('sync','quorum')` (P8 — предусловие переездов) | SQL-проба |
| `inventory-mismatch` | warning | фактические схемы `bucket_%` ≠ routing (P21/P23) | SQL-проба |
| `probe-failed` | info | Patroni/SQL-проба ошибки (детали в probe) | пробы |

SQL-алерты вычисляются только при включённых пробах; etcd-алерты — всегда.
`NOT_INITIALIZED`-бакеты — не переезды: `move-*` правила их не алертят
(`move-frozen-long`/`move-aborting` смотрят свои точные состояния,
`move-flipped-status-stuck` — требует `target`, у NOT_INITIALIZED его нет);
бейдж «не инициализирован» в UI + `cluster-not-initialized` (info) вместо
critical-шума от ещё не поднятого кластера.

## 5. SQL-каталог пробы (read-only, только `pg_catalog`/`pg_stat_*`)

Выполняются на мастере каждого шарда (DSN из etcd + пароль панели;
`default_transaction_read_only=on`):

```sql
-- sync-standby и лаги физических реплик (P8)
select application_name, client_addr, state, sync_state, pg_wal_lsn_diff(
         pg_current_wal_lsn(), replay_lsn) as lag_bytes
from pg_stat_replication;

-- слоты переездов: лаг/риск среза (P4)
select slot_name, slot_type, active, wal_status, safe_wal_size, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
from pg_replication_slots;

-- прогресс подписок (переезды)
select subname, received_lsn, latest_end_lsn, latest_end_time
from pg_stat_subscription;

-- инвентарь бакетов (сверка с routing, P21/P23)
select nspname from pg_namespace where nspname like 'bucket\_%' escape '\';

-- роль ноды (мастер или реплика)
select pg_is_in_recovery();
```

Образцы и тонкости (например, `like`-экранирование `_`) — из
`arch/scripts/buckets-common.sh`; запросы не меняются в SQL-семантике
без правки этого документа.

## 7. Kafka: панель и REST API (`/api/kafka/*`)

Третий домен (спец-файл kafka-admin-worker §5; etcd-контракт — 02 §10,
канон ключей — arch/15). Источник данных — отдельный снапшот `KafkaSnapshot`
(свой refresher, тик 3 с) + опциональная live-проба (тип 15 с, DescribeCluster;
группы/лаги — arch/15 §3-домен волны C). Пароль `app_password` в UI/API
не отдаётся никогда (02 §10.1).

### 7.1. Список эндпоинтов

| Метод+путь | Назначение |
|---|---|
| `GET /api/kafka/clusters` | сводный список kafka-кластеров |
| `POST /api/kafka/clusters` | создание кластера (02 §10.2-1): тело `CreateKafkaClusterRequestDto` → 201+`KafkaClusterCreatedDto` \| 400 \| 409 \| 503 |
| `GET /api/kafka/clusters/{cluster}` | детали: config, брокеры, топики (desired/missing), ротация; groups+lags — из пробы (волна C) |
| `DELETE /api/kafka/clusters/{cluster}` | перевод в TO_REMOVE (02 §10.2-2): 204 \| 404 \| 503 |
| `PUT /api/kafka/clusters/{cluster}/config` | изменение default-конфигов (02 §10.2-3): тело `KafkaConfigUpdateRequestDto` → 204 \| 400 \| 404 \| 409 \| 503 |
| `POST /api/kafka/clusters/{cluster}/brokers` | добавление брокера (02 §10.2-4): тело `AddKafkaBrokerRequestDto` → 201+`KafkaBrokerAddedDto` \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/brokers/{broker}` | маркер демонтажа брокера TO_REMOVE (02 §10.2-5): 204 \| 404 \| 409 \| 503 |
| `PUT /api/kafka/clusters/{cluster}/topics/{topic}` | конфиг-заявка топика (02 §10.2-6, волна C): тело `KafkaTopicDesiredRequestDto` → 204 \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired` | отмена конфиг-заявки (02 §10.2-7, волна C): 204 \| 404 \| 503 |
| `POST /api/kafka/clusters/{cluster}/app-password/rotate` | заявка ротации app-пароля (02 §10.2-8): без тела → 201+`KafkaPasswordRotatedDto` \| 404 \| 409 \| 503 |
| `POST /api/kafka/clusters/{cluster}/rebalance` | заявка ребалансировки партиций (02 §10.2-9): без тела → 201+`KafkaRebalanceRequestedDto` \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/rebalance` | отмена заявки ребалансировки (02 §10.2-10): 204 \| 404 \| 503 |

`GET /api/alerts` объединяет алерты обоих движков (kind уже различает
`kafka-*`); `GET /api/overview` получает kafka-сводку (clustersTotal,
clustersCritical — critical-алерты `kafka-broker-not-running`/
`kafka-endpoints-missing`).

### 7.2. DTO (ключевые поля)

```text
KafkaClusterSummaryDto: name, state(ACTIVE|NOT_INITIALIZED|TO_REMOVE),
    brokersTotal, brokersRunning, topicsCount, endpoints, rotationPending(bool)
KafkaClusterDto: name, state, replicationFactor, minInSyncReplicas,
    defaultPartitions, defaultRetentionMs, createdUnix, endpoints,
    brokers[KafkaBrokerDto], topics[KafkaTopicDto], groups[KafkaGroupDto]
    (волна C — из пробы), rotation{requestedUnix, requestedBy}?(nullable),
    rebalance{requestedUnix, requestedBy}?(nullable),
    reassignment{mode(DRAIN|BALANCE), drainBroker?, partitionsTotal,
    partitionsRemaining, updatedUnix}?(nullable — ключа нет = операции нет)
KafkaBrokerDto: name, state(raw: NOT_INITIALIZED|PROVISIONING|RUNNING|
    UNREACHABLE|REMOVING|TO_REMOVE), role(controller|broker|null — до
    provisioning), cpu, memGi, diskGi (nullable — заявка resources),
    live(bool|null — из пробы: брокер id/host виден в DescribeCluster),
    brokerId(int|null — из пробы)
KafkaTopicDto: name, partitions, replicationFactor, retentionMs, minInSyncReplicas
    (null — конфиг отсутствует в факте), desired{partitions, retentionMs,
    minInSyncReplicas, requestedUnix, requestedBy}?(nullable), missing(bool),
    syncedUnix
KafkaGroupDto: group, state, members, totalLag (волна C — из пробы)
CreateKafkaClusterRequestDto: name, brokers(1..9 def 3), replicationFactor
    (1..9 ≤ brokers def 3), minInSyncReplicas(1..RF def 2), defaultPartitions
    (1..1000 def 12), defaultRetentionMs(1..2147483647 def 604800000),
    cpu(0.01..64 def 2), memGi(1..65536 def 2), diskGi(1..65536 def 20)
    — валидация 02 §10.3
KafkaClusterCreatedDto: name, state:"NOT_INITIALIZED", brokers,
    replicationFactor, minInSyncReplicas, defaultPartitions,
    defaultRetentionMs, cpu, memGi, diskGi
KafkaConfigUpdateRequestDto: replicationFactor?, minInSyncReplicas?,
    defaultPartitions?, defaultRetentionMs? (хотя бы одно; границы 02 §10.3)
AddKafkaBrokerRequestDto: cpu, memGi, diskGi (границы 02 §10.3)
KafkaBrokerAddedDto: cluster, name (сгенерированное broker<k>), cpu, memGi,
    diskGi, state:"NOT_INITIALIZED"
KafkaTopicDesiredRequestDto: partitions?, retentionMs?, minInSyncReplicas?
    (хотя бы одно; partitions только > фактического) — волна C
KafkaPasswordRotatedDto: cluster, requestedUnix, requestedBy
KafkaRebalanceRequestedDto: cluster, requestedUnix, requestedBy
```

### 7.3. Панели UI

| Панель | Что показывает |
|---|---|
| **KafkaClusters** | список: имя, state-бейдж, брокеры running/всего, топики (кол-во), endpoints (сокращённо), бейдж ротации; кнопка «Создать кластер» → модальная форма §7.3.1 |
| **KafkaClusterDetails** | шапка: state-бейджи (TO_REMOVE/NOT_INITIALIZED), бейдж reassignment («drain broker4: осталось 5/12 партиций» / «ребалансировка: 7/20»), кнопки «Изменить параметры» (default-конфиги — модал), «Сменить app-пароль» (модал-предупреждение о rolling-перезапуске брокеров; 409 «уже запрошена» — текстом), «Перебалансировать» (модал-предупреждение о переносе данных между брокерами; живая заявка — «Отменить ребалансировку»; 409 — текстом), «Удалить кластер» (красная, подтверждение; при TO_REMOVE скрыты); вкладка **Брокеры**: name/state/role/resources/live, колонка действий «Убрать брокера» (controller/последний — дизейбл с пояснением, серверный 409 текстом; непустой — подпись «drain: осталось N партиций» по прогресс-ключу, кнопка активна: воркер сам дренирует и демонтирует) + кнопка «Добавить брокера» (форма resources); вкладки **Топики** и **Группы** — волна C (до неё — заглушка) |

### 7.3.1. Форма «Создать kafka-кластер»

Модальный диалог (Mantine) с кнопки «Создать кластер» на панели Kafka:
имя; брокеры (def 3); RF (def 3, ≤ брокеров); minISR (def 2, ≤ RF); партиции
(def 12); retention ms (def 7 дней); группа «Ресурсы брокера»: CPU/память/
диск (def 2/2/20). Клиентская валидация — зеркало 02 §10.3; серверная —
источник истины. Отправка — POST `/api/kafka/clusters`; успех → инвалидация
списка (новый кластер с бейджем «не инициализирован»); ошибка —
ProblemDetails в теле формы. Двойной клик — блокировка кнопки.

### 7.4. Каталог kafka-алертов (`KafkaAlertEngine`)

Чистая функция `KafkaSnapshot (prev, next) → Alert[]`; пороги —
`AdminPanel:KafkaAlerts`. sinceUnix — по стабильному `id = kind:target`
(§2-механика). Ротационный алерт живёт только у живого кластера: заявка
ротации удаляется демонтажем кластера (arch/16 X-фазы) — вечный
`kafka-rotation-pending` невозможен по построению.

| kind | severity | Условие |
|---|---|---|
| `kafka-cluster-not-initialized` | info | state=NOT_INITIALIZED |
| `kafka-cluster-to-remove` | info | state=TO_REMOVE |
| `kafka-broker-not-running` | critical | Active-кластер, broker state ∉ {RUNNING}, кроме fresh-PROVISIONING (< 60 с) |
| `kafka-endpoints-missing` | critical | Active без `endpoints` |
| `kafka-rotation-pending` | info | живая заявка ротации `/kafkaworker/rotations/<C>` |
| `kafka-rebalance-pending` | info | живая заявка ребалансировки `/kafkaworker/rebalances/<C>` |
| `kafka-reassignment-stale` | warning | прогресс-ключ `/kafkaworker/reassignments/<C>` жив, но `partitions_remaining` не двигается дольше `ReassignStaleSec` (900) — drain/баланс буксует |
| `kafka-key-malformed` | warning | kafka-ключ не разобран (parseError) |
| `kafka-topic-missing-desired` | warning | topics: `missing=true` (волна C) |
| `kafka-desired-stale` | warning | desired не снят дольше `StaleDesiredSec` (600) — волна C |
| `kafka-topic-under-replicated` | warning | проба: партиции с USR>0 — волна C |
| `kafka-group-lag-high` | warning | проба: totalLag > `GroupLagMessages` (100000) — волна C |

## 8. Версионирование контракта

Контракт API не версонируется (панель и API развёртываются одним артефактом,
фронт и бэк всегда согласованы). Изменение DTO — правкой этого документа
тем же PR, что и код.
