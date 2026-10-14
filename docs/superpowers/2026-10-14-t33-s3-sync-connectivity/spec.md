# t33-s3-sync-connectivity — спецификация

Контроль синхронности и связности кластеров ownS3: зонд S3Worker (процесс
F, arch/24 §5) — периодический и по команде API — пишет два ключа
наблюдения etcd (`connectivity`, `sync` — arch/23 §3): ping-матрица
настроенных линков и сверка содержимого bucket между нодами. Панель (t34)
и оператор получают честный источник истины о репликации: штатные метрики
ownS3 лаг данных не отражают (факт 7 `docs/s3-replication.md`). UI — вне
задачи; клиентская интеграция пула — t36.

## 1. Контекст и цель

S3Worker (канон [arch/24-s3worker.md](../../../arch/24-s3worker.md)) —
оркестратор S3-кластеров по декларативному контракту etcd
([arch/23-s3-clusters.md](../../../arch/23-s3-clusters.md)). Ноды —
инстансы ownS3 с mesh-репликацией (канон
[arch/owns3/07-replication.md](../../../arch/owns3/07-replication.md)):
уведомления + pull, доставку govern'ит доводка G (links → targets/join,
t40). В ветке уже реализованы: PKI peer-auth, join/deny (процесс G),
активная проба линков `POST /admin/replication/probe` (t37:
`ReplicationProber` + `GET /replication/ping`), админ-сверка листингов
`GET /admin/replication/sync-report` (XOR FNV-1a-свёртка пар
`(key, modTime)`), 202-заглушка `POST /api/s3/clusters/{c}/sync-probe`.

До задачи наблюдения синхронности/связности нет: ключи `connectivity`/
`sync` не пишутся (парсер относит их к unknownKeys), состояние mesh видно
только косвенно (журнал G, метрики ownS3, лаг которых данных не отражает).

Цель задачи:

1. **Процесс F (SyncProbeProcess)** в Active-ветке тика: периодический
   (гейт `SyncIntervalSec`) и по-командный (заявка API) прогон зонда,
   пишущий оба ключа наблюдения.
2. **Connectivity** — ping-матрица только НАСТРОЕННЫХ линков: зонд
   опрашивает `POST /admin/replication/probe` каждой RUNNING-ноды —
   состав её targets и живость каждого линка. PUT-пробы не используются:
   путь записи ownS3 не содержит синхронных взаимодействий с соседями
   (owns3/07 §1 — PUT всегда 0 с); ping-ок = «канал жив», отдельно от
   доставки и сверки данных.
3. **Sync** — сверка содержимого bucket per-install между нодами по
   настроенным парам: постраничная сверка `sync-report` (свёртка), при
   расхождении — количественный diff листингов S3-грани (`missing`/
   `extra`/`lag_bytes`); лаг доставки — fetch-очередь приёмника из
   статуса (`lag_objects`).
4. **API**: реализация `POST /api/s3/clusters/{c}/sync-probe` — заявка
   внепланового прогона (202), результат — в etcd.
5. Полнота меша и зеркальность состава НЕ контролируются (зона
   оркестратора G): частичный/незеркальный mesh дефектом связности не
   считается; линков не настроено — connectivity OK с пустой матрицей.

## 2. Канонические источники

- Текст задачи — [arch/roadmap/s3.md](../../../arch/roadmap/s3.md),
  пункт `t33-s3-sync-connectivity` (границы зонда и arch-first правки).
- Контракт etcd — [arch/23-s3-clusters.md](../../../arch/23-s3-clusters.md)
  §3 (ключи `connectivity`/`sync`, state-агрегаты — правка этой задачи:
  ping-матрица вместо PUT-проб, агрегаты по настроенным линкам), §8
  (новый координационный ключ `/s3worker/probes/<C>`).
- Оркестратор — [arch/24-s3worker.md](../../../arch/24-s3worker.md):
  §1.1 (эндпоинт `sync-probe`), §5 F (процесс зонда — правка этой
  задачи), §5 (классификация тика: F — последняя ступень Active-ветки
  C→D→E→G→F), §7 (метрики `sync_probe_*`), §8 (`SyncIntervalSec`).
- Грани ownS3 — [arch/owns3/07-replication.md](../../../arch/owns3/07-replication.md):
  §1 (принцип 1 — путь записи без соседей: почему PUT-пробы бессмысленны),
  §2 (`POST /admin/replication/probe` — контракт ответа
  `{active, targets:[{name, reachable, rttMs?}]}`, параллельность,
  per-target таймаут, probe не меняет состояние), §10 (`sync-report` со
  свёрткой `aggregateHash` — «внешний зонд синхронности трека s3
  опирается на этот эндпоинт»; `GET /admin/replication/status` —
  fetch-очереди/лаги per-source).
- Требования — `docs/owns3-replication-requirements.md` (R5 — сверка
  листингов как контрольный механизм), справочник фактов
  `docs/s3-replication.md` (факт 7 — метрики не отражают лаг данных).
- Реализация грани (готова, переиспользуется):
  `src/OwnS3.Replication/ReplicationProber.cs`,
  `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs`
  (`HandleProbeAsync`, `HandleSyncReportAsync`).

## 3. Ключевые решения дизайна

### 3.1. Зонд — читатель факта, не надзиратель

Зонд F ничего не чинит и не мутирует: только опрашивает грани нод и
пишет два ключа etcd (+ del заявки). Лечение — механика ownS3
(notifier/reconcile) и доводка G; зонд — источник истины для панели
(t34) и оператора. Это фиксирует roadmap: «настройка состава — зона
оркестратора, не зонда»; частичный/незеркальный mesh дефектом не
считается, зонд отражает то, что настроено фактически.

Обе половины зонда пишутся одним прогоном (общий `checked_unix`),
probe-опрос нод выполняется однократно — его результат кормит и матрицу
connectivity (состав targets + живость), и список пар сверки sync
(объединение targets).

### 3.2. Connectivity: ping-матрица настроенных линков

Шаги прогона:

1. Состав опроса — ноды кластера со `state=RUNNING` (не-RUNNING не
   опрашиваются: их «вне контура» несёт `nodes/<I>/state`; адреса — из
   `/s3worker/portalloc/<C>`, advertised-хост по правилу надзора
   `AdvertisedHost ?? host`). 0 RUNNING — no-op прогона (ключи не
   пишутся, journal-фаза `idle`).
2. Для каждой RUNNING-ноды — `POST /admin/replication/probe` (без
   `target` — все соседи параллельно; owns3/07 §2): ответ 200
   `{active, targets:[{name, reachable, rttMs?}]}`.
3. Строка матрицы ноды X — только её ФАКТИЧЕСКИЕ targets (состав
   `targets.json` инстанса, не декларация `links`): для каждого
   `{name: Y, reachable}` → `links.X.Y = reachable ? "ok" : "offline"`.
   `rttMs` в etcd не пишется (формат канона его не содержит).
4. Транспортный отказ probe-вызова ноды X (таймаут/не-2xx): строки X в
   матрице нет; диагностика — в `probe_error`; линки опрошенных соседей
   на X отразятся их собственными ответами (`reachable=false` — ping
   мёртвой ноды даёт таймаут). Зонд НЕ реконструирует состав X из
   прошлого значения или декларации — только факт текущего опроса.
5. Агрегат `state` (по собранной матрице — «настроенные линки»):
   - матрица пуста (никому не настроено линков) → `OK` с пустой
     `links` — пустой состав = OK (roadmap);
   - есть линки, все `ok` → `OK`;
   - часть линков `offline` → `DEGRADED`;
   - линков > 0, живых нет → `DOWN`;
   - полный отказ опроса (0 ответивших при непустом составе RUNNING) →
     `DOWN` + `probe_error` (связность не подтверждена ни по одному
     линку — алерт панели обязан всплыть; диагностика в `probe_error`).
6. Ключ `/s3/clusters/<C>/connectivity`, канонический JSON (arch/23 §3;
   порядок полей канона, snake_case):

```json
{"state":"OK","checked_unix":1760156900,
 "links":{"east1":{"east2":"ok"},"east2":{"east1":"ok"}}}
```

`probe_error` — опционально (непустая строка-диагностика опроса: какие
ноды не ответили и почему, усечённая как `last_error` журнала). Пишет
только зонд; X3 deprovisioning уносит ключ вместе с префиксом кластера.

### 3.3. Sync: пары, сверка листингов, агрегаты

**Пары** — неориентированные, из объединения направленных target-записей
всех опрошенных нод (тот же probe-опрос, что §3.2): пара {a,b}
существует ⟺ a держит target b ИЛИ b держит target a. Уникальные пары
упорядочиваются (`from = min(a,b)`, `to = max(a,b)` по Ordinal —
детерминизм записей между прогонами). Линков нет → пустой `pairs`,
state `SYNCED` (нечего сверять; отсутствие репликации — не дефект
состава, зона G/панели t34).

**Сверка пары {from,to}** (bucket per-install `S3Worker:Bucket`):

1. **Прет-чек sync-report** (админ-грань, SigV4 root-креды профиля):
   постранично от `marker=""` у обеих сторон; страницы равны
   (`count`, `firstKey`, `firstModTime`, `lastKey`, `lastModTime`,
   `totalSize`, `aggregateHash`, `nextMarker`) → следующая страница от
   общего `nextMarker`, до пустого `nextMarker` у обеих. Все страницы
   равны → пара без расхождений (`missing=0, extra=0`).
2. **Количественный diff** (первая расхождение-страница): полные
   постраничные листинги обеих сторон S3-гранью (ListObjectsV2,
   root-креды; элемент — `(key, lastModified→unixMs, size)`). Элемент
   сверки — `(key, modTimeUnixMs)` (как свёртка `aggregateHash`):
   - `missing` — элементы from, отсутствующие у to (нет ключа ИЛИ другая
     версия — иной modTime);
   - `extra` — элементы to, отсутствующие у from (симметрично);
   - `lag_bytes` — сумма `size` элементов из `missing` (объём
     недоставленного в направлении from→to; опциональное поле, пишется
     при `missing > 0`).
3. **Лаг доставки** `lag_objects` — fetch-очередь приёмника: из
   `GET /admin/replication/status` ноды `to` по соседу `from` —
   `fetchQueueDepth` (объектов ждут фетча; «догон известен»).
   Опциональное поле, пишется при `fetchQueueDepth > 0`.

**Бюджет страниц**: у каждой стороны пары не более `SyncMaxPages`
(конфиг, дефолт 20 = 20 000 объектов) страниц за прогон. Превышение —
пара не сверена до конца: в `pairs` не попадает, диагностика в
`last_error` («листинг пары east1↔east2 превышает бюджет страниц»).
Любая несвёрнутая пара не позволяет утверждать SYNCED: агрегат не лучше
`LAGGING` (+ `last_error`). Это защищает от ложного SYNCED на больших
бакетах при урезанном бюджете.

**Вердикты пары** (считаются per-pair, агрегат — худший):

- `synced` — `missing=0 ∧ extra=0`;
- `lagging` — есть расхождения, НО есть свидетельство прогресса
  доставки: `lag_objects > 0` (fetch-очередь приёмника непуста) ИЛИ
  сумма `missing+extra` пары меньше, чем в прошлом прогоне (догон
  сокращается — зонд читает предыдущее значение ключа `sync` перед
  записью: единственный писатель — сам зонд под клэймом, гонки нет);
- `diverged` — есть расхождения БЕЗ свидетельства прогресса:
  `lag_objects = 0` (доставка не активна) И `missing+extra` не
  сократились/выросли относительно прошлого прогона. «Дыра»:
  reconcile ownS3 (период 60 с) либо уже закрыл бы её, либо расхождение
  устойчиво — алерт оператору. Прошлого значения нет (первый прогон
  зонда) — свидетельства устойчивости тоже нет → `lagging` (не
  `diverged`).

**Агрегат** `state` ключа: все пары `synced` (пар 0 — тоже) → `SYNCED`;
есть `diverged` → `DIVERGED`; иначе (есть `lagging` или несвёрнутые
пары) → `LAGGING`.

Ключ `/s3/clusters/<C>/sync`, канонический JSON (порядок полей канона):

```json
{"state":"LAGGING","checked_unix":1760156900,
 "pairs":[{"from":"east1","to":"east2","missing":3,"extra":0,
           "lag_bytes":1048576,"lag_objects":3}]}
```

`last_error` — опционально (диагностика: недоступные листинги, бюджет
страниц, отказы статуса). Пример канона SYNCED — arch/23 §3 (все нули).

### 3.4. Триггеры прогона: период + команда

- **Периодический** — гейт по времени: прогон идёт, если ключ
  `connectivity` отсутствует ИЛИ `checked_unix` старше `SyncIntervalSec`
  (паттерн usage-сбора надзора: гейт по `updated_unix` значения ключа;
  обе половины пишутся вместе — одного гейта достаточно).
- **По команде** — заявка `/s3worker/probes/<C>` (обычный ключ;
  `{"requested_unix","requested_by"}` — формат rotations, arch/23 §8):
  зонд в начале тика видит заявку → прогон НЕЗАВИСИМО от гейта → после
  успешной записи обоих ключей — del заявки. Прогон не удался (Failed) —
  заявка живёт, ретрай следующим тиком (идемпотентность). Заявка не
  эксклюзивна: Active-ветка тика идёт целиком (C→D→E→G→F), в отличие от
  ротации; зонд — последние 2–3 etcd-записи прогона, конфликтов с G нет.
- Заявку ставит API (§3.7) put-if-absent (txn): повторный вызов при
  живой заявке → 202 без перезаписи (идемпотентно; `requested_by`
  первого заявителя сохраняется). Stale-заявка невозможна: сносится тем
  же прогоном, который её увидел (неуспешный прогон держит заявку до
  успеха — это желаемое поведение «команда исполнена»).

### 3.5. Процесс F в тике

`SyncProbeProcess` — восьмой процесс воркера (arch/24 §5 F), класс в
`S3Worker.Provisioning/Processes` по образцу `MeshGovernanceProcess`:

```
вход: снапшот кластера (состав/state нод), клэйм <C> (уже держим —
      Active-ветка), порталлок-адреса, креды профиля (SecretProfileStore),
      заявка probes (чтение), прошлые значения connectivity/sync (гейт
      и критерий прогресса)
шаги:
  1. 0 RUNNING → journal idle, del заявки (если была — исполнена
     «нечем зондировать»), выход Success;
  2. заявка есть ИЛИ гейт истёк → прогон; иначе — выход Success
     (journal не пишем: тихий тик);
  3. probe-опрос всех RUNNING-нод (последовательно по нодам; внутри
     ноды параллелит сама грань) → матрица §3.2 + пары §3.3;
  4. статус приёмников (для lag_objects — только если есть пары);
  5. свёрка пар §3.3 (sync-report → diff при расхождении);
  6. put connectivity, put sync (один `checked_unix`);
  7. del заявки (была); journal-фаза (probed/idle, warnings →
     last_error-подобная диагностика фазы).
```

- Все etcd-операции — failover по endpoints (S3ProcessCommon); отказ
  записи ключа → Failed прогона (ретрай тиком, заявка жива).
- Креды профиля недоступны (env) → диагностический skip (journal,
  образец G): грани ownS3 недостижимы без root-кредов.
- Порталлок-записи нет у RUNNING-ноды → warning, нода не опрашивается
  (transient — healит PortAllocHealer).
- Journal `op=sync-probe`: фазы `idle` / `probed` (+warnings) /
  `probe-failed` (last_error). Трек не затирает supervision-трек (S6).
- Последовательность в Active-ветке — последним (после G): зонд видит
  состав mesh уже доведённым тика.

### 3.6. Расширения клиента грани (`IOwnS3NodeClient`)

Новые методы (все — таймаут 7.5 с поллинг-инварианта, ретраи — тик;
camelCase-тела/запросы по контракту граней):

1. `ProbeAsync(host, port, creds)` → `Result<OwnS3ProbeReport>` —
   `POST /admin/replication/probe` (SigV4 root): 200 →
   `{active, targets:[{name, reachable, rttMs?}]}`; транспорт/не-2xx →
   Failed (транзиент — нода «не опрошена», §3.2 п.4).
2. `GetSyncReportPageAsync(host, port, creds, bucket, marker)` →
   `Result<OwnS3SyncReportPage>` — `GET /admin/replication/sync-report`
   (SigV4 root, query `bucket`/`marker`): страница
   `{count, firstKey, firstModTime, lastKey, lastModTime, totalSize,
   aggregateHash, nextMarker}` (nextMarker null/пусто — конец).
3. `ListObjectEntriesAsync(host, port, creds, bucket, marker)` →
   `Result<OwnS3ListPage>` — ListObjectsV2 (AWSSDK, по образцу
   `EnsureBucketAsync`; ForcePathStyle, retries off): страница записей
   `(key, modTimeUnixMs, size)` + nextContinuationToken. ModTime —
   миллисекунды (LastModified → unix-мс).
4. `GetReplicationStatusAsync` — расширение модели: per-neighbor записи
   `{name, fetchQueueDepth, fetchLagSeconds, fetchPaused,
   reconcileRunning}` (состав полей — owns3/07 §10); поле
   `NeighborNames` сохраняется (вычисляется из списка — G не меняется).

Клиент остаётся единственной точкой транспорта к граням ownS3; процесс
F не строит HTTP сам.

### 3.7. HTTP API: `POST /api/s3/clusters/{c}/sync-probe`

Реализация заглушки `SyncProbeHandler` (маршрут/метрики уже стоят):

- Гварды: config есть → иначе 404 `S3ClusterNotFoundException`;
  `state=Active` (нет поля state) → иначе 409
  `S3ClusterNotActiveException` (зонд осмысленен для живого кластера;
  NOT_INITIALIZED/TO_REMOVE — зоны A/B).
- Тело отсутствует (без параметров); источник `requested_by` — заголовок
  `X-Requested-By` (дефолт `"api"`), по образцу заявки ротации
  (`ApiModule.RequestedBy`).
- Запись: txn put-if-absent `/s3worker/probes/<C>` значения
  `{"requested_unix":now,"requested_by":<who>}`; проигрыш txn (заявка
  есть) → 202 без перезаписи (идемпотентно).
- Ответ: 202 Accepted (исполнение — ближайший тик держателя клэйма —
  секунды; результат — ключи arch/23 §3, панель/оператор читают их).

### 3.8. Парсер и снапшот

- `S3SnapshotParser`: лист-ключи `connectivity`, `sync` в префиксе
  кластера — распознанные служебные ключи наблюдения: НЕ unknownKeys,
  НЕ в снапшот тика (зонд читает их напрямую при гейте/прогрессе —
  как `pki`-материал у t40). Обратная совместимость: сегодня они
  попадают в unknownKeys (счётчик) — после задачи шум исчезает.
- `S3ProcessCommon`: ключи-хелперы `ConnectivityKey(cluster)`,
  `SyncKey(cluster)`, `ProbeRequestKey(cluster)`
  (`/s3worker/probes/<C>`).
- Модель: без изменений снапшота (`S3ClusterSnapshot` не расширяется).

### 3.9. Метрики и наблюдаемость

Meter `s3worker` (каркас arch/24 §7, рядом с `mesh_*`):

| Метрика | Тип | Смысл |
|---|---|---|
| `s3worker_sync_probe_runs_total{result}` | counter | прогоны зонда: ok / failed / idle |
| `s3worker_sync_probe_links_total{result}` | counter | линков за прогон: ok / offline |
| `s3worker_sync_probe_pairs_total{result}` | counter | пар сверки: synced / lagging / diverged |

Диагностика фаз — journal `/s3worker/work/<C>` (`op=sync-probe`);
статусы — ключи `connectivity`/`sync` (панель t34); значения секретов в
журнал/метрики не попадают.

### 3.10. Надёжность и границы

- **Идемпотентность**: повторный прогон теми же данными переписывает
  ключи тем же каноном; del заявки после успеха; снос кластера (X3)
  уносит оба ключа и заявку.
- **Поллинг-инвариант**: каждый вызов грани ≤7.5 с, ретраи — тиками
  (никаких внутренних retry-циклов); последовательность страниц —
  счётчик `SyncMaxPages`-бюджет (§3.3) ограничивает длительность
  прогона детерминированно.
- **Multi-host**: зонд ходит по advertised-адресам из portalloc (то же
  правило, что endpoints/надзор) — работает с любого инстанса воркера;
  все записи — etcd (реплицированный источник), локального состояния
  нет; «что теряется при отказе машины X» — ничего зонда (ключи в etcd,
  прогон повторит любой инстанс после takeover клэйма ≤ TTL 15 с).
- **Клэйм**: все записи зонда — только под живым клэймом `<C>` (S5);
  зонд не конкурирует с G за одни ключи (G — targets/deny, F —
  connectivity/sync/probes).
- **UNREACHABLE-нода**: не опрашивается (state ≠ RUNNING); «нода вне
  контура» отражает `nodes/<I>/state` (надзор) — алерты t34 читают
  оба источника.
- Зонд не трогает данные/курсоры/очереди нод (probe объявлен
  не-мутирующим — owns3/07 §2; sync-report/листинги — чтения).

## 4. Arch-first правки (выполнены; канон обновлён ДО кода)

1. **arch/23-s3-clusters.md §3**: connectivity — ping-матрица
   настроенных линков через `POST /admin/replication/probe` (вместо
   PUT-проб): матрица только по targets нод, полнота/зеркальность не
   контролируются, ping-ок = «канал жив» отдельно от доставки и сверки;
   PUT-пробы связность не детектируют (owns3/07 §1). Sync — пары по
   настроенным линкам, элемент сверки `(key, modTimeUnixMs)`, семантика
   missing/extra. Агрегаты: DOWN — нет живых среди настроенных при
   непустом составе; DEGRADED — часть offline; пустой состав — OK;
   алерты t34 (расхождение, отставание, нода вне контура). Пример
   connectivity — актуализирован (строка ноды = исходы её probe).
2. **arch/23-s3-clusters.md §8**: ключ `/s3worker/probes/<C>` — заявка
   внепланового зонда (`{"requested_unix","requested_by"}`, put-if-
   absent, del зонда по исполнении); панель его не читает
   (координационный).
3. **arch/24-s3worker.md §1.1**: `sync-probe` — 202-заявка (уже
   канонизированный эндпоинт), already → 202 идемпотентно.
4. **arch/24-s3worker.md §5 F**: зонд — реализация t33: периодический
   гейт `SyncIntervalSec` + заявка; обе половины (connectivity/sync) с
   механикой по граням owns3/07; per-попыточный таймаут + пульс.
5. **arch/24-s3worker.md §7**: счётчики `sync_probe_*`.
6. **arch/24-s3worker.md §8**: `Thresholds.SyncIntervalSec=60`.

Правки owns3/07 не потребовались: грани probe/sync-report/status уже
канонизированы и реализованы (t35/t37) — зонд только потребитель.

## 5. Отражение в коде (после одобрения плана; Фаза 6)

Проекты: `src/S3Worker.{Core,Etcd,Provisioning,App}`, тесты
`src/tests/S3Worker.{UnitTests,IntegrationTests}`. Код — только после
plan.md; состав изменений:

1. **S3Worker.Core**
   - `OwnS3/OwnS3NodeClient.cs`: методы §3.6 (ProbeAsync,
     GetSyncReportPageAsync, ListObjectEntriesAsync; расширение модели
     статуса per-neighbor с сохранением `NeighborNames`) + DTO
     (`OwnS3ProbeReport/Target`, `OwnS3SyncReportPage`,
     `OwnS3ListPage/Entry`, расширенный `OwnS3ReplicationStatus`).
2. **S3Worker.Etcd**
   - `Parsing/S3SnapshotParser.cs`: `connectivity`/`sync` — известные
     служебные листы (не unknownKeys, не в снапшот).
3. **S3Worker.Provisioning**
   - `Processes/S3ProcessCommon.cs`: `ConnectivityKey`/`SyncKey`/
     `ProbeRequestKey` + канонические сериализаторы значений
     (connectivity/sync JSON — порядок полей канона) и парсер прошлого
     значения sync (прогресс-критерий).
   - Новый `Processes/SyncProbeProcess.cs` (§3.2–§3.5): без docker, на
     фейке `IOwnS3NodeClient` (юнит-тестируем).
4. **S3Worker.App**
   - `Api/Operations/ClusterHandlers.cs`: реализация `SyncProbeHandler`
     (гварды 404/409, txn put-if-absent заявки, 202).
   - `Loops/S3ClusterProcesses.cs`: F в Active-ветке после G; DI-связка
     в `Program.cs`.
   - `Options.cs` + `appsettings.json`: `Thresholds.SyncIntervalSec=60`,
     `SyncMaxPages=20` (проброс в плоский срез `S3ProvisioningOptions`).
   - Метрики §3.9 (Meter `s3worker`, рядом с mesh-счётчиками).
5. **Тесты** — §8.

## 6. Фазы реализации (после одобрения spec → plan → execute)

1. **Клиент грани**: расширения §3.6 + юнит-тесты (парсинг ответов
   probe/sync-report, страницы листингов, обратная совместимость
   NeighborNames).
2. **Процесс F**: `SyncProbeProcess` (матрица, пары, свёртка, diff,
   вердикты, гейт, заявка) + юнит-тесты на фейках (§8.1).
3. **API + парсер + конфиг**: хендлер, ключи-хелперы, unknownKeys-тихо,
   опции; интеграционные тесты (§8.2).
4. **E2E (docker, Release)**: сценарии §8.3 — по канонам E2E
   (guid-контур, динамические порты, полный teardown, `[PHASE]`).

## 7. Ограничения и границы

- Панель UI (чтение ключей, алерты, матрица/пары в UI) — t34.
- Клиентская интеграция пула (`endpoints`, приоритеты чтения) — t36.
- Полнота меша/зеркальность состава, сверка соответствия targets
  декларации `links` — зона G (журнал mesh-governance), не зонда.
- Зонд не считает RPO и не прогнозирует догон (LAGGING/DIVERGED —
  состояние на момент прогона).
- Инкрементальная сверка (маркеры последней синхронной позиции per-pair,
  сверка «только хвостов») — вне: полный проход от пустого маркера
  достаточен домашнему контуру (бюджет `SyncMaxPages`); рост объёмов —
  повод roadmap-задачи.
- Ротация ключей/CA — вне (roadmap); зонд зависит от root-кредов
  профиля (админ-грань) — их смена меняет только env профиля.
- Код не трогается до одобрения plan.md (правило arch-first соблюдено:
  контракт обновлён, §4).

## 8. Критерии приёмки

### 8.1. Юнит-тесты (фейк `IOwnS3NodeClient`, фейк etcd-гейт где нужен)

1. **Матрица connectivity**: probe двух нод с зеркальными targets →
   полная матрица OK; reachable=false у одного → DEGRADED; все
   offline → DOWN; targets пусты у всех → OK с пустой матрицей; нода с
   отказом probe-вызова → её строки нет, диагностика в probe_error;
   0 ответивших при RUNNING>0 → DOWN + probe_error.
2. **Пары sync**: зеркальные targets → одна пара {min,max}; незеркальный
   target (a→b есть, b→a нет) → пара одна; targets нет → pairs пуст,
   SYNCED.
3. **Сверка**: равные страницы sync-report до конца (nextMarker пуст) →
   SYNCED 0/0; расхождение на 2-й странице → diff листингов: missing/
   extra/lag_bytes по элементам (key, modTime); одинаковый ключ с
   разными modTime — и в missing, и в extra.
4. **Вердикты**: synced (0/0); lagging по fetch-очереди >0; lagging по
   сокращению missing+extra против прошлого значения ключа; diverged —
   расхождение при пустой очереди и отсутствии сокращения; прошлого
   значения нет (первый прогон) и очередь пуста → lagging (не diverged:
   нет свидетельства устойчивости).
5. **Бюджет страниц**: листинг стороны > SyncMaxPages → пара не в
   pairs, last_error, агрегат не лучше LAGGING.
6. **Гейт/заявка**: свежий checked_unix < SyncIntervalSec и заявки нет
   → тик без прогона (нет put); заявка есть → прогон + del после
   записи; Failed прогона → заявка жива.
7. **Канонические JSON**: значения ключей — порядок полей и формат
   канона arch/23 §3 (примеры — ассерты строгой строки).
8. **Парсер**: connectivity/sync в /s3/ — не unknownKeys; снапшот без
   них.
9. **Хендлер API** (интеграционный уровень при необходимости): 404
   несуществующего кластера; 409 не-Active; 202 + put-if-absent; повтор
   при живой заявке → 202 без перезаписи.

### 8.2. Интеграционные тесты (etcd-контур, без docker)

1. Заявка через API (etcd-фикстура) → тик процессов (фейк-клиент нод)
   → оба ключа записаны канонически, заявка снесена.
2. Периодический гейт: первый тик без ключей → прогон; второй
   немедленно → без прогона; время прогона смещено (фейк-часы) →
   прогон.
3. Ошибка записи ключа (фейк etcd) → Failed, заявка жива, ретрай тика
   успешен.

### 8.3. E2E (docker, Release; каноны изоляции/телеметрии AGENTS.md)

1. **Штатная синхронность**: кластер 2 инстанса + links(пара) → объекты
   в bucket; после периода (или заявки `POST sync-probe` → 202) ключи:
   connectivity OK с полной матрицей, sync SYNCED 0/0.
2. **По команде**: `POST /api/s3/clusters/{c}/sync-probe` → 202 → ключи
   обновились (checked_unix вырос), заявка снесена; повторный вызов при
   живой заявке → 202 идемпотентно.
3. **Недоступная нода**: stop контейнера одной ноды → (после
   UNREACHABLE надзором) connectivity без её строки, sync без её пар;
   до UNREACHABLE — её линки offline у соседа (DEGRADED/DOWN по факту).
4. **Отставание**: запись объекта в инстанс A при остановленном B;
   возврат B → sync LAGGING (missing>0, lag_objects>0 — fetch-очередь)
   → после доставки (бюджет ≤5 мин, R4) → SYNCED. Прогон по команде на
   каждом шаге.
5. **Deny-инстанс**: deny ноды → после доводки G её связи снесены →
   зонд: connectivity OK с пустой матрицей (линков нет), sync SYNCED
   (пар нет) — отсутствие репликации не дефект зонда.
6. Полный teardown + ассерт чистоты контура сценария (guid-имена,
   динамические порты, артефакты `/tmp/pgw-e2e-artifacts-<guid>/`).

### 8.4. Регрессии

- Существующие сценарии S3Worker (provision/deprovision/supervise/
  add/remove/ротация/mesh) зелёные; G не затронут (NeighborNames
  сохранён).
- unknownKeys-счётчик тика перестаёт расти от connectivity/sync.
- `TreatWarningsAsErrors`, `dotnet build` без предупреждений.
