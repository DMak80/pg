# t34-s3-panel — панель UI: домен S3 (управление кластерами + наблюдение)

Spec (Фаза 1 dev-flow). Worktree: `feat-t34-s3-panel`.

## 1. Цель

Дать панели AdminPanel пятый домен — **S3-кластеры хранилища бэкапов**:

- **Управление**: создание кластера (с инстансами: имена, гео-placement,
  ресурсы/диски), изменение состава нод (добавление/вывод), изменение
  ресурсов, редактирование графа репликации `links`, отзыв/восстановление
  ключей инстансов (`deny`/`allow`), заявка ротации секретов профиля,
  внеплановый зонд синхронности/связности, удаление кластера. Все мутации —
  **прокси в HTTP API S3Worker** (`arch/24-s3worker.md` §1.1 — API полностью
  реализовано: 10 эндпоинтов + seed); панель в etcd домена S3 не пишет
  ничего («немой читатель», `arch/23-s3-clusters.md` §9).
- **Наблюдение**: состав кластеров (config/ноды/размещение/ресурсы),
  связность (`connectivity`), синхронность/лаг (`sync`) — из etcd-ключей
  зонда t33; занятость дисков нод (`usage`) — из etcd-ключей надзора
  воркера; `endpoints` — точка дискавери клиентов.
- **Инфра-грань**: панель становится владельцем серверного серта API
  s3worker — ключ `/workers/api_tls/s3worker` (канон `arch/24` §1.1:
  «пишет ТОЛЬКО панель — t34») и карточкой S3Worker на грани «Воркеры»
  (инстансы/health/серт/перезапуск).
- **Интеграция с гранью «Хранилище бэкапов»**: грань
  `arch/adminpanel/02-etcd-contract.md` §2.5 (read-only live-инвентарь
  бакета по env-креду) **не меняется** — без правок и без кросс-ссылок
  (решение пользователя). «Интеграция» = сосуществование двух граней:
  старая читает объектное хранилище напрямую как раньше, новая работает
  только с etcd и API оркестратора.

Проекция домена — глава S3 в `arch/adminpanel/02-etcd-contract.md` по
образцу §10 kafka / §11 valkey (arch-first: сначала главы, затем код).

## 2. Принципы

1. **arch-first**: сперва правки `arch/adminpanel/02-etcd-contract.md` и
   `arch/adminpanel/03-panels.md` (раздел 3 ниже), затем код. Утверждения о
   механиках — только по канонам `arch/23`/`arch/24`.
2. **Немая панель**: панель никогда не пишет в `/s3/` и `/s3worker/`; все
   мутации — зеркальные эндпоинты-прокси в API S3Worker с маппингом кодов
   1:1. Единственная etcd-запись панели в этой задаче —
   `/workers/api_tls/s3worker` (инфра-ключ §9.9, вне домена S3).
3. **Отдельный домен-снапшот** `S3Snapshot` (не `EtcdSnapshot`/`KafkaSnapshot`/
   `ValkeySnapshot`): свой refresher (тик 3 с, `AdminPanel:S3:
   RefreshIntervalSeconds`), общий транспорт `Shared.Etcd` и настройки
   endpoints — механика один в один как у valkey-домена.
4. **Панель не разговаривает с нодами S3 напрямую**: живость, связность,
   синхронность, занятость — только из etcd (зонд/надзор пишет воркер).
   Новых live-проб домена нет (решение пользователя: «должно с etcd, а
   s3worker уже работает напрямую с нодами»).
5. **PKI-материал скрыт**: `pki/ca`, `pki/orchestrator`, `nodes/<I>/pki`
   парсер пропускает молча (без `unknownKeys`-счётчика; прецедент —
   pg-ключи `app_user`/`app_password`); в снапшот/DTO/UI не попадают
   никогда (`arch/23` §6: «панель управление ключами не показывает —
   полностью скрыто бэком»).
6. **Толерантность читателя**: неизвестные ключи внутри `/s3/` — лог +
   счётчик `unknownKeys`; битый JSON — parseError-запись без падения тика;
   семантика `state` «отсутствие = Active»; форматы толерантны к расширению
   полей (парсер читает известные поля, новые не ломают чтение).
7. **Читаемые из `/s3worker/`** — только `api/` и `rotations/`
   (`arch/23` §8); остальные ключи префикса панель не читает и не пишет.
8. **Секреты**: панель не знает кредов S3-нод (per-install env-профили
   воркера, `arch/23` §6); в UI — только имя профиля из
   `config.secret_profile`. 422-ответ воркера (перечень доступных имён,
   без значений) показывается оператору в форме создания. Read-only
   access-ключи профилей панелью не используются (проб нод нет).
9. **UI**: русские тексты, английские идентификаторы; Mantine-модалы по
   образцу kafka/valkey; деструктив — confirm-модалы (снос кластера/ноды —
   с явным предупреждением об уничтожении данных бэкапов).

## 3. Arch-правки (Фаза 0)

### 3.1. `arch/adminpanel/02-etcd-contract.md`

1. **Преамбула**: дописать домен S3 в перечень (чтение `/s3/clusters/` +
   `/s3worker/{api,rotations}/`, 10 мутаций через API S3Worker — §12).
2. **Новая §2.3.4 «`/s3worker/api/…` — дискавери API S3Worker»** —
   симметрично §2.3.3 (valkey): lease-ключи `/s3worker/api/<id>` (ставит
   сам воркер; формат `{"url","instance","since_unix","cert_thumbprint"?}`)
   читаются s3-refresher'ом в `S3Snapshot.WorkerEndpoints` — источник URL
   для s3-мутаций §12.2. API — mTLS той же пакеты per-install API-CA
   (`AdminPanel:Workers:WorkerTls`, env `WORKERS_PANEL_TLS_*`); серверный
   серт — ключ `/workers/api_tls/s3worker` (§9.9). По тем же URL тик опроса
   `/healthz` (тем же поллером и интервалом, что у pg/kafka/valkey) пробит
   живые инстансы → `WorkerHealth[]` в `S3Snapshot.WorkerHealth`,
   warning-алерт `worker-unhealthy`. Отсутствие живых ключей → 503 мутаций
   + critical-алерт `worker-api-unreachable` (target `s3worker`).
   Остальные ключи `/s3worker/` панель не читает и не пишет (кроме
   `rotations/` — §12.1).
3. **§9.9**: расширить перечень воркеров — добавить ключ
   `/workers/api_tls/s3worker` (панель — единственный писатель; воркер
   читает при старте; env-фоллбек `S3W_API_TLS_{CERT,KEY,CLIENT_CA}`).
   Мутации generate/upload/delete/restart и валидация «серт не затрагивает
   исходящие коммуникации» — общие правила §9.9, worker-идентификатор
   `s3worker`.
4. **Новая глава §12 «S3 (чтение + мутации)»** по образцу §11 valkey
   (канон ключей — `arch/23`, дословно источник истины; здесь — панельная
   проекция):
   - **§12.1 Читаемые ключи** (таблица):

     | Префикс/ключ | В модель | Примечание |
     |---|---|---|
     | `/s3/clusters/<C>/config` | `S3ClusterInfo` (config + state-маппинг: отсутствие = Active) | `platform` (`owns3`), `nodes`, `secret_profile`, `created_unix` |
     | `/s3/clusters/<C>/nodes/<I>/{placement,resources,state,placed_on,usage}` | `S3NodeInfo` | `state` — raw-строка (толерантно к новым); `usage` — занятость диска |
     | `/s3/clusters/<C>/links` | список пар `S3ReplicationLink` | граф репликации |
     | `/s3/clusters/<C>/denylist` | список имён | отозванные инстансы |
     | `/s3/clusters/<C>/endpoints` | `S3ClusterInfo.Endpoints` | точка дискавери клиентов; отсутствие у Active с нодами — critical-алерт |
     | `/s3/clusters/<C>/connectivity` | `S3ConnectivityInfo` (state, checked_unix, links-матрица, probe_error?) | зонд t33 |
     | `/s3/clusters/<C>/sync` | `S3SyncInfo` (state, checked_unix, pairs, last_error?) | зонд t33 |
     | `/s3worker/rotations/<C>` | `S3RotationTicket` | живая заявка ротации секретов |
     | `pki/*`, `nodes/<I>/pki` | — (парсер пропускает молча, без unknownKeys) | PKI репликации — только воркеру |

     Неизвестные ключи внутри `/s3/` — лог + `unknownKeys`; битый JSON —
     parseError-запись + warning-алерт `s3-key-malformed`.
   - **§12.2 Мутации панели (10; исполняет S3Worker API — arch/24 §1.1)**.
     Общие правила как §11.2: панель отправляет команду в API воркера, URL —
     живой `/s3worker/api/<id>` (§2.3.4); воркер валидирует и пишет etcd;
     панель маппит коды 1:1 (503 — в т.ч. когда живых ключей нет).

     | # | Мутация | Эндпоинт панели → эндпоинт воркера | Отказы |
     |---|---|---|---|
     | 1 | Создание кластера | `POST /api/s3/clusters` → `POST /api/s3/clusters` | 400, 409, 422 (профиль секретов), 503 |
     | 2 | Удаление кластера (TO_REMOVE) | `DELETE /api/s3/clusters/{c}` → тот же | 404, 503 |
     | 3 | Добавление инстанса | `POST /api/s3/clusters/{c}/nodes` → тот же | 400, 404, 409, 503 |
     | 4 | Маркер демонтажа инстанса | `DELETE /api/s3/clusters/{c}/nodes/{n}` → тот же | 404, 409 (последняя нода), 503 |
     | 5 | Ресурсы инстанса | `PUT /api/s3/clusters/{c}/nodes/{n}/resources` → тот же | 400, 404, 503 |
     | 6 | Граф репликации (замена целиком) | `PUT /api/s3/clusters/{c}/links` → тот же | 400 (links), 404, 503 |
     | 7 | Отзыв ключа инстанса (deny) | `POST /api/s3/clusters/{c}/nodes/{n}/deny` → тот же | 404, 503 |
     | 8 | Снятие отзыва (allow) | `POST /api/s3/clusters/{c}/nodes/{n}/allow` → тот же | 404, 503 |
     | 9 | Заявка ротации секретов профиля | `POST /api/s3/clusters/{c}/rotations` → тот же | 404, 503 |
     | 10 | Внеплановый зонд | `POST /api/s3/clusters/{c}/sync-probe` → тот же | 404, 409 (не Active), 503 |

     Протоколы записи — канон исполнителя `arch/24` §1.1 (клэйм-txn,
     атомарность, идемпотентность already→204/202); панель — прокси, тело
     команды передаёт как есть. `requested_by` — username сессии панели
     (заголовок `X-Requested-By`; мутации 9–10 — с аудитом). Отмена заявок
     ротации из панели НЕТ (как valkey §11.2 — снятие только воркером).
   - **§12.3 Валидация** — ссылкой на `arch/24` §1.1 (сервер — источник
     истины, фронт дублирует для UX): имя кластера/инстанса
     `^[a-z][a-z0-9_]{0,62}$`; имя `orchestrator` зарезервировано;
     инстансы 0..9; placement — обязательный валидный селектор; resources —
     cpu 0.01..64, mem/disk 1..65536 GiB; `secret_profile` — непустое имя
     (422 с перечнем доступных); links — имена существуют, `a≠b`, пары
     уникальны.
5. **§2.5** — без правок (грань «Хранилище бэкапов» остаётся read-only
   live-инвентарём одного per-install endpoint; решений этой задачи она не
   касается).

### 3.2. `arch/adminpanel/03-panels.md`

1. **Новая глава §12 «S3: панель и REST API (`/api/s3/*`)»** по образцу
   §8 (valkey). Источник данных — отдельный снапшот `S3Snapshot` (свой
   refresher, тик 3 с); live-проб нод нет (живость — `nodes/*/state`,
   связность/синхронность — ключи зонда).
   - **§12.1 Список эндпоинтов** (REST панели):

     | Метод+путь | Назначение |
     |---|---|
     | `GET /api/s3/clusters` | сводный список S3-кластеров |
     | `POST /api/s3/clusters` | создание кластера (02 §12.2-1): тело `CreateS3ClusterPanelRequest` → 201 \| 400 \| 409 \| 422 \| 503 |
     | `GET /api/s3/clusters/{cluster}` | детали: config, ноды, links, denylist, endpoints, connectivity, sync, ротация |
     | `DELETE /api/s3/clusters/{cluster}` | перевод в TO_REMOVE (демонтаж асинхронный — воркер): 202 \| 204 \| 404 \| 503 |
     | `POST /api/s3/clusters/{cluster}/nodes` | добавление инстанса: тело `AddS3NodePanelRequest` → 201 \| 400 \| 404 \| 409 \| 503 |
     | `DELETE /api/s3/clusters/{cluster}/nodes/{node}` | маркер демонтажа (one-way): 202 \| 204 \| 404 \| 409 \| 503 |
     | `PUT /api/s3/clusters/{cluster}/nodes/{node}/resources` | ресурсы: 200 \| 400 \| 404 \| 503 |
     | `PUT /api/s3/clusters/{cluster}/links` | граф репликации целиком: 202 \| 400 \| 404 \| 503 |
     | `POST /api/s3/clusters/{cluster}/nodes/{node}/deny` | отзыв ключа: 202 \| 204 \| 404 \| 503 |
     | `POST /api/s3/clusters/{cluster}/nodes/{node}/allow` | снятие отзыва: 202 \| 204 \| 404 \| 503 |
     | `POST /api/s3/clusters/{cluster}/rotations` | заявка ротации секретов: 201 \| 200 \| 404 \| 503 |
     | `POST /api/s3/clusters/{cluster}/sync-probe` | внеплановый зонд: 202 \| 404 \| 409 \| 503 |

     `GET /api/alerts` объединяет алерты всех движков (kind `s3-*`);
     `GET /api/overview` получает s3-сводку (clustersTotal, clustersCritical
     — critical-алерты `s3-node-not-running`/`s3-endpoints-missing`/
     `s3-connectivity-down`/`s3-sync-diverged`).
   - **§12.2 DTO (ключевые поля)**:

     ```text
     S3ClusterSummaryDto: name, state(ACTIVE|NOT_INITIALIZED|TO_REMOVE),
         nodesTotal, nodesRunning, endpoints, connectivityState
         (OK|DEGRADED|DOWN|null), syncState(SYNCED|LAGGING|DIVERGED|null),
         rotationPending(bool)
     S3ClusterDto: name, state, platform, nodesDeclared, secretProfile,
         createdUnix, endpoints, nodesList[S3NodeDto], links[{a,b}],
         denylist[string], connectivity{state, checkedUnix,
         links<map<string,map<string,string>>>, probeError?},
         sync{state, checkedUnix, pairs[{from,to,missing,extra,lagBytes?,
         lagObjects?}], lastError?}, rotation{requestedUnix, requestedBy}?
     S3NodeDto: name, state(raw: NOT_INITIALIZED|PROVISIONING|RUNNING|
         UNREACHABLE|REMOVING|TO_REMOVE), placedOn, placement{dockerHost?,
         swarmNode?, swarmLabels?}, resources{cpu, memGi, diskGi}(nullable),
         usage{usedBytes, totalBytes, updatedUnix}(nullable), denied(bool)
     CreateS3ClusterPanelRequest: name, secretProfile, instances[]
       {name, placement{dockerHost?|swarmNode?|swarmLabels?},
        resources?{cpu?, memGi?, diskGi?}}, resources?{cpu?, memGi?,
        diskGi?} (общий fallback — зеркалит DTO воркера)
     AddS3NodePanelRequest: name, placement, resources
     UpdateS3ResourcesPanelRequest: cpu?, memGi?, diskGi? (null = не менять)
     PutS3LinksPanelRequest: links[{a,b}]
     ```

     Секреты (значения профилей) в DTO/API/UI не отдаются никогда.
   - **§12.3 Панели UI** (таблица, ниже в разделе 4.4 этой спеки — в канон
     переносится тем же содержанием).
   - **§12.4 Каталог s3-алертов** (`S3AlertEngine`) — чистая функция
     `S3Snapshot (prev, next) → Alert[]`; пороги `AdminPanel:S3Alerts`;
     sinceUnix по стабильному `id = kind:target` (механика §2); Hint/Remedy
     обязательны (§4.1):

     | kind | severity | Условие |
     |---|---|---|
     | `s3-cluster-not-initialized` | info | state=NOT_INITIALIZED |
     | `s3-cluster-to-remove` | info | state=TO_REMOVE |
     | `s3-node-not-running` | critical | Active-кластер, нода state ∉ {RUNNING}, кроме fresh-PROVISIONING (< 60 c) |
     | `s3-endpoints-missing` | critical | Active-кластер с нодами (nodes>0) без `endpoints` |
     | `s3-connectivity-degraded` | warning | connectivity.state=DEGRADED |
     | `s3-connectivity-down` | critical | connectivity.state=DOWN |
     | `s3-sync-lagging` | warning | sync.state=LAGGING |
     | `s3-sync-diverged` | critical | sync.state=DIVERGED |
     | `s3-sync-stale` | warning | у Active-кластера с RUNNING-нодами connectivity/sync отсутствуют или `checked_unix` старше `SyncStaleSec` (def 300) — зонд молчит |
     | `s3-node-disk-usage-high` | warning | `usage.used/usage.total > DiskUsagePercent` (def 85) или `used > resources.disk`-декларации |
     | `s3-nodes-same-host` | warning | ≥2 нод кластера с одинаковым `placed_on` (гео-раснос — обязанность оператора, arch/23 §4) |
     | `s3-node-not-in-mesh` | warning | нода Active-кластера без единой связи в `links` (частичный граф — риск безрепликационной доли) |
     | `s3-rotation-pending` | info | живая заявка `/s3worker/rotations/<C>` |
     | `s3-rotation-stale` | warning | заявка ротации жива дольше `RotationStaleSeconds` (def 1800) |
     | `s3-key-malformed` | warning | s3-ключ не разобран (parseError) |
     | `worker-api-unreachable` | critical | нет живых ключей `/s3worker/api/` — s3-мутации панели 503; target `s3worker` |
     | `worker-unhealthy` | warning | живой ключ, но `/healthz` ≠ 200; target `s3worker/<id>` |
2. **§3.7 «Воркеры»**: добавить карточку S3Worker (инстансы/health/целевой
   серт/статусы applied/pending restart/unmanaged/кнопки generate/upload/
   delete/restart) — общие правила §9.9.
3. **Точечные правки списков**: §1 (перечень мутирующих эндпоинтов +
   «модуль S3»), §3 (таблица панелей — строка S3; счётчик форм ввода),
   шапка документа.

### 3.3. `arch/23-s3-clusters.md` §9 / `arch/24-s3worker.md` §1.1

Сверка формулировок с новыми главами adminpanel/02 (проекция указывает на
t34 уже сейчас). Правки — только при расхождении (ожидаются точечные:
ссылка на главу adminpanel/02 §12 вместо общего описания). Пересказ канонов
в adminpanel/02 не допускается — форматы ключей ссылкой на arch/23.

## 4. Структура и компоненты (код)

### 4.1. `src/AdminPanel.Core/S3/`

- **`S3Snapshot.cs`** — immutable-записи (по образцу `ValkeySnapshot`):

  ```csharp
  S3Snapshot(BuiltAtUtc, EtcdReachable, ConsecutiveFailures,
      IReadOnlyList<S3ClusterInfo> Clusters,          // /s3/clusters/
      IReadOnlyList<S3RotationTicket> Rotations,      // /s3worker/rotations/
      IReadOnlyList<WorkerEndpoint> WorkerEndpoints,  // /s3worker/api/
      IReadOnlyList<WorkerHealth> WorkerHealth,       // опрос /healthz
      IReadOnlyList<Alert> Alerts,                    // S3AlertEngine
      IReadOnlyList<KeyParseError> ParseErrors,
      int UnknownKeyCount,
      WorkerApiCert? WorkerApiCert)                   // /workers/api_tls/s3worker

  S3ClusterInfo(Name, State /*Active|NotInitialized|ToRemove*/, Platform,
      NodesDeclared, SecretProfile, CreatedUnix, Endpoints,
      IReadOnlyList<S3NodeInfo> NodesList,
      IReadOnlyList<S3ReplicationLink> Links, IReadOnlyList<string> DenyList,
      S3ConnectivityInfo? Connectivity, S3SyncInfo? Sync,
      S3RotationTicket? Rotation)

  S3NodeInfo(Name, State /*raw-строка*/, PlacedOn, S3NodePlacement? Placement,
      S3NodeResources? Resources /*cpu decimal?, memGi/diskGi int? — парсинг "<n>Gi"*/,
      S3NodeUsage? Usage /*usedBytes,totalBytes,updatedUnix*/)

  S3ConnectivityInfo(State /*OK|DEGRADED|DOWN*/, CheckedUnix,
      IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Links,
      string? ProbeError)
  S3SyncInfo(State /*SYNCED|LAGGING|DIVERGED*/, CheckedUnix,
      IReadOnlyList<S3SyncPair>, string? LastError)
  S3RotationTicket(Cluster, RequestedUnix, RequestedBy)
  ```

  State-маппинг `S3ClusterStates.Parse` — толерантный (незнакомое → Active).
- **`S3Alerting/S3AlertEngine.cs`** (+ интерфейс) — правила каталога
  §12.4; каждое правило — Hint/Remedy (движитель: `WorkerAuto` — надзор/
  доводка/зонд воркера; `OperatorApi` — мутация через API панели;
  `OperatorRunbook`).

### 4.2. `src/AdminPanel.Etcd/`

- **`Parsing/S3Parser.cs`** — чистые функции парсинга префиксов `/s3/` и
  `/s3worker/rotations/` (толерантность §2 принципов; pki-ключи — молча).
- **`S3PanelOptions.cs`** — `AdminPanel:S3 { RefreshIntervalSeconds = 3 }`.
- **`S3SnapshotRefresher.cs`** + **`S3SnapshotStore.cs`** (+ интерфейсы
  `IS3SnapshotStore`/`IS3SnapshotReader`) — один в один по механике
  `ValkeySnapshotRefresher`: тик; range `/s3/clusters/` +
  `/s3worker/rotations/` + `/s3worker/api/` + точечный
  `/workers/api_tls/s3worker`; sticky-failover активного endpoint;
  транспортный провал любого чтения роняет тик (прежние данные,
  `EtcdReachable=false`, счётчик отказов); парсеры → сборка снапшота →
  `alertEngine.Evaluate(built, previous)` → атомарная замена в сторе.
- **`ModuleExtensions.AddS3()`** — регистрация стора/refresher'а
  (`Program.cs`: `.AddS3()`); **`TrustedWorkerThumbprints`** — добавить
  thumbprint из `IS3SnapshotStore` (доверие сертам, которые панель сама
  записала).
- **`Workers/WorkerApiGateway.cs`** — `ResolveEndpoints`: case
  `"s3worker" => s3Store.Current?.WorkerEndpoints` (+ конструктор-параметр
  `IS3SnapshotStore s3Store`).
- **`Workers/WorkerHealthPoller.cs`** — s3-блок (тот же тик/клиент/сторы:
  endpoints из `IS3SnapshotReader` → `ProbeAsync` → `IS3WorkerHealthStore`
  — новый стор по образцу valkey).

### 4.3. `src/AdminPanel.Api/`

- **`Inspection/S3Query.cs`** + регистрация `MapS3InspectionApi()`:
  `GET /api/s3/clusters`, `GET /api/s3/clusters/{cluster}` (404 — кластера
  нет); алерты/overview — расширение существующих `AlertsQuery`/
  `OverviewQuery` s3-сводкой.
- **`Operations/S3/`**: `S3Requests.cs` (тела панельных запросов — зеркало
  DTO воркера `S3Api.cs`: `CreateS3ClusterPanelRequest`, `AddS3Node…`,
  `UpdateS3Resources…`, `PutS3Links…`), `S3Commands.cs` (10 команд +
  хендлеры-прокси через `IWorkerApiGateway.SendAsync("s3worker", …)` — по
  образцу `ValkeyCommands`; `requestedBy` — на ротации/зонд), `S3OperationsModule.cs`
  (`MapS3OperationsApi()` — зеркальные эндпоинты §12.1 03-panels; успех —
  DTO/статус воркера как есть, ошибки — ProblemDetails как есть, нет API —
  503).
- **Грань «Воркеры»**: существующие `/api/workers/{worker}/api-cert/*` и
  `/api/workers/{worker}/restart` принимают `worker=s3worker` (валидатор
  имени + резолв серта/инстансов из `IS3SnapshotStore`); `GET /api/workers`
  отдаёт карточку s3worker.

### 4.4. Фронтенд (`frontend/src/`)

- **`pages/S3ClustersPage.tsx`** — список: имя, state-бейдж, ноды
  running/всего, endpoints (сокращённо), бейджи связности (OK/DEGRADED/
  DOWN/—) и синхронности (SYNCED/LAGGING/DIVERGED/—), бейдж ротации;
  кнопка «Создать кластер».
- **`pages/s3-cluster/S3ClusterDetailsPage.tsx`** — шапка: state-бейджи,
  endpoints, кнопки: «Проверить сейчас» (sync-probe; 409 — текстом),
  «Ротация секретов» (модал-предупреждение: rolling-пересоздание
  инстансов, PKI переиспользуется), «Удалить кластер» (красная; confirm:
  снос уничтожает данные бэкапов нод кластера — осознанное действие;
  при TO_REMOVE скрыты). Вкладки:
  - **Ноды**: таблица name/state/placed_on/placement (селектор)/resources/
    usage (used/total + процент от декларации disk); бейдж «отозван»
    (denylist) + кнопки Deny/Allow (deny — confirm: связи с инстансом
    сносятся, будущие join запрещены); колонка действий: «Добавить ноду»
    (в заголовке вкладки), «Изменить ресурсы», красная «Убрать ноду»
    (confirm: volume инстанса сносится — копия на остальных при
    настроенных связях; последняя нода — серверный 409 текстом).
  - **Репликация**: текущий граф — **таблица пар** + **матрица смежности
    нода×нода** (просмотр: отметка настроенных связей, deny-ноды
    помечены; выбранное решение пользователя); редактор — модал
    «таблица пар» (строки a/b — селекты имён нод, добавление/удаление
    строк) → `PUT links` целиком (202 — применит доводка воркера);
    подсказка «полный меш не обязателен; ноды одного кластера — на
    разных хостах/площадках»; анти-аффинити-индикация — совпадения
    `placed_on` подсвечиваются.
  - **Связность и синхронность**: connectivity-матрица (ok/offline по
    настроенным линкам, state OK/DEGRADED/DOWN, `checked_unix`-штамп,
    probe_error) и sync-таблица пар (from/to, missing/extra, lag_bytes/
    lag_objects, state SYNCED/LAGGING/DIVERGED, last_error). Пустые
    матрицы — «линков не настроено».
- **Модалы** (`pages/s3-cluster/`): `CreateS3ClusterModal` (имя;
  secret_profile — текст; платформа — read-only «owns3»; динамический
  список инстансов: имя + placement (docker_host / swarm_node /
  swarm_labels key-value-редактор) + ресурсы cpu/mem/disk; допустимо
  0 инстансов — подпись «декларация без развёртывания»; клиентская
  валидация — зеркало §12.3), `AddNodeModal`, `EditNodeResourcesModal`,
  `LinksEditorModal`, `RemoveNodeButton`, `DenyNodeButton`,
  `AllowNodeButton`, `RotateSecretsButton`, `SyncProbeButton`,
  `DeleteS3ClusterButton`.
- **`App.tsx`**: маршруты `/s3`, `/s3/:cluster`; **`AppLayout`**: пункт
  «S3»; **`OverviewPage`**: карточка S3 (кластеры, critical-алерты
  домена); **`WorkersPage`**: карточка S3Worker (по §3.7).
- **`api/dto.ts`/`queries.ts`**: s3-типы и запросы (invalidation по образцу
  kafka/valkey).

### 4.5. Тесты

- **`src/tests/AdminPanel.UnitTests`**:
  - парсер: канонические фикстуры — примеры `arch/23` §2.1 (config
    заявки/Active/0 инстансов/TO_REMOVE, placement plain/swarm/комбинация,
    resources, state, placed_on, usage, links, denylist, endpoints, pki —
    пропуск молча) и §3 (connectivity OK-матрица, sync SYNCED-пары) +
    вырожденные (отсутствующие поля, битый JSON → parseError, неизвестный
    ключ → unknownKeys);
  - `S3AlertEngine`: таблицы правил (условие → kind/severity; sinceUnix);
  - refresher: `RefreshOnceAsync` на моках (успех/провал KV — прежние
    данные).
- **`src/tests/AdminPanel.IntegrationTests`**: refresher end-to-end на
  testcontainers-etcd (сеем ключи снапшота → снапшот); API-прокси против
  мока API s3worker (коды 1:1; 503 без живых ключей `/s3worker/api/`).
- **dev-stand e2e-чек**: новый `dev-stand/adminpanel/checks/80-s3-panel.sh`
  (следующий свободный номер после `75-s3worker.sh`): против живого стенда — создание кластера
  через REST панели → Active → PUT links → sync-probe 202 → connectivity/
  sync в `GET /api/s3/clusters/{c}` → снос через панель → чистота etcd/
  docker; RunTag-имена, own-only зачистка, ассерт чистоты (канон
  `docs/e2e-isolation.md`).

## 5. Фазы

- **Ф0 — arch**: правки раздела 3 (adminpanel/02 §2.3.4/§9.9/§12,
  adminpanel/03 §12/§3.7/списки, сверка arch/23 §9 / arch/24 §1.1).
- **Ф1 — Core+Etcd**: `S3Snapshot`/`S3Parser`/`S3AlertEngine`/
  `S3SnapshotRefresher`+`Store`/`AddS3()`/шлюз+health-поллер+thumbprint'ы.
- **Ф2 — API**: `S3Query`/`S3OperationsModule` (10 прокси)/грань «Воркеры»
  (worker=s3worker).
- **Ф3 — фронтенд**: страницы/вкладки/модалы/навигация/overview/воркеры.
- **Ф4 — тесты и стенд**: юниты/интеграции/чек `80-s3-panel.sh`; прогон
  серий с зачисткой между ними.
- **Ф5 — финал**: roadmap-чистка мерж-гейтом (тег `t34-s3-panel` удаляется
  из `arch/roadmap/s3.md` тем же коммитом), практики в `docs/adminpanel/`
  при наличии пережитых граблей.

## 6. Ограничения

- **Не трогаем**: грань §2.5; код S3Worker (API готов; обнаруженный дефект
  API — отдельное обсуждение, не молчаливая правка); pki-ключи;
  координационные ключи `/s3worker/{leader,claims,work,portalloc,locks,
  instances,probes}`.
- Панель не пишет в `/s3/` и `/s3worker/` никогда (исключений нет;
  `/workers/api_tls/s3worker` — инфра-ключ §9.9, не домен S3).
- Вне задачи: клиентская интеграция пула и приоритеты чтения (t36);
  механика mesh-репликации ownS3 (`arch/owns3/07`); ротация PKI/CA
  (roadmap); enterprise-фичи.
- Тесты: комментарии по AAA; docker-порты только динамические;
  таймауты короткие (по канонам AGENTS.md); зачистка серий между
  прогонами.
- Секреты (значения профилей, PKI, PEM) — никогда в снапшоте/DTO/UI/логах.
- Язык: документация/UI русские, идентификаторы английские;
  `TreatWarningsAsErrors` — сборка без warning'ов.

## 7. Критерии приёмки

1. **Канон обновлён**: adminpanel/02 содержит §2.3.4/§9.9 (+s3worker)/§12;
   adminpanel/03 — §12 (эндпоинты/DTO/UI/алерты)/§3.7 (+карточка S3Worker);
   расхождений формулировок с arch/23/24 нет (пересказа нет — ссылки).
2. **Парсеры**: канонические примеры `arch/23` §2.1/§3 — эталонные
   фикстуры юнит-тестов (все парсятся корректно; битые → parseError;
   pki-ключи и `/s3worker/`-координация не читаются).
3. **Мутации**: 10 зеркальных эндпоинтов панели; коды 1:1 с `ApiModule`
   S3Worker (201/202/204/400/404/409/422/503); при отсутствии живых
   `/s3worker/api/` — 503 + critical-алерт `worker-api-unreachable`.
4. **Алерты**: каталог §12.4 реализован целиком; у каждого kind —
   Hint/Remedy; sinceUnix стабилен по `id`.
5. **UI**: список/детали/вкладки/модалы работают против стенда; редактор
   links — «таблица пар + матрица-визуализация»; deny-бейджи; confirm'ы
   деструктива несут предупреждение о потере данных; usage отображается с
   процентом от декларации.
6. **Грань «Воркеры»**: карточка S3Worker (инстансы/health/целевой серт/
   applied|pending restart|unmanaged/кнопки); thumbprint s3worker входит
   в доверенные панели.
7. **Overview**: карточка S3 со сводкой (кластеры/critical).
8. **Грань §2.5 не изменилась** (код/канон не тронуты).
9. **Тесты**: юниты+интеграции зелёные; e2e-чек `80-s3-panel.sh` стенда
   зелёный с полной самоочисткой; сборка решения и фронтенда без ошибок.
10. **Roadmap**: тег `t34-s3-panel` удалён из `arch/roadmap/s3.md`
    мерж-коммитом (вместе с `←`-зависимостью t36, если стоит).

## 8. Открытые вопросы

Нет — принципиальные решения зафиксированы: §2.5 не трогаем (ответ
пользователя); links-UI «таблица + визуализация» (ответ пользователя);
живость/наблюдение — только из etcd, без live-проб нод из панели.
