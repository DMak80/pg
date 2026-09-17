# 03. Панели и REST API

Спецификация UI-панелей и HTTP-контракта. Всё read-only, кроме мутаций:
`POST /api/clusters` (создание кластера, 02 §9), `DELETE /api/clusters/{name}`
(перевод в TO_REMOVE, 02 §9.4), `POST /api/clusters/{cluster}/shards`
(добавление шарда, 02 §9.5), `DELETE /api/clusters/{cluster}/shards/{shard}`
(маркер демонтажа шарда, 02 §9.6), `POST /api/clusters/{cluster}/moves`
(заявки на переезды бакетов, 02 §9.7.1), `POST /api/clusters/{cluster}/moves/
rollback` (заявки на откат, 02 §9.7.2), `POST /api/clusters/{cluster}/moves/
finalize` (заявка уборки старого шарда, 02 §9.7.3), `POST /api/clusters/
{cluster}/moves/abort` (заявка отмены переезда, 02 §9.7.4), `DELETE /api/
clusters/{cluster}/moves/{bucket}` (отмена стоящей заявки, 02 §9.7.5),
`POST /api/clusters/{cluster}/app-password/rotate`
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
| `POST /api/clusters/{cluster}/moves` | заявки на переезды бакетов (02 §9.7.1): тело `MoveBucketsRequestDto` → 201+`MovesQueuedDto` \| 400 \| 404 \| 409 \| 503 |
| `POST /api/clusters/{cluster}/moves/rollback` | заявки на откат бакетов (02 §9.7.2): тело `RollbackBucketsRequestDto` → 201+`RollbackQueuedDto` \| 400 \| 404 \| 409 \| 503 |
| `POST /api/clusters/{cluster}/moves/finalize` | заявка уборки старого шарда (02 §9.7.3): тело `FinalizeBucketRequestDto` → 201+`BucketFinalizeQueuedDto` \| 400 \| 404 \| 409 \| 503 |
| `POST /api/clusters/{cluster}/moves/abort` | заявка отмены незавершённого переезда (02 §9.7.4): тело `AbortBucketRequestDto` → 201+`BucketAbortQueuedDto` \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/clusters/{cluster}/moves/{bucket}` | отмена стоящей заявки (02 §9.7.5): 204 \| 404 \| 503 |
| `POST /api/clusters/{cluster}/app-password/rotate` | заявка ротации app-пароля кластера (02 §9.8): без тела → 201+`AppPasswordRotatedDto` \| 404 \| 409 \| 503 |
| `GET /api/ha` | список HA-scope'ов (сводный) |
| `GET /api/ha/{scope}` | детали scope: leader, members+runtime, optime, raw config, request_* |
| `GET /api/backups/storage` | грань «Хранилище бэкапов»: `configured` (false → только это поле+причина), endpoint/bucket (без ключей), health, место (etcd-ключ `storage` + live-инвентарь), дерево кластеров/шардов, сироты (реестр воркера + сверка панели), `inventoryUpdatedUnix`/`inventoryError` (t08, 02 §2.5) |
| `GET /api/backups/storage/{cluster}/{shard}` | детали шарда: полные (id/size/objectCount/lastModified + etcd state/verify/size + статус сверки), WAL (S3-факт + etcd-статус), активный restore (бейдж), 404 — нет ни в etcd, ни в S3-дереве |
| `GET /api/backups/objects?prefix=&maxKeys=200&continuationToken=` | on-demand list-v2 (единственный выход в MinIO на запрос): `prefix` пуст или `<кластерный паттерн>/…` **и** первый сегмент — кластер снапшота или S3-дерева инвентаря (иначе 400 — защита от произвольного листинга), `maxKeys` 1..1000 (иначе 400), `nextContinuationToken` |
| `GET /api/alerts` | все алерты; query `?severity=critical|warning|info`, `?kind=` |
| `GET /api/workers` | грань «Воркеры»: по `pgworker`/`kafkaworker` — живые инстансы (instance, url, since, health, cert_thumbprint), ЦЕЛЕВОЙ серт из `/workers/api_tls/<worker>` (метаданные: subject, issuer, SAN, not_before/not_after, sha256-thumbprint, updated_unix/by; PEM не отдаётся) и статус применения per-instance: `applied` \| `pending restart` \| `unmanaged` (ключа нет — env-серт) \| `unknown` (инстанс без thumbprint) |
| `POST /api/workers/{worker}/api-cert/generate` | сгенерировать self-signed серверный лист и записать в etcd (02 §9.9): без тела → 201 `{worker, thumbprint, updatedUnix, updatedBy, restartRequired:true}` \| 422 (влияет на исходящие — не записан) \| 503 |
| `PUT /api/workers/{worker}/api-cert` | загрузить готовый серт (02 §9.9): тело `{cert_pem, key_pem}` → 201 (те же коды, что generate) |
| `DELETE /api/workers/{worker}/api-cert` | отказ от управляемого серта (откат к env после перезапуска; 02 §9.9): 204 \| 404 (ключа нет) \| 503 |
| `POST /api/workers/{worker}/restart` | перезапуск воркера (02 §9.9): прокси `POST /api/restart` на каждый живой инстанс → 202 `{results:[{instance, accepted}\|{instance, error}]}`; живых нет → 503 |

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

Заявки на переезды бакетов (протокол — 02 §9.7.1): панель ставит очередь ключей
`/pgworker/moves/<C>/bucket_<i>`; выполнение — PgWorker (старейшая заявка
кластера, одна за раз — переезды последовательны по построению контракта).
Повтор после частичного сбоя идемпотентен: уже стоящие идентичные заявки
возвращаются в `skipped` без перезаписи.

Тело `MoveBucketsRequestDto` (валидация — 02 §9.7.1; ограничения — ProblemDetails
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
записи, битый config). Сбой посередине — без компенсации (02 §9.7 п.5 общего протокола):
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

### 1.7. Контракт `POST /api/clusters/{cluster}/moves/rollback`

Заявки на откат бакетов (протокол — 02 §9.7.2): откат возвращает бакеты на
прежний шард по живой обратной подписке — направление определяет PgWorker
(SQL-факт), оператор выбирает только какие бакеты. Откат — зеркальный
cutover с секундной заморозкой записи.

Тело `RollbackBucketsRequestDto` (валидация — 02 §9.7.2):

```text
RollbackBucketsRequestDto: buckets (непустой массив уникальных int — какие
                           ACTIVE-бакеты откатывать; порядок обработки =
                           по возрастанию id)
```

Ответ 201:

```text
RollbackQueuedDto: cluster, queued[int[]] (поставлены сейчас),
                    skipped[int[]] (идентичные op=rollback уже стояли)
```

Отказы: 400 (`buckets` пуст/дубликаты); 404 (кластер не найден, имя
неканоническое); 409 (кластер не Active; нешардированная БД; бакет вне
диапазона / без routing / не ACTIVE; на бакете уже стоит иная заявка;
конкурентная заявка — txn-клэйм); 503 (etcd-ошибка чтения/записи, битый
config). Если обратной подписки нет — перманентный отказ исполняет PgWorker
после постановки («откат только полным re-copy», причина — в
`/pgworker/work/<C>`, вкладка «Переезды»).

### 1.8. Контракт `POST /api/clusters/{cluster}/moves/finalize`

Заявка уборки старого шарда после переезда (протокол — 02 §9.7.3): DROP
подписок/публикаций/слотов и схемы бакета СО ДАННЫМИ на шарде `oldShard`.
Необратимо; владелец не трогается.

Тело `FinalizeBucketRequestDto`:

```text
FinalizeBucketRequestDto: bucket (int), oldShard (string — шард, артефакты
                          которого убирать; ≠ текущему владельцу)
```

Ответ 201: `BucketFinalizeQueuedDto: cluster, bucket, oldShard`.

Отказы: 400 (тело); 404 (кластер/шард не найдены, имена неканонические);
409 (кластер не Active; нешардированная БД; бакет вне диапазона / без
routing / не ACTIVE; `oldShard` = текущему владельцу; на бакете уже стоит
иная заявка; конкурентная заявка); 503 (etcd). `oldShard` в TO_REMOVE
допустим (финализация перед демонтажем — типовой путь эвакуации).

### 1.9. Контракты `POST /api/clusters/{cluster}/moves/abort` и `DELETE /api/clusters/{cluster}/moves/{bucket}`

**Abort** (протокол — 02 §9.7.4): отмена незавершённого переезда (уборка
артефактов, бакет возвращается владельцу). Тело `AbortBucketRequestDto`:
`bucket (int), force (bool, опц., по умолчанию false)`. Ответ 201:
`BucketAbortQueuedDto: cluster, bucket, force`.

`force` ломает две защиты (обе дублированы быстрыми серверными
пред-проверками — 409 с теми же текстами): свежесть статус-ключа
(`AbortMinAgeSec` — mover, возможно, ещё жив) и «routing уже = target»
(flip прошёл — abort станет уборкой старого шарда, как finalize).

Отказы abort: 400 (тело); 404 (кластер не найден); 409 (кластер не Active;
бакет вне диапазона / без routing; бакет не в переезде — ACTIVE/NOT_INITIALIZED;
свежий статус без force; routing==target без force; на бакете уже стоит иная
заявка; конкурентная заявка); 503 (etcd).

**Отмена заявки** (протокол — 02 §9.7.5): `DELETE` удаляет ключ стоящей
заявки — оператор снимает ещё не начатые заявки из очереди. Успех — 204;
404 «заявки нет» (повтор не идемпотентен осознанно); 503 (etcd). ⚠️ Удаление
не останавливает взятую в работу заявку: переезд доедет до конца по
статус-ключу; остановка начатого — только abort. UI-подтверждение
это предупреждает.

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
              по requestedUnix), work{op, phase, updatedUnix, lastError}?
              (nullable — журнал /pgworker/work/<C>, 02 §2.3.1; последний
              результат процесса воркера — результат исполненной/отвергнутой
              заявки переездов), heals[HealDto],
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
                      (§1.5, валидация 02 §9.7.1)
MovesQueuedDto: cluster, from, to, queued[int[]], skipped[int[]] — ответ 201 (§1.5)
RollbackBucketsRequestDto: buckets[int[]] — тело POST заявок на откат (§1.7,
                      валидация 02 §9.7.2)
RollbackQueuedDto: cluster, queued[int[]], skipped[int[]] — ответ 201 (§1.7)
FinalizeBucketRequestDto: bucket(int), oldShard(string) — тело POST заявки
                      уборки (§1.8, валидация 02 §9.7.3)
BucketFinalizeQueuedDto: cluster, bucket, oldShard — ответ 201 (§1.8)
AbortBucketRequestDto: bucket(int), force(bool, опц.) — тело POST заявки
                      отмены переезда (§1.9, валидация 02 §9.7.4)
BucketAbortQueuedDto: cluster, bucket, force — ответ 201 (§1.9)
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

Грань «Хранилище бэкапов» (t08): `BackupStorageDto`,
`BackupShardStorageDto`, `BackupObjectsPageDto` (+ вложенные `BackupFullDto`,
`BackupWalDto`, `BackupOrphanDto`, `MinioHealthDto`); camelCase; S3-ключи
(AccessKey/SecretKey) НЕ отдаются никогда.

## 3. Панели UI

| Панель | Что показывает |
|---|---|
| **Login** | форма логин/пароль; ошибка 401 |
| **Overview** | бейдж stale; карточки: etcd (reachable, endpoints ok/total; alarms — в ленте алертов и на панели etcd), кластеры (шарды/бакеты/переезды), активные переезды списком, лента алертов (critical/warning); сводка HA: скольки scope'ов без лидера (клиентская агрегация `GET /api/ha` — `OverviewDto` HA-полей не содержит); карточка «Бэкапы»: health MinIO (бейджи api/live/cluster) + место (used/quota, state-бейдж OK/WARN/CRIT воркера, live-факт) — клиентская агрегация `GET /api/backups/storage` (по образцу HA-агрегации, `OverviewDto` backup-полей не содержит) |
| **etcd** | таблица endpoints (reachable, latency, версия, raftTerm, ошибки, метка «активный»), members (+лидер), alarms; `lastRefreshUtc` |
| **Clusters** | список: имя, dbname, N, шард мастеровых/всего, активные переезды, пометки (incomplete, not-initialized, «к удалению» при `toRemove`); кнопка «Создать кластер» → модальная форма (§3.1) |
| **Cluster details** | вкладки: Шарды (dsn, replicas, master+leaseAlive, sync-standby, лаг слотов; ноды: имя+state; заявка ресурсов на ноду cpu/mem/disk; колонка действий — кнопка «Убрать шард» (красная, per-row; диалог со счётчиком бакетов шарда, дизейбл при N>0 с пояснением «сначала перевезите бакеты», серверный 409 — текстом ProblemDetails); бейдж «к удалению» у шарда state=TO_REMOVE; кнопка «Добавить шард» в заголовке вкладки — модальная форма §3.2: реплики/CPU/память/диск, без имени — генерируется; подпись «Шард стартует пустым — перераспределение бакетов выполняется отдельными явными переездами»; кнопки скрыты, когда кластер не Active — симметрия с «Удалить кластер»), Бакеты (грид id×owner×state, фильтр по owner/state, подсветка не-ACTIVE, возраст; вкладка скрыта при `sharded=false` — нешардированная БД 1×1 без карты бакетов, 02 §9.1; кнопка «Перенести бакеты» в заголовке вкладки (только Active && sharded — canScale) — модальная форма §3.3; колонка действий per-row у ACTIVE-бакетов при canScale — пост-переездные операции: «Откатить» (модал §3.4) и «Финализировать» (модал §3.5); у бакета со стоящей заявкой вместо кнопок бейдж «в очереди: <op>»), Переезды (только не-ACTIVE, кроме NOT_INITIALIZED: phase, updated, last_error; колонка действий per-row — красная «Отменить переезд» (abort, модал §3.6 с чекбоксом force) при canScale; блок «Очередь заявок» — `pendingMoves` по возрастанию requestedUnix: бакет, op, to, возраст заявки, кем поставлена, колонка действий «Снять заявку» (DELETE moves/{bucket}, подтверждение «начатый переезд доедет — остановка только abort») при canScale; исчезновение заявки без смены routing/status = отвергнута PgWorker'ом; блок «Журнал воркера» — поле `work` деталей: последний op/phase/updated/last_error процесса (результат исполненной/отвергнутой заявки)), Heals (журнал), «Стендовая топология» (блок по `standNodes` деталей — реестр `/cluster/nodes/`, скрыт при пустом); шапка: бейдж TO_REMOVE, кнопка «Сменить app-пароль» (только Active; → `POST /api/clusters/{cluster}/app-password/rotate` — 02 §9.8; модальное подтверждение с предупреждением «после применения подключения со старым паролем отвергаются до перечитывания кредов приложением — выполняйте в тихое окно»; 409 «уже запрошена» — текстом) и кнопка «Удалить кластер» (красная, с подтверждением; → `DELETE /api/clusters/{name}` — 02 §9.4; при `state=TO_REMOVE` обе кнопки скрыты — обратного перехода нет) |
| **HA** | список scope'ов: scope, cluster/shard, лидер, члены (роль/состояние), лаг max, пометка unmatched |
| **HA details** | leader, optime, таблица members: name/role/state/timeline/lag/probe-статус; блок «Заявленные ресурсы нод» (request_*, при наличии); raw config (свернуто) |
| **Alerts** | таблица всех алертов: severity-цвет, kind, target, message, since; фильтр по severity |
| **Воркеры** | `/workers` (§3.7): карточки PgWorker/KafkaWorker/ValkeyWorker — инстансы (url, uptime, health), целевой серт API (метаданные + статус применения applied/pending restart/unmanaged), действия: сгенерировать/загрузить/убрать сертификат, перезапустить воркера |
| **Хранилище бэкапов** | `/backups-storage` (t08, read-only): карточки Health (api/live/cluster + drives), Место (used/quota, прогресс-бар, state-бейдж OK/WARN/CRIT — вердикт воркера + штамп live), Buckets; таблица «Кластеры → шарды» (размер, полные шт., WAL-сегменты шт., пометки сверки); клик → детали шарда `/backups-storage/:cluster/:shard`: таблица полных (id/размер/дата/etcd state+verify/сверка), блок WAL (etcd-статус + S3-факт), бейдж активного restore, «Объекты» с on-demand пагинацией («Загрузить ещё»); блок «Осиротевшие префиксы» (реестр воркера OBSERVED/DELETING+TTL или «панель видит, в реестре нет»); `configured=false` — заглушка «не настроено (AdminPanel:Backups:S3)». Без форм ввода |

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
id». Клиентская валидация — зеркало 02 §9.7.1 (непустой выбор, from ≠ to);
серверная — источник истины. Отправка — POST `/api/clusters/{cluster}/moves`
(§1.5); успех → сводка-Alert в открытой форме («поставлено в очередь: N,
уже стояли: M» при непустом `skipped`) с кнопкой «Готово» — закрывает форму
(тостов нет — notification-библиотека в проект не входит); инвалидация
`clusters`-запросов и деталей кластера — сразу (очередь заявок появится на
вкладке «Переезды» со следующего тика, переезды начнутся асинхронно);
ошибка — ProblemDetails в теле формы
(409 — guard'ы 02 §9.7.1, 400 — по полям, 503 — «etcd недоступен»). Двойной
клик защищён блокировкой кнопки на время мутации.

Общие элементы: переключатель интервала polling (2/5/15 с/off, default 5 с,
выбор сохраняется в localStorage), тёмная тема, авто-logout при 401
(redirect на /login), stale-бейдж в шапке layout'а — по `snapshotAgeMs`/`stale`
ответа `/api/overview`, опрашиваемого с текущим polling-интервалом (при
недоступности данных — «нет данных»), счётчики critical/warning у пункта
«Алерты» в навигации (клиентский подсчёт по ответу `/api/alerts`, опрашиваемому
с тем же интервалом; скрыты при нуле/ошибке). Форм ввода семь: логин,
создание кластера (§3.1), добавление шарда (§3.2), перенос бакетов (§3.3),
откат бакета (§3.4), финализация бакета (§3.5) и отмена переезда (§3.6) —
всё остальное панель немая по отношению к данным.

### 3.4. Форма «Откатить бакет» (заявка rollback)

Модальный диалог per-row кнопки «Откатить» у ACTIVE-бакета на вкладке
Бакеты (только Active-кластер с `sharded=true`; у бакета со стоящей заявкой
кнопки пост-переездных операций заменены бейджем «в очереди: <op>»).
Тело — без полей выбора: «Откатить `bucket_<i>` на прежний шард» —
направление определяет PgWorker по живой обратной подписке `sub_<b>_rb`
(если SQL-проба включена и видит подписку — подпись «вернётся на
<шард>»; проба выключена/не видит — «куда — определит воркер по обратной
подписке»). Предупреждение: «откат — зеркальный cutover с секундной
заморозкой записи; если обратной подписки нет — воркер отвергнет заявку
(откат только полным re-copy)». Отправка — POST
`/api/clusters/{cluster}/moves/rollback` (§1.7) с `{buckets:[id]}`;
успех → закрыть форму, инвалидировать детали кластера (заявка появится
в очереди вкладки «Переезды» со следующего тика); ошибка — ProblemDetails
в теле формы (409 — guard'ы 02 §9.7.2, 400 — по полям, 503 — «etcd
недоступен»). Двойной клик защищён блокировкой кнопки.

### 3.5. Форма «Финализировать бакет» (заявка finalize)

Модальный диалог per-row красной кнопки «Финализировать» у ACTIVE-бакета
на вкладке Бакеты (условия те же, что §3.4). Поля: Select «Убрать артефакты
на шарде» — все шарды ≠ текущего владельца (при живой SQL-пробе — шарды,
где видны подписки бакета `sub_<b>`/`sub_<b>_rb`, помечены подсказкой
«живая подписка»; TO_REMOVE-шарды допустимы — финализация перед демонтажем).
Сильное подтверждение: «на выбранном шарде будет DROP SCHEMA `bucket_<i>`
СО ДАННЫМИ (необратимо); подписки/публикации/слоты срезаются; владелец
не трогается». Отправка — POST `/api/clusters/{cluster}/moves/finalize`
(§1.8); успех/ошибки — как §3.4 (тексты 409 — guard'ы 02 §9.7.3).

### 3.6. Форма «Отменить переезд» (заявка abort) и снятие заявки

**Abort** — модальный диалог красной per-row кнопки «Отменить переезд»
у не-ACTIVE строки вкладки Переезды (SYNCING/FROZEN/ABORTING; только
Active-кластер). Показывает маршрут owner→target, фазу, возраст статуса.
Чекбокс `force` (по умолчанию выключен) с пояснением: «ломает защиту
свежести (переезд, возможно, ещё жив) и разрешает доведение перевода,
когда flip уже прошёл (уборка старого шарда, как finalize) — включайте
только если mover точно мёрт». Предупреждение: «артефакты переезда
убираются, бакет возвращается владельцу». Отправка — POST
`/api/clusters/{cluster}/moves/abort` (§1.9); серверные 409 (свежий
статус / routing==target без force) — текстом ProblemDetails в теле формы.

**Снятие заявки** — подтверждение per-row кнопки «Снять заявку» строки
очереди заявок (вкладка Переезды; только Active-кластер): «заявка
`<op> bucket_<i>` будет удалена из очереди. Если переезд уже начат —
он доедет до конца; остановка начатого переезда — только „Отменить
переезд" (abort)». Отправка — DELETE `/api/clusters/{cluster}/moves/
{bucket}` (§1.9); успех → инвалидация деталей; 404 «заявки нет» —
тихо обновить (оператор мог опередить исполнение).

### 3.7. Грань «Воркеры» (сертификаты API + перезапуск)

Страница `/workers` — карточки PgWorker, KafkaWorker и ValkeyWorker
(02 §9.9; ValkeyWorker — t03). В каждой:

- **Инстансы**: instance id, URL, uptime (since_unix), health-бейдж,
  thumbprint применённого серта.
- **Целевой серт** (из `/workers/api_tls/<worker>`): subject, issuer, SAN,
  сроки, thumbprint, кем/когда обновлён; статус применения per-instance
  (`applied` / `pending restart` / `unmanaged` / `unknown`); при
  `pending restart` — бейдж «требуется перезапуск».
- Кнопка **«Сгенерировать сертификат»** (self-signed лист, 02 §9.9):
  подтверждение с предупреждением, что применится только после
  перезапуска.
- Кнопка **«Загрузить сертификат»**: форма PEM (cert + key; textarea×2 или
  файлы). Клиентская валидация — зеркало серверной (02 §9.9); серверная —
  источник истины: 422 «сертификат влияет на коммуникации воркеров с их
  подчинёнными сервисами — обновление отклонено» выводится ЯВНО
  (баннер-ошибка в теле формы, с причиной: CA / clientAuth / совпадение
  fingerprint с per-cluster CA).
- Кнопка **«Перезапустить воркера»**: подтверждение с предупреждением
  «идущие операции (provisioning/переезды/ротации) продолжатся после
  подъёма — клэймы и журнал в etcd; краткое окно недоступности API
  (секунды)»; после 202 — статус применения серта обновится тиками.
- Кнопка **«Убрать управляемый сертификат»** (красная; только при живом
  ключе): подтверждение «воркер вернётся к env-сертификату после
  перезапуска».

PEM-материалы (cert/key) в UI не отображаются и в API не отдаются — только
метаданные.

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
| `cluster-not-initialized` | info → warning | кластер в `NOT_INITIALIZED` (заявлен, ноды не подняты) — заметка; **эскалация** до warning, когда завис дольше нормы: возраст (с `created_unix`, fallback — возраст алерта) > `NotInitializedWarnSec` (900 c > бюджета PatroniBootSec воркера 600 c) | config.state |
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
| `probe-failed` | по цели (critical/warning) | ошибки проб **Active-целей**: SQL-проба шарда упала → **critical** («шард недоступен»: ни один хост DSN не принял подключение или writable-мастер не найден — 02 §6.2); Patroni-проба одного члена скопа упала → **warning**; Patroni-пробы **всех** членов matched-скопа упали → один **critical** на скоп (id `probe-failed:patroni-scope:<scope>`, per-member warning этого скопа не эмитятся — один факт, один алерт) | пробы |
| `backup-s3-unreachable` | warning | грань настроена && инвентарь-тик MinIO падал ≥ 2 подряд (configured && consecutiveFailures ≥ 2); снимается первым успешным тиком | MinIO live-тик (02 §2.5) |

SQL-алерты вычисляются только при включённых пробах; etcd-алерты — всегда.
`probe-failed` считается по целям текущего снапшота (исчезнувшая цель не
алертится) и только для **Active**-целей: кластеры/шарды в `NOT_INITIALIZED`
(подъём — ноды ещё не готовы) и `TO_REMOVE` (демонтаж — ноды снимаются)
не алертятся — нормальный жизненный цикл, не авария (прецедент — подавление
`shard-no-leader`); сами пробы по ним продолжают ходить, runtime-ошибки
остаются в UI деталей.
`NOT_INITIALIZED`-бакеты — не переезды: `move-*` правила их не алертят
(`move-frozen-long`/`move-aborting` смотрят свои точные состояния,
`move-flipped-status-stuck` — требует `target`, у NOT_INITIALIZED его нет);
бейдж «не инициализирован» в UI + `cluster-not-initialized` (info) вместо
critical-шума от ещё не поднятого кластера.

### 4.1. Объяснения и движитель (Hint / Remedy)

Каждый алерт несёт оператору не только факт, но и **что делать** — два
обязательных поля у каждого kind (модель `Alert` расширена; пустых нет):

- **`Hint`** (строка, русский): что именно не так, **как должно быть** и
  **для чего** этот ключ/инвариант существует. Пример (bucket-lost):
  «routing указывает на шард s3, которого нет в декларации кластера; routing —
  единственный авторитет "где бакет", дыры и висячие ссылки ломают переезды и
  SQL-сверку инвентаря; должен существовать ключ /clusters/<C>/shards/s3/replicas
  либо routing переведён на живой шард».
- **`Remedy`** (enum `AlertRemedy` + текст): **движитель** — кто закрывает
  алерт:
  - `WorkerAuto` — воркер сам репарирует/дожимает (алерт исчезнет сам;
    прогресс/причина — в `/pgworker/work/<C>` или `/kafkaworker/work/<C>`);
    принцип «PgWorker — хозяин кластера»: вечный алерт этого класса = дефект
    воркера, а не норма;
  - `OperatorApi` — оператор вызывает конкретную мутацию через API панели
    (в тексте — какая; напр. «POST /api/clusters/{c}/moves — вернуть бакет
    на живой шард»);
  - `OperatorRunbook` — ручной разбор (etcdctl/скрипты; ссылка на arch-док).

Маппинг движителей по каталогу: etcd-инфраструктурные (`etcd-*`,
`snapshot-stale`) и доступность воркеров (`worker-api-unreachable` —
`OperatorRunbook`/`OperatorApi`: «запустите воркер — ключи /pgworker/api/»),
конфигурация контроль-плейна (`cluster-incomplete`, `key-malformed`,
`bucket-*`, `shard-no-master`, `move-*`) — `WorkerAuto` либо `OperatorApi`;
probe-алерты (`ha-member-not-streaming`, `replica-lag-high`, `slot-*`,
`sync-standby-missing`, `inventory-mismatch`, `probe-failed`) — `WorkerAuto`
(надзор/репаратор) или `OperatorRunbook`; lifecycle-заметки
(`cluster-not-initialized`, `kafka-cluster-*`, `*-pending`) — `WorkerAuto`.
Точный текст Hint/Remedy каждого kind — код правил (тест-фикстуры эталон).

Новые kind доступности API исполнителей (в обоих снапшотах):

| kind | severity | Условие | Источник |
|---|---|---|---|
| `worker-api-unreachable` | critical | нет живых ключей `/pgworker/api/<id>` (или `/kafkaworker/api/<id>`) — мутации домена из панели недоступны (503) | снапшот (lease-ключи дискавери) |

Kind наблюдаемости провижининга и здоровья воркера (2026-09-01; источники —
02 §2.3.1 `work`-журналы и опрос `/healthz`):

| kind | severity | Условие | Источник |
|---|---|---|---|
| `provision-stuck` | warning | `/pgworker/work/<C>`: `last_error` жив + возраст серии фейлов (`now − fail_first_unix`) > `ProvisionStuckSec` (300 c) — воркер сообщил причину (текст — в Message/details), но кластер не инициализируется | `/pgworker/work/<C>` |
| `worker-unhealthy` | warning | живой lease-ключ `/pgworker/api/<id>` (или `/kafkaworker/api/<id>`, t09), но опрос `/healthz` ≠ 200 (503 degraded / сетевой сбой) — lease ещё не истёк, а процесс уже нездоров (docker-healthcheck гасит контейнер, ключи вот-вот исчезнут) | тик опроса /healthz |

UI: карточка алерта раскрывает Hint и Remedy (бейдж движителя); API
`/api/alerts` отдаёт поля `hint`, `remedy` (строка `worker-auto` /
`operator-api` /`operator-runbook`) и `remedyText` — расширение обратно
совместимо.

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
| `POST /api/kafka/clusters/{cluster}/topics` | создание топика — lifecycle-заявка (02 §10.2-9, t01): тело `CreateTopicRequestDto` → 201+`KafkaTopicCreatedDto` \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/topics/{topic}` | удаление топика — lifecycle-заявка (02 §10.2-10, t01; идемпотентен): 204 \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.create` | отмена заявки создания (02 §10.2-11, t01): 204 \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.delete` | отмена заявки удаления (02 §10.2-12, t01; окно деструктивности): 204 \| 404 \| 409 \| 503 |
| `POST /api/kafka/clusters/{cluster}/rebalance` | заявка ребалансировки партиций (02 §10.2-13, t02): без тела → 201+`KafkaRebalanceRequestedDto` \| 404 \| 409 \| 503 |
| `DELETE /api/kafka/clusters/{cluster}/rebalance` | отмена заявки ребалансировки (02 §10.2-14, t02): 204 \| 404 \| 503 |

`GET /api/alerts` объединяет алерты обоих движков (kind уже различает
`kafka-*`); `GET /api/overview` получает kafka-сводку (clustersTotal,
clustersCritical — critical-алерты `kafka-broker-not-running`/
`kafka-endpoints-missing`).

### 7.2. DTO (ключевые поля)

```text
KafkaClusterSummaryDto: name, state(ACTIVE|NOT_INITIALIZED|TO_REMOVE),
    brokersTotal, brokersRunning, topicsCount, endpoints,
    rotationPending(bool), rebalancePending(bool)
KafkaClusterDto: name, state, replicationFactor, minInSyncReplicas,
    defaultPartitions, defaultRetentionMs, createdUnix, endpoints,
    brokers[KafkaBrokerDto], topics[KafkaTopicDto], groups[KafkaGroupDto]
    (волна C — из пробы), rotation{requestedUnix, requestedBy}?(nullable),
    rebalance{requestedUnix, requestedBy}?(nullable),
    reassignment{mode(drain|balance), drainBroker?, partitionsTotal,
    partitionsRemaining, updatedUnix}?(nullable — ключа нет = операции нет)
KafkaBrokerDto: name, state(raw: NOT_INITIALIZED|PROVISIONING|RUNNING|
    UNREACHABLE|REMOVING|TO_REMOVE), role(controller|broker|null — до
    provisioning), cpu, memGi, diskGi (nullable — заявка resources),
    live(bool|null — из пробы: брокер id/host виден в DescribeCluster),
    brokerId(int|null — из пробы)
KafkaTopicDto: name, partitions, replicationFactor, retentionMs, minInSyncReplicas
    (null — конфиг отсутствует в факте), desired{partitions, retentionMs,
    minInSyncReplicas, requestedUnix, requestedBy}?(nullable), missing(bool),
    syncedUnix, lifecycle{op(create|delete), partitions?, replicationFactor?,
    retentionMs?, minInSyncReplicas?, requestedUnix, requestedBy}?(nullable — t01;
    create-заявка без факт-ключа — «виртуальная» строка: факт-поля null/0,
    параметры — в lifecycle-части)
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
CreateTopicRequestDto: name, partitions?(1..1000 def config.default_partitions),
    replicationFactor?(1..9 ≤ brokers def config.replication_factor),
    retentionMs?(1..2147483647 опц.), minInSyncReplicas?(1..RF опц.)
    — валидация 02 §10.3 (t01)
KafkaTopicCreatedDto: cluster, topic, partitions, replicationFactor (t01)
KafkaRebalanceRequestedDto: cluster, requestedUnix, requestedBy (t02)
```

### 7.3. Панели UI

| Панель | Что показывает |
|---|---|
| **KafkaClusters** | список: имя, state-бейдж, брокеры running/всего, топики (кол-во), endpoints (сокращённо), бейдж ротации; кнопка «Создать кластер» → модальная форма §7.3.1 |
| **KafkaClusterDetails** | шапка: state-бейджи (TO_REMOVE/NOT_INITIALIZED), бейдж reassignment («drain broker4: осталось 5/12 партиций» / «ребалансировка: 7/20»), кнопки «Изменить параметры» (default-конфиги — модал), «Сменить app-пароль» (модал-предупреждение о rolling-перезапуске брокеров; 409 «уже запрошена» — текстом), «Перебалансировать» (модал-предупреждение о переносе данных между брокерами; живая заявка — «Отменить ребалансировку»; 409 — текстом), «Удалить кластер» (красная, подтверждение; при TO_REMOVE скрыты); вкладка **Брокеры**: name/state/role/resources/live, колонка действий «Убрать брокера» (controller/последний — дизейбл с пояснением, серверный 409 текстом; непустой — подпись «drain: осталось N партиций» по прогресс-ключу, кнопка активна: воркер сам дренирует и демонтирует) + кнопка «Добавить брокера» (форма resources); вкладка **Топики** (t01): кнопка «Создать топик» (модал: name/partitions/RF/retention/minISR, дефолты из config кластера, клиентская валидация-зеркало 02 §10.3), per-row бейджи lifecycle-заявок («создание: N партиций, RF R» / «удаление…» + возраст/автор) с кнопкой «Отменить заявку», красная per-row «Удалить топик» (подтверждение с вводом имени топика: «данные будут удалены безвозвратно; заявка исполнится в течение ~15 с, до этого можно отменить»); create-заявка без факт-ключа — «виртуальная» строка (факт-поля `—`, параметры в бейдже); подпись: «создание/удаление топиков — заявками панели; внешние изменения (CLI/клиенты) подхватываются автосинком»; `canMutate` = Active; вкладка **Группы** — волна C (до неё — заглушка) |

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
| `kafka-topic-create-pending` | info | живая create-заявка `topics/<T>/desired.create` (t01) |
| `kafka-topic-delete-pending` | warning | живая delete-заявка `topics/<T>/desired.delete` (t01 — деструктивная близка к исполнению) |
| `kafka-lifecycle-stale` | warning | lifecycle-заявка не снята дольше `StaleDesiredSec` (600) — воркер буксует/кластер лежит (t01) |

## 8. Valkey: панель и REST API (`/api/valkey/*`) — t03

Четвёртый домен (etcd-контракт — 02 §11, канон ключей — arch/20; мутации
исполняет ValkeyWorker, arch/21 §1.1). Источник данных — отдельный снапшот
`ValkeySnapshot` (свой refresher, тик 3 с) + опциональная live-проба (тик 15 с,
PING по admin-креду из etcd; пароль в UI/API не отдаётся никогда — 02 §11.1).
v1: топология standalone `nodes=1` (всегда нода `node1`).

### 8.1. Список эндпоинтов

| Метод+путь | Назначение |
|---|---|
| `GET /api/valkey/clusters` | сводный список valkey-кластеров |
| `POST /api/valkey/clusters` | создание кластера (02 §11.2-1): тело `CreateValkeyClusterRequestDto` → 201+`ValkeyClusterCreatedDto` \| 400 \| 409 \| 503 |
| `GET /api/valkey/clusters/{cluster}` | детали: config, ноды (state/resources/live), endpoints, ротация |
| `DELETE /api/valkey/clusters/{cluster}` | перевод в TO_REMOVE (02 §11.2-2; демонтаж исполняет воркер): 202 \| 404 \| 503 |
| `PUT /api/valkey/clusters/{cluster}/config` | изменение `maxmemory_bytes`/`maxmemory_policy` (02 §11.2-3; converge `CONFIG SET` без рестартов): тело `ValkeyConfigUpdateRequestDto` → 200+`ValkeyConfigUpdatedDto` \| 400 \| 404 \| 503 |
| `PUT /api/valkey/clusters/{cluster}/nodes/{node}/resources` | лимиты ноды (02 §11.2-4; применит автоконверге надзора — пересоздание контейнера): тело `ValkeyResourcesRequestDto` → 200+`ValkeyResourcesUpdatedDto` \| 400 \| 404 \| 503 |
| `POST /api/valkey/clusters/{cluster}/password/rotate` | заявка ротации пароля app\|admin (02 §11.2-5; окно двух паролей, без рестартов): тело `{role}` → 202+`ValkeyPasswordRotatedDto` \| 400 \| 404 \| 409 \| 503 |

`GET /api/alerts` объединяет алерты всех движков (kind `valkey-*`);
`GET /api/overview` получает valkey-сводку (clustersTotal, clustersCritical —
critical-алерты `valkey-node-not-running`/`valkey-endpoints-missing`).

### 8.2. DTO (ключевые поля)

```text
ValkeyClusterSummaryDto: name, state(ACTIVE|NOT_INITIALIZED|TO_REMOVE),
    nodesTotal (=1), nodesRunning (=0|1), endpoints,
    rotationPending(bool), maxmemoryBytes, maxmemoryPolicy
ValkeyClusterDto: name, state, nodesTotal(=1), maxmemoryBytes,
    maxmemoryPolicy, createdUnix, endpoints, nodesList[ValkeyNodeDto],
    rotation{role(app|admin), requestedUnix, requestedBy}?(nullable)
ValkeyNodeDto: name(=node1), state(raw: NOT_INITIALIZED|PROVISIONING|
    RUNNING|UNREACHABLE|REMOVING|TO_REMOVE), cpu, memGi, diskGi
    (nullable — заявка resources), live(bool|null — из пробы PING;
    null — проба молчит/кредов нет), probeError?(string|null)
CreateValkeyClusterRequestDto: name, maxmemoryBytes?(def 536870912),
    maxmemoryPolicy?(def allkeys-lru; 8 значений канона),
    resources{cpu?(0.01..64 def 1), memGi?(1..65536 def 1),
    diskGi?(1..65536 def 10)} — валидация 02 §11.3 (инвариант
    maxmemoryBytes < memGi-лимита, R3)
ValkeyClusterCreatedDto: name, state:"NOT_INITIALIZED", nodes,
    maxmemoryBytes, maxmemoryPolicy, cpu, memGi, diskGi
ValkeyConfigUpdateRequestDto: maxmemoryBytes?, maxmemoryPolicy?
    (null = не менять)
ValkeyConfigUpdatedDto: cluster, maxmemoryBytes, maxmemoryPolicy
ValkeyResourcesRequestDto: cpu?, memGi?, diskGi? (null = не менять)
ValkeyResourcesUpdatedDto: cluster, node, cpu, memGi, diskGi
ValkeyRotateRequestDto: role(app|admin)
ValkeyPasswordRotatedDto: cluster, role, requestedUnix, requestedBy
```

### 8.3. Панели UI

| Панель | Что показывает |
|---|---|
| **ValkeyClusters** | список: имя, state-бейдж, нода running/всего, endpoints (сокращённо), maxmemory (MiB/GiB) + policy, бейдж ротации; кнопка «Создать кластер» → модальная форма §8.3.1 |
| **ValkeyClusterDetails** | шапка: state-бейджи (TO_REMOVE/NOT_INITIALIZED), endpoints, кнопки «Изменить конфиг» (maxmemory/policy — модал с UI-предупреждением R3: `maxmemory` < mem-лимита, иначе OOM-килл), «Сменить app-пароль» и «Сменить admin-пароль» (модалы-предупреждения: после применения подключения со старым паролем отвергаются до перечитывания кредов — окно двух паролей закрывает перекрытие; 409 «уже запрошена» — текстом), «Удалить кластер» (красная, подтверждение; при TO_REMOVE скрыты); вкладка **Нода**: name/state/resources/live (из PING-пробы; null — проба молчит) + кнопка «Изменить ресурсы» (модал cpu/mem/disk; подпись «применяется пересозданием контейнера — кеш холодным стартом восполняется приложениями», disk — инфо-поле); `canMutate` = Active; admin-креды пробы в UI/API не отдаются никогда — только факт живости (live) |

### 8.3.1. Форма «Создать valkey-кластер»

Модальный диалог (Mantine) с кнопки «Создать кластер» на панели Valkey: имя;
maxmemory (MiB, def 512); policy (select из 8 значений канона, def
allkeys-lru); группа «Ресурсы ноды»: CPU/память/диск (def 1/1/10). Клиентская
валидация — зеркало 02 §11.3 (вкл. инвариант maxmemory < mem); серверная —
источник истины. Отправка — POST `/api/valkey/clusters`; успех → инвалидация
списка (новый кластер с бейджем «не инициализирован»); ошибка — ProblemDetails
в теле формы. Двойной клик — блокировка кнопки.

### 8.4. Каталог valkey-алертов (`ValkeyAlertEngine`)

Чистая функция `ValkeySnapshot (prev, next) → Alert[]`; пороги —
`AdminPanel:ValkeyAlerts`. sinceUnix — по стабильному `id = kind:target`
(§2-механика). Ротационный алерт живёт только у живого кластера: заявка
ротации удаляется исполнением E2/E3 или демонтажом кластера (arch/21 X2) —
вечный `valkey-rotation-pending` невозможен по построению.

| kind | severity | Условие |
|---|---|---|
| `valkey-cluster-not-initialized` | info | state=NOT_INITIALIZED |
| `valkey-cluster-to-remove` | info | state=TO_REMOVE |
| `valkey-node-not-running` | critical | Active-кластер, нода state ∉ {RUNNING}, кроме fresh-PROVISIONING (< 60 с) |
| `valkey-endpoints-missing` | critical | Active без `endpoints` (arch/20 §5) |
| `valkey-rotation-pending` | info | живая заявка ротации `/valkeyworker/rotations/<C>` |
| `valkey-key-malformed` | warning | valkey-ключ не разобран (parseError; arch/20 §5) |
| `worker-api-unreachable` | critical | нет живых ключей `/valkeyworker/api/` (02 §2.3.3) — valkey-мутации панели 503; target `valkeyworker` |
| `worker-unhealthy` | warning | живой ключ, но `/healthz` ≠ 200 (02 §2.3.3); target `valkeyworker/<id>` |

## 9. Версионирование контракта

Контракт API не версонируется (панель и API развёртываются одним артефактом,
фронт и бэк всегда согласованы). Изменение DTO — правкой этого документа
тем же PR, что и код.
