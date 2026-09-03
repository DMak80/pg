# Спецификация: t07 — UI явных переездов бакетов из панели AdminPanel

Дата: 2026-09-03. Канон-контракты (обновлены этой задачей, arch-first):
[`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md)
§9.7 (подразделы §9.7.1–§9.7.5),
[`arch/adminpanel/03-panels.md`](../../../arch/adminpanel/03-panels.md)
§1.5–§1.9, §2 (DTO), §3 (вкладки, формы §3.4–§3.6),
[`arch/14-pgworker.md`](../../../arch/14-pgworker.md) §1.1 (таблица API).
Roadmap-строка:
[`arch/roadmap/pgworker.md`](../../../arch/roadmap/pgworker.md)
`t07-move-bucket-ui` — «UI явных переездов бакетов из панели AdminPanel
(кнопки „перевезти/откатить/finalize/abort" → заявки
`/pgworker/moves/<C>/bucket_<i>`, чтение очереди заявок и их результатов;
выбор „кто куда переезжает" — только оператор, никакой
автоперебалансировки)». Выделена из t06; контракт исполнения заявок — t01
(в main).

## 1. Цель

Дать оператору в панели полный цикл операторских операций над переездами
бакетов: сегодня панель умеет **ставить заявки `op=move`** (форма
«Перенести бакеты» → `POST /api/clusters/{c}/moves`) и **читать** очередь
заявок + статусы переездов. Не хватает:

1. **Мутаций `rollback` / `finalize` / `abort`** — процесс PgWorker
   (`MoveProcess`) исполняет все четыре op, но API воркера ставит только
   `move`; поставить остальные можно лишь etcdctl'ом (runbook).
2. **Отмены стоящих в очереди заявок** — оператор, поставивший N бакетов,
   не может снять ещё не начатые заявки из UI (arch/02 §9.7 до этой задачи:
   «отмена/правка заявок — вне панели (runbook)»).
3. **Чтения результатов заявок** — перманентно-отвергнутая заявка исчезает
   из очереди молча (без изменения routing/status); причина отказа живёт
   только в журнале `/pgworker/work/<C>` (панель читает его в снапшот
   `PgWorkerWork`, но в UI кластера не показывает — только алерт
   `provision-stuck`).

Выбор «кто куда переезжает» — всегда явное решение оператора; никакой
автоперебалансировки (roadmap-формулировка; отдельных guard'ов не требует —
мутаций «перевезти всё/автоматом» просто нет).

Не-цели: пакетный rollback/finalize/abort (кнопки per-row, одиночные —
кроме rollback, который как и move принимает `buckets[]`, но вызывается из
UI по одному); автоподбор old_shard для finalize (только UI-подсказка по
SQL-пробе); правка/перезапись чужих заявок (runbook: etcdctl); отмена уже
взятой в работу заявки как «стоп-переезд» (остановка начатого — только
abort); batch-операции «перевезти все бакеты шарда» (очередь и так
пакетная — форма §3.3 уже умеет «выбрать все»).

## 2. Принципы

1. **arch-first**: контракты arch/02 §9.7, arch/03 §1.5–§1.9, arch/14 §1.1
   обновлены до кода (этой задачей); код — отражение контракта.
2. **Панель в etcd не пишет ничего**: все мутации — прокси в HTTP API
   PgWorker (паттерн etcd-via-worker-api); заявки ставит и отменяет воркер
   (txn-клэйм `version==0`, порядок `requested_unix` — общий протокол
   arch/02 §9.7 п.1–5).
3. **Отдельные эндпоинты per op** (решение пользователя): тела и guard'ы у
   ops различны (finalize — `old_shard`, abort — `force`, move —
   `from/to/buckets[]`); существующий `POST /moves` не меняется.
4. **Д4-паттерн guard'ов**: быстрые серверные пред-проверки по прямым
   чтениям etcd (`ClusterGuardData`) — сразу 404/409 оператору;
   авторитетно перепроверяет `MoveProcess` (гонки ловит он).
5. **Идемпотентность постановки**: уже стоящая **идентичная** заявка →
   `skipped` (не перезапись); иная — 409. Отмена заявки идемпотентностью
   не обладает (повтор → 404 «заявки нет», образец kafka-мутации §10.2-11).
6. **Последовательность — зона PgWorker**: панель только упорядочивает
   `requested_unix` в конец очереди; «одна заявка кластера за раз»
   гарантирует процесс (t01).
7. **Оператор предупреждён о необратимом**: finalize = `DROP SCHEMA`
   СО ДАННЫМИ; abort с `force` ломает защиты свежести; снятие заявки не
   останавливает начатый переезд — всё это тексты модальных
   подтверждений (порт стиля RecreateNode/RotateAppPassword).
8. Язык документации — русский, идентификаторы — английские; тесты — AAA;
   интеграционные фикстуры — динамические порты docker, `BrokerBootSec` ≤
   100 с (для этой задачи etcd-only фикстуры, docker не нужен).

## 3. Рассмотренные подходы (зафиксированные решения)

### 3.1. Отдельные эндпоинты per op (выбор пользователя)

Отвергнуто: один расширяемый `POST /api/clusters/{c}/moves` с полем `op` в
теле (обратная совместимость через `op=move` по умолчанию). Выбраны
отдельные: `POST .../moves/rollback`, `POST .../moves/finalize`,
`POST .../moves/abort`, `DELETE .../moves/{bucket}` — каждое со своим
телом/валидацией/ошибками; эндпоинт move и его фронт-контракт не трогаем.

### 3.2. Отмена стоящих заявок — включена (выбор пользователя)

`DELETE /api/clusters/{c}/moves/{bucket}` с guard'ом «заявка жива» (404
если нет). Осознанная семантика (в arch/02 §9.7.5): удаление **не
останавливает** взятую в работу заявку — процесс ведёт фазы по
статус-ключу бакета и доедет до конца; остановка начатого — только abort.
Guard «ещё не начата» не вводится: старейшая заявка становится «в работе»
в пределах одного тика PgWorker, детерминированно отличить её по ключам
etcd нельзя (процесс не помечает взятую заявку). Прежнее правило arch/02
«отмена/правка заявок — вне панели (runbook)» заменено: панель отменяет
свои стоящие заявки; правка (перезапись) остаётся вне панели.

### 3.3. Размещение кнопок — per-row по состоянию бакета (выбор пользователя)

rollback/finalize — во вкладке «Бакеты» у ACTIVE-бакетов (пост-переездные
операции; только Active-кластер, `sharded=true`, нет живой заявки);
abort — во вкладке «Переезды» у SYNCING/FROZEN/ABORTING-строк; снятие
заявки — в строках очереди заявок вкладки «Переезды». Отвергнута «единая
панель действий во вкладке Переезды» — операции разнесены по контексту
состояния бакета, где оператор уже видит нужные данные.

### 3.4. Результаты заявок — блок «Журнал воркера» (допущение)

Roadmap требует «чтение … их результатов». Статусы исполнения (SYNCING/
FROZEN/ABORTING + phase + last_error) читаются; молчит только судьба
**отвергнутой** заявки. Решение: блок «Журнал воркера» на вкладке
«Переезды» — поле `work` деталей кластера (последняя запись
`/pgworker/work/<C>`: op/phase/updatedUnix/lastError; источник уже в
снапшоте `PgWorkerWork`). Нового etcd-чтения нет; work-ключ один на
кластер — показывает последний процесс воркера (op виден, «move →
rejected + причина» читается оператором). Отдельная история заявок
(журнал всех rejected) — вынос (§9).

## 4. Контракт (сводка; канон — обновлённые arch/02 §9.7, arch/03, arch/14 §1.1)

### 4.1. Ключи etcd

Формат значений и семантика `/pgworker/moves/<C>/bucket_<i>` — без
изменений (канон arch/14 §3.3, `MoveRequest` t01):
`{"op":"move|rollback|finalize|abort","to"?,"old_shard"?,"skip_reverse"?,"resume"?,"force"?,"requested_unix":<unix>,"requested_by"?}`.
Панель по-прежнему читает очередь (§2.3.1) — читателей не меняли.

### 4.2. Мутации (исполняет API PgWorker; панель — прокси)

| Метод+путь | Тело | Guard'ы (быстрые, Д4) | Ответ |
|---|---|---|---|
| `POST /api/clusters/{c}/moves/rollback` | `{buckets: int[]}` | кластер Active (409); нешардированный (409); бакет ∈ 0..N-1, routing есть, state ACTIVE (409); массив непустой/без дубликатов (400) | 201 `{cluster, queued[], skipped[]}` |
| `POST /api/clusters/{c}/moves/finalize` | `{bucket: int, oldShard: string}` | Active (409); нешардированный (409); бакет ACTIVE (409); `oldShard` существует (404), ≠ владельцу (409); `oldShard` в TO_REMOVE допустим (финализация перед демонтажем) | 201 `{cluster, bucket, oldShard}` |
| `POST /api/clusters/{c}/moves/abort` | `{bucket: int, force?: bool = false}` | Active (409); бакет ∈ 0..N-1, routing есть (409); статус-ключ жив, state ∈ SYNCING/FROZEN/ABORTING (409 иначе: ACTIVE → «пост-flip артефакты убирает finalize», NOT_INITIALIZED → «не переезд»); свежесть: `now − updated_unix < AbortMinAgeSec` и `!force` (409, текст процесса); `routing.owner == status.target` и `!force` (409, «осознанно: force») | 201 `{cluster, bucket, force}` |
| `DELETE /api/clusters/{c}/moves/{bucket}` | — | имена канонические (404); ключ заявки жив (404 «заявки нет»); state кластера не проверяется (TO_REMOVE: заявки чистит D2 — ручная отмена безвредна) | 204 |

Общий протокол постановки (arch/02 §9.7 п.1–5) — как у move: чтение
префикса `/pgworker/moves/` напрямую (идентичная заявка → `skipped`, иная
→ 409), `requested_unix = max(now, maxUnix+1) + k` (в конец очереди),
txn-клэйм `NotExists + put` на заявку, сбой посередине — 503 без
компенсации (повтор досдаёт: идентичные → `skipped`). «Идентичная»:
rollback — `op=rollback` на том же бакете; finalize — `op=finalize` +
тот же `old_shard`; abort — `op=abort` + тот же `force`.

Тела заявок (канонический JSON `MoveRequest`, snake_case, null-поля
опускаются): rollback — `{op, requested_unix, requested_by}`; finalize —
`{op, old_shard, requested_unix, requested_by}`; abort —
`{op, force?, requested_unix, requested_by}`. `force` — nullable-поле
TicketBody handler'а с `WhenWritingNull`: `true` пишется, `false`
опускается; процесс `MoveRequest.Parse` трактует отсутствие как `false`
(bool-поле DTO).

`requested_by` — заголовок `X-Requested-By` (панель шлёт username
сессии), fallback `"api"` — как у move.

### 4.3. Панель — чтение

`ClusterDto` + поле `work{op, phase, updatedUnix, lastError}?` (nullable;
маппинг из `snapshot.PgWorkerWork` по кластеру, arch/03 §2). Источник уже
читается refresher'ом — новых etcd-чтений нет.

## 5. PgWorker (API)

### 5.1. Общий код постановки заявок

Логика постановки (чтение префикса, идентичность, база `requested_unix`,
txn-клэйм, `ParseTickets`/`AllTicketsMaxUnix`) сейчас внутри
`MoveBucketsHandler` — вынести в общий внутренний класс
`src/PgWorker.App/Api/Operations/MoveTickets.cs` (методы:
`ReadQueueAsync(gateway, endpoints, ct)` → перечень заявок + maxUnix;
`ClaimAsync(gateway, endpoints, key, jsonBody, ct)` → txn-клэйм).
`MoveBucketsHandler` переходит на него без изменения поведения (рефакторинг
с сохранением текстов ошибок; тесты MovesApiTests — зелёные без правок
ожиданий).

### 5.2. Handler'ы

`src/PgWorker.App/Api/Operations/` (порт паттерна `MoveBucketsHandler`;
все читают `ClusterGuardData.ReadAsync` напрямую):

- **`RollbackBucketsHandler`**: валидация тела (400) → guard'ы §4.2 →
  постановка `op=rollback` по одному ключу на бакет (порядок по
  возрастанию id, `base+k`). Тело заявки: op/requested_unix/requested_by.
- **`FinalizeBucketHandler`**: одиночная заявка `op=finalize` +
  `old_shard`. Guard `oldShard` ≠ routing.owner; существование шарда —
  по `Shards` (replicas-ключ, как move-guard'ы).
- **`AbortBucketHandler`**: одиночная заявка `op=abort` (+`force:true`
  при force). Быстрые пред-проверки семантики force — по
  `ClusterGuardData.Status` (расширение — §5.3) и `Routing`; порог
  свежести — `AbortMinAgeSec` из конфигурации (DI `MovesRuntimeOptions`;
  единый источник с процессом, `appsettings.json` `PgWorker:Moves`).
- **`CancelMoveHandler`**: валидация leaf'а `bucket_<int>` (неканонический
  → 404) → чтение ключа одним range по точному ключу
  `/pgworker/moves/<C>/bucket_<i>` → нет → 404 «заявки нет»; есть →
  `DeleteAsync` → успех.

### 5.3. Расширение ClusterGuardData

`Status: bucket → (State, Owner, Target)` → добавить `UpdatedUnix` (long?,
из JSON `updated_unix` статус-ключа) — нужно пред-проверке свежести abort.
Парсинг толерантный (поле может отсутствовать у старых ключей — трактуется
как «несвежая не бывает», т.е. пред-проверка свежести пропускается, авторитетно
решит процесс).

### 5.4. Регистрация и исключения

`ApiModule.MapWorkerApi` — 4 маршрута (POST×3 + DELETE), маппинг
исключений — порт существующих веток (400 с `errors`, 404, 409, 503;
успех POST — 201 без Location, DELETE — 204). Новые исключения в
`WorkerApiExceptions.cs`:

- `BucketNotActiveForMoveOpException(bucket, owner, state)` — 409 (rollback/
  finalize/abort: бакет не в требуемом состоянии; текст различается по op —
  как у процесса: rollback/finalize «возможен только из ACTIVE», abort —
  см. §4.2);
- `FinalizeTargetIsOwnerException(cluster, bucket, shard)` — 409 «совпадает
  с текущим владельцем — убирать нечего»;
- `MoveStatusFreshException(ageSec, thresholdSec)` — 409 «статус свежий —
  переезд, возможно, ещё жив; если mover точно мёртв — force»;
- `MoveAlreadyFlippedException(target)` — 409 «routing уже указывает на
  target — abort станет уборкой старого шарда (как finalize) — осознанно:
  force»;
- `MoveTicketNotFoundException(cluster, bucket)` — 404 «заявки нет».

Повторное использование: `ClusterNotFoundException`,
`ClusterNotActiveException`, `NonShardedClusterException`,
`MoveRequestConflictException`, `MoveClaimLostException`,
`ShardNotFoundException`, `MoveBucketsValidationException` (или новый
`MoveOpValidationException` с тем же 400-маппингом) — тексты как в arch/02.

## 6. AdminPanel

### 6.1. Backend (прокси)

`src/AdminPanel.Api/Operations/` — 4 команды-прокси (порт
`MoveBucketsCommand`; `WorkerProxy.SendAsync`, `"pgworker"`,
`X-Requested-By` = username сессии):

- `RollbackBucketsCommand(cluster, buckets, requestedBy)` :
  `ICommand<RollbackQueuedDto>`;
- `FinalizeBucketCommand(cluster, bucket, oldShard, requestedBy)` :
  `ICommand<BucketFinalizeQueuedDto>`;
- `AbortBucketCommand(cluster, bucket, force, requestedBy)` :
  `ICommand<BucketAbortQueuedDto>`;
- `CancelMoveTicketCommand(cluster, bucket, requestedBy)` :
  `ICommand<MoveTicketCancelledDto>` (воркер отвечает 204 без тела — DTO
  не читается, образец `DeleteClusterCommand`/`ClusterDeletedDto`).

Регистрация в `OperationsModule` (маршруты arch/03 §1.5–§1.9, camelCase
тела, ProblemDetails 1:1). `ClusterDetailsQuery`: поле `work` в
`ClusterDetailsDto` (из `snapshot.PgWorkerWork.FirstOrDefault(w =>
w.Cluster == cluster)`; все null-safe).

### 6.2. Frontend

`frontend/src/api/`:

- `dto.ts`: `RollbackBucketsRequestDto`, `RollbackQueuedDto`,
  `FinalizeBucketRequestDto`, `BucketFinalizeQueuedDto`,
  `AbortBucketRequestDto`, `BucketAbortQueuedDto`; `ClusterDetailsDto` +
  `work?: { op, phase, updatedUnix, lastError } | null`;
  `MoveTicketDto` — без изменений.
- `queries.ts`: `rollbackBuckets`, `finalizeBucket`, `abortMove`,
  `cancelMoveTicket` (порт `moveBuckets`/`removeShard`).

`frontend/src/pages/cluster-details/` (Mantine; порт стиля
`MoveBucketsModal`/`RemoveBrokerButton`; клиентская валидация — зеркало
серверной, серверная — источник истины; ProblemDetails в теле формы:
409 — yellow, 400/503 — red; двойной клик — блокировка кнопки):

- **`BucketsTab`**: колонка «Действия» (при `canScale`) у ACTIVE-бакетов —
  кнопки «Откатить» (variant light) и «Финализировать» (red light);
  у бакета со стоящей заявкой (по `pendingMoves`) вместо кнопок — бейдж
  «в очереди: `<op>`». Не-ACTIVE строки — пустая колонка.
- **`RollbackBucketModal`** (§3.4 arch/03): текст «Откатить `bucket_<i>` на
  прежний шард — направление определит воркер по живой обратной подписке
  (конвенция имён `sub_<bucket>_rb`)» + подсказка «вернётся на `<shard>`»
  при живой SQL-пробе (по `shards[].runtime.subscriptions`; проба
  выключена/не видит — подпись «куда — определит воркер»);
  предупреждение о зеркальном cutover с секундной заморозкой записи и об
  отвержении при отсутствии подписки. Отправка — `rollbackBuckets` с
  `{buckets: [id]}`; успех → закрыть + инвалидация деталей кластера.
- **`FinalizeBucketModal`** (§3.5): Select «Убрать артефакты на шарде» —
  шарды ≠ текущего владельца (с живой SQL-пробой — метка «живая подписка»
  у шардов с подписками бакета `sub_<bucket>`/`sub_<bucket>_rb`;
  TO_REMOVE — метка «к удалению»); сильное подтверждение «DROP SCHEMA СО ДАННЫМИ (необратимо);
  владелец не трогается». Отправка — `finalizeBucket`.
- **`MovesTab`**: новая колонка «Действия» в таблице не-ACTIVE-переездов
  (при `canScale`) — красная «Отменить переезд» → **`AbortMoveModal`**
  (§3.6): маршрут owner→target, фаза, возраст статуса; чекбокс `force`
  (выключен по умолчанию) с пояснением (свежесть/mover жив; flip прошёл —
  доведение как finalize); отправка `abortMove`; серверные 409
  (свежесть/routing==target) — текстом в теле формы. В очереди заявок —
  колонка «Снять заявку» (при `canScale`) → подтверждение «начатый
  переезд доедет до конца; остановка начатого — только „Отменить
  переезд"»; отправка `cancelMoveTicket`; 404 — тихо инвалидировать
  (оператора опередил тик воркера). Блок **«Журнал воркера»** — при
  ненулевом `work`: op, phase, возраст `updatedUnix`, `lastError` (red,
  Tooltip полного текста) + подпись «последний процесс воркера кластера;
  отвергнутые заявки — с причиной».
- **`ClusterDetailsPage`**: прокинуть `canScale` и `work` в `MovesTab`
  (сейчас передаются только buckets/pendingMoves).

Никаких новых алертов (жизненный цикл заявок — не инциденты; существующие
`move-*` правила не меняются). Polling — существующий (очередь/статусы
обновляются тиком refresher'а ≤ 5 с).

## 7. Тесты

- **Unit (PgWorker.UnitTests)**:
  - валидаторы тел: rollback (пустой массив/дубликаты), finalize
    (bucket/oldShard), abort (bucket/force);
  - guard-логика новых handler'ов на моках gateway (новые кейсы по §4.2):
    ACTIVE-требования, oldShard=owner, свежесть по `updated_unix`
    (+отсутствие поля), routing==target, отмена — 404/204;
  - канонический JSON заявок: rollback/finalize/abort roundtrip
    `MoveRequest.Parse` (тело, которое пишет handler, парсится процессом).
- **Integration API (PgWorker.IntegrationTests, `PgApiFixture` — etcd +
  PgWorker.App in-memory; образец `MovesApiTests`)** —
  `MoveOpsApiTests.cs`:
  - rollback: 201 (ключи `op=rollback` в etcd, `requested_by` из
    `X-Requested-By`, порядок в конец очереди), повтор → `skipped`;
    конфликт с живой move-заявкой → 409; не-ACTIVE бакет → 409;
    нешардированный → 409; 400 (пустой массив);
  - finalize: 201 (ключ с `old_shard`), oldShard=owner → 409, шард не
    найден → 404, TO_REMOVE-приёмник — 201;
  - abort: 201 (ключ с `force` только при true), ACTIVE-бакет → 409,
    NOT_INITIALIZED → 409, свежий статус без force → 409 (сидируем
    `updated_unix=now`), с force → 201, routing==target без force → 409;
  - отмена: 204 + ключ исчез, повтор → 404, чужой кластер/битый leaf →
    404;
  - `work`-поле панельного ClusterDetailsQuery — тестом в
    `AdminPanel.IntegrationTests` (порт `MovesApiTests`/`ClustersApiTests`:
    маппинг WorkJournalInfo → DTO, null при отсутствии журнала).
- **Стенд e2e**: `dev-stand/adminpanel/checks/60-move-ops.sh` (следующий
  свободный номер): через API панели на демо-сиде (bucket с зависшим
  статусом из сида) — abort без force (свежий/несвежий сценарии по
  `updated_unix` сида), rollback-заявка на ACTIVE-бакет, finalize с
  old_shard, снятие заявки из очереди; проверки — чтением etcd/деталей
  кластера панели (очередь, статусы, work-журнал). Полный docker-цикл
  move→abort/rollback→finalize уже покрыт E2e-сценариями PgWorker (t01) —
  здесь только API/UI-слой, без поднятия PG.

## 8. Волны реализации

- **Волна A — PgWorker API**: рефакторинг `MoveTickets` (общая постановка),
  `ClusterGuardData.UpdatedUnix`, 4 handler'а + исключения + маршруты;
  unit + integration.
- **Волна B — панель**: 4 команды-прокси + маршруты + `work` в DTO;
  фронтенд (dto/queries, модалки, колонки действий, журнал); тесты панели.
- **Волна C — стенд**: e2e-чек 60 (прогон на живом стенде).

Каждая волна — зелёные `dotnet build src/PgWorker.slnx` (0 warnings) +
`dotnet test`.

## 9. Ограничения, допущения, выносы

Допущения (обоснование — §3):

- Rollback из UI — по одному бакету (API принимает массив — симметрия move,
  переиспользуется будущими сценариями);
- направление rollback определяет только воркер (SQL-факт); UI-подсказка —
  best-effort по SQL-пробе (выключена/устарела — подпись «определит
  воркер»);
- finalize: подсказка old_shard по подпискам пробы; оператор волен выбрать
  любой шард ≠ владельца;
- отмена заявки: без «не-в-работе»-guard'а (§3.2), повтор не идемпотентен;
- work-блок показывает последний процесс (не историю заявок);
- AbortMinAgeSec пред-проверка дублирует процесс (единый источник —
  конфигурация `PgWorker:Moves`, дефолт 120 с).

Выносы (roadmap при потребности): история заявок/аудит отвергнутых (свой
ключ), пакетные rollback/finalize из UI, «перевезти все бакеты шарда»
одной кнопкой, right-forced auto-rebalance (никогда — вне философии
системы), подсказка «переехал давно, подписка могла умереть» по возрасту
flip.

## 10. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный
   (unit без Docker; integration — etcd-фикстуры; комментарии тестов — AAA).
2. **API воркера** (напрямую и через прокси панели): `POST …/moves/
   rollback|finalize|abort` и `DELETE …/moves/{bucket}` — коды/тексты по
   arch/02 §9.7.2–§9.7.5 (400/404/409/503; 201/204); тела заявок в etcd —
   канонический JSON `MoveRequest` (парсится `MoveRequest.Parse`).
3. **Идемпотентность/конфликты**: повтор rollback → `skipped`; живая иная
   заявка → 409; отмена снятой заявки → 404; снятая заявка исчезает из
   `pendingMoves` без изменения routing/status.
4. **Guard'ы abort**: свежий статус без `force` → 409 с текстом про
   AbortMinAgeSec; с `force` → 201; routing==target без `force` → 409
   «осознанно: force».
5. **UI**: вкладка «Бакеты» — per-row «Откатить»/«Финализировать» у
   ACTIVE-бакетов canScale-кластера (модалки §3.4/§3.5 с
   предупреждениями; бейдж «в очереди» у занятых); вкладка «Переезды» —
   per-row «Отменить переезд» (модал §3.6 с `force`-чекбоксом) у
   SYNCING/FROZEN/ABORTING, «Снять заявку» в очереди (предупреждение
   «начатый доедет»), блок «Журнал воркера» (op/phase/возраст/lastError).
   Серверные 409 — текстом ProblemDetails в модалке.
6. **Результаты**: отвергнутая заявка (например, rollback без обратной
   подписки) исчезает из очереди, а её причина видна в блоке «Журнал
   воркера» (op=rollback, phase=rejected, lastError) ≤ 2 тиков поллера.
7. **Регресс**: существующий `POST /moves` и форма «Перенести бакеты» —
   без изменений поведения (MovesApiTests зелёные без правок ожиданий).
8. e2e-чек `60-move-ops.sh` зелёный с чистого состояния стенда.

## 11. Открытые вопросы

Нет — решения пользователя зафиксированы (§3.1–§3.3: per-op эндпоинты;
отмена заявок включена; per-row размещение кнопок); остальные — допущения
§9.
