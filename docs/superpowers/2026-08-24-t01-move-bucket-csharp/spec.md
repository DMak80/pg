# Спецификация: t01-move-bucket-csharp — плановый переезд бакета в PgWorker (C#-порт P1–P8)

Дата: 2026-08-24. Фаза dev-flow: spec. Режим автономный: вопросы пользователю
запрещены («апрув на гейты и мерж, меня не спрашивать») — все неоднозначности
решены исполнителем, каждое решение с обоснованием в §13 «Принятые решения».
Источники: `arch/11-bucket-sharding.md` §5–§8 (runbook переезда — главный),
`arch/12-bucket-pitfalls.md` (P1–P8 — обязательны к учёту), `arch/14-pgworker.md`
(канон PgWorker), `arch/scripts/move-bucket.sh` / `abort-move.sh` /
`buckets-common.sh` (референс-семантика, проверенная на стенде PG 18.4),
спецификация бэкенда `docs/superpowers/2026-08-23-pgworker-backend/spec.md`
(§2 out of scope — эта задача закрывает первый пункт), референс `../Puzzle`
(паттерны уже воплощены в кодовой базе: `Result`, `RetryPolicies`,
BackgroundService-циклы, health checks).

---

## 1. Цель

Перенести механику **планового онлайн-переезда бакета** (схемы) между живыми
шардами из shell-скриптов в PgWorker как управляемый фоновый процесс:

1. **Move** — полный цикл P1–P8 из arch/11 §5: префлайт → перенос DDL →
   PUBLICATION/SUBSCRIPTION (logical replication, `copy_data=true`,
   `failover` (конфигурируемо, PG17+ — Д11), `synchronous_commit=remote_apply`)
   → ожидание initial copy →
   cutover (заморозка REVOKE+LOCK P1, FROZEN, лаг 0, sequence→sequence P6,
   сверка строк P8, атомарный etcd-flip) → прямая подписка срезается, обратная
   (для отката) ставится.
2. **Rollback** — возврат бакета на прежний шард через живую обратную подписку
   (зеркальный cutover, arch/11 §6).
3. **Finalize** — уборка после переезда/отката: подписки/публикации/слоты/схема
   на не-владельце, включая осиротевшие tablesync-слоты (P8).
4. **Abort** — отмена незавершённого переезда с уборкой артефактов
   (abort-move.sh: журнал ABORTING до манипуляций, идемпотентная уборка,
   режим доведения при routing==target — P7).

Свойства (наследуются от PgWorker): несколько инстансов — координация
lease-клэймами; смерть контролирующего инстанса не роняет переезд — takeover
продолжает с записанной фазы; все шаги идемпотентны; состояние — в etcd
(статус-ключ бакета, формат совместим со скриптами) и в самих шардах.
Семантика портируется **1:1 со скриптов** (включая найденные на стенде нюансы:
REVOKE — не барьер, барьер `LOCK TABLE ACCESS EXCLUSIVE`; у sequences отбираются
USAGE и UPDATE; issued/next считаются на стороне SQL).

Зачем: эвакуация MVP закрывает только «шард умер целиком»; штатные переезды
живых шардов (балансировка, вывод шарда из эксплуатации — будущий t06) до
этого порта остаются внешними скриптами с ручным присмотром.

## 2. Границы

### In scope

- Новый процесс PgWorker — **MoveProcess** (машина состояний, тиковая),
  обрабатывающий декларативные заявки на переезд/откат/finalize/abort в etcd.
- Контракт: заявки в `/pgworker/moves/<C>/bucket_<i>` (см. §4); статус-ключ
  `/clusters/<C>/buckets/status/bucket_<i>` — существующий формат (SYNCING /
  FROZEN / ABORTING + phase), без изменения читателей (панель, скрипты,
  эвакуатор).
- Вся SQL-механика переезда (freeze/unfreeze, pub/sub, ожидание слота,
  sequence→sequence, сверки инвентаря P5 и строк P8, sync-standby-префлайт,
  уборка слотов/артефактов) — C#/Npgsql, без внешних psql/psql-процессов.
- Перенос DDL: `pg_dump --schema-only` через `docker exec` внутри
  мастер-контейнера источника (см. Д3) + применение Npgsql-ом на приёмнике.
- Интеграция: `IClusterProcesses`/`ReconcileLoop` (кластер Active → надзор →
  обработка заявок), клэймы, work-журнал, снапшоты P12, чистка заявок при
  deprovisioning.
- Тесты: unit (машина состояний на моках, SQL-тексты), integration (etcd-
  контракт заявок), e2e на существующем стенде E2eFixture (полный цикл
  move→призрак→rollback→move→finalize по мотивам `arch/stand/checks/65-move-e2e.sh`).
- Правки arch/ (см. §12).

### Out of scope (roadmap)

- Балансировка «куда переезжать» (выбор target оператором/панелью в заявке;
  автоподбор по метрикам — roadmap t06).
- UI-кнопки в AdminPanel для заявок (панель читает статусы уже сейчас;
  написание заявок — будущий t06/frontend).
- Прогнозный префлайт места (P4-концепция «хватит ли WAL на переезд») — в
  скриптах не реализован (только warning по `wal_status='lost'`), не портим.
- Large objects (`pg_dump --blobs`), DDL во время переезда (мораторий P5 —
  гарантия входа), параллельные переезды одного кластера (Д2).
- CLI-обёртка `move-bucket`-команд поверх сервиса: заявка кладётся в etcd
  напрямую (`etcdctl put`), отдельная CLI-утилита не создаётся (Д10).
- Слияние данных карантинного шарда (t05), метрики (t04), per-cluster секреты (t02).

## 3. Соответствие скрипт → C# (переносимая семантика)

| Скрипт (arch/scripts) | C#-эквивалент | Примечания |
|---|---|---|
| `move-bucket.sh move` (шаги 0–5) | `MoveProcess` фазы M0–M6 (§6.1) | подтверждение `--yes` заменяет сама заявка |
| `move-bucket.sh status` | читается из etcd напрямую (статус-ключ + work-журнал) | без отдельной команды (Д10) |
| `move-bucket.sh rollback` | заявка `op=rollback` (§6.3) | тот же cutover-блок |
| `move-bucket.sh finalize` | заявка `op=finalize` (§6.4) | + осиротевшие tablesync-слоты |
| `abort-move.sh list` | range статус-ключей (наблюдаемость — панель/etcd) | без команды |
| `abort-move.sh artifacts` | внутренний шаг abort (инвентаризация) (§6.5) | диагностика — в журнал/лог |
| `abort-move.sh abort` | заявка `op=abort` (§6.5) | `--force` → поле `force` заявки |
| `buckets-common.sh` SQL-хелперы | `MoveSql` (тексты) + `IMoveSqlExecutor` (§5) | перенос SQL 1:1 |
| `mover_conninfo` (HAProxy :5432) | conninfo из dsn-ключа источника (multi-host portalloc) + `bucket_mover` + пароль из env (Д4) | HAProxy-входа в образе узла нет (решение фазы исполнения бэкенда); multi-host libpq — эквивалент P2 |
| `pg_dump \| psql` (шаг 1) | `docker exec pg_dump --schema-only` → Npgsql (Д3) | pg_dump внутри контейнера узла |
| пароли `buckets.env` | `InstallSecrets` (env `PGW_*`, Д7 бэкенда) | без нового секрета |
| пороги `FREEZE_*`, `CUTOVER_TIMEOUT_SEC` и др. | `PgWorker:Moves` + `PgWorker:Thresholds` (§9) | значения по умолчанию — из скриптов |

## 4. Контракт etcd

### 4.1. Заявки (НОВЫЕ ключи, префикс `/pgworker/moves/`)

Префикс `/pgworker/` панелью не читается (спека бэкенда §4.3) — заявки
операционные команды оркестратору, вне снапшота панели.

```
/pgworker/moves/<C>/bucket_<i>   → JSON MoveRequest
```

```json
{
  "op": "move | rollback | finalize | abort",
  "to": "shard2",              // move: цель
  "old_shard": "shard1",       // finalize: убираемый не-владелец
  "skip_reverse": false,       // move: без обратной подписки (откат только re-copy)
  "resume": false,             // move: продолжить с ПУСТОЙ схемой на приёмнике
  "force": false,              // abort: ломает защиту свежести и routing==target
  "requested_unix": 1770000000,
  "requested_by": "operator"   // опционально (диагностика)
}
```

Жизненный цикл заявки: кладётся оператором (`etcdctl put` / будущая панель) →
`MoveProcess` (под клэймом кластера) исполняет → **успех или перманентный
валидационный отказ — заявка удаляется** (отказ фиксируется в
`/pgworker/work/<C>` `last_error` + лог); **transient-сбой — заявка остаётся**,
переезд живёт в статус-ключе бакета, следующий тик продолжает с фазы.
Отсутствие заявки + отсутствие статус-ключа = бакет ACTIVE, ничего не делать.

Одна заявка на бакет (ключ один). Порядок нескольких заявок кластера —
старейшая по `requested_unix` первой, остальные ждут (Д2).

### 4.2. Статус переезда (СУЩЕСТВУЮЩИЙ ключ, формат скриптов 1:1)

```
/clusters/<C>/buckets/status/bucket_<i>
```

Формат значения — ровно как в `move-bucket.sh`/`abort-move.sh` (совместимость
в обе стороны: след C#-переезда разбирает скрипт, и наоборот):

```json
{"bucket":"bucket_42","state":"SYNCING","owner":"shard1","target":"shard2",
 "started_unix":…,"updated_unix":…,"phase":"copy-wait"}
```

- `state`: `SYNCING | FROZEN` при переезде; `ABORTING` + план — журнал уборки
  (§6.5); нет ключа = ACTIVE.
- `phase` (значения скриптов): `ddl`, `pubsub`, `copy-wait`, `frozen`, `verify`,
  `flip`; ABORTING: `blocked`, `db-cleanup`, `drop-subscriptions`, `drop-slots`,
  `drop-publications`, `unfreeze-owner`, `sync-sequences`, `drop-schema`, `done`,
  `failed`. Расширение: `cutover-wait` (слот догоняет после FROZEN) — новое
  значение phase, читатели phase как opaque-строку толерантны.
- Читатели уже совместимы: `ClusterSnapshotParser` парсит state →
  `BucketMoveState` (SYNCING/FROZEN/ABORTING/NOT_INITIALIZED); панель
  отображает как строку; эвакуатор блокируется на этих state.

### 4.3. Журнал и снапшоты

- Work-журнал `/pgworker/work/<C>`: `op=move|rollback|finalize|abort`, phase —
  живая фаза (каждый тик), `last_error` при сбоях — существующий механизм.
- Снапшоты P12 (P12-точки переезда, как в скриптах): `move-<bucket>-start` —
  сразу после SYNCING-put, **обязателен** (сбой снапшота → переезд не
  начинается, фаза waiting-snapshot, ретрай); `flip-<bucket>-<shard>` — после
  атомарного flip, best-effort (сбой не роняет переезд, журнал + лог).

## 5. Архитектура

### 5.1. Новый проект `src/PgWorker.Moves`

Домен переездов изолируется от `PgWorker.Provisioning` (где процессы
жизненного цикла кластера). Зависимости: `PgWorker.Core`, `PgWorker.Etcd`,
`PgWorker.Docker` (только `IClusterDriver.ExecNodeAsync` — DDL-перенос),
Npgsql (уже в CPM, новой версии не добавляется). Регистрация — в `Program.cs`
по образцу существующих процессов.

| Компонент | Ответственность |
|---|---|
| `MoveRequest` (record) + `MoveRequestsStore` | чтение заявок (`range /pgworker/moves/<C>/`), выбор старейшей, удаление по завершении |
| `MoveStatus` (record) + `MoveStatusStore` | put/get/parсing статус-ключа (формат §4.2), атомарный flip-txn (compare routing → put+del) |
| `MoveProcess` | машина состояний M0–M6/rollback/finalize/abort (§6); тик идемпотентен |
| `CutoverSequence` | непрерывный блок cutover (заморозка → flip), общий для move/rollback |
| `AbortSequence` | инвентаризация артефактов, журнал ABORTING, идемпотентная уборка |
| `MoveDdl` | DDL-перенос: `pg_dump` через exec → применение на приёмнике |
| `MoveSql` | чистые функции-билдеры SQL-текстов (freeze/unfreeze, pub/sub, слоты, sequences, сверки, sync-standby, инвентарь) — перенос `buckets-common.sh` 1:1 |
| `IMoveSqlExecutor` | расширенный SQL-исполнитель: scalar/list/батч-в-транзакции-с-lock_timeout (Npgsql + Polly-ретраи `RetryPolicies`); отдельная грань от `ISqlExecutor` (существующий не меняем) |
| `ShardEndpoints` | адресация шардов: master-ключ → portalloc → host:pg-порт (вынос `ResolveMasterAsync` из `BucketEvacuator` в общий сервис, переиспользование) + построение admin-DSN (postgres) и mover-conninfo (libpq, из dsn-ключа) |
| `MoveArtifactsScanner` | инвентаризация артефактов на всех шардах (schema/pub/sub/slot по именам `pub_/sub_`-конвенций) — для abort и диагностики |

Имена артефактов переезда — конвенция скриптов (важно для совместимости со
скриптовыми остатками): `pub_<bucket>`, `sub_<bucket>` (прямые),
`pub_<bucket>_rb`, `sub_<bucket>_rb` (обратные).

### 5.2. Docker: exec для pg_dump

Расширение `IDockerEngine`: `ExecAsync(containerId, string[] cmd, ct) →
Result<string>` (POST `/containers/{id}/exec` + `/exec/{id}/start`,
демультиплексирование stdout-стрима); расширение `IClusterDriver`:
`ExecNodeAsync(cluster, shard, node, cmd, ct)` — plain: контейнер по имени
`pgw-<C>-<X>-<n>` на хосте из portalloc; swarm: сервис → `GET /tasks` →
containerId таска. Реализации — в обоих драйверах, идемпотентности не требует
(read-only утилита).

### 5.3. Интеграция в цикл

- `IClusterProcesses` расширяется: `Task<Result<ProcessOutcome>>
  ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct)`.
- `ReconcileLoop.ProcessClusterAsync`, ветка Active (default): после
  `SuperviseAsync` и эвакуаций — `RunClusterOpAsync(cluster, "moves", …)`.
  Мутации — только под живым клэймом кластера (инвариант spec бэкенда §4.3);
  кластеры в `NOT_INITIALIZED`/`TO_REMOVE` заявки не обрабатывают
  (валидационный отказ: кластер не инициализирован).
- `DeprovisioningProcess` D2: в чистку добавляется префикс
  `/pgworker/moves/<C>/` (заявки не переживают удаление кластера) — правка
  существующего списка удаляемых ключей.
- Секреты: существующий `InstallSecrets` (MoverPassword = env
  `PGW_BUCKET_MOVER_PASSWORD`); новых секретов нет.

## 6. Машины состояний

Общие правила: каждый шаг перепроверяет факт (pub/sub существуют? схема есть?
routing уже новый?) — повтор тика после сбоя безопасен; статус-ключ и
work-журнал обновляются перед каждым значимым шагом (journal-before-
manipulations); владелец — `/clusters/<C>/buckets/routing/bucket_<i>`
(отсутствие → перманентный отказ «контрол-плейн потерян, P12»). SQL — к
мастер-нодам шардов (admin-DSN postgres через `ShardEndpoints`).

### 6.1. Move (op=move): фазы M0–M6

```
M0 Валидация заявки + префлайт (все проверки — перманентный отказ:
   del заявки + work.last_error; кроме недоступности — transient)
   - кластер инициализирован (config без state), клэйм наш;
   - владелец из routing; to ≠ владелец; to зарегистрирован (dsn-ключ);
   - статус-ключ: нет → новый переезд; SYNCING/FROZEN с target=to → продолжение;
     SYNCING/FROZEN с другим target → перманентный отказ; ABORTING → отказ
     («сначала заверши abort»); started_unix наследуется при продолжении;
   - источник/приёмник SQL-доступны; схема бакета есть на источнике;
   - wal_level=logical на источнике; свободные слоты (count < max_replication_slots)
     и walsender'ы (count < max_wal_senders); warning при wal_status='lost' (P4);
   - mover-роль: SELECT 1 под bucket_mover на источнике + rolreplication;
   - P8: sync-standby у мастера приёмника (synchronous_standby_names непусто +
     pg_stat_replication sync_state IN ('sync','quorum'));
   - приёмник: схема без подписки → только при resume и ПУСТОЙ (сумма count(*)
     всех таблиц = 0), иначе перманентный отказ с подсказкой;
     остатки pub_<b>_rb/sub_<b>_rb → отказ («сначала finalize/abort»);
   → status SYNCING/ddl + снапшот move-<bucket>-start (обязателен, §4.3)
M1 DDL-перенос (если схемы на приёмнике нет):
   pg_dump --schema-only --schema=<bucket> --no-owner --no-privileges
   (exec в мастер-контейнере источника) → применение батчем на приёмнике
   (Npgsql, ON_ERROR_STOP-эквивалент: исключение → transient-отказ тика);
   гранты app-роли на приёмнике (GRANT USAGE/DML/sequences);
   P5-сверка инвентаря (relkind×relname списки источника и приёмника равны)
M2 Pub/Sub (идемпотентно): CREATE PUBLICATION pub_<b> FOR TABLES IN SCHEMA
   на источнике (если нет); CREATE SUBSCRIPTION sub_<b> на приёмнике (если нет):
   CONNECTION '<mover-conninfo источника>' PUBLICATION pub_<b>
   WITH (copy_data = true, failover = <FailoverSlots>, synchronous_commit = remote_apply)
M3 copy-wait (тик-поллинг, без общего таймаута — большой бакет копируется
   часами): pg_subscription_rel на приёмнике — готовы/всего; при недоступности
   приёмника > ConnFailBudgetSec подряд — work.last_error (журнал-алерт),
   тики продолжаются (транзиент-толерантность P8: failover приёмника не убивает
   переезд); каждый тик обновляет updated_unix
M4 Cutover — единый непрерывный блок одного тика (§6.2)
M5 Post-flip: DROP SUBSCRIPTION sub_<b> на приёмнике; при успехе и
   !skip_reverse → CREATE PUBLICATION pub_<b>_rb на приёмнике + CREATE
   SUBSCRIPTION sub_<b>_rb на источнике (copy_data = false, failover,
   remote_apply; conninfo через приёмник). Прямая срезается ДО обратной —
   иначе петля репликации (как в скрипте). Сбои M5 (например, DROP при
   недоступном источнике) НЕ отменяют состоявшийся flip: work.last_error +
   продолжение на M6 (как в референсе — «удали вручную позже»); остатки
   добирает finalize
M6 Done: снапшот flip-<bucket>-<to> (best-effort); del заявки; журнал:
   старый шард остаётся замороженным до rollback/finalize (P1-призраки)
```

### 6.2. Cutover (непрерывный блок; общий для move и rollback)

Параметры: cur (текущий владелец = источник cutover), new, слот подтверждения
(живёт на cur — создаётся подпиской приёмника/обратной), fail_state статуса
при отказе до flip. Все подшаги — точный перенос `cutover_flip`:

1. **Заморозка P1/P5** (до 3 попыток): список таблиц из pg_class (relkind
   r/p) → в ОДНОЙ транзакции: `SET LOCAL lock_timeout = 5s`;
   `REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA …FROM app`;
   `REVOKE USAGE, UPDATE ON ALL SEQUENCES … FROM app`; `REVOKE CREATE ON
   SCHEMA … FROM app`; `LOCK TABLE <все таблицы> IN ACCESS EXCLUSIVE MODE`
   (REVOKE — не барьер; барьер — LOCK; таймаут → повтор с паузой 2 с).
2. status `FROZEN/frozen`; пауза FreezeWaitSec (TTL кэша роутера).
3. `pg_current_wal_lsn()` на источнике — целевой LSN.
4. Ожидание слота: `active AND confirmed_flush_lsn >= lsn`; таймаут
   CutoverTimeoutSec → **разморозка** (GRANT-симметрия), status fail_state,
   transient-отказ (репликация продолжает догонять, заявка живёт).
5. **Sequences P6**: по всем sequence схемы — issued на источнике
   (`CASE WHEN is_called THEN last_value ELSE last_value-1 END`), next на
   приёмнике; `setval` только вперёд; отсутствие sequence на приёмнике →
   разморозка + отказ (дрейф P5). Инвариант: следующее выдаваемое у приёмника
   строго > последнего выданного на источнике.
6. **Сверка строк P8**: count(*) каждой таблицы источника/приёмника;
   расхождение → разморозка, status fail_state (`SYNCING/verify-failed` —
   ключ остаётся: репликация жива), перманентный отказ заявки с подсказкой
   «abort + повторный move» (свежий initial copy переприготовит копию;
   без abort повторный move снова упрётся в сверку — то же поведение
   референса).
7. status `FROZEN/flip`; **атомарный flip-txn**: compare
   routing=cur → put routing=new + delete status. Compare не сошёлся →
   заморозка ОСТАВЛЕНА, перманентный отказ «routing изменился под руками».

Отказ на подшагах 1–6 до flip → разморозка источника + возврат в SYNCING
(fail_state) — переезд можно повторить (репликация продолжает догонять).
После flip — только вперёд (M5/M6 или rollback позже).

Возобновление после смерти инстанца в FROZEN (owner ещё cur): повтор cutover
с начала безопасен — freeze идемпотентен (REVOKE повторно, LOCK повторно),
новый LSN захватится корректно (запись уже закрыта).

### 6.3. Rollback (op=rollback)

Требует: статус-ключа нет (ACTIVE); владелец известен; `sub_<b>_rb` найдена
ровно на одном НЕ-владельце (поиск по всем шардам). Затем — cutover
(cur=владелец, new=шард с sub_rb, слот sub_rb на cur). При отказе cutover до
flip: разморозка владельца + **удаление статус-ключа** (нет ключа = ACTIVE;
скриптовый референс писал state=ACTIVE в ключ — эквивалентная семантика без
нестандартного значения state, читатели оба трактуют как ACTIVE). После
flip: DROP SUBSCRIPTION sub_rb на вернувшемся владельце; DROP PUBLICATION
pub_rb на бывшем владельце; **разморозка** вернувшегося владельца (GRANT
DML/sequences/CREATE). Остатки на бывшем владельце (схема с данными) —
finalize. Сбои пост-шагов (DROP) — как в M5: не отменяют flip, last_error,
остатки уберёт finalize.
Отсутствие sub_rb → перманентный отказ «откат только полным re-copy» (§6
доки 11 — re-copy не портим в этой задаче, это тот же op=move после abort).

### 6.4. Finalize (op=finalize)

Требует ACTIVE и old_shard ≠ владелец. Порядок — как в референсе
(идемпотентно, каждый шаг перепроверяет существование): подписки (sub_rb на
old_shard, затем sub на владельце — они держат слоты и WAL) → публикации
(pub на old_shard, pub_rb на владельце) → осиротевшие tablesync-слоты
`sub_<b>_sync_%` на old_shard (неактивные — `pg_drop_replication_slot`,
активные — пропуск с журналом) → DROP SCHEMA <bucket> CASCADE на old_shard
(★ последним, с данными; владелец не трогается). Снапшот после. `DROP
SUBSCRIPTION` при недоступном источнике — fallback как в abort (§6.5).

### 6.5. Abort (op=abort) — порт abort-move.sh

1. **Валидация**: routing обязателен; статус-ключ обязателен (нет —
   перманентный отказ «ACTIVE, откатывать нечего; пост-flip артефакты —
   finalize»); владелец зарегистрирован. Продолжение журнала: если state
   уже ABORTING — продолжаем с записанной фазы (started_unix наследуется).
   Защита свежести: state≠ABORTING и updated_unix < now−AbortMinAgeSec и
   !force → transient-отказ (mover, возможно, ещё работает; тики повторят).
   routing==target (flip прошёл, статус завис) и !force → перманентный отказ
   с подсказкой force («abort станет доведением перевода»).
2. **Инвентаризация** артефактов на ВСЕХ шардах кластера (schema/pub/sub/slot
   по конвенции имён). Недоступный шард → журнал `ABORTING/blocked` +
   unreachable_shards, transient-отказ (повтор после возврата шарда).
3. **Журнал ДО манипуляций** (★): тот же статус-ключ, `state=ABORTING`,
   prev_state/owner/target/plan (список «шард|тип|имя»)/phase=db-cleanup.
4. **Уборка** (идемпотентно, фазы в журнал): подписки (DROP; при сбое —
   DISABLE → SET (slot_name = NONE) → DROP, слот-сирота добивается следом)
   → слоты (активный — terminate active_pid + ожидание ≤5 с; pg_drop) →
   публикации → re-GRANT на владельце (снятие P1/P5-заморозки; схемы нет —
   пропустить) → при routing==target: **доведение sequences** (setval только
   вперёд, ДО удаления старой схемы) → DROP SCHEMA на НЕ-владельцах (с
   данными; схема владельца не трогается никогда).
5. **Контрольная инвентаризация**: остатков (кроме схемы владельца) нет;
   иначе журнал failed + transient-отказ.
6. **del статус-ключа** = бакет снова ACTIVE у владельца; del заявки;
   снапшот.

## 7. Надёжность

- **Идемпотентность**: повтор тика на любой фазе безопасен (перепроверка
  факта; снапшот-точки переездов P12 см. §4.3). Именование артефактов
  детерминировано (конвенция pub_/sub_ + имя бакета).
- **Takeover**: фазы — в статус-ключе (etcd); смерть инстанса не трогает
  переезд (подписки живут в PG); следующий держатель клэйма продолжает с
  фазы. Двойной контроллер исключён клэймом кластера.
- **Транзиент-толерантность**: copy-wait и ожидание слота переживают обрывы
  (P8: failover приёмника; P3: слот при failover источника — при
  FailoverSlots=true, см. R1); ConnFailBudgetSec — порог журнала-алерта,
  не остановка.
- **Отказ etcd посреди переезда**: репликация продолжается без контрол-плейна;
  cutover (flip-txn) и статус-put ждут восстановления (transient-ретраи тиков);
  бакет живёт на владельце (P9).
- **Совместимость со скриптами**: формат статус-ключа и имена артефактов
  совпадают — скрипт может разбирать/дочищать след C#-переезда и наоборот.
  Одновременно скрипт и заявку на один бакет не запускать (у скрипта нет
  клэйма — операционная дисциплина, отражается в arch/14).

## 8. Наблюдаемость

- Статус-ключ (панель/etcd) — фаза переезда в реальном времени; work-журнал —
  op/phase/last_error; health — без изменений (цикл уже наблюдаем).
- Логи (ILogger): переходы фаз, префляйт-отказы, план abort-уборки, сверки
  (расхождения counts), flip, takeover.
- Лаг слота при copy-wait — в лог каждое изменение готовности таблиц
  (по образцу скрипта).

## 9. Конфигурация (appsettings, секция PgWorker)

```
PgWorker:Moves {
  PollIntervalSec = 2            # поллинг внутри ожиданий (copy-wait, слот)
  FreezeWaitSec = 5              # пауза после FROZEN (TTL кэша роутера)
  FreezeLockTimeoutSec = 5       # lock_timeout барьера P1
  FreezeLockTries = 3            # попытки заморозки
  AbortMinAgeSec = 120           # защита abort от живого mover
  FailoverSlots = true           # failover=true у подписок (PG17+; false для PG16-образа, см. R1)
}
PgWorker:Thresholds {
  CutoverTimeoutSec = 90         # бюджет слота на лаг 0
  ConnFailBudgetSec = 120        # бюджет недоступности шарда в ожиданиях
}
```

Новых секретов нет (MoverPassword уже в `InstallSecrets`). Новых NuGet-пакетов
нет (Npgsql/Polly уже в CPM).

## 10. Тестирование

- **Unit** (`tests/PgWorker.UnitTests/Moves/`, моки `IMoveSqlExecutor`,
  etcd-гейтвея, драйвера; AAA-комментарии по правилу проекта):
  - SQL-билдеры `MoveSql`: freeze (REVOKE×3+LOCK в одной транзакции с
    lock_timeout), unfreeze-симметрия, pub/sub (copy_data/failover/
    remote_apply, conninfo с паролем без утечки в лог), sequence-issued/next
    (CASE на стороне SQL), sync-standby, инвентарь, flip-txn (compare по
    routing);
  - машина состояний: таблицы фаз move (новый/продолжение/чужой target/
    ABORTING-блок), идемпотентность повтором тика, отказы префлайта —
    перманентные (заявка удалена) vs transient (заявка жива);
  - cutover: таймаут слота → разморозка + fail_state; сверка строк → отказ;
    flip-compare не сошёлся → заморозка оставлена;
  - abort: журнал ABORTING до манипуляций (порядок вызовов мока), blocked
    при недоступном шарде, routing==target без force — отказ, доведение
    sequences до drop schema, контрольная инвентаризация;
  - заявки: старейшая первой, del по успеху/перманентному отказу.
- **Integration** (`tests/PgWorker.IntegrationTests/`): etcd-контракт заявок
  (put → процесс-мок забирает старейшую; txn-flip конкурирует корректно);
  exec-механика драйвера (trait DockerAvailable).
- **E2E** (расширение `E2eScenarios` на стенде E2eFixture, два шарда,
  образ узла PG16 → FailoverSlots=false в тесте): полный цикл по мотивам
  `65-move-e2e.sh`: генератор записи в бакет → заявка move shard1→shard2 →
  запись жива всё время, кроме секундного FROZEN → routing=shard2, статус-
  ключа нет, sequence-инвариант, counts совпадают → призрак (сессия на
  старом шарде) получает permission denied (P1) → заявка rollback → routing
  вернулся, заморозки сняты → повторный move → finalize → артефактов нет
  (pubs/subs/слоты/схема на старом шарде). Abort-сценарий: остановка
  PgWorker посреди SYNCING → заявка abort → статус-ключ снят, артефактов
  нет, бакет ACTIVE у владельца. Deprovisioning кластера с висящей заявкой
  → `/pgworker/moves/<C>/` пуст.

## 11. Критерии приёмки (проверяемые)

1. `dotnet build src/PgWorker.slnx -c Release` — 0 warnings
   (`TreatWarningsAsErrors=true`); `dotnet test` — зелёные (unit всегда;
   integration-etcd — Testcontainers; docker/e2e — при доступном docker).
2. Unit-покрытие §10 выполнено (фазовые таблицы, идемпотентность,
   journal-before-manipulations, перманентные/transient отказы).
3. E2E-цикл §10 (move→призрак→rollback→move→finalize) проходит на стенде:
   длительность FROZEN ≤ FreezeWaitSec + несколько секунд; после finalize
   инвентаризация на старом шарде пуста (schema/pub/sub/slots).
4. Sequence-инвариант после каждого flip: следующее выдаваемое значение на
   новом владельце строго > последнего выданного на старом (SQL-проверка в
   e2e по всем sequence бакета).
5. Сверка `count(*)` всех таблиц после flip — источник/приёмник совпадают.
6. Abort e2e: после отмены — статус-ключ удалён, артефактов на всех шардах
   нет (кроме схемы владельца), запись в бакет работает у владельца.
7. Заявки: success/permanent-fail → ключа нет; transient → ключ жив,
   фаза в статус-ключе; старейшая заявка кластера обрабатывается первой
   (integration).
8. Deprovisioning кластера удаляет `/pgworker/moves/<C>/` (integration/e2e).
9. Совместимость: формат статус-ключа, имена pub_/sub_ и имена фаз — как в
   скриптах (проверка фикстурой против образцов значений из move-bucket.sh).

## 12. Deliverables в arch/ (правки в фазе исполнения до кода)

1. **`arch/14-pgworker.md`** — канон PgWorker:
   - §1/«Границы»: плановые переезды уходят из списка исключений; скрипты
     переездов — дублирующий ручной путь (не смешивать с заявками в одном
     окне переезда);
   - §3.3: новые ключи `/pgworker/moves/<C>/bucket_<i>` (формат заявки §4.1);
   - §5: новый процесс **F. MoveProcess** (фазы M0–M6, cutover, abort,
     rollback, finalize — краткая выжимка §6);
   - §3.2/Deprovisioning D2: чистка `/pgworker/moves/<C>/`;
   - §8: секция конфигурации PgWorker:Moves + пороги.
2. **`arch/11-bucket-sharding.md`** — §5 «Автоматизация»: абзац о C#-пути
   (заявки `/pgworker/moves/`, процесс PgWorker; скрипты остаются для
   стендов/кластеров вне PgWorker; формат статус-ключа общий).
3. **`arch/roadmap/pgworker.md`** — мерж-гейт: удалить пункт `t01-move-bucket-csharp`
   и зависимость `← t01-move-bucket-csharp` у t06 тем же коммитом.

## 13. Принятые решения (автономный режим)

- **Д1. Триггер переезда — декларативная заявка в etcd
  `/pgworker/moves/<C>/bucket_<i>`, а не CLI/API.** Обоснование: PgWorker —
  фоновый сервис с etcd-контрактом (панель декларирует, оркестратор
  исполняет); CLI-команда потребовала бы второго канала управления и
  аудита. Заявка = подтверждение (`--yes` скриптов). Префикс `/pgworker/`
  уже координационный, вне снапшота панели; писать может оператор
  (etcdctl) и будущая панель. Успех/перманентный отказ — удаление заявки,
  transient — фазы в статус-ключе (ретраи тиками).
- **Д2. Одна активная заявка на кластер (старейшая по requested_unix).**
  Обоснование: последовательность процессов кластера уже гарантирована
  клэймом; параллельные переезды ограничены слотами/воркерами (P4/P15) и
  бюджетом WAL — скриптовый мир тоже предполагал один mover (журнал один,
  эксклюзивности нет). Параллелизм внутри кластера — отдельное расширение.
- **Д3. DDL-перенос — `docker exec pg_dump --schema-only` внутри
  мастер-контейнера источника.** Обоснование: точная семантика pg_dump
  (референс-скрипт применяет `pg_dump | psql`; собственный генератор DDL из
  pg_catalog — большой и хрупкий: партиции, выражения, дефолты, ownership);
  pg_dump гарантированно в Spilo-образе; PgWorker уже владеет Engine API
  (exec — единственное новое расширение драйвера). Применение — Npgsql на
  приёмнике. Альтернатива «поднять HAProxy/psql-клиент в образе PgWorker»
  отвергнута: лишняя зависимость поставки.
- **Д4. Подписки ходят по multi-host DSN из dsn-ключа (portalloc-порты),
  роль bucket_mover, пароль из env.** Обоснование: HAProxy-входа в образе
  узла нет (решение фазы исполнения бэкенда — конфликт :5432 в одном
  netns); libpq multi-host сам перебирает адреса — семантический эквивалент
  P2 (подписка переживает failover источника и смерть ноды). Пароли в etcd
  не хранятся (P12/P17). Управление объектами (CREATE/DROP SUBSCRIPTION/
  PUBLICATION, freeze) — под postgres (admin-DSN), как весь SQL-слой
  PgWorker; скриптовое разграничение bucket_admin/mover — деталь
  исполнения, не контракта.
- **Д5. Тиковая машина состояний; cutover — непрерывный блок одного тика.**
  Обоснование: initial copy длится часами — часовая блокировка слота
  кластера и невидимость прогресса неприемлемы; тики (5 с) дают takeover,
  наблюдаемость и ретраи бесплатно. Cutover обязан быть неделимым (фриз —
  секунды, между заморозкой и flip нельзя вставить тик-паузу): его
  длительность ограничена FreezeWaitSec + CutoverTimeoutSec (≈95 с) —
  допустимо внутри одного тика (у ProvisioningProcess-ожидания уже есть
  прецеденты многоминутных тиков).
- **Д6. Формат статус-ключа и имена артефактов — 1:1 со скриптами.**
  Обоснование: двусторонняя совместимость (след C#-переезда дочищает
  скрипт и наоборот); читатели (панель, парсер, эвакуатор) уже толерантны.
  Новые значения phase — additive.
- **Д7. Rollback/finalize/abort — заявки того же процесса (не отдельные
  сервисы).** Обоснование: общий код cutover/уборки/журнала; разница только
  в параметризации (cur/new/слот/fail_state) и пре-пост-шагах — как в
  скриптах (cutover_flip общий).
- **Д8. Диагностические команды скриптов (status/list/artifacts) не
  портируются как команды.** Обоснование: в etcd-мире наблюдаемость уже
  есть (статус-ключ читается панелью/etcdctl; артефакты-инвентаризация —
  внутренний шаг abort, дампится в журнал/лог при исполнении). CLI-обёртка —
  roadmap, если понадобится.
- **Д9. Новый проект `src/PgWorker.Moves` (не раздувать Provisioning).**
  Обоснование: домен переездов крупный (процесс, cutover, abort, SQL-слой,
  заявки, DDL) и изолирован от жизненного цикла кластеров; соответствует
  принципу малых границ и структуре слоёв (Core/Etcd/Docker/Moves/App).
  `ShardEndpoints` (адресация мастера) выносится из BucketEvacuator в общий
  сервис — устранение дублирования, поведение эвакуатора не меняется
  (покрыто его тестами).
- **Д10. Прогнозный префлайт места (P4) не портится.** Обоснование: его нет
  и в скриптах (только warning wal_status='lost' — переносим); прогноз
  скорости×длительности — отдельная инженерия, roadmap.
- **Д11. FailoverSlots — конфиг (default true), e2e на PG16-образе идёт с
  false.** Обоснование: `failover=true` у подписки — синтаксис PG17+;
  образ узла сегодня spilo-16 (e2e-факт), дока 11 целится в PG18. Конфиг
  закрывает оба мира без форс-обновления образа в этой задаче; при false
  осознанно теряем P3-защиту (failover источника → fallback re-copy по доке
  11 §7). Обновление образа узла до PG17+ — вне скоупа (roadmap-кандидат).
- **Д12. Защита abort от живого mover — AbortMinAgeSec по updated_unix
  статус-ключа.** Обоснование: сервисный аналог скриптовой защиты; updated
  обновляется каждым тиком активного переезда — свежий статус означает
  живой процесс. `force` в заявке ломает защиту осознанно (как `--force`).

## 14. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ узла PG16: `failover=true` у подписки (PG17+) не применяется; P3-защита слотов отсутствует при false | Д11: конфиг FailoverSlots; e2e на PG16 без failover-слотов; сценарий fallback re-copy задокументирован (дока 11 §7); обновление образа — roadmap |
| R2 | `docker exec pg_dump` — зависимость DDL-шага от docker-доступности контейнера (не только SQL) | Шаг идемпотентен и ретраится тиками (transient); смена мастера между dump и apply ловится P5-сверкой инвентаря |
| R3 | Долгий cutover-блок в тике (≈FreezeWaitSec+CutoverTimeoutSec) держит слот кластера | Ограничено конфигом; параллельные кластеры не блокируются (SemaphoreSlim per-cluster); takeover между фазами безопасен (Д5) |
| R4 | Смешанная эксплуатация: скриптовый move параллельно с заявкой (у скрипта нет клэйма) | Операционная дисциплина в arch/14 (не смешивать в одном окне переезда); статус-ключ всегда показывает владельца процесса |
| R5 | Сверка count(*) больших таблиц удлиняет окно FROZEN (seq scan) | Наследие скриптового референса (дока 11 §4.6 — осознанно); при необходимости — отключаемый флаг в следующей итерации (не в скоупе) |
| R6 | etcd-restore откатывает заявки/журнал (R7 бэкенда) | Заявки идемпотентны; MoveProcess перепроверяет факт (routing/pub/sub/схемы) перед каждым шагом |
| R7 | Часовое копирование: WAL-раздувание источника сверх max_slot_wal_keep_size → инвалидация слота (P4) | Как в скриптах: preflight warning lost, наблюдаемость лага в логах; слот умирает — переезд (не шард), abort+повтор; прогноз места — Д10 |

## Дальше

Фаза plan: декомпозиция по слоям (arch-правки → Moves-модель/заявки/SQL-билдеры
→ exec/pg_dump-механика → машина move/cutover → abort/rollback/finalize →
интеграция в ReconcileLoop/deprovisioning → тесты unit/integration/e2e),
каждая задача — с тестами AAA.
