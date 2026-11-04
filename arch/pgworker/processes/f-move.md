# F. MoveProcess (плановые переезды бакетов, M0–M6)

Уровень 4. Онлайн-переезды, откаты, уборка и отмена по заявкам
`/pgworker/moves/<C>/bucket_<i>`. Функция —
[06-bucket-moves.md](../06-bucket-moves.md); механика логической
репликации и ловушки — [pg-clusters/11-buckets.md](../../pg-clusters/11-buckets.md)
§4–§5, [pg-clusters/12-bucket-pitfalls.md](../../pg-clusters/12-bucket-pitfalls.md); перечисления —
[enums.md](enums.md).

## 1. Резолв мастера шарда для SQL

Общий сервис `ShardEndpoints`; один и тот же у всех SQL-путей
(moves/эвакуация/ротация/репарация) — цепочка по убыванию доверия:

1. master-ключ — по имени ноды, затем `host:<doorman-port>` (усыновлённые:
   `node:<pg-port>`);
2. `/service/<scope>/leader` — имя лидера HA-контура → адрес ноды из
   portalloc (без Patroni-REST — работает при протухшем master-ключе в окне
   failover и на усыновлённых без REST);
3. Patroni-REST `/cluster` по нодам с `patroni≠0`.

Advertised-хост издателя для `CREATE SUBSCRIPTION` применяется только
когда приёмник — каноническая `pgw-`-нода (запись portalloc без `object`):
внешний приёмник (например, контейнер стендового кластера) видит адреса
dsn-ключа напрямую, подмена `AdvertisedPublisherHost` сломала бы
подключение.

## 2. Заявки и статус

Обрабатывается только держателем клэйма `<C>`; одновременно — старейшая
заявка кластера (по `requested_unix`). Успех или перманентный валидационный
отказ → заявка удаляется (отказ — `work.last_error` с подсказкой);
transient-сбой → заявка жива, ретраи тиками с фазы из статус-ключа.

Статус переезда — `/clusters/<C>/buckets/status/bucket_<i>`
(`SYNCING|FROZEN|ABORTING` + `phase`; нет ключа = ACTIVE); семантика и
таблица состояний — [pg-clusters/processes/enums.md](../../pg-clusters/processes/enums.md)
§3. Исполнитель один — PgWorker; скриптовых переездов в живой системе нет,
панель читает ключ как есть. Фазы — [enums.md](enums.md) §7.

## 3. Move (M0–M6)

```
M0 валидация/префлайт (routing/to≠owner/схема на источнике/
   wal_level=logical/слоты/walsender'ы/mover-роль/sync-standby приёмника P8/
   остатки _rb; отказы — перманентные, недоступность — transient) →
   статус SYNCING/ddl + снапшот move-<bucket>-start ОБЯЗАТЕЛЕН
   (сбой → фаза waiting-snapshot, ретрай)
M1 DDL-перенос (pg_dump-операция приёмника ноды-источника: воркер командует
   приёмнику /api/node/pgdump (канал
   [communications/02-channel-registry.md](../../communications/02-channel-registry.md)
   №22), тот снимает pg_dump --schema-only локально в контейнере ноды
   (локальный socket-коннект, без секретов) → применение на приёмнике →
   гранты app → сверка инвентаря P5)
M2 CREATE PUBLICATION pub_<b> на источнике + CREATE SUBSCRIPTION sub_<b> на
   приёмнике (copy_data = true, failover (PG17+, конфиг FailoverSlots),
   synchronous_commit = remote_apply — P3/P8)
M3 copy-wait (поллинг готовности таблиц, каждый тик обновляет updated_unix —
   защита abort Д12)
M4 cutover (ниже)
M5 post-flip: прямая подписка срезается, обратная pub_<b>_rb/sub_<b>_rb
   (copy_data = false) ставится при !skip_reverse — прямая ДО обратной,
   иначе петля репликации
M6 снапшот flip-<bucket>-<to> best-effort + del заявки. Старый шард
   остаётся замороженным до rollback/finalize (P1-призраки)
```

## 4. Cutover (M4)

Непрерывный блок одного тика; общий для move/rollback:

1. заморозка P1/P5 — REVOKE DML/sequences/CREATE у app-роли + барьер
   `LOCK TABLE ACCESS EXCLUSIVE` в одной транзакции с `lock_timeout`
   (REVOKE — не барьер);
2. `FROZEN/frozen` + пауза `FreezeWaitSec`;
3. `pg_current_wal_lsn` источника;
4. ожидание слота (`active AND confirmed_flush_lsn >= lsn`, бюджет
   `CutoverTimeoutSec`);
5. sequences P6 (issued источника → `setval` только вперёд на приёмнике);
6. сверка строк P8 (`count(*)` всех таблиц);
7. `FROZEN/flip` + атомарный txn-flip (compare routing=cur → put new +
   delete status).

Отказы: **transient** (freeze-failed / lsn-failed / catchup-timeout /
sequences-failed — разморозка, статус в fail_state, заявка жива, ретраи
тиками) и **permanent** (verify-failed — дефектная копия, разморозка
сделана, заявка удаляется с подсказкой «abort + повторный move»;
flip-conflict — routing изменился под руками, заморозка ОСТАВЛЕНА, разбор
вручную).

## 5. Rollback

Только из ACTIVE; по живой обратной подписке `sub_<b>_rb` (найдена ровно на
одном не-владельце) — зеркальный cutover; при отказе до flip — статус-ключ
удаляется (нет ключа = ACTIVE). Нет `sub_rb` → перманентный отказ «откат
только полным re-copy».

## 6. Finalize

Уборка не-владельца (подписки → публикации → осиротевшие tablesync-слоты P8
→ `DROP SCHEMA CASCADE` последним); `DROP SUBSCRIPTION` при недоступном
источнике — fallback (DISABLE → `SET (slot_name = NONE)` → DROP, слот-сирота
добивается следом).

## 7. Abort

Порт `abort-move.sh`: журнал `ABORTING` в статус-ключе ДО манипуляций (план
уборки, takeover продолжает фазу); защита свежести `AbortMinAgeSec` по
`updated_unix` (ломается `force`); routing==target без `force` → отказ, с
`force` — доведение перевода (sequences вперёд, затем уборка старого
шарда).

Снапшот-точки P12: `move-<bucket>-start` (после SYNCING-put, обязателен) и
`flip-<bucket>-<shard>` (после flip, best-effort). Конфигурация —
[../08-operability.md](../08-operability.md) §5 (`PgWorker:Moves` + пороги
`CutoverTimeoutSec`/`ConnFailBudgetSec`).

Отказы M0: move `to` = шард в TO_REMOVE → перманентный отказ «шард помечен к
удалению — выберите другую цель»; `to` без dsn → «шард ещё не поднят
(add-shard не завершён)»; finalize с `old_shard` без dsn → «шард удалён —
убирать нечего». Переезды ИЗ TO_REMOVE-шарда разрешены.
