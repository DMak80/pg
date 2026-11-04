# A. WalReceiverLoop (приёмник WAL)

Уровень 4. Автомат процесса-приёмника WAL: резолв условий, сессия стрима,
паузы, permanent-отказы. Функция — [03-wal-receiver.md](../03-wal-receiver.md);
политика/контроль — [backups/02-wal-archive.md](../../backups/02-wal-archive.md),
[backups/processes/b-wal-control.md](../../backups/processes/b-wal-control.md);
состояния — [enums.md](enums.md).

## 1. Состояния и переходы

```
starting
  → waiting_role        (нода не primary и не sync-standby)
  → waiting_password    (нет /clusters/<C>/backup_password)
  → waiting_policy      (нет policy.s3/pool/bucket)
  → waiting_slot        (слота нет/lost — воркер ensure/recreate)
  → waiting_disabled    (/pgworker/backups-disabled жив)
  → streaming           (роль есть, креды/слот/policy есть)
waiting_* → streaming   (условие выполнено; повторный резолв)
streaming → paused      (wal-ключ STOPPED или WAL_CHAIN_ERROR)
paused → streaming      (стоп-ключ снят; перерезолв)
любое → failed_permanent (несовместимый wal_segment_size, разрыв LSN
                          протокола, чужая цепочка, невалидный env)
```

`waiting_*` и `paused` — ожидание с периодическим перерезолвом, НЕ exit.
`failed_permanent` — состояние статус-ключа с причиной в `error`; лечение —
воркер/оператор (пересоздание ноды/демонтаж), перезапуск процесса причину
не лечит.

## 2. Резолв сессии

При старте/каждом цикле: роль своей ноды (Patroni REST loopback `/primary`,
`/sync`; CA — REST-CA ноды) → write-список (`write_master`/`write_replica`)
→ S3-креды/secrets пула → пароль `backup_exec` → хвост S3 read-списком →
слот. Всё — живым чтением etcd; смена policy/кредов/роли подхватывается
без пересоздания. Async-реплика/переходное окно — `waiting_role`, стрима
нет. Смена ролей (failover/promote) подхватывается самим приёмником.

## 3. Сессия стрима

`START_REPLICATION ... PHYSICAL` по слоту от явной стартовой позиции
(хвост S3, но не выше собственного `last_delivered_segment`; пустой префикс —
`restart_lsn` слота). Приём XLogData → буфер сегмента → дельта-куски каждые
`Wal:PartialIntervalSec`/4 МиБ с номером жизни цепочки из конфигурации
сессии (счётчик `wal-life` читается вместе с кредами/целями на каждом
цикле подключения; первый кусок сегмента — база-компак;
компакция раз в `Wal:PartialCompactIntervalSec` —
[03-wal-receiver.md](../03-wal-receiver.md) §2) → финальный put при закрытии.
Подтверждение слота — только после успешного put закрытого сегмента
(write/flush = конец доставленного). S3-исчерпание — стрим стоит
(backpressure), флаг `pool_exhausted` в статус-ключе, ретрай-цикл; успешный
put/инвентарь флаг снимает; граница сессии перепубликует факт.
Ошибка put, не являющаяся транспортной (ETag/md5 несовпадение) —
`failed_permanent` «foreign WAL chain»; воркер по свежему факту
инкрементирует `wal-life` (смена жизни, причина foreign-chain —
[../../backups/05-etcd-contract.md](../../backups/05-etcd-contract.md) §1).
58P01 — tli-процедура
([03-wal-receiver.md](../03-wal-receiver.md) §5), затем продолжение.

## 4. Инвентарь и `.history`

Вне hot-path при (ре)старте и далее периодом `Wal:InventoryPeriodSec`:
перечисление `.history` и закрытых сегментов `pg_wal` своей ноды и
идемпотентный put отсутствующих в `wal/` (границы — собственный прогресс;
открытый/нештатного размера сегмент не грузится; отказ — diagnostic-log,
следующий цикл повторяет). Переходы TLI вне окна стрима доезжают от любой
ноды, физически содержащей файл; нода без файла — `history_missing_tli`.

## 5. Статус-ключ

`/pgworker/backups/<C>/<X>/agent/<N>` (lease TTL 15 с): `node`, `role`
(`master|sync|none`), `state`, `last_delivered_segment/unix`,
`pool_exhausted`, `tli_jump`, `history_missing_tli`, `error`, `updated_unix`.
Ключ — факт СЕССИИ, не источник прогресса (истина — S3). Смерть процесса
гасит lease — воркер видит `absent`.
