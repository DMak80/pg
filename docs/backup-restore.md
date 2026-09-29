# Runbook: восстановление шарда из бэкапа (t05)

Операционное руководство по PITR-восстановлению шардов кластеров из S3-бэкапов.
Канон механики — [arch/19-backups.md](../arch/19-backups.md) §3.5/§4; API —
arch/14 §1.1. Тон — по [docs/runbook.md](runbook.md).

## 1. Сводка механики

Заявка restore — ключ `/pgworker/backups/<C>/<X>/restore/<id>` в etcd. Пишет
её API воркера (или оператор руками, тот же формат); исполняет воркер-держатель
клэйма кластера процессом `RestoreProcess` по фазам:

1. **PLANNED — валидация** (идемпотентна): шард не усыновлён (object-ноды не
   поддерживаются); выбор полного — `backup_id` заявки → новейший COMPLETED из
   etcd → новейший в S3 (DR-путь); факт целостности `full/<id>/backup_manifest`
   (частичный upload отсекается); стартовая точка WAL из статуса/`backup_label`;
   непрерывность WAL-цепочки от этой точки (дыра → FAILED с границами). Успех →
   статус **RUNNING** (фиксируются `backup_id`/`started_unix`).
2. **RUNNING — демонтаж + джоб**: WAL-агенты шарда вниз; ноды удаляются ВМЕСТЕ
   с data-volume («погиб диск»), ноды в state=REBUILDING, HA-scope Patroni
   чистится (`initialize/leader/sync`, `optime/`, `members/`; `request_*`
   остаётся). Затем ephemeral restore-джоб (`pgworker-backup`, фазы
   `downloading`→`recovering` в поле `phase`): качает полный в data-volume
   первой ноды, `pg_ctl` с `restore_command` (mc из `wal/`) накатывает WAL до
   цели (latest — конец архива; `time:<RFC3339>` — recovery_target_time).
   Бюджет наката — `Restore:RecoveryTimeoutSec` (дефолт 1800 с).
3. **REJOINING — rejoin**: первая нода поднимается на восстановленном volume,
   Patroni-проба ждёт `running` (бюджет PatroniBootSec); остальные ноды
   поднимаются чистыми (реплики догоняются `pg_basebackup`), все → RUNNING.
4. **COMPLETED — пост-обработка**: `finished_unix`/`restored_to_lsn` в статусе;
   **ключ `wal` шарда удаляется** — планировщик t02 немедленно переснимает
   полный (инвариант «поднятый шард имеет валидную цепочку или активный
   полный»), WAL-агент заводит цепочку заново. Мастер-ключ обновит сам лидер
   (lease-скрипт P11) — RestoreProcess его не пишет.

Свойства: операция **необратимая** (данные шарда уничтожаются демонтажём —
потому `confirm`); RPO = точка бэкапа/WAL-цели (data-loss = сегменты, не
успевшие в архив); S3 restore только читает; дубли заявок гасятся (txn 409 в
API, старейший исполняется; ручные дубли — permanent-FAILED «дубль заявки»);
выключенная подсистема (`PgWorker:Backups:Enabled=false`) restore не исполняет.
Поддерживается **только Mode=Plain**; усыновлённые (object) шарды — FAILED
«restore усыновлённых шардов не поддерживается».

Отмена бегущего restore не предусмотрена (осознанно, spec §1): он доводится
до COMPLETED/FAILED. Повторная заявка возможна после терминального статуса.

## 2. Команды

Заявка restore (API воркера, mTLS-грань):

```bash
# latest — конец WAL-архива
curl -sk https://<worker>/api/clusters/<C>/shards/<X>/restore \
  -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":"<X>"}'

# PITR на момент (RFC3339)
curl -sk https://<worker>/api/clusters/<C>/shards/<X>/restore \
  -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":"<X>","target_time":"2026-09-11T10:00:00Z"}'

# DR: source-override (восстановить из префикса другого шарда/кластера)
curl -sk https://<worker>/api/clusters/<C>/shards/<X>/restore \
  -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":"<X>","source_cluster":"SRC","source_shard":"shard1"}'

# конкретный полный (обычно не нужен — по умолчанию новейший COMPLETED)
-d '{"confirm":"<X>","backup_id":"20260911090000Z"}'
```

Ответы: `202` + `{"cluster","shard","restore_id","state":"PLANNED","target","source"}`
— заявка принята; `400` — confirm-мисматч / не-RFC3339 / валидация / нет тела;
`404` — кластер/шард не найден; `409` — кластер не Active / активная заявка уже
есть; `503` — etcd недоступен.

Чтение статусов (etcd, read-only):

```bash
etcdctl get --prefix /pgworker/backups/<C>/<X>/restore/   # заявки и статусы
etcdctl get /pgworker/backups/<C>/<X>/wal                  # цепочка (ACTIVE→...→нет)
etcdctl get --prefix /pgworker/backups/<C>/<X>/full/       # полные (переснятие)
etcdctl get /pgworker/work/<C>                             # журнал фаз воркера
```

Ручная постановка заявки (без API, формат канона arch/19 §4):

```bash
etcdctl put /pgworker/backups/<C>/<X>/restore/<id> \
  '{"state":"PLANNED","backup_id":"","source":"<C>/<X>","target":"latest",
    "node":"<X>a","requested_unix":1760000000,"requested_by":"operator"}'
```

`id` — `YYYYMMDDHHMMSSZ` UTC; `backup_id:""` — воркер возьмёт новейший
COMPLETED. Активный ключ тот же — проверьте отсутствие активных заявок шарда
перед ручной постановкой (API-гвард txn здесь не работает).

### Сироты бэкапов: hold / unhold / явное удаление (reliability t04)

```bash
# hold — защитить сироту до разбора (идемпотентен)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/hold -X POST \
  -H 'X-Requested-By: operator'

# unhold — снять hold (идемпотентен: нет ключа — тоже 204)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/hold -X DELETE

# заявка явного удаления — единственный путь удалить защищённую сироту
# (минует hold и автоправило; исполнит sweeper ближайшим проходом)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/delete -X POST \
  -H 'Content-Type: application/json' -d '{"confirm":"<C>/<X>"}'
```

Коды: `204`/`202` — принято; `400` — confirm-мисматч/нет тела; `404` — префикс
не в реестре / имена неканонические; `409` — запись в DELETING; `503` —
etcd-сбой или `PgWorker:Backups:Enabled=false`.

Ручной путь без API (формат канона arch/19 §4):

```bash
etcdctl put /pgworker/backups/orphan-holds/<C>/<X> \
  '{"set_unix":1760000000,"set_by":"operator"}'
etcdctl del /pgworker/backups/orphan-holds/<C>/<X>
etcdctl put /pgworker/backups/orphan-deletes/<C>/<X> \
  '{"requested_unix":1760000000,"requested_by":"operator"}'
etcdctl get --prefix /pgworker/backups/orphan-holds/   # кто под защитой
```

## 3. Сценарий 1 — полная потеря ноды

- **Умерла ОДНА нода** (контейнер жив-был, данные целы): restore НЕ нужен —
  `POST /api/ha/<C>-<X>/nodes/<node>/recreate` (Patroni rebuild с живого
  лидера; arch/14 §5 H). Restore — только для случая «данных нет нигде».
- **Умер весь шард** (алерты `provision`-семьи/`shard-no-leader`, ноды не
  поднимаются, volume утрачены):
  1. убедитесь, что полный + WAL-цепочка валидны
     (`/pgworker/backups/<C>/<X>/full/` — COMPLETED с `verify`, `wal/` жив);
  2. заявка restore latest (команда из §2);
  3. контроль: статус → COMPLETED (`restored_to_lsn` не пуст), ноды RUNNING,
     контрольные SELECT через мастер-порт шарда;
  4. новый полный снимется сам (wal-ключ сброшен) — проверьте появление нового
     `full/<id2>`.

## 4. Сценарий 2 — порча данных (PITR)

Данные испорчены оператором/багом (DROP/UPDATE без WHERE), нужно откатиться:

1. **остановите запись** в кластер (иначе порча уйдёт глубже в архив);
2. определите время порчи T_bad (логи приложения/`pg_stat`; с точностью до
   секунды) — цель берите **минутой раньше**: `target_time = T_bad - 60s`;
3. заявка restore с `target_time` (§2) — воркер восстановит состояние «на
   момент» и промотит шарды; состояние после цели (порча) накатана не будет;
4. контрольные SELECT; убедитесь, что данные на месте;
5. новый полный снимется сам; верните запись (новая временная линия — прежняя
   WAL-цепочка не продолжается, свежие заархивированные сегменты ниже по TLI
   не путаются: restore_command нового TLI берёт только свои сегменты).

## 5. Сценарий 3 — DR (новый кластер из S3)

etcd-контур кластера утрачен (или кластер удалён), S3-бакет жив:

1. пересоздайте кластер той же декларацией (`POST /api/clusters`, arch/02) и
   дождитесь Active (шарды пустые);
2. заявка restore на каждый шард с **source-override** на префикс погибшего
   кластера: `{"confirm":"<X>","source_cluster":"<C>","source_shard":"shard1"}`
   — все шарды восстанавливаются из одного консистентного префикса;
3. DR-выбор полного идёт по S3 (`full/`-каталоги, факт `backup_manifest`) —
   etcd-статусов после deprovision нет и не нужны;
4. контрольные SELECT-сверки по каждому шарду.

Источник с валидным полным удерживается автоправилом бессрочно (§5.1):
удаление возможно только явной заявкой (`orphan-deletes` с confirm, §2).

## 5.1. Судьба S3-объектов удалённого кластера

Объекты бэкапов удалённого (deprovisioned) кластера остаются в S3: воркер
не уничтожает потенциально ценные данные автоматикой (R4, arch/14). Супервизор
бэкапов (arch/19 §4) заносит осиротевшие префиксы в реестр
`/pgworker/backups/orphans` (виден в панели, алерт `backup-orphan`).

**Новая судьба (reliability t04, DR-hold):**

- **DR-источник с валидным полным** (объект `full/<id>/backup_manifest` в
  префиксе — тот же критерий, что у DR-выбора restore) **автоматикой не
  удаляется никогда** — автоправило «последнего полного»: запись реестра
  несёт `has_valid_full=true`, TTL-отбор её исключает. DR-восстановление
  (source-override, сценарий 3 выше) возможно бессрочно, торопиться некуда.
- TTL `PgWorker:Backups:Supervisor:OrphanTtlSec` чистит **только
  незащищённых** сирот (без полного и без hold; по умолчанию 7 суток; `0` —
  только алерт, авто-удаление выключено; заявки работают при любом значении).
- **hold** — защита «до разбора»: оператор помечает сироту ценной даже без
  полного (WAL-огрызки, подозрение на ценность, ожидание решения) —
  `POST /api/backups/orphans/<C>/<X>/hold` (§2). Снятие (unhold) возвращает
  сироту под общие правила.
- **Осознанное удаление защищённой сироты — только заявкой delete с confirm**
  (`POST /api/backups/orphans/<C>/<X>/delete` с `{"confirm":"<C>/<X>"}` — §2):
  единственный путь удалить защищённый DR-источник; исполняет sweeper
  ближайшим проходом.
- Накопление защищённых сирот в bucket наблюдаемо: панель (бейджи «полный:
  защита»/«hold»/«к удалению»), ключ `/pgworker/backups/storage` (занятость/
  квота) и квота-алерты t06; чистка — явной заявкой. Это осознанная плата
  «данные дороже места» (R4).

## 6. Таблица ошибок FAILED

| Причина (`error`) | Разбор | Повтор |
|---|---|---|
| `полные в <C>/<X> не найдены` | S3-префикс пуст/чужой: проверьте `mc ls <bucket>/<C>/<X>/full/`, source-override | после появления валидного полного |
| `полный <id> без backup_manifest (недокачан/бит)` | упавший на середине upload t02: префикс частичный | новой заявкой (воркер выберет другой полный) или переснятием полного |
| `full/<id>: backup_label недоступен/бит` | то же, объект `backup_label` отсутствует | аналогично |
| `дыра WAL-цепочки: ожидался <a>, найден <b>` | сегмент(ы) утрачены в S3 между a и b; накат до latest невозможен | восстановление на `target_time` до дыры; либо подтянуть утраченные сегменты |
| `recovery-бюджет исчерпан (<N> c)` | накат дольше `Restore:RecoveryTimeoutSec` (большая база/длинная цепочка) | повтор заявки (закачка заново) после увеличения `Restore:RecoveryTimeoutSec` |
| `target not reached`-семейство (exit джоба с ошибкой pg_ctl) | `target_time` вне досягаемости цепочки (раньше старта полного / позже конца архива) | заявка с target внутри [wal_start полного, конец архива] |
| `restore усыновлённых шардов не поддерживается` | object-ноды (усвоенный стендовый шард) | ручной путь (pg_basebackup от источника, docs/усыновление) |
| `cancelled-by-remove` | remove-shard демонтировал шард вместе с заявкой | заявка бессмысленна — шарда больше нет |
| `дубль заявки: активен старейший <id>` | ручная постановка при активной заявке | ничего: старейшая исполняется |
| `Patroni не поднялся за <N> с` | rejoin не сошёлся в бюджет (ресурсы/образ) | разбор логов нод, повторная заявка |
| `container-vanished`/`exit <N>` | docker-сбой/падение джоба | повторная заявка (идемпотентно, качает заново) |

Панель зажигает critical-алерт `restore-failed` по каждому FAILED живого
Active-кластера (разбор и повтор — оператор). Прогресс активной заявки — поле
`phase` (`downloading`/`recovering`) в статусе ключа.
