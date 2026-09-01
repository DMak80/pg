# 09. Диагностика и типовые проблемы

По возрастанию серьёзности: от «запрос тормозит» до «потеряли весь кластер».

---

## 0. Первая диагностика (всегда начинай отсюда)

```bash
# Кто сейчас лидер?
./scripts/find-leader.sh

# Полное состояние кластера
./scripts/patronictl.sh list
./scripts/patronictl.sh topology

# Здоровье etcd
ETCDCTL_API=3 etcdctl --endpoints=http://pg1:2379,http://pg2:2379,http://pg3:2379 endpoint health --cluster

# Patroni API каждой ноды
for h in pg1 pg2 pg3; do
  echo "=== $h ==="
  curl -s http://$h:8008/ | jq '{state,role,lag,pending_restart}'
done

# Логи
ssh pg1 'docker logs --tail 200 postgres'
ssh pg1 'docker logs --tail 200 etcd'
```

---

## 1. Нода «отстаёт» от лидера (lag > 0)

**Симптом**: в `patronictl list` у реплики `Lag in MB` растёт, или `state: stopped`.

**Причины**:
- Сетевые проблемы между pg-leader и репликой.
- Нагрузка на запись выше, чем реплика успевает применять (slow disk).
- Реплика была выключена и теперь догоняет.

**Что делать**:
```bash
# на реплике посмотреть, что делает postgres
docker exec -it postgres psql -U postgres -c "SELECT * FROM pg_stat_wal_receiver;"
docker exec -it postgres psql -U postgres -c "SELECT now()-pg_last_xact_replay_timestamp() AS lag;"
```
- Если лаг не уменьшается > 30 минут → `./scripts/patronictl.sh reinit <node>` (пересоздать
  реплику с нуля, через basebackup — часто быстрее, чем догонять).
- Проверь IOPS диска реплики (`iostat -x 1`).

---

## 2. Failover не происходит (лидер упал, новый не выбирается)

**Симптом**: упал лидер, прошло >2 минут, `find-leader.sh` пусто или показывает мёртвый хост.

**Причины**:
1. **etcd потерял кворум** (упали 2 из 3 узлов etcd). Проверь `endpoint health`. Пока
   кворума нет — Patroni не сможет провести выборы. Восстанови etcd (раздел 6).
2. **Patroni на паузе** (`patronictl pause`). Сними: `./scripts/patronictl.sh resume`.
3. **Ни одна реплика не свежая** (synchronous_mode + все реплики отстали). Patroni **не
   станет** повышать отстающую реплику в sync-режиме. Можно временно отключить:
   ```bash
   ./scripts/patronictl.sh edit-config   # → set synchronous_mode: false
   ```
   затем failover.
4. **REST API Patroni недоступен** между нодами (firewall по 8008).

**Принудительный failover** (последнее средство):
```bash
./scripts/patronictl.sh failover --candidate <имя_самой_свежей_реплики> --force
```

---

## 3. Split-brain (два лидера) — **очень редко**, но знать надо

**Симптом**: `find-leader.sh` показывает одного лидера, но на другой ноде
`SELECT pg_is_in_recovery()` тоже = `f` (то есть тоже primary).

**Причина**: разрушение DCS + ручные действия (например, кто-то запустил ноду с
`PATRONI_POSTGRESQL_BIN_DIR` и обошёл Patroni, или `pg_ctl promote` вручную).

**Что делать**:
1. **Немедленно** определи «правильного» лидера (тот, у которого данные самые свежие —
   сравни `pg_current_wal_lsn()`).
2. На «неправильном» лидере:
   ```bash
   docker compose -f /opt/postgres/docker-compose.yml stop
   # затем пересоздать как реплику через pg_rewind + reinit:
   ./scripts/patronictl.sh reinit <неправильная_нода> --force
   ```
3. **Никогда не пиши** на «неправильном» лидере — его изменения потеряются.

> Защита от split-brain в Patroni: пока DCS жив, его быть не может. Если DCS мёртв —
> Patroni **сам** уводит PG в read-only или останавливает. Если кто-то руками обошёл Patroni —
> это уже не кластер, а человеческая ошибка.

---

## 4. etcd потерял кворум (2 из 3 узлов упали)

Patroni не может проводить выборы → кластер «замораживается» (лидер либо продолжает работу,
если он сам + его Patroni видит хотя бы один etcd, либо уходит в read-only).

**Восстановление**:
1. Поднять упавшие узлы etcd:
   ```bash
   ssh pg2 'cd /opt/etcd && docker compose up -d'
   ssh pg3 'cd /opt/etcd && docker compose up -d'
   ```
2. Проверить: `etcdctl endpoint health --cluster` → все 3 healthy.
3. Patroni сам продолжит работу через ~`loop_wait` секунд.

### Если etcd-кластер разрушен полностью (нет данных)
> Это **крайний** случай. Patroni не сможет управлять кластером, но **данные PostgreSQL на
> нодах целы**.

Восстановление (если есть бэкап etcd):
```bash
# 1) стартовать etcd заново как новый кластер (INITIAL_CLUSTER_STATE=new)
# 2) восстановить snapshot:
etcdctl snapshot restore /backup/etcd.snap --data-dir=/data/etcd
```
Если бэкапа etcd нет — Patroni можно «объяснить», что кластер уже инициализирован:
см. процедуру `patroni reinit` для каждой ноды с `--force` на свежем DCS.

> Поэтому **бэкап etcd** (`etcdctl snapshot save`) — обязателен, хотя бы раз в сутки.

---

## 5. Диск переполнен на ноде PostgreSQL

**Симптом**: PG падает с `No space left on device`, репликация рвётся.

**Что делать**:
1. Очистить WAL, если они разрослись:
   ```bash
   docker exec -it postgres psql -U postgres -c "SELECT pg_switch_wal();"
   # проверь retention WALG/архивацию
   ```
2. Расширить том PGDATA (cloud volume / LVM extend).
3. Если переполнен именно WAL на лидере → увеличь `max_slot_wal_keep_size` или устрани
   «зависший» replication-слот:
   ```bash
   docker exec -it postgres psql -U postgres -c \
     "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
      FROM pg_replication_slots;"
   ```

---

## 6. Patroni видит etcd, но не становится лидером

**Симптом**: ноды есть, но лидер не выбирается, в логах Patroni:
```
failed to update leader lock
```
**Причина**: гонка при первом старте — несколько нод одновременно пытались инициализировать
кластер, остался «висящий» ключ `/initialize`.

**Fix**:
```bash
etcdctl --endpoints=... del /service/pgcluster/initialize
# затем reinit каждой ноды по очереди, начиная с одной (она станет лидером)
./scripts/patronictl.sh reinit pg1
```

---

## 7. HAProxy направляет writes не туда (или вообще никуда)

**Симптом**: `psql -h pg-lb -p 5432` висит или падает, хотя лидер есть.

**Диагностика**:
```bash
# 1) проверь health-check на каждом сервере в морде http://pg-lb:7000
# 2) curl-ни /primary напрямую:
for h in pg1 pg2 pg3; do
  echo "$h: $(curl -s -o /dev/null -w '%{http_code}' http://$h:8008/primary)"
done
```
- Если все 503 — нет лидера (см. раздел 2).
- Если 200 у лидера, но HAProxy показывает DOWN — проблема с резолвингом имён pg1/pg2/pg3
  из контейнера HAProxy (проверь `/etc/hosts` на ноде HAProxy; при `network_mode: host`
  он должен видеть имена хоста).
- После починки перечитай конфиг: `docker exec haproxy kill -HUP 1`.

---

## 8. Полезные SQL-снапшоты

```sql
-- Кто лидер в этой сессии? (true = реплика, false = primary)
SELECT pg_is_in_recovery();

-- На лидере: какие реплики подключены и насколько отстают
SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- Текущая позиция WAL
SELECT pg_current_wal_lsn();
-- На реплике:
SELECT pg_last_wal_replay_lsn();
```

---

## 9. Когда ничего не помогает — аварийный путь

1. **Сохранить данные**: snapshot дисков всех 3 нод (точка отката).
2. Снять `docker compose` со всех нод.
3. На самой свежей ноде (по WAL LSN) поднять PostgreSQL **напрямую** (без Patroni):
   ```bash
   docker run --rm -v /data/pg:/var/lib/postgresql/data \
     -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
   ```
   Это «read/write точка», с которой потом можно пересоздать кластер.
4. Пересоздать etcd-кластер, затем инициализировать Patroni с нуля на свежем DCS, указав
   эту ноду как источник (через `pg_create_restore` + `recovery` в `SPILO_CONFIGURATION`).
5. Подключить остальные ноды как реплики.

> Это **последний** путь. В 99% случаев достаточно разделов 1–7.

---

## 10. Превентивные меры (чтобы проблемы не возникали)

- ✅ **Бэкап etcd** ежедневно: `etcdctl snapshot save /backup/etcd-$(date +%F).snap`.
- ✅ **Бэкап PG** ежедневно + непрерывная архивация WAL (WAL-G/pgBackRest).
- ✅ Мониторинг `patroni_cluster_unlocked`, `etcd_server_has_leader`, лаг репликации.
- ✅ NTP на всех нодах, диски на SSD.
- ✅ Регулярные **учения** по failover (раз в месяц — уронить тестовую ноду, проверить
  поведение). Без этого любой «fail-safe» — теоретический.

---

## 11. Сироты `pgw-*`: контейнеры воркера без etcd-деклараций (домен PgWorker)

Терминология: [14-pgworker.md](14-pgworker.md). PgWorker управляет только тем,
что заявлено в etcd (`/clusters/<C>/…`); docker-объекты без живой декларации
воркер НЕ трогает (Deprovisioning D1 убирает сирот только в рамках
TO_REMOVE-кластера). Типовой источник: ручная чистка `etcdctl del --prefix
/clusters/<C>/` без остановки контейнеров, пересев чеков стенда без чистки
`/pgworker/portalloc/<C>`, экспериментальные прогоны.

### Диагностика

```bash
# Все docker-объекты воркера (включая Created/exited) на хосте:
docker ps -a --filter "name=pgw-" --format '{{.Names}}\t{{.Status}}\t{{.Ports}}'

# Живые декларации кластеров (в контуре etcd стенда):
docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 \
  get /clusters/ --prefix --keys-only | grep -v '^$' | cut -d/ -f3 | sort -u

# Сирота = имя pgw-<C>-…, где <C> нет в списке деклараций
# (или кластер есть, а state=NOT_INITIALIZED и порты расходятся — это уже
# лечит сам воркер: усыновление фактических портов, arch/14 §5 A P1).
```

### Процедура уборки

Сирота в `Created` (не стартовал — «port is already allocated» и т.п.), с
volume без данных — безопасен к удалению:

```bash
docker rm -f <имя-контейнера>        # volume pgw-…-data удалить отдельно, если он тоже сирота
docker volume rm <pgw-…-data>
```

⚠️ **Руками не трогать без приказа оператора**: сначала сверить, что это
действительно сирота (диагностика выше), а не нода живого кластера в
PROVISIONING. Контейнер running-кластера узнаваем по имени
`pgw-<C>-<X>-<n>`, где `<C>` есть в декларациях. Массовая уборка — только
списком, подготовленным диагностикой, по одной команде на имя (никаких
`docker rm $(docker ps -aq --filter name=pgw-)` по живому стенду).
