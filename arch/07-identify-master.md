# 07. Как узнать, кто сейчас мастер ★

Это **ключевой** раздел по задаче. Покажу **все** рабочие способы определить текущего
лидера PostgreSQL в кластере Patroni — от самого надёжного к более простым. Все способы
сведены в один скрипт `scripts/find-leader.sh`.

> TL;DR: **самый надёжный** способ — спросить у DCS (etcd) или у Patroni REST API лидера.
> PostgreSQL-сессия (`SELECT pg_is_in_recovery()`) скажет только про **ту ноду, к которой
> ты подключён**, а не про кластер в целом.

---

## Способ 1. Спросить у etcd (DCS) — самый надёжный

Patroni хранит в etcd ключ `/service/<scope>/leader` — его значение = имя текущего лидера.
Это **источник правды**, потому что именно этот ключ и определяет, кто лидер.

```bash
export ETCDCTL_API=3
SCOPE=pgcluster

etcdctl --endpoints=http://pg1:2379 get /service/$SCOPE/leader
# вывод: <имя лидера>, например:
# pg1
```

Можно с любой ноды, к любому etcd-endpoint:
```bash
etcdctl --endpoints=http://pg1:2379,http://pg2:2379,http://pg3:2379 get /service/pgcluster/leader
```

> ⭐ Рекомендуется как **первый** способ, потому что:
> - не зависит от того, жив ли сейчас сам лидер (etcd-кворум жив — ответ есть);
> - атомарный single-source-of-truth;
> - легко скриптуется/мониторится.

---

## Способ 2. Patroni REST API — `/cluster` (самый информативный)

На **любой** ноде есть endpoint `/cluster`, который возвращает JSON со всем состоянием,
включая явное поле `leader`:

```bash
curl -s http://pg1:8008/cluster | jq
```
Вывод (пример):
```json
{
  "members": [
    { "name": "pg1", "host": "10.0.0.11", "port": 5432, "role": "leader",   "state": "running",   "api_url": "http://10.0.0.11:8008", "timeline": 1 },
    { "name": "pg2", "host": "10.0.0.12", "port": 5432, "role": "replica",  "state": "streaming", "api_url": "http://10.0.0.12:8008", "timeline": 1, "lag": 0 },
    { "name": "pg3", "host": "10.0.0.13", "port": 5432, "role": "replica",  "state": "streaming", "api_url": "http://10.0.0.13:8008", "timeline": 1, "lag": 0 }
  ]
}
```

Достаём лидера одной строкой:
```bash
curl -s http://pg1:8008/cluster | jq -r '.members[] | select(.role=="leader") | .name'
# → pg1
```

И сразу его адрес:
```bash
curl -s http://pg1:8008/cluster | jq -r '.members[] | select(.role=="leader") | "\(.name) \(.host)"'
# → pg1 10.0.0.11
```

> Этот endpoint можно дёргать с **любой** ноды — Patroni читает состояние из DCS, поэтому
> все три узла вернут идентичную картину.

---

## Способ 3. Опрос каждой ноды Patroni API (`/primary`)

Если не хочется парсить JSON — просто спроси каждую ноду, лидер ли она:

```bash
for h in pg1 pg2 pg3; do
  code=$(curl -s -o /dev/null -w "%{http_code}" http://$h:8008/primary)
  echo "$h: /primary -> $code"
done
```
Вывод:
```
pg1: /primary -> 200   ← это лидер
pg2: /primary -> 503
pg3: /primary -> 503
```

> `/primary` возвращает:
> - **200** — нода является лидером;
> - **503** — нода НЕ лидер (или ещё не стартовала).

Аналогично есть `/replica` и `/read-only` — для реплик и для «всего живого» соответственно
(см. таблицу в `06-deploy-haproxy.md`).

---

## Способ 4. `patronictl list`

Готовая человекочитаемая таблица:
```bash
./scripts/patronictl.sh list
```
```
+ Cluster: pgcluster (7234...) -----------+----+-----------+
| Member | Host      | Role    | State    | TL | Lag in MB |
+--------+-----------+---------+----------+----+-----------+
| pg1    | 10.0.0.11 | Leader  | running  |  1 |           |   ← лидер
| pg2    | 10.0.0.12 | Replica | streaming|  1 |         0 |
| pg3    | 10.0.0.13 | Replica | streaming|  1 |         0 |
+--------+-----------+---------+----------+----+-----------+
```

Чтобы получить **только имя лидера** программно:
```bash
./scripts/patronictl.sh list --format json \
  | jq -r '.[] | select(.Role=="Leader") | .Member'
# → pg1
```

---

## Способ 5. Через PostgreSQL: `pg_is_in_recovery()`

Самый «низкоуровневый» — спросить у самой PG. Но это скажет только про **конкретное
соединение**:

```bash
docker exec -it postgres psql -U postgres -c "SELECT pg_is_in_recovery();"
#   pg_is_in_recovery
# --------------------
#  f          ← false = мы на PRIMARY
#  t          ← true  = мы на STANDBY (реплика)
```

> Минус: чтобы узнать лидера кластера, надо перебрать ноды. И при рассинхроне Patroni ↔ PG
> ответ может быть устаревшим. **Для определения лидера лучше использовать способы 1–4.**

Где это полезно: **приложение само** может понять, куда оно попало (например, после
переподключения проверить `pg_is_in_recovery()` и в panic-режиме подняться как primary —
но в нашей схеме этого делать **не нужно**, Patroni разруливает сам).

---

## Способ 6. HAProxy stats

Раз уж HAProxy маршрутизирует write-трафик на лидера — его морда показывает, какой сервер
активен в `bk_pg_master`:
```
http://pg-lb:7000/
```
В строке backend `bk_pg_master` будет **ровно один** сервер в состоянии UP (зелёный) —
это текущий лидер. Программно:
```bash
curl -s http://pg-lb:7000/\;csv | awk -F, 'NR==1{for(i=1;i<=NF;i++)h[$i]=i}
  $1 ~ /bk_pg_master/ && $h["svname"]!~/BACKEND|FRONTEND/ && $h["status"]=="UP"{print $h["svname"]}'
```

---

## Итоговая рекомендация: единый скрипт

Используй `scripts/find-leader.sh` — он пробует способы в порядке надёжности и печатает:

```
$ ./scripts/find-leader.sh
Leader = pg1  (10.0.0.11)   [source: patroni /cluster]
```

```bash
# минимальная «сердцевина» (см. полный скрипт в scripts/find-leader.sh):
SCOPE=${SCOPE:-pgcluster}
ETCD_ENDPOINTS=${ETCD_ENDPOINTS:-http://pg1:2379,http://pg2:2379,http://pg3:2379}

# 1) DCS (самый надёжный)
leader=$(etcdctl --endpoints="$ETCD_ENDPOINTS" get /service/$SCOPE/leader | head -1)

# 2) если DCS недоступен — через Patroni /cluster на любой ноде
if [ -z "$leader" ]; then
  for h in pg1 pg2 pg3; do
    leader=$(curl -fsS http://$h:8008/cluster 2>/dev/null \
             | jq -r '.members[] | select(.role=="leader") | .name' 2>/dev/null)
    [ -n "$leader" ] && break
  done
fi

echo "Leader = $leader"
```

---

## Частые вопросы

**В: Кто становится лидером после отказа текущего?**
О: Та реплика, у которой позиция WAL наиболее свежая (Patroni сравнивает `pg_current_wal_lsn`).
При `synchronous_mode: true` — гарантированно та, что была синхронной.

**В: Может ли быть два лидера?**
О: Нет, пока жив кворум DCS. Ключ `/leader` один; без возможности обновить lease нода обязана
уйти в read-only. **Split-brain = только при полном разрушении DCS и ручных действиях.**

**В: Как быстро происходит определение нового лидера после отказа?**
О: TTL lease + loop_wait Patroni. По умолчанию ~30–60 секунд. Настраивается в
`SPILO_CONFIGURATION` → `loop_wait`, `ttl` (см. `configs/postgres/pg.env`).

**В: А если я просто сделаю `SELECT * FROM pg_stat_replication` на лидере?**
О: Это покажет подключённых реплик, но **не** скажет, кто лидер — ты и так уже на лидере,
раз этот запрос работает на запись. Для «узнать лидера по сети» — способы 1–4.

---

## Дальше
→ [08-operations.md](08-operations.md): плановая смена лидера (switchover), бэкапы, добавление ноды.
→ [09-troubleshooting.md](09-troubleshooting.md): когда лидер «завис» или split-brain.
