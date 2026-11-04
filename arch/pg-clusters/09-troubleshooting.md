# Диагностика PG-кластера

Уровень 3. Типовые отказы HA-кластера и их разбор: лаг реплики, отсутствие
failover, split-brain, отказ etcd, диск, Patroni, doorman, сироты. Команды
«кто мастер» — [07-identify-master.md](07-identify-master.md) §5; SQL-снимки —
[08-operations.md](08-operations.md) §8.

## 1. Первая диагностика

```bash
# 1) кто лидер (канон — пробы Patroni)
for hp in <host1>:<patroni1> <host2>:<patroni2> <host3>:<patroni3>; do
  echo "$hp: $(curl -s -o /dev/null -w '%{http_code}' --cacert <ca.pem> https://$hp/primary)"
done

# 2) снимок кластера и конфиг
curl -s --cacert <ca.pem> https://<host>:<patroni>/cluster | jq
curl -s --cacert <ca.pem> https://<host>:<patroni>/config  | jq

# 3) кворум etcd
etcdctl --endpoints=<...> endpoint health --cluster
etcdctl --endpoints=<...> get /service/<C>-<X>/leader

# 4) логи ноды и контейнера
docker logs --tail 200 pgw-<C>-<X>-<n>
```

Порядок разбора — от стабильного к меняемому
([17-synchronization-principles.md](../17-synchronization-principles.md) S9):
сначала DCS/кворум и факт роли, затем контейнер/нода, затем конфиг.

## 2. Реплика отстаёт (lag > 0)

Причины: сеть между лидером и репликой, медленный диск реплики, реплика
догоняет после простоя, долгие транзакции на источнике (тормозят
декодирование). Диагностика — `pg_stat_wal_receiver` на реплике,
`pg_stat_replication` на лидере, `iostat`:

```bash
# на реплике
SELECT * FROM pg_stat_wal_receiver;
SELECT now() - pg_last_xact_replay_timestamp() AS lag;
```

Лаг не уменьшается > 30 минут — пересоздать реплику с лидера (PgWorker
rebuild; ручной `reinit` — через Patroni). Если реплика нужна срочно,
проверить, что лидер здоров, — клонирование с больного лидера повторит
дефект.

## 3. Failover не происходит

Симптом: лидер мёртв, прошло больше бюджета, `/primary` ни у кого.

1. **Нет кворума etcd** — выборы невозможны; восстановить etcd (§5).
2. **Patroni на паузе** (`patronictl pause`) — снять `resume`.
3. **Нет свежей реплики** при `synchronous_mode` — Patroni не повышает
   отстающую; принудительный `failover --candidate <самая свежая>` —
   последнее средство, затем разбор.
4. **REST `:8008` недоступен между нодами** — firewall/сеть/TLS.
5. **PgWorker не ускорил failover** — проверить journal
   (`/pgworker/work/<C>`, поля `last_failover`) и свидетельство смерти
   контейнера ([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)
   §3).

## 4. Split-brain (два primary)

Симптом: `/primary` = 200 у двух нод (или `pg_is_in_recovery()` = `f` на
двух). Причина — разрушение DCS и/или ручной обход Patroni (`pg_ctl
promote`, запуск PG мимо Patroni).

1. Немедленно определить «правильного» лидера по свежести WAL
   (`pg_current_wal_lsn()`), не писать на «неправильный».
2. Остановить «неправильную» ноду (docker stop/kill через PgWorker-путь),
   пересоздать как реплику (rebuild с лидера; при необходимости
   `pg_rewind`).
3. Никогда не писать на «неправильном» лидере — его изменения потеряются.

Пока DCS жив, split-brain невозможен: нода без lease обязана уйти в
read-only/остановку. Если кто-то обошёл Patroni — это человеческая ошибка,
а не кластер.

## 5. etcd потерял кворум

Patroni не может проводить выборы: контрол-плейн и DCS заморожены, живые
ноды работают до истечения lease, маршруты приложений — fail-open
([04-etcd-dcs.md](04-etcd-dcs.md) §5).

1. Поднять упавшие узлы etcd (контур установки; не ноды кластера).
2. `endpoint health --cluster` — все healthy; Patroni продолжит через
   `loop_wait`.
3. **Полная потеря etcd-данных**: данные PostgreSQL на нодах целы;
   восстановление — [docs/runbook.md](../../docs/runbook.md)
   §«Восстановление etcd из S3-выгрузки», DR-слепки —
   [etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md). Снапшот
   общий: restore возвращает ВСЕ системы к моменту слепка; после restore —
   сверка карты бакетов (§7).
4. Без бэкапа etcd — кластер Patroni «объясняется» заново (reinit нод на
   свежем DCS); данные нод при этом сохраняются.

## 6. Диск переполнен

`No space left on device` роняет PG и рвёт репликацию.

1. Проверить `pg_wal` и слоты: разросшийся WAL — обычно слот, держащий
   сегменты ([11-buckets.md](11-buckets.md) §9).
2. `pg_switch_wal()`, затем разобрать слот: `wal_status`, `safe_wal_size`,
   `restart_lsn`; «зависший» слот — `DROP SUBSCRIPTION`/лечение переезда.
3. `max_slot_wal_keep_size` — предохранитель: при переполнении должен
   инвалидироваться слот, а не диск; если диск всё равно переполнен —
   увеличить том/пересмотреть размер.
4. Staging бэкапов/дрилла в data-volume — проверить и почистить по
   политике ([backups/09-operability.md](../backups/09-operability.md) §5).

## 7. Patroni видит etcd, но лидер не выбирается

Симптом: ноды есть, лидера нет; в логах `failed to update leader lock`.

Причина: «висящий» ключ `/service/<scope>/initialize` после гонки первого
старта или ручных экспериментов.

```bash
etcdctl --endpoints=<...> del /service/<C>-<X>/initialize
# затем reinit нод по очереди (первая станет лидером)
```

Делать только при полной уверенности, что кластер не инициализирован;
удаление `initialize` у живого кластера ломает bootstrap-логику.

## 8. Клиенты не подключаются через doorman

1. **Мастер-ключ** — `/clusters/<C>/shards/<X>/master`: есть, lease жив,
   значение `host:<doorman-port>` соответствует фактическому primary
   ([07](07-identify-master.md)).
2. **Порт doorman** — публикация из portalloc; проверить доступность
   `host:doorman-port` с клиента (firewall — [13](13-network-security.md) §2).
3. **TLS** — клиент с `sslmode=require`; серт doorman материализован агентом
   (`/run/pgw/doorman/`); при ошибке серта — пересоздание/доставка через
   PgWorker.
4. **Аутентификация** — app-пара из etcd (`app_user`/`app_password`);
   `28P01` при ETCD-FIRST-ротации — транзиент окна, не инцидент
   ([pgworker/04-security.md](../pgworker/04-security.md) §6).
5. **Пул** — `max_db_connections` исчерпан (`maxwait`): клиенты ждут
   серверное соединение; разобрать долгие транзакции/сессии.
6. **FROZEN** — бакет переезжает: записи отклоняются retryable; чтения
   работают ([11](11-buckets.md) §3).

## 9. Нода `UNREACHABLE`

- Patroni REST молчит, контейнер работает — деградация наблюдаемости
  (нагрузка хоста, транспорт, долгий bootstrap). Снос здорового контейнера
  недопустим: нода остаётся `UNREACHABLE`, rebuild — только когда контейнер
  действительно мёртв (положительное свидетельство).
- Контейнер отсутствует/не running — rebuild (надзор PgWorker).
- Усыновлённая (`object`) нода — self-healing отключён, разбор оператором
  ([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)
  §7).

## 10. Сироты `pgw-*`

PgWorker управляет только заявленным в etcd (`/clusters/<C>/…`);
docker-объекты без живой декларации он не трогает (депроvisioning убирает
сирот только в рамках TO_REMOVE-кластера). Типовой источник — ручная чистка
`etcdctl del --prefix /clusters/<C>/` без остановки контейнеров, пересев
стенда без чистки `/pgworker/portalloc/<C>`, экспериментальные прогоны.

```bash
# все docker-объекты воркера (включая Created/exited) на хосте
docker ps -a --filter "name=pgw-" --format '{{.Names}}\t{{.Status}}\t{{.Ports}}'

# живые декларации кластеров (контур etcd)
etcdctl --endpoints=<...> get /clusters/ --prefix --keys-only \
  | grep -v '^$' | cut -d/ -f3 | sort -u
```

Сирота = имя `pgw-<C>-…`, где `<C>` нет в списке деклараций. Контейнер в
`Created` (не стартовал — «port is already allocated») с volume без данных
безопасен к удалению; running-контейнер живого кластера узнаваем по имени
`pgw-<C>-<X>-<n>`, где `<C>` есть в декларациях.

```bash
docker rm -f <имя-контейнера>
docker volume rm <pgw-…-data>   # если том тоже сирота
```

⚠️ Руками не трогать без сверки: это может быть нода живого кластера в
PROVISIONING. Массовая уборка — только списком, подготовленным диагностикой,
по одной команде на имя; никаких `docker rm $(docker ps -aq --filter
name=pgw-)` по живому стенду.

## 11. Что приложить к разбору

- `/primary` по всем нодам, `/cluster`, `/config` Patroni;
- `endpoint health`, `/service/<C>-<X>/leader`, master-ключ;
- `pg_stat_replication`, `pg_replication_slots`, `pg_stat_wal_receiver`;
- логи контейнера ноды и PgWorker (`/pgworker/work/<C>` — фаза и
  `last_error`);
- факты `last_failover`/`last_rebuild` (RTO);
- метрики Patroni `:8008/metrics` и место на дисках.
