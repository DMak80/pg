# etcd: DCS кластера и контрол-плейн домена

Уровень 3. Роль внешнего etcd для PG-шардов: контур установки, ключи DCS
Patroni, адреса для нод, поведение при отказах, TLS и снапшоты. Ключи
домена `/clusters/` и координация `/pgworker/` — контракт PgWorker
([pgworker/00-etcd-contract.md](../pgworker/00-etcd-contract.md)); формат
деклараций панели —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.

## 1. Роль и границы

**etcd — внешний инфраструктурный слой, а не роль кластера.** Его
развёртывание и эксплуатация кластером не выполняются; на нодах кластера
`:2379` не слушается вообще. Компрометация ноды шарда не даёт доступа к
контрол-плейну.

Один etcd обслуживает всю установку: несколько независимых кластеров
(систем), все воркеры, панель и Patroni-ноды. Состояние каждого кластера —
под своим префиксом `/clusters/<C>/`; Patroni каждого шарда — отдельно в
`/service/<C>-<X>/`. Системы друг про друга не знают (watch своего
префикса).

Ссылка на etcd — настройка при старте кластера: адреса передаются явно,
«второго etcd» в контуре нет
([synchronization/README.md](../synchronization/README.md) S1).

## 2. Контур установки

**HA-контур контроль-плейна** — 3-узловой etcd (нечётное число узлов:
кворум 2/3, переживает потерю одного). Состав доводит **EtcdWorker** по
декларации `/etcd/config`; детали — [etcdworker/README.md](../etcdworker/README.md),
[etcdworker/00-etcd-contract.md](../etcdworker/00-etcd-contract.md).

- **Топология** — адресация по именам узлов в общей docker-сети контура
  (`etcdw-net`); мультихост — только Swarm (имена резолвятся overlay),
  plain docker — один хост. Host-network и IP хостов в peer-URL не
  используются.
- **Бутстрап** — оператор поднимает ОДИН seed-узел (`deploy/etcd`,
  `--initial-cluster-state=new`, peer `http://<NODE_NAME>:2380`); далее
  EtcdWorker доводит состав (learner + promote) и выводит seed.
- **Параметры** — `--heartbeat-interval=250`, `--election-timeout=2000`,
  `--auto-compaction-retention=1`, `--quota-backend-bytes=8GiB`, образ
  `etcd:v3.5.21`; `--initial-cluster-state=new` — только seed на первом
  старте, managed-узлы всегда `existing`.
- **Advertised-правила потребителей** — клиенты получают список ВСЕХ
  клиентских URL; единственный endpoint допустим только для
  стендов/разработки. В advertise включаются только адреса, резолвимые
  потребителями контура: внутрисетевой алиас, не существующий в их сетях,
  даёт постоянные NXDOMAIN-ретраи и ложную деградацию DCS-сессии.
- **Кворум**: 1 узел недоступен — всё работает; большинства нет — кворум
  потерян: контрол-плейн заморожен (надзор/панель/DCS), датаплейн живёт
  сам ([09](09-troubleshooting.md) §5). Потеря узла навсегда — замена
  автоматикой EtcdWorker (member remove + add learner + promote); seed и
  внешние узлы — runbook.

## 3. Ключи DCS Patroni (`/service/<scope>/`)

`scope = <C>-<X>` — глобально уникален в общем etcd
([02-topology.md](02-topology.md) §5). Ключи Patroni:

| Ключ | Содержание | Кто пишет |
|---|---|---|
| `/service/<scope>/initialize` | одноразовый флаг инициализации кластера (защита от повторного bootstrap) | Patroni первого лидера |
| `/service/<scope>/leader` | имя текущего лидера; lease с TTL | Patroni-лидер (продление каждые `loop_wait`) |
| `/service/<scope>/config` | динамический конфиг кластера: тайминги, `synchronous_mode(_strict)`, `postgresql.parameters` | Patroni; конвергенция — PgWorker ([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md) §4) |
| `/service/<scope>/members/<name>` | член кластера: conn_url, роль, состояние, timeline | Patroni каждой ноды |
| `/service/<scope>/optime/leader` | позиция WAL лидера (сравнение свежести при выборах) | Patroni-лидер |
| `/service/<scope>/failover` | заявка на принудительный failover/switchover (кандидат) | оператор или PgWorker (ускорение failover) |
| `/service/<scope>/sync` | состояние синхронной репликации (sync-standby) | Patroni |
| `/service/<scope>/status` | служебный статус кластера Patroni (optime и пр.) | Patroni |

Ключи `/service/<scope>/request_{cpu,mem,disk}` — заявки ресурсов ноды
(панель при создании шарда); их читает PgWorker и передаёт в лимиты
контейнера и PGTune ([02-topology.md](02-topology.md) §6).

Мастер-ключ домена `/clusters/<C>/shards/<X>/master` — не DCS Patroni:
его lease пишет Patroni-callback, значение и сверка —
[07-identify-master.md](07-identify-master.md).

## 4. Адреса и TLS для нод

Ноды ходят в etcd по адресам `PgWorker:Etcd:AdvertisedEndpoints` (fallback
`Endpoints`); Spilo получает их как `ETCD3_HOSTS` в формате `host:port`
**без scheme** (полный URL Patroni оборачивает в `[...]` и падает;
v2-клиент Spilo с etcd 3.5 несовместим). Lease-скрипту мастер-ключа нужен
полный URL — env `PGW_ETCD`.

| Потребитель | Источник адресов | Формат |
|---|---|---|
| Ноды (Spilo/Patroni) | `PgWorker:Etcd:AdvertisedEndpoints` | `host:port`, список |
| Lease мастер-ключа ноды | тот же источник → `PGW_ETCD` | полные URL, через запятую |
| PgWorker | `PgWorker:Etcd:Endpoints` | полные URL |
| Панель | `AdminPanel__Etcd__Endpoints__0..2` | полные URL |

Клиентский канал etcd — TLS (серты etcd-слоя); peer-канал — только между
узлами контура. TLS-модель — [13-network-security.md](13-network-security.md)
§4.

## 5. Поведение при отказах

- **Доступность etcd = доступность маршрутизации.** Пропал кворум — новые
  маршруты не выдаются, переезды и выборы невозможны; старые маршруты
  продолжают работать из кэша приложений — **fail-open** без лимита
  времени ([11](11-buckets.md) §3, [12](12-bucket-pitfalls.md) P9).
- **Живые ноды не зависят от etcd для уже открытых соединений**: PG
  продолжает обслуживать до истечения lease; после — Patroni уводит ноду в
  read-only/остановку (защита от двух мастеров).
- **Смерть мастера при лежащем etcd** кладёт запись шарда: репликам не на
  чем взять лидерский lease. Простой до восстановления etcd неизбежен при
  любой политике; fail-open не ухудшает, fail-closed не спасает.
- **Перезапущенный инстанс приложения** не может забутстрапиться (статика
  — только адреса etcd); это принятое ограничение, лечится восстановлением
  etcd.
- **Восстановление** — [etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md)
  (DR-слепки в S3), операторский путь —
  [09-troubleshooting.md](09-troubleshooting.md) §5 и
  [etcdworker/06-operability.md](../etcdworker/06-operability.md) §«Восстановление etcd из
  S3-выгрузки». Снапшот покрывает все кластеры сразу: restore возвращает
  ВСЕ системы к моменту снапшота.

## 6. Снапшоты

Регулярные слепки и DR-выгрузка — зона EtcdWorker (S3-кластер
etcd-backup): плановый интервал + детекция изменений, восстановление с
проверкой целостности — [etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md).
PgWorker снимает локальные снапшоты в точках изменений домена
(provisioning, переезды, ротации) — это маршрутизация, а не DR-слепок;
выгрузки в S3 у него нет ([pgworker/08-operability.md](../pgworker/08-operability.md)
§3).
