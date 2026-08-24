# 14. PgWorker: оркестратор кластеров (provisioning/deprovisioning/надзор) ★

**PgWorker** — фоновый сервис (.NET 10), который по состоянию в etcd управляет
жизненным циклом шардированных HA-кластеров PostgreSQL через docker (plain /
docker swarm). Это исполнительная сторона декларативного контракта: панель
AdminPanel **заявляет** кластер (`config.state=NOT_INITIALIZED`, контракт
панели — репозиторий AdminPanel, `arch/02-etcd-contract.md` §9) — PgWorker
**поднимает** ноды, инициализирует БД/роли/схемы бакетов и переводит кластер
в рабочее состояние; перевод панелью в `TO_REMOVE` — PgWorker аккуратно
демонтирует кластер.

Пять процессов:
1. **Provisioning** — от `NOT_INITIALIZED` до рабочего кластера (§6);
2. **Deprovisioning** — от `TO_REMOVE` до чистого etcd и удалённых контейнеров;
3. **Контроль нод** (надзор) — failover отслеживает Patroni, умершая нода
   пересобирается, снесённый руками контейнер пересоздаётся;
4. **Эвакуация бакетов** — при полной смерти шарда: аварийный перевод бакетов
   на живые шарды (без логической репликации — источник недоступен) с
   карантином вернувшегося;
5. **Переезды бакетов** (MoveProcess, §5 F) — плановые онлайн-переезды/откаты/
   уборка/отмена по заявкам в etcd (t01).

Свойства: несколько инстансов PgWorker работают одновременно (координация —
lease-клэймы в etcd, §3); смерть контролирующего инстанса не роняет процессы —
другой берёт роль на себя (takeover ≤ TTL 15 с + тик); все операции
идемпотентны; всё значимое состояние переживает смерть контроллера (etcd +
самих нодах).

Границы (что НЕ входит): CLI-обёртки заявок и панельные кнопки переездов —
roadmap (t06); ручной скриптовый путь остаётся для стендов без PgWorker — не
смешивать с заявками в одном окне переезда; балансировка по метрикам,
per-cluster секреты, TLS к Docker API/SSH-туннели, Prometheus-метрики,
управление etcd-слоем, слияние данных карантинного шарда —
[roadmap/pgworker.md](roadmap/pgworker.md).

---

## 1. Роль в системе и разделение ответственности

```
AdminPanel (UI)          PgWorker (оркестратор)              docker-хосты
─────────────            ──────────────────────              ────────────
создание кластера  ──►   /clusters/<C>/config.state=          контейнеры/
(заявка структуры)       NOT_INITIALIZED          ──читает──► сервисы узлов
удаление кластера  ──►   config.state=TO_REMOVE   ──создаёт/  pgworker-node
                         ноды, БД, схемы бакетов    удаёт ──►  (Spilo+doorman
инспекция (read-   ◄──   dsn, nodes/<n>/state,                +haproxy)
only, всё видит)         снятие status-ключей,
                         снятие state                Patroni-ноды
                                                     пишут /service/<scope>/,
                                                     callback пишет
                                                     shards/X/master (P11)
```

- **Панель** — декларатор и наблюдатель: пишет ТОЛЬКО `state=NOT_INITIALIZED`
  (создание, claim-txn — AdminPanel 02 §9.2) и `state=TO_REMOVE` (удаление,
  02 §9.4); читает всё. Контракт панели не меняется: её толерантность к
  значениям `nodes/<n>/state` и отсутствию status-ключей уже описана (02 §2.1).
- **PgWorker** — исполнитель: единственный, кто поднимает/удаляет ноды,
  пишет `dsn`, меняет `nodes/<n>/state`, снимает `status/bucket_*`
  (→ ACTIVE) и поле `state` у `config`, чистит префикс кластера при
  TO_REMOVE.
- **Patroni** (внутри нод) — единственный писатель `shards/X/master`
  (callback + lease TTL 5 с, P11); PgWorker только сверяет и корректирует по
  фактическому primary (P11 «сверяющий демон», §6 C).

Связь со скриптами: скрипты жизненного цикла [11](11-bucket-sharding.md) §4.5
(`init-cluster.sh`, `add-shard.sh`, `remove-shard.sh`) остаются **ручным
путём** для уже поднятых кластеров; PgWorker — декларативный путь для новых
(см. §4.5 указатель). Скрипты переездов (`move-bucket.sh` и др.) —
дублирующий ручной путь (формат статус-ключа общий, но не смешивать с
заявками в одном окне переезда — у скрипта нет клэйма); C#-путь — процесс
F (§5).

---

## 2. Модель развертывания

### 2.1. Образ узлы `pgworker-node` (единая единица размещения)

Нода кластера-шарда = **один контейнер/сервис** из кастомного образа:
`ghcr.io/zalando/spilo-16:3.3-p3` + `pg_doorman` (опционально, R1) +
`supervisord` + python-скрипт мастер-lease (эталон —
[stand/sidecar/rolecheck.py](stand/sidecar/rolecheck.py): `/v3/lease/grant` +
keepalive цикл 1 с, TTL 5 с). Внутри контейнера всё общается через localhost —
работает и в plain, и в swarm (у swarm нет host-network и «подов»; sidecar'ы
отдельными сервисами громоздки).

Решения фазы исполнения (дока синхронизирована с кодом):

- **HAProxy в образе не поднимается**: его write-фронтенд `:5432` конфликтует
  с PostgreSQL в одном netns (Д4 — один контейнер на ноду). Write-вход MVP —
  прямой pg-порт master-ноды (portalloc, multi-host DSN); конфиг-генератор
  (`HaproxyConfigBuilder`) остаётся в Core для отдельного фронтенд-слоя (roadmap).
- **Patroni DCS — etcd v3 API** (env `ETCD3_HOSTS`, формат `host:port` БЕЗ
  scheme; v2-клиент Spilo с etcd 3.5 несовместим). Адреса etcd для нод —
  отдельная настройка `PgWorker:Etcd:AdvertisedEndpoints` (ноды ходят в etcd
  через docker-сеть, а не через endpoint'ы самого PgWorker).
- Ноды кластера подключаются к общей docker-сети `pgw-net` (alias = имя ноды):
  Patroni-репликация по внутренним адресам (в default bridge hostname-резолва нет).
- Callback мастер-ключа — `on_start` + `on_role_change` (в `on_start` Patroni
  роль аргументами не передаёт — скрипт узнаёт её сам по `GET /primary`).

Роли внутри:

| Сервис | Слушает | Роль |
|---|---|---|
| PostgreSQL | `:5432` | подписки переездов (прямой порт master из portalloc, P2) и админка; в образе без doorman — и клиентский вход |
| Patroni (в Spilo) | REST `:8008` | управляет локальным PG; callback `on_start`/`on_role_change` → lease-put `shards/X/master` (P11); REST `/primary` потребляет PgWorker (пробы/сверка) |
| pg_doorman | `:6432` | пулер приложений, бэкенд только `127.0.0.1:5432`; единственный пул `<dbname>` (`pool_mode=transaction`, TLS `sslmode=require`, P13/P14/P17); ставится при сборке с `DOORMAN_URL` (R1) |

Наружу (хост) публикуются порты: `pg` (5432→выделенный), `patroni`
(8008→выделенный), `doorman` (6432→выделенный) — тройка из порт-аллокатора
(§2.4). Конфиги (env Spilo, doorman.ini, haproxy.cfg) генерирует PgWorker при
создании ноды (§6.5 решения в коде; параметры — §4 [11](11-bucket-sharding.md)).

Volume: `pgw-<C>-<X>-<n>-data` → `/home/postgres/pgdata` (дефолтный
PGDATA-корень Spilo; переопределение `PGROOT` ломает bootstrap —
data-каталог создаётся от root и недоступен patroni под postgres).

### 2.2. Режим Plain (docker на выделенных хостах)

- Конфиг `PgWorker:Docker:Hosts[]` — таблица хостов `{Name, Endpoint}`
  (`tcp://10.0.1.11:2375` или `unix:///var/run/docker.sock` для локального).
  Каждый хост — свой клиент Docker Engine API (per-host connection).
- PgWorker сам вычисляет placement (§2.4) и создаёт контейнеры на выбранных
  хостах: `POST /containers/create?name=pgw-<C>-<X>-<n>` → `start`.
  Restart-политика `unless-stopped` (docker сам поднимает после ребута хоста).
- Анти-аффинити: ноды одного HA-кластера (шарда) — на разных хостах, если
  `hosts >= replicas`; иначе — равномерно по least-loaded (число занятых
  слотов из `/pgworker/portalloc` + `GET /containers/json`).
- Ограничение: хост открывает Engine API по TCP в сети PgWorker
  (home-окружение; TLS/RBAC — roadmap).

### 2.3. Режим Swarm

- Конфиг `PgWorker:Docker:SwarmManager` — endpoint любого manager-узла.
  Одна нода шарда = один сервис `pgw-<C>-<X>-<n>` с `replicas=1`
  (гранулярность = нода: точечный rebuild без `--force-update` всего шарда).
- Анти-аффинити: PgWorker назначает placement через constraint на конкретную
  ноду, вычисленную из `GET /nodes` (число работающих тасков — least-loaded).
  Drifted-сервисы сверяются с планом и пересоздаются (декларативность).
- Объекты нод кластера для сверок (надзор, guard D2, сироты) — список
  сервисов `GET /services` по префиксу `pgw-<C>-` (существование сервиса ≠
  живой таск: живость проверяют Patroni-пробы, не сверка имён).
- Порты: `publish mode=host` (без ingress-балансировщика) с выделенными
  аллокатором портами на машине таска; DSN строится по факту.

### 2.4. Placement и порты

1. Для каждого шарда `X` с `replicas=R`: планировщик выбирает R хостов — все
   разные (анти-аффинити), при `hosts < R` — равномерно с минимальным числом
   совпадений («если топология позволяет — на разных, иначе на одной»).
   Факт совпадений виден оператору (план в journal).
2. Порт-аллокатор: каждой ноде — тройка портов из диапазона конфига (напр.
   15000–15999: `pg=base+i`, `patroni=+3000`, `doorman=+1500`), с проверкой
   фактической занятости (`GET /containers/json` + свои записи). Закрепление —
   `/pgworker/portalloc/<C>` (переживает rebuild: та же нода = те же порты).
3. Итог — план размещения (node → host + порты); он же вход для генерации
   конфигов (DSN multi-host по нодам шарда; HAProxy-конфиг — генератор
   остаётся в Core, в контейнере не поднимается — см. §2.1).
4. Заявки ресурсов `/service/<scope>/request_{cpu,mem}` — лимиты ноды:
   plain — `HostConfig.NanoCPUs` (cores × 10⁹) / `HostConfig.Memory` (байты;
   суффиксы панели: `K/M/G/T` десятичные, `Ki/Mi/Gi/Ti` двоичные);
   swarm — `TaskTemplate.Resources.Limits` (те же поля). Нечитаемое значение —
   без лимита (заявка — не контракт). `request_disk` примитива лимита в docker
   не имеет — игнорируется (квоты volume — roadmap).

Сам PgWorker — контейнер с примонтированным `/var/run/docker.sock` (plain на
одном хосте / swarm manager), volume под снапшоты etcd, env-секреты (§8).
Масштабирование — N реплик: координацию разбирает etcd (§3).

---

## 3. Контракт etcd

Транспорт — HTTP JSON gateway `/v3/*` (как панель, AdminPanel 02 §1): тот же
клиент + расширение lease-операциями (`/v3/lease/grant`, `/v3/lease/keepalive`,
`/v3/kv/put` с lease, txn с compare по value/mod_revision и delete в
success-ветке). **Poll, без watch** (аргументация — AdminPanel 02 §5; тик 5 с
покрывает динамику).

### 3.1. Читаемые ключи (существующая схема [11](11-bucket-sharding.md) §2)

| Ключ | Зачем |
|---|---|
| `/clusters/<C>/config` | константы (buckets=N, dbname) + `state` (NOT_INITIALIZED/TO_REMOVE/отсутствует) |
| `/clusters/<C>/shards/<X>/replicas` | плановое число нод шарда |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | состояние нод (свои же записи — проверка идемпотентности) |
| `/clusters/<C>/shards/<X>/master` | живой мастер (lease TTL 5 с) — маршрутизация SQL-операций |
| `/clusters/<C>/buckets/routing/bucket_<i>` | раскладка бакетов (какие схемы создавать на каком шарде) |
| `/clusters/<C>/buckets/status/bucket_<i>` | `NOT_INITIALIZED` — снять после создания схемы; SYNCING/FROZEN/ABORTING — незавершённый переезд (эвакуация такого бакета запрещена) |
| `/service/<scope>/…` (scope=`<C>-<X>`) | Patroni DCS: `leader`, `members/<name>`, `initialize` — подтверждение поднятия HA-кластера |
| `/service/<scope>/request_{cpu,mem,disk}` | заявки ресурсов на ноду (лимиты контейнера/сервиса) |

### 3.2. Пишемые ключи (существующая схема)

| Ключ | Когда | Значение |
|---|---|---|
| `/clusters/<C>/shards/<X>/dsn` | после поднятия нод шарда | `host=h1,h2 port=15432,15433 dbname=<C> user=bucket_admin` (multi-host, **без пароля**, P12/P17; порты — выделенные аллокатором, §2.4) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | весь жизненный цикл | таблица состояний §5 |
| `/clusters/<C>/buckets/status/bucket_<i>` | DELETE при завершении provisioning | снятие = бакет ACTIVE (семантика [11](11-bucket-sharding.md) §2, панели 02 §2.1) |
| `/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без поля `state`** (инициализирован = поле отсутствует, 02 §2.1; compare по `mod_revision`) |
| `/clusters/<C>/…` (весь префикс) | TO_REMOVE, финал | `del --prefix` |
| `/service/<C>-shard<k>/request_*` | TO_REMOVE, финал | точечные `del` (свои заявки; остальное пространство Patroni не трогаем) |
| `/service/<C>-<X>/` (весь scope) | TO_REMOVE, после удаления нод | `del --prefix` (guard: контейнеров/сервисов нет) |
| `/clusters/<C>/shards/<X>/master` | ТОЛЬКО при рассинхроне (P11-сверка) | lease-put `host:<doorman-port>` по фактическому primary из Patroni REST |
| `/pgworker/moves/<C>/` (префикс) | TO_REMOVE, финал (D2) | `del --prefix` — заявки переездов не переживают удаление кластера |

**Финальное состояние кластера после provisioning**: `config` без поля
`state` (унификация с кластерами `init-cluster.sh` — панель видит «Active»
без правок), все `status/bucket_<i>` удалены (бакеты ACTIVE), у каждого
шарда есть `dsn`, у каждой ноды `state=RUNNING`.

### 3.3. НОВЫЕ ключи координации воркеров (префикс `/pgworker/`)

Панель этот префикс не читает (её снапшот ограничен `/clusters/`, `/service/`,
`/cluster/nodes/`) — координация не видна UI и не мешает контракту.

| Ключ | Тип | Назначение |
|---|---|---|
| `/pgworker/leader` | lease TTL 15 с | глобальный лидер для singleton-задач (регулярные снапшоты P12). Value: `{"instance":"<id>","since_unix":…}`. Захват: txn `version==0` + put-with-lease; продление keepalive раз в 5 с. Умер лидер → lease истёк → любой другой захватывает. |
| `/pgworker/claims/<C>` | lease TTL 15 с | **пер-кластерный клэйм** работы: exclusivity обработки кластера одним инстансом. Value: `{"instance":"<id>","since_unix":…,"phase":…}`. Захват txn `version==0` + put-with-lease; держатель продлевает. Takeover: lease истёк → ключ исчез сам → txn другого инстанса succeeds. |
| `/pgworker/work/<C>` | обычный | журнал текущего процесса кластера (journal-before-manipulations, по образцу P7): `{"op":"provision\|deprovision\|evacuate\|rebuild","phase":"…","updated_unix":…,"instance":"<id>","last_error"?}`. Крах оставляет самодокументирующийся след; следующий инстанс продолжает с записанной фазы. |
| `/pgworker/evacuations/<C>/<X>` | обычный | журнал эвакуации шарда: `{"evacuated_unix","reason","buckets":{...старый→новый владелец...},"state":"DONE\|QUARANTINED"}` — истина для разбора после возврата шарда. |
| `/pgworker/portalloc/<C>` | обычный | закрепление выделенных портов за нодами (§2.4): `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` — переживает смерть инстанса, переиспользуется при rebuild. |
| `/pgworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика; необязательно для работы) |
| `/pgworker/moves/<C>/bucket_<i>` | обычный | заявка на плановый переезд/откат/уборку/отмену (t01): `{"op":"move\|rollback\|finalize\|abort","to":…,"old_shard":…,"skip_reverse":…,"resume":…,"force":…,"requested_unix":…,"requested_by":…}`. Успех или перманентный валидационный отказ → ключ удаляется; transient-сбой → остаётся, фазы — в статус-ключе бакета. Обрабатывается только держателем клэйма `<C>`; одновременно — старейшая заявка кластера. Deprovisioning D2 чистит `/pgworker/moves/<C>/` (префикс). |

Инварианты: любая мутация чужих данных (`/clusters/`, docker) выполняется
**только держателем клэйма** `<C>`; txn-записи в `/clusters/` сопровождаются
compare (routing=старое значение, config.mod_revision) — «применилось, а
контрол-плейн не знает» невозможно (как flip в [11](11-bucket-sharding.md) §5
шаг 4.7).

---

## 4. Секреты

Per-install, из env PgWorker (не в git, не в etcd — P12/P17):
`PGW_PG_SUPERUSER_PASSWORD`, `PGW_PG_STANDBY_PASSWORD`,
`PGW_APP_ROLE_PASSWORD`, `PGW_BUCKET_ADMIN_PASSWORD`,
`PGW_BUCKET_MOVER_PASSWORD`. Прокидываются в ноды при создании (env
контейнера). Роли — по [11](11-bucket-sharding.md) §4 (app/bucket_admin/
bucket_mover + гранты). Per-cluster генерация/ротация — roadmap.

---

## 5. Процессы (машины состояний)

**Состояния ноды** (`/clusters/<C>/shards/<X>/nodes/<n>/state`, пишет
PgWorker; панель отображает как строку — правок не требует):

| Значение | Смысл |
|---|---|
| `NOT_INITIALIZED` | панель заявила (исходное) |
| `PROVISIONING` | контейнер/сервис создаётся, ждём Patroni |
| `RUNNING` | Patroni-нода в кластере (role master/replica из `/service/<scope>/members`) |
| `REBUILDING` | пересоздание с чистого места (rebuild) |
| `UNREACHABLE` | периодически недоступна (Patroni REST молчит), ждём/разбираемся |
| `QUARANTINED` | шард эвакуирован; нода при возврате не допускается к работе |
| `REMOVING` | в процессе deprovisioning (ключ удаляется в конце) |

Классификация в цикле: `config.state=NOT_INITIALIZED` → Provisioning;
`TO_REMOVE` → Deprovisioning; иначе (инициализирован) → надзор.

### A. ProvisioningProcess (P0–P5)

Все шаги идемпотентны — перепроверяют факт (эталон `init-cluster.sh`).
Guard входа: полный набор ключей панели (config, shards/*/replicas, nodes/*,
routing всех N) — иначе journal `phase=waiting-keys`, ожидание доустойчивости
ключей (полуфабрикат не provisioning'уем).

```
P0 claim + journal(/pgworker/work/<C>, op=provision)
P1 план: placement (§2.4) для всех шард/нод; порт-аллокация; journal phase=planned
P2 на каждый шард X:
   P2.1 для каждой ноды n: создать volume + контейнер/сервис с конфигом
       (Spilo env: SCOPE=<C>-<X>, ETCD3_HOSTS=host:port (etcd v3), ttl=5/loop_wait=2 (P11),
        wal_level=logical + sync_replication_slots + max_slot_wal_keep_size
        (P3/P4), max_connections=60 и бюджет P15, callback on_role_change →
        lease-скрипт мастер-ключа; doorman: пул <dbname>, TLS require;
        haproxy: бэкенды всех Patroni-нод шарда), env-секреты (§4);
        nodes/<n>/state=PROVISIONING; при существовании (re-run) — сверить
        и пропустить
   P2.2 ждать: /service/<C>-<X>/initialize есть + leader есть + у каждой
        ноды Patroni REST отвечает (бюджет 10 мин, транзиент-толерантно);
        nodes/<n>/state=RUNNING
   P2.3 на мастере шарда (адрес из master-ключа/Patroni): создать БД <dbname>
        (если нет), роли app/bucket_admin/bucket_mover + GRANT'ы (§4 доки 11),
        если нет
   P2.4 создать схемы bucket_<i> по routing (только шардовы бакеты;
        CREATE SCHEMA IF NOT EXISTS, GRANT USAGE) — идемпотентно
   P2.5 записать shards/X/dsn (multi-host, без пароля)
P3 снять ВСЕ status/bucket_<i> (txn-пакетами ≤128 ops) — бакеты ACTIVE
P4 config: txn (compare mod_revision) → put канонического JSON без state
P5 снапшот P12; journal phase=done; кластер переходит в обычный надзор
```

Отказ на шаге: journal `last_error` + фаза; ретрай следующим тиком (бэкофф).
Гонка «панель пишет TO_REMOVE посреди provisioning»: перед фазой процесс
перечитывает config — смена state безопасно прекращает provisioning (контейнеры
подчистит deprovisioning).

### B. DeprovisioningProcess (D0–D3)

```
D0 claim + journal(op=deprovision)
D1 для каждого шарда/ноды: остановить и удалить контейнер/сервис (swarm:
   service rm), удалить volume pgw-…-data; nodes/<n>/state=REMOVING;
   идемпотентно (404 = ок); сироты-контейнеры (ключей нет, docker вернул
   имена pgw-<C>-*) — тоже удаляются
D2 удалить префикс /clusters/<C>/ (del --prefix) + точечные
   /service/<C>-shard<k>/request_* + префикс /service/<C>-<X>/
   (guard: docker-объектов не осталось) + /pgworker/moves/<C>/ +
   /pgworker/{portalloc,work,claims}/<C>*
D3 снапшот P12; успех = пустой /clusters/<C>/ + снятый клэйм; имя
   освобождается (повторное создание панели пройдёт)
```

Удаление нод до чистки etcd — порядок осознанный: «мёртвые» ключи при
сбитом D1 безвредны (кластер в TO_REMOVE), повторный тик продолжает.
Клэйм снимается явно (del + revoke lease) — не ждём TTL.

### C. NodeSupervisor (надзор, тик внутри ReconcileLoop)

- Сверка декларации с фактом: каждой плановой ноде — контейнер/сервис (по
  имени); снесённый руками пересоздаётся (декларативное самовосстановление),
  state=PROVISIONING→RUNNING.
- Patroni-REST каждой ноды (`GET /cluster`, timeout 3 с). Нода недоступна
  дольше `NodeDeadSec` (90 с, конфиг) и **не лидер** и кворум шарда жив
  (мертва максимум одна нода: живых ≥ max(1, nodes−1) — обобщение «≥2»
  фазы исполнения для 2-нодовых шардов) → **rebuild**: удалить контейнер + volume, создать
  заново (Patroni сделает pg_basebackup с лидера — эталон
  `rebuild-node.sh`), state=REBUILDING→RUNNING. Лидер недоступен → ничего:
  failover делает Patroni (P11, окно ~5–8 с); лидер-призрак станет репликой
  и обработается общим путём.
- Весь шард недоступен (все ноды молчат, master-ключ протух) дольше
  `ShardDeadSec` (300 с, конфиг) → эвакуация (D). Пороговое время трекается
  в `/pgworker/work/<C>` (поле `unreachable`).
- **MasterKeyReconciler** (P11): у каждого шарда сверить master-ключ с
  фактом (`GET /primary` по нодам): расхождение или ключа нет при живом
  primary → lease-put коррекция `host:<doorman-port>` (пишет только при
  рассинхроне — не второй регулярный писатель).

### D. BucketEvacuator (аварийная эвакуация, E0–E4)

Guard'ы перед любым действием (journal-before-manipulations, P7): шард
недоступен целиком ≥ `ShardDeadSec`; ни один бакет шарда не в
SYNCING/FROZEN/ABORTING (незавершённый переезд — блокируем, alert-журнал);
живые шарды есть; снапшот P12 «до».

```
E0 journal /pgworker/evacuations/<C>/<X> (план: bucket → целевой шард;
   цели — живые шарды, баланс round-robin; при живых=0 — ждать)
E1 на целевых шардах: CREATE SCHEMA IF NOT EXISTS bucket_<i> + GRANT'ы
   (пустые схемы — источник недоступен, копировать нечего: данные шарда
   остаются на его дисках и вернутся вместе с ним)
E2 по каждому бакету: txn (compare routing=<старый шард>) put routing=
   <новый> — владение переведено; статус-ключей нет → бакет сразу ACTIVE
E3 ноды шарда: state=QUARANTINED; контейнеры НЕ удаляются (данные на
   месте!), но при возврате REST-доступности — остановить (docker stop),
   чтобы «призраки» не писали в осиротевшие схемы (P1-логика)
E4 journal state=DONE; снапшот P12 «после»
```

Возврат шарда: PgWorker видит journal `DONE` + живой REST → останавливает
ноды, держит QUARANTINED, в journal — `state=QUARANTINED, returned_unix`.
Слияние/восстановление — ручной runbook (roadmap); PgWorker ничего не
удаляет и не запускает сам. RPO эвакуации = момент смерти шарда (фиксируется
в journal).

### F. MoveProcess (плановые переезды бакетов, M0–M6; t01)

Порт `move-bucket.sh`/`abort-move.sh` (runbook [11](11-bucket-sharding.md)
§5–§7, ловушки P1–P8 — [12](12-bucket-pitfalls.md)) в тиковый процесс по
**заявкам** `/pgworker/moves/<C>/bucket_<i>` (§3.3): `op=move|rollback|
finalize|abort`. Обрабатывается только держателем клэйма `<C>`; одновременно —
старейшая заявка кластера (по `requested_unix`). Успех или перманентный
валидационный отказ → заявка удаляется (отказ — `work.last_error` с
подсказкой); transient-сбой → заявка жива, ретраи тиками с фазы из
статус-ключа. Статус переезда — существующий `/clusters/<C>/buckets/status/
bucket_<i>` в **формате скриптов 1:1** (`SYNCING|FROZEN|ABORTING` + `phase`;
нет ключа = ACTIVE): скрипты и PgWorker взаимозаменяемы на разборе, но не
смешивать их в одном окне переезда — у скрипта нет клэйма.

- **Move (M0–M6)**: M0 валидация/префлайт (routing/to≠owner/схема на
  источнике/wal_level=logical/слоты/walsender'ы/`mover`-роль/sync-standby
  приёмника P8/остатки `_rb`; отказы — перманентные, недоступность —
  transient) → статус `SYNCING/ddl` + снапшот `move-<bucket>-start`
  **обязателен** (сбой → фаза waiting-snapshot, ретрай); M1 DDL-перенос
  (`pg_dump --schema-only` через docker exec → применение → гранты app →
  сверка инвентаря P5); M2 `CREATE PUBLICATION pub_<b>` на источнике +
  `CREATE SUBSCRIPTION sub_<b>` на приёмнике (`copy_data = true`,
  `failover` (PG17+, конфиг `FailoverSlots`), `synchronous_commit =
  remote_apply` — P3/P8); M3 copy-wait (поллинг готовности таблиц, каждый
  тик обновляет `updated_unix` — защита abort Д12); M4 cutover (ниже);
  M5 post-flip: прямая подписка срезается, обратная `pub_<b>_rb`/`sub_<b>_rb`
  (`copy_data = false`) ставится при `!skip_reverse` — прямая ДО обратной,
  иначе петля репликации; M6 снапшот `flip-<bucket>-<to>` **best-effort** +
  del заявки. Старый шард остаётся замороженным до rollback/finalize
  (P1-призраки).
- **Cutover (M4, непрерывный блок одного тика; общий для move/rollback)**:
  1) заморозка P1/P5 — REVOKE DML/sequences/CREATE у app-роли + барьер
  `LOCK TABLE ACCESS EXCLUSIVE` в одной транзакции с `lock_timeout`
  (REVOKE — не барьер); 2) `FROZEN/frozen` + пауза `FreezeWaitSec`;
  3) `pg_current_wal_lsn()` источника; 4) ожидание слота
  (`active AND confirmed_flush_lsn >= lsn`, бюджет `CutoverTimeoutSec`);
  5) sequences P6 (issued источника → `setval` только вперёд на приёмнике);
  6) сверка строк P8 (`count(*)` всех таблиц); 7) `FROZEN/flip` +
  атомарный txn-flip (compare routing=cur → put new + delete status).
  Отказы: **transient** (freeze-failed / lsn-failed / catchup-timeout /
  sequences-failed — разморозка, статус в fail_state, заявка жива, ретраи
  тиками) и **permanent** (verify-failed — дефектная копия, разморозка
  сделана, заявка удаляется с подсказкой «abort + повторный move»;
  flip-conflict — routing изменился под руками, заморозка ОСТАВЛЕНА,
  разбор вручную).
- **Rollback**: только из ACTIVE; по живой обратной подписке `sub_<b>_rb`
  (найдена ровно на одном не-владельце) — зеркальный cutover; при отказе
  до flip — статус-ключ удаляется (нет ключа = ACTIVE). Нет `sub_rb` →
  перманентный отказ «откат только полным re-copy».
- **Finalize**: уборка не-владельца (подписки → публикации → осиротевшие
  tablesync-слоты P8 → `DROP SCHEMA CASCADE` последним); `DROP
  SUBSCRIPTION` при недоступном источнике — fallback (DISABLE →
  `SET (slot_name = NONE)` → DROP, слот-сирота добивается следом).
- **Abort**: порт `abort-move.sh` — журнал `ABORTING` в статус-ключе ДО
  манипуляций (план уборки, takeover продолжает фазу); защита свежести
  `AbortMinAgeSec` по `updated_unix` (ломается `force`); routing==target
  без `force` → отказ, с `force` — доведение перевода (sequences вперёд,
  затем уборка старого шарда).

Снапшот-точки P12: `move-<bucket>-start` (после SYNCING-put, обязателен) и
`flip-<bucket>-<shard>` (после flip, best-effort). Конфигурация — §8
(`PgWorker:Moves` + пороги `CutoverTimeoutSec`/`ConnFailBudgetSec`).

---

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть? БД
  есть? схема есть? routing уже переведён?) — повтор после сбоя безопасен;
  именование объектов детерминировано (`pgw-<C>-<X>-<n>`).
- **Takeover**: состояние процессов — в etcd (journal + фазы в
  `nodes/<n>/state`, `dsn`, portalloc); смерть инстанса гасит lease-клэймы
  ≤15 с, следующий инстанс продолжает с записанной фазы. Двойной контроллер
  невозможен: операции над кластером — только под живым lease-клэймом.
- **Атомарность etcd**: flip-подобные переходы — txn с compare (routing,
  config.mod_revision, `version==0` для клэймов); «нет ключа = ACTIVE» —
  инвариант (status-ключи снимаются пакетами после создания схем).
- **Снапшоты P12**: регулярные (лидер, раз в 6 ч) + в точках изменений
  (provisioning/deprovisioning/эвакуация — до и после). Restore — внешний
  рецепт (`restore-cluster.sh`), PgWorker только снимает.
- **Ретраи**: короткие сетевые/SQL — Polly jitter-политики; долгие ожидания
  (Patroni-подъём, догоняние) — транзиент-толерантные циклы с бюджетом;
  ошибка тика → journal.last_error + продолжение со следующего тика.
- **Отказ etcd**: контрол-плейн заморожен (P9): разрушительных операций без
  свежего клэйма нет; живые ноды от него не зависят.
- **Отказ docker-хоста**: размещение не меняется (portalloc фиксирован);
  недоступный хост → нода UNREACHABLE → сценарии надзора; новые ноды —
  только на живые хосты.

---

## 7. Наблюдаемость

- **Health checks** (`/healthz`): `etcd-reachable` (все endpoints),
  `docker-hosts` (per-host ping), `loops-alive` (последний тик каждого
  цикла), `claims` (сколько держим), `snapshot-freshness` (возраст
  последнего снапшота).
- **Логи**: ключевые события — claim/takeover кластера, фазы процессов
  (с journal-фазой), rebuild ноды, эвакуация (полный план), сверка
  мастер-ключа с коррекцией.
- **Diag-ключи etcd** несут наблюдаемость для панели/оператора:
  `/pgworker/work/<C>` (живая фаза), `nodes/<n>/state`, journal эвакуаций.
- Prometheus-метрики — roadmap; MVP — health + логи.

---

## 8. Конфигурация (appsettings)

```
PgWorker:Etcd:Endpoints[]                          # http://host:2379
PgWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}],
                  SwarmManager, PortRange{From,To}, Images{Node}, EnableDoorman }
PgWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, SnapshotIntervalMin=360,
                 ErrorDelayMs=2000 }
PgWorker:Thresholds { NodeDeadSec=90, ShardDeadSec=300, PatroniBootSec=600,
                     CutoverTimeoutSec=90, ConnFailBudgetSec=120 }
PgWorker:Moves { PollIntervalSec=2, FreezeWaitSec=5, FreezeLockTimeoutSec=5,
                 FreezeLockTries=3, AbortMinAgeSec=120, FailoverSlots=true,
                 AdvertisedPublisherHost=null } # host издателя, как виден из
                 # контейнеров приёмников (single-docker-host стенды:
                 # host.docker.internal; прод — null, адреса dsn достижимы)
PgWorker:Parallelism { MaxClusters=4 }
PgWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
# секреты — env PGW_* (§4)
```

Флаг `EnableDoorman=false` (риск R1): узел без пулера — компромисс для
стенда/сборки образа без doorman (DSN-точка — pg-порт).

---

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `pgworker-node` зависит от бинарников pg_doorman (молодой проект, P16) | версия — пин; при недоступности — `EnableDoorman=false` (узел без пулера) |
| R2 | Swarm `publish mode=host` + уникальные порты — нестандартный путь; коллизии при ручных контейнерах | portalloc проверяет фактическую занятость перед созданием; коллизия → сдвиг порта + перегенерация DSN |
| R3 | Spilo callback (master-lease) — сторонний скрипт; отказ = протухший master-ключ | P11-сверка MasterKeyReconciler восстанавливает ключ по Patroni REST — двойной контур |
| R4 | Эвакуация «пустыми схемами» теряет записи умершего с момента смерти | осознанное аварийное поведение; фиксируется в journal; восстановление/слияние — runbook (roadmap) |
| R5 | Ноды одного шарда на одном хосте (hosts < replicas) — пониженная отказоустойчивость | требование «если топология позволяет»; факт отражается в плане и виден оператору |
| R6 | Гонка «панель пишет TO_REMOVE посреди provisioning» | перечитывание config перед фазой; смена state → перепланирование, контейнеры подчистит deprovisioning |
| R7 | Restore etcd из снапшота откатывает journal/клэймы | клэймы — lease (не восстанавливаются с живым lease); journal может откатиться — процессы идемпотентны, повторная фаза безопасна |

---

## Дальше

→ Возврат к [README.md](README.md). Контракт etcd кластеров — [11](11-bucket-sharding.md)
§2; риски топологии — [12-bucket-pitfalls.md](12-bucket-pitfalls.md);
сетевая модель — [13](13-network-security.md); отложенные задачи —
[roadmap/pgworker.md](roadmap/pgworker.md).
