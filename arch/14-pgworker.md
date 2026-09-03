# 14. PgWorker: оркестратор кластеров (provisioning/deprovisioning/надзор) ★

**PgWorker** — фоновый сервис (.NET 10), который по состоянию в etcd управляет
жизненным циклом шардированных HA-кластеров PostgreSQL через docker (plain /
docker swarm). Это исполнительная сторона декларативного контракта: панель
AdminPanel **заявляет** кластер через **HTTP API воркера** (§1.1) — воркер
записывает декларацию (`config.state=NOT_INITIALIZED`, контракт панели —
`arch/adminpanel/02-etcd-contract.md` §9, перенесён из репозитория AdminPanel) — PgWorker
**поднимает** ноды, инициализирует БД/роли/схемы бакетов и переводит кластер
в рабочее состояние; перевод в `TO_REMOVE` — PgWorker аккуратно
демонтирует кластер. **Ответственность изменений etcd**: префиксы
`/clusters/`, `/pgworker/` и заявки `/service/<C>-<X>/request_*` пишет
ТОЛЬКО PgWorker (панель и сиды ходят через его API, п.1.1); панель etcd
только читает.

Девять процессов:
1. **Provisioning** — от `NOT_INITIALIZED` до рабочего кластера (§6);
2. **Deprovisioning** — от `TO_REMOVE` до чистого etcd и удалённых контейнеров;
3. **Контроль нод** (надзор) — failover отслеживает Patroni, умершая нода
   пересобирается, снесённый руками контейнер пересоздаётся;
4. **Эвакуация бакетов** — при полной смерти шарда: аварийный перевод бакетов
   на живые шарды (без логической репликации — источник недоступен) с
   карантином вернувшегося;
5. **Переезды бакетов** (MoveProcess, §5 F) — плановые онлайн-переезды/откаты/
   уборка/отмена по заявкам в etcd (t01);
6. **Add/remove шарда** (§5 G/H) — подъём/демонтаж отдельного шарда живого
   Active-кластера по декларации/маркеру от панели (t06); без автоматической
   перебалансировки бакетов;
7. **Ротация app-пароля** (AppPasswordRotator, §5 I) — смена per-cluster
   app-пароля на всех нодах кластера по заявке из etcd (панель);
8. **Усыновление кластера** (AdoptionProcess, §5 J) — Active-кластер с
   шардами без записей в portalloc («внешних» нод для воркера не существует):
   адреса восстанавливаются из HA-контура + docker-инспекции и закрепляются
   в portalloc; «не наших» объектов нет — воркер хозяин всего, что видит в
   `/clusters/`;
9. **Репарация брошенных переездов** (MoveRepairProcess, §5 K) —
   статус-ключи `SYNCING/FROZEN/ABORTING` без живого владельца (нет заявки,
   `updated_unix` постарел) воркер доводит до консистентного состояния
   (уборка/доведение/откат) — панельные алерты гаснут реальным ремонтом.

Свойства: несколько инстансов PgWorker работают одновременно (координация —
lease-клэймы в etcd, §3); смерть контролирующего инстанса не роняет процессы —
другой берёт роль на себя (takeover ≤ TTL 15 с + тик); все операции
идемпотентны; всё значимое состояние переживает смерть контроллера (etcd +
самих нодах).

Границы (что НЕ входит): панельные кнопки ЯВНЫХ переездов бакетов — roadmap
`t07-move-bucket-ui`; в t06 переезды инициируются только etcdctl'ом (заявки
`/pgworker/moves/`, t01); ручной скриптовый путь остаётся для стендов без PgWorker — не
смешивать с заявками в одном окне переезда; балансировка по метрикам,
per-cluster секреты, TLS к Docker API/SSH-туннели, Prometheus-метрики,
управление etcd-слоем, слияние данных карантинного шарда —
[roadmap/pgworker.md](roadmap/pgworker.md).

---

## 1. Роль в системе и разделение ответственности

```
AdminPanel (UI)          PgWorker (оркестратор)              docker-хосты
─────────────            ──────────────────────              ────────────
мутации (создание, ──►   HTTP API воркера (§1.1)              контейнеры/
удаление, шарды,         ──пишет──► /clusters/…/config.state=  сервисы узлов
переезды, ротация,       NOT_INITIALIZED/TO_REMOVE, …         pgworker-node
recreate, сид)           ──читает──► декларации               (Spilo+doorman
                         ──создаёт/удаёт──►                   +haproxy)
                         dsn, nodes/<n>/state,
                         снятие status-ключей,       Patroni-ноды
                         снятие state                пишут /service/<scope>/,
инспекция (read-   ◄──                                   callback пишет
only, всё видит;         ключ /pgworker/api/<id> =            shards/X/master (P11)
URL API — из etcd)       URL воркера (§1.1)
```

- **Панель** — декларатор и наблюдатель: **etcd только читает** (снапшот-тик);
  все мутации деклараций — создание кластера (claim-txn 02 §9.2), перевод в
  `TO_REMOVE` (02 §9.4), декларация add-shard (02 §9.5), маркер демонтажа
  шарда (02 §9.6), заявки переездов (02 §9.7), ротация app-пароля (02 §9.8),
  recreate ноды — отправляет в HTTP API воркера (§1.1). Контракт панели не
  меняется: её толерантность к значениям `nodes/<n>/state` и отсутствию
  status-ключей уже описана (02 §2.1).
- **PgWorker** — исполнитель: единственный, кто поднимает/удаляет ноды,
  пишет `dsn`, меняет `nodes/<n>/state`, снимает `status/bucket_*`
  (→ ACTIVE) и поле `state` у `config`, чистит префикс кластера при
  TO_REMOVE — и единственный, кто **записывает декларации** в etcd (приёмник
  мутаций панели и сида через свой API, §1.1).
- **Patroni** (внутри нод) — единственный писатель `shards/X/master`
  (callback + lease TTL 5 с, P11); PgWorker только сверяет и корректирует по
  фактическому primary (P11 «сверяющий демон», §6 C).

### 1.1. HTTP API воркера (мутации панели, сиды)

Та же HTTP-грань, что `/healthz` (порт `:8080`, Kestrel воркера). Префикс
`/api` — приёмник ВСЕХ мутаций декларативного контракта: панель (и только
она, плюс стендовые сиды) не пишет в etcd ничего — она отправляет команды
воркеру, воркер валидирует и записывает ключи в etcd сам (протоколы записи и
форматы значений — adminpanel/02 §9, без изменений; меняется исполнитель:
была панель напрямую, стал воркер). Воркер — «хозяин» префиксов `/clusters/`,
`/pgworker/` и заявок `/service/<C>-<X>/request_*`.

**Дискавери API**: ключ `/pgworker/api/<instanceId>` (lease TTL 15 с,
паттерн `instances/<id>`; §3.3) — value
`{"url":"http://<host>:<port>","instance":"<id>","since_unix":…}`. Воркер
ставит ключ сам при старте (keepalive-контур, вместе с `instances/<id>`);
URL — из `PgWorker:Api:AdvertiseUrl` (адрес, ДОСТИЖИМЫЙ клиентами API —
прежде всего панелью; docker-сети стендов — `host.docker.internal:<8080>`).
Ключ жив = инстанс жив и URL валиден (lease гасит мёртвые). Панель читает
ключи refresher-тиком, кеширует в снапшоте и при мутации зовёт любой живой
(при ошибке соединения — следующий; все умерли — 503 + critical-алерт
`worker-api-unreachable`, arch/adminpanel/03 §4.1).

**Аутентификация**: заголовок `X-Api-Key`, сверка с env-секуретом
`PGW_API_KEY` (пуст/не задан — проверка отключена: доверенная закрытая
docker-сеть; прод-профиль задаёт ключ). Ключи доступа в etcd секрета НЕ
содержат. TLS — roadmap (t03-docker-tls-ssh).

**Эндпоинты** (сигнатуры/коды — 1:1 UI-контракт панели 02 §9/03 §1; тело
и ответы не менялись):

| Метод+путь | Назначение | Протокол записи |
|---|---|---|
| `POST /api/clusters` | создание кластера (декларация) | 02 §9.1–§9.3: claim-txn + пакет PUT + компенсация |
| `DELETE /api/clusters/{c}` | перевод в `TO_REMOVE` | 02 §9.4 |
| `POST /api/clusters/{c}/shards` | декларация add-shard | 02 §9.5 |
| `DELETE /api/clusters/{c}/shards/{x}` | маркер демонтажа шарда | 02 §9.6 |
| `POST /api/clusters/{c}/moves` | заявки на переезды бакетов | 02 §9.7.1 |
| `POST /api/clusters/{c}/moves/rollback` | заявки на откат бакетов | 02 §9.7.2 |
| `POST /api/clusters/{c}/moves/finalize` | заявка уборки старого шарда | 02 §9.7.3 |
| `POST /api/clusters/{c}/moves/abort` | заявка отмены переезда | 02 §9.7.4 |
| `DELETE /api/clusters/{c}/moves/{bucket}` | отмена стоящей заявки (del ключа) | 02 §9.7.5 |
| `POST /api/clusters/{c}/app-password/rotate` | заявка ротации app-пароля | 02 §9.8 |
| `POST /api/ha/{scope}/nodes/{node}/recreate` | маркеры `TO_RECREATE`+`recreate=soft\|hard` | как §9.6-подобный маркер (02 §9, 03 §2): guards по `/service/<scope>/members` |
| `POST /api/seed/demo` | стендовый демо-сид pg-контура | §1.1.1 |

Guard'ы и валидации переносятся из панельных команд как есть; источником
данных вместо панельного снапшота — прямые чтения etcd воркером (config,
routing, `/service/<scope>/members`, очереди заявок): это устраняет гонку
«снапшок отстал» — воркер читает авторитетно перед записью.

### 1.1.1. Сид-эндпоинт (стендовый, demo)

`POST /api/seed/demo` — наливка ДЕМО-контроль-плейна pg-контура (кластер
`demo`: config, 2 шарда dsn/replicas/master, routing 16 бакетов, статусы
переездов bucket_3/7/11, heal, заявка move bucket_13, `/service/demo-s{1,2}/*`,
`/cluster/nodes/*`; времена — динамические от now, набор — 1:1 сид-фикстуры
интеграционных тестов панели). Пишет ключи, которые в живой системе пишут
ДРУГИЕ субъекты (Patroni, эмуляторы, скрипты): это осознанная стендовая
эмуляция, поэтому эндпоинт закрыт флагом `PgWorker:Api:EnableSeedEndpoint`
(default `false`; включается только в dev-стенде env-ом). Идемпотентен:
существующий `/clusters/demo/config` → 200 `{"seeded":false}` без записи.
Служит «стендом части» для панели без живых PG-нод; полный стенд
(00-up.sh) пользуется тем же эндпоинтом.

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
   фактической занятости: `GET /containers/json` (живые публикации docker) +
   записи portalloc ВСЕХ кластеров (`/pgworker/portalloc/*`, кроме своего —
   свой переиспользуется как закрепление; закрывает кросс-кластерную
   коллизию: без этого второй кластер получает порты первого, чьи контейнеры
   ещё не созданы, а portalloc уже записан). Закрепление —
   `/pgworker/portalloc/<C>` (переживает rebuild: та же нода = те же порты).
   Запись ноды: `{"host","pg","patroni","doorman"}` + опциональное `"object"`
   (§5 J): имя фактического docker-объекта **усыновлённой** ноды (контейнер
   внешнего происхождения, не `pgw-<C>-<X>-<n>`); отсутствие `object` =
   каноническая нода нашего провижининга. У усыновлённой ноды `patroni`/`doorman`
   могут быть `0` (внешние ноды без Patroni-REST/doorman в этом контейнере —
   Patroni-REST может жить сайдкаром, живость такой ноды — SQL-проба, §5 C).
   **Глобальный portalloc-клэйм** (t90): довыделение новых троек (недобор
   нод, а не переиспользование закреплений) — глобально взаимоисключающая
   секция «чтение занятости (docker ∪ portalloc соседей) → выбор троек →
   запись `/pgworker/portalloc/<C>`»: выполняется только держателем клэйма
   `/pgworker/locks/portalloc` (§3.3; txn `version==0` + put-with-lease TTL
   15 с, паттерн `/pgworker/leader`). Без клэйма два параллельно сеемых
   кластера (пер-кластерные клэймы друг друга не видят, §3.3) читают
   префикс ДО первой записи соседа — общей картины занятости нет, оба
   выбирают одинаковые порты (воспроизведено 2026-08-25: контейнеры второго
   кластера в Created с «port is already allocated»). Не взял клэйм —
   процесс возвращает InProgress (следующий тик ~5 с повторяет); смерть
   держателя гасит lease ≤15 с — takeover без оператора. Полностью
   закреплённый portalloc (rebuild, ранний выход без записи) клэйма не
   требует. Касается всех точек довыделения: provision P1, add-shard,
   adoption-реплан (§5 A/G/J).
3. Итог — план размещения (node → host + порты); он же вход для генерации
   конфигов (DSN multi-host по нодам шарда; HAProxy-конфиг — генератор
   остаётся в Core, в контейнере не поднимается — см. §2.1).
4. Заявки ресурсов `/service/<scope>/request_{cpu,mem}` — лимиты ноды:
   plain — `HostConfig.NanoCPUs` (cores × 10⁹) / `HostConfig.Memory` (байты;
   суффиксы панели: `K/M/G/T` десятичные, `Ki/Mi/Gi/Ti` двоичные);
   swarm — `TaskTemplate.Resources.Limits` (те же поля). Нечитаемое значение —
   без лимита (заявка — не контракт). `request_disk` примитива лимита в docker
   не имеет — игнорируется (квоты volume — roadmap).
5. **Advertised-имя хоста** (`PgWorker:Docker:AdvertisedHost`, advertised-правило
   arch/16, прецедент KafkaWorker:AdvertisedClientHost): адреса нод в etcd
   (portalloc/dsn) обязаны быть резолвимы КЛИЕНТАМИ записей — панелью. Внутреннее
   имя docker-хоста (напр. `local`) резолвится только контейнерами воркеров
   (`extra_hosts local:host-gateway`) — панельные пробы уходили в DNS-таймаут.
   Когда задано (single-host/tunnel-развёртывания, стенд —
   `host.docker.internal`; валидация старта: Plain + ровно один хост в Hosts),
   всё, что драйвер отдаёт наружу — плановые хосты, busy-кортежи, факты
   инспекции КАНОНИЧЕСКИХ нод (pgw-<C>-*) — несёт advertised-имя: один
   namespace адресов с записями portalloc. Внешние находки усыновления
   (object) advertised не получают (адресация операторская, R9-симметрия).
   AD2'-инвариант сам мигрирует легаси-записи: факт инспекции (advertised) ≠
   запись (внутреннее имя) → portalloc/dsn переписаны тиком (миграция касается
   и doorman-порта: при `EnableDoorman=false` нода не публикует пулер — факт
   инспекции даёт 0, portalloc-запись получает `doorman:0`). Мастер-ключ
   `host:<doorman>` пишут lease-демоны нод с env-хостом КОНТЕЙНЕРА — хост-часть
   может расходиться с portalloc (контейнеры, созданные до advertised-режима):
   сверка P11 и резолв мастера (§5 F) считают ключ корректным по doorman-порту
   (уникален per-node), хост-часть информативна — войны писателей нет.
   Уникальность doorman-порта — только при `EnableDoorman=true`; при
   `EnableDoorman=false` мастер-ключ вырождается в `host:0` (одинаков для всех
   нод single-host) и НЕдискриминантен по ноде: его смысл — «на шарде есть
   живой primary, ключ держится lease'ом», а не «какая нода мастер». Факт
   смены мастера потребители читают из проб Patroni (`/primary` по
   patroni-портам portalloc) или `pg_is_in_recovery`, а не из сравнения
   значений ключа.

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
| `/clusters/<C>/shards/<X>/state` | маркер демонтажа шарда `TO_REMOVE` (пишет ТОЛЬКО панель; отсутствие = обычный шард; t06) |
| `/clusters/<C>/app_user` | per-cluster логин приложения; значение `"app"`; пишет ТОЛЬКО PgWorker (P1.5 ensure, txn put-if-absent); читают PgWorker (роли) и приложение |
| `/clusters/<C>/app_password` | per-cluster пароль приложения; строка 32 симв `[A-Za-z0-9]`; те же писатель/читатели; удаляется с префиксом кластера (D2); ротация — процесс I по заявке `/pgworker/rotations/<C>` (§3.3) |
| `/clusters/<C>/shards/<X>/nodes/<n>/app_params` | per-node серверные параметры подключения (libpq `keyword=value` через пробел, дефолт `sslmode=require` из `PgWorker:AppParams:Default`); пишет ТОЛЬКО PgWorker put-if-absent (существующее значение — ручные правки оператора — НЕ перезаписывается); ensure в provisioning P2.5'/AddShard A5 и миграционно в надзоре C; читает приложение (concat к DSN, [11](11-bucket-sharding.md) §3); панель не читает |

### 3.2. Пишемые ключи (существующая схема)

| Ключ | Когда | Значение |
|---|---|---|
| `/clusters/<C>/shards/<X>/dsn` | после поднятия нод шарда | `host=h1,h2 port=15432,15433 dbname=<C> user=<bucket_admin> password=<per-cluster bucket_admin>` (multi-host; креды bucket_admin per-cluster из config с env-fallback, 6edc80b; порты — выделенные аллокатором, §2.4; app-секрет в DSN не попадает никогда) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | весь жизненный цикл | таблица состояний §5 |
| `/clusters/<C>/buckets/status/bucket_<i>` | DELETE при завершении provisioning | снятие = бакет ACTIVE (семантика [11](11-bucket-sharding.md) §2, панели 02 §2.1) |
| `/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без поля `state`** (инициализирован = поле отсутствует, 02 §2.1; compare по `mod_revision`) |
| `/clusters/<C>/…` (весь префикс) | TO_REMOVE, финал | `del --prefix` |
| `/service/<C>-shard<k>/request_*` | TO_REMOVE, финал | точечные `del` (свои заявки; остальное пространство Patroni не трогаем) |
| `/service/<C>-<X>/` (весь scope) | TO_REMOVE, после удаления нод | `del --prefix` (guard: контейнеров/сервисов нет) |
| `/clusters/<C>/shards/<X>/master` | ТОЛЬКО при рассинхроне (P11-сверка) | lease-put `host:<doorman-port>` по фактическому primary из Patroni REST |
| `/clusters/<C>/shards/<X>/nodes/<n>/app_params` | provisioning P2.5'/AddShard A5 (после dsn), миграционно в надзоре C (ноды шардов с dsn без ключа) | put-if-absent (txn NotExists): значение по умолчанию `PgWorker:AppParams:Default`; существующий ключ не перезаписывается (ручные правки живы) |
| `/clusters/<C>/app_password` | ротация (§5 I): после успешного ALTER ROLE на всех шардах | txn `[compare value==старый] [put новый, del /pgworker/rotations/<C>]` — атомарный коммит с удалением заявки |
| `/pgworker/rotations/<C>` | успех ротации или битая заявка-мусор (§5 I) | del (в той же txn, что и put app_password; мусор — отдельным del с journal); TO_REMOVE-финал D2 тоже чистит |
| `/pgworker/moves/<C>/` (префикс) | TO_REMOVE, финал (D2) | `del --prefix` — заявки переездов не переживают удаление кластера |

**Финальное состояние кластера после provisioning**: `config` без поля
`state` (унификация с кластерами `init-cluster.sh` — панель видит «Active»
без правок), все `status/bucket_<i>` удалены (бакеты ACTIVE), у каждого
шарда есть `dsn`, у каждой ноды `state=RUNNING`.

Дельта add/remove шарда живого кластера (t06; процессы §5 G/H):

| Ключ | Когда | Действие |
|---|---|---|
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | add-shard A3/A4 | `PROVISIONING` → `RUNNING` (те же значения) |
| `/clusters/<C>/shards/<X>/dsn` | add-shard A5 | put multi-host (порты из portalloc, с per-cluster bucket_admin user+password) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | remove-shard S2 | `REMOVING` |
| `/clusters/<C>/shards/<X>/` (весь префикс) | remove-shard S3, после удаления docker-объектов | del prefix (state/replicas/nodes/dsn/master — всё) |
| `/service/<C>-<X>/` (весь scope) | remove-shard S3, guard: docker-объектов нет | del prefix |
| `/service/<C>-<X>/request_{cpu,mem,disk}` | remove-shard S3 | точечные del (даже если scope ещё жив) |
| `/pgworker/portalloc/<C>` | remove-shard S3 | read-modify-write: из JSON удалить записи `"<X>/<n>"` (merge под клэймом) |
| `/pgworker/evacuations/<C>/<X>` | remove-shard S3 | del (журнал эвакуации не переживает демонтаж шарда) |

Контрактное **финальное состояние после add-shard**: у шарда есть `dsn`,
все ноды `RUNNING`, `nodes`-ключи = декларации, routing/status/schema-
мир кластера не изменён НИКАК. После remove-shard: префиксы `shards/<X>/`
и `/service/<C>-<X>/` пусты, записей шарда в portalloc нет, остальные
шарды кластера не затронуты.

### 3.3. НОВЫЕ ключи координации воркеров (префикс `/pgworker/`)

Панель читает префикс избирательно (`portalloc`, `moves`, `api`, `work` —
arch/adminpanel/02 §2.3.1); координационные `leader`/`claims`/`instances` ей
не видны и не мешают контракту.

| Ключ | Тип | Назначение |
|---|---|---|
| `/pgworker/leader` | lease TTL 15 с | глобальный лидер для singleton-задач (регулярные снапшоты P12). Value: `{"instance":"<id>","since_unix":…}`. Захват: txn `version==0` + put-with-lease; продление keepalive раз в 5 с. Умер лидер → lease истёк → любой другой захватывает. |
| `/pgworker/claims/<C>` | lease TTL 15 с | **пер-кластерный клэйм** работы: exclusivity обработки кластера одним инстансом. Value: `{"instance":"<id>","since_unix":…,"phase":…}`. Захват txn `version==0` + put-with-lease; держатель продлевает. Takeover: lease истёк → ключ исчез сам → txn другого инстанса succeeds. |
| `/pgworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** (t90, §2.4): взаимоисключение секции довыделения портов «чтение занятости → выбор троек → запись `/pgworker/portalloc/<C>`» (provision P1 / add-shard / adoption-реплан) — пер-кластерные клэймы кросс-кластерную гонку не закрывают. Value: `{"instance":"<id>","since_unix":…}`. Захват txn `version==0` + put-with-lease; освобождение по завершении секции (del + revoke lease), смерть держателя — TTL. Не взял → InProgress (следующий тик). Без keepalive: секция короткая (единицы секунд ≪ TTL). |
| `/pgworker/work/<C>` | обычный | журнал текущего процесса кластера (journal-before-manipulations, по образцу P7): `{"op":"provision\|deprovision\|evacuate\|rebuild","phase":"…","updated_unix":…,"instance":"<id>","last_error"?,"fail_count"?,"fail_first_unix"?,"retry_not_before_unix"?,"unreachable"?}`. Крах оставляет самодокументирующийся след; следующий инстанс продолжает с записанной фазы. Поля ретраев (2026-09-01): `fail_count` — подряд идущие фейлы (сбрасывается при успехе/`done`), `fail_first_unix` — первый фейл серии (возраст проблемы; живёт до закрытия серии), `retry_not_before_unix` — до какого времени тики процесса — skip (бэкофф §5 A); переносятся записями фаз внутри серии. Читает панель (алерт `provision-stuck`, arch/adminpanel/02 §2.3.1) и оператор. |
| `/pgworker/evacuations/<C>/<X>` | обычный | журнал эвакуации шарда: `{"evacuated_unix","reason","buckets":{...старый→новый владелец...},"state":"DONE\|QUARANTINED"}` — истина для разбора после возврата шарда. |
| `/pgworker/portalloc/<C>` | обычный | закрепление выделенных портов за нодами (§2.4): `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` (+опц. `"object"` для усыновлённых, §5 J) — переживает смерть инстанса, переиспользуется при rebuild; пишется также усыновлением (§5 J: read-modify-write merge под клэймом). |
| `/pgworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика; необязательно для работы) |
| `/pgworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** (§1.1): `{"url":"http://<host>:<port>","instance":"<id>","since_unix":…}` — ставит сам инстанс при старте; ключ жив = инстанс жив и его URL валиден. Читает панель (единственный способ найти API воркера) и оператор; в UI не отображается |
| `/pgworker/moves/<C>/bucket_<i>` | обычный | заявка на плановый переезд/откат/уборку/отмену (t01): `{"op":"move\|rollback\|finalize\|abort","to":…,"old_shard":…,"skip_reverse":…,"resume":…,"force":…,"requested_unix":…,"requested_by":…}`. Успех или перманентный валидационный отказ → ключ удаляется; transient-сбой → остаётся, фазы — в статус-ключе бакета. Обрабатывается только держателем клэйма `<C>`; одновременно — старейшая заявка кластера. Deprovisioning D2 чистит `/pgworker/moves/<C>/` (префикс). |
| `/pgworker/rotations/<C>` | обычный | заявка на ротацию app-пароля ВСЕГО кластера (панель, клэйм-txn `version==0` + put): `{"requested_unix":<unix>,"requested_by":"<username панели>"}`. Выполняет держатель клэйма `<C>` (§5 I): ALTER ROLE на мастере каждого поднятого шарда → атомарный txn-коммит (put `app_password` + del заявки). Уже стоит → панель получает 409 (идемпотентность повтора). Deprovisioning D2 удаляет ключ точечно. |

Инварианты: любая мутация чужих данных (`/clusters/`, docker) выполняется
**только держателем клэйма** `<C>`; txn-записи в `/clusters/` сопровождаются
compare (routing=старое значение, config.mod_revision) — «применилось, а
контрол-плейн не знает» невозможно (как flip в [11](11-bucket-sharding.md) §5
шаг 4.7).

---

## 4. Секреты

Три группы:

1. **per-cluster, в etcd, генерирует PgWorker**: `app_user`/`app_password`
   (provisioning P1.5: txn put-if-absent, 32 симв `[A-Za-z0-9]`; роль app
   в БД выравнивается идемпотентным `ALTER ROLE … PASSWORD` на каждом шарде).
2. **per-cluster, в etcd, задаётся снаружи** (config JSON кластера, fallback
   env): `bucket_admin_user`/`bucket_admin_password` — попадают в dsn-ключ
   шарда и env контейнера ноды.
3. **per-install, из env PgWorker** (не в git, не в etcd — P12/P17):
   `PGW_PG_SUPERUSER_PASSWORD`, `PGW_PG_STANDBY_PASSWORD`,
   `PGW_BUCKET_ADMIN_PASSWORD` (fallback группы 2), `PGW_BUCKET_MOVER_PASSWORD`.
   `PGW_APP_ROLE_PASSWORD` исключён (app-секрет — только группа 1).

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

Active-ветка после надзора выполняет scale-проход `ScaleShardsAsync` (t06):
remove-кандидаты (`shards/<X>/state=TO_REMOVE`) → затем add-кандидаты
(declared-ноды без `dsn`), по одному шард-за-тик; демонтаж освобождает
хосты/порты до подъёма (Д13).

**Автономный reconcile (директива «воркер — хозяин», 2026-09-01):** каждый
тик воркер СНАЧАЛА сверяет фактом (живые docker-контейнеры, HA-контур
`/service/`), затем чинит расхождения САМ: (а) запланированный порт ноды
фактически занят чужим (docker-биндинг / portalloc соседа) → нода
перепланируется на свободные порты, portalloc/dsn обновляются, контейнер
создаётся в том же тике (§5 A P1); (б) Patroni-проба обязана подтверждать
ИМЕННО нашу ноду (REST `/patroni` несёт `scope`/`name`; чужой ответ по
коллизионному порту ≠ success — иначе фальш-RUNNING/фальш-dsn, §5 A P2.2);
(в) Active-кластер: инвариант `portalloc`/`dsn` = факт живых контейнеров —
репарация каждый тик (§5 J AD2'); (г) HA-scope без лидера при доказанной
утрате данных всех нод → чистка scope, Patroni бутстрапится заново; данные
есть хоть у одной ноды → не трогать, журнал для оператора (§5 A P2.2, R11).
Оператор привлекается только там, где автоматика рисковала бы потерять
СОХРАННЫЕ данные.

### A. ProvisioningProcess (P0–P5)

Все шаги идемпотентны — перепроверяют факт (эталон `init-cluster.sh`).
Guard входа: полный набор ключей панели (config, shards/*/replicas, nodes/*,
routing всех N) — иначе journal `phase=waiting-keys`, ожидание доустойчивости
ключей (полуфабрикат не provisioning'уем).

```
P0 claim + journal(/pgworker/work/<C>, op=provision)
P1 план: placement (§2.4) для всех шард/нод; порт-аллокация; journal phase=planned.
    Сначала — усыновление факта (2026-09-01): P1 КАЖДЫЙ тик provision
    инспектирует живые контейнеры кластера (InspectNodesAsync, механика
    §5 J AD1) — расхождение нельзя узнать без инспекции, а полный portalloc
    может быть расходящимся (потерян и выделен заново): живой канонический
    контейнер pgw-<C>-<X>-<n> (pg>0, patroni>0) = положительное
    свидетельство — его фактические public-порты становятся каноном записи
    "<X>/<n>" (факт над записью: контейнер уже жив, portalloc — лишь след
    плана; совпадение записи с фактом — не пишем; перезапись только записей
    без object; неканонический/неоднозначный контейнер — пропуск с
    journal-заметкой). Ноды без находок — обычная аллокация §2.4. Запись
    merge: ничего не изменилось — не пишем вовсе (идемпотентность); ключ
    существовал — put (read-modify-write под клэймом), не существовал —
    txn version==0 (как раньше).
    Перепланирование занятых (Д1, per-node): подтверждение записи — факт
    контейнера САМОЙ ноды (все ненулевые порты записи публикует её
    контейнер; нулевые порты — режим R1 без пулера — игнорируются);
    занятость = ВСЕ docker-публикации (чужие и контейнеры СВОЕГО кластера:
    дубликат порта внутри кластера — такой же конфликт) ∪ portalloc-записи
    соседей. Закрепление, не подтверждённое фактом своего контейнера и
    занятое, снимается — нода аллокируется заново на свободные порты,
    portalloc перезаписывается, EnsureNode (P2.1) создаёт контейнер в ТОМ
    ЖЕ тике; для Allocate-переиспользования факты подтверждённых записей
    вычитаются из занятости (ConfirmedFact — иначе allocator не переисполь-
    зовал бы валидные записи и EnsureNode пересоздавал бы живые контейнеры);
    наследие-коллизии portalloc двух кластеров самолечатся, вечный цикл
    «закреплено и переиспользуется → create fail» невозможен
P1.5 ensure app-секрета: прочитать /clusters/<C>/{app_user,app_password};
    отсутствующие — сгенерировать (32 симв [A-Za-z0-9]) и положить ОДНОЙ txn
    (compare NotExists на отсутствующие + put); txn проигран (гонка/re-run) —
    re-read и использовать существующие; роль app на каждом шарде создаётся
    с этим паролем и выравнивается ALTER ROLE (идемпотентно)
P2 на каждый шард X:
   P2.1 для каждой ноды n: создать volume + контейнер/сервис с конфигом
       (Spilo env: SCOPE=<C>-<X>, ETCD3_HOSTS=host:port (etcd v3), ttl=5/loop_wait=2 (P11),
        wal_level=logical + sync_replication_slots + max_slot_wal_keep_size
        (P3/P4), max_connections=60 и бюджет P15, callback on_role_change →
        lease-скрипт мастер-ключа; doorman: пул <dbname>, TLS require;
        haproxy: бэкенды всех Patroni-нод шарда), env-секреты (§4);
        nodes/<n>/state=PROVISIONING; при существовании (re-run) — сверить
        имя И порты: фактические public-биндинги контейнера (inspect)
        обязаны совпадать с планом (5432→pg, 8008→patroni, 6432→doorman);
        расхождение → пересоздать контейнер (stop+rm, volume сохраняется —
        фаза PROVISIONING, данных нет; идемпотентность по имени одному
        оставляла контейнер навсегда на чужих портах: WaitPatroni бил в
        мёртвый порт); совпадение — пропустить
   P2.2 ждать: /service/<C>-<X>/initialize есть + leader есть + у каждой
        ноды Patroni REST отвечает ИДЕНТИФИЦИРУЯ ноду (GET /patroni несёт
        scope=<C>-<X> и name=<n>; чужой ответ по коллизионному порту —
        НЕ успех: фальш-RUNNING/фальш-dsn на чужие данные исключены;
        бюджет 10 мин, транзиент-толерантно); nodes/<n>/state=RUNNING.
        При исчерпании бюджета (лидера нет) — проверка данных scope (Д3):
        docker-exec `test -f …/PG_VERSION` по каждой ноде → Present/Absent/
        Unknown; ВСЕ Absent (доказанная утрата) → чистка HA-scope (точечные
        initialize/leader/sync + префиксы optime//members/; request_* —
        декларации панели — НЕ трогаем) с journal phase=reset-scope → Patroni
        бутстрапится заново, бюджет ожидания начинается снова; хоть одна
        Present → НЕ лечить (чистка уничтожила бы данные): journal-фейл
        «данные есть, лидера нет — разбор оператора» (панель: provision-stuck/
        shard-no-leader); Unknown (транспорт docker) → ждать, не лечить
   P2.3 на мастере шарда (адрес из master-ключа/Patroni): создать БД <dbname>
        (если нет), роли app/bucket_admin/bucket_mover + GRANT'ы (§4 доки 11),
        если нет
   P2.4 создать схемы bucket_<i> по routing (только шардовы бакеты;
        CREATE SCHEMA IF NOT EXISTS, GRANT USAGE) — идемпотентно
   P2.5 записать shards/X/dsn (multi-host, с per-cluster bucket_admin user+password)
P2.5' ensure app_params КАЖДОЙ ноды шарда: put-if-absent (txn NotExists)
      nodes/<n>/app_params = PgWorker:AppParams:Default ("sslmode=require");
      существующий ключ не трогаем (ручные правки оператора живы)
P3 снять ВСЕ status/bucket_<i> (txn-пакетами ≤128 ops) — бакеты ACTIVE
P4 config: txn (compare mod_revision) → put канонического JSON без state
P5 снапшот P12; journal phase=done; кластер переходит в обычный надзор
```

Отказ на шаге: journal `last_error` + фаза; ретрай следующим тиком с
**бэкоффом** (2026-09-01): подряд идущие фейлы provision наращивают
`fail_count` в `/pgworker/work/<C>`, задержка ретрая —
`min(ProvisionRetryBaseSec·2^(n−1), ProvisionRetryMaxSec)` (5 с → 60 с);
до `retry_not_before_unix` тик процесса — skip без записи (самолечение не
блокируется: серия сбрасывается успехом (`Done`); фазы прогресса серию
переносят — §3.3). Исчерпание бюджета
Patroni (P2.2) сбрасывает трекер бюджета: следующая попытка получает новый
бюджет (иначе каждый тик после первого таймаута фейлил мгновенно — 234
одинаковых отказа за 10 минут на живом стенде). Гонка «панель пишет
TO_REMOVE посреди provisioning»: перед фазой процесс
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
   /pgworker/rotations/<C> + /pgworker/{portalloc,work,claims}/<C>*
   + del --prefix /pgworker/evacuations/<C>/ — журналы эвакуаций не
   переживают удаление кластера (t06, симметрия с S3)
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
  рассинхроне — не второй регулярный писатель). **Усыновлённые шарды
  (записи portalloc с `object`, §5 J) сверки НЕ проходят**: их master-ключ
  пишет внешний HA-контур (Patroni-callback/эмулятор) в своём формате
  `node:port` — коррекция порождала бы войну писателей; резолв мастера (§5 F)
  понимает оба формата.
- **Усыновлённые ноды** (запись portalloc с `object`): живость — Patroni-REST,
  а при `patroni=0` — SQL-проба мастера (`SELECT 1` по admin-DSN — положительное
  свидетельство живости PG); сверка декларации к ним НЕ применяется вовсе —
  их контейнер вне docker-домена `pgw-<C>-` (ListNodeObjectsAsync отдаёт только
  канонические имена): живой object-контейнер — «нода на месте» (skip),
  мёртвый не rebuild'ится. Матчинг по имени `pgw-<C>-<X>-<n>` остаётся только
  для канонических нод. Self-healing (пересоздание/rebuild/TO_RECREATE) для
  усыновлённых нод отключён: rebuild поднял бы канонический `pgw-`-контейнер
  рядом с внешним orchestration-кругом (например, вторым «Patroni» на тот же
  scope, R9) — мёртвая усыновлённая нода получает `UNREACHABLE` + journal
  (реальная проблема, разбор оператором). Evacuation-кандидат — по общим
  правилам (allDead+master-ключ протух).
- **Миграция app_params** (ленивый ensure): у нод шардов с dsn (любого
  state — мастером может стать любая нода) без `nodes/<n>/app_params`
  (кластеры, созданные до ключа) — put-if-absent значения по умолчанию
  (как P2.5'). Модель снапшота уже несёт наличие ключа — прогон без
  etcd-запросов, put только для отсутствующих; после первого обеспечения
  последующие тики — no-op.

Границы надзора (t06): шард без `dsn` — домен AddShardProcess (пробы/
самовосстановление/UNREACHABLE-переходы не трогаем — state нод входит в
A1-гвард add); с маркером TO_REMOVE самовосстановление отключено (домен
RemoveShardProcess — не пересоздавать демонтируемое), пробы остаются (шард
жив и обслуживает бакеты до демонтажа). Ноды в `QUARANTINED`/`REMOVING` надзор
не пробирует и их state не трогает — карантин держится эвакуатором до разбора
runbook'ом (E3-инвариант; UNREACHABLE-перезезапись ломала бы G6/Д6), демонтаж
идёт своим процессом. Кандидат эвакуации требует `dsn` и
≥1 бакета на шарде по routing (эвакуация пустого/незарегистрированного шарда
бессмысленна и блокировала бы G6 карантином); шард с TO_REMOVE-маркером
кандидатом МОЖЕТ быть — эвакуация умирающего помеченного шарда освобождает
бакеты, после чего G3 пропускает демонтаж (Д6).

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

Резолв мастера шарда для SQL (общий сервис ShardEndpoints; один и тот же у
всех SQL-путей — moves/эвакуация/ротация/репарация) — цепочка по убыванию
доверия: (1) master-ключ — по имени ноды, затем `host:<doorman-port>`
(усыновлённые: `node:<pg-port>`); (2) `/service/<scope>/leader` — имя лидера
HA-контура → адрес ноды из portalloc (без Patroni-REST — работает при
протухшем master-ключе в окне failover и на усыновлённых без REST);
(3) Patroni-REST `/cluster` по нодам с `patroni≠0`. Advertised-хост издателя
для `CREATE SUBSCRIPTION` применяется только когда приёмник — каноническая
`pgw-`-нода (запись portalloc без `object`): внешний приёмник (например,
контейнер стендового кластера) видит адреса dsn-ключа напрямую, подмена
`AdvertisedPublisherHost` сломала бы подключение.

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

Отказы M0 (t06): move `to` = шард в TO_REMOVE → перманентный отказ
«шард помечен к удалению — выберите другую цель»; `to` без dsn → «шард ещё
не поднят (add-shard не завершён)»; finalize с `old_shard` без dsn →
«шард удалён — убирать нечего». Переезды ИЗ TO_REMOVE-шарда разрешены.

### G. AddShardProcess (A0–A6; t06)

Подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере (панель заявила декларацию
§9.5 контракта панели: replicas + nodes/NOT_INITIALIZED + request_*).
Машина состояний одного тика, идемпотентна (механика ProvisioningProcess
в scoped-to-shard виде: EnsureNode, WaitPatroni, portalloc-merge,
DatabaseProvisioner). Guard A1: кластер Active; полное объявление (replicas>0,
nodes.Count==replicas, ноды NOT_INITIALIZED/PROVISIONING — иначе
phase=waiting-keys); `dsn` нет (есть → Done); scope `/service/<C>-<X>/initialize`
отсутствует — либо есть, но лидер совпадает с именем нод НАШЕГО шарда (наш же
поднимающийся Patroni после A3 — идемпотентность повторных тиков); коллизия
имён — initialize с чужим лидером (перманентная ошибка);
имя шарда `^[a-z][a-z0-9_]{0,30}$`; перечитывание config (R6) — NOT_INITIALIZED/
TO_REMOVE → phase=aborted. Перед созданием ролей — ensure app-секрета
кластера (образец P1.5: у живого кластера ключи уже есть — читаем;
отсутствуют, кластер создан до app-секрета, — генерируем и кладём txn
put-if-absent; роль app выравнивается `ALTER ROLE … PASSWORD`).
A5: БД/роли — ТОЛЬКО они; СХЕМЫ БАКЕТОВ НЕ
СОЗДАЮТСЯ (шард пустой, routing не указывает). Routing/status не пишутся ВООБЩЕ.
В SQL-фазе A5 — свежий re-read app-кредов (пока нода поднималась, ротация
§5 I могла сменить app_password) и ensure app_params нод шарда (как P2.5').

### H. RemoveShardProcess (S0–S4; t06)

Демонтаж шарда по маркеру `shards/<X>/state=TO_REMOVE` (пишет панель).
Guard'ы G1–G7 в S1 перед любым разрушающим действием (таблица §4.4):
G1 кластер Active; G2 шард заявлен; G3 ни один routing не указывает на шард
(P23); G4 ни один status-ключ не ссылается (owner ИЛИ target); G5 нет заявок
`/pgworker/moves/<C>/` с to=X или old_shard=X (саморазрешающийся); G6 нет нод
QUARANTINED; G7 в кластере есть другой шард. Провал guard'а = journal
last_error с причиной + InProgress (маркер-состояние живёт; после уезда
бакетов демонтаж продолжится сам). Порядок «сначала docker, потом etcd»
сохранён (мёртвые ключи при сбое безвредны — повторный тик продолжает).
S3: del prefix shards/<X>/ + точечные request_* + del prefix scope +
portalloc-фильтрация "<X>/<n>" (read-modify-write под клэймом) +
del /pgworker/evacuations/<C>/<X>.

### I. AppPasswordRotator (ротация app-пароля кластера, R0–R4)

Смена per-cluster app-пароля по заявке `/pgworker/rotations/<C>` (ставит
панель, клэйм-txn; формат — §3.3). Исполнитель — держатель клэйма `<C>`;
цикл ReconcileLoop зовёт процесс в Active-ветке после scale-прохода (короткая
секундная операция — не ждёт длинных переездов). Порядок фаз гарантирует
консистентность «роль app на каждом шарде ⟺ app_password в etcd»:

```
R0 заявка есть → journal op=rotate-app-password phase=started; клэйм-гвард
R1 прочитать {app_user, app_password} (OLD); отсутствуют — ensure (P1.5)
R2 NEW = сгенерировать (32 симв [A-Za-z0-9]); для каждого шарда С dsn
   (поднятого; шард без dsn — домен AddShard: роль создастся по свежему
   app_password): мастер (master-ключ → Patroni fallback) → admin-DSN →
   ALTER ROLE "<app_user>" PASSWORD '<NEW>' (реплики получают pg_authid
   физической репликацией). Любой сбой → transient: journal last_error,
   заявка жива, следующий тик повторяет С НАЧАЛА со свежим NEW (ALTER
   идемпотентен перезаписью — регенерация между тиками безопасна)
R3 все шарды OK → ОДНА txn: [compare value(app_password)==OLD (NotExists,
   если ключа не было)] [put app_password=NEW; del /pgworker/rotations/<C>]
   — коммит и снятие заявки неразделимы (нет двойной ротации из-за сбоя
   между put и del). Compare проигран (внешняя запись etcdctl) → re-read,
   ретрай тиком со свежим OLD
R4 снапшот P12 (точка изменения) + journal phase=done
```

Пока R3 не прошёл, `app_password` в etcd НЕ меняется — приложение работает
со старым паролем; окно расхождения (часть шардов уже с NEW, etcd со OLD,
приложение падает на переехавших шардах) существует только при transient-отказе
посередине и закрывается ретраями. После R3 клиенты обязаны перечитать
`app_password` из etcd (живые пулы реконнектятся с ошибкой до перечитывания —
плановая операция, выполнять в тихое окно; предупреждение — в UI-модалке
панели). Битая заявка (не-JSON/без `requested_unix`) — мусор: процесс её
удаляет с journal-записью (панель до того получает 409 «уже запрошена»).
Заявка кластера в NOT_INITIALIZED/TO_REMOVE панелью не ставится
(guard 409, контракт панели 02 §9.8); Deprovisioning D2 удаляет ключ точечно.

### J. AdoptionProcess (усыновление кластера, AD0–AD4)

«Внешних» кластеров для воркера не существует: Active-кластер с шардами
(`dsn` есть), у которых в `/pgworker/portalloc/<C>` нет записей (portalloc
потерян / кластер поднят вне провижининга — сид, стенд, восстановление etcd),
воркер **усыновляет**: восстанавливает адреса и переводит в обычный домен
(надзор §5 C с границами усыновлённых, moves §5 F). Кандидаты на усыновление
определяются положительным свидетельством в docker: ни одного контейнера
кластера не найдено → тихий skip (кластер живёт вне docker-хостов воркера —
его резолв мастера остаётся HA-фоллбэком (2), SQL-пути — transient, панель
алертит честную недоступность).

```
AD0 claim-гвард; journal op=adopt phase=started
AD1 на каждый шард X с dsn без записей portalloc:
    имена нод — /service/<C>-<X>/members/* (+role/state);
    docker-инспекция по Docker:Hosts: контейнер = нода, если его hostname
    ИЛИ сетевой алиас равен имени ноды; сайдкар Patroni-REST — env
    NODE_NAME равен имени ноды; порты — public-биндинги: 5432→pg,
    8008→patroni (в контейнере ноды или сайдкара), 6432→doorman (нет → 0);
    нода без контейнера → пропущена (частичное усыновление допустимо)
AD2 portalloc merge (read-modify-write под клэймом): только ОТСУТСТВУЮЩИЕ
    записи "<X>/<n>" = {host=docker-хост находки, pg, patroni, doorman,
    object=имя контейнера}; существующие записи не перезаписываются
AD2' инвариант адресов Active (каждый тик, Д2): portalloc = факт живых
    канонических контейнеров кластера (merge как в P1: тот же фильтр
    канонического имени + перезапись расходящихся записей без object) +
    перепланирование занятых чужим (Д1); dsn каждого шарда пересобирается
    из фактического portalloc (multi-host по кандидатам nodes-ключи ∪ HA-members, как AD1) —
    расхождение ключа → put; dsn пересобирается только для канонических
    (без object) нод — object-шарды: dsn операторский факт (R9-симметрия);
    репарации журналируются (phase=repaired-
    portalloc / repaired-dsn). Фальш-Active (dsn на чужие порты —
    наследие коллизии) самолечится, вечные UNREACHABLE невозможны;
    0 docker-находок → тихий skip (кластер вне docker-хостов, §2.5);
    transport-провал инспекции → transient, следующий тик повторит;
    запись канонической ноды (без object) без ЖИВОГО контейнера — Created/
    exited-черепок или снесённый контейнер при state=RUNNING (процессные
    пути скипают RUNNING, инспекция running-only) → EnsureNode напрямую
    (сверка портов → stop+rm+create по плану), journal phase=recreated-node
AD3 nodes-ключи: нодам кластера без /clusters/<C>/shards/<X>/nodes/<n>/state
    — put RUNNING (декларация следует за фактом; dsn уже есть — шард
    зарегистрирован); app-секрет ensure (P1.5); роли БД ensure на мастере
    каждого шарда (app/bucket_admin/bucket_mover — идемпотентный P2.3);
    app_params ensure (P2.5')
AD4 снапшот P12 (точка изменения); journal phase=done — кластер в обычном
    надзоре; повторные тики AD1–AD4 — no-op (все записи на месте)
```

Отказ docker-хоста/etcd на любом шаге — transient: journal last_error, тик
повторит (идемпотентность merge/put-if-absent). Deprovisioning D2 сносит
portalloc вместе с префиксом — усыновление повторяется при пересоздании.

Ensure-инвариант каждого тика Active («воркер — хозяин», живой-Ф7'):
для всех dsn-шардов ensure БД и ролей выполняется и когда усыновлять нечего.
Ensure БД кластера идёт подключением к `postgres` (паттерн P2.x) + схемы
бакетов-владельцев по routing (`CREATE SCHEMA IF NOT EXISTS`, P2.6): после
утраты данных и re-bootstrap Patroni артефакты etdc (dsn/portalloc/nodes)
есть, а базы/схем нет (initdb создал только postgres) — целевое подключение
падало вечным 3D000, панели не сходился inventory (routing ↔ схемы).
Гварды идемпотентны — здоровый кластер платит несколько дешёвых SELECT за тик.

### K. MoveRepairProcess (репарация брошенных переездов, MR0–MR3)

Статус-ключ `/clusters/<C>/buckets/status/bucket_<i>` без живого владельца
воркер не бросает: живой владелец — заявка `/pgworker/moves/<C>/bucket_<i>`
(домен MoveProcess §5 F; обновляет `updated_unix` каждый тик M3, Д12) или
свежий статус. Брошенный = нет заявки И `now − updated_unix` превысил порог:
`FROZEN` (заморозка режет запись — чиним быстрее) — `RepairFrozenSec`;
`SYNCING`/`ABORTING` — `RepairStaleSec` (= StaleMoveSeconds панели: ремонт
стартует, когда панель уже заалертила; алерт загорелся → ремонт пошёл →
алерт погас реальным ремонтом). Кластеры — только Active.

```
MR0 claim-гвард; статусы — из снапшота (updated_unix — из статус-ключей)
MR1 на каждый статус без заявки (кроме NOT_INITIALIZED — домен P3):
    возраст > порога состояния → синтетическая заявка put-if-absent
    (txn version==0 — не затирает операторскую):
    - ABORTING/*                          → {"op":"abort"} (resuming-доводка)
    - SYNCING/* при routing==owner        → {"op":"abort"} (свежесть пройдёт:
                                            возраст > RepairStaleSec ≥ AbortMinAgeSec)
    - SYNCING|FROZEN при routing==target  → {"op":"abort","force":true}
                                            (flip прошёл, статус завис — доведение
                                            перевода, без force это permanent-отказ)
    - FROZEN/* при routing==owner         → {"op":"abort"} (уборка + re-GRANT =
                                            разморозка владельца)
    - phase=rollback-post-flip            → {"op":"rollback"} (доведение отката
                                            по живой sub_<b>_rb, §5 F rollback)
    все заявки: requested_by="pgworker-repair"
MR2 дальше работает MoveProcess (старейшая заявка тика) — вся механика
    доведения/журналов/идемпотентности переиспользуется 1:1
MR3 journal op=repair (сколько/какие статусы диспатчены; повторный тик —
    no-op: заявка уже стоит)
```

Недоступный шард → уборка честно висит (ABORTING/blocked, P7) — панель
продолжает алертить реальную проблему; отдельного fail-state НЕ вводим
(фазы/журнал уже несут семантику). Ручной скриптовый переезд без обновления
статуса дольше `RepairStaleSec` будет репарирован — скриптовый путь
однопроходный, окно переезда со скриптом и репарацией не смешивать (общее
правило «не смешивать скрипты и заявки», §1).

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
                     CutoverTimeoutSec=90, ConnFailBudgetSec=120,
                     ProvisionRetryBaseSec=5, ProvisionRetryMaxSec=60 }
                     # бэкофф ретраев provision (§5 A): задержка
                     # min(Base·2^(fail_count−1), Max) — 5,10,20,40,60,60…;
                     # PatroniBootSec — он же бюджет отсутствия лидера для
                     # лечения HA-scope при доказанной утрате данных (Д3, §5 A P2.2)
PgWorker:Moves { PollIntervalSec=2, FreezeWaitSec=5, FreezeLockTimeoutSec=5,
                 FreezeLockTries=3, AbortMinAgeSec=120, FailoverSlots=true,
                 AdvertisedPublisherHost=null } # host издателя, как виден из
                 # контейнеров приёмников (single-docker-host стенды:
                 # host.docker.internal; прод — null, адреса dsn достижимы);
                 # применяется только для канонических pgw-приёмников (§5 F)
PgWorker:Moves { RepairStaleSec=600, RepairFrozenSec=120 } # репарация брошенных
                 # статусов (§5 K): 600 = StaleMoveSeconds панели (ремонт
                 # синхронизирован с алертом), 120 = AbortMinAgeSec (FROZEN
                 # режет запись — чиним быстрее, живой cutover межтиков
                 # невозможен: непрерывный блок одного тика)
PgWorker:Parallelism { MaxClusters=4 }
PgWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
PgWorker:AppParams { Default="sslmode=require" }  # per-node ключ
                  # shards/<X>/nodes/<n>/app_params (P2.5'/A5/C; P17)
PgWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false }  # §1.1: URL API
                  # (достижимый панелью) в /pgworker/api/<id>; демо-сид-эндпоинт
# секреты — env PGW_* (§4) + PGW_API_KEY (§1.1, аутентификация API)
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
| R8 | Усыновление: master-ключ внешнего формата `node:port` vs reconciler `host:doorman` — война писателей | MasterKeyReconciler пропускает усыновлённые шарды (§5 C); master-ключ пишет внешний HA-контур, резолв мастера понимает оба формата (§5 F) |
| R9 | Rebuild усыновлённой ноды поднял бы дубль (второй Patroni на scope, конфликт портов) | self-healing для `object`-нод отключён (§5 C): UNREACHABLE + journal, разбор оператором |
| R10 | Репарация прибивает живой скриптовый переезд (скрипт не тикает updated_unix в copy-wait) | порог RepairStaleSec = панельному stale (панель уже считает это аномалией); правило «не смешивать скрипты и заявки в одном окне» (§1); заявка репарации помечена requested_by=pgworker-repair |
| R11 | Чистка HA-scope (Д3) при живых данных = потеря кластера | трёхуровневая проба данных (Present/Absent/Unknown через docker-exec `test -f PG_VERSION`): чистка ТОЛЬКО при Absent у ВСЕХ нод scope; Unknown (транспорт docker) и хоть одна Present — не лечить (Present → журнал-фейл «разбор оператора»); одна чистка на scope за бюджет (новый бюджет после чистки); journal phase=reset-scope — оператор видит каждую чистку |
| R12 | Перепланирование портов (Д1/Д2) меняет dsn — клиенты держат старые адреса | dsn-ключ — единственная точка входа (клиенты перечитывают etcd, паттерн app_password-ротации §5 I); репарация только при расхождении с фактом (стабильный кластер не трогается); journal repaired-dsn — событие видно |

---

## Дальше

→ Возврат к [README.md](README.md). Контракт etcd кластеров — [11](11-bucket-sharding.md)
§2; риски топологии — [12-bucket-pitfalls.md](12-bucket-pitfalls.md);
сетевая модель — [13](13-network-security.md); отложенные задачи —
[roadmap/pgworker.md](roadmap/pgworker.md).
