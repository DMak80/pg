# PgWorker: модель размещения и узлы кластера

Уровень 3. Как устроена нода кластера: образ, роли, порты, тайминги Patroni,
PGTune, режимы Plain/Swarm, транспорт Engine API, placement и адресация.
Процессы, использующие модель, — [жизненный цикл](02-cluster-lifecycle.md),
[масштабирование шардов](03-shard-scaling.md); секреты —
[04-security.md](04-security.md).

## 1. Нода кластера = контейнер `pgworker-node`

Нода кластера-шарда = **один контейнер/сервис** из кастомного образа:
`ghcr.io/zalando/spilo-18:4.1-p2` (Spilo + PG18 + **Patroni 4.x**) +
`pg_doorman` (обязателен: бинарник — пин мульти-арх контейнер-образа проекта
pg_doorman, обязательный build-arg `DOORMAN_IMAGE` — fail-fast без него, R1;
целевая платформа x64 — amd64-сборка, arm64 — платформа разработки,
[pg-clusters/03-prerequisites.md](../pg-clusters/03-prerequisites.md) §1) + **агент ноды PgNode**
(программа `pgnode` supervisord'а: приём WAL бэкапов —
[backups/02-wal-archive.md](../backups/02-wal-archive.md), физическая репликация
`127.0.0.1:5432` своей ноды → S3-пул; загрузочный агент ноды и менеджер
пулера — доставляет секреты по каналу воркера —
[04-security.md](04-security.md) §3, поднимает spilo/pg_doorman после
рендера конфигов, ведёт конфиг/серты doorman; жизненный цикл старта —
статусная машина `not_initialized → initializing → ready | start_failed` с
хендшейком воркера) + `supervisord` + .NET CLI `pgwnodetool master-lease`
(параметр `--env-file /home/postgres/pgw-node.env`): `/v3/lease/grant` +
keepalive цикл 1 с, TTL 5 с. Supervisord автостартует ТОЛЬКО pgnode
(spilo/pg_doorman — `autostart=false`; их стартует агент после доставки
секретов — без них процессы не жизнеспособны).

Программа `spilo` supervisord'а — обёртка над апстримным `/launch.sh`:
апстрим ставит `runsvdir -P` (каждый `runsv` в СВОЕЙ группе процессов —
групповой stop/kill supervisord'а `stopasgroup`/`killasgroup` их не
достаёт; сироты держат `supervise/lock`, и Patroni следующего запуска не
стартует никогда — «unable to lock supervise/lock», ломает restore-стоп/
старт Patroni ноды, [backups/03-restore.md](../backups/03-restore.md)). Обёртка гасит
runit-дерево сама: TERM → штатный `sv stop patroni` + сервисы (бюджет, до
окна supervisord'а), затем точечный добор `runsv`/`runsvdir` — лок
освобождён, `start spilo` поднимает Patroni чисто. Внутри контейнера всё
общается через localhost — работает и в plain, и в swarm (у swarm нет
host-network и «подов»; sidecar'ы отдельными сервисами громоздки).

Приёмник WAL идентичность получает env'ом ноды (`PGW_BACKUP_CLUSTER/SHARD/
NODE` + per-install `PGW_BACKUP_S3_BUCKET`, опц. `PGW_BACKUP_S3_CA_PEM` —
[backups/02-wal-archive.md](../backups/02-wal-archive.md)/[backups/08-secrets.md](../backups/08-secrets.md); адреса etcd — уже существующий
`PGW_ETCD`); всё остальное (роль источника, цели S3, креды, слот) резолвит
сам из etcd — воркер приёмником не управляет, стоп-семантика — через
etcd-ключи ([backups/02-wal-archive.md](../backups/02-wal-archive.md)/[backups/05-etcd-contract.md](../backups/05-etcd-contract.md)). Нода, созданная до
включения приёмника в образ (без env `PGW_BACKUP_*`), приёмника не несёт:
WAL-архивация с неё невозможна, обновление образа ноды — пересозданием
(rebuild/recreate, данные в volume сохраняются).

**Роли внутри:**

| Сервис | Слушает | Роль |
|---|---|---|
| PostgreSQL | `:5432` | подписки переездов (прямой порт master из portalloc, P2) и админка |
| Patroni (в Spilo) | REST `:8008` | управляет локальным PG; callback `on_start`/`on_role_change` → lease-put `shards/X/master` (P11); REST `/primary` потребляет PgWorker (пробы/сверка) |
| pg_doorman | `:6432` | пулер приложений, бэкенд только `127.0.0.1:5432`; единственный пул `<dbname>` (`pool_mode=transaction`, TLS `sslmode=require`, P13/P14/P17); обязателен — сборка образа без `DOORMAN_IMAGE` — fail-fast (R1); конфиг/серты ведёт загрузочный агент ([04-security.md](04-security.md) §3) |
| pgnode (агент PgNode) | `:8080` (/metrics + API операций бэкапов), `:8081` (mTLS-канал доставки) | приёмник WAL бэкапов ([backups/02-wal-archive.md](../backups/02-wal-archive.md)): стрим `127.0.0.1:5432` → S3-пул; на `:8080` — `/metrics` (Prometheus через ScrapeNetwork, [metrics/README.md](../metrics/02-dictionary.md) §8/§5.4) И HTTP API операций бэкапов ([pgnode/02-api.md](../pgnode/02-api.md)) по каналу `alias:8080` — общей сети воркера и нод ([communications/02-channel-registry.md](../communications/02-channel-registry.md) №22); на `:8081` — mTLS-сервер загрузочного агента (канал доставки секретов, [04-security.md](04-security.md) §3; только для PgWorker); наружу хоста НЕ публикуются оба порта (docker и swarm) |

Наружу (хост) публикуются порты: `pg` (5432→выделенный), `patroni`
(8008→выделенный), `doorman` (6432→выделенный) — тройка из порт-аллокатора
(§5). Конфиги ноды генерирует PgWorker: SPILO_CONFIGURATION (целиком, с
секретами) и серт ноды — в bootstrap-ключ etcd
([00-etcd-contract.md](00-etcd-contract.md) §3), доставляет загрузочный
агент; DOORMAN_CONFIG_TEMPLATE — неконфиденциальный шаблон конфига пулера в
docker-env (пароль — плейсхолдер), пароль подставляет агент (параметры
пулера — [04-security.md](04-security.md) §4,
[pg-clusters/11-buckets.md](../pg-clusters/11-buckets.md)).

Volume: `pgw-<C>-<X>-<n>-data` → `/home/postgres/pgdata` (дефолтный
PGDATA-корень Spilo; переопределение `PGROOT` ломает bootstrap —
data-каталог создаётся от root и недоступен patroni под postgres).

## 2. Канонические тайминги Patroni и PGTune

**Тайминги Patroni**: `ttl=20, loop_wait=1, retry_timeout=3` (плюс
`synchronous_mode=true`; `synchronous_mode_strict` — НЕ глобальная
константа, а per-cluster опция из `/clusters/<C>/config`,
[00-etcd-contract.md](00-etcd-contract.md) §2: отсутствие поля = `true`,
дефолт durability-first; bootstrap ноды подставляет значение кластера в
`bootstrap.dcs` SPILO_CONFIGURATION, живые кластеры приводит конвергенция
DCS — динамический параметр, без рестартов PG).

Обоснование: Patroni 4.x валидирует динамический конфиг жёсткими полами —
`loop_wait≥1`, `retry_timeout≥3`, **`ttl≥20`** — и правилом
`loop_wait + 2*retry_timeout ≤ ttl`; заниженное значение молча поднимается
до пола и записывается обратно в DCS (заявленный ttl=5 Patroni 4.1 заменил
на 20 — «failover за 5с» через ttl недостижим; бюджет смены лидера —
[processes/c-node-supervisor.md](processes/c-node-supervisor.md)). Значения
задаются в `bootstrap.dcs` SPILO_CONFIGURATION (применяется при ПЕРВОЙ
инициализации DCS-кластера, далее действует динамический конфиг из etcd) и
**гарантируются конвергенцией**: воркер сверяет активный динамический
конфиг (GET /config Patroni-REST) с каноном и патчит (PATCH /config) при
расхождении — нода/кластер, работающие на отсутствующем/чужом/дефолтном
конфиге, приводятся к канону без пересоздания; при неконвергентности
(шард не принимает патч) действуют общий self-healing-контур
(rebuild/эвакуация) — «старое с плохими параметрами» не живёт параллельно
канону. Общие принципы синхронизации распределённого состояния (единый
контур etcd, конвергенция DCS, инварианты внешних систем, детерминизм
мутаций, флап≠смерть, порядок расследования) —
[synchronization/README.md](../synchronization/README.md).

**Параметры PG в `bootstrap.dcs` — PGTune**: `postgresql.parameters` ноды
рассчитываются ядром `PgTune.Calculate` (чистая функция по нормативу
[09-pgtune.md](09-pgtune.md): формулы §4, порядок вывода §5, псевдокод
§6) от характеристик ноды — память/CPU из заявок
`/service/<scope>/request_{mem,cpu}`, остальные входы — опции
`PgWorker:Pgtune` ([08-operability.md](08-operability.md) §5). Применение —
merge(PGTune ∪ канон PgWorker) в `SpiloEnvBuilder`: сначала вывод PGTune в
порядке §5.2 (минус `ExcludeParams`), затем канон «поверх» с перезаписью по
имени — `wal_level=logical`, `hot_standby`, `sync_replication_slots`,
`max_slot_wal_keep_size`, `max_wal_senders=max_replication_slots=10`,
`wal_keep_size`, `checkpoint_timeout` и лог-блок (P3/P4). Константы
`max_connections`/`shared_buffers`/`effective_cache_size`/
`random_page_cost`/`checkpoint_completion_target` из канона **убраны** — их
несёт PGTune.

Параметры пересчитываются при КАЖДОМ EnsureNode-пути
(provision/add-shard/пересоздание/rebuild) от актуальных заявок — **БЕЗ
фиксации в etcd** (осознанное «никакого состояния», etcd-контракт не
меняется; следствие — дрейф pg-конфига между нодами шарда при изменении
заявок работающего кластера: ноды, поднятые ранее, продолжают работать на
прежнем конфиге — консистентность на операторе). Конвергенция pg-параметров
работающих нод реализована: желаемый набор — единый источник и для bootstrap
(SPILO_CONFIGURATION), и для живого конфига (DCS `postgresql.parameters`
через PATCH /config); bootstrap и конвергенция не могут разойтись по
определению. Факт среды (Spilo 4.1-p2): `bootstrap.dcs.parameters` при
инициализации ноды НЕ попадают ни в живой DCS-конфиг (`/config`), ни в
postgresql.conf — Spilo пересчитывает часть значений сам (например
`shared_buffers` — из памяти контейнера). Свежеподнятая нода стартует не на
нашем наборе — к канону её приводит первый же тик конвергенции. Следствие
для проверок: фактическое бутовое значение postmaster-параметра на ноде — не
литерал из DCS (ассерты строятся от значения, захваченного на ноде до
изменения заявок). Дрейф конфига между нодами шарда при изменении заявок
устранён на уровне DCS: динамика применяется сразу (reload в пределах
loop_wait), postmaster-параметры — через `pending_restart` до ближайшего
рестарта ноды (воркер никогда не инициирует рестарт PG). Doorman-бюджет
обновляется при создании контейнера (P15 согласован: фактическое применение
postmaster идёт через rebuild — пересоздание контейнера несёт и новый шаблон
`DOORMAN_CONFIG_TEMPLATE` в env; применение рендерит агент). Ветвь PGTune
`dbType=desktop` (`wal_level=minimal`, `max_wal_senders=0`) не применяется
никогда — отвергается валидацией старта (несовместима с P3).

Решения фазы исполнения (дока синхронизирована с кодом):

- **HAProxy в топологии нет**: в образе ноды он не поднимается — его
  write-фронтенд `:5432` конфликтует с PostgreSQL в одном netns (Д4 — один
  контейнер на ноду), отдельным контейнером воркер его не строит. Write-вход
  — прямой pg-порт master-ноды (portalloc, multi-host DSN,
  `target_session_attrs=read-write` — писателя находит libpq); подписки
  переездов — тот же multi-host DSN
  ([pg-clusters/11-buckets.md](../pg-clusters/11-buckets.md) §4).
- **Patroni DCS — etcd v3 API** (env `ETCD3_HOSTS`, формат `host:port` БЕЗ
  scheme; v2-клиент Spilo с etcd 3.5 несовместим). Адреса etcd для нод —
  отдельная настройка `PgWorker:Etcd:AdvertisedEndpoints` (ноды ходят в etcd
  через docker-сеть, а не через endpoint'ы самого PgWorker). HA-контур
  контроль-плейна: список ВСЕХ узлов etcd —
  [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2 (advertised-правила
  потребителей).
- Callback мастер-ключа — `on_start` + `on_role_change` (в `on_start`
  Patroni роль аргументами не передаёт — скрипт узнаёт её сам по
  `GET /primary`).
- **Механика REST-TLS ноды** (REST `:8008` — только HTTPS,
  [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4): серт/ключ ноды
  доставляет загрузочный агент (bootstrap-ключ etcd,
  [04-security.md](04-security.md) §3) и материализует в tmpfs `/run/pgw`;
  обёртка spilo-supervised.sh перед launch.sh экспортирует их как env
  ПРОЦЕССА `SSL_RESTAPI_CERTIFICATE`/`SSL_RESTAPI_PRIVATE_KEY`
  (материализация файлов и `restapi.certfile/keyfile` — штатный Spilo); в
  docker-env остаётся только публичный `SSL_RESTAPI_CA` (REST-слой
  верифицируется клиентами по цепочке к CA без hostname-проверки —
  [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4). Секция `restapi`
  SPILO_CONFIGURATION: `connect_address: <полное имя ноды>:8008` (DNS
  per-cluster сети, перекрывает шаблонный IP; api_url в DCS = DNS из SAN) +
  `authentication {username, password}` — per-cluster пара `rest_password`
  ([04-security.md](04-security.md) §1; unsafe-эндпоинты). Серт ноды — не
  etcd-состояние узла навсегда: генерация воркером из per-install CA (кеш
  процесса, канон серверных сертов KafkaWorker) + публикация в
  bootstrap-ключе для доставки агентом. Lease-скрипт мастер-ключа (P11) в
  `on_start` опрашивает `https://127.0.0.1:8008/primary` с верификацией по
  ca-файлу ноды (entrypoint материализует PEM из docker-env в файл и пишет
  путь строкой в pgw-node.env — PEM в KEY=VALUE-файл не переносится).

## 3. Режимы Plain / Swarm

### 3.1. Plain (docker на выделенных хостах)

- Конфиг `PgWorker:Docker:Hosts[]` — таблица хостов `{Name, Endpoint}`.
  Endpoint — одна из схем (транспорт к Engine API — §4):
  `unix:///var/run/docker.sock` (локальный хост),
  `tcp://10.0.1.11:2376` (+TLS-конфиг `PgWorker:Docker:Tls`),
  `ssh://user@10.0.1.11[:22]` (SSH-туннель `PgWorker:Docker:Ssh`).
  Каждый хост — свой клиент Docker Engine API (per-host connection).
  Клиент Engine API — общая сборка трёх воркеров `Shared.Docker` (три копии
  движка объединены; SSH/TLS — опции фабрики, доступные всем доменам;
  create при отсутствии образа — pull+retry).
- PgWorker сам вычисляет placement (§5) и создаёт контейнеры на выбранных
  хостах: `POST /containers/create?name=pgw-<C>-<X>-<n>` → `start`.
  Restart-политика `unless-stopped` (docker сам поднимает после ребута
  хоста).
- Анти-аффинити: ноды одного HA-кластера (шарда) — на разных хостах, если
  `hosts >= replicas`; иначе — равномерно по least-loaded (число занятых
  слотов из `/pgworker/portalloc` + `GET /containers/json`).
- Ограничение: хост открывает Engine API только по защищённому транспорту
  (§4); plaintext `tcp://…:2375` — не канон (допустим в dev/тестах,
  warning-лог воркера).

### 3.2. Swarm

- Конфиг `PgWorker:Docker:SwarmManager` — endpoint любого manager-узла (те
  же схемы транспорта, что §4: unix / tcp+TLS / ssh). Одна нода шарда =
  один сервис `pgw-<C>-<X>-<n>` с `replicas=1` (гранулярность = нода:
  точечный rebuild без `--force-update` всего шарда).
- Анти-аффинити: PgWorker назначает placement через constraint на
  конкретную ноду, вычисленную из `GET /nodes` (число работающих тасков —
  least-loaded). Drifted-сервисы сверяются с планом и пересоздаются
  (декларативность).
- Объекты нод кластера для сверок (надзор, guard D2, сироты) — список
  сервисов `GET /services` по префиксу `pgw-<C>-` (существование сервиса ≠
  живой таск: живость проверяют Patroni-пробы, не сверка имён).
- Порты: `publish mode=host` (без ingress-балансировщика) с выделенными
  аллокатором портами на машине таска; DSN строится по факту.

## 4. Транспорт к Engine API: TLS, SSH-туннели, RBAC

Engine API = root на docker-хосте: транспорт и доступ к нему защищаются
per-install docker-PKI и каноном хостов (Docker CE тонкого RBAC не имеет —
authz-плагины вне скоупа, граница зафиксирована выше).

- **TLS (tcp://)**: docker-демон удалённого хоста слушает
  `tcp://127.0.0.1:2376` (loopback) или выделенный интерфейс — ТОЛЬКО с
  `--tlsverify` (systemd drop-in: `-H tcp://0.0.0.0:2376 --tlsverify
  --tlscacert=ca.pem --tlscert=<host>.crt --tlskey=<host>.key`); серверный
  серт хоста подписан per-install docker-CA (генерация —
  `deploy/tls/gen-docker.sh <host>`, SAN = имя/IP хоста), клиентский серт
  воркера (`pgworker-docker.crt`, CN=`pgworker-docker`) — из той же CA.
  Конфиг клиента — `PgWorker:Docker:Tls {CaPem|CaPath,
  ClientCertPem|ClientCertPath, ClientKeyPem|ClientKeyPath}` (env
  `PGW_DOCKER_TLS_{CA,CERT,KEY}[_PATH]`); применяется к tcp://-эндпоинтам
  (fail-fast при частичной конфигурации); unix:// — TLS игнорирует.
- **SSH-туннель (ssh://)**: воркер держит SSH-сессию к хосту
  (key-аутентификация, `PgWorker:Docker:Ssh {KeyPem|KeyPath,
  RemoteDaemonHost=127.0.0.1, RemoteDaemonPort=2376, FingerprintSha256?}`;
  env-секрет `PGW_DOCKER_SSH_KEY[_PATH]`) и локальный форвардинг
  (ForwardedPortLocal `127.0.0.1:0 → <RemoteDaemonHost>:<RemoteDaemonPort>`);
  Engine-клиент хоста ходит по выделенному локальному порту как обычный
  tcp:// (+TLS, если демон на daemon-порту с tlsverify — канон). Fingerprint
  хоста не задан → accept + warning (TOFU-примитив); задан — строго. Отказ
  туннеля = transient (healthz-пинг хоста честно красный, тики повторяют);
  keepalive SSH-сессии + reconnect. Позволяет демону НЕ слушать TCP вовсе?
  Нет — daemon-порт на loopback всё же нужен (SSH-канал — прямое
  TCP-соединение к daemon-порту через сервер); канон daemon'а:
  `-H tcp://127.0.0.1:2376 --tlsverify` (loopback + mTLS, наружу не слушает).
- **RBAC/docker-группы (канон хостов)**: локальный доступ — unix-socket
  группы `docker`; контейнер PgWorker примонтированный socket =
  root-эквивалент хоста — запуск с `group_add: [<gid docker>]` и выделенным
  пользователем (compose deploy — комментарий/пример); firewall: 2375 НЕ
  слушается никогда, 2376 — только с mTLS и только из сети
  воркеров/админки (матрица [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md)
  §2); каждый хост — свой серверный серт; компрометация клиентского серта
  воркера = root всех docker-хостов установки → серт в env-секрете,
  ротация — перегенерация пакета.
- **Реализация транспорта** (unix / tcp+TLS / ssh-туннель с кэшем и
  reconnect, docker-PKI-материал) — общая сборка `Shared.Docker`;
  env-секреты `PGW_DOCKER_TLS_*`/`PGW_DOCKER_SSH_*` — pg-слой
  `PgWorker.App` (`DockerEnvBindings`).

## 5. Placement, порты и адресация

1. Для каждого шарда `X` с `replicas=R`: планировщик выбирает R хостов —
   все разные (анти-аффинити), при `hosts < R` — равномерно с минимальным
   числом совпадений («если топология позволяет — на разных, иначе на
   одной»). Факт совпадений виден оператору (план в journal).
2. Порт-аллокатор: порты ноды — ПОСЛЕДОВАТЕЛЬНЫЕ слоты из диапазона
   `PgWorker:Docker:PortRange` (схема смещений `pg=base+i`, `patroni=+3000`,
   `doorman=+1500` упразднена: каждая публикация ноды — следующий свободный
   слот диапазона, состав портов ноды фиксируется её portalloc-записью).
   Диапазон параметризуется средой развертывания: прод — фиксированный
   per-install диапазон; E2E — per-contour НЕПЕРЕСЕКАЮЩЕЕСЯ окно, которое
   резервирует окружение теста физическим bind'ом (первым портом окна
   публикуется etcd-контейнер контура; занят — окно сдвигается на следующий
   блок, [docs/e2e-isolation.md](../../docs/e2e-isolation.md) §5):
   межконтурные коллизии host-портов исключены конструкцией (окна не
   пересекаются), а не только проверкой. Внутри контура — проверка
   фактической занятости: `GET /containers/json` (живые публикации docker) +
   записи portalloc ВСЕХ кластеров (`/pgworker/portalloc/*`, кроме своего —
   свой переиспользуется как закрепление; закрывает кросс-кластерную
   коллизию: без этого второй кластер получает порты первого, чьи
   контейнеры ещё не созданы, а portalloc уже записан). Advertised-адреса —
   только от фактических портов публикаций. Закрепление —
   `/pgworker/portalloc/<C>` (переживает rebuild: та же нода = те же порты).
   Запись ноды: `{"host","pg","patroni","doorman"}` + опциональное
   `"object"` (усыновление): имя фактического docker-объекта
   **усыновлённой** ноды (контейнер внешнего происхождения, не
   `pgw-<C>-<X>-<n>`); отсутствие `object` = каноническая нода нашего
   провижининга. У усыновлённой ноды `patroni`/`doorman` могут быть `0`
   (внешние ноды без Patroni-REST/doorman в этом контейнере — Patroni-REST
   может жить сайдкаром, живость такой ноды — SQL-проба,
   [processes/c-node-supervisor.md](processes/c-node-supervisor.md)) +
   опциональные `"alias"`/`"net"`: сетевая идентичность ноды для скрейпа —
   пишутся ТОЛЬКО каноническим нодам при заданном
   `PgWorker:Docker:ScrapeNetwork` (`alias` = полное docker-имя
   `pgw-<C>-<X>-<n>`, глобально уникально, резолвится user-defined DNS сети
   контура; `net` = имя сети контура — информационное, для инспекции etcd).
   Read-modify-write portalloc (remove-shard, adoption-merge, repair) поля
   сохраняет, а не стирает (симметричная сериализация). `alias` НЕ входит в
   dsn/endpoints/панельные пробы (потребитель один — file_sd-генератор
   [metrics/README.md](../metrics/05-storage.md) §4); записи без полей —
   легаси/деградационный контур.
   **Глобальный portalloc-клэйм**: довыделение новых троек (недобор нод, а
   не переиспользование закреплений) — глобально взаимоисключающая секция
   «чтение занятости (docker ∪ portalloc соседей) → выбор троек → запись
   `/pgworker/portalloc/<C>`»: выполняется только держателем клэйма
   `/pgworker/locks/portalloc` ([00-etcd-contract.md](00-etcd-contract.md)
   §4; txn `version==0` + put-with-lease TTL 15 с, паттерн
   `/pgworker/leader`). Без клэйма два параллельно сеемых кластера
   (per-cluster клэймы друг друга не видят) читают префикс ДО первой
   записи соседа — общей картины занятости нет, оба выбирают одинаковые
   порты (контейнеры второго кластера в Created с «port is already
   allocated»). Не взял клэйм — процесс возвращает InProgress (следующий тик
   ~5 с повторяет); смерть держателя гасит lease ≤15 с — takeover без
   оператора. Полностью закреплённый portalloc (rebuild, ранний выход без
   записи) клэйма не требует. Касается всех точек довыделения: provision P1,
   add-shard, adoption-реплан.
3. Итог — план размещения (node → host + порты); он же вход для генерации
   конфигов (DSN multi-host по нодам шарда — §1).
4. Заявки ресурсов `/service/<scope>/request_{cpu,mem}` — ОБЯЗАТЕЛЬНАЯ
   информация о ноде (панель пишет при создании шарда,
   [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.5).
   Лимиты контейнера: plain — `HostConfig.NanoCPUs` (cores × 10⁹) /
   `HostConfig.Memory` (байты; суффиксы панели: `K/M/G/T` десятичные,
   `Ki/Mi/Gi/Ti` двоичные); swarm — `TaskTemplate.Resources.Limits` (те же
   поля) — И входы PGTune-расчёта параметров ноды. Отсутствие/нечитаемость
   заявки — фейл фазы тика (journal-ошибка + транзиент-ретрай): без заявки
   воркер НЕ знает размера ноды и НЕ выдумывает дефолтов. `request_disk`
   примитива лимита в docker не имеет — игнорируется (квоты volume —
   roadmap).
5. **Advertised-имя хоста** (`PgWorker:Docker:AdvertisedHost`,
   advertised-правило [kafkaworker/01-placement.md](../kafkaworker/01-placement.md),
   прецедент KafkaWorker:AdvertisedClientHost): адреса нод в etcd
   (portalloc/dsn) обязаны быть резолвимы КЛИЕНТАМИ записей — панелью.
   Внутреннее имя docker-хоста (напр. `local`) резолвится только
   контейнерами воркеров (`extra_hosts local:host-gateway`) — панельные
   пробы уходили в DNS-таймаут. Когда задано (single-host/tunnel-
   развёртывания, стенд — `host.docker.internal`; валидация старта: Plain +
   ровно один хост в Hosts), всё, что драйвер отдаёт наружу — плановые
   хосты, busy-кортежи, факты инспекции КАНОНИЧЕСКИХ нод (pgw-<C>-*) —
   несёт advertised-имя: один namespace адресов с записями portalloc.
   Внешние находки усыновления (object) advertised не получают (адресация
   операторская, R9-симметрия). AD2'-инвариант сам мигрирует легаси-записи:
   факт инспекции (advertised) ≠ запись (внутреннее имя) → portalloc/dsn
   переписаны тиком. Мастер-ключ `host:<doorman>` пишут lease-демоны нод с
   env-хостом КОНТЕЙНЕРА — хост-часть может расходиться с portalloc
   (контейнеры, созданные до advertised-режима): сверка P11 и резолв мастера
   считают ключ корректным по doorman-порту (уникален per-node: пулер
   обязателен на каждой ноде — ключ дискриминантен по ноде всегда),
   хост-часть информативна — войны писателей нет. Факт смены мастера
   потребители читают из проб Patroni (`/primary` по patroni-портам
   portalloc) или `pg_is_in_recovery`, а не из сравнения значений ключа
   ([synchronization/README.md](../synchronization/README.md) S4).
6. **SAN-канон серта REST-эндпоинта ноды** (§1): DNS `<n>` (короткий alias
   в `pgw-net-<C>`) + DNS `pgw-<C>-<X>-<n>` (полное docker-имя) + IP
   `127.0.0.1` (loopback lease-скрипта). Advertised-хост в SAN не входит —
   клиенты advertised-адресов верифицируют цепочку без hostname-проверки
   ([pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4).

### 5.1. Сети

Ноды кластера подключаются к per-cluster docker-сети `pgw-net-<C>`
(alias = имя ноды): Patroni-репликация и резолв мастера — по внутренним
адресам (в default bridge hostname-резолва нет). Per-cluster (как
`kfw-net-<C>` у KafkaWorker, [kafkaworker/README.md](../kafkaworker/README.md);
согласовано с advertised-правилами
[pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2 — «per-cluster сети
воркера»): единая сеть делала DNS-зону общей для всех кластеров
docker-хоста — перекрёстная видимость алиасов чужих кластеров/тестов,
сетевая изоляция контуров отсутствовала; per-cluster сеть даёт её по
построению (уникальное имя кластера ⇒ уникальная DNS-зона). Сеть создаётся
движком при provisioning (идемпотентно, 409 already exists = успех) и
удаляется при полном демонтаже кластера (пул subnet'ов конечен — сиротские
сети не копим). Приёмник WAL живёт процессом внутри контейнера ноды
([backups/02-wal-archive.md](../backups/02-wal-archive.md)) — отдельной сетевой сущности не
образует, источник репликации для него — `127.0.0.1:5432` своей ноды.

Дополнительно, при заданном `PgWorker:Docker:ScrapeNetwork` (имя docker-сети
контура мониторинга поставки) канонические ноды Ensure-attach'атся движком
к этой сети (поверх `pgw-net-<C>`, идемпотентно по inspect) — сеть несёт и
скрейп метрик приёмника WAL (`:8080` внутри контейнера ноды,
[metrics/README.md](../metrics/02-dictionary.md) §8). Сеть контура движком НЕ создаётся и
НЕ удаляется (владелец — поставка/сценарий); отсутствует при включённом
ключе — fail-fast провижининга с diagnose. Усыновлённые контейнеры
(`object`) не подключаются (R9 — чужой контейнер движок не трогает).
Назначение сети — скрейп Prometheus по сетевым адресам
([metrics/README.md](../metrics/05-storage.md) §4); клиентская адресация
(dsn/endpoints/пробы) сетью НЕ меняется (advertised-правило §5 п.5).

**Сеть контура (режим поставки, опционально)**: настройка
`PgWorker:Docker:ContourNetwork` (env `PGW_CONTOUR_NETWORK`) — имя ВНЕШНЕЙ
сети контура, принадлежащей поставке/сценарию (стенд — одна стендовая сеть
всего контура; E2E-класс — своя изолированная сеть). Задана → ноды кластера
создаются в этой одной сети; `pgw-net-<C>` не создаётся и не удаляется
движком — адресация внутри контура по именам сети (API приёмника —
`alias:8080` по имени ноды; S3 — по имени сети поставки). Пусто
(прод/умолчание) — per-cluster `pgw-net-<C>`, как выше. Сеть контура движок
не создаёт и не удаляет; ensure-подключение идемпотентно (сеть уже
существует у поставки, 409 = успех). Не поддерживается в Mode=Swarm
(fail-fast старта). Scrape-attach (`Docker:ScrapeNetwork`) — без изменений:
на стенде указывает на ту же сеть контура (attach — no-op поверх).

**Инвариант общей сети воркера и нод**: контейнеры воркера и нод ВСЕГДА
находятся в одной сети — контур (сеть контура поставки) либо прод
docker/swarm (per-cluster `pgw-net-<C>`/overlay — воркер в ней по
построению поставки). Этим построением адресация HTTP API приёмника
`alias:8080` по имени ноды работает в обоих режимах
([communications/02-channel-registry.md](../communications/02-channel-registry.md) №22;
[backups/01-full-backup.md](../backups/01-full-backup.md)/[backups/03-restore.md](../backups/03-restore.md)) — приёмник `:8080` наружу хоста
НЕ публикуется. Недостижимость alias (DNS/отказ/5xx/таймаут) — transient
тика процесса воркера с journal-диагностикой (факт с адресом), статус не
портится, краш тика недопустим.

Сам PgWorker — контейнер с примонтированным `/var/run/docker.sock` (plain на
одном хосте / swarm manager), volume под снапшоты etcd, env-секреты
([08-operability.md](08-operability.md) §5). Масштабирование — N реплик:
координацию разбирает etcd ([00-etcd-contract.md](00-etcd-contract.md)).

Деплой-рецепт поставки ≥2 инстансов: `deploy/docker-compose.yml` поднимает
**два** инстанса по умолчанию — SPOF надзора устраняется дефолтом, а не
опцией. Каждый инстанс — отдельный сервис того же образа с уникальным
хост-портом публикации API (`PGW_API_HOST_PORT` / `PGW_API_HOST_PORT2`, ряд
8080/8083) и уникальным `Api:AdvertiseUrl` (панель резолвит API по
lease-ключу `/pgworker/api/<InstanceId>`: одинаковые URL двух инстансов
гасили бы оба ключа при смерти контейнера-владельца порта). Уникальность
инстанса — случайный `InstanceId` при старте
([00-etcd-contract.md](00-etcd-contract.md) §4), env-идентификация
(`PGW_INSTANCE_ID` и т.п.) не нужна. Общие per-install ресурсы на оба
инстанса: docker.sock, TLS-том `pgw-api-tls:ro` и **общий том снапшотов**
`pgw-snapshots` (глобальное лидерство снапшотов мигрирует между инстансами —
свой том у каждого терял бы снапшоты при миграции лидера); env-секреты
([08-operability.md](08-operability.md) §5) наследуются идентично. Смерть
одного контейнера: клэймы/дискавери мигрируют второму ≤ TTL 15 с,
надзор (provisioning/rebuild/ротации/бэкапы) не прерывается.

## 6. Где это используется

- Создание кластера — [A. ProvisioningProcess](processes/a-provisioning.md).
- Добавление шарда — [G. AddShardProcess](processes/g-add-shard.md).
- Надзор и пересоздание ноды —
  [C. NodeSupervisor](processes/c-node-supervisor.md).
- Усыновление Active-кластера —
  [J. AdoptionProcess](processes/j-adoption.md).
