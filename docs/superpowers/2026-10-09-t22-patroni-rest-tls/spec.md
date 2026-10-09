# t22-patroni-rest-tls — TLS + per-cluster basic-auth на Patroni REST :8008

**Фаза 1 dev-flow (spec, ревизия 3 — ротация `rest_password` в процессе I
ClusterSecretRotator; семантика лидера в миграции канонизирована по
транспорту: soft — TLS-ноды, легаси-http лидер — жёсткий снос с выборами).**
Roadmap: `arch/roadmap/reliability.md` (тег `t22-patroni-rest-tls`, P4 —
долговременные улучшения, характеристика N; формулировка: «TLS/аутентификация
Patroni REST :8008: открытый HTTP во внутренней зоне кластера (arch/13);
access-логи идентифицируют панель только User-Agent'ом»). Сводная строка —
`arch/roadmap/reliability-report.md`, раздел «Осталось» (P4/N, «TLS/
аутентификация Patroni REST :8008»); там же в сводке характеристики N —
«Patroni REST без TLS/аутентификации».

Решения пользователя по развилкам (зафиксированы до дизайна, не пересматриваются):

1. **TLS + basic-auth**: HTTPS на Patroni REST `:8008` (серверные серты нод) +
   `restapi.authentication` — ЕДИНАЯ пара Patroni на кластер (одна на все ноды,
   включая межнодовые REST-вызовы; пер-клиентские учётки нативно невозможны).
2. **CA — существующий per-install API-CA `kfw-install-ca`**
   (`deploy/tls/gen.sh`): воркер выпускает серверные серты REST-эндпоинтов
   нод при создании ноды; SAN = alias сети контура (`pgw-metrics`) + имя ноды.
   Один корень доверия на установку.
3. **Креды basic-auth — per-cluster секрет**: воркер генерирует и хранит рядом
   с прочими секретами кластера (etcd, канон `ClusterSecretEnsurer`);
   доставку панели/Prometheus проектирует спека (см. §2 п.1: доставлять
   нечего — им нужен только CA).
4. **Эмуляторы стенда `hc*`** — серверная сторона переводится на HTTPS тем же
   CA (серт `hc` в `gen.sh`); basic-auth у эмуляторов нет (мутационных
   эндпоинтов нет — только GET-семантика Patroni).

## 1. Цель

Patroni REST `:8008` нод PgWorker перестаёт быть открытым HTTP: транспорт —
TLS с серверными сертификатами per-install API-CA, мутационная грань —
basic-auth одной пары на кластер. Закрываются:

- **пассивное чтение внутренней зоны**: `:8008` — последний служебный канал
  контура без шифрования (arch/13 §4 «внутренняя зона кластера»): топология
  (`/cluster`), конфиг (`/config`) и метрики (`/metrics`) читаются слушателем
  сети без единого пакета от себя;
- **анонимные мутации**: `POST /switchover`, `PATCH /config` и прочие
  unsafe-эндпоинты выполняются от любого клиента сети без аутентификации;
- **строка модели угроз** arch/13 §6 «перехват сети» пополняется закрытым
  `:8008`.

Клиенты REST `:8008` после задачи (полный список, других нет):

| Клиент | Эндпоинты | Транспорт | Аутентификация |
|---|---|---|---|
| PgWorker (`ShardProbe`) | GET `/cluster`, `/patroni`, `/primary`, `/config`; PATCH `/config`; POST `/switchover` | HTTPS, доверие `kfw-install-ca`, hostname-проверка выключена | basic-auth per-cluster — только мутации |
| Patroni-ноды друг друга | межнодовые вызовы по `connect_address` (`fetch_node_status`, switchover) | HTTPS (свой серт + `restapi.cafile` соседей, строгая верификация) | basic-auth — пару наносит сам Patroni |
| `master-lease.py` ноды (P11, callback `on_start`) | GET `/primary` на `127.0.0.1:8008` (loopback) | HTTPS, верификация по ca-файлу ноды (IP `127.0.0.1` в SAN) | не требуется (GET) |
| Панель (`PatroniRestProbe`) | GET `/cluster` | HTTPS, доверие `/tls-workers/ca.pem` | не требуется (GET); User-Agent `AdminPanel` остаётся |
| Prometheus (`patroni-nodes`) | GET `/metrics` | HTTPS, `tls_config.ca_file` | не требуется (GET) |
| Эмуляторы стенда (`hc*`) | серверная сторона проб панели/скрейпа | HTTPS (серт `hc` из того же CA) | — |

## 2. Установленные факты (Patroni 4.1 / Spilo 4.1-p2 / код репо)

Образ нод — `ghcr.io/zalando/spilo-18:4.1-p2` (`docker/node/Dockerfile`,
Patroni 4.1). Факты Patroni/Spilo проверены по исходникам (api.py, request.py,
config.py Patroni; `configure_spilo.py` Spilo); факты репо — сверены по коду
worktree. Спека опирается на них как на аксиомы.

1. **Basic-auth (`@check_access`) защищает ТОЛЬКО мутационные эндпоинты**:
   PATCH/PUT `/config`, POST `/reload`, `/restart`, `/sigterm`, `/failover`,
   `/reinitialize`, DELETE `/restart` и др.; `POST /switchover` делегирует
   декорированному `do_POST_failover` — тот же эффект (без кредов 401). Все
   GET (`/cluster`, `/config`, `/metrics`, `/patroni`, `/primary`, `/health`,
   `/replica`, `/liveness`, `/readiness`, `/history`) открыты и при включённой
   `restapi.authentication` — семантика health-check HAProxy. **Следствие**:
   панели, Prometheus и lease-скрипту ноды креды функционально НЕ нужны — их
   канал доставки это только CA: панели и Prometheus он уже смонтирован
   (`deploy/tls` bind → `/tls-workers` у панели, `/tls` у as-prometheus), ноде
   доставляет воркер (§5.2). Креды нужны одному PgWorker (PATCH `/config`,
   POST `/switchover`) и самому Patroni (межнодовые вызовы).
2. **Spilo нативно поднимает REST-TLS из env**: при заданных
   `SSL_RESTAPI_CERTIFICATE` + `SSL_RESTAPI_PRIVATE_KEY` (PEM-содержимое с
   РЕАЛЬНЫМИ переносами строк — Spilo пишет значение в файл буквально,
   экранирование `\n` как у KafkaWorker-properties не нужно) configure_spilo.py
   материализует их в файлы `<RW_DIR=/run>/certs/rest-api-server.crt` /
   `rest-api-server.key` (chmod 600), а `SSL_RESTAPI_CA` — в
   `rest-api-ca.crt`; шаблон `patroni.yml` подставляет
   `restapi.certfile/keyfile/cafile`. Кастомный entrypoint-код для Patroni не
   требуется.
3. **`SPILO_CONFIGURATION` побеждает шаблон** (`deep_update(user_config,
   template)`): секция `restapi` из SPILO_CONFIGURATION мержится поверх
   шаблонной — в ней задаём `connect_address` (DNS-именем ноды, не IP
   контейнера) и `authentication {username, password}`. `listen` не
   переопределяем (шаблонный `:8008` корректен).
4. **Межнодовые REST-вызовы Patroni** несут basic-auth из
   `restapi.authentication` автоматически и верифицируют серверные серты
   соседей строго (CERT_REQUIRED по `restapi.cafile`, hostname проверяется).
   Отсюда: (а) пара обязана быть ЕДИНОЙ на кластер — иначе ноды не
   аутентифицируются друг с другом; (б) SAN серта ноды обязан покрывать её
   `connect_address`/`api_url`; (в) `SSL_RESTAPI_CA` нужен ноде даже при
   отсутствии `verify_client` — им пользуется межнодовый КЛИЕНТ.
5. **`restapi.authentication` — локальный конфиг ноды** (patroni.yml), не
   динамический DCS-параметр: смена пары применяется пересозданием контейнера
   ноды, плавной ротации нет.
6. **Loopback-клиент внутри ноды — lease-скрипт мастер-ключа (P11)**:
   `docker/node/master-lease.py` в callback `on_start` (Patroni не передаёт
   роль аргументами) узнаёт её запросом `GET /primary` локального Patroni —
   сегодня голый `http://127.0.0.1:8008/primary` (master-lease.py:39). При
   TLS-only REST без переделки скрипта ветка всегда выходит в `None` — демон
   мастер-ключа не стартует после рестарта мастера, основной писатель P11
   молча деградирует до одного контура (MasterKeyReconciler). Перевод скрипта
   на https — часть задачи (§5.2); IP `127.0.0.1` входит в SAN серта ноды.
7. **Ensure-путь надзора НЕ converge'ит env живых контейнеров**:
   `EnsureDeclaredNodesAsync` (NodeSupervisor) делает `continue`, как только
   docker-объект ноды существует, — `EnsureNodeAsync` для живого контейнера
   надзором не вызывается вовсе (его ветки — отсутствующий объект,
   port-mismatch, rebuild/TO_RECREATE). Миграция живых HTTP-нод требует
   ОТДЕЛЬНОГО шага надзора (§5.5).
8. `allowlist`/`allowlist_include_members` (фильтр источников по IP) и
   `verify_client` (mTLS-клиенты REST) — НЕ используем: сегментация контуров
   уже обеспечена docker-сетями (`pgw-net-<C>`, `pgw-metrics`), грань клиента
   закрывает basic-auth (решение 1); mTLS — сверх решения. `ctl.*`
   (patronictl) — не настраиваем: интерактивный инструмент оператора, вне
   автоматики поставки.

## 3. Принципы

- **arch-first**: дельты `arch/13/14/18/02/adminpanel` (§4) — ДО кода.
- **Один корень доверия на установку**: серверные серты REST-эндпоинтов нод
  выпускает воркер из per-install API-CA `kfw-install-ca` (того же, что
  подписывает mTLS API воркеров) — все клиенты верифицируют цепочку к одному
  `ca.pem`; второй корень не появляется.
- **MULTI-HOST**: серт ноды — НЕ состояние. SAN детерминирован именем ноды;
  rebuild ноды на любом docker-хосте выпускает серт заново живым воркером (кеш
  процесса, etcd-хранения сертов нет — канон серверных сертов KafkaWorker-нод);
  ноды получают только PEM в env через Engine API, CA-ключ хостам нод НЕ
  доставляется и там не нужен. Per-cluster REST-креды — в реплицированном etcd
  (переживают гибель любого docker-хоста целиком). **Что теряется при
  отключении машины X**: хост ноды — ничего (серт перевыпустит живой воркер,
  креды в etcd); deploy-хост (оба инстанса воркера + том `pgw-api-tls` с
  `ca.key`) — возможность выпуска НОВЫХ сертов до восстановления пакета из
  бэкапа: серты живых нод остаются валидными (верификация по ca.pem),
  наблюдение и живые кластеры не страдают. **Граница `ca.key` честно
  фиксируется** (§4 arch/14 §9, R16): ключ живёт в именованном томе поставки
  deploy-хоста и не реплицируется; `gen.sh` на пустом месте поднимет НОВЫЙ CA —
  доверие rebuild-сертов сломается, поэтому бэкап пакета `deploy/tls` (вкл.
  `ca.key`) и восстановление из него после гибели deploy-хоста — обязательный
  шаг аварийного восстановления (runbook; катастрофа-кейс, не штатная
  операция). На стенде панель/as-prometheus монтируют `deploy/tls` целиком
  вместе с `ca.key` (bind ro, домашний контур — принято).
- **Шифрование обязательно, hostname-проверка — нет** (канон doorman P17,
  arch/13 §4 «require, не verify-full»): клиент REST верифицирует ЦЕПОЧКУ
  против `kfw-install-ca`, но не сверяет hostname с адресом подключения —
  адрес ноды приходит из реплицированного контракта (portalloc/HostMap),
  доверие адресу обеспечивает канал etcd. Исключения, где проверка строгая:
  сетевые таргеты Prometheus `alias:8008` (имя = SAN серта) и loopback-клиент
  ноды (IP-проверка по SAN, §5.2); межнодовые вызовы Patroni строги самим
  Patroni (факт §2 п.4).
- **SAN серта REST-эндпоинта ноды** = DNS `<n>` (короткий alias в
  `pgw-net-<C>`) + DNS `pgw-<C>-<X>-<n>` (полное docker-имя: alias в
  per-cluster сети, имя контейнера в `pgw-metrics` — оно же `connect_address`
  межнодовых вызовов) + IP `127.0.0.1` (loopback lease-скрипта). Advertised
  host (`host.docker.internal`) в SAN НЕ входит — клиенты advertised-адресов
  ходят без hostname-проверки (принцип выше).
- **Basic-auth — ровно там, где он у Patroni есть**: креды снимает один
  потребитель-мутатор (воркер); панель/Prometheus остаются на открытых
  GET-эндпоинтах за TLS (факт §2 п.1). Двойных веток http/https у клиентов
  REST нет: схема HTTPS-only у всех.
- **Идемпотентность поставки**: `gen.sh` остаётся идемпотентным пофайлово
  (guard `[ -f hc.crt ] ||`); 00-up/90-down симметричны (сертификаты —
  статические ассеты, сирот docker-объектов не порождают).

## 4. Дельты arch/ (по разделам, до кода)

| Документ | Раздел | Правка |
|---|---|---|
| `arch/13` | §2 матрица | Строка «HAProxy → Patroni REST 8008 HTTP без аутентификации» → «HTTPS (серт kfw-install-ca); health-GET без basic-auth (семантика Patroni); unsafe-эндпоинты — basic-auth per-cluster». Строка «сверяющий демон (P11)» → «PgWorker → Patroni REST :8008: HTTPS, basic-auth per-cluster (мутации)». Строка «мониторинг → 8008 HTTP scrape» → «Prometheus → Patroni :8008 HTTPS scrape (ca.pem), сеть pgw-metrics». Новые строки: «панель → Patroni REST :8008 HTTPS (GET /cluster, ca.pem)»; «Patroni-нода → Patroni-нода :8008 HTTPS (межнодовые, basic-auth кластера)»; «lease-скрипт ноды → 127.0.0.1:8008 HTTPS (ca-файл ноды, P11)» |
| `arch/13` | §2 матрица (синкеры) | Строка «HAProxy-синкеры (динамические адреса) … etcd watch + `GET /whoami`» — транспорт HTTPS (доверие ca.pem, health-GET без basic-auth): синкеры — клиенты `:8008` по TLS, как все (согласовано с принципом §3 п.4). Реализация синкеров (фронтенд-слой) — вне скоупа t22, матрица канонизирует целевой транспорт |
| `arch/13` | §3 принцип 4 | Клиенты `:8008` — прежний состав (HAProxy, сверяющий демон (= PgWorker), HAProxy-синкеры, админка) плюс панель, Prometheus, межнодовые вызовы Patroni и loopback lease-скрипт ноды (P11) — всё по TLS |
| `arch/13` | §4 таблица TLS | Строка «Patroni `:8008`, HAProxy `:7000` — без TLS» → «Patroni `:8008` — TLS: серверные серты per-install CA (SAN — arch/14 §2.4), unsafe-эндпоинты + basic-auth per-cluster; верификация клиентами — цепочка к CA без hostname-проверки (канон P17)» |
| `arch/13` | §6 модель угроз | Строка «перехват сети»: `:8008` переносится в «зашифровано» (HTTPS; мутационная грань — basic-auth) |
| `arch/14` | §2.1 | Механика REST-TLS ноды: env `SSL_RESTAPI_CERTIFICATE`/`SSL_RESTAPI_PRIVATE_KEY`/`SSL_RESTAPI_CA` (PEM, выпускает воркер из per-install CA при сборке env; материализация файлов и `restapi.certfile/keyfile/cafile` — штатный Spilo); секция `restapi` SPILO_CONFIGURATION: `connect_address: <полное имя ноды>:8008` (DNS per-cluster сети, перекрывает шаблонный IP; api_url в DCS = DNS из SAN) + `authentication {username, password}` — per-cluster пара; серт ноды — не etcd-состояние (генерация + кеш процесса, канон серверных сертов KafkaWorker); lease-скрипт мастер-ключа (P11) в `on_start` опрашивает `https://127.0.0.1:8008/primary` с верификацией по ca-файлу ноды (entrypoint материализует PEM из env в файл и пишет путь строкой в pgw-node.env — PEM в KEY=VALUE-файл не переносится) |
| `arch/14` | §2.4 | SAN-канон серта REST-эндпоинта: DNS `<n>` + DNS `pgw-<C>-<X>-<n>` + IP `127.0.0.1` (§3 спеки); advertised-хост в SAN не входит — клиенты advertised-адресов верифицируют цепочку без hostname-проверки |
| `arch/14` | §4 группа 1 | Новый per-cluster секрет `rest_password` (etcd, ensure txn put-if-absent, `AppSecretGenerator`, 32 симв; username — константа `patroni`, отдельного user-ключа нет: Patroni требует пару username+password в конфиге каждой ноды). Ротация — процесс I (заявка панели, применение — rolling-пересоздание нод, §5 I). Экспозиция — класс `app_password`/`mover_password` (etcd без per-key ACL; парсеры панели его не разбирают, в UI не попадает); вынос секретов из etcd — `t02-external-secret-manager` (pgworker-трек), класс экспозиции здесь не расширяется |
| `arch/14` | §4 группа 3 | Новые env `PGW_REST_TLS_CA`/`PGW_REST_TLS_CA_KEY` (`…_PATH`-варианты из TLS-тома `/tls`): per-install API-CA и его ключ для выпуска серверных сертов REST-эндпоинтов нод; отсутствие/неполнота пары — fail-fast валидации старта воркера (HTTP-режим провижининга не существует; WAF-фикстуры передают тестовый CA — двойной семантики нет) |
| `arch/14` | §5 A | P2.1: сборка env ноды дополнительно несёт REST-TLS-материал (`SSL_RESTAPI_*` PEM + секция `restapi` SPILO_CONFIGURATION с `connect_address`/`authentication` из per-cluster `rest_password`) |
| `arch/14` | §5 C | Пробы/сверки надзора — HTTPS к `host:patroni-port` portalloc с верификацией цепочки к kfw-install-ca без hostname-проверки; PATCH `/config` и POST `/switchover` несут `Authorization: Basic` per-cluster (GET — без заголовка). НОВЫЙ общий шаг пересоздания нод надзора с ДВУМЯ входами: (а) REST-TLS-конвергенция живых канонических нод — инспекция env существующего контейнера (не более одного пересоздания на тик надзора кластера; volume сохраняется; живой TLS-лидер — сначала graceful-switchover, затем снос следующим тиком — семантика TO_RECREATE-soft; легаси-http лидер — soft транспортно недостижим (https-проба отказывает) — жёсткий снос с failover-выборами Patroni и паузой записи на окно выборов, failover-свидетель — живая по DCS-членству реплика: осознанная граница однократной миграции, не штатный режим); (б) заказ rolling-ротации REST-пары от процесса I (§5 I) — та же механика пересоздания, критерий выбора ноды — hash пары в env (ротация всегда soft: её ноды — TLS). Детали — §5.5 спеки, в arch — одним абзацем семантики |
| `arch/14` | §5 I | Процесс ротации пополняется четвёртым секретом — `rest_password` (заявка та же, ставит панель). R1 — ensure четвёрки (OLD). R2 — NEW×4: SQL-тройка — как сегодня (ALTER ROLE на мастере каждого dsn-шарда); `rest_password` — ROLLING-пересоздание нод кластера общим шагом надзора (§5 C: ≤1 нода/тик, лидер — soft-switchover со сносом следующим тиком, volume сохраняется, гварды кворума/restore/TO_REMOVE; шард без dsn — skip: его ноды создадутся с новой парой после R3). Пара NEW «в полёте» фиксируется полем `rest_pending` журнала работы (`/pgworker/work/<C>`, op=rotate) — повтор тиками ПРОДОЛЖАЕТ проход с той же парой (регенерация между тиками безопасна, но обнуляла бы прогресс rolling); прогресс по нодам — факт env (`PGW_REST_PASSWORD_HASH`, инспекция) — takeover-безопасен. R3 — ОДНА txn пополняется compare `rest_password`==OLD и put NEW (коммит после применения на ВСЕХ нодах — канон «пока R3 не прошёл, креды в etcd НЕ меняются» сохраняется); сброс `rest_pending` — фазовой записью done. Проигрыш compare — re-read и повтор прохода со свежей парой. Окно rolling (от первого пересоздания до R3): ноды разных поколений несут разные пары (dual-auth у Patroni нет) → межнодовые REST-вызовы между поколениями получают 401 — HA-инициации Patroni (fetch_node_status/switchover) деградированы; запись/репликация/DCS-heartbeat/master-ключ НЕ затронуты (мимо REST). Гварды окна: воркер не инициирует switchover и PATCH `/config` кластера по REST до R3 (мутации ретраются после txn); ускорение failover мёртвого лидера — DCS-ключ, не REST, работает. Сборка env нод в окне берёт пару из `rest_pending` (эффективная пара: pending ?? ключ). Длительность окна — единицы минут (2–3 ноды × пересоздание со своим PGDATA + тик soft-switchover), записи не рвёт |
| `arch/14` | §1.1 API-таблица | Строка `POST /api/clusters/{c}/secrets/rotate`: состав — app + bucket_admin + mover + `rest_password` (формат заявки/протокол — без изменений, 02 §9.8; состав ротируемого — домен воркера) |
| `arch/14` | §3.3 | Строка `/pgworker/rotations/<C>`: заявка на ротацию per-cluster секретов ВСЕГО кластера — app, bucket_mover, bucket_admin И `rest_password`; исполнение §5 I (для rest — rolling-пересоздание + txn-коммит) |
| `arch/14` | §8 | `PgWorker:Docker:RestTls {CaPem\|CaPath, CaKeyPem\|CaKeyPath}` — per-install CA + ключ выпуска сертов нод; env-имена `PGW_REST_TLS_*`; оба обязательны (fail-fast старта, симметрия запрета `Pgtune:DbType=desktop`); WAF-фикстуры — тестовый CA |
| `arch/14` | §9 | Новый риск R16: компрометация `ca.key` = выпуск серверных сертов REST/API от имени установки. Митигация: ключ в TLS-томе поставки (ro), права 600, в etcd/registry не попадает; на стенде панель/as-prometheus монтируют `deploy/tls` целиком (bind ro — домашний контур, принято; точечное монтирование — опция поставки); потеря deploy-хоста = потеря `ca.key` — обязательный бэкап пакета `deploy/tls` (runbook), серты живых нод не инвалидируются |
| `arch/18` | §2.5 | Скрейп реальных нод — `scheme: https` + `tls_config.ca_file`; сетевые таргеты `alias:8008` верифицируются полностью (имя = SAN). Advertised-ветка (запись без alias) — деградационная зона шире прежней: не только достижимость, но и TLS-верификация имени по advertised-хосту не гарантируется (SAN его не несёт) — таргет уходит в down; в поставке таких таргетов нет (демо-кластер несёт static-джобу `patroni`) |
| `arch/18` | §5.2 | Джоба `patroni-nodes`: `scheme: https` + `tls_config {ca_file: /tls/ca.pem}`. Джоба `patroni` (эмуляторы `hc*`): `scheme: https` + тот же `ca_file` (серт эмуляторов — из per-install CA, gen.sh) |
| `arch/18` | §5.4 | Механика file_sd/единой сети — без изменений; фиксируется: REST-скрейп нод и эмуляторов — по TLS одного per-install CA |
| `arch/18` | §6 | Приёмка: docker-E2E REST-TLS (§5.10) + https-ассерты E2E file_sd; чек 65 — patroni/patroni-nodes по https |
| `arch/02` | §2 таблица портов | `8008/tcp`: «Patroni REST API — HTTPS (серт kfw-install-ca; unsafe-эндпоинты — basic-auth per-cluster)» |
| `arch/adminpanel/02` | §6.1 | Проба: `GET https://<host>:<port>/cluster` — TLS с доверием per-install CA (bind `deploy/tls` → `/tls-workers/ca.pem`, уже смонтирован; env `WORKERS_PANEL_TLS_SERVER_CA_PATH`), hostname-проверка не выполняется (верификация цепочки — канон P17-стиль, прецедент CustomRootTrust). Basic-auth не требуется (GET-эндпоинт вне зоны authentication); User-Agent `AdminPanel` остаётся (идентификация панели в журналах) |
| `arch/adminpanel/04` | §1 таблица hc* | Эмуляторы слушают HTTPS `:8008` (серт `hc.crt` из per-install CA, SAN `hc1a,hc1b,hc2a,hc2b` + `127.0.0.1` — host-публикации 8011–8022 для чеков curl); HostMap-значения схемы адресов не меняют (`hc1a:8008` и т.д.) |

Сопровождающие документы: `deploy/.env.example` — комментарий раздела
TLS-пакета (наполнение deploy-тома теперь включает `ca.key` — выпуск сертов
нод); `docs/runbook.md` — новый раздел «REST-TLS Patroni»: диагностика проб/
скрейпа при TLS-отказе (включая окно REST-ротации: межпоколенческие 401
самозакрываются txn-коммитом), бэкап/восстановление пакета `deploy/tls` с
явным предупреждением: `gen.sh` на пустом месте поднимет НОВЫЙ CA и сломает
доверие rebuild-сертов — восстанавливать ДО новых выпусков.

## 5. Дизайн по компонентам

### 5.1. PKI REST-сертов нод (PgWorker.Core)

- **Опции** `PgWorker:Docker:RestTls {CaPem|CaPath, CaKeyPem|CaKeyPath}`
  (`DockerOptions`; env `PGW_REST_TLS_CA[_PATH]` /
  `PGW_REST_TLS_CA_KEY[_PATH]` через `DockerEnvBindings`; deploy-compose
  задаёт `/tls/ca.pem` + `/tls/ca.key`). Валидация старта воркера: оба поля
  обязательны, PEM разбираются на старте (fail-fast с diagnose при
  отсутствии/неполноте/битом PEM — поставка без TLS-пакета считается битой;
  HTTP-режима не существует). Интеграционные WAF-фикстуры воркера
  (`PgWorkerApiFactory` и производные) передают in-memory тестовый
  самоподписанный CA (RSA-2048, .NET) через те же env — отдельного
  «тестового режима» и двойной семантики нет; fail-fast не роняет
  интеграционные серии.
- **Выпуск** — новый `RestPki` (`PgWorker.Core/Templates/`; порт `ClusterPki`
  KafkaWorker): `CertificateRequest` .NET, RSA-2048, EKU ServerAuth, CN =
  полное имя ноды `pgw-<C>-<X>-<n>`, SAN = DNS `<n>` + DNS `pgw-<C>-<X>-<n>`
  + IP `127.0.0.1`, срок 10 лет с зажимом NotAfter в NotAfter CA. Разбор
  PEM-CA и ключа — `TryParse`-обёртки (паттерн `ClusterPki`).
- **Кеш процесса** `RestCertificateCache` (DI-синглтон, порт
  `BrokerCertificateCache`): серт генерируется один раз на (cluster, node,
  hash CA-ключа) в рамках жизни процесса — повторные сборки env
  (надзор/ensure/rebuild) дают тот же PEM; смена CA-ключа — новый серт.
  etcd-хранения сертов нет (принцип §3 MULTI-HOST).
- **Формат в env** — PEM с реальными переносами строк (факт §2 п.2).
- **Доставляется в драйвер**: `PlainClusterDriver`/`SwarmClusterDriver`
  получают `RestCertificateCache` (как `scrapeNetwork` — параметром из
  Program.cs); `BuildSpec` выпускает серт ноды и добавляет REST-TLS-env.

### 5.2. Нода: env, SPILO_CONFIGURATION, entrypoint, lease-скрипт

- `SpiloEnvBuilder.Build` получает REST-TLS-вход (PEM серта/ключа/CA ноды —
  из `RestCertificateCache`, per-cluster `rest_password`) и добавляет:
  - `SSL_RESTAPI_CERTIFICATE` = PEM серта ноды;
  - `SSL_RESTAPI_PRIVATE_KEY` = PEM ключа (PKCS#8);
  - `SSL_RESTAPI_CA` = PEM `ca.pem` (→ `restapi.cafile`: серверу для
    клиентских сертов он не нужен — `verify_client` не задан; нужен
    межнодовому КЛИЕНТУ Patroni, факт §2 п.4);
  - в `SPILO_CONFIGURATION` — секция (user-конфиг побеждает шаблон,
    §2 п.3):

    ```yaml
    restapi:
      connect_address: "<pgw-C-X-n>:8008"   # DNS per-cluster сети; из SAN
      authentication:
        username: patroni
        password: "<per-cluster rest_password>"
    ```

    `connect_address` замещает шаблонный IP контейнера (недетерминирован):
    api_url в DCS становится DNS-именем — межнодовые вызовы идут по имени,
    резолвимому в `pgw-net-<C>` (полное имя — NetworkAlias per-cluster сети;
    оно же — имя контейнера в `pgw-metrics`), и верифицируются против SAN.
- `ClusterDriver.BuildSpec` прокидывает материал поверх `SpiloEnvBuilder`
  (симметрично `PGW_NODE_HOST`); порт-карта не меняется (`8008 →
  addr.Ports.Patroni`): host-публикация остаётся — advertised-клиенты (панель,
  host-воркер E2E) ходят по host-портам, TLS поверх той же публикации.
- Env ноды дополнительно несёт `PGW_REST_PASSWORD_HASH` — короткий sha256
  (hex) ЭФФЕКТИВНОЙ REST-пары, с которой собран контейнер: факт «контейнер
  несёт эту пару» читается инспекцией env без раскрытия секрета (сверка
  прогресса rolling-ротации, takeover-безопасность — §5.5).
- **Loopback-клиент lease-скрипта (P11, урок ревью М1)**:
  `master-lease.py` переводится на `https://127.0.0.1:8008/primary` с
  верификацией серта по ca-файлу (IP `127.0.0.1` — в SAN). Доставка CA
  скрипту: PEM НЕ переносим в `pgw-node.env` (entrypoint пишет строки
  `VAR=…` — многострочный PEM сломал бы KEY=VALUE-файл); вместо этого
  entrypoint (`docker-entrypoint.sh`) материализует `$SSL_RESTAPI_CA` в
  `/home/postgres/pgw-node-ca.pem` (паттерн HAPROXY_CONFIG:
  `printf '%s\n' "$VAR" > file`, права 600, владелец postgres) и дописывает в
  `pgw-node.env` строку-путь `PGW_NODE_CA=/home/postgres/pgw-node-ca.pem`.
  `local_role()` читает путь из env (env-переменные приоритетнее файла —
  механика `load_env_file` уже так работает) и строит ssl-контекст
  (`urllib.request` + `ssl.create_default_context(cafile=…)`). Отсутствие
  `PGW_NODE_CA` — лог-ошибка и `local_role() → None` (как при недоступном
  Patroni): http-ветки в скрипте не остаётся.
- Dockerfile образа не меняется: меняется содержимое уже копируемых файлов
  (master-lease.py, docker-entrypoint.sh).

### 5.3. Per-cluster креды rest_password

- Седьмой ensure-ключ `ClusterSecretEnsurer`:
  `/clusters/<C>/rest_password` — чтение → генерация отсутствующего
  (`AppSecretGenerator`, 32 симв `[A-Za-z0-9]`) → txn put-if-absent → re-read
  (механика шести существующих 1:1). Username — константа `patroni`.
- `ClusterSnapshotParser` разбирает ключ в снапшот кластера (паттерн
  `app_password`); потребители: Provisioning P1.5/AddShard-ensure (env ноды),
  NodeSupervisor (миграция §5.5 + Authorization мутаций), E2E-тесты.
- Чистка: демонтаж D2 сносит префикс `/clusters/<C>/` — ключ уходит сам.
- **Ротация — процесс I** (ClusterSecretRotator): `rest_password` — четвёртый
  секрет заявки `POST /api/clusters/{c}/secrets/rotate` / `/pgworker/rotations/<C>`;
  применение — rolling-пересоздание нод общим шагом надзора (§5.5, вход
  «ротация»), коммит — общей txn процесса I. Фазы, фиксация пары в полёте,
  окно и гварды — §5.5; arch-дельта — §4 (arch/14 §5 I).
- Экспозиция — класс `app_password` (§4 arch/14).

### 5.4. Клиент воркера: ShardProbe (HTTPS + basic-auth на мутациях)

- `BuildUri` — `https://host:patroni-port/…`.
- TLS: именованный HttpClient `patroni` (`Program.cs`, уже существует)
  конфигурируется `SslClientAuthenticationOptions`-callback: доверие
  kfw-install-ca из `PgWorker:Docker:RestTls`, hostname-проверка отключена
  (принцип §3; advertised-хосты вне SAN). Образец — CustomRootTrust-паттерн
  панели (`AdminPanel.Probes`/`Shared.Tls`).
- Basic-auth: `PatchConfigAsync`/`SwitchoverAsync` принимают кред-пару
  (username-константа + `rest_password` кластера из снапшота процесса) и
  ставят заголовок `Authorization: Basic …`; GET-методы заголовок не несут
  (не требуется Patroni, поведение до/после идентично). Вызывающие процессы
  (конвергенция DCS, RecreateMarked soft-switchover, REST-TLS-конвергенция)
  передают пароль из снапшота; после ensure (§5.3) ключ есть у всех
  канонических кластеров.
- Таймауты не меняются (проба 3 с / switchover 15 с — TLS-хендшейк в
  LAN-бюджете).

### 5.5. Общий шаг пересоздания нод надзора: два входа

Пересоздание живого канонического контейнера с сохранением volume — ОДНА
механика с ДВУМЯ входами:

- **вход «конвергенция»** — миграция живых нод, созданных до задачи: они
  держат контейнеры без REST-TLS-env, а ensure-путь надзора живые контейнеры
  не трогает (факт §2 п.7);
- **вход «ротация»** — заказ процесса I (§5.3): rolling-применение NEW-пары
  `rest_password` на нодах кластера.

Шаг живёт в тик-цикле надзора (место — после EnsureDeclared/app_params-миграции,
до проб; гварды как у соседних шагов); общие правила для обоих входов:

- **Гварды домена**: кластер Active; шард с `dsn` (без dsn — домен
  AddShardProcess: новые ноды создаются уже с TLS; для ротации — шард без dsn
  skip'ается и получит новую пару при создании после R3); шард не в TO_REMOVE
  (демонтаж не ускоряем) и не в активном restore (владелец — restore-процесс);
  только канонические ноды (запись portalloc без `object` — R9, чужой
  контейнер не трогаем).
- **Инспекция env живого контейнера**: `DockerContainerInspect.Env` /
  `InspectContainerEnvAsync`. Критерий выбора ноды — по входу: конвергенция —
  отсутствие `SSL_RESTAPI_CERTIFICATE`; ротация — `PGW_REST_PASSWORD_HASH` !=
  hash NEW-пары. Отдельного ключа-версии в etcd НЕ вводим: признак — факт env
  контейнера (декларативная сверка «факт над записью», takeover-безопасна);
  после пересоздания признак исчезает — вход становится no-op (прошёл).
- **Не более ОДНОЙ ноды кластера за тик** (жёсткая гранулярность — и решение,
  и пересоздание): rolling растянут по тикам, единовременный снос нескольких
  нод исключён (инвариант «живых ≥ max(1, nodes−1)» не нарушается
  конструктивно). Вход ротации и вход конвергенции суммарно дают не более
  одного пересоздания на тик (единый шаг).
- **Пересоздание с сохранением volume**: stop + rm (БЕЗ удаления volume —
  `RemoveNodeAsync` не годится, он сносит `pgw-…-data`; паттерн
  port-mismatch-ветки `EnsureNodeAsync`: `StopContainerAsync` +
  `RemoveContainerAsync(force)`) → `EnsureNodeAsync` создаёт контейнер с
  текущим env (REST-TLS-материал уже в BuildSpec; любой вход ставит полный
  env — TLS и пару — входы сходятся, конкуренции за одну ноду нет). Нода
  возвращается со СВОИМ PGDATA (не basebackup) — пауза ограничена стартом
  Patroni/PG одной ноды.
- **Живой лидер — семантика TO_RECREATE-soft, если лидер TLS-нода**:
  https-проба подтверждает живость (`IsAliveAsync`) → graceful-switchover на
  живую реплику (POST `/switchover` по https с кредами кластера — §5.4), снос
  следующим тиком, когда нода уже не лидер; снапшот тика устареет сам. Это
  единственный путь для входа ротации (все ноды к моменту ротации — TLS) и для
  повторных пересозданий мигрировавших нод. Гвард кворума: пересозданию
  предшествует живой (или плановый) свидетель помимо неё — иначе шаг ждёт
  (паттерн TO_RECREATE-гварда).
- **Легаси-http лидер (вход конвергенции, первая волна миграции) — жёсткий
  снос с выборами Patroni**: soft транспортно недостижим — легаси-нода
  слушает голый http, https-проба всегда отказывает, живость по REST не
  подтверждается и switchover не отправить. Шаг сносит контейнер сразу;
  лидерство принимает очередная реплика выборами Patroni (пауза записи —
  окно выборов жёсткой смерти лидера, ограничено полом `ttl≥20` Patroni,
  arch/14 §2.1/§5 C; данные не теряются — strict-кластер ждёт sync-standby).
  Failover-свидетель — реплика, живая по DCS-членству
  (`/service/<scope>/members/<n>` state running — heartbeat'ы Patroni ходят
  через etcd, мимо REST; REST-проба легаси-контура не работает): свидетеля
  нет — шаг ждёт, лидерство в пустоту не сносим. Осознанная граница
  ОДНОКРАТНОЙ миграции, не штатный режим: после пересоздания нода — TLS, все
  последующие пересоздания (ротации, TO_RECREATE) уходят soft-путём без
  паузы записи. «Снести TLS-лидера сразу с паузой записи» отвергнуто: там
  soft-механика существует и не рвёт запись.
- Перед первым пересозданием кластера вход конвергенции делает ensure
  `rest_password` (§5.3; put-if-absent, паттерн app_params-миграции) — env
  собирается с паролем, мутации воркера сразу с кредами.
- Идемпотентность надзора не страдает: Patroni вернувшейся ноды пишет api_url
  (DNS) в DCS сам; мастер-ключ на паузу прикрыт lease TTL ≤5 с и двойным
  контуром P11 (MasterKeyReconciler).
- **Взаимодействие с пробами надзора**: проба https к ещё не мигрированной
  (легаси-http) ноде отказывает — до её пересоздания трек недоступности
  честно копится (журнальный факт; решения шага — выше — от трека не
  зависят: жёсткий снос легаси-лидера делает сам шаг, не порог rebuild).
  Порядок шага — РАНЬШЕ проб шарда в том же тике, а гранулярность «одна
  нода за тик» (тик ~5 с) укладывает миграцию шарда в единицы тиков —
  задолго до порога `NodeDeadSec=90 с`, rebuild/UNREACHABLE надзора не
  успевают сработать (на 2-нодовом шарде конвергенция завершается двумя
  тиками). Интеграционный тест (§5.10) фиксирует: за окно миграции кластер
  не порождает rebuild и UNREACHABLE-переходов.

**Вход «ротация» — фазы в процессе I** (канон arch/14 §5 I; заявка
`/pgworker/rotations/<C>` ставит панель, исполняет держатель клэйма `<C>`):

- **R0–R1** — как сегодня: журнал (op=rotate) + ensure четвёрки секретов —
  OLD-значения (включая `rest_password`).
- **R2 (NEW×4)**: SQL-тройка — как сегодня (ALTER ROLE на мастере каждого
  dsn-шарда, ретраи тиками со свежими NEW безопасны). Для `rest_password`:
  - если в журнале работы нет поля `rest_pending` — зафиксировать фазой
    `rotate-rest-start` пару NEW (`rest_pending` в `/pgworker/work/<C>`);
    повторные тики ПРОДОЛЖАЮТ проход с той же парой. Выбор хранилища —
    журнал работы: каноническое служебное состояние процесса (поля ретраев/
    треков уже там), новых etcd-сущностей не заводим; панель незнакомые поля
    журнала игнорирует (прецедент portalloc); экспозиция — класс
    `app_password` (etcd, вынос — `t02-external-secret-manager`). Потеря
    поля (битый журнал) — проход начинается заново со свежей парой
    (безопасно: окно см. ниже);
  - каждый тик — инспекция env нод dsn-шардов кластера: есть нода с
    `PGW_REST_PASSWORD_HASH` != hash(NEW) → заказ общему шагу пересоздания
    ровно одной такой ноды (§5.5, механика выше); нет — R2 завершён;
  - шард без dsn / restore / TO_REMOVE — skip (см. гварды): ноды создадутся
    с новой парой после R3 (эффективная пара ниже).
- **R3 (txn-коммит)**: все ноды всех dsn-шардов несут hash(NEW) → ОДНА txn
  процесса I пополняется compare `rest_password`==OLD и put NEW — вместе с
  тройкой SQL-секретов, перезаписью dsn и del заявки. Канон сохранён: пока
  R3 не прошёл, креды в etcd НЕ меняются. Фазовая запись `done` сбрасывает
  `rest_pending`. Проигрыш compare (внешняя запись etcdctl) — re-read и
  повтор прохода со свежей парой (новое `rest_pending`, повторный rolling —
  идемпотентен, окно продлевается).
- **Окно rolling** (от первого пересоздания входа ротации до R3): ноды разных
  поколений несут разные пары (dual-auth у Patroni нет — факт §2 п.4) →
  межнодовые REST-вызовы между поколениями получают 401; HA-инициации Patroni
  (fetch_node_status, switchover) в окне деградированы. НЕ затронуто: запись
  (SQL, мимо REST), физическая репликация (:5432), DCS-heartbeat и выборы
  (etcd), мастер-ключ (lease). Гварды окна: до R3 воркер не инициирует через
  REST кластера ни switchover (RecreateMarked-soft), ни PATCH `/config`
  (конвергенция DCS — транзиент-skip тика); подавленные мутации ретраются
  после txn. Исключение — сам rolling: его soft-switchover лидера есть часть
  ротации. Ускорение failover мёртвого лидера — DCS-ключ etcd, не REST,
  работает. Длительность окна — единицы минут (2–3 ноды × пересоздание
  со своим PGDATA + тик soft-switchover), записи не рвёт.
- **Эффективная пара сборки env**: пока журнал несёт `rest_pending` — env
  нод кластера собирается из pending (единый резолвор процессов перед
  EnsureNode), после R3 — из ключа etcd. Все EnsureNode-пути окна (rebuild,
  TO_RECREATE, конвергенция после закрытия) не расширяют окно: пересоздание
  любым путём ставит NEW-пару.
- Вход конвергенции в окне ротации приостанавливается (гвард окна): его
  пересоздание поставит полный env вместе — но чтобы не тянуть два входа к
  одной ноде в одном тике, конвергенция ждёт закрытия окна; после R3
  недомигрированные TLS-ноды дорабатываются обычным порядком.

### 5.6. Панель: PatroniRestProbe

- URL `https://…` (HostMapResolver/порт — без изменений, §6.1 контракта).
- Typed HttpClient панели (`AddHttpClient<PatroniRestProbe>`): валидация
  цепочки против `/tls-workers/ca.pem` (env `WORKERS_PANEL_TLS_SERVER_CA_PATH`
  уже смонтирован), без hostname-проверки (CustomRootTrust-паттерн). Креды не
  вводятся (факт §2 п.1). User-Agent `AdminPanel` сохраняется.
- Усыновлённые HA-скопы: контур панели требует HTTPS на `:8008` у ВСЕХ
  наблюдаемых scope (единая ветка). Стендовый демо-контур закрывается
  переводом эмуляторов (§5.8); внешний HTTP-контур усыновления честно
  отображается ошибкой пробы (деградация наблюдаемости видна, тихой http-ветки
  нет).

### 5.7. Prometheus и sd-generator

- `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`:
  - `patroni-nodes`: `scheme: https` + `tls_config {ca_file: /tls/ca.pem}`
    (сетевые alias-таргеты верифицируются полностью — имя таргета в SAN);
  - `patroni` (эмуляторы): `scheme: https` + тот же `ca_file` (§5.8).
  Клиентские серты (`prometheus.crt`) этой грани не нужны (`verify_client`
  отсутствует); tls_config джоб воркеров не меняется.
- `sd-generator`/`TargetMapping` — БЕЗ изменений: таргет — адрес, транспорт
  определяет джоба.

### 5.8. Эмуляторы стенда (hc*)

Переводим на HTTPS тем же per-install CA (решение 4): единообразие — панель и
джоба `patroni` получают одну https-ветку; http-ветка эмуляторов потребовала
бы двойной схемы пробы/джобы ради сугубо тестовых контейнеров. Basic-auth у
эмуляторов нет (мутационных эндпоинтов нет).

- `deploy/tls/gen.sh`: новый серт `hc` (serverAuth, SAN
  `DNS:hc1a,DNS:hc1b,DNS:hc2a,DNS:hc2b,IP:127.0.0.1` — один на четыре
  инстанса: тестовые контейнеры одного пакета; IP 127.0.0.1 — host-публикации
  8011–8022 для curl-чеков, прецедент SAN сертов воркеров); идемпотентный
  guard `[ -f hc.crt ] ||`.
- `dev-stand/adminpanel/sidecar/emulator.py`: HTTPS-обёртка слушателя
  (`ssl.SSLContext` + `wrap_socket`; серт/ключ из файлов, пути через env
  `HC_TLS_CERT`/`HC_TLS_KEY`); HTTP-режим не оставляем — стенд всегда
  поднимается 00-up.sh, который генерит пакет.
- compose-сервисы hc* монтируют `../../deploy/tls:ro` и передают пути.
- Чеки, ходящие на эмуляторы напрямую curl'ом (30-failover: host-публикации
  8011/8012) — переводятся на `https://` + `--cacert $ROOT/deploy/tls/ca.pem`
  (hostname проходит: 127.0.0.1 в SAN). Чеки через панель/Prometheus API не
  меняются.

### 5.9. deploy / стенд / чек 65 / 00-up / 90-down

- `deploy/docker-compose.yml` (x-pgworker-env):
  `PGW_REST_TLS_CA_PATH: /tls/ca.pem`, `PGW_REST_TLS_CA_KEY_PATH:
  /tls/ca.key` — обоим инстансам (общий TLS-том `pgw-api-tls`).
- `00-up.sh`: наполнение deploy-тома `deploy_pgw-api-tls` дополняется
  `ca.key` (cp-строка в существующем docker-run alpine). Порядок подъёма не
  меняется (gen.sh идемпотентен, `hc.*` guard пофайловый — повторный 00-up
  no-op по сертам).
- `deploy/.env.example`: комментарий раздела TLS-пакета (§4).
- Чек 65: patroni/patroni-nodes — https-таргеты; ассерты прежние по форме
  (health==up, счётчик `patroni_up`); шаг 2.1 — без изменений механики
  (файл file_sd валиден).
- `90-down.sh`: без изменений (серты — файлы deploy/tls и содержимое тома,
  docker-объектов не порождают; сеть `pgw-metrics` — прежние ассерты).

### 5.10. Тесты

- **Юниты**:
  - `RestPki`: SAN (DNS×2 + IP 127.0.0.1)/CN/EKU/срок с зажимом в CA; битый
    PEM → fail;
  - `RestCertificateCache`: стабильность PEM на повторе, смена CA-ключа —
    новый серт;
  - `SpiloEnvBuilder`: `SSL_RESTAPI_*` PEM-значения с реальными переносами
    строк; restapi-секция SPILO_CONFIGURATION (connect_address DNS,
    authentication) мержится в YAML корректно;
  - entrypoint-материализация ca-файла + строка `PGW_NODE_CA` в pgw-node.env
    (sh-юнит);
  - `master-lease.py`: `local_role()` на https (py-юнит: сборка ssl-контекста
    по ca-файлу; отсутствие `PGW_NODE_CA` → None);
  - `ClusterSecretEnsurer`: rest_password put-if-absent/re-read (7-й ключ в
    существующих кейсах);
  - `ClusterSecretRotator` (rest-ветка процесса I): фиксация `rest_pending`
    в журнале и продолжение прохода той же парой на повторном тике; потеря
    поля — свежая пара; выбор ноды по `PGW_REST_PASSWORD_HASH`; txn
    compare/put `rest_password` в общем коммите R3; сброс pending фазой done;
    проигрыш compare — повтор прохода; гварды окна (switchover/PATCH
    подавлены, ускорение failover — работает); no-op без заявки;
  - общий шаг пересоздания (ветки лидера): TLS-лидер — soft (switchover,
    снос следующим тиком); легаси-http лидер — жёсткий снос, свидетель —
    DCS-членство реплики (отсутствие — ожидание);
  - `ShardProbe`: https-URI; Authorization на PATCH/POST, отсутствие на GET.
- **Интеграции**:
  - PatroniRestProbe (панель) — https-стаб на Kestrel с тестовым CA,
    проверка доверия CA и User-Agent;
  - ensure rest_password с живым etcd;
  - валидация старта воркера: отсутствие `PGW_REST_TLS_*` → fail-fast;
    WAF-фикстуры с тестовым CA поднимаются;
  - **шаг REST-TLS-конвергенции на живом контейнере** (docker-профиль):
    движковая фикстура создаёт канонический контейнер ноды с урезанным env
    (без `SSL_RESTAPI_CERTIFICATE` — имитация легаси-подъёма; полный живой
    HTTP-кластер в тесте не поднимается) + data-маркер в volume → прогон шага
    → ассерты: контейнер пересоздан, env несёт `SSL_RESTAPI_CERTIFICATE`,
    volume и маркер на месте, ensure rest_password выполнен; повторный прогон
    — no-op; полный teardown. Путь обязан исполняться тестом целиком: шаг
    мутационный (меняет живые контейнеры прод-контуров), юнитов сверки
    признака недостаточно;
  - **авторотация rest_password** (docker-профиль, тот же класс фикстур):
    заявка `/pgworker/rotations/<C>` → ensure OLD → rolling-пересоздание
    контейнера ноды (env несёт hash NEW; volume/маркер на месте) → txn-коммит:
    `rest_password`==NEW с compare==OLD, заявка удалена, `rest_pending`
    сброшен; идемпотентность: повторная заявка после коммита — новый проход
    с новой парой; сбой посреди прохода (фейл до txn) — продолжение с той же
    pending-парой; no-op тика без заявки; гварды окна — PATCH /config не
    уходит (журнал/счётчик вызовов стаба). SQL-тройка мокается (стратегия
    процесса): тестируется rest-механика.
- **docker-E2E**:
  - `E2eEnvironment` генерирует per-contour тестовый CA (RSA-2048
    self-signed в .NET-фикстуре; PEM-файлы в артефакты контура) и передаёт
    host-воркерам extraEnv `PGW_REST_TLS_*` — ВСЕ docker-E2E контуры создают
    ноды в TLS-режиме (дефолт поставки), http-режима в тестах нет;
  - новый профильный класс `E2ePatroniRestTlsScenarios` (маркер мерж-гейта):
    кластер поднимается → (а) `http://` на patroni-порт ноды отказывает,
    `https://` с ca.pem отвечает 200 `/cluster`; (б) серт ноды верифицируется
    против CA контура; (в) PATCH `/config` без Authorization → 401, с кредами
    (`/clusters/<C>/rest_password` читается из etcd) → 200; (г) воркер
    доводит провижининг до Active (пробы/конвергенция шли по https);
    (д) **P11-демон на TLS**: docker restart мастера → callback `on_start`
    через https-`/primary` узнаёт роль → мастер-ключ под lease жив;
    (е) шаг REST-TLS-конвергенции: нода с урезанным env → пересоздание с
    TLS-env (объединяется с (д) рестартом или отдельным кейсом); полный
    teardown + ассерт чистоты — канон e2e-isolation;
  - `E2ePatroniFileSdScenarios`: prometheus.yml сценария — `scheme: https` +
    `tls_config.ca_file` (CA-файл контура в volume прометея), ассерты таргетов
    up/серий — прежние (скрейп по TLS).

## 6. Фазы

| # | Фаза | Содержимое |
|---|---|---|
| Д0 | arch-first | Дельты arch/13/14/18/02/adminpanel (§4) + deploy/.env.example/runbook — до кода |
| Д1 | PKI и креды | `RestPki`/`RestCertificateCache` (Core); опции `RestTls` + fail-fast валидации старта; `rest_password` в ClusterSecretEnsurer/ClusterSnapshotParser; юниты |
| Д2 | Нода | `SpiloEnvBuilder` (`SSL_RESTAPI_*`, restapi-секция), `ClusterDriver.BuildSpec` + доставка кеша в драйверы; entrypoint: ca-файл + `PGW_NODE_CA`; `master-lease.py` https; юниты |
| Д3 | Клиент воркера и надзор | HttpClient `patroni` (CA, без hostname-check); ShardProbe https + Authorization на мутациях; общий шаг пересоздания нод — вход конвергенции (инспекция env, одно пересоздание на тик, лидер — soft-switchover, гварды; §5.5); `rest_password` в процессе I (вход ротации: rolling, `rest_pending`-журнал, hash-сверка, txn, гварды окна; §5.5); юниты + интеграционные тесты обоих входов |
| Д4 | Панель и мониторинг | PatroniRestProbe https + CA; эмуляторы hc* HTTPS (gen.sh серт `hc`, emulator.py, compose); prometheus.yml scheme/tls_config; PatroniRestProbe-интеграции на https-стабе |
| Д5 | Стенд | deploy-compose env `PGW_REST_TLS_*`; 00-up.sh `ca.key` в томе; чек 65 зелёный (https-таргеты); curl-чеки эмуляторов на https; идемпотентность 00-up/90-down |
| Д6 | E2E | E2eEnvironment per-contour CA; `E2ePatroniRestTlsScenarios`; https-режим `E2ePatroniFileSdScenarios`; прогоны |
| Д7 | Мерж-гейт | docker-E2E на свежем Release: маркер `Scale_AddEmptyShard` + профильный `E2ePatroniRestTlsScenarios` — оба зелёные; юниты/интеграции Pg зелёные; roadmap-гейт трека reliability (§8 п.8) |

## 7. Ограничения (не-скоуп)

- `allowlist`/`allowlist_include_members`/`verify_client` (mTLS) REST — не
  настраиваются (§2 п.8); mTLS этой грани — отдельная roadmap-задача, если
  понадобится.
- Ротация `rest_password` — только по заявке панели (процесс I, как у
  остальных per-cluster секретов); возрастная/периодическая ротация без
  заявки не вводится. Окно rolling (межпоколенческие 401 REST) — осознанная
  плата механики применения (§5.5); сужение окна (dual-auth) нативно
  невозможно у Patroni.
- Мониторинг сроков сертов (REST/CA) — `t16-cert-expiry-monitoring`
  (roadmap reliability), здесь не делается (сроки — 10 лет, зажаты в CA).
- Kafka/Valkey-домены не затронуты (их REST-грани нет; серты их нод —
  per-cluster CA своего канона).
- Advertised-ветка file_sd (записи без alias) при TLS теряет
  скрейп-гарантию (§4 arch/18): в поставке таких таргетов нет, сознательная
  деградационная зона.
- SAN не включает advertised-хост/IP host-публикаций (единственное
  исключение — loopback `127.0.0.1`): клиенты host-адресов — только
  ca-верификация; расширение SAN — отдельным решением при появлении строгого
  host-клиента.
- Внешние усыновлённые контуры на HTTP `:8008` панелью не поддерживаются
  (единая https-ветка, §5.6): их перевод — зона оператора того контура.
- `HaproxyConfigBuilder` (write-фронтенд, в контейнере не поднимается —
  roadmap-фронтенд-слой) строит `option httpchk GET /primary` без ssl: при
  возрождении фронтенд-слоя требуется доработка (`ssl verify <ca>` на check;
  basic-auth check не требует — GET-эндпоинт). Здесь не правится.
- Хост-публикации 8008/5432/6432 нод сохраняются (advertised-клиенты);
  extra_hosts/сети не меняются.
- Пер-клиентские учётки basic-auth (панель/воркер раздельно) — нативно
  невозможны у Patroni (решение 1); журнал Patroni идентифицирует панель
  User-Agent'ом как раньше.

## 8. Критерии приёмки

1. **Юниты Pg** зелёные (§5.10): PKI/кеш/env-builder/entrypoint-ca-файл/
   master-lease https/ensure-кредов/probe-авторизация/rest-ветка процесса I
   (pending/hash/txn/гварды окна/no-op) + регресс существующих.
2. **Интеграции**: PatroniRestProbe на https-стабе (доверие CA, User-Agent);
   ensure `rest_password` с живым etcd; fail-fast старта без `PGW_REST_TLS_*`;
   WAF-фикстуры с тестовым CA не роняют серии; шаг REST-TLS-конвергенции на
   живом контейнере (пересоздание, volume-маркер, ensure-кред, no-op на
   повторе); авторотация `rest_password` (заявка → rolling-пересоздание с
   hash NEW → txn-коммит compare/put → заявка удалена/pending сброшен;
   продолжение после сбоя с той же парой; no-op без заявки).
3. **docker-E2E профильный** `E2ePatroniRestTlsScenarios` зелёный на свежем
   Release: http отвергнут / https+CA работает; PATCH без кредов 401, с
   кредами 200; кластер Active (пробы/конвергенция по https); P11-демон жив
   после рестарта мастера (https-`/primary`); шаг конвергенции исполняется;
   teardown чист (ассерт чистоты).
4. **docker-E2E file_sd** `E2ePatroniFileSdScenarios` зелёный по https-скрейпу
   (таргеты up, серии `patroni_*` в TSDB, host-форвардинга нет).
5. **docker-E2E маркер мерж-гейта**: `Scale_AddEmptyShard` на свежем Release
   зелёный (все E2E-контуры уже TLS через per-contour CA в E2eEnvironment —
   падение маркера = регресс провижининга).
6. **Стенд**: чек 65 зелёный (patroni/patroni-nodes — https-таргеты up);
   эмуляторы отдают `/cluster`/`/metrics` по https с ca.pem; панель
   показывает live-пробы демо (40-live-probes); чек 30 (failover) зелёный на
   https-curl.
7. **Идемпотентность**: повторный `00-up.sh` — no-op по сертам (guard
   gen.sh), повторный подъём зелёный; `90-down.sh` — без сирот (сеть
   `pgw-metrics` — прежние ассерты; серты — файлы deploy/tls, не удаляются).
8. **Мерж-гейт трека reliability** (одним мерж-коммитом): пункт `t22` снят из
   `arch/roadmap/reliability.md`; в `arch/roadmap/reliability-report.md` строка
   перенесена из «Осталось» в «Сделано» (merge-коммит + вклад в N —
   внутренняя зона кластера без открытого HTTP, мутационная грань REST за
   аутентификацией), из формулировок отчёта убрано «Patroni REST без
   TLS/аутентификации»; правки arch (Д0) — тем же коммитом.
