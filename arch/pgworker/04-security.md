# PgWorker: секреты и ротации

Уровень 3. Функция «безопасность»: per-cluster и per-install креды,
транспортный TLS, доставка секретов в ноду каналом агента, ротация
per-cluster секретов без остановки записи. Полные автоматы — процессы
[I. ClusterSecretRotator](processes/i-secret-rotator.md),
[L. NodeAgent](processes/l-node-agent.md); ключи —
[00-etcd-contract.md](00-etcd-contract.md) §2–§3; TLS-модель —
[13-network-security.md](../13-network-security.md).

## 1. Per-cluster секреты в etcd

Генерирует PgWorker, класс экспозиции — etcd без per-key ACL (парсеры
панели их не разбирают, в UI не попадают):

- `app_user`/`app_password` — provisioning P1.5: txn put-if-absent, 32 симв
  `[A-Za-z0-9]`; роль app в БД выравнивается идемпотентным
  `ALTER ROLE … PASSWORD` на каждом шарде.
- `mover_password` — роль `bucket_mover` (REPLICATION — подписки переездов
  P2/P3); имя роли фиксировано, отдельного user-ключа нет.
- `bucket_admin_user`/`bucket_admin_password` — DSN-точка входа; попадают в
  dsn-ключ шарда и env контейнера ноды; порядок чтения ключи → config → env.
- `rest_password` — basic-auth пара Patroni REST `:8008` (мутационные
  эндпоинты; username — константа `patroni`, отдельного user-ключа нет:
  Patroni требует пару username+password в конфиге каждой ноды).

Канон для всех ролей один: ensure P1.5/R1/adopt — txn put-if-absent (для
bucket_admin вход — config JSON или генерация). Ротация — процесс I: заявка
панели, ETCD-FIRST, rolling reload Patroni по нодам без остановки PG
([§6](#6-ротация-per-cluster-секретов)).

По тому же прецеденту per-cluster в etcd — клиентские креды S3-кластеров
хранилища бэкапов (`/s3/clusters/<C>/secrets`,
[s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md) §6): генерирует
S3Worker (PgWorker — только читатель, ключ `backups`); класс экспозиции
тот же — парсеры панели разбирают значения только для внутреннего
S3-клиента грани «Хранилище бэкапов» (ключ `panel`), в UI/API их не
отдают. Вынос секретов из etcd — `t02-external-secret-manager`
(pgworker-трек), класс экспозиции не расширяется.

## 2. Per-install секреты из env PgWorker

Не в git, не в etcd (P12/P17):

- `PGW_PG_SUPERUSER_PASSWORD`, `PGW_PG_STANDBY_PASSWORD`,
  `PGW_BUCKET_ADMIN_PASSWORD`, `PGW_BUCKET_MOVER_PASSWORD` —
  fallback-значения группы 1 ДО первого ensure (порядок чтения кредов:
  ключи etcd → config → env). `PGW_APP_ROLE_PASSWORD` исключён
  (app-секрет — только группа 1).
- TLS/транспорт ([07-api.md](07-api.md) §2, [01-placement.md](01-placement.md)
  §4): `PGW_API_TLS_{CERT,KEY}` (серверный серт mTLS API — с управлением из
  панели это БУТСТРАП-ФОЛЛБЭК: приоритет у etcd-ключа
  `/workers/api_tls/pgworker`; `…_PATH` из volume), `PGW_API_TLS_CLIENT_CA`
  (валидатор клиентов — только env, панелью не меняется),
  `PGW_DOCKER_TLS_{CA,CERT,KEY}` (клиентский транспорт Engine API),
  `PGW_DOCKER_SSH_KEY[_PATH]` (key SSH-туннелей),
  `PGW_DOCKER_SSH_FINGERPRINT` (опц. pin host-key). Серверный серт из
  etcd-ключа — материал входящей грани, не исходящих коммуникаций
  (docker/PG/etcd-транспорт воркера его не использует; валидация при
  записи — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
  §9.9); `PGW_API_KEY` исключён (заменён mTLS).
- REST-TLS нод: `PGW_REST_TLS_CA`/`PGW_REST_TLS_CA_KEY` (`…_PATH`-варианты
  из TLS-тома `/tls`) — per-install API-CA и его ключ для выпуска серверных
  сертов REST-эндпоинтов нод (`SSL_RESTAPI_*`,
  [01-placement.md](01-placement.md) §2); отсутствие/неполнота пары —
  fail-fast валидации старта воркера (HTTP-режим провижининга не
  существует; WAF-фикстуры передают тестовый CA — двойной семантики нет).

## 3. Доставка секретов в ноду: env без секретов, канал агента

Env контейнера ноды секретов НЕ несёт: `docker inspect` ноды не раскрывает
ничего конфиденциального. В env — только неконфиденциальное:
`DOORMAN_CONFIG_TEMPLATE` — шаблон конфига пулера (пароль — плейсхолдер
`${PGW_SU_PASSWORD}` вместо значения), адреса etcd (`ETCD3_HOSTS`/
`PGW_ETCD`), идентичность ноды и lease-скрипта (`SCOPE`, `PGW_BACKUP_*`,
`PGW_NODE_*`, `PGW_MASTER_KEY`, `PGW_DOORMAN_PORT`,
`USE_DATA_DIR_FOR_WAL`), публичные CA (не секретны: `SSL_RESTAPI_CA`
REST-слоя, CA верификации клиентов mTLS-канала агента). Supervisord
поднимает ТОЛЬКО pgnode — агент PgNode (программы spilo и pg_doorman —
autostart=false): PgNode — загрузочный агент ноды и менеджер пулера.

Разделение доставки по группам секретов:

1. **Per-cluster/пер-нодовое — через etcd** (агент забирает сам, доступ к
   etcd у него уже есть): воркер пишет bootstrap-ключ ноды
   `/clusters/<C>/shards/<X>/nodes/<n>/bootstrap`
   ([00-etcd-contract.md](00-etcd-contract.md) §3) — полный
   `spilo_configuration` (SPILO_CONFIGURATION с rest_password внутри) и
   серверный серт ноды `cert_pem`+`key_pem` (per-install REST-CA, SAN —
   [01-placement.md](01-placement.md) §5; ОДИН серт ноды обслуживает REST
   `:8008`, doorman `:6432` и mTLS-канал агента `:8081`).
2. **Per-install пароли PG (`SuPassword`/`StandbyPassword`) — прямая
   передача по mTLS-каналу**: в etcd НЕ попадают (Д7 — группа 2), в env
   контейнера — тоже; воркер передаёт их агенту телом mTLS-запроса при
   старте ноды. Это единственная полезная нагрузка канала помимо сигналов.

**Канал** — mTLS, инициатор ВСЕГДА воркер (нода про PgWorker ничего не
знает и ничего не инициирует): сервер — агент, порт `:8081` ВНУТРИ
docker-сети кластера, на хост не публикуется (строка матрицы —
[13-network-security.md](../13-network-security.md) §2). Верификация:
воркер проверяет серверный серт агента по per-install REST-CA (SAN имени
ноды); агент проверяет клиентский серт воркера по публичному CA из env
контейнера + EKU ClientAuth + CN=pgworker (серверные серты нод того же CA
клиентом канала НЕ являются; сертификаты — §2 гр.3, выпуск из REST-CA).
Граница канала — ПОРТ: маршруты команд `/api/bootstrap*` обслуживаются
ТОЛЬКО листенером `:8081`; по plaintext `:8080` (metrics/ops-API) — `404`
(единый роутер процесса границей доверия не является).

По сигналу воркера агент перечитывает etcd, рендерит конфиги
(SPILO_CONFIGURATION, конфиг doorman из шаблона `DOORMAN_CONFIG_TEMPLATE` +
пароля), материализует серты и применяет замены — семантика применения ПО
МАТЕРИАЛУ: rest-пароль — reload Patroni (SIGHUP/REST POST /reload),
rest-серт — рестарт Patroni-программы (реплики) либо пересоздание
контейнера (мастер — воркером), doorman — рестарт (отдельная программа
пулера, её рестарты допустимы; supervisorctl как менеджер пулера, обёртка
spilo-supervised.sh). Права доставляемых файлов сертов обязан обеспечивать
поставщик (tmpfs 0600 postgres): root-владелец файла — PermissionError
Patroni на reload. Секретные файлы — ТОЛЬКО tmpfs `/run/pgw` (в памяти
контейнера, 0600 postgres): КЭША СЕКРЕТОВ НА ДИСКЕ НЕТ (writable-слой/тома
секретов не несут). Процессы Spilo получают секреты env'ом ПРОЦЕССА:
обёртка spilo-supervised.sh перед launch.sh экспортирует
`SPILO_CONFIGURATION`, `PGPASSWORD_SUPERUSER`, `PGPASSWORD_STANDBY`,
`SSL_RESTAPI_{CERTIFICATE,PRIVATE_KEY}` из tmpfs-файлов агента —
docker-env контейнера при этом чист.

Рестарт контейнера — секреты tmpfs утрачены, нода НЕ поднимается сама:
агент PgNode работает (его идентичность — в docker-env) и ждёт доставки;
воркер тиком надзора доставляет заново (статусная машина —
[L. NodeAgent](processes/l-node-agent.md)). Нода без воркера не
перезапускается — осознанная граница (воркер — инициатор всех переходов
ноды). Recreate делает только воркер — он же тут же доставляет данные.
Ротация `rest_password` — БЕЗ пересоздания контейнера и БЕЗ рестарта
Patroni-процесса: перезапись bootstrap-ключа + сигнал применения → агент
применяет конфигурацию Patroni reload'ом (SIGHUP / REST POST /reload; PG не
останавливается).

## 4. Статусная машина агента и хендшейк старта

Ключ `/pgworker/nodes/<C>/<X>/<N>/status`
([00-etcd-contract.md](00-etcd-contract.md) §4) пишет ТОЛЬКО агент PgNode
lease-сессией TTL 15 с (как статус приёмника), воркер читает-мониторит;
смерть процесса гасит ключ — статус не «застревает» от мёртвого агента.
Состояния: `not_initialized` → `initializing` → `ready` | `start_failed`;
полный автомат, фазы `initializing`, tool-путь и реакции воркера —
[L. NodeAgent](processes/l-node-agent.md).

Кратко: агент публикует `not_initialized` при старте (листенер `:8081` уже
поднят); воркер, увидев его, доставляет bootstrap-ключ и per-install пароли
PG телом mTLS-запроса; агент атомарно переводит состояние в `initializing`
txn-сравнением, стартует компоненты (до 3 попыток на компонент) и ждёт
`SELECT 1` без бюджета; все готовы → `ready` с `applied_revision`. На
`ready` замена материала — отдельная команда применения (обновляет
`applied_revision`, машина остаётся `ready`). `start_failed` → воркер
пересоздаёт ноду с новым диском по правилам
[C. NodeSupervisor](processes/c-node-supervisor.md).

## 5. SAN и TLS-инварианты

- SAN серверного серта REST-ноды — DNS `<n>`, DNS `pgw-<C>-<X>-<n>`, IP
  `127.0.0.1`; advertised-хост в SAN не входит (клиенты advertised-адресов
  верифицируют цепочку без hostname-проверки,
  [13-network-security.md](../13-network-security.md) §4).
- SAN серверного серта API воркера — `pgworker`, `localhost`,
  `host.docker.internal`, `127.0.0.1` (R13,
  [08-operability.md](08-operability.md) §6); панель валидирует цепочку
  (CustomRootTrust) — hostname-check не ломает её даже при расхождении.
- TLS к Engine API и SSH-туннели — [01-placement.md](01-placement.md) §4;
  mTLS API — [07-api.md](07-api.md) §2.

## 6. Ротация per-cluster секретов

Смена ВСЕХ per-cluster кредов кластера — app (`app_user`/`app_password`),
bucket_mover (`mover_password`) и bucket_admin (`bucket_admin_user`/
`bucket_admin_password`) — по заявке `/pgworker/rotations/<C>` (ставит
панель, клэйм-txn; формат — [00-etcd-contract.md](00-etcd-contract.md) §4).
Исполнитель — держатель клэйма `<C>`; цикл ReconcileLoop зовёт процесс в
Active-ветке после scale-прохода (короткая секундная операция — не ждёт
длинных переездов).

Порядок фаз — **ETCD-FIRST**: etcd — источник истины, NEW-креды
коммитятся в etcd В НАЧАЛЕ окна; PG следует etcd финальным ALTER'ом в
КОНЦЕ (принцип «etcd — источник, PG следует» — как выравнивание ролей
provisioning). Это открывает окно, в котором etcd несёт NEW, роли PG —
ещё OLD: SQL-подключения с NEW из etcd получают SCRAM-отказ (28P01) —
**транзиент по определению окна, НЕ инцидент**; окно закрывается финальным
ALTER'ом. Гварды окна: пока флаг `rest_window_open` журнала работы стоит,
воркер не инициирует через REST кластера ни switchover (RecreateMarked-soft),
ни PATCH `/config` (конвергенция DCS — транзиент-skip тика); подавленные
мутации ретраются после done.

Полная механика R0–R4 (единая txn NEW-четвёрки, rolling reload Patroni,
лайвлок-гвард, R3-серт, закрытие окна) —
[I. ClusterSecretRotator](processes/i-secret-rotator.md).
