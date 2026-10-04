# Runbook — эксплуатация PgWorker

Операционный manual: как запускать и обслуживать систему. Архитектура — в
[README.md](../README.md) и [arch/](../arch/); правила разработки — в
[AGENTS.md](../AGENTS.md).

- **Полный dev-стенд** (панель :5050, PG-шарды, оба воркера, метрики):
  `dev-stand/adminpanel/checks/00-up.sh` (профили/секреты — `dev-stand/adminpanel/`).
- **etcd-контур стенда** отдельно: `dev-stand/compose.yaml`.
- **Тесты**: `dotnet test src/PgWorker.slnx`; docker-E2E — `PGW_TEST_DOCKER=1`
  (обязателен в мерж-гейте задач воркеров — см. AGENTS.md).

## Вторые инстансы воркеров (t07)

Деплой и dev-стенд поднимают по **два** инстанса каждого воркера
(PgWorker/KafkaWorker/ValkeyWorker) всегда — SPOF надзора устранён дефолтом,
не опцией. Смерть одного контейнера: lease-ключи (`/pgworker/api/*`,
`/instances/*`, `/claims/*`) гаснут ≤15 с, клэймы мигрируют второму инстансу —
надзор (provisioning/rebuild/ротации/бэкапы) не прерывается; панель и
Prometheus видят оба инстанса.

- **Порты** (прод-ряд, `deploy/.env`): первые — 8080 (pgw) / 8081 (kfw) /
  8082 (vwk); вторые — `PGW_API_HOST_PORT2=8083` / `KFW_API_HOST_PORT2=8084` /
  `VWK_API_HOST_PORT2=8085`. AdvertiseUrl вторых следует за их портом
  автоматически. На стенде kfw-2/vwk-2 публикуются только compose-DNS
  (`kafkaworker-2:8080`, `valkeyworker-2:8080`) — хост-порт не занимают.
- **Обслуживание одного инстанса**: `cd deploy && docker compose stop
  pgworker-2` (стенд: `docker stop as-kafkaworker-2`) — второй продолжает
  надзор; после работ `docker compose start pgworker-2` — оба ключа
  восстанавливаются ≤15 с. Останавливать ОБА — только для обслуживания etcd:
  надзор замирает, датаплейн (PG/Kafka/Valkey) живёт сам.
- **Ничего не дублировать**: тома снапшотов (`pgw-snapshots`/`kfw-snapshots`/
  `vw-snapshots`) и TLS-тома — ОБЩИЕ на пару инстансов (лидерство снапшотов
  мигрирует между ними); секреты per-install наследуются вторым инстансом
  идентично. Уникальность инстанса — случайный InstanceId, env не задаётся.
- **Проверка**: `dev-stand/adminpanel/checks/35-worker-second-instance.sh` —
  kill→takeover всех трёх воркеров с восстановлением; docker-E2E —
  `PGW_TEST_DOCKER=1 dotnet test … --filter FullyQualifiedName~SecondInstance`.

## PGTune-параметры PG-нод (`PgWorker:Pgtune`)

`postgresql.conf` нод рассчитывается алгоритмом PGTune (`PgTune.Calculate`,
норматив [`pgtune-calculation-spec.md`](pgtune-calculation-spec.md)) при
создании контейнера ноды: **память/CPU — из etcd-заявок
`/service/<scope>/request_{cpu,mem}` на ноду** (панель пишет их при создании
шарда; arch/14 §2.1 п.4 — они же лимиты контейнера; заявки ОБЯЗАТЕЛЬНЫ:
отсутствие/нечитаемость — фейл фазы provisioning с journal-ошибкой и ретраем),
остальные
входы — константы конфигурации `PgWorker:Pgtune` (канон — arch/14 §2.1/§8).
Канон PgWorker (P3: `wal_level=logical`, walsenders/slots) перекрывает расчёт;
бюджет doorman синхронизирован: `max(10, max_connections − 5)` (P15).

- **Настройка в деплое** — `PGW_PGTUNE_*` в `deploy/.env` (полный список и
  семантика — `deploy/.env.example`; закомментировано = дефолты appsettings).
  Память/CPU через env НЕ задаются — только etcd-заявками `request_*`.
  `DbType=desktop` запрещён (fail-fast старта воркера).
- **Пересчёт без фиксации**: параметры НЕ хранятся в etcd — рассчитываются
  заново при каждом создании контейнера от актуальных `request_*`. Смена заявок
  или опций подхватывается только новыми нодами; живые ноды продолжают работать
  на прежнем конфиге (осознанный дрейф; автоматическое выравнивание —
  `t11-pgtune-params-convergence`, roadmap). Консистентность при смене заявок
  живого шарда — на операторе (пересоздать все ноды шарда или не менять заявки).
- **io_method/io_workers** по умолчанию исключены из применения
  (`ExcludeParams`): `io_uring` требует сборки PG с liburing (Spilo-18 не
  проверен). После проверки сборки — убрать из exclude в `deploy/.env`.

## Внешние docker-образы — локальный registry `192.168.0.1:5000`

Все **внешние** образы (dev-стенд, тесты, деплой) берутся из приватного registry
локальной сети `https://192.168.0.1:5000`; если registry недоступен — из
апстрима (Docker Hub / quay.io / ghcr.io / mcr.microsoft.com). Исчерпывающая
инструкция по самому registry (TLS, auth, API, бэкап) — [`../../docker-registry.md`](../../docker-registry.md)
в каталоге над репозиторием.

**Правило приоритета: сначала registry, при недоступности — Docker Hub/апстрим.**
Перед подъёмом стенда и прогонами docker-тестов:

```bash
dev-stand/images/pull-images.sh
```

Скрипт тянет каждый образ из `192.168.0.1:5000/<образ>` и перетегирует в
каноническое имя (`postgres:18`, `quay.io/coreos/etcd:v3.5.21`, …) — compose-файлы
и тесты остаются на канонических именах и не знают про registry. Если registry
недоступен — образ тянется напрямую из апстрима (скрипт печатает `upstream:`).

### Разовая настройка клиента

CA-сертификат registry должен быть доверен Docker и выполнен логин (подробно —
в `../../docker-registry.md`): CA положить в
`~/.docker/certs.d/192.168.0.1:5000/ca.crt` и перезапустить Docker Desktop,
затем `docker login 192.168.0.1:5000 -u dm` (пароль — в инструкции registry).

### Имена в registry

`192.168.0.1:5000/<образ>`: для Docker Hub — краткое имя как есть
(`192.168.0.1:5000/postgres:18`, `192.168.0.1:5000/apache/kafka:4.0.0`), для
чужих реестров — с хостом (`192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`,
`192.168.0.1:5000/mcr.microsoft.com/dotnet/sdk:10.0`). Рядом с мульти-арх тегом
лежат per-arch теги `…-amd64` / `…-arm64`.

### ⚠️ Локально собираемые образы в registry НЕ класть — никогда

`pgworker:dev`, `kafkaworker:dev`, `pgworker-backup:dev`, `adminpanel:dev`,
`pgworker-node:dev|e2e`, `pgworker-backup:e2e`, сайдкары/opsbox стенда —
артефакты конкретной машины/ветки. В общий registry они **категорически
запрещены**: реестр — зеркало воспроизводимых внешних зависимостей, а не хранилище
сборок. `mirror-image.sh` для таких имён не запускать, в `images.txt` не вносить.

### Добавить новый внешний образ / обновить версию

1. Добавить строку `<образ>` в [`dev-stand/images/images.txt`](../dev-stand/images/images.txt)
   (единый источник списка).
2. Зеркалировать: `dev-stand/images/mirror-image.sh <образ>` (мульти-арх
   amd64+arm64, пер-арх pull по digest'ам → per-arch push → манифест-лист);
   всё разом — `mirror-all.sh`.
3. Проверить: `docker manifest inspect --insecure 192.168.0.1:5000/<образ>` —
   в списке `amd64` и `arm64`.

### Известная особенность: TLS-сертификат registry и Go-клиенты

Сертификат registry (самоподписанный, RSA-4096/SHA-256) отвергается новыми
Go-1.24-клиентами с ошибкой `x509: certificate is not standards compliant`
(docker-daemon при этом работает). Поэтому зеркалирование идёт через daemon
(pull/push) + `docker manifest --insecure`, а не через `docker buildx imagetools`.
Если сборка манифест-листа падает по TLS — это оно.

### Каталог зеркалируемых образов

| Образ | Где используется |
|---|---|
| `alpine:3.20` | тесты PgWorker (Docker/Exec/SshTunnel/TlsEngine), наполнение TLS-volume в `00-up.sh` |
| `apache/kafka:4.0.0` | KafkaWorker: дефолт образа брокера (`Options`), интеграция-тесты, kafka-e2e чек стенда |
| `ghcr.io/zalando/spilo-18:4.1-p2` | база образа PG-ноды `pgworker-node` (`docker/node/Dockerfile`) |
| `grafana/grafana:13.0.8` | dev-стенд (метрики) |
| `mcr.microsoft.com/dotnet/sdk:10.0` | build-стадии Dockerfile'ов PgWorker/KafkaWorker/AdminPanel |
| `mcr.microsoft.com/dotnet/aspnet:10.0` | runtime-стадии тех же Dockerfile'ов |
| `minio/mc:RELEASE.2025-08-13T08-35-41Z` | WAL-агенты и бакетные проверки E2E, `00-up.sh` (запинен — supply-chain) |
| `minio/minio:RELEASE.2025-09-07T16-13-09Z` | dev-стенд, бэкап-фикстуры (MinioFixture, E2eEnvironment) |
| `nginx:alpine` | TlsEngineProxyTests |
| `node:22-alpine` | AdminPanel.Dockerfile (сборка SPA) |
| `postgres:17-alpine` | WalSqlTests |
| `postgres:18` | dev-стенд (шарды), тесты AdminPanel, база `pgworker-backup` и opsbox |
| `prom/alertmanager:v0.28.1` | dev-стенд (метрики) |
| `prom/prometheus:v3.14.0` | dev-стенд (метрики) |
| `python:3.12-alpine` | сайдкар эмулятора Patroni (dev-stand) |
| `quay.io/coreos/etcd:v3.5.21` | etcd-контур: стенд ×3 (HA), deploy/etcd ×3 (HA, по узлу на хост), E2E/интеграция-фикстуры (×5) |

При смене версии образа в коде/компоузе — обновить строку в `images.txt` и
перемазеркалировать (старую версию из registry можно удалить — см. «Удаление
образа» в инструкции registry).

## HA-etcd контур контроль-плейна (t09)

Канон — `arch/04-deploy-etcd.md` §8 (два применения одного рецепта; параметры,
advertised-правила потребителей, кворум-семантика, чек-лист). Кратко:

- **Подъём (прод)**: скопировать `deploy/etcd/{docker-compose.yml,etcd.env}` на
  3 РАЗНЫХ docker-хоста; в `etcd.env` каждого — свой `NODE_NAME/NODE_IP`
  (`PEERS`/`CLUSTER_TOKEN` одинаковые; образ — `192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`).
  Первый старт: `INITIAL_CLUSTER_STATE=new` на всех трёх,
  `docker compose --env-file etcd.env up -d` (переменные узла интерполируются в
  YAML из env-файла) в пределах election-timeout друг от друга; после сбора
  кворума перевести в `existing` на всех узлах (arch/04 §5). Воркеры/панель
  получают список `*_ETCD_ENDPOINT_0..2` / `AdminPanel__Etcd__Endpoints__0..2`.
- **Стендовое зеркало**: дев-стенд всегда поднимает тот же 3-узловой контур,
  что рецепт `deploy/etcd` (`as-etcd-1/2/3`, публикации 2379/2381/2383,
  env `ETCD{1,2,3}_HOST_PORT`).
- **Чеки**: стендовый `dev-stand/adminpanel/checks/43-etcd-ha.sh` (member list
  3 started → stop узла: запись/healthz/master-ключ живы → start → 3/3);
  прод-чек-лист — arch/04 §8 п.5 (`endpoint health --cluster`, панель
  `/api/etcd/status` = 3 члена/единый leader).
- **Потеря узла/кворума**: 1 узел — не-событие (кворум 2/3); 2 узла —
  контроль-плейн заморожен, датаплейн живёт сам, восстановление — arch/09 §4;
  замена узла навсегда — `member remove` + `member add` + data-dir заново
  (arch/04 §8 п.4).

## Управление сертом API воркера из панели

Серверные сертификаты входящих mTLS-граней PgWorker/KafkaWorker хранятся в
etcd (`/workers/api_tls/<worker>`), панель — единственный писатель; воркеры
читают ключ ТОЛЬКО при старте (приоритет etcd > env `PGW_API_TLS_*` /
`KFW_API_TLS_*`). Применение — всегда перезапуском: страница «Воркеры» →

1. «Сгенерировать сертификат» — self-signed лист (SAN — хосты живых
   advertise-URL + localhost/127.0.0.1, EKU serverAuth, 825 дней);
   повторная генерация при живом ключе → 409 (замените через PUT или DELETE).
2. «Загрузить сертификат» — своя PEM-пара (например, от install-CA).
3. «Перезапустить воркера» — 202, инстанс делает graceful self-stop;
   контейнер поднимает docker-политика `restart: unless-stopped` (deploy).
4. «Убрать управляемый сертификат» — DELETE ключа; после перезапуска воркер
   вернётся к env-серту (статус `unmanaged`).

Статусы применения: `applied` (thumbprint инстанса == целевому),
`pending restart` (ключ новее грани — перезапустите), `unmanaged` (ключа нет,
env-серт), `unknown` (старая версия воркера не сообщает thumbprint).

Ограничение защиты: сертификат, пригодный в исходящих коммуникациях воркеров
(CA, clientAuth, совпадение fingerprint с per-cluster CA kafka или сертами
панели), ОТКЛОНЯЕТСЯ с 422 «сертификат влияет на коммуникации воркеров с их
подчинёнными сервисами — обновление отклонено» — исходящие (PG-шарды/docker/
etcd/S3, kafka-брокеры) управляемым сертом не затрагиваются.

Без restart-политики (запуск вне docker) рестарт оставит процесс
остановленным — панель покажет недоступность API воркера.

Автоматическая проверка на живом стенде — `dev-stand/adminpanel/checks/70-worker-cert.sh`
(generate → pending restart → restart → applied → откат на env).

## TLS-подключения к Valkey-кластерам (t06)

Клиентский порт Valkey-кластеров — TLS с t06 (`--tls-port 6379 --port 0`):
plain-подключения отклоняются, адреса/portalloc не менялись (адрес тот же,
транспорт TLS). Подключение требует per-cluster CA из etcd —
`/valkey/clusters/<C>/ca_pem` (PEM одной строкой с `\n`):

- `valkey-cli`/`redis-cli`:
  `valkey-cli --tls --cacert ca.pem -h <host> -p <port> -u app -a <app_password>`
  (значение из etcd развернуть на многострочный PEM: `sed 's/\\n/\n/g'`);
- StackExchange.Redis: `Ssl=true` + доверие `ca_pem` через
  certificate-validation callback (образец — `PuzzleServer.Infrastructure.App.Valkey`);
- клиенты библиотеки HA.Valkey: `GetClientConfig()` отдаёт `ssl=true` и CA
  автоматически (`ssl=true ⟺ ca_pem` прочитан).

Plain-порт закрыт с t06 — это заявленный breaking change релиза: клиенты без
TLS после обновления получают отказ на хендшейке. Пересоздание нод (надзор)
перевыпускает серты автоматически (volume `vwk-<C>-tls`), вмешательство не
требуется. Остаточный вектор — сеть контроль-плейна (arch/21 §9 R5); ротация
CA — roadmap.

## Ротация CA valkey (t07)

Ротация per-cluster CA и серверного серта выполняется воркером (процесс
CaRotator, arch/21 §5 K) по заявке `/valkeyworker/ca_rotations/<C>` — окно
двойного доверия P→D→R→C без остановки обслуживания:

- (P) staging новой CA в `ca_next_pem`/`ca_next_key` (put-if-absent);
- (D) `ca_pem` = bundle OLD+NEW — перечитавшие дискавери доверяют обоим;
- (R) пересоздание node1 с сертом от NEW (volume `vwk-<C>-tls`);
- (C) атомарный txn: `ca_pem`/`ca_key` ← NEW, del staging, del заявки.

### Ручная проверка состояния окна

- `ca_pem` содержит ДВА блока `BEGIN CERTIFICATE` → окно D→C открыто
  (один блок — ротации нет/коммит завершён);
- ключи `ca_next_pem`/`ca_next_key` есть → staging жив (окно открыто);
- ключ `/valkeyworker/ca_rotations/<C>` есть → заявка ждёт/исполняется;
- журнал `/valkeyworker/work/<C>`: op `rotate-ca` c фазой `phase-p`,
  `phase-d`, `phase-r`, `phase-r/node1`, `committed` или `done`;
  `waiting-*` — заявка отложена (кластер не поднят/жива ротация пароля).

### Снятие зависшей заявки

`etcdctl del /valkeyworker/ca_rotations/<C>` — заявка снимается; если окно
ещё не открыто (staging нет), ротация не начнётся. Открытое окно снимением
заявки НЕ откатывается — цикл доигрывается по факту staging (идемпотентно,
без заявки): последний тик коммитит NEW и удаляет staging.

### Осиротевший staging

Если воркер упал между P и C, staging `ca_next_*` остаётся. Отдельно чистить
НЕ нужно: re-read следующего тика переиспользует существующую staging (одна
генерация на жизнь ротации), доигрывает R и коммитит. Ручное удаление
`ca_next_*` при открытом bundle-окне оставит кластер с двойным доверием и
сертом OLD — не делать без полного понимания.

### Примечание для клиентов (окно D→C)

Между D и C `ca_pem` — bundle OLD+NEW: клиенты, читающие `ca_pem` из etcd и
доверяющие ВСЕМ блокам сертификата (многосертовое чтение), работают
непрерывно. Клиенты с одноблочным парсером слепы к NEW-серту: обновить их ДО
первой ротации (правка Puzzle-клиента — ветка `t07-valkey-ca-bundle`).
После коммита `ca_pem` = NEW — клиенты с закешированным OLD перечитывают
`ca_pem` из etcd.

---

## Восстановление etcd из S3-выгрузки (t08)

Снапшоты etcd (контроль-плейн: декларации кластеров, маршрутизация, секреты,
клэймы, portalloc) уходят в S3 каждым снятием при
`PGW_SNAPSHOTS_EXPORT_ENABLED=true` (deploy/.env): bucket бэкапов, префикс
`etcd/`, пары `snapshot-<id>.db` + `snapshot-<id>.meta.json` (sha256/размер/
ревизия/instance). Потеря docker-хоста вместе с etcd — восстановимая авария.

1. **Диагностика**: подтвердить наличие выгрузок —
   `mc ls --recursive <alias>/<bucket>/etcd/` — выбрать новейший
   `snapshot-<id>.db` и его `.meta.json`. Пусто → см. границы (п.6).
2. **Проверка целостности ДО восстановления** (обязательный шаг):
   скачать `.db`; сверить sha256 файла с `sha256` из `.meta.json` — ручная
   сверка и есть операторский барьер (ловит порчу до restore); затем
   `etcdctl snapshot status <file>` — информационный шаг (структура/размер/
   ревизия слепка; etcd 3.5.x печатает hash, но при status его НЕ сверяет —
   целостность им не доказывается); restore дополнительно верифицирует сам
   слепок (`etcdutl snapshot restore` проверяет sha256 при разворачивании,
   шаг 4). Расхождение sha256 или провал restore ⇒ взять предыдущий слепок
   (ретенция хранит N последних пар); файл с битым хешем не разворачивать
   НИКОГДА.
3. **Свежий etcd**: контейнер(ы) etcd с чистым data-dir
   (`INITIAL_CLUSTER_STATE=new`). Прод-3-ноды: сначала single-member,
   остальные — `etcdctl member add` после старта.
4. **Restore**: остановить etcd → `etcdutl snapshot restore <file>
   --data-dir=<data-dir>` (restore — офлайн-операция над слепком;
   верифицирует sha256 слепка при разворачивании — финальный барьер
   целостности) → стартовать etcd на восстановленном data-dir.
5. **Контроль**: `etcdctl endpoint health`; воркеры/панель переподключаются
   сами (poll-тик; клэймы переснимутся lease-механикой; R7 arch/14: journal
   мог откатиться к точке слепка — процессы идемпотентны и доводят фазы);
   панель видит кластеры, healthz воркеров зелёные. Статус-ключ
   `/pgworker/etcd-snapshots` появится первым же слепком воркера.
6. **Границы**: слепок = точка его снятия (RPO = точка последнего изменения
   минус окно transient-отказа S3 — закрывается доводкой тика
   `RetryIntervalSec`); локальные тома снапшотов (если пережили) —
   альтернативный источник того же формата; WAL-данные PG-шардов живут в S3
   бэкапов независимо (arch/19) — восстановление etcd их не касается.
   Алерты панели: `etcd-snapshot-export-failed` (выгрузка провалилась —
   проверить доступность S3/креды `PGW_BACKUP_S3_*`),
   `etcd-snapshot-export-stale` (выгрузка молчит — обе инстанции воркера
   лежат или вечный transient).
