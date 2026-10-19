# 04. Локальный dev-стенд (docker)

Собственный **self-contained** стенд в репозитории AdminPanel: поднимает
ядро (etcd-контур, воркеры, мониторинг) и панель на пустом контуре,
чтобы панель показывала живой контроль-плейн; ресурсы (кластеры доменов,
S3-пулы) создаются вручную через панель. Паттерны
заимствованы из `arch/stand/`, но стенд не зависит от репозитория pg (этот
монорепозиторий) — все файлы скопированы/адаптированы и живут здесь
(`dev-stand/adminpanel/`).

## 1. Профили compose (`dev-stand/adminpanel/docker-compose.yml`, name: `adminpanel-stand`)

| Профиль | Состав | Когда |
|---|---|---|
| `quick` (по умолчанию) | `etcd` (+ панель) | цикл бэкенд-разработки: API/алерты на пустом контроль-плейне, PG не нужна |
| `full` | quick + `as-minio` (переходный read-only-архив, `:9000/:9001`); PG-кластер — вручную через панель | live-пробы (Patroni REST, SQL), failover-сценарии, e2e |
| `kafka` | `kafkaworker` (живой воркер; полный `00-up.sh`) | kafka-домен панели против живого исполнителя |
| `valkey` | `valkeyworker` (живой воркер; полный `00-up.sh`; демо-кластер — вручную через панель: контейнер `vwk-<C>-node<k>`) | valkey-домен панели: live-PING-пробы, мутации, E2E-цикл |
| `metrics` | `prometheus`/`sd-generator`/`grafana`/`alertmanager` | мониторинг полного стенда |

Полный подъём `00-up.sh` поднимает ТОЛЬКО ядро и панель: etcd-контур,
панель, воркеры (стендовые `kafkaworker`/`valkeyworker` профилями +
deploy-контур `pgworker`/`s3worker` ×2) и мониторинг (профиль `metrics`).
Ресурсы — кластеры всех доменов (PG/Kafka/Valkey), S3-пулы, ownS3 —
стендом НЕ создаются и никакие сиды штатным подъёмом не наливаются:
ресурс создаёт оператор вручную через панель (API воркеров). Сервис
`as-minio` (профиль `full`) и standalone-сервис `owns3` deploy-проекта
стендом не поднимаются (`as-minio` — переходный архив под ручной подъём;
S3-хранилище в контуре появляется как ресурс — кластер `/s3/`, который
провижинит S3Worker образом `owns3` по заявке через панель).

Панель (Kestrel) — **сервис compose `adminpanel`, всегда в докере** (AGENTS.md:
ВСЕГДА ЗНАЧИТ ВСЕГДА — хостовая панель не достаёт до стендовых нод, её
live-пробы таймаутятся, критичный функционал не работает). Живёт в сети
стендовой сети, публикует `5050:8080`, конфиг — env (etcd/auth) +
`adminpanel.appsettings.json`, монтируемый как `appsettings.Stand.json`
(`ASPNETCORE_ENVIRONMENT=Stand`). Хост-публикации портов compose-сервисов
фиксируются таблицей ниже; `container_name` — с префиксом `as-`
(`as-etcd-1/2/3`, `as-minio`, …): имена контейнеров pg (этот
монорепозиторий)-стенда совпадают, сосуществование двух стендов не
конфликтует именами (порты на хост не дублируются). PG-ноды демо-кластера —
НЕ compose-сервисы: их создаёт движок PgWorker (контейнеры `pgw-demo-*`,
§2.2), публикации портов portalloc — наружу для оператора/дебага.

Стенд — ОДИН контур = ОДНА стендовая сеть `adminpanel-stand-net` (§2.3):
в ней все контейнеры стенда; deploy-проект (воркеры, ownS3) подключается
external через override `docker-compose.contour.yml`.

| Контейнер | Внутри | На хосте |
|---|---|---|
| `etcd1` | 2379 | 2379 (env `ETCD1_HOST_PORT`) |
| `etcd2` | 2379 | 2381 (env `ETCD2_HOST_PORT`) |
| `etcd3` | 2379 | 2383 (env `ETCD3_HOST_PORT`) |
| `adminpanel` | 8080 | 5050 |
| `as-minio` (full; переходный read-only-архив) | 9000 (S3 API) / 9001 (консоль) | 9000 / 9001 |
| `pgw-<C>-*` (движок; ноды кластеров после их создания вручную) | 5432 / 8008 | portalloc `:15xxx` / `:18xxx` |
| `s3w-<C>-<I>` (движок S3Worker; ноды кластеров `/s3/`) | 9000 | portalloc `:18xxx` (ряд S3W) |
| `vwk-<C>-node<k>` (движок ValkeyWorker) | 6379 | portalloc `:17xxx` |

Панель получает env грани «Хранилище бэкапов»: только
`AdminPanel__Backups__S3__Bucket=pgworker-backups` (path-style — дефолт
appsettings). Endpoint и креды грань берёт из снапшота S3-домена
per-cluster ([02](02-etcd-contract.md) §12): endpoints Active-кластеров
`/s3/` + read-only ключ `panel` их secrets-ключей
`/s3/clusters/<C>/secrets` (контейнер пула `backups` провижинит S3Worker
по заявке через панель — §2.2; до создания пула грань configured с
health-отказом).

`as-etcd-1/2/3` — **единственный etcd полной системы** — 3 узла ОДНИМ
кластером (кворум 2/3; рецепт — arch/04 §8, зеркало прода): advertise
`host.docker.internal:2379/2381/2383` потребляют Patroni-ноды,
создаваемые PgWorker в своих сетях; PgWorker из `deploy/` подключается по
`PGW_ETCD_ENDPOINT_0..2=http://host.docker.internal:2379/2381/2383`
(публикации трёх узлов; шаг 9 `00-up.sh`).

Адреса live-проб панель берёт из etcd: SQL — advertised-адрес ноды
portalloc (`host.docker.internal:15xxx`), Patroni — `http://host.docker.internal:18xxx`
(§2.3 — extra_hosts панели резолвят advertised-имя docker-хоста, пробы идут
на etcd-адреса напрямую). `AdminPanel:Probes:HostMap` на стенде не задаётся;
контракт override'а при точном совпадении `host:port` — [02](02-etcd-contract.md) §6
(прод-умолчание — пуст).

## 2. Сервисы

### 2.1. `etcd1/etcd2/etcd3` (оба профиля)

- `quay.io/coreos/etcd:v3.5.21`, 3 узла `etcd1/etcd2/etcd3` одним
  static-bootstrap кластером (arch/04 §8; стенд зеркалит прод — надёжность
  дефолтом); peer по compose-DNS, клиентские публикации 2379/2381/2383;
  named volumes `etcd1-data`/`etcd2-data`/`etcd3-data` (переживают
  `docker compose down`); healthcheck `etcdctl endpoint health`.

### 2.2. Ресурсы стенда (только вручную через панель)

`00-up.sh` ресурсы не создаёт — ядро и панель поднимаются на ПУСТОМ
контуре (ключей доменов в etcd нет; панель наполняется данными по мере
создания ресурсов оператором вручную через панель — мутации идут через
API воркеров: `POST /api/clusters` PgWorker, `POST /api/s3/clusters`
S3Worker, valkey/kafka — через страницы доменов). Сид-эндпоинт воркеров
(`POST /api/seed/demo`, за флагом `EnableSeedEndpoint`) штатным подъёмом
не зовётся; скрипт `05-seed.sh` (ветки `kafka`/`valkey`) — не часть
штатного подъёма: его вызывают чеки `50`/`51` и ручные прогоны.

Чеки стендa, требующие демо-ресурсов (`10`–`60`, `45`/`46`/`75`/`80`),
запускаются после ручного создания ресурсов через панель
(самодостаточностью не обзаводятся — стенд не тесты).

### 2.3. Единая стендовая сеть (контур стенда)

Стенд — ОДИН контур = ОДНА стендовая сеть `adminpanel-stand-net` (владелец —
стендовый compose; deploy-проект подключается external, override
`docker-compose.contour.yml`): в ней etcd-контур, панель, воркеры,
контурные ноды доменов (`pgw-*` движка PgWorker, `s3w-*` S3Worker,
`vwk-*` ValkeyWorker — контурный режим `ContourNetwork` каждого воркера
получает имя этой сети), wal-агенты, ephemeral-джобы бэкапов и
мониторинг; у каждого контейнера — ровно одна сеть. Standalone-сервис
`owns3` deploy-проекта стендом НЕ поднимается: S3-хранилище в контуре —
кластер `/s3/` (контейнеры `s3w-<C>-<I>`), провижинимый S3Worker по
заявке (ресурс, §1); грань панели «Хранилище бэкапов» до создания пула —
configured с health-отказом (ожидаемое состояние пустого контура). Усыновлённые as-ноды/patroni-эмуляторы/HostMap
— демонтированы; панель пробирует канонические ноды по advertised-адресам
portalloc напрямую (extra_hosts панели — резолв advertised-имени
docker-хоста `host.docker.internal`, не контурный переход).

### 2.4. `valkeyworker` (профиль `valkey`)

Живой ValkeyWorker по образцу сервиса `kafkaworker`: образ `valkeyworker:dev`
(сборка `docker/ValkeyWorker.Dockerfile`; в registry НЕ класть), сеть стенда,
`/var/run/docker.sock` (ноды-контейнеры `vwk-*` поднимает воркер на docker-хосте
стенда), том `vw-snapshots`, том `vw-api-tls:ro` + env
`VWK_API_TLS_{CERT,KEY,CLIENT_CA}_PATH` (общая пакета per-install API-CA со
стендовыми сертами — та же, что у pgw/kfw-граней панели), env:
`ValkeyWorker__Etcd__Endpoints__0=http://etcd:2379`,
`ValkeyWorker__AdvertisedClientHost=host.docker.internal` (endpoints дискавери
достижимы с хоста и из сети панели — порт из portalloc публикуется docker-хостом),
`ValkeyWorker__Api__AdvertiseUrl=https://valkeyworker:8080` (compose-DNS из
сети панели; lease-ключ `/valkeyworker/api/<id>`),
`ValkeyWorker__Api__EnableSeedEndpoint=true` (только стенд-образы).
API-порт на хост не публикуется (панель ходит по compose-DNS; хост-публикация
8082 у kfw — наследие его чеков, для valkey не нужна — чеки ходят через панель).
Хост-порты нод — из диапазона 17000–17999 portalloc: пересекаются с
ValkeyWorker-стендом `deploy/` при одновременном подъёме — на одном хосте
работает ОДИН valkey-контур (стенд ИЛИ deploy), как у pgw; чеки это не
проверяют — выбор за оператором.

**Валkey-сид** (`05-seed.sh valkey` — вручную или чеком `51`; штатный
подъём его НЕ зовёт, §2.2): `POST /api/seed/demo` живого воркера —
демо-кластер `demo` (512 MiB, `allkeys-lru`, ресурсы 1/1Gi/10Gi). В отличие
от pg/kafka-сидов (статичные ключи), valkey-«сид» — ЗАЯВКА: её доигрывает
Reconcile-цикл живого воркера (контейнер `vwk-demo-node1`, ключи
endpoints/креды/RUNNING появляются тиками). Идемпотентность та же: живой
config → no-op. Воркер в `valkey`-профиле продолжает жить после сида
(панель видит живой кластер, PING-проба идёт по endpoints из etcd);
чистка демо-контейнера — `90-down.sh` (воркер останавливается, контейнер
`vwk-demo-node1` удаляется сценарием teardown — restart-политика
`unless-stopped` не переживает удаление).

## 3. Проверки (`dev-stand/adminpanel/checks/*.sh`, стиль `arch/stand/checks/`)

| Скрипт | Сценарий | Ожидание |
|---|---|---|
| `00-up.sh` | стендовый compose (владелец сети `adminpanel-stand-net`, профили kafka/valkey/metrics — ядро: etcd/панель/воркеры/мониторинг) → deploy-контур contour-override (воркеры; owns3 НЕ поднимается) → wait-on-healthy (etcd, heartbeat kafkaworker/valkeyworker, healthz панели) | стенд поднят: ядро + панель на пустом контуре (ресурсов нет, ключи доменов в etcd отсутствуют) |
| `05-seed.sh` | сид через API воркеров — ветки `kafka`/`valkey` (`all` = обе); НЕ часть штатного подъёма: вызывают чеки `50`/`51` и ручные прогоны | сид на месте, идемпотентно (живой config → no-op) |
| `10-smoke-api.sh` | панель против стенда: login → 401 без cookie, `/api/overview`, `/api/etcd/status`, `/api/clusters/demo`, `/api/ha/demo-shard1`, `/api/alerts`; мастер-ассерты без хардкода адресов нод (portalloc-динамичны: `masterAddress` непуст) | 200, структура, данные живого demo видны |
| `20-alerts.sh` | сценарии на живом каноническом кластере: move туда-обратно (`demo/shard1`↔`demo/shard2`); `shard-no-master` (critical): `etcdctl del /clusters/demo/shards/shard2/master` → алерт → возврат ключа фактическим мастером (значение — из API) → гашение; демо-аномалии UI насыпает сам | алерты появляются ≤ 2 тиков; после восстановления гаснут |
| `30-failover.sh` | kill мастера по leader-ключу `/clusters/demo/shards/shard1/master` (`docker kill pgw-demo-shard1-<node>`) → Patroni (DCS etcd) промотит реплику — wait нового мастера (leader-ключ сменился, lease жив) → надзор движка rejoin'ит убитую ноду (wait RUNNING) | цикл алерт→успокоение; финал — оба члена RUNNING, master-ключ жив |
| `40-live-probes.sh` | панель (в докере, стендовая сеть): Patroni/SQL-пробы — advertised portalloc-адреса нод (из `/api/clusters/demo`: patroni `:18xxx`, pg `:15xxx` через `host.docker.internal`); `/api/ha/demo-shard1` lag/state заполнены, инвентарь ACTIVE-схем сверен | поля не null, probe-ошибок нет, `inventory-mismatch` нет |
| `43-etcd-ha.sh` | кворум/health/member list; stop as-etcd-2 → запись/healthz/master-ключ живы; start → снова 3/3 | каждый шаг ≤ бюджета |
| `45-backups-storage.sh` | грань «Хранилище бэкапов» против ownS3 (контур: `owns3:9000` внутри / `:9010` снаружи): гвард healthz `:9010`, идемпотентный mc-налив снаружи (`demo/shard1`), дерево — реальные бэкапы demo + налив, `clusterOk == true` | `configured=true`, health api/live ok, дерево `demo/shard1` с полными |
| `51-valkey-api.sh` | valkey-домен против живого воркера (профиль `valkey`): сид `demo` → панель видит кластер (список/детали/нода RUNNING/endpoints), live-PING `live=true`; полный цикл мутаций через панель→прокси→API воркера: create (RunTag-имя) → NOT_INITIALIZED → RUNNING ≤ бюджета тиков, config-мутация (maxmemory converge), resources (пересоздание контейнера), ротации app+admin (заявки 202, исполнение окном двух паролей), delete → контейнера и ключей нет; 409-ветки (имя занято, двойная ротация) | каждый шаг ≤ бюджета; после delete — ни контейнера `vwk-<tag>-*`, ни ключей `/valkey/clusters/<tag>/` |
| `52-valkey-contour.sh` | приёмка контурного режима сетей ValkeyWorker (arch/21 §2, ручной чек): логин в панель → создание valkey-кластера (уникальное имя) ЧЕРЕЗ панель → wait Active/нода RUNNING → инспект контейнера `vwk-<C>-*`: сеть `adminpanel-stand-net`; `endpoints` — advertised `host:port` (portalloc, формат не изменился) → удаление кластера через панель → ассерт чистоты (контейнера и ключей `/valkey/clusters/<C>/` нет) | контейнер ноды — в стендовой сети; advertised-формат прежний; после чека следов нет |
| `60-move-ops.sh` | операции переездов через панель→API воркера на живом demo; гвард — живой `/clusters/demo/config` (не NOT_INITIALIZED) + ноды RUNNING (wait до 60 c) | заявки исполняются, бакеты переезжают (`demo/shard1`↔`demo/shard2`) |
| `90-down.sh` | разбор: стендовый + deploy compose-проекты + движковые `pgw-*`-контейнеры/тома/сети (созданы Engine API — compose их не трогает) + страховочный prune сетей (с опцией `-v` — стереть данные) | контура не осталось (контейнеры/сети/тома) |

Скрипты — bash+jq (как в pg (этот монорепозиторий)), гоняются вручную;
CI не требуют (интеграционные тесты используют Testcontainers,
не стенд). Полный e2e-прогон — последовательность `00 → 10 → 20 → 30 → 40`
(панель — сервис стенда, поднимается шагом 00; порядок важен: 30-й меняет
топологию demo (Patroni-failover)
и возвращает её в консистентный вид rejoin'ом надзора; 40-й рассчитан на неё);
`90` — разбор. Повторный прогон — с чистого состояния (`90 -v` → `00`).

## 4. Отличия от стенда `arch/stand` (осознанные)

| В pg (этот монорепозиторий) | У нас | Почему |
|---|---|---|
| HAProxy per-шард + hasync (топология из etcd) | нет HAProxy; DSN multi-host прямо на ноды | панель читает и probe'ит, клиентский роутинг ей не нужен; минус 4 контейнера |
| opsbox с etcdctl/psql | нет; роль ops выполняют host-скрипты + `docker compose exec etcd etcdctl` | меньше движущихся частей |

## 5. Быстрый старт (для README)

```bash
# одним скриптом: ядро + панель (панель — в докере, сервис adminpanel)
cd dev-stand/adminpanel && checks/00-up.sh
# ресурсы (кластеры/пулы) — вручную через панель; сиды чеков — checks/05-seed.sh

open http://localhost:5050                # логин admin/admin (env сервиса)
```

Конфиг докерной панели: env сервиса (`AdminPanel:Etcd:Endpoints=http://etcd:2379`,
`AdminPanel:Auth:Username=admin`, `Password=admin`, `AllowHttp=true`)
+ `adminpanel.appsettings.json` → `appsettings.Stand.json`
(доп-настройки проб).
