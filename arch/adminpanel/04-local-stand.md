# 04. Локальный dev-стенд (docker)

Собственный **self-contained** стенд в репозитории AdminPanel: поднимает
etcd + шардированную PG с контроль-плейном, чтобы панель показывала живые
данные. Паттерны заимствованы из `arch/stand/` (postgres:18,
сайдкары-эмуляторы Patroni REST), но стенд не зависит от репозитория pg (этот монорепозиторий) —
все файлы скопированы/адаптированы и живут здесь (`dev-stand/adminpanel/`).

## 1. Профили compose (`dev-stand/adminpanel/docker-compose.yml`, name: `adminpanel-stand`)

| Профиль | Состав | Когда |
|---|---|---|
| `quick` (по умолчанию) | `etcd` (+ панель; демо-сид — временно поднятым воркером через его API) | цикл бэкенд-разработки: API/алерты на сидированном контроль-плейне, PG не нужна |
| `full` | quick + 2 «шарда» PG (s1a/s1b, s2a/s2b) + patroni-эмуляторы (`hc*`) | live-пробы (Patroni REST, SQL), failover-сценарии, e2e |
| `kafka` | `kafkaworker` (живой воркер; поднимается чеками `50-59` и полным `00-up.sh`) | kafka-домен панели против живого исполнителя |
| `valkey` (t03) | `valkeyworker` (живой воркер; полный `00-up.sh`; демо-кластер `demo` из сида — контейнер `vwk-demo-node1`) | valkey-домен панели: live-PING-пробы, мутации, E2E-цикл |

Панель (Kestrel) — **сервис compose `adminpanel`, всегда в докере** (AGENTS.md:
ВСЕГДА ЗНАЧИТ ВСЕГДА — хостовая панель не достаёт до стендовых нод, её
live-пробы таймаутятся, критичный функционал не работает). Живёт в сети
стенда, публикует `5050:8080`, конфиг — env (etcd/auth) +
`adminpanel.appsettings.json`, монтируемый как `appsettings.Stand.json`
(`ASPNETCORE_ENVIRONMENT=Stand`). Публикация портов на хост (compose
`publish`; фиксируется этой таблицей — compose из `t10-dev-stand` ей
соответствует). Имена **сервисов** compose — канонические (`etcd1/etcd2/etcd3`, `s1a`, …:
резолвятся DNS-сети стенда — на них построены DSN/`primary_conninfo`/скрипты),
а `container_name` — с префиксом `as-` (`as-etcd-1/2/3`, `as-s1a`, …): имена
контейнеров pg (этот монорепозиторий)-стенда совпадают, сосуществование двух стендов не должно
рукнуться конфликтом имён (порты pg (этот монорепозиторий) на хост не публикует — конфликта
портов нет):

| Контейнер | Внутри | На хосте | Ключ `HostMap` (адрес ноды из etcd) |
|---|---|---|---|
| `etcd1` | 2379 | 2379 (env `ETCD1_HOST_PORT`) | — (панель — списки `AdminPanel__Etcd__Endpoints__0..2`, §2.1) |
| `etcd2` | 2379 | 2381 (env `ETCD2_HOST_PORT`) | — |
| `etcd3` | 2379 | 2383 (env `ETCD3_HOST_PORT`) | — |
| `s1a` | 5432 | 5433 | — (панель в сети стенда: резолв напрямую) |
| `s1b` | 5432 | 5434 | — |
| `s2a` | 5432 | 5435 | — |
| `s2b` | 5432 | 5436 | — |
| `hc1a` (эмулятор s1a) | 8008 (HTTPS) | 8011 | `s1a:8008` → `hc1a:8008` |
| `hc1b` (эмулятор s1b) | 8008 (HTTPS) | 8012 | `s1b:8008` → `hc1b:8008` |
| `hc2a` (эмулятор s2a) | 8008 (HTTPS) | 8021 | `s2a:8008` → `hc2a:8008` |
| `hc2b` (эмулятор s2b) | 8008 (HTTPS) | 8022 | `s2b:8008` → `hc2b:8008` |
| `adminpanel` | 8080 | 5050 | — |

Эмуляторы `hc*` слушают HTTPS `:8008` (серт `hc.crt` из per-install CA,
SAN `hc1a,hc1b,hc2a,hc2b` + `127.0.0.1` — host-публикации 8011–8022 для
чеков curl); HostMap-значения схемы адресов не меняют (`hc1a:8008`
и т.д.).

Панель (t08) получает env грани «Хранилище бэкапов»:
`AdminPanel__Backups__S3__Endpoint=http://host.docker.internal:9010`,
`...__Bucket=pgworker-backups`,
`...__AccessKey/SecretKey=panel-reader/panel-reader-pwd` (read-only ключ
ownS3, arch/owns3/05 §2; стендовые примеры, прод — per-install env) —
ownS3 живёт в deploy-компоузе (другая compose-сеть), панель ходит через
хост-публикацию (`extra_hosts` host-gateway); path-style — дефолт
appsettings.

`as-etcd-1/2/3` — **единственный etcd полной системы** — 3 узла ОДНИМ
кластером (кворум 2/3; рецепт — arch/04 §8, зеркало прода): advertise
`host.docker.internal:2379/2381/2383` потребляют Patroni-ноды,
создаваемые PgWorker в своих сетях; PgWorker из `deploy/` подключается по
`PGW_ETCD_ENDPOINT_0..2=http://host.docker.internal:2379/2381/2383`
(публикации трёх узлов; шаг 9 `00-up.sh`).

Адреса live-проб панель берёт из etcd (DSN `host=s1a,s1b port=5432`,
Patroni `http://<host>:8008`). SQL-хосты compose-сети резолвятся панелью
напрямую; но `:8008` слушают эмуляторы `hc*` — отдельные контейнеры с ДРУГИМ
именем, чем нода: адрес `<host>:8008` пропускается через
`AdminPanel:Probes:HostMap` (контракт — [02](02-etcd-contract.md) §6: адрес
из etcd → override при точном совпадении `host:port` → прямое подключение);
значения стенда — последняя колонка таблицы и §2.3. В проде `HostMap` пуст.

## 2. Сервисы

### 2.1. `etcd1/etcd2/etcd3` (оба профиля)

- `quay.io/coreos/etcd:v3.5.21`, 3 узла `etcd1/etcd2/etcd3` одним
  static-bootstrap кластером (arch/04 §8; стенд зеркалит прод — надёжность
  дефолтом); peer по compose-DNS, клиентские публикации 2379/2381/2383;
  named volumes `etcd1-data`/`etcd2-data`/`etcd3-data` (переживают
  `docker compose down`); healthcheck `etcdctl endpoint health`.

### 2.2. pg-демо-контур (канонический) и сид API воркеров

Pg-демо-контур — канонический кластер движка: `00-up.sh` после healthz
воркера зовёт `POST /api/clusters` (mTLS healthcheck-парой; тело — имя
`demo`, 16 бакетов, 2 шарда, 2 реплики, request_cpu=2/request_mem=2Gi,
request_disk=100Gi). Идемпотентность: живой `/clusters/demo/config` → 409;
пересборка demo — полный цикл стенда (`90-down.sh -v` → `00-up.sh`). Движок
провижинит Spilo-ноды с Patroni (arch/14), портами portalloc (публикации —
наружу, оператор/дебаг), инициализацией БД/ролей/схем `bucket_%`, переводом
в Active; wait-гейт `00-up.sh` — Active, ноды RUNNING, status-ключи бакетов
сняты. Сид `POST /api/seed/demo` pg-веткой стендом НЕ наливается (эндпоинт
остаётся в коде, no-op на живом кластере); kafka/valkey-ветки `05-seed.sh` —
без изменений. Демо-аномалии UI насыпает чек 20-alerts сам.

### 2.3. Единая стендовая сеть (контур стенда)

Стенд — ОДИН контур = ОДНА стендовая сеть `adminpanel-stand-net` (владелец —
стендовый compose; deploy-проект подключается external, override
`docker-compose.contour.yml`): в ней etcd-контур, панель, воркеры, ноды demo
(`pgw-demo-*`, поднимает движок), wal-агенты, ephemeral-джобы бэкапов,
ownS3 (alias `owns3:9000` внутри, `:9010` наружу) и мониторинг; у каждого
контейнера — ровно одна сеть. Усыновлённые as-ноды/patroni-эмуляторы/HostMap
— демонтированы; панель пробирует канонические ноды по advertised-адресам
portalloc напрямую (extra_hosts панели — резолв advertised-имени
docker-хоста `host.docker.internal`, не контурный переход).

### 2.4. `valkeyworker` (профиль `valkey`; t03)

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

**Валkey-сид** (`05-seed.sh`, t03): `POST /api/seed/demo` живого воркера —
демо-кластер `demo` (512 MiB, `allkeys-lru`, ресурсы 1/1Gi/10Gi). В отличие
от pg/kafka-сидов (статичные ключи), valkey-«сид» — ЗАЯВКА: её доигрывает
Reconcile-цикл живого воркера (контейнер `vwk-demo-node1`, ключи
endpoints/креды/RUNNING появляются тиками). Идемпотентность та же: живой
config → no-op. Воркер в `valkey`-профиле продолжает жить после сида (полная
система: панель видит живой кластер, PING-проба идёт по endpoints из etcd);
чистка демо-контейнера — `90-down.sh` (воркер останавливается, контейнер
`vwk-demo-node1` удаляется сценарием teardown — restart-политика
`unless-stopped` не переживает удаление).

Сопоставление «etcd-адрес ноды → адрес, достижимый из контейнера панели»
задаётся настройкой `AdminPanel:Probes:HostMap` (порядок разрешения —
[02](02-etcd-contract.md) §6). Значения для стенда — в
`dev-stand/adminpanel/adminpanel.appsettings.json` (монтируется в контейнер
панели как `appsettings.Stand.json`; через env словарь с ключами `host__port`
не задаётся — env-провайдер .NET режет по `__` на вложенные секции):

```json
"AdminPanel": {
  "Probes": {
    "HostMap": {
      "s1a__8008": "hc1a:8008",
      "s1b__8008": "hc1b:8008",
      "s2a__8008": "hc2a:8008",
      "s2b__8008": "hc2b:8008"
    }
  }
}
```

В appsettings ключ записывается как `host__port` (':' в ключах режется
конфиг-провайдерами .NET), канонический формат `host:port` действует в
памяти/тестах и приоритетен при наличии обоих (см. `HostMapResolver`).

SQL-проба идёт напрямую на `s1a:5432`/`s1b:5432` (DSN-хосты compose-сети;
`SslMode=Prefer` — стенд без SSL пускает plain, Spilo hostssl шифруется),
Patroni-проба — на `hc1a:8008`/`hc1b:8008` (override `<host>:8008` для
member'ов scope'а).

## 3. Проверки (`dev-stand/adminpanel/checks/*.sh`, стиль `arch/stand/checks/`)

| Скрипт | Сценарий | Ожидание |
|---|---|---|
| `00-up.sh` | стендовый compose (владелец сети `adminpanel-stand-net`, профили full/kafka/valkey/metrics) → deploy-контур contour-override (воркеры + owns3 external-сетью) → wait-on-healthy (etcd, heartbeat kafkaworker/valkeyworker, healthz панели) → `POST /api/clusters` (канонический demo) + wait-гейт конвергенции (Active, ноды RUNNING, status-ключи бакетов сняты) | стенд поднят (полная система: панель + канонический PG-demo + kafka + valkey + PgWorker + owns3, один контур одна сеть) |
| `05-seed.sh` | сид через API воркеров — только kafka/valkey-ветки (pg-ветка не наливается: demo создаёт `00-up.sh` через `POST /api/clusters`) | сид на месте, идемпотентно (живой config → no-op) |
| `10-smoke-api.sh` | панель против стенда: login → 401 без cookie, `/api/overview`, `/api/etcd/status`, `/api/clusters/demo`, `/api/ha/demo-shard1`, `/api/alerts`; мастер-ассерты без хардкода адресов нод (portalloc-динамичны: `masterAddress` непуст) | 200, структура, данные живого demo видны |
| `20-alerts.sh` | сценарии на живом каноническом кластере: move туда-обратно (`demo/shard1`↔`demo/shard2`); `shard-no-master` (critical): `etcdctl del /clusters/demo/shards/shard2/master` → алерт → возврат ключа фактическим мастером (значение — из API) → гашение; демо-аномалии UI насыпает сам | алерты появляются ≤ 2 тиков; после восстановления гаснут |
| `30-failover.sh` | kill мастера по leader-ключу `/clusters/demo/shards/shard1/master` (`docker kill pgw-demo-shard1-<node>`) → Patroni (DCS etcd) промотит реплику — wait нового мастера (leader-ключ сменился, lease жив) → надзор движка rejoin'ит убитую ноду (wait RUNNING) | цикл алерт→успокоение; финал — оба члена RUNNING, master-ключ жив |
| `40-live-probes.sh` | панель (в докере, стендовая сеть): Patroni/SQL-пробы — advertised portalloc-адреса нод (из `/api/clusters/demo`: patroni `:18xxx`, pg `:15xxx` через `host.docker.internal`); `/api/ha/demo-shard1` lag/state заполнены, инвентарь ACTIVE-схем сверен | поля не null, probe-ошибок нет, `inventory-mismatch` нет |
| `43-etcd-ha.sh` (t09) | кворум/health/member list; stop as-etcd-2 → запись/healthz/master-ключ живы; start → снова 3/3 | каждый шаг ≤ бюджета |
| `45-backups-storage.sh` | грань «Хранилище бэкапов» против ownS3 (контур: `owns3:9000` внутри / `:9010` снаружи): гвард healthz `:9010`, идемпотентный mc-налив снаружи (`demo/shard1`), дерево — реальные бэкапы demo + налив, `clusterOk == null` | `configured=true`, health api/live ok, дерево `demo/shard1` с полными |
| `51-valkey-api.sh` (t03) | valkey-домен против живого воркера (профиль `valkey`): сид `demo` → панель видит кластер (список/детали/нода RUNNING/endpoints), live-PING `live=true`; полный цикл мутаций через панель→прокси→API воркера: create (RunTag-имя) → NOT_INITIALIZED → RUNNING ≤ бюджета тиков, config-мутация (maxmemory converge), resources (пересоздание контейнера), ротации app+admin (заявки 202, исполнение окном двух паролей), delete → контейнера и ключей нет; 409-ветки (имя занято, двойная ротация) | каждый шаг ≤ бюджета; после delete — ни контейнера `vwk-<tag>-*`, ни ключей `/valkey/clusters/<tag>/` |
| `60-move-ops.sh` | операции переездов через панель→API воркера на живом demo; гвард — живой `/clusters/demo/config` (не NOT_INITIALIZED) + ноды RUNNING (wait до 60 c) | заявки исполняются, бакеты переезжают (`demo/shard1`↔`demo/shard2`) |
| `90-down.sh` | разбор: стендовый + deploy compose-проекты + движковые `pgw-*`-контейнеры/тома/сети (созданы Engine API — compose их не трогает) + страховочный prune сетей (с опцией `-v` — стереть данные) | контура не осталось (контейнеры/сети/тома) |

Скрипты — bash+jq (как в pg (этот монорепозиторий)), гоняются вручную и в рамках задачи
`t10-dev-stand`; CI не требуют (интеграционные тесты используют Testcontainers,
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
# одним скриптом: стенд + панель (панель — в докере, сервис adminpanel)
cd dev-stand/adminpanel && checks/00-up.sh           # или: docker compose up -d (quick-профиль: etcd+seed+панель)

open http://localhost:5050                # логин admin/admin (env сервиса)
```

Конфиг докерной панели: env сервиса (`AdminPanel:Etcd:Endpoints=http://etcd:2379`,
`AdminPanel:Auth:Username=admin`, `Password=admin`, `AllowHttp=true`)
+ `adminpanel.appsettings.json` → `appsettings.Stand.json`
(`AdminPanel:Probes:HostMap` — маппинг Patroni-адресов проб, §2.3).
