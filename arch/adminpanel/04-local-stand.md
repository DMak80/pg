# 04. Локальный dev-стенд (docker)

Собственный **self-contained** стенд в репозитории AdminPanel: поднимает
etcd + шардированную PG с контроль-плейном, чтобы панель показывала живые
данные. Паттерны заимствованы из `../pg/arch/stand/` (postgres:18,
сайдкары-эмуляторы Patroni REST), но стенд не зависит от репозитория `../pg` —
все файлы скопированы/адаптированы и живут здесь (`dev-stand/`).

## 1. Профили compose (`dev-stand/docker-compose.yml`, name: `adminpanel-stand`)

| Профиль | Состав | Когда |
|---|---|---|
| `quick` (по умолчанию) | `etcd` + `seed` | цикл бэкенд-разработки: API/алерты на сидированном контроль-плейне, PG не нужна |
| `full` | quick + 2 «шарда» PG (s1a/s1b, s2a/s2b) + patroni-эмуляторы (`hc*`) | live-пробы (Patroni REST, SQL), failover-сценарии, e2e |

Панель (Kestrel) всегда работает на хосте (`dotnet run`); compose-сеть нужна
только её исходящим подключениям. Порт etcd `2379` проброшен на хост;
PG-порты — на хост (`5433`=s1a, `5434`=s1b, `5435`=s2a, `5436`=s2b),
Patroni-REST эмуляторов — `8011/8012/8021/8022`.

## 2. Сервисы

### 2.1. `etcd` (оба профиля)

- `quay.io/coreos/etcd:v3.5.21`, одиночный (как в `../pg`-стенде — этого
  достаточно: панель лишь читает); флаги CLI (gateway `/v3/*` включён по
  умолчанию), named volume `etcd-data` (переживает `docker compose down`).

### 2.2. `seed` (оба профиля, `restart: no`)

Одноразовый контейнер с `etcdctl` (образ на базе `quay.io/coreos/etcd` —
бинарик уже внутри), скрипт `seed.sh` пишет idempotent-сид контроль-плейна:

- кластер `demo`: `config` `{"buckets":16,"dbname":"demo","created_unix":…}`;
- шарды `s1`, `s2`: `dsn` `host=s1a,s1b port=5432 dbname=demo user=postgres`
  (s2 → `s2a,s2b`), `replicas` = `1`;
- `routing` всех 16 бакетов поровну round-robin (8/8);
- **живые** статус-кейи переездов для UI: `bucket_3` = `SYNCING`
  (свежий `updated_unix`), `bucket_7` = `ABORTING` c `phase: "cleanup"`,
  `bucket_11` = `FROZEN` с **протухшим** `updated_unix` (старше порога —
  сразу виден алерт `move-stale`);
- журнал `/clusters/demo/heals/bucket_5` (одна запись `restore-heal`);
- HA-DCS `/service/demo-s1/` и `/service/demo-s2/`: `leader`,
  `members/s1a` (`conn_url: s1a:5432`, `role: master`), `members/s1b`
  (`role: replica`), `optime/leader`, `initialize`;
- стендовая топология `/cluster/nodes/s1a|s1b|s2a|s2b` (в full-профиле её
  вместо сида пишут эмуляторы, в quick — сам сид, чтобы блок «стендовая
  топология» был виден).

Сид перезапускаем (`docker compose run --rm seed`) после `down -v` — скрипт
проверяет пустоту префиксов и не портит существующее состояние.

### 2.3. `s1a/s1b/s2a/s2b` + `hc1a/hc1b/hc2a/hc2b` (профиль `full`)

- PG-ноды: `postgres:18`, `wal_level=logical`, trust (только стенд);
  s1b/s2b — физические реплики через `pg_basebackup -R` (паттерн compose
  скопирован из `../pg/arch/stand/docker-compose.yml`, упрощён: без
  HAProxy/hasync — панель подключается к нодам напрямую по multi-host DSN
  `host=s1a,s1b`, что Npgsql разрешает через `TargetSessionAttributes`).
- `hc*` — patroni-эмуляторы (python, образ из `dev-stand/sidecar/`;
  развитие `../pg/arch/stand/sidecar/rolecheck.py`): 
  - REST `:8008`: `GET /cluster` → JSON в формате Patroni
    (`members[]{name,role,state,timeline,lag,host,port}` — роль определяет
    `pg_is_in_recovery()`), `GET /primary` → 200 только на мастере,
    `GET /replica` — 200 только на реплике;
  - мастер шейвит `/clusters/demo/shards/<X>/master` = `<host>:5432`
    **с lease TTL 5 c** (продление раз в 1–2 c) — воспроизведение
    Patroni-callback `on_role_change`;
  - каждая нода регистрируется в `/service/demo-s<X>/members/<node>` и
    `/cluster/nodes/<node>` (lease TTL 5 c).
  Рестарт-политика эмуляторов — без `always` (урок `../pg`-стенда: рестарт
  сайдкара не должен реанимировать остановленную ноду).

## 3. Проверки (`dev-stand/checks/*.sh`, стиль `../pg/arch/stand/checks/`)

| Скрипт | Сценарий | Ожидание |
|---|---|---|
| `00-up.sh` | `docker compose --profile full up -d` + wait-on-healthy (etcd, PG-реплики, seed) | стенд поднят, сид на месте |
| `10-smoke-api.sh` | панель против стенда: login → 401 без cookie, `/api/overview`, `/api/etcd/status`, `/api/clusters/demo`, `/api/ha/demo-s1`, `/api/alerts` | 200, структура, сидированные данные видны |
| `20-alerts.sh` | seeded-аномалии: FROZEN-протухший → `move-stale`, `bucket_7` → `move-aborting`; удалить `master`-ключ s2 → `shard-no-master` (critical) | алерты появляются ≤ 2 тиков |
| `30-failover.sh` | `docker stop s1a` → lease гаснет → `shard-no-master` + `/service/demo-s1/leader` жив (DCS не мгновенный); promote s1b руками (`pg_ctl promote`) → эмулятор s1b берёт lease, алерт гаснет, Patroni-REST показывает нового мастера | цикл алерт→успокоение |
| `40-live-probes.sh` | `/api/ha/demo-s1` содержит lag/state от Patroni-REST; `/api/clusters/demo` shards[].runtime заполнен (sync-standby, inventory 16/16 routing) | поля не null |
| `90-down.sh` | разбор (с опцией `-v` — стереть данные) | — |

Скрипты — bash+jq (как в `../pg`), гоняются вручную и в рамках задачи
`t10-dev-stand`; CI не требуют (интеграционные тесты используют Testcontainers,
не стенд).

## 4. Отличия от стенда `../pg/arch/stand` (осознанные)

| В `../pg` | У нас | Почему |
|---|---|---|
| HAProxy per-шард + hasync (топология из etcd) | нет HAProxy; DSN multi-host прямо на ноды | панель читает и probe'ит, клиентский роутинг ей не нужен; минус 4 контейнера |
| сайдкар = `/primary` + регистрация IP | то же + полноценный `/cluster` (Patroni-формат) + master-lease шардового ключа | панели нужны Patroni-REST-данные и живой lease-сценарий |
| сайдкары в netns нод (`network_mode: service:…`) | отдельные контейнеры в общей сети | проще пробросить :8008/:5432 наружу для отладки; ценой — IP-адреса нод не совпадают с адресами эмуляторов (не критично: lease-ключ пишет эмулятор с именем хоста ноды) |
| opsbox с etcdctl/psql | нет; роль ops выполняют host-скрипты + `docker compose exec etcd etcdctl` | меньше движущихся частей |

## 5. Быстрый старт (для README)

```bash
# терминал 1 — панель
dotnet run --project src/AdminPanel.Api   # appsettings читает Endpoints=http://localhost:2379

# терминал 2 — стенд
cd dev-stand && checks/00-up.sh           # или: docker compose up -d (quick-профиль: только etcd+seed)

open http://localhost:5000                # логин admin/admin из appsettings.Development.json
```

`appsettings.Development.json` содержит: `AdminPanel:Etcd:Endpoints=http://localhost:2379`,
`AdminPanel:Auth:Username=admin`, `Password=admin`, `AllowHttp=true`,
`AdminPanel:Probes:Password=` (пусто — SQL-проба trust на стенде).
