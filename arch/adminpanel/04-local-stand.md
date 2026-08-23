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
только её исходящим подключениям. Публикация портов на хост (compose
`publish`; фиксируется этой таблицей — compose из `t10-dev-stand` ей
соответствует). Имена **сервисов** compose — канонические (`etcd`, `s1a`, …:
резолвятся DNS-сети стенда — на них построены DSN/`primary_conninfo`/скрипты),
а `container_name` — с префиксом `as-` (`as-etcd`, `as-s1a`, …): имена
контейнеров `../pg`-стенда совпадают, сосуществование двух стендов не должно
рукнуться конфликтом имён (порты `../pg` на хост не публикует — конфликта
портов нет):

| Контейнер | Внутри | На хосте | Ключ `HostMap` (адрес ноды из etcd) |
|---|---|---|---|
| `etcd` | 2379 | 2379 | — (панель использует `Endpoints=http://localhost:2379`) |
| `s1a` | 5432 | 5433 | `s1a:5432` → `127.0.0.1:5433` |
| `s1b` | 5432 | 5434 | `s1b:5432` → `127.0.0.1:5434` |
| `s2a` | 5432 | 5435 | `s2a:5432` → `127.0.0.1:5435` |
| `s2b` | 5432 | 5436 | `s2b:5432` → `127.0.0.1:5436` |
| `hc1a` (эмулятор s1a) | 8008 | 8011 | `s1a:8008` → `127.0.0.1:8011` |
| `hc1b` (эмулятор s1b) | 8008 | 8012 | `s1b:8008` → `127.0.0.1:8012` |
| `hc2a` (эмулятор s2a) | 8008 | 8021 | `s2a:8008` → `127.0.0.1:8021` |
| `hc2b` (эмулятор s2b) | 8008 | 8022 | `s2b:8008` → `127.0.0.1:8022` |

Адреса live-проб панель берёт из etcd (DSN `host=s1a,s1b port=5432`,
Patroni `http://<host>:8008`), но с хоста панели compose-имена не
резолвятся, а `:8008` слушают эмуляторы `hc*` — отдельные контейнеры.
Поэтому каждая проба пропускает адрес через `AdminPanel:Probes:HostMap`
(контракт — [02](02-etcd-contract.md) §6: адрес из etcd → override при
точном совпадении `host:port` → прямое подключение); значения стенда —
последняя колонка таблицы и §2.3. В проде `HostMap` пуст.

## 2. Сервисы

### 2.1. `etcd` (оба профиля)

- `quay.io/coreos/etcd:v3.5.21`, одиночный (как в `../pg`-стенде — этого
  достаточно: панель лишь читает); флаги CLI (gateway `/v3/*` включён по
  умолчанию), named volume `etcd-data` (переживает `docker compose down`).

### 2.2. `seed` (оба профиля, `restart: no`)

Одноразовый контейнер с `etcdctl` (образ `dev-stand/seed/Dockerfile`:
`alpine` + `etcdctl`, скопированный из `quay.io/coreos/etcd:v3.5.21`
`/usr/local/bin/etcdctl` — сам официальный образ distroless, без shell),
скрипт `seed.sh` пишет idempotent-сид контроль-плейна:

- кластер `demo`: `config` `{"buckets":16,"dbname":"demo","created_unix":…}`;
- шарды `s1`, `s2`: `dsn` `host=s1a,s1b port=5432 dbname=demo user=postgres`
  (s2 → `s2a,s2b`), `replicas` = `1`; плюс `master`-ключи **статично**
  (`s1a:5432`, `s2a:5432`) — в quick-профиле эмуляторов нет, ключи живут
  без lease; в full эмуляторы-мастера переписывают их с lease (§2.3);
- `routing` всех 16 бакетов фикс-раскладкой (s1 — 10, s2 — 6) — в точности
  значения `EtcdSeed` интеграционных тестов (seed.sh обязан писать те же);
- **живые** статус-кейи переездов для UI: `bucket_3` = `SYNCING`
  (свежий `updated_unix` = `now − 60`), `bucket_7` = `ABORTING` c
  `phase: "cleanup"` (`now − 900`), `bucket_11` = `FROZEN` с **протухшим**
  `updated_unix` = `now − 7200` (старше порога `StaleMoveSeconds` — сразу
  виден алерт `move-stale`). Времена — динамические от `now`, чтобы
  аномалии были живыми; интеграционные фикстуры (`EtcdSeed`) используют
  зафиксированные эквиваленты той же раскладки (10/6, те же owner/target);
- журнал `/clusters/demo/heals/bucket_5` (одна запись `restore-heal`);
- HA-DCS `/service/demo-s1/` и `/service/demo-s2/`: `leader`,
  `members/s1a` (`conn_url: s1a:5432`, `role: master`), `members/s1b`
  (`role: replica`), `optime/leader`, `initialize` — статично (quick);
  в full эмуляторы переписывают `members/<node>` и `leader`/`optime/leader`
  с lease (§2.3);
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
  Ноды `s*a` — **self-healing**: при пустом `PGDATA` если пир — мастер,
  клонируются от него репликой (иначе первый старт — initdb-мастер);
  паттерн `s2a` из `../pg` — он даёт повторные прогоны e2e после failover
  (30-й чек пересоздаёт s1a репликой s1b).
- Инвентарь: `00-up.sh` создаёт на мастерах БД `demo` и схемы `bucket_%` —
  по одному на **ACTIVE**-бакет владельца (s1 — 8, s2 — 5; статусные
  бакеты переездов исключены — так сверяет `inventory-mismatch`,
  сверка только по ACTIVE). Итого 13 схем; расхождение с routing —
  алерт `inventory-mismatch` (проверяется 40-м чеком с обратным знаком).
- `hc*` — patroni-эмуляторы (python, образ из `dev-stand/sidecar/`;
  развитие `../pg/arch/stand/sidecar/rolecheck.py`): 
  - REST `:8008`: `GET /cluster` → JSON в формате Patroni
    (`members[]{name,role,state,timeline,lag,host,port}`) — всегда 200,
    пока жив контейнер эмулятора: состав членов scope задан env
    (`MEMBERS=s1a,s1b`), каждый опрашивается по PG (роль —
    `pg_is_in_recovery()`); недоступная нода — `state: "stopped"` с
    последней известной ролью (Patroni ведёт себя так же, а панели
    нужна запись с `name` каждого member'а — иначе ошибка пробы);
    `GET /primary` → 200 только на мастере, `GET /replica` — 200 только
    на реплике (503 — PG ноды недоступна);
  - мастер шейвит `/clusters/demo/shards/<X>/master` = `<host>:5432`
    **с lease TTL 5 c** (продление раз в 1 c) — воспроизведение
    Patroni-callback `on_role_change`; тем же lease-циклом мастер пишет
    `/service/<scope>/leader` = `{"name":"<node>"}` и `optime/leader`
    (`pg_current_wal_lsn()`) — смерть мастера гасит все три ключа ≤ 5 c
    (алерты `shard-no-master` + `shard-no-leader`), promote реплики —
    возобновляет от её имени;
  - каждая нода регистрируется в `/service/demo-s<X>/members/<node>`
    (роль/state живьём из PG) и `/cluster/nodes/<node>` (lease TTL 5 c);
    lease продлевается **только пока PG ноды отвечает** — смерть ноды
    убирает её из DCS через TTL, как у Patroni.
  Рестарт-политика эмуляторов — без `always` (урок `../pg`-стенда: рестарт
  сайдкара не должен реанимировать остановленную ноду).

Сопоставление «etcd-адрес ноды → адрес на хосте панели» задаётся настройкой
`AdminPanel:Probes:HostMap` (порядок разрешения — [02](02-etcd-contract.md)
§6). Значения для стенда (соответствуют публикации портов §1) в
`appsettings.Development.json`:

```json
"AdminPanel": {
  "Probes": {
    "HostMap": {
      "s1a__5432": "127.0.0.1:5433",
      "s1b__5432": "127.0.0.1:5434",
      "s2a__5432": "127.0.0.1:5435",
      "s2b__5432": "127.0.0.1:5436",
      "s1a__8008": "127.0.0.1:8011",
      "s1b__8008": "127.0.0.1:8012",
      "s2a__8008": "127.0.0.1:8021",
      "s2b__8008": "127.0.0.1:8022"
    }
  }
}
```

В appsettings ключ записывается как `host__port` (':' в ключах режется
конфиг-провайдерами .NET), канонический формат `host:port` действует в
памяти/тестах и приоритетен при наличии обоих (см. `HostMapResolver`).

SQL-проба идёт на `127.0.0.1:5433/5434` (override `host:port` из DSN),
Patroni-проба — на `127.0.0.1:8011/8012` (override `<host>:8008` для
member'ов scope'а).

## 3. Проверки (`dev-stand/checks/*.sh`, стиль `../pg/arch/stand/checks/`)

| Скрипт | Сценарий | Ожидание |
|---|---|---|
| `00-up.sh` | `docker compose --profile full up -d` + wait-on-healthy (etcd, PG-реплики, seed); затем БД `demo` + 13 схем `bucket_%` (§2.3) и `synchronous_standby_names` (ALTER SYSTEM — паттерн `../pg`: не флагами `-c`) | стенд поднят, сид на месте, реплики streaming |
| `10-smoke-api.sh` | панель против стенда: login → 401 без cookie, `/api/overview`, `/api/etcd/status`, `/api/clusters/demo`, `/api/ha/demo-s1`, `/api/alerts` | 200, структура, сидированные данные видны |
| `20-alerts.sh` | seeded-аномалии: FROZEN-протухший → `move-stale`, `bucket_7` → `move-aborting`; затем `shard-no-master` (critical): в full перед `etcdctl del master`-ключа s2 остановить эмуляторы `hc2a`/`hc2b` (keepalive перепишет ключ), в конце вернуть и дождаться восстановления lease (в quick эмуляторов нет — просто del/put) | алерты появляются ≤ 2 тиков; после восстановления гаснут |
| `30-failover.sh` | `docker stop s1a` → lease гаснет → `shard-no-master` + `shard-no-leader` (`leader`-ключ тоже под lease, §2.3); promote s1b руками (`pg_ctl promote`) → эмулятор s1b берёт lease, алерты гаснут, Patroni-REST показывает нового мастера; финал — rejoin: `docker compose rm -sf s1a && up -d s1a` (self-healing клон от s1b) + sync-names на s1b | цикл алерт→успокоение; стенд снова консистентен для 40 |
| `40-live-probes.sh` | панель на хосте с `HostMap` (§2.3): `/api/ha/demo-s1` содержит lag/state от Patroni-REST (пробы идут на `127.0.0.1:8011/8012`); `/api/clusters/demo` shards[].runtime заполнен (sync-standby, инвентарь 8+5 ACTIVE-схем — `inventory-mismatch` нет; SQL-пробы на `127.0.0.1:5433/5434`) | поля не null, probe-ошибок нет |
| `90-down.sh` | разбор (с опцией `-v` — стереть данные) | — |

Скрипты — bash+jq (как в `../pg`), гоняются вручную и в рамках задачи
`t10-dev-stand`; CI не требуют (интеграционные тесты используют Testcontainers,
не стенд). Полный e2e-прогон — последовательность `00 → 10 → 20 → 30 → 40`
(панель запущена на хосте до 10-го, порядок важен: 30-й меняет топологию s1
и возвращает её в консистентный вид rejoin'ом; 40-й рассчитан на неё);
`90` — разбор. Повторный прогон — с чистого состояния (`90 -v` → `00`).

## 4. Отличия от стенда `../pg/arch/stand` (осознанные)

| В `../pg` | У нас | Почему |
|---|---|---|
| HAProxy per-шард + hasync (топология из etcd) | нет HAProxy; DSN multi-host прямо на ноды | панель читает и probe'ит, клиентский роутинг ей не нужен; минус 4 контейнера |
| сайдкар = `/primary` + регистрация IP | то же + полноценный `/cluster` (Patroni-формат) + master-lease шардового ключа | панели нужны Patroni-REST-данные и живой lease-сценарий |
| сайдкары в netns нод (`network_mode: service:…`) | отдельные контейнеры в общей сети, `:8008`/`:5432` опубликованы на хост (§1) | панель с хоста достигает эмуляторы и PG через `HostMap` (§2.3); lease-ключ пишет эмулятор с именем хоста ноды — номинальные адреса проб в etcd расходятся с реальными слушателями, расхождение закрывает маппинг |
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
`AdminPanel:Probes:Password=` (пусто — SQL-проба trust на стенде),
`AdminPanel:Probes:HostMap` (маппинг адресов проб стенда — §2.3).
