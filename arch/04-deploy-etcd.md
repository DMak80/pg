# 04. Деплой кластера etcd

etcd — фундамент всего. Поднимаем **первым**, до PostgreSQL. На каждой из 3 нод —
по одному узлу etcd. Все 3 образуют кластер с кворумом 2/3.

> Конфиги: `configs/etcd/docker-compose.yml` + `configs/etcd/etcd.env`.

---

## 0. Два применения одного рецепта

Этот документ — рецепт etcd-контура ВООБЩЕ (кворум 2/3, параметры §3). Два
применения различаются моделью адресации и bootstrap:

- **(а) DCS Patroni-стенда** (arch/01–13): etcd на тех же 3 нодах, что и PG
  (pg1/pg2/pg3, static bootstrap, host-network) — §1–7 ниже; локальный мир
  01–13, вне воркер-инсталляции;
- **(б) HA-контур контроль-плейна воркер-инсталляции** (arch/14+): адресация
  ТОЛЬКО по именам узлов в общей docker-сети (мультихост — Swarm), бутстрап —
  seed-узел, состав доводит EtcdWorker; потребители — воркеры
  (PgWorker/KafkaWorker/ValkeyWorker/S3Worker/EtcdWorker), панель AdminPanel
  и Patroni-ноды, создаваемые воркером — §8. Host-network-модель (IP хостов
  в peer-URL) в этом применении НЕ используется.

Параметры кластера одни (§3); процедура потери кворума — общая (09 §4).

---

## 1. Почему именно так

- **3 узла** — минимальное количество для отказоустойчивости (переживает потерю 1).
- **Static bootstrap** (`--initial-cluster` с явным списком) — самый надёжный способ
  стартовать кластер: не зависит от discovery-сервиса, полностью детерминирован.
- **`--initial-cluster-state new`** — флаг «первого старта». При рестарте контейнера
  его **нельзя** ставить снова в `new`; мы выносим его в env-файл, который для первого
  старта используется как есть, потом — меняется/убирается (см. раздел 5).
- **Каждый узел знает своё имя и адрес** (`ETCD_NAME`, `--initial-advertise-peer-urls`).
  Имя — это имя хоста (`pg1`/`pg2`/`pg3`).

---

## 2. Подготовь переменные

На каждой ноде создай каталог и положи env-файл:

```bash
sudo mkdir -p /opt/etcd && cd /opt/etcd
# скопируй configs/etcd/docker-compose.yml сюда
# скопируй configs/etcd/etcd.env сюда и ОТРЕДАКТИРУЙ под конкретную ноду
```

Файл `etcd.env` (пример для **pg1** — на pg2/pg3 меняется только `NODE_NAME` и `NODE_IP`):

```bash
# configs/etcd/etcd.env — пример для pg1
NODE_NAME=pg1
NODE_IP=10.0.0.11

# одно и то же для всех трёх узлов:
ETCD_IMAGE=quay.io/coreos/etcd:v3.5.21
CLUSTER_TOKEN=pgcluster-etcd-token-2026
PEERS=pg1=http://10.0.0.11:2380,pg2=http://10.0.0.12:2380,pg3=http://10.0.0.13:2380
DATA_DIR=/data/etcd

# bootstrap flag. После первого успешного старта кластера — перевести в existing
INITIAL_CLUSTER_STATE=new
```

> ⚠️ `PEERS` и `CLUSTER_TOKEN` **должны быть одинаковыми** на всех трёх узлах.
> `NODE_NAME`/`NODE_IP` — **своими** на каждом.

---

## 3. docker-compose.yml (узел etcd)

> Полная версия в `configs/etcd/docker-compose.yml`. Здесь — с пояснениями.

```yaml
# /opt/etcd/docker-compose.yml
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.21
    container_name: etcd
    restart: unless-stopped
    env_file: etcd.env
    network_mode: host          # самый простой способ: контейнер видит IP хоста как свой
    volumes:
      - ${DATA_DIR}:/data
    command:
      - /usr/local/bin/etcd
      - --name=${NODE_NAME}
      - --data-dir=/data
      - --listen-peer-urls=http://0.0.0.0:2380
      - --listen-client-urls=http://0.0.0.0:2379
      - --initial-advertise-peer-urls=http://${NODE_IP}:2380
      - --advertise-client-urls=http://${NODE_IP}:2379
      - --initial-cluster=${PEERS}
      - --initial-cluster-token=${CLUSTER_TOKEN}
      - --initial-cluster-state=${INITIAL_CLUSTER_STATE}
      # важные тайминги/настройки
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
      - --quota-backend-bytes=8589934592   # 8 ГБ лимт (с запасом)
```

### Ключевые флаги — что значат
| Флаг | Смысл |
|---|---|
| `--initial-advertise-peer-urls` | адрес, который **этот узел сообщает другим** для raft. **Должен быть IP этой ноды.** |
| `--advertise-client-urls` | адрес для клиентов (Patroni). Тоже **IP этой ноды.** |
| `--initial-cluster` | список всех узлов кластера в формате `name=url`. **Одинаковый на всех.** |
| `--initial-cluster-state` | `new` при первом старте кластера; `existing` при перезапуске/добавлении узла. |
| `--heartbeat-interval` / `--election-timeout` | raft тайминги (мс). На стабильной LAN — 250/2000 достаточно. |
| `--auto-compaction-retention` | автоочистка старых ревизий (иначе etcd пухнет). 1 час — норм. |

> Почему `network_mode: host`? Упрощение: не надо пробрасывать порты, контейнер сразу
> видит IP хоста, peer-ссылки работают «как есть». Для прод можно перейти на overlay-сеть
> ( Swarm/K8s) или macvlan, но для 3 нод host-сеть — адекватный выбор.

---

## 4. Запуск (по очереди, но в целом параллельно)

На каждой ноде:

```bash
cd /opt/etcd
# ПЕРВЫЙ запуск кластера — все три ноды стартуют с INITIAL_CLUSTER_STATE=new.
# Важно стартовать в пределах election-timeout друг от друга (можно почти одновременно).
# --env-file обязателен: интерполяция ${…} в YAML идёт из env compose, а не из env_file
# (тот передаёт переменные только внутрь контейнера) — без него up падает на пустых значениях.
docker compose --env-file etcd.env up -d
docker compose logs -f etcd | head -40      # смотрим, что raft сошёлся
```

### Проверка здоровья кластера (с любой ноды)
```bash
export ETCDCTL_API=3
etcdctl --endpoints=http://pg1:2379,http://pg2:2379,http://pg3:2379 \
        endpoint health --cluster

# ожидаемый вывод:
# http://10.0.0.11:2379 is healthy: successfully committed proposal...
# http://10.0.0.12:2379 is healthy: successfully committed proposal...
# http://10.0.0.13:2379 is healthy: successfully committed proposal...

# статус членов кластера
etcdctl --endpoints=http://pg1:2379 member list -w table
```

Должно быть **3 члена**, статус `started`, isLeader=true у одного.

---

## 5. После первого старта: переключить в `existing`

Кластер сформирован. Теперь при рестарте контейнера флаг `--initial-cluster-state=new`
**не нужен и может навредить** (etcd начнёт «искать новый кластер»). Поэтому:

```bash
# на каждой ноде:
sed -i 's/^INITIAL_CLUSTER_STATE=new/INITIAL_CLUSTER_STATE=existing/' /opt/etcd/etcd.env
docker compose --env-file etcd.env up -d      # применить (контейнер перезапустится с новым флагом)
```

> Подробно о том, что делать при крахе всего etcd-кластера — в `09-troubleshooting.md`.

---

## 6. Типовые проблемы на этом шаге

| Симптом | Причина / fix |
|---|---|
| `connection refused` на peer 2380 | firewall закрывает 2380, либо неверный `NODE_IP`. |
| Кластер сходится как 3 отдельных | разные `CLUSTER_TOKEN` или `--initial-cluster` между нодами. |
| `apply entries took too long` | медленный диск под `/data/etcd` → ставь на SSD. |
| `failed to sync commit` | нет места на диске или квота (`--quota-backend-bytes`). |
| При рестарте etcd не входит в кластер | не сменил `INITIAL_CLUSTER_STATE=new` → `existing`. |

---

## 7. Чек-лист

```text
[ ] На всех 3 нодах: /opt/etcd/{docker-compose.yml,etcd.env} (со своими NODE_NAME/NODE_IP)
[ ] etcd.env: PEERS, CLUSTER_TOKEN одинаковые на всех; DATA_DIR=/data/etcd
[ ] INITIAL_CLUSTER_STATE=new на момент первого старта
[ ] docker compose --env-file etcd.env up -d на всех трёх
[ ] etcdctl endpoint health --cluster → все 3 healthy
[ ] etcdctl member list → 3 started
[ ] INITIAL_CLUSTER_STATE переключён в existing на всех нодах
```

## 8. HA-контур контроль-плейна воркер-инсталляции

Контроль-плейн (декларации кластеров, координация воркеров, Patroni-DCS
создаваемых нод, мастер-ключи) живёт в ОТДЕЛЬНОМ от PG-нод etcd-контуре.
Отказ единственного etcd = заморозка надзора, панели и DCS (характеристика R,
reliability-report) — контур обязан быть 3-узловым. Бутстрап контура —
оператор: ОДИН **seed-узел** (рецепт `deploy/etcd/` — compose-файл одного
узла в общей сети, peer-URL по имени узла); состав до целевого (нечётное
число, рекомендация 3) доводит оркестратор EtcdWorker по декларации
`/etcd/config` ([etcdworker/00-etcd-contract.md](etcdworker/00-etcd-contract.md)).

1. **Топология — адресация только по именам узлов** (IP хостов и
   host-network в контуре воркер-инсталляции НЕ используются): peer-URL
   каждого узла — `http://<имя-узла>:2380` в ОБЩЕЙ docker-сети контура
   (`etcdw-net`; managed-узлы `etcdw-node-<k>`, seed — имя из env рецепта).
   Мультихост — ТОЛЬКО Swarm (имена резолвятся overlay-сетью; managed-узлы —
   сервисы, разносятся по swarm-нодам — анти-аффинити кворума: потеря
   docker-ноды ≠ потеря кворума); plain docker — контур живёт на ОДНОМ
   docker-хосте (имена резолвятся общей сетью). Требования к машине —
   SSD под data-dir, NTP.
2. **Параметры** — те же, что §3: `--heartbeat-interval=250`,
   `--election-timeout=2000`, `--auto-compaction-retention=1`,
   `--quota-backend-bytes=8GiB`, образ v3.5.21; `--initial-cluster-state=new`
   — только seed на первом старте; managed-узлы всегда `existing`
   (членство задаётся member-API до старта контейнера, arch/25 §3.1).
3. **Advertised-правила потребителей** (таблица источников — под списком,
   п.3а): клиенты контура всегда получают список ВСЕХ клиентских URL;
   единственный endpoint допустим только для стендов/разработки.
   Peer-адресация — имена сети (п.1); клиентская грань — host:port
   публикаций (клиентский порт узла публикуется на хост — portalloc
   EtcdWorker / публикации compose); `--advertise-client-urls` — СПИСОК:
   `http://<имя-узла>:2379` (сеть контура) + `http://<advertised-host>:<port>`
   (потребители вне сети — прецедент `as-etcd` стенда). В список включаются
   ТОЛЬКО адреса, резолвимые для потребителей контура: внутрисетевой алиас,
   не существующий в сетях потребителей, в advertise НЕ включается — Patroni
   ретраит весь список целиком, недостижимый URL даёт постоянные
   NXDOMAIN-ретраи (`getaddrinfo` empty в логах нод) и ложную деградацию
   DCS-сессии.
4. **Кворум-семантика**: 1 узел недоступен — всё работает (клиенты с failover
   даже не обязаны переключаться); большинства нет — кворум потерян:
   контроль-плейн заморожен (надзор/панель/DCS), датаплейн живёт сам;
   восстановление — 09 §4. Потеря узла навсегда: managed-узел заменяет
   автоматика EtcdWorker (member remove + add learner + promote, arch/25
   §3.1); seed/внешние узлы — runbook (member remove/add — peer-URL по
   имени узла той же сети контура). После доводки состава seed
   рекомендуется вывести (контур полностью managed): `etcdctl member
   remove <seed-ID>` + `docker compose down` — runbook-шаг.
5. **Бутстрап (оператор)**: `docker network create etcdw-net` (если нет) →
   `deploy/etcd`: `docker compose --env-file etcd.env up -d` (один узел,
   `--initial-cluster-state=new`, peer `http://<NODE_NAME>:2380` в сети
   `etcdw-net`) → EtcdWorker подключается (env endpoints), панель/API
   заявляет `nodes=N` → EtcdWorker доводит состав (learner + promote,
   managed-узлы в `etcdw-net` / Swarm-overlay) → seed выводится (п.4).
   Swarm-контур: seed поднимается сервисом Swarm в overlay-сети
   (`docker network create -d overlay etcdw-net`), далее — тот же путь
   доводки.
6. **Чек-лист контура (прод)**: `member list` = N started (все peer-URL —
   имена, без IP); `endpoint health --cluster` = все healthy; все
   воркеры/панель видят один и тот же список endpoints; healthz воркеров
   `etcd-reachable` жив; панель `GET /api/etcd/status` — N членов с единым
   leader/term.
7. **Стендовое зеркало**: дев-стенд поднимает тот же 3-узловой контур
   внешними compose-узлами (`dev-stand/adminpanel/docker-compose.yml`,
   `etcd1/etcd2/etcd3` — peer по именам compose-сети); EtcdWorker стенда —
   наблюдение (декларация не ставится, managed нет); отказоустойчивость
   гоняет чек `43-etcd-ha.sh` на каждом прогоне стенда.

Таблица к п.3 (источники адресов потребителей):

| Потребитель | Откуда берёт адреса | Формат |
|---|---|---|
| Воркеры (deploy) | env `PGW/KFW/VWK/S3W/ETCDW_ETCD_ENDPOINT_0..2` | полные URL |
| Панель | `AdminPanel__Etcd__Endpoints__0..2` | полные URL |
| Patroni-ноды (Spilo) | `PgWorker:Etcd:AdvertisedEndpoints` (fallback `Endpoints`) → `ETCD3_HOSTS` | `host:port` без scheme, список |
| lease мастер-ключа нод | тот же источник → `PGW_ETCD` | полные URL, список через запятую |

Кластер DCS готов → [05-deploy-postgres.md](05-deploy-postgres.md).
