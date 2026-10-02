# t09-ha-etcd-canon — план реализации (канон + чеки HA-etcd, 3-нодовый контур)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Устранить SPOF контроль-плейна: канон 3-узлового etcd-контура (arch/04), деплой-рецепт `deploy/etcd/`, стенд на 3 узлах по дефолту, чек `43-etcd-ha.sh`, перебор endpoints в `master-lease.py`, docker-E2E HA-сценарий.

**Architecture:** Код C# (воркеры/панель) уже умеет failover по списку endpoints — меняются только канон (arch/), деплой-конфиги, стенд, два Python-артефакта периферии (`docker/node/master-lease.py`, `dev-stand/adminpanel/sidecar/emulator.py`) и тестовая инфраструктура E2E (опция `haEtcd`). Контур всегда ОДИН (3 узла = один кластер, кворум 2/3).

**Tech Stack:** docker compose, etcd v3.5.21 (образ уже в `dev-stand/images/images.txt`), bash-чеки, Python 3 (stdlib urllib), C# testcontainers (DotNet.Testcontainers), xUnit.

**Spec:** `docs/superpowers/2026-10-01-t09-ha-etcd-canon/spec.md` (в этой же папке; исполнитель читает spec + план).

## Global Constraints

- **НЕ-цели spec §1.3 (критично):** НЕ трогаем `src/PgWorker.*`, `src/KafkaWorker.*`, `src/ValkeyWorker.*`, `src/Shared.*`, `src/AdminPanel.*`. Единственные правки кода: `docker/node/master-lease.py`, `dev-stand/adminpanel/sidecar/emulator.py` и тесты (`src/tests/**`, `dev-stand/**`, `deploy/**`). Выяснилось, что нужен C#-код воркеров → СТОП и пересогласование.
- **arch-first:** задача 1 (arch/) коммитится РАНЬШЕ всего остального (spec §12.1).
- Контур всегда один: 3 узла = ОДИН etcd-кластер; никаких «вторых etcd» (AGENTS.md, spec §2.2).
- Только существующие образы: `quay.io/coreos/etcd:v3.5.21` уже в `dev-stand/images/images.txt`; НОВЫХ внешних образов нет; локально собираемые образы в registry 192.168.0.1:5000 НЕ класть.
- Прод-шаблон `deploy/etcd/` использует образ из локального registry: `192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`; стенд — `quay.io/coreos/etcd:v3.5.21` как сейчас.
- Docker-тесты: хост-порты только динамические (зонд `E2eFixture.FreePort()` / `assignRandomHostPort: true`), никаких литералов портов в expects; бюджеты фикстур ≤ 100 с.
- Зачистка после КАЖДОЙ docker-серии: дожидаться финальной строки прогона, затем чистить свои контейнеры/сети (правила AGENTS.md); перед стендом — `dev-stand/images/pull-images.sh`.
- Комментарии тестов — AAA-нотация (`// Arrange`, `// Act`, `// Assert`).
- `TreatWarningsAsErrors=true` — сборка без ворнингов; тексты/комментарии — по-русски, идентификаторы — английские.
- Коммит в feature-ветке — свободно; в `main` — только по явному приказу (базовые правила dev-flow).
- Порядок задач = порядок spec §10: arch → deploy → стенд → код → чеки → E2E → docs; roadmap-чистка — тем же мерж-коммитом (задача 8).
- Все команды выполняются из корня WORKTREE `/Users/demakaev/ZCodeProject/worktrees/feat-t09-ha-etcd-canon` (ниже — `$WT`).

---

### Task 1: Канон arch/04 §0+§8 и ссылки из соседних arch-документов

**Вход (предусловие):** worktree чист (кроме `docs/superpowers/...`), spec одобрен.

**Files:**
- Modify: `arch/04-deploy-etcd.md` (новые §0 и §8; §1–7 без структурных правок)
- Modify: `arch/09-troubleshooting.md` (§0 и §4 — ссылки на arch/04 §8)
- Modify: `arch/14-pgworker.md` (~строка 304, §2.2 Patroni DCS; ~строка 1284, §8 `PgWorker:Etcd:Endpoints[]`)
- Modify: `arch/16-kafkaworker.md` (~строка 824, §8 `KafkaWorker:Etcd { Endpoints[] }`)
- Modify: `arch/21-valkeyworker.md` (~строка 537, §8 `ValkeyWorker:Etcd { Endpoints[] }`)
- Modify: `arch/17-synchronization-principles.md` (~строка 28, упоминание `as-etcd`)
- Modify: `arch/adminpanel/04-local-stand.md` (§1 таблица портов, §2.1 — кластер etcd1/2/3, `PGW_ETCD_ENDPOINT_0..2`)

**Выход:** arch-канон описывает HA-контур контроль-плейна; все ссылки ведут на arch/04 §8; ничего кроме `arch/**` в коммите нет.

**Spec:** §3 (весь), §5 п.4 (arch/adminpanel/04), §10 фаза 1, §12.1.

- [x] **Step 1.1: arch/04 — вставить §0 «Два применения одного рецепта»** сразу после заголовка/лида (до «## 1.»), текст:

```markdown
## 0. Два применения одного рецепта

Этот документ — рецепт etcd-контура ВООБЩЕ (3 узла, static bootstrap, кворум 2/3).
Два применения различаются размещением и advertised-правилами:

- **(а) DCS Patroni-стенда** (arch/01–13): etcd на тех же 3 нодах, что и PG
  (pg1/pg2/pg3, host-network) — §1–7 ниже;
- **(б) HA-контур контроль-плейна воркер-инсталляции** (arch/14+): etcd — внешний
  контур, «запускается отдельно» (deploy/docker-compose.yml), потребители —
  воркеры PgWorker/KafkaWorker/ValkeyWorker, панель AdminPanel и Patroni-ноды,
  создаваемые воркером — §8.

Параметры кластера одни (§3); процедура потери кворума — общая (09 §4).
```

- [x] **Step 1.2: arch/04 — дописать §8 в конец файла** (после §7, до финальной строки «Кластер DCS готов → …», которую оставить последней), текст:

```markdown
## 8. HA-контур контроль-плейна воркер-инсталляции

Контроль-плейн (декларации кластеров, координация воркеров, Patroni-DCS
создаваемых нод, мастер-ключи) живёт в ОТДЕЛЬНОМ от PG-нод etcd-контуре.
Отказ единственного etcd = заморозка надзора, панели и DCS (характеристика R,
reliability-report) — контур обязан быть 3-узловым. Рецепт узла:
`deploy/etcd/{docker-compose.yml,etcd.env.example}` (зона оператора).

1. **Топология**: 3 узла, static bootstrap (`--initial-cluster`, токен один),
   кворум 2/3, ПО ОДНОМУ узлу на docker-хосте — хосты РАЗНЫЕ (анти-аффинити:
   потеря одного хоста ≠ потеря кворума; допустимо совмещение с docker-хостами
   воркера, но не всех трёх на одном). Требования к хосту — как §2/03: SSD под
   data-dir, стабильные IP/DNS, NTP.
2. **Параметры** — те же, что §3: `--heartbeat-interval=250`,
   `--election-timeout=2000`, `--auto-compaction-retention=1`,
   `--quota-backend-bytes=8GiB`, образ v3.5.21; `--initial-cluster-state=new`
   только на первом старте, затем `existing` (§5 — общий).
3. **Advertised-правила потребителей** (ключевое отличие от (а)):

   | Потребитель | Откуда берёт адреса | Формат |
   |---|---|---|
   | Воркеры (deploy) | env `PGW/KFW/VWK_ETCD_ENDPOINT_0..2` | полные URL |
   | Панель | `AdminPanel__Etcd__Endpoints__0..2` | полные URL |
   | Patroni-ноды (Spilo) | `PgWorker:Etcd:AdvertisedEndpoints` (fallback `Endpoints`) → `ETCD3_HOSTS` | `host:port` без scheme, список |
   | lease мастер-ключа нод | тот же источник → `PGW_ETCD` | полные URL, список через запятую |

   Правило: клиенты контура всегда получают список ВСЕХ клиентских URL;
   единственный endpoint допустим только для стендов/разработки. Advertise
   каждого узла — адрес(а), резолвимые из КАЖДОЙ сети потребителей: etcd
   допускает список в `--advertise-client-urls` (на стенде — compose-DNS для
   сети стенда + `host.docker.internal:PORT` для per-cluster сетей воркера;
   в проде — IP хоста узла, host-network как §3).
4. **Кворум-семантика**: 1 узел недоступен — всё работает (клиенты с failover
   даже не обязаны переключаться); 2 узла — кворума нет: контроль-плейн
   заморожен (надзор/панель/DCS), датаплейн живёт сам; восстановление —
   09 §4. Потеря узла НАВСЕГДА (замена хоста): `member remove` + `member add`
   + data-dir заново:
   ```bash
   etcdctl member remove <ID>                          # на живом члене
   # на НОВОМ хосте: очистить data-dir, в etcd.env — INITIAL_CLUSTER_STATE=existing
   etcdctl member add etcdN --peer-urls=http://<NEW_IP>:2380
   # обновить PEERS на всех узлах (добавленный адрес) и перезапустить узел
   ```
5. **Чек-лист контура (прод)**: `member list` = 3 started; `endpoint health
   --cluster` = 3 healthy; все воркеры/панель видят один и тот же список
   endpoints; healthz воркеров `etcd-reachable` жив; панель `GET
   /api/etcd/status` — 3 члена с единым leader/term.
6. **Стендовое зеркало**: дев-стенд поднимает тот же 3-узловой контур
   (`dev-stand/adminpanel/docker-compose.yml`, `etcd1/etcd2/etcd3`);
   отказоустойчивость гоняет чек `43-etcd-ha.sh` на каждом прогоне стенда.
```

- [x] **Step 1.3: ссылки из соседних документов.** Точечные правки (по одной строке/абзацу, без перестройки разделов):
  - `arch/09-troubleshooting.md` §4, первый абзац: дописать «(рецепт HA-контура контроль-плейна — [04](04-deploy-etcd.md) §8)»; §0 (первая диагностика) — добавить строку «etcd-контур (3 узла): arch/04 §8; чек-лист контура — там же п.5».
  - `arch/14-pgworker.md` §2.2, абзац про `PgWorker:Etcd:AdvertisedEndpoints` (~строка 304): дописать «HA-контур контроль-плейна: список ВСЕХ узлов etcd — [04](04-deploy-etcd.md) §8 (advertised-правила потребителей)». §8 конфиг-блок (~строка 1284, строка `PgWorker:Etcd:Endpoints[]`): дописать комментарий `# список всех узлов HA-контура — 04 §8`.
  - `arch/16-kafkaworker.md` §8 (~строка 824, строка `KafkaWorker:Etcd { Endpoints[] }`): тот же комментарий `# список всех узлов HA-контура — 04 §8 (env KFW_ETCD_ENDPOINT_0..2)`.
  - `arch/21-valkeyworker.md` §8 (~строка 537, строка `ValkeyWorker:Etcd { Endpoints[] }`): аналогично с `VWK_ETCD_ENDPOINT_0..2`.
  - `arch/17-synchronization-principles.md` (~строка 28): «стендовый as-etcd» → «стендовый as-etcd-1 (3-узловой контур, arch/adminpanel/04)».
  - `arch/adminpanel/04-local-stand.md`:
    - §1, таблица «Контейнер/Внутри/На хосте»: строку `etcd | 2379 | 2379` заменить на три строки `etcd1 | 2379 | 2379 (env ETCD1_HOST_PORT)`, `etcd2 | 2379 | 2381 (env ETCD2_HOST_PORT)`, `etcd3 | 2379 | 2383 (env ETCD3_HOST_PORT)`.
    - Абзац «`as-etcd` — единственный etcd полной системы…» (~строки 51–54) переписать: контур — 3 узла `as-etcd-1/2/3` ОДНИМ кластером (кворум 2/3, рецепт — arch/04 §8, зеркало прода); PgWorker из `deploy/` подключается по `PGW_ETCD_ENDPOINT_0..2=http://host.docker.internal:2379/2381/2383` (публикации трёх узлов).
    - §2.1 «`etcd` (оба профиля)»: «одиночный» → «3 узла `etcd1/etcd2/etcd3` одним static-bootstrap кластером (arch/04 §8; стенд зеркалит прод — надёжность дефолтом)», named volumes `etcd1-data/etcd2-data/etcd3-data`, healthcheck `etcdctl endpoint health`.
    - §3, таблица чеков: добавить строку `| 43-etcd-ha.sh (t09) | кворум/health/member list; stop as-etcd-2 → запись/healthz/master-ключ живы; start → снова 3/3 | каждый шаг ≤ бюджета |`.

- [x] **Step 1.4: Проверка.** Убедиться, что §0/§8 на месте и все ссылки живы:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t09-ha-etcd-canon
grep -n "^## 0\.\|^## 8\." arch/04-deploy-etcd.md          # → §0 и §8 есть
grep -rn "04-deploy-etcd.md.*§8\|04 §8" arch/09-troubleshooting.md arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md
grep -n "as-etcd-1/2/3\|etcd1/etcd2/etcd3" arch/adminpanel/04-local-stand.md
grep -rn "as-etcd\b" arch/ | grep -v "as-etcd-1"            # → пусто (кроме архивных docs/)
```
Ожидание: §0/§8 найдены; ссылки есть в 4 файлах; устаревших одиночных `as-etcd` в arch/ не осталось (в `docs/superpowers/` архив не трогаем).

- [x] **Step 1.5: Commit** (только `arch/`):

```bash
git add arch/
git commit -m "docs(t09): arch/04 §0+§8 — канон HA-etcd контура контроль-плейна (3 узла, advertised-правила, кворум, потеря узла, чек-лист); ссылки из arch/09/14/16/21/17 + arch/adminpanel/04"
```

---

### Task 2: Деплой — шаблон узла `deploy/etcd/` и списки endpoints воркеров

**Вход (предусловие):** задача 1 закоммичена (arch-first).

**Files:**
- Create: `deploy/etcd/docker-compose.yml`
- Create: `deploy/etcd/etcd.env.example`
- Modify: `deploy/docker-compose.yml` (строки 16, 94, 128 — env-анкоры трёх воркеров)
- Modify: `deploy/.env.example` (блок `PGW_ETCD_ENDPOINT`)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (~строка 88 — комментарий блока 1b: имена env; правка текста, подъём стенда проверяется в Task 3)

**Interfaces:**
- Produces (для задачи 3 и стенда): env-имена `PGW_ETCD_ENDPOINT_0/1/2`, `KFW_ETCD_ENDPOINT_0/1/2`, `VWK_ETCD_ENDPOINT_0/1/2`; стендовые дефолты `http://host.docker.internal:2379|2381|2383`.

**Выход:** прод-оператор может поднять 3 узла контура копированием `deploy/etcd/` на 3 хоста; все три воркера получают `Endpoints__0..2`.

**Spec:** §4 (весь), §12.2.

- [x] **Step 2.1: `deploy/etcd/etcd.env.example`** — создать:

```bash
# deploy/etcd/etcd.env.example — env ОДНОГО узла HA-контура контроль-плейна
# (arch/04 §8; зона оператора воркер-инсталляции). Копируется на каждый из
# 3 хостов вместе с docker-compose.yml; на каждом узле РЕДАКТИРУЕТСЯ только
# NODE_NAME/NODE_IP (паттерн arch/04 §2). PEERS/CLUSTER_TOKEN — ОДИНАКОВЫЕ
# на всех трёх. Образ — из локального registry (docs/runbook.md).
NODE_NAME=etcd1
NODE_IP=10.0.1.11

# одно и то же для всех трёх узлов:
ETCD_IMAGE=192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21
CLUSTER_TOKEN=pgworker-controlplane-2026
PEERS=etcd1=http://10.0.1.11:2380,etcd2=http://10.0.1.12:2380,etcd3=http://10.0.1.13:2380
DATA_DIR=/data/etcd

# bootstrap-флаг: ПОСЛЕ первого успешного старта кластера перевести в
# existing на ВСЕХ узлах (arch/04 §5 — общий для двух применений).
INITIAL_CLUSTER_STATE=new
```

- [x] **Step 2.2: `deploy/etcd/docker-compose.yml`** — создать (по образцу `arch/configs/etcd/docker-compose.yml`, с шапкой «зона оператора»):

```yaml
# Узел HA-контура контроль-плейна воркер-инсталляции (t09, arch/04 §8).
# ПОДНИМАЕТСЯ ОПЕРАТОРОМ на 3 РАЗНЫХ docker-хостах (анти-аффинити):
#   sudo mkdir -p /opt/etcd && cd /opt/etcd
#   # скопировать сюда docker-compose.yml + etcd.env (из etcd.env.example,
#   # ОТРЕДАКТИРОВАТЬ NODE_NAME/NODE_IP под узел)
#   docker compose up -d        # первый старт — все 3 узла в пределах election-timeout
# Проверка контура и перевод INITIAL_CLUSTER_STATE=existing — arch/04 §4/§5/§8.
services:
  etcd:
    image: ${ETCD_IMAGE}
    container_name: etcd
    restart: unless-stopped
    env_file: etcd.env
    network_mode: host          # peer/client работают «как есть» на IP хоста (arch/04 §3)
    logging:
      driver: json-file
      options: { max-size: "10m", max-file: "3" }
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
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
      - --quota-backend-bytes=8589934592   # 8 ГБ (arch/04 §3)
```

- [x] **Step 2.3: `deploy/docker-compose.yml` — три слота endpoints.** Заменить в `x-pgworker-env` (строка 16), `x-kafkaworker-env` (строка 94), `x-valkeyworker-env` (строка 128) по одному ключу на три (дефолты — стендовые публикации узлов 2379/2381/2383, spec §4 п.2; узла 2/3 может не быть в не-HA прогоне — failover клиентов просто переберёт список):

```yaml
    # HA-контур контроль-плейна (t09, arch/04 §8): список ВСЕХ клиентских URL;
    # дефолты — стендовые публикации (as-etcd-1/2/3), прод переопределяет в
    # deploy/.env на адреса узлов deploy/etcd. Узла 2/3 может не быть (не-HA
    # прогон) — failover переберёт список, недоступный endpoint пропускается.
    PgWorker__Etcd__Endpoints__0: ${PGW_ETCD_ENDPOINT_0:-http://host.docker.internal:2379}
    PgWorker__Etcd__Endpoints__1: ${PGW_ETCD_ENDPOINT_1:-http://host.docker.internal:2381}
    PgWorker__Etcd__Endpoints__2: ${PGW_ETCD_ENDPOINT_2:-http://host.docker.internal:2383}
```

Симметрично (префиксы `KafkaWorker__Etcd__Endpoints__0..2: ${KFW_ETCD_ENDPOINT_0..2:-…}` и `ValkeyWorker__Etcd__Endpoints__0..2: ${VWK_ETCD_ENDPOINT_0..2:-…}`); старые одноэлементные строки `…__Endpoints__0: ${PGW/KFW/VWK_ETCD_ENDPOINT:-…}` удалить. Комментарий в шапке файла «etcd запускается отдельно (dev-stand/compose.yaml или свой кластер)» → «etcd запускается отдельно: HA-контур — `deploy/etcd/` на 3 хостах (arch/04 §8); не-HA прогон — один endpoint».

- [x] **Step 2.4: `deploy/.env.example`** — блок etcd (строки 14–16) заменить на:

```bash
# etdc-контур контроль-плейна (t09, arch/04 §8): СПИСОК всех клиентских URL
# трёх узлов. Стендовые дефолты — публикации as-etcd-1/2/3 (2379/2381/2383).
# ПРОД: адреса трёх узлов контура deploy/etcd (arch/04 §8); допустимы 1–2
# узла (не-HA прогон) — failover клиентов перебирает список.
PGW_ETCD_ENDPOINT_0=http://host.docker.internal:2379
PGW_ETCD_ENDPOINT_1=http://host.docker.internal:2381
PGW_ETCD_ENDPOINT_2=http://host.docker.internal:2383
# KafkaWorker/ValkeyWorker — тот же контур (симметрично; дефолты compose).
KFW_ETCD_ENDPOINT_0=http://host.docker.internal:2379
KFW_ETCD_ENDPOINT_1=http://host.docker.internal:2381
KFW_ETCD_ENDPOINT_2=http://host.docker.internal:2383
VWK_ETCD_ENDPOINT_0=http://host.docker.internal:2379
VWK_ETCD_ENDPOINT_1=http://host.docker.internal:2381
VWK_ETCD_ENDPOINT_2=http://host.docker.internal:2383
```

(опечатку «etdc» не копировать — «etcd».)

- [x] **Step 2.5: `checks/00-up.sh` — комментарий блока 1b (~строки 86–89) под новые имена** (spec §4 п.3 относит это к деплой-фазе): `as-etcd` → `as-etcd-1/2/3`, `PGW_ETCD_ENDPOINT=host.docker.internal:2379` → `PGW_ETCD_ENDPOINT_0..2=host.docker.internal:2379/2381/2383 (дефолты deploy/docker-compose.yml, t09)`. Только текст комментария — функциональные правки 00-up.sh (ect()/wait кворума) делает Task 3 вместе с compose стенда.

- [x] **Step 2.6: Проверка (валидация compose, без подъёма):**

```bash
# env_file: etcd.env резолвится от compose-файла — для config-валидации
# подкладываем копию примера и убираем за собой (в git не попадает):
cp deploy/etcd/etcd.env.example deploy/etcd/etcd.env
docker compose -f deploy/etcd/docker-compose.yml --env-file deploy/etcd/etcd.env config >/dev/null \
  && echo OK-etcd-node
rm deploy/etcd/etcd.env
( cd deploy && docker compose --env-file .env.example config 2>/dev/null | grep -c "Endpoints__[12]" ) # → 6 (3 воркера × 2 слота)
grep -c "ETCD_ENDPOINT_[12]" deploy/.env.example   # → 6
grep -rn "PGW_ETCD_ENDPOINT\b\|KFW_ETCD_ENDPOINT\b\|VWK_ETCD_ENDPOINT\b" deploy/ dev-stand/ docs/runbook.md | grep -v "_[012]"  # → пусто
```

- [x] **Step 2.7: Commit:**

```bash
git add deploy/ dev-stand/adminpanel/checks/00-up.sh
git commit -m "feat(t09): deploy/etcd — шаблон узла 3-нодового HA-контура (static bootstrap, arch/04 §8); воркеры получают Endpoints__0..2 (PGW/KFW/VWK), HA-дефолты в .env.example; комментарий 00-up.sh под новые env-имена"
```

---

### Task 3: Дев-стенд — 3 узла etcd, потребители на списках, эмулятор с перебором

**Вход (предусловие):** задачи 1–2 закоммичены; образы стенда свежие (`bash dev-stand/images/pull-images.sh`); порт 2379 на хосте свободен или занят только текущим стендом.

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (сервис `etcd` → `etcd1/etcd2/etcd3`; env потребителей)
- Modify: `dev-stand/adminpanel/sidecar/emulator.py` (перебор `ETCD_ENDPOINTS`)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (`ect()`, wait кворума, комментарий)
- Modify: `dev-stand/adminpanel/checks/66-kafka-worker-churn.sh` (`as-etcd` → `as-etcd-1`)
- Modify: `dev-stand/adminpanel/README.md` (3-узловой контур + чек 43)

**Interfaces:**
- Consumes: env-имена `*_ETCD_ENDPOINT_0..2` (задача 2), hosts-публикации `2379/2381/2383` (env `ETCD{1,2,3}_HOST_PORT`).
- Produces (для задач 4–6): контур `as-etcd-1/2/3`, чек-клиент `docker exec as-etcd-1 etcdctl`, compose-DNS `etcd1/etcd2/etcd3`.

**Выход:** quick и full поднимают ОДИН 3-узловой кластер; все потребители стенда настроены на списки; эмулятор переживает stop узла.

**Spec:** §5 (весь), §12.3.

- [x] **Step 3.1: compose — кластер.** В `dev-stand/adminpanel/docker-compose.yml` заменить сервис `etcd` (строки 10–25) на три сервиса (общий анкор, peer по compose-DNS, клиентские публикации 2379/2381/2383, advertise двумя URL — сеть стенда + host.docker.internal для per-cluster сетей воркера, healthcheck — `ectd endpoint health`; volumes `etcd1-data/etcd2-data/etcd3-data` вместо `etcd-data`):

```yaml
  # HA-контур: 3 узла ОДНИМ кластером (t09, arch/04 §8; стенд зеркалит прод —
  # надёжность дефолтом, прецедент t07). Peer — compose-DNS; клиентские
  # публикации: 2379 (etcd1 — совместимость инструментария), 2381/2382+1 —
  # etcd2/etcd3 (env-override при коллизиях, прецедент METRICS_*). Advertise
  # двумя URL: etcdN:2379 — потребители сети стенда; host.docker.internal:PORT —
  # Patroni-ноды PgWorker из per-cluster сетей (паттерн прежнего as-etcd).
  x-etcd-common: &etcd-common
    image: quay.io/coreos/etcd:v3.5.21
    restart: unless-stopped
    logging: { driver: json-file, options: { max-size: "10m", max-file: "3" } }
    healthcheck:
      test: ["CMD", "etcdctl", "--endpoints=http://localhost:2379", "endpoint", "health"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 20s

  etcd1:
    <<: *etcd-common
    container_name: as-etcd-1
    ports:
      - "${ETCD1_HOST_PORT:-2379}:2379"
    command:
      - etcd
      - --name=etcd1
      - --data-dir=/var/etcd/data
      - --listen-client-urls=http://0.0.0.0:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd1:2380
      - --advertise-client-urls=http://etcd1:2379,http://host.docker.internal:${ETCD1_HOST_PORT:-2379}
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-token=as-etcd-cluster
      - --initial-cluster-state=new
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
    volumes:
      - etcd1-data:/var/etcd/data

  etcd2:
    <<: *etcd-common
    container_name: as-etcd-2
    ports:
      - "${ETCD2_HOST_PORT:-2381}:2379"
    command:
      - etcd
      - --name=etcd2
      - --data-dir=/var/etcd/data
      - --listen-client-urls=http://0.0.0.0:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd2:2380
      - --advertise-client-urls=http://etcd2:2379,http://host.docker.internal:${ETCD2_HOST_PORT:-2381}
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-token=as-etcd-cluster
      - --initial-cluster-state=new
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
    volumes:
      - etcd2-data:/var/etcd/data

  etcd3:
    <<: *etcd-common
    container_name: as-etcd-3
    ports:
      - "${ETCD3_HOST_PORT:-2383}:2379"
    command:
      - etcd
      - --name=etcd3
      - --data-dir=/var/etcd/data
      - --listen-client-urls=http://0.0.0.0:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd3:2380
      - --advertise-client-urls=http://etcd3:2379,http://host.docker.internal:${ETCD3_HOST_PORT:-2383}
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-token=as-etcd-cluster
      - --initial-cluster-state=new
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
    volumes:
      - etcd3-data:/var/etcd/data
```

В секции `volumes:` файла: `etcd-data:` заменить на `etcd1-data:` / `etcd2-data:` / `etcd3-data:`.

ВАЖНО: `initial-cluster-state=new` в compose переживает `docker compose down` без `-v` — узел стартует с существующим data-dir и игнорирует bootstrap-флаги (etcd сам видит членство в data-dir), это подтверждено поведением etcd; при `down -v` кластер пересобирается с нуля. Комментарий об этом добавить в шапку блока.

- [x] **Step 3.2: compose — потребители на списках endpoints:**

  - `adminpanel` (строка 277): `AdminPanel__Etcd__Endpoints__0: http://etcd:2379` → три строки:
    ```yaml
      AdminPanel__Etcd__Endpoints__0: http://etcd1:2379
      AdminPanel__Etcd__Endpoints__1: http://etcd2:2379
      AdminPanel__Etcd__Endpoints__2: http://etcd3:2379
    ```
  - `kafkaworker`, `kafkaworker-2`, `valkeyworker`, `valkeyworker-2` (строки 335, 365, 396, 421): `…__Etcd__Endpoints__0: http://etcd:2379` → по три строки `…__Etcd__Endpoints__0..2: http://etcd1:2379 / http://etcd2:2379 / http://etcd3:2379`; `depends_on: [etcd]` → `depends_on: [etcd1, etcd2, etcd3]`.
  - Patroni-эмуляторы `hc1a/hc1b/hc2a/hc2b` (строки 218, 231, 243, 256): `ETCD_ENDPOINTS: http://etcd:2379` → `ETCD_ENDPOINTS: http://etcd1:2379,http://etcd2:2379,http://etcd3:2379`.
  - Проверить отсутствие остаточных `http://etcd:2379` в файле (см. Step 3.7).

- [x] **Step 3.3: `emulator.py` — перебор endpoints** (spec §5 п.3; единственное место — `etcd_post`, вызовы `etcd_put_leased/lease_grant/lease_keepalive` не меняются). Заменить строку 19 и функцию `etcd_post` (строки 79–84) на:

```python
# HA-контур (t09, arch/04 §8): ETCD_ENDPOINTS — СПИСОК URL через запятую;
# активный кешируется, транспортная ошибка → перебор до первого живого
# (single-URL значения остаются валидными — не-HA прогоны).
ETCD_LIST = [u.strip().rstrip("/") for u in os.getenv("ETCD_ENDPOINTS", "http://etcd1:2379").split(",") if u.strip()]
_active = {"url": ETCD_LIST[0] if ETCD_LIST else ""}


def etcd_post(path, payload):
    # Активный endpoint (кеш) первым; исключение/5xx — следующий из списка;
    # успех на новом адресе обновляет кеш (переключение видно в логе).
    order = [_active["url"]] + [u for u in ETCD_LIST if u != _active["url"]]
    last_error = None
    for url in order:
        try:
            req = urllib.request.Request(
                url + path, data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as r:
                if url != _active["url"]:
                    print(f"{NODE}: etcd endpoint switch {_active['url']} -> {url}", flush=True)
                    _active["url"] = url
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise  # 4xx — не отказ узла, протоколная ошибка
            last_error = e
        except Exception as e:
            last_error = e
    raise last_error
```

- [x] **Step 3.4: `checks/00-up.sh`:**
  - Строку 56 `ect() { docker compose exec -T etcd … }` → `ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }`.
  - Блок «1) etcd жив» (строки 60–64) → ждём КВОРУМ (3 члена started; election-timeout 2 с — сбор за секунды). Комментарий блока 1b уже обновлён в Task 2 (Step 2.5) — здесь не трогаем:

```bash
# 1) etcd-контур: кворум собран (3 узла одним кластером, arch/04 §8)
ect_ok() { [ "$(ect member list 2>/dev/null | grep -c 'started')" = "3" ]; }
for i in $(seq 1 100); do ect_ok && break; sleep 1; done
ect_ok || { echo "  ❌ кворум as-etcd-1/2/3 не собрался за 100 c (docker compose logs etcd1 etcd2 etcd3)"; exit 1; }
echo "  etcd-контур ready (3/3 started)"
```

  - Комментарий блока 1b (строки 86–89): `as-etcd` → `as-etcd-1/2/3`, `PGW_ETCD_ENDPOINT=host.docker.internal:2379` → `PGW_ETCD_ENDPOINT_0..2=host.docker.internal:2379/2381/2383 (дефолты deploy/docker-compose.yml, t09)`.
- [x] **Step 3.5: `checks/66-kafka-worker-churn.sh`:** строки 24, 68, 70: `docker exec as-etcd` / `docker exec -i as-etcd` → `as-etcd-1`.
- [x] **Step 3.6: `README.md` стенда:**
  - Строку 11: «и ВСЕ на одном etcd (as-etcd, источник правды, контур один)» → «и ВСЕ на одном etcd-кластере as-etcd-1/2/3 (источник правды, контур один, 3 узла — зеркало прода, arch/04 §8)».
  - Строку 13: `docker compose up -d   # стенд части: etcd+панель` → `etcd-кластер+панель`.
  - Строку 134 (отладка): `docker compose exec etcd` → `docker compose exec etcd1`.
  - В раздел «Профили»/после MinIO добавить абзац: «**HA-etcd (t09)**: контур всегда 3 узла (`as-etcd-1/2/3`, порты 2379/2381/2383, env `ETCD{1,2,3}_HOST_PORT` при коллизиях); отказоустойчивость проверяет `checks/43-etcd-ha.sh` (stop узла → кворум пишет, master-ключ жив → start → 3/3)».
- [x] **Step 3.7: Проверка quick-профиля:**

```bash
cd dev-stand/adminpanel
docker compose config >/dev/null && echo compose-OK
! grep -n "http://etcd:2379" docker-compose.yml        # → нет остаточных
docker compose up -d                                     # quick: etcd1/2/3 + панель
for i in $(seq 1 60); do docker exec as-etcd-1 etcdctl --endpoints=http://localhost:2379 member list 2>/dev/null | grep -qc started && break; sleep 1; done
docker exec as-etcd-1 etcdctl --endpoints=http://localhost:2379 member list -w table   # → 3 члена, 1 лидер
curl -fsS http://localhost:5050/api/healthz >/dev/null && echo panel-OK
docker compose down                                      # чистка прогона
```

Ожидание: 3 started, панель жива; после — зачистка (`docker compose down`, страховочно `docker network prune -f`).

- [x] **Step 3.8: Проверка полного подъёма (00-up.sh, quick→full+kafka+valkey+metrics):**

```bash
bash checks/90-down.sh -v 2>/dev/null || true            # чистый старт
bash checks/00-up.sh                                      # полная система на 3-узловом контуре
# после: быстрая smoke-проверка панели
curl -fsS http://localhost:5050/api/healthz >/dev/null && echo stand-OK
```
Ожидание: `✓ стенд поднят …` (кворум собрался, эмуляторы/воркеры зарегистрировались — их ключи теперь на кластере). Стенд ОСТАВИТЬ поднятым для задачи 5.

- [x] **Step 3.9: Commit:**

```bash
git add dev-stand/adminpanel/docker-compose.yml dev-stand/adminpanel/sidecar/emulator.py dev-stand/adminpanel/checks/00-up.sh dev-stand/adminpanel/checks/66-kafka-worker-churn.sh dev-stand/adminpanel/README.md
git commit -m "feat(t09): dev-стенд — etcd-контур 3 узла одним кластером (as-etcd-1/2/3, порты 2379/2381/2383); панель/kfw×2/vwk×2/эмуляторы на списках endpoints; эмулятор перебирает ETCD_ENDPOINTS; 00-up ждёт кворум"
```

---

### Task 4: `master-lease.py` — перебор списка `PGW_ETCD` + интеграционный тест (TDD)

**Вход (предусловие):** задачи 1–3 закоммичены; стенд задачи 3 не нужен для этого теста (свои контейнеры).

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/Docker/MasterLeaseFailoverTests.cs`
- Modify: `docker/node/master-lease.py` (константа `ETCD`, функция `etcd_post`)

**Interfaces:**
- Consumes: `SpiloEnvBuilder` уже передаёт `PGW_ETCD="url1,url2"` (ассерт `NodeConfigBuildersTests`); `docker-entrypoint.sh` прокидывает как есть — не трогаем.
- Produces: лог-маркер `master-lease: endpoint switch <old> -> <new>` (проверяется тестом).

**Выход:** lease мастер-ключа переживает отказ первого endpoint из списка; интеграционный тест зелёный; C#-код (`src/**` кроме тестов) не изменён.

**Spec:** §6 (весь), §12.4.

- [x] **Step 4.1: написать падающий тест** `src/tests/PgWorker.IntegrationTests/Docker/MasterLeaseFailoverTests.cs`:

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.IntegrationTests.E2e;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Перебор endpoints lease-скриптом мастер-ключа (t09, spec §6): PGW_ETCD —
// СПИСОК URL; при отказе активного endpoint скрипт продолжает писать/продлевать
// мастер-ключ через следующий живой. Скрипт запускается ПРОЦЕССОМ в контейнере
// python:3.12-alpine (тот же stdlib-набор, что в образе ноды; fork-демон жив,
// пока жив PID1-сессия sleep) с bind-mount файла docker/node/master-lease.py.
// Два ОДИНОЧНЫХ etcd-контейнера (не кластер): lease-скрипту безразличен кворум,
// важен перебор transport-отказов. Гейт PGW_TEST_DOCKER=1 (DockerTrait).
public class MasterLeaseFailoverTests
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private const string PythonImage = "python:3.12-alpine";
    private const int LeaseTtlSec = 5;

    [Fact]
    public async Task MasterLease_FailoverToSecondEndpoint_KeepaliveContinues()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: два живых одиночных etcd (динамические хост-порты) +
        // runner-контейнер со скриптом; скрипт-демон пишет ключ через endpoint №1.
        var etcd1 = new ContainerBuilder(EtcdImage)
            .WithName($"pgw-it-ml1-{tag}")
            .WithCommand("etcd", "--name=ml1", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        var etcd2 = new ContainerBuilder(EtcdImage)
            .WithName($"pgw-it-ml2-{tag}")
            .WithCommand("etcd", "--name=ml2", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-mlrun-{tag}")
            .WithCommand("sleep", "600")
            .WithBindMount(Path.Combine(root, "docker", "node", "master-lease.py"),
                "/tmp/master-lease.py", AccessMode.ReadOnly)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .Build();
        await etcd1.StartAsync(ct);
        await etcd2.StartAsync(ct);
        await runner.StartAsync(ct);
        var p1 = etcd1.GetMappedPublicPort(2379);
        var p2 = etcd2.GetMappedPublicPort(2379);
        var ep1 = $"http://host.docker.internal:{p1}";
        var ep2 = $"http://host.docker.internal:{p2}";
        var key = $"/pgw-it/master-lease-{tag}/master";
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var gateway = new EtcdGateway(http);

        try
        {
            var startScript = $"PGW_ETCD='{ep1},{ep2}' PGW_MASTER_KEY='{key}' "
                + "PGW_NODE_HOST='node1a' PGW_DOORMAN_PORT='6432' "
                + "python3 -u /tmp/master-lease.py master >/tmp/lease.log 2>&1";
            await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c", startScript], ct);

            // до отказа скрипт пишет на АКТИВНОМ endpoint — первом элементе
            // списка (ep1): ждём появления ключа именно там (узлы независимые,
            // не кластер — на ep2 ключа до переключения нет).
            var appeared = await E2eFixture.WaitForAsync(async () =>
                (await gateway.GetAsync(ep1, key, ct)).Value is not null,
                TimeSpan.FromSeconds(15), ct);
            appeared.Should().BeTrue("демон master-lease обязан писать мастер-ключ (логи: docker exec … cat /tmp/lease.log)");

            // Act: останавливаем ПЕРВЫЙ контейнер — активный endpoint умирает.
            await etcd1.StopAsync(ct);

            // Assert: ключ продолжает продлеваться через второй узел — непрерывно
            // жив дольше 2×TTL (умерший lease погасил бы ключ ≤ 5 c), лог сообщает
            // о переключении активного endpoint.
            await Task.Delay(LeaseTtlSec, ct); // гарантия: выживание не «прошлым» put
            var alive = true;
            for (var probe = 0; probe < 5 && alive; probe++)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                var kv = (await gateway.GetAsync(ep2, key, ct)).Value;
                alive = kv is { Value.Length: > 0 };
            }
            alive.Should().BeTrue($"ключ {key} не гаснет на 5 пробах × 2 c (10 c > 2×TTL) через второй endpoint после смерти первого");

            var log = await E2eFixture.RunProcessAsync(
                "docker", ["exec", runner.Name, "cat", "/tmp/lease.log"], ct);
            log.Should().Contain("endpoint switch",
                "лог скрипта обязан сообщать, через какой endpoint он пишет (диагностика)");
        }
        finally
        {
            // Teardown (любой исход): гасим демон по PID-файлу, убираем контейнеры.
            try
            {
                await E2eFixture.RunProcessAsync("docker",
                    ["exec", runner.Name, "sh", "-c", "kill $(cat /tmp/master-lease.pid) 2>/dev/null || true"], ct);
            }
            catch
            {
                // демон не успел стартовать — не ошибка teardown
            }

            await runner.DisposeAsync();
            await etcd1.DisposeAsync();
            await etcd2.DisposeAsync();
        }

        // Ассерт чистоты: своих контейнеров не осталось (guid-имена).
        var left = await E2eFixture.RunProcessAsync("docker",
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-it-ml"], CancellationToken.None);
        left.Should().BeEmpty("teardown теста обязан убрать оба etcd и runner");
    }
}
```

Примечание к коду: переменную `net` (не нужна) — удалить при реализации; `http` объявить до `try`, чтобы `finally` видел; `runner.Name` — testcontainers отдаёт заданное имя (если свойство иначе — использовать `runner.Name ?? "pgw-it-mlrun-" + tag` по фактическому API `IContainer`).

- [x] **Step 4.2: красный прогон.**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~MasterLeaseFailover
```
Ожидание: FAIL — текущий скрипт читает `PGW_ETCD` как один URL (`urlopen("url1,url2/v3/...")` → транспортная ошибка), ключ не появляется (таймаут 15 с).

- [x] **Step 4.3: правка `docker/node/master-lease.py`** — заменить строку 48 (`ETCD = os.getenv(...)`) и функцию `etcd_post` (строки 58–63):

```python
# HA-контур (t09, arch/04 §8): PGW_ETCD — СПИСОК URL через запятую (SpiloEnvBuilder
# передаёт все узлы контура); активный endpoint кешируется, транспортная
# ошибка/5xx → перебор до первого живого. Один URL — частный случай.
ETCD_LIST = [u.strip().rstrip("/") for u in os.getenv("PGW_ETCD", "http://etcd:2379").split(",") if u.strip()]
_active = {"url": ETCD_LIST[0] if ETCD_LIST else ""}


def etcd_post(path, payload):
    # Активный endpoint (кеш) первым; исключение/5xx — сброс и следующий из
    # списка; успех на новом адресе обновляет кеш (переключение — в лог).
    order = [_active["url"]] + [u for u in ETCD_LIST if u != _active["url"]]
    last_error = None
    for url in order:
        try:
            req = urllib.request.Request(
                url + path, data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as r:
                if url != _active["url"]:
                    print(f"master-lease: endpoint switch {_active['url']} -> {url}", flush=True)
                    _active["url"] = url
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise  # 4xx — протоколная ошибка, не отказ узла
            last_error = e
        except Exception as e:
            last_error = e
    raise last_error
```

Семантика TTL сохранена: при всех мёртвых endpoint `etcd_post` бросает, `lease_loop` (строки 99–118) уже сбрасывает `lease_id=None` и продолжает попытки каждую секунду — ключ погаснет по TTL и восстановится при оживании любого узла (P11 не меняется).

- [x] **Step 4.4: зелёный прогон + чистота:**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~MasterLeaseFailover    # → PASS
docker ps -a --format '{{.Names}}' | grep pgw-it-ml  # → пусто
```

- [x] **Step 4.5: Commit:**

```bash
git add docker/node/master-lease.py src/tests/PgWorker.IntegrationTests/Docker/MasterLeaseFailoverTests.cs
git commit -m "feat(t09): master-lease.py перебирает список PGW_ETCD (активный endpoint кеш, отказ → следующий живый; лог переключения) + интеграционный тест на двух etcd (stop первого — ключ продлевается через второй)"
```

---

### Task 5: Стендовый чек `43-etcd-ha.sh`

**Вход (предусловие):** задачи 1–4 закоммичены; стенд поднят задачей 3 (Step 3.8) и healthy.

**Files:**
- Create: `dev-stand/adminpanel/checks/43-etcd-ha.sh` (chmod +x)

**Interfaces:**
- Consumes: контур `as-etcd-1/2/3`; панель `$BASE` (env `ADMINPANEL_URL`); healthz PgWorker по mTLS (паттерн `00-up.sh`: `MTLS="curl -fsS -m 3 --cacert deploy/tls/ca.pem --cert deploy/tls/healthcheck.crt --key deploy/tls/healthcheck.key"`); порты API из `.env` (`PGW_API_HOST_PORT`/`PGW_API_HOST_PORT2`).

**Выход:** чек доказывает: контур цел → stop узла не-событие (запись/healthz/master-ключ живы) → start → 3/3.

**Spec:** §7 (весь), §12.5.

**Зафиксированное отступление от spec §7 п.2:** «put/del тестового ключа через панель/api воркера» заменён на put/get/del etcdctl'ом ВНУТРИ as-etcd-1 — панель не предоставляет API произвольной записи в etcd (`GET /api/etcd/status` — read-only), а у воркера нет эндпоинта записи произвольных ключей. «Кворум пишет» доказывает etcdctl-цикл, живость панельной/api-грани при отказе узла — `/api/healthz`, `/api/clusters/demo` (masterLeaseAlive) и mTLS `/healthz` обоих инстансов PgWorker; совокупно покрывает замысел spec («контур пишет, потребители живы»).

- [x] **Step 5.1: написать чек** (идемпотентен; чистка своего — в trap; stop-узел возвращается в trap при любом исходе):

```bash
#!/usr/bin/env bash
# HA-etcd контур (t09, arch/04 §8): кворум/health/member list; отказ ОДНОГО
# узла — не-событие (запись идёт, healthz воркеров жив, master-ключ демо-кластера
# не гаснет); возврат узла — снова 3/3. Самодостаточен: свой тестовый ключ
# убирает (trap), as-etcd-2 возвращает (trap).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
PGW_API_HOST_PORT2="${PGW_API_HOST_PORT2:-8083}"
MTLS="curl -fsS -m 5 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
# etcdctl выполняется ВНУТРИ as-etcd-1 (default endpoint 127.0.0.1:2379 — сам узел);
# все три узла адресуемся compose-DNS сети стенда (etcdN:2379 — клиентский порт
# контейнера один): ХОСТОВЫЕ публикации localhost:2379/2381/2383 внутри контейнера
# НЕ слушаются, host.docker.internal — зависел бы от Docker Desktop. Живость
# хостовых публикаций отдельно доказывают воркеры (etcd-reachable healthz — они
# ходят именно публикациями host.docker.internal).
ECT() { docker exec as-etcd-1 etcdctl "$@"; }
EP3="http://etcd1:2379,http://etcd2:2379,http://etcd3:2379"
KEY="/tmp/t09-check43/$(date +%s)"
JAR="$(mktemp)"

cleanup() {
  ECT del "$KEY" >/dev/null 2>&1 || true
  docker start as-etcd-2 >/dev/null 2>&1 || true   # узел обязан вернуться при любом исходе
  rm -f "$JAR"
}
trap cleanup EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login панели (стенд поднят? 00-up.sh)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) Контур цел: 3 started, единый leader; панель видит 3 члена с одним лидером
[ "$(ECT member list | grep -c started)" = "3" ] \
  || { echo "❌ member list ≠ 3 started (docker logs as-etcd-1/2/3)"; exit 1; }
ECT endpoint health --endpoints="$EP3" >/dev/null \
  || { echo "❌ endpoint health 3/3 (compose-DNS etcd1/etcd2/etcd3 из as-etcd-1)"; exit 1; }
api /api/etcd/status | jq -e '(.members | length) == 3
  and ([.members[] | select(.isLeader)] | length) == 1' >/dev/null \
  || { echo "❌ /api/etcd/status: 3 члена/единый leader"; exit 1; }
echo "  контур цел: 3 started, health 3/3, единый leader"

# 2) Отказ одного узла — НЕ-событие: кворум пишет, воркеры/панель/master-ключ живы
docker stop as-etcd-2 >/dev/null
sleep 2
ECT put "$KEY" check43 >/dev/null \
  || { echo "❌ кворум не пишет после stop as-etcd-2 (member list / raft-статус as-etcd-1)"; exit 1; }
ECT get "$KEY" --print-value-only | grep -qx check43 \
  || { echo "❌ read-after-write не сходится"; exit 1; }
ECT del "$KEY" >/dev/null
$MTLS "https://localhost:${PGW_API_HOST_PORT}/healthz" >/dev/null \
  && $MTLS "https://localhost:${PGW_API_HOST_PORT2}/healthz" >/dev/null \
  || { echo "❌ healthz PgWorker (etcd-reachable) погас при живом кворуме"; exit 1; }
api /api/healthz >/dev/null || { echo "❌ панель не отвечает при живом кворуме"; exit 1; }
api /api/clusters/demo | jq -e 'all(.shards[]; .masterLeaseAlive == true)' >/dev/null \
  || { echo "❌ master-ключ демо-кластера погас (эмулятор должен писать через уцелевший узел)"; exit 1; }
# ожидаемая деградация: 2 healthy / 1 unreachable — НЕ ошибка (вывод для журнала)
ECT endpoint health --endpoints="$EP3" 2>&1 | grep -q "is healthy" && \
  ECT endpoint health --endpoints="$EP3" 2>&1 | grep -qc "unreachable" \
  || true
echo "  stop as-etcd-2: запись/healthz×2/панель/master-ключ живы (2 healthy, 1 unreachable — ожидаемо)"

# 3) Возврат узла: member list снова 3 started, health 3/3 (retry-вхождение в кворум)
docker start as-etcd-2 >/dev/null
ok3=0
for i in $(seq 1 30); do
  if [ "$(ECT member list 2>/dev/null | grep -c started)" = "3" ] \
     && ECT endpoint health --endpoints="$EP3" >/dev/null 2>&1; then ok3=1; break; fi
  sleep 2
done
[ "$ok3" = 1 ] || { echo "❌ as-etcd-2 не вернулся в кворум за 60 c (docker logs as-etcd-2)"; exit 1; }
ECT del "$KEY" >/dev/null 2>&1 || true
echo "✓ etcd-ha: 3/3 → stop узла (не-событие) → 3/3"
```

(После вставки реального файла — `chmod +x`; строку про «unreachable» упростить до одного `ECT endpoint health --endpoints="$EP3" 2>&1 | tee /dev/stderr | grep -c "unreachable"` без `|| true`, если ревью потребует строгий ассерт «ровно 1 unreachable»; в базовом виде — информационный вывод.)

- [x] **Step 5.2: прогон на живом стенде:**

```bash
bash dev-stand/adminpanel/checks/43-etcd-ha.sh
```
Ожидание: `✓ etcd-ha: 3/3 → stop узла (не-событие) → 3/3`; после чека `docker ps --format '{{.Names}}' | grep as-etcd` → все три `Up`; `docker exec as-etcd-1 etcdctl get /tmp/t09-check43/ --prefix --keys-only` → пусто.

- [x] **Step 5.3: Commit:**

```bash
git add dev-stand/adminpanel/checks/43-etcd-ha.sh
git commit -m "feat(t09): чек стенда 43-etcd-ha — member list/health/leader, stop узла без потери записи и master-ключа, возврат в 3/3 (trap-чистка своего)"
```

---

### Task 6: E2E — опция `haEtcd` окружения + HA-сценарий (kill узла посреди add-shard)

**Вход (предусловие):** задачи 1–5 закоммичены; docker-демон свободен (соседние серии зачищены).

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (опция `haEtcd`, список `EtcdEndpoints`, env воркера, стоп/старт узла, etcdctl-хелпер, healthz-проба)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (`HostInstance.ApiPort`)
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eHaEtcdScenarios.cs`

**Interfaces:**
- Consumes: `E2eFixture.FreePort/WaitForAsync/RunProcessAsync`; `EtcdGateway`; `DockerTrait`; правка задачи 4 уже в образе ноды (`EnsureStaticAsync` соберёт свежий `pgworker-node:e2e`).
- Produces: `E2eEnvironment.EtcdEndpoints` (`IReadOnlyList<string>`, localhost-URL всех узлов), `StopEtcdNodeAsync(int)`/`StartEtcdNodeAsync(int)`/`EtcdctlAsync(params string[])`/`HealthzOkAsync(HostInstance)`; тест-имя фильтра `HaEtcd` (мерж-гейт).

**Выход:** HA-сценарий зелёный на свежем Release; обычные сценарии не дорожают (дефолт `haEtcd=false` — окружение поднимает 1 etcd, как сейчас).

**Spec:** §8 (весь), §12.6; изоляция/телеметрия — `docs/e2e-isolation.md`, `docs/e2e-launch.md`.

- [x] **Step 6.1: `E2eFixture.cs` — порт API у `HostInstance`.** В ctor-список `HostInstance(string name, Process process, string snapshotsDir, HttpClient healthHttp, StreamWriter? logWriter = null)` добавить параметр `int apiPort = 0` и свойство:

```csharp
    public int ApiPort { get; }   // хост-порт /healthz (проба живости из сценариев)
```

- [x] **Step 6.2: `E2eEnvironment.cs` — сигнатуры и состояние.**
  - `StartAsync`/`StartOnceAsync`: добавить параметр `bool haEtcd = false` (после `withMinio`).
  - Поле `private readonly IContainer _etcd;` → `private readonly IReadOnlyList<IContainer> _etcdNodes;` + `private readonly IReadOnlyList<string> _etcdNames;` (имена контейнеров: `pgw-ee1-{runId}`, `pgw-ee2-{runId}`, `pgw-ee3-{runId}`; одиночный — `pgw-ee1-{runId}`, прежнее имя `pgw-ee-{runId}` больше нигде не парсится — OwnName ищет `runId`).
  - Свойство `public string EtcdEndpoint { get; }` оставить, добавить:

```csharp
    /// <summary>Все клиентские URL контура окружения (localhost-публикации);
    /// при haEtcd=false — один (прежняя семантика).</summary>
    public IReadOnlyList<string> EtcdEndpoints { get; }
```

  - Конструктор: принимает `IReadOnlyList<IContainer> etcdNodes, IReadOnlyList<string> etcdNames, IReadOnlyList<string> etcdEndpoints`; `EtcdEndpoint = etcdEndpoints[0]`.
  - В шапке класса doc-комментарий дополнить: «опция haEtcd: контур — 3 узла static-bootstrap кластером (кворум 2/3, arch/04 §8); хост-порты — зонд, advertise host.docker.internal (Patroni-ноды из per-cluster сетей)».

- [x] **Step 6.3: `StartOnceAsync` — подъём контура.** Заменить блок создания etcd (строки 205–220): порты `var etcdPorts = Enumerable.Range(0, haEtcd ? 3 : 1).Select(_ => E2eFixture.FreePort()).ToArray();`; узлы в цикле: alias в сети `e2e-etcd{i+1}` (`WithNetworkAliases`), имена `pgw-ee{i+1}-{runId}`, порт-биндинг `(etcdPorts[i], 2379)`; команда:

```csharp
var initialCluster = haEtcd
    ? string.Join(",", Enumerable.Range(1, 3).Select(n => $"e2e-etcd{n}=http://e2e-etcd{n}:2380"))
    : "e2e-etcd1=http://e2e-etcd1:2380";
// команда узла: static bootstrap по внутренним alias (peer 2380 в сети окружения),
// advertise двумя URL: compose-alias (сеть окружения) + host.docker.internal:PORT
// (Patroni-ноды из per-cluster сетей — прецедент прежнего одиночного etcd).
var command = new List<string> { "etcd", $"--name=e2e-etcd{i + 1}", "--data-dir=/etcd-data",
    "--listen-client-urls=http://0.0.0.0:2379", "--listen-peer-urls=http://0.0.0.0:2380",
    $"--initial-advertise-peer-urls=http://e2e-etcd{i + 1}:2380",
    $"--advertise-client-urls=http://e2e-etcd{i + 1}:2379,http://host.docker.internal:{etcdPorts[i]}",
    $"--initial-cluster={initialCluster}", $"--initial-cluster-token=e2e-{runId}",
    "--initial-cluster-state=new", "--heartbeat-interval=250", "--election-timeout=2000" };
```

  Wait-стратегию `UntilHttpRequestIsSucceeded(/health)` у узлов УБРАТЬ (первый узел без кворума повис бы в `StartAsync`) — вместо неё после старта всех узлов явное ожидание готовности:

```csharp
// Готовность контура (бюджет ≤ 100 c — быстрое падение фикстуры, AGENTS.md):
// одиночный — /health 200; HA — /health 200 на всех трёх + лидер избран
// (POST /v3/maintenance/status → leader ≠ 0; grpc-gateway отдаёт uint64 строкой).
using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
var ready = await E2eFixture.WaitForAsync(async () =>
{
    foreach (var url in endpoints)
    {
        try
        {
            using var health = await probe.GetAsync(url + "/health", ct);
            if (!health.IsSuccessStatusCode) return false;
            using var status = await probe.PostAsync(url + "/v3/maintenance/status",
                new StringContent("{}", Encoding.UTF8, "application/json"), ct);
            if (!status.IsSuccessStatusCode) return false;
            using var doc = JsonDocument.Parse(await status.Content.ReadAsStringAsync(ct));
            if (doc.RootElement.TryGetProperty("leader", out var leader)
                && leader.GetString() == "0") return false;
        }
        catch (Exception)
        {
            return false; // ещё поднимается / кворум не собран
        }
    }
    return true;
}, TimeSpan.FromSeconds(100), ct);
ready.Should().BeTrue($"etcd-контур ({(haEtcd ? 3 : 1)} узла) обязан собраться за 100 c");
```

  (usings: `System.Net`, `System.Text`, `System.Text.Json` добавить при необходимости.)

- [x] **Step 6.4: env воркера — список.** В `StartHostOnPortAsync` заменить строки 336–338 (`["PgWorker__Etcd__Endpoints__0"] = EtcdEndpoint` + `AdvertisedEndpoints__0`) на:

```csharp
            // HA-контур (t09, arch/04 §8): воркер получает СПИСОК всех узлов
            // (failover перебирает); Advertised — те же URL с host.docker.internal
            // (их получают Patroni-ноды и master-lease из per-cluster сетей).
            for (var i = 0; i < EtcdEndpoints.Count; i++)
            {
                env[$"PgWorker__Etcd__Endpoints__{i}"] = EtcdEndpoints[i];
                env[$"PgWorker__Etcd__AdvertisedEndpoints__{i}"] = EtcdEndpoints[i].Replace(
                    "localhost:", "host.docker.internal:", StringComparison.Ordinal);
            }
```

  Конструирование `HostInstance` — передать порт (`new HostInstance(name, process, snapshotsDir, _healthHttp, logWriter, port)`).

- [x] **Step 6.5: helpers HA-окружения** (рядом с `RunDockerAsync`):

```csharp
    /// <summary>Стоп/старт узла контура по индексу (0-based) — сценарии отказа;
    /// docker stop/start по имени: контейнер и data-dir переживают, член возвращается в кворум.</summary>
    public Task StopEtcdNodeAsync(int index, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", ["stop", _etcdNames[index]], ct);

    public Task StartEtcdNodeAsync(int index, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", ["start", _etcdNames[index]], ct);

    /// <summary>etcdctl в узле №1 (сценарии: member list / endpoint health).</summary>
    public Task<string> EtcdctlAsync(params string[] args)
        => E2eFixture.RunProcessAsync("docker",
            ["exec", _etcdNames[0], "etcdctl", "--endpoints=http://localhost:2379", .. args]);

    /// <summary>Проба /healthz инстанса (живость API на интервале отказа узла):
    /// true = маршрут отвечает (не 404), транспорт досягаем.</summary>
    public async Task<bool> HealthzOkAsync(HostInstance host)
    {
        try
        {
            using var response = await _healthHttp.GetAsync(
                $"https://127.0.0.1:{host.ApiPort}/healthz", CancellationToken.None);
            return response.StatusCode != System.Net.HttpStatusCode.NotFound;
        }
        catch (HttpRequestException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
```

  `DisposeAsync`/`FailedTearDownAsync`: `await _etcd.DisposeAsync()` → `foreach (var node in _etcdNodes) await node.DisposeAsync();` (dispose/stop соответственно; телеметрия `CollectDiagnosticsAsync` уже снимает все свои контейнеры по OwnName — три узла попадают автоматически). В catch-ветке частичного подъёма (строки 257–292) — цикл по списку.

- [x] **Step 6.6: сценарий** `src/tests/PgWorker.IntegrationTests/E2e/E2eHaEtcdScenarios.cs`:

```csharp
using System.Text.Json;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// HA-etcd контур E2E (t09, spec §8): отказ ОДНОГО узла 3-нодового контура
// посреди надзора — не-событие. Изоляция/телеметрия — docs/e2e-isolation.md,
// docs/e2e-launch.md (guid-имена, own-only teardown, MarkFailed — стоп без
// удаления). Проверки после отказа узла №1 идут через выживший endpoint
// (EtcdEndpoints[1]) — активный URL узла №1 мёртв по построению.
public class E2eHaEtcdScenarios
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoints[1]; // выживший узел при отказе №1

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task HaEtcd_KillNodeMidAddShard_SupervisionAndMasterLeaseSurvive()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("ha-etcd", haEtcd: true, ct: ct);
        Fx = fx;
        var cluster = $"haetcd{Fx.ClusterTag}";
        try
        {
            // Arrange: 3-узловой контур (кворум собран фикстурой), воркер на
            // списке endpoints, кластер запровиженен, master-ключ жив.
            await SeedClusterAsync(cluster);
            await using var h1 = await Fx.StartHostAsync("ha1", ct: ct);
            var provisioned = await E2eFixture.WaitForAsync(
                () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning обязан дойти до Active на 3-узловом контуре");
            Fx.EtcdEndpoints.Should().HaveCount(3);
            (await Fx.EtcdctlAsync("member", "list")).Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Should().HaveCount(3, "member list = 3 члена до отказа");

            // Act: останавливаем узел №1 (кворум 2/3 живёт) и СРАЗУ добавляем шард —
            // provisioning идёт через уцелевшие endpoints (failover воркера).
            await Fx.StopEtcdNodeAsync(0, ct);
            await SeedAddDeclarationAsync(cluster, "shard3", ct);

            // Assert (интервал отказа): healthz жив, lease-ключ API не гаснет,
            // master-ключ пережил отказ (непрерывно жив > 2×TTL 5 c — пишет
            // через второй endpoint, задача 4).
            for (var probe = 0; probe < 5; probe++)
            {
                (await Fx.HealthzOkAsync(h1)).Should().BeTrue(
                    $"healthz воркера жив на всём интервале отказа (проба {probe + 1}/5)");
                (await GetOrNullAsync("/pgworker/api/")).Should().NotBeNull(
                    "keepalive-ключи API публикуются (надзор не прерывался)");
                (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master"))!.Value
                    .Should().NotBeEmpty($"master-ключ жив (проба {probe + 1}/5)");
                await Task.Delay(2000, ct);
            }

            var added = await E2eFixture.WaitForAsync(
                () => ShardRegisteredAsync(cluster, "shard3"), TimeSpan.FromSeconds(360), ct);
            added.Should().BeTrue($"add-shard доведён через уцелевшие endpoints; work={await WorkDumpAsync(cluster, ct)}");

            // Возврат узла: member list снова 3, health 3/3 (узел догоняет кластер).
            await Fx.StartEtcdNodeAsync(0, ct);
            var back = await E2eFixture.WaitForAsync(async () =>
            {
                var members = (await Fx.EtcdctlAsync("member", "list"))
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries);
                return members.Length == 3 && members.All(m => m.Contains("started"));
            }, TimeSpan.FromSeconds(60), ct);
            back.Should().BeTrue("узел №1 обязан вернуться в кворум (data-dir пережил stop)");
        }
        catch (Exception)
        {
            // Телеметрия (docs/e2e-launch.md): упавший сценарий помечается —
            // teardown ОСТАНАВЛИВАЕТ контейнеры, не удаляя (разбор по логам).
            Fx.MarkFailed();
            throw;
        }
    }

    // ===== Хелперы (копии приёмов E2eScaleScenarios — приватные там) =====

    private async Task<Shared.Etcd.Client.Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
                if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                    return false;
        }

        return (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Count == 0;
    }

    private async Task<bool> ShardRegisteredAsync(string cluster, string shard)
    {
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
            return false;
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return true;
    }

    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}""",
            null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 6; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task SeedAddDeclarationAsync(string cluster, string shard, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }
}
```

  Уточнения по месту: `using FluentAssertions;` добавить; `/pgworker/api/` — префикс-ключ без get — использовать `RangeAsync("/pgworker/api/")` (get по префиксу невалиден): заменить пробу keepalive на `(await RangeAsync("/pgworker/api/")).Should().NotBeEmpty("keepalive-ключ API публикуется (надзор жив)")`.

- [x] **Step 6.7: сборка + прогон HA-сценария (свежий Release — фикстура собирает сама):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~HaEtcd
# зачистка серии (после финальной строки прогона):
docker ps -a --format '{{.Names}}' | grep -E 'pgw-' | awk '{print $1}' | xargs -r docker rm -f
docker network prune -f
```
Ожидание: PASS. Чистота прогона наследуется teardown-ассертом `E2eEnvironment.DisposeAsync` (own-only по идентификатору прогона: шаги 5–7 teardown включают ассерты «не осталось ни контейнера/тома/сети своего окружения» — красный teardown роняет тест); ручной гейт выше — страховка правил AGENTS.md против осиротевших сетей движка (`pgw-net-*` ryuk не подбирает).

- [x] **Step 6.8: регресс обычных сценариев (окружение с 1 etcd не сломано):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard
# зачистка серии — как в Step 6.7 (чистота — teardown-ассерт окружения; ручной гейт — страховка)
```
Ожидание: PASS.

- [x] **Step 6.9: Commit:**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs src/tests/PgWorker.IntegrationTests/E2e/E2eHaEtcdScenarios.cs
git commit -m "feat(t09): E2eEnvironment опция haEtcd (3 узла static bootstrap, кворум-бюджет 100 c, динамические порты) + HA-сценарий: kill узла №1 посреди add-shard — надзор/healthz/master-ключ живы, возврат в 3/3"
```

---

### Task 7: Runbook — раздел «HA-etcd контур»

**Вход (предусловие):** задачи 1–6 закоммичены.

**Files:**
- Modify: `docs/runbook.md` (новый раздел после блока «Внешние docker-образы…» (после подраздела «Каталог зеркалируемых образов», ~строка 150); строка таблицы образов ~строка 147)

**Выход:** операторский рецепт HA-контура в ранбуке.

**Spec:** §9, §12.8.

- [x] **Step 7.1: новый раздел** (текст):

```markdown
## HA-etcd контур контроль-плейна (t09)

Канон — `arch/04-deploy-etcd.md` §8 (два применения одного рецепта; параметры,
advertised-правила потребителей, кворум-семантика, чек-лист). Кратко:

- **Подъём (прод)**: скопировать `deploy/etcd/{docker-compose.yml,etcd.env}` на
  3 РАЗНЫХ docker-хоста; в `etcd.env` каждого — свой `NODE_NAME/NODE_IP`
  (`PEERS`/`CLUSTER_TOKEN` одинаковые; образ — `192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`).
  Первый старт: `INITIAL_CLUSTER_STATE=new` на всех трёх, `docker compose up -d`
  в пределах election-timeout друг от друга; после сбора кворума перевести в
  `existing` на всех узлах (arch/04 §5). Воркеры/панель получают список
  `*_ETCD_ENDPOINT_0..2` / `AdminPanel__Etcd__Endpoints__0..2`.
- **Стендовое зеркало**: дев-стенд всегда поднимает `as-etcd-1/2/3` одним
  кластером (публикации 2379/2381/2383, env `ETCD{1,2,3}_HOST_PORT`).
- **Чеки**: стендовый `dev-stand/adminpanel/checks/43-etcd-ha.sh` (member list
  3 started → stop узла: запись/healthz/master-ключ живы → start → 3/3);
  прод-чек-лист — arch/04 §8 п.5 (`endpoint health --cluster`, панель
  `/api/etcd/status` = 3 члена/единый leader).
- **Потеря узла/кворума**: 1 узел — не-событие (кворум 2/3); 2 узла —
  контроль-плейн заморожен, датаплейн живёт сам, восстановление — arch/09 §4;
  замена узла навсегда — `member remove` + `member add` + data-dir заново
  (arch/04 §8 п.4).
```

- [x] **Step 7.2: строка таблицы образов** (~строка 147):

```markdown
| `quay.io/coreos/etcd:v3.5.21` | etcd-контур: стенд ×3 (HA), deploy/etcd ×3 (HA, по узлу на хост), E2E/интеграция-фикстуры (×5) |
```

- [x] **Step 7.3: Проверка:**

```bash
grep -n "^## HA-etcd контур" docs/runbook.md          # → раздел на месте
grep -n "quay.io/coreos/etcd:v3.5.21" docs/runbook.md  # строка таблицы образов содержит "×3" и "HA"
grep -c "deploy/etcd" docs/runbook.md                  # ≥ 3 (подъём/чеки/зеркало ссылаются на рецепт)
```

- [x] **Step 7.4: Commit:**

```bash
git add docs/runbook.md
git commit -m "docs(t09): runbook — раздел HA-etcd контура (подъём deploy/etcd, стендовое зеркало, чек 43, потеря узла/кворума); строка образа etcd ×3 HA"
```

---

### Task 8: Мерж-гейт — полные прогоны + roadmap-чистка тем же мерж-коммитом

**Вход (предусловие):** задачи 1–7 закоммичены; docker-демон свободен от чужих серий; стенд задачи 5 разобран (`bash dev-stand/adminpanel/checks/90-down.sh -v`), страховочно `docker network prune -f`.

**Files:**
- Modify (в мерж-коммит): `arch/roadmap/reliability.md` (удалить пункт `t09-ha-etcd-canon`, ~строки 41–44)
- Modify (в мерж-коммит): `arch/roadmap/reliability-report.md` (строка «Осталось» ~106; сводка R ~60–61; таблица «Деплой-уровень» ~92; таблица «Сделано»)

**Выход:** задача принята; roadmap и отчёт синхронны с мержем (правило мерж-гейта трека reliability).

**Spec:** §9 (мерж-гейт трека), §12.7; AGENTS.md (мерж-гейт docker-E2E).

- [x] **Step 8.1: юниты + интеграция без docker** (быстрая серия):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release
```
Ожидание: зелёные (docker-тесты скипнуты без `PGW_TEST_DOCKER`); зачистка не нужна.

- [x] **Step 8.2: docker-E2E мерж-гейта** (свежий Release — фикстура собирает сама; после СЕРИИ — зачистка):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~HaEtcd|FullyQualifiedName~MasterLeaseFailover"
# дождаться финальной строки, затем:
docker ps -a --format '{{.Names}}' | grep 'pgw-' | awk '{print $1}' | xargs -r docker rm -f
docker network prune -f
```
Ожидание: все три зелёные (кейс-маркер AGENTS.md + HA-сценарий spec §8 + интеграционный тест §6).

- [x] **Step 8.3: стендовая серия (финальная приёмка чеков):**

```bash
bash dev-stand/adminpanel/checks/00-up.sh \
  && bash dev-stand/adminpanel/checks/43-etcd-ha.sh \
  && bash dev-stand/adminpanel/checks/40-live-probes.sh
bash dev-stand/adminpanel/checks/90-down.sh -v
docker network prune -f
```
Ожидание: все чеки зелёные (43 — новый; 40 — не регрессировал на 3-узловом контуре).

- [x] **Step 8.4: roadmap-чистка (в тот же мерж-коммит, что и код; до пуша):**
  - `arch/roadmap/reliability.md`: удалить пункт `- **t09-ha-etcd-canon** — …` (строки 41–44); проверить `grep -n "t09" arch/roadmap/*.md` — в `←`-зависимостях других пунктов тег не фигурирует (проверить и убедиться).
  - `arch/roadmap/reliability-report.md`:
    - «Осталось»: удалить строку `| t09-ha-etcd-canon | канон и чеки 3-нодового etcd прода | P2 | R |`.
    - «Сделано в рамках трека» — добавить строку: `| t09-ha-etcd-canon | — (мерж-коммит t09) | деплой/стенд: etcd-контур 3 узла одним кластером (deploy/etcd + as-etcd-1/2/3, Endpoints__0..2 всем потребителям, master-lease/эмулятор перебирают endpoints) — SPOF контроль-плейна снят; чек 43 (stop узла — запись/healthz/master-ключ живы) + docker-E2E HaEtcd (kill узла посреди add-shard) доказывают отказоустойчивость |`.
    - Сводка «R»: из «Открытые разрывы» убрать «etcd одиночный — SPOF контроль-плейна (`t09`…)».
    - Таблица «Деплой-уровень»: «etcd один endpoint (`t09`)» → «etcd 3-узловой HA-контур (`t09`)».
- [ ] **Step 8.5: мерж-коммит** (по явному приказу пользователя «мерж»: diff на ревью → мерж → roadmap-правки 8.4 в ТОМ ЖЕ коммите → push → автоочистка worktree по базовым правилам). **ПРЕДУСЛОВИЕ (код-ревью Фазы 7):** выполняется только после Tasks 9–10 ниже — правки по findings ревью (инструкция `deploy/etcd` + AGENTS.md) и полная docker-E2E серия на изменённой общей фикстуре `E2eEnvironment`.

---

### Task 9 (код-ревью Фазы 7): инструкция запуска `deploy/etcd` через `--env-file` + актуализация AGENTS.md

**Вход (предусловие):** Tasks 1–8.4 исполнены (8 коммитов ветки); findings 1–2 код-ревью Фазы 7.

**Files:**
- Modify: `deploy/etcd/docker-compose.yml` (шапка-инструкция, строки 1–7; сам сервис НЕ меняется)
- Modify: `docs/runbook.md` (раздел «HA-etcd контур контроль-плейна», блок «Подъём (прод)», ~строка 161)
- Modify: `arch/04-deploy-etcd.md` (§8, первый абзац рядом со ссылкой на рецепт, ~строка 196)
- Modify: `AGENTS.md` (~строка 35 — правило «etcd-кластер ВСЕГДА ТОЛЬКО ОДИН»)

**Interfaces:**
- Consumes: фактический сервис `deploy/etcd/docker-compose.yml` (Task 2: `image: ${ETCD_IMAGE}`, `volumes: ${DATA_DIR}:/data`, `command: --name=${NODE_NAME} …` — интерполяция YAML-переменных, `env_file: etcd.env` — переменные внутрь контейнера).
- Produces: единая команда запуска `docker compose --env-file etcd.env up -d` во всех трёх местах (compose-шапка, runbook, arch/04 §8).

**Выход:** рецепт узла поднимается по собственной инструкции (интерполяция `${…}` в YAML — из `--env-file`, без экспорта переменных в shell); живое правило AGENTS.md описывает фактический контур стенда.

**Spec:** §4 п.1 (рецепт узла «копируется на каждый из 3 хостов»), §12.2; отступлений от spec нет — правка доставки рецепта.

- [x] **Step 9.1: `deploy/etcd/docker-compose.yml` — шапка-инструкция.** Строку `#   docker compose up -d        # первый старт — все 3 узла …` заменить на:

```yaml
#   docker compose --env-file etcd.env up -d   # первый старт — все 3 узла в пределах election-timeout
```

и сразу после блока инструкции добавить пояснение (2 строки комментария):

```yaml
# ВАЖНО: env_file передаёт переменные ТОЛЬКО внутрь контейнера; интерполяция
# ${…} в этом YAML (image/volumes/command) идёт из --env-file — поэтому запуск
# всегда с --env-file etcd.env (без него up упадёт на пустом image).
```

Секцию `services:` НЕ трогать.

- [x] **Step 9.2: `docs/runbook.md` — «Подъём (прод)»** (строка ~161): «Первый старт: `INITIAL_CLUSTER_STATE=new` на всех трёх, `docker compose up -d` в пределах election-timeout…» → «…`docker compose --env-file etcd.env up -d` в пределах election-timeout…» (переменные узла интерполируются в YAML из env-файла).

- [x] **Step 9.3: `arch/04-deploy-etcd.md` §8** (~строка 196, абзац «Рецепт узла: `deploy/etcd/{docker-compose.yml,etcd.env.example}` (зона оператора)»): дополнить «; запуск — `docker compose --env-file etcd.env up -d` (интерполяция переменных узла в YAML — из env-файла, не из shell)».

- [x] **Step 9.4: `AGENTS.md` ~строка 35** — правило «etcd-кластер ВСЕГДА ТОЛЬКО ОДИН»: «(`as-etcd` стенда, публикация+advertise `host.docker.internal:2379`)» → «(`as-etcd-1/2/3` стенда — 3-узловой кластер одним контуром, публикации 2379/2381/2383)». Правка текста живого правила под фактический стенд (код не затрагивает, НЕ-цели §1.3 не нарушает).

- [x] **Step 9.5: Проверка:**

```bash
# 1) Команда ИЗ ИНСТРУКЦИИ валидна БЕЗ экспорта переменных в shell.
#    Сначала подкладываем etcd.env (паттерн Task 2 Step 2.6): сервис объявляет
#    env_file: etcd.env, резолвится от директории compose-файла — без файла
#    config падает «env file … not found» (сообщение НЕ содержит «variable
#    is not set» — счётчик без этого был бы вакуумным гейтом).
#    Доказательство closing finding 1: код выхода config = 0 (OK-config)
#    И ноль warnings о неустановленных переменных.
#    (Третья строка в блоке ревьюера содержала опечатку — env-файл передан
#    флагом -f как compose-файл; здесь исправлено на --env-file, замысел
#    команды сохранён: счётчик warnings от той же config-команды.)
cp deploy/etcd/etcd.env.example deploy/etcd/etcd.env
docker compose -f deploy/etcd/docker-compose.yml --env-file deploy/etcd/etcd.env config >/dev/null && echo OK-config
docker compose -f deploy/etcd/docker-compose.yml --env-file deploy/etcd/etcd.env config 2>&1 | grep -ci "variable is not set"   # → 0
rm deploy/etcd/etcd.env
# 2) Инструкция с --env-file — во всех трёх местах:
grep -rn -- "--env-file etcd.env up" deploy/etcd/docker-compose.yml docs/runbook.md arch/04-deploy-etcd.md
# 3) AGENTS.md описывает фактический контур; старой формулировки нет:
grep -n "as-etcd-1/2/3" AGENTS.md
! grep -n '`as-etcd` стенда' AGENTS.md    # → пусто
```

Ожидание: п.1 — `OK-config` напечатан И счётчик 0 (только тогда finding 1 код-ревью Фазы 7 считается доказанно закрытым); п.2 — строка найдена в трёх файлах; п.3 — новая строка есть, старой нет.

- [x] **Step 9.6: Commit:**

```bash
git add deploy/etcd/docker-compose.yml docs/runbook.md arch/04-deploy-etcd.md AGENTS.md
git commit -m "fix(t09): рецепт deploy/etcd запускается через --env-file etcd.env (env_file не интерполирует YAML-переменные — up без него падал на пустом image; инструкция в шапке compose/runbook/arch/04 §8) + AGENTS.md: контур стенда as-etcd-1/2/3 (код-ревью Фазы 7)"
```

---

### Task 10 (код-ревью Фазы 7): полная docker-E2E серия на изменённой общей фикстуре — финальный гейт перед Step 8.5

**Вход (предусловие):** Task 9 закоммичен; стенд разобран (`bash dev-stand/adminpanel/checks/90-down.sh -v`), сети зачищены (`docker network prune -f`), docker-демон свободен от чужих серий.

**Files:** изменений файлов нет — прогон (обоснование: Task 6 изменил ОБЩИЙ путь старта etcd-окружения `E2eEnvironment` для ВСЕХ E2E-сценариев, включая `haEtcd=false`: снята testcontainers wait-стратегия одиночного etcd, готовность — `WaitForAsync` /health + leader≠0; мерж-гейт Task 8 прогнал только `Scale_AddEmptyShard|HaEtcd|MasterLeaseFailover`, остальные ~15 docker-E2E сценариев (E2eBackup/Restore/Move/SecondInstance/Supervisor/Strict/Rotate/Retention/Pgtune и др.) на изменённой фикстуре не гонялись; канон AGENTS.md «полный прогон E2eFixture — при изменении provisioning/portalloc/moves-процессов» — изменение общего пути фикстуры именно такой случай).

**Interfaces:**
- Consumes: `src/tests/PgWorker.IntegrationTests` целиком (E2e-сценарии + Docker/Etcd-серии, вкл. `HaEtcd` и `MasterLeaseFailover`); автосборка Release фикстурой (`EnsureAppDllAsync`, БЕЗ `PGW_TEST_E2E_NOBUILD`).

**Выход:** полная docker-E2E серия зелёная на свежем Release — доказано, что изменение общего пути `E2eEnvironment` не регрессировало остальные сценарии; готовность к Step 8.5 (мерж по приказу).

**Spec:** §8 (E2E-окружение), §12.6–12.7; `docs/e2e-isolation.md`/`docs/e2e-launch.md` (телеметрия/чистота); AGENTS.md (полный прогон E2eFixture).

- [x] **Step 10.1: предусловия-зачистка** (серия тяжёлая и живёт на общем хосте — чужих контуров быть не должно):

```bash
bash dev-stand/adminpanel/checks/90-down.sh -v 2>/dev/null || true
docker ps -a --format '{{.Names}}' | grep 'pgw-' | awk '{print $1}' | xargs -r docker rm -f
docker network prune -f
```

- [x] **Step 10.2: полная docker-E2E серия** (одна команда, без фильтров, без NOBUILD — фикстура сама собирает свежий Release, урок t09):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release
```

Ожидание: все тесты зелёные (весь `PgWorker.IntegrationTests`: E2e-сценарии на изменённой фикстуре — haEtcd=false-путь, Docker-серии вкл. `MasterLeaseFailover`, Etcd-контракты).

**Правила при красной серии (docs/e2e-launch.md):** упавший сценарий помечен `MarkFailed()` — его контейнеры ОСТАНОВЛЕНЫ, не удалены; логи сняты в `/tmp/pgw-e2e-artifacts-<guid>/`. Разбор по логам БЕЗ перезапуска тестов; повторный прогон — только после полного анализа причин и согласия пользователя. Красная серия = СТОП (Step 8.5 недостижим до фикс-коммита и повторной полной серии).

- [x] **Step 10.3: зачистка после финальной строки серии** (дождаться завершения команды Step 10.2):

```bash
docker ps -a --format '{{.Names}}' | grep 'pgw-' | awk '{print $1}' | xargs -r docker rm -f
docker network prune -f
```

- [x] **Step 10.4: готовность к мержу.** Только при зелёной Step 10.2: все предусловия Step 8.5 выполнены (Tasks 1–10); мерж — по явному приказу пользователя («мерж» → Step 8.5).

---

## Self-review (выполнено при написании плана)

- **Покрытие spec:** §3 → Task 1; §4 → Task 2; §5 → Task 3; §6 → Task 4; §7 → Task 5; §8 → Task 6; §9 → Tasks 7–8; §10 фазы 1–7 = Tasks 1–7 (фаза 7 «docs/roadmap» = Task 7 + roadmap-часть Task 8 в мерж-гейте, как требует правило трека); §12.1–12.8 закрыты соответствующими задачами.
- **НЕ-цели соблюдены:** правок `src/PgWorker.*`/`KafkaWorker.*`/`ValkeyWorker.*`/`Shared.*`/`AdminPanel.*` нет (только `src/tests/**`); t08/tls/5-узлы/`dev-stand/compose.yaml`/E2eFixture-дефолт — не тронуты.
- **Именование согласовано:** env `*_ETCD_ENDPOINT_0..2` (Task 2 = Task 3 = arch Task 1); контейнеры `as-etcd-1/2/3` (Tasks 3/5); лог-маркер `endpoint switch` (Task 4 тест ↔ скрипт; Task 3 эмулятор — тот же паттерн с префиксом NODE); API `E2eEnvironment.{EtcdEndpoints,StopEtcdNodeAsync,StartEtcdNodeAsync,EtcdctlAsync,HealthzOkAsync}`/`HostInstance.ApiPort` (Task 6 внутри себя).

## Правки по ревью Фазы 4 (spec-plan-reviewer)

1. Task 4 Arrange: ключ до отказа ждём на `ep1` (активный endpoint — первый элемент списка); после `StopAsync` пробы на `ep2` — как и было.
2. Task 5 `EP3`: etcdctl изнутри `as-etcd-1` адресует узлы compose-DNS (`http://etcd1:2379,…`) — хостовые `localhost:2381/2383` внутри контейнера не слушаются.
3. Правка комментария `00-up.sh` (env-имена) перенесена в Task 2 (Step 2.5) — проверка grep Task 2 больше не ложно-красная; Task 3 правит только `ect()`/wait кворума.
4. Task 2 Step 2.6: для config-валидации шаблона узла `env_file` подкладывается копией примера (`cp … && docker compose config && rm`).
5. Task 4 Assert: `Task.Delay(TimeSpan.FromSeconds(2), ct)`; текст ассерта синхронизирован с фактическим окном (5 проб × 2 с = 10 c > 2×TTL).
6. Task 5: зафиксировано отступление от spec §7 п.2 — запись тестового ключа etcdctl'ом (у панели нет API произвольной записи), живость панели/api — healthz/`/api/clusters/demo`.
7. Task 7: добавлен шаг проверки (Step 7.3, grep раздела/строки образа).
8. Task 6 Steps 6.7/6.8: формулировка уточнена — чистота наследуется teardown-ассертом `E2eEnvironment.DisposeAsync`, ручной гейт — страховка AGENTS.md против осиротевших сетей движка.

## Правки по код-ревью Фазы 7 (добавлены после исполнения Tasks 1–8.4)

1. [impl/major → **Task 9**]: рецепт `deploy/etcd` не поднимался по собственной инструкции — `env_file` передаёт переменные ТОЛЬКО в контейнер и не интерполирует `${…}` в YAML; команда запуска во всех трёх местах (шапка compose, runbook «Подъём (прод)», arch/04 §8) — `docker compose --env-file etcd.env up -d` (минимальный вариант ревью; реорганизация на `environment:` не выбрана). Контроль (Step 9.5 п.1, уточнён кругом 3 ревью Фазы 4): cp example→etcd.env (иначе config падает «env file … not found» — сообщение не содержит «variable is not set», счётчик был вакуумным) → config с проверкой кода выхода (`OK-config`) → счётчик warnings «variable is not set» = 0 → rm копии; закрытие finding 1 доказано только парой «OK-config + 0».
2. [impl/minor → **Task 9**]: AGENTS.md ~строка 35 — живое правило описывало несуществующий контейнер `as-etcd`; обновлено на фактический контур `as-etcd-1/2/3` (3-узловой кластер, публикации 2379/2381/2383).
3. [plan/minor → **Task 10**]: Task 6 изменил ОБЩИЙ путь старта etcd-окружения для всех E2E-сценариев (снята wait-стратегия одиночного etcd), а мерж-гейт прогонял только 3 фильтрованных теста — добавлена обязательная ПОЛНАЯ docker-E2E серия `src/tests/PgWorker.IntegrationTests` (Step 10.2, PGW_TEST_DOCKER=1, Release, без NOBUILD) с зачисткой и правилами телеметрии; Step 8.5 получил явное предусловие «после Tasks 9–10».
