# t07-worker-second-instance — план реализации (второй инстанс воркеров)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Деплой-канон и dev-стенд поднимают по два инстанса каждого воркера (PgWorker, KafkaWorker, ValkeyWorker): смерть одного контейнера не прерывает надзор — клэймы мигрируют второму ≤15 с существующим механизмом, без изменений кода воркеров.

**Architecture:** Правится только деплой-слой: `deploy/docker-compose.yml` (YAML-анкоры + 3 вторых сервиса, уникальные хост-порты/AdvertiseUrl, общие тома), стендовые compose/Prometheus/чеки, новый стендовый чек kill→takeover и docker-E2E сценарий (инстансы — контейнеры). Координация (InstanceId-GUID, lease TTL 15 c, PortAllocLock, failover панели) уже готова — не трогаем. arch-first: сначала §2-дополнения arch/14/16/21.

**Tech Stack:** docker compose (YAML anchors/merge keys), bash-чеки стенда, xUnit + testcontainers (существующая E2eFixture/E2eEnvironment), etcd-ключи `/pgworker/{api,instances,claims}/…`.

**Spec:** [`spec.md`](spec.md) — план реализует его §4 (структура) и §7 (AC1–AC7); фазы §5: arch → deploy → стенд → чек+E2E (независимы) → доки+мерж-гейт.

**WORKTREE:** вся работа в `/Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance` (ветка `t07-worker-second-instance`). Коммитить свободно (feature-ветка); мерж/пуш — только по явной просьбе пользователя.

## Global Constraints (из spec §3/§6 — действуют для каждой задачи)

- Код `src/PgWorker.*`, `src/KafkaWorker.*`, `src/ValkeyWorker.*`, `src/Shared.*`, `src/AdminPanel.*` НЕ меняется. Единственные новые код-артефакты — тест docker-E2E (`src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs`). Если выяснится, что нужен код воркера/панели — СТОП и пересогласование с пользователем.
- Ровно два инстанса на воркера, всегда (никаких профилей `ha`/`redundancy`, никакой параметризации N).
- Каждый второй инстанс: уникальный хост-порт публикации + уникальный `Api:AdvertiseUrl`. Одинаковые URL двух инстансов запрещены (оба lease-ключа `/api/*` мертвы при смерти владельца порта).
- Общие per-install ресурсы на оба инстанса: docker.sock, TLS-тома (`:ro`), общий том снапшотов (дублировать тома НЕЛЬЗЯ — миграция лидера снапшотов). Никаких новых секретов: env-набор наследуется.
- Новый ряд хост-портов: 8083 (`PGW_API_HOST_PORT2`) / 8084 (`KFW_API_HOST_PORT2`) / 8085 (`VWK_API_HOST_PORT2`); не пересекаются с portalloc (15xxx/17xxx/18xxx) и портами стенда.
- Стендовые `kafkaworker-2`/`valkeyworker-2` — БЕЗ хост-публикации портов (compose-DNS advertise).
- `container_name` вторым сервисам deploy-композа НЕ задаём (compose даёт `deploy-pgworker-2-1` и т.п.).
- В тестах только динамические порты (зонд свободного порта), никаких литералов `:16000`; start-бюджеты инстансов ≤100 c; E2E полностью чистит за собой (canon `docs/e2e-isolation.md`, `docs/e2e-launch.md`).
- Локально собираемые образы (`*:dev`, `pgworker:e2e-*`) в registry `192.168.0.1:5000` НЕ кладём.
- Доки и комментарии — по-русски; идентификаторы — по-английски.
- После КАЖДОЙ docker-серии (стенд/тесты) — зачистка контейнеров/сетей/томов серии; следующая серия — только после финальной строки предыдущей и зачистки.

---

### Task 1: arch-first — рецепт поставки двух инстансов в arch/14 §2, arch/16 §2.1, arch/21 §2

**Files:**
- Modify: `arch/14-pgworker.md` (§2, абзац «Сам PgWorker — контейнер…» у строки 477–479)
- Modify: `arch/16-kafkaworker.md` (§2.1, после пункта «Сам воркер — контейнер с `docker.sock`…», строки 202–207)
- Modify: `arch/21-valkeyworker.md` (§2, пункт «Сам воркер — контейнер с `docker.sock`…», строки 175–176)

**Interfaces:**
- Consumes: готовые механизмы (spec §2): `InstanceId = Guid` (`src/Shared.Etcd/Coordination/ClaimStore.cs`), lease TTL 15 с, ключи `<prefix>/api/<id>`, `<prefix>/instances/<id>`, `<prefix>/claims/<C>`.
- Produces: канон поставки «2 инстанса по умолчанию» (порты 8083/8084/8085, уникальные AdvertiseUrl, общие тома), на который ссылаются задачи 2–7.

- [ ] **Step 1.1: Дополнить arch/14-pgworker.md §2**

После строк (файл, ~строки 477–479):

```
Сам PgWorker — контейнер с примонтированным `/var/run/docker.sock` (plain на
одном хосте / swarm manager), volume под снапшоты etcd, env-секреты (§8).
Масштабирование — N реплик: координацию разбирает etcd (§3).
```

добавить абзац:

```
Деплой-рецепт поставки ≥2 инстансов (t07): `deploy/docker-compose.yml` поднимает
**два** инстанса по умолчанию — SPOF надзора устраняется дефолтом, а не опцией.
Каждый инстанс — отдельный сервис того же образа с уникальным хост-портом
публикации API (`PGW_API_HOST_PORT` / `PGW_API_HOST_PORT2`, ряд 8080/8083) и
уникальным `Api:AdvertiseUrl` (панель резолвит API по lease-ключу
`/pgworker/api/<InstanceId>`: одинаковые URL двух инстансов гасили бы оба ключа
при смерти контейнера-владельца порта). Уникальность инстанса — случайный
`InstanceId` при старте (§3), env-идентификация (`PGW_INSTANCE_ID` и т.п.) не
нужна. Общие per-install ресурсы на оба инстанса: docker.sock, TLS-том
`pgw-api-tls:ro` и **общий том снапшотов** `pgw-snapshots` (глобальное
лидерство снапшотов мигрирует между инстансами — свой том у каждого терял бы
снапшоты при миграции лидера); env-секреты (§8) наследуются идентично.
Смерть одного контейнера: клэймы/дискавери мигрируют второму ≤ TTL 15 с (§3),
надзор (provisioning/rebuild/ротации/бэкапы) не прерывается.
```

- [ ] **Step 1.2: Дополнить arch/16-kafkaworker.md §2.1**

В §2.1 после пункта, начинающегося «- Сам воркер — контейнер с `docker.sock` (или swarm-manager), volume снапшотов (`kfw-snapshots`), volume TLS-секретов API (`kfw-api-tls`), поставляется через `deploy/docker-compose.yml`…» (заканчивается «…живут в etcd, генерирует воркер (§4).»), добавить пункт:

```
- **Два инстанса по умолчанию** (t07): деплой-рецепт поднимает второй сервис
  `kafkaworker-2` того же образа — уникальный хост-порт публикации
  (`KFW_API_HOST_PORT2`, ряд 8081/8084) и уникальный `Api:AdvertiseUrl`
  (lease-ключ `/kafkaworker/api/<InstanceId>`), общие на оба инстанса тома
  `kfw-snapshots` и `kfw-api-tls:ro` (лидерство снапшотов мигрирует между
  инстансами), уникальность — случайный `InstanceId` (§4) без env-идентификации.
  Dev-стенд — симметрично: `as-kafkaworker-2` (advertise по compose-DNS
  `https://kafkaworker-2:8080`, без хост-публикации порта).
```

- [ ] **Step 1.3: Дополнить arch/21-valkeyworker.md §2**

После пункта «- Сам воркер — контейнер с `docker.sock`, поставляется через `deploy/docker-compose.yml` (сборка — t02, по образцу KafkaWorker).» добавить пункт:

```
- **Два инстанса по умолчанию** (t07): как у KafkaWorker — второй сервис
  `valkeyworker-2` того же образа (`VWK_API_HOST_PORT2`, ряд 8082/8085,
  уникальный `Api:AdvertiseUrl`), общие на оба инстанса тома `vw-snapshots` и
  `vw-api-tls:ro`, уникальность — случайный `InstanceId` без env-идентификации.
  Dev-стенд — `as-valkeyworker-2` (advertise `https://valkeyworker-2:8080` по
  compose-DNS, без хост-публикации порта).
```

- [ ] **Step 1.4: Проверить (рендер markdown, ссылки на разделы корректны)**

Run: у абзацев нет битых ссылок: §3 (arch/14), §4 (arch/16) существуют — `grep -n "^## 3\.\|^## 4\." arch/14-pgworker.md arch/16-kafkaworker.md`.
Ожидание: обе команды находят заголовки — ссылки в новых абзацах валидны; `git diff --stat` — 3 файла arch.

- [ ] **Step 1.5: Commit**

```bash
git add arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md
git commit -m "t07: arch/14 §2, arch/16 §2.1, arch/21 §2 — рецепт поставки двух инстансов воркеров (порты/advertise/общие тома, InstanceId без env)"
```

---

### Task 2: deploy/docker-compose.yml — анкоры + вторые сервисы; deploy/.env.example

**Files:**
- Modify: `deploy/docker-compose.yml` (полная переработка секции services: x-анкоры + `pgworker-2`, `kafkaworker-2`, `valkeyworker-2`)
- Modify: `deploy/.env.example` (секция «вторые инстансы»)

**Interfaces:**
- Consumes: Task 1 (канон портов 8083/8084/8085, общие тома).
- Produces: compose-сервисы `pgworker-2` / `kafkaworker-2` / `valkeyworker-2` (используют задачи 4, 5); env-имена `PGW_API_HOST_PORT2`, `KFW_API_HOST_PORT2`, `VWK_API_HOST_PORT2` (используют задачи 4, 5, 7-доки).

**Ключевая техника:** YAML merge keys. Анкор верхнего уровня (`<<: *pgworker-base`) делает **shallow** merge — `environment` второго сервиса заменил бы env анкора целиком. Поэтому env вынесен в ОТДЕЛЬНЫЙ анкор (`x-pgworker-env`), и каждый сервис собирает `environment: {<<: *pgworker-env, <только AdvertiseUrl>}` — вложенный merge. `build`, `image`, `volumes`, `extra_hosts`, `restart` — из базового анкора (порты у второго отличаются, поэтому `ports` в анкор НЕ входит).

- [ ] **Step 2.1: Переписать deploy/docker-compose.yml (сервисы pgworker/pgworker-2)**

Заменить текущий сервис `pgworker:` (строки 5–87) на (комментарии оригинала сохранить, они переезжают в анкоры):

```yaml
# Вторые инстансы воркеров (t07, arch/14 §2): SPOF надзора устранён дефолтом —
# по два контейнера каждого воркера, координацию разбирает etcd (клэймы
# мигрируют ≤ TTL 15 c). Отличие второго сервиса ТОЛЬКО в публикации порта и
# AdvertiseUrl: env/volumes/build — общие анкоры (merge keys). Тома снапшотов
# и TLS — ОБЩИЕ на оба инстанса (лидерство снапшотов мигрирует между ними).
# Запуск PgWorker (spec §5.4, §10): docker.sock (plain на одном хосте /
# swarm manager), том снапшотов P12, секреты per-install из env (Д7).
# etcd запускается отдельно (dev-stand/compose.yaml или свой кластер).
x-pgworker-env: &pgworker-env
  # Обязательные секреты установки (Д7): не в git, не в etcd.
  PGW_PG_SUPERUSER_PASSWORD: ${PGW_PG_SUPERUSER_PASSWORD:?задайте секрет}
  PGW_PG_STANDBY_PASSWORD: ${PGW_PG_STANDBY_PASSWORD:?задайте секрет}
  PGW_BUCKET_ADMIN_PASSWORD: ${PGW_BUCKET_ADMIN_PASSWORD:?задайте секрет}
  PGW_BUCKET_MOVER_PASSWORD: ${PGW_BUCKET_MOVER_PASSWORD:?задайте секрет}
  # Конфигурация — env-оверрайды appsettings (пример: endpoints etcd).
  PgWorker__Etcd__Endpoints__0: ${PGW_ETCD_ENDPOINT:-http://localhost:2379}
  # Advertised-имя docker-хоста в portalloc/dsn (advertised-правило arch/16,
  # симметрия KafkaWorker:AdvertisedClientHost): адреса нод в etcd обязаны
  # быть резолвимы клиентами — панель пробирует host.docker.internal:15xxx/
  # 18xxx; внутреннее имя "local" резолвится только контейнерами воркеров
  # (extra_hosts), панель видела DNS-таймауты проб. Пусто (прод/мульти-хост)
  # → имя docker-хоста как есть.
  PgWorker__Docker__AdvertisedHost: ${PGW_ADVERTISED_HOST:-host.docker.internal}
  # HTTP API воркера (arch/14 §1.1, t03): mTLS-only грань — advertise
  # https://, ключи-серты из /tls (per-install пакет kfw-install-ca);
  # демо-сид POST /api/seed/demo за флагом (стенд; прод — false).
  # X-Api-Key удалён (t03) — аутентификация транспортная (mTLS).
  # AdvertiseUrl ЗАДАЁТСЯ В СЕРВИСАХ (уникален per инстанс, t07).
  PGW_API_TLS_CERT_PATH: /tls/pgserver.crt
  PGW_API_TLS_KEY_PATH: /tls/pgserver.key
  PGW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
  PgWorker__Api__EnableSeedEndpoint: ${PGW_API_ENABLE_SEED:-false}
  # Подсистема бэкапов (arch/19, t01): каркас — Enabled=false default;
  # S3-креды per-install только env (PGW_BACKUP_S3_*, deploy/.env.example).
  # Пустые значения валидны при Enabled=false (валидация старта воркера).
  PgWorker__Backups__Enabled: ${PGW_BACKUPS_ENABLED:-false}
  PgWorker__Backups__S3__Endpoint: ${PGW_BACKUP_S3_ENDPOINT:-}
  PgWorker__Backups__S3__Region: ${PGW_BACKUP_S3_REGION:-}
  PgWorker__Backups__S3__Bucket: ${PGW_BACKUP_S3_BUCKET:-}
  PgWorker__Backups__S3__AccessKey: ${PGW_BACKUP_S3_ACCESS_KEY:-}
  PgWorker__Backups__S3__SecretKey: ${PGW_BACKUP_S3_SECRET_KEY:-}
  # S3 из контейнеров агентов/джобов (single-host: host.docker.internal; пусто
  # → ENDPOINT как есть — паттерн Etcd:AdvertisedEndpoints, t03).
  PgWorker__Backups__S3__AdvertisedEndpoint: ${PGW_BACKUP_S3_ADVERTISED_ENDPOINT:-}
  # Образ джоба полного бэкапа (t02): тег = дефолт Job.Image; сборка —
  # build-сервис pgworker-backup ниже (compose build) или 00-up.sh.
  PgWorker__Backups__Job__Image: ${PGW_BACKUP_JOB_IMAGE:-pgworker-backup:dev}
  # PGTune-параметры PG-нод (arch/14 §2.1/§8): postgresql.conf ноды
  # рассчитывается при СОЗДАНИИ контейнера; память/CPU — из etcd-заявок
  # request_{cpu,mem} ноды (НЕ env), остальное — эти константы; канон P3
  # (wal_level=logical, walsenders) перекрывает расчёт; doorman =
  # Connections − 5 (P15). DbType=desktop запрещён (fail-fast старта).
  PgWorker__Pgtune__DbVersion: ${PGW_PGTUNE_DB_VERSION:-18}
  PgWorker__Pgtune__DbType: ${PGW_PGTUNE_DB_TYPE:-oltp}
  PgWorker__Pgtune__HdType: ${PGW_PGTUNE_HD_TYPE:-ssd}
  PgWorker__Pgtune__DbSize: ${PGW_PGTUNE_DB_SIZE:-mid_ram}
  PgWorker__Pgtune__Connections: ${PGW_PGTUNE_CONNECTIONS:-60}
  # ExcludeParams — env-массив задаётся ЦЕЛИКОМ (__0/__1 заменяют appsettings);
  # io_method/io_workers выключены, пока Spilo-18 не проверен на liburing.
  PgWorker__Pgtune__ExcludeParams__0: ${PGW_PGTUNE_EXCLUDE_0:-io_method}
  PgWorker__Pgtune__ExcludeParams__1: ${PGW_PGTUNE_EXCLUDE_1:-io_workers}

x-pgworker-base: &pgworker-base
  build:
    context: ..
    dockerfile: docker/PgWorker.Dockerfile
  image: pgworker:dev
  restart: unless-stopped
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    - pgw-snapshots:/snapshots
    # mTLS HTTP API (t03, arch/14 §1.1): per-install пакет kfw-install-ca
    # (bash deploy/tls/gen.sh; наполнение volume — dev-stand 00-up.sh).
    - pgw-api-tls:/tls:ro
    # RBAC вместо сокета наружу (arch/14 §2.2.1, НЕ включено: текущие стенды
    # работают root'ом контейнера): выделенный пользователь + группа docker
    # host-машины — раскомментировать и указать gid (getent group docker):
    #   user: "10001:10001"
    #   group_add: ["<gid docker>"]
    # и заменить bind сокета на tcp://…:2376 (mTLS, deploy/tls/gen-docker.sh).
  extra_hosts:
    # Ноды несут docker-хост "local" (appsettings Docker:Hosts) и пробируются
    # по этому имени; из контейнера резолвим его в docker-хост
    # (на Docker Desktop = host.docker.internal, на Linux — шлюз bridge).
    - "local:host-gateway"
    # advertised-хост записей portalloc/dsn — резолвим и его (Linux-паритет;
    # Docker Desktop резолвит сам, extra_hosts безвреден).
    - "host.docker.internal:host-gateway"

services:
  pgworker:
    <<: *pgworker-base
    ports:
      # Хост-публикация API параметризуется (коллизии на хосте, прецедент
      # METRICS_*_PORT стенда); внутри контейнера канонический 8080.
      - "${PGW_API_HOST_PORT:-8080}:8080"
    environment:
      <<: *pgworker-env
      PgWorker__Api__AdvertiseUrl: ${PGW_API_ADVERTISE_URL:-https://host.docker.internal:${PGW_API_HOST_PORT:-8080}}

  # Второй инстанс PgWorker (t07, arch/14 §2): тот же образ/секреты/тома;
  # отличается ТОЛЬКО порт публикации (ряд 8080→8083) и AdvertiseUrl —
  # advertise следует за портом автоматически (compose-дефолт, отдельная
  # env не нужна). container_name НЕ задаём (compose: deploy-pgworker-2-1).
  pgworker-2:
    <<: *pgworker-base
    ports:
      - "${PGW_API_HOST_PORT2:-8083}:8080"
    environment:
      <<: *pgworker-env
      PgWorker__Api__AdvertiseUrl: https://host.docker.internal:${PGW_API_HOST_PORT2:-8083}
```

- [ ] **Step 2.2: Переписать сервисы kafkaworker/kafkaworker-2**

Заменить текущий сервис `kafkaworker:` (строки 92–123) на:

```yaml
# KafkaWorker (arch/16 §2.1/§8): docker.sock, том снапшотов P12.
# Env-секретов per-install нет (единственный секрет — per-cluster
# app_password в etcd). AdvertiseUrl — в сервисах (уникален per инстанс).
x-kafkaworker-env: &kafkaworker-env
  KafkaWorker__Etcd__Endpoints__0: ${KFW_ETCD_ENDPOINT:-http://localhost:2379}
  # Advertised-хост CLIENT-listener → endpoints в etcd резолвимы клиентами
  # (arch/16 advertised-правило; null допустим только когда имя docker-хоста
  # резолвимо клиентами само по себе).
  KafkaWorker__AdvertisedClientHost: ${KFW_ADVERTISED_CLIENT_HOST:-host.docker.internal}
  # HTTP API воркера (arch/16 §1.1): advertise-URL https:// в /kafkaworker/api/<id>,
  # mTLS-серты из /tls, демо-сид за флагом (стенд; прод — false). KFW_API_KEY
  # удалён (t03) — грань mTLS-only.
  KFW_API_TLS_CERT_PATH: /tls/server.crt
  KFW_API_TLS_KEY_PATH: /tls/server.key
  KFW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
  KafkaWorker__Api__EnableSeedEndpoint: ${KFW_API_ENABLE_SEED:-false}

x-kafkaworker-base: &kafkaworker-base
  build:
    context: ..
    dockerfile: docker/KafkaWorker.Dockerfile
  image: kafkaworker:dev
  restart: unless-stopped
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    - kfw-snapshots:/snapshots
    # mTLS HTTP API (t03, arch/16 §1.1): per-install TLS-пакет из deploy/tls
    # (генерация — bash deploy/tls/gen.sh; наполнение volume — 00-up.sh стенда).
    - kfw-api-tls:/tls:ro
  extra_hosts:
    # docker-хост "local" (appsettings KafkaWorker:Docker:Hosts) — из контейнера
    # резолвим его в шлюз docker-хоста (прецедент pgworker выше).
    - "local:host-gateway"

  kafkaworker:
    <<: *kafkaworker-base
    ports:
      - "8081:8080" # /healthz (8081 — не конфликтует с pgworker)
    environment:
      <<: *kafkaworker-env
      KafkaWorker__Api__AdvertiseUrl: ${KFW_API_ADVERTISE_URL:-https://host.docker.internal:8081}

  # Второй инстанс KafkaWorker (t07, arch/16 §2.1): ряд 8081→8084; advertise
  # следует за портом (compose-дефолт). Тома kfw-snapshots/kfw-api-tls — общие.
  kafkaworker-2:
    <<: *kafkaworker-base
    ports:
      - "${KFW_API_HOST_PORT2:-8084}:8080"
    environment:
      <<: *kafkaworker-env
      KafkaWorker__Api__AdvertiseUrl: https://host.docker.internal:${KFW_API_HOST_PORT2:-8084}
```

ВНИМАНИЕ на отступ: `kafkaworker:`/`kafkaworker-2:` — на уровне `services:` (2 пробела), анкоры `x-*` — на уровне 0; в блоке выше между анкором и `kafkaworker:` не должно быть пустой строки с отступом.

- [ ] **Step 2.3: Переписать сервисы valkeyworker/valkeyworker-2**

Заменить текущий сервис `valkeyworker:` (строки 128–158) на:

```yaml
# ValkeyWorker (arch/21 §2/§8): docker.sock, том снапшотов P12. Хост-порт API
# 8082 — продолжение ряда pg 8080 / kfw 8081. Томов данных нод НЕТ
# (persistence off — томов у домена не бывает). AdvertiseUrl — в сервисах.
x-valkeyworker-env: &valkeyworker-env
  ValkeyWorker__Etcd__Endpoints__0: ${VWK_ETCD_ENDPOINT:-http://localhost:2379}
  # Advertised-хост клиентского порта нод → endpoints в etcd резолвимы
  # клиентами (arch/21 §2 advertised-правило).
  ValkeyWorker__AdvertisedClientHost: ${VWK_ADVERTISED_CLIENT_HOST:-host.docker.internal}
  # HTTP API воркера (arch/21 §1.1): advertise-URL https:// в /valkeyworker/api/<id>,
  # mTLS-серты из /tls, демо-сид за флагом (стенд; прод — false).
  VWK_API_TLS_CERT_PATH: /tls/server.crt
  VWK_API_TLS_KEY_PATH: /tls/server.key
  VWK_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
  ValkeyWorker__Api__EnableSeedEndpoint: ${VWK_API_ENABLE_SEED:-false}

x-valkeyworker-base: &valkeyworker-base
  build:
    context: ..
    dockerfile: docker/ValkeyWorker.Dockerfile
  image: valkeyworker:dev
  restart: unless-stopped
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    - vw-snapshots:/snapshots
    # mTLS HTTP API (arch/21 §1.1): per-install TLS-пакет из deploy/tls
    # (генерация — bash deploy/tls/gen.sh; наполнение volume — 00-up.sh стенда).
    - vw-api-tls:/tls:ro
  extra_hosts:
    # docker-хост "local" (appsettings Docker:Hosts) + advertised-хост нод —
    # из контейнера резолвим их в шлюз docker-хоста.
    - "local:host-gateway"
    - "host.docker.internal:host-gateway"

  valkeyworker:
    <<: *valkeyworker-base
    ports:
      - "${VWK_API_HOST_PORT:-8082}:8080" # /healthz (ряд pg 8080 / kfw 8081 / vwk 8082)
    environment:
      <<: *valkeyworker-env
      ValkeyWorker__Api__AdvertiseUrl: ${VWK_API_ADVERTISE_URL:-https://host.docker.internal:8082}

  # Второй инстанс ValkeyWorker (t07, arch/21 §2): ряд 8082→8085; advertise
  # следует за портом (compose-дефолт). Тома vw-snapshots/vw-api-tls — общие.
  valkeyworker-2:
    <<: *valkeyworker-base
    ports:
      - "${VWK_API_HOST_PORT2:-8085}:8080"
    environment:
      <<: *valkeyworker-env
      ValkeyWorker__Api__AdvertiseUrl: https://host.docker.internal:${VWK_API_HOST_PORT2:-8085}
```

Секция `pgworker-backup` (профиль build) и `volumes:` внизу файла — БЕЗ изменений.

- [ ] **Step 2.4: Дополнить deploy/.env.example**

В `deploy/.env.example` после блока комментария `# PGW_API_HOST_PORT=8080` (строки 36–39) добавить:

```
# Вторые инстансы воркеров (t07, arch/14 §2 / arch/16 §2.1 / arch/21 §2): по
# два контейнера каждого воркера ВСЕГДА (SPOF надзора устранён дефолтом).
# Хост-порты вторых — продолжение ряда 8080/8081/8082; advertise вторых
# инстансов следует за их портами автоматически (compose-дефолт) — отдельная
# advertise-env не нужна.
# PGW_API_HOST_PORT2=8083
# KFW_API_HOST_PORT2=8084
# VWK_API_HOST_PORT2=8085
```

- [ ] **Step 2.5: Проверить валидность compose и наследование env**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance
[ -f deploy/.env ] || cp deploy/.env.example deploy/.env
docker compose -f deploy/docker-compose.yml --env-file deploy/.env config --services | sort
```

Ожидание: ровно 6 сервисов (профиль `build` в config без `--profiles` не раскрывается): `kafkaworker`, `kafkaworker-2`, `pgworker`, `pgworker-2`, `valkeyworker`, `valkeyworker-2`.

Затем проверить nested-merge env (критично: shallow merge не должен был потерять секреты у вторых сервисов):

```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env config | \
  awk '/^  pgworker-2:/,/^  [a-z-]+:/' | grep -c 'PGW_PG_SUPERUSER_PASSWORD\|PgWorker__Api__AdvertiseUrl'
docker compose -f deploy/docker-compose.yml --env-file deploy/.env config | \
  awk '/^  kafkaworker-2:/,/^  [a-z-]+:/' | grep -c 'KafkaWorker__Api__AdvertiseUrl'
```

Ожидание: первая команда ≥2 (секрет И AdvertiseUrl есть у `pgworker-2`), вторая ≥1.

**Fallback (если merge `environment: {<<: *anchor}` не поддержан вашей версией compose — config показывает у `pgworker-2` пустой/неполный environment):** вынести ПОЛНЫЙ env каждого второго сервиса явным текстом (копия набора анкора + своя строка AdvertiseUrl), без вложенного merge в `environment`. Остальная структура не меняется.

Дополнительная проверка уникальности advertise (интерполяция):

```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env config | grep 'AdvertiseUrl'
```

Ожидание: 6 строк; у `pgworker`/`pgworker-2` — `...:8080`/`...:8083`, у kfw — `...:8081`/`...:8084`, у vwk — `...:8082`/`...:8085` (при defaults).

- [ ] **Step 2.6: Commit**

```bash
git add deploy/docker-compose.yml deploy/.env.example
git commit -m "t07: deploy — вторые сервисы pgworker-2/kafkaworker-2/valkeyworker-2 (ряд 8083/8084/8085, YAML-анкоры env+base, общие тома) + .env.example"
```

---

### Task 3: Стенд — вторые сервисы kfw-2/vwk-2, SAN TLS-пакета, Prometheus-таргеты

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (новые сервисы `kafkaworker-2`, `valkeyworker-2` после соответствующих первых)
- Modify: `deploy/tls/gen.sh` (SAN `DNS:kafkaworker-2,DNS:valkeyworker-2` + условие перегенерации)
- Modify: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` (static_configs джоб kafkaworker/valkeyworker)

**Interfaces:**
- Consumes: Task 1 (канон стендовых `-2` без хост-публикации, compose-DNS advertise).
- Produces: контейнеры `as-kafkaworker-2` / `as-valkeyworker-2` (используют задачи 4, 5), SAN-покрытие `kafkaworker-2`/`valkeyworker-2` в `server.crt` (без него Prometheus-таргет `-2` не поднимется — AC6), таргеты Prometheus.

**Обоснование правки gen.sh (не противоречит НЕ-целям):** панель валидирует серт воркера по цепочке CA без проверки имени хоста (`src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs`), но Prometheus `tls_config` проверяет имя таргета против SAN. Стендовые `-2` имеют host `kafkaworker-2`/`valkeyworker-2` → без SAN чек 65 упадёт на «все таргеты up». `deploy/tls/gen.sh` — скрипт поставки (деплой-слой), не код воркера. Деплойные pgw-инстансы ходят по `host.docker.internal` — уже в SAN `pgserver.crt`, там правок не нужно.

- [ ] **Step 3.1: Добавить сервис kafkaworker-2 в стендовый compose**

В `dev-stand/adminpanel/docker-compose.yml` после сервиса `kafkaworker:` (заканчивается `depends_on: [etcd]`, ~строка 345) добавить (клон первого с точечными отличиями — container_name, advertise, без ports):

```yaml
  # Второй инстанс KafkaWorker (t07, arch/16 §2.1): тот же образ/env/сеть,
  # compose-DNS advertise; БЕЗ хост-публикации порта (панель и Prometheus
  # ходят из сети стенда; хост-порт 8082 остаётся у первого для curl-чеков).
  kafkaworker-2:
    build:
      context: ../..
      dockerfile: docker/KafkaWorker.Dockerfile
    image: kafkaworker:dev
    container_name: as-kafkaworker-2
    restart: unless-stopped
    profiles: ["kafka"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - kfw-snapshots:/snapshots
      - ../../deploy/tls:/tls:ro
    extra_hosts:
      - "local:host-gateway"
    environment:
      KafkaWorker__Etcd__Endpoints__0: http://etcd:2379
      KafkaWorker__AdvertisedClientHost: host.docker.internal
      KafkaWorker__Api__AdvertiseUrl: https://kafkaworker-2:8080
      KFW_API_TLS_CERT_PATH: /tls/server.crt
      KFW_API_TLS_KEY_PATH: /tls/server.key
      KFW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
      KafkaWorker__Api__EnableSeedEndpoint: "true"
    depends_on: [etcd]
```

- [ ] **Step 3.2: Добавить сервис valkeyworker-2**

После сервиса `valkeyworker:` (заканчивается `depends_on: [etcd]`, ~строка 376) добавить:

```yaml
  # Второй инстанс ValkeyWorker (t07, arch/21 §2): симметрично kafkaworker-2 —
  # compose-DNS advertise, без хост-публикации; клэймы/portalloc разрулит etcd.
  valkeyworker-2:
    build:
      context: ../..
      dockerfile: docker/ValkeyWorker.Dockerfile
    image: valkeyworker:dev
    container_name: as-valkeyworker-2
    restart: unless-stopped
    profiles: ["valkey"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - vw-snapshots:/snapshots
      - ../../deploy/tls:/tls:ro
    extra_hosts:
      - "local:host-gateway"
    environment:
      ValkeyWorker__Etcd__Endpoints__0: http://etcd:2379
      ValkeyWorker__AdvertisedClientHost: host.docker.internal
      ValkeyWorker__Api__AdvertiseUrl: https://valkeyworker-2:8080
      VWK_API_TLS_CERT_PATH: /tls/server.crt
      VWK_API_TLS_KEY_PATH: /tls/server.key
      VWK_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
      ValkeyWorker__Api__EnableSeedEndpoint: "true"
    depends_on: [etcd]
```

- [ ] **Step 3.3: Расширить SAN серверного серта в deploy/tls/gen.sh**

В `deploy/tls/gen.sh` заменить строку 12:

```bash
SERVER_SAN="DNS:kafkaworker,DNS:valkeyworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
```

на:

```bash
SERVER_SAN="DNS:kafkaworker,DNS:kafkaworker-2,DNS:valkeyworker,DNS:valkeyworker-2,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
```

и заменить блок перегенерации (строки 36–39):

```bash
# t03: старый server-серт без DNS:valkeyworker — перегенерируем (CA жив).
if [ ! -f server.crt ] || ! openssl x509 -in server.crt -noout -text 2>/dev/null | grep -q 'DNS:valkeyworker'; then
  issue server kafkaworker serverAuth "$SERVER_SAN"
fi
```

на (условие t03 расширено до t07: серт без DNS:kafkaworker-2 не покрывает и стендовые вторые имена — новый SAN содержит всё):

```bash
# t03+t07: старый server-серт без DNS:valkeyworker (t03) или без
# DNS:kafkaworker-2/DNS:valkeyworker-2 (стендовые вторые инстансы, t07 —
# Prometheus проверяет имя таргета против SAN) — перегенерируем (CA жив).
if [ ! -f server.crt ] || ! openssl x509 -in server.crt -noout -text 2>/dev/null | grep -q 'DNS:kafkaworker-2'; then
  issue server kafkaworker serverAuth "$SERVER_SAN"
fi
```

- [ ] **Step 3.4: Prometheus static_configs — оба инстанса kfw/vwk**

В `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` заменить:

```yaml
    static_configs: [{targets: ["kafkaworker:8080"]}]
```

на:

```yaml
    static_configs: [{targets: ["kafkaworker:8080", "kafkaworker-2:8080"]}]
```

и

```yaml
    static_configs: [{targets: ["valkeyworker:8080"]}]
```

на:

```yaml
    static_configs: [{targets: ["valkeyworker:8080", "valkeyworker-2:8080"]}]
```

(джоба `pgworker` — file_sd, переписывает 00-up.sh в задаче 4; `adminpanel`/`patroni` не трогаем).

- [ ] **Step 3.5: Проверить валидность compose стенда и SAN**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance/dev-stand/adminpanel
docker compose --profile kafka --profile valkey config --services | sort | grep -E 'kafkaworker|valkeyworker'
bash ../../deploy/tls/gen.sh
openssl x509 -in ../../deploy/tls/server.crt -noout -text | grep -o 'DNS:kafkaworker-2\|DNS:valkeyworker-2'
```

Ожидание: в списке сервисов есть `kafkaworker`, `kafkaworker-2`, `valkeyworker`, `valkeyworker-2`; gen.sh печатает `✓ TLS-пакет…`; openssl выводит обе строки `DNS:kafkaworker-2` и `DNS:valkeyworker-2` (серверный серт перегенерирован, ca.pem и клиентские серты не тронуты).

- [ ] **Step 3.6: Commit**

```bash
git add dev-stand/adminpanel/docker-compose.yml deploy/tls/gen.sh dev-stand/adminpanel/metrics/prometheus/prometheus.yml
git commit -m "t07: стенд — kafkaworker-2/valkeyworker-2 (compose-DNS advertise, без хост-портов), SAN вторых имён в gen.sh, оба таргета kfw/vwk в Prometheus"
```

---

### Task 4: 00-up.sh — подъём/проверка обоих инстансов; аудит 90-down.sh и чеков, точечные правки чеков 20/50/65

**Files:**
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (шапка-порты, file_sd, .env-sync, подъём `pgworker pgworker-2`, healthz обоих, ≥2 ключей всех трёх воркеров)
- Modify: `dev-stand/adminpanel/checks/20-alerts.sh:62-67,91,164,177` (stop/start ОБЕИХ pgworker — и в arrange-паузе репарации, и в сценарии worker-api-unreachable)
- Modify: `dev-stand/adminpanel/checks/50-kafka-api.sh:184` (stop ОБЕИХ kafkaworker)
- Modify: `dev-stand/adminpanel/checks/65-metrics.sh:22,31-40` (start/`up -d` обоих kfw/vwk)
- Audit (без правок по умолчанию; все 15 оставшихся чеков каталога — 90-down отдельно в шаге 4.5, остальные 14 в таблице шага 4.7): `90-down.sh`, `05-seed.sh`, `10-smoke-api.sh`, `15-cluster-create.sh`, `30-failover.sh`, `40-live-probes.sh`, `45-backups-storage.sh`, `51-valkey-api.sh`, `55-kafka-e2e.sh`, `57-kafka-worker-health.sh`, `58-kafka-probe-churn.sh`, `59-kafka-regen.sh`, `60-move-ops.sh`, `66-kafka-worker-churn.sh`, `70-worker-cert.sh`

**Interfaces:**
- Consumes: сервисы задач 2–3 (`pgworker-2`, `as-kafkaworker-2`, `as-valkeyworker-2`), env `PGW_API_HOST_PORT2`.
- Produces: 00-up.sh, поднимающий и проверяющий 2×3 инстансов (AC1/AC3-подготовка); чеки, совместимые с двумя инстансами (использует задача 5).

- [ ] **Step 4.1: 00-up.sh — порты/файл file_sd/.env-синхронизация**

В `00-up.sh` заменить блок строк 10–16 (комментарий хост-порта + `export PGW_API_HOST_PORT…` + `printf … pgworker-targets.json`) на:

```bash
# Хост-публикация API pgworker параметризуется (коллизии портов на хосте;
# канон 8080, второй инстанс — 8083, t07). Advertise обязан совпадать с
# фактической публикацией (панель и чеки стучатся по advertise),
# prometheus-target — файл file_sd (оба инстанса).
export PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
export PGW_API_HOST_PORT2="${PGW_API_HOST_PORT2:-8083}"
[ "${PGW_API_ADVERTISE_URL:-}" ] || export PGW_API_ADVERTISE_URL="https://host.docker.internal:${PGW_API_HOST_PORT}"
printf '[{"targets": ["host.docker.internal:%s", "host.docker.internal:%s"]}]\n' \
  "$PGW_API_HOST_PORT" "$PGW_API_HOST_PORT2" \
  > metrics/prometheus/pgworker-targets.json
```

- [ ] **Step 4.2: 00-up.sh — синхронизация PGW_API_HOST_PORT2 в deploy/.env**

Рядом с существующей синхронизацией `PGW_API_HOST_PORT` (строки 98–102, блок `if grep -q '^PGW_API_HOST_PORT=' …`) добавить сразу после него тот же блок для второго порта:

```bash
# t07: тот же sync для второго инстанса — чеки читают порт из .env.
if grep -q '^PGW_API_HOST_PORT2=' "$ROOT/deploy/.env"; then
  sed -i.bak "s/^PGW_API_HOST_PORT2=.*/PGW_API_HOST_PORT2=$PGW_API_HOST_PORT2/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
else
  printf 'PGW_API_HOST_PORT2=%s\n' "$PGW_API_HOST_PORT2" >> "$ROOT/deploy/.env"
fi
```

- [ ] **Step 4.3: 00-up.sh — подъём ОБЕИХ pgworker**

Заменить строку 106 (в ретрай-цикле `pg_up_ok`):

```bash
  if ( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker 2>&1 | tail -2 ); then
```

на (обе записи в одном up — атомарно для ретрая):

```bash
  if ( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 2>&1 | tail -2 ); then
```

и замену healthz-блока (строки 113–116):

```bash
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (https :${PGW_API_HOST_PORT:-8080}/healthz по mTLS; docker logs deploy-pgworker-1)"; exit 1; }
echo "  pgworker жив (https :${PGW_API_HOST_PORT:-8080}/healthz, mTLS, общий etcd-контур)"
```

на (healthz обоих портов + ≥2 ключа `/pgworker/api/`):

```bash
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (https :${PGW_API_HOST_PORT:-8080}/healthz по mTLS; docker logs deploy-pgworker-1)"; exit 1; }
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null \
  || { echo "❌ pgworker-2 не ожил за 60 c (https :${PGW_API_HOST_PORT2:-8083}/healthz по mTLS; docker logs deploy-pgworker-2-1)"; exit 1; }
# t07: оба инстанса публикуют свой lease-ключ дискавери (InstanceId = GUID,
# уникальный per контейнер) — двухинстансовый надзор доказан ключами.
pgw_api_keys=0
for i in $(seq 1 30); do
  pgw_api_keys="$(ect get /pgworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${pgw_api_keys:-0}" -ge 2 ] && break
  sleep 2
done
[ "${pgw_api_keys:-0}" -ge 2 ] \
  || { echo "❌ /pgworker/api/ содержит ${pgw_api_keys:-0} ключей (<2) — второй инстанс не публикует AdvertiseUrl/keepalive (docker logs deploy-pgworker-2-1)"; exit 1; }
echo "  pgworker жив ×2 (:${PGW_API_HOST_PORT:-8080} + :${PGW_API_HOST_PORT2:-8083}/healthz mTLS; ключей /pgworker/api/: ${pgw_api_keys})"
```

Обновить комментарий блока 1b (строки 83–93): в первой строке заменить «воркер из deploy/docker-compose.yml» на «воркеры (2 инстанса, t07) из deploy/docker-compose.yml».

- [ ] **Step 4.4: 00-up.sh — усиление проверок kfw/vwk до ≥2 ключей**

Шаг 7 (kafkaworker, строки 221–229): заменить оба вхождения условия

```bash
  [ -n "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | head -1)" ] && break
```

и последующий ассерт — на подсчёт ключей API-дискавери обоих инстансов:

```bash
# 7) kafkaworker жив ×2 (t07): по два lease-ключа /kafkaworker/api/ и
#    /kafkaworker/instances/ (два инстанса = два ключа каждого вида).
kfw_keys=0
for i in $(seq 1 60); do
  kfw_keys="$(ect get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${kfw_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${kfw_keys:-0}" -ge 2 ] \
  || { echo "❌ /kafkaworker/api/ содержит ${kfw_keys:-0} ключей (<2; docker compose logs kafkaworker kafkaworker-2)"; exit 1; }
[ "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /kafkaworker/instances/ меньше 2 ключей"; exit 1; }
echo "  kafkaworker жив ×2 (heartbeat /kafkaworker/{api,instances}/*: по ${kfw_keys})"
```

Шаг 7b (valkeyworker, строки 231–239) — аналогично:

```bash
# 7b) valkeyworker жив ×2 (t03+t07): по два lease-ключа /valkeyworker/api/ и
#     /valkeyworker/instances/ — их ждут панель (WorkerEndpoints) и чек 51.
vwk_keys=0
for i in $(seq 1 60); do
  vwk_keys="$(ect get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${vwk_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${vwk_keys:-0}" -ge 2 ] \
  || { echo "❌ /valkeyworker/api/ содержит ${vwk_keys:-0} ключей (<2; docker compose logs valkeyworker valkeyworker-2)"; exit 1; }
[ "$(ect get /valkeyworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /valkeyworker/instances/ меньше 2 ключей"; exit 1; }
echo "  valkeyworker жив ×2 (heartbeat /valkeyworker/{api,instances}/*: по ${vwk_keys})"
```

- [ ] **Step 4.5: Аудит 90-down.sh (без правок при положительном исходе)**

Прогнать и убедиться (критерий: down нигде не перечисляет сервисы воркеров по одному — иначе живым оставался бы `-2`):

```bash
grep -n "pgworker\|kafkaworker\|valkeyworker" dev-stand/adminpanel/checks/90-down.sh
```

Ожидание: единственное совпадение grep — строка 5, комментарий шапки «…valkeyworker + демо-контейнер vwk-demo-node1…» (сам `docker rm -f vwk-demo-node1` в строке 10 паттерна `pgworker|kafkaworker|valkeyworker` не содержит и grep'ом не виден — это не расхождение аудита). Команды `docker compose … down` (строки 14/17) гасят ВСЕ сервисы профилей, включая `-2`, точечных перечислений сервисов воркеров нет. Демо-контейнер сида valkey по-прежнему один: клэйм кластера demo один, каким бы инстансом он ни держался. Deploy-проект (`deploy-pgworker*`) 90-down не трогает — осознанный дизайн (комментарий 00-up.sh шаг 1b: «контейнер deploy-проекта переживает 90-down (другой compose-проект)»), полной системой управляет 00-up.sh. Вердикт: правок не требуется. Если grep нашёл ТОЧЕЧНУЮ команду `stop/start <один сервис воркера>` — включить оба сервиса в эту строку (прецедент — правки 4.6).

- [ ] **Step 4.6: Правки чеков, семантически противоречащих двум инстансам**

Четыре точечные правки в трёх чеках (остальные чеки — аудит в 4.7):

1) `20-alerts.sh` — ТРИ пары замен stop/start: пауза ОБОИХ инстансов нужна и в arrange (Assert 2), и в сценарии 503/алерта (шаг 6). С живым вторым инстансом «тишина» чека невозможна: репарация брошенных статусов выполняется ТОЛЬКО держателем клэйма кластера (`src/PgWorker.Moves/Process/MoveRepairProcess.cs:32` — `if (!claims.IsMine(cluster))`) — при клэйме у `deploy-pgworker-2-1` репарация вообще не прерывается, при клэйме у первого — мигрирует второму ≤15 c; в обоих случаях гашение статусов обгоняет тик панели и Assert 2 (`wait_alert move-stale …`, бюджет 15 с) становится гонкой.

1a) Arrange-пауза (строки 65–66):

```bash
  echo "  (full) пауза PgWorker (deploy-pgworker-1) до нахерачивания"
  docker stop -t 3 deploy-pgworker-1 >/dev/null
```

заменить на (echo обновляем вместе с командой):

```bash
  # t07: паузим ОБА инстанса — репарация идёт у держателя клэйма, живой
  # второй гасил бы статусы быстрее тика панели (гонка Assert 2)
  echo "  (full) пауза PgWorker (deploy-pgworker-1 + deploy-pgworker-2-1) до нахерачивания"
  docker stop -t 3 deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null
```

1b) Возврат из паузы (строка 91):

```bash
docker start deploy-pgworker-1 >/dev/null
```

заменить на:

```bash
# t07: оба инстанса (пауза была у обоих)
docker start deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null
```

(последующий healthz-цикл строки 92 ждёт первого — его достаточно как маркера возврата; второй поднимается тем же `docker start`.)

1c) Шаг 6 «мутация → 503 + worker-api-unreachable» (строка 164) требует НУЛЯ живых инстансов (правило срабатывает только при нуле живых ключей; один живой `-2` = ни 503, ни алерта). Заменить:

```bash
  ( cd ../../deploy && docker compose stop pgworker >/dev/null 2>&1 )
```

на:

```bash
  # t07: оба инстанса — иначе второй держит ключ /pgworker/api/ (ни 503, ни алерта)
  ( cd ../../deploy && docker compose stop pgworker pgworker-2 >/dev/null 2>&1 )
```

и строку 177:

```bash
  ( cd ../../deploy && docker compose start pgworker >/dev/null 2>&1 )
```

на:

```bash
  ( cd ../../deploy && docker compose start pgworker pgworker-2 >/dev/null 2>&1 )
```

2) `50-kafka-api.sh` строка 184 (финал: стоп воркера + ассерт worker-api-unreachable). Заменить:

```bash
docker compose --profile kafka stop kafkaworker >/dev/null 2>&1
```

на:

```bash
# t07: оба инстанса — второй живой держал бы ключ /kafkaworker/api/ (алерта нет)
docker compose --profile kafka stop kafkaworker kafkaworker-2 >/dev/null 2>&1
```

(65-metrics.sh поднимает обоих — правка ниже; end-state «после сида воркеры остановлены» сохраняется для обоих.)

3) `65-metrics.sh` — гейт живости перед «все таргеты up»: остановленный `-2` = лежащий таргет = падение шага 2. Строку 22:

```bash
docker start as-kafkaworker >/dev/null 2>&1 || true
```

заменить на:

```bash
# t07: оба инстанса kafkaworker (50-й останавливает обоих; таргет kafkaworker-2
# обязан быть жив к шагу «все scrape-джобы up»)
docker start as-kafkaworker as-kafkaworker-2 >/dev/null 2>&1 || true
```

Блок 0.1 (строки 31–40): строку 31

```bash
docker compose --profile valkey up -d valkeyworker >/dev/null 2>&1 || true
```

заменить на:

```bash
docker compose --profile valkey up -d valkeyworker valkeyworker-2 >/dev/null 2>&1 || true
```

и дополнить healthz-проверку: после существующего цикла/ассерта для `valkeyworker` добавить симметричный для второго (exec внутрь контейнера, mTLS-пара healthcheck):

```bash
for i in $(seq 1 60); do
  docker compose exec -T valkeyworker-2 curl -fsS -m 3 \
    --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1
done
docker compose exec -T valkeyworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ valkeyworker-2 не ожил за 60 c (:8080/healthz mTLS изнутри; docker compose logs valkeyworker-2)"; exit 1; }
```

- [ ] **Step 4.7: Аудит остальных чеков — зафиксировать вердикты «без правок»**

Прогнать просмотр каждого и подтвердить (критерий: чек не адресует «инстанс ровно один» — хост-порт/имя первого инстанса остаётся валидным, первый никуда не девается):

| Чек | Вердикт | Основание |
|---|---|---|
| `05-seed.sh` | без правок | `force-recreate pgworker`/`up -d kafkaworker|valkeyworker` поднимают первого; сид идемпотентен (живой config → 200 no-op), клэйм-гонка с живым вторым безвредна (идемпотентность по факту) |
| `10-smoke-api.sh` | без правок | панельные ассерты кластера/алертов, инстансы воркеров не адресует |
| `15-cluster-create.sh` | без правок | мутации панель→прокси; failover панели по списку endpoints — второй инстанс только помогает |
| `30-failover.sh` | без правок | PG-ноды/эмуляторы, воркеров не трогает |
| `40-live-probes.sh` | без правок | live-пробы нод через панель |
| `45-backups-storage.sh` | без правок | грань «Хранилище бэкапов» — панель/MinIO (`mc` + `/api/backups/storage`), к инстансам воркеров не адресуется |
| `51-valkey-api.sh` | без правок | мутации через панель→прокси (failover); `vwk_curl` — внутрь первого, поднимаемого 05-seed |
| `55-kafka-e2e.sh`, `59-kafka-regen.sh` | без правок | `--profile kafka down -v` гасит весь профиль, включая `-2` |
| `57-kafka-worker-health.sh` | без правок | стоп etcd деградирует ОБА инстанса → worker-unhealthy загорается; рестарт-ассерт (`StartedAt`) — по первому, чек его и стоп/не трогает |
| `58-kafka-probe-churn.sh` | без правок | CPU панели, воркеров не касается |
| `60-move-ops.sh` | без правок | move-ops через API панели, чек сам НЕ останавливает воркера и инстансы не считает; заявки/статусы разбирает единственный держатель клэйма — каким инстансом он ни был, семантика тика неизменна |
| `66-kafka-worker-churn.sh` | без правок | ассерты «≤» (CPU/логи/потоки первого); клэйм churnkw может взять второй — чек не падает (чувствительность снижается осознанно, ассертов «ровно один» нет) |
| `70-worker-cert.sh` | без правок | карточки ДОМЕНОВ: панель безусловно отдаёт ТРИ (pgworker/kafkaworker/valkeyworker — `GetWorkersQueryHandler`, `src/AdminPanel.Api/Operations/WorkersModule.cs`; подтверждено `src/tests/AdminPanel.IntegrationTests/ValkeyApiTests.cs:203`); число доменов НЕ зависит от инстансов. Известное вне scope t07: ассерт `.workers \| length == 2` чека устарел относительно кода панели (упал бы по несвязанной с t07 причине) — чек 70 в задачу 5 не входит, фикс отдельной задачей. `instances[0].applyStatus` — broadcast-рестарт применяет серт обоим, applied у первого в списке достигается; SAN: оба деплой-инстанса на host.docker.internal (Distinct хостов не меняется) |

Если при просмотре найден ассерт вида «инстанс/ключ ровно один» или «нулевой ключ при живом втором» — править по образцу 4.6 (включить оба инстанса в стоп/старт); иначе ничего не менять.

- [ ] **Step 4.8: Синтаксическая проверка правленных скриптов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance/dev-stand/adminpanel
bash -n checks/00-up.sh && bash -n checks/20-alerts.sh && bash -n checks/50-kafka-api.sh && bash -n checks/65-metrics.sh && echo OK
```

Ожидание: `OK`.

- [ ] **Step 4.9: Commit**

```bash
git add dev-stand/adminpanel/checks/00-up.sh dev-stand/adminpanel/checks/20-alerts.sh dev-stand/adminpanel/checks/50-kafka-api.sh dev-stand/adminpanel/checks/65-metrics.sh
git commit -m "t07: 00-up — оба pgworker (healthz ×2, ≥2 ключей всех воркеров, file_sd 2 таргета, .env PORT2); чек 20 — стоп/старт обоих в arrange-паузе репарации и сценарии 503/алерта (репарация у держателя клэйма, нуль живых для worker-api-unreachable); чеки 50/65 — оба инстанса kfw/vwk"
```

---

### Task 5: Чек 35-worker-second-instance.sh + полный прогон стенда (AC1–AC4, AC6)

**Files:**
- Create: `dev-stand/adminpanel/checks/35-worker-second-instance.sh` (chmod +x)

**Interfaces:**
- Consumes: задачи 2–4 (2×3 инстансов на стенде, `PGW_API_HOST_PORT2` в `.env`, `deploy-pgworker-1`/`deploy-pgworker-2-1`, `as-kafkaworker(-2)`, `as-valkeyworker(-2)`, ключи `/pgworker|kafkaworker|valkeyworker/{api,instances,claims}`).
- Produces: приёмочный чек R (AC4) — kill→takeover каждого воркера с восстановлением; финальная валидация AC1/AC2/AC3/AC6 полного стенда.

**Контракт ключей для ассертов:** lease-ключ `<prefix>/api/<InstanceId>` (TTL 15 c, keepalive 5 c) и `<prefix>/instances/<InstanceId>` гаснут со смертью инстанса; клэйм кластера `<prefix>/claims/<C>` — JSON `{"instance":"<guid>","since_unix":…,"phase":…}` у текущего держателя (`src/Shared.Etcd/Coordination/ClaimStore.cs`).

- [ ] **Step 5.1: Создать чек 35 целиком**

Файл `dev-stand/adminpanel/checks/35-worker-second-instance.sh` (новый, полный текст):

```bash
#!/usr/bin/env bash
# 35-worker-second-instance.sh (t07, spec §4.4): kill→takeover на полном
# стенде. Для каждого воркера (pgw — deploy-контейнеры, kfw/vwk — стендовые
# as-*): Arrange (оба инстанса живы, по 2 ключа дискавери) → Act (docker stop
# ПЕРВОГО: unless-stopped не поднимает контейнер после явного stop — окно
# владения чисто уходит второму) → Assert ≤60 c (ключей ровно 1, healthz
# второго жив, клэйм кластера у второго, панель не 503) → Restore (docker
# start первого, оба ключа снова). Идемпотентен, стенд в исходном состоянии.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"

# Хост-порты API pgworker: env → deploy/.env (пишет 00-up.sh) → defaults.
env_or_dotenv() { # <env-имя> <default>
  local v="${!1:-}"
  [ -n "$v" ] && { echo "$v"; return; }
  v="$(awk -F= -v k="$1" '$1==k{print $2}' "$ROOT/deploy/.env" 2>/dev/null)"
  echo "${v:-$2}"
}
PGW_PORT1="$(env_or_dotenv PGW_API_HOST_PORT 8080)"
PGW_PORT2="$(env_or_dotenv PGW_API_HOST_PORT2 8083)"
# Контейнер первого инстанса pgw: compose-проект deploy (00-up.sh поднимает из
# deploy/); env-оверрайд на случай нестандартного имени проекта.
PGW1="${PGW1_CONTAINER:-deploy-pgworker-1}"
MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
keys() { ect get "$1" --prefix --keys-only 2>/dev/null | grep -c . || true; }
wait_keys() { # <prefix> <want> <budget-sec> <label>
  local got=""
  for _ in $(seq 1 $(( $3 / 2 ))); do
    got="$(keys "$1")"
    [ "${got:-0}" = "$2" ] && return 0
    sleep 2
  done
  echo "❌ $4: ключей $1 = ${got:-?}, ожидалось $2 за $3 c"; return 1
}

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? поднимите стенд: checks/00-up.sh)"; exit 1; }
api() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$BASE$1"; }

# ===== 1) PgWorker (deploy-контейнеры) =====
echo ">>> pgworker: оба инстанса живы (Arrange)"
$MTLS https://localhost:${PGW_PORT1}/healthz >/dev/null || { echo "❌ pgworker-1 не жив (:${PGW_PORT1}/healthz mTLS; стенд поднят?)"; exit 1; }
$MTLS https://localhost:${PGW_PORT2}/healthz >/dev/null || { echo "❌ pgworker-2 не жив (:${PGW_PORT2}/healthz mTLS; 00-up.sh поднял оба?)"; exit 1; }
wait_keys /pgworker/api/ 2 30 "Arrange pgworker"

echo ">>> pgworker: docker stop первого ($PGW1)"
docker stop "$PGW1" >/dev/null
# ключи первого гаснут ≤ lease TTL 15 c — остаётся ровно один (выживший)
wait_keys /pgworker/api/ 1 60 "takeover pgworker: ключ первого погас"
survivor="$(ect get /pgworker/api/ --prefix --keys-only 2>/dev/null | head -1 | awk -F/ '{print $NF}')"
[ -n "$survivor" ] || { echo "❌ не найден выживший инстанс (/pgworker/api/)"; exit 1; }
$MTLS https://localhost:${PGW_PORT2}/healthz >/dev/null \
  || { echo "❌ выживший pgworker-2 не жив (:${PGW_PORT2}/healthz)"; exit 1; }
[ "$(keys /pgworker/instances/)" = "1" ] || { echo "❌ /pgworker/instances/ не сократился до 1"; exit 1; }
# клэйм живого кластера стенда (demo) — у выжившего; кластера нет → пропуск
claim="$(ect get /pgworker/claims/demo --print-value-only 2>/dev/null || true)"
if [ -n "$claim" ]; then
  echo "$claim" | jq -e --arg i "$survivor" '.instance == $i' >/dev/null \
    || { echo "❌ клэйм demo держит не выживший: $claim (survivor=$survivor)"; exit 1; }
  echo "  клэйм demo у выжившего инстанса ${survivor:0:8}…"
else
  echo "  (кластера demo нет — claims-ассерт пропущен)"
fi
# панель жива и видит инстансы (не 503; карточка pgworker деградировала до 1)
code="$(api /api/workers)"
[ "$code" = 200 ] || { echo "❌ /api/workers = $code при живом втором инстансе (ожидался 200)"; exit 1; }
echo "  надзор у второго: ключи погасли ≤60 c, healthz :${PGW_PORT2} жив, панель 200"

echo ">>> pgworker: docker start первого (Restore)"
docker start "$PGW1" >/dev/null
wait_keys /pgworker/api/ 2 60 "restore pgworker"
$MTLS https://localhost:${PGW_PORT1}/healthz >/dev/null \
  || { echo "❌ первый pgworker не ожил после start (:${PGW_PORT1}/healthz)"; exit 1; }
echo "  ✓ pgworker: оба инстанса снова живы"

# ===== 2) KafkaWorker (стендовые as-*) =====
echo ">>> kafkaworker: оба инстанса живы (Arrange)"
[ "$(keys /kafkaworker/api/)" = "2" ] || { echo "❌ /kafkaworker/api/ != 2 ключа (профиль kafka поднят?)"; exit 1; }
docker stop as-kafkaworker >/dev/null
wait_keys /kafkaworker/api/ 1 60 "takeover kafkaworker"
docker exec as-kafkaworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ выживший as-kafkaworker-2 не жив (/healthz mTLS изнутри)"; exit 1; }
echo "  надзор у as-kafkaworker-2: ключ первого погас ≤60 c, healthz жив"
docker start as-kafkaworker >/dev/null
wait_keys /kafkaworker/api/ 2 60 "restore kafkaworker"
echo "  ✓ kafkaworker: оба инстанса снова живы"

# ===== 3) ValkeyWorker (стендовые as-*) =====
echo ">>> valkeyworker: оба инстанса живы (Arrange)"
[ "$(keys /valkeyworker/api/)" = "2" ] || { echo "❌ /valkeyworker/api/ != 2 ключа (профиль valkey поднят?)"; exit 1; }
docker stop as-valkeyworker >/dev/null
wait_keys /valkeyworker/api/ 1 60 "takeover valkeyworker"
docker exec as-valkeyworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ выживший as-valkeyworker-2 не жив (/healthz mTLS изнутри)"; exit 1; }
echo "  надзор у as-valkeyworker-2: ключ первого погас ≤60 c, healthz жив"
docker start as-valkeyworker >/dev/null
wait_keys /valkeyworker/api/ 2 60 "restore valkeyworker"
echo "  ✓ valkeyworker: оба инстанса снова живы"

echo "✓ 35-worker-second-instance: kill→takeover всех трёх воркеров зелёный (стенд в исходном состоянии)"
```

- [ ] **Step 5.2: Сделать исполняемым + bash -n**

```bash
chmod +x dev-stand/adminpanel/checks/35-worker-second-instance.sh
bash -n dev-stand/adminpanel/checks/35-worker-second-instance.sh && echo OK
```

Ожидание: `OK`.

- [ ] **Step 5.3: Полный прогон стенда (чистый)**

Перед прогоном: `dev-stand/images/pull-images.sh` (канон runbook). Затем:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance/dev-stand/adminpanel
./checks/90-down.sh -v          # чистый старт (если был поднят)
./checks/00-up.sh
```

Ожидание: `✓ стенд поднят …` без ❌; в выводе: `pgworker жив ×2`, `kafkaworker жив ×2`, `valkeyworker жив ×2` (AC3-подъём, AC1-часть).

- [ ] **Step 5.4: Проверить AC1 (деплой: 6 контейнеров, healthz, по 2 ключа)**

```bash
docker ps --format '{{.Names}}' | grep -E 'deploy-pgworker|as-kafkaworker|as-valkeyworker' | sort
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 get /pgworker/api/ --prefix --keys-only | grep -c .
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 get /kafkaworker/api/ --prefix --keys-only | grep -c .
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 get /valkeyworker/api/ --prefix --keys-only | grep -c .
```

Ожидание: 6 имён (`deploy-pgworker-1`, `deploy-pgworker-2-1`, `as-kafkaworker`, `as-kafkaworker-2`, `as-valkeyworker`, `as-valkeyworker-2`); каждый count = 2.

- [ ] **Step 5.5: Проверить AC2 (панель: по 2 инстанса, Healthy) и AC6 (Prometheus)**

```bash
JAR=$(mktemp)
curl -fsS -c "$JAR" -o /dev/null -X POST http://localhost:5050/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'
curl -fsS -b "$JAR" http://localhost:5050/api/workers | jq -c '[.workers[] | {worker, instances: (.instances | length)}]'
curl -fsS "http://localhost:9090/api/v1/query" --data-urlencode 'query=up{job=~"pgworker|kafkaworker|valkeyworker"}' | jq -c '[.data.result[] | {job: .metric.job, instance: .metric.instance, up: .value[1]}]'
rm -f "$JAR"
```

Ожидание: jq выводит ТРИ карточки доменов — pgworker, kafkaworker, valkeyworker (панель отдаёт valkeyworker безусловно, `GetWorkersQueryHandler`) — и у КАЖДОЙ `instances:2`; Prometheus: 6 серий `up`, все `up:1`, instance-лейблы различимы (`host.docker.internal:8080`/`:8083`, `kafkaworker:8080`/`kafkaworker-2:8080`, `valkeyworker:8080`/`valkeyworker-2:8080`). Статусы Healthy на грани «Воркеры» панель считает по healthz-поллеру — 2 инстанса видны через `/api/workers`; полная визуальная проверка — открыть `http://localhost:5050` (manual). (Ассерт `.workers | length == 2` чека 70 устарел относительно кода панели — три домена; вне scope t07, см. 4.7.)

- [ ] **Step 5.6: Прогнать чек 35 (AC4)**

```bash
./checks/35-worker-second-instance.sh && ./checks/35-worker-second-instance.sh
```

Ожидание: два подряд зелёных прогона (идемпотентность): `✓ 35-worker-second-instance: kill→takeover всех трёх воркеров зелёный`.

- [ ] **Step 5.7: Прогнать правленные чеки (регресс 20/50/65) и соседние**

```bash
./checks/20-alerts.sh
./checks/50-kafka-api.sh
./checks/65-metrics.sh
```

Ожидание: все `✓`/`✅` без ❌ (50-й заканчивается «kafkaworker остановлен… kafka-грань алерта видна» — оба инстанса остановлены; 65-й шаг 0 поднимает обоих обратно).

- [ ] **Step 5.8: Проверить AC3-часть (90-down сносит всё) и зачистить серию**

```bash
./checks/90-down.sh
docker ps -a --format '{{.Names}}' | grep -E '^as-|^deploy-pgworker' || echo "чисто"
docker network ls --format '{{.Name}}' | grep -c 'adminpanel-stand\|kfw-net' || true
```

Ожидание: после 90-down не осталось `as-*`-контейнеров стендовых профилей (deploy-pgworker* остаются — осознанно, другой compose-проект; если нужно полное уничтожение — вручную `cd ../../deploy && docker compose down`); осиротевших сетей нет. КРИТИЧНО (AGENTS.md): это страховочный гейт чистоты между сериями — дожидаться финальной строки каждой серии, следующую серию запускать только после зачистки.

- [ ] **Step 5.9: Commit**

```bash
git add dev-stand/adminpanel/checks/35-worker-second-instance.sh
git commit -m "t07: чек 35-worker-second-instance — kill→takeover на стенде (pgw/kfw/vwk: ключи гаснут ≤60 c, клэйм у выжившего, restore, идемпотентен)"
```

---

### Task 6: docker-E2E — второй инстанс контейнером (AC5)

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs`

**Interfaces:**
- Consumes (тестовая инфраструктура, без правок): `E2eEnvironment.StartAsync(slug, withMinio:false)` (своя сеть+etcd, `EtcdEndpoint`, `Gateway`, `RunDockerAsync`, `ClusterTag`, `MarkFailed`, `CollectDiagnosticsAsync`, DisposeAsync-телеметрия), `E2eFixture.WaitForAsync/FreePort`, `DockerTrait.SkipIfUnavailable` (гейт `PGW_TEST_DOCKER=1`), `E2eTestPki.GenerateCa/Issue` (свой PKI сценария), образ `pgworker-node:e2e` (собирает фикстура).
- Produces: тест `SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates` (маркер мерж-гейта `--filter FullyQualifiedName~SecondInstance`).

**Решения (по spec §4.5):** инстансы — `docker run -d --restart no` с динамическими хост-портами (зонд `FreePort`), PEM-дуализм mTLS (`PGW_API_TLS_CERT/KEY/CLIENT_CA` значениями — поддержано воркером, volume не нужен), свой PKI сценария (CA+server+client через `E2eTestPki`; клиент валидирует только доверие CA — SAN не критичен, но выпускаем с SAN localhost/127.0.0.1/host.docker.internal по канону); снапшоты — ephemeral ФС контейнера (тома не монтируем НИ у одного инстанса — тест симметричен: оба «без тома», лидерство не мигрирует между разными носителями); healthz-готовность — по lease-ключам `/pgworker/api/` (надёжнее TCP-пробы свежего docker-порта).

- [ ] **Step 6.1: Создать E2eSecondInstanceScenarios.cs целиком**

Файл `src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs` (новый, полный текст):

```csharp
using System.Diagnostics;
using System.Text.Json;
using PgWorker.IntegrationTests.Docker;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// Docker-E2E второго инстанса (t07 spec §4.5): инстансы PgWorker — КОНТЕЙНЕРЫ
// образа pgworker:e2e-<tag> (деплой-слой), docker kill первого посреди A3
// add-shard — выживший контейнер доносит шард (takeover по lease-клэймам,
// БЕЗ правок кода воркера). Каноны docs/e2e-isolation.md / docs/e2e-launch.md:
// окружение E2eEnvironment (свой etcd/сеть на прогон), guid-имена всех
// объектов, полный teardown при любом исходе + ассерт чистоты; телеметрия в
// /tmp/pgw-e2e-artifacts-<guid>/ (MarkFailed — стоп без удаления).
public class E2eSecondInstanceScenarios
{
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("second-instance", ct: ct);
        Fx = fx;
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);

        // Идентификаторы прогона: имена контейнеров/тег образа содержат тег
        // кластера — teardown окружения опознаёт их как СВОИ (OwnName) и
        // удаляет; телеметрия снимает их логи до удаления.
        var tag = Fx.ClusterTag;
        var cluster = $"si2w{tag}";
        var image = $"pgworker:e2e-{tag}";
        var w1 = $"pgw-ew1-{tag}";
        var w2 = $"pgw-ew2-{tag}";

        // PKI сценария (PECULIARITY: статический пакет фикстуры приватен —
        // свой CA/серты снимают зависимость; клиент доверяет только своей CA).
        var ca = E2eTestPki.GenerateCa("si2");
        var (serverCert, serverKey) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "pgworker", ["localhost", "127.0.0.1", "host.docker.internal"], ip: null);
        var (clientPem, clientKeyPem) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "si2-healthcheck", ["si2-healthcheck"], ip: null);

        try
        {
            // ---------- Arrange: образ + два инстанса-контейнера ----------
            // Образ воркера: сборка в сценарии, тег с идентификатором прогона
            // (кэш слоёв делает повторные прогоны быстрыми; свежий код всегда).
            var buildSw = Stopwatch.StartNew();
            await Fx.RunDockerAsync(["build", "-q", "-f", $"{root}/docker/PgWorker.Dockerfile", "-t", image, root], ct);
            Console.Error.WriteLine($"[PHASE] build {image}: {buildSw.Elapsed.TotalSeconds:F0} c");

            var p1 = E2eFixture.FreePort();
            var p2 = E2eFixture.FreePort();
            await RunWorkerContainerAsync(w1, image, p1, serverCert, serverKey, ca, ct);
            await RunWorkerContainerAsync(w2, image, p2, serverCert, serverKey, ca, ct);

            // Готовность ОБОИХ: по 2 lease-ключа api/instances (start-бюджет
            // <=100 c; ключ жив = процесс поднялся, etcd-keepalive тикает).
            var bothUp = await E2eFixture.WaitForAsync(async () =>
                (await RangeAsync("/pgworker/api/")).Count == 2
                && (await RangeAsync("/pgworker/instances/")).Count == 2,
                TimeSpan.FromSeconds(100), ct);
            bothUp.Should().BeTrue("оба контейнерных инстанса обязаны опубликовать дискавери-ключи за 100 c");

            // ---------- Arrange: живой кластер + add-декларация shard3 ----------
            await SeedClusterAsync(cluster);
            var provisioned = await E2eFixture.WaitForAsync(
                () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning кластера должен дойти до Active до старта add");

            await SeedAddDeclarationAsync(cluster, "shard3", ct);
            var a3Started = await E2eFixture.WaitForAsync(
                () => DockerHasAsync($"pgw-{cluster}-shard3-"), TimeSpan.FromSeconds(120), ct);
            a3Started.Should().BeTrue("первый инстанс должен начать A3 (появился контейнер shard3)");

            // ---------- Act: docker kill ПЕРВОГО контейнера воркера ----------
            // --restart no: не воскреснет сам — доносит ТОЛЬКО выживший.
            await Fx.RunDockerAsync(["kill", w1], ct);

            // ---------- Assert: выживший донёс, дублей нет, клэйм у него ----------
            var finished = await E2eFixture.WaitForAsync(
                () => ShardRegisteredAsync(cluster, "shard3"), TimeSpan.FromSeconds(360), ct);
            finished.Should().BeTrue(
                $"выживший контейнер должен донести shard3 после takeover; work={await WorkDumpAsync(cluster, ct)}");

            var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-", all: true);
            containers.Should().HaveCount(2, "контейнеров нового шарда ровно 2 (нет дублей после takeover)");

            var apiKeys = await RangeAsync("/pgworker/api/");
            apiKeys.Should().HaveCount(1, "после kill первого жив ровно один дискавери-ключ (lease погас)");
            (await RangeAsync("/pgworker/instances/")).Should().HaveCount(1, "instance-ключ первого погас вместе с lease");
            var survivor = apiKeys[0].Key.Split('/')[^1];

            var claim = (await GetOrNullAsync($"/pgworker/claims/{cluster}"))?.Value;
            claim.Should().NotBeNull("клэйм живого кластера обязан существовать после takeover");
            using (var doc = JsonDocument.Parse(claim!))
                doc.RootElement.GetProperty("instance").GetString().Should().Be(survivor,
                    "клэйм кластера держит выживший инстанс — надзор мигрировал вторым (work-журнал: фазы доигрывает instance-ид из клэйма)");

            // ---------- Teardown-подготовка (успех): свои контейнеры и образ ----------
            await Fx.RunDockerAsync(["rm", "-f", w1, w2], ct);
            await Fx.RunDockerAsync(["image", "rm", "-f", image], ct);
        }
        catch
        {
            // Телеметрия e2e-launch: упавший сценарий помечаем — teardown
            // ОСТАНОВИТ контейнеры (не удалит: разбор по живым объектам).
            fx.MarkFailed();
            throw;
        }
    }

    // Запуск инстанса-контейнера PgWorker: динамический хост-порт, docker.sock
    // (воркер управляет нодами), extra_hosts host-gateway (etcd/ноды через
    // host.docker.internal), mTLS PEM-значениями (без volume), уникальный
    // AdvertiseUrl; env-набор — как у хост-инстансов фикстуры (быстрые тики).
    private async Task RunWorkerContainerAsync(
        string name, string image, int hostPort,
        string serverCert, string serverKey, (string CaPem, string CaKeyPem) ca,
        CancellationToken ct)
    {
        var etcdHost = Endpoint.Replace("localhost:", "host.docker.internal:", StringComparison.Ordinal);
        var env = new Dictionary<string, string>
        {
            // Секреты установки (Д7) — как у хост-инстансов фикстуры.
            ["PGW_PG_SUPERUSER_PASSWORD"] = E2eFixture.SuPassword,
            ["PGW_PG_STANDBY_PASSWORD"] = E2eFixture.StandbyPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = E2eFixture.BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = E2eFixture.MoverPassword,

            // etcd окружения: из контейнера — публикацией на хост; Advertised —
            // для Patroni-нод (те ходят из своих контейнеров).
            ["PgWorker__Etcd__Endpoints__0"] = etcdHost,
            ["PgWorker__Etcd__AdvertisedEndpoints__0"] = etcdHost,
            ["PgWorker__Docker__Mode"] = "Plain",
            ["PgWorker__Docker__Hosts__0__Name"] = "host.docker.internal",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            ["PgWorker__Docker__PortRange__From"] = "15100",
            ["PgWorker__Docker__PortRange__To"] = "15200",
            ["PgWorker__Docker__Images__Node"] = E2eEnvironment.NodeImage,
            ["PgWorker__Docker__EnableDoorman"] = "false",

            // Ускоренные циклы/пороги e2e (ассерты ждут секунды, не минуты).
            ["PgWorker__Loops__ScanIntervalSec"] = "1",
            ["PgWorker__Loops__KeepaliveSec"] = "1",
            ["PgWorker__Loops__ErrorDelayMs"] = "500",
            ["PgWorker__Loops__SnapshotIntervalMin"] = "360",
            ["PgWorker__Thresholds__NodeDeadSec"] = "6",
            ["PgWorker__Thresholds__ShardDeadSec"] = "5",
            ["PgWorker__Thresholds__PatroniBootSec"] = "300",
            ["PgWorker__Parallelism__MaxClusters"] = "2",
            ["PgWorker__Snapshots__Dir"] = "/snapshots",
            ["PgWorker__Snapshots__RetentionFiles"] = "10",

            // mTLS API (t03): PEM-дуализм env освобождает от volume; уникальный
            // AdvertiseUrl per инстанс (t07: одинаковые URL гасят оба ключа).
            ["PGW_API_TLS_CERT"] = serverCert,
            ["PGW_API_TLS_KEY"] = serverKey,
            ["PGW_API_TLS_CLIENT_CA"] = ca.CaPem,
            ["PgWorker__Api__Tls__AllowInsecureHttp"] = "false",
            ["PgWorker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{hostPort}",

            ["ASPNETCORE_URLS"] = "https://+:8080",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        var args = new List<string>
        {
            "run", "-d", "--name", name, "--restart", "no",
            "-p", $"{hostPort}:8080",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "--add-host", "local:host-gateway",
            "--add-host", "host.docker.internal:host-gateway",
        };
        args.AddRange(env.Select(p => new[] { "-e", $"{p.Key}={p.Value}" }).SelectMany(x => x));
        args.Add(image);
        await Fx.RunDockerAsync([.. args], ct);
    }

    // ===== Хелперы (приёмы E2eScaleScenarios, scoped на кластер) =====

    private async Task<Shared.Etcd.Client.Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

    // Кластер Active: config без state, dsn всех нод, ноды RUNNING, статусов нет.
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

    // Шард поднят и зарегистрирован: dsn записан, все ноды RUNNING.
    private async Task<bool> ShardRegisteredAsync(string cluster, string shard)
    {
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
            return false;
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return true;
    }

    // Сид кластера в стиле панели (копия E2eScaleScenarios.SeedClusterAsync).
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

    // Add-декларация в стиле панели: replicas + nodes + request_*, БЕЗ dsn.
    private async Task SeedAddDeclarationAsync(string cluster, string shard, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }

    private async Task<bool> DockerHasAsync(string prefix)
        => (await ListContainerNamesAsync(prefix)).Count > 0;

    private async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await Fx.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}
```

Сигнатуры сверены с кодом (изменений не требуется): `E2eTestPki.GenerateCa(string)` → `(string CaPem, string CaKeyPem)`; `E2eTestPki.Issue(string caCertPem, string caKeyPem, string commonName, IReadOnlyList<string> dnsNames, IPAddress? ip)` → `(string CertPem, string KeyPem)` (`src/tests/PgWorker.IntegrationTests/E2e/E2eTestPki.cs`); `EtcdGateway.RangeAsync/GetAsync/PutAsync` и `Shared.Etcd.Client.Kv` — как в соседних сценариях; FluentAssertions — глобальный using тестового проекта (`PgWorker.IntegrationTests.csproj`).

- [ ] **Step 6.2: Сборка тестов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --nologo
```

Ожидание: сборка без ошибок и warnings (`TreatWarningsAsErrors=true`).

- [ ] **Step 6.3: Прогон сценария (маркер мерж-гейта)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~SecondInstance --no-build
```

Ожидание: PASS. Первый прогон включает сборку образа `pgworker:e2e-<tag>` (docker build с `dotnet publish` внутри — минуты; строка `[PHASE] build …` в выводе — правило медленных фаз). Ассерты сценария: bothUp ≤100 c → provisioning ≤360 c → kill → донос ≤360 c → ровно 2 контейнера шарда → 1 ключ api → клэйм = survivor. Время всего теста может превысить 5 мин (provisioning-бюджеты) — это ожидаемые фазы WaitForAsync с `[PHASE]`-логами, НЕ зависание: логи контейнеров не анализировать онлайн, пока фазы вкладываются в бюджеты.

- [ ] **Step 6.4: Зачистка после серии (обязательный гейт)**

```bash
docker ps -a --format '{{.Names}}' | grep -E 'pgw-ew|si2w' || echo "воркеров сценария нет"
docker images --format '{{.Repository}}:{{.Tag}}' | grep 'pgworker:e2e-' || echo "образ сценария удалён"
docker network ls --format '{{.Name}}' | grep -E 'pgw-en-|kfw-net' || echo "сетей нет"
```

Ожидание: teardown сценария уже всё удалил (DisposeAsync + ассерт чистоты зелёный в логе теста; команды выше — подтверждение). Если фильтр что-то нашёл — НЕ перезапускать тест: разобрать по `/tmp/pgw-e2e-artifacts-<guid>/` (README-cleanup.txt при провале), зачистить руками, повторный прогон — только после анализа.

- [ ] **Step 6.5: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs
git commit -m "t07: docker-E2E SecondInstance — kill контейнера воркера посреди A3, выживший инстанс доносит шард (контейнерные инстансы, PEM-mTLS, полная самоочистка)"
```

---

### Task 7: Доки + roadmap-гейт (AC7)

**Files:**
- Modify: `docs/runbook.md` (раздел «Вторые инстансы воркеров»)
- Modify: `arch/roadmap/reliability.md` (снять пункт `t07-worker-second-instance`)
- Modify: `arch/roadmap/reliability-report.md` (строку t07 — в «Сделано»; сводка R; таблица «Деплой-уровень»)

**Interfaces:**
- Consumes: задачи 2–6 (реализованные порты/чеки/E2E; фактический merge-хэш неизвестен до мержа — в «Сделано» пишется «(мерж-коммит t07)» по прецеденту t06).
- Produces: AC7; roadmap-гейт трека (правка едет в ветке и попадает в main мерж-коммитом закрытия — правило reliability.md).

- [ ] **Step 7.1: docs/runbook.md — раздел про вторые инстансы**

В `docs/runbook.md` после вводного списка (после строки 11, перед `## PGTune-параметры…`) добавить раздел:

```
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
```

- [ ] **Step 7.2: arch/roadmap/reliability.md — снять пункт t07**

Удалить пункт (строки 41–45):

```
- **`t07-worker-second-instance`** — второй инстанс воркеров в деплое
  (`deploy/docker-compose.yml`): код полностью готов (lease-клэймы, takeover
  ≤15 с, PortAllocLock — доказано E2E AC3/mid-add/mid-move), но деплой
  поднимает по одному контейнеру — SPOF надзора (датаплейн при этом живёт
  сам).
```

Проверить: других упоминаний `t07-worker-second-instance` в `arch/roadmap/*.md` нет (`grep -rn "t07-worker-second-instance" arch/roadmap/` — пусто; зависимости `← t07` у других пунктов не было).

- [ ] **Step 7.3: arch/roadmap/reliability-report.md — три правки**

1) Сводка «R — быстрая самовосстанавливаемость», абзац «Открытые разрывы» (строки 57–61): заменить

```
Открытые разрывы: деплой воркеров и etcd одиночный — SPOF надзора
(`t07`, `t09`); снапшоты etcd не покидают хост (`t08`); зависшие
ротационные заявки без таймаутов (`t10`); Swarm без ускоренного failover
(`t11`); нет watchdog зависших циклов (`t12`); сценарии отказа хоста/DC
не отработаны (`t20`, `t21`).
```

на

```
Открытые разрывы: etcd одиночный — SPOF контроль-плейна (`t09`; воркеры
с t07 деплоятся по два инстанса — SPOF надзора закрыт); снапшоты etcd не
покидают хост (`t08`); зависшие ротационные заявки без таймаутов (`t10`);
Swarm без ускоренного failover (`t11`); нет watchdog зависших циклов
(`t12`); сценарии отказа хоста/DC не отработаны (`t20`, `t21`).
```

2) Таблица «Осталось»: удалить строку

```
| `t07-worker-second-instance` | второй инстанс воркеров в деплое | P2 | R |
```

3) Таблица «Сделано в рамках трека»: добавить строку (первой, свежайшая сверху — прецедент t04/t06 внизу, ставим по возрастанию после t06 — фактический порядок в таблице хронологический, добавляем ПОСЛЕ строки `t06-sync-strict-option`):

```
| `t07-worker-second-instance` | — (мерж-коммит t07) | деплой и dev-стенд поднимают по два инстанса каждого воркера (8083/8084/8085 + стендовые as-*-2, общие тома, advertise per инстанс): смерть контейнера — клэймы мигрируют второму ≤15 с, надзор не прерывается; приёмка — чек 35 kill→takeover + docker-E2E SecondInstance (контейнерные инстансы, kill посреди A3) |
```

4) Таблица «База: что уже сделано», строка «Деплой-уровень» (строка 89): заменить

```
| Деплой-уровень | слабая | воркеры по одному контейнеру, etcd один endpoint (код мультинстанса готов) |
```

на

```
| Деплой-уровень | средне-высокая | воркеры по два инстанса (t07: клэймы мигрируют ≤15 с, надзор без SPOF); etcd один endpoint (`t09`) |
```

- [ ] **Step 7.4: Проверить согласованность трека**

```bash
grep -rn "t07-worker-second-instance" /Users/demakaev/ZCodeProject/worktrees/t07-worker-second-instance/arch/roadmap/
```

Ожидание: единственное совпадение — строка в таблице «Сделано» reliability-report.md (пункт из reliability.md и «Осталось» сняты; рассинхрона «задача закрыта / отчёт нет» нет).

- [ ] **Step 7.5: Commit**

```bash
git add docs/runbook.md arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "docs: runbook — вторые инстансы воркеров (t07); roadmap-гейт — t07-worker-second-instance снят, reliability-report: сводка R, Деплой-уровень, строка в «Сделано»"
```

---

## Итоговая приёмка (прогон мерж-гейта — после всех задач)

- [ ] **AC1–AC4, AC6**: повторить шаги 5.3–5.8 на чистом стенде (90-down -v → 00-up → проверки → 35 → 20/50/65 → 90-down).
- [ ] **AC5**: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~SecondInstance` — зелёный; зачистка серии (6.4).
- [ ] **AC7**: задача 7 закоммичена; `grep -rn "t07-worker-second-instance" arch/roadmap/` — только «Сделано».
- [ ] Код воркеров/панели/Shared не менялся: `git diff --stat main...t07-worker-second-instance -- src/ | grep -v 'src/tests/' || echo "src чист (кроме тестов)"`.

Дальше — код-ревью и мерж по явной просьбе пользователя (roadmap-правки едут в main мерж-коммитом закрытия — уже в ветке).

## Self-review плана (выполнен автором)

- **Покрытие spec §4:** §4.1 → Task 1; §4.2 → Task 2; §4.3 → Tasks 3–4 (compose/prometheus/00-up/90-down-аудит/аудит чеков); §4.4 → Task 5; §4.5 → Task 6; §4.6 → Task 7. AC1→5.4, AC2→5.5, AC3→5.3/5.8, AC4→5.6, AC5→6.3, AC6→5.5, AC7→Task 7.
- **Отступления/уточнения от буквы spec (технически вынужденные, деплой-слой):**
  1. `deploy/tls/gen.sh` SAN `DNS:kafkaworker-2,DNS:valkeyworker-2` — spec §4.3 не называет файл, но без SAN Prometheus не скрейпит стендовые `-2` (AC6): Prometheus проверяет имя таргета, панель — нет (проверено по `WorkerTlsHandler.cs`).
  2. Аудит чеков (spec §4.3: «список — в plan-фазе») — покрыты ВСЕ чеки каталога: правим 3 (20/50/65), аудит 15 «без правок» (90-down — шаг 4.5, остальные 14 — таблица шага 4.7). Правки по критерию «семантически противоречит двум инстансам»: 20-alerts — ТРИ точки (arrange-пауза репарации: репарация идёт ТОЛЬКО у держателя клэйма `MoveRepairProcess.cs:32` — живой второй снимает «тишину», гонка Assert 2 `wait_alert move-stale` 15 c; шаг 6: нуль живых инстансов для 503/worker-api-unreachable), 50-kafka-api — нуль живых ключей kfw, 65-metrics — «все таргеты up» требует живых `-2`.
  3. Work-журнал PgWorker не содержит instance-id (проверено: `ClaimPayload` живёт в `/pgworker/claims/<C>`); ассерт spec §4.5 п.5 «продолжение фаз вторым instance-id» реализован клэймом: `.instance == <survivor-id>` + физическая невозможность продолжения убитым (`--restart no`).
  4. E2E использует собственный PKI сценария (`E2eTestPki`), а не приватный статический пакет фикстуры — нулевые правки `E2eEnvironment`.
  5. `/api/workers` панели отдаёт ТРИ карточки доменов (pg/kfw/vwk — безусловно, `GetWorkersQueryHandler`, подтверждено `ValkeyApiTests.cs:203`); число доменов НЕ зависит от инстансов. Известное вне scope t07: ассерт `.workers | length == 2` чека 70 устарел относительно кода панели — чек 70 в приёмку (задача 5) не входит; фикс — отдельной задачей.
- **Ревью Фазы 4 (CHANGES_REQUESTED) внесено (ревизия 2):** (1) 20-alerts arrange-пауза обоих инстансов (4.6 п.1a/1b — `docker stop/start -t 3 deploy-pgworker-1 deploy-pgworker-2-1`); (2) таблица 4.7 дополнена 45-backups-storage и 60-move-ops; (3) ожидание аудита 90-down переформулировано по фактическому grep (единственное совпадение — строка 5, комментарий; `rm vwk-demo-node1` в строке 10 паттерна не содержит); (4) основание по 70-му и ожидание 5.5 — три карточки доменов; (5) 4.1 — «строки 10–16».
- **Плейсхолдеры:** отсутствуют — все шаги содержат полный код/команды/критерии.
- **Консистентность имён:** `PGW_API_HOST_PORT2`/`KFW_API_HOST_PORT2`/`VWK_API_HOST_PORT2`, `pgworker-2`/`kafkaworker-2`/`valkeyworker-2`, `as-kafkaworker-2`/`as-valkeyworker-2`, `deploy-pgworker-1`/`deploy-pgworker-2-1`, `35-worker-second-instance.sh`, `SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates` — едины по всему плану.
