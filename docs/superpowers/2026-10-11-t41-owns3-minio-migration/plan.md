# t41-owns3-minio-migration: перевод подсистемы бэкапов на ownS3 + канонический demo-контур — план (v5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Подсистема бэкапов и панельная грань «Хранилище бэкапов» работают против ownS3; весь стенд — ОДНА стендовая сеть со всеми контейнерами (etcd, панель, воркеры, ноды demo, wal-агенты, джобы, owns3, прометей); demo-кластер — канонический (движок, Spilo+Patroni, `POST /api/clusters`); сетевой режим контура PgWorker — опционален (`PGW_CONTOUR_NETWORK`, пусто = прод-канон без изменений).

**Architecture:** arch-first: каноны (arch/14, arch/19, owns3/05, adminpanel/04) → код PgWorker с TDD (режим сети контура: единая точка `PgWorkerOptions.BackupsRuntime()` читает `Docker.ContourNetwork`; драйвер/спеки джобов/адрес источника; юнит-таблицы двух режимов) → стенд (перестройка demo, демонтаж усыновления, publish-COPY wal-образа, deploy-compose контур-override без второй сети) → стендовая приёмка (инспект ровно-одной-сети, живые полные verify OK, дрилл) → операционная mc-mirror-миграция (с тестовым прогоном в ранбуке) → docker-E2E PgWorker в мерж-гейте. Панель (фолбэк health 404/403) — исполнена и зелёная, не трогается.

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions, docker compose (+ `!reset`-override), bash-чеки стенда, `mc`, Spilo-образ `ghcr.io/zalando/spilo-18:4.1-p2` (локальный registry).

**Spec:** `docs/superpowers/2026-10-11-t41-owns3-minio-migration/spec.md` (v5) — план аргументируется от спеки; исполнители читают обе.

**Статусы исполнения:** у каждой задачи строка «Статус». Части v2-плана исполнены (13 коммитов: каноны v2-уровня, панель 404/403 + тесты 6/6 зелёные, env/compose/чеки/README/runbook v2-модели). Подлежат доработке по v5: каноны (новая сетевая буква), стендовые файлы, deploy-compose. НЕ ТРОГАТЬ: код панели (`MinioS3Client` + `MinioS3ClientHealthTests`). Первые открытые шаги — T1 (arch/14).

**Ключевые технические решения плана (фиксируются здесь):**
1. **Контур-режим deploy-compose — override-файл** `deploy/docker-compose.contour.yml`: сеть `contour: {name: ${PGW_CONTOUR_NETWORK:?…}, external: true}`; у сервисов сбрасываются ОБЕ сети базового файла (`default: !reset null` у всех; `pgw-metrics: !reset null` у `pgworker`/`pgworker-2`) и подключается одна `contour` (aliases: pgworker×2 — скрейп, owns3 — DNS-имя); top-level объявление `pgw-metrics: !reset null` — сеть на стенде не создаётся вовсе. Прод: базовый файл без изменений.
2. **Стендовая сеть** — default-сеть стендового компоуза с явным именем `adminpanel-stand-net` (`networks: {default: {name: adminpanel-stand-net}}`); `PGW_CONTOUR_NETWORK=adminpanel-stand-net`; `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK` на стенде — то же имя (sync-блок 00-up.sh несёт этот дефолт), отдельной `pgw-metrics` на стенде нет.
3. **Порядок 00-up.sh меняется**: стендовый compose поднимается ДО deploy-контура (владелец сети контура — стендовый compose; deploy подключается external).
4. **Bucket-сид и чек 45 ходят СНАРУЖИ** через хост-публикацию `:9010` (ephemeral mc + host-gateway, прецедент чека 46) — хост-переходы только наружу (чеки/миграция/оператор).
5. **`extra_hosts: host.docker.internal:host-gateway` у панели СОХРАНЯЕТСЯ**: панель резолвит advertised-имя docker-хоста (SQL/Patroni-пробы нод по portalloc `host.docker.internal:15xxx/18xxx`) — это резолв публичного advertised-адреса, а не хост-переход к сервису контура (ownS3 панель адресует по имени `owns3:9000`). Аналогично extra_hosts остаются у kafkaworker/valkeyworker deploy-компоуза (их `local`-резолв Engine API).
6. **Единая точка сборки `BackupsRuntimeOptions`**: метод `PgWorkerOptions.BackupsRuntime()` на корне опций (читает `Backups` + `Docker.ContourNetwork`); ВСЕ 12 точек `…Backups.ToRuntime()` в Program.cs (строки 223, 230, 341, 599, 618, 657, 676, 695, 712, 730, 745, 869) заменяются на `…BackupsRuntime()` — иначе контур-режим не доехал бы до спек джобов/агентов/DSN (процессы-потребители получают опции именно через эти точки).
7. **Канонические имена demo**: кластер `demo`, шарды `shard1`/`shard2`, ноды `shard1a/shard1b/shard2a/shard2b`; контейнеры движка `pgw-demo-shard1-shard1a` и т.д.; scope `demo-shard1`.
8. **«Integration без docker»**: docker-кейсы `PgWorker.IntegrationTests` скипаются сами (`DockerTrait.SkipIfUnavailable`, env `PGW_TEST_DOCKER≠1`) — прогон без env и есть без-docker серия.

## Global Constraints

- .NET 10, `TreatWarningsAsErrors=true`; комментарии по-русски, идентификаторы англ.; тесты AAA.
- arch-first: каноны → код → стенд; код PgWorker — TDD (красный → реализация → зелёный).
- Прод-режим сетей движка НЕ меняется: `PGW_CONTOUR_NETWORK` пуст = текущий канон arch/14; юнит-таблицы подтверждают оба режима; E2E (прод-режим) обязаны остаться зелёными.
- Никаких контейнеров в двух сетях на стенде (у КАЖДОГО контейнера стенда — ровно одна сеть); `pgw-net-demo` и `pgw-metrics` на стенде не существуют; хост-переходы внутри контура запрещены (только наружу; исключение-резолвы advertised-имени хоста — решение 5 шапки).
- Внешние образы — из локального registry `192.168.0.1:5000` (`dev-stand/images/pull-images.sh` перед стендом/docker-тестами); локально собираемые образы в registry НЕ класть; спило-образ уже зеркалирован (E2E использует).
- `dotnet test` — с `DOTNET_CLI_UI_LANGUAGE=en`; длинные команды — в фоне (`run_in_background`), ожидания ≤30 с за проверку; таймауты фикстур `BrokerBootSec`-класса ≤100 с.
- После КАЖДОЙ тестовой серии — финальная строка прогона + зачистка docker-остатков; движковые сети `pgw-net-*` между сериями — контроль `docker network ls` + страховочный prune (AGENTS.md).
- Пути — от корня worktree `$WT = /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration`.
- Документы (`arch/**`, `docs/**` кроме superpowers) — текущее/планируемое состояние без атрибуций задач.

---

## Фаза 0 — канон (arch-first)

### Task 1: arch/14-pgworker.md — режим «сеть контура задаётся поставкой»

**Статус:** не начат (канон пока описывает только per-cluster-сети).

**Вход:** ветка `feat-t41-owns3-minio-migration`, работа в `$WT`.

**Files:** Modify: `arch/14-pgworker.md` (§2.1 — сети движка; §2.4 — подсистема бэкапов/контейнеры подсистемы).

**Interfaces:** нет (канон; фиксирует имя настройки `PgWorker:Docker:ContourNetwork` / env `PGW_CONTOUR_NETWORK`).

**Связь со spec:** §5.1 строка 1 (таблица), §4.7 режим, §2 не-цель «прод без изменений».

- [ ] **Step 1.1: §2.1 — режим сети контура**

В §2.1 (сети движка, после абзаца о per-cluster `pgw-net-<C>`) добавить блок:

```markdown
- **Сеть контура (режим поставки, опционально)**: настройка
  `PgWorker:Docker:ContourNetwork` (env `PGW_CONTOUR_NETWORK`) — имя ВНЕШНЕЙ
  сети контура, принадлежащей поставке/сценарию (стенд — одна стендовая сеть
  всего контура; E2E-класс — своя изолированная сеть). Задана → ноды кластера,
  wal-агенты И ephemeral-джобы бэкапов (полный/verify/drill/restore)
  создаются в этой одной сети; `pgw-net-<C>` не создаётся и не удаляется
  движком; фиксированные `ExtraHosts`-хост-переходы спек джобов
  (`host.docker.internal`/`local`) не применяются — адресация внутри контура
  по именам сети (источник джоба полного — имя ноды `:5432`, симметрично
  `ResolveNodeRef` wal-агента; S3 — по имени сети поставки). Пусто
  (прод/умолчание) — per-cluster `pgw-net-<C>`, ephemeral-джобы в default
  bridge с extra_hosts, как выше. Сеть контура движок не создаёт и не
  удаляет; ensure-подключение идемпотентно (сеть уже существует у поставки,
  409 = успех). Не поддерживается в Mode=Swarm (fail-fast старта).
  Scrape-attach (`Docker:ScrapeNetwork`) — без изменений: на стенде указывает
  на ту же сеть контура (attach — no-op поверх).
```

- [ ] **Step 1.2: §2.4 — контейнеры подсистемы бэкапов**

В §2.4 (где описаны wal-агенты/джобы и их сети) дополнить: «В режиме сети контура (`Docker:ContourNetwork`) wal-агенты и ephemeral-джобы создаются в сети контура единой сетью с нодами кластера и S3-хранилищем поставки; `pgw-net-<C>` не используется; extra_hosts джобов не применяются».

- [ ] **Step 1.3: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -n "ContourNetwork" arch/14-pgworker.md   # 2+ вхождения
git add arch/14-pgworker.md && git commit -m "arch(t41): 14-pgworker §2.1/§2.4 — опциональный режим сети контура поставки (PGW_CONTOUR_NETWORK; прод-умолчание без изменений)"
```

**Выход:** канон движка описывает оба режима.

---

### Task 2: arch/19-backups.md — сеть контура в механиках §2/§3, адресация §7, риски §10

**Статус:** частично — вступление/§1/§10-строки исполнены в v2 (коммит `3b7fe3b3`); доработка v5: §2/§3/§7 + §10 (включая строку 903).

**Вход:** Task 1 закоммичен.

**Files:** Modify: `arch/19-backups.md` (§2, §3, §7, §10 — строка 889-блока и строка 903).

**Связь со spec:** §5.1 строка 2.

- [ ] **Step 2.1: §2 — джоб полного**

В §2 (механика полных, после описания advertised-адреса источника джоба) дополнить: «В режиме сети контура (`PgWorker:Docker:ContourNetwork`, arch/14 §2.1) джоб создаётся в сети контура (extra_hosts не применяются), адрес источника для канонических нод — имя ноды сети контура `:5432` (симметрично §3 `ResolveNodeRef`); в прод-режиме — advertised `host:pg-port` из portalloc, как выше».

- [ ] **Step 2.2: §3 — wal-агенты**

В §3 (где описана сеть агентов `pgw-net-<C>` и `ResolveNodeRef`) дополнить: «В режиме сети контура агент создаётся в сети контура (единая сеть с нодами кластера); `pgw-net-<C>` не используется; прод-режим — без изменений».

- [ ] **Step 2.3: §7 — S3-адресация**

В §7 (env-комплект `PGW_BACKUP_S3_*`) дополнить: «Стенд/контур-режим: адресация S3 всех потребителей контура — по имени сети (`http://owns3:9000` на стенде; `PGW_BACKUP_S3_ADVERTISED_ENDPOINT` не задаётся — равен endpoint); хост-публикация хранилища — только для внешних потребителей (чеки с хоста, mc-миграция, оператор). Прод — endpoint per-install (выше)».

- [ ] **Step 2.4: §10 — риск-строки (включая строку 903)**

1. Дополнить строку риска ownS3-стенда (внесённую v2): «…; стенд: адресация по имени стендовой сети (`owns3:9000`), хост-порт `:9010` — наружу; переходный as-minio до критерия `docs/owns3-migration.md`». Добавить строку: `| Сетевой режим контура ломает прод-поведение | режим строго опционален (пусто = прод-канон); юнит-таблицы режимов; docker-E2E PgWorker в мерж-гейте |`.
2. Строка 903 (v2-текст «… стенд — `host.docker.internal:9010` (extra_hosts host-gateway в джобе)») — заменить хвост на: «стенд: адресация по имени сети контура (`owns3:9000`, без хост-переходов); extra_hosts host-gateway в джобе — прод-режим».

- [ ] **Step 2.5: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -c "сети контура\|ContourNetwork" arch/19-backups.md          # >=4
grep -n "host.docker.internal:9010" arch/19-backups.md | grep -v наружу || echo "ok: 903 переведена на имя сети"
git add arch/19-backups.md && git commit -m "arch(t41): 19-backups §2/§3/§7/§10 — сеть контура в механиках джоба/агента, S3-адресация по имени сети, риски (вкл. строку недостижимости)"
```

**Выход:** arch/19 описывает оба режима механик; устаревших `:9010`-хвоста нет.

---

### Task 3: arch/22 + arch/owns3/01 + arch/owns3/05 — сетевая модель ownS3-контура

**Статус:** частично — базовые правки v2 исполнены (коммиты `dd2d0e83`, `fcfb7122`); доработка v5: сетевая модель §6 owns3/05 + упоминание адресации в 22/01.

**Вход:** Task 2 закоммичен.

**Files:** Modify: `arch/owns3/05-access-config.md` (§6), `arch/22-owns3.md` (вступление), `arch/owns3/01-overview.md` (§1).

**Связь со spec:** §5.1 строки 3–5.

- [ ] **Step 3.1: owns3/05 §6 — стендовая сетевая модель**

В §6 (стендовый пункт, переформулированный v2) заменить фрагмент про стендовый контур на:

```markdown
Стендовая сетевая модель — весь стенд ОДНА сеть (стендовый compose —
владелец): в ней живут etcd-контур, панель, воркеры deploy-проекта, ноды
demo-кластера, wal-агенты, ephemeral-джобы бэкапов, ownS3 и мониторинг;
контейнеров ownS3 в двух сетях нет. Адресация S3 всех потребителей контура —
по имени сети: `http://owns3:9000` — единый endpoint воркера/агентов/джобов/
панели (deploy-сервис подключается к стендовой сети external с alias
`owns3`); хост-публикация `${OWNS3_HOST_PORT:-9010}` — только наружу (чеки с
хоста, mc-миграция, оператор). Число инстансов ownS3 на установке — вопрос
эксплуатации (не канон этой модели); стенд — один инстанс.
```

- [ ] **Step 3.2: arch/22 + owns3/01 — адресация по имени сети**

В `arch/22-owns3.md` вступление (после фразы про MinIO-контур переходного периода) дополнить: «Стенд — адресация S3 по имени стендовой сети (`owns3:9000`, owns3/05 §6)». В `arch/owns3/01-overview.md` §1 маркер «Клиенты» дополнить тем же предложением.

- [ ] **Step 3.3: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -n "owns3:9000" arch/owns3/05-access-config.md arch/22-owns3.md arch/owns3/01-overview.md
git add arch/22-owns3.md arch/owns3/ && git commit -m "arch(t41): owns3/05 §6 — стенд: один контур одна сеть, owns3:9000 внутри / :9010 наружу; 22/01 — адресация по имени сети"
```

**Выход:** каноны ownS3 описывают сетевую модель стенда.

---

### Task 4: arch/adminpanel/01–04 — панельные каноны v5

**Статус:** частично — 01/02 исполнены в v2 ровно по букве v5 (коммит `e8d2bcdd`; сверка), `02 §2.5` уже несёт 404/403; основная доработка — `04-local-stand.md` §2.2–2.3/§3.

**Вход:** Task 3 закоммичен.

**Files:** Modify: `arch/adminpanel/04-local-stand.md` (§2.2, §2.3, §3). Verify (без правок при соответствии): `arch/adminpanel/01-architecture.md`, `02-etcd-contract.md`.

**Связь со spec:** §5.1 строки 6–8.

- [ ] **Step 4.1: сверка 01/02**

`grep -n "404/403" arch/adminpanel/02-etcd-contract.md` — формулировка на месте («404/403 → фолбэк», «ClusterOk — null вне MinIO-контура (404/403/нет ответа)»); при расхождении — довести по букве §5.1 v5. 01: «S3-инвентарь бэкапов… health-проба с фолбэком /healthz» — на месте.

- [ ] **Step 4.2: 04 §2.2 — pg-демо-контур канонический**

Раздел §2.2 (pg-демо стенда) заменить на:

```markdown
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
```

- [ ] **Step 4.3: 04 §2.3 — единая стендовая сеть**

Раздел §2.3 (раздел усыновлённых as-нод/hc-эмуляторов/HostMap — заменить целиком):

```markdown
Стенд — ОДИН контур = ОДНА стендовая сеть `adminpanel-stand-net` (владелец —
стендовый compose; deploy-проект подключается external, override
`docker-compose.contour.yml`): в ней etcd-контур, панель, воркеры, ноды demo
(`pgw-demo-*`, поднимает движок), wal-агенты, ephemeral-джобы бэкапов,
ownS3 (alias `owns3:9000` внутри, `:9010` наружу) и мониторинг; у каждого
контейнера — ровно одна сеть. Усыновлённые as-ноды/patroni-эмуляторы/HostMap
— демонтированы; панель пробирует канонические ноды по advertised-адресам
portalloc напрямую (extra_hosts панели — резолв advertised-имени
docker-хоста `host.docker.internal`, не контурный переход).
```

- [ ] **Step 4.4: 04 §3 — чеки по новой механике**

В §3 (список чеков) обновить описания: 05 — только kafka/valkey; 10 — `demo-shard1`, гварды без хардкода адресов нод (portalloc-динамичны); 20 — аномалии насыпает сам; 30 — kill мастера по leader-ключу → Patroni-promote → надзор; 40 — advertised portalloc-адреса; 45 — гвард healthz `:9010`, налив снаружи, `clusterOk == null`; 60 — гвард живого `/clusters/demo/config`; 90 — снос compose-проектов + движковых `pgw-*`-контейнеров.

- [ ] **Step 4.5: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -n "adminpanel-stand-net\|POST /api/clusters" arch/adminpanel/04-local-stand.md
git add arch/adminpanel/ && git commit -m "arch(t41): adminpanel/04 §2.2-2.3/§3 — канонический demo через POST /api/clusters, одна стендовая сеть, демонтаж усыновления, чеки"
```

**Выход:** панельные каноны синхронны v5; расхождений «проект vs arch» нет.

---

## Фаза 1 — код (TDD)

### Task 5: PgWorker.App — `Docker:ContourNetwork` + единая точка `BackupsRuntime()`

**Статус:** не начат.

**Вход:** Фаза 0 закоммичена.

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (`DockerOptions.ContourNetwork` — новое свойство; `BackupsOptions.ToRuntime` — принимает contourNetwork; новый метод `PgWorkerOptions.BackupsRuntime()`)
- Modify: `src/PgWorker.App/Program.cs` — ВСЕ 12 точек потребления: строки 223, 230 (OrphanHold/OrphanDelete), 341 (BackupS3), 599 (BackupProcess), 618 (RestoreProcess), 657 (WalStreamProcess), 676 (RetentionProcess), 695 (BackupVerifyProcess), 712 (RestoreDrillProcess), 730/745 (Supervisor/Sweeper), 869 (ReloadableBackupS3); создание `PlainClusterDriver` ~310
- Test: `src/tests/PgWorker.UnitTests/Docker/DockerEnvBindingsTests.cs` (биндинг `PGW_CONTOUR_NETWORK`)

**Interfaces:**
- Consumes: —.
- Produces: `DockerOptions.ContourNetwork: string` (пусто = прод-режим); `PgWorkerOptions.BackupsRuntime(): BackupsRuntimeOptions` — ЕДИНСТВЕННАЯ точка сборки (читает `Backups` + `Docker.ContourNetwork`); `PlainClusterDriver(..., contourNetwork: string?)`; `BackupsRuntimeOptions.ContourNetwork: string?` — потребляется Tasks 6–8 ЧЕРЕЗ ВСЕ процессы (Backup/Restore/WalStream/Retention/Verify/Drill/Supervisor), а не только через планировщик полных.

**Связь со spec:** §4.7 (настройка «внешняя сеть контура», рабочее имя `PgWorker:Docker:ContourNetwork`/`PGW_CONTOUR_NETWORK`), §5.2.

- [ ] **Step 5.1: Красный тест биндинга**

Механика биндинга (по фактам кода): `PgWorkerOptions` собирается из `builder.Configuration.GetSection("PgWorker")` (appsettings + env-провайдер `PgWorker__*`, Program.cs:42–43); маппинга `PGW_CONTOUR_NETWORK` → конфиг в коде НЕТ (и не добавляется) — имя в дерево приносит deploy-compose (`PgWorker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}`, Step 10.2). Тест проверяет биндинг узла → `DockerOptions.ContourNetwork` через in-memory-конфиг (стиль файла: `ConfigurationManager` уже используется кейсами TLS/SSH/REST; inject-словарь вместо мутации окружения):

В `DockerEnvBindingsTests.cs` добавить:

```csharp
    // AAA (t41): PgWorker:Docker:ContourNetwork биндится в DockerOptions
    // (режим сети контура; значение приносит env PgWorker__Docker__ContourNetwork
    // из deploy-compose — PGW_CONTOUR_NETWORK сам по себе не маппится).
    [Fact]
    public void ContourNetwork_Binds_FromConfigTree()
    {
        // Arrange — in-memory-конфиг: узел PgWorker:Docker тем же путём,
        // каким его кладут appsettings/env-провайдер PgWorker__*.
        var config = new ConfigurationManager();
        config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PgWorker:Docker:ContourNetwork"] = "adminpanel-stand-net",
        });

        // Act
        var options = config.GetSection("PgWorker").Get<PgWorkerOptions>()!;

        // Assert
        options.Docker.ContourNetwork.Should().Be("adminpanel-stand-net");
    }
```

(Если по месту исполнения удобнее env-форма — эквивалент: `config.AddInMemoryCollection` заменить на установку `PgWorker__Docker__ContourNetwork` + `AddEnvironmentVariables()`; ассерт тот же. Выбрать одну форму, вторую не дублировать.)

Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter FullyQualifiedName~ContourNetwork_Binds --nologo` — FAIL (свойства `DockerOptions.ContourNetwork` нет — не компилируется; после Step 5.2 зелёный).

- [ ] **Step 5.2: Свойство DockerOptions**

В `DockerOptions` после `ScrapeNetwork` (Options.cs:204):

```csharp
    /// <summary>
    /// Внешняя сеть контура поставки (arch/14 §2.1/§2.4): задана → ноды кластера,
    /// wal-агенты и ephemeral-джобы бэкапов создаются в ней одной; pgw-net-&lt;C&gt;
    /// не создаётся, extra_hosts спек джобов не применяются (адресация по именам
    /// сети). Пусто (прод/умолчание) — per-cluster pgw-net-&lt;C&gt; + default bridge
    /// с extra_hosts у джобов. Сеть принадлежит поставке/сценарию (движок не
    /// создаёт и не удаляет; ensure идемпотентен, 409 = успех). Не поддерживается
    /// в Mode=Swarm (fail-fast старта). Env: PGW_CONTOUR_NETWORK.
    /// </summary>
    public string ContourNetwork { get; set; } = "";
```

Рядом с валидацией ScrapeNetwork (Mode=Swarm → fail-fast) добавить то же для ContourNetwork.

- [ ] **Step 5.3: Единая точка BackupsRuntime() + замена всех потребителей**

1. `BackupsOptions.ToRuntime(string? contourNetwork = null)` — новый параметр; в `new BackupsRuntimeOptions(...)` добавить `ContourNetwork: contourNetwork`; `BackupsRuntimeOptions` (record) — параметр `string? ContourNetwork = null` после `WalAgentImage`.
2. Новый метод на корне `PgWorkerOptions`:

```csharp
    /// <summary>Единая точка сборки BackupsRuntimeOptions (t41): контур-сеть
    /// читается ЗДЕСЬ из Docker-секции — все процессы-потребители (полные/
    /// restore/wal/ретенция/verify/drill/супервизор) получают один и тот же
    /// режим сетей спек джобов и адресации источника.</summary>
    public BackupsRuntimeOptions BackupsRuntime() => Backups.ToRuntime(
        Docker.ContourNetwork is { Length: > 0 } ? Docker.ContourNetwork : null);
```

3. Program.cs — заменить ВСЕ 12 вхождений `…Backups.ToRuntime()` на `…BackupsRuntime()` (строки 223, 230, 341, 599, 618, 657, 676, 695, 712, 730, 745, 869; формы `sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()` и `…IOptions<PgWorkerOptions>>().Value.BackupsRuntime()`, `opts.BackupsRuntime()`).

```bash
grep -c "Backups.ToRuntime()" src/PgWorker.App/Program.cs   # ожидание после правки: 0
```

4. Создание драйвера (Program.cs ~310) — дополнить параметром (рядом с `scrapeNetwork`):

```csharp
    return new PlainClusterDriver(hosts, factory, docker.EnableDoorman, docker.Images.Node, docker.AdvertisedHost,
        pgtuneExclude: pgtuneExclude, scrapeNetwork: docker.ScrapeNetwork,
        contourNetwork: docker.ContourNetwork is { Length: > 0 } ? docker.ContourNetwork : null,
        restCertificates: sp.GetRequiredService<RestCertificateCache>());
```

- [ ] **Step 5.4: Зелёный + прогоны + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter FullyQualifiedName~DockerEnvBindings --nologo   # PASS
grep -c "Backups.ToRuntime()" src/PgWorker.App/Program.cs || echo "0: все точки на BackupsRuntime()"
git add src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Docker/DockerEnvBindingsTests.cs
git commit -m "feat(t41): Docker:ContourNetwork (env PGW_CONTOUR_NETWORK) + единая точка PgWorkerOptions.BackupsRuntime() — контур-режим доезжает до всех процессов подсистемы; прокидывание в PlainClusterDriver"
```

**Выход:** настройка читается env; ВСЕ потребители BackupsRuntimeOptions получают ContourNetwork одной точкой (фактический список строк Interfaces).

---

### Task 6: PlainClusterDriver — контур-режим сетей нод/агентов

**Статус:** не начат.

**Вход:** Task 5 закоммичен (`contourNetwork` в ctor).

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs`
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (новые кейсы; изолированные пути без Engine — по образцу существующих)

**Interfaces:**
- Consumes: ctor-параметр `string? contourNetwork = null` (Task 5).
- Produces: в контур-режиме сети нод/агентов = сеть контура; `pgw-net-<C>` не создаётся/не удаляется; ensure идемпотентен (409 = успех).

**Связь со spec:** §5.2 (ClusterDriver), §4.7, AC4/AC8.

- [ ] **Step 6.1: Красные тесты (таблица режимов)**

В `ClusterDriverTests.cs` добавить кейсы — через публичный контракт драйвера с фейковым `IDockerEngine` (образец существующих кейсов EnsureNodeAsync; фейк копит вызовы `EnsureNetworkAsync(name)` / `CreateContainerAsync(spec)` / `DeleteNetworkAsync(name)`):

```csharp
    // AAA (t41): сеть ноды/ensure по режиму — контур поставки или per-cluster
    // pgw-net-<C> (прод); alias'ы спеки (nodeName, pgw-<C>-<X>-<n>) не меняются.
    [Theory]
    [InlineData(null, "pgw-net-demo")]                           // прод-режим
    [InlineData("adminpanel-stand-net", "adminpanel-stand-net")] // контур
    public async Task EnsureNode_Network_ByMode(string? contour, string expectedNetwork)
    {
        // Arrange — фейковый движок (нет контейнера → путь создания)
        var (driver, engine) = NewDriverWithFake(contourNetwork: contour);

        // Act
        (await driver.EnsureNodeAsync(SampleTopology(), "n1", SampleAddr(), …, CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert — ensure и спека несут сеть режима; alias'ы на месте
        engine.EnsureNetworkCalls.Should().Contain(expectedNetwork);
        engine.CreatedSpecs.Single().Network.Should().Be(expectedNetwork);
        engine.CreatedSpecs.Single().NetworkAliases
            .Should().Contain(new[] { "n1", "pgw-demo-s1-n1" });
    }
```

Плюс кейсы: (1) `EnsureBackupAgentAsync` в контур-режиме создаёт агента с `spec.Network = contour` (фейк ловит create-спеку); (2) `RemoveNodeAsync` в контур-режиме НЕ зовёт `DeleteNetworkAsync` (пусто в счётчике фейка), в прод-режиме — зовёт с `pgw-net-<C>`; (3) ensure сети контура: фейк отвечает ошибкой-409 → `EnsureNodeAsync` успех (идемпотентность уже-существующей сети поставки).

Прогон: `--filter FullyQualifiedName~ClusterDriver` — новые FAIL.

- [ ] **Step 6.2: Реализация**

В `PlainClusterDriver`:

```csharp
    // Сеть контура поставки (t41, arch/14 §2.1): задана → ноды/агенты живут в ней
    // (pgw-net-<C> не создаётся и не удаляется движком; сеть — владелец поставка).
    private readonly string? contourNetwork;

    // Сеть нод/агентов кластера по режиму: контур или per-cluster pgw-net-<C>.
    private string NodesNet(string cluster) => contourNetwork ?? NetworkName(cluster);
```

(primary-ctor параметр `string? contourNetwork = null` → поле). Замены:
- `EnsureNodeAsync` (~232): `EnsureNetworkAsync(NodesNet(topology.Cluster))`; ensure-инвариант (~259): `var clusterNet = NodesNet(topology.Cluster);` (комментарий дополнить: «в контур-режиме — сеть контура, подключение идемпотентно»).
- `EnsureBackupAgentAsync` (~406/415/431): `EnsureNetworkAsync(NodesNet(cluster))`; `spec with { Network = NodesNet(cluster) }`; инвариант `var clusterNet = NodesNet(cluster);`.
- `BuildSpec` (~754): `Network: NodesNet(topology.Cluster)`.
- `RemoveNodeAsync` (~345–368): блок демонтажа сети обернуть `if (contourNetwork is null) { … }` с комментарием: «сеть контура принадлежит поставке — движок её не удаляет».

- [ ] **Step 6.3: Зелёный + прогоны + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter FullyQualifiedName~ClusterDriver --nologo   # PASS, оба режима
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --nologo                   # регрессия
git add src/PgWorker.Docker/Drivers/ClusterDriver.cs src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs
git commit -m "feat(t41): PlainClusterDriver — режим сети контура: ноды/wal-агенты в сети поставки, pgw-net-<C> не создаётся/не удаляется; таблица режимов"
```

**Выход:** драйвер умеет контур-режим; прод-режим подтверждён таблицей.

---

### Task 7: Спеки ephemeral-джобов — сеть контура, отмена ExtraHosts

**Статус:** не начат.

**Вход:** Task 5 закоммичен (`BackupsRuntimeOptions.ContourNetwork` через `BackupsRuntime()`).

**Files:**
- Modify: `src/PgWorker.Backups/Job/BackupJobSpec.cs`, `Job/VerifyJobSpec.cs`, `Drill/DrillJobSpec.cs`, `Restore/RestoreJobSpec.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupJobSpecTests.cs` (+ Verify-кейс там же, строка 98), `DrillJobSpecTests.cs`, `RestoreJobSpecTests.cs`

**Interfaces:**
- Consumes: `BackupsRuntimeOptions.ContourNetwork`.
- Produces: в контур-режиме все 4 спеки — `Network = ContourNetwork`, `ExtraHosts = null`; прод-режим — прежние (`Network: null`, `ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"]`).

**Связь со spec:** §5.2, §4.7, AC8.

- [ ] **Step 7.1: Красные тесты (таблица двух режимов × 4 спеки)**

В каждый Spec-тест добавить теорию (образец для BackupJobSpecTests):

```csharp
    // AAA (t41): сеть джоба по режиму — контур (без extra_hosts, адресация по
    // именам сети) или прод (default bridge + фиксированные extra_hosts).
    [Theory]
    [InlineData(null, null, new[] { "host.docker.internal:host-gateway", "local:host-gateway" })]
    [InlineData("adminpanel-stand-net", "adminpanel-stand-net", null)]
    public void Network_ByContourMode(string? contour, string? expectedNet, string[]? expectedHosts)
    {
        // Arrange
        var opts = SampleOpts() with { ContourNetwork = contour };

        // Act
        var spec = BackupJobSpec.Build(opts, "demo", "shard1", "20260911a", SampleNode(), "pwd");

        // Assert
        spec.Network.Should().Be(expectedNet);
        spec.ExtraHosts.Should().BeEquivalentTo(expectedHosts ?? (IEnumerable<string>[]);
    }
```

(`SampleOpts`/`SampleNode` — существующие хелперы файла; для Verify/Drill/Restore — их сигнатуры Build.) Прогон: новые FAIL.

- [ ] **Step 7.2: Реализация (4 файла, одинаково)**

В каждой спеке заменить пару `Network: null` / `ExtraHosts: […]` на:

```csharp
            // Режим сети контура (t41, arch/14 §2.1): джоб в сети контура,
            // extra_hosts не применяются (адресация по именам сети; S3/источник
            // резолвятся compose-DNS/alias). Прод-режим — default bridge +
            // фиксированные host-переходы advertised-хостов (arch/19 §2/§6).
            Network: opts.ContourNetwork,
            ExtraHosts: opts.ContourNetwork is null
                ? ["host.docker.internal:host-gateway", "local:host-gateway"]
                : null,
```

- [ ] **Step 7.3: Зелёный + регрессия + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~JobSpec" --nologo   # PASS
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --nologo
git add src/PgWorker.Backups/ src/tests/PgWorker.UnitTests/Backups/
git commit -m "feat(t41): ephemeral-джобы бэкапов (full/verify/drill/restore) — сеть контура без extra_hosts в режиме поставки; таблица режимов 4 спек"
```

**Выход:** спеки джобов двурежимны.

---

### Task 8: BackupProcess — адрес источника джоба `nodeName:5432` в контур-режиме

**Статус:** не начат.

**Вход:** Task 7 закоммичен.

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (~строки 226–239: вычисление `node` + `BackupJobSpec.Build`; и второй вызов ~382)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`

**Interfaces:**
- Produces: DSN джоба в контур-режиме для канонической ноды — `host=<имя ноды> port=5432` (симметрия `ResolveNodeRef`); усыновлённая (object)/прод — `host:pg-port` portalloc, как сейчас.

**Связь со spec:** §4.7 («адрес источника джоба полного — имя ноды в сети контура :5432»), §5.2, AC3.

- [ ] **Step 8.1: Красный тест**

В `BackupProcessTests.cs` (по образцу существующих кейсов запуска джоба):

```csharp
    // AAA (t41): контур-режим — DSN джоба указывает на имя канонической ноды
    // сети контура :5432 (симметрия ResolveNodeRef); прод-режим — host:pg-port
    // portalloc без изменений.
    [Theory]
    [InlineData(null, "local", 15001)]
    [InlineData("adminpanel-stand-net", "shard1a", 5432)]
    public async Task FullJob_Dsn_ByContourMode(string? contour, string expectedHost, int expectedPort)
    {
        // Arrange — топология demo/shard1, нода shard1a в portalloc (host local:15001);
        // фейковый движок ловит CreateContainerAsync; opts.ContourNetwork = contour.
        …existing harness…

        // Act — тик планировщика полных запускает джоб.

        // Assert — env PGW_BK_DSN джоба: host/port ожидаемого режима.
        capturedSpec.Env["PGW_BK_DSN"].Should().Contain($"host={expectedHost}");
        capturedSpec.Env["PGW_BK_DSN"].Should().Contain($"port={expectedPort}");
    }
```

Прогон: FAIL (контур-кейс даёт `host=local port=15001`).

Плюс юнит-кейс планового тика по object-сиду — зеркало `ReproPlanned_ContourDsn_OnlyCanonicalNode` (супервизорный путь, BackupProcessTests.cs): `PlannedTick_ContourDsn_OnlyCanonicalNode` — portalloc с object-нодой (`["shard1/shard1a"] = new("local", new NodePorts(15433, 18011, 0), Object: "as-shard1a")`), плановый тик планировщика → env `PGW_BK_DSN` джоба `host=local`/`port=15433` (portalloc-адрес усыновлённой ноды), НЕ `host=shard1a port=5432`. Прогон: FAIL (guard планового тика не различает object-ноду).

- [ ] **Step 8.2: Реализация**

В месте вычисления `node` (~226–231) сохранить found-флаг и модифицировать источник для спеки:

```csharp
            var nodeKv = addresses.Value.FirstOrDefault(p =>
                    p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)
                    && p.Value.Host == source.Value.Host && p.Value.Ports == source.Value.Ports);
            var node = nodeKv.Key?.Split('/')[1] ?? source.Value.Host;

            // Режим сети контура (t41, arch/14 §2.1/§19 §2): адрес источника
            // канонической ноды — её имя в сети контура :5432 (симметрия
            // ResolveNodeRef §3); усыновлённая (object)/прод — portalloc-адрес.
            var jobSource = options.ContourNetwork is { Length: > 0 }
                && nodeKv.Key is not null && nodeKv.Value.Object is null
                ? source.Value with { Host = node, Ports = source.Value.Ports with { Pg = 5432 } }
                : source.Value;
```

и передать `jobSource` в `BackupJobSpec.Build` (оба места: плановый тик ~239 и повтор/активный ~382 — во втором применить тот же тернар). Guard планового тика — обязательно с `nodeKv.Value.Object is null` (симметрия повтора/активного тика ~394: `source.Object is null`): контур-DSN с именем ноды — ТОЛЬКО для канонических нод; object-нода (усыновлённая) остаётся на portalloc-адресе (закрывается кейсом `PlannedTick_ContourDsn_OnlyCanonicalNode`). Комментарий над запуском («extra_hosts, без портов») уточнить: «(прод: extra_hosts; контур: сеть поставки)».

- [ ] **Step 8.3: Зелёный + прогоны + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter FullyQualifiedName~BackupProcess --nologo   # PASS
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --nologo
git add src/PgWorker.Backups/Process/BackupProcess.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
git commit -m "feat(t41): DSN джоба полного — имя ноды :5432 сети контура в режиме поставки (симметрия ResolveNodeRef); прод без изменений"
```

**Выход:** адрес источника двурежимен.

---

### Task 9: Фаза 1 — сводные прогоны; панель не трогаем

**Статус:** код панели исполнен и зелёный (не трогается); прогоны — после Tasks 5–8.

**Вход:** Tasks 5–8 закоммичены.

**Files:** нет.

**Связь со spec:** §6 Фаза 1, §7.1/§7.8, §9 («Unit+Integration без docker»).

- [ ] **Step 9.1: Юниты PgWorker + Integration без docker + панель**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --nologo
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj --nologo
#   ^ без PGW_TEST_DOCKER: docker-кейсы — авто-skip (DockerTrait.SkipIfUnavailable)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --nologo
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --nologo
```

Ожидание: все зелёные (docker-кейсы PgWorker — skipped), 0 warnings; после каждой серии — финальная строка + зачистка docker-остатков (AGENTS.md). PgWorker docker-E2E — мерж-гейт (Task 20).

**Выход:** Фаза 1 закрыта.

---

## Фаза 2 — стенд/деплой

### Task 10: deploy — env контура, override-сеть (без второй сети), wal build-сервис

**Статус:** частично — v2-исполнено: `PGW_BACKUP_S3_*` на ownS3 + ключи `OWNS3_ACCESS_KEYS__0/1` (коммит `a7b52bdf`); v5-доработка: endpoint по имени сети, `PGW_CONTOUR_NETWORK` + METRICS-ключи, contour-override (с逐 сбросом обеих сетей), wal build-сервис.

**Вход:** Task 9 пройден.

**Files:**
- Modify: `deploy/.env.example` (блок `PGW_BACKUP_S3_*`; `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK`; новый `PGW_CONTOUR_NETWORK`)
- Modify: `deploy/docker-compose.yml` (env сервисов `pgworker`×2; build-сервис `pgworker-wal`; комментарий сервиса `owns3`)
- Create: `deploy/docker-compose.contour.yml` (новый override)

**Interfaces:**
- Produces: `PGW_BACKUP_S3_ENDPOINT=http://owns3:9000` (advertised НЕ задаётся); `PGW_CONTOUR_NETWORK=adminpanel-stand-net`; `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK` = `adminpanel-stand-net` на стенде (прод-дефолт `pgw-metrics` сохранён в sync-блоке 00-up.sh); контур-подъём = `-f docker-compose.yml -f docker-compose.contour.yml --env-file .env`; в контур-модели НЕТ ни отдельной default-сети deploy, ни `pgw-metrics`.

**Связь со spec:** §4.6/§4.7, §5.3 (deploy), AC4.

- [ ] **Step 10.1: .env.example**

Блок `PGW_BACKUP_S3_*` (v2-исполненный) — заменить endpoint и комментарий:

```bash
# Подсистема бэкапов шардов (arch/19, t02: полный контур на стенде):
# S3 = ownS3 (deploy-compose, alias owns3 в стендовой сети; bucket
# pgworker-backups создаёт 00-up.sh root-кредами); воркер с Enabled=true;
# прод — per-install S3, креды ТОЛЬКО env (arch/14 §4 п.3). Стендовый
# endpoint — ИМЯ сети контура (хост-порт :9010 — только наружу: чеки/
# миграция/оператор); advertised НЕ задаётся (= endpoint). Значение
# ACCESS/SECRET_KEY обязано совпадать с writer-ключом ownS3 ниже
# (OWNS3_ACCESS_KEYS__0__*) — один комплект S3-секретов (arch/19 §7).
PGW_BACKUPS_ENABLED=true
PGW_BACKUP_S3_ENDPOINT=http://owns3:9000
PGW_BACKUP_S3_REGION=
PGW_BACKUP_S3_BUCKET=pgworker-backups
PGW_BACKUP_S3_ACCESS_KEY=pgw-backups-writer
PGW_BACKUP_S3_SECRET_KEY=pgw-backups-writer-pwd
#PGW_BACKUP_S3_ADVERTISED_ENDPOINT=
```

Блок `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK` (строки 39–40) — дополнить комментарий и стендовое значение:

```bash
# Единая сеть контура мониторинга: на стенде = стендовая сеть контура
# (отдельная pgw-metrics НЕ создаётся — весь стенд в одной сети; t41);
# прод — владелец deploy (pgw-metrics), стендовый Prometheus — external.
PGW_METRICS_NETWORK=adminpanel-stand-net
PGW_SCRAPE_NETWORK=adminpanel-stand-net
```

Новый блок (после METRICS-ключей):

```bash
# Сеть контура поставки (t41, arch/14 §2.1): имя внешней сети, в которой
# живут ВСЕ контейнеры контура (стенд — adminpanel-stand-net, владелец —
# стендовый compose; deploy подключается external через
# docker-compose.contour.yml; METRICS-ключи выше — то же имя). ПРОД:
# закомментировать — пусто = per-cluster pgw-net-<C> (канон arch/14),
# METRICS-ключи вернуть на pgw-metrics.
PGW_CONTOUR_NETWORK=adminpanel-stand-net
```

- [ ] **Step 10.2: deploy-compose — env + owns3-комментарий**

В env сервисов `pgworker`/`pgworker-2` (рядом с `PgWorker__Docker__ScrapeNetwork`, строка 80) добавить `PgWorker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}`. Комментарий сервиса `owns3` дополнить: «стенд: alias `owns3` в стендовой сети (override `docker-compose.contour.yml`), `:9010` — наружу».

- [ ] **Step 10.3: Override-файл docker-compose.contour.yml (новый, целиком)**

```yaml
# Контур-режим стенда (t41, arch/14 §2.1, arch/owns3/05 §6): ВСЕ сервисы
# deploy-проекта — в ОДНОЙ внешней стендовой сети (владелец — стендовый
# compose). Сбрасываются ОБЕ сети базового файла: default-сеть deploy-проекта
# и pgw-metrics (у воркеров) — контейнеров в двух сетях нет, отдельные сети
# deploy-проекта на стенде не создаются. Подъём: docker compose -f
#   docker-compose.yml -f docker-compose.contour.yml --env-file .env up -d …
# Прод: файл НЕ применяется (сети базового файла, вкл. pgw-metrics).
name: deploy

networks:
  contour:
    name: ${PGW_CONTOUR_NETWORK:?задайте PGW_CONTOUR_NETWORK (имя стендовой сети)}
    external: true
  # Объявление базового файла снимается: сеть pgw-metrics на стенде не
  # создаётся (ни один сервис после override на неё не ссылается).
  pgw-metrics: !reset null

services:
  pgworker:
    networks:
      default: !reset null
      pgw-metrics: !reset null
      contour:
        aliases: [pgworker]
  pgworker-2:
    networks:
      default: !reset null
      pgw-metrics: !reset null
      contour:
        aliases: [pgworker-2]
  kafkaworker:
    networks: { default: !reset null, contour: {} }
  kafkaworker-2:
    networks: { default: !reset null, contour: {} }
  valkeyworker:
    networks: { default: !reset null, contour: {} }
  valkeyworker-2:
    networks: { default: !reset null, contour: {} }
  owns3:
    networks:
      default: !reset null
      contour:
        aliases: [owns3]
```

(kafkaworker/valkeyworker×2/owns3 в базовом файле сетей не объявляют — живут в default; override сбрасывает default и подключает contour. Их собственные `extra_hosts` (`local:host-gateway`) сохраняются — это резолв Engine API docker-хоста, не контурный переход.)

- [ ] **Step 10.4: build-сервис pgworker-wal**

В `deploy/docker-compose.yml` рядом с `pgworker-backup` (профиль `build`) добавить:

```yaml
  # Образ wal-агента (t27/t41, arch/19 §3): publish НА ХОСТЕ (00-up.sh,
  # [PHASE]) → compose build пакует только вывод (канон E2E-образов).
  pgworker-wal:
    build:
      context: ..
      dockerfile: docker/PgWorker.Wal.Dockerfile
      args:
        WAL_PUBLISH_DIR: artifacts/wal/publish
    image: pgworker-wal:dev
    profiles: ["build"]
    entrypoint: ["true"]
```

- [ ] **Step 10.5: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration/deploy
PGW_CONTOUR_NETWORK=adminpanel-stand-net docker compose -f docker-compose.yml -f docker-compose.contour.yml --env-file .env.example config --quiet && echo "contour compose ok"
# инспект итоговой модели: ни один сервис не ссылается на pgw-metrics/default
PGW_CONTOUR_NETWORK=adminpanel-stand-net docker compose -f docker-compose.yml -f docker-compose.contour.yml --env-file .env.example config | grep -c "pgw-metrics" || echo "0: pgw-metrics вне контур-модели"
docker compose --env-file .env.example config --quiet && echo "base compose ok (прод без изменений)"
cd .. && git add deploy/.env.example deploy/docker-compose.yml deploy/docker-compose.contour.yml
git commit -m "feat(t41): deploy — PGW_BACKUP_S3_* на owns3:9000, PGW_CONTOUR_NETWORK + METRICS=стендовая сеть, contour-override одной сети (сброс default И pgw-metrics), build-сервис pgworker-wal"
```

**Выход:** deploy-контракт контура готов: одна сеть, без pgw-metrics/default deploy на стенде; прод-файл неизменен.

---

### Task 11: docker/PgWorker.Wal.Dockerfile — publish-COPY

**Статус:** не начат.

**Вход:** Task 10 закоммичен.

**Files:** Modify: `docker/PgWorker.Wal.Dockerfile` (целиком).

**Связь со spec:** §4.7 («образ wal-агентов»), §5.3.

- [ ] **Step 11.1: Переписать Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1
# Образ WAL-агента-приёмника (t27, arch/19 §2/§3): dotnet publish НА ХОСТЕ,
# в контейнер — ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников;
# канон E2E-образов, по образцу docker/OwnS3.Dockerfile). CLI (узкий контекст):
#   dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -o artifacts/wal/publish
#   docker build -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev artifacts/wal/publish
# Compose (контекст — корень репо, ARG указывает путь вывода):
#   build: { context: .., dockerfile: docker/PgWorker.Wal.Dockerfile,
#            args: { WAL_PUBLISH_DIR: artifacts/wal/publish } }
# ENTRYPOINT без shell; параметры воркер передаёт ТОЛЬКО env (§7).
ARG WAL_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/runtime:10.0
ARG WAL_PUBLISH_DIR
WORKDIR /app
COPY ${WAL_PUBLISH_DIR} ./
ENTRYPOINT ["dotnet", "PgWorker.WalReceiver.dll"]
```

- [ ] **Step 11.2: Проверка сборки + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -o artifacts/wal/publish --nologo
docker build -q -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev artifacts/wal/publish
git add docker/PgWorker.Wal.Dockerfile && git commit -m "feat(t41): PgWorker.Wal.Dockerfile — publish-COPY (ARG WAL_PUBLISH_DIR, без sdk-стадии/COPY src)"
```

**Выход:** wal-образ собирается по канону E2E-образов.

---

### Task 12: стендовый compose — демонтаж усыновления, одна сеть, панель на owns3

**Статус:** частично — v2-исполнено: панель env ownS3 + extra_hosts (коммит `1176282a`, endpoint `host.docker.internal:9010` → v5: `owns3:9000`, extra_hosts СОХРАНЯЕТСЯ — резолв advertised-хостов); v5: снос as-нод/hc/sidecar, имя сети, прометей без pgw-metrics, HostMap.

**Вход:** Task 11 закоммичен.

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (удалить сервисы `s1a/s1b/s2a/s2b`, `hc1a/hc1b/hc2a/hc2b`; сети; env панели; прометей)
- Modify: `dev-stand/adminpanel/adminpanel.appsettings.json` (удалить секцию `AdminPanel:Probes:HostMap`)
- Delete: `dev-stand/adminpanel/sidecar/` (каталог сборки эмуляторов)

**Связь со spec:** §4.7 (демонтаж усыновления, стенд-сеть), §5.3.

- [ ] **Step 12.1: Сервисы, тома, sidecar**

1. Удалить сервисы `s1a`, `s1b`, `s2a`, `s2b` (postgres:18), `hc1a`, `hc1b`, `hc2a`, `hc2b` (build `./sidecar`) целиком.
2. `minio` (as-minio) — остаётся в full-профиле (переходный read-only-архив); в комментарии сервиса ссылку «docs/owns3-migration.md §7» заменить на «§4 (критерий сноса)/§6 (порядок сноса)».
3. Env панели: `AdminPanel__Backups__S3__Endpoint: http://owns3:9000`, AccessKey/SecretKey `panel-reader/panel-reader-pwd` (v2-значения остаются); комментарий: «ownS3 — alias в стендовой сети (deploy external-attach); `:9010` — наружу». **`extra_hosts` сервису `adminpanel` СОХРАНИТЬ** (обновить комментарий: «резолв advertised-имени docker-хоста `host.docker.internal` — SQL/Patroni-пробы нод по portalloc `:15xxx/:18xxx` и kafka-endpoints; ownS3 адресуется по имени сети `owns3:9000` — контурного хост-перехода нет»).
4. Прометей: из `networks` убрать `pgw-metrics` (остаётся `default: {}`); комментарий: «таргеты — имена стендовой сети (pgworker:8080, ноды по alias portalloc-скрейпа); отдельной сети мониторинга на стенде нет».
5. Блок `networks:` (внизу файла) заменить на:

```yaml
# Стенд — ОДИН контур = ОДНА сеть (t41, arch/adminpanel/04 §2.3): в ней ВСЕ
# контейнеры стенда; deploy-проект подключается external
# (docker-compose.contour.yml). Явное имя — для PGW_CONTOUR_NETWORK.
networks:
  default:
    name: adminpanel-stand-net
```

6. Удалить каталог sidecar-эмуляторов: `git rm -r dev-stand/adminpanel/sidecar` (входит в коммит шага).
7. Шапку файла (строки 1–6: «профили quick … и full (+PG-шарды и patroni-эмуляторы) … сиды …») переписать под фактическое состояние: профили quick (etcd+панель) и full (+as-minio — переходный read-only-архив); PG-демо — канонический кластер движка (`pgw-demo-*` вне compose; `POST /api/clusters` из 00-up.sh); patroni-эмуляторы/усыновлённые as-ноды — демонтированы.

- [ ] **Step 12.2: HostMap**

В `adminpanel.appsettings.json` удалить секцию `AdminPanel:Probes:HostMap` (ноды пробируются по advertised portalloc напрямую).

- [ ] **Step 12.3: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration/dev-stand/adminpanel
docker compose config --quiet && echo "compose ok"
grep -c "s1a\|hc1a\|sidecar" docker-compose.yml   # 0
test ! -d sidecar && echo "sidecar снят"
grep -A2 "extra_hosts" docker-compose.yml | grep -q host-gateway && echo "extra_hosts панели на месте"
cd ../.. && git add -A dev-stand/adminpanel/docker-compose.yml dev-stand/adminpanel/adminpanel.appsettings.json dev-stand/adminpanel/sidecar
git commit -m "feat(t41): стендовый compose — одна сеть adminpanel-stand-net, демонтаж as-нод/hc/HostMap/sidecar, панель на owns3:9000 (extra_hosts — резолв advertised), прометей без pgw-metrics"
```

**Выход:** стендовый compose — v5-модель, одна сеть; sidecar удалён из репо.

---

### Task 13: 00-up.sh — перестройка подъёма

**Статус:** частично — v2-исполнено: bucket-сид снаружи :9010, комментарий as-minio (коммит `ad9588a8`); v5: wal publish/build, порядок сетей + METRICS-sync на стендовую сеть, POST /api/clusters + wait-гейт, снос as-блоков.

**Вход:** Task 12 закоммичен.

**Files:** Modify: `dev-stand/adminpanel/checks/00-up.sh`.

**Связь со spec:** §4.7, §5.3 (00-up.sh), AC2.

- [ ] **Step 13.1: Wal publish/build**

После блока publish OwnS3.App:

```bash
# WAL-агент (t41): publish НА ХОСТЕ → образ publish-COPY (канон E2E-образов;
# сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish PgWorker.WalReceiver ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" \
  -c Release -o "$ROOT/artifacts/wal/publish" --nologo \
  || { echo "❌ publish WalReceiver не удался"; exit 1; }
docker build -q -f "$ROOT/docker/PgWorker.Wal.Dockerfile" -t pgworker-wal:dev "$ROOT/artifacts/wal/publish" \
  || { echo "❌ образ pgworker-wal:dev не собрался"; exit 1; }
echo "  образ pgworker-wal:dev готов"
```

- [ ] **Step 13.2: Порядок подъёма — стендовый compose ДО deploy; METRICS-sync на стендовую сеть**

1. В sync-блоке METRICS-ключей (00-up.sh:87–96) дефолт заменить на стендовую сеть:

```bash
# t15/t41: единая сеть контура = стендовая сеть (adminpanel-stand-net);
# отдельная pgw-metrics на стенде НЕ создаётся (весь стенд в одной сети).
for k in PGW_METRICS_NETWORK PGW_SCRAPE_NETWORK; do
  v="${!k:-adminpanel-stand-net}"
  …
```

2. Блок §1b-ранний (deploy up до стендового) заменить: СНАЧАЛА стендовый compose (владелец сети `adminpanel-stand-net`), ЗАТЕМ deploy с contour-override:

```bash
# Стенд — один контур одна сеть (t41): владелец сети adminpanel-stand-net —
# стендовый compose; поднимается ПЕРВЫМ, deploy-контур подключается external
# (docker-compose.contour.yml; отдельная pgw-metrics не создаётся).
[ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
…sync PGW_API_HOST_PORT/2 в .env (существующий блок)…
echo ">>> поднимаю стенд (docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build)"
compose_up_ok=0
for _ in 1 2 3; do
  if docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build 2>&1 | tail -5; then
    compose_up_ok=1; break
  fi
  echo "  compose up не удался — пауза 10 c"; sleep 10
done
[ "$compose_up_ok" = 1 ] || { echo "❌ стенд не поднялся за 3 попытки"; exit 1; }

# deploy-контур (воркеры + owns3) — в стендовую сеть (контур-override).
pg_up_ok=0
for _ in 1 2 3; do
  if ( cd "$ROOT/deploy" && docker compose -f docker-compose.yml -f docker-compose.contour.yml \
       --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 owns3 2>&1 | tail -2 ); then
    pg_up_ok=1; break
  fi
  echo "  deploy up не удался (порт не отдан после recreate) — пауза 15 c"; sleep 15
done
[ "$pg_up_ok" = 1 ] || { echo "❌ deploy-контур не поднялся за 3 попытки"; exit 1; }
```

- [ ] **Step 13.3: bucket-сид + owns3 healthz (снаружи :9010 — v2-блок сохраняется)**

Существующие v2-блоки (healthz-поллинг `localhost:${OWNS3_PORT}/healthz` + mc mb снаружи) — оставить; комментарий уточнить: «хост-переход наружу: bucket-сид — чек-подобная операция с хоста (внутри контура адресация owns3:9000)».

- [ ] **Step 13.4: Снос as-блоков; pg-сид → POST /api/clusters + wait-гейт**

1. Удалить блоки: §1a (as-minio: health-гвард + bucket-сид — as-minio остаётся сервисом full-профиля как пассивный переходный архив, гварды подсистеме не нужны), §1a-2 (сборка pgworker-backup — СОХРАНИТЬ: образ нужен джобам!), §2 (waits/patch_hba нод), §5 (set_sync), §6 (schemas), §4 (эмуляторы/master-ключи), §1c (pg-сид `05-seed.sh pg` + wait сида).
2. На месте §1c — создание кластера (гейт конвергенции по arch/14: `config` без `NOT_INITIALIZED` + ноды `RUNNING` + status-ключи бакетов СНЯТЫ — их отсутствие и есть завершение провижининга, arch/14: «снятие = бакет ACTIVE»):

```bash
# 1c) Канонический demo (t41, arch/adminpanel/04 §2.2): декларация ЧЕРЕЗ API
#     воркера (mTLS healthcheck-парой): движок провижинит Spilo-ноды с
#     Patroni в стендовой сети, portalloc-порты, схемы bucket_*, → Active.
#     Идемпотентность: живой config → 409 (перезапуск 00-up на живом стенде —
#     no-op); пересборка demo — полный цикл (90-down.sh -v → 00-up.sh).
echo ">>> [PHASE] создание кластера demo (POST /api/clusters, $(date +%H:%M:%S))"
create_code="$(curl -s -o /tmp/pgw-create-demo.json -w '%{http_code}' -m 10 \
  --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" \
  --key "$ROOT/deploy/tls/healthcheck.key" \
  -X POST "https://localhost:${PGW_API_HOST_PORT:-8080}/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","buckets":16,"shards":2,"replicas":2,"requestCpu":2,"requestMem":2,"requestDisk":100}')"
case "$create_code" in
  201) echo "  demo: заявка принята (201)";;
  409) echo "  demo уже существует (409) — пропускаю создание";;
  *) { echo "❌ POST /api/clusters → $create_code"; cat /tmp/pgw-create-demo.json; exit 1; };;
esac

# Wait-гейт конвергенции (arch/14: Active = config без NOT_INITIALIZED, все
# ноды RUNNING, status/bucket_* СНЯТЫ): бюджет 600 c (Spilo-инициализация),
# [PHASE]-метки.
echo ">>> [PHASE] жду конвергенцию demo (бюджет 600 c, $(date +%H:%M:%S))"
demo_ready() {
  conf="$(ect get /clusters/demo/config --print-value-only 2>/dev/null || true)"
  [ -n "$conf" ] && ! echo "$conf" | grep -q '"state":"NOT_INITIALIZED"' || return 1
  for s in shard1 shard2; do
    for n in a b; do
      [ "$(ect get "/clusters/demo/shards/$s/nodes/$s$n/state" --print-value-only 2>/dev/null)" = "RUNNING" ] || return 1
    done
  done
  [ -z "$(ect get /clusters/demo/buckets/status/ --prefix --keys-only 2>/dev/null)" ] || return 1
}
for i in $(seq 1 300); do demo_ready && break; sleep 2; done
demo_ready || { echo "❌ demo не сошёлся за 600 c (docker logs deploy-pgworker-1; docker ps | grep pgw-demo)"; exit 1; }
echo "  demo: Active, ноды RUNNING, status-ключи бакетов сняты (shard1/shard2 × a/b)"
```

3. Блоки §7 (kafkaworker keys), §7b (valkey), §7c (`05-seed.sh valkey`), §8 (панель), §9 (прометей) — сохранить (порядок: после создания demo).
4. Шапку скрипта (строки 1–6: «реплики, sync-standby, инвентарь схем … + сид demo (t03)») переписать под фактический порядок подъёма: publish-фазы образов ([PHASE]: OwnS3, wal-агент, панель) → стендовый compose (владелец сети) → deploy-контур (contour-override) → bucket-сид → `POST /api/clusters` (канонический demo) → панель/прометей.

- [ ] **Step 13.5: Синтаксис + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
bash -n dev-stand/adminpanel/checks/00-up.sh && echo "syntax ok"
grep -c "adminpanel-stand-net" dev-stand/adminpanel/checks/00-up.sh   # >=3 (sync + комментарии)
git add dev-stand/adminpanel/checks/00-up.sh
git commit -m "feat(t41): 00-up.sh — wal-образ publish-COPY, стендовая сеть первой + deploy contour-override (METRICS=стендовая сеть), канонический demo (POST /api/clusters + wait-гейт со снятыми status-ключами), демонтаж as-блоков"
```

**Выход:** подъём — v5-модель.

---

### Task 14: 05-seed.sh + чеки 10/20/30/40/60

**Статус:** не начат (чек 45 — v2-исполнен, доработка в Task 15).

**Вход:** Task 13 закоммичен.

**Files:**
- Modify: `dev-stand/adminpanel/checks/05-seed.sh` (удалить pg-ветку)
- Modify: `checks/10-smoke-api.sh`, `20-alerts.sh`, `30-failover.sh`, `40-live-probes.sh`, `60-move-ops.sh`

**Связь со spec:** §4.7 (сид), §5.3 (чеки), §7.6.

- [ ] **Step 14.1: 05-seed.sh — только kafka/valkey**

Удалить pg-ветку (описание/вызовы `05-seed.sh pg`, help, маппинг воркера); default — `kafka valkey`. Хелп-текст обновить.

- [ ] **Step 14.2: 10-smoke-api.sh**

Замены: `.shards[] | select(.name=="s1")` → `select(.name=="shard1")`; мастер-ассерты без хардкода адреса (portalloc-динамичен): `(.masterAddress | length) > 0` вместо `== "s1a:5432"`; `/api/ha/demo-s1` → `/api/ha/demo-shard1`, `leaderName` — непустой, members == 2; echo-строки — по факту. Проверки heal/buckets (16) — оставить.

- [ ] **Step 14.3: 20-alerts.sh**

Усыновлённый блок (portalloc-объекты `s1/s1a…`, object-контейнеры as-*) удалить. Сценарии на живом каноническом кластере: (1) move туда-обратно (существующий сценарий чека) — scope/шарды `demo/shard1`, `demo/shard2`; (2) `shard-no-master` (сохраняется): `ect del /clusters/demo/shards/shard2/master` → `wait_alert shard-no-master demo/shard2` → возврат ключа фактическим мастером (значение — из `GET /api/ha/demo-shard2` поле leader или `/api/clusters/demo` masterAddress) → `wait_no_alert`. Все литералы `s1/s2/s1a:5432` → `shard1/shard2` и фактические значения из API.

- [ ] **Step 14.4: 30-failover.sh**

Переписать на Patroni-механику: (1) мастер `shard1` — из leader-ключа `/clusters/demo/shards/shard1/master`; (2) `docker kill` контейнера `pgw-demo-shard1-<master-node>` (имя ноды из ключа; контейнер вида `pgw-demo-shard1-shard1a`); (3) Patroni (DCS etcd) сам промотит реплику — wait нового мастера (leader-ключ сменился, lease жив); (4) алерты: `shard-no-master` вспыхнул-погас (или не вспыхнул при быстром промоте — чек ждёт гашения при живом мастере); (5) надзор движка rejoin'ит убитую ноду (wait ноды RUNNING в `/nodes/shard1*/state`); (6) финал: оба члена RUNNING, master-ключ жив. Бюджеты: [PHASE]-метки; при нестабильности — фиксация в журнале задачи и вынос чека из гейта t41 отдельным решением (спека §8).

- [ ] **Step 14.5: 40-live-probes.sh**

SQL/Patroni-пробы — advertised portalloc: адреса нод из `/api/clusters/demo` (masterAddress/nodes) — curl по `host.docker.internal:18xxx` (patroni) и psql по `:15xxx`; ассерты `masterLeaseAlive == true`, алерты чисты. Убрать хардкоды `5433-5436`; шард-имена `shard1/shard2`. (Чек гоняется после 30-го — топология любая: мастер по факту.)

- [ ] **Step 14.6: 60-move-ops.sh**

Гвард: живой `/clusters/demo/config` (не NOT_INITIALIZED) + ноды RUNNING (wait до 60 c); `"old_shard":"s2"` → `"old_shard":"shard2"`; `demo/s1|s2`-строки → `demo/shard1|shard2`.

- [ ] **Step 14.7: Синтаксис + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration/dev-stand/adminpanel
for f in checks/05-seed.sh checks/10-smoke-api.sh checks/20-alerts.sh checks/30-failover.sh checks/40-live-probes.sh checks/60-move-ops.sh; do bash -n "$f" || exit 1; done && echo "syntax ok"
grep -rc "s1a\|demo/s1\b" checks/ | grep -v ":0" || echo "хардкодов as-модели нет"
cd ../.. && git add dev-stand/adminpanel/checks/
git commit -m "feat(t41): чеки 05/10/20/30/40/60 — канонический demo (shard1/shard2, portalloc-динамика, Patroni-failover), pg-ветка сида снята"
```

**Выход:** чеки синхронны канонической модели.

---

### Task 15: чек 45 (реальные бэкапы, shard1) + 90-down (pgw-контейнеры)

**Статус:** частично — чек 45 v2-исполнен (коммит `e9f54520`: гвард healthz :9010, mc-налив снаружи, clusterOk==null); v5: налив/гварды на `demo/shard1` (реальные шарды), дерево — реальные бэкапы.

**Вход:** Task 14 закоммичен.

**Files:**
- Modify: `dev-stand/adminpanel/checks/45-backups-storage.sh`
- Modify: `dev-stand/adminpanel/checks/90-down.sh`

**Связь со spec:** §5.3 (45/90), §7.6.

- [ ] **Step 15.1: чек 45**

1. mc-налив (снаружи :9010 — v2-блок): префиксы `demo/s1/full/…` → `demo/shard1/full/20260913a/base.tar`, `demo/shard1/wal/000000010000000000000001`.
2. jq-гварды дерева: `.shard == "s1"` → `"shard1"`; `fullsCount >= 1` остаётся (реальный шард несёт живые полные — тестовый налив только гарантирует минимум до первого тика планировщика).
3. Комментарий шапки: «грань против ownS3 (контур: owns3:9000 внутри / :9010 снаружи); дерево — реальные бэкапы demo + идемпотентный тестовый налив».

- [ ] **Step 15.2: 90-down.sh**

После стендового down и deploy down добавить блок сноса движковых объектов (ноды/агенты/джобы — вне compose):

```bash
# Канонические контейнеры движка (t41): pgw-demo-* ноды, wal-агенты, джобы —
# созданы Engine API (не compose), полный down их не трогает. Снос явно +
# страховочный prune сетей (README-cleanup).
docker ps -aq --filter name='^pgw-' --format '{{.ID}}' | xargs -r docker rm -f
docker volume ls -q --filter name='^pgw-' --format '{{.Name}}' | xargs -r docker volume rm -f
docker network ls --filter name='^pgw-net-' --format '{{.Name}}' | xargs -r docker network rm 2>/dev/null || true
docker network prune -f
```

- [ ] **Step 15.3: Синтаксис + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
bash -n dev-stand/adminpanel/checks/45-backups-storage.sh && bash -n dev-stand/adminpanel/checks/90-down.sh && echo "syntax ok"
grep -c '"shard1"' dev-stand/adminpanel/checks/45-backups-storage.sh   # >=2
git add dev-stand/adminpanel/checks/45-backups-storage.sh dev-stand/adminpanel/checks/90-down.sh
git commit -m "feat(t41): чек 45 — реальные бэкапы demo/shard1 (налив идемпотентен); 90-down — снос движковых pgw-* контейнеров/томов/сетей"
```

**Выход:** чеки приёмки S3-грани и teardown — v5.

---

### Task 15b: мониторинг — демонтаж эмуляторной модели (prometheus.yml + чек 65)

**Статус:** не начат (конфиг/чек мониторинга не покрывались задачами плана: джоба «patroni» скрейпит демонтированные hc-контейнеры — таргеты вечно down; чек 65 требует «patroni up ≥ 2» — гарантированно красный после демонтажа эмуляторов).

**Вход:** Task 15 закоммичен.

**Files:**
- Modify: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`
- Modify: `dev-stand/adminpanel/checks/65-metrics.sh`

**Связь со spec:** §4.7 (демонтаж усыновления/эмуляторов — доводится до мониторинга), §5.3 (стендовые файлы и чеки).

- [ ] **Step 15b.1: prometheus.yml — джоба «patroni» и комментарий сети**

1. Удалить джобу `patroni` (строки ~37–41: static-таргеты `hc1a:8008`…`hc2b:8008` — демонтированные эмуляторы; реальные ноды скрейпит джоба `patroni-nodes` file_sd — без изменений).
2. Комментарий шапки scrape_configs (строки ~10–12): «в единой сети контура pgw-metrics» → «в единой стендовой сети контура adminpanel-stand-net» (сети pgw-metrics на стенде нет — решение 2 шапки плана).

- [ ] **Step 15b.2: 65-metrics.sh — требование эмуляторов и упоминания эмуляторной модели**

1. Строки ~77–79: удалить `patroni_up`-блок (jq-подсчёт + ассерт «up ≥ 2») и «включая patroni: $patroni_up» из echo-строки — джобы patroni больше нет.
2. Упоминания эмуляторной/усыновлённой модели убрать: комментарий шага 2 (~64–67 — «остановленный эмулятор демо-кластера»; остаются легитимные причины down patroni-nodes: rebuild ноды ~90 с, устаревший file_sd); комментарий классификации (~89–92 — «advertised усыновлённого контура», R9/extra_hosts-мост); else-ветку ~112–119 («наблюдение несёт джоба patroni (шаг 2: $patroni_up)») — удалить целиком: мёртвая после демонтажа усыновления, ссылается на удалённую джобу/переменную.

- [ ] **Step 15b.3: Синтаксис + grep + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
bash -n dev-stand/adminpanel/checks/65-metrics.sh && echo "syntax ok"
grep -c "hc1a\|hc2b\|pgw-metrics" dev-stand/adminpanel/metrics/prometheus/prometheus.yml   # 0 (patroni-nodes остаётся)
grep -c "patroni_up\|эмулятор" dev-stand/adminpanel/checks/65-metrics.sh   # 0
git add dev-stand/adminpanel/metrics/prometheus/prometheus.yml dev-stand/adminpanel/checks/65-metrics.sh
git commit -m "feat(t41): мониторинг — демонтаж эмуляторной модели: джоба patroni (hc*:8008) из prometheus.yml и требование её таргетов из чека 65; сеть комментария — adminpanel-stand-net"
```

**Выход:** конфиг и чек мониторинга синхронны канонической модели (эмуляторов нет; реальные ноды — patroni-nodes).

**Проверка:** живой прогон `checks/65-metrics.sh` на стенде — зелёный: джоба patroni в конфиге отсутствует, все джобы кроме patroni-nodes строго up, patroni-nodes — условно (шаг 2.1); прогон включён в цепочку Step 18.4.

---

### Task 16: README стенда

**Статус:** частично — v2-исполнен (коммит `78e83a2f`); v5-доработка.

**Вход:** Tasks 15, 15b закоммичены.

**Files:** Modify: `dev-stand/adminpanel/README.md`.

**Связь со spec:** §5.3 (README).

- [ ] **Step 16.1: Правки**

1. Шапка «Быстрый старт»: «всё — одним скриптом; стенд = ОДИН контур ОДНА сеть `adminpanel-stand-net` (etcd, панель, PG-ноды demo канонические (движок), kafkaworker/valkeyworker, PgWorker+owns3 (deploy), мониторинг)».
2. Профили: `full` = `+ as-minio (переходный архив, :9000/:9001)` — as-ноды/hc из списка уходят; metrics — тот же контур (pgw-metrics нет).
3. Раздел «Хранилище бэкапов (ownS3…)» — дополнить: owns3 — alias стендовой сети (`owns3:9000` внутри; `:9010` наружу: чеки/миграция/оператор); demo — канонический кластер (`POST /api/clusters`, Spilo/Patroni, `pgw-demo-*`); стенд задаёт `PGW_CONTOUR_NETWORK` (arch/14 режим сети контура).
4. Раздел «Сиды через API воркеров»: pg-ветка — «demo создаётся `00-up.sh` через `POST /api/clusters` (не сид); `05-seed.sh` — kafka/valkey».
5. E2E-порядок: чек 30 — Patroni-failover; повторный прогон с чистого состояния.

- [ ] **Step 16.2: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -c "adminpanel-stand-net\|POST /api/clusters\|owns3:9000" dev-stand/adminpanel/README.md   # >=3
grep -c "s1a\|hc1a" dev-stand/adminpanel/README.md || echo "as-модель из README ушла"
git add dev-stand/adminpanel/README.md && git commit -m "docs(t41): README стенда — один контур одна сеть, канонический demo, owns3:9000/:9010, PGW_CONTOUR_NETWORK"
```

**Выход:** README описывает v5-стенд.

---

### Task 17: docs/owns3-migration.md — контур-адресация + раздел «Тестовый прогон»

**Статус:** частично — создан в v2 (коммит `cd57ea73`); v5: адресация по имени сети + НОВЫЙ раздел «Тестовый прогон» (spec §5.4).

**Вход:** Task 16 закоммичен.

**Files:** Modify: `docs/owns3-migration.md`.

**Связь со spec:** §5.4 (включая «тестовый прогон: налив в as-minio → mirror → гейт → чистка»), §4.2/§4.3.

- [ ] **Step 17.1: Контур-адресация (правки существующих разделов)**

1. §2 Предпосылки: добавить «стенд — один контур одна сеть; endpoint воркера/панели — `http://owns3:9000` (имя сети); миграция выполняется СНАРУЖИ через хост-публикации `:9000`/`:9010`».
2. §3.3 Переключение: п.1 — `PGW_BACKUP_S3_ENDPOINT=http://owns3:9000` (advertised не задаётся); п. «стендовый env панели» — `http://owns3:9000`.
3. §5 Откат: env-флип на `http://minio:9000` (compose-DNS as-minio в стендовой сети), креды minioadmin — НЕ `host.docker.internal:9000`.

- [ ] **Step 17.2: Новый раздел «Тестовый прогон» (после §6)**

```markdown
## 7. Тестовый прогон процедуры (стенд без живых цепочек в MinIO)

Когда as-minio пуст (свежий стенд), процедура доказывается тестовым прогоном:

1. **Bucket-сид в as-minio**: `MC "mc alias set src http://localhost:9000
   minioadmin minioadmin >/dev/null && mc mb --ignore-existing
   src/pgworker-backups"` (as-minio — сервис стендового компоуза; bucket
   сидится только здесь — 00-up.sh его больше не создаёт).
2. **Временный env-флип на MinIO**: `PGW_BACKUP_S3_ENDPOINT=http://minio:9000`,
   креды `minioadmin/minioadmin` → рестарт `pgworker pgworker-2` (контур-
   override) → дождаться ≥1 COMPLETED-полного на шард + wal-объекты в
   as-minio (наблюдение — панель/etcd; полные снимает планировщик).
3. **Процедура §3 целиком**: mirror×3 (снаружи `:9000`→`:9010`) → гейт §3.5 →
   переключение на `http://owns3:9000` → рестарт.
4. **Чистка тестовых цепочек** (возврат стенда к рабочему состоянию):
   - вернуть `deploy/.env` (`http://owns3:9000`, writer-ключ) — если не
     возвращено шагом 3;
   - удалить перенесённые тестовые цепочки из ownS3 и as-minio:
     `mc rm --recursive --force <alias>/pgworker-backups/demo/` в ОБОИХ
     хранилищах (только тестовые префиксы, созданные прогоном);
   - etcd-статусы тестовых полных/wal-ключи: подсистема сойдётся сама
     (супервизор/ретенция уберут осиротевшие записи; форсировать руками
     не нужно).
   Прогон на стенде с ЖИВЫМИ цепочками — шаги 1–3 без чистки (данные
   остаются рабочими).
```

- [ ] **Step 17.3: Проверка + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
grep -c "owns3:9000\|minio:9000" docs/owns3-migration.md   # >=3 (endpoint'ы по имени сети)
grep -n "## 7. Тестовый прогон" docs/owns3-migration.md
git add docs/owns3-migration.md && git commit -m "docs(t41): owns3-migration — endpoint'ы по имени стендовой сети (owns3:9000, откат minio:9000), раздел «Тестовый прогон» (сид → флип → mirror → гейт → чистка)"
```

**Выход:** ранбук синхронен сетевой модели и содержит тестовый прогон.

---

## Фаза 3 — стендовая приёмка

### Task 18: Полный цикл приёмки v5

**Статус:** не начат (v2-частично: зачистка/подъём/чек 46 проходили; v5 — после перестройки, целиком заново).

**Вход:** Tasks 5–17 закоммичены; образы — `dev-stand/images/pull-images.sh` (спило-образ зеркалирован).

**Files:** нет (операционная приёмка; порт/root ownS3 — парсингом из `deploy/.env`, см. v2-паттерн).

**Связь со spec:** §6 Фаза 3, §7.2–§7.7, AC4.

- [ ] **Step 18.1: Чистый цикл**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration/dev-stand/adminpanel
checks/90-down.sh -v          # сносит и движковые pgw-* (Task 15)
mv "$PWD/../../deploy/.env" /tmp/deploy-env.pre-v5 2>/dev/null || true   # свежий .env из шаблона
checks/00-up.sh               # фон (run_in_background), контроль task_id, шаги ≤30 c
```

Ожидание в журнале: `[PHASE] publish WalReceiver`, `образ pgworker-wal:dev готов`, стенд → deploy contour → `owns3 жив` → `bucket … готов` → pgworker ×2 healthz → `создание кластера demo (201)` → `[PHASE] жду конвергенцию` → `demo: Active, ноды RUNNING, status-ключи бакетов сняты` → панель/прометей → `✓ стенд поднят`. Исчерпание бюджета конвергенции — диагностика по `docker logs deploy-pgworker-1` + `docker ps | grep pgw-demo` (телеметрия AGENTS.md; перезапуск теста ради «что было» — запрещён).

- [ ] **Step 18.2: AC4 — инспект ровно-одной-сети (все контейнеры из сети)**

```bash
NET=adminpanel-stand-net
# Состав контейнеров — из самой сети (ничего не забыто: kfw/vwk/demo/агенты):
members="$(docker network inspect "$NET" --format '{{range $k,$v := .Containers}}{{$v.Name}} {{end}}')"
echo "в $NET: $members"
for c in as-etcd-1 as-etcd-2 as-etcd-3 as-adminpanel as-minio as-prometheus \
         deploy-pgworker-1 deploy-pgworker-2 deploy-owns3-1; do
  echo "$members" | grep -qw "$c" || { echo "❌ $c не в $NET"; exit 1; }
done
echo "$members" | grep -q "deploy-kafkaworker-1" || echo "⚠ kafka-профиль не поднят (kafkaworker вне проверки)"
echo "$members" | grep -cq "pgw-demo-" || { echo "❌ ноды demo вне стендовой сети"; exit 1; }
echo "$members" | grep -cq "pgw-backup-wal-" || { echo "❌ wal-агенты вне стендовой сети"; exit 1; }
# РОВНО одна сеть у КАЖДОГО контейнера сети; запрещённых сетей нет:
for c in $members; do
  n="$(docker inspect "$c" -f '{{len .NetworkSettings.Networks}}')"
  [ "$n" = 1 ] || { echo "❌ $c в $n сетях"; exit 1; }
done
docker network ls --format '{{.Name}}' | grep -E '^pgw-net-demo$|^pgw-metrics$' && { echo "❌ запрещённые сети существуют"; exit 1; } || echo "pgw-net-demo / pgw-metrics не существуют"
```

(Имена deploy-контейнеров сверить по `docker ps` — префикс проекта `deploy-`.)

- [ ] **Step 18.3: AC3 — подсистема в ownS3**

```bash
WT=/Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
OWNS3_PORT=…; OWNS3_ROOT_USER=…; OWNS3_ROOT_PASSWORD=…   # парсинг из deploy/.env (паттерн чека 46)
docker run --rm --add-host host.docker.internal:host-gateway --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set o http://host.docker.internal:${OWNS3_PORT} '$OWNS3_ROOT_USER' '$OWNS3_ROOT_PASSWORD' >/dev/null \
      && mc ls --recursive o/pgworker-backups | head -30"    # demo/shard*/full/… и demo/shard*/wal/…
docker ps --format '{{.Names}}' | grep -c pgw-backup-wal    # >=4 агента (2 шарда × 2 ноды) — live
cd "$WT/dev-stand/adminpanel"
docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 get /pgworker/backups/ --prefix > /tmp/pgw-backups.txt
grep -c '"state":"COMPLETED"' /tmp/pgw-backups.txt                    # >=2
grep -c '"state":"OK"' /tmp/pgw-backups.txt || echo "verify OK: ждём тик"   # verify.state=OK полных
grep -c BROKEN /tmp/pgw-backups.txt || echo "BROKEN: 0"
grep -c '"state":"FAILED"' /tmp/pgw-backups.txt || echo "FAILED: 0"
# as-minio не получает объектов (переходный архив пуст на свежем стенде):
docker run --rm --entrypoint /bin/sh --network "$(docker inspect as-minio -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://as-minio:9000 minioadmin minioadmin >/dev/null && mc ls --recursive a/pgworker-backups"   # пусто
```

Полные снимаются планировщиком после конвергенции (минуты; поллинг ≤30 с шагами, контроль `docker logs deploy-pgworker-1`). Дрилл (AC7): дождаться ≥1 `SUCCEEDED`-дрилла в гейт-окне наблюдения (поле статуса дрилла — по arch/19; если окно наблюдения короче интервала дрилов — фиксация в `docs/owns3-migration.md`, критерий §4.4 п.2 остаётся блокером сноса).

- [ ] **Step 18.4: AC5 — панель; чеки 45/46 + 10/20/30/40/60/65**

```bash
JAR=$(mktemp)
curl -fsS -c "$JAR" -o /dev/null -X POST http://localhost:5050/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'
curl -fsS -b "$JAR" http://localhost:5050/api/backups/storage | jq '{configured, api: .health.apiOk, live: .health.liveOk, cluster: .health.clusterOk, clusters: [.clusters[] | {cluster, shards: [.shards[] | .shard]}]}'
rm -f "$JAR"
# ожидание: configured/api/live=true, cluster=null; дерево demo/shard1|shard2 — реальные бэкапы
# live-пробы Patroni/SQL нод: /api/clusters/demo — masterLeaseAlive=true; /api/alerts — без probe-failed
cd dev-stand/adminpanel
checks/46-owns3.sh && checks/45-backups-storage.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh && checks/60-move-ops.sh && checks/65-metrics.sh
```

**Выход:** AC2–AC7 доказаны; стенд остаётся живым для Task 19.

---

## Фаза 4 — операционная миграция

### Task 19: Тестовый прогон docs/owns3-migration.md (§7 ранбука)

**Статус:** не начат.

**Вход:** Task 18 пройден; живых цепочек в as-minio нет (свежий стенд).

**Files:** нет (прогон ранбука §7; результат — в журнале задачи).

**Связь со spec:** §6 Фаза 4, §7.9, §5.4 (тестовый прогон).

- [ ] **Step 19.0: Bucket-сид в as-minio (ранбук §7 п.1)**

```bash
docker run --rm --entrypoint /bin/sh --network "$(docker inspect as-minio -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://as-minio:9000 minioadmin minioadmin >/dev/null \
      && mc mb --ignore-existing a/pgworker-backups" \
  || { echo "❌ bucket в as-minio не создан"; exit 1; }
```

(00-up.sh as-minio-bucket больше не сидит — без этого шага запись воркера упадёт.)

- [ ] **Step 19.1: Налив исходных цепочек (ранбук §7 п.2)**

Временно переключить `deploy/.env`: `PGW_BACKUP_S3_ENDPOINT=http://minio:9000`, креды `minioadmin/minioadmin` (as-minio — compose-DNS стендовой сети); рестарт `pgworker pgworker-2` (контур-override); дождаться ≥1 полного на шард COMPLETED + wal-объекты в `as-minio/pgworker-backups` (mc; наблюдение панель/etcd; планировщик снимает сам — не форсировать).

- [ ] **Step 19.2: Процедура ранбука §3 целиком (ранбук §7 п.3)**

mirror×3 (снаружи, `:9000`→`:9010`), переключение env на `http://owns3:9000` + рестарт, гейт полноты (`comm -23` пуст). Вывод гейта — в журнал задачи.

- [ ] **Step 19.3: Бесшовность**

`BROKEN: 0`; полные из 19.1 — те же id в list ownS3; wal-ключи ACTIVE; `fullsCount >= 1` шардов в панели; новых объектов в as-minio после переключения нет. (Команды — v2-паттерн Steps 13.3 с парсингом env.)

- [ ] **Step 19.4: Чистка тестовых цепочек (ранбук §7 п.4)**

```bash
# 1) .env — уже возвращён шагом 19.2 (owns3:9000 + writer-ключ) — сверить.
grep -E '^PGW_BACKUP_S3_ENDPOINT=|^PGW_BACKUP_S3_ACCESS_KEY=' deploy/.env
# 2) тестовые цепочки — из ОБОИХ хранилищ (только префиксы прогона):
docker run --rm --add-host host.docker.internal:host-gateway --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set o http://host.docker.internal:${OWNS3_PORT} '$OWNS3_ROOT_USER' '$OWNS3_ROOT_PASSWORD' >/dev/null \
      && mc rm --recursive --force o/pgworker-backups/demo/"
docker run --rm --entrypoint /bin/sh --network "$(docker inspect as-minio -f '…{{end}}')" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://as-minio:9000 minioadmin minioadmin >/dev/null \
      && mc rm --recursive --force a/pgworker-backups/demo/"
# 3) etcd-статусы сойдутся сами (супервизор/ретенция; не форсировать) —
#    наблюдение до чистых деревьев панели.
```

**Выход:** миграция доказана (AC9); стенд возвращён к рабочему состоянию (подсистема пишет в ownS3).

---

### Task 19b: Диагностика и ускорение конвергенции demo-кластера (бюджет ≤45 c)

**Статус:** не начат.

**Вход:** Task 18 пройден (живой стенд с каноническим demo, wait-гейт Step 13.4 в 00-up.sh); Task 13 исполнен. Целевой бюджет конвергенции (от `POST /api/clusters` до закрытия wait-гейта Active/ноды RUNNING/status-ключи сняты): **≤45 с**; 600 с в Step 13.4 — только аварийный потолок (сигнал разбора по логам, не норма).

**Files:** Modify (по месту находок): `dev-stand/adminpanel/checks/00-up.sh` (телеметрия wait-гейта; шаг поллинга); `src/PgWorker.App/appsettings*.json` / стендовые env-параметры (интервалы тика провижининга — только стендовые значения); при доказанной необходимости и отсутствии влияния на прод — точечные правки стендового пути движка (параллельный bootstrap).

**Interfaces:**
- Consumes: wait-гейт Step 13.4; процесс провижининга движка (arch/14); телеметрия `/tmp/pgw-e2e-artifacts-*`-паттерна → для стенда — журнал прогона 00-up.sh + `phases.log`.
- Produces: пофазовая телеметрия конвергенции в `phases.log` ([PHASE]-строки + суммарная строка времени); бюджет ≤45 с (или обоснование недостижимости с цифрами).

**Связь со spec:** §6 Фаза 3 (приёмка: полный цикл `00-up.sh` — конвергенция demo входит в метрику подъёма), §8 риск-строка «Конвергенция канонического demo в 00-up.sh — минуты | wait-гейт с бюджетом, [PHASE]-метки» — задача сводит риск к норме ≤45 с с цифрами по подфазам.

- [ ] **Step 19b.1: Инструментировать wait-гейт и подъём пофазово**

В `00-up.sh` вокруг/внутри wait-гейта Step 13.4 добавить телеметрию — `[PHASE]`-строка с таймингом КАЖДОЙ подфазы в журнал прогона и `phases.log` (каталог рядом с журналом; канон `docs/e2e-launch.md` — «никакой тихой фазы»):

подфазы: (1) создание сети/контейнеров движком (от 201-ответа до первого RUNNING-контейнера — по `docker ps --filter name=pgw-demo-`); (2) первый ответ Patroni REST каждой ноды (`curl :18xxx/cluster` по portalloc-портам из etcd); (3) появление leader-ключа `/clusters/demo/shards/shard*/master`; (4) готовность реплик streaming (`pg_stat_replication` через Patroni REST/API или SQL-пробу — по месту); (5) снятие движком `buckets/status/*`; (6) итоговый Active (закрытие `demo_ready`). Финал — суммарная строка: `demo: конвергенция N c (фазы: сеть/контейнеры a, patroni b, leader c, streaming d, status e)`.

Снять метку времени ДО `POST /api/clusters` и на каждом пороге (`date +%s` в переменные, разности в вывод).

- [ ] **Step 19b.2: Снять тайминги живого прогона, разложить по подфазам**

Прогон `90-down.sh -v && 00-up.sh` (фон, task_id, контроль ≤30 с шагами) → прочитать `phases.log`, разложить итог по подфазам, определить доминирующую (ожидаемо: bootstrap Spilo-нод или межтиковые паузы воркера — подтвердить/опровергнуть цифрами).

- [ ] **Step 19b.3: Проверить типовых подозреваемых (по коду/конфигам)**

Чек-лист (каждый пункт — цифра из 19b.1 или кода/конфига):
1. Шаг поллинга wait-гейта: `sleep 2` в цикле — не грубее 1–2 с (грубее — теряется до шага на каждой подфазе; при шаге 2 с и N порогах вклад мал — проверить).
2. Интервал тиков воркера между фазами провижининга: сколько секунд теряется на ожидании следующего тика процесса (интервал — appsettings PgWorker, провижининговый процесс; сверить по коду `src/PgWorker.App`); если фазы двигаются разными тиками с длинным интервалом — главный кандидат.
3. Последовательный vs параллельный bootstrap нод движком: провижинит ли движок 4 ноды по очереди (каждая — initdb-минуты Spilo?) или параллельно; при последовательном — вклад = Σ нод вместо max.
4. Patroni loop_wait/ttl дефолты Spilo (реф. 5 c/30 c): сколько циклов Patroni проходит до leader/реплики; стендовое сокращение — только через параметры стендового объявления (SPILO-configuration/env нод), не прод-канон.
5. Ретраи с фиксированными паузами (LongCallPolling/Ensure-циклы движка) — паузы в стендовом пути.
6. Лишние ожидания готовности сети/DNS перед create (контурная сеть уже существует — стендовый compose поднял её ДО deploy; движок ждать не должен).
7. Pull/сборка образов в момент create — не должно быть: спило-образ локален после `pull-images.sh`, wal/backup-образы собраны до создания кластера (проверить `docker events`/лог за прогон).

- [ ] **Step 19b.4: Устранить найденное**

По доминирующим подфазам: шаг поллинга wait-гейта → 1 с; стендовые интервалы тиков провижининга — сократить стендовым значением (env/appsettings стенда, прод-дефолты не трогать); параллельный подъём нод — оценить: если последовательный bootstrap доминирует, правка параллелизма в стендовом пути/параметре; правка, задевающая прод-механику, — только с обоснованием отсутствия влияния (юнит-таблицы + E2E Task 20 подтверждают прод-режим) и фиксацией в журнале задачи; снятие лишних ожиданий/ретраев — по месту. Каждое изменение — отдельный коммит с цифрой «до → после» в сообщении.

- [ ] **Step 19b.5: Повторный прогон — факт ≤45 c (или обоснование)**

`90-down.sh -v && 00-up.sh` → `phases.log`: суммарная конвергенция ≤45 с с пофазовыми цифрами. Если объективно недостижимо (аппаратное/образное узкое место — например, initdb 4 Spilo-нод на данном хосте) — зафиксировать в отчёте шага: итог, разложение по подфазам, причина-узкое место, прогноз на целевом хосте; бюджет 45 с в этом случае помечается в плане/журнале как обоснованно-недостижимый (600 с остаётся аварийным потолком wait-гейта).

**Выход:** конвергенция demo ≤45 с с пофазовой телеметрией (или обоснование с цифрами по подфазам); «тихих фаз» в прогоне нет.

**Проверка:** в `phases.log` прогона — пофазовые тайминги + суммарная строка ≤45 с (или отчёт-обоснование с цифрами); ожидания шага — ≤30 с на проверку.

---

## Фаза 5 — мерж-гейт

### Task 20: E2E PgWorker + полный прогон + roadmap

**Статус:** не начат.

**Вход:** Tasks 1–19 выполнены.

**Files:** Modify: `arch/roadmap/owns3.md` (снять тег t41).

**Связь со spec:** §6 Фаза 5, §9 (вкл. «PgWorker Unit+Integration без docker»), §7.1/§7.8.

- [ ] **Step 20.1: docker-E2E PgWorker (свежий Release; прод-режим сетей — контур не задан)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard --nologo
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~E2e --nologo    # полный E2eFixture (правки затрагивают provisioning/сети)
```

Ожидание: зелёные (E2E не задают контур-сеть → прод-режим → зелёность сохраняется — AC8). После КАЖДОЙ серии: финальная строка, зачистка `docker network ls | grep -E 'pgw-net|kfw-net'`, `docker network prune -f` (AGENTS.md — движковые сети). Телеметрия/диагностика упавших — по `docs/e2e-launch.md` (без перезапуска до анализа).

- [ ] **Step 20.2: Полные прогоны тестов (Unit + Integration без docker)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --nologo
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj --nologo
#   ^ без PGW_TEST_DOCKER: docker-кейсы — авто-skip (DockerTrait)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --nologo
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --nologo
```

- [ ] **Step 20.3: Roadmap-гейт**

В `arch/roadmap/owns3.md`: удалить пункт `t41-owns3-minio-migration`; во вступлении заменить фрагмент «ownS3 — целевое S3-хранилище подсистемы бэкапов: после приёмочной грани клиентами оно вытесняет MinIO (клиенты переключаются env-endpoint'ом, без изменений кода клиентов); миграция данных MinIO→ownS3 — задача `t41-owns3-minio-migration`.» на «ownS3 — действующее S3-хранилище подсистемы бэкапов (стенд — адресация по имени сети контура; as-minio — переходный read-only-архив до критерия сноса, `docs/owns3-migration.md`).». Проверка: `grep -rn "t41" arch/ docs/ --include="*.md" | grep -v superpowers | grep -v roadmap/README` — пусто.

- [ ] **Step 20.4: Финальная зачистка + коммит**

```bash
docker ps -a --format '{{.Names}}' | head; docker network prune -f
cd /Users/demakaev/ZCodeProject/worktrees/feat-t41-owns3-minio-migration
git add arch/roadmap/owns3.md && git commit -m "chore(t41): мерж-гейт — снятие тега t41-owns3-minio-migration с roadmap"
```

**Выход:** ветка готова к мержу (мерж в `main` — только по явной просьбе пользователя).

---

## Self-Review (выполнен автором плана)

1. **Покрытие spec v5:** §5.1 все 8 строк таблицы → Tasks 1–4; §5.2 PgWorker → Tasks 5–8 (опции+единая точка T5, драйвер T6, спеки T7, DSN T8; юнит-таблицы обоих режимов + идемпотентность 409); панель — исполнена (не трогаем); §5.3 деплой/стенд → Tasks 10–16; §5.4 (вкл. тестовый прогон) → T17; Фаза 3 → T18 (AC2–AC7; инспект AC4 — состав из `docker network inspect`, ровно-одна-сеть всем, отсутствие `pgw-net-demo`/`pgw-metrics`); Фаза 4 → T19 (AC9; сид+чистка по ранбуку §7); между фазами 4/5 — T19b: диагностика и ускорение конвергенции demo (бюджет ≤45 с, пофазовая [PHASE]-телеметрия в phases.log, 600 с — только аварийный потолок; закрывает риск-строку §8 «Конвергенция канонического demo — минуты» цифрами); Фаза 5 → T20 (E2E + Integration без docker + roadmap). Не-цели §2 соблюдены.
2. **Плейсхолдеры:** отсутствуют; вилок для NEEDS_CONTEXT нет — решения (override со сбросом обеих сетей, `BackupsRuntime()` единая точка, extra_hosts панели/воркеров — резолв advertised, bucket-сид as-minio в тестовом прогоне) зафиксированы в шапке и задачах.
3. **Консистентность:** `DockerOptions.ContourNetwork` → `BackupsRuntime()` (12 точек Program.cs перечислены в Interfaces T5, grep-контроль замены) → `BackupsRuntimeOptions.ContourNetwork` → все 4 спеки + DSN BackupProcess; `NodesNet()` — единая точка сетей драйвера; override согласован с AC4-инспектом (len==1); METRICS-ключи = стендовая сеть в env-шаблоне и sync-блоке; креды-примеры — v2-значения.
4. **Статусы исполнения:** v2-исполненные части помечены (13 коммитов; каноны v2-уровня дорабатываются; код панели — зелёный, изолирован); первые открытые шаги — T1.
5. **Ревизия по ревью Фазы 4 (10 findings, все закрыты):** F1 — единая точка `PgWorkerOptions.BackupsRuntime()` вместо точечных прокидываний (полный список 12 строк, Interfaces T5); F2 — override сбрасывает `default` И `pgw-metrics` (сервисы + top-level объявление), METRICS-sync/env — `adminpanel-stand-net`, AC4-инспект — len==1 всем + отсутствие обеих сетей; F3 — состав контейнеров из `docker network inspect`, полный перебор; F4 — `PgWorker.IntegrationTests` без `PGW_TEST_DOCKER` (DockerTrait авто-skip) в T9.1/T20.2; F5 — раздел «Тестовый прогон» §7 в ранбуке (T17) + Step 19.0 (bucket-сид as-minio) и Step 19.4 (чистка); F6 — `extra_hosts` панели СОХРАНЁН (резолв advertised, не контурный переход; зафиксировано в шапке-решении 5 и T4 §2.3); F7 — `git rm -r sidecar` в теле Step 12.1, противоречие из «Выход» снято; F8 — wait-гейт дополнен снятием `buckets/status/*` (arch/14: снятие = бакет ACTIVE); F9 — Step 2.4 дополнен правкой строки 903 arch/19; F10 — grep-проверки в Steps 16.2/17.3.
6. **Ревизия по код-ревью исполнения:** T15b — демонтаж эмуляторной модели доведён до мониторинга (джоба «patroni» hc*:8008 удалена из prometheus.yml, комментарий сети → adminpanel-stand-net; требование «patroni up ≥ 2» и упоминания эмуляторной/усыновлённой модели убраны из чека 65, мёртвая else-ветка снята; живой прогон 65 — в цепочке Step 18.4); impl-правки ревью — T8 (guard планового тика `nodeKv.Value.Object is null`, симметрия ~394, + юнит-кейс `PlannedTick_ContourDsn_OnlyCanonicalNode`), T12 (шапка docker-compose.yml под фактическое состояние; ссылка minio §7 → §4/§6), T13 (шапка 00-up.sh: publish-фазы → стенд → deploy-контур → POST /api/clusters).
