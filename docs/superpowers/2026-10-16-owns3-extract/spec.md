# Spec: вынос проекта ownS3 из монорепо pg в самостоятельный репозиторий ../OwnS3

Дата: 2026-10-16. Worktree: `refactor-owns3-extract`.

## 1. Цель

Вынести проект **ownS3** (собственное S3-совместимое объектное хранилище:
код, тесты, канон `arch/`, документацию, историю задач) из монорепо `pg` в
самостоятельный репозиторий `/Users/demakaev/ZCodeProject/OwnS3` (далее
`../OwnS3`). После выноса:

- `../OwnS3` — полностью самостоятельный проект: своя сборка, свои тесты,
  свой канон, свой `AGENTS.md`, свой скрипт сборки docker-образа. Сервис
  ownS3 **не зависит от pg и его проектов никак** — ни кодом, ни сборкой.
- Все, кто зависит от ownS3 (S3Worker, подсистема бэкапов PgWorker,
  AdminPanel), остаются в монорепо `pg` и обращаются к ownS3 **только через
  API контейнера** (docker-образ `owns3:dev` + S3 HTTP API / healthz). Ни
  одной `ProjectReference` из проектов pg на проекты OwnS3 не остаётся.
- Репозиторий `pg` меняет ссылки на новый проект: образ `owns3:dev` больше
  не собирается из исходников pg — pg использует готовый образ,
  собранный `../OwnS3` (fail-fast с подсказкой, если образ не собран).

## 2. Контекст: периметр ownS3 в монорепо pg (по факту кодовой базы)

### 2.1. Что выносится (периметр ownS3)

**Код** (`src/`):
- `OwnS3.App`, `OwnS3.Pki`, `OwnS3.Protocol`, `OwnS3.Replication`,
  `OwnS3.Storage` — 5 проектов хранилища.
- `tests/OwnS3.UnitTests` (юниты: SigV4, storage, erasure, peer-auth, …),
  `tests/OwnS3.IntegrationTests` (Api-сценарии на WebApplicationFactory +
  E2e-сценарии за `PGW_TEST_DOCKER=1`, Testcontainers, self-build образа).

**Переиспользуемые библиотеки — дублируются** (по решению пользователя,
только используемые OwnS3):
- `Shared.Core` (нужен Protocol/Storage/Replication/App),
  `Shared.Metrics` (нужен App), `Shared.Tls` (нужен App).
  `Shared.Etcd` и `Shared.Docker` НЕ нужны: ownS3 — standalone-сервис без
  etcd и без docker.sock — и НЕ копируются.
- Тесты дублей: `tests/Shared.Core.UnitTests`,
  `tests/Shared.Metrics.UnitTests` (тестов Shared.Tls в pg нет).
- Корневые build-файлы: `src/Directory.Build.props`,
  `src/Directory.Packages.props`, `src/NuGet.Config` — копии as-is.

**Канон и документация**:
- `arch/22-owns3.md` (вводная канона) + `arch/owns3/{01..07}*.md` (главы:
  overview, operations, protocol, storage, access-config, erasure,
  replication).
- `arch/roadmap/owns3.md` (+ адаптированный `arch/roadmap/README.md`).
- `docs/owns3-migration.md` (runbook MinIO→ownS3),
  `docs/owns3-replication-requirements.md`, `docs/s3-replication.md`.
- История задач: каталоги `docs/superpowers/*owns3*` (t35–t42 и др.;
  S3Worker-задачи t31–t34, t33, t40-s3-cluster-governance — остаются в pg).

**Контейнер**:
- `docker/OwnS3.Dockerfile` (runtime-слой, publish на хосте).

### 2.2. Что остаётся в pg и как переключается на новый проект

- **S3Worker** (`src/S3Worker.*` + тесты + `arch/23-s3-clusters.md`,
  `arch/24-s3worker.md`, `arch/roadmap/s3.md`) — оркестратор S3-кластеров,
  четвёртый воркер монорепо. Остаётся в pg целиком.
- **Кодовая связь, которую надо разорвать**: `S3Worker.Core` →
  `OwnS3.Protocol` (ProjectReference). Использование по факту:
  - `OwnS3.Protocol.Auth` — SigV4-подпись GET-запросов к нодам
    (`S3Worker.Core/OwnS3/{OwnS3NodeClient,SigV4GetSigner}.cs`);
  - `OwnS3.Protocol.PeerAuth` — PKI peer-сертификатов репликации
    (`S3Worker.Core/OwnS3/PkiMaterial.cs`,
    `S3Worker.Provisioning/{InstanceEnvComposer,PkiStore}.cs`);
  - тесты S3Worker дополнительно используют `OwnS3.Protocol.Errors`,
    `OwnS3.Protocol.Requests`.
- **Сервисные связи — не меняются** (уже через API контейнера):
  подсистема бэкапов PgWorker (AWSSDK.S3, env-endpoint, S3-агностична —
  arch/19), AdminPanel (`AdminPanel__Backups__S3__Endpoint`), панель
  «Хранилище бэкапов» (MinioS3Client), etcd-контракт `/s3/` + `/s3worker/`.
- **deploy/dev-stand pg**: сервисы `owns3`, `owns3-ec` (профиль) в
  `deploy/docker-compose.yml` + алиас в `docker-compose.contour.yml` —
  остаются (ownS3 — сервис контура pg), но переключаются с `build:` на
  готовый `image: owns3:dev`. `deploy/.env.example` (`OWNS3_*`) — остаётся
  (конфиг сервиса в контуре pg). `deploy/tls/gen.sh` — серт `owns3`
  остаётся (TLS-грань сервиса контура pg).
- **Чеки стенда**: `dev-stand/adminpanel/checks/46-owns3.sh`,
  `47-owns3-ec.sh` — остаются в pg (интеграционные чеки контура pg с
  внешним образом). `00-up.sh` — publish-фаза OwnS3.App удаляется,
  добавляется fail-fast-проверка наличия образа `owns3:dev`.
- **S3Worker E2E** (`S3E2eEnvironment`): сам собирает `owns3:dev`
  (publish из src pg + docker build) — переключается на проверку наличия
  готового образа с fail-fast «собери в ../OwnS3».

### 2.3. Исходное состояние цели

`/Users/demakaev/ZCodeProject/OwnS3` существует: выполнен `git init`,
ветка `main`, коммитов нет. История — свежая (первый коммит = полный
снапшот вынесенного проекта); перенос истории git (filter-repo/subtree)
НЕ выполняется — история разработки остаётся в git-логе репозитория pg.

## 3. Решения пользователя (зафиксированы вопрос-ответом)

| Вопрос | Решение |
|---|---|
| Периметр | Выносится ownS3 (хранилище) и его зависимости; зависящие от ownS3 остаются в pg, доступ — только через API контейнера |
| История git | Свежая (git init уже сделан) |
| Образ owns3:dev | Собирает `../OwnS3`; pg использует готовый образ (fail-fast с подсказкой) |
| Доки/история задач | Переносятся в `../OwnS3`, из pg удаляются |
| Дубли Shared | Только используемые: Core, Metrics, Tls (+ тесты, + build-файлы) |
| Чеки 46/47 стенда | Остаются в pg |

## 4. Принципы

1. **arch-first**: сначала канон переносится/адаптируется (и правится
   arch/ pg), затем код. Канон переносится **as-is** с адаптацией только
   путей/ссылок (никакого пересказа и «улучшений» канона).
2. **Никакого рефакторинга кода при переносе**: копия = копия (за
   исключением механических правок путей/ссылок сборки). Изменение
   поведения — вне рамок задачи.
3. **Самостоятельность**: `../OwnS3` не ссылается на pg ни кодом, ни
   сборкой, ни скриптами. Общее рабочее окружение (базовые правила
   `../AGENTS.base.md`, референс `../minio`, подходы `../Puzzle`) —
   файловые ссылки того же уровня каталогов, не связи с проектом pg.
4. **pg знает о OwnS3 минимум**: имя образа `owns3:dev`, S3 API, healthz,
   ключи/конфиг сервиса. Внутреннее устройство OwnS3 (структура проектов,
   сборка) из скриптов pg не видно.
5. **Каноны E2E/тестов сохраняются** в обоих репо: динамические порты,
   полная самоочистка, guid-изоляция, телеметрия прогона, `BrokerBootSec`
   ≤ 100 с, сборка образов на хосте с publish-COPY.
6. **Registry-правило**: `owns3:dev` — локально собираемый образ, в
   registry `192.168.0.1:5000` не попадает никогда (правило действует в
   обоих репо). Внешние образы (aspnet:10.0, `mc`, testcontainers-зависимости)
   — из локального registry по ранбуку.

## 5. Целевая структура ../OwnS3

```
OwnS3/
├── AGENTS.md                  ← копия AGENTS.md pg, исправленная (см. §5.3)
├── README.md                  ← новый: назначение, сборка, тесты, образ, standalone-запуск
├── arch/
│   ├── README.md              ← точка входа канона (адаптированная)
│   ├── 01-owns3.md            ← бывший arch/22-owns3.md (вводная канона)
│   ├── owns3/{01..07}.md      ← главы канона as-is
│   └── roadmap/
│       ├── README.md          ← правила ведения (адаптация)
│       └── owns3.md           ← трек задач ownS3
├── docs/
│   ├── owns3-migration.md
│   ├── owns3-replication-requirements.md
│   ├── s3-replication.md
│   ├── runbook.md             ← минимальный ранбук OwnS3: локальный registry, зеркалирование внешних образов, сборка owns3:dev
│   └── superpowers/           ← перенесённая история owns3-задач
├── src/
│   ├── Directory.Build.props  ← копия as-is
│   ├── Directory.Packages.props ← копия as-is
│   ├── NuGet.Config           ← копия as-is
│   ├── OwnS3.slnx             ← НОВЫЙ solution
│   ├── OwnS3.{App,Pki,Protocol,Replication,Storage}/
│   ├── Shared.{Core,Metrics,Tls}/   ← дубли
│   └── tests/
│       ├── OwnS3.UnitTests/
│       ├── OwnS3.IntegrationTests/
│       ├── Shared.Core.UnitTests/
│       └── Shared.Metrics.UnitTests/
├── docker/
│   └── OwnS3.Dockerfile       ← копия as-is
└── deploy/
    ├── build-image.sh         ← сборка owns3:dev (publish + docker build, [PHASE]-логи)
    ├── docker-compose.yml     ← standalone-контур ownS3 (сервис + тома + env + healthz)
    ├── .env.example           ← standalone-переменные (OWNS3_*)
    └── tls/gen.sh             ← генерация TLS-серта ownS3 (минимальная версия из deploy/tls/gen.sh pg)
```

### 5.1. OwnS3.slnx

Новый solution (по образцу структуры PgWorker.slnx):
- `/common/`: Directory.Build.props, Directory.Packages.props,
  Shared.Core, Shared.Metrics, Shared.Tls;
- `/owns3/`: 5 проектов OwnS3.*;
- `/tests/`: OwnS3.UnitTests, OwnS3.IntegrationTests,
  Shared.Core.UnitTests, Shared.Metrics.UnitTests.

### 5.2. deploy/ нового репо

- `build-image.sh`: `dotnet publish src/OwnS3.App -c Release -o
  artifacts/owns3/publish` + `docker build -f docker/OwnS3.Dockerfile -t
  owns3:dev artifacts/owns3/publish` — с `[PHASE]`-метками и таймингом в
  лог (канон стиля 00-up.sh). Скрипт — единственный способ собрать образ
  для потребителей (pg).
- `docker-compose.yml` (standalone): сервис `owns3` на образе
  `owns3:dev`, порт S3-грани 9000 (publish на хост по `.env`), healthz,
  том `/data`, env из `.env.example` (`OWNS3_ROOT_USER/PASSWORD`,
  `OWNS3_ACCESS_KEYS__*`, `OWNS3_DATA_DIR`, TLS-опции). Профиль
  `owns3-ec` (erasure, 4 тома) — переносится как опция standalone-контура
  по образцу pg. Требование к контуру: сервис без etcd, без docker.sock.
- `.env.example`: dev-значения по образцу pg (`deploy/.env.example`),
  fail-fast на пустые обязательные переменные — как в pg.
- `tls/gen.sh`: только owns3-часть генератора pg (SAN по контуру
  standalone); если выделение нетривиально — минимальный генератор
  self-signed CA+серт для OWNS3_TLS_CERT_PATH/KEY_PATH.

### 5.3. AGENTS.md нового репо (копия pg, исправленная)

- Ссылка на базовые правила: `[../AGENTS.base.md](../AGENTS.base.md)` —
  та же относительная позиция, что у pg (общие рабочие правила
  пользователя, не связь с проектом pg).
- **Убирается** вся специфика pg: блоки PgWorker/KafkaWorker/ValkeyWorker,
  AdminPanel, dev-stand (панель, as-etcd, checks), E2E-маркеры
  PgWorker.slnx, roadmap-указатель треков pg, перенос t34-домена.
- **Остаётся/адаптируется**: .NET 10-блок (LangVersion, Nullable,
  TreatWarningsAsErrors, Directory.Packages.props); каноны docker-портов
  (динамические, BrokerBootSec ≤ 100 с), зачистки контейнеров/сетей,
  полноты teardown E2E, телеметрии E2E (`[PHASE]`, MarkFailed, артефакты
  `/tmp/…`), per-cluster сетей; блок «сборка образов на хосте, publish-COPY
  в контейнер» — с примерами путей OwnS3; registry-правила (внешние из
  `192.168.0.1:5000`, `owns3:dev` в registry не класть); multi-host
  надёжность (переформулирована под ownS3: гарантии строятся от
  реплицированных источников — erasure-сеты и mesh-репликация, локальный
  том — кэш без durability); Puzzle-блок (`../Puzzle`); roadmap-правила
  (arch/roadmap/).
- **Добавляется**: OwnS3 — standalone-сервис, запускается всегда в
  docker (standalone-compose или как сервис чужого контура); потребители
  (pg) работают с ownS3 только по S3 API/healthz и образу `owns3:dev`
  (сборка — `deploy/build-image.sh`); решение о смене образа/грани —
  через канон arch/ и без ломки S3-стандарта (критерий правильности —
  стандарт S3 API).

### 5.4. README.md нового репо

Назначение (S3-совместимое хранилище, критерий — стандарт S3 API,
MinIO — референс), quick start: сборка (`dotnet build src/OwnS3.slnx`),
тесты (`dotnet test`; E2e за `PGW_TEST_DOCKER=1`), образ
(`deploy/build-image.sh`), запуск (standalone-compose), ссылка на
`arch/01-owns3.md` и `arch/README.md`.

## 6. Изменения в репозитории pg (arch-first)

### 6.1. arch/ (первая фаза)

- **Удаляются**: `arch/22-owns3.md`, `arch/owns3/` (каталог),
  `arch/roadmap/owns3.md`. Нумерация остальных файлов НЕ сдвигается
  (23, 24 остаются с прежними номерами — перенумерация ломала бы ссылки
  в комментариях кода S3Worker вида «arch/24 §5 F»).
- `arch/README.md`: удалить пункт «22-owns3.md» из дерева и списка
  литературы; в дереве/списке остаются 23-s3-clusters, 24-s3worker.
- `arch/19-backups.md`: ссылки на `arch/owns3/05`,
  `docs/owns3-migration.md` заменить на указание внешнего репозитория
  OwnS3 (`../OwnS3`, канон — там); формулировки «действующее хранилище
  ownS3» сохраняются — меняется только адрес канона.
- `arch/23-s3-clusters.md`, `arch/24-s3worker.md`: упоминания
  `OwnS3.Protocol` как общей сборки — заменить на внутреннюю клиентскую
  копию (см. §6.2); упоминания owns3-канона (`arch/owns3/…`) — на
  `../OwnS3`; образ `owns3:dev` описать как внешний (собирается
  репозиторием OwnS3).
- `arch/roadmap/README.md` и `arch/roadmap/s3.md`: убрать упоминания
  трека `owns3.md` (трек уехал).

### 6.2. src/ pg — разрыв кодовой связи

- В `S3Worker.Core` заводится **внутренняя клиентская копия протокола**:
  каталог `src/S3Worker.Core/OwnS3/Protocol/` c файлами, скопированными
  из `OwnS3.Protocol` (namespace `OwnS3.Protocol.*` сохраняется — минимум
  диффа, это имена типов протокола, а не ссылка на сборку). Точный
  состав копии — по факту использования (фиксируется в плане):
  `Auth` (SigV4-ядро/подпись), `PeerAuth` (PKI: CA, peer-сертификаты,
  подпись join-запросов), используемые тестами части `Errors`,
  `Requests`, и их внутренние зависимости.
- Удаляются `ProjectReference` на `OwnS3.Protocol` из
  `S3Worker.Core.csproj` (единственная кодовая связь pg→OwnS3). Если
  копии нужны `System.IO.Hashing` и другие пакеты — добавляются в
  S3Worker.Core.csproj из Directory.Packages.props (версии централизованы).
- Пакеты `AWSSDK.S3` в S3Worker.Core остаются как есть.
- Правило для будущего: клиентская копия в pg развивается независимо;
  канон протокола — в `../OwnS3/arch/owns3/03-protocol.md` (и 07 для
  peer-auth), pg-копия обязана ему соответствовать.

### 6.3. src/ pg — удаление и solution

- Удаляются: `src/OwnS3.{App,Pki,Protocol,Replication,Storage}/`,
  `src/tests/OwnS3.{UnitTests,IntegrationTests}/`.
- `src/PgWorker.slnx`: удалить папку `/owns3/` (5 проектов) и проекты
  тестов OwnS3 из `/tests/`. Остальные проекты (включая все S3Worker.*,
  Shared.*, Metrics.SdGenerator) — без изменений.

### 6.4. deploy/ и dev-stand/ pg

- `deploy/docker-compose.yml`: сервисы `owns3` и `owns3-ec` — убрать
  `build:` (готовый `image: owns3:dev` остаётся); комментарий-инструкция
  «образ собирается репозиторием ../OwnS3 (deploy/build-image.sh)».
  Env-блоки `OWNS3_*` без изменений. `docker-compose.contour.yml` — без
  изменений (алиасы).
- `deploy/.env.example`: без изменений (OWNS3_*/S3W_* — конфиг сервисов
  контура pg).
- `deploy/tls/gen.sh`: без изменений (серт owns3 — часть TLS-пакета
  контура pg).
- `docker/OwnS3.Dockerfile` — удаляется из pg (уехал в ../OwnS3).
- `dev-stand/adminpanel/checks/00-up.sh`: удалить publish-фазу
  `OwnS3.App`; перед подъёмом контура — fail-fast-проверка
  `docker image inspect owns3:dev` с сообщением-подсказкой («собери в
  ../OwnS3: deploy/build-image.sh»).
- Чеки `46-owns3.sh`, `47-owns3-ec.sh` — без изменений (проверяют
  внешний образ в контуре pg).
- `dev-stand/images/images.txt` — без изменений (внешние образы).

### 6.5. S3Worker E2E

- `src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`:
  `EnsureImageAsync(OwnS3Image, publish+build)` заменить на проверку
  наличия локального образа `owns3:dev` (docker image inspect) c
  fail-fast и командой сборки из `../OwnS3`; обновить комментарии
  бисект-режима (`PGW_TEST_E2E_NOBUILD`): owns3:dev всегда внешний.
  Канон «пересборка каждый прогон» для owns3-образа меняется на
  «внешний статический ассет» (freshness контролирует разработчик
  пересборкой в ../OwnS3); s3worker:e2e-образ остаётся self-build.

### 6.6. AGENTS.md pg

- В блоке образов: `owns3:dev` исключить из числа собираемых pg и
  указать как внешний (репозиторий `../OwnS3`, сборка
  `deploy/build-image.sh`, в registry не класть).
- Прочие блокы pg (стенд, E2E-маркеры, roadmap-треки) — без изменений;
  трек owns3 удалён из roadmap-указателей правкой §6.1.

### 6.7. docs/ pg

- Удаляются: `docs/owns3-migration.md`,
  `docs/owns3-replication-requirements.md`, `docs/s3-replication.md`,
  каталоги `docs/superpowers/*owns3*` (перенесены в ../OwnS3).
- Файлы pg, ссылающиеся на перенесённые доки (например
  `docs/adminpanel/*`, `dev-stand/adminpanel/README.md`): ссылки
  заменить на `../OwnS3` (полный список ссылок фиксируется в плане
  grep'ом).

## 7. Подходы (выбор выполнен)

- **A. Единая задача, один dev-flow (выбрано)**: в одной ветке
  worktree pg выполняются и наполнение `../OwnS3` (файловые операции +
  initial commit в отдельный git), и чистка pg с разрывом связи.
  Плюсы: один мерж-гейт, нет окна жизни задвоенного кода, приёмочные
  проверки обоих репо проходят одновременно. Минусы: большой диф —
  mitigируется фазированием и тем, что переносы — копии as-is.
- **B. Две последовательные задачи** (сначала собрать и приёмить
  ../OwnS3, затем чистить pg): отклонён — между задачами код ownS3 живёт
  в двух репо, любое изменение приходится делать дважды; второй мерж
  тяжелее из-за накопленного дрейфа.
- **C. Перенос истории git (filter-repo/subtree)**: отклонён
  пользователем — свежая история.

## 8. Фазы (каркас для плана)

1. **Фаза 0 — arch-first**: перенос канона в `../OwnS3/arch` (as-is,
   адаптация ссылок/путей; `22-owns3.md` → `01-owns3.md`); перенос
   roadmap/owns3; правки arch/ pg (§6.1). Проверка: ссылки в arch/ pg не
   указывают на удалённые файлы.
2. **Фаза 1 — репозиторий ../OwnS3**: копирование кода/тестов/дублей
   Shared/build-файлов, OwnS3.slnx, docker/, deploy/ (build-image.sh,
   standalone-compose, .env.example, tls), AGENTS.md, README.md, перенос
   docs/ + superpowers-истории. Проверка: `dotnet build src/OwnS3.slnx`
   и `dotnet test` зелёные; юниты — обязательно, интеграции/E2e
   (`PGW_TEST_DOCKER=1`) — по канонам с зачисткой; сборка образа
   `deploy/build-image.sh` успешна; standalone-compose поднимается и
   healthz зелёный; teardown чист.
3. **Фаза 2 — разрыв связи в pg**: клиентская копия протокола в
   S3Worker.Core (§6.2), удаление ProjectReference, правка slnx
   (ProjectsReference OwnS3 из тестов S3Worker тоже — тесты переходят на
   копию). Проверка: сборка pg зелёная, тесты S3Worker зелёные.
4. **Фаза 3 — чистка pg**: удаление OwnS3-проектов/тестов/доков/
   roadmap/superpowers-истории, правки deploy/00-up.sh/S3Worker-E2E/
   AGENTS.md (§6.3–6.7). Проверка: `grep -r "OwnS3" src/` в pg показывает
   только S3Worker-клиентскую копию; `grep -ril owns3 docs/ arch/` —
   только легитимные указатели на ../OwnS3.
5. **Фаза 4 — приёмка**: полный стенд pg с предварительно собранным из
   ../OwnS3 образом (00-up.sh → чеки 10/46/47/75/80 → 90-down.sh);
   отрицательный тест: без образа owns3:dev — 00-up.sh падает с понятной
   подсказкой; `dotnet test` обоих репо; initial commit в `../OwnS3`
   (полный снапшот); roadmap-гейт (тег задачи удалён из архива).
6. **Фаза 5 — ревью и мерж**: ревью диффа pg (перенос-копии читаются по
   спискам файлов), мерж в main по стандартному гейту; push — по
   отдельному требованию.

## 9. Ограничения и риски

- **Копии расходятся навсегда**: Shared.{Core,Metrics,Tls} в ../OwnS3 и
  pg — независимые ветки одного кода; синхронизация не предусматривается
  (решение пользователя — дублирование). Клиентская копия протокола в
  S3Worker.Core — тоже: канон один (arch/owns3/03,07 в ../OwnS3),
  соответствие — забота разработчика.
- **Аккуратность состава копии протокола**: недобор классов сломает
  сборку (ловится компиляцией), перебор — лишний мёртвый код в pg
  (ловится ревью по фактическому использованию).
- **E2E-свежесть образа**: pg-тесты/стенд больше не пересобирают
  owns3:dev автоматически — устаревший образ может маскировать регрессии
  (урок t09). Митигация: fail-fast-подсказки с командой пересборки;
  в README ../OwnS3 — правило пересборки перед прогонами.
- **Publish-вывод и артефакты**: каталоги `artifacts/` в git обоих репо
  не попадают (gitignore копируется/проверяется).
- **NuGet.Config**: копия as-is; доступность лент (локальный registry)
  обязательна для сборки — фиксируется в README ../OwnS3.
- **Порядок коммитов**: initial commit ../OwnS3 выполняется после
  приёмки Фазы 1 (сборка+тесты+образ), до мержа pg — чтобы к моменту
  удаления кода из pg новый репо уже был дееспособен.
- В рамках задачи НЕ выполняются: перенумерация arch/ pg, переименование
  сборок/namespace'ов, изменение версий пакетов, чистка
  Directory.Packages.props от неиспользуемых версий (копия as-is).

## 10. Критерии приёмки

1. `../OwnS3`: `dotnet build src/OwnS3.slnx` и `dotnet test` зелёные;
   `deploy/build-image.sh` собирает `owns3:dev`; standalone-compose
   поднимается, healthz отвечает, teardown чист. Свежий initial commit в
   `main` репозитория ../OwnS3.
2. `../OwnS3` не содержит ссылок на pg: ни `ProjectReference` на
   проекты pg, ни путей вида `../pg` в сборке/скриптах/конфигурации
   (исторические упоминания в перенесённом архиве `docs/superpowers/`
   допустимы; `../AGENTS.base.md`, `../minio`, `../Puzzle` — допустимы).
3. pg: сборка и `dotnet test` зелёные без OwnS3-проектов; в pg нет ни
   одной `ProjectReference` на `OwnS3.*`; каталогов
   `src/OwnS3.*`, `tests/OwnS3.*`, `arch/owns3/`, `arch/22-owns3.md`,
   `arch/roadmap/owns3.md`, `docs/owns3-*.md`, `docs/s3-replication.md`,
   `docs/superpowers/*owns3*`, `docker/OwnS3.Dockerfile` не существует.
4. pg: сервисы `owns3`/`owns3-ec` в deploy-compose используют готовый
   образ без `build:`; `00-up.sh` падает с понятной подсказкой при
   отсутствующем образе; с собранным из ../OwnS3 образом полный стенд
   зелёный, включая чеки 46, 47, 75, 80.
5. E2E-прогон S3Worker (маркерные сценарии свежего Release с внешним
   `owns3:dev`) зелёный; юниты/интеграции S3Worker зелёные на клиентской
   копии протокола.
6. arch/ pg не содержит ссылок на удалённые owns3-доки; упоминания
   ownS3-канона ведут в `../OwnS3`.
7. Оба AGENTS.md соответствуют своим репозиториям (в pg — внешний образ
   owns3:dev; в ../OwnS3 — standalone-каноны, без pg-специфики).
