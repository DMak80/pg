# Spec: t31-vwk-contour-network — контурный режим сетей ValkeyWorker/S3Worker; стенд = ядро + панель

## 1. Цель

1. **Контурный режим сетей ValkeyWorker** — по образцу PgWorker
   ([arch/14](../../../../arch/14-pgworker.md) §2.1) и S3Worker
   ([arch/24](../../../../arch/24-s3worker.md) §2): опция
   `ValkeyWorker:Docker:ContourNetwork` — задана → контейнеры vwk-нод
   создаются в ОДНОЙ сети контура (стенд — единая стендовая сеть
   `adminpanel-stand-net`); пусто (прод/умолчание) — прежнее поведение
   (default bridge docker-хоста запуска). vwk-ноды перестают быть
   единственными контейнерами стенда вне стендовой сети.
2. **S3Worker — выравнивание Swarm-режима** с каноном PgWorker: заданная
   `ContourNetwork` при `Mode=Swarm` сейчас тихо игнорируется — станет
   fail-fast старта (сеть overlay в Swarm назначает сам Swarm; опция сети
   в Swarm — ошибка конфигурации).
3. **Стенд поднимает только ядро и панель, БЕЗ сидинга ресурсов**:
   `00-up.sh` не создаёт ресурсы (PG-кластеры, S3-пулы, valkey/kafka
   демо), не наливает никакие сиды и не поднимает сервисы ресурсов
   (`owns3`, `as-minio`). Вся наливка демо из `00-up.sh` просто
   УДАЛЯЕТСЯ — никуда не переезжает. Ресурсы создаются оператором
   вручную через панель — и только так. Существующие `05-seed.sh` (ветки
   `kafka`/`valkey`) и чеки `10`–`90` не меняются; для приёмки контурного
   режима на стенде пишется СВОЙ новый скрипт (§3.6).

Дополнительную сеть указывают ТОЛЬКО тесты и подъём стенда;
прод-конфигурация сеть не указывает (пустые дефолты).

## 2. Принципы

- **Приоритет №1 — E2E не меняется ВООБЩЕ** (директива пользователя):
  задача не добавляет, не убирает и не меняет в E2E НИЧЕГО — ни файлы
  тестов, ни env-словари E2E-окружений, ни команды/фильтры/порядок
  запуска серий, ни E2ePrepare, ни покрытие (проверка контурного режима
  в E2E НЕ добавляется). E2E остаётся ровно таким, какой есть. Любая
  проверка нового режима — ТОЛЬКО вне E2E: юнит-тесты и стенд (свои
  скрипты).
- **Arch-first**: контракты обновлены до кода — `arch/21` §2 «Сеть» и §8
  (опция + fail-fast Swarm), `arch/24` §2 (fail-fast Swarm),
  `arch/adminpanel/04` (ядро+панель на пустом контуре; ресурсы — только
  вручную через панель; `05-seed.sh` — не часть штатного подъёма; новый
  чек `52-valkey-contour.sh`). Код зеркалит arch; новых штатных операций
  сверх arch не выдумывать.
- **Никакого сидинга ресурсов на стенде**: стендные скрипты подъёма не
  создают ресурсов и не наливают сиды; `05-seed.sh` не расширяется
  (ветки `pg`/`s3` НЕ добавляются) и не удаляется — остаётся как есть.
- **Сеть контура принадлежит поставке/сценарию**: воркер её не создаёт
  и не удаляет; vwk-нода получает сеть при СОЗДАНИИ контейнера
  (`ContainerSpec.Network`); существующие контейнеры сетью не трогаются.
- **Advertised-правило не меняется** (arch/21 §2): endpoints/portalloc —
  host-публикации (`AdvertisedClientHost`), контурная сеть НЕ меняет
  клиентскую адресацию — она консолидирует контейнеры контура в одной
  сети.
- **Симметрия с S3Worker** (ближайший домен-аналог: ноды без inter-node
  трафика): опция приезжает срезом опций процессов
  (`ValkeyProvisioningOptions.ContourNetwork`) и параметром
  `EnsureNodeAsync(spec, contourNetwork, ct)`.
- **Swarm**: за сеть отвечает сам Swarm — опция `ContourNetwork` в
  `Mode=Swarm` есть ошибка конфигурации → fail-fast старта воркера
  (единообразно: PgWorker — эталон, S3Worker выравнивается, ValkeyWorker
  вводится сразу с fail-fast).
- **Стенд ≠ тесты**: существующие чеки стенда не переделываются; чеки,
  требующие демо-ресурсов, запускаются после ручного создания ресурсов
  через панель; новые проверки — новыми своими скриптами, не правками
  старых.

## 3. Структура / компоненты

### 3.1. Arch (внесено в worktree этой же задачей)

| Файл | Правка |
|---|---|
| `arch/21-valkeyworker.md` §2 «Сеть» | Контурный режим: опция `ContourNetwork` (стенд/E2E-инфраструктура сценария), сеть не создаётся/не удаляется воркером, advertised/portalloc не меняются, пусто — default bridge, Mode=Swarm — fail-fast старта |
| `arch/21-valkeyworker.md` §8 | `ValkeyWorker:Docker { …, ContourNetwork=null }` |
| `arch/24-s3worker.md` §2 | Дополнение к опции `ContourNetwork`: не поддерживается в Mode=Swarm — fail-fast старта |
| `arch/adminpanel/04-local-stand.md` | Шапка (ядро+панель на пустом контуре; ресурсы — только вручную через панель); §1: таблица профилей, абзац «00-up.sh поднимает ТОЛЬКО ядро и панель… никакие сиды штатным подъёмом не наливаются», таблица портов, env грани «Хранилище бэкапов» → `http://s3w-backups-node1:9000`; §2.2: ресурсы — только вручную через панель, `05-seed.sh` — не часть штатного подъёма; §2.3: стендовая сеть без standalone-owns3; §2.4: валkey-сид — вручную/чеком 51, штатный подъём не зовёт; §3: описания `00-up.sh`/`05-seed.sh` + НОВАЯ строка `52-valkey-contour.sh`; §5: быстрый старт |

### 3.2. ValkeyWorker — контурный режим (код, вне E2E)

- `src/ValkeyWorker.App/Options.cs`: `DockerOptions.ContourNetwork`
  (`string?`; коммент-канон по образцу `S3Worker.App/Options.cs`).
- `src/ValkeyWorker.App/Program.cs`:
  - валидация fail-fast: `Mode=Swarm` + непустой `ContourNetwork` →
    `ApplicationException` (текст по образцу PgWorker: «…is not
    supported in Mode=Swarm — contour network mode is implemented for
    plain containers»);
  - нормализация: пусто → `null`;
  - прокладка в срез процессов (как `S3Worker.App/Program.cs` передаёт
    `opts.Docker.ContourNetwork` в `S3ProvisioningOptions`).
- `src/ValkeyWorker.Provisioning/Processes/ProcessCommon.cs`:
  `ValkeyProvisioningOptions` += `string? ContourNetwork = null`
  (опциональный параметр, как `S3ProvisioningOptions`).
- `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`:
  - `IClusterDriver.EnsureNodeAsync(ValkeyNodeSpec spec, string?
    contourNetwork, CancellationToken ct)` — сигнатура по образцу S3;
    call-sites (4 шт.: `ProvisioningProcess`, `NodeSupervisor`,
    `CaRotator`, `TlsMigrator`) передают опцию из среза;
  - `PlainClusterDriver.EnsureNodeAsync`: в `ContainerSpec` нового
    контейнера добавляется `Network: contourNetwork` (существующий
    контейнер — без изменений, идемпотентность прежняя);
  - `SwarmClusterDriver.EnsureNodeAsync`: параметр принимается и не
    используется (fail-fast старта исключает комбинацию; комментарий в
    коде).
- Юнит-тесты `src/tests/ValkeyWorker.UnitTests` (юнит-проект, НЕ E2E;
  по образцу `S3Worker.UnitTests/Docker/ClusterDriverTests.cs`):
  - Plain: `contourNetwork: "contour"` → `CreatedSpec.Network ==
    "contour"`; `null` → `Network == null` (прежнее поведение);
  - валидация старта: Swarm+ContourNetwork → исключение (по механизму
    существующих тестов валидации VWK).
- **Файлы E2E-тестов ValkeyWorker не трогаются** (включая
  `ValkeyE2eEnvironment` и его env-словарь): E2E продолжает работать с
  нодами в default bridge — прежнее поведение (опция не задана).

### 3.3. S3Worker — fail-fast Swarm (код)

- `src/S3Worker.App/Program.cs`: в валидацию старта — `Mode=Swarm` +
  непустой `Docker.ContourNetwork` → `ApplicationException` (текст по
  образцу PgWorker). Место — рядом с текущей топология-проверкой
  Docker-опций; канон — одно сообщение fail-fast.
- Юнит-тест на валидацию (юнит-проект S3Worker).

### 3.4. Deploy и стендовый compose

- `deploy/docker-compose.yml`, `x-valkeyworker-env` +=
  `ValkeyWorker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}`
  (общий env сети контура — симметрия S3Worker-строки
  `S3Worker__Docker__ContourNetwork: ${PGW_CONTOUR_NETWORK:-}`; коммент
  по образцу).
- `dev-stand/adminpanel/docker-compose.yml`, сервисы `valkeyworker` /
  `valkeyworker-2`: env += `ValkeyWorker__Docker__ContourNetwork:
  adminpanel-stand-net` (сеть стендового compose; значение — константа
  стендового сервиса, без интерполяции deploy-`.env`).

### 3.5. Стенд: 00-up.sh и README (без сидинга)

`dev-stand/adminpanel/checks/00-up.sh` — после подъёма в стенде живут
ТОЛЬКО ядро и панель:

- поднимается: стендовый compose `--profile kafka --profile valkey
  --profile metrics` (etcd×3, панель, kafkaworker×2, valkeyworker×2,
  прометей/графана/алертменеджер/sd-generator) + deploy-контур
  contour-override `pgworker pgworker-2 s3worker s3worker-2` (без
  `owns3`); профиль `full` (as-minio) из подъёма убирается;
- УДАЛЯЕТСЯ из 00-up.sh целиком (без переезда в другие скрипты):
  - наливка PG-демо `POST /api/clusters` + весь wait-гейт конвергенции
    (`demo_ready`/`conv_*`/детектор стагнации/диагностика отказа) и
    связанные шаги ожидания;
  - наливка S3-пула `backups` (`POST /api/s3/clusters` + ожидание);
  - вызов `"$PWD/checks/05-seed.sh" valkey` и его комментарии;
  - фазы standalone-owns3: fail-fast проверка образа `owns3:dev`,
    healthz `:9010`, bucket etcd-слепков `pgworker-etcd-snapshots`
    (mc-налив);
- остаются: зачистка контура (90-down.sh -v), publish-фазы образов
  (SdGenerator, wal, S3Worker — s3worker входит в ядро), TLS-пакет и
  volume, deploy/.env-механика (PGW_METRICS_NETWORK/PGW_SCRAPE_NETWORK,
  секреты s3w-профилей — сервисы ядра их требуют), etcd-кворум,
  healthz/lease-ключи воркеров (pgw/kfw/vwk/s3w ×2), панель, прометей;
- шапка-описание и финальная строка переписываются: «stand is up (core +
  panel; resources are created manually via the panel)».

`dev-stand/adminpanel/checks/05-seed.sh` — **не меняется**: ветки
`kafka`/`valkey` остаются как есть; расширение ветками `pg`/`s3`
запрещено; потребители — чеки `50`/`51` и ручные прогоны.

`dev-stand/adminpanel/README.md` — отражение нового порядка: подъём
ядра 00-up.sh (пустой контур, ресурсов нет); ресурсы — только вручную
через панель; 00-up.sh больше не создаёт demo и не зовёт 05-seed.sh;
owns3-сервис — ручной подъём при необходимости (чек 46); новый чек
`52-valkey-contour.sh`. Существующие чеки `05`/`10`–`90` — без
изменений.

### 3.6. Новый стенд-скрипт приёмки контурного режима (СВОЙ скрипт)

Новый файл `dev-stand/adminpanel/checks/52-valkey-contour.sh` (канон
таблицы чеков — arch/adminpanel/04 §3), ручной чек приёмки контурного
режима ValkeyWorker, самодостаточный и самоочищающийся:

1. гварды: ядро стенда живо (панель healthz, lease-ключи valkeyworker);
2. логин в панель (cookie) → создание valkey-кластера с УНИКАЛЬНЫМ
   именем ЧЕРЕЗ панель (панельный API мутаций valkey — паттерн чека 51);
3. wait: кластер Active, нода RUNNING (бюджет — тики воркера, по
   образцу чека 51);
4. ассерты: `docker inspect` контейнера `vwk-<C>-*` — сеть
   `adminpanel-stand-net`; `endpoints` в etcd — advertised
   `host:port` (portalloc, формат не изменился);
5. teardown при любом исходе (trap): удаление кластера через панель;
   ассерт чистоты — контейнера `vwk-<C>-*` и ключей
   `/valkey/clusters/<C>/` нет.

Чек НЕ входит в 00-up.sh, никем автоматически не вызывается (ручная
приёмка), создаёт только временный собственный ресурс и полностью его
убирает. Аналогичная приёмка fail-fast Swarm — юнит-тестами (§3.2/§3.3),
стендового скрипта не требует.

### 3.7. Известные следствия (принимаются, не лечатся)

- Грань панели «Хранилище бэкапов» до создания пула через панель —
  configured с health-отказом (endpoint `s3w-backups-node1:9000`
  отвечает только после провижининга пула): ожидаемое состояние пустого
  контура.
- Чек `46-owns3.sh` требует подъёма owns3-сервиса вручную; чеки,
  требующие демо-ресурсов (`10`–`60`, `45`/`75`/`80`), — после ручного
  создания ресурсов через панель. Существующие чеки не переделываются.
- Комментарий-упоминание «сид 00-up.sh» внутри `45-backups-storage.sh`
  остаётся как есть (чекы не трогаем; источник истины — arch/README).

## 4. Фазы

1. **Arch** — выполнено (§3.1; правки в worktree).
2. **ValkeyWorker** (вне E2E): Options → Program (валидация/прокладка)
   → срез опций → драйвер (`EnsureNodeAsync` + `Network` в
   ContainerSpec) → юнит-тесты.
3. **S3Worker**: fail-fast Swarm + юнит-тест.
4. **Deploy/стенд-compose**: `deploy/docker-compose.yml` (vwk env),
   `dev-stand/adminpanel/docker-compose.yml` (vwk env).
5. **Стенд**: `00-up.sh` (удаление наливки/сидов/owns3-фаз/full-профиля),
   `README.md`, новый `52-valkey-contour.sh`. `05-seed.sh` и чеки
   `10`–`90` — без изменений.
6. **Прогоны**: юниты VWK/S3W → интеграция VWK/S3W (юнит/интеграционные
   проекты, НЕ E2E) → ручная приёмка стенда: `00-up.sh` (ассерты
   «ядра+пустого контура», §6 п.4) → `52-valkey-contour.sh` (§3.6).
   E2E-серии задачей не запускаются/не меняются: существующие проектные
   процедуры (если гоняются) — как есть, задача их не модифицирует.
   Зачистка контуров после каждой серии — по канонам.

## 5. Ограничения

1. **⛔ ПРИОРИТЕТ №1 — НЕ МЕНЯТЬ ВСЁ, ЧТО КАСАЕТСЯ E2E.** Запрет
   абсолютный: файлы E2E-тестов (включая env-словари E2E-окружений
   VWK/S3W/PgW), команды, фильтры, порядок запуска серий,
   `PgWorker.E2ePrepare`, покрытие E2E (проверка контурного режима в
   E2E НЕ добавляется; ничего не убирается), процедуры мерж-гейта с
   участием E2E — всё остаётся РОВНО таким, какое есть. Задача не
   добавляет, не убирает и не меняет в E2E НИЧЕГО; в diff отсутствуют
   файлы E2E-тестов и скрипты их запуска. Штатный регресс-прогон
   неизменённых серий — существующий процесс проекта, задача его не
   модифицирует и не инициирует.
2. **Никакого сидинга ресурсов на стенде**: `00-up.sh` ничего не
   наливает; новых сид-скриптов не появляется; `05-seed.sh` не
   расширяется и не удаляется. Ресурсы — только вручную через панель.
3. **Bash-чеки стенда не менять** (`05`, `10`–`90`): поведение прежнее.
   Новые проверки — только новыми своими скриптами (§3.6).
4. **KafkaWorker — вне скоупа**: контурный режим его нод (`kfw-net-<C>`)
   не вводится.
5. **PgWorker — эталон, не трогаем** (код/deploy/arch без изменений).
6. **Прод не указывает сеть**: дефолты пустые; `PGW_CONTOUR_NETWORK`
   живёт только в стендовом шаблоне `deploy/.env.example` (значение
   `adminpanel-stand-net`).
7. Локально собираемые образы в registry не класть; owns3:dev — внешний
   образ.
8. Никаких новых штатных операций и веток поведения сверх arch/21,
   arch/24, arch/adminpanel/04.

## 6. Критерии приёмки

1. **E2E не тронут (приоритетный критерий)**: в diff задачи НЕТ ни
   одного изменения файлов E2E-тестов (`src/tests/*.IntegrationTests`
   E2E-каталоги, env-словари окружений), скриптов/команд запуска серий
   и `PgWorker.E2ePrepare`; покрытие E2E не изменилось (проверка
   контурного режима в E2E не добавлялась).
2. **VWK контурный режим (юнит-уровень)**: `EnsureNodeAsync` с
   `contourNetwork: "contour"` создаёт контейнер с
   `Network == "contour"`; `null` — `Network == null` (поведение
   прежнее).
3. **VWK контурный режим (стенд)**: ручной чек
   `52-valkey-contour.sh` на стенде зелёный: контейнер ноды — в
   `adminpanel-stand-net`, endpoints — advertised `host:port`, после
   чека следов нет (контейнер/ключи удалены).
4. **Fail-fast Swarm**: VWK и S3W — `Mode=Swarm` + непустой
   `ContourNetwork` → старт воркера падает с диагностическим
   сообщением (юнит-тесты валидации зелёные).
5. **Стенд = ядро + панель, пустой контур**: после `00-up.sh` в docker
   НЕТ контейнеров `owns3`, `as-minio`, `pgw-*`, `vwk-*`, `s3w-*`;
   подняты etcd×3, панель, kafkaworker×2, valkeyworker×2, pgworker×2,
   s3worker×2, прометей/графана/алертменеджер/sd-generator; в etcd НЕТ
   ключей ресурсов (`/clusters/`, `/valkey/clusters/`,
   `/s3/clusters/`) — только lease-ключи воркеров; панель
   `:5050/api/healthz` — 200; финальная строка прогона отражает
   «core + panel».
6. **Существующие стенд-скрипты не менялись**: в diff отсутствуют
   `checks/05-seed.sh` и `checks/10-*.sh`–`checks/90-*.sh`; чеки `50`/`51`
   работают прежним образом (самостоятельно зовут `05-seed.sh`).
7. **Качество**: сборка VWK/S3W чистая (`TreatWarningsAsErrors=true`),
   юниты/интеграция (не E2E) зелёные; тестовые контуры зачищены.
