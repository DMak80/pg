# Спецификация t03-doorman-mandatory: пулер pg_doorman обязателен, режим «нода без пулера» снят

Дата: 2026-10-30 (актуализирована по результатам эмпирической проверки
pg_doorman v3.11.0). Источник задачи: `arch/roadmap/pgworker.md`, тег
`t03-doorman-mandatory`. Контракт уже изменён arch-first (arch/14
§2.1/§2.4 п.5/§8/§9 R1, arch/17 S4) — узлов без пулера нет, клиентский
вход только через пулер `:6432`, master-ключ `host:<doorman>` полноценный.

## 1. Цель

Удалить из кода, образа ноды и тестов ветвление «нода без пулера»
(`EnableDoorman=false`): пулер pg_doorman становится обязательной частью
образа `pgworker-node`, воркер ВСЕГДА публикует порт 6432, ставит env
`DOORMAN_CONFIG`/`PGW_DOORMAN_PORT`, сверка портов всегда ждёт тройку
(5432/8008/6432). Пулер при этом должен РЕАЛЬНО работать: конфиг и
запуск приводятся к фактическому формату pg_doorman v3.11.0 (Rust,
YAML `general`/`pools`, позиционный аргумент конфига) — включая
работающий механизм допуска клиентов через пул (per-pool `auth_query`,
см. §3.3.1/§6 п.6) и обязательные поля `general.admin_username`/
`admin_password`. Итоговая проверка цели — клиентский SQL через
опубликованный `:6432` в E2E.

Отдельная проблема поставки (решена, см. §6 п.1–2):
записанный в arch/14 R1 способ «build-arg DOORMAN_URL — curl бинарника
релиза» неисполним на arm64-хосте (стенд/E2E): релизные бинарники
pg_doorman существуют ТОЛЬКО под linux-amd64. По платформенной политике
пользователя (решение 2026-10-30, внесена в arch/03 §1): целевая
платформа проекта — x64, arm64 — только платформа для разработки;
doorman ставится одним мульти-арх (amd64+arm64) контейнер-образом
`ghcr.io/ozontech/pg_doorman:v3.11.0` — docker автоматически берёт
нужную архитектуру: amd64 на целевом x64-хосте (целевое решение),
arm64 на девелоперских ARM-машинах. Контракт arch/14 (§2.1 ×2, §8,
§9 R1) и arch/03 §1 обновлены arch-first ЭТОЙ фазой (файлы в worktree
уже содержат правки).

## 2. Принципы

- **arch-first**: контракт поставки меняется — сначала правка `arch/14`,
  затем код. Остальной контракт (обязательность пулера) уже в arch —
  код догоняет. Arch-правки этой задачи (arch/03 §1 — политика платформ,
  arch/14 — DOORMAN_IMAGE) УЖЕ внесены в worktree фазой spec; фаза кода
  их только сверяет.
- **Платформенная политика** (arch/03 §1): целевая платформа проекта —
  x64 (x86_64); arm64 — только платформа для разработки. Поставка
  doorman — один мульти-арх контейнер-образ: amd64-сборка — целевое
  решение, arm64 — сборка для разработки.
- **Пулер обязателен = fail-fast везде**: сборка образа без источника
  бинарника — отказ сборки; старт контейнера без бинарника/конфига —
  отказ старта контейнера (ненулевой exit главного entrypoint ДО
  supervisord), никаких молчаливых «not starting».
- **Пулер обязан пускать клиентов**: конфиг строится под проверенный на
  v3.11.0 механизм допуска (per-pool `auth_query`, §6 п.6); форматы,
  которые v3.11.0 отвергает (INI/pgbouncer-стиль, флаг `-c`, пустой
  users-список без auth_query, отсутствие admin-полей), не используются.
- **S4 (ключи не вырождаться)**: после обязательности пулера master-ключ
  `host:<doorman-port>` всегда дискриминантен; все спец-ветки «а если
  doorman-порт 0» из кода удаляются. Guard от вырожденных значений в
  `ClusterSecretRotator.ResolveMasterAsync` остаётся (защита по S4, не
  ветка режима).
- **Усыновлённые (object) записи не трогаем**: `patroni`/`doorman = 0` у
  object-записей легальны (arch/14 §2.4 п.5) — они не участвуют в
  `MatchesFact` и поведением не меняются.
- **Канон поставки внешних образов**: pg_doorman-образ — ВНЕШНИЙ, идёт
  через локальный registry `192.168.0.1:5000` (строка в `images.txt` +
  `mirror-image.sh`), под каноническим именем после `pull-images.sh`.
- Пины версий живут в точках сборки (роадмап): стенд `00-up.sh`, E2E
  (`E2eEnvironment`), ранбук `docs/runbook.md`.
- Никаких P/Invoke; `TreatWarningsAsErrors=true`; таймауты/бюджеты
  существующих тестов не меняются.

## 3. Структура и компоненты (что меняем)

### 3.1. arch/ (правка контракта поставки — ВНЕСЕНА фазой spec)

Фаза spec уже внесла правки в worktree (arch-first); фаза кода сверяет
их наличие и не дублирует:

- `arch/03-prerequisites.md` §1: платформенная политика — «целевая
  платформа проекта — x64 (x86_64); arm64 — только платформа для
  разработки» (решение пользователя 2026-10-30).
- `arch/14-pgworker.md` §2.1 (состав образа ноды), §2.1 таблица
  процессов, §8 (абзац об обязательности пулера), §9 R1: DOORMAN_URL →
  обязательный build-arg `DOORMAN_IMAGE` — мульти-арх контейнер-образ
  проекта pg_doorman (fail-fast без него); целевая платформа x64 —
  amd64-сборка (целевое решение), arm64 — платформа разработки (arch/03
  §1); бинарники GitHub-релиза — только amd64, отдельным путём поставки
  не служат.

Механизм допуска клиентов через пулер (auth_query) — деталь реализации
конфига: arch/14 механизм аутентификации клиентов пулера не
детализирует, контракт arch не меняется. Термин «DOORMAN_URL» из arch/
уходит полностью (остаётся только в тексте задачи роадмапа — удаляется
мерж-коммитом).

### 3.2. Образ ноды (`docker/node/`)

**Dockerfile**:
- `ARG DOORMAN_IMAGE=""`; ДО какого-либо использования — guard-RUN с
  диагностикой и `exit 1` (fail-fast сборки). BuildKit не раскрывает
  ARG напрямую в `COPY --from` — алиас-стадия `FROM ${DOORMAN_IMAGE…}`
  как источник бинарника (guard сохраняет понятный отказ при пустом
  аргументе).
- Бинарник — из стадии-образа pg_doorman (внутри официального образа —
  `/usr/bin/pg_doorman`) в `/usr/local/bin/pg_doorman`; docker сам
  выбирает архитектуру под платформу сборки (мульти-арх).
- В `apt-get install` добавить `openssl` (генерация self-signed
  TLS-серта пулера в entrypoint; детерминизм вместо надежды на базовый
  образ).
- Комментарии шапки обновить: пулер обязателен, «узел без doorman»
  больше не существует.

**`docker-entrypoint.sh`** (главный entrypoint, до старта supervisord):
- Проверка бинарника: `[ -x /usr/local/bin/pg_doorman ] || { echo …; exit 1; }`.
- `DOORMAN_CONFIG`: пустой/отсутствующий — `echo` диагностики и `exit 1`
  (отказ старта контейнера; воркер увидит exited-контейнер). Непустой —
  писать в `/etc/pg_doorman/pg_doorman.yaml` (было `.ini`).
- Генерация self-signed TLS-сертификата пулера при каждом старте
  (файлы вне volume — ключ не переживает рестарт, это допустимо:
  клиенты по канону P17 ходят `sslmode=require` без верификации цепочки,
  серт шифрования не несёт идентичности):
  `openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=pg_doorman" -keyout /etc/pg_doorman/tls.key -out /etc/pg_doorman/tls.crt`.

**`docker-entrypoint-doorman.sh`** — УДАЛИТЬ (вместе со строкой COPY и
chmod в Dockerfile): молчаливого режима больше нет, fail-fast поднят в
главный entrypoint, supervisord запускает бинарник напрямую.

**`supervisord.conf`**, секция `[program:pg_doorman]`:
- `command=/usr/local/bin/pg_doorman /etc/pg_doorman/pg_doorman.yaml`
  (позиционный аргумент конфига — фактический CLI pg_doorman).
- Паттерн надёжности как у `walreceiver`: `autorestart=unexpected`,
  `exitcodes=0`, `startsecs=5`, `startretries=0` — рестарты только
  падений проработавшего процесса; старт-отказ конфига — FATAL без
  ретраев (не лупит).
- Комментарий: пулер `:6432` обязателен (P13/P14/P15/P17); отказ старта
  (нет бинарника/конфига) — главным entrypoint'ом, контейнер гаснет.

### 3.3. Код PgWorker

| Файл | Правка |
|---|---|
| `src/PgWorker.App/Options.cs` (~183) | Удалить свойство `EnableDoorman`. |
| `src/PgWorker.App/appsettings.json` (~21) | Удалить ключ `"EnableDoorman"`. |
| `src/PgWorker.App/Program.cs` (~331, ~342) | Убрать аргумент `docker.EnableDoorman` из конструкторов обоих драйверов. |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | Удалить параметр `enableDoorman` из `PlainClusterDriver` и `SwarmClusterDriver` (и из внутреннего plain в `SwarmClusterDriver.EnsureNodeAsync`, ~712). `PortsMatchPlan` (~304): ожидаемая тройка всегда (5432, 8008, 6432), ветку удалить, комментарий «5432→pg, 8008→patroni, 6432→doorman». `BuildSpec` (~593–612): env `DOORMAN_CONFIG`/`PGW_DOORMAN_PORT` и порт `6432` — всегда, ветвления удалить; вызов генератора — `DoormanConfigBuilder.Build(topology.Cluster, serverConnections, secrets.SuPassword)` (креды superuser'а уже приходят в BuildSpec через `InstallSecrets`). |
| `src/PgWorker.Provisioning/Processes/MasterKeyReconciler.cs` (~93–110) | `IsCurrent`: удалить ветку `primary.Ports.Doorman <= 0` (невозможна: reconciler усыновлённые шарды скипает, канонические ноды всегда с пулером) и упоминание EnableDoorman в doc-комментарии; остаётся точное сравнение + сравнение по doorman-порту (advertised-режим, arch/16). |
| `src/PgWorker.Core/Planning/PortPlanConvergence.cs` (~70–76) | `MatchesFact`: удалить `.Where(p => p > 0)` — все три порта записи обязаны подтверждаться фактом контейнера. Комментарий: нулевой doorman канонической записи невозможен (пулер обязателен); устаревшая запись с `doorman=0` (или живой контейнер старого образа без 6432) — расхождение → detach → recreate; object-записи в `MatchesFact` не попадают (у них нет self-fact). |
| `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs` (~311–317) | Только комментарий `ResolveMasterAsync`: убрать «при EnableDoorman=false ключ вырождается в host:0»; мотивация — S4 (вырожденное значение недискриминантно; в штатном режиме невозможно, guard `doorman > 0` остаётся как защита). Код не меняется. |
| `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (`DoormanConfigBuilder.Build`, ~261) | Формат фактического pg_doorman v3.11.0 — YAML (§3.3.1): обязательные `general.admin_username`/`admin_password`, per-pool `auth_query` от superuser'а (пароль — параметр `suPassword`); сигнатура `Build(string dbname, int serverConnections, string suPassword)`. `ServerConnections` (бюджет P15) не меняется. Doc-комментарий класса переписать под фактический механизм («passthrough по умолчанию без users/auth_query» — неверная предпосылка, §6 п.6). Правка doc-комментария `InstallSecrets` (§14–16): прежнее «НИКОГДА не попадают в конфиги doorman/haproxy (текстовые файлы томов)» уточняется — `SuPassword` передаётся в `DOORMAN_CONFIG` как `auth_query.password`; класс экспозиции НЕ расширяется (секрет уже в env этого же контейнера — `PGPASSWORD_SUPERUSER` Spilo-env, тот же `docker inspect`), в файлы volume'ов по-прежнему не попадает (конфиг генерится в env, файл живёт в контейнере). |

#### 3.3.1. Целевой DOORMAN_CONFIG (YAML, проверен на v3.11.0)

```yaml
# pg_doorman: single pool {dbname} per node (P13/P14/P15/P17).
# Clients connect on :6432 with sslmode=require (P17); admission — auth_query.
general:
  host: "0.0.0.0"
  port: 6432
  admin_username: "pgwd_admin"
  admin_password: "pgwd_admin_password"
  tls_mode: "require"
  tls_certificate: "/etc/pg_doorman/tls.crt"
  tls_private_key: "/etc/pg_doorman/tls.key"
pools:
  {dbname}:
    server_host: "127.0.0.1"
    server_port: 5432
    server_database: {dbname}
    pool_mode: "transaction"
    max_db_connections: {serverConnections}
    auth_query:
      query: "SELECT passwd FROM pg_shadow WHERE usename = $1"
      user: "postgres"
      password: "{suPassword}"
```

Обоснование ключей:
- `general.host/port` — слушающий вход; `tls_mode: "require"` + серт/ключ
  (обязательная пара для TLS) — P17 (серты генерирует entrypoint ноды).
- `general.admin_username`/`admin_password` — ОБЯЗАТЕЛЬНЫЕ поля v3.11.0:
  без них парсер конфига отказывает (exit 78). Админ-консоль пулера
  продуктом не потребляется — значения детерминированные литералы
  (`pgwd_admin`/`pgwd_admin_password`): конфиг обязан быть стабильным
  между reconcile'ами (иначе вечное пересоздание контейнеров), секретом
  эти поля не являются.
- Пул `{dbname}` = имя кластера, бэкенд `127.0.0.1:5432` (loopback ноды,
  P14); `max_db_connections` — вычисляемый бюджет P15 (`max(10,
  max_connections − 5)`). Клиентские лимиты не задаём (дефолт 8192
  достаточен).
- **`auth_query` — единственный работающий на v3.11.0 механизм допуска**
  (эмпирически: без объявленных пользователей КАЖДЫЙ клиент отклоняется
  «FATAL: No connection pool configured for database … user …»; не
  спасают ни `pg_hba` (trust/scram-sha-256), ни auth_query уровня
  `general`). Per-pool auth_query: пулер исполняет запрос верификаторов
  из `pg_shadow` на бэкенде и проверяет клиентское SCRAM-доказательство
  по полученному verifier'у, затем аутентифицируется к бэкенду
  переиспользованием клиентского доказательства (passthrough) — пароли
  клиентов в конфиге не хранятся.
- Роль исполнения auth_query — СУЩЕСТВУЮЩИЙ superuser `postgres`
  (пароль per-install `SuPassword`, единый с DSN-путями воркера —
  `DatabaseProvisioner.BuildAdminDsn`): `pg_shadow` читаем только
  суперпользователем; новая роль НЕ создаётся, provisioning SQL не
  меняется. Алфавит `SuPassword` ограничен DSN-safe
  `[A-Za-z0-9._-]` (`InstallSecretsGuard`, fail-fast старта воркера) —
  вставка в YAML-строку в двойных кавычках безопасна без экранирования.
- Файл конфига: `/etc/pg_doorman/pg_doorman.yaml`; env-имя
  `DOORMAN_CONFIG` сохранено (содержимое — YAML).

### 3.4. Пин версии в точках сборки

Пин — `ghcr.io/ozontech/pg_doorman:v3.11.0` (последний релиз на момент
задачи; мульти-арх amd64+arm64):

- `dev-stand/images/images.txt`: строка `ghcr.io/ozontech/pg_doorman:v3.11.0`.
- Зеркалирование: `dev-stand/images/mirror-image.sh ghcr.io/ozontech/pg_doorman:v3.11.0`
  → `192.168.0.1:5000/ghcr.io/ozontech/pg_doorman:v3.11.0`; пре-пулл —
  `pull-images.sh` (ретаг в каноническое имя, как у spilo).
- `dev-stand/adminpanel/checks/00-up.sh` (~93–98): в docker build ноды
  добавить `--build-arg "DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0"`,
  комментарий про пин.
- `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` (`EnsureStaticAsync`,
  docker build ноды ~1931): добавить пару `"--build-arg",
  $"DOORMAN_IMAGE={DoormanImage}"`; константа-пин по прецеденту `McImage`:
  `internal const string DoormanImage = "ghcr.io/ozontech/pg_doorman:v3.11.0";`
- `docs/runbook.md`: обновить команду сборки §«Образ PG-ноды» (+ build-arg);
  в каталог зеркалируемых — строку `ghcr.io/ozontech/pg_doorman:v3.11.0`;
  описать рутину обновления версии (изменить пин в трёх точках:
  00-up.sh, E2eEnvironment, этот раздел + перемirrorить образ).

Обновление пина на новую версию — отдельная рутина по ранбуку, не часть
задачи.

### 3.5. Тесты

**Юниты (`src/tests/PgWorker.UnitTests/`)**:
- `Docker/ClusterDriverTests.cs`: из всех конструкторов
  Plain/Swarm-драйвера убрать `enableDoorman:` (строки ~242, 472, 549,
  626, 653, 1014, 1075, 1115, 1165); хелпер `NewPlainDriver` — без
  параметра. УДАЛИТЬ тест `EnsureNode_DoormanDisabled_NoDoormanPortAndConfig`
  (~647–666, ветка без пулера). Ассерты `DOORMAN_CONFIG` в
  `EnsureNode_NewContainer_EnvFromBuildersAndVolumeAndPorts` и
  `EnsureNode_WithTuning_SpiloCarriesMergedAndDoormanSynced` — под YAML:
  `pool_mode: transaction`, `max_db_connections: 55`, обязательные
  `admin_username`/`admin_password`, `auth_query` с `user: postgres` и
  паролем от SuPassword-стаба фикстуры; плюс ассерт `PGW_DOORMAN_PORT`.
- `Planning/PortPlanConvergenceTests.cs` (~85–110): тест
  `DetachColliding_DoormanDisabledZeroPort_RecordConfirmed` переписать с
  обратной семантикой — запись `doorman=0` при факте без 6432 НЕ
  подтверждена → detach (расхождение с обязательной тройкой).
- `Provisioning/ClusterSecretRotatorTests.cs` (~240):
  `Tick_SharedHostMasterKey_ResolvesByDoormanPort` остаётся (логика
  актуальна); комментарий переписать — мотивация S4 (резолв по
  уникальному doorman-порту), без упоминания EnableDoorman.
- `Templates/NodeConfigBuildersTests.cs`: затрагивается 3-аргументной
  сигнатурой `Build` (§3.3) — перевод трёх вызовов на новую сигнатуру с
  SuPassword-стабом: `Secrets_AreInEnv_ButNeverInDoorman` (~180),
  `Doorman_SingleTransactionPoolWithBudget` (~215),
  `Doorman_BudgetSynchronizedWithMaxConnections` (~334) — иначе CS7036,
  юнит-проект не скомпилируется. Тест
  `Secrets_AreInEnv_ButNeverInDoorman` (~174–186) переработать под
  новый механизм допуска: `su-secret` ЛЕГИТИМНО входит в
  `DOORMAN_CONFIG` как `auth_query.password` (ассерт Contain для него);
  `NotContainAny` — только для остальных секретов установки
  (`standby-secret`, `admin-secret`, `mover-secret`: в конфиг пулера не
  попадают никогда).
- Тестовые данные `NodePorts(…, 0)` с `Object:` (усыновлённые записи:
  Move/ShardEndpoints/NodeSupervisor/BackupProcess/RestoreProcess/
  PortallocTests и т.д.) НЕ трогать — для object-записей doorman=0
  легален (arch/14 §2.4 п.5).

**Интеграционные (`src/tests/PgWorker.IntegrationTests/`)**:
- `Docker/DockerDriverTests.cs` (~34): `NewDriver` — без `enableDoorman: false`.
- `Docker/RestTlsConvergenceTests.cs` (~134–136, ~155): убрать
  `enableDoorman: false` из конструктора драйвера; в portalloc-сидe
  третья компонента троек — `FreePort()` вместо `0` (после обязательности
  пулера драйвер публикует 6432→`addr.Ports.Doorman`; порт 0 в спеке
  контейнера — дефект теста). Стаб DOORMAN_CONFIG — валидный YAML.

**E2E (`src/tests/PgWorker.E2eTests/E2e/`)**:
- `E2eEnvironment.cs` (~66–67, ~1017): удалить env-переопределение
  `PgWorker__Docker__EnableDoorman`; комментарий у `NodeImage`
  переписать («образ несёт обязательный пулер — пин DOORMAN_IMAGE при
  сборке EnsureStaticAsync»).
- `E2eSecondInstanceScenarios.cs` (~155): удалить env-строку.
- `E2ePgtuneScenarios.cs` (~44–47): удалить `extraEnv` с
  `EnableDoorman=true` (ассерт бюджета теперь безусловно; процесс пулера
  реально стартует в E2E); комментарий «Doorman включаем ТОЛЬКО ради
  env-ассерта» — удалить; ассерт doorman-бюджета — под YAML.
- Комментарии «master-ключ host:0 при EnableDoorman=false» /
  «недискриминантен при EnableDoorman=false» переписать в:
  `E2eDrillScenarios` (~411), `E2eMoveScenarios` (~453),
  `E2eScaleScenarios` (~410), `E2eScenarios` (~323, ~621),
  `E2eWalStreamScenarios` (~1106), `E2eWalStreamMasterDownScenarios`
  (~466), `E2eWalStreamPromoteScenarios` (~399),
  `E2eWalStreamTliScenarios` (~804), `E2eTliSwitchScenarios` (~940),
  `E2eAppSecretScenarios` (~62), `E2ePgtuneScenarios` (~499). Формулировка:
  master-ключ `host:<doorman-port>` дискриминантен (S4), но резолв
  мастера остаётся по пробам Patroni `/primary` (первоисточник, S4).
  Логика сценариев не меняется (резолв и так по пробам).
- **Ассерт клиентского входа через пулер** (фаза `ac2-doorman-sql`): в
  базовом сценарии `E2eScenarios` после достижения Active — подключение
  Npgsql к опубликованному doorman-порту мастера шарда
  (`localhost:{Doorman}`, `sslmode=require`, креды per-cluster
  app-секрета) и простой SELECT. Это функциональная проверка механизма
  допуска (auth_query): без него «пулер обязателен» не проверяется
  (до задачи пулер ни разу реально не запускался ни в одном контуре).
- Ассерт provisioning'а: portalloc-тройки нод — все три порта > 0
  (12 уникальных на 4 ноды кластера).

### 3.6. docs

- `docs/architecture-review.md`: пункт A1 (два канона клиентского входа,
  EnableDoorman=false) — удалить как закрытый задачей (в доке — только
  текущее состояние); в A7 убрать фразу «Семантика значения зависит от
  EnableDoorman». Остальные пункты не трогать.
- `docs/runbook.md` — см. §3.4.

## 4. Фазы исполнения

1. **arch**: правки УЖЕ внесены фазой spec (§3.1: arch/03 §1 + arch/14
   ×4) — фаза кода сверяет их и начинает с образа ноды.
2. **Образ ноды**: Dockerfile (fail-fast ARG + алиас-стадия FROM
   DOORMAN_IMAGE + openssl), главный entrypoint (fail-fast DOORMAN_CONFIG,
   yaml-файл, TLS-серты), удаление wrapper'а, supervisord.
3. **Код**: удаление EnableDoorman/enableDoorman по таблице §3.3,
   DoormanConfigBuilder → YAML с auth_query и admin-полями (§3.3.1),
   правка doc-комментария InstallSecrets.
4. **Поставку и пины**: images.txt + mirror + 00-up.sh + E2eEnvironment +
   runbook (§3.4).
5. **Тесты**: правки/удаления/новые ассерты по §3.5.
6. **docs**: architecture-review (§3.6).
7. **Мерж-гейт** (порядок строгий, двумя отдельными командами сборка и
   тесты; перед сериями — зачистка контейнеров/сетей между сериями):
   - зеркалирование образа `mirror-image.sh ghcr.io/ozontech/pg_doorman:v3.11.0`
     и `pull-images.sh` (операторский шаг, вне кода);
   - юниты + интеграционные PgWorker;
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`;
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`;
   - полный E2E-прогон PgWorker E2eFixture (изменены provisioning и
     portalloc-конвергенция — канон AGENTS.md требует полный прогон),
     `--no-build`, минимум кейс-маркер `Scale_AddEmptyShard` в той же
     сессии; fresh Release.

## 5. Ограничения

- **Платформенная политика** (arch/03 §1, решение пользователя
  2026-10-30): целевая платформа — x64; arm64 — только платформа для
  разработки. Поставка doorman — ЕДИНЫЙ мульти-арх контейнер-образ
  (никаких отдельных arm64-механизмов и никакой x64-эмуляции arm64-хостов
  разработки); amd64-сборка — целевое решение.
- **Provisioning SQL не меняется; выделенная роль для auth_query НЕ
  создаётся** — исполнение запроса от существующей роли superuser
  `postgres` (per-install `SuPassword`).
- **Класс экспозиции секретов не расширяется**: `SuPassword` в
  `DOORMAN_CONFIG` добавляется там же, где он уже есть (env того же
  контейнера ноды — Spilo-env `PGPASSWORD_SUPERUSER`); новых мест
  хранения (volume-файлы, etcd) не появляется.
- Не менять: `DoormanConfigBuilder.ServerConnections` (бюджет P15),
  patroni/REST-TLS механики, панель (по grep EnableDoorman в
  `src/AdminPanel.*` ветвлений нет), swarm-логику сверх удаления
  параметра.
- Не трогать легаси-тестовые данные `NodePorts(…, 0)` с `Object:`.
- Никаких новых таймаутов/бюджетов; существующие не расширять.
- Локально собираемые образы в registry не класть; pg_doorman — внешний,
  в `images.txt` вносится.
- Русский язык комментариев/доков, английский — идентификаторы; AAA в
  тестах.
- В arch/roadmap/pgworker.md пункт `t03-doorman-mandatory` удаляется
  мерж-коммитом (правила роадмапа).

## 6. Принятые решения (закрытые развилки)

1. **Метод поставки бинарника — ОДИН мульти-арх контейнер-образ,
   обязательный build-arg `DOORMAN_IMAGE`** (СОГЛАСОВАНО пользователем
   2026-10-30: «doorman должен быть внутри ноды/контейнера с pg, меня
   устроит один мульти[архивальный] образ»; doorman живёт внутри
   контейнера PG-ноды — supervisord-программа, отдельного контейнера
   пулера нет). Источник — официальный `ghcr.io/ozontech/pg_doorman`
   (amd64+arm64), зеркалируется в локальный registry по канону
   (`images.txt` + `mirror-image.sh`), в Dockerfile — алиас-стадия
   `FROM ${DOORMAN_IMAGE}` + guard-RUN (BuildKit не раскрывает ARG в
   `COPY --from` напрямую); docker автоматически выбирает архитектуру
   под платформу сборки.
2. **Платформенная политика — обще-проектная** (СОГЛАСОВАНО
   пользователем 2026-10-30): целевая платформа x64, arm64 — только
   платформа для разработки; doorman собирается x64 как целевое решение,
   arm64 — для разработки. Внесена в arch/03 §1 (политика) + arch/14
   (конкретика doorman); реализуется мульти-арх образом из п.1 без
   отдельных механизмов. **Версия-пин — v3.11.0** (последний релиз на
   2026-10-30; обновление — рутина ранбука).
3. **«Отказ старта контейнера» = ненулевой exit ГЛАВНОГО entrypoint до
   supervisord** (контейнер гаснет, воркер/docker видит exited). Вариант
   «ненулевой exit wrapper'а supervisord-программы» отвергнут: он даёт
   бесконечные рестарты программы при живом контейнере — это не «отказ
   старта контейнера».
4. **Wrapper `docker-entrypoint-doorman.sh` удаляется**; supervisord
   запускает `pg_doorman /etc/pg_doorman/pg_doorman.yaml` напрямую.
   Проверки (бинарник есть, конфиг непуст) перенесены в главный
   entrypoint.
5. **TLS пулера — self-signed серт, генерируемый entrypoint'ом при
   каждом старте** (`openssl req -x509`, пакет openssl добавлен в
   Dockerfile). Per-install CA/серты из воркера не вводятся: клиенты по
   канону P17 ходят `sslmode=require` без верификации — серт шифрования
   не несёт идентичности; минимум движущихся частей. ⚠️ ПРИНЯТО БЕЗ
   СОГЛАСОВАНИЯ (вопрос не задавался отдельно — решение исполнителя
   спеки, может быть переиграно на ревью в пользу per-install CA).
6. **Механизм допуска клиентов — per-pool `auth_query`** (эмпирически
   проверено на запиненном v3.11.0 в изолированной лаборатории):
   конфиг БЕЗ объявленных пользователей (расчёт на «passthrough по
   умолчанию») отвергает КАЖДОГО клиента («FATAL: No connection pool
   configured for database … user …»); не спасают ни `pg_hba`
   (trust/scram-sha-256), ни auth_query уровня `general`. Работающий
   механизм — per-pool auth_query `SELECT passwd FROM pg_shadow WHERE
   usename = $1`: пулер верифицирует клиентский SCRAM по verifier'у из
   `pg_shadow` и переиспользует клиентское доказательство к бэкенду
   (SELECT 1 через `:6432` c `sslmode=require` проходит). **Роль
   исполнения auth_query — существующий superuser `postgres`**
   (per-install `SuPassword`, уже доступен генератору через BuildSpec);
   provisioning SQL не меняется, выделенная роль не создаётся —
   `pg_shadow` читаем только суперпользователем, отдельная роль с
   SECURITY DEFINER не вводится (минимум движущихся частей; класс
   экспозиции `SuPassword` не расширяется — секрет уже в env того же
   контейнера). Пароли клиентов в конфиге не хранятся.
7. **`general.admin_username`/`admin_password` — обязательные поля
   v3.11.0** (без них отказ парса конфига, exit 78); админ-консоль
   пулера продуктом не потребляется — детерминированные литералы
   `pgwd_admin`/`pgwd_admin_password` (конфиг обязан быть стабильным
   между reconcile'ами — случайные значения давали бы вечное
   пересоздание контейнеров; границей секьюрити эти поля не являются).
8. **`PortPlanConvergence.MatchesFact` — полное удаление zero-ignore**
   (не только doorman): для канонических записей нулевых портов не
   бывает, object-записи в MatchesFact не попадают; легаси-запись/контейнер
   без 6432 конвергирует recreate'ом (штатный механизм, runbook §обновление
   образа ноды).
9. **E2E-ассерт клиентского SQL через 6432 добавляется** в базовый
   `E2eScenarios` (фаза `ac2-doorman-sql`). ⚠️ ПРИНЯТО БЕЗ СОГЛАСОВАНИЯ:
   расширение объёма сверх буквы роадмапа (роадмап про новый ассерт
   молчит) — без него обязательность пулера функционально не
   проверяется.
10. **`docs/architecture-review.md` правится** (A1 удаляется, A7 — фраза).
    ⚠️ ПРИНЯТО БЕЗ СОГЛАСОВАНИЯ: в роадмапе файла нет, но правило «в
    документах только текущее состояние» делает правку обязательной —
    иначе док утверждает снятый режим.
11. **Свежесть перехода**: живые контейнеры нод старого образа (без
    бинарника пулера) после апгрейда пересоздаются конвергенцией
    portalloc (запись/факт без 6432 → detach → recreate) — ожидаемое
    поведение, не дефект; данные в volume сохраняются.
12. **supervisord-программа пулера**: `autorestart=unexpected`,
    `exitcodes=0`, `startsecs=5`, `startretries=0` (паттерн walreceiver:
    рестарты только падений проработавшего процесса).

## 7. Критерии приёмки

1. `grep -rn "EnableDoorman\|enableDoorman\|DOORMAN_URL"` по `src/`,
   `docker/`, `dev-stand/`, `deploy/`, `docs/` (кроме
   `docs/superpowers/` и `arch/roadmap/`) — пусто.
2. `grep -rn "DOORMAN_URL" arch/` — пусто (вне `arch/roadmap/`); контракт
   DOORMAN_IMAGE согласован в arch/14 §2.1/§8/§9 R1; политика платформ
   (целевая x64, arm64 — разработка) — в arch/03 §1.
3. Сборка `docker build -f docker/node/Dockerfile` без `--build-arg
   DOORMAN_IMAGE=…` падает с сообщением об обязательном пулере
   (fail-fast); с build-arg собирается и на x64-, и на arm64-хосте
   (мульти-арх).
4. Запуск контейнера ноды без env `DOORMAN_CONFIG` — ненулевой exit с
   диагностикой (контейнер не живой).
5. Юниты PgWorker зелёные; тест ветки без пулера удалён;
   `PortPlanConvergenceTests` проверяет «doorman=0 → detach»;
   ClusterDriverTests ассертят YAML-конфиг (тройка портов всегда,
   `admin_username`/`admin_password`, `auth_query` c `user: postgres`
   и паролем SuPassword-стаба, `PGW_DOORMAN_PORT`).
6. Интеграционные PgWorker зелёные (RestTlsConvergence с живой тройкой
   портов в portalloc-сиде; DOORMAN_CONFIG-стаб — валидный YAML).
7. Полный docker-E2E PgWorker на свежем Release зелёный (порядок §4.7),
   включая фазу `ac2-doorman-sql` — клиентский SELECT через
   опубликованный doorman-порт с `sslmode=require` и per-cluster
   app-кредами (функциональная проверка auth_query-допуска); env
   `PgWorker__Docker__EnableDoorman` в E2E отсутствует; portalloc-тройки
   нод — все три порта > 0.
8. `images.txt` содержит `ghcr.io/ozontech/pg_doorman:v3.11.0`; ранбук
   описывает пин и рутину обновления; `architecture-review.md` не
   упоминает снятый режим.
9. Зачистка тестовых контейнеров/сетей между сериями выполнена; после
   прогонов осиротевших `pgw-net-*` нет (`docker network ls | grep -c
   pgw-net` при нуле контейнеров — ноль).
