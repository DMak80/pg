# Spec: per-cluster app-секрет в etcd (etcd password field)

Дата: 2026-08-28. Ветка: `feat-etcd-password-field`. Ревизия 3 (правки по
независимому ревью план↔spec: критерий 2 — в e2e прямой pg-порт ноды,
doorman `:6432` — канон деплоя; ensure-шаг — P1.5; add-shard — процесс G.
Ревизия 2 — гейт user-review: механизм bucket_admin в etcd сохранён, откат
отменён.)

## 1. Цель

Клиентское приложение шардированного кластера должно работать, зная **только
про etcd**: адреса оно уже берёт из ключей кластера (`master`, `dsn`), а
логин и пароль роли `app` теперь тоже живут в etcd — рядом с описанием
кластера, в своих ключах. Пароль **создаёт** PgWorker (генерация при
provisioning), пароль **хранится** в etcd в единственном месте, и больше
нигде не светится: не в env контейнеров и сервиса, не внутри DSN-ключей,
не в config JSON, не в настройках AdminPanel, не в UI/логах. Решение
«убрать app-пароль из etcd» — правка в одном месте контракта (два ключа),
без разноса по коду.

Существующий механизм **bucket_admin** (коммиты 6edc80b/4c98338 в main:
per-cluster поля `bucket_admin_user`/`bucket_admin_password` в config JSON,
пароль внутри DSN-ключа, чтение панелью из DSN с fallback на
`AdminPanel:Probes:Password`) **не трогается** — задача его не изменяет и
не откатывает (решение пользователя на гейте user-review, 2026-08-28).

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Какие пароли переезжают в etcd | **Только `app`** — приложение должно знать только про etcd и работать с данными БД. Логин+пароль — один на все ноды/шарды кластера, хранится в описании кластера. Остальные роли (`superuser`, `standby`, `bucket_admin`, `bucket_mover`) остаются при текущей схеме (bucket_admin — как в main, прочие — per-install env) |
| Формат хранения | **Два строковых ключа** `/clusters/<C>/app_user` и `/clusters/<C>/app_password` (не JSON, не поля config) |
| Судьба механизма bucket_admin в etcd (6edc80b) | **Не трогать** — дословно: «механизм bucket_admin из etcd откатывать не нужно, я не просил откатывать». Остаётся как есть: config-поля, `password=` в DSN, парсинг панелью |

## 2. Принципы

1. **arch/-first**: контракт etcd меняется сначала в `arch/` (11, 14,
   adminpanel/02), затем отражается в коде. Попутно канон фиксирует уже
   фактическое поведение bucket_admin (оно в main с 6edc80b, но в arch/
   не описано) — документирование без изменения поведения.
2. **Один секрет — одно место**: значение `app_password` существует в etcd
   и нигде больше не хранится и не дублируется (в памяти процессов — как
   unavoidable). Все потребители читают его из etcd.
3. **Генерирует владелец процесса**: ключи пишет только PgWorker — держатель
   клэйма `<C>`, txn `put-if-absent` (идемпотентность, защита от гонок).
   Панель ключи не пишет и не читает.
4. **App-пароль не светится**: не в env контейнера/сервиса, не в DSN-ключах,
   не в config, не в UI/API/логах панели, не в журнале `/pgworker/work/`.
   Существующие Redact-механики PgWorker сохраняются; SQL-тексты с паролем
   не попадают в тексты ошибок. (Про светимость bucket_admin-пароля задача
   не высказывается — тот механизм вне изменений.)
5. **Идемпотентность и живучесть**: повторный provisioning/re-run, тики
   надзора, add-shard и rebuild нод не перегенерируют существующий app-пароль;
   deprovision кластера удаляет ключи автоматически (вместе с префиксом).
6. **Не трогаем работающее**: поведение bucket_admin (config-поля, DSN с
   паролем, пробы панели) не меняется ни на одном шаге задачи.
7. **YAGNI**: ротация пароля, шифрование at rest, `secret-manager`, перенос
   остальных ролей в etcd — вне scope (roadmap `t02-per-cluster-secrets`).

## 3. Контракт etcd (канон — правки arch/)

### 3.1. Новые ключи

| Ключ | Значение | Пишет | Читает |
|---|---|---|---|
| `/clusters/<C>/app_user` | строка, имя роли приложения; PgWorker пишет `"app"` | PgWorker (provisioning P1.5, add-shard; txn put-if-absent) | PgWorker (DatabaseProvisioner — CREATE/ALTER ROLE, гранты); приложение |
| `/clusters/<C>/app_password` | строка — сгенерированный пароль, 32 символа `[A-Za-z0-9]` | то же | то же |

Свойства:

- Пара ключей — per-cluster: одна роль `app` с одним паролем на все
  шарды/ноды кластера (роль создаётся на каждом шарде с этим значением).
- **Пишет только PgWorker**, держатель клэйма `<C>`, одной txn
  (`compare version(app_user)==0 AND version(app_password)==0` → `put`
  обоих). Проигрыш txn → re-read существующих значений и использование их
  (идемпотентность re-run после сбоя).
- Deprovision (D2, `del --prefix /clusters/<C>/`) удаляет ключи автоматически;
  remove-shard/add-shard ключи не трогают (секрет пер-кластерный, не
  пер-шардовый).
- AdminPanel: ключи находятся в читаемом панелью префиксе `/clusters/`, но
  панель их **осознанно игнорирует** (не читает значение, не отображает,
  не считает `unknownKeys`) — канон adminpanel/02 §2.1 фиксирует это явно.
- Приложение-потребитель: адрес — `shards/<X>/master` (`host:6432`,
  doorman текущего мастера, lease TTL 5 c; канон arch/11 §2 «для роутера
  приложений»), креды — `app_user`/`app_password`, вход — doorman `:6432`
  c TLS (P17). `shards/<X>/dsn` — инфраструктурный вход (HAProxy `:5432`,
  панель/mover), приложение его для подключения не использует. Ничего
  кроме etcd приложению знать не нужно. Это канон **деплоя**; e2e-стенд
  задачи работает без doorman (`EnableDoorman=false`) и проверяет креды
  по прямому pg-порту ноды (см. §7.2).

### 3.2. Правки arch/11-bucket-sharding.md

- §2, список ключей кластера: добавить `app_user`/`app_password` с
  формулировкой «per-cluster креды приложения; пишет PgWorker при
  provisioning (генерация), читают PgWorker и приложение».
- §2, пояснение к `shards/X/dsn`: привести к факту (без изменения
  поведения): у кластеров под PgWorker DSN пишется с `password=` роли
  bucket_admin (per-cluster, из config; коммит 6edc80b); app-секрет в DSN
  не попадает никогда. Снять устаревшее «пароли в etcd не хранятся
  (P12/P17)» → «в etcd хранятся: per-cluster app-пара (app_user/
  app_password) и per-cluster креды bucket_admin (config + DSN);
  superuser/standby/bucket_mover — per-install env PgWorker».
- §4, строка таблицы про app-роль: источник пароля — ключи
  `/clusters/<C>/app_user|app_password` (генерирует PgWorker), а не
  env/секрет-хранилище приложения.
- §5 (строки ~573–575, «Пароли шардов — там же … в etcd паролей нет»):
  уточнить — для кластеров под управлением PgWorker в etcd хранятся
  app-пара и bucket_admin-креды; для ручных скриптовых кластеров всё как
  было (`buckets.env`).

### 3.3. Правки arch/14-pgworker.md

- §3.1 (ключи `/clusters/`): добавить строки `app_user`/`app_password`
  (тип, писатель — PgWorker, семантика из §3.1 этого spec); у `dsn`
  зафиксировать фактический формат с `password=` bucket_admin.
- §4 «Секреты»: переработать — таблица из трёх групп:
  - **per-cluster, в etcd, генерирует PgWorker**: `app_user`/`app_password`
    (provisioning, `put-if-absent`, 32 симв `[A-Za-z0-9]`);
  - **per-cluster, в etcd, задаётся снаружи** (как в main, 6edc80b):
    `bucket_admin_user`/`bucket_admin_password` в config JSON с fallback
    на env `PGW_BUCKET_ADMIN_PASSWORD`; попадают в DSN-ключ шарда;
  - **per-install, из env** (не в git): `PGW_PG_SUPERUSER_PASSWORD`,
    `PGW_PG_STANDBY_PASSWORD`, `PGW_BUCKET_ADMIN_PASSWORD` (fallback),
    `PGW_BUCKET_MOVER_PASSWORD`. `PGW_APP_ROLE_PASSWORD` исключён.
- §5, процесс A (P0–P5): новый шаг **P1.5** «Ensure app-секрета: прочитать
  `app_user`/`app_password`; отсутствуют → сгенерировать и положить txn
  put-if-absent (оба ключа), при проигрыше txn — re-read»; P2.3 дополняется:
  роль `app` создаётся/обновляется с паролем из ключей (CREATE ROLE guard +
  безусловный идемпотентный `ALTER ROLE … PASSWORD` — согласованность
  ключ ↔ роль, включая кластеры, созданные до этой задачи); P2.5 — без
  изменений (dsn с bucket_admin-кредами, как сейчас).
- §5, процесс G (add-shard): тот же ensure-шаг перед созданием ролей нового
  шарда (у живого кластера ключи уже есть — читаем; отсутствуют — создаём,
  миграция старых кластеров). Остальное — без изменений.

### 3.4. Правки arch/adminpanel/02-etcd-contract.md

- §2.1, строка `dsn`: привести канон к факту (4c98338): DSN может нести
  `password=` (PgWorker-кластеры, bucket_admin); панель разбирает его в
  `ShardInfo.Password`, SQL-проба использует `shard.Password ??
  AdminPanel:Probes:Password`. Поведение не меняется — фиксируется как есть.
- §2.1: отдельная строка-декларация: ключи `app_user`/`app_password` —
  креды приложения, панель их **не читает и не отображает** (expected
  keys: парсер пропускает их без `unknownKeys`-счётчика, значение не
  попадает в модель/UI/API).
- §6.2 (SQL-проба): привести к факту — пароль из DSN при наличии, иначе из
  настроек панели (текущее поведение `SqlProbe`).

### 3.5. Правки arch/roadmap/pgworker.md

- `t02-per-cluster-secrets`: сузить формулировку — генерация per-cluster
  app-секрета в etcd сделана этой задачей; остаются ротация (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Отдельные пункты не добавляются.

## 4. Структура и компоненты (отражение в коде)

### 4.1. PgWorker — генерация и потребление app-секрета

- **`PgWorker.Core/Templates/NodeConfigBuilders.cs`**: `InstallSecrets`
  теряет `AppPassword` (остаётся `SuPassword`, `StandbyPassword`,
  `BucketAdminPassword`, `MoverPassword`, `BucketAdminUser="bucket_admin"`).
  `SpiloEnvBuilder` перестаёт прокидывать `PGW_APP_PASSWORD` в env контейнера
  ноды (app-пароль больше не должен светиться нигде, включая env контейнеров).
  `PGW_BUCKET_ADMIN_PASSWORD`/`PGW_BUCKET_ADMIN_USER` env ноды — без
  изменений.
- **`PgWorker.App/Program.cs`**: `SecretsFromEnv` — без
  `PGW_APP_ROLE_PASSWORD` (fail-fast список из 4 переменных).
- **`PgWorker.Core/Model/Domain.cs`**: `ClusterConfig` не меняется
  (`BucketAdminUser`/`BucketAdminPassword` остаются — механизм bucket_admin
  жив). Новый record `AppCredentials(string User, string Password)`;
  `ClusterSnapshot` получает `AppCredentials? App` (nullable: ключей может
  не быть до provisioning).
- **`PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs`**: добавить разбор
  `app_user`/`app_password` (строковые leaf-ключи сегмента `<C>`) в
  `ClusterSnapshot.App`. Чтение `bucket_admin_user`/`bucket_admin_password`
  из config — без изменений.
- **Новый генератор** (например `PgWorker.Core/DI` или Provisioning):
  `AppSecretGenerator` — `System.Security.Cryptography.RandomNumberGenerator`,
  32 символа, алфавит `[A-Za-z0-9]` (без спецсимволов: безопасно для
  SQL-литералов, libpq/Npgsql-строк, env, JSON без экранирования).
  Чистая функция — легко тестируется.
- **Ensure-шаг** (общий для Provisioning/AddShard, например сервис в
  `PgWorker.Provisioning`): read → при отсутствии generate + txn
  put-if-absent (2 compare + 2 put) → re-read. Возвращает актуальные
  `AppCredentials`.
- **`PgWorker.Provisioning/Sql/DatabaseProvisioner.cs`**:
  `BuildRoleGuardsSql` — роль `app` с именем/паролем из `AppCredentials`
  (новые параметры; bucket_admin-параметры остаются как есть); добавляется
  идемпотентный `ALTER ROLE "<app>" PASSWORD '<…>'` (исполняется после
  guard-CREATE — гарантирует соответствие роли etcd-ключу на любом шарде,
  включая ранее созданные кластеры); `BuildSchemasSql` — гранты с
  параметризованным именем app-роли (сейчас хардкод `"app"`).
- **`ProvisioningProcess.cs` / `AddShardProcess.cs`**: ensure-шаг перед
  созданием ролей; роль app и гранты — из `snapshot.App`; всё поведение
  bucket_admin (override из config, запись dsn с `password=`) — без
  изменений.
- **Журнал `/pgworker/work/<C>`**: фазы/ошибки не содержат значений секрета
  (journal пишет только phase/last_error-текст исключений; SQL-тексты в
  исключения не попадают — фиксируется тестом).

### 4.2. AdminPanel — только ожидание новых ключей

- **`AdminPanel.Etcd/Parsing/ClustersParser.cs`**: leaf-ключи
  `app_user`/`app_password` — осознанный skip без `unknownKeys`-счётчика и
  без попадания значения в модель.
- Больше в панели ничего не меняется: `DsnParser` (с `Password`),
  `ShardInfo.Password`, `SqlProbe` (`shard.Password ?? options.Password`)
  остаются как в main.
- Гарантия «app-креды не светятся в панели»: значения ключей не попадают
  в модель/UI/API (skip на уровне парсера); в UI/API их нет.

### 4.3. Стенд, deploy, тесты

- **`deploy/docker-compose.yml`**: убрать только `PGW_APP_ROLE_PASSWORD`
  (строка 25); остальные env-секреты — без изменений.
- **`dev-stand/seed.sh`**: без изменений (per-cluster bucket_admin креды
  BA_USER/BA_PASS сеются как раньше; app-ключи сесть НЕ надо — генерирует
  PgWorker).
- **E2e (`E2eFixture`, сценарии)**: env фикстуры без
  `PGW_APP_ROLE_PASSWORD`; проверки: после provisioning существуют
  `app_user`/`app_password` (формат значения), подключение `user=app` с
  паролем из etcd-ключа выполняет SELECT (в e2e — прямой pg-порт ноды:
  стенд без doorman; doorman `:6432` — канон деплоя, §3.1); повторный
  проход не меняет `app_password`; add-shard не меняет его; deprovision
  удаляет. Подключения `bucket_admin` — как в fixture сейчас (значение
  `E2eFixture.BucketAdminPassword` из config-сида), без изменений.
  Существующие сценарии, использующие app-пароль (`E2eMoveScenarios`,
  `E2eScaleScenarios` — сейчас `E2eFixture.AppPassword`), мигрируют на
  чтение `/clusters/<C>/app_password` через Gateway фикстуры — e2e-стенд
  становится потребителем секрета тем же путём, что и приложение.
- **Unit-тесты**: парсер (app-ключи → `Snapshot.App`; отсутствие → null),
  генератор (длина/алфавит/непредсказуемость — статистическая проверка
  алфавита), ensure-шаг (put-if-absent txn, re-read после проигрыша),
  SQL-текстостроители (роль app из кредов, ALTER ROLE идемпотентен,
  escaping), `Redact` по-прежнему маскирует `password=` в DSN-подобных
  строках (app-пароль в DSN не попадает, Redact — защита для остальных
  случаев), ClustersParser панели скипает app-ключи без `unknownKeys`,
  сбой SQL не выносит app-пароль в текст ошибки/journal (§4.1).

## 5. Фазы

0. **Канон**: правки arch/11, arch/14, arch/adminpanel/02, arch/roadmap
   (§3 этого spec; фиксация фактического bucket_admin-поведения + новые
   app-ключи). Коммит «docs(arch): …».
1. **Ядро PgWorker**: модель (`AppCredentials`, `ClusterSnapshot.App`),
   парсер app-ключей, генератор, `InstallSecrets` без `AppPassword` — с
   unit-тестами (TDD).
2. **Процессы**: ensure-шаг, `DatabaseProvisioner` (app из кредов +
   ALTER ROLE, параметризация грантов), Provisioning/AddShard (ensure
   перед ролями), `SpiloEnvBuilder` без `PGW_APP_PASSWORD`, `Program.cs`.
   Bucket_admin-пути не изменяются.
3. **AdminPanel**: expected-skip app-ключей в `ClustersParser` + тесты.
4. **Стенд/deploy/e2e**: compose (убрать `PGW_APP_ROLE_PASSWORD`),
   миграция существующих app-сценариев e2e на чтение секрета из etcd,
   e2e-сценарии и фикстуры (app-проверки).
5. **Верификация**: полный прогон unit + integration (Testcontainers etcd,
   включая все e2e-сценарии), e2e на dev-станде, сверка критериев приёмки
   (включая «bucket_admin-поведение не изменилось»), ревью.

## 6. Ограничения

- Механизм bucket_admin (config-поля, пароль в DSN, чтение панелью) —
  вне изменений задачи; любые его правки — отдельная задача, если
  пользователь их попросит.
- Ротация app-пароля (смена без остановки записи), сверка drift'а
  «роль ↔ ключ», `secret-manager` — roadmap `t02`, не эта задача.
- `superuser`/`standby`/`bucket_mover` остаются per-install env-секретами;
  их генерация per-cluster — roadmap.
- Шифрование значений в etcd (encryption at rest) — вне scope: среда
  считается безопасной (условие задачи).
- Ручные скриптовые кластеры (`init-cluster.sh`, `buckets.env`) не
  меняются: для них пароли остаются в `buckets.env`; правка — только
  формулировка канона arch/11 §5.
- Миграция существующих кластеров PgWorker на app-секрет: автоматическая —
  первый ensure-шаг (provisioning re-run/add-shard) создаёт ключи и
  `ALTER ROLE`-ом выравнивает роль на пароль из ключа; отдельного
  migration-раннера нет.
- UI панели не показывает app-креды — «посмотреть пароль» не появится
  (панель их не читает).
- E2e работает без doorman (`EnableDoorman=false`): путь doorman `:6432`
  в e2e не проверяется — это канон деплоя (§3.1), не тестовое окружение.

## 7. Критерии приёмки

1. После provisioning нового кластера в etcd есть
   `/clusters/<C>/app_user = "app"` и
   `/clusters/<C>/app_password` — 32 символа `[A-Za-z0-9]`; app-пароль
   отсутствует в DSN-ключах, config JSON, env сервиса и контейнеров нод.
2. E2e: подключение `user=app` с паролем из etcd-ключа выполняет запрос к
   данным (после создания схем). В e2e — прямой pg-порт ноды (стенд без
   doorman, `EnableDoorman=false`, как все существующие сценарии);
   doorman `:6432` — канон деплоя для приложения (§3.1) и отдельно
   в этой задаче не проверяется.
3. Идемпотентность: повторный provisioning-проход (re-run) и add-shard не
   меняют значение `app_password` (txn put-if-absent; e2e-проверка
   стабильности значения).
4. Deprovision кластера удаляет `app_user`/`app_password` (с префиксом).
5. Поведение bucket_admin не изменилось: config-поля
   `bucket_admin_user`/`bucket_admin_password` работают (override env),
   DSN-ключ содержит `password=` bucket_admin, панель читает его из DSN
   с fallback `AdminPanel:Probes:Password` — существующие e2e/тесты
   bucket_admin зелёные без правок ожиданий.
6. `PGW_APP_ROLE_PASSWORD` отсутствует в `Program.cs`,
   `deploy/docker-compose.yml` и env контейнеров нод
   (`SpiloEnvBuilder`); старт PgWorker без неё успешен.
7. AdminPanel: API/UI не отдаёт и не отображает app-креды; app-ключи не
   увеличивают `unknownKeys`.
8. Канон соответствует коду: arch/11 §2/§4/§5, arch/14 §3.1/§4/§5,
   adminpanel/02 §2.1/§6.2 описывают app-ключи и фактическое
   bucket_admin-поведение без противоречий.
9. Roadmap `t02-per-cluster-secrets` переформулирован (генерация app
   сделана; остались ротация/bucket_mover/secret-manager).
10. Все тесты зелёные (`TreatWarningsAsErrors=true`), включая новые
    unit/e2e из §4.3 и полный integration-прогон; журнал
    `/pgworker/work/` и логи не содержат значений app-пароля
    (фиксируется тестом сбоя SQL, §4.1).

## 8. Риски и их закрытие

| Риск | Закрытие |
|---|---|
| Существующий кластер: роль `app` со старым env-паролем, новый сгенерированный в etcd → рассинхрон | Безусловный `ALTER ROLE … PASSWORD` при ensure (идемпотентен), e2e-сценарий «старый кластер» |
| Пароль попадает в логи через тексты SQL/исключений | Алфавит без кавычек/спецсимволов; SQL не включается в сообщения исключений; `Redact`; unit-тест сбоя SQL (критерий 10) |
| Два инстанса PgWorker одновременно генерируют (гонка) | txn put-if-absent + клэйм `<C>`; проигрыш txn → re-read |
| Rebuild ноды: роль пересоздаётся другим паролем | Роль всегда создаётся/обновляется из etcd-ключей (ключ не перегенерируется) |
| Панель споткнётся о новые ключи | expected-skip в парсере; канон adminpanel/02; тесты фикстур |
| Случайное задевание bucket_admin-механизма при рефакторинге `DatabaseProvisioner`/процессов | Критерий 5 (регресс: существующие bucket_admin-тесты без правок ожиданий); фаза 2 не меняет bucket_admin-путей |
