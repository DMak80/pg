# t31-s3-cluster-creds — per-cluster клиентские креды S3-кластеров в etcd

Спецификация задачи `t31-s3-cluster-creds` (roadmap: `arch/roadmap/s3.md`).
Уровень: spec → plan → код (dev-flow). Источник истины — `arch/`; первая фаза
работ — правка arch-канонов, код зеркалит их после.

## 1. Цель

Перенести клиентские креды S3-кластеров (клиентская S3-грань: root-пара +
access keys) из per-install env-профилей воркеров в **etcd — per-cluster**,
по прецеденту per-cluster секретов PgWorker (arch/14 §4, группа 1) и
репликационного PKI S3Worker (arch/23 §6 «PKI репликации»):

- **S3Worker генерирует** при provisioning кластера root-пару и access keys
  (read-write — бэкапам, read-only — панели) и **хранит их per-cluster в etcd**
  (ключ `/s3/clusters/<C>/secrets`); пишет только S3Worker, txn put-if-absent.
- В config кластера (arch/23 §2) вводится признак **«бэкапный кластер etcd»**
  (`etcd_backup`): единственный кластер установки, у которого read-write ключ
  НЕ генерируется, а подставляется из **спец-ключа бэкапа etcd** — env-пары
  воркера `S3W_ETCD_BACKUP_*`. Root-пара и read-only ключ генерируются как у
  всех; формат secrets-ключа единый. Потребитель спец-ключа (запись слепков
  etcd в этот кластер) — будущий etcdworker `t32-etcdworker`; в t31 он читает
  значение так же, как остальные клиенты — из secrets-ключа в etcd.
- **Упраздняются**: per-install env-профили `S3W_NODE_SECRETS__*` (S3Worker),
  `PGW_BACKUP_S3_PROFILES__*` (PgWorker), поле `config.secret_profile` с
  валидацией 422 (arch/23 §6, arch/24 §1.1/§4/P1–P3 пересматриваются).
- **Потребители читают креды из etcd**: пул бэкапов PgWorker (arch/19 §7) —
  read-write ключ вместо env-профиля; панель — read-only ключ для грани
  «Хранилище бэкапов» вместо env-пары.
- Панель: создание кластера без профиля; просмотр состава per-cluster кредов
  (без значений) и ротация (существующая заявка).
- Стенд: демо-блок `S3W_NODE_SECRETS__BACKUPS-MAIN__*` уходит из
  `deploy/.env`; сид и чеки переводятся на per-cluster креды.

### 1.1. Не входит (граница)

- `t32-etcdworker` — сам etcdworker: Materialизация/потребление спец-ключа,
  выгрузка слепков etcd в бэкапный кластер, runbook DR. В t31 — только
  контрактная точка: признак в config, подстановка спец-ключа в secrets,
  чтение клиентами.
- Ротация ключей репликации/CA (roadmap, owns3/07 §12) — без изменений.
- Статические внешние S3-цели (`s3-external-targets`) — без изменений.
- Слепочная per-install пара `PGW_SNAPSHOTS_S3_*` (arch/19 §7) — НЕ
  переиспользуется и не меняется: источник правды о DR-слепках не живёт в
  восстанавливаемом etcd.
- Вынос секретов из etcd во внешний secret-manager — вне (класс экспозиции
  не расширяется, arch/14 §4).
- Перенос имени bucket панели из env в etcd — отдельная roadmap-задача
  (заводится записью в `arch/roadmap/`; см. §3.6/§7); в t31 bucket панели
  остаётся в env.

## 2. Принципы

1. **Arch-first**: контракт меняется сначала в `arch/` (фаза 0), затем код.
2. **etcd — источник правды**: креды per-cluster живут в etcd, переживают
   пересоздание контейнеров и смерть воркера (takeover ≤ TTL 15 с); env
   контейнеров инстансов — транспорт доставки значений (как у PKI, arch/23
   §6), не хранилище.
3. **Пишет только S3Worker**: `/s3/clusters/<C>/secrets` — ключ
   `/s3/`-домена, инвариант клэйма `<C>` (arch/24 §3) соблюдён; панель и
   PgWorker — читатели (PgWorker в `/s3/` не пишет — arch/23 §7).
4. **Единый формат**: secrets-ключ одинаков для всех кластеров (включая
   etcd-backup); клиенты (PgWorker, панель, будущий etcdworker) читают его
   однотипно. Различие etcd-backup кластера — только источник read-write
   значения (env спец-ключ вместо генерации) и признак в config.
5. **Значения секретов не покидают доверенную зону**: не попадают в
   журнал (`/s3worker/work/<C>`), логи, метрики, API-ответы, UI; передаются
   только в env контейнеров инстансов/джобов/агентов и в S3-клиенты
   процессов-читателей. Диагностики — только имена/признаки, никогда
   значения (прецедент SecretProfileStore/BackupSecretProfileStore).
6. **Идемпотентность каждого шага** (arch/17): генерация — txn
   put-if-absent (проигрыш → re-read, первый записавший — истина);
   пересоздание контейнера надзором — с тем же secrets-материалом.
7. **MULTI-HOST**: креды не привязаны к машине; отказ любого docker-хоста
   не теряет креды (они в etcd).
8. **Никакой миграции**: существующие кластеры (с `secret_profile` в config
   и контейнерами на старом env-профиле) пересоздаются; парсеры поле
   `secret_profile` игнорируют (unknownKeys-толерантность arch/23 §2).
   Стенд пересеивается сидом.

## 3. Структура / компоненты

### 3.1. Контракт etcd — ключ `/s3/clusters/<C>/secrets` (новый)

Формат значения — JSON, единый для всех кластеров:

```json
{"root_user":"s3w-<C>-root",
 "root_password":"<32 симв [A-Za-z0-9]>",
 "access_keys":[
   {"name":"backups","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-write"},
   {"name":"panel","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-only"}]}
```

- **Пишет**: S3Worker — лениво при provisioning (фаза P2, рядом с
  PKI-выпуском), txn put-if-absent; ротация — замена целиком (txn по
  `version==1`, см. §3.4). Удаляется автоматически deprovisioning'ом (X3
  `del --prefix /s3/clusters/<C>/`), отдельных команд чистки нет.
- **Читают**: S3Worker (env инстансов, root-креды админ-грани
  join/deny/usage/EnsureBucket), PgWorker (ключ `name=backups`,
  read-write — джобы/агенты/клиент воркера), панель (ключ `name=panel`,
  read-only — грань «Хранилище бэкапов»), t32 etcdworker (`name=backups`).
- **Генерация значений**: `root_password`/`access_key`/`secret_key` —
  криптостойкие 32 симв `[A-Za-z0-9]` (по образцу `AppSecretGenerator`,
  RandomNumberGenerator; алфавит без спецсимволов — безопасно для env,
  JSON, SigV4). `root_user` — детерминированное имя `s3w-<C>-root`
  (не секрет; удобно для диагностики и env-сверки ротатора).
- **etcd-backup кластер**: состав тот же; `name=backups` получает значения
  из env-пары воркера `S3W_ETCD_BACKUP_ACCESSKEY` /
  `S3W_ETCD_BACKUP_SECRETKEY` (полупары — fail-fast старта воркера по
  образцу §8 arch/24; при создании кластера с признаком без полного
  спец-ключа — 422). Остальные компоненты генерируются.
- Парсеры читателей толерантны к расширению полей (неизвестные поля —
  игнор, битый JSON — parseError-запись без падения тика; панель — по
  общим правилам §12.1 adminpanel/02).

### 3.2. Признак «бэкапный кластер etcd» в config (arch/23 §2)

- Поле `"etcd_backup":true` (bool, default `false`) в `config` кластера;
  ставится при создании через API (тело запроса `etcdBackup`), меняется
  только пересозданием кластера (как `platform`).
- **Единственность**: в `/s3/clusters/` не более одного кластера с
  `etcd_backup=true` — валидация API создания (409 при коллизии, чтение
  существующих config'ов перед txn).
- Канонический пример config (создание etcd-backup кластера):

```json
{"platform":"owns3","nodes":3,"etcd_backup":true,
 "created_unix":1760156800,"state":"NOT_INITIALIZED"}
```

### 3.3. S3Worker (arch/24, код `src/S3Worker.*`)

- **`ClusterSecretsStore`** (новый, `S3Worker.Provisioning`; по образцу
  `PkiStore`): генерация + txn put-if-absent, чтение с проверкой полноты
  (root-пара + оба ключа; неполный/битый — Failed с диагностикой без
  значений), подстановка внешнего спец-ключа для etcd-backup кластера.
  Модель — существующий `OwnS3NodeSecrets` (Core) + имена ключей.
- **ProvisioningProcess**: P1 — валидация env-профиля УПРАЗДНЯЕТСЯ
  (вместо неё: для etcd-backup кластера — проверка спец-ключа env);
  P2 — выпуск secrets (EnsureSecrets, put-if-absent) рядом с PKI; env
  инстансов собирается из secrets (`InstanceEnvComposer` без изменений —
  источник `OwnS3NodeSecrets` меняется с профиля на store). 0-инстансовый
  кластер: secrets генерируются (etcd-операция, docker-мутаций нет).
- **Надзор/ротатор/AddNode/RemoveNode**: все места `SecretProfileStore`
  заменяются на `ClusterSecretsStore`; пересоздание контейнера — env из
  secrets (PKI-паттерн «переживает rebuild» распространяется на креды).
- **Админ-грани ownS3** (join/deny/usage/EnsureBucket, SigV4 root) — root
  из secrets.
- **API создания** (`CreateClusterHandler`): поле `secretProfile` из
  запроса/DTO/валидатора УХОДИТ (вместе с 422-перечнем профилей);
  добавляются `etcdBackup` (опционально, bool) + гварды: единственность
  (409), полнота спец-ключа env для etcd-backup (422).
- **`SecretProfileStore` удаляется** целиком (вместе с
  `S3W_NODE_SECRETS__*`-биндингами compose/env-примерами и
  fail-fast-валидацией полупар старого формата).
- **SecretRotator**: читает заявку `/s3worker/rotations/<C>` → генерирует
  новые root+access keys (etcd-backup: генерируемые компоненты новые,
  спец-ключ пере-подставляется из env — смена его значения = правка env
  воркера + заявка ротации) → замена secrets-ключа целиком (txn) →
  rolling-пересоздание по одному за тик с новым env (объём механики не
  меняется; env-сверка — по `root_user` + составу `OWNS3_ACCESS_KEYS__<i>`)
  → del заявки. Семантика заявки/эксклюзивности — без изменений (arch/23
  §8, arch/24 §4).
- **Snapshot-парсер** (`S3SnapshotParser`): `config.etcd_backup` — в
  модель; `secret_profile` — больше не читается (игнор).
- **Seed** (`Seed.cs`, стендовый сид пулов): создание кластера без
  профиля.

### 3.4. Конкурентность ротации и provisioning

Замена secrets при ротации — txn `version(secrets)==<прочитано>` (RMW по
версии; рассинхрон с параллельным put-if-absent provisioning'а
невозможен — ротация исполняется только для Active-кластера под живым
клэймом, provisioning — только для NOT_INITIALIZED; Different-state
кластеры воркером не пересекаются).

### 3.5. PgWorker — пул бэкапов (arch/19 §7, код `src/PgWorker.*`)

- **`S3PoolStore`/`S3PoolModel`** (`PgWorker.Etcd/S3`): читает
  `/s3/clusters/<C>/secrets` → модель (root не используется; ключ
  `name=backups` read-write); `SecretProfile` из модели УХОДИТ.
- **`BackupPoolResolver`**: креды — из secrets пула (вместо
  `BackupSecretProfileStore`); отсутствие/битость secrets — существующий
  transient-путь (`Failed`-резолв → маркер `s3-pool-exhausted`, ретраи
  тиком, НЕ краш тика — arch/19 §4).
- **`BackupSecretProfileStore` удаляется**; fail-fast старта «хотя бы один
  env-профиль `PGW_BACKUP_S3_PROFILES__*`» (Program.cs, arch/19 §9)
  упраздняется — проверок наличия кредов в etcd на старте НЕ вводится
  (креды появляются/исчезают динамически, резолв — живой).
- Передача в джобы/агенты — env контейнера как раньше (bucket + списки
  endpoints по роли + креды read-write из secrets). Рестарт-луп
  контейнера при устаревших кредах после ротации → пересоздание со
  свежими env — уже канонизировано (arch/19 §3).
- Кандидаты на ротацию в PgWorker: WAL-агенты пересоздаются воркером при
  следующем супервизе-тике с новыми env (креды читаются резолвером живо);
  ephemeral джобы получают креды при создании.

### 3.6. Панель (arch/adminpanel/02 §12, код `src/AdminPanel.*`)

- **S3Parser**: разбирает `/s3/clusters/<C>/secrets` в модель снапшота
  (`S3ClusterInfo.Secrets`: root_user, состав access_keys с именами/
  политиками, ЗНАЧЕНИЯ — только для внутреннего S3-клиента); в API/UI
  значения НЕ отдаются никогда (прецедент `app_password`, arch/14 §4 —
  класс экспозиции расширяется парсингом, но не отдачей).
- **Форма создания кластера**: поле «профиль секретов» уходит; добавляется
  опция «бэкапный кластер etcd» (переключатель; фронт дублирует гварды —
  единственность, спец-ключ).
- **Мутация 1** (§12.2): отказ 422 «профиль секретов» уходит; появляются
  409 (единственность etcd-backup) и 422 (спец-ключ env неполон — только
  для etcd-backup кластера).
- **Просмотр кредов** (страница кластера S3): состав без значений —
  root_user (имя), список access keys (`name`/`policy`), признак
  «read-write внешний (спец-ключ бэкапа etcd)» для etcd-backup кластера.
- **Ротация** — существующая мутация 9 (`POST …/rotations`), без
  изменений протокола.
- **Грань «Хранилище бэкапов»** (`BackupStorageQuery`/S3-клиент панели):
  креды read-only (`name=panel`) и endpoint — из снапшота S3-домена
  (secrets + endpoints выбранного пула, перебор первого доступного);
  env `AdminPanel__Backups__S3__{AccessKey,SecretKey,Endpoint}` уходят;
  `AdminPanel__Backups__S3__Bucket` остаётся (имя bucket per-install в
  etcd не живёт — конфиг S3Worker/PgWorker; перенос bucket панели в etcd
  — отдельная roadmap-задача, вне скоупа t31 — см. §7).

### 3.7. Стенд и deploy

- `deploy/.env.example` + `deploy/.env`: блок `S3W_NODE_SECRETS__BACKUPS-MAIN__*`
  уходит; блок `PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__*` уходит; появляется
  (комментарием-опцией) пара `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}` для
  etcd-backup кластера. Комментарии про дефис в именах профилей
  (docker-compose.yml) уходят.
- `dev-stand/adminpanel/docker-compose.yml`: env
  `AdminPanel__Backups__S3__{AccessKey,SecretKey}` уходят.
- `dev-stand/adminpanel/checks/00-up.sh`: сид пулов — без профиля; fail-fast
  grep'ы `S3W_NODE_SECRETS__BACKUPS-MAIN__*` уходят.
- `checks/80-s3-panel.sh`: тело создания без `secretProfile`; проверка
  создания — по config + появлению secrets-ключа (без значений в выводе).
- `checks/45-backups-storage.sh`: root-креды больше не из
  `deploy/.env` — чек читает secrets из etcd (JSON-gateway `/v3/kv/range`,
  как воркеры; или read-only ключ `panel` вместо root — листингу хватает);
  значения в stdout не печатаются.
- Ранбук `docs/runbook.md` — раздел о зеркалировании/env-секрете профиля
  обновить (S3W_NODE_SECRETS убрать; спец-ключ описать).

## 4. Фазы работ

### Фаза 0 — arch-каноны (ПЕРВАЯ; без неё код не начинается)

1. `arch/23-s3-clusters.md`:
   - §2: ключ `secrets` в таблицу (формат, пишущий, ленивый put-if-absent,
     чистка X3); поле `etcd_backup` в config (единственность, immutable);
     `secret_profile` из таблицы/примеров §2.1 убрать.
   - §2.1: канонические примеры значений — config с `etcd_backup`,
     secrets-ключ (критерий приёмки парсеров).
   - §6: подраздел «Креды клиентской грани» переписать: per-cluster в etcd
     (генерация воркером, состав root+rw+ro, спец-ключ etcd-backup из env,
     экспозиция — «парсеры панели разбирают для S3-клиента, в UI/API не
     отдают»); env-профили `S3W_NODE_SECRETS__*` упразднить.
   - §7: PgWorker-читатель добавляет `secrets`.
   - §8: строка `rotations` — уточнить семантику (генерация + rolling).
2. `arch/24-s3worker.md`:
   - §1.1: валидация — `secret_profile`/422-профиль убрать; `etcdBackup`
     (гварды 409/422), спец-ключ env.
   - §4 «Секреты»: per-cluster в etcd (источник — ClusterSecretsStore);
     передача env; ротация — генерация+замена+rolling; спец-ключ.
   - §5 A (P1/P2): валидация профиля → генерация secrets; §5 G — root-креды
     админ-грани из secrets.
   - §8: env `S3W_ETCD_BACKUP_*`; `S3W_NODE_SECRETS__*` убрать.
   - §9: риск-строка про DR-окно ротации спец-ключа (см. §7 ограничений).
3. `arch/19-backups.md` §7: пункт (б) — креды пулов из
   `/s3/clusters/<C>/secrets` (ключ `backups`, read-write); §9 — fail-fast
   профилей упраздняется.
4. `arch/14-pgworker.md` §4: прецедент per-cluster секретов дополнить
   S3-кредами (класс экспозиции: панель разбирает для S3-клиента, не
   отдаёт в UI).
5. `arch/adminpanel/02-etcd-contract.md` §12: 12.1 — `secrets` в модель
   (значения только внутреннему S3-клиенту); 12.2 — отказы мутации 1;
   12.3 — валидация без `secretProfile`; 12.4 — креды/endpoint грани
   «Хранилище бэкапов» из снапшота.
6. `arch/roadmap/s3.md`: пункт `t31-s3-cluster-creds` — по правилам
   roadmap (удаляется мерж-коммитом задачи).

### Фаза 1 — S3Worker

`ClusterSecretsStore` (+юниты: генерация/put-if-absent/битость/спец-ключ);
`OwnS3NodeSecrets`-интеграция; ProvisioningProcess/надзор/ротатор/
AddNode/RemoveNode/Seed на secrets; API (создание без профиля, etcdBackup,
гварды); удаление `SecretProfileStore` + env-биндингов; snapshot-парсер.

### Фаза 2 — PgWorker

S3PoolStore/Model + BackupPoolResolver на secrets; удаление
`BackupSecretProfileStore` и fail-fast профилей; env джобов/агентов —
значения из etcd (точки передачи не меняются).

### Фаза 3 — панель

S3Parser (secrets → модель); форма создания (без профиля, с
etcd-backup-опцией); просмотр состава кредов; грань «Хранилище бэкапов»
на креды/endpoint из снапшота; удаление env-кредов из dev-stand compose.

### Фаза 4 — стенд/deploy/docs

`.env.example`, сид `00-up.sh`, чеки `45`/`80`, runbook, комментарии
compose.

### Фаза 5 — тесты (мерж-гейт)

- Юниты: ClusterSecretsStore (AAA), парсеры (канонические примеры §2.1),
  валидатор API (etcdBackup-гварды), ротатор (генерация+пере-подстановка
  спец-ключа), резолвер PgWorker (secrets-путь, transient-отказы).
- Обновить существующие: ProvisioningProcessTests, SecretRotatorTests,
  ClusterMutationsApiTests, S3SnapshotParserTests, S3PoolMappingTests,
  BackupPoolResolver-тесты, S3E2eRotationTests, фикстуры
  (clusters-canonical.json), Program.cs-валидации обоих воркеров.
- Интеграция/E2E: создание кластера без профиля → provisioning →
  secrets-ключ в etcd (значения не в журнале/логах); ротация — новые
  креды в etcd + rolling + PgWorker подхватывает (рестарт-луп агентов);
  etcd-backup кластер (спец-ключ из env, 422 без env, 409 второй);
  изоляция E2E по docs/e2e-isolation.md (guid, own-only чистка, ассерт
  чистоты, динамические порты, BrokerBootSec ≤ 100 с).
- Мерж-гейт: docker-E2E PgWorker на свежем Release (кейс-маркер
  `Scale_AddEmptyShard`; полный E2eFixture — provisioning/ротация
  затронуты) + E2ePrepare перед тестами; зачистка контейнеров/сетей
  между сериями.

## 5. Ограничения

- Значения секретов — никогда в журнал/логи/метрики/API/UI/stdout чеков.
- Класс экспозиции etcd не расширяется beyond «панель разбирает для
  внутреннего S3-клиента» (без per-key ACL — как arch/14 §4).
- Никакой backward-миграции: `config.secret_profile` игнорируется,
  кластеры пересоздаются; env-профили удаляются одним мержем (стенд
  пересеивается).
- `.NET 10, Nullable=enable, TreatWarningsAsErrors=true`;
  централизованные версии пакетов; комментарии/документация — по-русски,
  идентификаторы — по-английски; тесты — AAA.
- Правила E2E: динамические порты, полный teardown при любом исходе,
  ассерт чистоты, `[PHASE]`-метки фаз > 60 с, без перезапуска упавших.
- Таймауты ожиданий ≤ 30 с (агент/субагент), поллинг-инварианты t19.

## 6. Критерии приёмки

1. **Контракт**: arch/23 §2/§2.1/§6/§7/§8, arch/24 §1.1/§4/§5/§8,
   arch/19 §7/§9, arch/14 §4, adminpanel/02 §12 обновлены до кода
   (первая фаза); `secret_profile` из канонов исчез; в git-истории
   примеров больше нет.
2. **Создание кластера**: без профиля; API принимает `etcdBackup`;
   400/409/422-семантика — по §3.2/§3.3 (409 второй etcd-backup, 422 без
   спец-ключа env).
3. **Provisioning**: у Active-кластера в etcd есть
   `/s3/clusters/<C>/secrets` единого формата; env инстансов собран из
   него; значения отсутствуют в `/s3worker/work/<C>`, логах воркера,
   docker-inspect-выводах тестов.
4. **etcd-backup кластер**: `name=backups` равен спец-ключу из
   `S3W_ETCD_BACKUP_*`; root/`panel` сгенерированы; признак в config.
5. **Ротация**: заявка → новые root+ключи в secrets (etcd-backup:
   спец-ключ пере-подставлен из env) → rolling-пересоздание (по одной за
   тик) → заявка погашена; PgWorker-агенты пересоздаются со свежими креды
   без ручного вмешательства.
6. **PgWorker**: пул бэкапов пишет/читает с кредами из etcd;
   `PGW_BACKUP_S3_PROFILES__*` отсутствует в коде/стенде; fail-fast
   старта про профили удалён; transient-отсутствие secrets — маркер
   `s3-pool-exhausted`, не краш.
7. **Панель**: кластер создаётся без поля профиля; страница кластера
   показывает состав кредов (имена/политики, без значений — проверяется
   отсутствием значений в API-ответах); грань «Хранилище бэкапов» работает
   на креды/endpoint из снапшота; env-креды панели удалены.
8. **Стенд**: `00-up.sh` поднимает полный контур; `deploy/.env` не
   содержит `S3W_NODE_SECRETS__*`/`PGW_BACKUP_S3_PROFILES__*`; чеки 45/80
   зелёные (креды чека — из etcd, без печати значений).
9. **Тесты**: юниты/интеграция/E2E фазы 5 зелёные; мерж-гейт E2E на
   свежем Release пройден; серии зачищены (контейнеры/сети/тома).
10. **Чистота кода**: `grep -r "S3W_NODE_SECRETS\|PGW_BACKUP_S3_PROFILES\|secret_profile"`
    по `src/`, `deploy/`, `dev-stand/` — пусто (кроме arch-истории в git).

## 7. Открытые вопросы и вызовы исполнителя (не блокируют план)

- **DR-окно ротации спец-ключа** (риск для arch/24 §9): спец-ключ живёт в
  env воркера и (копией) в secrets etcd; слепок etcd, снятый до ротации,
  несёт старое значение — восстановление из него даёт etcdworker'у
  устаревший ключ до первой свежей записи. Фиксируется риск-строкой;
  оперативное лечение — runbook t32 (пере-подстановка через заявку
  ротации). В t31 механик DR не строим.
- **Имена** `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}`, поле `etcd_backup`,
  `root_user=s3w-<C>-root`, имена ключей `backups`/`panel` — вызовы
  исполнителя по прецедентам именования (`S3W_*`, snake_case config);
  утверждение — ревью фазы 0 (arch-правки).
- **Bucket панели**: `AdminPanel__Backups__S3__Bucket` остаётся env (имя
  bucket per-install в etcd не живёт). Перенос bucket панели из env в etcd
  — отдельная roadmap-задача: main-агент заводит её записью в
  `arch/roadmap/` в этой же ветке (уходит мержем t31 вместе с настоящим
  spec); исполнение переноса — вне скоупа t31.
