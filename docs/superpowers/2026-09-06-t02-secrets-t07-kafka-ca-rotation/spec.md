# Spec: ротация per-cluster секретов PgWorker (t02) и ротация per-cluster CA/сертов KafkaWorker (t07)

Задачи roadmap: [`t02-per-cluster-secrets`](../../../arch/roadmap/pgworker.md) и
[`t07-kafka-ca-rotation`](../../../arch/roadmap/kafkaworker.md) — обе закрываются
этой спекой одним проходом dev-flow (общая тема: ротация per-cluster
креденциальных материалов без остановки записи).

## 1. Цель

**Часть A (t02, PgWorker).** Все per-cluster секреты PgWorker генерируются
воркером, хранятся в etcd и ротируются по одной заявке без остановки записи:

1. `bucket_mover` переводится с per-install env-секрета на **per-cluster
   генерацию** в etcd (по образцу app-секрета feat-etcd-password-field).
2. Заявка `/pgworker/rotations/<C>` расширяется: один тикет ротирует **все
   per-cluster креды кластера** — `app`, `bucket_admin`, `bucket_mover`
   (сегодня тикет ротирует только app).
3. «Интеграция с secret-manager»: **etcd — единственное хранилище**
   per-cluster секретов (решение пользователя). Внешний secret-manager — вне
   scope, откладывается отдельным roadmap-пунктом (см. §7).

**Часть B (t07, KafkaWorker).** Ротация per-cluster CA и серверных
сертификатов брокеров без остановки записи и разрыва клиентов: окно двойного
доверия (OLD+NEW CA в truststore и в точке дискавери `ca_pem`), rolling-
пересоздание брокеров, финальный коммит на NEW CA. Запуск — ручная заявка
панели (протокол §9.8).

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Что такое «интеграция с secret-manager» | etcd = единственное хранилище; внешняя SM-интеграция — отдельная отложенная задача |
| Какие per-cluster секреты ротируются | mover → per-cluster; ротация bucket_admin и bucket_mover по той же заявке (app уже есть) |
| Триггер ротации CA (t07) | ручная заявка панели; авто-мониторинг сроков — вне scope |

## 2. Принципы

1. **arch-first**: правки контракта — сначала в `arch/` (arch/14, arch/15,
   arch/16, arch/adminpanel/02), затем отражение в коде.
2. **etcd — единственное хранилище per-cluster секретов**; генерирует
   воркер-держатель клэйма; панель секреты не читает (bucket_admin/mover) и
   не пишет (прокси в API воркера — arch/02 §9.8, arch/14 §1.1).
3. **Протокол заявок §9.8 без изменений**: клэйм-txn `version==0` + put
   `{"requested_unix","requested_by"}`; повторная заявка → 409; исполнение —
   только держателем клэйма `<C>`.
4. **Ротация без остановки записи**: `ALTER ROLE` не убивает живые
   соединения; клиенты перечитывают etcd и реконнектятся (окно ошибок
   реконнекта — как у существующей ротации app-пароля, arch/14 §5 I).
5. **Атомарный коммит**: все записи etcd по итогам ротации — ОДНА txn
   (put новых значений + del заявки), journal-фазы до/после; transient-сбой —
   заявка жива, следующий тик повторяет с начала (идемпотентно).
6. **Kafka-специфика**: inter-креды и CONTROLLER-кворум не ротируются
   (канон arch/16 §2.2); ротация CA не меняет SASL-роли и ACL; доверие
   клиентов — по CA, поэтому замена серта ноды в рамках доверенного CA/окна
   клиентов не ломает.
7. Язык: документация — русский, идентификаторы — английский.

## 3. Контракт etcd (канон — правки arch/)

### 3.1. Новые ключи PgWorker (arch/14 §3.3/§4, префикс `/clusters/<C>/`)

| Ключ | Кто пишет | Значение |
|---|---|---|
| `/clusters/<C>/mover_password` | ensure (P1.5/R1/adopt), ротация R3 | per-cluster пароль роли `bucket_mover`; строка 32 симв `[A-Za-z0-9]` (генератор app-секрета). Имя роли фиксировано (`bucket_mover`, REPLICATION) — отдельного `mover_user` нет |
| `/clusters/<C>/bucket_admin_user` | ensure, ротация R3 | per-cluster user DSN-точки входа (канон после этой задачи; `"bucket_admin"` по умолчанию) |
| `/clusters/<C>/bucket_admin_password` | ensure, ротация R3 | per-cluster пароль bucket_admin |

Правила чтения bucket_admin кредов (порядок приоритета): ключи
`bucket_admin_*` → `config.BucketAdmin*` → env `PGW_BUCKET_ADMIN_*`
(переходный fallback; новый кластер всегда получает ключи через ensure).
`mover_password`: ключ → env `PGW_BUCKET_MOVER_PASSWORD` (переходный
fallback). env-переменные не удаляются: superuser/standby остаются
per-install (Patroni/ноды), остальные — fallback/bootstrap до ensure.

### 3.2. Изменение семантики `/pgworker/rotations/<C>` (arch/14 §3.3/§5 I, arch/adminpanel/02 §9.8)

Формат заявки не меняется (`{"requested_unix","requested_by"}`); расширяется
семантика исполнения: тикет ротирует **все** per-cluster креды кластера
(`app`, `bucket_admin`, `mover`). Панельный эндпоинт
`POST /api/clusters/{c}/app-password/rotate` переименовывается в
`POST /api/clusters/{c}/secrets/rotate` (панельная команда-прокси —
аналогично; старый путь удалить в одном коммите с новым — панель и воркер
выкатываются вместе). Тексты UI: «ротация per-cluster секретов кластера»
(модалка предупреждает о кратком окне ошибок реконнекта приложений).

### 3.3. Новые ключи KafkaWorker (arch/15 §4, префикс `/kafka/clusters/<C>/`)

| Ключ | Кто пишет | Значение |
|---|---|---|
| `/kafka/clusters/<C>/ca_next_key` | ротатор, фаза prepare | приватный ключ НОВОЙ CA (PKCS#8 PEM), staging на окно ротации |
| `/kafka/clusters/<C>/ca_next_pem` | ротатор, фаза prepare | публичный серт НОВОЙ CA, staging |
| `/kafka/clusters/<C>/ca_pem` | ротатор, фазы dual-trust → commit | в окне ротации — **bundle** (конкатенация PEM OLD+NEW), после коммита — только NEW |
| `/kafka/clusters/<C>/ca_key` | ротатор, фаза commit | перезаписывается ключом NEW CA после rolling-фазы |
| `/kafkaworker/ca_rotations/<C>` | панель-прокси → API воркера | заявка §9.8 (формат один в один с `rotations`/`admin_rotations`) |

`ca_next_*` живут только в окне ротации (удаляются в фазе commit); до
ротации ключи отсутствуют — ensure их не создаёт. Парсеры снапшота
(KafkaSnapshotParser, панельный KafkaParser) игнорируют незнакомые ключи —
обратная совместимость сохраняется.

### 3.4. Правки arch/

- **arch/14-pgworker.md**: §3.3 (новые ключи, переименованный эндпоинт),
  §4 Секреты (mover/bucket_admin — per-cluster, env — fallback), §5 I
  (переименование процесса в `ClusterSecretRotator`, R1–R3 с тремя ролями и
  перезаписью dsn), §8 (панельные команды).
- **arch/15-kafka-clusters.md**: §4 (ключи `ca_next_*`, `ca_rotations`,
  bundle-семантика `ca_pem`), §5 (дискавери: truststore из `ca_pem` — bundle
  в окне ротации, PEM-конкатенация стандартна для truststore).
- **arch/16-kafkaworker.md**: §2.3 (канон ротации CA: окно двойного доверия,
  порядок фаз, судьба OLD-ключа), §4/§5 (процесс `CaRotator`, фазы),
  §7 (риски R8/R10 — ссылка на ротацию как на закрытие).
- **arch/adminpanel/02-etcd-contract.md**: §9.8 (семантика pg-заявки —
  все per-cluster креды), таблица kafka-команд (новая команда №17
  `POST /api/kafka/clusters/{c}/ca/rotate`), чтение `ca_pem`-bundle пробами.
- **arch/roadmap/** (мерж-гейт): удалить `t02-per-cluster-secrets` и
  `t07-kafka-ca-rotation`; дописать новую отложенную задачу «интеграция с
  внешним secret-manager» (трек pgworker, очередной свободный NN).

## 4. Структура и компоненты (отражение в коде)

### 4.1. PgWorker — per-cluster креды и ClusterSecretRotator

- **`AppSecretEnsurer` → `ClusterSecretEnsurer`** (PgWorker.Provisioning,
  интерфейс `IAppSecretEnsurer` → `IClusterSecretEnsurer`): ensure тройки
  кредов (app, mover, bucket_admin) txn put-if-absent по §3.1. Вызывается из
  P1.5 (Provisioning/AddShard), adopt-процесса и R1 ротатора.
- **`InstallSecrets`**: `MoverPassword`/`BucketAdminPassword` остаются как
  fallback-значения для гвардов ролей до ensure (BuildRoleGuardsSql получает
  актуальные креды из ensure-результата, не из env напрямую).
- **`ClusterSnapshot`**: парсер дополняется `MoverPassword`,
  `BucketAdminUser`, `BucketAdminPassword` (новые ключи; null при
  отсутствии — fallback на env в DSN-билдерах до первого ensure).
- **DSN-билдеры** (`ShardEndpoints.BuildMoverDsn`, `MoveProcess`,
  `DatabaseProvisioner.BuildAdminDsn`): mover/bucket_admin креды берутся из
  снапшота (per-cluster), env — только fallback. DSN-ключи шардов при
  провижининге продолжают встраивать фактические bucket_admin креды.
- **`AppPasswordRotator` → `ClusterSecretRotator`** (файл переименовать,
  класс расширить):
  - R0/R-старт, malformed-ticket, journal — без изменений;
  - R1: ensure тройки (§ выше) — OLD-значения существуют;
  - R2: на мастере каждого шарда с dsn — `ALTER ROLE` для app, bucket_admin
    и bucket_mover (три SQL-текста из DatabaseProvisioner);
  - R3: **одна txn**: put `app_password`, `mover_password`,
    `bucket_admin_user`, `bucket_admin_password` + перезапись dsn-ключей
    всех шардов (новый bucket_admin password, regex-замена по образцу
    `ShardEndpoints.UserRegex`) + del заявки; compare по существующим OLD-
    значениям ключей (ключ, которого не было — NotExists-compare);
  - R4: снапшот P12 + journal done.
  - Кейс bucket_admin: если до ротации креды жили только в env/config —
    после ротации канон — ключи (§3.1), dsn перезаписан, fallback больше не
    используется этим кластером.
- **API** (`PgWorker.App/Api`): `RotateAppPasswordHandler` →
  `RotateClusterSecretsHandler` (маршрут `/api/clusters/{c}/secrets/rotate`,
  та же claim-txn-механика).

### 4.2. KafkaWorker — CaRotator (фазы, образец PasswordRotator §5 H)

Новый процесс `CaRotator` (KafkaWorker.Provisioning.Processes), тик по
заявке `/kafkaworker/ca_rotations/<C>`, только держатель клэйма, journal
`op=rotate-ca`:

- **Фаза P (prepare)**: генерация НОВОЙ CA (`ClusterPki.GenerateCa`,
  случайно — не из сида); txn put-if-absent `ca_next_key`/`ca_next_pem`.
  Повторный тик — re-read, ключи уже есть.
- **Фаза D (dual-trust)**: txn put `ca_pem` ← OLD+NEW bundle
  (конкатенация PEM). После фазы все клиенты, перечитавшие `ca_pem`
  (приложения — 15 §5, панель-пробы), доверяют сертам обоих CA. Старый
  `ca_key` не трогается.
- **Фаза R (rolling)**: по одному брокеру за тик (порядок — как у
  PasswordRotator фазы A: сначала обычные брокеры, кворумные контроллеры —
  в конце, по одному): env брокера пересобирается с сертом, подписанным
  NEW CA (`BrokerCertificateCache.GetOrCreate` с NEW-материалом; ключ кеша
  по хешу CA-ключа — новый серт детерминированно), truststore = bundle
  `ca_pem` (OLD+NEW); пересоздание контейнера механикой NodeRegenerator
  (том сохраняется), ожидание healthy/ISR до следующего брокера. Каждый
  шаг — journal `rolling/<broker>`.
- **Фаза C (commit)**: все брокеры на NEW — **одна txn**: put `ca_pem` ←
  NEW, `ca_key` ← NEW key, del `ca_next_*`, del заявки; compare
  `value(ca_next_key)==сгенерированный` (защита от параллельной ротации).
  OLD-ключ CA уничтожается перезаписью — после окна он никем не доверяется.
- **Фаза F (финал)**: снапшот-делегат + journal done.
- **NodeEnvBuilder**: спека брокера получает раздельные
  `SigningCaPem/SigningCaKey` (подпись серта) и `TrustCaPem` (truststore,
  bundle) — в норме совпадают, в окне ротации различаются.
- **API** (`KafkaWorker.App/Api`): `POST /api/kafka/clusters/{c}/ca/rotate`
  — claim-txn `version==0` + put заявки (образец admin-password/rotate);
  409 при активной ротации.
- **Пробы панели** (`AdminPanel.Probes`, librdkafka): `ca_pem`-bundle
  подаётся как есть (ssl.ca.location/PEM-коллекция поддерживает
  конкатенацию) — изменений в коде проб, кроме передачи значения, не
  требуется; проверить тестом.

### 4.3. AdminPanel

- Команда-прокси pg: `RotateAppPasswordCommand` → `RotateClusterSecretsCommand`
  (новый путь API воркера, §3.2); UI-модалка pg-кластера — текст
  «ротация per-cluster секретов».
- Команда-прокси kafka: `RotateCaCommand` → `POST /api/kafka/clusters/{c}/ca/rotate`;
  UI-модалка kafka-кластера: кнопка «Ротация CA» с предупреждением о
  rolling-пересоздании брокеров и окне двойного доверия.
- Пробы/снапшот: чтение `ca_pem` — без структурных правок (bundle — строка).

### 4.4. Тесты

- **Юниты pg**: ensure тройки (put-if-absent, гонки), ротатор (три ALTER,
  txn-коммит с перезаписью dsn, compare/конфликты, malformed, mover-fallback
  до ensure), DSN-билдеры из снапшота.
- **Юниты kfw**: фазы CaRotator на фейковых etcd/docker-гранях (P→D→R→C→F,
  re-entry после сбоя на каждой фазе), bundle-конкатенация, 409-семантика
  API, NodeEnvBuilder (Signing/Trust раздельно).
- **Интеграция pg** (docker): ротация на живом кластере — writer-нагрузка не
  останавливается (подключения реконнектятся после перечитывания), dsn-ключи
  обновлены, заявка снята.
- **Интеграция kfw** (docker, TLS-кластер): ротация CA — клиенты с OLD-CA
  доверяют в окне (bundle), после коммита — только NEW; брокеры
  пересоздаются по одному, ISR восстанавливается.
- **E2E на свежем Release** — обязателен (трогаются `PgWorker.App/` и
  `Provisioning`): полный `E2eFixture` (8/8) + ротационный сценарий.

## 5. Фазы (порядок исполнения)

1. **arch/**: правки arch/14, arch/15, arch/16, arch/adminpanel/02 (§3.4) —
   контракт всех ключей и фаз до кода.
2. **PgWorker per-cluster креды**: ClusterSecretEnsurer (ensure тройки),
   снапшот-парсер, DSN-билдеры, гварды ролей; юнит-тесты.
3. **PgWorker ClusterSecretRotator + API + панель-прокси**: расширение
   ротатора, переименование эндпоинта, UI; юнит-тесты.
4. **Интеграция pg**: docker-тест ротации с writer-нагрузкой.
5. **KafkaWorker CaRotator**: NodeEnvBuilder (Signing/Trust), фазы P/D/R/C/F,
   API-эндпоинт; юнит-тесты.
6. **Интеграция kfw**: TLS-кластер, docker-тест окна двойного доверия.
7. **AdminPanel**: прокси-команды и UI-модалки (pg + kafka).
8. **E2E Release + документация**: полный E2eFixture, README/deploy-заметки
   (env-фоллбеки, генерация пакетов), roadmap-правки — в мерж-коммите.

## 6. Ограничения

- Внешний secret-manager — вне scope (отложенный roadmap-пункт).
- Per-install секреты (superuser/standby, API/Docker TLS-пакеты) — вне
  scope ротации; env-переменные не удаляются (fallback/bootstrap).
- inter-креды Kafka и CONTROLLER-кворум не ротируются (arch/16 §2.2).
- Авто-мониторинг сроков действия CA/сертов — вне scope (серты 10 лет).
- .NET 10, `TreatWarningsAsErrors`; порты в тестах — динамические; после
  каждой тестовой серии — зачистка контейнеров и сетей; таймауты интеграционных
  фикстур ≤ 100 с; E2E — на свежем Release.
- Панель секреты (bucket_admin/mover) не читает и не отображает.

## 7. Риски и их закрытие

| Риск | Закрытие |
|---|---|
| Окно ошибок приложений при ротации bucket_admin (пароль внутри dsn) | Живые соединения не рвутся (ALTER не убивает); новые подключения после перечитывания dsn — как у app-ротации; UI-предупреждение |
| Ротация во время активного переезда (mover-DSN) | MoveProcess строит DSN из свежего снапшота на тик; упавшая фаза возобновляется с journal-фазы с новыми кредами; заметка в runbook/UI |
| Клиенты с кэшированным OLD `ca_pem` после rolling-фазы | Фаза D кладёт bundle ДО пересоздания брокеров — клиенты с OLD-bundle доверяют NEW; коммит NEW-only — после того как все брокеры на NEW |
| Параллельная ротация CA / внешняя запись etcdctl | put-if-absent `ca_next_*`, compare в фазе C, 409 панели при живой заявке |
| Панельные пробы не понимают bundle | PEM-конкатенация стандартна для truststore/librdkafka; интеграционный тест пробы на bundle |
| Расхождение spec↔arch при правках | arch-правки — фаза 1 до кода; ревью plan↔spec и кода — по чек-листам |

## 8. Критерии приёмки

**Часть A (t02):**
1. Заявка `/pgworker/rotations/<C>` ротирует app + bucket_admin + mover:
   после тика — новые значения в etcd, dsn-ключи всех шардов содержат новый
   bucket_admin-пароль, заявка снята, journal `done`, все роли ALTER'нуты на
   мастерах всех шардов с dsn.
2. Новый кластер получает per-cluster mover/bucket_admin ключи при
   провижининге/adopt/add-shard (ensure); env используется только как
   fallback до ensure.
3. `POST /api/clusters/{c}/secrets/rotate` (панель-прокси) работает; старый
   путь `app-password/rotate` удалён вместе с панельной командой.
4. Интеграционный тест: writer-нагрузка переживает ротацию без остановки
   записи (допустимы ретраи реконнекта).

**Часть B (t07):**
5. Заявка `/kafkaworker/ca_rotations/<C>`: prepare → dual-trust → rolling
   (по одному брокеру, healthy между шагами) → commit: `ca_pem`=NEW,
   `ca_key`=NEW, `ca_next_*` удалены, заявка снята, journal по фазам.
6. В окне ротации клиенты, доверяющие OLD CA и bundle, подключаются к
   брокерам обоих поколений сертов (интеграционный тест).
7. `POST /api/kafka/clusters/{c}/ca/rotate` — claim-txn, 409 при активной
   ротации; панельная модалка с предупреждением.

**Общие:**
8. arch/14, arch/15, arch/16, arch/adminpanel/02 согласованы с кодом
   (ключи, фазы, имена процессов/эндпоинтов).
9. Юниты → интеграция → E2E зелёные; E2E — свежий Release, полный
   E2eFixture; docker-зачистка после каждой серии.
10. Мерж-коммит: t02/t07 удалены из arch/roadmap/, добавлен пункт про
    внешнюю SM-интеграцию.
