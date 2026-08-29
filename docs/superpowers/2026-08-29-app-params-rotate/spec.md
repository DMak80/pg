# Спецификация: per-node app_params в etcd + ротация app-пароля кластера (2026-08-29)

## 1. Цель и контекст

Позиция пользователя: **клиентское приложение не должно додумывать настройки
сервера, влияющие на соединение**. Все серверные (не клиентские) параметры
подключения должны читаться из etcd, чтобы клиент ничего не предполагал.

Сегодня клиентский DSN роутера (arch/11 §3 п.6) содержит хардкод
`sslmode=require` — сервернозависимая настройка зашита на клиенте. Per-cluster
креды уже есть (`/clusters/<C>/app_user`, `/clusters/<C>/app_password`,
генерирует PgWorker, arch/14 §4 гр. 1), но: (а) серверных параметров
подключения в etcd нет вовсе; (б) смена app-пароля на живом кластере
невозможна — пароль выравнивается только при provisioning/add-shard/rebuild.

Задача:

1. **`app_params`** — новый per-node ключ: части строки подключения
   Npgsql/libpq, которые зависят от сервера и конкатенируются (concat) клиентом
   к своей строке (`sslmode`, `Trust Server Certificate` и т.п.). Ведёт
   PgWorker (provisioning). Задаётся **для каждой ноды кластера отдельно**
   (параметры подключения к конкретной ноде; могут меняться в дальнейшем).
2. **Ротация app-пароля всего кластера**: обойти все ноды (роль `app` в
   PostgreSQL каждого шарда), обновив etcd-ключ `app_password`.
3. **Кнопка смены app-пароля** в UI AdminPanel на странице кластера.

Контракты **уже обновлены в arch/** (arch-first, тем же изменением, что и
этот spec): `arch/11-bucket-sharding.md` §2/§3, `arch/14-pgworker.md`
§3.1/§3.2/§3.3/§5/§8, `arch/adminpanel/02-etcd-contract.md` (§2.1, §9.8,
шапка), `arch/adminpanel/03-panels.md` (§1.6, §2, §3). Данный файл —
исполнимая спецификация отражения контрактов в коде.

## 2. Принципы

- **arch-first**: контракт etcd — источник истины; код следует за ним (уже
  сделано, §1).
- **Клиент не додумывает**: хост/порт — master-ключ, dbname — config,
  креды — app_user/app_password, серверные параметры соединения —
  app_params ноды. В DSN роутера не остаётся ни одного сервернозависимого
  хардкода (`sslmode=require` переезжает из arch/11 §3 в значение app_params).
- **Панель декларирует — PgWorker исполняет**: панель НЕ ходит в SQL нод
  (SQL-проба панели read-only: `default_transaction_read_only=on`), НЕ пишет
  `app_password`/`app_params` (ключи PgWorker). Кнопка ротации = заявка в
  etcd по образцу move-заявок (§9.7) и recreate-маркеров; мутации БД — только
  держатель клэйма `<C>` (инвариант arch/14 §3.3).
- **Идемпотентность**: каждый шаг перепроверяет факт; put-if-absent для
  app_params (ручные правки оператора живы); повторная ротация после любого
  сбоя безопасна.
- **Per-node гранулярность app_params**: контракт — на ноду (не на шард/кластер):
  ноды одного шарда обычно несут одинаковое значение, но поэтапные изменения
  (например перевод на verify-ca по одной ноде) не требуют отдельного
  механизма.

## 3. Контракт etcd (новое)

### 3.1. Ключ `app_params` (per-node)

```
/clusters/<C>/shards/<X>/nodes/<n>/app_params = "sslmode=require"
```

- Формат значения: libpq-строка `keyword=value`, пары через пробел
  (`"sslmode=require"`, теоретически `"sslmode=verify-ca "
  "sslrootcert=..."`-класс параметров — строка целиком конкатенируется
  клиентом). Запрещённые (клиентские) ключевые слова в значении: `host`,
  `port`, `dbname`, `user`, `password` — PgWorker их никогда не пишет;
  конвенция для ручных правок (валидации ручных значений нет — осознанно).
- Дефолт при создании: `PgWorker:AppParams:Default` (appsettings; дефолт
  `"sslmode=require"` — P17: doorman `tls_mode=require`, прямой pg-порт Spilo
  тоже SSL; «require, не verify» — канон arch/13 §4).
- Писатель: ТОЛЬКО PgWorker, put-if-absent (txn `NotExists` + put):
  существующее значение (в т.ч. изменённое оператором etcdctl'ом) НЕ
  перезаписывается — механизм изменения «в дальнейшем» = ручной etcdctl,
  PgWorker reconciliation существующих значений НЕ делает.
- Пустое значение ключа допустимо («нет серверных параметров»), тоже не
  перезаписывается; отсутствие ключа = не обеспечен (клиент обязан
  интерпретировать как ошибку конфигурации, НЕ подставлять дефолт сам —
  иначе снова «додумывание»).
- Читатели: приложение-роутер (concat к DSN); PgWorker (наличие — для
  ensure); панель — expected-skip (не читает, не отображает, без
  unknownKeys-счётчика — как app_user/app_password).
- Жизненный цикл: пишется при provisioning/add-shard (после dsn) и
  миграционно в надзоре; переживает rebuild ноды (ключ в etcd, не в
  контейнере); удаляется вместе с префиксом шарда/кластера (S3/D2 — уже
  существуют).

### 3.2. Заявка ротации `/pgworker/rotations/<C>`

```
/pgworker/rotations/<C> = {"requested_unix":<unix>,"requested_by":"<username>"}
```

- Пишет панель (мутация §9.8): ОДНА txn `[compare version==0] [put]`
  (клэйм-паттерн §9.7 п.5); уже стоящая заявка → 409 (панель не
  перезаписывает живые заявки; отмена — runbook/etcdctl).
- Читает/удаляет PgWorker (AppPasswordRotator): удаление — в той же txn,
  что и put нового `app_password` (атомарность «коммит + снятие заявки»).
- Deprovisioning D2: точечный `del` (заявки не переживают удаление кластера).

## 4. Компоненты и изменения

### 4.1. PgWorker — модель и парсер (`src/PgWorker.Core/Model/Domain.cs`,
`src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs`)

- `NodeSpec` → добавить `string? AppParams = null` (nullable — ключа нет).
- Парсер: case `shards/.../nodes/<n>/app_params` (segments.Length==8,
  segments[7]=="app_params") → значение в аккумулятор ноды (пустое → null).
- Конфиг `PgWorkerOptions`: секция `AppParams { string Default = "sslmode=require" }`
  (arch/14 §8).

### 4.2. PgWorker — `AppParamsEnsurer` (новый, `src/PgWorker.Provisioning/Processes/`)

По образцу `AppSecretEnsurer` (failover по endpoints, txn put-if-absent):

- `EnsureNodeAsync(cluster, shard, node)`: ключа нет → txn
  `[NotExists] [put Default]`; проигрыш compare — законный исход (никто
  не перезаписывает). Возвращает ничего (Result).
- `EnsureShardAsync(cluster, shard, nodes)`: цикл по нодам шарда.
- Вызовы (все — под клэймом `<C>`):
  - **ProvisioningProcess**: новая фаза **P2.5'** после записи dsn
    (`ProvisionShardSqlAsync`) — ensure всех нод шарда;
  - **AddShardProcess**: в SQL-фазе A5 после dsn (см. §4.4);
  - **NodeSupervisor**: ленивая миграция — в тике надзора для всех нод
    шардов **с dsn** (зарегистрированных), у которых `AppParams == null`
    (кластеры, созданные до app_params; состояние ноды не важно — клиент
    строит DSN к мастеру, мастером может стать любая нода). Модель
    снапшота уже несёт наличие ключа → без etcd-запросов на прогон; после
    первого обеспечения тики — no-op. Ноды шарда без dsn — домен
    AddShardProcess (A5 обеспечит).

### 4.3. PgWorker — `AppPasswordRotator` (новый процесс, arch/14 §5 I)

Файл `src/PgWorker.Provisioning/Processes/AppPasswordRotator.cs`; вход —
снапшот кластера + чтение заявки; зависимости: `IEtcdGateway`, endpoints,
`ISqlExecutor`, `ShardProbe`, `ClaimStore`, `WorkJournal`, `InstallSecrets`,
`IAppSecretEnsurer`, генератор `AppSecretGenerator`. Ди — через
`IClusterProcesses` (новый метод `RotateAppPasswordAsync(snap, ct)`).

Машина состояний одного тика (все шаги идемпотентны):

- **R0**: заявка `/pgworker/rotations/<C>` читается по снапшоту тика
  (префикс `/pgworker/` ReconcileLoop не читает — процесс читает ключ сам,
  одним GET с failover); заявки нет → Done(no-op). Клэйм-гвард: мутации —
  только держателем. Journal: op=`rotate-app-password`, phase=`started`.
- **R1**: `{app_user, app_password}` → `IAppSecretEnsurer.EnsureAsync`
  (отсутствующие создаст put-if-absent — P1.5); `OLD` = текущий app_password.
- **R2**: `NEW = AppSecretGenerator.Generate()`. Для каждого шарда **с dsn**
  (поднятого; шард без dsn — домен AddShardProcess, роль там создаётся/выравнивается
  по свежему app_password): мастер (`ResolveMaster`-паттерн: master-ключ →
  fallback Patroni REST, как в ProvisioningProcess) → admin-DSN
  (`DatabaseProvisioner.BuildAdminDsn`, user=postgres) →
  `DatabaseProvisioner.BuildAlterAppPasswordSql(new AppCredentials(appUser, NEW))`.
  Реплики получают `pg_authid` физической репликацией — ALTER только на
  мастере шарда. Любой сбой (нода/мастер недоступен, SQL-ошибка) →
  **transient**: journal `last_error`, заявка жива, `app_password` НЕ
  меняется, тик завершён; следующий тик повторяет **с начала со свежим NEW**
  (регенерация безопасна: ALTER — идемпотентная перезапись). Перманентных
  отказов нет (несуществующий шард/роль создаются ensure'ом; битая заявка —
  чужой JSON читается толерантно, заявка без `requested_unix` игнорируется и
  удаляется как мусор с journal-записью).
- **R3**: все шарды OK → ОДНА txn:
  `[compare value(/clusters/<C>/app_password)==OLD или NotExists]
   [put app_password=NEW; delete /pgworker/rotations/<C>]`.
  Compare проигран (внешняя запись etcdctl между R1 и R3) → re-read, ретрай
  тиком со свежим OLD (безопасно: ALTER перезапишет).
- **R4**: снапшот P12 (точка изменения — делегат, как A6/P5) + journal
  `phase=done`.

Порядок в `ReconcileLoop.ProcessClusterAsync` (default/Active-ветка):
supervise → scale-shards → **rotate-app-password** → evacuate → moves.
Обоснование: ротация — короткая (секунды) плановая операция, не должна
ждать за длинными переездами; перед эвакуацией — потому что эвакуация
реагирует на аварию и порядок с ней неважен.

`DeprovisioningProcess` D2: добавить точечный
`del /pgworker/rotations/<C>` (рядом с `/pgworker/moves/<C>/`).

### 4.4. PgWorker — закрытие гонки «add-shard ↔ ротация»

`AddShardProcess.ProvisionShardSqlAsync`: перед выполнением
`BuildAlterAppPasswordSql` — **свежий re-read** app-кредов
(`IAppSecretEnsurer.EnsureAsync` повторно в SQL-фазе), т.к. между
началом тика (ensure до подъёма контейнеров) и SQL-фазой прошли минуты
ожидания Patroni — ротация могла сменить app_password. Окно «между re-read
и ALTER» — миллисекунды; residual-риск коллизии закрывается повторной
ротацией (кнопка). Также A5: после dsn — `AppParamsEnsurer.EnsureShardAsync`.

`ProvisioningProcess` гонки не имеет: кластер NOT_INITIALIZED — rotator
работает только в Active-ветке.

### 4.5. AdminPanel — backend

- **Парсер** `src/AdminPanel.Etcd/Parsing/ClustersParser.cs`: case
  `segments.Length==8 && segments[5]=="nodes" && segments[7]=="app_params"`
  → break (expected-skip, без unknownKeys; значение не попадает в модель).
- **Команда** `src/AdminPanel.Api/Operations/RotateAppPasswordCommand.cs`
  (по образцу `RecreateNodeCommand`): (1) имя кластера каноническое
  (`^[a-z][a-z0-9_]{0,62}$`), иначе 404; (2) активный endpoint из снапшота,
  иначе 503; (3) config напрямую у etcd: нет → 404, `state` не null → 409
  `ClusterNotActiveException`; (4) GET `/pgworker/rotations/<C>` напрямую:
  есть → 409 `RotationAlreadyRequestedException`; (5) txn
  `[compare version==0] [put {"requested_unix":now,"requested_by":user}]`;
  проигрыш → 409 `RotationAlreadyRequestedException`; etcd-сбой → 503.
  `requested_by` — ClaimsPrincipal сессии (аудит, как §9.7).
- **Эндпоинт** `OperationsModule`:
  `POST /api/clusters/{cluster}/app-password/rotate` → 201
  `AppPasswordRotatedDto(cluster, requestedUnix, requestedBy)` | 404 | 409 |
  503 (ProblemDetails; маппинг по образцу moves-эндпоинта). Тела нет.

### 4.6. AdminPanel — фронтенд

- `frontend/src/api/dto.ts`: `AppPasswordRotatedDto`.
- `frontend/src/api/queries.ts`: `rotateAppPassword(cluster)` — POST без тела
  (по образцу `deleteCluster`/`recreateNode`).
- `frontend/src/pages/cluster-details/RotateAppPasswordButton.tsx` (по образцу
  `DeleteClusterButton`): кнопка в шапке `ClusterDetailsPage` рядом с
  «Удалить кластер»; видимость — только `data.state === 'ACTIVE'` (у
  TO_REMOVE обе кнопки скрыты, у NOT_INITIALIZED ротация не имеет смысла —
  пароль ещё не создан/не используется). Модальное подтверждение с
  предупреждением: «После применения (секунды) подключения со старым паролем
  начнут отвергаться, пока приложение не перечитает app_password из etcd —
  выполняйте в тихое окно». 409 «уже запрошена» — текстом из ProblemDetails.
  После 201: инвалидация `['clusters']` не обязательна (визуально ничего не
  меняется) — показ success-нотификации «заявка отправлена; выполняет
  PgWorker».

### 4.7. Что НЕ меняется

- `dsn`-ключ шарда (формат, креды bucket_admin), app_user (значение "app"),
  генератор паролей, `DoormanConfigBuilder` (`tls_mode=require` — серверная
  сторона, не клиентская), HAProxy-конфиг, move-процессы.
- Панель по-прежнему не отображает app_user/app_password/app_params;
  статус ротации в UI не отслеживается (исчезновение заявки — прогресс
  вне панели; отображение `/pgworker/work/<C>` — roadmap, вне scope).

## 5. Фазы реализации

Фаза 0 (сделано вместе со spec): контракты arch/ — arch/11 §2/§3,
arch/14 §3/§5/§8, adminpanel/02 §2.1/§9.8/шапка, adminpanel/03 §1/§1.6/§2/§3.

Фаза 1 — PgWorker app_params (TDD: unit → код):
1. `NodeSpec.AppParams` + парсер (фикстуры: ключ есть/пустой/отсутствует).
2. `PgWorkerOptions.AppParams.Default` + appsettings.
3. `AppParamsEnsurer` (unit: put-if-absent, проигрыш compare, failover).
4. Интеграция: P2.5' ProvisioningProcess, A5 AddShardProcess (+ свежий
   re-read кредов), NodeSupervisor-миграция.

Фаза 2 — PgWorker ротация:
1. `AppPasswordRotator` (unit: fake etcd/sql — успех, частичный отказ,
   ретрай с регенерацией, txn-коммит, проигрыш compare, битая заявка).
2. `IClusterProcesses.RotateAppPasswordAsync` + ReconcileLoop-вызов + DI.
3. `DeprovisioningProcess` D2: del заявки.

Фаза 3 — AdminPanel backend:
1. Парсер expected-skip (unit).
2. `RotateAppPasswordCommand` + эндпоинт (integration: 201/404/409×2/503,
   txn-клэйм, идемпотентность повтора после «заявки нет»).

Фаза 4 — фронтенд: dto + query + кнопка/модалка (сборка `npm run build`,
ручная проверка на dev-стенде).

Фаза 5 — E2E (см. §7) + smoke dev-stand (`dev-stand/adminpanel/checks/` —
опционально дополнить чек кнопки).

## 6. Ограничения и риски

| # | Риск/ограничение | Митигация/решение |
|---|---|---|
| О1 | Окно расхождения ротации: часть шардов уже с NEW, etcd со OLD → приложение падает на переехавших шардах до завершения | Только при transient-отказе посередине (шард недоступен); заявка жива, ретраи тиками с закрытием окна; journal.last_error — оператору |
| О2 | Живые пулы клиентов реконнектятся со старым паролем после R3 | Плановая операция: предупреждение в UI-модалке; клиенты обязаны перечитывать app_password (контракт уже требует читать ключ) |
| О3 | Гонка add-shard SQL-фаза ↔ ротация (роль нового шарда со старым паролем) | Свежий re-read кредов в A5 перед ALTER (окно — миллисекунды); residual — повторная ротация |
| О4 | Ручные значения app_params не валидируются (оператор etcdctl) | Осознанно: PgWorker не второй писатель; ошибка значения → ошибка подключения у клиента, диагностируется откатом ключа |
| О5 | Клиент не знает, чей app_params брать (master-ключ = host:port без имени ноды) | Канон arch/11 §3: hosts/ports dsn идут по возрастанию имён нод, имена — leaf'и nodes/; индекс master host:port в dsn-списках → имя ноды. В штатном режиме все ноды шарда несут одинаковое значение |
| О6 | Смена app_params в дальнейшем не подхватывается PgWorker | Осознанно (put-if-absent, без reconciliation): изменение = etcdctl; откат к дефолту — удаление ключа (надзор восстановит) |
| О7 | doorman passthrough кэширует SCRAM-верификацию | Doorman проверяет креды клиента на каждом новом соединении против pg_authid — после ALTER новые коннекты требуют новый пароль; клиентские пулы см. О2 |

## 7. Критерии приёмки

1. **Контракты**: правки arch/ (11/14/adminpanel-02/adminpanel-03) слиты
   вместе с кодом; §-ссылки кода (комментарии) указывают на новые секции.
2. **app_params после provisioning (e2e, по образцу
   `E2eAppSecretScenarios`)**: после догона provisioning до dsn у КАЖДОЙ
   ноды каждого шарда есть `/clusters/<C>/shards/<X>/nodes/<n>/app_params`
   со значением из конфига (дефолт `sslmode=require`); ключи стабильны
   между тиками; значение НЕ содержит user/password.
3. **Миграция (unit/e2e)**: кластер, сиенный без app_params (нод-ключи
   только state), после первого тика надзора получает app_params у RUNNING-нод;
   повторные тики не меняют значение; вручную записанное значение
   (`sslmode=verify-full`) put-if-absent НЕ перезаписывает.
4. **Парсер PgWorker**: `NodeSpec.AppParams` заполняется/чистится по
   фиксстурам (есть/пусто/нет); панели — expected-skip без unknownKeys
   (unit-тест ClustersParser).
5. **Ротация (e2e)**: на живом e2e-кластере заявка
   `etcdctl put /pgworker/rotations/<C> '{"requested_unix":…,"requested_by":"e2e"}'`
   → в пределах нескольких тиков: (а) заявка исчезла; (б) app_password
   изменился (32 симв, отличается от старого); (в) подключение user=app с
   НОВЫМ паролем выполняет SELECT 1; (г) со СТАРЫМ паролем — auth fail.
6. **Частичный отказ (unit, fake ISqlExecutor)**: один шард недоступен →
   app_password в etcd НЕ изменён, заявка жива, journal несёт last_error;
   «оживление» шарда → ретрай доводит до коммита (п.5).
7. **Атомарность коммита (unit, fake IEtcdGateway)**: txn содержит compare
   по старому значению И оба op (put+del) — двойной ротации из-за сбоя
   между put и del нет; проигрыш compare → ретрай.
8. **Панель API (integration)**: POST `/api/clusters/{c}/app-password/rotate`
   → 201 с телом; повтор до исполнения → 409 «уже запрошена»; не-Active →
   409; нет кластера → 404; etcd недоступен → 503; панель НЕ пишет
   `/clusters/<C>/app_password` ни в каком сценарии.
9. **UI**: на странице Active-кластера есть кнопка «Сменить app-пароль» с
   модальным предупреждением; у TO_REMOVE/NOT_INITIALIZ — кнопки нет;
   SPA-сборка проходит (`npm run build` без ошибок).
10. **Тесты**: новые тесты — с AAA-комментариями; весь существующий набор
    (unit/integration/e2e) зелёный; `TreatWarningsAsErrors` не нарушен.
