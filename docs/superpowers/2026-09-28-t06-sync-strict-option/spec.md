# t06-sync-strict-option — per-cluster опция `synchronous_mode_strict` для Patroni-кластеров

- **Дата**: 2026-09-28
- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), тег `t06-sync-strict-option` (трек reliability, P1, влияние D — durability). Снимается из roadmap И обновляет строку в [`arch/roadmap/reliability-report.md`](../../../arch/roadmap/reliability-report.md) (строка 93 реестра «Осталось») **тем же мерж-коммитом** — мерж-гейт трека.
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t06-sync-strict-option` (ветка `feat-t06-sync-strict-option` от `main@0271805`); правки — только здесь.
- **Тип**: контрактное расширение домена — per-cluster настройка HA-режима (etcd-контракт + API воркера + панель + генерация конфигурации Patroni + конвергенция DCS) + тесты (юнит/интеграция/E2E).
- **Канон (обновляется этой задачей, arch-first, до кода)**: оркестратор — [`arch/14-pgworker.md`](../../../arch/14-pgworker.md) §1.1 (эндпоинт), §2.1 (канон Patroni-конфигурации: strict перестаёт быть константой), §3 (контракт etcd), §5 A/C/G (provisioning/надзор/add-shard); панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §2.1 (config-JSON), §3 (модель снапшота), §9.1/§9.3 (набор ключей создания + валидация), новый §9.10 (мутация config); UI/API — [`arch/adminpanel/03-panels.md`](../../../arch/adminpanel/03-panels.md) §1/§1.1/§2 (эндпоинт + DTO), §3.1/§3.2 (формы), §4 (каталог алертов — severity `sync-standby-missing`). Полный перечень правок arch/ — §5.1.
- **Решения пользователя (гейты уточнений, зафиксированы 2026-09-28)**:
  1. **Дефолт для новых кластеров — `true`** (durability-first): запрос создания без опции создаёт strict-кластер.
  2. **Опция мутабельная**: меняется у живого кластера через API (канал применения — конвергенция DCS, без рестартов).
  3. **Отсутствие поля в config = `true`, буквально для всех существующих кластеров**, включая однорепликные (после апгрейда воркера такой кластер блокирует запись; выключается через API/панель — осознанный выбор, единая семантика дефолта).
  4. **Алерт `sync-standby-missing` эскалируется до critical для strict-кластеров** (запись блокирована = инцидент), для не-strict остаётся warning.
  5. **Полный UI панели**: галочка в форме создания (дефолт true), отображение + переключатель на странице кластера.
  6. **Валидация противоречия**: включение `strict=true` требует `replicas ≥ 2` на всех шардах кластера (создание / add-shard / включение) — иначе 400; выключение разрешено всегда.

## 1. Цель

Сделать выбор «долговечность vs доступность» при потере синхронных реплик
настройкой кластера, а не глобальной константой. Сегодня
`synchronous_mode_strict: false` захардкожен в `SpiloEnvBuilder`
(`src/PgWorker.Core/Templates/NodeConfigBuilders.cs`, SPILO_CONFIGURATION,
bootstrap.dcs): при потере всех реплик мастер продолжает принимать запись
асинхронно, последующий failover теряет «хвост» транзакций; панель видит лишь
warning `sync-standby-missing`. С задачей: оператор при создании (или позже,
на живом кластере) выбирает режим; strict-кластер при отсутствии sync-standby
**блокирует запись** (Patroni держит `synchronous_standby_names` — ни один
коммит не подтверждён репликой), гарантируя отсутствие потерь ценой
доступности на время деградации.

Функциональные результаты:

1. **Поле `synchronous_mode_strict` (bool) в `/clusters/<C>/config`** —
   per-cluster факт; отсутствие ключа-поля трактуется как `true` (решение
   пользователя №3).
2. **Создание кластера**: опциональное поле запроса (отсутствие = `true`);
   strict=true требует `replicas ≥ 2` (400 при противоречии).
3. **Мутация живого кластера**: `PUT /api/clusters/{c}/config` (воркер) +
   зеркальный панельный эндпоинт + переключатель в UI; RMW-txn по
   `mod_revision` (образец — KafkaWorker `PUT /api/kafka/clusters/{c}/config`,
   adminpanel/02 §10.2); гвард — кластер Active.
4. **Применение к Patroni**: (а) bootstrap — `SpiloEnvBuilder` подставляет
   значение из config кластера в `bootstrap.dcs` SPILO_CONFIGURATION;
   (б) конвергенция DCS (`NodeSupervisor.ConvergeDcsConfigAsync` +
   `DcsConfigConvergence`) — `synchronous_mode_strict` входит в сверку,
   ожидаемое значение из config кластера; динамический параметр — применяется
   PATCH /config без рестартов.
5. **Панель**: снапшот читает поле; алерт `sync-standby-missing` — critical
   для strict, warning для остальных; UI — галочка создания (дефолт true),
   отображение + переключатель на Cluster details.
6. **Add-shard**: новый шард в strict-кластере — `replicas ≥ 2` (400).
7. **Тесты**: юниты (конвергенция/парсер/валидатор/план), интеграции
   (API-эндпоинт), docker-E2E кейс (strict применяется в DCS, переключение
   конвергенцией, валидация 400) — по канонам E2E-изоляции.

## 2. Принципы

1. **Arch-first**: контракт в `arch/14` + `arch/adminpanel/02`/`03`
   обновляется ДО кода; код зеркалит канон. Спека — развёртка канона в
   решения кода.
2. **Единый источник опции — `/clusters/<C>/config`**: и bootstrap при
   (пере)создании нод, и конвергенция DCS, и панель читают одно и то же
   поле. Нет второго места правды (никаких env-переопределений опции).
3. **Конвергенция как канал применения к живым кластерам**: изменение
   конфигурации — это запись в etcd; воркер существующим тиком надзора
   приводит DCS-конфиг каждого шарда к канону (механика t09/t11 —
   «старое не живёт параллельно канону»). Воркер НИКОГДА не инициирует
   рестарт PG (решение 2026-09-14): strict — динамический параметр Patroni,
   рестартов не требует.
4. **Идемпотентность и транзиент-толерантность**: PATCH /config при
   расхождении повторяется тиками (недоступность REST — skip тика, как
   сейчас); мутация config — txn по `mod_revision` (проигрыш compare →
   503, retry клиентом).
5. **Durability-first по умолчанию, но без ловушек при создании**:
   дефолт `true` + жёсткая валидация `replicas ≥ 2` — противоречие
   (strict + однорепликный шард = вечная блокировка записи) не создаётся
   в принципе. Для СУЩЕСТВУЮЩИХ однорепликных кластеров принято
   «буквально для всех» (решение №3): апгрейд делает их strict — запись
   блокируется до явного выключения; это осознанный риск, подстрахованный
   эскалированным алертом (№4) с Hint/Remedy «выключите strict».
6. **Границы домена не расширяются**: KafkaWorker/ValkeyWorker не
   затрагиваются; усыновлённые шарды (записи portalloc с `object`) —
   конвергенция DCS к ним и не применяется (канон arch/14 §5 C), опция на
   них не влияет; Patroni REST :8008 — существующая грань, новый эндпоинт
   не вводится.
7. **Язык/идентификаторы**: документация по-русски, идентификаторы
   английские; config-JSON — snake_case (`synchronous_mode_strict` — имя
   1:1 как у Patroni, без сокращений), REST/DTO — camelCase
   (`synchronousModeStrict`) по существующей конвенции DTO.

## 3. Структура и компоненты

### 3.1. Контракт etcd: config-JSON кластера

`/clusters/<C>/config` — JSON расширяется полем:

```json
{
  "buckets": 10,
  "dbname": "demo",
  "created_unix": 1234567890,
  "state": "ACTIVE",
  "synchronous_mode_strict": true
}
```

- Тип — bool; отсутствие поля, битое значение не-bool — трактуется как
  `true` (семантика решения №3: «нет поля = strict»; не-bool не молчит в
  false). Пишется: созданием кластера, мутацией config; никогда — воркером
  по собственной инициативе (воркер только читает).
- Парсер PgWorker (`ClusterSnapshotParser.ParseConfig` → `ClusterConfig`):
  новое свойство `SyncStrict` (bool, дефолт `true` при отсутствии/битом).
- Парсер панели (`EtcdSnapshot`/`ClusterInfo`): то же поле, та же семантика
  (adminpanel/02 §2.1 строка 64 + §3 модель).
- Специфичное `state`-поле не трогается; прочие поля config при RMW
  переносятся (как в §9.4 для TO_REMOVE: «сохранением остальных полей»).

### 3.2. Создание кластера

- `CreateClusterRequest` (PgWorker.Core.Writing): новое опциональное поле
  `bool? SyncStrict = null` (отсутствие = true). REST/DTO тела панели —
  поле `synchronousModeStrict` (bool?, отсутствие = true).
- `CreateClusterValidator`: при `SyncStrict != false` (т.е. true или null)
  и `Replicas < 2` — ошибка по полю `syncStrict`: «strict-режим требует
  replicas ≥ 2 на каждом шарде (без sync-standby запись блокируется)».
  Границы существующих полей не меняются.
- `ClusterCreatePlan.Build`: `ConfigJson` получает
  `synchronous_mode_strict` (значение после Normalize: null → true).
- Панель (форма §3.5) и сид-эндпоинт `POST /api/seed/demo` — пишут поле
  явно (`true` у сида; форма — по галочке).

### 3.3. Мутация config живого кластера

- **API воркера**: `PUT /api/clusters/{c}/config` — тело
  `{"synchronousModeStrict": bool}` (меняется ровно это поле). Коды: 204 (успех), 400 (валидация:
  включение strict при наличии шарда `replicas < 2`; битое тело),
  404 (кластер не найден), 409 (кластер не Active — NOT_INITIALIZED
  «дождитесь инициализации» / TO_REMOVE «кластер удаляется»), 503
  (etcd недоступен / проигрыш mod_revision — retry клиентом).
- **Протокол записи** (образец KafkaWorker §10.2, админпанельный §9.10):
  (1) чтение config напрямую у etcd (не из снапшота) — нет → 404, state
  не Active → 409; (2) чтение `shards/*/replicas` — валидация включения;
  (3) txn `compare mod_revision(config) == прочитанной` + put config
  (поле обновлено, прочие поля перенесены без изменений); проигрыш
  compare → 503. Идемпотентность: повтор с тем же значением при том же
  revision — успех; значение уже совпадает → 204 без записи.
- **Панель**: зеркальный `PUT /api/clusters/{cluster}/config`
  (AdminPanel.Api → дискавери живого инстанса PgWorker API → эндпоинт
  воркера; механика прокси существующая, как у прочих мутаций).
- **Применение**: записи в etcd НЕ достаточно — её подбирает тик конвергенции
  (§3.4); панель после 204 показывает актуальное значение опции и статус
  DCS-применения не отражает (как у Kafka converge: «применит воркер»,
  наблюдаемость — через HA-страницу `/service/<scope>/config` raw-JSON и
  журнал воркера `dcs-converge`).

### 3.4. Применение к Patroni (воркер)

- **Bootstrap** (`SpiloEnvBuilder.Build`): сигнатура расширяется параметром
  `bool syncStrict` — без дефолта: каждый вызов обязан передать значение
  явно (тестовые пути драйвера передают `false` — инвариант
  байт-в-байт SPILO_CONFIGURATION старых тестов сохраняется только у путей
  с false). Значение — из `ClusterConfig.SyncStrict` кластера; попадает в
  `bootstrap.dcs` рядом с `synchronous_mode: true`. Вызовы в
  ProvisioningProcess/NodeSupervisor (EnsureNode-пути: provision, rebuild,
  пересоздание) передают значение из снапшота кластера.
- **Конвергенция DCS** (`DcsConfigConvergence.Analyze` + вызов в
  `NodeSupervisor.ConvergeDcsConfigAsync`): в сверку таймингов добавляется
  `synchronous_mode_strict` — ожидаемое значение передаётся в Analyze
  (расширение сигнатуры: per-cluster значение, не константа
  `PatroniTimings`); расхождение попадает в тот же единственный PATCH
  /config тика. Порядок ключей патча: тайминги → strict → параметры —
  детерминированный документ.
- Усыновлённые шарды (`object`-записи) — без изменений: probeNode с
  `Object is null` — конвергенция к внешним контурам не применяется.
- Журнал: патч с изменением strict — существующая фазовая запись
  `dcs-converge` (в note виден diff-патч); отдельного шума не вводится.

### 3.5. Панель

- **Снапшот/DTO**: `ClusterInfo` + поле `SynchronousModeStrict` (bool);
  в списке кластеров и Cluster details.
- **Алерт** `sync-standby-missing` (`SyncStandbyMissingRule`): severity
  вычисляется от strict кластера — strict → **critical** (запись
  блокирована: «кластер недоступен на запись»), не-strict → warning (как
  сейчас). Текст/`standbiesTotal` без изменений; Hint/Remedy: для strict
  добавляется ремеди «запись блокирована — восстановите реплику (rebuild
  воркера) или выключите strict (PUT config)». Каталог в adminpanel/03 §4 —
  строка правила меняет severity на «strict ? critical : warning».
- **UI — форма «Создать кластер»** (03-panels §3.1): чекбокс
  «Синхронный strict-режим (блокировать запись при потере sync-реплики)»,
  дефолт включён; клиентская валидация: strict + replicas=1 → ошибка у
  поля реплик (зеркало серверной); при выключенном strict ограничений нет.
- **UI — Cluster details**: отображение текущего значения (бейдж
  «strict»/«availability») + переключатель (мутация только для Active;
  не-Active — контрол заблокирован с подсказкой). Подтверждение
  переключения — модальный диалог с предупреждением о последствиях
  (включение: запись блокируется при потере sync-standby; выключение:
  возможна потеря «хвоста» при failover).
- **Add-shard форма** (§3.2): при strict-кластере поле «реплики» —
  минимум 2 (клиентская валидация; серверная — §3.6).

### 3.6. Add-shard

`AddShardHandler` (воркер) + панельная команда: при strict-кластере
(`config.synchronous_mode_strict` = true) запрос с `replicas < 2` → 400
(«strict-кластер: replicas ≥ 2 на каждом шарде»). Выключенного strict —
без изменений (минимум 1).

### 3.7. Файлы кода (ориентир для плана, не исчерпывающий список)

| Компонент | Файл |
|---|---|
| Bootstrap-конфиг | `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (SpiloEnvBuilder) |
| Конвергенция | `src/PgWorker.Core/Templates/DcsConfigConvergence.cs` |
| Модель | `src/PgWorker.Core/Model/Domain.cs` (ClusterConfig) |
| Парсер | `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` |
| Запрос/валидатор/план | `src/PgWorker.Core/Writing/CreateClusterRequest.cs`, `ClusterCreatePlan.cs` |
| API воркера | `src/PgWorker.App/Api/Operations/` (новый `UpdateClusterConfigHandler`, правка `CreateClusterHandler`/`AddShardHandler`, `ApiModule`) |
| Надзор | `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (ConvergeDcsConfigAsync + EnsureNode-вызовы SpiloEnvBuilder) |
| Панель: снапшот | `src/AdminPanel.Etcd/` (парсер снапшота), `src/AdminPanel.Core/` (ClusterInfo) |
| Панель: алерт | `src/AdminPanel.Core/Alerting/Rules/SyncStandbyMissingRule.cs` |
| Панель: API | `src/AdminPanel.Api/Operations/` (новая команда PUT config) |
| Панель: UI | фронтенд (форма создания, Cluster details) |
| Сид | `PgWorker.App` seed/demo (поле в config) |

## 4. Фазы (порядок исполнения)

1. **Фаза 0 — arch/ (первая, до кода)**: правки канона по перечню §5.1.
   Каждая правка — раздел соответствующего документа, формулировки
   контракта (не реализации).
2. **Фаза 1 — PgWorker**: модель + парсер (поле config) → запрос/валидатор/
   план создания → bootstrap (`SpiloEnvBuilder`) → конвергенция
   (`DcsConfigConvergence` + `NodeSupervisor`) → API: `PUT /api/clusters/{c}/config`
   + гварды/валидация add-shard. Юнит-тесты на каждый шаг (TDD).
3. **Фаза 2 — панель**: парсер снапшота/DTO → алерт (severity от strict) →
   панельный эндпоинт-прокси → UI (форма + Cluster details). Юниты алерта/
   парсера, интеграция эндпоинта.
4. **Фаза 3 — интеграция/E2E**: интеграционный тест API (реальный etcd:
   RMW-txn, гварды, валидация); docker-E2E кейс §6 п.7–8. Зачистка контуров
   по канонам E2E-изоляции (`docs/e2e-isolation.md`, `docs/e2e-launch.md`).
5. **Фаза 4 — мерж-гейт трека**: снятие тега `t06-sync-strict-option` из
   `arch/roadmap/reliability.md` + обновление строки 93 реестра в
   `reliability-report.md` тем же мерж-коммитом.

## 5. Ограничения

1. **Код — только в фазе execute** после одобренного plan.md; на фазе спеки
   правится только arch/ + документация (base-rules).
2. **Воркер не инициирует рестарт PG** — строгий режим применяется только
   динамически (PATCH /config); `pending_restart` strict не порождает.
3. **Только канонические шарды**: конвергенция strict не затрагивает
   усыновлённые (`object`) шарды и внешние Patroni-контуры.
4. **Поле только bool**; никаких «трёхзначных» состояний в etcd (отсутствие
   = true — семантика по умолчанию, не «неизвестно»). Воркер поле не
   пишет — только создание/мутация через API.
5. **Мутация — только Active-кластерам** (409 иначе; Kafka-прецедент).
   Меняется ровно одно поле; endpoint не «конфиг-швейцарский нож» —
   будущие поля добавляются расширением тела, не обобщением.
6. **TreatWarningsAsErrors=true**, `LangVersion=latest`, `Nullable=enable`;
   централизованные версии пакетов (`Directory.Packages.props`) — новых
   пакетов задача не вводит.
7. **E2E-каноны**: изоляция guid-именами, динамические порты
   (`assignRandomHostPort: true`), полный teardown при любом исходе, ассерт
   чистоты, `BrokerBootSec ≤ 100 с`, без хардкода портов; телеметрия по
   `docs/e2e-launch.md` (снятие логов в teardown, MarkFailed — контейнеры
   стоят, не удаляются).
8. **Стиль**: комментарии/документация по-русски, идентификаторы
   английские; тесты — AAA-комментарии.

### 5.1. Правки arch/ (Фаза 0, перечень)

| Документ | Раздел | Суть правки |
|---|---|---|
| `arch/14-pgworker.md` | §1.1 (таблица эндпоинтов) | + `PUT /api/clusters/{c}/config` (мутация synchronous_mode_strict; протокол — adminpanel/02 §9.10) |
| `arch/14-pgworker.md` | §2.1 (канон таймингов Patroni) | strict больше не константа false: per-cluster опция config; дефолт true; bootstrap из config, применение конвергенцией |
| `arch/14-pgworker.md` | §3 (контракт etcd) | config-JSON: поле `synchronous_mode_strict` (отсутствие = true; пишет API создания/мутации, воркер читает) |
| `arch/14-pgworker.md` | §5 A (ProvisioningProcess) | SpiloEnvBuilder: strict из config кластера (bootstrap.dcs) |
| `arch/14-pgworker.md` | §5 C (NodeSupervisor, конвергенция) | сверка + `synchronous_mode_strict` из config кластера (per-cluster ожидание) |
| `arch/14-pgworker.md` | §5 G (AddShardProcess) | strict-кластер: replicas ≥ 2 (400) |
| `arch/adminpanel/02-etcd-contract.md` | §2.1 (строка config) | поле в JSON-схеме config + семантика отсутствия = true |
| `arch/adminpanel/02-etcd-contract.md` | §3 (модель снапшота) | `ClusterInfo.SynchronousModeStrict` |
| `arch/adminpanel/02-etcd-contract.md` | §9.1/§9.3 | поле в наборе ключей создания + валидация strict→replicas≥2 |
| `arch/adminpanel/02-etcd-contract.md` | §9.10 (новый) | протокол мутации config: RMW-txn по mod_revision, гварды, коды, образец §10.2 |
| `arch/adminpanel/03-panels.md` | §1 (таблица), §1.1 (тело создания) | + `PUT /api/clusters/{cluster}/config`; тело создания + `synchronousModeStrict` |
| `arch/adminpanel/03-panels.md` | §2 (DTO) | `ClusterInfoDto`/`CreateClusterRequestDto` + поле |
| `arch/adminpanel/03-panels.md` | §3.1/§3.2 (формы) | чекбокс strict в создании; минимум replicas=2 в add-shard при strict |
| `arch/adminpanel/03-panels.md` | §3 (Cluster details) | отображение + переключатель strict (Active only) |
| `arch/adminpanel/03-panels.md` | §4 (каталог алертов) | `sync-standby-missing`: severity «strict ? critical : warning» + Hint/Remedy |

Формулировки правок выполняются в фазе 0 этой же задачей (arch-first);
вышеприведённая таблица — обязательный объём, план фазы execute ссылается
на обновлённые разделы.

## 6. Критерии приёмки

1. **Контракт**: `/clusters/<C>/config` нового кластера содержит
   `synchronous_mode_strict`; парсер PgWorker и панели читают поле;
   отсутствие поля в существующем config даёт `SyncStrict == true` (юнит:
   парсер — поле есть/отсутствует/битое не-bool).
2. **Создание**: `POST /api/clusters` без поля → strict-кластер;
   `{"synchronousModeStrict": true, "replicas": 1}` → 400 с ошибкой по
   полю; `{"synchronousModeStrict": false, "replicas": 1}` → 201.
3. **Bootstrap**: SPILO_CONFIGURATION свежеподнятой strict-ноды содержит
   `synchronous_mode_strict: true` (юнит SpiloEnvBuilder: true/false/
   сигнатура всех вызовов).
4. **Конвергенция**: `DcsConfigConvergence.Analyze` включает strict в
   патч при расхождении (юнит: конфиг без поля/с чужим значением/битый →
   патч; конвергентно → null); `NodeSupervisor` передаёт per-cluster
   значение (юнит с фикстурой).
5. **Мутация**: `PUT /api/clusters/{c}/config` — 204 и поле в etcd;
   включение strict при `replicas=1` → 400; не-Active → 409; нет кластера
   → 404; mod_revision-гонка → 503 (интеграционный тест с реальным etcd).
6. **Add-shard**: strict-кластер + `replicas=1` → 400 (юнит + интеграция).
7. **E2E (docker, свежий Release)**: кластер strict (replicas=2) после
   подъёма — GET /config шарда содержит `synchronous_mode_strict: true`;
   `PUT .../config` → `false` → конвергенция приводит DCS-конфиг в
   пределах нескольких тиков надзора (без рестартов нод); попытка
   включения strict кластеру с однорепликным шардом → 400. Полный
   teardown + ассерт чистоты; телеметрия по docs/e2e-launch.md.
8. **Мерж-гейт E2E**: задачи, трогающие код воркеров, — кейс-маркер
   `Scale_AddEmptyShard` зелёный на свежем Release (общее правило AGENTS.md);
   новый кейс strict входит в серию PgWorker E2E.
9. **Панель**: алерт `sync-standby-missing` — critical у strict-кластера
   без sync-standby, warning у не-strict (юниты правила с обеими
   конфигурациями); UI-формы по §3.5 (ручная проверка на dev-стенде в
   docker — панель всегда в докере).
10. **Roadmap-гейт**: тег снят из `arch/roadmap/reliability.md`, строка 93
    реестра `reliability-report.md` обновлена — тем же мерж-коммитом.

## 7. Риски и осознанные решения

- **Апгрейд с существующими однорепликными кластерами**: после обновления
  воркера они становятся strict (отсутствие поля = true) и блокируют
  запись. Решение пользователя («буквально для всех») зафиксировано;
  смягчение — critical-алерт с ремеди «выключите strict», выключение не
  ограничено валидацией replicas≥2.
- **Strict + transient-деградация**: rebuild единственной реплики
  2-репликного strict-шарда блокирует запись до возврата реплики — это
  штатная семантика strict (durability), не авария; ускорение failover
  и rebuild-механика не меняются.
- **Расширение сигнатуры SpiloEnvBuilder** ломает компиляцию всех
  вызовов — намеренно (явность значения в каждом пути); тесты
  изолированных путей драйвера передают `false` и сохраняют прежний
  YAML-инвариант.
- **Патч конвергенции меняет формат** (появляется ключ strict): журналы
  `dcs-converge` и юниты DcsConfigConvergence обновляются вместе; патчи
  остаются минимальными (только расходящиеся ключи).
