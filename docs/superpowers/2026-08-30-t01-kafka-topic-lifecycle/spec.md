# Спецификация: t01 — lifecycle топиков Kafka (создание/удаление из панели)

> Dev-flow, фаза спецификации. Worktree: `feat-t01-kafka-topic-lifecycle`, от `main`
> `4fb7f8f`. Дата: 2026-08-30. Задача — `arch/roadmap/kafkaworker.md` `t01-kafka-topic-lifecycle`
> (out of scope спеки `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8).
>
> Снимает действующее ограничение канона: «desired применим только к существующим топикам;
> missing-топик — заявка не исполнима, создание — только CLI/клиентами» (arch/15 §3).
> Теперь создание и удаление топиков — **заявки панели** (`topics/<T>/desired.create` /
> `topics/<T>/desired.delete`), исполняет KafkaWorker через AdminClient. Удаление кластера,
> брокеров, ротация — вне изменений (уже реализованы).

## 1. Цель

1. **Панель** получает две новые мутации kafka-домена: **создание топика** (форма с
   параметрами) и **удаление топика** (деструктивная, с гвардами и подтверждением), плюс
   отмена обеих заявок до исполнения. Состав топиков по-прежнему синхронизируется
   автосинком (CLI/клиенты продолжают работать параллельно — реестр сходится к факту).
2. **KafkaWorker** (процесс D `TopicSyncProcess`) исполняет lifecycle-заявки: create →
   `CreateTopicsAsync` (с начальными конфигами), delete → `DeleteTopicsAsync`; все
   операции идемпотентны, safe-to-retry, под клэймом `<C>`.
3. **Контракт etcd — источник истины**: расширение канона `arch/15-kafka-clusters.md`
   (§2 таблица + §2.1 примеры + §3 протокол + §5 дискавери + §6 толерантность) и
   отражение в `arch/16-kafkaworker.md` (§1, §3.2, §5 D, границы) и
   `arch/adminpanel/02-etcd-contract.md` (§10.1–10.4), `arch/adminpanel/03-panels.md`
   (§7: эндпоинты/DTO/UI/алерты). **Arch-first: правка `arch/` — первая фаза плана;
   код — строго по обновлённому канону.** На фазе spec arch-файлы не правятся —
   фиксируются изменения ниже.

Ключевой инвариант сохраняется: **факт-ключ `topics/<T>` целиком принадлежит воркеру**
(панель по-прежнему не создаёт и не удаляет факт-ключи); панель пишет только заявки —
`desired`-часть факт-ключа (RMW, как сейчас) и новые lifecycle-leaf-ключи.

## 2. Принципы

1. **Паттерн `topics/<T>/desired.create`** (дословно из roadmap): lifecycle-заявка —
   отдельный leaf-ключ рядом с факт-ключом, живёт от постановки до исполнения;
   присутствие ключа = живая заявка. Постановка — клэйм-txn `version==0` (порт
   ротации, arch/15 §4): повторная постановка → 409. Исполнение/чистка — del ключа
   заявки (воркером); отмена — del (панелью).
2. **Деструктивность удаления — три гварда**: (а) серверные пред-проверки панели
   (только Active-кластер; топик существует и не missing; нет живых create/конфиг-
   заявок — 404/409), (б) UI-подтверждение с явным текстом «данные будут удалены
   безвозвратно» и вводом имени топика, (в) окно отмены: заявка висит до тика воркера
   (≤ `TopicSyncIntervalSec` 15 с) — `DELETE .../desired.delete` снимает её.
3. **Идемпотентность и сходимость**: describe→decide→act перепроверяет факт; create при
   уже существующем топике и delete при уже отсутствующем — «исполнено», а не ошибка;
   транзиенты — jitter-ретраи, перманентное — журнал. Отказ между Kafka-мутацией и
   del заявки безопасен: следующий тик разрулит по факту.
4. **Реестр = факт остаётся**: lifecycle не меняет протокол автосинка; факт-ключ после
   create кладёт автосинк следующего тика (как для CLI-созданных), после delete —
   исчезает вместе с топиком (del воркером в той же txn, что и del заявки; desired
   конфиг-заявки удаляемого топика гасится вместе с ключом).
5. **Копирование из референсов**: `CreateTopicAsync`/`EnsureTopic` — порт
   `../Puzzle docs/01.16-kafka.md` §7 (`IKafkaTopicAdmin`: create с RF и конфигами,
   идемпотентность, jitter-ретраи поверх оркестрации). Существующий seam
   `IKafkaAdminClient` расширяется двумя методами; Confluent-типы остаются только в
   адаптере `KafkaAdminClient`.
6. **Язык и стиль**: документация/комментарии — русский; идентификаторы — английский;
   .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, CPM без новых пакетов;
   тесты — AAA-комментарии. KafkaWorker — docker (`deploy/docker-compose.yml`),
   AdminPanel — хост-процесс с React SPA (Mantine), как сейчас.

## 3. Контракт etcd — правки канона `arch/15` (источник истины)

### 3.1. Новые ключи (правка §2 таблицы + §2.1 примеры)

В таблицу §2 после строки `topics/<T>` добавляются две строки:

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `topics/<T>/desired.create` | JSON `{"partitions":P,"replication_factor":R,"configs"?:{...},"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка создания; `configs` — опциональные начальные конфиги (только управляемые: `retention.ms`, `min.insync.replicas`); отсутствие = брокерные дефолты кластера |
| `topics/<T>/desired.delete` | JSON `{"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка удаления (деструктивная) |

Канонические примеры (добавить в §2.1):

```json
// topics/orders/desired.create
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"86400000","min.insync.replicas":"2"},
 "requested_unix":1750000000,"requested_by":"admin"}

// topics/orders/desired.create — без начальных конфигов (брокерные дефолты)
{"partitions":6,"replication_factor":3,
 "requested_unix":1750000050,"requested_by":"admin"}

// topics/orders/desired.delete
{"requested_unix":1750000100,"requested_by":"admin"}
```

Свойства:
- Имя `<T>` — валидное Kafka-имя без `/` (Kafka запрещает), поэтому разбор по сегментам
  однозначен: факт-ключ — 6 сегментов (`.../topics/<T>`), заявка — 7
  (`.../topics/<T>/desired.{create,delete}`); leaf-имена фиксированы.
- `internal`-топики (`__*`): панель заявку не ставит (404 по неканоническому имени);
  заявка на `__`-имя — etcd-мусор: воркер чистит (del + журнал), не исполняя.
- Обе заявки одновременно на один `<T>` — запрещено панелью (409); etcd-мусор —
  воркер чистит create-заявку, delete авторитетен (§3.2).
- Deprovisioning X2 (`del --prefix /kafka/clusters/<C>/`) забирает и заявки —
  поведение без изменений.

### 3.2. Протокол исполнения (новый подраздел §3 канона)

Воркер, тик `TopicSyncIntervalSec` под клэймом `<C>` (расширение процесса D,
describe→decide→act). **Порядок decide: delete-заявки → create-заявки → факт-синк**
(существующий). Один топик — не более одного lifecycle-действия за тик.

| Ситуация | Действие воркера |
|---|---|
| `desired.delete` + топик в факте Kafka | journal → `DeleteTopicsAsync` → одной txn: del `topics/<T>` (факт-ключ; живой `desired` гасится вместе с ним) + del заявки. Транзиент Kafka → ретраи jitter, заявка жива |
| `desired.delete` + топика нет в факте | del заявки (+ del факт-ключа, если висит `missing`-ключ) — «исполнено внешне» (удалён CLI раньше) |
| `desired.create` + топика нет в факте | journal → `CreateTopicsAsync(name, partitions, RF, configs?)` → del заявки. Факт-ключ `topics/<T>` кладёт следующий автосинк-тик (≤ 15 с; как для CLI-созданных). **Не** применять конфиги из заявки отдельно — они задаются при создании |
| `desired.create` + топик уже есть в факте | del заявки как исполненной + journal-note «топик уже существует, параметры заявки не применены» (RF/начальные конфиги к существующему не мутируются — для этого отдельная desired-заявка) |
| обе заявки живы (мусор) | del `desired.create` + journal-warning; исполняется delete (деструктивная заявка доминирует) |
| заявка на `__`-имя / битый JSON заявки | битый JSON — parseError-запись + warning-алерт `kafka-key-malformed` (§6, без изменений); `__` — del + журнал, не исполняя |

Идемпотентность (расширение §6 идемпотентности канона):
- create: `CreateTopicsAsync` → `AlreadyExists` трактуется как исполнено (адаптер
  классифицирует исход, процессы не парсят строки ошибок);
- delete: `DeleteTopicsAsync` → `TopicDoesNotExist` — исполнено;
- отказ между Kafka-мутацией и del заявки: повтор тика видит «топик есть/нет + живая
  заявка» → ветки выше сходятся без побочных эффектов;
- `missing`-семантика не меняется: create-заявка на missing-топик — это «пересоздание»
  (топика нет в факте → create исполнится; появившийся топик снимет `missing` штатно).
  Панель требует сначала отменить живой `desired` missing-ключа (§5.1 п.9, 409); прямой
  etcd-обход (desired жив + create поставлен) воркером не ломается: после create
  `missing=false`, `desired` применится к новому топику штатным автосинком.

### 3.3. Прочие правки arch/15

- **§5 дискавери**: пункт 3 дополняется — читатель реестра `topics/` (префикс) видит и
  leaf-ключи заявок `desired.{create,delete}`; библиотека дискавери (t05) фильтрует по
  числу сегментов (факт-ключи — 6 сегментов). Состав реестра для клиентов не меняется.
- **§6 толерантность**: строка про битый JSON дополняется `desired.create/delete`;
  неизвестный leaf под `topics/<T>/` — `unknownKeys`.

## 4. KafkaWorker — правки канона `arch/16` и кода

### 4.1. Канон arch/16

- **§1 диаграмма/таблица**: панель дополнительно заявляет «создание/удаление топика →
  `topics/<T>/desired.{create,delete}`»; воркер — «исполняет lifecycle-заявки топиков».
- **§3.2 пишемые ключи**: + `topics/<T>/desired.{create,delete}` — del после
  исполнения/чистки (одной txn с del факт-ключа при delete).
- **§5 D (TopicSyncProcess)** — дополняется протоколом §3.2 этой спеки (порядок
  delete→create→sync, guards, идемпотентность). Нового процесса не вводится: заявки
  исполняет тот же тик D под тем же клэймом (меньше гонок, общий journal/ретраи).
- **Границы (вступление «что НЕ входит»)**: пункт «создание/удаление топиков из панели»
  удаляется; §2.1 «создание топиков — явное, CLI/клиентами» → «явное: панелью (lifecycle-
  заявки) или CLI/клиентами; автосоздание продюсером по-прежнему `false`».
- **§6 снапшоты P12**: без изменений — lifecycle не добавляет точек снапшотов (как
  add/remove брокеров); journal-before-manipulations достаточно.
- **Roadmap-гейт**: пункт `t01-kafka-topic-lifecycle` удаляется из
  `arch/roadmap/kafkaworker.md` тем же коммитом мержа (правила README трека).

### 4.2. Код

| Единица | Изменение |
|---|---|
| `IKafkaAdminClient` / `KafkaAdminClient` | + `Task<Result<TopicCreateOutcome>> CreateTopicAsync(string topic, int partitions, short rf, IReadOnlyDictionary<string,string>? configs, CancellationToken)`; + `Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken)`. `TopicCreateOutcome { Created, AlreadyExists }`, `TopicDeleteOutcome { Deleted, NotFound }` — адаптер разбирает отчёты Confluent (`CreateTopicReport`/`DeleteTopicReport`; `ErrorCode.TopicAlreadyExists`/`UnknownTopicOrPartition` → исход, не Failed). Конфиги — `ConfigEntry` при создании (порт Puzzle §7.1) |
| `KafkaDomain` | + `record TopicLifecycleTicket(string Topic, string Op /* "create"\|"delete" */, int Partitions, short? ReplicationFactor, IReadOnlyDictionary<string,string>? Configs, long RequestedUnix, string? RequestedBy)`; `KafkaClusterSnapshot` + `IReadOnlyList<TopicLifecycleTicket> LifecycleTickets` |
| `KafkaSnapshotParser` | ветка `topics` → 7 сегментов с leaf `desired.create`/`desired.delete` → тикеты (битый JSON → parseErrors; прочие leaf → unknownKeys); 6 сегментов — как сейчас |
| `TopicSyncDecision` | + decide-ветки lifecycle (чистые функции, таблица §3.2): `LifecycleDelete`, `LifecycleCreate`, `LifecycleCleanup` (del заявки: исполнена внешне / мусор / `__`); порядок: delete→create→sync |
| `TopicSyncProcess` | + act-ветки: `LifecycleDelete` → journal → DeleteTopics → txn `[del факт-ключа][del заявки]`; `LifecycleCreate` → journal → CreateTopics → del заявки; `LifecycleCleanup` → del. Отказ Kafka — транзиент (тиком); del-этап — txn по mod_revision заявки (проигрыш → следующий тик) |
| Тесты | `KafkaWorker.UnitTests`: decide-таблица всех веток §3.2 (включая коллизию заявок, missing+create, `__`, AlreadyExists/NotFound-исходы), парсер leaf-ключей, canonical-JSON тикетов. `KafkaWorker.IntegrationTests` (Testcontainers etcd + `apache/kafka:4.0.0`): постановка create-заявки → тик → топик в Kafka с заданными partitions/RF/конфигами, заявка снята, факт-ключ следующим тиком; delete-заявка → топик исчез, ключи удалены; повтор тика после «отказа между мутацией и del» — сходимость |

Адаптер — по-прежнему единственное место с Confluent-типами; AAA-комментарии в тестах.

## 5. AdminPanel — правки канона и кода

### 5.1. Мутации (канон adminpanel/02 §10.2: +4 строки 9–12; код — `KafkaCommands`)

Общие: как существующие — активный endpoint, config напрямую у etcd, ProblemDetails,
имя кластера каноническое.

| # | Мутация | Протокол записи | Отказы |
|---|---|---|---|
| 9 | **Создание топика** `POST /api/kafka/clusters/{c}/topics` | тело `{name, partitions?, replicationFactor?, retentionMs?, minInSyncReplicas?}`. Проверки: кластер Active; имя — Kafka-паттерн без `__`; факт-ключ `topics/<t>` отсутствует **или** `missing=true` (пересоздание) — иначе 409 «топик существует»; нет живых `desired.create`/`desired.delete` — 409; нет живого `desired` у missing-ключа — 409 «сначала отмените конфиг-заявку». Развёртка дефолтов из config кластера: partitions = `default_partitions`, replicationFactor = `replication_factor` (значения пишутся в etcd полностью). Клэйм-txn `version(desired.create)==0` + put канонического JSON | 400 (валидация §5.2), 404 (кластер), 409 (не Active / топик есть / заявка жива), 503 |
| 10 | **Удаление топика** `DELETE /api/kafka/clusters/{c}/topics/{t}` | клэйм-txn `version(desired.delete)==0` + put `{"requested_unix","requested_by"}`. Пред-проверки: Active; факт-ключ существует и не missing (иначе 404); нет живого `desired.create` (409 «сначала отмените заявку создания»); нет живого `desired` (409 «сначала отмените конфиг-заявку» — явность лучше неявного уничтожения заявки). Идемпотентно: живая delete-заявка → 204 без записи (порт TO_REMOVE-семантики кластера/брокера) | 404, 409, 503 |
| 11 | **Отмена заявки создания** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired.create` | del ключа заявки; 404 если заявки нет | 404, 409 (не Active), 503 |
| 12 | **Отмена заявки удаления** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired.delete` | del ключа заявки; 404 если заявки нет. **Окно деструктивности**: снимает удаление до тика воркера | 404, 409, 503 |

POST create → 201 `{cluster, topic, partitions, replicationFactor}`; DELETE → 204.
Без компенсаций: неудавшаяся клэйм-txn запись просто не встала (повтор безопасен),
как у ротации.

### 5.2. Валидация создания (канон §10.3: +строки)

| Поле | Правило |
|---|---|
| `name` | `^[a-zA-Z0-9._-]{1,249}$`, без `__`-префикса (как мутации 6–7) |
| `partitions` | целое 1..1000, def = config.default_partitions |
| `replicationFactor` | целое 1..9 и ≤ config.brokers, def = config.replication_factor |
| `retentionMs` | 1..2147483647, опц. (нет → брокерный default, не пишется в configs) |
| `minInSyncReplicas` | ≥ 1 и ≤ эффективного RF, опц. |

`configs` заявки — только управляемые ключи (лишние — 400), сервер собирает из
retentionMs/minInSyncReplicas (как `KafkaTopicDesiredPlan.Build`).

### 5.3. Чтение: снапшот/DTO/UI (канон adminpanel/03 §7 + код)

- **Парсер `KafkaParser`**: ветка `topics` → leaf-ключи заявок в
  `KafkaLifecycleTicket {Topic, Op, Partitions?, ReplicationFactor?, RetentionMs?,
  MinInSyncReplicas?, RequestedUnix, RequestedBy}` в `KafkaClusterInfo`; битый JSON →
  parseError, прочие leaf → unknownKeys. Чтение бесплатное — refresher уже range'ит
  весь префикс `/kafka/clusters/`.
- **DTO**: `KafkaTopicDto` + поле `lifecycle?: TopicLifecycleDto`; create-заявка без
  факт-ключа → «виртуальная» строка (факт-поля null/0, бейдж создания).
- **UI (вкладка Топики)**: кнопка **«Создать топик»** — модал (name, partitions,
  replicationFactor, retention, minISR; дефолты из config кластера; клиентская
  валидация-зеркало §5.2); per-row бейджи заявок («создание: 12 партиций, RF 3» /
  «удаление…» + возраст/автор) с кнопкой **«Отменить заявку»**; красная per-row
  **«Удалить топик»** → подтверждающий модал: текст «топик и все его данные будут
  удалены из Kafka безвозвратно; заявка исполнится в течение ~15 с, до этого можно
  отменить» + **ввод имени топика** для активации кнопки. Подпись вкладки
  обновляется: «создание/удаление топиков — заявками панели; внешние изменения
  (CLI/клиенты) подхватываются автосинком». `canMutate` = Active — как сейчас.
- **Алерты (`KafkaAlertEngine`, канон §7.4: +строки)**:

| kind | severity | Условие |
|---|---|---|
| `kafka-topic-create-pending` | info | живая create-заявка |
| `kafka-topic-delete-pending` | warning | живая delete-заявка (деструктивная близка к исполнению) |
| `kafka-lifecycle-stale` | warning | lifecycle-заявка не снята дольше `StaleDesiredSec` (600) — воркер буксует/кластер лежит |

- **Тесты**: `AdminPanel.UnitTests` — валидация create (границы/межполевые),
  хендлеры 9–12 на fake-гейтвее (EtcdFixtures): 201/204/400/404/409/идемпотентность
  DELETE, развёртка дефолтов, канонический JSON заявок; парсер leaf-ключей; правила
  алертов. AAA-комментарии.

## 6. Стенд и e2e

- **`dev-stand/adminpanel/kafka-seed.sh`**: в сид-кластер `events` добавляются
  `topics/audit/desired.create` (12/3 + retention) и `topics/orders/desired.delete` —
  архетипы заявок для API-чека (воркер в API-профиле не поднят).
- **`checks/50-kafka-api.sh`**: +шаги — GET деталей: бейджи заявок в DTO
  (`audit` — виртуальная строка создания, `orders` — удаление); негативы: повторный
  create `audit` → 409; create существующего `payments` → 409; delete missing-топика
  → 404; отмена несуществующей заявки → 404; позитив: отмена create `audit` → 204,
  ключ заявки исчез из etcd.
- **`checks/55-kafka-e2e.sh`**: +шаги (после missing-ветки, перед демонтажём брокера):
  (10) POST create топика `e2e-panel` (6 партиций, RF 3, retention 1 д) → ждём
  факт-ключ (partitions 6, RF 3, retention 86400000, без заявок) + `kafka-topics
  --describe` сверка кредами из etcd; (11) негативы: повторный create → 409, RF 10 →
  400, partitions 0 → 400; (12) DELETE `e2e-panel` из панели → ждём: факт-ключ и
  заявка исчезли, `kafka-topics --list` пуст для топика; (13) отмена создания:
  POST create `e2e-cancel` → сразу DELETE desired.create (204) → ключ заявки исчез
  из etcd; выжидаем 2 тика и требуем отсутствия факт-ключа `topics/e2e-cancel`
  (если тик воркера успел раньше отмены и топик создался — чек доводит его до
  удаления DELETE-мутацией шага 12-паттерном и продолжает; гонка задокументирована).
- **Сборка/поставка**: без изменений (образ воркера и compose уже в стволе; новых
  проектов/пакетов нет).

## 7. Фазы плана (одна волна мержа; порядок строгий)

1. **Арх-канон**: правки `arch/15` (§2/§2.1/§3/§5/§6), `arch/16` (§1/§3.2/§5 D/границы),
   `arch/adminpanel/02` §10, `arch/adminpanel/03` §7 — до кода (arch-first).
2. **Воркер**: seam-методы + модель/парсер + decide/act + юнит/интеграция.
3. **Панель**: парсер/DTO + мутации 9–12 + UI + алерты + юнит-тесты.
4. **Стенд/e2e**: сид + чеки 50/55; прогон всех чеков с чистого состояния.

## 8. Ограничения, допущения, выносы в roadmap

Допущения (без пользователя, по канонам-прецедентам):

- RF создаётся и не мутируется (read-only факт) — начальный RF задаётся только
  create-заявкой; изменение RF существующего топика — по-прежнему `t02-kafka-reassignment`.
- Живость брокеров при create не проверяется (детерминированный guard RF ≤
  config.brokers; прецедент «live-проверку панель НЕ делает»); «повисшее» создание
  видно как `kafka-lifecycle-stale`.
- Конфиг-заявка `desired` у удаляемого топика гасится вместе с факт-ключом (панель
  отсекает живой desired 409-ом раньше; прямой обход — крайний случай, сходимость та же).
- Факт-ключ после create кладёт следующий автосинк-тик (≤ 15 с окно «топик есть,
  ключа нет») — консистентно с CLI-созданием; P12-снапшоты не добавляются.
- Массовых лимитов на заявки за тик нет (панель ставит по одной; отказ одной операции
  прерывает тик — остальные следующим тиком, как сейчас для desired).

Без изменений: ротация, add/remove брокера, деprovisioning, SASL/безопасность
(`t03`), метрики (`t04`), дискавери-библиотека (`t05`), reassignment (`t02`).
Roadmap-гейт: `t01` удаляется из `arch/roadmap/kafkaworker.md` коммитом мержа.

## 9. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный (unit без
   Docker, integration с Docker); все новые тесты с AAA-комментариями.
2. **Создание**: POST на живом стенде → заявка `desired.create` в etcd → ≤ 2 тиков
   воркера топик в Kafka ровно с заявленными partitions/RF/конфигами, заявка снята,
   факт-ключ автосинком с теми же значениями; «топик уже существует» (создан параллельно
   CLI) — del заявки + запись журнала, без применения параметров; повтор POST при живой
   заявке → 409.
3. **Удаление**: DELETE → `desired.delete` → топик и оба ключа (факт + заявка) исчезли;
   повтор DELETE при живой заявке → 204; отмена до тика → топик остаётся; топик,
   удалённый CLI при живой заявке, — del заявки без ошибки.
4. **Гварды**: не-Active → 409; несуществующий/missing-топик в delete/create-by-name →
   404/409; живые create/delete/desired-конфликты → 409; `__`-имя и неканонические
   имена → 404; RF 10 / partitions 0 / minISR > RF → 400 (ProblemDetails).
5. **Идемпотентность/надёжность**: перезапуск воркера между Kafka-мутацией и del
   заявки — сходимость следующим тиком; takeover клэйма ≤ TTL 15 с + тик; jitter-ретраи
   транзиентов; AlreadyExists/NotFound — не ошибки.
6. **Панель**: виртуальная строка создания и бейджи заявок (возраст/автор) рядом
   с missing/desired-бейджами; подтверждение удаления требует ввода
   имени; алерты create/delete/stale приходят и уходят с заявками; снапшот переживaет
   отказ etcd; битый JSON заявки не роняет парсер (parseError + `kafka-key-malformed`).
7. **Дискавери**: после create клиент по ключам etcd видит топик в реестре (эндпоинт
   §3.5), leaf-ключи заявок в реестр не попадают.
8. e2e: `50-kafka-api.sh`, `55-kafka-e2e.sh` (с новыми шагами) зелёные с чистого
   состояния; остальные чеки стенда не сломаны.
9. **Канон**: `arch/15`/`arch/16`/`adminpanel/02`/`adminpanel/03` обновлены и
   соответствуют реализации; пункт t01 удалён из `arch/roadmap/kafkaworker.md`.

## 10. Открытые вопросы

Нет — паттерн `topics/<T>/desired.create` задан задачей и roadmap; клэйм-постановка,
идемпотентный DELETE, пороги staleness и гварды — порты действующих прецедентов
канона (ротация §4, TO_REMOVE-семантика, `StaleDesiredSec`); детали — допущения §8.
