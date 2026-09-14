# t91-kafka-seed-api-flaky — флейк SeedDemo_AlreadySeeded_NoOp (spec)

- **Дата**: 2026-09-14 (правки по ревью Фазы 4, 2026-09-14)
- **Roadmap**: `arch/roadmap/kafkaworker.md`, тег `t91-kafka-seed-api-flaky` (снимается в рамках мерж-гейта отдельным коммитом в feature-ветке и попадает в `main` тем же мержем задачи — прецедент t11)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky`
- **Тип**: починка тестовой инфраструктуры; prod-код НЕ меняется; контракт `arch/` НЕ меняется (обоснование в §2.4)
- **Решение по развилке roadmap (A/B)**: **A+B** — выбрано пользователем 2026-09-14 (вопрос задан в Фазе 1 dev-flow): и самодостаточный Arrange флейкового кейса (A), и own-only teardown классов-загрязнителей (B).

## 1. Цель

Устранить флейк интеграционного теста
`KafkaWorker.IntegrationTests.Api.KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp`
(вскрыт мерж-гейтом 2026-09-14, фильтр `Api|Etcd`, ветка `feat-panel-worker-restart-cert`,
50/51). Тест обязан быть зелёным при **любом** порядке выполнения классов и методов
коллекции `kafka-api` — порядок в xUnit внутри коллекции недетерминирован, и добавление
в коллекцию новых классов не должно ломать существующие кейсы.

Дополнительно (вариант B) классы-загрязнители коллекции получают own-only teardown —
буква правила AGENTS.md «каждый интеграционный тест полностью чистит за собой».

## 2. Диагноз (проверен по коду worktree)

### 2.1. Контур коллекции

`KafkaApiCollection` (`KafkaApiFactory.cs`, строки 66–70) — collection-fixture
`KafkaApiFixture`: **один общий etcd-контейнер** (`EtcdFixture`) + одна WAF-фабрика
на все Api-классы. В коллекции живут: `ClusterMutationsApiTests`,
`TopicMutationsApiTests`, `KafkaSeedApiTests`, `UpdateBrokerResourcesApiTests`,
`WorkerApiCertStartupTests` (последние два добавлены веткой сертов/рестарта — сами за
собой чистят, но их появление сменило расклад порядка классов и вскрыло флейк).

### 2.2. Механика флейка (все элементы проверены по коду)

1. **Жертва без предочистки.** `KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp`
   (строки 92–111): Arrange делает **первый** `POST /api/seed/demo` БЕЗ чистки
   префиксов сида (в отличие от соседнего кейса `SeedDemo_EmptyEtcd_SeedsCanonicalKeySet`,
   строки 43–45, где чистка есть), затем читает `/kafka/clusters/events/config` и
   `/kafkaworker/rotations/events` для фиксации «до».
2. **Идемпотентная ветка прод-кода.** `SeedDemoHandler.HandleAsync`
   (`src/KafkaWorker.App/Api/Operations/SeedDemoHandler.cs`, строки 25–30): при живом
   `/kafka/clusters/events/config` возвращает `{"seeded":false}` и **не пишет ничего** —
   в том числе не пишет `/kafkaworker/rotations/events`.
3. **Загрязнители.** `ClusterMutationsApiTests` (кейсы `PutConfig`, `PostBroker`,
   `DeleteBroker_BrokerOnly`, `DeleteBroker_Unknown`, `Rotate_LiveTicket`,
   `Rebalance`, `RotateAdminPassword`) и `TopicMutationsApiTests` (кейсы `PutDesired`,
   `PutDesired_PartitionsDecrease`, `DeleteDesired`, `PostTopic`,
   `PostTopic_LiveTicket`) в Arrange зовут `KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events")`
   — хелпер чистит и наливает `/kafka/clusters/events/` (включая `config`). Teardown у
   кейсов этих классов **нет** → после них `/kafka/clusters/events/config` остаётся жить.
   При этом `/kafkaworker/rotations/events` эти кейсы не пишут (кроме
   `Rotate_LiveTicket`, который пишет заявку `ops` напрямую — тогда падения нет,
   ассерт «до == после» проходит на чужом значении).
4. **Падение.** Если последним перед жертвой выполнился кейс-загрязнитель без записи
   rotations: первый POST жертвы уходит в no-op, `/kafkaworker/rotations/events`
   отсутствует → `GetAsync` возвращает `Result<Kv?>` с `Value == null` → Assert
   `rotation.Value!.Value.Should().Be(...)` (строка 110) падает с
   **NullReferenceException**.

### 2.3. Почему флейк, а не детерминированное падение

Зависимость от порядка недетерминированных сущностей: (а) порядок **классов** внутри
коллекции xUnit не гарантирован; (б) порядок **методов** внутри класса не является
контрактом; (в) спасение зависит от того, какой именно кейс-загрязнитель оказался
последним (с записью rotations — зелено, без — красно). Подтверждено прогонами
2026-09-14: класс изолированно — 3/3 зелёный; фильтр на `main` (без новых классов) —
41/41; в ветке — 50/51.

### 2.4. Контракт `arch/` не меняется (правило источника истины)

Roadmap-пункт t91 прямо фиксирует: «Прод-код не затронут — чисто тестовая
инфраструктура». Идемпотентность `SeedDemoHandler` по живому `config` — канон
(перенос kafka-seed.sh 1:1, arch/16 §1.1.1); дефект не в поведении воркера, а в том,
что Arrange теста не гарантирует собственное стартовое состояние. Инвариант коллекции
«общий etcd» менять не нужно — лечится предочисткой (A) и самоочисткой (B).

## 3. Принципы

1. **Самодостаточность Arrange**: кейс не полагается на то, что соседи прибрали за
   собой, и не полагается на порядок выполнения — предочистка своих префиксов в
   Arrange (прецедент: `SeedDemo_EmptyEtcd_SeedsCanonicalKeySet`; тот же паттерн в
   `PostTopic`/`DeleteTopic`/`CancelLifecycle` из TopicMutationsApiTests).
2. **Own-only чистка в teardown**: кейс убирает **только** созданное им (свои
   кластеры и свои заявки) при любом исходе (`IAsyncLifetime.DisposeAsync`);
   никаких «выключек» общих префиксов коллекции.
3. **Никаких sleep/ретраев/таймингов** — флейк лечится устранением зависимости от
   порядка, не ожиданиями.
4. **Минимальный diff**: правятся только три файла тестовой инфраструктуры
   (`KafkaSeedApiTests.cs`, `ClusterMutationsApiTests.cs`, `TopicMutationsApiTests.cs`);
   массовый рефакторинг остальных классов коллекции не делается (§4.3).
5. **Зелёный критерий**: фильтр `Api|Etcd` (тот же, что вскрывал флейк) зелёный
   **дважды подряд** — флейк устранён, а не замаскирован удачным порядком.

## 4. Структура / компоненты (что именно меняется)

Все изменения — в `src/tests/KafkaWorker.IntegrationTests/Api/`.

### 4.1. Вариант A — самодостаточный Arrange жертвы

`KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp`: перед первым `POST /api/seed/demo`
добавить чистку всех префиксов сида — точная копия прецедента соседнего кейса
`SeedDemo_EmptyEtcd_SeedsCanonicalKeySet` (строки 43–45):

```
/kafka/clusters/events/
/kafka/clusters/pending/
/kafkaworker/rotations/
/kafkaworker/rebalances/
/kafkaworker/reassignments/
```

(DeleteAsync с `prefix: true`, CancellationToken теста — как в прецеденте.)

Эффект: первый POST гарантированно идёт по ветке наливки (`seeded:true`) и пишет
канонический набор, включая `/kafkaworker/rotations/events`; семантика кейса не
меняется (Act — повторный POST; Assert — `seeded:false` и значения «до == после»).
Дополнить комментарий Arrange объяснением, почему чистка обязательна (порядок
классов/методов не гарантирован; чужой `events/config` уводил первый POST в no-op
без записи rotations). Кейс становится устойчивым к **любым** будущим
загрязнителям, а не только к текущим двум.

### 4.2. Вариант B — own-only teardown классов-загрязнителей

**Семантика xUnit (важно).** Оба класса реализуют `IAsyncLifetime` — а это
**per-test** teardown: xUnit создаёт новый экземпляр тест-класса на каждый тест-метод,
поэтому `DisposeAsync` выполняется после **каждого кейса** класса, а не однократно
после всех (однократный class-level teardown — это `IClassFixture<T>`/collection
fixture). Per-test — осознанно и безопасно, чистка только усиливается:

- мусор не накапливается между кейсами одного класса (каждый кейс стартует на etcd,
  прибранном собственным DisposeAsync предыдущего кейса);
- все кейсы этих классов и так самодостаточны (каждый наливает свой сид в Arrange) —
  чистка после кейса не отнимает нужное у следующего;
- `DeleteAsync` по несуществующим ключам/префиксам — no-op (кейс мог не создавать
  часть ключей своего списка — например, `reb` без rebalance-кейса);
- тест-методы одного класса не параллелятся (параллелизм xUnit — на уровне
  коллекций; внутри коллекции классы, а значит и их кейсы, идут последовательно) —
  гонок между DisposeAsync одного кейса и Arrange следующего нет.

Сигнатуры — `ValueTask`, как у `KafkaApiFixture` (совместимо с primary-constructor;
прецедент хоста per-class — `RestartApiTests.RestartHost`): `InitializeAsync` —
`ValueTask.CompletedTask`, `DisposeAsync` — чистка **только своих** ключей.
CancellationToken в DisposeAsync — `CancellationToken.None` (токен теста к моменту
teardown уже неактуален; удаления — быстрые локальные HTTP до fixture-etcd).

**`ClusterMutationsApiTests`** — чистить:

- кластерные префиксы `/kafka/clusters/{c}/` для `c` ∈ {`smoke`, `dup`, `race`,
  `gone`, `events`, `events2`, `rotme`, `rotme2`, `reb`} (кластеры `bad`/`nosuch`
  не создаются — невалидное тело/404; их префиксы в список не входят, но no-op
  DeleteAsync при желании план может включить — результат тот же);
- заявки точечными ключами (не общими префиксами `/kafkaworker/...`):
  `/kafkaworker/rotations/events`, `/kafkaworker/rotations/rotme`,
  `/kafkaworker/rotations/rotme2`, `/kafkaworker/rebalances/events`,
  `/kafkaworker/rebalances/reb` (обычно уже снят DELETE внутри кейса — чистка
  идемпотентна), `/kafkaworker/admin_rotations/events`.

**`TopicMutationsApiTests`** — чистить кластерные префиксы `/kafka/clusters/{c}/`
для `c` ∈ {`events`, `events2`, `dx`, `cx`} (lifecycle-заявки `topics/<t>/desired.*`
живут под префиксом кластера и удаляются вместе с ним).

Списки префиксов/ключей оформить статическими массивами в DisposeAsync; комментарий —
«own-only: чистим только созданное этим классом (после каждого кейса — per-test
teardown IAsyncLifetime) при любом исходе (правило полной самоочистки AGENTS.md)».
Существующие предочистки внутри отдельных кейсов (`PostTopic`, `DeleteTopic`,
`CancelLifecycle`) НЕ убираются — они защищают от остатков KafkaSeedApiTests, который
демо-сид оставляет (его teardown в скоуп не входит, см. §4.3).

### 4.3. Осознанные ограничения скоупа

- **`KafkaSeedApiTests` не получает teardown**: кейсы наливают демо-сид и оставляют
  его (так ведет себя и `EmptyEtcd` с момента создания; соседи уже адаптированы
  предочистками — комментарий в `PostTopic`: «сид (KafkaSeedApiTests) оставляет
  живую audit/desired.create»). Изменение этого контракта соседства — отдельное
  решение, здесь не делается (минимальный diff).
- **`UpdateBrokerResourcesApiTests` не трогается**: наливает guid-уникальные
  кластеры (`res{guid}`), ни с кем не пересекается и не является источником данного
  флейка; гигиенический teardown guid-кластеров — при необходимости отдельный пункт
  roadmap (самовольно не заводится).
- `KafkaApiTestSeed`, `KafkaApiFixture`/`KafkaApiFactory`, `MtlsApiTests`,
  `MetricsTests`, `RestartApiTests`, `WorkerApiCertStartupTests` — без изменений
  (последние два уже чистят за собой).
- Прод-код (`src/KafkaWorker.*`) — без изменений.

## 5. Фазы

1. **Ф1 — вариант A**: правка `KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp` (§4.1).
   Точечная проверка: класс изолированно —
   `dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --filter FullyQualifiedName~KafkaSeedApiTests`
   — 3/3 зелёные.
2. **Ф2 — вариант B**: правка `ClusterMutationsApiTests` и `TopicMutationsApiTests`
   (§4.2). Точечная проверка обоих классов + жертвы фильтром
   `FullyQualifiedName~ClusterMutationsApiTests|FullyQualifiedName~TopicMutationsApiTests|FullyQualifiedName~KafkaSeedApiTests`
   — все зелёные.
3. **Ф3 — мерж-гейт-серия ×2**: полный фильтр `Api|Etcd` (тот же, что вскрывал флейк)
   **дважды подряд**, оба прогона зелёные; между сериями и после — зачистка docker по
   AGENTS.md: `docker rm -f` всех контейнеров **кроме стендовых** + `docker network
   prune -f`; контроль — 0 контейнеров помимо стендовых. Канонический список
   стендовых исключений (сверен по container_name/compose-проектам файлов стенда):
   префиксы **`as-`** (dev-stand/adminpanel/docker-compose.yml: as-etcd, as-minio,
   as-s1a/s1b/s2a/s2b/hc1a/hc1b/hc2a/hc2b, as-adminpanel, as-kafkaworker,
   as-prometheus, as-grafana, as-alertmanager), **`pgw-stand-`**
   (dev-stand/compose.yaml: pgw-stand-etcd) и **`deploy-`** (deploy/docker-compose.yml
   без container_name — имена даёт compose-проект по каталогу `deploy`:
   deploy-pgworker-*, deploy-kafkaworker-*). Юнит-серия KafkaWorker — безусловный
   прогон (код тестов компилируется, `TreatWarningsAsErrors=true`). E2E Release
   PgWorker не требуется — prod-код воркеров не тронут.

## 6. Ограничения

- Меняются только файлы `src/tests/KafkaWorker.IntegrationTests/Api/`:
  `KafkaSeedApiTests.cs`, `ClusterMutationsApiTests.cs`, `TopicMutationsApiTests.cs`.
- `arch/` не меняется (обоснование §2.4); roadmap-тег `t91` снимается из
  `arch/roadmap/kafkaworker.md` в рамках мерж-гейта **отдельным коммитом в
  feature-ветке** и попадает в `main` тем же мержем задачи (прецедент t11).
- Никаких sleep/ретраев/ожиданий; таймауты не вводятся (не нужны).
- Порты: ничего не хардкодится (фикстура уже на динамических портах).
- Комментарии — по-русски, идентификаторы — на английском.
- Тесты — по AAA (Arrange/Act/Assert в комментариях; правки A и B — Arrange/teardown
  соответственно, тела Act/Assert жертвы не меняются).

## 7. Критерии приёмки

1. `SeedDemo_AlreadySeeded_NoOp` в Arrange чистит 5 префиксов сида (§4.1) перед
   первым POST — кейс проходит при любом порядке классов/методов коллекции.
2. `ClusterMutationsApiTests` и `TopicMutationsApiTests` реализуют
   `IAsyncLifetime.DisposeAsync` с own-only чисткой ровно своих кластеров и заявок
   (списки §4.2); чистка выполняется после **каждого кейса** класса (per-test
   семантика xUnit — экземпляр класса на каждый тест) и при любом исходе кейса.
3. Фильтр `Api|Etcd` (`dotnet test src/tests/KafkaWorker.IntegrationTests -c Release
   --filter "FullyQualifiedName~Api|FullyQualifiedName~Etcd"`) зелёный **дважды
   подряд** (второй прогон — доказательство нефлейковости; если красно — анализ логов,
   затем фикс; перезапуск «для выяснения» запрещён).
4. Класс `KafkaSeedApiTests` изолированно — 3/3 зелёные.
5. Юнит-серия KafkaWorker зелёная; сборка без ворнингов (`TreatWarningsAsErrors`).
6. После серий — зачистка docker: 0 контейнеров помимо стендовых исключений
   (канонический список §5 Ф3: префиксы `as-`, `pgw-stand-`, `deploy-`); сети —
   `docker network prune -f`; dev-стенд не тронут.
7. `arch/` без изменений; roadmap-тег `t91-kafka-seed-api-flaky` снят из
   `arch/roadmap/kafkaworker.md` отдельным коммитом в feature-ветке в рамках
   мерж-гейта и попадает в `main` тем же мержем задачи.
