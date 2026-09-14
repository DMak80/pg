# t12-integration-red-debt — долг красных интеграционных тестов (spec)

- **Дата**: 2026-09-14
- **Roadmap**: `arch/roadmap/pgworker.md`, тег `t12-integration-red-debt` (снимается тем же коммитом мержа в `main` — мерж-гейт)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt`
- **Тип**: починка тестовой инфраструктуры; prod-код НЕ меняется; контракт `arch/` НЕ меняется (обоснование в §2.4)

## 1. Цель

Полная интеграционная серия PgWorker (`src/tests/PgWorker.IntegrationTests`) обязана
быть зелёной: устранить 4 предсуществующих падения, выявленных на мерж-гейте t07
(2026-09-13, падают идентично на базе до диффа t07 — то есть t07 их не создавал,
а лишь вскрыл):

1. **`AdoptionContractTests` ×3** — сиды «внешних» кластеров не обновлены под
   обязательные заявки `request_{cpu,mem}`, введённые коммитом `f6d4574`
   (2026-09-12, fix(pgtune): заявки ОБЯЗАТЕЛЬНЫ — дефолтов нет).
2. **Флейк клэйма «sc3»** в `ShardScaleContractTests.RemoveShardProcess_OnRealEtcd_CleansKeysWithRealTxnAndDel`
   — при полной сборке клэйм кластера «sc3» перехватывается джобой соседнего
   сценария той же etcd-коллекции.

Мерж-гейты не должны принимать красную базу как норму: зелёная полная серия —
критерий готовности задачи.

## 2. Диагноз (проверен по коду worktree)

### 2.1. AdoptionContractTests ×3 — нет обязательных заявок в сидах

Коммит `f6d4574` сделал `/service/<C>-<X>/request_{cpu,mem}` единственным
источником размера ноды (канон `arch/14-pgworker.md` §2.1 п.4):

- `PgtuneInputsFactory.Create(NodeResources?)` — fail-fast: заявки нет →
  `InvalidOperationException` («обязательная заявка request_mem
  отсутствует/нечитаема…»).
- `AdoptionProcess` (репарационный путь, строки ~395–401): для каждого шарда с
  живыми контейнерами `ReadShardResourcesAsync(cluster, shard)` читает
  `/service/<cluster>-<shard>/request_{cpu,mem}`; чтение не удалось → `null` →
  фабрика фейлит тик.

В `AdoptionContractTests.cs` коммит `f6d4574` поправил только сигнатуру
`PgtuneSettings` (удалил параметр дефолт-памяти `8589934592`), но **сид заявок
не добавил** — интеграционный класс остался на старом контракте:

| Тест | Кластер | Шард | Нужный scope заявок |
|---|---|---|---|
| `Adopt_ExternalCluster_WritesPortallocAndNodeStates` | `adoptc1` | `s1` | `/service/adoptc1-s1/request_{cpu,mem}` |
| `Adopt_PartialDiscovery_JournalContainsSkipped` | `adoptc2` | `s1` | `/service/adoptc2-s1/request_{cpu,mem}` |
| `Adopt_LegacyDockerHostNameInPortalloc_RepairsToAdvertisedHost` | `adoptadv` | `shard1` | `/service/adoptadv-shard1/request_{cpu,mem}` |

Первые два теста сидируются хелпером `SeedExternalClusterAsync` (scope
`{cluster}-s1`), третий — собственным сидом в теле теста (scope
`adoptadv-shard1`). Итог каждого: тик `AdoptionProcess.TickAsync` возвращает
`Failed` → красный ассерт `IsSuccess.Should().BeTrue(...)`.

### 2.2. Флейк клэйма «sc3» — общий контур коллекции + неуникальные имена

Механика (все элементы проверены по коду):

1. **Общий etcd на коллекцию.** `EtcdCollection` (`EtcdFixture.cs`, строка 102):
   один etcd-контейнер на все contract/coordination-классы; инвариант
   зафиксирован комментарием «ключи не пересекаются». Классы коллекции xUnit
   выполняет последовательно (Backups-* по алфавиту FQN раньше Etcd-*).
2. **Соседний сценарий с тем же именем.** `BackupSupervisorProcessTests`
   (добавлен задачей t07, 2026-09-13) клэймит кластеры **`sc1`, `sc2`, `sc3`** —
   имена, уже занятые `ShardScaleContractTests` (t06, `sc1`..`sc6`). Инвариант
   «ключи не пересекаются» нарушен.
3. **Утечка клэйма на 15 с.** Хелпер `SeedAsync` BackupSupervisor-класса чистит
   `/pgworker/claims/<C>` только **перед** клэймом; после теста клэйм не
   отпускается: `ClaimStore` (поле класса) не диспозится, keepalive-цикл не
   запущен, ключ не удаляется. `ClaimStore` ставит ключ на lease **TTL 15 с**
   (`ClaimTtlSec`), без keepalive ключ живёт до 15 с после теста.
4. **Жертва без предочистки.** `ShardScaleContractTests` клэймит
   `TryClaimClusterAsync("sc3")` без предварительного `DeleteAsync` клэйм-ключа
   (в отличие от Backups-классов, см. `SeedAsync`-паттерн). Клэйм — txn
   `compare version==0` (`NotExists`): если с последнего PUT соседа прошло <15 с,
   txn проваливается → `Result.Value == false` → красный ассерт
   `.Value.Should().BeTrue()`.

Отсюда «флейк в полной сборке»: время между последним sc3-PUT соседа и
`TryClaim("sc3")` жертвы плавает (параллельные коллекции грузят машину) — при
<15 с падение, при ≥15 с зелено.

### 2.3. Почему именно sc3, а не другие пересечения

- Пересечение `sc1` (`ShardScale.SeedActiveClusterAsync` vs BackupSupervisor
  `SeedAsync("sc1")`) не стреляет: ShardScale-тесты на `sc1`/`sc2` клэймов не
  делают, а BackupSupervisor пишет только `/pgworker/{claims,work,backups}/sc1`,
  не трогая `/clusters/sc1`.
- Пересечение `c1` (`RestoreProcessTests` × `WalStreamProcessTests` × default
  `BuildSnap`) не стреляет: оба класса чистят клэйм-ключ перед клэймом
  (`SeedAsync`: `DeleteAsync /pgworker/claims/<C>` → `TryClaim`).

Оба «несющих» пересечения — латентные (чистка перед клэймом держит их на
конвенции); систематическое закрытие — в §4.3 (ограничение скоупа).

### 2.4. Контракт `arch/` не меняется (правило источника истины)

- Обязательность заявок — **действующий канон** `arch/14-pgworker.md` §2.1 п.4,
  канонизирован самим `f6d4574` (вместе с §5 J, §8). Задача приводит тесты к
  канону, а не меняет его.
- Инвариант EtcdCollection «один etcd, ключи не пересекаются» — комментарий
  фикстуры, не `arch/`-контракт; починка — следование инварианту.
- Смена контракта не нужна: prod-код (AdoptionProcess, ClaimStore,
  PgtuneInputsFactory) ведёт себя ровно как задумано; дефект — в сидах и
  изоляции тестов.

## 3. Принципы

1. **Никаких sleep / ожиданий TTL** — флейк лечится устранением общего контекста
   (уникальные имена, чистка), не таймингами (AGENTS.base.md: сон-поллинг ≤30 с
   запрещён и не нужен).
2. **Механическая гарантия, не конвенция**: непересечение имён кластеров
   достигается per-class guid-тегом (канон `docs/e2e-isolation.md` §1: guid во
   всех именах; эталон — `E2eEnvironment.ClusterTag = runId[..8]`), а не
   «подбором других литералов» — литеральная конвенция уже сломалась один раз
   (t07 взял занятые имена).
3. **Own-only чистка и предочистка**: тест не полагается на то, что сосед
   прибрал за собой (предочистка своего клэйм-ключа перед `TryClaim` — паттерн
   Backups-классов), и сам не оставляет живых клэймов после себя (`ClaimStore` в
   `await using` — `DisposeAsync` отзывает lease, ключ исчезает немедленно, не
   через 15 с).
4. **Минимальный diff**: правится только тестовая инфраструктура двух
   диагносцированных дефектов; массовая переливка имён всей коллекции не
   делается (§4.3); prod-код не трогается.
5. **Сиды как у панели**: формат заявок — канонический строковый вид панели
   (`request_cpu="2"`, `request_mem="4Gi"`; парсер `NodeResourcesParser`:
   десятичные ядра + суффиксы Ki/Mi/Gi/Ti), тот же, что в
   `ShardScaleContractTests.SeedAddDeclarationAsync` и сид-хелперах E2E.
6. **Зелёный критерий**: полная серия зелёная дважды подряд (флейк устранён, а
   не замаскирован удачным таймингом); зачистка docker-остатков между сериями —
   по AGENTS.md.

## 4. Структура / компоненты (что именно меняется)

Все изменения — в `src/tests/PgWorker.IntegrationTests/`.

### 4.1. AdoptionContractTests — посев обязательных заявок

1. `SeedExternalClusterAsync(string cluster)` — добавить два PUT (закрывает
   `adoptc1-s1` и `adoptc2-s1`):
   - `/service/{cluster}-s1/request_cpu` = `"2"`
   - `/service/{cluster}-s1/request_mem` = `"4Gi"`
2. Сид теста `Adopt_LegacyDockerHostNameInPortalloc_RepairsToAdvertisedHost` —
   добавить те же два PUT для scope `adoptadv-shard1`.
3. Комментарий сида — дополнить: заявки обязательны (arch/14 §2.1 п.4), сид
   зеркалит то, что пишет панель при создании шарда.

Ожидаемый эффект: `ReadShardResourcesAsync` возвращает ресурсы,
`Pgtune.Create` считает тюнинг, все три тика `Done` — ассерты зелёные.

### 4.2. Флейк «sc3» — изоляция клэймов

1. **Per-class guid-тег имени кластера** (механическая уникальность) в двух
   конфликтующих классах:
   - `ShardScaleContractTests`: `private static readonly string Tag =
     Guid.NewGuid().ToString("N")[..8];` — имена кластеров строятся как
     `$"sc1{Tag}"`…`$"sc6{Tag}"`. Etcd-ключи уже интерполируются от имени
     кластера (`/clusters/{c}/...`, `/service/{c}-{shard}/...`,
     `/pgworker/portalloc/{c}`, `/pgworker/evacuations/{c}/{shard}`,
     `/pgworker/backups/{c}/...`); кроме того, интерполяции требуют сиды,
     несущие имя кластера литералами: `NodeObjects` StubScaleDriver
     (`"pgw-sc3-shard1-shard1a"` → `$"pgw-sc3{Tag}-..."` — RemoveShardProcess
     матчит префикс `pgw-{cluster}-{shard}-`, строки 98/254/256) и имя
     backup-агента `pgw-backup-wal-sc6-shard1` (сид + ассерты теста AC6);
   - `BackupSupervisorProcessTests`: тот же механизм (`$"sc1{Tag}"` и т.д.),
     интерполяции требуют `BuildSnap`, S3-префиксы/объекты
     (`("sc3/shard1/full/...")` → от тегированного имени) и журнальные
     ожидания.
2. **Предочистка клэйма перед `TryClaim`** в `ShardScaleContractTests` для
   клэймящих тестов (`sc3`, `sc5`, `sc6`): `DeleteAsync /pgworker/claims/{c}`
   перед `TryClaimClusterAsync` — паттерн `SeedAsync` Backups-классов; тест
   перестаёт зависеть от таймингов соседа.
3. **Own-only teardown клэйма**: локальные `ClaimStore` в тестах обоих классов
   оборачиваются `await using` — `DisposeAsync` отзывает lease, живых
   `/pgworker/claims/<C>` после теста не остаётся (ключи исчезают вместе с
   lease — отдельный ассерт чистоты не нужен); для per-class поля `_claims` в
   BackupSupervisor класс реализует `IAsyncLifetime`, `DisposeAsync` диспозит
   ClaimStore.
4. **Фиксация инварианта**: комментарий `EtcdCollection` в `EtcdFixture.cs`
   дополняется правилом: имена кластеров в классах коллекции обязаны нести
   per-class guid-тег; клэймящие тесты чистят `/pgworker/claims/<C>` перед
   клэймом и отпускают клэйм в teardown.

### 4.3. Осознанные ограничения скоупа

- **Латентные пересечения `c1` (Restore × WalStream × BuildSnap-default) не
  переливаются**: оба класса уже держат паттерн «предочистка перед клэймом»,
  падений нет. Массовая уникализация имён всех классов EtcdCollection — отдельное
  решение (если понадобится — пункт roadmap), в этой задаче не делается (принцип
  минимального diff; YAGNI).
- `EtcdFixture` (random host port, CTOR fixed-port для тестов недоступности) не
  трогается: порты уже динамические, хардкодов нет.
- Уникализация делается guid-тегом в имени кластера, а не переименованием
  литералов: литеральная конвенция без механической гарантии уже допустила
  коллизию t06/t07.

## 5. Фазы

1. **Ф1 — сиды заявок Adoption**: правки §4.1; точечная проверка:
   `dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter
   FullyQualifiedName~AdoptionContractTests` — 3/3 зелёные.
2. **Ф2 — изоляция клэймов**: правки §4.2 (guid-теги, предочистка, `await using`,
   комментарий-инвариант); точечная проверка фильтрами
   `~ShardScaleContractTests` и `~BackupSupervisorProcessTests` — зелёные.
3. **Ф3 — полная серия ×2**: полный прогон `src/tests/PgWorker.IntegrationTests`
   дважды подряд (оба зелёные — флейк устранён, не замаскирован); после каждой
   серии — зачистка docker по AGENTS.md (`docker rm -f $(docker ps -aq)` +
   `docker network prune -f`; контейнеры dev-стенда `as-*`/`adminpanel` не
   трогать; контроль `docker ps -aq | wc -l == 0` с учётом стенда); юниты —
   безусловный прогон (код тестов компилируется, `TreatWarningsAsErrors=true`).

## 6. Ограничения

- Prod-код (`src/PgWorker.*`) не меняется; меняются только файлы
  `src/tests/PgWorker.IntegrationTests/`.
- `arch/` не меняется (обоснование §2.4); roadmap-тег `t12` снимается из
  `arch/roadmap/pgworker.md` тем же коммитом мержа в `main` (мерж-гейт).
- Никаких sleep/ожиданий TTL; таймауты ожиданий ≤30 с (здесь не нужны вовсе).
- Порты: ничего нового не хардкодится (фикстура уже на динамических портах).
- Комментарии — по-русски, идентификаторы — на английском.
- Тесты пишутся по AAA (Arrange/Act/Assert в комментариях).

## 7. Критерии приёмки

1. `AdoptionContractTests` — 3/3 зелёные; в сидах присутствуют
   `/service/<scope>/request_cpu` и `request_mem` (формат панели).
2. Полная интеграционная серия `PgWorker.IntegrationTests` зелёная **дважды
   подряд** (второй прогон — доказательство нефлейковости, не перезапуск для
   «выяснения, что было»: если что-то красное — анализ логов, затем фикс).
3. Имена кластеров `ShardScaleContractTests` и `BackupSupervisorProcessTests`
   содержат per-class guid-тег — пересечение имён механически невозможно.
4. Клэймящие тесты `ShardScaleContractTests` делают предочистку
   `/pgworker/claims/<C>` перед `TryClaim`; `ClaimStore` отпускается в teardown
   (`await using`/`IAsyncLifetime.DisposeAsync`) — после тестов живых клэйм-ключей
   этих классов в etcd нет.
5. Никаких sleep/TTL-ожиданий в лечении; порты динамические.
6. Юнит-серия зелёная; сборка без ворнингов (`TreatWarningsAsErrors`).
7. После серий — зачистка docker: 0 посторонних контейнеров/сетей (стенд не
   трогать).
8. `arch/` без изменений; единственное не-тестовое изменение репозитория —
   снятие тега `t12` из roadmap в мерж-коммите.
