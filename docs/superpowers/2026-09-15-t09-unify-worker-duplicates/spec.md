# t09-unify-worker-duplicates — унификация Pg↔Kfw-дублей вне панельного контура (spec)

- **Дата**: 2026-09-15 (правка по ревью plan↔spec Фазы 4, замечание №5)
- **Roadmap**: `arch/roadmap/pgworker.md`, тег `t09-unify-worker-duplicates` (снимается тем же коммитом мержа в `main` — мерж-гейт; см. §11)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/refactor-t09-unify-worker-duplicates`
- **Тип**: структурный рефакторинг (дедупликация кода с параметризацией). Поведение систем не меняется, КРОМЕ двух осознанных и согласованных с пользователем изменений (§7: семантика unreachable-трека у KafkaWorker; компаратор сортировки имён планировщика).
- **Шаблон**: `../Puzzle` — принцип «общие подсистемы в отдельных сборках вне домена» (каркас `Infrastructure.App`); прямого аналога координации воркеров в Puzzle нет, паттерн `Shared.*` из t08 легализован в решении.
- **Прецедент**: t08 (`docs/superpowers/2026-09-14-t08-unify-adminpanel-duplicates/`, слит в `main` коммитом cf06ce0) — общие сборки `src/Shared.{Core,Etcd,Tls,Metrics}` созданы той же линией унификации; t09 — её продолжение на Pg↔Kfw-дублях, осознанно не тронутых t08 (§5 В1/В2 того spec).

## 1. Цель

Устранить оставшиеся Pg↔Kfw-дубли вне панельного контура: координацию
воркеров (`ClaimStore`/`PortAllocLock`/`WorkJournal`), обслуживание etcd
(`SnapshotJob`), нейтральные writing-records (`PlanPut`/`ValidationError`) и
планировщики размещения/портов (`PlacementPlanner`/`PortAllocator`). Дубли
переносятся в существующие общие сборки `Shared.Etcd`/`Shared.Core` по
действующему паттерну t08; копии удаляются. Контракт etcd-ключей
(значения, TTL, txn-протоколы захвата) не меняется; меняется только
расположение кода и способ построения ключей (префикс — параметр).

Состав переносов определён по результатам фактического сравнения кода
(карта §2); три ключевых проектных решения согласованы с пользователем
через AskUserQuestion (§8).

## 2. Исходное состояние (проверено diff по коду worktree)

### 2.1. Карта дублей

| # | Подсистема | Копии | Строки Pg/Kfw | Степень расхождения |
|---|---|---|---|---|
| Д1 | **Coordination/ClaimStore** — пер-кластерные lease-клэймы + глобальный лидер + instance/api-ключи дискавери | `PgWorker.Etcd/Coordination`, `KafkaWorker.Etcd/Coordination` | 358/358 | **Идентичны** после замены префикса `/pgworker/`→`/kafkaworker/` (4 ключа: leader, claims, instances, api) и комментариев (arch/14↔arch/16) |
| Д2 | **Coordination/PortAllocLock** — глобальный portalloc-клэйм (txn version==0 + lease TTL 15 с) | там же | 164/164 | **Идентичны** после замены префикса (ключ `<prefix>/locks/portalloc`); `Key` — public const |
| Д3 | **Coordination/WorkJournal** — журнал фаз процесса `/<prefix>/work/<C>` + событие `PhaseWritten` (метрики, arch/18 §2.2) | там же | 200/138 | **Семантическое** (см. §2.2): Pg `WritePhaseAsync` сохраняет unreachable-трек (фикс AC6), Kfw `WriteAsync` его стирает; Pg несёт поля серии ретраев и эвакуационный журнал; Kfw `WriteSupervisionAsync` имеет параметр `lastError` |
| Д4 | **SnapshotJob** — снапшоты/compact/defrag etcd с ретеншном | `PgWorker.Provisioning/Snapshots`, `KafkaWorker.Etcd` | 133/133 | **Побайтово идентичны** (только namespace) |
| Д5 | **Writing/PlanPut** — нейтральная пара «ключ-значение» планов записи | `PgWorker.Core/Writing`, `KafkaWorker.Core/Writing` | 6/6 | **Побайтово идентичны** |
| Д6 | **Writing/ValidationError** — ошибка валидации поля (ProblemDetails) | там же | 5/5 | Идентичны (комментарий) |
| Д7 | **Planning/PlacementPlanner** — анти-аффинити нод по docker-хостам | `PgWorker.Core/Planning`, `KafkaWorker.Core/Planning` | 58/53 | **Доменное**: Pg — вход группы-шарды `ShardSpec`, выход `NodePlacement(Shard, Node, Host)`; Kfw — плоский список брокеров, `NodePlacement(Node, Host)`. `HostInfo(Name, UsedSlots)` — дубль в обоих файлах. Алгоритм один; компаратор сортировки имён: Pg default-culture, Kfw Ordinal |
| Д8 | **Planning/PortAllocator** — закрепление/довыделение портов из диапазона | там же | 79/62 | **Доменное**: Pg — тройка портов (pg=base/patroni=base+3000/doorman=base+1500), ключ результата «shard/node», модель `NodeAddress(Host, NodePorts)`; Kfw — один client-порт, ключ = нода, `NodeAddress(Host, ClientPort)`. Схема одна: pinned-reuse + первый свободный с шагом 1 |

Тестовые дубли:

- `WorkJournalPhaseEventTests` (Pg/Kfw, 146/143 строки, расхождение — имя
  метода и мелочи);
- `CoordinationTests` (Pg-юнит, прямой: `PgWorker.UnitTests/Etcd`);
- `PortAllocLockTests` — **дубль у обоих** (`PgWorker.UnitTests/Etcd`, 141
  строка; `KafkaWorker.UnitTests/Etcd`, 142): 6 кейсов совпадают поимённо
  (второй инстанс/false; release-такеover+идемпотентность; повторный
  TryAcquire тем же объектом; busy-гейт параллельных тиков; release не
  удаляет чужой ключ после перехвата; сбой txn → Failed) — расхождение
  только namespace и используемый фейк (`Fakes.FakeEtcd` соответствующего
  проекта);
- `PortAllocLockRaceTests` (интеграционные, у обоих, доступ к
  `PortAllocLock.Key`);
- прочее покрытие Kfw-координации (`ClaimStore`, `WorkJournal`) — через
  процессные тесты; `PlacementPlannerTests`/`PortAllocatorTests` — только
  Pg-юниты.

### 2.2. Семантическое расхождение WorkJournal (разобрано с пользователем)

- **PgWorker**: `WritePhaseAsync` без явного трека ПЕРЕЧИТЫВАЕТ ключ и
  СОХРАНЯЕТ существующий `unreachable`-трек (фикс AC6 фикс-гейта:
  процессы adopt/rotate/moves пишут фазы в тот же ключ
  `/pgworker/work/<C>` каждый тик и раньше стирали трек надзора →
  supervise перечитывал пустоту → пороги NodeDead/ShardDead не истекали
  никогда, эвакуация не стартовала).
- **KafkaWorker**: `WriteAsync` перезаписывает `WorkState` БЕЗ трека —
  **та же скрытая бага жива**: AddBrokerProcess/CaRotator/NodeRegenerator/
  DeprovisioningProcess и др. пишут фазы в `/kafkaworker/work/<C>`, куда
  NodeSupervisor пишет трек молчания; каждая фазовая запись сбрасывает
  first_seen → порог BrokerDead истекает дольше положенного.
- **Прочие расхождения API**: Pg `WriteSupervisionAsync(cluster, instance,
  unreachable, ct)` vs Kfw `(…, unreachable, lastError, ct)` — union
  добавляет `lastError?` (Pg передаёт null); Pg-only: `RetrySeries`-поля в
  `WorkState` + `WriteEvacuationAsync`/`ReadEvacuationAsync` +
  `EvacuationJournal` на отдельном ключе `/pgworker/evacuations/<C>/<X>`
  (у Kfw эвакуации нет — **это не дубль**).

### 2.3. Существующая инфраструктура

- `src/Shared.Core` (DI, Result, CQRS, Retry, HealthChecks и пр.) и
  `src/Shared.Etcd` (Client/: `EtcdGateway`, `IEtcdGateway`, txn-модели) —
  созданы t08; тесты `src/tests/Shared.{Core,Etcd,Metrics}.UnitTests`
  (в `Shared.Etcd.UnitTests` пока только протокольные `EtcdGatewayTests`,
  in-memory-фейка `IEtcdGateway` нет — появляется с этим переносом, §6).
  `PgWorker.Etcd`/`KafkaWorker.Etcd` уже ссылаются на `Shared.Etcd`.
- Регистрация симметрична в `Program.cs` обоих воркеров: `ClaimStore(endpoints,
  gateway, clock, advertiseApiUrl, certThumbprint)`, `PortAllocLock(endpoints,
  gateway, clock, claimStore.InstanceId)`, `WorkJournal(gateway, endpoints)`,
  `SnapshotJob(etcd, endpoints, dir, retention, maintenanceMin)`.
- Контракты ключей описаны в arch/14 §3.3 (Pg), arch/15/16 (Kfw), панель
  читает `/pgworker/work/<C>` (arch/adminpanel/02 §2.3.1) — значения ключей
  НЕ меняются.

## 3. Принципы

1. **arch-first**: поведение Kfw-журнала меняется (осознанный фикс, §7) —
   контракт значения `/kafkaworker/work/<C>` уточняется в `arch/16` ДО кода
   (фаза A, §10).
2. **Паттерн `Shared.*`** (t08): общие сборки не зависят от доменных
   (`PgWorker.*`/`KafkaWorker.*`); новых сборок НЕ создаётся — расширяются
   существующие `Shared.Etcd`/`Shared.Core`.
3. **Контракт etcd нетронут**: значения ключей, TTL, txn-протоколы захвата,
   JSON-payload — дословно те же; префикс ключа — параметр конструктора
   (литералы `"/pgworker"`/`"/kafkaworker"` живут у потребителей в
   `Program.cs`).
4. **Обнаруженное расхождение семантики — остановить и решить явно**
   (не «подтихо выбрать одну версию»): единственное такое расхождение —
   unreachable-трек, решение пользователя — §8.1.
5. **Минимальный diff у потребителей**: замены namespace закрываются
   глобальными using на уровне csproj (механика t08, точный перечень —
   в плане).
6. **TreatWarningsAsErrors=true**: после каждого переноса — чистка using и
   полный `dotnet build`.
7. **Язык**: комментарии/доки — русский; идентификаторы — английские.

## 4. Решение: состав переносов

### 4.1. `Shared.Etcd/Coordination` — Д1–Д3

| Тип | Что переносится | Параметризация |
|---|---|---|
| `ClaimStore` | as-is (Pg-копия — канон: идентичны) | ctor-аргумент `string keyPrefix`; ключи строятся: `{prefix}/leader`, `{prefix}/claims/<C>`, `{prefix}/instances/<id>`, `{prefix}/api/<id>`; `PayloadJson` — internal в Shared |
| `PortAllocLock` | as-is | ctor-аргумент `string keyPrefix`; `public const string Key` → **инстанс-свойство** `public string Key => $"{prefix}/locks/portalloc"` |
| `WorkJournal` | Pg-копия как база + union-API (ниже) | ctor-аргумент `string keyPrefix`; ключ `{prefix}/work/<C>` |
| `RetrySeries` | record-тройка (FailCount, FailFirstUnix, RetryNotBeforeUnix) — нейтральные числа | без параметризации |
| `WorkState` | **общая запись со всеми полями** (Op, Phase, Instance, UpdatedUnix, LastError, Unreachable?, FailCount?, FailFirstUnix?, RetryNotBeforeUnix?) | nullable-поля серии при null опускаются (`WhenWritingNull`) — JSON KafkaWorker побайтово не меняется |

**Union-API `WorkJournal`** (решение пользователя §8.2):

- `WritePhaseAsync(cluster, op, phase, instance, lastError, ct, RetrySeries? series = null, IReadOnlyDictionary<string, long>? unreachable = null)` — единственный метод фазовой записи: **Pg-семантика для обоих** (без явного трека — перечитать ключ и сохранить существующий `unreachable`); Kfw-вызовы `WriteAsync` переименовываются в `WritePhaseAsync` (~20 точек, механика);
- `ReadAsync`, `ReadUnreachableAsync` — as-is;
- `WriteSupervisionAsync(cluster, instance, unreachable, lastError?, ct)` — union: Kfw передаёт агрегированные warning-ы, Pg — `null`; событие `PhaseWritten` по-прежнему НЕ эмитит (arch/18 §2.2);
- событие `PhaseWritten` + пассивные наблюдатели — as-is.

**Не переносится (остаётся в `PgWorker.Etcd/Coordination`)**:
`EvacuationJournal` + `WriteEvacuationAsync`/`ReadEvacuationAsync` — Pg-домен
на отдельном ключе `/pgworker/evacuations/<C>/<X>`, у Kfw аналога нет. Код
выделяется в тонкий класс `EvacuationJournalStore(IEtcdGateway, string[]
endpoints)` (Put/Get + JSON + failover, ~45 строк as-is из Pg-копии);
потребители эвакуационного журнала (BucketEvacuator, DeprovisioningProcess,
RemoveShardProcess и др. — по grep `WriteEvacuationAsync|ReadEvacuationAsync`)
переключаются на него.

### 4.2. `Shared.Etcd/Maintenance` — Д4

- `SnapshotJob` — as-is (побайтовый дубль, параметризация не нужна),
  namespace `Shared.Etcd.Maintenance`. Копии удаляются:
  `PgWorker.Provisioning/Snapshots/SnapshotJob.cs`,
  `KafkaWorker.Etcd/SnapshotJob.cs`.
- Статическая `_lastMaintenanceUtc` остаётся static (воркеры — разные
  процессы; семантика не меняется).

### 4.3. `Shared.Core/Writing` — Д5–Д6

- `PlanPut`, `ValidationError` — as-is, namespace `Shared.Core.Writing`.
  Потребители (API-хендлеры обоих воркеров, seed-планы
  `PostgresDemoSeedPlan`/`KafkaDemoSeedPlan`, `ClusterCreatePlan`/
  `ShardScalePlan`/`KafkaWriting`) — через глобальные using.

### 4.4. `Shared.Core/Planning` — Д7–Д8 (обобщение, решение пользователя §8.3)

- **Модели**: `HostInfo(Name, UsedSlots)` (дубль в обоих planner-файлах —
  канонизируется здесь), `NodeGroup(Name, Nodes)`, `NodePlacement(Group,
  Node, Host)`, `PlacementPlan(Placements)`.
- **PlacementPlanner.Plan(IReadOnlyList<NodeGroup> groups, IReadOnlyList<HostInfo> hosts)** —
  алгоритм as-is: для каждой группы (по имени, Ordinal) — анти-аффинити
  нод группы по хостам, кандидаты least-loaded (загрузка, имя), при
  невозможности — наименее загруженный хост.
  - Pg-адаптация: `ShardSpec` → `NodeGroup(shard.Name, shard.Nodes.Select(n => n.Name))`; группа = шард. Маппинг — в вызывающем коде (или локальный хелпер `PgPlanning`, точка — план);
  - Kfw-адаптация: одна группа на кластер — `NodeGroup(cluster, nodes)`
    (имя группы — имя кластера, доступное в месте вызова; на алгоритм не
    влияет, только диагностика);
  - `NodePlacement.Group` у Kfw игнорируется потребителем (использует Node/Host).
- **PortAllocator.Allocate&lt;TAddress&gt;** — generic по модели адреса:
  `Allocate(plan, existing: IReadOnlyDictionary<string, TAddress>, taken:
  IReadOnlySet<(string Host, int Port)>, rangeFrom, rangeTo, portsOf:
  Func<TAddress, IReadOnlyList<int>>, makeAddress: Func<string/*host*/,
  int/*base*/, TAddress>) → Result<Dictionary<string, TAddress>>`.
  Схема as-is: pinned-reuse (все порты адреса свободны) + первый свободный
  base с шагом 1 + `Result.Failed` при исчерпании диапазона.
  - Pg-инстанс: `portsOf = a => [a.Ports.Pg, a.Ports.Patroni, a.Ports.Doorman]`, `makeAddress = (h, b) => new NodeAddress(h, new NodePorts(b, b + 3000, b + 1500))`, ключ результата «shard/node» (строки `"${Group}/${Node}"` у потребителя);
  - Kfw-инстанс: `portsOf = a => [a.ClientPort]`, `makeAddress = (h, p) => new NodeAddress(h, p)`, ключ = имя ноды;
  - доменные `NodeAddress`/`NodePorts` остаются в `PgWorker.Core/Model/Domain.cs` и `KafkaWorker.Core/Model/NodeAddress.cs` — Shared доменных моделей не знает.
- **Компаратор**: сортировка имён групп/нод/хостов — единый
  `StringComparer.Ordinal` (детерминизм; для ASCII-имён текущее поведение
  идентично — Pg раньше использовал default-culture, см. §7.2).

### 4.5. Целевая структура ссылок

```
/common/   Shared.Etcd   + Coordination/{ClaimStore, PortAllocLock, WorkJournal, RetrySeries, WorkState}
                          + Maintenance/SnapshotJob
                          (новых зависимостей нет: уже → Shared.Core)
           Shared.Core   + Writing/{PlanPut, ValidationError}
                          + Planning/{PlacementPlanner, PortAllocator, HostInfo}
                          (без новых пакетов)
/etcd/     PgWorker.Etcd     → теряет Coordination/{ClaimStore, PortAllocLock, WorkJournal};
                              + Coordination/EvacuationJournalStore (Pg-домен)
           KafkaWorker.Etcd  → теряет Coordination/* и SnapshotJob
/core/     PgWorker.Core     → теряет Writing/{PlanPut, ValidationError}, Planning/{PlacementPlanner, PortAllocator}
           KafkaWorker.Core  → аналогично
/prov/     PgWorker.Provisioning → теряет Snapshots/SnapshotJob
/tests/    Shared.Etcd.UnitTests  + координация/журнал (§6)
           Shared.Core.UnitTests  + Planning (§6)
```

- `Program.cs` обоих воркеров: литералы префиксов (`"/pgworker"`/
  `"/kafkaworker"`) передаются в ctors; остальная регистрация не меняется.
- `PgWorker.slnx` — без изменений (проекты существующие).

## 5. Что НЕ входит в скоуп (осознанно)

- **Фейки тестов** (`FakeEtcd` в `PgWorker.UnitTests/Provisioning/Fakes.cs`
  (493 строки) и `KafkaWorker.UnitTests/Provisioning/Fakes.cs` (341),
  `FakeEtcdGateway` в `PgWorker.UnitTests/Api`): roadmap-пункт t09 их не
  перечисляет; копии разошлись (seeder-ы доменных ключей, fault-инъекции) —
  объединение дало бы параметризованный полигон без выгоды текущей задаче.
- **`KafkaWriting`, `NodeRegenPlanner`, `EvacuationPlanner`,
  `PortPlanConvergence`, `PortAllocHealer`, `PortAllocIndex`** — per-worker
  потребители/специализации, не дубли (упомянуты как контекст адаптации).
- **Панельный контур** (`AdminPanel.*`) — уже закрыт t08; панель НЕ трогается
  (читает те же ключи — контракт не меняется).
- **Новые NuGet-пакеты/сборки** — не добавляются (CPM и slnx не меняются).
- **Изменение txn-протоколов захвата, TTL, структуры JSON-payload** —
  запрещено (контракт arch/14 §3.3, arch/15/16, adminpanel/02).

## 6. Перенос тестов

- `WorkJournalPhaseEventTests` ×2 → объединяются в
  `Shared.Etcd.UnitTests/WorkJournalTests.cs`: кейсы события
  (успех/провал Put/терминальные фазы/supervise-молчание) + **новые кейсы
  сохранения трека** (фазовая запись без явного unreachable сохраняет
  существующий трек — регресс-покрытие баги Kfw; перенос серии ретраев
  полями `series`).
- `CoordinationTests.cs` (PgWorker.UnitTests/Etcd) →
  `Shared.Etcd.UnitTests/Coordination/CoordinationTests.cs` as-is с
  параметризацией префикса.
- `PortAllocLockTests` — **обе копии вливаются** в один
  `Shared.Etcd.UnitTests/Coordination/PortAllocLockTests.cs` (решение по
  ревью Фазы 4, замечание №5 — цель дедупликации t09): кейсы копий
  совпадают поимённо (6 шт.), каноном берётся одна копия, уникальные кейсы
  (если выявятся сверкой в плане) переносятся; локальные файлы-дубли
  `PgWorker.UnitTests/Etcd/PortAllocLockTests.cs` и
  `KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs` удаляются; ассерты на
  `PortAllocLock.Key` переключаются со static-const на инстанс-свойство с
  фактическим префиксом теста.
- In-memory-фейк `IEtcdGateway` для `Shared.Etcd.UnitTests` (сегодня его
  там нет, §2.3) — берётся из переносимых копий (мини-фейк
  `CoordinationTests`/`WorkJournalPhaseEventTests`; выбор канона фейка —
  план). `Fakes.FakeEtcd`-копии проектов (§5) при этом остаются на месте —
  процессные тесты их не покидают.
- `PlacementPlannerTests` + `PortAllocatorTests` (PgWorker.UnitTests/Planning)
  → `Shared.Core.UnitTests/Planning`: существующие Pg-кейсы (группы-шарды,
  тройка портов, pinned-reuse, исчерпание диапазона) + **зеркальные
  Kfw-кейсы** (одна группа, один порт) — оба инстанса generic-аллокатора.
- Интеграционные `PortAllocLockRaceTests` ×2 — остаются по месту (Pg/Kfw
  против реального etcd), правка доступа `PortAllocLock.Key` (const →
  инстанс-свойство).
- Kfw-процессные тесты (AddBroker/CaRotator/NodeRegenerator/…) и
  Pg-процессные (Adoption/NodeSupervisor/…) — индикаторы регрессии
  семантики трека; при необходимости усилить кейсом «фаза в тике процесса
  не сбрасывает трек надзора» (точка — план).
- Правила тестов AGENTS.md (динамические порты, полный teardown, зачистка
  контейнеров/сетей между сериями, телеметрия e2e-launch) — без изменений.

## 7. Осознанные изменения поведения (согласованы, единственные)

1. **KafkaWorker: фазовые записи сохраняют unreachable-трек** (решение
   пользователя §8.1). Эффект: трек молчания брокеров больше не стирается
   фазами процессов в `/kafkaworker/work/<C>`; пороги BrokerDead истекают
   корректно. Это исправление скрытой баги, идентичной исправленной у
   PgWorker (AC6); JSON-ключ остаётся тем же, меняется только сохранение
   поля `unreachable` при фазовых записях. Отражение в контракте — arch/16
   (фаза A).
2. **Компаратор сортировки имён планировщика — Ordinal** (следствие
   объединения; для ASCII-имён вида `s1a`/`b1` результат идентичен).
   Pg-планировщик раньше сортировал default-culture comparer — формально
   изменение, практически детерминизм строже.

Иных изменений поведения нет: значения etcd-ключей, REST API, docker-образы,
конфигурация, метрики (событие `PhaseWritten` — тот же контракт) — нетронуты.

## 8. Решения пользователя (AskUserQuestion, 2026-09-15)

1. **Семантика WorkJournal**: «Pg-семантика для обоих» — общий
   `WritePhaseAsync` сохраняет unreachable-трек у обеих систем; чинит
   скрытую багу KafkaWorker.
2. **Модели журнала**: «Общая WorkState + Pg-store эвакуаций» — `WorkState`
   в Shared со всеми полями (серия — nullable, JSON Kfw не меняется);
   `RetrySeries` — нейтральная тройка в Shared; `EvacuationJournal` + ключи
   `/pgworker/evacuations/*` — Pg-домен, тонкий `EvacuationJournalStore`
   в `PgWorker.Etcd`.
3. **Планировщики**: «Обобщить оба в Shared» — Group-обобщение
   `PlacementPlanner`, generic `PortAllocator<TAddress>`; потребители
   адаптируются.

## 9. Риски и меры

| Риск | Мера |
|---|---|
| Регрессия семантики трека Kfw (переход на сохранение) | Фаза A фиксирует контракт в arch/16; новые кейсы WorkJournalTests (§6); процессные тесты Kfw + полные интеграции; полный E2E (фаза F.4) |
| Регрессия захвата клэймов/лока (txn/lease) при параметризации префикса | Код as-is, меняется только построение ключей; CoordinationTests/PortAllocLockTests (обе копии вливаются, §6) переносятся с параметром; интеграционные Race-тесты ×2 гейт |
| Опечатка в префиксе (ключ уехал в другой префикс = потеря клэймов) | Литералы префикса — в одном месте Program.cs каждого воркера; ассерты интеграционных тестов на фактические пути ключей (`/pgworker/locks/portalloc` и т.п.) |
| Регрессия планирования (Group-обобщение, generic-аллокатор, Ordinal) | Перенос Pg-юнитов планирования as-is + зеркальные Kfw-кейсы; E2E `Scale_AddEmptyShard` (минимум) и полный E2eFixture — portalloc затронут (AGENTS.md) |
| `TreatWarningsAsErrors` на осиротевших using | Полный `dotnet build` после каждой фазы |
| Осколки дублей после переноса | Grep-гейт (§12.1) на фазе чистки |
| Образы воркеров не собираются после смены ссылок | Фаза F: сборка `pgworker:dev` и `kafkaworker:dev` (docker/{PgWorker,KafkaWorker}.Dockerfile), compose-config валиден |
| Панель теряет чтение `/pgworker/work/*` (JSON) | WorkState-Pg сериализация не меняется: те же поля/имена/WhenWritingNull; панельный парсер не трогаем |

## 10. Фазы (скелет для плана)

Каждая фаза заканчивается зелёными build+unit своего контура; порядок
обязателен.

- **Фаза A — arch-first (контракт до кода)**: `arch/16-kafkaworker.md`:
  §5 (строка ~476, описание `/kafkaworker/work/<C>.unreachable`) —
  дополнить «фазовые записи процессов сохраняют существующий unreachable-трек
  (унификация с PgWorker, фикс сброса порогов BrokerDead)»; §2.1 (строка
  ~187, «порт PlacementPlanner PgWorker») — «общий `Shared.Core/Planning`
  (порт t09)». `arch/14-pgworker.md` — без правок (поведение Pg и структура
  ключей не меняются; сохранить сверку §3.3 при ревью). Сверка: иные живые
  упоминания переносимых типов вне `docs/superpowers/` (история — не трогаем).
- **Фаза B — Shared.Etcd/Coordination+Maintenance**: перенести ClaimStore,
  PortAllocLock, WorkJournal+RetrySeries+WorkState (union-API §4.1),
  SnapshotJob (§4.2); создать `EvacuationJournalStore` в PgWorker.Etcd;
  переключить `Program.cs` ×2 (префиксы в литералах); удалить дубли ×6
  файлов; перенести/объединить тесты (§6, вкл. вливание обеих копий
  `PortAllocLockTests`); глобальные using при необходимости.
- **Фаза C — Shared.Core/Writing**: PlanPut, ValidationError; переключить
  потребителей (хендлеры/планы/seed) на глобальные using; удалить дубли ×4
  файла.
- **Фаза D — Shared.Core/Planning**: обобщённые PlacementPlanner
  (NodeGroup/NodePlacement/HostInfo) и PortAllocator&lt;TAddress&gt;;
  адаптировать потребителей: Pg — AdoptionProcess, ProvisioningProcess,
  AddShardProcess (+маппинг ShardSpec→NodeGroup); Kfw — ProvisioningProcess,
  AddBrokerProcess, PortAllocHealer (одна группа); удалить дубли ×4 файла;
  перенести тесты + зеркальные Kfw-кейсы (§6).
- **Фаза E — чистка**: grep-гейт остатков (§12.1); ревизия using/неймспейсов
  в затронутых тестах; сверка slnx (без изменений).
- **Фаза F — верификация и гейты**:
  1. `dotnet build src/PgWorker.slnx -c Release` (0 warnings);
  2. все юнит-серии (вкл. Shared.*.UnitTests) — с зачисткой контейнеров/сетей
     между сериями по AGENTS.md;
  3. интеграционные серии (docker): PgWorker полные, KafkaWorker полные,
     AdminPanel — дым (панель не тронута, но читает `/pgworker/work/*`);
  4. **E2E-гейт**: тронуты `PgWorker.Core`/`PgWorker.Etcd`/
     `PgWorker.Provisioning` и portalloc → **полный docker-E2E прогон
     E2eFixture на свежем Release** (`PGW_TEST_DOCKER=1 dotnet test
     src/PgWorker.slnx -c Release`; минимум-маркер
     `--filter FullyQualifiedName~Scale_AddEmptyShard` — недостаточен,
     т.к. portalloc затронут);
  5. сборка docker-образов `pgworker:dev` и `kafkaworker:dev`
     (docker/{PgWorker,KafkaWorker}.Dockerfile), `deploy/docker-compose.yml`
     config валиден;
  6. телеметрия E2E — по постоянным правилам (`docs/e2e-launch.md`):
     упавший сценарий не перезапускается без анализа логов.

## 11. Мерж-гейт (по правилам `arch/roadmap/README.md` и AGENTS.md)

Тем же мерж-коммитом в `main`:

- удалить пункт `t09-unify-worker-duplicates` из
  `arch/roadmap/pgworker.md` (и из `←`-зависимостей, если упомянут);
- история задачи — `docs/superpowers/2026-09-15-t09-unify-worker-duplicates/`.

## 12. Критерии приёмки

1. **Дедупликация**: в `src/` ровно одна копия каждого типа Д1–Д8:
   grep-гейт — `grep -rn "class ClaimStore\|class PortAllocLock\|
   class WorkJournal\|class SnapshotJob\|record PlanPut\|
   record ValidationError\|class PlacementPlanner\|class PortAllocator"` по
   `src/` находит определения только в `Shared.*` (+ допустимый Pg-доменный
   `EvacuationJournalStore`); тестовый дубль `PortAllocLockTests` удалён —
   юнит-тест один, в `Shared.Etcd.UnitTests` (§6).
2. `dotnet build src/PgWorker.slnx -c Release` — зелёный (0 warnings).
3. Все тестовые серии зелёные: юниты (все проекты, вкл. Shared.*), интеграции
   PgWorker/KafkaWorker/AdminPanel (docker, с зачисткой), полный E2E-гейт
   (§10 F.4).
4. Контракт etcd не изменился: значения/имена ключей, TTL, txn-протоколы —
   те же (ассерты интеграционных тестов на пути ключей проходят без правок
   ожиданий, кроме §7.1-эффекта сохранения `unreachable`).
5. Изменения поведения ограничены §7 (два пункта, оба отражены в arch/16).
6. Docker-образы `pgworker:dev` и `kafkaworker:dev` собираются;
   `deploy/`/`dev-stand/` конфигурации не менялись.
7. arch/-доки обновлены (фаза A); roadmap-гейт исполнен (§11).

## 13. Open questions

Нет: три проектных развилки закрыты пользователем (§8); фактическая поправка
по ревью Фазы 4 (замечание №5 — вливание Kfw-копии `PortAllocLockTests`)
внесена без развилки: рекомендация ревьюера согласуется с целью
дедупликации t09. Иных развилок, меняющих архитектуру или объём на порядок,
не обнаружено — направление единственное (расширение существующих `Shared.*`
по паттерну t08).
