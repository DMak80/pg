# t09-unify-worker-duplicates — план реализации (унификация Pg↔Kfw-дублей)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перенести Pg↔Kfw-дубли (координация ClaimStore/PortAllocLock/WorkJournal, SnapshotJob, Writing-типы, планировщики Placement/PortAlloc) в существующие сборки `Shared.Etcd`/`Shared.Core`, удалить копии (вкл. тестовый дубль `PortAllocLockTests`), сохранив etcd-контракт байт-в-байт (кроме двух осознанных изменений §7 spec).

**Architecture:** Расширение существующих `Shared.*` по паттерну t08: общие сборки не зависят от доменных; префикс etcd-ключей — параметр конструктора, литералы живут в `Program.cs` воркеров; Pg-доменная эвакуация выделяется в тонкий `EvacuationJournalStore`. Порядок фаз A→F обязателен: arch-first, затем Shared-код, переключение потребителей (прод + юниты + интеграции), удаление дублей, тесты, чистка, верификация.

**Tech Stack:** .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`, xunit.v3 + FluentAssertions, testcontainers/docker (интеграции и E2E).

**Spec:** `docs/superpowers/2026-09-15-t09-unify-worker-duplicates/spec.md` (в worktree; редакция от ревью Фазы 4 — читать вместе с планом).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/refactor-t09-unify-worker-duplicates` — все пути ниже относительны его корня.

**Ревизия плана:** v6 — правка по пятому ревью Фазы 4 (единственное замечание: статические обращения `PortAllocLock.Key` правятся в B2/B4 в момент переключения на Shared, а не в B5 — класс «файл, удаляемый позже, компилируется до удаления»). Прежние ревизии: v2 — первое ревью №1–№5; v3 — повторное №1–№3; v4 — третье №1–№5 (формы записи точек); v5 — четвёртое №1–№4 (временные правки CoordinationTests, миграция Pg-юнитов в D3, using Snapshots).

## Global Constraints

- `dotnet build src/PgWorker.slnx -c Release` — 0 warnings (TreatWarningsAsErrors).
- Контракт etcd нетронут: значения ключей, TTL (15 с), txn-протоколы захвата, JSON-payload — дословно; префикс — параметр ctor (spec §3.3).
- Новых сборок/пакетов/slnx-правок — НЕТ (spec §4.5, §5).
- Осознанные изменения поведения — ТОЛЬКО два (spec §7): Kfw-фазовые записи сохраняют unreachable-трек; компаратор планировщика — Ordinal.
- Комментарии/доки — русский; идентификаторы — английские (AGENTS.base §7).
- Тесты: динамические порты, полный teardown, зачистка контейнеров/сетей между сериями, `BrokerBootSec ≤ 100 с` (AGENTS.md).
- E2E — только на свежем Release; перезапуск упавших сценариев без анализа логов запрещён (docs/e2e-launch.md).
- Коммит после каждой задачи; сообщения — краткие, в тоне истории (`git log --oneline -10` для сверки).
- **Глобальные using НЕ транзитивны между сборками** (урок ревью №1/v2): каждый csproj, чьи исходники используют переносимый тип, получает собственный `<Using Include="…"/>`; перечень по фазам — в задачах (включая Docker- и IntegrationTests-проекты).
- **Integration-проекты входят в slnx** (src/PgWorker.slnx, папка /tests/) — гейт каждой фазы `dotnet build src/PgWorker.slnx` компилирует и их: все точки вызовов переносимых типов (прод + юниты + интеграции) обязаны быть закрыты в задаче ДО гейта (урок ревью №1/v3).
- **Файл, удаляемый позже, обязан компилироваться до удаления** (уроки ревью №1/v5 и №1/v6): если файл удаляется в задаче N+k, а API/ctor/СТАТИЧЕСКИЕ ЧЛЕНЫ меняется в задаче N — в N добавляется временная правка файла (паттерн: PgWorker.UnitTests/Etcd/PortAllocLockTests.cs, WorkJournalPhaseEventTests.cs, CoordinationTests.cs; статические `PortAllocLock.Key` → инстанс-ссылки в момент переключения на Shared — B2/B4).
- **Механика правки точек вызовов обязана покрывать три формы записи** (урок ревью №1–№4/v4): (а) явные ctor-вызовы `new ClaimStore(…)`; (б) target-typed `new(…)` (`= new([ep], …)`, `=> new([ep], …)`, `??= new([ep], …)`, вкл. многострочные хелперы) — grep `new ClaimStore(` их НЕ матчит; (в) fully-qualified упоминания типов (`KafkaWorker.Etcd.Coordination.WorkJournal`, `PgWorker.Core.Writing.ValidationError`) — глобальные using их не чинят. Источник истины по полноте — grep-контроль в гейте задачи + компилятор (CS7036/CS1503/CS1061/CS0246/CS0120); численные счётчики в пунктах — «~»-ориентиры, не норма.
- Замечания по трём уточнениям реализации (не меняют поведение; соответствуют тексту spec §4.4 «доменные модели остаются у воркеров, ключ результата — у потребителя»):
  1. `PortAllocator.Allocate<TAddress>` получает дополнительный параметр-делегат `keyOf: Func<NodePlacement, string>` (Pg: `$"{Group}/{Node}"`, Kfw: `Node`) — без него невозможно соблюсти формат ключей JSON `/kafkaworker/portalloc/<C>` (ключи-ноды; контракт менять запрещено §5).
  2. Делегат `hostOf: Func<TAddress, string>` (у обоих `a => a.Host`) — проверка закрепления `pinned.Host == placement.Host` as-is требует доступа к хосту доменного адреса, которого Shared не знает.
  3. Возвращаемый тип `Allocate` — `Result<IReadOnlyDictionary<string, TAddress>>` (внутри `Dictionary`), как у текущих Pg/Kfw-версий — процессы возвращают `Result<IReadOnlyDictionary<…>>` и `return allocated;` обязан продолжить компилироваться (принцип минимального diff §3.5).

---

## Фаза A — arch-first: контракт Kfw-журнала до кода

### Задача A1: правки arch/16 (поведение + структура)

**Вход:** worktree чист (`git status`), spec прочитан.

**Действие:**
- [ ] В `arch/16-kafkaworker.md` §2.1 (строка ~187): фрагмент разорван переводом строки — «…анти-аффинити нод по docker-хостам (порт␊`  `PlacementPlanner PgWorker); порт-аллокатор…». Заменить двухстрочную скобку «(порт␊`  `PlacementPlanner PgWorker)» на однострочную «(общий Shared.Core/Planning, порт t09)».
- [ ] В `arch/16-kafkaworker.md` §5 (строка ~476, фрагмент «трек first_seen — в `/kafkaworker/work/<C>`.unreachable, порт PgWorker;») дополнить сразу после «порт PgWorker» словами: «фазовые записи процессов сохраняют существующий unreachable-трек (унификация с PgWorker, фикс сброса порогов BrokerDead)».
- [ ] `arch/14-pgworker.md` — НЕ трогать (поведение Pg и структура ключей не меняются; §3.3 сверяется на ревью).

**Выход:** arch/16 отражает оба осознанных изменения поведения (spec §7) и новое место планировщика.

**Проверка:** `git diff arch/16-kafkaworker.md` — ровно две смысловые правки; `grep -n "PlacementPlanner PgWorker" arch/16-kafkaworker.md` → пусто (до правки — hit на строке ~187).

**Spec:** §10 фаза A, §7.

### Задача A2: сверка живых упоминаний переносимых типов

**Вход:** A1 завершена.

**Действие:**
- [ ] `grep -rn "PgWorker.Etcd.Coordination\|KafkaWorker.Etcd.Coordination\|PgWorker.Provisioning.Snapshots" arch/ docs/ --include="*.md" | grep -v superpowers` — разобрать каждый hit: живое описание контракта → уточнить нейтрально («общая сборка Shared.Etcd.Coordination, префикс — параметр»); исторические записи (пост-мортемы, changelog-и) — НЕ трогать.

**Выход:** arch/ не содержит устаревших указаний на местоположение переносимых типов.

**Проверка:** вывод grep разобран построчно, решения зафиксированы в сообщении коммита.

**Spec:** §10 фаза A («сверка»).

### Задача A3: коммит фазы A

- [ ] `git add arch/ && git commit -m "docs(arch): t09 — unreachable-трек Kfw сохраняется фазовыми записями; планировщик — общий Shared.Core/Planning"`

---

## Фаза B — Shared.Etcd: Coordination + Maintenance

### Задача B1: Shared-код координации и обслуживания (новые файлы)

**Вход:** фаза A закоммичена.

**Действие:** создать четыре файла в `src/Shared.Etcd/` (код as-is из Pg-копий — канон, с перечисленными правками):

- [ ] `src/Shared.Etcd/Coordination/ClaimStore.cs` — донор `src/PgWorker.Etcd/Coordination/ClaimStore.cs`. Правки: namespace `Shared.Etcd.Coordination`; using `PgWorker.Core` → `Shared.Core`; ctor `ClaimStore(string keyPrefix, string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null, string? certThumbprint = null)`; все литералы ключей через параметр: `ClaimKey(cluster) => $"{keyPrefix}/claims/{cluster}"`, `TryBecomeLeaderAsync` — `($"{keyPrefix}/leader")`, `EnsureInstanceKeyAsync` — `($"{keyPrefix}/instances/{InstanceId}")` и `($"{keyPrefix}/api/{InstanceId}")`; класс `PayloadJson` перенести в этот же файл как `internal static class PayloadJson` (виден внутри сборки).
- [ ] `src/Shared.Etcd/Coordination/PortAllocLock.cs` — донор `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`. Правки: namespace `Shared.Etcd.Coordination`; using → `Shared.Core`; ctor `PortAllocLock(string keyPrefix, string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)`; `public const string Key = "/pgworker/locks/portalloc"` → `public string Key => $"{keyPrefix}/locks/portalloc"`; в комментарии к классу заменить упоминания «/pgworker/…» на параметризованные («{prefix}/…»); `PortLockBusyException` переносится в этот же файл с новым ctor: `public sealed class PortLockBusyException(string key) : Exception($"{key}: занят другим инстансом — повторить следующим тиком")`.
- [ ] `src/Shared.Etcd/Coordination/WorkJournal.cs` — донор `src/PgWorker.Etcd/Coordination/WorkJournal.cs`. Переносятся: `WorkState` (все поля, включая nullable-серию и unreachable — уже общий формат), `RetrySeries`, `WorkJournal`. Правки:
  - namespace `Shared.Etcd.Coordination`; using → `Shared.Core`;
  - ctor `WorkJournal(string keyPrefix, IEtcdGateway gateway, string[] endpoints)`; `WorkKey(cluster) => $"{keyPrefix}/work/{cluster}"`;
  - `WritePhaseAsync` — сигнатура и семантика Pg-копии as-is (сохранение трека при отсутствии явного `unreachable`), комментарий обобщить: «фазовая запись БЕЗ явного трека сохраняет существующий (fix сброса порогов NodeDead/BrokerDead; унификация t09)»;
  - `WriteSupervisionAsync` — union-сигнатура Kfw-копии as-is, БЕЗ default у `lastError` (как в Kfw-оригинале `KafkaWorker.Etcd/Coordination/WorkJournal.cs:89-91` — минимальный diff к оригиналу): `Task<Result> WriteSupervisionAsync(string cluster, string instance, IReadOnlyDictionary<string, long> unreachable, string? lastError, CancellationToken ct)`; тело Kfw-копии as-is;
  - `ReadAsync`, `ReadUnreachableAsync`, событие `PhaseWritten` + `WorkPhaseEntry` — as-is;
  - НЕ переносить: `EvacuationJournal`, `WriteEvacuationAsync`, `ReadEvacuationAsync`, `EvacuationKey` (→ задача B2);
  - сообщение об ошибке `ReadAsync` — через `WorkKey(cluster)` (уже параметризовано).
- [ ] `src/Shared.Etcd/Maintenance/SnapshotJob.cs` — донор `src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs`, правки только namespace `Shared.Etcd.Maintenance` и using → `Shared.Core`. Параметризация НЕ нужна; `_lastMaintenanceUtc` static — оставить (воркеры — разные процессы).
- [ ] `src/Shared.Etcd/Shared.Etcd.csproj`: добавить `<InternalsVisibleTo Include="PgWorker.UnitTests"/>` (доступ к `internal SnapshotJob.ResetMaintenanceState()` из `BucketEvacuatorTests`).

**Выход:** Shared-сборка содержит параметризованные типы; дубли ещё на месте (сборка пока не переключена).

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный (новые типы никому не видны, конфликтов нет).

**Spec:** §4.1, §4.2, §3.3 (контракт), §4.5.

### Задача B2: Pg-сторона — переключение координации, эвакуационный стор, тесты (юниты + интеграции)

**Вход:** B1 собрана.

**Действие:**
- [ ] `src/PgWorker.App/Program.cs`: вверху рядом с остальными — `const string KeyPrefix = "/pgworker";` (единое место литерала); регистрации:
  - `new ClaimStore(KeyPrefix, …)` (было: 5 аргументов без префикса);
  - `new WorkJournal(KeyPrefix, …)`;
  - `new PortAllocLock(KeyPrefix, …)`.
- [ ] Создать `src/PgWorker.Etcd/Coordination/EvacuationJournalStore.cs` (namespace `PgWorker.Etcd.Coordination`; using `System.Text.Json`, `System.Text.Json.Serialization`, `Shared.Core`, `Shared.Etcd.Client`) — перенести из Pg-копии WorkJournal as-is record `EvacuationJournal` + тонкий стор целиком:
  ```csharp
  // Журнал эвакуации шарда (Pg-домен): /pgworker/evacuations/<C>/<X> — ключ жёсткий,
  // у Kfw аналога нет (t09: WorkJournal ушёл в Shared, эвакуации остались доменом).
  public sealed class EvacuationJournalStore(IEtcdGateway gateway, string[] endpoints)
  {
      private static readonly JsonSerializerOptions Json = new()
      {
          DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
      };

      public Task<Result> WriteAsync(string cluster, string shard, EvacuationJournal j, CancellationToken ct)
          => WithFailoverAsync(endpoint => gateway.PutAsync(
              endpoint, EvacuationKey(cluster, shard), JsonSerializer.Serialize(j, Json), lease: null, ct));

      public async Task<Result<EvacuationJournal?>> ReadAsync(string cluster, string shard, CancellationToken ct)
      {
          var result = await WithFailoverAsync(endpoint => gateway.GetAsync(endpoint, EvacuationKey(cluster, shard), ct));
          if (!result.IsSuccess)
              return Result<EvacuationJournal?>.Failed(result.Error!);

          if (result.Value is not { } kv)
              return Result<EvacuationJournal?>.Success(null);

          try
          {
              return Result<EvacuationJournal?>.Success(JsonSerializer.Deserialize<EvacuationJournal>(kv.Value, Json));
          }
          catch (JsonException e)
          {
              return Result<EvacuationJournal?>.Failed(new ApplicationException(
                  $"битый журнал эвакуации /pgworker/evacuations/{cluster}/{shard}: {e.Message}", e));
          }
      }

      private static string EvacuationKey(string cluster, string shard) => $"/pgworker/evacuations/{cluster}/{shard}";

      private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
      {
          Result<T>? last = null;
          foreach (var endpoint in endpoints)
          {
              var result = await call(endpoint);
              if (result.IsSuccess)
                  return result;
              last = result;
          }
          return last!;
      }

      private async Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
      {
          Result? last = null;
          foreach (var endpoint in endpoints)
          {
              var result = await call(endpoint);
              if (result.IsSuccess)
                  return result;
              last = result;
          }
          return last!;
      }
  }
  ```
- [ ] Удалить `src/PgWorker.Etcd/Coordination/{ClaimStore.cs, PortAllocLock.cs, WorkJournal.cs}` (namespace `PgWorker.Etcd.Coordination` остаётся живым из-за EvacuationJournalStore).
- [ ] `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs`: ctor — добавить параметр `EvacuationJournalStore evacuations` (после `WorkJournal journal`); заменить все `journal.WriteEvacuationAsync(` → `evacuations.WriteAsync(` (4 точки: строки ~112, ~162, ~181, ~248) и `journal.ReadEvacuationAsync(` → `evacuations.ReadAsync(` (1 точка: строка ~65); фазовые `journal.WritePhaseAsync` — без изменений.
- [ ] `src/PgWorker.App/Program.cs`: регистрация `builder.Services.AddSingleton(sp => new EvacuationJournalStore(sp.GetRequiredService<IEtcdGateway>(), sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));` + добавить аргумент `sp.GetRequiredService<EvacuationJournalStore>()` в регистрацию `BucketEvacuator` (после WorkJournal).
- [ ] `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (строка ~165): `WriteSupervisionAsync(cluster, claims.InstanceId, track, ct)` → `WriteSupervisionAsync(cluster, claims.InstanceId, track, null, ct)` (union-API добавил обязательный `lastError` — без default, см. B1).
- [ ] Точки броска `new PortLockBusyException()` → `new PortLockBusyException(portLock.Key)` в `AdoptionProcess.cs` (~291), `ProvisioningProcess.cs` (~282), `AddShardProcess.cs` (~198) — переменная `portLock` в скоупе есть у всех трёх.
- [ ] Глобальные using (механика t08) — добавить в `<ItemGroup>`:
  - `src/PgWorker.App/PgWorker.App.csproj`, `src/PgWorker.Provisioning/PgWorker.Provisioning.csproj`, `src/PgWorker.Moves/PgWorker.Moves.csproj`, `src/PgWorker.Backups/PgWorker.Backups.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj`:
    `<Using Include="Shared.Etcd.Coordination"/>`.
- [ ] Тестовые ctor-точки, юниты (PgWorker.UnitTests): `new WorkJournal(` → `new WorkJournal("/pgworker", `, `new ClaimStore(` → `new ClaimStore("/pgworker", `, `new PortAllocLock(` → `new PortAllocLock("/pgworker", ` — во всех файлах по `grep -rln "new WorkJournal(\|new ClaimStore(\|new PortAllocLock(" src/tests/PgWorker.UnitTests` (вкл. `Etcd/PortAllocLockTests.cs` — до удаления в B5). Точки `new SnapshotJob(` НЕ менять.
- [ ] **Статические `PortAllocLock.Key` → инстанс-ссылки, Pg-файлы** (ревью №1/v6 — `Key` стал инстанс-свойством в B1: статические обращения дают CS0120 уже на гейте B2, НЕ в B5; переменные — по фактическому скоупу кейса):
  - `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`: ~122 `Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct)` → `Gateway.GetAsync(Endpoint, first.Key, ct)`; ~146 — то же → `first.Key` (в скоупе обоих кейсов есть `first`);
  - `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs` (удаляется в B5 — правка временная, паттерн B2): ~35 `etcd.Store[PortAllocLock.Key].Value` → `etcd.Store[first.Key].Value`; ~56 → `second.Key` (держатель после takeover); ~78 → `locks.Key`; ~114 `etcd.Seed(PortAllocLock.Key, …)` → `etcd.Seed(mine.Key, …)` (путь ключа определяется префиксом — одинаков у всех инстансов теста); ~120 → `mine.Key`. Фейк `Fakes.FakeEtcd` в юните НЕ меняется — `.Store[…].Value` остаётся валидным до удаления файла в B5.
- [ ] **Временная правка `src/tests/PgWorker.UnitTests/Etcd/CoordinationTests.cs` до удаления в B5** (ревью №1/v5 — файл удаляется в B5, но на гейте B2 обязан компилироваться; паттерн PortAllocLockTests/WorkJournalPhaseEventTests):
  - target-typed хелперы (строки ~143-146) — вставить `"/pgworker", ` первым аргументом:
    ```csharp
    private static ClaimStore NewStore(FakeGateway gateway, string? advertiseApiUrl = null)
        => new("/pgworker", ["http://etcd:2379"], gateway, TimeProvider.System, advertiseApiUrl);

    private static WorkJournal NewJournal(FakeGateway gateway) => new("/pgworker", gateway, ["http://etcd:2379"]);
    ```
  - кейс `WorkJournal_RoundTrip_EvacuationJournal` (~402-426; вызовы ~415-416): API эвакуаций ушёл из WorkJournal — переключить на стор:
    ```csharp
    // было: await journal.WriteEvacuationAsync("shop", "shard2", original, CancellationToken.None);
    //       var result = await journal.ReadEvacuationAsync("shop", "shard2", CancellationToken.None);
    // стало:
    var store = new EvacuationJournalStore(gateway, ["http://etcd:2379"]);
    await store.WriteAsync("shop", "shard2", original, CancellationToken.None);
    var result = await store.ReadAsync("shop", "shard2", CancellationToken.None);
    ```
    (record `EvacuationJournal` остаётся в живом `PgWorker.Etcd.Coordination` — using в файле уже есть).
- [ ] **Тестовые ctor-точки, интеграции — явные вызовы** (ревью №1/v3): механически зеркально юнит-пункту — `grep -rln "new ClaimStore(\|new WorkJournal(\|new PortAllocLock(" src/tests/PgWorker.IntegrationTests` (13 файлов: `Backups/` ×7 — {BackupOrphanSweeperTests, BackupSelfHealTests, BackupSupervisorProcessTests, BackupVerifyProcessTests, RestoreProcessTests, RetentionProcessTests, WalStreamProcessTests}; `Etcd/` ×6 — {AdoptionContractTests, EtcdContractTests, EtcdCoordinationTests, PortAllocLockRaceTests, RepairContractTests, ShardScaleContractTests}; 42 явные точки — ориентир, источник истины grep, вкл. `new PortAllocLock(...)` в AdoptionContractTests ~70) — во всех hits вставить `"/pgworker", ` первым аргументом ctor.
- [ ] **Тестовые ctor-точки, интеграции — target-typed `new(…)`** (ревью №2/v4 — grep `new ClaimStore(` их НЕ матчит): 7 точек Pg — вставить `"/pgworker", ` первым аргументом внутри `new(…)`:
  - `Backups/BackupSelfHealTests.cs:31`: `private readonly ClaimStore _claims = new("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);` (аналогично строки ниже);
  - `Backups/BackupSupervisorProcessTests.cs:25`, `Backups/BackupVerifyProcessTests.cs:34` (`??= new("/pgworker", [Fx.Endpoint], …)`), `Backups/RestoreProcessTests.cs:29`, `Backups/RetentionProcessTests.cs:29`, `Backups/WalStreamProcessTests.cs:21`;
  - `Etcd/EtcdCoordinationTests.cs:15`: `private ClaimStore NewClaimStore() => new("/pgworker", [Endpoint], Gateway, TimeProvider.System);`
- [ ] **4-аргументные `WriteSupervisionAsync` в Pg-тестах** (ревью №2/v3 — union-сигнатура без default; CS7036 без этой правки): вставить `null, ` перед ct в 4 точках:
  - `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` ~103, ~796: `await journal.WriteSupervisionAsync("shop", "seed", track, null, CancellationToken.None);` и ~1175: `await journal.WriteSupervisionAsync("shopA", "seed", new Dictionary<string, long> { … }, null, CancellationToken.None);`
  - `src/tests/PgWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs` ~122 (временная правка до удаления файла в B5 — тот же паттерн, что для `Etcd/PortAllocLockTests.cs`): `await journal.WriteSupervisionAsync("demo", "i1", new Dictionary<string, long>(), null, TestContext.Current.CancellationToken);`
- [ ] Риг `src/tests/PgWorker.UnitTests/Provisioning/BucketEvacuatorTests.cs` (ревью №4/v2 — эвакуационные API ушли из WorkJournal):
  - record `Rig` (строка ~75): добавить поле `EvacuationJournalStore Store` (рядом с `WorkJournal Journal`);
  - `NewRig`: `var journal = new WorkJournal("/pgworker", etcd, [Ep]);` + `var store = new EvacuationJournalStore(etcd, [Ep]);`; ctor `BucketEvacuator` — вставить `store` после `journal`;
  - 4 точки: `rig.Journal.ReadEvacuationAsync("shop", "shard1", …)` (строки ~171, ~201, ~243) → `rig.Store.ReadAsync(…)`; `rig.Journal.WriteEvacuationAsync("shop", "shard1", new EvacuationJournal(…), …)` (~229) → `rig.Store.WriteAsync(…)`.
- [ ] Чистка using: `grep -rln "using PgWorker.Etcd.Coordination" src/` — из этих файлов удалить строку, если файл не использует `EvacuationJournalStore`/`EvacuationJournal` (namespace жив — unused using не ошибка, но чистим по §3.6; файлы, использующие эвакуации, — оставляют using).

**Выход:** Pg-контур (прод + юниты + интеграции, вкл. файлы до их удаления в B5) собирается на Shared-координации; эвакуации — Pg-доменный стор; риг эвакуаций компилируется.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `grep -rn "PortAllocLock\.Key" src/tests/PgWorker.UnitTests src/tests/PgWorker.IntegrationTests` → пусто (статические обращения закрыты — ревью №1/v6); `grep -rn "new ClaimStore(\[\|new WorkJournal(\[\|new PortAllocLock(\[" src/tests/PgWorker.IntegrationTests` → пусто; **target-typed-контроль, толерантный к `=> new(` и многострочности**: `grep -rnE "(ClaimStore|WorkJournal|PortAllocLock)[^;]*(=|=>) *new\(\[" src/tests/PgWorker.UnitTests src/tests/PgWorker.IntegrationTests` → пусто + точечно `grep -n "new(\[\"http://etcd\|new(gateway" src/tests/PgWorker.UnitTests/Etcd/CoordinationTests.cs` → пусто; `grep -rn "WriteEvacuationAsync\|ReadEvacuationAsync" src/tests/PgWorker.UnitTests` → пусто; `grep -rn "WriteSupervisionAsync(.*track, CancellationToken\|WriteSupervisionAsync(.*Dictionary<string, long> {, CancellationToken" src/tests/PgWorker.UnitTests` → пусто; `dotnet test src/tests/PgWorker.UnitTests -c Release` — зелёный (процессные тесты — индикаторы регрессии семантики трека, spec §6).

**Spec:** §4.1 (вкл. «не переносится»), §4.5, §6.

### Задача B3: SnapshotJob — переключение обеих систем

**Вход:** B2 зелёная.

**Действие:**
- [ ] Удалить `src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs` и `src/KafkaWorker.Etcd/SnapshotJob.cs`.
- [ ] Глобальные using `<Using Include="Shared.Etcd.Maintenance"/>` в: `src/PgWorker.App/PgWorker.App.csproj`, `src/KafkaWorker.App/KafkaWorker.App.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj`.
- [ ] **Явные using удаляемого namespace** (ревью №3/v5 — фактические строки, не полагаться на «CS0246 укажет»): удалить `using PgWorker.Provisioning.Snapshots;` в `src/PgWorker.App/Program.cs` (строка ~23) и `src/PgWorker.App/Loops/SnapshotLoop.cs` (~8) — namespace исчезает с удалением файла. У Kfw: `using KafkaWorker.Etcd;` в `Program.cs` (~16) и `SnapshotLoop.cs` (~8) остаётся живым (namespace жив — Parsing) — почистить вручную, если станет неиспользуемым.
- [ ] Потребители (упоминание типа `SnapshotJob`): `src/PgWorker.App/Loops/SnapshotLoop.cs`, `src/PgWorker.App/Program.cs`, `src/KafkaWorker.App/Loops/SnapshotLoop.cs`, `src/KafkaWorker.App/Program.cs`, `src/tests/KafkaWorker.UnitTests/App/LoopsHealthResetTests.cs`, `src/tests/PgWorker.UnitTests/Provisioning/BucketEvacuatorTests.cs` — резолв через глобальный using.

**Выход:** SnapshotJob существует только в Shared.Etcd.Maintenance.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `grep -rn "class SnapshotJob" src/` → единственный hit `src/Shared.Etcd/Maintenance/SnapshotJob.cs`; `grep -rn "using PgWorker.Provisioning.Snapshots" src/` → пусто.

**Spec:** §4.2, §12.1.

### Задача B4: Kfw-сторона — переключение координации + WritePhaseAsync

**Вход:** B3 зелёная.

**Действие:**
- [ ] `src/KafkaWorker.App/Program.cs`:
  - регистрации: `const string KeyPrefix = "/kafkaworker";`; `new ClaimStore(KeyPrefix, …)`, `new WorkJournal(KeyPrefix, …)`, `new PortAllocLock(KeyPrefix, …)`;
  - **fully-qualified ссылка** (ревью №4/v4): строка ~46 `sp.GetRequiredService<KafkaWorker.Etcd.Coordination.WorkJournal>().PhaseWritten += e => m.OnJournalPhase(…)` → `sp.GetRequiredService<WorkJournal>().PhaseWritten += …` (резолв через глобальный using `Shared.Etcd.Coordination`, добавляемый в `KafkaWorker.App.csproj` этой же задачей; после удаления `KafkaWorker.Etcd/Coordination/` квалификация — CS0246).
- [ ] Удалить `src/KafkaWorker.Etcd/Coordination/` целиком (ClaimStore.cs, PortAllocLock.cs, WorkJournal.cs).
- [ ] Переименование метода журнала — 13 прод-файлов, 57 вызовов: `journal.WriteAsync(`/`Journal.WriteAsync(` → `…WritePhaseAsync(` в:
  `src/KafkaWorker.Provisioning/Processes/{SecurityMigrator, ProvisioningProcess, NodeRegenerator, PasswordRotator, CaRotator, RemoveBrokerProcess, TopicSyncProcess, DeprovisioningProcess, AddBrokerProcess, PortAllocHealer, NodeSupervisor, PartitionReassignerProcess}.cs` и `src/KafkaWorker.App/Loops/ReconcileLoop.cs`. Сигнатуры позиционно совместимы (6 аргументов), семантика меняется осознанно (§7.1: без явного трека — сохранение существующего).
- [ ] **Временная правка Kfw-юнита до удаления в B5** (ревью №1/v4 — гейт B4 компилирует юнит-проекты; паттерн B2): `src/tests/KafkaWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs` строки ~86, ~103, ~137 — `journal.WriteAsync(` → `journal.WritePhaseAsync(` (сигнатуры позиционно совместимы; файл удаляется в B5).
- [ ] `NodeSupervisor.cs` Kfw: `WriteSupervisionAsync(…, lastError, ct)` — уже 5-аргументный вызов, НЕ менять.
- [ ] Точки броска `new PortLockBusyException()` → `new PortLockBusyException(portLock.Key)` (ревью №3/v2 — у PortAllocHealer ДВЕ): `ProvisioningProcess.cs` (~178), `AddBrokerProcess.cs` (~124), `PortAllocHealer.cs` **~80 (ReconstructAsync) и ~133 (ReallocateAsync)** — переменная `portLock` в скоупе есть во всех точках.
- [ ] Глобальные using `<Using Include="Shared.Etcd.Coordination"/>` в: `src/KafkaWorker.App/KafkaWorker.App.csproj`, `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj`, `src/KafkaWorker.Etcd/KafkaWorker.Etcd.csproj` (если в сборке остались упоминания типов координации; иначе не добавлять), `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj`, `src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`.
- [ ] Тестовые ctor-точки, юниты: `new WorkJournal("/kafkaworker", …)`, `new ClaimStore("/kafkaworker", …)`, `new PortAllocLock("/kafkaworker", …)` по `grep -rln` в `src/tests/KafkaWorker.UnitTests` — **вкл. `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs`** (правится здесь, удаляется в B5 при вливании копий).
- [ ] **Статические `PortAllocLock.Key` → инстанс-ссылки, Kfw-файлы** (ревью №1/v6 — те же переменные, что у Pg-зеркала):
  - `src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`: ~131 `Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct)` → `first.Key`; ~155 — то же → `first.Key`;
  - `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs` (удаляется в B5 — правка временная): ~36 `etcd.Store[PortAllocLock.Key].Value` → `etcd.Store[first.Key].Value`; ~57 → `second.Key`; ~79 → `locks.Key`; ~114 `etcd.Seed(PortAllocLock.Key, …)` → `etcd.Seed(mine.Key, …)`; ~120 → `mine.Key`. Фейк `Fakes.FakeEtcd` не меняется — `.Store[…].Value` валиден до удаления в B5.
- [ ] **Тестовые ctor-точки, интеграции — явные вызовы** (ревью №1/v3): механически — `grep -rln "new ClaimStore(\|new WorkJournal(\|new PortAllocLock(" src/tests/KafkaWorker.IntegrationTests` (14 файлов: `Kafka/` ×12 — {KafkaClusterFixture — общий риг всей Kafka-серии, AdminRotationTests, CaRotationTests, KafkaActiveGateTests, KafkaClientChurnTests, NodeRegenTests, ProvisioningTests, ReassignmentTests, SecurityMigrationTests, TlsClusterTests, TopicLifecycleTests, TopicSyncTests}; `Etcd/` ×2 — {ClaimStoreTests, PortAllocLockRaceTests}; ~50 явных точек — ориентир, источник истины grep) — во всех hits вставить `"/kafkaworker", ` первым аргументом ctor.
- [ ] **Тестовые ctor-точки, интеграции — target-typed `new(…)`** (ревью №2/v4): `src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs:18` — `private ClaimStore NewClaimStore() => new("/kafkaworker", [Endpoint], Gateway, TimeProvider.System);`.
- [ ] Чистка using: `grep -rln "using KafkaWorker.Etcd.Coordination" src/` — удалить строки (namespace исчез; CS0246 подсветит пропуски).

**Выход:** Kfw-контур (прод + юниты + интеграции, вкл. общий риг KafkaClusterFixture и файлы до их удаления в B5) на Shared-координации; Kfw-журнал — Pg-семантика сохранения трека.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `grep -rn "PortAllocLock\.Key" src/tests/KafkaWorker.UnitTests src/tests/KafkaWorker.IntegrationTests` → пусто; `grep -rn "journal\.WriteAsync(\|Journal\.WriteAsync(" src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests --include="*.cs" | grep -v WritePhase | grep -v WriteSupervision` → пусто; `grep -rn "KafkaWorker.Etcd.Coordination" src/ --include="*.cs"` → пусто; `grep -rn "new PortLockBusyException()" src/` → пусто; `grep -rn "new ClaimStore(\[\|new WorkJournal(\[\|new PortAllocLock(\[" src/tests/KafkaWorker.IntegrationTests` → пусто; `grep -rnE "(ClaimStore|WorkJournal|PortAllocLock)[^;]*(=|=>) *new\(\[" src/tests/KafkaWorker.IntegrationTests` → пусто; `dotnet test src/tests/KafkaWorker.UnitTests -c Release` — зелёный.

**Spec:** §4.1 (union-API), §7.1, §6 (индикаторы процессных тестов).

### Задача B5: тесты координации/журнала (вкл. вливание обеих копий PortAllocLockTests)

**Вход:** B4 зелёная; ctor-точки, временные правки юнитов и инстанс-ассерты `Key` уже закрыты в B2/B4 (здесь — только перенос/вливание тестов).

**Действие:**
- [ ] **Канон in-memory-фейка** (spec §6: «выбор канона фейка — план»; ревью №5/v2): каноном берётся мини-фейк из `PgWorker.UnitTests/Etcd/CoordinationTests.cs` — единственный из переносимых с полной моделью txn-compare (Version/Value) + lease grant/revoke/keepalive; мини-фейк `WorkJournalPhaseEventTests` (только `FailPuts`) беднее и поглощается им. Создать `src/tests/Shared.Etcd.UnitTests/Coordination/FakeCoordinationGateway.cs`: перенести `private sealed class FakeGateway` как `internal sealed class FakeCoordinationGateway : IEtcdGateway` (namespace `Shared.Etcd.UnitTests`); расширить: `public Func<TxnRequest, Result<TxnResult>>? TxnFault;` (в начале `TxnAsync`: `if (TxnFault is { } f) return Task.FromResult(f(req));`) и `public void Seed(string key, string value) => Store[key] = value;`. Доменные `Fakes.FakeEtcd`-копии проектов остаются на месте (spec §5/§6).
- [ ] Создать `src/tests/Shared.Etcd.UnitTests/Coordination/CoordinationTests.cs` (имя файла as-is по spec §6): ClaimStore-кейсы из Pg-копии (9 тестов: txn-compare version==0, занято-другим, лидер, keepalive-тик ×4 lease, release, потеря keepalive, etcd-down-при-старте, потеря instance-lease, dispose); все ctor → `new ClaimStore(Prefix, ["http://etcd:2379"], gateway, TimeProvider.System, …)` с `private const string Prefix = "/unit";`; ассерты ключей `/pgworker/…` → `/unit/…` (например `"/unit/claims/shop"`).
- [ ] Создать `src/tests/Shared.Etcd.UnitTests/Coordination/PortAllocLockTests.cs` — **вливание ОБЕИХ копий** (spec §6, ревью №5/v2): канон — Pg-копия `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs` (6 кейсов; статические `Key` уже переведены на инстанс-ссылки в B2/B4); сверить поимённо с Kfw-копией `src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs` (те же 6: `TryAcquire_SecondInstance_GetsFalse`, `Release_AllowsTakeover_AndIsIdempotent`, `TryAcquire_AlreadyHeldBySameObject_ReturnsFalse`, `SameObject_SecondTickBlockedUntilRelease`, `Release_AfterTakeover_DoesNotDeleteForeignKey`, `TryAcquire_EtcdTxnFailure_ReturnsFailed`; расхождение — только фейк: `Fakes.FakeEtcd` соответствующего проекта) — уникальных кейсов нет, различия тел поглотить каноном; `Fakes.FakeEtcd` → `FakeCoordinationGateway`; ctor `new PortAllocLock(Prefix, [Ep], etcd, TimeProvider.System, "inst-1")`; ассерты `etcd.Store[first.Key].Value` (Store — `Dictionary<string,string>`, `.Value` уходит — значение уже строка); `etcd.Seed(mine.Key, …)`; `TxnFault = _ => …` — совместимо с новым полем фейка.
- [ ] Создать `src/tests/Shared.Etcd.UnitTests/WorkJournalTests.cs` (корень проекта, путь по spec §6) — объединение `WorkJournalPhaseEventTests` ×2 + journal-кейсы из `CoordinationTests` + НОВЫЕ кейсы трека:
  - перенесённые (с `new WorkJournal(Prefix, gateway, ["http://fake"])`): эвент успеха с (cluster, op, phase); неудачный Put — без эвента (`FailPuts`); `WriteSupervisionAsync("demo", "i1", [], null, ct)` — без эвента; терминальная фаза `crashed` — с эвентом; camelCase-JSON (`"op":"provision"` и т.д. на ключе `/unit/work/shop`); round-trip WorkState; серия ретраев (`RetrySeries(3, …)` переносится и `"fail_count"` присутствует/отсутствует); legacy-JSON без полей серии → null;
  - НОВЫЙ регресс-кейс Kfw-баги (§7.1), код целиком:
    ```csharp
    [Fact]
    public async Task WritePhaseAsync_WithoutTrack_PreservesExistingUnreachableTrack()
    {
        // Arrange: supervise записал трек молчания (фаза без явного unreachable)
        var gateway = new FakeCoordinationGateway();
        var journal = new WorkJournal(Prefix, gateway, ["http://fake"]);
        await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b1"] = 100 }, null, TestContext.Current.CancellationToken);

        // Act: фазовая запись процесса БЕЗ трека (путь Kfw-процессов до t09)
        var result = await journal.WritePhaseAsync("demo", "reassign", "running", "i1", null, TestContext.Current.CancellationToken);

        // Assert: трек сохранён — пороги NodeDead/BrokerDead не сбрасываются фазами
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        state.Value!.Unreachable.Should().ContainKey("b1").WhoseValue.Should().Be(100);
    }
    ```
  - НОВЫЙ: `WritePhaseAsync_WithExplicitTrack_OverwritesExisting` — явный `unreachable` заменяет трек (владелец трека);
  - НОВЫЙ: `WriteSupervisionAsync_OverwritesTrackCompletely` — надзор перезаписывает трек актуальным множеством (пустой словарь → `Unreachable` пуст).
- [ ] Создать `src/tests/PgWorker.UnitTests/Etcd/EvacuationJournalStoreTests.cs`: round-trip-кейс `WorkJournal_RoundTrip_EvacuationJournal` из `CoordinationTests.cs` на `new EvacuationJournalStore(fakeEtcd, ["http://etcd:2379"])` (фейк — `Fakes.FakeEtcd` из `Provisioning/Fakes.cs`; в B2 кейс уже переключён на стор — здесь оформляется отдельным файлом в Pg-юнитах).
- [ ] Удалить: `src/tests/PgWorker.UnitTests/Etcd/{CoordinationTests.cs, PortAllocLockTests.cs}`, `src/tests/PgWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs`, `src/tests/KafkaWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs` и **`src/tests/KafkaWorker.UnitTests/Etcd/PortAllocLockTests.cs`** (влит в Shared, spec §6/§12.1).
- [ ] Контроль переноса: интеграционные Race-тесты ×2 уже переведены на инстанс-`Key` (B2/B4) и префикс-ctors (B2/B4); их Allocate-вызовы правятся в D3/D4 вместе с удалением доменных планировщиков (см. Задачи D3/D4) — здесь НЕ трогать.

**Выход:** единые Shared-тесты координации с параметризованным префиксом; тестовый дубль `PortAllocLockTests` устранён (обе копии влиты, локальные удалены); регресс-кейс сохранения трека.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `dotnet test src/tests/Shared.Etcd.UnitTests -c Release` — зелёный; `dotnet test src/tests/PgWorker.UnitTests src/tests/KafkaWorker.UnitTests -c Release` — зелёные; `grep -rn "class PortAllocLockTests" src/tests` → единственный hit `Shared.Etcd.UnitTests`; интеграционные Race/EtcdCoordination/ClaimStore — в фазе F (docker).

**Spec:** §6 (перенос, вливание обеих копий PortAllocLockTests, канон фейка), §12.1, §9 (регрессия трека).

### Задача B6: коммит фазы B

- [ ] `git add -A && git commit -m "refactor(coordination): t09 — ClaimStore/PortAllocLock/WorkJournal/SnapshotJob в Shared.Etcd; PortAllocLockTests влит из двух копий; Pg-эвакуации — EvacuationJournalStore; Kfw-фазы сохраняют unreachable-трек"`

---

## Фаза C — Shared.Core/Writing

### Задача C1: перенос PlanPut/ValidationError

**Вход:** фаза B закоммичена.

**Действие:**
- [ ] Создать `src/Shared.Core/Writing/PlanPut.cs` и `src/Shared.Core/Writing/ValidationError.cs` — код as-is из Pg-копий (`public sealed record PlanPut(string Key, string Value);`, `public sealed record ValidationError(string Field, string Message);`), namespace `Shared.Core.Writing`, комментарий русский as-is.
- [ ] Удалить 4 файла: `src/PgWorker.Core/Writing/{PlanPut.cs, ValidationError.cs}`, `src/KafkaWorker.Core/Writing/{PlanPut.cs, ValidationError.cs}`.
- [ ] Глобальные using `<Using Include="Shared.Core.Writing"/>` в: `src/PgWorker.Core/PgWorker.Core.csproj`, `src/KafkaWorker.Core/KafkaWorker.Core.csproj`, `src/PgWorker.App/PgWorker.App.csproj`, `src/KafkaWorker.App/KafkaWorker.App.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj`.
- [ ] **Fully-qualified ссылки на ValidationError** (ревью №3/v4 — 13 вхождений, глобальный using их не чинит; после удаления доменных дублей — CS0246 в App-проектах): заменить `PgWorker.Core.Writing.ValidationError` / `KafkaWorker.Core.Writing.ValidationError` → `ValidationError` в 3 файлах:
  - `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (10 вхождений, строки ~11–120);
  - `src/KafkaWorker.App/Api/Operations/KafkaExceptions.cs` (2: строки ~25, ~28);
  - `src/KafkaWorker.App/Api/Operations/AddBrokerHandler.cs` (1: строка ~43).
- [ ] Чистка локальных `using PgWorker.Core.Writing;`/`using KafkaWorker.Core.Writing;` — файлы по `grep -rln "using PgWorker.Core.Writing\|using KafkaWorker.Core.Writing" src/` (namespace остаётся живым из-за ClusterCreatePlan/ShardScalePlan/KafkaWriting — CS0246 не подсветит; чистить вручную по неиспользуемым).

**Выход:** Writing-типы существуют только в Shared.Core.Writing.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `grep -rn "record PlanPut\|record ValidationError" src/` → только Shared.Core/Writing; `grep -rn "Core.Writing.ValidationError" src/` → пусто; `dotnet test src/tests/PgWorker.UnitTests src/tests/KafkaWorker.UnitTests -c Release` — зелёные (Writing-тесты: ClusterCreatePlanTests, ShardScalePlanTests, KafkaWritingPlanTests).

**Spec:** §4.3.

### Задача C2: коммит фазы C

- [ ] `git add -A && git commit -m "refactor(writing): t09 — PlanPut/ValidationError в Shared.Core.Writing"`

---

## Фаза D — Shared.Core/Planning (обобщение)

### Задача D1: модели и PlacementPlanner в Shared

**Вход:** фаза C закоммичена.

**Действие:**
- [ ] Создать `src/Shared.Core/Planning/PlacementPlanner.cs` (namespace `Shared.Core.Planning`):
  ```csharp
  /// <summary>Хост размещения: имя + занятые слоты (ноды всех кластеров).</summary>
  public sealed record HostInfo(string Name, int UsedSlots);

  /// <summary>Группа нод с анти-аффинитетом (Pg — шард; Kfw — кластер).</summary>
  public sealed record NodeGroup(string Name, IReadOnlyList<string> Nodes);

  /// <summary>Назначение ноды группы на хост.</summary>
  public sealed record NodePlacement(string Group, string Node, string Host);

  /// <summary>План размещения: по записи на каждую плановую ноду.</summary>
  public sealed record PlacementPlan(IReadOnlyList<NodePlacement> Nodes);

  /// <summary>
  /// Планировщик размещения по docker-хостам (t09, обобщение Pg/Kfw): анти-аффинити —
  /// ноды одной группы на разных хостах, если позволяет топология; иначе least-loaded.
  /// Детерминизм: группы/ноды/кандидаты сортируются StringComparer.Ordinal.
  /// </summary>
  public static class PlacementPlanner
  {
      public static PlacementPlan Plan(IReadOnlyList<NodeGroup> groups, IReadOnlyList<HostInfo> hosts)
      {
          if (hosts.Count == 0)
              throw new InvalidOperationException("PlacementPlanner: список docker-хостов пуст");

          // Текущая загрузка: исходные UsedSlots + уже размещённые этим планом ноды.
          var load = hosts.ToDictionary(h => h.Name, h => h.UsedSlots);
          var placements = new List<NodePlacement>();

          foreach (var group in groups.OrderBy(g => g.Name, StringComparer.Ordinal))
          {
              // Хосты, уже занятые этой группой текущим планом (анти-аффинити).
              var takenByGroup = new HashSet<string>();

              foreach (var node in group.Nodes.OrderBy(n => n, StringComparer.Ordinal))
              {
                  // Кандидаты — хосты, ещё не занятые группой, least-loaded;
                  // если топология не позволяет — наименее загруженный хост.
                  var host = hosts
                     .Where(h => !takenByGroup.Contains(h.Name))
                     .OrderBy(h => load[h.Name])
                     .ThenBy(h => h.Name)
                     .FirstOrDefault()
                   ?? hosts
                         .OrderBy(h => load[h.Name])
                         .ThenBy(h => h.Name)
                         .First();

                  placements.Add(new NodePlacement(group.Name, node, host.Name));
                  load[host.Name]++;
                  takenByGroup.Add(host.Name);
              }
          }

          return new PlacementPlan(placements);
      }
  }
  ```
  Отличия от Pg-копии: `ShardSpec` → `NodeGroup`; все `OrderBy(…)` — с `StringComparer.Ordinal` (Pg-копия сортировала default-culture — осознанное изменение §7.2; для ASCII-имён результат идентичен).
- [ ] Ничего не удалять в этой задаче (Pg/Kfw-копии Planning удаляются в D3/D4 после адаптации потребителей).

**Выход:** обобщённый планировщик доступен потребителям.

**Проверка:** `dotnet build src/Shared.Core -c Release` — зелёный.

**Spec:** §4.4 (модели, компаратор), §7.2.

### Задача D2: generic PortAllocator в Shared

**Вход:** D1 собрана.

**Действие:**
- [ ] Создать `src/Shared.Core/Planning/PortAllocator.cs` (namespace `Shared.Core.Planning`) — финальный код целиком:
  ```csharp
  /// <summary>
  /// Аллокатор портов (t09, обобщение Pg/Kfw): схема одна — закрепление
  /// переиспользуется (нода на том же хосте и все порты адреса свободны),
  /// новый адрес — первый свободный base с шагом 1; диапазон исчерпан —
  /// Result.Failed. Доменная модель адреса — параметры-делегаты:
  /// portsOf (тройка Pg / один порт Kfw), hostOf, makeAddress, keyOf
  /// (Pg — "shard/node", Kfw — имя ноды; строки-ключи строит потребитель, spec §4.4).
  /// </summary>
  public static class PortAllocator
  {
      public static Result<IReadOnlyDictionary<string, TAddress>> Allocate<TAddress>(
          PlacementPlan plan,
          IReadOnlyDictionary<string, TAddress> existing,
          IReadOnlySet<(string Host, int Port)> busy,
          int rangeFrom,
          int rangeTo,
          Func<TAddress, IReadOnlyList<int>> portsOf,
          Func<TAddress, string> hostOf,
          Func<string, int, TAddress> makeAddress,
          Func<NodePlacement, string> keyOf)
      {
          var result = new Dictionary<string, TAddress>();
          // Порты, выделенные этим вызовом: кандидаты не должны пересекаться
          // не только с busy, но и между собой.
          var taken = new HashSet<(string Host, int Port)>(busy);

          foreach (var placement in plan.Nodes)
          {
              var key = keyOf(placement);

              // Закреплённый адрес переиспользуется, если нода на том же хосте
              // и порты никто не занял.
              if (existing.TryGetValue(key, out var pinned)
                  && hostOf(pinned) == placement.Host
                  && IsFree(pinned))
              {
                  MarkTaken(pinned);
                  result[key] = pinned;
                  continue;
              }

              // Новый base: первый свободный с шагом 1 (все порта кандидата свободны).
              var allocated = false;
              for (var port = rangeFrom; port < rangeTo; port++)
              {
                  var candidate = makeAddress(placement.Host, port);
                  if (!IsFree(candidate))
                      continue;

                  MarkTaken(candidate);
                  result[key] = candidate;
                  allocated = true;
                  break;
              }

              if (!allocated)
                  return Result<IReadOnlyDictionary<string, TAddress>>.Failed(
                      new InvalidOperationException(
                          $"PortAllocator: нет свободного порта на хосте {placement.Host} " +
                          $"в диапазоне [{rangeFrom},{rangeTo}) для {key}"));
          }

          return Result<IReadOnlyDictionary<string, TAddress>>.Success(result);

          // Все порты адреса свободны (не заняты docker и этим вызовом).
          bool IsFree(TAddress addr) => portsOf(addr).All(p => !taken.Contains((hostOf(addr), p)));

          void MarkTaken(TAddress addr)
          {
              foreach (var p in portsOf(addr))
                  taken.Add((hostOf(addr), p));
          }
      }
  }
  ```
  Пояснение к делегату `hostOf` (дополнение к Global Constraints): проверка закрепления `pinned.Host == placement.Host` из обеих копий требует доступа к хосту доменного адреса, которого Shared не знает; `hostOf: Func<TAddress, string>` (у обоих воркеров `a => a.Host`) сохраняет проверку as-is — та же причина, что у `keyOf`.

**Выход:** generic-аллокатор с единой схемой pinned-reuse/первый-свободный.

**Проверка:** `dotnet build src/Shared.Core -c Release` — зелёный.

**Spec:** §4.4 (PortAllocator), Global Constraints (уточнения 1–2).

### Задача D3: Pg-адаптация + миграция юнитов планирования (PgPlanning, 3 процесса, Race-тест Pg, using-листы, Shared-тесты Pg-кейсов)

**Вход:** D2 собрана.

**Действие:**
- [ ] Удалить `src/PgWorker.Core/Planning/PlacementPlanner.cs` и `src/PgWorker.Core/Planning/PortAllocator.cs`.
- [ ] Создать `src/PgWorker.Core/Planning/PgPlanning.cs` (namespace `PgWorker.Core.Planning`):
  ```csharp
  using PgWorker.Core.Model;
  using Shared.Core.Planning;

  /// <summary>Pg-инстанс обобщённых планировщиков (t09): группы = шарды,
  /// адрес = тройка портов pg/patroni/doorman, ключ результата — "shard/node".</summary>
  public static class PgPlanning
  {
      public static IReadOnlyList<NodeGroup> ToGroups(IReadOnlyList<ShardSpec> shards)
          => shards.Select(s => new NodeGroup(s.Name, s.Nodes.Select(n => n.Name).ToList())).ToList();

      public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.Ports.Pg, a.Ports.Patroni, a.Ports.Doorman];

      public static string HostOf(NodeAddress a) => a.Host;

      public static NodeAddress MakeAddress(string host, int basePort)
          => new(host, new NodePorts(basePort, basePort + 3000, basePort + 1500));

      public static string KeyOf(NodePlacement p) => $"{p.Group}/{p.Node}";
  }
  ```
- [ ] `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (~324-325):
  `PlacementPlanner.Plan(snap.Shards, hosts.Value)` → `PlacementPlanner.Plan(PgPlanning.ToGroups(snap.Shards), hosts.Value)`;
  `PortAllocator.Allocate(plan, existing, taken, placementOpts.PortFrom, placementOpts.PortTo)` → `PortAllocator.Allocate(plan, existing, taken, placementOpts.PortFrom, placementOpts.PortTo, PgPlanning.PortsOf, PgPlanning.HostOf, PgPlanning.MakeAddress, PgPlanning.KeyOf)`.
- [ ] `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (~217-218): `Plan([shard], …)` → `Plan(PgPlanning.ToGroups([shard]), …)`; Allocate — аналогично с делегатами.
- [ ] `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` (~318-319): `Plan(dsnShards, …)` → `Plan(PgPlanning.ToGroups(dsnShards), …)`; Allocate — с делегатами.
- [ ] **Глобальные using `<Using Include="Shared.Core.Planning"/>`** (ревью №1/v2 — глобальные using НЕ транзитивны; без них CS0246 на сборке slnx):
  - `src/PgWorker.Core/PgWorker.Core.csproj`, `src/PgWorker.Provisioning/PgWorker.Provisioning.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/Shared.Core.UnitTests/Shared.Core.UnitTests.csproj` (для Shared-тестов из миграции ниже; при необходимости — через `GlobalUsings.cs`: `global using Shared.Core.Planning;`);
  - **`src/PgWorker.Docker/PgWorker.Docker.csproj`** — `Drivers/ClusterDriver.cs` использует `HostInfo` в сигнатурах `GetHostsAsync` (строки 24/146/747);
  - **`src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj`** — `Etcd/{StubScaleDriver.cs, PortAllocLockRaceTests.cs, AdoptionContractTests.cs, ShardScaleContractTests.cs}`, `Backups/{BackupSelfHealTests.cs, BackupVerifyProcessTests.cs, RestoreProcessTests.cs}` используют типы Planning.
- [ ] **Чистка локальных using** (ревью №1/v2: namespace `PgWorker.Core.Planning` остаётся живым — PgPlanning/PortPlanConvergence, поэтому CS0246 НЕ подсветит осиротевшие директивы — чистить вручную): `grep -rln "using PgWorker.Core.Planning" src/PgWorker.Docker src/tests/PgWorker.IntegrationTests` — удалить строки, где после правок using не используется (файлы, использующие `PgPlanning`, — оставляют).
- [ ] **Миграция Pg-юнитов планирования в этой же задаче** (ревью №2/v5 — старые юниты `Plan(shards, hosts)` со `ShardSpec` и `Allocate` без делегатов несовместимы с новой сигнатурой: CS1503/CS7036 на гейте D3, если отложить до D5; источник кейсов должен быть доступен в момент переноса — Pg-часть D5 слита сюда):
  - создать `src/tests/Shared.Core.UnitTests/Planning/PlacementPlannerTests.cs`: перенести 5 Pg-кейсов из `src/tests/PgWorker.UnitTests/Planning/PlacementPlannerTests.cs`, вход строится через группы:
    ```csharp
    private static NodeGroup Group(string name, int replicas) =>
        new(name, Enumerable.Range(0, replicas).Select(i => $"{name}{(char)('a' + i)}").ToList());
    // кейсы: hosts==replicas → уникальные хосты; 1 хост → все на нём; hosts<replicas → 2+1;
    // UsedSlots → least-loaded; SameInput → SameOutput (детерминизм)
    ```
    ассерты на `n.Node`/`n.Host` остаются (у `NodePlacement` поля Group/Node/Host);
  - создать `src/tests/Shared.Core.UnitTests/Planning/PortAllocatorTests.cs`: перенести 6 Pg-кейсов из `src/tests/PgWorker.UnitTests/Planning/PortAllocatorTests.cs` с делегатами (локальные `Pg-инстанс`-хелперы файла):
    ```csharp
    private static IReadOnlyList<int> PortsOf(PgAddr a) => [a.Pg, a.Patroni, a.Doorman];
    private static string HostOf(PgAddr a) => a.Host;
    private static PgAddr MakeAddress(string host, int basePort) => new(host, basePort, basePort + 3000, basePort + 1500);
    private static string KeyOf(NodePlacement p) => $"{p.Group}/{p.Node}";
    private sealed record PgAddr(string Host, int Pg, int Patroni, int Doorman); // локальная модель вместо доменной

    // шаблон вызова (все 6 кейсов):
    var result = PortAllocator.Allocate(plan, existing, busy, 15000, 16000,
        PortsOf, HostOf, MakeAddress, KeyOf);
    ```
    (аргумент `plan` — `new PlacementPlan([new NodePlacement("shard1", "shard1a", "h1")])`; ассерты ключей `"shard1/shard1a"` сохраняются; ассерты портов `.Ports.Pg` → `.Pg` локальной модели);
  - удалить `src/tests/PgWorker.UnitTests/Planning/{PlacementPlannerTests.cs, PortAllocatorTests.cs}`;
  - класс `PortPlanConvergenceAllConfirmedTests` (жил в конце PortAllocatorTests.cs, строка ~121) перенести в конец существующего `src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs` (5 кейсов as-is — Pg-домен).
- [ ] **Race-тест Pg** (ревью №2/v2): `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`, `CriticalSectionAsync` (строки ~61-63):
  ```csharp
  // было:
  var plan = new PlacementPlan([new NodePlacement("shard1", "n1", "h1")]);
  var allocated = PortAllocator.Allocate(
      plan, new Dictionary<string, NodeAddress>(), busy, 15000, 15100);
  // стало (NodePlacement уже 3-аргументный: Group/Node/Host; Allocate + 4 делегата):
  var plan = new PlacementPlan([new NodePlacement("shard1", "n1", "h1")]);
  var allocated = PortAllocator.Allocate(
      plan, new Dictionary<string, NodeAddress>(), busy, 15000, 15100,
      PgPlanning.PortsOf, PgPlanning.HostOf, PgPlanning.MakeAddress, PgPlanning.KeyOf);
  ```
  (`PgPlanning` доступен: PgWorker.IntegrationTests ссылается на PgWorker.Core; резолв — через глобальный using `Shared.Core.Planning` для моделей и живой namespace `PgWorker.Core.Planning` для хелпера).

**Выход:** Pg-процессы, Race-тест и юниты планирования на обобщённых планировщиках; Pg-копии (прод + тесты) удалены; Docker/Integration-сборки резолвят Planning-типы.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный (сборка интеграционных проектов входит в slnx — прямой гейт); `grep -rn "class PlacementPlannerTests\|class PortAllocatorTests" src/tests` → единственные hits в `Shared.Core.UnitTests`; `dotnet test src/tests/Shared.Core.UnitTests src/tests/PgWorker.UnitTests -c Release` — зелёные.

**Spec:** §4.4 (Pg-инстанс), §6 (перенос Pg-юнитов планирования), §10 фаза D, §3.5.

### Задача D4: Kfw-адаптация (KfwPlanning + 3 процесса + Race-тест Kfw + using-листы)

**Вход:** D3 зелёная.

**Действие:**
- [ ] Удалить `src/KafkaWorker.Core/Planning/PlacementPlanner.cs` и `src/KafkaWorker.Core/Planning/PortAllocator.cs` (HostInfo-дубль в них уходит).
- [ ] Создать `src/KafkaWorker.Core/Planning/KfwPlanning.cs` (namespace `KafkaWorker.Core.Planning`):
  ```csharp
  using KafkaWorker.Core.Model;
  using Shared.Core.Planning;

  /// <summary>Kfw-инстанс обобщённых планировщиков (t09): одна группа на кластер,
  /// адрес = один client-порт, ключ результата — имя ноды.</summary>
  public static class KfwPlanning
  {
      public static IReadOnlyList<NodeGroup> Group(string cluster, IReadOnlyList<string> nodes)
          => [new(cluster, nodes)];

      public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.ClientPort];

      public static string HostOf(NodeAddress a) => a.Host;

      public static NodeAddress MakeAddress(string host, int port) => new(host, port);

      public static string KeyOf(NodePlacement p) => p.Node;
  }
  ```
- [ ] `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` (~194-195): `Plan(wanted, hosts.Value)` → `Plan(KfwPlanning.Group(cluster, wanted), hosts.Value)`; `Allocate(plan, existing, busy, options.PortFrom, options.PortTo)` → + `KfwPlanning.PortsOf, KfwPlanning.HostOf, KfwPlanning.MakeAddress, KfwPlanning.KeyOf`.
- [ ] `src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs` (~143, ~149): `Plan(missing, …)` → `Plan(KfwPlanning.Group(cluster, missing), …)`; Allocate — с делегатами.
- [ ] `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs` (~162-163): `Plan([broker], …)` → `Plan(KfwPlanning.Group(cluster, [broker]), …)`; Allocate — с делегатами.
- [ ] **Глобальные using `<Using Include="Shared.Core.Planning"/>`** (ревью №1/v2):
  - `src/KafkaWorker.Core/KafkaWorker.Core.csproj`, `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj`, `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj` (проверить `NodeRegenPlanner`/`NodeRegenPlannerTests` на использование `HostInfo` — резолв уйдёт в Shared);
  - **`src/KafkaWorker.Docker/KafkaWorker.Docker.csproj`** — `Drivers/ClusterDriver.cs`, `Engine/{DockerEngine.cs, IDockerEngine.cs}` (все три имеют `using KafkaWorker.Core.Planning;`);
  - **`src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`** — `Kafka/NodeRegenTests.cs`, `Etcd/PortAllocLockRaceTests.cs`.
- [ ] **Чистка локальных using вручную** (ревью №1/v2): `grep -rln "using KafkaWorker.Core.Planning" src/KafkaWorker.Docker src/tests/KafkaWorker.IntegrationTests` — удалить осиротевшие (namespace жив — KfwPlanning/NodeRegenPlanner; CS0246 не подсветит).
- [ ] **Race-тест Kfw** (ревью №2/v2): `src/tests/KafkaWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`, `CriticalSectionAsync` (~строки 61-64):
  ```csharp
  // было (2-аргументный NodePlacement доменной копии):
  var plan = new PlacementPlan([new NodePlacement("broker1", "h1")]);
  var allocated = PortAllocator.Allocate(
      plan, new Dictionary<string, NodeAddress>(), busy, 16000, 16100);
  // стало (3-аргументный: Group/Node/Host — группа = cluster из параметра метода;
  // Allocate + 4 делегата):
  var plan = new PlacementPlan([new NodePlacement(cluster, "broker1", "h1")]);
  var allocated = PortAllocator.Allocate(
      plan, new Dictionary<string, NodeAddress>(), busy, 16000, 16100,
      KfwPlanning.PortsOf, KfwPlanning.HostOf, KfwPlanning.MakeAddress, KfwPlanning.KeyOf);
  ```

**Выход:** Kfw-процессы и Race-тест на обобщённых планировщиках; Kfw-копии удалены; Docker/Integration-сборки резолвят Planning-типы.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — зелёный; `dotnet test src/tests/KafkaWorker.UnitTests -c Release` — зелёный (planner-юнитов у Kfw нет — переключений не требуется).

**Spec:** §4.4 (Kfw-инстанс: одна группа, один порт), §3.5.

### Задача D5: зеркальные Kfw-кейсы планирования в Shared-тестах

**Вход:** D3/D4 зелёные (Shared-тесты с Pg-кейсами созданы в D3).

**Действие:**
- [ ] Дополнить `src/tests/Shared.Core.UnitTests/Planning/PlacementPlannerTests.cs` (создан в D3) зеркальным Kfw-кейсом:
  ```csharp
  [Fact]
  public void Plan_SingleGroup_ClusterNodes_SpreadAcrossHosts()
  {
      // Arrange: одна группа (Kfw-инстанс: кластер) из 3 нод, 3 хоста
      var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0), new("h3", 0) };
      var groups = new List<NodeGroup> { new("c1", ["b1", "b2", "b3"]) };

      // Act
      var plan = PlacementPlanner.Plan(groups, hosts);

      // Assert: анти-аффинити внутри группы; Group отражён в каждой записи
      plan.Nodes.Select(n => n.Host).Should().OnlyHaveUniqueItems();
      plan.Nodes.Should().OnlyContain(n => n.Group == "c1");
  }
  ```
- [ ] Дополнить `src/tests/Shared.Core.UnitTests/Planning/PortAllocatorTests.cs` (создан в D3) зеркальными Kfw-кейсами (generic-инстанс с одним портом):
  ```csharp
  private sealed record KfwAddr(string Host, int Port); // локальная модель вместо доменной

  [Fact]
  public void Allocate_SinglePort_NewNode_GetsFirstFree()
  {
      // Arrange: одна нода, один client-порт (Kfw-инстанс)
      var plan = new PlacementPlan([new NodePlacement("c1", "b1", "h1")]);

      // Act
      var result = PortAllocator.Allocate(plan, new Dictionary<string, KfwAddr>(),
          new HashSet<(string, int)>(), 16000, 17000,
          a => [a.Port], a => a.Host, (h, p) => new KfwAddr(h, p), p => p.Node);

      // Assert: первый свободный порт диапазона; ключ — имя ноды
      result.Value["b1"].Port.Should().Be(16000);
  }

  [Fact]
  public void Allocate_SinglePort_PinnedReused_AndRangeExhausted_Fails()
  {
      // Arrange: закрепление b1→(h1,16000) переиспользуется; диапазон [16000,16001)
      var plan = new PlacementPlan([
          new NodePlacement("c1", "b1", "h1"),
          new NodePlacement("c1", "b2", "h1")]);
      var existing = new Dictionary<string, KfwAddr> { ["b1"] = new("h1", 16000) };

      // Act
      var result = PortAllocator.Allocate(plan, existing,
          new HashSet<(string, int)>>(), 16000, 16001,
          a => [a.Port], a => a.Host, (h, p) => new KfwAddr(h, p), p => p.Node);

      // Assert: pinned без изменений; b2 — свободного порта нет
      result.IsSuccess.Should().BeFalse();
      result.Error.Should().NotBeNull();
  }
  ```

**Выход:** планирование покрыто юнитами обоих инстансов в Shared (Pg-кейсы — D3, зеркальные Kfw — здесь).

**Проверка:** `dotnet test src/tests/Shared.Core.UnitTests src/tests/PgWorker.UnitTests src/tests/KafkaWorker.UnitTests -c Release` — зелёные.

**Spec:** §6 (зеркальные Kfw-кейсы), §4.4.

### Задача D6: коммит фазы D

- [ ] `git add -A && git commit -m "refactor(planning): t09 — обобщённые PlacementPlanner/PortAllocator в Shared.Core.Planning; Pg-юниты мигрированы, Kfw-зеркало добавлено; Race-тесты на generic-сигнатуре"`

---

## Фаза E — чистка

### Задача E1: grep-гейты остатков и ревизия

**Вход:** фазы B–D закоммичены.

**Действие:**
- [ ] Гейт определений (spec §12.1): `grep -rn "class ClaimStore\|class PortAllocLock\|class WorkJournal\|class SnapshotJob\|record PlanPut\|record ValidationError\|class PlacementPlanner\|class PortAllocator" src/` → определения ТОЛЬКО в `src/Shared.*` (+ допустим `EvacuationJournalStore` в PgWorker.Etcd).
- [ ] Гейт тестовых дублей: `grep -rn "class PortAllocLockTests" src/tests` → единственный hit `src/tests/Shared.Etcd.UnitTests`; `grep -rn "class PlacementPlannerTests\|class PortAllocatorTests" src/tests` → единственные hits в `src/tests/Shared.Core.UnitTests`.
- [ ] Гейт осиротевших namespace: `grep -rn "PgWorker.Etcd.Coordination\|KafkaWorker.Etcd.Coordination" src/ --include="*.cs"` → только Pg-файлы эвакуаций; `grep -rn "PgWorker.Provisioning.Snapshots" src/` → пусто.
- [ ] Гейт статических обращений к инстанс-членам: `grep -rn "PortAllocLock\.Key" src/ --include="*.cs"` → пусто (CS0120-класс закрыт в B2/B4).
- [ ] Гейт старых вызовов: `grep -rn "\.WriteAsync(" src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests --include="*.cs" | grep -v WritePhase | grep -v WriteSupervision` → пусто; `grep -rn "WriteEvacuationAsync\|ReadEvacuationAsync" src/` → пусто; `grep -rn "new PortLockBusyException()" src/` → пусто.
- [ ] Гейт безпрефиксных ctor-вызовов координации (явные): `grep -rn "new ClaimStore(\[\|new ClaimStore(\"http\|new WorkJournal(\[\|new WorkJournal(\"http\|new PortAllocLock(\[\|new PortAllocLock(\"http" src/ --include="*.cs"` → пусто.
- [ ] Гейт безпрефиксных ctor-вызовов координации (target-typed, толерантный к `=> new(`): `grep -rnE "(ClaimStore|WorkJournal|PortAllocLock)[^;]*(=|=>) *new\(\[" src/tests` → пусто.
- [ ] Гейт fully-qualified ссылок: `grep -rn "KafkaWorker.Etcd.Coordination\|PgWorker.Etcd.Coordination.ClaimStore\|PgWorker.Etcd.Coordination.PortAllocLock\|PgWorker.Etcd.Coordination.WorkJournal" src/ --include="*.cs"` → только Pg-файлы эвакуаций; `grep -rn "Core.Writing.ValidationError" src/` → пусто.
- [ ] Ревизия using в затронутых тестах (`dotnet build` подсвечивает CS0246; ручная чистка — для живых namespace по задачам B2/B3/B4/C1/D3/D4).
- [ ] Сверка slnx: `git diff src/PgWorker.slnx` → пусто (проекты не менялись).

**Выход:** дублируемость устранена доказуемо.

**Проверка:** все grep-гейты зелёные; `dotnet build src/PgWorker.slnx -c Release` — 0 warnings.

**Spec:** §10 фаза E, §12.1.

### Задача E2: коммит фазы E

- [ ] При наличии правок: `git add -A && git commit -m "chore(t09): чистка using и остатков дублей после унификации"`; иначе — пропустить.

---

## Фаза F — верификация и гейты

Порядок серий обязателен; между КАЖДОЙ серией — зачистка docker (контейнеры/сети: `docker ps -aq --filter name=pgw- -q | xargs -r docker rm -f; docker network prune -f` + сверка `docker network ls | grep -c kfw-net` при нуле контейнеров = 0) и ожидание финальной строки прогона. Упавший сценарий — не перезапускать без анализа логов (docs/e2e-launch.md).

### Задача F1: сборка Release

- [ ] `dotnet build src/PgWorker.slnx -c Release` — 0 warnings, 0 errors. **Spec §12.2.**

### Задача F2: юнит-серии (с зачисткой между)

- [ ] `dotnet test src/tests/Shared.Core.UnitTests -c Release` → зелёный.
- [ ] Зачистка docker; `dotnet test src/tests/Shared.Etcd.UnitTests -c Release` → зелёный.
- [ ] Зачистка; `dotnet test src/tests/Shared.Metrics.UnitTests src/tests/AdminPanel.UnitTests -c Release` → зелёные.
- [ ] Зачистка; `dotnet test src/tests/PgWorker.UnitTests -c Release` → зелёный.
- [ ] Зачистка; `dotnet test src/tests/KafkaWorker.UnitTests -c Release` → зелёный.
**Spec §12.3 (юниты).**

### Задача F3: интеграционные серии (docker)

- [ ] Зачистка; `PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release` (вкл. EtcdCoordinationTests, PortAllocLockRaceTests с generic-Allocate — ассерты на фактические пути ключей `/pgworker/locks/portalloc` и т.п. проходят без правки ожиданий) → зелёный.
- [ ] Зачистка; `dotnet test src/tests/KafkaWorker.IntegrationTests -c Release` → зелёный (ClaimStoreTests, PortAllocLockRaceTests — префикс `/kafkaworker`; вся Kafka-серия на KafkaClusterFixture-риге с префикс-ctors).
- [ ] Зачистка; дым AdminPanel: `dotnet test src/tests/AdminPanel.IntegrationTests -c Release` → зелёный (панель не тронута, читает `/pgworker/work/*` — парсер JSON не изменился).
**Spec §10 F.3, §12.3-4.**

### Задача F4: E2E-гейт — полный docker-E2E на свежем Release

**Вход:** F1–F3 зелёные. Тронуты `PgWorker.Core`/`PgWorker.Etcd`/`PgWorker.Provisioning` и portalloc → минимум-маркера НЕдостаточно, прогоняется полный E2eFixture.

- [ ] Зачистка; полный прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release` (E2eFixture сам собирает свежий Release; инкрементальный no-op). Наблюдение прогона по правилам телеметрии: фазы >60 с — `[PHASE]`-строки; упавший сценарий — `MarkFailed()`, teardown останавливает без удаления, разбор по `/tmp/pgw-e2e-artifacts-<guid>/`, перезапуск только после анализа.
- [ ] Отдельно подтвердить присутствие кейс-маркера в полном прогоне: `Scale_AddEmptyShard` зелёный.
**Spec §10 F.4, §12.3; AGENTS.md E2E-гейт.**

### Задача F5: docker-образы и compose

- [ ] `docker build -f docker/PgWorker.Dockerfile -t pgworker:dev .` → успешно.
- [ ] `docker build -f docker/KafkaWorker.Dockerfile -t kafkaworker:dev .` → успешно.
- [ ] `docker compose -f deploy/docker-compose.yml config -q` → валиден; `git diff deploy/ dev-stand/` → пусто.
**Spec §10 F.5, §12.6.**

### Задача F6: финальная сверка критериев приёмки

- [ ] Пройти чек-лист §12 spec по пунктам 1–7 (grep-гейты — E1; сборка F1; серии F2–F4; контракт — ассерты интеграций без правки ожиданий; поведение — только §7, отражено в arch/16 фазой A; образы F5; roadmap — на мерже).
- [ ] `git add -A && git commit -m "test(t09): полная верификация — build 0 warnings, юниты/интеграции/E2E зелёные, образы собираются"` (если остались незакоммиченные артефакты).

---

## Мерж-гейт (при переходе в `main`, по отдельному указанию пользователя)

Тем же мерж-коммитом в `main`:
- удалить пункт `t09-unify-worker-duplicates` из `arch/roadmap/pgworker.md` (вкл. `←`-зависимости, если упомянут);
- история задачи остаётся в `docs/superpowers/2026-09-15-t09-unify-worker-duplicates/`.

**Spec §11.**

---

## Self-review плана v6 (выполнен составителем после правки по пятому ревью Фазы 4)

- **Замечание пятого ревью — закрыто (единственное):**
  - **№1 (high, семейство ассертов `PortAllocLock.Key`):** правка статических обращений перенесена из B5 в момент переключения на Shared: B2 получил пункт «Статические `PortAllocLock.Key` → инстанс-ссылки, Pg-файлы» — Race (~122, ~146 → `first.Key`, переменная в скоупе кейсов подтверждена по коду) и юнит `Etcd/PortAllocLockTests.cs` (~35 → `first.Key`, ~56 → `second.Key`, ~78 → `locks.Key`, ~114 Seed → `mine.Key`, ~120 → `mine.Key`; фейк `Fakes.FakeEtcd` не меняется — `.Store[…].Value` валиден до удаления в B5); B4 — зеркальный пункт для Kfw-файлов (Race ~131, ~155 → `first.Key`; юнит ~36/~57/~79/~114/~120 → те же переменные, копии зеркальны — подтверждено по коду). Гейты B2/B4 дополнены контролем `grep -rn "PortAllocLock\.Key" src/tests/…` → пусто; тот же гейт добавлен в E1. Пункт B5 «Интеграционные правки» сокращён до контроля переноса (инстанс-`Key` и префикс-ctors уже сделаны в B2/B4; Allocate Race-тестов — D3/D4); в B5-описании PortAllocLockTests-вливания учтено, что статические `Key` уже переведены. В Global Constraints правило «файл, удаляемый позже, компилируется до удаления» расширено статическими членами (CS0120).
- **Покрытие spec (без изменений с v5):** Д1–Д8 → задачи B1–B5 (Д1–Д4), C1 (Д5–Д6), D1–D5 (Д7–Д8); arch-first → A1–A2; чистка → E1; верификация → F1–F6; мерж-гейт → финальная секция. Тестовые переносы §6 → B5 (вкл. вливание обеих копий PortAllocLockTests и канон фейка FakeCoordinationGateway из CoordinationTests), D3 (Pg-юниты планирования), D5 (зеркальные Kfw-кейсы). Риски §9 закрыты: трек — B4/B5 + процессные тесты B2/B4 + F3/F4; txn/lease — B5 + Race ×2 (префикс и инстанс-Key — B2/B4, generic-Allocate — D3/D4); опечатка префикса — литерал-константа в Program.cs + ассерты интеграций; планирование — D3/D5 + E2E F4; using — build после каждой задачи + ручная чистка живых namespace (B3/D3/D4); осколки — E1; образы — F5; панель — JSON WorkState не меняется, F3 дым.
- **Отступления от буквы spec (зафиксированы в Global Constraints):** параметры `keyOf` и `hostOf` у Allocate (ключ и хост — у потребителя/домена, буква §4.4 «строки у потребителя», «доменные модели остаются у воркеров»); возврат `Result<IReadOnlyDictionary<…>>` вместо `Dictionary` (текущие сигнатуры процессов возвращают IReadOnlyDictionary — минимальный diff).
- **Типы-консистентность:** `NodeGroup(Name, Nodes)` / `NodePlacement(Group, Node, Host)` / `PlacementPlan(Nodes)` едины в D1–D5; делегаты `portsOf/hostOf/makeAddress/keyOf` — один порядок во всех вызовах D3/D4/D5 (вкл. Race-тесты); префикс — первый аргумент ctor во всех точках B2/B4/B5 (прод + юниты + интеграции, явные и target-typed); `WriteSupervisionAsync` — 5-аргументная сигнатура без default во всех вызовах B2 (NodeSupervisor.cs, 4 тестовые точки); `PortAllocLock.Key` — только инстанс-обращения с B2/B4.
