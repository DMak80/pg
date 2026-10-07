# t12-loop-watchdog — план доработки: heartbeat-семантика watchdog (поверх Tasks 1–11)

> **Для agentic-воркеров:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — выполнять план задача-за-задачей.
> Шаги используют синтаксис чекбоксов (`- [ ]`) для отслеживания.

**Цель доработки:** после правки спеки (гейт пройден 2026-10-07) firing watchdog —
по отсутствию **АКТИВНОСТИ** цикла (тик ИЛИ прогресс-отметка), а не по «нет тика»:
легитимно длинная итерация reconcile (бутстрап нод с pg_basebackup ~41 с, cutover
переездов до `CutoverTimeoutSec=90 с`) отмечает живость прогресс-отметками и
рестарта не даёт; пороги и формулы `LoopStaleness` НЕ меняются. Tasks 1–11
исходного плана УЖЕ реализованы и закоммичены (12 коммитов, HEAD `ebdac99`) —
этот план содержит только доработки поверх существующего кода.

**Спека:** [`spec.md`](spec.md) — переписана под heartbeat (в рабочем дереве,
одобрена пользователем; закоммитить первым шагом). Исполнители читают спеку и
этот план; решения Q1–Q5 и heartbeat-решения шапки спеки не пересматриваются.

## Карта «уже сделано — НЕ переделывать» (Tasks 1–11, в ветке)

| Механизм | Где (готово) |
|---|---|
| Формулы порогов | `src/Shared.Core/HealthChecks/LoopStaleness.cs` (`FastLoops`/`SnapshotLoop`) + `LoopStalenessTests` |
| Watchdog-компонент | `src/Shared.Core/Hosting/LoopWatchdog.cs` — `LoopHeartbeat(Name, LastTickAt, StaleAfter)` (переименовать поле — Task B), `ILoopsVitality`, `WatchdogOptions`, firing/grace/`StopApplication`; `LoopWatchdogServiceCollectionExtensions.AddLoopWatchdog` |
| Метрика | `WorkerMetricsInstrumentation.WatchdogRestart(loop)` → counter `worker_watchdog_restarts_total{loop}` |
| Тики sweeper | `BackupOrphanSweeperLoop` → `HealthState.MarkOrphanSweepTick` + `HealthSnapshot.LastOrphanSweepTick`, loops-alive PgWorker |
| healthz | трое `*WorkerHealth` на хелпере `LoopStaleness`, секция `data["watchdog"]` (`disabled`/`starting`/`armed; stale=<loop\|нет>`) |
| Витальность | `src/{PgWorker,KafkaWorker,ValkeyWorker}.App/LoopsVitality.cs` (4/3/3 цикла) + подключения `AddLoopWatchdog` в `Program.cs` |
| Конфигурация | `LoopsOptions.Watchdog` + appsettings трёх воркеров |
| Тесты | юниты `Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs`, per-app `HealthTests`/`LoopsVitalityTests`/`BackupOrphanSweeperLoopTests`; интеграционные `PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs` ( FakeVitality(bool stale)) + `MetricsTests` |
| Документация | arch/14 §6–8, arch/16, arch/21, arch/18 §2.2 (строка серии), `docs/runbook.md` (раздел watchdog) |
| Roadmap-гейт | правки УЖЕ в рабочем дереве: `arch/roadmap/reliability.md` (пункт t12 снят), `reliability-report.md` (перенос в «Сделано», сводка R) — закоммитить в Task I после AC3 |

**Граница доработки (буква замечаний, ничего сверх):** семантика firing по
активности; поле активности в `HealthState`; `ILoopProgress`; точки §4.6 спеки;
обновление тестов под новую семантику; точечные правки формулировок arch/runbook;
`MarkFailed`-обёртка E2eScaleScenarios; корректная команда проверки AC3; коммиты
по задачам. Пороги/формулы/метрики/HTTP-статусы healthz/loops-alive (по тикам) —
НЕ меняются.

## Глобальные ограничения

- Вся работа — ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/t12-loop-watchdog`
  (ветка t12-loop-watchdog); коммиты в feature-ветке свободны, мерж в main — по
  отдельному приказу пользователя.
- `TreatWarningsAsErrors=true` — каждая задача завершается зелёной сборкой
  (`dotnet build`/`dotnet test` 0 warnings); шаги-задачи последовательны, сборка
  зелёная после КАЖДОЙ.
- Комментарии и документация — на русском; идентификаторы — на английском; тесты —
  комментарии по нотации AAA.
- Тесты: никаких хардкодов хост-портов; каждое отдельное ожидание/поллинг ≤30 с
  (AGENTS.base.md §12); docker-контейнеры поднимает только финальный E2E-маркер
  фикстурой (каноны `docs/e2e-isolation.md`/`docs/e2e-launch.md`).
- Watchdog не делает сетевых вызовов и не пишет в etcd; прогресс-отметка —
  lock-free-запись в `HealthState` (микросекунды), наблюдатель не влияет на процесс.
- Поведение healthz бит-в-бит: loops-alive считается ПО ТИКАМ, поле активности в
  healthz НЕ читается (спека §4.6).
- Метрики — пассивные наблюдатели: марк-методы никогда не бросают исключений.
- В документах (arch/, docs/, runbook) — только текущее/планируемое состояние,
  без пометок задачи (история — в git и docs/superpowers/).
- Локально собираемые образы в registry `192.168.0.1:5000` не кладём (E2E-образы
  собирает фикстура сама).
- `PgWorker.Moves`/`PgWorker.Provisioning`/`ValkeyWorker.Provisioning` имеют
  транзитивную ссылку на `Shared.Core` (через `PgWorker.Core`/`ValkeyWorker.Core`)
  — `ILoopProgress` доступен без новых ProjectReference (проверено).

## Карта файлов доработки

| Файл | Действие |
|---|---|
| `docs/superpowers/2026-10-07-t12-loop-watchdog/{spec.md,plan.md}` | коммит переписанной спеки + этого плана (Шаг 0) |
| `arch/14-pgworker.md`, `arch/16-kafkaworker.md`, `arch/21-valkeyworker.md`, `arch/18-metrics.md`, `docs/runbook.md` | формулировки «не тикал» → «не проявлял активности (тик или прогресс-отметка)» (Task A) |
| `src/Shared.Core/Hosting/LoopWatchdog.cs` | `LoopHeartbeat.LastTickAt` → `LastActivityAt`; firing/grace по активности; журнал «не проявлял активности» (Task B) |
| `src/Shared.Core/Hosting/ILoopProgress.cs` (create) | `ILoopProgress { void Mark(); }` (Task B) |
| `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs` | кейсы под активность (Task B) |
| `src/{PgWorker,KafkaWorker,ValkeyWorker}.App/HealthState.cs` | `MarkReconcileActivity` + `LastReconcileActivity` + тик обновляет активность; Pg/Valkey — `: ILoopProgress` (Task C) |
| `src/{PgWorker,KafkaWorker,ValkeyWorker}.App/LoopsVitality.cs` | reconcile → `snap.LastReconcileActivity` (Task C) |
| `src/tests/{PgWorker,KafkaWorker,ValkeyWorker}.UnitTests/App/LoopsVitalityTests.cs` | `.LastTickAt` → `.LastActivityAt` + кейсы активности (Tasks B/C) |
| `src/PgWorker.App/Loops/ReconcileLoop.cs`; `src/PgWorker.App/Program.cs` | старт-итерация + регистрация `ILoopProgress` (Task D) |
| `src/PgWorker.Provisioning/Processes/{ProvisioningProcess,AddShardProcess}.cs` | отметки после `EnsureNodeAsync`/проб (`IdentifyAsync`/`IsAliveAsync`) (Task D) |
| `src/PgWorker.Moves/Process/{MoveProcess,CutoverSequence,AbortSequence}.cs` | отметки в поллинг-циклах (заморозка/слот/дезактивация) (Task D) |
| `src/ValkeyWorker.App/Loops/ReconcileLoop.cs`; `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs`; `src/ValkeyWorker.App/Program.cs` | старт-итерация + контейнеры + PING-цикл `AwaitBootAsync` + регистрация (Task E) |
| `src/KafkaWorker.App/Loops/ReconcileLoop.cs` | старт-итерация — единственная точка (Task F) |
| `src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs` | FakeVitality под активность + кейс «долгая фаза с отметками» (Task G) |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs` | try/catch + `Fx.MarkFailed()` оба сценария (Task H) |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | roadmap-гейт: коммит готовых правок (Task I) |

Порядок зависимостей: Шаг 0 → A (arch-first) → B (Shared.Core) → C (HealthState/
vitality) → D/E/F (точки: PgWorker → Valkey → Kafka) → G (интеграция) → H (E2E-
обёртка) → I (мерж-гейт: AC3 + roadmap). D/E/F зависят от C; G — от D; I — последний.

---

## Шаг 0: коммит спеки и плана

- [ ] **0.1** Убедиться, что в рабочем дереве только `docs/superpowers/2026-10-07-t12-loop-watchdog/spec.md` (переписан под heartbeat), `plan.md` (этот файл) и правки `arch/roadmap/*` (гейт Task I, НЕ коммитить сейчас): `git status --short`.
- [ ] **0.2** Закоммитить спеку+план:

```bash
git add docs/superpowers/2026-10-07-t12-loop-watchdog/spec.md docs/superpowers/2026-10-07-t12-loop-watchdog/plan.md
git commit -m "docs(t12): спека+план под heartbeat-семантику watchdog — firing по отсутствию активности (тик или прогресс-отметка), ILoopProgress и точки §4.6; план переведён в режим доработки поверх Tasks 1-11"
```

---

## Task A: arch/runbook — формулировки активности (arch-first, только документация)

**Связь со spec:** §4.1 (канон в arch), §4.5 (строка журнала), §4.8 (runbook); замечание 6.

**Вход:** закоммиченные arch/14 §6–8, arch/16 §6–8, arch/21 §6–8, arch/18 §2.2,
`docs/runbook.md` — формулируют watchdog через «возраст тиков»/«не тикал».

**Действие** (точечные правки, без перестройки разделов; текущее/планируемое
состояние, без пометок задачи):

- [ ] **A.1** `arch/14-pgworker.md` §6, пункт «Watchdog зависших циклов»:
  - «следит за возрастом тиков всех фоновых циклов … по отметкам `HealthState`» →
    «следит за возрастом **активности** всех фоновых циклов … по отметкам
    `HealthState` (активность = тик или прогресс-отметка; долгие фазы итерации —
    создание контейнеров нод, ожидание готовности нод, поллинг-циклы переездов —
    отмечают живость)»;
  - «Отметка `null` («цикл ещё не тикал») не firing» → «Отметка `null` («цикл ещё
    не проявлял активности») не firing».
- [ ] **A.2** `arch/16-kafkaworker.md` §6, пункт «Watchdog зависших циклов»:
  симметричная замена «возраст тиков» → «возраст активности (тик или
  прогресс-отметка; глубоких долгих фаз у KafkaWorker нет — ожидания расползаются
  по тикам)»; «null («цикл ещё не тикал»)» → «null («цикл ещё не проявлял
  активности»)».
- [ ] **A.3** `arch/21-valkeyworker.md` §6: то же, в перечне долгих фаз — «создание
  контейнеров нод, PING-цикл ожидания готовности (бюджет `NodeBootSec`)».
- [ ] **A.4** `arch/18-metrics.md` §2.2, строка `worker_watchdog_restarts_total`:
  «остановки цикла по staleness» → «остановки цикла по отсутствию активности
  (тик или прогресс-отметка)».
- [ ] **A.5** `docs/runbook.md`, раздел «Watchdog зависших циклов воркера»:
  - «следит за возрастом тиков фоновых циклов» → «следит за возрастом активности
    фоновых циклов (активность = тик или прогресс-отметка долгой фазы: бутстрап
    нод, cutover переездов, PING-циклы готовности)»;
  - «`watchdog: цикл <loop> не тикал N c (порог M c)`» → «`watchdog: цикл <loop>
    не проявлял активности N c (порог M c) — инициирован self-restart`»;
  - «причины зависания тика» → «причины зависания итерации»;
  - «легитимно-длинных тиков» → «легитимно-длинных итераций (закрываются
    прогресс-отметками, не окном порога)»;
  - «та же точка самовосстановления, но по staleness тиков» → «…по отсутствию
    активности цикла (тик или прогресс-отметка)».

**Выход:** arch/14/16/18/21 и runbook формулируют watchdog через активность.

**Проверка:**
- [ ] **A.6** `grep -n "не тикал\|возрастом тиков\|staleness тиков" arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md arch/18-metrics.md docs/runbook.md` — пусто (п. R10 arch/14 про скриптовый переезд НЕ трогать: там «не тикает updated_unix» — другой контекст, фильтровать глазами; допускаются только такие нерелевантные вхождения).
- [ ] **A.7** Коммит:

```bash
git add arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md arch/18-metrics.md docs/runbook.md
git commit -m "docs(t12): arch/14/16/18/21 + runbook — firing watchdog по отсутствию активности (тик или прогресс-отметка), формулировки долгих фаз (spec §4.1/§4.5)"
```

---

## Task B: Shared.Core — LastActivityAt, heartbeat-семантика, ILoopProgress

**Связь со spec:** §4.2 (LoopHeartbeat/`ILoopsVitality`/firing/grace), §4.5
(строка журнала), §4.6 (`ILoopProgress`); замечания 1, 3.

**Вход:** `src/Shared.Core/Hosting/LoopWatchdog.cs` (field `LastTickAt`, firing по
тику, журнал «не тикал»); юнит-кейсы `StaleTick_*`/`FreshTicks_*`/`NullTick_*`;
потребители `.LastTickAt` — только юниты `LoopsVitalityTests` трёх воркеров
(проверено grep: других именованных использованием нет, конструкторы
позиционные).

**Действие:**

- [ ] **B.1** `LoopHeartbeat`: переименовать поле `LastTickAt` → `LastActivityAt`
  (тип/позиция те же), XML-комментарий: «момент последней активности цикла: тик
  ИЛИ прогресс-отметка (healthz loops-alive — не консюмерер, он читает
  HealthSnapshot по тикам напрямую)».
- [ ] **B.2** `LoopWatchdog.ExecuteAsync`: читать `heartbeat.LastActivityAt`;
  обновить комментарии: firing — «отсутствие активности (ни тика, ни
  прогресс-отметки) за порог»; grace — «цикл ещё не проявлял активности».
  Логика сравнения/порогов НЕ меняется (формулы и множители прежние).
- [ ] **B.3** `FireAndStopAsync`: строка журнала →
  `"watchdog: цикл {Loop} не проявлял активности {Age:F0} c (порог {Threshold:F0} c) — инициирован self-restart"`
  (спека §4.5; маркер grep для AC3 — «инициирован self-restart» — сохраняется).
- [ ] **B.4** Создать `src/Shared.Core/Hosting/ILoopProgress.cs`:

```csharp
namespace Shared.Core.Hosting;

/// <summary>Узкий интерфейс доставки прогресс-отметок долгих фаз reconcile в
/// глубину процессов (Provisioning/Moves ниже App и HealthState не видят):
/// реализация — HealthState воркера (Mark() = MarkReconcileActivity),
/// регистрируется app; вызовы — микросекунды, без исключений.</summary>
public interface ILoopProgress
{
    /// <summary>Прогресс-отметка: итерация жива (активность без тика).</summary>
    void Mark();
}
```

- [ ] **B.5** Юнит-тесты `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs`
  — обновить под семантику (тела кейсов не меняются, меняются имена/комментарии;
  `LoopStalenessTests` и DI-кейсы `AddLoopWatchdog_*` не трогать):
  - `StaleTick_FiresOnce_LogsCritical_AndMarksMetric` →
    `StaleActivity_FiresOnce_LogsCritical_AndMarksMetric` (комментарий: «активности
    нет ни тиком, ни отметкой — возраст LastActivityAt превысил порог»; ассерт
    журнала — сообщение содержит «не проявлял активности»);
  - `FreshTicks_DoNotStop` → `FreshActivity_EvenLongIteration_DoesNotStop`
    (комментарий: «прогресс-отметки внутри порога — долгая, но живая итерация;
    тик может быть давним»);
  - `NullTick_InGraceWindow_DoesNotFire` / `NullTick_AfterGrace_Fires` →
    `NullActivity_InGraceWindow_DoesNotFire` / `NullActivity_AfterGrace_Fires`
    (grace по активности: «цикл ещё не проявлял активности»).
- [ ] **B.6** Механическая правка потребителей (компиляция): в трёх
  `src/tests/{PgWorker,KafkaWorker,ValkeyWorker}.UnitTests/App/LoopsVitalityTests.cs`
  переименовать `.LastTickAt` → `.LastActivityAt` (семантика пока «активность =
  тик» — vitality ещё передают тик; меняется в Task C).

**Выход:** `LoopHeartbeat.LastActivityAt`, firing/grace по активности, журнал
«не проявлял активности», `Shared.Core.Hosting.ILoopProgress`.

**Проверка:**
- [ ] **B.7** `dotnet test src/tests/Shared.Core.UnitTests -c Debug` → PASS, 0 warnings;
  `dotnet build src/PgWorker.slnx -c Debug` → 0 errors / 0 warnings (сборка
  решения целиком — vitality-файлы ещё не переключены на новое поле снимка,
  но компилируются).
- [ ] **B.8** Коммит:

```bash
git add src/Shared.Core/Hosting/LoopWatchdog.cs src/Shared.Core/Hosting/ILoopProgress.cs src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs src/tests/PgWorker.UnitTests/App/LoopsVitalityTests.cs src/tests/KafkaWorker.UnitTests/App/LoopsVitalityTests.cs src/tests/ValkeyWorker.UnitTests/App/LoopsVitalityTests.cs
git commit -m "feat(t12): LoopHeartbeat.LastActivityAt — firing/grace watchdog по активности (тик или прогресс-отметка), журнал «не проявлял активности», интерфейс ILoopProgress (Shared.Core.Hosting) (spec §4.2/§4.5/§4.6)"
```

---

## Task C: HealthState трёх воркеров — поле активности + ILoopProgress-реализация

**Связь со spec:** §4.2 («активность — новое поле рядом с тиками»), §4.6
(механизм отметок: `MarkReconcileTick` обновляет и тик, и активность;
`MarkReconcileActivity` — только активность; реализации ILoopProgress — в
HealthState воркера); замечания 2, 3.

**Вход:** `src/{PgWorker,KafkaWorker,ValkeyWorker}.App/HealthState.cs` —
идентичные классы с `MarkReconcileTick`/`MarkKeepaliveTick`/`MarkSnapshotTick`
(у PgWorker ещё `MarkOrphanSweepTick`); `HealthSnapshot` — позиционный record
(прямых `new HealthSnapshot(` в тестах нет — проверено, добавление поля в конец
безопасно); vitality ×3 передают для reconcile `snap.LastReconcileTick`.

**Действие** (симметрично в трёх `HealthState.cs`):

- [ ] **C.1** Поле `private DateTimeOffset? _lastReconcileActivity;` рядом с
  `_lastReconcileTick`.
- [ ] **C.2** `MarkReconcileTick`: в существующем lock дополнительно
  `_lastReconcileActivity = clock.GetUtcNow();` (тик — тоже активность).
- [ ] **C.3** Метод:

```csharp
/// <summary>Прогресс-отметка reconcile (heartbeat долгих фаз итерации:
/// контейнеры нод, ожидание готовности, поллинг переездов) — активность без
/// тика; читает только watchdog, healthz loops-alive — по тикам.</summary>
public void MarkReconcileActivity()
{
    lock (_sync)
    {
        _lastReconcileActivity = clock.GetUtcNow();
    }
}
```

- [ ] **C.4** `HealthSnapshot`: добавить `DateTimeOffset? LastReconcileActivity`
  последним полем (у PgWorker — после `LastOrphanSweepTick`; у Kafka/Valkey —
  после `ClaimsHeld`); `Snapshot()` передаёт `_lastReconcileActivity` последним
  аргументом.
- [ ] **C.5** PgWorker и Valkey: `HealthState` реализует интерфейс —
  `public sealed class HealthState(TimeProvider clock) : Shared.Core.Hosting.ILoopProgress`
  и метод `public void Mark() => MarkReconcileActivity();` (XML: «ILoopProgress —
  доставка прогресс-отметок в процессы ниже App»). У KafkaWorker интерфейс НЕ
  реализовывать (глубоких точек нет — спека §4.6), поле/методы (C.1–C.4) —
  добавить: старт-итерации отмечает цикл напрямую.
- [ ] **C.6** Три `LoopsVitality.cs`: строка reconcile →
  `new LoopHeartbeat("reconcile", snap.LastReconcileActivity, fast)` + комментарий
  «активность = тик или прогресс-отметка; keepalive/snapshot/orphan-sweep —
  активность = тик (долгих фаз нет)» (остальные строки не меняются).
- [ ] **C.7** Юнит per-app — дописать в каждый `LoopsVitalityTests.cs` кейс:

```csharp
[Fact]
public void Snapshot_ReconcileActivity_UpdatedWithoutTick()
{
    // Arrange: долгая итерация — только прогресс-отметка, тика нет
    var health = new HealthState(TimeProvider.System);
    health.MarkReconcileActivity();

    // Act
    var beats = new PgWorkerLoopsVitality(Options, health).Snapshot(); // тип — свой воркер

    // Assert: активность свежая (watchdog не firing), тик остался null
    // (healthz loops-alive по тикам — Degraded, HTTP-семантика не меняется)
    beats.First(b => b.Name == "reconcile").LastActivityAt.Should().NotBeNull();
    health.Snapshot().LastReconcileTick.Should().BeNull();
    health.Snapshot().LastReconcileActivity.Should().NotBeNull();
}
```

и дополнить существующий кейс `Snapshot_PassesHealthStateTicks` ассертом:
`health.Snapshot().LastReconcileActivity.Should().NotBeNull();` с комментарием
«MarkReconcileTick обновляет и тик, и активность».

**Выход:** `HealthState.MarkReconcileActivity`/`HealthSnapshot.LastReconcileActivity`
у трёх воркеров; `MarkReconcileTick` обновляет оба; Pg/Valkey `HealthState :
ILoopProgress`; vitality отдают активность для reconcile.

**Проверка:**
- [ ] **C.8** `dotnet test src/tests/PgWorker.UnitTests src/tests/KafkaWorker.UnitTests src/tests/ValkeyWorker.UnitTests -c Debug` → PASS, 0 warnings
  (существующие `HealthTests` не меняются — healthz по тикам; сверьте: кейсы
  `MarkReconcileTick` остаются зелёными).
- [ ] **C.9** Коммит:

```bash
git add src/PgWorker.App/HealthState.cs src/KafkaWorker.App/HealthState.cs src/ValkeyWorker.App/HealthState.cs src/PgWorker.App/LoopsVitality.cs src/KafkaWorker.App/LoopsVitality.cs src/ValkeyWorker.App/LoopsVitality.cs src/tests/PgWorker.UnitTests/App/LoopsVitalityTests.cs src/tests/KafkaWorker.UnitTests/App/LoopsVitalityTests.cs src/tests/ValkeyWorker.UnitTests/App/LoopsVitalityTests.cs
git commit -m "feat(t12): HealthState трёх воркеров — MarkReconcileActivity/LastReconcileActivity (тик обновляет активность), Pg/Valkey реализуют ILoopProgress, vitality отдают активность reconcile (spec §4.2/§4.6)"
```

---

## Task D: PgWorker — heartbeat-точки (запуск контейнеров / готовность / moves)

**Связь со spec:** §4.6, таблица точек (строки PgWorker) + обоснование «CutoverSequence
легитимно ждёт до CutoverTimeoutSec=90 с»; замечание 4 (пп. PgWorker).

**Вход:** `ReconcileLoop.TickAsync` (отметка тика — в конце, строка ~148);
`ProvisioningProcess`/`AddShardProcess` (PgWorker.Provisioning) — `EnsureNodesAsync`
(после каждого `driver.EnsureNodeAsync`), `WaitPatroniAsync` (пробы:
`probe.IdentifyAsync` в ProvisioningProcess, `probe.IsAliveAsync` в
AddShardProcess); `MoveProcess` — `CutoverSequence` создаётся field-инициализатором
`new(sql, new MoveStatusStore(etcd, etcdEndpoints), secrets)`, `DropSlotAsync` —
цикл дезактивации 5×1 с, `RunAbortAsync` — `new AbortSequence(sql, status,
requests, journal, shards, secrets)`; фабрики в `src/PgWorker.App/Program.cs`
(`new ProvisioningProcess(...)` ~строка 322, `new AddShardProcess(...)` ~428,
`new MoveProcess(...)` ~463).

**Действие:**

- [ ] **D.1** Старт итерации reconcile: `src/PgWorker.App/Loops/ReconcileLoop.cs`,
  `TickAsync` — сразу после проверки `endpoints.Length` (до первого
  `RangeWithFailoverAsync(endpoints, "/clusters/", ct)`) добавить
  `health.MarkReconcileActivity(); // heartbeat: старт итерации (долгие фазы отмечаются глубже)`.
- [ ] **D.2** DI: в `src/PgWorker.App/Program.cs` рядом с регистрацией
  `HealthState` добавить:
  `builder.Services.AddSingleton<Shared.Core.Hosting.ILoopProgress>(sp => sp.GetRequiredService<HealthState>());`
- [ ] **D.3** `ProvisioningProcess` (Provisioning): в primary-конструктор
  последним optional-параметром `Shared.Core.Hosting.ILoopProgress? progress = null`
  (после `snapshot`); в `EnsureNodesAsync` — после каждого
  `driver.EnsureNodeAsync(...)` (сразу после вызова, до проверки `ensured.IsSuccess`)
  — `progress?.Mark();`; в `WaitPatroniAsync` — в `foreach` по нодам после каждой
  пробы `probe.IdentifyAsync(...)` — `progress?.Mark();`. В фабрике Program.cs
  последним аргументом — `sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()`.
- [ ] **D.4** `AddShardProcess` (add-shard A-фазы — тот же класс долгих фаз):
  симметрично D.3: optional-параметр `progress`; `EnsureNodesAsync` — Mark после
  каждого `driver.EnsureNodeAsync`; `WaitPatroniAsync` — Mark после каждой пробы
  `probe.IsAliveAsync`; фабрика Program.cs — передать.
- [ ] **D.5** `CutoverSequence`: в конструктор добавить
  `Shared.Core.Hosting.ILoopProgress? progress = null`; в цикле заморозки
  (`for (var attempt = 1; attempt <= Math.Max(1, o.FreezeLockTries); ...)`) —
  `progress?.Mark();` после каждой попытки (после ветки
  ScalarAsync/ExecuteTransactionalAsync, до `Task.Delay`-паузы); в цикле ожидания
  слота (`while (true)` с `SlotCaughtUp`, таймаут `CutoverTimeoutSec=90`) —
  `progress?.Mark();` в теле после каждого `sql.ScalarAsync(... caught ...)`.
  Комментарий: «поллинг cutover — до CutoverTimeoutSec за один тик: отмечаем
  живость каждым проходом, иначе легитимное ожидание догоняния слота расстрелял
  бы watchdog».
- [ ] **D.6** `MoveProcess`: в primary-конструктор optional-параметр
  `Shared.Core.Hosting.ILoopProgress? progress = null` (после `snapshot`);
  field-инициализатор `cutover` → `new(sql, new MoveStatusStore(etcd, etcdEndpoints), secrets, progress)`;
  `DropSlotAsync` — в цикле дезактивации (`for (var attempt = 0; attempt < 5; ...)`)
  `progress?.Mark();` в теле (проход дезактивации ≤5×1 с); `RunAbortAsync` —
  `new AbortSequence(sql, status, requests, journal, shards, secrets, progress)`.
  Фабрика Program.cs — последним аргументом `sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()`.
- [ ] **D.7** `AbortSequence`: в конструктор optional-параметр `progress`; в цикле
  дезактивации слота (`for (var attempt = 0; attempt < 5; ...)`) —
  `progress?.Mark();` в теле.
- [ ] **D.8** XML-комментарии изменённых конструкторов/методов дополнить одной
  строкой: «progress — heartbeat-отметки долгих фаз (null в тестах/без DI)».

**Выход:** точки PgWorker по таблице §4.6 спеки: старт итерации; каждый
create/start контейнера ноды (provisioning + add-shard); каждая итерация опроса
готовности (IdentifyAsync/IsAliveAsync); каждая итерация поллинг-циклов moves
(заморозка: попытка; слот: проход; дезактивация: проход — MoveProcess.DropSlotAsync
и AbortSequence).

**Проверка:**
- [ ] **D.9** `dotnet build src/PgWorker.slnx -c Debug` → 0/0 (optional-параметры:
  существующие прямые `new ProvisioningProcess/AddShardProcess/MoveProcess/
  CutoverSequence/AbortSequence` в юнитах не ломаются);
  `dotnet test src/tests/PgWorker.UnitTests -c Debug` → PASS, 0 warnings.
- [ ] **D.10** Сверка полноты точек: в ревью-чеклисте задачи перечислить grep-ом
  все места `progress?.Mark()` (ожидается: ReconcileLoop ×1 + Provisioning ×2 +
  AddShard ×2 + Cutover ×2 + DropSlot ×1 + Abort ×1) — соответствие таблице §4.6.
- [ ] **D.11** Коммит:

```bash
git add src/PgWorker.App/Loops/ReconcileLoop.cs src/PgWorker.App/Program.cs src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/PgWorker.Provisioning/Processes/AddShardProcess.cs src/PgWorker.Moves/Process/MoveProcess.cs src/PgWorker.Moves/Process/CutoverSequence.cs src/PgWorker.Moves/Process/AbortSequence.cs
git commit -m "feat(t12): heartbeat-точки PgWorker — старт итерации reconcile, контейнеры/опрос готовности provisioning+add-shard, поллинг-циклы moves (заморозка/слот/дезактивация) через ILoopProgress (spec §4.6)"
```

---

## Task E: ValkeyWorker — heartbeat-точки (контейнеры / PING-цикл)

**Связь со spec:** §4.6, таблица (строки ValkeyWorker: старт итерации; контейнеры
нод; каждая итерация PING-цикла `AwaitBootAsync` — бюджет `NodeBootSec` × N нод);
замечание 4 (Valkey).

**Вход:** `src/ValkeyWorker.App/Loops/ReconcileLoop.cs` (`TickAsync`: проверка
endpoints → `processes.TickAsync` → `MarkEtcdOk` → `MarkReconcileTick`);
`src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs` —
`EnsureContainersAsync` (цикл по нодам, `driver.EnsureNodeAsync(new ValkeyNodeSpec(...))`),
`AwaitBootAsync` (последовательно по нодам, `while (true)` с
`valkey.PingAsync(endpoint, ct)`, бюджет `options.NodeBootSec`); фабрика в
`src/ValkeyWorker.App/Program.cs` (`new ProvisioningProcess(` ~строка 188).

**Действие:**

- [ ] **E.1** `ReconcileLoop.TickAsync`: `health.MarkReconcileActivity();` сразу
  после проверки `endpoints.Length` (до `processes.TickAsync(ct)`).
- [ ] **E.2** `ProvisioningProcess`: в primary-конструктор optional-параметр
  `Shared.Core.Hosting.ILoopProgress? progress = null` (после `snapshot`, до
  `clock`); `EnsureContainersAsync` — `progress?.Mark();` после каждого
  `driver.EnsureNodeAsync(...)`; `AwaitBootAsync` — `progress?.Mark();` после
  каждой пробы `valkey.PingAsync(endpoint, ct)` (в теле `while`, до
  `break`-проверки). Комментарий: «бюджет NodeBootSec × ноды за один тик —
  PING-цикл обязан отмечать живость каждой пробой».
- [ ] **E.3** DI: `src/ValkeyWorker.App/Program.cs` — рядом с регистрацией
  `HealthState`:
  `builder.Services.AddSingleton<Shared.Core.Hosting.ILoopProgress>(sp => sp.GetRequiredService<HealthState>());`
  фабрика `ProvisioningProcess` — последним аргументом (до `clock`, который
  остаётся последним positional) передать
  `sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()` (порядок аргументов
  привести к фактической сигнатуре после E.2).

**Выход:** точки ValkeyWorker по таблице §4.6: старт итерации; каждый ensure-вызов
контейнера ноды; каждая PING-проба `AwaitBootAsync`.

**Проверка:**
- [ ] **E.4** `dotnet build src/PgWorker.slnx -c Debug` → 0/0;
  `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` → PASS, 0 warnings
  (существующие юниты ProvisioningProcess создают процесс напрямую — optional-
  параметр не ломает).
- [ ] **E.5** Коммит:

```bash
git add src/ValkeyWorker.App/Loops/ReconcileLoop.cs src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs src/ValkeyWorker.App/Program.cs
git commit -m "feat(t12): heartbeat-точки ValkeyWorker — старт итерации reconcile, контейнеры нод, каждая PING-проба AwaitBootAsync через ILoopProgress (spec §4.6)"
```

---

## Task F: KafkaWorker — точка старта итерации

**Связь со spec:** §4.6 («KafkaWorker — глубоких точек не требуется»:
`WaitReadyAsync` делает ровно один `DescribeCluster` с `RequestTimeout=10 с` за
тик, бутстрап-бюджет считается между тиками; тик — единицы секунд, запас до
порога 60 с кратный); замечание 4 (Kafka).

**Вход:** `src/KafkaWorker.App/Loops/ReconcileLoop.cs` — `TickAsync`: проверка
`endpoints` → чтение `/kafka/clusters/` → обработка → `MarkReconcileTick` в конце.

**Действие:**

- [ ] **F.1** `TickAsync`: `health.MarkReconcileActivity();` сразу после проверки
  `endpoints.Length` (до `RangeWithFailoverAsync(endpoints, "/kafka/clusters/", ct)`)
  с комментарием: «единственная точка KafkaWorker: ожидания расползаются по тикам
  (один DescribeCluster RequestTimeout=10 с за тик) — спека §4.6».

**Выход:** старт-итерация KafkaWorker отмечает активность; глубоких точек нет
(обоснование зафиксировано в спеке и коде-комментарии).

**Проверка:**
- [ ] **F.2** `dotnet build src/PgWorker.slnx -c Debug` → 0/0;
  `dotnet test src/tests/KafkaWorker.UnitTests -c Debug` → PASS, 0 warnings.
- [ ] **F.3** Коммит:

```bash
git add src/KafkaWorker.App/Loops/ReconcileLoop.cs
git commit -m "feat(t12): heartbeat-точка KafkaWorker — старт итерации reconcile (единственная: ожидания расползаются по тикам, spec §4.6)"
```

---

## Task G: интеграционные кейсы heartbeat (не docker)

**Связь со spec:** §4.7 (интеграционный блок: устаревшая активность → остановка;
долгая фаза с отметками → хост живёт); замечание 5.

**Вход:** `src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs` —
существуют: `FakeVitality(bool stale)` (один раз фиксирует `at` по флагу),
`WatchdogFactory` (снимает циклы, оставляет watchdog, уменьшенные интервалы
`Loops` 2/2 → healthz 21 с, watchdog ×2 = 42 с), кейсы
`StaleHeartbeat_HostStopsWithinBudget`, `FreshHeartbeats_HostAlive_AndHealthzWatchdogSection`
(ассерт `"armed; stale=нет"`).

**Действие:**

- [ ] **G.1** `FakeVitality` — переделать под активность с режимом «долгая фаза»:

```csharp
// Витальность с семантикой активности: stale — активность заморожена давно
// (зависание без исключения); longPhase — «долгая итерация»: активность
// обновляется прогресc-отметками каждые markEvery (тик при этом давний/отсутствует).
private sealed class FakeVitality(bool stale, TimeSpan? markEvery = null) : ILoopsVitality
{
    private DateTimeOffset? _lastMark;

    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        var threshold = TimeSpan.FromSeconds(21) * 2; // FastLoops(2,2)=21 c ×2
        var now = DateTimeOffset.UtcNow;
        if (stale)
            return [new LoopHeartbeat("reconcile", now - TimeSpan.FromMinutes(5), threshold)];
        if (_lastMark is null || now - _lastMark >= markEvery)
            _lastMark = now; // прогресс-отметка долгой фазы
        return [new LoopHeartbeat("reconcile", _lastMark, threshold)];
    }
}
```

  (`WatchdogFactory`/`WatchdogHost` — прокинуть новый параметр `markEvery`;
  `stale:false`-завод по умолчанию — `markEvery: TimeSpan.FromSeconds(3)`.)
- [ ] **G.2** Кейс остановки: переименовать `StaleHeartbeat_HostStopsWithinBudget` →
  `StaleActivity_HostStopsWithinBudget` (тело то же: поллинг `ApplicationStopping`
  ≤30 с бюджет, шаг 1 с; комментарий Arrange: «активности нет — ни тика, ни
  отметки, возраст LastActivityAt 5 мин при пороге 42 с»).
- [ ] **G.3** Кейс долгой фазы: переименовать
  `FreshHeartbeats_HostAlive_AndHealthzWatchdogSection` →
  `LongPhase_ProgressMarks_HostAlive_AndHealthzWatchdogSection`: наблюдение
  увеличить до 15 с (3+ цикла отметок при `markEvery` 3 с — отметки регулярно
  обновляются, тика нет вовсе), ассерты: `ApplicationStopping` не завершён,
  `/healthz` 200, секция `watchdog` содержит `armed` (тело/ассерты прежние,
  интервал наблюдения и комментарии обновить).
- [ ] **G.4** Комментарий класса файла дополнить: семантика — активность
  (тиков может не быть); порог 42 с от уменьшенных интервалов.

**Выход:** интеграционные кейсы §4.7 под heartbeat: устаревшая активность →
graceful-остановка в бюджет; долгая фаза с прогресс-отметками → хост живёт.

**Проверка:**
- [ ] **G.5** `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Hosting.LoopWatchdogTests"` → PASS
  (кейсы укладываются в ~16–20 с и ~20 с соответственно; каждое ожидание ≤30 с).
- [ ] **G.6** Коммит:

```bash
git add src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs
git commit -m "test(t12): интеграционные кейсы heartbeat — устаревшая активность останавливает хост, долгая фаза с прогресс-отметками живёт (spec §4.7, AC2)"
```

---

## Task H: E2eScaleScenarios — MarkFailed-обёртка

**Связь со spec:** канон `docs/e2e-launch.md` §3 (упавший сценарий — телеметрия
в артефактах + контейнеры остановлены, но не удалены до разбора); замечание 7.

**Вход:** `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs` — оба
`[Fact]` (`Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused`,
`Scale_TakeoverMidAdd_SecondInstanceFinishesNoDuplicates`) исполняют тело без
try/catch; эталон — `E2eMoveScenarios.Move_Lifecycle_Chain` и WalStream-сценарии:
`try { тело } catch { Fx.MarkFailed(); throw; }` с комментарием.

**Действие:**

- [ ] **H.1** Оба факта: тело (всё после `Fx = fx;` и объявления `cluster`) обернуть:

```csharp
try
{
    // …существующее тело без изменений…
}
catch
{
    // docs/e2e-launch.md §3: упавший сценарий — окружение ОСТАНОВИТЬ, не
    // удалить (телеметрия в артефактах + живые объекты для разбора).
    Fx.MarkFailed();
    throw;
}
```

  (копия паттерна E2eMoveScenarios строка ~53; `await using var fx` остаётся снаружи).

**Выход:** упавший Scale-сценарий сохраняет контейнеры/тома/сети до разбора,
README-cleanup.txt в артефактах.

**Проверка:**
- [ ] **H.2** `dotnet build src/tests/PgWorker.IntegrationTests -c Debug` → 0/0
  (запуск E2E — не здесь; это Task I).
- [ ] **H.3** Коммит:

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs
git commit -m "test(t12): E2eScaleScenarios — MarkFailed-обёртка тел сценариев (канон e2e-launch: упавший сценарий сохраняет окружение до разбора)"
```

---

## Task I: мерж-гейт — AC3-перезапуск, артефакты серии, roadmap-гейт

**Связь со spec:** §4.7 (E2E-маркер), §4.8 (roadmap-гейт); AC3/AC7; замечания 8, 9.

**Вход:** все задачи A–H закоммичены; roadmap-правки лежат в рабочем дереве
(`arch/roadmap/reliability.md` — пункт t12 снят; `reliability-report.md` — строка
перенесена в «Сделано», сводка R без «нет watchdog зависших циклов (t12)»).

**Действие:**

- [ ] **I.1** Полная сборка Release: `dotnet build src/PgWorker.slnx -c Release`
  → 0 errors / 0 warnings.
- [ ] **I.2** Гейт чистоты ДО серии — только диагностика, без удаления: серия ещё
  не запущена — своего guid нет, любые контуры `pgw-*`/`kfw-net-*` и стенд
  (`as-*`, `adminpanel`) — чужие и могут быть живыми. Проверить `docker ps -a` и
  `docker network ls`; при остатках НЕ чистить (`docker rm`/`network prune` по
  общим фильтрам запрещены — сносится живой контур соседней сессии) — СТОП и
  вопрос пользователю.
- [ ] **I.3** E2E-маркер AC3 (в фоне, следить за прогрессом; фикстура собирает
  Release сама — инкрементальный no-op):

  ```
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
  ```

  Ожидание: PASS 1/1 — инстансы проживают полный provisioning (бутстрап нод с
  pg_basebackup, add-shard, переезды) без остановки. Тест идёт дольше 5 минут —
  онлайн-анализ логов по канону AGENTS.base.md §13, тест не останавливать.
- [ ] **I.4** AC3 — нет ложных срабатываний за серию. Путь артефактов —
  `Path.GetTempPath()` (на macOS `/var/folders/...`, НЕ `/tmp` — глобальный grep
  по `/tmp/pgw-e2e-artifacts-*` давал бы вечный ложный ноль и запрещён):
  1) определить каталог СВОЕЙ серии: самый свежий
     `ls -td "$TMPDIR"pgw-e2e-artifacts-* | head -1`; guid сверить с серией
     (имена `pgw-en-<runId>`/`pgw-ee1-<runId>` из `docker ps` во время прогона
     или строкой прогона);
  2) `grep -r "инициирован self-restart" "<каталог серии>" | wc -l` → **0**
     (счётчик `worker_watchdog_restarts_total` инкрементируется той же точкой
     кода, что пишет журнал — отсутствие записи = отсутствие инкремента);
  3) пусто = AC3 закрыт: бутстрап с heartbeat-точками не даёт ни одного
     self-restart.
- [ ] **I.5** Страховочный own-only гейт СВОЕЙ серии (основная чистка — teardown
  фикстуры; канон `docs/e2e-isolation.md`): взять runId/ClusterTag своей серии;
  перечислить кандидатов ДО удаления (`docker ps -aq --filter name=<runId>` и
  `--filter name=<ClusterTag>`, `docker network ls | grep -e <runId> -e <ClusterTag>`);
  убедиться, что контуры не живые (серия завершена, процесса-владельца нет);
  удалить только своих и поимённо (`docker rm -f <имя>`, `docker network rm <имя>`);
  контроль — `docker ps -aq --filter name=<runId>` → 0. Кандидат без своего
  guid/тега или с признаками живого владельца — СТОП и вопрос пользователю;
  `prune` не использовать; контейнеры упавшего сценария (`MarkFailed`) не
  удалять до окончания разбора.
- [ ] **I.6** Roadmap-гейт: правки уже в рабочем дереве — сверить полноту и
  закоммитить. Контроль:
  `grep -rn "t12-loop-watchdog" arch/roadmap/` → единственные вхождения: строка
  «Сделано» в `reliability-report.md` и её merge-колонка («— (мерж-коммит
  t12-loop-watchdog)»); в `reliability.md` — пусто; сводка R — без «нет watchdog
  зависших циклов».

```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "merge-gate(t12): пункт t12-loop-watchdog снят из roadmap; reliability-report — перенос в «Сделано» + сводка R (heartbeat-семантика: firing по отсутствию активности, ILoopProgress, точки §4.6)"
```

- [ ] **I.7** Контрольная не-E2E серия: перед серией — гейт чистоты диагностикой
  (как I.2); `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName!~E2e"`
  → PASS; дождаться финальной строки, контуры фикстур убирает их teardown
  (testcontainers/ryuk); остатки в `docker ps -a`/`docker network ls` — СТОП и
  вопрос пользователю (чужое не трогать).

**Выход:** AC3 доказан на свежем Release (PASS 1/1, 0 self-restart за серию по
артефактам своего каталога), roadmap-гейт закоммичен, вся не-E2E серия зелёная.

**Проверка:** шаги I.3–I.7 сами являются проверкой; план задачи закрыт, ветка
готова к ревью и мержу в main по отдельному приказу пользователя.

---

## Соответствие задач критериям приёмки спеки

| AC | Чем закрыт (доработка) |
|---|---|
| AC1 (юнит — механика) | Task B (heartbeat-семантика: активность внутри порога → не firing; активности нет → firing; grace по активности), Task C (MarkReconcileActivity без тика; тик обновляет оба) |
| AC2 (интеграция) | Task G (устаревшая активность → остановка в бюджет; долгая фаза с отметками → хост живёт) |
| AC3 (нет ложных срабатываний) | Task I (перезапуск маркера PASS 1/1 + grep артефактов своей серии → 0; корректный путь Path.GetTempPath) |
| AC4 (наблюдаемость) | уже сделано (Tasks 1–11) + Task B (журнал «не проявлял активности») |
| AC5 (полнота перечня) | уже сделано (Tasks 1–11), не пересматривается |
| AC6 (симметрия + точки §4.6) | Task D/E/F — точки по таблице §4.6 (Kafka — только старт итерации), один ILoopProgress Shared.Core |
| AC7 (канон и гейт) | Task A (arch/runbook), Task I (roadmap-гейт) |
| Замечание 7 (e2e-launch) | Task H (MarkFailed-обёртка) |

## Самопроверка плана (выполнена при написании)

- Все 9 замечаний правки спеки покрыты: 1 → B/C; 2 → C; 3 → B (интерфейс) + C
  (реализации в HealthState) + D/E (инжекция в процессы); 4 → D/E/F (точки
  сверены с фактическим кодом: `IdentifyAsync`/`IsAliveAsync`, `CutoverSequence`
  field-инициализатор в MoveProcess, `AwaitBootAsync`/`PingAsync`,
  `EnsureContainersAsync`); 5 → B/C/G; 6 → A; 7 → H; 8 → I.4; 9 → I.3–I.6;
  10 → порядок A→B→C→D/E/F→G→H→I и коммиты по задачам.
- Факты кода проверены по worktree (HEAD `ebdac99`): потребителей `.LastTickAt`
  кроме витальностей/`LoopWatchdog` нет; прямых `new HealthSnapshot(` в тестах
  нет; `PgWorker.Moves`/`PgWorker.Provisioning`/`ValkeyWorker.Provisioning`
  имеют транзитивный `Shared.Core`; артефакты E2E уже пишутся в
  `Path.GetTempPath()` (правка — только команда проверки в плане); roadmap-правки
  уже в рабочем дереве.
- Каждая задача завершается зелёной сборкой: B включает механическое переименование
  потребителей, C переключает витальности на новое поле снимка.
- Плейсхолдеров нет; сигнатуры/имена/точки вставки — фактические.
- Пороги/формулы/метрики/HTTP-семантика healthz не меняются нигде.
