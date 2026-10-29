# Живость свипера S3-сирот: цикл orphan-sweep + алерт OrphanSweepStalled — план

> **Для исполнителя (executor=coder):** план исполняет субагент coder ЦЕЛИКОМ сам
> (включая подпункты — субагентов не создаёт, отдаёт их себе как точные задания).
> Шаги оформлены чекбоксами. Перед исполнением прочитать
> `/Users/demakaev/ZCodeProject/AGENTS.base.md` и `/Users/demakaev/ZCodeProject/pg/AGENTS.md`
> (ссылку передавать любым субагентам, если они появятся).

**Цель:** закрыть слепую зону алертов на `BackupOrphanSweeperLoop` — цикл начинает
эмитить `worker_loop_ticks_total` / `worker_loop_last_success_timestamp_seconds` /
`worker_loop_duration_seconds` с `loop="orphan-sweep"`, появляется правило
`OrphanSweepStalled`; arch-first (сначала arch/18+arch/19, затем код).

**Архитектура:** без новых серий/лейблов — только словарь arch/18 §2.2 с лейблом
`loop="orphan-sweep"` (имя уже канонизировано watchdog-каналом `LoopStaleness`).
Единственная точка инструментации — лидерская ветка `BackupOrphanSweeperLoop`
(Stopwatch + `LoopTick`/`LoopDuration` по итогам `SweepAsync`); отдельное
alert-правило с фиксированным порогом 3× интервала. Метрики — пассивные
наблюдатели: точки вызова не влияют на поведение цикла.

**Стек:** .NET 10 (`TreatWarningsAsErrors=true`, `Nullable=enable`), System.Diagnostics.Metrics
(`WorkerMetricsInstrumentation`), Prometheus `rules.yml`, bash-чеки dev-стенда.

**Spec:** `docs/superpowers/2026-10-21-fix-slepaya-zona-alertov-sviper-sirot/spec.md`
(исполнитель читает spec И этот план).

**Рабочий каталог ВСЕХ правок и команд** (кроме шага 5.4): worktree
`/Users/demakaev/ZCodeProject/worktrees/fix-slepaya-zona-alertov-sviper-sirot`
(ветка `fix-slepaya-zona-alertov-sviper-sirot`). Относительные пути ниже — от его корня.

## Global Constraints

- Сборка и тесты — ВСЕГДА двумя ОТДЕЛЬНЫМИ командами: сначала `dotnet build`, потом
  `dotnet test --no-build`; env-префикс при самой команде (`DOTNET_CLI_UI_LANGUAGE=en ...`),
  не `export`.
- Перед docker-E2E PgWorker — обязательная статическая подготовка ОДНИМ процессом:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`.
- Команды с длинным выводом (build/test/run) — запускать в фоне, полный вывод в
  лог-файл `/tmp/...`, анализ только из файла; сразу фиксировать task_id.
- Комментарии в коде и документации — русские; идентификаторы — английские; тесты —
  с AAA-комментариями (Arrange/Act/Assert).
- Никаких новых имён серий/лейблов: только серии словаря §2.2 с `loop="orphan-sweep"`
  (решение Д1). Порог правила фиксирован 1800 с = 3× дефолта `Supervisor:IntervalSec`
  (Д3); severity `warning` (Д4); `LoopDuration` добавляем (Д5); `for: 0m`; никаких
  `absent()`-условий.
- Границы (spec §3.8 — НЕ меняем): `WorkerLoopStalled` (регэкс/порог/сseverity),
  `SnapshotStale` и прочие правила; дашборд `workers.json`; `HealthState`/watchdog/
  `PulsingDelay`; `LoopStaleness.cs`; KafkaWorker/S3Worker/ValkeyWorker;
  интеграционный `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs` (Д8 — без
  ассерта `orphan-sweep`).
- Точки БЕЗ метрик (Д2): не-лидерская ветка, `Backups:Enabled=false`, потеря
  leader-lease (`OperationCanceledException` при живом `stoppingToken`) — серию не пишем.
- В arch-правки не добавлять исторических пассажей/атрибуций (AGENTS.md).
- `TreatWarningsAsErrors=true`: сборка Release обязана пройти без варнингов.

## Review Focus (что spec подразумевает, но автотесты задач не пинают)

1. **Не-лидер / `Backups:Enabled=false` не эмитят серию** (AC4): единственная точка
   вызова `LoopTick`/`LoopDuration` — внутри `if (claims.IsLeader && Backups.Enabled)`
   после завершения `SweepAsync`. Закрыто код-ревью в шаге 2 (проверка: grep по файлу
   цикла — вызовы только в одном месте) — тест намеренно не пишется (spec AC4).
2. **Потеря leader-lease посреди прохода** — серия не пишется (проход не завершён):
   catch-ветка `OperationCanceledException` не содержит вызовов метрик. Закрыто ревью
   шага 2 (граничит с HangingS3-тестом, который остаётся зелёным).
3. **`LoopDuration` пишется по ЛЮБОМУ завершённому проходу (ok=true и ok=false)** —
   длительность известна независимо от исхода; порядок вызовов: `LoopDuration` до
   `LoopTick`. Закрыто ревью шага 2.
4. **Серия `orphan-sweep` у инстанса до первого прохода лидера не существует** (§5,
   ObservableGauge эмитит существующий стейт) — смерть до первого прохода ловит
   `ServiceDown`. Закрыто ревью шага 2 (никаких seed-вызовов метрик в конструкторе).
5. **Правило не содержит `absent()`, порог/тексты дословны из spec §3.6, группа
   `workers`, job-фильтра нет** (Д3/Д4). Закрыто сверкой текста в шаге 3; регистрация
   22 правил — чек 65 шага 5.3 (при поднятом стенде).

---

### Задача 1 (Фаза A): arch-канон — arch/18 §2.2 + arch/19 §4 (ДО кода)

**Файлы:**
- Modify: `arch/18-metrics.md` (строки 87, 88, абзац строк 108–114)
- Modify: `arch/19-backups.md` (строка 597, строка таблицы ключа `/pgworker/backups/orphans`)

**Вход:** worktree чист (`git status`), spec прочитан.

**Действие (правки текстов дословно):**

1.1. `arch/18-metrics.md`, строка 87 таблицы §2.2 — в строке `worker_loop_ticks_total`
заменить перечень циклов (решение Д6): `тики циклов (Reconcile/Keepalive/Snapshot)` →
`тики циклов (Reconcile/Keepalive/Snapshot/OrphanSweep)`. Остальная часть строки без изменений.

1.2. `arch/18-metrics.md`, строка 88 таблицы §2.2 — в строке
`worker_loop_last_success_timestamp_seconds` ПОСЛЕ фрагмента
«его живость закрывает SnapshotStale по `worker_snapshot_age_seconds`» и ДО закрывающей
скобки ячейки добавить (решения Д2/Д3/Д4, текст spec §3.1.2):

```
; цикл `orphan-sweep` (BackupOrphanSweeperLoop, тик лидера раз в `Backups:Supervisor:IntervalSec`, дефолт 600 с; `ok` = успех прохода `SweepAsync`) — живость закрывает отдельное правило **OrphanSweepStalled** (`time() - worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"} > 1800`, порог 3× дефолтного интервала; серия эмитится только лидером при `Backups:Enabled=true` — выключенная подсистема не алертится, смерть процесса ловит ServiceDown)
```

Итоговая ячейка «Смысл» заканчивается на `...ловит ServiceDown)) |` (внешняя скобка
ячейки + внутренняя скобка добавляемого фрагмента).

1.3. `arch/18-metrics.md`, абзац строк 108–114 (стационарные ops без терминальной
фазы) — заменить последние две строки абзаца
`эмитят: их живость закрывает \`worker_loop_last_success_timestamp_seconds\` (алерт
WorkerLoopStalled), вечные серии не копятся.`
на (spec §3.1.3, решения Д1/Д2):

```
эмитят, вечные серии не копятся: живость `supervise`/`evacuate` закрывают клэйм-циклы
(`loop="reconcile"` → алерт WorkerLoopStalled), живость `backup-orphan-sweeper` —
цикл `orphan-sweep` (LoopTick по итогам SweepAsync → алерт OrphanSweepStalled).
```

(первая часть абзаца — «Стационарные ops без терминальной фазы — `supervise` … в
work-фазовые метрики не входит) — фазовых серий НЕ» — остаётся как есть).

1.4. `arch/19-backups.md`, строка 597 (таблица §4, ключ `/pgworker/backups/orphans`) —
вставить предложение ПОСЛЕ фрагмента «пишет ТОЛЬКО глобальный лидер-проход
`/pgworker/leader` (паттерн storage/supervisor).» — сразу ПОСЛЕ ТОЧКИ, отделяя одним
пробелом, НЕ трогая идущий далее текст «`first_seen_unix` переносится…» (spec §3.2, без
пересказа словаря arch/18 — только ссылка). Итоговый фрагмент строки:

```
…пишет ТОЛЬКО глобальный лидер-проход `/pgworker/leader` (паттерн storage/supervisor). Живость лидер-прохода наблюдаема серией `worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"}` (тик цикла по итогам прохода; алерт OrphanSweepStalled — [18](18-metrics.md) §2.2). `first_seen_unix` переносится…
```

Вставляемое предложение (без многоточий-обрамления):

```
Живость лидер-прохода наблюдаема серией `worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"}` (тик цикла по итогам прохода; алерт OrphanSweepStalled — [18](18-metrics.md) §2.2).
```

**Выход:** arch/18 §2.2 и arch/19 §4 описывают цикл `orphan-sweep`, правило
OrphanSweepStalled и семантику подавленных ops раздельно; исторических пассажей нет.

**Проверка:**
- `grep -n "OrphanSweepStalled\|orphan-sweep" arch/18-metrics.md arch/19-backups.md` —
  находит все точки (строка 87 не попадает — там `OrphanSweep` без дефиса; отдельно
  `grep -n "OrphanSweep" arch/18-metrics.md`).
- `grep -n "WorkerLoopStalled" arch/18-metrics.md` — в §2.2 нет утверждения «живость
  backup-orphan-sweeper закрывает WorkerLoopStalled» без оговорки про отдельный
  цикл/алерт (гейт Фазы A spec §4).
- Исторических пассажей/атрибуций в правках нет.

**Связь со spec:** §3.1, §3.2, Д1–Д6; Фаза A.

**Коммит:** `docs(arch): живость свипера сирот — цикл orphan-sweep и алерт OrphanSweepStalled (arch/18 §2.2, arch/19 §4)`

Подпункты (механика) — не выделяются: правки документации делает сильная модель,
тексты зафиксированы выше дословно.

---

### Задача 2 (Фаза B): инструментация цикла + DI + комментарии + адаптация тестов конструктора

**Файлы:**
- Modify: `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs`
- Modify: `src/PgWorker.App/Program.cs` (фабрика, строки ~779–784)
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` (только комментарии)
- Modify: `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs` (подпункт 2.4)

**Interfaces (производит):** вызовы
`metrics.LoopTick("orphan-sweep", ok: result.IsSuccess)` и
`metrics.LoopDuration("orphan-sweep", elapsed.TotalSeconds)` — сигнатуры из
`WorkerMetricsInstrumentation`: `public void LoopTick(string loop, bool ok)`,
`public void LoopDuration(string loop, double seconds)`.

**Вход:** задача 1 закоммичена (arch-first).

**Действие:** четыре подпункта 2.1–2.4 ниже (механика), затем сам ревьюит диф: единственная
точка вызова метрик — в лидерской ветке после завершения `SweepAsync`; catch-ветки и
не-лидерская ветка метрик не касаются; `HealthState`/`PulsingDelay`/пульс не тронуты.

**Подпункт 2.1. `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs` — три правки:**

(а) Добавить `using System.Diagnostics;` к существующим using-ам файла (нужен `Stopwatch`;
по образцу `src/PgWorker.App/Loops/SnapshotLoop.cs`, где он есть).

(б) В списке параметров primary-конструктора класса `BackupOrphanSweeperLoop` добавить
четвёртым параметром (после `BackupOrphanSweeper sweeper`, перед `HealthState health`)
`Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics`. Итоговая сигнатура:

```csharp
internal sealed class BackupOrphanSweeperLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    BackupOrphanSweeper sweeper,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics,
    HealthState health,
    ILogger<BackupOrphanSweeperLoop> logger) : BackgroundService
```

(в) В методе `ExecuteAsync`, в лидерской ветке `if (claims.IsLeader && options.CurrentValue.Backups.Enabled)`,
заменить блок (комментарий «Ошибка прохода» + вызов + лог):

```csharp
                        // Ошибка прохода — лог (SweepAsync сам Result; transient-отказы
                        // S3/etcd повторит следующий проход). Реестр в etcd переживает
                        // смену лидера — продолжение с факта, не с нуля.
                        var result = await sweeper.SweepAsync(tick);
                        if (!result.IsSuccess)
                            logger.LogWarning("orphan-sweep: {Error}", result.Error?.Message);
```

на:

```csharp
                        // Ошибка прохода — лог (SweepAsync сам Result; transient-отказы
                        // S3/etcd повторит следующий проход). Реестр в etcd переживает
                        // смену лидера — продолжение с факта, не с нуля. Тик/длительность
                        // цикла orphan-sweep (arch/18 §2.2): ok = успех ЗАВЕРШЁННОГО
                        // прохода лидера; не-лидер и Backups:Enabled=false серию не пишут.
                        var startedAt = Stopwatch.GetTimestamp();
                        var result = await sweeper.SweepAsync(tick);
                        metrics.LoopDuration("orphan-sweep", Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
                        metrics.LoopTick("orphan-sweep", ok: result.IsSuccess);
                        if (!result.IsSuccess)
                            logger.LogWarning("orphan-sweep: {Error}", result.Error?.Message);
```

Больше в файле НИЧЕГО не менять (не-лидерская ветка, `OperationCanceledException`-ветки,
`PulsingDelay`, XML-комментарий класса — без изменений; метрики — пассивные наблюдатели,
исключения наружу не бросают: `LoopTick`/`LoopDuration` уже содержат try/catch внутри).

**Подпункт 2.2. `src/PgWorker.App/Program.cs` — фабрика BackupOrphanSweeperLoop (строки ~779–784):**

В регистрации `builder.Services.AddSingleton(sp => new PgWorker.App.Loops.BackupOrphanSweeperLoop(...))`
добавить параметром после `sp.GetRequiredService<PgWorker.Backups.Supervisor.BackupOrphanSweeper>()`
строку `sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()`
(синглтон уже зарегистрирован строками ~99–109). Итог:

```csharp
builder.Services.AddSingleton(sp => new PgWorker.App.Loops.BackupOrphanSweeperLoop(
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<PgWorker.Backups.Supervisor.BackupOrphanSweeper>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>(),
    sp.GetRequiredService<HealthState>(),
    sp.GetRequiredService<ILogger<PgWorker.App.Loops.BackupOrphanSweeperLoop>>()));
```

**Подпункт 2.3. `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` — только два комментария, код не трогать:**

(а) Комментарий над `SuppressedOps` (строки ~18–24) — после строки «не входит).» добавить
две строки:

```csharp
    // Живость: supervise/evacuate — клэйм-циклы (алерт WorkerLoopStalled),
    // backup-orphan-sweeper — цикл orphan-sweep (алерт OrphanSweepStalled, arch/18 §2.2).
```

(б) Комментарий над `OnJournalPhase` (строки ~545–549) — заменить строку
`//    живость надзора закрывает WorkerLoopStalled (решение ревью Ф4-2);`
на две строки:

```csharp
    //    живость supervise/evacuate закрывает WorkerLoopStalled (решение ревью Ф4-2),
    //    backup-orphan-sweeper — цикл orphan-sweep (алерт OrphanSweepStalled);
```

**Подпункт 2.4. `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs` — обновить два существующих вызова конструктора цикла (ДО гейта сборки: тест-проект входит в `src/PgWorker.slnx`, без этой правки `dotnet build src/PgWorker.slnx` падает CS7036 на старой 5-аргументной сигнатуре):**

(а) В using-блок файла (после `using Shared.Core.Hosting;`) добавить:
`using System.Diagnostics.Metrics;` и `using PgWorker.Etcd.S3;`
(`Meter`/`MeterListener` — подпункт 2.4(б)/(в) и Задача 4; `IS3PoolStore` — приватный
класс Задачи 4 (`FaultingPoolStore`); между Задачами 2 и 4 `using PgWorker.Etcd.S3`
временно не используется — это НЕ варнинг компилятора, гейт сборки проходит;
`IBackupS3`, `Result`, `S3ObjectInfo` уже доступны — глобальный using `PgWorker.Backups`
в `GlobalUsings.cs` проекта).

(б) В приватном методе `Create(int scanIntervalSec, int supervisorIntervalSec)` перед
строкой `var loop = new BackupOrphanSweeperLoop(...)` добавить:

```csharp
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
            new Meter("TestOrphanSweep"), TimeProvider.System);
```

и изменить вызов конструктора на:

```csharp
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
```

(в) В тесте `ExecuteAsync_LeaderHangingSweep_PulsesActivityBeyondTick` перед
`var loop = new BackupOrphanSweeperLoop(...)` добавить:

```csharp
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
            new Meter("TestOrphanSweep"), TimeProvider.System);
```

и в конструктор цикла четвёртым аргументом (после `sweeper`, перед `health`) передать
`metrics`. Остальное тело теста не менять.

**Выход:** цикл инструментирован, DI подключён, комментарии не врут, существующие
тесты адаптированы под новый конструктор — решение `src/PgWorker.slnx` собирается
целиком; все другие проекты решения не тронуты.

**Проверка (сборка ВСЕГО решения — теперь зелёная после 2.4; фоном, лог в файл):**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/plan-orphan-sweep-build.log 2>&1
```
Ожидание: в конце лога `Build succeeded`, `0 Warning(s)`, `0 Error(s)`
(`TreatWarningsAsErrors=true` — любой варнинг = ошибка). Дополнительно:
`grep -c "LoopTick\|LoopDuration" src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs` → `2`
(по одному вызову, только в лидерской ветке).

**Связь со spec:** §3.3, §3.4, §3.5, Д1, Д2, Д5, Д7; Фаза B; Фаза D.1 (тесты получают
настоящий `WorkerMetricsInstrumentation` — паттерн `ReconcileLoopTests`).

**Коммит:** `feat(metrics): тики цикла orphan-sweep в BackupOrphanSweeperLoop (LoopTick/LoopDuration по итогам SweepAsync) + адаптация конструктора в юнит-тестах`

---

### Задача 3 (Фаза C): alert-правило OrphanSweepStalled + комментарии чека 65

**Файлы:**
- Modify: `dev-stand/adminpanel/metrics/prometheus/rules.yml` (группа `workers`)
- Modify: `dev-stand/adminpanel/checks/65-metrics.sh` (строки ~150 и ~153)

**Вход:** задачи 1–2 закоммичены.

**Действие:** подпункты 3.1–3.2, затем ревью дифа (правило дословно, соседние правила не тронуты).

**Подпункт 3.1. `dev-stand/adminpanel/metrics/prometheus/rules.yml`:**

В группе `workers` вставить НОВОЕ правило СРАЗУ ПОСЛЕ правила `WorkerLoopStalled`
(после строки с его `description: "...runbook — arch/18 §2.2"`), ПЕРЕД `- alert: SnapshotStale`,
дословно (spec §3.6; отступы — 6 пробелов у `- alert:`, как у соседних):

```yaml
      - alert: OrphanSweepStalled
        expr: time() - worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"} > 1800
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.job }} orphan sweeper not sweeping for >30m"
          description: "fixed threshold 3x Supervisor:IntervalSec (600s default); series emitted only by the leader with Backups:Enabled=true; runbook — arch/18 §2.2, arch/19 §4"
```

Ничего другое в файле не менять (`WorkerLoopStalled`, `SnapshotStale`, группа `backups` — как были).

**Подпункт 3.2. `dev-stand/adminpanel/checks/65-metrics.sh` — только два текста (порог `>= 21` НЕ менять, Д9):**

(а) Строка ~150, комментарий:
`# 4) rules зарегистрированы (21 алерт: 11 + 7 группы backups §2.7 + 3 t15 patroni-nodes)` →
`# 4) rules зарегистрированы (22 алерта: 12 + 7 группы backups §2.7 + 3 t15 patroni-nodes)`

(б) Строка ~153, echo:
`echo "  rules: $rules alerts registered (18 + 3 t15: patroni-nodes)"` →
`echo "  rules: $rules alerts registered (19 + 3 t15: patroni-nodes)"`

(12 без t15 = 4 workers + OrphanSweepStalled + 3 kafka + 3 valkey + 1 pg; 12+7=19, 19+3=22.)

**Выход:** rules.yml содержит 22-е alerting-правило; чек 65 описывает актуальный счёт.

**Проверка:**
- `grep -n "alert:" dev-stand/adminpanel/metrics/prometheus/rules.yml | wc -l` → `22`.
- `grep -n "OrphanSweepStalled" -A 6 dev-stand/adminpanel/metrics/prometheus/rules.yml` —
  правило в группе `workers`, текст совпадает с подпунктом 3.1 посимвольно; `absent(`
  в файле отсутствует.
- `bash -n dev-stand/adminpanel/checks/65-metrics.sh` — синтаксис ok.
- Полная валидация YAML/регистрации — чеком 65 в задаче 5.3 (при поднятом стенде).

**Связь со spec:** §3.6, §3.7, Д3, Д4, Д9; Фаза C.

**Коммит:** `feat(alerts): правило OrphanSweepStalled (1800s, warning) + актуализация комментариев чека 65-metrics`

---

### Задача 4 (Фаза D): юнит-тесты тиков orphan-sweep

**Файлы:**
- Modify: `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`

**Interfaces (потребляет):** конструктор
`BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health, logger)` из задачи 2
(подпункт 2.1; вызовы в существующих тестах уже обновлены подпунктом 2.4);
`WorkerMetricsInstrumentation(Meter, TimeProvider)`; counter-инструмент `worker.loop.ticks`
(теги `loop` — string, `ok` — bool).

**Вход:** задача 2 закоммичена — сборка `src/PgWorker.slnx` зелёная, существующие вызовы
конструктора в `BackupOrphanSweeperLoopTests` уже адаптированы (подпункт 2.4), using-и
`System.Diagnostics.Metrics`/`PgWorker.Etcd.S3` уже в файле.

**Действие:** подпункты 4.1–4.2 (только НОВЫЕ тесты и их приватные классы), затем ревью:
старые тесты не сломаны, AAA-комментарии на месте, поллинг ≤5 с.

**Подпункт 4.1. Успешный проход — тик `ok=true` (MeterListener):**

(а) В конец класса (после вложенного класса `HangingS3`) добавить приватный класс —
S3-стаб с мгновенными пустыми list-ответами (все члены интерфейса обязаны существовать;
нереализуемые — NotSupportedException, по образцу `HangingS3` в этом же файле):

```csharp
    // S3-стаб с мгновенными пустыми list-ответами: проход sweeper'а завершается
    // сразу (HangingS3 не подходит — нужен ЗАВЕРШАЮЩИЙСЯ фейк для тика ok=true).
    private sealed class EmptyS3 : IBackupS3
    {
        public Task<Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>.Success(
                (IReadOnlyList<PgWorker.Backups.S3ObjectInfo>)[]));
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
    }
```

(б) Добавить тест (после `ExecuteAsync_LeaderHangingSweep_PulsesActivityBeyondTick`):

```csharp
    // AAA: лидерский проход с Backups:Enabled=true и живым S3 — по итогам SweepAsync
    // инструментация пишет тик цикла orphan-sweep с ok=true (arch/18 §2.2; ок
    // последнего успешного тика двигает last_success — серия появляется).
    [Fact]
    public async Task ExecuteAsync_LeaderSweep_TicksOrphanSweepOkTrue()
    {
        // Arrange: Enabled=true, лидерство возьмёт FakeEtcd-txn; S3 отвечает пустыми
        // list'ами мгновенно; MeterListener собирает counter worker.loop.ticks.
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0 },
            Backups = new BackupsOptions { Enabled = true },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        using var meter = new Meter("TestOrphanSweep");
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(meter, TimeProvider.System);
        var seen = new List<(string Loop, bool Ok)>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name != "worker.loop.ticks")
                return;
            var pairs = tags.ToArray();
            seen.Add((
                pairs.First(t => t.Key == "loop").Value!.ToString()!,
                (bool)pairs.First(t => t.Key == "ok").Value!));
        });
        listener.InstrumentPublished = (instrument, enable) =>
        {
            if (instrument.Meter.Name == "TestOrphanSweep")
                enable.EnableMeasurementEvents(instrument);
        };
        listener.Start();
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Wrap(new EmptyS3()), UnitPoolFactory.Store(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Act: цикл идёт; ждём первого завершённого прохода лидера (поллинг ≤5 с).
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && seen.All(s => s.Loop != "orphan-sweep"); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: успешный проход отметил тик цикла orphan-sweep с ok=true.
        seen.Should().Contain(s => s.Loop == "orphan-sweep" && s.Ok);
    }
```

**Подпункт 4.2. Провальный проход — тик `ok=false`:**

Механизм (уточнение к spec §4 Фаза D.2: «S3-фейк, list которого возвращает
Result-ошибку»): единственный list, чья `Result`-ошибка проваливает `SweepAsync`, —
`IS3PoolStore.ListPoolNamesAsync` (первый шаг прохода: `BackupOrphanSweeper.SweepAsync`
сразу возвращает `Result.Failed`); ошибка `ListPrefixAsync` поглощается `continue`
(transient-паттерн arch/19 §5) и проход завершается успехом. Пул-стор — часть S3-слоя.

(а) Рядом с `EmptyS3` добавить приватный класс:

```csharp
    // Пул-стор S3 с ошибкой list пулов: SweepAsync сразу возвращает Result.Failed —
    // проход лидера провальный (ok=false; ListPrefixAsync-ошибку проход поглотил бы).
    private sealed class FaultingPoolStore : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string pool, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Failed(
                new ApplicationException("pool store unreachable")));
    }
```

(б) Добавить тест (после `ExecuteAsync_LeaderSweep_TicksOrphanSweepOkTrue`) — полный
код (отличия от теста 4.1(б): имя, AAA-комментарий, sweeper на `Disabled()` +
`FaultingPoolStore`, Assert на `!s.Ok`):

```csharp
    // AAA: провальный проход лидера (list пулов S3 вернул Result-ошибку) — тик цикла
    // orphan-sweep с ok=false; last_success не двигает (семантика закреплена
    // Shared.Metrics.UnitTests LoopTick_OkFalse_DoesNotMoveLastSuccess).
    [Fact]
    public async Task ExecuteAsync_LeaderSweepFails_TicksOrphanSweepOkFalse()
    {
        // Arrange: Enabled=true, лидерство возьмёт FakeEtcd-txn; list пулов S3
        // возвращает Result-ошибку → SweepAsync проваливается на первом шаге.
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0 },
            Backups = new BackupsOptions { Enabled = true },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        using var meter = new Meter("TestOrphanSweep");
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(meter, TimeProvider.System);
        var seen = new List<(string Loop, bool Ok)>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name != "worker.loop.ticks")
                return;
            var pairs = tags.ToArray();
            seen.Add((
                pairs.First(t => t.Key == "loop").Value!.ToString()!,
                (bool)pairs.First(t => t.Key == "ok").Value!));
        });
        listener.InstrumentPublished = (instrument, enable) =>
        {
            if (instrument.Meter.Name == "TestOrphanSweep")
                enable.EnableMeasurementEvents(instrument);
        };
        listener.Start();
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Disabled(), new FaultingPoolStore(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Act: цикл идёт; ждём первого завершённого (провального) прохода лидера
        // (поллинг ≤5 с).
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && seen.All(s => s.Loop != "orphan-sweep"); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: провальный проход отметил тик цикла orphan-sweep с ok=false.
        seen.Should().Contain(s => s.Loop == "orphan-sweep" && !s.Ok);
    }
```

**Выход:** все тесты класса `BackupOrphanSweeperLoopTests` (3 старых + 2 новых) зелёные.

**Проверка (две ОТДЕЛЬНЫЕ команды; фоном, лог в файл):**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/plan-orphan-sweep-build2.log 2>&1
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~PgWorker.UnitTests.App.BackupOrphanSweeperLoopTests" > /tmp/plan-orphan-sweep-unit.log 2>&1
```
Ожидание: `Passed! - Failed: 0` в логе теста (5 тестов класса).

**Связь со spec:** §4 Фаза D.2 (Фаза D.1 — подпункт 2.4 задачи 2), Д2, Д8; критерии приёмки 2–3.

**Коммит:** `test(metrics): юниты тиков orphan-sweep — ok=true (пустой S3) и ok=false (list пулов с ошибкой)`

---

### Задача 5 (Фаза E): гейты и закрытие пункта ревью

**Файлы:**
- Modify: `/Users/demakaev/ZCodeProject/pg/.my/review_project.md` (только шаг 5.4 — ОСНОВНОЙ репо, не worktree)

**Вход:** задачи 1–4 закоммичены, сборка Release зелёная.

**Действие (шаги 5.1–5.4 выполняет сам; все команды — фоном, логи в `/tmp`, фиксировать task_id):**

**5.1. Полные юниты PgWorker + Shared.Metrics** (сборка уже свежая после задачи 4):
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~PgWorker.UnitTests|FullyQualifiedName~Shared.Metrics.UnitTests" > /tmp/plan-orphan-sweep-units-full.log 2>&1
```
Ожидание: `Passed!` — 0 Failed (AC6). Мониторинг по правилам: старт/не завис — по логу
в первые 15 с; зависание >5 мин — онлайн-разбор логов без остановки.

**5.2. docker-E2E на свежем Release (AC7; менялся `src/PgWorker.App` — обязателен), три
команды ПО ПОРЯДКУ, каждая — отдельной командой:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/plan-orphan-sweep-e2eprepare.log 2>&1
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/plan-orphan-sweep-build3.log 2>&1
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/plan-orphan-sweep-e2e.log 2>&1
```
Ожидание: E2ePrepare — штамп готовности без ошибок; build — `0 Warning(s)`; тест —
`Passed!`. После прогона — страховочная зачистка (AGENTS.md): убедиться, что остаточных
контейнеров/сетей прогона нет (`docker ps -a --format '{{.Names}}' | grep -c pgw-` → 0;
при остатках — разбор по логу прогона, затем `docker network prune -f`).
Полную серию E2eFixture не гонять (provisioning/portalloc/moves не тронуты).

**5.3. E2E-чек стенда 65-metrics.sh — ТОЛЬКО при поднятом стенде** (AC5). Проверка
живости стенда: `curl -fsS -m 3 http://localhost:9090/-/ready` (Prometheus стенда).
- Стенд поднят → `bash dev-stand/adminpanel/checks/65-metrics.sh > /tmp/plan-orphan-sweep-check65.log 2>&1`:
  в логе `rules: 22 alerts registered` (значение ≥21 чека проходит), отсутствие `❌`.
- Стенд НЕ поднят → шаг пропустить, зафиксировав в журнале задачи «чек 65 не прогонян:
  стенд не поднят» (условие spec §4 Фаза E.3); не поднимать стенд самовольно.

**5.4. Закрытие пункта ревью #4 — файл ОСНОВНОГО репо (НЕ worktree):
`/Users/demakaev/ZCodeProject/pg/.my/review_project.md`.** Правка вручную (файл не под
git-контролем воркtree; коммитов в main НЕ делать):
- удалить строку 30 таблицы «Что найдено»: `| [4](#p4) | 🟠 Major | Л-obs | Слепая зона
  алертов на свипере сирот: ... |`;
- удалить строки 77–93 целиком: пустую строку 77 (перед `<a id="p4"></a>`), якорь
  `<a id="p4"></a>` (строка 78), заголовок `## 🟠 #4. Слепая зона алертов на свипере
  сирот: канон обещает то, чего нет` (строка 79), все строки секции
  (Суть/Механизм/Проверка/Как чинится) и завершающий секцию `---` (строка 93).
  Разделитель `---` строки 76 ОСТАЁТСЯ: итог между последней строкой секции #3
  (строка 74) и `<a id="p5"></a>` (строка 95) — пустая строка, ровно один `---`,
  пустая строка (бывшая строка 94).
- Нумерацию остальных пунктов и якоря p1–p3, p5+ НЕ менять (AGENTS.base §16); строку 6
  (сводка шапки) НЕ трогать — историческая сводка ревью, не пункт.

**Выход:** все гейты spec пройдены; пункт ревью #4 закрыт.

**Проверка:**
- `grep -c "id=\"p4\"\|Слепая зона алертов" /Users/demakaev/ZCodeProject/pg/.my/review_project.md` → `0`
  (проверять только по факту правки 5.4; шапка-строка 6 исключается точным паттерном
  «Слепая зона алертов» с заглавной — если grep зацепил её, это НЕ дефект: сверить,
  что остались только строки шапки, а не секция).
- Логи 5.1–5.3 прочитаны из файлов; все финальные строки — успех (или обоснованный
  пропуск 5.3).

**Связь со spec:** §4 Фаза E.1–E.4; критерии приёмки 5–8.

**Коммит:** нет (правка 5.4 — вне git; коммиты задач 1–4 уже сделаны).

---

## Соответствие критериям приёмки spec (§7)

| AC | Где закрывается |
|---|---|
| 1 (arch/18+arch/19) | Задача 1 (+ проверка grep) |
| 2 (серии лидера после первого прохода) | Задачи 2+4 (тест `...OkTrue`) |
| 3 (ok=false, last_success стоит) | Задача 4 (`...OkFalse`) + `LoopTick_OkFalse_DoesNotMoveLastSuccess` (уже есть в Shared.Metrics.UnitTests) |
| 4 (не-лидер/выключено — серии нет) | Задача 2 (единственная точка вызова; ревью) |
| 5 (правило, 22 правила) | Задача 3 + 5.3 (при стенде) |
| 6 (юниты + сборка) | Задачи 2/4 (сборка), 5.1 |
| 7 (docker-E2E Scale_AddEmptyShard) | 5.2 |
| 8 (пункт #4 удалён) | 5.4 |

## Отчёт исполнителя

По завершении вернуть main-агенту: список коммитов (задачи 1–4), итоги гейтов 5.1–5.3
(финальные строки логов / обоснованный пропуск 5.3), факт правки 5.4. При любом падении —
полный лог из файла + разбор причин (перезапуск упавших тестов без анализа запрещён).
