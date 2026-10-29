# Spec: живость свипера S3-сирот в контракте наблюдаемости (ревью #4)

## 1. Цель

Закрыть слепую зону алертов на `BackupOrphanSweeperLoop` (единственный уборщик
S3-сирот PgWorker): сегодня arch/18 §2.2 заявляет, что подавленная op
`backup-orphan-sweeper` не эмитит фазовых серий и её живость закрывает
`worker_loop_last_success_timestamp_seconds` (алерт WorkerLoopStalled), — но
фактически цикл метрик не инжектирует вовсе, серии `orphan-sweep` не существует
ни в одном воркере, а WorkerLoopStalled матчит только
`{loop=~"reconcile|keepalive"}`. Затык или смерть уборщика (тип тика
`Supervisor:IntervalSec` = 600 с, TTL-удаление сирот) невидимы для
Prometheus/Grafana/алертов.

Фикс arch-first: сначала контракт arch/18 §2.2 (+ ссылка в arch/19 §4), затем
инструментация цикла (`LoopTick("orphan-sweep", ok)` по итогам `SweepAsync`) и
отдельное алерт-правило `OrphanSweepStalled`.

## 2. Принципы

- **arch-first**: `arch/` — единственный источник истины; правка контракта
  наблюдаемости начинается в arch/18/arch/19, код зеркалит его (AGENTS.base §1).
- **Метрики — пассивные наблюдатели**: инструментация никогда не влияет на
  поведение цикла (никаких throw наружу — уже гарантирует
  `WorkerMetricsInstrumentation.LoopTick`).
- **Никаких новых имён серий/лейблов**: используются только серии словаря
  §2.2 (`worker_loop_ticks_total`, `worker_loop_last_success_timestamp_seconds`,
  `worker_loop_duration_seconds`) с лейблом `loop="orphan-sweep"`.
- **Слоёщаяся observability**: алерт `OrphanSweepStalled` ловит «процесс жив, а
  уборка не делается»; смерть всего процесса остаётся за `ServiceDown`
  (`up == 0`); внутренний watchdog/healthz не заменяются, а дополняются
  внешним Prometheus-каналом.
- **Формат правил — по образцу соседних**: отдельное `*Stalled`-правило с
  фиксированным порогом ≥3× интервала тика (паттерн Kafka/Valkey/
  SdGenerator CollectorStalled в `rules.yml`).

## 3. Структура/компоненты (что меняем)

### 3.1. arch/18-metrics.md §2.2 (первая правка, до кода)

1. Таблица §2.2, строка `worker_loop_ticks_total`: перечень циклов
   «(Reconcile/Keepalive/Snapshot)» → «(Reconcile/Keepalive/Snapshot/
   OrphanSweep)».
2. Таблица §2.2, строка `worker_loop_last_success_timestamp_seconds` — после
   текста про snapshot-цикл добавить: цикл `orphan-sweep`
   (BackupOrphanSweeperLoop, тик лидера раз в `Backups:Supervisor:IntervalSec`,
   дефолт 600 с; `ok` = успех прохода `SweepAsync`) — живость закрывает
   отдельное правило **OrphanSweepStalled** (`time() -
   worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"} > 1800`,
   порог 3× дефолтного интервала; серия эмитится только лидером при
   `Backups:Enabled=true` — выключенная подсистема не алертится, смерть
   процесса ловит ServiceDown).
3. Абзац §2.2 о стационарных ops без терминальной фазы (SuppressedOps, ныне
   «…их живость закрывает `worker_loop_last_success_timestamp_seconds` (алерт
   WorkerLoopStalled), вечные серии не копятся») — уточнить раздельно:
   `supervise`/`evacuate` — живость клэйм-циклов (`loop="reconcile"` → алерт
   WorkerLoopStalled); `backup-orphan-sweeper` — живость цикла
   `orphan-sweep` (LoopTick по итогам SweepAsync → алерт OrphanSweepStalled).

### 3.2. arch/19-backups.md §4

В строку таблицы ключа `/pgworker/backups/orphans` (фрагмент «пишет ТОЛЬКО
глобальный лидер-проход `/pgworker/leader` (паттерн storage/supervisor)»)
добавить одно предложение: «живость лидер-прохода наблюдаема серией
`worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"}` (тик цикла по
итогам прохода; алерт OrphanSweepStalled — [18](18-metrics.md) §2.2)». Без
пересказа словаря arch/18 — только ссылка.

### 3.3. Код: src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs

- В primary-конструктор добавить параметр
  `Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics` (после
  `sweeper`, симметрично SnapshotLoop).
- В лидерской ветке (`claims.IsLeader && Backups.Enabled`) обернуть
  `sweeper.SweepAsync(tick)` в `Stopwatch.GetTimestamp()/
  GetElapsedTime` и после получения `result`:
  - `metrics.LoopDuration("orphan-sweep", elapsed.TotalSeconds);`
  - `metrics.LoopTick("orphan-sweep", ok: result.IsSuccess);`
- Точки WITHOUT метрик: не-лидерская ветка, `Backups:Enabled=false` (no-op),
  ветка потери leader-lease (`OperationCanceledException` при живом
  `stoppingToken`) — проход не завершён/не выполнялся, серию не пишем.
- Существующие `HealthState.MarkOrphanSweep*`, пульс активности и
  `PulsingDelay` не трогаем.

### 3.4. Код: src/PgWorker.App/Program.cs (DI, строки ~779–784)

В фабрику `BackupOrphanSweeperLoop` добавить
`sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()`
(синглтон зарегистрирован строками ~99–109).

### 3.5. Код: src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs

Только комментарии (контракт без изменения поведения):
- комментарий `SuppressedOps` (~строки 18–24): живость `backup-orphan-sweeper`
  закрывает тик цикла `orphan-sweep` (алерт OrphanSweepStalled), а не
  WorkerLoopStalled;
- комментарий `OnJournalPhase` (~строки 546–549): аналогичное уточнение для
  ветки подавленных ops.

### 3.6. dev-stand/adminpanel/metrics/prometheus/rules.yml, группа `workers`

Новое правило сразу после `WorkerLoopStalled` (WorkerLoopStalled не меняем —
порог 60 с несовместим с интервалом 600 с):

```yaml
- alert: OrphanSweepStalled
  expr: time() - worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"} > 1800
  for: 0m
  labels: {severity: warning}
  annotations:
    summary: "{{ $labels.job }} orphan sweeper not sweeping for >30m"
    description: "fixed threshold 3x Supervisor:IntervalSec (600s default); series emitted only by the leader with Backups:Enabled=true; runbook — arch/18 §2.2, arch/19 §4"
```

### 3.7. dev-stand/adminpanel/checks/65-metrics.sh

Порог чека `rules >= 21` остаётся валидным (правил станет 22); обновить только
устаревающие комментарии/echo: строка ~150 «21 алерт: 11 + 7 группы backups
§2.7 + 3 t15 patroni-nodes» → «22 алерта: 12 + 7 + 3» и echo-строку ~153.

### 3.8. НЕ меняем (границы)

- `WorkerLoopStalled` (регэкс/порог), `SnapshotStale`, прочие правила.
- Дашборд `workers.json`: панель «Loop staleness»
  (`time() - worker_loop_last_success_timestamp_seconds`) подхватит серию
  `orphan-sweep` автоматически — правка не нужна.
- `HealthState`/watchdog/`LoopStaleness` — имя канала `orphan-sweep` уже
  канонизировано (`LoopStaleness.cs`, `LoopsVitalityTests`), переиспользуем.
- KafkaWorker/S3Worker/ValkeyWorker — свипер сирот существует только в
  PgWorker, ничего не добавляем.

## 4. Фазы

**Фаза A — arch (worktree, до кода).** Правки 3.1–3.2. Гейт: формулировки
§2.2 не обещают несуществующих серий; grep по arch/18 §2.2 не находит
утверждения «живость backup-orphan-sweeper закрывает WorkerLoopStalled» без
оговорки про отдельный цикл/алерт.

**Фаза B — код.** Правки 3.3–3.5. Сборка `DOTNET_CLI_UI_LANGUAGE=en dotnet
build src/PgWorker.slnx -c Release --nologo` — без варнингов
(`TreatWarningsAsErrors=true`).

**Фаза C — алерты/чек стенда.** Правки 3.6–3.7. Синтаксис YAML валиден
(правило в стиле соседних).

**Фаза D — тесты.**
1. Обновить `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`:
   конструктор цикла получает настоящий
   `new WorkerMetricsInstrumentation(new Meter("TestOrphanSweep"),
   TimeProvider.System)` — паттерн `ReconcileLoopTests` (строки ~35–36).
2. Новый юнит-тест в том же файле (AAA-комментарии по канону):
   - **успешный проход**: `Backups.Enabled=true`, FakeEtcd-лидер, S3-фейк с
     быстрыми пустыми list-ответами (по образцу `UnitPoolFactory.Wrap`,
     `HangingS3` не подходит — нужен завершающийся фейк); MeterListener на
     инструмент `worker.loop.ticks` фиксирует
     `{loop="orphan-sweep", ok="true"}`;
   - **провальный проход**: S3-фейк, list которого возвращает
     `Result`-ошибку; listener фиксирует `{loop="orphan-sweep", ok="false"}`;
     семантика «ok=false не двигает last_success» уже покрыта
     `Shared.Metrics.UnitTests` (`LoopTick_OkFalse_DoesNotMoveLastSuccess`).
3. Интеграционный `PgWorker.IntegrationTests/Api/MetricsTests.cs` — БЕЗ
   обязательного ассерта `orphan-sweep`: серия требует лидерства, `Enabled=true`
   и завершённого прохода SweepAsync в WAF-фикстуре — хрупкая зависимость
   (решение зафиксировано в §6).

**Фаза E — гейты.**
1. Юниты PgWorker + Shared.Metrics: `DOTNET_CLI_UI_LANGUAGE=en dotnet test
   src/PgWorker.slnx -c Release --no-build --filter
   FullyQualifiedName~PgWorker.UnitTests` (+ Shared.Metrics.UnitTests).
2. Меняется `src/PgWorker.App` → обязательный docker-E2E на свежем Release
   (AGENTS.md): сначала `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
   src/tests/PgWorker.E2ePrepare -c Release`, затем сборка и тесты двумя
   ОТДЕЛЬНЫМИ командами: минимум
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
   -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`.
   Полный E2eFixture не требуется (provisioning/portalloc/moves не тронуты).
3. E2E-чек стенда `checks/65-metrics.sh` — при поднятом стенде: 22 правила,
   группа workers содержит OrphanSweepStalled.
4. Закрытие пункта ревью: удалить раздел «#4. Слепая зона алертов на свипере
   сирот» (якорь p4) из `/Users/demakaev/ZCodeProject/pg/.my/review_project.md`
   (в worktree файла нет; правка в основном репо, нумерация остальных пунктов
   не меняется — AGENTS.base §16).

## 5. Ограничения

- Порог 1800 с фиксирован в правиле и привязан к ДЕФОЛТУ
  `Supervisor:IntervalSec=600` (валидация конфига допускает ≥60 с): повышение
  интервала оператором сверх 600 с потребует правки порога — та же практика,
  что у `BackupWalStale`/`BackupWalLagHigh` (порог = дефолт per-install
  конфига, пометка в description правила).
- Серия `orphan-sweep` появляется только после ПЕРВОГО завершённого прохода
  лидера (ObservableGauge эмитит существующий стейт): смерть воркера до первого
  прохода ловится `ServiceDown` — консистентно с reconcile/keepalive.
- Никаких absent()-условий в правиле: отсутствие серии = «подсистема выключена
  или инстанс не лидер» — штатные состояния, не алерт (см. §6, решение Д4).
- Язык документации — русский; идентификаторы — английские; в правки arch/ не
  добавлять исторических пассажей/атрибуций.

## 6. Принятые решения (закрытые развилки)

- **Д1. Имя лейбла цикла — `orphan-sweep`.** Уже канонизировано в проекте как
  имя watchdog-канала (`Shared.Core/HealthChecks/LoopStaleness.cs` «Быстрые
  циклы (reconcile/keepalive/orphan-sweep)»; `LoopsVitalityTests`:
  `Equal("reconcile", "keepalive", "snapshot", "orphan-sweep")`) и совпадает с
  предложением ревью. Альтернатива `backup-orphan-sweeper` отвергнута: это имя
  op-журнала, а лейбл `loop` в §2.2 — короткие имена циклов.
- **Д2. Семантика тика — `ok = SweepAsync.IsSuccess`, только по итогам
  завершённого прохода лидера.** Не-лидер и `Backups:Enabled=false` серию не
  пишут. Это единственная семантика, закрывающая сценарий ревью «процесс жив,
  а лидерство утрачено/подвисло или проход стабильно падает»: last_success
  стоит/отсутствует → алерт горит. Тик `ok=true` в не-лидерской ветке (по
  образцу SnapshotLoop) скрывал бы смерть уборки за живостью цикла — отвергнуто.
  Смерть воркера целиком остаётся за ServiceDown (слоёщаяся защита).
- **Д3. Отдельное правило OrphanSweepStalled вместо расширения регэкспа
  WorkerLoopStalled.** Порог 60 с несовместим с интервалом 600 с (расширение
  регэкспа давало бы вечный firing). Порог 1800 с = 3× дефолтного
  `Supervisor:IntervalSec` — паттерн проекта «fixed threshold ≥3× интервала»
  (KafkaCollectorStalled/ValkeyCollectorStalled/SdGeneratorStalled). `for: 0m`,
  группа `workers` (семейство живости циклов), `job`-agnostic — свипер есть
  только у job `pgworker`, но фильтр по job не добавляем (серия сама существует
  только у PgWorker; консистентно с WorkerLoopStalled).
- **Д4. Severity — `warning` (решение принято без ответа пользователя — см.
  open_questions в сводке main-агента).** Обоснование: деградация медленная и
  обратимая — S3-сироты копятся (утечка места), данные не теряются; зеркало
  всех медленных Stalled-алертов проекта (Kafka/Valkey/SdGenerator — warning с
  порогом ≥3× интервала). Critical оставлен только WorkerLoopStalled (сердце
  системы, тик 5 с). Смена severity — одна строка в rules.yml + §2.2 arch/18.
- **Д5. `LoopDuration("orphan-sweep", seconds)` — добавляем (решение принято
  без ответа пользователя — см. open_questions).** Обоснование: паттерн
  инструментов «duration + tick» циклов Reconcile/Snapshot; проход свипера —
  тяжёлый (list всего bucket, batch-delete), его длительность — прямая
  диагностика «проход затянулся» при разборе горящего алерта; серия
  `worker_loop_duration_seconds{loop}` уже в словаре §2.2, новых имён нет.
  KeepaliveLoop (без duration) — не образец: его тики мгновенны.
- **Д6. `worker_loop_ticks_total` (строка таблицы §2.2) — циклы
  Reconcile/Keepalive/Snapshot/OrphanSweep.** Счётчик пишется тем же LoopTick —
  изменение перечня в описании обязательно, иначе словарь врёт.
- **Д7. Комментарии SuppressedOps/OnJournalPhase в WorkerMetricsInstrumentation
  обновляем.** Они дословно обещают «живость надзора закрывает
  WorkerLoopStalled» — после фикса это ложь для backup-orphan-sweeper;
  дезинформирующий комментарий — тот же класс дефекта, что и расхождение arch
  (ревью #6 — прецедент).
- **Д8. Интеграционный MetricsTests без ассерта orphan-sweep** (см. Фаза D.3):
  детерминированность важнее формального покрытия; канон имён §2.2 остаётся
  зафиксирован юнит-уровнем (MeterListener) и фактическими сериями стенда.
- **Д9. Чек 65-metrics.sh — только комментарии.** Порог `>= 21` остаётся
  (22 ≥ 21), ужесточение до `>= 22` не делаем: чек описывает минимум канона,
  а не точный счётчик (иначе каждая новая задача метрик правит чек).

## 7. Критерии приёмки

1. arch/18 §2.2: описания серий `worker_loop_ticks_total` /
   `worker_loop_last_success_timestamp_seconds` и абзац SuppressedOps
   соответствуют фактическому поведению (grep «WorkerLoopStalled» по §2.2 не
   оставляет заявления о покрытии живости backup-orphan-sweeper без оговорки);
   arch/19 §4 содержит ссылку на серию `orphan-sweep` и алерт
   OrphanSweepStalled.
2. Лидер PgWorker с `Backups:Enabled=true` после первого завершённого прохода
   эмитит `worker_loop_ticks_total{loop="orphan-sweep",ok=…}` и
   `worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"}` (юнит-тест
   с MeterListener; на стенде — панель «Loop staleness»).
3. Провальный проход: тик `ok="false"`, last_success не двигается (юнит +
   семантика Shared.Metrics.UnitTests).
4. Не-лидер / `Backups:Enabled=false`: серии `orphan-sweep` у инстанса нет
   (код-ревью по точкам вызова 3.3).
5. `rules.yml`: правило OrphanSweepStalled (warning, порог 1800) в группе
   workers; Prometheus регистрирует 22 alerting-правила (чек 65 при стенде).
6. Юниты PgWorker.UnitTests + Shared.Metrics.UnitTests зелёные; сборка Release
   без варнингов.
7. docker-E2E `Scale_AddEmptyShard` на свежем Release (после E2ePrepare)
   зелёный — регрессий провижининга нет.
8. Пункт #4 удалён из `/Users/demakaev/ZCodeProject/pg/.my/review_project.md`,
   нумерация остальных пунктов не тронута.
