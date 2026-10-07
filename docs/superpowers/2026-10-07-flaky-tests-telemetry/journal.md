# Журнал t29-flaky-tests: исполнение плана + приёмка

Worktree: `feat-flaky-tests-telemetry`. Коммиты задач 1–10 — по одному на
задачу (см. `git log`); ниже — приёмка (план задача 11, spec §8).

## Приёмка

### 1. Полная сборка решения

`dotnet build src/PgWorker.slnx -c Release` — **0 Error(s), 0 Warning(s)**.

### 2. AC8 — гейт неизменности бюджетов

`git diff main..HEAD -- src/tests/PgWorker.IntegrationTests | grep -E '^[+-].*(FromSeconds|FromMinutes|FromMilliseconds|Task\.Delay|CancelAfter)'`:

- все существующие бюджеты — парные переносы «−/+» без изменения числа:
  FromMinutes(10) ×4 (статические build/publish), 360 ×4 (provisioning-фазы
  Move/Wal/HaEtcd/Si2 и shard3-фазы HaEtcd/Si2), 300 ×2 (wal history/glue),
  120 ×2 (si2-a3-started, wal-tli-ready), 100 (si2-instances-up), 30
  (cert-discovery), 15 (move-finalize-op), 10 (ac5-failover);
- новые числа — ТОЛЬКО из белого списка плана: `E2ePhase`
  SlowPhaseThreshold=60 с / TickInterval=5 с / PollInterval=500 мс +
  `Task.Delay(PollInterval)`, `DrainAsync`-страховка дренажа 5 с, явный
  `FromMinutes(2)` у docker build SecondInstance (перенос дефолтного
  120-с бюджета той же перегрузки);
- удаление `- if (sw.Elapsed > TimeSpan.FromSeconds(60))` — порог приватной
  копии WaitPhaseAsync (E2eScenarios), перенесён в `E2ePhase` (см. белый
  список);
- ассерты `Elapsed.Should/BeLessThanOrEqualTo` — диф пуст (нетронуты);
  HaEtcd-пробы 5×2 с, WaitReady 30×1 с, probe 3 с, etcd-контур 100 с — в
  диф не попали (не менялись).

Гейт пройден.

### 3. AC7 — контрольный не-E2E прогон

`PGW_TEST_DOCKER=1 dotnet test … --filter RestoreDrillProcessTests|EtcdContractTests|EtcdCoordinationTests`:
**Passed 34 / Failed 0 (37 с)** — транзиентный таймаут пробы ретрай-цикл не
роняет. После финальной строки — чистота: контейнеры/сети/тома pgw-* = 0/0/0.

### 4. AC10 — E2E t29-прогон на каноническом N=5

Фильтр всех 6 t29-классов, обычный рабочий фон (рядом шёл разбор старых
артефактов, никаких условий/ожиданий):

**Test Run Successful: 7/7 passed, 6,32 мин** (Acceptance 6 м 17 с;
Move 2 м 21 с; WalStream 2 м 45 с; HaEtcd 1 м 52 с; Si2 1 м 55 с;
WorkerCert 19 с + 4 с). Упавших нет — перезапуски не потребовались.

Полнота телеметрии (7 контуров, идентифицированы по содержимому):

- `phases.log` есть в контуре каждого класса с фазовыми ожиданиями;
  суммарно 48 строк `[PHASE] … budget=…` — все фазы всех классов
  (WorkerCert-Lifecycle 4, BrokenKey 0 — сценарий без фаз, Si2 4, Move 14,
  HaEtcd 3, Wal 8, Acceptance 15; счёт совпадает с ожидаемым по коду);
- прогресс-тики `[PHASE-TICK]` каждые 5 с: move-provisioning (динамика
  PROVISIONING→RUNNING), wal-history-present (listing), ac5-failover;
- снапшоты в итоговых строках: `src=66, dst=66` (sub_rb), `api=2,
  instances=2` (si2), `tli=2` (wal), `primary=shard1a, master-key=есть`
  (ac5), state-ключи+dsn+work (haetcd/si2 shard3);
- AC3: slow-phase-сбор сработал у обеих фаз > 60 с — артефакты
  `20261007-111957-slow-phase-wal-history-present.txt` (фаза 79,7 с) и
  `20261007-112132-slow-phase-ac5-rebuild.txt` (фаза 70,0 с);
- AC4: failed-phase-сбор не срабатывал (ок=False не было — все фазы ok=True);
  механизм включён кодом задачи 1 (фейл-путь);
- наблюдение: `[PROBE]`-строки HaEtcd (Console.WriteLine, класс без
  ITestOutputHelper — код по плану) идут в stdout процесса xunit и vstest
  detailed-лог их не капитурит (как и Console-строки E2ePhase); фазы и тики
  гарантированно наблюдаемы в `phases.log` контура.

### 5. AC5/AC6 по артефактам прогона

- AC5-разложение в журнале Acceptance (блок Standard Output):
  `[PHASE] ac5-docker-stop: elapsed=3,2s` и `AC5 … leader failover took
  3193 ms` — ассерт ≤ 5 с пройден от того же T0 (3193 ≤ 5000 мс); фаза
  `ac5-failover: ok=True, budget=10s, progress=primary=shard1a,
  master-key=есть` — в phases.log.
- AC6: build уложился (13 с), полный вывод записан в
  `pgw-e2e-artifacts-7b55…/process-build-7b55ae15.log`; метки старта/финиша
  `[PHASE] build pgworker:e2e-7b55ae15: старт … / 13 c` — в журнале
  (Console.Error). Хвост при таймауте — код-путь задачи 2 (DrainAsync +
  OutputTail 40) проверен ревью; в прогоне не срабатывал (таймаута не было).

### 6. Зачистка

После финальной строки серии: контейнеры pgw-* = 0, сети pgw-|kfw-net = 0,
тома pgw-* = 0. До серии — те же нули (упавших сценариев нет, MarkFailed-режим
не включался, ручная зачистка не требовалась).

## Сверка с критериями приёмки (spec §8)

| AC | Статус | Основание |
|---|---|---|
| AC1 место сбоя видно | ✓ | фаза+elapsed+budget+тики в phases.log каждого контура (п.4) |
| AC2 единый формат + phases.log | ✓ | 48 строк единого формата; phases.log во всех контурах классов с фазами |
| AC3 порог 60 с всюду в t29 | ✓ | slow-phase-* артефакты двух фаз > 60 с (wal-history 79,7 с; ac5-rebuild 70,0 с) |
| AC4 провал окна = картина момента | ✓ (код-путь) | failed-phase-сбор в E2ePhase до возврата; в прогоне ok=False не случилось |
| AC5 AC5-разложение | ✓ | ac5-docker-stop 3,2 с отдельной строкой; failover 3193 мс; ассерт нетронут (п.2) |
| AC6 build с хвостом | ✓ | process-build-*.log + метки старта/финиша; таймаут-путь — ревью задачи 2 |
| AC7 ретрай готовности etcd | ✓ | не-E2E серия 34/34 зелёная (п.3) |
| AC8 бюджеты не тронуты | ✓ | гейт п.2: парные переносы + белый список |
| AC9 документация | ✓ | docs/e2e-launch.md обновлён; arch/** не тронут |
| AC10 валидация телеметрии | ✓ | N=5, 7/7, полнота по всем фазам (п.4) |

Итог: цель spec достигнута — по логам/артефактам прогона любого t29-класса
видно место торможения/сбоя (фаза, elapsed, budget, прогресс-снапшот,
последний успешный шаг) без перезапуска тестов и без чтения кода.

## Приёмка-дополнение §3.9/§3.10 (задачи 12–14, база диффа ce0bf7c)

После подтяжки свежего main (t28) спека дополнена уликами §3.9
(WalStream-MasterDown) и §3.10 (Strict); мигрированы два оставшихся класса.

### 12. `E2eWalStreamMasterDownScenarios` (spec §3.9)

Все 7 ожиданий + repair-цикл → `E2ePhase` (8 точек): `wal-ac4-provisioning`
360 с, `wal-ac4-master-resolved` 120 с (×3 исполнения), `wal-ac4-agents-up`
300 с, `wal-ac4-repair` 600 с с 30-с окнами внутри condition (каденция
тиков ≈ окно, числа дословно), `wal-ac4-delivery-grew` 300 с (+прогресс
листинга S3 `objects=N (before=M)`), `wal-ac4-key-seen` 30 с,
`wal-ac4-new-agent` 300 с, `wal-ac4-tli-ready` 120 с (хелпер, в факте не
вызывается — для единообразия класса). Дамп → `ArtifactsDir/wal-diag-<cluster>.txt`.
Ассерт на repair НЕ добавлялся.

### 13. `E2eStrictScenarios` (spec §3.10)

6 точек / 7 исполнений: `strict-provisioning` 360 с (×2; прогресс — state-ключи
нод с учётом replicas и dsn), `strict-active` 60 с (×2; прогресс
`state=<висящий|нет (Active)>`), `strict-dcs-converged` 360 с (прогресс
work-ключа: `work.phase=dcs-converge, strict-патч=есть`), `strict-discovery`
30 с (×2; прогресс `api-keys=N`). Patroni GET /config — только условие фазы
конвергенции; в тиках — etcd-чтения.

### 14. Приёмка-дополнение

1. `dotnet build src/PgWorker.slnx -c Release` — 0 Error(s), 0 Warning(s).
2. AC8-гейт (`git diff ce0bf7c..HEAD` по двум файлам, белый список НЕ
   применяется): ТОЛЬКО парные переносы — Strict 360/60/360/360/60/30;
   MasterDown 600 (repairDeadline→бюджет фазы), Task.Delay(30) дословно,
   300 (delivery), 120 (tli-хелпер). Ассерты (`Should().BeTrue`,
   `NotBe`, `IsContinuous`) — диф пуст.
3. AC10 — E2E t29-фильтром полного перечня §3 (8 классов, 10 фактов),
   канонический N=5, обычный рабочий фон:
   **Test Run Successful: 10/10 passed, 12,0 мин** (Acceptance 5 м 49 с;
   Move 2 м 26 с; Wal-Promote 2 м 38 с; Wal-MasterDown ~11 мин; HaEtcd
   2 м 4 с; Si2 2 м 9 с; WorkerCert 25 с + 11 с; Strict 1 м 1 с + 32 с).
   Полнота телеметрии:
   - все фазы новых классов в `phases.log` с budget (список выше) + тики:
     `wal-ac4-repair t=30,0s…581,1s: unreachable=да, окно 1…19` (каденция
     ≈30,5 с — окна видны), `strict-provisioning … [PROVISIONING]→[RUNNING]`
     (однорепликный shard2 теста 2 виден как `[PROVISIONING]`);
   - AC3: `20261007-120828-slow-phase-wal-ac4-repair.txt` (фаза > 60 с);
   - AC4: `20261007-121739-failed-phase-wal-ac4-repair.txt` — реальное
     срабатывание немедленного сбора при провале окна: `wal-ac4-repair
     ok=False, elapsed=612,7s, budget=600s` (нода НЕ восстановилась за 19
     окон), при этом ТЕСТ ЗЕЛЁНЫЙ: repair-фаза не ассертится (итог — только
     телеметрия, по плану), доставка выросла (`objects=3 (before=0)`),
     ключ жив, агент новой мастер-ноды running — полная картина
     «недовосстановление ноды + зелёный тест» в артефактах без перезапуска;
   - тики delivery-гейта и discovery (api-keys=1) — в phases.log контуров.
4. Чистота: до серии 0/0/0 и после финальной строки 0/0/0 (контейнеры/
   сети/тома); упавших сценариев нет, MarkFailed не срабатывал, дампы
   wal-diag не создавались (фейл-путь не понадобился).

Итог дополнения: AC1–AC4/AC8/AC10 подтверждены на полном перечне §3
(10 фактов, включая оба новых класса); полнота телеметрии — по артефактам
каждого контура.

