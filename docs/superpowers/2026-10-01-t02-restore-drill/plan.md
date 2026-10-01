# t02-restore-drill — план реализации (плановое тестовое восстановление по расписанию)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять план задача за задачей. Шаги отмечаются чекбоксами (`- [ ]`).

**Цель:** RestoreDrillProcess воркера по расписанию разворачивает бэкап реального шарда в изолированный ephemeral-контур (контейнер+volume без сети), контролирует «старт postgres + выход из recovery», фиксирует исход в etcd-ключе `drill` с доводимой фазой сноса `cleaning`, сносит контур при любом исходе; панель — алерты `backup-drill-failed`/`backup-drill-stale`, per-shard статус и мутация интервала; runbook + docker-E2E + мерж-гейт трека reliability.

**Архитектура:** тиковая машина `RestoreDrillProcess` под клэймом `<C>` (Op `backup-drill`, образец `BackupVerifyProcess`) + ephemeral drill-джоб `pgw-backup-drill-<C>-<X>-<id>` (образ `pgworker-backup`, механика restore-джоба t05 целиком — обёртки, см. «Решения гейтов»); состояние — один etcd-ключ `/pgworker/backups/<C>/<X>/drill` (перезаписывается, история — в журнале воркера); отбор «один шард за проход, наименее свежий по дриллу» — чистая функция `DrillPlanner.SelectCandidate`; интервал per-cluster `policy.drill.interval_days` (дефолт `Drill:IntervalDays=1`, 0 — выкл); панель читает снапшот и мутирует интервал через команду-прокси в policy-API воркера (панель в etcd не пишет).

**Стек:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; xUnit + FluentAssertions; testcontainers (OwnEtcd, E2eEnvironment); React/Mantine (панель).

**Spec:** [`spec.md`](spec.md) — план аргументирует от spec; исполнители читают ОБА документа. Каноны: [arch/19-backups.md](../../../arch/19-backups.md), [arch/adminpanel/02-etcd-contract.md](../../../arch/adminpanel/02-etcd-contract.md), [arch/14-pgworker.md](../../../arch/14-pgworker.md) §1.1, [AGENTS.base.md](../../../AGENTS.base.md) §12 (E2E), [AGENTS.md](../../../AGENTS.md).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/t02-restore-drill` (ветка `t02-restore-drill`); все пути ниже — относительно корня worktree. Команды `dotnet` выполнять из корня worktree.

## Решения гейтов (фиксируются кодом, не пересуживать)

- **DrillJobCommand/DrillJobLog — обёртки (решение пользователя, гейт плана 2026-10-01)**: `DrillJobCommand.Build()` делегирует `Restore.RestoreJobCommand.Build()`; `DrillJobLog.Parse(logs)` делегирует `Restore.RestoreJobLog.Parse(logs)`. Один bash-скрипт механики t05 на restore и дрилл — будущие инцидентные правки restore-механики автоматически достаются дриллу (spec §2.2 «дрилл доказывает ровно тот путь, который сработает при реальном DR»). Лишнее поле `system_id` в result джоба воркер дрилла игнорирует (в ключ `drill` не попадает); `TARGET_TIME=""` даёт latest.
- Решения пользователя из spec (шапка spec, п.1–5): процесс+ephemeral джоб; один шард за проход (least-recently-drilled); `drill.interval_days` (0 — выкл); контроль «старт + выход из recovery» (SQL-проверок данных нет); изоляция без нод в кластере/HA-scope; снос — доводимая фаза `cleaning` в etcd, переживает рестарт воркера.

## Глобальные ограничения

- **Arch-first**: правки `arch/` — ПЕРЕД кодом (Task 1, отдельный ранний коммит); код зеркалит канон.
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; центральное версионирование — `Directory.Packages.props` (новые пакеты НЕ вводить).
- Комментарии/документация — по-русски; идентификаторы — на английском; тесты — с AAA-комментариями.
- Порты docker-контейнеров в тестах — ТОЛЬКО динамические (`assignRandomHostPort: true` / `GetMappedPublicPort`); никаких литералов-портов.
- Каждый интеграционный/E2E-тест: своё окружение (guid в именах), полный teardown при любом исходе, ассерт чистоты. `BrokerBootSec`/`AgentBootSec` ≤ 100 с; ожидания воркера в тестах — через `E2eFixture.WaitForAsync`.
- После КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (сводный гейт Task 16, страховой после каждой серии).
- Правки чужих подсистем (restore t05, verify t04, retention t06, supervisor t07, сироты) — НЕ делаются: дрилл только читатель S3 и своих ключей/контейнера/volume.
- Все коммиты — в ветке `t02-restore-drill`; в `main` не мержить (мерж — отдельной командой пользователя после гейта Task 16).
- Точка входа тестов: `dotnet test src/PgWorker.slnx -c Release` (+ `PGW_TEST_DOCKER=1` для docker-серий; `DOTNET_CLI_UI_LANGUAGE=en`).

---

### Task 1: Arch-first — канон дрилла (arch/19, adminpanel/02, arch/14)

**Files:**
- Modify: `arch/19-backups.md` (новый §3.6; §4 — строка ключа `drill` + поле policy; §9 — секция Drill; §10 — риски)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.3.1 — строка ключа `drill`; §9 — новая под-секция мутации policy из панели)
- Modify: `arch/14-pgworker.md` (§1.1 — тело policy-API расширяется `drill.interval_days`)

**Interfaces:**
- Consumes: spec §3.1–§3.8 (проект правок уже написан в spec — переносится в каноническую лексику).
- Produces: канон, на который ссылаются все последующие задачи (ревью plan↔spec и код↔arch).

- [ ] **Step 1.1: arch/19 §3.6 — раздел «Дрилл восстановимости (reliability t02)»**

  **Вход:** spec §3.3–§3.5 прочитаны; `arch/19-backups.md` открыт (разделы идут после §3.5, перед §4).
  **Действие:** вставить новый `## 3.6. Дрилл восстановимости (reliability t02)` текстом spec §3.3–§3.5 в канонической лексике: роль (НЕдеструктивный прогон DR-пути: тот же кандидат и валидация, что у restore-заявки t05 — новейший COMPLETED + манифест + WalChain); RestoreDrillProcess под клэймом `<C>` (супервиз/таймаут `DrillTimeoutSec`/takeover `drill-vanished`); контроль «старт postgres + выход из recovery» (`pg_is_in_recovery()=false` + `restored_to_lsn`; SQL-проверок данных нет — целостность файлов за verify t04); drill-джоб `pgw-backup-drill-<C>-<X>-<id>` (образ `pgworker-backup`, свой ephemeral volume, `Ports: []`, `Network: null`, unix-socket); фазы джоба `downloading|recovering`; отбор — один шард за проход, наименее свежий по `finished_unix`, гварды (active restore владеет шардом, `interval_days=0` — выкл); снос контура — доводимая фаза `cleaning` (journal `drill-cleanup/<X>/<id>` ДО rm; ключ = чистый терминальный итог после подтверждённого удаления контейнера И volume; переживает рестарт воркера — идемпотентная доводка по детерминированным именам); изоляция — нод в кластере не создаёт, HA-scope и synchronous-режим мастера не затрагивает, S3 только читает; стоп-семантика `Backups:Enabled=false` (доводка активного и сноса без новых запусков); deprovisioning D1 — префикс `pgw-backup-drill-<C>-` в чистке джобов бэкапов (контейнер+volume одно имя), D2 — ключ уходит с per-cluster префиксом.
  **Выход:** раздел §3.6 в arch/19.
  **Проверка:** визуально: раздел на месте, порядок §3.5 → §3.6 → §4 сохранён; §8 (карта задач) НЕ тронут.
  **Spec:** §3.8.1, §2.2–§2.5.

- [ ] **Step 1.2: arch/19 §4 — строка ключа `drill` и поле policy**

  **Вход:** Step 1.1 выполнен.
  **Действие:** в таблицу ключей §4 добавить строку:
  `| /pgworker/backups/<C>/<X>/drill | состояние последнего/текущего дрилла шарда (reliability t02, §3.6): {"state":"RUNNING\|SUCCEEDED\|FAILED","id","backup_id","started_unix","finished_unix"?,"phase"?,"restored_to_lsn"?,"error"?}; id — как у полных (§2); phase — downloading\|recovering (фазы джоба) \| cleaning (идёт снос тестового контура после терминального исхода); снятый phase у терминального ключа = контейнер и volume подтверждённо удалены — чистый итог; ключ перезаписывается каждым новым дриллом (история — фазы журнала backup-drill/<X>/<id>); пишет воркер (держатель клэйма <C>) |`
  В строку ключа `policy` добавить `"drill":{"interval_days":N}` (N=0 — дрилл кластера выключен; отсутствует → дефолт `PgWorker:Backups:Drill:IntervalDays`); в блок правил §4 — абзац о backwards-compat (панельный парсер толерантен к отсутствию drill-ключей и поля: правила дрилла молчат).
  **Выход:** §4 отражает новый ключ и поле.
  **Проверка:** таблица §4 содержит обе правки; deprovisioning-абзац §4 упоминает префикс `pgw-backup-drill-<C>-` в D1-чистке джобов бэкапов.
  **Spec:** §3.1, §3.8.1.

- [ ] **Step 1.3: arch/19 §9 — секция конфигурации Drill; §10 — риски**

  **Вход:** Step 1.2 выполнен.
  **Действие:** в §9 после `Restore { … }` добавить: `Drill { IntervalDays=1, TimeoutSec=21600 }` (reliability t02: дефолт периода дрилов шардов кластеров без policy-поля, `0` — глобальное выключение новых запусков при доводке активных; бюджет активного дрилла от `started_unix` → FAILED `drill-timeout`; бюджет наката WAL внутри джоба — общий `Restore:RecoveryTimeoutSec`). В валидацию старта §9 добавить: отрицательные `Drill:IntervalDays`/`Drill:TimeoutSec` — fail-fast. В §10 добавить два риска: «нагрузка дрилла (скачивание полного + накат WAL) на фоне живых бэкапов — большие базы» → закрытие: лимиты `Agent { Cpu, Mem }`, один дрилл на кластер за проход, расписание `interval_days`; «параллельные дриллы разных кластеров одновременно (глобального лидер-гварда нет — по одному на клэйм; домашняя установка 1–2 кластера)» → закрытие: зафиксирован риском, глобальный гвард не вводится (spec §5).
  **Выход:** §9/§10 дополнены.
  **Проверка:** grep `Drill {` arch/19-backups.md — одно вхождение в §9; в §10 две новые строки.
  **Spec:** §3.2, §3.8.1, §5.

- [ ] **Step 1.4: arch/adminpanel/02 §2.3.1 + §9**

  **Вход:** Step 1.3 выполнен.
  **Действие:** в таблицу §2.3.1 добавить строку ключа `/pgworker/backups/<C>/<X>/drill` (формат из arch/19 §4; в модель — `DrillInfo` §3; панель читает, пишет только воркер; кормит алерты `backup-drill-failed`/`backup-drill-stale` и per-shard статус грани «Хранилище бэкапов»). В §9 — новая под-секция «§9.x. Мутация policy бэкапов: интервал дрила» после §9.11: панель НЕ пишет в etcd — PUT `/api/clusters/{c}/backups/policy` панели (прокси) → команда `UpdateBackupsPolicyCommand` → POST `/api/clusters/{c}/backups/policy` API воркера (arch/14 §1.1) полным телом (retention + full_max_age_sec + verify + drill); поле «Дрилл каждые N суток» (0 = выкл) в грани «Хранилище бэкапов»; применяется следующим проходом воркера без рестарта.
  **Выход:** adminpanel/02 отражает чтение и мутацию.
  **Проверка:** grep `drill` arch/adminpanel/02-etcd-contract.md — есть в §2.3.1 и §9.
  **Spec:** §3.6, §3.8.2.

- [ ] **Step 1.5: arch/14 §1.1 — тело policy-API**

  **Вход:** Step 1.4 выполнен.
  **Действие:** в §1.1 к строке эндпоинта `POST /api/clusters/{cluster}/backups/policy` (т06) дописать одну фразу: тело дополнительно принимает `"drill":{"interval_days":int}` (валидация [0..3650]; поле отсутствует → секция `drill` в записываемую policy не кладётся — кластер живёт на глобальном дефолте; замещение целиком сохраняется — reliability t02, arch/19 §4).
  **Выход:** arch/14 синхронен.
  **Проверка:** grep `interval_days` arch/14-pgworker.md — одно вхождение в §1.1.
  **Spec:** §3.6 (API воркера), §3.8.3.

- [ ] **Step 1.6: Коммит arch-правок**

  **Вход:** Steps 1.1–1.5 выполнены.
  **Действие:**
  ```bash
  git add arch/19-backups.md arch/adminpanel/02-etcd-contract.md arch/14-pgworker.md
  git commit -m "arch(t02-restore-drill): §3.6 дрилл восстановимости, ключ drill + policy.drill.interval_days, конфигурация Drill, панель/adminpanel-02/arch-14 проекции"
  ```
  **Выход:** arch-only коммит в ветке `t02-restore-drill` (до любого кода — arch-first).
  **Проверка:** `git log --oneline -1` + `git show --stat HEAD` — только 3 arch-файла.
  **Spec:** §2.1, AC11.

---

### Task 2: Контракт в коде — модель DrillState, имена, DrillStatusJson

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` (enum `DrillStatus`; record `DrillState`; `BackupPolicy.DrillIntervalDays`; `ShardBackups.Drill`)
- Modify: `src/PgWorker.Backups/BackupNames.cs` (`DrillKey`/`DrillContainerName`/`DrillVolumeName`/`DrillJobContainerPrefix`)
- Create: `src/PgWorker.Backups/Drill/DrillStatusJson.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/DrillStatusJsonTests.cs` (новый); `src/tests/PgWorker.UnitTests/Backups/BackupNamesTests.cs` (дополнение)

**Interfaces:**
- Consumes: формат ключа `drill` из arch/19 §4 (Task 1).
- Produces (для Tasks 3–8): `DrillStatus { Running, Succeeded, Failed }`; `DrillState(string Id, DrillStatus State, string BackupId, long StartedUnix, long? FinishedUnix = null, string? Phase = null, string? RestoredToLsn = null, string? Error = null)`; `BackupPolicy(…, int? DrillIntervalDays = null)`; `ShardBackups(…, DrillState? Drill = null)`; `BackupNames.DrillKey(cluster, shard)`, `DrillContainerName(cluster, shard, id)`, `DrillVolumeName(cluster, shard, id)`, `DrillJobContainerPrefix(cluster)`; `DrillStatusJson.Serialize(DrillState) → string`.

- [ ] **Step 2.1: Пишу failing-тесты DrillStatusJson + имён (TDD)**

  **Вход:** модель/имена/сериализатор ещё не существуют.
  **Действие:** создать `src/tests/PgWorker.UnitTests/Backups/DrillStatusJsonTests.cs` (по образцу `RestoreStatusJsonTests.cs`):
  ```csharp
  // AAA: сериализация статуса дрилла в формат канона arch/19 §4 —
  // обязательные state/id/backup_id/started_unix; null-поля не пишутся
  [Fact]
  public void Serialize_Running_Minimal_WritesCoreFields() { … } // {"state":"RUNNING","id":"…","backup_id":"…","started_unix":N}
  [Fact]
  public void Serialize_TerminalWithPhase_WritesPhaseAndFinished() { … } // + "phase":"cleaning","finished_unix":N
  [Fact]
  public void Serialize_Succeeded_WritesRestoredToLsn() { … } // + "restored_to_lsn":"0/…"
  [Fact]
  public void Serialize_Failed_WritesError() { … }
  ```
  В `BackupNamesTests.cs` добавить:
  ```csharp
  // AAA: имена дрилла детерминированы (takeover-инвариант), volume = имя контейнера
  [Fact]
  public void DrillNames_AreDeterministic() { /* DrillKey, DrillContainerName, DrillVolumeName, DrillJobContainerPrefix — точные строки */ }
  ```
  **Выход:** тесты написаны, проект тестов не собирается (типы отсутствуют).
  **Проверка:** `dotnet build src/PgWorker.slnx` — ошибки компиляции отсутствия `DrillState`/`DrillStatusJson` (ожидаемый RED).
  **Spec:** §3.1 (формат ключа), AC4/AC9 (детерминированные имена).

- [ ] **Step 2.2: Реализация модели, имён, сериализатора**

  **Вход:** Step 2.1 (RED).
  **Действие:**
  1) `BackupsModel.cs` — после `RestoreOperationState`:
  ```csharp
  /// <summary>Состояние дрилла восстановимости шарда (ключ
  /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill, reliability t02, arch/19 §4).</summary>
  public enum DrillStatus { Running, Succeeded, Failed }

  /// <summary>Последний/текущий дрилл шарда: state/id/backup_id/started_unix
  /// обязательны; Phase — downloading|recovering|cleaning (снятие у терминального =
  /// контур подтверждённо снесён); ключ перезаписывается каждым дриллом.</summary>
  public sealed record DrillState(
      string Id, DrillStatus State, string BackupId, long StartedUnix,
      long? FinishedUnix = null, string? Phase = null,
      string? RestoredToLsn = null, string? Error = null);
  ```
  `BackupPolicy` — последний параметр `int? DrillIntervalDays = null` (+ doc-комментарий: null — поля нет в policy → дефолт конфига). `ShardBackups` — последний параметр `DrillState? Drill = null` (doc: null — ключа нет).
  2) `BackupNames.cs`:
  ```csharp
  /// <summary>Ключ дрилла шарда (reliability t02, arch/19 §4):
  /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill — один на шард.</summary>
  public static string DrillKey(string cluster, string shard)
      => $"/pgworker/backups/{cluster}/{shard}/drill";

  /// <summary>Имя drill-джоба: pgw-backup-drill-&lt;C&gt;-&lt;X&gt;-&lt;id&gt; —
  /// D1-префикс pgw-backup-*, супервиз takeover-инвариантен.</summary>
  public static string DrillContainerName(string cluster, string shard, string id)
      => $"pgw-backup-drill-{cluster}-{shard}-{id}";

  public static string DrillVolumeName(string cluster, string shard, string id)
      => DrillContainerName(cluster, shard, id);

  public static string DrillJobContainerPrefix(string cluster)
      => $"pgw-backup-drill-{cluster}-";
  ```
  3) `src/PgWorker.Backups/Drill/DrillStatusJson.cs` (по образцу `RestoreStatusJson.cs`; namespace `PgWorker.Backups.Drill`):
  ```csharp
  // Сериализация статуса дрилла в JSON канона /pgworker/backups/<C>/<X>/drill
  // (arch/19 §4, reliability t02): пишет ТОЛЬКО воркер под клэймом; null/опциональные
  // поля не сериализуются (образец RestoreStatusJson). Парсинг — снапшотный
  // BackupsParser.TryParseDrill ( PgWorker.Etcd ), панель — своим парсером.
  public static class DrillStatusJson
  {
      private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
      public static string Serialize(DrillState state) { /* state→"RUNNING"|"SUCCEEDED"|"FAILED"; ядро id/backup_id/started_unix; опционально finished_unix/phase/restored_to_lsn/error */ }
  }
  ```
  **Выход:** типы существуют, сериализация формата канона.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~DrillStatusJson|FullyQualifiedName~BackupNames"` — PASS (GREEN).
  **Spec:** §3.1, §3.8.1.

- [ ] **Step 2.3: Коммит**

  **Вход:** Step 2.2 зелёный.
  **Действие:** `git add -A src/PgWorker.Etcd/Parsing/BackupsModel.cs src/PgWorker.Backups/BackupNames.cs src/PgWorker.Backups/Drill/ src/tests/PgWorker.UnitTests/Backups/ && git commit -m "feat(backups): модель DrillState + policy.drill.interval_days + имена/DrillStatusJson дрилла (t02-restore-drill)"`
  **Выход:** коммит контракта.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.1.

---

### Task 3: DrillPlanner — чистый отбор кандидата

**Files:**
- Create: `src/PgWorker.Backups/Drill/DrillPlanner.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/DrillPlannerTests.cs`

**Interfaces:**
- Consumes: `ShardSpec` (`PgWorker.Core.Model`: `Name`, `ToRemove`), `ShardBackups` (Task 2), `DrillState`, `RestoreStatus`.
- Produces (для Task 7): `DrillPlanner.SelectCandidate(IReadOnlyList<ShardSpec> shards, IReadOnlyDictionary<string, ShardBackups> backups, IReadOnlyDictionary<string, DrillState> drills, int intervalDays, long nowUnix) → string?` — имя шарда-кандидата или null.

- [ ] **Step 3.1: Пишу failing-тесты отбора (TDD)**

  **Вход:** DrillPlanner не существует.
  **Действие:** `src/tests/PgWorker.UnitTests/Backups/DrillPlannerTests.cs` (AAA-комментарии; хелперы-фабрики shard/full/drill/restore):
  ```csharp
  // AAA: интервал ≤ 0 — запусков нет (выключено)
  [Fact] SelectCandidate_IntervalZero_ReturnsNull()
  // AAA: незавершённый дрилл кластера (RUNNING у любого шарда) блокирует новые
  [Fact] SelectCandidate_ActiveDrill_ReturnsNull()
  // AAA: терминальный дрилл с phase=cleaning блокирует новые (снос не доведён)
  [Fact] SelectCandidate_UnclosedCleaning_ReturnsNull()
  // AAA: ключа дрилла нет + есть COMPLETED-полный → готов немедленно (первый дрилл)
  [Fact] SelectCandidate_NoDrillKey_CompletedFull_Ready()
  // AAA: с момента finished_unix последнего дрилла < interval_days — не готов
  [Fact] SelectCandidate_RecentSuccess_WaitsInterval()
  // AAA: now − finished ≥ interval_days×86400 → готов
  [Fact] SelectCandidate_ExpiredInterval_Ready()
  // AAA: FAILED-дрилл тоже считается «последним» (готовность по finished_unix)
  [Fact] SelectCandidate_FailedDrill_CountsAsLast()
  // AAA: активная restore-заявка шарда исключает шард (restore владеет ЖЦ)
  [Fact] SelectCandidate_ActiveRestore_ExcludesShard()
  // AAA: ToRemove-шард и шард без COMPLETED-полных — не кандидаты
  [Fact] SelectCandidate_ToRemoveOrNoCompleted_Excluded()
  // AAA: среди готовых выбирается наименее свежий (min finished_unix; «никогда»=0 первым)
  [Fact] SelectCandidate_PicksLeastRecentlyDrilled()
  ```
  **Выход:** тесты написаны (RED — нет типа).
  **Проверка:** `dotnet build src/PgWorker.slnx` — RED.
  **Spec:** §3.3 п.3, AC1, AC8.

- [ ] **Step 3.2: Реализация DrillPlanner**

  **Вход:** Step 3.1 (RED).
  **Действие:** `src/PgWorker.Backups/Drill/DrillPlanner.cs`:
  ```csharp
  namespace PgWorker.Backups.Drill;

  /// <summary>Чистый отбор кандидата дрилла (reliability t02, arch/19 §3.6):
  /// один шард за проход кластера — наименее свежий по последнему терминальному
  /// дриллу («никогда» = 0 — обслуживается первым). Гварды: интервал ≤ 0 — выкл;
  /// незавершённый дрилл кластера (RUNNING либо терминальный с phase=cleaning)
  /// блокирует новые; restore-заявка владеет ЖЦ шарда; ToRemove/без
  /// COMPLETED-полных — не кандидаты. Без I/O — юнит-тестируемо.</summary>
  public static class DrillPlanner
  {
      public const long DaySec = 86400;

      public static string? SelectCandidate(
          IReadOnlyList<ShardSpec> shards,
          IReadOnlyDictionary<string, ShardBackups> backups,
          IReadOnlyDictionary<string, DrillState> drills,
          int intervalDays,
          long nowUnix)
      {
          // 1) интервал выключен / любой незавершённый дрилл кластера → null
          // 2) кандидаты: !ToRemove; backups[X].Full есть Completed;
          //    Restores без Planned/Running/Rejoining
          // 3) готовность: drills[X] нет → готов (last=0);
          //    терминальный без cleaning → готов iff nowUnix − (FinishedUnix ?? 0) ≥ intervalDays×DaySec
          // 4) выбор: MIN lastFinished, tie-break по имени (Ordinal) — детерминизм
      }
  }
  ```
  **Выход:** чистая функция отбора.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~DrillPlanner` — PASS.
  **Spec:** §3.3 п.3.

- [ ] **Step 3.3: Коммит**

  **Вход:** Step 3.2 зелёный.
  **Действие:** `git add src/PgWorker.Backups/Drill/DrillPlanner.cs src/tests/PgWorker.UnitTests/Backups/DrillPlannerTests.cs && git commit -m "feat(backups): DrillPlanner — чистый отбор кандидата дрилла (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.3 п.3, AC1/AC8.

---

### Task 4: Drill-джоб — обёртки DrillJobCommand/DrillJobLog + DrillJobSpec

**Files:**
- Create: `src/PgWorker.Backups/Drill/DrillJobCommand.cs`
- Create: `src/PgWorker.Backups/Drill/DrillJobLog.cs`
- Create: `src/PgWorker.Backups/Drill/DrillJobSpec.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/DrillJobSpecTests.cs` (новый); `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs`/`RestoreJobLogTests.cs` — образцы стиля

**Interfaces:**
- Consumes: `RestoreJobCommand.Build()`, `RestoreJobLog.Parse(logs) → RestoreJobMarkers(string? Phase, RestoreJobResult? Result)`, `RestoreJobCommand.Env*`, `WalAgentCommand.McHost`, `ContainerSpec`, `BackupsRuntimeOptions` (`AgentS3Endpoint`, `S3Bucket`, `S3AccessKey`, `S3SecretKey`, `JobImage`, `AgentCpu`, `AgentMem`, `RestoreRecoveryTimeoutSec`), `BackupNames` (Task 2).
- Produces (для Task 6/7): `DrillJobCommand.Build() → IReadOnlyList<string>`; `DrillJobLog.Parse(string logs) → RestoreJobMarkers`; `DrillJobSpec.Build(BackupsRuntimeOptions opts, string cluster, string shard, string id) → ContainerSpec`.

- [ ] **Step 4.1: Пишу failing-тесты спеки джоба (TDD)**

  **Вход:** DrillJobSpec не существует.
  **Действие:** `src/tests/PgWorker.UnitTests/Backups/DrillJobSpecTests.cs` (по образцу `RestoreJobSpecTests.cs`, AAA):
  ```csharp
  // AAA: спека дрилла — свой ephemeral volume с именем джоба, PGDATA /drill/pgroot/data,
  // latest (TARGET_TIME=""), бюджет recovery из RestoreRecoveryTimeoutSec
  [Fact] Build_MountsOwnDrillVolume_AndLatestTarget() { /* VolumeName==BackupNames.DrillVolumeName(C,X,id); VolumeDest=="/drill"; env PGW_RESTORE_DATA_DIR=="/drill"; env PGW_RESTORE_PGDATA=="/drill/pgroot/data"; env BACKUP_ID==id; env SRC_PREFIX=="<C>/<X>"; env TARGET_TIME==""; env PGW_RECOVERY_TIMEOUT_SEC=="1800" */ }
  // AAA: изоляция — без сети, без портов, рестарт no, inline-команда, лимиты Agent
  [Fact] Build_Isolated_NoPortsNoNetwork_RestartNo() { /* Ports пуст; Network null; RestartPolicy "no"; ResetEntrypoint; Cpu/Mem из Agent */ }
  // AAA: команда джоба — механика t05 (обёртка, решение гейта плана)
  [Fact] Command_DelegatesToRestoreJobCommand() { /* DrillJobCommand.Build() эквивалентен RestoreJobCommand.Build() */ }
  // AAA: парсер логов — обёртка RestoreJobLog (фазы downloading/recovering + result)
  [Fact] LogParses_DelegatesToRestoreJobLog() { /* вход с {"phase":"recovering"} и {"ok":true,"restored_to_lsn":"0/42"} → Phase=="recovering", Result.Ok, RestoredToLsn */ }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet build src/PgWorker.slnx` — RED.
  **Spec:** §3.4; решение гейта плана (обёртки).

- [ ] **Step 4.2: Реализация обёрток и спеки**

  **Вход:** Step 4.1 (RED).
  **Действие:**
  ```csharp
  // src/PgWorker.Backups/Drill/DrillJobCommand.cs
  namespace PgWorker.Backups.Drill;

  // Команда drill-джоба (reliability t02, arch/19 §3.6) — механика restore-джоба
  // t05 ЦЕЛИКОМ (решение пользователя, гейт плана 2026-10-01): один inline-bash
  // скрипта RestoreJobCommand на оба сценария — дрилл доказывает ровно тот DR-путь
  // (spec §2.2); TARGET_TIME="" → latest (без recovery_target_*), unix-socket,
  // pg_is_in_recovery-поллинг, pg_current_wal_lsn. Лишнее для дрилла поле
  // system_id в result воркер дрилла игнорирует (в ключ drill не попадает).
  public static class DrillJobCommand
  {
      public static IReadOnlyList<string> Build() => Restore.RestoreJobCommand.Build();
  }
  ```
  ```csharp
  // src/PgWorker.Backups/Drill/DrillJobLog.cs
  // Парсер stdout drill-джоба — обёртка RestoreJobLog (протокол идентичен:
  // фазы downloading|recovering + result-JSON; решение гейта плана).
  public static class DrillJobLog
  {
      public static Restore.RestoreJobMarkers Parse(string logs) => Restore.RestoreJobLog.Parse(logs);
  }
  ```
  `DrillJobSpec.cs` — по образцу `RestoreJobSpec.Build`, отличия: volume — `BackupNames.DrillVolumeName(cluster, shard, id)`, `dataDir: "/drill"` (PGDATA `$"/drill/pgroot/data"`), `TARGET_TIME: ""`, `SRC_PREFIX: $"{cluster}/{shard}"` (own-source всегда), `Hostname: BackupNames.DrillContainerName(...)`, `Cmd: DrillJobCommand.Build()`; всё прочее (env S3-комплект MC_HOST, `Ports: []`, `Network: null`, `RestartPolicy: "no"`, `Agent { Cpu, Mem }`, `ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"]`, `ResetEntrypoint: true`) — как у restore-джоба.
  **Выход:** спека drill-джоба изолированного контура.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~DrillJobSpec"` — PASS.
  **Spec:** §3.4, §2.4.

- [ ] **Step 4.3: Коммит**

  **Вход:** Step 4.2 зелёный.
  **Действие:** `git add src/PgWorker.Backups/Drill/ src/tests/PgWorker.UnitTests/Backups/DrillJobSpecTests.cs && git commit -m "feat(backups): DrillJobCommand/DrillJobLog (обёртки t05) + DrillJobSpec изолированного drill-джоба (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.4.

---

### Task 5: Воркерный снапшотный парсер — ключ drill + policy.drill

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs` (case `/pgworker/backups/<C>/<X>/drill` → `TryParseDrill`; `TryParsePolicy` — `drill.interval_days`)
- Test: `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs` (дополнение)

**Interfaces:**
- Consumes: `DrillState`/`DrillStatus`/`ShardBackups.Drill`/`BackupPolicy.DrillIntervalDays` (Task 2).
- Produces: снапшотная модель с drill-ключами (вход `RestoreDrillProcess`, Tasks 6–7) и интервалом policy.

- [ ] **Step 5.1: Пишу failing-тесты парсера (TDD)**

  **Вход:** парсер drill-ключей не читает.
  **Действие:** в `PgWorker.UnitTests/Etcd/BackupsParserTests.cs` добавить (AAA):
  ```csharp
  // AAA: ключ <X>/drill парсится в ShardBackups.Drill (все поля)
  [Fact] Parse_DrillKey_FillsDrillState() { /* {"state":"SUCCEEDED","id":"…","backup_id":"…","started_unix":N,"finished_unix":M,"restored_to_lsn":"0/1"} */ }
  // AAA: phase читается (cleaning/downloading/recovering)
  [Fact] Parse_DrillKey_PhasePreserved() { … }
  // AAA: битый JSON / неизвестное state → parseErrors + Drill=null (шард жив)
  [Fact] Parse_DrillKey_BadJson_Reported() { … }
  // AAA: policy.drill.interval_days → BackupPolicy.DrillIntervalDays; отсутствие → null
  [Fact] Parse_PolicyDrillInterval_ReadAndDefault() { … }
  // AAA: старая policy без drill-поля парсится без ошибок (backwards-compat)
  [Fact] Parse_PolicyWithoutDrill_NoError() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupsParserTests"` — RED на новых.
  **Spec:** §3.1 (backwards-compat), §3.3 п.3 (интервал policy).

- [ ] **Step 5.2: Реализация в BackupsParser**

  **Вход:** Step 5.1 (RED).
  **Действие:** в switch парсера добавить `case 6 when segments[4].Length > 0 && segments[5] == "drill":` → `GetOrAdd(acc.Shards, segments[4], …).DrillRaw = kv.Value;` (`ShardAcc` + `string? DrillRaw`); в `BuildCluster` — `TryParseDrill(acc.Name, pair.Key, pair.Value.DrillRaw, errors)`. `TryParseDrill` — по образцу `TryParseRestore`: обязательны `state` (RUNNING|SUCCEEDED|FAILED)/`id`/`backup_id`/`started_unix`; опциональны `finished_unix`/`phase`/`restored_to_lsn`/`error`; битое/неизвестное state → error + null. `TryParsePolicy`: после verify-блока — `int? drillIntervalDays = null;` из `root.drill.interval_days` (Number|int; невалидное → null), передать в ctor `BackupPolicy`.
  **Выход:** снапшот несёт drill-состояние и интервал.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupsParserTests"` — PASS.
  **Spec:** §3.1.

- [ ] **Step 5.3: Коммит**

  **Вход:** Step 5.2 зелёный.
  **Действие:** `git add src/PgWorker.Etcd/Parsing/BackupsParser.cs src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs && git commit -m "feat(etcd): парсинг ключа drill и policy.drill.interval_days в снапшот бэкапов (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.1.

---

### Task 6: RestoreDrillProcess — скелет, супервиз, снос-cleaning, краш-рекавери

**Files:**
- Create: `src/PgWorker.Backups/Process/RestoreDrillProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs` (новый; окружение по образцу `BackupVerifyProcessTests`: `OwnEtcd` + фейк-движок + `FakeBackupS3` из `FakeBackupDeps.cs`)

**Interfaces:**
- Consumes: `DrillStatusJson.Serialize`, `BackupNames.Drill*`, `DrillJobLog`, `DrillJobSpec`, `DrillPlanner` (Task 7), `SupervisionTimeouts.IsTimedOut`, `ShardEndpoints.ReadPortAllocAsync`, `WorkJournal.WritePhaseAsync`, `ClaimStore.IsMine`, `BackupPlanner.NextId`, `WalChain`/`WalFileName`/`BackupLabel`, `IBackupS3` (`DownloadTextAsync`, `ListWalAsync`).
- Produces: `RestoreDrillProcess(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ShardEndpoints shardEndpoints, IBackupS3 s3, ClaimStore claims, WorkJournal journal, BackupsRuntimeOptions options, TimeProvider time, ILogger<RestoreDrillProcess> logger)` с `Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)`; const `Op = "backup-drill"`.

- [ ] **Step 6.1: Скелет процесса + окружение теста**

  **Вход:** Tasks 2–5 слиты; образцы `BackupVerifyProcess.cs`/`BackupVerifyProcessTests.cs` изучены.
  **Действие:** создать `RestoreDrillProcess.cs` — primary-ctor как в Interfaces, каркас `TickAsync`:
  1. гвард клэйма (`claims.IsMine(cluster)` — иначе Failed, образец verify);
  2. `snap.Config.State != ClusterState.Active` → Done;
  3. `Enabled=false` — НЕ выход: блокируются только новые запуски (флаг используется в отборе Task 7; супервиз/доводка продолжаются);
  4. сбор drill-ключей кластера `mine.Shards[x].Drill`;
  5. сортировка `(shard, drill)` по `StartedUnix`; обработка ПЕРВОГО RUNNING (супервиз) или терминального с `Phase=="cleaning"` (доводка); была активность → `return Done` (отбор — следующим тиком).
  В тесте — окружение Fact'а: `OwnEtcd Fx` (свой etcd на Fact), `ClaimStore Claims` (один на окружение), фейк-движок `FakeDrillEngine` (копия `FakeVerifyEngine` из `BackupVerifyProcessTests` — те же члены/флаги), драйвер `FakeDrillDriver` (копия `FakeVerifyDriver`), `FakeBackupS3` с сидом полнного+wal, сид кластера/config/portalloc (хелперы по образцу verify-тестов).
  **Выход:** процесс компилируется (методы супервиза — заглушки `Task.CompletedTask` НЕ оставлять: супервиз реализуется в 6.2, но каркас обязан собираться — временно приватные методы кидать `NotImplementedException` допустимо только до 6.2 в одном коммите задачи).
  **Проверка:** `dotnet build src/PgWorker.slnx -c Release`.
  **Spec:** §3.3 п.1–2.

- [ ] **Step 6.2: Failing-интеграции супервиза и сноса (TDD)**

  **Вход:** скелет 6.1.
  **Действие:** в `RestoreDrillProcessTests.cs` (каждый Fact — своё `OwnEtcd`; AAA):
  ```csharp
  // AAA: RUNNING + контейнер running → phase из логов пишется в ключ при изменении
  [Fact] Supervise_RunningContainer_UpdatesPhaseFromLogs() { /* сид: ключ RUNNING, фейк-контейнер running с логами {"phase":"recovering"}; тик → ключ содержит "phase":"recovering" */ }
  // AAA: exited 0 + result ok:true → SUCCEEDED + finished_unix + restored_to_lsn → cleaning → контейнер и volume удалены, phase снят, журнал drill-done
  [Fact] Supervise_ExitOk_SucceedsAndCleansUp() { /* фейк-контейнер exited(0) с {"ok":true,"restored_to_lsn":"0/42"}; тик(и) до чистого итога; asserts: ключ "SUCCEEDED", нет "phase", engine.Removed содержит имя, engine.RemovedVolumes содержит volume */ }
  // AAA: exited 1 → FAILED с error из result; снос тот же
  [Fact] Supervise_ExitFail_FailsWithErrorAndCleansUp() { /* логи {"ok":false,"error":"boom"}; asserts: "FAILED", "boom", контейнер/volume снесены, phase снят */ }
  // AAA: рестарт посреди сноса: терминальный ключ с phase=cleaning + живые контейнер/volume → следующий тик дочищает до чистого итога
  [Fact] Cleanup_RestartMidCleaning_NextTickFinishes() { /* сид: ключ SUCCEEDED+phase=cleaning, фейк-контейнер exited + volume; тик → оба удалены, phase снят; второй тик — no-op */ }
  // AAA: transient docker rm — ключ остаётся с phase=cleaning, следующий тик повторяет
  [Fact] Cleanup_TransientRemove_KeepsCleaningPhase() { /* фейк: RemoveContainerAsync fail один раз */ }
  ```
  **Выход:** интеграции (RED — супервиз не реализован).
  **Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreDrillProcessTests` — RED (сборка должна проходить).
  **Spec:** §3.3 п.2, AC4, AC3.

- [ ] **Step 6.3: Реализация супервиза и сноса**

  **Вход:** Step 6.2 (RED).
  **Действие:** в `RestoreDrillProcess`:
  - `SuperviseRunningAsync(snap, shard, drill)`: адреса `shardEndpoints.ReadPortAllocAsync` → docker-хост первой ноды шарда (`shard.Nodes.Min(n => n.Name)`; шард/ноды исчезли → fallback первый хост `driver.GetHostsAsync` — образец `EngineForShardAsync` verify); `engine` null или list-fail → transient (`journal` + InProgress, статус не трогаем);
    - таймаут: `SupervisionTimeouts.IsTimedOut(drill.StartedUnix, now, options.DrillTimeoutSec)` → терминальный FAILED `drill-timeout: {age} с > {DrillTimeoutSec} с` → снос;
    - контейнера нет (после успешного list) → путь `drill-vanished`/transient — реализуется в Task 7 (в этой задаче — transient-ожидание, статус не меняем);
    - `created` → `StartContainerAsync`; `running` → логы `GetContainerLogsAsync` → `DrillJobLog.Parse` → `phase != drill.Phase` → put `drill with { Phase = phase }` (failover-put по образцу `BackupVerifyProcess.PutAsync`); ждём (InProgress);
    - `exited` → `InspectContainerAsync` exit-код + `DrillJobLog.Parse`: `exitCode == 0 && Result is { Ok: true }` → SUCCEEDED `with { FinishedUnix = now, RestoredToLsn = Result.RestoredToLsn }`; иначе FAILED `Error = Result?.Error ?? $"exit {exitCode}"` → снос.
  - `FinishCleanupAsync(cluster, shard, drill)` — снос после терминального исхода (все пути сходятся):
    1. `journal.WritePhaseAsync(cluster, Op, $"drill-cleanup/{shard}/{drill.Id}", claims.InstanceId, null, ct)` — ДО rm;
    2. put ключа: терминальный `state` + `finished_unix` + `Phase = "cleaning"` (если ещё не стоит);
    3. `engine.RemoveContainerAsync(имя, force: true)` + `engine.RemoveVolumeAsync(BackupNames.DrillVolumeName(...))` (404=ок);
    4. подтверждение: повторный `ListContainersAsync(имя, all: true)` пуст → put терминального ключа БЕЗ phase (чистый итог) + `journal` `drill-done/<X>/<id>`|`drill-failed/<X>/<id>`; list-fail/не пусто → ключ остаётся с `cleaning`, следующий тик повторяет (идемпотентно);
  - `PutDrillAsync` — failover-put по endpoint'ам (образец verify), значение `DrillStatusJson.Serialize`.
  **Выход:** супервиз и доводимый снос.
  **Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreDrillProcessTests` — PASS; после серии — зачистка `docker ps -a --format '{{.Names}}' | grep pgw-` (пусто) и осиротевших сетей `docker network ls | grep -c 'pgw\|kfw-net'` → при остатках `docker network prune -f`.
  **Spec:** §3.3 п.2 (все подпункты), §2.5, AC4.

- [ ] **Step 6.4: Коммит**

  **Вход:** Step 6.3 зелёный.
  **Действие:** `git add src/PgWorker.Backups/Process/RestoreDrillProcess.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs && git commit -m "feat(backups): RestoreDrillProcess — супервиз drill-джоба и доводимый снос cleaning (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.3 п.2.

---

### Task 7: RestoreDrillProcess — отбор, валидация, запуск, гварды, таймауты

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreDrillProcess.cs` (ветки отбора/валидации/запуска/vanished/гвардов)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs` (дополнение)

**Interfaces:**
- Consumes: `DrillPlanner.SelectCandidate` (Task 3), `DrillJobSpec`/`DrillJobCommand` (Task 4), `BackupPlanner.NextId`, `WalChain.Check`, `WalFileName.TryParse`, `Restore.BackupLabel.WalStartSegment`, `FakeBackupS3` (сид манифеста/label/wal).
- Produces: полный тик `RestoreDrillProcess` (потребитель — врезка Task 8).

- [ ] **Step 7.1: Failing-интеграции отбора/валидации/запуска (TDD)**

  **Вход:** Task 6 слит.
  **Действие:** добавить Fact'ы (AAA; сжатое время — интервал policy 1–2 с НЕ возможен: интервал в СУТКАХ; «просрочка» проверяется сидом готового ключа с старым `finished_unix`):
  ```csharp
  // AAA: первый дрилл стартует немедленно: COMPLETED-полный есть, ключа нет → тик пишет RUNNING + создаёт+стартует контейнер pgw-backup-drill-<C>-<X>-<id>
  [Fact] Tick_NoDrillYet_StartsFirstDrill() { /* asserts: ключ "RUNNING", engine.Created[0] имя == DrillContainerName, Cmd — bash; journal started */ }
  // AAA: свежий SUCCEEDED (finished_unix=now) → новых запусков нет до interval_days
  [Fact] Tick_RecentSuccess_NoNewStart() { /* сид SUCCEEDED finished=now-60, policy drill.interval_days=1 → тик: created пуст, ключ не изменился */ }
  // AAA: interval_days=0 (policy) и DrillIntervalDays=0 (конфиг) — запусков нет
  [Fact] Tick_IntervalZero_NoStart() { … }
  // AAA: активная restore-заявка шарда — шард не кандидат
  [Fact] Tick_ActiveRestore_ShardExcluded() { /* сид restore/<id> RUNNING → created пуст */ }
  // AAA: валидационный провал (манифеста нет) → FAILED без запуска джоба: started/finished поставлены, error причина, контейнера/volume нет, ключ БЕЗ phase (чистый итог), journal failed
  [Fact] Tick_ManifestMissing_FailsWithoutJob() { … }
  // AAA: дыра WAL-цепочки → FAILED с границами от WalChain, без джоба
  [Fact] Tick_WalGap_FailsWithGapError() { /* сид wal-объектов с дырой */ }
  // AAA: takeover: ключ RUNNING + живой контейнер от «прошлого инстанса» — новый процесс супервизит до исхода (уже покрыто 6.2 частично; здесь — полный путь от RUNNING до SUCCEEDED одним сценарием)
  [Fact] Tick_Takeover_SeesJobThrough() { … }
  // AAA: RUNNING возраст > DrillTimeoutSec (в тесте options с малым бюджетом) → FAILED drill-timeout + kill+rm контейнера/volume через cleaning
  [Fact] Tick_RunningTooLong_FailsTimeoutAndCleans() { … }
  // AAA: RUNNING без контейнера, возраст > бюджета → FAILED drill-vanished + volume сносится
  [Fact] Tick_Vanished_FailsAndRemovesVolume() { … }
  // AAA: RUNNING без контейнера, возраст < бюджета → transient-ожидание (статус не меняется)
  [Fact] Tick_VanishedYoung_Waits() { … }
  // AAA: Enabled=false — доводка активного и сноса работает, новых запусков нет
  [Fact] Tick_Disabled_FinishesActive_NoNewStarts() { /* сид RUNNING+running-контейнер, options Enabled=false → тик доводит до терминала/сноса; затем при COMPLETED-полном новый не стартует */ }
  // AAA: интервал = policy.drill.interval_days ?? конфига (policy перекрывает)
  [Fact] Tick_PolicyInterval_OverridesConfig() { /* policy interval=0 → нет запуска при конфиге 1 */ }
  ```
  **Выход:** интеграции (RED).
  **Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreDrillProcessTests` — RED на новых, GREEN на Task 6.
  **Spec:** §3.3 п.2 (таймаут/vanished) п.3–5, AC1/AC8/AC9/AC10.

- [ ] **Step 7.2: Реализация отбора, валидации, запуска**

  **Вход:** Step 7.1 (RED).
  **Действие:** в `TickAsync` после супервиза:
  - отбор: `intervalDays = mine.Policy?.DrillIntervalDays ?? options.DrillIntervalDays`; `intervalDays <= 0 || !options.Enabled` → нет новых запусков; `candidate = DrillPlanner.SelectCandidate(snap.Shards, mine.Shards, drills, intervalDays, now)`; null → Done;
  - валидация кандидата (чистые шаги по образцу `RestoreProcess.ValidateAsync` own-source): свежий COMPLETED = `fulls.Where(Completed).MaxBy(Id, Ordinal)` (нет → Done молча); `s3.DownloadTextAsync(cluster, shard, $"full/{id}/backup_manifest")` — fail → валидационный FAILED `полный {id} без backup_manifest (недокачан/бит)`; `walStart = full.WalStartSegment ?? BackupLabel.WalStartSegment(backup_label)` (fail → FAILED причины); `WalFileName.TryParse` (null → FAILED); `s3.ListWalAsync` fail → transient; `WalChain.Check` дыра → FAILED `chain.GapError`; S3-отказы list → transient (статус не трогаем);
  - валидационный FAILED: `id = BackupPlanner.NextId([], time.GetUtcNow())`; put ключа `DrillStatusJson.Serialize(new DrillState(id, Failed, backupId, now, FinishedUnix: now, Error: причина))` — БЕЗ phase, контейнера не было, снос не нужен; `journal failed/{shard}/{id}`;
  - запуск: journal-before-manipulations — put `new DrillState(id, Running, backupId, now)` → `engine.CreateContainerAsync(DrillJobSpec.Build(options, cluster, shard, id), BackupNames.DrillContainerName(...))` → `StartContainerAsync`; create/start fail → transient (ключ остаётся RUNNING — следующий тик создаст по имени идемпотентно); успех → `journal started/{shard}/{id}` + log;
  - vanished-ветка в `SuperviseRunningAsync`: контейнера нет при успешном list → `IsTimedOut` → FAILED `drill-vanished` + снос (volume); иначе transient-ожидание.
  **Выход:** полный тик дрилла.
  **Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreDrillProcessTests` — PASS; зачистка серии (как в 6.3).
  **Spec:** §3.3 п.3–5, §2.6.

- [ ] **Step 7.3: Коммит**

  **Вход:** Step 7.2 зелёный.
  **Действие:** `git add src/PgWorker.Backups/Process/RestoreDrillProcess.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs && git commit -m "feat(backups): отбор/валидация/запуск дрилла, таймаут и vanished (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.3 п.3–5.

---

### Task 8: Опции Drill, DI, врезка ReconcileLoop, D1-чистка

**Files:**
- Modify: `src/PgWorker.Backups/Options.cs` (`DrillIntervalDays = 1`, `DrillTimeoutSec = 21600` — в конец record)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsDrillOptions`; `BackupsOptions.Drill`; `ToRuntime`; `IsValid`)
- Modify: `src/PgWorker.App/Program.cs` (регистрация `RestoreDrillProcess`)
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (`IClusterProcesses.DrillAsync` + реализация)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (врезка после `backup-restore`, до `repair`, БЕЗ `if Enabled`)
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (`BackupJobsCleaner.DrillContainerPrefix` + в массив чистки)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (дополнение); `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (дополнение при необходимости по образцу существующих врезок)

**Interfaces:**
- Consumes: `RestoreDrillProcess` (Tasks 6–7).
- Produces: `PgWorker:Backups:Drill { IntervalDays=1, TimeoutSec=21600 }` (конфиг); runtime `BackupsRuntimeOptions.DrillIntervalDays/DrillTimeoutSec`; `IClusterProcesses.DrillAsync(snap, backups, ct)`.

- [ ] **Step 8.1: Failing-тесты опций (TDD)**

  **Вход:** секции Drill нет.
  **Действие:** в `BackupsOptionsTests.cs` добавить (AAA):
  ```csharp
  // AAA: Drill-секция склеивается в runtime (дефолты 1/21600)
  [Fact] ToRuntime_Drill_DefaultsOneDaySixHours() { … }
  // AAA: отрицательные IntervalDays/TimeoutSec — fail-fast IsValid
  [Fact] IsValid_NegativeDrill_Fails() { … }
  // AAA: IntervalDays=0 валиден (глобальное выключение новых запусков)
  [Fact] IsValid_ZeroInterval_Valid() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~BackupsOptionsTests` — RED на новых.
  **Spec:** §3.2.

- [ ] **Step 8.2: Реализация опций + DI + врезка + D1**

  **Вход:** Step 8.1 (RED).
  **Действие:**
  1) `src/PgWorker.App/Options.cs`: `public sealed class BackupsDrillOptions { public int IntervalDays { get; set; } = 1; public int TimeoutSec { get; set; } = 21600; }` (doc: arch/19 §9, reliability t02); в `BackupsOptions` — `public BackupsDrillOptions Drill { get; set; } = new();`; `ToRuntime()` — именованные `DrillIntervalDays: Drill.IntervalDays, DrillTimeoutSec: Drill.TimeoutSec`; `IsValid()` — `&& Drill.IntervalDays >= 0 && Drill.TimeoutSec > 0`.
  2) `src/PgWorker.Backups/Options.cs`: в конец record `int DrillIntervalDays = 1, int DrillTimeoutSec = 21600` (doc-комментарий §3.2 spec).
  3) `Program.cs`: по образцу `BackupVerifyProcess` — `builder.Services.AddSingleton(sp => new PgWorker.Backups.Process.RestoreDrillProcess( …, sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(), sp.GetRequiredService<TimeProvider>(), sp.GetRequiredService<ILoggerFactory>().CreateLogger<…>()));` (+ `ShardEndpoints`, `IClusterDriver`, `IBackupS3`, `ClaimStore`, `WorkJournal` — те же, что у verify).
  4) `ClusterProcesses.cs`: в интерфейс — `Task<Result<ProcessOutcome>> DrillAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct);` (doc: reliability t02, arch/19 §3.6 — после backup-restore, до repair; зовётся ВСЕГДА: Enabled=false — стоп-семантика доводки); в реализацию — ctor-параметр `PgWorker.Backups.Process.RestoreDrillProcess drill` + метод `=> drill.TickAsync(snap, backups, ct);`.
  5) `ReconcileLoop.cs`: между блоками `backup-restore` и `repair` —
  ```csharp
  // Дрилл восстановимости (reliability t02, arch/19 §3.6): после backup-restore
  // (гвард restore-владения отработал в отборе), до repair. Зовётся ВСЕГДА:
  // Backups:Enabled=false / интервал 0 — стоп-семантика в процессе (доводка
  // активного дрилла и сноса контура, новых запусков нет).
  await RunClusterOpAsync(cluster, "backup-drill",
      () => processes.DrillAsync(snap, backups, ct), ct);
  ```
  6) `ClusterDriver.cs` `BackupJobsCleaner`: `public const string DrillContainerPrefix = "pgw-backup-drill-";` и в массив `new[] { (JobContainerPrefix, false), (VerifyContainerPrefix, true), (DrillContainerPrefix, true) }` (контейнер и volume — одно имя; чистка D1).
  **Выход:** опции/DI/врезка/D1.
  **Проверка:** `dotnet build src/PgWorker.slnx -c Release` — без warnings-ошибок; `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupsOptionsTests|FullyQualifiedName~ReconcileLoopTests"` — PASS.
  **Spec:** §3.2, §3.5, AC10.

- [ ] **Step 8.3: Коммит**

  **Вход:** Step 8.2 зелёный.
  **Действие:** `git add -A src/PgWorker.Backups/Options.cs src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/PgWorker.App/Loops/ src/PgWorker.Docker/Drivers/ClusterDriver.cs src/tests/PgWorker.UnitTests/App/ && git commit -m "feat(app): опции Drill + DI RestoreDrillProcess + врезка backup-drill в ReconcileLoop + D1-префикс (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.2, §3.5.

---

### Task 9: BackupsPolicyHandler — поле drill.interval_days

**Files:**
- Modify: `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs` (`DrillBody`; валидация; условная сериализация)
- Test: `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs` (дополнение)

**Interfaces:**
- Consumes: формат policy из arch/19 §4 (Task 1).
- Produces: POST `/api/clusters/{cluster}/backups/policy` принимает `"drill":{"interval_days":int}`; панель (Task 12) шлёт полный набор.

- [ ] **Step 9.1: Failing-API-интеграции (TDD)**

  **Вход:** handler не знает drill.
  **Действие:** в `BackupsPolicyApiTests.cs` добавить (AAA; фабрика `PgWorkerApiFactory` — как существующие):
  ```csharp
  // AAA: тело с drill.interval_days=3 → 200, policy-ключ содержит "drill":{"interval_days":3} рядом с прежними секциями
  [Fact] Policy_WithDrillInterval_AcceptedAndStored() { … }
  // AAA: drill в теле отсутствует → в записанной policy секции drill НЕТ (кластер на глобальном дефолте)
  [Fact] Policy_WithoutDrill_DrillSectionOmitted() { … }
  // AAA: interval_days=-1 / 3651 → 400 с "drill.interval_days" в перечне
  [Fact] Policy_DrillOutOfRange_Rejected400() { … }
  // AAA: interval_days=0 — валиден (выключение дрилов кластера)
  [Fact] Policy_DrillZero_Valid() { … }
  // AAA: мисматч остального тела по-прежнему 400 (замещение целиком не ослаблено)
  [Fact] Policy_BadRetentionWithDrill_Rejected400() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~BackupsPolicyApiTests` — RED на новых.
  **Spec:** §3.6 (API воркера), AC7.

- [ ] **Step 9.2: Реализация в handler**

  **Вход:** Step 9.1 (RED).
  **Действие:** `private sealed record DrillBody([property: JsonPropertyName("interval_days")] int? IntervalDays);`; в `PolicyBody` — `[property: JsonPropertyName("drill")] DrillBody? Drill;`; валидация: `var drillDays = body.Drill?.IntervalDays; if (drillDays is < 0 or > 3650) errors.Add(new("drill.interval_days", "период дрилов — целое в [0..3650]"));`; в payload: `if (drillDays is { } d) ["drill"] = new Dictionary<string, object> { ["interval_days"] = d };` (отсутствие → секция не кладётся — замещение целиком сохраняется).
  **Выход:** policy-API принимает интервал дрилов.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~BackupsPolicyApiTests` — PASS.
  **Spec:** §3.6.

- [ ] **Step 9.3: Коммит**

  **Вход:** Step 9.2 зелёный.
  **Действие:** `git add src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs && git commit -m "feat(api): policy-API бэкапов принимает drill.interval_days [0..3650] (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.6, §3.8.3.

---

### Task 10: Панель — модель DrillInfo + панельный парсер (drill-ключи, полный policy)

**Files:**
- Modify: `src/AdminPanel.Core/BackupInfo.cs` (`DrillInfo`; `ClusterBackupsInfo.ShardsDrills` + `Policy: BackupsPolicyInfo` — retention/verify/drill для формы)
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` (drill-ключ 6-го сегмента; policy — полный разбор)
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` (дополнение)

**Interfaces:**
- Consumes: формат arch/19 §4 / adminpanel/02 §2.3.1 (Task 1).
- Produces (для Tasks 11–13): `DrillInfo(string Cluster, string Shard, string Id, string State, string BackupId, long StartedUnix, long? FinishedUnix, string? Phase, string? RestoredToLsn, string? Error)`; `ClusterBackupsInfo(…, IReadOnlyDictionary<string, DrillInfo>? ShardsDrills = null, BackupsPolicyInfo? Policy = null)`; `BackupsPolicyInfo(int? RetentionDays, int? RetentionWeeks, int? RetentionMonths, long? FullMaxAgeSec, bool? VerifyOnCreate, int? DrillIntervalDays)` (null-поля = отсутствуют в policy-ключе; форма фронта читает и дефолтирует).

- [ ] **Step 10.1: Failing-тесты панельного парсера (TDD)**

  **Вход:** панельный парсер drill-ключи не читает; policy читает только full_max_age_sec.
  **Действие:** в `AdminPanel.UnitTests/BackupsParserTests.cs` добавить (AAA):
  ```csharp
  // AAA: <X>/drill → DrillInfo в ShardsDrills (state/фазы/lsn/error)
  [Fact] Parse_DrillKey_FillsShardsDrills() { … }
  // AAA: битый drill-JSON → KeyParseError + пропуск (толерантный читатель)
  [Fact] Parse_DrillKey_BadJson_ErrorAndSkip() { … }
  // AAA: drill-ключей нет → ShardsDrills null (подсистема не включена/дрилов не было)
  [Fact] Parse_NoDrillKeys_NullDictionary() { … }
  // AAA: policy полный — retention/verify/drill читаются; старая policy без drill — DrillIntervalDays=null, без ошибок
  [Fact] Parse_PolicyFull_AndLegacy_NoErrors() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~AdminPanel.UnitTests.BackupsParserTests"` — RED на новых.
  **Spec:** §3.6 (парсер), §3.1 (backwards-compat).

- [ ] **Step 10.2: Реализация модели и парсера**

  **Вход:** Step 10.1 (RED).
  **Действие:** `BackupInfo.cs` — record'ы из Interfaces (doc: панель читает, панель в etcd не пишет); `ClusterBackupsInfo` — два опциональных параметра в конец. `AdminPanel.Etcd/Parsing/BackupsParser.cs` — новая ветка `segments.Length == 6 && segments[5] == "drill"` (рядом с веткой `wal`): разбор state (RUNNING|SUCCEEDED|FAILED; незнакомое → KeyParseError + пропуск), обязательны `id`/`backup_id`/`started_unix`, опциональны finished/phase/lsn/error; словарь `drills` + джойн в `ClusterBackupsInfo.ShardsDrills`; policy-ветка — полный разбор в `BackupsPolicyInfo` (retention.days/weeks/months, full_max_age_sec — прежняя логика, verify.on_create, drill.interval_days; битое поле → null). Сборка `ClusterBackupsInfo` — с `ShardsDrills` (null если пусто) и `Policy`.
  **Выход:** панельная модель несёт дрилл и полную policy.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~AdminPanel.UnitTests"` — PASS (регресс старых парсеров).
  **Spec:** §3.6.

- [ ] **Step 10.3: Коммит**

  **Вход:** Step 10.2 зелёный.
  **Действие:** `git add src/AdminPanel.Core/BackupInfo.cs src/AdminPanel.Etcd/Parsing/BackupsParser.cs src/tests/AdminPanel.UnitTests/BackupsParserTests.cs && git commit -m "feat(panel): DrillInfo + полный policy в модели бэкапов, парсер drill-ключей (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.6.

---

### Task 11: Панельные правила — backup-drill-failed / backup-drill-stale

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupDrillFailedRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupDrillStaleRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupDrillFailedRuleTests.cs`; `src/tests/AdminPanel.UnitTests/BackupDrillStaleRuleTests.cs` (по образцу `BackupVerifyFailedRuleTests`/`RestoreFailedRuleTests`)

**Interfaces:**
- Consumes: `ClusterBackupsInfo.ShardsDrills`/`ShardLastCompletedUnix`/`Policy.DrillIntervalDays` (Task 10), `EtcdSnapshot.Clusters` (Active-гвард по образцу `RestoreFailedRule`).
- Produces: kind'ы `backup-drill-failed` (critical) и `backup-drill-stale` (warning) в `AlertEngine` (DI-автоматика `InjectAsSingleton(typeof(IAlertRule))`).

- [ ] **Step 11.1: Failing-тесты правил (TDD)**

  **Вход:** правил нет.
  **Действие:** `BackupDrillFailedRuleTests.cs` (AAA):
  ```csharp
  // AAA: последний дрилл FAILED живого Active-кластера → critical, labels drillId/backupId, текст error
  [Fact] Evaluate_FailedDrill_CriticalAlert() { … }
  // AAA: SUCCEEDED/RUNNING/фазы — без алерта
  [Fact] Evaluate_NonFailed_NoAlert() { … }
  // AAA: не-Active кластер / шард удалён из декларации — молчим
  [Fact] Evaluate_NonActiveOrGoneShard_Silent() { … }
  // AAA: drill-ключей нет (подсистема не включена) — молчим
  [Fact] Evaluate_NoDrills_Silent() { … }
  ```
  `BackupDrillStaleRuleTests.cs` (AAA):
  ```csharp
  // AAA: COMPLETED-полные есть, интервал включён (policy 1), успешного дрилла нет → warning
  [Fact] Evaluate_NoSuccess_Warning() { … }
  // AAA: успешный старше 2×interval → warning; свежий (≤ interval) → молчит
  [Fact] Evaluate_StaleSuccess_Warning_FreshSilent() { … }
  // AAA: последний FAILED не «освежает» — при включённом интервале без свежего успеха правило горит (молчание только при свежем SUCCEEDED)
  [Fact] Evaluate_LastFailed_NoFreshSuccess_Warning() { … }
  // AAA: interval_days=0 (policy) → выключено, молчит; пустой префикс бэкапов кластера (нет COMPLETED) → молчит
  [Fact] Evaluate_DisabledOrEmptyPrefix_Silent() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~BackupDrill"` — RED.
  **Spec:** §3.6 (алерты), AC5, AC6.

- [ ] **Step 11.2: Реализация правил**

  **Вход:** Step 11.1 (RED).
  **Действие:** `BackupDrillFailedRule` — образец `RestoreFailedRule`: Active-кластер с живым шардом + `ShardsDrills[X].State == "FAILED"` → Alert(Critical, KindName, `$"{C}/{X}"`, текст `$"дрилл восстановления шарда {X} кластера {C} провалился: {Error}"`, labels drillId/backupId/startedUnix, hint: «дрилл восстановления провалился — восстановимость из бэкапа не доказана», remedy: OperatorRunbook, action: «разбор по docs/backup-restore.md §Дрилл; восстановимость доказывается повторным дриллом после лечения»). `BackupDrillStaleRule` — образец `BackupFullStaleRule`: `intervalDays = Policy?.DrillIntervalDays ?? 1` (константа дефолта 1 — канон arch/19 §9); `intervalDays <= 0` → молчит; для шарда со значением `ShardLastCompletedUnix[X] != null` (есть COMPLETED): `lastSuccess = ShardsDrills[X] is { State: "SUCCEEDED", FinishedUnix: { } f } ? f : 0`; горит если `nowUnix - lastSuccess > 2L * intervalDays * 86400` (0 = «успешного не было» — горит при наличии полных); пустой префикс — словарь пуст, правило молчит; Alert(Warning, …, hint «восстановимость не доказывается — дриллы молча не исполняются (вечный transient/выключение)»).
  **Выход:** правила в AlertEngine.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~AdminPanel.UnitTests"` — PASS.
  **Spec:** §3.6, AC5/AC6.

- [ ] **Step 11.3: Коммит**

  **Вход:** Step 11.2 зелёный.
  **Действие:** `git add src/AdminPanel.Core/Alerting/Rules/BackupDrill*.cs src/tests/AdminPanel.UnitTests/BackupDrill*.cs && git commit -m "feat(panel): алерты backup-drill-failed (critical) и backup-drill-stale (warning) (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.6.

---

### Task 12: Панельный API — команда-прокси UpdateBackupsPolicyCommand + policy в DTO грани

**Files:**
- Create: `src/AdminPanel.Api/Operations/BackupsCommands.cs` (команда + handler)
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs` (`MapPut("/api/clusters/{cluster}/backups/policy", …)`)
- Modify: `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs` (`BackupShardStorageDto.Drill`; `BackupClusterStorageDto.Policy`)
- Test: `src/tests/AdminPanel.IntegrationTests/BackupsPolicyPanelApiTests.cs` (новый; фабрика `BackupsWebFactory` — как `BackupsShardApiTests`)

**Interfaces:**
- Consumes: `WorkerProxy.SendAsync` (`IWorkerApiGateway`, образец `OrphansCommands`), панельная модель Task 10, POST-эндпоинт воркера (Task 9).
- Produces (для Task 13): панельный `PUT /api/clusters/{cluster}/backups/policy` (тело: полный набор полей формы); `BackupShardStorageDto.Drill: BackupDrillBadgeDto | null`; `BackupClusterStorageDto.Policy: BackupPolicyDto | null`.

- [ ] **Step 12.1: Failing-панельные интеграции (TDD)**

  **Вход:** эндпоинта/полей нет.
  **Действие:** `BackupsPolicyPanelApiTests.cs` (AAA; сид etcd через `EtcdSeed`/`BackupsWebFactory`):
  ```csharp
  // AAA: GET деталей шарда отдаёт drill-бейдж из ключа <X>/drill
  [Fact] ShardDetails_ReturnsDrillBadge() { … }
  // AAA: GET грани отдаёт policy кластера (retention/verify/drill) из policy-ключа
  [Fact] StorageCluster_ReturnsPolicy() { … }
  // AAA: PUT policy — панель проксирует в API воркера ПОЛНОЕ тело (drill в теле), 200
  [Fact] UpdatePolicy_ProxiesFullBodyToWorker() { /* мок IWorkerApiGateway: захваченное тело содержит retention+full_max_age_sec+verify+drill */ }
  // AAA: ошибка валидации воркера (400) пробрасывается панелью
  [Fact] UpdatePolicy_Worker400_Mapped() { … }
  // AAA: живых инстансов воркера нет → 503 (образец прочих мутаций)
  [Fact] UpdatePolicy_NoWorkers_503() { … }
  ```
  **Выход:** тесты (RED).
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~BackupsPolicyPanelApiTests` — RED.
  **Spec:** §3.6 (DTO и UI/API), AC7.

- [ ] **Step 12.2: Реализация команды, эндпоинта, DTO**

  **Вход:** Step 12.1 (RED).
  **Действие:**
  1) `BackupsCommands.cs` (образец `OrphansCommands`; панель в etcd НЕ пишет):
  ```csharp
  public sealed record UpdateBackupsPolicyCommand(
      string Cluster, int RetentionDays, int RetentionWeeks, int RetentionMonths,
      long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays, string RequestedBy)
      : ICommand<string>;

  [InjectAsScoped]
  public sealed class UpdateBackupsPolicyCommandHandler(IWorkerApiGateway api)
      : ICommandHandler<UpdateBackupsPolicyCommand, string>
  {
      public async ValueTask<Result<string>> Handle(UpdateBackupsPolicyCommand c, CancellationToken ct)
          => await WorkerProxy.SendAsync<string>(api, "pgworker", HttpMethod.Post,
              $"/api/clusters/{c.Cluster}/backups/policy",
              body: new
              {
                  retention = new { days = c.RetentionDays, weeks = c.RetentionWeeks, months = c.RetentionMonths },
                  full_max_age_sec = c.FullMaxAgeSec,
                  verify = new { on_create = c.VerifyOnCreate },
                  drill = new { interval_days = c.DrillIntervalDays },
              },
              requestedBy: c.RequestedBy, ct);
  }
  ```
  2) `OperationsModule.cs`: `MapPut("/api/clusters/{cluster}/backups/policy", …)` — валидация тела панели (числа неотрицательные, drill 0..3650 — дублирование для UX, истина — сервер воркера), диспетч команды; маппинг ошибок по образцу `UpdateClusterConfigCommand` (400 валидация / 503 недоступен / 404 кластер).
  3) `BackupStorageQuery.cs`: `BackupShardStorageDto` + `BackupDrillBadgeDto? Drill` (state/phase/started/finished/lsn/error — маппинг из `ClusterBackupsInfo.ShardsDrills` в `MapShard`); `BackupClusterStorageDto` + `BackupPolicyDto? Policy` (retention/verify/drill из `ClusterBackupsInfo.Policy`; null — ключа нет, фронт дефолтирует).
  **Выход:** панельный read+mutate путь дрилла.
  **Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~AdminPanel.IntegrationTests"` — PASS.
  **Spec:** §3.6, §3.8.2.

- [ ] **Step 12.3: Коммит**

  **Вход:** Step 12.2 зелёный.
  **Действие:** `git add src/AdminPanel.Api/Operations/BackupsCommands.cs src/AdminPanel.Api/Operations/OperationsModule.cs src/AdminPanel.Api/Inspection/BackupStorageQuery.cs src/tests/AdminPanel.IntegrationTests/BackupsPolicyPanelApiTests.cs && git commit -m "feat(panel): команда-прокси UpdateBackupsPolicy + drill-бейдж/policy в DTO грани бэкапов (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.6.

---

### Task 13: Фронт — статус дрилла и поле интервала

**Files:**
- Modify: `frontend/src/api/dto.ts` (`BackupDrillBadgeDto`; `BackupShardStorageDto.drill`; `BackupClusterStorageDto.policy: BackupPolicyDto | null`)
- Modify: `frontend/src/api/queries.ts` (мутация `updateBackupsPolicy(cluster, body)` → PUT `/api/clusters/{c}/backups/policy`)
- Modify: `frontend/src/pages/backups-storage/BackupsShardDetailsPage.tsx` (блок «Дрилл восстановимости»: state/фаза/возраст/LSN/error; поле «Дрилл каждые N суток» + форма PUT полной policy)
- Modify: `frontend/src/pages/BackupsStoragePage.tsx` (строка кластера — индикатор дрилла: последний исход по шардам; при минимуме — только в деталях шарда)

**Interfaces:**
- Consumes: DTO/эндпоинты Task 12.
- Produces: UI-грань AC7 (поле интервала с PUT) и видимость per-shard статуса.

- [ ] **Step 13.1: DTO + мутация**

  **Вход:** Task 12 слит.
  **Действие:** `dto.ts` —
  ```ts
  export interface BackupDrillBadgeDto {
    state: string; phase: string | null; startedUnix: number;
    finishedUnix: number | null; restoredToLsn: string | null; error: string | null;
  }
  export interface BackupPolicyDto {
    retentionDays: number | null; retentionWeeks: number | null; retentionMonths: number | null;
    fullMaxAgeSec: number | null; verifyOnCreate: boolean | null; drillIntervalDays: number | null;
  }
  // BackupShardStorageDto += drill: BackupDrillBadgeDto | null;
  // BackupClusterStorageDto += policy: BackupPolicyDto | null;
  ```
  `queries.ts` — мутация `useUpdateBackupsPolicy` (PUT, invalidates запросы грани).
  **Выход:** типизированный API-клиент фронта.
  **Проверка:** `cd frontend && npm run typecheck` — без ошибок.
  **Spec:** §3.6 (DTO и UI).

- [ ] **Step 13.2: UI — блок дрилла и форма интервала**

  **Вход:** Step 13.1.
  **Действие:** `BackupsShardDetailsPage.tsx`: блок «Дрилл восстановимости» (Badge: SUCCEEDED зелёный / FAILED красный / RUNNING синий с фазой `downloading|recovering|cleaning`; возраст от started/finished; `restored_to_lsn`; error-строка) — по образцу блока restore-бейджа; ниже — форма «Дрилл каждые N суток» (NumberInput 0..3650, 0 = выкл; кнопка «Сохранить»): читает текущие значения из `policy` кластера (дефолты фронта при null: 7/4/6/86400/true/1), PUTит ПОЛНЫЙ набор (`updateBackupsPolicy`) — как требует AC7/канон замещения; успех/ошибка — Notifications. `BackupsStoragePage.tsx`: в строке кластера — компактный бейдж «дрилл: ok/failed/—» (агрегат по шардам из тех же данных; реализация по вкусу существующих сводок).
  **Выход:** видимая грань дрилла.
  **Проверка:** `cd frontend && npm run build` — успешно (tsc + vite).
  **Spec:** §3.6, AC7.

- [ ] **Step 13.3: Коммит**

  **Вход:** Step 13.2 зелёный.
  **Действие:** `git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/backups-storage/BackupsShardDetailsPage.tsx frontend/src/pages/BackupsStoragePage.tsx && git commit -m "feat(frontend): статус дрилла per-shard и поле интервала с PUT-policy (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.6.

---

### Task 14: Runbook — раздел «Дрилл восстановимости»

**Files:**
- Modify: `docs/backup-restore.md` (новый раздел после основных; тон по `docs/runbook.md`)

**Interfaces:**
- Consumes: механика Tasks 6–9; arch/19 §3.6 (Task 1).
- Produces: операционный текст AC11.

- [ ] **Step 14.1: Раздел runbook**

  **Вход:** все предыдущие задачи слиты (кроме E2E).
  **Действие:** добавить раздел «Дрилл восстановимости»: что такое ключ `drill` и его фазы (`downloading|recovering|cleaning`; снятая фаза у терминального = контур подтверждённо снесён; `RUNNING` = идёт); чтение статуса (`etcdctl get /pgworker/backups/<C>/<X>/drill` + панель, грань «Хранилище бэкапов»); действия при FAILED (разбор `error`: «полный без манифеста» / границы WAL-дыры — лечение по разделам verify/restore; восстановимость доказывается повторным дриллом после лечения — следующий по `interval_days`); настройка интервала (curl POST policy-API c `"drill":{"interval_days":N}` + панель; 0 — выключение; применяется следующим проходом без рестарта воркера); явная строка: дрилл не влияет на Patroni-контур и synchronous-режим мастера — нод в кластере не создаёт, HA-scope не трогает, пишет только в свой ephemeral volume `pgw-backup-drill-*`; взаимосвязь с verify (verify — целостность файлов, дрилл — разворачиваемость: старт postgres и выход из recovery; зелёный verify при красном дрилле = проблема механики разворачивания — разбор по `error` статуса дрилла).
  **Выход:** runbook-раздел.
  **Проверка:** визуально; grep `Дрилл восстановимости` docs/backup-restore.md.
  **Spec:** §3.7, AC11.

- [ ] **Step 14.2: Коммит**

  **Вход:** Step 14.1.
  **Действие:** `git add docs/backup-restore.md && git commit -m "docs: раздел «Дрилл восстановимости» в runbook бэкапов (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §3.7.

---

### Task 15: Docker-E2E — Drill_Succeeds_CleansUp (маркер) + Drill_Failed_OnCorruptedWal

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs` (два Fact'а; окружение `E2eEnvironment.StartAsync(slug, withMinio: true)`, хелперы `SeedClusterAsync`/`StartBackupHostAsync`/`FullKeysAsync`)

**Interfaces:**
- Consumes: вся система (Tasks 2–9); `E2eFixture.WaitForAsync`; образ `pgworker-backup:e2e` (`E2eEnvironment.JobImage`); `DockerTrait.SkipIfUnavailable`.
- Produces: кейс-маркер `Drill_Succeeds_CleansUp` для мерж-гейта (Task 16).

- [ ] **Step 15.1: Кейс Drill_Succeeds_CleansUp (маркер)**

  **Вход:** хелперы E2eBackupScenarios изучены (Backup_Verify_Ok_OnCreate — образец).
  **Действие:** добавить Fact (AAA; ОДИН Fact — одно окружение, teardown в `DisposeAsync` базового класса уже есть — `Fx` поле):
  ```csharp
  // AAA (AC1/AC3/AC4): первый дрилл стартует сам после COMPLETED-полного,
  // докатывает WAL, выходит из recovery (SUCCEEDED + restored_to_lsn),
  // контур снесён (контейнер/volume отсутствуют, phase снят)
  [Fact]
  public async Task Drill_Succeeds_CleansUp()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await using var fx = await E2eEnvironment.StartAsync("bk-drill", withMinio: true, ct: ct);
      Fx = fx;
      var cluster = $"bkdrll{Fx.ClusterTag}";
      await SeedClusterAsync(cluster);
      await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
          """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);
      await using var app = await StartBackupHostAsync("bkdrill", ct);

      // Act 1 — COMPLETED-полный (бюджет 300 c)
      var completed = await E2eFixture.WaitForAsync(
          () => FullKeysAsync(cluster, "shard1").ContinueWith(t => t.Result.Any(f => f.Value.Contains("COMPLETED"))),
          TimeSpan.FromSeconds(300), ct);
      completed.Should().BeTrue("полный обязан сняться");

      // Act 2 — дрилл дошёл до SUCCEEDED (бюджет 600 c: скачать + накат)
      var succeeded = await E2eFixture.WaitForAsync(async () =>
      {
          var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
          return kv.Value?.Value.Contains("\"SUCCEEDED\"") == true;
      }, TimeSpan.FromSeconds(600), ct);
      succeeded.Should().BeTrue("первый дрилл стартует немедленно и обязан выйти из recovery");

      // Assert — restored_to_lsn в ключе; чистый итог: без phase
      var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
      drill.Should().Contain("\"restored_to_lsn\":\"", "LSN восстановления фиксируется");
      drill.Should().NotContain("\"phase\"", "чистый терминальный итог — контур снесён");

      // Assert — прод-ноды шарда живы весь дрилл (AC2: изоляция — Patroni-контур не тронут)
      var nodes = await Fx.RunDockerAsync(
          ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-shard1-"], ct);
      nodes.Length.Should().BeGreaterThan(0, "дрилл не демонтирует прод-ноды шарда (state RUNNING)");

      // Assert — контейнер и volume дрилла отсутствуют (ассерт чистоты, AC4/AC2)
      var cleaned = await E2eFixture.WaitForAsync(async () =>
      {
          var containers = await Fx.RunDockerAsync(
              ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
          var volumes = await Fx.RunDockerAsync(
              ["volume", "ls", "-q", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
          return containers.Length == 0 && volumes.Length == 0;
      }, TimeSpan.FromSeconds(60), ct);
      cleaned.Should().BeTrue("drill-контур сносится после итога");

      // Assert — оба парсера без ошибок; панельные правила молчат на успехе (AC5)
      var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
      var parsed = BackupsParser.Parse(kvs, out var parseErrors);
      parseErrors.Should().BeEmpty();
      var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs);
      panel.Errors.Should().BeEmpty();
      var panelCluster = panel.Clusters.Single(c => c.Cluster == cluster);
      var failedAlerts = new BackupDrillFailedRule().Evaluate(
          SnapshotWith(panelCluster, activeCluster: cluster), DefaultAlertContext()).ToList();
      failedAlerts.Should().BeEmpty("SUCCEEDED — без алерта провала");
      var staleAlerts = new BackupDrillStaleRule().Evaluate(
          SnapshotWith(panelCluster, activeCluster: cluster), DefaultAlertContext()).ToList();
      staleAlerts.Should().BeEmpty("свежий SUCCEEDED — без алерта молчания");
  }
  ```
  (Хелперы `SnapshotWith`/`DefaultAlertContext` — локальные в классе: сборка минимального `EtcdSnapshot`+`AlertContext` по образцу панельных юнитов правил; фактические kvs кормят правила — «панельный алерт» в E2E.)
  **Выход:** кейс-маркер.
  **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Drill_Succeeds_CleansUp` — PASS; после прогона — зачистка серии: контейнеры `docker ps -a --format '{{.Names}}' | grep pgw-` (пусто), сети `docker network ls | grep kfw-net` → `docker network prune -f` при остатках.
  **Spec:** §4 Фаза 5, AC1–AC5, AGENTS.base §12.

- [ ] **Step 15.2: Кейс Drill_Failed_OnCorruptedWal**

  **Вход:** Step 15.1 зелёный; серия зачищена.
  **Действие:** добавить Fact (AAA; детерминированный провал без гонки: запуск дрилов отключен глобально env, порча WAL, затем ВКЛЮЧЕНИЕ policy-полем — тестирует и применение policy на лету):
  ```csharp
  // AAA (AC1/AC3/AC5): дыра WAL-цепочки → валидационный FAILED без джоба
  // (контейнера/volume нет, ключ без phase) + панельный алерт backup-drill-failed
  [Fact]
  public async Task Drill_Failed_OnCorruptedWal()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await using var fx = await E2eEnvironment.StartAsync("bk-drillf", withMinio: true, ct: ct);
      Fx = fx;
      var cluster = $"bkdrfl{Fx.ClusterTag}";
      await SeedClusterAsync(cluster);
      await using var app = await Fx.StartHostAsync("bkdrillf", extraEnv: new Dictionary<string, string>
      {
          /* StartBackupHostAsync-набор + */ ["PgWorker__Backups__Drill__IntervalDays"] = "0",
      }, ct: ct);

      // Arrange — COMPLETED-полный + wal-сегменты снялись; дрилов нет (выключены)
      var completed = await E2eFixture.WaitForAsync(
          () => FullKeysAsync(cluster, "shard1").ContinueWith(t => t.Result.Any(f => f.Value.Contains("COMPLETED"))),
          TimeSpan.FromSeconds(300), ct);
      completed.Should().BeTrue("полный обязан сняться");
      // портим: mc rm ПЕРВЫЙ объект-сегмент префикса wal/ (разрывает цепочку от wal_start)
      await Fx.RunDockerAsync(["run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh",
          E2eEnvironment.McImage, "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
          + $" && OBJ=$(mc ls t/{Bucket}/{cluster}/shard1/wal/ | awk 'NR==1{{print $NF}}')"
          + $" && mc rm \"t/{Bucket}/{cluster}/shard1/wal/$OBJ\""], ct);

      // Act — включаем дриллы policy-полем (применяется следующим тиком без рестарта)
      await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
          """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);

      // Assert — валидационный FAILED (бюджет 120 c: тик + валидация list)
      var failed = await E2eFixture.WaitForAsync(async () =>
      {
          var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
          return kv.Value?.Value.Contains("\"FAILED\"") == true;
      }, TimeSpan.FromSeconds(120), ct);
      failed.Should().BeTrue("дыра цепочки — честный исход «восстановимость не доказана»");
      var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
      drill.Should().Contain("дыра", "причина WalChain — в error").And.NotContain("\"phase\"", "без джоба — чистый итог");

      // Assert — контейнеров/volume дрилла нет вовсе; панельный алерт провала горит
      var containers = await Fx.RunDockerAsync(
          ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
      containers.Should().BeEmpty("джоб не запускался — валидационный отказ");
      var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
      var panelCluster = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs)
          .Clusters.Single(c => c.Cluster == cluster);
      var alerts = new BackupDrillFailedRule().Evaluate(
          SnapshotWith(panelCluster, activeCluster: cluster), DefaultAlertContext()).ToList();
      alerts.Should().ContainSingle(a => a.Kind == "backup-drill-failed", "провал дрилла — critical-алерт");
  }
  ```
  **Выход:** негативная ветка.
  **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Drill_Failed_OnCorruptedWal` — PASS; зачистка серии (как 15.1).
  **Spec:** §4 Фаза 5 (негативная ветка), AC3/AC5.

- [ ] **Step 15.3: Коммит**

  **Вход:** Steps 15.1–15.2 зелёные.
  **Действие:** `git add src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs && git commit -m "test(e2e): Drill_Succeeds_CleansUp + Drill_Failed_OnCorruptedWal (t02-restore-drill)"`
  **Выход:** коммит.
  **Проверка:** `git show --stat HEAD`.
  **Spec:** §4 Фаза 5.

---

### Task 16: Мерж-гейт — полный прогон, кейс-маркеры, roadmap-тег + reliability-report

**Files:**
- Modify: `arch/roadmap/reliability.md` (снять пункт `t02-restore-drill`)
- Modify: `arch/roadmap/reliability-report.md` (строку t02 из «Осталось» перенести в «Сделано» + правка сводки D)

**Interfaces:**
- Consumes: все задачи слиты; AGENTS.md (мерж-гейт E2E на свежем Release); правило мерж-гейта трека reliability.
- Produces: ветка, готовая к мержу (сам мерж в `main` — ТОЛЬКО по отдельной команде пользователя).

- [ ] **Step 16.1: Полный прогон без docker (юниты + интеграции)**

  **Вход:** Tasks 1–15 слиты.
  **Действие:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release` — все тестовые проекты (PgWorker/AdminPanel/Kafka/Valkey/Shared юниты и интеграции без docker-гейтов).
  **Выход:** зелёный прогон.
  **Проверка:** итоговая строка `Passed!` без failed; зачистка остаточных контейнеров/сетей серии при их появлении.
  **Spec:** AC12.

- [ ] **Step 16.2: Docker-E2E кейс-маркеры на свежем Release**

  **Вход:** Step 16.1 зелёный.
  **Действие:** серия 1 — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` (кейс-маркер AGENTS.md; E2eFixture соберёт Release сама — инкрементальный no-op); дождаться финальной строки, зачистить серию (контейнеры/сети). Серия 2 — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Drill_Succeeds_CleansUp|FullyQualifiedName~Drill_Failed_OnCorruptedWal"` (drill-кейс гейта); зачистить серию. НЕ запускать серию поверх незачищенной предыдущей.
  **Выход:** оба маркера зелёные на свежем Release-бинарнике.
  **Проверка:** `Passed!` в обеих сериях; `docker ps -a --format '{{.Names}}' | grep -c pgw-` → 0; `docker network ls | grep -c kfw-net` → 0 (иначе `docker network prune -f` и разбор причины).
  **Spec:** AC12, AGENTS.md (мерж-гейт), §4 Фаза 5.

- [ ] **Step 16.3: Снятие roadmap-тега + строка reliability-report (тем же коммитом)**

  **Вход:** Step 16.2 зелёный.
  **Действие:** ОДИН коммит: (1) `arch/roadmap/reliability.md` — удалить пункт `**t02-restore-drill**` (строку из списка задач; проверить `←`-зависимости других пунктов на t02 — их нет по текущему файлу, но проверить grep `t02-restore-drill`); (2) `arch/roadmap/reliability-report.md` — перенести строку t02 из перечня открытых разрывов в «Сделано» (формат раздела), в сводке D убрать фразу «восстановимость не доказывается регулярно — drill нет (t02)» и добавить краткое описание дрилла (по формату сводки). Никаких пометок «закрыта» в надёжности списков.
  ```bash
  git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
  git commit -m "chore(roadmap): t02-restore-drill слит — тег снят, отчёт надёжности обновлён (reliability merge-gate)"
  ```
  **Выход:** roadmap-гейт трека reliability закрыт тем же коммитом.
  **Проверка:** `grep -rn "t02-restore-drill" arch/roadmap/` — 0 вхождений (кроме, при наличии, исторических сносок отчёта «Сделано» — там строка остаётся с merge-фактом); `git show --stat HEAD` — ровно 2 файла.
  **Spec:** AC12, roadmap README (мерж-гейт трека).

- [ ] **Step 16.4: Итоговая готовность ветки (мерж — по команде пользователя)**

  **Вход:** Step 16.3 выполнен.
  **Действие:** `git log --oneline main..t02-restore-drill` — сводка коммитов для ревью; доложить пользователю: план исполнен, гейты зелёные, ветка готова к ревью/мержу. В `main` НЕ мержить и НЕ пушить — только по явной команде.
  **Выход:** ветка готова.
  **Проверка:** рабочий каталог чист (`git status --porcelain` — пусто).
  **Spec:** AC11/AC12, AGENTS.base §7.

---

## Self-review (выполнен при написании плана)

- **Покрытие spec:** §3.1 → Tasks 1/2/5/10; §3.2 → Task 8 (+1.3); §3.3 → Tasks 3/6/7/8; §3.4 → Task 4; §3.5 → Tasks 8/1.1; §3.6 → Tasks 9/10/11/12/13; §3.7 → Task 14; §3.8 → Task 1; §4 фазы 1–5 → Tasks 2–5 / 6–7 / 8–9 / 10–13 / 14–16; AC1→15.1/7.x; AC2→15.1+7; AC3→15.1/15.2/6.2; AC4→6.2/6.3/15.1; AC5→11/15.1/15.2; AC6→11; AC7→9/12/13/15.2; AC8→3/7; AC9→6/7; AC10→7/8; AC11→1/14; AC12→16.
- **Плейсхолдеры:** шаги содержат конкретные файлы/сигнатуры/критерии; кодовые блоки — образцы требуемой формы (не полные тела — исполнитель пишет по образцам с указанными путями).
- **Типы:** `DrillState`/`DrillStatus`/`BackupPolicy.DrillIntervalDays`/`ShardBackups.Drill` (Task 2) используются единообразно в Tasks 3/5/6/7/10; имена `BackupNames.Drill*` — в Tasks 4/6/7/8; сигнатура `TickAsync(snap, backups, ct)` — Tasks 6/7/8.
- **Ограничения проекта:** arch-first (Task 1 первым, отдельный коммит), TDD (каждая задача RED→GREEN), E2E-каноны §12 (своё окружение, teardown, ассерт чистоты, зачистка серий, таймауты WaitFor), мерж-гейт reliability (16.3 тем же коммитом), язык RU/EN.
