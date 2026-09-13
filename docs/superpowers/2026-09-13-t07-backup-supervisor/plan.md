# t07-backup-supervisor — план реализации (супервизор-цикл бэкапов)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Замкнуть подсистему бэкапов (t02–t06) самовосстанавливающимся контуром: BROKEN-самолечение WAL-цепочки пересъёмом полного, возрастные бюджеты зависших джобов (kill+FAILED+переснятие), сверка S3↔etcd (мусор живых шардов — сразу, сироты исчезнувших владельцев — реестр+TTL) и панельные алерты `backup-chain-broken`/`backup-orphan`.

**Architecture:** Изменение контракта — сначала в `arch/19-backups.md` (фаза 0). Затем: (1) четвёртое значение `state=BROKEN` в wal-ключе заменяет in-memory `_chainBroken` (переживает рестарт/takeover), ratchet `chain_start_segment` не понижается, `BackupPlanner.IsDue` получает признак разрыва; (2) чистые решения `SupervisionTimeouts` применяются супервизией существующих процессов (BackupProcess/BackupVerifyProcess/RestoreProcess); (3) новый `BackupSupervisorProcess` (per-cluster, клэйм `<C>`) чистит мусор `full/<id>/` без etcd-ключа, глобальный `BackupOrphanSweeperLoop` (лидер `/pgworker/leader`, паттерн SnapshotLoop) ведёт реестр `/pgworker/backups/orphans` с TTL; (4) панель вычисляет два новых алерта над снапшотом; (5) docker-E2E на свежем Release.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions, реальный etcd (testcontainers) в интеграциях, FakeBackupDeps-фейки, docker-E2E (E2eEnvironment, MinIO).

**Spec:** `docs/superpowers/2026-09-13-t07-backup-supervisor/spec.md` — план аргументируется от спека; исполнители читают оба документа. Канон подсистемы — `arch/19-backups.md`.

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — сборка без ворнингов или не сборка.
- Русский язык комментариев/доков, английский — идентификаторы. Тесты — AAA (Arrange/Act/Assert комментарии).
- Префикс `/pgworker/backups/*` пишет ТОЛЬКО PgWorker: per-cluster часть — под клэймом `<C>`, глобальный ключ `orphans` — только глобальным лидером `/pgworker/leader` (arch/19 §4, spec §2).
- Паттерны arch/17: идемпотентность каждого шага, journal-before-manipulations, transient (S3/etcd/docker-транспорт — статус не меняем, тик повторит) vs permanent (FAILED с причиной).
- Удаляющий воркер консервативен: мгновенно — только `full/<id>/` живого шарда без etcd-ключа; всё, что может иметь DR-ценность, — TTL 7 сут + алерт (spec §2).
- Воркер алертов НЕ пишет — панельные правила над снапшотом (arch/19 §4).
- Тесты: порты docker-контейнеров динамические (`WithPortBinding(..., assignRandomHostPort: true)` / зонд свободных портов) — никаких хардкодов вида `:16000`; после КАЖДОЙ тестовой серии — `docker rm -f $(docker ps -aq)` (кроме стендовых `as-*`/`adminpanel`, если стенд поднят) + `docker network prune -f`; `docker ps -aq | wc -l == 0` перед следующей серией.
- E2E — только на свежем Release по правилам `docs/e2e-launch.md` (docker-логи/inspect/host.log в teardown ДО удаления, фазы >60 с — сбор логов + `[PHASE]`, MarkFailed → stop-режим); изоляция по `docs/e2e-isolation.md` (guid-имена, own-only teardown, ассерт чистоты). Перезапуск упавших E2E «выяснить что было» — запрещён.
- Каждая задача плана завершается коммитом; коммит/пуш — по флоу dev-flow (мы в worktree `feat-t07-backup-supervisor`, ветка уже существует).
- Конфиг-дефолты из решений пользователя (spec): `Supervisor { IntervalSec=600, OrphanTtlSec=604800 }` (0 — только алерт), `Job { FullTimeoutSec=21600, VerifyTimeoutSec=21600, RestoreTimeoutSec=86400 }`.

---

### Task 0: Фаза 0 — правки контракта arch/19 (arch-first, до кода)

**Files:**
- Modify: `arch/19-backups.md` (§2, §3, §4, §6, §8, §9, §10)

**Interfaces:**
- Consumes: spec §5 «Фаза 0».
- Produces: канон, по которому пишутся все дальнейшие задачи; §4-таблица wal-ключа с `BROKEN` + ratchet; глобальный ключ `/pgworker/backups/orphans`; §9-конфиг `Supervisor`/`Job`; §8-строка t07; §10-риски.

- [ ] **Step 1: §3 — дыра/слот → BROKEN**

Заменить абзац (строка ~192–196 канона):

```
Дыра (нет next-сегмента / TLI-переход без history) → `DEGRADED` +
`error` c границами дыры; лечение — переснятие полного (t07/t02),
t03 сигнализирует.
```

на:

```
Дыра (нет next-сегмента / TLI-переход без history) и исчезновение слота
при живой цепочке → `BROKEN` (t07): permanent-деградация разрыва, агент
остановлен, лечение — НОВЫЙ полный бэкап (планировщик §2 реагирует на
BROKEN пересъёмом); `error` несёт границы разрыва. `DEGRADED` — только
transient-деградации (lag/тишина): агент жив, ретраи тиками, пересъём НЕ
триггерит. Слот при BROKEN: жив — не трогаем (копит WAL в пределах
max_slot_wal_keep_size), исчез — пересоздаётся immediate+reserved сразу
же (к старту пересъёма полного слот держит позицию ≤ wal_start нового
полного; механика первого старта). Ratchet `chain_start_segment`:
записанное значение НИКОГДА не понижается; в BROKEN-записи — граница
разрыва (последний непрерывный сегмент контроля; для «слот исчез» —
`last_uploaded_segment` на момент обнаружения). При контроле цепочки
стартовая точка = min(wal_start_segment COMPLETED-полных с wal_start ≥
записанного chain_start) — полные со стартом ниже границы разрыва
игнорируются для контроля (их цепь до их точки может быть цела; дыра
выше). Контроль при BROKEN — каждый тик (без VerifyIntervalSec-расчёта:
скорость заживления; list дырного префикса дёшев); появился COMPLETED-
полный со стартом ≥ границы и цепь от него непрерывна → `ACTIVE`, агент
поднимается этим же тиком. Restore COMPLETED удаляет wal-ключ целиком
(t05 AC4) — ratchet сбрасывается вместе с ключом, противоречий нет.
```

Также в §3 абзац «Слот на мастере» (~строка 156–160) ничего; §6 строку риска «по исчерпании слот инвалидируется PG → цепочка рвётся → алерт t03 + переснятие полного (t07)» оставить как есть (уже про t07).

- [ ] **Step 2: §2 — планировщик due по BROKEN**

В §2 абзац планировщика после `> full_max_age_sec` **или ключ `wal` шарда отсутствует**` (t05: ...) дополнить: «**или wal-ключ шарда в `BROKEN`** (t07: разрыв цепочки — пересъём безусловно, инвариант «у живого шарда валидная цепочка или активный полный»; бэкофф серии FAILED работает как всегда — шторм пересъёмов исключён)».

- [ ] **Step 3: §4 — таблица wal-ключа + новый глобальный ключ orphans + гибрид сирот**

В таблице ключей строку `/pgworker/backups/<C>/<X>/wal` заменить значение `state` на `"ACTIVE|DEGRADED|STOPPED|BROKEN"` и дополнить описание после таблицы (в «Правилах»):

```
`BROKEN` — разрыв цепочки (дыра WalChain / исчезновение слота при живой
цепочке): агент остановлен, лечение — пересъём полного (§2/§3, t07).
`chain_start_segment` в BROKEN-записи — граница разрыва; ratchet —
значение никогда не понижается (§3).
```

Добавить в таблицу новую строку глобального ключа (после `storage`):

```
| `/pgworker/backups/orphans` | реестр осиротевших S3-префиксов (t07): `{"orphans":[{"prefix":"<C>/<X>","kind":"shard|cluster","size_bytes":N,"first_seen_unix":T,"state":"OBSERVED|DELETING"}],"updated_unix":T}`; ключ ГЛОБАЛЬНЫЙ (вне per-cluster префиксов, D2-чистки не касается); пишет ТОЛЬКО глобальный лидер-проход `/pgworker/leader` (паттерн storage/supervisor). `first_seen_unix` переносится из предыдущей записи (merge — TTL от первого наблюдения); владелец воскрес (кластер/шард появился в `/clusters/`) → запись удаляется, идущая DELETING-доводка отменяется; OBSERVED старше `Supervisor:OrphanTtlSec` → DELETING (journal) → batch-delete префикса → list-подтверждение → del записи; один префикс за проход; `OrphanTtlSec=0` — авто-удаление выключено, только алерт панели |
```

Уточнить абзац «Deprovisioning кластера» (конец §4): «префикс S3 без etcd-владельца = orphan, его видит супервизор (t07)» → «префикс S3 без etcd-владельца попадает в реестр сирот супервизора t07: алерт панели + удаление по `Supervisor:OrphanTtlSec` (гибрид: мусор живого шарда — `full/<id>/` без ключа — удаляется немедленно per-cluster-проходом; исчезнувшие владельцы — реестр+TTL)».

В абзаце «Ретенция … ЕДИНСТВЕННЫЙ осознанный удаляющий S3-объектов воркера» заменить на: «Ретенция и супервизор t07 — осознанные удаляющие S3-объектов воркера (ретенция — по политике GFS/cutoff; супервизор — только мусор `full/<id>/` живого шарда без etcd-ключа и сироты по TTL): стоп-семантика/deprovisioning объекты по-прежнему не трогают (R4, §3/§4)».

- [ ] **Step 4: §6 — бюджеты зависших джобов**

В §6 после абзаца «Лимиты джоба/агента» добавить:

```
- **Бюджеты зависших джобов (t07)**: активный полный (PLANNED/RUNNING/
  UPLOADING) с возрастом (`now − started_unix`) > `Job:FullTimeoutSec`
  (default 6 ч) → docker kill+rm контейнера и staging-volume → `FAILED`
  `error="job-timeout: <age> с > FullTimeoutSec"` → переснятие по общему
  бэкоффу. Verify-джоб running дольше `Job:VerifyTimeoutSec` (default 6 ч;
  возраст — docker-факт StartedAt инспекта: в etcd-статусе кандидата
  времени запуска нет) → kill+rm, кандидат остаётся PENDING с
  `checked_unix = now` (попытка зачтена, лив-лок немедленных перезапусков
  исключён; вердикт FAILED по таймауту НЕ ставится — данные не виноваты).
  Restore-джоб (фаза RUNNING заявки) старше `Job:RestoreTimeoutSec`
  (default 24 ч — канон §10 допускает «часами», сутки — явный завис) →
  kill+rm + `FAILED` `restore-job-timeout` с чисткой щита initialize;
  REJOINING не таймаутится (бюджеты Patroni-проб — свои, arch/14).
```

- [ ] **Step 5: §8 — строка t07; §9 — конфиг; §10 — риски**

§8, строка t07 заменить на: `| t07-backup-supervisor | reconcile S3↔etcd, сироты (реестр+TTL), BROKEN-самолечение цепочки, бюджеты зависших джобов, рестарт-устойчивость (§4, §6) |`.

§9 после `Restore { RecoveryTimeoutSec=1800 }` дополнить: `, Supervisor { IntervalSec=600, OrphanTtlSec=604800 }` (t07: период сверок per-cluster и глобального лидер-прохода; TTL сирот, `0` — только алерт) и `Job { Image="pgworker-backup:dev", FullTimeoutSec=21600, VerifyTimeoutSec=21600, RestoreTimeoutSec=86400 }` (t07: бюджеты зависших джобов). В конец §9-абзаца о валидации добавить: «отрицательные таймауты/TTL супервизора — fail-fast».

§10 добавить три строки:

```
| Таймаут зависшего джоба ложноположителен на очень медленном валидном бэкапе (большая база/медленный S3) | дефолт 6 ч с запасом поверх «часовых» бэкапов; конфиг Job:FullTimeoutSec; FAILED по таймауту проходит общий бэкофф — шторм пересъёмов исключён |
| DELETING-сирота vs воскресший владелец (DR-restore в окне удаления) | TTL-окно 7 сут покрывает разбор; гвард: владелец появился в etcd → доводка отменяется, запись гаснет; остаточный риск (частично удалённое при самом старте DR) зафиксирован |
| Ratchet chain_start vs restore/PITR-назад | restore COMPLETED удаляет wal-ключ целиком (t05 AC4) — ratchet уходит вместе с ключом; новая цепочка строится планировщиком с нуля |
```

- [ ] **Step 6: Проверка и коммит**

Run: `grep -c "BROKEN" arch/19-backups.md` → Expected: ≥ 5 (§2/§3/§4/§8 покрыты); `grep -c "orphans" arch/19-backups.md` → ≥ 2.
Убедиться, что документ линкуется (markdown не проверяется сборкой — визуальная сверка структуры с существующими таблицами).

```bash
git add arch/19-backups.md
git commit -m "docs(arch): t07-backup-supervisor — канон супервизора бэкапов (arch/19 §2/§3/§4/§6/§8/§9/§10): state=BROKEN + ratchet, ключ orphans (реестр+TTL), бюджеты зависших джобов, гибрид сирот"
```

**Связь со spec:** §5 фаза 0, AC8. **Выход:** канон обновлён — весь дальнейший код аргументируется от него.

---

### Task 1: Модель `BROKEN` — `WalStreamStatus.Broken` + `WalStatusWriter`

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` (enum `WalStreamStatus`)
- Modify: `src/PgWorker.Backups/WalStatusWriter.cs` (`StateName`/`StateOf`)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalStatusWriterTests.cs`

**Interfaces:**
- Consumes: контракт arch/19 §4 (Task 0).
- Produces: `WalStreamStatus.Broken` (enum-значение); сериализация `"BROKEN"` в `WalStatusWriter.ToJson`, парсинг `"BROKEN"` → `Broken` в `StateOf`. Все задачи фаз 1–4 используют это значение.

- [ ] **Step 1: Падающий тест сериализации/парсинга BROKEN**

В `WalStatusWriterTests.cs` (по образцу существующих roundtrip-тестов файла) добавить:

```csharp
// AAA: BROKEN пишется как "BROKEN" и читается обратно (контракт arch/19 §4, t07)
[Fact]
public void ToJson_StateBroken_ПишетBROKEN()
{
    // Arrange — состояние разрыва цепочки с границей в chain_start
    var state = new WalStreamState(
        WalStreamStatus.Broken, "pgw_bkp_c1_shard1", "shard1a",
        "000000010000000000000005", "000000010000000000000005",
        "000000010000000000000005", 1757500000, null, "дыра WAL-цепочки …");

    // Act
    var json = WalStatusWriter.ToJson(state);

    // Assert — машиночитаемое поле state=BROKEN (spec §3.1)
    json.Should().Contain("\"state\":\"BROKEN\"");
}
```

и симметричный тест `Parse_БрокенКлюч_ЧитаетBroken` (сид JSON-строки с `"state":"BROKEN"` через публичный путь чтения `WalStatusWriter.ReadAsync` — по образцу существующих тестов чтения; либо через `ToJson`→`Parse` roundtrip, если чтение в файле тестируется через фикстуру etcd — тогда roundtrip: `ToJson(state)` → put/read недоступен в юните, поэтому Assert на `StateOf`-эквивалент: десериализация payload-строки внутренним путём файла; следовать существующему паттерну тестов этого файла).

- [ ] **Step 2: Прогнать тест — упал**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WalStatusWriterTests"`
Expected: FAIL — `WalStreamStatus.Broken` не существует (ошибка компиляции).

- [ ] **Step 3: Реализация**

`BackupsModel.cs`:

```csharp
/// <summary>Состояние WAL-потока шарда (arch/19 §4; t07: Broken — разрыв
/// цепочки/слот исчез, лечение — пересъём полного; DEGRADED — только
/// transient lag/тишина).</summary>
public enum WalStreamStatus
{
    Active,
    Degraded,
    Stopped,
    Broken,
}
```

`WalStatusWriter.StateName` добавить ветку `WalStreamStatus.Broken => "BROKEN"`, `StateOf` — `"BROKEN" => WalStreamStatus.Broken`.

- [ ] **Step 4: Прогнать тест — зелёный; вся сборка зелёная**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WalStatusWriterTests"`
Expected: PASS.
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx`
Expected: 0 errors/warnings (новое значение enum не ломает существующие switch — проверить exhaustiveness-ворнинги в `StateName`, `_ => "ACTIVE"`-fallback уже держит).

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Etcd/Parsing/BackupsModel.cs src/PgWorker.Backups/WalStatusWriter.cs src/tests/PgWorker.UnitTests/Backups/WalStatusWriterTests.cs
git commit -m "feat(backups): state=BROKEN в wal-ключе — модель и WalStatusWriter (arch/19 §4, t07)"
```

**Связь со spec:** §3.1 (модель), AC8. **Выход:** контрактное значение `BROKEN` доступно воркеру.

---

### Task 2: Чистые решения — ratchet `RatchetedStart` + `BackupPlanner.IsDue(walChainBroken)`

**Files:**
- Modify: `src/PgWorker.Backups/Model/WalChain.cs`
- Modify: `src/PgWorker.Backups/Process/BackupPlanner.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs`

**Interfaces:**
- Consumes: `WalFileName` (`TryParse`, `Name`, сравнение Ordinal).
- Produces:
  - `public static WalFileName? WalChain.RatchetedStart(WalFileName? recorded, IEnumerable<string?> completedWalStarts)` — стартовая точка контроля с ratchet.
  - `public static bool BackupPlanner.IsDue(IReadOnlyList<FullBackupState> fulls, bool walKeyExists, long fullMaxAgeSec, long nowUnix, long? lastRestoreFinishedUnix = null, bool walChainBroken = false)` — сигнатура по spec §3.2.

- [ ] **Step 1: Падающие юниты ratchet**

В `WalChainTests.cs` (AAA-комментарии, по стилю файла):

```csharp
// AAA: ratchet — полные со стартом НИЖЕ границы игнорируются (AC1: контроль не
// видит дыру снова от старого полного)
[Fact]
public void RatchetedStart_ПолныеНижеГраницы_Игнорируются()
{
    // Arrange — записанная граница ..05; полные ..01 (старый дырный) и ..09
    var recorded = WalFileName.TryParse("000000010000000000000005");

    // Act
    var start = WalChain.RatchetedStart(recorded,
        ["000000010000000000000001", "000000010000000000000009"]);

    // Assert — контроль от ..09 (min кандидатов ≥ границы), не от ..01
    start!.Value.Name.Should().Be("000000010000000000000009");
}

// AAA: ratchet — кандидатов выше границы нет → записанная точка держится
[Fact]
public void RatchetedStart_НетКандидатов_ВозвращаетЗаписанную()
{
    // Arrange
    var recorded = WalFileName.TryParse("000000010000000000000005");

    // Act
    var start = WalChain.RatchetedStart(recorded, ["000000010000000000000001"]);

    // Assert
    start.Should().Be(recorded);
}

// AAA: записи нет — min всех COMPLETED-стартов (поведение t03 сохранено)
[Fact]
public void RatchetedStart_БезЗаписи_MinПолных()
{
    // Act
    var start = WalChain.RatchetedStart(null,
        ["000000010000000000000003", "000000010000000000000001", null]);

    // Assert — null-старты (ранние FAILED без wal_start) отфильтрованы
    start!.Value.Name.Should().Be("000000010000000000000001");
}
```

- [ ] **Step 2: Прогнать — упал** (метода нет: CS0117)

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WalChainTests"`

- [ ] **Step 3: Реализация RatchetedStart (в WalChain.cs)**

```csharp
/// <summary>Стартовая точка контроля цепочки с ratchet (t07, arch/19 §3):
/// min(wal_start COMPLETED-полных со стартом ≥ записанной границы); полные
/// ниже границы разрыва для контроля игнорируются (их цепь может быть цела —
/// дыра выше). Кандидатов нет → записанная точка (ratchet не понижается);
/// записи нет → min всех валидных стартов. Чистая функция.</summary>
public static WalFileName? RatchetedStart(
    WalFileName? recorded, IEnumerable<string?> completedWalStarts)
    => completedWalStarts
        .Select(TryParse)
        .OfType<WalFileName>()
        .Where(s => recorded is null || string.CompareOrdinal(s.Name, recorded.Value.Name) >= 0)
        .OrderBy(s => s.Name, StringComparer.Ordinal)
        .Cast<WalFileName?>()
        .FirstOrDefault()
       ?? recorded;
```

- [ ] **Step 4: Прогнать ratchet-тесты — зелёные**

- [ ] **Step 5: Падающие юниты IsDue**

В `BackupPlannerTests.cs`:

```csharp
// AAA: BROKEN-цепочка → due безусловно, даже при свежем валидном полном (AC1)
[Fact]
public void IsDue_WalChainBroken_DueБезусловно()
{
    // Arrange — свежий валидный COMPLETED (час назад, окно 86400), но wal=BROKEN
    var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
    var fulls = new[]
    {
        Full("20260911110000Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1)),
            verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now.AddHours(-1)))),
    };

    // Act / Assert
    BackupPlanner.IsDue(fulls, walKeyExists: true, 86400, Unix(Now),
            walChainBroken: true)
        .Should().BeTrue("разрыв цепочки лечится только пересъёмом полного");
}

// AAA: transient-деградация НЕ триггерит пересъём (AC3): walChainBroken=false
// при несвежем полном — due по возрасту, при свежем — не due
[Fact]
public void IsDue_WalChainBrokenFalse_СвежийПолный_НеDue()
{
    // Arrange — свежий валидный COMPLETED, wal жив (ACTIVE/DEGRADED — не важно)
    var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
    var fulls = new[]
    {
        Full("20260911110100Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1))),
    };

    // Act / Assert
    BackupPlanner.IsDue(fulls, walKeyExists: true, 86400, Unix(Now),
            walChainBroken: false)
        .Should().BeFalse("DEGRADED по lag/тишине — transient, пересъём не триггерит");
}
```

- [ ] **Step 6: Прогнать — упал** (CS1501: нет параметра `walChainBroken`)

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupPlannerTests"`

- [ ] **Step 7: Реализация IsDue**

Сигнатура и первая строка тела (комментарий-док обновить: t07):

```csharp
public static bool IsDue(
    IReadOnlyList<FullBackupState> fulls, bool walKeyExists, long fullMaxAgeSec, long nowUnix,
    long? lastRestoreFinishedUnix = null, bool walChainBroken = false)
{
    // t07 (arch/19 §2): BROKEN wal-ключа — пересъём безусловно (разрыв лечит
    // только новый полный); бэкофф серии FAILED применяется вызывающим как
    // всегда — шторм пересъёмов исключён.
    if (walChainBroken)
        return true;
    ... // существующее тело без изменений
}
```

- [ ] **Step 8: Прогнать оба набора — зелёные; вся сборка**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupPlannerTests|FullyQualifiedName~WalChainTests"`
Expected: PASS (вкл. все прежние тесты IsDue — обратная совместимость опционального параметра).

- [ ] **Step 9: Коммит**

```bash
git add src/PgWorker.Backups/Model/WalChain.cs src/PgWorker.Backups/Process/BackupPlanner.cs src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs
git commit -m "feat(backups): ratchet chain_start (WalChain.RatchetedStart) + IsDue по признаку BROKEN — чистые решения (arch/19 §2/§3, t07)"
```

**Связь со spec:** §3.1 (ratchet), §3.2 (IsDue), AC1/AC3. **Выход:** чистые функции самолечения.

---

### Task 3: WalStreamProcess — BROKEN-ветви, отказ от `_chainBroken`, контроль каждый тик при BROKEN

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs`

**Interfaces:**
- Consumes: `WalStreamStatus.Broken` (Task 1), `WalChain.RatchetedStart` (Task 2), FakeBackupDeps (`FakeWalSqlExecutor`, `FakeBackupS3`, `StubScaleDriver`), `EtcdFixture`.
- Produces: `ControlOutcome` без in-memory маркера: `public sealed record ControlOutcome(WalStreamState? State) { public bool ChainBroken => State is { State: WalStreamStatus.Broken }; }`. Поведение: дыра/слот → ключ `BROKEN` + агент вниз; при BROKEN контроль каждый тик; заживление (COMPLETED-полный ≥ границы + непрерывная цепь) → `ACTIVE` + агент тем же тиком.

- [ ] **Step 1: Падающие интеграционные тесты (AAA, по образцу существующих `Контроль_*`)**

В `WalStreamProcessTests.cs` добавить (используя существующие хелперы `SeedAsync`, `BuildSnap`, `BuildProcess`, `Options`, `FullShard`, `SeedSegments`, `ReadWal`, `MutableClock`):

```csharp
// AAA (AC1): дыра → wal.state=BROKEN с границей разрыва в chain_start,
// агент не поднимается повторными тиками (маркер — ключ, не память)
[Fact]
public async Task Контроль_дыра_пишет_BROKEN_и_держит_агента_внизу()
{
    // Arrange — full wal_start=..01; S3: 1,3 (дыра на ..02)
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("cb1");
    (await _claims.TryClaimClusterAsync("cb1", ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var s3 = new FakeBackupS3();
    SeedSegments(s3, "cb1", 1, 1);
    s3.Objects.Add(("cb1", "shard1", "000000010000000000000003"));
    var driver = new StubScaleDriver();
    var process = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups("cb1", null,
        new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

    // Act — тик контроля + повторный тик
    (await process.TickAsync(BuildSnap("cb1"), backups, ct)).IsSuccess.Should().BeTrue();
    (await process.TickAsync(BuildSnap("cb1"), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — BROKEN с границей (последний непрерывный = ..01); агент не поднимается
    var wal = await ReadWal("cb1");
    wal!.State.Should().Be(WalStreamStatus.Broken);
    wal.Error.Should().Contain("000000010000000000000002");
    wal.ChainStartSegment.Should().Be("000000010000000000000001", "граница разрыва — последний непрерывный сегмент");
    driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN держит агента внизу (ключ, не память)");
}

// AAA (AC2): слот исчез при живой цепочке → BROKEN + слот ПЕРЕСОЗДАН, агент вниз
[Fact]
public async Task Слот_исчез_пишет_BROKEN_и_пересоздает_слот()
{
    // Arrange — ключ wal ACTIVE (цепочка ..01-..02), слота в FakeSql нет, агент жив
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("cb2");
    (await _claims.TryClaimClusterAsync("cb2", ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var s3 = new FakeBackupS3();
    SeedSegments(s3, "cb2", 1, 2);
    var driver = new StubScaleDriver();
    driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
        "id-agent-cb2", ["/pgw-backup-wal-cb2-shard1"], "running", "img"));
    var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    var liveWal = new WalStreamState(
        WalStreamStatus.Active, "pgw_bkp_cb2_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000002",
        "000000010000000000000002", 1757500000, 1, null);
    await writer.WriteIfChangedAsync("cb2", "shard1", liveWal, ct);
    var process = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups("cb2", null,
        new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal) });

    // Act
    (await process.TickAsync(BuildSnap("cb2"), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — BROKEN (chain_start = last_uploaded на момент обнаружения);
    // агент снят; слот пересоздан immediate+reserved (держит позицию к пересъёму)
    var wal = await ReadWal("cb2");
    wal!.State.Should().Be(WalStreamStatus.Broken);
    wal.Error.Should().Contain("слот");
    wal.ChainStartSegment.Should().Be("000000010000000000000002");
    driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb2-shard1");
    sql.Slots.Should().ContainKey("pgw_bkp_cb2_shard1", "слот пересоздаётся при BROKEN (spec §3.2)");
}

// AAA (AC1/заживление): новый COMPLETED-полный ≥ границы + непрерывная цепь →
// ACTIVE + агент ТЕМ ЖЕ тиком (даже без прошедшего VerifyIntervalSec)
[Fact]
public async Task Контроль_при_BROKEN_каждый_тик_заживляет_одним_тиком()
{
    // Arrange — BROKEN-ключ (граница ..01, дыра ..02); VerifyIntervalSec=3600;
    // появился full wal_start=..05; S3: 5,6 (цепь от нового полного непрерывна —
    // закрытые сегменты набора -X stream дублируются в wal/)
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("cb3");
    (await _claims.TryClaimClusterAsync("cb3", ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var s3 = new FakeBackupS3();
    SeedSegments(s3, "cb3", 5, 6);
    var driver = new StubScaleDriver();
    var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    await writer.WriteIfChangedAsync("cb3", "shard1", new WalStreamState(
        WalStreamStatus.Broken, "pgw_bkp_cb3_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000001",
        "000000010000000000000001", 1757500000, null, "дыра WAL-цепочки"), ct);
    var process = BuildProcess(Options(verify: 3600), sql, s3, driver, clock: TimeProvider.System);
    var backups = new ClusterBackups("cb3", null,
        new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000005") });

    // Act — ОДИН тик (расписание 3600 с НЕ наступило — BROKEN контролирует каждый тик)
    (await process.TickAsync(BuildSnap("cb3"), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — ACTIVE с chain_start от нового полного; агент поднят тем же тиком
    var wal = await ReadWal("cb3");
    wal!.State.Should().Be(WalStreamStatus.Active);
    wal.ChainStartSegment.Should().Be("000000010000000000000005");
    driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cb3-shard1");
}

// AAA (AC4 — рестарт-устойчивость): BROKEN живёт в etcd — пересоздание
// процесса (новая фабрика = «рестарт воркера») не поднимает агента
[Fact]
public async Task BROKEN_переживает_рестарт_процесса()
{
    // Arrange — доводим до BROKEN первым процессом (дыра ..02)
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("cb4");
    (await _claims.TryClaimClusterAsync("cb4", ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var s3 = new FakeBackupS3();
    SeedSegments(s3, "cb4", 1, 1);
    s3.Objects.Add(("cb4", "shard1", "000000010000000000000003"));
    var driver = new StubScaleDriver();
    var first = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups("cb4", null,
        new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });
    (await first.TickAsync(BuildSnap("cb4"), backups, ct)).IsSuccess.Should().BeTrue();
    (await ReadWal("cb4")).State.Should().Be(WalStreamStatus.Broken);

    // Act — «рестарт»: НОВАЯ инстанция процесса (in-memory словарей нет),
    // backups перечитан из etcd (backups-аргумент тика — как в ReconcileLoop)
    var restarted = BuildProcess(Options(), sql, s3, driver);
    var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    var reread = await reader.ReadAsync("cb4", "shard1", ct);
    var freshBackups = new ClusterBackups("cb4", null,
        new Dictionary<string, ShardBackups>
        {
            ["shard1"] = new(FullShard("000000010000000000000001").Full, reread.Value),
        });
    (await restarted.TickAsync(BuildSnap("cb4"), freshBackups, ct)).IsSuccess.Should().BeTrue();

    // Assert — агент не поднимается: маркер разрыва — ключ wal, не память
    driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN в etcd переживает рестарт воркера");
}
```

- [ ] **Step 1b: Регресс-тест AC9 (смена мастера — не дублируем, закрываем регрессионно)**

```csharp
// AAA (AC9-регресс): смена мастера при живом ключе — агент пересоздаётся с нового
// (masterChanged-ветка работает с новым ControlOutcome: ключ не BROKEN)
[Fact]
public async Task Смена_мастера_агент_пересоздается_с_нового()
{
    // Arrange — ключ wal ACTIVE master_node=shard1a; агент running на «shard1a»;
    // portalloc-сид переводит мастера на shard1b (master-ключ /clusters/<C>/shards/shard1/master
    // и portalloc-адрес — по образцу SeedAsync/BuildSnap с двумя нодами); цепочка сплошная
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("cm1");
    (await _claims.TryClaimClusterAsync("cm1", ct)).Value.Should().BeTrue();
    ... // сид: ключ wal ACTIVE (master_node="shard1a"), агент running в StubScaleDriver,
        // portalloc/master-ключ указывают на shard1b; snap с нодами shard1a+shard1b
    var process = BuildProcess(Options(), sql, s3, driver);

    // Act
    (await process.TickAsync(snapTwoNodes, backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — старый агент снят, новый поднят (пересоздание с нового мастера)
    driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cm1-shard1");
    driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cm1-shard1");
}
```

- [ ] **Step 2: Прогнать — упали** (сейчас пишется DEGRADED, агент-маркер in-memory)

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WalStreamProcessTests"`
Expected: 4 новых FAIL (BROKEN-контракты); регресс смены мастера — PASS до и после правок (он охраняет masterChanged-ветку от поломки новым ControlOutcome).

- [ ] **Step 3: Реализация WalStreamProcess**

Изменения (все — в границах arch/17, стиль файла сохранить):

1. Удалить поле `_chainBroken` и метод `ChainBrokenOf`; `ControlOutcome` заменить на record с вычисляемым свойством (сигнатура в начале файла, док-комментарий обновить под t07).
2. `ControlDueAsync`: расписание — пропуск только для не-BROKEN:
```csharp
if (wal is not { State: WalStreamStatus.Broken }
    && _lastVerifyUnix.TryGetValue(key, out var last)
    && now - last < options.WalVerifyIntervalSec)
    return new ControlOutcome(wal);
```
3. `chain_start` — через ratchet (замена текущего `fromFull`-блока):
```csharp
var ratchet = wal is { ChainStartSegment.Length: > 0 }
    ? WalFileName.TryParse(wal.ChainStartSegment) : null;
WalFileName? fromFull = WalChain.RatchetedStart(ratchet,
    shardBackups?.Full
        .Where(f => f.State == FullBackupStatus.Completed)
        .Select(f => f.WalStartSegment) ?? []);
var chainStart = fromFull ?? minObject; // ratchet уже внутри fromFull (кандидатов нет → recorded)
```
4. Ветку дыры `if (!chain.IsContinuous)` — вместо `DegradeAsync(...Degraded...)` вызвать `BreakAsync`:
```csharp
var broken = await BreakAsync(cluster, shard, wal, slot, masterRef, ct,
    baseStart: chain.LastSegment?.Name ?? start.Name, // граница разрыва (§3.1)
    baseLast: lastForBase, baseUnix: lastModifiedForBase.ToUnixTimeSeconds(),
    error: chain.GapError!, recreateSlot: false);
return new ControlOutcome(broken);
```
5. `DegradeAsync` переименовать/заменить на `BreakAsync(WalStreamStatus.Broken, ..., bool recreateSlot)`: пишет `State = WalStreamStatus.Broken`, `ChainStartSegment = baseStart`, стопает агента (`RemoveBackupAgentsAsync` — идемпотентно), при `recreateSlot: true` — `sql.EnsureSlotAsync` (ошибка ensure — исключение наверх, пер-шардовый catch); НЕ пишет `_chainBroken`. Ветвь «слот исчез» в `TickShardAsync`:
```csharp
if (!slotExists.Value)
{
    if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)
    {
        // BROKEN + слот пересоздаётся immediate+reserved СРАЗУ (spec §3.2):
        // к старту пересъёма полного слот уже держит позицию ≤ wal_start нового.
        await BreakAsync(cluster, shard.Name, wal, slot, masterRef, ct,
            baseStart: wal.LastUploadedSegment is { Length: > 0 }
                ? wal.LastUploadedSegment : wal.ChainStartSegment,
            baseLast: wal.LastUploadedSegment,
            baseUnix: wal.LastUploadedUnix!.Value,
            error: $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап",
            recreateSlot: true);
        return;
    }
    // первый старт ИЛИ BROKEN (слотensure: жив не трогаем — выше; исчез — создаём)
    var created = await sql.EnsureSlotAsync(adminDsn, slot, ct);
    ...
}
```
(при BROKEN-ключе и исчезнувшем слоте — просто ensure, повторного Break не нужно).
6. Успешный контроль (цепь непрерывна): существующая запись `next` — убрать `_chainBroken[key] = false`.
7. `TickShardAsync` хвост (агент): гвард уже `if (!controlled.ChainBroken)` — теперь вычисляется из State; док-комментарий обновить («решение агент не поднимать — чтение ключа wal.State == BROKEN, переживает рестарт/takeover, spec §3.2»).

- [ ] **Step 4: Обновить СТАРЫЕ тесты под новый контракт (осознанные правки)**

В `WalStreamProcessTests.cs`: тесты `Контроль_дыра_DEGRADED_границы_стоп_и_блокировка_подъема_AC4a`, `Контроль_инвалидация_слота_DEGRADED_стоп_агента_AC4b`, `Контроль_дыра_без_прошлого_ключа_пишет_DEGRADED_AC4_тотальность` — заменить ожидания `Degraded` → `Broken` (переименовать тесты: `..._пишет_BROKEN_...`); в слот-тесте добавить `sql.Slots.Should().ContainKey(...)` (слот пересоздан); в дыра-тесте — `sql.Slots` не проверяется (слот жив, FakeSql создаёт его первым тиком — оставить прежний ассерт про «не пересоздаётся» → заменить на отсутствие ensure-вызова: `sql.EnsureCalls` если есть, иначе убрать ассерт). Тест `Контроль_новый_полный_выше_дыры_восстанавливает_ACTIVE_тем_же_тиком_AC4c` остаётся зелёным (DEGRADED-сид → теперь контроль от ratchet; сид-ключ DEGRADED с границей ..01 → ratchet ..01 → новый полный ..05 ≥ → заживление) — проверить, при необходимости сид-ключ сделать BROKEN.

- [ ] **Step 5: Прогнать весь файл — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WalStreamProcessTests"`
Expected: PASS (новые + обновлённые + нетронутые: transient-lag/stale, стоп-семантика, restore-гвард).

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Backups/WalStreamProcess.cs src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs
git commit -m "feat(backups): BROKEN-самолечение WAL-цепочки — дыра/слот пишут state=BROKEN (etcd-маркер вместо _chainBroken), слот пересоздаётся до пересъёма, контроль при BROKEN каждый тик от ratchet-границы, заживление одним тиком (arch/19 §3, t07)"
```

**Связь со spec:** §3.2, AC1/AC2/AC4. **Выход:** WalStream-контур самолечения.

---

### Task 4: BackupProcess — передача признака разрыва в IsDue + сквозная интеграция самолечения

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (одна строка вызова `IsDue`)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (сквозной) — или новый файл `src/tests/PgWorker.IntegrationTests/Backups/BackupSelfHealTests.cs` (рекомендуется: сквозной сценарий WalStream+BackupProcess на реальном etcd)

**Interfaces:**
- Consumes: `BackupPlanner.IsDue(..., walChainBroken:)` (Task 2), `WalStreamStatus.Broken` (Task 1).
- Produces: планировщик создаёт PLANNED-полный для шарда с `Wal is { State: Broken }` независимо от свежести последнего полного.

- [ ] **Step 1: Падающий юнит BackupProcessTests**

По образцу существующих G3-тестов файла (сид кластера `shop`, FakeBackupEngine, PLANNED+RUNNING пишутся в FakeEtcd):

```csharp
// AAA (AC1): wal-ключ BROKEN → планировщик создаёт новый полный, даже если
// последний COMPLETED свежий (инвариант одного активного/бэкофф — как всегда)
[Fact]
public async Task Тик_при_BROKEN_wal_планирует_пересъём()
{
    // Arrange — свежий COMPLETED (5 мин назад) + wal-ключ BROKEN; джобов нет
    ... // сид по образцу: ключ full/<id> COMPLETED, ключ wal state=BROKEN
        // backups-аргумент тика с ShardBackups(fulls, wal: Broken)

    // Act
    var outcome = await process.TickAsync(snap, backups, ct);

    // Assert — появился PLANNED-полный с НОВЫМ id (due по walChainBroken)
    var planned = ... // range /pgworker/backups/shop/shard1/full/ → value contains PLANNED
    planned.Should().NotBeNull("BROKEN лечится пересъёмом (spec §3.2)");
}
```

- [ ] **Step 2: Прогнать — упал** (IsDue не получает признак → due=false при свежем полном).

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupProcessTests"`

- [ ] **Step 3: Реализация — одна правка в BackupProcess.cs**

В G3-блоке заменить вызов:

```csharp
if (!BackupPlanner.IsDue(fulls, walKeyExists: shardBackups?.Wal is not null,
        fullMaxAgeSec, nowUnix, lastRestoreFinished,
        walChainBroken: shardBackups?.Wal is { State: WalStreamStatus.Broken }))
    continue;
```

(комментарий над блоком дополнить: «t07: BROKEN wal-ключа — пересъём безусловно»).

- [ ] **Step 4: Юнит зелёный; сквозной интеграционный тест самолечения**

Новый `BackupSelfHealTests.cs` (`[Collection(EtcdCollection.Name)]`, хелперы копией из `WalStreamProcessTests` — паттерн репо):

```csharp
// AAA (AC1 сквозной): дыра → BROKEN → планировщик PLANNED → «джоб завершился»
// (симуляция: etcd-ключ COMPLETED с wal_start выше границы + объекты S3) →
// WalStream контроль → ACTIVE + агент поднят
[Fact]
public async Task Дыра_BROKEN_пересъём_и_заживление_сквозным_циклом()
{
    // Arrange 1 — WalStream тик на дырной цепочке → wal=BROKEN
    // Arrange 2 — BackupProcess тик (FakeBackupEngine без контейнера: PLANNED
    //   создан; create/start вернёт успех фейка — статус RUNNING; можно
    //   остановиться на PLANNED/RUNNING — суть: планировщик отреагировал)
    // Arrange 3 — симуляция завершения пересъёма: put etcd-ключа полного
    //   newId COMPLETED с wal_start=..05 (BackupStatusJson.Serialize),
    //   SeedSegments(s3, 5, 6)
    // Act — WalStream тик (контроль при BROKEN — каждый тик)
    // Assert — wal.state=ACTIVE, chain_start=..05, агент поднят
}
```

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupSelfHealTests"`
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Backups/Process/BackupProcess.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/BackupSelfHealTests.cs
git commit -m "feat(backups): планировщик полных реагирует на BROKEN пересъёмом (IsDue walChainBroken) + сквозная интеграция самолечения дыра→BROKEN→пересъём→ACTIVE (t07 AC1)"
```

**Связь со spec:** §3.2, AC1. **Выход:** контур самолечения замкнут (фаза 1 завершена).

---

### Task 5: Конфигурация — `Supervisor`/`Job`-таймауты, валидация, appsettings

**Files:**
- Modify: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsJobOptions`, новый `BackupsSupervisorOptions`, `BackupsOptions.Supervisor`, `ToRuntime()`, `IsValid()`)
- Modify: `src/PgWorker.App/appsettings.json` (секция `Backups`)
- Test: `src/tests/PgWorker.UnitTests/App/` (тесты `IsValid` — найти существующий файл тестов опций, напр. `PgWorkerOptionsTests.cs`; если нет — добавить в `src/tests/PgWorker.UnitTests/Backups/BackupsOptionsTests.cs`)

**Interfaces:**
- Consumes: arch/19 §9 (Task 0).
- Produces (потребляют задачи 6–11):
  - `BackupsRuntimeOptions` += `int SupervisorIntervalSec = 600`, `long SupervisorOrphanTtlSec = 604800`, `int JobFullTimeoutSec = 21600`, `int JobVerifyTimeoutSec = 21600`, `int JobRestoreTimeoutSec = 86400`.
  - `BackupsJobOptions` += `int FullTimeoutSec { get; set; } = 21600;` `int VerifyTimeoutSec { get; set; } = 21600;` `int RestoreTimeoutSec { get; set; } = 86400;`
  - `public sealed class BackupsSupervisorOptions { public int IntervalSec { get; set; } = 600; public long OrphanTtlSec { get; set; } = 604800; }` + `public BackupsSupervisorOptions Supervisor { get; set; } = new();` в `BackupsOptions`.

- [ ] **Step 1: Падающий тест валидации**

```csharp
// AAA (spec §3.6): отрицательные таймауты/TTL супервизора — fail-fast старта
[Theory]
[InlineData(-1, 604800, 21600, 21600, 86400)]
[InlineData(600, -1, 21600, 21600, 86400)]
[InlineData(600, 604800, 0, 21600, 86400)]
[InlineData(600, 604800, 21600, -5, 86400)]
[InlineData(600, 604800, 21600, 21600, 0)]
public void IsValid_ОтрицательныеБюджеты_FailFast(
    int interval, long ttl, int full, int verify, int restore)
{
    // Arrange
    var options = new BackupsOptions { Supervisor = new() { IntervalSec = interval, OrphanTtlSec = ttl } };
    options.Job.FullTimeoutSec = full;
    options.Job.VerifyTimeoutSec = verify;
    options.Job.RestoreTimeoutSec = restore;

    // Act / Assert
    options.IsValid().Should().BeFalse("отрицательные/нулевые бюджеты — мусорный конфиг");
}

// AAA: OrphanTtlSec=0 — валиден (только алерт, без авто-удаления)
[Fact]
public void IsValid_TtlZero_Валиден() { /* Supervisor.OrphanTtlSec = 0 → true */ }
```

- [ ] **Step 2: Прогнать — упал** (полей нет).

- [ ] **Step 3: Реализация**

`BackupsRuntimeOptions` — добавить 5 полей в конец (именованные аргументы в `ToRuntime` — record расширялся с обеих сторон, прецедент t05/t06). `BackupsJobOptions` — 3 свойства. `BackupsSupervisorOptions` — новый класс (док-комментарий: t07, arch/19 §9; `OrphanTtlSec=0` — авто-удаление выключено, только алерт). `ToRuntime()` — дописать именованные аргументы. `IsValid()` — дополнить:

```csharp
&& Supervisor.IntervalSec >= 60          // как Retention.IntervalSec — тик не молотит
&& Supervisor.OrphanTtlSec >= 0
&& Job.FullTimeoutSec > 0 && Job.VerifyTimeoutSec > 0 && Job.RestoreTimeoutSec > 0
```

`appsettings.json` (секция `Backups`) — привести к канону §9:

```json
"Job": { "Image": "pgworker-backup:latest", "FullTimeoutSec": 21600, "VerifyTimeoutSec": 21600, "RestoreTimeoutSec": 86400 },
"Supervisor": { "IntervalSec": 600, "OrphanTtlSec": 604800 },
```

- [ ] **Step 4: Прогнать тесты + сборку (ToRuntime-компиляция)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupsOptionsTests|FullyQualifiedName~PgWorker.UnitTests.App"`
Expected: PASS.
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx` — 0 warnings.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Backups/Options.cs src/PgWorker.App/Options.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests/Backups/BackupsOptionsTests.cs
git commit -m "feat(backups): конфиг супервизора — Job {Full/Verify/Restore TimeoutSec} + Supervisor {IntervalSec, OrphanTtlSec}, fail-fast валидация (arch/19 §9, t07)"
```

**Связь со spec:** §3.6, AC8. **Выход:** конфиг-скелет фаз 2–3.

---

### Task 6: Бюджет полных — `SupervisionTimeouts` + применение в BackupProcess (kill+FAILED+переснятие)

**Files:**
- Create: `src/PgWorker.Backups/Process/SupervisionTimeouts.cs`
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (`SuperviseActiveAsync`)
- Test: `src/tests/PgWorker.UnitTests/Backups/SupervisionTimeoutsTests.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`

**Interfaces:**
- Consumes: `BackupsRuntimeOptions.JobFullTimeoutSec` (Task 5), `BackupNames.ContainerName/VolumeName`, `TimeProvider time` в BackupProcess.
- Produces: `public static bool SupervisionTimeouts.IsTimedOut(long startedUnix, long nowUnix, long timeoutSec) => nowUnix - startedUnix > timeoutSec;` и `public static IReadOnlyList<FullBackupState> SelectTimedOut(IReadOnlyList<FullBackupState> fulls, long nowUnix, long timeoutSec)` — активные (PLANNED/RUNNING/UPLOADING) с возрастом > таймаута (чистые, юнит-тестируемы; потребуют задачи 7–8 симметрично).

- [ ] **Step 1: Падающие юниты чистого решения**

```csharp
// AAA: активный полный старше бюджета — кандидат таймаута (AC5)
[Fact]
public void SelectTimedOut_ВозрастВышеBudgeta_ОтбираетАктивных()
{
    // Arrange — RUNNING начат 7 ч назад (бюджет 6 ч); COMPLETED/FAILED не в счёт
    var now = Unix(Now);
    var fulls = new[]
    {
        Full("20260911060000Z", FullBackupStatus.Running, now - 7 * 3600),
        Full("20260910060000Z", FullBackupStatus.Completed, now - 40 * 3600, now - 39 * 3600),
        Full("20260911050000Z", FullBackupStatus.Failed, now - 8 * 3600, now - 7 * 3600),
    };

    // Act
    var timedOut = SupervisionTimeouts.SelectTimedOut(fulls, now, 21600);

    // Assert — только активный RUNNING
    timedOut.Select(f => f.Id).Should().Equal("20260911060000Z");
}
```

(+кейс «все активные младше бюджета → пусто».)

- [ ] **Step 2: Прогнать — упал. Реализовать `SupervisionTimeouts` (чистый static class, док-комментарий t07/arch/19 §6). Прогнать — зелёный.**

- [ ] **Step 3: Падающий юнит применения (BackupProcessTests)**

```csharp
// AAA (AC5): RUNNING-полный с «вечным» контейнером старше 6 ч → контейнер/volume
// удалены, статус FAILED "job-timeout…", переснятие по бэкоффу (Retry 2/4 c)
[Fact]
public async Task Супервиз_полный_старше_бюджета_FAILED_jobtimeout_и_переснятие()
{
    // Arrange — активный RUNNING started = now-7h; FakeBackupEngine содержит
    // контейнер pgw-backup-full-shop-shard1-<id> в состоянии running без логов
    // (вечный); MutableClock/сид started_unix давно
    // Act 1 — тик
    // Assert 1 — статус FAILED, error содержит "job-timeout"; контейнер и volume
    //   в engine.Removed/RemovedVolumes; journal содержит phase "job-timeout/..."
    // Act 2 — часы вперёд на RetryBaseSec (2 c), тик
    // Assert 2 — новый PLANNED с ДРУГИМ id (переснятие по общему правилу)
}
```

- [ ] **Step 4: Прогнать — упал. Реализовать в `SuperviseActiveAsync`.**

В начале тела цикла по `active` (после резолва engine, до/рядом с list):

```csharp
// t07 (arch/19 §6): возрастной бюджет зависшего джоба — kill+rm контейнера и
// staging-volume → FAILED → переснятие по общему бэкоффу. PLANNED без
// контейнера — FAILED без kill; transport-отказ list → transient (не время
// решать). journal-before-manipulations: статус FAILED пишется ДО cleanup.
var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
if (nowUnix - active.StartedUnix > options.JobFullTimeoutSec)
{
    var age = nowUnix - active.StartedUnix;
    var timedOut = active with
    {
        State = FullBackupStatus.Failed,
        FinishedUnix = nowUnix,
        Error = $"job-timeout: {age} с > {options.JobFullTimeoutSec}",
    };
    var putTimeout = await PutAsync(
        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(timedOut), ct);
    if (!putTimeout.IsSuccess)
        return putTimeout;
    if (found is not null)
        await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct); // kill+rm контейнера и volume
    await journal.WritePhaseAsync(cluster, Op, $"job-timeout/{shard.Name}/{active.Id}",
        claims.InstanceId, timedOut.Error, ct);
    continue;
}
```

(вставить в `SuperviseActiveAsync` сразу после вычисления `var found = ...` — ДО ветки «PLANNED: джоб ещё не стартовал» идемпотентного запуска: зависший PLANNED обязан получить FAILED, а не очередной create; таймаут строго по возрасту `active.StartedUnix` — transport-отказ list выше по коду уже вернул `continue`).

- [ ] **Step 5: Прогнать юниты — зелёные; интеграционные бэкап-тесты не сломаны**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupProcessTests|FullyQualifiedName~SupervisionTimeoutsTests"`
Expected: PASS (прежние S-ветви — container-vanished, exited-итоги — не задеты: таймаут строго по возрасту).

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Backups/Process/SupervisionTimeouts.cs src/PgWorker.Backups/Process/BackupProcess.cs src/tests/PgWorker.UnitTests/Backups/SupervisionTimeoutsTests.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
git commit -m "feat(backups): бюджет зависших полных — SupervisionTimeouts + kill/rm + FAILED job-timeout + переснятие по бэкоффу (arch/19 §6, t07 AC5)"
```

**Связь со spec:** §3.3 (полный), AC5. **Выход:** неуспех полного «по любой причине» сходится к FAILED.

---

### Task 7: Бюджет verify — `DockerContainerInspect.StartedAtUnix` + таймаут verify-джоба (PENDING + checked_unix-квота)

**Files:**
- Modify: `src/PgWorker.Docker/Engine/IDockerEngine.cs` (`DockerContainerInspect`)
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (`ContainerStateDto.StartedAt`, парсинг RFC3339 → unix)
- Modify: `src/PgWorker.Backups/Process/BackupVerifyProcess.cs` (`SuperviseAsync` + due-фильтр pending)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs` (ContainerRec += StartedUnix)
- Test (юнит-копия движка, если используется в BackupProcessTests — синхронно обновить internal FakeBackupEngine)

**Interfaces:**
- Consumes: `BackupsRuntimeOptions.JobVerifyTimeoutSec` (Task 5), `SupervisionTimeouts.IsTimedOut` (Task 6).
- Produces: `DockerContainerInspect(..., bool? Running = null, int? ExitCode = null, long? StartedAtUnix = null)` — опциональный хвостовой параметр (не ломает существующие конструкторы/фейки). Verify-таймаут: kill+rm джоба; кандидат получает `Verify = прежний state (null → Pending) with CheckedUnix = now`; pending-фильтр due учитывает `CheckedUnix` (лив-лок исключён).

- [ ] **Step 1: Падающий интеграционный тест**

В `BackupVerifyProcessTests.cs` (по образцу существующих тестов супервиза verify-джоба):

```csharp
// AAA (AC5): verify-джоб running дольше бюджета → kill+rm, кандидат PENDING с
// checked_unix=now; лив-лок исключён — следующий due только через interval
[Fact]
public async Task Супервиз_verify_старше_бюджета_kill_и_квота_попытки()
{
    // Arrange — COMPLETED-полный с Verify=PENDING (checked=null); FakeBackupEngine
    // держит контейнер pgw-backup-verify-<C>-<shard1>-<id> state=running с
    // StartedUnix = now - 7h (бюджет 6 ч); статус джоба — без result-логов
    // Act — тик BackupVerifyProcess
    // Assert 1 — контейнер и volume в Removed/RemovedVolumes
    // Assert 2 — ключ полного: verify.state=PENDING, checked_unix≈now (>0)
    // Act 2 — повторный тик немедленно
    // Assert 3 — НОВЫЙ джоб НЕ создан (checked только что; pending due — по interval)
}
```

- [ ] **Step 2: Прогнать — упал. Реализовать.**

1. `DockerContainerInspect` += `long? StartedAtUnix = null` (док-комментарий: docker-факт возраста running-джоба, t07). `DockerEngine.ContainerStateDto` += `[JsonPropertyName("StartedAt")] public string? StartedAt { get; set; }`; в маппинге инспекта — `DateTimeOffset.TryParse(...)` → `.ToUnixTimeSeconds()` (null при отсутствии/битой строке).
2. `FakeBackupEngine` (интеграционный): `ContainerRec` += `long? StartedUnix = null`; `InspectContainerAsync` пробрасывает; тесты могут сидировать.
3. `BackupVerifyProcess.SuperviseAsync`, ветка `state is "running" or "restarting"` — до `return`:

```csharp
// t07 (arch/19 §6): возраст running-джоба — docker-факт StartedAt (в etcd-статусе
// кандидата времени запуска нет). Бюджет исчерпан → kill+rm; вердикт FAILED НЕ
// ставим (данные не виноваты) — попытка зачитывается checked_unix=now: следующий
// due через verify.interval_sec, лив-лок немедленных перезапусков исключён.
var inspectForAge = await engine.InspectContainerAsync(containerName, ct);
if (inspectForAge.IsSuccess
    && inspectForAge.Value.StartedAtUnix is { } started
    && SupervisionTimeouts.IsTimedOut(started, nowUnix, options.JobVerifyTimeoutSec))
{
    var quota = full.Verify is null
        ? new BackupVerify(BackupVerifyStatus.Pending, nowUnix)
        : full.Verify with { CheckedUnix = nowUnix };
    var putQuota = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
        BackupStatusJson.Serialize(full with { Verify = quota }), ct);
    if (!putQuota.IsSuccess)
        return; // transient — статус не сменился, снесём следующим тиком
    await journal.WritePhaseAsync(cluster, Op, $"verify-timeout/{shard}/{id}",
        claims.InstanceId, $"verify-джоб старше {options.JobVerifyTimeoutSec} с — kill, попытка зачтена", ct);
    await CleanupJobAsync(engine, containerName, ct);
    return;
}
```

4. Due-резолв `TickShardAsync` — pending-фильтр учитывает квоту:

```csharp
var pending = fulls
    .Where(f => f.State == FullBackupStatus.Completed
                && f.Verify is { State: BackupVerifyStatus.Pending }
                // t07: checked_unix после verify-таймаута — квота попытки:
                // повторный due через interval (лив-лок исключён); null — on_create, due сразу
                && (f.Verify.CheckedUnix is null || nowUnix - f.Verify.CheckedUnix > intervalSec))
    .OrderBy(f => f.StartedUnix)
    .ToList();
```

(при `intervalSec <= 0` — таймаутнувшийся кандидат не перезапускается автоматически; поведение отражено в комментарии.)

- [ ] **Step 3: Прогнать — зелёный; все verify-тесты живы**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupVerifyProcessTests"`
Expected: PASS (прежние: ok/failed/transient-скачивание, orphan/stale-снос).

- [ ] **Step 4: Коммит**

```bash
git add src/PgWorker.Docker/Engine/IDockerEngine.cs src/PgWorker.Docker/Engine/DockerEngine.cs src/PgWorker.Backups/Process/BackupVerifyProcess.cs src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs
git commit -m "feat(backups): бюджет verify-джоба — StartedAtUnix в инспекте движка, kill+rm по VerifyTimeoutSec, квота попытки checked_unix (лив-лок исключён) (arch/19 §6, t07 AC5)"
```

**Связь со spec:** §3.3 (verify), AC5. **Выход:** verify-джобы не висят вечно.

---

### Task 8: Бюджет restore — статусный таймаут фазы RUNNING (kill+rm+FAILED+щит)

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs` (`RunAsync`)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`

**Interfaces:**
- Consumes: `BackupsRuntimeOptions.JobRestoreTimeoutSec` (Task 5), `SupervisionTimeouts.IsTimedOut` (Task 6), `FailPermanentAsync` (чистка щита initialize при Planned/Running — fd5342e-механика).
- Produces: RUNNING-заявка старше `JobRestoreTimeoutSec` → kill+rm restore-джоба (+rm data-volume первой ноды, по образцу exit-FAIL) → `FAILED error="restore-job-timeout: <age> с > RestoreTimeoutSec"`. REJOINING не таймаутится; `RecoveryTimeoutSec` внутри джоба остаётся (recovering-фаза завершится раньше сама).

- [ ] **Step 1: Падающий интеграционный тест**

В `RestoreProcessTests.cs` (по образцу существующих running-фаз тестов, FakeBackupEngine):

```csharp
// AAA (AC5): RUNNING-restore старше суток (бюджет сжат до 60 c в опциях теста)
// → restore-джоб kill+rm, заявка FAILED "restore-job-timeout", щит initialize снят
[Fact]
public async Task Running_старше_бюджета_FAILED_jobtimeout_щит_снят()
{
    // Arrange — заявка RUNNING started_unix = now - 2h; FakeBackupEngine держит
    // контейнер pgw-backup-restore-<C>-<shard1>-<id> running без маркеров;
    // опции: JobRestoreTimeoutSec=60; сид /service/<scope>/initialize=restore-in-progress
    // Act — тик RestoreProcess
    // Assert — контейнер в engine.Removed; статус FAILED с error содержит
    // "restore-job-timeout"; ключ initialize отсутствует (щит снят FailPermanentAsync);
    // volume pgw-<C>-<shard1>-<n1>-data в RemovedVolumes
}
```

- [ ] **Step 2: Прогнать — упал. Реализовать — в начале `RunAsync` (до резолва движка, чтобы бюджет работал и при transient-циклах без контейнера).**

```csharp
// t07 (arch/19 §6): статусный бюджет заявки (downloading/демонтаж могут
// transient-циклиться без контейнера — возраст считаем от started_unix).
// Канон допускает «restore-джоб часами»; сутки — явный завис. REJOINING не
// таймаутим (бюджеты Patroni-проб — свои); RecoveryTimeoutSec внутри джоба
// закрывает recovering раньше.
var startedUnix = op.StartedUnix ?? NowUnix();
if (SupervisionTimeouts.IsTimedOut(startedUnix, NowUnix(), options.JobRestoreTimeoutSec))
{
    var engineAny = driver.EngineFor(addr.Host); // если движок известен — kill
    ... // удалить контейнер по имени (force) и volume первой ноды (404=ок),
        // игнорируя отказы удаления (transient docker: статус FAILED важнее)
    await FailPermanentAsync(cluster, shard.Name, op,
        $"restore-job-timeout: {NowUnix() - startedUnix} с > {options.JobRestoreTimeoutSec}", ct);
    return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
}
```

(вставить после read портов/first-нод резолва, чтобы знать имя volume; удаление контейнера — best-effort c try/catch по образцу `FailPermanentAsync`; существующий recovery-бюджет в running-ветке остаётся без изменений.)

- [ ] **Step 3: Прогнать — зелёный; существующие restore-тесты живы**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~RestoreProcessTests"`
Expected: PASS.

- [ ] **Step 4: Коммит**

```bash
git add src/PgWorker.Backups/Process/RestoreProcess.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs
git commit -m "feat(backups): статусный бюджет restore RUNNING — kill+rm+FAILED restore-job-timeout с чисткой щита initialize (arch/19 §6, t07 AC5)"
```

**Связь со spec:** §3.3 (restore), AC5. **Выход:** фаза 2 завершена.

---

### Task 9: `BackupSupervisorProcess` — per-cluster сверка S3↔etcd (мусор `full/<id>/` без ключа)

**Files:**
- Create: `src/PgWorker.Backups/Supervisor/SupervisorPlanner.cs` (чистые функции)
- Create: `src/PgWorker.Backups/Supervisor/BackupSupervisorProcess.cs`
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (интерфейс + реализация)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (врезка)
- Modify: `src/PgWorker.App/Program.cs` (DI)
- Test: `src/tests/PgWorker.UnitTests/Backups/SupervisorPlannerTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupSupervisorProcessTests.cs`

**Interfaces:**
- Consumes: `IBackupS3.ListFullsAsync/ListPrefixAsync/DeleteKeysAsync`, `ClaimStore.IsMine`, `WorkJournal.WritePhaseAsync`, `BackupsRuntimeOptions.SupervisorIntervalSec` (Task 5), `EtcdFixture`.
- Produces:
  - `public static IReadOnlyList<string> SupervisorPlanner.SelectUnownedFulls(IReadOnlyList<string> s3FullIds, IReadOnlyList<FullBackupState> etcdFulls)` — S3-id без etcd-ключа.
  - `public Task<Result<ProcessOutcome>> BackupSupervisorProcess.TickAsync(ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)` — сигнатура как `RetentionProcess.TickAsync`.
  - `IClusterProcesses.SuperviseBackupsAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)`.

- [ ] **Step 1: Падающие юниты чистого отбора**

```csharp
// AAA (AC6): full/<id>/ в S3 без etcd-ключа — мусор; с ключом (любой state) — нет
[Fact]
public void SelectUnownedFulls_только_без_ключа()
{
    // Arrange
    var etcd = new[]
    {
        new FullBackupState("20260911110000Z", FullBackupStatus.Completed, "n1",
            BackupSourceRole.Replica, 1757500000, 1757500300, "s", 1, null, null),
        new FullBackupState("20260911120000Z", FullBackupStatus.Failed, "n1",
            BackupSourceRole.Replica, 1757504000, 1757504100, null, null, "err", null),
    };

    // Act
    var swept = SupervisorPlanner.SelectUnownedFulls(
        ["20260911110000Z", "20260911120000Z", "20260911130000Z", "20260911140000Z"], etcd);

    // Assert — только префиксы без etcd-владельца
    swept.Should().Equal("20260911130000Z", "20260911140000Z");
}
```

- [ ] **Step 2: Прогнать — упал. Реализовать `SupervisorPlanner` (статический класс, док-комментарий t07/arch/19 §4: восстановление не выберет полный без etcd-ключа — выбора кандидатов нет). Прогнать — зелёный.**

- [ ] **Step 3: Падающие интеграционные тесты процесса**

`BackupSupervisorProcessTests.cs` (`[Collection(EtcdCollection.Name)]`, снапшот/клэйм-хелперы копией из `WalStreamProcessTests`):

```csharp
// AAA (AC6): мусор full/<id>/ живого шарда удаляется первым проходом, journal
// пишет supervisor/swept-full/<X>/<id>; повторный проход — no-op
[Fact]
public async Task Проход_мусор_живого_шарда_удаляется_идемпотентно()
{
    // Arrange — клэйм c1; FakeBackupS3.PrefixObjects: объекты c1/shard1/full/A/...
    // и c1/shard1/full/B/...; etcd-ключ только для A (COMPLETED); S3 wal-объекты — живы
    // Act 1 — TickAsync
    // Assert 1 — PrefixObjects: объекты B удалены (DeletedKeys), A живы, wal/ живы
    //          journal /pgworker/work/c1 содержит "swept-full/shard1/B"
    // Act 2 — повторный TickAsync
    // Assert 2 — новых удалений нет (list подтверждает пустоту — no-op)
}

// AAA (AC9): шард в активном restore — супервизор его не трогает
[Fact]
public async Task Проход_скипает_шард_в_активном_restore() { /* Restores=[Planned] → PrefixObjects живы */ }

// AAA: Enabled=false / не-Active кластер / чужой клэйм — no-op/отказ (гварды)
[Fact]
public async Task Гварды_Enabled_и_клэйм() { /* по образцу RetentionProcess-тестов */ }
```

- [ ] **Step 4: Прогнать — упали. Реализовать процесс.**

`BackupSupervisorProcess` (ctor: `IEtcdGateway etcd, string[] endpoints, IBackupS3 s3, ClaimStore claims, WorkJournal journal, Func<BackupsRuntimeOptions?> runtime, TimeProvider time, ILogger? logger = null` — runtime-функция как WalStreamProcess для Enabled-стоп-семантики; `private const string Op = "backup-supervisor"`):
- клэйм-гвард → `runtime() is null` → Done (no-op) → Active-гвард.
- расписание per-cluster `ConcurrentDictionary<string,long> _lastPassUnix` по `SupervisorIntervalSec` (образец `RetentionProcess`).
- на каждый шард снапшота `!s.ToRemove` c restore-гвардом (`shardBackups?.Restores.Any(Planned|Running|Rejoining)`):
  - `s3.ListFullsAsync` → `SupervisorPlanner.SelectUnownedFulls(ids, shardBackups?.Full ?? [])`;
  - на каждый мусорный id: `ListPrefixAsync($"{cluster}/{shard}/full/{id}/")` → `DeleteKeysAsync` → повторный list пуст (иначе журнал-ошибка, следующий проход повторит — образец `FinishDeletionAsync`) → `journal.WritePhaseAsync(cluster, Op, $"swept-full/{shard}/{id}", ...)`; WAL-префикс НЕ трогаем (владельцы — контроль t03 и ретенция t06).
- ошибка шарда не роняет остальные (пер-шардовый try/catch + журнал — образец WalStreamProcess).

Врезка `ReconcileLoop.cs` — после `backups-retention`, до `backup-restore` (spec §3.7):

```csharp
// Сверка S3↔etcd (t07, arch/19 §4): мусор full/<id>/ живого шарда без
// etcd-ключа; после ретенции (гигиена FAILED может оставить объекты), до
// restore (гвард владельца). Выключенная подсистема не зовётся.
if (options.CurrentValue.Backups.Enabled)
    await RunClusterOpAsync(cluster, "backup-supervisor",
        () => processes.SuperviseBackupsAsync(snap, backups, ct), ct);
```

`ClusterProcesses`: метод интерфейса + реализация `=> supervisor.TickAsync(snap, backups.FirstOrDefault(b => b.Cluster == snap.Config.Cluster), ct);` + ctor-параметр `PgWorker.Backups.Supervisor.BackupSupervisorProcess supervisor`. `Program.cs`: регистрация по образцу `WalStreamProcess` (runtime-функция через `IOptionsMonitor`).

- [ ] **Step 5: Прогнать всё — зелёное**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~SupervisorPlannerTests|FullyQualifiedName~BackupSupervisorProcessTests"`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Backups/Supervisor/ src/PgWorker.App/Loops/ClusterProcesses.cs src/PgWorker.App/Loops/ReconcileLoop.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Backups/SupervisorPlannerTests.cs src/tests/PgWorker.IntegrationTests/Backups/BackupSupervisorProcessTests.cs
git commit -m "feat(backups): BackupSupervisorProcess — per-cluster сверка S3↔etcd: batch-delete full/<id>/ без etcd-ключа + journal swept-full, врезка в ReconcileLoop (arch/19 §4, t07 AC6)"
```

**Связь со spec:** §3.4 (per-cluster), §3.7, AC6/AC9. **Выход:** мусор живых шардов не копится.

---

### Task 10: Реестр сирот — `OrphanRegistry` (модель, JSON, merge, TTL) — чистые функции

**Files:**
- Create: `src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs`

**Interfaces:**
- Consumes: контракт ключа `/pgworker/backups/orphans` (arch/19 §4, Task 0), `S3ObjectInfo`.
- Produces (потребляет Task 11 и панель Task 12):
  - `public enum OrphanState { Observed, Deleting }`
  - `public sealed record OrphanEntry(string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, OrphanState State)`
  - `public sealed record OrphanRegistry(IReadOnlyList<OrphanEntry> Orphans, long UpdatedUnix)`
  - `public static IReadOnlyDictionary<string, long> OrphanRegistry.GroupShardPrefixes(IReadOnlyList<S3ObjectInfo> objects)` — сумма размеров по префиксам `<C>/<X>/` нашей формы (валидные имена `^[a-z][a-z0-9_]{0,62}$`; посторонние корневые объекты — вне реестра).
  - `public static OrphanRegistry OrphanRegistry.Merge(OrphanRegistry? current, IReadOnlyDictionary<string, long> observedSizes, IReadOnlySet<(string Cluster, string Shard)> liveOwners, long nowUnix)` — new → OBSERVED first_seen=now; существующие → перенос first_seen, обновление size; воскресшие (живой владелец) → запись удаляется (DELETING-доводка отменяется).
  - `public static string? OrphanRegistry.SelectTtlCandidate(OrphanRegistry registry, long ttlSec, long nowUnix)` — первый DELETING (доводка) либо первый OBSERVED с `now - first_seen > ttlSec`; `ttlSec=0` → только DELETING-доводка, без новых кандидатов; null — нет.
  - `public static string OrphanRegistry.ToJson(OrphanRegistry registry)` / `public static OrphanRegistry? OrphanRegistry.Parse(string raw)` — формат arch/19 §4 (`{"orphans":[{"prefix":…,"kind":"shard|cluster","size_bytes":…,"first_seen_unix":…,"state":"OBSERVED|DELETING"}],"updated_unix":…}`; битый JSON → null).
  - `public static bool OrphanRegistry.SameOrphans(OrphanRegistry a, OrphanRegistry b)` — сравнение наблюдаемой части (без updated_unix) — put только при изменении.

Kind: префикс `<C>/<X>`, где кластер `<C>` жив, а шарда нет → `shard`; кластер исчез → `cluster` (Kind вычисляется в Merge по liveOwners — передавать `IReadOnlySet<string> liveClusters` + `IReadOnlySet<(string, string)> liveShards`).

- [ ] **Step 1: Падающие юниты (AAA)**

```csharp
// AAA (AC7): группировка — посторонние корневые объекты вне реестра
[Fact]
public void GroupShardPrefixes_только_нашей_формы()
{
    // Arrange — объекты c1/s1/full/A/x, c1/s1/wal/seg, чужой root.txt
    // Act / Assert — словарь {["c1/s1"] = сумма}, root.txt не сгруппирован
}

// AAA (AC7): merge переносит first_seen (TTL от первого наблюдения)
[Fact]
public void Merge_ПереноситFirstSeen()
{
    // Arrange — current: c2/s3 OBSERVED first_seen=T0; observed: c2/s3 size новый
    // Act / Assert — first_seen=T0, size обновлён, state OBSERVED
}

// AAA (AC7): воскресение владельца гасит запись (и отменяет DELETING)
[Fact]
public void Merge_ВоскресшийВладелец_ЗаписьГаснет() { /* DELETING-запись + live → нет в результате */ }

// AAA (AC7): TTL-кандидат — старейший просроченный OBSERVED; ttl=0 — нет новых
[Fact]
public void SelectTtlCandidate_TtlИстёк()
{
    // Arrange — OBSERVED first_seen = now - 8 сут (ttl 7 сут); DELETING-запись
    // Act / Assert — DELETING-доводка приоритетна; без неё — просроченный OBSERVED;
    //                ttl=0 → null при отсутствии DELETING
}

// AAA (AC8): JSON roundtrip симметричен (формат arch/19 §4)
[Fact]
public void ToJsonParse_Roundtrip() { /* Serialize → Parse → равенство записей; битый JSON → null */ }
```

- [ ] **Step 2: Прогнать — упали. Реализовать `OrphanRegistry` (файл-модель с XML-доками t07/arch/19 §4; JSON — `JsonSerializer` с `JsonPropertyName`-атрибутами по образцу `WalStatusWriter.WalStatusPayload`). Прогнать — зелёные.**

- [ ] **Step 3: Коммит**

```bash
git add src/PgWorker.Backups/Supervisor/OrphanRegistry.cs src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs
git commit -m "feat(backups): OrphanRegistry — реестр сирот S3 (модель, JSON-ключ /pgworker/backups/orphans, merge first_seen, TTL-отбор, гвард воскресения) — чистые функции (arch/19 §4, t07 AC7)"
```

**Связь со spec:** §3.4 (глобальная часть), AC7. **Выход:** чистая логика реестра.

---

### Task 11: `BackupOrphanSweeper` + `BackupOrphanSweeperLoop` — глобальный лидер-проход

**Files:**
- Create: `src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` (один проход, тестируемый класс)
- Create: `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs` (BackgroundService)
- Modify: `src/PgWorker.App/Program.cs` (DI: sweeper-синглтон + hosted service)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`

**Interfaces:**
- Consumes: `OrphanRegistry` (Task 10), `IBackupS3.ListPrefixAsync("")/DeleteKeysAsync`, `ClaimStore.IsLeader/TryBecomeLeaderAsync`, `EtcdGateway.RangeAsync("/clusters/")`, `ClusterSnapshotParser.ParseClusters`, `BackupsRuntimeOptions.SupervisorIntervalSec/SupervisorOrphanTtlSec`.
- Produces: `public sealed class BackupOrphanSweeper(IEtcdGateway etcd, string[] endpoints, IBackupS3 s3, ClaimStore claims, WorkJournal journal, Func<BackupsRuntimeOptions?> runtime, TimeProvider time, ILogger? logger = null)` с `public async Task<Result> SweepAsync(CancellationToken ct)` — один проход (лидерство/расписание — в Loop); ключ `/pgworker/backups/orphans` (константа `OrphanRegistry.Key = "/pgworker/backups/orphans"`).

- [ ] **Step 1: Падающие интеграционные тесты (реальный etcd + FakeBackupS3 + MutableClock)**

```csharp
// AAA (AC7): префикс исчезнувшего шарда → OBSERVED в реестре; повторный проход
// переносит first_seen (не сбрасывает)
[Fact]
public async Task Проход_сирота_в_реестре_first_seen_переносится() { /*...*/ }

// AAA (AC7): TTL истёк (сжатое время) → DELETING → объекты удалены → запись удалена
[Fact]
public async Task Проход_TTL_истёк_удаляет_префикс_и_запись() { /*...*/ }

// AAA (AC7): OrphanTtlSec=0 — реестр живёт, удаления нет
[Fact]
public async Task Проход_TtlZero_только_реестр() { /*...*/ }

// AAA (AC7): воскресение владельца (шард снова заявлен в /clusters/) — запись гаснет
[Fact]
public async Task Проход_владелец_воскрес_запись_гаснет() { /*...*/ }

// AAA: не-лидер ничего не пишет (гвард IsLeader в SweepAsync или Loop —
// проверка вызовом без лидерства: ключ реестра не появился)
[Fact]
public async Task Проход_без_лидерства_не_пишет() { /* TryBecomeLeaderAsync не звали → SweepAsync no-op/отказ */ }
```

- [ ] **Step 2: Прогнать — упали. Реализовать `BackupOrphanSweeper.SweepAsync`.**

Алгоритм прохода (все шаги идемпотентны, transient-отказы — Result.Failed без мутаций):
1. Гвард: `claims.IsLeader` (иначе Done); `runtime() is null` → Done (Enabled=false).
2. `s3.ListPrefixAsync("")` → `OrphanRegistry.GroupShardPrefixes` (транзиент → Failed).
3. `RangeAsync("/clusters/")` → `ClusterSnapshotParser.ParseClusters` → liveShards = `(C, X)` всех кластеров (state ≠ TO_REMOVE-семантики парсера) и их шардов `!ToRemove`; liveClusters = имена.
4. Сироты = наблюдаемые префиксы без владельца; `current = Parse(await GetAsync(OrphanRegistry.Key))`; `merged = OrphanRegistry.Merge(current, orphans, liveShards, liveClusters, now)`.
5. Put реестра при `SameOrphans`-изменении (failover-put по образцу `WalStreamProcess.PutAsync`; updated_unix — now).
6. TTL: `SelectTtlCandidate(merged, ttlSec, now)`:
   - нет → Done;
   - есть: пометить `DELETING` (journal-before-manipulations: put реестра с DELETING + `journal.WritePhaseAsync(cluster: "-", Op, $"orphan-deleting/{prefix}", ...)` — journal-ключ кластера: префикс `<C>/…` сироты, кластер может не существовать → использовать первый компонент префикса), затем list префикса → `DeleteKeysAsync` → list-подтверждение пустоты → удалить запись из реестра (put) + journal `orphan-deleted/<prefix>`. transient-отказ S3 → запись остаётся DELETING (следующий проход доведёт).
7. Живой владелец появился → Merge уже погасил запись (доводка отменяется — осознанный остаточный риск arch/19 §10).

`BackupOrphanSweeperLoop` (internal sealed, `BackgroundService` — паттерн `SnapshotLoop`, упрощённый без health-обёртки):

```csharp
// ExecuteAsync: цикл — if (!claims.IsLeader) TryBecomeLeaderAsync (log при успехе);
// if (claims.IsLeader && Backups.Enabled): await sweeper.SweepAsync(ct)
//   (ошибка → лог + журнал не требуется: SweepAsync сам Result); задержка
//   Supervisor:IntervalSec; иначе — задержка Loops:ScanIntervalSec (как SnapshotLoop).
```

DI в `Program.cs` (по образцу WalStreamProcess: runtime-функция `IOptionsMonitor`):

```csharp
builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupOrphanSweeper(...));
builder.Services.AddSingleton<BackupOrphanSweeperLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<BackupOrphanSweeperLoop>());
```

- [ ] **Step 3: Прогнать — зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupOrphanSweeperTests|FullyQualifiedName~OrphanRegistryTests"`
Expected: PASS.

- [ ] **Step 4: Коммит**

```bash
git add src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs
git commit -m "feat(backups): BackupOrphanSweeper + лидер-цикл — глобальный реестр сирот /pgworker/backups/orphans, merge first_seen, TTL-удаление по одному префиксу за проход, гвард воскресения (arch/19 §4, t07 AC7)"
```

**Связь со spec:** §3.4 (глобальная), §3.7, AC7. **Выход:** фаза 3 завершена.

---

### Task 12: Панель — `WalStreamInfoState.Broken`, парсер orphans, правила `backup-chain-broken`/`backup-orphan`

**Files:**
- Modify: `src/AdminPanel.Core/BackupsInfo.cs` (enum + `BackupOrphansInfo`)
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+`BackupOrphansInfo? BackupOrphans = null`)
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` (`StateOf` + ключ orphans + `BackupsParseResult.Orphans`)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (проброс в оба конструктора `EtcdSnapshot`)
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupChainBrokenRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupOrphanRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (`Backups.OrphanTtlSec`)
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupChainBrokenRuleTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupOrphanRuleTests.cs`

**Interfaces:**
- Consumes: контракт arch/19 §4 (Task 0), `OrphanRegistry` JSON-формат (Task 10), образцы `WalChainBrokenRule`/`BackupDeletingStuckRule(+Tests)`.
- Produces:
  - `public enum WalStreamInfoState { Active, Degraded, Stopped, Broken }`
  - `public sealed record BackupOrphanInfo(string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, string State)`; `public sealed record BackupOrphansInfo(IReadOnlyList<BackupOrphanInfo> Orphans, long UpdatedUnix)`
  - `BackupsParseResult(..., BackupOrphansInfo? Orphans = null)`; `EtcdSnapshot(..., BackupOrphansInfo? BackupOrphans = null)`.
  - Правила: `BackupChainBrokenRule` — kind `backup-chain-broken`, critical; `BackupOrphanRule` — kind `backup-orphan`, warning, вход `IOptions<AlertsOptions>` (порог `AlertsOptions.Backups.OrphanTtlSec`, default 604800).

- [ ] **Step 1: Падающие юниты парсера**

```csharp
// AAA (AC8): state=BROKEN читается; незнакомое state — KeyParseError (как прежде)
[Fact]
public void Parse_WalBroken_Читается() { /* kv /pgworker/backups/c1/s1/wal со state BROKEN → WalStreamInfoState.Broken */ }

// AAA (AC7/AC8): глобальный ключ orphans парсится; битый — ошибка ключа
[Fact]
public void Parse_Orphans_Ключ_Читается() { /* kv /pgworker/backups/orphans → BackupsParseResult.Orphans с записью */ }
```

- [ ] **Step 2: Прогнать — упали. Реализовать модель+парсер: `StateOf` += `"BROKEN" => WalStreamInfoState.Broken`; ветка `segments.Length == 4 && segments[3] == "orphans"` — по образцу storage (поля prefix/kind/size_bytes/first_seen_unix/state OBSERVED|DELETING; битое → KeyParseError); `SnapshotRefresher` проброс. Прогнать — зелёные.**

- [ ] **Step 3: Падающие юниты правил (по образцу BackupDeletingStuckRuleTests)**

```csharp
// AAA (AC8→алерт): wal=BROKEN живого Active-кластера → critical backup-chain-broken
// с текстом сбоя И действия воркера; DEGRADED/STOPPED/BROKEN мёртвого кластера — молчание
[Fact]
public void Broken_ЖивойКластер_Critical() { /* Alert Kind="backup-chain-broken", Severity=Critical, Remedy=WorkerAuto, Message содержит error и «переснимает» */ }

// AAA (AC7→алерт): запись реестра → warning backup-orphan с размером и остатком TTL;
[Fact]
public void Orphan_Observed_Warning_СTtl() { /* Message содержит prefix, size, «TTL» */ }

// AAA (AC7): OrphanTtlSec=0 → «удаление вручную»; DELETING → «идёт удаление»
[Fact]
public void Orphan_TtlZero_Ручное() { /*...*/ }
```

- [ ] **Step 4: Прогнать — упали. Реализовать правила.**

`BackupChainBrokenRule` (по каркасу `WalChainBrokenRule`; автоскан `[InjectAsSingleton(typeof(IAlertRule))]` уже даёт регистрацию):

```csharp
// backup-chain-broken (critical, t07, arch/19 §3/§4): state=BROKEN wal-ключа шарда
// живого Active-кластера — разрыв цепочки; воркер сам лечит пересъёмом полного
// (планировщик §2). Текст показывает и сбой, и действие. DEGRADED — transient,
// остаётся правилу wal-chain-broken (t03).
yield return new Alert(
    $"{KindName}:{backups.Cluster}/{shard}",
    AlertSeverity.Critical, KindName, $"{backups.Cluster}/{shard}",
    $"разрыв WAL-цепочки шарда {shard} кластера {backups.Cluster}: {wal.Error ?? "без причины"} — воркер переснимает полный бэкап",
    new Dictionary<string, string> { ["state"] = "BROKEN", ["slot"] = wal.Slot, ["error"] = wal.Error ?? "" },
    null,
    "цепочка full+WAL невосстановима наращиванием: новая точка — только новый полный",
    AlertRemedy.WorkerAuto,
    "воркер переснимает полный бэкап и поднимет агента (t07); затяжное лечение видно по backup-full-stale/серии FAILED — разбор по runbook arch/19");
```

`BackupOrphanRule`: вход `snapshot.BackupOrphans`; на каждую запись warning с `prefix`/`size`/`state`; остаток TTL = `first_seen + ttl − now` (человеческим видом «X сут»); `ttl <= 0` → «удаление вручную»; `DELETING` → «идёт удаление»; Remedy = `OperatorRunbook` при ttl=0, иначе `WorkerAuto`.

`AlertsOptions.Backups` += `public long OrphanTtlSec { get; set; } = 604800;` (док: синхронизирован с воркерным `Supervisor:OrphanTtlSec` — паттерн `WalLagMaxSegments`).

- [ ] **Step 5: Прогнать панельные юниты целиком — зелёные (AlertHintRemedy/AutoRegistration не сломаны новыми правилами)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add src/AdminPanel.Core/BackupsInfo.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Etcd/Parsing/BackupsParser.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/AdminPanel.Core/Alerting/Rules/BackupChainBrokenRule.cs src/AdminPanel.Core/Alerting/Rules/BackupOrphanRule.cs src/AdminPanel.Core/Alerting/AlertsOptions.cs src/tests/AdminPanel.UnitTests/
git commit -m "feat(panel): backup-chain-broken (critical, BROKEN) + backup-orphan (warning, реестр сирот с TTL-остатком) — модель/парсер/правила, WalStreamInfoState.Broken (arch/19 §4, t07 AC7/AC8)"
```

**Связь со spec:** §3.5, AC7/AC8. **Выход:** фаза 4 завершена (UI-инвентарь — t08, вне границ).

---

### Task 13: Фаза 5 — docker-E2E на свежем Release: `Backup_ChainBroken_SelfHeals`, `Backup_OrphanRegistry_TtlDelete`, кейс-маркер

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eSupervisorScenarios.cs`
- Test (существующий, прогон): `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs` (`Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused` — кейс-маркер AGENTS)

**Interfaces:**
- Consumes: `E2eEnvironment.StartAsync(slug, withMinio: true)` (guid-изоляция, own-only teardown, динамические порты), `Fx.StartHostAsync(extraEnv: …)` (оверрайд конфига `PgWorker__Backups__…`), `E2eFixture.WaitForAsync`, `mc`-посев/удаление через `Fx.RunDockerAsync` (образец `E2eRetentionScenarios`), телеметрия `docs/e2e-launch.md` (уже в E2eEnvironment).

**Границы E2E-запуска (AGENTS, обязательны):** запуск ТОЛЬКО по правилам `docs/e2e-launch.md` (фазы >60 с — сбор логов; упавший сценарий — разбор по артефактам, без перезапуска); изоляция — `docs/e2e-isolation.md`; между сериями — `docker rm -f $(docker ps -aq)` (не трогая стендовые `as-*`/`adminpanel`) + `docker network prune -f`; `docker ps -aq | wc -l` (вне стенда) == 0 перед следующей серией.

- [ ] **Step 1: Сценарий `Backup_ChainBroken_SelfHeals`**

В `E2eSupervisorScenarios.cs` (каркас — копия `E2eRetentionScenarios`: Fx/Endpoint/G-поля, `SeedClusterAsync`, `MasterPgAsync`, `SwitchWalsAsync`, `ProvisionedAsync`, mc-хелперы):

```csharp
// AAA (AC1/AC10): живой кластер с валидной цепочкой → дыра (rm сегмента ВНУТРИ
// цепочки) → контроль → wal=BROKEN → планировщик переснимает полный → цепь
// непрерывна от нового wal_start → wal=ACTIVE, агент жив
[Fact]
public async Task Backup_ChainBroken_SelfHeals()
{
    // Arrange 1 — окружение bk-spr (withMinio), кластер bkspr<тег>, policy
    //   full_max_age_sec=3600 (НЕ из-за возраста — триггер только BROKEN),
    //   verify on_create=false (не мешает), Wal VerifyIntervalSec=5,
    //   Supervisor IntervalSec=60; воркер StartHostAsync
    // Arrange 2 — provisioning DONE; генерация WAL ДО полного (SwitchWalsAsync 16 —
    //   образец E2eRetentionScenarios Arrange 3: цепочка длиннее одного сегмента),
    //   реальный COMPLETED полный (WaitForAsync ≤ 300 c);
    //   wal-ключ ACTIVE; mc ls wal/ — фактическая цепочка сегментов
    // Arrange 3 — ДЫРА: выбрать средний сегмент ЦЕПОЧКИ (не последний!) и
    //   mc rm t/<bucket>/<C>/shard1/wal/<seg> (внутри chain_start..last)
    // Act 1 — WaitForAsync: wal-ключ содержит "BROKEN" (≤ 120 c: контроль 5 c)
    // Assert 1 — wal.state=BROKEN, error содержит границы дыры
    // Act 2 — WaitForAsync: НОВЫЙ PLANNED/RUNNING полный с id ≠ старого (≤ 300 c),
    //   затем COMPLETED (≤ 600 c: pg_basebackup со spread-чекпоинтом)
    // Act 3 — WaitForAsync: wal.state=ACTIVE (≤ 120 c после COMPLETED)
    // Assert 2 — chain_start_segment нового ACTIVE-ключа ≥ wal_start_segment
    //   нового полного; агент pgw-backup-wal-<C>-shard1 жив (docker ps)
}
```

- [ ] **Step 2: Сценарий `Backup_OrphanRegistry_TtlDelete`**

```csharp
// AAA (AC7/AC10): shard-префикс без владельца → реестр OBSERVED → сжатый TTL →
// DELETING → объекты удалены → запись гаснет
[Fact]
public async Task Backup_OrphanRegistry_TtlDelete()
{
    // Arrange 1 — окружение bk-orf (withMinio), кластер bkorf<тег>, воркер с
    //   Supervisor { IntervalSec=60, OrphanTtlSec=120 } (сжатое время)
    // Arrange 2 — provisioning DONE (воркер лидер: единственный инстанс);
    //   mc-посев сиротского префикса ghost<тег>/shard1/full/20260901.../x
    //   (кластера ghost<тег> нет в /clusters/)
    // Act 1 — WaitForAsync: ключ /pgworker/backups/orphans содержит ghost<тег>/shard1
    //   и "OBSERVED" (≤ 180 c)
    // Act 2 — WaitForAsync (TTL 120 c + проходы 60 c, бюджет ≤ 420 c): объекты
    //   префикса удалены (mc ls пуст) И записи в orphans-ключе нет
    // Assert — pass: first_seen переживался (запись жила ≥ 2 прохода), удаление
    //   произошло только после TTL
}
```

(перед посевом чужого префикса проверить: имя `<C>` валидно по regex и не пересекается с кластерами теста; teardown окружения — own-only по `Fx.ClusterTag`, сиротский префикс в ЕГО MinIO — уходит вместе с окружением.)

- [ ] **Step 3: Прогон новых сценариев (серия 1) + зачистка**

Run (из корня worktree):
```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Backup_ChainBroken_SelfHeals|FullyQualifiedName~Backup_OrphanRegistry_TtlDelete"
```
Expected: PASS оба. E2eFixture сам собирает Release; таймауты сценариев — по бюджетам WaitFor (никаких BrokerBootSec-аналогов >100 c в фикстурах).
После серии (независимо от исхода):
```
docker rm -f $(docker ps -aq)   # стендовые as-*/adminpanel не трогать, если стенд поднят
docker network prune -f
docker ps -aq | wc -l           # == 0 (вне стенда) — гейт следующей серии
```

- [ ] **Step 4: Кейс-маркер мерж-гейта (серия 2) + зачистка**

Run:
```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```
Expected: PASS. Зачистка — как в Step 3.

- [ ] **Step 5: Коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eSupervisorScenarios.cs
git commit -m "test(e2e): сценарии Backup_ChainBroken_SelfHeals (дыра→BROKEN→пересъём→ACTIVE) и Backup_OrphanRegistry_TtlDelete (реестр→сжатый TTL→удаление) — изоляция/телеметрия по docs/e2e-isolation.md, docs/e2e-launch.md (t07 AC10)"
```

**Связь со spec:** §5 фаза 5, AC10. **Выход:** E2E-гейт зелёный.

---

### Task 14: Финал — runbook-упоминание сирот, roadmap-гейт, полный прогон

**Files:**
- Modify: `docs/backup-restore.md` (раздел про судьбу бэкапов удалённого кластера)
- Modify: `arch/roadmap/backup.md` (снять тег `t07-backup-supervisor` — тем же мерж-коммитом, прецедент `chore(roadmap)` t06)

**Interfaces:**
- Consumes: всё сделанное выше.
- Produces: AC11 — гигиена проекта.

- [ ] **Step 1: docs/backup-restore.md — упоминание сирот**

В раздел про DR/удаление кластера (или новый подраздел «Судьба S3-объектов удалённого кластера») добавить:

```
Объекты бэкапов удалённого (deprovisioned) кластера остаются в S3: воркер
не уничтожает потенциально ценные данные автоматикой (R4). Супервизор t07
заносит осиротевшие префиксы в реестр /pgworker/backups/orphans (виден в
панели, алерт backup-orphan) и удаляет их автоматически по TTL
PgWorker:Backups:Supervisor:OrphanTtlSec (по умолчанию 7 суток; 0 — только
алерт, удаление вручную). DR-восстановление из S3 (source-override) — см.
раздел «Восстановление в новый кластер»: успеть до истечения TTL либо
выключить авто-удаление.
```

- [ ] **Step 2: Снять roadmap-тег t07**

В `arch/roadmap/backup.md`: удалить пункт `t07-backup-supervisor` (строка ~16) и очистить его упоминания из `←`-зависимостей других пунктов (grep `t07` по `arch/roadmap/`). Правила — `arch/roadmap/README.md` (никаких пометок «сделано»).

- [ ] **Step 3: Полный прогон юнитов + интеграций (с зачисткой серий по AGENTS)**

Run (серия за серией, между ними docker-зачистка из Task 13 Step 3):
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.UnitTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.IntegrationTests&FullyQualifiedName!~E2e"
```
Expected: все зелёные, `TreatWarningsAsErrors` чисто.

- [ ] **Step 4: Коммит**

```bash
git add docs/backup-restore.md arch/roadmap/backup.md
git commit -m "docs+roadmap: runbook бэкапов — сироты S3 удалённого кластера (реестр+TTL 7 сут); снять тег t07-backup-supervisor (мерж-гейт, t07 AC11)"
```

**Связь со spec:** AC11. **Выход:** задача готова к мерж-гейту dev-flow (code-review → merge в main; roadmap-правка уезжает тем же мерж-коммитом).

---

## Самопроверка плана (выполнена составителем)

1. **Spec coverage:** §3.1→Tasks 1–3; §3.2→Tasks 2–4; §3.3→Tasks 5–8; §3.4→Tasks 9–11; §3.5→Task 12; §3.6→Task 5; §3.7→Tasks 9, 11; §4 границы соблюдены (verify/ретенция/restore-механика не переписываются — только точки таймаутов; WAL-чистка супервизором отсутствует; новых метрик нет); §5 фазы 0–5 = Tasks 0–14 по порядку; AC1→T2/T3/T4/T13, AC2→T3, AC3→T2/T3, AC4→T3, AC5→T6/T7/T8, AC6→T9, AC7→T10/T11/T12/T13, AC8→T0/T1/T12, AC9→T3 Step 1b (смена мастера — регресс) + T9 (restore-гвард супервизора) + T11/T13 (remove-shard S3-сирота в реестре) + T14 (полный прогон существующих интеграций), AC10→T13, AC11→T14.
2. **Placeholder-scan:** каждый код-шаг содержит сигнатуры/скелеты; расплывчатых «добавить обработку» нет — исключение: интеграционные тесты Tasks 9/10/11 даны телами-скелетами с точными Assert-инвариантами (полный код — по хелперам существующих файлов, на которые даны точные ссылки-образцы).
3. **Type-consistency:** `WalStreamStatus.Broken` (T1) ↔ `WalStreamInfoState.Broken` (T12); `WalChain.RatchetedStart(WalFileName?, IEnumerable<string?>)` (T2) ↔ вызов в T3; `IsDue(..., bool walChainBroken = false)` (T2) ↔ вызов в T4; `BackupsRuntimeOptions.SupervisorIntervalSec/SupervisorOrphanTtlSec/JobFullTimeoutSec/JobVerifyTimeoutSec/JobRestoreTimeoutSec` (T5) ↔ потребление T6–T11; `OrphanRegistry` API (T10) ↔ T11/T12; `DockerContainerInspect.StartedAtUnix` (T7) — хвостовой опциональный параметр, существующие конструкторы не ломаются.

## Порядок исполнения и зависимости

`Task 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14`.
Task 9 зависит от 5 (конфиг); Tasks 10–11 — от 5 и 10 друг от друга; Task 12 независим от 9–11 по коду, но по контракту опирается на 0/1/10. Коммиты — после каждой задачи. E2E (13) — только после 1–12; между сериями — docker-зачистка (AGENTS).
