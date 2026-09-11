# t04-backup-verify — план реализации (проверки полных бэкапов)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** верификация полных бэкапов каждого шарда — `pg_verifybackup` (SHA256 по `backup_manifest`) + полнота WAL-цепочки `wal/` от `wal_start_segment` до точки бэкапа; вердикт `verify{state,checked_unix,error}` в etcd; периодическая перепроверка по политике; проваленный verify не считается свежестью (переснятие); панельный алерт `backup-verify-failed` (critical).

**Архитектура:** новый процесс `BackupVerifyProcess` (тик под клэймом `<C>`): due-резолв кандидатов (PENDING от on_create / периодика по `verify.interval_sec`) → цепочечная проверка list-ами S3 + строгий LSN-разбор `.history` (`WalChain.CheckRange` + `WalHistory`, без скачивания сегментов) → ephemeral verify-джоб `pgw-backup-verify-<C>-<X>-<id>` (inline bash `mc cp` + `pg_verifybackup`, result-JSON в stdout + exit-код — протокол t02) → супервиз по детерминированному имени → итог в ключ полного. Планировщик t02 и панель считают «последний валидный» = COMPLETED с `verify ≠ FAILED`.

**Тех-стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK.S3 (уже подключён, новых пакетов нет), testcontainers (MinIO/etcd), xUnit + FluentAssertions, тесты с AAA-комментариями.

**Spec:** `docs/superpowers/2026-09-11-t04-backup-verify/spec.md` (в этом же каталоге). Канон: `arch/19-backups.md` (правки Ф0 уже внесены в рабочей копии этой ветки — незакоммичены), `arch/adminpanel/02-etcd-contract.md` §2.3.1.

## Глобальные ограничения (из spec §5)

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; пакеты — `Directory.Packages.props`, НОВЫХ пакетов нет (AWSSDK.S3 уже подключён).
- Порты docker в тестах — только динамические (`assignRandomHostPort: true`/`GetMappedPublicPort`), никаких литералов-портов; таймауты фикстур ≤ 100 c; E2E-бюджеты короткие (форсирование политикой `interval_sec`, не ожиданием).
- После КАЖДОЙ тестовой серии — ТОЛЬКО фильтрованная форма зачистки (не задевает контейнеры dev-стенда `as-*`/`adminpanel`, даже если стенд поднят): `docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f`; никогда не запускать следующую серию поверх незачищенной предыдущей.
- Каждый E2E/интеграционный тест полностью чистит за собой (teardown при любом исходе — per-Fact `E2eEnvironment`); проверка чистоты — ассерт теста.
- Язык: документация/комментарии — русский; идентентификаторы — английский; тесты — AAA-комментарии.
- Verify-джоб: портов не публикует; S3-креды — ТОЛЬКО env (не argv/ps/статусы/логи); образ — тот же `Backups:Job.Image`; локальные образы в registry `192.168.0.1:5000` не класть.
- Контракт etcd — только расширение: `verify.error` (опционально) + `verify.interval_sec` policy; старые записи без полей парсятся (обратная совместимость).
- FAILED-verify терминален — не перепроверяется автоматически (осознанное решение spec §1).
- S3-объекты verify только читает (никаких удалений/перезаписей).
- Все команды `dotnet` — с `DOTNET_CLI_UI_LANGUAGE=en`; рабочая директория — worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t04-backup-verify`.

## Карта файлов

| Файл | Действие | Ответственность |
|---|---|---|
| `src/PgWorker.Etcd/Parsing/BackupsModel.cs` | Modify | `BackupVerify.Error`, `BackupPolicy.VerifyIntervalSec` |
| `src/PgWorker.Etcd/Parsing/BackupsParser.cs` | Modify | чтение `verify.error`, `verify.interval_sec` |
| `src/PgWorker.Backups/Job/BackupStatusJson.cs` | Modify | запись `verify.error` |
| `src/PgWorker.Backups/Model/WalHistory.cs` | Create | строгий парсер `.history` (parentTLI + switchWALLSN) |
| `src/PgWorker.Backups/Model/WalChain.cs` | Modify | `CheckRange(start, end, objects, historyContents)` |
| `src/PgWorker.Backups/VerifyJobCommand.cs` | Create | inline bash verify-джоба (env-контракт `PGW_BK_*`) |
| `src/PgWorker.Backups/Job/VerifyJobSpec.cs` | Create | `ContainerSpec` verify-джоба (образец `BackupJobSpec`) |
| `src/PgWorker.Backups/BackupS3.cs` | Modify | `ListAsync(prefix)`, `GetObjectAsync` |
| `src/PgWorker.Backups/BackupNames.cs` | Modify | verify-имена контейнера/volume/префикса |
| `src/PgWorker.Backups/Options.cs` | Modify | `VerifyIntervalSec=604800` |
| `src/PgWorker.Backups/Process/BackupVerifyProcess.cs` | Create | тиковая машина verify |
| `src/PgWorker.Backups/Process/BackupPlanner.cs` | Modify | валидность в `IsDue`/`BackoffPassed` |
| `src/PgWorker.App/Options.cs` | Modify | `BackupsPolicyOptions.VerifyIntervalSec`, `ToRuntime()` |
| `src/PgWorker.App/Program.cs` | Modify | DI `BackupVerifyProcess`, `ReloadableBackupS3` прокси |
| `src/PgWorker.App/Loops/ClusterProcesses.cs` | Modify | `VerifyBackupsAsync` |
| `src/PgWorker.App/Loops/ReconcileLoop.cs` | Modify | вызов после `WalStreamAsync`, до `repair` |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | Modify | `BackupJobsCleaner` + verify-префикс |
| `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` | Modify | counter `pgworker_backup_verify_total{result}` |
| `src/AdminPanel.Core/BackupInfo.cs` | Modify | `ShardVerifyFailure`, `ClusterBackupsInfo.ShardVerifyFailures` |
| `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` | Modify | verify-поля, валидная свежесть, failures |
| `src/AdminPanel.Core/Alerting/Rules/BackupVerifyFailedRule.cs` | Create | алерт `backup-verify-failed` (critical) |
| Тесты (см. задачи) | Create/Modify | юниты/интеграции/E2E |

---

### Задача 1: Ф0 — фиксация канона и spec в ветке

Канон-правки (`arch/19-backups.md`, `arch/adminpanel/02-etcd-contract.md`) внесены в spec-фазе в рабочую копию; spec и этот план лежат в `docs/superpowers/2026-09-11-t04-backup-verify/`. Задача фиксирует их первым коммитом ветки (принцип arch-first: канон в ветке ДО кода, AC9).

**Files:**
- Commit: `arch/19-backups.md`, `arch/adminpanel/02-etcd-contract.md`, `docs/superpowers/2026-09-11-t04-backup-verify/spec.md`, `docs/superpowers/2026-09-11-t04-backup-verify/plan.md`

**Interfaces:**
- Consumes: ничего (первая задача).
- Produces: коммит канона; последующие задачи ссылаются на `arch/19-backups.md` §2–§5/§8–§10 как на источник правды.

- [ ] **Шаг 1: закоммитить канон + spec + план**

  - Вход: worktree `feat-t04-backup-verify`, изменения канона уже в рабочих файлах (`git status` показывает `M arch/19-backups.md`, `M arch/adminpanel/02-etcd-contract.md`, untracked `docs/superpowers/2026-09-11-t04-backup-verify/`).
  - Действие:
    ```bash
    cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-backup-verify
    git add arch/19-backups.md arch/adminpanel/02-etcd-contract.md docs/superpowers/2026-09-11-t04-backup-verify
    git commit -m "docs(arch): t04-backup-verify — канон verify (arch/19 §2-§5/§8-§10, adminpanel/02 §2.3.1) + spec/plan"
    ```
  - Выход: канон-правки Ф0 и spec зафиксированы первым коммитом ветки.
  - Проверка: `git log --oneline -2` показывает новый коммит; `git status` без канона/docs.
  - Spec: принцип 1 (arch-first), Ф0, AC9.

---

### Задача 2: Модель и контракт (verify.error, VerifyIntervalSec) — Ф1

Расширение модели etcd-контракта бэкапов: `verify.error` в статусе полного, `verify.interval_sec` в policy. Обратная совместимость: опциональные поля, старые записи парсятся без изменений (spec §5).

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs`
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs` (`TryParsePolicy`, `TryParseFull`)
- Modify: `src/PgWorker.Backups/Job/BackupStatusJson.cs`
- Modify: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsPolicyOptions`, `ToRuntime()`)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupStatusJsonTests.cs`, `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs`

**Interfaces:**
- Consumes: существующие `BackupVerify(BackupVerifyStatus State, long? CheckedUnix)`, `BackupPolicy(...)` — позиционные records.
- Produces (для задач 8–9, 11):
  - `public sealed record BackupVerify(BackupVerifyStatus State, long? CheckedUnix, string? Error = null);`
  - `public sealed record BackupPolicy(int RetentionDays, int RetentionWeeks, int RetentionMonths, long FullMaxAgeSec, bool VerifyOnCreate, long? VerifyIntervalSec = null);` — `null` = не задан в policy-ключе → потребитель берёт дефолт конфига.
  - `BackupsRuntimeOptions.VerifyIntervalSec` (`long`, дефолт `604800`) — дефолт конфига `PgWorker:Backups:Policy:VerifyIntervalSec`.

- [ ] **Шаг 1: написать падающие юнит-тесты сериализации**

  - Вход: задача 1 слита; существующие `BackupStatusJsonTests` (round-trip статуса).
  - Действие: добавить в `BackupStatusJsonTests` тесты (AAA):
    ```csharp
    // AAA: FAILED-verify сериализуется с error и checked_unix (канон arch/19 §4)
    [Fact]
    public void Serialize_VerifyFailed_ErrorИCheckedUnix()
    {
        // Arrange
        var full = new FullBackupState("20260911120000Z", FullBackupStatus.Completed, "n1",
            BackupSourceRole.Replica, 1757500000, 1757500300, "000000010000000000000001",
            1024, null, new BackupVerify(BackupVerifyStatus.Failed, 1757500600, "дыра WAL-цепочки: ожидался X, найден Y"));

        // Act
        var json = BackupStatusJson.Serialize(full);

        // Assert — verify.error присутствует, checked_unix пишется и при FAILED
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var verify = doc.RootElement.GetProperty("verify");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("checked_unix").GetInt64().Should().Be(1757500600);
        verify.GetProperty("error").GetString().Should().Contain("дыра WAL-цепочки");
    }

    // AAA: verify без error — поля error в JSON нет (опционально, как остальные nullable)
    [Fact]
    public void Serialize_VerifyБезError_ПоляНет()
    {
        // Arrange
        var full = new FullBackupState("20260911120000Z", FullBackupStatus.Completed, "n1",
            BackupSourceRole.Replica, 1757500000, 1757500300, null, null, null,
            new BackupVerify(BackupVerifyStatus.Ok, 1757500600));

        // Act
        var json = BackupStatusJson.Serialize(full);

        // Assert
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        doc.RootElement.GetProperty("verify").TryGetProperty("error", out _).Should().BeFalse();
    }
    ```
  - Выход: два новых теста, не компилирующихся (у `BackupVerify` нет третьего параметра).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.UnitTests -c Debug 2>&1 | tail -5` — ошибка компиляции CS7036 (нет перегрузки с 3 аргументами).
  - Spec: §3.7 (`BackupStatusJson`: писать `verify.error` опционально), §3.1 (`checked_unix` и при OK, и при FAILED).

- [ ] **Шаг 2: расширить модель и сериализацию**

  - Вход: шаг 1 (тесты падают на компиляции).
  - Действие:
    - `BackupsModel.cs`: `BackupVerify` → добавить `string? Error = null`; `BackupPolicy` → добавить `long? VerifyIntervalSec = null`.
    - `BackupStatusJson.cs`, в блоке `if (state.Verify is { } verify)` после `checked_unix`:
      ```csharp
      if (verify.Error is { } verifyError)
          v["error"] = verifyError;
      ```
    - `BackupsParser.cs` (воркера), `TryParseFull`: `verify = new BackupVerify(verifyState.Value, ReadLong(verifyEl, "checked_unix"), ReadString(verifyEl, "error"));`
    - `BackupsParser.cs`, `TryParsePolicy`: в блоке `verify`-объекта после `on_create` читать интервал:
      ```csharp
      long? verifyIntervalSec = null;
      if (root.TryGetProperty("verify", out var verifyObj)
          && verifyObj.ValueKind == JsonValueKind.Object
          && verifyObj.TryGetProperty("interval_sec", out var interval))
          verifyIntervalSec = interval.ValueKind is JsonValueKind.Number && interval.TryGetInt64(out var sec)
              ? sec
              : interval.ValueKind == JsonValueKind.String && long.TryParse(interval.GetString(), out var parsed)
                  ? parsed
                  : null;
      ```
      и передать в `new BackupPolicy(..., verifyOnCreate, verifyIntervalSec)`; отсутствие `interval_sec` → `VerifyIntervalSec=null` (политика НЕ null — дефолт подставит потребитель, spec §3.7).
  - Выход: модель/JSON/парсер читают и пишут `verify.error` + `verify.interval_sec`.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupStatusJson"` — PASS.
  - Spec: §3.7 (модель: `BackupVerify` + `Error`; `BackupPolicy` + `VerifyIntervalSec`; JSON: `verify.error`), Ф1.

- [ ] **Шаг 3: юнит-тесты парсера (воркер) — error/interval_sec/толерантность**

  - Вход: шаг 2 (модель расширена).
  - Действие: добавить в `PgWorker.UnitTests/Etcd/BackupsParserTests.cs` (по существующим там паттернам сида Kv):
    ```csharp
    // AAA: verify.error читается из full-ключа; interval_sec — из policy-ключа
    [Fact]
    public void Parse_VerifyErrorИIntervalSec_Читаются()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/backups/c1/policy",
                """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":3600}}"""),
            new Kv("/pgworker/backups/c1/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1757500000,"finished_unix":1757500300,"verify":{"state":"FAILED","checked_unix":1757500600,"error":"pg_verifybackup failed"}}"""),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs, out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value.Single(c => c.Cluster == "c1").Shards["shard1"].Full.Single();
        full.Verify!.Error.Should().Be("pg_verifybackup failed");
        full.Verify.State.Should().Be(BackupVerifyStatus.Failed);
        parsed.Value.Single(c => c.Cluster == "c1").Policy!.VerifyIntervalSec.Should().Be(3600);
    }

    // AAA: старые записи (verify без error, policy без interval_sec) парсятся — обратная совместимость
    [Fact]
    public void Parse_СтарыеЗаписи_БезНовыхПолей_Ок()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/backups/c2/policy",
                """{"full_max_age_sec":86400,"verify":{"on_create":false}}"""),
            new Kv("/pgworker/backups/c2/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1757500000,"verify":{"state":"OK","checked_unix":1757500600}}"""),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs, out var errors);

        // Assert — null, а не null-политика: дефолт подставляет потребитель
        errors.Should().BeEmpty();
        var cluster = parsed.Value.Single(c => c.Cluster == "c2");
        cluster.Policy.Should().NotBeNull();
        cluster.Policy!.VerifyIntervalSec.Should().BeNull();
        cluster.Shards["shard1"].Full.Single().Verify!.Error.Should().BeNull();
    }
    ```
  - Выход: тесты контракта парсера зелёные.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupsParserTests"` — PASS (включая старые).
  - Spec: §3.7, §5 (обратная совместимость).

- [ ] **Шаг 4: конфиг-дефолт VerifyIntervalSec**

  - Вход: шаг 2 (модель扩展ена).
  - Действие:
    - `src/PgWorker.Backups/Options.cs`: в `BackupsRuntimeOptions` добавить параметр `long VerifyIntervalSec = 604800` (в конец, именованные аргументы у существующих вызовов сохранятся).
    - `src/PgWorker.App/Options.cs`: в `BackupsPolicyOptions` — `public long VerifyIntervalSec { get; set; } = 604800;` (комментарий: период перепроверки оставшихся полных, `verify.interval_sec` policy-ключа перекрывает; `<= 0` — периодика выключена); в `ToRuntime()` — `VerifyIntervalSec: Policy.VerifyIntervalSec`.
  - Выход: `PgWorker:Backups:Policy:VerifyIntervalSec` (дефолт 604800) доезжает до рантайма.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug 2>&1 | tail -3` — Build succeeded (0 Warning(s), 0 Error(s)).
  - Spec: §3.7 (`BackupsRuntimeOptions` + `VerifyIntervalSec=604800`; options-маппер App), решение пользователя «периодическая перепроверка».

- [ ] **Шаг 5: коммит**

  - Вход: шаги 1–4 зелёные.
  - Действие:
    ```bash
    git add -A src/PgWorker.Etcd src/PgWorker.Backups src/PgWorker.App src/tests/PgWorker.UnitTests
    git commit -m "feat(backups): контракт verify — verify.error + policy verify.interval_sec (модель/JSON/парсеры, дефолт 604800)"
    ```
  - Выход: коммит задачи 2.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф1 (модель), §3.7.

---

### Задача 3: WalHistory — строгий парсер `.history` — Ф1

Парсер timeline-history (формат PostgreSQL: строки `<parentTLI> <switchWALLSN> [<reason>]`, последняя строка файла `NNNNNNNN.history` описывает сам TLI N — ответвление от parentTLI в точке switchWALLSN). Используется `WalChain.CheckRange` (задача 4) при строгой валидации TLI-переходов (spec §3.4).

**Files:**
- Create: `src/PgWorker.Backups/Model/WalHistory.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalHistoryTests.cs`

**Interfaces:**
- Consumes: ничего (чистая функция).
- Produces (для задачи 4): `WalHistory.Parse(string content)` → `IReadOnlyList<WalHistoryEntry>?` (`null` — ни одной валидной строки); `public sealed record WalHistoryEntry(uint ParentTli, string SwitchLsn);` — LSN канонической строкой `X/Y` (hex).

- [ ] **Шаг 1: написать падающие тесты**

  - Вход: задача 2 закоммичена.
  - Действие: создать `WalHistoryTests.cs`:
    ```csharp
    using FluentAssertions;
    using PgWorker.Backups;
    using Xunit;

    namespace PgWorker.UnitTests.Backups;

    // Парсер .history (arch/19 §3, t04): строки "parentTLI switchWALLSN [reason]";
    // последняя запись — сам TLI файла (ответвление от parentTLI в switchWALLSN).
    public class WalHistoryTests
    {
        // AAA: валидный history — записи в порядке строк, LSN без изменений
        [Fact]
        public void Parse_Валидный_возвращаетЗаписи()
        {
            // Arrange — 00000003.history: линия 3 ответвилась от 2, 2 — от 1
            const string content = "1\t0/2000000\tunknown\n2\t16/37D3000\tno promotion\n";

            // Act
            var entries = WalHistory.Parse(content);

            // Assert
            entries.Should().NotBeNull().And.HaveCount(2);
            entries![0].Should().Be(new WalHistoryEntry(1, "0/2000000"));
            entries[1].Should().Be(new WalHistoryEntry(2, "16/37D3000"));
        }

        // AAA: строки-мусор (не 2 первых поля hex/LSN) пропускаются, не роняют разбор
        [Fact]
        public void Parse_МусорныеСтроки_Пропускаются()
        {
            // Arrange
            const string content = "garbage line\n\n1\t0/2000000\n";

            // Act
            var entries = WalHistory.Parse(content);

            // Assert
            entries.Should().ContainSingle().Which.Should().Be(new WalHistoryEntry(1, "0/2000000"));
        }

        // AAA: пустой/полностью битый файл — null (строгий разбор не состоялся)
        [Fact]
        public void Parse_ПустойИлиБитый_null()
        {
            // Arrange / Act / Assert
            WalHistory.Parse("").Should().BeNull();
            WalHistory.Parse("no records here").Should().BeNull();
        }

        // AAA: parentTLI hex (формат PG — hex без 0x), LSN строго X/Y
        [Fact]
        public void Parse_HexParentИПлохойLsn_СтрогаяВалидация()
        {
            // Arrange — parentTLI "00000001" (hex-вид, как пишет PG), битый LSN в другой строке
            const string content = "00000001\t0/2000000\n2\tNOT_AN_LSN\n";

            // Act
            var entries = WalHistory.Parse(content);

            // Assert — битая строка пропущена, hex-parent распознан
            entries.Should().ContainSingle().Which.ParentTli.Should().Be(1);
        }
    }
    ```
  - Выход: тесты не компилируются (`WalHistory` не существует).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.UnitTests -c Debug 2>&1 | tail -3` — CS0246 `WalHistory`.
  - Spec: §3.4 (`WalHistory`: файл = строки `<parentTLI> <switchWALLSN> [<reason>]`; LSN `<hex>/<hex>`), AC4.

- [ ] **Шаг 2: реализовать WalHistory**

  - Вход: шаг 1.
  - Действие: создать `src/PgWorker.Backups/Model/WalHistory.cs`:
    ```csharp
    using System.Globalization;

    namespace PgWorker.Backups;

    /// <summary>Строка timeline-history: родитель и точка переключения (LSN «X/Y»).</summary>
    public sealed record WalHistoryEntry(uint ParentTli, string SwitchLsn);

    /// <summary>Строгий парсер `.history` (arch/19 §3, t04): строки
    /// «parentTLI switchWALLSN [reason]» (tab-разделение, как пишет PostgreSQL);
    /// последняя запись файла &lt;newTLI&gt;.history описывает сам newTLI — ответвление
    /// от parentTLI в switchWALLSN. Мусорные строки пропускаются; ни одной
    /// валидной → null (строгий разбор невозможен — фолбэк на эвристику t03).</summary>
    public static class WalHistory
    {
        public static IReadOnlyList<WalHistoryEntry>? Parse(string content)
        {
            List<WalHistoryEntry>? entries = null;
            foreach (var line in content.Split('\n'))
            {
                var fields = line.Trim('\r', ' ').Split('\t', ' ');
                if (fields.Length < 2)
                    continue;
                if (!uint.TryParse(fields[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var parent))
                    continue;
                if (!IsLsn(fields[1]))
                    continue;
                (entries ??= []).Add(new WalHistoryEntry(parent, fields[1]));
            }

            return entries is { Count: > 0 } list ? list : null;
        }

        // LSN — строго «X/Y»: обе половины hex (ненулевой длины).
        private static bool IsLsn(string value)
        {
            var parts = value.Split('/');
            return parts.Length == 2
                   && parts[0].Length is > 0 and <= 8
                   && parts[1].Length is > 0 and <= 8
                   && ulong.TryParse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out _)
                   && ulong.TryParse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out _);
        }
    }
    ```
  - Выход: парсер `.history` с валидацией LSN.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WalHistoryTests"` — PASS (4 теста).
  - Spec: §3.4, AC4.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный.
  - Действие: `git add src/PgWorker.Backups/Model/WalHistory.cs src/tests/PgWorker.UnitTests/Backups/WalHistoryTests.cs && git commit -m "feat(backups): WalHistory — строгий парсер .history (parentTLI + switchWALLSN, AC4)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф1, AC4.

---

### Задача 4: WalChain.CheckRange — непрерывность [start..end] + строгие TLI-переходы — Ф1

Расширение gap-детектора: проверка диапазона ДО точки бэкапа (end = последний сегмент набора `full/<id>/pg_wal/`); сегмент end обязан присутствовать среди объектов `wal/`; TLI-переходы при наличии содержимого history — строго по LSN, иначе фолбэк на эвристику t03 (spec §3.4).

**Files:**
- Modify: `src/PgWorker.Backups/Model/WalChain.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs`

**Interfaces:**
- Consumes: `WalFileName` (`TryParse`/`TryParseHistory`/`FromLsn`/`Next`), `WalHistory.Parse` (задача 3), существующий `WalChain.Check` (остаётся без изменений — runtime t03).
- Produces (для задачи 8):
  ```csharp
  public static ChainResult CheckRange(
      WalFileName chainStart,
      WalFileName end,
      IEnumerable<string> objectNames,                 // объекты wal/ (сегменты + *.history)
      IReadOnlyDictionary<uint, string>? historyContents = null)  // TLI → содержимое .history
  ```
  Семантика: непрерывность `[start..end]` (end включительно); `IsContinuous=true` ⇔ цепочка дошла ровно до end; сегменты выше end игнорируются; отсутствие `wal/<end>` → дыра «сегмент набора отсутствует в wal/».

- [ ] **Шаг 1: написать падающие тесты CheckRange**

  - Вход: задача 3 закоммичена.
  - Действие: добавить в `WalChainTests.cs` (используя существующий хелпер `Seg`):
    ```csharp
    // ── CheckRange (t04, arch/19 §3): непрерывность [start..end] ──

    // AAA: сплошной диапазон [1..3] с end=3 — непрерывен, LastSegment == end
    [Fact]
    public void CheckRange_СплошнойДиапазон_Непрерывен()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000010000000000000002", "000000010000000000000003" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000003");
    }

    // AAA: сегменты ВЫШЕ end (после точки бэкапа) не мешают — диапазон закрыт
    [Fact]
    public void CheckRange_СегментыВышеEnd_Игнорируются()
    {
        // Arrange — end=2, но wal/ содержит ещё 3 и 4 (поток t03 продолжился)
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000003", "000000010000000000000004",
        };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 2), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000002");
    }

    // AAA: дыра внутри [start..end] — разрыв с границами (AC3)
    [Fact]
    public void CheckRange_ДыраДоEnd_РазрывСГраницами()
    {
        // Arrange — нет 2, диапазон [1..3]
        var objects = new[] { "000000010000000000000001", "000000010000000000000003" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000002")
            .And.Contain("000000010000000000000003");
    }

    // AAA: сегмент end есть в наборе (pg_wal), но объекта wal/<end> нет — дыра
    // дублирования t02 (AC3)
    [Fact]
    public void CheckRange_НетEndОбъектаВWal_Дыра()
    {
        // Arrange — wal/ обрывается на 2, end (из листинга набора) = 3
        var objects = new[] { "000000010000000000000001", "000000010000000000000002" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000003");
    }

    // AAA: пустой wal/ при заданном end — дыра от старта
    [Fact]
    public void CheckRange_ПустойWal_ДыраОтСтарта()
    {
        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 2), []);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000001");
    }

    // ── CheckRange: TLI-переходы ──

    // AAA: fallback — без historyContents работает эвристика t03 (history-объект
    // в листинге + первый сегмент нового TLI ∈ {последний старого, Next(last)})
    [Fact]
    public void CheckRange_ПереходБезСодержимого_Эвристика()
    {
        // Arrange — TLI 1: seg 1; TLI 2: seg 2 (== Next(1)); history-объект есть
        var objects = new[]
        {
            "000000010000000000000001", "000000020000000000000002", "00000002.history",
        };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    // AAA: строгий переход — switchWALLSN внутри границ (сегмент позиции == первому
    // сегменту нового TLI) → непрерывен (AC4)
    [Fact]
    public void CheckRange_СтрогийПереход_ТочкаВГраницах_Ок()
    {
        // Arrange — первый сегмент TLI 2 = 2 (позиция 0/2000000); история: parent=1, lsn в сегменте 2
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "1\t0/2000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    // AAA: строгий переход — точка вне границ (switchLSN указывает на сегмент 5,
    // а первый сегмент нового TLI — 2) → FAILED с границами (AC4)
    [Fact]
    public void CheckRange_СтрогийПереход_ТочкаВнеГраниц_Дыра()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "1\t0/5000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("00000002.history").And.Contain("0/5000000");
    }

    // AAA: строгий переход — parent не совпадает (history говорит parent=5) → дыра
    [Fact]
    public void CheckRange_СтрогийПереход_ЧужойParent_Дыра()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "5\t0/2000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("parent");
    }
    ```
  - Выход: тесты не компилируются (`CheckRange` нет).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.UnitTests -c Debug 2>&1 | tail -3` — CS0117 `CheckRange`.
  - Spec: §3.4 (`CheckRange`), AC3 (юниты: непрерывность, дыра, отсутствие end), AC4 (строгий LSN-разбор).

- [ ] **Шаг 2: реализовать CheckRange**

  - Вход: шаг 1.
  - Действие: в `WalChain.cs` тело цикла `Check` вынести в общий приватный `Walk(chainStart, end, objectNames, historyContents)` (Check вызывает `Walk(start, null, names, null)` — поведение идентично прежнему, что защищают существующие тесты) и добавить:
    ```csharp
    /// <summary>Проверка диапазона [chainStart..end] (t04, arch/19 §3): end —
    /// последний сегмент набора full/&lt;id&gt;/pg_wal/ (точка бэкапа); сегмент end
    /// ОБЯЗАН присутствовать среди объектов wal/ (дублирование t02). TLI-переходы
    /// в диапазоне: при наличии содержимого history (TLI → текст) — строго по
    /// switchWALLSN; без содержимого — эвристика t03 (Check). Сегменты выше end
    /// игнорируются (дальше — домен wal-статуса t03).</summary>
    public static ChainResult CheckRange(
        WalFileName chainStart, WalFileName end, IEnumerable<string> objectNames,
        IReadOnlyDictionary<uint, string>? historyContents = null)
    {
        var (segments, histories) = Collect(objectNames);
        WalFileName? last = null;
        var expected = chainStart;
        foreach (var segment in segments)
        {
            if (segment.Tli == expected.Tli)
            {
                if (segment.Log == expected.Log && segment.Seg == expected.Seg)
                {
                    last = segment;
                    expected = segment.Next();
                    if (segment.Tli == end.Tli && Position(segment) == Position(end))
                        return new ChainResult(true, null, segment); // диапазон закрыт
                }
                else if (Position(segment) > Position(expected))
                    return new ChainResult(false,
                        $"дыра WAL-цепочки: ожидался {expected.Name}, найден {segment.Name}", last);
                // сегмент ниже expected (хвост/дубли) — игнор
            }
            else if (segment.Tli > expected.Tli)
            {
                // переход: строго по содержимому history, иначе эвристика t03.
                // Проверку «segment == end» делаем ПОСЛЕ валидации перехода —
                // end может быть первым сегментом нового TLI.
                if (historyContents is not null
                    && historyContents.TryGetValue(segment.Tli, out var content)
                    && WalHistory.Parse(content) is { Count: > 0 } entries)
                {
                    var entry = entries[^1]; // последняя строка — сам TLI файла
                    if (entry.ParentTli != expected.Tli)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: history-файл {segment.Tli:x8}.history: " +
                            $"parent {entry.ParentTli} ≠ {expected.Tli}", last);
                    var switchPos = WalFileName.FromLsn(0, entry.SwitchLsn);
                    var inBounds = (switchPos.Log, switchPos.Seg) == (segment.Log, segment.Seg)
                        || (last is { } prev && (switchPos.Log, switchPos.Seg) == (prev.Log, prev.Seg));
                    if (!inBounds)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: switchWALLSN {entry.SwitchLsn} " +
                            $"вне границы сегментов {(last is { } l ? l.Name : "нет предыдущего")}/{segment.Name} " +
                            $"({segment.Tli:x8}.history)", last);
                }
                else
                {
                    // эвристика t03 (Check): history-объект + первый сегмент ∈ {last, Next(last)}
                    if (!histories.Contains(segment.Tli))
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli} без {segment.Tli:x8}.history", last);
                    var lastOrNext = last is { } prev
                        ? (segment.Log, segment.Seg) == (prev.Log, prev.Seg)
                          || (segment.Log, segment.Seg) == (prev.Next().Log, prev.Next().Seg)
                        : false;
                    if (!lastOrNext)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: первый сегмент {segment.Name} " +
                            $"не совпадает с точкой переключения ({(last is { } l ? l.Name : "нет предыдущего")})", last);
                }

                last = segment;
                expected = segment.Next();
                if (segment.Tli == end.Tli && Position(segment) == Position(end))
                    return new ChainResult(true, null, segment); // диапазон закрыт
            }
            // сегмент старого TLI после перехода — дубли ниже expected — игнор
        }

        // список кончился, end не достигнут: цепочка шла сплошно, но сегмент набора
        // не продублирован в wal/ (дублирование t02, spec §3.4)
        return new ChainResult(false,
            $"сегмент {end.Name} из набора бэкапа отсутствует в wal/ (ожидался после " +
            $"{(last is { } l ? l.Name : chainStart.Name)})", last);
    }

    // Позиция сегмента внутри TLI (сравнение/сортировка — как в Check).
    private static long Position(WalFileName s) => (long)(s.Log * WalFileName.SegsPerLog + s.Seg);
    ```
    `Collect(objectNames)` — вынесенный из `Check` разбор (сегменты + history-TLI-множество) и сортировка — тот же код, что сейчас в начале `Check`.
  - Выход: `CheckRange` с фолбэком; `Check` — тонкая обёртка (старые тесты без изменений зелёные).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WalChainTests"` — PASS (старые + 9 новых).
  - Spec: §3.4, AC3, AC4.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный.
  - Действие: `git add src/PgWorker.Backups/Model/WalChain.cs src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs && git commit -m "feat(backups): WalChain.CheckRange — диапазон до точки бэкапа + строгие TLI-переходы по LSN (AC3/AC4)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф1, AC3, AC4.

---

### Задача 5: VerifyJobCommand + VerifyJobSpec — inline-джоб checksums — Ф1

Инлайн-команда verify-джоба (паттерн `WalAgentCommand`: inline bash, `Cmd` — полная замена ENTRYPOINT) и его `ContainerSpec` (паттерн `BackupJobSpec`). Протокол — образец t02: result-JSON в stdout + exit-код; фазы `download`/`verify` различают transient/permanent (spec §3.2).

**Files:**
- Create: `src/PgWorker.Backups/VerifyJobCommand.cs`
- Create: `src/PgWorker.Backups/Job/VerifyJobSpec.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/VerifyJobCommandTests.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupJobSpecTests.cs` (добавить verify-кейсы)

**Interfaces:**
- Consumes: `ContainerSpec` (`Cmd` — полная замена ENTRYPOINT, контракт `IDockerEngine.cs`), `BackupsRuntimeOptions` (`AgentS3Endpoint`, `StagingQuotaBytes`, `StagingDir`, `AgentCpu`, `AgentMem`, `JobImage`), env-контракт t02 `PGW_BK_*`.
- Produces (для задачи 8):
  ```csharp
  public static class VerifyJobCommand
  {
      public const string EnvMcHostVariable = "MC_HOST_pgw"; // alias pgw — образец entrypoint t02
      public const string EnvS3Endpoint = "PGW_BK_S3_ENDPOINT";
      public const string EnvS3Region = "PGW_BK_S3_REGION";
      public const string EnvS3Bucket = "PGW_BK_S3_BUCKET";
      public const string EnvS3AccessKey = "PGW_BK_S3_ACCESS_KEY";
      public const string EnvS3SecretKey = "PGW_BK_S3_SECRET_KEY";
      public const string EnvPrefix = "PGW_BK_PREFIX";
      public const string EnvId = "PGW_BK_ID";
      public const string EnvStagingDir = "PGW_BK_STAGING_DIR";
      public static string McHost(string endpoint, string accessKey, string secretKey); // scheme://ak:sk@authority, URL-escape кредов
      public static IReadOnlyList<string> Build(); // ["bash","-c",script]
  }
  public static class VerifyJobSpec
  {
      public static ContainerSpec Build(BackupsRuntimeOptions opts, string cluster, string shard, string id);
  }
  ```

- [ ] **Шаг 1: написать падающие тесты команды**

  - Вход: задача 4 закоммичена.
  - Действие: создать `VerifyJobCommandTests.cs` (паттерн `WalAgentCommandTests` — там ассерты на содержимое скрипта/env-имён):
    ```csharp
    using FluentAssertions;
    using PgWorker.Backups;
    using Xunit;

    namespace PgWorker.UnitTests.Backups;

    // Inline-команда verify-джоба (arch/19 §2/§5, t04): mc cp → pg_verifybackup;
    // протокол t02 — result-JSON в stdout + exit-код; фазы download/verify.
    public class VerifyJobCommandTests
    {
        // AAA: Cmd = bash -c script; скрипт качает full/<id>/ 1:1 и зовёт pg_verifybackup
        [Fact]
        public void Build_BashСкрипт_КачаетИВерифицирует()
        {
            // Act
            var cmd = VerifyJobCommand.Build();

            // Assert
            cmd.Should().HaveCount(3).And.HaveElementAt(0, "bash").And.HaveElementAt(1, "-c");
            var script = cmd[2];
            script.Should().Contain("mc cp --recursive \"pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/\" \"$PGW_BK_STAGING_DIR/full/\"");
            script.Should().Contain("pg_verifybackup \"$PGW_BK_STAGING_DIR/full\"");
        }

        // AAA: протокол — phase-маркеры download/verify и result-JSON ok/error; exit 1 при провале
        [Fact]
        public void Build_ПротоколResultJson_иФазы()
        {
            // Act
            var script = VerifyJobCommand.Build()[2];

            // Assert
            script.Should().Contain("\"phase\":\"download\"").And.Contain("\"phase\":\"verify\"");
            script.Should().Contain("\"ok\":true");
            script.Should().Contain("exit 1"); // FAIL() — провал фазы
            script.Should().Contain("tr -d '\"'"); // JSON-безопасность ошибки — образец FAIL() t02
        }

        // AAA: McHost — scheme://ak:sk@authority с URL-escape кредов (секреты не в argv)
        [Fact]
        public void McHost_ФорматИEscape()
        {
            // Act
            var value = VerifyJobCommand.McHost("http://minio:9000", "ak/1", "sk:2");

            // Assert
            value.Should().Be("http://ak%2F1:sk%3A2@minio:9000");
        }
    }
    ```
  - Выход: тесты не компилируются.
  - Проверка: сборка тестов — CS0246 `VerifyJobCommand`.
  - Spec: §3.2 (скрипт: `mc cp` → FAIL download → `pg_verifybackup` → FAIL verify → успех `{"ok":true}`), Ф1 (юниты: команда джоба — env-контракт).

- [ ] **Шаг 2: реализовать VerifyJobCommand**

  - Вход: шаг 1.
  - Действие: создать `src/PgWorker.Backups/VerifyJobCommand.cs`:
    ```csharp
    namespace PgWorker.Backups;

    /// <summary>Билдер inline bash-команды verify-джоба (arch/19 §2/§5, t04; паттерн
    /// WalAgentCommand): скачивание full/&lt;id&gt;/ 1:1 в staging (mc) → pg_verifybackup
    /// (manifest SHA256, вкл. pg_wal/ набора). Протокол t02: result-JSON в stdout +
    /// exit-код; вывод утилиты — stderr. S3-креды — ТОЛЬКО env (MC_HOST_pgw собирает
    /// воркер; секреты не в argv/ps). Статус PENDING/OK/FAILED пишет воркер — джоб
    /// etcd не касается. Идемпотентен: S3 только читает.</summary>
    public static class VerifyJobCommand
    {
        public const string EnvMcHostVariable = "MC_HOST_pgw";
        public const string EnvS3Endpoint = "PGW_BK_S3_ENDPOINT";
        public const string EnvS3Region = "PGW_BK_S3_REGION";
        public const string EnvS3Bucket = "PGW_BK_S3_BUCKET";
        public const string EnvS3AccessKey = "PGW_BK_S3_ACCESS_KEY";
        public const string EnvS3SecretKey = "PGW_BK_S3_SECRET_KEY";
        public const string EnvPrefix = "PGW_BK_PREFIX";
        public const string EnvId = "PGW_BK_ID";
        public const string EnvStagingDir = "PGW_BK_STAGING_DIR";

        /// <summary>Значение env MC_HOST_pgw: scheme://access:secret@authority
        /// (URL-escape кредов — per-install спецсимволы).</summary>
        public static string McHost(string endpoint, string accessKey, string secretKey)
        {
            var uri = new Uri(endpoint);
            return $"{uri.Scheme}://{Uri.EscapeDataString(accessKey)}:{Uri.EscapeDataString(secretKey)}@{uri.Authority}";
        }

        /// <summary>Cmd контейнера: ["bash","-c",script] — полная замена ENTRYPOINT.</summary>
        public static IReadOnlyList<string> Build()
        {
            const string script = """
    set -euo pipefail
    # протокол t02: result-JSON — stdout, вывод утилит — stderr/stdin-перехват
    LOG() { printf '%s\n' "$1"; }
    FAIL() {
      LOG "{\"ok\":false,\"phase\":\"$1\",\"error\":\"$(printf '%s' "$2" | tr '\n' ' ' | tr -d '"')\"}"
      exit 1
    }
    LOG '{"phase":"download"}'
    mc cp --recursive "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/" "$PGW_BK_STAGING_DIR/full/" \
      || FAIL download "mc cp failed"
    LOG '{"phase":"verify"}'
    if OUT="$(pg_verifybackup "$PGW_BK_STAGING_DIR/full" 2>&1)"; then
      LOG '{"ok":true}'
    else
      FAIL verify "$(printf '%s' "$OUT" | tail -n 1)"
    fi
    """;
            return ["bash", "-c", script];
        }
    }
    ```
  - Выход: команда джоба с фазовым протоколом.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~VerifyJobCommandTests"` — PASS (3 теста).
  - Spec: §3.2, принцип 4 (exit-код — истина итога), §5 (креды только env).

- [ ] **Шаг 3: тесты VerifyJobSpec (по образцу BackupJobSpecTests) и реализация**

  - Вход: шаг 2; существующий `BackupJobSpecTests.cs` — образец ассертов на env/VolumeName/Tmpfs/Ports/ExtraHosts.
  - Действие: добавить в `BackupJobSpecTests.cs` тесты:
    ```csharp
    // AAA: verify-спека — полный PGW_BK_* env-комплект (вкл. REGION) + MC_HOST_pgw
    // (креды только env), Cmd — полная замена ENTRYPOINT, лимиты Agent { Cpu, Mem },
    // портов нет, restart no (t04, arch/19 §5, spec §3.2)
    [Fact]
    public void VerifyJobSpec_EnvContract_и_Контейнер()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions
        {
            Enabled = true, S3Endpoint = "http://minio:9000", S3Region = "us-east-1", S3Bucket = "bkt",
            S3AccessKey = "ak", S3SecretKey = "sk", JobImage = "pgworker-backup:dev",
            StagingDir = "/backup-staging", AgentCpu = 0.5, AgentMem = 536870912,
        };

        // Act
        var spec = VerifyJobSpec.Build(opts, "c1", "shard1", "20260911120000Z");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        spec.Env["PGW_BK_S3_ENDPOINT"].Should().Be("http://minio:9000");
        spec.Env["PGW_BK_S3_REGION"].Should().Be("us-east-1");
        spec.Env["PGW_BK_S3_BUCKET"].Should().Be("bkt");
        spec.Env["PGW_BK_PREFIX"].Should().Be("c1/shard1");
        spec.Env["PGW_BK_ID"].Should().Be("20260911120000Z");
        spec.Env["PGW_BK_STAGING_DIR"].Should().Be("/backup-staging");
        spec.Env[VerifyJobCommand.EnvMcHostVariable].Should().Contain("ak:sk@minio:9000");
        spec.Cmd.Should().BeEquivalentTo(VerifyJobCommand.Build(), o => o.WithStrictOrdering());
        spec.CpuCores.Should().Be(0.5, "лимиты Agent { Cpu, Mem } — как у джоба t02 (spec §3.2)");
        spec.MemoryBytes.Should().Be(536870912);
        spec.Ports.Should().BeEmpty();
        spec.RestartPolicy.Should().Be("no");
        spec.ExtraHosts.Should().BeEquivalentTo(["host.docker.internal:host-gateway", "local:host-gateway"]);
        spec.Label.Should().Be("c1");
    }

    // AAA: квота staging → tmpfs size=; без квоты → named volume
    [Fact]
    public void VerifyJobSpec_Квота_Tmpfs()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions { StagingQuotaBytes = 1024, StagingDir = "/backup-staging" };

        // Act
        var withQuota = VerifyJobSpec.Build(opts, "c1", "s1", "id1");
        var noQuota = VerifyJobSpec.Build(opts with { StagingQuotaBytes = null }, "c1", "s1", "id1");

        // Assert — ENOSPC → download-phase transient (арх. §6 канона)
        withQuota.Tmpfs!["/backup-staging"].Should().Be("size=1024");
        withQuota.VolumeName.Should().BeEmpty();
        noQuota.VolumeName.Should().Be(BackupNames.VerifyVolumeName("c1", "s1", "id1"));
    }
    ```
    И создать `src/PgWorker.Backups/Job/VerifyJobSpec.cs` (образец — `BackupJobSpec.Build`; отличия: без DSN/пароля PG; `Cmd: VerifyJobCommand.Build()`; `VolumeName: opts.StagingQuotaBytes is null ? BackupNames.VerifyVolumeName(cluster, shard, id) : ""`; `Hostname: BackupNames.VerifyContainerName(cluster, shard, id)`; лимиты `CpuCores: opts.AgentCpu, MemoryBytes: opts.AgentMem` — как у джоба t02 (spec §3.2); env — из интерфейсов выше + `PGW_BK_S3_REGION = opts.S3Region ?? string.Empty` + `MC_HOST_pgw = VerifyJobCommand.McHost(opts.S3Endpoint, opts.S3AccessKey, opts.S3SecretKey)`).
    Примечание: `BackupNames.VerifyContainerName/VerifyVolumeName` создаются здесь же минимально (методы из задачи 7 приходят раньше — см. ниже порядок: имена добавляются в ЭТОЙ задаче шагом 4).
  - Выход: спека джоба с env-контрактом, tmpfs-квотой, Cmd-заменой ENTRYPOINT.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupJobSpecTests"` — PASS (включая старые t02).
  - Spec: §3.2 (контейнер: образ `Backups:Job.Image`, `Cmd` — замена ENTRYPOINT, портов нет, staging-volume + квота → tmpfs, лимиты Agent, restart no, extra_hosts как t02).

- [ ] **Шаг 4: BackupNames — verify-имена**

  - Вход: шаг 3 использует имена.
  - Действие: добавить в `BackupNames.cs`:
    ```csharp
    // t04: имена verify-джоба — детерминированы (супервиз takeover-инвариантен);
    // контейнер и volume — одно имя (volume ephemeral, чистится после итога).
    public static string VerifyContainerName(string cluster, string shard, string id)
        => $"pgw-backup-verify-{cluster}-{shard}-{id}";

    public static string VerifyVolumeName(string cluster, string shard, string id)
        => $"pgw-backup-verify-{cluster}-{shard}-{id}";

    public static string VerifyJobContainerPrefix(string cluster) => $"pgw-backup-verify-{cluster}-";
    ```
  - Выход: канонические имена (существующие вызовы не затронуты).
  - Проверка: полная сборка юнитов зелёная: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~Backups"` — PASS.
  - Spec: §3.2/§3.3 (имя `pgw-backup-verify-<C>-<X>-<id>`, супервиз по детерминированному имени).

- [ ] **Шаг 5: коммит**

  - Вход: шаги 1–4 зелёные.
  - Действие: `git add -A src/PgWorker.Backups src/tests/PgWorker.UnitTests && git commit -m "feat(backups): VerifyJobCommand + VerifyJobSpec — ephemeral джоб pg_verifybackup (env-контракт PGW_BK_*, фазы download/verify)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф1, §3.2.

---

### Задача 6: S3 — ListAsync(произвольный префикс) + GetObjectAsync — Ф2

Обобщение list-обёртки (пагинация list-v2 на любой префикс `<C>/<X>/…`) и GET маленьких history-объектов. `ListWalAsync` остаётся в интерфейсе (вызовы t03 не меняются) и рефакторится на общий путь (spec §3.4).

**Files:**
- Modify: `src/PgWorker.Backups/BackupS3.cs` (интерфейс `IBackupS3` + реализация)
- Modify: `src/PgWorker.App/Program.cs` (`ReloadableBackupS3` + `DisabledBackupS3` — прокси новых методов)
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` (`FakeBackupS3`)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs`

**Interfaces:**
- Consumes: `IBackupS3.ListWalAsync` (существующий), AWSSDK `ListObjectsV2`/`GetObject`.
- Produces (для задач 8–9):
  ```csharp
  // prefix — относительно <C>/<X>/: "wal/" | "full/<id>/pg_wal/"
  Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);
  // key — относительно <C>/<X>/: "wal/00000002.history"; возвращает содержимое (крошечные history)
  Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default);
  ```

- [ ] **Шаг 1: падающие интеграционные тесты (MinIO)**

  - Вход: задача 5 закоммичена; `MinioFixture`/`MinioCollection` существуют.
  - Действие: добавить в `BackupS3Tests.cs` (сеется прямым AWSSDK-клиентом, как существующие):
    ```csharp
    // AAA: ListAsync произвольного префикса full/<id>/pg_wal/ — только объекты префикса
    [Fact]
    public async Task ListAsync_ПрефиксНабора_ВозвращаетТолькоЕгоОбъекты()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/full/20260911120000Z/pg_wal/000000010000000000000001", ContentBody = "x" }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/wal/000000010000000000000001", ContentBody = "x" }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/full/20260911120000Z/PG_VERSION", ContentBody = "x" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListAsync("v1", "shard1", "full/20260911120000Z/pg_wal/", ct: ct);

        // Assert
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Select(o => o.Name).Should().BeEquivalentTo(["000000010000000000000001"]);
    }

    // AAA: ListAsync пагинация на произвольном префиксе (maxKeys=2, 5 объектов)
    [Fact]
    public async Task ListAsync_Пагинация_СобираетВсе()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        for (var i = 1; i <= 5; i++)
            await client.PutObjectAsync(new PutObjectRequest
            { BucketName = MinioFixture.Bucket, Key = $"v2/shard1/wal/0000000100000000000000{i:x2}", ContentBody = "x" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListAsync("v2", "shard1", "wal/", maxKeysPerTest: 2, ct: ct);

        // Assert
        listed.Value.Should().HaveCount(5);
    }

    // AAA: GetObjectAsync — содержимое history-объекта (крошечный GET)
    [Fact]
    public async Task GetObject_ОтдаетСодержимое()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v3/shard1/wal/00000002.history", ContentBody = "1\t0/2000000\n" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var got = await s3.GetObjectAsync("v3", "shard1", "wal/00000002.history", ct);

        // Assert
        got.IsSuccess.Should().BeTrue();
        got.Value.Should().Be("1\t0/2000000\n");
    }

    // AAA: GetObjectAsync несуществующего — Failed (не исключение мимо Result)
    [Fact]
    public async Task GetObject_Отсутствует_Failed()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var got = await s3.GetObjectAsync("v4", "shard1", "wal/00000009.history", TestContext.Current.CancellationToken);

        // Assert
        got.IsSuccess.Should().BeFalse();
    }
    ```
  - Выход: тесты не компилируются (`ListAsync`/`GetObjectAsync` нет в `BackupS3`).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Debug 2>&1 | tail -3` — CS1061.
  - Spec: §3.4 (`BackupS3`: `ListAsync(cluster, shard, prefix)` — обобщение; `GetObjectAsync` — history), Ф2, AC4 (verify использует содержимое history — GET из S3).

- [ ] **Шаг 2: реализация BackupS3 + прокси Reloadable/Disabled**

  - Вход: шаг 1.
  - Действие:
    - В `IBackupS3` добавить `ListAsync`/`GetObjectAsync` (сигнатуры выше, XML-комментарии как у `ListWalAsync`).
    - `BackupS3`: тело `ListWalAsync` переиспользовать — общий приватный `ListByPrefixAsync(string fullPrefix, int? maxKeys, ct)`; `ListWalAsync` → `ListByPrefixAsync($"{cluster}/{shard}/wal/", ...)`; `ListAsync` → `ListByPrefixAsync($"{cluster}/{shard}/{prefix}", ...)`; `GetObjectAsync` → `_client.GetObjectAsync(new GetObjectRequest { BucketName = _bucket, Key = $"{cluster}/{shard}/{key}" })` с чтением `ResponseStream` (`StreamReader`, UTF8), ошибки — `Result<string>.Failed(new ApplicationException($"S3 get {key}: {e.Message}", e))`.
    - `Program.cs`: `ReloadableBackupS3` — прокси-методы `ListAsync`/`GetObjectAsync` по образцу существующего `ListWalAsync`-прокси; `DisabledBackupS3` — те же методы с `Failed(new ApplicationException("Backups:Enabled=false"))`.
  - Выход: S3-обёртка листит произвольный префикс и отдаёт содержимое объектов; горячая конфигурация и заглушка консистентны.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug 2>&1 | tail -3` — Build succeeded.
  - Spec: §3.4, Ф2.

- [ ] **Шаг 3: расширить FakeBackupS3 + прогнать интеграции S3**

  - Вход: шаг 2; `FakeBackupS3` — поверхность `IBackupS3` (реализует интерфейс — упадёт компиляцией, пока не расширить).
  - Действие: в `FakeBackupDeps.cs` расширить `FakeBackupS3`:
    ```csharp
    // Объекты с полным ключом после <C>/<X>/ (наборы full/<id>/pg_wal/…): сид тестов
    // задач 8–9. Содержимое history — в Contents. Голые wal-имена остаются в
    // Objects (совместимость с тестами t03): ListAsync("wal/") их ОБЪЕДИНЯЕТ с
    // PrefixedObjects-ключами на "wal/" — единая картина wal/-префикса для CheckRange.
    public List<(string Cluster, string Shard, string Key)> PrefixedObjects { get; } = [];
    public Dictionary<(string Cluster, string Shard, string Key), string> Contents { get; } = [];
    public bool Fails { get; set; }           // транспорт-сбой ВСЕХ операций S3 (transient-тест list задачи 8)
    public bool FailsGetObject { get; set; }  // падает только GET (transient-тест GET history задачи 8)

    public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        if (Fails)
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("s3 down")));
        var prefixed = PrefixedObjects
            .Where(o => o.Cluster == cluster && o.Shard == shard
                        && o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(o => new WalObject(o.Key[(o.Key.LastIndexOf('/') + 1)..], LastModified));
        // wal/-префикс пополняется Objects (голые имена wal-сегментов/history);
        // дубликаты имён схлопываются — порядок для CheckRange не важен (сортирует).
        var walNames = prefix == "wal/"
            ? Objects.Where(o => o.Cluster == cluster && o.Shard == shard)
                .Select(o => new WalObject(o.Name, LastModified))
            : [];
        var merged = prefixed.Concat(walNames)
            .GroupBy(o => o.Name, StringComparer.Ordinal)
            .Select(g => g.First())
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
            (IReadOnlyList<WalObject>)merged));
    }

    public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default)
        => Task.FromResult(Fails || FailsGetObject || !Contents.TryGetValue((cluster, shard, key), out var content)
            ? Result<string>.Failed(new ApplicationException("s3 get failed"))
            : Result<string>.Success(content));
    ```
    (существующие `BucketExistsAsync`/`ListWalAsync` — без изменений; при `Fails=true` `ListWalAsync` тоже отдаёт Failed — добавить тот же гвард).
  - Выход: фейк готов для задач 8–9.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupS3Tests"` — PASS (старые + 4 новых). После серии (docker): `docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f` (фильтр не задевает dev-стенд `as-*`/`adminpanel`).
  - Spec: §3.4, Ф2 (интеграция против MinIO-testcontainer, динамический порт).

- [ ] **Шаг 4: коммит**

  - Вход: шаг 3 зелёный, зачистка серий выполнена.
  - Действие: `git add -A src/PgWorker.Backups src/PgWorker.App src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): S3 ListAsync(префикс)+GetObjectAsync — list-v2 пагинация и GET history (Ф2)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф2, §3.4.

---

### Задача 7: D1 — Deprovisioning чистит verify-джобы — Ф3 (часть wiring)

`BackupJobsCleaner` (общая чистка джоб-контейнеров кластера) дополняется verify-префиксом: контейнеры `pgw-backup-verify-<C>-*` + volume с тем же именем (spec §3.3).

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (`BackupJobsCleaner`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (добавить кейс) или новый `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs`

**Interfaces:**
- Consumes: `BackupNames.VerifyJobContainerPrefix` (задача 5 шаг 4), `IDockerEngine.ListContainersAsync/RemoveContainerAsync/RemoveVolumeAsync`.
- Produces: `BackupJobsCleaner.VerifyContainerPrefix = "pgw-backup-verify-"` (константа канона, дубль без ссылки — как существующие).

- [ ] **Шаг 1: падающий юнит-тест чистки**

  - Вход: задача 6 закоммичена; `InternalsVisibleTo("PgWorker.UnitTests")` уже есть в `PgWorker.Docker.csproj`.
  - Действие: новый `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs` с локальным фейком `IDockerEngine` (список контейнеров/томов в памяти, по образцу `FakeBackupEngine` из `BackupProcessTests`, только нужные 3 метода; остальные — `throw new NotSupportedException()`):
    ```csharp
    // AAA: чистка D1 убирает и full-, и verify-джобы кластера с их volumes (t04)
    [Fact]
    public async Task Remove_чищает_full_и_verify_джобы_сVolumes()
    {
        // Arrange — движок с full-джобом (t02) и verify-джобом (t04) кластера c1
        var engine = new FakeCleanerEngine
        {
            Containers =
            [
                ("pgw-backup-full-c1-shard1-20260911120000Z", "exited"),
                ("pgw-backup-verify-c1-shard1-20260911120000Z", "exited"),
                ("pgw-backup-verify-c2-shard1-20260911130000Z", "running"), // чужой кластер
            ],
        };

        // Act
        var removed = await BackupJobsCleaner.RemoveAsync([engine], "c1", CancellationToken.None);

        // Assert — контейнер c2 не тронут; volume выводится из имени:
        // full → pgw-backup-<C>-<X>-<id>; verify → имя контейнера (volume = имя)
        removed.IsSuccess.Should().BeTrue();
        engine.RemovedContainers.Should().BeEquivalentTo(
            ["pgw-backup-full-c1-shard1-20260911120000Z", "pgw-backup-verify-c1-shard1-20260911120000Z"]);
        engine.RemovedVolumes.Should().BeEquivalentTo(
            ["pgw-backup-c1-shard1-20260911120000Z", "pgw-backup-verify-c1-shard1-20260911120000Z"]);
    }
    ```
  - Выход: тест падает — verify-контейнер не удаляется текущей реализацией.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupJobsCleanerTests"` — FAIL (verify-имена остаются).
  - Spec: §3.3 (Deprovisioning D1 дополняется префиксом `pgw-backup-verify-<C>-*` +volume-префикс), AC8.

- [ ] **Шаг 2: расширить BackupJobsCleaner**

  - Вход: шаг 1.
  - Действие: в `BackupJobsCleaner.RemoveAsync` обрабатывать оба префикса:
    ```csharp
    public const string VerifyContainerPrefix = "pgw-backup-verify-";

    public static async Task<Result> RemoveAsync(IEnumerable<IDockerEngine> engines, string cluster, CancellationToken ct)
    {
        // t04: verify-джобы — контейнер и volume НОСЯТ ОДНО ИМЯ (в отличие от full:
        // pgw-backup-full-<C>-<X>-<id> → volume pgw-backup-<C>-<X>-<id>).
        // volumePrefix ОБЯЗАН содержать кластер (существующий код ClusterDriver.cs:
        // pgw-backup-<C>-) — имя контейнера без префикса даёт только <X>-<id>.
        foreach (var (prefix, sameVolumeName) in new[]
                 { (JobContainerPrefix, false), (VerifyContainerPrefix, true) })
        {
            var containerPrefix = $"{prefix}{cluster}-";
            var volumePrefix = $"{JobVolumePrefix}{cluster}-"; // pgw-backup-<C>-
            foreach (var engine in engines)
            {
                var list = await engine.ListContainersAsync(containerPrefix, all: true, ct);
                if (!list.IsSuccess) return list;
                foreach (var container in list.Value.Where(c => c.Names.Any(n => n.StartsWith(containerPrefix, StringComparison.Ordinal))))
                {
                    var name = container.Names.First(n => n.StartsWith(containerPrefix, StringComparison.Ordinal));
                    var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                    if (!removed.IsSuccess) return removed;
                    var volume = sameVolumeName ? name : volumePrefix + name[containerPrefix.Length..];
                    var volumeRemoved = await engine.RemoveVolumeAsync(volume, ct);
                    if (!volumeRemoved.IsSuccess) return volumeRemoved;
                }
            }
        }

        return Result.Success();
    }
    ```
    (существующее поведение для full-префикса не меняется — защищают текущие тесты deprovisioning).
  - Выход: D1 чистит оба класса джобов.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupJobsCleanerTests|FullyQualifiedName~Deprovisioning"` — PASS.
  - Spec: §3.3, AC8.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный.
  - Действие: `git add src/PgWorker.Docker/Drivers/ClusterDriver.cs src/tests/PgWorker.UnitTests/Docker && git commit -m "feat(backups): D1-чистка deprovisioning — verify-джобы и их volumes (AC8)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф3 (D1-префиксы), AC8.

---

### Задача 8: BackupVerifyProcess — due-резолв, цепочка, запуск джоба — Ф3 (ядро тика)

Тиковая машина: guard'ы (клэйм, Enabled, Active), супервиз-приоритет живого джоба, выбор due-кандидата (PENDING раньше периодики, по одному за тик на шард), permanent-гварды (`wal_start_segment`, пустой набор), цепочечная проверка list-ами + GET history, запуск verify-джоба на docker-хосте источника. Итоги exited-джоба — задача 9; здесь запуск и цепочечный вердикт (spec §3.1, §3.3).

**Files:**
- Create: `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`
- Create (тестовая инфраструктура): `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` (реальный etcd `EtcdCollection` + локальные фейк-движок/драйвер + расширенный `FakeBackupS3`)

**Interfaces:**
- Consumes: `BackupNames.Verify*` (з.5), `WalChain.CheckRange`+`WalHistory` (з.3–4), `IBackupS3.ListAsync/GetObjectAsync` (з.6), `VerifyJobSpec` (з.5), `BackupStatusJson.Serialize`, `FullBackupState/BackupVerify/BackupPolicy` (з.2), `ShardEndpoints.ReadPortAllocAsync`, `ClaimStore.IsMine`, `WorkJournal.WritePhaseAsync`, `IClusterDriver.EngineFor`.
- Produces (для задач 9–10):
  ```csharp
  public sealed class BackupVerifyProcess(
      IEtcdGateway etcd,
      string[] endpoints,
      IClusterDriver driver,
      ShardEndpoints shardEndpoints,
      IBackupS3 s3,
      ClaimStore claims,
      WorkJournal journal,
      BackupsRuntimeOptions options,
      TimeProvider time,
      ILogger<BackupVerifyProcess> logger,
      Action<string, string, string>? verifyObserver = null)  // (cluster, shard, result: ok|failed|transient)
  {
      private const string Op = "backup-verify";
      public Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct);
  }
  ```
  Журнальные фазы (op=`backup-verify`): `started/<X>/<id>`, `verified-ok/<X>/<id>`, `verify-failed/<X>/<id>`, `engine-fallback/<X>` (выбор fallback docker-хоста), `shard-error` (ошибка шарда не роняет остальные) (+ `download-retry/<X>/<id>` — з.9).

- [ ] **Шаг 1: тестовая инфраструктура (фейк-движок/драйвер) и падающие тесты happy-path**

  - Вход: задачи 2–7 закоммичены; `EtcdFixture`/`EtcdCollection`, `WalStreamProcessTests` — образец сида (portalloc, клэйм, снапшот).
  - Действие: создать `BackupVerifyProcessTests.cs`. Инфраструктура целиком:
    ```csharp
    using System.Text.Json;
    using FluentAssertions;
    using PgWorker.Backups;
    using PgWorker.Backups.Job;
    using PgWorker.Core.Model;
    using PgWorker.Docker.Drivers;
    using PgWorker.Docker.Engine;
    using PgWorker.Etcd.Client;
    using PgWorker.Etcd.Coordination;
    using PgWorker.Etcd.Parsing;
    using PgWorker.IntegrationTests.Etcd;
    using PgWorker.Core.Templates;
    using PgWorker.Provisioning.Endpoints;
    using PgWorker.Provisioning.Probes;
    using Xunit;

    namespace PgWorker.IntegrationTests.Backups;

    // Интеграции BackupVerifyProcess (t04 spec Ф3): реальный etcd (статусы/журнал)
    // + фейки docker-движка и S3 (паттерны FakeBackupDeps/FakeBackupEngine).
    [Collection(EtcdCollection.Name)]
    public class BackupVerifyProcessTests(EtcdFixture fixture)
    {
        private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

        // ── Фейк docker-движка (по образцу BackupProcessTests.FakeBackupEngine;
        //    тест управляет State/ExitCode/Logs — супервиз-ветки задачи 9) ──
        internal sealed class FakeVerifyEngine : IDockerEngine
        {
            internal sealed record ContainerRec(string Id, string State, int ExitCode, string Logs);

            public readonly Dictionary<string, ContainerRec> Containers = [];
            public readonly List<(string Name, ContainerSpec Spec)> Created = [];
            public readonly List<string> Started = [];
            public readonly List<string> Removed = [];
            public readonly List<string> RemovedVolumes = [];
            public bool ListFails { get; set; }
            public bool LogsFails { get; set; }
            public bool InspectFails { get; set; }

            public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

            public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
                string namePrefix, bool all, CancellationToken ct)
            {
                if (ListFails)
                    return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Failed(
                        new ApplicationException("docker: list failed")));
                var list = Containers
                    .Where(p => p.Key.Contains(namePrefix, StringComparison.Ordinal))
                    .Select(p => new DockerContainer(p.Value.Id, [p.Key], p.Value.State, "img"))
                    .ToList();
                return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                    (IReadOnlyList<DockerContainer>)list));
            }

            public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
            {
                if (InspectFails)
                    return Task.FromResult(Result<DockerContainerInspect>.Failed(
                        new ApplicationException("docker: inspect failed")));
                var found = Containers.FirstOrDefault(p => p.Value.Id == id || p.Key == id);
                return Task.FromResult(found.Key is null
                    ? Result<DockerContainerInspect>.Failed(new KeyNotFoundException(id))
                    : Result<DockerContainerInspect>.Success(new DockerContainerInspect(
                        found.Value.Id, found.Key, [], [], [],
                        found.Value.State == "running", found.Value.ExitCode)));
            }

            public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
            {
                if (LogsFails)
                    return Task.FromResult(Result<string>.Failed(new ApplicationException("docker: logs failed")));
                return Task.FromResult(Containers.TryGetValue(idOrName, out var rec)
                    ? Result<string>.Success(rec.Logs)
                    : Result<string>.Failed(new KeyNotFoundException(idOrName)));
            }

            public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
            {
                Created.Add((name, spec));
                Containers[name] = new ContainerRec(Guid.NewGuid().ToString("N"), "created", -1, "");
                return Task.FromResult(Result.Success());
            }

            public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
            {
                Started.Add(idOrName);
                if (Containers.TryGetValue(idOrName, out var rec))
                    Containers[idOrName] = rec with { State = "running" };
                return Task.FromResult(Result.Success());
            }

            public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
                => Task.FromResult(Result.Success());

            public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
            {
                Removed.Add(idOrName);
                Containers.Remove(idOrName);
                return Task.FromResult(Result.Success());
            }

            public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
            {
                RemovedVolumes.Add(name);
                return Task.FromResult(Result.Success());
            }

            // Не используется в тестах verify — заглушки по образцу FakeBackupEngine.
            public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
                => Task.FromResult(Result<string>.Success(string.Empty));
            public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
            public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
                => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));
            public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => Task.FromResult(Result.Success());
            public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
            public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
                => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
            public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
                => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));
            public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
                => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(
                    (IReadOnlySet<(string, int)>)new HashSet<(string, int)>()));
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }

        // Драйвер с единственным хостом h1 (portalloc-сид даёт shard1/shard1a → h1);
        // GetHostsAsync — «таблица Docker:Hosts» (fallback EngineForShard, spec §3.2).
        internal sealed class FakeVerifyDriver(FakeVerifyEngine engine) : IClusterDriver
        {
            public bool SupportsRunningInspection => true;
            public IDockerEngine? EngineFor(string host) => engine;
            public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
                => Task.FromResult(Result.Success());
            public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
                => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(
                    (IReadOnlyList<HostInfo>)[new HostInfo("h1", 0)]));
            public Task<Result> EnsureBackupAgentAsync(
                string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
                => Task.FromResult(Result.Success());
            public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
                => Task.FromResult(Result.Success());
            public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string cluster, CancellationToken ct)
                => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success([]));

            private static NotSupportedException NotSupported() => new("не используется в тестах verify");
            public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
            public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
                InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct) => throw NotSupported();
            public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
            public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
            public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct) => throw NotSupported();
            public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
                string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct) => throw NotSupported();
            public Task<Result<string>> ExecNodeAsync(string cluster, string shard, string node,
                IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
            public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
            public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct) => throw NotSupported();
        }

        // ── Сид-хелперы ──
        private static ClusterSnapshot BuildSnap(string cluster) => new(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
            [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
                [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
            []);

        private async Task SeedAsync(string cluster)
        {
            var ct = TestContext.Current.CancellationToken;
            await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
            await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
                Portalloc.Serialize(new Dictionary<string, NodeAddress>
                {
                    ["shard1/shard1a"] = new("h1", new NodePorts(16001, 18001, 17001)),
                }), null, ct);
            (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        }

        private BackupVerifyProcess BuildProcess(
            string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null)
            => new(
                fixture.Gateway, [fixture.Endpoint], driver,
                new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
                s3, _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
                options ?? new BackupsRuntimeOptions(
                    Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                    S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
                    StagingDir: "/backup-staging"),
                TimeProvider.System,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance);

        private static IReadOnlyList<ClusterBackups> BackupsOf(string cluster, params FullBackupState[] fulls)
            => [new ClusterBackups(cluster, null,
                new Dictionary<string, ShardBackups> { ["shard1"] = new(fulls, null) })];

        private static FullBackupState Completed(string id, string walStart, BackupVerify? verify = null) =>
            new(id, FullBackupStatus.Completed, "shard1a", BackupSourceRole.Replica,
                1757500000, 1757500300, walStart, 1024, null, verify);

        private async Task<JsonElement> ReadVerifyAsync(string cluster, string id)
        {
            var kv = await fixture.Gateway.GetAsync(
                fixture.Endpoint, $"/pgworker/backups/{cluster}/shard1/full/{id}",
                TestContext.Current.CancellationToken);
            kv.Value.Should().NotBeNull("итог verify пишется в ключ полного");
            return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!["verify"];
        }
    ```
    Тест 1 (on_create → джоб запущен):
    ```csharp
    // AAA: PENDING-кандидат при on_create — цепочка цела → verify-джоб создан и
    // запущен (AC1-юнитная часть; итог exited — задача 9)
    [Fact]
    public async Task Тик_PendingКандидат_ЦепочкаЦела_ЗапускаетДжоб()
    {
        // Arrange — full COMPLETED verify=PENDING; wal/: сегменты 1..3 + history нет;
        // pg_wal набора: сегмент 3 (end)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc1");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000002"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc1", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var process = BuildProcess("vc1", driver, s3);
        var backups = BackupsOf("vc1", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc1"), backups, ct);

        // Assert — контейнер pgw-backup-verify-vc1-shard1-20260911120000Z создан+запущен
        result.IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc1-shard1-20260911120000Z");
        engine.Started.Should().Contain("pgw-backup-verify-vc1-shard1-20260911120000Z");
        engine.Created.Single().Spec.Cmd.Should().BeEquivalentTo(VerifyJobCommand.Build(), o => o.WithStrictOrdering());
    }
    ```
    Тест 2 (дыра цепочки → FAILED без джоба, AC3-интеграция):
    ```csharp
    // AAA: дыра в wal/ внутри [start..end] → permanent FAILED с границами,
    // verify-джоб НЕ создаётся (AC3: контейнер не создаётся)
    [Fact]
    public async Task Тик_ДыраЦепочки_FAILED_безДжоба()
    {
        // Arrange — wal/: 1,3 (нет 2); набор: pg_wal/3 → end=3
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc2");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc2", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var process = BuildProcess("vc2", driver, s3);
        var backups = BackupsOf("vc2", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — verify.state=FAILED + error с границами + checked_unix; джоба нет
        var verify = await ReadVerifyAsync("vc2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("000000010000000000000002");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        engine.Created.Should().BeEmpty("вердикт определён цепочкой — скачивание не тратим (spec §3.1)");
    }
    ```
    Тест 3 (нет wal_start_segment → permanent FAILED без джоба, §3.7):
    ```csharp
    // AAA: COMPLETED без wal_start_segment (аномалия) — permanent FAILED без джоба
    [Fact]
    public async Task Тик_НетWalStart_FAILED_безДжоба()
    {
        // Arrange — wal_start_segment = null (ранний упавший UPLOADING не бывает
        // COMPLETED — аномалия; но guard обязателен, spec §3.7)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc3");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var process = BuildProcess("vc3", driver, new FakeBackupS3());
        var backups = BackupsOf("vc3", Completed("20260911120000Z", null,
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc3", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("wal_start_segment");
        engine.Created.Should().BeEmpty();
    }
    ```
    Тесты 4–5 (строгий TLI через GET history — AC4-интеграция):
    ```csharp
    // AAA: строгий TLI-переход с валидным switchWALLSN (GET history из S3) —
    // цепочка цела, джоб запускается (AC4: verify использует содержимое history)
    [Fact]
    public async Task Тик_СтрогийTLI_ВалиднаяТочка_ЗапускаетДжоб()
    {
        // Arrange — wal/: tli1/seg1, history, tli2/seg2; набор: pg_wal/tli2/seg2
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc4");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc4", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc4", "shard1", "00000002.history"));
        s3.Objects.Add(("vc4", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc4", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc4", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        var process = BuildProcess("vc4", driver, s3);
        var backups = BackupsOf("vc4", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc4"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc4-shard1-20260911120000Z");
    }

    // AAA: строгий TLI-переход с НЕвалидной точкой (switchWALLSN вне границы) —
    // FAILED с упоминанием history, без джоба (AC4)
    [Fact]
    public async Task Тик_СтрогийTLI_ТочкаВнеГраниц_FAILED()
    {
        // Arrange — как vc4, но history говорит parent=1, lsn=0/5000000 (сегмент 5)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc5");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc5", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc5", "shard1", "00000002.history"));
        s3.Objects.Add(("vc5", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc5", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc5", "shard1", "wal/00000002.history")] = "1\t0/5000000\n";
        var process = BuildProcess("vc5", driver, s3);
        var backups = BackupsOf("vc5", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc5", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("00000002.history");
        engine.Created.Should().BeEmpty();
    }
    ```
    Тест 6 (transient list/GET — AC2-сторона S3; использует `FakeBackupS3.Fails` из задачи 6):
    ```csharp
    // AAA: transient S3 (list wal/ недоступен) — шард-skip тиком: тик успешен,
    // джоб не создаётся, ключ полного НЕ изменён (spec §3.1: статус не трогаем)
    [Fact]
    public async Task Тик_TransientS3_СтатусНеТрогаем_ДжобаНет()
    {
        // Arrange — PENDING-ключ в etcd (записан руками — ассерт «не изменён»);
        // S3 «лежит» (FakeBackupS3.Fails)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc6");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3 { Fails = true };
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = $"/pgworker/backups/vc6/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc6", driver, s3);
        var backups = BackupsOf("vc6", candidate);

        // Act
        var result = await process.TickAsync(BuildSnap("vc6"), backups, ct);

        // Assert — transient: тик без ошибки (шард-skip), ничего не создано/не записано
        result.IsSuccess.Should().BeTrue("transient S3 — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("джоб без цепочки не стартует");
        var after = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "статус PENDING не трогаем — ретрай следующим тиком");
    }
    ```
    Тест 7 (transient GET history — AC2-сторона GET; листинг `wal/` ок, падает только GET):
    ```csharp
    // AAA: transient GET history (spec §3.1): цепочка с TLI-переходом требует
    // скачивания .history → GET падает → шард-skip, джоба нет, статус не тронут
    [Fact]
    public async Task Тик_TransientGetHistory_СтатусНеТрогаем_ДжобаНет()
    {
        // Arrange — сид как в vc4 (TLI-переход в диапазоне), но GET падает;
        // PENDING-ключ в etcd для ассерта «не изменён»
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc7");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3 { FailsGetObject = true };
        s3.Objects.Add(("vc7", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc7", "shard1", "00000002.history"));
        s3.Objects.Add(("vc7", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc7", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc7", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/vc7/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc7", driver, s3);

        // Act
        var result = await process.TickAsync(BuildSnap("vc7"), BackupsOf("vc7", candidate), ct);

        // Assert — list прошёл (переход в диапазоне найден), GET упал → transient
        result.IsSuccess.Should().BeTrue("transient GET — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("без содержимого history строгий разбор невозможен — джоб не стартует");
        var after = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "статус PENDING не трогаем — ретрай следующим тиком");
    }
    ```
    Тест 8 (fallback docker-хоста — spec §3.2, journal-факт):
    ```csharp
    // AAA: нода-источник исчезла из portalloc → джоб стартует на ПЕРВОМ хосте
    // таблицы Docker:Hosts (GetHostsAsync) + journal-факт engine-fallback (spec §3.2)
    [Fact]
    public async Task Тик_УзелИсчезИзPortalloc_ДжобНаПервомХостеТаблицы()
    {
        // Arrange — portalloc ПУСТ (узел shard1a исчез); PENDING-кандидат с node=shard1a
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc8");
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/portalloc/vc8",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>()), null, ct);
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine); // GetHostsAsync → [h1]
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("vc8", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var process = BuildProcess("vc8", driver, s3);
        var backups = BackupsOf("vc8", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc8"), backups, ct);

        // Assert — джоб создан fallback-движком; выбор хоста зафиксирован журналом
        result.IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc8-shard1-20260911120000Z");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/vc8", ct);
        journal.Value!.Value.Should().Contain("engine-fallback/shard1");
    }
    ```
  - Выход: 8 тестов падает (`BackupVerifyProcess` не существует).
  - Проверка: сборка IntegrationTests — CS0246 `BackupVerifyProcess`.
  - Spec: §3.1 (порядок проверок, дыра → FAILED без джоба; transient list/GET → шард-skip), §3.2 (fallback docker-хоста: первый engine таблицы Docker:Hosts + journal-факт), §3.3 (машина тика), §3.7 (нет wal_start_segment), AC2 (list/GET-сторона), AC3, AC4.

- [ ] **Шаг 2: реализовать BackupVerifyProcess (ядро: guard'ы + due + цепочка + запуск)**

  - Вход: шаг 1.
  - Действие: создать `BackupVerifyProcess.cs`. Структура (полный код — исполнитель пишет по этой схеме, все ветки обязательны):
    ```csharp
    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster)) return Failed("клэйм не наш");
        // Guard 2: Backups:Enabled=false → no-op (идущие ephemeral-джобы не убиваем).
        if (!options.Enabled) return Done;
        // Guard 3: только Active-кластер.
        if (snap.Config.State != ClusterState.Active) return Done;

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster) ?? пустой;
        var intervalSec = mine.Policy?.VerifyIntervalSec ?? options.VerifyIntervalSec;
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);  // fail → Failed

        foreach (var shard in snap.Shards.Where(s => !s.ToRemove))
        {
            // шард без ключей полных — пропускается (dsn не нужен: verify чисто по S3)
            if (!mine.Shards.TryGetValue(shard.Name, out var shardBackups) || shardBackups.Full.Count == 0)
                continue;
            try { await TickShardAsync(cluster, shard.Name, shardBackups, addresses.Value, intervalSec, nowUnix, ct); }
            catch (Exception ex) { logger.LogError(...); journal "shard-error"; } // ошибка шарда не роняет остальные
        }
        return Done;
    }

    private async Task TickShardAsync(...)
    {
        // (1) Супервиз живого verify-контейнера шарда: list по префиксу
        //     BackupNames.VerifyContainerName(cluster, shard, "") — точнее префиксу
        //     $"pgw-backup-verify-{cluster}-{shard}-"; найден → SuperviseAsync (задача 9
        //     добавит; в этой задаче — заглушка: found != null → return, новых не стартуем).
        // (2) due-резолв (PENDING-очередь раньше периодики; по одному за тик):
        //     pending = fulls.Where(COMPLETED && Verify is {State: Pending}).OrderBy(StartedUnix)
        //     periodic = intervalSec > 0
        //         ? fulls.Where(COMPLETED && (Verify is null ||
        //              Verify is {State: Ok, CheckedUnix: var c} && nowUnix - (c ?? 0) > intervalSec))
        //              .OrderBy(f => f.Verify?.CheckedUnix ?? 0)
        //         : []
        //     candidate = pending.FirstOrDefault() ?? periodic.FirstOrDefault();
        //     verify FAILED — терминален: не попадает ни в один список.
        // (3) wal_start_segment == null → permanent FAILED("нет wal_start_segment") + checked_unix,
        //     put полной перезаписью BackupStatusJson.Serialize, journal "verify-failed",
        //     observer "failed"; return.
        // (4) Цепочка:
        //     walList = s3.ListAsync(cluster, shard, "wal/") — fail → observer "transient"; return (шард-skip, статус не трогаем);
        //     setList = s3.ListAsync(cluster, shard, $"full/{id}/pg_wal/") — fail → transient; return;
        //     end = max(Tli,Log,Seg) сегментов setList (TryParse; .partial/.history — мимо);
        //          пусто → permanent FAILED("в наборе нет WAL-сегментов pg_wal") как (3); return;
        //     historyContents: для каждого имени walList с TryParseHistory(name) is { } tli
        //          && tli > start.Tli && tli <= end.Tli (переходы диапазона):
        //          GetObjectAsync(cluster, shard, $"wal/{name}") — fail → transient; return;
        //          dict[tli] = content;
        //     check = WalChain.CheckRange(start, end, walList.Select(o => o.Name), historyContents);
        //     !check.IsContinuous → permanent FAILED(check.GapError) как (3); return.
        // (5) Джоб: engine = await EngineForShardAsync(cluster, shard, addresses, $"{shard}/{candidate.Node}", ct);
        //          null → transient (return; следующий тик);
        //     spec = VerifyJobSpec.Build(options, cluster, shard, id);
        //     create fail → transient return; start fail → transient return
        //     (PENDING остаётся — идемпотентный перезапуск следующим тиком);
        //     journal "started/<X>/<id>"; logger.LogInformation.
    }

    // Хост джоба (spec §3.2): хост ноды-источника полного (node-факт) из
    // portalloc; узел исчез из portalloc → ПЕРВЫЙ хост таблицы Docker:Hosts
    // (driver.GetHostsAsync — plain-драйвер отдаёт конфиг-таблицу) → EngineFor;
    // выбор fallback-хоста — journal-факт (phase "engine-fallback/<X>").
    private async Task<IDockerEngine?> EngineForShardAsync(
        string cluster, string shard,
        IReadOnlyDictionary<string, NodeAddress> addresses, string nodeKey, CancellationToken ct)
    {
        if (addresses.TryGetValue(nodeKey, out var addr))
            return driver.EngineFor(addr.Host);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        if (first is null)
            return null;
        var engine = driver.EngineFor(first.Host);
        if (engine is not null)
            await journal.WritePhaseAsync(cluster, Op, $"engine-fallback/{shard}",
                claims.InstanceId, first.Host, ct);
        return engine;
    }

    // Failover-put как BackupProcess.PutAsync (первый успешный endpoint).
    ```
  - Выход: тиковая машина запускает verify-джобы и пишет цепочечные вердикты.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupVerifyProcessTests"` — 8 новых PASS; затем зачистка: `docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f`.
  - Spec: §3.1 (цепочка; transient list/GET → шард-skip, статус не трогаем), §3.2 (docker-хост джоба: node-факт portalloc; узел исчез → первый engine таблицы Docker:Hosts, journal-факт выбора), §3.3 (guard'ы, due-порядок, transient/permanent, максимум один джоб на шард), AC2 (S3-сторона), AC3, AC4.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный + зачистка.
  - Действие: `git add src/PgWorker.Backups/Process/BackupVerifyProcess.cs src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): BackupVerifyProcess — due-резолв, цепочечная проверка, запуск verify-джоба (Ф3)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф3, §3.1/§3.3.

---

### Задача 9: BackupVerifyProcess — супервиз, transient/permanent, периодика, инвариант — Ф3 (продолжение)

Супервиз exited-джоба (exit-код + result-JSON: `phase=verify` → permanent FAILED, `phase=download` → transient), vanished-джоб → перезапуск, периодическая перепроверка по `interval_sec` (`checked_unix` растёт; `interval_sec<=0` — только on_create; FAILED не перепроверяется), инвариант одного джоба на шард (spec §3.1–§3.3).

**Files:**
- Create: `src/PgWorker.Backups/Job/VerifyLog.cs`
- Modify: `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`

**Interfaces:**
- Consumes: протокол result-JSON t02 (однострочные JSON в stdout — образец `BackupJobLog.Parse`), `FakeVerifyEngine` (з.8; тест управляет `State/ExitCode/Logs`).
- Produces: `VerifyLog.Parse(string logs)` → `VerifyJobResult?` (`record VerifyJobResult(bool Ok, string? Phase, string? Error)`); завершённая машина (для задачи 10 wiring); фазы журнала `verified-ok`/`verify-failed`/`download-retry`.

- [ ] **Шаг 1: падающие тесты супервиза**

  - Вход: задача 8 закоммичена (хелперы `SeedAsync`/`BuildProcess`/`BackupsOf`/`Completed`/`ReadVerifyAsync`, `FakeVerifyEngine` с `State/ExitCode/Logs` и флагами `ListFails/LogsFails/InspectFails` уже есть).
  - Действие: добавить в `BackupVerifyProcessTests.cs` (общий паттерн: сид wal/+набора как в задаче 8; первый тик запускает джоб; движок переводится в exited; второй тик — супервиз):
    ```csharp
    // Локальный хелпер: PENDING-кандидат + непрерывная цепочка 1..3 + джоб запущен первым тиком.
    private async Task<FakeVerifyEngine> StartJobAsync(
        string cluster, FakeBackupS3 s3, string id = "20260911120000Z")
    {
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000002"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add((cluster, "shard1", $"full/{id}/pg_wal/000000010000000000000003"));
        var process = BuildProcess(cluster, driver, s3);
        var backups = BackupsOf(cluster, Completed(id, "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        (await process.TickAsync(BuildSnap(cluster), backups, TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        engine.Created.Should().NotBeEmpty("джоб запущен первым тиком (предусловие)");
        return engine;
    }

    // AAA: exited exit=0 + ok:true → verify OK + checked_unix; контейнер и volume снесены (AC1)
    [Fact]
    public async Task Супервиз_Exit0_Ok_чисткаДжоба()
    {
        // Arrange — джоб exited с ok:true
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv1");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv1", s3);
        var name = "pgw-backup-verify-sv1-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };

        // Act — второй тик (супервиз итога)
        var process = BuildProcess("sv1", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv1"), BackupsOf("sv1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("sv1", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(name); // volume имя == имени контейнера
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/sv1", ct);
        journal.Value!.Value.Should().Contain("verified-ok/shard1/20260911120000Z");
    }

    // AAA: exited + phase=verify → permanent FAILED с error от pg_verifybackup (AC2)
    [Fact]
    public async Task Супервиз_VerifyPhaseFailed_FAILED()
    {
        // Arrange — джоб exited с result-JSON phase=verify
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv2");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv2", s3);
        var name = "pgw-backup-verify-sv2-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 1,
            Logs = "{\"ok\":false,\"phase\":\"verify\",\"error\":\"checksum mismatch failed\"}" };

        // Act
        var process = BuildProcess("sv2", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv2"), BackupsOf("sv2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("sv2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("checksum mismatch");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
    }

    // AAA: exited + phase=download → transient: контейнер снесён, статус ОСТАЛСЯ PENDING (AC2)
    [Fact]
    public async Task Супервиз_DownloadPhase_Transient_ОстаетсяPending()
    {
        // Arrange — джоб exited с result-JSON phase=download (mc/staging ENOSPC);
        // ключ кандидата в etcd записан руками (transient итог НЕ пишет — паттерн vc6/vc7)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv3");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv3", s3);
        var name = "pgw-backup-verify-sv3-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 1,
            Logs = "{\"ok\":false,\"phase\":\"download\",\"error\":\"mc cp failed\"}" };
        var key = "/pgworker/backups/sv3/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, before, null, ct);

        // Act — супервиз
        var process = BuildProcess("sv3", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — статус НЕ изменён (остался PENDING), контейнер/volume снесены
        var after = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "download-phase transient: статус PENDING не трогаем");
        engine.Removed.Should().Contain(name);

        // Act 2 — следующий тик: PENDING снова due → джоб перезапущен (ретрай тиками)
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert 2
        engine.Created.Should().HaveCount(2, "transient ретраится следующим тиком (spec §3.1)");
    }

    // AAA: vanished-джоб (контейнера нет при PENDING) → перезапуск со шага цепочки (AC8)
    [Fact]
    public async Task Супервиз_Vanished_Перезапуск()
    {
        // Arrange — PENDING-кандидат, движок ПУСТ (контейнер исчез после рестарта docker-хоста)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv4");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv4", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv4", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv4", new FakeVerifyDriver(engine), s3);

        // Act — тик без контейнера
        (await process.TickAsync(BuildSnap("sv4"), BackupsOf("sv4",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — идемпотентный запуск заново (цепочка → create+start), не FAILED
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-sv4-shard1-20260911120000Z");
    }

    // AAA: два due-кандидата — по одному за тик; PENDING раньше периодики (AC8)
    [Fact]
    public async Task ДваDue_ПоОдномуЗаТик_PendingРаньше()
    {
        // Arrange — full A (старее): verify=OK, CheckedUnix=now-7200 (периодика due при
        // interval 3600); full B (свежее): verify=PENDING (on_create)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv5");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv5", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv5", new FakeVerifyDriver(engine), s3);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var backups = new ClusterBackups("sv5",
            new BackupPolicy(7, 4, 6, 86400, true, VerifyIntervalSec: 3600),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(
                [
                    Completed("20260911090000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Ok, now - 7200)),
                    Completed("20260911120000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Pending, null)),
                ], null),
            });

        // Act — тик 1: только B (PENDING-очередь раньше периодики)
        (await process.TickAsync(BuildSnap("sv5"), [backups], ct)).IsSuccess.Should().BeTrue();

        // Assert — один джоб, и это B
        engine.Created.Should().ContainSingle()
            .Which.Name.Should().Be("pgw-backup-verify-sv5-shard1-20260911120000Z");
    }

    // AAA: периодика (AC5): OK-полный перепроверяется по interval_sec — цикл
    // ДОКАНЦА: exited ok → checked_unix РАСТЁТ; interval_sec<=0 — только
    // on_create; verify=null («никогда не проверялся») — периодика due;
    // FAILED не перепроверяется (терминален)
    [Fact]
    public async Task Периодика_ПоInterval_Отключение_и_ТерминальностьFAILED()
    {
        // Arrange — OK-полный CheckedUnix=now-7200; policy interval_sec=3600 → due
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv6");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv6", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv6", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var okBackup = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Ok, now - 7200));
        ClusterBackups WithInterval(long? interval, FullBackupState? full = null) => new("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, interval),
            new Dictionary<string, ShardBackups> { ["shard1"] = new([full ?? okBackup], null) });

        // Act 1 — interval=3600: джоб запущен (перепроверка due)
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv6", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle("OK-полный старше interval — перепроверка (AC5)");

        // Act 1b — джоб завершился ok:true → супервиз пишет итог
        var name = "pgw-backup-verify-sv6-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();

        // Assert 1b — verify OK и checked_unix РАСТЁТ (было now-7200, стало ~now)
        var verify = await ReadVerifyAsync("sv6", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(now - 7200,
            "периодическая перепроверка обновляет checked_unix (AC5)");

        // Act 2 / Assert 2 — interval=0: не due (только on_create)
        var engineOff = new FakeVerifyEngine();
        var processOff = BuildProcess("sv6", new FakeVerifyDriver(engineOff), s3);
        (await processOff.TickAsync(BuildSnap("sv6"), [WithInterval(0)], ct)).IsSuccess.Should().BeTrue();
        engineOff.Created.Should().BeEmpty("interval_sec<=0 — периодика выключена (AC5)");

        // Act 2b / Assert 2b — verify=null («никогда не проверялся», spec §3.1
        // due-periodic): ловится ТОЛЬКО периодикой (не on_create — verify нет)
        var engineNull = new FakeVerifyEngine();
        var processNull = BuildProcess("sv6", new FakeVerifyDriver(engineNull), s3);
        var neverVerified = Completed("20260911120000Z", "000000010000000000000001", verify: null);
        (await processNull.TickAsync(BuildSnap("sv6"), [WithInterval(3600, neverVerified)], ct))
            .IsSuccess.Should().BeTrue();
        engineNull.Created.Should().ContainSingle("непроверенный полный (verify=null) due по периодике (§3.1)");

        // Act 3 / Assert 3 — FAILED не перепроверивается ни при каком interval
        var failed = new ClusterBackups("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, 1),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Failed, now, "bad"))], null),
            });
        var engineFailed = new FakeVerifyEngine();
        var processFailed = BuildProcess("sv6", new FakeVerifyDriver(engineFailed), s3);
        (await processFailed.TickAsync(BuildSnap("sv6"), [failed], ct)).IsSuccess.Should().BeTrue();
        engineFailed.Created.Should().BeEmpty("FAILED терминален (spec §3.1)");
    }

    // AAA: транспорт-отказ docker (list) — статус не меняем (transient, spec §3.2)
    [Fact]
    public async Task Супервиз_TransportОтказ_СтатусНеТрогаем()
    {
        // Arrange — джоб exited с ok:true, но list падает; ключ кандидата в etcd
        // записан руками (никто другой его не пишет до итога — паттерн vc6/vc7)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv7");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv7", s3);
        var name = "pgw-backup-verify-sv7-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        engine.ListFails = true;
        var key = "/pgworker/backups/sv7/shard1/full/20260911120000Z";
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, BackupStatusJson.Serialize(
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), null, ct);
        var before = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;

        // Act
        var process = BuildProcess("sv7", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv7"), BackupsOf("sv7",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ не изменён, контейнер не тронут (следующий тик повторит супервиз)
        var after = (await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/sv7/shard1/full/20260911120000Z", ct)).Value!.Value;
        after.Should().Be(before);
        engine.Removed.Should().NotContain(name);
    }

    // AAA: инвариант «максимум один verify-джоб на шард» (AC8): running-джоб
    // кандидата A жив + кандидат B due (PENDING) → новый джоб НЕ стартуется,
    // статус B не тронут (spec §3.3 п.1)
    [Fact]
    public async Task ЖивойДжобШарда_БлокируетНовый_СтатусBTакойЖе()
    {
        // Arrange — в движке уже running-контейнер джоба кандидата A; B — PENDING;
        // ключи обоих кандидатов записаны в etcd руками (ассерт «не тронут»)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sv8");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv8", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var nameA = "pgw-backup-verify-sv8-shard1-20260911090000Z";
        engine.Containers[nameA] = new(
            Guid.NewGuid().ToString("N"), "running", -1, ""); // ContainerRec: Id, State, ExitCode, Logs
        var a = Completed("20260911090000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var b = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var keyB = "/pgworker/backups/sv8/shard1/full/20260911120000Z";
        var beforeB = BackupStatusJson.Serialize(b);
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            "/pgworker/backups/sv8/shard1/full/20260911090000Z", BackupStatusJson.Serialize(a), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, keyB, beforeB, null, ct);
        var process = BuildProcess("sv8", new FakeVerifyDriver(engine), s3);

        // Act — тик: супервиз видит живой джоб A (running → ждать), B due
        (await process.TickAsync(BuildSnap("sv8"), BackupsOf("sv8", a, b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — новый джоб не создан; ключ B не изменён
        engine.Created.Should().BeEmpty("живой verify-джоб шарда блокирует запуск нового (инвариант §3.3)");
        var afterB = (await fixture.Gateway.GetAsync(fixture.Endpoint, keyB, ct)).Value!.Value;
        afterB.Should().Be(beforeB, "статус due-кандидата не трогаем, пока жив чужой джоб шарда");
    }
    ```
  - Выход: 8 тестов падают (супервиз — заглушка задачи 8).
  - Проверка: прогон фильтром `BackupVerifyProcessTests` — новые FAIL (супервиз-ветки).
  - Spec: §3.1 (exit 0 → OK+checked_unix; verify-phase → FAILED; download-phase → transient), §3.2 (супервиз: running → ждать; exited → итог; нет контейнера при PENDING → перезапуск; после итога — снос), §3.3 п.1 (живой verify-контейнер шарда → супервиз, новых не стартуем — инвариант одного джоба; по одному за тик; PENDING раньше периодики), AC1, AC2, AC5, AC8.

- [ ] **Шаг 2: реализовать супервиз в BackupVerifyProcess**

  - Вход: шаг 1.
  - Действие: сначала создать тонкий парсер протокола verify-джоба `src/PgWorker.Backups/Job/VerifyLog.cs` (по образцу `BackupJobLog.Parse` — те же JSON-строки stdout, но поля `ok/phase/error`):
    ```csharp
    using System.Text.Json;

    namespace PgWorker.Backups.Job;

    /// <summary>Result-JSON verify-джоба: {"ok":true} либо
    /// {"ok":false,"phase":"download|verify","error":…} (arch/19 §5, t04).</summary>
    public sealed record VerifyJobResult(bool Ok, string? Phase, string? Error);

    // Парсер stdout verify-джоба: последняя своя JSON-строка; чужие строки игнорит
    // (паттерн BackupJobLog).
    public static class VerifyLog
    {
        public static VerifyJobResult? Parse(string logs)
        {
            VerifyJobResult? result = null;
            foreach (var line in logs.Split('\n'))
            {
                var trimmed = line.Trim();
                if (!trimmed.StartsWith('{') || !trimmed.EndsWith('}'))
                    continue;
                try
                {
                    using var doc = JsonDocument.Parse(trimmed);
                    var root = doc.RootElement;
                    if (root.ValueKind != JsonValueKind.Object)
                        continue;
                    if (root.TryGetProperty("ok", out var okEl) && okEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                        result = new VerifyJobResult(
                            okEl.ValueKind == JsonValueKind.True,
                            root.TryGetProperty("phase", out var phaseEl) && phaseEl.ValueKind == JsonValueKind.String
                                ? phaseEl.GetString()
                                : null,
                            root.TryGetProperty("error", out var errEl) && errEl.ValueKind == JsonValueKind.String
                                ? errEl.GetString()
                                : null);
                }
                catch (JsonException)
                {
                    // чужая JSON-подобная строка — не наш контракт
                }
            }

            return result;
        }
    }
    ```
    Затем заменить заглушку шага (1) задачи 8 на полный `SuperviseAsync(engine, cluster, shard, containerName, shardBackups)`:
    ```csharp
    // id = containerName[префикс шарда..]; full = shardBackups.Full.SingleOrDefault(Id == id);
    // full == null || full.State != COMPLETED || full.Verify is { State: Failed } →
    //   orphan/stale-контейнер (ключ ушёл: deprovisioning-гонка, внешний FAILED):
    //   RemoveContainer + RemoveVolume, итог НЕ писать, return.
    //   ПРИМЕЧАНИЕ: verify == null | PENDING | OK — ЛЕГИТИМНЫЙ кандидат джоба
    //   (PENDING — on_create; OK — периодика, статус остаётся OK до итога).
    // list (all:true) → transport-fail → return (transient);
    // found == null → return (перезапуск — общий путь due ниже: PENDING снова due);
    // found running/restarting → return (ждём);
    // found created → StartContainerAsync, return;
    // found exited:
    //   logs = GetContainerLogsAsync(name, tail: 200) → fail → return (transient);
    //   result = VerifyLog.Parse(logs);
    //   inspect → fail → return (transient); exitCode = inspect.ExitCode ?? -1;
    //   exitCode == 0 && result is { Ok: true }
    //     → итог OK: Verify = new(Ok, now, Error: null); observer "ok"; journal "verified-ok";
    //   result is { Ok: false, Phase: "verify" or null, Error: { } err }
    //     → permanent FAILED: Verify = new(Failed, now, err); observer "failed"; journal "verify-failed";
    //   иначе (Ok:false с Phase="download" / exit без результата) → transient:
    //     observer "transient"; journal "download-retry"; контейнер+volume снести;
    //     СТАТУС НЕ МЕНЯЕМ (PENDING остаётся — ретрай следующим тиком);
    //   итог (OK/FAILED) → put BackupStatusJson.Serialize (полная перезапись); CleanupJobAsync.
    ```
  - Выход: полная машина супервиза с transient/permanent-семантикой.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupVerifyProcessTests"` — PASS (16: 8 из з.8 + 8 новых); зачистка серий (`docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f`).
  - Spec: §3.2 (супервиз), принцип 4 (transient ≠ permanent), принцип 5 (идемпотентность), AC1/AC2/AC5/AC8.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный + зачистка.
  - Действие: `git add -A src/PgWorker.Backups src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): супервиз verify-джоба — exit-код/фазы, transient/permanent, периодика, vanished-перезапуск (Ф3, AC1/AC2/AC5/AC8)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф3.

---

### Задача 10: Wiring + метрика — Ф3 (интеграция в цикл)

`IClusterProcesses.VerifyBackupsAsync` после `WalStreamAsync`, до `repair`; DI в `Program.cs`; counter `pgworker_backup_verify_total{result}` по образцу t03 (`verifyObserver`-делегат).

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs`
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs`
- Modify: `src/PgWorker.App/Program.cs`
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Test: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs`, `src/tests/Shared.Metrics.UnitTests/` (метрика)

**Interfaces:**
- Consumes: `BackupVerifyProcess` (з.8–9), `WorkerMetricsInstrumentation` (паттерн `BackupWalLag`).
- Produces:
  ```csharp
  // IClusterProcesses
  Task<Result<ProcessOutcome>> VerifyBackupsAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct);
  // WorkerMetricsInstrumentation
  public void BackupVerify(string cluster, string shard, string result); // counter pgworker_backup_verify_total{result=ok|failed|transient}
  ```

- [ ] **Шаг 1: падающие тесты (порядок цикла + метрика)**

  - Вход: задача 9 закоммичена; `ReconcileLoopTests` — образец мока `IClusterProcesses` с записью порядка вызовов.
  - Действие:
    - В `ReconcileLoopTests`: мок получает `VerifyBackupsAsync`; новый тест — при `Backups.Enabled=true` порядок вызовов содержит `["backup-wal", "backup-verify", "repair"]` подряд (verify строго между); тест `Enabled=false` — `backup-verify` не вызывается (как `backups`).
    - В `src/tests/Shared.Metrics.UnitTests/` (по образцу существующих тестов instrumentation через `DebugSnapshot()`): марк `BackupVerify("c1","s1","ok")` трижды + `"failed"` один → внутренний счётчик `_verifyTotal[("ok")]==3`, `[("failed")]==1` (для этого расширить `DebugState` полем `IReadOnlyDictionary<string, long> BackupVerifyTotals` — по образцу `WalLag`).
  - Выход: тесты падают (метода/вызова нет).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~ReconcileLoopTests"` — FAIL на новых; аналогично метрики.
  - Spec: §3.3 (wiring: после `WalStreamAsync`, до `repair`; тик non-blocking), §3.8 (метрика-counter).

- [ ] **Шаг 2: реализация wiring и метрики**

  - Вход: шаг 1.
  - Действие:
    - `WorkerMetricsInstrumentation`: в конструкторе `var backupVerify = meter.CreateCounter<long>("pgworker_backup_verify_total", description: "Результаты verify полных бэкапов (arch/19 §5, t04)")`; поле `private readonly Dictionary<string, long> _backupVerify = new();` метод:
      ```csharp
      /// <summary>Counter pgworker_backup_verify_total{result=ok|failed|transient}: итог
      /// проверки полного (BackupVerifyProcess; кластер/шард — только трассировка вызова).</summary>
      public void BackupVerify(string cluster, string shard, string result)
      {
          try
          {
              lock (_lock)
              {
                  _backupVerify[result] = _backupVerify.TryGetValue(result, out var n) ? n + 1 : 1;
              }
              // марк-делегат counter.Add(1, label result) — как LoopTickMark
          }
          catch { /* пассивный наблюдатель */ }
      }
      ```
      (марк-делегат `BackupVerifyMark` — по образцу `LoopTickMark`; `DebugState` расширить).
    - `ClusterProcesses`: метод интерфейса + реализация `=> verifyProcess.TickAsync(snap, backups, ct);` + DI-параметр `BackupVerifyProcess verifyProcess`.
    - `ReconcileLoop` (после `backup-wal`, до `repair`):
      ```csharp
      // Проверки полных бэкапов (t04, arch/19 §5): после WAL-потока, до repair;
      // тик запускает/поллит verify-джобы, не ждёт их (non-blocking).
      if (options.CurrentValue.Backups.Enabled)
          await RunClusterOpAsync(cluster, "backup-verify",
              () => processes.VerifyBackupsAsync(snap, backups, ct), ct);
      ```
    - `Program.cs`: синглтон `BackupVerifyProcess` (по образцу `BackupProcess`; `verifyObserver`: `sp.GetRequiredService<WorkerMetricsInstrumentation>().BackupVerify`).
  - Выход: процесс в цикле; метрика пишется.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~ReconcileLoopTests"` и `--filter "FullyQualifiedName~Shared.Metrics"` — PASS; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` — 0 warnings.
  - Spec: §3.3 (wiring), §3.8 (наблюдаемость).

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный.
  - Действие: `git add -A src/PgWorker.App src/Shared.Metrics src/tests/PgWorker.UnitTests src/tests/Shared.Metrics.UnitTests && git commit -m "feat(backups): wiring BackupVerifyProcess в reconcile (после wal, до repair) + counter pgworker_backup_verify_total (Ф3)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф3, §3.3, §3.8.

---

### Задача 11: Планировщик t02 — валидность в IsDue/BackoffPassed — Ф4

«Последний валидный» = COMPLETED с `verify == null | PENDING | OK`; проваленный verify не даёт свежести; бэкофф растёт и от verify-фейлов (spec §3.5).

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupPlanner.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs`

**Interfaces:**
- Consumes: `BackupVerifyStatus` (з.2).
- Produces: семантика `IsDue`/`BackoffPassed` на валидных (для AC6 воркерной части; панельный аналог — задача 12).

- [ ] **Шаг 1: падающие юнит-тесты**

  - Вход: задача 2 закоммичена (модель с verify); `BackupPlannerTests.Full`-хелпер уже принимает 10 аргументов.
  - Действие: добавить в `BackupPlannerTests.cs` (расширить хелпер `Full` опциональным `BackupVerify? verify = null`):
    ```csharp
    // AAA: COMPLETED+verify FAILED не даёт свежести → IsDue=true (AC6)
    [Fact]
    public void IsDue_СвежийНоБитыйПолный_Due()
    {
        // Arrange — COMPLETED час назад (в окне), но verify FAILED
        var fulls = new[]
        {
            Full("20260911110000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)),
                Unix(Now.AddHours(-1)), verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now.AddHours(-1)), "bad")),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue("проваленный verify свежестью не считается");
    }

    // AAA: валидный = verify null | PENDING | OK — все три дают свежесть
    [Fact]
    public void IsDue_ВсеВидыВалидных_ГасятDue()
    {
        // Arrange — три конфигурации свежего COMPLETED (час назад, окно 86400)
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var finished = Unix(Now.AddHours(-1));
        var noVerify = new[] { Full("20260911110000Z", FullBackupStatus.Completed, fresh, finished) };
        var pending = new[] { Full("20260911110001Z", FullBackupStatus.Completed, fresh, finished,
            verify: new BackupVerify(BackupVerifyStatus.Pending, null)) };
        var ok = new[] { Full("20260911110002Z", FullBackupStatus.Completed, fresh, finished,
            verify: new BackupVerify(BackupVerifyStatus.Ok, finished)) };

        // Act / Assert — каждая конфигурация сама по себе гасит due (в окне)
        BackupPlanner.IsDue(noVerify, 86400, Unix(Now)).Should().BeFalse();
        BackupPlanner.IsDue(pending, 86400, Unix(Now)).Should().BeFalse();
        BackupPlanner.IsDue(ok, 86400, Unix(Now)).Should().BeFalse();
    }

    // AAA: бэкофф n считает и verify-фейлы: COMPLETED+FAILED-verify после последнего
    // валидного — попытка, окно растёт (AC6)
    [Fact]
    public void BackoffPassed_VerifyФейлУвеличиваетN()
    {
        // Arrange — старый валидный COMPLETED (2 дня назад) + свежий COMPLETED с
        // verify FAILED (100 c назад), Base=300
        var fulls = new[]
        {
            Full("20260909030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))),
            Full("20260911025840Z", FullBackupStatus.Completed, Unix(Now.AddSeconds(-100)), Unix(Now.AddSeconds(-100)),
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now), "bad")),
        };

        // Act / Assert — n=1: окно Base=300 c от последней попытки (verify-фейл считается)
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now).AddSeconds(301)).Should().BeTrue();
    }
    ```
  - Выход: тесты падают на текущей реализации (свежий FAILED-verify гасит due).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupPlannerTests"` — FAIL на новых.
  - Spec: §3.5 (IsDue: max finished_unix среди валидных; BackoffPassed: n = FAILED-джобы + COMPLETED с verify=FAILED), AC6, решение пользователя «влияет ли verify на свежесть».

- [ ] **Шаг 2: реализовать валидность в BackupPlanner**

  - Вход: шаг 1.
  - Действие: в `BackupPlanner.cs`:
    ```csharp
    // Валидный полный (t04, arch/19 §2): COMPLETED и verify ≠ FAILED
    // (null — не проверялся; PENDING — идёт; OK — проверен).
    private static bool IsValid(FullBackupState f)
        => f.State == FullBackupStatus.Completed
           && f.Verify is not { State: BackupVerifyStatus.Failed };
    ```
    `IsDue`: фильтр `fulls.Where(IsValid)`, сортировка по `FinishedUnix ?? StartedUnix` (desc); `BackoffPassed`: `lastValidStarted` — по валидным; `failures = tail.Count(f => f.State == FullBackupStatus.Failed || f.Verify is { State: BackupVerifyStatus.Failed })`.
  - Выход: планировщик оперирует валидными; старые тесты (без verify) остаются зелёными (verify=null валиден).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupPlannerTests"` — PASS (старые + новые).
  - Spec: §3.5, AC6.

- [ ] **Шаг 3: коммит**

  - Вход: шаг 2 зелёный.
  - Действие: `git add src/PgWorker.Backups/Process/BackupPlanner.cs src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs && git commit -m "feat(backups): планировщик — свежесть по валидным полным (verify != FAILED), бэкофф от verify-фейлов (Ф4, AC6)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф4, AC6.

---

### Задача 12: Панель — парсер verify + правило backup-verify-failed — Ф5

`BackupsParser` панели читает `verify{state,checked_unix,error}`; `ShardLastCompletedUnix` — «последний валидный»; новое `ShardVerifyFailures` (последний FAILED по checked_unix — для текста алерта); правило `backup-verify-failed` (critical, per-shard); юниты `backup-full-stale` — на валидных (spec §3.6).

**Files:**
- Modify: `src/AdminPanel.Core/BackupInfo.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupVerifyFailedRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs`, Create: `src/tests/AdminPanel.UnitTests/BackupVerifyFailedRuleTests.cs`, Modify: `src/tests/AdminPanel.UnitTests/BackupFullStaleRuleTests.cs`

**Interfaces:**
- Consumes: `ClusterBackupsInfo` (t02-модель), паттерн `BackupFullStaleRule`/`WalChainBrokenRule`, `InjectAsSingleton(typeof(IAlertRule))`.
- Produces:
  ```csharp
  public sealed record ShardVerifyFailure(string Shard, string Id, string Error, long? CheckedUnix);
  public sealed record ClusterBackupsInfo(
      string Cluster, long? FullMaxAgeSec,
      IReadOnlyDictionary<string, long?> ShardLastCompletedUnix,
      IReadOnlyDictionary<string, WalStreamInfo?>? Shards = null,
      IReadOnlyDictionary<string, ShardVerifyFailure>? ShardVerifyFailures = null);
  ```

- [ ] **Шаг 1: падающие тесты парсера**

  - Вход: задача 2 закоммичена (контракт полей); `BackupsParserTests` — образец сида Kv.
  - Действие: добавить в панельные `BackupsParserTests.cs`:
    ```csharp
    // AAA: FAILED-verify полный не считается «последним COMPLETED» (свежесть — валидные);
    // фиксируется в ShardVerifyFailures с error/checked_unix
    [Fact]
    public void Parse_FailedVerify_НеДаетСвежести_иПопадаетВFailures()
    {
        // Arrange — битый полный СВЕЖЕЕ валидного
        var kvs = new[]
        {
            new Kv("/pgworker/backups/p1/shard1/full/20260910120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":100,"finished_unix":200,"verify":{"state":"OK","checked_unix":300}}"""),
            new Kv("/pgworker/backups/p1/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1000,"finished_unix":1100,"verify":{"state":"FAILED","checked_unix":1200,"error":"дыра WAL-цепочки: ожидался A, найден B"}}"""),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — свежесть = валидный (200), failure = битый с его текстом
        var cluster = result.Clusters.Single(c => c.Cluster == "p1");
        cluster.ShardLastCompletedUnix["shard1"].Should().Be(200);
        cluster.ShardVerifyFailures!["shard1"].Should().Be(
            new ShardVerifyFailure("shard1", "20260911120000Z", "дыра WAL-цепочки: ожидался A, найден B", 1200));
    }

    // AAA: несколько FAILED — последний по checked_unix; битый verify.state →
    // KeyParseError + verify игнор (полный валиден, непроверен)
    [Fact]
    public void Parse_НесколькоБитых_иТолерантность()
    {
        // Arrange — два FAILED-полных (checked_unix 100 и 200) + битый verify.state
        var kvs = new[]
        {
            new Kv("/pgworker/backups/p2/shard1/full/20260910120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1,"finished_unix":2,"verify":{"state":"FAILED","checked_unix":100,"error":"первый фейл"}}"""),
            new Kv("/pgworker/backups/p2/shard1/full/20260910130000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":3,"finished_unix":4,"verify":{"state":"FAILED","checked_unix":200,"error":"второй фейл"}}"""),
            new Kv("/pgworker/backups/p2/shard1/full/20260910140000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":5,"finished_unix":6,"verify":{"state":"BROKEN"}}"""),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — failure = последний по checked_unix; битый verify — диагностика,
        // запись жива и непроверена (валидна); парсер не падает
        var cluster = result.Clusters.Single(c => c.Cluster == "p2");
        cluster.ShardVerifyFailures!["shard1"].Error.Should().Be("второй фейл");
        cluster.ShardVerifyFailures["shard1"].CheckedUnix.Should().Be(200);
        result.Errors.Should().ContainSingle(e => e.Key.EndsWith("20260910140000Z"));
    }
    ```
  - Выход: тесты не компилируются (`ShardVerifyFailures` нет).
  - Проверка: сборка `AdminPanel.UnitTests` — CS1061.
  - Spec: §3.6 (парсер: verify-поля; `ShardLastCompletedUnix` — валидные; `ShardVerifyFailures`; битые — parseErrors + пропуск), решение пользователя «панельные алерты».

- [ ] **Шаг 2: реализовать парсер панели**

  - Вход: шаг 1.
  - Действие:
    - `BackupInfo.cs`: `ShardVerifyFailure` + расширение `ClusterBackupsInfo` (опциональное поле — совместимость).
    - `BackupsParser` (панельный): в ветке `full/<id>` при `state == "COMPLETED"` читать `verify`-объект: `state=="FAILED"` → не повышать `ShardLastCompletedUnix`; записать в `verifyFailures[cluster][shard]` кандидата `{id, error, checked_unix}`, замещая при `checked_unix` больше; `verify` отсутствует/null → прежнее поведение (валиден). Неизвестный `verify.state` → `errors.Add(new(kv.Key, "неизвестное verify.state — verify игнор"))` и запись считается непроверенной (валидной). `ClusterBackupsInfo` собирается с `ShardVerifyFailures` (кластер без FAILED — пустой словарь).
  - Выход: панель видит verify-вердикты и валидную свежесть.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~BackupsParserTests"` — PASS (старые + новые).
  - Spec: §3.6, adminpanel/02 §2.3.1.

- [ ] **Шаг 3: правило BackupVerifyFailedRule + тесты; обновить BackupFullStaleRuleTests**

  - Вход: шаг 2.
  - Действие: создать `BackupVerifyFailedRule.cs` (паттерн `BackupFullStaleRule`):
    ```csharp
    // backup-verify-failed (critical, t04): в статусе любого COMPLETED-полного
    // шарда verify.state=FAILED — полный невалиден (checksums/цепочка); текст —
    // verify.error; воркер переснимает автоматически (свежесть не считает).
    [InjectAsSingleton(typeof(IAlertRule))]
    public sealed class BackupVerifyFailedRule : IAlertRule
    {
        public const string KindName = "backup-verify-failed";
        public string Kind => KindName;

        public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
        {
            foreach (var cluster in snapshot.Backups)
            foreach (var (shard, failure) in cluster.ShardVerifyFailures ?? new Dictionary<string, ShardVerifyFailure>())
                yield return new Alert(
                    $"{KindName}:{cluster.Cluster}/{shard}",
                    AlertSeverity.Critical,
                    KindName,
                    $"{cluster.Cluster}/{shard}",
                    $"полный бэкап {failure.Id} шарда {shard} кластера {cluster.Cluster} невалиден: {failure.Error}",
                    new Dictionary<string, string>
                    {
                        ["backupId"] = failure.Id,
                        ["checkedUnix"] = failure.CheckedUnix?.ToString() ?? string.Empty,
                    },
                    null,
                    "проверка полного бэкапа провалена (pg_verifybackup/WAL-цепочка) — восстановимость под угрозой",
                    AlertRemedy.OperatorRunbook,
                    "воркер переснимает полный автоматически (verify FAILED не считается свежестью); для разбора — runbook t05; удаление битого — t06");
        }
    }
    ```
    Тесты `BackupVerifyFailedRuleTests.cs` (паттерн `BackupFullStaleRuleTests`: `TestSnapshots.Healthy(Now) with { Backups = [...] }`, локальный хелпер `Evaluate`):
    ```csharp
    using AdminPanel.Core;
    using AdminPanel.Core.Alerting;
    using AdminPanel.Core.Alerting.Rules;
    using FluentAssertions;
    using Xunit;

    namespace AdminPanel.UnitTests;

    // backup-verify-failed (t04, spec §3.6/AC7): critical per-shard на невалидный
    // полный, текст — verify.error; без FAILED и на пустом префиксе — молчит.
    public class BackupVerifyFailedRuleTests
    {
        private static readonly DateTimeOffset Now = new(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);

        private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
            => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

        private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
            => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

        // AAA: FAILED → critical с текстом verify.error и target C/X (AC7)
        [Fact]
        public void FailedVerify_CriticalAlertСErrorТекстом()
        {
            // Arrange — шарда s1: невалидный полный с причиной цепочки
            var rule = new BackupVerifyFailedRule();
            var snapshot = Snapshot(new ClusterBackupsInfo("demo", null,
                new Dictionary<string, long?> { ["s1"] = null },
                null,
                new Dictionary<string, ShardVerifyFailure>
                {
                    ["s1"] = new("s1", "20260911120000Z", "дыра WAL-цепочки: ожидался A, найден B", 1757500600),
                }));

            // Act
            var alerts = Evaluate(rule, snapshot);

            // Assert — канон: kind/severity/target/id + текст ошибки в message
            var alert = alerts.Should().ContainSingle().Subject;
            alert.Kind.Should().Be("backup-verify-failed");
            alert.Severity.Should().Be(AlertSeverity.Critical);
            alert.Target.Should().Be("demo/s1");
            alert.Id.Should().Be("backup-verify-failed:demo/s1");
            alert.Message.Should().Contain("20260911120000Z").And.Contain("дыра WAL-цепочки");
            alert.Details!["backupId"].Should().Be("20260911120000Z");
        }

        // AAA: без FAILED — молчит (AC7)
        [Fact]
        public void БезFailed_Молчит()
        {
            // Arrange — свежие валидные полные, failures пуст
            var rule = new BackupVerifyFailedRule();
            var snapshot = Snapshot(new ClusterBackupsInfo("demo", null,
                new Dictionary<string, long?> { ["s1"] = Now.ToUnixTimeSeconds() - 3600 },
                null,
                new Dictionary<string, ShardVerifyFailure>()));

            // Act
            var alerts = Evaluate(rule, snapshot);

            // Assert
            alerts.Should().BeEmpty();
        }

        // AAA: пустой префикс — молчит (AC7)
        [Fact]
        public void ПустойПрефикс_Молчит()
        {
            // Arrange — подсистема не включена (нет Backups вовсе)
            var rule = new BackupVerifyFailedRule();

            // Act
            var alerts = Evaluate(rule, TestSnapshots.Healthy(Now));

            // Assert
            alerts.Should().BeEmpty();
        }
    }
    ```
    Обновить `BackupFullStaleRuleTests` — кейс «свежий-но-битый последний полный горит» (AC6 панельная часть; вход правила уже «валидные» — код правила не меняется):
    ```csharp
    // AAA: единственный COMPLETED — с FAILED-verify: парсер отдаёт null (валидных
    // нет) → «никогда не завершался» (свежий-но-битый не гасит stale, t04/AC6)
    [Fact]
    public void OnlyFailedVerified_NeverCompleted()
    {
        // Arrange — битый полный завершён час назад, но ShardLastCompletedUnix=null
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = Snapshot(ClusterInfo("demo", null, ("s1", null)));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Message.Should().Contain("никогда не завершался");
    }
    ```
  - Выход: правило и панельная свежесть согласованы с воркером.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~BackupVerifyFailedRule|FullyQualifiedName~BackupFullStaleRule|FullyQualifiedName~BackupsParserTests"` — PASS.
  - Spec: §3.6 (правило: critical per-shard, текст verify.error, remedy; `BackupFullStaleRule` — вход уже валидные), AC6, AC7.

- [ ] **Шаг 4: коммит**

  - Вход: шаг 3 зелёный.
  - Действие: `git add -A src/AdminPanel.Core src/AdminPanel.Etcd src/tests/AdminPanel.UnitTests && git commit -m "feat(adminpanel): парсер verify + ShardVerifyFailures + алерт backup-verify-failed (critical); stale на валидных (Ф5, AC6/AC7)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф5, §3.6, AC6, AC7.

---

### Задача 13: E2E — маркер Backup_Verify_Ok_OnCreate + сценарий порчи — Ф6

Docker-E2E на свежем Release (per-Fact `E2eEnvironment` c MinIO + образ `pgworker-backup:e2e`): полный цикл on_create-verify и порча объекта → периодическая перепроверка → FAILED (spec §4 Ф6).

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs`

**Interfaces:**
- Consumes: хелперы `E2eBackupScenarios` (`SeedClusterAsync`, `StartBackupHostAsync`, `FullKeysAsync`, `McLsAsync`, `MasterPgAsync`), `E2eEnvironment.StartAsync(slug, withMinio: true)`, `E2eFixture.WaitForAsync`, `DockerTrait.SkipIfUnavailable`; процесс (з.8–10).
- Produces: мерж-гейт-маркер `Backup_Verify_Ok_OnCreate` (з.14).

- [ ] **Шаг 1: написать E2E-сценарий on_create (маркер)**

  - Вход: задачи 8–10 закоммичены; docker доступен (`DockerTrait`).
  - Действие: добавить в `E2eBackupScenarios.cs` (структура — копия `Backup_FullDaily_Completes`, свои slug/кластер для per-Fact изоляции):
    ```csharp
    // AAA: on_create-verify (AC1): COMPLETED → verify PENDING → verify-джоб
    // pgw-backup-verify-* → verify.state=OK + checked_unix; парсеры без parseErrors
    [Fact]
    public async Task Backup_Verify_Ok_OnCreate()
    {
        // Arrange — окружение с MinIO; кластер bkvrfy; policy on_create=true
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-verify", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkvrfy";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":0}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkverify", ct);

        // Act 1 — фаза PENDING пройдена: в ключе PENDING (t02 пишет при COMPLETED)
        // и/или жив контейнер pgw-backup-verify-<C>-* (PENDING держится в ключе
        // до итога — стабильное условие; контейнер — свидетельство джоба)
        var sawPending = await E2eFixture.WaitForAsync(async () =>
        {
            if ((await FullKeysAsync(cluster, "shard1"))
                .Any(f => f.Value.Contains(""""verify":{"state":"PENDING"""")))
                return true;
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length > 0;
        }, TimeSpan.FromSeconds(180), ct);
        sawPending.Should().BeTrue("on_create: verify обязан стартовать (PENDING в ключе / контейнер pgw-backup-verify-*)");

        // Act 2 — ждём verify.state=OK (бюджет 300 c: полный ~минуты + verify-скачивание)
        var verified = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);

        // Assert 1 — OK + checked_unix (фаза PENDING зафиксирована Act 1)
        verified.Should().BeTrue("on_create: verify должен дойти до OK (AC1)");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["verify"].GetProperty("state").GetString().Should().Be("OK");
        status["verify"].GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);

        // Assert 2 — verify-джоб отработал и снесён (контейнер/volume-префиксы чисты)
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            var volumes = await Fx.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length == 0 && volumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("verify-джоб и volume сносятся после итога (AC8)");

        // Assert 3 — воркерный и панельный парсеры читают без parseErrors (AC1);
        // панельный Kv — отдельный тип (AdminPanel.Etcd.Client.Kv), маппинг 1:1
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panelKvs = kvs.Select(kv => new AdminPanel.Etcd.Client.Kv(kv.Key, kv.Value, kv.ModRevision)).ToList();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(panelKvs);
        panel.Errors.Should().BeEmpty();
        panel.Clusters.Single(c => c.Cluster == cluster).ShardVerifyFailures.Should().BeEmpty();
    }
    ```
  - Выход: E2E-маркер написан; на зелёной реализации проходит.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Backup_Verify_Ok_OnCreate` — PASS (первый прогон может собрать Release — инкрементальный). Затем зачистка: `docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f` (фильтр не задевает dev-стенд; контрольный `docker ps -aq --filter "name=pgw-*" | wc -l` → 0).
  - Spec: Ф6, AC1, AC9 (мерж-гейт-маркер), AGENTS.md (E2E на свежем Release обязателен).

- [ ] **Шаг 2: E2E-сценарий порчи (checksums → FAILED по периодике)**

  - Вход: шаг 1 зелёный.
  - Действие: добавить сценарий (per-Fact окружение `bk-corrupt`, кластер `bkcrpt`; политика с малым `interval_sec` — перепроверка форсируется политикой, не ожиданием):
    ```csharp
    // AAA: порча набора (AC2): удалить объект из full/<id>/ в MinIO → policy
    // interval_sec мал → перепроверка → verify.state=FAILED + error; переснятия
    // в окне теста нет (full_max_age_sec велик)
    [Fact]
    public async Task Backup_Verify_Corruption_Fails()
    {
        // Arrange — окружение + кластер; interval_sec=5 форсирует перепроверку
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-corrupt", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkcrpt";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":5}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkcorrupt", ct);

        // Assert 1 — первый verify OK (как в маркере)
        var firstOk = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);
        firstOk.Should().BeTrue("исходный набор валиден");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var id = done.Key.Split('/').Last();

        // Act — портим: mc rm один объект из full/<id>/ (PG_VERSION из листинга набора)
        await Fx.RunDockerAsync(
            ["run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
                "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                      + $" && mc rm t/{Bucket}/{cluster}/shard1/full/{id}/PG_VERSION"], ct);

        // Assert 2 — перепроверка по interval → FAILED + error (бюджет 180 c:
        // interval 5 c + тик + джоб со скачиванием)
        var failed = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"FAILED"""")),
            TimeSpan.FromSeconds(180), ct);
        failed.Should().BeTrue("порча набора обязана дать verify FAILED (AC2)");
        var corrupted = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(corrupted.Value)!;
        status["verify"].GetProperty("error").GetString().Should().NotBeNullOrEmpty("причина pg_verifybackup — в verify.error");

        // Assert 3 — в момент провала verify переснятие не УСПЕЛО завершиться.
        // Точная механика (задача 11): после verify FAILED валидных полных нет →
        // IsDue=true СРАЗУ; переснятие сдерживает только BackoffPassed (n растёт
        // и от verify-фейлов; окно Retry.BaseSec=2 c из StartBackupHostAsync) —
        // новая ПОПЫТКА (PLANNED/RUNNING-ключ) допустима, но полный снимается
        // минуты → COMPLETED в момент этого ассерта обязан быть один.
        (await FullKeysAsync(cluster, "shard1")).Count(f => f.Value.Contains("COMPLETED"))
            .Should().Be(1, "новый COMPLETED-полный не успевает появиться в момент провала verify");
    }
    ```
  - Выход: AC2 покрыт E2E.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Backup_Verify_Corruption` — PASS; зачистка серий (`docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f`).
  - Spec: Ф6, AC2 (checksums-часть; download-сбой покрыт интеграцией з.9), §4 Ф6 (сценарий порчи).

- [ ] **Шаг 3: коммит**

  - Вход: шаги 1–2 зелёные + зачистка.
  - Действие: `git add src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs && git commit -m "test(e2e): Backup_Verify_Ok_OnCreate (мерж-гейт-маркер) + порча набора → FAILED по периодике (Ф6, AC1/AC2)"`
  - Выход: коммит.
  - Проверка: `git log --oneline -1`.
  - Spec: Ф6, AC1, AC2, AC9.

---

### Задача 14: Мерж-гейт — полный прогон, зачистка, roadmap — Ф7

Полный прогон юниты → интеграция → E2E на свежем Release; зачистка контейнеров/сетей после КАЖДОЙ серии; снятие тега `t04-backup-verify` из roadmap тем же мерж-коммитом (AC9).

**Files:**
- Modify: `arch/roadmap/backup.md` (снять тег; в конце задачи — мерж-коммит)

**Interfaces:**
- Consumes: всё выше.
- Produces: зелёная ветка, готовая к ревью и мержу в `main`.

- [ ] **Шаг 1: полный прогон юнитов**

  - Вход: задачи 1–13 закоммичены.
  - Действие:
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName!~IntegrationTests"
    ```
  - Выход: все юниты (PgWorker/AdminPanel/KafkaWorker/Shared.Metrics) зелёные.
  - Проверка: последняя строка прогона — Passed; 0 Failed.
  - Spec: Ф7, AC9.

- [ ] **Шаг 2: полный прогон интеграций (docker)**

  - Вход: шаг 1; docker-демон доступен; dev-стенд поднят — его контейнеры (`as-*`, `adminpanel`) не трогаем.
  - Действие:
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug
    docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f
    DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug
    docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f
    ```
    (зачистка между проектами-сериями обязательна; `docker ps -aq | wc -l` — контрольный ноль сверх dev-стенда).
  - Выход: интеграции зелёные; остаточных контейнеров/сетей нет.
  - Проверка: обе серии Passed; `docker network ls | grep -c 'kfw-net\|pgw'` → 0.
  - Spec: Ф7, AGENTS.md (зачистка после КАЖДОЙ серии).

- [ ] **Шаг 3: E2E-маркер на свежем Release + вся E2eFixture-серия бэкапов**

  - Вход: шаг 2, зачистка выполнена.
  - Действие:
    ```bash
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Backup_Verify_Ok_OnCreate
    docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f
    DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eBackupScenarios"
    docker rm -f $(docker ps -aq --filter "name=pgw-*") 2>/dev/null; docker network prune -f
    ```
  - Выход: маркер и вся серия бэкап-E2E (t02/t03/t04 вместе — регрессии нет) зелёные на свежем Release.
  - Проверка: обе серии Passed; зачистка подтверждена (`docker ps -aq | wc -l` — 0 сверх dev-стенда).
  - Spec: Ф7, AC1, AC9, AGENTS.md (E2E на свежем Release — мерж-гейт; урок t09).

- [ ] **Шаг 4: roadmap-гейт**

  - Вход: шаги 1–3 зелёные; ревью ветки пройдено (отдельный этап dev-flow).
  - Действие: убрать из `arch/roadmap/backup.md` пункт `t04-backup-verify` и зависимость `← t04-backup-verify` из других пунктов (строка ~16 и ~40); этим ЖЕ мерж-коммитом в `main` (правило AGENTS.md: тег снимается мерж-коммитом; в ветке — подготовить правку, коммитить вместе с мержем или последним коммитом ветки по решению ревью).
  - Выход: roadmap не содержит снятых задач.
  - Проверка: `grep -rn "t04-backup-verify" arch/roadmap/` → пусто (после мержа).
  - Spec: Ф7, AC9, AGENTS.md (roadmap-гейт).

---

## Покрытие spec → задачи (self-review)

| Требование spec | Задача |
|---|---|
| Ф0 канон (внесено в spec-фазе) | 1 (фиксация коммитом) |
| §3.7 модель `verify.error`/`VerifyIntervalSec`/JSON/парсеры/конфиг | 2 |
| §3.4 `WalHistory` (LSN-парсер, AC4) | 3 |
| §3.4 `WalChain.CheckRange` (AC3-юниты, fallback) | 4 |
| §3.2 `VerifyJobCommand`/`VerifyJobSpec`/имена | 5 |
| §3.4 `BackupS3.ListAsync/GetObjectAsync` (Ф2) | 6 |
| §3.3 D1-чистка verify (AC8-deprovisioning) | 7 |
| §3.1/§3.3 процесс: due/цепочка/запуск (AC3/AC4-интеграция) | 8 |
| §3.1/§3.2 супервиз, transient/permanent, периодика (AC1/AC2/AC5/AC8) | 9 |
| §3.3 wiring + §3.8 метрика | 10 |
| §3.5 планировщик на валидных (AC6-воркер) | 11 |
| §3.6 панель: парсер/правило/stale (AC6-панель, AC7) | 12 |
| Ф6 E2E маркер + порча (AC1/AC2) | 13 |
| Ф7 мерж-гейт/зачистка/roadmap (AC9) | 14 |

Решения пользователя (spec §1) разложены: до конца набора (з.4 — `end` из `pg_wal/`); периодика 7 дней + policy-override (з.2/з.8/з.9); полный LSN-разбор `.history` в verify, runtime t03 — эвристика (з.3/з.4 — `Check` не меняется); одно правило `backup-verify-failed` (з.12); валидность в планировщике/панели (з.11/з.12).

Семантика transient (статус не меняем, ретраи тиками): list/GET S3, transport docker, download-phase джоба, ENOSPC staging. Permanent (FAILED + checked_unix): ненулевой `pg_verifybackup`, дыра цепочки, нет `wal_start_segment`, пустой `pg_wal` набора.
