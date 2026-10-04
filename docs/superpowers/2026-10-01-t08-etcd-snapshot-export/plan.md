# t08-etcd-snapshot-export — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** каждый слепок etcd (плановый и внеочередной) уходит в S3-хранилище установки (bucket бэкапов, служебный префикс `etcd/`, sha256-мета, ретенция N пар), со статус-ключом `/pgworker/etcd-snapshots`, панельными алертами и операторским каноном восстановления (runbook).

**Архитектура:** опциональный sink-интерфейс `ISnapshotSink` в `Shared.Etcd` вызывается из `SnapshotJob.TakeAsync` сразу после записи локального файла (сбой выгрузки не роняет снятие); S3-реализация `EtcdSnapshotSink` в `PgWorker.Backups` — путь ОДИН, без сравнения с прошлым состоянием (каждый снятый слепок уезжает новой парой db+meta): put `.db`+`.meta.json` → ретенция → статус-ключ OK/FAILED (`last_uploaded_unix` = метка покрытого слепка, не now); `SnapshotLoop` лидера доводит отстающую выгрузку тиком `RetryIntervalSec`; панель читает статус-ключ точечным Range и алертит `etcd-snapshot-export-failed`/`etcd-snapshot-export-stale`; восстановление — операторский runbook-рецепт (`etcdctl snapshot status` + `snapshot restore`).

**Стек:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; AWSSDK.S3 3.7.511.8 (`ChecksumSHA256`); testcontainers (OwnEtcd/OwnMinio/E2eEnvironment); etcd-образ `quay.io/coreos/etcd:v3.5.21` (содержит `etcdctl`).

**Spec:** [`docs/superpowers/2026-10-01-t08-etcd-snapshot-export/spec.md`](spec.md) — план аргументируется от spec; исполнители читают оба документа.

## Глобальные ограничения

- .NET 10, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — код собирается без warnings.
- Язык документации/комментариев — русский; идентификаторы — английские (AGENTS.base §8).
- Тесты — AAA-нотация (комментарии `// Arrange` / `// Act` / `// Assert`).
- Arch-first: правки `arch/` (Task 1) ДО кода; roadmap-снятие (Task 14) — мерж-гейтом тем же мерж-коммитом (spec §3.9).
- KafkaWorker/ValkeyWorker не затрагиваются: их `new SnapshotJob(...)` (KafkaWorker.App/Program.cs:202, ValkeyWorker.App/Program.cs:139) остаются сигнатурно совместимыми — параметр sink опциональный (spec §3.4, §5).
- Опция `Snapshots:Export:Enabled=false` (дефолт) — поведение воркера байт-в-байт прежнее (spec §2.10, AC10).
- Интеграции/E2E: своё окружение на сценарий (OwnEtcd `pgw-ee-<guid>` / OwnMinio `pgw-em-<guid>` / E2eEnvironment `pgw-en-<guid>`), динамические порты (`assignRandomHostPort`/`FreePort`), полный teardown при любом исходе, ассерт чистоты, бюджеты готовности ≤ 100 с (AGENTS.base §12, AGENTS.md).
- Коммиты — в feature-ветку `feat-t08-etcd-snapshot-export` по шагам плана; мерж в main — только по явной просьбе пользователя.
- Статус-ключ: `/pgworker/etcd-snapshots`; формат значения — spec §3.1 (буквально).
- S3-комплект — только из `PgWorker:Backups:S3` (второй набор секретов не заводится, spec §2.5, arch/19 §7); `Backups:Enabled` для экспорта не требуется (spec §3.3).
- Дедуп выгрузок НЕ вводится (spec §2.7/§5, явный запрет дедуп-ветви в реализации — §3.6): каждый снятый слепок уезжает новой парой db+meta — sha256 целого слепка самореферентно недостижим (статус-ключ живёт в снимаемом etcd и обновляется каждой выгрузкой, поэтому sha следующего слепка всегда отличается); `last_sha256` в статус-ключе — наблюдаемость, в решении о put не участвует; рост объёма S3 ограничен ретенцией N пар.

## Карта файлов

| Файл | Действие | Ответственность |
|---|---|---|
| `arch/14-pgworker.md`, `arch/19-backups.md`, `arch/09-troubleshooting.md`, `arch/adminpanel/02-etcd-contract.md`, `arch/adminpanel/03-panels.md` | правка (Task 1) | канон до кода |
| `src/Shared.Etcd/Maintenance/ISnapshotSink.cs` | создать (Task 3) | контракт sink |
| `src/Shared.Etcd/Maintenance/SnapshotJob.cs` | править (Task 3) | опциональный sink + ревизия |
| `src/PgWorker.Backups/EtcdExport/EtcdSnapshotStatus.cs` | создать (Task 2) | статус-JSON, IsBehind, TakenUnixFromName |
| `src/PgWorker.Backups/EtcdExport/EtcdSnapshotMeta.cs` | создать (Task 2) | meta-JSON слепка |
| `src/PgWorker.Backups/EtcdExport/EtcdExportRetention.cs` | создать (Task 2) | чистый отбор ретенции |
| `src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs` | создать (Task 5) | put/ретенция/статус/доводка |
| `src/PgWorker.Backups/BackupS3.cs` | править (Task 4) | `PutObjectAsync` |
| `src/PgWorker.App/Options.cs` | править (Task 6) | `SnapshotExportOptions` |
| `src/PgWorker.App/Program.cs` | править (Task 6) | fail-fast + склейка sink |
| `src/PgWorker.App/Loops/SnapshotLoop.cs` | править (Task 7) | доводка + сон |
| `src/PgWorker.App/appsettings.json` | править (Task 6) | секция Export |
| `deploy/docker-compose.yml`, `deploy/.env.example` | править (Task 8) | env-включение |
| `src/AdminPanel.Core/BackupsInfo.cs` | править (Task 9) | `EtcdSnapshotExportInfo` |
| `src/AdminPanel.Core/EtcdSnapshot.cs` | править (Task 9) | поле `EtcdSnapshots` |
| `src/AdminPanel.Etcd/Parsing/EtcdSnapshotsParser.cs` | создать (Task 9) | толерантный парсер |
| `src/AdminPanel.Etcd/SnapshotRefresher.cs` | править (Task 9) | точечное чтение ключа |
| `src/AdminPanel.Core/Alerting/Rules/EtcdSnapshotExportFailedRule.cs`, `...StaleRule.cs` | создать (Task 10) | алерты |
| `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs` | править (Task 11) | DTO + маппер |
| `frontend/src/pages/BackupsStoragePage.tsx` | править (Task 11) | карточка UI |
| `docs/runbook.md` | править (Task 12) | операторский путь |
| `src/tests/...` (юниты/интеграции/E2E) | создать/править | по таскам |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | править (Task 14) | мерж-гейт |

Рабочий каталог всех команд — worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t08-etcd-snapshot-export` (ниже — относительные пути от него).

---

## Task 1: Arch-first — канон (spec §3.9 п.1–4)

**Вход:** spec одобрен; worktree чист (кроме `docs/superpowers/2026-10-01-t08-etcd-snapshot-export/`).
**Действие:** правки пяти arch-файлов (содержание — ниже по шагам).
**Выход:** канон обновлён ДО кода; последующие таски зеркалят его.
**Проверка:** `git diff --stat` показывает 5 arch-файлов; текст каждого дополнения соответствует пунктам spec §3.9.
**Связь со spec:** §3.9 п.1–4, §2.1 (arch-first), AC9.

- [x] **Step 1.1: `arch/14-pgworker.md` — три правки**

  1. §3.3 таблица «НОВЫЕ ключи координации воркеров» — новая строка после `/pgworker/rotations/<C>`:

  ```markdown
  | `/pgworker/etcd-snapshots` | обычный | **статус выгрузки снапшотов etcd в S3** (reliability t08): `{"enabled":bool,"state":"OK"\|"FAILED","last_uploaded_unix":N,"last_object":"etcd/snapshot-<id>.db","last_sha256":"<hex>","size_bytes":N,"interval_min":360,"error"?:"…"}`. Каждый снятый слепок выгружается; статус-ключ обновляется после каждой выгрузки. `last_uploaded_unix` — семантика «покрытия», не времени put-запроса: метка снятия последнего слепка, чьё содержимое подтверждённо доставлено в S3 фактическим upload'ом; поле двигается каждым успешным проходом sink'а (иначе детект отставания и stale-алерт врут на живом контуре). `last_object` — объект последней выгрузки, реально лежащий в S3; `last_sha256` — sha256 этой выгрузки (наблюдаемость). Пишет ТОЛЬКО инстанс PgWorker, выполнивший экспорт-операцию (лидер снапшотов или процесс в точках изменений — put одним ключом без RMW); панель читает (adminpanel/02 §2.3.1). Наблюдаемость; источником для восстановления НЕ является (etcd мёртв — ключа нет). |
  ```

  2. §6 «Надёжность», пункт «Снапшоты P12» — дополнить (после «Restore — внешний рецепт…»):

  ```markdown
  - **Экспорт слепков в S3 (reliability t08)**: каждый снятый слепок (плановый
    и внеочередной) уезжает в bucket per-install подсистемы бэкапов служебным
    префиксом `etcd/` парой `.db`+`.meta.json` (sha256-мета; arch/19 §5) —
    слепок выгружается целиком, статус-ключ покрытия
    `/pgworker/etcd-snapshots` обновляется после каждой выгрузки
    (`last_uploaded_unix` продвигается к метке покрытого слепка; §3).
    Экспортёр — PgWorker (единственный владелец
    S3-кредов per-install); выгружает держатель глобального клэйма снапшотов
    `/pgworker/leader` и процессы в точках изменений (слепок любого воркера =
    весь etcd). Сбой выгрузки НЕ роняет снятие: ошибка — в ключе
    `/pgworker/etcd-snapshots`, доводка — тиком лидера `RetryIntervalSec`
    (re-export новейшего локального слепка). Ретенция S3 — N
    последних пар (guard ≥1). Восстановление — операторский runbook
    (docs/runbook.md §t08, arch/09 §4): etcd без воркера не живёт,
    автоматизировать некому.
  ```

  3. §8 «Конфигурация»: заменить строку `PgWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }` на:

  ```markdown
  PgWorker:Snapshots { Dir="/snapshots", RetentionFiles=10,
                       MaintenanceIntervalMin=60,
                       Export { Enabled=false, RetentionObjects=28,
                                RetryIntervalSec=300, TimeoutSec=30 } }
                  # t08: выгрузка слепков в S3 (bucket бэкапов, префикс etcd/);
                  # S3-комплект переиспользуется из PgWorker:Backups:S3
                  # (Backups:Enabled НЕ требуется — контроль-плейн не зависит от
                  # подсистемы бэкапов PG); TimeoutSec — общий бюджет ОДНОЙ
                  # попытки экспорта (ExportAsync целиком, не шага);
                  # Enabled=true при пустых
                  # Backups:S3 {Endpoint,Bucket,AccessKey,SecretKey} и диапазоны
                  # RetentionObjects>=1 / RetryIntervalSec>0 / TimeoutSec>0 —
                  # fail-fast старта
  ```

- [x] **Step 1.2: `arch/19-backups.md` — §5 и §7**

  §5 после блока layout (перед «Ретенционная чистка (t06…)») — новый пункт:

  ```markdown
  - **Служебный префикс `etcd/` (reliability t08)**: выгрузка снапшотов etcd
    (контроль-плейн) — `etcd/snapshot-<id>.db` 1:1 + `etcd/snapshot-<id>.meta.json`
    (sha256/size/revision?/taken_unix/uploaded_unix/instance; `uploaded_unix` —
    фактическое время put'а объекта, в отличие от «покрытия» в статус-ключе).
    Каждый снятый слепок выгружается; статус-ключ покрытия
    `/pgworker/etcd-snapshots` (arch/14 §3) обновляется после каждой выгрузки.
    Вне per-cluster layout: не форма `<C>/<X>/` → реестр сирот t07 его не группирует (ключи короче
    3 сегментов); ретенция t06 и sweeper t07 к `etcd/` не прикасаются (их пути
    адресны: `full/<id>/`, `wal/`, `<C>/<X>/`). Единственный удаляющий — ретенция
    самого экспорта (N последних пар, guard ≥1 полная пара). Объекты `etcd/`
    входят в `used_bytes` ключа `/pgworker/backups/storage` (list всего bucket —
    занятость bucket, корректно).
  ```

  §7 первый пункт — дополнить в конец: «тот же комплект S3 обслуживает и выгрузку etcd-снапшотов (t08: `Snapshots:Export` — см. [14-pgworker.md](14-pgworker.md) §8); второй набор S3-секретов не заводится».

- [x] **Step 1.3: `arch/09-troubleshooting.md` §4** — в подраздел «Если etcd-кластер разрушен полностью (нет данных)» заменить абзац «Восстановление (если есть бэкап etcd):» на:

  ```markdown
  Восстановление (источник — S3-выгрузка t08: `etcd/snapshot-<id>.db` в bucket
  бэкапов; полный операторский путь с проверкой целостности ДО восстановления —
  docs/runbook.md §«Восстановление etcd из S3-выгрузки (t08)»; локальный файл
  тома `pgw-snapshots` (если пережил) — fallback того же формата):
  ```bash
  # 1) скачать новейший snapshot-<id>.db (+ .meta.json), сверить sha256
  #    и etcdctl snapshot status (обязательный шаг, runbook t08)
  # 2) стартовать etcd заново как новый кластер (INITIAL_CLUSTER_STATE=new)
  # 3) восстановить snapshot:
  etcdctl snapshot restore /backup/etcd.snap --data-dir=/data/etcd
  ```
  ```

  И финальную строку секции «Поэтому бэкап etcd… обязателен» дополнить: «; при `Snapshots:Export:Enabled=true` выгрузка уходит в S3 автоматически каждым слепком (t08)».

- [x] **Step 1.4: `arch/adminpanel/02-etcd-contract.md` §2.3.1** — две правки: (1) в преамбуле секции (строка ~112: «панель читает точечно — пять ключа-семейства, остальные ключи префикса…») заменить «пять» → «шесть» (добавляется семейство из п.2); (2) в таблицу (после строки `/pgworker/backups/<C>/<X>/drill`) новая строка:

  ```markdown
  | `/pgworker/etcd-snapshots` | JSON `{"enabled":bool,"state":"OK"\|"FAILED","last_uploaded_unix"?<unix>,"last_object"?,"last_sha256"?,"size_bytes"?<n>,"interval_min"?<n>,"error"?}` (канон — arch/14 §3.3, reliability t08) | `EtcdSnapshotExportInfo` (§3) | статус выгрузки etcd-снапшотов в S3: пишет ТОЛЬКО PgWorker (инстанс-исполнитель экспорта), панель читает; кормит алерты `etcd-snapshot-export-failed`/`etcd-snapshot-export-stale` (03 §4) и карточку «etcd-снапшоты» грани «Хранилище бэкапов»; `enabled=false`/отсутствие ключа — правила молчат (толерантный читатель: битый JSON — parseError-запись) |
  ```

- [x] **Step 1.5: `arch/adminpanel/03-panels.md` §4** — в таблицу каталога алертов (после `backup-s3-unreachable`) две строки:

  ```markdown
  | `etcd-snapshot-export-failed` | critical | `enabled=true` и `state=FAILED` — текст `error`; контроль-плейн не защищён от потери хоста | `/pgworker/etcd-snapshots` |
  | `etcd-snapshot-export-stale` | warning | `enabled=true` и (`last_uploaded_unix` отсутствует или старше `2×interval_min`, панельный дефолт 360 мин); «выгрузка молчит» (обе инстанции воркера лежат / вечный transient) | `/pgworker/etcd-snapshots` |
  ```

  И в §3 (Панели UI) строку грани «Хранилище бэкапов» дополнить упоминанием карточки «etcd-снапшоты» (последняя выгрузка: время/возраст, sha256-префикс, размер, статус OK/FAILED, ошибка; read-only).

- [x] **Step 1.6: Commit**

```bash
git add arch/14-pgworker.md arch/19-backups.md arch/09-troubleshooting.md arch/adminpanel/02-etcd-contract.md arch/adminpanel/03-panels.md
git commit -m "docs(t08): arch-first — канон экспорта etcd-снапшотов в S3 (ключ /pgworker/etcd-snapshots, префикс etcd/ в arch/19 §5, arch/14 §3.3/§6/§8, arch/09 §4 восстановление из выгрузки, adminpanel/02 §2.3.1 + adminpanel/03 §4 алерты)"
```

---

## Task 2: Чистые функции экспорта (spec §3.1/§3.2/§3.6)

**Вход:** Task 1 смержён в ветку (канон есть).
**Действие:** три новых файла в `src/PgWorker.Backups/EtcdExport/` + юнит-тесты + гвард-тест `GroupPrefixes`.
**Выход:** сериализация/парсинг статус-JSON и meta-JSON, отбор ретенции, `IsBehind`/`TakenUnixFromName` — чистые, покрытые юнитами; гвард изоляции префикса доказан.
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~EtcdExport|FullyQualifiedName~OrphanRegistry"` — зелёные.
**Связь со spec:** §3.1 (формат ключа), §3.2 (layout/ретенция/guard), §3.6 (чистые функции), Фаза 1, AC4/AC6/AC8.

- [x] **Step 2.1: Failing-тесты статус-JSON** — создать `src/tests/PgWorker.UnitTests/Backups/EtcdExportStatusJsonTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups.EtcdExport;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Статус-ключ /pgworker/etcd-snapshots (t08, arch/14 §3.3): сериализация OK/FAILED
// (FAILED сохраняет поля последнего успеха), толерантный парсинг, IsBehind-детект.
public class EtcdExportStatusJsonTests
{
    // AAA: OK-ключ — полный формат канона §3.1; last_uploaded_unix — метка
    // ПОКРЫТОГО слепка, не время put-запроса (§3.1)
    [Fact]
    public void Ok_полный_формат()
    {
        // Act
        var json = EtcdSnapshotStatusJson.Ok(
            coveredTakenUnix: 1759330000, lastObject: "etcd/snapshot-20261001-120000.db",
            lastSha256: "abc123", sizeBytes: 2048, intervalMin: 360);

        // Assert — snake_case-поля, error отсутствует
        json.Should().Contain("\"state\":\"OK\"")
            .And.Contain("\"enabled\":true")
            .And.Contain("\"last_uploaded_unix\":1759330000")
            .And.Contain("\"last_object\":\"etcd/snapshot-20261001-120000.db\"")
            .And.Contain("\"last_sha256\":\"abc123\"")
            .And.Contain("\"size_bytes\":2048")
            .And.Contain("\"interval_min\":360")
            .And.NotContain("\"error\"");
    }

    // AAA: FAILED сохраняет поля последнего успеха (оператор видит, насколько отстал)
    [Fact]
    public void Failed_сохраняет_поля_последнего_успеха()
    {
        // Arrange
        var lastOk = EtcdSnapshotStatusJson.Parse(EtcdSnapshotStatusJson.Ok(
            1759330000, "etcd/snapshot-20261001-120000.db", "abc123", 2048, 360))!;

        // Act
        var json = EtcdSnapshotStatusJson.Failed(lastOk, "S3 put: connection refused", 360);

        // Assert
        json.Should().Contain("\"state\":\"FAILED\"")
            .And.Contain("\"error\":\"S3 put: connection refused\"")
            .And.Contain("\"last_uploaded_unix\":1759330000")
            .And.Contain("\"last_sha256\":\"abc123\"");
    }

    // AAA: Failed без прошлого успеха — поля факта отсутствуют, enabled/interval/error есть
    [Fact]
    public void Failed_без_прошлого_успеха_минимальный_формат()
    {
        // Act
        var json = EtcdSnapshotStatusJson.Failed(null, "boom", 360);

        // Assert
        json.Should().Contain("\"state\":\"FAILED\"")
            .And.Contain("\"error\":\"boom\"")
            .And.NotContain("\"last_uploaded_unix\"")
            .And.NotContain("\"last_sha256\"");
    }

    // AAA: парсинг толерантен к отсутствующим полям; битый JSON → null
    [Fact]
    public void Parse_толерантный_и_битый()
    {
        // Act/Assert — валидный минимальный ключ
        var parsed = EtcdSnapshotStatusJson.Parse("""{"enabled":true,"state":"OK","interval_min":60}""");
        var status = parsed!; // Parse возвращает nullable-ссылку (не Nullable<T>): .Value нет
        status.Enabled.Should().BeTrue();
        status.State.Should().Be("OK");
        status.LastUploadedUnix.Should().BeNull();
        // Act/Assert — битый JSON → null (панель/парсер молчат)
        EtcdSnapshotStatusJson.Parse("{не json").Should().BeNull();
    }

    // AAA: IsBehind — FAILED/ключа нет/локальный новее подтверждённого покрытия
    // (uploaded = метка ПОКРЫТОГО слепка — инвариант §3.5 п.1: каждый успешный
    // проход sink'а продвигает поле, отставание закрывает)
    [Theory]
    [InlineData("FAILED", 1759330100, 1759330000, true)]   // FAILED — всегда отстаёт
    [InlineData("OK", 1759330100, 1759330000, true)]       // локальный слепок снят позже покрытия
    [InlineData("OK", 1759330000, 1759330100, false)]      // покрытие свежее локального — здорово
    public void IsBehind_детект(string state, long uploaded, long localTaken, bool expected)
    {
        // Arrange
        EtcdSnapshotStatus? status = new(true, state, uploaded, null, "abc", 1, 360, null);

        // Act
        var behind = EtcdSnapshotStatus.IsBehind(status, localTaken);

        // Assert
        behind.Should().Be(expected);
    }

    // AAA: ключа нет вовсе (включённая опция) — отстаёт; статус выключен — нет
    [Fact]
    public void IsBehind_нет_ключа_или_выключен()
    {
        // Act/Assert
        EtcdSnapshotStatus.IsBehind(null, 1759330100).Should().BeTrue("ключа нет при включённой опции — выгрузка отстаёт");
        EtcdSnapshotStatus.IsBehind(new EtcdSnapshotStatus(false, "FAILED", null, null, null, null, 360, null), 1759330100)
            .Should().BeFalse("enabled=false — доводка не нужна");
    }

    // AAA: таймстемп имени слепка (yyyyMMdd-HHmmss, UTC — формат SnapshotJob)
    [Fact]
    public void TakenUnixFromName_из_имени_файла()
    {
        // Act/Assert
        EtcdSnapshotStatus.TakenUnixFromName("snapshot-20261001-120000.db")
            .Should().Be(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());
        EtcdSnapshotStatus.TakenUnixFromName("иное.db").Should().BeNull();
    }
}
```

- [x] **Step 2.2: Run → FAIL** (`dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~EtcdExportStatusJson` — не компилируется: типов нет).

- [x] **Step 2.3: Реализация** — создать `src/PgWorker.Backups/EtcdExport/EtcdSnapshotStatus.cs`:

```csharp
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Значение статус-ключа /pgworker/etcd-snapshots (t08, arch/14 §3.3):
/// факт последней выгрузки слепка etcd в S3. Наблюдаемость; источник для
/// восстановления НЕ является (etcd мёртв — ключа нет). LastUploadedUnix —
/// семантика «покрытия» (§3.1): метка снятия последнего слепка, чьё содержимое
/// подтверждённо доставлено в S3 фактическим upload'ом — двигается каждым
/// успешным проходом sink'а.</summary>
public sealed record EtcdSnapshotStatus(
    bool Enabled,
    string State,               // "OK" | "FAILED"
    long? LastUploadedUnix,
    string? LastObject,
    string? LastSha256,
    long? SizeBytes,
    int? IntervalMin,
    string? Error)
{
    /// <summary>Выгрузка отстаёт: FAILED, либо ключа нет вовсе (включённая
    /// опция), либо новейший локальный слепок снят позже подтверждённого
    /// покрытия (возможен невыгруженный слепок). enabled=false — не отстаёт.
    /// Инвариант §3.5 п.1: каждый успешный проход sink'а продвигает
    /// last_uploaded_unix к метке обработанного слепка — «отстаёт» всегда
    /// означает «есть слепок новее покрытия», а не «давно не было put».</summary>
    public static bool IsBehind(EtcdSnapshotStatus? status, long? latestLocalTakenUnix)
        => status is null
           ? latestLocalTakenUnix is not null
           : status.Enabled
             && (status.State == "FAILED"
                 || (latestLocalTakenUnix is { } taken
                     && (status.LastUploadedUnix is not { } uploaded || taken > uploaded)));

    /// <summary>Таймстемп имени слепка snapshot-&lt;yyyyMMdd-HHmmss&gt;.db (UTC,
    /// формат SnapshotJob); чужое имя → null.</summary>
    public static long? TakenUnixFromName(string fileName)
    {
        if (!fileName.StartsWith("snapshot-", StringComparison.Ordinal)
            || !fileName.EndsWith(".db", StringComparison.Ordinal))
            return null;
        var stamp = fileName.Substring("snapshot-".Length, fileName.Length - "snapshot-".Length - ".db".Length);
        return DateTimeOffset.TryParseExact(
            stamp, "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var taken)
            ? taken.ToUnixTimeSeconds()
            : null;
    }
}

/// <summary>Чистые функции статус-ключа: сериализация OK/FAILED (FAILED
/// сохраняет поля последнего успеха — оператор видит, насколько отстал),
/// толерантный парсинг. IsBehind/TakenUnixFromName — на record
/// EtcdSnapshotStatus (call-сайты зовут их от типа записи).</summary>
public static class EtcdSnapshotStatusJson
{
    public const string Key = "/pgworker/etcd-snapshots";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private sealed record Payload(
        [property: JsonPropertyName("enabled")] bool Enabled,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("last_uploaded_unix")] long? LastUploadedUnix,
        [property: JsonPropertyName("last_object")] string? LastObject,
        [property: JsonPropertyName("last_sha256")] string? LastSha256,
        [property: JsonPropertyName("size_bytes")] long? SizeBytes,
        [property: JsonPropertyName("interval_min")] int? IntervalMin,
        [property: JsonPropertyName("error")] string? Error);

    /// <summary>Успешная выгрузка: state=OK, поля факта;
    /// coveredTakenUnix — метка ПОКРЫВАЕМОГО слепка (taken из имени),
    /// не now (§3.1/§3.6: фактическое время put'а — только meta.uploaded_unix).</summary>
    public static string Ok(long coveredTakenUnix, string lastObject, string lastSha256, long sizeBytes, int intervalMin)
        => Serialize(new Payload(true, "OK", coveredTakenUnix, lastObject, lastSha256, sizeBytes, intervalMin, null));

    /// <summary>Неудача: state=FAILED + error; поля последнего успеха переносятся
    /// (null-поля lastOk опускаются — минимальный формат).</summary>
    public static string Failed(EtcdSnapshotStatus? lastOk, string error, int intervalMin)
        => Serialize(new Payload(
            true, "FAILED", lastOk?.LastUploadedUnix, lastOk?.LastObject, lastOk?.LastSha256,
            lastOk?.SizeBytes, intervalMin, error));

    /// <summary>Толерантный парсинг: битый JSON/поля → null (панель молчит,
    /// воркер перезапишет фактом); отсутствующие поля — null-компоненты.</summary>
    public static EtcdSnapshotStatus? Parse(string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<Payload>(raw, Json);
            if (payload?.State is not ("OK" or "FAILED"))
                return null;
            return new EtcdSnapshotStatus(
                payload.Enabled, payload.State, payload.LastUploadedUnix, payload.LastObject,
                payload.LastSha256, payload.SizeBytes, payload.IntervalMin, payload.Error);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Serialize(Payload payload) => JsonSerializer.Serialize(payload, Json);
}
```

(Если slicing-вариант `TakenUnixFromName` окажется громче прямой проверки — допустимо упростить до `Regex`/двух `StartsWith`-гвардов + `TryParseExact` средней части; тест из 2.1 обязан проходить.)

- [x] **Step 2.4: Run → PASS** (`--filter FullyQualifiedName~EtcdExportStatusJson`).

- [x] **Step 2.5: Failing-тесты meta-JSON** — создать `src/tests/PgWorker.UnitTests/Backups/EtcdExportMetaJsonTests.cs` (кейсы AAA):

```csharp
// AAA: полный формат — revision присутствует; uploaded_unix — ФАКТИЧЕСКОЕ время
// put'а объекта (в отличие от «покрытия» last_uploaded_unix статус-ключа, §3.2)
[Fact]
public void Serialize_полный_формат()
{
    // Act
    var json = EtcdSnapshotMetaJson.Serialize(new EtcdSnapshotMeta("abc123", 2048, 42, 1759330000, 1759330001, "inst-A"));
    // Assert — snake_case, revision на месте
    json.Should().Contain("\"sha256\":\"abc123\"").And.Contain("\"size_bytes\":2048")
        .And.Contain("\"revision\":42").And.Contain("\"taken_unix\":1759330000")
        .And.Contain("\"uploaded_unix\":1759330001").And.Contain("\"instance\":\"inst-A\"");
}

// AAA: revision=null — поле ОПУСКАЕТСЯ (best-effort ревизии, слепок не виноват)
[Fact]
public void Serialize_без_ревизии_опускает_поле()
{
    // Act
    var json = EtcdSnapshotMetaJson.Serialize(new EtcdSnapshotMeta("abc", 1, null, 2, 3, "i"));
    // Assert
    json.Should().NotContain("\"revision\"");
}

// AAA: парсинг 1:1; битый → null
[Fact]
public void Parse_круговорот_и_битый()
{
    // Arrange
    var meta = new EtcdSnapshotMeta("abc123", 2048, null, 1759330000, 1759330001, "inst-A");
    // Act/Assert
    EtcdSnapshotMetaJson.Parse(EtcdSnapshotMetaJson.Serialize(meta)).Should().Be(meta);
    EtcdSnapshotMetaJson.Parse("{oops").Should().BeNull();
}
```

- [x] **Step 2.6: Реализация** — `src/PgWorker.Backups/EtcdExport/EtcdSnapshotMeta.cs`: record `EtcdSnapshotMeta(string Sha256, long SizeBytes, long? Revision, long TakenUnix, long UploadedUnix, string Instance)` (doc: `UploadedUnix` — фактическое время put'а объекта; TakenUnix — метка снятия из имени) + static `EtcdSnapshotMetaJson { string Serialize(EtcdSnapshotMeta); EtcdSnapshotMeta? Parse(string) }` (паттерн `EtcdSnapshotStatusJson`: private record `Payload` c `JsonPropertyName`, `JsonIgnoreCondition.WhenWritingNull`).

- [x] **Step 2.7: Failing-тесты ретенции** — `src/tests/PgWorker.UnitTests/Backups/EtcdExportRetentionTests.cs`:

```csharp
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Ретенция S3-выгрузки etcd (t08, arch/19 §5): пары .db+.meta.json сортируются
// по id (таймстемп лексикографически = хронологически), старше N последних —
// к удалению; одиночный .meta.json без .db — мусор; guard ≥1 полной пары.
public class EtcdExportRetentionTests
{
    private static S3ObjectInfo Obj(string key, long size = 100) => new(key, size, DateTimeOffset.UnixEpoch);

    // AAA: 3 пары при N=2 — старейшая пара (db+meta) к удалению, ровно 2 остаются
    [Fact]
    public void Select_сверх_лимита_старейшая_пара_к_удалению()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json"),
            Obj("etcd/snapshot-20261001-020000.db"), Obj("etcd/snapshot-20261001-020000.meta.json"),
            Obj("etcd/snapshot-20261001-030000.db"), Obj("etcd/snapshot-20261001-030000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, retentionObjects: 2);

        // Assert
        stale.Should().BeEquivalentTo(new[] { "etcd/snapshot-20261001-010000.db", "etcd/snapshot-20261001-010000.meta.json" });
    }

    // AAA: пар не больше лимита — ничего не удаляется (guard ≥1 по построению)
    [Fact]
    public void Select_в_пределах_лимита_пусто()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 28);

        // Assert
        stale.Should().BeEmpty();
    }

    // AAA: одиночный .meta.json без .db — мусор ретенции, сносится даже в пределах лимита
    [Fact]
    public void Select_одиночная_meta_мусор()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json"),
            Obj("etcd/snapshot-20261001-003000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 28);

        // Assert — пара цела, сирота-meta снесена
        stale.Should().BeEquivalentTo(new[] { "etcd/snapshot-20261001-003000.meta.json" });
    }

    // AAA: посторонние ключи префикса не трогаются
    [Fact]
    public void Select_чужие_ключи_не_трогает()
    {
        // Arrange
        var objects = new[] { Obj("etcd/README"), Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 1);

        // Assert
        stale.Should().NotContain("etcd/README");
    }
}
```

- [x] **Step 2.8: Реализация** — `src/PgWorker.Backups/EtcdExport/EtcdExportRetention.cs`:

```csharp
using PgWorker.Backups;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Чистый отбор ретенции S3-выгрузки etcd (t08, arch/19 §5): из list
/// префикса etcd/ выбирает ключи к batch-delete — объекты пар, старше N последних
/// (сортировка id по Ordinal = хронология), и одиночные .meta.json без .db (мусор).
/// Guard по построению: удаляется только старее оставляемых — ≥1 полная пара
/// остаётся всегда (при N ≥ 1, валидация старта).</summary>
public static class EtcdExportRetention
{
    public const string Prefix = "etcd/";

    /// <summary>Ключи к удалению. Пары задают id (файл .db); id вне последних
    /// retentionObjects — вся пара (db+meta); meta без пары — всегда.</summary>
    public static IReadOnlyList<string> Select(IReadOnlyList<S3ObjectInfo> objects, int retentionObjects)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        var metaWithoutDb = new List<string>();
        foreach (var o in objects)
        {
            var name = o.Key.StartsWith(Prefix, StringComparison.Ordinal)
                ? o.Key[Prefix.Length..]
                : null;
            if (name is null)
                continue; // чужой ключ — не наш префикс (посторонний не трогаем)
            if (name.EndsWith(".meta.json", StringComparison.Ordinal))
            {
                var id = name[..^".meta.json".Length];
                if (!objects.Any(x => x.Key == $"{Prefix}{id}.db"))
                    metaWithoutDb.Add(o.Key); // сирота-meta — мусор
            }
            else if (name.EndsWith(".db", StringComparison.Ordinal))
                ids.Add(name[..^".db".Length]);
        }

        var stale = new List<string>(metaWithoutDb);
        foreach (var id in ids.Take(Math.Max(0, ids.Count - retentionObjects)))
        {
            stale.Add($"{Prefix}{id}.db");
            stale.Add($"{Prefix}{id}.meta.json");
        }

        return stale;
    }
}
```

(`S3ObjectInfo` уже в namespace `PgWorker.Backups` — using одноимённый допустим опустить при warning'е CA/IDE; сборка без warnings обязательна.)

- [x] **Step 2.9: Гвард изоляции префикса** — дописать в `src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs` один Fact:

```csharp
// AAA (t08): объекты etcd/* не группируются реестром сирот — префикс служебный,
// не форма <C>/<X>/ (ключи короче 3 сегментов); реестр пуст (spec §3.2, AC8)
[Fact]
public void GroupPrefixes_объекты_etcd_мимо_реестра()
{
    // Arrange
    var objects = new[]
    {
        new S3ObjectInfo("etcd/snapshot-20261001-010000.db", 2048, DateTimeOffset.UnixEpoch),
        new S3ObjectInfo("etcd/snapshot-20261001-010000.meta.json", 100, DateTimeOffset.UnixEpoch),
    };

    // Act
    var grouping = OrphanRegistry.GroupPrefixes(objects);

    // Assert — ни размеров, ни fulls: префикс вне группировки
    grouping.Sizes.Should().BeEmpty();
    grouping.FullPrefixes.Should().BeEmpty();
}
```

- [x] **Step 2.10: Run all** — `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~EtcdExport|FullyQualifiedName~OrphanRegistry"` — PASS.

- [x] **Step 2.11: Commit**

```bash
git add src/PgWorker.Backups/EtcdExport/ src/tests/PgWorker.UnitTests/Backups/
git commit -m "feat(backups): t08 — чистые функции экспорта: статус-JSON /pgworker/etcd-snapshots (OK/FAILED с сохранением полей успеха), meta-JSON слепка, ретенционный отбор EtcdExportRetention (пары/сироты-meta/guard), IsBehind/таймстемп имени; гвард GroupPrefixes: etcd/* мимо реестра сирот"
```

---

## Task 3: Shared.Etcd — ISnapshotSink + SnapshotJob (spec §3.4)

**Вход:** Task 2 смержён (типы PgWorker.Backups.EtcdExport существуют — понадобятся только в Task 5; этот таск не зависит от них).
**Действие:** новый интерфейс `ISnapshotSink`; опциональный параметр `sink` в `SnapshotJob`; вызов после записи локального файла и локальной ретенции; ревизия best-effort (`StatusAsync`-failover, паттерн `MaintainAsync`); сбой sink НЕ роняет `TakeAsync`.
**Выход:** контракт sink + расширение SnapshotJob с обратной совместимостью (Kafka/Valkey-фабрики не меняются).
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~SnapshotJob` — зелёные; `dotnet build src/PgWorker.slnx -c Debug` — без warnings (фабрики Kafka/Valkey компилируются без правок).
**Связь со spec:** §3.4, §2.2/§2.3 (экспорт в точке снятия; сбой не роняет снятие), AC1/AC3.

- [x] **Step 3.1: Failing-тесты** — создать `src/tests/Shared.Etcd.UnitTests/Maintenance/SnapshotJobSinkTests.cs`. Мини-fake gateway — inline в файле (в Shared.Etcd.UnitTests фейка нет, EtcdGatewayTests гоняет живое API):

```csharp
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Maintenance;
using FluentAssertions;
using Xunit;

namespace Shared.Etcd.UnitTests.Maintenance;

// SnapshotJob + опциональный sink (t08, spec §3.4): экспорт вызывается после
// записи локального файла с best-effort ревизией; сбой sink (Result.Failed и
// исключение) НЕ роняет снятие; sink=null — прежнее поведение.
public class SnapshotJobSinkTests
{
    private const string Ep = "http://etcd:2379";

    private sealed class FakeEtcd(byte[] snapshot) : IEtcdGateway
    {
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success(snapshot));
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new EtcdStatusPayload(null, null, null, null, null, 77)));
        // остальные члены интерфейса — Success-заглушки (не нужны сценарию)
        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string e, string p, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([]));
        public Task<Result<Kv?>> GetAsync(string e, string k, CancellationToken ct) => Task.FromResult(Result<Kv?>.Success(null));
        public Task<Result> PutAsync(string e, string k, string v, long? l, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> DeleteAsync(string e, string k, bool p, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<TxnResult>> TxnAsync(string e, TxnRequest r, CancellationToken ct) => Task.FromResult(Result<TxnResult>.Success(new TxnResult(true)));
        public Task<Result<long>> LeaseGrantAsync(string e, int t, CancellationToken ct) => Task.FromResult(Result<long>.Success(1));
        public Task<Result> LeaseRevokeAsync(string e, long l, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> LeaseKeepaliveAsync(string e, long l, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string e, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string e, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));
        public Task<Result> CompactAsync(string e, long r, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> DefragmentAsync(string e, CancellationToken ct) => Task.FromResult(Result.Success());
    }

    private sealed class RecordingSink : ISnapshotSink
    {
        public List<(string FileName, byte[] Data, long? Revision)> Calls { get; } = [];
        public bool Throw { get; set; }
        public Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
        {
            if (Throw) throw new ApplicationException("sink взорвался");
            Calls.Add((snapshotFileName, data, revision));
            return Task.FromResult(Result.Success());
        }
    }

    private static string TempDir() => Directory.CreateTempSubdirectory("snap-sink-").FullName;

    // AAA: снятие вызывает sink именем файла, байтами и ревизией StatusAsync
    [Fact]
    public async Task TakeAsync_вызывает_sink_после_локальной_записи()
    {
        // Arrange
        var sink = new RecordingSink();
        var job = new SnapshotJob(new FakeEtcd([1, 2, 3]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert — путь локального файла (прежний контракт) + вызов sink
        shot.IsSuccess.Should().BeTrue();
        File.Exists(shot.Value).Should().BeTrue();
        sink.Calls.Should().ContainSingle(c => c.FileName.StartsWith("snapshot-") && c.FileName.EndsWith(".db")
            && c.Data.SequenceEqual(new byte[] { 1, 2, 3 }) && c.Revision == 77);
    }

    // AAA: Result.Failed sink не роняет снятие (локальный файл — истина снятия)
    [Fact]
    public async Task TakeAsync_сбой_sink_не_роняет_снятие()
    {
        // Arrange
        var sink = new FailingSink();
        var job = new SnapshotJob(new FakeEtcd([1]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue("сбой выгрузки — транзиент, доводка догонит (spec §2.3)");
        File.Exists(shot.Value).Should().BeTrue();
    }

    // AAA: исключение реализации sink — тоже не роняет снятие
    [Fact]
    public async Task TakeAsync_исключение_sink_не_роняет_снятие()
    {
        // Arrange
        var sink = new RecordingSink { Throw = true };
        var job = new SnapshotJob(new FakeEtcd([1]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue();
    }

    // AAA: sink=null — прежнее поведение (KafkaWorker/ValkeyWorker)
    [Fact]
    public async Task TakeAsync_без_sink_пишет_файл()
    {
        // Arrange
        var job = new SnapshotJob(new FakeEtcd([9]), [Ep], TempDir(), 10, 60);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue();
        await File.ReadAllBytesAsync(shot.Value, TestContext.Current.CancellationToken).Should().BeEquivalentTo([9].AsMemory());
    }

    private sealed class FailingSink : ISnapshotSink
    {
        public Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
            => Task.FromResult(Result.Failed(new ApplicationException("S3 недоступен")));
    }
}
```

(Строку `File.ReadAllBytesAsync...Should().BeEquivalentTo` при реализации заменить на прямой `Assert`-стиль проекта: `var bytes = await File.ReadAllBytesAsync(...); bytes.Should().Equal(9);` — главное: содержимое файла равно.)

- [x] **Step 3.2: Run → FAIL** (типа `ISnapshotSink` и перегрузки нет).

- [x] **Step 3.3: Реализация** — создать `src/Shared.Etcd/Maintenance/ISnapshotSink.cs`:

```csharp
using Shared.Core;

namespace Shared.Etcd.Maintenance;

/// <summary>Получатель выгрузки etcd-слепка во внешнее хранилище (t08, spec §3.4).
/// Вызывается SnapshotJob.TakeAsync сразу после записи локального файла и
/// локальной ретенции — каждый слепок (плановый и внеочередной) уезжает целиком.
/// Контракт: попытка доставить; Result — успех/транзиент-отказ (статус-ключ,
/// ретенция — внутри реализации); сбой НЕ роняет снятие (локальный слепок —
/// истина). Бюджет времени попытки — забота реализации (TimeoutSec).</summary>
public interface ISnapshotSink
{
    Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct);
}
```

  Править `src/Shared.Etcd/Maintenance/SnapshotJob.cs`: primary-конструктор получает 6-й параметр `ISnapshotSink? sink = null`; в `TakeAsync` после `ApplyRetention()` (до `return Result<string>.Success(path)`):

```csharp
// Экспорт слепка в S3 (t08, spec §3.4): каждый слепок — плановый и внеочередной.
// Ревизия — best-effort (failover по endpoints, паттерн MaintainAsync): не
// снялась — null, слепок не виноват. Сбой sink НЕ роняет снятие (принцип 3
// spec §2): статус пишет сам sink, доводка SnapshotLoop догонит.
if (sink is not null)
{
    try
    {
        long? revision = null;
        foreach (var endpoint in endpoints)
        {
            var status = await etcd.StatusAsync(endpoint, ct);
            if (status.IsSuccess)
            {
                revision = (long?)status.Value.Revision;
                break;
            }
        }

        await sink.ExportAsync(Path.GetFileName(path), data, revision, ct);
    }
    catch
    {
        // транзиент экспорта не мешает снятию; ошибка — в статус-ключе sink'а
    }
}
```

  Doc-комментарий класса дополнить строкой про опциональный sink.

- [x] **Step 3.4: Run → PASS; сборка всего solution** — `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~SnapshotJobSink && dotnet build src/PgWorker.slnx -c Debug` (без warnings; фабрики Kafka/Valkey не менялись и компилируются).

- [x] **Step 3.5: Commit**

```bash
git add src/Shared.Etcd/Maintenance/ src/tests/Shared.Etcd.UnitTests/Maintenance/
git commit -m "feat(etcd): t08 — ISnapshotSink + опциональный sink в SnapshotJob: экспорт каждого слепка сразу после локальной записи (best-effort ревизия, сбой sink не роняет снятие); KafkaWorker/ValkeyWorker без изменений (параметр опциональный)"
```

---

## Task 4: IBackupS3.PutObjectAsync (spec §3.6)

**Вход:** Task 3 смержён.
**Действие:** один новый метод интерфейса + реализация (`ChecksumSHA256` — серверная проверка целостности: MinIO отвергает put при несовпадении) + интеграционный тест на живом OwnMinio.
**Выход:** put байтов с sha256-проверкой транспорта доступен sink'у.
**Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~BackupS3Tests"` — зелёные; после серии — зачистка (`docker ps -a | grep pgw-em-` пусто).
**Связь со spec:** §3.6 (PutObjectAsync), §3.2 (sha256-мета/проверка целостности), AC1/AC5.

- [x] **Step 4.1: Failing-интеграция** — дописать в `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs` (паттерн файла — OwnMinio на Fact, SeedClient уже есть):

```csharp
// AAA (t08): put байтов с sha256 — объект читается обратно 1:1 (транспорт цел)
[Fact]
public async Task PutObject_байты_доставлены_1к1()
{
    // Arrange — своё MinIO-окружение Fact'а
    var ct = TestContext.Current.CancellationToken;
    await using var minio = await OwnMinio.StartAsync("s3put", ct);
    await using var s3 = new BackupS3(minio.Runtime());
    var data = new byte[] { 1, 2, 3, 4, 5 };

    // Act
    var put = await s3.PutObjectAsync("etcd/snapshot-test.db", data, Sha256Hex(data), ct);

    // Assert — прямой клиент читает те же байты
    put.IsSuccess.Should().BeTrue();
    using var client = SeedClient(minio);
    var response = await client.GetObjectAsync(new GetObjectRequest { BucketName = OwnMinio.Bucket, Key = "etcd/snapshot-test.db" }, ct);
    using var ms = new MemoryStream();
    await response.ResponseStream.CopyToAsync(ms);
    ms.ToArray().Should().Equal(data);
}

// AAA (t08): несовпадающий sha256 — MinIO отвергает put (BadDigest) → Failed
[Fact]
public async Task PutObject_битый_ша_отклонён_сервером()
{
    // Arrange
    var ct = TestContext.Current.CancellationToken;
    await using var minio = await OwnMinio.StartAsync("s3bad", ct);
    await using var s3 = new BackupS3(minio.Runtime());

    // Act
    var put = await s3.PutObjectAsync("etcd/snapshot-test.db", [1, 2, 3], "0000…нет-такого-хеша", ct);

    // Assert — серверная проверка целостности отклонила (не Result.Success)
    put.IsSuccess.Should().BeFalse("сервер обязан отвергнуть put с неверной чексуммой");
}

private static string Sha256Hex(byte[] data)
    => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(data)).ToLowerInvariant();
```

  (Хеш `"0000…"` в тесте — реальная 64-символьная hex-строка нулей/неверного значения; подставить корректную длину.)

- [x] **Step 4.2: Run → FAIL** (метода нет — не компилируется).

- [x] **Step 4.3: Реализация** — `src/PgWorker.Backups/BackupS3.cs`: в `IBackupS3` после `DeleteKeysAsync`:

```csharp
/// <summary>put байтов с SHA256-проверкой целостности транспорта (t08:
/// экспорт etcd-слепков): ChecksumSHA256 уходит с запросом — сервер сверяет
/// содержимое и отвергает put при несовпадении (BadDigest); успех = байты
/// доставлены целыми. sha256=null — без проверки (мелкие служебные объекты).</summary>
Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default);
```

  В `BackupS3` (рядом с `DeleteKeysAsync`):

```csharp
public async Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
{
    try
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            InputStream = new MemoryStream(data),
            ChecksumSHA256 = sha256,
        };
        await _client.PutObjectAsync(request, ct);
        return Result.Success();
    }
    catch (Exception e)
    {
        return Result.Failed(new ApplicationException($"S3 put {key}: {e.Message}", e));
    }
}
```

- [x] **Step 4.4: Run → PASS** (docker-серия; после — зачистка проверена teardown'ом OwnMinio + `docker ps -a --filter name=pgw-em-` пусто).

- [x] **Step 4.5: Commit**

```bash
git add src/PgWorker.Backups/BackupS3.cs src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs
git commit -m "feat(backups): t08 — IBackupS3.PutObjectAsync: put байтов с серверной SHA256-проверкой (ChecksumSHA256/BadDigest), интеграция на живом MinIO"
```

---

## Task 5: EtcdSnapshotSink + интеграции (spec §3.6, Фаза 2)

**Вход:** Task 2–4 смержены.
**Действие:** `EtcdSnapshotSink` (путь один, без ветви сравнения с прошлым состоянием — §3.6: put db → put meta → ретенция → статус OK; неудача → статус FAILED + `Result.Failed`; `CatchUpAsync` — доводка новейшего локального); интеграции на OwnEtcd+OwnMinio, вкл. `etcdctl snapshot status` через docker exec.
**Выход:** полный S3-конвейер выгрузки с доказательством разворачиваемости и операторского барьера (AC5: валидный слепок — положительный `snapshot status`; порча байта — провал sha256-сверки с `.meta.json` и `etcdutl snapshot restore`).
**Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~EtcdSnapshotSink"` — зелёные; в `ExportAsync` нет ветви сравнения sha с прошлым состоянием (явный запрет §3.6); зачистка серий после прогона.
**Связь со spec:** §3.1/§3.2/§3.6, §2.6/§2.7/§2.8 (идемпотентность/каждый слепок уезжает/takeover), §4 Ф2 (верификация: валидный — `snapshot status`, порча — sha256 + `etcdutl restore`), AC1–AC6 (AC5 — эхо новой буквы: статус на валидном, порча байта — sha256-расхождение + провал restore, только интеграция).

- [x] **Step 5.1: Реализация** — создать `src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs`:

```csharp
using System.Security.Cryptography;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Maintenance;

namespace PgWorker.Backups.EtcdExport;

/// <summary>S3-sink снапшотов etcd (t08, spec §3.6): путь ОДИН, без сравнения
/// с прошлым состоянием — каждый снятый слепок уезжает новой парой (§2.7):
/// put etcd/&lt;имя&gt;.db (серверная SHA256-проверка транспорта) → put
/// .meta.json → ретенция N пар (EtcdExportRetention) → статус-ключ OK с
/// семантикой покрытия: last_uploaded_unix = метка ПОКРЫВАЕМОГО слепка (taken
/// из имени, не now — фактическое время заливки фиксируется только
/// meta.uploaded_unix; инвариант IsBehind §3.5 п.1); last_object — объект
/// этой выгрузки, реально лежащий в S3; last_sha256 — наблюдаемость, в
/// решении о put не участвует. Любая неудача — статус FAILED + error (поля
/// последнего успеха сохраняются) и Result.Failed: S3-транспорт всегда
/// транзиент, догоняет доводка SnapshotLoop (CatchUpAsync). Все шаги
/// идемпотентны (put поверх; коллизия имён в одну секунду — новее поверх).
/// Единый бюджет timeoutSec — на всю попытку ExportAsync (linked-CTS
/// CancelAfter: puts+ретенция+статус суммарно; транзиент не тормозит снятие).</summary>
public sealed class EtcdSnapshotSink(
    IBackupS3 s3,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    int retentionObjects,
    int timeoutSec,
    string instanceId,
    int intervalMin) : ISnapshotSink
{
    public async Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
    {
        // Шаги под бюджетом таймаута: единый linked-токен на попытку.
        using var step = CancellationTokenSource.CreateLinkedTokenSource(ct);
        step.CancelAfter(TimeSpan.FromSeconds(timeoutSec));

        var sha = Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

        // Прошлый статус — ТОЛЬКО для FAILED-ветки (перенос полей последнего
        // успеха, §3.6); в решении о put не участвует: каждый слепок уезжает
        // новой парой (§2.7 — ветви сравнения sha с прошлым состоянием нет).
        var last = await ReadStatusAsync(step.Token);
        if (!last.IsSuccess)
            return await FailAsync(last.Value, last.Error!, step.Token);

        // Метки (§3.1/§3.2): taken — метка ПОКРЫВАЕМОГО слепка (из имени;
        // нераспознанное имя — now), uploaded — фактическое время put'а
        // (живёт только в meta.uploaded_unix, НЕ в статус-ключе).
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var taken = EtcdSnapshotStatus.TakenUnixFromName(snapshotFileName) ?? now;

        var objKey = $"{EtcdExportRetention.Prefix}{snapshotFileName}"; // etcd/snapshot-<id>.db
        var put = await s3.PutObjectAsync(objKey, data, sha, step.Token);
        if (!put.IsSuccess)
            return await FailAsync(last.Value, put.Error!, step.Token);

        var meta = EtcdSnapshotMetaJson.Serialize(
            new EtcdSnapshotMeta(sha, data.Length, revision, taken, now, instanceId));
        // Мета-ключ — по id БЕЗ ".db": канон §3.2 etcd/snapshot-<id>.meta.json.
        // НЕ $"{objKey}.meta.json": ключ snapshot-<id>.db.meta.json ретенция
        // (EtcdExportRetention.Select) разберёт как мету с id snapshot-<id>.db
        // без пары .db.db → сирота → снос первым же ретенционным проходом.
        var metaPut = await s3.PutObjectAsync(
            $"{EtcdExportRetention.Prefix}{snapshotFileName[..^".db".Length]}.meta.json",
            System.Text.Encoding.UTF8.GetBytes(meta), null, step.Token);
        if (!metaPut.IsSuccess)
            return await FailAsync(last.Value, metaPut.Error!, step.Token);

        // Ретенция: неудача — транзиент (следующий слепок пересоберёт); снятие
        // не откатываем, но статус честно FAILED — доводка повторит.
        var retention = await ApplyRetentionAsync(step.Token);
        if (!retention.IsSuccess)
            return await FailAsync(last.Value, retention.Error!, step.Token);

        // Статус OK с семантикой ПОКРЫТИЯ: last_uploaded_unix = taken (не now) —
        // инвариант IsBehind без рваных сравнений (§3.6).
        var statusPut = await PutStatusAsync(EtcdSnapshotStatusJson.Ok(
            taken, objKey, sha, data.Length, intervalMin), step.Token);
        if (!statusPut.IsSuccess)
            return Result.Failed(statusPut.Error!); // статус не записан — устареет (алерт stale)

        return Result.Success();
    }

    /// <summary>Доводка отстающей выгрузки (spec §3.5 п.1/п.3): отставание — по
    /// СТАТУС-ключу (EtcdSnapshotStatus.IsBehind: FAILED, либо ключа нет, либо
    /// новейший локальный снят позже подтверждённого покрытия), а не по факту
    /// доводки. Возврат: true — выгрузка отстаёт (в т.ч. FAILED при ПУСТОМ томе:
    /// догонять нечего, но короткий сон RetryIntervalSec важнее полного
    /// интервала — сигнал оператору); false — здорова (безделье). При отставании
    /// новейший локальный слепок (если есть) перечитывается и уезжает тем же
    /// конвейером — успешный проход продвигает last_uploaded_unix и закрывает
    /// отставание (§3.5 п.1); промежуточные невыгруженные слепки — нижние
    /// ревизии того же ряда, восстановление интересует только
    /// последний. Ошибка re-export → Failed (за SnapshotLoop — короткий сон).</summary>
    public async Task<Result<bool>> CatchUpAsync(string snapshotsDir, CancellationToken ct)
    {
        var last = await ReadStatusAsync(ct);
        if (!last.IsSuccess)
            return Result<bool>.Failed(last.Error!);
        var latest = LatestLocalFile(snapshotsDir);
        if (!EtcdSnapshotStatus.IsBehind(
                last.Value, EtcdSnapshotStatus.TakenUnixFromName(Path.GetFileName(latest ?? ""))))
            return Result<bool>.Success(false); // здорова — безделье

        if (latest is null)
            return Result<bool>.Success(true); // отстаёт (FAILED), догонять нечего

        var data = await File.ReadAllBytesAsync(latest, ct);
        var exported = await ExportAsync(Path.GetFileName(latest), data, revision: null, ct);
        return exported.IsSuccess
            ? Result<bool>.Success(true)
            : Result<bool>.Failed(exported.Error!);
    }

    /// <summary>Чтение статус-ключа (failover по endpoints, паттерн WalStatusWriter);
    /// null-значение = ключа ещё нет.</summary>
    public async Task<Result<EtcdSnapshotStatus?>> ReadStatusAsync(CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in etcdEndpoints)
        {
            var got = await etcd.GetAsync(endpoint, EtcdSnapshotStatusJson.Key, ct);
            if (!got.IsSuccess)
            {
                last = got; // отказ — следующий endpoint
                continue;
            }

            return got.Value is not { } kv
                ? Result<EtcdSnapshotStatus?>.Success(null)
                : Result<EtcdSnapshotStatus?>.Success(EtcdSnapshotStatusJson.Parse(kv.Value));
        }

        return Result<EtcdSnapshotStatus?>.Failed(last?.Error
            ?? new ApplicationException("нет живых endpoints etcd"));
    }

    // Ретенция: list префикса → чистый отбор → batch-delete.
    private async Task<Result> ApplyRetentionAsync(CancellationToken ct)
    {
        var listed = await s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct: ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!);
        var stale = EtcdExportRetention.Select(listed.Value, retentionObjects);
        return stale.Count == 0
            ? Result.Success()
            : await s3.DeleteKeysAsync(stale, ct);
    }

    // Неудача: статус FAILED + error, поля последнего успеха сохраняются.
    // Отказ записи статуса — не маскирует исходную ошибку (алерт stale заметит).
    private async Task<Result> FailAsync(EtcdSnapshotStatus? lastOk, Exception error, CancellationToken ct)
    {
        await PutStatusAsync(EtcdSnapshotStatusJson.Failed(lastOk, error.Message, intervalMin), ct);
        return Result.Failed(error);
    }

    private async Task<Result> PutStatusAsync(string json, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in etcdEndpoints)
        {
            var put = await etcd.PutAsync(endpoint, EtcdSnapshotStatusJson.Key, json, lease: null, ct);
            if (put.IsSuccess)
                return Result.Success();
            last = put;
        }

        return Result.Failed(last?.Error ?? new ApplicationException("нет живых endpoints etcd"));
    }

    // Новейший локальный слепок (имя — таймстемп, Ordinal-сортировка = время).
    // public (круг 7): call-сайт за пределами сборки — SnapshotLoop
    // (PgWorker.App) повторно считает отставание после TakeAsync (T7 Step 7.3).
    public static string? LatestLocalFile(string dir)
        => Directory.Exists(dir)
            ? Directory.GetFiles(dir, "snapshot-*.db").OrderByDescending(f => f, StringComparer.Ordinal).FirstOrDefault()
            : null;
}
```

- [x] **Step 5.2: OwnEtcd — публичное имя контейнера** (нужно для `docker exec etcdctl`): в `src/tests/PgWorker.IntegrationTests/Backups/OwnEtcd.cs` добавить `public string ContainerName => …;` (сейчас приватное авто-свойство `private string ContainerName => $"pgw-ee-{RunId}";` — сменить модификатор на `public`).

- [x] **Step 5.3: Интеграции** — создать `src/tests/PgWorker.IntegrationTests/Backups/EtcdSnapshotSinkTests.cs`. Окружение Fact'а: `await using var etcdFx = await OwnEtcd.StartAsync("sink", ct); await using var minioFx = await OwnMinio.StartAsync("sink", ct);` (обе фикстуры — teardown+ассерт чистоты). Клиенты: `var s3 = new BackupS3(minioFx.Runtime());` — НО: `Runtime()` хост-эндпоинт MinIO, а `etcdFx.Endpoint` — для шлюза. Хелпер-заголовок файла:

```csharp
// S3-sink снапшотов etcd (t08): OwnEtcd + OwnMinio на Fact (своё окружение,
// динамические порты, teardown при любом исходе — e2e-isolation §1/§3).
// Сценарии: экспорт после TakeAsync, каждый слепок новой парой, ретенция,
// транзиент → FAILED → доводка, верификация целостности (валидный слепок —
// snapshot status; порча байта — sha256-расхождение + провал etcdutl restore;
// круг 7/AC5), takeover.
public class EtcdSnapshotSinkTests
{
    private static BackupsRuntimeOptions BadS3Runtime(OwnMinio minio)
        => minio.Runtime() with { S3Endpoint = $"http://localhost:{E2eFixture.FreePort()}" }; // закрытый порт — транзиент

    private static async Task<EtcdSnapshotSink> SinkAsync(OwnEtcd etcd, OwnMinio minio, string instance,
        int retention = 28, CancellationToken ct = default)
    {
        var s3 = new BackupS3(minio.Runtime());
        return new EtcdSnapshotSink(s3, etcd.Gateway, [etcd.Endpoint], retention, 5, instance, 360);
    }
    // … Fact'ы ниже
}
```

  Fact'ы (каждый — AAA; первый — полный код, остальные по той же схеме с точными ассертами):

  1. **`Export_после_TakeAsync_объекты_мета_статус_OK`** (AC1 + AC8-хвост used_bytes) — полный код:

```csharp
[Fact]
public async Task Export_после_TakeAsync_объекты_мета_статус_OK()
{
    // Arrange — своё etcd + MinIO; sink; SnapshotJob с sink'ом
    var ct = TestContext.Current.CancellationToken;
    await using var etcdFx = await OwnEtcd.StartAsync("sink1", ct);
    await using var minioFx = await OwnMinio.StartAsync("sink1", ct);
    var dir = Directory.CreateTempSubdirectory("sink-export-").FullName;
    var sink = await SinkAsync(etcdFx, minioFx, "inst-A");
    var job = new Shared.Etcd.Maintenance.SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

    // Act
    var shot = await job.TakeAsync(ct);

    // Assert — снятие успешно; в S3 ровно пара db+meta; статус OK с фактом
    shot.IsSuccess.Should().BeTrue();
    var s3 = new BackupS3(minioFx.Runtime());
    var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
    listed.Should().HaveCount(2).And.Contain(k => k.Key.EndsWith(".db")).And.Contain(k => k.Key.EndsWith(".meta.json"));
    // Assert — канон нейминга §3.2 (фиксация от регрессии): мета-ключ
    // etcd/snapshot-<id>.meta.json — БЕЗ «.db»; ключ «.db.meta.json» ретенция
    // (EtcdExportRetention) сочла бы сиротой-метой и снесла бы первым проходом.
    listed.Should().Contain(k => k.Key.EndsWith(".meta.json"))
        .And.NotContain(k => k.Key.EndsWith(".db.meta.json"));
    var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
    var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
    status.State.Should().Be("OK");
    status.IntervalMin.Should().Be(360);
    status.LastSha256.Should().Be(Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
        await File.ReadAllBytesAsync(shot.Value, ct))).ToLowerInvariant());
    // Assert (AC8-хвост): объекты etcd/* видны list'ом ВСЕГО bucket (префикс "")
    // — база used_bytes ключа storage (t06 считает list ""; ср. BackupS3Tests:189)
    var wholeBucket = (await s3.ListPrefixAsync("", ct: ct)).Value!;
    wholeBucket.Select(o => o.Key).Should().Contain(listed.Select(o => o.Key),
        "объекты etcd/ входят в used_bytes — list всего bucket их видит");
    // meta: taken <= uploaded, instance, ревизия best-effort есть (OwnEtcd жив)
    var metaRaw = /* прямой GetObject AWSSDK-клиентом по ключу etcd/snapshot-<id>.meta.json */;
    var meta = EtcdSnapshotMetaJson.Parse(metaRaw)!;
    meta.TakenUnix.Should().BeLessThanOrEqualTo(meta.UploadedUnix);
    meta.Instance.Should().Be("inst-A");
    meta.Revision.Should().NotBeNull();
}
```

  2. **`Каждый_слепок_выгружается_новой_парой`** (AC2: каденс не срывается) — два `TakeAsync`, между ними пауза 1.1 c (имена слепков расходятся по секундам: taken2 > taken1; содержимое слепков на живом etcd различно и без операторских изменений — статус-ключ пишется каждым экспортом, MVCC растёт). Assert: (а) в S3 4 ключа — ДВЕ пары db+meta (второй слепок уехал новой парой: объектов +2), локальных файлов 2; (б) статус `OK`, `last_uploaded_unix == taken2 > taken1` (покрытие продвинуто вторым слепком), `last_object` — имя ВТОРОГО слепка, `last_sha256`/`size_bytes` — его факт; (в) `EtcdSnapshotStatus.IsBehind(статус, taken2)` — false (отставания нет: сон лидера остаётся `SnapshotIntervalMin`, stale-алерт молчит).
  3. **`Ретенция_сверх_лимита_старейшие_пары_снесены`** — sink с `retention: 2`; 3 TakeAsync с паузами 1.1 c между ними (имена расходятся по секундам — каждый слепок уезжает своей парой). Assert: ровно 2 пары (4 ключа), старейшего id нет; сид одиночной `.meta.json` без `.db` (прямой AWSSDK-клиент) → четвёртый TakeAsync → сирота снесена (AC4).
  4. **`Транзиент_S3_не_роняет_снятие_статус_FAILED`** — sink на `BadS3Runtime` (закрытый порт). Act: TakeAsync. Assert: `shot.IsSuccess` true, локальный файл существует; статус-ключ: `State=="FAILED"`, `Error` не пуст (AC3).
  5. **`Доводка_CatchUp_выгружает_новейший_локальный`** — после сценария 4: живой sink (`SinkAsync` на реальном MinIO) → `CatchUpAsync(dir, ct)` → `Result<bool>` true; объекты в `etcd/` есть, статус OK, sha256 = SHA256(новейшего локального файла) (AC3).
  6. **`Верификация_целостности_status_и_restore`** (AC5; круг 7 — по обновлённой букве spec) — после успешного экспорта: (а) валидный слепок: скачать `.db` AWSSDK в temp-файл → `docker cp` в контейнер OwnEtcd → `docker exec … etcdctl snapshot status /tmp/snap.db` — положительный вердикт структуры/размера/ревизии (etcd 3.5.x печатает hash, но при status его НЕ сверяет — целостность им не доказывается); (б) порча байта (`data[100] ^= 0xFF`, перезаписать temp → cp поверх): sha256 файла расходится с `sha256` из `.meta.json` (ручная сверка — операторский барьер runbook-шага 2) И `docker exec … etcdutl snapshot restore /tmp/snap.db --data-dir=/tmp/restore-bad` проваливается (restore верифицирует sha256 при разворачивании — финальный барьер; exit ≠ 0 проверять обёрткой `sh -c "etcdutl … && echo OK"` и отсутствием "OK" в выводе) (AC5).
  7. **`Takeover_статус_пишет_инстанс_исполнитель`** — sink A (`instance:"inst-A"`) экспортирует слепок; пауза 1.1 c (имена расходятся по секундам); sink B (`instance:"inst-B"`, те же etcd+minio) экспортирует следующий слепок (новая пара). Assert: `.meta.json` новейшего объекта содержит `"instance":"inst-B"`; в S3 две пары; статус OK (AC6).

- [x] **Step 5.4: Run** — `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~EtcdSnapshotSink` — PASS; зачистка после серии (`docker ps -a --filter name=pgw-ee-` / `pgw-em-` пусто; при остаточных сетях — `docker network prune -f` по правилам AGENTS.md).

- [x] **Step 5.5: Commit**

```bash
git add src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs src/tests/PgWorker.IntegrationTests/Backups/EtcdSnapshotSinkTests.cs src/tests/PgWorker.IntegrationTests/Backups/OwnEtcd.cs
git commit -m "feat(backups): t08 — EtcdSnapshotSink: put db+meta (серверная проверка) → ретенция → статус OK/FAILED; CatchUpAsync-доводка новейшего локального; интеграции OwnEtcd+OwnMinio: экспорт/каждый слепок новой парой/ретенция/транзиент→FAILED→доводка/верификация: валидный слепок — etcdctl snapshot status; порча байта — sha256-расхождение + провал etcdutl snapshot restore/takeover"
```

---

## Task 6: Опции + fail-fast + Program-склейка (spec §3.3)

**Вход:** Task 5 смержен.
**Действие:** `SnapshotExportOptions` в `Options.cs`; `.Validate(...)` в Program.cs; фабрики: `SnapshotJob` получает sink, `EtcdSnapshotSink` регистрируется только при `Enabled=true`; секция в `appsettings.json`.
**Выход:** конфигурируемый экспорт с fail-fast; `Enabled=false` — sink не создаётся вовсе.
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~SnapshotExportOptions` + сборка solution без warnings.
**Связь со spec:** §3.3 (таблица опций/дефолты/валидация), §2.10 (нулевое влияние при false), AC10.

- [x] **Step 6.1: Failing-тесты валидации** — создать `src/tests/PgWorker.UnitTests/App/SnapshotExportOptionsTests.cs`:

```csharp
using PgWorker.App;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.App;

// Fail-fast включения экспорта etcd-снапшотов (t08, spec §3.3): Enabled=true
// требует полный S3-комплект из Backups:S3 (Backups:Enabled НЕ требуется);
// диапазоны — всегда; false — подсистема не активна (нулевое влияние, §2.10).
public class SnapshotExportOptionsTests
{
    private static BackupsS3Options S3(bool full = true) => new()
    {
        Endpoint = full ? "http://minio:9000" : "",
        Bucket = full ? "b" : "",
        AccessKey = full ? "k" : "",
        SecretKey = full ? "s" : "",
    };

    // AAA: дефолты — выключено, валидно при пустом S3 (нулевое влияние)
    [Fact]
    public void Выключено_по_умолчанию_валидно_без_S3()
    {
        // Act
        var valid = new SnapshotExportOptions().IsValid(S3(full: false));

        // Assert
        valid.Should().BeTrue();
    }

    // AAA: Enabled=true + полный S3 — валидно даже при Backups:Enabled=false
    [Fact]
    public void Включено_с_полным_S3_валидно()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true };

        // Act/Assert
        options.IsValid(S3()).Should().BeTrue("контроль-плейн не зависит от подсистемы бэкапов PG");
    }

    // AAA: Enabled=true + пустой S3-комплект — ошибка старта
    [Fact]
    public void Включено_без_S3_невалидно()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true };

        // Act/Assert
        options.IsValid(S3(full: false)).Should().BeFalse();
    }

    // AAA: диапазоны — всегда (мусорный конфиг виден на старте)
    [Theory]
    [InlineData(0, 300, 30)]   // RetentionObjects < 1
    [InlineData(28, 0, 30)]    // RetryIntervalSec <= 0
    [InlineData(28, 300, 0)]   // TimeoutSec <= 0
    public void Диапазоны_невалидны_всегда(int retention, int retrySec, int timeoutSec)
    {
        // Arrange
        var options = new SnapshotExportOptions
            { Enabled = false, RetentionObjects = retention, RetryIntervalSec = retrySec, TimeoutSec = timeoutSec };

        // Act/Assert
        options.IsValid(S3(full: false)).Should().BeFalse();
    }
}
```

- [x] **Step 6.2: Run → FAIL** (типа нет).

- [x] **Step 6.3: Реализация** — `src/PgWorker.App/Options.cs`: внутрь `SnapshotOptions` добавить `public SnapshotExportOptions Export { get; set; } = new();` и новый класс (после `SnapshotOptions`):

```csharp
/// <summary>Выгрузка снапшотов etcd в S3 (t08, arch/14 §8): каждый слепок уходит
/// в bucket подсистемы бэкапов префиксом etcd/ (S3-комплект — Backups:S3;
/// Backups:Enabled НЕ требуется — контроль-плейн не зависит от бэкапов PG).
/// Enabled=false (дефолт) — sink не подключается, поведение прежнее (§2.10).</summary>
public sealed class SnapshotExportOptions
{
    /// <summary>Выгрузка включена.</summary>
    public bool Enabled { get; set; }

    /// <summary>Сколько последних пар .db+.meta.json держать в S3 (≥1;
    /// 28 ≈ неделя при интервале 6 ч).</summary>
    public int RetentionObjects { get; set; } = 28;

    /// <summary>Сон лидера-тика при отстающей выгрузке, с (&gt;0;
    /// вместо полного SnapshotIntervalMin — RPO-окно транзиента закрывается минутами).</summary>
    public int RetryIntervalSec { get; set; } = 300;

    /// <summary>Единый бюджет ОДНОЙ попытки экспорта (&gt;0): ExportAsync целиком
    /// (puts + ретенция + статус, linked-CTS CancelAfter) — транзиент не тормозит
    /// фазы процессов кластеров.</summary>
    public int TimeoutSec { get; set; } = 30;

    /// <summary>Fail-fast старта (образец BackupsOptions.IsValid): Enabled=true
    /// требует полный S3-комплект Backups:S3; диапазоны — всегда.</summary>
    public bool IsValid(BackupsS3Options s3)
        => (!Enabled
            || (!string.IsNullOrWhiteSpace(s3.Endpoint)
                && !string.IsNullOrWhiteSpace(s3.Bucket)
                && !string.IsNullOrWhiteSpace(s3.AccessKey)
                && !string.IsNullOrWhiteSpace(s3.SecretKey)))
           && RetentionObjects >= 1
           && RetryIntervalSec > 0
           && TimeoutSec > 0;
}
```

  `src/PgWorker.App/Program.cs` — в цепочку `.Validate(...)` (после `Pgtune`):

```csharp
    // Экспорт etcd-снапшотов в S3 (reliability t08): Enabled=true требует
    // S3-комплект из Backups:S3 (Backups:Enabled не нужен); диапазоны — всегда.
    .Validate(o => o.Snapshots.Export.IsValid(o.Backups.S3),
        "PgWorker:Snapshots:Export: Enabled=true требует непустые PgWorker:Backups:S3:Endpoint/Bucket/AccessKey/SecretKey (env PGW_BACKUP_S3_*); RetentionObjects>=1, RetryIntervalSec>0, TimeoutSec>0")
```

  Фабрика `SnapshotJob` (Program.cs:261–268) — склейка через ЕДИНЫЙ синглтон sink (его берут обе фабрики — SnapshotJob и SnapshotLoop):

```csharp
// t08: S3-sink экспорта — один синглтон на процесс; null при выключенной опции
// (креды фиксируются на старте: env-секреты деплоя меняются recreate контейнера).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return opts.Snapshots.Export.Enabled
        ? new EtcdSnapshotSink(
            new BackupS3(opts.Backups.ToRuntime()),
            sp.GetRequiredService<IEtcdGateway>(),
            opts.Etcd.Endpoints,
            opts.Snapshots.Export.RetentionObjects,
            opts.Snapshots.Export.TimeoutSec,
            sp.GetRequiredService<ClaimStore>().InstanceId,
            opts.Loops.SnapshotIntervalMin)
        : null;
});

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений) + опциональный
// S3-sink (t08): Enabled=false → sink null, поведение прежнее (§2.10).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin,
        sp.GetService<EtcdSnapshotSink>());
});
```

  (Регистрация sink — ЕДИНСТВЕННЫЙ синглтон-блок выше; SnapshotLoop-фабрика в Task 7 берёт его тем же `sp.GetService<EtcdSnapshotSink>()` — может вернуть null при выключенной опции, это рабочий режим.) `appsettings.json` строку `"Snapshots": { "Dir": "/snapshots", "RetentionFiles": 10 }` заменить на `"Snapshots": { "Dir": "/snapshots", "RetentionFiles": 10, "MaintenanceIntervalMin": 60, "Export": { "Enabled": false, "RetentionObjects": 28, "RetryIntervalSec": 300, "TimeoutSec": 30 } }`.

- [x] **Step 6.4: Run → PASS + сборка**

```bash
dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~SnapshotExportOptions
dotnet build src/PgWorker.slnx -c Debug
```

- [x] **Step 6.5: Commit**

```bash
git add src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests/App/SnapshotExportOptionsTests.cs
git commit -m "feat(app): t08 — SnapshotExportOptions (Enabled/RetentionObjects/RetryIntervalSec/TimeoutSec) + fail-fast (S3-комплект из Backups:S3, Backups:Enabled не требуется) + Program-склейка sink (только при Enabled=true)"
```

---

## Task 7: SnapshotLoop — доводка и сон (spec §3.5)

**Вход:** Task 6 смержен (sink регистрируется, `CatchUpAsync`/`ReadStatusAsync` есть).
**Действие:** тик лидера: доводка перед снятием (`exportSink.CatchUpAsync(dir)`), повторный расчёт отставания ПОСЛЕ снятия по свежему статус-ключу (круг 7: транзиент S3 в момент `TakeAsync` не тянет полный сон), сон `RetryIntervalSec` при отставании, иначе `SnapshotIntervalMin` как раньше.
**Выход:** RPO-окно транзиента закрывается минутами (в т.ч. при первом транзиенте в самом тике снятия); takeover без изменений.
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~SnapshotLoopExport` — зелёные.
**Связь со spec:** §3.5 (п.1–3; п.3 — повторный расчёт отставания после TakeAsync: сон по устаревшему «здорово» запрещён), §2.8 (takeover наследуется), AC3.

- [x] **Step 7.1: Failing-тест** — создать `src/tests/PgWorker.UnitTests/App/SnapshotLoopExportTests.cs` (паттерн `KafkaWorker.UnitTests/App/LoopsHealthResetTests`; PgWorker.App internals видимы — `InternalsVisibleTo Include="PgWorker.UnitTests"` уже есть). Использовать `FakeEtcd` из `PgWorker.UnitTests.Provisioning` (`SnapshotSaveAsync` → `[1,2,3]`, `StatusAsync` → ревизия) и `FakeEtcdGateway` из `PgWorker.UnitTests.Api` как хранилище статус-ключа; S3-часть — мини-фейк `IBackupS3` в памяти:

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using FluentAssertions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using PgWorker.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.App;

// Доводка выгрузки в SnapshotLoop (t08, spec §3.5): тик лидера перед снятием
// догоняет отстающую выгрузку (статус FAILED/ключа нет/локальный новее) —
// re-export новейшего локального слепка; сон при отставании RetryIntervalSec
// (наблюдаемо: второй слепок в пределах тестового бюджета, не 360 мин).
public class SnapshotLoopExportTests
{
    // S3 в памяти: put/list/delete по словарю (для CatchUpAsync-конвейера sink'а);
    // PutObjectFails — инъекция отказа put (транзиент S3: статус FAILED держится).
    private sealed class MemoryS3 : IBackupS3
    {
        public Dictionary<string, byte[]> Objects { get; } = [];
        public bool PutObjectFails { get; set; }
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            if (PutObjectFails)
                return Task.FromResult(Result.Failed(new ApplicationException("S3 недоступен (инъекция)")));
            Objects[key] = data;
            return Task.FromResult(Result.Success());
        }
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? m = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
                Objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
                    .Select(k => new S3ObjectInfo(k, Objects[k].Length, DateTimeOffset.UnixEpoch)).ToList()));
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
        { foreach (var k in keys) Objects.Remove(k); return Task.FromResult(Result.Success()); }
        // не нужны сценарию
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct = default) => Task.FromResult(Result<bool>.Success(true));
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string c, string s, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string c, string s, string p, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
        public Task<Result<string>> GetObjectAsync(string c, string s, string k, CancellationToken ct = default) => Task.FromResult(Result<string>.Success(""));
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string c, string s, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result<string>> DownloadTextAsync(string c, string s, string k, CancellationToken ct = default) => Task.FromResult(Result<string>.Success(""));
    }

    private sealed class FixedMonitor(PgWorkerOptions value) : IOptionsMonitor<PgWorkerOptions>
    {
        public PgWorkerOptions CurrentValue => value;
        public IDisposable? OnChange(Action<PgWorkerOptions, string?> listener) => null;
        public PgWorkerOptions Get(string? name) => value;
    }

    // AAA: доводка перед снятием — локальный слепок с FAILED-статусом доезжает в S3
    [Fact]
    public async Task Тик_доводит_отстающую_выгрузку()
    {
        // Arrange — etcd-фейк (снимает [1,2,3]), каталог с уже снятым слепком,
        // статус-ключ FAILED (прошлый транзиент), sink на памяти
        var etcd = new Provisioning.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-export-").FullName;
        var fileName = $"snapshot-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.db";
        await File.WriteAllBytesAsync(Path.Combine(dir, fileName), [1, 2, 3], TestContext.Current.CancellationToken);
        await etcd.PutAsync("http://etcd:2379", EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "S3 был недоступен", 360), null, TestContext.Current.CancellationToken);
        var s3 = new MemoryS3();
        var sink = new EtcdSnapshotSink(s3, etcd, ["http://etcd:2379"], 28, 5, "inst-A", 360);

        var loop = BuildLoop(etcd, dir, sink, retrySec: 1, snapshotMin: 360);

        // Act — пара тиков лидера (доводка п.1 до TakeAsync)
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);
        var caught = await WaitUntilAsync(() => s3.Objects.ContainsKey($"etcd/{fileName}"));

        // Assert — новейший локальный файл доехал ДО/вместе со снятием; статус OK
        caught.Should().BeTrue("доводка обязана выгрузить новейший локальный слепок (spec §3.5 п.1)");
        await loop.StopAsync(CancellationToken.None);
    }

    // AAA: сон при отставании — RetryIntervalSec (второй слепок за секунды, не 360 мин):
    // статус FAILED держится отказом S3-put — доводка не чинит, каждый тик отстаёт.
    // Тик 1: CatchUpAsync(true: FAILED, том пуст — догонять нечего) → TakeAsync
    // слепок 1 (встроенный export падает, статус FAILED) → короткий сон 1 c →
    // тик 2: слепок 2. Второй файл = строгое доказательство короткого межтика.
    [Fact]
    public async Task Сон_при_отставании_RetryIntervalSec()
    {
        // Arrange — FAILED-статус, том пуст, S3-фейк с отказом put (статус не чинится)
        var etcd = new Provisioning.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-retry-").FullName;
        await etcd.PutAsync("http://etcd:2379", EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "транзиент", 360), null, TestContext.Current.CancellationToken);
        var s3 = new MemoryS3 { PutObjectFails = true };
        var sink = new EtcdSnapshotSink(s3, etcd, ["http://etcd:2379"], 28, 5, "inst-A", 360);

        var loop = BuildLoop(etcd, dir, sink, retrySec: 1, snapshotMin: 360);
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act — ждём ВТОРОЙ локальный слепок: первый тик снимает свой слепок ещё
        // до сна, второй возможен только при RetryIntervalSec=1 c (не 360 мин)
        var second = await WaitUntilAsync(() =>
            Directory.GetFiles(dir, "snapshot-*.db").Length >= 2, budgetMs: 15_000);

        // Assert
        second.Should().BeTrue("сон при отставании — RetryIntervalSec=1 c, не SnapshotIntervalMin=360");
        await loop.StopAsync(CancellationToken.None);
    }

    // AAA (крайний случай, §3.5 п.3): FAILED при ПУСТОМ томе — выгрузка отстаёт
    // (CatchUpAsync → true без re-export): сон короткий, не 360-минутный
    [Fact]
    public async Task CatchUp_пустой_том_FAILED_отстаёт()
    {
        // Arrange — FAILED-статус, локальных слепков нет (догонять нечего)
        var etcd = new Provisioning.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-empty-").FullName;
        await etcd.PutAsync("http://etcd:2379", EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "транзиент", 360), null, TestContext.Current.CancellationToken);
        var sink = new EtcdSnapshotSink(new MemoryS3(), etcd, ["http://etcd:2379"], 28, 5, "inst-A", 360);

        // Act
        var catchUp = await sink.CatchUpAsync(dir, TestContext.Current.CancellationToken);

        // Assert — отстаёт (true, без ошибки): SnapshotLoop уйдёт в RetryIntervalSec
        catchUp.IsSuccess.Should().BeTrue();
        catchUp.Value.Should().BeTrue("FAILED держит короткий сон даже при пустом томе");
    }

    // AAA (круг 7, §3.5 п.3): ПЕРВЫЙ транзиент S3 в момент TakeAsync — статус
    // ДО тика здоров (OK), слепок снят, встроенный экспорт падает и пишет
    // FAILED уже ПОСЛЕ расчёта «до» — повторный расчёт после TakeAsync обязан
    // дать короткий сон: второй слепок в пределах RetryIntervalSec, не 360 мин.
    // (Отличие от кейса Сон_при_отставании_RetryIntervalSec: там статус
    // предзасеян FAILED — короткий сон виден и по старому расчёту «до»;
    // здесь короткий сон доказывает ИМЕННО повторный расчёт после снятия.)
    [Fact]
    public async Task Первый_транзиент_в_тике_TakeAsync_сон_короткий()
    {
        // Arrange — ЗДОРОВЫЙ статус OK (прошлая выгрузка успешна, отставания
        // нет), том пуст, S3-фейк с отказом put: тик 1 снимет слепок,
        // встроенный экспорт упадёт — статус-ключ станет FAILED после снятия
        var etcd = new Provisioning.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-fresh-retry-").FullName;
        await etcd.PutAsync("http://etcd:2379", EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Ok(0, "etcd/snapshot-19700101-000000.db", "abc", 1, 360),
            null, TestContext.Current.CancellationToken);
        var s3 = new MemoryS3 { PutObjectFails = true };
        var sink = new EtcdSnapshotSink(s3, etcd, ["http://etcd:2379"], 28, 5, "inst-A", 360);

        var loop = BuildLoop(etcd, dir, sink, retrySec: 1, snapshotMin: 360);
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act — ждём ВТОРОЙ локальный слепок: CatchUpAsync до снятия вернул
        // «здорово» (OK, не отстаёт); FAILED появился только в момент
        // TakeAsync — короткий сон даёт повторный расчёт ПОСЛЕ снятия
        var second = await WaitUntilAsync(() =>
            Directory.GetFiles(dir, "snapshot-*.db").Length >= 2, budgetMs: 15_000);

        // Assert
        second.Should().BeTrue("первый транзиент в тике TakeAsync — повторный расчёт даёт сон RetryIntervalSec=1 c, не SnapshotIntervalMin=360 (spec §3.5 п.3)");
        await loop.StopAsync(CancellationToken.None);
    }

    private static SnapshotLoop BuildLoop(Provisioning.FakeEtcd etcd, string dir, EtcdSnapshotSink sink,
        int retrySec, int snapshotMin)
    {
        var options = new FixedMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 1, SnapshotIntervalMin = snapshotMin },
            Snapshots = new SnapshotOptions
            {
                Dir = dir,
                Export = new SnapshotExportOptions { Enabled = true, RetryIntervalSec = retrySec },
            },
        });
        var job = new Shared.Etcd.Maintenance.SnapshotJob(etcd, ["http://etcd:2379"], dir, 10, 60, sink);
        return new SnapshotLoop(
            options,
            new ClaimStore("/pgworker", ["http://etcd:2379"], etcd, TimeProvider.System),
            job, NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System), TimeProvider.System,
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System));
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> done, int budgetMs = 10_000)
    {
        for (var i = 0; i < budgetMs / 50 && !done(); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        return done();
    }
}
```

  Примечания: `Provisioning.FakeEtcd` — файл `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`, класс `internal sealed class FakeEtcd : IEtcdGateway` в namespace `PgWorker.UnitTests.Provisioning` (уточнить фактический namespace при реализации и импортировать). Если его `PutAsync`/`GetAsync` недостаточны (нет `GetAsync`-нюансов) — расширять фейк нельзя без нужды: статус-ключ пишется/читается через те же Put/Get — достаточно. `ClaimStore` — реальный (паттерн LoopsHealthResetTests).

- [x] **Step 7.2: Run → FAIL** (SnapshotLoop ещё не зовёт CatchUpAsync).

- [x] **Step 7.3: Реализация** — `src/PgWorker.App/Loops/SnapshotLoop.cs`: конструктор получает последний опциональный параметр `EtcdSnapshotSink? exportSink = null`; в ветке `if (claims.IsLeader)` — ПЕРЕД `TakeAsync`:

```csharp
// t08 (spec §3.5 п.1/п.3): доводка отстающей выгрузки ДО снятия — транзиент S3
// не растягивает RPO-окно до планового тика. CatchUpAsync возвращает отставание
// ПО СТАТУС-ключу (IsBehind: FAILED/ключа нет/локальный новее — включая FAILED
// при пустом томе, без re-export); ошибка доводки — как ошибка экспорта
// (статус-ключ пишется внутри sink) — тоже короткий сон.
var behind = false;
if (exportSink is not null)
{
    var catchUp = await exportSink.CatchUpAsync(
        options.CurrentValue.Snapshots.Dir, stoppingToken);
    behind = catchUp.IsSuccess ? catchUp.Value : true;
}
```

  ПОСЛЕ `TakeAsync` (при `exportSink != null`) — повторный расчёт отставания по СВЕЖЕМУ статус-ключу (круг 7, impl-major: расчёт «до» не видит транзиент S3 в момент самого снятия — статус-ключ становится FAILED уже после него; сон по устаревшему «здорово» тянул бы полный `SnapshotIntervalMin` вопреки §3.5 п.3):

```csharp
// t08 (круг 7, spec §3.5 п.3): повторный расчёт ПОСЛЕ снятия — первый
// транзиент S3 в тике TakeAsync переводит статус-ключ в FAILED уже ПОСЛЕ
// расчёта «до»; без пере-расчёта лидер ушёл бы в полный SnapshotIntervalMin
// с невыгруженным слепком (запрещено §3.5 п.3). Считаем по свежему
// статус-ключу и метке новейшего локального слепка (тот самый, только что
// снятый); отказ чтения ключа — трактуем как отставание (короткий сон).
if (exportSink is not null)
{
    var fresh = await exportSink.ReadStatusAsync(stoppingToken);
    var latest = EtcdSnapshotSink.LatestLocalFile(options.CurrentValue.Snapshots.Dir);
    behind = !fresh.IsSuccess
             || EtcdSnapshotStatus.IsBehind(
                 fresh.Value, EtcdSnapshotStatus.TakenUnixFromName(Path.GetFileName(latest ?? "")));
}
```

  (`EtcdSnapshotSink.LatestLocalFile` — public (правка T5 Step 5.1, круг 7); `using PgWorker.Backups.EtcdExport` в SnapshotLoop.cs — добавить при необходимости.)

  Сон (замена `Task.Delay(TimeSpan.FromMinutes(...SnapshotIntervalMin), ...)` в ветке лидера; `behind` — свежий, после пере-расчёта):

```csharp
// Сон тика лидера (spec §3.5 п.3): выгрузка здорова (state=OK, не отстаёт —
// каждый успешный проход sink'а продвигает покрытие к метке нового слепка) —
// SnapshotIntervalMin как раньше; отстаёт/FAILED — RetryIntervalSec
// (RPO-окно транзиента закрывается минутами; исправный контур выгружает
// каждый плановый слепок — каденс остаётся плановым, AC2).
var delay = exportSink is not null && behind
    ? TimeSpan.FromSeconds(options.CurrentValue.Snapshots.Export.RetryIntervalSec)
    : TimeSpan.FromMinutes(options.CurrentValue.Loops.SnapshotIntervalMin);
await Task.Delay(delay, stoppingToken);
```

  Регистрация в Program.cs: `builder.Services.AddSingleton<SnapshotLoop>();` → явная фабрика (последний параметр — sink, может быть null):

```csharp
builder.Services.AddSingleton(sp => new SnapshotLoop(
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<SnapshotJob>(),
    sp.GetRequiredService<ILogger<SnapshotLoop>>(),
    sp.GetRequiredService<HealthState>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>(),
    sp.GetService<EtcdSnapshotSink>()));
```

  (Аккуратно с фактическим конструктором: параметры — `IOptionsMonitor<PgWorkerOptions> options, ClaimStore claims, SnapshotJob snapshots, ILogger<SnapshotLoop> logger, HealthState health, TimeProvider clock, WorkerMetricsInstrumentation metrics`; последний новый параметр `EtcdSnapshotSink? exportSink = null`. `sp.GetService<EtcdSnapshotSink>()` возвращает null при выключенной опции — registrations из Task 6 хранят `EtcdSnapshotSink?`; для резолва типа `EtcdSnapshotSink?` использовать `sp.GetService<EtcdSnapshotSink>()`.)

- [x] **Step 7.4: Run → PASS** (`--filter FullyQualifiedName~SnapshotLoopExport`); полные юниты: `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~PgWorker.UnitTests"`.

- [x] **Step 7.5: Commit**

```bash
git add src/PgWorker.App/Loops/SnapshotLoop.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/SnapshotLoopExportTests.cs
git commit -m "feat(app): t08 — SnapshotLoop: доводка отстающей выгрузки перед снятием (CatchUpAsync новейшего локального) + сон RetryIntervalSec при отставании; takeover без изменений"
```

---

## Task 8: Deploy — compose и .env.example (spec §3.3)

**Вход:** Task 6–7 смержены.
**Действие:** `deploy/docker-compose.yml` (x-pgworker-env — оба инстанса наследуют) + `deploy/.env.example`.
**Выход:** включение экспорта на деплое — переменной `PGW_SNAPSHOTS_EXPORT_ENABLED`.
**Проверка:** `docker compose -f deploy/docker-compose.yml --env-file deploy/.env.example config` — интерполяция без ошибок.
**Связь со spec:** §3.3 (развёртывание), §2.5.

- [x] **Step 8.1: compose** — в `deploy/docker-compose.yml`, x-pgworker-env после блока `PgWorker__Backups__Job__Image`:

```yaml
  # Экспорт etcd-снапшотов в S3 (reliability t08, arch/14 §8): тот же bucket
  # бэкапов (PGW_BACKUP_S3_*), служебный префикс etcd/; S3-креды отдельной
  # переменной НЕ задаются. false → sink не подключается (поведение прежнее).
  PgWorker__Snapshots__Export__Enabled: ${PGW_SNAPSHOTS_EXPORT_ENABLED:-false}
```

- [x] **Step 8.2: .env.example** — после блока `PGW_BACKUP_S3_*`:

```bash
# Экспорт etcd-снапшотов в S3 (t08): тот же bucket/креды, что PGW_BACKUP_S3_*;
# слепки уходят префиксом etcd/ каждым снятием (план + внеочередные).
PGW_SNAPSHOTS_EXPORT_ENABLED=false
```

- [x] **Step 8.3: Проверка + Commit**

```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env.example config >/dev/null && echo OK
git add deploy/docker-compose.yml deploy/.env.example
git commit -m "feat(deploy): t08 — PGW_SNAPSHOTS_EXPORT_ENABLED в x-pgworker-env (оба инстанса) + .env.example; S3-креды переиспользуются из PGW_BACKUP_S3_*"
```

- [x] **Step 8.4: Примечание dev-stand (без правок кода).** Dev-stand (`dev-stand/adminpanel/`) отдельного compose-сервиса pgworker не содержит — PgWorker стенда поднимается своим `deploy/docker-compose.yml` (`00-up.sh`: `--env-file deploy/.env`, оба инстанса). Поэтому правки dev-stand не требуются: включение экспорта на стенде = `PGW_SNAPSHOTS_EXPORT_ENABLED=true` в `deploy/.env` (секретов новых нет — S3-креды уже там) + пересоздание pgworker-контейнеров. Отражено в runbook Task 12 (раздел ссылается на переменную).

---

## Task 9: Панель — модель, парсер, чтение ключа (spec §3.7)

**Вход:** Task 1 смержён (канон adminpanel/02 §2.3.1); Tasks 2–8 — любые (независимо).
**Действие:** `EtcdSnapshotExportInfo` (Core); поле `EtcdSnapshots` в `EtcdSnapshot`; парсер `EtcdSnapshotsParser` (толерантный); точечный Range в `SnapshotRefresher` (паттерн WorkerApiCert).
**Выход:** статус выгрузки — в снапшоте панели.
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~EtcdSnapshotsParser` — зелёные.
**Связь со spec:** §3.7 (парсер), AC7.

- [x] **Step 9.1: Failing-тесты парсера** — создать `src/tests/AdminPanel.UnitTests/EtcdSnapshotsParserTests.cs`:

```csharp
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /pgworker/etcd-snapshots (t08, adminpanel/02 §2.3.1): полный формат,
// толерантность к отсутствующим полям, битый JSON — parseError (правила молчат).
public class EtcdSnapshotsParserTests
{
    // AAA: полный формат → модель 1:1
    [Fact]
    public void Parse_полный_ключ()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots",
            """{"enabled":true,"state":"OK","last_uploaded_unix":1759330000,"last_object":"etcd/snapshot-20261001-120000.db","last_sha256":"abc","size_bytes":2048,"interval_min":360}""", 1);

        // Act
        var (parsed, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        errors.Should().BeEmpty();
        var info = parsed!; // Parse возвращает nullable-ссылку (не Nullable<T>): .Value нет
        info.Enabled.Should().BeTrue();
        info.State.Should().Be("OK");
        info.LastUploadedUnix.Should().Be(1759330000);
        info.IntervalMin.Should().Be(360);
    }

    // AAA: FAILED c error — модель несёт ошибку
    [Fact]
    public void Parse_случай_FAILED_с_ошибкой()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots", """{"enabled":true,"state":"FAILED","error":"S3 недоступен"}""", 1);

        // Act
        var (parsed, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        errors.Should().BeEmpty();
        var info = parsed!;
        info.State.Should().Be("FAILED");
        info.Error.Should().Be("S3 недоступен");
        info.LastUploadedUnix.Should().BeNull();
    }

    // AAA: битый JSON — parseError-запись, модель null (правила молчат)
    [Fact]
    public void Parse_битый_json_parseError()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots", "{не-json", 1);

        // Act
        var (info, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        info.Should().BeNull();
        errors.Should().ContainSingle(e => e.Key == "/pgworker/etcd-snapshots");
    }

    // AAA: ключа нет (null) — «выгрузка не включена», без ошибок
    [Fact]
    public void Parse_нет_ключа_null()
    {
        // Act
        var (info, errors) = EtcdSnapshotsParser.Parse(null);

        // Assert
        info.Should().BeNull();
        errors.Should().BeEmpty();
    }
}
```

- [x] **Step 9.2: Run → FAIL**.

- [x] **Step 9.3: Реализация**:

  1. `src/AdminPanel.Core/BackupsInfo.cs` — добавить record:

```csharp
/// <summary>Статус выгрузки etcd-снапшотов в S3 (t08, adminpanel/02 §2.3.1):
/// ключ /pgworker/etcd-snapshots, пишет PgWorker; null-поля — толерантный
/// парсинг (битые/старые ключи); enabled=false/ключа нет — правила молчат.</summary>
public sealed record EtcdSnapshotExportInfo(
    bool Enabled, string? State, long? LastUploadedUnix, string? LastObject,
    string? LastSha256, long? SizeBytes, int? IntervalMin, string? Error);
```

  2. `src/AdminPanel.Core/EtcdSnapshot.cs` — последний параметр record: `EtcdSnapshotExportInfo? EtcdSnapshots = null);` (после `WorkerApiCert`).

  3. Создать `src/AdminPanel.Etcd/Parsing/EtcdSnapshotsParser.cs` (паттерн WorkerCertParser/JsonValues):

```csharp
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Толерантный парсер ключа /pgworker/etcd-snapshots (t08): битый
/// JSON/state → parseError-запись + null (правила молчат, толерантный читатель);
/// отсутствующие поля — null-компоненты.</summary>
public static class EtcdSnapshotsParser
{
    public static (EtcdSnapshotExportInfo? Info, IReadOnlyList<KeyParseError> Errors) Parse(Kv? kv)
    {
        if (kv is null)
            return (null, []);
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            var enabled = root.TryGetProperty("enabled", out var e) && e.ValueKind == System.Text.Json.JsonValueKind.True;
            var state = JsonValues.ReadString(root, "state");
            var uploaded = JsonValues.ReadLong(root, "last_uploaded_unix");
            var lastObject = JsonValues.ReadString(root, "last_object");
            var sha = JsonValues.ReadString(root, "last_sha256");
            var size = JsonValues.ReadLong(root, "size_bytes");
            var interval = JsonValues.ReadInt(root, "interval_min");
            var error = JsonValues.ReadString(root, "error");
            if (state is not null and not ("OK" or "FAILED"))
                return (null, [new KeyParseError(kv.Key, $"неизвестный state статуса выгрузки: {state}")]);
            return (new EtcdSnapshotExportInfo(enabled, state, uploaded, lastObject, sha, size, interval, error), []);
        }
        catch (System.Text.Json.JsonException ex)
        {
            return (null, [new KeyParseError(kv.Key, $"битый JSON статуса выгрузки etcd-снапшотов: {ex.Message}")]);
        }
    }
}
```

  (Хелперы `JsonValues.ReadString/ReadLong/ReadInt` уже есть в `AdminPanel.Etcd/Parsing/JsonValues.cs` (internal static) — новых не добавлять.)

  4. `src/AdminPanel.Etcd/SnapshotRefresher.cs`: в `Prefixes` — `public const string EtcdSnapshots = "/pgworker/etcd-snapshots";`; среди параллельных чтений — `var etcdSnapshotsTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdSnapshots, t), ct);` (точечный префикс-запрос — вернёт ровно один ключ; паттерн WorkerApiCert); провал — в общий гвард «KV-чтения etcd не удались»; парсинг `var etcdSnapshotsParsed = EtcdSnapshotsParser.Parse(etcdSnapshotsKv.Value.FirstOrDefault());` → прокинуть в `SnapshotBuilder.Build(...)` (новый параметр) и `EtcdSnapshots = etcdSnapshotsParsed.Info` в `built with {...}`; parseErrors — аппендить в коллекцию ошибок сборки; в `FailTick` — `previous?.EtcdSnapshots` (ключ переживает отказный тик). `SnapshotBuilder.Build` — прокинуть параметр в конструктор `EtcdSnapshot` (последним).

- [x] **Step 9.4: Run → PASS** (`--filter FullyQualifiedName~EtcdSnapshotsParser`; сборка `AdminPanel.*` без warnings).

- [x] **Step 9.5: Commit**

```bash
git add src/AdminPanel.Core/BackupsInfo.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Etcd/Parsing/EtcdSnapshotsParser.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/AdminPanel.Etcd/SnapshotBuilder.cs src/tests/AdminPanel.UnitTests/EtcdSnapshotsParserTests.cs
git commit -m "feat(panel): t08 — EtcdSnapshotExportInfo + толерантный парсер /pgworker/etcd-snapshots + точечное чтение в SnapshotRefresher (ключ переживает отказный тик)"
```

---

## Task 10: Панель — правила алертов (spec §3.7)

**Вход:** Task 9 смержен.
**Действие:** два правила `IAlertRule` по образцу `BackupDrillFailedRule`/`BackupDrillStaleRule`.
**Выход:** AC7-алгоритм: FAILED → critical c `error`; тишина > 2×interval_min → warning; enabled=false/нет ключа — молчат; живой каденс выгрузки — под порогом (AC2).
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~EtcdSnapshotExportRule` — зелёные.
**Связь со spec:** §3.7 (алерты, порог честен благодаря семантике «покрытия» §3.1), AC2 (stale молчит на живом контуре)/AC7, adminpanel/03 §4 (Task 1).

- [x] **Step 10.1: Failing-тесты** — `src/tests/AdminPanel.UnitTests/EtcdSnapshotExportRuleTests.cs` (сборка снапшота — `TestSnapshots.Healthy(...) with { EtcdSnapshots = new EtcdSnapshotExportInfo(...) }`; AlertContext — `new AlertContext(null, now, 3)`):

```csharp
// AAA-кейсы (все — правила, snapshot с EtcdSnapshots; полный код кейса 1,
// остальные — по той же схеме Arrange/Act/Assert):
// 1. FAILED + enabled → critical etcd-snapshot-export-failed, Description содержит error
// 2. FAILED + enabled=false → молчит (выключено)
// 3. EtcdSnapshots=null (ключа нет/битый) → оба правила молчат
// 4. OK + enabled, last_uploaded старше 2×interval_min (interval_min=360) → warning etcd-snapshot-export-stale
// 5. OK + enabled, свежая выгрузка → молчит; живой каденс (AC2): каждый
//    слепок (плановый и внеочередной) продвигает last_uploaded_unix —
//    исправный контур под порогом, stale молчит (семантика «покрытия» §3.1/§3.7)
// 6. OK + enabled, last_uploaded отсутствует → warning stale («выгрузка молчит»)
// 7. interval_min отсутствует → панельный дефолт 360 (порог 720 мин)
[Fact]
public void Failed_включено_critical_алерт_с_ошибкой()
{
    // Arrange — снапшот с FAILED-статусом выгрузки
    var snapshot = TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
    {
        EtcdSnapshots = new EtcdSnapshotExportInfo(true, "FAILED", 1759330000, null, "abc", 1, 360, "S3 недоступен"),
    };
    var ctx = new AlertContext(null, DateTimeOffset.UtcNow, 3);

    // Act
    var alerts = new EtcdSnapshotExportFailedRule().Evaluate(snapshot, ctx).ToList();

    // Assert
    alerts.Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-failed"
        && a.Severity == AlertSeverity.Critical
        && a.Description.Contains("S3 недоступен"));
}

[Fact]
public void Failed_выключено_молчит()
{
    // Arrange — enabled=false (ключ воркера при выключенной опции)
    var snapshot = TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
    {
        EtcdSnapshots = new EtcdSnapshotExportInfo(false, "FAILED", null, null, null, null, 360, "err"),
    };

    // Act/Assert
    new EtcdSnapshotExportFailedRule().Evaluate(snapshot, ctx).Should().BeEmpty();
    new EtcdSnapshotExportStaleRule().Evaluate(snapshot, ctx).Should().BeEmpty();
}
// … кейсы 3–7 аналогично: stale-порог nowUnix - uploaded > 2*intervalMin*60
```

  (Кейсы 3–7 — по образцу этих двух: меняются поля `EtcdSnapshotExportInfo` и ожидаемый алерт; `ctx` — общее поле фикстуры класса.)

- [x] **Step 10.2: Реализация** — два файла в `src/AdminPanel.Core/Alerting/Rules/`:

```csharp
// EtcdSnapshotExportFailedRule.cs
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdSnapshotExportFailedRule : IAlertRule
{
    public const string KindName = "etcd-snapshot-export-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var export = snapshot.EtcdSnapshots;
        if (export is not { Enabled: true, State: "FAILED" })
            yield break; // выключено/нет ключа — молчим (образец backup-full-stale)

        yield return new Alert(
            KindName, AlertSeverity.Critical, KindName, "etcd",
            $"выгрузка снапшотов etcd в S3 провалилась: {export.Error ?? "без причины"}",
            new Dictionary<string, string> { ["state"] = "FAILED", ["lastUploadedUnix"] = export.LastUploadedUnix?.ToString() ?? string.Empty },
            null,
            "контроль-плейн не защищён от потери хоста — выгрузка снапшотов etcd в S3 провалилась",
            AlertRemedy.OperatorRunbook,
            "docs/runbook.md §«Восстановление etcd из S3-выгрузки (t08)»: проверь доступность S3 (PGW_BACKUP_S3_*) и статус-ключ /pgworker/etcd-snapshots; воркер доводит выгрузку тиком RetryIntervalSec");
    }
}
```

```csharp
// EtcdSnapshotExportStaleRule.cs
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdSnapshotExportStaleRule : IAlertRule
{
    public const string KindName = "etcd-snapshot-export-stale";

    // Панельный дефолт планового интервала (мин) при отсутствии interval_min.
    public const int DefaultIntervalMin = 360;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var export = snapshot.EtcdSnapshots;
        if (export is not { Enabled: true })
            yield break; // выключено/ключа нет — молчим

        var intervalMin = export.IntervalMin ?? DefaultIntervalMin;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        if (export.LastUploadedUnix is { } uploaded && nowUnix - uploaded <= 2L * intervalMin * 60)
            yield break; // свежо

        var description = export.LastUploadedUnix is { } last
            ? $"успешная выгрузка снапшотов etcd молчит {nowUnix - last} c — порог 2×{intervalMin} мин"
            : "успешной выгрузки снапшотов etcd нет при включённой опции";
        yield return new Alert(
            KindName, AlertSeverity.Warning, KindName, "etcd", description,
            new Dictionary<string, string> { ["intervalMin"] = intervalMin.ToString(), ["lastUploadedUnix"] = export.LastUploadedUnix?.ToString() ?? string.Empty },
            null,
            "выгрузка молчит — контроль-плейн копится только в локальном томе лидера (обе инстанции воркера лежат / вечный transient / лидерство снапшотов не исполняется)",
            AlertRemedy.OperatorRunbook,
            "проверь healthz обеих инстанций PgWorker и статус-ключ /pgworker/etcd-snapshots; доступ к S3 — runbook t08");
    }
}
```

  (Сигнатуру `Alert`-конструктора сверить с существующими правилами: `id, severity, kind, target, description, attrs, null, hint, remedy, action` — как в `BackupDrillFailedRule`.)

- [x] **Step 10.3: Run → PASS** (`--filter FullyQualifiedName~EtcdSnapshotExportRule`).

- [x] **Step 10.4: Commit**

```bash
git add src/AdminPanel.Core/Alerting/Rules/EtcdSnapshotExportFailedRule.cs src/AdminPanel.Core/Alerting/Rules/EtcdSnapshotExportStaleRule.cs src/tests/AdminPanel.UnitTests/EtcdSnapshotExportRuleTests.cs
git commit -m "feat(panel): t08 — алерты etcd-snapshot-export-failed (critical, текст error) и etcd-snapshot-export-stale (warning, 2×interval_min, панельный дефолт 360); enabled=false/нет ключа — молчат"
```

---

## Task 11: Панель — DTO грани и карточка UI (spec §3.7)

**Вход:** Task 9–10 смержены.
**Действие:** `BackupStorageDto.EtcdSnapshots` + маппер; карточка «etcd-снапшоты» в `BackupsStoragePage.tsx`; панельная интеграция (сид ключа → GET /api/backups/storage).
**Выход:** оператор видит выгрузку в грани «Хранилище бэкапов» (read-only).
**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~EtcdSnapshotExportDto|FullyQualifiedName~BackupsStorageApi"` — зелёные; `frontend` — типы согласованы (сборка SPA в docker-образе — Task 14/E2E прогонит).
**Связь со spec:** §3.7 (UI-карточка), AC7.

- [x] **Step 11.1: DTO + маппер** — `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs`:

```csharp
// Статус выгрузки etcd-снапшотов (t08): ключ /pgworker/etcd-snapshots.
public sealed record EtcdSnapshotsDto(
    bool Enabled, string? State, long? LastUploadedUnix, string? LastObject,
    string? LastSha256, long? SizeBytes, int? IntervalMin, string? Error);
```

  В `BackupStorageDto` — поле `EtcdSnapshotsDto? EtcdSnapshots` (после `Etcd`); в `BackupStorageMappers.MapStorage` — прокинуть `snapshot.EtcdSnapshots` в маппер:

```csharp
public static EtcdSnapshotsDto? MapEtcdSnapshots(EtcdSnapshotExportInfo? export)
    => export is null ? null : new EtcdSnapshotsDto(
        export.Enabled, export.State, export.LastUploadedUnix, export.LastObject,
        export.LastSha256, export.SizeBytes, export.IntervalMin, export.Error);
```

  (В обеих ветках MapStorage — configured/notConfigured: `EtcdSnapshots: MapEtcdSnapshots(snapshot.EtcdSnapshots)` — карточка живёт в грани независимо от настроенности S3-грани панели: ключ etcd читается всегда.)

- [x] **Step 11.2: Failing-юнит маппера** — `src/tests/AdminPanel.UnitTests/EtcdSnapshotExportDtoTests.cs`:

```csharp
// AAA: маппер статуса выгрузки — 1:1; null → null (карточка «не включена»)
[Fact]
public void MapEtcdSnapshots_полный_1к1()
{
    // Arrange
    var info = new EtcdSnapshotExportInfo(true, "OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null);

    // Act
    var dto = BackupStorageMappers.MapEtcdSnapshots(info);

    // Assert
    dto.Should().BeEquivalentTo(new EtcdSnapshotsDto(true, "OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null));
}

[Fact]
public void MapEtcdSnapshots_null_нет_ключа()
{
    // Act/Assert
    BackupStorageMappers.MapEtcdSnapshots(null).Should().BeNull();
}
```

  Run → FAIL → реализация (11.1) → PASS.

- [x] **Step 11.3: Frontend-карточка** — `frontend/src/pages/BackupsStoragePage.tsx`: в шапку (рядом `StorageCard`) добавить:

```tsx
// Карточка «etcd-снапшоты» (t08): последняя выгрузка слепка etcd в S3 —
// статус OK/FAILED, время/возраст, sha256-префикс, размер, ошибка; read-only.
function EtcdSnapshotsCard({ etcd }: { etcd: EtcdSnapshotsDto | null }) {
  const { classes } = useCreateStyles({ root: {} });
  if (!etcd) {
    return (
      <Card withBorder padding="md" radius="md" w={340}>
        <Text size="sm" fw={600}>etcd-снапшоты</Text>
        <Text c="dimmed" size="sm" mt={4}>Выгрузка не включена (ключа /pgworker/etcd-snapshots нет)</Text>
      </Card>
    );
  }
  const ok = etcd.state === 'OK';
  const ageSec = etcd.lastUploadedUnix ? Math.floor(Date.now() / 1000 - etcd.lastUploadedUnix) : null;
  return (
    <Card withBorder padding="md" radius="md" w={340}>
      <Group justify="space-between">
        <Text size="sm" fw={600}>etcd-снапшоты</Text>
        <Badge color={ok ? 'green' : 'red'}>{etcd.state ?? '?'}</Badge>
      </Group>
      <Stack gap={2} mt={6}>
        <Text size="sm">Последняя выгрузка: {etcd.lastUploadedUnix ? new Date(etcd.lastUploadedUnix * 1000).toLocaleString() : '—'}</Text>
        <Text c="dimmed" size="sm">возраст: {ageSec !== null ? `${Math.floor(ageSec / 60)} мин` : 'нет выгрузок'}</Text>
        <Text c="dimmed" size="sm" className={classes.root}>sha256: {etcd.lastSha256 ? etcd.lastSha256.slice(0, 12) : '—'}</Text>
        <Text c="dimmed" size="sm">размер: {etcd.sizeBytes ?? '—'} Б</Text>
        {!ok && etcd.error && <Text c="red" size="sm">{etcd.error}</Text>}
      </Stack>
    </Card>
  );
}
```

  В компоненте страницы — `<EtcdSnapshotsCard etcd={data.etcdSnapshots} />` после `<StorageCard data={data} />`; в типах API-клиента фронта (где описан `BackupStorageDto` — `frontend/src/api/…`, найти по `interface BackupStorageDto`) — поле `etcdSnapshots: EtcdSnapshotsDto | null` + интерфейс `EtcdSnapshotsDto { enabled: boolean; state: string | null; lastUploadedUnix: number | null; lastObject: string | null; lastSha256: string | null; sizeBytes: number | null; intervalMin: number | null; error: string | null }`. (Стиль — как соседние карточки; `useCreateStyles` не обязателен — свернуть к локальной разметке по образцу `StorageCard`.)

- [x] **Step 11.4: Панельная интеграция** — в `src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs` (Collection `backups`, `EtcdContainerFixture` + `BackupsWebFactory` с MinIO — классы серии) добавить Fact:

```csharp
// AAA (t08): ключ /pgworker/etcd-snapshots попадает в DTO грани (панель читает,
// пишет только PgWorker)
[Fact]
public async Task Storage_EtcdSnapshots_в_DTO()
{
    // Arrange — свой etcd-класс серии + сид статус-ключа
    var etcd = ...; // IClassFixture<EtcdContainerFixture> класса
    await EtcdSeed.PutAsync(etcd.Endpoint, "/pgworker/etcd-snapshots",
        """{"enabled":true,"state":"OK","last_uploaded_unix":1759330000,"last_object":"etcd/snapshot-20261001-120000.db","last_sha256":"abc123","size_bytes":2048,"interval_min":360}""",
        TestContext.Current.CancellationToken);
    var client = await BackupsLogin.LoginAsync(_factory);

    // Act
    var response = await client.GetAsync("/api/backups/storage", TestContext.Current.CancellationToken);

    // Assert
    response.StatusCode.Should().Be(HttpStatusCode.OK);
    var dto = await response.Content.ReadFromJsonAsync<BackupStorageDto>(TestContext.Current.CancellationToken);
    dto!.EtcdSnapshots.Should().NotBeNull();
    dto.EtcdSnapshots!.State.Should().Be("OK");
    dto.EtcdSnapshots.LastSha256.Should().Be("abc123");
}
```

  (Класс теста — по образцу соседних `BackupsStorage*ApiTests` той же коллекции: фикстуры и логин — как у них; имя и структура — фактические на месте.)

- [x] **Step 11.5: Run → PASS; Commit**

```bash
dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~EtcdSnapshotExportDto|FullyQualifiedName~BackupsStorageApi"
git add src/AdminPanel.Api/Inspection/BackupStorageQuery.cs src/tests/AdminPanel.UnitTests/EtcdSnapshotExportDtoTests.cs src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs frontend/src/pages/BackupsStoragePage.tsx
git commit -m "feat(panel): t08 — DTO etcdSnapshots грани «Хранилище бэкапов» + карточка «etcd-снапшоты» (время/возраст, sha-префикс, размер, OK/FAILED, ошибка); интеграция: ключ из etcd доезжает в GET /api/backups/storage"
```

---

## Task 12: Runbook — операторский путь восстановления (spec §3.8)

**Вход:** Task 1 смержен; код Tasks 2–7 даёт фактические имена/опции.
**Действие:** раздел в `docs/runbook.md` (в конец файла, после раздела «Ротация CA valkey (t07)»).
**Выход:** полный операторский рецепт «погиб etcd/хост → контроль-плейн жив» (AC9).
**Проверка:** текст содержит все 6 шагов spec §3.8; arch/09 §4 (Task 1) ссылается на раздел.
**Связь со spec:** §3.8 (проверка целостности ДО восстановления: ручная sha256-сверка с `.meta.json` — операторский барьер, `snapshot status` — информационный, финальный барьер — restore-верификация `etcdutl`), AC5/AC9.

- [x] **Step 12.1: Раздел** — добавить в `docs/runbook.md` (круг 7: шаги 2/4 переформулированы под обновлённый AC5 — ручная sha256-сверка как барьер, `snapshot status` — информационный, restore — `etcdutl snapshot restore`):

```markdown
## Восстановление etcd из S3-выгрузки (t08)

Снапшоты etcd (контроль-плейн: декларации кластеров, маршрутизация, секреты,
клэймы, portalloc) уходят в S3 каждым снятием при
`PGW_SNAPSHOTS_EXPORT_ENABLED=true` (deploy/.env): bucket бэкапов, префикс
`etcd/`, пары `snapshot-<id>.db` + `snapshot-<id>.meta.json` (sha256/размер/
ревизия/instance). Потеря docker-хоста вместе с etcd — восстановимая авария.

1. **Диагностика**: подтвердить наличие выгрузок —
   `mc ls --recursive <alias>/<bucket>/etcd/` — выбрать новейший
   `snapshot-<id>.db` и его `.meta.json`. Пусто → см. границы (п.6).
2. **Проверка целостности ДО восстановления** (обязательный шаг):
   скачать `.db`; сверить sha256 файла с `sha256` из `.meta.json` — ручная
   сверка и есть операторский барьер (ловит порчу до restore); затем
   `etcdctl snapshot status <file>` — информационный шаг (структура/размер/
   ревизия слепка; etcd 3.5.x печатает hash, но при status его НЕ сверяет —
   целостность им не доказывается); restore дополнительно верифицирует сам
   слепок (`etcdutl snapshot restore` проверяет sha256 при разворачивании,
   шаг 4). Расхождение sha256 или провал restore ⇒ взять предыдущий слепок
   (ретенция хранит N последних пар); файл с битым хешем не разворачивать
   НИКОГДА.
3. **Свежий etcd**: контейнер(ы) etcd с чистым data-dir
   (`INITIAL_CLUSTER_STATE=new`). Прод-3-ноды: сначала single-member,
   остальные — `etcdctl member add` после старта.
4. **Restore**: остановить etcd → `etcdutl snapshot restore <file>
   --data-dir=<data-dir>` (restore — офлайн-операция над слепком;
   верифицирует sha256 слепка при разворачивании — финальный барьер
   целостности) → стартовать etcd на восстановленном data-dir.
5. **Контроль**: `etcdctl endpoint health`; воркеры/панель переподключаются
   сами (poll-тик; клэймы переснимутся lease-механикой; R7 arch/14: journal
   мог откатиться к точке слепка — процессы идемпотентны и доводят фазы);
   панель видит кластеры, healthz воркеров зелёные. Статус-ключ
   `/pgworker/etcd-snapshots` появится первым же слепком воркера.
6. **Границы**: слепок = точка его снятия (RPO = точка последнего изменения
   минус окно transient-отказа S3 — закрывается доводкой тика
   `RetryIntervalSec`); локальные тома снапшотов (если пережили) —
   альтернативный источник того же формата; WAL-данные PG-шардов живут в S3
   бэкапов независимо (arch/19) — восстановление etcd их не касается.
   Алерты панели: `etcd-snapshot-export-failed` (выгрузка провалилась —
   проверить доступность S3/креды `PGW_BACKUP_S3_*`),
   `etcd-snapshot-export-stale` (выгрузка молчит — обе инстанции воркера
   лежат или вечный transient).
```

- [x] **Step 12.2: Commit**

```bash
git add docs/runbook.md
git commit -m "docs(t08): runbook — восстановление etcd из S3-выгрузки (диагностика/целостность до restore/свежий etcd/restore/контроль/R7-границы)"
```

---

## Task 13: E2E — полный контур экспорта (spec §4 Фаза 5)

**Вход:** Tasks 2–8, 10 смержены (воркер экспортирует; правила панели есть).
**Действие:** `E2eEtcdSnapshotExportScenarios.cs` (E2eEnvironment с MinIO, хост-воркер с включённым экспортом) + публичное имя etcd-контейнера окружения для verify слепка (`etcdctl snapshot status`).
**Выход:** сквозное доказательство: слепок → S3 (db+meta, sha256) → ключ OK → ретенция → verify: положительный `snapshot status` на валидном слепке (AC5; порча байта в E2E не проверяется — покрыта интеграциями T5 Fact 6); негативная ветка → FAILED + панельный алерт.
**Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eEtcdSnapshotExport` — зелёные; зачистка окружения после серии.
**Связь со spec:** §4 Ф5, AC1/AC3/AC5 (положительный `snapshot status` на валидном слепке; порча байта — интеграции T5)/AC7 (сквозные), AGENTS.md (E2E-каноны).

- [x] **Step 13.1: E2eEnvironment** — добавить публичное свойство имени etcd-контейнера (для `docker cp`/`exec etcdctl`): в `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` рядом с `EtcdEndpoint` — `public string EtcdContainerName { get; }` = фактическое имя `pgw-ee-{runId}` (присвоить в StartOnceAsync).

- [x] **Step 13.2: Сценарии** — создать `src/tests/PgWorker.IntegrationTests/E2e/E2eEtcdSnapshotExportScenarios.cs` (шапка и хелперы — по образцу `E2eBackupScenarios`: `Fx`, `Endpoint`, `G`, `McLsAsync`,SeedClusterAsync не нужен — кластеров поднимать НЕ надо, снапшот-контур живёт без них):

```csharp
// E2E экспорта etcd-снапшотов (t08): изолированное окружение E2eEnvironment
// (своя сеть/etcd/MinIO — withMinio:true) + хост-воркер с включённым
// Snapshots:Export. Каждый Fact — своё окружение (per-method изоляция,
// e2e-isolation §1/§3): метрики/ключи/объекты сценария не пересекаются.
public class E2eEtcdSnapshotExportScenarios
{
    private const string Bucket = "pgworker-backups";
    private E2eEnvironment Fx = null!;
    private string Endpoint => Fx.EtcdEndpoint;
    private EtcdGateway G => Fx.Gateway;

    // Хост-воркер с включённым экспортом (S3 — MinIO окружения).
    private Task<HostInstance> StartExportHostAsync(string name, CancellationToken ct,
        int intervalMin = 1, int retrySec = 5, int retention = 28, string? s3Override = null)
        => Fx.StartHostAsync(name, snapshotIntervalMin: intervalMin, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Snapshots__Export__Enabled"] = "true",
            ["PgWorker__Snapshots__Export__RetryIntervalSec"] = retrySec.ToString(),
            ["PgWorker__Snapshots__Export__RetentionObjects"] = retention.ToString(),
            ["PgWorker__Backups__S3__Endpoint"] = s3Override ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
        }, ct: ct);
```

  Fact'ы (первый — полный код, остальные — по той же схеме):

  1. **`Export_плановый_слепок_уезжает_в_S3_ключ_OK`**:

```csharp
// AAA: включённый экспорт — слепок лидера уезжает парой db+meta в MinIO,
// ключ OK с фактом, sha256 совпадает, валидный слепок проходит etcdctl
// snapshot status (порча байта — интеграции T5, не E2E).
[Fact]
public async Task Export_плановый_слепок_уезжает_в_S3_ключ_OK()
{
    // Arrange — своё окружение (сеть/etcd/MinIO) + хост-воркер с экспортом
    DockerTrait.SkipIfUnavailable();
    var ct = TestContext.Current.CancellationToken;
    await using var fx = await E2eEnvironment.StartAsync("snap-exp", withMinio: true, ct: ct);
    Fx = fx;
    await using var app = await StartExportHostAsync("snapexp", ct, intervalMin: 1);

    // Act — ждём первой успешной выгрузки (первый тик лидера снимает сразу)
    var okKey = await E2eFixture.WaitForAsync(async () =>
        (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"OK\"") == true,
        TimeSpan.FromSeconds(120), ct);

    // Assert — ключ OK с интервалом и фактом
    okKey.Should().BeTrue("первый тик лидера снимает слепок и выгружает его");
    var kv = (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value!.Value;
    kv.Should().Contain("\"interval_min\":1").And.Contain("\"last_object\":\"etcd/snapshot-");
    // Assert — объекты в MinIO: пара db+meta
    var listing = await McLsAsync("etcd/");
    listing.Should().ContainMatch("* etcd/snapshot-*.db").And.ContainMatch("* etcd/snapshot-*.meta.json");
    // Assert — sha256 выгрузки == sha256 статуса (целостность)
    var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(kv)!;
    var dbKey = status.LastObject!;
    using var s3 = new PgWorker.Backups.BackupS3(Fx.MinioRuntimeForHost()); // helper-обёртка на Fx.S3Endpoint/креды (добавить рядом с McLsAsync по образцу OwnMinio.Runtime)
    // … скачать dbKey AWSSDK-клиентом теста → SHA256 == status.LastSha256
    // Assert — verify слепка (AC5): etcdctl snapshot status на ВАЛИДНОМ слепке
    // в контейнере etcd окружения — положительный вердикт структуры/ревизии
    // (etcd 3.5.x печатает hash, но при status его НЕ сверяет — порча байта
    // в E2E не проверяется, покрыта интеграциями T5 Fact 6)
    var tempFile = Path.Combine(Path.GetTempPath(), $"pgw-e2e-snap-{Guid.NewGuid():N}.db");
    // … записать скачанные байты в tempFile
    await Fx.RunDockerAsync(["cp", tempFile, $"{Fx.EtcdContainerName}:/tmp/snap.db"], ct);
    var verdict = await Fx.RunDockerAsync(
        ["exec", Fx.EtcdContainerName, "sh", "-c", "etcdctl snapshot status /tmp/snap.db && echo VERIFY_OK"], ct);
    verdict.Should().Contain("VERIFY_OK", "валидный слепок из S3 проходит etcdctl snapshot status (AC5)");
}
```

  2. **`Export_ретенция_держит_N_пар`** (AAA): хост с `retention: 1`, `intervalMin: 1`; после первого OK-ключа ждать OK-ключ со свежим `last_object` (каждый плановый слепок уезжает новой парой — интервал 1 мин; бюджет ≤ 180 с) → `McLsAsync("etcd/")`: ровно 2 ключа (одна пара), старейшего id нет (AC4 сквозно).
  3. **`Export_негатив_S3_недоступен_слепок_снят_ключ_FAILED_алерт`** (AAA): хост с `s3Override: "http://host.docker.internal:1"` (закрытый порт — протокольный «всегда закрыт», паттерн `Backup_FailsOnBadS3`); ждать ключ `\"state\":\"FAILED\"` + непустой `error` (бюджет 120 с); Assert: локальный файл снят (`Directory.GetFiles(app.SnapshotsDir, "snapshot-*.db")` непуст — HostInstance несёт SnapshotsDir) и цикл жив (ключ `/pgworker/api/` инстанса существует); панельный алерт — паттерн drill-ассерта E2eBackupScenarios:741–747: приватный хелпер строит минимальный `EtcdSnapshot` с `EtcdSnapshots = AdminPanel.Etcd.Parsing.EtcdSnapshotsParser.Parse(new Kv("/pgworker/etcd-snapshots", kvValue, 1)).Info` → `new EtcdSnapshotExportFailedRule().Evaluate(snapshot, new AlertContext(null, now, 3))` содержит critical-алерт (AC3, AC7).

- [x] **Step 13.3: Run + зачистка** — `PGW_TEST_DOCKER=1 DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eEtcdSnapshotExport`; teardown окружений отработал → `docker ps -a | grep pgw-e` пусто; остаточные сети — `docker network ls | grep kfw-net` → при мусоре `docker network prune -f` (AGENTS.md).

- [x] **Step 13.4: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eEtcdSnapshotExportScenarios.cs src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs
git commit -m "test(e2e): t08 — сквозной экспорт etcd-снапшотов: слепок→S3 (db+meta, sha256)→ключ OK→retention→etcdctl snapshot status; негатив: S3 недоступен — слепок снят, ключ FAILED, панельный critical-алерт"
```

---

## Task 14: Мерж-гейт — полные прогоны + roadmap (spec §4 Ф5, AC11)

**Вход:** Tasks 1–13 смержены в ветку.
**Действие:** серии юниты → интеграции → E2E (с зачисткой между сериями), кейс-маркер `Scale_AddEmptyShard` на свежем Release, снятие тега t08 из roadmap + перенос строки reliability-report.
**Выход:** ветка готова к ревью/мержу; мерж-гейт трека reliability исполнен.
**Проверка:** все серии зелёные; `grep t08-etcd-snapshot-export arch/roadmap/reliability.md` пусто; reliability-report.md содержит строку в «Сделано».
**Связь со spec:** §4 Ф5, AC11, AGENTS.md (E2E на свежем Release обязателен).

- [x] **Step 14.1: Юниты** — `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~UnitTests"` — 0 failed, 0 warnings.

- [x] **Step 14.2: Интеграции (docker)** — `PGW_TEST_DOCKER=1 DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~IntegrationTests"` — дождаться финальной строки; зачистка: `docker ps -a --filter name=pgw- --format '{{.Names}}'` пусто (кроме поднятого dev-стенда, если есть — свои префиксы), `docker network prune -f`.

- [x] **Step 14.3: E2E свежий Release** — `PGW_TEST_DOCKER=1 DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eEtcdSnapshotExport"` (свежая сборка — E2eFixture собирает сам; `PGW_TEST_E2E_NOBUILD` НЕ ставить) + кейс-маркер мерж-гейта: `PGW_TEST_DOCKER=1 DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`. Между сериями — зачистка контейнеров/сетей.

- [ ] **Step 14.4: Roadmap-гейт** (перенесён на мерж-коммит, Фаза 8; блокер гейта —
  флэйк WalStream закарантинен отдельной задачей t27-wal-staging-loss,
  arch/roadmap/reliability.md) — `arch/roadmap/reliability.md`: удалить пункт `t08-etcd-snapshot-export` (и `←`-ссылки на него, если есть — поиском по файлам `arch/roadmap/*.md`); `arch/roadmap/reliability-report.md`: перенести строку `| t08-etcd-snapshot-export | … | P2 | D, R |` из «Осталось» в «Сделано в рамках трека» + дополнить сводку D (RPO контроль-плейна закрыт S3-выгрузкой слепков) и R (самовосстанавливаемость: доводка тика) по формату раздела.

- [ ] **Step 14.5: Commit**

```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "merge: t08-etcd-snapshot-export — мерж-гейт трека reliability: тег снят из reliability.md, строка перенесена в «Сделано» reliability-report.md (сводка D/R)"
```

- [ ] **Step 14.6: Итог мерж-коммита** — финальный merge-коммит в main (по явной просьбе пользователя) обязан нести этот roadmap-гейт тем же коммитом (AGENTS.md: правило мерж-гейта).

---

## Self-Review (обновлён после седьмого круга ревью — Ф4 к.5/6 + Ф7 code-review; + круг 8 — 2 текстовые правки)

- **Дедуп убран совсем (круг 4, contract HIGH — синхронизация с обновлённым spec §2.7/§3.6):** сравнение sha256 слепка с `last_sha256` до put отвергнуто ревью как самореферентно недостижимое — статус-ключ живёт в снимаемом etcd и обновляется каждой выгрузкой, поэтому sha следующего слепка всегда отличается после первого цикла. `ExportAsync` (T5) — путь один: put db → put meta → ретенция → статус `Ok(taken, objKey, sha, size, interval)`; чтение статуса СОХРАНЯЕТСЯ только для FAILED-ветки (перенос полей последнего успеха — §3.6); в реализации нет ветви сравнения sha с прошлым состоянием (явный запрет §3.6), дедуп-тестов нет. AC2 «каждый слепок уезжает»: T5 Fact 2 — повторный TakeAsync ⇒ в S3 +2 объекта (новая пара db+meta), статус OK, `last_uploaded_unix == taken2 > taken1`, `last_object`/`last_sha256` — второго слепка, `IsBehind=false` (сон `SnapshotIntervalMin`, stale молчит; пауза 1.1 c разводит имена по секундам); T10 кейс 5 — stale молчит при живом каденсе (каждый слепок продвигает `last_uploaded_unix`); T7 сон-комментарий — `state=OK` + не отстаёт ⇒ `SnapshotIntervalMin`. Arch-редакции T1 — формулировки §3.9 обновлённого spec без дедупа: «каждый снятый слепок выгружается; статус-ключ покрытия обновляется после каждой выгрузки» (arch/14 §3.3+§6, arch/19 §5); сняты «не кладёт объектов, но обновляет…» и «дедуп … не кладёт объектов». `last_sha256` в статус-ключе остаётся (наблюдаемость, §3.1; тесты парсинга T2/T9 живы). Барьер «дедуп не вводится» зафиксирован в глобальных ограничениях плана (зеркало spec §2.7/§5).
- **Мета-нейминг по канону §3.2 (круг 5/6, plan HIGH):** мета-объект кладётся ключом `etcd/snapshot-<id>.meta.json` — по id БЕЗ `.db` (`snapshotFileName[..^".db".Length]`), НЕ `"{objKey}.meta.json"`: ключ `snapshot-<id>.db.meta.json` ретенция `EtcdExportRetention.Select` разобрала бы как мету с id `snapshot-<id>.db` без пары (`.db.db`) → сирота → снос первым же ретенционным проходом того же `ExportAsync` (красные Fact 1/2/3/7 и T13, недостижимы AC1/AC5). Fact 1 (T5) фиксирует нейминг ассертом: листинг содержит ключ с окончанием `.meta.json` и НЕ содержит `.db.meta.json`; комментарий Step 5.1 объясняет запрет. Layout согласован со всеми остальными местами плана (юниты T2, гвард T2 Step 2.9, runbook T12, E2E T13 — `snapshot-*.meta.json`).
- **AC5-эха обновлённой буквы spec (круг 7, contract-echo; spec уточнён по Ф7 code-review):** проверка целостности развёрнута по ролям инструментов — `etcdctl snapshot status` даёт только положительный вердикт структуры/ревизии на валидном слепке (etcd 3.5.x печатает hash, но при status его НЕ сверяет); порчу байта ловят ручная sha256-сверка с `.meta.json` (операторский барьер) и restore-верификация (`etcdutl snapshot restore` верифицирует sha256 при разворачивании — финальный барьер). Отражено: T12 runbook-шаг 2 (sha256 — барьер, status — информационный, «Расхождение sha256 или провал restore ⇒ предыдущий») и шаг 4 (`etcdutl snapshot restore`, не etcdctl); T5 Fact 6 — валидный: положительный status; порча: sha256-расхождение + провал `etcdutl restore` (интеграция); T13 — E2E проверяет только валидный слепок (порча байта в E2E НЕ добавляется — spec её больше не требует, эхо «restore-verify» заменено на «verify слепка»); Связи-строки T5/T12/T13 несут AC5-расшифровку. Требует доработки исполнителем: тест/доки Tasks 5/12 уже смержены со старыми формулировками — правка по этим пунктам плана (как и фикс сна ниже) выполняется в Фазе 6 по этому плану, ВКЛЮЧАЯ arch/09 §4-цитату T1 Step 1.3: `etcdctl snapshot restore` → `etcdutl snapshot restore`; в комментарии шага 1 статус — информационный, барьер — ручная sha256-сверка (+ restore-верификация) — иначе исполнитель по Self-Review починит только T5/T12/T7, и канон arch/09 §4 разъедется с runbook/AC5.
- **Step 7.3 — точность сна при первом транзиенте в тике снятия (круг 7, impl-major):** CatchUpAsync вычисляет `behind` ДО `TakeAsync`, а сон берётся после — при ранее здоровом статусе транзиент S3 в момент `TakeAsync` (статус-ключ становится FAILED уже после расчёта «до») давал бы полный `SnapshotIntervalMin` вопреки §3.5 п.3. Фикс: после `TakeAsync` (при `sink != null`) повторный расчёт по свежему статус-ключу — `ReadStatusAsync` → `behind = !ok || IsBehind(fresh, TakenUnixFromName(имя новейшего локального слепка))`; для этого `EtcdSnapshotSink.LatestLocalFile` стал `public` (правка T5 Step 5.1: call-сайт из PgWorker.App). Новый юнит-кейс T7 `Первый_транзиент_в_тике_TakeAsync_сон_короткий` (Arrange: здоровый OK-статус + `MemoryS3.PutObjectFails=true`; ассерт ≥2 слепков за бюджет — короткий сон доказывает именно повторный расчёт); существующий кейс с предзасеянным FAILED (`Сон_при_отставании_RetryIntervalSec`) остаётся.
- **Покрытие spec:** §3.1→T2/T5/T9; §3.2→T2/T5; §3.3→T6/T8 (+dev-stand примечание T8 Step 8.4); §3.4→T3; §3.5→T5(CatchUpAsync, инвариант покрытия)+T7(сон п.3); §3.6→T2/T4/T5(единый путь put→meta→ретенция→статус); §3.7→T9/T10/T11; §3.8→T12; §3.9→T1 (п.1–4, формулировки без дедупа + семантика «покрытия» в arch/14/arch/19-редакциях, «пять»→«шесть» в преамбуле adminpanel/02 §2.3.1) + T14 (п.5); Фазы §4→T2(Ф1)/T3–T5(Ф2)/T6–T8(Ф3)/T9–T11(Ф4)/T12–T14(Ф5); AC1–AC11 распределены (AC2 — «каждый слепок уезжает новой парой», AC6 — семантика «покрытия»). **AC8 полный:** гвард `GroupPrefixes` (T2 Step 2.9) + вхождение `etcd/*` в list всего bucket = база `used_bytes` (T5 Fact 1). **AC10-хвост** — косвенное покрытие (приемлемо по ревью Ф4): писатель статус-ключа при `Enabled=false` отсутствует по построению (T6), T3 Fact `TakeAsync_без_sink` + T10 кейсы 2–3 закрывают поведение.
- **TimeoutSec — примечание к букве spec:** семантика TimeoutSec уточнена относительно spec §3.3/§3.6 («бюджет одного S3-put-шага» / «таймаут шага») по решению ревью Фазы 4: единый бюджет ОДНОЙ попытки `ExportAsync` (linked-CTS CancelAfter: puts+ретенция+статус суммарно) — строже и безопаснее пошагового. Правка spec сознательно НЕ делается; канон — arch §8-редакция Task 1 Step 1.1 п.3. Формулировки выровнены в T1 (arch)/T3/T5/T6; CTS-механика T5 не менялась.
- **CatchUpAsync — семантика отставания (вариант (б) ревью Ф4-круг2, принят; согласован с кругами 3/4/5):** возврат `Result<bool>` = «выгрузка отстаёт ПО СТАТУС-ключУ» (`EtcdSnapshotStatus.IsBehind`: FAILED / ключа нет / локальный новее подтверждённого покрытия), не «была ли попытка доводки» — закрывает краевой случай «FAILED при пустом томе»; успешный re-export продвигает `last_uploaded_unix` и закрывает отставание (§3.5 п.1). Ошибка re-export → `Failed` → формула сна T7 `behind = catchUp.IsSuccess ? catchUp.Value : true` даёт короткий сон. Тесты T7: Fact 2 достижим (пустой том + FAILED + `PutObjectFails`: тик 1 → слепок 1, сон 1 c → тик 2 → слепок 2; предзасеянный файл сознательно НЕ используется — вакуумировал бы ассерт «≥2»), Fact `CatchUp_пустой_том_FAILED_отстаёт`.
- **Типы/имена:** `IsBehind`/`TakenUnixFromName` — статические методы на record `EtcdSnapshotStatus` (круг 4, LOW: call-сайты T2 Step 2.1/T5/T7 зовут их от типа записи; в `EtcdSnapshotStatusJson` остаётся только JSON: `Ok(coveredTakenUnix, …)/Failed/Parse/Key`); `EtcdSnapshotMeta`(+`EtcdSnapshotMetaJson`)/`EtcdExportRetention`/`EtcdSnapshotSink`/`ISnapshotSink`/`SnapshotExportOptions`/`EtcdSnapshotExportInfo`/`EtcdSnapshotsParser`/`EtcdSnapshotExportFailedRule`/`EtcdSnapshotExportStaleRule` — согласованы между тасками; статус-ключ `/pgworker/etcd-snapshots` один везде; `PutObjectAsync(string, byte[], string?, CancellationToken)` — сигнатура из spec §3.6; `MemoryS3.PutObjectFails` — инъекция транзиента (T7); парсер панели зовёт фактические `JsonValues.ReadString/ReadLong/ReadInt`. Регистрация `EtcdSnapshotSink` — единственный синглтон-блок (T6 Step 6.3), обе фабрики берут его `sp.GetService<EtcdSnapshotSink>()`. `Parse` возвращает nullable-ссылку, не `Nullable<T>` — доступ в тестах через локаль `var p = parsed!;` (круг 5).
- **Реальные файлы:** все Modify-пути проверены по worktree (SnapshotJob.cs, SnapshotLoop.cs, BackupS3.cs, OrphanRegistry.cs, Options.cs, Program.cs, SnapshotRefresher.cs, BackupsParser.cs-паттерн, BackupStorageQuery.cs, BackupsStoragePage.tsx, deploy/*, arch/*, runbook.md).
- **Open-точки исполнителя** (сверить на месте, не решения): фактический namespace `FakeEtcd` (Provisioning/Fakes.cs); сигнатура `Alert`-record; стиль карточки фронта по соседним.
- **Правки ревью:** круг 1 (LOW/INFO): AC8-хвост used_bytes (T5 Fact 1); dev-stand (T8 Step 8.4); дубль DI-регистрации (T6); семантика TimeoutSec в плане; имена JsonValues (T9); «один Fact» (T2). Круг 2: тест T7 Fact 2 — статус-семантика CatchUpAsync + `PutObjectFails`-Arrange + Fact краевого случая; примечание TimeoutSec-vs-spec; «пять»→«шесть» (Step 1.4); фиксация косвенного AC10-покрытия. Круг 3 (contract): дедуп-пропуск = успешная выгрузка с обновлением статус-ключа покрытия (впоследствии перекрыт кругом 4). Круг 4 (contract HIGH): дедуп убран совсем — T5 `ExportAsync` (один путь put→meta→ретенция→статус; чтение статуса только для FAILED-переноса полей), T5 Fact 2 (AC2: +2 объекта/новая пара, taken2 > taken1, IsBehind=false), T1 (arch-редакции arch/14 §3.3+§6, arch/19 §5 без дедупа), T2 (комментарии Ok/IsBehind), T7 сон, T10 кейс 5 (живой каденс), шапка/карта файлов/глобальное ограничение-барьер. Круг 5 (LOW/косметика): `parsed!.Value` → локаль `var status = parsed!;` (T2 Step 2.1; идентичная CS1061-проблема `info!.Value` поправлена и в T9 Step 9.1 — Parse возвращает nullable-ссылку); ссылка BackupS3Tests:188 → 189 (T5 Fact 1, фактическая строка `ListPrefixAsync("")`). Круг 5/6 (plan HIGH): мета-нейминг по канону §3.2 — Step 5.1: мета-ключ `{Prefix}{snapshotFileName[..^".db".Length]}.meta.json` (по id без `.db`, не `objKey.meta.json` — иначе ретенция сносит мету как сироту в том же проходе); Fact 1: комментарий + ассерт нейминга (`NotContain ".db.meta.json"`). Круг 7 (Ф7: AC5 contract-echo + Step 7.3 impl-major): T12 шаг 2 (ручная sha256-сверка — операторский барьер; `snapshot status` — информационный: 3.5.x hash при status НЕ сверяет; финальный барьер — restore-верификация) и шаг 4 (`etcdutl snapshot restore`, не etcdctl); T5 Fact 6 переписан (валидный — положительный status; порча — sha256-расхождение + провал `etcdutl restore`) + Выход/хелпер/Связь; T13 — verify только на валидном слепке (порча байта в E2E НЕ добавляется, spec её не требует; «restore-verify» → «verify слепка»); Step 7.3 — повторный расчёт `behind` после `TakeAsync` по свежему статус-ключу (транзиент в тике снятия не тянет полный сон, §3.5 п.3), `LatestLocalFile` → `public` (T5 Step 5.1), новый юнит-кейс T7 `Первый_транзиент_в_тике_TakeAsync_сон_короткий`; Связи-строки T5/T7/T12/T13 дополнены AC5/§3.5-п.3-расшифровками.
