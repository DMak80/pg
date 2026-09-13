# t05-backup-restore — план реализации (восстановление шарда из бэкапа, PITR)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — реализация по задачам, шаги
> отмечаются чекбоксами (`- [ ]`).

**Цель:** восстановление шарда из S3-бэкапов по команде оператора (полный +
накат WAL до `latest`/`target_time`), с API-эндпоинтом, etcd-контрактом,
гвардами контуров, панельным алертом, E2E-гейтом и runbook.

**Архитектура:** заявка-статус в etcd (`restore/<id>`) → `RestoreProcess`
(машина тика под клэймом `<C>`: валидация → демонтаж шарда → ephemeral
restore-джоб (`pg_ctl` + `restore_command` через mc) → rejoin EnsureNode) →
пост-обработка (сброс `wal`-ключа → планировщик t02 переснимает полный).
Гварды исключают шард из остальных контуров воркера на время restore.

**Технологии:** .NET 10 (Nullable=enable, TreatWarningsAsErrors=true),
минимал-API mTLS-грань, etcd v3 HTTP API, docker engine API, testcontainers
(etcd/MinIO), FluentAssertions + xUnit.

**Spec:** `docs/superpowers/2026-09-11-t05-backup-restore/spec.md` — план
реализует его фазы Ф0–Ф7; аргументация от spec, исполнители читают оба.

## Глобальные ограничения (из spec §5)

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`;
  пакеты только через `Directory.Packages.props` — **новых пакетов нет**.
- Порты docker в тестах — только динамические (`assignRandomHostPort: true` +
  `GetMappedPublicPort`), никаких литералов-портов; таймауты фикстур ≤ 100 с,
  E2E-бюджеты фаз ≤ 300 с.
- Каждый E2E/интеграционный тест полностью чистит за собой (teardown при любом
  исходе); между сериями `docker rm -f $(docker ps -aq)` (не трогая стендовые
  `as-*`/`adminpanel`) + `docker network prune -f`.
- Язык: доки/комментарии — русский, идентификаторы — английский; тесты —
  AAA-комментариями (Arrange/Act/Assert).
- Не трогать: HA-контур живых шардов, конфиги Patroni нод, чужие префиксы etcd;
  S3-объекты restore только читает (никогда не удаляет).
- `BackupsParser` (t01) — обратная совместимость: существующие ключи не
  меняются, restore-ключи добавляются толерантно.
- Локально собираемые образы в registry 192.168.0.1:5000 не класть; образ
  `pgworker-backup` НЕ меняется.
- Все пути ниже — от корня воркtree `/Users/demakaev/ZCodeProject/worktrees/feat-t05-backup-restore`
  (в командах — относительные из его корня; рабочая директория shell сбрасывается
  между вызовами — использовать абсолютные пути или `cd` в каждом вызове).

## Соглашения плана

- Каждая задача имеет: **Вход** (предусловие), **Действие** (файлы/изменения),
  **Выход** (что готово), **Проверку** (команда/критерий), **Spec** (закрываемое
  требование). Внутри — bite-size TDD-шаги.
- Команды сборки/тестов: из корня воркtree:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter ...`
  (юниты воркера: `src/tests/PgWorker.UnitTests`; панель: `src/tests/AdminPanel.UnitTests`).
- Коммит-шаги: `git add <точные пути> && git commit -m "<conventional message>"`.

---

### Task 1: Ф0 — зафиксировать правки канона (arch/19, arch/14)

**Вход:** воркtree `feat-t05-backup-restore`; правки канона уже лежат в
рабочем дереве (внесены на фазе brainstorming этой же задачей): `arch/19-backups.md`
(§2 контракт образа + инвариант планировщика, §3.5 restore, §4 ключ
`restore/<id>`, §8 карта, §9 `Restore { RecoveryTimeoutSec }`, §10 риски),
`arch/14-pgworker.md` §1.1 (эндпоинт). Spec одобрен на гейте user-review.

**Действие:** ревью-прогон диффа канона на соответствие spec §3.1–§3.9
(структура ключа, имена статусов, контракт эндпоинта); коммит без кода.

**Выход:** канон закоммичен, дальше код аргументируется от него.

**Проверка:** `git diff --stat` показывает только `arch/14-pgworker.md`,
`arch/19-backups.md` (+ untracked spec-каталог); после коммита
`git log -1 --stat` — один коммит канона.

**Spec:** §2 п.1 (arch-first), Ф0.

- [ ] **Шаг 1. Ревью диффа**: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-backup-restore && git diff arch/19-backups.md arch/14-pgworker.md` — сверить: §3.5 описывает фазы PLANNED/RUNNING/REJOINING/COMPLETED/FAILED, ключ `restore/<id>` с полями spec §3.1, D1-префикс `pgw-backup-*`, remove-shard `cancelled-by-remove`.
- [ ] **Шаг 2. Коммит**:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-backup-restore
git add arch/19-backups.md arch/14-pgworker.md docs/superpowers/2026-09-11-t05-backup-restore/
git commit -m "docs(arch): t05-backup-restore — канон восстановления (arch/19 §2/§3.5/§4/§8/§9/§10, arch/14 §1.1) + spec"
```

---

### Task 2: Ф1 — модель restore в etcd-парсере воркера

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs`
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs` (дописать кейсы; если файла нет — создать рядом с существующими тестами парсера: `src/tests/PgWorker.UnitTests/Etcd/`)

**Interfaces:**
- Produces (для задач 3–12):
  - `enum RestoreStatus { Planned, Running, Rejoining, Completed, Failed }`
  - `record RestoreOperationState(string Id, RestoreStatus State, string BackupId, string Source, string Target, string Node, long RequestedUnix, string RequestedBy, long? StartedUnix = null, long? FinishedUnix = null, string? Phase = null, string? RestoredToLsn = null, string? Error = null)`
  - `ShardBackups` получает третий компонент `IReadOnlyList<RestoreOperationState> Restores = []` (дефолт — существующие вызовы `new(fulls, wal)` компилируются).

**Вход:** Task 1 закоммичен; `BackupsParser.Parse` уже собирает policy/full/wal.

**Действие:**
1. В `BackupsModel.cs` — enum + record выше (комментарий-док: ключ
   `/pgworker/backups/<C>/<X>/restore/<id>`, arch/19 §4; `Source` = `<srcC>/<srcX>`,
   `Target` = `latest` | `time:<RFC3339>`).
2. `ShardBackups` — добавить `Restores` с дефолтом `[]` (обратная совместимость).
3. В `BackupsParser.cs`: у ключа `/pgworker/backups/<C>/<X>/restore/<id>` сегментов 7 (`["", "pgworker", "backups", C, X, "restore", id]`) — как у `full/<id>`. Добавить ветку `case 7 when segments[4].Length > 0 && segments[5] == "restore" && segments[6].Length > 0` → `acc.Shards[X].Restores.Add((segments[6], kv.Value))`. Разбор значения — `TryParseRestore` по образцу `TryParseFull`: обязательны `state` (PLANNED|RUNNING|REJOINING|COMPLETED|FAILED), `backup_id`, `source`, `target`, `node`, `requested_unix`, `requested_by`; опциональны `started_unix`/`finished_unix`/`phase`/`restored_to_lsn`/`error`; битое → parseErrors + пропуск. Сортировка по `Id` (Ordinal) при сборке.
4. Тесты (AAA): восстановление полного статуса из JSON; битый JSON → parseErrors, запись пропущена; неизвестное state → пропуск с ошибкой; шард с full+wal+restore собирает всё; restore-ключи сортированы по Id.

**Выход:** префикс `/pgworker/backups/` парсится с restore-операциями; модель доступна процессам и гвардам.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupsParser"` — зелёные (старые + новые).

**Spec:** §3.1 (etcd-контракт, модель/парсер), §5 (обратная совместимость t01).

- [ ] **Шаг 1. Тесты парсера (failing)** — дописать в `BackupsParserTests.cs` (или создать):
```csharp
// AAA: restore-ключ разбирается в RestoreOperationState со всеми полями
[Fact]
public void Parse_restore_ключ_полный_статус()
{
    // Arrange — KV полного restore-статуса канона arch/19 §4
    var kv = new Kv("/pgworker/backups/shop/shard1/restore/20260911120000Z", """
        {"state":"RUNNING","backup_id":"20260911090000Z","source":"shop/shard1",
         "target":"latest","node":"shard1a","requested_unix":1760000000,
         "requested_by":"operator","started_unix":1760000005,"phase":"recovering"}
        """);
    // Act
    var parsed = BackupsParser.Parse([kv], out var errors);
    // Assert
    errors.Should().BeEmpty();
    var restore = parsed.Value.Single().Shards["shard1"].Restores.Single();
    restore.Id.Should().Be("20260911120000Z");
    restore.State.Should().Be(RestoreStatus.Running);
    restore.BackupId.Should().Be("20260911090000Z");
    restore.Target.Should().Be("latest");
    restore.Phase.Should().Be("recovering");
}

// AAA: битый/неизвестный state — parseErrors, запись пропущена, шард жив
[Fact]
public void Parse_restore_битый_статус_толерантно()
{
    // Arrange
    var kv = new Kv("/pgworker/backups/shop/shard1/restore/x1", """{"state":"WAT"}""");
    // Act
    var parsed = BackupsParser.Parse([kv], out var errors);
    // Assert
    errors.Should().ContainSingle(e => e.Contains("restore/x1"));
    parsed.Value.Single().Shards["shard1"].Restores.Should().BeEmpty();
}
```
- [ ] **Шаг 2. Прогнать — падают** (`dotnet test ... --filter FullyQualifiedName~BackupsParser`).
- [ ] **Шаг 3. Реализация** модели+парсера (п.1–3 «Действия»).
- [ ] **Шаг 4. Прогнать — зелёные**; весь юнит-проект: `dotnet test src/tests/PgWorker.UnitTests -c Debug` (регрессия ShardBackups-вызовов).
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Etcd/Parsing/BackupsModel.cs src/PgWorker.Etcd/Parsing/BackupsParser.cs src/tests/PgWorker.UnitTests && git commit -m "feat(backups): модель RestoreOperationState + разбор restore/<id> парсером t01 (t05 §3.1)"`.

---

### Task 3: Ф1 — имена (BackupNames) и JSON-сериализация статуса restore

**Files:**
- Modify: `src/PgWorker.Backups/BackupNames.cs`
- Create: `src/PgWorker.Backups/Restore/RestoreStatusJson.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupNamesTests.cs` (дописать; если нет — создать), `src/tests/PgWorker.UnitTests/Backups/RestoreStatusJsonTests.cs` (новый)

**Interfaces:**
- Consumes: `RestoreOperationState` (Task 2).
- Produces:
  - `BackupNames.RestoreKey(cluster, shard, id)` → `/pgworker/backups/<C>/<X>/restore/<id>`
  - `BackupNames.RestoreContainerName(cluster, shard, id)` → `pgw-backup-restore-<C>-<X>-<id>`
  - `RestoreStatusJson.Serialize(RestoreOperationState state)` → string (JSON канона: `state/backup_id/source/target/node/requested_unix/requested_by` всегда; `started_unix/finished_unix/phase/restored_to_lsn/error` — при наличии; сериализация через `JsonSerializerDefaults.Web`, null-поля не пишутся).

**Вход:** Task 2 смержен в ветку (модель есть).

**Действие:** добавить методы имён; создать `RestoreStatusJson` по образцу
`BackupStatusJson` (`src/PgWorker.Backups/Job/BackupStatusJson.cs`) —
`StateName` switch: Planned→PLANNED, Running→RUNNING, Rejoining→REJOINING,
Completed→COMPLETED, Failed→FAILED. Roundtrip-тест: `Serialize` → `BackupsParser.Parse`
восстанавливает record (это же — гарантия согласованности writer/reader).

**Выход:** канонические имена и сериализация для RestoreProcess/API.

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RestoreStatusJson|FullyQualifiedName~BackupNames"` — зелёные.

**Spec:** §3.1 (ключ), §3.3 (имя контейнера), Ф1.

- [ ] **Шаг 1. Тесты (failing)** — `RestoreStatusJsonTests.cs`:
```csharp
// AAA: полный roundtrip Serialize → парсер t05 восстанавливает все поля
[Fact]
public void Serialize_roundtrip_через_парсер()
{
    // Arrange
    var state = new RestoreOperationState(
        "20260911120000Z", RestoreStatus.Rejoining, "20260911090000Z",
        "src/shop", "time:2026-09-11T10:00:00Z", "shard1a", 1760000000, "operator",
        StartedUnix: 1760000005, Phase: null, RestoredToLsn: "0/3000028");
    // Act
    var json = RestoreStatusJson.Serialize(state);
    var parsed = BackupsParser.Parse(
        [new Kv($"/pgworker/backups/c/shard1/restore/{state.Id}", json)], out var errors);
    // Assert
    errors.Should().BeEmpty();
    parsed.Value.Single().Shards["shard1"].Restores.Single().Should().Be(state);
}

// AAA: null-поля не сериализуются (канон §4 — по факту)
[Fact]
public void Serialize_опциональные_поля_отсутствуют_в_json()
{
    // Arrange
    var state = new RestoreOperationState(
        "id1", RestoreStatus.Planned, "b1", "c/s", "latest", "s1a", 1, "api");
    // Act
    var json = RestoreStatusJson.Serialize(state);
    // Assert
    json.Should().NotContain("started_unix").And.NotContain("finished_unix")
        .And.NotContain("phase").And.NotContain("restored_to_lsn").And.NotContain("error");
}
```
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** `RestoreKey`/`RestoreContainerName`/`RestoreStatusJson`.
- [ ] **Шаг 4. Прогнать — зелёные.**
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/BackupNames.cs src/PgWorker.Backups/Restore/RestoreStatusJson.cs src/tests/PgWorker.UnitTests && git commit -m "feat(backups): BackupNames restore-имена + RestoreStatusJson (t05 §3.1/§3.3)"`.

---

### Task 4: Ф1 — билдер скрипта restore-джоба (RestoreJobCommand) и парсер логов (RestoreJobLog)

**Files:**
- Create: `src/PgWorker.Backups/Restore/RestoreJobCommand.cs`
- Create: `src/PgWorker.Backups/Restore/RestoreJobLog.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs` (новый), `src/tests/PgWorker.UnitTests/Backups/RestoreJobLogTests.cs` (новый)

**Interfaces:**
- Produces:
  - `RestoreJobCommand.Build()` → `IReadOnlyList<string>` = `["bash","-c",script]` (параметров не требует — всё env).
  - env-имена (константы `RestoreJobCommand.Env*`): `MC_HOST_pgwbkp` (значение — `WalAgentCommand.McHost(endpoint, access, secret)`), `S3_BUCKET`, `SRC_PREFIX` (`<srcC>/<srcX>`), `BACKUP_ID`, `TARGET_TIME` (пусто = latest), `PGW_RECOVERY_TIMEOUT_SEC`, `PGW_RESTORE_DATA_DIR` (точка монтирования volume; скрипт с дефолтом `/restore`), `PGW_RESTORE_PGDATA` (путь PGDATA Spilo-layout; скрипт с дефолтом `$DATA_DIR/pgdata/pgroot/data`).
  - `record RestoreJobResult(bool Ok, string? RestoredToLsn, string? Error)`; `record RestoreJobMarkers(string? Phase, RestoreJobResult? Result)`; `RestoreJobLog.Parse(string logs)` — по образцу `BackupJobLog.Parse` (JSON-строки, чужой шум игнор).

**Вход:** Task 3 (имя контейнера для Hostname не нужно здесь — Cmd только).

**Действие:** полный скрипт (единственное место restore-механики; layout
Spilo, uid 101, recovery.signal, unix-socket; пути каталогов — из env с
дефолтами, чтобы точка монтирования/PGDATA конфигурировались env-контрактом
джоба как в spec §3.3):

```bash
set -euo pipefail
DATA_DIR="${PGW_RESTORE_DATA_DIR:-/restore}"
PGDATA="${PGW_RESTORE_PGDATA:-$DATA_DIR/pgdata/pgroot/data}"
LOG() { printf '%s\n' "$1"; }
FAIL() { LOG "{\"ok\":false,\"error\":\"$(printf '%s' "$1" | tr '\n' ' ' | tr -d '"')\"}"; exit 1; }

LOG '{"phase":"downloading"}'
mc cp --recursive "pgwbkp/$S3_BUCKET/$SRC_PREFIX/full/$BACKUP_ID/" "$PGDATA/" \
  || FAIL "download full/$BACKUP_ID failed"
[ -f "$PGDATA/backup_label" ] || FAIL "full/$BACKUP_ID: no backup_label"
chown -R 101:101 "${PGDATA%%/pgdata/pgroot/data}" || FAIL "chown 101:101 failed"

# restore_command: mc качает сегмент/.history из wal/-префикса прямо в %p;
# объекта нет → mc exit != 0 → конец WAL (канон §3.5)
cat > "$PGDATA/restore-wal.sh" <<'WALSH'
#!/bin/bash
set -o pipefail
exec mc cp "pgwbkp/$S3_BUCKET/$SRC_PREFIX/wal/$1" "$2"
WALSH
chmod 755 "$PGDATA/restore-wal.sh"

AUTO="$PGDATA/postgresql.auto.conf"
printf "restore_command = '/bin/bash %s/restore-wal.sh %%f %%p'\n" "$PGDATA" >> "$AUTO"
printf "recovery_target_action = 'promote'\n" >> "$AUTO"
if [ -n "$TARGET_TIME" ]; then
  printf "recovery_target_time = '%s'\n" "$TARGET_TIME" >> "$AUTO"
fi
: > "$PGDATA/recovery.signal"
# временный локальный trust для поллинга (сокет-only; после rejoin Patroni
# перепишет pg_hba своим конфигом)
sed -i '1i local all all trust' "$PGDATA/pg_hba.conf"

LOG '{"phase":"recovering"}'
PGCTL() { setpriv --reuid=101 --regid=101 --clear-groups pg_ctl "$@"; }
PGCTL -D "$PGDATA" -l /tmp/restore-pg.log -w -t 60 \
  -o "-c listen_addresses='' -c unix_socket_directories='/tmp'" start \
  || FAIL "pg_ctl start failed: $(tail -n 3 /tmp/restore-pg.log | tr '\n' ' ')"

DEADLINE=$(( $(date +%s) + PGW_RECOVERY_TIMEOUT_SEC ))
while :; do
  IN_REC=$(psql -h /tmp -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null || echo err)
  [ "$IN_REC" = "f" ] && break
  [ "$IN_REC" = "t" ] || FAIL "psql probe failed: $IN_REC"
  [ "$(date +%s)" -lt "$DEADLINE" ] || { PGCTL -D "$PGDATA" -m fast stop || true; FAIL "recovery budget exceeded ($PGW_RECOVERY_TIMEOUT_SEC s)"; }
  sleep 5
done
LSN=$(psql -h /tmp -U postgres -tAc "SELECT pg_current_wal_lsn()" | tr -d ' ')
PGCTL -D "$PGDATA" -m fast stop || FAIL "pg_ctl stop failed"

# убрать recovery-остатки: сигнал, restore/recovery-строки, trust-строку
rm -f "$PGDATA/recovery.signal"
sed -i -e '/restore-wal\.sh/d' -e '/recovery_target/d' "$AUTO"
sed -i '/^local all all trust$/d' "$PGDATA/pg_hba.conf"

LOG "{\"ok\":true,\"restored_to_lsn\":\"$LSN\"}"
```

Тонкости (зафиксировать комментариями в коде): `mc cp` с хвостовым `/` у
источника кладёт содержимое каталога в цель (прецедент entrypoint t02);
`setpriv` — запуск pg_ctl под uid:gid 101 Spilo-postgres (в postgres:18
локальный postgres — uid 999, PGDATA принадлежит 101 — численное chown и
запуск от 101 обязательны); chown делаем по корню pgdata-дерева Spilo
(`$DATA_DIR/pgdata`), выведенному из PGDATA; цель latest — БЕЗ
`recovery_target_*`: конец WAL (restore_command exit≠0) завершает targeted
recovery; `recovery.signal` — targeted recovery (в отличие от standby.signal
не ждёт новые сегменты вечно); DATA_DIR/PGDATA приходят env (дефолты — те же
значения, что и в `RestoreJobSpec`, спецификация путей — env-контракт §3.3).

**Выход:** скрипт джоба и парсер его маркеров — ядро механики t05.

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RestoreJob"` — зелёные.

**Spec:** §3.3 (скрипт джоба: фазы, chown 101, auto.conf по цели, restore-wal.sh, бюджет; env-контракт путей), §1 решения 3/5.

- [ ] **Шаг 1. Тесты (failing)** — ключевые ассерты `RestoreJobCommandTests`:
```csharp
// AAA: скрипт содержит фазы протокола t02 и download из source-префикса
[Fact]
public void Build_маркеры_фаз_и_download_из_source_префикса()
{
    // Arrange/Act
    var cmd = RestoreJobCommand.Build();
    // Assert
    cmd.Should().Equal("bash", "-c", cmd[2]);
    cmd[2].Should().Contain("""{"phase":"downloading"}""");
    cmd[2].Should().Contain("""{"phase":"recovering"}""");
    cmd[2].Should().Contain(@"mc cp --recursive ""pgwbkp/$S3_BUCKET/$SRC_PREFIX/full/$BACKUP_ID/"" ""$PGDATA/""");
    cmd[2].Should().Contain("chown -R 101:101");
}

// AAA: цель latest — без recovery_target_time; восстановление auto.conf после stop
[Fact]
public void Build_recovery_цели_и_очистка()
{
    // Arrange/Act
    var script = RestoreJobCommand.Build()[2];
    // Assert — target только через env: строка конфига пишется под if
    script.Should().Contain("""recovery_target_time = '$TARGET_TIME'""");
    script.Should().Contain("""if [ -n "$TARGET_TIME" ]; then""");
    script.Should().Contain("recovery_target_action = 'promote'");
    script.Should().Contain("recovery.signal");
    script.Should().Contain("sed -i -e '/restore-wal\\.sh/d' -e '/recovery_target/d'");
    script.Should().Contain("setpriv --reuid=101 --regid=101");
    script.Should().Contain("""{"ok":true,"restored_to_lsn":""");
}

// AAA: пути каталогов — из env-контракта джоба с дефолтами Spilo-layout (§3.3)
[Fact]
public void Build_пути_data_dir_pgdata_через_env()
{
    // Arrange/Act
    var script = RestoreJobCommand.Build()[2];
    // Assert
    script.Should().Contain("""DATA_DIR="${PGW_RESTORE_DATA_DIR:-/restore}" """);
    script.Should().Contain("""PGDATA="${PGW_RESTORE_PGDATA:-$DATA_DIR/pgdata/pgroot/data}" """);
}
```
  И `RestoreJobLogTests` (по образцу `BackupJobLogTests`): фаза+result из смеси строк шума; битый JSON-шум игнорируется.
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** (`RestoreJobCommand` с константами env и raw string literal скрипта; `RestoreJobLog` — копия структуры `BackupJobLog` с полями `phase/restored_to_lsn/error`).
- [ ] **Шаг 4. Прогнать — зелёные.**
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/Restore src/tests/PgWorker.UnitTests/Backups && git commit -m "feat(backups): RestoreJobCommand — inline-скрипт PITR-restore + RestoreJobLog (t05 §3.3)"`.

---

### Task 5: Ф1 — RestoreJobSpec (ContainerSpec) и разбор backup_label

**Files:**
- Create: `src/PgWorker.Backups/Restore/RestoreJobSpec.cs`
- Create: `src/PgWorker.Backups/Restore/BackupLabel.cs`
- Modify: `src/PgWorker.Backups/Options.cs`, `src/PgWorker.App/Options.cs`, `src/PgWorker.App/appsettings.json` (конфиг — здесь же, не отдельной задачей)
- Test: `src/tests/PgWorker.UnitTests/Backups/RestoreJobSpecTests.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupLabelTests.cs`

**Interfaces:**
- Consumes: `RestoreJobCommand.Build()` (Task 4), `WalAgentCommand.McHost`, `BackupNames.RestoreContainerName`.
- Produces:
  - `RestoreJobSpec.Build(BackupsRuntimeOptions opts, string cluster, string shard, string id, string nodeVolumeName, string targetTime, string srcCluster, string srcShard, string dataDir = "/restore")` → `ContainerSpec` (`nodeVolumeName` — имя data-volume первой ноды, напр. `pgw-c1-shard1-shard1a-data`):
    - `Image: opts.JobImage`; `Env`: `MC_HOST_pgwbkp = WalAgentCommand.McHost(opts.S3Endpoint, opts.S3AccessKey, opts.S3SecretKey)`, `S3_BUCKET`, `SRC_PREFIX = $"{srcCluster}/{srcShard}"`, `BACKUP_ID = id`, `TARGET_TIME = targetTime` (может быть ""), `PGW_RECOVERY_TIMEOUT_SEC = opts.RestoreRecoveryTimeoutSec.ToString()`, `PGW_RESTORE_DATA_DIR = dataDir`, `PGW_RESTORE_PGDATA = $"{dataDir}/pgdata/pgroot/data"`;
    - `VolumeName` = `nodeVolumeName` (data-volume первой ноды; docker создаст named volume при create контейнера), `VolumeDest: dataDir`;
    - `Ports: []`, `Hostname: BackupNames.RestoreContainerName(...)`, `CpuCores/MemoryBytes: opts.AgentCpu/opts.AgentMem`, `Label: cluster`, `Cmd: RestoreJobCommand.Build()`, `Network: null`, `Tmpfs: null`, `ExtraHosts: ["host.docker.internal:host-gateway","local:host-gateway"]` (как t02), `RestartPolicy: "no"`.
  - `BackupLabel.WalStartSegment(string backupLabelRaw)` → `string?` — sed-эквивалент прецедента entrypoint t02: `START WAL LOCATION: <lsn> (file <segment>)` → `<segment>`; нет совпадения → null.
  - Конфиг: `BackupsRuntimeOptions.RestoreRecoveryTimeoutSec = 1800`; App `BackupsRestoreOptions { RecoveryTimeoutSec = 1800 }` + `BackupsOptions.Restore` + проброс в `ToRuntime()`; appsettings `"Restore": { "RecoveryTimeoutSec": 1800 }`.

**Вход:** Task 4; `BackupsRuntimeOptions` ещё без `RestoreRecoveryTimeoutSec` — добавить в этой задаче.

**Действие:** реализация по Produces; тесты ниже.

**Выход:** джоб-спека (env-контракт с путями PGDATA) и чтение стартовой точки WAL из S3-объекта.

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RestoreJobSpec|FullyQualifiedName~BackupLabel"` зелёные; `dotnet build src/PgWorker.slnx -c Debug` без warn-as-error.

**Spec:** §3.3 (env вкл. PGDATA-путь, volume, лимиты, extra_hosts), §3.6 (backup_label), §3.8 (конфиг).

- [ ] **Шаг 1. Тесты (failing)** — `RestoreJobSpecTests` (по образцу `BackupJobSpecTests`):
```csharp
// AAA: спека монтирует data-volume первой ноды в /restore, env — S3-комплект + цель + пути
[Fact]
public void Build_data_volume_env_и_лимиты()
{
    // Arrange
    var opts = new BackupsRuntimeOptions(
        Enabled: true, S3Endpoint: "http://minio:9000", S3Bucket: "bkt",
        S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:dev",
        AgentCpu: 2, AgentMem: 4_000_000_000, RestoreRecoveryTimeoutSec: 77);
    // Act
    var spec = RestoreJobSpec.Build(opts, "c1", "shard1", "id9", "pgw-c1-shard1-shard1a-data",
        "2026-09-11T10:00:00Z", "srcc", "srcx");
    // Assert
    spec.Image.Should().Be("pgworker-backup:dev");
    spec.VolumeName.Should().Be("pgw-c1-shard1-shard1a-data");
    spec.VolumeDest.Should().Be("/restore");
    spec.Ports.Should().BeEmpty();
    spec.Cmd.Should().Equal(RestoreJobCommand.Build());
    spec.RestartPolicy.Should().Be("no");
    spec.ExtraHosts.Should().Equal("host.docker.internal:host-gateway", "local:host-gateway");
    spec.Env["SRC_PREFIX"].Should().Be("srcc/srcx");
    spec.Env["TARGET_TIME"].Should().Be("2026-09-11T10:00:00Z");
    spec.Env["PGW_RECOVERY_TIMEOUT_SEC"].Should().Be("77");
    spec.Env["PGW_RESTORE_DATA_DIR"].Should().Be("/restore");
    spec.Env["PGW_RESTORE_PGDATA"].Should().Be("/restore/pgdata/pgroot/data");
    spec.Env["MC_HOST_pgwbkp"].Should().Contain("ak:sk@minio:9000");
    spec.Env.Should().NotContainKey("PGW_BK_DSN"); // никаких паролей PG
}

// AAA: BackupLabel — сегмент из START WAL LOCATION; мусор → null
[Fact]
public void WalStartSegment_из_backup_label()
{
    // Arrange — реальный формат backup_label pg_basebackup
    const string raw = """
        START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
        CHECKPOINT LOCATION: 0/2000028
        BACKUP METHOD: streamed
        """;
    // Act / Assert
    BackupLabel.WalStartSegment(raw).Should().Be("000000010000000000000002");
    BackupLabel.WalStartSegment("no label here").Should().BeNull();
}
```
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация**: `BackupLabel` (регекс `^START WAL LOCATION: .*\((file [0-9A-F]+)\)` — брать имя файла), `RestoreJobSpec`, конфиг-опции (`Options.cs` обе + appsettings).
- [ ] **Шаг 4. Прогнать — зелёные** + `dotnet build src/PgWorker.slnx -c Debug` (0 warnings).
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups src/PgWorker.App/Options.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests && git commit -m "feat(backups): RestoreJobSpec + BackupLabel + конфиг Restore:RecoveryTimeoutSec (t05 §3.3/§3.6/§3.8)"`.

---

### Task 6: Ф2 — BackupS3.ListFullsAsync / DownloadTextAsync (интеграции MinIO)

**Files:**
- Modify: `src/PgWorker.Backups/BackupS3.cs` (интерфейс `IBackupS3` + реализация)
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs` (новые кейсы)
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` (FakeBackupS3 — реализовать новые методы)
- Modify: `src/PgWorker.App/Program.cs` (`ReloadableBackupS3` — делегировать новые методы)

**Interfaces:**
- Produces (в `IBackupS3`):
  - `Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)` — list-v2 с пагинацией по префиксу `<cluster>/<shard>/full/` c `Delimiter="/"`, id = последний компонент CommonPrefixes (`full/<id>/` → `<id>`).
  - `Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default)` — GetObject маленького текстового объекта (`objectKey` — путь внутри префикса шарда, напр. `full/<id>/backup_label`); NotFound → Failed (не исключение наружу). Используется и для `backup_manifest`-факта (Task 8).

**Вход:** Task 5; `MinioFixture` поднимает MinIO с динамическим портом.

**Действие:** AWSSDK `ListObjectsV2Request { BucketName, Prefix = $"{cluster}/{shard}/full/", Delimiter = "/" }`, собирать `page.CommonPrefixes` (пагинация по `IsTruncated`/`NextContinuationToken`), id = `prefix[..^1]` после последнего `/`. `DownloadTextAsync` — `GetObjectAsync` + `StreamReader.ReadToEndAsync`. `ReloadableBackupS3` — проксировать оба. `FakeBackupS3` — in-memory: полный список id из `Objects` с `Name` вида `full/<id>/...`; тексты из словаря `Texts` (ключ — objectKey).

**Выход:** DR-валидация умеет искать полные в S3, читать backup_label и проверять факт backup_manifest.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupS3Tests"` — зелёные; после прогона `docker ps -aq | wc -l` — чисто (фикстуры самозачищаются) + страховочно `docker network prune -f`.

**Spec:** §3.6 (два новых метода), Ф2.

- [ ] **Шаг 1. Интеграционные тесты (failing)** в `BackupS3Tests.cs`:
```csharp
// AAA: ListFulls возвращает id полных по CommonPrefixes (пагинация)
[Fact]
public async Task ListFulls_возвращает_id_полных()
{
    // Arrange — два полных в разных full/<id>/ (сид прямым клиентом)
    var ct = TestContext.Current.CancellationToken;
    using var client = SeedClient(fixture);
    foreach (var id in new[] { "20260911090000Z", "20260911120000Z" })
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard1/full/" + id + "/backup_manifest",
        }, ct);
    await using var s3 = new BackupS3(fixture.Runtime());
    // Act
    var listed = await s3.ListFullsAsync("c1", "shard1", ct: ct);
    // Assert
    listed.IsSuccess.Should().BeTrue();
    listed.Value.Should().Equal("20260911090000Z", "20260911120000Z"); // сортировка ключей
}

// AAA: DownloadText читает маленький текстовый объект; отсутствующий — Failed
[Fact]
public async Task DownloadText_backup_label_и_notfound()
{
    // Arrange
    var ct = TestContext.Current.CancellationToken;
    using var client = SeedClient(fixture);
    await client.PutObjectAsync(new PutObjectRequest
    {
        BucketName = MinioFixture.Bucket,
        Key = "c1/shard1/full/b1/backup_label",
        ContentBody = "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n",
    }, ct);
    await using var s3 = new BackupS3(fixture.Runtime());
    // Act
    var text = await s3.DownloadTextAsync("c1", "shard1", "full/b1/backup_label", ct);
    var missing = await s3.DownloadTextAsync("c1", "shard1", "full/nope/backup_label", ct);
    // Assert
    text.IsSuccess.Should().BeTrue();
    text.Value.Should().Contain("START WAL LOCATION");
    missing.IsSuccess.Should().BeFalse();
}
```
- [ ] **Шаг 2. Прогнать — падают** (методов нет — не компилируется; это ожидаемый красный).
- [ ] **Шаг 3. Реализация** в `BackupS3` + `IBackupS3` + `ReloadableBackupS3` + `FakeBackupS3`.
- [ ] **Шаг 4. Прогнать — зелёные**; зачистка контейнеров/сетей после серии:
```bash
docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/BackupS3.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): BackupS3.ListFullsAsync/DownloadTextAsync — DR-поиск полных и backup_label (t05 §3.6)"`.

---

### Task 7: Ф2 — инвариант цепочки планировщика (IsDue по wal-ключу) + гвард restore в BackupProcess

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupPlanner.cs`
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs` (дописать), `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (дописать)

**Interfaces:**
- Consumes: `ShardBackups.Restores` (Task 2).
- Produces: `BackupPlanner.IsDue(IReadOnlyList<FullBackupState> fulls, bool walKeyExists, long fullMaxAgeSec, long nowUnix)` — новая сигнатура со вторым параметром `walKeyExists` (все вызовы обновить). Семантика: нет COMPLETED → true (прежнее); иначе `nowUnix - finished > fullMaxAgeSec` **ИЛИ** `!walKeyExists`.

**Вход:** Task 2 (Restores в модели), Task 6.

**Действие:**
1. `BackupPlanner.IsDue` — параметр `bool walKeyExists`, условие ИЛИ (см. Produces).
2. `BackupProcess.TickAsync`: в цикле по шардам — два гварда:
   - restore-гвард: `if (shardBackupsRestores.Any(r => r.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining)) continue;` (шард в restore — полный не снимается, §3.4);
   - вызов `IsDue(fulls, walKeyExists: shardBackups?.Wal is not null, fullMaxAgeSec, nowUnix)`.

**Выход:** инвариант «поднятый шард имеет валидную цепочку или активный полный»: после restore (wal-ключ удалён) полный переснимается немедленно.

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupPlanner|FullyQualifiedName~BackupProcess"` — зелёные.

**Spec:** §3.5 (расширение планировщика), §3.4 (гварды: планировщик скипает шард), AC4/AC6.

- [ ] **Шаг 1. Тесты (failing)** — `BackupPlannerTests`:
```csharp
// AAA: wal-ключа нет (цепочка сброшена restore'ом) → полный due НЕЗАВИСИМО от возраста
[Fact]
public void IsDue_без_wal_ключа_due_даже_при_свежем_полном()
{
    // Arrange — COMPLETED только что
    var fulls = new[] { new FullBackupState("id1", FullBackupStatus.Completed, "n",
        BackupSourceRole.Replica, 1000, 1010, "seg", 1, null, null) };
    // Act / Assert
    BackupPlanner.IsDue(fulls, walKeyExists: false, fullMaxAgeSec: 86400, nowUnix: 1020)
        .Should().BeTrue("цепочка не заведена — нужен новый полный (инвариант §3.5)");
    BackupPlanner.IsDue(fulls, walKeyExists: true, fullMaxAgeSec: 86400, nowUnix: 1020)
        .Should().BeFalse("живой wal-ключ + свежий полный — not due");
}

// AAA: due по возрасту при живом wal-ключе — прежнее поведение
[Fact]
public void IsDue_по_возрасту_при_живом_wal()
{
    var fulls = new[] { new FullBackupState("id1", FullBackupStatus.Completed, "n",
        BackupSourceRole.Replica, 1000, 1010, "seg", 1, null, null) };
    BackupPlanner.IsDue(fulls, walKeyExists: true, fullMaxAgeSec: 100, nowUnix: 2000)
        .Should().BeTrue();
}
```
  И в `BackupProcessTests` — AAA-кейс: шард с активным restore (PLANNED в `ShardBackups.Restores`) при due-условиях не создаёт новый полный (нет PutAsync `full/` и CreateContainer).
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** (`IsDue` + вызов + гвард).
- [ ] **Шаг 4. Прогнать — зелёные** (весь юнит-проект воркера).
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/Process src/tests/PgWorker.UnitTests && git commit -m "feat(backups): IsDue-инвариант wal-ключа + гвард restore в планировщике полных (t05 §3.4/§3.5)"`.

---

### Task 8: Ф2 — RestoreProcess: каркас + PLANNED-валидация + процессный гвард дублей

**Files:**
- Create: `src/PgWorker.Backups/Process/RestoreProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` (новый; реальный etcd `EtcdFixture` + `FakeDriver`/`FakeBackupS3`/fake-engine — по образцу `WalStreamProcessTests`)

**Interfaces:**
- Consumes: модель (Task 2), `RestoreStatusJson` (Task 3), `IBackupS3.ListWalAsync/ListFullsAsync/DownloadTextAsync`, `WalChain.Check`, `WalFileName.TryParse`, `BackupLabel.WalStartSegment`, `BackupNames.RestoreKey`.
- Produces:
  - `public sealed class RestoreProcess(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, IBackupS3 s3, ClaimStore claims, WorkJournal journal, BackupsRuntimeOptions options, InstallSecrets secrets, EtcdEndpoints etcdEndpoints, IClusterSecretEnsurer appSecret, ShardProbe probe, ThresholdsOptions thresholds, TimeProvider time, ILogger<RestoreProcess>? logger = null)` — сигнатура финальная уже здесь (задачи 9–10 используют все параметры).
  - `public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)`.

**Вход:** Tasks 2–6. Интеграционная фикстура etcd — `EtcdCollection`/`EtcdFixture` (см. `WalStreamProcessTests`).

**Действие:** каркас машины:

```csharp
public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
{
    var cluster = snap.Config.Cluster;
    if (!claims.IsMine(cluster))
        return Result<ProcessOutcome>.Failed(new ApplicationException(
            $"backup-restore {cluster}: клэйм не наш (или потерян) — мутации запрещены"));
    if (!options.Enabled)
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    if (snap.Config.State != ClusterState.Active)
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

    var mine = backups.FirstOrDefault(b => b.Cluster == cluster);
    var actives = mine?.Shards
        .SelectMany(kv => kv.Value.Restores.Select(r => (Shard: kv.Key, Op: r)))
        .Where(p => p.Op.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining)
        .OrderBy(p => p.Op.Id).ThenBy(p => p.Shard).ToList() ?? [];
    if (actives.Count == 0) return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

    // Процессный гвард «максимум один активный на шард» (§3.1): API-гвард (txn
    // 409) обычно не допускает дублей, но ручная запись/гонка могут — младшие
    // дубли того же шарда гасим permanent-FAILED, старейший исполняется.
    var oldest = actives[0];
    foreach (var dup in actives.Skip(1).Where(p => p.Shard == oldest.Shard))
        await FailPermanentAsync(cluster, dup.Shard, dup.Op with { },
            error: $"дубль заявки: активен старейший {oldest.Op.Id}", ct);

    var shard = snap.Shards.FirstOrDefault(s => s.Name == oldest.Shard);
    if (shard is null) return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // шард убрали — заявку закроет remove-shard ветка
    try
    {
        return oldest.Op.State switch
        {
            RestoreStatus.Planned => await ValidateAsync(snap, shard, oldest.Op, ct),
            RestoreStatus.Running => await RunAsync(snap, shard, oldest.Op, ct),       // Task 9
            RestoreStatus.Rejoining => await RejoinAsync(snap, shard, oldest.Op, ct),  // Task 10
            _ => Result<ProcessOutcome>.Success(ProcessOutcome.Done),
        };
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
    catch (Exception ex)
    {
        // ошибка шарда не роняет тик (§3.4)
        logger?.LogError(ex, "backup-restore {Cluster}/{Shard}: {Message}", cluster, shard.Name, ex.Message);
        await journal.WritePhaseAsync(cluster, Op, "crashed", claims.InstanceId, ex.Message, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }
}
```

`ValidateAsync` (PLANNED, все шаги идемпотентны):
1. Шард с `object`-нодами (`shard.Nodes` + portalloc `Object is {Length:>0}`) → **permanent FAILED** `restore усыновлённых шардов не поддерживается (ручной путь — docs/backup-restore.md)` (Fail-хелпер: put статуса FAILED + finished_unix + error + журнал).
2. Определить `srcC/srcX` — из `Source` статуса (`"<srcC>/<srcX>"`; default записан API как собственный `<C>/<X>`).
3. `backup_id`: из заявки; иначе свой шард → max COMPLETED из `mine.Shards[shard].Full` (etcd-статусы); source-override или своих нет → `s3.ListFullsAsync(srcC, srcX)` → max Id. Пусто → permanent FAILED `полные в <srcC>/<srcX> не найдены`.
4. **Факт целостности кандидата**: `s3.DownloadTextAsync(srcC, srcX, $"full/{backup_id}/backup_manifest")` — Failed → permanent FAILED `полный <backup_id> без backup_manifest (недокачан/бит)`. Обоснование: upload t02 (`docker/backup/entrypoint.sh`) — единый `mc cp --recursive` без атомарности и без гарантии «манифест последним»: упавший на середине джоб оставляет частичный префикс `full/<id>/`, а при DR etcd-статусов нет — частичный кандидат обязан отсеиваться проверкой манифеста (инвариант «префикс ⇔ манифест» механикой t02 НЕ обеспечивается).
5. `walStart`: свой свежий полный → `WalStartSegment` etcd-статуса; иначе (override/отсутствует) → `s3.DownloadTextAsync(srcC, srcX, $"full/{backup_id}/backup_label")` → `BackupLabel.WalStartSegment` (null/Failed → permanent FAILED `full/<id>: backup_label недоступен/бит`). Наконец `WalFileName.TryParse(walStart)` (null → permanent).
6. Цепочка: `s3.ListWalAsync(srcC, srcX)` → `WalChain.Check(chainStart, names)`; `IsContinuous == false` → permanent FAILED с `GapError` (границы дыры). S3-отказ (Failed) → **transient**: статус не меняем, журнал `s3-unavailable`, следующий тик повторит.
7. Успех → put статуса `Running` + `BackupId` зафиксирован + `StartedUnix` (journal-before-manipulations: RUNNING до демонтажа — Task 9), журнал `validated/<shard>/<id>`.

**Выход:** PLANNED-заявки валидируются (вкл. manifest-факт и дубль-гвард); permanent/transient разведены.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestoreProcessTests"` — зелёные; зачистка `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`.

**Spec:** §3.4 (фазы PLANNED/валидация), §3.1 (гвард процесса «максимум один активный»), AC7 (permanent/transient).

- [ ] **Шаг 1. Интеграционные тесты (failing)** — `RestoreProcessTests.cs` (AAA; фикстуры по образцу `WalStreamProcessTests`: сид кластера/portalloc/клэйма, `FakeBackupS3` c полными/wal-объектами, `FakeDriver`):
  - `Валидация_проходит_фиксирует_backup_id_и_started_unix_переходя_в_RUNNING` (свой полный COMPLETED + непрерывная цепочка + манифест в fake-S3);
  - `Валидация_полный_без_backup_manifest_permanent_FAILED` (FakeBackupS3: префикс `full/<id>/` есть, `Texts` без `full/<id>/backup_manifest` — имитация упавшего upload t02);
  - `Валидация_дубль_заявки_младшие_permanent_FAILED_старейшая_исполняется` (два PLANNED-ключа одного шарда → младший FAILED с «дубль заявки», старший валидируется);
  - `Валидация_усыновлённый_шард_permanent_FAILED`;
  - `Валидация_дыра_цепочки_permanent_FAILED_с_границами` (FakeBackupS3: сегмент пропущен; ассерт error содержит «дыра WAL-цепочки: ожидался»);
  - `Валидация_полных_нет_в_S3_permanent_FAILED` (DR-ветка: override source, ListFulls пуст);
  - `Валидация_S3_недоступен_статус_не_меняется` (FakeBackupS3 c флагом ListFails → transient: PLANNED остаётся, тик Ok).
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** каркаса + `ValidateAsync` + хелперы `PutStatusAsync`/`FailPermanentAsync` (failover-обёртки Put/Del по образцу `BackupProcess.PutAsync`).
- [ ] **Шаг 4. Прогнать — зелёные** + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/Process/RestoreProcess.cs src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): RestoreProcess — каркас тика + PLANNED-валидация + гвард дублей (t05 §3.1/§3.4)"`.

---

### Task 9: Ф2 — RestoreProcess: RUNNING — демонтаж + restore-джоб + супервиз

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` (дописать)

**Interfaces:**
- Consumes: `RestoreJobSpec` (Task 5), `RestoreJobLog` (Task 4), `BackupNames.RestoreContainerName`, `IClusterDriver.RemoveBackupAgentsAsync/RemoveNodeAsync/EngineFor`, `IDockerEngine` (List/Inspect/Logs/Create/Start/Remove).
- Produces: внутренние фазы `RunAsync` — для задач 10 и 12 поведение статусов RUNNING→(REJOINING|FAILED).

**Вход:** Task 8 (каркас, `ValidateAsync` пишет RUNNING).

**Действие:** `RunAsync(snap, shard, op)`:

1. **Демонтаж** (идемпотентно, каждый шаг 404=ок):
   - `driver.RemoveBackupAgentsAsync(cluster, shard.Name, ct)`;
   - для каждой ноды шарда: put `nodes/<n>/state=REBUILDING` (если не Removing), `driver.RemoveNodeAsync(cluster, shard.Name, node)` — снесёт контейнер+data-volume;
   - HA-scope чистка Д3-образец (копия логики `ProvisioningProcess.ResetScopeAsync`, дубль осознан — прецедент кодовой базы): del `/service/<C>-<X>/{initialize,leader,sync}` (точечно) + del prefix `/service/<C>-<X>/optime/`, `/service/<C>-<X>/members/`; `request_*` НЕ трогаем;
   - journal op=`backup-restore` phase=`demolished/<shard>/<id>`.
2. **Джоб**: первая нода = `shard.Nodes.Min(n => n.Name)`; адрес из portalloc; `engine = driver.EngineFor(host)` (null → transient, статус не меняем); volume-имя `pgw-<C>-<X>-<node>-data`; спека `RestoreJobSpec.Build(options, cluster, shard.Name, op.Id, volumeName, targetTime, srcC, srcX)`; имя `BackupNames.RestoreContainerName`. Супервиз по образцу `BackupProcess.SuperviseActiveAsync`:
   - контейнера нет → create+start (идемпотентно; create/start отказ → transient, следующий тик);
   - `created` → довыгоняем start;
   - `running` → логи → `RestoreJobLog.Parse`: `phase` → put статуса (Phase поле; писать только при изменении); бюджет: `now - started_unix > RecoveryTimeoutSec + 60` → `RemoveContainerAsync(force)` → FAILED `recovery-бюджет исчерпан (<N> c)` + журнал;
   - `exited` → inspect exit-код: `0 && Result.Ok` → put `Rejoining` (+RestoredToLsn из result) → задача 10 продолжает; иначе → FAILED (error = result.Error ?? `exit <code>`), чистка: RemoveContainerAsync + volume первой ноды RemoveVolumeAsync (volume мог остаться битым; FAILED = конец, шард остаётся разобранным — разбор по runbook);
   - transport-отказ list/logs/inspect → transient (статус не меняем).
3. target_time для спеки: из `op.Target == "time:<RFC3339>"` → вырезать после `time:`; `latest` → "".

**Выход:** RUNNING-заявки доводятся до REJOINING (джоб ok) или FAILED (джоб fail/бюджет).

**Проверка:** `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestoreProcessTests"` — зелёные; зачистка серии.

**Spec:** §3.4 фазы RUNNING (демонтаж/джоб/супервиз), AC7.

- [ ] **Шаг 1. Тесты (failing)**:
  - `Демонтаж_сносит_агента_ноды_и_чистит_ha_scope_ставит_REBUILDING` (FakeDriver фиксирует RemoveNodeAsync по всем нодам; etcd: ноды REBUILDING, scope-ключи удалены, request_cpu жив);
  - `Джоб_exit0_ok_переход_REJOINING_с_lsn` (fake-engine контейнер exited 0, логи с result `{"ok":true,"restored_to_lsn":"0/42"}` → статус REJOINING, RestoredToLsn=`0/42`);
  - `Джоб_exit1_пишет_FAILED_с_error_и_чистит_контейнер` (логи `{"ok":false,"error":"boom"}`);
  - `Джоб_сверх_бюджета_докилл_и_FAILED` (started_unix в прошлом, контейнер running → RemoveContainerAsync force вызван, error «бюджет»);
  - `RUNNING_без_контейнера_перезапуск_идемпотентен` (PLANNED-паттерн t02: create+start снова);
  - `Transient_docker_отказ_статус_не_меняется` (ListFails → RUNNING остаётся).
  Для фейк-engine переиспользовать `BackupProcessTests.FakeBackupEngine` (перенести в общие тест-утилиты: вынести копию в `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs` публичным).
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** `RunAsync`.
- [ ] **Шаг 4. Прогнать — зелёные** + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/Process/RestoreProcess.cs src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): RestoreProcess — RUNNING: демонтаж + restore-джоб + супервиз (t05 §3.4)"`.

---

### Task 10: Ф2 — RestoreProcess: REJOINING + COMPLETED (пост-обработка)

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` (дописать)

**Interfaces:**
- Consumes: `driver.EnsureNodeAsync(topology, nodeName, addr, secrets, etcd, resources, ct)`, `ShardProbe.GetClusterAsync(addr, ct)` (Patroni-пробы), `IClusterSecretEnsurer.EnsureAsync`, `ThresholdsOptions.PatroniBootSec`, del `wal`-ключа `/pgworker/backups/<C>/<X>/wal`.
- Produces: финальные переходы REJOINING→COMPLETED (del wal-ключа + finished_unix + restored_to_lsn) — на них опираются E2E (AC4) и планировщик (Task 7).

**Вход:** Task 9.

**Действие:** `RejoinAsync(snap, shard, op)`:
1. Креды: `appSecret.EnsureAsync(cluster, snap.Config)` → `secrets with { BucketAdminUser/Password, MoverPassword }` (копия P1.5 ProvisioningProcess); portalloc: чтение `/pgworker/portalloc/<C>` через `Portalloc.Parse` (как `BackupProcess`).
2. Topology = приватная копия `Topology(cluster, shard.Name, addresses)` (как `ProvisioningProcess.Topology`).
3. Первая нода: `EnsureNodeAsync(topology, first, addresses[$"{shard}/{first}"], secrets, etcdEndpoints, resources, ct)`. `resources` — чтение `/service/<scope>/request_cpu|request_mem` (упрощённая копия `ReadShardResourcesAsync`; null допустим).
4. Идентифицирующая Patroni-проба (P2.2-образец): `probe.GetClusterAsync(address)` → member с `Name == first`, `State == "running"`. Не готово → память-трекер `ConcurrentDictionary<string,long> _rejoinWaitSince` (ключ `<C>/<X>/<id>`): первый раз фиксируем now; `now - since > PatroniBootSec` → FAILED `Patroni не поднялся за PatroniBootSec` (трекер снять); иначе InProgress (следующий тик).
5. Остальные ноды: `EnsureNodeAsync` (чистые volume создаст драйвер — реплики догоняются `pg_basebackup` от лидера). Ждём пробы всех нод running тем же трекером; после всех: каждой ноде put `state=RUNNING`.
6. **COMPLETED**: put статуса `Completed` + `FinishedUnix` (+`RestoredToLsn` уже в статусе); **del ключа `wal`** шарда (`/pgworker/backups/<C>/<X>/wal`) — сброс цепочки; журнал phase=`done/<shard>/<id>`; снять трекер. Мастер-ключ шарда обновит сам лидер (lease-скрипт мастер-ключа P11) — RestoreProcess его не пишет.
7. Takeover: трекер — диагностика (как `_patroniWaitSince` в ProvisioningProcess); всё остальное — из etcd-статуса и детерминированных имён.

**Выход:** полный цикл PLANNED→…→COMPLETED; wal-ключ удаляется (AC4).

**Проверка:** `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestoreProcessTests"` — зелёные.

**Spec:** §3.4 фазы REJOINING/COMPLETED, AC4, AC7 (takeover).

- [ ] **Шаг 1. Тесты (failing)**:
  - `Rejoin_ensure_первой_ноды_и_ожидание_пробы` (FakeDriver.EnsuredNodes содержит shard1/first; probe fake отвечает не-ready → InProgress, статус REJOINING);
  - `Rejoin_все_ноды_running_статусы_RUNNING_и_COMPLETED_wal_удалён` (probe ready по всем → etcd: nodes RUNNING, restore COMPLETED+finished_unix+lsn, wal-ключа нет, журнал done);
  - `Rejoin_сверх_PatroniBootSec_permanent_FAILED` (FakeTimeProvider/старый since);
  - `Takeover_новый_инстанс_продолжает_по_статусу` (тот же процесс без in-memory состояния: тик с REJOINING-статусом после «рестарта» — продолжает EnsureNode, не начиная заново).
  Для probe — `ShardProbe` обёртка над HttpClient: использовать `HttpMessageHandler`-fake, отвечающий Patroni-JSON `/cluster` (как `DeadHandler` в BackupProcessTests): `{"members":[{"name":"shard1a","state":"running","role":"leader"}]}`.
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** `RejoinAsync`.
- [ ] **Шаг 4. Прогнать — зелёные** + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Backups/Process/RestoreProcess.cs src/tests/PgWorker.IntegrationTests && git commit -m "feat(backups): RestoreProcess — REJOINING/COMPLETED, сброс wal-ключа (t05 §3.4, AC4)"`.

---

### Task 11: Ф2 — врезка в цикл (ClusterProcesses/ReconcileLoop/DI) + гвард WalStreamProcess

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs`
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs`
- Modify: `src/PgWorker.App/Program.cs`
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/App/` (тесты ReconcileLoop, если есть — дописать кейс; интеграционный кейс гварда — в `WalStreamProcessTests.cs`)

**Interfaces:**
- Consumes: `RestoreProcess.TickAsync(snap, backups, ct)` (Tasks 8–10).
- Produces: `IClusterProcesses.RestoreAsync(snap, backups, ct)`; вызов в Active-ветке ReconcileLoop после `backup-wal`.

**Вход:** Tasks 8–10.

**Действие:**
1. `IClusterProcesses` — метод `Task<Result<ProcessOutcome>> RestoreAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)` + реализация `=> restoreProcess.TickAsync(snap, backups, ct)` (DI-параметр `PgWorker.Backups.RestoreProcess restoreProcess`).
2. `ReconcileLoop.ProcessClusterAsync` Active-ветка — после `backup-wal`, до `repair`:
```csharp
// Restore шардов из бэкапов (t05, arch/19 §3.5): после wal-потока (агент
// снесён демонтажем — процесс сам стопает), до repair/moves. Выключенная
// подсистема не зовётся.
if (options.CurrentValue.Backups.Enabled)
    await RunClusterOpAsync(cluster, "backup-restore",
        () => processes.RestoreAsync(snap, backups, ct), ct);
```
3. `Program.cs`: `builder.Services.AddSingleton(sp => new PgWorker.Backups.RestoreProcess(...))` — повторить способ существующей регистрации `BackupProcess` (etcd, endpoints, driver, `IBackupS3`, claims, journal, options, secrets, etcdEndpoints, appSecret, probe, thresholds, TimeProvider, logger).
4. Гвард `WalStreamProcess.TickShardAsync`: первой строкой — если `shardBackups?.Restores.Any(r => r.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true` → `return;` (агент снесён демонтажем restore; процесс поднял бы его на снесённом мастере — §3.4 «контуры не трогают шард»).

**Выход:** restore исполняется живым воркером; агент не поднимается на время restore.

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug` + `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalStreamProcessTests"` — зелёные; `dotnet build src/PgWorker.slnx -c Release` (0 warnings).

**Spec:** §3.4 (вызов из ClusterProcesses/ReconcileLoop после WalStreamAsync; одна заявка за тик; гварды), Ф2 (DI + врезка).

- [ ] **Шаг 1. Тесты (failing)**:
  - юнит ReconcileLoop (если существующий файл тестов цикла мокает `IClusterProcesses` — дописать AAA: при `Backups.Enabled=true` Active-кластер зовёт `RestoreAsync` после `WalStreamAsync`; при false — не зовёт; если мока нет — зафиксировать интеграцией ниже);
  - интеграция `WalStreamProcessTests`: `Тик_скипает_шард_с_активным_restore` (shardBackups с PLANNED restore → агент не ensure, wal-ключ не пишется).
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** (4 файла).
- [ ] **Шаг 4. Прогнать — зелёные** (юниты + интеграция wal) + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.App src/PgWorker.Backups/WalStreamProcess.cs src/tests && git commit -m "feat(backups): врезка RestoreProcess в ReconcileLoop + гвард WalStream (t05 §3.4/Ф2)"`.

---

### Task 12: Ф2 — гварды исключения: NodeSupervisor, BucketEvacuator, RemoveShard, D1/D2-чистка

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`
- Modify: `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs`
- Modify: `src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (только `BackupJobsCleaner`)
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` + `ReconcileLoop.cs` (сигнатуры SuperviseAsync/EvacuateAsync)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`, `BucketEvacuatorTests.cs`, `RemoveShardProcessTests.cs`, `DeprovisioningProcessTests.cs` (дописать кейсы)

**Interfaces:**
- Consumes: `ShardBackups.Restores` (Task 2).
- Produces:
  - `IClusterProcesses.SuperviseAsync(snap, backups, ct)` и `IClusterProcesses.EvacuateAsync(snap, deadShard, backups, ct)` — новые сигнатуры (парс бэкапов идёт в надзор и эвакуатор);
  - `NodeSupervisor.TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups>? backups, CancellationToken ct)`;
  - `BucketEvacuator.TickAsync(ClusterSnapshot snap, string deadShard, IReadOnlyList<ClusterBackups>? backups, CancellationToken ct)`.

**Вход:** Task 11.

**Действие:**
1. **NodeSupervisor** (`TickAsync(snap, backups, ct)`): в начале — собрать `var restoring = new HashSet<string>(...)` из `backups` (шарды с активным restore). Пропуск:
   - `EnsureDeclaredNodesAsync` — не зовётся для нод восстанавливаемых шардов (отфильтровать `snap.Shards` на входе шага 1);
   - цикл §2 (`foreach shard`) — `if (restoring.Contains(shard.Name)) continue;` (не пробы/не rebuild/не TO_RECREATE-обработка/не deadShards-кандидат — «не считает ShardDeadSec»);
   - `ConvergeDcsConfigAsync`-цикл — тот же скип.
2. **BucketEvacuator** (`TickAsync(snap, deadShard, backups, ct)`): гвард первой фазой — если `deadShard` имеет активный restore → `Result.InProgress` + журнал phase=`skipped-restore` (`"шард в restore — эвакуация не выполняется"`) и возврат БЕЗ записи `/pgworker/evacuations/<C>/<X>` (и `HandleReturnedShardAsync` не зовётся — вход общий). Это второй рубеж: надзор не отдаёт таких кандидатов, но DONE-журнал прошлой эвакуации/гонка снапшота не должны привести к эвакуации/карантину восстанавливаемого шарда (риск «ложная эвакуация» §7). `ClusterProcesses.EvacuateAsync` + `ReconcileLoop` — проброс `backups`.
3. **RemoveShardProcess.TickAsync**: после G-гвардов (перед S1.5) — чтение `/pgworker/backups/<C>/<X>/restore/`: активные заявки → каждая: put FAILED `error="cancelled-by-remove"` + `finished_unix`; точечная чистка restore-джоб-контейнеров шарда — новый маленький метод `IClusterDriver.RemoveRestoreJobsAsync(cluster, shard, ct)`: реализация в `PlainClusterDriver`/`SwarmClusterDriver` — list по префиксу `pgw-backup-restore-<C>-<X>-`, remove force (volume НЕ трогаем — это data-volume ноды, его снесёт RemoveNodeAsync). `BackupJobsCleaner.JobContainerPrefix` обобщить: чистить ДВА префикса `pgw-backup-full-<C>-` и `pgw-backup-restore-<C>-` (volume-выведение — только для full-имён; restore-джобу volume не принадлежит). Интерфейс `IClusterDriver.RemoveBackupJobsAsync` — обновить XML-комментарий («полные + restore»).
4. **AdoptionProcess/MoveRepairProcess — БЕЗ правок кода** (обоснование, зафиксировано здесь и в self-review): усыновление берёт в кандидаты только dsn-шарды с ОТСУТСТВУЮЩИМИ записями portalloc (`AdoptionProcess` AD1/AD2: «кандидаты — шарды с dsn; недостающие ноды = HA-members − portalloc», merge — «только отсутствующие записи»), а restore держит portalloc живым (§3.4: «portalloc/dsn НЕ трогаем») → шард не кандидат ни в одну ветку усыновления. MoveRepairProcess работает по routing-статусам переездов и живым заявкам moves (не по состоянию шарда): для снесённого мастера его последовательности дают обычный transient «мастер недоступен» — ровно то, что фиксирует канон arch/19 §3.5 («усыновление/репарация переездов не касаются; переезды бакетов получают обычный transient „мастер недоступен“»). Поведение естественно безопасно; отдельный гвард — YAGNI.
5. **D1/D2-тест deprovisioning**: расширить существующий прецедент `DeprovisioningProcessTests` (кейс «D2 t02 — убивает джобы бэкапов и чистит их префикс etcd», строки ~169–182) restore-кейсом: сид restore-ключа + restore-джоб в FakeDriver → тик `DeprovisioningProcess` → ассерты: `RemoveBackupJobsAsync` вызван (D1, теперь чистит и `pgw-backup-restore-`), префикс `/pgworker/backups/<C>/` пуст — включая `restore/<id>` (D2).

**Выход:** контуры воркера не вмешиваются в шард на время restore (AC6, вкл. эвакуатор); remove-shard/deprovision корректно гасят restore (ключи и джобы).

**Проверка:** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeSupervisor|FullyQualifiedName~BucketEvacuator|FullyQualifiedName~RemoveShard|FullyQualifiedName~Deprovisioning"` зелёные; `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestoreProcessTests"` зелёные.

**Spec:** §3.4 (гварды исключения — NodeSupervisor/BucketEvacuator/BackupProcess/RemoveShard/D1; Adoption/MoveRepair не касаются), §3.1 (D2 чистит весь префикс), AC6, §7 (риск «ложная эвакуация»).

- [ ] **Шаг 1. Тесты (failing)**:
  - NodeSupervisor: `Надзор_скипает_шард_с_активным_restore` (backups с RUNNING restore → EnsureDeclaredNodes не касается нод шарда, probe не зовётся, deadShards пуст даже при «мёртвых» нодах);
  - BucketEvacuator: `Эвакуатор_не_берёт_шард_с_активным_restore` (снапшот с deadShard-кандидатом + backups с RUNNING restore шарда → тик: InProgress, ключа `/pgworker/evacuations/<C>/<X>` НЕТ, схемы на целевых шардах не создавались, журнал `skipped-restore`; контрольный кейс без restore — прежнее поведение уже покрыт существующими тестами эвакуатора);
  - RemoveShard: `Remove_помечает_активный_restore_FAILED_cancelled_by_remove_и_чистит_джобы` (сид restore RUNNING → после тика статус FAILED error=cancelled-by-remove, `RemoveRestoreJobsAsync` вызван);
  - Deprovisioning: `D1_D2_restore_джобы_убиты_и_ключи_префикса_не_переживают` (п.5).
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** (NodeSupervisor-скипы; BucketEvacuator-гвард; сигнатуры SuperviseAsync/EvacuateAsync в ClusterProcesses/ReconcileLoop; RemoveShard-ветка; `RemoveRestoreJobsAsync` в IClusterDriver+Plain+Swarm+FakeDriver(тестовый); BackupJobsCleaner).
- [ ] **Шаг 4. Прогнать — зелёные** (юниты воркера целиком — сигнатуры заденут тесты ReconcileLoop/ClusterProcesses) + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.Provisioning src/PgWorker.Docker src/PgWorker.App src/tests && git commit -m "feat(backups): гварды исключения restore — надзор/эвакуатор/remove-shard/D1+D2 (t05 §3.4, AC6)"`.

---

### Task 13: Ф3 — API: RestoreShardHandler + роут + WAF-тесты

**Files:**
- Create: `src/PgWorker.App/Api/Operations/RestoreShardHandler.cs`
- Modify: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs`
- Modify: `src/PgWorker.App/Program.cs` (DI)
- Test: `src/tests/PgWorker.IntegrationTests/Api/RestoreApiTests.cs` (новый; WAF по образцу `RecreateRotateApiTests`)

**Interfaces:**
- Consumes: `BackupNames.RestoreKey`, `RestoreStatusJson`, `RestoreOperationState` (заполняет Node/Target/Source сам), кластерные гварды `ClusterGuardData.ReadAsync`, `EtcdFailover.CallAsync`, `TxnRequest.Of` (клэйм-паттерн ротации).
- Produces:
  - `record RestoreShardRequest(string? BackupId, string? TargetTime, string? SourceCluster, string? SourceShard, string Confirm, string? RequestedBy)` (JSON-имена snake_case: `backup_id`/`target_time`/`source_cluster`/`source_shard`/`confirm`/`requested_by`).
  - `record RestoreRequestedDto(string Cluster, string Shard, string RestoreId, string State, string Target, string Source)` — 202.
  - Исключения (в `WorkerApiExceptions.cs`): `ConfirmMismatchException(string expected, string got)` → 400 (`confirm обязан совпадать с именем шарда: ожидался '<X>'`); `InvalidTargetTimeException(string raw)` → 400 (`target_time не RFC3339: '<raw>'`); `RestoreAlreadyActiveException(string cluster, string shard)` → 409 (`restore шарда <C>/<X> уже активен — дождитесь завершения (ключ restore/<id>)`); `RestoreValidationException(IReadOnlyList<ValidationError> errors)` → 400 с errors по полям (паттерн `CreateClusterValidationException`); `ShardNotFoundException` (существует) → 404.

**Вход:** Task 2/3 (модель+json), Task 11.

**Действие:**
1. `RestoreShardHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)`:
   - имена канонические (`^[a-z][a-z0-9_]{0,62}$` кластер, `^[a-z][a-z0-9_]{0,30}$` шард — как `RecreateNodeHandler`);
   - `ClusterGuardData.ReadAsync`: config нет/битый → 404/503; state не Active → 409 (`ClusterNotActiveException`); шард не в декларации (нет нод `<X>/`) → 404 `ShardNotFoundException`;
   - `confirm != shard` → 400 `ConfirmMismatchException`;
   - `target_time` задан → парсинг: `DateTimeOffset.TryParseExact` форматы RFC3339 (`"yyyy-MM-ddTHH:mm:ssK"`, `"yyyy-MM-ddTHH:mm:ss.fffffffK"`) — не спарсился → 400 `InvalidTargetTimeException`; спарсился → `target = $"time:{targetTime}"` (исходная строка оператора), иначе `target = "latest"`;
   - source: `source_cluster`+`source_shard` оба или никто (один → `RestoreValidationException` с errors по полям); заданы → `source = $"{sc}/{sx}"`, иначе `$"{cluster}/{shard}"`;
   - активный restore уже есть: range `/pgworker/backups/<C>/<X>/restore/` → парс JSON state, любая активная (PLANNED/RUNNING/REJOINING) → 409;
   - `id = BackupPlanner.NextId(существующие restore-ids, time.GetUtcNow().UtcDateTime)`;
   - клэйм-txn put-if-not-exists (`TxnCompare.NotExists(RestoreKey)`, образец ротации): проигрыш → 409; статус PLANNED: `Node` = min имя ноды декларации шарда, `RequestedUnix`, `RequestedBy` (тело → X-Requested-By → "api");
   - успех → `RestoreRequestedDto`.
2. Роут в `ApiModule` (POST, тело JSON, битое тело/нет тела → 400 «Invalid body» как recreate):
```csharp
// POST /api/clusters/{cluster}/shards/{shard}/restore — заявка восстановления
// шарда из бэкапа (t05, arch/19 §3.5/§4): 202 Accepted (не 201 — операция
// разрушающая, долгая; воркер пишет PLANNED-ключ сам, исполняет держатель
// клэйма). Гварды: confirm/активная заявка/RFC3339/Active/шард заявлен.
endpoints.MapPost("/api/clusters/{cluster}/shards/{shard}/restore", async (
    string cluster, string shard, RestoreShardRequest? body, HttpRequest http,
    RestoreShardHandler handler, CancellationToken ct) =>
{
    if (body is null)
        return Results.Problem(statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid body", detail: "тело запроса обязательно: {\"confirm\":\"<shard>\", …}");
    var requestedBy = body.RequestedBy
        ?? (http.Headers.TryGetValue("X-Requested-By", out var by) && !string.IsNullOrWhiteSpace(by)
            ? by.ToString() : "api");
    var result = await handler.HandleAsync(cluster, shard, body, requestedBy, ct);
    if (result.IsSuccess)
        return Results.Accepted((string?)null, result.Value); // 202
    return result.Error switch
    {
        RestoreValidationException validation => Results.Problem(statusCode: 400, title: "Validation failed",
            detail: result.Error.Message, extensions: new Dictionary<string, object?>
            { ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }) }),
        ConfirmMismatchException or InvalidTargetTimeException => Results.Problem(statusCode: 400,
            title: "Restore rejected", detail: result.Error.Message),
        ClusterNotFoundException or ShardNotFoundException => Results.Problem(statusCode: 404,
            title: "Not found", detail: result.Error.Message),
        ClusterNotActiveException or RestoreAlreadyActiveException => Results.Problem(statusCode: 409,
            title: "Restore rejected", detail: result.Error.Message),
        EtcdWriteUnavailableException => Results.Problem(statusCode: 503,
            title: "Etcd write unavailable", detail: result.Error.Message),
        _ => Results.Problem(statusCode: 503, title: "Etcd write failed", detail: result.Error!.Message),
    };
});
```
3. DI: `builder.Services.AddSingleton(sp => new RestoreShardHandler(sp.GetRequiredService<IEtcdGateway>(), endpoints, sp.GetRequiredService<TimeProvider>()))` (по образцу RecreateNodeHandler).

**Выход:** оператор ставит заявку через mTLS-API (AC5).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestoreApiTests"` — зелёные; зачистка серии.

**Spec:** §3.2 (эндпоинт, валидации, 202/400/409/404), AC5, §1 решения 1/4.

- [ ] **Шаг 1. WAF-тесты (failing)** — `RestoreApiTests.cs` (WAF-фабрика `PgApiFixture`; сид `ApiTestSeed.SeedActiveClusterAsync`):
  - `Restore_202_и_PLANNED_ключ_в_etcd` (body confirm=shard1 → 202; etcd-ключ `/pgworker/backups/<c>/shard1/restore/<id>` содержит `"state":"PLANNED"`, `"target":"latest"`, `"requested_by"`);
  - `Restore_confirm_мисматч_400` (confirm=чужое → 400, тело содержит ожидаемое имя; ключа нет);
  - `Restore_target_time_не_RFC3339_400`;
  - `Restore_активная_заявка_409` (сид PLANNED-ключа → повторный POST 409, значение не перезаписано);
  - `Restore_кластер_не_Active_409` (config с state=TO_REMOVE);
  - `Restore_шард_не_заявлен_404` (shard9);
  - `Restore_кластер_нет_404`;
  - `Restore_source_cluster_без_source_shard_400` (errors по полям);
  - `Restore_без_тела_400`.
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** handler+исключения+роут+DI.
- [ ] **Шаг 4. Прогнать — зелёные** + зачистка серии.
- [ ] **Шаг 5. Коммит**: `git add src/PgWorker.App src/tests/PgWorker.IntegrationTests && git commit -m "feat(api): POST /api/clusters/{c}/shards/{x}/restore — заявка PITR-restore, 202/400/404/409 (t05 §3.2, AC5)"`.

---

### Task 14: Ф4 — панель: парсер restore + правило restore-failed

**Files:**
- Modify: `src/AdminPanel.Core/BackupsInfo.cs` (или `BackupInfo.cs` — модель агрегата)
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/RestoreFailedRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` (дописать), `src/tests/AdminPanel.UnitTests/RestoreFailedRuleTests.cs` (новый, по образцу `BackupFullStaleRuleTests`)

**Interfaces:**
- Produces:
  - `record RestoreOperationInfo(string Cluster, string Shard, string Id, string State, string? Error, long RequestedUnix, long? StartedUnix, long? FinishedUnix, string? Phase)` — «только поля статусов» (панель UI — t08).
  - `ClusterBackupsInfo` — добавить `IReadOnlyDictionary<string, IReadOnlyList<RestoreOperationInfo>>? ShardsRestores = null`.
  - `RestoreFailedRule : IAlertRule`, `Kind = "restore-failed"`, critical.

**Вход:** Task 2 зафиксировал etcd-формат (панель читает тот же контракт).

**Действие:**
1. Модель `RestoreOperationInfo` (в `BackupsInfo.cs` рядом с WalStreamInfo).
2. `AdminPanel.Etcd/Parsing/BackupsParser`: ветка `segments.Length == 7 && segments[5] == "restore"` → JSON: обязательны `state` (строка из PLANNED|RUNNING|REJOINING|COMPLETED|FAILED) и `requested_unix` (число); опциональны error/started_unix/finished_unix/phase; битое → `KeyParseError` + пропуск; агрегировать в `ShardsRestores[cluster][shard]` (сортировка по Id).
3. `RestoreFailedRule` (по образцу `WalStreamStoppedRule`):
```csharp
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class RestoreFailedRule : IAlertRule
{
    public const string KindName = "restore-failed";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, restores) in backups.ShardsRestores ?? new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>())
        foreach (var restore in restores.Where(r => r.State == "FAILED"))
        {
            // только живой Active-кластер (демонтаж удаляет ключи сам)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null) continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}/{restore.Id}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"восстановление шарда {shard} кластера {backups.Cluster} не удалось: {restore.Error ?? "без причины"}",
                new Dictionary<string, string>
                {
                    ["restoreId"] = restore.Id,
                    ["error"] = restore.Error ?? string.Empty,
                },
                null,
                "разбор по docs/backup-restore.md (диагностика FAILED) и повтор заявки restore",
                AlertRemedy.OperatorRunbook,
                "активный restore (PLANNED/RUNNING/REJOINING) без алерта — фазы видны в статусе");
        }
    }
}
```
4. `SnapshotBuilder` — прокинуть `ShardsRestores` в `ClusterBackupsInfo` (найти место сборки `BackupsParseResult` → `ClusterBackupsInfo` и дополнить маппинг).

**Выход:** панель видит restore-операции, FAILED зажигает critical-алерт (AC8).

**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~BackupsParser|FullyQualifiedName~RestoreFailed"` — зелёные.

**Spec:** §3.7 (панель: парсер + restore-failed), AC8.

- [ ] **Шаг 1. Тесты (failing)**:
  - парсер: restore-ключ → `RestoreOperationInfo`; битый state → KeyParseError; шард без restore — пусто/отсутствует;
  - правило: FAILED живого Active-кластера → один critical-алерт с error в тексте и remedy про runbook; RUNNING/COMPLETED — алерта нет; FAILED кластера нет в `Clusters` (удалён) — алерта нет.
- [ ] **Шаг 2. Прогнать — падают.**
- [ ] **Шаг 3. Реализация** (4 файла + SnapshotBuilder).
- [ ] **Шаг 4. Прогнать — зелёные**: весь `dotnet test src/tests/AdminPanel.UnitTests -c Debug` (регрессия парсера).
- [ ] **Шаг 5. Коммит**: `git add src/AdminPanel.Core src/AdminPanel.Etcd src/tests/AdminPanel.UnitTests && git commit -m "feat(adminpanel): парсер restore-ключей + правило restore-failed (critical) (t05 §3.7, AC8)"`.

---

### Task 15: Ф5 — E2E: Restore_Latest_RebuildsDestroyedShard (AC1 + AC4)

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs`
- Modify (при необходимости): `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (ничего менять не планируется — `withMinio:true` уже есть)

**Interfaces:**
- Consumes: `E2eEnvironment.StartAsync(slug, withMinio: true)` (per-method изоляция, динамические порты, teardown в DisposeAsync), `Fx.StartHostAsync(name, extraEnv)`, `Fx.RunDockerAsync`, `E2eFixture.WaitForAsync`.

**Принятое отступление (ревью Ф4, зам. 5):** spec Ф5/AC1 описывает E2E как «POST restore latest → COMPLETED», но E2E ставит заявку **прямым PUT PLANNED-ключа в etcd** (формат `RestoreStatusJson`) по прецеденту `E2eRotateScenarios` (заявка ротации туда же пишется прямым put, а не через API). Обоснование: E2E проверяет механику воркера (процесс/джоб/rejoin/инварианты), а API-путь (HTTP-гварды, 202/400/409/404, клэйм-запись) полностью покрыт WAF-тестами Task 13 (AC5) и будет пройден на ручном стенде Ф6/Task 18 через реальный curl; mTLS-грань E2E-хоста не усложняется клиентскими сертами в тесте. Spec НЕ меняется; отступление живёт здесь и в Self-Review.

**Вход:** Tasks 8–13 смержены; `docker/PgWorker.Backup.Dockerfile` и образы `pgworker-node:e2e`/`pgworker-backup:e2e` собираются `E2eFixture`.

**Действие:** тест `Restore_Latest_RebuildsDestroyedShard` (Release, `PGW_TEST_DOCKER=1`, `DockerTrait.SkipIfUnavailable`, AAA):
1. **Arrange**: `E2eEnvironment.StartAsync("rs-latest", withMinio: true)`; сид кластера (копия `SeedClusterAsync` из `E2eBackupScenarios`); хост с бэкап-комплектом (копия `StartWalHostAsync`); дождаться Active/dsn (бюджет ≤ 300 c).
2. Контрольные данные: через master-pg (паттерн `MasterPgAsync` + `BuildAdminDsn`) `CREATE TABLE e2e_restore_probe(id int, note text); INSERT 1..N`.
3. Полный COMPLETED + WAL-сегменты: ждать `full/`-ключ COMPLETED (≤ 300 c); `GenerateWalAsync`-нагрузка (INSERT + `pg_switch_wal`) → ждать ≥ 2 сегментов в `wal/` (ListWal, ≤ 300 c).
4. **Act**: симуляция гибели — снос контейнеров+volume нод шарда: `Fx.RunDockerAsync(["rm","-f", ...$pgw-<c>-<X>-<n>...])` + volume rm `pgw-<c>-<X>-<n>-data` (полный docker-снос, как «погиб диск»); поставить restore-заявку PUT-ключа PLANNED (`RestoreStatusJson.Serialize(new RestoreOperationState(<id>, Planned, "", $"<C>/<X>", "latest", firstNode, now, "e2e"))` — backup_id пуст → валидация возьмёт новейший COMPLETED; см. «Принятое отступление» выше).
5. **Assert** (поллинг ≤ 300 c на пункт):
   - restore-ключ → COMPLETED, `restored_to_lsn` не пуст;
   - SELECT: строки `e2e_restore_probe` читаются через master-pg шарда (данные восстановлены, включая послебэкапные — WAL накатился до latest);
   - ноды шарда `state=RUNNING`, dsn-ключ не изменился (снять до/после);
   - **AC4**: ключ `wal` удалён; появился новый `full/<id2>` PLANNED/RUNNING (планировщик §3.5); агент вернулся: контейнер `pgw-backup-wal-<c>-<X>` running + ключ wal снова ACTIVE (после нового полного).
   - контроль чистоты teardown — уже в `E2eEnvironment.DisposeAsync` (сеть/etcd/MinIO/контейнеры слага).
6. Диагностика провала — дамп журнала `/pgworker/work/<c>`, restore-ключа, `docker ps` (паттерн Assert-сообщений `E2eBackupScenarios`).

**Выход:** AC1+AC4 закрыт автоматическим docker-E2E на свежем Release.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Restore_Latest_RebuildsDestroyedShard"` — PASS; после прогона ОБЯЗАТЕЛЬНО `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f; docker ps -aq | wc -l` → 0.

**Spec:** §4 Ф5 (кейс 1), AC1, AC4; отступление PUT-vs-POST — см. блок выше.

- [ ] **Шаг 1. Прогон сборки Release**: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` (0 warnings).
- [ ] **Шаг 2. Написать тест** (полный код — по хелперам `E2eBackupScenarios`, скопировать `SeedClusterAsync`/`StartWalHostAsync`/`MasterPgAsync`/`GenerateWalAsync` в private-секцию `E2eRestoreScenarios`; AAA-комментарии).
- [ ] **Шаг 3. Прогон маркера** — зелёный (при падении: диагностика по dump'у, чинить код, НЕ бюджеты; бюджеты ≤ 300 c фиксированы).
- [ ] **Шаг 4. Зачистка серии** (команда выше; проверить `docker ps -aq | wc -l == 0`).
- [ ] **Шаг 5. Коммит**: `git add src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs && git commit -m "test(e2e): Restore_Latest_RebuildsDestroyedShard — восстановление уничтоженного шарда + пост-инвариант (t05 Ф5, AC1/AC4)"`.

---

### Task 16: Ф5 — E2E: PITR target_time (AC2) + DR source-override (AC3)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs` (дописать два кейса)

**Вход:** Task 15 (хелперы scenario-класса готовы, маркер зелёный). Заявки restore — прямым PUT PLANNED-ключа (принятое отступление Task 15).

**Действие:**
1. `Restore_TargetTime_PitrRollback` (AAA):
   - Arrange: кластер Active; данные T0 (таблица `pitr_probe` со строками T0); полный COMPLETED (≤ 300 c); данные T1 (новые строки T1); WAL-нагрузка `pg_switch_wal` (сегменты ушли в S3); зафиксировать `T_cut = UtcNow` (RFC3339, до порчи); после паузы ≥ 2 c и ещё WAL-нагрузки — «порча»: `DROP TABLE pitr_probe` (+ сегмент);
   - Act: PUT restore PLANNED с `Target = $"time:{T_cut:yyyy-MM-ddTHH:mm:ssZ}"`;
   - Assert (≤ 300 c): COMPLETED + `restored_to_lsn`; таблица `pitr_probe` ЖИВА со строками T0 и T1 (данные до цели), повторно SELECT — порчи нет; AC4-инвариант (wal удалён/новый полный) — по образцу Task 15.
2. `Restore_NewCluster_FromSourcePrefix` (DR, AAA):
   - Arrange: кластер A с данными и полным COMPLETED (бэкапы в `A/shard1`); запомнить контрольные строки; **deprovision**: PUT config `state=TO_REMOVE` → дождаться: пустого `/clusters/A/` (etcd чист), **пустого префикса `/pgworker/backups/A/`** (явный D2-ассерт: range по префиксу возвращает 0 ключей — restore/full/wal не переживают кластер, AC6), при этом S3 жив — ассерт `ListFulls(A, "shard1")` не пуст (≤ 300 c);
   - Act: пересоздать кластер A той же декларацией (`SeedClusterAsync`-сид заново) → Active (пустые шарды, ≤ 300 c); на каждый шард PUT restore PLANNED `Source="A/shard1"` (source-override; префикс S3 прежний — фактический DR-механизм list-S3 без etcd-статусов: их нет после deprovision);
   - Assert: COMPLETED; SELECT контрольных строк через master-pg — данные совпадают с моментом бэкапа.

**Выход:** AC2/AC3 закрыты E2E; D2-чистка префикса подтверждена end-to-end.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2eRestoreScenarios"` — все три зелёные; зачистка серии (см. Task 15).

**Spec:** §4 Ф5 (кейсы 2/3), §3.1 (D2 чистит весь префикс), AC2, AC3, AC6 (D2-часть).

- [ ] **Шаг 1. Написать два теста** (AAA; диагноз-дампы в сообщениях ассертов).
- [ ] **Шаг 2. Прогон серии** — зелёные; упало — чинить по dump'у.
- [ ] **Шаг 3. Зачистка серии** + `docker network ls | grep -c <slug>` → 0.
- [ ] **Шаг 4. Коммит**: `git add src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs && git commit -m "test(e2e): Restore_TargetTime_PitrRollback + Restore_NewCluster_FromSourcePrefix + D2-ассерт (t05 Ф5, AC2/AC3/AC6)"`.

---

### Task 17: Ф6 — runbook `docs/backup-restore.md`

**Files:**
- Create: `docs/backup-restore.md`

**Вход:** Tasks 13–16 (API/etcd-контракт/сценарии финализированы).

**Действие:** runbook на русском (образец тона — `docs/runbook.md`):
1. **Сводка механики**: что делает воркер по фазам (PLANNED-валидация → RUNNING-демонтаж → джоб downloading/recovering → REJOINING → COMPLETED; сброс wal-ключа → новый полный), необратимость и RPO = точка бэкапа/WAL-цели, S3 только читается.
2. **Команды** (curl 1:1 API): `POST /api/clusters/{c}/shards/{x}/restore` с телами latest/target_time/source-override/confirm; чтение статусов etcdctl'ом (`etcdctl get --prefix /pgworker/backups/<C>/<X>/restore/`, ключ `wal`, `full/`).
3. **Сценарий 1 — полная потеря ноды**: живой шард → `POST /api/ha/{scope}/nodes/{node}/recreate` (Patroni rebuild; restore НЕ нужен); весь шард мёртв (алерты provision/shard-no-leader) → restore latest → контрольные SELECT.
4. **Сценарий 2 — порча данных**: остановить запись → определить время порчи → restore `target_time` за минуту до → проверка → новый полный снимется сам.
5. **Сценарий 3 — DR (новый кластер)**: etcd-контур погиб, S3 жив → пересоздать кластер декларацией (`POST /api/clusters`) → Active → restore с `source_cluster`/`source_shard` → SELECT-сверка.
6. **Таблица ошибок FAILED**: дыра цепочки / target не достигнут (конец WAL раньше цели) / бюджет наката (`Restore:RecoveryTimeoutSec`) / бэкап не найден / полный без backup_manifest (недокачан) / усыновлённый шард — причина + разбор + повтор заявки (новая заявка, отмена бегущего — YAGNI).

**Выход:** AC9 (документальная часть).

**Проверка:** файл существует; все curl-команды соответствуют роуту Task 13 (селф-чек по ApiModule); ключи etcd — по §3.1 spec.

**Spec:** §3.9, AC9.

- [ ] **Шаг 1. Написать runbook** (разделы 1–6).
- [ ] **Шаг 2. Сверка команд**: путь/поля/коды ответов против `ApiModule`/`RestoreShardHandler`; статусы против `RestoreStatusJson`.
- [ ] **Шаг 3. Коммит**: `git add docs/backup-restore.md && git commit -m "docs: runbook backup-restore — сценарии latest/target_time/DR, диагностика FAILED (t05 §3.9, AC9)"`.

---

### Task 18: Ф7 — мерж-гейт: полный Release-прогон + ручной стенд + roadmap-гейт

**Files:**
- Modify: `arch/roadmap/backup.md` (снятие тега t05) — В КОММИТЕ МЕРЖА (см. Шаг 6).

**Вход:** Tasks 1–17 в ветке; dev-стенд доступен (`dev-stand/adminpanel/checks/00-up.sh`, deploy/).

**Действие:**
1. Полный прогон на свежем Release (каждая серия — с зачисткой `docker rm -f $(docker ps -aq)` (не трогая стенд `as-*`/`adminpanel`, если поднят) + `docker network prune -f` МЕЖДУ сериями):
   - юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName!~IntegrationTests"`;
   - интеграция воркера (etcd/WAF): `dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Restore"`;
   - AdminPanel: `dotnet test src/tests/AdminPanel.UnitTests -c Release`;
   - E2E-маркеры: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eRestoreScenarios` (свежий Release-бинарь — `PGW_TEST_E2E_NOBUILD` запрещён, урок t09).
2. Ручной прогон dev-станда (AC9): `dev-stand/adminpanel/checks/00-up.sh` (etcd-контур один, `as-etcd`); прогнать ТРИ сценария runbook по `docs/backup-restore.md` через реальный API (latest-restore уничтоженного шарда стенда; target_time; DR) — зафиксировать в задаче команды/SELECT-сверки; зачистка: restore-джобы и тестовые ключи стенда.
3. Ревью канона: правки Ф0 в ветке, расхождений с реализацией нет.
4. roadmap-гейт: удалить пункт `t05-backup-restore` из `arch/roadmap/backup.md` (и `←`-зависимости других пунктов — проверить t07 `← t04, t05, t06`: снять только `t05-backup-restore`) ТЕМ ЖЕ коммитом мержа в `main`.

**Выход:** AC10 — ветка готова к мержу в `main`.

**Проверка:** все серии зелёные; `docker ps -aq | wc -l` → 0 после финальной зачистки; `grep -rn "t05-backup-restore" arch/roadmap/` → пусто (в мерж-коммите); ручной стенд — результаты SELECT-сверок зафиксированы в задаче.

**Spec:** §4 Ф7, AC9/AC10; AGENTS.md (E2E на свежем Release, зачистка, roadmap-гейт).

- [ ] **Шаг 1. Юниты Release** — зелёные.
- [ ] **Шаг 2. Интеграция Release** (`Restore*`) — зелёные + зачистка.
- [ ] **Шаг 3. AdminPanel Release** — зелёные.
- [ ] **Шаг 4. E2E Release** (`PGW_TEST_DOCKER=1`, маркеры `E2eRestoreScenarios`) — зелёные + зачистка + `docker network ls | grep -c <slug-prefix>` → 0.
- [ ] **Шаг 5. Ручной стенд**: подъём + три сценария runbook (через реальный API) + фиксация результатов.
- [ ] **Шаг 6. Запрос ревью** (superpowers:requesting-code-review) и после аппрува — мерж в `main` коммитом, включающим снятие тега `t05-backup-restore` из `arch/roadmap/backup.md` (+ `←`-зависимости t07). Пуш — только по требованию пользователя.

---

## Self-Review (выполнен автором плана)

**Покрытие spec:** §3.1→Tasks 2/3 (+8: гвард процесса «максимум один активный»); §3.2→Task 13; §3.3→Tasks 4/5 (env-контракт путей PGDATA); §3.4→Tasks 8–12 (вкл. BucketEvacuator-гвард); §3.5→Task 7; §3.6→Tasks 5/6; §3.7→Task 14; §3.8→Task 5; §3.9→Task 17; §4 Ф0–Ф7→Tasks 1–18; AC1–AC10→Tasks 13–18 (AC1/4→15, AC2/3→16, AC5→13, AC6→12/16 (D1+D2-ассерты в обоих), AC7→8/9/10, AC8→14, AC9→17/18, AC10→18). Гэпов нет.

**Вне скоупа (spec §1):** verify полных (t04), ретенция (t06), супервизор-reconcile (t07), UI (t08), отмена бегущего restore, инкрементальные резюмы — задач нет, верно.

**Типы/сигнатуры:** `RestoreOperationState` (Task 2) единообразно используется в 3/8/9/10/13/15; `IBackupS3` расширяется один раз (Task 6); `IsDue` меняет сигнатуру один раз (Task 7) с обновлением вызова; `SuperviseAsync`/`EvacuateAsync` меняют сигнатуры один раз (Task 12) с обновлением ReconcileLoop и тестов; `RestoreJobSpec.Build(opts, cluster, shard, id, nodeVolumeName, targetTime, srcCluster, srcShard, dataDir="/restore")` едина в Tasks 5/9.

**Принятые отступления/решения (по итогам ревью Ф4):**
1. **E2E ставит заявку прямым PUT PLANNED-ключа, а не POST API** (Tasks 15/16): прецедент `E2eRotateScenarios`; API-путь полностью покрыт WAF-тестами Task 13 (AC5) и ручным стендом Task 18 (реальный curl). Spec не меняется.
2. **AdoptionProcess/MoveRepairProcess — без гвардов** (Task 12 п.4): усыновление берёт только dsn-шарды с отсутствующими portalloc-записями (restore держит portalloc живым — не кандидат); MoveRepairProcess работает по routing-статусам и получает обычный transient «мастер недоступен» — ровно семантика канона arch/19 §3.5. Гвард — YAGNI.
3. **DR-выбор полного — проверка backup_manifest фактом** (Task 8 п.4), а не «инвариант атомарности»: upload t02 (`mc cp --recursive`) не атомарен и не гарантирует манифест последним — упавший джоб оставляет частичный `full/<id>/`, который отсеивается `DownloadTextAsync(full/<id>/backup_manifest)`.
4. **Дубли активных заявок шарда** (Task 8): API-гвард (txn 409) — первичный; процессный гвард гасит младшие дубли permanent-FAILED «дубль заявки: активен старейший <id>» (ручная запись/гонка), старейший исполняется.
5. **Пути DATA_DIR/PGDATA — env-контракт джоба** (Tasks 4/5: `PGW_RESTORE_DATA_DIR`/`PGW_RESTORE_PGDATA` со скриптовыми дефолтами) — как требует spec §3.3 (env: «…PGDATA-путь, бюджет recovery»).
