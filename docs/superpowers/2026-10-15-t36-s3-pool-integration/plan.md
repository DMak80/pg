# t36-s3-pool-integration — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** клиентская интеграция пула S3-серверов (ownS3, домен `/s3/`) в подсистему бэкапов PgWorker: явный per-cluster выбор пула, приоритеты нод записи per-cluster (отдельные списки мастера/реплики), etcd-слепки на отдельную env-пару, креды-профили, страница «Пул бэкапов» панели.

**Архитектура:** PgWorker — читатель `/s3/clusters/<C>/{config,nodes/<I>/state,endpoints}` + `/s3worker/portalloc/<C>` (резолв имён нод в адреса). Policy бэкапов (`/pgworker/backups/<C>/policy`) получает опциональную секцию `s3` со ссылками `pool + имена нод`; воркер резолвит списки в адреса и строит пул-клиент `PoolBackupS3` (перебор нод: запись — список роли источника, чтение — все ноды пула). Env-контракты агентов/джобов несут списки endpoints; entrypoint-скрипты перебирают mc-хосты с per-попыточным таймаутом. Панель — read-минимум `/s3/` + модал-мутация через API воркера.

**Тех-стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK.S3, xUnit, testcontainers (E2E), React+Mantine (панель), docker-compose (стенд).

**Spec:** `docs/superpowers/2026-10-15-t36-s3-pool-integration/spec.md` (рядом; исполнитель читает spec и план вместе).

## Глобальные ограничения

- **arch-first**: контракт меняется сначала в `arch/` (Task 1), затем код. Код зеркалит канон.
- .NET 10, `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`**; пакеты — централизованно в `src/Directory.Packages.props`.
- PgWorker **в `/s3/` не пишет** — только чтение (arch/23 преамбула/§7).
- **Один bucket per-install** (`PgWorker:Backups:S3:Bucket`, env `PGW_BACKUP_S3_BUCKET`), layout `<C>/<X>/…` без изменений; разные пулы — раздельные экземпляры bucket.
- Секреты — **только env** (`PGW_BACKUP_S3_PROFILES__*`, `PGW_SNAPSHOTS_S3_*`), не etcd/не git; в диагностиках — только имена, не значения.
- Policy: отсутствие секции `s3` = пул не выбран — подсистема бэкапов кластера **не пишет и не читает** (алерт панели `backup-pool-missing`); никаких env-дефолтов.
- Имена: пул `<C>` и инстансы `<I>` — `^[a-z][a-z0-9_]{0,62}$`; поля списков в policy — `write_master`/`write_replica` (по роли источника). **Инстансы пула — ключи `nodes/<I>/…` (placement/state)**; portalloc — только адреса (нода заявлена без portalloc-записи = заявлена, но не провижинена — это не инвалид policy).
- Маркер исчерпания пула — строка-литерал **`s3-pool-exhausted`** в `Error` статусов воркера (wal `DEGRADED` / full `FAILED`) — по нему панель зажигает `backup-pool-unreachable`.
- Перебор нод пула — «быстрый отказ/успех, не молчание» (R3, arch/23 §7): каждая попытка несёт per-попыточный таймаут (S3-клиент — `requestTimeout`; mc-скрипты — coreutils `timeout`).
- Тесты: docker-порты только динамические (зонд/`GetMappedPublicPort`), полная самоочистка контуров, ассерт чистоты, бюджеты ожидания ≤ 100 с, изоляция по `docs/e2e-isolation.md`, телеметрия по `docs/e2e-launch.md`. Комментарии тестов — AAA.
- Комментарии/доки — по-русски; идентификаторы — английские. В документах (кроме `docs/superpowers/`) — только текущее/планируемое состояние, без истории.
- Feature-ветка: коммиты свободны; мерж в `main` и roadmap-чистка — мерж-гейтом (Task 21).
- Чистка docker-контейнеров/сетей между сериями тестов обязательна (`docker rm -f` контейнеры, `docker network prune -f` при осиротевших `pgw-*`/`kfw-net-*` сетях).

## Карта файлов

Новые (Create):

| Файл | Ответственность |
|---|---|
| `src/PgWorker.Etcd/S3/S3PoolModel.cs` | Модель `S3Pool`/`S3PoolNode` + парсинг `config`/`endpoints`-строки/portalloc |
| `src/PgWorker.Etcd/S3/S3PoolStore.cs` | Чтение `/s3/clusters/<C>/{config,nodes/<I>/state,nodes/<I>/placement,nodes/<I>/placed_on,endpoints}` + `/s3worker/portalloc/<C>`, резолв `<I>→host:port` |
| `src/PgWorker.Backups/BackupSecretProfileStore.cs` | env-профили `PGW_BACKUP_S3_PROFILES__<P>__{ACCESS_KEY,SECRET_KEY}` |
| `src/PgWorker.Backups/BackupPoolResolver.cs` | policy.s3 + пул + профили → `BackupPoolContext` (WriteMaster/WriteReplica/ReadAll/креды) |
| `src/PgWorker.Backups/PoolBackupS3.cs` | `IBackupS3` с перебором нод (write-список/ReadAll); фабрика целей `BackupS3` |
| `src/PgWorker.Backups/S3PoolMarkers.cs` | Литерал `s3-pool-exhausted` (общий для воркера/приёмника/джобов) |
| `src/PgWorker.App/Api/Operations/BackupsPolicyS3Validator.cs` | Валидация секции `s3` policy-API (пул существует, не TO_REMOVE, имена по `nodes/<I>/placement`, списки) |
| `src/AdminPanel.Core/S3PoolInfo.cs` | Панельная модель пула (config/nodes/endpoints) |
| `src/AdminPanel.Etcd/S3/S3PoolSnapshotStore.cs` + `S3PoolSnapshotRefresher.cs` | Read-минимум `/s3/clusters/*/` (по образцу Valkey-домена) |
| `src/AdminPanel.Core/Alerting/Rules/BackupPoolMissingRule.cs`, `BackupPoolUnreachableRule.cs` | Новые алерты |
| `frontend/src/pages/backups-storage/BackupsPoolModal.tsx` | Модал «Пул бэкапов» (Select пула + два упорядоченных списка) |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupPoolScenarios.cs` | E2E-серия AC1–AC8 пула |

Изменяемые (Modify) — ключевые:

| Файл | Что меняется |
|---|---|
| `arch/19-backups.md`, `arch/14-pgworker.md`, `arch/23-s3-clusters.md`, `arch/adminpanel/02-etcd-contract.md`, `arch/adminpanel/03-panels.md` | Таблица arch-правок spec §3 |
| `src/PgWorker.Etcd/Parsing/BackupsModel.cs` + `BackupsParser.cs` | `BackupPolicy.S3` (опц.), толерантность |
| `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`) | Уход endpoint/кредов/advertised; остаются Bucket/CaBundlePath |
| `src/PgWorker.App/Options.cs` | `BackupsS3Options` → `{Bucket, CaBundlePath}`; новая `SnapshotExportOptions.S3`; валидация |
| `src/PgWorker.Backups/WalStreamProcess.cs` | `AgentEnv` несёт `S3_ENDPOINTS`/`S3_READ_ENDPOINTS` per-role; без policy.s3 агенты не поднимаются; DEGRADED-маркер |
| `src/PgWorker.WalReceiver/{WalReceiverOptions,Program,WalReceiverCore}.cs` | `S3_ENDPOINTS` env, пул-клиент, маркер исчерпания |
| `src/PgWorker.Backups/Job/{BackupJobSpec,VerifyJobSpec}.cs`, `Restore/RestoreJobSpec.cs`, `Restore/RestoreJobCommand.cs`, `Drill/DrillJobSpec.cs` | env списков; `RestoreOperationState.SourcePool` |
| `docker/backup/entrypoint.sh` | `PGW_BK_S3_ENDPOINTS` + mc-проба/перебор с per-попыточным `timeout` |
| `src/PgWorker.Backups/Process/{BackupProcess,RestoreProcess,BackupVerifyProcess,RestoreDrillProcess}.cs`, `Retention/RetentionProcess.cs`, `Supervisor/{BackupSupervisorProcess,BackupOrphanSweeper,StorageStatusJson}.cs` | Перевод на пул-клиент фабрики; гварды policy.s3; source_pool; union пулов |
| `src/PgWorker.App/Program.cs` | Фабрика пула в DI, процессы на фабрике, sink слепков из отдельной пары, валидация |
| `src/PgWorker.App/Api/Operations/{BackupsPolicyHandler,RestoreShardHandler}.cs` | Секция `s3` / поле `source_pool` |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | `BackupAgentLogsAsync` (хвост лога агента) |
| `src/AdminPanel.Api/Operations/BackupsCommands.cs` | s3-поля полного policy-тела |
| `src/AdminPanel.Probes/S3/{MinioOptions,MinioS3Client,MinioInventoryLoop}.cs` | `Endpoints` (запятая) + перебор целей с первого доступного |
| `frontend/src/pages/BackupsStoragePage.tsx` + `api/dto.ts` | Колонка «Пул», кнопка, s3 в форме policy |
| `deploy/.env.example`, `deploy/docker-compose.yml` | Новые env, убрать ушедшие |
| `dev-stand/adminpanel/checks/00-up.sh` (+ сид) | Сид S3-кластера `/s3/` через S3Worker API |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` + существующие бэкап-сценарии | Пул-режим: сид `/s3/`-кластера из MinIO-ноды |
| `arch/roadmap/s3.md` | Мерж-гейт: удалить тег t36, добавить пункт external-цели |

Порядок задач = фазы spec §8: arch → домен → процессы → слепки → API → панель → стенд/E2E → гейт.

---

### Task 1: arch-first — правки канона (вся таблица spec §3)

**Вход:** spec одобрен; worktree чист.
**Действие:** правки 5 документов по таблице §3 спеки (тексты правок — дословно из спеки).
**Выход:** канон описывает пул-контракт; коду разрешено следовать.
**Проверка:** `git diff --stat` затрагивает ровно 5 файлов; контрольное чтение каждого раздела.
**Spec:** §3 (таблица), §8 фаза 1.

**Files:**
- Modify: `arch/19-backups.md` (§2, §3, §4, §5, §7, §9)
- Modify: `arch/14-pgworker.md` (§1.1, §6, §8)
- Modify: `arch/23-s3-clusters.md` (§7)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.5, новая глава «S3 (read-минимум)», §9.12)
- Modify: `arch/adminpanel/03-panels.md` (§3, §4)

**Interfaces:** Produces — термины и форматы, на которые ссылается код: policy-секция `{"s3":{"pool","write_master","write_replica"}}`; env `PGW_BACKUP_S3_PROFILES__<PROFILE>__{ACCESS_KEY,SECRET_KEY}`; `PgWorker:Snapshots:Export:S3` (env `PGW_SNAPSHOTS_S3_*`); маркер `s3-pool-exhausted`; алерты `backup-pool-missing`/`backup-pool-unreachable`; панельный env `AdminPanel:Backups:S3:Endpoints`.

- [ ] **Step 1.1: arch/19-backups.md** — пять правок:
  1. §4: после описания policy-JSON добавить секцию `"s3": {"pool":"<C пула>","write_master":["<I>",…],"write_replica":["<I>",…]}` — пул = имя кластера `/s3/clusters/`; списки — упорядоченные имена инстансов пула (ссылки, не адреса; резолв — воркером через `endpoints`/portalloc). Отсутствие секции — пул не выбран: подсистема не пишет и не читает (алерт `backup-pool-missing`); замещение целиком сохраняется.
  2. §2: env джоба полного бэкапа несёт список endpoints по роли источника (реплика → `write_replica`, fallback-мастер → `write_master`); entrypoint до загрузки выбирает первый доступный короткой mc-пробой (`mc ls`, per-попыточный таймаут), грузит на него; падение загрузки → следующий endpoint списка (каждая попытка — per-попыточный таймаут; быстрый отказ/успех, не молчание — R3 arch/23 §7); исчерпание → FAILED с маркером `s3-pool-exhausted`.
  3. §3: env WAL-агента несёт список его роли (`S3_ENDPOINTS`) + read-список (`S3_READ_ENDPOINTS`, резолв хвоста); приёмник перебирает при отказе текущей (per-попыточный таймаут уже в `BackupS3`); исчерпание — ретрай-цикл (стрим стоит, слот держит, RPO-инвариант) + маркер `s3-pool-exhausted` (`DEGRADED`); смена роли источника — пересоздание агента со списком новой роли.
  4. §5: «один endpoint per-install» → «пул per-cluster (policy) + bucket per-install единый на всех нодах пула»; чтение — перебор нод пула «где данные есть» (точечные GET; листинги — первая доступная); удаления — на первой доступной ноде (mesh разносит tombstone); глобальные сверки — union по всем существующим пулам `/s3/clusters/` (первый доступный endpoint каждого).
  5. §7: (а) bucket per-install остаётся env; (б) креды — env-профили `PGW_BACKUP_S3_PROFILES__<PROFILE>__{ACCESS_KEY,SECRET_KEY}` (read-write; пул ссылается профилем `config.secret_profile` — arch/23 §6); (в) etcd-слепки — ОТДЕЛЬНАЯ пара `PgWorker:Snapshots:Export:S3` (env `PGW_SNAPSHOTS_S3_*`), обязательна при `Snapshots:Export:Enabled=true` (fail-fast), `PGW_BACKUP_S3_*` не переиспользует; CA — прежний per-install `CaBundlePath`/`S3_CA_PEM` на все цели. Точка расширения: статические внешние цели (`external`) — вне задачи.
  6. §9: секция `S3 { Bucket, CaBundlePath, Profiles (env-only) }`; Endpoint/Region/AccessKey/SecretKey/AdvertisedEndpoint — уходят; валидация старта: `Backups:Enabled=true` → непустой Bucket + ≥1 профиль; `Snapshots:Export:Enabled=true` → полная слепочная пара.
- [ ] **Step 1.2: arch/14-pgworker.md** — §1.1: `POST /api/clusters/{c}/backups/policy` дополнительно принимает `"s3":{"pool","write_master","write_replica"}` (валидация — 400 по полям: пул существует и не `TO_REMOVE`, имена списков — инстансы пула (`nodes/<I>/placement` существует), без дублей, списки непустые; отсутствие секции — удаление); `POST /api/clusters/{c}/shards/{x}/restore` — опциональное `source_pool` (DR: данные старого кластера в другом пуле; отсутствие — собственный пул). §6/§8: экспорт слепков — отдельная env-пара `PgWorker:Snapshots:Export:S3` (обязательна при Enabled, fail-fast); префикс `etcd/` — в её bucket.
- [ ] **Step 1.3: arch/23-s3-clusters.md §7** — дополнить: реализация t36 фиксируется — PgWorker читатель `/s3/clusters/<C>/{config,nodes/<I>/state,endpoints}` (+`/s3worker/portalloc/<C>` для резолва имён); списки приоритетов записи per-PG-кластер живут в policy бэкапов (arch/19 §4) ссылками на имена инстансов; PgWorker в `/s3/` не пишет; чтение — все ноды без приоритетов (порядок `endpoints`).
- [ ] **Step 1.4: arch/adminpanel/02-etcd-contract.md** — §2.5: env `AdminPanel:Backups:S3:Endpoints` (запятая; одиночный `Endpoint` — валидный частный случай), list с первого доступного (быстрый отказ per-попытки), DTO несёт список endpoints (без ключей). Новая глава «S3 (read-минимум)» после valkey: панель читает `/s3/clusters/*/` (config, nodes/<I>/state, endpoints) — только для страницы бэкапов (список пулов/нод/адресов); полное управление пулами — t34. §9.12: форма шлёт полный policy (retention+full_max_age+verify+drill+s3); сброс секции s3 — отправкой policy без `s3`.
- [ ] **Step 1.5: arch/adminpanel/03-panels.md** — §3: строка кластера таблицы «Кластеры → шарды» грани «Хранилище бэкапов»: бейдж пула (или красный «пул не выбран») + кнопка «Пул бэкапов» (модал: Select пула, два упорядоченных списка мастера/реплики из нод пула). §4: правила AlertEngine `backup-pool-missing` (critical, per-cluster: Active-кластер, policy без s3) и `backup-pool-unreachable` (critical, per-cluster: маркер `s3-pool-exhausted` в error-статусах воркера — wal DEGRADED / full FAILED).
- [ ] **Step 1.6: Контрольное чтение + коммит.** Прочитать каждый изменённый раздел целиком: термины согласованы (`write_master`/`write_replica`, `s3-pool-exhausted`, `backups-main` как пример профиля), перекрёстные ссылки валидны, исторических пассажей нет. `git add arch/ && git commit -m "arch(t36): пул S3 в подсистеме бэкапов — канон (19/14/23/adminpanel)"`.

---

### Task 2: S3PoolStore — чтение `/s3/` воркером

**Вход:** Task 1 (канон).
**Действие:** модель пула + чтение ключей etcd + резолв `<I>→host:port` через portalloc; **имена инстансов — из ключей `nodes/<I>/state|placement`** (portalloc — только адреса).
**Выход:** `S3PoolStore.ReadAsync(pool)` → `S3Pool` с именами нод и адресами.
**Проверка:** юниты парсинга зелёные.
**Spec:** §4.1 (S3PoolStore, модель со `PlacedOn`), §2 (PgWorker — читатель), §4.3 (источник имён инстансов — `nodes/<I>/placement`).

**Files:**
- Create: `src/PgWorker.Etcd/S3/S3PoolModel.cs`
- Create: `src/PgWorker.Etcd/S3/S3PoolStore.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/S3PoolModelTests.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/S3PoolStoreParseTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.Etcd.S3;
  // Нода пула: имя инстанса <I> (из ключей nodes/<I>/state|placement),
  // состояние nodes/<I>/state, фактический хост nodes/<I>/placed_on;
  // Host/S3Port — из /s3worker/portalloc/<C> (null — нода заявлена, но не
  // провижинена: адрес появится, пул поднимается).
  public sealed record S3PoolNode(
      string Name, string? State, string? PlacedOn, string? Host, int? S3Port);
  // Пул: имя кластера /s3/clusters/<C>; ToRemove = config.state == "TO_REMOVE";
  // Endpoints — host:port в порядке endpoints-ключа (порядок чтения ReadAll).
  public sealed record S3Pool(
      string Name, string? SecretProfile, bool ToRemove,
      IReadOnlyList<S3PoolNode> Nodes, IReadOnlyList<string> Endpoints);
  public sealed class S3PoolStore(IEtcdGateway gateway, string[] endpoints)
  {
      public Task<Result<S3Pool?>> ReadAsync(string pool, CancellationToken ct);
      public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct);
  }
  ```
- Consumes: `IEtcdGateway.RangeAsync` (failover-паттерн `EtcdFailover.CallAsync` из `PgWorker.App.Api.Operations`; для библиотеки — локальная копия цикла по endpoints, как `WalStreamProcess.GetAsync`).

Ключевые форматы (источники: arch/23 §2/§8, `S3Worker.Provisioning/Processes/PortAllocIndex.cs`):
- `/s3/clusters/<C>/config` = `{"platform":"owns3","nodes":N,"secret_profile":"…","created_unix":T}` (+`"state":"TO_REMOVE"|"NOT_INITIALIZED"` опц.);
- `/s3/clusters/<C>/nodes/<I>/state` = строка; `nodes/<I>/placement` = JSON-селектор (существование ключа = инстанс заявлен); `nodes/<I>/placed_on` = строка (ставит воркер S3Worker);
- `/s3/clusters/<C>/endpoints` = `"h1:p1,h2:p2,..."` (host — advertised, порт — S3);
- `/s3worker/portalloc/<C>` = `{"<I>":{"host":"h","s3":18001}}` — только источник адресов (не имён).

- [ ] **Step 2.1: Тест модели (AAA)**

```csharp
// src/tests/PgWorker.UnitTests/Etcd/S3PoolModelTests.cs
namespace PgWorker.UnitTests.Etcd;

public class S3PoolModelTests
{
    // AAA: endpoints-строка → упорядоченные пары host:port (порядок = порядок чтения)
    [Fact]
    public void ParseEndpoints_OrderPreserved()
    {
        var parsed = S3PoolModel.ParseEndpoints("h1:18001,h2:18002");
        parsed.Should().Equal("h1:18001", "h2:18002");
    }

    // AAA: битые элементы пропускаются без исключения (unknownKeys-толерантность)
    [Fact]
    public void ParseEndpoints_ToleratesGarbage()
    {
        var parsed = S3PoolModel.ParseEndpoints("h1:18001,,noport,x:нечисло");
        parsed.Should().Equal("h1:18001");
    }

    // AAA: config-JSON без state — Active-пул (state отсутствует = Active, arch/23 §2)
    [Fact]
    public void ParseConfig_ActiveCluster_HasNoToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
        cfg.SecretProfile.Should().Be("backups-main");
        cfg.ToRemove.Should().BeFalse();
    }

    // AAA: config с state=TO_REMOVE — ToRemove=true (валидация policy-API)
    [Fact]
    public void ParseConfig_ToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":0,"secret_profile":"p","created_unix":1,"state":"TO_REMOVE"}""");
        cfg.ToRemove.Should().BeTrue();
    }

    // AAA: portalloc — только адрес по имени; лишние/битые узлы молча пропускаются
    [Fact]
    public void ParsePortAlloc_ResolvesNodeName()
    {
        var map = S3PoolModel.ParsePortAlloc(
            """{"east1":{"host":"h1","s3":18001},"east2":{"host":"h2","s3":18002}}""");
        map["east1"].Should().Be(("h1", 18001));
        map.Count.Should().Be(2);
    }

    // AAA: имена инстансов — из ключей nodes/<I>/state И nodes/<I>/placement
    // (union; инстанс с placement без state — заявлен, валиден)
    [Fact]
    public void InstanceNames_UnionOfStateAndPlacementKeys()
    {
        var names = S3PoolModel.InstanceNames(
        [
            new Kv("/s3/clusters/b/nodes/east1/state", "\"RUNNING\""),
            new Kv("/s3/clusters/b/nodes/east2/placement", """{"docker_host":"h2"}"""),
            new Kv("/s3/clusters/b/endpoints", "h1:1"),
        ]);
        names.Should().Equal("east1", "east2");
    }
}
```

- [ ] **Step 2.2: Запуск — ожидание FAIL** (`S3PoolModel` не существует):

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~S3PoolModelTests`
Expected: FAIL компиляция (тип не найден).

- [ ] **Step 2.3: Реализация `S3PoolModel`** — статик-парсеры `ParseEndpoints` (split `,`, фильтр `host:int`), `ParseConfig` (record `S3PoolConfig(string? SecretProfile, bool ToRemove)`; `JsonSerializer` + try/catch `JsonException` → null-конфиг с диагностикой), `ParsePortAlloc` (`Dictionary<string,(string Host,int Port)>`, try/catch как `PortAllocIndex`), `InstanceNames` (path-парс `nodes/<I>/<leaf>`, union, Ordinal-сортировка). Комментарии — по-русски, ссылки на arch/23 §2/§8.

- [ ] **Step 2.4: Прогон юнитов — PASS.** Та же команда; Expected: PASS.

- [ ] **Step 2.5: Тест+реализация `S3PoolStore`** — `ReadAsync`: range-запросы (`/s3/clusters/<C>/config`; `/s3/clusters/<C>/nodes/` префиксом — имена из state|placement-ключей, state/placed_on по листам; `/s3/clusters/<C>/endpoints`; `/s3worker/portalloc/<C>`); сборка `S3Pool`: **Nodes — из `InstanceNames` (ключи заявки), адреса Host/S3Port — из portalloc-записи той же ноды (нет записи → null-адрес — заявлена, не провижинена)**, порядок `Endpoints` — порядок endpoints-ключа. `ListPoolNamesAsync`: range `/s3/clusters/` → уникальные `<C>` из `config`-ключей. Юнит: сборка пула из словаря `Kv`-заглушек (`Shared.Etcd.Client.Kv`) — AAA-кейсы: полный набор ключей → пул с адресами и `PlacedOn`; **инстанс с placement без portalloc-записи → нода в Nodes с `Host=null` (не выпадает из пула)**; нет config → null.

- [ ] **Step 2.6: Прогон + коммит.** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~S3Pool"`; PASS → `git add -A && git commit -m "feat(t36): S3PoolStore — чтение /s3/ воркером (модель+portalloc-резолв)"`.

---

### Task 3: Policy-модель — секция `s3`

**Вход:** Task 1–2.
**Действие:** расширить `BackupPolicy` опциональной секцией; парсер толерантен.
**Выход:** `BackupsParser` читает policy с `s3`/без.
**Проверка:** юниты парсера (с s3/без/битая секция).
**Spec:** §3 (arch/19 §4), §5 (формат), §4.1.

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` (≈ строки 56–72)
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs` (дополнить существующий)

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.Etcd.Parsing;
  // Секция s3 policy бэкапов (t36, arch/19 §4): пул = имя кластера /s3/clusters/;
  // списки — упорядоченные имена инстансов пула (ссылки, не адреса).
  public sealed record BackupPolicyS3(
      string Pool, IReadOnlyList<string> WriteMaster, IReadOnlyList<string> WriteReplica);
  // BackupPolicy получает: BackupPolicyS3? S3 = null (null = пул не выбран).
  ```

- [ ] **Step 3.1: Тесты (AAA)** — в `BackupsParserTests`:
  1. `ParsePolicy_WithS3_ReadsLists`: JSON `{"retention":{…},"full_max_age_sec":…,"verify":{…},"s3":{"pool":"backups","write_master":["east1","east2"],"write_replica":["east2","east1"]}}` → `Policy.S3!.Pool == "backups"`, списки в порядке.
  2. `ParsePolicy_WithoutS3_Null`: прежний JSON без `s3` → `Policy.S3` null, остальные поля читаются (backwards-compat).
  3. `ParsePolicy_BitS3Section_Null`: `"s3":"мусор"` → `S3` null, policy читается (unknownKeys-толерантность — parseError не валид).
- [ ] **Step 3.2: Запуск — FAIL** (нет `S3`-поля). `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~BackupsParserTests`.
- [ ] **Step 3.3: Реализация** — `BackupPolicyS3`-record; `BackupPolicy` — параметр `BackupPolicyS3? S3 = null`; `BackupsParser`: try-parse секции `s3` (число полей фикс: pool строка непустая, списки строки; любое несоответствие → null + счётчик unknownKeys). Комментарий: «отсутствие/битая секция = пул не выбран — как drill, толерантность парсеров (arch/19 §4)».
- [ ] **Step 3.4: Прогон — PASS; коммит** `git commit -am "feat(t36): policy бэкапов — секция s3 (pool + списки write_master/write_replica)"`.

---

### Task 4: BackupSecretProfileStore — env-профили кредов

**Вход:** Task 1.
**Действие:** читатель `PGW_BACKUP_S3_PROFILES__<P>__{ACCESS_KEY,SECRET_KEY}` по образцу `S3Worker.Provisioning/SecretProfileStore.cs`.
**Выход:** профили кредов доступны резолверу; fail-fast полупар на старте.
**Проверка:** юниты (полный/неполный/пустой env, диагностики без значений).
**Spec:** §4.1 (креды), §4.4, arch/19 §7.

**Files:**
- Create: `src/PgWorker.Backups/BackupSecretProfileStore.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupSecretProfileStoreTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.Backups;
  // Читатель per-install env-профилей S3-кредов (arch/19 §7, t36):
  // PGW_BACKUP_S3_PROFILES__<PROFILE>__{ACCESS_KEY,SECRET_KEY} — read-write ключи
  // профилей пулов (пул ссылается config.secret_profile, arch/23 §6).
  // Диагностики — только имена, никогда значения.
  public sealed class BackupSecretProfileStore(
      Func<string?, string?>? getenv = null, IEnumerable<string>? envNames = null)
  {
      public IReadOnlyList<string> ProfileNames { get; }
      public IReadOnlyList<string> ValidateEnvPairs(); // неполные профили (fail-fast старта)
      public bool TryGet(string profile, out string? accessKey, out string? secretKey, out string? diagnostic);
  }
  ```

- [ ] **Step 4.1: Тесты (AAA)** — `BackupSecretProfileStoreTests`:
  1. `TryGet_FullProfile_ReturnsPair`: env `PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY=ak`, `…SECRET_KEY=sk` → true, ak/sk.
  2. `TryGet_MissingProfile_DiagnosticWithoutValues`: профиль не задан → false; diagnostic содержит имя профиля и перечень `ProfileNames`, НЕ содержит значений.
  3. `ValidateEnvPairs_HalfPair`: только ACCESS_KEY → имя профиля в списке полупар.
  4. `ProfileNames_Sorted`: два профиля → отсортированы по Ordinal.
- [ ] **Step 4.2: Запуск — FAIL.** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~BackupSecretProfileStore`.
- [ ] **Step 4.3: Реализация** — копия механики `SecretProfileStore` (инъекция getenv/envNames для тестов; префикс `PGW_BACKUP_S3_PROFILES__`; `<PROFILE>` — сегмент до первого `__`).
- [ ] **Step 4.4: Прогон — PASS; коммит** `git commit -am "feat(t36): BackupSecretProfileStore — env-профили S3-кредов пула"`.

---

### Task 5: Options — новая секция S3, слепочная пара, валидация старта

**Вход:** Task 4.
**Действие:** `BackupsS3Options` → `{Bucket, CaBundlePath}` (Endpoint/Region/AccessKey/SecretKey/AdvertisedEndpoint/PathStyle — удалить); `SnapshotExportOptions` — собственная секция `S3`; `BackupsRuntimeOptions` — зачистка; валидация `Program.cs`.
**Выход:** конфиг-контракт кода соответствует arch/19 §9; старта-валидация новая.
**Проверка:** юниты валидации; сборка решения без warn (TreatWarningsAsErrors).
**Spec:** §4.4, §3 (arch/19 §7/§9, arch/14 §6).

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (`BackupsS3Options`, `SnapshotExportOptions`, `ToRuntime`, `IsValid`, `PgWorkerOptions.BackupsRuntime()`)
- Modify: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions` — удалить `S3Endpoint/S3AdvertisedEndpoint/S3Region/S3AccessKey/S3SecretKey/S3PathStyle/AgentS3Endpoint`; оставить `S3Bucket`, `S3CaBundlePath`)
- Modify: `src/PgWorker.App/Program.cs` (валидация старта, строки ~43–65)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsValidationTests.cs` (создать; проверить существующие тесты опций — `grep -rn "IsValid\|BackupsS3" src/tests/PgWorker.UnitTests/App/`)

**Interfaces:**
- Produces:
  ```csharp
  // src/PgWorker.App/Options.cs
  public sealed class BackupsS3Options
  {
      public string Bucket { get; set; } = "";        // per-install, единый на всех пулах (arch/19 §5)
      public string? CaBundlePath { get; set; }        // per-install CA https (t42)
      // Креды — env-профили BackupSecretProfileStore (env-only, не конфиг).
  }
  public sealed class SnapshotExportS3Options   // отдельная пара слепков (arch/14 §6/§8)
  {
      public string Endpoint { get; set; } = "";
      public string Bucket { get; set; } = "";
      public string AccessKey { get; set; } = "";
      public string SecretKey { get; set; } = "";
      public string? Region { get; set; }
      public string? CaBundlePath { get; set; }
  }
  // SnapshotExportOptions: + public SnapshotExportS3Options S3 { get; set; } = new();
  // IsValid: Enabled=true → полная пара S3 (Endpoint/Bucket/AccessKey/SecretKey непусты).
  ```
- Consumes: Task 4 store (валидация полупар на старте), `BackupCaPem.ReadIfConfigured` (остаётся от CaBundlePath).

Порядок миграции: Task 5 добавляет новые секции/валидацию; удаление старых полей — Task 11 «Зачистка единого endpoint» (между задачами ветка обязана собираться).

- [ ] **Step 5.1: Тесты валидации (AAA)**:
  1. `Backups_EnabledWithoutBucket_Invalid`: `Backups.Enabled=true`, Bucket пуст → `IsValid()==false`.
  2. `Backups_EnabledWithoutProfiles_Invalid`: Enabled=true, Bucket задан, профилей 0 → false.
  3. `Snapshots_EnabledWithoutPair_Invalid`: `Snapshots.Export.Enabled=true`, `S3.Endpoint` пуст → false; полная пара → true.
  4. `Snapshots_Disabled_AnyPairValid`: Enabled=false → true независимо от S3.
- [ ] **Step 5.2: Запуск — FAIL.**
- [ ] **Step 5.3: Реализация** — новые секции и `IsValid` (старые поля `BackupsS3Options`/`BackupsRuntimeOptions` пока остаются — их удаляет Task 11); `Program.cs`: fail-fast — `Backups:Enabled=true` требует Bucket + `ProfileNames.Count>0` (строка ошибки: `"PgWorker:Backups: Enabled=true требует непустые PgWorker:Backups:S3:Bucket (env PGW_BACKUP_S3_BUCKET) и хотя бы один профиль PGW_BACKUP_S3_PROFILES__* (arch/19 §7/§9)"`); `Snapshots:Export:Enabled=true` — полная пара (`"PgWorker:Snapshots:Export: Enabled=true требует полную пару PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*)"`); + `ValidateEnvPairs()` → бросок при полупарах. Регистрация `BackupSecretProfileStore` в DI (singleton).
- [ ] **Step 5.4: Прогон юнитов; сборка** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` — 0 warnings. Коммит `git commit -am "feat(t36): конфиг S3 бэкапов — bucket+профили, отдельная пара слепков, валидация старта"`.

---

### Task 6: BackupPoolResolver — policy.s3 → списки адресов

**Вход:** Task 2, 3, 4.
**Действие:** резолв per-cluster: policy (`/pgworker/backups/<C>/policy`) + `S3PoolStore` + профили → контекст списков.
**Выход:** единая точка «какой кластер куда пишет/читает».
**Проверка:** юниты (полный резолв, пул не найден, профиль без кредов, списки пусты, policy без s3 → null, нода без адреса → transient).
**Spec:** §4.1 (BackupPoolResolver), §1, §2, §4.3 (инстанс без portalloc — валиден, адрес появится).

**Files:**
- Create: `src/PgWorker.Backups/BackupPoolResolver.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPoolResolverTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.Backups;
  // Резолв per-cluster (t36, arch/19 §4/§2.6-2.7): null = пул не выбран
  // (policy без s3 — подсистема кластера молчит); списки Write* — по роли
  // источника, ReadAll — все ноды пула в порядке endpoints.
  public sealed record BackupPoolContext(
      string Pool, string AccessKey, string SecretKey,
      IReadOnlyList<string> WriteMaster, IReadOnlyList<string> WriteReplica,
      IReadOnlyList<string> ReadAll);
  public sealed class BackupPoolResolver(
      PgWorker.Etcd.S3.S3PoolStore pools,
      BackupSecretProfileStore secrets,
      Func<string, CancellationToken, Task<PgWorker.Etcd.Parsing.BackupPolicy?>>? policyReader = null)
  {
      // Исходы: null — policy без s3 (не выбран); Failed — диагностическая ошибка
      // (пул не найден / TO_REMOVE / профиль без кредов / списки пусты / имя ноды
      // не в пуле / нода без адреса) — ВСЕ Failed-исходы transient для процессов:
      // ретраи тиком + алертная проекция, НЕ краш тика и НЕ 400 на приёме policy
      // (валидация приёма — Task 14 по nodes/<I>/placement, а не по адресам).
      public Task<Result<BackupPoolContext?>> ResolveAsync(string cluster, CancellationToken ct);
  }
  ```
- Consumes: `S3PoolStore.ReadAsync` (имена нод — из ключей заявки, адреса — из portalloc), `BackupPolicy.S3`, `BackupSecretProfileStore.TryGet`.

Резолв адреса: имя `<I>` → portalloc-пара пула; URI = `http://<host>:<port>` (scheme фиксирован http — owns3-контур; https появится вместе с external-целями — зафиксировать комментарием).

- [ ] **Step 6.1: Тесты (AAA)** — фейковые store/reader:
  1. `Resolve_FullPolicy_ListsByRole`: пул 2 ноды (east1/east2 с адресами), policy `s3 {pool, write_master:[east1,east2], write_replica:[east2]}` → WriteMaster=["http://h1:p1","http://h2:p2"], WriteReplica=["http://h2:p2"], ReadAll — обе в порядке endpoints; креды профиля `backups-main`.
  2. `Resolve_PolicyWithoutS3_ReturnsNull` (не Failed — кластер без выбора).
  3. `Resolve_PoolMissing_Failed` (диагностика содержит имя пула).
  4. `Resolve_PoolToRemove_Failed`.
  5. `Resolve_ProfileWithoutCreds_Failed` (диагностика — имя профиля, без значений).
  6. `Resolve_EmptyWriteList_Failed` (пустой список = «не выбрано» — это отсутствие секции; записанная битая policy — transient-диагностика).
  7. `Resolve_UnknownNodeName_Failed` (имя не в Nodes пула — нет `nodes/<I>/placement`: transient-диагностика «нода <I> не в пуле», оператор правит policy; не адресный вопрос).
  8. `Resolve_NodeWithoutAddress_TransientFailed` (**нода в Nodes, но portalloc-записи нет — инстанс заявлен, не провижинен: Failed-диагностика «нода <I> без адреса (portalloc) — пул поднимается» — ретраи тиком; в ReadAll нода без адреса пропускается**).
- [ ] **Step 6.2: Запуск — FAIL.**
- [ ] **Step 6.3: Реализация** — последовательность: policy → `S3` null → Success(null); `pools.ReadAsync(S3.Pool)`: null/ToRemove → Failed; профиль → `secrets.TryGet(pool.SecretProfile)`; имена списков обязаны быть в `pool.Nodes` (иначе Failed); адреса нод списков из portalloc (нет адреса → Failed, transient); ReadAll — адреса нод пула в порядке `Endpoints` (ноды без адреса пропускаются — пул поднимается).
- [ ] **Step 6.4: Прогон — PASS; коммит** `git commit -am "feat(t36): BackupPoolResolver — per-cluster списки записи/чтения пула"`.

---

### Task 7: PoolBackupS3 — IBackupS3 с перебором нод

**Вход:** Task 5, 6.
**Действие:** реализация `IBackupS3` поверх фабрики per-цели `BackupS3`; семантика перебора записи/чтения/листингов.
**Выход:** пул-клиент для всех процессов + фабрика `IBackupS3Factory`.
**Проверка:** юниты переборов (успех первой/второй, исчерпание с маркером, NotFound-чтение перебор).
**Spec:** §4.1 (PoolBackupS3), §1 (п.2/п.4), §2 (паттерны).

**Files:**
- Create: `src/PgWorker.Backups/S3PoolMarkers.cs`
- Create: `src/PgWorker.Backups/PoolBackupS3.cs`
- Create: `src/PgWorker.Backups/BackupS3Factory.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/PoolBackupS3Tests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.Backups;
  // Маркер исчерпания пула (t36, arch/19 §4): в Error статусов воркера
  // (wal DEGRADED / full FAILED); панель зажигает backup-pool-unreachable.
  public static class S3PoolMarkers
  {
      public const string Exhausted = "s3-pool-exhausted";
  }

  // Общая запись ОДНОЙ S3-цели (t36): endpoint+bucket+креды+CA; потребитель —
  // BackupS3 (второй конструктор), фабрика пула, слепочная пара (Task 13),
  // WalReceiver Program. Заменяет завязку BackupS3 на BackupsRuntimeOptions.
  public sealed record S3ClientOptions(
      string Endpoint, string Bucket, string AccessKey, string SecretKey,
      string? Region = null, string? CaBundlePath = null);

  // Пул-клиент: write-цели (список роли), read-цели (ReadAll); цель —
  // существующий BackupS3 (endpoint+креды одного адреса), кэш per-цели;
  // targetFactory — тестовая инъекция (прод: uri => new BackupS3(new S3ClientOptions(...))).
  public sealed class PoolBackupS3 : IBackupS3, IAsyncDisposable
  {
      public PoolBackupS3(
          IReadOnlyList<string> writeTargets,   // uri по порядку приоритета
          IReadOnlyList<string> readTargets,    // все ноды пула (endpoints-порядок)
          string accessKey, string secretKey,
          string bucket, string? caBundlePath,
          TimeSpan? requestTimeout = null,
          Func<string, IBackupS3>? targetFactory = null);
  }

  // Фабрика per-cluster клиента (t36): резолв policy+пула + кэш контекста на
  // тик; restore/глобальные проходы — по имени пула напрямую.
  public sealed class BackupS3Factory(
      BackupPoolResolver resolver,
      PgWorker.Etcd.S3.S3PoolStore pools,
      BackupSecretProfileStore secrets,
      Func<string> bucket,                       // runtime Bucket per-install
      Func<string?> caBundlePath,
      TimeSpan requestTimeout)
  {
      public Task<Result<PoolBackupS3>> CreateForClusterAsync(string cluster, CancellationToken ct);
      public Task<Result<PoolBackupS3>> CreateForPoolAsync(string pool, CancellationToken ct); // ReadAll=Write=all ноды
  }
  ```
- Consumes: `BackupS3` (существующий; строится per-цели; пер-попыточный таймаут запроса уже в `BackupS3` — «быстрый отказ, не молчание», arch/23 §7).

Семантика (spec §4.1):
- `PutObjectAsync`/`DeleteKeysAsync` — write-цели: попытка на текущей, отказ → следующая; исчерпание → `Failed(ApplicationException($"{S3PoolMarkers.Exhausted}: put {key} — все ноды списка недоступны"))`.
- `GetObjectAsync`/`DownloadTextAsync`/`ListFullsAsync` — read-цели: NotFound/транспортный отказ → следующая; успех — оттуда; исчерпание → Failed (для точечных GET текст ошибки исходный; NotFound не считается успехом).
- `ListAsync`/`ListWalAsync`/`ListPrefixAsync` — первая доступная read-цель (успех = пустой список тоже успех).
- `BucketExistsAsync` — любая доступная (write-цели, затем read).

- [ ] **Step 7.1: Тесты (AAA)** — через инъекцию `targetFactory` (фабрика цели) — тесты на фейках `FakeTargetS3 : IBackupS3` (управляемый отказ):
  1. `Put_FailsOverToSecondTarget`: target1 всегда Failed, target2 успех → успех; put шёл на target1, затем target2.
  2. `Put_AllTargetsFail_ExhaustedMarker`: оба Failed → Error.Message содержит `s3-pool-exhausted`.
  3. `Get_NotFoundOnFirst_FoundOnSecond`: target1 NotFound-стайл Failed, target2 Success → значение со второй.
  4. `List_FirstAliveWins`: target1 Failed, target2 успех → список c target2, target3 не трогался.
  5. `BucketExists_AnyAlive_True`.
  6. `Delete_Failover`: симметрично put.
- [ ] **Step 7.2: Запуск — FAIL.**
- [ ] **Step 7.3: Реализация** — `PoolBackupS3` (кэш `Dictionary<string, BackupS3>` per-цели, перебор всегда с первой — «первый доступный по списку»); `S3PoolMarkers`; `BackupS3Factory` (создание per-вызов, dispose вызывающим; кэш per-цели внутри PoolBackupS3).
- [ ] **Step 7.4: Прогон — PASS; коммит** `git commit -am "feat(t36): PoolBackupS3 — перебор нод пула (запись/чтение/листинги) + фабрика"`.

---

### Task 8: WAL-агент — списки per-role, перебор приёмника, маркер DEGRADED

**Вход:** Task 6, 7.
**Действие:** `WalStreamProcess.AgentEnv` несёт `S3_ENDPOINTS` (роль источника) + `S3_READ_ENDPOINTS`; приёмник строит `PoolBackupS3`; драйвер-логи; контроль пишет маркер.
**Выход:** WAL-поток пишет в пул по ролям; исчерпание — ретрай-цикл + маркер.
**Проверка:** юниты AgentEnv/маркеров; прогон сборки.
**Spec:** §4.2 (WAL-агент), §1 (п.2/п.3), AC4, AC5.

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` (AgentEnv ≈451–478; TickShard ≈125–180 — гвард policy.s3; контроль DEGRADED ≈650–665)
- Modify: `src/PgWorker.WalReceiver/WalReceiverOptions.cs` (env: `S3_ENDPOINTS`, `S3_READ_ENDPOINTS`)
- Modify: `src/PgWorker.WalReceiver/Program.cs` (PoolBackupS3)
- Modify: `src/PgWorker.WalReceiver/WalReceiverCore.cs` (маркер исчерпания в PutWithRetry)
- Modify: `src/PgWorker.WalReceiver/WalReceiverMarkers.cs` (+ pool-exhausted маркер)
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (+ `BackupAgentLogsAsync`)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalStreamAgentEnvTests.cs` (или существующий файл AgentEnv-тестов — найти `grep -rn AgentEnv src/tests`)

**Interfaces:**
- Consumes: `BackupS3Factory.CreateForClusterAsync`, `BackupPoolContext.WriteMaster/WriteReplica/ReadAll`, `PoolBackupS3`.
- Produces: env-контракт агента: `S3_ENDPOINTS` (запятая URI, порядок списка роли источника), `S3_READ_ENDPOINTS` (запятая, ReadAll), `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (креды профиля пула), `S3_CA_PEM` (как сейчас); `S3_ENDPOINT`/`S3_REGION`/`S3_PATHSTYLE` — уходят (Task 11 чистит потребителей).
  ```csharp
  // IClusterDriver:
  Task<Result<string>> BackupAgentLogsAsync(string cluster, string containerName, int tailLines, CancellationToken ct);
  // WalReceiverMarkers:
  public static string PoolExhausted(); // {"pool_exhausted": ...} — приёмник в ретрай-цикле, не чаще 1/5 c
  ```

Механика:
- `TickShardAsync`: до создания агентов — `resolver.ResolveAsync(cluster)`: `null` → стоп-семантика кластера (демонтаж агентов, wal-ключ не пишем/оставляем, journal-фаза `waiting-backup-pool`); `Failed` → journal-фаза с ошибкой, тик не валится (transient).
- `EnsureAgentsAsync`/`AgentEnv`: master-источник → `WriteMaster`-URI; sync-источник → `WriteReplica`-URI; `S3_READ_ENDPOINTS` — общий ReadAll. Смена роли = пересоздание агента (существующая механика foreign/missing) — env уже со списком новой роли.
- Приёмник `Program`: парсинг двух списков → `PoolBackupS3(write, read, access, secret, bucket, ca)`; `WalReceiverOptions.Parse` — требование `S3_ENDPOINTS` непустого.
- `PutWithRetryAsync`: если `put.Error.Message` содержит `S3PoolMarkers.Exhausted` → `Emit(WalReceiverMarkers.PoolExhausted())` перед `Task.Delay(retry)` (маркер в stdout — для контроля и логов).
- Контроль DEGRADED (WalStreamProcess ≈650–665): при переходе в `Degraded` — для мастера-агента `driver.BackupAgentLogsAsync(cluster, name, 50)`; хвост содержит `pool_exhausted` → `error = $"{S3PoolMarkers.Exhausted}: {error}"` (алерт-маркер в wal-ключе).

- [ ] **Step 8.1: Тест AgentEnv (AAA)** — `AgentEnv_MastersAgentGetsMasterList` / `AgentEnv_SyncAgentGetsReplicaList`: `AgentEnv(options, cluster, shard, slot, host, port, password, writeEndpoints, readEndpoints, accessKey, secretKey)` (сигнатура расширена) → env `S3_ENDPOINTS` = join(",", writeEndpoints), `S3_READ_ENDPOINTS` = join(",", read), креды профиля. Тест WalReceiverOptions: `S3_ENDPOINTS` парсится в список (порядок сохранён), отсутствие → ошибка permanent (код 5).
- [ ] **Step 8.2: Запуск — FAIL.**
- [ ] **Step 8.3: Реализация** — по механике выше; `WalStreamProcess` получает `BackupS3Factory` в конструктор (Program.cs подключает сразу).
- [ ] **Step 8.4: Прогон юнитов WalStream** (`--filter FullyQualifiedName~WalStream` — существующие тесты AgentEnv обновить на новую сигнатуру) — PASS.
- [ ] **Step 8.5: Коммит** `git commit -am "feat(t36): WAL-агент — списки пула per-role в env, перебор приёмника, маркер s3-pool-exhausted"`.

---

### Task 9: Полный бэкап — env-списки, entrypoint-перебор, гвард policy.s3

**Вход:** Task 6, 7.
**Действие:** `BackupJobSpec` несёт `PGW_BK_S3_ENDPOINTS` (роль источника); `entrypoint.sh` — mc-проба первого доступного (per-попыточный `timeout`) + загрузка на него + переливка при падении; `BackupProcess` — не планирует полный без policy.s3, передаёт список роли.
**Выход:** полный бэкап пишется в пул; FAILED с маркером при исчерпании.
**Проверка:** юниты BackupJobSpec; сборка.
**Spec:** §4.2 (полный бэкап: mc-проба с таймаутом per-попытки, переливка), §1 (п.1/п.2), AC1, AC2, AC4.

**Files:**
- Modify: `src/PgWorker.Backups/Job/BackupJobSpec.cs`
- Modify: `docker/backup/entrypoint.sh`
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (гвард ≈175–210; вызов `BackupJobSpec.Build` ≈248)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupJobSpecTests.cs` (создать/дополнить)

**Interfaces:**
- Consumes: `BackupS3Factory`, `BackupPoolContext.WriteMaster/WriteReplica`.
- Produces: env джоба: `PGW_BK_S3_ENDPOINTS` = "uri1,uri2" (порядок списка роли), `PGW_BK_S3_BUCKET`, `PGW_BK_S3_ACCESS_KEY`, `PGW_BK_S3_SECRET_KEY` (профиля), `PGW_BK_PREFIX`, `S3_CA_PEM`; `PGW_BK_S3_ENDPOINT`/`PGW_BK_S3_REGION` — уходят.
  ```csharp
  // BackupJobSpec.Build — сигнатура:
  public static ContainerSpec Build(
      BackupsRuntimeOptions opts, string cluster, string shard, string id,
      NodeAddress source, string backupPassword,
      IReadOnlyList<string> s3Endpoints, string s3AccessKey, string s3SecretKey);
  ```

Entrypoint-перебор (sh, POSIX — образ postgres:18; coreutils `timeout` — per-попыточный таймаут: проба 5 с, загрузка 300 с; «быстрый отказ/успех, не молчание» — R3, arch/23 §7):

```sh
# Список endpoints пула (роль источника, порядок = приоритет записи).
# MC_HOST_pgw<i> — алиас per-нода; PROBE_TIMEOUT/UPLOAD_TIMEOUT — пер-попыточные
# таймауты: висящий endpoint НЕ вешает попытку до внешних бюджетов.
S3_ALIASES=""
MC_N=0
for ep in $(echo "$PGW_BK_S3_ENDPOINTS" | tr ',' ' '); do
  export MC_HOST_pgw$MC_N="${ep%%://*}://$PGW_BK_S3_ACCESS_KEY:$PGW_BK_S3_SECRET_KEY@${ep#*://}"
  S3_ALIASES="$S3_ALIASES pgw$MC_N"; MC_N=$((MC_N+1))
done
[ "$MC_N" -gt 0 ] || FAIL "PGW_BK_S3_ENDPOINTS: пустой список пула"

# До загрузки — выбор первого доступного (короткая mc-проба, таймаут per-попытки).
alias_alive() { timeout 5 mc ls "$1/$PGW_BK_S3_BUCKET" >/dev/null 2>&1; }
FIRST_ALIVE=""
for a in $S3_ALIASES; do alias_alive "$a" && { FIRST_ALIVE="$a"; break; }; done
[ -n "$FIRST_ALIVE" ] || FAIL "s3-pool-exhausted: mc ls недоступен на всех нодах пула"

# Загрузка: первая живая нода; падение загрузки → следующая по списку
# (переливка идемпотентна: имена объектов детерминированы).
upload() { # $1 — mc-источник (с {A}-плейсхолдером алиаса), $2 — цель
  for a in $S3_ALIASES; do
    timeout 300 mc cp --recursive "$(echo "$1" | sed "s/{A}/$a/")" "$a/$PGW_BK_S3_BUCKET/$2" \
      && return 0
  done
  return 1
}
upload "{A}/$STAGING/full/" "$PGW_BK_PREFIX/full/$PGW_BK_ID/" \
  || FAIL "s3-pool-exhausted: upload full failed on all pool endpoints"
```

- [ ] **Step 9.1: Тест BackupJobSpec (AAA)** — `Build_EnvCarriesEndpointListByRole`: два endpoints → `PGW_BK_S3_ENDPOINTS == "http://h1:p1,http://h2:p2"`, креды переданы, `PGW_BK_S3_ENDPOINT` отсутствует.
- [ ] **Step 9.2: FAIL → реализация** — Build-сигнатура + env; `BackupProcess`: в начале тика кластера `resolver.ResolveAsync` → null: continue (полный не планируется; journal не пишем — молчание = «пул не выбран», алерт панель зажигает по policy); Failed → journal-фаза `waiting-backup-pool`, continue; успех → списки по `role` (existing `BackupSourceRole` уже вычисляется ≈213–215) в `Build`.
- [ ] **Step 9.3: Entrypoint** — заменить блок MC_HOST/upload на функции выше (полный файл: `docker/backup/entrypoint.sh`; upload wal-сегментов — та же `upload`-функция с `{A}`-плейсхолдером).
- [ ] **Step 9.4: Прогон** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~BackupJobSpec` — PASS; коммит `git commit -am "feat(t36): полный бэкап — PGW_BK_S3_ENDPOINTS + mc-проба/перебор с per-попыточным timeout"`.

---

### Task 10: Verify/Restore/Drill — перебор чтения

**Вход:** Task 7.
**Действие:** env джобов чтения — read-список; inline-скрипты restore/drill и verify-команда перебирают mc-алиасы до найденного (per-попыточный `timeout`).
**Выход:** чтение — все ноды пула без приоритетов.
**Проверка:** юниты билдеров env; сборка.
**Spec:** §4.2 (verify/restore/drill-джобы), §1 (п.4), AC6, AC8.

**Files:**
- Modify: `src/PgWorker.Backups/Job/VerifyJobSpec.cs`, `src/PgWorker.Backups/VerifyJobCommand.cs`
- Modify: `src/PgWorker.Backups/Restore/RestoreJobSpec.cs`, `src/PgWorker.Backups/Restore/RestoreJobCommand.cs` (Script)
- Modify: `src/PgWorker.Backups/Drill/DrillJobSpec.cs`
- Modify: процессы запуска (`BackupVerifyProcess`, `RestoreProcess`, `RestoreDrillProcess`) — передача read-списка
- Test: `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs` (создать/дополнить), `VerifyJobSpecTests`

**Interfaces:**
- Produces: env чтения: `S3_ENDPOINTS` (запятая, ReadAll — общий контракт трёх джобов), `S3_ACCESS_KEY`/`S3_SECRET_KEY` (профиль), `S3_BUCKET`; MC_HOST-алиасы строит скрипт (pgwbkp0..N). Голова `RestoreJobCommand.Script` (общая константа `RestoreJobCommand.AliasesScript` — переиспользуют verify и drill):
  ```sh
  # Голова скрипта (заменяет единый MC_HOST_pgwbkp): алиасы per-ноды пула.
  MC_ALIAS_N=0
  for ep in $(echo "$S3_ENDPOINTS" | tr ',' ' '); do
    export MC_HOST_pgwbkp$MC_ALIAS_N="${ep%%://*}://$S3_ACCESS_KEY:$S3_SECRET_KEY@${ep#*://}"
    MC_ALIAS_N=$((MC_ALIAS_N+1))
  done
  [ "$MC_ALIAS_N" -gt 0 ] || { printf '{"ok":false,"error":"S3_ENDPOINTS empty"}\n'; exit 1; }
  # Копирование с перебором нод (per-попыточный timeout — быстрый отказ, R3):
  # NotFound/отказ → следующая (лаг mesh допущен), исчерпание всех → код ≠ 0.
  mc_copy_first() { # $1 — mc-источник С алиасом-плейсхолдером {A}; $2 — цель
    i=0
    while [ $i -lt $MC_ALIAS_N ]; do
      timeout 300 mc cp --recursive "$(echo "$1" | sed "s/{A}/pgwbkp$i/")" "$2" && return 0
      i=$((i+1))
    done
    return 1
  }
  ```
  Точечный `restore-wal.sh` (сегмент из `wal/`): тот же цикл `while [ $i -lt $MC_ALIAS_N ]`: `timeout 30 mc cp "pgwbkp$i/$S3_BUCKET/$SRC_PREFIX/wal/$SEG" "$2" && exit 0`; конец списка → `exit 1` = конец WAL (канон §3.5); переменная `MC_ALIAS_N` в restore-wal.sh вычисляется заново из `S3_ENDPOINTS` (shell-переменные не переживают execve).
- Consumes: `BackupS3Factory.CreateForClusterAsync` / `CreateForPoolAsync(sourcePool)`.

- [ ] **Step 10.1: Тесты (AAA)** — `RestoreJobSpec_EnvCarriesReadEndpoints`: env `S3_ENDPOINTS` = join запятая read-списка, `S3_ACCESS_KEY`/`S3_SECRET_KEY` профиля; `VerifyJobSpec_SameContract`. Скрипты — интеграционно (E2E Task 20).
- [ ] **Step 10.2: FAIL → реализация** — Build-сигнатуры трёх спек (+параметры `s3Endpoints/accessKey/secretKey`), Script-голова restore/drill (общая — вынести в константу `RestoreJobCommand.AliasesScript`), verify-команда (inline bash VerifyJobCommand.Build — та же голова + `mc_copy_first` вокруг скачивания `full/<id>/`).
- [ ] **Step 10.3: Процессы** — `BackupVerifyProcess`/`RestoreProcess`/`RestoreDrillProcess` берут read-контекст фабрикой (`CreateForClusterAsync`; restore — `CreateForPoolAsync(op.SourcePool ?? собственный)` — источник restore-чтения: собственный пул или `source_pool` DR); списки в спеки.
- [ ] **Step 10.4: Прогон** `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RestoreJob|FullyQualifiedName~VerifyJob|FullyQualifiedName~Drill"` — PASS; коммит `git commit -am "feat(t36): verify/restore/drill — перебор чтения по всем нодам пула (timeout per-попытки)"`.

---

### Task 11: Ретенция/супервизор/сироты + зачистка единого endpoint + DI-склейка

**Вход:** Task 7–10.
**Действие:** ретенционные list/delete — пул-клиент кластера; `used_bytes`/реестр сирот — union пулов; `ReloadableBackupS3` и старые поля `BackupsRuntimeOptions` удалить; все процессы на фабрике.
**Выход:** единственный путь записи — пул; код без единого endpoint.
**Проверка:** сборка 0 warnings; юниты/интеграции ретенции зелёные.
**Spec:** §4.2 (ретенция и супервизор), §4.1, §6.

**Files:**
- Modify: `src/PgWorker.Backups/Retention/RetentionProcess.cs`, `Retention/StorageStatusJson.cs`
- Modify: `src/PgWorker.Backups/Supervisor/BackupSupervisorProcess.cs`, `Supervisor/BackupOrphanSweeper.cs`
- Modify: `src/PgWorker.App/Program.cs` (удалить `ReloadableBackupS3`, регистрацию `IBackupS3` singleton; процессы получают `BackupS3Factory`)
- Modify: `src/PgWorker.Backups/Options.cs`, `src/PgWorker.App/Options.cs` (удаление устаревших полей — завершение Task 5)
- Test: существующие интеграции `src/tests/PgWorker.IntegrationTests/Backups/*` (FakeBackupDeps/FakeBackupEngine — перевести на пул-сиды)

**Interfaces:**
- Consumes: `BackupS3Factory`, `S3PoolStore.ListPoolNamesAsync`.
- Produces:
  - `RetentionProcess`: per-cluster проход — `CreateForClusterAsync` (гвард policy.s3: null → кластер молчит); глобальный storage-статус (`used_bytes`) — union: `ListPoolNamesAsync` → `CreateForPoolAsync(pool).ListPrefixAsync("")` каждого, дедуп ключей по `(pool, key)`, суммирование.
  - `BackupSupervisorProcess`: per-cluster `CreateForClusterAsync` (удаления сирот full/<id>/ — первая доступная нода: put/delete-семантика пула).
  - `BackupOrphanSweeper`: как storage-статус — union пулов.
  - `BackupCaPem`/env `S3_CA_PEM` — остаётся (CaBundlePath per-install).

- [ ] **Step 11.1: Юнит-тест union** (AAA) — `StorageUnion_MergesPoolsDedup`: два fake-пула с общим ключом (mesh-дубликат) → в `used_bytes` ключ учтён один раз per-pool (дедуп `(pool,key)`).
- [ ] **Step 11.2: Реализация** — по Interfaces; `Program.cs`: `builder.Services.AddSingleton(sp => new BackupS3Factory(...))`; удалить `AddSingleton<IBackupS3, ReloadableBackupS3>` и класс; процессы (Backup/Restore/Verify/Drill/WalStream/Retention/Supervisor/OrphanSweeper) — конструкторы получают фабрику (частично сделано в 8–10 — здесь закрываются оставшиеся).
- [ ] **Step 11.3: Зачистка** — удалить из `BackupsRuntimeOptions`: `S3Endpoint`, `S3AdvertisedEndpoint`, `S3Region`, `S3AccessKey`, `S3SecretKey`, `S3PathStyle`, `AgentS3Endpoint`; из `BackupsS3Options`: Endpoint/Region/AccessKey/SecretKey/PathStyle/AdvertisedEndpoint; конструктор `BackupS3(BackupsRuntimeOptions)` заменить на `BackupS3(S3ClientOptions)` (тип введён Task 7; EtcdSnapshotSink в Program.cs собирает цель `S3ClientOptions` из текущих полей — до Task 13 допустима пара из `Backups`-конфига, Task 13 переключит её на `Snapshots:Export:S3`); `BackupCaPem.ReadIfConfigured` — работает от `S3CaBundlePath` (остаётся). Потребителей правит компиляция.
- [ ] **Step 11.4: Прогоны** — `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` (0 warnings); `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug`; интеграции не-docker: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Backups"` (Etcd-фикстуры локальные). Коммит `git commit -am "feat(t36): ретенция/супервизор на пуле, union пулов; зачистка единого S3-endpoint"`.

---

### Task 12: Restore — поле `source_pool`

**Вход:** Task 10.
**Действие:** `RestoreOperationState.SourcePool` (опц.); API-приём + валидация; `RestoreProcess` — чтение из пула DR-источника.
**Выход:** DR-restore из чужого пула.
**Проверка:** юниты; API-интеграции.
**Spec:** §3 (arch/14 §1.1), §4.2 (restore-заявка), §4.3, AC8.

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` (`RestoreOperationState` + `string? SourcePool = null`)
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs`
- Modify: `src/PgWorker.App/Api/Operations/RestoreShardHandler.cs` (тело `RestoreShardRequest` + валидация)
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs` (пул чтения)
- Modify: `src/PgWorker.Backups/Restore/RestoreJobSpec.cs` (воркер резолвит адреса пула источника — джоб получает уже read-список)
- Test: `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs` (дополнить restore-кейсы)

**Interfaces:**
- Produces:
  ```csharp
  // RestoreShardRequest: + [JsonPropertyName("source_pool")] string? SourcePool
  // RestoreOperationState: + string? SourcePool = null
  ```
- Consumes: `BackupS3Factory.CreateForPoolAsync`; валидация `source_pool` в хендлере — живое чтение `/s3/clusters/<C>/config` через `S3PoolStore` (пул существует, не `TO_REMOVE` → иначе 400; Task 14 при выносе `BackupsPolicyS3Validator` переиспользует ту же механику).

- [ ] **Step 12.1: Тесты (AAA)** — интеграционные API: `Restore_UnknownSourcePool_400` (поле с несуществующим пулом → ProblemDetails 400, поле `source_pool`); `Restore_SourcePoolAccepted_Planned` (валидный пул → PLANNED-ключ содержит SourcePool); юнит парсера: `SourcePool` null у старых ключей.
- [ ] **Step 12.2: FAIL → реализация** — модель/парсер/хендлер; `RestoreProcess`: read-цели от `SourcePool ?? policy.S3.Pool` через `CreateForPoolAsync`.
- [ ] **Step 12.3: Прогон** `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Restore"`; коммит `git commit -am "feat(t36): restore — опциональный source_pool (DR из чужого пула)"`.

---

### Task 13: Слепки etcd — отдельная env-пара

**Вход:** Task 5.
**Действие:** `EtcdSnapshotSink` строится из `PgWorker:Snapshots:Export:S3`; префикс `etcd/` — в её bucket; fail-fast при Enabled без пары.
**Выход:** DR-слепки не зависят от пула бэкапов.
**Проверка:** юниты; сборка.
**Spec:** §1 (п.5), §3 (arch/14 §6/§8), §4.2 (слепки), §4.4, AC7.

**Files:**
- Modify: `src/PgWorker.App/Program.cs` (singleton `EtcdSnapshotSink` ≈351–369: цель `BackupS3` строится из `opts.Snapshots.Export.S3` через `S3ClientOptions` (Task 7); построение — в тестируемой фабрике `EtcdSnapshotSinkFactory.Create`)
- Test: `src/tests/PgWorker.UnitTests/App/SnapshotExportOptionsTests.cs` (валидация из Task 5), интеграция `src/tests/PgWorker.IntegrationTests/Etcd/*` (найти существующие sink-тесты: `grep -rn EtcdSnapshotSink src/tests`)

**Interfaces:**
- Consumes: `S3ClientOptions` (введён Task 7), `SnapshotExportOptions.S3`, `SnapshotExportOptions.IsValid`.
- Produces: Program-цель слепочной пары: `new S3ClientOptions(opts.Snapshots.Export.S3.Endpoint, …S3.Bucket, …S3.AccessKey, …S3.SecretKey, Region, CaBundlePath)`; вынести построение sink в тестируемую фабрику `static EtcdSnapshotSinkFactory.Create(PgWorkerOptions opts, IServiceProvider sp)`.

- [ ] **Step 13.1: Тесты (AAA)** — `SinksClient_UsesOwnBucket`: sink, построенный `EtcdSnapshotSinkFactory.Create` (fake `IBackupS3` фиксирует bucket целей — или unit на фабрику: собранные `S3ClientOptions` равны ожидаемым из env-примера). Fail-fast: `Snapshots_EnabledWithoutPair_Throws` (проверка Program-валидации из Task 5).
- [ ] **Step 13.2: FAIL → реализация** — Program: sink из `opts.Snapshots.Export.S3`; `BackupS3Factory`/`PoolBackupS3` на `S3ClientOptions` (профиль→цель); WalReceiver Program — тот же формат.
- [ ] **Step 13.3: Прогоны** — юниты снапшот-серий: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~Snapshot`; коммит `git commit -am "feat(t36): etcd-слепки — отдельная env-пара PgWorker:Snapshots:Export:S3"`.

---

### Task 14: API policy — приём и валидация секции `s3`

**Вход:** Task 2, 3.
**Действие:** `BackupsPolicyHandler` принимает `"s3"`; валидатор `BackupsPolicyS3Validator`; отсутствие секции — удаление из policy.
**Выход:** policy-API полный контракт канона.
**Проверка:** API-интеграции (валидный/все 400-кейсы/удаление).
**Spec:** §4.3 (в т.ч. «имена списков — инстансы пула (nodes/<I>/placement существует)»), §3 (arch/14 §1.1), AC8, §5.

**Files:**
- Create: `src/PgWorker.App/Api/Operations/BackupsPolicyS3Validator.cs`
- Modify: `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs` (дополнить)

**Interfaces:**
- Produces:
  ```csharp
  namespace PgWorker.App.Api.Operations;
  // Валидация секции s3 policy (t36, arch/14 §1.1): ЖИВОЕ чтение /s3/ (не кэш
  // снапшота). Ошибки — ValidationError-поля для 400-тела.
  public sealed class BackupsPolicyS3Validator(IEtcdGateway gateway, string[] endpoints)
  {
      public Task<Result<IReadOnlyList<ValidationError>>> ValidateAsync(
          BackupsPolicyS3Body? s3, CancellationToken ct);
  }
  public sealed record BackupsPolicyS3Body(
      [property: JsonPropertyName("pool")] string? Pool,
      [property: JsonPropertyName("write_master")] IReadOnlyList<string>? WriteMaster,
      [property: JsonPropertyName("write_replica")] IReadOnlyList<string>? WriteReplica);
  ```
- Consumes: `S3PoolStore.ReadAsync` — **множество инстансов пула = имена из ключей `nodes/<I>/placement` (заявка; `S3Pool.Nodes` их несёт), НЕ portalloc**: инстанс заявлен, но не провижинен (portalloc-записи нет) — policy ВАЛИДНА (адрес появится при provisioning), API НЕ отвечает 400.

Валидационные правила (400 с полями — spec §4.3):
- `s3.pool`: не существует → `s3.pool: пул <имя> не найден в /s3/clusters/`; `TO_REMOVE` → `s3.pool: пул <имя> в состоянии TO_REMOVE`;
- имя в списке — не инстанс пула (**нет `nodes/<I>/placement`**) → `s3.write_master[i]: нода <имя> не в пуле <C>`;
- дубль внутри списка → `s3.write_master: дубликат <имя>`;
- пустой список/отсутствует при заданной секции → `s3.write_master: список пуст (пусто = отсутствие секции s3 целиком)`;
- `pool` пуст при заданных списках → `s3.pool: обязателен`.
- Отсутствие секции `s3` в теле целиком → секция не кладётся в policy-JSON (выключение бэкапов кластера).
- Live-чтение: `S3PoolStore` от тех же endpoints etcd (не снапшот); etcd-отказ → `EtcdWriteUnavailableException` (503).

- [ ] **Step 14.1: Интеграционные тесты (AAA)** — по правилам выше + happy-path: политика с секцией пишется в ключ канонично (`PUT` читает ключ и видит `s3` в JSON-формате spec §5); повторный PUT без `s3` — секция исчезла; старые поля без изменений; **кейс «инстанс заявлен (placement есть), portalloc-записи нет → policy ВАЛИДНА (201/200, без 400)»**. Префиксный сид `/s3/clusters/backups/…` в etcd-фикстуре теста.
- [ ] **Step 14.2: FAIL → реализация** — `BackupsPolicyS3Validator`; `BackupsPolicyHandler`: `PolicyBody` + `S3`; сборка payload: `"s3"` в Dictionary при валидной секции.
- [ ] **Step 14.3: Прогон** `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter FullyQualifiedName~BackupsPolicyApi`; коммит `git commit -am "feat(t36): policy-API — секция s3 с валидацией пула/нод/списков"`.

---

### Task 15: AdminPanel.Etcd — домен S3 (read-минимум)

**Вход:** Task 1.
**Действие:** чтение `/s3/clusters/*/` в домен-снапшот `S3PoolInfo` (по образцу Valkey: store+refresher+snapshot-поле).
**Выход:** панель знает пулы/ноды/адреса для страницы бэкапов.
**Проверка:** юниты парсера домена.
**Spec:** §4.5 (read-минимум), §3 (adminpanel/02 глава «S3»).

**Files:**
- Create: `src/AdminPanel.Core/S3PoolInfo.cs`
- Create: `src/AdminPanel.Etcd/S3/S3PoolSnapshotStore.cs`, `src/AdminPanel.Etcd/S3/S3PoolSnapshotRefresher.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+ поле `S3Pools`), `src/AdminPanel.Etcd/ModuleExtensions.cs` (регистрация), `src/AdminPanel.Api/Program.cs` (если refresher регистрируется там — по образцу Valkey)
- Test: `src/tests/AdminPanel.UnitTests/S3PoolParserTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace AdminPanel.Core;
  // Пул S3 (read-минимум adminpanel/02 «S3», t36): только страница бэкапов.
  public sealed record S3PoolInfo(
      string Name, string? SecretProfile, bool Active,   // Active = нет state-заявки
      IReadOnlyList<S3PoolNodeInfo> Nodes, string[] Endpoints);
  public sealed record S3PoolNodeInfo(string Name, string? State);
  // EtcdSnapshot: + IReadOnlyList<S3PoolInfo> S3Pools (может быть пуста)
  ```
- Consumes: панельный etcd-транспорт (как `ValkeySnapshotStore`: range `/s3/clusters/` с префиксом; poll-тик 3 с).

- [ ] **Step 15.1: Юниты (AAA)** — `Parse_ConfigNodesEndpoints_BuildsPool`; `Parse_BitConfig_SkipWithParseError`; `Parse_NoStateMeansActive`; endpoints-строка → массив по порядку.
- [ ] **Step 15.2: FAIL → реализация** — по образцу Valkey-домена (store держит `ImmutableList<S3PoolInfo>`; refresher — hosted-тик; толерантность unknownKeys/parseError — как прочие домены).
- [ ] **Step 15.3: Прогон** `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter FullyQualifiedName~S3Pool`; коммит `git commit -am "feat(t36): панель — домен S3 read-минимум (/s3/clusters)"`.

---

### Task 16: AdminPanel.API — s3 в policy-мутации + данные модала

**Вход:** Task 15.
**Действие:** `UpdateBackupsPolicyCommand` несёт s3-поля; read-API отдаёт пулы (для Select модала) и policy кластера.
**Выход:** модал умеет сохранять полный policy.
**Проверка:** интеграции панельного API.
**Spec:** §4.5, §3 (adminpanel/02 §9.12), AC9.

**Files:**
- Modify: `src/AdminPanel.Api/Operations/BackupsCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs` (PUT-эндпоинт ≈192–215)
- Modify: read-API (где панель читает policy кластера — найти `grep -rn "policy" src/AdminPanel.Api/Inspection/`; дополнить s3-полями DTO)
- Test: `src/tests/AdminPanel.IntegrationTests/BackupsPolicyPanelApiTests.cs` (дополнить)

**Interfaces:**
- Produces:
  ```csharp
  public sealed record UpdateBackupsPolicyRequest(
      int RetentionDays, int RetentionWeeks, int RetentionMonths,
      long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays,
      S3PolicyBody? S3);                                   // null — сброс секции
  public sealed record S3PolicyBody(string? Pool, IReadOnlyList<string>? WriteMaster, IReadOnlyList<string>? WriteReplica);
  ```
- Consumes: `WorkerProxy.SendAsync` (существующий), `EtcdSnapshot.S3Pools` (read: список пулов для UI).

- [ ] **Step 16.1: Интеграционные тесты (AAA)** — `Put_PolicyWithS3_ProxiedToWorker` (тело содержит s3; мок worker-API — паттерн существующих тестов); `Put_PolicyWithoutS3_FieldOmitted` (json не содержит секции — выключение); read: `Get_ClusterPolicy_IncludesS3`.
- [ ] **Step 16.2: FAIL → реализация** — тело команды сериализует `s3` только при non-null (анонимный объект с условием).
- [ ] **Step 16.3: Прогон** `dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter FullyQualifiedName~BackupsPolicy`; коммит `git commit -am "feat(t36): панель-API — s3 в policy-мутации + пулы для модала"`.

---

### Task 17: Frontend — бейдж пула + модал «Пул бэкапов»

**Вход:** Task 16.
**Действие:** колонка «Пул» в таблице «Кластеры → шарды»; кнопка «Пул бэкапов» → модал (Select пула + два упорядоченных списка чекбоксами + кнопки вверх/вниз); сохранение полного policy.
**Выход:** оператор управляет выбором пула со страницы.
**Проверка:** e2e-чек `45-backups-storage.sh` расширен; ручной чек стенда.
**Spec:** §4.5 (страница), §3 (adminpanel/03 §3), AC1, AC9.

**Files:**
- Create: `frontend/src/pages/backups-storage/BackupsPoolModal.tsx`
- Modify: `frontend/src/pages/BackupsStoragePage.tsx` (колонка + кнопка)
- Modify: `frontend/src/api/dto.ts` (+ `S3PoolDto`, `PolicyS3Dto`), `frontend/src/api/queries.ts` (хук пулов/мутации)
- Modify: `dev-stand/adminpanel/checks/45-backups-storage.sh` (дополнить сценарий модала)

**Interfaces:**
- Consumes: read-API: `GET /api/backups/s3-pools` → `[{name, active, nodes:[{name,state}], endpoints:[…]}]` (новый read-эндпоинт Inspection-модуля — добавить в Task 16 Files при необходимости); policy-PUT Task 16.
- UI-контракт: Mantine `Modal` + `Select` + `Checkbox` + `Button` (вверх/вниз; без DnD-библиотек); «Не выбрано» — допустимое сохраняемое значение; подпись-пояснение про разные ноды мастера/реплики; ошибки ProblemDetails в теле модала (400 — по полям, 503 — воркер/etcd); блокировка кнопки при отправке (двойной клик).

- [ ] **Step 17.1: Модал** — `BackupsPoolModal.tsx`: props `{cluster, policy, pools, onSave, onClose}`; состояние: выбранный пул (строка или null = «Не выбрано»), два массива выбранных имён (порядок — массив). Каркас:
  ```tsx
  // Модал «Пул бэкапов» (t36, arch/adminpanel/03 §3): Select пула + два
  // упорядоченных списка нод (мастер/реплика) — чекбоксы + кнопки вверх/вниз.
  export function BackupsPoolModal({ cluster, policy, pools, onSave, onClose }: Props) {
    const [pool, setPool] = useState<string | null>(policy.s3?.pool ?? null);
    const [master, setMaster] = useState<string[]>(policy.s3?.write_master ?? []);
    const [replica, setReplica] = useState<string[]>(policy.s3?.write_replica ?? []);
    const poolNodes = pools.find(p => p.name === pool)?.nodes ?? [];
    const [error, setError] = useState<string | null>(null);
    const [saving, setSaving] = useState(false);
    // OrderedList: чекбокс добаляет имя в КОНЕЦ массива (приоритет), кнопки
    // ↑/↓ двигают элемент; снятие чекбокса удаляет из массива.
    // onSave: полный policy — retention/full_max_age_sec/verify/drill из текущего
    // + (pool ? { pool, write_master: master, write_replica: replica } : НЕ включать s3).
    // 400 — строки ProblemDetails в error; 503 — «воркер/etcd недоступен».
    // Кнопка «Сохранить» disabled={saving} (двойной клик исключён).
  }
  ```
- [ ] **Step 17.2: Таблица** — колонка «Пул»: `<Badge>` имя пула либо `<Badge color="red">пул не выбран</Badge>`; кнопка per-строка кластера.
- [ ] **Step 17.3: DTO/запросы** — `S3PoolDto`/`PolicyS3Dto`; мутация — существующий policy-PUT клиент.
- [ ] **Step 17.4: Чек** — `dev-stand/adminpanel/checks/45-backups-storage.sh`: сценарий «выбор пула → сохранить → policy в etcd содержит s3; сброс → s3 нет» (curl + jq по существующему паттерну чека). Прогон чека на стенде — Task 19.
- [ ] **Step 17.5: Коммит** `git commit -am "feat(t36): панель — бейдж пула и модал «Пул бэкапов» (выбор+списки мастера/реплики)"`.

---

### Task 18: Алерты + Endpoints-инвентарь панели

**Вход:** Task 15, 16.
**Действие:** правила `backup-pool-missing`/`backup-pool-unreachable`; `MinioOptions.S3.Endpoints` (запятая) — list с первого доступного.
**Выход:** наблюдение пула на панели; инвентарь переживает отказ первого адреса.
**Проверка:** юниты правил/опций + юнит перебора целей.
**Spec:** §4.5 (алерты, live-инвентарь), §3 (adminpanel/02 §2.5), AC9 (в т.ч. «Endpoints-инвентарь переживает недоступность первого адреса»).

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupPoolMissingRule.cs`, `BackupPoolUnreachableRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/AlertEngine.cs` (регистрация)
- Modify: `src/AdminPanel.Core/BackupInfo.cs`/`BackupsInfo.cs` (policy-модель панели + `S3` — читать из policy-ключа парсером панели)
- Modify: `src/AdminPanel.Probes/S3/MinioOptions.cs` (+`Endpoints`), `MinioS3Client.cs` (список целей + перебор), `MinioInventoryLoop.cs` (передача списка целей в DTO)
- Test: `src/tests/AdminPanel.UnitTests/BackupPoolMissingRuleTests.cs`, `BackupPoolUnreachableRuleTests.cs`, `MinioOptionsTests`, `MinioS3ClientFailoverTests`

**Interfaces:**
- Produces:
  ```csharp
  // backup-pool-missing (critical, per-cluster): Active-кластер, подсистема
  // включена, policy без s3. Remedy: «выбери пул на странице "Хранилище
  // бэкапов" — без выбора бэкапы кластера не пишутся (t36, arch/19 §4)».
  // backup-pool-unreachable (critical, per-cluster): wal.Error или любой
  // full.Error содержит "s3-pool-exhausted". Remedy: «проверь ноды пула
  // <имя> (/s3/): воркер не доставил запись ни на одну ноду списков».
  // MinioOptions.MinioS3Options: + public string Endpoints { get; set; } = "";
  //   (запятая; Endpoint — валидный частный случай: один элемент)
  // MinioS3Client: цели — массив; ListBuckets/ListPage/health — перебор целей
  //   с первого доступного (per-попыточный HTTP-таймаут — существующий
  //   TimeoutSec); внутренняя фабрика клиентов — internal для юнит-инъекции.
  ```
- Consumes: снапшот `ClusterBackups`-проекция панели (`Policy.S3`), `MinioStorageInfo`.

- [ ] **Step 18.1: Юниты (AAA)** — missing: Active+policy без s3 → critical-алерт; с s3 → нет; не-Active → нет. unreachable: wal DEGRADED с `s3-pool-exhausted` → алерт; без маркера → нет; full FAILED с маркером → алерт; гасение: статус чист → алерта нет (правила — чистые функции от снапшота). MinioOptions: `Endpoints="http://a:1,http://b:2"` → 2 цели; пустые оба → грань выключена; задан только `Endpoint` → один. **Перебор: `ListBuckets_FirstTargetDown_SecondWins`** — `MinioS3Client` с двумя целями (internal-инъекция фабрики `Func<string, IMinioS3>`-обёрток или тестовый HttpMessageHandler): первая цель — отказ соединения → `ListBucketsAsync`/`ListPageAsync` успешны со второй, `Configured==true`; оба отказа → Failed (грань недоступна этим тиком).
- [ ] **Step 18.2: FAIL → реализация** — правила по образцу `BackupS3UnreachableRule`; AlertEngine-регистрация; `MinioS3Client` — массив целей + перебор; `MinioInventoryLoop` — единый экземпляр клиента с целями (порядок: `Endpoints`, затем `Endpoint`).
- [ ] **Step 18.3: Прогон** `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~BackupPool|FullyQualifiedName~Minio"`; коммит `git commit -am "feat(t36): алерты backup-pool-missing/unreachable + Endpoints-инвентарь с перебором целей"`.

---

### Task 19: Стенд — env, сид пула, слепки на owns3

**Вход:** Task 5–14 (код воркера готов).
**Действие:** `deploy/.env.example`/`docker-compose.yml` — новые env, убрать ушедшие; `00-up.sh` — сид S3-кластера `/s3/` (пул из одной ноды через S3Worker); owns3-сервис — цель слепков (отдельная пара).
**Выход:** полный стенд работает в пул-режиме.
**Проверка:** подъём `00-up.sh`; чеки 45/46/75 зелёные; журнал воркера без ошибок старта.
**Spec:** §4.4, §4.6, §7 (миграция), AC1 (стендовая часть).

**Files:**
- Modify: `deploy/.env.example` (бэкап-блок ≈85–110): убрать `PGW_BACKUP_S3_ENDPOINT/REGION/ACCESS_KEY/SECRET_KEY/ADVERTISED_ENDPOINT`; добавить:
  ```dotenv
  PGW_BACKUP_S3_BUCKET=pgworker-backups
  # Профиль кредов пула стенда (read-write ключ ownS3-контура — OWNS3_ACCESS_KEYS__0):
  PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY=pgw-backups-writer
  PGW_BACKUP_S3_PROFILES__backups-main__SECRET_KEY=pgw-backups-writer-pwd
  #PGW_BACKUP_S3_CA_BUNDLE_PATH=/tls/ca.pem
  # Слепки etcd — ОТДЕЛЬНАЯ пара (t36): адрес существующего owns3-сервиса,
  # свой bucket; обязательна при PGW_SNAPSHOTS_EXPORT_ENABLED=true.
  #PGW_SNAPSHOTS_S3_ENDPOINT=http://owns3:9000
  #PGW_SNAPSHOTS_S3_BUCKET=pgworker-etcd-snapshots
  #PGW_SNAPSHOTS_S3_ACCESS_KEY=pgw-backups-writer
  #PGW_SNAPSHOTS_S3_SECRET_KEY=pgw-backups-writer-pwd
  ```
- Modify: `deploy/docker-compose.yml` (env-биндинги PgWorker ≈44–62: `PgWorker__Backups__S3__Bucket`, профили `PgWorker__Backups__S3__Profiles__*` НЕ биндить — читатель env напрямую; `PgWorker__Snapshots__Export__S3__*`; убрать Endpoint/Region/AccessKey/SecretKey/AdvertisedEndpoint-биндинги)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (сид: создание S3-кластера через API S3Worker, ожидание `endpoints`; bucket `pgworker-backups` на первом endpoint)
- Modify: `dev-stand/adminpanel/checks/45-backups-storage.sh` (модал-сценарий Task 17)

**Interfaces:**
- Consumes: S3Worker API `POST /api/s3/clusters` — фактический DTO (camelCase, `src/S3Worker.App/Api/Operations/S3Api.cs:120–131`): `CreateS3ClusterRequest { name, platform, secretProfile, instances:[{name, placement:{dockerHost|swarmNode|swarmLabels}, resources:{cpu,memGi,diskGi}}], resources }`; ответ 201 `S3ClusterCreatedDto { name, state, nodes, secretProfile, instances:[имена] }`.

Сид пула (sh-фрагмент 00-up.sh; mTLS-клиент по паттерну `75-s3worker.sh`; идемпотентен: существующий Active-кластер → skip):
```sh
# 1) заявка S3-кластера «backups» — camelCase-DTO S3Worker (S3Api.cs):
body='{"name":"backups","platform":"owns3","secretProfile":"backups-main",'
body="$body"'"instances":[{"name":"node1","placement":{"dockerHost":"local"},'
body="$body"'"resources":{"cpu":0.5,"memGi":1,"diskGi":5}}]}'
#    POST $API/api/s3/clusters -d "$body"  (201/409 уже-существует = ok)
# 2) ожидание: /s3/clusters/backups/nodes/node1/state == RUNNING
#    и непустой /s3/clusters/backups/endpoints (полл ≤ 100 с — бюджет старта)
# 3) bucket на первом endpoint пула (mc, root-креды профиля стенда):
EP="$(первый из endpoints)"; mc alias set pool "http://$EP" "$OWNS3_ROOT_USER" "$OWNS3_ROOT_PASSWORD"
mc mb --ignore-existing pool/pgworker-backups
```

- [ ] **Step 19.1: env/compose** — правки выше; `docker compose -f deploy/docker-compose.yml config` валиден (переменные из .env).
- [ ] **Step 19.2: Сид 00-up.sh** — функция сида по скетчу (идемпотентна).
- [ ] **Step 19.3: Подъём и чеки** — `bash dev-stand/adminpanel/checks/00-up.sh`; затем `45-backups-storage.sh`, `46-owns3.sh`, `75-s3worker.sh` — зелёные; журнал PgWorker (`docker logs`) без `waiting-backup-pool` после зада политики сида. Примечание: до заведения policy кластеры стенда без `s3` — `backup-pool-missing` горит по канону (осознанно, spec §7).
- [ ] **Step 19.4: Зачистка стенда + коммит** — `bash dev-stand/adminpanel/checks/90-down.sh`; `docker network prune -f`; `git commit -am "feat(t36): стенд — env-профили, сид пула /s3/, слепки на отдельной паре"`.

---

### Task 20: Docker-E2E — фикстуры пул-режим + сценарии AC1–AC10

**Вход:** Task 11–19 (весь код), ветка собирается Release.
**Действие:** `E2eEnvironment` поднимает MinIO как ноду пула: сид `/s3/clusters/<slug>/…` + `/s3worker/portalloc/<slug>` + endpoints + policy; существующие бэкап-серии переведены на пул-режим (AC10); новая серия `E2eBackupPoolScenarios` (AC1–AC8).
**Выход:** E2E-приёмка задачи.
**Проверка:** `PGW_TEST_DOCKER=1` прогон бэкап-серий зелёный.
**Spec:** §9 (AC1–AC10), §10.

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (+метод `SeedS3PoolAsync(string pool, CancellationToken ct)` — пишет config/nodes/state/placement/portalloc/endpoints + env профиля воркеру; MinIO-нода = инстанс пула)
- Modify: существующие сценарии: `E2eBackupJobsScenarios.cs`, `E2eBackupVerifyScenarios.cs`, `E2eBackupFailsOnBadS3Scenarios.cs`, `E2eDrillScenarios.cs`, `E2eRestoreScenarios.cs`, `E2eRestoreNewClusterScenarios.cs`, `E2eRetentionScenarios.cs`, `E2eEtcdSnapshotExportScenarios.cs`, `E2eWalStream*.cs`, `E2eSupervisor*.cs` — env `PgWorker__Backups__S3__Endpoint` → сид пула + `PGW_BACKUP_S3_PROFILES__…`; policy с s3 (сид или API).
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupPoolScenarios.cs`

**Interfaces:**
- Consumes: `E2eEnvironment` (окно портов, MinIO, gateway), `E2eFixture.WaitForAsync`, каноны `docs/e2e-isolation.md`/`docs/e2e-launch.md` (артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `[PHASE]`, MarkFailed-контур, teardown при любом исходе, ассерт чистоты).

Сид пула в E2E (одна MinIO-нода):
```csharp
// /s3/clusters/<pool>/config — Active, secret_profile=backups-main
// /s3/clusters/<pool>/nodes/node1/placement = {"docker_host":"local"}
// /s3/clusters/<pool>/nodes/node1/state = "RUNNING"
// /s3worker/portalloc/<pool> = {"node1":{"host":"host.docker.internal","s3":<mapped>}}
// /s3/clusters/<pool>/endpoints = "host.docker.internal:<mapped>"
// policy кластера: {"s3":{"pool":"<pool>","write_master":["node1"],"write_replica":["node1"]}}
```
env воркера: `PGW_BACKUP_S3_BUCKET=pgworker-backups`, `PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY/SECRET_KEY` (minioadmin); слепочная пара при need (`E2eEtcdSnapshotExportScenarios` — отдельный bucket на том же MinIO).

- [ ] **Step 20.1: SeedS3PoolAsync + перевод фикстур** — метод в E2eEnvironment; все перечисленные сценарии: замена env на профили + сид пула/политики в `Arrange` (AC10 — существующие ассерты без изменений, кроме адреса S3-проверок: инспекция через endpoints пула).
- [ ] **Step 20.2: Прогон переведённых серий** (последовательно, с зачисткой docker между сериями: контейнеры + `docker network prune -f`):
  ```
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
    --filter "FullyQualifiedName~E2eBackupJobs|FullyQualifiedName~E2eBackupVerify|FullyQualifiedName~E2eBackupFails"
  ```
  далее аналогично по группам (WalStream → Restore/Drill → Retention/Supervisor → EtcdSnapshotExport). Каждая серия — зелёная; упавшая — teardown-артефакты в `/tmp/pgw-e2e-artifacts-*`, анализ по логам, БЕЗ перезапуска до формулировки причин (AGENTS.md).
- [ ] **Step 20.3: E2eBackupPoolScenarios (AC1–AC8)** — каждый Fact — своё окружение (per-method, паттерн E2eBackupJobsScenarios); сценарии:
  - AC1 `Pool_NotSelected_NoBackup_AndAlert`: кластер без s3 → полный не планируется (нет full-ключа за окно), policy-API с s3 → полный COMPLETED, объекты на ноде пула.
  - AC2 `Pool_MasterReplicaSplit_ObjectsOnTheirNodes`: пул из двух адресов (MinIO + вторая MinIO-нода фикстуры), `write_master=[n1]`,`write_replica=[n2]`: WAL мастер-агента наблюдаются на n1, sync-агента — на n2 (инспекция list по endpoint каждой), полный с реплики — объекты `full/<id>/` на n2.
  - AC3 `Pool_SingleNode_BothStreams_RestoreOk`: списки `[n1]/[n1]` → оба агента пишут на n1, restore COMPLETED.
  - AC4 `Pool_FailoverListAndExhaustion`: `[n1,n2]/[n2,n1]`, стоп n1 → мастер-агент доезжает до n2 (стрим продолжается, RPO: слот подтверждён после put на n2); `[n1]/[n2]`, стоп n1 → wal DEGRADED с `s3-pool-exhausted`, подъём n1 → самозаживление (ACTIVE).
  - AC5 `Pool_RoleSwitch_AgentRecreatedWithMasterList`: promote слейва → агент пересоздан, объекты его потока на ноде master-списка, цепочка непрерывна (`.history`).
  - AC6 `Pool_ReadAnyNode_RestoreAfterNodeStop` — **по букве spec §9 AC6**: данные писались на n1; сид-этап сценария выполняет `mc mirror` содержимого бакета n1 → n2 (эмуляция mesh-доставки: вторая нода держит копию объектов — как отставшая/догнавшая реплика пула); затем стоп n1 → verify/restore/drill читают со второй ноды пула (перебор ReadAll до найденного), restore COMPLETED. Хелпер `MirrorBucketAsync(n1, n2)` — mc-контейнер фикстуры (`E2eEnvironment.RunMcAsync`, паттерн создания bucket).
  - AC7 `Snapshots_OwnPair_BucketAndFailFast`: Enabled + пара → слепки в `pgworker-etcd-snapshots` (префикс `etcd/`), статус-ключ OK; Enabled без пары → контейнер воркера падает стартом (fail-fast, лог содержит имя секции).
  - AC8 `PolicyValidation_400_ByField` + `Restore_SourcePool_Dr`: через API воркера — 400-набор (Task 14, включая кейс «инстанс без portalloc — валиден») и restore из `source_pool` чужого префикса → COMPLETED.
  Изоляция: guid-имена, динамические порты (окно контура), полный teardown + ассерт чистоты (не осталось контейнеров/сетей/ключей префикса сценария).
- [ ] **Step 20.4: Полный прогон серии пула** — `PGW_TEST_DOCKER=1 … --filter FullyQualifiedName~E2eBackupPool` — зелёный; зачистка docker после.
- [ ] **Step 20.5: Коммит** `git commit -am "test(t36): E2E пул-режим — сид /s3/ в фикстурах + серия AC1-AC8 (AC6 — mc-mirror эмуляция mesh)"`.

---

### Task 21: Мерж-гейт — полные прогоны + roadmap-чистка

**Вход:** Task 1–20.
**Действие:** полный мерж-гейт: юниты всех сборок, интеграции, docker-E2E маркер + полная E2eFixture, AdminPanel-серии, чеки стенда; roadmap: удалить тег `t36-s3-pool-integration` из `arch/roadmap/s3.md` (из списков и `←`-зависимостей) тем же коммитом, что и мерж; добавить пункт external-целей (если ещё не отражён).
**Выход:** ветка готова к мержу (по отдельной команде пользователя).
**Проверка:** все серии зелёные; `arch/roadmap/s3.md` без t36.
**Spec:** §10 (мерж-гейт), §6 (external — отметить в roadmap).

**Files:**
- Modify: `arch/roadmap/s3.md`

**Interfaces:** — (финальный гейт; мерж в `main` — только по явной команде пользователя).

- [ ] **Step 21.1: Полные юниты** — `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~UnitTests"` — зелёные. Проверка: `docker ps -q --filter name=pgw- | wc -l` → 0.
- [ ] **Step 21.2: Интеграции (не-E2E)** — `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName!~E2e"`; `dotnet test src/tests/AdminPanel.IntegrationTests -c Debug`. Зачистка между сериями (`docker rm -f $(docker ps -aq --filter name=pgw-)`; `docker network prune -f`).
- [ ] **Step 21.3: Docker-E2E мерж-гейт (маркер, свежий Release)** — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` — зелёный; зачистка.
- [ ] **Step 21.4: Полная E2eFixture серия (Release)** — код воркеров менялся + env-контракты джобов: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2e"` — зелёный; телеметрия по канону (артефакты в teardown, `[PHASE]`, без перезапусков упавших). Зачистка.
- [ ] **Step 21.5: AdminPanel docker-серии + стенд** — панельные интеграции/E2E-чеки; полный подъём `00-up.sh` → чеки 45/46/75 → `90-down.sh`; зачистка.
- [ ] **Step 21.6: Roadmap-чистка** — `arch/roadmap/s3.md`: удалить пункт `t36-s3-pool-integration` (списки + `←`-ссылки из других пунктов); убедиться, что пункт про подключение внешних S3-целей (`external`) присутствует (если нет — добавить одной строкой со ссылкой на arch/19 §7 точку расширения). Без пометок «закрыта».
- [ ] **Step 21.7: Коммит гейта** — `git commit -am "chore(t36): мерж-гейт — roadmap-чистка (тег t36 слит)"` (по правилам roadmap — тем же коммитом, что и мерж; если мерж откладывается — коммит roadmap-правки готов в ветке и уезжает мержем). Мерж в `main` — ТОЛЬКО по явной команде пользователя.

---

## Self-Review (выполнен при написании; обновлён по итогам ревью Фазы 4)

- **Покрытие spec:** §3 → Task 1; §4.1 → Tasks 2–7; §4.2 → Tasks 8–13; §4.3 → Tasks 12, 14; §4.4–4.5 → Tasks 5, 15–18; §4.6 → Task 19; §5 → Tasks 3, 14; §7 → Task 19; §9 (AC1–AC10) → Task 20 (AC6 — с `mc mirror`-эмуляцией mesh-доставки, буква критерия сохранена; AC9 — Tasks 17–19 + чек 45; AC10 — Task 20.1–20.2); §10 → Task 21.
- **Источник имён инстансов:** `nodes/<I>/placement` (заявка) — Task 2 (InstanceNames), Task 14 (валидация 400), Task 6 (нода без portalloc-адреса — transient, не инвалид-policy), Task 20 (сид пишет placement-ключ). Portalloc — только адреса.
- **R3 «быстрый отказ, не молчание»:** per-попыточные таймауты — `BackupS3.requestTimeout` (Task 7), coreutils `timeout` в entrypoint (Task 9: проба 5 с / upload 300 с; Task 10: mc_copy_first 300 с / restore-wal 30 с), панельный инвентарь — TimeoutSec (Task 18).
- **Плейсхолдеры:** шаги содержат код/точные сигнатуры; механические правки описаны диффами-указателями с номерами строк; DTO сида S3Worker — по фактическому коду `S3Api.cs` (camelCase, `instances[].name`).
- **Типы:** `S3Pool`/`S3PoolNode(Name, State, PlacedOn, Host, S3Port)` (Task 2, со spec-наброском `PlacedOn`) ↔ `BackupPoolResolver` (6) ↔ `PoolBackupS3`/`BackupS3Factory`/`S3ClientOptions` (7, тип вводится здесь — зачистка Task 11 зависит от него) ↔ процессы (8–11) — единые имена; env-имена (`S3_ENDPOINTS`, `S3_READ_ENDPOINTS`, `PGW_BK_S3_ENDPOINTS`, `PGW_BACKUP_S3_PROFILES__*`, `PGW_SNAPSHOTS_S3_*`) сквозные; панельные файлы — фактические (`MinioS3Client.cs`/`MinioInventoryLoop.cs`/`MinioInventoryStore.cs`).
