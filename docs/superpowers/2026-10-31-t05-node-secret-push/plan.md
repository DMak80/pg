# t05-node-secret-push — план реализации (Фаза 3 dev-flow, рев. 8)

> **Для исполнителя (субагент coder):** работать по шагам, следом за заголовком
> «Глобальные ограничения». Каждый шаг: Вход → Действие → Выход → Проверка →
> Связь со spec. Подпункты — самодостаточные спецификации механической работы
> (по умолчанию исполняет сам кодер; по явной просьбе пользователя передаются
> слабой модели coder-flash без остального контекста).
>
> **Статус исполнения:** шаги 1–10, 12, 13 УЖЕ РЕАЛИЗОВАНЫ в ветке
> (feat-t05-node-secret-push, серия коммитов `feat/fix/test(t05)` — см.
> git-лог; сборка зелёная, юниты/интеграции/E2E t05-сценарии в ветке).
> Актуальная задача — **шаг 11 (ротация ETCD-FIRST, маркер окна — переносимый
> флаг `rest_window_open`)**, затем шаг 14 (мерж-гейт). Выполненные шаги
> оставлены как статусные строки.

**Цель:** контейнер PG-ноды без секретов в docker-env; доставка агентом PgNode
по статусной машине (`not_initialized → initializing → ready | start_failed`,
ключ `/pgworker/nodes/<C>/<X>/<N>/status` с lease); ротация per-cluster
секретов — порядок **ETCD-FIRST** (решение пользователя 19a): NEW-креды
коммитятся txn'ом в etcd В НАЧАЛЕ окна, rolling-применение rest-пары каналом
агента, финальный `ALTER ROLE` закрывает окно; маркер окна — переносимый
булев флаг `rest_window_open` журнала работы; `start_failed` — пересоздание
ноды с новым диском; компонент walreceiver переименован в PgNode.

**Спецификация:** `docs/superpowers/2026-10-31-t05-node-secret-push/spec.md`
(§3.7.4, §8-19a/19b). Канон — arch/14 §2.1/§3.2/§3.3 (work-строка:
`rest_window_open`)/§4/§5 C/§5 I (R0–R4), arch/13, arch/25 №23, arch/18,
arch/19.

Все пути — от корня worktree
`/Users/demakaev/ZCodeProject/worktrees/feat-t05-node-secret-push`.
Коммитить в feature-ветку свободно. Комментарии в коде — по-русски.

## Глобальные ограничения

- P/Invoke запрещён; только управляемый .NET.
- Таймауты/бюджеты существующих фаз НЕ менять (тик ~5 с; клиент канала —
  HttpClient 10 с; интервалы проб агента 5 с / Npgsql Timeout=5; фаза (б)
  initializing — без бюджета; `BrokerBootSec` ≤ 100 с).
- E2E/интеграции: полное самоочищение, динамические порты, guid-имена,
  зачистка контейнеров и сетей между сериями; сборка и тесты — ОТДЕЛЬНЫМИ
  командами; образы — publish на хосте.
- Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
  Release --nologo` — 0w/0e.
- Канал несёт ТОЛЬКО команды; статусы — из etcd. Ключ tmpfs env-паролей —
  `/run/pgw/spilo-env`. WAL-функции (`walreceiver_*`, `fetch-wal`,
  `WalReceiverLoop/Core/StatusSession`, `PGW_WALRECEIVER_NO_STREAM`) — не
  переименовываются.
- **Ротация ETCD-FIRST (канон):** `rest_pending` не существует; **маркер
  открытого окна — ТОЛЬКО переносимый булев флаг `rest_window_open`
  WorkState** (true — записью фазы `rotate-etcd-committed` R2, false —
  записью `done` R4; carry-forward ВСЕМИ писателями журнала — та же механика,
  что `unreachable`/HA-факты; журнал last-write-wins, supervise-записи
  каждый тик фазовый маркер затёрли бы). **Инвариант шага 11: предикат окна,
  takeover-детект и гварды строятся ОТ ФЛАГА; имена фаз `rotate-*` — только
  прогресс-наблюдаемость; failure-фазы (`alter/<X>`, `portalloc`,
  `backups-read`, `scope/<X>`, `snapshot`, `committing`, `commit-conflict`)
  и pre-window фазы (`started`, `waiting-nodes-ready`) на предикат НЕ
  влияют.** В открытом окне 28P01 (NEW из etcd против OLD pg_authid) —
  транзиент для всех потребителей; проба агента — не гейт статуса.
  Backup-креды (`backup_password`/`backup_exec`) — ВНЕ тикета t05
  (spec §8-19b: статичны с ensure; ротация — отдельная задача
  t06-rotate-backup-creds).

## Review Focus

1. Утечка secrets в `docker inspect` — закрыта E2E (шаг 13, в ветке).
2. Дребезг `mod_revision` — симметричная сериализация + put при изменении
   (реализовано; пробы `IsOnRevisionAsync` в ветке).
3. Ок-пинок/txn по VALUE (`TxnCompare.ValueEqual`) — реализовано.
4. **Маркер окна ротации:** затирание фазового маркера supervise-записями
   (журнал last-write-wins) исключено ТОЛЬКО переносимым флагом
   `rest_window_open` (carry-forward всеми писателями, включая
   `WriteSupervisionAsync`); открытие — строго записью
   `rotate-etcd-committed`, закрытие — строго записью `done` (шаг 11).
5. **Окно ETCD-FIRST:** txn-коммит NEW В НАЧАЛЕ делает всех etcd-потребителей
   видящими NEW до ALTER — 28P01 транзиент по определению окна; rolling
   (реплики→лидер, ≤1 нода/тик, безусловный выход после доставки) и финальный
   ALTER — последний; takeover — по флагу, NEW — из etcd (шаг 11).
6. `start_failed` → новый диск — границы object/restore/TO_REMOVE (в ветке).

---

## Шаги 1–10 — выполнены в ветке (статусные строки)

1. **Переименование WalReceiver → PgNode** — выполнено (проект
   `src/PgWorker.PgNode`, namespace, бинарь `pgnode` `/opt/pgw-node`,
   `[program:pgnode]`, `PgNodeIdentity`/`PgNodeAgentStatus`, Meter
   `PgWorker.PgNode`, `pgnode-nodes` file_sd + `SdGenerator:PgNodeOutputPath`,
   скрипты сборки образов, runbook; restore_command
   `/opt/pgw-node/pgnode fetch-wal %f %p`).
2. **Клиентский серт воркера** — выполнено (`RestPki.IssueWorkerClientCertificate`,
   CN=pgworker, DI-синглтон X509Certificate2).
3. **Core: env без секретов / NodeBootstrapBuilder / doorman-шаблон** —
   выполнено (TLS-пути шаблона `/run/pgw/doorman/tls.*`; плейсхолдер
   `${PGW_SU_PASSWORD}`; симметричный JSON).
4. **Драйвер BuildSpec** — выполнено (`DOORMAN_CONFIG_TEMPLATE`,
   `PGW_AGENT_CLIENT_CA`, tmpfs `/run/pgw`; EnsureNodeAsync без секретов).
5. **Агент: базовые блоки** — выполнено (`EnvironmentFile`,
   `NodeBootstrapData.IsValidForStart`, `BootstrapRenderer` (spilo-env),
   `SupervisorCommands`, `PgNodeStatus`, `NodeStatusSession` с txn по
   `TxnCompare.ValueEqual` (generic `EtcdEndpointFailover.CallAsync<T>`),
   `ComponentProbes` Timeout=5).
6. **Агент: статусная машина `PgNodeMachine`** — выполнено (публикация
   not_initialized; хендшейк: валидация→400 / txn→initializing / ок-пинок;
   фаза (а) ≤3 попытки параллельно, is_tool_start→prepared; фаза (б)
   SELECT 1 без бюджета; ApplyAsync — ФОНОВОЕ применение (фикс fe7e671c);
   чтение app_user/app_password/config(dbname) из etcd).
7. **Агент: листенер :8081 + команды** — выполнено (`POST /api/bootstrap`,
   `POST /api/bootstrap/apply`; GET нет; Program-интеграция).
8. **Воркер: PgNodeStatusReader + NodeChannelClient** — выполнено
   (Handshake/Apply; SAN-пиннинг; DI `pgnode-channel` 10 c).
9. **Воркер: NodeBootstrapDelivery + Ensure-хвосты** — выполнено
   (`EnsureAsync`: put при изменении → статус → команда; `IsOnRevisionAsync`
   content-проба; excludeParams из `PgtuneInputsFactory.ExcludeParams`;
   хвосты всех EnsureNode-путей; `DeliveryOutcome` Delivered/Wait/Transient).
10. **NodeSupervisor: шаг хендшейка/конвергенции; start_failed → новый
    диск** — выполнено (`EnsureBootstrapConvergenceAsync`; RecreateRestTls/
    InspectNodeEnv/env-хеши удалены; D2-чистка `/pgworker/nodes/<C>/`).
    Остаются связанные правки шага 11: гварды окна → флаг `RestWindowOpen`,
    эффективная пара — ключ etcd (П11.3).

12. **Образ ноды** — выполнено (autostart только pgnode; entrypoint без
    openssl/`DOORMAN_CONFIG_TEMPLATE`; обёртка spilo с fail-fast tmpfs и
    `source /run/pgw/spilo-env`; образ собирается — 4e36668d).
13. **E2E t05** — выполнено (4 сценария NodeSecrets + Rotate StartedAt —
    d8d98f0f, b63d0e33, f8fe5ebc, fe7e671c). Прогон с ETCD-FIRST — шаг 14.

---

## Шаг 11. ClusterSecretRotator: ETCD-FIRST (R0–R4), маркер окна `rest_window_open` — АКТУАЛЬНАЯ ЗАДАЧА

**Вход:** шаги 1–10 в ветке; ротатор — прежний порядок (R2-Alter в середине,
`rest_pending`, txn-финал) — `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs`
(534 строки); arch/14 §5 I/§3.3 и spec §3.7.4/§8-19a переписаны под
ETCD-FIRST + флаг `rest_window_open`.

**Действие (кодер):** перевести ротатор на ETCD-FIRST, маркер окна —
переносимый флаг; удалить `rest_pending`-инфраструктуру; обновить тесты.

1. **WorkJournal — флаг вместо pending** (`src/Shared.Etcd/Coordination/WorkJournal.cs`;
   механика carry-forward — строки 119–120, 173; П11.2): `WorkState`
   получает `[JsonPropertyName("rest_window_open")] bool? RestWindowOpen =
   null` (null — поля нет в JSON: старые записи/окно не открыто); параметр
   `restPending`/`dropRestPending` `WritePhaseAsync` заменяется на `bool?
   restWindowOpen = null` (null → carry-forward из текущей записи; true —
   открыть; false — закрыть); `WriteSupervisionAsync` переносит флаг
   (`RestWindowOpen: current.Value?.RestWindowOpen`) — supervise-запись
   каждый тик НЕ гасит открытое окно; `SupervisionState.RestPending` →
   `bool RestWindowOpen` (`ReadSupervisionStateAsync`: `state.Value?.
   RestWindowOpen ?? false`).
2. **Новый порядок `TickAsync`** (arch/14 §5 I; П11.1): **R0** заявка /
   битая-мусор (без изменений); **takeover-детект окна — ТОЛЬКО по флагу**:
   `journal.ReadAsync` → `work.Value?.RestWindowOpen == true` → окно открыто:
   продолжение БЕЗ заявки, все пары NEW из etcd (гвард waiting-nodes-ready
   НЕ применяется); иначе работа только по живой заявке; **гвард
   `waiting-nodes-ready`** (`AllNodesReadyAsync`) — до открытия окна (все
   канонические ноды ready), InProgress, заявка жива; **R1** ensure → OLD
   (compare-база); **R2 txn-коммит NEW В НАЧАЛЕ**: NEW ×4 (app, mover,
   bucket_admin, rest); txn `[ValueEqual × app/mover/admin/rest OLD + каждый
   dsn]` → `[put NEW ×4 + dsn-перезапись + del заявки]`; успех →
   `WritePhaseAsync(..., "rotate-etcd-committed", ..., restWindowOpen: true)`
   — окно открыто; compare проигран → `commit-conflict` (pre-window, ретрай
   тиком со свежими OLD); **backup_exec-ветка (gexec) удалена** (spec
   §8-19b: вне тикета t05); **R3 rolling** — прежний блок дословно (restore/
   TO_REMOVE/object-гейты; реплики→лидер; `IsOnRevisionAsync(newRest из
   etcd)` → continue; иначе `EnsureAsync(...)` → БЕЗУСЛОВНЫЙ `Finish(
   rotate-rest-rolling/<X>/<n>, Done)`; ≤1 нода/тик; применение у агента
   фоновое — `Delivered` = команда принята, прогресс — проба тиками); все
   не-пропущенные прошли → **R4 финальный ALTER** (закрытие окна): мастер
   каждого dsn-шарда (`ResolveMasterAsync`; null → `Finish(
   rotate-waiting-master/<X>, Done)` — флаг переносится, окно живёт) →
   admin-DSN → `ALTER ROLE app/bucket_admin/bucket_mover PASSWORD '<NEW из
   etcd>'` (ключи etcd, не локальные переменные R2 — takeover); сбой →
   `FailAsync alter/<X>` (флаг переносится); все ALTER-ены → снапшот P12 →
   `Finish("done", Done, restWindowOpen: false)` — окно закрыто. ALTER
   идемпотентен перезаписью; повторные тики в фазах `alter/*` безопасны.
3. **NodeSupervisor** (П11.3): эффективная rest-пара тика —
   `ensuredSecrets.Value.RestPassword` (ключ etcd: после R2 несёт NEW;
   `RestRotation.EffectivePassword`-вызов удалить); гварды окна
   (`dcs-converge-skipped-rest-window` ~463, `recreate-skipped-rest-window`
   ~760) — условие `restPending is not null` → `restRotationWindowOpen`
   (= `supervision.Value.RestWindowOpen`); проброс параметра `string?
   restPending` из `TickAsync`/`RecreateMarkedNodesAsync`/
   `ConvergeDcsConfigAsync` удалить (заменить на `bool
   restRotationWindowOpen`).
4. **RestRotation** — метод `EffectivePassword` удалить (потребитель один —
   надзор); комментарий `RestoreProcess.cs:483` обновить («REST-пара — ключ
   etcd (arch/14 §5 I ETCD-FIRST)»).
5. **PgNodeMachine** — без правок (фоновое применение, 28P01-проба —
   waiting-повтор, не гейт; `ready` пост-apply не ревалидируется); юнит-кейсы
   окна — П11.6.
6. **Тесты** (MAJOR): WorkJournalTests-секция → флаг (П11.4);
   ClusterSecretRotatorTests → ETCD-FIRST, полный список (П11.5);
   RestoreProcessTests-сид окна → `rest_window_open` (П11.7);
   RestRotationTests-интеграция → ETCD-FIRST (П11.6); E2E Rotate — прогон в
   шаге 14.

**Выход:** ротация ETCD-FIRST с переносимым маркером окна; `rest_pending`
отсутствует в коде и тестах; гварды/предикты — от флага.

**Проверка:** сборка Release 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/Shared.Etcd.UnitTests -c Release --no-build` → зелёные;
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c
Release --no-build` → зелёные; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.IntegrationTests -c Release --no-build --filter
"FullyQualifiedName~RestRotation|FullyQualifiedName~RestoreProcessTests"`
→ зелёные.

**Связь со spec:** §3.7.4, §6.3, §8-19a (ETCD-FIRST + флаг)/§8-19b (backup
вне тикета); arch/14 §3.3 (`rest_window_open`), §5 I (R0–R4, гварды от
флага, failure-фазы на предикат не влияют).

**Подпункты:**

- П11.1. **Ротатор: новый порядок.** В
  `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs` заменить тело
  `TickAsync` (после блока битой заявки) на приведённый ниже порядок. Полный
  скелет новых/изменённых блоков (точные имена — из фактического файла):
```csharp
        // Takeover-детект открытого окна (arch/14 §5 I/§3.3): маркер —
        // ТОЛЬКО переносимый флаг rest_window_open (заявки уже нет — снята
        // txn R2; supervise-записи надзора флаг переносят, фазовый маркер
        // затирался бы). Имена фаз — только прогресс: failure-фазы на
        // предикат окна не влияют.
        var work = await journal.ReadAsync(cluster, ct);
        if (!work.IsSuccess)
            return await FailAsync(cluster, work.Error!, "journal-read", ct);
        var windowOpen = work.Value?.RestWindowOpen == true;

        if (!windowOpen)
        {
            if (ticket.Value is null)
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // нет заявки и окна — no-op

            // Гвард «тихого окна» (ДО записи started — как в текущем коде
            // строк 82–91): до ОТКРЫТИЯ окна все канонические ноды ready
            // (ETCD-FIRST: после R2 etcd несёт NEW — initializing-нода
            // держит 28P01 до R4; окно не открываем поверх стартов).
            var addressesGuard = await ReadPortAllocAsync(cluster, ct);
            if (!addressesGuard.IsSuccess)
                return await FailAsync(cluster, addressesGuard.Error!, "portalloc", ct);
            if (!await AllNodesReadyAsync(cluster, snap, addressesGuard.Value, ct))
                return await Finish(cluster, "waiting-nodes-ready", ProcessOutcome.InProgress, ct);

            var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
            if (!started.IsSuccess)
                return Result<ProcessOutcome>.Failed(started.Error!);

            // R1: ensure четвёрки — OLD-значения (compare-база R2).
            var credsResult = await appSecret.EnsureAsync(cluster, snap.Config, ct);
            if (!credsResult.IsSuccess)
                return await FailAsync(cluster, credsResult.Error!, "ensure-app-secret", ct);
            var creds = credsResult.Value;

            // R2 (ETCD-FIRST, arch/14 §5 I): txn-коммит NEW-четвёрки В
            // НАЧАЛЕ окна — etcd несёт NEW, роли PG ещё OLD; заявка снимается
            // той же txn (неразделимо). backup_exec вне тикета t05 (spec
            // §8-19b: backup_password статичен с ensure).
            var newAppPassword = AppSecretGenerator.Generate();
            var newMoverPassword = AppSecretGenerator.Generate();
            var newBucketAdminPassword = AppSecretGenerator.Generate();
            var newRestPassword = AppSecretGenerator.Generate();
            var compares = new List<TxnCompare>
            {
                TxnCompare.ValueEqual(PasswordKey(cluster), creds.App.Password),
                TxnCompare.ValueEqual(MoverKey(cluster), creds.MoverPassword),
                TxnCompare.ValueEqual(BucketAdminPasswordKey(cluster), creds.BucketAdmin.Password),
                TxnCompare.ValueEqual(RestKey(cluster), creds.RestPassword),
            };
            var ops = new List<TxnOp>
            {
                new TxnOp.Put(PasswordKey(cluster), newAppPassword, null),
                new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
                new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
                new TxnOp.Put(RestKey(cluster), newRestPassword, null),
                new TxnOp.Delete(TicketKey(cluster), Prefix: false),
            };
            foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
            {
                compares.Add(TxnCompare.ValueEqual(DsnKey(cluster, shard.Name), shard.Dsn!));
                ops.Add(new TxnOp.Put(DsnKey(cluster, shard.Name),
                    PasswordRegex().Replace(shard.Dsn!,
                        m => (m.Value.StartsWith(' ') ? " " : "") + "password=" + newBucketAdminPassword),
                    null));
            }

            var committed = await TxnAsync(TxnRequest.Of([.. compares], [.. ops]), ct);
            if (!committed.IsSuccess)
                return await FailAsync(cluster, committed.Error!, "committing", ct);
            if (!committed.Value.Succeeded)
                return await FailAsync(cluster,
                    new ApplicationException(
                        "credentials/dsn changed since they were read (external write?) — retry on the next tick"),
                    "commit-conflict", ct);
            // Открытие окна: флаг true — ЭТОЙ записью фазы rotate-etcd-committed.
            var etcdCommitted = await journal.WritePhaseAsync(
                cluster, Op, "rotate-etcd-committed", claims.InstanceId, null, ct,
                restWindowOpen: true);
            if (!etcdCommitted.IsSuccess)
                return Result<ProcessOutcome>.Failed(etcdCommitted.Error!);
        }

        // Открытое окно: все пары — из etcd (NEW с R2; takeover-безопасно).
        var restKv = await GetAsync(RestKey(cluster), ct);
        if (!restKv.IsSuccess || restKv.Value is not { } restKey)
            return await FailAsync(cluster,
                new ApplicationException("open rotation window: rest_password key is unreadable"),
                "rotate-etcd-read", ct);
        var newRest = restKey.Value;
        var addressesResult = await ReadPortAllocAsync(cluster, ct);
        if (!addressesResult.IsSuccess)
            return await FailAsync(cluster, addressesResult.Error!, "portalloc", ct);
        var addressMap = addressesResult.Value;

        // R3: rolling rest-пары каналом агента (текущий блок фактического
        // файла — сохранить дословно; объявление `restoring` живёт ЗДЕСЬ,
        // R4 его не использует) … (существующий блок, см. механику (г)) …

        // R4 (финальный ALTER — закрытие окна, «etcd — источник, PG следует»):
        // NEW читаются из etcd (не локальные переменные R2 — takeover).
        var newApp = await GetAsync(PasswordKey(cluster), ct);
        var newMover = await GetAsync(MoverKey(cluster), ct);
        var newAdmin = await GetAsync(BucketAdminPasswordKey(cluster), ct);
        var appUser = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!newApp.IsSuccess || newApp.Value is null || !newMover.IsSuccess
            || newMover.Value is null || !newAdmin.IsSuccess || newAdmin.Value is null
            || !appUser.IsSuccess)
            return await FailAsync(cluster,
                new ApplicationException("open rotation window: NEW credentials unreadable from etcd"),
                "rotate-etcd-read", ct);
        var credsNow = appUser.Value;
        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            var master = await ResolveMasterAsync(cluster, shard, addressMap, ct);
            if (master is null)
                return await Finish(cluster, $"rotate-waiting-master/{shard.Name}", ProcessOutcome.Done, ct);
            var dsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, snap.Config.DbName, secrets);
            foreach (var (role, password) in new[]
                     {
                         (credsNow.App.User, newApp.Value!.Value),
                         (credsNow.BucketAdmin.User, newAdmin.Value!.Value),
                         (MoverRole, newMover.Value!.Value),
                     })
            {
                var altered = await db.ExecuteAsync(
                    dsn, DatabaseProvisioner.BuildAlterRolePasswordSql(role, password), ct);
                if (!altered.IsSuccess)
                    return await FailAsync(cluster, altered.Error!, $"alter/{shard.Name}", ct);
            }
        }

        // Окно закрыто: снапшот P12 + done (флаг false — ЭТОЙ записью).
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }
        return await Finish(cluster, "done", ProcessOutcome.Done, ct, restWindowOpen: false);
```
  Механика переноса: (а) `rest_pending`-блок (строки фактического файла
  ~110–134) и параметр `dropRestPending` у `Finish`/`FailAsync` (~465–482) —
  удалить; сигнатуры `Finish/FailAsync` получают `bool? restWindowOpen =
  null` и проксируют в `WritePhaseAsync`; (б) прежний R2-Alter-блок
  (~138–186, включая backup_exec gexec-ветку) — удалить целиком (ALTER
  переехал в R4; backup вне тикета, spec §8-19b) и УДАЛИТЬ приватный
  хелпер `BackupKey` (строка ~350 — `private static string BackupKey(string
  cluster) => $"/clusters/{cluster}/backup_password";`: единственные
  потребители — удаляемые R2-Alter/R3-txn блоки, после правок grep
  `BackupKey` в файле пуст); (в) прежний R3-txn-блок (~246–283) — удалить
  (txn теперь R2 в начале); (г) R3-rolling-блок (~188–244) — сохранить
  дословно, заменив условие входа `if (work.Value?.RestPending is not
  null) {…}` на безусловное выполнение (окно уже открыто/только что
  открыто), источник пары `newRestPassword` → `newRest`, обращения
  `addresses.Value` → `addressMap` (порт-план читается ОДИН раз после
  ветвления; guard-чтение `addressesGuard` — только в заявочной ветке);
  объявление `var restoring = …` (~197) остаётся ВНУТРИ R3-блока — R4 его не
  использует и не объявляет; (д) порядок фаз: `(waiting-nodes-ready)` →
  `started` → `rotate-etcd-committed` → `rotate-rest-rolling/<X>/<n>` →
  (`rotate-waiting-master/<X>` | `alter/<X>`) → `done` — гвард ДО записи
  started (как в скелете и фактическом коде строк 82–91).
- П11.2. **WorkJournal: флаг `rest_window_open`.** В
  `src/Shared.Etcd/Coordination/WorkJournal.cs`:
  а) record `WorkState` — параметр `[property: JsonPropertyName(
  "rest_pending")] string? RestPending = null` (строка 23) заменить на
  `[property: JsonPropertyName("rest_window_open")] bool? RestWindowOpen =
  null`; конструктор-вызовы `new WorkState(...)` — заменить `carryRest`/
  `RestPending:` на `RestWindowOpen:`;
  б) `WritePhaseAsync` — параметры `string? restPending = null, bool
  dropRestPending = false` (строка 96) заменить на `bool? restWindowOpen =
  null`; carry-механику (строки 119–120 `if (carryRest is null &&
  !dropRestPending) carryRest = current.Value?.RestPending;`) заменить на
  `carryWindow ??= current.Value?.RestWindowOpen;` с чтением current при
  `track is null || restWindowOpen is null` (локальная `bool? carryWindow =
  restWindowOpen;`); XML-комментарий: «rest_window_open — маркер открытого
  окна REST-ротации (arch/14 §3.3/§5 I): true/false задают открытие/
  закрытие, null — carry-forward всеми писателями (как unreachable/факты)»;
  в) `WriteSupervisionAsync` (строки 161–175) — аргумент `RestPending:
  current.Value?.RestPending` заменить на `RestWindowOpen: current.Value?.
  RestWindowOpen` (supervise-запись каждый тик НЕ гасит открытое окно);
  чтение `ReadAsync` перед put — в объёме carry-forward (unreachable/факты/
  флаг), поведение остальных полей не менять;
  г) record `SupervisionState` — поле `string? RestPending = null` заменить
  на `bool RestWindowOpen`; `ReadSupervisionStateAsync` — передавать
  `state.Value?.RestWindowOpen ?? false`.
- П11.3. **NodeSupervisor: гварды от флага; эффективная пара — ключ.** В
  `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`:
  а) строки ~93–103 — заменить вычисление эффективной пары на:
```csharp
        // Эффективная REST-пара тика — ключ etcd (arch/14 §5 I ETCD-FIRST:
        // с R2-txn ключ несёт NEW; ensure идемпотентен — легаси-кластер без
        // rest_password получает ключ до EnsureNode-путей тика).
        var ensuredSecrets = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!ensuredSecrets.IsSuccess)
            return Fail(ensuredSecrets.Error!);
        var restPassword = ensuredSecrets.Value.RestPassword;
        var restRotationWindowOpen = supervision.Value.RestWindowOpen;
```
  б) `ConvergeDcsConfigAsync` (сигнатура ~426: параметр `string? restPending`
  → `bool restRotationWindowOpen`): гвард (~463) — `if (restPending is not
  null)` → `if (restRotationWindowOpen)`; вызов ~255 — передавать
  `restRotationWindowOpen`; XML-комментарий: «окно ротации открыто (флаг
  rest_window_open) — PATCH /config транзиент-skip»;
  в) `RecreateMarkedNodesAsync` (сигнатура ~716: то же) — гвард switchover
  (~760): `if (restPending is not null)` → `if (restRotationWindowOpen)`;
  вызов ~166 — передавать `restRotationWindowOpen`.
- П11.4. **WorkJournalTests: секция флага** (`src/tests/Shared.Etcd.UnitTests/
  WorkJournalTests.cs`, строки ~376–485 — ШЕСТЬ тестов секции rest_pending):
  а) `WritePhaseAsync_WithRestPending_RoundTrip` → `WritePhaseAsync_OpenRestWindow_RoundTrip`
  — фаза rotate-etcd-committed с `restWindowOpen: true` → JSON work-ключа
  содержит `"rest_window_open":true`, `ReadAsync().Value!.RestWindowOpen ==
  true`;
  б) `WritePhaseAsync_WithoutRestPending_CarriesForward` →
  `WritePhaseAsync_WithoutRestWindow_CarriesForward` — при открытом флаге
  ЛЮБАЯ другая фазовая запись переносит флаг (не гасит);
  в) `WritePhaseAsync_DropRestPending_ClearsField` →
  `WritePhaseAsync_CloseRestWindow_ClearsFlag` — фаза done с
  `restWindowOpen: false` → `RestWindowOpen == false`/JSON без `true`;
  г) `WriteSupervisionAsync_DoesNotEraseRestPending` (:437) — УДАЛИТЬ
  (перенос supervise-записью уже покрыт тестом (б) carry-forward; дубль);
  д) `ReadSupervisionStateAsync_ReturnsRestPending` (:456) →
  `ReadSupervisionStateAsync_ReturnsRestWindowOpen` — открытый флаг →
  `SupervisionState.RestWindowOpen == true` (опора П11.3); закрытый/
  отсутствующий → `false`;
  е) `ReadAsync_LegacyKeyWithoutRestPending_Null` (:472) →
  `ReadAsync_LegacyKeyWithoutRestWindow_False` — легаси-JSON work-ключа БЕЗ
  поля `rest_window_open` → `RestWindowOpen == null` (поле отсутствует) и
  `ReadSupervisionStateAsync` даёт `RestWindowOpen == false`.
  Все `rest_pending`-подстроки в ассертах секции удалить.
- П11.5. **ClusterSecretRotatorTests → ETCD-FIRST — полный список**
  (`src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretRotatorTests.cs`;
  rig: FakeEtcd/FakeSql/`FakeChannel` с `Action? OnApply`-инъекцией,
  `NewRig` ~157, `RunUntilCommitAsync` ~186):
  (а) `Tick_Ticket_BackupExecRoleAbsent_CreatedByGuard` (:284–308) —
  УДАЛИТЬ (тест удалённой gexec-ветки backup_exec, spec §8-19b);
  (б) `Tick_Ticket_AltersAllShardsAndCommitsAtomically` (:216–280) →
  `Rotate_EtcdFirstCommitsUpfront_ThenRolls_ThenAlters` — Arrange прежний
  (сид, ready-статусы); Act — цикл `RunUntilDoneAsync`; Assert: (1) txn
  первого тика — состав БЕЗ backup: `commit.Ops` содержит РОВНО 4 put кред
  (`/clusters/shop/app_password|mover_password|bucket_admin_password|
  rest_password`) + dsn-перепись каждого dsn-шарда + `Delete
  /pgworker/rotations/shop`; `commit.Compare` — ValueEqual по 4 кредам
  (OLD) + каждому dsn; НЕТ `backup_password` в ops/compare (ассерты
  :242–245/:253–254/:266–267 про backup — убрать по §8-19b); (2) txn
  коммитится ДО резолва мастера и ДО любых доставок канала (журналы фейков:
  индекс txn < первый ApplyAsync/ALTER); (3) rolling — реплика раньше лидера,
  ≤1 delivery/тик; (4) ФИНАЛЬНЫЕ ALTER — после последнего apply: sqlTexts —
  по 3 ALTER (app/bucket_admin/bucket_mover) на мастера каждого шарда,
  пароли == NEW-ключам etcd; (5) заявка снята txn'ем R2; журнальный флаг
  `RestWindowOpen` true после R2 и false после `done`;
  (в) хелпер `RunUntilCommitAsync` (:184–198) → `RunUntilDoneAsync`:
  условие выхода — «журнальная фаза `done` (work.Phase == "done") ИЛИ
  `work.RestWindowOpen == false` после хотя бы одного открытого окна» →
  `(Done: true, Failed: false)`; лимит тиков 10 сохранить; первый Failed —
  `(Done: false, Failed: true)`; все вызовы в файле перевести на новый
  хелпер;
  (г) `Tick_SharedHostMasterKey_ResolvesByDoormanPort` (:316–348) —
  переписать: имя сохранить; Assert: txn-коммит UPFRONT (первый тик:
  креды NEW в etcd, заявка снята — ДО резолва мастера); резолв мастера по
  doorman-порту — в R4 (sqlTexts: ALTER-тексты несут Dsn хоста мастера,
  найденного по порту); RunUntilDoneAsync → done;
  (д) `Tick_TicketShardWithoutMaster_PasswordUntouchedTicketAlive`
  (:350–368) → `Rotate_ShardWithoutMaster_WindowOpen_WaitingMaster`:
  Assert: первый тик — txn-коммит (заявка СНЯТА — не «TicketAlive»:
  ETCD-FIRST снимает заявку R2); фаза `rotate-waiting-master/<X>`,
  `RestWindowOpen == true` (окно живёт), креды в etcd NEW (не
  «PasswordUntouched-навсегда» — NEW закоммичены, ALTER ждёт мастера);
  следующий тик при появившемся мастере — до done;
  (е) compare-lost — ДВА обязательных теста (dsn-вариант уже реализован
  кодером; кред-вариант ОТСУТСТВУЕТ — добавить):
  (е1) `Rotate_ExternalCredChange_CompareLostRetriable` (НОВЫЙ, кред) —
  механика инъекции ОТЛИЧАЕТСЯ от dsn-варианта: кред-compare строится по
  СВЕЖЕМУ ensure-чтению тика — до-тиковая перезапись креда была бы прочитана
  ensure'ом как OLD и compare ПРОШЁЛ бы. Поэтому «внешняя перезапись
  etcdctl» инъецируется хуком `FakeEtcd.OnTxnBeforeCompare`
  (`Action<TxnRequest>?`, Fakes.cs:59 — вызывается ПОСЛЕ ensure-чтения и ДО
  применения txn ротатора): в тесте после `SeedTicket` и снапшота поставить
  `rig.Etcd.OnTxnBeforeCompare = _ => rig.Etcd.Store["/clusters/shop/
  app_password"] = new Fakes.FakeEtcd.Entry("ExternallyRewritten0000000000X",
  modRevision: 9, 9);` (значение ≠ OLD-сида "OldPassword000000000000000000A").
  Assert (первый тик): `outcome.IsSuccess` false; `work.Phase ==
  "commit-conflict"`; `work.RestWindowOpen` NotTrue (окно не открыто);
  креды etcd НЕ изменены нашим txn (`app_password` == инъецированному,
  `bucket_admin_password`/`mover_password`/`rest_password` == OLD-сиду);
  заявка жива (`/pgworker/rotations/shop` в Store). Снять хук → ретрай
  `RunUntilDoneAsync` до `done` (свежий ensure читает инъецированное как
  OLD, txn проходит: заявка снята, флаг закрыт);
  (е2) `Rotate_ExternalDsnChange_CompareLostRetriable` (dsn — реализован
  кодером, :492): сверить с эталоном — снапшот ДО инъекции
  (`var staleSnapshot = await Snapshot(rig.Etcd);`), затем перезапись
  `/clusters/shop/shards/shard1/dsn` в Store ДО первого тика (dsn-compare
  строится по ПЕРЕДАННОМУ снапшоту — до-тиковая инъекция корректна);
  ассерты первого тика: outcome false, заявка жива, окно не открыто
  (`RestWindowOpen` NotTrue), креды OLD; далее `RunUntilDoneAsync` до
  done. Эталоном считать фактическую реализацию кодера;
  (з) серия `Rotate_Rest*` + сопутствующие (:492–666): `Rotate_RestStart_
  FixesPendingAndDeliversReplicaFirst` (:492) → `Rotate_FirstTick_Commits_
  Upfront_AndDeliversReplicaFirst` (первый тик: txn-коммит + флаг true +
  первая доставка — реплика; без rest_pending-ассертов);
  `Rotate_RestRolling_ContinuesSamePending` (:520) → `Rotate_Rolling_
  ContinuesByWindowFlag_Takeover` (новый ротатор, work `RestWindowOpen:
  true` без заявки — продолжение, пара из etcd, лидер последним);
  `Rotate_RestCommit_TxnComparePutAndPendingDrop` (:545) →
  `Rotate_Done_ClosesWindow` (после done флаг false; повторный тик без
  заявки — no-op); `Rotate_RestPendingLost_FreshPair` (:572) — УДАЛИТЬ
  (pending не существует); `Rotate_ShardWithoutDsn_NotDelivered` (:597) —
  сохранить, ассерты завершить done (шард без dsn: без доставок и без
  ALTER, txn всё равно коммитит креды); `AssertWindowCommittedAsync`
  (:616–621) → `AssertWindowClosedAsync` (заявки нет; rest_password NEW
  32-симв; `RestWindowOpen` false — вместо `RestPending.Should().BeNull`);
  `Rotate_RestoreShard_DoesNotHoldRestWindow` (:628) и
  `Rotate_AdoptedNode_DoesNotHoldRestWindow` (:652) — сохранить смысл
  (restore/object-ноды окно не удерживают: их ноды не участвуют в rolling),
  финальные ассерты — через `AssertWindowClosedAsync`, комментарии
  «создадутся с новой парой после R3» → «…после закрытия окна (NEW в etcd
  с R2)». Тесты `Tick_NoTicket_NoOp` (:201), `Tick_MalformedTicket_
  RemovedAsGarbage` (:438), `Tick_ClaimNotMine_MutationsForbidden` (:456) —
  без правок (no-op-тест дополнить: при `RestWindowOpen:true` без заявки —
  продолжение, не no-op — если кейс ещё не покрыт — см.
  `Rotate_Rolling_ContinuesByWindowFlag_Takeover` в (з)).
- П11.6. **Интеграционные тесты ротации и юниты окна.**
  а) `src/tests/PgWorker.IntegrationTests/Docker/RestRotationTests.cs` —
  ETCD-FIRST-сценарий (каркас сохраняется: EtcdFixture, SeedAsync,
  StubDriver/StubSql, FakeChannel, SeedStatusAsync):
  `Rotation_EtcdFirst_CommitsThenRollsThenAlters` (AAA): Arrange — сид,
  ensure OLD-четвёрки, статусы всех нод ready+applied==mod(OLD); Act —
  заявка, цикл тиков до фазы `done`; Assert: (1) первый тик — txn-коммит ДО
  первой доставки (ключи etcd NEW, заявка снята, фаза
  rotate-etcd-committed, work-флаг `rest_window_open:true`); (2) в окне
  supervise-подобная фазовая запись флаг НЕ гасит (проверка через повторное
  чтение work после промежуточной фазы); (3) rolling — по одной ноде за тик,
  реплика раньше лидера; (4) финальный ALTER — после последнего apply:
  StubSql — по три ALTER (app/bucket_admin/bucket_mover) на мастера каждого
  шарда, пароли == NEW-ключам etcd; (5) `done`: флаг false; повторный тик
  без заявки — no-op; (6) takeover: новый ротатор при `rest_window_open:
  true` продолжает без заявки; (7) вторая заявка — новый проход; (8) в
  work-JSON НЕТ подстроки `rest_pending`. Teardown — префиксы кластера.
  б) юниты гвардов надзора (по образцу NodeSupervisorBootstrapStepTests,
  фейки): work-сид `{"op":"rotate-app-password","phase":"alter/shard1",
  "rest_window_open":true}` (failure-фаза! гвард срабатывает — предикат от
  флага) → PATCH-гвард и RecreateMarked-soft пишут skip-фазы; сид без флага
  (false/отсутствует) → гварды НЕ срабатывают; supervise-запись поверх
  открытого окна → флаг переносится (гварды продолжают действовать).
  в) PgNodeMachineTests — кейсы окна (если ещё нет): doorman-проба false
  (28P01) в фазе (б) → `initializing` (waiting-повтор, НЕ start_failed),
  после true → `ready`; apply на ready → фоновые рестарты, статус остаётся
  `ready`.
- П11.7. **RestoreProcessTests: сид окна.** В
  `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`
  (~1253–1278, `RejoinRotationWindowEnsureCarriesPendingPair`): сид work-
  ключа заменить на
```csharp
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/work/c1",
            """{"op":"rotate-app-password","phase":"rotate-rest-rolling","instance":"i","updated_unix":1,"rest_window_open":true}""",
            null, ct);
```
  комментарий Arrange обновить: «журнал работы несёт rest_window_open=true
  (открытое окно REST-ротации, ETCD-FIRST): EnsureNode-путь restore окно НЕ
  расширяет — нода поднимается с NEW-парой из etcd (arch/14 §5 I)»;
  переименовать тест в `RejoinRotationWindowEnsureCarriesWindowFlag` (или
  оставить имя, обновив только сид/комментарий — ассертов по pending-строке
  в тесте нет).

## Шаг 12. Образ ноды — выполнен

(autostart только pgnode; entrypoint `DOORMAN_CONFIG_TEMPLATE` без openssl;
обёртка spilo: fail-fast tmpfs + `source /run/pgw/spilo-env` + export
SPILO_CONFIGURATION/SSL_RESTAPI_*; образ собирается — 4e36668d.)

## Шаг 13. E2E t05 — выполнено; прогон с ETCD-FIRST — шаг 14

(4 сценария NodeSecrets + Rotate StartedAt — в ветке. Правки шага 11 контур
E2E не меняют; StartedAt-ассерт валиден — ETCD-FIRST не пересоздаёт
контейнеры.)

## Шаг 14. Мерж-гейт (приёмка spec §9)

**Вход:** шаг 11 завершён.

**Действие (кодер):** полный гейт ОТДЕЛЬНЫМИ командами; зачистка
контейнеров/сетей между сериями; упавшие тесты не перезапускать без разбора
логов.

1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
   --nologo` → 0w/0e.
2. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests
   -c Release --no-build` и `… src/tests/Shared.Etcd.UnitTests -c Release
   --no-build` → зелёные; интеграции: `… src/tests/PgWorker.IntegrationTests
   -c Release --no-build` → зелёные.
3. `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
   src/tests/PgWorker.E2ePrepare -c Release` → штамп готовности.
4. docker-E2E: минимум `DOTNET_CLI_UI_LANGUAGE=en dotnet test
   src/tests/PgWorker.E2eTests -c Release --no-build --filter
   FullyQualifiedName~Scale_AddEmptyShard`; полный прогон E2eFixture
   (`… --no-build`) → зелёные (NodeSecrets-сценарии, Rotate StartedAt,
   ac2-doorman-sql, WalStream).
5. Grep-контроль: `arch/` без «walreceiver» как имени компонента; в `src/`
   (код и тесты) нет `rest_pending`/`RestPending`/`dropRestPending`/
   `EffectivePassword` (имена упразднены); ВАЖНО: `rest_window_open`/
   `RestWindowOpen` — канонный флаг (arch/14 §3.3), grep-гейт на него НЕ
   распространяется; `docker inspect` нод — без секретов (E2E-ассерт).
6. Роадмап: тег `t05-node-secret-push` удалить из
   `arch/roadmap/pgworker.md` мерж-коммитом; прочие `← t05`-маркеры, если
   появятся у уже реализованных пунктов, — удалить тем же коммитом; пункт
   `t06-rotate-backup-creds` (ротация backup-кредов, spec §8-19b) —
   ОТДЕЛЬНАЯ задача: в мерж t05 НЕ входит и СОХРАНЯЕТ маркер `← t05` до
   своей реализации.

**Выход:** критерии §9 закрыты; ветка готова к ревью/мержу.

**Связь со spec:** §9, §5, §8-19b (граница тикета).

---

## Самопроверка плана (выполнена plan-writer'ом)

- **Рев. 8 (текущая, фиксы целевого ревью шага 11):** (MAJOR) П11.5 — полный
  список `ClusterSecretRotatorTests` (фактические имена/строки: :216–280,
  :284–308, :316–348, :350–368, :370–398, :400–435, :492–666): удаление
  backup-gexec-теста; атомарный тест → ETCD-FIRST (txn-состав без backup,
  коммит ДО мастера/доставок, ALTER последним); `RunUntilCommitAsync` →
  `RunUntilDoneAsync` (выход по done/флагу false); мастер-резолв/без-
  мастера/compare-lost-тесты — под txn-в-начале (инъекции ДО первого тика;
  заявка снимается R2, окно живёт на флаге); серия Rotate_Rest* —
  переименована/удалена по смыслу; `AssertWindowCommittedAsync` →
  `AssertWindowClosedAsync`. (LOW) П11.4 — все ШЕСТЬ тестов секции
  WorkJournalTests (~376–485): 3 переписаны на флаг + DoesNotEraseRestPending
  удалён (дубль carry-forward) + ReturnsRestPending → ReturnsRestWindowOpen
  + легаси-JSON → false. (LOW) П11.1(б) — удаление приватного хелпера
  `BackupKey` (:350, потребители — удаляемые блоки). (LOW) шаг 14.6 —
  формулировка ← t05-маркеров снята: t06 сохраняет маркер до своей
  реализации.
- **Рев. 7 (подтверждена ревью):** маркер окна — переносимый флаг
  `rest_window_open` (открытие/закрытие строго записями rotate-etcd-
  committed/done; carry-forward всеми писателями; предикат/гварды/takeover —
  от флага; failure-фазы на предикат не влияют); тесты
  WorkJournal/Rotator/RestoreProcess-сид; LOW-1/LOW-2; grep-гейт шага 14.
- Шаги 1–10/12/13 — выполнены в ветке (git-лог), статусные строки.
- Имена/сигнатуры — из фактического кода/тестов ветки (ротатор 534 строки;
  WorkJournal 119–120/161–175; гварды надзора 463/760; тестовые имена и
  строки сверены grep'ом по фактическим файлам).
- Развилок нет: состав txn/флаг/порядок/имена фаз/границы тикета — канон
  arch/14 §3.3+§5 I, spec §8-19a/19b дословно; судьба каждого теста
  перечислена явно (удалить/переименовать/переписать с точными ассертами).
- История ревизий: рев. 3 — TLS-пути + content-проба; рев. 4 — безусловный
  выход/перенос restoring/txn ValueEqual; рев. 5 — generic CallAsync<T>/
  переменная delivered; рев. 6 — ETCD-FIRST v1; рев. 7 — флаг окна; все
  реализованы в ветке либо поглощены рев. 8.
