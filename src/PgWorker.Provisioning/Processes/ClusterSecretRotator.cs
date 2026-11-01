using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Parsing;
using Shared.Etcd.Client;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster секретов по заявке /pgworker/rotations/&lt;C&gt; —
/// порядок ETCD-FIRST (arch/14 §5 I/§3.3): R0 заявка/битая-мусор + takeover
/// открытого окна по флагу rest_window_open → гвард «тихого окна» (все
/// канонические ноды ready — до открытия) → R1 ensure кредов (OLD —
/// compare-база) → R2 txn-коммит NEW-четвёрки (app/bucket_admin/bucket_mover/
/// rest + dsn-перепись + del заявки) В НАЧАЛЕ окна (фаза
/// rotate-etcd-committed, флаг true) → R3 rolling rest-пары каналом агента
/// по NEW из etcd (реплики вперёд, лидер последним, ≤1 нода/тик; в окне 28P01
/// — транзиент) → R4 финальный ALTER app/bucket_admin/bucket_mover на мастере
/// каждого dsn-шарда (rotate-waiting-master/&lt;X&gt; при недоступном мастере,
/// флаг переносится) → снапшот P12 → done (флаг false). backup_exec — вне
/// тикета (spec §8-19b). Имя journal-op сохранено (rotate-app-password) —
/// совместимость журналов (образец RotationRole.Phase, arch/16 §5 H). Вызывается
/// только держателем клэйма &lt;C&gt;. Ротация при активном переезде: mover-DSN
/// строится из свежего снапшота на тик — оборванная ротацией фаза переезда
/// возобновляется с journal-фазы уже с новыми кредами (etcd).
/// </summary>
public sealed partial class ClusterSecretRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    IClusterSecretEnsurer appSecret,
    INodeBootstrapDelivery bootstrap,
    PgtuneInputsFactory pgtune,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate-app-password";

    /// <summary>MR-#2 (spec §8-24): порог тихих тиков rolling БЕЗ прогресса до
    /// R4-ALTER (канон N=3 — образец триады оконных таймаутов §5 I).</summary>
    internal const int QuietTicksThreshold = 3;

    // Имя mover-роли фиксировано (arch/14 §4); user-ключа mover нет.
    private const string MoverRole = "bucket_mover";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §3.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim is not ours (or lost) — mutations forbidden"));

        // R0: заявка (цикл префикс /pgworker/ не читает — читаем ключ сами).
        // Раннего no-op-выхода при отсутствии заявки НЕТ (arch/14 §5 I
        // ETCD-FIRST): после R2-txn заявка снята, а окно продолжает жить —
        // тики R3/R4 работают БЕЗ заявки; no-op — в ветке !windowOpen ниже.
        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result<ProcessOutcome>.Failed(ticket.Error!);

        // Битая заявка (не-JSON/без requested_unix) — мусор: удалить с journal-записью.
        if (ticket.Value is not null && !IsWellFormed(ticket.Value.Value))
        {
            var cleaned = await DeleteAsync(TicketKey(cluster), ct);
            if (!cleaned.IsSuccess)
                return Result<ProcessOutcome>.Failed(cleaned.Error!);
            await journal.WritePhaseAsync(
                cluster, Op, "malformed-ticket-removed", claims.InstanceId, ticket.Value.Value, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

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

            // Гвард «тихого окна» (ДО записи started): до ОТКРЫТИЯ окна все
            // канонические ноды ready (ETCD-FIRST: после R2 etcd несёт NEW —
            // initializing-нода держит 28P01 до R4; окно не открываем поверх
            // стартов).
            var addressesGuard = await ReadPortAllocAsync(cluster, ct);
            if (!addressesGuard.IsSuccess)
                return await FailAsync(cluster, addressesGuard.Error!, "portalloc", ct);
            var restoringGuard = await ReadRestoringShardsAsync(cluster, ct);
            if (!restoringGuard.IsSuccess)
                return await FailAsync(cluster, restoringGuard.Error!, "backups-read", ct);
            var readiness = await AllNodesReadyAsync(
                cluster, snap, addressesGuard.Value, restoringGuard.Value, ct);
            if (!readiness.IsSuccess)
                return await FailAsync(cluster, readiness.Error!, "nodes-status-read", ct);
            if (!readiness.Value)
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
            // MR-#3 (spec §8-25): флаг окна в ТОЙ ЖЕ txn — NEW×4 + dsn +
            // del заявки + флаг неразделимы (коммит = окно открыто, атомарно).
            var workPayload = System.Text.Json.JsonSerializer.Serialize(
                new WorkJournalWindowPayload(
                    "rotate-app-password", "rotate-etcd-committed", claims.InstanceId,
                    DateTimeOffset.UtcNow.ToUnixTimeSeconds(), RestWindowOpen: true),
                new System.Text.Json.JsonSerializerOptions
                {
                    DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                });
            var ops = new List<TxnOp>
            {
                new TxnOp.Put(PasswordKey(cluster), newAppPassword, null),
                new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
                new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
                new TxnOp.Put(RestKey(cluster), newRestPassword, null),
                new TxnOp.Put(WorkKey(cluster), workPayload, null),
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

        // R3: rolling rest-пары каналом агента БЕЗ пересоздания (≤1 нода/тик;
        // реплики вперёд, лидер последним; restore/TO_REMOVE-шарды и
        // усыновлённые ноды гейт пропускает — их ноды создадутся с NEW после
        // закрытия окна — NEW в etcd с R2). Прогресс — немутирующая проба
        // IsOnRevisionAsync(newRest): content хранимого ключа == рендер под
        // NEW + ready + applied_revision == mod_revision. Применение у агента
        // фоновое — Delivered = команда принята, прогресс — проба тиками;
        // исход доставки завершает тик.
        var restoring = await ReadRestoringShardsAsync(cluster, ct);
        if (!restoring.IsSuccess)
            return await FailAsync(cluster, restoring.Error!, "backups-read", ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            if (restoring.Value.Contains(shard.Name))
                continue; // демонтажем владеет restore-процесс
            var scopeKvs = await RangeAsync($"/service/{cluster}-{shard.Name}/", ct);
            if (!scopeKvs.IsSuccess)
                return await FailAsync(cluster, scopeKvs.Error!, $"scope/{shard.Name}", ct);
            var leader = ClusterSnapshotParser.ParseService(scopeKvs.Value)
                .FirstOrDefault()?.LeaderName;
            var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
            var tuning = pgtune.Create(resources);
            var ordered = shard.Nodes
                .Where(n => n.State is not (NodeState.Quarantined or NodeState.Removing))
                .OrderBy(n => n.Name == leader ? 1 : 0); // реплики вперёд, лидер последним
            foreach (var node in ordered)
            {
                if (!addressMap.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                    || addr.Object is not null)
                    continue; // записи нет / усыновлённая (R9) — гейт пропускает
                var topology = new ShardTopology(cluster, shard.Name, $"{cluster}-{shard.Name}",
                    addressMap.Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
                        .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));
                if (await bootstrap.IsOnRevisionAsync(
                        topology, node.Name, addr, tuning, pgtune.ExcludeParams,
                        snap.Config.SyncStrict, newRest, ct))
                    continue; // применила NEW — следующая нода

                // ОДНА доставка на тик (spec §3.7.4): доставка завершает тик В
                // ЛЮБОМ случае — применение у агента фоновое; следующая нода —
                // следующим тиком. Finish фиксирует фазу
                // rotate-rest-rolling/<X>/<n>; исход диагностируется следующим
                // тиком (проба IsOnRevisionAsync: применила — идём дальше).
                await bootstrap.EnsureAsync(
                    topology, node.Name, addr, secrets, tuning, pgtune.ExcludeParams,
                    snap.Config.SyncStrict, newRest, isToolStart: false, ct);
                // MR-#2: delivery = прогресс rolling — quiet-счётчик обнулён.
                return await Finish(cluster,
                    $"rotate-rest-rolling/{shard.Name}/{node.Name}", ProcessOutcome.Done, ct,
                    restWindowOpen: true, rotateQuietTicks: 0);
            }
        }

        // R4 (финальный ALTER — закрытие окна, «etcd — источник, PG следует»):
        // NEW читаются из etcd (не локальные переменные R2 — takeover).
        // MR-#2 (spec §8-24) лайвлок-гвард R4: вход в ALTER — полное схождение
        // (все канонические ноды IsOnRevision) ИЛИ (3+ тихих тика подряд БЕЗ
        // прогресса rolling — любой delivery обнуляет счётчик) И неприменившие
        // ноды — ТОЛЬКО initializing/not_initialized (меньшинство; ready-нода,
        // не применившая NEW — refuse, НЕ меньшинство: её bootstrap-ключ не
        // рендерится под NEW). При ALTER-входе ОКНО ОСТАЁТСЯ ОТКРЫТЫМ
        // (флаг/гварды действуют), R3 продолжается до схождения; ALTER
        // идемпотентен — повторяется тиком после схождения безвредно.
        var allApplied = true;
        var unappliedNotReady = true; // все неприменившие — не ready
        var statusesRange = await RangeAsync($"/pgworker/nodes/{cluster}/", ct);
        var statuses = new Dictionary<string, string>(StringComparer.Ordinal);
        if (statusesRange.IsSuccess)
            foreach (var kv in statusesRange.Value)
            {
                var seg = kv.Key.Split('/');
                if (seg.Length == 7 && seg[6] == "status")
                    statuses[$"{seg[4]}/{seg[5]}"] = PgNodeStatusFact.Parse(kv.Value)?.State ?? "";
            }

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            var scopeKvs = await RangeAsync($"/service/{cluster}-{shard.Name}/", ct);
            var leader = scopeKvs.IsSuccess
                ? ClusterSnapshotParser.ParseService(scopeKvs.Value).FirstOrDefault()?.LeaderName
                : null;
            foreach (var node in shard.Nodes
                     .Where(n => n.State is not (NodeState.Quarantined or NodeState.Removing))
                     .OrderBy(n => n.Name == leader ? 1 : 0))
            {
                if (!addressMap.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                    || addr.Object is not null)
                    continue; // усыновлённая — вне rolling (R3-гейт)
                var topology = new ShardTopology(cluster, shard.Name, $"{cluster}-{shard.Name}",
                    addressMap.Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
                        .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));
                if (await bootstrap.IsOnRevisionAsync(
                        topology, node.Name, addr, pgtune.Create(
                            await ReadShardResourcesAsync(cluster, shard.Name, ct)),
                        pgtune.ExcludeParams, snap.Config.SyncStrict, newRest, ct))
                    continue; // применила NEW

                allApplied = false;
                // Состояние ноды:Initializing/not_initialized/нет ключа —
                // «не ready» (меньшинство-кандидат); ready без NEW — refuse.
                var nodeState = statuses.TryGetValue($"{shard.Name}/{node.Name}", out var st)
                    ? st
                    : "unreachable";
                if (nodeState != "initializing" && nodeState != "not_initialized")
                    unappliedNotReady = false;
            }
        }

        // QuietTicks: carry в WorkState (MR-#2): прогресс (delivery) обнуляет.
        var quietTicks = (await journal.ReadAsync(cluster, ct)).Value?.RotateQuietTicks ?? 0;
        if (!allApplied)
        {
            var quietAllowed = quietTicks >= QuietTicksThreshold && unappliedNotReady;
            if (!quietAllowed)
            {
                await journal.WritePhaseAsync(cluster, Op,
                    quietTicks >= QuietTicksThreshold
                        ? "rotate-waiting-ready-refusal"
                        : "rotate-waiting-unapplied", claims.InstanceId, null, ct,
                    restWindowOpen: true, rotateQuietTicks: quietTicks + 1);
                return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
            }
            // ALTER выполняется ниже; окно остаётся открытым (флаг не гасится).
        }
        await journal.WritePhaseAsync(cluster, Op, "rotate-etcd-quiet-passed",
            claims.InstanceId, null, ct, restWindowOpen: true, rotateQuietTicks: 0);

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
        return await Finish(cluster, "done", ProcessOutcome.Done, ct,
            restWindowOpen: false, rotateQuietTicks: 0);
    }

    // Замена password= в conninfo dsn-ключа (пароль bucket_admin внутри dsn).
    // Пароли AppSecretGenerator — [A-Za-z0-9], экранирования в conninfo не требуют.
    [GeneratedRegex(@"(^| )password=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    private static string TicketKey(string cluster) => $"/pgworker/rotations/{cluster}";

    private static string WorkKey(string cluster) => $"/pgworker/work/{cluster}";

    /// <summary>Тело work-ключа для txn-put R2 (MR-#3, spec §8-25): JSON
    /// формата WorkState (op/phase/instance/updated_unix + rest_window_open)
    /// — совместим с десериализацией WorkState/чтением панели.</summary>
    private sealed record WorkJournalWindowPayload(
        [property: System.Text.Json.Serialization.JsonPropertyName("op")] string Op,
        [property: System.Text.Json.Serialization.JsonPropertyName("phase")] string Phase,
        [property: System.Text.Json.Serialization.JsonPropertyName("instance")] string Instance,
        [property: System.Text.Json.Serialization.JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: System.Text.Json.Serialization.JsonPropertyName("rest_window_open")] bool RestWindowOpen);

    // Гвард «тихого окна» (arch/14 §5 I): все канонические ноды кластера
    // должны иметь статусную машину ready ДО мутаций ротации (R4 ALTER app
    // меняет pg_authid — initializing-нода с etcd-кредами теряет вход через
    // doorman навсегда, пока окно не закроется). Триада исходов: Success(true)
    // — все ready, Success(false) — есть неготовые (waiting), Failed — отказ
    // чтения статусных ключей (различим, не маскируется под waiting). Один
    // range статусных ключей кластера; гварды надзора повторены:
    // QUARANTINED/REMOVING, усыновлённые (object), ноды без записи
    // portalloc — вне проверки.
    private async Task<Result<bool>> AllNodesReadyAsync(
        string cluster, ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses,
        IReadOnlySet<string> restoring, CancellationToken ct)
    {
        var range = await RangeAsync($"/pgworker/nodes/{cluster}/", ct);
        if (!range.IsSuccess)
            return Result<bool>.Failed(range.Error!);
        var statuses = new Dictionary<string, PgNodeStatusFact>();
        foreach (var kv in range.Value)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 7 || segments[6] != "status")
                continue;
            var fact = PgNodeStatusFact.Parse(kv.Value);
            if (fact is not null)
                statuses[$"{segments[4]}/{segments[5]}"] = fact;
        }

        // MR-#13: restore-шарды исключены СИНХРОННО с R3-гейтом (их ноды вне
        // rolling) — гвард не ждёт их готовности; в противном случае
        // restore-шард с погашенными машинами вечно держал бы окно закрытым.
        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove)
                     .Where(s => !restoring.Contains(s.Name)))
        {
            foreach (var node in shard.Nodes)
            {
                if (node.State is NodeState.Quarantined or NodeState.Removing)
                    continue;
                if (!addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                    || addr.Object is not null)
                    continue;
                if (!statuses.TryGetValue($"{shard.Name}/{node.Name}", out var status)
                    || status.State != "ready")
                    return Result<bool>.Success(false); // нода ещё стартует/не готова
            }
        }

        return Result<bool>.Success(true);
    }

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    private static string MoverKey(string cluster) => $"/clusters/{cluster}/mover_password";

    private static string BucketAdminPasswordKey(string cluster) => $"/clusters/{cluster}/bucket_admin_password";

    private static string RestKey(string cluster) => $"/clusters/{cluster}/rest_password";

    private static string DsnKey(string cluster, string shard) => $"/clusters/{cluster}/shards/{shard}/dsn";

    // restore-шарды кластера (PLANNED/RUNNING/REJOINING) — те же факты, что
    // гварды надзора (§3.4): демонтажем владеет restore-процесс, окно
    // REST-ротации такие шарды не удержывают (ноды создадутся с новой парой).
    private async Task<Result<HashSet<string>>> ReadRestoringShardsAsync(string cluster, CancellationToken ct)
    {
        var range = await RangeAsync("/pgworker/backups/", ct);
        if (!range.IsSuccess)
            return Result<HashSet<string>>.Failed(range.Error!);
        var parsed = BackupsParser.Parse(range.Value, out _);
        if (!parsed.IsSuccess)
            return Result<HashSet<string>>.Failed(parsed.Error!);
        var restoring = new HashSet<string>();
        if (parsed.Value.FirstOrDefault(b => b.Cluster == cluster) is { } mine)
            foreach (var (shard, status) in mine.Shards)
                if (status.Restores.Any(r => r.State
                        is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining))
                    restoring.Add(shard);
        return Result<HashSet<string>>.Success(restoring);
    }

    // Валидная заявка: JSON с числовым requested_unix (панель §9.8 п.3).
    private static bool IsWellFormed(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.TryGetProperty("requested_unix", out var unix)
                   && unix.ValueKind == JsonValueKind.Number;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Мастер шарда: точное имя ноды из master-ключа → doorman-порт (уникален
    // per-node) → HA-лидер контура /service/<C>-<X>/leader → Patroni REST
    // (паттерн ShardEndpoints.ResolveMasterAsync). Совпадение master-ключа ПО
    // ХОСТУ недопустимо: вырожденное значение host:0 недискриминантно (S4;
    // в штатном режиме невозможно — пулер обязателен, guard ниже — защита),
    // и все ноды одного хоста равнозначны — резолв вернул бы произвольную
    // (первую/случайный порядок portalloc) ноду, ALTER ROLE на реплике падает
    // 25006 (read-only) — ротация зацикливалась в фазе started (факт стенда).
    private async Task<NodeAddress?> ResolveMasterAsync(
        string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var shardNodes = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value);

        if (!string.IsNullOrWhiteSpace(shard.Master))
        {
            var parts = shard.Master.Split(':');
            if (shardNodes.TryGetValue(parts[0], out var byName))
                return byName;
            if (parts.Length == 2 && int.TryParse(parts[1], out var doorman) && doorman > 0)
            {
                var byDoormanPort = shardNodes.FirstOrDefault(p => p.Value.Ports.Doorman == doorman);
                if (byDoormanPort.Value is not null)
                    return byDoormanPort.Value;
            }
        }

        // HA-лидер контура (окно failover с протухшим master-ключом): значение
        // leader-ключа — ИМЯ лидера (arch/14 §5 F, шаг 2 резолва; голое имя
        // Patroni, JSON-обёртка демо-сидов — легаси; обе формы — LeaderKeyName).
        var leader = await GetAsync($"/service/{cluster}-{shard.Name}/leader", ct);
        if (leader.IsSuccess && Endpoints.LeaderKeyName.From(leader.Value?.Value) is { Length: > 0 } leaderName
            && shardNodes.TryGetValue(leaderName, out var leaderNode))
            return leaderNode;

        foreach (var node in shardNodes)
        {
            var members = await probe.GetClusterAsync(node.Value, ct);
            if (!members.IsSuccess)
                continue;
            // Patroni 3.x в /cluster называет мастера "leader" (legacy: "master").
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && shardNodes.TryGetValue(master.Name, out var addr))
                return addr;
        }

        return null;
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        return Portalloc.Parse(cluster, kv.Value);
    }

    private async Task<Result<ProcessOutcome>> Finish(
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct,
        bool? restWindowOpen = null, int? rotateQuietTicks = null)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct,
            restWindowOpen: restWindowOpen, rotateQuietTicks: rotateQuietTicks);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct, bool? restWindowOpen = null)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct,
            restWindowOpen: restWindowOpen);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает (образец AddShardProcess).
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, prefix, ct));

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    // Лимиты шарда от заявки request_* (копия NodeSupervisor.ReadShardResourcesAsync
    // — у ротатора своей не было; прецедент упрощённой копии RestoreProcess).
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }
}
