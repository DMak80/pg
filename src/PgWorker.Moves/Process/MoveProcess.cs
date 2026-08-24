using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Moves;

/// <summary>Фазы статус-ключа переезда (значения — как в скриптах move-bucket.sh).</summary>
public static class MovePhases
{
    /// <summary>Снапшот-точка «после начала» ещё не снята — тик повторит (C#-фаза).</summary>
    public const string WaitingSnapshot = "waiting-snapshot";

    public const string Ddl = "ddl";
    public const string PubSub = "pubsub";
    public const string CopyWait = "copy-wait";
    public const string CutoverWait = "cutover-wait";
}

/// <summary>
/// MoveProcess — тиковая машина состояний планового переезда бакета M0–M6 по заявкам
/// /pgworker/moves/&lt;C&gt;/bucket_&lt;i&gt; (t01 задача 11, spec §6.1, arch/14 §5 F): тик
/// идемпотентен (каждый шаг перепроверяет факт), мутации — только под живым клэймом
/// кластера; одновременно обрабатывается старейшая заявка (Д2). Отказы M0:
/// permanent (del заявки + журнал rejected) — дефект заявки/факт-несоответствие;
/// transient (заявка жива, work.last_error) — недоступность, ретраи тиками.
/// M4–M6 (задача 14): cutover с классификацией исходов, post-flip, done.
/// Rollback/finalize/abort — задачи 15–16.
/// </summary>
public sealed class MoveProcess(
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    IMoveSqlExecutor sql,
    MoveDdl ddl,
    IClusterDriver driver,
    ShardEndpoints shards,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    MovesRuntimeOptions options,
    TimeProvider clock,
    ILogger<MoveProcess>? logger = null,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private readonly MoveRequestsStore requests = new(etcd, etcdEndpoints);
    private readonly MoveStatusStore status = new(etcd, etcdEndpoints);

    // Cutover-блок M4/rollback (задача 12/14) — свой стор статуса поверх того же etcd.
    private readonly CutoverSequence cutover = new(
        sql, new MoveStatusStore(etcd, etcdEndpoints), secrets);

    // Читаются фазами M1–M6 (задачи 13–16): DDL-перенос, exec-транспорт, опции ожиданий.
    private readonly MoveDdl ddl = ddl;
    private readonly IClusterDriver driver = driver;
    private readonly MovesRuntimeOptions options = options;

    // Последняя записанная в лог готовность подписки (cluster/bucket → «N/N») —
    // лог только на изменение; кластеры обрабатываются параллельно.
    private readonly ConcurrentDictionary<string, string> _lastReady = [];

    /// <summary>Один тик: клэйм-гвард → старейшая заявка кластера → диспетчер по op.</summary>
    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"moves {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var oldest = await requests.OldestAsync(cluster, ct);
        if (!oldest.IsSuccess)
            return Result<ProcessOutcome>.Failed(oldest.Error!);

        // Битые заявки молча висеть не должны (ревью №2, план Task 3 Step 3):
        // парсинг их уже пропустил — громко называем ключ оператору.
        foreach (var parseError in oldest.Value.ParseErrors)
            logger?.LogWarning("moves {cluster}: {error} — исправь или удали ключ", cluster, parseError);

        if (oldest.Value.Request is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // заявок нет

        var (bucket, request) = oldest.Value.Request.Value;
        return request.Op switch
        {
            MoveOp.Move => await RunMoveAsync(snap, bucket, request, ct),
            MoveOp.Rollback => await RunRollbackAsync(snap, bucket, request, ct),
            MoveOp.Finalize => await RunFinalizeAsync(snap, bucket, request, ct),
            MoveOp.Abort => await RunAbortAsync(snap, bucket, request, ct),
            _ => Result<ProcessOutcome>.Failed(new ApplicationException($"неизвестная операция заявки: {request.Op}")),
        };
    }

    // ── M0: валидация заявки + префлайт (spec §6.1 M0; перенос шага 0 move-bucket.sh) ──

    private async Task<Result<ProcessOutcome>> RunMoveAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Валидации заявки — по порядку; каждая: permanent / transient / продолжение.
        if (snap.Config.State != ClusterState.Active)
            return await RejectAsync(cluster, bucket,
                $"кластер не Active ({snap.Config.State}) — заявки переездов обрабатываются только в Active", ct);
        if (!MoveNames.ValidateIdentifier(bucket))
            return await RejectAsync(cluster, bucket, $"недопустимое имя бакета '{bucket}'", ct);

        var owner = snap.Routing.FirstOrDefault(r => $"bucket_{r.Id}" == bucket)?.Owner;
        if (owner is null)
            return await RejectAsync(cluster, bucket,
                $"нет {MoveNames.RoutingKey(cluster, bucket)} — владелец неизвестен, переезд невозможен (восстанови контрол-плейн, P12)", ct);

        if (request.To is not { } to || !MoveNames.ValidateIdentifier(to))
            return await RejectAsync(cluster, bucket, "заявка без валидной цели 'to'", ct);
        if (to == owner)
            return await RejectAsync(cluster, bucket, $"бакет уже на '{to}'", ct);

        var srcShard = snap.Shards.FirstOrDefault(s => s.Name == owner);
        var dstShard = snap.Shards.FirstOrDefault(s => s.Name == to);
        if (dstShard?.Dsn is null)
            return await RejectAsync(cluster, bucket, $"шард-приёмник '{to}' не зарегистрирован (нет dsn-ключа)", ct);
        if (srcShard?.Dsn is null)
            return await RejectAsync(cluster, bucket, $"шард-источник '{owner}' не зарегистрирован (нет dsn-ключа)", ct);

        // Статус-ключ: новый переезд / resume (started_unix наследуется) / конфликт.
        var existing = await status.GetAsync(cluster, bucket, ct);
        if (!existing.IsSuccess)
            return await FailTransientAsync(cluster, existing.Error!, ct);
        long startedUnix = Now();
        var snapshotRequired = true;
        var reachedCutover = false;
        if (existing.Value is { } prev)
        {
            switch (prev.State)
            {
                case MoveStates.Syncing or MoveStates.Frozen when prev.Target == to:
                    startedUnix = prev.StartedUnix; // resume: возраст переезда сохраняется
                    snapshotRequired = prev.Phase == MovePhases.WaitingSnapshot;
                    // M3 пройден (или прошлый cutover сорвался) — тик идёт сразу в M4:
                    // повтор cutover с начала безопасен (freeze идемпотентен, spec §6.2).
                    reachedCutover = ReachedCutover(prev.State, prev.Phase);
                    break;
                case MoveStates.Aborting:
                    return await RejectAsync(cluster, bucket,
                        "уборка прерванного переезда не закончена (state=ABORTING) — сначала заверши abort", ct);
                default:
                    return await RejectAsync(cluster, bucket, prev.State is MoveStates.Syncing or MoveStates.Frozen
                        ? $"переезд уже идёт на '{prev.Target}', а запрошен '{to}'"
                        : $"неожиданное состояние статус-ключа: {prev.State}", ct);
            }
        }

        // Адреса мастеров → admin-DSN обоих шардов (SQL — к мастерам, P2).
        var dsns = await ResolveShardDsnsAsync(snap, srcShard, dstShard, ct);
        if (!dsns.IsSuccess)
            return await FailTransientAsync(cluster, dsns.Error!, ct);
        var (srcDsn, dstDsn) = dsns.Value;

        // Схема бакета есть на источнике (ревью №5): нет — переезжать нечего.
        var schemaSrc = await sql.ScalarAsync(srcDsn, MoveSql.SchemaExists(bucket), ct);
        if (!schemaSrc.IsSuccess)
            return await FailTransientAsync(cluster, schemaSrc.Error!, ct);
        if (ToBool(schemaSrc.Value) != true)
            return await RejectAsync(cluster, bucket, $"схемы '{bucket}' нет на '{owner}'?! (владелец без схемы бакета)", ct);

        // SQL-префлайт источника: недоступность → transient, факт → permanent.
        var wal = await sql.ScalarAsync(srcDsn, MoveSql.WalLevel(), ct);
        if (!wal.IsSuccess)
            return await FailTransientAsync(cluster, wal.Error!, ct);
        if (ToText(wal.Value) != "logical")
            return await RejectAsync(cluster, bucket,
                $"wal_level='{ToText(wal.Value)}' на '{owner}', нужно 'logical' (рестарт кластера)", ct);

        var maxSlots = await sql.ScalarAsync(srcDsn, MoveSql.MaxSlots(), ct);
        if (!maxSlots.IsSuccess)
            return await FailTransientAsync(cluster, maxSlots.Error!, ct);
        var usedSlots = await sql.ScalarAsync(srcDsn, MoveSql.UsedSlots(), ct);
        if (!usedSlots.IsSuccess)
            return await FailTransientAsync(cluster, usedSlots.Error!, ct);
        if (ToLong(usedSlots.Value) >= ToLong(maxSlots.Value))
            return await RejectAsync(cluster, bucket,
                $"replication-слоты на '{owner}' кончились ({ToLong(usedSlots.Value)}/{ToLong(maxSlots.Value)})", ct);

        var maxSenders = await sql.ScalarAsync(srcDsn, MoveSql.MaxWalSenders(), ct);
        if (!maxSenders.IsSuccess)
            return await FailTransientAsync(cluster, maxSenders.Error!, ct);
        var usedSenders = await sql.ScalarAsync(srcDsn, MoveSql.UsedWalSenders(), ct);
        if (!usedSenders.IsSuccess)
            return await FailTransientAsync(cluster, usedSenders.Error!, ct);
        if (ToLong(usedSenders.Value) >= ToLong(maxSenders.Value))
            return await RejectAsync(cluster, bucket,
                $"walsender'ы на '{owner}' кончились ({ToLong(usedSenders.Value)}/{ToLong(maxSenders.Value)})", ct);

        // P4-префлайт: lost-слоты — только предупреждение в журнал (прошлая подписка умерла).
        var lost = await sql.ScalarAsync(srcDsn, MoveSql.LostSlots(), ct);
        if (lost.IsSuccess && ToLong(lost.Value) > 0)
            await journal.WritePhaseAsync(cluster, "move", "preflight", claims.InstanceId,
                $"warning: на '{owner}' {ToLong(lost.Value)} слотов(а) с wal_status='lost' (P4) — прибери: abort/finalize", ct);

        // Пробы mover-роли ПО MOVER-DSN источника (ревью №2): доступность + REPLICATION.
        var moverDsn = ShardEndpoints.MoverNpgsqlDsn(srcShard.Dsn, secrets);
        var probe = await sql.ScalarAsync(moverDsn, "SELECT 1", ct);
        if (!probe.IsSuccess)
            return await FailTransientAsync(cluster, probe.Error!, ct);
        var roleOk = await sql.ScalarAsync(moverDsn, MoveSql.MoverRoleOk(), ct);
        if (!roleOk.IsSuccess)
            return await FailTransientAsync(cluster, roleOk.Error!, ct);
        if (ToBool(roleOk.Value) != true)
            return await RejectAsync(cluster, bucket,
                $"mover-роль на '{owner}' без атрибута REPLICATION — подписка не сможет создаться", ct);

        // P8: sync-standby у мастера приёмника (remote_apply без него вырожден).
        var syncNames = await sql.ScalarAsync(dstDsn, MoveSql.SyncStandbyNames(), ct);
        if (!syncNames.IsSuccess)
            return await FailTransientAsync(cluster, syncNames.Error!, ct);
        if (string.IsNullOrWhiteSpace(ToText(syncNames.Value)))
            return await RejectAsync(cluster, bucket,
                $"synchronous_standby_names на '{to}' пуст — remote_apply подписки вырожден (P8)", ct);
        var syncCount = await sql.ScalarAsync(dstDsn, MoveSql.SyncStandbyCount(), ct);
        if (!syncCount.IsSuccess)
            return await FailTransientAsync(cluster, syncCount.Error!, ct);
        if (ToLong(syncCount.Value) < 1)
            return await RejectAsync(cluster, bucket,
                $"у мастера '{to}' нет живого sync/quorum-standby (P8) — remote_apply вырожден", ct);

        // Приёмник: схема без подписки — только resume и только ПУСТАЯ; остатки _rb — отказ.
        var subDst = await sql.ScalarAsync(dstDsn, MoveSql.SubExists(MoveNames.Sub(bucket)), ct);
        if (!subDst.IsSuccess)
            return await FailTransientAsync(cluster, subDst.Error!, ct);
        var schemaDst = await sql.ScalarAsync(dstDsn, MoveSql.SchemaExists(bucket), ct);
        if (!schemaDst.IsSuccess)
            return await FailTransientAsync(cluster, schemaDst.Error!, ct);
        if (ToBool(subDst.Value) != true && ToBool(schemaDst.Value) == true)
        {
            if (!request.Resume)
                return await RejectAsync(cluster, bucket,
                    $"схема '{bucket}' уже есть на '{to}' без подписки (остаток сорванного запуска?) — resume=true или DROP SCHEMA", ct);
            // resume допустим только для ПУСТОЙ схемы: copy_data=true в непустую даст дубликаты.
            var gen = await sql.ScalarAsync(dstDsn, MoveSql.EmptySchemaCheckSqlGen(bucket), ct);
            if (!gen.IsSuccess)
                return await FailTransientAsync(cluster, gen.Error!, ct);
            if (ToText(gen.Value) is not { } generatedSql)
                return await FailTransientAsync(cluster,
                    new ApplicationException($"генератор проверки пустоты схемы на '{to}' вернул пустоту"), ct);
            var rows = await sql.ScalarAsync(dstDsn, generatedSql, ct);
            if (!rows.IsSuccess)
                return await FailTransientAsync(cluster, rows.Error!, ct);
            if (ToLong(rows.Value) != 0)
                return await RejectAsync(cluster, bucket,
                    $"схема на '{to}' не пустая ({ToLong(rows.Value)} строк) — остатки данных, а не сорванный DDL; DROP SCHEMA и запускай без resume", ct);
        }

        var pubRb = await sql.ScalarAsync(dstDsn, MoveSql.PubExists(MoveNames.PubRb(bucket)), ct);
        if (!pubRb.IsSuccess)
            return await FailTransientAsync(cluster, pubRb.Error!, ct);
        if (ToLong(pubRb.Value) != 0)
            return await RejectAsync(cluster, bucket,
                $"на '{to}' осталась {MoveNames.PubRb(bucket)} — сначала разберись (finalize?)", ct);
        var subRb = await sql.ScalarAsync(srcDsn, MoveSql.SubExists(MoveNames.SubRb(bucket)), ct);
        if (!subRb.IsSuccess)
            return await FailTransientAsync(cluster, subRb.Error!, ct);
        if (ToLong(subRb.Value) != 0)
            return await RejectAsync(cluster, bucket,
                $"на '{owner}' осталась {MoveNames.SubRb(bucket)} — сначала finalize прошлого переезда", ct);

        // SYNCING + обязательная снапшот-точка «после начала» (P12): фаза waiting-snapshot
        // пишется ДО снапшота (крах между ними оставляет «снапшот не взят» — тик повторит),
        // успешный переход в ddl — только после снятого снапшота.
        if (snapshotRequired)
        {
            var waiting = await PutStatusAsync(cluster, bucket, owner, to, startedUnix, MovePhases.WaitingSnapshot, ct);
            if (!waiting.IsSuccess)
                return await FailTransientAsync(cluster, waiting.Error!, ct);
            if (snapshot is not null)
            {
                var shot = await snapshot(ct);
                if (!shot.IsSuccess)
                    return await FailTransientAsync(cluster, shot.Error!, ct);
            }

            var put = await PutStatusAsync(cluster, bucket, owner, to, startedUnix, MovePhases.Ddl, ct);
            if (!put.IsSuccess)
                return await FailTransientAsync(cluster, put.Error!, ct);

            logger?.LogInformation("move {cluster}/{bucket}: SYNCING {owner} → {to} (префлайт пройден)",
                cluster, bucket, owner, to);

            // M0-конец: точка старта зафиксирована снапшотом — M1 продолжит следующим
            // тиком (повтор тика перепроверяет факты, идемпотентность spec §7).
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // Resume: снапшот-точка уже есть, статус ≥ ddl — фазы M1–M3 (или сразу M4,
        // если initial copy завершён) этим же тиком.
        return await RunMovePhasesAsync(
            snap, bucket, owner, to, srcShard, dstShard, srcDsn, dstDsn, startedUnix,
            subOnDst: ToBool(subDst.Value) == true,
            schemaOnDst: ToBool(schemaDst.Value) == true,
            reachedCutover, request, ct);
    }

    // ── M1–M3 (t01 задача 13, spec §6.1): DDL → pub/sub → copy-wait ──

    private async Task<Result<ProcessOutcome>> RunMovePhasesAsync(
        ClusterSnapshot snap, string bucket, string owner, string to,
        ShardSpec srcShard, ShardSpec dstShard, string srcDsn, string dstDsn, long startedUnix,
        bool subOnDst, bool schemaOnDst, bool reachedCutover, MoveRequest request,
        CancellationToken ct)
    {
        // M4–M6: initial copy завершён (cutover-wait) или прошлый cutover сорвался —
        // единый непрерывный блок этого тика (задача 14, spec §6.1 M4/§6.2).
        if (reachedCutover)
            return await RunCutoverAsync(
                snap, bucket, owner, to, srcShard, dstShard, srcDsn, dstDsn, startedUnix, request, ct);

        var cluster = snap.Config.Cluster;

        // M1: DDL-перенос — только когда схемы на приёмнике нет (resume пропускает).
        if (!subOnDst && !schemaOnDst)
        {
            // Имя ноды мастера для docker exec (pg_dump): формат master-ключа —
            // <host>:<doormanPort> (писатели — Patroni-callback/reconciler),
            // поэтому резолвим адрес через ShardEndpoints и ищем ноду шарда по
            // её PG-порту (уникален) — Split(':')[0] давал бы host, не имя (e2e t01).
            var addresses = await shards.ReadPortAllocAsync(cluster, ct);
            if (!addresses.IsSuccess)
                return await FailTransientAsync(cluster, addresses.Error!, ct);
            var srcMaster = await shards.ResolveMasterAsync(srcShard, addresses.Value, ct);
            if (!srcMaster.IsSuccess)
                return await FailTransientAsync(cluster, srcMaster.Error!, ct);
            if (srcMaster.Value is not { } master)
                return await FailTransientAsync(cluster, new ApplicationException(
                    $"мастер '{owner}' не определён — имя ноды для pg_dump неизвестно"), ct);
            var masterEntry = addresses.Value.FirstOrDefault(p =>
                p.Key.StartsWith($"{owner}/", StringComparison.Ordinal) && p.Value.Ports.Pg == master.Ports.Pg);
            if (masterEntry.Key is not { Length: > 0 } entry)
                return await FailTransientAsync(cluster, new ApplicationException(
                    $"мастер '{owner}' (pg:{master.Ports.Pg}) не найден среди нод шарда в portalloc"), ct);
            var dump = await ddl.DumpAsync(cluster, owner, entry.Split('/')[1], snap.Config.DbName, bucket, ct);
            if (!dump.IsSuccess)
                return await FailTransientAsync(cluster, dump.Error!, ct);
            var applied = await ddl.ApplyAsync(dstDsn, dump.Value, ct);
            if (!applied.IsSuccess)
                return await FailTransientAsync(cluster, applied.Error!, ct);
        }

        // Гранты app-роли на приёмнике — всегда (идемпотентный GRANT, grant_app_role).
        var granted = await ddl.GrantAppOnSchemaAsync(dstDsn, bucket, ct);
        if (!granted.IsSuccess)
            return await FailTransientAsync(cluster, granted.Error!, ct);

        // P5: двойная сверка инвентаря — мораторий DDL могли нарушить до переезда.
        var inventory = await ddl.InventoryMatchesAsync(srcDsn, dstDsn, bucket, ct);
        if (!inventory.IsSuccess)
            return await FailTransientAsync(cluster, inventory.Error!, ct);
        if (inventory.Value != true)
            return await RejectAsync(cluster, bucket,
                $"инвентарь '{bucket}' на '{owner}' и '{to}' расходится (inventory-mismatch) — мораторий DDL (P5) нарушен?", ct);

        // M2: pub/sub идемпотентно — pub на источнике, sub на приёмнике (P3/P8).
        var pub = await sql.ScalarAsync(srcDsn, MoveSql.PubExists(MoveNames.Pub(bucket)), ct);
        if (!pub.IsSuccess)
            return await FailTransientAsync(cluster, pub.Error!, ct);
        if (ToLong(pub.Value) == 0)
        {
            var createPub = await sql.ExecuteAsync(
                srcDsn, MoveSql.CreatePublication(MoveNames.Pub(bucket), bucket), ct);
            if (!createPub.IsSuccess)
                return await FailTransientAsync(cluster, createPub.Error!, ct);
        }

        if (!subOnDst)
        {
            // copy_data=true (initial copy), failover-флаг конфигурируем (PG17+, R1),
            // synchronous_commit=remote_apply — P8; CONNECTION — mover-роль источника.
            var createSub = await sql.ExecuteAsync(dstDsn,
                MoveSql.CreateSubscription(MoveNames.Sub(bucket),
                    ShardEndpoints.MoverConninfo(srcShard.Dsn!, secrets, options.AdvertisedPublisherHost),
                    MoveNames.Pub(bucket), copyData: true, failover: options.FailoverSlots), ct);
            if (!createSub.IsSuccess)
                return await FailTransientAsync(cluster, createSub.Error!, ct);
        }

        var pubsub = await PutStatusAsync(cluster, bucket, owner, to, startedUnix, MovePhases.PubSub, ct);
        if (!pubsub.IsSuccess)
            return await FailTransientAsync(cluster, pubsub.Error!, ct);

        // M3: copy-wait — каждый тик перезаписывает статус-ключ с обновлённым
        // updated_unix (Д12: по нему abort отличает живой mover, ревью №4); большой
        // бакет копируется часами — общего таймаута нет (спека §6.1 M3).
        var copyWait = await PutStatusAsync(cluster, bucket, owner, to, startedUnix, MovePhases.CopyWait, ct);
        if (!copyWait.IsSuccess)
            return await FailTransientAsync(cluster, copyWait.Error!, ct);

        var ready = await sql.ScalarAsync(dstDsn, MoveSql.SubSyncReady(MoveNames.Sub(bucket)), ct);
        if (!ready.IsSuccess)
            return await FailTransientAsync(cluster, new ApplicationException(
                $"приёмник '{to}' недоступен ({ready.Error!.Message}) — тики продолжаются, бюджеты недоступности: ConnFailBudgetSec={options.ConnFailBudgetSec}с"), ct);
        var readyText = ToText(ready.Value);
        if (readyText is null || !TryParseReady(readyText, out var done, out var total))
            return await FailTransientAsync(cluster, new ApplicationException(
                $"нечитаемая готовность подписки на '{to}': '{readyText}'"), ct);

        await LogCopyProgressAsync(cluster, bucket, srcDsn, readyText, ct);

        if (done == total)
        {
            // initial copy завершён: фаза cutover-wait — cutover продолжит следующим
            // тиком (единый непрерывный блок, M4 — задача 14).
            var cutoverWait = await PutStatusAsync(cluster, bucket, owner, to, startedUnix, MovePhases.CutoverWait, ct);
            if (!cutoverWait.IsSuccess)
                return await FailTransientAsync(cluster, cutoverWait.Error!, ct);
            logger?.LogInformation("move {cluster}/{bucket}: initial copy завершён — готов к cutover", cluster, bucket);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }

    // ── M4–M6 (t01 задача 14, spec §6.1/§6.2): cutover → post-flip → done ──

    private async Task<Result<ProcessOutcome>> RunCutoverAsync(
        ClusterSnapshot snap, string bucket, string owner, string to,
        ShardSpec srcShard, ShardSpec dstShard, string srcDsn, string dstDsn,
        long startedUnix, MoveRequest request, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // M4: cutover — единый непрерывный блок (слот подтверждения sub_<b> живёт на
        // источнике). Снапшот flip — best-effort: неудача пишется в журнал, flip не
        // отменяет (P12); снапшот-колбэк снимается внутри cutover сразу после flip.
        var postFlipErrors = new List<string>();
        var flip = await cutover.RunAsync(shards, snap,
            new CutoverContext(cluster, bucket, owner, to, MoveNames.Sub(bucket), MoveStates.Syncing),
            options, ct,
            snapshot is null ? null : async token =>
            {
                var shot = await snapshot(token);
                if (!shot.IsSuccess)
                    postFlipErrors.Add($"снапшот flip-{bucket}-{to} не снялся: {shot.Error!.Message} — сними вручную");
                return shot;
            });
        if (!flip.IsSuccess)
        {
            // Перманентные исходы (ревью №1): verify-failed (дефектная копия — «abort +
            // повторный move»; статус SYNCING/verify-failed оставлен: переезд живёт до
            // abort) и flip-conflict (заморозка ОСТАВЛЕНА cutover'ом — разбор вручную).
            if (flip.Error is CutoverPermanentException)
                return await RejectAsync(cluster, bucket, flip.Error.Message, ct);

            // Transient (freeze/lsn/catchup/sequences): статус уже записан cutover'ом,
            // заявка жива — ретраи тиками (повтор cutover с начала безопасен).
            return await FailTransientAsync(cluster, flip.Error!, ct);
        }

        // M5: прямая подписка срезается ДО обратной — иначе петля репликации; сбой
        // НЕ отменяет состоявшийся flip (work.last_error, остатки добьёт finalize).
        var drop = await sql.ExecuteAsync(dstDsn, MoveSql.DropSubscription(MoveNames.Sub(bucket)), ct);
        if (!drop.IsSuccess)
        {
            postFlipErrors.Add(
                $"не удалось срезать {MoveNames.Sub(bucket)} на '{to}' ({drop.Error!.Message}) — добьёт finalize");
        }
        else if (!request.SkipReverse)
        {
            var pubRb = await sql.ExecuteAsync(
                dstDsn, MoveSql.CreatePublication(MoveNames.PubRb(bucket), bucket), ct);
            if (pubRb.IsSuccess)
            {
                var subRb = await sql.ExecuteAsync(srcDsn,
                    MoveSql.CreateSubscription(MoveNames.SubRb(bucket),
                        ShardEndpoints.MoverConninfo(dstShard.Dsn!, secrets, options.AdvertisedPublisherHost),
                        MoveNames.PubRb(bucket), copyData: false, failover: options.FailoverSlots), ct);
                if (!subRb.IsSuccess)
                    postFlipErrors.Add(
                        $"не удалось поставить {MoveNames.SubRb(bucket)} на '{owner}' ({subRb.Error!.Message}) — rollback недоступен, поставь вручную");
            }
            else
            {
                postFlipErrors.Add(
                    $"не удалось создать {MoveNames.PubRb(bucket)} на '{to}' ({pubRb.Error!.Message}) — обратная подписка не ставится, rollback недоступен");
            }
        }

        // M6: del заявки + журнал done (снапшот уже снят cutover'ом; накопленные
        // post-flip ошибки не отменяют завершение — старый шард остаётся замороженным
        // до rollback/finalize, P1-призраки).
        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, "move", "done", claims.InstanceId,
            postFlipErrors.Count > 0 ? string.Join("; ", postFlipErrors) : null, ct);
        logger?.LogInformation("move {cluster}/{bucket}: переехал {owner} → {to} (старый шард заморожен до rollback/finalize)",
            cluster, bucket, owner, to);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Фаза «дошли до cutover»: initial copy завершён, заморозка прошлого тика или
    // fail-фаза cutover — M1–M3 уже пройдены, тик продолжается блоком M4–M6.
    private static bool ReachedCutover(string? state, string? phase) =>
        state == MoveStates.Frozen
        || phase is MovePhases.CutoverWait
            or CutoverPhases.FreezeFailed
            or CutoverPhases.LsnFailed
            or CutoverPhases.CatchupTimeout
            or CutoverPhases.SequencesFailed
            or CutoverPhases.VerifyFailed;

    // Лог готовности «N/N» + лаг слота — только на ИЗМЕНЕНИЕ готовности (образец —
    // шаг 3 move-bucket.sh, ревью №7); лог-высказывание, не контракт: unit не покрывается.
    private async Task LogCopyProgressAsync(
        string cluster, string bucket, string srcDsn, string ready, CancellationToken ct)
    {
        if (logger is null)
            return;

        var key = $"{cluster}/{bucket}";
        if (_lastReady.TryGetValue(key, out var prev) && prev == ready)
            return;

        _lastReady[key] = ready;
        var lag = await sql.ScalarAsync(srcDsn, MoveSql.SlotLag(MoveNames.Sub(bucket)), ct);
        logger.LogInformation("move {cluster}/{bucket}: таблицы готовы {ready}, лаг слота {lag} байт",
            cluster, bucket, ready, lag.IsSuccess ? ToLong(lag.Value).ToString() : "?");
    }

    // «ready/total» из pg_subscription_rel (sub_sync скрипта).
    private static bool TryParseReady(string text, out int ready, out int total)
    {
        ready = 0;
        total = 0;
        var parts = text.Split('/');
        return parts.Length == 2
               && int.TryParse(parts[0], out ready)
               && int.TryParse(parts[1], out total)
               && total >= 0;
    }

    // ── Rollback (t01 задача 15, spec §6.3): зеркальный cutover из ACTIVE ──

    private async Task<Result<ProcessOutcome>> RunRollbackAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
    {
        const string op = "rollback";
        var cluster = snap.Config.Cluster;

        if (snap.Config.State != ClusterState.Active)
            return await RejectAsync(cluster, bucket,
                $"кластер не Active ({snap.Config.State}) — откат недоступен", ct, op);
        if (!MoveNames.ValidateIdentifier(bucket))
            return await RejectAsync(cluster, bucket, $"недопустимое имя бакета '{bucket}'", ct, op);

        var owner = snap.Routing.FirstOrDefault(r => $"bucket_{r.Id}" == bucket)?.Owner;
        if (owner is null)
            return await RejectAsync(cluster, bucket,
                $"нет {MoveNames.RoutingKey(cluster, bucket)} — владелец неизвестен (восстанови контрол-плейн, P12)", ct, op);

        // Откат — только из ACTIVE: живой переезд сначала заверши (или отмени abort'ом).
        var existing = await status.GetAsync(cluster, bucket, ct);
        if (!existing.IsSuccess)
            return await FailTransientAsync(cluster, existing.Error!, ct, op);
        if (existing.Value is { } live)
            return await RejectAsync(cluster, bucket,
                $"откат возможен только из ACTIVE (сейчас state={live.State}) — сначала заверши переезд/abort", ct, op);

        // Обратная подписка — ровно на одном НЕ-владельце (поиск по всем шардам).
        var dsns = await ResolveAllDsnAsync(snap, ct);
        if (!dsns.IsSuccess)
            return await FailTransientAsync(cluster, dsns.Error!, ct, op);

        var onOwner = await sql.ScalarAsync(dsns.Value[owner], MoveSql.SubExists(MoveNames.SubRb(bucket)), ct);
        if (!onOwner.IsSuccess)
            return await FailTransientAsync(cluster, onOwner.Error!, ct, op);
        if (ToLong(onOwner.Value) > 0)
            return await RejectAsync(cluster, bucket,
                $"странно: {MoveNames.SubRb(bucket)} найдена на текущем владельце '{owner}' — разберись вручную", ct, op);

        string? reverseShard = null;
        foreach (var (shard, dsn) in dsns.Value)
        {
            if (shard == owner)
                continue;
            var subRb = await sql.ScalarAsync(dsn, MoveSql.SubExists(MoveNames.SubRb(bucket)), ct);
            if (!subRb.IsSuccess)
                return await FailTransientAsync(cluster, subRb.Error!, ct, op);
            if (ToLong(subRb.Value) > 0)
            {
                if (reverseShard is not null)
                    return await RejectAsync(cluster, bucket,
                        $"{MoveNames.SubRb(bucket)} найдена на нескольких шардах ('{reverseShard}', '{shard}') — разберись вручную", ct, op);
                reverseShard = shard;
            }
        }

        if (reverseShard is null)
            return await RejectAsync(cluster, bucket,
                $"обратная подписка {MoveNames.SubRb(bucket)} не найдена ни на одном шарде — откат только полным re-copy (§6 доки 11: abort + повторный move)", ct, op);

        // Зеркальный cutover: Cur=владелец, New=шард с sub_rb (слот sub_rb — на Cur,
        // создан обратной подпиской). Отказ ДО flip: cutover сам разморозит Cur и
        // УДАЛИТ статус-ключ (DropStatusOnFail: нет ключа = ACTIVE — эквивалент
        // скриптового state=ACTIVE без нестандартного значения state).
        var flip = await cutover.RunAsync(shards, snap,
            new CutoverContext(cluster, bucket, owner, reverseShard, MoveNames.SubRb(bucket),
                MoveStates.Syncing, DropStatusOnFail: true),
            options, ct,
            snapshot is null ? null : async token => await SnapshotBestEffortAsync(cluster, token));
        if (!flip.IsSuccess)
        {
            if (flip.Error is CutoverPermanentException)
                return await RejectAsync(cluster, bucket, flip.Error.Message, ct, op);
            return await FailTransientAsync(cluster, flip.Error!, ct, op);
        }

        // Пост-flip: срез sub_rb на вернувшемся владельце, DROP pub_rb на бывшем —
        // best-effort (остатки доберёт finalize); разморозка вернувшегося владельца
        // ОБЯЗАТЕЛЬНА (P1: владелец без записи не может считаться откатом).
        var postErrors = new List<string>();
        var subDrop = await SubscriptionDrop.DropAsync(sql, dsns.Value[reverseShard], MoveNames.SubRb(bucket), ct);
        if (!subDrop.IsSuccess)
            postErrors.Add($"не удалось срезать {MoveNames.SubRb(bucket)} на '{reverseShard}' ({subDrop.Error!.Message}) — добьёт finalize");
        var pubDrop = await sql.ExecuteAsync(dsns.Value[owner], MoveSql.DropPublication(MoveNames.PubRb(bucket)), ct);
        if (!pubDrop.IsSuccess)
            postErrors.Add($"не удалось удалить {MoveNames.PubRb(bucket)} на '{owner}' ({pubDrop.Error!.Message}) — удали вручную");

        var unfrozen = await sql.ExecuteAsync(
            dsns.Value[reverseShard], MoveSql.Unfreeze(bucket, MoveNames.AppRole), ct);
        if (!unfrozen.IsSuccess)
            return await FailTransientAsync(cluster, new ApplicationException(
                $"откат прошёл (routing='{reverseShard}'), но владельца не разморозить: {unfrozen.Error!.Message} — верни GRANT вручную (P1)"), ct, op);

        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, op, "done", claims.InstanceId,
            postErrors.Count > 0 ? string.Join("; ", postErrors) : null, ct);
        logger?.LogInformation("rollback {cluster}/{bucket}: вернулся {owner} → {shard} (остатки бывшего владельца — finalize)",
            cluster, bucket, owner, reverseShard);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Finalize (t01 задача 15, spec §6.4): уборка старого шарда после flip ──

    private async Task<Result<ProcessOutcome>> RunFinalizeAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
    {
        const string op = "finalize";
        var cluster = snap.Config.Cluster;

        if (snap.Config.State != ClusterState.Active)
            return await RejectAsync(cluster, bucket,
                $"кластер не Active ({snap.Config.State}) — finalize недоступен", ct, op);
        if (!MoveNames.ValidateIdentifier(bucket))
            return await RejectAsync(cluster, bucket, $"недопустимое имя бакета '{bucket}'", ct, op);

        var owner = snap.Routing.FirstOrDefault(r => $"bucket_{r.Id}" == bucket)?.Owner;
        if (owner is null)
            return await RejectAsync(cluster, bucket,
                $"нет {MoveNames.RoutingKey(cluster, bucket)} — владелец неизвестен (восстанови контрол-плейн, P12)", ct, op);

        // Finalize — только из ACTIVE: незавершённый переезд убирает abort.
        var existing = await status.GetAsync(cluster, bucket, ct);
        if (!existing.IsSuccess)
            return await FailTransientAsync(cluster, existing.Error!, ct, op);
        if (existing.Value is { } live)
            return await RejectAsync(cluster, bucket,
                $"finalize возможен только из ACTIVE (сейчас state={live.State}) — незавершённый переезд убирает abort", ct, op);

        if (request.OldShard is not { } old || !MoveNames.ValidateIdentifier(old))
            return await RejectAsync(cluster, bucket, "заявка без валидного old_shard", ct, op);
        if (old == owner)
            return await RejectAsync(cluster, bucket,
                $"old_shard ('{old}') совпадает с текущим владельцем — убирать нечего", ct, op);

        var oldShard = snap.Shards.FirstOrDefault(s => s.Name == old);
        var ownerShard = snap.Shards.FirstOrDefault(s => s.Name == owner);
        if (oldShard?.Dsn is null)
            return await RejectAsync(cluster, bucket, $"шард '{old}' не зарегистрирован (нет dsn-ключа)", ct, op);
        if (ownerShard?.Dsn is null)
            return await RejectAsync(cluster, bucket, $"шард-владелец '{owner}' не зарегистрирован (нет dsn-ключа)", ct, op);

        var dsns = await ResolveShardDsnsAsync(snap, oldShard, ownerShard, ct);
        if (!dsns.IsSuccess)
            return await FailTransientAsync(cluster, dsns.Error!, ct, op);
        var (oldDsn, ownerDsn) = dsns.Value;

        // 1) Подписки — первыми: держат слоты (и WAL) на источнике. Fallback при
        //    недоступном источнике: слот-сирота добивается шагом слотов ниже.
        var subRb = await sql.ScalarAsync(oldDsn, MoveSql.SubExists(MoveNames.SubRb(bucket)), ct);
        if (!subRb.IsSuccess)
            return await FailTransientAsync(cluster, subRb.Error!, ct, op);
        if (ToLong(subRb.Value) > 0)
        {
            var droppedRb = await SubscriptionDrop.DropAsync(sql, oldDsn, MoveNames.SubRb(bucket), ct);
            if (!droppedRb.IsSuccess)
                return await FailTransientAsync(cluster, droppedRb.Error!, ct, op);
        }

        var sub = await sql.ScalarAsync(ownerDsn, MoveSql.SubExists(MoveNames.Sub(bucket)), ct);
        if (!sub.IsSuccess)
            return await FailTransientAsync(cluster, sub.Error!, ct, op);
        if (ToLong(sub.Value) > 0)
        {
            var dropped = await SubscriptionDrop.DropAsync(sql, ownerDsn, MoveNames.Sub(bucket), ct);
            if (!dropped.IsSuccess)
                return await FailTransientAsync(cluster, dropped.Error!, ct, op);
        }

        // 2) Публикации (pub на old, pub_rb у владельца).
        var pub = await sql.ScalarAsync(oldDsn, MoveSql.PubExists(MoveNames.Pub(bucket)), ct);
        if (!pub.IsSuccess)
            return await FailTransientAsync(cluster, pub.Error!, ct, op);
        if (ToLong(pub.Value) > 0)
        {
            var dropPub = await sql.ExecuteAsync(oldDsn, MoveSql.DropPublication(MoveNames.Pub(bucket)), ct);
            if (!dropPub.IsSuccess)
                return await FailTransientAsync(cluster, dropPub.Error!, ct, op);
        }

        var pubRb = await sql.ScalarAsync(ownerDsn, MoveSql.PubExists(MoveNames.PubRb(bucket)), ct);
        if (!pubRb.IsSuccess)
            return await FailTransientAsync(cluster, pubRb.Error!, ct, op);
        if (ToLong(pubRb.Value) > 0)
        {
            var dropPubRb = await sql.ExecuteAsync(ownerDsn, MoveSql.DropPublication(MoveNames.PubRb(bucket)), ct);
            if (!dropPubRb.IsSuccess)
                return await FailTransientAsync(cluster, dropPubRb.Error!, ct, op);
        }

        // 3) Слоты на old: основной (сирота после fallback-среза подписки — глушим
        //    активного walsender'а и ждём дезактивации, как cleanup_slots скрипта)
        //    и осиротевшие tablesync-слоты (P8: failover приёмника рестартует
        //    синхронизацию таблицы новым слотом; активные — громкий пропуск).
        var mainSlot = await DropSlotAsync(oldDsn, MoveNames.Sub(bucket), ct);
        if (!mainSlot.IsSuccess)
            return await FailTransientAsync(cluster, mainSlot.Error!, ct, op);

        var warnings = new List<string>();
        var orphans = await sql.ListAsync(oldDsn, MoveSql.OrphanTablesyncSlots(MoveNames.Sub(bucket)), ct);
        if (!orphans.IsSuccess)
            return await FailTransientAsync(cluster, orphans.Error!, ct, op);
        foreach (var orphan in orphans.Value)
        {
            var active = await sql.ScalarAsync(oldDsn, MoveSql.SlotActive(orphan), ct);
            if (!active.IsSuccess)
                return await FailTransientAsync(cluster, active.Error!, ct, op);
            if (ToBool(active.Value) == true)
            {
                warnings.Add($"sync-слот {orphan} на '{old}' ещё активен — пропущен, прибери вручную");
                continue;
            }
            var dropped = await sql.ExecuteAsync(oldDsn, MoveSql.DropSlot(orphan), ct);
            if (!dropped.IsSuccess)
                return await FailTransientAsync(cluster, dropped.Error!, ct, op);
        }

        // 4) DROP SCHEMA на old — последним, СО ДАННЫМИ; владелец не трогается.
        var schema = await sql.ScalarAsync(oldDsn, MoveSql.SchemaExists(bucket), ct);
        if (!schema.IsSuccess)
            return await FailTransientAsync(cluster, schema.Error!, ct, op);
        if (ToBool(schema.Value) == true)
        {
            var droppedSchema = await sql.ExecuteAsync(oldDsn, MoveSql.DropSchemaCascade(bucket), ct);
            if (!droppedSchema.IsSuccess)
                return await FailTransientAsync(cluster, droppedSchema.Error!, ct, op);
        }

        // 5) Снапшот (best-effort) → del заявки → done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                warnings.Add($"снапшот finalize-{bucket} не снялся: {shot.Error!.Message} — сними вручную");
        }

        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, op, "done", claims.InstanceId,
            warnings.Count > 0 ? string.Join("; ", warnings) : null, ct);
        logger?.LogInformation("finalize {cluster}/{bucket}: старый шард '{old}' вычищен (владелец '{owner}' не тронут)",
            cluster, bucket, old, owner);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Слот на шарде: активного walsender'а глушим и ждём дезактивации (≤5×1с,
    // cleanup_slots abort-move.sh); не дезактивировался — отказ (кто-то читает).
    private async Task<Result> DropSlotAsync(string dsn, string slot, CancellationToken ct)
    {
        var exists = await sql.ScalarAsync(dsn, MoveSql.SlotExists(slot), ct);
        if (!exists.IsSuccess)
            return exists;
        if (ToLong(exists.Value) == 0)
            return Result.Success(); // идемпотентность: срезан самой подпиской

        var active = await sql.ScalarAsync(dsn, MoveSql.SlotActive(slot), ct);
        if (!active.IsSuccess)
            return active;
        if (ToBool(active.Value) == true)
        {
            var killed = await sql.ExecuteAsync(dsn, MoveSql.TerminateSlotBackend(slot), ct);
            if (!killed.IsSuccess)
                return killed;
            for (var attempt = 0; attempt < 5; attempt++)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                var recheck = await sql.ScalarAsync(dsn, MoveSql.SlotActive(slot), ct);
                if (!recheck.IsSuccess)
                    return recheck;
                if (ToBool(recheck.Value) != true)
                    break;
            }
            var last = await sql.ScalarAsync(dsn, MoveSql.SlotActive(slot), ct);
            if (!last.IsSuccess)
                return last;
            if (ToBool(last.Value) == true)
                return Result.Failed(new ApplicationException(
                    $"слот {slot} всё ещё активен — кто-то читает; разберись вручную"));
        }

        return await sql.ExecuteAsync(dsn, MoveSql.DropSlot(slot), ct);
    }

    // op=abort — делегирует AbortSequence (t01 задача 16, spec §6.5): журнал
    // ABORTING ДО манипуляций, идемпотентная уборка, AbortMinAgeSec/force.
    private Task<Result<ProcessOutcome>> RunAbortAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
    {
        var abort = new AbortSequence(sql, status, requests, journal, shards, secrets);
        return abort.RunAsync(snap, bucket, request, claims, clock, options, ct, snapshot);
    }

    // ── Исходы M0/оп-веток ──

    // Перманентный отказ: del заявки + журнал rejected + Failed (spec §4.1/§6.1).
    private async Task<Result<ProcessOutcome>> RejectAsync(
        string cluster, string bucket, string reason, CancellationToken ct, string op = "move")
    {
        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, op, "rejected", claims.InstanceId, reason, ct);
        logger?.LogWarning("{op} {cluster}/{bucket} отвергнут: {reason}", op, cluster, bucket, reason);
        return Result<ProcessOutcome>.Failed(new ApplicationException($"{op} {cluster}/{bucket}: {reason}"));
    }

    // Transient-сбой: журнал last_error (алерт), заявка жива — ретраи тиками.
    private async Task<Result<ProcessOutcome>> FailTransientAsync(
        string cluster, Exception error, CancellationToken ct, string op = "move")
    {
        await journal.WritePhaseAsync(cluster, op, "waiting", claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // ── Общие хелперы ──

    // Admin-DSN мастеров ВСЕХ шардов кластера (поиск артефактов по конвенциям).
    private async Task<Result<Dictionary<string, string>>> ResolveAllDsnAsync(
        ClusterSnapshot snap, CancellationToken ct)
    {
        var addresses = await shards.ReadPortAllocAsync(snap.Config.Cluster, ct);
        if (!addresses.IsSuccess)
            return Result<Dictionary<string, string>>.Failed(addresses.Error!);

        var result = new Dictionary<string, string>();
        foreach (var shard in snap.Shards)
        {
            var master = await shards.ResolveMasterAsync(shard, addresses.Value, ct);
            if (!master.IsSuccess)
                return Result<Dictionary<string, string>>.Failed(master.Error!);
            if (master.Value is null)
                return Result<Dictionary<string, string>>.Failed(new ApplicationException(
                    $"мастер '{shard.Name}' не определён — ждём (Patroni-выборы?)"));
            result[shard.Name] = ShardEndpoints.AdminDsn(master.Value, snap.Config.DbName, secrets);
        }

        return Result<Dictionary<string, string>>.Success(result);
    }

    // Снапшот-точка переезда — best-effort: неудача в журнал (P12: сними вручную).
    private async Task<Result> SnapshotBestEffortAsync(string cluster, CancellationToken ct)
    {
        if (snapshot is null)
            return Result.Success();

        var shot = await snapshot(ct);
        if (!shot.IsSuccess)
            await journal.WritePhaseAsync(cluster, "move", "post-flip", claims.InstanceId,
                $"снапшот не снялся: {shot.Error!.Message} — сними вручную (P12)", ct);
        return shot;
    }

    private async Task<Result<(string Src, string Dst)>> ResolveShardDsnsAsync(
        ClusterSnapshot snap, ShardSpec srcShard, ShardSpec dstShard, CancellationToken ct)
    {
        var addresses = await shards.ReadPortAllocAsync(snap.Config.Cluster, ct);
        if (!addresses.IsSuccess)
            return Result<(string, string)>.Failed(addresses.Error!);

        var srcMaster = await shards.ResolveMasterAsync(srcShard, addresses.Value, ct);
        if (!srcMaster.IsSuccess)
            return Result<(string, string)>.Failed(srcMaster.Error!);
        if (srcMaster.Value is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"мастер '{srcShard.Name}' не определён — ждём (Patroni-выборы?)"));

        var dstMaster = await shards.ResolveMasterAsync(dstShard, addresses.Value, ct);
        if (!dstMaster.IsSuccess)
            return Result<(string, string)>.Failed(dstMaster.Error!);
        if (dstMaster.Value is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"мастер '{dstShard.Name}' не определён — ждём (Patroni-выборы?)"));

        return Result<(string, string)>.Success((
            ShardEndpoints.AdminDsn(srcMaster.Value, snap.Config.DbName, secrets),
            ShardEndpoints.AdminDsn(dstMaster.Value, snap.Config.DbName, secrets)));
    }

    private Task<Result> PutStatusAsync(
        string cluster, string bucket, string owner, string target, long startedUnix, string phase, CancellationToken ct)
        => status.PutAsync(cluster, new MoveStatus(bucket, MoveStates.Syncing, owner, target,
            startedUnix, Now(), phase), ct);

    private long Now() => clock.GetUtcNow().ToUnixTimeSeconds();

    // Скаляры из Npgsql приходят типизированными (bool/long/string), фейки — как есть.
    private static string? ToText(object? value) => value?.ToString();

    private static bool? ToBool(object? value) => value switch
    {
        bool b => b,
        "t" or "true" or "True" => true,
        "f" or "false" or "False" => false,
        _ => long.TryParse(ToText(value), out var number) ? number != 0 : null,
    };

    private static long ToLong(object? value)
        => long.TryParse(ToText(value), out var number) ? number : 0;
}
