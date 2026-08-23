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
/// Фазы M1–M6/rollback/finalize/abort — задачи 13–16.
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
        if (oldest.Value is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // заявок нет

        var (bucket, request) = oldest.Value.Value;
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
        if (existing.Value is { } prev)
        {
            switch (prev.State)
            {
                case MoveStates.Syncing or MoveStates.Frozen when prev.Target == to:
                    startedUnix = prev.StartedUnix; // resume: возраст переезда сохраняется
                    snapshotRequired = prev.Phase == MovePhases.WaitingSnapshot;
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

        // Resume: снапшот-точка уже есть, статус ≥ ddl — фазы M1–M3 этим же тиком.
        return await RunMovePhasesAsync(
            snap, bucket, owner, to, srcShard, srcDsn, dstDsn, startedUnix,
            subOnDst: ToBool(subDst.Value) == true,
            schemaOnDst: ToBool(schemaDst.Value) == true,
            ct);
    }

    // ── M1–M3 (t01 задача 13, spec §6.1): DDL → pub/sub → copy-wait ──

    private async Task<Result<ProcessOutcome>> RunMovePhasesAsync(
        ClusterSnapshot snap, string bucket, string owner, string to,
        ShardSpec srcShard, string srcDsn, string dstDsn, long startedUnix,
        bool subOnDst, bool schemaOnDst, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // M1: DDL-перенос — только когда схемы на приёмнике нет (resume пропускает).
        if (!subOnDst && !schemaOnDst)
        {
            if (srcShard.Master?.Split(':')[0] is not { } srcNode)
                return await FailTransientAsync(cluster, new ApplicationException(
                    $"мастер '{owner}' без master-ключа — имя ноды для pg_dump неизвестно"), ct);
            var dump = await ddl.DumpAsync(cluster, owner, srcNode, snap.Config.DbName, bucket, ct);
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
                    ShardEndpoints.MoverConninfo(srcShard.Dsn!, secrets),
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

    // Заглушки op-веток (реализация — задачи 15–16: rollback/finalize/abort).
    private Task<Result<ProcessOutcome>> RunRollbackAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
        => throw new NotSupportedException("op=rollback — реализация в задаче 15");

    private Task<Result<ProcessOutcome>> RunFinalizeAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
        => throw new NotSupportedException("op=finalize — реализация в задаче 15");

    private Task<Result<ProcessOutcome>> RunAbortAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request, CancellationToken ct)
        => throw new NotSupportedException("op=abort — реализация в задаче 16");

    // ── Исходы M0 ──

    // Перманентный отказ: del заявки + журнал rejected + Failed (spec §4.1/§6.1).
    private async Task<Result<ProcessOutcome>> RejectAsync(
        string cluster, string bucket, string reason, CancellationToken ct)
    {
        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, "move", "rejected", claims.InstanceId, reason, ct);
        logger?.LogWarning("move {cluster}/{bucket} отвергнут: {reason}", cluster, bucket, reason);
        return Result<ProcessOutcome>.Failed(new ApplicationException($"move {cluster}/{bucket}: {reason}"));
    }

    // Transient-сбой: журнал last_error (алерт), заявка жива — ретраи тиками.
    private async Task<Result<ProcessOutcome>> FailTransientAsync(
        string cluster, Exception error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, "move", "waiting", claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // ── Общие хелперы ──

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
