using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Moves;

/// <summary>Фазы журнала уборки в статус-ключе (state=ABORTING; abort-move.sh).</summary>
public static class AbortPhases
{
    /// <summary>Недоступен шард — инвентаризация неполна, уборка не начиналась.</summary>
    public const string Blocked = "blocked";

    /// <summary>План записан, манипуляции с БД идут (★ журнал ДО manipulations).</summary>
    public const string DbCleanup = "db-cleanup";

    /// <summary>Контрольная инвентаризация нашла остатки.</summary>
    public const string Failed = "failed";

    /// <summary>Уборка завершена — ключ сейчас будет удалён (нет ключа = ACTIVE).</summary>
    public const string Done = "done";
}

/// <summary>
/// Abort — отмена незавершённого переезда и уборка его артефактов (t01 задача 16,
/// spec §6.5; порт abort-move.sh): валидация (routing/статус-ключ, защита свежести
/// AbortMinAgeSec по updated_unix — mover возможно жив, Д12; routing==target без
/// force — доведение осознанно) → инвентаризация на ВСЕХ шардах (недоступен —
/// журнал ABORTING/blocked + transient) → журнал ABORTING/db-cleanup с планом
/// ★ ДО манипуляций → идемпотентная уборка subs → slots → pubs → re-GRANT владельца
/// → [доведение sequences при routing==target, setval только вперёд, ДО drop schema]
/// → DROP SCHEMA не-владельцев (схема владельца не трогается никогда) → контрольная
/// инвентаризация → del статус-ключа (= ACTIVE) + del своей заявки + снапшот.
/// Контракт «одна заявка на бакет» (spec §4.1, ревью №6): ключ
/// /pgworker/moves/&lt;C&gt;/bucket_&lt;i&gt; один — abort-заявка оператора перезаписывает
/// move-заявку; здесь удаляется только СВОЯ (op=abort) заявка.
/// </summary>
public sealed class AbortSequence(
    IMoveSqlExecutor sql,
    MoveStatusStore status,
    MoveRequestsStore requests,
    WorkJournal journal,
    ShardEndpoints shards,
    InstallSecrets secrets)
{
    public async Task<Result<ProcessOutcome>> RunAsync(
        ClusterSnapshot snap, string bucket, MoveRequest request,
        ClaimStore claims, TimeProvider clock, MovesRuntimeOptions o, CancellationToken ct,
        Func<CancellationToken, Task<Result>>? snapshot = null)
    {
        var cluster = snap.Config.Cluster;

        // (1) Валидация: routing и статус-ключ обязательны; владелец зарегистрирован.
        if (!MoveNames.ValidateIdentifier(bucket))
            return await RejectAsync(cluster, bucket, claims, $"недопустимое имя бакета '{bucket}'", ct);
        var owner = snap.Routing.FirstOrDefault(r => $"bucket_{r.Id}" == bucket)?.Owner;
        if (owner is null)
            return await RejectAsync(cluster, bucket, claims,
                $"нет {MoveNames.RoutingKey(cluster, bucket)} — владелец неизвестен, уборка небезопасна (восстанови контрол-плейн, P12)", ct);
        if (snap.Shards.FirstOrDefault(s => s.Name == owner && s.Dsn is not null) is null)
            return await RejectAsync(cluster, bucket, claims,
                $"владелец '{owner}' не зарегистрирован (нет dsn-ключа)", ct);

        var existing = await status.GetAsync(cluster, bucket, ct);
        if (!existing.IsSuccess)
            return await FailTransientAsync(cluster, claims, existing.Error!, ct);
        if (existing.Value is null)
            return await RejectAsync(cluster, bucket, claims,
                "статус-ключа нет — бакет ACTIVE, откатывать нечего; пост-flip артефакты убирает finalize", ct);

        var prev = existing.Value;
        var resuming = prev.State == MoveStates.Aborting; // продолжение незавершённой уборки

        // Защита свежести (не для продолжения уборки): свежий статус — mover,
        // возможно, ещё работает; force ломает защиту (Д12: updated_unix пишет каждый тик).
        if (!resuming && !request.Force)
        {
            var age = clock.GetUtcNow().ToUnixTimeSeconds() - prev.UpdatedUnix;
            if (age < o.AbortMinAgeSec)
                return await FailTransientAsync(cluster, claims, new ApplicationException(
                    $"статус обновлён {age}с назад (< AbortMinAgeSec={o.AbortMinAgeSec}с) — переезд, возможно, ещё жив; если mover точно мёртв — force"), ct);
        }

        // routing уже указывает на target: flip прошёл, статус завис — abort станет
        // ДОВЕДЕНИЕМ перевода (как finalize); осознанно — только с force.
        if (prev.Target == owner && !request.Force)
            return await RejectAsync(cluster, bucket, claims,
                $"routing уже указывает на target '{prev.Target}' — похоже, flip прошёл, а статус-ключ остался; такой abort станет уборкой СТАРОГО шарда (как finalize) — осознанно: force", ct);

        // (2) Инвентаризация артефактов на ВСЕХ шардах: pub/sub/slot по конвенциям
        //     + схема; недоступный шард — картина неполна, уборку не начинаем.
        var dsns = await ResolveAllDsnAsync(snap, ct);
        if (!dsns.IsSuccess)
            return await FailTransientAsync(cluster, claims, dsns.Error!, ct);

        var scan = await ScanArtifactsAsync(cluster, bucket, owner, dsns.Value, ct);
        if (!scan.IsSuccess)
            return await FailTransientAsync(cluster, claims, scan.Error!, ct);
        if (scan.Value.Unreachable.Count > 0)
        {
            var blocked = await WriteJournalAsync(cluster, bucket, prev, AbortPhases.Blocked,
                $"недоступны шарды: {string.Join(", ", scan.Value.Unreachable)} — инвентаризация неполна, уборка не начиналась",
                plan: [], scan.Value.Unreachable, clock, ct);
            if (!blocked.IsSuccess)
                return Result<ProcessOutcome>.Failed(blocked.Error!);
            return await FailTransientAsync(cluster, claims, new ApplicationException(
                $"abort {cluster}/{bucket}: недоступны шарды ({string.Join(", ", scan.Value.Unreachable)}) — журнал ABORTING/blocked, повтор после возврата шарда"), ct);
        }

        // (3) ★ Журнал ДО любых манипуляций с БД: state=ABORTING + план уборки
        //     (крах посреди уборки оставляет самодокументирующийся след, P7).
        var started = resuming ? prev.StartedUnix : clock.GetUtcNow().ToUnixTimeSeconds();
        var cleanup = await WriteJournalAsync(cluster, bucket, prev, AbortPhases.DbCleanup, null,
            scan.Value.Plan, [], clock, ct);
        if (!cleanup.IsSuccess)
            return Result<ProcessOutcome>.Failed(cleanup.Error!);

        // (4) Уборка идемпотентно, в порядке скрипта; фазы — в work-журнал.
        var planByKind = scan.Value.Plan
            .GroupBy(item => item.Kind)
            .ToDictionary(g => g.Key, g => g.ToList());

        await journal.WritePhaseAsync(cluster, "abort", "drop-subscriptions", claims.InstanceId, null, ct);
        if (planByKind.TryGetValue("sub", out var subs))
            foreach (var item in subs)
            {
                var dropped = await SubscriptionDrop.DropAsync(sql, dsns.Value[item.Shard], item.Name, ct);
                if (!dropped.IsSuccess)
                    return await FailTransientAsync(cluster, claims, dropped.Error!, ct);
            }

        await journal.WritePhaseAsync(cluster, "abort", "drop-slots", claims.InstanceId, null, ct);
        if (planByKind.TryGetValue("slot", out var slots))
            foreach (var item in slots)
            {
                var dropped = await DropSlotAsync(dsns.Value[item.Shard], item.Name, ct);
                if (!dropped.IsSuccess)
                    return await FailTransientAsync(cluster, claims, dropped.Error!, ct);
            }

        await journal.WritePhaseAsync(cluster, "abort", "drop-publications", claims.InstanceId, null, ct);
        if (planByKind.TryGetValue("pub", out var pubs))
            foreach (var item in pubs)
            {
                var exists = await sql.ScalarAsync(dsns.Value[item.Shard], MoveSql.PubExists(item.Name), ct);
                if (!exists.IsSuccess)
                    return await FailTransientAsync(cluster, claims, exists.Error!, ct);
                if (!Exists(exists.Value))
                    continue;
                var dropped = await sql.ExecuteAsync(dsns.Value[item.Shard], MoveSql.DropPublication(item.Name), ct);
                if (!dropped.IsSuccess)
                    return await FailTransientAsync(cluster, claims, dropped.Error!, ct);
            }

        // re-GRANT на владельце — снятие P1/P5-заморозки (схемы нет — нечего).
        await journal.WritePhaseAsync(cluster, "abort", "unfreeze-owner", claims.InstanceId, null, ct);
        var ownerSchema = await sql.ScalarAsync(dsns.Value[owner], MoveSql.SchemaExists(bucket), ct);
        if (!ownerSchema.IsSuccess)
            return await FailTransientAsync(cluster, claims, ownerSchema.Error!, ct);
        if (Exists(ownerSchema.Value))
        {
            var unfrozen = await sql.ExecuteAsync(dsns.Value[owner], MoveSql.Unfreeze(bucket, MoveNames.AppRole), ct);
            if (!unfrozen.IsSuccess)
                return await FailTransientAsync(cluster, claims, unfrozen.Error!, ct);
        }

        // routing==target (доведение): sequences не реплицируются — если cutover
        // прошёл без шага sequences, счётчик владельца отстаёт (P6); setval только
        // ВПЕРЁД, ДО удаления старой схемы (иначе issued читать неоткуда).
        if (prev.Target == owner)
        {
            await journal.WritePhaseAsync(cluster, "abort", "sync-sequences", claims.InstanceId, null, ct);
            var synced = await SyncSequencesAsync(cluster, bucket, owner, dsns.Value, scan.Value.Plan, ct);
            if (!synced.IsSuccess)
                return await FailTransientAsync(cluster, claims, synced.Error!, ct);
        }

        // DROP SCHEMA на НЕ-владельцах — последним, с данными; владелец не трогается.
        await journal.WritePhaseAsync(cluster, "abort", "drop-schema", claims.InstanceId, null, ct);
        foreach (var item in scan.Value.Plan.Where(p => p.Kind == "schema" && p.Shard != owner))
        {
            var exists = await sql.ScalarAsync(dsns.Value[item.Shard], MoveSql.SchemaExists(bucket), ct);
            if (!exists.IsSuccess)
                return await FailTransientAsync(cluster, claims, exists.Error!, ct);
            if (!Exists(exists.Value))
                continue;
            var dropped = await sql.ExecuteAsync(dsns.Value[item.Shard], MoveSql.DropSchemaCascade(bucket), ct);
            if (!dropped.IsSuccess)
                return await FailTransientAsync(cluster, claims, dropped.Error!, ct);
        }

        // (5) Контрольная инвентаризация: остатков (кроме схемы владельца) нет.
        var control = await ScanArtifactsAsync(cluster, bucket, owner, dsns.Value, ct);
        if (!control.IsSuccess)
            return await FailTransientAsync(cluster, claims, control.Error!, ct);
        if (control.Value.Unreachable.Count > 0)
        {
            var failed = await WriteJournalAsync(cluster, bucket, prev, AbortPhases.Failed,
                $"при контроле недоступны шарды: {string.Join(", ", control.Value.Unreachable)} — повтор позже",
                scan.Value.Plan, control.Value.Unreachable, clock, ct);
            if (!failed.IsSuccess)
                return Result<ProcessOutcome>.Failed(failed.Error!);
            return await FailTransientAsync(cluster, claims, new ApplicationException(
                $"abort {cluster}/{bucket}: контрольная инвентаризация не прошла — шарды недоступны"), ct);
        }

        var leftover = control.Value.Plan.Where(p => p.Shard != owner || p.Kind != "schema").ToList();
        if (leftover.Count > 0)
        {
            var failed = await WriteJournalAsync(cluster, bucket, prev, AbortPhases.Failed,
                $"остались артефакты: {string.Join("; ", leftover.Select(p => $"{p.Shard}:{p.Kind} {p.Name}"))}",
                scan.Value.Plan, [], clock, ct);
            if (!failed.IsSuccess)
                return Result<ProcessOutcome>.Failed(failed.Error!);
            return await FailTransientAsync(cluster, claims, new ApplicationException(
                $"abort {cluster}/{bucket}: после уборки остались артефакты ({leftover.Count}) — см. журнал ABORTING/failed"), ct);
        }

        // (6) del статус-ключа = ACTIVE; del СВОЕЙ заявки; снапшот best-effort.
        var done = await WriteJournalAsync(cluster, bucket, prev, AbortPhases.Done, null,
            scan.Value.Plan, [], clock, ct);
        if (!done.IsSuccess)
            return Result<ProcessOutcome>.Failed(done.Error!);

        var deletedStatus = await status.DeleteAsync(cluster, bucket, ct);
        if (!deletedStatus.IsSuccess)
            return Result<ProcessOutcome>.Failed(deletedStatus.Error!);

        var deletedRequest = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deletedRequest.IsSuccess)
            return Result<ProcessOutcome>.Failed(deletedRequest.Error!);

        if (snapshot is not null)
            await snapshot(ct); // best-effort: ключ уже ACTIVE, сбоем не роняем

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Инвентаризация (scan_artifacts): строки «шард|тип|имя» + недоступные ──

    private sealed record Scan(IReadOnlyList<AbortPlanItem> Plan, IReadOnlyList<string> Unreachable);

    private async Task<Result<Scan>> ScanArtifactsAsync(
        string cluster, string bucket, string owner,
        IReadOnlyDictionary<string, string> dsns, CancellationToken ct)
    {
        var plan = new List<AbortPlanItem>();
        var unreachable = new List<string>();
        foreach (var (shard, dsn) in dsns)
        {
            var alive = await sql.ScalarAsync(dsn, "SELECT 1", ct);
            if (!alive.IsSuccess)
            {
                unreachable.Add(shard);
                continue;
            }

            foreach (var (kind, name, query) in Checks(bucket))
            {
                var exists = await sql.ScalarAsync(dsn, query, ct);
                if (!exists.IsSuccess)
                    return Result<Scan>.Failed(exists.Error!);
                if (Exists(exists.Value))
                    plan.Add(new AbortPlanItem(shard, kind, name));
            }
        }

        return Result<Scan>.Success(new Scan(plan, unreachable));
    }

    // Проверки по конвенциям имён (scan_artifacts скрипта); порядок = план уборки.
    private static (string Kind, string Name, string Sql)[] Checks(string bucket) =>
    [
        ("sub", MoveNames.Sub(bucket), MoveSql.SubExists(MoveNames.Sub(bucket))),
        ("sub", MoveNames.SubRb(bucket), MoveSql.SubExists(MoveNames.SubRb(bucket))),
        ("slot", MoveNames.Sub(bucket), MoveSql.SlotExists(MoveNames.Sub(bucket))),
        ("slot", MoveNames.SubRb(bucket), MoveSql.SlotExists(MoveNames.SubRb(bucket))),
        ("pub", MoveNames.Pub(bucket), MoveSql.PubExists(MoveNames.Pub(bucket))),
        ("pub", MoveNames.PubRb(bucket), MoveSql.PubExists(MoveNames.PubRb(bucket))),
        ("schema", bucket, MoveSql.SchemaExists(bucket)),
    ];

    // Доведение P6 (routing==target): issued читается на НЕ-владельце со схемой,
    // next — у владельца; setval только вперёд (пост-flip записи уже расходовали
    // значения владельца).
    private async Task<Result> SyncSequencesAsync(
        string cluster, string bucket, string owner,
        IReadOnlyDictionary<string, string> dsns, IReadOnlyList<AbortPlanItem> plan, CancellationToken ct)
    {
        foreach (var schema in plan.Where(p => p.Kind == "schema" && p.Shard != owner))
        {
            var names = await sql.ListAsync(dsns[schema.Shard], MoveSql.SequenceNames(bucket), ct);
            if (!names.IsSuccess)
                return Result.Failed(names.Error!);
            foreach (var sequence in names.Value)
            {
                var issued = await sql.ScalarAsync(
                    dsns[schema.Shard], MoveSql.SequenceIssued(bucket, sequence), ct);
                if (!issued.IsSuccess)
                    return Result.Failed(issued.Error!);
                var next = await sql.ScalarAsync(dsns[owner], MoveSql.SequenceNext(bucket, sequence), ct);
                if (!next.IsSuccess)
                    return Result.Failed(next.Error!);
                if (ToLong(next.Value) <= ToLong(issued.Value))
                {
                    var setval = await sql.ExecuteAsync(
                        dsns[owner], MoveSql.SetvalForward(bucket, sequence, ToLong(issued.Value)), ct);
                    if (!setval.IsSuccess)
                        return Result.Failed(setval.Error!);
                }
            }
        }

        return Result.Success();
    }

    // Слот: активен → terminate walsender'а + ожидание дезактивации (≤5×1с,
    // cleanup_slots скрипта); не дезактивировался — отказ; затем pg_drop.
    private async Task<Result> DropSlotAsync(string dsn, string slot, CancellationToken ct)
    {
        var exists = await sql.ScalarAsync(dsn, MoveSql.SlotExists(slot), ct);
        if (!exists.IsSuccess)
            return exists;
        if (!Exists(exists.Value))
            return Result.Success();

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

    // ── Журнал уборки (journal_set): тот же статус-ключ, state=ABORTING константой ──

    private Task<Result> WriteJournalAsync(
        string cluster, string bucket, MoveStatus prev, string phase, string? error,
        IReadOnlyList<AbortPlanItem> plan, IReadOnlyList<string> unreachable,
        TimeProvider clock, CancellationToken ct)
        => status.PutRawAsync(cluster, bucket, new AbortJournal(
            bucket, prev.State, prev.Owner, prev.Target,
            prev.StartedUnix, clock.GetUtcNow().ToUnixTimeSeconds(),
            phase, error, plan, unreachable).Serialize(), ct);

    // ── Исходы ──

    // Перманентный отказ: del заявки + журнал rejected + Failed.
    private async Task<Result<ProcessOutcome>> RejectAsync(
        string cluster, string bucket, ClaimStore claims, string reason, CancellationToken ct)
    {
        var deleted = await requests.DeleteAsync(cluster, bucket, ct);
        if (!deleted.IsSuccess)
            return Result<ProcessOutcome>.Failed(deleted.Error!);

        await journal.WritePhaseAsync(cluster, "abort", "rejected", claims.InstanceId, reason, ct);
        return Result<ProcessOutcome>.Failed(new ApplicationException($"abort {cluster}/{bucket}: {reason}"));
    }

    // Transient: work.last_error, заявка жива — ретраи тиками (журнал ABORTING в etcd).
    private async Task<Result<ProcessOutcome>> FailTransientAsync(
        string cluster, ClaimStore claims, Exception error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, "abort", "waiting", claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // ── Общие хелперы ──

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

    // Exists-скаляр: счётчик (count(*)) или bool (to_regnamespace) — от фейка и Npgsql.
    private static bool Exists(object? value) => ToBool(value) == true || ToLong(value) > 0;
}
