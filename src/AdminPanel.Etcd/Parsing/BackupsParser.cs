using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

public sealed record BackupsParseResult(
    IReadOnlyList<ClusterBackupsInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    // глобальный ключ /pgworker/backups/storage (null — ключа нет/битый).
    BackupStorageInfo? Storage = null,
    // глобальный ключ /pgworker/backups/orphans (null — ключа нет/битый).
    BackupOrphansInfo? Orphans = null);

// Чистая функция: KV префикса /pgworker/backups/ (arch/19 §4, +):
// policy full_max_age_sec + per-shard последний COMPLETED finished_unix
// (правило backup-full-stale) + WAL-статусы шардов (правила
// wal-chain-broken/wal-stream-lag/wal-stream-stopped). Битые значения —
// KeyParseError + пропуск записи (паттерн панели). Шард с любыми ключами
// бэкапов попадает в словари: null = COMPLETED не было («полного никогда
// не было», spec §3.5); шарда нет в словаре = ключей нет вообще (правила
// молчат — подсистема не включена).
public static class BackupsParser
{
    public const string Prefix = "/pgworker/backups/";

    public static BackupsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        // "/pgworker/backups/<C>/policy" | "/pgworker/backups/<C>/<X>/full/<id>"
        // | "/pgworker/backups/<C>/<X>/wal" 
        var policies = new Dictionary<string, long?>();
        // reliability : полная policy (retention/verify/drill) + drill-ключи per-shard.
        var policiesFull = new Dictionary<string, BackupsPolicyInfo>();
        var drills = new Dictionary<string, Dictionary<string, DrillInfo>>();
        var shards = new Dictionary<string, Dictionary<string, long?>>();
        var wal = new Dictionary<string, Dictionary<string, WalStreamInfo?>>();
        var deleting = new Dictionary<string, Dictionary<string, List<DeletingFullInfo>>>();
        var restores = new Dictionary<string, Dictionary<string, List<RestoreOperationInfo>>>();
        // все etcd-полные per-shard (вход S3Reconciler и деталей шарда).
        var fulls = new Dictionary<string, Dictionary<string, List<BackupFullInfo>>>();
        BackupStorageInfo? storage = null;
        BackupOrphansInfo? orphans = null;
        // hold/заявки сирот (arch/19 §4) — per-префиксные ключи; собираем
        // ВСЕГДА, в модель кладём только при валидном orphans-ключе (без реестра
        // не информативны); сборка BackupOrphansInfo — ПОСЛЕ цикла: ключи в
        // списке не упорядочены (hold может идти раньше/позже orphans).
        List<BackupOrphanInfo>? orphansEntries = null;
        long orphansUpdated = 0;
        var holds = new Dictionary<string, OrphanHoldInfo>(StringComparer.Ordinal);
        var deleteRequests = new Dictionary<string, OrphanDeleteRequestInfo>(StringComparer.Ordinal);
        var verifyFailures = new Dictionary<string, Dictionary<string, ShardVerifyFailure>>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');

            // Hold/заявки сирот (arch/19 §4): 6 сегментов — ДО гварда длины
            // (гвард "< 5 → continue" их не пропускает, но ветка orphans-ключа —
            // раньше; единый ранний разбор глобальных ключей). Формат
            // {"set_unix":T,"set_by":"…"} / {"requested_unix":T,"requested_by":"…"};
            // битые — KeyParseError + пропуск (толерантный читатель).
            if (segments.Length == 6 && segments[1] == "pgworker" && segments[2] == "backups"
                && (segments[3] == "orphan-holds" || segments[3] == "orphan-deletes"))
            {
                var isHold = segments[3] == "orphan-holds";
                var orphanPrefix = $"{segments[4]}/{segments[5]}";
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    var unix = Long(root, isHold ? "set_unix" : "requested_unix");
                    var by = String(root, isHold ? "set_by" : "requested_by");
                    if (unix is null || by is null)
                        errors.Add(new(kv.Key, isHold
                            ? "broken orphan hold key (set_unix/set_by)"
                            : "broken orphan delete request key (requested_unix/requested_by)"));
                    else if (isHold)
                        holds[orphanPrefix] = new OrphanHoldInfo(orphanPrefix, unix.Value, by);
                    else
                        deleteRequests[orphanPrefix] = new OrphanDeleteRequestInfo(orphanPrefix, unix.Value, by);
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken orphan hold/delete request JSON: {e.Message}"));
                }

                continue;
            }

            // Глобальный ключ /pgworker/backups/storage : ДО гварда длины —
            // у него 4 сегмента, гвард "< 5 → continue" его не пропускает.
            if (segments.Length == 4 && segments[3] == "storage")
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    var used = Long(root, "used_bytes");
                    var updated = Long(root, "updated_unix");
                    var state = String(root, "state") switch
                    {
                        "OK" => BackupStorageState.Ok,
                        "WARN" => BackupStorageState.Warn,
                        "CRIT" => BackupStorageState.Crit,
                        _ => (BackupStorageState?)null,
                    };
                    if (used is null || updated is null || state is null)
                        errors.Add(new(kv.Key, "broken storage status (used_bytes/updated_unix/state)"));
                    else
                        storage = new BackupStorageInfo(
                            used.Value, Long(root, "quota_bytes"), Double(root, "used_percent"),
                            state.Value, updated.Value);
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken storage JSON: {e.Message}"));
                }

                continue;
            }

            // Глобальный ключ /pgworker/backups/orphans : реестр сирот S3 —
            // 4 сегмента; формат arch/19 §4 (записи prefix/kind/size_bytes/
            // first_seen_unix/state OBSERVED|DELETING). Битая запись → KeyParseError
            // на весь ключ (частичный реестр — ложные алерты).
            if (segments.Length == 4 && segments[3] == "orphans")
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    var updated = root.ValueKind == JsonValueKind.Object
                        ? Long(root, "updated_unix") : null;
                    if (updated is null
                        || !root.TryGetProperty("orphans", out var list)
                        || list.ValueKind != JsonValueKind.Array)
                    {
                        errors.Add(new(kv.Key, "broken orphans key (orphans/updated_unix)"));
                    }
                    else
                    {
                        var entries = new List<BackupOrphanInfo>();
                        var malformed = false;
                        foreach (var item in list.EnumerateArray())
                        {
                            var prefix = String(item, "prefix");
                            var kind = String(item, "kind");
                            var size = Long(item, "size_bytes");
                            var seen = Long(item, "first_seen_unix");
                            var state = String(item, "state");
                            if (prefix is null || kind is null || size is null || seen is null
                                || state is not ("OBSERVED" or "DELETING"))
                            {
                                malformed = true;
                                break;
                            }

                            // has_valid_full — (DR-hold): отсутствие поля (старый
                            // воркер) — валидно, false (ближайший проход пересчитает).
                            entries.Add(new BackupOrphanInfo(prefix, kind, size.Value, seen.Value, state,
                                Bool(item, "has_valid_full") ?? false));
                        }

                        if (malformed)
                            errors.Add(new(kv.Key,
                                "broken orphan entry (prefix/kind/size_bytes/first_seen_unix/state)"));
                        else
                        {
                            orphansEntries = entries;
                            orphansUpdated = updated.Value;
                        }
                    }
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken orphans JSON: {e.Message}"));
                }

                continue;
            }

            if (segments.Length < 5 || segments[1] != "pgworker" || segments[2] != "backups")
                continue; // чужой префикс

            var cluster = segments[3];
            if (cluster.Length == 0)
                continue;

            if (segments.Length == 5 && segments[4] == "policy")
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    policies[cluster] = root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("full_max_age_sec", out var age)
                        && age.ValueKind == JsonValueKind.Number
                        && age.TryGetInt64(out var value)
                        ? value
                        : null;

                    // reliability : полный разбор (retention/verify/drill);
                    // битое/отсутствующее поле → null (форма фронта дефолтирует;
                    // панель — толерантный читатель, писатель — policy-API воркера).
                    int? retentionDays = null, retentionWeeks = null, retentionMonths = null;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("retention", out var retention)
                        && retention.ValueKind == JsonValueKind.Object)
                    {
                        retentionDays = Int32Of(retention, "days");
                        retentionWeeks = Int32Of(retention, "weeks");
                        retentionMonths = Int32Of(retention, "months");
                    }

                    bool? verifyOnCreate = null;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("verify", out var verify)
                        && verify.ValueKind == JsonValueKind.Object)
                        verifyOnCreate = Bool(verify, "on_create");

                    int? drillIntervalDays = null;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("drill", out var drillEl)
                        && drillEl.ValueKind == JsonValueKind.Object)
                        drillIntervalDays = Int32Of(drillEl, "interval_days");

                    // секция s3 (pool + списки роли источника); битая → null
                    // (правила backup-pool-* на мусоре молчат — как drill).
                    BackupsPolicyS3Info? s3 = null;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("s3", out var s3El)
                        && s3El.ValueKind == JsonValueKind.Object)
                    {
                        var pool = s3El.TryGetProperty("pool", out var p) && p.ValueKind == JsonValueKind.String
                            ? p.GetString() : null;
                        var master = StringListOf(s3El, "write_master");
                        var replica = StringListOf(s3El, "write_replica");
                        if (!string.IsNullOrEmpty(pool) && master is not null && replica is not null)
                            s3 = new BackupsPolicyS3Info(pool!, master, replica);
                    }

                    policiesFull[cluster] = new BackupsPolicyInfo(
                        retentionDays, retentionWeeks, retentionMonths,
                        policies[cluster], verifyOnCreate, drillIntervalDays, s3);
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken policy JSON: {e.Message}"));
                }

                continue;
            }

            // reliability : /<X>/drill — статус дрилла шарда (state обязателен:
            // RUNNING|SUCCEEDED|FAILED; незнакомое → KeyParseError + пропуск —
            // правила дрилла на мусоре молчат; id/backup_id/started_unix обязательны).
            if (segments.Length == 6 && segments[5] == "drill" && segments[4].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind != JsonValueKind.Object)
                    {
                        errors.Add(new(kv.Key, "drill status is not a JSON object"));
                        continue;
                    }

                    var state = String(root, "state") is "RUNNING" or "SUCCEEDED" or "FAILED"
                        ? String(root, "state")
                        : null;
                    var id = String(root, "id");
                    var backupId = String(root, "backup_id");
                    var started = Long(root, "started_unix");
                    if (state is null || id is null || backupId is null || started is null)
                    {
                        errors.Add(new(kv.Key, "broken drill status (state/id/backup_id/started_unix)"));
                        continue;
                    }

                    if (!drills.TryGetValue(cluster, out var perShardDrills))
                        drills[cluster] = perShardDrills = [];
                    perShardDrills[segments[4]] = new DrillInfo(
                        cluster, segments[4], id, state, backupId, started.Value,
                        Long(root, "finished_unix"), String(root, "phase"),
                        String(root, "restored_to_lsn"), String(root, "error"));
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken drill JSON: {e.Message}"));
                }

                continue;
            }

            if (segments.Length == 6 && segments[5] == "wal" && segments[4].Length > 0)
            {
                // WAL-статус шарда — state/slot/master_node/last_uploaded_unix
                // обязательны (формат воркера arch/19 §4), lag_segments/error опциональны;
                // незнакомое state / нет обязательных — KeyParseError + пропуск.
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind != JsonValueKind.Object)
                    {
                        errors.Add(new(kv.Key, "wal status is not a JSON object"));
                        continue;
                    }

                    var state = StateOf(String(root, "state"));
                    var unix = Long(root, "last_uploaded_unix");
                    if (state is null || unix is null)
                    {
                        errors.Add(new(kv.Key, "broken wal status (state/last_uploaded_unix)"));
                        continue;
                    }

                    // gap — опциональное поле (толерантно: отсутствует/битое → null,
                    // parseError не пишется — backwards-compat правила канона arch/19 §4).
                    WalGapInfo? gap = null;
                    if (root.TryGetProperty("gap", out var gapEl) && gapEl.ValueKind == JsonValueKind.Object)
                    {
                        var gapFrom = String(gapEl, "from");
                        var gapTo = String(gapEl, "to");
                        var gapTli = Long(gapEl, "tli");
                        if (gapFrom is not null && gapTo is not null && gapTli is not null)
                            gap = new WalGapInfo(gapFrom, gapTo, (uint)gapTli.Value);
                    }

                    if (!wal.TryGetValue(cluster, out var perShardWal))
                        wal[cluster] = perShardWal = [];
                    perShardWal[segments[4]] = new WalStreamInfo(
                        cluster, segments[4], state.Value,
                        String(root, "slot") ?? "",
                        String(root, "master_node") ?? "",
                        unix.Value,
                        Long(root, "lag_segments"),
                        String(root, "error"),
                        String(root, "last_uploaded_segment"),
                        gap);
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken wal JSON: {e.Message}"));
                }

                continue;
            }

            // restore/<id> — заявки восстановления (вход правила restore-failed);
            // state/requested_unix обязательны, error/started/finished/phase — по факту;
            // незнакомое state / нет обязательных — KeyParseError + пропуск.
            if (segments.Length == 7 && segments[5] == "restore" && segments[4].Length > 0 && segments[6].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind != JsonValueKind.Object)
                    {
                        errors.Add(new(kv.Key, "restore status is not a JSON object"));
                        continue;
                    }

                    var state = String(root, "state") switch
                    {
                        "PLANNED" or "RUNNING" or "REJOINING" or "COMPLETED" or "FAILED"
                            => String(root, "state"),
                        _ => null,
                    };
                    var requested = Long(root, "requested_unix");
                    if (state is null || requested is null)
                    {
                        errors.Add(new(kv.Key, "broken restore status (state/requested_unix)"));
                        continue;
                    }

                    if (!restores.TryGetValue(cluster, out var perShardRestores))
                        restores[cluster] = perShardRestores = [];
                    if (!perShardRestores.TryGetValue(segments[4], out var list))
                        perShardRestores[segments[4]] = list = [];
                    list.Add(new RestoreOperationInfo(
                        cluster, segments[4], segments[6], state,
                        String(root, "error"), requested.Value,
                        Long(root, "started_unix"), Long(root, "finished_unix"),
                        String(root, "phase")));
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken restore JSON: {e.Message}"));
                }

                continue;
            }

            if (segments.Length == 7 && segments[5] == "full" && segments[4].Length > 0 && segments[6].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("state", out var state)
                        && state.ValueKind == JsonValueKind.String)
                    {
                        // Ключи бэкапов есть → шард В СЛОВАРЕ даже без COMPLETED:
                        // null = «полного никогда не было» (spec §3.5/§9.6);
                        // молчание правила — только для ПУСТОГО префикса (шарда
                        // нет в словаре вовсе).
                        var perShard = GetOrAdd(shards, cluster);
                        perShard.TryAdd(segments[4], null);

                        // verify-вердикт COMPLETED-полного. FAILED → свежесть
                        // не даёт и попадает в ShardVerifyFailures (последний по
                        // checked_unix); битое verify.state → диагностика + запись
                        // считается непроверенной (валидной); PENDING/OK → валиден.
                        long? checkedUnix = null;
                        string? verifyError = null;
                        string? verifyState = null;
                        var verifyFailed = false;
                        if (root.TryGetProperty("verify", out var verifyEl)
                            && verifyEl.ValueKind == JsonValueKind.Object
                            && verifyEl.TryGetProperty("state", out var verifyStateEl))
                        {
                            checkedUnix = Long(verifyEl, "checked_unix");
                            verifyError = String(verifyEl, "error");
                            verifyState = verifyStateEl.ValueKind == JsonValueKind.String
                                ? verifyStateEl.GetString()
                                : null;
                            verifyFailed = verifyStateEl.ValueKind == JsonValueKind.String
                                           && verifyStateEl.GetString() == "FAILED";
                            if (!verifyFailed && verifyStateEl.ValueKind == JsonValueKind.String
                                && verifyStateEl.GetString() is not ("OK" or "PENDING"))
                                errors.Add(new(kv.Key, "unknown verify.state — verify ignored"));
                        }

                        // полный факт per-full (BackupFullInfo) — state читается
                        // КАК ЕСТЬ (панель — толерантный читатель, писатель — воркер,
                        // незнакомые state'ы терпимы); новой валидации/пути отказа нет.
                        if (!fulls.TryGetValue(cluster, out var perShardFulls))
                            fulls[cluster] = perShardFulls = [];
                        if (!perShardFulls.TryGetValue(segments[4], out var fullList))
                            perShardFulls[segments[4]] = fullList = [];
                        fullList.Add(new BackupFullInfo(
                            segments[6],
                            state.GetString()!,
                            String(root, "error"),
                            Long(root, "started_unix") ?? 0,
                            Long(root, "finished_unix"),
                            Long(root, "size_bytes"),
                            verifyState,
                            checkedUnix,
                            verifyError));

                        if (state.GetString() == "COMPLETED"
                            && root.TryGetProperty("finished_unix", out var finished)
                            && finished.ValueKind == JsonValueKind.Number
                            && finished.TryGetInt64(out var value))
                        {
                            if (verifyFailed)
                            {
                                // проваленный verify НЕ повышает свежесть (AC6-панель)
                                if (!verifyFailures.TryGetValue(cluster, out var perShardFailures))
                                    verifyFailures[cluster] = perShardFailures = [];
                                var candidate = new ShardVerifyFailure(
                                    segments[4], segments[6], verifyError ?? "verify FAILED", checkedUnix);
                                if (!perShardFailures.TryGetValue(segments[4], out var existing)
                                    || (checkedUnix ?? 0) >= (existing.CheckedUnix ?? 0))
                                    perShardFailures[segments[4]] = candidate;
                            }
                            else
                            {
                                var current = perShard[segments[4]];
                                perShard[segments[4]] = current is null || value > current ? value : current;
                            }
                        }

                        // DELETING-полные — вход правила backup-deleting-stuck.
                        // started_unix обязателен по НОВОМУ правилу ретенции
                        // (возраст считается по finished_unix, иначе по нему);
                        // нет/битый → KeyParseError + пропуск записи.
                        if (state.GetString() == "DELETING")
                        {
                            var started = Long(root, "started_unix");
                            if (started is null)
                            {
                                errors.Add(new(kv.Key, "broken DELETING full backup (started_unix required)"));
                            }
                            else
                            {
                                if (!deleting.TryGetValue(cluster, out var perShardDeleting))
                                    deleting[cluster] = perShardDeleting = [];
                                if (!perShardDeleting.TryGetValue(segments[4], out var list))
                                    perShardDeleting[segments[4]] = list = [];
                                list.Add(new DeletingFullInfo(
                                    segments[6], started.Value, Long(root, "finished_unix")));
                            }
                        }
                    }
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"broken full backup JSON: {e.Message}"));
                }
            }
        }

        // Кластер с policy, но без шардов — в списке с пустым словарём
        // (правило по пустому словарю молчит).
        var clusters = shards.Keys
            .Concat(policies.Keys.Where(p => !shards.ContainsKey(p)))
            .Concat(wal.Keys.Where(w => !shards.ContainsKey(w) && !policies.ContainsKey(w)))
            .Concat(restores.Keys.Where(r => !shards.ContainsKey(r) && !policies.ContainsKey(r)
                && !wal.ContainsKey(r)))
            .Concat(drills.Keys.Where(d => !shards.ContainsKey(d) && !policies.ContainsKey(d)
                && !wal.ContainsKey(d) && !restores.ContainsKey(d)))
            .Distinct()
            .OrderBy(c => c, StringComparer.Ordinal)
            .Select(c => new ClusterBackupsInfo(
                c,
                policies.TryGetValue(c, out var age) ? age : null,
                (shards.TryGetValue(c, out var perShard)
                    ? perShard
                    : new Dictionary<string, long?>())
                .ToDictionary(p => p.Key, p => p.Value),
                (wal.TryGetValue(c, out var perShardWal)
                    ? perShardWal
                    : new Dictionary<string, WalStreamInfo?>())
                .ToDictionary(p => p.Key, p => p.Value),
                (deleting.TryGetValue(c, out var perShardDeleting)
                    ? perShardDeleting
                    : new Dictionary<string, List<DeletingFullInfo>>())
                .ToDictionary(p => p.Key, p => (IReadOnlyList<DeletingFullInfo>)p.Value),
                (verifyFailures.TryGetValue(c, out var perShardFailures)
                    ? perShardFailures
                    : new Dictionary<string, ShardVerifyFailure>())
                .ToDictionary(p => p.Key, p => p.Value),
                (restores.TryGetValue(c, out var perShardRestores)
                    ? perShardRestores.ToDictionary(
                        p => p.Key,
                        p => (IReadOnlyList<RestoreOperationInfo>)p.Value
                            .OrderBy(r => r.Id, StringComparer.Ordinal).ToList())
                    : new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>()),
                // etcd-полные per-shard (пусто → null — парсер их не собрал)
                fulls.TryGetValue(c, out var perShardFulls) && perShardFulls.Count > 0
                    ? perShardFulls.ToDictionary(
                        p => p.Key,
                        p => (IReadOnlyList<BackupFullInfo>)p.Value
                            .OrderBy(f => f.Id, StringComparer.Ordinal).ToList())
                    : null,
                // reliability : дриллы per-shard (пусто → null — ключей не было)
                drills.TryGetValue(c, out var perShardDrills) && perShardDrills.Count > 0
                    ? perShardDrills.ToDictionary(p => p.Key, p => p.Value)
                    : null,
                policiesFull.TryGetValue(c, out var policyFull) ? policyFull : null))
            .ToList();
        // реестр сирот собирается после цикла — с джойном hold/заявок
        // (ключи произвольного порядка); orphans-ключа нет/битый → null
        // (hold/заявки без реестра не информативны — только errors).
        orphans = orphansEntries is null
            ? null
            : new BackupOrphansInfo(orphansEntries, orphansUpdated,
                holds.Count > 0 ? holds : null,
                deleteRequests.Count > 0 ? deleteRequests : null);
        return new(clusters, errors, storage, orphans);
    }

    private static WalStreamInfoState? StateOf(string? raw) => raw switch
    {
        "ACTIVE" => WalStreamInfoState.Active,
        "DEGRADED" => WalStreamInfoState.Degraded,
        "STOPPED" => WalStreamInfoState.Stopped,
        "WAL_CHAIN_ERROR" => WalStreamInfoState.WalChainError,
        _ => null,
    };

    private static string? String(JsonElement root, string name)
        => root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    private static long? Long(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && v.ValueKind == JsonValueKind.Number
           && v.TryGetInt64(out var parsed)
            ? parsed
            : null;

    //  (DR-hold): has_valid_full записи сироты; отсутствие/не-bool — null (валидно).
    private static bool? Bool(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && v.ValueKind is JsonValueKind.True or JsonValueKind.False
            ? v.GetBoolean()
            : null;

    // reliability : целочисленные поля policy (days/weeks/months/interval_days).
    // массив строк из секции s3; не-массив/не-строчные элементы → null.
    private static IReadOnlyList<string>? StringListOf(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty(name, out var el)
            || el.ValueKind != JsonValueKind.Array)
            return null;
        var items = new List<string>();
        foreach (var item in el.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
                return null;
            items.Add(item.GetString()!);
        }
        return items;
    }

    private static int? Int32Of(JsonElement root, string name)
        => Long(root, name) is { } v && v is >= int.MinValue and <= int.MaxValue ? (int?)v : null;

    private static double? Double(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && v.ValueKind == JsonValueKind.Number
           && v.TryGetDouble(out var parsed)
            ? parsed
            : null;

    private static Dictionary<string, long?> GetOrAdd(
        Dictionary<string, Dictionary<string, long?>> source, string cluster)
    {
        if (source.TryGetValue(cluster, out var perShard))
            return perShard;
        var created = new Dictionary<string, long?>();
        source[cluster] = created;
        return created;
    }
}
