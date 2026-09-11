using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

public sealed record BackupsParseResult(
    IReadOnlyList<ClusterBackupsInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    // t06: глобальный ключ /pgworker/backups/storage (null — ключа нет/битый).
    BackupStorageInfo? Storage = null);

// Чистая функция: KV префикса /pgworker/backups/ (arch/19 §4, t02+t03):
// policy full_max_age_sec + per-shard последний COMPLETED finished_unix
// (правило backup-full-stale, t02) + WAL-статусы шардов (правила
// wal-chain-broken/wal-stream-lag/wal-stream-stopped, t03). Битые значения —
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
        // | "/pgworker/backups/<C>/<X>/wal" (t03)
        var policies = new Dictionary<string, long?>();
        var shards = new Dictionary<string, Dictionary<string, long?>>();
        var wal = new Dictionary<string, Dictionary<string, WalStreamInfo?>>();
        var deleting = new Dictionary<string, Dictionary<string, List<DeletingFullInfo>>>();
        BackupStorageInfo? storage = null;
        var verifyFailures = new Dictionary<string, Dictionary<string, ShardVerifyFailure>>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');

            // Глобальный ключ /pgworker/backups/storage (t06): ДО гварда длины —
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
                        errors.Add(new(kv.Key, "битый storage-статус (used_bytes/updated_unix/state)"));
                    else
                        storage = new BackupStorageInfo(
                            used.Value, Long(root, "quota_bytes"), Double(root, "used_percent"),
                            state.Value, updated.Value);
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON storage: {e.Message}"));
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
                    policies[cluster] = doc.RootElement.ValueKind == JsonValueKind.Object
                        && doc.RootElement.TryGetProperty("full_max_age_sec", out var age)
                        && age.ValueKind == JsonValueKind.Number
                        && age.TryGetInt64(out var value)
                        ? value
                        : null;
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON policy: {e.Message}"));
                }

                continue;
            }

            if (segments.Length == 6 && segments[5] == "wal" && segments[4].Length > 0)
            {
                // t03: WAL-статус шарда — state/slot/master_node/last_uploaded_unix
                // обязательны (формат воркера arch/19 §4), lag_segments/error опциональны;
                // незнакомое state / нет обязательных — KeyParseError + пропуск.
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind != JsonValueKind.Object)
                    {
                        errors.Add(new(kv.Key, "wal-статус не JSON-объект"));
                        continue;
                    }

                    var state = StateOf(String(root, "state"));
                    var unix = Long(root, "last_uploaded_unix");
                    if (state is null || unix is null)
                    {
                        errors.Add(new(kv.Key, "битый wal-статус (state/last_uploaded_unix)"));
                        continue;
                    }

                    if (!wal.TryGetValue(cluster, out var perShardWal))
                        wal[cluster] = perShardWal = [];
                    perShardWal[segments[4]] = new WalStreamInfo(
                        cluster, segments[4], state.Value,
                        String(root, "slot") ?? "",
                        String(root, "master_node") ?? "",
                        unix.Value,
                        Long(root, "lag_segments"),
                        String(root, "error"));
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON wal: {e.Message}"));
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

                        // t04: verify-вердикт COMPLETED-полного. FAILED → свежесть
                        // не даёт и попадает в ShardVerifyFailures (последний по
                        // checked_unix); битое verify.state → диагностика + запись
                        // считается непроверенной (валидной); PENDING/OK → валиден.
                        long? checkedUnix = null;
                        string? verifyError = null;
                        var verifyFailed = false;
                        if (root.TryGetProperty("verify", out var verifyEl)
                            && verifyEl.ValueKind == JsonValueKind.Object
                            && verifyEl.TryGetProperty("state", out var verifyStateEl))
                        {
                            checkedUnix = Long(verifyEl, "checked_unix");
                            verifyError = String(verifyEl, "error");
                            verifyFailed = verifyStateEl.ValueKind == JsonValueKind.String
                                           && verifyStateEl.GetString() == "FAILED";
                            if (!verifyFailed && verifyStateEl.ValueKind == JsonValueKind.String
                                && verifyStateEl.GetString() is not ("OK" or "PENDING"))
                                errors.Add(new(kv.Key, "неизвестное verify.state — verify игнор"));
                        }

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

                        // t06: DELETING-полные — вход правила backup-deleting-stuck.
                        // started_unix обязателен по НОВОМУ правилу ретенции
                        // (возраст считается по finished_unix, иначе по нему);
                        // нет/битый → KeyParseError + пропуск записи.
                        if (state.GetString() == "DELETING")
                        {
                            var started = Long(root, "started_unix");
                            if (started is null)
                            {
                                errors.Add(new(kv.Key, "битый DELETING-полный (started_unix обязателен)"));
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
                    errors.Add(new(kv.Key, $"битый JSON full: {e.Message}"));
                }
            }
        }

        // Кластер с policy, но без шардов — в списке с пустым словарём
        // (правило по пустому словарю молчит).
        var clusters = shards.Keys
            .Concat(policies.Keys.Where(p => !shards.ContainsKey(p)))
            .Concat(wal.Keys.Where(w => !shards.ContainsKey(w) && !policies.ContainsKey(w)))
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
                .ToDictionary(p => p.Key, p => p.Value)))
            .ToList();
        return new(clusters, errors, storage);
    }

    private static WalStreamInfoState? StateOf(string? raw) => raw switch
    {
        "ACTIVE" => WalStreamInfoState.Active,
        "DEGRADED" => WalStreamInfoState.Degraded,
        "STOPPED" => WalStreamInfoState.Stopped,
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
