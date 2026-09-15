using System.Text.Json;
using PgWorker.Core;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.Parsing;

// Парсер префикса /pgworker/backups/ в модель подсистемы бэкапов (arch/19
// §4, t01) по образцу ClusterSnapshotParser: чистые функции Kv[] → модель;
// битые значения — в parseErrors с пропуском записи (не исключение), неизвестные
// ключи — игнор (обратная совместимость). Подключение к снапшоту воркера —
// чтение без изменений поведения (потребители — t02+).
public static class BackupsParser
{
    public static Result<IReadOnlyList<ClusterBackups>> Parse(
        IReadOnlyList<Kv> kvs, out IReadOnlyList<string> parseErrors)
    {
        var errors = new List<string>();
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/pgworker/backups/<C>/…" → ["", "pgworker", "backups", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5
                || segments[1] != "pgworker"
                || segments[2] != "backups"
                || segments[3].Length == 0)
            {
                continue; // чужой префикс (в т.ч. /clusters/) — не наша забота
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments.Length)
            {
                // "/pgworker/backups/<C>/policy"
                case 5 when segments[4] == "policy":
                    acc.PolicyRaw = kv.Value;
                    break;

                // "/pgworker/backups/<C>/<X>/full/<id>"
                case 7 when segments[4].Length > 0
                    && segments[5] == "full"
                    && segments[6].Length > 0:
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc())
                        .Fulls.Add((segments[6], kv.Value));
                    break;

                // "/pgworker/backups/<C>/<X>/restore/<id>" (t05)
                case 7 when segments[4].Length > 0
                    && segments[5] == "restore"
                    && segments[6].Length > 0:
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc())
                        .Restores.Add((segments[6], kv.Value));
                    break;

                // "/pgworker/backups/<C>/<X>/wal"
                case 6 when segments[4].Length > 0 && segments[5] == "wal":
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc()).WalRaw = kv.Value;
                    break;

                default:
                    // система развивается — неизвестный ключ не ошибка, просто игнор
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(a => a.Name, StringComparer.Ordinal)
            .Select(a => BuildCluster(a, errors))
            .ToList();

        parseErrors = errors;
        return Result<IReadOnlyList<ClusterBackups>>.Success(clusters);
    }

    private sealed class ShardAcc
    {
        public readonly List<(string Id, string Raw)> Fulls = [];

        public readonly List<(string Id, string Raw)> Restores = [];

        public string? WalRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;

        public string? PolicyRaw;

        public readonly Dictionary<string, ShardAcc> Shards = [];
    }

    private static ClusterBackups BuildCluster(ClusterAcc acc, List<string> errors)
    {
        var policy = TryParsePolicy(acc.Name, acc.PolicyRaw, errors);
        var shards = acc.Shards
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToDictionary(
                pair => pair.Key,
                pair => new ShardBackups(
                    pair.Value.Fulls
                        .Select(f => TryParseFull(acc.Name, pair.Key, f.Id, f.Raw, errors))
                        .Where(f => f is not null)
                        .Select(f => f!)
                        .OrderBy(f => f.Id, StringComparer.Ordinal)
                        .ToList(),
                    TryParseWal(acc.Name, pair.Key, pair.Value.WalRaw, errors),
                    pair.Value.Restores
                        .Select(r => TryParseRestore(acc.Name, pair.Key, r.Id, r.Raw, errors))
                        .Where(r => r is not null)
                        .Select(r => r!)
                        .OrderBy(r => r.Id, StringComparer.Ordinal)
                        .ToList()));
        return new ClusterBackups(acc.Name, policy, shards);
    }

    // policy: отсутствует → null без ошибки (дефолт — PgWorker:Backups:Policy);
    // отсутствующие поля — дефолты канона (толерантность будущих версий).
    private static BackupPolicy? TryParsePolicy(string cluster, string? raw, List<string> errors)
    {
        if (raw is null)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                // валидный JSON-не-объект (число/строка/массив) — не исключение,
                // а толерантный пропуск записи (TryGetProperty вне объекта бросает)
                errors.Add($"/pgworker/backups/{cluster}/policy: значение не JSON-объект");
                return null;
            }

            var days = 7;
            var weeks = 4;
            var months = 6;
            if (root.TryGetProperty("retention", out var retention)
                && retention.ValueKind == JsonValueKind.Object)
            {
                days = (int?)ReadLong(retention, "days") ?? days;
                weeks = (int?)ReadLong(retention, "weeks") ?? weeks;
                months = (int?)ReadLong(retention, "months") ?? months;
            }

            var verifyOnCreate = true;
            if (root.TryGetProperty("verify", out var verify)
                && verify.ValueKind == JsonValueKind.Object
                && verify.TryGetProperty("on_create", out var flag))
                verifyOnCreate = flag.ValueKind is JsonValueKind.True or JsonValueKind.String
                    && flag.ToString() is "true" or "True";

            // t04: период перепроверки — verify.interval_sec policy-ключа
            // (перекрывает дефолт конфига); отсутствие → null (дефолт — потребитель).
            long? verifyIntervalSec = null;
            if (root.TryGetProperty("verify", out var verifyObj)
                && verifyObj.ValueKind == JsonValueKind.Object
                && verifyObj.TryGetProperty("interval_sec", out var interval))
                verifyIntervalSec = interval.ValueKind is JsonValueKind.Number && interval.TryGetInt64(out var sec)
                    ? sec
                    : interval.ValueKind == JsonValueKind.String && long.TryParse(interval.GetString(), out var parsed)
                        ? parsed
                        : null;

            return new BackupPolicy(
                days, weeks, months,
                ReadLong(root, "full_max_age_sec") ?? 86400,
                verifyOnCreate, verifyIntervalSec);
        }
        catch (JsonException)
        {
            errors.Add($"/pgworker/backups/{cluster}/policy: битый JSON");
            return null;
        }
    }

    // full/<id>: обязательны state/node/role/started_unix; wal_start_segment —
    // с фазы UPLOADING (§4, t02), до этого null; неизвестное state/role —
    // пропуск записи с диагностикой; verify — опциональный (битный
    // verify.state → Verify=null, запись жива).
    private static FullBackupState? TryParseFull(
        string cluster, string shard, string id, string raw, List<string> errors)
    {
        var key = $"/pgworker/backups/{cluster}/{shard}/full/{id}";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "PLANNED" => FullBackupStatus.Planned,
                "RUNNING" => FullBackupStatus.Running,
                "UPLOADING" => FullBackupStatus.Uploading,
                "COMPLETED" => FullBackupStatus.Completed,
                "FAILED" => FullBackupStatus.Failed,
                "DELETING" => FullBackupStatus.Deleting,
                _ => (FullBackupStatus?)null,
            };
            var role = ReadString(root, "role") switch
            {
                "replica" => BackupSourceRole.Replica,
                "master" => BackupSourceRole.Master,
                _ => (BackupSourceRole?)null,
            };
            var startedUnix = ReadLong(root, "started_unix");
            var node = ReadString(root, "node");
            if (state is null || role is null || startedUnix is null || string.IsNullOrEmpty(node))
            {
                errors.Add($"{key}: битый JSON или неизвестное state/role, обязательное поле отсутствует");
                return null;
            }

            BackupVerify? verify = null;
            if (root.TryGetProperty("verify", out var verifyEl)
                && verifyEl.ValueKind == JsonValueKind.Object)
            {
                var verifyState = ReadString(verifyEl, "state") switch
                {
                    "PENDING" => BackupVerifyStatus.Pending,
                    "OK" => BackupVerifyStatus.Ok,
                    "FAILED" => BackupVerifyStatus.Failed,
                    _ => (BackupVerifyStatus?)null,
                };
                if (verifyState is null)
                    errors.Add($"{key}: неизвестное verify.state — verify пропущен");
                else
                    verify = new BackupVerify(verifyState.Value, ReadLong(verifyEl, "checked_unix"), ReadString(verifyEl, "error"));
            }

            return new FullBackupState(
                id, state.Value, node, role.Value, startedUnix.Value,
                ReadLong(root, "finished_unix"), ReadString(root, "wal_start_segment"),
                ReadLong(root, "size_bytes"), ReadString(root, "error"), verify);
        }
        catch (JsonException)
        {
            errors.Add($"{key}: битый JSON");
            return null;
        }
    }

    // restore/<id> (t05): обязательны state (PLANNED|RUNNING|REJOINING|COMPLETED|
    // FAILED)/backup_id/source/target/node/requested_unix/requested_by;
    // опциональны started_unix/finished_unix/phase/restored_to_lsn/error;
    // битое/неизвестное state — пропуск записи с диагностикой.
    private static RestoreOperationState? TryParseRestore(
        string cluster, string shard, string id, string raw, List<string> errors)
    {
        var key = $"/pgworker/backups/{cluster}/{shard}/restore/{id}";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "PLANNED" => RestoreStatus.Planned,
                "RUNNING" => RestoreStatus.Running,
                "REJOINING" => RestoreStatus.Rejoining,
                "COMPLETED" => RestoreStatus.Completed,
                "FAILED" => RestoreStatus.Failed,
                _ => (RestoreStatus?)null,
            };
            // backup_id опционален (t05 §3.5): PLANNED-заявка API/E2E может не
            // указывать полный — воркер-исполнитель резолвит новейший COMPLETED
            // (etcd → S3-list); RUNNING/финальные статусы пишут его всегда.
            var backupId = ReadString(root, "backup_id") ?? "";
            var source = ReadString(root, "source");
            var target = ReadString(root, "target");
            var node = ReadString(root, "node");
            var requestedUnix = ReadLong(root, "requested_unix");
            var requestedBy = ReadString(root, "requested_by");
            if (state is null || string.IsNullOrEmpty(source)
                || string.IsNullOrEmpty(target) || string.IsNullOrEmpty(node)
                || requestedUnix is null || string.IsNullOrEmpty(requestedBy))
            {
                errors.Add($"{key}: битый JSON или неизвестное state, обязательное поле отсутствует");
                return null;
            }

            return new RestoreOperationState(
                id, state.Value, backupId, source, target, node,
                requestedUnix.Value, requestedBy,
                ReadLong(root, "started_unix"), ReadLong(root, "finished_unix"),
                ReadString(root, "phase"), ReadString(root, "restored_to_lsn"),
                ReadString(root, "error"))
            {
                SystemId = ReadString(root, "system_id"),
            };
        }
        catch (JsonException)
        {
            errors.Add($"{key}: битый JSON");
            return null;
        }
    }

    // wal: обязательны state/slot/master_node/цепочка сегментов/last_uploaded_unix;
    // опциональны lag_segments/error.
    private static WalStreamState? TryParseWal(
        string cluster, string shard, string? raw, List<string> errors)
    {
        if (raw is null)
            return null; // нет ключа — агент не поднимался (t03), не ошибка

        var key = $"/pgworker/backups/{cluster}/{shard}/wal";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "ACTIVE" => WalStreamStatus.Active,
                "DEGRADED" => WalStreamStatus.Degraded,
                "STOPPED" => WalStreamStatus.Stopped,
                // t07 (arch/19 §4): BROKEN обязан читаться СНАПШОТНЫМ парсером.
                // Прогон 2026-09-13: неизвестное BROKEN давало Wal=null → контроль
                // шёл от ratchet=null (min COMPLETED, ниже границы разрыва), дыра
                // «вечно свежая», ключ замерал в BROKEN, планировщик штормовал
                // пересъёмами (walKeyExists=false). Интеграционные тесты t07 это
                // пропустили: они читают ключ через WalStatusWriter.ReadAsync
                // (там BROKEN добавлен), а не через снапшотный BackupsParser.
                "BROKEN" => WalStreamStatus.Broken,
                _ => (WalStreamStatus?)null,
            };
            var slot = ReadString(root, "slot");
            var masterNode = ReadString(root, "master_node");
            var chainStart = ReadString(root, "chain_start_segment");
            var lastReceived = ReadString(root, "last_received_segment");
            var lastUploaded = ReadString(root, "last_uploaded_segment");
            var lastUploadedUnix = ReadLong(root, "last_uploaded_unix");
            if (state is null || string.IsNullOrEmpty(slot) || string.IsNullOrEmpty(masterNode)
                || string.IsNullOrEmpty(chainStart) || string.IsNullOrEmpty(lastReceived)
                || string.IsNullOrEmpty(lastUploaded) || lastUploadedUnix is null)
            {
                errors.Add($"{key}: битый JSON или неизвестное state, обязательное поле отсутствует");
                return null;
            }

            return new WalStreamState(
                state.Value, slot, masterNode, chainStart, lastReceived,
                lastUploaded, lastUploadedUnix,
                ReadLong(root, "lag_segments"), ReadString(root, "error"));
        }
        catch (JsonException)
        {
            errors.Add($"{key}: битый JSON");
            return null;
        }
    }

    // Толерантное чтение полей JSON-значений: строки-числа, отсутствующие поля
    // (копия хелперов ClusterSnapshotParser — общие утилиты не рефакторим в t01).
    private static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String or JsonValueKind.Number
            ? element.ToString()
            : null;

    private static long? ReadLong(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(name, out var element))
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.Number => element.TryGetInt64(out var value) ? value : null,
            JsonValueKind.String when long.TryParse(element.GetString(), out var value) => value,
            _ => null,
        };
    }

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
