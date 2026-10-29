using System.Text.Json;
using PgWorker.Core;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.Parsing;

// Парсер префикса /pgworker/backups/ в модель подсистемы бэкапов (arch/19
// §4) по образцу ClusterSnapshotParser: чистые функции Kv[] → модель;
// битые значения — в parseErrors с пропуском записи (не исключение), неизвестные
// ключи — игнор (обратная совместимость). Подключение к снапшоту воркера —
// чтение без изменений поведения (потребители — +).
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

                // "/pgworker/backups/<C>/<X>/restore/<id>" 
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

                // "/pgworker/backups/<C>/<X>/drill" (reliability)
                case 6 when segments[4].Length > 0 && segments[5] == "drill":
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc()).DrillRaw = kv.Value;
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

        public string? DrillRaw;
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
                        .ToList(),
                    TryParseDrill(acc.Name, pair.Key, pair.Value.DrillRaw, errors)));
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
                errors.Add($"/pgworker/backups/{cluster}/policy: value is not a JSON object");
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

            // период перепроверки — verify.interval_sec policy-ключа
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

            // reliability : период дрилов — drill.interval_days policy-ключа
            // (перекрывает дефолт конфига); отсутствие/невалидное → null.
            int? drillIntervalDays = null;
            if (root.TryGetProperty("drill", out var drill)
                && drill.ValueKind == JsonValueKind.Object
                && drill.TryGetProperty("interval_days", out var drillDays)
                && drillDays.ValueKind == JsonValueKind.Number
                && drillDays.TryGetInt32(out var daysValue))
                drillIntervalDays = daysValue;

            // возрастной гейт мусора супервизора — garbage_min_age_sec
            // policy-ключа (перекрывает дефолт конфига); отсутствие/невалидное →
            // null (дефолт — потребитель); 0 — гейт снят.
            long? garbageMinAgeSec = null;
            if (root.TryGetProperty("garbage_min_age_sec", out var garbage)
                && garbage.ValueKind == JsonValueKind.Number
                && garbage.TryGetInt64(out var garbageValue))
                garbageMinAgeSec = garbageValue;

            // секция s3 — пул + списки по роли источника. Отсутствие/битая
            // секция = пул не выбран — как drill, толерантность парсеров
            // (arch/19 §4): parseError не валид, S3=null, policy читается.
            var s3 = TryParsePolicyS3(root);

            return new BackupPolicy(
                days, weeks, months,
                ReadLong(root, "full_max_age_sec") ?? 86400,
                verifyOnCreate, verifyIntervalSec, drillIntervalDays, garbageMinAgeSec, s3);
        }
        catch (JsonException)
        {
            errors.Add($"/pgworker/backups/{cluster}/policy: broken JSON");
            return null;
        }
    }

    // Секция s3 (arch/19 §4): pool — непустая строка, списки — массивы
    // строк; любое несоответствие формы → null (пул не выбран).
    private static BackupPolicyS3? TryParsePolicyS3(JsonElement root)
    {
        if (!root.TryGetProperty("s3", out var s3) || s3.ValueKind != JsonValueKind.Object)
            return null;

        var pool = ReadString(s3, "pool");
        if (string.IsNullOrEmpty(pool))
            return null;

        var master = ReadStringList(s3, "write_master");
        var replica = ReadStringList(s3, "write_replica");
        if (master is null || replica is null)
            return null;

        return new BackupPolicyS3(pool, master, replica);
    }

    // Массив строк из секции s3; не-массив/не-строчные элементы → null.
    private static IReadOnlyList<string>? ReadStringList(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty(name, out var element)
            || element.ValueKind != JsonValueKind.Array)
            return null;

        var items = new List<string>();
        foreach (var item in element.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
                return null;
            items.Add(item.GetString()!);
        }

        return items;
    }

    // full/<id>: обязательны state/node/role/started_unix; wal_start_segment —
    // с фазы UPLOADING (§4), до этого null; неизвестное state/role —
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
                errors.Add($"{key}: broken JSON or unknown state/role, mandatory field missing");
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
                    errors.Add($"{key}: unknown verify.state — verify skipped");
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
            errors.Add($"{key}: broken JSON");
            return null;
        }
    }

    // restore/<id> : обязательны state (PLANNED|RUNNING|REJOINING|COMPLETED|
    // FAILED)/backup_id/source/target/node/requested_unix/requested_by;
    // опциональны started_unix/finished_unix/phase/phase_unix/restored_to_lsn/
    // error; битое/неизвестное state — пропуск записи с диагностикой.
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
            // backup_id опционален (§3.5): PLANNED-заявка API/E2E может не
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
                errors.Add($"{key}: broken JSON or unknown state, mandatory field missing");
                return null;
            }

            return new RestoreOperationState(
                id, state.Value, backupId, source, target, node,
                requestedUnix.Value, requestedBy,
                ReadLong(root, "started_unix"), ReadLong(root, "finished_unix"),
                ReadString(root, "phase"), ReadString(root, "restored_to_lsn"),
                ReadString(root, "error"),
                // пул DR-источника (arch/14 §1.1); отсутствие — свой пул.
                ReadString(root, "source_pool"))
            {
                SystemId = ReadString(root, "system_id"),
                // Unix перехода в текущую фазу (пишет воркер): стенка наката
                // restore — от перехода в recovering, не от старта заявки.
                PhaseUnix = ReadLong(root, "phase_unix"),
            };
        }
        catch (JsonException)
        {
            errors.Add($"{key}: broken JSON");
            return null;
        }
    }

    // drill (reliability): обязательны state (RUNNING|SUCCEEDED|FAILED)/id/
    // backup_id/started_unix; опциональны finished_unix/phase/restored_to_lsn/
    // error; битое/неизвестное state → error + null (шард жив, ключа нет).
    private static DrillState? TryParseDrill(
        string cluster, string shard, string? raw, List<string> errors)
    {
        if (raw is null)
            return null; // нет ключа — дрилов не было, не ошибка

        var key = $"/pgworker/backups/{cluster}/{shard}/drill";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "RUNNING" => DrillStatus.Running,
                "SUCCEEDED" => DrillStatus.Succeeded,
                "FAILED" => DrillStatus.Failed,
                _ => (DrillStatus?)null,
            };
            var id = ReadString(root, "id");
            var backupId = ReadString(root, "backup_id");
            var startedUnix = ReadLong(root, "started_unix");
            if (state is null || string.IsNullOrEmpty(id) || string.IsNullOrEmpty(backupId)
                || startedUnix is null)
            {
                errors.Add($"{key}: broken JSON or unknown state, mandatory field missing");
                return null;
            }

            return new DrillState(
                id, state.Value, backupId, startedUnix.Value,
                ReadLong(root, "finished_unix"), ReadString(root, "phase"),
                ReadString(root, "restored_to_lsn"), ReadString(root, "error"));
        }
        catch (JsonException)
        {
            errors.Add($"{key}: broken JSON");
            return null;
        }
    }

    // wal: обязательны state/slot/master_node/цепочка сегментов/last_uploaded_unix;
    // опциональны lag_segments/error.
    private static WalStreamState? TryParseWal(
        string cluster, string shard, string? raw, List<string> errors)
    {
        if (raw is null)
            return null; // нет ключа — агент не поднимался, не ошибка

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
                //  (arch/19 §4): WAL_CHAIN_ERROR обязан читаться СНАПШОТНЫМ
                // парсером. Прогон 2026-09-13: неизвестное состояние разрыва
                // давало Wal=null → контроль шёл от ratchet=null (min COMPLETED,
                // ниже границы разрыва), дыра «вечно свежая», ключ замерал в
                // разрыве, планировщик штормовал пересъёмами
                // (walKeyExists=false). Интеграционные тесты это
                // пропустили: они читают ключ через WalStatusWriter.ReadAsync
                // (там состояние разрыва добавлено), а не через снапшотный
                // BackupsParser.
                "WAL_CHAIN_ERROR" => WalStreamStatus.WalChainError,
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
                errors.Add($"{key}: broken JSON or unknown state, mandatory field missing");
                return null;
            }

            //  (arch/19 §4): границы RPO-зазора tli-прыжка — обязательны для
            // СНАПШОТНОГО чтения (планировщик переснимает полный по
            // walGapUncovered из снапшота, arch/19 §2). Прогон 
            // (2026-10-27): снапшотный парсер Gap терял — живой непокрытый
            // зазор для планировщика не существовал, пересъём не планировался
            // (wal-ключ вечно DEGRADED+gap), а WalStreamProcess (свой парсер)
            // метку удерживал — расхождение «контроль видит gap, планировщик
            // нет».
            WalGapState? gap = null;
            if (root.TryGetProperty("gap", out var gapEl)
                && gapEl.ValueKind == JsonValueKind.Object)
            {
                var from = ReadString(gapEl, "from");
                var to = ReadString(gapEl, "to");
                var tli = ReadLong(gapEl, "tli");
                if (from is { Length: > 0 } && to is { Length: > 0 } && tli is { } gapTli)
                    gap = new WalGapState(from, to, (uint)gapTli);
            }

            return new WalStreamState(
                state.Value, slot, masterNode, chainStart, lastReceived,
                lastUploaded, lastUploadedUnix,
                ReadLong(root, "lag_segments"), ReadString(root, "error"),
                Gap: gap);
        }
        catch (JsonException)
        {
            errors.Add($"{key}: broken JSON");
            return null;
        }
    }

    // Толерантное чтение полей JSON-значений: строки-числа, отсутствующие поля
    // (копия хелперов ClusterSnapshotParser — общие утилиты не рефакторим в).
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
