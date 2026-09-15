using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using PgWorker.Backups;
using PgWorker.Backups.Restore;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Writing;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.App.Api.Operations;

// POST /api/clusters/{c}/shards/{x}/restore (t05, arch/19 §3.5/§4): заявка
// восстановления шарда из бэкапа. Хендлер ТОЛЬКО валидирует и пишет
// PLANNED-ключ (put-if-not-exists, txn 409 при гонке) — исполняет воркер,
// держатель клэйма (RestoreProcess). Гварды: confirm = имя шарда (необратимая
// операция), Active-кластер, шард в декларации, RFC3339-цель, source-пара,
// максимум один активный restore на шард.
public sealed partial class RestoreShardHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardPattern();

    public async Task<Result<RestoreRequestedDto>> HandleAsync(
        string cluster, string shard, RestoreShardRequest? body, string requestedBy, CancellationToken ct)
    {
        // 1) Канонические имена.
        if (!ClusterPattern().IsMatch(cluster) || !ShardPattern().IsMatch(shard))
            return Result<RestoreRequestedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Гварды декларации: config → 404/503/409; шард заявлен (есть ноды).
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<RestoreRequestedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<RestoreRequestedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<RestoreRequestedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<RestoreRequestedDto>.Failed(new ClusterNotActiveException(cluster, state));
        if (!info.Shards.Contains(shard) || !info.NodeStates.Keys.Any(k =>
                k.StartsWith($"{shard}/", StringComparison.Ordinal)))
            return Result<RestoreRequestedDto>.Failed(new ShardNotFoundException(cluster, shard));

        // 3) confirm обязан совпадать с именем шарда (необратимая операция).
        if (body?.Confirm != shard)
            return Result<RestoreRequestedDto>.Failed(new ConfirmMismatchException(shard, body?.Confirm ?? ""));

        // 4) Цель: latest | time:<RFC3339>.
        string target;
        if (string.IsNullOrWhiteSpace(body.TargetTime))
        {
            target = "latest";
        }
        else if (DateTimeOffset.TryParseExact(body.TargetTime,
                     ["yyyy-MM-ddTHH:mm:ssK", "yyyy-MM-ddTHH:mm:ss.fffffffK"],
                     CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                     out _))
        {
            target = $"time:{body.TargetTime}";
        }
        else
        {
            return Result<RestoreRequestedDto>.Failed(new InvalidTargetTimeException(body.TargetTime));
        }

        // 5) Source: пара или отсутствие (по одному — ошибка валидации по полям).
        var hasSourceCluster = !string.IsNullOrWhiteSpace(body.SourceCluster);
        var hasSourceShard = !string.IsNullOrWhiteSpace(body.SourceShard);
        string source;
        if (hasSourceCluster != hasSourceShard)
        {
            var errors = new List<ValidationError>();
            if (!hasSourceCluster)
                errors.Add(new ValidationError("source_cluster", "указан source_shard без source_cluster"));
            if (!hasSourceShard)
                errors.Add(new ValidationError("source_shard", "указан source_cluster без source_shard"));
            return Result<RestoreRequestedDto>.Failed(new RestoreValidationException(errors));
        }
        source = hasSourceCluster ? $"{body.SourceCluster}/{body.SourceShard}" : $"{cluster}/{shard}";

        // 6) Гвард «максимум один активный restore на шард» (§3.1) + существующие
        //    id (NextId суффиксирует коллизию в ту же секунду).
        var range = await EtcdFailover.CallAsync(endpoints,
            e => gateway.RangeAsync(e, $"/pgworker/backups/{cluster}/{shard}/restore/", ct));
        if (!range.IsSuccess)
            return Result<RestoreRequestedDto>.Failed(range.Error!);
        var existingIds = new List<string>();
        foreach (var kv in range.Value)
        {
            existingIds.Add(kv.Key.Split('/')[^1]);
            if (IsRestoreActive(kv.Value))
                return Result<RestoreRequestedDto>.Failed(new RestoreAlreadyActiveException(cluster, shard));
        }

        var id = BackupPlanner.NextId(existingIds, time.GetUtcNow().UtcDateTime);

        // 7) PLANNED-ключ: node — первая нода декларации шарда; put-if-not-exists
        //    (txn) — гонка двух API-запросов → проигравший получает 409.
        var node = info.NodeStates.Keys.Where(k => k.StartsWith($"{shard}/", StringComparison.Ordinal))
            .Select(k => k.Split('/')[^1])
            .OrderBy(n => n, StringComparer.Ordinal)
            .First();
        var op = new RestoreOperationState(id, RestoreStatus.Planned, body.BackupId ?? "",
            source, target, node, time.GetUtcNow().ToUnixTimeSeconds(), requestedBy);
        var key = BackupNames.RestoreKey(cluster, shard, id);
        var txn = await EtcdFailover.CallAsync(endpoints,
            e => gateway.TxnAsync(e,
                TxnRequest.Of([TxnCompare.NotExists(key)],
                    [new TxnOp.Put(key, RestoreStatusJson.Serialize(op), null)]), ct));
        if (!txn.IsSuccess)
            return Result<RestoreRequestedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<RestoreRequestedDto>.Failed(new RestoreAlreadyActiveException(cluster, shard));

        return Result<RestoreRequestedDto>.Success(
            new RestoreRequestedDto(cluster, shard, id, "PLANNED", target, source));
    }

    // config без state (или state:"ACTIVE"-пустота) = Active — как RecreateNodeHandler.
    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }

    // Активный (не COMPLETED/FAILED) restore в статусе; битый JSON — не активен
    // (валидацию такого ключа делает воркер, гвард API — только дубли).
    private static bool IsRestoreActive(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("state", out var s)
                   && s.ValueKind == JsonValueKind.String
                   && s.GetString() is "PLANNED" or "RUNNING" or "REJOINING";
        }
        catch (JsonException)
        {
            return false;
        }
    }
}

/// <summary>Тело заявки restore (t05 §3.2); JSON-поля snake_case.</summary>
public sealed record RestoreShardRequest(
    [property: JsonPropertyName("backup_id")] string? BackupId,
    [property: JsonPropertyName("target_time")] string? TargetTime,
    [property: JsonPropertyName("source_cluster")] string? SourceCluster,
    [property: JsonPropertyName("source_shard")] string? SourceShard,
    [property: JsonPropertyName("confirm")] string Confirm,
    [property: JsonPropertyName("requested_by")] string? RequestedBy);

/// <summary>Ответ 202: заявка принята (PLANNED), воркер исполнит.</summary>
public sealed record RestoreRequestedDto(
    [property: JsonPropertyName("cluster")] string Cluster,
    [property: JsonPropertyName("shard")] string Shard,
    [property: JsonPropertyName("restore_id")] string RestoreId,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("target")] string Target,
    [property: JsonPropertyName("source")] string Source);
