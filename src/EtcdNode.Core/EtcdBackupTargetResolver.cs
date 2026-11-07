using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdNode.Core;

/// <summary>Цель выгрузки DR-слепков (arch/etcdnode/02-snapshots.md §1): bucket — константа
/// EtcdSnapshotBucket.Name; endpoint'ы/креды — живой резолв из /s3/clusters/
/// (единственный кластер с etcd_backup=true). Все отказы — маркер
/// etcd-backup-missing в сообщении (алерт панели, transient для цикла).</summary>
public sealed record EtcdBackupTarget(
    IReadOnlyList<string> Endpoints, string AccessKey, string SecretKey)
{
    public string Bucket => Shared.Core.EtcdSnapshotBucket.Name;
}

/// <summary>Маркер недоступности цели (литерал — панельный алерт-признак).</summary>
public static class EtcdBackupMarkers
{
    public const string Missing = "etcd-backup-missing";
}

public sealed class EtcdBackupTargetResolver(IEtcdGateway gateway, string[] endpoints)
{
    private const string ClustersPrefix = "/s3/clusters/";

    public async Task<Result<EtcdBackupTarget>> ResolveAsync(CancellationToken ct)
    {
        var range = await RangeWithFailoverAsync(ClustersPrefix, ct);
        if (!range.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(range.Error!);

        // Единственный etcd_backup=true (гвард создания у S3Worker); первый по
        // Ordinal — детерминизм.
        string? cluster = null;
        foreach (var kv in range.Value.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[^1] != "config")
                continue;
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && doc.RootElement.TryGetProperty("etcd_backup", out var flag)
                    && flag.ValueKind == JsonValueKind.True)
                {
                    cluster = segments[3];
                    break;
                }
            }
            catch (JsonException)
            {
                // битый config — пропустить
            }
        }

        if (cluster is null)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: no /s3/clusters/ config with etcd_backup=true"));

        var config = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(config.Error!);
        if (config.Value is { } cfg)
        {
            try
            {
                using var doc = JsonDocument.Parse(cfg.Value);
                if (doc.RootElement.TryGetProperty("state", out var st)
                    && st.ValueKind == JsonValueKind.String
                    && st.GetString() == "TO_REMOVE")
                    return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                        $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} is in TO_REMOVE state"));
            }
            catch (JsonException)
            {
                // битый config не должен был попасть в отбор — прозрачно пропускаем
            }
        }

        var endpointsKv = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/endpoints", ct);
        if (!endpointsKv.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(endpointsKv.Error!);
        var endpointList = endpointsKv.Value is { } ep
            ? ParseEndpoints(ep.Value)
            : [];
        if (endpointList.Count == 0)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} has no readable endpoints"));

        var secretsKv = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/secrets", ct);
        if (!secretsKv.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(secretsKv.Error!);
        var secrets = secretsKv.Value is { } sec ? ParseBackupsSecrets(sec.Value) : null;
        if (secrets is null)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} has broken secrets (key backups)"));

        return Result<EtcdBackupTarget>.Success(new EtcdBackupTarget(
            endpointList.Select(e => $"http://{e}").ToList(), secrets.Value.Access, secrets.Value.Secret));
    }

    // "h1:p1,h2:p2" → ["h1:p1",...] (битые элементы пропускаются; перенос
    // механики S3PoolModel.ParseEndpoints — PgWorker.Etcd недоступен сборке).
    internal static IReadOnlyList<string> ParseEndpoints(string raw)
    {
        var result = new List<string>();
        foreach (var item in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colon = item.LastIndexOf(':');
            if (colon <= 0 || colon == item.Length - 1)
                continue;
            if (!int.TryParse(item[(colon + 1)..], out var port) || port <= 0)
                continue;
            result.Add(item);
        }
        return result;
    }

    // secrets-JSON → пара ключа backups (перенос механики S3PoolModel.ParseSecrets).
    internal static (string Access, string Secret)? ParseBackupsSecrets(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("access_keys", out var keys)
                || keys.ValueKind != JsonValueKind.Array)
                return null;
            foreach (var item in keys.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object
                    || !item.TryGetProperty("name", out var name) || name.GetString() != "backups"
                    || !item.TryGetProperty("access_key", out var access) || access.ValueKind != JsonValueKind.String
                    || !item.TryGetProperty("secret_key", out var secret) || secret.ValueKind != JsonValueKind.String)
                    continue;
                var a = access.GetString();
                var s = secret.GetString();
                if (!string.IsNullOrEmpty(a) && !string.IsNullOrEmpty(s))
                    return (a, s);
            }
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return Result<IReadOnlyList<Kv>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        var range = await RangeWithFailoverAsync(key, ct);
        return range.Bind(kvs => Result<Kv?>.Success(kvs.Count > 0 ? kvs[0] : null));
    }
}
