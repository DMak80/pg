using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Закрепление client host-портов managed-узлов (arch/25 §5): один
/// JSON-ключ /etcdworker/portalloc {"<k>":{"host","client"}}; RMW по
/// mod_revision ПОД глобальным клэймом PortAllocLock (занятость дополнительно
/// сверяется с docker-публикациями вызывающего).</summary>
public sealed class EtcdPortAllocStore(IEtcdGateway gateway, string[] endpoints)
{
    public const string Key = "/etcdworker/portalloc";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase, // {"host":...,"client":...}
        WriteIndented = false,
    };

    public sealed record NodePort(string Host, int Client);

    public async Task<Result<IReadOnlyDictionary<int, NodePort>>> ReadAllAsync(CancellationToken ct)
    {
        var kv = await GetAsync(ct);
        if (!kv.IsSuccess)
            return Result<IReadOnlyDictionary<int, NodePort>>.Failed(kv.Error!);
        if (kv.Value is null)
            return Result<IReadOnlyDictionary<int, NodePort>>.Success(
                new Dictionary<int, NodePort>());
        try
        {
            using var doc = JsonDocument.Parse(kv.Value.Value);
            var map = new Dictionary<int, NodePort>();
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (!int.TryParse(prop.Name, out var k)
                    || !prop.Value.TryGetProperty("host", out var host)
                    || host.ValueKind != JsonValueKind.String
                    || !prop.Value.TryGetProperty("client", out var client)
                    || !client.TryGetInt32(out var port))
                    continue; // битая запись — пропустить (толерантный читатель)
                map[k] = new NodePort(host.GetString()!, port);
            }
            return Result<IReadOnlyDictionary<int, NodePort>>.Success(map);
        }
        catch (JsonException e)
        {
            return Result<IReadOnlyDictionary<int, NodePort>>.Failed(
                new ApplicationException($"{Key}: broken portalloc JSON: {e.Message}", e));
        }
    }

    /// <summary>Закрепить client-порт для узла k: наименьший свободный порт
    /// диапазона [from,to], не занятый записями ключа и busyPorts. RMW-txn по
    /// mod_revision; проигрыш — повтор (до 3 попыток → transient-Failed).</summary>
    public async Task<Result<NodePort>> AllocateAsync(
        int k, string host, int from, int to, IReadOnlySet<int> busyPorts, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var kv = await GetAsync(ct);
            if (!kv.IsSuccess)
                return Result<NodePort>.Failed(kv.Error!);
            // Разбор текущих значений (номера → {host,client}).
            var current = new Dictionary<int, NodePort>();
            var modRevision = 0L;
            if (kv.Value is { } existing)
            {
                modRevision = (long)existing.ModRevision;
                try
                {
                    using var doc = JsonDocument.Parse(existing.Value);
                    foreach (var prop in doc.RootElement.EnumerateObject())
                    {
                        if (int.TryParse(prop.Name, out var kk)
                            && prop.Value.TryGetProperty("host", out var h)
                            && h.ValueKind == JsonValueKind.String
                            && prop.Value.TryGetProperty("client", out var c)
                            && c.TryGetInt32(out var p))
                            current[kk] = new NodePort(h.GetString()!, p);
                    }
                }
                catch (JsonException e)
                {
                    return Result<NodePort>.Failed(
                        new ApplicationException($"{Key}: broken portalloc JSON: {e.Message}", e));
                }
            }

            var taken = current.Values.Select(v => v.Client).Concat(busyPorts).ToHashSet();
            var port = Enumerable.Range(from, to - from + 1).FirstOrDefault(p => !taken.Contains(p));
            if (port == 0)
                return Result<NodePort>.Failed(new ApplicationException(
                    $"{Key}: no free client port in [{from},{to}]"));

            current[k] = new NodePort(host, port);
            var payload = Serialize(current);
            var txn = await TxnAsync(
                kv.Value is null
                    ? TxnRequest.Of([TxnCompare.NotExists(Key)], [new TxnOp.Put(Key, payload, null)])
                    : TxnRequest.Of([TxnCompare.ModRevisionEqual(Key, modRevision)],
                        [new TxnOp.Put(Key, payload, null)]),
                ct);
            if (!txn.IsSuccess)
                return Result<NodePort>.Failed(txn.Error!);
            if (txn.Value.Succeeded)
                return Result<NodePort>.Success(current[k]);
            // гонка — повтор RMW
        }

        return Result<NodePort>.Failed(new ApplicationException(
            $"{Key}: allocate race lost 3 times — retried next tick"));
    }

    /// <summary>Снять закрепление узла k (RMW; отсутствующий узел — успех).</summary>
    public async Task<Result> ReleaseAsync(int k, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var kv = await GetAsync(ct);
            if (!kv.IsSuccess)
                return Result.Failed(kv.Error!);
            if (kv.Value is null)
                return Result.Success();
            var modRevision = (long)kv.Value.ModRevision;
            Dictionary<int, NodePort> current;
            try
            {
                current = Parse(kv.Value.Value);
            }
            catch (JsonException e)
            {
                return Result.Failed(new ApplicationException($"{Key}: broken JSON: {e.Message}", e));
            }

            if (!current.Remove(k))
                return Result.Success();
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.ModRevisionEqual(Key, modRevision)],
                    [new TxnOp.Put(Key, Serialize(current), null)]),
                ct);
            if (!txn.IsSuccess)
                return Result.Failed(txn.Error!);
            if (txn.Value.Succeeded)
                return Result.Success();
        }

        return Result.Failed(new ApplicationException($"{Key}: release race lost 3 times"));
    }

    private static Dictionary<int, NodePort> Parse(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        var map = new Dictionary<int, NodePort>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (int.TryParse(prop.Name, out var k)
                && prop.Value.TryGetProperty("host", out var h)
                && h.ValueKind == JsonValueKind.String
                && prop.Value.TryGetProperty("client", out var c)
                && c.TryGetInt32(out var p))
                map[k] = new NodePort(h.GetString()!, p);
        }
        return map;
    }

    private static string Serialize(IReadOnlyDictionary<int, NodePort> map)
        => JsonSerializer.Serialize(map.ToDictionary(
            p => p.Key.ToString(System.Globalization.CultureInfo.InvariantCulture),
            p => p.Value), Json);

    private async Task<Result<Kv?>> GetAsync(CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, Key, ct);
            if (got.IsSuccess)
                return got;
            last = got;
        }
        return Result<Kv?>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }
        return Result<TxnResult>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
