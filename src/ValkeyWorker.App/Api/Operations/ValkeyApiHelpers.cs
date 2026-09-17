using System.Globalization;
using System.Text.Json;
using Shared.Core;
using Shared.Core.Writing;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Failover по endpoints для хендлеров API (порт EtcdFailover): первый
// успешный ответ выигрывает; все недоступны → EtcdWriteUnavailableException = 503.
internal static class ValkeyEtcdFailover
{
    public static async Task<Result<T>> CallAsync<T>(string[] endpoints, Func<string, Task<Result<T>>> call)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
        }

        return Result<T>.Failed(new EtcdWriteUnavailableException());
    }

    public static async Task<Result> CallAsync(string[] endpoints, Func<string, Task<Result>> call)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new EtcdWriteUnavailableException());
    }
}

// Общие хелперы чтения valkey-ключей для хендлеров API: config с revision
// (RMW), точечные чтения; активный endpoint — свой список с failover.
internal static class ValkeyApiHelpers
{
    // Чтение config-ключа с revision: (значение, mod_revision) — для RMW-мутаций.
    internal sealed record ConfigRead(ValkeyConfigJson? Value, long? Revision, Exception? Error);

    internal static async Task<ConfigRead> ReadConfigAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var kv = await gateway.GetAsync(endpoint, ConfigKey(cluster), ct);
            if (!kv.IsSuccess)
                continue;
            if (kv.Value is null)
                return new ConfigRead(null, null, null);
            try
            {
                return new ConfigRead(ValkeyConfigJson.Parse(kv.Value.Value), (long)kv.Value.ModRevision, null);
            }
            catch (JsonException)
            {
                return new ConfigRead(null, null, new InvalidValkeyConfigException(cluster));
            }
        }

        return new ConfigRead(null, null, new EtcdWriteUnavailableException());
    }

    // Точечное чтение ключа (null = отсутствует).
    internal static async Task<Result<Kv?>> ReadKeyAsync(
        IEtcdGateway gateway, string[] endpoints, string key, CancellationToken ct)
        => await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.GetAsync(endpoint, key, ct));

    internal static string ConfigKey(string cluster) => $"/valkey/clusters/{cluster}/config";

    internal static string NodeKey(string cluster, string node, string leaf)
        => $"/valkey/clusters/{cluster}/nodes/{node}/{leaf}";
}
