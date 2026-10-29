namespace PgWorker.NodeTools;

// Тонкая обёртка Shared.Etcd: endpoint-failover по всем URL из параметра.
// put под lease — по канону P11/регистрации ноды (lease decimal в gateway).
public static class EtcdHelper
{
    public static async Task<IEtcdGateway> CreateAsync()
    {
        var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        return new EtcdGateway(httpClient);
    }

    public static async Task<long> LeaseGrantAsync(
        IEtcdGateway gateway, IReadOnlyList<string> endpoints, int ttl, CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => gateway.LeaseGrantAsync(endpoint, ttl, token), ct);
        return result.Throw().Value;
    }

    public static async Task PutAsync(
        IEtcdGateway gateway, IReadOnlyList<string> endpoints,
        string key, string value, long lease, CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => gateway.PutAsync(endpoint, key, value, lease, token), ct);
        result.Throw();
    }

    public static async Task<string?> GetAsync(
        IEtcdGateway gateway, IReadOnlyList<string> endpoints, string key, CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => gateway.GetAsync(endpoint, key, token), ct);
        return result.Throw().Value?.Value;
    }

    public static async Task<bool> KeepaliveAsync(
        IEtcdGateway gateway, IReadOnlyList<string> endpoints, long lease, CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => gateway.LeaseKeepaliveAsync(endpoint, lease, token), ct);
        return result.IsSuccess;
    }
}
