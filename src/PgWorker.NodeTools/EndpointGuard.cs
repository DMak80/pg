using System.Net;

namespace PgWorker.NodeTools;

// Границы исходящих etcd-запросов (CWE-918): разрешены только http/https;
// DNS-имя — доверенная конфигурация контура, литеральный IP — только
// loopback/private. Username/fragment в URL запрещены.
public static class EndpointGuard
{
    public static IReadOnlyList<string> ValidateEtcdEndpoints(IReadOnlyList<string> endpoints)
    {
        if (endpoints.Count == 0)
            throw new ArgumentException("etcd endpoints are empty");

        foreach (var endpoint in endpoints)
        {
            var uri = ValidateHttpUrl(endpoint, "etcd endpoint");
            if (IPAddress.TryParse(uri.Host, out var address) && !NetworkAddress.IsLoopbackOrPrivate(address))
                throw new ArgumentException($"blocked non-private etcd ip: {uri.Host}");
        }

        return endpoints;
    }

    public static Uri ValidateHttpUrl(string url, string kind)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) ||
            uri.Scheme is not ("http" or "https") ||
            string.IsNullOrWhiteSpace(uri.Host) ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Fragment))
            throw new ArgumentException($"invalid {kind} scheme/form: {url}");

        return uri;
    }
}
