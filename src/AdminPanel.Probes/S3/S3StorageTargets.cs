namespace AdminPanel.Probes.S3;

using AdminPanel.Core.S3;

/// <summary>Цели инвентаря грани «Хранилище бэкапов», резолвнутые из
/// снапшота S3-домена (adminpanel/02 §12.4): креды — read-only ключ
/// panel per-cluster secrets; endpoints — перебор с первого доступного.
/// Значения — только S3-клиенту панели.</summary>
public sealed record S3StorageTargets(
    string Cluster,
    IReadOnlyList<string> Endpoints, // http://host:port — порядок endpoints-ключа
    string AccessKey,
    string SecretKey);

/// <summary>Резолвер целей: первый по Ordinal Active-кластер снапшота с
/// непустыми endpoints и валидным secrets (ключ panel, read-only);
/// null — грань не настроена (нет пула — configured=false).</summary>
public static class S3TargetsResolver
{
    public static S3StorageTargets? Resolve(S3Snapshot snapshot)
    {
        // «Первый по Ordinal» — свойство выбора (не полагаемся на порядок
        // входа: парсер сортирует, прямой конструктор снапшота — нет).
        foreach (var cluster in snapshot.Clusters.OrderBy(c => c.Name, StringComparer.Ordinal))
        {
            if (cluster.State != S3ClusterState.Active
                || string.IsNullOrWhiteSpace(cluster.Endpoints))
                continue;
            var panel = cluster.Secrets?.AccessKeys.FirstOrDefault(
                k => k.Name == "panel" && k.Policy == "read-only");
            if (panel is null
                || string.IsNullOrEmpty(panel.AccessKey)
                || string.IsNullOrEmpty(panel.SecretKey))
                continue;
            var endpoints = cluster.Endpoints
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(e => $"http://{e}")
                .ToList();
            if (endpoints.Count == 0)
                continue;
            return new S3StorageTargets(cluster.Name, endpoints, panel.AccessKey, panel.SecretKey);
        }

        return null;
    }
}
