using OwnS3.Protocol.Requests;

namespace OwnS3.App.Routing;

// Роутер 22 операций (arch/owns3/02; порядок специфичное→общее — референс
// api-router.go): path-style разбор, query-дискриминаторы по наличию ключа,
// заголовочный дискриминатор x-amz-copy-source. Вне-наборные сабресурсы
// (глава 02 §1) и attributes-на-не-GET → RejectedSubresource (501 до аутентификации);
// не-матч → None без сабресурса (400 InvalidArgument «Unsupported request»).
public static class S3Router
{
    // 24 вне-наборных сабресурса главы 02 §1 (+ attributes на не-GET — расширение планом t36).
    private static readonly IReadOnlySet<string> RejectedSubresources = new HashSet<string>(StringComparer.Ordinal)
    {
        "acl", "tagging", "retention", "legal-hold", "torrent", "restore", "versioning",
        "lifecycle", "replication", "encryption", "policy", "cors", "website", "notification",
        "accelerate", "object-lock", "logging", "metrics", "inventory", "intelligent-tiering",
        "ownershipControls", "publicAccessBlock", "requestPayment", "select",
    };

    public static S3Route Route(string method, string rawPath, string rawQuery, bool copySource)
    {
        var path = S3PathParser.Parse(rawPath);
        var queryKeys = QueryKeys(rawQuery);
        var hasBucket = path.Bucket is not null;
        var hasKey = path.Key is not null;

        // Вне-наборные сабресурсы — до таблицы маршрутов (501).
        foreach (var key in queryKeys)
        {
            if (RejectedSubresources.Contains(key)
                || (key == "attributes" && !string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase)))
                return new S3Route(S3Operation.None, path.Bucket, path.Key, key);
        }

        var hasUploadId = queryKeys.Contains("uploadId");
        var hasPartNumber = queryKeys.Contains("partNumber");

        if (!hasBucket)
            return method.ToUpperInvariant() switch
            {
                "GET" when !hasKey => new S3Route(S3Operation.ListBuckets, null, null, null),
                _ => Unmatched(path),
            };

        if (!hasKey)
        {
            return method.ToUpperInvariant() switch
            {
                "GET" => queryKeys switch
                {
                    var q when q.Contains("location") => new S3Route(S3Operation.GetBucketLocation, path.Bucket, null, null),
                    var q when q.Contains("uploads") => new S3Route(S3Operation.ListMultipartUploads, path.Bucket, null, null),
                    var q when q.Contains("versions") => new S3Route(S3Operation.ListObjectVersions, path.Bucket, null, null),
                    var q when q.Contains("list-type") => new S3Route(S3Operation.ListObjectsV2, path.Bucket, null, null),
                    _ => new S3Route(S3Operation.ListObjects, path.Bucket, null, null),
                },
                "PUT" => new S3Route(S3Operation.CreateBucket, path.Bucket, null, null),
                "DELETE" => new S3Route(S3Operation.DeleteBucket, path.Bucket, null, null),
                "HEAD" => new S3Route(S3Operation.HeadBucket, path.Bucket, null, null),
                "POST" when queryKeys.Contains("delete") => new S3Route(S3Operation.DeleteObjects, path.Bucket, null, null),
                _ => Unmatched(path),
            };
        }

        return method.ToUpperInvariant() switch
        {
            "GET" => queryKeys switch
            {
                var q when q.Contains("attributes") => new S3Route(S3Operation.GetObjectAttributes, path.Bucket, path.Key, null),
                var q when q.Contains("uploadId") => new S3Route(S3Operation.ListParts, path.Bucket, path.Key, null),
                _ => new S3Route(S3Operation.GetObject, path.Bucket, path.Key, null),
            },
            "HEAD" => new S3Route(S3Operation.HeadObject, path.Bucket, path.Key, null),
            "PUT" when hasPartNumber && hasUploadId && copySource => new S3Route(S3Operation.UploadPartCopy, path.Bucket, path.Key, null),
            "PUT" when hasPartNumber && hasUploadId => new S3Route(S3Operation.UploadPart, path.Bucket, path.Key, null),
            "PUT" when copySource => new S3Route(S3Operation.CopyObject, path.Bucket, path.Key, null),
            "PUT" => new S3Route(S3Operation.PutObject, path.Bucket, path.Key, null),
            "DELETE" when hasUploadId => new S3Route(S3Operation.AbortMultipartUpload, path.Bucket, path.Key, null),
            "DELETE" => new S3Route(S3Operation.DeleteObject, path.Bucket, path.Key, null),
            "POST" when queryKeys.Contains("uploads") => new S3Route(S3Operation.CreateMultipartUpload, path.Bucket, path.Key, null),
            "POST" when hasUploadId => new S3Route(S3Operation.CompleteMultipartUpload, path.Bucket, path.Key, null),
            _ => Unmatched(path),
        };
    }

    private static S3Route Unmatched(S3Path path) => new(S3Operation.None, path.Bucket, path.Key, null);

    // Ключи query без значений (дискриминатор = наличие ключа).
    private static IReadOnlySet<string> QueryKeys(string rawQuery)
    {
        if (string.IsNullOrEmpty(rawQuery))
            return new HashSet<string>(StringComparer.Ordinal);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var pair in rawQuery.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=');
            keys.Add(eq < 0 ? pair : pair[..eq]);
        }
        return keys;
    }
}
