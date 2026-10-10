using OwnS3.App.Pipeline;
using OwnS3.App.Routing;
using OwnS3.Protocol.Uris;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Листинговые операции (глава 02 §3): ListObjects (v1), ListObjectsV2,
// ListObjectVersions (unversioned-вид). Общие query-парсеры — листинговый
// контур един; вариант передаётся в ListQuery.
public static class ListHandlers
{
    public sealed class ListObjectsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListObjects;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: листинговые параметры (лимит обрезается до 1000)
            BucketHandlers.EnsureBucketName(context.Bucket);
            var (prefix, delimiter, marker, _, _, maxKeys, encodingType, _) = ParseQuery(context);
            OperationValidation.ParseEncodingType(encodingType);

            // Act
            var page = await Store.ListObjectsAsync(context.Bucket, new ListQuery(
                prefix, delimiter, marker, null, null, maxKeys, encodingType, false, ListVariant.V1), ct);

            // Respond: ListBucketResult v1-вид
            await context.WriteXmlAsync(S3Xml.Serialize(new ListBucketResult
            {
                Name = context.Bucket,
                Prefix = Encode(prefix, encodingType) ?? string.Empty,
                Marker = Encode(marker, encodingType) ?? string.Empty,
                NextMarker = Encode(page.NextMarker, encodingType),
                MaxKeys = maxKeys ?? 1000,
                IsTruncated = page.IsTruncated,
                Delimiter = Encode(delimiter, encodingType),
                EncodingType = encodingType,
                Contents = [.. page.Contents.Select(e => new ContentsEntry
                {
                    Key = Encode(e.Key, encodingType) ?? string.Empty,
                    LastModified = S3HandlerContext.FormatDate(e.LastModified),
                    ETag = e.ETag, Size = e.Size,
                })],
                CommonPrefixes = [.. page.CommonPrefixes.Select(p => new OwnS3.Protocol.Xml.CommonPrefixEntry
                {
                    Prefix = Encode(p.Prefix, encodingType) ?? string.Empty,
                })],
            }), ct);
        }
    }

    public sealed class ListObjectsV2Handler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListObjectsV2;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: V2-поля (start-after, continuation-token, fetch-owner)
            BucketHandlers.EnsureBucketName(context.Bucket);
            var (prefix, delimiter, _, startAfter, continuationToken, maxKeys, encodingType, fetchOwner) = ParseQuery(context);
            OperationValidation.ParseEncodingType(encodingType);

            // Act
            var page = await Store.ListObjectsAsync(context.Bucket, new ListQuery(
                prefix, delimiter, null, startAfter, continuationToken, maxKeys, encodingType, fetchOwner, ListVariant.V2), ct);

            // Respond: ListBucketResult V2-вид (KeyCount, ContinuationToken)
            await context.WriteXmlAsync(S3Xml.Serialize(new ListBucketResult
            {
                Name = context.Bucket,
                Prefix = Encode(prefix, encodingType) ?? string.Empty,
                StartAfter = Encode(startAfter, encodingType),
                ContinuationToken = continuationToken,
                NextContinuationToken = page.NextContinuationToken,
                KeyCount = page.KeyCount,
                MaxKeys = maxKeys ?? 1000,
                IsTruncated = page.IsTruncated,
                Delimiter = Encode(delimiter, encodingType),
                EncodingType = encodingType,
                Contents = [.. page.Contents.Select(e => new ContentsEntry
                {
                    Key = Encode(e.Key, encodingType) ?? string.Empty,
                    LastModified = S3HandlerContext.FormatDate(e.LastModified),
                    ETag = e.ETag, Size = e.Size,
                    Owner = fetchOwner ? new OwnerEntry { ID = "owns3", DisplayName = "owns3" } : null,
                })],
                CommonPrefixes = [.. page.CommonPrefixes.Select(p => new OwnS3.Protocol.Xml.CommonPrefixEntry
                {
                    Prefix = Encode(p.Prefix, encodingType) ?? string.Empty,
                })],
            }), ct);
        }
    }

    public sealed class ListObjectVersionsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListObjectVersions;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: key-marker (версий нет — marker-семантика ключей)
            BucketHandlers.EnsureBucketName(context.Bucket);
            var (prefix, delimiter, _, _, _, maxKeys, encodingType, _) = ParseQuery(context);
            var keyMarker = QueryValue(context, "key-marker");
            OperationValidation.ParseEncodingType(encodingType);

            // Act
            var page = await Store.ListObjectsAsync(context.Bucket, new ListQuery(
                prefix, delimiter, keyMarker, null, null, maxKeys, encodingType, false, ListVariant.Versions), ct);

            // Respond: ListVersionsResult — unversioned-вид без VersionId/IsLatest
            await context.WriteXmlAsync(S3Xml.Serialize(new ListVersionsResult
            {
                Name = context.Bucket,
                Prefix = Encode(prefix, encodingType) ?? string.Empty,
                NextKeyMarker = Encode(page.NextMarker, encodingType),
                MaxKeys = maxKeys ?? 1000,
                IsTruncated = page.IsTruncated,
                Delimiter = Encode(delimiter, encodingType),
                EncodingType = encodingType,
                Versions = [.. page.Contents.Select(e => new VersionEntry
                {
                    Key = Encode(e.Key, encodingType) ?? string.Empty,
                    LastModified = S3HandlerContext.FormatDate(e.LastModified),
                    ETag = e.ETag, Size = e.Size,
                })],
                CommonPrefixes = [.. page.CommonPrefixes.Select(p => new OwnS3.Protocol.Xml.CommonPrefixEntry
                {
                    Prefix = Encode(p.Prefix, encodingType) ?? string.Empty,
                })],
            }), ct);
        }
    }

    // Общий парсер листинговых query: prefix/delimiter/marker/start-after/
    // continuation-token/max-keys/encoding-type/fetch-owner.
    private static (string? Prefix, string? Delimiter, string? Marker, string? StartAfter,
        string? ContinuationToken, int? MaxKeys, string? EncodingType, bool FetchOwner) ParseQuery(S3HandlerContext context)
    {
        var maxKeysRaw = QueryValue(context, "max-keys");
        int? maxKeys = maxKeysRaw is null
            ? null
            : OperationValidation.ParsePagingLimit(maxKeysRaw, "max-keys");
        return (
            QueryValue(context, "prefix"),
            QueryValue(context, "delimiter"),
            QueryValue(context, "marker"),
            QueryValue(context, "start-after"),
            QueryValue(context, "continuation-token"),
            maxKeys,
            QueryValue(context, "encoding-type"),
            string.Equals(QueryValue(context, "fetch-owner"), "true", StringComparison.Ordinal));
    }

    private static string? QueryValue(S3HandlerContext context, string name) =>
        OwnS3.Protocol.Auth.CanonicalRequestBuilder.QueryPairs(context.Request.Model.RawQuery)
            .FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
            .Value;

    // encoding-type=url: ключи и префиксы ответа URL-кодированы (глава 02).
    private static string? Encode(string? value, string? encodingType) =>
        encodingType == "url" && value is not null ? UriEncoding.EncodeValue(value) : value;
}
