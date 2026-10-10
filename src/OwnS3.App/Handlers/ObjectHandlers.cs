using System.Globalization;
using OwnS3.App.Pipeline;
using OwnS3.App.Routing;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Uris;
using OwnS3.Protocol.Validation;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Объектные операции (глава 02 §2): PutObject, GetObject, HeadObject,
// DeleteObject, DeleteObjects, CopyObject, GetObjectAttributes.
public static class ObjectHandlers
{
    public sealed class PutObjectHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.PutObject;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имена (глава 03 §6) + лимит 5 ГБ по фактической длине тела
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            var contentLength = ResolveContentLength(context);
            OperationValidation.ValidateObjectSize(contentLength);

            // Parse: метаданные (Content-Type дефолт, x-amz-meta-* без префикса);
            // x-amz-storage-class принимается и игнорируется (глава 02).
            var metadata = UploadMetadata(context, contentTypeDefault: "application/octet-stream");

            // Тело: чанковый режим → AwsChunkedReader; hex-sha256/Content-MD5 → HashingBodyStream.
            Stream body = context.Http.Body;
            if (context.TryGetChunkedContext(out var chunked))
                body = new AwsChunkedReader(body, chunked!);
            body = new HashingBodyStream(body,
                Sha256HexOrNull(context),
                context.Http.Headers.ContentMD5.ToString() is { Length: > 0 } md5 ? md5 : null);

            // Act
            var result = await Store.PutObjectAsync(context.Bucket, context.Key, body, contentLength, metadata, ct);

            // Respond: 200 + ETag (тела нет)
            context.Response.StatusCode = 200;
            context.Response.Headers.ETag = result.ETag;
        }
    }

    public sealed class GetObjectHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.GetObject;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: response-* применяются к ответу, conditional + Range —
            // в опции чтения (оценка — t37); Range-форматная невалидность — 416 сразу.
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            var options = ReadOptions(context);

            // Act
            var content = await Store.GetObjectAsync(context.Bucket, context.Key, options, ct);

            // Respond: 304 без тела с ETag/Last-Modified; 206 + Content-Range при
            // применённом Range; иначе 200 (глава 02 §1)
            await using (content.Body)
            {
                context.Response.ContentType = content.Metadata.ContentType;
                context.Response.Headers.ETag = content.Metadata.ETag;
                context.Response.Headers.LastModified = content.Metadata.LastModified.ToString("R");
                context.Response.Headers.AcceptRanges = "bytes";
                foreach (var (name, value) in content.Metadata.UserMetadata)
                    context.Response.Headers["x-amz-meta-" + name] = value;
                ApplyResponseOverrides(context);
                if (content.NotModified)
                {
                    // 304 обязан идти без тела, но с заголовками идентичности
                    context.Response.StatusCode = 304;
                    return;
                }
                if (content.Range is { } r)
                {
                    context.Response.StatusCode = 206;
                    context.Response.Headers.ContentRange = $"bytes {r.Start}-{r.End}/{r.Total}";
                    context.Response.Headers.ContentLength = r.End - r.Start + 1;
                }
                else
                {
                    context.Response.StatusCode = 200;
                    context.Response.Headers.ContentLength = content.Metadata.Size;
                }
                await content.Body.CopyToAsync(context.Response.Body, ct);
            }
        }
    }

    public sealed class HeadObjectHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.HeadObject;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: как GetObject — различие только в отсутствии тела
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            var options = ReadOptions(context);

            // Act
            var content = await Store.HeadObjectAsync(context.Bucket, context.Key, options, ct);

            // Respond: те же исходы, что GetObject, без тела (глава 02 §1)
            var metadata = content.Metadata;
            context.Response.ContentType = metadata.ContentType;
            context.Response.Headers.ETag = metadata.ETag;
            context.Response.Headers.LastModified = metadata.LastModified.ToString("R");
            context.Response.Headers.AcceptRanges = "bytes";
            foreach (var (name, value) in metadata.UserMetadata)
                context.Response.Headers["x-amz-meta-" + name] = value;
            ApplyResponseOverrides(context);
            if (content.NotModified)
            {
                context.Response.StatusCode = 304;
                return;
            }
            if (content.Range is { } r)
            {
                context.Response.StatusCode = 206;
                context.Response.Headers.ContentRange = $"bytes {r.Start}-{r.End}/{r.Total}";
                context.Response.Headers.ContentLength = r.End - r.Start + 1;
            }
            else
            {
                context.Response.StatusCode = 200;
                context.Response.Headers.ContentLength = metadata.Size;
            }
        }
    }

    public sealed class DeleteObjectHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.DeleteObject;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Act (идемпотентен — 204 и для отсутствующего)
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            await Store.DeleteObjectAsync(context.Bucket, context.Key, ct);

            // Respond: 204 No Content
            context.Response.StatusCode = 204;
        }
    }

    public sealed class DeleteObjectsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.DeleteObjects;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имя бакета
            BucketHandlers.EnsureBucketName(context.Bucket);

            // Parse: XML Delete; битый → MalformedXML; лимит 0/1000 — MalformedXML.
            // Тело — через HashingBodyStream (глава 02: Content-MD5 опционален,
            // проверяется при наличии — сверка не обходится и для XML-тел).
            var body = new HashingBodyStream(context.Http.Body, null,
                context.Http.Headers.ContentMD5.ToString() is { Length: > 0 } md5 ? md5 : null);
            using var reader = new StreamReader(body);
            var request = S3Xml.Deserialize<DeleteRequest>(await reader.ReadToEndAsync(ct));
            var keys = request.Objects.Select(o => o.Key).ToList();
            OperationValidation.ValidateDeleteKeys(keys.Count);

            // Act
            var results = await Store.DeleteObjectsAsync(context.Bucket, keys, request.Quiet, ct);

            // Respond: DeleteResult (Quiet — только ошибки)
            var result = new DeleteResult
            {
                Deleted = [.. results.Where(r => r.Deleted && !request.Quiet).Select(r => new DeletedEntry { Key = r.Key })],
                Errors = [.. results.Where(r => !r.Deleted).Select(r => new DeleteErrorEntry
                {
                    Key = r.Key, Code = r.ErrorCode ?? "InternalError", Message = r.ErrorMessage ?? string.Empty,
                })],
            };
            await context.WriteXmlAsync(S3Xml.Serialize(result), ct);
        }
    }

    public sealed class CopyObjectHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.CopyObject;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имена приёмника; источник — парсинг copy-source (глава 02)
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            var (sourceBucket, sourceKey) = OperationValidation.ParseCopySource(
                context.Http.Headers["x-amz-copy-source"].FirstOrDefault());
            var directive = OperationValidation.ParseMetadataDirective(
                context.Http.Headers["x-amz-metadata-directive"].FirstOrDefault());

            // Parse: новые метаданные при REPLACE; copy-условия к источнику
            var newMetadata = directive == "REPLACE" ? UploadMetadata(context, contentTypeDefault: null) : null;

            // Act
            var result = await Store.CopyObjectAsync(new CopyRequest(
                sourceBucket, sourceKey, context.Bucket, context.Key,
                directive == "REPLACE", newMetadata, CopyConditions(context)), ct);

            // Respond: CopyObjectResult
            await context.WriteXmlAsync(S3Xml.Serialize(new CopyObjectResult
            {
                LastModified = S3HandlerContext.FormatDate(DateTimeOffset.UtcNow),
                ETag = result.ETag,
            }), ct);
        }
    }

    public sealed class GetObjectAttributesHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.GetObjectAttributes;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имена + обязательный непустой список атрибутов (глава 02);
            // вне-наборные значения (вкл. Checksum) — 501 NotImplemented.
            BucketHandlers.EnsureBucketName(context.Bucket);
            EnsureObjectKey(context.Key);
            var attributes = ParseObjectAttributes(
                context.Http.Headers["x-amz-object-attributes"].FirstOrDefault());

            // Parse: пагинация ObjectParts + conditional (GET-семантика, P10)
            var maxParts = PagingLimit(context, "x-amz-max-parts");
            var partNumberMarker = int.TryParse(context.Http.Headers["x-amz-part-number-marker"].FirstOrDefault(),
                NumberStyles.Integer, CultureInfo.InvariantCulture, out var marker) ? marker : 0;
            var conditions = ReadOptions(context).Conditions;

            // Act
            var result = await Store.GetObjectAttributesAsync(context.Bucket, context.Key, attributes,
                maxParts == OperationValidation.DefaultPagingLimit ? null : maxParts,
                partNumberMarker == 0 ? null : partNumberMarker, conditions, ct);

            // Respond: Not-Modified → 304 без тела с ETag/Last-Modified (P10)
            if (result.NotModifiedMetadata is { } nm)
            {
                context.Response.StatusCode = 304;
                context.Response.Headers.ETag = nm.ETag;
                context.Response.Headers.LastModified = nm.LastModified.ToString("R");
                return;
            }
            var attrs = result.Attributes!;

            // GetObjectAttributesOutput — только запрошенные элементы
            var requested = new HashSet<string>(context.Http.Headers["x-amz-object-attributes"].ToString()
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), StringComparer.Ordinal);
            var output = new GetObjectAttributesOutput
            {
                ETag = requested.Contains("ETag") ? attrs.ETag : null,
                ObjectSize = requested.Contains("ObjectSize") ? attrs.ObjectSize : null,
                StorageClass = requested.Contains("StorageClass") ? attrs.StorageClass : null,
                ObjectParts = requested.Contains("ObjectParts") && attrs.Parts is not null
                    ? new ObjectParts
                    {
                        PartsCount = attrs.Parts.PartsCount,
                        PartNumberMarker = attrs.Parts.PartNumberMarker,
                        NextPartNumberMarker = attrs.Parts.NextPartNumberMarker,
                        MaxParts = attrs.Parts.MaxParts,
                        IsTruncated = attrs.Parts.IsTruncated,
                        Parts = [.. attrs.Parts.Parts.Select(p => new AttributesPartEntry { PartNumber = p.PartNumber, Size = p.Size })],
                    }
                    : null,
            };
            context.Response.Headers.ETag = attrs.ETag;
            context.Response.Headers.LastModified = DateTimeOffset.UtcNow.ToString("R");
            await context.WriteXmlAsync(S3Xml.Serialize(output), ct);
        }
    }

    // — общие парсеры объектного семейства —

    internal static void EnsureObjectKey(string key)
    {
        if (!ObjectKeyValidator.IsValid(key))
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Invalid key length or empty key");
    }

    // Метаданные загрузки: Content-Type (дефолт при null) + x-amz-meta-* без префикса.
    internal static ObjectUploadMetadata UploadMetadata(S3HandlerContext context, string? contentTypeDefault)
    {
        var contentType = context.Http.Headers.ContentType.ToString();
        if (contentType.Length == 0)
            contentType = contentTypeDefault ?? string.Empty;
        var userMetadata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in context.Http.Headers)
        {
            if (name.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(name, "x-amz-metadata-directive", StringComparison.OrdinalIgnoreCase))
            {
                userMetadata[name["x-amz-meta-".Length..].ToLowerInvariant()] = value.ToString();
            }
        }
        return new ObjectUploadMetadata(contentType, userMetadata);
    }

    // Опции чтения: conditional + Range + If-Range (парсинг формата; оценка — Storage).
    internal static ObjectReadOptions ReadOptions(S3HandlerContext context)
    {
        ObjectConditions? conditions = null;
        var ifMatch = context.Http.Headers.IfMatch.ToString();
        var ifNoneMatch = context.Http.Headers.IfNoneMatch.ToString();
        var ifModifiedSince = OperationValidation.ParseHttpDate(context.Http.Headers.IfModifiedSince.ToString());
        var ifUnmodifiedSince = OperationValidation.ParseHttpDate(context.Http.Headers.IfUnmodifiedSince.ToString());
        if (ifMatch.Length > 0 || ifNoneMatch.Length > 0 || ifModifiedSince is not null || ifUnmodifiedSince is not null)
            conditions = new ObjectConditions(
                ifMatch.Length > 0 ? ifMatch : null,
                ifNoneMatch.Length > 0 ? ifNoneMatch : null,
                ifModifiedSince, ifUnmodifiedSince);

        var range = OperationValidation.ParseRange(context.Http.Headers.Range.ToString());
        var ifRange = context.Http.Headers["If-Range"].FirstOrDefault();
        return new ObjectReadOptions(conditions, range, ifRange is { Length: > 0 } ? ifRange : null);
    }

    // response-*: переопределения заголовков ответа (любой подписанный GET, глава 02).
    internal static void ApplyResponseOverrides(S3HandlerContext context)
    {
        var rawQuery = context.Request.Model.RawQuery;
        foreach (var (name, value) in OwnS3.Protocol.Auth.CanonicalRequestBuilder.QueryPairs(rawQuery))
        {
            if (name is null || value is null || !name.StartsWith("response-", StringComparison.Ordinal))
                continue;
            var headerName = ResponseOverrideHeaderName(name);
            if (headerName is not null)
                context.Response.Headers[headerName] = value;
        }
    }

    // Маппинг response-* → канонические имена заголовков: в ответе ставятся
    // КАНОНИЧЕСКИЕ имена (Cache-Control, …), не «response-*»; прочие
    // response-* игнорируются.
    internal static string? ResponseOverrideHeaderName(string queryName) => queryName switch
    {
        "response-cache-control" => "Cache-Control",
        "response-content-disposition" => "Content-Disposition",
        "response-content-encoding" => "Content-Encoding",
        "response-content-language" => "Content-Language",
        "response-content-type" => "Content-Type",
        "response-expires" => "Expires",
        _ => null,
    };

    internal static long ResolveContentLength(S3HandlerContext context)
    {
        // Чанковый режим — x-amz-decoded-content-length; иначе Content-Length (глава 02).
        var decoded = context.Http.Headers["x-amz-decoded-content-length"].FirstOrDefault();
        if (decoded is { Length: > 0 } && long.TryParse(decoded, NumberStyles.Integer, CultureInfo.InvariantCulture, out var d))
            return d;
        return context.Http.ContentLength ?? 0;
    }

    private static string? Sha256HexOrNull(S3HandlerContext context)
    {
        var value = context.Http.Headers["x-amz-content-sha256"].FirstOrDefault();
        return value is { Length: 64 } ? value : null;
    }

    private static ObjectConditions CopyConditions(S3HandlerContext context)
    {
        var headers = context.Http.Headers;
        return new ObjectConditions(
            headers["x-amz-copy-source-if-match"].FirstOrDefault(),
            headers["x-amz-copy-source-if-none-match"].FirstOrDefault(),
            OperationValidation.ParseHttpDate(headers["x-amz-copy-source-if-modified-since"].FirstOrDefault()),
            OperationValidation.ParseHttpDate(headers["x-amz-copy-source-if-unmodified-since"].FirstOrDefault()));
    }

    internal static List<ObjectAttributeName> ParseObjectAttributes(string? header)
    {
        if (string.IsNullOrWhiteSpace(header))
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Invalid attribute name specified");
        var result = new List<ObjectAttributeName>();
        foreach (var name in header.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            switch (name)
            {
                case "ETag": result.Add(ObjectAttributeName.ETag); break;
                case "ObjectSize": result.Add(ObjectAttributeName.ObjectSize); break;
                case "StorageClass": result.Add(ObjectAttributeName.StorageClass); break;
                case "ObjectParts": result.Add(ObjectAttributeName.ObjectParts); break;
                default:
                    // Checksum и прочие — вне-наборные грани (глава 02 §1) → 501.
                    throw new S3ProtocolException(S3ErrorCode.NotImplemented,
                        "A header you provided implies functionality that is not implemented");
            }
        }
        return result;
    }

    internal static int PagingLimit(S3HandlerContext context, string headerName)
    {
        var value = context.Http.Headers[headerName].FirstOrDefault();
        return OperationValidation.ParsePagingLimit(value, headerName);
    }
}
