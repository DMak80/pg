using System.Globalization;
using OwnS3.App.Pipeline;
using OwnS3.App.Routing;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Multipart-операции (глава 02 §5): CreateMultipartUpload, UploadPart,
// UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts,
// ListMultipartUploads. Протокольные контуры t36; дисковая механика — t38.
public static class MultipartHandlers
{
    public sealed class CreateMultipartUploadHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.CreateMultipartUpload;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: метаданные будущего объекта (storage-class игнорируется)
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            var metadata = ObjectHandlers.UploadMetadata(context, contentTypeDefault: "application/octet-stream");

            // Act
            var uploadId = await Store.CreateMultipartUploadAsync(context.Bucket, context.Key, metadata, ct);

            // Respond: InitiateMultipartUploadResult
            await context.WriteXmlAsync(S3Xml.Serialize(new InitiateMultipartUploadResult
            {
                Bucket = context.Bucket, Key = context.Key, UploadId = uploadId,
            }), ct);
        }
    }

    public sealed class UploadPartHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.UploadPart;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: partNumber 1–10000 + лимит 5 ГБ части
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            var partNumber = OperationValidation.ParsePartNumber(QueryValue(context, "partNumber"));
            var uploadId = RequiredQuery(context, "uploadId");
            var contentLength = ObjectHandlers.ResolveContentLength(context);
            OperationValidation.ValidateObjectSize(contentLength);

            // Тело: как у PutObject (чанковая подпись / sha256 / Content-MD5)
            Stream body = context.Http.Body;
            if (context.TryGetChunkedContext(out var chunked))
                body = new AwsChunkedReader(body, chunked!);
            var sha256 = context.Http.Headers["x-amz-content-sha256"].FirstOrDefault();
            var md5 = context.Http.Headers.ContentMD5.ToString();
            body = new HashingBodyStream(body, sha256 is { Length: 64 } ? sha256 : null,
                md5.Length > 0 ? md5 : null);

            // Act
            var result = await Store.UploadPartAsync(context.Bucket, context.Key, uploadId, partNumber,
                body, contentLength, ct);

            // Respond: 200 + ETag части
            context.Response.StatusCode = 200;
            context.Response.Headers.ETag = result.ETag;
        }
    }

    public sealed class UploadPartCopyHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.UploadPartCopy;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: partNumber + источник (copy-source, copy-source-range)
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            var partNumber = OperationValidation.ParsePartNumber(QueryValue(context, "partNumber"));
            var uploadId = RequiredQuery(context, "uploadId");
            var (sourceBucket, sourceKey) = OperationValidation.ParseCopySource(
                context.Http.Headers["x-amz-copy-source"].FirstOrDefault());
            var sourceRange = OperationValidation.ParseCopySourceRange(
                context.Http.Headers["x-amz-copy-source-range"].FirstOrDefault());

            // Act
            var result = await Store.UploadPartCopyAsync(new PartCopyRequest(
                sourceBucket, sourceKey, context.Bucket, context.Key, uploadId, partNumber,
                sourceRange,
                new ObjectConditions(
                    context.Http.Headers["x-amz-copy-source-if-match"].FirstOrDefault(),
                    context.Http.Headers["x-amz-copy-source-if-none-match"].FirstOrDefault(),
                    OperationValidation.ParseHttpDate(context.Http.Headers["x-amz-copy-source-if-modified-since"].FirstOrDefault()),
                    OperationValidation.ParseHttpDate(context.Http.Headers["x-amz-copy-source-if-unmodified-since"].FirstOrDefault()))), ct);

            // Respond: CopyPartResult
            await context.WriteXmlAsync(S3Xml.Serialize(new CopyPartResult
            {
                LastModified = S3HandlerContext.FormatDate(DateTimeOffset.UtcNow),
                ETag = result.ETag,
            }), ct);
        }
    }

    public sealed class CompleteMultipartUploadHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.CompleteMultipartUpload;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: манифест — минимум 1 часть, строгий рост номеров
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            var uploadId = RequiredQuery(context, "uploadId");
            using var reader = new StreamReader(context.Http.Body);
            var manifest = S3Xml.Deserialize<CompleteMultipartUploadRequest>(await reader.ReadToEndAsync(ct));
            OperationValidation.ValidateCompleteManifest(
                [.. manifest.Parts.Select(p => (p.PartNumber, p.ETag))]);

            // Act
            var result = await Store.CompleteMultipartUploadAsync(context.Bucket, context.Key, uploadId,
                [.. manifest.Parts.Select(p => new PartEtag(p.PartNumber, p.ETag))], ct);

            // Respond: CompleteMultipartUploadResult (Location http://<host>/<bucket>/<key>)
            var host = context.Request.Model.Host;
            await context.WriteXmlAsync(S3Xml.Serialize(new CompleteMultipartUploadResult
            {
                Location = $"http://{host}/{context.Bucket}/{context.Key}",
                Bucket = context.Bucket, Key = context.Key, ETag = result.ETag,
            }), ct);
        }
    }

    public sealed class AbortMultipartUploadHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.AbortMultipartUpload;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Act: NoSuchUpload-исход — от Storage (глава 02)
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            await Store.AbortMultipartUploadAsync(context.Bucket, context.Key, RequiredQuery(context, "uploadId"), ct);

            // Respond: 204 No Content
            context.Response.StatusCode = 204;
        }
    }

    public sealed class ListPartsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListParts;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: max-parts, part-number-marker
            BucketHandlers.EnsureBucketName(context.Bucket);
            ObjectHandlers.EnsureObjectKey(context.Key);
            var uploadId = RequiredQuery(context, "uploadId");
            var maxParts = OperationValidation.ParsePagingLimit(QueryValue(context, "max-parts"), "max-parts");
            var markerRaw = QueryValue(context, "part-number-marker");
            var marker = int.TryParse(markerRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var m) ? m : 0;

            // Act
            var page = await Store.ListPartsAsync(context.Bucket, context.Key, uploadId,
                maxParts == OperationValidation.DefaultPagingLimit ? null : maxParts,
                marker == 0 ? null : marker, ct);

            // Respond: ListPartsResult (Initiator/Owner — заполнители)
            await context.WriteXmlAsync(S3Xml.Serialize(new ListPartsResult
            {
                Bucket = context.Bucket, Key = context.Key, UploadId = uploadId,
                PartNumberMarker = marker,
                NextPartNumberMarker = page.NextPartNumberMarker,
                MaxParts = maxParts,
                IsTruncated = page.IsTruncated,
                Parts = [.. page.Parts.Select(p => new OwnS3.Protocol.Xml.PartEntry
                {
                    PartNumber = p.PartNumber,
                    LastModified = S3HandlerContext.FormatDate(p.LastModified),
                    ETag = p.ETag, Size = p.Size,
                })],
                Initiator = new() { ID = "owns3", DisplayName = "owns3" },
                Owner = new() { ID = "owns3", DisplayName = "owns3" },
            }), ct);
        }
    }

    public sealed class ListMultipartUploadsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListMultipartUploads;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Parse: key-marker, upload-id-marker, max-uploads, encoding-type
            BucketHandlers.EnsureBucketName(context.Bucket);
            var prefix = QueryValue(context, "prefix");
            var delimiter = QueryValue(context, "delimiter");
            var keyMarker = QueryValue(context, "key-marker");
            var uploadIdMarker = QueryValue(context, "upload-id-marker");
            var maxUploads = OperationValidation.ParsePagingLimit(QueryValue(context, "max-uploads"), "max-uploads");
            var encodingType = OperationValidation.ParseEncodingType(QueryValue(context, "encoding-type"));

            // Act
            var page = await Store.ListMultipartUploadsAsync(context.Bucket, new UploadsQuery(
                prefix, delimiter, keyMarker, uploadIdMarker,
                maxUploads == OperationValidation.DefaultPagingLimit ? null : maxUploads, encodingType), ct);

            // Respond: ListMultipartUploadsResult (Owner/Initiator — заполнители)
            await context.WriteXmlAsync(S3Xml.Serialize(new ListMultipartUploadsResult
            {
                Bucket = context.Bucket,
                KeyMarker = keyMarker ?? string.Empty,
                UploadIdMarker = uploadIdMarker ?? string.Empty,
                NextKeyMarker = page.NextKeyMarker,
                NextUploadIdMarker = page.NextUploadIdMarker,
                Prefix = prefix,
                Delimiter = delimiter,
                MaxUploads = maxUploads,
                IsTruncated = page.IsTruncated,
                Uploads = [.. page.Uploads.Select(u => new OwnS3.Protocol.Xml.UploadEntry
                {
                    Key = u.Key, UploadId = u.UploadId,
                    Initiated = S3HandlerContext.FormatDate(u.Initiated),
                    Owner = new OwnerEntry { ID = "owns3", DisplayName = "owns3" },
                    Initiator = new OwnerEntry { ID = "owns3", DisplayName = "owns3" },
                })],
                CommonPrefixes = [.. page.CommonPrefixes.Select(p => new OwnS3.Protocol.Xml.CommonPrefixEntry { Prefix = p.Prefix })],
            }), ct);
        }
    }

    private static string? QueryValue(S3HandlerContext context, string name) =>
        CanonicalRequestBuilder.QueryPairs(context.Request.Model.RawQuery)
            .FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
            .Value;

    private static string RequiredQuery(S3HandlerContext context, string name) =>
        QueryValue(context, name)
        ?? throw new OwnS3.Protocol.Errors.S3ProtocolException(OwnS3.Protocol.Errors.S3ErrorCode.InvalidArgument,
            $"{name} is a required parameter");
}
