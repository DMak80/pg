using OwnS3.App.Pipeline;
using OwnS3.App.Routing;
using OwnS3.Protocol.Uris;
using OwnS3.Protocol.Validation;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Бакетные операции (глава 02 §4): ListBuckets, CreateBucket, DeleteBucket,
// HeadBucket, GetBucketLocation (полностью протокольная — регион us-east-1).
public static class BucketHandlers
{
    // Валидация имени бакета — общая предпроверка бакетных операций (глава 03 §6).
    public static void EnsureBucketName(string bucket)
    {
        if (!BucketNameValidator.IsValid(bucket))
            throw new OwnS3.Protocol.Errors.S3ProtocolException(OwnS3.Protocol.Errors.S3ErrorCode.InvalidBucketName);
    }

    public sealed class ListBucketsHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.ListBuckets;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Act: список всех бакетов тома
            var buckets = await Store.ListBucketsAsync(ct);

            // Respond: ListAllMyBucketsResult с Owner-заполнителем ownS3, по алфавиту
            var result = new ListAllMyBucketsResult
            {
                Owner = new OwnerEntry { ID = "owns3", DisplayName = "owns3" },
                Buckets = [.. buckets
                    .OrderBy(b => b.Name, StringComparer.Ordinal)
                    .Select(b => new OwnS3.Protocol.Xml.BucketEntry { Name = b.Name, CreationDate = S3HandlerContext.FormatDate(b.CreationDate) })],
            };
            await context.WriteXmlAsync(S3Xml.Serialize(result), ct);
        }
    }

    public sealed class CreateBucketHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.CreateBucket;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имя бакета (глава 03 §6)
            EnsureBucketName(context.Bucket);

            // Parse: опциональное CreateBucketConfiguration — принимается и игнорируется
            if (context.Http.ContentLength is > 0)
            {
                using var reader = new StreamReader(context.Http.Body);
                _ = S3Xml.Deserialize<CreateBucketConfigurationRequest>(await reader.ReadToEndAsync(ct));
            }

            // Act
            await Store.CreateBucketAsync(context.Bucket, ct);

            // Respond: 200 + Location
            context.Response.StatusCode = 200;
            context.Response.Headers.Location = "/" + context.Bucket;
        }
    }

    public sealed class DeleteBucketHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.DeleteBucket;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Act
            EnsureBucketName(context.Bucket);
            await Store.DeleteBucketAsync(context.Bucket, ct);

            // Respond: 204 No Content
            context.Response.StatusCode = 204;
        }
    }

    public sealed class HeadBucketHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.HeadBucket;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate / Act
            EnsureBucketName(context.Bucket);
            if (!await Store.BucketExistsAsync(context.Bucket, ct))
                throw new OwnS3.Protocol.Errors.S3ProtocolException(OwnS3.Protocol.Errors.S3ErrorCode.NoSuchBucket);

            // Respond: 200 + регион (HEAD — без тела)
            context.Response.StatusCode = 200;
            context.Response.Headers["x-amz-bucket-region"] = "us-east-1";
        }
    }

    // GetBucketLocation — единственная полностью протокольная операция t36
    // (spec §3.4.1): регион — константа us-east-1, объектный слой не нужен.
    public sealed class GetBucketLocationHandler(IObjectStore store) : OperationHandlerBase(store)
    {
        public override S3Operation Operation => S3Operation.GetBucketLocation;

        public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
        {
            // Validate: имя бакета (глава 03 §6) — ошибка каноническая
            EnsureBucketName(context.Bucket);

            // Respond: LocationConstraint — пустой элемент (= us-east-1)
            await context.WriteXmlAsync(S3Xml.Serialize(new LocationConstraint()), ct);
        }
    }
}
