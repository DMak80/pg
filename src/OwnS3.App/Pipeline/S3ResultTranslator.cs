using Microsoft.AspNetCore.Http;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;

namespace OwnS3.App.Pipeline;

// Трансляция исходов Result хендлеров в S3-ошибки (конвенция главы 01 §4; формат —
// канон 03 §5 + коды каталога главы 02): ObjectStoreException — доменный код,
// XlInvalidArgumentException — 400 InvalidArgument, прочие Error — 500 InternalError.
public static class S3ResultTranslator
{
    public static S3Error Translate(Exception error, string resource, string requestId, string? hostId) =>
        error switch
        {
            ObjectStoreException ex => new S3Error(MapStorageCode(ex.Code),
                Resource: resource, RequestId: requestId, HostId: hostId),
            XlInvalidArgumentException ex => new S3Error(S3ErrorCode.InvalidArgument,
                Resource: resource, RequestId: requestId, HostId: hostId,
                MessageOverride: ex.Message),
            _ => new S3Error(S3ErrorCode.InternalError,
                Resource: resource, RequestId: requestId, HostId: hostId),
        };

    // Запись ошибки в HTTP-ответ — единый helper всех граней (компенсация
    // демонтированных middleware-веток): статус по каталогу, XML-тело канона
    // 03 §5, Resource — Request.Path (как в middleware-ветках), HEAD — без тела;
    // hostId — сырой nullable (null → пустой <HostId></HostId>, как сегодня).
    public static async Task WriteAsync(HttpContext context, Exception error,
        string requestId, string? hostId)
    {
        var s3Error = Translate(error, context.Request.Path, requestId, hostId);
        context.Response.StatusCode = s3Error.Info.HttpStatus;
        if (HttpMethods.IsHead(context.Request.Method))
            return; // HEAD-ошибки — без тела (глава 03 §5)
        context.Response.ContentType = "application/xml";
        await context.Response.WriteAsync(S3ErrorXmlWriter.Write(s3Error), context.RequestAborted);
    }

    // Маппинг доменных кодов Storage в каталог S3-ошибок (имена синхронны) —
    // перенесён из S3Middleware без изменений.
    private static S3ErrorCode MapStorageCode(ObjectStoreErrorCode code) => code switch
    {
        ObjectStoreErrorCode.NoSuchBucket => S3ErrorCode.NoSuchBucket,
        ObjectStoreErrorCode.NoSuchKey => S3ErrorCode.NoSuchKey,
        ObjectStoreErrorCode.BucketAlreadyOwnedByYou => S3ErrorCode.BucketAlreadyOwnedByYou,
        ObjectStoreErrorCode.BucketNotEmpty => S3ErrorCode.BucketNotEmpty,
        ObjectStoreErrorCode.InvalidPart => S3ErrorCode.InvalidPart,
        ObjectStoreErrorCode.InvalidPartOrder => S3ErrorCode.InvalidPartOrder,
        ObjectStoreErrorCode.NoSuchUpload => S3ErrorCode.NoSuchUpload,
        ObjectStoreErrorCode.InvalidRange => S3ErrorCode.InvalidRange,
        ObjectStoreErrorCode.PreconditionFailed => S3ErrorCode.PreconditionFailed,
        ObjectStoreErrorCode.NotModified => S3ErrorCode.NotModified,
        ObjectStoreErrorCode.EntityTooLarge => S3ErrorCode.EntityTooLarge,
        ObjectStoreErrorCode.InsufficientReadQuorum => S3ErrorCode.InsufficientReadQuorum,
        ObjectStoreErrorCode.InsufficientWriteQuorum => S3ErrorCode.InsufficientWriteQuorum,
        _ => S3ErrorCode.InternalError,
    };
}
