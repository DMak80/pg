namespace OwnS3.Protocol.Errors;

// Полный каталог кодов S3-ошибок ownS3 (arch/owns3/03 §5, вкл. InvalidAccessKeyId).
// Каждый код: строковое имя, канонический HTTP-статус и Message.
public enum S3ErrorCode
{
    NoSuchBucket,
    NoSuchKey,
    BucketAlreadyExists,        // справочный алиас стандарта 409 — ownS3 не эмитирует
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidRange,
    PreconditionFailed,
    NotModified,
    EntityTooLarge,
    InvalidPart,
    InvalidPartOrder,
    MalformedXML,
    AuthorizationHeaderMalformed,
    AuthorizationQueryParametersError,
    SignatureDoesNotMatch,
    InvalidAccessKeyId,
    AccessDenied,
    RequestTimeTooSkewed,
    BadDigest,
    NoSuchUpload,
    InvalidArgument,
    InvalidBucketName,
    InvalidRequest,
    NotImplemented,
    InternalError,
}

// Информация кода ошибки для ответа (арх-таблица маппинга главы 03 §5).
public sealed record S3ErrorInfo(string Code, int HttpStatus, string Message);

public static class S3ErrorCatalog
{
    private static readonly IReadOnlyDictionary<S3ErrorCode, S3ErrorInfo> Catalog = new Dictionary<S3ErrorCode, S3ErrorInfo>
    {
        [S3ErrorCode.NoSuchBucket] = new("NoSuchBucket", 404, "The specified bucket does not exist"),
        [S3ErrorCode.NoSuchKey] = new("NoSuchKey", 404, "The specified key does not exist."),
        [S3ErrorCode.BucketAlreadyExists] = new("BucketAlreadyExists", 409, "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."),
        [S3ErrorCode.BucketAlreadyOwnedByYou] = new("BucketAlreadyOwnedByYou", 409, "Your previous request to create the named bucket succeeded and you already own it."),
        [S3ErrorCode.BucketNotEmpty] = new("BucketNotEmpty", 409, "The bucket you tried to delete is not empty"),
        [S3ErrorCode.InvalidRange] = new("InvalidRange", 416, "The requested range is not satisfiable"),
        [S3ErrorCode.PreconditionFailed] = new("PreconditionFailed", 412, "At least one of the pre-conditions you specified did not hold"),
        [S3ErrorCode.NotModified] = new("NotModified", 304, "Not Modified"),
        [S3ErrorCode.EntityTooLarge] = new("EntityTooLarge", 400, "Your proposed upload exceeds the maximum allowed object size."),
        [S3ErrorCode.InvalidPart] = new("InvalidPart", 400, "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag."),
        [S3ErrorCode.InvalidPartOrder] = new("InvalidPartOrder", 400, "The list of parts was not in ascending order. The parts list must be specified in order by part number."),
        [S3ErrorCode.MalformedXML] = new("MalformedXML", 400, "The XML you provided was not well-formed or did not validate against our published schema."),
        [S3ErrorCode.AuthorizationHeaderMalformed] = new("AuthorizationHeaderMalformed", 400, "The authorization header is malformed."),
        [S3ErrorCode.AuthorizationQueryParametersError] = new("AuthorizationQueryParametersError", 400, "Query-string authentication parameters are invalid."),
        [S3ErrorCode.SignatureDoesNotMatch] = new("SignatureDoesNotMatch", 403, "The request signature we calculated does not match the signature you provided. Check your key and signing method."),
        [S3ErrorCode.InvalidAccessKeyId] = new("InvalidAccessKeyId", 403, "The AWS access key Id you provided does not exist in our records."),
        [S3ErrorCode.AccessDenied] = new("AccessDenied", 403, "Access Denied."),
        [S3ErrorCode.RequestTimeTooSkewed] = new("RequestTimeTooSkewed", 403, "The difference between the request time and the server's time is too large."),
        [S3ErrorCode.BadDigest] = new("BadDigest", 400, "The Content-Md5 you specified did not match what we received."),
        [S3ErrorCode.NoSuchUpload] = new("NoSuchUpload", 404, "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."),
        [S3ErrorCode.InvalidArgument] = new("InvalidArgument", 400, "Invalid argument"),
        [S3ErrorCode.InvalidBucketName] = new("InvalidBucketName", 400, "The specified bucket is not valid."),
        [S3ErrorCode.InvalidRequest] = new("InvalidRequest", 400, "Invalid Request"),
        [S3ErrorCode.NotImplemented] = new("NotImplemented", 501, "A header you provided implies functionality that is not implemented"),
        [S3ErrorCode.InternalError] = new("InternalError", 500, "We encountered an internal error, please try again."),
    };

    public static S3ErrorInfo Get(S3ErrorCode code) =>
        Catalog.TryGetValue(code, out var info)
            ? info
            : throw new ArgumentOutOfRangeException(nameof(code), code, "Код отсутствует в каталоге ошибок S3");
}
