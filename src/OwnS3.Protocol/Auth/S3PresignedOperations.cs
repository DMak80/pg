namespace OwnS3.Protocol.Auth;

// Разрешённые операции через presigned (arch/owns3/03 §2): ровно 10 —
// потребности клиентов бэкапов и mc.
public static class S3PresignedOperations
{
    public static readonly IReadOnlySet<string> Allowed = new HashSet<string>(StringComparer.Ordinal)
    {
        "GetObject",
        "PutObject",
        "DeleteObject",
        "HeadObject",
        "CreateMultipartUpload",
        "UploadPart",
        "UploadPartCopy",
        "CompleteMultipartUpload",
        "AbortMultipartUpload",
        "ListParts",
    };
}
