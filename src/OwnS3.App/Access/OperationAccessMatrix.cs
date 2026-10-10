using OwnS3.App.Routing;

namespace OwnS3.App.Access;

// Матрица прав (arch/owns3/05 §3): роль → минимальные права на операцию.
// Замечание t36: read-only допускается к ListParts/ListMultipartUploads без
// фильтра «своих» загрузок — фильтр появляется с данными загрузок (t38).
public static class OperationAccessMatrix
{
    private static readonly IReadOnlySet<S3Operation> ReadOnly =
        new HashSet<S3Operation>
        {
            S3Operation.GetObject, S3Operation.HeadObject,
            S3Operation.ListObjects, S3Operation.ListObjectsV2, S3Operation.ListObjectVersions,
            S3Operation.HeadBucket, S3Operation.ListBuckets, S3Operation.GetBucketLocation,
            S3Operation.ListMultipartUploads, S3Operation.ListParts,
        };

    private static readonly IReadOnlySet<S3Operation> ReadWrite =
        new HashSet<S3Operation>(ReadOnly)
        {
            S3Operation.PutObject, S3Operation.DeleteObject, S3Operation.DeleteObjects,
            S3Operation.CopyObject, S3Operation.GetObjectAttributes,
            S3Operation.CreateMultipartUpload, S3Operation.UploadPart, S3Operation.UploadPartCopy,
            S3Operation.CompleteMultipartUpload, S3Operation.AbortMultipartUpload,
        };

    public static bool IsAllowed(AccessPolicy policy, S3Operation operation) => policy switch
    {
        AccessPolicy.Admin => operation != S3Operation.None,
        AccessPolicy.ReadWrite => ReadWrite.Contains(operation),
        AccessPolicy.ReadOnly => ReadOnly.Contains(operation),
        _ => false,
    };
}
