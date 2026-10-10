namespace OwnS3.App.Routing;

// Операции S3-грани ownS3 (глава 02): 22 операции + None (не-матч/501-сабресурс).
public enum S3Operation
{
    None,
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    GetBucketLocation,
    ListObjects,
    ListObjectsV2,
    ListObjectVersions,
    ListMultipartUploads,
    DeleteObjects,
    PutObject,
    CopyObject,
    GetObject,
    HeadObject,
    DeleteObject,
    GetObjectAttributes,
    CreateMultipartUpload,
    UploadPart,
    UploadPartCopy,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
}
