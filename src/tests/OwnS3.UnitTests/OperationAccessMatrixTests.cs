using OwnS3.App.Access;
using OwnS3.App.Routing;

namespace OwnS3.UnitTests;

// Матрица прав (arch/owns3/05 §3): параметризованная проверка 3 роли × 22 операции.
public sealed class OperationAccessMatrixTests
{
    // read-only: чтения и справочники (+ ListParts/ListMultipartUploads — допуск
    // без фильтра «своих» в t36; фильтр появляется с данными загрузок в t38).
    private static readonly HashSet<S3Operation> ReadOnly =
    [
        S3Operation.GetObject, S3Operation.HeadObject,
        S3Operation.ListObjects, S3Operation.ListObjectsV2, S3Operation.ListObjectVersions,
        S3Operation.HeadBucket, S3Operation.ListBuckets, S3Operation.GetBucketLocation,
        S3Operation.ListMultipartUploads, S3Operation.ListParts,
    ];

    private static readonly HashSet<S3Operation> ReadWrite =
    [
        .. ReadOnly,
        S3Operation.PutObject, S3Operation.DeleteObject, S3Operation.DeleteObjects,
        S3Operation.CopyObject, S3Operation.GetObjectAttributes,
        S3Operation.CreateMultipartUpload, S3Operation.UploadPart, S3Operation.UploadPartCopy,
        S3Operation.CompleteMultipartUpload, S3Operation.AbortMultipartUpload,
    ];

    public static TheoryData<AccessPolicy, S3Operation, bool> Matrix { get; } = Build();

    private static TheoryData<AccessPolicy, S3Operation, bool> Build()
    {
        var data = new TheoryData<AccessPolicy, S3Operation, bool>();
        foreach (S3Operation operation in Enum.GetValues<S3Operation>())
        {
            if (operation == S3Operation.None)
                continue;
            data.Add(AccessPolicy.ReadOnly, operation, ReadOnly.Contains(operation));
            data.Add(AccessPolicy.ReadWrite, operation, ReadWrite.Contains(operation));
            data.Add(AccessPolicy.Admin, operation, true);
        }
        return data;
    }

    [Theory, MemberData(nameof(Matrix))]
    public void IsAllowed_MatchesChapter05Matrix(AccessPolicy policy, S3Operation operation, bool expected)
    {
        // Arrange / Act
        var allowed = OperationAccessMatrix.IsAllowed(policy, operation);
        // Assert
        allowed.Should().Be(expected);
    }

    [Fact]
    public void IsAllowed_Admin_ManagesBuckets()
    {
        // Arrange / Act / Assert: мутации бакетов — только admin
        OperationAccessMatrix.IsAllowed(AccessPolicy.Admin, S3Operation.CreateBucket).Should().BeTrue();
        OperationAccessMatrix.IsAllowed(AccessPolicy.Admin, S3Operation.DeleteBucket).Should().BeTrue();
        OperationAccessMatrix.IsAllowed(AccessPolicy.ReadWrite, S3Operation.CreateBucket).Should().BeFalse();
        OperationAccessMatrix.IsAllowed(AccessPolicy.ReadOnly, S3Operation.DeleteBucket).Should().BeFalse();
    }
}
