using OwnS3.App.Routing;

namespace OwnS3.UnitTests;

// Таблица маршрутов 22 операций (arch/owns3/02; порядок — референс api-router.go,
// spec §3.4). Дискриминатор = наличие query-ключа, КРОМЕ list-type —
// дискриминатор СО ЗНАЧЕНИЕМ: матчится ровно «2», иное/пустое → маршрут v1
// (глава 02: list-type=2 — обязательный дискриминатор; поведение референса).
// UploadPart-семейство = оба ключа (partNumber И uploadId); одиночные
// параметры — вне-наборные не-сабресурсы, игнорируются (глава 02 §1).
public sealed class S3RouterTests
{
    // (метод, путь, query, copy-source) → ожидаемая операция
    public static TheoryData<string, string, string, bool, S3Operation> Routes = new()
    {
        // корень
        { "GET",     "/",                 "",         false, S3Operation.ListBuckets },
        // бакет: GET-дискриминаторы от специфичного к общему
        { "GET",     "/b",                "location",  false, S3Operation.GetBucketLocation },
        { "GET",     "/b",                "uploads",   false, S3Operation.ListMultipartUploads },
        { "GET",     "/b",                "versions",  false, S3Operation.ListObjectVersions },
        { "GET",     "/b",                "list-type=2", false, S3Operation.ListObjectsV2 },
        { "GET",     "/b",                "list-type=1", false, S3Operation.ListObjects }, // иное значение → v1 (референс)
        { "GET",     "/b",                "list-type=",  false, S3Operation.ListObjects }, // пустое значение → v1
        { "GET",     "/b",                "",          false, S3Operation.ListObjects },
        { "GET",     "/b",                "prefix=x&marker=y", false, S3Operation.ListObjects }, // листинговые параметры — не дискриминаторы
        // бакет: прочие методы
        { "PUT",     "/b",                "",          false, S3Operation.CreateBucket },
        { "DELETE",  "/b",                "",          false, S3Operation.DeleteBucket },
        { "HEAD",    "/b",                "",          false, S3Operation.HeadBucket },
        { "POST",    "/b",                "delete",    false, S3Operation.DeleteObjects },
        // объект GET: attributes раньше uploadId раньше GetObject
        { "GET",     "/b/k",              "attributes", false, S3Operation.GetObjectAttributes },
        { "GET",     "/b/k",              "uploadId=x", false, S3Operation.ListParts },
        { "GET",     "/b/k",              "",          false, S3Operation.GetObject },
        { "HEAD",    "/b/k",              "",          false, S3Operation.HeadObject },
        // объект PUT: copy-семейство и part-семейство раньше PutObject
        { "PUT",     "/b/k",              "partNumber=1&uploadId=x", true,  S3Operation.UploadPartCopy },
        { "PUT",     "/b/k",              "partNumber=1&uploadId=x", false, S3Operation.UploadPart },
        { "PUT",     "/b/k",              "",          true,  S3Operation.CopyObject },
        { "PUT",     "/b/k",              "",          false, S3Operation.PutObject },
        // объект DELETE/POST
        { "DELETE",  "/b/k",              "uploadId=x", false, S3Operation.AbortMultipartUpload },
        { "DELETE",  "/b/k",              "",          false, S3Operation.DeleteObject },
        { "POST",    "/b/k",              "uploads",   false, S3Operation.CreateMultipartUpload },
        { "POST",    "/b/k",              "uploadId=x", false, S3Operation.CompleteMultipartUpload },
    };

    [Theory, MemberData(nameof(Routes))]
    public void Route_MatchesOperations(string method, string path, string query, bool copySource, S3Operation expected)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource);
        // Assert
        route.Operation.Should().Be(expected);
    }

    // Вне-наборные сабресурсы (глава 02 §1) → операция None + имя сабресурса (501)
    public static TheoryData<string, string, string, string> RejectedSubresources = new()
    {
        { "GET",     "/b/k", "acl",       "acl" },
        { "PUT",     "/b/k", "tagging",   "tagging" },
        { "GET",     "/b",   "versioning", "versioning" },
        { "GET",     "/b",   "lifecycle", "lifecycle" },
        { "PUT",     "/b",   "policy",    "policy" },
        { "GET",     "/b/k", "retention", "retention" },
        // attributes на не-GET — конфигурация вне контракта GetObjectAttributes
        { "PUT",     "/b/k", "attributes", "attributes" },
    };

    [Theory, MemberData(nameof(RejectedSubresources))]
    public void Route_RejectsOutOfScopeSubresources(string method, string path, string query, string subresource)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource: false);
        // Assert: сабресурс известен, но вне набора 22 операций → 501
        route.Operation.Should().Be(S3Operation.None);
        route.RejectedSubresource.Should().Be(subresource);
    }

    // Не-матч: известный путь, неподдерживаемый метод → None без сабресурса (400)
    public static TheoryData<string, string, string> UnmatchedRequests = new()
    {
        { "POST",    "/b",   "" },
        { "POST",    "/b/k", "" },
        { "PATCH",   "/b/k", "" },
        { "POST",    "/",    "" },
    };

    [Theory, MemberData(nameof(UnmatchedRequests))]
    public void Route_Unmatched_YieldsNoneWithoutSubresource(string method, string path, string query)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource: false);
        // Assert: не-матч → 400 InvalidArgument «Unsupported request» (arch-правка 6)
        route.Operation.Should().Be(S3Operation.None);
        route.RejectedSubresource.Should().BeNull();
    }
}
