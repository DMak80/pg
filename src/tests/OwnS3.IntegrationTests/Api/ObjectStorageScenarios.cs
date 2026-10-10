using System.Net;
using System.Text;
using System.Xml.Linq;
using OwnS3.Protocol.Auth;

namespace OwnS3.IntegrationTests.Api;

// Полный HTTP-цикл 15 операций t37 через подписанные запросы (спека §8):
// Put/Head/Get/List(v1/V2/Versions)/Copy/Attributes/Delete/DeleteObjects,
// Range/conditional исходы, вложенные ключи, байтовый порядок листинга,
// multipart — 500 до t38. Правило имён: каждый кейс — уникальный бакет
// СВОЕГО кейса (ограничение 8); multipart — на никогда не создаваемом «bucket».
public sealed class ObjectStorageScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private static readonly string HelloEtag = "\"5d41402abc4b2a76b9719d911017c592\""; // md5("hello")

    // Namespace S3-XML-моделей листингов/копии/атрибутов (Element("X") без ns
    // в этих документах даёт null).
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    private static async Task<string> BodyAsync(HttpResponseMessage response) =>
        await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

    private static async Task<string> ErrorXmlAsync(HttpResponseMessage response) =>
        await RoutingScenarios.ErrorXmlAsync(response);

    [Fact]
    public async Task FullCycle_PutHeadGetListCopyAttributesDeleteDeleteObjects()
    {
        // Arrange
        var client = NewClient();

        // Act / Assert: создание бакета (admin)
        var create = await client.SendSignedAsync("PUT", "/b-cycle", credentials: OwnS3TestClient.Admin());
        create.StatusCode.Should().Be(HttpStatusCode.OK);

        // PUT "hello" + user-meta + Content-Type
        var put = await client.SendSignedAsync("PUT", "/b-cycle/obj-key",
            headers: new Dictionary<string, string>
            {
                ["Content-Type"] = "text/plain",
                ["x-amz-meta-a"] = "b",
            },
            body: Encoding.UTF8.GetBytes("hello"));
        put.StatusCode.Should().Be(HttpStatusCode.OK);
        put.Headers.ETag!.Tag.Should().Be(HelloEtag);

        // HEAD: заголовки без тела
        var head = await client.SendSignedAsync("HEAD", "/b-cycle/obj-key");
        head.StatusCode.Should().Be(HttpStatusCode.OK);
        head.Content!.Headers.ContentLength.Should().Be(5);
        head.Content.Headers.ContentType!.MediaType.Should().Be("text/plain");
        head.Headers.ETag!.Tag.Should().Be(HelloEtag);
        (await BodyAsync(head)).Should().BeEmpty("HEAD — без тела");

        // GET: тело/ETag/Last-Modified/user-meta
        var get = await client.SendSignedAsync("GET", "/b-cycle/obj-key");
        get.StatusCode.Should().Be(HttpStatusCode.OK);
        (await BodyAsync(get)).Should().Be("hello");
        get.Headers.ETag!.Tag.Should().Be(HelloEtag);
        get.Content!.Headers.LastModified.Should().Be(OwnS3AppFactory.HostTime);
        get.Headers.Should().Contain(h => h.Key == "x-amz-meta-a" && h.Value.Single() == "b");

        // вложенный юникод-ключ (спека §8: спецсимволы хотя бы в одном кейсе)
        var putUni = await client.SendSignedAsync("PUT", "/b-cycle/nested/caf%C3%A9.txt",
            body: Encoding.UTF8.GetBytes("юникод"));
        putUni.StatusCode.Should().Be(HttpStatusCode.OK);
        var getUni = await client.SendSignedAsync("GET", "/b-cycle/nested/caf%C3%A9.txt");
        (await BodyAsync(getUni)).Should().Be("юникод");

        // List v1: оба ключа в Contents
        var listV1 = await client.SendSignedAsync("GET", "/b-cycle");
        var v1 = XDocument.Parse(await BodyAsync(listV1));
        v1.Root!.Name.LocalName.Should().Be("ListBucketResult");
        v1.Root.Elements().Where(e => e.Name.LocalName == "Contents")
            .Select(e => e.Element(S3Ns + "Key")!.Value).Should().Equal("nested/café.txt", "obj-key");

        // List V2: KeyCount
        var listV2 = await client.SendSignedAsync("GET", "/b-cycle?list-type=2");
        var v2 = XDocument.Parse(await BodyAsync(listV2));
        v2.Root!.Element(S3Ns + "KeyCount")!.Value.Should().Be("2");

        // List Versions: тот же обход в variants-обёртке
        var versions = await client.SendSignedAsync("GET", "/b-cycle?versions");
        var vv = XDocument.Parse(await BodyAsync(versions));
        vv.Root!.Name.LocalName.Should().Be("ListVersionsResult");
        vv.Root.Elements().Where(e => e.Name.LocalName == "Version")
            .Select(e => e.Element(S3Ns + "Key")!.Value).Should().Equal("nested/café.txt", "obj-key");

        // Copy: новый объект — копия
        var copy = await client.SendSignedAsync("PUT", "/b-cycle/copy-key",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/b-cycle/obj-key" });
        copy.StatusCode.Should().Be(HttpStatusCode.OK);
        var copyXml = XDocument.Parse(await BodyAsync(copy));
        copyXml.Root!.Name.LocalName.Should().Be("CopyObjectResult");
        copyXml.Root.Element(S3Ns + "ETag")!.Value.Should().Be(HelloEtag); // наследован
        var getCopy = await client.SendSignedAsync("GET", "/b-cycle/copy-key");
        (await BodyAsync(getCopy)).Should().Be("hello");

        // GetObjectAttributes: запрошенные атрибуты + заголовок ETag quoted
        var attrs = await client.SendSignedAsync("GET", "/b-cycle/obj-key?attributes",
            headers: new Dictionary<string, string> { ["x-amz-object-attributes"] = "ETag,ObjectSize,ObjectParts" });
        attrs.StatusCode.Should().Be(HttpStatusCode.OK);
        attrs.Headers.ETag!.Tag.Should().Be(HelloEtag);
        var attrsXml = XDocument.Parse(await BodyAsync(attrs));
        attrsXml.Root!.Name.LocalName.Should().Be("GetObjectAttributesOutput");
        attrsXml.Root.Element(S3Ns + "ETag")!.Value.Should().Be(HelloEtag);
        attrsXml.Root.Element(S3Ns + "ObjectSize")!.Value.Should().Be("5");
        attrsXml.Root.Element(S3Ns + "ObjectParts")!.Element(S3Ns + "PartsCount")!.Value.Should().Be("1");

        // Delete: 204
        var delete = await client.SendSignedAsync("DELETE", "/b-cycle/obj-key");
        delete.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var afterDelete = await client.SendSignedAsync("GET", "/b-cycle/obj-key");
        afterDelete.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(afterDelete)).Should().Be("NoSuchKey");

        // DeleteObjects: XML Delete/DeleteResult (одна ошибка + один успех)
        var batch = """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>copy-key</Key></Object><Object><Key>no-such</Key></Object><Quiet>false</Quiet></Delete>""";
        var multi = await client.SendSignedAsync("POST", "/b-cycle?delete",
            body: Encoding.UTF8.GetBytes(batch));
        multi.StatusCode.Should().Be(HttpStatusCode.OK);
        var multiXml = XDocument.Parse(await BodyAsync(multi));
        multiXml.Root!.Name.LocalName.Should().Be("DeleteResult");
        multiXml.Root.Elements().Where(e => e.Name.LocalName == "Deleted")
            .Select(e => e.Element(S3Ns + "Key")!.Value).Should().Equal("copy-key", "no-such");

        // остатний объект (юникод-ключ) — удалить перед удалением бакета
        await client.SendSignedAsync("DELETE", "/b-cycle/nested/caf%C3%A9.txt");

        // HeadBucket 200 → DeleteBucket 204 (бакет опустел)
        var headBucket = await client.SendSignedAsync("HEAD", "/b-cycle");
        headBucket.StatusCode.Should().Be(HttpStatusCode.OK);
        var deleteBucket = await client.SendSignedAsync("DELETE", "/b-cycle",
            credentials: OwnS3TestClient.Admin());
        deleteBucket.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Range_Get_206ContentRange_416InvalidRange()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-range", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-range/rng",
            body: Encoding.UTF8.GetBytes("abcdef"));

        // Act: диапазон 2-3
        var slice = await client.SendSignedAsync("GET", "/b-range/rng",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=2-3" });

        // Assert: 206 + Content-Range + срез
        slice.StatusCode.Should().Be(HttpStatusCode.PartialContent);
        slice.Content!.Headers.ContentRange!.ToString().Should().Be("bytes 2-3/6");
        (await BodyAsync(slice)).Should().Be("cd");

        // Act: start за размером → 416
        var oob = await client.SendSignedAsync("GET", "/b-range/rng",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=100-" });

        // Assert
        oob.StatusCode.Should().Be(HttpStatusCode.RequestedRangeNotSatisfiable);
        (await ErrorXmlAsync(oob)).Should().Be("InvalidRange");

        // Act: пустой объект + Range → 416
        await client.SendSignedAsync("PUT", "/b-range/empty", body: []);
        var empty = await client.SendSignedAsync("GET", "/b-range/empty",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=0-" });

        // Assert
        empty.StatusCode.Should().Be(HttpStatusCode.RequestedRangeNotSatisfiable);
    }

    [Fact]
    public async Task Conditional_Get_304WithoutBody_WithHeaders_412()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-cond", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-cond/c",
            body: Encoding.UTF8.GetBytes("hello"));
        var etag = HelloEtag;

        // Act: If-None-Match совпал → 304 без тела с заголовками идентичности
        var notModified = await client.SendSignedAsync("GET", "/b-cond/c",
            headers: new Dictionary<string, string> { ["If-None-Match"] = etag });

        // Assert
        notModified.StatusCode.Should().Be(HttpStatusCode.NotModified);
        notModified.Headers.ETag!.Tag.Should().Be(etag);
        notModified.Content!.Headers.LastModified.Should().Be(OwnS3AppFactory.HostTime);
        (await BodyAsync(notModified)).Should().BeEmpty("304 — без тела");

        // Act: If-Match не совпал → 412
        var failed = await client.SendSignedAsync("GET", "/b-cond/c",
            headers: new Dictionary<string, string> { ["If-Match"] = "\"other\"" });

        // Assert
        failed.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        (await ErrorXmlAsync(failed)).Should().Be("PreconditionFailed");

        // Act: If-Range совпал → 206; не совпал → 200 полным
        var ifRangeMatch = await client.SendSignedAsync("GET", "/b-cond/c",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=2-3", ["If-Range"] = etag });
        var ifRangeMiss = await client.SendSignedAsync("GET", "/b-cond/c",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=2-3", ["If-Range"] = "\"other\"" });

        // Assert
        ifRangeMatch.StatusCode.Should().Be(HttpStatusCode.PartialContent);
        (await BodyAsync(ifRangeMatch)).Should().Be("ll"); // bytes 2-3 объекта «hello»
        ifRangeMiss.StatusCode.Should().Be(HttpStatusCode.OK);
        (await BodyAsync(ifRangeMiss)).Should().Be("hello");
    }

    [Fact]
    public async Task Head_Range_206WithoutBody()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-head", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-head/h",
            body: Encoding.UTF8.GetBytes("abcdef"));

        // Act
        var head = await client.SendSignedAsync("HEAD", "/b-head/h",
            headers: new Dictionary<string, string> { ["Range"] = "bytes=2-3" });

        // Assert: 206 с Content-Range/Content-Length диапазона, тела нет
        head.StatusCode.Should().Be(HttpStatusCode.PartialContent);
        head.Content!.Headers.ContentRange!.ToString().Should().Be("bytes 2-3/6");
        head.Content.Headers.ContentLength.Should().Be(2);
        (await BodyAsync(head)).Should().BeEmpty();
    }

    [Fact]
    public async Task Attributes_Conditional_304_412()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-attrs", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-attrs/a",
            body: Encoding.UTF8.GetBytes("hello"));
        var attrHeaders = new Dictionary<string, string>
        {
            ["x-amz-object-attributes"] = "ETag,ObjectSize",
        };

        // Act: If-None-Match совпал → 304 без тела с ETag/Last-Modified (P10)
        var notModified = await client.SendSignedAsync("GET", "/b-attrs/a?attributes",
            headers: MergeHeaders(attrHeaders, new Dictionary<string, string> { ["If-None-Match"] = HelloEtag }));

        // Assert
        notModified.StatusCode.Should().Be(HttpStatusCode.NotModified);
        notModified.Headers.ETag!.Tag.Should().Be(HelloEtag);
        (await BodyAsync(notModified)).Should().BeEmpty();

        // Act: If-Match не совпал → 412
        var failed = await client.SendSignedAsync("GET", "/b-attrs/a?attributes",
            headers: MergeHeaders(attrHeaders, new Dictionary<string, string> { ["If-Match"] = "\"other\"" }));

        // Assert
        failed.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        (await ErrorXmlAsync(failed)).Should().Be("PreconditionFailed");
    }

    [Fact]
    public async Task Errors_NoSuchBucket_NoSuchKey_XmlCodes()
    {
        // Arrange: бакет «bucket» никогда не создаётся; созданный b-err — без ключа
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-err", credentials: OwnS3TestClient.Admin());

        // Act / Assert: несозданный бакет
        var noBucket = await client.SendSignedAsync("GET", "/bucket/k");
        noBucket.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(noBucket)).Should().Be("NoSuchBucket");

        // Act / Assert: созданный бакет, ключа нет
        var noKey = await client.SendSignedAsync("GET", "/b-err/no-such-key");
        noKey.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(noKey)).Should().Be("NoSuchKey");
    }

    [Fact]
    public async Task CreateBucket_Twice_409BucketAlreadyOwnedByYou()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-409", credentials: OwnS3TestClient.Admin());

        // Act
        var again = await client.SendSignedAsync("PUT", "/b-409", credentials: OwnS3TestClient.Admin());

        // Assert
        again.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ErrorXmlAsync(again)).Should().Be("BucketAlreadyOwnedByYou");
    }

    [Fact]
    public async Task DeleteBucket_NonEmpty_409BucketNotEmpty()
    {
        // Arrange: бакет с живым объектом
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-nonempty", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-nonempty/k", body: Encoding.UTF8.GetBytes("x"));

        // Act
        var delete = await client.SendSignedAsync("DELETE", "/b-nonempty",
            credentials: OwnS3TestClient.Admin());

        // Assert
        delete.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ErrorXmlAsync(delete)).Should().Be("BucketNotEmpty");
    }

    [Fact]
    public async Task DeleteBucket_Missing_404NoSuchBucket()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("DELETE", "/b-no-such-bucket",
            credentials: OwnS3TestClient.Admin());

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task PutObject_Overwrite_EtagChanges_LastContentWins()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-ow", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-ow/k", body: Encoding.UTF8.GetBytes("v1"));

        // Act
        var second = await client.SendSignedAsync("PUT", "/b-ow/k", body: Encoding.UTF8.GetBytes("v2"));

        // Assert: содержимое побеждает последней записью, ETag изменился
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        second.Headers.ETag!.Tag.Should().NotBe(HelloEtag); // md5("hello") — от v1 не осталось
        var get = await client.SendSignedAsync("GET", "/b-ow/k");
        (await BodyAsync(get)).Should().Be("v2");
    }

    [Fact]
    public async Task NestedKeys_PutGetDelete()
    {
        // Arrange: a и a/b — оба читаются
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-nested", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-nested/a", body: Encoding.UTF8.GetBytes("flat"));
        await client.SendSignedAsync("PUT", "/b-nested/a/b", body: Encoding.UTF8.GetBytes("nested"));

        // Act / Assert: оба живы
        var flat = await client.SendSignedAsync("GET", "/b-nested/a");
        (await BodyAsync(flat)).Should().Be("flat");
        var nested = await client.SendSignedAsync("GET", "/b-nested/a/b");
        (await BodyAsync(nested)).Should().Be("nested");

        // Act: DELETE a не трогает a/b (канон 04 §4 п.5)
        await client.SendSignedAsync("DELETE", "/b-nested/a");

        // Assert
        var nestedAfter = await client.SendSignedAsync("GET", "/b-nested/a/b");
        nestedAfter.StatusCode.Should().Be(HttpStatusCode.OK);
        (await BodyAsync(nestedAfter)).Should().Be("nested");
        var flatAfter = await client.SendSignedAsync("GET", "/b-nested/a");
        flatAfter.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Listings_ByteOrder_Paging_Delimiter()
    {
        // Arrange: строгий байтовый порядок + delimiter/CP + пейджинг
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-list", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/b-list/a", body: Encoding.UTF8.GetBytes("x"));
        await client.SendSignedAsync("PUT", "/b-list/a%21z", body: Encoding.UTF8.GetBytes("x"));
        await client.SendSignedAsync("PUT", "/b-list/a/m", body: Encoding.UTF8.GetBytes("x"));
        await client.SendSignedAsync("PUT", "/b-list/ab", body: Encoding.UTF8.GetBytes("x"));
        foreach (var k in new[] { "logs/x1", "logs/x2", "logs/x3", "logs/x4", "root" })
            await client.SendSignedAsync("PUT", "/b-list/" + k, body: Encoding.UTF8.GetBytes("x"));

        // Act: порядок (a < a!z < a/m < ab — байтовый, решение пользователя)
        var plain = await client.SendSignedAsync("GET", "/b-list");
        var xml = XDocument.Parse(await BodyAsync(plain));
        xml.Root!.Elements().Where(e => e.Name.LocalName == "Contents")
            .Select(e => e.Element(S3Ns + "Key")!.Value)
            .Should().Equal("a", "a!z", "a/m", "ab", "logs/x1", "logs/x2", "logs/x3", "logs/x4", "root");

        // Act: delimiter → CP a/ (из a/m) и logs/ + Contents без свёрнутых
        var folded = await client.SendSignedAsync("GET", "/b-list?delimiter=%2F");
        var foldXml = XDocument.Parse(await BodyAsync(folded));
        foldXml.Root!.Elements().Where(e => e.Name.LocalName == "CommonPrefixes")
            .Select(e => e.Element(S3Ns + "Prefix")!.Value).Should().Equal("a/", "logs/");
        foldXml.Root.Elements().Where(e => e.Name.LocalName == "Contents")
            .Select(e => e.Element(S3Ns + "Key")!.Value).Should().Equal("a", "a!z", "ab", "root");

        // Act: V2 maxKeys=1 → токен → вторая страница
        var page1 = await client.SendSignedAsync("GET", "/b-list?list-type=2&max-keys=1");
        var p1 = XDocument.Parse(await BodyAsync(page1));
        p1.Root!.Element(S3Ns + "KeyCount")!.Value.Should().Be("1");
        p1.Root.Element(S3Ns + "IsTruncated")!.Value.Should().Be("true");
        var token = p1.Root.Element(S3Ns + "NextContinuationToken")!.Value;
        var page2 = await client.SendSignedAsync("GET",
            "/b-list?list-type=2&continuation-token=" + Uri.EscapeDataString(token));
        var p2 = XDocument.Parse(await BodyAsync(page2));
        p2.Root!.Element(S3Ns + "Contents")!.Element(S3Ns + "Key")!.Value.Should().Be("a!z");

        // Act: v1 marker
        var v1 = await client.SendSignedAsync("GET", "/b-list?marker=a%2Fm");
        var v1Xml = XDocument.Parse(await BodyAsync(v1));
        v1Xml.Root!.Elements().Where(e => e.Name.LocalName == "Contents")
            .Select(e => e.Element(S3Ns + "Key")!.Value).Should().Equal("ab", "logs/x1", "logs/x2", "logs/x3", "logs/x4", "root");
    }

    [Fact]
    public async Task Multipart_Operations_Still500()
    {
        // Arrange: бакет «bucket» никогда не создаётся — до Storage дело не доходит,
        // заглушки multipart-методов отвечают 500 InternalError (критерий §10.8)
        var client = NewClient();

        // Act
        var create = await client.SendSignedAsync("POST", "/bucket/key?uploads", body: []);
        var part = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            body: Encoding.UTF8.GetBytes("part"));

        // Assert
        create.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        (await ErrorXmlAsync(create)).Should().Be("InternalError");
        part.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        (await ErrorXmlAsync(part)).Should().Be("InternalError");
    }

    // Слияние словарей заголовков (короткий локальный хелпер кейса Attributes).
    private static Dictionary<string, string> MergeHeaders(Dictionary<string, string> first,
        Dictionary<string, string> second)
    {
        var result = new Dictionary<string, string>(first);
        foreach (var (name, value) in second)
            result[name] = value;
        return result;
    }
}
