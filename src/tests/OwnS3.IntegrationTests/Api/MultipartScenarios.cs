using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using OwnS3.Protocol.Auth;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.Api;

// Полный HTTP-цикл multipart (глава 02 §5) через подписанные запросы: 7 операций,
// составной ETag, Range через стыки, права/видимость, DeleteBucket-гейт, ошибки.
// Изоляция: каждый мутационный кейс — уникальный бакет b-mp-<суффикс> (admin).
public sealed class MultipartScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static readonly byte[] FiveMiB = new byte[5 * 1024 * 1024]; // нули

    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    // CreateMultipartUpload: тело канонически отсутствует, но отправляем ОДИН байт —
    // пустое body НЕ создаёт request.Content у тест-клиента, и Content-Type/
    // x-amz-meta-* не дошли бы до сервера (при этом попадая в подпись — 403).
    // Payload по умолчанию UNSIGNED-PAYLOAD, хендлер POST ?uploads тело игнорирует.
    private static async Task<string> CreateUploadAsync(OwnS3TestClient client, string bucket, string key,
        Dictionary<string, string>? headers = null, OwnS3TestClient.Credentials? credentials = null)
    {
        var response = await client.SendSignedAsync("POST", $"/{bucket}/{key}?uploads",
            headers: headers, credentials: credentials, body: " "u8.ToArray());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        return xml.Root!.Element(S3Ns + "UploadId")!.Value;
    }

    private static async Task<string> UploadPartAsync(OwnS3TestClient client, string bucket, string key,
        string uploadId, int partNumber, byte[] body)
    {
        var response = await client.SendSignedAsync("PUT", $"/{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId}",
            body: body, payloadString: Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return response.Headers.ETag!.Tag; // «"hex"»
    }

    private static async Task<string> CompleteAsync(OwnS3TestClient client, string bucket, string key,
        string uploadId, IReadOnlyList<(int Number, string ETag)> manifest)
    {
        var parts = string.Concat(manifest.Select(p =>
            $"<Part><PartNumber>{p.Number}</PartNumber><ETag>{p.ETag}</ETag></Part>"));
        var xml = $"""<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="{S3Ns}">{parts}</CompleteMultipartUpload>""";
        var response = await client.SendSignedAsync("POST", $"/{bucket}/{key}?uploadId={uploadId}",
            body: Encoding.UTF8.GetBytes(xml));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var root = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        return root.Element(S3Ns + "ETag")!.Value;
    }

    private static string CompositeEtag(params string[] hexEtags) =>
        "\"" + hexEtags.Length + "-" + Convert.ToHexString(MD5.HashData(
            Encoding.ASCII.GetBytes(string.Concat(hexEtags)))).ToLowerInvariant() + "\"";

    [Fact]
    public async Task FullCycle_CreatePartsListCompleteGetHeadDelete()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-cycle", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-cycle", "mp-obj",
            new Dictionary<string, string> { ["Content-Type"] = "text/plain", ["x-amz-meta-k"] = "v" });

        // Act: UploadPart ×3 (перезагрузка №2), ListParts (пагинация), Complete
        var e1 = await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 1, FiveMiB);
        await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 2, new byte[10]); // перезагрузка
        var tail = "abc"u8.ToArray();
        var e2 = await UploadPartAsync(client, "b-mp-cycle", "mp-obj", uploadId, 2, tail);
        var list = await client.SendSignedAsync("GET", $"/b-mp-cycle/mp-obj?uploadId={uploadId}&max-parts=1");
        var listXml = XDocument.Parse(await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var nextMarker = listXml.Root!.Element(S3Ns + "NextPartNumberMarker")!.Value;
        var list2 = await client.SendSignedAsync("GET",
            $"/b-mp-cycle/mp-obj?uploadId={uploadId}&part-number-marker={nextMarker}");
        var list2Xml = XDocument.Parse(await list2.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        list2Xml.Root!.Elements(S3Ns + "Part").Single().Element(S3Ns + "PartNumber")!.Value
            .Should().Be("2"); // перезагруженная часть — последняя версия в журнале
        var completeEtag = await CompleteAsync(client, "b-mp-cycle", "mp-obj", uploadId, [(1, e1), (2, e2)]);

        // Assert: составной ETag; GET-тело; Head-метаданные; List; повторный Complete — 404;
        // Delete; повторный Delete — 204; GET после Delete — 404 (полный цикл спеки §7)
        completeEtag.Should().Be(CompositeEtag(e1.Trim('"'), e2.Trim('"')));
        list.StatusCode.Should().Be(HttpStatusCode.OK);
        listXml.Root!.Element(S3Ns + "IsTruncated")!.Value.Should().Be("true");
        var get = await client.SendSignedAsync("GET", "/b-mp-cycle/mp-obj");
        (await get.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Length
            .Should().Be(FiveMiB.Length + 3);
        get.Headers.ETag!.Tag.Should().Be(completeEtag);
        var head = await client.SendSignedAsync("HEAD", "/b-mp-cycle/mp-obj");
        head.StatusCode.Should().Be(HttpStatusCode.OK);
        head.Content.Headers.ContentType!.ToString().Should().StartWith("text/plain");
        // List: собранный объект листится как обычный (листинг — по xl.meta, ETag составной)
        var objects = await client.SendSignedAsync("GET", "/b-mp-cycle?list-type=2&prefix=mp-obj");
        objects.StatusCode.Should().Be(HttpStatusCode.OK);
        var objXml = XDocument.Parse(await objects.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        objXml.Root!.Element(S3Ns + "KeyCount")!.Value.Should().Be("1");
        objXml.Root!.Elements(S3Ns + "Contents").Single().Element(S3Ns + "Key")!.Value
            .Should().Be("mp-obj");
        objXml.Root!.Elements(S3Ns + "Contents").Single().Element(S3Ns + "ETag")!.Value
            .Should().Be(completeEtag);
        // Повторный Complete завершённой загрузки — 404 NoSuchUpload (объект ещё жив:
        // исход именно «загрузка не существует», канон 02 §5); манифест валиден —
        // до Storage доходит именно проверка загрузки, не XML-валидация App
        var again = await CompleteAsync2(client, "b-mp-cycle", "mp-obj", uploadId, [(1, e1), (2, e2)]);
        again.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(again)).Should().Be("NoSuchUpload");
        // Delete multipart-объекта — как простого PUT (спека §1): 204 + идемпотентный повтор
        var del = await client.SendSignedAsync("DELETE", "/b-mp-cycle/mp-obj");
        del.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var delAgain = await client.SendSignedAsync("DELETE", "/b-mp-cycle/mp-obj");
        delAgain.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var getGone = await client.SendSignedAsync("GET", "/b-mp-cycle/mp-obj");
        getGone.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(getGone)).Should().Be("NoSuchKey");
    }

    [Fact]
    public async Task Range206_AcrossPartBoundary()
    {
        // Arrange: объект 5 МиБ (нули) + «ccc»; срез через стык
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-range", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-range", "obj");
        var e1 = await UploadPartAsync(client, "b-mp-range", "obj", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-range", "obj", uploadId, 2, "ccc"u8.ToArray());
        await CompleteAsync(client, "b-mp-range", "obj", uploadId, [(1, e1), (2, e2)]);

        // Act: срез (5 МиБ - 2) .. (+4) — 2 байта хвоста первой части + 3 второй
        var start = 5 * 1024 * 1024 - 2;
        var response = await client.SendSignedAsync("GET", "/b-mp-range/obj",
            headers: new Dictionary<string, string> { ["Range"] = $"bytes={start}-{start + 4}" });

        // Assert: 206, тело = стык частей; Content-Range по полному размеру
        response.StatusCode.Should().Be(HttpStatusCode.PartialContent);
        (await response.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken))
            .Should().Equal([(byte)0, (byte)0, (byte)'c', (byte)'c', (byte)'c']);
        response.Content.Headers.ContentRange!.ToString()
            .Should().Be($"bytes {start}-{start + 4}/{5 * 1024 * 1024 + 3}");
    }

    [Fact]
    public async Task Conditional_IfMatchCompositeEtag()
    {
        // Arrange: multipart-объект с составным ETag
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-cond", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-cond", "obj");
        var e1 = await UploadPartAsync(client, "b-mp-cond", "obj", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-cond", "obj", uploadId, 2, "ccc"u8.ToArray());
        var composite = await CompleteAsync(client, "b-mp-cond", "obj", uploadId, [(1, e1), (2, e2)]);

        // Act / Assert: If-Match составной → 200; чужой → 412
        var ok = await client.SendSignedAsync("GET", "/b-mp-cond/obj",
            headers: new Dictionary<string, string> { ["If-Match"] = composite });
        ok.StatusCode.Should().Be(HttpStatusCode.OK);
        var failed = await client.SendSignedAsync("GET", "/b-mp-cond/obj",
            headers: new Dictionary<string, string> { ["If-Match"] = "\"foreign\"" });
        failed.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        (await RoutingScenarios.ErrorXmlAsync(failed)).Should().Be("PreconditionFailed");
    }

    [Fact]
    public async Task Attributes_RealParts()
    {
        // Arrange: 3 части (5 МиБ, 5 МиБ, 3 байта)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-attrs", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-attrs", "obj");
        var e1 = await UploadPartAsync(client, "b-mp-attrs", "obj", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-attrs", "obj", uploadId, 2, FiveMiB);
        var e3 = await UploadPartAsync(client, "b-mp-attrs", "obj", uploadId, 3, "abc"u8.ToArray());
        var composite = await CompleteAsync(client, "b-mp-attrs", "obj", uploadId,
            [(1, e1), (2, e2), (3, e3)]);

        // Act: страница max-parts=2, продолжение за маркером (пагинация Attributes —
        // заголовки x-amz-max-parts / x-amz-part-number-marker, глава 02 §2;
        // ETag запрашиваем тоже — ответ содержит только запрошенные атрибуты)
        var page1 = await client.SendSignedAsync("GET", "/b-mp-attrs/obj?attributes",
            headers: new Dictionary<string, string>
            {
                ["x-amz-object-attributes"] = "ETag,ObjectParts",
                ["x-amz-max-parts"] = "2",
            });
        var xml1 = XDocument.Parse(await page1.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        var nextMarker = xml1.Element(S3Ns + "ObjectParts")!.Element(S3Ns + "NextPartNumberMarker")!.Value;
        var page2 = await client.SendSignedAsync("GET", "/b-mp-attrs/obj?attributes",
            headers: new Dictionary<string, string>
            {
                ["x-amz-object-attributes"] = "ObjectParts",
                ["x-amz-max-parts"] = "2",
                ["x-amz-part-number-marker"] = nextMarker,
            });
        var xml2 = XDocument.Parse(await page2.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;

        // Assert: реальные части/размеры; PartsCount=3; пагинация
        page1.StatusCode.Should().Be(HttpStatusCode.OK);
        xml1.Element(S3Ns + "ObjectParts")!.Element(S3Ns + "PartsCount")!.Value.Should().Be("3");
        xml1.Element(S3Ns + "ObjectParts")!.Element(S3Ns + "IsTruncated")!.Value.Should().Be("true");
        xml1.Element(S3Ns + "ObjectParts")!.Elements(S3Ns + "Part").Select(p => p.Element(S3Ns + "PartNumber")!.Value)
            .Should().Equal("1", "2");
        var part1Size = long.Parse(xml1.Element(S3Ns + "ObjectParts")!.Elements(S3Ns + "Part")
            .First().Element(S3Ns + "Size")!.Value);
        part1Size.Should().Be(5 * 1024 * 1024);
        xml2.Element(S3Ns + "ObjectParts")!.Elements(S3Ns + "Part").Select(p => p.Element(S3Ns + "PartNumber")!.Value)
            .Should().Equal("3");
        var etagValue = xml1.Element(S3Ns + "ETag")!.Value;
        etagValue.Should().Be(composite);
    }

    [Fact]
    public async Task UploadPartCopy_WholeAndRange()
    {
        // Arrange: источник 5 МиБ + «hello» (PUT; первая часть манифеста обязана
        // быть ≥ 5 МиБ — канон 02 §1); загрузка приёмника
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-upc", credentials: OwnS3TestClient.Admin());
        byte[] source = [.. FiveMiB, .. Encoding.UTF8.GetBytes("hello")];
        await client.SendSignedAsync("PUT", "/b-mp-upc/src", body: source);
        var uploadId = await CreateUploadAsync(client, "b-mp-upc", "dst");

        // Act: вся копия — часть 1; диапазон хвоста [1,3] — часть 2
        var whole = await client.SendSignedAsync("PUT", $"/b-mp-upc/dst?partNumber=1&uploadId={uploadId}",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/b-mp-upc/src" });
        var ranged = await client.SendSignedAsync("PUT", $"/b-mp-upc/dst?partNumber=2&uploadId={uploadId}",
            headers: new Dictionary<string, string>
            {
                ["x-amz-copy-source"] = "/b-mp-upc/src",
                ["x-amz-copy-source-range"] = $"bytes={source.Length - 3}-{source.Length - 1}",
            });
        var wholeXml = XDocument.Parse(await whole.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        var rangedXml = XDocument.Parse(await ranged.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        var e1 = wholeXml.Element(S3Ns + "ETag")!.Value.Trim('"');
        var e2 = rangedXml.Element(S3Ns + "ETag")!.Value.Trim('"');

        // Assert: ETag части 1 = md5(источника); части 2 = md5("llo")
        e1.Should().Be(Convert.ToHexString(MD5.HashData(source)).ToLowerInvariant());
        e2.Should().Be(Convert.ToHexString(MD5.HashData("llo"u8.ToArray())).ToLowerInvariant());

        // Act: Complete из обеих частей
        var composite = await CompleteAsync(client, "b-mp-upc", "dst", uploadId,
            [(1, "\"" + e1 + "\""), (2, "\"" + e2 + "\"")]);

        // Assert: тело = источник + «llo»
        composite.Should().Be(CompositeEtag(e1, e2));
        var get = await client.SendSignedAsync("GET", "/b-mp-upc/dst");
        (await get.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken))
            .Should().Equal([.. source, .. "llo"u8.ToArray()]);
    }

    [Fact]
    public async Task Copy_MultipartSource_InheritsCompositeEtag()
    {
        // Arrange: multipart-источник
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-copy", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-copy", "src");
        var e1 = await UploadPartAsync(client, "b-mp-copy", "src", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-copy", "src", uploadId, 2, "xyz"u8.ToArray());
        var composite = await CompleteAsync(client, "b-mp-copy", "src", uploadId, [(1, e1), (2, e2)]);

        // Act: CopyObject multipart-источника
        var copy = await client.SendSignedAsync("PUT", "/b-mp-copy/dst",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/b-mp-copy/src" });

        // Assert: CopyObjectResult ETag = составной ETag источника (арх-правка §3.5/Q3)
        copy.StatusCode.Should().Be(HttpStatusCode.OK);
        var copyXml = XDocument.Parse(await copy.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        copyXml.Element(S3Ns + "ETag")!.Value.Should().Be(composite);
        var head = await client.SendSignedAsync("HEAD", "/b-mp-copy/dst");
        head.Headers.ETag!.Tag.Should().Be(composite);
        head.Content.Headers.ContentLength.Should().Be(5 * 1024 * 1024 + 3);
    }

    [Fact]
    public async Task Abort_Sequence()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-abort", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-abort", "obj");
        await UploadPartAsync(client, "b-mp-abort", "obj", uploadId, 1, FiveMiB);

        // Act: Abort → 204; повторный → 404 NoSuchUpload
        var abort = await client.SendSignedAsync("DELETE", $"/b-mp-abort/obj?uploadId={uploadId}");
        abort.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var abortAgain = await client.SendSignedAsync("DELETE", $"/b-mp-abort/obj?uploadId={uploadId}");
        abortAgain.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(abortAgain)).Should().Be("NoSuchUpload");

        // Assert: Complete/UploadPart/ListParts после Abort — 404 NoSuchUpload
        var complete = await CompleteAsync2(client, "b-mp-abort", "obj", uploadId, [(1, "\"x\"")]);
        complete.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(complete)).Should().Be("NoSuchUpload");
        var part = await client.SendSignedAsync("PUT", $"/b-mp-abort/obj?partNumber=1&uploadId={uploadId}",
            body: FiveMiB);
        part.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(part)).Should().Be("NoSuchUpload");
        var list = await client.SendSignedAsync("GET", $"/b-mp-abort/obj?uploadId={uploadId}");
        list.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(list)).Should().Be("NoSuchUpload");
        // объекта нет
        var get = await client.SendSignedAsync("GET", "/b-mp-abort/obj");
        get.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task InvalidPart_Cases()
    {
        // Arrange: валидная загрузка из одной малой части
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-invpart", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-invpart", "obj");
        var e1 = await UploadPartAsync(client, "b-mp-invpart", "obj", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-invpart", "obj", uploadId, 2, "abc"u8.ToArray());

        // Act / Assert: чужой ETag
        var wrongEtag = await CompleteAsync2(client, "b-mp-invpart", "obj", uploadId,
            [(1, "\"deadbeef\""), (2, e2)]);
        wrongEtag.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(wrongEtag)).Should().Be("InvalidPart");

        // Дыра в номерах: манифест ссылается на несуществующую часть 3
        var hole = await CompleteAsync2(client, "b-mp-invpart", "obj", uploadId,
            [(1, e1), (3, "\"x\"")]);
        hole.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(hole)).Should().Be("InvalidPart");

        // Часть < 5 МиБ не последняя (загружены 2+3 байта, манифест [1,2])
        var uploadId2 = await CreateUploadAsync(client, "b-mp-invpart", "small");
        var s1 = await UploadPartAsync(client, "b-mp-invpart", "small", uploadId2, 1, new byte[3]);
        var s2 = await UploadPartAsync(client, "b-mp-invpart", "small", uploadId2, 2, new byte[3]);
        var small = await CompleteAsync2(client, "b-mp-invpart", "small", uploadId2, [(1, s1), (2, s2)]);
        small.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(small)).Should().Be("InvalidPart");
    }

    // Complete без ассерта исхода (для негативных кейсов).
    private static async Task<HttpResponseMessage> CompleteAsync2(OwnS3TestClient client, string bucket, string key,
        string uploadId, IReadOnlyList<(int Number, string ETag)> manifest)
    {
        var parts = string.Concat(manifest.Select(p =>
            $"<Part><PartNumber>{p.Number}</PartNumber><ETag>{p.ETag}</ETag></Part>"));
        var xml = $"""<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="{S3Ns}">{parts}</CompleteMultipartUpload>""";
        return await client.SendSignedAsync("POST", $"/{bucket}/{key}?uploadId={uploadId}",
            body: Encoding.UTF8.GetBytes(xml));
    }

    [Fact]
    public async Task InvalidPartOrder_App()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-order", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-order", "obj");
        var e1 = await UploadPartAsync(client, "b-mp-order", "obj", uploadId, 1, FiveMiB);
        var e2 = await UploadPartAsync(client, "b-mp-order", "obj", uploadId, 2, "ab"u8.ToArray());

        // Act: манифест 2,1 — нарушение строгого роста (валидация App t36)
        var response = await CompleteAsync2(client, "b-mp-order", "obj", uploadId, [(2, e2), (1, e1)]);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidPartOrder");
    }

    [Fact]
    public async Task EntityTooLarge_Part()
    {
        // Arrange: НЕпустое тело (байт) + ОБЫЧНЫЙ request-заголовок
        // x-amz-decoded-content-length=5368709121: уходит на провод и корректно
        // входит в подпись (ObjectHandlers.ResolveContentLength читает decoded-
        // заголовок первым, ValidateObjectSize отказывает ДО чтения тела)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-etl", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-etl", "obj");

        // Act
        var response = await client.SendSignedAsync("PUT", $"/b-mp-etl/obj?partNumber=1&uploadId={uploadId}",
            body: [0x00],
            headers: new Dictionary<string, string> { ["x-amz-decoded-content-length"] = "5368709121" });

        // Assert: 400 EntityTooLarge — до чтения тела
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("EntityTooLarge");
    }

    [Fact]
    public async Task NoSuchUpload_UnknownId()
    {
        // Arrange
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-nsu", credentials: OwnS3TestClient.Admin());

        // Act / Assert: несуществующий uploadId на UploadPart/ListParts/Complete/Abort
        var part = await client.SendSignedAsync("PUT", "/b-mp-nsu/obj?partNumber=1&uploadId=no-such",
            body: [1]);
        part.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(part)).Should().Be("NoSuchUpload");
        var list = await client.SendSignedAsync("GET", "/b-mp-nsu/obj?uploadId=no-such");
        list.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(list)).Should().Be("NoSuchUpload");
        var complete = await CompleteAsync2(client, "b-mp-nsu", "obj", "no-such", [(1, "\"x\"")]);
        complete.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(complete)).Should().Be("NoSuchUpload");
        var abort = await client.SendSignedAsync("DELETE", "/b-mp-nsu/obj?uploadId=no-such");
        abort.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(abort)).Should().Be("NoSuchUpload");
    }

    [Fact]
    public async Task ReaderRights_CreateForbidden_ListingsFiltered()
    {
        // Arrange: admin создаёт бакет; writer — загрузку; reader пытается всё читать
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-ro", credentials: OwnS3TestClient.Admin());
        var forbidden = await CreateUploadAsync2(client, "b-mp-ro", "obj",
            credentials: OwnS3TestClient.Reader());
        forbidden.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(forbidden)).Should().Be("AccessDenied");
        var uploadId = await CreateUploadAsync(client, "b-mp-ro", "obj", credentials: OwnS3TestClient.Writer());
        await UploadPartAsync(client, "b-mp-ro", "obj", uploadId, 1, FiveMiB);

        // Act: reader ListMultipartUploads → пусто (фильтр «своих»); ListParts чужой
        // uploadId → 404 NoSuchUpload; writer видит свою
        var readerList = await client.SendSignedAsync("GET", "/b-mp-ro?uploads",
            credentials: OwnS3TestClient.Reader());
        var readerListXml = XDocument.Parse(
            await readerList.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;
        readerListXml.Elements(S3Ns + "Upload").Should().BeEmpty();
        var readerParts = await client.SendSignedAsync("GET", $"/b-mp-ro/obj?uploadId={uploadId}",
            credentials: OwnS3TestClient.Reader());
        readerParts.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(readerParts)).Should().Be("NoSuchUpload");
        var writerList = await client.SendSignedAsync("GET", "/b-mp-ro?uploads",
            credentials: OwnS3TestClient.Writer());
        var writerListXml = XDocument.Parse(
            await writerList.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;

        // Assert: writer видит свою загрузку (Key/UploadId)
        var upload = writerListXml.Elements(S3Ns + "Upload").Single();
        upload.Element(S3Ns + "Key")!.Value.Should().Be("obj");
        upload.Element(S3Ns + "UploadId")!.Value.Should().Be(uploadId);
    }

    // CreateMultipartUpload без ассерта исхода (для негативных кейсов прав).
    private static async Task<HttpResponseMessage> CreateUploadAsync2(OwnS3TestClient client, string bucket,
        string key, OwnS3TestClient.Credentials credentials)
    {
        var response = await client.SendSignedAsync("POST", $"/{bucket}/{key}?uploads",
            credentials: credentials, body: " "u8.ToArray());
        return response;
    }

    [Fact]
    public async Task DeleteBucket_LiveUpload409_AfterAbort204()
    {
        // Arrange: admin — бакет, writer — загрузка
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-delb", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-delb", "obj", credentials: OwnS3TestClient.Writer());

        // Act / Assert: живая загрузка → 409 BucketNotEmpty; Abort → DELETE → 204
        var del = await client.SendSignedAsync("DELETE", "/b-mp-delb", credentials: OwnS3TestClient.Admin());
        del.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await RoutingScenarios.ErrorXmlAsync(del)).Should().Be("BucketNotEmpty");
        var abort = await client.SendSignedAsync("DELETE", $"/b-mp-delb/obj?uploadId={uploadId}",
            credentials: OwnS3TestClient.Writer());
        abort.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var delAgain = await client.SendSignedAsync("DELETE", "/b-mp-delb",
            credentials: OwnS3TestClient.Admin());
        delAgain.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task ListMultipartUploads_Xml()
    {
        // Arrange: ключи a/1, a/2, c (delimiter «/» сворачивает a/* в CP «a/»)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-list", credentials: OwnS3TestClient.Admin());
        await CreateUploadAsync(client, "b-mp-list", "a/1");
        await CreateUploadAsync(client, "b-mp-list", "a/2");
        var cUpload = await CreateUploadAsync(client, "b-mp-list", "c");

        // Act
        var response = await client.SendSignedAsync("GET", "/b-mp-list?uploads&delimiter=%2F");
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Root!;

        // Assert: Upload только «c»; CommonPrefixes «a/»; маркеры/поля на месте
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        xml.Elements(S3Ns + "Upload").Select(u => u.Element(S3Ns + "Key")!.Value).Should().Equal("c");
        xml.Elements(S3Ns + "Upload").Single().Element(S3Ns + "UploadId")!.Value.Should().Be(cUpload);
        xml.Elements(S3Ns + "CommonPrefixes").Select(p => p.Element(S3Ns + "Prefix")!.Value)
            .Should().Equal("a/");
        xml.Element(S3Ns + "IsTruncated")!.Value.Should().Be("false");
        xml.Element(S3Ns + "MaxUploads")!.Value.Should().Be("1000");
        xml.Element(S3Ns + "Bucket")!.Value.Should().Be("b-mp-list");
    }

    [Fact]
    public async Task UploadPart_ChunkedSignature_PartStored()
    {
        // Arrange: чанковая подпись тела части (полный паттерн ChunkedPut
        // BodyIntegrityScenarios — seed из подписи, тело собирает signer)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-chunked", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-chunked", "part-obj");
        var date = OwnS3AppFactory.HostTime;
        var payload = PayloadHashModeClassifier.StreamingValue;
        var host = client.Http.BaseAddress!.Authority;
        var pathAndQuery = $"/b-mp-chunked/part-obj?partNumber=1&uploadId={uploadId}";
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "5"),
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT",
            "/b-mp-chunked/part-obj", "partNumber=1&uploadId=" + uploadId, headers, payload, date, "us-east-1");
        var body = Encoding.UTF8.GetBytes("hello");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, body, signature, date, "us-east-1");
        using var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Put, pathAndQuery);
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", "5");
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: сверка конвейера прошла; ETag = md5("hello")
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Headers.ETag!.Tag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\"");

        // Assert: часть реально в журнале загрузки (ListParts — одна часть)
        var list = await client.SendSignedAsync("GET", $"/b-mp-chunked/part-obj?uploadId={uploadId}");
        list.StatusCode.Should().Be(HttpStatusCode.OK);
        XDocument.Parse(await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Root!.Elements(S3Ns + "Part").Should().ContainSingle();
    }

    [Theory]
    [InlineData("PUT", "?partNumber=1&uploadId={0}", "part")]
    [InlineData("POST", "?uploadId={0}", "manifest")]
    [InlineData("DELETE", "?uploadId={0}", "")]
    public async Task Reader_MultipartMutations_403AccessDenied(string method, string queryTemplate,
        string bodyKind)
    {
        // Arrange: admin — бакет, writer — живая загрузка; reader выполняет мутацию
        // (UploadPart с телом / Complete с манифестом / Abort)
        var client = NewClient();
        await client.SendSignedAsync("PUT", "/b-mp-ro3", credentials: OwnS3TestClient.Admin());
        var uploadId = await CreateUploadAsync(client, "b-mp-ro3", "obj", credentials: OwnS3TestClient.Writer());
        byte[]? body = bodyKind switch
        {
            "part" => Encoding.UTF8.GetBytes("part"),
            "manifest" => Encoding.UTF8.GetBytes(
                """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part></CompleteMultipartUpload>"""),
            _ => null,
        };

        // Act: read-only на мутацию — право отсекается ДО Storage (матрица 05 §3)
        var response = await client.SendSignedAsync(method, "/b-mp-ro3/obj" +
            queryTemplate.Replace("{0}", uploadId), body: body, credentials: OwnS3TestClient.Reader());

        // Assert: 403 AccessDenied; загрузка writer не пострадала (ListParts жив)
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AccessDenied");
        var list = await client.SendSignedAsync("GET", $"/b-mp-ro3/obj?uploadId={uploadId}",
            credentials: OwnS3TestClient.Writer());
        list.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
