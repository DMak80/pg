using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using OwnS3.Storage;

namespace OwnS3.IntegrationTests.Api;

// Деградация host-FS в EC-топологии 4/1 (t40, план Task 15; спека §6.2):
// offline-диск читается реконструкцией и пишет кворумом; возврат диска
// доливается heal; bitrot переживается чтением и лечится; потеря кворума —
// 503 Insufficient* (не 404); EC-серии метрик присутствуют после heal.
// Каждый кейс возвращает диск на место в finally (полная чистка), том —
// отдельная коллекция, не влияющая на контрактовую серию.
[Collection(OwnS3EcDegrTestCollection.Name)]
public class ErasureDegradationScenarios(OwnS3EcDegrAppFactory factory)
{
    // детерминированное тело 2.5 МиБ (3 erasure-блока: 2 полных + хвост)
    private static byte[] Body(int seed)
    {
        var data = new byte[5 * 512 * 1024 + 123];
        new Random(seed).NextBytes(data);
        return data;
    }

    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    // Драйв хоста — публичная перепроверка online (то же, что тикер 30 с)
    private ErasureXlDrive Drive => factory.Services.GetRequiredService<ErasureXlDrive>();

    private string DiskDir(int index) => factory.DataDirs[index];

    // Перевод диска в offline с сохранением копии каталога (возврат — RestoreDisk)
    private string TakeDiskOffline(int index)
    {
        var backup = Path.Combine(Path.GetTempPath(), "owns3-degr-backup-" + Guid.NewGuid().ToString("N"));
        CopyDirectory(DiskDir(index), backup);
        Directory.Delete(DiskDir(index), recursive: true);
        Drive.RefreshOnline();
        return backup;
    }

    // Возврат диска из копии (или удаление копии, если каталог на месте)
    private void RestoreDisk(int index, string? backup)
    {
        if (backup is null)
            return;
        if (!Directory.Exists(DiskDir(index)))
            Directory.Move(backup, DiskDir(index));
        else if (Directory.Exists(backup))
            Directory.Delete(backup, recursive: true);
        Drive.RefreshOnline();
    }

    private static void CopyDirectory(string source, string dest)
    {
        Directory.CreateDirectory(dest);
        foreach (var file in Directory.EnumerateFiles(source))
            File.Copy(file, Path.Combine(dest, Path.GetFileName(file)));
        foreach (var dir in Directory.EnumerateDirectories(source))
            CopyDirectory(dir, Path.Combine(dest, Path.GetFileName(dir)));
    }

    // part.N-файлы объекта на диске (encoded-путь ключа не предполагаем — перебор)
    private static IReadOnlyList<string> FindPartFiles(string diskRoot, string bucket) =>
        Directory.EnumerateFiles(Path.Combine(diskRoot, bucket), "part.*", SearchOption.AllDirectories)
            .Where(p => Path.GetFileName(p).StartsWith("part.", StringComparison.Ordinal))
            .ToList();

    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = Environment.TickCount64 + 15_000;
        while (!condition())
        {
            Environment.TickCount64.Should().BeLessThan(deadline, "таймаут ожидания: {0}", because);
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task OfflineDisk_ReadReconstruction()
    {
        // Arrange: объект 2.5 МиБ на всех 4 дисках; диск 3 offline (каталог удалён)
        var body = Body(1);
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/degr-read", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        (await client.SendSignedAsync("PUT", "/degr-read/k", body: body))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var backup = TakeDiskOffline(3);
        try
        {
            // Act: GET/HEAD/Range/листинг при 3 онлайн (k=3)
            var get = await client.SendSignedAsync("GET", "/degr-read/k");
            var head = await client.SendSignedAsync("HEAD", "/degr-read/k");
            var range = await client.SendSignedAsync("GET", "/degr-read/k",
                headers: new Dictionary<string, string> { ["Range"] = "bytes=1-3" });
            var list = await client.SendSignedAsync("GET", "/degr-read?list-type=2");

            // Assert: реконструкция из 3 источников — байты равны исходнику
            get.StatusCode.Should().Be(HttpStatusCode.OK);
            (await get.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Should().Equal(body);
            head.StatusCode.Should().Be(HttpStatusCode.OK);
            head.Content!.Headers.ContentLength.Should().Be(body.Length);
            range.StatusCode.Should().Be(HttpStatusCode.PartialContent);
            (await range.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken))
                .Should().Equal(body[1..4]);
            list.StatusCode.Should().Be(HttpStatusCode.OK);
            (await list.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Contain("k");
        }
        finally
        {
            RestoreDisk(3, backup);
        }
    }

    [Fact]
    public async Task OfflineDisk_WriteStillWorks()
    {
        // Arrange: диск 3 offline
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/degr-write", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var backup = TakeDiskOffline(3);
        try
        {
            // Act: PUT и полный multipart-цикл при 3 онлайн (writeQuorum=3)
            var put = await client.SendSignedAsync("PUT", "/degr-write/plain", body: "v"u8.ToArray());
            var create = await client.SendSignedAsync("POST", "/degr-write/mp?uploads", body: " "u8.ToArray());
            var uploadId = System.Xml.Linq.XDocument.Parse(
                await create.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
                .Root!.Element(System.Xml.Linq.XNamespace.Get("http://s3.amazonaws.com/doc/2006-03-01/") + "UploadId")!
                .Value;
            var part = await client.SendSignedAsync("PUT",
                $"/degr-write/mp?partNumber=1&uploadId={uploadId}", body: "part"u8.ToArray());
            var manifest = """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"""
                + part.Headers.ETag!.Tag
                + """</ETag></Part></CompleteMultipartUpload>""";
            var complete = await client.SendSignedAsync("POST",
                $"/degr-write/mp?uploadId={uploadId}", body: Encoding.UTF8.GetBytes(manifest));

            // Assert: кворумная запись успешна; данные читаются реконструкцией
            put.StatusCode.Should().Be(HttpStatusCode.OK);
            complete.StatusCode.Should().Be(HttpStatusCode.OK);
            var get = await client.SendSignedAsync("GET", "/degr-write/mp");
            (await get.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Be("part");
        }
        finally
        {
            RestoreDisk(3, backup);
        }
    }

    [Fact]
    public async Task ReturnDisk_HealRestores()
    {
        // Arrange: объект A на 4 дисках; диск 3 offline → объект B пишется без него
        var bodyA = Body(3);
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/degr-heal", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        (await client.SendSignedAsync("PUT", "/degr-heal/a", body: bodyA))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var backup = TakeDiskOffline(3);
        var bodyB = "written-while-offline"u8.ToArray();
        (await client.SendSignedAsync("PUT", "/degr-heal/b", body: bodyB))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        try
        {
            // Act: возврат диска с данными (A есть, B нет) → heal доливает B
            RestoreDisk(3, backup);
            backup = null; // каталог вернулся на место из копии
            var heal = await client.SendSignedAsync("POST", "/admin/heal",
                credentials: OwnS3TestClient.Admin());
            heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
            await WaitUntilAsync(() =>
            {
                try
                {
                    // A и B — по одному part.1: двух файлов на диске 3 нет до доливки B
                    return FindPartFiles(DiskDir(3), "degr-heal").Count >= 2;
                }
                catch (DirectoryNotFoundException)
                {
                    return false;
                }
            }, "heal не долил объект B на вернувшийся диск 3");

            // Assert: статус — полнота (все online); оба объекта целы
            var status = await client.SendSignedAsync("GET", "/admin/storage",
                credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));
            status.StatusCode.Should().Be(HttpStatusCode.OK);
            var json = JsonDocument.Parse(
                await status.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            json.GetProperty("sets")[0].GetProperty("drives").EnumerateArray()
                .Should().OnlyContain(d => d.GetProperty("online").GetBoolean());
            var getA = await client.SendSignedAsync("GET", "/degr-heal/a");
            (await getA.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Should().Equal(bodyA);
            var getB = await client.SendSignedAsync("GET", "/degr-heal/b");
            (await getB.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Should().Equal(bodyB);
        }
        finally
        {
            RestoreDisk(3, backup);
        }
    }

    [Fact]
    public async Task Bitrot_ReadAliveThenHeal()
    {
        // Arrange: объект 2.5 МиБ; копия шард-файла part.1 диска 2; байт испорчен
        var body = Body(5);
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/degr-bitrot", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        (await client.SendSignedAsync("PUT", "/degr-bitrot/k", body: body))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var partPath = FindPartFiles(DiskDir(2), "degr-bitrot").Single(p => p.EndsWith("part.1", StringComparison.Ordinal));
        var pristine = await File.ReadAllBytesAsync(partPath, TestContext.Current.CancellationToken);
        await using (var stream = File.Open(partPath, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Position = pristine.Length / 2;
            stream.WriteByte((byte)(pristine[pristine.Length / 2] ^ 0xFF)); // порча шард-блока
        }

        // Act: GET жив (bitrot → реконструкция из k источников)
        var get = await client.SendSignedAsync("GET", "/degr-bitrot/k");

        // Assert: байты равны исходнику
        get.StatusCode.Should().Be(HttpStatusCode.OK);
        (await get.Content.ReadAsByteArrayAsync(TestContext.Current.CancellationToken)).Should().Equal(body);

        // Act: heal перезаписывает битый шард реконструированным (tmp+rename)
        var heal = await client.SendSignedAsync("POST", "/admin/heal",
            credentials: OwnS3TestClient.Admin());
        heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
        await WaitUntilAsync(() =>
            File.ReadAllBytes(partPath).SequenceEqual(pristine),
            "heal не восстановил битый шард диска 2");

        // Assert: файл побайтово равен исходному шард-потоку
        (await File.ReadAllBytesAsync(partPath, TestContext.Current.CancellationToken)).Should().Equal(pristine);
    }

    [Fact]
    public async Task TwoDisksLost_503Not404()
    {
        // Arrange: объект на 4 дисках; диски 2 и 3 offline (онлайн 2 < k=3)
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/degr-503", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        (await client.SendSignedAsync("PUT", "/degr-503/k", body: "x"u8.ToArray()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var backup2 = TakeDiskOffline(2);
        var backup3 = TakeDiskOffline(3);
        try
        {
            // Act: GET/PUT/ListObjects при потере read/write-кворума
            var get = await client.SendSignedAsync("GET", "/degr-503/k");
            var put = await client.SendSignedAsync("PUT", "/degr-503/k2", body: "y"u8.ToArray());
            var list = await client.SendSignedAsync("GET", "/degr-503?list-type=2");

            // Assert: 503 Insufficient*Quorum (НЕ 404 — кворумная недоступность);
            // PUT отказывает на бакетной границе — read-кворум существования
            // (EnsureBucket), до стадии записи с write-кворумом
            get.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
            (await RoutingScenarios.ErrorXmlAsync(get)).Should().Be("InsufficientReadQuorum");
            put.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
            (await RoutingScenarios.ErrorXmlAsync(put)).Should().Be("InsufficientReadQuorum");
            list.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
            (await RoutingScenarios.ErrorXmlAsync(list)).Should().Be("InsufficientReadQuorum");
        }
        finally
        {
            RestoreDisk(3, backup3);
            RestoreDisk(2, backup2);
        }
    }

    [Fact]
    public async Task Metrics_EcSeriesPresent()
    {
        // Arrange: EC-хост поднялся, старт-проход heal уже прошёл
        using var client = factory.CreateClient();
        var admin = NewClient();

        // Act: внеплановый heal-проход + поллинг scrape (тик reader OTel)
        var heal = await admin.SendSignedAsync("POST", "/admin/heal",
            credentials: OwnS3TestClient.Admin());
        heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var deadline = Environment.TickCount64 + 15_000;
        string body;
        while (true)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("ownS3_heal_passes_total", StringComparison.Ordinal)
                || Environment.TickCount64 >= deadline)
                break;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        // Assert: EC-серии экспорта присутствуют (гейджи + счётчик heal)
        body.Should().Contain("ownS3_ec_set_readable");
        body.Should().Contain("ownS3_ec_drive_online");
        body.Should().Contain("ownS3_heal_passes_total");
    }
}

// Фабрика деградационной серии: СВОЙ 4-дисковый EC-том на коллекцию —
// деградация не влияет на контрактовую серию OwnS3Ec.
public sealed class OwnS3EcDegrAppFactory : OwnS3EcAppFactory;

[CollectionDefinition(Name)]
public sealed class OwnS3EcDegrTestCollection : ICollectionFixture<OwnS3EcDegrAppFactory>
{
    public const string Name = "OwnS3EcDegr";
}
