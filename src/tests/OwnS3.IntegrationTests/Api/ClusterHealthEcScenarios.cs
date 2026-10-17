using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using OwnS3.Storage;

namespace OwnS3.IntegrationTests.Api;

// /s3/health/cluster в EC-топологии 4/1 (канон 05 §5, глава 06): 200 при всех
// writable-сетах; write-деградация сета (в 4/1 write-кворум = 3 → двух
// офлайн-дисков достаточно) → 503, offlineDrives > 0. Своя фабрика-коллекция —
// деградация не влияет на соседние EC-серии (образец OwnS3EcDegrAppFactory).
[Collection(ClusterHealthEcCollection.Name)]
public sealed class ClusterHealthEcScenarios(OwnS3EcHealthAppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());
    private ErasureXlDrive Drive => factory.Services.GetRequiredService<ErasureXlDrive>();
    private string DiskDir(int index) => factory.DataDirs[index];

    // Перевод диска в offline с копией каталога (возврат — RestoreDisk);
    // образец ErasureDegradationScenarios.
    private string TakeDiskOffline(int index)
    {
        var backup = Path.Combine(Path.GetTempPath(), "owns3-ch-backup-" + Guid.NewGuid().ToString("N"));
        CopyDirectory(DiskDir(index), backup);
        Directory.Delete(DiskDir(index), recursive: true);
        Drive.RefreshOnline();
        return backup;
    }

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

    private static async Task<JsonDocument> JsonAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(body);
    }

    [Fact]
    public async Task ClusterHealth_Ec_200_AllSetsWritable()
    {
        // Arrange / Act: здоровый пул 4/1 — анонимная проба
        var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
            TestContext.Current.CancellationToken);

        // Assert: 200 + счётчики по всем дискам всех сетов
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var json = await JsonAsync(response);
        json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(4);
        json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(0);
        json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(4);
    }

    [Fact]
    public async Task ClusterHealth_Ec_503_WhenWriteQuorumLost()
    {
        // Arrange: прогрев хоста, затем диски 2 и 3 offline (онлайн 2 < write-кворума 3)
        (await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var backup2 = TakeDiskOffline(2);
        var backup3 = TakeDiskOffline(3);
        try
        {
            // Act
            var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                TestContext.Current.CancellationToken);

            // Assert: write-деградация сета → 503, офлайн-диски видны
            response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
            using var json = await JsonAsync(response);
            json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(2);
            json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(2);
            json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(4);
        }
        finally
        {
            RestoreDisk(3, backup3);
            RestoreDisk(2, backup2);
        }
    }
}

// Фабрика health-серии: СВОЙ 4-дисковый EC-том на коллекцию.
public sealed class OwnS3EcHealthAppFactory : OwnS3EcAppFactory;

[CollectionDefinition(Name)]
public sealed class ClusterHealthEcCollection : ICollectionFixture<OwnS3EcHealthAppFactory>
{
    public const string Name = "OwnS3EcHealth";
}
