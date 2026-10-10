using System.Net;

namespace OwnS3.IntegrationTests.Api;

// /healthz по состоянию тома (спека §6.2, критерий §10.7): 200 на валидном томе;
// 503 при недоступном. Изоляция кейсов: 503-кейс ломает только tmp-каталог тома
// (touch-проба падает) и восстанавливает его — порядок кейсов не важен.
public sealed class HealthzVolumeScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    [Fact]
    public async Task Healthz_200OnValidVolume()
    {
        // Arrange / Act
        var response = await factory.CreateClient().GetAsync("/healthz",
            TestContext.Current.CancellationToken);

        // Assert: volume.json валиден + touch-проба записи проходит
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Healthz_503WhenVolumeUnavailable()
    {
        // Arrange: tmp-каталог недоступен — touch-проба записи невозможна
        var tmpDir = Path.Combine(factory.TempVolumeDir, ".owns3.sys", "tmp");
        if (Directory.Exists(tmpDir))
            Directory.Delete(tmpDir, recursive: true);
        try
        {
            // Act
            var response = await factory.CreateClient().GetAsync("/healthz",
                TestContext.Current.CancellationToken);

            // Assert: недоступный том → 503
            response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        }
        finally
        {
            // Teardown: восстановить структуру для соседних кейсов класса
            Directory.CreateDirectory(tmpDir);
        }
    }
}
