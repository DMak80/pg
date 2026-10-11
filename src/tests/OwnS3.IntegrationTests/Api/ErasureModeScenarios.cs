namespace OwnS3.IntegrationTests.Api;

// EC-режим приложения (t40, план Task 14): healthz поднимается, метрики EC
// присутствуют после трафика. Контракт 21 операции в EC — Task 15.
[Collection(OwnS3EcTestCollection.Name)]
public class ErasureModeScenarios(OwnS3EcAppFactory factory)
{
    [Fact]
    public async Task Healthz_EcMode_200()
    {
        // Arrange — EC-хост поднялся (фабрика), формат валиден
        using var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/healthz", TestContext.Current.CancellationToken);

        // Assert — формат валиден + touch каждого онлайн-диска
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
    }

    [Fact]
    public async Task Metrics_EcSeriesPresent()
    {
        // Arrange — метрики доступны без подписи
        using var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert — EC-серии присутствуют (гейджи наборов)
        body.Should().Contain("ownS3_ec_set_readable");
        body.Should().Contain("ownS3_ec_drive_online");
        body.Should().Contain("ownS3_disk_total_bytes");
    }
}
