using System.Net;
using System.Text.Json;

namespace OwnS3.IntegrationTests.Api;

// /s3/health/cluster (канон 05 §5, критерии §9–§11): публичная проба
// (без подписи) готовности хранилища обслуживать запись — single-режим.
// 200/503 + JSON drives-счётчики camelCase; шаг 0 конвейера — до роутинга
// 21 операции (анонимный запрос не получает 403). Изоляция 503-кейса —
// tmp-каталог тома с восстановлением (образец HealthzVolumeScenarios).
public sealed class ClusterHealthScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private static async Task<JsonDocument> JsonAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(body);
    }

    [Fact]
    public async Task ClusterHealth_200_JsonFields_NoSignature()
    {
        // Arrange / Act: анонимный GET (никаких Authorization-заголовков)
        var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
            TestContext.Current.CancellationToken);

        // Assert: 200 (не 403 — путь перехвачен до роутинга), поля camelCase
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var json = await JsonAsync(response);
        json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(1);
        json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(0);
        json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(1);
        json.RootElement.TryGetProperty("healingDrives", out _).Should().BeFalse(
            "heal-статус — в /admin/storage, не в кластер-health");
    }

    [Fact]
    public async Task ClusterHealth_503_JsonFields_WhenVolumeUnavailable()
    {
        // Arrange: прогрев (ленивый старт хоста — как HealthzVolumeScenarios),
        // затем ломаем том удалением tmp-каталога
        (await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var tmpDir = Path.Combine(factory.TempVolumeDir, ".owns3.sys", "tmp");
        if (Directory.Exists(tmpDir))
            Directory.Delete(tmpDir, recursive: true);
        try
        {
            // Act
            var response = await factory.CreateClient().GetAsync("/s3/health/cluster",
                TestContext.Current.CancellationToken);

            // Assert: 503 + счётчики отказа (0/1/1)
            response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
            using var json = await JsonAsync(response);
            json.RootElement.GetProperty("healthyDrives").GetInt64().Should().Be(0);
            json.RootElement.GetProperty("offlineDrives").GetInt64().Should().Be(1);
            json.RootElement.GetProperty("totalDrives").GetInt64().Should().Be(1);
        }
        finally
        {
            // Teardown: восстановить структуру для соседних кейсов класса
            Directory.CreateDirectory(tmpDir);
        }
    }
}
