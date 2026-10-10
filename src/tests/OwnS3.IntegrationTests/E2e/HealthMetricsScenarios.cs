namespace OwnS3.IntegrationTests.E2e;

// Health/метрики ownS3 в докер-контуре (t39, arch/owns3/05 §5): healthz —
// доступность без аутентификации (том жив); metrics-факт — Задача 9 плана.
[Collection(OwnS3E2eCollection.Name)]
public sealed class HealthMetricsScenarios
{
    [Fact]
    public async Task Healthz_200_NoSignature()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: служебный путь вне S3-конвейера (подпись не нужна), том жив
        await OwnS3E2eEnvironment.RunScenarioAsync("hm1", async fx =>
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

            // Act
            var response = await http.GetAsync($"{fx.S3BaseUrl}/healthz", ct);

            // Assert
            response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
                "healthz не требует подписи и отвечает 200 на живом томе (канон 05 §5)");
        }, ct);
    }
}
