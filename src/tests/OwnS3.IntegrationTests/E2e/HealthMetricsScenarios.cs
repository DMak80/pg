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

    [Fact]
    public async Task Metrics_Prometheus_Series_AfterTraffic()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: SDK-трафик (PUT/GET/LIST) → /metrics несёт каталожные серии
        // канона 05 §5 (requests_total с operation/code, duration-гистограмма,
        // дисковые gauge); OTel reader периодический — полл (0.5 c, бюджет 30 c)
        await OwnS3E2eEnvironment.RunScenarioAsync("hm2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}hm";
            await admin.PutBucketAsync(new Amazon.S3.Model.PutBucketRequest { BucketName = bucket }, ct);
            var body = OwnS3E2eBytes.Deterministic(4096, 77);
            await s3.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
            {
                BucketName = bucket, Key = "metrics.bin", InputStream = new MemoryStream(body),
            }, ct);
            using var get = await s3.GetObjectAsync(
                new Amazon.S3.Model.GetObjectRequest { BucketName = bucket, Key = "metrics.bin" }, ct);
            await s3.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
                { BucketName = bucket }, ct);

            // Act: полл /metrics (plain HttpClient, без подписи) до всех паттернов
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            var deadline = Environment.TickCount64 + 30_000;
            string metrics = "";
            while (true)
            {
                metrics = await http.GetStringAsync($"{fx.S3BaseUrl}/metrics", ct);
                var hasAll = metrics.Contains("ownS3_requests_total", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"PutObject\"", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"GetObject\"", StringComparison.Ordinal)
                    && metrics.Contains("operation=\"ListObjectsV2\"", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_request_duration_seconds_bucket", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_disk_used_bytes", StringComparison.Ordinal)
                    && metrics.Contains("ownS3_disk_total_bytes", StringComparison.Ordinal);
                if (hasAll || Environment.TickCount64 >= deadline)
                    break;
                await Task.Delay(500, ct);
            }

            // Assert: серии и лейблы (форма лейблов OTel — порядок переменный,
            // поэтому отдельные подстроки)
            metrics.Should().Contain("ownS3_requests_total");
            metrics.Should().Contain("operation=\"PutObject\"", "PUT был; вывод:\n{0}", metrics);
            metrics.Should().Contain("operation=\"GetObject\"");
            metrics.Should().Contain("operation=\"ListObjectsV2\"");
            metrics.Should().Contain("code=\"200\"", "все запросы сценария успешны");
            metrics.Should().Contain("ownS3_request_duration_seconds_bucket");
            metrics.Should().Contain("ownS3_disk_used_bytes");
            metrics.Should().Contain("ownS3_disk_total_bytes");

            // Assert: gauge-значения (used > 0 после PUT; total >= used)
            var used = ParseGauge(metrics, "ownS3_disk_used_bytes");
            var total = ParseGauge(metrics, "ownS3_disk_total_bytes");
            used.Should().BeGreaterThan(0, "после PUT том занят (снимок: {0})", metrics);
            total.Should().BeGreaterThanOrEqualTo(used);
        }, ct);
    }

    // Gauge-строка Prometheus OTel: «имя{лейблы} значение» (лейблы могут
    // отсутствовать — «имя значение»); значение — последний токен строки.
    private static double ParseGauge(string metrics, string name)
    {
        var line = metrics.Split('\n')
            .FirstOrDefault(l => l.StartsWith(name + " ", StringComparison.Ordinal)
                                 || l.StartsWith(name + "{", StringComparison.Ordinal))
            ?? throw new Xunit.Sdk.XunitException($"gauge {name} не найден в /metrics");
        var valuePart = line.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)[^1];
        return double.Parse(valuePart,
            System.Globalization.CultureInfo.InvariantCulture);
    }
}
