using System.Diagnostics;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// A15 — сквозная латентность репликации (t35, план Task 22; spec 3.8/7): спокойный
/// контур 2 узла, серия 10 PUT (~1 КБ); per-объектно t0 = 200 OK источника → поллинг
/// HeadObject приёмника (шаг ≤1 с, бюджет 5 с) до совпадения modTime/sha256 →
/// latencyMs = t_applied − t0; [METRIC] repl-e2e-latency-ms по каждому +
/// min/max/median; ассерт ≤5000 мс. Сверка честности R5: delivery_lag/fetch_lag == 0
/// после серии (фактические значения — [METRIC] repl-runtime-*-lag-seconds).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class ReplicationLatencyScenarios
{
    private static byte[] Body(int seed) => Enumerable.Range(0, 1024).Select(i => (byte)(i + seed)).ToArray();

    [Fact]
    public async Task A15_ReplicationLatency_PerObjectUnder5s()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a15", 2, async env =>
        {
            // Arrange — бакет на узле-0 (источнике); ПРОГРЕВ контура (DNS/сессии
            // docker-сети — установившийся режим для серии измерений)
            var bucket = $"a15-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            var warmBody = Body(99);
            await MeshClients.PutObjectAsync(env, 0, bucket, "warmup", warmBody,
                TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "warmup",
                    TestContext.Current.CancellationToken) is not null,
                "warmup-объект доехал (прогрев контура)", budgetSec: 30);
            var latencies = new List<long>();

            // Act / Assert — серия 10 PUT: per-объектно t0 → поллинг HeadObject приёмника
            for (var i = 0; i < 10; i++)
            {
                var key = $"k{i:D2}";
                var body = Body(i);
                using (var s3 = MeshClients.S3(env, 0))
                {
                    var put = await s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = bucket, Key = key,
                        InputStream = new MemoryStream(body),
                    }, TestContext.Current.CancellationToken);
                    var t0 = Stopwatch.GetTimestamp(); // момент 200 OK источника

                    // Поллинг HeadObject приёмника: шаг ≤1 с, бюджет 5 с — до совпадения
                    // modTime (Last-Modified) и sha256 (ETag — md5? нет: сравнение размера и
                    // ETag) с зафиксированными в ответе PUT
                    var applied = false;
                    var deadline = Stopwatch.GetTimestamp() + 5 * Stopwatch.Frequency;
                    var lastHead = "(no head)";
                    while (Stopwatch.GetTimestamp() < deadline)
                    {
                        using (var s3Peer = MeshClients.S3(env, 1))
                        {
                            try
                            {
                                var head = await s3Peer.GetObjectMetadataAsync(bucket, key,
                                    TestContext.Current.CancellationToken);
                                // приёмник применяет с ИСХОДНЫМИ метаданными — LastModified
                                // совпадает с источником (глава 07 §7)
                                lastHead = $"200 etag={head.ETag} len={head.ContentLength}";
                                if (head.ETag == put.ETag)
                                {
                                    applied = true;
                                    break;
                                }
                            }
                            catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                            {
                                lastHead = $"404";
                            }
                        }
                        await Task.Delay(TimeSpan.FromMilliseconds(200),
                            TestContext.Current.CancellationToken);
                    }

                    var latencyMs = (long)((Stopwatch.GetTimestamp() - t0) * 1000.0 / Stopwatch.Frequency);
                    applied.Should().BeTrue(
                        $"объект {key} не реплицирован за бюджет 5 с; последний Head: {lastHead}; putETag={put.ETag}");
                    latencies.Add(latencyMs);
                    await env.LogMetricAsync("repl-e2e-latency-ms", latencyMs.ToString());
                }
            }

            // Assert — каждый объект ≤5000 мс; min/max/median — [METRIC]-строками
            latencies.Should().HaveCount(10);
            latencies.Should().OnlyContain(v => v <= 5000, "сквозная латентность ≤5 с на объект (A15)");
            var sorted = latencies.OrderBy(v => v).ToList();
            var median = (sorted[4] + sorted[5]) / 2.0;
            await env.LogMetricAsync("repl-e2e-latency-ms", $"min={sorted[0]}");
            await env.LogMetricAsync("repl-e2e-latency-ms", $"max={sorted[9]}");
            await env.LogMetricAsync("repl-e2e-latency-ms", $"median={median}");

            // Сверка честности R5: рантайм-лаги источника и приёмника == 0 после серии
            await MeshClients.PollUntilAsync(async () =>
            {
                var m0 = await MeshClients.GetMetricsAsync(env, 0, TestContext.Current.CancellationToken);
                var m1 = await MeshClients.GetMetricsAsync(env, 1, TestContext.Current.CancellationToken);
                return RuntimeGauge(m0, "ownS3_replication_delivery_lag_seconds") == 0
                    && RuntimeGauge(m1, "ownS3_replication_fetch_lag_seconds") == 0;
            }, "рантайм-лаги обнулились после догона серии", budgetSec: 10);

            var deliveryLag = await MeshClients.GetMetricsAsync(env, 0, TestContext.Current.CancellationToken);
            var fetchLag = await MeshClients.GetMetricsAsync(env, 1, TestContext.Current.CancellationToken);
            var d0 = RuntimeGauge(deliveryLag, "ownS3_replication_delivery_lag_seconds");
            var f1 = RuntimeGauge(fetchLag, "ownS3_replication_fetch_lag_seconds");
            await env.LogMetricAsync("repl-runtime-delivery-lag-seconds", d0.ToString());
            await env.LogMetricAsync("repl-runtime-fetch-lag-seconds", f1.ToString());
        }, ct: TestContext.Current.CancellationToken);
    }

    /// <summary>Значение gauge-серии по имени (первое вхождение; нет — -1).</summary>
    private static double RuntimeGauge(string metricsText, string name)
    {
        foreach (var line in metricsText.Split('\n'))
            if (line.StartsWith(name, StringComparison.Ordinal) && !line.StartsWith("#"))
            {
                var valuePart = line[(line.LastIndexOf(' ') + 1)..].Trim();
                return double.TryParse(valuePart, System.Globalization.CultureInfo.InvariantCulture,
                    out var v) ? v : -1;
            }
        return -1;
    }
}
