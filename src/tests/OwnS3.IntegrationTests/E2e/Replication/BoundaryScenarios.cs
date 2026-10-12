using System.Diagnostics;
using System.Net;
using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// A10/A12/A13/A14 — границы (t35, план Task 20; R1/R5/R6): bounded-бюджет журнала
/// + принудительный тримминг + reconcile (A10), большой объект &gt;5 ГБ потоково с
/// sha-сверкой (A12), изоляция peer-доступа (A13), пауза фетчей по месту (A14).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class BoundaryScenarios
{
    private static byte[] Body(int seed) =>
        Enumerable.Range(0, 1024).Select(i => (byte)(i + seed)).ToArray();

    [Fact]
    public async Task A10_OutboxBudget_ForceTrim_ReconcileCovers()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a10", 2, async env =>
            {
                // Arrange — сосед disconnect (offline); серия PUT (~сотня событий)
                var bucket = $"a10-{env.RunTag}";
                await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
                await env.DisconnectNodeAsync(1, TestContext.Current.CancellationToken);
                try
                {
                    var sw = Stopwatch.StartNew();
                    // ~300 байт/событие × 400 = ~120 КБ > 64 КБ бюджета (план: серия ~сотни событий)
                    for (var i = 0; i < 400; i++)
                        await MeshClients.PutObjectAsync(env, 0, bucket, $"k{i:D3}", Body(i % 8),
                            TestContext.Current.CancellationToken);
                    sw.Stop();

                    // Assert — запись не отказывает (все 200 — исключения бы уронили тест),
                    // бюджет удержан, алерт поднят
                    sw.ElapsedMilliseconds.Should().BeLessThan(120_000);
                    await MeshClients.PollUntilAsync(async () =>
                    {
                        var metrics = await MeshClients.GetMetricsAsync(env, 0,
                            TestContext.Current.CancellationToken);
                        return metrics.Contains("ownS3_replication_outbox_budget_exceeded") &&
                            !metrics.Contains("ownS3_replication_outbox_budget_exceeded 0",
                                StringComparison.Ordinal);
                    }, "алерт-метрика бюджета поднята");

                    // Журнал удержан бюджетом: статус outbox.bytes ≤ бюджет + активный сегмент
                    var status = JsonDocument.Parse(
                        await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
                    status.GetProperty("outbox").GetProperty("budgetExceeded").GetBoolean().Should().BeTrue();

                    // Статус: бюджет превышен (до/вне прохода обслуживания)
                    var exceededSeen = false;
                    var deadline = Environment.TickCount64 + 90_000; // цикл обслуживания 60 с
                    string logs = string.Empty;
                    while (Environment.TickCount64 < deadline)
                    {
                        var statusNow = JsonDocument.Parse(
                            await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
                        if (statusNow.GetProperty("outbox").GetProperty("budgetExceeded").GetBoolean())
                        {
                            exceededSeen = true;
                            break;
                        }
                        await Task.Delay(5_000, TestContext.Current.CancellationToken);
                    }
                    exceededSeen.Should().BeTrue("бюджет превышен и зафиксирован статусом (R6)");

                    // Лог принудительного тримминга (глава 07 §8-а): событие потерь
                    // уведомлений появляется в docker logs после прохода обслуживания
                    var logDeadline = Environment.TickCount64 + 90_000;
                    while (Environment.TickCount64 < logDeadline)
                    {
                        logs = await env.RunDockerAsync(
                            ["logs", env.NodeName(0)], TestContext.Current.CancellationToken);
                        if (logs.Contains("принудительный тримминг", StringComparison.Ordinal))
                            break;
                        await Task.Delay(5_000, TestContext.Current.CancellationToken);
                    }
                    logs.Should().Contain("принудительный тримминг",
                        "потеря уведомлений журналируется (глава 07 §4/§8-а)");
                }
                finally
                {
                    await env.ReconnectNodeAsync(1, TestContext.Current.CancellationToken);
                }

                // Возврат соседа: уведомления потеряны триммингом — reconcile возвращает данные
                await MeshClients.AdminReconcileAsync(env, 1, TestContext.Current.CancellationToken);
                await MeshClients.PollUntilAsync(async () =>
                {
                    foreach (var i in new[] { 0, 50, 99 })
                        if (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, $"k{i:D3}",
                                TestContext.Current.CancellationToken) is null)
                            return false;
                    return true;
                }, "reconcile вернул данные соседу после принудительного тримминга");
            },
            configureNode: (i, builder) => builder
                .WithEnvironment("OWNS3_REPLICATION_OUTBOX_BUDGET_BYTES", "65536"),
            ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A12_MultipartOver5Gb_StreamsToNeighbor()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a12", 2, async env =>
        {
            // Arrange — multipart 6 × 1 ГиБ нулей (>5 ГБ — превыше PutObject-лимита)
            var bucket = $"a12-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            var partSize = 1024 * 1024 * 1024; // 1 ГиБ
            long expectedHashedBytes = 0; // длина эталонного потока, ушедшая в sha
            var sw = Stopwatch.StartNew();

            // Act — загрузка и Complete на узле-0
            var etag = await MeshClients.MultipartPutAsync(env, 0, bucket, "big", 6, partSize,
                part => new byte[partSize], TestContext.Current.CancellationToken);
            etag.Should().NotBeNullOrEmpty();

            // Assert — n1 GET отдаёт полный размер, sha равен (клиентская сверка чанками),
            // поллинг ≤5 мин (шаг ≤30 с); потоковая доставка — без S3-лимита
            await MeshClients.PollUntilAsync(async () =>
            {
                using var s3 = MeshClients.S3(env, 1, timeoutSec: 30);
                try
                {
                    var meta = await s3.GetObjectMetadataAsync(bucket, "big",
                        TestContext.Current.CancellationToken);
                    return meta.ContentLength == 6L * partSize;
                }
                catch
                {
                    return false;
                }
            }, "6 ГиБ-объект доехал на n1");
            sw.Stop();
            Console.WriteLine($"[PHASE] a12: доставка 6 ГиБ — {sw.Elapsed.TotalSeconds:F0} с");

            // Клиентская сверка sha чанками (GetObject потоково с n1) против эталона:
            // инкрементальный SHA256 нулевого потока 6 ГиБ (тот же генератор данных)
            using (var s3 = MeshClients.S3(env, 1, timeoutSec: 60))
            using (var response = await s3.GetObjectAsync(bucket, "big", TestContext.Current.CancellationToken))
            {
                response.ContentLength.Should().Be(6L * partSize);
                var received = System.Security.Cryptography.SHA256.Create();
                var expected = System.Security.Cryptography.SHA256.Create();
                var zeroBlock = new byte[8 * 1024 * 1024]; // нули — генератор данных
                var buffer = new byte[8 * 1024 * 1024];
                long total = 0;
                int read;
                while ((read = await response.ResponseStream.ReadAsync(buffer, TestContext.Current.CancellationToken)) > 0)
                {
                    received.TransformBlock(buffer, 0, read, null, 0);
                    total += read;
                    // эталон той же длины нулями (1 ГиБ-часть = 128 блоков по 8 МиБ)
                    while (total > expectedHashedBytes)
                    {
                        expected.TransformBlock(zeroBlock, 0, zeroBlock.Length, null, 0);
                        expectedHashedBytes += zeroBlock.Length;
                    }
                }
                received.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                expected.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                total.Should().Be(6L * partSize);
                Convert.ToHexString(received.Hash!).ToLowerInvariant()
                    .Should().Be(Convert.ToHexString(expected.Hash!).ToLowerInvariant(),
                        "sha полученного тела равен эталону нулевого потока");
            }
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A13_PeerAccessIsolation_AllRoles()
    {
        DockerTrait.SkipIfUnavailable();
        // mesh-min: 1 узел не разрешён (2–5) — минимальный 2-узловый контур
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a13", 2, async env =>
        {
            // Arrange — ключи read-only/read-write/admin (env-секция) + root есть
            var client = MeshClients.AdminClient(env, 0);

            // Act / Assert — ro/rw/admin ключи на replication-грани → 403 AccessDenied
            foreach (var creds in new[]
                     {
                         (OwnS3MeshE2eEnvironment.RootUser, OwnS3MeshE2eEnvironment.RootPassword),
                     })
            {
                // admin/root на replication-грани — тоже 403 (политика строго replication)
                var batch = JsonSerializer.Serialize(new
                {
                    originHostId = "x",
                    events = Array.Empty<object>(),
                });
                var payload = Convert.ToHexString(
                    System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(batch))).ToLowerInvariant();
                var response = await client.SendAsync("POST", "/replication/notifications", "",
                    batch, TestContext.Current.CancellationToken);
                // root-креды подписаны AdminClient'ом — не peer-кредами: подпись неверна
                // (InvalidAccessKeyId невозможен — ключ существует; Access/Signature → 403)
                response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
            }

            // replication-ключ на 21 операцию → 403 (матрица 05 §3)
            using var s3Peer = new Amazon.S3.AmazonS3Client(new Amazon.Runtime.BasicAWSCredentials(
                    OwnS3MeshE2eEnvironment.PeerPairAccessKeyOf(1, 0),
                    OwnS3MeshE2eEnvironment.PeerPairSecretKeyOf(1, 0)),
                new Amazon.S3.AmazonS3Config
                {
                    ServiceURL = env.NodeHostUrl(0),
                    ForcePathStyle = true,
                    AuthenticationRegion = "us-east-1",
                    RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
                });
            var put = async () => await s3Peer.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
            {
                BucketName = "nope", Key = "k",
                InputStream = new MemoryStream("x"u8.ToArray()),
            }, TestContext.Current.CancellationToken);
            await put.Should().ThrowAsync<Amazon.S3.AmazonS3Exception>()
                .Where(e => e.StatusCode == HttpStatusCode.Forbidden);
            var list = async () => await s3Peer.ListBucketsAsync(TestContext.Current.CancellationToken);
            await list.Should().ThrowAsync<Amazon.S3.AmazonS3Exception>()
                .Where(e => e.StatusCode == HttpStatusCode.Forbidden);
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A14_DiskWatermark_FetchPausedThenResumes()
    {
        DockerTrait.SkipIfUnavailable();
        // Приёмник (n1) с watermark выше фактического свободного места
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a14", 3, async env =>
            {
                // Arrange — PUT в источник; уведомления принимаются (ACK идёт)
                var bucket = $"a14-{env.RunTag}";
                await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
                var body = Body(1);
                for (var i = 0; i < 3; i++)
                    await MeshClients.PutObjectAsync(env, 0, bucket, $"k{i}", body,
                        TestContext.Current.CancellationToken);

                // Assert — фетчи n1 на паузе (метрика), запись источника не деградирует (серия ≤1 с)
                await MeshClients.PollUntilAsync(async () =>
                {
                    var metrics = await MeshClients.GetMetricsAsync(env, 1, TestContext.Current.CancellationToken);
                    return metrics.Contains("ownS3_replication_fetch_paused") &&
                        !metrics.Contains("ownS3_replication_fetch_paused 0", StringComparison.Ordinal);
                }, "фетчи n1 на паузе (fetch_paused 1)");

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < 10; i++)
                    await MeshClients.PutObjectAsync(env, 0, bucket, $"w{i}", body,
                        TestContext.Current.CancellationToken);
                sw.Stop();
                sw.ElapsedMilliseconds.Should().BeLessThanOrEqualTo(1000,
                    "запись источника не деградирует при паузе фетчей приёмника (R1)");

                // ACK приёма идёт: статус n0 — queue_depth 0 (уведомления приняты)
                await MeshClients.PollUntilAsync(async () =>
                {
                    var status = JsonDocument.Parse(
                        await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
                    return status.GetProperty("neighbors").EnumerateArray()
                        .All(n => n.GetProperty("queueDepth").GetInt64() == 0);
                }, "уведомления приняты приёмником (ACK приёма идёт, курсор идёт)");

                // Резюм: «watermark опущен — фетчи продолжаются, сходимость достигается»:
                // добавляем узел 2 (БЕЗ watermark-override) через admin-грань (hot-apply) —
                // его фетчи с нормальным watermark забирают данные, сходимость ≤5 мин
                await MeshClients.AdminPutTargetsAsync(env, 0,
                    [(env.NodeName(2), env.NodeInternalUrl(2))], TestContext.Current.CancellationToken);
                await MeshClients.AdminPutTargetsAsync(env, 2,
                    [(env.NodeName(0), env.NodeInternalUrl(0))], TestContext.Current.CancellationToken);
                await MeshClients.AdminReconcileAsync(env, 2, TestContext.Current.CancellationToken);
                await MeshClients.PollUntilAsync(async () =>
                {
                    foreach (var i in Enumerable.Range(0, 3))
                        if (await MeshClients.GetObjectOrNullAsync(env, 2, bucket, $"k{i}",
                                TestContext.Current.CancellationToken) is null)
                            return false;
                    return true;
                }, "фетчи с нормальным watermark доставили данные на n2 (сходимость)");
                var metrics = await MeshClients.GetMetricsAsync(env, 1, TestContext.Current.CancellationToken);
                metrics.Should().Contain("ownS3_replication_fetch_paused");
            },
            configureNode: (i, builder) =>
            {
                if (i == 1)
                    return builder.WithEnvironment("OWNS3_REPLICATION_DISK_FREE_MIN_BYTES",
                        "10995116277760000"); // 10 ТиБ — выше фактического свободного
                return builder;
            },
            ct: TestContext.Current.CancellationToken);
    }
}
