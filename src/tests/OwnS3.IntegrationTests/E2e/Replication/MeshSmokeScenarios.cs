using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// Smoke mesh-репликации (t35, план Task 16): контур 2 узла (полный mesh через
/// env-seed); PUT в узел-0 → поллинг (шаг ≤5 с в первых циклах, бюджет 5 мин):
/// GET с узла-1 отдаёт объект (доставка уведомление→фетч); teardown-ассерты
/// чистоты — зелёные. Коллекция owns3-mesh-e2e — DisableParallelization.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OwnS3MeshE2eCollection
{
    public const string Name = "owns3-mesh-e2e";
}

[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class MeshSmokeScenarios
{
    [Fact]
    public async Task Put_Node0_ConvergesToNode1()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("mesh-smoke", 2, async env =>
        {
            // Arrange — бакет создан на узле-0 (admin); объект записан writer'ом
            var bucket = $"mesh-smoke-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            var body = "mesh-smoke-body"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k", body, TestContext.Current.CancellationToken);

            // Act / Assert — сходимость: узел-1 отдаёт объект (поллинг ≤5 мин, шаг ≤5 с)
            await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k", TestContext.Current.CancellationToken)
                    is { } got && got.SequenceEqual(body),
                "PUT узла-0 доезжает до узла-1");
        }, ct: TestContext.Current.CancellationToken);
    }
}
