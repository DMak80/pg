using Microsoft.Extensions.Options;
using S3Worker.App.Api.Operations;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// POST /api/seed/demo: флаг S3Worker:Api:EnableSeedEndpoint=false → 404;
// on → создать demo-кластер (инстансы east1/east2, профиль backups-main,
// placement docker_host=local ×2, cpu 1/memGi 1/diskGi 10) через
// CreateClusterHandler-логику + put links {east1,east2} (демонстрация графа);
// живой demo → 200 идемпотентно.
public sealed class SeedDemoHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    CreateClusterHandler create,
    IOptionsMonitor<S3WorkerOptions> options)
{
    public async Task<Result<SeedDemoDto>> HandleAsync(CancellationToken ct)
    {
        if (!options.CurrentValue.Api.EnableSeedEndpoint)
            return Result<SeedDemoDto>.Failed(new WorkerApiNotFoundException(
                "seed endpoint is disabled (S3Worker:Api:EnableSeedEndpoint=false)"));

        // Живой demo → 200 идемпотентно.
        var existing = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, "demo", ct);
        if (!existing.IsSuccess)
            return Result<SeedDemoDto>.Failed(existing.Error!);
        if (existing.Value is not null)
            return Result<SeedDemoDto>.Success(new SeedDemoDto(false));

        var request = new CreateS3ClusterRequest(
            "demo", S3Limits.Platform, "backups-main",
            [
                new S3InstanceDto("east1", new S3PlacementDto("local", null, null), null),
                new S3InstanceDto("east2", new S3PlacementDto("local", null, null), null),
            ],
            new S3ResourcesDto(1m, 1, 10));
        var created = await create.HandleAsync(request, ct);
        if (!created.IsSuccess)
            return Result<SeedDemoDto>.Failed(created.Error!);

        // Граф демо-кластера: пара east1↔east2 (канонический JSON arch/23 §2.1).
        var links = await S3ApiHelpers.CallAsync<bool>(endpoints, async endpoint =>
        {
            var write = await gateway.PutAsync(endpoint, S3ApiHelpers.LinksKey("demo"),
                S3LinksValidator.CanonicalJson([new S3LinkDto("east1", "east2")]), null, ct);
            return write.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(write.Error!);
        });
        if (!links.IsSuccess)
            return Result<SeedDemoDto>.Failed(links.Error!);
        return Result<SeedDemoDto>.Success(new SeedDemoDto(true));
    }
}

public sealed record SeedDemoDto(bool Seeded);
