using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 200 POST /api/seed/demo (порт SeedDemoHandler kfw; идемпотентен).
public sealed record SeedDemoDto(bool Seeded);

// Демо-сид valkey-домена через API воркера: идемпотентен по живому
// /valkey/clusters/demo/config (существующий config => состояние засеяно, НЕ
// перезаписываем). Пакет плоских PutAsync без txn; config ставится первым —
// сбой посередине повтор увидит и не тронет данные. Флаг
// EnableSeedEndpoint=false → псевдо-404 до любых чтений.
// Seed-набор (spec §4.7): nodes=1, maxmemory_bytes=536870912, allkeys-lru,
// resources {"cpu":"1","mem":"1Gi","disk":"10Gi"} (512MiB < 1Gi — инвариант).
public sealed class SeedDemoHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider clock, bool enabled)
{
    public async Task<Result<SeedDemoDto>> HandleAsync(CancellationToken ct)
    {
        // 1) Стендовый эндпоинт выключен — 404 (до идемпотентности и записей).
        if (!enabled)
            return Result<SeedDemoDto>.Failed(
                new WorkerApiNotFoundException("seed-эндпоинт выключен (ValkeyWorker:Api:EnableSeedEndpoint)"));

        // 2) Идемпотентность: сбой чтения → 503; config жив → {"seeded":false}.
        var config = await ValkeyEtcdFailover.CallAsync(endpoints,
            endpoint => gateway.GetAsync(endpoint, "/valkey/clusters/demo/config", ct));
        if (!config.IsSuccess)
            return Result<SeedDemoDto>.Failed(config.Error!);
        if (config.Value is not null)
            return Result<SeedDemoDto>.Success(new SeedDemoDto(false));

        // 3) Пакет put по сид-набору (без txn — образец скрипта).
        var createdUnix = clock.GetUtcNow().ToUnixTimeSeconds();
        var configJson = new ValkeyConfigJson(
            1, 536870912, "allkeys-lru", createdUnix, "NOT_INITIALIZED").Serialize();
        var puts = new (string Key, string Value)[]
        {
            ("/valkey/clusters/demo/config", configJson),
            ("/valkey/clusters/demo/nodes/node1/state", "NOT_INITIALIZED"),
            ("/valkey/clusters/demo/nodes/node1/resources",
                JsonSerializer.Serialize(new ValkeyResourcesJson("1", "1Gi", "10Gi"))),
        };
        foreach (var (key, value) in puts)
        {
            var result = await ValkeyEtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, key, value, null, ct));
            if (!result.IsSuccess)
                return Result<SeedDemoDto>.Failed(result.Error!);
        }

        return Result<SeedDemoDto>.Success(new SeedDemoDto(true));
    }
}
