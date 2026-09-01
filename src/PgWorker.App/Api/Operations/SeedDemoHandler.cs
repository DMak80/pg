using PgWorker.Core;
using PgWorker.Core.Seed;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Ответ 200 POST /api/seed/demo (arch/14 §1.1.1).
public sealed record SeedDemoDto(bool Seeded);

// Демо-сид pg-контура через API воркера (task etcd-via-worker-api): перенос
// dev-stand/adminpanel/seed.sh 1:1 (план — PostgresDemoSeedPlan). Идемпотентен
// по живому /clusters/demo/config (образец скрипта: существующий config =>
// состояние засеяно, НЕ перезаписываем). Пакет плоских PutAsync без txn —
// как скрипт; флаг EnableSeedEndpoint=false → псевдо-404 до любых чтений.
public sealed class SeedDemoHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider clock, bool enabled)
{
    public async Task<Result<SeedDemoDto>> HandleAsync(CancellationToken ct)
    {
        // 1) Стендовый эндпоинт выключен — 404 (до идемпотентности и записей).
        if (!enabled)
            return Result<SeedDemoDto>.Failed(
                new WorkerApiNotFoundException("seed-эндпоинт выключен (PgWorker:Api:EnableSeedEndpoint)"));

        // 2) Идемпотентность: сбой чтения → 503; config жив → no-op.
        var config = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.GetAsync(endpoint, "/clusters/demo/config", ct));
        if (!config.IsSuccess)
            return Result<SeedDemoDto>.Failed(config.Error!);
        if (config.Value is not null)
            return Result<SeedDemoDto>.Success(new SeedDemoDto(false));

        // 3) Пакет put по плану (без txn — образец скрипта). Сбой посередине —
        //    частичная наливка: config ставится первым, повтор увидит его и не
        //    тронет данные (та же семантика, что у seed.sh).
        var plan = new PostgresDemoSeedPlan(clock.GetUtcNow().ToUnixTimeSeconds());
        foreach (var put in plan.Puts)
        {
            var result = await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, put.Key, put.Value, null, ct));
            if (!result.IsSuccess)
                return Result<SeedDemoDto>.Failed(result.Error!);
        }

        return Result<SeedDemoDto>.Success(new SeedDemoDto(true));
    }
}
