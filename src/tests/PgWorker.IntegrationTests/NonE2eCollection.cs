using PgWorker.IntegrationTests.Api;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests;

/// <summary>Последовательная группа ВСЕХ не-E2E тестов сборки (t24, решение
/// гейта конфигурации Фазы 6: «не е2е должны быть последовательно, их в
/// коллекцию надо закинуть»). DisableParallelization выводит коллекцию из
/// параллельного пула xUnit — не-E2E тесты не конкурируют между собой
/// (эмпирика: WalReceiverIntegrationTests падал под конкуренцией в
/// беззащитной конфигурации). Фикстуры прежних коллекций (общий etcd,
/// API-хосты, метрики) подняты на эту коллекцию — та же семантика «один
/// контур на серию», какой обладали прежние коллекции. E2e-классы остаются
/// collection-per-class и параллельны до maxParallelThreads из
/// xunit.runner.json. ОДНА коллекция обязательна: разные коллекции с
/// DisableParallelization параллельны МЕЖДУ собой — несколько групп
/// гарантии последовательности не дают.
///
/// Инвариант непересечения ключей (унаследован от прежней EtcdCollection)
/// держат сами классы: имена кластеров ОБЯЗАНЫ нести per-class guid-тег
/// (канон docs/e2e-isolation.md §1), клэймящие тесты чистят
/// /pgworker/claims/&lt;C&gt; перед TryClaim и отпускают клэйм в teardown
/// (await using / IAsyncLifetime) — литеральная коллизия имён (инцидент t07:
/// sc3 BackupSupervisor × ShardScale) даёт флейк полной сборки из-за
/// lease-TTL 15с.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class NonE2eCollection :
    ICollectionFixture<EtcdFixture>,
    ICollectionFixture<PgApiFixture>,
    ICollectionFixture<PgMetricsFixture>
{
    public const string Name = "non-e2e-sequential";
}
