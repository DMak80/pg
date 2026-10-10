using Xunit;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>Последовательная коллекция ВСЕХ E2E-классов ownS3 (spec §4.7):
/// DisableParallelization выводит её из параллельного пула xUnit. In-memory
/// Api-сценарии AWS SDK не используют — параллельность коллекций не
/// конфликтует.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OwnS3E2eCollection
{
    public const string Name = "owns3-e2e";
}
