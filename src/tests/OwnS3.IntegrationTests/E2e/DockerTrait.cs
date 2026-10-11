using Xunit;

namespace OwnS3.IntegrationTests.E2e;

// Гейт docker-серии ownS3 (прецедент PgWorker DockerTrait): без
// PGW_TEST_DOCKER=1 тесты пропускаются — CI без docker остаётся зелёным
// (единая env docker-серий монорепо).
public static class DockerTrait
{
    public const string EnvVar = "PGW_TEST_DOCKER";

    public static void SkipIfUnavailable()
    {
        if (Environment.GetEnvironmentVariable(EnvVar) != "1")
            Assert.Skip($"docker-тесты выключены (установите {EnvVar}=1 для включения)");
    }
}
