using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Гейт docker-серии (решение фазы plan №3): без PGW_TEST_DOCKER=1 тесты
// пропускаются — CI без docker остаётся зелёным.
public static class DockerTrait
{
    public const string EnvVar = "PGW_TEST_DOCKER";

    public static void SkipIfUnavailable()
    {
        if (Environment.GetEnvironmentVariable(EnvVar) != "1")
            Assert.Skip($"docker-тесты выключены (установите {EnvVar}=1 для включения)");
    }
}
