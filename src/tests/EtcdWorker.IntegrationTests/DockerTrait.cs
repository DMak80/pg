using Xunit;

namespace EtcdWorker.IntegrationTests;

// Гейт docker-серий EtcdWorker (прецедент PgWorker/S3Worker DockerTrait): без
// PGW_TEST_DOCKER=1 тесты пропускаются — CI без docker остаётся зелёным.
public static class DockerTrait
{
    public const string EnvVar = "PGW_TEST_DOCKER";

    public static void SkipIfUnavailable()
    {
        if (Environment.GetEnvironmentVariable(EnvVar) != "1")
            Assert.Skip($"docker tests are disabled (set {EnvVar}=1 to enable)");
    }
}
