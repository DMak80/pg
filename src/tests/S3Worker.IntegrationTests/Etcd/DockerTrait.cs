using Xunit;

namespace S3Worker.IntegrationTests.Etcd;

// Гейт docker-серий S3Worker (прецедент PgWorker/OwnS3 DockerTrait): без
// PGW_TEST_DOCKER=1 тесты пропускаются — CI без docker остаётся зелёным.
public static class DockerTrait
{
    public const string EnvVar = "PGW_TEST_DOCKER";

    public static void SkipIfUnavailable()
    {
        if (Environment.GetEnvironmentVariable(EnvVar) != "1")
            Assert.Skip($"docker-тесты выключены (установите {EnvVar}=1 для включения)");
    }
}
