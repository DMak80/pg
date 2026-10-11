using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace OwnS3.IntegrationTests.Api;

// Fail-fast root-пары (глава 05 §1): невалидная конфигурация — отказ старта
// хоста (OptionsValidationException); валидная — стартует (контрольный кейс).
public sealed class FailFastScenarios
{
    // Чистая фабрика с единственной in-memory парой root (без статических ключей).
    private sealed class RootPairFactory(string user, string password) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["OwnS3:Root:User"] = user,
                ["OwnS3:Root:Password"] = password,
            }));
        }
    }

    [Theory]
    [InlineData("ab", "rootpassword")]   // user короче 3
    [InlineData("root", "short")]        // password короче 8
    [InlineData("", "rootpassword")]     // пустой user
    [InlineData("root", "")]             // пустой password
    public void InvalidRootPair_FailsFastWithOptionsValidationException(string user, string password)
    {
        // Arrange
        using var factory = new RootPairFactory(user, password);

        // Act: построение хоста (CreateClient стартует сервер)
        var act = () => factory.CreateClient();

        // Assert: старт падает — сервис не поднимается со слабым секретом
        act.Should().Throw<OptionsValidationException>();
    }

    [Fact]
    public void ValidRootPair_StartsSuccessfully()
    {
        // Arrange: базовая фабрика с валидной root-парой
        using var factory = new OwnS3AppFactory();

        // Act
        using var client = factory.CreateClient();

        // Assert: контрольный кейс — хост стартует
        client.Should().NotBeNull();
    }

    // TLS не задан — хост стартует как раньше (AC1: поведение по умолчанию не меняется).
    [Fact]
    public void TlsEmpty_StartsHttp()
    {
        using var factory = new TlsFactory();
        using var client = factory.CreateClient();
        client.Should().NotBeNull();
    }

    private sealed class TlsFactory : WebApplicationFactory<Program>
    {
        // Томат данных (образец OwnS3AppFactory.TempVolumeDir): дефолтный /data
        // в testhost недоступен/невалиден — без подмены старт упал бы в fail-fast
        // режима/тома (Environment.Exit) ещё до этого кейса.
        private readonly string _volumeDir =
            Path.Combine(Path.GetTempPath(), "owns3-tls-waf-" + Guid.NewGuid().ToString("N"));

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["OwnS3:DataDir"] = _volumeDir,
                ["OwnS3:Root:User"] = "root",
                ["OwnS3:Root:Password"] = "rootpassword",
            }));
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                try
                {
                    if (Directory.Exists(_volumeDir))
                        Directory.Delete(_volumeDir, recursive: true);
                }
                catch (IOException)
                {
                    // хост мог ещё держать файлы — том в temp, ОС уберёт
                }
            }
        }
    }
}
