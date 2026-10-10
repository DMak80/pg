using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace OwnS3.IntegrationTests.Api;

// In-memory хост ownS3 (решение пользователя 3): без докера/портов; конфигурация
// — in-memory секция OwnS3 с root-парой и тремя статическими ключами (роли
// read-only/read-write/admin). Тестовые креды фиксированы литералами.
public sealed class OwnS3AppFactory : WebApplicationFactory<Program>
{
    public const string RootAccessKey = "testroot";
    public const string RootSecretKey = "testrootsecret";
    public const string ReaderAccessKey = "testreader";
    public const string ReaderSecretKey = "testreadersecret";
    public const string WriterAccessKey = "testwriter";
    public const string WriterSecretKey = "testwritersecret";
    public const string AdminAccessKey = "testadmin";
    public const string AdminSecretKey = "testadminsecret";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Root:User"] = "root",
            ["OwnS3:Root:Password"] = "rootpassword",
            ["OwnS3:HostId"] = "owns3-test",
            ["OwnS3:AccessKeys:0:AccessKey"] = ReaderAccessKey,
            ["OwnS3:AccessKeys:0:SecretKey"] = ReaderSecretKey,
            ["OwnS3:AccessKeys:0:Policy"] = "read-only",
            ["OwnS3:AccessKeys:1:AccessKey"] = WriterAccessKey,
            ["OwnS3:AccessKeys:1:SecretKey"] = WriterSecretKey,
            ["OwnS3:AccessKeys:1:Policy"] = "read-write",
            ["OwnS3:AccessKeys:2:AccessKey"] = AdminAccessKey,
            ["OwnS3:AccessKeys:2:SecretKey"] = AdminSecretKey,
            ["OwnS3:AccessKeys:2:Policy"] = "admin",
        }));
    }
}
