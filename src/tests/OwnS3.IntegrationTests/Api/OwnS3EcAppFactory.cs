using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace OwnS3.IntegrationTests.Api;

// Фабрика EC-режима (t40): override in-memory конфигурации базовой фабрики —
// 4 temp-подкаталога, setSize 4, parity 1 (канон 05 §4/06 §1). Root-пара и
// статические ключи наследуются от базовой (те же креды сценариев).
// Не sealed: наследник деградационной серии (OwnS3EcDegrAppFactory) — свой том.
public class OwnS3EcAppFactory : OwnS3AppFactory
{
    public IReadOnlyList<string> DataDirs { get; } = Enumerable.Range(1, 4)
        .Select(i => Path.Combine(Path.GetTempPath(), "owns3-waf-ec-" + Guid.NewGuid().ToString("N") + $"-d{i}"))
        .ToList();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:DataDir"] = string.Join(',', DataDirs),
            ["OwnS3:Ec:SetSize"] = "4",
            ["OwnS3:Ec:Parity"] = "1",
        }));
    }

    // Teardown: все 4 temp-корня удаляются при любом исходе
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            foreach (var dir in DataDirs)
            {
                try
                {
                    if (Directory.Exists(dir))
                        Directory.Delete(dir, recursive: true);
                }
                catch (IOException)
                {
                    // хост мог ещё держать файлы — корни в temp, ОС уберёт
                }
            }
        }
    }
}

// Коллекция EC-режима: один EC-хост на сценарные классы контракта (Task 15).
[CollectionDefinition(Name)]
public sealed class OwnS3EcTestCollection : ICollectionFixture<OwnS3EcAppFactory>
{
    public const string Name = "OwnS3Ec";
}
