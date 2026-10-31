using PgWorker.PgNode;

// AAA: BootstrapRenderer — все 7 файлов tmpfs /run/pgw (spec §3.5), 0600,
// doorman-шаблон с подставленным паролем, идемпотентность повторного рендера.
public class BootstrapRendererTests
{
    private const string SpiloYaml = "---\nrestapi:\n  password: \"restpw\"\n";
    private const string CertPem = "-----BEGIN CERTIFICATE-----\nTESTCERT\n-----END CERTIFICATE-----\n";
    private const string KeyPem = "-----BEGIN PRIVATE KEY-----\nTESTKEY\n-----END PRIVATE KEY-----\n";
    private const string DoormanTemplate =
        "# pooler template\npassword: \"${PGW_SU_PASSWORD}\"\n" +
        "tls_certificate: \"/run/pgw/doorman/tls.crt\"\n" +
        "tls_private_key: \"/run/pgw/doorman/tls.key\"\n";

    private static NodeBootstrapData Bootstrap() => new(SpiloYaml, CertPem, KeyPem);

    [Fact]
    public void Render_CreatesAllSevenFilesWithPushedPasswords()
    {
        // Arrange: временный корень рендера (rootDir — тестовый seam tmpfs).
        var root = Path.Combine(Path.GetTempPath(), $"pgw-render-{Guid.NewGuid():N}");

        try
        {
            // Act: рендер всех файлов.
            BootstrapRenderer.Render(Bootstrap(), "su-secret", "standby-secret",
                DoormanTemplate, root);

            // Assert: все 7 файлов на месте; spilo-env — ровно 4 строки
            // KEY=VALUE с пуш-паролями (включая завершающую пустую строку).
            File.Exists($"{root}/spilo-env").Should().BeTrue();
            File.Exists($"{root}/spilo-configuration.yml").Should().BeTrue();
            File.Exists($"{root}/rest-cert.pem").Should().BeTrue();
            File.Exists($"{root}/rest-key.pem").Should().BeTrue();
            File.Exists($"{root}/doorman/pg_doorman.yaml").Should().BeTrue();
            File.Exists($"{root}/doorman/tls.crt").Should().BeTrue();
            File.Exists($"{root}/doorman/tls.key").Should().BeTrue();

            var spiloEnv = File.ReadAllText($"{root}/spilo-env");
            var lines = spiloEnv.Split('\n');
            lines.Should().HaveCount(5, "4 KEY=VALUE lines plus trailing empty line");
            lines[0].Should().Be("PGUSER_SUPERUSER=postgres");
            lines[1].Should().Be("PGPASSWORD_SUPERUSER=su-secret");
            lines[2].Should().Be("PGUSER_STANDBY=standby");
            lines[3].Should().Be("PGPASSWORD_STANDBY=standby-secret");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Render_DoormanConfig_CarriesRealPasswordInsteadOfPlaceholder()
    {
        // Arrange: временный корень рендера.
        var root = Path.Combine(Path.GetTempPath(), $"pgw-render-{Guid.NewGuid():N}");

        try
        {
            // Act: рендер.
            BootstrapRenderer.Render(Bootstrap(), "su-secret", "standby-secret",
                DoormanTemplate, root);

            // Assert: плейсхолдер заменён паролем; TLS-пути — tmpfs-рендера
            // (spec §3.5, шаблон DOORMAN_CONFIG_TEMPLATE).
            var config = File.ReadAllText($"{root}/doorman/pg_doorman.yaml");
            config.Should().Contain("password: \"su-secret\"");
            config.Should().NotContain(BootstrapRenderer.SuPasswordPlaceholder);
            config.Should().Contain("tls_certificate: \"/run/pgw/doorman/tls.crt\"");
            config.Should().Contain("tls_private_key: \"/run/pgw/doorman/tls.key\"");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Render_FilesAreOwnerReadWriteOnly()
    {
        // Arrange: временный корень рендера.
        var root = Path.Combine(Path.GetTempPath(), $"pgw-render-{Guid.NewGuid():N}");

        try
        {
            // Act: рендер.
            BootstrapRenderer.Render(Bootstrap(), "su", "sb", DoormanTemplate, root);

            // Assert: права 0600 (UserRead|UserWrite, без групп/остальных) на
            // всех секретных файлах — tmpfs-канон spec §3.5. Unix-режим —
            // платформенный API (guard CA1416): на Windows рендер пропускает
            // unix-права (см. BootstrapRenderer.WriteAll) — ассерт применим
            // только на unix (прод/тесты — Linux/macOS).
            if (OperatingSystem.IsWindows())
                return;
            foreach (var path in new[]
                     {
                         $"{root}/spilo-env", $"{root}/spilo-configuration.yml",
                         $"{root}/rest-cert.pem", $"{root}/rest-key.pem",
                         $"{root}/doorman/pg_doorman.yaml",
                         $"{root}/doorman/tls.crt", $"{root}/doorman/tls.key",
                     })
            {
                var mode = File.GetUnixFileMode(path);
                mode.Should().Be(UnixFileMode.UserRead | UnixFileMode.UserWrite,
                    $"{path} must be 0600");
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
