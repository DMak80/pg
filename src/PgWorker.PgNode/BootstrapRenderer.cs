using System.IO;

namespace PgWorker.PgNode;

/// <summary>Рендер tmpfs /run/pgw (arch/pgworker/04-security.md, spec §3.5): все файлы 0600,
/// процесс агента работает под postgres (владелец верен по построению).
/// Кэша на диске нет — tmpfs память контейнера; повторный рендер перезаписывает
/// файлы идемпотентно.</summary>
public static class BootstrapRenderer
{
    public const string Root = "/run/pgw";
    public const string SpiloEnvPath = Root + "/spilo-env";
    public const string SpiloConfigurationPath = Root + "/spilo-configuration.yml";
    public const string RestCertPath = Root + "/rest-cert.pem";
    public const string RestKeyPath = Root + "/rest-key.pem";
    public const string DoormanConfigPath = Root + "/doorman/pg_doorman.yaml";
    public const string DoormanCertPath = Root + "/doorman/tls.crt";
    public const string DoormanKeyPath = Root + "/doorman/tls.key";

    // Плейсхолдер шаблона doorman (spec §3.5/§3.8-11).
    public const string SuPasswordPlaceholder = "${PGW_SU_PASSWORD}";

    public static void Render(
        NodeBootstrapData bootstrap, string suPassword, string standbyPassword,
        string doormanTemplate, string? rootDir = null)
    {
        // MR-#23 fail-fast на агенте: пароль вне алфавита [A-Za-z0-9._-] —
        // ArgumentException (в combine с catch рендера → start_failed), не
        // молчаливая запись.
        if (!EnvironmentFile.IsAlphabetSafe(suPassword))
            throw new ArgumentException("suPassword is outside the DSN-safe alphabet");
        if (!EnvironmentFile.IsAlphabetSafe(standbyPassword))
            throw new ArgumentException("standbyPassword is outside the DSN-safe alphabet");
        var root = rootDir ?? Root;
        Directory.CreateDirectory(root + "/doorman");
        var mode = UnixFileMode.UserRead | UnixFileMode.UserWrite; // 0600
        WriteAll($"{root}/spilo-env", string.Join("\n",
            EnvironmentFile.GenerateEnvironmentVariable("PGUSER_SUPERUSER", "postgres"),
            EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_SUPERUSER", suPassword),
            EnvironmentFile.GenerateEnvironmentVariable("PGUSER_STANDBY", "standby"),
            EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_STANDBY", standbyPassword),
            string.Empty), mode);
        WriteAll($"{root}/spilo-configuration.yml", bootstrap.SpiloConfiguration, mode);
        WriteAll($"{root}/rest-cert.pem", bootstrap.CertPem, mode);
        WriteAll($"{root}/rest-key.pem", bootstrap.KeyPem, mode);
        WriteAll($"{root}/doorman/pg_doorman.yaml",
            doormanTemplate.Replace(SuPasswordPlaceholder, suPassword, StringComparison.Ordinal), mode);
        WriteAll($"{root}/doorman/tls.crt", bootstrap.CertPem, mode);
        WriteAll($"{root}/doorman/tls.key", bootstrap.KeyPem, mode);
    }

    private static void WriteAll(string path, string content, UnixFileMode mode)
    {
        // 0600 (платформенный guard CA1416, прецедент WalReceiverLoop): прод —
        // Linux-контейнер ноды; на Windows guard пропускает unix-режим (тесты
        // рендера на Windows без прав — канон агента не меняется).
        var options = OperatingSystem.IsWindows()
            ? new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None }
            : new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.None,
                UnixCreateMode = mode,
            };
        using (var stream = new FileStream(path, options))
        using (var writer = new StreamWriter(stream))
        {
            writer.Write(content);
        }

        // MR-#15: перерендер поверх ЧУЖОГО файла (созданного с иными правами)
        // обязан возвращать 0600 — FileMode.Create обрезает содержимое, но не
        // пересоздаёт права существующего файла.
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, mode);
    }
}
