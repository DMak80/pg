using System.Globalization;
using OwnS3.Protocol.PeerAuth;

namespace OwnS3.Pki;

// CLI-утилита мини-PKI ownS3 (глава 07 §2, spec §4.6): выпуск CA и
// сертификатов серверов. Криптика — целиком в OwnS3.Protocol
// (PeerCertificateAuthority); здесь только разбор аргументов, файлы, печать.
// Сервер ownS3 ключи не генерирует — материал поставляется владельцем
// инсталляции через эти команды и env-строки выдачи (spec §4.6).
internal static class Program
{
    private static int Main(string[] args)
    {
        try
        {
            if (args is ["ca", "create", .. var caArgs])
                return RunCaCreate(ParseOptions(caArgs));
            if (args is ["server", "issue", .. var serverArgs])
                return RunServerIssue(ParseOptions(serverArgs));
            PrintUsage();
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ошибка: {ex.Message}");
            return 1;
        }
    }

    // Разбор пар "--ключ значение"; голый ключ или посторонний токен — ошибка использования.
    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        if (args.Length % 2 != 0)
            throw new ArgumentException(
                $"аргумент {args[^1]} без значения (ожидаются пары --ключ значение)");
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i += 2)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException(
                    $"неожиданный аргумент {args[i]} (ожидаются пары --ключ значение)");
            options[args[i]] = args[i + 1];
        }

        return options;
    }

    private static string Require(IReadOnlyDictionary<string, string> options, string key) =>
        options.TryGetValue(key, out var value)
            ? value
            : throw new ArgumentException($"обязателен аргумент {key}");

    // Имя становится частью имён файлов и env-строк — разделители пути запрещены.
    private static string RequireName(IReadOnlyDictionary<string, string> options)
    {
        var name = Require(options, "--name");
        if (name.Length == 0 || name == "." || name == ".."
            || name.Contains('/') || name.Contains('\\')
            || name.Contains(Path.DirectorySeparatorChar) || name.Contains(Path.AltDirectorySeparatorChar))
            throw new ArgumentException($"--name: недопустимое имя «{name}»");
        return name;
    }

    // ca create: пара ключей CA → ca.json (trust-anchor) + ca.private.pem (PKCS#8).
    private static int RunCaCreate(IReadOnlyDictionary<string, string> options)
    {
        var name = RequireName(options);
        var outDir = Require(options, "--out");
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa(name);
        Directory.CreateDirectory(outDir);
        var caJsonPath = Path.GetFullPath(Path.Combine(outDir, "ca.json"));
        var caKeyPath = Path.GetFullPath(Path.Combine(outDir, "ca.private.pem"));
        File.WriteAllText(caJsonPath, caJson);
        File.WriteAllText(caKeyPath, caPrivatePem);

        Console.WriteLine($"# CA «{name}» создан: {caJsonPath} + {caKeyPath}");
        Console.WriteLine("# Дай trust-anchor каждому серверу инсталляции (join требует его на обеих сторонах):");
        Console.WriteLine($"OWNS3_REPLICATION_TRUST_CA_PATH={caJsonPath}");
        return 0;
    }

    // server issue: выпуск сертификата сервера под CA + env-строки развёртывания.
    private static int RunServerIssue(IReadOnlyDictionary<string, string> options)
    {
        var name = RequireName(options);
        var outDir = Require(options, "--out");
        var caJsonPath = Require(options, "--ca");
        var caKeyPath = Require(options, "--ca-key");
        var expiresDays = PeerCertificateAuthority.DefaultExpiresDays;
        if (options.TryGetValue("--expires-days", out var raw))
        {
            if (!int.TryParse(raw, CultureInfo.InvariantCulture, out expiresDays))
                throw new ArgumentException($"--expires-days: не целое число «{raw}»");
        }

        // Чтение CA: FromJson — fail-fast парсинг (битый ca.json → исключение → код 1).
        var ca = PeerTrustAnchor.FromJson(File.ReadAllText(caJsonPath));
        var caPrivatePem = File.ReadAllText(caKeyPath);
        var (cert, privatePem) = PeerCertificateAuthority.IssueServer(
            name, ca, caPrivatePem, DateTimeOffset.UtcNow, expiresDays);

        Directory.CreateDirectory(outDir);
        var certPath = Path.GetFullPath(Path.Combine(outDir, $"{name}.cert.json"));
        var keyPath = Path.GetFullPath(Path.Combine(outDir, $"{name}.private.pem"));
        File.WriteAllText(certPath, cert.ToJson());
        File.WriteAllText(keyPath, privatePem);

        Console.WriteLine(
            $"# Сертификат сервера «{name}» выпущен (CA «{ca.Name}»): {certPath} + {keyPath}");
        Console.WriteLine("# Env для контейнера сервера:");
        Console.WriteLine($"OWNS3_REPLICATION_CERT_PATH={certPath}");
        Console.WriteLine($"OWNS3_REPLICATION_PRIVATE_KEY_PATH={keyPath}");
        Console.WriteLine($"OWNS3_REPLICATION_ADVERTISED_ENDPOINT=http://{name}:9000");
        return 0;
    }

    private static void PrintUsage()
    {
        Console.Error.WriteLine("""
            Использование:
              OwnS3.Pki ca create --name <ca> --out <dir>
                  → <dir>/ca.json + <dir>/ca.private.pem + env-строка trust-anchor.
              OwnS3.Pki server issue --name <n> --ca <ca.json> --ca-key <ca.private.pem>
                                     [--expires-days N] --out <dir>
                  → <dir>/<n>.cert.json + <dir>/<n>.private.pem + env-строки для
                    контейнера сервера (expires-days ≤ 0 — бессрочный сертификат).
            """);
    }
}
