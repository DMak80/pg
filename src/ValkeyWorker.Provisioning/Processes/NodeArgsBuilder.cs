namespace ValkeyWorker.Provisioning.Processes;

// Сборка аргументов контейнера ноды (arch/21 §2): ACL при старте + maxmemory +
// persistence off. Детерминизм: аргументы ТОЛЬКО из etcd-факта (декларация +
// креды) → пересоздание контейнера собирает актуальные пароли (spec §4.5.1).
// BuildCmd — cmd-обёртка env-TLS (arch/21 §2): раскатка PEM из env в /tls и
// exec valkey-server с каноническим набором args (shell-экранированным).
public static class NodeArgsBuilder
{
    public static IReadOnlyList<string> Build(
        long maxmemoryBytes, string maxmemoryPolicy, string adminPassword, string appPassword)
        =>
        [
            "valkey-server",
            "--user", "default", "off",
            "--user", "admin", "on", $">{adminPassword}", "~*", "+@all",
            "--user", "app", "on", $">{appPassword}", "~*", "+@read", "+@write",
            "--maxmemory", maxmemoryBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--maxmemory-policy", maxmemoryPolicy,
            "--save", "",
            "--appendonly", "no",
            // TLS (t06, arch/21 §2): тот же клиентский порт portalloc (контейнерный
            // 6379 слушает TLS), plain закрыт; серты — /tls (env-TLS: PEM в env
            // VALKEY_TLS_{CERT,KEY,CA}, раскатка обёрткой BuildCmd); клиенты без
            // сертификатов — принципалы из ACL.
            "--tls-port", "6379",
            "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no",
        ];

    /// <summary>
    /// Cmd-обёртка старта ноды (env-TLS, arch/21 §2): ["sh","-c", раскатка +
    /// "; exec " + shell-escape(args)]. Раскатка — umask 077 + /tls + printf
    /// PEM из env; entrypoint образа при $1 == sh делает exec "$@" как есть —
    /// обёртка срабатывает ДО valkey-server (Р7-верификация на пине образа).
    /// Детерминизм от args — основа Cmd-сверки идемпотентности (свежий серт в
    /// env случаен и побайтовой сверки не имеет).
    /// </summary>
    public static IReadOnlyList<string> BuildCmd(IReadOnlyList<string> args)
        =>
        [
            "sh",
            "-c",
            "umask 077; mkdir -p /tls; "
            + "printf %s \"$VALKEY_TLS_CERT\" > /tls/node.crt; "
            + "printf %s \"$VALKEY_TLS_KEY\" > /tls/node.key; "
            + "printf %s \"$VALKEY_TLS_CA\" > /tls/ca.pem; "
            + "exec " + string.Join(" ", args.Select(ShellEscape)),
        ];

    // Экранирование литерала POSIX-shell: каждый arg → '…' (внутренние ' → '\'');
    // пустой arg → ''. ACL-токен ">пароль" обязан попасть valkey-server как
    // литерал — без кавычек shell трактует '>' как редирект (риск Р8).
    private static string ShellEscape(string arg)
        => "'" + arg.Replace("'", "'\\''", StringComparison.Ordinal) + "'";
}
