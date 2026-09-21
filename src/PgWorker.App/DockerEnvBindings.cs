using Microsoft.Extensions.Configuration;

namespace PgWorker.App;

// env-секреты docker-транспорта PgWorker (t07: перенесены из опций-моделей
// Shared.Docker — env-имена PGW_DOCKER_* — pg-специфика, модели в общей сборке
// их не знают). PEM-дуализм env-секретов — значение или _PATH-файл; перенос
// env → конфиг-дерево — до всего остального (Program.cs).
internal static class DockerEnvBindings
{
    // env-секреты → конфиг-дерево: PEM-значения и _PATH-файлы (паттерн WorkerTlsHandler.EnvBindings).
    private static readonly (string Env, string Key)[] TlsBindings =
    [
        ("PGW_DOCKER_TLS_CA", "PgWorker:Docker:Tls:CaPem"),
        ("PGW_DOCKER_TLS_CERT", "PgWorker:Docker:Tls:ClientCertPem"),
        ("PGW_DOCKER_TLS_KEY", "PgWorker:Docker:Tls:ClientKeyPem"),
        ("PGW_DOCKER_TLS_CA_PATH", "PgWorker:Docker:Tls:CaPath"),
        ("PGW_DOCKER_TLS_CERT_PATH", "PgWorker:Docker:Tls:ClientCertPath"),
        ("PGW_DOCKER_TLS_KEY_PATH", "PgWorker:Docker:Tls:ClientKeyPath"),
    ];

    // env-секреты → конфиг-дерево (паттерн WorkerTlsHandler.EnvBindings).
    private static readonly (string Env, string Key)[] SshBindings =
    [
        ("PGW_DOCKER_SSH_KEY", "PgWorker:Docker:Ssh:KeyPem"),
        ("PGW_DOCKER_SSH_KEY_PATH", "PgWorker:Docker:Ssh:KeyPath"),
        ("PGW_DOCKER_SSH_FINGERPRINT", "PgWorker:Docker:Ssh:FingerprintSha256"),
    ];

    // Перенос env TLS → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplyTlsEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
        => Apply(configuration, TlsBindings, getenv);

    // Перенос env SSH → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplySshEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
        => Apply(configuration, SshBindings, getenv);

    private static void Apply(ConfigurationManager configuration, (string Env, string Key)[] bindings, Func<string, string?>? getenv)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in bindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }
}
