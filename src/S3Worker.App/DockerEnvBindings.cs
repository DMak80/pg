using Microsoft.Extensions.Configuration;

namespace S3Worker.App;

// env-секреты docker-транспорта S3Worker (arch/24 §2: транспорт — общий
// Shared.Docker; env-имена S3W_DOCKER_* — s3-специфика, модели в общей сборке
// их не знают; REST-TLS-блока нет — он pg-специфичен). PEM-дуализм env-секретов —
// значение или _PATH-файл; перенос env → конфиг-дерево — до всего остального
// (Program.cs).
internal static class DockerEnvBindings
{
    // env-секреты → конфиг-дерево: PEM-значения и _PATH-файлы (паттерн TlsEndpoints.EnvBindings).
    private static readonly (string Env, string Key)[] TlsBindings =
    [
        ("S3W_DOCKER_TLS_CA", "S3Worker:Docker:Tls:CaPem"),
        ("S3W_DOCKER_TLS_CERT", "S3Worker:Docker:Tls:ClientCertPem"),
        ("S3W_DOCKER_TLS_KEY", "S3Worker:Docker:Tls:ClientKeyPem"),
        ("S3W_DOCKER_TLS_CA_PATH", "S3Worker:Docker:Tls:CaPath"),
        ("S3W_DOCKER_TLS_CERT_PATH", "S3Worker:Docker:Tls:ClientCertPath"),
        ("S3W_DOCKER_TLS_KEY_PATH", "S3Worker:Docker:Tls:ClientKeyPath"),
    ];

    // env-секреты → конфиг-дерево (паттерн TlsEndpoints.EnvBindings).
    private static readonly (string Env, string Key)[] SshBindings =
    [
        ("S3W_DOCKER_SSH_KEY", "S3Worker:Docker:Ssh:KeyPem"),
        ("S3W_DOCKER_SSH_KEY_PATH", "S3Worker:Docker:Ssh:KeyPath"),
        ("S3W_DOCKER_SSH_FINGERPRINT", "S3Worker:Docker:Ssh:FingerprintSha256"),
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
