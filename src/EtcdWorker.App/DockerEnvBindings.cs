using Microsoft.Extensions.Configuration;

namespace EtcdWorker.App;

// env-секреты docker-транспорта EtcdWorker (arch/s3worker/01-placement.md: транспорт — общий
// Shared.Docker; env-имена ETCDW_DOCKER_* — etcd-специфика, модели в общей сборке
// их не знают; REST-TLS-блока нет — он pg-специфичен). PEM-дуализм env-секретов —
// значение или _PATH-файл; перенос env → конфиг-дерево — до всего остального
// (Program.cs).
internal static class DockerEnvBindings
{
    // env-секреты → конфиг-дерево: PEM-значения и _PATH-файлы (паттерн TlsEndpoints.EnvBindings).
    private static readonly (string Env, string Key)[] TlsBindings =
    [
        ("ETCDW_DOCKER_TLS_CA", "EtcdWorker:Docker:Tls:CaPem"),
        ("ETCDW_DOCKER_TLS_CERT", "EtcdWorker:Docker:Tls:ClientCertPem"),
        ("ETCDW_DOCKER_TLS_KEY", "EtcdWorker:Docker:Tls:ClientKeyPem"),
        ("ETCDW_DOCKER_TLS_CA_PATH", "EtcdWorker:Docker:Tls:CaPath"),
        ("ETCDW_DOCKER_TLS_CERT_PATH", "EtcdWorker:Docker:Tls:ClientCertPath"),
        ("ETCDW_DOCKER_TLS_KEY_PATH", "EtcdWorker:Docker:Tls:ClientKeyPath"),
    ];

    // env-секреты → конфиг-дерево (паттерн TlsEndpoints.EnvBindings).
    private static readonly (string Env, string Key)[] SshBindings =
    [
        ("ETCDW_DOCKER_SSH_KEY", "EtcdWorker:Docker:Ssh:KeyPem"),
        ("ETCDW_DOCKER_SSH_KEY_PATH", "EtcdWorker:Docker:Ssh:KeyPath"),
        ("ETCDW_DOCKER_SSH_FINGERPRINT", "EtcdWorker:Docker:Ssh:FingerprintSha256"),
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
