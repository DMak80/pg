using Microsoft.Extensions.Configuration;

namespace Shared.Tls;

// Перенос env-секретов → конфиг-дерево (PEM-значения и _PATH-файлы); наборы пар
// остаются у потребителей (t08: три копии env→config слиты).
public static class TlsEnv
{
    public static void ApplyEnvOverrides(
        (string Env, string Key)[] bindings,
        ConfigurationManager configuration,
        Func<string, string?>? getenv = null)
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
