namespace AdminPanel.Probes;

// Разрешение адреса цели пробы: адрес из etcd → override при точном совпадении
// host:port → прямое подключение к полученному адресу (arch/02 §6, spec §4.5).
// Значения карты — полные "host:port"; чистая функция — unit-тестируется без сети.
public static class HostMapResolver
{
    public static string Resolve(IReadOnlyDictionary<string, string> hostMap, string host, int port)
        => hostMap.TryGetValue($"{host}:{port}", out var mapped) ? mapped : $"{host}:{port}";
}
