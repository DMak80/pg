namespace AdminPanel.Probes;

// Разрешение адреса цели пробы: адрес из etcd → override при точном совпадении
// host:port → прямое подключение к полученному адресу (arch/02 §6, spec §4.5).
// Значения карты — полные "host:port"; чистая функция — unit-тестируется без сети.
public static class HostMapResolver
{
    // Конфиг-провайдеры .NET режут ключи секций по ':', поэтому словарь из
    // appsettings с ключами "host:port" биндится пустым. В конфигурации ключ
    // задаётся как "host__port" (t10: единственный способ задать HostMap
    // через appsettings; в памяти/тестах работает и канонический "host:port").
    public static string Resolve(IReadOnlyDictionary<string, string> hostMap, string host, int port)
        => hostMap.TryGetValue($"{host}:{port}", out var mapped)
           || hostMap.TryGetValue($"{host}__{port}", out mapped)
            ? mapped
            : $"{host}:{port}";
}
