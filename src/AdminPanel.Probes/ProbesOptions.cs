using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Probes;

// [Config]-POCO live-проб: секция AdminPanel:Probes (arch/01 §6, arch/02 §6; spec §4.4).
// Суффикс Seconds — прецедент EtcdOptions (t03 §3.3).
[Config("AdminPanel:Probes")]
public class ProbesOptions
{
    // Patroni REST :8008/cluster — включена по умолчанию (arch/02 §6.1).
    public bool PatroniEnabled { get; set; } = true;

    // SQL-проба Npgsql — включена по умолчанию; в проде — на усмотрение (arch/02 §6.2).
    public bool SqlEnabled { get; set; } = true;

    // Тик оркестратора (arch/02 §4). <= 0 — fallback 15 c с LogWarning.
    public double IntervalSeconds { get; set; } = 15;

    // Таймаут одной пробы: HTTP-запрос / connection+command SQL (arch/01 §6). <= 0 — 3 c.
    public double TimeoutSeconds { get; set; } = 3;

    // Пароль SQL-проб: в DSN из etcd пароля нет никогда (arch/02 §2.1); пусто —
    // ключ не попадает в строку (стенд trust, arch/04 §5). Секрет — env поверх json.
    public string Password { get; set; } = "";

    // «etcd-адрес ноды host:port» → «адрес, достижимый с хоста панели» (arch/02 §6):
    // точное совпадение ключа, иначе адрес без изменений; по умолчанию пуст (прод).
    public Dictionary<string, string> HostMap { get; set; } = [];
}
