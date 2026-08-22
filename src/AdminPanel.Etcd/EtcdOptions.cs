using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO etcd-подключения: секция AdminPanel:Etcd (arch/01 §6, spec §8.1).
[Config("AdminPanel:Etcd")]
public class EtcdOptions
{
    // HTTP JSON gateway endpoints, напр. "http://etcd1:2379". Обязателен хотя бы один.
    public string[] Endpoints { get; set; } = [];

    // Тик снапшота (arch/02 §4). <= 0 — fallback 3 c с LogWarning.
    public double RefreshIntervalSeconds { get; set; } = 3;

    // Таймаут HTTP-запроса к одному endpoint (arch/01 §6). <= 0 — fallback 2 c.
    public double RequestTimeoutSeconds { get; set; } = 2;
}
