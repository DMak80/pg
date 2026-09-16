namespace ValkeyWorker.Core.Model;

/// <summary>
/// Адрес ноды: docker-хост размещения + выделенный клиентский host-порт
/// (контейнерный 6379 публикуется портом из диапазона 17000–17999).
/// Формат значения /valkeyworker/portalloc/&lt;C&gt;: {"node<k>":{"host":"h","client":17001}}.
/// </summary>
public sealed record NodeAddress(string Host, int ClientPort);
