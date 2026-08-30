namespace KafkaWorker.Core.Model;

/// <summary>
/// Адрес брокера: docker-хост размещения + выделенный клиентский host-порт
/// (контейнерный 9094 публикуется портом из диапазона 16000–16999).
/// Формат значения /kafkaworker/portalloc/&lt;C&gt;: {"broker<k>":{"host":"h","client":16001}}.
/// </summary>
public sealed record NodeAddress(string Host, int ClientPort);
