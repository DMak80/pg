namespace AdminPanel.Etcd.Workers;

// Единый набор имён воркеров панели (гварды прокси-модулей и ResolveEndpoints
// шлюза): новый воркер добавляется ОДНОЙ правкой здесь — пропущенный литерал
// в одном месте больше не даёт «ArgumentOutOfRangeException → 500».
public static class WorkerNames
{
    public static readonly string[] All =
        ["pgworker", "kafkaworker", "valkeyworker", "s3worker", "etcdworker"];

    // Гвард прокси-хендлеров: неизвестное имя — 404 до похода в шлюз
    // (иначе ResolveEndpoints бросил бы ArgumentOutOfRangeException → 500).
    public static bool IsKnown(string worker) => All.Contains(worker);

    // Fallback ResolveEndpoints: неизвестное имя шлюза — контрактная ошибка кода.
    public static ArgumentOutOfRangeException Unknown(string worker)
        => new ArgumentOutOfRangeException(nameof(worker), worker,
            $"expected {string.Join("|", All)}");
}
