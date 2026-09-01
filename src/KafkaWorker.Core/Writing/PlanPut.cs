namespace KafkaWorker.Core.Writing;

// Нейтральная пара «ключ-значение» планов записи декларативного контракта:
// планы чистые (не зависят от etcd-клиента), txn/put выполняет хендлер API
// (task etcd-via-worker-api; прежде у панели — KvPut из AdminPanel.Etcd.Client).
public sealed record PlanPut(string Key, string Value);
