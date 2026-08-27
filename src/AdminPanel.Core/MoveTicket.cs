namespace AdminPanel.Core;

// Заявка на переезд — значение /pgworker/moves/<C>/<bucket> (arch/02 §2.3.1,
// формат PgWorker MoveRequest). Op — raw-строка канона op (move|rollback|finalize|abort);
// BucketId — id из leaf'а "bucket_<i>" (null у неканонического leaf'а).
public sealed record MoveTicket(
    string Cluster, string Bucket, int? BucketId,
    string Op, string? To, long RequestedUnix, string? RequestedBy);
