namespace OwnS3.App.Routing;

// Результат маршрутизации: операция, бакет/ключ (декодированные) и имя
// вне-наборного сабресурса (для 501; null при не-матче).
public sealed record S3Route(S3Operation Operation, string? Bucket, string? Key, string? RejectedSubresource);
