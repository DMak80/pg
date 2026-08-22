namespace AdminPanel.Infrastructure.CQRS;

// Хендлер запроса: чистое чтение, без транзакций и контекста БД.
public interface IQueryHandler<in TQ, TR>
    where TQ : IQuery<TR>
{
    ValueTask<Result<TR>> Handle(TQ query, CancellationToken ct);
}
