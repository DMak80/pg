namespace AdminPanel.Infrastructure.CQRS;

// Хендлер команды: без GetContext/IDbContext из референса — у панели нет БД,
// роль транзакции выполняет etcd-txn клэйма (spec t12 §3.4).
public interface ICommandHandler<in TC, TR>
    where TC : ICommand<TR>
{
    ValueTask<Result<TR>> Handle(TC command, CancellationToken ct);
}
