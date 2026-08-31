using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves (arch/03 §1.5). Buckets nullable:
// null/отсутствие поля ловит валидатор воркера (400), не NRE.
public sealed record MoveBucketsRequest(string From, string To, IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные уже стояли (arch/03 §1.5).
public sealed record MovesQueuedDto(
    string Cluster, string From, string To,
    IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Заявки на переезды бакетов — прокси в API PgWorker (arch/02 §9.7).
public sealed record MoveBucketsCommand(
    string Cluster, string From, string To, IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<MovesQueuedDto>;

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// requestedBy передаётся заголовком X-Requested-By (шлюз Task 12) — воркер
// пишет его в requested_by заявок, значения etcd не меняются (spec §3.7).
[InjectAsScoped]
public sealed class MoveBucketsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<MoveBucketsCommand, MovesQueuedDto>
{
    public async ValueTask<Result<MovesQueuedDto>> Handle(MoveBucketsCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<MovesQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves",
            new MoveBucketsRequest(command.From, command.To, command.Buckets),
            command.RequestedBy, ct);
}
