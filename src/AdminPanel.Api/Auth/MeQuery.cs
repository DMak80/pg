using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// Запрос текущей сессии: username кладётся в query из ClaimsPrincipal эндпоинтом.
public sealed record MeQuery(string Username) : IQuery<MeDto>;

// Ответ GET /api/auth/me.
public sealed record MeDto(string Username);

// Хендлер: чистое чтение без внешних зависимостей.
[InjectAsScoped]
public sealed class MeQueryHandler : IQueryHandler<MeQuery, MeDto>
{
    public ValueTask<Result<MeDto>> Handle(MeQuery query, CancellationToken ct)
        => ValueTask.FromResult(Result<MeDto>.Success(new MeDto(query.Username)));
}
