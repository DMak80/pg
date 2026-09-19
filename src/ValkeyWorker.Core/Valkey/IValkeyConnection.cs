using Shared.Core;

namespace ValkeyWorker.Core.Valkey;

// Точка подключения + креды + CA (admin-пробы воркера / converge).
// CaPem — per-cluster CA (t06): PEM якоря для TLS-валидации сервера;
// обязательный — plain-ветки нет (plain-порт закрыт).
public sealed record ValkeyEndpoint(string Host, int Port, string User, string Password, string CaPem);

// RESP-миниклиент (arch/21; spec §4.3): одна команда = одно короткоживущее
// TCP-соединение (пробы/команды тиковые, мультиплексирование не нужно);
// таймаут — секунды; ретраи — Polly снаружи (оркестрация, не клиент).
// Ошибка сети/протокола → Result.Failed (S7: слепая проба — не исключение).
public interface IValkeyConnection
{
    // PING → PONG (готовность V4, надзор C).
    Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct);

    // CONFIG GET <parameter> → словарь param → value (converge D).
    Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(ValkeyEndpoint ep, string parameter, CancellationToken ct);

    // CONFIG SET <parameter> <value> (converge D, без рестартов).
    Task<Result> ConfigSetAsync(ValkeyEndpoint ep, string parameter, string value, CancellationToken ct);

    // ACL LIST → массив строк-правил (converge D, ACL-план).
    Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct);

    // ACL SETUSER <user> <args…> (ротация E1/E3, converge D) — args целиком.
    Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct);
}
