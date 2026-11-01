using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using PgWorker.PgNode.Operations;

namespace PgWorker.PgNode;

/// <summary>Команды применения ротаций на живом Patroni (arch/14 §5 I, spec
/// §8-29, решение 29): rest-ПАРОЛЬ — reload Patroni (перегенерация
/// /run/postgres.yml через configure_spilo --force + REST POST /reload;
/// postmaster PID/start_time неизменны — PG не останавливается; рестарт
/// Patroni-процесса/контейнера для пароля ЗАПРЕЩЁН), серт — рестарт
/// Patroni-программы supervisorctl'ом (легитимная граница запрета — ТОЛЬКО
/// реплика; гвард роли — в PgNodeMachine).</summary>
public interface IPatroniApplyCommands
{
    /// <summary>Применение rest-пары: перегенерация конфига Patroni из YAML +
    /// reload. restPasswordCurrent — пароль ТЕКУЩЕГО конфига Patroni в памяти
    /// (аутентификация reload-запроса: сервер сверяет с in-memory конфигом —
    /// факт пробы t05probe: NEW до применения даёт 401); restPasswordNew —
    /// применяемая пара (fallback, если in-memory уже NEW после рестарта
    /// процесса). Ожидание — 202.</summary>
    Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
        string restPasswordNew, CancellationToken ct);

    /// <summary>Рестарт Patroni-программы (supervisorctl restart spilo) —
    /// применение REST-серта на реплике (рестарт покрывает и rest-пароль —
    /// reload при этом не выполняется).</summary>
    Task RestartProgramAsync(CancellationToken ct);
}

/// <summary>Прод-реализация: configure_spilo.py (child-процесс через
/// IProcessRunner, ArgumentList-канон) + REST POST /reload (HttpClient по
/// образцу PatroniRoleProbe: доверие CA из env SSL_RESTAPI_CA, SAN-пиннинг
/// loopback 127.0.0.1; таймаут 10 c — единый с клиентом канала).</summary>
public sealed class PatroniApplyCommands(ISupervisorCommands supervisor) : IPatroniApplyCommands
{
    private const int RestPort = 8008;
    private const string PatroniUser = "patroni";

    // REST-клиент reload'а: один на процесс (демон-lifetime; CA-кеш по PEM —
    // как у PatroniRoleProbe). Таймаут 10 c — единый с клиентом канала
    // (pgnode-channel), новых констант не вводится.
    private static readonly SemaphoreSlim ClientGate = new(1, 1);
    private static string? _clientCaPem;
    private static HttpClient? _client;

    /// <summary>Применение rest-пары: (1) configure_spilo.py patroni --force
    /// (env процесса = env агента + SPILO_CONFIGURATION = применяемый YAML;
    /// факт образа spilo-18:4.1-p2: БЕЗ --force существующий
    /// /run/postgres.yml не перезаписывается); (2) POST /reload.
    /// Аутентификация reload-запроса — по конфигу Patroni В ПАМЯТИ; к моменту
    /// apply возможны ровно два состояния: S1 = OLD (первое применение новой
    /// ревизии: валиден OLD, NEW даёт 401) и S2 = NEW (идемпотентный повтор:
    /// прошлый reload применил, но ревизия/статус не зафиксировались; или
    /// Patroni стартовал уже из новых tmpfs-файлов: валиден NEW, OLD даёт
    /// 401). Попытки [current, new] — покрытие обоих детерминированных
    /// состояний (не подбор); 202 от любой = успех; 401 от обеих / 5xx /
    /// транспорт — отказ с фактом (повтор по коулдауну apply-ветки).
    /// Факт живой пробы S1 (t05probe-s1): NEW до применения → 401;
    /// POST /reload OLD → 202; postmaster не останавливался.</summary>
    public async Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
        string restPasswordNew, CancellationToken ct)
    {
        await RegeneratePatroniConfigAsync(spiloConfigurationYaml, restPasswordNew, ct);

        var attempts = new[] { restPasswordCurrent, restPasswordNew }
            .Where(p => !string.IsNullOrEmpty(p))
            .Distinct()
            .ToArray();
        var codes = new List<int>();
        foreach (var password in attempts)
        {
            var code = await PostReloadAsync(password!, ct);
            if (code == 202)
                return;
            codes.Add(code);
            if (code != 401)
                break; // не аутентификация (5xx/транспорт) — ретрай пароля бессмыслен
        }

        throw new ApplicationException(
            $"patroni reload: no credentials applied the configuration (codes: {string.Join(", ", codes)})");
    }

    /// <summary>Рестарт Patroni-программы — supervisorctl restart spilo
    /// (только серт-ветка реплики; обёртка spilo-supervised.sh гасит
    /// runit-дерево чисто, supervisord поднимает программу — configure_spilo
    /// регенерирует конфиг из tmpfs, Patroni усыновляет локальный PG).</summary>
    public Task RestartProgramAsync(CancellationToken ct)
        => supervisor.RestartAsync("spilo", ct);

    // configure_spilo.py patroni --force: child-процесс; env = env агента +
    // SPILO_CONFIGURATION = применяемый YAML (upstream-скрипт собирает
    // postgres.yml из этой переменной). Exit-код НЕ является критерием:
    // скрипт штатно завершается sys.exit(int(not USE_WALG)) — exit 1 при
    // выключенном WAL-G (факт образа spilo-18:4.1-p2; файл /run/postgres.yml
    // при этом записан). Критерий применения — содержимое файла: новый
    // restapi-пароль присутствует. Пустой restPasswordNew — дефект данных
    // (RestPasswordOf не нашёл пароль в YAML) — явный отказ, не успех.
    private async Task RegeneratePatroniConfigAsync(
        string yaml, string restPasswordNew, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(restPasswordNew))
            throw new ApplicationException("restapi password not found in YAML — regeneration refused");
        var psi = new ProcessStartInfo { FileName = "python3" };
        psi.ArgumentList.Add("/scripts/configure_spilo.py");
        psi.ArgumentList.Add("patroni");
        psi.ArgumentList.Add("--force");
        psi.Environment["SPILO_CONFIGURATION"] = yaml;
        var runner = new ProcessRunner();
        var (exit, _, error) = await runner.RunAsync(psi, ct);
        var config = await File.ReadAllTextAsync(PatroniConfigPath, ct);
        if (!config.Contains(restPasswordNew, StringComparison.Ordinal))
            throw new ApplicationException(
                $"configure_spilo.py: exit {exit}, /run/postgres.yml has no new rest password " +
                $"(stderr: {FirstLineOf(error)})");
    }

    private const string PatroniConfigPath = "/run/postgres.yml";

    private static string FirstLineOf(string text)
        => text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? "<empty>";

    // POST https://127.0.0.1:8008/reload (basic-auth) — код ответа; транспорт —
    // по образцу PatroniRoleProbe (CA из env SSL_RESTAPI_CA, SAN loopback).
    private static async Task<int> PostReloadAsync(string password, CancellationToken ct)
    {
        var client = await GetClientAsync(ct);
        using var request = new HttpRequestMessage(HttpMethod.Post, $"https://127.0.0.1:{RestPort}/reload");
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{PatroniUser}:{password}")));
        using var response = await client.PostAsync(request.RequestUri, request.Content, ct);
        return (int)response.StatusCode;
    }

    // CA-кеш и клиент (единообразно с PatroniRoleProbe: temp-файл ОДИН раз,
    // SAN-пиннинг loopback; таймаут — 10 c канала).
    private static async Task<HttpClient> GetClientAsync(CancellationToken ct)
    {
        var caPem = Environment.GetEnvironmentVariable("SSL_RESTAPI_CA") ?? "";
        if (_client is not null && _clientCaPem == caPem)
            return _client;

        await ClientGate.WaitAsync(ct);
        try
        {
            if (_client is not null && _clientCaPem == caPem)
                return _client;

            var ca = await PatroniRoleProbe.LoadCaAsync(caPem, ct)
                     ?? throw new ApplicationException("patroni reload: env SSL_RESTAPI_CA material failed");
            var handler = new SocketsHttpHandler
            {
                SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    {
                        if (certificate is null) return false;
                        var cert = certificate is System.Security.Cryptography.X509Certificates.X509Certificate2 c2
                            ? c2
                            : new System.Security.Cryptography.X509Certificates.X509Certificate2(certificate);
                        return Shared.Tls.TlsChain.ValidateChain(cert, ca)
                               && PatroniRoleProbe.SanContainsLoopback(cert);
                    },
                },
            };
            var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };
            _client = client;
            _clientCaPem = caPem;
            return _client;
        }
        finally
        {
            ClientGate.Release();
        }
    }
}
