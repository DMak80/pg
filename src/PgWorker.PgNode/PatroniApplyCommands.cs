using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
    /// процесса); pgSuPassword/pgStandbyPassword — per-install пароли PG для
    /// env регенерации (PGPASSWORD_*: без них configure_spilo пишет секцию
    /// postgresql.authentication дефолтами шаблона — zalando/standby; пустые
    /// не прокидываются — путь без доставки паролей). Ожидание — 202.</summary>
    Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
        string restPasswordNew, string pgSuPassword, string pgStandbyPassword,
        CancellationToken ct);

    /// <summary>Рестарт Patroni-программы (supervisorctl restart spilo) —
    /// применение REST-серта на реплике (рестарт покрывает и rest-пароль —
    /// reload при этом не выполняется).</summary>
    Task RestartProgramAsync(CancellationToken ct);
}

/// <summary>Прод-реализация: configure_spilo.py (child-процесс через
/// IProcessRunner, ArgumentList-канон) + REST POST /reload (HttpClient по
/// образцу PatroniRoleProbe: доверие CA из env SSL_RESTAPI_CA, SAN-пиннинг
/// loopback 127.0.0.1; таймаут 10 c — единый с клиентом канала).
/// Диагностика — через ILogger хоста (строки гарантированно попадают в
/// docker-лог контейнера; прецедент «молчащего stderr» красного прогона
/// окна ротации: error-статус публиковался, а Console.Error-строки
/// применителя в лог не попали).</summary>
public sealed class PatroniApplyCommands(
    ISupervisorCommands supervisor, ILogger? logger = null) : IPatroniApplyCommands
{
    private const int RestPort = 8008;
    private const string PatroniUser = "patroni";

    // Логгер хоста (docker-лог контейнера); NullLogger — юниты/без DI.
    private readonly ILogger _log = logger ?? NullLogger.Instance;

    // REST-клиент reload'а: один на процесс (демон-lifetime; CA-кеш по PEM —
    // как у PatroniRoleProbe). Таймаут 10 c — единый с клиентом канала
    // (pgnode-channel), новых констант не вводится.
    private static readonly SemaphoreSlim ClientGate = new(1, 1);
    private static string? _clientCaPem;
    private static HttpClient? _client;

    /// <summary>Применение rest-пары: (1) configure_spilo.py patroni --force —
    /// env процесса = env агента + SPILO_CONFIGURATION = применяемый YAML +
    /// SSL_RESTAPI_CERTIFICATE_FILE/SSL_RESTAPI_PRIVATE_KEY_FILE = пути
    /// пер-инсталл рест-серт ноды /run/certs/ (материализованы bootstrap'ом
    /// из тех же env сертов, что spilo-supervised.sh передаёт в launch; факт
    /// образа spilo-18:4.1-p2: БЕЗ --force существующий /run/postgres.yml не
    /// перезаписывается; БЕЗ env-путей placeholders SSL_RESTAPI_*_FILE пусты
    /// — у агента (user=postgres) SSL_RESTAPI_CERTIFICATE/PRIVATE_KEY env НЕТ
    /// (секреты — только tmpfs/память), шаблон restapi ТЕРЯЕТ certfile/keyfile
    /// и первое перечитывание пересоздаёт REST-листенер В PLAIN HTTP —
    /// mTLS-канал агента умирает, факт t05probe2); (2) POST /reload.
    /// Аутентификация reload-запроса — по конфигу Patroni В ПАМЯТИ; к моменту
    /// apply возможны ровно два состояния: S1 = OLD (первое применение новой
    /// ревизии: валиден OLD, NEW даёт 401) и S2 = NEW (идемпотентный повтор:
    /// прошлый reload применил, но ревизия/статус не зафиксировались; или
    /// Patroni стартовал уже из новых tmpfs-файлов: валиден NEW, OLD даёт
    /// 401). Попытки [current, new] — покрытие обоих детерминированных
    /// состояний (не подбор); 202 от любой = успех; 401 от обеих / 5xx /
    /// транспорт — отказ с фактом (повтор по коулдауну apply-ветки).
    /// Факт живой пробы S1 (t05probe-s1): NEW до применения → 401;
    /// POST /reload OLD → 202; postmaster не останавливался. Бывшее
    /// «противоречие» (reload 401 при PATCH 400 тем же паролем) РАЗРЕШЕНО:
    /// reload отправлялся PostAsync(uri, content) — Authorization терялся
    /// (401 на любой пароль), PATCH-зонд ходил SendAsync (заголовок доходит,
    /// 400 = auth пройдена); изолированная проба полного YAML (t05probe3a):
    /// reload OLD → 202, PATCH OLD → 400, GET / → 200, применение NEW
    /// подтверждено (OLD → 401, NEW → 400).</summary>
    public async Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
        string restPasswordNew, string pgSuPassword, string pgStandbyPassword, CancellationToken ct)
    {
        await RegeneratePatroniConfigAsync(
            spiloConfigurationYaml, restPasswordNew, pgSuPassword, pgStandbyPassword, ct);

        var attempts = new[] { restPasswordCurrent, restPasswordNew }
            .Where(p => !string.IsNullOrEmpty(p))
            .Distinct()
            .ToArray();
        var codes = new List<int>();
        foreach (var password in attempts)
        {
            var code = await PostReloadAsync(password!, ct);
            if (code == 202)
            {
                // 202 — Patroni ПРИНЯЛ reload, но перечитывает конфиг (вкл.
                // rest-пароль) асинхронно своим HA-циклом: до фактического
                // перечитывания REST отвечает по СТАРОЙ паре (факт прогона
                // гейта: converged опубликован сразу после 202, патрон
                // перечитал на ~0.7-2.7 с позже — проба окна в этом зазоре
                // ловила OLD). Применение ПОДТВЕРЖДАЕТСЯ зондом PATCH /config
                // NEW-парой до auth-ok (400) — converged агента (и закрытие
                // окна ротатора) наступают только по факту применения
                // (arch/14 §5 I: applied_revision — ПРИМЕНЁННОГО ключа).
                await WaitPasswordAppliedAsync(restPasswordNew, ct);
                return;
            }
            codes.Add(code);
            // Диагностика (разбор по логам, без значений секретов): зонд
            // немедленно повторяет тот же пароль МУТИРУЮЩИМ PATCH /config
            // (как внешние пробы): расхождение кодов reload-vs-patch
            // локализует отказ (эндпоинт/пароль/канал).
            if (code == 401)
                _log.LogWarning(
                    "pgnode: reload 401 diagnostic: password length={Len}, is-current={IsCurrent}, " +
                    "is-new={IsNew}, patch-same-password={PatchCode}",
                    password!.Length, password == restPasswordCurrent, password == restPasswordNew,
                    await PatchConfigProbeAsync(password!, ct));
            else
                break; // не аутентификация (5xx/транспорт) — ретрай пароля бессмыслен
        }

        throw new ApplicationException(
            $"patroni reload: no credentials applied the configuration (codes: {string.Join(", ", codes)})");
    }

    // Бюджет ожидания фактического перечитывания пароля Patroni после
    // принятого reload: HA-цикл патрона ~1 c; загрузка лидера полной серии
    // расширяет зазор до единиц секунд — бюджет с запасом, ниже клиентского
    // таймаута канала (10 c). Истечение — отказ apply (повтор по коулдауну):
    // окно ротации НЕ закрывается, пока пароль фактически не применён.
    private static readonly TimeSpan ApplyConfirmBudget = TimeSpan.FromSeconds(8);

    // Поллинг PATCH /config NEW-парой: 400 — аутентификация пройдена
    // (применено), 401 — Patroni ещё на старой паре.
    private async Task WaitPasswordAppliedAsync(string restPasswordNew, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(restPasswordNew))
            return; // данных нет — подтверждать нечем (Regenerate уже отказался бы)
        var deadline = DateTime.UtcNow + ApplyConfirmBudget;
        while (DateTime.UtcNow < deadline)
        {
            if (await PatchConfigProbeAsync(restPasswordNew, ct) == 400)
                return;
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
        }

        throw new ApplicationException(
            $"patroni reload accepted (202) but the new rest password is not active within " +
            $"{ApplyConfirmBudget.TotalSeconds:0}s (patch probe still unauthorized) — apply retried");
    }

    // Диагностический зонд: PATCH /config с тем же паролем ({} — при
    // пройденной аутентификации Patroni отвечает 400, при отвергнутой — 401).
    private static async Task<int> PatchConfigProbeAsync(string password, CancellationToken ct)
    {
        var client = await GetClientAsync(ct);
        using var request = new HttpRequestMessage(HttpMethod.Patch, $"https://127.0.0.1:{RestPort}/config")
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{PatroniUser}:{password}")));
        using var response = await client.SendAsync(request, ct);
        return (int)response.StatusCode;
    }

    /// <summary>Рестарт Patroni-программы — supervisorctl restart spilo
    /// (только серт-ветка реплики; обёртка spilo-supervised.sh гасит
    /// runit-дерево чисто, supervisord поднимает программу — configure_spilo
    /// регенерирует конфиг из tmpfs, Patroni усыновляет локальный PG).</summary>
    public Task RestartProgramAsync(CancellationToken ct)
        => supervisor.RestartAsync("spilo", ct);

    // configure_spilo.py patroni --force: child-процесс; env = env агента +
    // RegenerationEnvironment (SPILO_CONFIGURATION, env-пути рест-серт ноды
    // и per-install пароли PG — имена те же, что spilo-supervised.sh
    // экспортирует launch'у из spilo-env: без PGPASSWORD_* configure_spilo
    // собирает секцию postgresql.authentication ДЕФОЛТАМИ шаблона
    // (zalando/standby — факт t05probe3a), из-за чего перечитывание конфига
    // опускает фактические пароли в Patroni-память; с per-install env
    // регенерация сохраняет их в /run/postgres.yml). Без env-путей сертов
    // placeholders SSL_RESTAPI_*_FILE пусты (env сертов у агента нет —
    // секреты не покидают память/tmpfs), шаблон restapi теряет
    // certfile/keyfile и первое перечитывание конфига пересоздаёт
    // REST-листенер в plain HTTP (t05probe2). Exit-код НЕ является
    // критерием: скрипт штатно завершается sys.exit(int(not USE_WALG)) —
    // exit 1 при выключенном WAL-G (факт образа spilo-18:4.1-p2; файл
    // /run/postgres.yml при этом записан). Критерий применения —
    // содержимое файла: новый restapi-пароль присутствует (+ per-install
    // пароли, если заданы). Пустой restPasswordNew — дефект данных
    // (RestPasswordOf не нашёл пароль в YAML) — явный отказ, не успех.
    private async Task RegeneratePatroniConfigAsync(
        string yaml, string restPasswordNew, string pgSuPassword, string pgStandbyPassword,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(restPasswordNew))
            throw new ApplicationException("restapi password not found in YAML — regeneration refused");
        var psi = new ProcessStartInfo { FileName = "python3" };
        psi.ArgumentList.Add("/scripts/configure_spilo.py");
        psi.ArgumentList.Add("patroni");
        psi.ArgumentList.Add("--force");
        foreach (var (name, value) in RegenerationEnvironment(
                     yaml, pgSuPassword, pgStandbyPassword))
            psi.Environment[name] = value;
        var runner = new ProcessRunner();
        var (exit, _, error) = await runner.RunAsync(psi, ct);
        var config = await File.ReadAllTextAsync(PatroniConfigPath, ct);
        var missingSu = pgSuPassword.Length > 0
            && !config.Contains(pgSuPassword, StringComparison.Ordinal);
        var missingStandby = pgStandbyPassword.Length > 0
            && !config.Contains(pgStandbyPassword, StringComparison.Ordinal);
        if (!config.Contains(restPasswordNew, StringComparison.Ordinal) || missingSu || missingStandby)
        {
            // Диагностика (разбор по логам): ПОЛНЫЙ stderr child-процесса —
            // в лог агента (docker-лог контейнера и teardown-артефакты).
            _log.LogError("pgnode: configure_spilo stdout/stderr begin\n{Stderr}\n" +
                "pgnode: configure_spilo stdout/stderr end", error);
            throw new ApplicationException(
                $"configure_spilo.py: exit {exit}, /run/postgres.yml misses expected credentials " +
                $"(rest={config.Contains(restPasswordNew, StringComparison.Ordinal)}, " +
                $"su={!missingSu}, standby={!missingStandby}; stderr: {FirstLineOf(error)})");
        }
    }

    // env регенерации configure_spilo (юнит-seam): применяемый YAML +
    // env-пути пер-инсталл рест-серт ноды + per-install пароли PG (имена —
    // как в spilo-env, который spilo-supervised.sh экспортирует launch'у).
    // Пустые пароли НЕ прокидываются (путь без доставки per-install паролей
    // — configure_spilo подставит дефолты шаблона, прежнее поведение).
    internal static IReadOnlyDictionary<string, string> RegenerationEnvironment(
        string yaml, string pgSuPassword, string pgStandbyPassword)
    {
        var env = new Dictionary<string, string>
        {
            ["SPILO_CONFIGURATION"] = yaml,
            ["SSL_RESTAPI_CERTIFICATE_FILE"] = RestCertFilePath,
            ["SSL_RESTAPI_PRIVATE_KEY_FILE"] = RestKeyFilePath,
        };
        if (pgSuPassword.Length > 0)
        {
            env["PGUSER_SUPERUSER"] = "postgres";
            env["PGPASSWORD_SUPERUSER"] = pgSuPassword;
        }
        if (pgStandbyPassword.Length > 0)
        {
            env["PGUSER_STANDBY"] = "standby";
            env["PGPASSWORD_STANDBY"] = pgStandbyPassword;
        }
        return env;
    }

    private const string PatroniConfigPath = "/run/postgres.yml";

    // Пер-инсталл рест-серт ноды (спило-константы get_placeholders для env
    // SSL_RESTAPI_CERTIFICATE/PRIVATE_KEY): материализованы bootstrap'ом
    // (write_restapi_certificates из env сертов spilo-supervised.sh) — те же
    // PEM, что в bootstrap-ключе ноды.
    private const string RestCertFilePath = "/run/certs/rest-api-server.crt";
    private const string RestKeyFilePath = "/run/certs/restapi-api-server.key";

    private static string FirstLineOf(string text)
        => text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? "<empty>";

    // POST https://127.0.0.1:8008/reload (basic-auth) — код ответа; транспорт —
    // по образцу PatroniRoleProbe (CA из env SSL_RESTAPI_CA, SAN loopback).
    // Отправка — SendAsync(request): заголовок Authorization живёт В
    // HttpRequestMessage; PostAsync(uri, content) строит НОВЫЙ запрос без
    // заголовков сущности/запроса — Authorization терялся, Patroni отвечал
    // «no auth header received» 401 на ЛЮБОЙ пароль (корень «codes: 401, 401»
    // окна ротации; факт пробы t05probe3a: reload с корректно отправленным
    // заголовком = 202; PATCH-зонд всегда ходил SendAsync — отсюда его 400).
    private static async Task<int> PostReloadAsync(string password, CancellationToken ct)
    {
        var client = await GetClientAsync(ct);
        using var request = new HttpRequestMessage(HttpMethod.Post, $"https://127.0.0.1:{RestPort}/reload");
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{PatroniUser}:{password}")));
        using var response = await client.SendAsync(request, ct);
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
