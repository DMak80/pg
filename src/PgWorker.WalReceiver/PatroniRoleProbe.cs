namespace PgWorker.WalReceiver;

/// <summary>Роль источника по REST-пробе Patroni loopback (arch/19 §3):
/// GET https://127.0.0.1:8008/primary → 200 ⇒ master; /sync → 200 ⇒ sync;
/// любой не-200/ошибка ⇒ none (переходное окно/async). Доверие — CA REST-Api
/// ноды (env SSL_RESTAPI_CA, PEM материализуется во временный файл ОДИН раз за
/// процесс и удаляется после загрузки сертификата — прецедент pgwnodetool;
/// 127.0.0.1 в SAN серта — arch/14 §2.4 п.6). Long-running демон: CA и
/// HttpClient кешируются на процесс — проба не создаёт файлы/транспорты.</summary>
public sealed class PatroniRoleProbe
{
    private const int RestPort = 8008;

    // CA-материал и транспорт — один на процесс (long-running демон: проба
    // каждые ~5 c; прежний вариант плодил temp-файл и HttpClient на пробу).
    private static readonly SemaphoreSlim CaGate = new(1, 1);
    private static string? _caPemSource;
    private static System.Security.Cryptography.X509Certificates.X509Certificate2? _ca;
    private static HttpClient? _probeClient;

    /// <summary>CA сертификата: материализация PEM в temp-файл → загрузка →
    /// удаление файла. Кеш по содержимому PEM (env ноды стабилен за процесс);
    /// подмена PEM (дрейф) — перезагрузка сертификата.</summary>
    private static async Task<System.Security.Cryptography.X509Certificates.X509Certificate2?> GetCaAsync(
        string caPem, CancellationToken ct)
    {
        if (_ca is not null && _caPemSource == caPem)
            return _ca;

        await CaGate.WaitAsync(ct);
        try
        {
            if (_ca is not null && _caPemSource == caPem)
                return _ca;

            var caPath = Path.Combine(Path.GetTempPath(), $"pgw-rest-ca-{Guid.NewGuid():N}.pem");
            try
            {
                await File.WriteAllTextAsync(caPath, caPem + "\n", ct);
                // Сертификат БЕЗ ключа: CA-PEM per-install — публичная часть CA
                // (RestCertificateCache.CaPem — caCertPem); CreateFromPemFile здесь
                // НЕ подходит (требует пару cert+key и падает на ключе).
                var loaded = System.Security.Cryptography.X509Certificates.X509CertificateLoader
                    .LoadCertificateFromFile(caPath);
                _ca = loaded;
                _caPemSource = caPem;
                return _ca;
            }
            finally
            {
                try
                {
                    File.Delete(caPath); // temp-файл не живёт дольше загрузки
                }
                catch
                {
                    // удаление temp — лучшие усилия (ФС может быть недоступна)
                }
            }
        }
        finally
        {
            CaGate.Release();
        }
    }

    /// <summary>HttpClient пробы: ОДИН на процесс (handler доверяет текущему
    /// CA-сертификату через замыкание на кеш — смена PEM подхватывается).
    /// Живёт до завершения процесса (дemon-lifetime, явного dispose не требует).
    /// SAN-пиннинг loopback (arch/14 §2.4 п.6, прецедент pgwnodetool):
    /// цепочка валидна И сертификат несёт IP 127.0.0.1 в SAN — валидная
    /// цепочка чужого серта не принимается.</summary>
    private static HttpClient GetProbeClient()
    {
        if (_probeClient is { } client)
            return client;
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
                    try
                    {
                        var ca = Volatile.Read(ref _ca);
                        if (ca is null)
                            return false; // CA ещё не загружен — доверять нечем
                        if (!Shared.Tls.TlsChain.ValidateChain(cert, ca))
                        {
                            _ = System.Console.Error.WriteLineAsync(
                                $"walreceiver role probe: server cert chain rejected (subject={cert.Subject})");
                            return false;
                        }
                        if (!SanContainsLoopback(cert))
                        {
                            _ = System.Console.Error.WriteLineAsync(
                                $"walreceiver role probe: server cert SAN has no 127.0.0.1 (subject={cert.Subject}) — pinned loopback required");
                            return false;
                        }
                        return true;
                    }
                    catch (Exception e)
                    {
                        _ = System.Console.Error.WriteLineAsync(
                            $"walreceiver role probe: chain validation threw: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                        return false;
                    }
                },
            },
        };
        _probeClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(3) };
        return _probeClient;
    }

    /// <summary>Проба: caPem (env SSL_RESTAPI_CA) → CA-кеш процесса (temp-файл
    /// ОДИН раз, после загрузки удаляется) → валидация цепочки сервера
    /// (TlsChain + SAN loopback). Любая ошибка транспорта/ФС — none
    /// (переходное окно, ожидание). Диагностика — только ПЕРЕХОД роли от
    /// последней пробы: штатный не-200 /primary на sync/async-ноде — молчание
    /// (построчный лог каждой пробы ×5 c — десятки тысяч строк/сутки в общий
    /// docker json-лог ноды).</summary>
    private static readonly object TransitionGate = new();
    private static WalReceiverRole _lastRole = WalReceiverRole.None;

    // Дедуп CA-диагностики (проба каждые ~5 c): лог ТОЛЬКО на переходе
    // CA-ok/CA-бит — построчный лог деградации даёт десятки тысяч строк/сутки.
    private static bool? _lastCaState;

    private static void LogCaState(bool ok, string message)
    {
        lock (TransitionGate)
        {
            if (_lastCaState == ok)
                return;
            _lastCaState = ok;
        }
        _ = System.Console.Error.WriteLineAsync($"walreceiver role probe: {message}");
    }

    public static async Task<WalReceiverRole> ProbeAsync(string caPem, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(caPem))
        {
            // CA REST-Api не задан — проба невозможна (env ноды дрейфовал?).
            LogCaState(false, "env SSL_RESTAPI_CA is not set — role stays none");
            return WalReceiverRole.None;
        }

        var ca = await GetCaAsync(caPem, ct);
        if (ca is null)
        {
            LogCaState(false, "CA material failed — role stays none");
            return WalReceiverRole.None; // ФС/PEM недоступны — роли нет (ожидание)
        }
        LogCaState(true, "CA restored — role probing resumes");

        var client = GetProbeClient();
        var primary = await TryGetAsync(client, "primary", ct);
        WalReceiverRole role;
        string detail;
        if (primary.Ok)
        {
            role = WalReceiverRole.Master;
            detail = "/primary → 200";
        }
        else
        {
            var sync = await TryGetAsync(client, "sync", ct);
            role = sync.Ok ? WalReceiverRole.Sync : WalReceiverRole.None;
            detail = sync.Ok ? "/sync → 200" : $"{primary.Reason}; {sync.Reason}";
        }

        lock (TransitionGate)
        {
            if (role != _lastRole)
            {
                _ = System.Console.Error.WriteLineAsync(
                    $"walreceiver role probe: role {RoleText(_lastRole)}→{RoleText(role)} ({detail})");
                _lastRole = role;
            }
        }
        return role;
    }

    private static string RoleText(WalReceiverRole role) => role switch
    {
        WalReceiverRole.Master => "master",
        WalReceiverRole.Sync => "sync",
        _ => "none",
    };

    // Одна GET-проба: без диагностического лога (штатные отказы — молчание;
    // переход роли логируется ProbeAsync целиком, с деталями обеих проб).
    private static async Task<(bool Ok, string Reason)> TryGetAsync(
        HttpClient client, string path, CancellationToken ct)
    {
        try
        {
            using var response = await client.GetAsync($"https://127.0.0.1:{RestPort}/{path}", ct);
            return (response.IsSuccessStatusCode, $"/{path} → {(int)response.StatusCode}");
        }
        catch (Exception e)
        {
            return (false, $"/{path} transport: {e.GetType().Name}");
        }
    }

    // SAN-пиннинг: ТОЧНО IP 127.0.0.1 (цель пробы) среди iPAddress-имён
    // расширения subjectAltName (OID 2.5.29.17) — соседние loopback-адреса
    // (127.0.0.2, ::1) валидным пиннингом не считаются. Мини-разбор DER:
    // SEQUENCE { GeneralName* }, [7] IMPLICIT OCTET STRING — iPAddress.
    private static bool SanContainsLoopback(
        System.Security.Cryptography.X509Certificates.X509Certificate2 cert)
    {
        foreach (var ext in cert.Extensions)
        {
            if (ext.Oid?.Value != "2.5.29.17")
                continue;
            var data = ext.RawData;
            var i = 0;
            if (data.Length < 2 || data[i] != 0x30)
                continue;
            i++;
            i += DerLengthSize(data, i);
            while (i + 1 < data.Length)
            {
                var tag = data[i++];
                var size = DerLength(data, ref i);
                if (i + size > data.Length)
                    break;
                if (tag == 0x87 && size == 4 // iPAddress — ТОЧНО 127.0.0.1 (цель пробы)
                    && data[i] == 127 && data[i + 1] == 0 && data[i + 2] == 0 && data[i + 3] == 1)
                {
                    return true;
                }
                i += size;
            }
        }
        return false;
    }

    // DER-длина: возвращает значение и сдвигает индекс; длинная форма —
    // следующий байт задаёт число байтов длины.
    private static int DerLength(byte[] data, ref int i)
    {
        var first = data[i++];
        if ((first & 0x80) == 0)
            return first;
        var count = first & 0x7F;
        var value = 0;
        for (var k = 0; k < count && i < data.Length; k++)
            value = value << 8 | data[i++];
        return value;
    }

    // Размер (в байтах) поля DER-длины, начинающегося в i (для пропуска
    // внешней SEQUENCE-обёртки SAN).
    private static int DerLengthSize(byte[] data, int i)
    {
        var first = data[i];
        return (first & 0x80) == 0 ? 1 : 1 + (first & 0x7F);
    }
}
