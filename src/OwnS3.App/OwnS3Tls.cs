using System.Security.Cryptography.X509Certificates;
using Shared.Tls;

namespace OwnS3.App;

// Резолв TLS-материала ownS3 (t42, arch/owns3/03 §6 / 05 §4): PEM-пара
// (ServerCertPem/ServerKeyPem) или файлы _PATH — PEM приоритетнее пути
// (образец RestTlsMaterial воркеров). null — HTTP-режим (материал не задан);
// неполная пара/нечитаемый файл/битый PEM — исключение → fail-fast старта
// Program (Critical + exit 1), сервис не поднимается «молча на HTTP».
public static class OwnS3Tls
{
    public static X509Certificate2? Resolve(TlsOptions tls)
    {
        var certGiven = !string.IsNullOrWhiteSpace(tls.ServerCertPem)
                        || !string.IsNullOrWhiteSpace(tls.ServerCertPath);
        var keyGiven = !string.IsNullOrWhiteSpace(tls.ServerKeyPem)
                       || !string.IsNullOrWhiteSpace(tls.ServerKeyPath);
        if (!certGiven && !keyGiven)
            return null; // HTTP-режим (канон 05 §4: дефолт/стенд)
        if (!certGiven || !keyGiven)
            throw new InvalidOperationException(
                "OwnS3:Tls: задан только один из пары сертификат/ключ — укажите оба " +
                "(env OWNS3_TLS_CERT[_PATH] + OWNS3_TLS_KEY[_PATH])");

        var certPem = tls.ServerCertPem ?? TlsMaterial.ReadPemFile(tls.ServerCertPath);
        var keyPem = tls.ServerKeyPem ?? TlsMaterial.ReadPemFile(tls.ServerKeyPath);
        if (string.IsNullOrWhiteSpace(certPem) || string.IsNullOrWhiteSpace(keyPem))
            throw new InvalidOperationException(
                "OwnS3:Tls: файл сертификата/ключа не читается или пуст (OWNS3_TLS_*_PATH)");

        try
        {
            return TlsMaterial.LoadPemPair(certPem, keyPem);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"OwnS3:Tls: невалидный PEM TLS-материала — {ex.Message}", ex);
        }
    }
}
