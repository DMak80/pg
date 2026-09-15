using System.Security.Cryptography.X509Certificates;

namespace Shared.Tls;

// Загрузка PEM-материала (t08: копии из ApiTlsEndpoints/TlsEndpoints/WorkerTlsHandler/DockerEngine слиты).
public static class TlsMaterial
{
    // PFX round-trip: ключ из CreateFromPem эфемерный (не экспортируемый) —
    // SslStream (macOS) не может его использовать без ре-импорта.
    public static X509Certificate2 LoadPemPair(string certPem, string keyPem)
    {
        var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    }

    // CA-сертификат; на macOS — ре-импорт через PFX (паттерн WorkerTlsHandler).
    public static X509Certificate2 LoadPem(string caPem)
    {
        var ca = X509Certificate2.CreateFromPem(caPem);
        return OperatingSystem.IsMacOS()
            ? X509CertificateLoader.LoadPkcs12(ca.Export(X509ContentType.Pkcs12), null)
            : ca;
    }

    // PATH-дуализм: null — файла нет/не задан.
    public static string? ReadPemFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
