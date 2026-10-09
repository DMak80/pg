using System.Security.Cryptography.X509Certificates;

namespace PgWorker.IntegrationTests.E2e;

// PKI FakePatroni (t22): статическая тестовая CA + серверный серт
// (PFX round-trip — эфемерный ключ CreateFromPem macOS-сервером не читается).
public static class FakePatroniPki
{
    public static readonly (string CaPem, string CaKeyPem) Ca = E2eTestPki.GenerateCa("fake-patroni");

    public static readonly X509Certificate2 CaCert = X509Certificate2.CreateFromPem(Ca.CaPem);

    public static readonly X509Certificate2 ServerCert = BuildServerCert();

    private static X509Certificate2 BuildServerCert()
    {
        var (certPem, keyPem) = E2eTestPki.Issue(
            Ca.CaPem, Ca.CaKeyPem, "fake-patroni", ["fake-patroni"], ip: null);
        using var ephemeral = X509Certificate2.CreateFromPem(certPem, keyPem);
        return X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), null);
    }
}
