using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using PgWorker.Core;
using PgWorker.Core.Templates;
using PgWorker.UnitTests.Templates;
using PgWorker.PgNode;
using Xunit;

namespace PgWorker.UnitTests.PgNode;

// Клиентский валидатор mTLS-канала (spec §8-26, MR-#5): цепочка к
// per-install CA + EKU ClientAuth + CN=pgworker. Серверный серт ноды
// (EKU ServerAuth, CN — имя ноды) отвергается; серт чужого CA — отвергается.
public class NodeClientValidatorTests
{
    private static (X509Certificate2 Ca, string CaPem, string CaKeyPem) NewCa()
    {
        var (caPem, caKeyPem) = TestRestCa.Generate();
        var ca = X509CertificateLoader.LoadCertificate(X509Certificate2.CreateFromPem(caPem, caKeyPem)
            .Export(X509ContentType.Cert));
        return (ca, caPem, caKeyPem);
    }

    private static X509Certificate2 PemToCert(string pem)
        => X509CertificateLoader.LoadCertificate(
            X509Certificate2.CreateFromPem(pem).Export(X509ContentType.Cert));

    [Fact]
    public void NodeClientValidator_RejectsServerCerts_AcceptsWorkerCert()
    {
        // Arrange: per-install CA; клиентский серт воркера и серверный серт ноды.
        var (ca, caPem, caKeyPem) = NewCa();
        var (workerPem, _) = RestPki.IssueWorkerClientCertificate(caPem, caKeyPem, "pgworker");
        var worker = PemToCert(workerPem);
        var (nodePem, _) = RestPki.IssueNodeCertificate(caPem, caKeyPem, "shard1a", "pgw-shop-shard1-shard1a");
        var nodeCert = PemToCert(nodePem);

        // Act/Assert: валидный воркер-серт принят (цепочка+EKU ClientAuth+CN).
        ClientCertValidator.Validate(worker, ca).Should().BeTrue("worker cert: chain+ClientAuth+CN=pgworker");
        // Серверный серт ноды отвергается (EKU ServerAuth, CN — имя ноды).
        ClientCertValidator.Validate(nodeCert, ca).Should().BeFalse(
            "node server cert carries no ClientAuth EKU and CN=node name");
        // Ссылки — фиксируют объекты до конца теста.
        ca.Thumbprint.Should().NotBeNullOrEmpty();
        nodePem.Should().NotBeEmpty();
    }

    [Fact]
    public void NodeClientValidator_ForeignCaCert_Rejected()
    {
        // Arrange: чужой CA и его воркер-серт — цепочка к per-install CA не строится.
        var (foreignCaPem, foreignCaKeyPem) = TestRestCa.Generate();
        var (foreignWorkerPem, _) = RestPki.IssueWorkerClientCertificate(foreignCaPem, foreignCaKeyPem, "pgworker");
        var foreignWorker = PemToCert(foreignWorkerPem);
        var (ca, _, _) = NewCa();

        // Act/Assert: чужая цепочка не доверена; null-серт — тоже false.
        ClientCertValidator.Validate(foreignWorker, ca).Should().BeFalse("a foreign CA chain is not trusted");
        ClientCertValidator.Validate(null, ca).Should().BeFalse("no client certificate at all");
        foreignCaPem.Should().NotBeEmpty();
        foreignCaKeyPem.Should().NotBeEmpty();
    }
}
