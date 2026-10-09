using PgWorker.App;
using PgWorker.Core.Templates;
using PgWorker.UnitTests.Templates;

namespace PgWorker.UnitTests.App;

// Опции REST-TLS нод (t22, arch/14 §8): полнота пары PEM|PATH и
// разбираемость PEM — предикаты fail-fast валидации старта воркера.
public class RestTlsOptionsTests
{
    [Fact]
    public void IsComplete_BothPemOrPathPairs_True()
    {
        // Arrange / Act / Assert: оба PEM заданы — пара полна.
        new RestTlsOptions { CaPem = "pem", CaKeyPem = "key" }.IsComplete().Should().BeTrue();
        // Пути вместо PEM (TLS-том /tls) — пара полна.
        new RestTlsOptions { CaPath = "/tls/ca.pem", CaKeyPath = "/tls/ca.key" }.IsComplete().Should().BeTrue();
        // Смешанный вариант — тоже полон.
        new RestTlsOptions { CaPath = "/tls/ca.pem", CaKeyPem = "key" }.IsComplete().Should().BeTrue();
    }

    [Fact]
    public void IsComplete_MissingCaOrKey_False()
    {
        // Arrange / Act / Assert: отсутствует CA — пара неполна.
        new RestTlsOptions { CaKeyPem = "key" }.IsComplete().Should().BeFalse();
        new RestTlsOptions { CaKeyPath = "/tls/ca.key" }.IsComplete().Should().BeFalse();
        // Отсутствует ключ — пара неполна.
        new RestTlsOptions { CaPem = "pem" }.IsComplete().Should().BeFalse();
        new RestTlsOptions { CaPath = "/tls/ca.pem" }.IsComplete().Should().BeFalse();
        // Пустой объект — неполна (HTTP-режима не существует).
        new RestTlsOptions().IsComplete().Should().BeFalse();
    }

    [Fact]
    public void IsValidPemPair_ValidTestCa_True()
    {
        // Arrange: валидный тестовый CA (RSA-2048 self-signed).
        var (caPem, caKeyPem) = TestRestCa.Generate();

        // Act / Assert: пара разбирается.
        RestTlsOptions.IsValidPemPair(caPem, caKeyPem).Should().BeTrue();
    }

    [Fact]
    public void IsValidPemPair_BrokenOrMissingPem_False()
    {
        // Arrange: валидный CA и мусор.
        var (caPem, caKeyPem) = TestRestCa.Generate();

        // Act / Assert: битый PEM-серт / битый ключ / отсутствующий компонент —
        // пара невалидна (fail-fast старта с diagnose про PGW_REST_TLS_*).
        RestTlsOptions.IsValidPemPair("not a pem", caKeyPem).Should().BeFalse();
        RestTlsOptions.IsValidPemPair(caPem, "not a pem").Should().BeFalse();
        RestTlsOptions.IsValidPemPair(null, caKeyPem).Should().BeFalse();
        RestTlsOptions.IsValidPemPair(caPem, null).Should().BeFalse();
    }
}
