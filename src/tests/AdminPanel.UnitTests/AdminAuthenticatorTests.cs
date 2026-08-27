using System.Security.Cryptography;
using System.Text;
using AdminPanel.Api.Auth;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты constant-time проверки учётных данных админа (spec t02 §9.1).
public class AdminAuthenticatorTests
{
    private static AdminAuthenticator Make(AuthOptions options)
        => new(Options.Create(options), NullLogger<AdminAuthenticator>.Instance);

    // Строит PBKDF2-hash в формате $pbkdf2-sha256$i$salt-b64$hash-b64 (32-байтный ключ).
    private static string MakeHash(string password, byte[] salt, int iterations)
    {
        var hash = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password), salt, iterations, HashAlgorithmName.SHA256, 32);
        return $"$pbkdf2-sha256${iterations}${Convert.ToBase64String(salt)}${Convert.ToBase64String(hash)}";
    }

    [Fact]
    public void PlainPassword_ValidCredentials_ReturnsTrue()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("admin", "s3cret");

        // Assert
        ok.Should().BeTrue();
    }

    [Fact]
    public void PlainPassword_WrongPassword_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("admin", "wrong");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void WrongUsername_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("root", "s3cret");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void PasswordHash_PrecedenceOverPlainPassword()
    {
        // Arrange: заданы оба — проверяется hash, plain игнорируется.
        var authenticator = Make(new AuthOptions
        {
            Username = "admin",
            Password = "plain",
            PasswordHash = MakeHash("hashed", [1, 2, 3], 1000),
        });

        // Act / Assert
        authenticator.Authenticate("admin", "hashed").Should().BeTrue();
        authenticator.Authenticate("admin", "plain").Should().BeFalse();
    }

    [Fact]
    public void PasswordHash_ValidPbkdf2_ReturnsTrue()
    {
        // Arrange
        var authenticator = Make(new AuthOptions
        {
            Username = "admin",
            PasswordHash = MakeHash("s3cret", [9, 8, 7, 6, 5], 2000),
        });

        // Act / Assert
        authenticator.Authenticate("admin", "s3cret").Should().BeTrue();
        authenticator.Authenticate("admin", "other").Should().BeFalse();
    }

    [Theory]
    [InlineData("not-a-hash")]
    [InlineData("$pbkdf2-sha256$0$c2FsdA==$aGFzaA==aGFzaA==")]
    [InlineData("$pbkdf2-sha256$1000$!!notbase64!!$aGFzaA==")]
    [InlineData("$pbkdf2-sha256$1000$c2FsdA==$c2hvcnQ=")]
    public void PasswordHash_Malformed_ReturnsFalse(string hash)
    {
        // Arrange: битый формат — fail-closed (spec t02 §3.4).
        var authenticator = Make(new AuthOptions { Username = "admin", PasswordHash = hash });

        // Act
        var ok = authenticator.Authenticate("admin", "whatever");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void EmptyConfig_ReturnsFalse()
    {
        // Arrange: пароль не сконфигурирован вовсе — fail-closed (spec t02 §3.5).
        var authenticator = Make(new AuthOptions { Username = "admin" });

        // Act
        var ok = authenticator.Authenticate("admin", "anything");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void EmptyUsernameAndPassword_Input_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act / Assert: пустые входы не совпадают с конфигом.
        authenticator.Authenticate(null, null).Should().BeFalse();
        authenticator.Authenticate("", "").Should().BeFalse();
    }
}
