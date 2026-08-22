using AdminPanel.Api.Auth;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты оркестратора логина: rate-limit и учётные данные (spec t02 §9.2).
public class AdminLoginServiceTests
{
    private static (AdminLoginService Service, FixedTimeProvider Time) Make()
    {
        var time = new FixedTimeProvider();
        var authenticator = new AdminAuthenticator(
            Options.Create(new AuthOptions { Username = "admin", Password = "pw" }),
            NullLogger<AdminAuthenticator>.Instance);
        var service = new AdminLoginService(new LoginRateLimiter(time), authenticator);
        return (service, time);
    }

    [Fact]
    public void ValidCredentials_ReturnsOk()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("admin", "pw", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.Ok);
        result.RetryAfterSeconds.Should().Be(0);
    }

    [Fact]
    public void WrongPassword_ReturnsInvalidCredentials()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("admin", "nope", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void WrongUsername_ReturnsInvalidCredentials()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("root", "pw", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_SixthAttemptSameIp_ReturnsRateLimited()
    {
        // Arrange
        var (service, _) = Make();

        // Act: пять неудачных попыток в одном окне.
        for (var i = 0; i < 5; i++)
            service.Login("admin", "wrong", "1.1.1.1");
        var sixth = service.Login("admin", "wrong", "1.1.1.1");

        // Assert
        sixth.Status.Should().Be(LoginStatus.RateLimited);
        sixth.RetryAfterSeconds.Should().BeInRange(1, 60);
    }

    [Fact]
    public void RateLimit_WindowReset_AllowsAgain()
    {
        // Arrange
        var (service, time) = Make();
        for (var i = 0; i < 5; i++)
            service.Login("admin", "wrong", "1.1.1.1");

        // Act: окно сместилось — время ушло на 61 c вперёд.
        time.Utc += TimeSpan.FromSeconds(61);
        var result = service.Login("admin", "wrong", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_DifferentIp_Independent()
    {
        // Arrange
        var (service, _) = Make();

        // Act: IP-A исчерпал окно, IP-B приходит впервые.
        for (var i = 0; i < 6; i++)
            service.Login("admin", "wrong", "1.1.1.1");
        var other = service.Login("admin", "wrong", "2.2.2.2");

        // Assert
        other.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_CountsSuccessfulLogins()
    {
        // Arrange
        var (service, _) = Make();

        // Act: пять успешных логинов тоже занимают слоты окна (spec t02 §3.6).
        for (var i = 0; i < 5; i++)
            service.Login("admin", "pw", "1.1.1.1");
        var sixth = service.Login("admin", "pw", "1.1.1.1");

        // Assert
        sixth.Status.Should().Be(LoginStatus.RateLimited);
    }
}
