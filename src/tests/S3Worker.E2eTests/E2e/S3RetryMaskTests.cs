using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Юнит-факты ретрай-маски старта окружения (docs/e2e-isolation.md §6):
// гонка сети И занятость зондируемого хост-порта — в ретрай; прочие причины — нет.
public class S3RetryMaskTests
{
    [Fact]
    public void AlreadyAllocated_IsRetryable()
    {
        // Arrange: docker-ошибка занятости публикуемого порта.
        var ex = new ApplicationException("failed: port is already allocated");

        // Act/Assert: причина в ретрай-маске.
        S3E2eEnvironment.IsRetryableStartException(ex).Should().BeTrue();
    }

    [Fact]
    public void AddressInUse_IsRetryable()
    {
        // Arrange: proxy-bind ошибка занятости.
        var ex = new ApplicationException("bind: address already in use");

        // Act/Assert: причина в ретрай-маске.
        S3E2eEnvironment.IsRetryableStartException(ex).Should().BeTrue();
    }

    [Fact]
    public void NetworkRace_IsRetryable()
    {
        // Arrange: чужой глобальный prune снёс сеть старта.
        var ex = new ApplicationException("network not found");

        // Act/Assert: причина в ретрай-маске.
        S3E2eEnvironment.IsRetryableStartException(ex).Should().BeTrue();
    }

    [Fact]
    public void OtherFailure_NotRetryable()
    {
        // Arrange: причина вне маски (нет образа и пр.).
        var ex = new ApplicationException("image missing");

        // Act/Assert: не ретраится.
        S3E2eEnvironment.IsRetryableStartException(ex).Should().BeFalse();
    }
}
