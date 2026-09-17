using System.Security.Cryptography;
using FluentAssertions;
using ValkeyWorker.Core.Model;
using Xunit;

namespace ValkeyWorker.UnitTests.Core;

// Генератор per-cluster паролей (arch/21 §4): 32 символа [A-Za-z0-9].
public class ValkeyPasswordGeneratorTests
{
    [Fact]
    public void Generate_Длина32АлфавитАlphaNum()
    {
        // Arrange/Act
        var password = ValkeyPasswordGenerator.Generate();

        // Assert: 32 символа из [A-Za-z0-9].
        password.Should().HaveLength(32);
        password.Should().Match(p => p.All(c => char.IsAsciiLetterOrDigit(c)));
    }

    [Fact]
    public void Generate_ДваВызова_РазныеЗначения()
    {
        // Arrange/Act
        var first = ValkeyPasswordGenerator.Generate();
        var second = ValkeyPasswordGenerator.Generate();

        // Assert: CSPRNG — коллизия подряд невероятна.
        first.Should().NotBe(second);
    }

    [Fact]
    public void Generate_ТысячаГенераций_ВсеТриКлассаСимволов()
    {
        // Arrange: 1000 генераций (канон 32 симв [A-Za-z0-9]).
        var seenUpper = false;
        var seenLower = false;
        var seenDigit = false;

        // Act
        for (var i = 0; i < 1000; i++)
        {
            var password = ValkeyPasswordGenerator.Generate();
            seenUpper |= password.Any(char.IsAsciiLetterUpper);
            seenLower |= password.Any(char.IsAsciiLetterLower);
            seenDigit |= password.Any(char.IsDigit);
        }

        // Assert: алфавит покрывает все три класса.
        seenUpper.Should().BeTrue();
        seenLower.Should().BeTrue();
        seenDigit.Should().BeTrue();
    }
}
