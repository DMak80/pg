using OwnS3.Protocol.Validation;

namespace OwnS3.UnitTests;

// Лимиты имён (arch/owns3/03 §6): бакет 3–63 [a-z0-9-] с буквенно-цифровыми
// краями; ключ — непустой, <= 1024 байт UTF-8.
public sealed class NameValidationTests
{
    [Theory]
    [InlineData("my-bucket")]
    [InlineData("ab1")]
    [InlineData("000")]
    [InlineData("example-bucket-with-long-but-valid-name-0123456789-abcdefghij")]
    public void BucketName_Valid(string name)
    {
        // Arrange / Act
        var valid = BucketNameValidator.IsValid(name);
        // Assert
        valid.Should().BeTrue();
    }

    [Fact]
    public void BucketName_Exactly63Chars_Valid()
    {
        // Arrange: ровно 63 символа — граница
        var name = new string('a', 63);
        // Act / Assert
        BucketNameValidator.IsValid(name).Should().BeTrue();
    }

    [Theory]
    [InlineData("ab")]            // короче 3
    [InlineData("A-upper")]       // заглавные запрещены
    [InlineData("-lead")]         // ведущий дефис
    [InlineData("trail-")]        // замыкающий дефис
    [InlineData("under_score")]   // подчёркивание вне алфавита
    [InlineData("точка.net")]     // точка вне алфавита
    [InlineData("")]
    public void BucketName_Invalid(string name)
    {
        // Arrange / Act
        var valid = BucketNameValidator.IsValid(name);
        // Assert
        valid.Should().BeFalse();
    }

    [Fact]
    public void BucketName_64Chars_Invalid()
    {
        // Arrange: 64 символа — за границей
        var name = new string('a', 64);
        // Act / Assert
        BucketNameValidator.IsValid(name).Should().BeFalse();
    }

    [Fact]
    public void ObjectKey_Empty_Invalid()
    {
        // Arrange / Act / Assert
        ObjectKeyValidator.IsValid("").Should().BeFalse();
    }

    [Fact]
    public void ObjectKey_1023Bytes_Valid()
    {
        // Arrange: 1021 'a' + 'é' (2 байта UTF-8) = 1023 байта
        var key = new string('a', 1021) + "é";
        // Act / Assert
        ObjectKeyValidator.IsValid(key).Should().BeTrue();
    }

    [Fact]
    public void ObjectKey_Exactly1024Bytes_Valid()
    {
        // Arrange: ровно 1024 байта — граница включительно
        var key = new string('é', 512);
        // Act / Assert
        ObjectKeyValidator.IsValid(key).Should().BeTrue();
    }

    [Fact]
    public void ObjectKey_1025Bytes_Invalid()
    {
        // Arrange: 1025 байт (многобайтовые символы) — за границей
        var key = new string('é', 512) + "a";
        // Act / Assert
        ObjectKeyValidator.IsValid(key).Should().BeFalse();
    }
}
