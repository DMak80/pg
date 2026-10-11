using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты арифметики GF(2^8) с полиномом 0x11D (t40, план Task 1).
public class GaloisField256Tests
{
    [Fact]
    public void Multiply_IsCommutativeAndDistributiveOverXor()
    {
        // Arrange
        byte a = 0x57, b = 0x83, c = 0x29;

        // Act
        var ab = GaloisField256.Multiply(a, b);
        var ba = GaloisField256.Multiply(b, a);
        var distrib = GaloisField256.Multiply(a, (byte)(b ^ c));

        // Assert — коммутативность и дистрибутивность над XOR
        ab.Should().Be(ba);
        distrib.Should().Be((byte)(GaloisField256.Multiply(a, b) ^ GaloisField256.Multiply(a, c)));
    }

    [Fact]
    public void Multiply_ByZero_IsZero()
    {
        // Arrange
        byte a = 0xC2;

        // Act
        var byZero = GaloisField256.Multiply(a, 0);
        var zeroBy = GaloisField256.Multiply(0, a);

        // Assert
        byZero.Should().Be(0);
        zeroBy.Should().Be(0);
    }

    [Fact]
    public void Inverse_MultipliedBack_GivesOne()
    {
        // Arrange — все ненулевые элементы поля
        for (int v = 1; v < 256; v++)
        {
            // Act
            var product = GaloisField256.Multiply((byte)v, GaloisField256.Inverse((byte)v));

            // Assert
            product.Should().Be(1, "для v={0} обратный элемент обязан давать 1", v);
        }
    }

    [Fact]
    public void Tables_SatisfyExpLogInverse()
    {
        // Arrange
        var exp = GaloisField256.ExpTable;
        var log = GaloisField256.LogTable;

        // Act/Assert — периодичность exp: exp[i] == exp[i % 255] для i >= 255
        exp.Should().HaveCount(512);
        log.Should().HaveCount(256);
        for (int i = 255; i < 512; i++)
        {
            exp[i].Should().Be(exp[i % 255], "exp периодичен с периодом 255 (мультипликативная группа поля)");
        }

        // exp[log[v]] == v для всех ненулевых v
        for (int v = 1; v < 256; v++)
        {
            exp[log[v]].Should().Be((byte)v);
        }

        // Divide(a, b) * b == a для всех a и ненулевых b
        var rng = new Random(7);
        for (int t = 0; t < 200; t++)
        {
            byte a = (byte)rng.Next(256);
            byte b = (byte)rng.Next(1, 256);
            GaloisField256.Multiply(GaloisField256.Divide(a, b), b).Should().Be(a);
        }
    }

    [Fact]
    public void Divide_BySelf_IsOne()
    {
        // Arrange
        var rng = new Random(11);

        // Act/Assert
        for (int t = 0; t < 100; t++)
        {
            byte a = (byte)rng.Next(1, 256);
            GaloisField256.Divide(a, a).Should().Be(1);
        }
    }

    [Fact]
    public void MultiplyInPlace_ScalesSlice()
    {
        // Arrange
        var rng = new Random(13);
        byte coef = 0x8E;
        var data = Enumerable.Range(0, 257).Select(_ => (byte)rng.Next(256)).ToArray();
        var expected = data.Select(b => GaloisField256.Multiply(coef, b)).ToArray();

        // Act
        GaloisField256.MultiplyInPlace(coef, data);

        // Assert
        data.Should().Equal(expected);
    }

    [Fact]
    public void Polynomial_Is11D()
    {
        // Arrange/Act/Assert — полином поля зафиксирован референсом
        GaloisField256.Polynomial.Should().Be(0x11D);
    }
}
