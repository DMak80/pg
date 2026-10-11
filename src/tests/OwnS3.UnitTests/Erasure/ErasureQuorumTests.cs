using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты кворумов канона 06 §7 (t40, план Task 8).
public class ErasureQuorumTests
{
    [Theory]
    [InlineData(3, 1, 3, 3)]
    [InlineData(2, 2, 2, 3)]
    [InlineData(8, 8, 8, 9)]
    [InlineData(15, 1, 15, 15)]
    public void Quorum_Boundaries(int k, int m, int expectedRead, int expectedWrite)
    {
        // Arrange/Act/Assert
        ErasureQuorum.Read(k, m).Should().Be(expectedRead);
        ErasureQuorum.Write(k, m).Should().Be(expectedWrite);
    }

    [Fact]
    public void MapStorageCode_NewCodes_MapTo503()
    {
        // Arrange/Act/Assert — новые коды каталога: статус 503 (глава 02 «Недоступность кворума»)
        OwnS3.Protocol.Errors.S3ErrorCatalog.Get(OwnS3.Protocol.Errors.S3ErrorCode.InsufficientReadQuorum)
            .HttpStatus.Should().Be(503);
        OwnS3.Protocol.Errors.S3ErrorCatalog.Get(OwnS3.Protocol.Errors.S3ErrorCode.InsufficientWriteQuorum)
            .HttpStatus.Should().Be(503);
    }
}
