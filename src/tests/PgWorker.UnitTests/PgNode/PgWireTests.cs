using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.UnitTests.PgNode;

// Wire-протокол PG v3 (spec t01 §3.1): кодек фреймов, SCRAM-SHA-256 по
// эталонному вектору RFC 5802, cleartext-цикл, неподдерживаемые методы.
public class PgWireTests
{
    // AAA: кадр write→read проходит roundtrip по паре MemoryStream.
    [Fact]
    public async Task FrameCodec_RoundTrip()
    {
        // Arrange
        var stream = new MemoryStream();
        var payload = new byte[] { 1, 2, 3 };

        // Act
        await PgWireReplicationClient.WriteFrameAsync(stream, (byte)'D', payload, CancellationToken.None);
        stream.Position = 0;
        var message = await PgWireReplicationClient.ReadFrameAsync(stream, CancellationToken.None);

        // Assert
        message.Should().NotBeNull();
        message!.Type.Should().Be((byte)'D');
        message.Payload.Should().Equal(payload);
    }

    // AAA: мусорная длина кадра (<4) — исключение, а не чтение «сколько есть».
    [Fact]
    public async Task FrameCodec_GarbageLength_Throws()
    {
        // Arrange — тип 'D' и длина 3 (меньше собственного заголовка).
        var stream = new MemoryStream([(byte)'D', 0, 0, 0, 3]);

        // Act
        var act = () => PgWireReplicationClient.ReadFrameAsync(stream, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ApplicationException>();
    }

    // AAA: SCRAM-SHA-256 по фиксированным nonce/salt/iter/password — proof и
    // server-signature совпадают с ручным расчётом по RFC 5802.
    [Fact]
    public void Scram_ReferenceVector_MatchesManualComputation()
    {
        // Arrange — эталонный вектор RFC 5802 (адаптированный под SHA-256).
        const string password = "pencil";
        const string clientNonce = "rOprNGfwEbeRWgbNEkqO";
        var clientFirstBare = "n=,r=" + clientNonce;
        const string serverFirst =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        const string withoutProof =
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        var authMessage = clientFirstBare + "," + serverFirst + "," + withoutProof;
        var salt = Convert.FromBase64String("W22ZaJ0SNY7soEsUEjb6gQ==");
        var salted = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password.Normalize(NormalizationForm.FormKC)),
            salt, 4096, HashAlgorithmName.SHA256, 32);
        var clientKey = HMACSHA256.HashData(salted, "Client Key"u8);
        var storedKey = SHA256.HashData(clientKey);
        var signature = HMACSHA256.HashData(storedKey, Encoding.UTF8.GetBytes(authMessage));
        var proof = new byte[clientKey.Length];
        for (var i = 0; i < proof.Length; i++)
            proof[i] = (byte)(clientKey[i] ^ signature[i]);
        var expectedFinal = withoutProof + ",p=" + Convert.ToBase64String(proof);
        var expectedServerSignature = HMACSHA256.HashData(
            HMACSHA256.HashData(salted, "Server Key"u8),
            Encoding.UTF8.GetBytes(authMessage));

        // Act
        var result = ScramSha256.BuildClientFinal(password, clientFirstBare, serverFirst);
        var verified = ScramSha256.VerifyServerFinal(
            "v=" + Convert.ToBase64String(expectedServerSignature), result.ServerSignature);

        // Assert
        result.Message.Should().Be(expectedFinal);
        result.ServerSignature.Should().Equal(expectedServerSignature);
        verified.Should().BeTrue();
    }

    // AAA: cleartext-цикл — R(3) → клиент отвечает 'p' с паролем → 'Z'
    // завершает подключение.
    [Fact]
    public async Task CleartextAuth_SendsPasswordFrame_ThenReady()
    {
        // Arrange — фейк-«сервер»: R(3) и затем ReadyForQuery.
        var server = new MemoryStream();
        await WriteFrameAsync(server, (byte)'R', CodePayload(3));
        await WriteFrameAsync(server, (byte)'Z', []);
        server.Position = 0;
        var clientOutput = new MemoryStream();
        var client = new PgWireReplicationClient(
            "127.0.0.1", 5432, "backup_exec", "secret", server, clientOutput);

        // Act
        await client.AuthenticateAsync(CancellationToken.None);

        // Assert — startup-сообщение и кадр пароля в выводе клиента.
        clientOutput.Position = 0;
        var lengthBytes = new byte[4];
        await clientOutput.ReadExactlyAsync(lengthBytes, CancellationToken.None);
        var startupLength = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
        var startupBody = new byte[startupLength - 4];
        await clientOutput.ReadExactlyAsync(startupBody, CancellationToken.None);
        BinaryPrimitives.ReadInt32BigEndian(startupBody).Should().Be(196608); // 3.0
        var startupText = Encoding.UTF8.GetString(startupBody);
        startupText.Should().Contain("replication").And.Contain("database");
        var passwordFrame = await PgWireReplicationClient.ReadFrameAsync(clientOutput, CancellationToken.None);
        passwordFrame!.Type.Should().Be((byte)'p');
        Encoding.UTF8.GetString(passwordFrame.Payload).Should().Be("secret\0");
    }

    // AAA: md5-аутентификация (код 5) — fail-fast с кодом в тексте.
    [Fact]
    public async Task UnsupportedAuth_Code5_Throws()
    {
        // Arrange
        var server = new MemoryStream();
        await WriteFrameAsync(server, (byte)'R', CodePayload(5));
        server.Position = 0;
        var client = new PgWireReplicationClient(
            "127.0.0.1", 5432, "backup_exec", "secret", server, new MemoryStream());

        // Act
        var act = () => client.AuthenticateAsync(CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Be("unsupported auth: 5");
    }

    private static async Task WriteFrameAsync(Stream stream, byte type, byte[] payload)
    {
        var header = new byte[5];
        header[0] = type;
        BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(1), payload.Length + 4);
        await stream.WriteAsync(header);
        await stream.WriteAsync(payload);
    }

    private static byte[] CodePayload(int code)
    {
        var payload = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(payload, code);
        return payload;
    }
}
