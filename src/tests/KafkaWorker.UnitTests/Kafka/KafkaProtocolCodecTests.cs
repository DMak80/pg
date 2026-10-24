using System.Text;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Kafka;

// Кодек примитивов Kafka-протокола (arch/16 §2.4; spec t01 §4.1/§4.8,
// Приложение A): golden bytes против apache/kafka 4.0.0.
public sealed class KafkaProtocolCodecTests
{
    private const string ClientId = "kfw-reassign";

    [Fact]
    public void Writer_Int_BigEndian()
    {
        // Arrange: целые little-endian хоста должны уйти big-endian.

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteInt16(0x0102);
        w.WriteInt32(0x03040506);
        w.WriteInt64(0x0708090A0B0C0D0E);

        // Assert: порядок байтов big-endian, размер без изменений.
        w.ToPayload().Should().Equal(Convert.FromHexString(
            "0102" + "03040506" + "0708090A0B0C0D0E"));
    }

    [Fact]
    public void Writer_UVarint_Encodings()
    {
        // Arrange/Act: однострочные значения + двухбайтные 128 и 300.
        byte[] Bytes(ulong v)
        {
            var w = new KafkaProtocolWriter();
            w.WriteUVarint(v);
            return w.ToPayload();
        }

        // Assert: 7 бит на байт, старший бит — продолжение.
        Bytes(0).Should().Equal(Convert.FromHexString("00"));
        Bytes(1).Should().Equal(Convert.FromHexString("01"));
        Bytes(127).Should().Equal(Convert.FromHexString("7F"));
        Bytes(128).Should().Equal(Convert.FromHexString("8001"));
        Bytes(300).Should().Equal(Convert.FromHexString("AC02"));
    }

    [Fact]
    public void Writer_OldStyle_Vs_Compact_Strings_Arrays()
    {
        // Arrange: old-style (int16/int32) против compact (uvarint).

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteString("abc");
        w.WriteString(null);
        w.WriteCompactString("abc");
        w.WriteCompactString(null);
        w.WriteArrayHeader(2);
        w.WriteCompactArrayHeader(2);
        w.WriteBytes([0x41, 0x42]);

        // Assert: изоляция примитивов — длины и null-маркеры разных стилей.
        w.ToPayload().Should().Equal(Convert.FromHexString(
            "0003" + "616263"        // old-style "abc"
            + "FFFF"                 // old-style null
            + "04" + "616263"        // compact "abc" (len+1)
            + "00"                   // compact null
            + "00000002"             // old-style массив 2
            + "03"                   // compact массив 2 (2+1)
            + "00000002" + "4142")); // bytes
    }

    [Fact]
    public void Writer_ApiVersions_V0_Frame_Golden()
    {
        // Arrange: ApiVersions v0 — header v1 + ПУСТОЕ тело (pre-auth, до SASL).

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV1(apiKey: 18, apiVersion: 0, correlationId: 1, clientId: ClientId);

        // Assert: кадр = int32 длина (22) + header v1; тело пустое.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "00000016",                                  // длина кадра: 22
            "0012" + "0000" + "00000001",                // api 18, v0, corr 1
            "000C" + "6B66772D726561737369676E")));       // client_id old-style (12 байт)
    }

    [Fact]
    public void Writer_SaslHandshake_V1_Frame_Golden()
    {
        // Arrange: SaslHandshake v1 — old-style строка "PLAIN".

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV1(apiKey: 17, apiVersion: 1, correlationId: 2, clientId: ClientId);
        w.WriteString("PLAIN");

        // Assert: header v1 (22 байта) + тело 7 байт = кадр 29.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "0000001D",                                  // длина кадра: 29
            "0011" + "0001" + "00000002",                // api 17, v1, corr 2
            "000C" + "6B66772D726561737369676E",         // client_id
            "0005" + "504C41494E")));                    // "PLAIN"
    }

    [Fact]
    public void Writer_SaslAuthenticate_V0_Frame_Golden()
    {
        // Arrange: SaslAuthenticate v0 — bytes \0user\0password (SASL PLAIN:
        // пустой authzid + NUL + authcid + NUL + passwd), user=admin, pw=pw.

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV1(apiKey: 36, apiVersion: 0, correlationId: 3, clientId: ClientId);
        w.WriteBytes([0x00, .. "admin"u8.ToArray(), 0x00, .. "pw"u8.ToArray()]);

        // Assert: header v1 + int32 длина 9 + 9 байт PLAIN-токена = кадр 35.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "00000023",                                  // длина кадра: 35
            "0024" + "0000" + "00000003",                // api 36, v0, corr 3
            "000C" + "6B66772D726561737369676E",         // client_id
            "00000009"                                   // AuthBytes длина 9
            + "00" + "61646D696E" + "00" + "7077")));    // \0admin\0pw
    }

    [Fact]
    public void Writer_RequestHeaderV2_Adds_Empty_TaggedSection()
    {
        // Arrange: flexible-запрос (header v2) — после old-style client_id
        // идёт uvarint 0 (клиентская tagged-секция пуста).

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV2(apiKey: 45, apiVersion: 0, correlationId: 4, clientId: ClientId);

        // Assert
        w.ToPayload().Should().Equal(Convert.FromHexString(string.Concat(
            "002D" + "0000" + "00000004",
            "000C" + "6B66772D726561737369676E",
            "00")));
    }

    [Fact]
    public void Reader_RoundTrip_And_Null_Markers()
    {
        // Arrange: писатель и читатель — взаимно обратны.
        var w = new KafkaProtocolWriter();
        w.WriteInt32(-2);
        w.WriteUVarint(300);
        w.WriteString("abc");
        w.WriteCompactString(null);
        w.WriteCompactString("abc");

        // Act
        var r = new KafkaProtocolReader(w.ToPayload());

        // Assert
        r.ReadInt32().Should().Be(-2);
        r.ReadUVarint().Should().Be(300);
        r.ReadString().Should().Be("abc");
        r.ReadCompactString().Should().BeNull();
        r.ReadCompactString().Should().Be("abc");
        r.Remaining.Should().Be(0);
    }

    [Fact]
    public void Reader_UVarint_Malformed_Throws()
    {
        // Arrange: 11 байт с установленным continuation-битом — терминатора нет.

        // Act/Assert: переполнение uvarint — исключение, не цикл.
        var r = new KafkaProtocolReader(Convert.FromHexString("8080808080808080808080"));
        var act = () => r.ReadUVarint();
        act.Should().Throw<ApplicationException>();
    }

    [Fact]
    public void Reader_SkipTaggedSection_Skips_UnknownTag_WithSize()
    {
        // Arrange: tagged-секция из одного поля: tag=1, size=3, "ABC"; после —
        // int32 0 (брокер ответил unknown-tag полем в header v1).
        var payload = Convert.FromHexString("01" + "01" + "03" + "414243" + "00000000");

        // Act
        var r = new KafkaProtocolReader(payload);
        r.SkipTaggedSection();

        // Assert: скип по size — последующие данные читаются.
        r.ReadInt32().Should().Be(0);
    }

    [Fact]
    public void BuildAlterReassignmentsRequest_Golden()
    {
        // Arrange: два топика, три партиции; реплики [1,2,3]/[2]/[2].
        var moves = new List<ReassignMove>
        {
            new("beta", 0, [2]),
            new("alpha", 1, [2]),
            new("alpha", 0, [1, 2, 3]), // вход НЕ отсортирован — кадр детерминирован
        };

        // Act
        var frame = KafkaProtocolClient.BuildAlterReassignmentsRequest(moves, correlationId: 4);

        // Assert: header v2 + timeout 60000 + compact-структуры по апстрим-схеме
        // AlterPartitionReassignmentsRequest.json (tagged-секции — в конце
        // каждой структуры: партиции, топики, тело).
        frame.Should().Equal(Convert.FromHexString(string.Concat(
            "00000052",                                               // длина кадра: 82
            "002D" + "0000" + "00000004",                             // api 45, v0, corr 4
            "000C" + "6B66772D726561737369676E" + "00",               // client_id (12 байт) + tagged header
            "0000EA60",                                               // timeout_ms 60000
            "03",                                                     // 2 топика
            "06" + "616C706861" + "03",                               // "alpha", 2 партиции
            "00000000" + "04" + "00000001" + "00000002" + "00000003" + "00", // p0 [1,2,3] + tagged
            "00000001" + "02" + "00000002" + "00",                    // p1 [2] + tagged
            "00",                                                     // tagged топика "alpha"
            "05" + "62657461" + "02",                                 // "beta", 1 партиция
            "00000000" + "02" + "00000002" + "00",                    // p0 [2] + tagged
            "00",                                                     // tagged топика "beta"
            "00")));                                                  // tagged тела
    }

    [Fact]
    public void ParseAlterReassignmentsResponse_Success()
    {
        // Arrange: throttle 0, ошибок нет; "alpha" p0 error 0, сообщения null.
        // Tagged-секции — в конце структур (партиция, топик, тело).
        var body = Convert.FromHexString(string.Concat(
            "00000000",                          // throttle_time_ms
            "0000",                              // error_code
            "00",                                // error_message null
            "02",                                // responses: 1 топик
            "06" + "616C706861" + "02",          // "alpha", 1 партиция
            "00000000" + "0000" + "00" + "00",   // p0, error 0, msg null, tagged партиции
            "00",                                // tagged топика
            "00"));                              // tagged тела

        // Act
        var result = KafkaProtocolClient.ParseAlterReassignmentsResponse(body);

        // Assert: план принят.
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public void ParseAlterReassignmentsResponse_TopLevelError()
    {
        // Arrange: error 35 + сообщение; responses = null (compact 0).
        // "no reassignment" — 15 символов → compact-строка uvarint(15+1)=0x10.
        // Tagged-секция тела — в конце тела ВСЕГДА (в т.ч. при null-responses):
        // апстрим AlterPartitionReassignmentsResponse.json — flexible 0+.
        var body = Convert.FromHexString(string.Concat(
            "00000000",                                        // throttle
            "0023",                                            // error 35
            "10" + "6E6F20726561737369676E6D656E74",           // "no reassignment" (len+1=16)
            "00",                                              // responses null
            "00"));                                            // tagged тела

        // Act
        var result = KafkaProtocolClient.ParseAlterReassignmentsResponse(body);

        // Assert: единый Failed с кодом и сообщением; nullable массив не падает.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("35").And.Contain("no reassignment");
    }

    [Fact]
    public void ParseAlterReassignmentsResponse_PerPartitionError()
    {
        // Arrange: top-level ок; p0 error 42 (msg null), p1 error 0.
        var body = Convert.FromHexString(string.Concat(
            "00000000",
            "0000",
            "00",
            "02",                                            // responses: 1 топик
            "06" + "616C706861" + "03",                      // "alpha", 2 партиции
            "00000000" + "002A" + "00" + "00",               // p0 error 42 + tagged
            "00000001" + "0000" + "00" + "00",               // p1 error 0 + tagged
            "00",                                            // tagged топика
            "00"));                                          // tagged тела

        // Act
        var result = KafkaProtocolClient.ParseAlterReassignmentsResponse(body);

        // Assert: сводка вида topic[p]: <code>.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("alpha[0]: 42");
    }

    [Fact]
    public void ParseApiVersions_SupportsAlter()
    {
        // Arrange: error 0, api 18 (v0–4) и api 45 (v0–0).
        var body = Convert.FromHexString(
            "0000" + "00000002" + "0012" + "0000" + "0004" + "002D" + "0000" + "0000");

        // Act
        var (supported, error) = KafkaProtocolClient.ParseApiVersions(body);

        // Assert
        supported.Should().BeTrue();
        error.Should().BeNull();
    }

    [Fact]
    public void ParseApiVersions_NoAlterKey()
    {
        // Arrange: apiKey 45 в ответе отсутствует.
        var body = Convert.FromHexString("0000" + "00000001" + "0012" + "0000" + "0004");

        // Act
        var (supported, error) = KafkaProtocolClient.ParseApiVersions(body);

        // Assert: внятный fail-fast, а не загадочный отказ.
        supported.Should().BeFalse();
        error.Should().Be("broker does not support AlterPartitionReassignments");
    }

    [Fact]
    public void ParseApiVersions_ErrorCode()
    {
        // Arrange: брокер ответил ошибкой ApiVersions.
        var body = Convert.FromHexString("0023" + "00000000");

        // Act
        var (supported, error) = KafkaProtocolClient.ParseApiVersions(body);

        // Assert
        supported.Should().BeFalse();
        error.Should().Contain("35");
    }

    [Fact]
    public void ParseCertificates_Splits_Bundle()
    {
        // Arrange: bundle из двух одинаковых CERTIFICATE-блоков (окно ротации).
        var (pem, _) = ClusterPki.GenerateCa("unit-codec");
        var bundle = pem + "\n" + pem;

        // Act
        var anchors = KafkaProtocolClient.ParseCertificates(bundle);

        // Assert: каждый блок — отдельный якорь доверия.
        anchors.Should().HaveCount(2);
    }
}
