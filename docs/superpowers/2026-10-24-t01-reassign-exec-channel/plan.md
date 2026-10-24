# План t01-reassign-exec-channel: подача reassignment по Kafka-протоколу, docker-exec убирается

> **Для агентов-исполнителей:** план исполняет субагент coder (сильная модель) по шагам;
> подпункты внутри шага — самодостаточные задания (механика, не требующая знания
> проекта). Спорковать субагентов не нужно: coder делает всё сам. Шаги отмечаются
> чекбоксами (`- [ ]`) по мере готовности.

**Цель:** заменить docker-exec подачу `kafka-reassign-partitions.sh --execute` в
`PartitionReassignerProcess` операцией Kafka-протокола `AlterPartitionReassignments`
(KIP-455) из собственного протокольного миниклиента воркера; `ExecNodeAsync`
исчезает из KafkaWorker целиком (spec отклонение arch/25 №24 закрывается).

**Архитектура:** новый internal-миниклиент `KafkaProtocolClient`
(TcpClient+SslStream, per-call соединение, паттерн `ValkeyConnection.ExecuteAsync`)
вызывается из адаптера `KafkaAdminClient` через seam
`IKafkaAdminClient.AlterReassignmentsAsync`; процессы по-прежнему видят только
seam. Кодек примитивов протокола — `KafkaProtocolCodec.cs` (big-endian, uvarint,
compact/old-style строки и массивы, tagged-поля, кадры и заголовки). TLS-доверие —
все CERTIFICATE-блоки `ca_pem` (bundle окна ротации) через новую перегрузку
`Shared.Tls.TlsChain.ValidateChain`. Прогресс-ключ etcd, `ReassignPlanner`,
AdminPanel-контракт — без изменений.

**Стек:** .NET 10, `TreatWarningsAsErrors=true`, без новых NuGet-пакетов (клиент и
кодек — stdlib), Confluent.Kafka остаётся для describe-операций.

**Spec:** `docs/superpowers/2026-10-24-t01-reassign-exec-channel/spec.md` (план
аргументирует от spec; исполнитель читает оба документа).

**Worktree:** все пути ниже — от корня
`/Users/demakaev/ZCodeProject/worktrees/feat-t01-reassign-exec-channel`.

## Глобальные ограничения (spec §2.6, §6)

- Никаких новых NuGet-пакетов; примитивы протокола — stdlib.
- `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` —
  всегда ОТДЕЛЬНОЙ командой до тестов; тесты — только с `--no-build`.
- Сборка обязана быть зелёной без предупреждений (`TreatWarningsAsErrors`).
- Комментарии/XML-доки — по-русски; идентификаторы — английские; тесты — AAA.
- Таймауты: `BrokerBootSec` интеграционных фикстур ≤ 100 с (не трогаем — уже так);
  бюджеты подач — 30 с per-endpoint (`NetworkSubmitTimeout`) и 60 с внешний
  (`ReassignSubmitSec`), реквестный `timeout_ms=60000`.
- Docker-интеграции: после КАЖДОЙ серии — зачистка контуров (контейнеры/сети
  `kfw-*`); литералов портов в тестах нет и не появляется.
- Никаких изменений: `ReassignPlanner`, etcd-ключи/форматы
  (`/kafkaworker/reassignments/<C>`, `rebalances`, `ticket_outcomes`), AdminPanel,
  сид, `KafkaAdminClientFactory` (кэш/unhealthy/дрейн), PgWorker, `Shared.Docker`.
- Нулевое предупреждение = нулевая правка «на глаз»: чистка `using` после удаления
  типов (включая `KafkaWorker.Docker.Drivers` из
  `PartitionReassignerProcess.cs`, если типы оттуда больше не используются).

## Review Focus (риски, не покрытые напрямую тестами шагов)

1. Брокер без apiKey 45 (старый/чужой образ) → внятный fail-fast, а не падение
   парсера: пинит юнит-тест `ParseApiVersions_NoAlter-Key` (шаг 3, подпункт 3.2).
2. Несовпадение correlation id в ответе → `Result.Failed`, а не молчаливый
   разсинхрон: поведение зафиксировано кодом `KafkaProtocolClient.RoundTripAsync`
   (шаг 3, подпункт 3.1 — сверка после чтения каждого заголовка ответа).
3. Битый/чрезмерный uvarint (нет терминатора) → исключение сразу, а не зависание
   цикла чтения: пинит юнит-тест `Reader_UVarint_Malformed_Throws` (шаг 1,
   подпункт 1.2).
4. Nullable compact-массив `responses` (uvarint 0) при top-level ошибке ответа →
   парсер не падает NRE: пинит юнит-тест `ParseAlterReassignmentsResponse_TopLevelError`
   (шаг 3, подпункт 3.2).
5. CA-bundle из нескольких CERTIFICATE-блоков (окно ротации OLD+NEW) → доверие
   обоим якорям: парсер пинит юнит-тест `ParseCertificates_Splits_Bundle` (шаг 3,
   подпункт 3.2); валидация цепочки с bundle — интеграционно (реальный
   per-cluster CA фикстуры).

---

## Шаг 1. Кодек KafkaProtocolCodec + юнит-тесты golden bytes

- [x] выполнен (golden-байты client_id «kfw-reassign» скорректированы: длина 12 байт = 0x000C, кадры 0x16/0x1D/0x23 — в тексте плана была длина 11)

**Вход:** worktree чист; спецификации примитивов — spec §4.1 и Приложение A.

**Действие:** создать два файла по подпунктам 1.1 и 1.2 (полный код дан в
подпунктах; `KafkaWorker.Provisioning.csproj` уже содержит
`<InternalsVisibleTo Include="KafkaWorker.UnitTests"/>` — отдельная правка не
нужна).

**Выход:** internal-классы `KafkaProtocolWriter`/`KafkaProtocolReader` в
`src/KafkaWorker.Provisioning/Kafka/KafkaProtocolCodec.cs`; тесты
`src/tests/KafkaWorker.UnitTests/Kafka/KafkaProtocolCodecTests.cs` зелёные.

**Проверка:**
1. Сборка юнит-цепочки: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --nologo` — зелёная, без предупреждений.
2. Тесты кодека: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build --filter "FullyQualifiedName~KafkaProtocolCodecTests"` — все зелёные.

**Связь со spec:** §4.1 (состав примитивов), Приложение A (точные схемы), §5.1
(фаза «кодек»), §8.3.

### Подпункт 1.1. Создать файл `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolCodec.cs`

Новый файл со следующим содержимым (два internal-класса, namespace
`KafkaWorker.Provisioning.Kafka`; без внешних зависимостей):

```csharp
using System.IO;
using System.Text;

namespace KafkaWorker.Provisioning.Kafka;

// Писатель примитивов Kafka-протокола (arch/16 §2.4, spec t01 §4.1): всё
// big-endian; compact-примитивы (uvarint) — flexible-тела, old-style (int16/
// int32 длины) — заголовки и не-flexible тела. Пишет в память; ToFrame()
// отдаёт кадр «int32 длина + payload».
internal sealed class KafkaProtocolWriter
{
    private readonly MemoryStream _buffer = new();

    public void WriteInt16(short value) => WriteRaw([(byte)(value >> 8), (byte)value]);
    public void WriteInt32(int value) => WriteRaw([(byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value]);
    public void WriteInt64(long value)
    {
        WriteInt32((int)(value >> 32));
        WriteInt32((int)value);
    }

    // Unsigned varint: 7 бит на байт, старший бит — продолжение (KIP-482).
    public void WriteUVarint(ulong value)
    {
        while (value >= 0x80)
        {
            _buffer.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        _buffer.WriteByte((byte)value);
    }

    // Old-style строка (int16 длина + UTF-8; -1 = null) — заголовки, не-flexible тела.
    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteInt16(-1);
            return;
        }
        WriteInt16((short)Encoding.UTF8.GetByteCount(value));
        WriteRaw(Encoding.UTF8.GetBytes(value));
    }

    // Compact-строка (uvarint длина+1; 0 = null) — flexible тела.
    public void WriteCompactString(string? value)
    {
        if (value is null)
        {
            WriteUVarint(0);
            return;
        }
        WriteUVarint((ulong)Encoding.UTF8.GetByteCount(value) + 1);
        WriteRaw(Encoding.UTF8.GetBytes(value));
    }

    // Байты (int32 длина + данные) — SaslAuthenticate.AuthBytes.
    public void WriteBytes(byte[] value)
    {
        WriteInt32(value.Length);
        WriteRaw(value);
    }

    // Old-style массив: int32 счётчик.
    public void WriteArrayHeader(int count) => WriteInt32(count);

    // Compact-массив: uvarint счётчик+1 (0 = null — не используется писателем).
    public void WriteCompactArrayHeader(int count) => WriteUVarint((ulong)count + 1);

    // Пустая секция tagged-полей (наши сообщения tagged-полей не несут).
    public void WriteTaggedSection() => WriteUVarint(0);

    // RequestHeader v1 (не-flexible запросы): api_key, api_version,
    // correlation_id, client_id old-style (int16 — ВСЕГДА old-style, даже в
    // flexible-запросах, апстрим RequestHeader.json).
    public void WriteRequestHeaderV1(short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        WriteInt16(apiKey);
        WriteInt16(apiVersion);
        WriteInt32(correlationId);
        WriteString(clientId);
    }

    // RequestHeader v2 (flexible запросы): v1 + пустая tagged-секция.
    public void WriteRequestHeaderV2(short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        WriteRequestHeaderV1(apiKey, apiVersion, correlationId, clientId);
        WriteTaggedSection();
    }

    // Payload без кадра-длины.
    public byte[] ToPayload() => _buffer.ToArray();

    // Кадр: int32 длина + payload (запрос; ответ имеет тот же формат).
    public byte[] ToFrame()
    {
        var payload = _buffer.ToArray();
        var frame = new byte[4 + payload.Length];
        frame[0] = (byte)(payload.Length >> 24);
        frame[1] = (byte)(payload.Length >> 16);
        frame[2] = (byte)(payload.Length >> 8);
        frame[3] = (byte)payload.Length;
        payload.CopyTo(frame, 4);
        return frame;
    }

    private void WriteRaw(byte[] bytes) => _buffer.Write(bytes, 0, bytes.Length);
}

// Читатель примитивов Kafka-протокола (arch/16 §2.4, spec t01 §4.1): поверх
// payload-байтов (после кадра-длины); любое повреждение (нехватка данных,
// чрезмерная длина) — ApplicationException, не зависание.
internal sealed class KafkaProtocolReader(byte[] payload)
{
    private int _position;

    public int Remaining => payload.Length - _position;

    public short ReadInt16() => (short)((ReadByte() << 8) | ReadByte());
    public int ReadInt32() => (ReadInt16() << 16) | (ushort)ReadInt16();
    public long ReadInt64() => ((long)ReadInt32() << 32) | (uint)ReadInt32();

    public ulong ReadUVarint()
    {
        ulong value = 0;
        for (var shift = 0; ; shift += 7)
        {
            if (shift >= 64)
                throw new ApplicationException("Kafka protocol: uvarint is too long");
            var b = ReadByte();
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return value;
        }
    }

    // Old-style nullable строка (int16 длина; -1 = null).
    public string? ReadString()
    {
        var length = ReadInt16();
        if (length < 0)
            return null;
        return Encoding.UTF8.GetString(ReadRaw(length));
    }

    // Compact nullable строка (uvarint длина+1; 0 = null).
    public string? ReadCompactString()
    {
        var length = (int)ReadUVarint();
        if (length == 0)
            return null;
        return Encoding.UTF8.GetString(ReadRaw(length - 1));
    }

    // Байты (int32 длина + данные).
    public byte[] ReadBytes()
    {
        var length = ReadInt32();
        if (length < 0)
            throw new ApplicationException($"Kafka protocol: negative bytes length {length}");
        return ReadRaw(length);
    }

    // Old-style массив: int32 счётчик.
    public int ReadArrayHeader() => ReadInt32();

    // Compact-массив: uvarint счётчик+1 (0 = null → -1).
    public int ReadCompactArrayHeader()
    {
        var value = (int)ReadUVarint();
        return value - 1;
    }

    // Скип tagged-полей ответа (unknown tags): uvarint count, для каждого
    // uvarint tag, uvarint size, size байт (spec t01 §4.1).
    public void SkipTaggedSection()
    {
        var count = (int)ReadUVarint();
        for (var i = 0; i < count; i++)
        {
            _ = ReadUVarint(); // tag
            var size = (int)ReadUVarint();
            _ = ReadRaw(size);
        }
    }

    // ResponseHeader v0 (не-flexible ответы): int32 correlation_id.
    public int ReadResponseHeaderV0() => ReadInt32();

    // ResponseHeader v1 (flexible ответы): correlation_id + tagged-секция (скип).
    public int ReadResponseHeaderV1()
    {
        var correlationId = ReadInt32();
        SkipTaggedSection();
        return correlationId;
    }

    private byte ReadByte()
    {
        if (_position >= payload.Length)
            throw new ApplicationException("Kafka protocol: unexpected end of payload");
        return payload[_position++];
    }

    private byte[] ReadRaw(int length)
    {
        if (length < 0 || _position + length > payload.Length)
            throw new ApplicationException("Kafka protocol: unexpected end of payload");
        var result = new byte[length];
        Array.Copy(payload, _position, result, 0, length);
        _position += length;
        return result;
    }
}
```

### Подпункт 1.2. Создать файл `src/tests/KafkaWorker.UnitTests/Kafka/KafkaProtocolCodecTests.cs`

Новый файл (namespace `KafkaWorker.UnitTests.Kafka`; `Xunit` и `FluentAssertions`
уже глобальные usings проекта). Golden-байты собраны полями с комментариями —
копировать как есть:

```csharp
using System.Text;
using KafkaWorker.Provisioning.Kafka;

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

        // Assert: кадр = int32 длина (21) + header v1; тело пустое.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "00000015",                                  // длина кадра: 21
            "0012" + "0000" + "00000001",                // api 18, v0, corr 1
            "000B" + "6B66772D726561737369676E")));       // client_id old-style
    }

    [Fact]
    public void Writer_SaslHandshake_V1_Frame_Golden()
    {
        // Arrange: SaslHandshake v1 — old-style строка "PLAIN".

        // Act
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV1(apiKey: 17, apiVersion: 1, correlationId: 2, clientId: ClientId);
        w.WriteString("PLAIN");

        // Assert: header v1 (21 байт) + тело 7 байт = кадр 28.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "0000001C",                                  // длина кадра: 28
            "0011" + "0001" + "00000002",                // api 17, v1, corr 2
            "000B" + "6B66772D726561737369676E",         // client_id
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

        // Assert: header v1 + int32 длина 9 + 9 байт PLAIN-токена = кадр 34.
        w.ToFrame().Should().Equal(Convert.FromHexString(string.Concat(
            "00000022",                                  // длина кадра: 34
            "0024" + "0000" + "00000003",                // api 36, v0, corr 3
            "000B" + "6B66772D726561737369676E",         // client_id
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
            "000B" + "6B66772D726561737369676E",
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
}
```

---

## Шаг 2. Перегрузка `TlsChain.ValidateChain` для CA-bundle + ссылка на проект

- [x] выполнен

**Вход:** шаг 1 завершён (сборка юнит-цепочки зелёная).

**Действие:** подпункты 2.1 (перегрузка в `src/Shared.Tls/TlsChain.cs`) и 2.2
(ProjectReference `Shared.Tls` в `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj`
— сейчас KafkaWorker-проекты на `Shared.Tls` НЕ ссылаются, а миниклиенту шага 3
он нужен; ссылка на `ValkeyPki` не добавляется — spec §7.12).

**Выход:** `TlsChain.ValidateChain(X509Certificate?, IReadOnlyList<X509Certificate2>)`
существует; `KafkaWorker.Provisioning` компилируется со ссылкой на `Shared.Tls`.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj -c Release --nologo` — зелёная (существующие вызовы ValkeyWorker не меняются: односертовая перегрузка сохраняет сигнатуру).

**Связь со spec:** §4.3, §7.12.

### Подпункт 2.1. Правка `src/Shared.Tls/TlsChain.cs`

Заменить ВЕСЬ файл на (старая сигнатура сохранена и делегирует новой; существующие
вызовы ValkeyWorker не меняются):

```csharp
using System.Security.Cryptography.X509Certificates;

namespace Shared.Tls;

// Валидация цепочки серта против приватных per-install CA (t08: 4 копии слиты;
// сигнатура — как DockerTlsMaterial.ValidateChain: принимает X509Certificate?).
// Перегрузка anchors — CA-bundle (окно ротации: OLD+NEW одновременно, t01):
// доверие цепочке, подписанной ЛЮБЫМ якорем bundle.
public static class TlsChain
{
    public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
        => ValidateChain(certificate, [ca]);

    public static bool ValidateChain(X509Certificate? certificate, IReadOnlyList<X509Certificate2> anchors)
    {
        var cert2 = certificate as X509Certificate2
            ?? (certificate is null ? null : new X509Certificate2(certificate));
        if (cert2 is null)
            return false;
        using var chain = new X509Chain();
        // Per-install приватная CA не публикует CRL/OCSP — онлайн-проверка отзыва
        // всегда падала бы и отвергала валидные клиентские серты.
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        foreach (var anchor in anchors)
            chain.ChainPolicy.CustomTrustStore.Add(anchor);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(cert2);
    }
}
```

### Подпункт 2.2. Правка `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj`

В первом `<ItemGroup>` после строки
`<InternalsVisibleTo Include="KafkaWorker.UnitTests"/>` ничего не менять; добавить
НОВЫЙ `<ItemGroup>` перед закрывающим `</Project>`:

```xml
    <ItemGroup>
        <!-- t01: протокольный миниклиент AlterReassignments (§2.4) — валидация
             цепочки через Shared.Tls (CA-bundle окна ротации). -->
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
    </ItemGroup>
```

---

## Шаг 3. Миниклиент `KafkaProtocolClient` + тесты парсеров

- [x] выполнен (правки отладки: `new X509Certificate2(byte[])` → `X509CertificateLoader.LoadCertificate` — SYSLIB0057 при TreatWarningsAsErrors; golden client_id 12 байт)

**Вход:** шаги 1–2 завершены (кодек и `TlsChain.ValidateChain(…, anchors)` доступны).

**Действие:** подпункт 3.1 — создать `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolClient.cs`
(полный код ниже: сетевой метод + internal-парсеры, тестируемые юнитно);
подпункт 3.2 — дописать тесты парсеров в `KafkaProtocolCodecTests.cs`.

**Выход:** миниклиент компилируется; парсеры ответов пинят golden-байтами;
fail-fast apiKey 45 покрыт тестом.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --nologo` — зелёная.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build --filter "FullyQualifiedName~KafkaProtocolCodecTests"` — все зелёные (включая новые).

**Связь со spec:** §4.2 (поведение), §7.1/§7.3–§7.6/§7.8/§7.12/§7.13/§7.15,
Приложение A.4.

### Подпункт 3.1. Создать файл `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolClient.cs`

Новый файл (namespace `KafkaWorker.Provisioning.Kafka`; класс internal;
`Result` доступен через глобальный using `Shared.Core` в csproj проекта):

```csharp
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.Provisioning.Kafka;

// Протокольный миниклиент AlterPartitionReassignments (arch/16 §2.4; spec t01
// §4.2): одна подача = одно короткоживущее TCP+TLS-соединение (паттерн
// ValkeyConnection.ExecuteAsync), диалог ApiVersions v0 → SaslHandshake v1 →
// SaslAuthenticate v0 (PLAIN) → AlterPartitionReassignments v0 (KIP-455).
// Версии фиксированы (образ pinned apache/kafka:4.0.0, R1) — переговоров нет;
// ApiVersions читается ТОЛЬКО для fail-fast проверки apiKey 45.
internal sealed class KafkaProtocolClient
{
    internal const string ClientId = "kfw-reassign";

    private const short ApiVersionsKey = 18;
    private const short SaslHandshakeKey = 17;
    private const short SaslAuthenticateKey = 36;
    private const short AlterReassignmentsKey = 45;

    // Пустой батч — защитный guard (процесс такие не подаёт).
    public async Task<Result> AlterReassignmentsAsync(
        string bootstrap, string user, string password, string? caPem,
        IReadOnlyList<ReassignMove> moves, TimeSpan connectAndCommandTimeout, CancellationToken ct)
    {
        if (moves.Count == 0)
            return Result.Failed(new ApplicationException("kfw-reassign: empty reassignment batch"));

        var endpoints = ParseEndpoints(bootstrap);
        if (endpoints.Count == 0)
            return Result.Failed(new ApplicationException($"kfw-reassign: invalid bootstrap '{bootstrap}'"));

        // Failover по списку CLIENT endpoints (spec §7.6): первый полный успех
        // выигрывает; AlterPartitionReassignments форвардируется на контроллер —
        // годится любой брокер.
        var attemptErrors = new List<string>();
        foreach (var (host, port) in endpoints)
        {
            var attempt = await TryEndpointAsync(
                host, port, user, password, caPem, moves, connectAndCommandTimeout, ct);
            if (attempt.IsSuccess)
                return Result.Success();

            attemptErrors.Add(attempt.Error!.Message);
        }

        return Result.Failed(new ApplicationException(
            $"kfw-reassign: all bootstrap endpoints failed: {string.Join("; ", attemptErrors)}"));
    }

    // Одна endpoint-попытка: connect + TLS + SASL + round-trip под единым
    // бюджетом (linked CTS). Любой отказ — Result.Failed (без паники); отмена
    // host'а пробрасывается (адаптер не помечает клиента unhealthy).
    private static async Task<Result> TryEndpointAsync(
        string host, int port, string user, string password, string? caPem,
        IReadOnlyList<ReassignMove> moves, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(timeout);
            var token = timeoutCts.Token;

            using var tcp = new TcpClient();
            await tcp.ConnectAsync(host, port, token);

            // TLS: доверие — ВСЕ CERTIFICATE-блоки caPem (bundle OLD+NEW окна
            // ротации) + SAN хоста endpoint (SAN-правило arch/16 §2.3); caPem
            // null — без валидации (тесты/fake, семантика фабрики адаптеров).
            IReadOnlyList<X509Certificate2> anchors = caPem is null ? [] : ParseCertificates(caPem);
            bool Validate(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
                => caPem is null
                   || (certificate is not null
                       && Shared.Tls.TlsChain.ValidateChain(certificate, anchors)
                       && SanMatchesHost(certificate, host));

            using var ssl = new SslStream(tcp.GetStream(), false, Validate);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = host,
                RemoteCertificateValidationCallback = Validate,
            }, token);

            var correlation = 0;

            // ApiVersions v0 до аутентификации (SASL-листенер отвечает pre-auth):
            // fail-fast по поддержке apiKey 45.
            correlation++;
            var versions = new KafkaProtocolWriter();
            versions.WriteRequestHeaderV1(ApiVersionsKey, 0, correlation, ClientId);
            var versionsBody = await RoundTripAsync(
                ssl, versions.ToFrame(), headerV1: false, correlation, "api-versions", token);
            var (supported, versionsError) = ParseApiVersions(versionsBody);
            if (!supported)
                return Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} api-versions: {versionsError}"));

            // SaslHandshake v1: механизм PLAIN (задан конфигом кластера; список
            // механизмов ответа читаем, но не сверяем жёстко).
            correlation++;
            var handshake = new KafkaProtocolWriter();
            handshake.WriteRequestHeaderV1(SaslHandshakeKey, 1, correlation, ClientId);
            handshake.WriteString("PLAIN");
            var handshakeBody = await RoundTripAsync(
                ssl, handshake.ToFrame(), headerV1: false, correlation, "sasl-handshake", token);
            var handshakeReader = new KafkaProtocolReader(handshakeBody);
            var handshakeError = handshakeReader.ReadInt16();
            _ = handshakeReader.ReadArrayHeader(); // механизмы — не сверяем
            if (handshakeError != 0)
                return Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} sasl-handshake: error {handshakeError}"));

            // SaslAuthenticate v0: PLAIN-токен \0user\0password (пустой authzid).
            correlation++;
            var auth = new KafkaProtocolWriter();
            auth.WriteRequestHeaderV1(SaslAuthenticateKey, 0, correlation, ClientId);
            auth.WriteBytes([0x00, .. Encoding.UTF8.GetBytes(user), 0x00, .. Encoding.UTF8.GetBytes(password)]);
            var authBody = await RoundTripAsync(
                ssl, auth.ToFrame(), headerV1: false, correlation, "sasl-authenticate", token);
            var authReader = new KafkaProtocolReader(authBody);
            var authError = authReader.ReadInt16();
            var authMessage = authReader.ReadString(); // nullable old-style
            _ = authReader.ReadBytes(); // auth_bytes — читаем и игнорируем
            if (authError != 0)
                return Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} sasl-authenticate: {authMessage ?? $"error {authError}"}"));

            // AlterPartitionReassignments v0 (flexible, header v2). Идемпотентна
            // (KIP-455): повторная подача того же assignment замещает текущий.
            correlation++;
            var request = BuildAlterReassignmentsRequest(moves, correlation);
            var body = await RoundTripAsync(
                ssl, request, headerV1: true, correlation, "alter-reassignments", token);
            return ParseAlterReassignmentsResponse(body);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «endpoint молчит»
        }
        catch (OperationCanceledException ex)
        {
            return Result.Failed(new TimeoutException(
                $"kfw-reassign {host}:{port}: endpoint budget {timeout.TotalSeconds:F0} s exhausted", ex));
        }
        catch (System.Security.Authentication.AuthenticationException ex)
        {
            return Result.Failed(new ApplicationException(
                $"kfw-reassign {host}:{port} tls: {ex.Message}", ex));
        }
        catch (Exception ex)
        {
            return Result.Failed(new ApplicationException(
                $"kfw-reassign {host}:{port}: {ex.Message}", ex));
        }
    }

    // Записать кадр, прочитать кадр-ответ, разобрать ResponseHeader (v0 для
    // не-flexible, v1 + tagged-скип для flexible) и сверить correlation id.
    private static async Task<byte[]> RoundTripAsync(
        SslStream ssl, byte[] frame, bool headerV1, int expectedCorrelation, string stage, CancellationToken ct)
    {
        await ssl.WriteAsync(frame, ct);
        await ssl.FlushAsync(ct);
        var payload = await ReadFrameAsync(ssl, ct);
        var reader = new KafkaProtocolReader(payload);
        var correlation = headerV1 ? reader.ReadResponseHeaderV1() : reader.ReadResponseHeaderV0();
        if (correlation != expectedCorrelation)
            throw new ApplicationException(
                $"{stage}: correlation mismatch (expected {expectedCorrelation}, got {correlation})");
        return payload[(payload.Length - reader.Remaining)..];
    }

    // Кадр ответа: int32 длина + ровно length байт payload.
    private static async Task<byte[]> ReadFrameAsync(Stream stream, CancellationToken ct)
    {
        var header = new byte[4];
        await ReadExactlyAsync(stream, header, ct);
        var length = (header[0] << 24) | (header[1] << 16) | (header[2] << 8) | header[3];
        if (length < 0)
            throw new ApplicationException($"Kafka protocol: negative frame length {length}");
        var payload = new byte[length];
        await ReadExactlyAsync(stream, payload, ct);
        return payload;
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0)
                throw new ApplicationException("Kafka protocol: stream closed mid-frame");
            offset += read;
        }
    }

    // Запрос AlterPartitionReassignments v0 (flexible, header v2). Тело —
    // Приложение A.4: timeout_ms=60000 (апстрим-дефолт; таймаут обработки
    // заявки, НЕ ожидание физического переноса данных), compact-массив топиков,
    // реплики всегда не-null (отмену переезда не используем), пустые
    // tagged-секции. Группировка: OrderBy(Topic).ThenBy(Partition) —
    // детерминированный кадр (golden-тест).
    internal static byte[] BuildAlterReassignmentsRequest(IReadOnlyList<ReassignMove> moves, int correlationId)
    {
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV2(AlterReassignmentsKey, 0, correlationId, ClientId);
        w.WriteInt32(60_000); // timeout_ms
        var topics = moves
            .GroupBy(m => m.Topic, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .ToList();
        w.WriteCompactArrayHeader(topics.Count);
        foreach (var topic in topics)
        {
            w.WriteCompactString(topic.Key);
            w.WriteTaggedSection();
            var partitions = topic.OrderBy(m => m.Partition).ToList();
            w.WriteCompactArrayHeader(partitions.Count);
            foreach (var move in partitions)
            {
                w.WriteInt32(move.Partition);
                w.WriteCompactArrayHeader(move.Replicas.Count); // НЕ null — отмена не используется
                foreach (var replica in move.Replicas)
                    w.WriteInt32(replica);
                w.WriteTaggedSection();
            }
        }
        w.WriteTaggedSection();
        return w.ToFrame();
    }

    // Ответ AlterPartitionReassignments v0 (тело ПОСЛЕ ResponseHeader v1):
    // throttle_time_ms, error_code, nullable error_message, compact-массив
    // responses (nullable: 0 при top-level ошибке). Top-level ИЛИ любой
    // per-partition error ≠ 0 → единый Result.Failed со сводкой (spec §7.13).
    internal static Result ParseAlterReassignmentsResponse(byte[] body)
    {
        var r = new KafkaProtocolReader(body);
        _ = r.ReadInt32(); // throttle_time_ms
        var error = r.ReadInt16();
        var message = r.ReadCompactString();
        var failures = new List<string>();
        if (error != 0)
            failures.Add($"top-level error {error}" + (message is null ? "" : $" ({message})"));

        var topicsCount = r.ReadCompactArrayHeader();
        for (var t = 0; t < topicsCount; t++)
        {
            var topic = r.ReadCompactString();
            r.SkipTaggedSection();
            var partitionsCount = r.ReadCompactArrayHeader();
            for (var p = 0; p < partitionsCount; p++)
            {
                var partition = r.ReadInt32();
                var partitionError = r.ReadInt16();
                var partitionMessage = r.ReadCompactString();
                r.SkipTaggedSection();
                if (partitionError != 0)
                    failures.Add($"{topic}[{partition}]: {partitionError}"
                        + (partitionMessage is null ? "" : $" ({partitionMessage})"));
            }
        }

        return failures.Count == 0
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"AlterPartitionReassignments: {string.Join("; ", failures)}"));
    }

    // Ответ ApiVersions v0 (тело ПОСЛЕ ResponseHeader v0): error_code,
    // old-style массив {api_key, min, max}. Fail-fast: apiKey 45 обязан
    // поддерживать версию 0 (spec §4.2).
    internal static (bool Supported, string? Error) ParseApiVersions(byte[] body)
    {
        var r = new KafkaProtocolReader(body);
        var error = r.ReadInt16();
        if (error != 0)
            return (false, $"error {error}");
        var count = r.ReadArrayHeader();
        var hasAlter = false;
        for (var i = 0; i < count; i++)
        {
            var apiKey = r.ReadInt16();
            var min = r.ReadInt16();
            var max = r.ReadInt16();
            if (apiKey == AlterReassignmentsKey && min <= 0 && 0 <= max)
                hasAlter = true;
        }

        return hasAlter
            ? (true, null)
            : (false, "broker does not support AlterPartitionReassignments");
    }

    // Все CERTIFICATE-блоки PEM (bundle окна ротации OLD+NEW → 2 якоря).
    internal static IReadOnlyList<X509Certificate2> ParseCertificates(string caPem)
    {
        var anchors = new List<X509Certificate2>();
        var lines = caPem.Split('\n');
        var block = new List<string>();
        foreach (var raw in lines)
        {
            var line = raw.Trim();
            if (line.StartsWith("-----BEGIN CERTIFICATE-----", StringComparison.Ordinal))
            {
                block.Clear();
                block.Add(line);
            }
            else if (line.StartsWith("-----END CERTIFICATE-----", StringComparison.Ordinal) && block.Count > 0)
            {
                block.Add(line);
                anchors.Add(new X509Certificate2(
                    Encoding.UTF8.GetBytes(string.Join('\n', block))));
                block.Clear();
            }
            else if (block.Count > 0)
            {
                block.Add(line);
            }
        }

        return anchors;
    }

    // SAN-хост (arch/16 §2.3): advertised DNS либо IP; сверяем весь список SAN.
    private static bool SanMatchesHost(X509Certificate certificate, string host)
    {
        var cert = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
        using var _ = cert;
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
        if (san is null)
            return false;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return san.EnumerateIPAddresses().Contains(ip);
        return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
    }

    // bootstrap "host:port[,host:port…]" → endpoints.
    private static List<(string Host, int Port)> ParseEndpoints(string bootstrap)
    {
        var endpoints = new List<(string Host, int Port)>();
        foreach (var raw in bootstrap.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = raw.LastIndexOf(':');
            if (separator <= 0 || !int.TryParse(raw[(separator + 1)..], out var port))
                continue;
            endpoints.Add((raw[..separator], port));
        }

        return endpoints;
    }
}
```

Полный список using файла — в начале блока выше (`System.Net.Security`,
`System.Net.Sockets`, `System.Security.Cryptography.X509Certificates`,
`System.Text`, `KafkaWorker.Provisioning.Processes`); `Result` доступен через
глобальный using `Shared.Core` в csproj проекта.

### Подпункт 3.2. Дописать тесты парсеров в `src/tests/KafkaWorker.UnitTests/Kafka/KafkaProtocolCodecTests.cs`

В конец класса `KafkaProtocolCodecTests` (перед закрывающей `}`) добавить:
в начало файла — `using KafkaWorker.Core.Templates;` и
`using KafkaWorker.Provisioning.Processes;` (после существующих using); методы:

```csharp
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

        // Assert: header v2 + timeout 60000 + compact-структуры по Приложению A.4.
        frame.Should().Equal(Convert.FromHexString(string.Concat(
            "00000051",                                               // длина кадра: 81
            "002D" + "0000" + "00000004",                             // api 45, v0, corr 4
            "000B" + "6B66772D726561737369676E" + "00",               // client_id + tagged
            "0000EA60",                                               // timeout_ms 60000
            "03",                                                     // 2 топика
            "06" + "616C706861" + "00" + "03",                        // "alpha", tagged, 2 партиции
            "00000000" + "04" + "00000001" + "00000002" + "00000003" + "00", // p0 [1,2,3]
            "00000001" + "02" + "00000002" + "00",                    // p1 [2]
            "05" + "62657461" + "00" + "02",                          // "beta", tagged, 1 партиция
            "00000000" + "02" + "00000002" + "00",                    // p0 [2]
            "00")));                                                  // tagged тела
    }

    [Fact]
    public void ParseAlterReassignmentsResponse_Success()
    {
        // Arrange: throttle 0, ошибок нет; "alpha" p0 error 0, сообщения null.
        var body = Convert.FromHexString(string.Concat(
            "00000000",                          // throttle_time_ms
            "0000",                              // error_code
            "00",                                // error_message null
            "02",                                // responses: 1 топик
            "06" + "616C706861" + "00" + "02",   // "alpha", tagged, 1 партиция
            "00000000" + "0000" + "00" + "00")); // p0, error 0, msg null, tagged

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
        var body = Convert.FromHexString(string.Concat(
            "00000000",                                        // throttle
            "0023",                                            // error 35
            "10" + "6E6F20726561737369676E6D656E74",           // "no reassignment" (len+1=16)
            "00"));                                            // responses null

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
            "06" + "616C706861" + "00" + "03",               // "alpha", tagged, 2 партиции
            "00000000" + "002A" + "00" + "00",               // p0 error 42
            "00000001" + "0000" + "00" + "00"));             // p1 error 0

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
```

Примечание к `ParseAlterReassignmentsResponse_TopLevelError`: в `Topic`
compact-строка nullable — `topic` может прийти null (broker не заполняет);
`$"{topic}[{partition}]"` с null-топиком даст пустой префикс — это не падение,
для наших кейсов (per-partition ошибки) broker всегда заполняет имя. Код менять
не нужно — поведение зафиксировано тестом с не-null именем.

---

## Шаг 4. Seam: `IKafkaAdminClient.AlterReassignmentsAsync` + адаптер + фейки

- [x] выполнен

**Вход:** шаг 3 завершён; `ReassignMove` (namespace
`KafkaWorker.Provisioning.Processes`, файл
`src/KafkaWorker.Provisioning/Processes/ReassignPlanner.cs`) существует.

**Действие:** подпункты 4.1–4.4. Метод добавляется в интерфейс — синхронно
добавляются реализации: адаптер (прод) и ОБА тестовых фейка
(`FakeKafkaAdminClient` и приватный `FakeAdmin` в
`KafkaMetricsCollectorTests.cs`), иначе юнит-проект не соберётся.

**Выход:** seam-метод декларирован и реализован; сборка решения зелёная; юнит-серия
KafkaWorker зелёная.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — зелёная, без предупреждений.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build` — вся серия зелёная.
3. Закоммитить шаги 1–4 одним коммитом: `git add -A && git commit -m "t01-reassign-exec-channel: протокольный миниклиент AlterReassignments (кодек+клиент+TlsChain-bundle+seam)"`.

**Связь со spec:** §4.2 (метод интерфейса), §4.4 (реализация адаптера), §7.2, §7.8
(`NetworkSubmitTimeout`), §7.16.

### Подпункт 4.1. Правка `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs`

В начало файла (после `using KafkaWorker.Core;`) добавить
`using KafkaWorker.Provisioning.Processes;`. Перед закрывающей `}` интерфейса
`IKafkaAdminClient` (после метода `DeleteAclsAsync`) добавить:

```csharp
    // Подача reassignment-батча (KIP-455, arch/16 §2.4): план принят либо
    // Result.Failed (top-level/per-partition ошибки Kafka). Идемпотентна.
    Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct);
```

### Подпункт 4.2. Правка `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs`

В классе `KafkaAdminClient` (объявлен в этом же файле) после метода
`DeleteAclsAsync` (перед приватным `AnyAclFilter`) добавить:

```csharp
    // ===== Reassignment (t01, arch/16 §2.4): подача AlterReassignments =====

    // Per-endpoint бюджет подач (connect+TLS+SASL+round-trip): подача
    // регистрируется контроллером и отвечает быстро — это НЕ ожидание
    // физического переноса данных (реквестный timeout_ms=60000 — таймаут
    // обработки заявки). Внешний бюджет процесса — ReassignSubmitSec.
    internal static readonly TimeSpan NetworkSubmitTimeout = TimeSpan.FromSeconds(30);

    // Клиент протокола per-call: соединение короткоживущее, кэшировать нечего
    // (нативного клиента нет — churn-проблемы t05 не применимы). Каркас RunAsync
    // как у остальных операций: счётчик, unhealthy-пометка при ошибке, отмена
    // host'а не фейлит.
    public Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct)
        => RunAsync(async _ => await new KafkaProtocolClient().AlterReassignmentsAsync(
            bootstrap, user, password, caPem, moves, NetworkSubmitTimeout, ct), ct);
```

В начало файла добавить `using KafkaWorker.Provisioning.Processes;` (после
`using KafkaWorker.Core;`). Комментарий над классом (строки 9–13 файла) дополнить
не нужно — существующая XML-документация фабрики не упоминает reassign.

### Подпункт 4.3. Правка `src/tests/KafkaWorker.UnitTests/Provisioning/FakeKafkaAdminClient.cs`

В начало файла добавить `using KafkaWorker.Provisioning.Processes;` (после
`using KafkaWorker.Provisioning.Kafka;`). В класс `FakeKafkaAdminClient`:

1) рядом с полем `public List<string> DeletedTopics = [];` добавить поля:

```csharp
    // Reassignment (t01): журнал подач + переключатель отказа (юзает
    // PartitionReassignerProcessTests).
    public List<ReassignMove> SubmittedMoves = [];
    public bool FailSubmits;
```

2) перед методом `DisposeAsync` добавить реализацию seam-метода:

```csharp
    public Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct)
    {
        SubmittedMoves.AddRange(moves);
        return Task.FromResult(FailSubmits
            ? Result.Failed(new ApplicationException("kafka: submit failed"))
            : Result.Success());
    }
```

### Подпункт 4.4. Правка `src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs`

В приватный класс `FakeAdmin : IKafkaAdminClient` добавить заглушку нового
метода интерфейса (рядом с остальными заглушками, например после
`AlterBrokerConfigsAsync`) и using `using KafkaWorker.Provisioning.Processes;`
в начало файла:

```csharp
        // Reassignment (t01): коллектор подач не делает — заглушка интерфейса.
        public Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct)
            => Task.FromResult(Result.Success());
```

---

## Шаг 5. Прод-код: опции rename + процесс на новый submit + Program.cs

**Вход:** шаг 4 завершён, seam существует.

**Действие:** подпункты 5.1–5.5. Все 6 call-site'ов конструктора процесса
закрыты планом (обновлённый spec §4.5.1/Прил. B): `Program.cs` — шаг 5.4;
`PartitionReassignerProcessTests` (два места) — шаг 6.1; `ReassignmentTests`,
`RotationTimeoutTests`, `KafkaActiveGateTests` — шаги 7.1–7.3;
`KafkaClusterFixture` конструкторов процесса не содержит и не правится.
ВНИМАНИЕ: после этого шага тестовые проекты (юнит-риг и интеграционные риги)
ещё НЕ компилируются — их правят шаги 6–7. Проверка шага 5 — точечная сборка
прод-проектов (не всего решения).

**Выход:** `KafkaWorker.App` собирается: процесс без `driver`, подача через
`admin.AlterReassignmentsAsync`, опция `ReassignSubmitSec` сквозная
(appsettings → Options → Program → ReassignOptions.SubmitSec).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/KafkaWorker.App/KafkaWorker.App.csproj -c Release --nologo` — зелёная, без предупреждений.

**Связь со spec:** §4.5, §4.6, §7.7, §7.8, §7.11.

### Подпункт 5.1. Правка `src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs`

Заменить XML-док и запись `ReassignOptions` (строки 33–46) на:

```csharp
/// <summary>
/// Параметры процесса reassign I (arch/16 §8): интервал тиков, размер батча
/// подач, бюджет reassign-подачи батча (§2.4) и окно дедупа переподачи одного
/// батча. RotationTicketTimeoutSec — возраст не-начатой заявки rebalances до снятия (t10).
/// </summary>
public sealed record ReassignOptions(
    int IntervalSec,
    int BatchPartitions,
    int SubmitSec,
    int RetrySubmitSec,
    int RotationTicketTimeoutSec = 3600)
{
    public static ReassignOptions Default { get; } = new(15, 10, 60, 120, 3600);
}
```

### Подпункт 5.2. Правка `src/KafkaWorker.App/Options.cs`

В классе `ThresholdsOptions` заменить свойство `ReassignExecSec` (строки 167–168):

```csharp
    /// <summary>Бюджет reassign-подачи батча (§2.4): connect+TLS+SASL+
    /// AlterReassignments round-trip по всем endpoint-попыткам.</summary>
    public int ReassignSubmitSec { get; set; } = 60;
```

### Подпункт 5.3. Правка `src/KafkaWorker.App/appsettings.json`

В секции `"Thresholds"` заменить строку `"ReassignExecSec": 180,` на:

```json
      "ReassignSubmitSec": 60,
```

### Подпункт 5.4. Правка `src/KafkaWorker.App/Program.cs`

В регистрации `PartitionReassignerProcess` (строки 292–307): удалить строку
`sp.GetRequiredService<IClusterDriver>(),` (аргумент после
`opts.Etcd.Endpoints,`) и заменить `opts.Thresholds.ReassignExecSec` на
`opts.Thresholds.ReassignSubmitSec`. Итоговый блок:

```csharp
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value;
    return new PartitionReassignerProcess(
        sp.GetRequiredService<IEtcdGateway>(),
        opts.Etcd.Endpoints,
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<IKafkaAdminClientFactory>(),
        new ReassignOptions(
            opts.Loops.ReassignIntervalSec, opts.Loops.ReassignBatchPartitions,
            opts.Thresholds.ReassignSubmitSec, opts.Thresholds.ReassignRetrySubmitSec,
            opts.Thresholds.RotationTicketTimeoutSec),
        sp.GetRequiredService<TimeProvider>());
});
```

### Подпункт 5.5. Правка `src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`

Пять точечных правок:

1) Удалить строку `using KafkaWorker.Docker.Drivers;` (строка 6) — после удаления
параметра `driver` типы этого namespace в файле не используются
(`BrokerEnvBuilder` — свой namespace `KafkaWorker.Provisioning.Processes`).

2) XML-док класса (строки 30–33): фразу

```
/// describe-all (включая __-топики), подаются идемпотентные батчи ≤
/// BatchPartitions через kafka-reassign-partitions CLI docker-exec'ом в
/// контейнер брокера; завершение по факту (drain-брокер вне Replicas и нет
/// USR).
```

заменить на

```
/// describe-all (включая __-топики), подаются идемпотентные батчи ≤
/// BatchPartitions операцией AlterReassignments (KIP-455) протокольным
/// миниклиентом (arch/16 §2.4); завершение по факту (drain-брокер вне
/// Replicas и нет USR).
```

3) Конструктор (строки 45–53): удалить параметр `IClusterDriver driver,` (третья
строка списка параметров). Остальные параметры не трогать.

4) Передача адаптера в сценарии: `RunAsync` уже создаёт
`await using var admin = adminFactory.Create(...)` (строка 131) — объём его
времени жизни охватывает вызовы сценариев. Изменить сигнатуры и вызовы:
- вызов `RunDrainAsync(snap, all, drainCandidate.Name, previous, now, ct)` → `RunDrainAsync(snap, admin, all, drainCandidate.Name, previous, now, ct)`;
- объявление `RunDrainAsync(KafkaClusterSnapshot snap, IReadOnlyList<KafkaTopicView> all, string drainBroker, ...)` → добавить вторым параметром `IKafkaAdminClient admin,`;
- вызов `RunBalanceAsync(snap, all, previous, now, ct)` → `RunBalanceAsync(snap, admin, all, previous, now, ct)`;
- объявление `RunBalanceAsync` — добавить вторым параметром `IKafkaAdminClient admin,`;
- внутри `RunDrainAsync`/`RunBalanceAsync` вызов `SubmitBatchAsync(snap, all, plan.Value, "drain", ...)` / `SubmitBatchAsync(snap, all, plan, "balance", ...)` → добавить `admin,` вторым аргументом;
- объявление `SubmitBatchAsync(KafkaClusterSnapshot snap, IReadOnlyList<KafkaTopicView> all, ...)` → добавить вторым параметром `IKafkaAdminClient admin,`.

5) Тело `SubmitBatchAsync`: блок от комментария `// Bootstrap: INTERNAL-имена живых брокеров` до строки
`return Fail(cluster, exec.Error!, "submitting-batch"); // следующий тик переподает`
ВКЛЮЧИТЕЛЬНО (строки 396–431) заменить на:

```csharp
        // Подача AlterReassignments (KIP-455, arch/16 §2.4) протокольным
        // миниклиентом по CLIENT endpoints — контейнер-цель не нужна; бюджет
        // SubmitSec покрывает все endpoint-попытки клиента (per-endpoint —
        // внутренние 30 с NetworkSubmitTimeout адаптера).
        using var submitCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        submitCts.CancelAfter(TimeSpan.FromSeconds(options.SubmitSec));
        var submit = await admin.AlterReassignmentsAsync(batch, submitCts.Token);
        if (!submit.IsSuccess)
            return Fail(cluster, submit.Error!, "submitting-batch"); // следующий тик переподает
```

Комментарий метода `// D5–D6: батч Pending-партиций + дедуп переподачи + exec + прогресс.`
заменить на `// D5–D6: батч Pending-партиций + дедуп переподачи + подача + прогресс.`

---

## Шаг 6. Юнит-риг и тесты процесса на новый канал

**Вход:** шаг 5 завершён (прод-код собирается).

**Действие:** подпункты 6.1–6.3 — правки
`src/tests/KafkaWorker.UnitTests/Provisioning/PartitionReassignerProcessTests.cs`.
Интеграционные риги правятся шагом 7 — сборка всего решения вернётся там.

**Выход:** юнит-тесты процесса компилируются и зелёные на новом канале
наблюдения `SubmittedMoves`; exec-механических тестов нет.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --nologo` — зелёная.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build --filter "FullyQualifiedName~PartitionReassignerProcessTests"` — все зелёные.

**Связь со spec:** §4.8 (юнит-тесты процесса), §7.11.

### Подпункт 6.1. Риг `NewRig` и `Claim_Not_Ours`

В `NewRig` (строки 55–71):
1) удалить строки:
```csharp
        var driver = new Fakes.FakeKafkaDriver();
        driver.NodeObjects.AddRange(Enumerable.Range(1, 4).Select(k => $"kfw-events-broker{k}"));
```
2) конструктор процесса — убрать аргумент `driver` и заменить `ExecSec: 180` на `SubmitSec: 60`:
```csharp
        var process = new PartitionReassignerProcess(
            etcd, [Ep], claims, journal, new FakeAdminFactory(admin),
            new ReassignOptions(IntervalSec: 0, BatchPartitions: 10, SubmitSec: 60, RetrySubmitSec: retrySubmitSec),
            time);
```
3) из record `Rig` удалить поле `Fakes.FakeKafkaDriver Driver,` и из `return new Rig(...)` — аргумент `driver`.

В `Claim_Not_Ours` (строки 353–355): из конструктора процесса убрать аргумент
`new Fakes.FakeKafkaDriver()`:
```csharp
        var process = new PartitionReassignerProcess(
            etcd, [Ep], claims, new WorkJournal("/kafkaworker", etcd, [Ep]),
            new FakeAdminFactory(new FakeKafkaAdminClient()), ReassignOptions.Default, new FixedTimeProvider());
```

### Подпункт 6.2. Замена exec-ассертов на `SubmittedMoves` (по тестам)

Каждый тест — точечная правка Assert-части (Arrange/Act не трогать, кроме
оговорённого):

1. `Drain_Submits_Batch_And_Writes_Progress`: комментарий Arrange «…exec успешен.»
   → «…подача успешна.»; заменить
```csharp
        var exec = Assert.Single(rig.Driver.Execs);
        string.Join(' ', exec.Cmd).Should().Contain("--execute");
```
на
```csharp
        // Обе партиции уезжают с broker4: батч ≤ BatchPartitions, у каждого
        // мува 3 реплики без drain-брокера.
        var moves = rig.Admin.SubmittedMoves;
        moves.Should().HaveCount(2);
        moves.Should().OnlyContain(m => m.Topic == "orders"
            && !m.Replicas.Contains(4) && m.Replicas.Count == 3);
```
2. `Drain_Completion_Clears_Progress`: `rig.Driver.Execs.Should().BeEmpty();` → `rig.Admin.SubmittedMoves.Should().BeEmpty();`.
3. `Drain_MinIsr_Failure`: то же, что п.2.
4. `Drain_USR_Holds_Completion`: то же, что п.2.
5. `Drain_Resubmit_Dedup`: `rig.Driver.Execs.Should().HaveCount(1);` → `rig.Admin.SubmittedMoves.Should().HaveCount(2);` (первый батч — 2 мува) и `rig.Driver.Execs.Should().HaveCount(2);` → `rig.Admin.SubmittedMoves.Should().HaveCount(4);` (два полных батча).
6. Тест `Drain_Exec_To_Drain_Container_Failed_Resubmit_Via_RUNNING` (строки 195–223) — УДАЛИТЬ ЦЕЛИКОМ (механизм exec-fallback удалён, spec §4.8).
7. `Drain_Converges_Without_Drain_Broker_Container`: заменить тест ЦЕЛИКОМ на
(механика exec-цели удалена, сценарий сохранён: подача → факт двинулся → done):

```csharp
    [Fact]
    public async Task Drain_Converges_Without_Drain_Broker_Container()
    {
        // Arrange: broker4 TO_REMOVE с репликами; поданный батч Kafka
        // применяет между тиками.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];

        // Act: тик 1 подаёт батч; факт «доиграл» поданный батч (реплик broker4
        // нет, ISR полный) — тик 2 завершает drain по факту.
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2], [2, 1]], [[1, 2], [2, 1]])];
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: drain завершён по факту — прогресс удалён, journal done;
        // подача ровно одна (тик 2 без подач) — канал сетевой, цели-контейнера нет.
        result.IsSuccess.Should().BeTrue();
        rig.Admin.SubmittedMoves.Should().HaveCount(2);
        rig.Etcd.Store.Keys.Should().NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Op.Should().Be("reassign");
        state.Value!.Phase.Should().Be("done");
    }
```
8. `Balance_Executes_And_Retires_Ticket`: `rig.Driver.Execs.Should().HaveCount(1);` → `rig.Admin.SubmittedMoves.Should().HaveCount(2);` (добор обеих партиций до RF=3).
9. `Balance_Waits_For_Drain`: заменить
```csharp
        var exec = Assert.Single(rig.Driver.Execs);
        exec.Node.Should().Be("broker4");
```
на
```csharp
        rig.Admin.SubmittedMoves.Should().NotBeEmpty();
        rig.Admin.SubmittedMoves.Should().OnlyContain(m => !m.Replicas.Contains(4));
```
10. `Balance_Ticket_Cancellation`: `rig.Driver.Execs.Should().BeEmpty();` → `rig.Admin.SubmittedMoves.Should().BeEmpty();`.
11. `Blind_Probe`: то же, что п.10.
12. `Run_LiveBalanceProgress_OldTicket_NotExpired`: `rig.Driver.Execs.Should().HaveCount(1);` → `rig.Admin.SubmittedMoves.Should().HaveCount(2);`.

Комментарий шапки файла (строки 11–13) не меняется (docker-exec там не упоминается).

### Подпункт 6.3. Новый тест отказа подачи

В конец класса (после `Run_BalanceConverged_WritesDoneOutcome`) добавить:

```csharp
    [Fact]
    public async Task Submit_Fails_TickFails_ProgressNotWritten_NextTickResubmits()
    {
        // Arrange: broker4 TO_REMOVE с репликами; подача падает (FailSubmits).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];
        rig.Admin.FailSubmits = true;

        // Act: тик с падающей подачей.
        var failed = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик Failed с фазой submitting-batch, прогресс-ключ НЕ писан;
        // вызов подачи зафиксирован.
        failed.IsSuccess.Should().BeFalse();
        failed.Error!.Message.Should().Contain("kafka: submit failed");
        rig.Admin.SubmittedMoves.Should().HaveCount(2);
        rig.Etcd.Store.Keys.Should().NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Op.Should().Be("reassign");
        state.Value.Phase.Should().Be("submitting-batch");

        // Act 2: сеть починилась — следующий тик переподаёт и пишет прогресс.
        rig.Admin.FailSubmits = false;
        var ok = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert
        ok.IsSuccess.Should().BeTrue();
        rig.Admin.SubmittedMoves.Should().HaveCount(4);
        rig.Etcd.Store.Keys.Should().Contain(ProgressKey);
        ReadProgress(rig.Etcd).Mode.Should().Be("drain");
    }
```

---

## Шаг 7. Интеграционные риги на новый конструктор + идемпотентность через seam

**Вход:** шаг 6 завершён.

**Действие:** подпункты 7.1–7.4. Правки чисто компилятивные + замена Act 2 в
`Reassign_ResubmissionIsSafe`; контент остальных сценариев (ассерты, бюджеты,
teardown) не меняется.

**Выход:** сборка ВСЕГО решения снова зелёная; интеграционные риги без `driver`,
`SubmitSec: 60`; идемпотентность подаётся двойным seam-вызовом.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — зелёная, без предупреждений.

**Связь со spec:** §4.8 (интеграционные), §7.11.

### Подпункт 7.1. Правка `src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs`

1) Риг `NewRigAsync` (строки 62–65): из конструктора `PartitionReassignerProcess`
удалить аргумент `fixture.Driver,` и заменить `ExecSec: 180` на `SubmitSec: 60`:
```csharp
            new PartitionReassignerProcess(
                fixture.Gateway, [fixture.Endpoint], claims, journal,
                fixture.AdminFactory, new ReassignOptions(IntervalSec: 0, BatchPartitions: 10, SubmitSec: 60, RetrySubmitSec: 120),
                TimeProvider.System),
```
2) Тест `Reassign_ResubmissionIsSafe`, Act 2 (строки 350–366): заменить блок
```csharp
            // Act 2: повторная CLI-подача того же assignment (факт == план) —
            // Kafka обязана принять идемпотентно (KIP-455).
            var before = await DescribeAllAsync(creds);
            var moves = before
                .SelectMany(t => t.ReplicasPerPartition.Select((_, p) => new ReassignMove(t.Topic, p, [.. t.ReplicasPerPartition[p]])))
                .ToList();
            var bootstrap = ReassignCli.Bootstrap(["broker1", "broker2", "broker3"]);
            // CLI-канон t03: SASL_SSL command-config с admin-кредами и per-cluster
            // CA из etcd (литералы "adminpw"/"CAPEM" — легаси PLAINTEXT-эпохи).
            var adminUser = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_user");
            var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
            var cmd = ReassignCli.BuildExecCommand(moves, bootstrap, adminUser!, adminPassword!, creds.CaPem);
            for (var i = 0; i < 2; i++)
            {
                var exec = await fixture.Driver.ExecNodeAsync(cluster, "broker1", cmd, ct);
                exec.IsSuccess.Should().BeTrue($"resubmission #{i + 1} must not fail: {exec.Error?.Message}");
            }
```
на
```csharp
            // Act 2: двойная подача того же assignment seam-методом (факт ==
            // план) — Kafka обязана принять идемпотентно (KIP-455, §2.4).
            var before = await DescribeAllAsync(creds);
            var moves = before
                .SelectMany(t => t.ReplicasPerPartition.Select((_, p) => new ReassignMove(t.Topic, p, [.. t.ReplicasPerPartition[p]])))
                .ToList();
            var adminUser = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_user");
            var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
            await using var admin = fixture.AdminFactory.Create(
                creds.Bootstrap, adminUser!, adminPassword!, creds.CaPem);
            for (var i = 0; i < 2; i++)
            {
                var submit = await admin.AlterReassignmentsAsync(moves, ct);
                submit.IsSuccess.Should().BeTrue($"resubmission #{i + 1} must not fail: {submit.Error?.Message}");
            }
```
Assert-часть теста (describe до/после идентичен) — без изменений.

### Подпункт 7.2. Правка `src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs`

1) Риг (строки 77–81): из конструктора `PartitionReassignerProcess` удалить
аргумент `fixture.Driver,`; `ExecSec: 180` → `SubmitSec: 60`:
```csharp
            new PartitionReassignerProcess(
                fixture.Gateway, [fixture.Endpoint], claims, journal,
                fixture.AdminFactory,
                new ReassignOptions(IntervalSec: 0, BatchPartitions: ReassignBatchPartitions, SubmitSec: 60, RetrySubmitSec: 120),
                TimeProvider.System));
```
2) Комментарий строки ~208 «≤ExecSec=180 exec» → «≤SubmitSec=60 подачи»; комментарий
строки ~705 «Backstop 600 с = 3 × ExecSec (540 с на три подряд …)» → «Backstop
600 с = 3 × SubmitSec (180 с на три подряд …)». Только текст комментариев, код не трогать.

### Подпункт 7.3. Правка `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaActiveGateTests.cs`

Строки 61–62: убрать аргумент `fixture.Driver,` и заменить позиционное значение
бюджета 180 → 60 (третий аргумент `ReassignOptions`):
```csharp
            new PartitionReassignerProcess(fixture.Gateway, ep, claims, journal,
                factory, new ReassignOptions(15, 10, 60, 120), TimeProvider.System),
```

### Подпункт 7.4. Правка `src/tests/KafkaWorker.IntegrationTests/Kafka/PortAllocHealingTests.cs`

Удалить строки 66–67 (проверка «контейнер жив» через exec; живость уже покрыта
соседними ассертами `ListNodeObjectsAsync` выше и `admin2.DescribeClusterAsync`
ниже):

```csharp
        (await fixture.Driver.ExecNodeAsync(cluster, "broker1", ["true"], ct))
            .IsSuccess.Should().BeTrue("container alive (was not recreated)");
```

Комментарий Assert 1 (строка 58) «…и отвечает на exec (пересоздания нет)» →
«…и кластер отвечает (пересоздания нет)».

---

## Шаг 8. Чистка exec-механики из KafkaWorker

**Вход:** шаг 7 завершён, решение собирается.

**Действие:** подпункты 8.1–8.4. `Shared.Docker` и PgWorker НЕ трогаются
(отклонение №23 — отдельная задача t03; spec §7.10).

**Выход:** `ReassignCli`, `ExecNodeAsync` (интерфейс + обе реализации + фейк),
exec-хелперы — удалены; критерии приёмки §8.1 выполняются.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — зелёная.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build` — зелёная.
3. Grep-критерии (из корня worktree, вывод ПУСТ):
   - `grep -rn "ExecNodeAsync" src/KafkaWorker.* src/tests/KafkaWorker.*`
   - `grep -rn "ReassignCli\|kafka-reassign-partitions" src/KafkaWorker.* src/tests/KafkaWorker.*`
4. Закоммитить шаги 5–8: `git add -A && git commit -m "t01-reassign-exec-channel: процесс на AlterReassignments-подачу, ReassignSubmitSec, exec-механика удалена"`.

**Связь со spec:** §4.7, §7.9, §7.10, §8.1.

### Подпункт 8.1. Удалить файлы

- `src/KafkaWorker.Provisioning/Processes/ReassignCli.cs` — удалить целиком
  (`git rm src/KafkaWorker.Provisioning/Processes/ReassignCli.cs`).
- `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs` — удалить
  целиком (`git rm src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs`).

### Подпункт 8.2. Правка `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs`

1) Из интерфейса `IClusterDriver` удалить объявление (строки 69–73):

```csharp
    // Выполнить команду в контейнере живого брокера (arch/16 §2.4; t02:
    // kafka-reassign-partitions CLI): plain — running-контейнер по имени
    // kfw-<C>-<b> перебором хостов; swarm — running-таск сервиса →
    // ContainerId. stdout → строка; exit != 0 → Failed (движок).
    Task<Result<string>> ExecNodeAsync(string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct);
```

2) Из `PlainClusterDriver` удалить метод `ExecNodeAsync` (строки 362–389) вместе
с комментарием «// Контейнер брокера по имени kfw-<C>-<b>: перебор хостов (аналог
Remove), на первом, где найден running-контейнер — exec (порт PgWorker t01).».

3) Из `SwarmClusterDriver` удалить метод `ExecNodeAsync` (строки 508–530) вместе
с комментарием «// Контейнер брокера — running-таск сервиса (ContainerID уже в
ответе /tasks; порт PgWorker t01).».

### Подпункт 8.3. Правка `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`

Из `FakeKafkaDriver` удалить блок (строки 332–341):

```csharp
        // Записывающий мок exec: команды видят тесты; опциональный хук
        // симулирует Kafka (применение поданного reassignment тестом).
        public readonly List<(string Node, IReadOnlyList<string> Cmd)> Execs = [];
        public Func<string, IReadOnlyList<string>, Result<string>>? ExecHandler { get; set; }

        public Task<Result<string>> ExecNodeAsync(string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct)
        {
            lock (_gate) { Execs.Add((nodeName, cmd)); }
            return Task.FromResult(ExecHandler is { } h ? h(nodeName, cmd) : Result<string>.Success(""));
        }
```

### Подпункт 8.4. Контроль чистоты using

После удалений проверить сборку: если компилятор/анализатор сообщит о
неиспользуемых using в правимых файлах — убрать только их. Ожидаемые кандидаты
отсутствуют (в `ClusterDriver.cs` и `Fakes.cs` остальные члены используют те же
namespace), но проверка обязательна: `TreatWarningsAsErrors`.

---

## Шаг 9. Прогоны и приёмка (сборка → юниты → docker-интеграции → зачистка)

**Вход:** шаг 8 завершён и закоммичен.

**Действие:** прогон серий строго в порядке ниже; после КАЖДОЙ docker-серии —
зачистка контуров и проверка остатков. Обязательный минимум — серии 1–4 и 8;
smoke-серии 5–7 гоняют файлы, чьи риги/код правились шагами 7.1–7.4 (spec §5.4
«smoke соседних Kafka-серий фикстуры при правках рига»). Перезапуск упавших
тестов «для выяснения» запрещён (AGENTS.md): упавшая серия — разбор по логам,
фикс, повтор только после анализа.

**Выход:** все серии зелёные; grep-критерии §8.1 пустые; docker-хост чист от
`kfw-*`.

**Проверка (последовательность команд, каждая — завершена зелёным перед
следующей; серия идёт дольше 5 минут — онлайн-разбор лога по правилам
AGENTS.base §13, не ожидание):**

1. Сборка решения (отдельной командой, свежесть бинаря видна её выводом):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
2. Юнит-серия KafkaWorker:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Release --no-build`
   (после старта — убедиться по первым строкам лога, что прогон пошёл).
3. Docker-интеграции reassignment (фикстура поднимает свой etcd-контейнер и
   kfw-кластеры; E2ePrepare НЕ нужен — это не PgWorker):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj -c Release --no-build --filter "FullyQualifiedName~ReassignmentTests"`
   Ожидание: 3/3 зелёные (Drain_RemovesNonEmptyBroker_ReducesRF,
   Reassign_ResubmissionIsSafe, Balance_RestoresRF_AfterRepeatedAdd).
4. Зачистка после КАЖДОЙ docker-серии (3, затем 5–7):
   `docker ps -a --filter "name=kfw-" --format "{{.Names}}"` и
   `docker network ls --filter "name=kfw-" --format "{{.Name}}"` — вывод пуст
   (фикстура демонтирует контейнеры/сети в DisposeAsync); остатки — разобрать
   адресно (`docker rm -f <name>` / `docker network rm <name>`), разобраться по
   логу, почему teardown не зачистил.
5. Smoke правки PortAllocHealingTests (файл менялся — шаг 7.4):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj -c Release --no-build --filter "FullyQualifiedName~PortAllocHealingTests"`
   Затем зачистка по п.4.
6. Smoke правки RotationTimeoutTests (риг без driver + бюджет SubmitSec —
   шаг 7.2): 
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj -c Release --no-build --filter "FullyQualifiedName~RotationTimeoutTests"`
   Затем зачистка по п.4.
7. Smoke правки KafkaActiveGateTests (риг без driver — шаг 7.3):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj -c Release --no-build --filter "FullyQualifiedName~KafkaActiveGateTests"`
   Затем зачистка по п.4.
8. Финальные grep-критерии spec §8.1 (из корня worktree) — вывод пуст:
   `grep -rn "ExecNodeAsync" src/KafkaWorker.* src/tests/KafkaWorker.*`
   `grep -rn "ReassignCli\|kafka-reassign-partitions" src/KafkaWorker.* src/tests/KafkaWorker.*`
9. Если всё зелёное — отметить чекбоксы шагов в plan.md; итоговый коммит не
   требуется (шаг 8 уже закоммичен; шаг 9 код не меняет).

**Связь со spec:** §5.4 (включая smoke соседних серий при правках рига), §8.1–§8.4
(критерии приёмки 1–4), §8.5 (инварианты T7.1/баланс/идемпотентность —
закрываются серией 3 и smoke-сериями 5–7 без изменений ассертов).

---

## Самопроверка покрытия spec (для исполнителя перед началом)

- §1 (цель: exec исчезает, подача протоколом, прогресс-ключ не меняется) — шаги 3–8.
- §4.1 кодек — шаг 1; §4.2 клиент — шаг 3; §4.3 TlsChain — шаг 2; §4.4 seam — шаг 4;
  §4.5 процесс — шаг 5; §4.6 опции — шаг 5; §4.7 удаления — шаг 8; §4.8 тесты —
  шаги 1, 3, 6, 7; §4.9 «не меняется» — глобальные ограничения.
- §5 фазы 1–4 = шаги 1–4 / 5–7 / 8 / 9.
- §7: все 17 решений использованы (см. ссылки в шагах); §8 критерии 1–4 — шаг 9,
  критерий 5 — серии без изменений ассертов, критерий 6 — arch уже в ветке
  (коммит 61aa3b16), roadmap-пункт — мерж-гейт (вне плана).
