using System.Security.Cryptography.X509Certificates;

namespace PgWorker.Provisioning.Probes;

/// <summary>Верификация серверного серта агента PgNode в mTLS-канале (spec
/// §3.4): цепочка к per-install REST-CA (Shared.Tls) И SAN-пиннинг полного
/// docker-имени ноды (SslStream.TargetHostName = host URL; мини-разбор DER
/// SAN, тег 0x82 dNSName — образец PatroniRoleProbe.SanContainsLoopback,
/// arch/13 §4). false — сертификат/цепочка/SAN не прошли (транспорт-фейл
/// тика, transient).</summary>
public static class NodeChannelSsl
{
    public static bool Validate(object? sender, X509Certificate? certificate, X509Certificate2 ca)
        => sender is System.Net.Security.SslStream ssl
            && certificate is { } raw
            && Shared.Tls.TlsChain.ValidateChain(raw, ca)
            && SanContainsDnsName(new X509Certificate2(raw), ssl.TargetHostName);

    // SAN-пиннинг: TargetHostName (полное docker-имя ноды) среди dNSName-
    // имён расширения subjectAltName (OID 2.5.29.17): SEQUENCE { GeneralName* },
    // [2] IMPLICIT IA5String (тег 0x82) — dNSName (мини-разбор DER по образцу
    // PatroniRoleProbe.SanContainsLoopback).
    private static bool SanContainsDnsName(X509Certificate2 cert, string expected)
    {
        if (expected is not { Length: > 0 })
            return false;
        foreach (var ext in cert.Extensions)
        {
            if (ext.Oid?.Value != "2.5.29.17")
                continue;
            var data = ext.RawData;
            var i = 0;
            if (data.Length < 2 || data[i] != 0x30)
                continue;
            i++;
            i += DerLengthSize(data, i);
            while (i + 1 < data.Length)
            {
                var tag = data[i++];
                var size = DerLength(data, ref i);
                if (i + size > data.Length)
                    break;
                if (tag == 0x82 // dNSName — точное совпадение имени ноды
                    && size == expected.Length
                    && System.Text.Encoding.ASCII.GetString(data, i, size) == expected)
                {
                    return true;
                }
                i += size;
            }
        }
        return false;
    }

    // DER-длина: возвращает значение и сдвигает индекс; длинная форма —
    // следующий байт задаёт число байтов длины.
    private static int DerLength(byte[] data, ref int i)
    {
        var first = data[i++];
        if ((first & 0x80) == 0)
            return first;
        var count = first & 0x7F;
        var value = 0;
        for (var k = 0; k < count && i < data.Length; k++)
            value = value << 8 | data[i++];
        return value;
    }

    // Размер (в байтах) поля DER-длины, начинающегося в i (для пропуска
    // заголовка SEQUENCE).
    private static int DerLengthSize(byte[] data, int i)
    {
        var first = data[i];
        if ((first & 0x80) == 0)
            return 0;
        return first & 0x7F;
    }
}
