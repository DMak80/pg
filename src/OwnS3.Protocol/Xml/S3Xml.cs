using System.Text;
using System.Xml;
using System.Xml.Serialization;
using OwnS3.Protocol.Errors;

namespace OwnS3.Protocol.Xml;

// Единый XML-хелпер ownS3 (arch/owns3/03 §4): XmlSerializer, UTF-8, namespace
// S3XmlNamespace.Value; безопасный writer — недопустимые для XML 1.0 кодпоинты
// в значениях пишутся числовыми ссылками &#x<hex>;; нераспарсиваемый запросный
// XML — S3ProtocolException(MalformedXML).
public static class S3Xml
{
    public static string Serialize<T>(T value) where T : class
    {
        var serializer = new XmlSerializer(typeof(T));
        // Пустые префиксы с нашим namespace: без явных namespaces XmlSerializer
        // добавляет на корень xmlns:xsi/xmlns:xsd.
        var namespaces = new XmlSerializerNamespaces();
        namespaces.Add(string.Empty, S3XmlNamespace.Value);
        using var stream = new MemoryStream();
        using (var writer = new SafeXmlWriter(XmlWriter.Create(stream, new XmlWriterSettings
        {
            Indent = false,
            Encoding = new UTF8Encoding(false),
            OmitXmlDeclaration = false,
        })))
        {
            serializer.Serialize(writer, value, namespaces);
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    public static T Deserialize<T>(string xml) where T : class
    {
        try
        {
            using var reader = new StringReader(xml);
            var serializer = new XmlSerializer(typeof(T));
            return (T)(serializer.Deserialize(reader) ?? throw new S3ProtocolException(
                S3ErrorCode.MalformedXML, "The XML you provided was not well-formed or did not validate against our published schema."));
        }
        catch (Exception ex) when (ex is InvalidOperationException or XmlException or S3ProtocolException)
        {
            throw new S3ProtocolException(S3ErrorCode.MalformedXML,
                "The XML you provided was not well-formed or did not validate against our published schema.");
        }
    }

    // Делегирующий XmlWriter: WriteString пропускает допустимые кодпоинты XML 1.0
    // (#x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | пары суррогатов),
    // прочие — числовой ссылкой &#x<hex>; через WriteRaw.
    private sealed class SafeXmlWriter(XmlWriter inner) : XmlWriter
    {
        public override WriteState WriteState => inner.WriteState;
        public override string? XmlLang => inner.XmlLang;
        public override XmlSpace XmlSpace => inner.XmlSpace;

        public override void WriteString(string? text)
        {
            if (text is null)
            {
                inner.WriteString(text);
                return;
            }
            var plain = new StringBuilder(text.Length);
            for (var i = 0; i < text.Length; i++)
            {
                var c = text[i];
                if (IsAllowedWithSurrogate(text, i))
                {
                    plain.Append(c);
                    if (char.IsHighSurrogate(c))
                        plain.Append(text[++i]);   // валидная пара пишется целиком
                }
                else
                {
                    if (plain.Length > 0)
                    {
                        inner.WriteString(plain.ToString());
                        plain.Clear();
                    }
                    inner.WriteRaw($"&#x{(int)c:X};");
                }
            }
            if (plain.Length > 0)
                inner.WriteString(plain.ToString());
        }

        private static bool IsAllowedWithSurrogate(string text, int i)
        {
            var c = text[i];
            if (char.IsHighSurrogate(c))
                return i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]);
            if (char.IsLowSurrogate(c))
                return false;   // одиночный низкий суррогат — недопустим
            return IsXmlChar(c);
        }

        private static bool IsXmlChar(char c) => c is '\t' or '\n' or '\r'
            || (c >= '\u0020' && c <= '\uD7FF')
            || (c >= '\uE000' && c <= '\uFFFD');

        // Остальные члены — проброс во внутренний writer.
        public override void Flush() => inner.Flush();
        public override string? LookupPrefix(string ns) => inner.LookupPrefix(ns);
        public override void WriteBase64(byte[] buffer, int index, int count) => inner.WriteBase64(buffer, index, count);
        public override void WriteCData(string? text) => inner.WriteCData(text);
        public override void WriteCharEntity(char ch) => inner.WriteCharEntity(ch);
        public override void WriteChars(char[] buffer, int index, int count) => inner.WriteChars(buffer, index, count);
        public override void WriteComment(string? text) => inner.WriteComment(text);
        public override void WriteDocType(string name, string? pubid, string? sysid, string? subset) => inner.WriteDocType(name, pubid, sysid, subset);
        public override void WriteEndAttribute() => inner.WriteEndAttribute();
        public override void WriteEndDocument() => inner.WriteEndDocument();
        public override void WriteEndElement() => inner.WriteEndElement();
        public override void WriteEntityRef(string name) => inner.WriteEntityRef(name);
        public override void WriteFullEndElement() => inner.WriteFullEndElement();
        public override void WriteProcessingInstruction(string name, string? text) => inner.WriteProcessingInstruction(name, text);
        public override void WriteRaw(string? data) => inner.WriteRaw(data!);
        public override void WriteRaw(char[] buffer, int index, int count) => inner.WriteRaw(buffer, index, count);
        public override void WriteStartAttribute(string? prefix, string localName, string? ns) => inner.WriteStartAttribute(prefix, localName, ns);
        public override void WriteStartDocument() => inner.WriteStartDocument();
        public override void WriteStartDocument(bool standalone) => inner.WriteStartDocument(standalone);
        public override void WriteStartElement(string? prefix, string localName, string? ns) => inner.WriteStartElement(prefix, localName, ns);
        public override void WriteSurrogateCharEntity(char lowChar, char highChar) => inner.WriteSurrogateCharEntity(lowChar, highChar);
        public override void WriteWhitespace(string? ws) => inner.WriteWhitespace(ws);
    }
}
