using System.Text;
using OwnS3.Protocol.Errors;

namespace OwnS3.Protocol.Xml;

// Каноническая S3-ошибка (arch/owns3/03 §5): код из каталога, Resource —
// path-style путь запроса, RequestId — UUID на запрос (генерирует App),
// HostId — из конфигурации; MessageOverride подменяет канонический Message.
public sealed record S3Error(
    S3ErrorCode Code,
    string? Resource = null,
    string? RequestId = null,
    string? HostId = null,
    string? MessageOverride = null)
{
    public S3ErrorInfo Info => S3ErrorCatalog.Get(Code);

    public string Message => MessageOverride ?? Info.Message;
}

// Канонический XML ошибки: <Error><Code/><Message/><Resource/><RequestId/><HostId/></Error>.
public static class S3ErrorXmlWriter
{
    public static string Write(S3Error error) =>
        new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\"?>")
            .Append("<Error>")
            .Append("<Code>").Append(Escape(error.Info.Code.ToString())).Append("</Code>")
            .Append("<Message>").Append(Escape(error.Message)).Append("</Message>")
            .Append("<Resource>").Append(Escape(error.Resource ?? string.Empty)).Append("</Resource>")
            .Append("<RequestId>").Append(Escape(error.RequestId ?? string.Empty)).Append("</RequestId>")
            .Append("<HostId>").Append(Escape(error.HostId ?? string.Empty)).Append("</HostId>")
            .Append("</Error>")
            .ToString();

    private static string Escape(string value) => value
        .Replace("&", "&amp;", StringComparison.Ordinal)
        .Replace("<", "&lt;", StringComparison.Ordinal)
        .Replace(">", "&gt;", StringComparison.Ordinal)
        .Replace("\"", "&quot;", StringComparison.Ordinal);
}
