using OwnS3.Protocol.Errors;

namespace OwnS3.Protocol.Auth;

// Режим payload-строки x-amz-content-sha256 (arch/owns3/03 §1, таблица значений).
public enum PayloadHashMode
{
    HexSha256,
    UnsignedPayload,
    Streaming,
    StreamingTrailer,
}

// Результат классификации: режим и исходное значение (hex или режимная строка).
public sealed record PayloadHashModeClassification(PayloadHashMode Mode, string HexValue);

public static class PayloadHashModeClassifier
{
    public const string StreamingValue = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    public const string StreamingTrailerValue = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    public const string UnsignedPayloadValue = "UNSIGNED-PAYLOAD";

    // Классификация значения x-amz-content-sha256: вне перечня режимов — 400
    // InvalidRequest (глава 03 §1); чанковые режимы на не-PUT — 400 InvalidRequest
    // (arch-правка 4: isRequestSignStreamingV4 = значение ∧ PUT).
    public static PayloadHashModeClassification Classify(string? value, string method)
    {
        switch (value)
        {
            case UnsignedPayloadValue:
                return new(PayloadHashMode.UnsignedPayload, value);
            case StreamingValue:
            case StreamingTrailerValue:
                if (!string.Equals(method, "PUT", StringComparison.OrdinalIgnoreCase))
                    throw new S3ProtocolException(S3ErrorCode.InvalidRequest,
                        "Streaming signature is only supported on PUT requests");
                return new(value == StreamingValue ? PayloadHashMode.Streaming : PayloadHashMode.StreamingTrailer, value);
            case not null when IsHex64(value):
                return new(PayloadHashMode.HexSha256, value);
            default:
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Unsupported x-amz-content-sha256 value");
        }
    }

    private static bool IsHex64(string value) =>
        value.Length == 64 && value.All(c => c is (>= '0' and <= '9') or (>= 'a' and <= 'f'));
}
