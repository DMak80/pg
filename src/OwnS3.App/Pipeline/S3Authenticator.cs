using Microsoft.AspNetCore.Http;
using OwnS3.App.Access;
using OwnS3.App.Routing;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;
using OwnS3.Protocol.Xml;

namespace OwnS3.App.Pipeline;

// Аутентификатор S3-грани (глава 03 §1–2; решения §3.4 шаг 4): выбор режима —
// заголовочная подпись / presigned / аноним; чанковые режимы готовят контекст
// обёртки тела для хендлера (AwsChunkedReader навешивается над Request.Body).
public sealed class S3Authenticator(
    SigV4HeaderVerifier headerVerifier,
    PresignedRequestVerifier presignedVerifier,
    AccessKeyRegistry registry)
{
    public AuthenticationOutcome Authenticate(HttpContext http, S3RequestModel model, S3Operation operation)
    {
        var hasAuthorization = http.Request.Headers.ContainsKey("Authorization");
        var hasPresignedParams = HasQueryParameter(model.RawQuery, "X-Amz-Algorithm");

        // Анонимный запрос — public-доступа нет (глава 03 §1).
        if (!hasAuthorization && !hasPresignedParams)
            return AuthenticationOutcome.Failed(new S3Error(S3ErrorCode.AccessDenied,
                Resource: model.RawPath));

        if (hasAuthorization)
        {
            var result = headerVerifier.Verify(model, accessKey => registry.Find(accessKey)?.SecretKey);
            if (result is SigV4Result.Fail fail)
                return AuthenticationOutcome.Failed(new S3Error(fail.Code, Resource: model.RawPath,
                    MessageOverride: fail.Detail));

            var ok = (SigV4Result.Ok)result;
            var record = registry.Find(ok.AccessKey)!;

            // Чанковые режимы: тело запроса оборачивается AwsChunkedReader
            // (глава 03 §3) — контекст передаётся хендлеру через Items.
            if (ok.Mode is PayloadHashMode.Streaming or PayloadHashMode.StreamingTrailer)
                http.Items[ChunkedContextKey] = BuildChunkedContext(http, model, ok);

            return AuthenticationOutcome.Authenticated(new AuthenticatedIdentity(ok.AccessKey, record.Policy));
        }

        // Presigned: имя операции определено роутером раньше (глава 03 §2).
        var presigned = presignedVerifier.Verify(model, operation.ToString(),
            accessKey => registry.Find(accessKey)?.SecretKey);
        if (presigned is SigV4Result.Fail presignedFail)
            return AuthenticationOutcome.Failed(new S3Error(presignedFail.Code, Resource: model.RawPath,
                MessageOverride: presignedFail.Detail));

        var presignedOk = (SigV4Result.Ok)presigned;
        var presignedRecord = registry.Find(presignedOk.AccessKey)!;
        return AuthenticationOutcome.Authenticated(new AuthenticatedIdentity(presignedOk.AccessKey, presignedRecord.Policy));
    }

    // Ключ Items для контекста чанковой обёртки (забирает хендлер задачи 10).
    public const string ChunkedContextKey = "OwnS3.ChunkedContext";

    private AwsChunkedReadingContext? BuildChunkedContext(HttpContext http, S3RequestModel model, SigV4Result.Ok ok)
    {
        var decodedLength = ParseLong(http.Request.Headers["x-amz-decoded-content-length"].FirstOrDefault())
            ?? -1;
        var trailerNames = (http.Request.Headers["x-amz-trailer"].FirstOrDefault() ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(n => n.ToLowerInvariant())
            .ToList();

        return new AwsChunkedReadingContext(
            SecretKey: registry.Find(ok.AccessKey)!.SecretKey,
            AmzDate: ok.AmzDate,
            Scope: ok.Scope,
            SeedSignature: ok.SeedSignature,
            DecodedContentLength: decodedLength,
            WithTrailers: ok.Mode == PayloadHashMode.StreamingTrailer,
            TrailerNames: trailerNames);
    }

    private static long? ParseLong(string? value) =>
        long.TryParse(value, out var parsed) ? parsed : null;

    private static bool HasQueryParameter(string rawQuery, string name)
    {
        foreach (var pair in rawQuery.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=');
            var key = eq < 0 ? pair : pair[..eq];
            if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}

// Исход аутентификации: успех с идентичностью либо каноническая ошибка.
public sealed record AuthenticationOutcome(AuthenticatedIdentity? Identity, S3ProtocolException? Error)
{
    public static AuthenticationOutcome Authenticated(AuthenticatedIdentity identity) => new(identity, null);
    public static AuthenticationOutcome Failed(S3Error error) => new(null,
        new S3ProtocolException(error.Code, error.MessageOverride ?? error.Info.Message));
}
