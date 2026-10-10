namespace OwnS3.UnitTests;

// Тест-векторы SigV4 (план t36, таблица векторов): официальные примеры AWS
// (векторы 1–4, подписи совпадают с опубликованными дословно) и выведенные
// из того же seed (5–6). Учётка/дата/регион единые.
public static class TestVectors
{
    public const string AccessKey = "AKIAIOSFODNN7EXAMPLE";
    public const string SecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    public const string Region = "us-east-1";
    public const string ScopeDate = "20130524";
    public const string Scope = "20130524/us-east-1/s3/aws4_request";
    public const string AmzDate = "20130524T000000Z";
    public const string Host = "examplebucket.s3.amazonaws.com";

    public const string EmptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Вектор 1: GET /test.txt с Range (официальный пример AWS GetObject).
    public const string GetObjCreqSha = "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";
    public const string GetObjSignature = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

    // Вектор 2: GET /?max-keys=2&prefix=J (официальный пример AWS ListObjects).
    public const string ListObjSignature = "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7";

    // Вектор 3: presigned GET /test.txt, X-Amz-Expires=86400 (официальный пример AWS).
    public const string PresignedSignature = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

    // Вектор 4: PUT /test%24file.text с телом и storage-class (официальный пример AWS PutObject).
    public const string PutBody = "Welcome to Amazon S3.";
    public const string PutBodySha = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";
    public const string PutSignature = "1ee3a9a719bf9cd67d34043a52b3d1f8b674e378dc99c0748019b43f49b5b9bb";

    // Вектор 5: чанковая цепочка от seed вектора 1 (один чанк данных + 0-чанк).
    public const string Chunk1Signature = "c115618c80d5492b8fbd802845b0449cdbc16a5dc617f64114b42f23320f47de";
    public const string Chunk0Signature = "b01db302cacbbd831a862a7bdc9c4fa4919d2b93bc99a8523ce727e9a14f49b0";

    // Вектор 6: трейлер crc32 чанкового режима вектора 5.
    public const string Crc32B64 = "Ox7nCg==";
    public const string Sha256B64 = "RM591nyVng01JP+sF3Hfu6h9K2tLTpnkIDSouAP4sHI=";
    public const string TrailerSignature = "570042c8f9828c63e1aa1cf9c13ca35aa2efe47cec642c4cc890d46bd5916783";

    // Фиксированное «сейчас» для skew-тестов: внутри ±15 мин от AmzDate и вне.
    public static readonly DateTimeOffset FixedTime = new(2013, 5, 24, 0, 5, 0, TimeSpan.Zero);
    public static readonly DateTimeOffset SkewedTime = new(2013, 5, 24, 0, 20, 0, TimeSpan.Zero);
}

// Фиксированный TimeProvider (внутри skew-окна или вне — по переданному времени).
public sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => utcNow;
}
