namespace OwnS3.UnitTests.Erasure;

// Тест-векторы HighwayHash-256, сгенерированные Go-реализацией
// github.com/minio/highwayhash v1.0.3 (Sum256; источник истины порта t40).
// Генерация: go run с highwayhash.New(key) + Write(data) + Sum(nil).
public static class HighwayHashVectors
{
    // Магический ключ bitrot-контроля референса (cmd/bitrot.go, magicHighwayHash256Key).
    public const string MagicKeyHex = "4be734fa8e238acd263e83e6bb968552040f935da39f441497e09d1322de36a0";

    private const string Key00001FHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    // math/rand.NewSource(42).Read(1000 байт) — hex-дамп для воспроизведения в C#.
    private const string Rand1000Hex =
        "538c7f96b164bf1b97bb9f4bb472e89f5b1484f25209c9d9343e92ba09dd9d52dfd79b4d76429b617a0c9f9f0d3ba55b0cc0d6144c888535841acbe0709b0758083f61d375bc02b41df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4ffe1cebea546d8fac13dd1aac04ce2ea2877c5579cfa2c78e1b0bafae881b82a751108a42ed3c903caa43465a78620616978aed0ce3c6c4f3ae7bc3e0495b5712fefdbe0c102887e100dacd2d885f692cb607da00a11c1c7071e796a2dc2dc25a5b74b2e129705e273f05c92326828e2b056e3817658e1061498947fdf344410ed4c116023fa8e3576b6fed27ff8974bac0cafd9ad05692b13619e738964dfdc79e8d534373661cfd66d74fec1e1b89491ab7236e4b75216290cf2beb42c3ca27328560f1aac067cea6e8bf46d4ab2b4680402c5fb2820e885d3260f1de978283d4a09a36f96c20941746e3ed4da646a9ae8b4fa7b4fc3a20bafa1a75ed327a86b8b0c39af1cfd2b3b51219ea79533abf448d2c479d32607533905914c8cc1cf09da39e0e929d024abcbcb169397bb734e7ef0a6e01f1854deb5fe424fef7ac210891f5975e292aa2b8aa047c61cdb33373ebe72c27a098c02197dae6b732c351df668f874e2c9f1ce09ca86017e7e21748303ff41c1b23e11c48ed17539d685f76f2a798bc64de0e5db2864b2ad3c26ce6382342765a13d696e52df760f6c3465e29a0dca46ac3a0d570133bc21571f5f54a643105513fd8429b194ae1ba9f2a8386ae72b3661fe82c2ac64dbf46151824e2305312637c99c2b87d6df0dd5ef2836719072d672f61faa0be86f457921ec37c0e460cf0d703d07dc5f5661a450133409a712b6a66abd7ba65224ca26978d9a5ce62feb11a838f16622eb54cee3e6755229c183c1933a916585e183bb709192db0e6c8d709b9d7383b993c69499945b0b0cfe3dc2ec1fc9610d7eeb19d0916a27423c29e442f0e31c2c9539660f05fb37e45cd1bf0bcfc1a346c434f11cf00416fa61695b9202e68363a1c787f4784e0aed9c78564184e131f1e7c811fbd885bcc6734e8933f3cb2f25d52f9f8c7547ad7e1d6278c9f7c872f840f9d94d9fdedae93db2495718f580b8c8f2becc6aa76c3b0f948afbe6127a5cfd56bc11b3c2ea837a1c669a822f76864ec2ae0cd6bfddf21b1144474bfd37cd206b51ace050ab6199ec7b396d618fde0e1a2999a6f3d9f8b7ab8149f72deba5c6cd4fd4d392c92b4b21ac812034062dfc3063a7a6862b0fef1052919c2a87a4994fc1e401e11c5a49c7e8ea9e65d6cc40465fd352f2c42ec3d3ef60db69740517dc7652b9eeff516535aa1c9f33db13ed1f7ae1be384132328d103f871af9bcd15233c393978f5c17089e8f3267a0ccc7e39bfca";

    public sealed record Vector(string Name, byte[] Key, byte[] Data, string ExpectedHex256);

    private static byte[] HexToBytes(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        }

        return bytes;
    }

    public static IReadOnlyList<Vector> All { get; } = BuildVectors();

    private static List<Vector> BuildVectors()
    {
        var magic = HexToBytes(MagicKeyHex);
        var key00001f = HexToBytes(Key00001FHex);
        var rnd1000 = HexToBytes(Rand1000Hex);

        var one = new byte[] { 0x61 };
        var b31 = Enumerable.Range(0, 31).Select(i => (byte)(i + 1)).ToArray();
        var b32 = Enumerable.Range(0, 32).Select(i => (byte)(255 - i)).ToArray();
        var b33 = Enumerable.Range(0, 33).Select(i => (byte)(i * 7)).ToArray();
        var b64 = Enumerable.Range(0, 64).Select(i => (byte)(i * 3)).ToArray();

        return new List<Vector>
        {
            new("magic_empty", magic, Array.Empty<byte>(), "5e76d207cf4ab20866fdc03c83e8a0f4e8f458e880777956ec0bae4e9f23f6c5"),
            new("magic_one", magic, one, "4d6dc44bec31fb10c8f7abac6ee37e0c0a289ee5415af0b814b196ed3c16f194"),
            new("magic_31", magic, b31, "9e12e331819a75a6355707734a4ef1555ac5b0f91edcd18fd444874fbecf792b"),
            new("magic_32", magic, b32, "7972ebdb8cc2e9c753d7c9c91a9e99ba2a53d79b7ee745b28504859c70cdb3ab"),
            new("magic_33", magic, b33, "0c015dab5c43a4633cabfad6e10649cdd52e753ccc3939025a7abdeefa99c935"),
            new("magic_64", magic, b64, "dcdd82b5976f2d83baa869edb587ee40a902d17c0c5531df08cc18be2d991fb7"),
            new("magic_rand1000", magic, rnd1000, "292c8ad92dcc3cede0d7960d846b4261fccad1066695dfb3939c45108ccd9d23"),
            new("magic_5mib_5a", magic, FilledBytes(5 * 1024 * 1024, 0x5a), "dfd73c435ae0ddb297f884e2589ef037f52bc531cbe7087dbd1423477b3cd86d"),
            new("key00_empty", key00001f, Array.Empty<byte>(), "f574c8c22a4844dd1f35c713730146d9ff1487b9ccbeaeb3f41d75453123da41"),
            new("key00_one", key00001f, one, "27854f198301f1d6471d31b2c0881282d8242cc73588cc354ad0320ff3ce005a"),
            new("key00_rand1000", key00001f, rnd1000, "6a20b2fddc9e4d2ec60b107f9d878c6ab213f4b4b667421146e59c2cdfade36b"),
        };
    }

    private static byte[] FilledBytes(int len, byte fill)
    {
        var data = new byte[len];
        Array.Fill(data, fill);
        return data;
    }
}
