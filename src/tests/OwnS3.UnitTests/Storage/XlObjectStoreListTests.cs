using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Листинги: строгий UTF-8 байтовый порядок, marker/continuation/start-after,
// prefix/delimiter/CommonPrefixes, max-keys, спецключи, битые xl.meta.
// Изоляция: каждый кейс — на уникальном бакете СВОЕГО кейса (ограничение 8 —
// delimiter/CP видят всё дерево бакета).
public class XlObjectStoreListTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private XlObjectStore Store => fixture.Store;
    private string Root => fixture.Root;

    private static readonly ObjectUploadMetadata Meta =
        new("text/plain", new Dictionary<string, string>());

    // Уникальный бакет кейса.
    private async Task<string> NewBucketAsync()
    {
        var name = "bl-" + Guid.NewGuid().ToString("N")[..10];
        await Store.CreateBucketAsync(name, TestContext.Current.CancellationToken);
        return name;
    }

    private async Task PutAsync(string bucket, string key, string content = "x") =>
        await Store.PutObjectAsync(bucket, key, new MemoryStream(Encoding.UTF8.GetBytes(content)),
            Encoding.UTF8.GetByteCount(content), Meta, TestContext.Current.CancellationToken);

    private Task<ListPage> ListAsync(string bucket, ListQuery query) =>
        Store.ListObjectsAsync(bucket, query, TestContext.Current.CancellationToken);

    private static ListQuery Q(string? prefix = null, string? delimiter = null, string? marker = null,
        string? startAfter = null, string? token = null, int? maxKeys = null, ListVariant variant = ListVariant.V2) =>
        new(prefix, delimiter, marker, startAfter, token, maxKeys, null, false, variant);

    private static List<string> Keys(ListPage page) => page.Contents.Select(e => e.Key).ToList();
    private static List<string> Prefixes(ListPage page) => page.CommonPrefixes.Select(p => p.Prefix).ToList();

    [Fact]
    public async Task Order_StrictUtf8ByteLexicographic()
    {
        // Arrange: смесь ASCII/разделителей/диакритики/вне-BMP
        var b = await NewBucketAsync();
        foreach (var key in new[] { "b", "ab", "a/m", "a!z", "a", "~", "\u00c1", "\U0001F600", "Z" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q());

        // Assert: строгий байтовый порядок UTF-8 (P9): Z < a < a!z < a/m < ab < b < ~ < Á < 😀
        Keys(page).Should().Equal("Z", "a", "a!z", "a/m", "ab", "b", "~", "\u00c1", "\U0001F600");
        // ETag листинга — в кавычках, как у Get/Head (P8)
        page.Contents.Should().OnlyContain(e => e.ETag.StartsWith('"') && e.ETag.EndsWith('"'));
    }

    [Theory]
    [InlineData("a", new[] { "a!z", "a/m", "ab", "b" })]
    [InlineData("a!z", new[] { "a/m", "ab", "b" })]
    [InlineData("a/m", new[] { "ab", "b" })]
    public async Task Marker_AfterInsideMixedShape(string after, string[] expected)
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a", "a!z", "a/m", "ab", "b" })
            await PutAsync(b, key);

        // Act: строго после маркера (внутри «смешанной» формы ключа)
        var page = await ListAsync(b, Q(marker: after, variant: ListVariant.V1));

        // Assert
        Keys(page).Should().Equal(expected);
    }

    [Fact]
    public async Task Marker_SkipSiblingPrefixTrap()
    {
        // Arrange: контрпример ревью M-2 — после «m!z» дети «m0»/«mz» обязаны эмититься
        var b = await NewBucketAsync();
        foreach (var key in new[] { "m", "m!z", "m0", "mz" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(marker: "m!z", variant: ListVariant.V1));

        // Assert: ни «m» (< after), ни «m!z» (== after); ровно m0, mz
        Keys(page).Should().Equal("m0", "mz");
    }

    [Fact]
    public async Task List_BrokenXlMeta_SkippedWithWarning()
    {
        // Arrange: объект с мусорными xl.meta и xl.meta.bkp
        var b = await NewBucketAsync();
        await PutAsync(b, "broken");
        await PutAsync(b, "healthy");
        var dir = Path.Combine(Root, b, "broken");
        File.WriteAllText(Path.Combine(dir, "xl.meta"), "мусор");
        File.WriteAllText(Path.Combine(dir, "xl.meta.bkp"), "мусор");

        // Act
        var page = await ListAsync(b, Q());

        // Assert: битый объект не появляется и обход не роняет
        Keys(page).Should().Equal("healthy");
    }

    [Fact]
    public async Task NestedKeys_BothListed()
    {
        // Arrange: ключ a и вложенный a/b сосуществуют
        var b = await NewBucketAsync();
        await PutAsync(b, "a");
        await PutAsync(b, "a/b");

        // Act / Assert
        Keys(await ListAsync(b, Q())).Should().Equal("a", "a/b");
    }

    [Fact]
    public async Task DirKey_ListsAsKeyWithTrailingSlash()
    {
        // Arrange: ключ-каталог — полноценный ключ
        var b = await NewBucketAsync();
        await PutAsync(b, "dir/");

        // Act / Assert
        Keys(await ListAsync(b, Q())).Should().Equal("dir/");
    }

    [Fact]
    public async Task Prefix_Filter()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "photos/1", "photos/2", "videos/1" })
            await PutAsync(b, key);

        // Act / Assert
        Keys(await ListAsync(b, Q(prefix: "photos/"))).Should().Equal("photos/1", "photos/2");
    }

    [Fact]
    public async Task Prefix_PartialLastSegment()
    {
        // Arrange: префикс режет последний сегмент — байтовый префикс
        var b = await NewBucketAsync();
        foreach (var key in new[] { "photos/1", "photos/2", "photos/2x", "photos/2020/x", "photos/3" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(prefix: "photos/2"));

        // Assert
        Keys(page).Should().Equal("photos/2", "photos/2020/x", "photos/2x");
    }

    [Fact]
    public async Task Delimiter_CommonPrefixes_Dedup_CountedInMaxKeys()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/1", "a/2", "b" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(delimiter: "/"));

        // Assert: свёртка в один CP, дедуп; Contents — только «b»
        Prefixes(page).Should().Equal("a/");
        Keys(page).Should().Equal("b");
        page.KeyCount.Should().Be(2);

        // Act 2: maxKeys=1 — считается и CP; усечение
        var truncated = await ListAsync(b, Q(delimiter: "/", maxKeys: 1));

        // Assert
        Prefixes(truncated).Should().Equal("a/");
        Keys(truncated).Should().BeEmpty();
        truncated.IsTruncated.Should().BeTrue();
    }

    [Fact]
    public async Task Delimiter_DirKeyFoldsIntoItself()
    {
        // Arrange: ключ-каталог «photos/» со свёрткой — CP «photos/»
        var b = await NewBucketAsync();
        await PutAsync(b, "photos/");
        await PutAsync(b, "root");

        // Act
        var page = await ListAsync(b, Q(delimiter: "/"));

        // Assert
        Prefixes(page).Should().Equal("photos/");
        Keys(page).Should().Equal("root");
    }

    [Fact]
    public async Task Marker_StrictlyAfter_SkipsSubtree()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/w", "a/x", "a/y", "b" })
            await PutAsync(b, key);

        // Act: строго после «a/x»
        var page = await ListAsync(b, Q(marker: "a/x", variant: ListVariant.V1));

        // Assert: «a/w» не эмитится (раньше маркера), «a/y» и «b» — эмитятся
        Keys(page).Should().Equal("a/y", "b");
    }

    [Fact]
    public async Task ContinuationToken_Paging_Deterministic()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "k1", "k2", "k3" })
            await PutAsync(b, key);

        // Act: страница 1
        var page1 = await ListAsync(b, Q(maxKeys: 2));

        // Assert
        Keys(page1).Should().Equal("k1", "k2");
        page1.IsTruncated.Should().BeTrue();
        page1.NextContinuationToken.Should().NotBeNull();

        // Act: страница 2 по токену; повторный вызов — тот же результат
        var page2 = await ListAsync(b, Q(token: page1.NextContinuationToken));
        var page2again = await ListAsync(b, Q(token: page1.NextContinuationToken));

        // Assert: детерминированность (деревья читаются с диска)
        Keys(page2).Should().Equal("k3");
        page2.IsTruncated.Should().BeFalse();
        Keys(page2again).Should().Equal(Keys(page2));
    }

    [Fact]
    public async Task ContinuationToken_Invalid_InvalidArgument()
    {
        // Arrange
        var b = await NewBucketAsync();
        await PutAsync(b, "k1");

        // Act: не-base64url токен
        var act = async () => await ListAsync(b, Q(token: "не токен!!"));

        // Assert
        await act.Should().ThrowAsync<XlInvalidArgumentException>()
            .WithMessage("Invalid continuation token");
    }

    [Fact]
    public async Task StartAfter_IgnoredWhenTokenPresent()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "k1", "k2", "k3" })
            await PutAsync(b, key);

        // Act: токен (k1) выигрывает у start-after (k3)
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes("k1"))
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var page = await ListAsync(b, Q(startAfter: "k3", token: token));

        // Assert: продолжение от k1, start-after проигнорирован
        Keys(page).Should().Equal("k2", "k3");
    }

    [Fact]
    public async Task Delimiter_MarkerContinuation_AfterCommonPrefix()
    {
        // Arrange: усечение НА CommonPrefix — продолжение строго после CP
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/1", "a/2", "b" })
            await PutAsync(b, key);

        // Act: страница 1 — только CP «a/»
        var page1 = await ListAsync(b, Q(delimiter: "/", maxKeys: 1));

        // Assert
        Prefixes(page1).Should().Equal("a/");
        Keys(page1).Should().BeEmpty();
        page1.IsTruncated.Should().BeTrue();

        // Act: страница 2 по continuation-токену (== CP «a/»)
        var page2 = await ListAsync(b, Q(delimiter: "/", token: page1.NextContinuationToken));

        // Assert: CP «a/» НЕ пере-эмитится; только «b»
        Prefixes(page2).Should().BeEmpty();
        Keys(page2).Should().Equal("b");
        page2.KeyCount.Should().Be(1);
        page2.IsTruncated.Should().BeFalse();

        // Act: v1 marker = CP «a/» — тот же исход
        var v1 = await ListAsync(b, Q(delimiter: "/", marker: "a/", variant: ListVariant.V1));

        // Assert
        Prefixes(v1).Should().BeEmpty();
        Keys(v1).Should().Equal("b");
    }

    [Fact]
    public async Task MaxKeys_ExactlyTotal_NotTruncated()
    {
        // Arrange: ровно maxKeys эмитов — усечения нет (S3: IsTruncated=false,
        // лишней пустой страницы не будет)
        var b = await NewBucketAsync();
        foreach (var key in new[] { "k1", "k2", "k3" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(maxKeys: 3));

        // Assert
        Keys(page).Should().Equal("k1", "k2", "k3");
        page.IsTruncated.Should().BeFalse();
        page.NextContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task MaxKeys_ExactlyTotal_WithDelimiter_NotTruncated()
    {
        // Arrange: CP + ключ ровно по maxKeys
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/1", "b" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(delimiter: "/", maxKeys: 2));

        // Assert
        Prefixes(page).Should().Equal("a/");
        Keys(page).Should().Equal("b");
        page.IsTruncated.Should().BeFalse();
        page.NextContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task MaxKeys_Zero_EmptyNotTruncated()
    {
        // Arrange
        var b = await NewBucketAsync();
        await PutAsync(b, "k1");

        // Act
        var page = await ListAsync(b, Q(maxKeys: 0));

        // Assert
        Keys(page).Should().BeEmpty();
        page.IsTruncated.Should().BeFalse();
    }

    [Fact]
    public async Task NextMarker_V1_OnlyWithDelimiter()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/1", "a/2", "b" })
            await PutAsync(b, key);

        // Act
        var withDelimiter = await ListAsync(b, Q(delimiter: "/", maxKeys: 1, variant: ListVariant.V1));
        var withoutDelimiter = await ListAsync(b, Q(maxKeys: 1, variant: ListVariant.V1));

        // Assert: NextMarker — только при delimiter (иначе клиент продолжает по последнему Key)
        withDelimiter.NextMarker.Should().Be("a/");
        withoutDelimiter.NextMarker.Should().BeNull();
    }

    [Fact]
    public async Task KeyCount_V2_SumsContentsAndPrefixes()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a/1", "a/2", "b", "c" })
            await PutAsync(b, key);

        // Act
        var page = await ListAsync(b, Q(delimiter: "/"));

        // Assert
        Prefixes(page).Should().Equal("a/");
        Keys(page).Should().Equal("b", "c");
        page.KeyCount.Should().Be(3);
    }

    [Fact]
    public async Task Versions_SameWalkAsV1()
    {
        // Arrange
        var b = await NewBucketAsync();
        foreach (var key in new[] { "a", "a/b", "b" })
            await PutAsync(b, key);

        // Act: Versions — тот же обход (variants-обёртки добавляет App)
        var page = await ListAsync(b, Q(variant: ListVariant.Versions));

        // Assert
        Keys(page).Should().Equal("a", "a/b", "b");
    }

    [Fact]
    public async Task MissingBucket_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await Store.ListObjectsAsync("missing", Q(),
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task EmptyBucket_EmptyPage()
    {
        // Arrange
        var b = await NewBucketAsync();

        // Act / Assert
        var page = await ListAsync(b, Q());
        Keys(page).Should().BeEmpty();
        Prefixes(page).Should().BeEmpty();
        page.IsTruncated.Should().BeFalse();
        page.KeyCount.Should().Be(0);
    }

    [Fact]
    public async Task SpecialKeys_ListedDecoded()
    {
        // Arrange
        var b = await NewBucketAsync();
        await PutAsync(b, "a//b");
        await PutAsync(b, "100%");
        await PutAsync(b, "plain");

        // Act / Assert: ключи листятся в декодированной форме
        Keys(await ListAsync(b, Q())).Should().Equal("100%", "a//b", "plain");
    }
}
