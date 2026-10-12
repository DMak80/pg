using System.Net.Http.Json;
using System.Text.Json;

namespace OwnS3.IntegrationTests.Api;

// Админ-грань /admin/storage и /admin/heal (t40, план Task 14; канон 05 §7):
// SigV4 admin/root — доступ, прочим 403; EC-статус — схема с сетами;
// single-режим — статус без сетов, heal — 202 no-op.
[Collection(OwnS3EcTestCollection.Name)]
public class AdminScenarios(OwnS3EcAppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task Storage_RootKey_200_EcSchema()
    {
        // Arrange
        var client = NewClient();

        // Act — root-ключ (admin-политика); ждём завершения идущего heal-прохода
        // (старт-проход/внеплановый из соседних кейсов коллекции гоняют состояние)
        JsonDocument json;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (true)
        {
            var response = await client.SendSignedAsync("GET", "/admin/storage",
                credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));
            json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
            if (!json.RootElement.GetProperty("heal").GetProperty("running").GetBoolean()
                || DateTimeOffset.UtcNow >= deadline)
                break;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        // Assert — режим XL Erasure, сет 1, 4 диска онлайн
        json.RootElement.GetProperty("mode").GetString().Should().Be("XL Erasure");
        var sets = json.RootElement.GetProperty("sets");
        sets.GetArrayLength().Should().Be(1);
        sets[0].GetProperty("readable").GetBoolean().Should().BeTrue();
        sets[0].GetProperty("writable").GetBoolean().Should().BeTrue();
        sets[0].GetProperty("drives").GetArrayLength().Should().Be(4);
        sets[0].GetProperty("drives").EnumerateArray().Should().OnlyContain(d => d.GetProperty("online").GetBoolean());
        json.RootElement.GetProperty("heal").GetProperty("running").GetBoolean().Should().BeFalse();
    }

    // Дисковые поля (owns3/05 §7, t32): EC — per-drive + суммы верхнего уровня.
    [Fact]
    public async Task Storage_EcDiskFields_PerDriveAndSum()
    {
        // Arrange — root-ключ (admin-политика)
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("GET", "/admin/storage",
            credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));
        var json = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;

        // Assert — каждый диск: поля не-null, used ≤ total; верхний уровень — суммы по дискам пула
        var drives = json.GetProperty("sets")[0].GetProperty("drives");
        long sumUsed = 0, sumTotal = 0;
        foreach (var d in drives.EnumerateArray())
        {
            d.TryGetProperty("usedBytes", out var usedEl).Should().BeTrue();
            d.TryGetProperty("totalBytes", out var totalEl).Should().BeTrue();
            usedEl.ValueKind.Should().Be(JsonValueKind.Number);
            totalEl.ValueKind.Should().Be(JsonValueKind.Number);
            var used = usedEl.GetInt64();
            var total = totalEl.GetInt64();
            used.Should().BeGreaterThanOrEqualTo(0);
            used.Should().BeLessThanOrEqualTo(total);
            sumUsed += used;
            sumTotal += total;
        }

        json.GetProperty("usedBytes").GetInt64().Should().Be(sumUsed);
        json.GetProperty("totalBytes").GetInt64().Should().Be(sumTotal);
        // 4 диска-подкаталога одного физического тома: сумма total равна 4×размер тома
        var volumeTotal = new DriveInfo(Path.GetFullPath(factory.DataDirs[0])).TotalSize;
        sumTotal.Should().Be(volumeTotal * 4);
    }

    [Fact]
    public async Task Storage_AdminKey_200()
    {
        // Arrange/Act — статический admin-ключ
        var client = NewClient();
        var response = await client.SendSignedAsync("GET", "/admin/storage", credentials: OwnS3TestClient.Admin());

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
    }

    [Fact]
    public async Task Storage_ReadWriteKey_403()
    {
        // Arrange/Act — read-write НЕ админ: 403 AccessDenied
        var client = NewClient();
        var response = await client.SendSignedAsync("GET", "/admin/storage", credentials: OwnS3TestClient.Writer());

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.Forbidden);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("AccessDenied");
    }

    [Fact]
    public async Task Heal_AdminKey_202()
    {
        // Arrange/Act
        var client = NewClient();
        var response = await client.SendSignedAsync("POST", "/admin/heal", credentials: OwnS3TestClient.Admin());

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.Accepted);
    }

    [Fact]
    public async Task Storage_Anonymous_403()
    {
        // Arrange/Act — без подписи: 403
        var client = NewClient();
        var response = await client.Http.GetAsync("/admin/storage", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.Forbidden);
    }

    // single-ветка на существующей single-фабрике (грань доступна всегда — 05 §7)
    [Collection(OwnS3TestCollection.Name)]
    public class SingleModeEdge(OwnS3AppFactory singleFactory)
    {
        private OwnS3TestClient NewClient() => new(singleFactory.CreateClient());

        [Fact]
        public async Task AdminEdge_SingleMode_StorageIsSingle_HealIs202NoOp()
        {
            // Arrange
            var client = NewClient();

            // Act
            var storage = await client.SendSignedAsync("GET", "/admin/storage",
                credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));
            var heal = await client.SendSignedAsync("POST", "/admin/heal",
                credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));

            // Assert — режим XL Single без сетов; heal — 202 no-op
            storage.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
            var json = JsonDocument.Parse(
                await storage.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            json.GetProperty("mode").GetString().Should().Be("XL Single");
            json.GetProperty("sets").GetArrayLength().Should().Be(0);
            heal.StatusCode.Should().Be(System.Net.HttpStatusCode.Accepted);
        }

        // Single: дисковые поля верхнего уровня присутствуют и согласованы с томом (owns3/05 §7, t32).
        [Fact]
        public async Task Storage_SingleMode_DiskFieldsMatchDataRoot()
        {
            // Arrange — single-фабрика; root-креды
            var client = NewClient();

            // Act
            var response = await client.SendSignedAsync("GET", "/admin/storage",
                credentials: new OwnS3TestClient.Credentials("root", "rootpassword"));
            var json = JsonDocument.Parse(
                await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;

            // Assert — поля есть; used в [0, total]; total согласован с фактическим томом корня
            var used = json.GetProperty("usedBytes").GetInt64();
            var total = json.GetProperty("totalBytes").GetInt64();
            used.Should().BeGreaterThanOrEqualTo(0);
            total.Should().BeGreaterThan(0);
            used.Should().BeLessThanOrEqualTo(total);
            var root = singleFactory.TempVolumeDir;
            new DriveInfo(Path.GetFullPath(root)).TotalSize.Should().Be(total);
        }
    }
}
