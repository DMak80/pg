namespace OwnS3.IntegrationTests.Api;

// Контракт 21 операции в EC-топологии (t40, план Task 15; спека §6.2):
// наследники контрактовых серий на коллекции OwnS3Ec (4 диска 3+1) — все кейсы
// Access/Auth/BodyIntegrity/ErrorFormat/Multipart/ObjectStorage/Routing
// исполняются против EC-драйва без правок ассертов; платформенная проверка
// «бакет реально на томе» — каталог на каждом диске пула (канон 06 §8).
// Изоляция имён: мутационные бакеты серий уникальны (b-*, b-mp-*), общий
// EC-том коллекции не даёт конфликтов; «bucket» никогда не создаётся.

[Collection(OwnS3EcTestCollection.Name)]
public sealed class AccessEcScenarios(OwnS3EcAppFactory factory) : AccessScenariosBase(factory)
{
    protected override void AssertBucketExistsOnVolume(string bucket)
    {
        // Assert: бакет — каталог в корне КАЖДОГО диска пула
        foreach (var dir in factory.DataDirs)
            Directory.Exists(Path.Combine(dir, bucket)).Should().BeTrue(
                "бакет {0} обязан присутствовать на диске {1}", bucket, dir);
    }
}

[Collection(OwnS3EcTestCollection.Name)]
public sealed class AuthEcScenarios(OwnS3EcAppFactory factory) : AuthScenariosBase(factory);

[Collection(OwnS3EcTestCollection.Name)]
public sealed class BodyIntegrityEcScenarios(OwnS3EcAppFactory factory) : BodyIntegrityScenariosBase(factory);

[Collection(OwnS3EcTestCollection.Name)]
public sealed class ErrorFormatEcScenarios(OwnS3EcAppFactory factory) : ErrorFormatScenariosBase(factory);

[Collection(OwnS3EcTestCollection.Name)]
public sealed class MultipartEcScenarios(OwnS3EcAppFactory factory) : MultipartScenariosBase(factory);

[Collection(OwnS3EcTestCollection.Name)]
public sealed class ObjectStorageEcScenarios(OwnS3EcAppFactory factory) : ObjectStorageScenariosBase(factory);

[Collection(OwnS3EcTestCollection.Name)]
public sealed class RoutingEcScenarios(OwnS3EcAppFactory factory) : RoutingScenariosBase(factory)
{
    protected override void AssertBucketExistsOnVolume(string bucket)
    {
        // Assert: бакет — каталог в корне КАЖДОГО диска пула
        foreach (var dir in factory.DataDirs)
            Directory.Exists(Path.Combine(dir, bucket)).Should().BeTrue(
                "бакет {0} обязан присутствовать на диске {1}", bucket, dir);
    }
}
