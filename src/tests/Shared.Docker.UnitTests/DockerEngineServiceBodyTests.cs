using System.Text.Json;
using FluentAssertions;
using Shared.Docker;
using Xunit;

// BuildServiceBody — тело POST /services/create (arch/24 §2): label-пара
// s3worker=<C> обязана жить на ОБОИХ уровнях — top-level Labels сервиса
// (перечисление объектов кластера ListServiceRecordsAsync читает Spec.Labels)
// и TaskTemplate.ContainerSpec.Labels (контейнеры тасков).
public class DockerEngineServiceBodyTests
{
    // Сериализация в JSON — как реальный провод Engine API (не casts словарей).
    private static JsonElement Build(ServiceSpec spec)
        => JsonSerializer.SerializeToElement(DockerEngine.BuildServiceBody(spec));

    [Fact]
    public void BuildServiceBody_LabelOnBothLevels()
    {
        // Arrange: сервис с label-парой кластера (spec-шаблон несёт LabelKey/Label).
        var spec = new ServiceSpec(
            "s3w-c1-east1",
            new ContainerSpec(
                "owns3:dev",
                [new PortMap(9000, 18001)],
                "east1",
                VolumeName: "s3w-c1-east1-data",
                VolumeDest: "/data",
                LabelKey: "s3worker",
                Label: "c1"),
            NodeConstraint: "id-a");

        // Act
        var body = Build(spec);

        // Assert: top-level Labels сервиса (его списывает Spec.Labels).
        body.GetProperty("Labels").GetProperty("s3worker").GetString().Should().Be("c1");

        // Assert: TaskTemplate.ContainerSpec.Labels (контейнеры тасков).
        body.GetProperty("TaskTemplate").GetProperty("ContainerSpec")
            .GetProperty("Labels").GetProperty("s3worker").GetString().Should().Be("c1");
    }

    [Fact]
    public void BuildServiceBody_NoLabel_EmptyServiceLabels()
    {
        // Arrange: сервис без label-пары (LabelKey/Label optional в спеке).
        var spec = new ServiceSpec(
            "s3w-c1-east1",
            new ContainerSpec("owns3:dev", [new PortMap(9000, 18001)], "east1"),
            NodeConstraint: "id-a");

        // Act
        var body = Build(spec);

        // Assert: top-level Labels присутствует и пуст (Spec.Labels спарсится
        // пустым словарём — HasClusterLabel корректно false).
        var labels = body.GetProperty("Labels");
        labels.ValueKind.Should().Be(JsonValueKind.Object);
        labels.EnumerateObject().Should().BeEmpty();
    }
}
