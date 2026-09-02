using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Writing;
using Xunit;

namespace KafkaWorker.UnitTests.Writing;

// Валидация мутации №15 (t06, spec §4.2): границы §10.3, null = не менять,
// хотя бы одно поле обязательно; эффективные значения = new ?? current.
public class KafkaResourcesUpdateValidatorTests
{
    private static readonly BrokerResources Current = new(2m, 4, 40);

    [Fact]
    public void Validate_AllFieldsNull_SingleError()
    {
        // Arrange
        var request = new KafkaResourcesUpdateRequest(null, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().ContainSingle().Which.Field.Should().Be("");
    }

    [Fact]
    public void Validate_PartialUpdate_EffectiveValuesInBounds_NoErrors()
    {
        // Arrange — меняется только cpu, mem/disk наследуются
        var request = new KafkaResourcesUpdateRequest(4m, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void Validate_NewCpuOutOfBounds_Error()
    {
        // Arrange
        var request = new KafkaResourcesUpdateRequest(100m, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().Contain(e => e.Field == "cpu");
    }

    [Fact]
    public void Validate_NewMemInvalid_Error()
    {
        // Arrange — новый memGi вне границ
        var request = new KafkaResourcesUpdateRequest(null, 0, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().Contain(e => e.Field == "memGi");
    }

    [Fact]
    public void Validate_DiskDecreaseAllowed_NoErrors()
    {
        // Arrange — уменьшение разрешено (spec §3.5: риск OOM — оператор)
        var request = new KafkaResourcesUpdateRequest(1m, 2, 10);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().BeEmpty();
    }
}
