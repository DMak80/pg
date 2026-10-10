using Xunit;

namespace Shared.Core.UnitTests;

/// <summary>
/// Инварианты Result/Result&lt;T&gt;: значимый тип с ранним ветвлением обязан
/// наблюдаемо вести себя как прежняя классовая реализация — проброс исходного
/// исключения, короткое замыкание, семантика Throw/стека, конверсии, равенство.
/// </summary>
public class ResultTests
{
    private static Exception Fail(string message = "boom")
        => new ApplicationException(message);

    // --- Создание / базовые свойства ---

    [Fact]
    public void Success_IsSuccess_AndNoError()
    {
        // Arrange
        // Act
        var result = Result<int>.Success(5);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Error.Should().BeNull();
        result.Value.Should().Be(5);
    }

    [Fact]
    public void Failed_NotSuccess_AndSameError()
    {
        // Arrange
        var error = Fail();

        // Act
        var result = Result<int>.Failed(error);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void Failed_FillsMissingStackTrace()
    {
        // Arrange — исключение создано без throw, трейса нет
        var error = Fail();
        error.StackTrace.Should().BeNull();

        // Act
        _ = Result<int>.Failed(error);

        // Assert — Failed восполняет трейс через ExceptionDispatchInfo
        error.StackTrace.Should().NotBeNull();
    }

    [Fact]
    public void Failed_KeepsExistingStackTrace()
    {
        // Arrange
        var error = ExceptionWithStackTrace();

        // Act
        var result = Result<int>.Failed(error);

        // Assert — исключение с трейсом не пересоздаётся
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void UnitResult_SuccessAndDefault_AreEqual()
    {
        // Arrange
        // Act
        var success = Result.Success();

        // Assert — default(Result) — валидный успех
        success.IsSuccess.Should().BeTrue();
        success.Should().Be(default(Result));
    }

    // --- Синхронные комбинаторы ---

    [Fact]
    public void Bind_OnSuccess_CallsSelector()
    {
        // Arrange
        var result = Result<int>.Success(10);

        // Act
        var mapped = result.Bind(v => Result<string>.Success($"v={v}"));

        // Assert
        mapped.IsSuccess.Should().BeTrue();
        mapped.Value.Should().Be("v=10");
    }

    [Fact]
    public void Bind_OnFail_ShortCircuitsSameError()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);
        var calls = 0;

        // Act
        var mapped = result
            .Bind<int>(v => { calls++; return Result<int>.Success(v + 1); })
            .Map(v => { calls++; return v * 2; })
            .Apply(_ => calls++)
            .Bind<int>(v => { calls++; return Result<int>.Success(v); });

        // Assert — селекторы мёртвой ветки не вызываются, исключение тот же объект
        calls.Should().Be(0);
        mapped.IsSuccess.Should().BeFalse();
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void BindToSameType_OnFail_ReturnsEquivalentResult()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);

        // Act
        var mapped = result.Bind(v => Result<int>.Success(v + 1));

        // Assert — Bind(T → Result<T>) на ошибке возвращает исходный результат
        mapped.Should().Be(result);
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void Map_OnSuccess_TransformsValue()
    {
        // Arrange
        var result = Result<int>.Success(21);

        // Act
        var mapped = result.Map(v => v * 2);

        // Assert
        mapped.Value.Should().Be(42);
    }

    [Fact]
    public void Apply_OnSuccess_RunsSideEffectAndKeepsResult()
    {
        // Arrange
        var result = Result<int>.Success(1);
        var seen = 0;

        // Act
        var applied = result.Apply(v => seen = v);

        // Assert
        seen.Should().Be(1);
        applied.Should().Be(result);
    }

    [Fact]
    public void Apply_OnFail_SkipsSideEffect()
    {
        // Arrange
        var result = Result<int>.Failed(Fail());
        var called = false;

        // Act
        var applied = result.Apply(_ => called = true);

        // Assert
        called.Should().BeFalse();
        applied.Should().Be(result);
    }

    [Fact]
    public void Match_FoldsBothOutcomes()
    {
        // Arrange
        var error = Fail();

        // Act
        var ok = Result<int>.Success(7).Match(v => $"ok:{v}", e => $"err:{e.Message}");
        var err = Result<int>.Failed(error).Match(v => $"ok:{v}", e => $"err:{e.Message}");

        // Assert
        ok.Should().Be("ok:7");
        err.Should().Be("err:boom");
    }

    // --- Throw ---

    [Fact]
    public void Throw_OnSuccess_ReturnsSameResult()
    {
        // Arrange
        var result = Result<int>.Success(3);

        // Act
        var returned = result.Throw();

        // Assert
        returned.Should().Be(result);
    }

    [Fact]
    public void Throw_OnFail_ThrowsSameException()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);

        // Act
        var act = () => result.Throw();

        // Assert
        act.Should().Throw<ApplicationException>().Which.Should().BeSameAs(error);
    }

    [Fact]
    public void UnitResult_Throw_OnFail_ThrowsSameException()
    {
        // Arrange
        var error = Fail();
        Result result = Result.Failed(error);

        // Act
        var act = () => result.Throw();

        // Assert
        act.Should().Throw<ApplicationException>().Which.Should().BeSameAs(error);
    }

    // --- Конверсии ---

    [Fact]
    public void Implicit_FromException_MakesFailed()
    {
        // Arrange
        var error = Fail();

        // Act
        Result<int> result = error;

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void Implicit_FromValue_MakesSuccess()
    {
        // Arrange
        // Act
        Result<string> result = "hello";

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("hello");
    }

    [Fact]
    public void Implicit_ToResult_KeepsOutcome()
    {
        // Arrange
        var error = Fail();

        // Act
        Result fromOk = Result<int>.Success(5);
        Result fromErr = Result<int>.Failed(error);

        // Assert — успех теряет значение, ошибка сохраняется
        fromOk.IsSuccess.Should().BeTrue();
        fromErr.IsSuccess.Should().BeFalse();
        fromErr.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void Implicit_ToException_OnFail_ReturnsSameException()
    {
        // Arrange
        var error = Fail();
        Result<int> result = error;

        // Act
        var converted = (Exception)result;

        // Assert
        converted.Should().BeSameAs(error);
    }

    [Fact]
    public void Implicit_ToException_OnSuccess_ThrowsNre()
    {
        // Arrange
        Result<int> result = Result<int>.Success(1);

        // Act
        var act = () => { var _ = (Exception)result; };

        // Assert
        act.Should().Throw<NullReferenceException>();
    }

    // --- From / FromAsync / FromValue ---

    [Fact]
    public void From_CapturesThrownException()
    {
        // Arrange
        var error = Fail();

        // Act
        var result = Result<int>.From(() => throw error);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public async Task FromAsync_CapturesThrownException()
    {
        // Arrange
        var error = Fail();

        // Act
        var result = await Result<int>.FromAsync(() => ValueTask.FromException<int>(error));

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void FromValue_Null_ReturnsApplicationException()
    {
        // Arrange
        string? value = null;

        // Act
        var result = Result.FromValue(value, "значение обязательно");

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ApplicationException>()
            .Which.Message.Should().Be("значение обязательно");
    }

    [Fact]
    public void FromValue_NotNull_ReturnsSuccess()
    {
        // Arrange
        const string value = "v";

        // Act
        var result = Result.FromValue(value, "err");

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(value);
    }

    // --- Next / CollBind ---

    [Fact]
    public void Next_OnFail_DoesNotMoveEnumerator()
    {
        // Arrange — IEnumerator явно: struct-энумератор List при передаче боксится
        var items = new List<int> { 1, 2 };
        IEnumerator<int> enumerator = items.GetEnumerator();
        enumerator.MoveNext();
        Result result = Result.Failed(Fail());

        // Act
        var moved = result.Next(enumerator);

        // Assert
        moved.Should().BeFalse();
        enumerator.Current.Should().Be(1); // не сдвинулся
    }

    [Fact]
    public void Next_OnSuccess_MovesEnumerator()
    {
        // Arrange
        var items = new List<int> { 1, 2 };
        IEnumerator<int> enumerator = items.GetEnumerator();
        Result result = Result.Success();

        // Act
        var moved = result.Next(enumerator);

        // Assert
        moved.Should().BeTrue();
        enumerator.Current.Should().Be(1);
    }

    [Fact]
    public void CollBind_StopsAfterFirstError()
    {
        // Arrange
        var error = Fail();
        var items = new[] { 1, 2, 3 };
        var processed = new List<int>();

        // Act
        var result = items.CollBind(i =>
        {
            processed.Add(i);
            return i == 2 ? Result<int>.Failed(error) : Result<int>.Success(i);
        });

        // Assert — fail-fast: третий элемент не обрабатывается, ошибка — та же
        processed.Should().BeEquivalentTo(new[] { 1, 2 });
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void CollBind_AllSuccess_ProcessesEverything()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };
        var sum = 0;

        // Act
        var result = items.CollBind(i =>
        {
            sum += i;
            return Result.Success();
        });

        // Assert
        result.IsSuccess.Should().BeTrue();
        sum.Should().Be(6);
    }

    // --- Async-комбинаторы ---

    [Fact]
    public async Task BindAsync_OnFail_ShortCircuitsWithoutSelector()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);
        var calls = 0;

        // Act
        var task = result.BindAsync<int>(v =>
        {
            calls++;
            return ValueTask.FromResult(Result<int>.Success(v));
        });

        // Assert — fail-ветка завершается синхронно, селектор не вызывается
        task.IsCompletedSuccessfully.Should().BeTrue();
        var mapped = await task;
        calls.Should().Be(0);
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public async Task BindAsync_OnSuccess_CallsSelector()
    {
        // Arrange
        var result = Result<int>.Success(5);

        // Act
        var mapped = await result.BindAsync(v =>
            ValueTask.FromResult(Result<string>.Success($"v={v}")));

        // Assert
        mapped.IsSuccess.Should().BeTrue();
        mapped.Value.Should().Be("v=5");
    }

    [Fact]
    public async Task MapAsync_OnFail_ShortCircuitsSameError()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);

        // Act
        var mapped = await result.MapAsync(v => ValueTask.FromResult(v * 2));

        // Assert
        mapped.IsSuccess.Should().BeFalse();
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public async Task ApplyAsync_OnFail_SkipsSideEffect()
    {
        // Arrange
        var result = Result<int>.Failed(Fail());
        var called = false;

        // Act
        var applied = await result.ApplyAsync(_ =>
        {
            called = true;
            return ValueTask.CompletedTask;
        });

        // Assert
        called.Should().BeFalse();
        applied.Should().Be(result);
    }

    [Fact]
    public async Task MatchAsync_FoldsBothOutcomes()
    {
        // Arrange — MatchAsync объявлен только у не-generic Result (как в классовой версии)
        var error = Fail();
        Result okResult = Result.Success();
        Result errResult = Result.Failed(error);

        // Act
        var ok = await okResult
            .MatchAsync(() => ValueTask.FromResult("ok"), e => ValueTask.FromResult($"err:{e.Message}"));
        var err = await errResult
            .MatchAsync(() => ValueTask.FromResult("ok"), e => ValueTask.FromResult($"err:{e.Message}"));

        // Assert
        ok.Should().Be("ok");
        err.Should().Be("err:boom");
    }

    // --- MapSuccessAsync / MapFailedAsync: семантика «чужой» ветки ---

    [Fact]
    public async Task MapSuccessAsync_OnFail_KeepsResultVariantsLegacyBehaviour()
    {
        // Arrange — зафиксированное поведение классовой версии: селектор чужой
        // ветки не вызывается, Result-вариант на ошибке возвращает Success()
        var error = Fail();
        var result = Result<int>.Failed(error);
        var selectorCalls = 0;

        // Act
        var unitVariant = await result.MapSuccessAsync(r =>
        {
            selectorCalls++;
            return ValueTask.FromResult(Result.Success());
        });

        // Assert
        selectorCalls.Should().Be(0);
        unitVariant.IsSuccess.Should().BeTrue(); // аномалия оригинала, сохранена

        // Act — T-вариант пробрасывает ошибку как есть
        var tVariant = await result.MapSuccessAsync(r => ValueTask.FromResult(r));

        // Assert
        tVariant.IsSuccess.Should().BeFalse();
        tVariant.Error.Should().BeSameAs(error);
    }

    [Fact]
    public async Task MapSuccessAsync_OnSuccess_CallsSelector()
    {
        // Arrange
        var result = Result<int>.Success(5);
        Result seen = Result.Failed(Fail());

        // Act
        var mapped = await result.MapSuccessAsync(r =>
        {
            seen = r;
            return ValueTask.FromResult(Result.Success());
        });

        // Assert — селектор получает сам результат
        seen.IsSuccess.Should().BeTrue();
        mapped.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task MapFailedAsync_OnSuccess_KeepsResultVariantsLegacyBehaviour()
    {
        // Arrange
        var result = Result<int>.Success(5);
        var selectorCalls = 0;

        // Act
        var unitVariant = await result.MapFailedAsync(r =>
        {
            selectorCalls++;
            return ValueTask.FromResult(Result.Success());
        });

        // Assert
        selectorCalls.Should().Be(0);
        unitVariant.IsSuccess.Should().BeTrue(); // успех пробрасывается, селектор чужой ветки не зовётся
    }

    [Fact]
    public async Task MapFailedAsync_OnFail_CallsSelectorWithSameError()
    {
        // Arrange
        var error = Fail();
        var result = Result<int>.Failed(error);
        Result<int> seen = Result<int>.Success(0);

        // Act
        var mapped = await result.MapFailedAsync(r =>
        {
            seen = r;
            return ValueTask.FromResult(Result<int>.Success(-1));
        });

        // Assert
        seen.Error.Should().BeSameAs(error);
        mapped.Value.Should().Be(-1);
    }

    [Fact]
    public async Task UnitResult_MapSuccessAndFailedAsync_DispatchByOutcome()
    {
        // Arrange
        var error = Fail();
        Result failed = Result.Failed(error);
        Result success = Result.Success();
        Result seenSuccess = Result.Failed(Fail());
        Result seenFailed = Result.Failed(Fail());

        // Act
        var fromOk = await success.MapSuccessAsync(
            (r, _) => { seenSuccess = r; return ValueTask.FromResult(Result.Success()); }, TestContext.Current.CancellationToken);
        var fromErr = await failed.MapFailedAsync(
            (r, _) => { seenFailed = r; return ValueTask.FromResult(Result.Success()); }, TestContext.Current.CancellationToken);

        // Assert
        seenSuccess.IsSuccess.Should().BeTrue();
        fromOk.IsSuccess.Should().BeTrue();
        seenFailed.Error.Should().BeSameAs(error);
        fromErr.IsSuccess.Should().BeTrue();
    }

    // --- Расширения над ValueTask ---

    [Fact]
    public async Task ValueTaskExtensions_PropagateFailure()
    {
        // Arrange
        var error = Fail();
        var calls = 0;
        ValueTask<Result<int>> task = ValueTask.FromResult(Result<int>.Failed(error));

        // Act
        var mapped = await task
            .Bind<int>(v =>
            {
                calls++;
                return Result<int>.Success(v + 1);
            })
            .Map(v =>
            {
                calls++;
                return v * 2;
            });

        // Assert
        calls.Should().Be(0);
        mapped.Error.Should().BeSameAs(error);
    }

    // --- Равенство ---

    [Fact]
    public void Equality_ByValue()
    {
        // Arrange
        var error = Fail();

        // Act / Assert — равенство по полям: значение и то же исключение
        Result<int>.Success(5).Should().Be(Result<int>.Success(5));
        Result<int>.Failed(error).Should().Be(Result<int>.Failed(error));
        Result<int>.Failed(Fail()).Should().NotBe(Result<int>.Failed(Fail()));
        default(Result<int>).Should().Be(Result<int>.Success(0));
    }

    private static Exception ExceptionWithStackTrace()
    {
        try
        {
            throw new InvalidOperationException("с трейсом");
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
