using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace Shared.Core;

// Результат операции: успех (Error == null) либо ошибка. Значимый тип — создание
// и цепочки Bind/Map/Apply не аллоцируют; комбинаторы ветвятся напрямую (раннее
// ветвление), пробрасывая исходный Exception без пересоздания.
public readonly record struct Result(Exception? Error = null)
{
    public bool IsSuccess => Error == null;

    public static Result From(Action action)
    {
        try
        {
            action();
            return Success();
        }
        catch (Exception e)
        {
            return e;
        }
    }

    public static async ValueTask<Result> FromAsync(Func<ValueTask> action)
    {
        try
        {
            await action();
            return Success();
        }
        catch (Exception e)
        {
            return e;
        }
    }

    public static Result Success() => default;

    public static Result Failed(Exception error)
        => error.StackTrace == null
            ? new Result(ExceptionDispatchInfo.SetCurrentStackTrace(error))
            : new Result(error);

    public static implicit operator Exception(Result r)
        => r.Error ?? throw new NullReferenceException();

    public static implicit operator Result(Exception e)
        => Failed(e);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result Bind(Func<Result> func)
        => IsSuccess ? func() : this;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result<T> Bind<T>(Func<Result<T>> func)
        => IsSuccess ? func() : Result<T>.Failed(Error!);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result<T> Map<T>(Func<T> func)
        => IsSuccess ? Result<T>.Success(func()) : Result<T>.Failed(Error!);

    public Result Apply(Action action)
    {
        if (IsSuccess)
        {
            action();
        }

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T Match<T>(Func<T> onSuccess, Func<Exception, T> onFailure)
        => IsSuccess ? onSuccess() : onFailure(Error!);

    // Свёртка без async-машины: обе ветки уже возвращают ValueTask
    public ValueTask<T> MatchAsync<T>(Func<ValueTask<T>> onSuccess, Func<Exception, ValueTask<T>> onFailure)
        => IsSuccess ? onSuccess() : onFailure(Error!);

    public Result Throw()
        => IsSuccess ? this : throw Error!;

    // «Итератор с коротким замыканием»: на ошибке не двигаем перечислитель
    public bool Next<T>(IEnumerator<T> enumerator)
        => IsSuccess && enumerator.MoveNext();

    // Async-комбинаторы: fail-ветка возвращается синхронно (completed ValueTask),
    // без входа в async-состояние
    public ValueTask<Result> BindAsync(Func<ValueTask<Result>> func)
        => IsSuccess ? func() : ValueTask.FromResult(Failed(Error!));

    public ValueTask<Result<T>> BindAsync<T>(Func<ValueTask<Result<T>>> func)
        => IsSuccess ? func() : ValueTask.FromResult(Result<T>.Failed(Error!));

    public async ValueTask<Result<T>> MapAsync<T>(Func<ValueTask<T>> func)
        => IsSuccess ? Result<T>.Success(await func()) : Result<T>.Failed(Error!);

    public async ValueTask<Result> ApplyAsync(Func<ValueTask> action)
    {
        if (IsSuccess)
        {
            await action();
        }

        return this;
    }

    public static Result<T> FromValue<T>(T? value, string error)
        where T : class
        => value == null
            ? Result<T>.Failed(new ApplicationException(error))
            : Result<T>.Success(value);

    // Поведение унаследовано от классовой версии: селектор «чужой» ветки не
    // вызывается, а Result-варианты на ошибке возвращают Success()
    public ValueTask<Result> MapSuccessAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => IsSuccess ? func(this, ct) : ValueTask.FromResult(Success());

    public ValueTask<Result> MapFailedAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => IsSuccess ? ValueTask.FromResult(this) : func(this, ct);
}

// См. Result: то же для команд/запросов со значением. Value валиден только при
// IsSuccess; на ошибке — default.
public readonly record struct Result<T>(T Value = default!, Exception? Error = null)
{
    public bool IsSuccess => Error == null;

    public static Result<T> From(Func<T> func)
    {
        try
        {
            return Success(func());
        }
        catch (Exception e)
        {
            return e;
        }
    }

    public static async ValueTask<Result<T>> FromAsync(Func<ValueTask<T>> action)
    {
        try
        {
            return Success(await action());
        }
        catch (Exception e)
        {
            return e;
        }
    }

    public static Result<T> Success(T value) => new(Value: value);

    public static Result<T> Failed(Exception error)
        => error.StackTrace == null
            ? new Result<T>(Error: ExceptionDispatchInfo.SetCurrentStackTrace(error))
            : new Result<T>(Error: error);

    public static implicit operator Exception(Result<T> r)
        => r.Error ?? throw new NullReferenceException();

    public static implicit operator Result<T>(Exception e)
        => Failed(e);

    public static implicit operator Result<T>(T value)
        => Success(value);

    public static implicit operator Result(Result<T> r)
        => r.IsSuccess ? Result.Success() : Result.Failed(r.Error!);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result Bind(Func<T, Result> func)
        => IsSuccess ? func(Value) : Result.Failed(Error!);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result<T> Bind(Func<T, Result<T>> func)
        => IsSuccess ? func(Value) : this;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result<TU> Bind<TU>(Func<T, Result<TU>> func)
        => IsSuccess ? func(Value) : Result<TU>.Failed(Error!);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Result<TU> Map<TU>(Func<T, TU> func)
        => IsSuccess ? Result<TU>.Success(func(Value)) : Result<TU>.Failed(Error!);

    public Result<T> Apply(Action<T> action)
    {
        if (IsSuccess)
        {
            action(Value);
        }

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TU Match<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onFailure)
        => IsSuccess ? onSuccess(Value) : onFailure(Error!);

    public Result<T> Throw()
        => IsSuccess ? this : throw Error!;

    public ValueTask<Result> BindAsync(Func<T, ValueTask<Result>> func)
        => IsSuccess ? func(Value) : ValueTask.FromResult(Result.Failed(Error!));

    public ValueTask<Result<T>> BindAsync(Func<T, ValueTask<Result<T>>> func)
        => IsSuccess ? func(Value) : ValueTask.FromResult(this);

    public ValueTask<Result<TU>> BindAsync<TU>(Func<T, ValueTask<Result<TU>>> func)
        => IsSuccess ? func(Value) : ValueTask.FromResult(Result<TU>.Failed(Error!));

    public async ValueTask<Result<TU>> MapAsync<TU>(Func<T, ValueTask<TU>> func)
        => IsSuccess ? Result<TU>.Success(await func(Value)) : Result<TU>.Failed(Error!);

    public async ValueTask<Result<T>> ApplyAsync(Func<T, ValueTask> action)
    {
        if (IsSuccess)
        {
            await action(Value);
        }

        return this;
    }

    // Поведение унаследовано от классовой версии: на «чужом» исходе Result-варианты
    // возвращают Success(), T-варианты — исходный результат
    public ValueTask<Result<T>> MapSuccessAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => IsSuccess ? func(this) : ValueTask.FromResult(this);

    public ValueTask<Result<T>> MapFailedAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => IsSuccess ? ValueTask.FromResult(this) : func(this);

    public ValueTask<Result> MapSuccessAsync(Func<Result<T>, ValueTask<Result>> func)
        => IsSuccess ? func(this) : ValueTask.FromResult(Result.Success());

    public ValueTask<Result> MapFailedAsync(Func<Result<T>, ValueTask<Result>> func)
        => IsSuccess ? ValueTask.FromResult(Result.Success()) : func(this);
}

public static class ResultExtensions
{
    extension(ValueTask<Result> r)
    {
        public async ValueTask<Result> Bind(Func<Result> func)
            => (await r).Bind(func);

        public async ValueTask<Result<T>> Bind<T>(Func<Result<T>> func)
            => (await r).Bind(func);

        public async ValueTask<Result<T>> Map<T>(Func<T> func)
            => (await r).Map(func);

        public async ValueTask<Result> Apply(Action action)
            => (await r).Apply(action);

        public async ValueTask<T> Match<T>(Func<T> onSuccess, Func<Exception, T> onFailure)
            => (await r).Match(onSuccess, onFailure);

        public async ValueTask<T> MatchAsync<T>(Func<ValueTask<T>> onSuccess, Func<Exception, ValueTask<T>> onFailure)
            => await (await r).MatchAsync(onSuccess, onFailure);

        public async ValueTask<Result> Throw()
            => (await r).Throw();

        public async ValueTask<Result> BindAsync(Func<ValueTask<Result>> func)
            => await (await r).BindAsync(func);

        public async ValueTask<Result<T>> BindAsync<T>(Func<ValueTask<Result<T>>> func)
            => await (await r).BindAsync(func);

        public async ValueTask<Result<T>> MapAsync<T>(Func<ValueTask<T>> func)
            => await (await r).MapAsync(func);

        public async ValueTask<Result> ApplyAsync(Func<ValueTask> action)
            => await (await r).ApplyAsync(action);

        public async ValueTask<Result> MapSuccessAsync(
            Func<Result, CancellationToken, ValueTask<Result>> func,
            CancellationToken ct)
            => await (await r).MapSuccessAsync(func, ct);

        public async ValueTask<Result> MapFailedAsync(
            Func<Result, CancellationToken, ValueTask<Result>> func,
            CancellationToken ct)
            => await (await r).MapFailedAsync(func, ct);
    }

    extension<T>(ValueTask<Result<T>> r)
    {
        public async ValueTask<Result> Bind(Func<T, Result> func)
            => (await r).Bind(func);

        public async ValueTask<Result<T>> Bind(Func<T, Result<T>> func)
            => (await r).Bind(func);

        public async ValueTask<Result<TU>> Bind<TU>(Func<T, Result<TU>> func)
            => (await r).Bind(func);

        public async ValueTask<Result<TU>> Map<TU>(Func<T, TU> func)
            => (await r).Map(func);

        public async ValueTask<Result<T>> Apply(Action<T> action)
            => (await r).Apply(action);

        public async ValueTask<TU> Match<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onFailure)
            => (await r).Match(onSuccess, onFailure);

        public async ValueTask<Result<T>> Throw()
            => (await r).Throw();

        public async ValueTask<Result> BindAsync(Func<T, ValueTask<Result>> func)
            => await (await r).BindAsync(func);

        public async ValueTask<Result<T>> BindAsync(Func<T, ValueTask<Result<T>>> func)
            => await (await r).BindAsync(func);

        public async ValueTask<Result<TU>> BindAsync<TU>(Func<T, ValueTask<Result<TU>>> func)
            => await (await r).BindAsync(func);

        public async ValueTask<Result<TU>> MapAsync<TU>(Func<T, ValueTask<TU>> func)
            => await (await r).MapAsync(func);

        public async ValueTask<Result<T>> ApplyAsync(Func<T, ValueTask> action)
            => await (await r).ApplyAsync(action);

        public async ValueTask<Result<T>> MapSuccessAsync(Func<Result<T>, ValueTask<Result<T>>> func)
            => await (await r).MapSuccessAsync(func);

        public async ValueTask<Result<T>> MapFailedAsync(Func<Result<T>, ValueTask<Result<T>>> func)
            => await (await r).MapFailedAsync(func);

        public async ValueTask<Result> MapSuccessAsync(Func<Result<T>, ValueTask<Result>> func)
            => await (await r).MapSuccessAsync(func);

        public async ValueTask<Result> MapFailedAsync(Func<Result<T>, ValueTask<Result>> func)
            => await (await r).MapFailedAsync(func);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result CollBind<T>(this IEnumerable<T> items, Func<T, Result> map)
    {
        var result = Result.Success();
        using var enumerator = items.GetEnumerator();
        while (result.Next(enumerator))
        {
            result = map(enumerator.Current);
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async ValueTask<Result> CollBindAsync<T>(
        this IEnumerable<T> items,
        Func<T, CancellationToken, ValueTask<Result>> map,
        CancellationToken ct = default)
    {
        var result = Result.Success();
        using var enumerator = items.GetEnumerator();
        while (result.Next(enumerator))
        {
            result = await map(enumerator.Current, ct);
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result CollBind<T>(this Result result, IEnumerable<T> items, Func<T, Result> map)
    {
        using var enumerator = items.GetEnumerator();
        while (result.Next(enumerator))
        {
            result = map(enumerator.Current);
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async ValueTask<Result> CollBindAsync<T>(
        this Result result,
        IEnumerable<T> items,
        Func<T, CancellationToken, ValueTask<Result>> map,
        CancellationToken ct = default)
    {
        using var enumerator = items.GetEnumerator();
        while (result.Next(enumerator))
        {
            result = await map(enumerator.Current, ct);
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async ValueTask<Result> CollBind<T>(
        this ValueTask<Result> sresult,
        IEnumerable<T> items,
        Func<T, Result> map)
    {
        var result = await sresult;
        return result.CollBind(items, map);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async ValueTask<Result> CollBindAsync<T>(
        this ValueTask<Result> sresult,
        IEnumerable<T> items,
        Func<T, CancellationToken, ValueTask<Result>> map,
        CancellationToken ct = default)
    {
        var result = await sresult;
        return await result.CollBindAsync(items, map, ct);
    }
}
