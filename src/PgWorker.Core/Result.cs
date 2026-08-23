using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace PgWorker.Core;

public abstract record Result(Exception? Error = null)
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

    public static Result Success()
        => ResultSuccess.Instance;

    public static Result Failed(Exception error)
        => error.StackTrace == null
            ? new ResultError(ExceptionDispatchInfo.SetCurrentStackTrace(error))
            : new ResultError(error);

    public static implicit operator Exception(Result r)
        => r.Error ?? throw new NullReferenceException();

    public static implicit operator Result(Exception e)
        => Failed(e);

    public abstract Result Bind(Func<Result> func);

    public abstract Result<T> Bind<T>(Func<Result<T>> func);

    public abstract Result<T> Map<T>(Func<T> func);

    public abstract Result Apply(Action action);

    public abstract T Match<T>(Func<T> onSuccess, Func<Exception, T> onFailure);

    public abstract ValueTask<T> MatchAsync<T>(Func<ValueTask<T>> onSuccess, Func<Exception, ValueTask<T>> onFailure);

    public abstract Result Throw();

    public abstract bool Next<T>(IEnumerator<T> enumerator);

    public abstract ValueTask<Result> BindAsync(Func<ValueTask<Result>> func);

    public abstract ValueTask<Result<T>> BindAsync<T>(Func<ValueTask<Result<T>>> func);

    public abstract ValueTask<Result<T>> MapAsync<T>(Func<ValueTask<T>> func);

    public abstract ValueTask<Result> ApplyAsync(Func<ValueTask> action);

    public static Result<T> FromValue<T>(T? value, string error)
        where T : class
        => value == null
            ? new ResultError<T>(new ApplicationException(error))
            : new ResultSuccess<T>(value);

    public abstract ValueTask<Result> MapSuccessAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct);

    public abstract ValueTask<Result> MapFailedAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct);
}

public record ResultSuccess : Result
{
    public static readonly ResultSuccess Instance = new();

    private ResultSuccess()
    {
    }

    public override Result Bind(Func<Result> func)
        => func();

    public override Result<T> Bind<T>(Func<Result<T>> func)
        => func();

    public override Result<T> Map<T>(Func<T> func)
        => ResultSuccess<T>.Success(func());

    public override Result Apply(Action action)
    {
        action();
        return this;
    }

    public override T Match<T>(Func<T> onSuccess, Func<Exception, T> onFailure)
        => onSuccess();

    public override ValueTask<T> MatchAsync<T>(Func<ValueTask<T>> onSuccess, Func<Exception, ValueTask<T>> onFailure)
        => onSuccess();

    public override Result Throw()
        => this;

    public override bool Next<T>(IEnumerator<T> enumerator)
        => enumerator.MoveNext();

    public override ValueTask<Result> BindAsync(Func<ValueTask<Result>> func)
        => func();

    public override ValueTask<Result<T>> BindAsync<T>(Func<ValueTask<Result<T>>> func)
        => func();

    public override async ValueTask<Result<T>> MapAsync<T>(Func<ValueTask<T>> func)
        => ResultSuccess<T>.Success(await func());

    public override async ValueTask<Result> ApplyAsync(Func<ValueTask> action)
    {
        await action();
        return this;
    }

    public override ValueTask<Result> MapSuccessAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => func(this, ct);

    public override ValueTask<Result> MapFailedAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => new(this);
}

public record ResultError(Exception Error) : Result(Error)
{
    public override Result Bind(Func<Result> func)
        => this;

    public override Result<T> Bind<T>(Func<Result<T>> func)
        => Result<T>.Failed(Error!);

    public override Result<T> Map<T>(Func<T> func)
        => Result<T>.Failed(Error!);

    public override Result Apply(Action action)
        => this;

    public override T Match<T>(Func<T> onSuccess, Func<Exception, T> onFailure)
        => onFailure(Error!);

    public override ValueTask<T> MatchAsync<T>(Func<ValueTask<T>> onSuccess, Func<Exception, ValueTask<T>> onFailure)
        => onFailure(Error!);

    public override Result Throw()
        => throw Error!;

    public override bool Next<T>(IEnumerator<T> enumerator)
        => false;

    public override ValueTask<Result> BindAsync(Func<ValueTask<Result>> func)
        => new(Error!);

    public override ValueTask<Result<T>> BindAsync<T>(Func<ValueTask<Result<T>>> func)
        => new(Error!);

    public override ValueTask<Result<T>> MapAsync<T>(Func<ValueTask<T>> func)
        => new(Error!);

    public override ValueTask<Result> ApplyAsync(Func<ValueTask> action)
        => new(this);

    public override ValueTask<Result> MapSuccessAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => new(this);

    public override ValueTask<Result> MapFailedAsync(
        Func<Result, CancellationToken, ValueTask<Result>> func,
        CancellationToken ct)
        => func(this, ct);
}

public abstract record Result<T>(T Value, Exception? Error = null)
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

    public static Result<T> Success(T value)
        => new ResultSuccess<T>(value);

    public static Result<T> Failed(Exception error)
        => error.StackTrace == null
            ? new ResultError<T>(ExceptionDispatchInfo.SetCurrentStackTrace(error))
            : new ResultError<T>(error);

    public static implicit operator Exception(Result<T> r)
        => r.Error ?? throw new NullReferenceException();

    public static implicit operator Result<T>(Exception e)
        => Failed(e);

    public static implicit operator Result<T>(T value)
        => Success(value);

    public static implicit operator Result(Result<T> r)
    {
        return r.Bind(_ => Result.Success());
    }

    public abstract Result Bind(Func<T, Result> func);

    public abstract Result<T> Bind(Func<T, Result<T>> func);

    public abstract Result<TU> Bind<TU>(Func<T, Result<TU>> func);

    public abstract Result<TU> Map<TU>(Func<T, TU> func);

    public abstract Result<T> Apply(Action<T> action);

    public abstract TU Match<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onFailure);

    public abstract Result<T> Throw();

    public abstract ValueTask<Result> BindAsync(Func<T, ValueTask<Result>> func);

    public abstract ValueTask<Result<T>> BindAsync(Func<T, ValueTask<Result<T>>> func);

    public abstract ValueTask<Result<TU>> BindAsync<TU>(Func<T, ValueTask<Result<TU>>> func);

    public abstract ValueTask<Result<TU>> MapAsync<TU>(Func<T, ValueTask<TU>> func);

    public abstract ValueTask<Result<T>> ApplyAsync(Func<T, ValueTask> action);

    public abstract ValueTask<Result<T>> MapSuccessAsync(Func<Result<T>, ValueTask<Result<T>>> func);

    public abstract ValueTask<Result<T>> MapFailedAsync(Func<Result<T>, ValueTask<Result<T>>> func);

    public abstract ValueTask<Result> MapSuccessAsync(Func<Result<T>, ValueTask<Result>> func);

    public abstract ValueTask<Result> MapFailedAsync(Func<Result<T>, ValueTask<Result>> func);
}

public record ResultSuccess<T>(T Value) : Result<T>(Value)
{
    public override Result<T> Throw()
        => this;

    public override ValueTask<Result> BindAsync(Func<T, ValueTask<Result>> func)
        => func(Value);

    public override ValueTask<Result<T>> BindAsync(Func<T, ValueTask<Result<T>>> func)
        => func(Value);

    public override ValueTask<Result<TU>> BindAsync<TU>(Func<T, ValueTask<Result<TU>>> func)
        => func(Value);

    public override async ValueTask<Result<TU>> MapAsync<TU>(Func<T, ValueTask<TU>> func)
        => Result<TU>.Success(await func(Value));

    public override async ValueTask<Result<T>> ApplyAsync(Func<T, ValueTask> action)
    {
        await action(Value);
        return this;
    }

    public override ValueTask<Result<T>> MapSuccessAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => func(this);

    public override ValueTask<Result<T>> MapFailedAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => new(this);

    public override ValueTask<Result> MapSuccessAsync(Func<Result<T>, ValueTask<Result>> func)
        => func(this);

    public override ValueTask<Result> MapFailedAsync(Func<Result<T>, ValueTask<Result>> func)
        => new(Result.Success());

    public override Result Bind(Func<T, Result> func)
        => func(Value);

    public override Result<T> Bind(Func<T, Result<T>> func)
        => func(Value);

    public override Result<TU> Bind<TU>(Func<T, Result<TU>> func)
        => func(Value);

    public override Result<TU> Map<TU>(Func<T, TU> func)
        => Result<TU>.Success(func(Value));

    public override Result<T> Apply(Action<T> action)
    {
        action(Value);
        return this;
    }

    public override TU Match<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onFailure)
        => onSuccess(Value);
}

public record ResultError<T>(Exception Error) : Result<T>(default!, Error)
{
    public override Result<T> Throw()
        => throw Error!;

    public override ValueTask<Result> BindAsync(Func<T, ValueTask<Result>> func)
        => new(Result.Failed(Error!));

    public override ValueTask<Result<T>> BindAsync(Func<T, ValueTask<Result<T>>> func)
        => new(this);

    public override ValueTask<Result<TU>> BindAsync<TU>(Func<T, ValueTask<Result<TU>>> func)
        => new(Result<TU>.Failed(Error!));

    public override ValueTask<Result<TU>> MapAsync<TU>(Func<T, ValueTask<TU>> func)
        => new(Result<TU>.Failed(Error!));

    public override ValueTask<Result<T>> ApplyAsync(Func<T, ValueTask> action)
        => new(this);

    public override ValueTask<Result<T>> MapSuccessAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => new(this);

    public override ValueTask<Result<T>> MapFailedAsync(Func<Result<T>, ValueTask<Result<T>>> func)
        => func(this);

    public override ValueTask<Result> MapSuccessAsync(Func<Result<T>, ValueTask<Result>> func)
        => new(Result.Success());

    public override ValueTask<Result> MapFailedAsync(Func<Result<T>, ValueTask<Result>> func)
        => func(this);

    public override Result Bind(Func<T, Result> func)
        => Result.Failed(Error!);

    public override Result<T> Bind(Func<T, Result<T>> func)
        => this;

    public override Result<TU> Bind<TU>(Func<T, Result<TU>> func)
        => Result<TU>.Failed(Error!);

    public override Result<TU> Map<TU>(Func<T, TU> func)
        => Result<TU>.Failed(Error!);

    public override Result<T> Apply(Action<T> action)
        => this;

    public override TU Match<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onFailure)
        => onFailure(Error!);
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