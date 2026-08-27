namespace AdminPanel.Infrastructure.DI;

public abstract class DiTypeBehaviour
{
    public virtual void Handle(IReadOnlyCollection<Type> types)
    {
        foreach (var type in types.Where(Filter))
        {
            Handle(type);
        }
    }

    protected abstract bool Filter(Type type);

    protected abstract void Handle(Type type);

    protected static Result<T> GetAttribute<T>(Type type)
        where T : Attribute
        => Result.FromValue(
            type.GetCustomAttributes(false).OfType<T>().FirstOrDefault(),
            $"Type {type.Name} does not have the attribute {typeof(T).Name}");
}