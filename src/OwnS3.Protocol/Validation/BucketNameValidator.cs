namespace OwnS3.Protocol.Validation;

// Валидация имени бакета (arch/owns3/03 §6): 3–63 символа, [a-z0-9-],
// края — буква или цифра.
public static class BucketNameValidator
{
    public static bool IsValid(string name)
    {
        if (name.Length is < 3 or > 63)
            return false;
        if (!char.IsAsciiLetterOrDigit(name[0]) || !char.IsAsciiLetterOrDigit(name[^1]))
            return false;
        foreach (var c in name)
        {
            if (c is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-')
                return false;
        }
        return true;
    }
}
