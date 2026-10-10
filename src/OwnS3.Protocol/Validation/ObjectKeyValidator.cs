using System.Text;

namespace OwnS3.Protocol.Validation;

// Валидация ключа объекта (arch/owns3/03 §6): непустой, <= 1024 байт UTF-8.
public static class ObjectKeyValidator
{
    public static bool IsValid(string key) =>
        key.Length > 0 && Encoding.UTF8.GetByteCount(key) <= 1024;
}
