namespace OwnS3.Storage.Erasure;

// GF(2^8) с полиномом 0x11D (как референс cmd/erasure-coding.go): таблицы
// exp(512)/log(256), умножение/деление/обратный, умножение среза на коэффициент.
// Статический класс: таблицы строятся один раз, потоки не влияют на состояние.
public static class GaloisField256
{
    // Порождающий полином поля — зафиксирован референсом (0x11D = 285, вне диапазона byte).
    public const int Polynomial = 0x11D;

    // Порядок мультипликативной группы: 255 ненулевых элементов.
    private const int GroupOrder = 255;

    private static readonly byte[] s_exp = BuildExpTable();
    private static readonly byte[] s_log = BuildLogTable(s_exp);

    public static byte[] ExpTable => (byte[])s_exp.Clone();

    public static byte[] LogTable => (byte[])s_log.Clone();

    // Умножение через log/exp-таблицы с нулевым коротким замыканием.
    public static byte Multiply(byte left, byte right)
    {
        if (left == 0 || right == 0)
        {
            return 0;
        }

        var logProduct = s_log[left] + s_log[right];
        if (logProduct >= GroupOrder)
        {
            logProduct -= GroupOrder;
        }

        return s_exp[logProduct];
    }

    public static byte Divide(byte left, byte right)
    {
        if (right == 0)
        {
            throw new DivideByZeroException("деление на нуль в GF(2^8)");
        }

        if (left == 0)
        {
            return 0;
        }

        var logQuotient = s_log[left] - s_log[right];
        if (logQuotient < 0)
        {
            logQuotient += GroupOrder;
        }

        return s_exp[logQuotient];
    }

    public static byte Inverse(byte value)
    {
        if (value == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "нуль не имеет обратного в GF(2^8)");
        }

        // exp[255 - log[v]] == v^(-1): log[v] + log[v^-1] == 255.
        return s_exp[GroupOrder - s_log[value]];
    }

    // data[i] *= coefficient — цикл по срезу (используется кодеком при приведении матриц).
    public static void MultiplyInPlace(byte coefficient, Span<byte> data)
    {
        if (coefficient == 0)
        {
            data.Clear();
            return;
        }

        if (coefficient == 1)
        {
            return;
        }

        for (var i = 0; i < data.Length; i++)
        {
            data[i] = Multiply(coefficient, data[i]);
        }
    }

    // Таблица экспонент с периодом 255 (порядок мультипликативной группы):
    // exp[i] = x^i; хвост exp[255..511] — повторение с периодом 255 для сложения
    // логарифмов без ветвления (сумма логарифмов до 508).
    private static byte[] BuildExpTable()
    {
        var exp = new byte[512];
        int x = 1;
        for (var i = 0; i < GroupOrder; i++)
        {
            exp[i] = (byte)x;
            x <<= 1;
            if (x >= 256)
            {
                // редукция по порождающему полиному 0x11D (модуль 0x100 поглощается XOR)
                x ^= Polynomial;
            }
        }

        for (var i = GroupOrder; i < exp.Length; i++)
        {
            exp[i] = exp[i - GroupOrder];
        }

        return exp;
    }

    private static byte[] BuildLogTable(byte[] exp)
    {
        var log = new byte[256];
        for (var i = 0; i < GroupOrder; i++)
        {
            log[exp[i]] = (byte)i;
        }

        // log[0] не определён; остаётся 0 и никогда не читается (нулевое замыкание в Multiply/Divide).
        return log;
    }
}
