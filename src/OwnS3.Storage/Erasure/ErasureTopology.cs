namespace OwnS3.Storage.Erasure;

// Топология из конфига (канон 06 §1): N путей, setSize 4–16, parity 1..setSize/2.
public sealed record ErasureTopology(int DriveCount, int SetSize, int Parity)
{
    // Требует N % SetSize == 0 (валидация — Validate).
    public int SetCount => DriveCount / SetSize;

    // k = setSize − m.
    public int DataShards => SetSize - Parity;

    // Дефолт-авто: наибольший делитель N из {16, 8, 4}; 0 — нет делителя (отказ старта).
    public static int DefaultSetSize(int driveCount)
    {
        foreach (var size in new[] { 16, 8, 4 })
        {
            if (driveCount % size == 0)
                return size;
        }

        return 0;
    }

    // Валидации канона 06 §1: N >= 2, N % setSize == 0, setSize 4–16, parity 1..setSize/2.
    public void Validate()
    {
        if (DriveCount < 2)
            throw new InvalidOperationException($"EC-топология: число дисков {DriveCount} < 2");
        if (DriveCount % SetSize != 0)
            throw new InvalidOperationException($"EC-топология: {DriveCount} не делится на setSize {SetSize}");
        if (SetSize is < 4 or > 16)
            throw new InvalidOperationException($"EC-топология: setSize {SetSize} вне 4–16");
        if (Parity < 1 || Parity > SetSize / 2)
            throw new InvalidOperationException($"EC-топология: parity {Parity} вне 1..{SetSize / 2}");
    }
}
