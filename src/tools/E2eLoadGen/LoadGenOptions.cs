using System.Globalization;

namespace PgWorker.IntegrationTests.E2eLoadGen;

/// <summary>Аргументы генератора (t24, spec §5.1: управление — аргументы
/// запуска): -k/--count — контуров (1..8); -p/--profile — dns|cpu|io|full;
/// -d/--duration — минуты (0.05..480); -o/--out — каталог лога. Невалидное —
/// ApplicationException с usage (Program печатает в stderr, exit 2).</summary>
public sealed record LoadGenOptions(int Count, string Profile, double DurationMinutes, string OutDir)
{
    public const string ProfileDns = "dns";
    public const string ProfileCpu = "cpu";
    public const string ProfileIo = "io";
    public const string ProfileFull = "full";

    private const string Usage =
        "usage: e2eloadgen [-k count=1] [-p dns|cpu|io|full=full]"
        + " [-d minutes=10] [-o outdir=/tmp/pgw-noise-<guid>]";

    public TimeSpan Duration => TimeSpan.FromMinutes(DurationMinutes);

    public static LoadGenOptions Parse(string[] args)
    {
        var count = 1;
        var profile = ProfileFull;
        var duration = 10.0;
        var outDir = $"/tmp/pgw-noise-{Guid.NewGuid():N}";
        for (var i = 0; i < args.Length; i++)
        {
            var flag = args[i];
            string Value()
            {
                if (i + 1 >= args.Length)
                    throw new ApplicationException($"нет значения для {flag}\n{Usage}");
                return args[++i];
            }

            switch (flag)
            {
                case "-k":
                case "--count":
                    if (!int.TryParse(Value(), CultureInfo.InvariantCulture, out count) || count is < 1 or > 8)
                        throw new ApplicationException($"-k: ожидано целое 1..8\n{Usage}");
                    break;
                case "-p":
                case "--profile":
                    profile = Value();
                    if (profile is not (ProfileDns or ProfileCpu or ProfileIo or ProfileFull))
                        throw new ApplicationException($"-p: неизвестный профиль '{profile}'\n{Usage}");
                    break;
                case "-d":
                case "--duration":
                    if (!double.TryParse(Value(), CultureInfo.InvariantCulture, out duration)
                        || duration is < 0.05 or > 480)
                        throw new ApplicationException($"-d: ожиданы минуты 0.05..480\n{Usage}");
                    break;
                case "-o":
                case "--out":
                    outDir = Value();
                    break;
                default:
                    throw new ApplicationException($"неизвестный аргумент '{flag}'\n{Usage}");
            }
        }

        return new LoadGenOptions(count, profile, duration, outDir);
    }
}
