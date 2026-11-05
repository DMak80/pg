using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Итог GFS-отбора (arch/backups/05-etcd-contract.md): Keep — id оставляемых COMPLETED,
/// Delete — кандидаты на удаление в порядке исполнения (verify-FAILED первыми,
/// далее по возрастанию started_unix — место освобождается от самого старого).</summary>
public sealed record RetentionSelection(
    IReadOnlySet<string> Keep,
    IReadOnlyList<string> Delete);

/// <summary>Вердикт занятости хранилища: OK/WARN/CRIT относительно квоты.</summary>
public enum StorageState
{
    Ok,
    Warn,
    Crit,
}

/// <summary>Результат EvaluateStorage: state + фактические числа; UsedPercent —
/// null при незаданной квоте.</summary>
public sealed record StorageVerdict(
    StorageState State, long UsedBytes, long QuotaBytes, double? UsedPercent);

/// <summary>Чистые функции ретенции (arch/backups/05-etcd-contract.md/§5): без I/O, момент —
/// аргументом (TimeProvider не нужен). Полное юнит-покрытие — риск «удалили
/// нужное» закрывается тестами детерминированности (spec §2 п.5).</summary>
public static partial class RetentionPlanner
{
    /// <summary>GFS-отбор по календарю UTC (spec §3.1): дневные — все COMPLETED
    /// последних retention.days календарных суток (включая текущую); недельные —
    /// последний COMPLETED каждой из retention.weeks свежейших предыдущих
    /// ISO-недель; месячные — аналогично по календарным месяцам. Активные
    /// (PLANNED/RUNNING/UPLOADING) и DELETING в отборе не участвуют. Guard:
    /// самый свежий COMPLETED — всегда в Keep; если после отбора Delete непусто,
    /// а Keep пуст — старейший кандидат возвращается в Keep.</summary>
    public static RetentionSelection SelectKeep(
        IReadOnlyList<FullBackupState> fulls, BackupPolicy policy, long nowUnix)
    {
        var now = DateTimeOffset.FromUnixTimeSeconds(nowUnix).UtcDateTime;
        var completed = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .OrderBy(f => f.StartedUnix)
            .ToList();

        var keep = new HashSet<string>();

        // Дневные: все COMPLETED последних retention.days календарных суток UTC
        // (окно [today − (days−1) .. today] по дате started_unix).
        var dayCutoff = DateOnly.FromDateTime(now).AddDays(-(policy.RetentionDays - 1));
        var dailyWindow = completed
            .Where(f => DateOnly.FromDateTime(
                DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime) >= dayCutoff)
            .ToList();
        foreach (var f in dailyWindow)
            keep.Add(f.Id);

        // Недельные: полные ВНЕ ТЕКУЩЕЙ ISO-недели группируются по ISO-неделе UTC;
        // из каждой из retention.weeks предыдущих свежайших групп — последний.
        AddCalendarPoint(completed, keep, policy.RetentionWeeks, ISOWeekYearOf, now);

        // Месячные: аналогично по календарному месяцу UTC.
        AddCalendarPoint(completed, keep, policy.RetentionMonths, MonthOf, now);

        var freshest = completed.MaxBy(f => f.StartedUnix);
        if (freshest is not null)
            keep.Add(freshest.Id); // guard: самый свежий COMPLETED — всегда

        var delete = completed
            .Where(f => !keep.Contains(f.Id))
            .OrderBy(f => f.Verify is { State: BackupVerifyStatus.Failed } ? 0 : 1)
            .ThenBy(f => f.StartedUnix)
            .Select(f => f.Id)
            .ToList();

        // Guard-доводка: удалять нечего, если не остаётся ни одного COMPLETED
        // (последний валидный не удаляется — spec §3.1, AC2).
        if (delete.Count > 0 && keep.Count == 0)
        {
            var rescued = delete[0];
            keep.Add(rescued);
            delete.RemoveAt(0);
        }

        return new RetentionSelection(keep, delete);
    }

    /// <summary>Точка отсчёта WAL-ретенции и контроля цепочки (arch/backups/README.md
    /// §4 п.5/§3): wal_start новейшего (по started_unix) COMPLETED-полного с
    /// verify.state=OK — ОДНА точка для cutoff-чистки WAL и chain_start.
    /// verify-OK-полного нет (PENDING/FAILED/verify отсутствует) → null:
    /// безопасной точки привязки нет, прунинг WAL запрещён. Чистая функция.
    /// Дефективный wal_start кандидата (ранний FAILED-хвост/ручная правка)
    /// пропускается — берётся следующий OK ниже.</summary>
    public static WalFileName? LatestVerifiedWalStart(IReadOnlyList<FullBackupState> fulls)
        => fulls
            .Where(f => f.State == FullBackupStatus.Completed
                        && f.Verify is { State: BackupVerifyStatus.Ok })
            .Select(f => (f.StartedUnix, Start: WalFileName.TryParse(f.WalStartSegment ?? "")))
            .Where(x => x.Start is not null)
            .OrderByDescending(x => x.StartedUnix)
            .Select(x => x.Start)
            .Cast<WalFileName?>()
            .FirstOrDefault();

    /// <summary>Самый старый ОСТАВЛЯЕМЫЙ COMPLETED-полный по (TLI, позиции)
    /// wal_start (нижняя граница WAL-чистки, arch/backups/processes/
    /// c-retention.md §2): цепочка полного идёт от его wal_start через все
    /// TLI-переходы, поэтому объекты TLI выше этой границы не удаляются,
    /// пока полный жив. null — оставляемых полных с wal_start нет (чистка
    /// ниже cutoff небезопасна). Чистая функция.</summary>
    public static WalFileName? EarliestKeptWalStart(IReadOnlyList<FullBackupState> kept)
        => kept
            .Where(f => f.State == FullBackupStatus.Completed)
            .Select(f => WalFileName.TryParse(f.WalStartSegment ?? ""))
            .Where(w => w is not null)
            .Select(w => w!.Value)
            .OrderBy(w => w.Tli)
            .ThenBy(w => (long)w.Log * WalFileName.SegsPerLog + w.Seg)
            .Cast<WalFileName?>()
            .FirstOrDefault();

    /// <summary>План чистки WAL (arch/backups/processes/c-retention.md §2):
    /// сегменты строго ниже cutoff (TLI ниже ИЛИ тот же TLI с позицией
    /// log·256+seg ниже cutoff). С нижней границей oldestKeptStart (wal_start
    /// самого старого оставляемого полного) удаляются только объекты вне
    /// цепочек оставляемых полных: строго ниже oldest (TLI/позиция) и
    /// сегменты cutoff-TLI ниже cutoff; объекты TLI между oldest и cutoff —
    /// живут (restore по старому полному идёт через их TLI-переходы).
    /// `.history` — только TLI ≤ oldest.TLI (переход в TLI самого старого
    /// оставляемого полного уже позади); история выше — часть цепочек
    /// оставляемых полных (PITR). oldestKeptStart null — прежняя граница
    /// (консервативно: вызывающий не передал состав Keep; история — TLI ниже
    /// cutoff). `.partial` и нераспознаваемые имена — не входят; объект TLI
    /// выше cutoff позицией ниже cutoff не трогаем (контроль его игнорирует,
    /// удалять незачем). Чистая функция.</summary>
    public static IReadOnlyList<string> SelectWalForDeletion(
        IReadOnlyList<string> objectNames, WalFileName cutoff,
        WalFileName? oldestKeptStart = null)
    {
        var result = new List<string>();
        foreach (var name in objectNames)
        {
            if (WalFileName.TryParse(name) is { } segment)
            {
                if (BelowWalCutoff(segment, cutoff, oldestKeptStart))
                    result.Add(name);
            }
            else if (WalFileName.TryParseHistory(name) is { } tli)
            {
                if (oldestKeptStart is { } oldest ? tli <= oldest.Tli : tli < cutoff.Tli)
                    result.Add(name); // .history TLI вне цепочек оставляемых полных
            }
        }

        return result;
    }

    // Сегмент ниже границы чистки: вне цепочек оставляемых полных (строго ниже
    // oldest по TLI/позиции) либо хвост cutoff-TLI до старта новейшего полного.
    private static bool BelowWalCutoff(
        WalFileName segment, WalFileName cutoff, WalFileName? oldestKeptStart)
    {
        var pos = (long)segment.Log * WalFileName.SegsPerLog + segment.Seg;
        if (oldestKeptStart is { } oldest)
        {
            var oldestPos = (long)oldest.Log * WalFileName.SegsPerLog + oldest.Seg;
            if (segment.Tli < oldest.Tli || (segment.Tli == oldest.Tli && pos < oldestPos))
                return true;
            return segment.Tli == cutoff.Tli
                && pos < (long)cutoff.Log * WalFileName.SegsPerLog + cutoff.Seg;
        }

        // Граница неизвестна — прежнее правило (TLI ниже cutoff / позиция в нём).
        return segment.Tli < cutoff.Tli
            || (segment.Tli == cutoff.Tli
                && pos < (long)cutoff.Log * WalFileName.SegsPerLog + cutoff.Seg);
    }

    /// <summary>Номер жизни ПОСЛЕДНЕГО полного шарда — вход гейта ветки (д)
    /// кусочной гигиены (arch/backups/processes/d-wal-sweeper.md): пока L —
    /// жизнь последнего `full/&lt;id&gt;`, куски жизни L остаются (RPO-хвост
    /// повторного restore/drill в окне до пересъёма). Полный — максимум по id
    /// (id — сортируемая метка времени; список etcd-статусов уже отсортирован,
    /// максимум не зависит от порядка). null — полных нет или у записи нет
    /// chain_life (старые ключи) — гейта нет. Чистая функция.</summary>
    public static uint? LastFullLife(IReadOnlyList<FullBackupState> fulls)
        => fulls.Count == 0
            ? null
            : fulls.MaxBy(f => f.Id, StringComparer.Ordinal)?.ChainLife;

    /// <summary>Мусорные объекты кусочного семейства незакрытого сегмента
    /// (arch/backups/05-etcd-contract.md, arch/backups/processes/d-wal-sweeper.md, spec §4.1;
    /// чистая функция): (а) поглощённые финалом — лежит wal/&lt;seg&gt; ⇒ все
    /// версии и куски мусор; (б) куски ниже лучшей базы жизни (база — компак
    /// start=0 с max end; дельты выше базы — живое покрытие, не мусор);
    /// (в) устаревшие компаки — остаётся длиннейшая база; (г) семейство
    /// сегмента ниже cutoff (cutoff null — ветка не выполняется); (д) куски
    /// ЧУЖИХ номеров жизни и безномерные t14-версии сегмента с куском текущего
    /// номера — мусор СРАЗУ, без cutoff, КРОМЕ кусков жизни последнего полного
    /// шарда (lastFullLife): пока L — номер жизни последнего `full/&lt;id&gt;`,
    /// куски L — RPO-хвост повторного restore/drill выбранного полного в окне
    /// до пересъёма; появился полный с более новой жизнью — ветка (д) для L
    /// открывается (гейт arch/backups/processes/d-wal-sweeper.md). chainLife
    /// == null (гвард restore) — ветка (д) не выполняется: чужие жизни не
    /// трогаются, чистка (б)/(в) идёт в рамках группы своей жизни;
    /// lastFullLife == null (полных нет/старые ключи без chain_life) — гейта
    /// нет. Финалы/.history не трогает.</summary>
    public static IReadOnlyList<string> SelectPartialForDeletion(
        IReadOnlyList<string> objectNames, WalFileName? cutoff, uint? chainLife,
        uint? lastFullLife = null, WalFileName? oldestKeptStart = null)
    {
        var finals = new HashSet<WalFileName>();
        var versions = new Dictionary<WalFileName, List<(string Name, ulong EndLsn)>>();
        var pieces = new Dictionary<WalFileName, List<(string Name, WalDeltaName.Piece Piece)>>();
        foreach (var name in objectNames)
        {
            if (WalFileName.TryParse(name) is { } final)
                finals.Add(final);
            else if (WalDeltaName.TryParse(name) is { } piece)
            {
                if (!pieces.TryGetValue(piece.Segment, out var list))
                    pieces[piece.Segment] = list = [];
                list.Add((name, piece));
            }
            else if (WalPartialName.TryParse(name) is { } p)
            {
                if (!versions.TryGetValue(p.Segment, out var list))
                    versions[p.Segment] = list = [];
                list.Add((name, p.EndLsn));
            }
        }

        var doomed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var segment in versions.Keys.Union(pieces.Keys))
        {
            var segmentVersions = versions.TryGetValue(segment, out var v) ? v : [];
            var segmentPieces = pieces.TryGetValue(segment, out var pcs) ? pcs : [];

            if (finals.Contains(segment))
            {
                // (а) финал по сегменту лежит — кусочное семейство поглощено
                doomed.UnionWith(segmentVersions.Select(x => x.Name));
                doomed.UnionWith(segmentPieces.Select(x => x.Name));
                continue;
            }
            if (cutoff is { } limit
                && BelowWalCutoff(segment, limit, oldestKeptStart))
            {
                // (г) всё кусочное семейство сегмента ниже cutoff
                doomed.UnionWith(segmentVersions.Select(x => x.Name));
                doomed.UnionWith(segmentPieces.Select(x => x.Name));
                continue;
            }

            // (д) смена жизни: чужие номера и безномерные-при-наличии-текущего —
            // мусор сразу; при chainLife == null (restore) ветка молчит. Куски
            // жизни последнего полного шарда НЕ мусор (гейт окна до пересъёма:
            // их читает повторный restore/drill выбранного полного).
            if (chainLife is { } life)
            {
                foreach (var (name, piece) in segmentPieces)
                    if (piece.ChainLife != life && piece.ChainLife != lastFullLife)
                        doomed.Add(name);
                if (segmentPieces.Any(x => x.Piece.ChainLife == life))
                    doomed.UnionWith(segmentVersions.Select(x => x.Name));
            }

            // (б)/(в) в рамках группы одной жизни: база стартует покрытие с 0,
            // всё с end ≤ базы (вкл. устаревшие компаки) — мусор; дельты выше живут.
            var groups = chainLife is { } currentLife
                ? segmentPieces.Where(x => x.Piece.ChainLife == currentLife)
                    .GroupBy(x => x.Piece.ChainLife)
                : segmentPieces.GroupBy(x => x.Piece.ChainLife); // гвард: каждая жизнь своей группой
            foreach (var group in groups)
            {
                var list = group.ToList();
                var segmentStart = WalDeltaName.SegmentStartOf(segment);
                var bases = list.Where(x => x.Piece.Start == segmentStart).ToList();
                var baseEnd = bases.Select(x => x.Piece.End).DefaultIfEmpty(0UL).Max();
                if (baseEnd == 0)
                    continue; // базы нет — покрытие жизни не начато, куски не трогаем
                // Остаётся одна длиннейшая база (равные — детерминированно первая по имени).
                var keeper = bases.Where(x => x.Piece.End == baseEnd)
                    .OrderBy(x => x.Name, StringComparer.Ordinal).First().Name;
                foreach (var (name, piece) in list)
                    if (piece.End <= baseEnd && name != keeper)
                        doomed.Add(name); // (б) ниже базы / (в) устаревший компак
            }

            // Безномерные t14-версии — прежнее правило (б): остаётся длиннейшая
            // (только когда кусков этой жизни у сегмента нет — иначе (д) выше).
            var hasCurrentPieces = chainLife is { } life3
                && segmentPieces.Any(x => x.Piece.ChainLife == life3);
            if (!hasCurrentPieces && segmentVersions.Count > 0)
            {
                var keep = segmentVersions.MaxBy(x => x.EndLsn).Name;
                doomed.UnionWith(segmentVersions.Where(x => x.Name != keep).Select(x => x.Name));
            }
        }

        return doomed.ToList();
    }

    /// <summary>Вердикт занятости (spec §3.1): квота 0/не задана → OK без
    /// процентов; иначе usedPercent &gt;= crit → CRIT, &gt;= warn → WARN, иначе OK.</summary>
    public static StorageVerdict EvaluateStorage(long usedBytes, long quotaBytes, int warnPercent, int critPercent)
    {
        if (quotaBytes <= 0)
            return new StorageVerdict(StorageState.Ok, usedBytes, 0, null);

        var percent = Math.Round(usedBytes * 100.0 / quotaBytes, 2);
        var state = percent >= critPercent ? StorageState.Crit
            : percent >= warnPercent ? StorageState.Warn
            : StorageState.Ok;
        return new StorageVerdict(state, usedBytes, quotaBytes, percent);
    }

    /// <summary>Гигиена FAILED-истории (spec §3.3 п.5): держать последние
    /// keepFailed по started_unix, старше — del. Бэкофф не ломается: n
    /// остаётся ≤ keepFailed, а BaseSec·2^(n−1) упирается в MaxSec раньше
    /// границы (AC5).</summary>
    public static IReadOnlyList<string> SelectFailedForPrune(
        IReadOnlyList<FullBackupState> fulls, int keepFailed)
        => fulls
            .Where(f => f.State == FullBackupStatus.Failed)
            .OrderByDescending(f => f.StartedUnix)
            .Skip(Math.Max(0, keepFailed))
            .OrderBy(f => f.StartedUnix) // старейшие вперёд (порядок исполнения)
            .Select(f => f.Id)
            .ToList();

    // Группа ISO-недели: (ISO-week-year, номер недели). Год — ОБЯЗАТЕЛЬНО
    // ISOWeek.GetYear, НЕ календарный d.Year: дни на стыке календарных годов
    // одной ISO-недели (2024-12-30 и 2025-01-05 — обе ISO-неделя 1 ISO-2025;
    // 2027-01-01..03 — ISO-неделя 53 ISO-2026) при календарном годе рвутся на
    // ДВЕ группы — расщеплённая неделя расходует два слота retention.weeks,
    // реальная недельная точка вытесняется и удаляется (замечание ревью Ф4 №1).
    private static (int Year, int Week) ISOWeekYearOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (ISOWeek.GetYear(d), ISOWeek.GetWeekOfYear(d));
    }

    private static (int Year, int Month) MonthOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (d.Year, d.Month);
    }

    // Универсальная гранула (недели/месяцы): кандидаты — полные ПЕРИОДА
    // СТРОГО РАНЬШЕ текущего (arch/backups/05-etcd-contract.md: счётчики считают предыдущие
    // календарные периоды; полные текущей недели/месяца вне дневного окна
    // точками не удерживаются — текущий период держит только дневная гранула)
    // → группы по календарному периоду UTC → N свежайших групп (сравнение
    // (Year, Num) лексикографически) → из каждой последний COMPLETED
    // (max started_unix). counter = 0 → гранула исключена.
    private static void AddCalendarPoint<TGroup>(
        List<FullBackupState> completed, HashSet<string> keep, int counter,
        Func<FullBackupState, TGroup> groupOf, DateTime now)
        where TGroup : IComparable<TGroup>
    {
        if (counter <= 0)
            return;

        var current = groupOf(new FullBackupState(
            "now", FullBackupStatus.Completed, "n1", BackupSourceRole.Replica,
            new DateTimeOffset(now).ToUnixTimeSeconds(), null, null, null, null, null));
        var previous = completed
            .Where(f => groupOf(f).CompareTo(current) < 0)
            .GroupBy(groupOf)
            .OrderByDescending(g => g.Key)
            .Take(counter);
        foreach (var group in previous)
        {
            var last = group.MaxBy(f => f.StartedUnix); // ПОСЛЕДНИЙ периода (AC1)
            if (last is not null)
                keep.Add(last.Id);
        }
    }
}
