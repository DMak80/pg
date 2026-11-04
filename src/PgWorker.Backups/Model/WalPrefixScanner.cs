namespace PgWorker.Backups;

/// <summary>Валидный префикс незакрытого WAL-сегмента (arch/19 §3): байты до
/// конца последней ПОЛНОЙ записи, без нулей-добивки. Страничная механика PG:
/// страница 8 КиБ; первая страница сегмента — long-заголовок 40 байт, прочие —
/// short 24; запись, не помещающаяся в остаток страницы, ПЕРЕНОСИТСЯ на
/// следующую страницу (продолжение — после её заголовка; xlp_rem_len), а
/// следующая запись выравнивается по 8 (MAXALIGN); заголовок записи: xl_tot_len
/// LE u32 @0 (первое поле XLogRecord), минимум 24 байта. Любая аномалия — стоп
/// на конце последней полной записи (консервативно: короткий префикс теряет
/// лишь часть RPO-хвоста, финал восстановит полный). Чистая функция.</summary>
public static class WalPrefixScanner
{
    public const int PageBytes = 8192;       // XLOG_BLCKSZ
    public const int LongHeaderBytes = 40;   // XLogLongPageHeaderData — первая страница сегмента
    public const int ShortHeaderBytes = 24;  // XLogPageHeaderData — прочие страницы
    public const int MinRecordBytes = 24;    // SizeOf(XLogRecord)
    private const long RecordAlign = 8;      // MAXALIGN длины записи

    /// <summary>Длина валидного префикса, байт; 0 — ни одной полной записи
    /// (буфер короче long-заголовка / только заголовок / нули).</summary>
    public static int ValidPrefixLength(byte[] buffer, int count)
    {
        if (count < LongHeaderBytes)
            return 0;

        var prefix = 0; // конец последней полной записи
        long pos = 0;
        var firstPage = true;
        while (pos < count)
        {
            if (pos % PageBytes == 0)
            {
                // Заголовок страницы: long — только на первой странице сегмента.
                pos += firstPage ? LongHeaderBytes : ShortHeaderBytes;
                firstPage = false;
                if (pos > count)
                    return prefix; // заголовок не дописан — записей на странице нет
            }

            var pageEnd = (pos / PageBytes + 1) * PageBytes;
            if (pageEnd - pos < MinRecordBytes)
            {
                // Хвост страницы — добивка (запись целиком ушла на следующую).
                pos = pageEnd;
                continue;
            }
            if (count - pos < MinRecordBytes)
                return prefix; // заголовок записи ещё не принят потоком

            // xl_tot_len — ПЕРВОЕ поле XLogRecord (@0): @4 — это xl_xid,
            // чтение длины оттуда резало префикс посреди следующей записи
            // (факт E2E: CRC-ошибка восстановления на границе префикса).
            var totalLen = (uint)(buffer[pos] | buffer[pos + 1] << 8
                | buffer[pos + 2] << 16 | buffer[pos + 3] << 24);
            if (totalLen < MinRecordBytes)
                return prefix; // нули/мусор — конец валидного WAL

            // Физический конец записи: байты записи переносятся через страницы
            // (продолжение — после short-заголовка следующей страницы), затем
            // MAXALIGN-добивка до 8 (в неё же попадает хвост последней страницы).
            var (recordEnd, next) = Advance(buffer, count, pos, totalLen);
            if (recordEnd < 0)
                return prefix; // запись ещё пишется — принимаем только полные
            prefix = (int)recordEnd;
            pos = next;
        }

        return prefix;
    }

    // Продвижение по WAL-контенту на totalLen байт записи + MAXALIGN-добивку
    // с пропуском short-заголовков страниц. recordEnd — физический конец байтов
    // записи (-1 — байты записи ещё не приняты целиком); next — позиция
    // следующей записи (padding включён; если padding/конец не приняты —
    // next = int.MaxValue ⇒ цикл завершится на prefix).
    private static (long RecordEnd, long Next) Advance(
        byte[] buffer, int count, long pos, uint totalLen)
    {
        var padded = (long)((totalLen + RecordAlign - 1) & ~(RecordAlign - 1));
        long cur = pos;
        long left = totalLen;
        while (left > 0)
        {
            if (cur % PageBytes == 0)
                cur += ShortHeaderBytes; // продолжение записи на новой странице
            var room = PageBytes - cur % PageBytes;
            var take = Math.Min(room, left);
            cur += take;
            left -= take;
            if (cur > count)
                return (-1, long.MaxValue); // байты записи не приняты целиком
        }

        var recordEnd = cur;
        left = padded - totalLen;
        while (left > 0)
        {
            if (cur % PageBytes == 0)
                cur += ShortHeaderBytes;
            var room = PageBytes - cur % PageBytes;
            var take = Math.Min(room, left);
            cur += take;
            left -= take;
            if (cur > count)
                return (recordEnd, long.MaxValue); // запись цела, добивки ещё нет
        }

        return (recordEnd, cur);
    }
}
