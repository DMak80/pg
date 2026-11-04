namespace PgWorker.Backups;

/// <summary>Валидный префикс незакрытого WAL-сегмента (arch/19 §3): байты до
/// конца последней ПОЛНОЙ записи, без нулей-добивки. Страничная механика PG:
/// страница 8 КиБ; первая страница сегмента — long-заголовок 40 байт, прочие —
/// short 24; запись, не помещающаяся в остаток страницы, ПЕРЕНОСИТСЯ на
/// следующую страницу (продолжение — после её заголовка; xlp_rem_len); запись
/// может НАЧИНАТЬСЯ в хвосте страницы, если в него влезает хотя бы первое поле
/// xl_tot_len (4 байта) — остаток 24-байтового заголовка продолжится после
/// short-заголовка следующей страницы; в остатке меньше 4 байт — добивка;
/// следующая запись выравнивается по 8 (MAXALIGN). Сегмент может начинаться
/// ПРОДОЛЖЕНИЕМ записи из предыдущего сегмента (XLP_FIRST_IS_CONTRECORD +
/// xlp_rem_len в заголовке первой страницы) — остаток записи пропускается.
/// Заголовок записи: xl_tot_len LE u32 @0 (первое поле XLogRecord), минимум
/// 24 байта. Любая аномалия — стоп на конце последней полной записи
/// (консервативно: короткий префикс теряет лишь часть RPO-хвоста, финал
/// восстановит полный). Чистая функция.</summary>
public static class WalPrefixScanner
{
    public const int PageBytes = 8192;       // XLOG_BLCKSZ
    public const int LongHeaderBytes = 40;   // XLogLongPageHeaderData — первая страница сегмента
    public const int ShortHeaderBytes = 24;  // XLogPageHeaderData — прочие страницы
    public const int MinRecordBytes = 24;    // SizeOf(XLogRecord)
    private const int TotalLenBytes = sizeof(uint); // xl_tot_len — первое поле XLogRecord
    private const ushort XlpFirstIsContrecord = 0x0001; // XLP_FIRST_IS_CONTRECORD
    private const long RecordAlign = 8;      // MAXALIGN длины записи

    /// <summary>Длина валидного префикса, байт; 0 — ни одной полной записи
    /// (буфер короче long-заголовка / только заголовок / нули).</summary>
    public static int ValidPrefixLength(byte[] buffer, int count)
    {
        if (count < LongHeaderBytes)
            return 0;

        var prefix = 0; // конец последней полной записи
        long pos = LongHeaderBytes; // первая страница: long-заголовок 40 байт

        // Сегмент может начинаться ПРОДОЛЖЕНИЕМ записи из предыдущего сегмента
        // (XLP_FIRST_IS_CONTRECORD + xlp_rem_len первой страницы): пропускаем
        // остаток записи через short-заголовки страниц, иначе продолжение будет
        // прочитано как заголовок новой записи.
        if ((LeU16(buffer, 2) & XlpFirstIsContrecord) != 0)
        {
            var remaining = LeU32(buffer, 16); // xlp_rem_len
            if (remaining == 0)
                return 0; // битый заголовок: пустого продолжения не бывает
            var contEnd = Skip(count, pos, remaining);
            if (contEnd < 0)
                return 0; // продолжение ещё не принято целиком
            prefix = (int)contEnd;
            pos = (contEnd + RecordAlign - 1) & ~(RecordAlign - 1); // MAXALIGN
            if (pos > count)
                return prefix; // добивка записи ещё не принята
        }

        while (pos < count)
        {
            if (pos % PageBytes == 0)
            {
                // Заголовок страницы: long — только первая страница сегмента
                // (обработана выше), все последующие — short.
                pos += ShortHeaderBytes;
                if (pos > count)
                    return prefix; // заголовок не дописан — записей на странице нет
            }

            var pageEnd = (pos / PageBytes + 1) * PageBytes;
            if (pageEnd - pos < TotalLenBytes)
            {
                // В остатке страницы меньше 4 байт — xl_tot_len не помещается,
                // запись там начаться не может: это добивка.
                pos = pageEnd;
                continue;
            }
            if (count - pos < TotalLenBytes)
                return prefix; // даже xl_tot_len ещё не принят

            var totalLen = LeU32(buffer, (int)pos);
            if (totalLen < MinRecordBytes)
                return prefix; // нули/мусор — конец валидного WAL

            // Физический конец записи: байты записи (включая заголовок, если он
            // разрезан границей страницы) переносятся через страницы, затем
            // MAXALIGN-добивка до 8 (в неё же попадает хвост последней страницы).
            var (recordEnd, next) = Advance(count, pos, totalLen);
            if (recordEnd < 0)
                return prefix; // запись ещё пишется — принимаем только полные
            prefix = (int)recordEnd;
            pos = next;
        }

        return prefix;
    }

    // Продвижение по WAL-контенту на length байт (тело записи/её продолжение)
    // с пропуском short-заголовков страниц. -1 — байты ещё не приняты целиком.
    private static long Skip(int count, long pos, uint length)
    {
        long left = length;
        while (left > 0)
        {
            if (pos % PageBytes == 0)
                pos += ShortHeaderBytes; // продолжение на новой странице
            var room = PageBytes - pos % PageBytes;
            var take = Math.Min(room, left);
            pos += take;
            left -= take;
            if (pos > count)
                return -1;
        }

        return pos;
    }

    // Продвижение по WAL-контенту на totalLen байт записи + MAXALIGN-добивку
    // с пропуском short-заголовков страниц. recordEnd — физический конец байтов
    // записи (-1 — байты записи ещё не приняты целиком); next — позиция
    // следующей записи (padding включён; если padding/конец не приняты —
    // next = long.MaxValue ⇒ цикл завершится на prefix).
    private static (long RecordEnd, long Next) Advance(
        int count, long pos, uint totalLen)
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

    private static ushort LeU16(byte[] b, int offset)
        => (ushort)(b[offset] | b[offset + 1] << 8);

    private static uint LeU32(byte[] b, int offset)
        => (uint)(b[offset] | b[offset + 1] << 8 | b[offset + 2] << 16 | b[offset + 3] << 24);
}
