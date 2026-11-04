namespace PgWorker.Backups;

/// <summary>Валидный префикс незакрытого WAL-сегмента (arch/19 §3): байты до
/// конца последней ПОЛНОЙ записи, без нулей-добивки. Страничная механика PG:
/// страница 8 КиБ; первая страница сегмента — long-заголовок 40 байт, прочие —
/// short 24; запись НЕ пересекает границу страницы (не влезающая пишется целиком
/// на следующей, остаток текущей добит нулями); заголовок записи: xl_tot_len
/// LE u32 @0 (поле первое в XLogRecord), минимум 24 байта; следующая запись —
/// с выравниванием длины по 8 (MAXALIGN). Любая аномалия — стоп на конце последней
/// полной записи (консервативно: короткий префикс теряет лишь часть RPO-хвоста,
/// финал восстановит полный). Чистая функция.</summary>
public static class WalPrefixScanner
{
    public const int PageBytes = 8192;       // XLOG_BLCKSZ
    public const int LongHeaderBytes = 40;   // XLogLongPageHeaderData — первая страница сегмента
    public const int ShortHeaderBytes = 24;  // XLogPageHeaderData — прочие страницы
    public const int MinRecordBytes = 24;    // SizeOf(XLogRecord)
    private const uint RecordAlign = 8;      // MAXALIGN длины записи

    /// <summary>Длина валидного префикса, байт; 0 — ни одной полной записи
    /// (буфер короче long-заголовка / только заголовок / нули).</summary>
    public static int ValidPrefixLength(byte[] buffer, int count)
    {
        if (count < LongHeaderBytes)
            return 0;

        var prefix = 0; // конец последней полной записи
        var pos = 0;
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
            var remainingInPage = pageEnd - pos;
            if (remainingInPage < MinRecordBytes)
                return prefix; // хвост страницы — нули-добивка
            if (count - pos < MinRecordBytes)
                return prefix; // заголовок записи ещё не принят потоком

            // xl_tot_len — ПЕРВОЕ поле XLogRecord (@0): @4 — это xl_xid,
            // чтение длины оттуда резало префикс посреди следующей записи
            // (факт E2E: CRC-ошибка восстановления на границе префикса).
            var totalLen = (uint)(buffer[pos] | buffer[pos + 1] << 8
                | buffer[pos + 2] << 16 | buffer[pos + 3] << 24);
            if (totalLen < MinRecordBytes || totalLen > (uint)remainingInPage)
                return prefix; // нули/мусор/запись не влезает в страницу
            var recordEnd = pos + (int)totalLen;
            if (recordEnd > count)
                return prefix; // запись пишется — принимаем только полные

            // Следующая запись — с выравниванием (MAXALIGN); байты добивки
            // выравнивания в WAL нулевые, границу префикса не сдвигают.
            prefix = recordEnd;
            pos = recordEnd + (int)((RecordAlign - totalLen % RecordAlign) % RecordAlign);
        }

        return prefix;
    }
}
