namespace OwnS3.Storage;

// Невосстановимая порча xl-данных: битые xl.meta+xl.meta.bkp, checksum-mismatch
// при чтении, несоответствие фактической длины тела. App-конвейер отдаёт 500
// InternalError (catch-all), диагностика — в лог.
public sealed class XlIntegrityException(string message) : Exception(message);
