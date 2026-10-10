namespace OwnS3.Storage;

// Доменный InvalidArgument-исход Storage (канон 04 §1 лимит сегмента; канон 02 §3
// невалидный continuation-token): InvalidArgument в ObjectStoreErrorCode отсутствует
// (перечень t37 закрыт спекой), App маппит по типу в S3 InvalidArgument 400.
public sealed class XlInvalidArgumentException(string message) : Exception(message);
