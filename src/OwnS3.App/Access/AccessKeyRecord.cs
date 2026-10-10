namespace OwnS3.App.Access;

// Учётная запись для сверки подписи: access key, секрет и роль.
public sealed record AccessKeyRecord(string AccessKey, string SecretKey, AccessPolicy Policy);
