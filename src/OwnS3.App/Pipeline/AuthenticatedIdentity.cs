using OwnS3.App.Access;

namespace OwnS3.App.Pipeline;

// Идентичность аутентифицированного запроса: access key и его роль.
public sealed record AuthenticatedIdentity(string AccessKey, AccessPolicy Policy);
