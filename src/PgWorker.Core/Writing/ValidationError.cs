namespace PgWorker.Core.Writing;

// Ошибка валидации одного поля (ProblemDetails errors, arch/02 §9.3).
// Перенос из AdminPanel.Etcd.Writing (task etcd-via-worker-api).
public sealed record ValidationError(string Field, string Message);
