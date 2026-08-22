# Трек: infra (каркас решения, аутентификация, поставка)

Контекст: [../01-architecture.md](../01-architecture.md). Референс подходов —
`../Puzzle` (копировать каркас, обрезать до read-only).

## Задачи

- `t02-auth` ← `t01-skeleton` — аутентификация. Cookie-сессия из настроек
  (`AdminPanel:Auth:*`: Username, Password|PasswordHash PBKDF2, SessionHours,
  AllowHttp), `POST /api/auth/login` (rate-limit 5/мин на IP, constant-time
  сравнение), `POST /api/auth/logout`, `GET /api/auth/me`; middleware:
  всё `/api/*`, кроме login и healthz, → 401. Integration-тесты
  (WebApplicationFactory): login ok/bad, 401 без cookie, logout.
- `t11-finalize` ← `t08-frontend-clusters`, `t09-frontend-ha`, `t10-dev-stand`
  — финализация. README корня (запуск, стенд, карта репо), docs/ в стиле
  Puzzle (индекс + документы подсистем с чек-листами/граблями: каркас DI/CQRS,
  etcd-контракт, пробы, фронт), многостадийный Dockerfile (node build фронта →
  dotnet publish → runtime), полный прогон build+test+e2e стенда, чистка
  warning'ов как ошибок, финальное ревью.
