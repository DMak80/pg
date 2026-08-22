# Трек: etcd (клиент, снапшот, инспекция etcd)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) (ключи, транспорт,
модель снапшота), [../01-architecture.md](../01-architecture.md) §1–2.

## Задачи

- `t04-etcd-api` ← `t02-auth`, `t03-etcd-snapshot` — API инспекции etcd и
  каркас алертов. `AlertEngine` (чистая функция, стабильные id
  `kind:target`, сравнение с прошлым снапшотом для `sinceUnix`) с etcd-частью
  каталога (03 §4): `etcd-unreachable`, `etcd-no-quorum`, `etcd-endpoint-down`,
  `etcd-alarm`, `snapshot-stale`, `cluster-incomplete`, `key-malformed`.
  Эндпоинты: `GET /api/overview` (сводка без шардирования — поля-заглушки),
  `GET /api/etcd/status`, `GET /api/alerts` (с query-параметрами), ProblemDetails.
  Хендлеры — `IQuery` через диспетчер, маппинг снапшот→DTO. Unit на
  AlertEngine + DTO-мапперы; integration-смоук API против Testcontainers-etcd.
