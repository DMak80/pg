# Телеметрия: экспозиция и безопасность

Уровень 3. Экспозиция `/metrics` и границы доверия. Родитель —
[README.md](README.md); соседние документы — [01-framework.md](01-framework.md),
[02-dictionary.md](02-dictionary.md), [04-collectors.md](04-collectors.md),
[05-storage.md](05-storage.md), [06-testing.md](06-testing.md),
[07-risks.md](07-risks.md).

- Воркеры: `/metrics` на том же Kestrel `:8080`, что `/healthz` —
  открытая грань доверенной docker-сети И хост-публикаций портов API
  docker-хоста (`localhost:{PORT}`; публикации нужны панели — AdvertiseUrl
  `host.docker.internal` — и хостовым потребителям: чеки, скрейп): HTTPS
  без клиентского серта (mTLS у воркеров обязателен только на `/api`,
  [pgworker/07-api.md](../pgworker/07-api.md) §2; порт /healthz-канона,
  симметрия панели). Граница доверия грани — периметр docker-хоста, а не
  только docker-сеть; опциональное сужение (bind `127.0.0.1` отдельным
  env-флагом; ломает `host.docker.internal`-потребителей) — роадмап,
  каноном не предписывается.
- Панель: `/metrics` без cookie-авторизации (guard — только `/api/*`);
  бандл SPA секретов не содержит.
- Формат — Prometheus text/OpenMetrics (content-negotiation экспортёра);
  scrape-интервал стенда 15 с.
