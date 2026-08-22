# Трек: frontend (React-панели)

Контекст: [../01-architecture.md](../01-architecture.md) §5 (стек, сборка,
polling), [../03-panels.md](../03-panels.md) (панели, DTO).

## Задачи

- `t07-frontend-base` ← `t02-auth`, `t04-etcd-api` — каркас SPA.
  `frontend/`: Vite + React + TypeScript, Mantine, React Router, TanStack
  Query; vite `outDir` → `src/AdminPanel.Api/wwwroot` (+ скрипт dev с proxy
  `/api` на Kestrel); layout с навигацией и переключателем polling-интервала
  (2/5/15/off, default 5 c), страница Login, guard по `/api/auth/me`
  (401 → редирект), страницы-заглушки остальных панелей, общий API-клиент
  (типы DTO, обработка stale-бейджа по `snapshotAgeMs`). Проверка:
  `npm run build` + `dotnet run` отдаёт SPA и login работает.
- `t08-frontend-clusters` ← `t05-sharding-api`, `t07-frontend-base` — панели
  Overview, etcd, Clusters. Overview: карточки etcd/кластеров/HA-сводки,
  активные переезды, лента алертов. etcd: endpoints (+метка «активный»),
  members/лидер, alarms, lastRefresh. Clusters: список → детали с вкладками
  Шарды / Бакеты (грид id×owner×state, фильтры, подсветка не-ACTIVE,
  возраст) / Переезды / Heals (+блок «Стендовая топология», если есть).
- `t09-frontend-ha` ← `t06-ha-api`, `t07-frontend-base` — панели HA и Alerts.
  HA: список scope'ов (cluster/shard, лидер, члены, max-лаг, unmatched) →
  детали (members: role/state/timeline/lag/probe-статус, optime, raw config
  свёрнуто). Alerts: таблица с severity-цветами, kind, target, since,
  фильтр severity; количество critical/warning — в навигации. Сводные поля
  HA доливаются в Overview.
