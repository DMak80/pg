# Трек: frontend (React-панели)

Контекст: [../01-architecture.md](../01-architecture.md) §5 (стек, сборка,
polling), [../03-panels.md](../03-panels.md) (панели, DTO).

## Задачи

- `t09-frontend-ha` ← `t06-ha-api`, `t07-frontend-base` — панели HA и Alerts.
  HA: список scope'ов (cluster/shard, лидер, члены, max-лаг, unmatched) →
  детали (members: role/state/timeline/lag/probe-статус, optime, raw config
  свёрнуто). Alerts: таблица с severity-цветами, kind, target, since,
  фильтр severity; количество critical/warning — в навигации. Сводные поля
  HA доливаются в Overview.
