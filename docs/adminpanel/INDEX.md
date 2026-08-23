# docs/ — практические документы подсистем AdminPanel

Здесь — **как устроен код и что сломается при изменении**: чек-листы и грабли из
опыта задач t01–t10. Контракт (что и почему строим) — в [`../arch/`](../arch/README.md):
arch — источник истины, docs — практики; при расхождении правится arch, затем docs.
История задач (spec/plan по каждой) — в [`superpowers/`](superpowers/). Каркас
Infrastructure скопирован из референса `../Puzzle` (его docs — родитель стиля и
механик DI/CQRS/Result).

## Документы

| Документ | Подсистема | Назначение |
|---|---|---|
| [01 — Каркас](01-framework.md) | `AdminPanel.Infrastructure` | attribute-DI, CQRS-queries + `Result`, модульная композиция, health-checks; грабля статического кеша сборок. |
| [02 — etcd-снапшот](02-etcd-snapshot.md) | `AdminPanel.Etcd` | HTTP JSON gateway `/v3/*`, парсеры, `SnapshotRefresher`/`SnapshotStore`; инвариант «API не ходит в etcd на запрос». |
| [03 — Пробы и алерты](03-probes-alerts.md) | `AdminPanel.Probes` + `Core/Alerting` | Patroni/SQL live-пробы, HostMap, `AlertEngine` — 25 правил. |
| [04 — Фронтенд](04-frontend.md) | `frontend/` | Сборка SPA в wwwroot, api-слой, polling, guard; TS7-css и registry-грабли. |
| [05 — Dev-стенд](05-dev-stand.md) | `dev-stand/` | Профили quick/full, сид, patroni-эмуляторы с lease, e2e-чеки и их порядок. |

## Соглашения

- Новый документ подсистемы: `NN-slug.md`, следующий свободный NN; строка в таблицу
  выше; шапка `> Назад: [docs/README.md](README.md)`; финал документа — разделы
  «Чек-лист при изменениях» и «Грабли».
- Грабли пишем только пережитые (ссылка на задачу/коммит); предположения — не грабли.
