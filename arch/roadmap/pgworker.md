# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-per-cluster-secrets`** — ротация секретов per-cluster (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Генерация per-cluster app-секрета в etcd сделана
  (2026-08-28, feat-etcd-password-field).
- **`t03-docker-tls-ssh`** — TLS к Docker Engine API и SSH-туннели к
  docker-хостам (сейчас plaintext TCP/unix-socket в доверенной сети),
  RBAC/docker-группы; сюда же — транспортная безопасность HTTP API
  PgWorker (arch/14 §1.1): mTLS/сертификаты вместо голого `X-Api-Key`
  в закрытой сети, отдельные креды панели и сида.
- **`t04-metrics`** — Prometheus-метрики PgWorker (фазы процессов, клэймы,
  лаги, возраст снапшотов) + алертинг во внешние системы.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t90-portalloc-parallel-race`** — гонка ПАРАЛЛЕЛЬНОГО provisioning: два
  свежих кластера на одном docker-хосте, засеянные одновременно, могут
  получить одинаковые порты — PortAllocator дедуплицирует по живым
  docker-биндингам и записям portalloc всех кластеров
  (`/pgworker/portalloc/*`, arch/14 §2.4), но два инстанса, прочитавшие
  префикс одновременно ДО первой записи друг друга, общей картины не имеют
  (воспроизведено на dev-станде 2026-08-25 — контейнеры вторых кластеров в
  Created с «port is already allocated»). Нужно: глобальный txn/курсор на
  общий диапазон хоста. Обход: сеять кластеры последовательно.
- **`t07-move-bucket-ui`** — UI явных переездов бакетов из панели AdminPanel
  (кнопки «перевезти/откатить/finalize/abort» → заявки
  `/pgworker/moves/<C>/bucket_<i>`, чтение очереди заявок и их результатов;
  выбор «кто куда переезжает» — только оператор, никакой автоперебалансировки).
  Выделена из t06 по решению пользователя; зависимостей нет (контракт заявок —
  t01, в main).
- **`t08-unify-adminpanel-duplicates`** — унификация дублей кода после переноса
  AdminPanel в монорепо (2026-08-27): etcd-клиент `AdminPanel.Etcd/Client/`
  (`EtcdGateway`/`IEtcdGateway`/`Kv` — урезанный аналог `PgWorker.Etcd/Client`,
  без Coordination) → перевод панели на `PgWorker.Etcd`; Puzzle-каркас
  `AdminPanel.Infrastructure` (attribute-DI, CQRS, `Result`, Traces) → перевод
  на `PgWorker.Core`. Механика: панель получает ProjectReference на общие
  сборки, дубли удаляются; поведение обеих систем не меняется (тесты зелёные).
