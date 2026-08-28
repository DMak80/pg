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
  RBAC/docker-группы.
- **`t04-metrics`** — Prometheus-метрики PgWorker (фазы процессов, клэймы,
  лаги, возраст снапшотов) + алертинг во внешние системы.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t90-portalloc-parallel-race`** — гонка параллельного provisioning: свежие
  кластеры на одном docker-хосте, засеянные одновременно, получают одинаковые
  порты (PortAllocator дедуплицирует только по живым docker-биндингам и своему
  /pgworker/portalloc/<C>, между кластерами нет общей картины; воспроизведено
  на dev-станде 2026-08-25 — контейнеры вторых кластеров в Created с «port is
  already allocated»). Нужно: учитывать portalloc ВСЕХ кластеров + глобальный
  txn/курсор на общий диапазон хоста. Обход: сеять кластеры последовательно.
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
