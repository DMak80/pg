# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-per-cluster-secrets`** — генерация и ротация секретов per-cluster
  (сейчас per-install из env, Д7): пароли app/bucket_admin/bucket_mover на
  кластер, смена без остановки записи, интеграция с secret-manager.
- **`t03-docker-tls-ssh`** — TLS к Docker Engine API и SSH-туннели к
  docker-хостам (сейчас plaintext TCP/unix-socket в доверенной сети),
  RBAC/docker-группы.
- **`t04-metrics`** — Prometheus-метрики PgWorker (фазы процессов, клэймы,
  лаги, возраст снапшотов) + алертинг во внешние системы.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t07-move-bucket-ui`** — UI явных переездов бакетов из панели AdminPanel
  (кнопки «перевезти/откатить/finalize/abort» → заявки
  `/pgworker/moves/<C>/bucket_<i>`, чтение очереди заявок и их результатов;
  выбор «кто куда переезжает» — только оператор, никакой автоперебалансировки).
  Выделена из t06 по решению пользователя; зависимостей нет (контракт заявок —
  t01, в main).
