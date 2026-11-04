# PgNode: эксплуатация

Уровень 3. Функция «эксплуатация»: статусная машина агента и хендшейк
доставки, надёжность, наблюдаемость, риски. Процесс статусной машины —
[processes/f-node-agent.md](processes/f-node-agent.md); состояния —
[processes/enums.md](processes/enums.md); секреты —
[pgworker/04-security.md](../pgworker/04-security.md) §3–§4.

## 1. Статусная машина агента

Ключ `/pgworker/nodes/<C>/<X>/<N>/status` (lease TTL 15 с) пишет ТОЛЬКО
агент PgNode, воркер читает-мониторит; смерть процесса гасит ключ — статус
не «застревает». Состояния `not_initialized → initializing → ready |
start_failed`; фазы `initializing` (старт компонентов vs tool-путь
`is_tool_start`, ожидание `SELECT 1` без бюджета), применение материала на
`ready`, реакция воркера на `start_failed` — полный автомат в
[processes/f-node-agent.md](processes/f-node-agent.md).

Кратко: агент публикует `not_initialized` при старте (листенер `:8081` уже
поднят); воркер, увидев его, доставляет bootstrap-ключ и per-install пароли
PG телом mTLS-запроса; агент атомарно переводит состояние в `initializing`
txn-сравнением, стартует компоненты (до 3 попыток на компонент) и ждёт
`SELECT 1` без бюджета; все готовы → `ready` с `applied_revision`. На
`ready` замена материала — отдельная команда применения (обновляет
`applied_revision`, машина остаётся `ready`). `start_failed` → воркер
пересоздаёт ноду с новым диском по правилам
[pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md) §9.

## 2. Надёжность

- **Процесс под supervisord'ом**: autorestart при падении; permanent-условия
  — состояние `failed_permanent`, не exit (воркер не может перезапустить
  процесс чужого контейнера).
- **Идемпотентность**: put сегмента — для тех же байтов; повторные
  команды/пины — no-op по материальному гейту; staging/scratch чистятся при
  любом исходе; резюмов операций нет — повтор с нуля.
- **RPO**: подтверждение слота только после put; рестарт приёмника — от
  хвоста S3, дыра досыпается из слота.
- **Данные неприкосновенны**: дрилл — только scratch; restore — только по
  команде и на пустом PGDATA; stop/return Patroni — с подтверждением и в
  finally.
- **Изоляция операций**: общий ops-реестр — одна ops-операция на процесс;
  полный и ops не блокируют друг друга.
- **Секреты**: tmpfs `/run/pgw`, env процесса postgres, не в argv/GET/логах.

## 3. Наблюдаемость

- `/metrics` на `:8080` внутри контейнера ноды — метрики приёмника WAL
  ([18-metrics.md](../18-metrics.md) §2.8); scrape — через ScrapeNetwork.
- Статус-ключи: `/pgworker/backups/<C>/<X>/agent/<N>` (факт сессии
  приёмника: роль, state, `last_delivered`, `pool_exhausted`, `tli_jump`,
  `history_missing_tli`, `error`) и `/pgworker/nodes/<C>/<X>/<N>/status`
  (статусная машина, `applied_revision`, компоненты, `error`).
- GET-снимки операций (`/api/backup/full`, `/api/ops/restore`,
  `/api/ops/verify`) — источник фаз для воркера; воркер отражает их в
  etcd-статусах (etcd пишет ТОЛЬКО воркер).
- Логи процесса — диагностика фаз; секретов в логах нет.

## 4. Риски

| Риск | Закрытие |
|---|---|
| Процесс делит ресурсы с нодой — тяжёлый verify/restore может давить PG | операции идут последовательно в общем ops-реестре; staging/scratch на data-volume, ENOSPC — реактивный FAILED; память буфера — `wal_segment_size` (~16 МиБ) |
| Рестарт процесса посреди операции — операция vanished | GET 404 → воркер FAILED `operation-vanished` / повтор; резюмов нет (осознанно) |
| Чужая инкарнация в префиксе `wal/` | гвард финальных объектов (ETag/md5); несовпадение — `failed_permanent` «foreign WAL chain» до восстановления/демонтажа |
| Долгий fetch-wal на мёртвой ноде пула | внутренние ретраи S3-клиента выключены, жёсткий общий дедлайн вызова; моргания покрываются повторными вызовами restore_command |
| Patroni вмешивается в заполняемую PGDATA | stop `spilo` с подтверждением перед restore, возврат в finally; повторное удаление `standby.signal` перед `pg_ctl start` |
| Секреты в argv/логах | пароль `backup_exec` — env child-процесса; S3-контекст — только в POST; tmpfs 0600 postgres |
