# Bugs (pg)

Падения тестов/дефекты вне текущих задач. К каждому пункту — папка логов
`bugs/<tNN-slug>/` (вне git, см. `.gitignore`). Пункт закрывается мерж-гейтом
задачи-фикса: тег удаляется тем же мерж-коммитом.

## Открытые

- **t90-walsql-ensurealive-flaky** — флак `WalSqlTests.ProbeEnsureAliveRecreateIdempotentOnLiveAndMissingSlot`
  под параллельной нагрузкой полного прогона. Логи: `bugs/t90-walsql-ensurealive-flaky/`.
  - Серия/фильтр: `PgWorker.IntegrationTests`, тест
    `FullyQualifiedName~WalSqlTests.ProbeEnsureAliveRecreateIdempotentOnLiveAndMissingSlot`.
  - Команда запуска: полный прогон `DOTNET_CLI_UI_LANGUAGE=en bash scripts/run-all-tests.sh`
    (run-dir `/tmp/pgw-testruns-20261106-165307`; изолированно не воспроизводил себя).
  - Коммит/ветка: `feat-etcdnode-backup`, коммит на момент прогона — `59ddf685`
    (перенос DR-бэкапа etcd в EtcdNode; WalSql не тронут).
  - Ошибка: `Expected ensured.IsSuccess to be True, but found False` —
    `NpgsqlWalSqlExecutor.EnsureSlotAliveAsync` (WalSqlTests.cs:57) вернул Failed
    на живом postgres-контейнере при том, что соседний `SlotProbeAsync` прошёл.
  - Ожидание: ensure/повторный ensure слота `pgw_bkp_test` — успех и идемпотентность.
    Факт: первый ensure Failed (вероятно, таймаут/конкуренция подключений при
    параллельном E2E-потоке полного прогона).
  - Логи: `run-20261106-165307.log` (лог серии, стектрейс), `run-all-stdout.log`
    (stdout прогона: соседние серии зелёные, нагрузка E2E параллельна).

- **t91-walstream-tli-degraded** — падение E2E
  `E2eWalStreamTliScenarios.WalStream_TliJump_GapMarked_FullRetaken` в полном
  прогоне (WAL-поток PgWorker после прыжка timeline не вернулся в Active).
  Логи: `bugs/t91-walstream-tli-degraded/`.
  - Серия/фильтр: `PgWorker.E2eTests`, тест
    `FullyQualifiedName~E2eWalStreamTliScenarios.WalStream_TliJump_GapMarked_FullRetaken`.
  - Команда запуска: полный прогон `DOTNET_CLI_UI_LANGUAGE=en bash scripts/run-all-tests.sh`
    (run-dir `/tmp/pgw-testruns-20261106-165307`; сборка/статика — фазы скрипта).
  - Коммит/ветка: `feat-etcdnode-backup`, коммит на момент прогона — `59ddf685`
    (задача переноса DR-бэкапа etcd в EtcdNode; WAL-поток PgWorker не тронут).
  - Ошибка (E2eWalStreamTliScenarios.cs:460): `Expected finalRead.Value!.State
    to be WalStreamStatus.Active ... but found WalStreamStatus.Degraded` —
    конвергенция ключа wal после TLI-прыжка не достигла ACTIVE (§3.7 inv.2),
    бюджет окна исчерпан (кейс шёл 2 м 15 с).
  - Ожидание: после TLI-прыжка разрыв помечается, полный WAL передаётся
    заново, ключ wal → ACTIVE. Факт: Degraded на финальном чтении.
  - Логи/артефакты: `series.log` (лог серии, стектрейс), `summary.tsv`,
    `e2e-artifacts/` (docker-логи/inspect всех контейнеров сценария гуида
    fcda5ce5…, сняты teardown до удаления; контейнеры/сеть/тома зачищены по
    README-cleanup после снятия).

