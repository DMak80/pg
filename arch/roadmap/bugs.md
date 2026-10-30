# Roadmap: баги (падения тестов / дефекты вне текущих задач)

Сюда складываем найденные баги: падения тестов и дефекты продукта, обнаруженные
вне области текущей задачи (мерж-гейты, серии, ревью) и не решаемые в ней по
канону AGENTS.md («фейл вне области — отдельная задача»).

## Правила ведения

- Нумерация NN — своя внутри трека, тег `tNN-slug`; порядок номеров = порядок
  исполнения по умолчанию (правила общие — [README.md](README.md)).
- **К каждому пункту — своя подпапка логов** `arch/roadmap/bugs/<tNN-slug>/`:
  журналы серий (относящиеся фрагменты или целиком, если небольшой),
  телеметрия/артефакты E2E (docker-логи/inspect, timing, `README-cleanup.txt`),
  тексты ассертов. Папка **вне git** (путь в `.gitignore`): логи в репозиторий
  не коммитятся, живут только локально. Суммарно по кейсу > 100 МБ — содержимое
  подпапки архивируется в tar.gz внутри неё же (указано в пункте).
- Состав сведений пункта: точное имя теста/фильтр; команда запуска с
  env-префиксами; серия/дата/коммит/ветка; пути к логам (подпапка); текст/суть
  ошибки; наблюдаемое vs ожидаемое; гипотезы, если есть.
- **Закрытие бага**: пункт удаляется из этого файла тем же коммитом, что
  закрывает баг, — **вместе со своей подпапкой логов**
  `arch/roadmap/bugs/<tNN-slug>/` (папка — локально, git её не хранит).
  Пункт удаляется только когда баг реально
  разобран и исправлен/закрыт, не раньше; история остаётся в git и
  `docs/superpowers/`.

## Задачи
- `b01-restore-promote-hang` — E2E `Pool_ReadAnyNode_RestoreAfterNodeStop`
  (PgWorker.E2eTests, `dotnet test src/tests/PgWorker.E2eTests -c Release
  --no-build --filter FullyQualifiedName~Pool_ReadAnyNode_RestoreAfterNodeStop`,
  env `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1`): серия №18, 2026-10-30,
  ветка `fix-t90-e2e-session-isolation`, коммиты 66703562/26efbb4f; падение —
  `restore pac6…/shard1/20261030121612Z-shard1 did not reach COMPLETED: RUNNING`
  (WaitRestoreAsync 90 с). Продуктовая зона: restore-операция приёмника ноды при
  мёртвом первом endpoint'е пула (тест остановил ownS3-ноду 1 — носитель alias
  `e2e-owns3`): recovery УСПЕШНО завершился (fetch-wal перебор пула работает —
  node2 отдал сегменты, «redo done», «selected new timeline ID: 2»), но
  финализация (promote по `recovery_target_action='promote'` → рестарт
  postmaster → `pg_is_in_recovery()='f'`) молчит 3+ минуты — состояние
  RUNNING до бюджета; postmaster после «selected timeline» не пишет в
  restore-pg.log ничего. Ожидание: промоут завершается, операция Succeeded →
  COMPLETED. Фактически: вечный RUNNING. Логи: `arch/roadmap/bugs/
  b01-restore-promote-hang/` (restore-pg.log, host-worker.log, owns3-node2.log,
  README-cleanup). Гипотезы: постмастер-рестарт при промоуте конфликтует с
  supervisorctl-остановленным spilo ноды (restore исполняется в контейнере
  ноды с остановленной supervisord-программой — кто забирает/перезапускает
  postmaster?); либо fetch-wal во время промоут-поиска WAL тянет 45-с
  транспортные ретраи по мёртвому endpoint'у и растягивает старт.
