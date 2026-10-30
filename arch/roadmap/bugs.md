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

- **t01-adminpanel-integration-wwwroot** — серия `AdminPanel.IntegrationTests` падает пачкой (157 failed / 27 passed) на absent-среде: collection fixture `AuthWebFactory` бросает `System.IO.DirectoryNotFoundException: …/src/AdminPanel.Api/wwwroot/` (StaticWebAssets требует SPA-статику; на хосте `wwwroot` не существует — SPA собирается только внутри docker-образа панели). Ожидание: интеграционные тесты панели поднимаются без собранного SPA; факт: ни один тест серии не стартует. Команда: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --nologo`; серия юнит-прогона 2026-10-30, ветка `main`, коммит `7d40f613` (worktree с правками A2 — фейл с правкой не связан, правки только в PgWorker.* и arch). Логи: `arch/roadmap/bugs/t01-adminpanel-integration-wwwroot/` (units-full.log, units-summary.txt). Гипотеза: тестовый хост в Development включает StaticWebAssets — фикстуре нужно отключать статику (или собирать SPA перед серией). Направление: `AuthWebFactory` — `UseStaticWebAssets(false)`/`UseContentRoot` на тестовый контент.
- **t02-walreceiver-restart-slot-active** — `PgWorker.IntegrationTests.Backups.WalReceiverIntegrationTests.RestartCatchesUpFromTail` падает: `the condition was not met within the 30 s budget; debug: none` (бюджет теста, WaitUntilAsync на строке 143 → 227), в stdout повторное `start_failed: START_REPLICATION pgw_bkp_it: 55006: replication slot "pgw_bkp_it" is active for PID 100` — WalReceiver-процесс после рестарта не может подцепиться к слоту (слот держит активный PID прошлой репликации) в бюджет 30 с. Ожидание: после рестарта WalReceiver догоняет хвост слота в бюджет; факт: START_REPLICATION отвергается «slot is active» до истечения бюджета. Фейл вне области текущей задачи (правки ветки — TLS-грань HTTP API воркеров; WalReceiver/replication-механика не тронута). Команда: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build` (полная серия: Failed 1, Passed 374, Total 375, 6m31s); серия 2026-10-30, ветка `feat-t04-open-metrics-healthz-facet`, коммит `0d29a867` (рабочее дерево с правками ветки — фикс AllowAnyClientCertificate в TlsEndpoints; с WalReceiver не связан). Логи: `arch/roadmap/bugs/t02-walreceiver-restart-slot-active/` (pgworker-integration-full.log, build.log). Гипотеза: гонка «старый процесс ещё держит слот активным PID» на момент рестарта — тесту нужно ожидание освобождения слота (pg_replication_slots.active=false) перед стартом нового WalReceiver, либо бюджетный флак под параллельной docker-нагрузкой прогона.
