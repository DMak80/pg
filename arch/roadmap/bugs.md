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
- **t02-scale-addshard-readonly-dsn** — E2E `Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused` падает (1 failed, 2 м 18 с): add-shard shard3 навсегда в `phase=started`, ассерт «AddShardProcess must raise shard3 (dsn/RUNNING)» ложен; воркер циклит `SQL failed [Host=host.docker.internal;Port=15111;…]: 25006: cannot execute CREATE DATABASE in a read-only transaction` (ReconcileLoop scale-shards, AddShardProcess phase sql, итерации каждые ~5 с). При этом shard3-контейнеры здоровы: patroni-эмулятор выбрал лидером **shard3b** («the leader with the lock»), shard3a — secondary; sql-фаза подключается на порт 15111 не-лидерской ноды (или dsn зафиксировался до переизбрания) — CREATE DATABASE уходит на read-only реплику. Ожидание: dsn sql-фазы — лидер шарда (либо доведение dsn за переизбранием); факт: процесс не доезжает до dsn/RUNNING в бюджете теста. Команда: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` (до неё `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` и сборка решения); мерж-гейт задачи t33 2026-10-30, ветка `t33-s3-node-failover`, коммит `402ea52d` (фейл вне области задачи: правки t33 — только слепочный sink/Options/deploy, Provisioning не тронут; одновременно в другом worktree шёл чужой прогон того же теста — контур kfw-re17f628e9c жил с ранее). Логи: `arch/roadmap/bugs/t02-scale-addshard-readonly-dsn/` (container-pgw-ew-s1-*.log — ошибки 25006, shard3a/shard3b.log — здоровье patroni, gate3-scale-test.log, failed.txt, README-cleanup.txt). Гипотеза: гонка «dsn sql-фазы ↔ переизбрание лидера эмулятора» — dsn порт назначается/фиксируется до стабилизации роли; направление: AddShardProcess — sql-фаза по dsn текущего лидера (роль из patroni/emulator-стейта) либо waiting-key на смену роли с пере-резолвом dsn.
