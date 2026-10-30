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
- **t02-e2e-etcd-dns-saturation** — docker-E2E PgWorker каскадно падает по бюджетам (12 из 73 тестов, все ~3 мин = исчерпание бюджета сценария) при прогоне на хосте с ОДНОВРЕМЕННО поднятым полным dev-стендом (16+ контейнеров нод, 4 воркера, patroni-эмуляторы, kafka/valkey/S3, панель): в логах нод массовые `WARNING: failed to resolve host e2e-etcd1: Temporary failure in name resolution` и `ReadTimeoutError ... e2e-etcd1:2379` (в wal-сессии 505 вхождений; во вторых сессиях 50–61), Patroni теряет etcd → лидерство/lease сыпется (`status revoke failed: lease not found`), bootstrap PG затягивается за 60–90 с → «provisioning is stuck … config.state not lifted», SQL попадает на реплику (25006). Те же классы падений зафиксированы в артефактах ДО этой ветки (каталоги `/tmp` 2026-10-29/30: slug=second-instance/scale-add/wal-tli/pool-ac*). Команда: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --nologo`; прогон 2026-10-30 19:29–20:08 (38 м 49 с), ветка `feat-t03-doorman-mandatory`, коммит `033a857a`; итог 57 passed / 14 failed / 2 skipped (2 TLI — штатный `_prepareSkip`). Логи: `arch/roadmap/bugs/t02-e2e-etcd-dns-saturation/` (e2e-gate-2026-10-30.log — сводка прогона; wal-shard1a-etcd-errors-sample.log — образцы etcd-сбоев). Ожидание: E2E-сессии деградируют допустимо при «чужой нагрузке» (t90); факт: embedded-DNS docker не выдерживает одновременный churn ~50 контейнеров E2E + полный стенд. Направление: изоляция DNS-нагрузки (свои networks с `--opt=com.docker.network.bridge.name`/alias-кэширование не помогают — резолв идёт через встроенный DNS daemon); варианты — прогон полного E2E при остановленном стенде в гейте, снижение maxParallelThreads, retry-политика в фикстуре на DNS-флапы, вынос etcd-алиасов на IP.
- **t03-addshard-master-host-match** — `AddShardProcess.ResolveMasterAsync` (src/PgWorker.Provisioning/Processes/AddShardProcess.cs ~382) резолвит master-ноду для SQL-фаз по ХОСТ-части master-ключа (`shard.Master.Split(':')[0]`, `FirstOrDefault(p => p.Value.Host == left || p.Key == left)`): на однохостовом контуре (E2E host-модель, все ноды `host.docker.internal`) матч недискриминантен — FirstOrDefault возвращает первую ноду словаря (shard3a), даже когда ключ указывает на другую ноду. Факт: shard3b — «leader with the lock» (стабильно по логам Patroni), SQL `CREATE DATABASE` упорно уходит на порт shard3a → `25006: cannot execute CREATE DATABASE in a read-only transaction`, add-shard не доводится (тест `E2eSecondInstanceScenarios.SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates` [FAIL], прогон 2026-10-30 19:47, ветка `feat-t03-doorman-mandatory`, коммит `033a857a`; итоги в e2e-логе кейса t02). Сиблинг «finding P1» (недискриминантный host-match в `ClusterSecretRotator.ResolveMasterAsync` — там резолв переведён на doorman-порт): здесь тот же класс — матчить по ПОЛНОМУ ключу `host:<doorman-port>` (S4: после t03 пулер обязателен, ключ всегда дискриминантен), fallback по пробам Patroni остаётся. До t03 ветка маскировалась исходом выборов Patroni (лидер совпадал с первым узлом словаря). Команда повтора: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~SecondInstance_ContainerKillMidAdd --nologo`. Логи: `arch/roadmap/bugs/t03-addshard-master-host-match/` (worker-ew2-25006.log — лог воркера с итерациями 25006).
