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

- **`t01-kafka-rotation-timeout`** — джоб rotate KafkaWorker не выходит из фазы
  `waiting-cluster` в фазный бюджет: `KafkaWorker.IntegrationTests.Kafka.RotationTimeoutTests.BothTicketsPasswordAndCa_PlayThroughWithoutExpiry`
  падал 2/2 прогонов полной серии (4 м 39 с и 4 м 47 с), 2026-10-23, ветка
  `feat-t24-walreceiver-tli-jump` (HEAD `4f80809e`, база main `91358f63`);
  ветка задачей не меняла код KafkaWorker.
  Фильтр: `--filter "FullyQualifiedName~RotationTimeoutTests"`; команда серии:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
  -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`.
  Ошибка: `Expected passwordSeenDone to be True because H must have completed
  within the phase budget (with a LIVE ca ticket — deadlock broken) …, but
  found False` (`RotationTimeoutTests.cs:353/401`); журнал джоба rotate — все
  записи `{"op":"rotate","phase":"waiting-cluster",…}` без смены фазы.
  Ожидаемое: джоб H завершается в фазном бюджете при живом CA-тикет
  (deadlock broken). Наблюдаемое: джоб циклится в `waiting-cluster` весь
  бюджет. Логи: `arch/roadmap/bugs/t01-kafka-rotation-timeout/`
  (обе серии целиком; фейлы — series1: строки 616–630, series2: 67–80).
  Гипотезы: стабильный 2/2 фейл под параллельной нагрузкой полной серии —
  проверить вне серии на main-базе; разбор гейтов готовности кластера
  (`waiting-cluster`) и тайкетов rotate.
- **`t03-docker-image-pull-timeout`** — таймаут HttpClient 7,5 с на
  `/images/create` под нагрузкой серии:
  `PgWorker.IntegrationTests.Docker.EngineSupersetTests.CreateContainer_MissingImage_PullsAndRetries`
  падал 2/2 прогонов полной серии (7 с каждый), 2026-10-23, ветка
  `feat-t24-walreceiver-tli-jump` (HEAD `4f80809e`, база main `91358f63`).
  Фильтр: `--filter "FullyQualifiedName~EngineSupersetTests"`; команда серии —
  как в t01. Ошибка: `Expected created.Error!.Message "The request was canceled
  due to the configured HttpClient.Timeout of 7,5 seconds elapsing." to contain
  "/images/create" because the engine must have performed the pull fallback
  (§7.2 canon-superset)` (`EngineSupersetTests.cs:44`).
  Ожидаемое: создание контейнера с отсутствующим образом → движок выполняет
  pull-fallback, сообщение ошибки содержит `/images/create` с деталями pull.
  Наблюдаемое: HTTP-запрос `/images/create` отменяется по `HttpClient.Timeout`
  7,5 с — docker-демон под параллельной нагрузкой серии не успевает ответить.
  Логи: `arch/roadmap/bugs/t03-docker-image-pull-timeout/` (фрагменты обеих
  серий). Гипотезы: жёсткий `HttpClient.Timeout` docker-клиента против
  медленного pull под нагрузкой — разобрать таймаут/ретрай операции pull
  (или предтягивание образа в фикстуре), канон §7.2 не ослабляя.
- **`t04-tligap-writeifchanged-flaky`** — флаки юнит-теста на границе секунд:
  `PgWorker.IntegrationTests.Backups.WalStreamProcessTests.TliGap_WriteIfChanged_Idempotent`,
  2026-10-23, ветка `feat-t02-pool-failover-selfheal` (HEAD `c8204e83`; тот же
  код на `14191d92` — 54/54 зелёным). Команда:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build
  --filter "FullyQualifiedName~WalStreamProcessTests"`. Ошибка: `Expected
  journalUnix1 to be 1792766055L, but found 1792766054L (difference of -1)`
  (`WalStreamProcessTests.cs:1750`). Ожидаемое: второй тик не переписывает
  journal-ключ `/pgworker/work/<C>` (updated_unix идемпотентен). Наблюдаемое:
  updated_unix изменился на +1 между чтениями — запись журнала тика 1 легла в
  одну реальную секунду, а повторная запись (между тиками/во втором тике)
  — в следующую; ассерт на реальных часах нестабилен. Логи:
  `arch/roadmap/bugs/t04-tligap-writeifchanged-flaky/` (лог упавшей серии).
  Гипотезы: секундная гранулярность `DateTimeOffset.UtcNow` в
  `WorkJournal.WritePhaseAsync` + журнальная запись, выполняющаяся при
  удержании gap/между тиками; воспроизводимость ~редкая (1 из прогонов).
  Разобрать писателя второй записи и стабилизировать ассерт (фейковые часы
  журнала либо сравнение payload без updated_unix).
- **`t05-wal-image-aspnet-base`** — образы `pgworker-wal` (dev и e2e) падали на
  старте агента: `Framework 'Microsoft.AspNetCore.App' version '10.0.0' … No
  frameworks were found`. Причина: транзитивный FrameworkReference
  (WalReceiver → PgWorker.Backups → Shared.Metrics) объявляет ASP.NET Core в
  runtimeconfig, а база образов — `dotnet/runtime:10.0` (без ASP.NET Core).
  Вскрылось docker-E2E AC4 (`Pool_FailoverList_And_Exhaustion_SelfHeal`,
  2026-10-23, ветка `feat-t02-pool-failover-selfheal`, артефакты
  `arch/roadmap/bugs/t05-wal-image-aspnet-base/`) — гейт Arrange
  «agents before the node failure» ждал 480 с (агенты в restart-лупе).
  Точечный фикс в задаче t02: база обоих Dockerfile — `dotnet/aspnet:10.0`.
  Оставшееся: проверить, нужен ли WalReceiver'у ASP.NET-рантайм вообще
  (разрыв транзитивной ссылки метрик — задача владельца свипера/метрик).
- **`t06-scale-runner-container-vanished`** — единичный инфраструктурный фейл
  кейс-маркера: `PgWorker.IntegrationTests.E2e.E2eScaleScenarios.Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused`,
  2026-10-23, ветка `feat-t02-pool-failover-selfheal` (HEAD `1bc949ca`;
  в трёх предыдущих прогонах того же дня — зелёный). Команда: `DOTNET_CLI_UI_LANGUAGE=en
  PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter
  FullyQualifiedName~Scale_AddEmptyShard`. Ошибка: «the runner finished with an
  error … runner logs unavailable: docker logs pgw-er-20997ab50f57 → Error
  response from daemon: No such container» — раннер-контейнер исчез до снятия
  docker-логов (в артефакте два run-каталога NameReused-фазы, runner.log
  отсутствует, teardown.txt первого контура failed=False — штатный). Ожидаемое:
  раннер исполняет факт, лог сохраняется. Наблюдаемое: контейнер удалён
  внешним актором до снятия логов — исход недоступен. Логи:
  `arch/roadmap/bugs/t06-scale-runner-container-vanished/`. Гипотезы: конкуренция
  внешнего docker-prune/очистки с живым контуром; внутрираннерный crash без
  передачи exit-кода. Разобрать телеметрию pgw-er-* (telemetry-флаг на снимок
  логов раннера ДО его сноса). ПОВТОР (тот же день, после анализа): второй фейл
  2/2 — ДРУГАЯ точка: `EnableSyncModeAsync` (E2eScaleScenarios.cs:490) — «master
  shard3 must have a sync-standby after the PATCH, but found False» (гейт
  Patroni synchronous_mode в scale-механике, вне области t02); в stack trace
  пути сборки `worktrees/fix-t01-kafka-rotation-timeout` — docker-тег
  `pgw-e2e-runner:e2e` ОБЩИЙ на хосте: параллельный агент перезаписал образ
  между prepare и прогоном (интерференция параллельных агентов). Разобрать:
  per-worktree тег раннера (guid/ветка в имени) + гейт sync-standby (механика
  PATCH/retry).
