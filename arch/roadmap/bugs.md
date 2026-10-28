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

- **`t14-restore-rejoin-patroni-boot-flake`** — rejoin-фаза restore-заявки:
  Patroni ноды не приходит в бюджет 120 c при живой restore-операции
  (2026-10-28, ветка `bugs-t10-t11-t13`, база main `8b160759`; команда — как
  в t10, фильтры `FullyQualifiedName~E2eRestoreScenarios` и
  `FullyQualifiedName~E2eTliSwitch`). Тесты:
  `E2eRestoreScenarios.Restore_Latest_RebuildsDestroyedShard` (2/2 последних
  прогонов), `E2eTliSwitchScenarios.Restore_TargetTimeThroughTliSwitch_StopsAtGoal`
  и `Restore_LatestThroughTliSwitch_PicksUpWholeTail` (класс-фейл tlsw).
  Восстановление честно COMPLETED (restored_to_lsn корректен, послебакапные
  коммиты применены), дальше REJOINING → permanent-FAILED
  `Patroni did not come up within 120s` в двух видах: (а) probe
  «The SSL connection could not be established» — проба Patroni ноды не
  отвечает всё окно; (б) `members=shard1a:replica:running,shard1b:replica:stopped`
  — восстановленная первая нода поднялась РЕПЛИКОЙ (лидера нет), вторая —
  uninitialized при живом контейнере; в HA-scope остаточный ключ
  `failover={"leader":"shard1a","member":"shard1b","scheduled_at":<момент
  демонтажа>}`, last_failover cause=accelerated. Гипотезы: (1) демонтаж
  (arch/19 §3.5) чистит initialize/leader/sync/status, но НЕ `failover` —
  запланированный switchover прошлой жизни выполняется Patroni
  восстановленной ноды accelerated → лидерство уходит на неинициализированную
  ноду → гейт firstReady (лидер на первой ноде) недостижим; (2) источник
  failover-ключа уточнить (кто пишет его в момент демонтажа). Логи:
  `arch/roadmap/bugs/t14-restore-rejoin-patroni-boot-flake/`.
- **`t15-walstream-promote-ac5-chain-glue`** — AC5 промоут-сценария стрима:
  `E2eWalStreamPromoteScenarios.WalStream_Promote_TliHistory_ChainGlued`,
  2026-10-28, ветка `bugs-t10-t11-t13` (та же серия, что t14; команда — как
  в t10, фильтр `FullyQualifiedName~E2eWalStreamPromote`). После останова
  мастера (docker stop, промоут shard1b, tli=2) гейт «wal-chain-glued»
  (WalChain.CheckWithRestart continuous + wal-ключ ACTIVE, бюджет 300 c)
  не сходится: objects=36, maxTli=0x2, history=present, ok=False — цепочка
  TLI2 с дырой, wal-ключ в WAL_CHAIN_ERROR весь бюджет. Класс t097
  (досыпка старого TLI при смене таймлайна), но t097-фикс (CHECKPOINT на
  мастере) неприменим — мастер stopнут docker'ом. Ожидаемое: CheckWithRestart
  перезапускается от первого сегмента maxTli, цепь непрерывна, ключ ACTIVE.
  Логи: `arch/roadmap/bugs/t15-walstream-promote-ac5-chain-glue/`.
