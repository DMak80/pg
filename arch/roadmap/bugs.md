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

- **`t10-pgw-e2e-series-fails`** — пачка красных PgWorker.IntegrationTests
  E2E в полных сериях solutionx (2026-10-24, команда:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
  -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`):
  серия A2 (HEAD `7448fdd1`) — 17
  фейлов (Restore/Drill/WalStream/Pool…, 8–16 м, артефакты контуров —
  `$TMPDIR/pgw-e2e-artifacts-<guid>/`); приёмочная серия — 55 мгновенных
  fail-fast «sources are newer than the preparation» (процедурный эпизод —
  прогон с устаревшим штампом E2ePrepare, не баг кода); серия D2
  (HEAD `71fff40f`) — 17 фейлов (Pool/TliSwitch/WalStream/Drill/Restore…,
  10–15 м; прогон оборван внешним лимитом хоста на PgWorker-фазе ПОСЛЕ
  зелёного KafkaWorker-прогона, MSB4166 «Child node exited prematurely»).
  Списки фейлов и выжимки — `arch/roadmap/bugs/t08-pgw-e2e-series-fails/`;
  полные логи серий — `/tmp/pgw-kfw-t01-series-1.log`,
  `/tmp/pgw-kfw-t01-d2-series.log` (временные). Гипотезы: PgWorker E2E под
  параллельной нагрузкой полной series solutionx — нагрузочный класс,
  разбирать по артефактам контуров (docker-логи, timing).
- **`t11-walreceiver-restapi-ca-probe`** — PatroniRoleProbe в приёмнике WAL
  не поднимает роль из CA-материала: `X509Certificate2.CreateFromPemFile`
  требует пару cert+key, а env `SSL_RESTAPI_CA` (RestCertificateCache.CaPem —
  `caCertPem`) несёт ТОЛЬКО сертификат → CryptographicException «The key
  contents do not contain a PEM…» → роль навсегда none → приёмник замирает в
  `waiting_role`, доставка WAL не идёт. Найдено в мерж-гейте t25
  (2026-10-24, ветка `feat-t25-wal-receiver-in-pgnode`, Worktree-прогон
  wal/pool/tli): 10/10 фейлов серий `E2eWalStream|E2eTliSwitch|E2eBackupPool`
  (8–18 мин, каскады «both receivers must stream»), офлайн-контур
  pgworker-node:dev воспроизвёл и подтвердил (логи в подпапке). Исправление
  сделано в t25 (`X509CertificateLoader.LoadCertificateFromFile` —
  сертификат-онли); пункт закрывает собой объяснение пачки фейлов t08
  (WalStream/Pool/TliSwitch — те же ассерты «both agents/receivers …»):
  при разборе t08 проверить её серии на этот корень и закрыть пункт t08
  как дубликат, если подтверждается. Дополнительный угол t25-серии (логи в
  подпапке): двойной промоут 2→3→4 в `E2eTliSwitchScenarios` —
  `00000003.history` отсутствует в S3 (transition не наблюдён приёмником —
  по spec §3.1 п.6 это легитимный `history_missing`-факт), restore
  `target_time` честно FAILED «TLI transition 2→3 without 00000003.history»;
  гарантия доставки .history при ПРОПУСКЕ перехода (промоут-гонка) — вне
  контракта t25, лечение (периодический ре-доклад истории приёмником или
  иной канал) — отдельная задача.
  Логи: `arch/roadmap/bugs/t11-walreceiver-restapi-ca-probe/`
  (выжимка серии walpool2: 10 фейлов, [PHASE]-хвосты, `[TIMING]`,
  diag-фрагменты; повторные прогоны после фикса — там же).
- **`t12-pool-failover-exhaustion-marker-missing`** —
  `PoolFailoverExhaustionScenarios.Pool_FailoverList_And_Exhaustion_SelfHeal`
  (E2E, docker): Act B «policy [node1] + стоп node1» — DEGRADED с маркером
  `s3-pool-exhausted` в wal-ключе НЕ появляется за 300 c (ассерт
  E2eBackupPoolScenarios.cs:600); приёмник мастера при этом жив (supervisord
  RUNNING, роль master по REST-пробе, ключ agent/&lt;N&gt; жив). Юнит-контур
  «флаг pool_exhausted в agent-ключе → DEGRADED с маркером» покрыт и зелёный
  (WalStreamProcessTests.ExhaustionFlagDegraded_ThenFlagClearedHealsActive,
  t25). Разбор невозможен: teardown Pool-контура при MarkFailed НЕ сохранил
  docker-логи нод (в артефактах только etcd/mc/owns3/worker — канон
  docs/e2e-launch.md требует «docker-логи/inspect ВСЕХ контейнеров») —
  телеметрия Pool-контуров подлежит починке. Прогон 2026-10-24, ветка
  `feat-t25-wal-receiver-in-pgnode` (Шаг 12 t25, серия pool2); лог серии:
  `arch/roadmap/bugs/t12-pool-failover-exhaustion-marker-missing/`
  (pool2-лог + runner.log выжимка). Гипотезы: (а) приёмник в Act B стоит в
  ожидании (waiting_policy/slot) — флаг не ставится; (б) put-маркер
  PoolBackupS3 теряется между приёмником и статус-put; (в) гонка рестарта
  воркера и policy-обновления. Повтор прогонки — с починенной телеметрией.
- **`t13-restore-pitr-recovery-short-of-archive`** — E2E restore PITR
  (`E2eRestoreScenarios.Restore_TargetTime_PitrRollback`,
  `Restore_Latest_RebuildsDestroyedShard`, 2026-10-25, ветка
  `feat-t25-wal-receiver-in-pgnode`, серия right1 Шага 12 t25): оба факта —
  «Expected "10"… but "5"» — recovery применил только T0-порцию (до полного),
  восстановлено 5 контрольных строк из 10; архив ПЕРЕД заявкой полон
  (сценарные гейты WalArchivedAsync ws+5/ws+8 и WalChainContinuous прошли,
  restore COMPLETED, восстановленный LSN — внутри сегмента), т.е. recovery
  НЕ докатился по FULL архиву. Restore-джобы и архивация t25 не менялись;
  restore-джоб-логи невозможно разобрать: демонтаж сносит джоб-контейнеры
  ДО телеметрии teardown (docker logs недоступны; etcd-journal сносит
  teardown) — телеметрия restore-сценариев подлежит починке (лог джоба в
  артефакты до сноса). Прогон/логи:
  `arch/roadmap/bugs/t13-restore-pitr-recovery-short-of-archive/`
  (right1-лог, runner.log, host.log воркеров rs-time/rs-latest). Гипотезы:
  (а) restore-джоб скачивает WAL-список снимком на старте и не докачивает
  поздние сегменты (restore_command retry-цикл); (б) recovery_target_time
  останавливается на конце скачанного архива (promote без target);
  (в) гонка «архивация приёмником ↔ PLANNED-заявка». Разобрать по
  телеметрии джоба.
