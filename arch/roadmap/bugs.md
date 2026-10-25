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

- **`t06-kfw-topiclifecycle-deleteticket-flake`** — del-заявка не удаляет
  топик за гейт: `KafkaWorker.IntegrationTests.Kafka.TopicLifecycleTests.DeleteTicket_RemovesTopicAndKeys`
  — FAIL 38 с (серия `FullyQualifiedName~IntegrationTests`, 2026-10-24,
  HEAD `7448fdd1` — прод-код KafkaWorker идентичен базе). Команда серии:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
  -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`.
  Ошибка: `Expected (TopicExistsAsync(fixture, cluster, topic, ct)) to be
  False, but found True` (`TopicLifecycleTests.cs:185`).
  Ожидаемое: после del-тикета топик удалён. Наблюдаемое: топик жив.
  Логи: `arch/roadmap/bugs/t04-kfw-topiclifecycle-deleteticket-flake/`
  (выжимка из series-1). Гипотезы: delete-тик не успел за гейт под
  параллельной нагрузкой / TopicSync-сходимость медленнее окна — разобрать
  гейт ожидания удаления.
- **`t07-kfw-metrics-workerseries-flake`** — серия метрик без loop=reconcile:
  `KafkaWorker.IntegrationTests.Api.MetricsTests.Metrics_WorkerSeries_
  AfterFirstTick_SingleInstrumentation` — FAIL 178 мс (полный прогон
  `KafkaWorker.IntegrationTests`, 2026-10-24, HEAD `86953df0`). Команда:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
  src/tests/KafkaWorker.IntegrationTests -c Release --no-build`.
  Ошибка: `Expected matches to contain a single item matching
  l.Contains("loop=\"reconcile\"") …, but no such item was found`
  (`MetricsTests.cs:56`; в срезе только `loop="keepalive"`).
  Ожидаемое: после первого тика в метриках есть серия loop=reconcile.
  Наблюдаемое: reconcile-серии нет (тик не успел к срезу). В соседнем
  прогоне (d2-full) тест зелёный — тайминг-флейк. Логи:
  `arch/roadmap/bugs/t05-kfw-metrics-workerseries-flake/` (выжимка).
  Гипотезы: срез метрик до первого reconcile-тика под параллельным стартом
  — разобрать синхронизацию среза/тика.
- **`t08-valkey-metricsdomain-flake`** — живой кластер Valkey без словарей:
  `ValkeyWorker.IntegrationTests.Api.MetricsDomainTests.Metrics_
  LiveCluster_All15DictionaryNames` — FAIL 1 м 44 с (серия
  `FullyQualifiedName~IntegrationTests`, 2026-10-24, HEAD `7448fdd1`).
  Команда серии — та же (см. t06). Ожидаемое: 15 словарей в метриках живого
  кластера. Наблюдаемое:
  фейл ассерта словарей (текст — в логе серии); в следующей полной серии
  (d2-series) тест зелёный — флейк. Логи:
  `arch/roadmap/bugs/t06-valkey-metricsdomain-flake/` (выжимка). Гипотезы:
  тайминг-зависимый срез под нагрузкой серии.
- **`t09-kfw-reassignment-port-brokerhandle-flake`** — два эпизода одного
  теста `KafkaWorker.IntegrationTests.Kafka.ReassignmentTests.Drain_
  RemovesNonEmptyBroker_ReducesRF`:
  (а) серия 2026-10-24 (HEAD `7448fdd1`) — 3 подряд Failed provisioning-тика
  `docker POST /containers/kfw-re11a5aae36-broker4/start responded 500:
  Bind for 0.0.0.0:21643 failed: port is already allocated` (`Reassignment
  Tests.cs:119`) — порт-коллизия portalloc-диапазона с чем-то на хосте в
  момент серии; (б) изолят 2026-10-24 (HEAD `71fff40f`) — FAIL 17 с:
  provisioning-тик Failed `Local: Broker handle destroyed without
  termination` (класс фабрики AdminClient — кандидат 351abbec; в
  соседнем прогоне d2-full тест зелёный — транзиент). Логи:
  `arch/roadmap/bugs/t07-kfw-reassignment-port-brokerhandle-flake/`
  (обе выжимки). Гипотезы: (а) коллизия портов серии с внешними слушателями —
  проверить зонд/диапазон; (б) гонка дрейна-замены клиента фабрики —
  разобрать по arch/16 §6.
- **`t10-pgw-e2e-series-fails`** — пачка красных PgWorker.IntegrationTests
  E2E в полных сериях solutionx (2026-10-24, команда — та же, см. t06):
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
- **`t11-mimosa-clusterdriver-test-creds`** — Mimosa L3 pre-commit на мерж-гейте
  `t01-reassign-exec-channel` (2026-10-25, база `b6c9f4c3`): 112 high +
  6 medium «жёстко прописанные креды». High-часть — inline-литералы у
  кред-имён в тестах (`restPassword: "rest-test-pw"` и т.п.) — закрыта
  мержем t01: вынесены в константы по канону `Fakes.cs` (значения без
  изменений, 31 файл, юниты зелёные). Остаток: 6 medium без построчной
  расшифровки в вердикте хука — классифицировать отдельным прогоном
  (security_scan normal depth), ложные подавить, реальные устранить.
  Логи: `arch/roadmap/bugs/t11-mimosa-clusterdriver-test-creds/`.
