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

- **`t02-pool-failover-selfheal`** — E2E-самолечение пула после исчерпания не
  сходится в бюджет: `PgWorker.IntegrationTests.E2e.PoolFailoverExhaustionScenarios.Pool_FailoverList_And_Exhaustion_SelfHeal`
  — FAIL 11 м 19 с (серия `FullyQualifiedName~IntegrationTests`, 2026-10-23,
  ветка `feat-t24-walreceiver-tli-jump`, HEAD `4f80809e`, база main
  `91358f63`); сценарий `pool-ac4`, run `98792608d19c4b2896411e73bcc5c8a0`.
  Ошибка: `Expected boolean to be True because raising the node — self-healing
  back to ACTIVE (retry loop), but found False` (`E2eBackupPoolScenarios.cs:610`).
  Ожидаемое: после исчерпания пула (exhaustion) и подъёма ноды воркер возвращает
  кластер в ACTIVE в бюджете гейта (retry loop). Наблюдаемое: за 11 м 19 с
  кластер в ACTIVE не вернулся; в timing прогона — хвостовые ожидания чтения
  writer 185,6 с и 300,4 с. Артефакты (63 МБ, целиком):
  `arch/roadmap/bugs/t02-pool-failover-selfheal/e2e-artifacts-6adb2309774347f699932468c20e45c8/`
  (docker-логи/inspect всех 13 контейнеров `pgw-*pac498792608*`, `pgw-ew-*`
  воркеров, `timing.log`, `snapshots/`, `README-cleanup.txt` — контур был
  СТОПНУТ и не удалён для разбора, зачищен после фиксации артефактов);
  фрагмент журнала серии — `series2-pool-failover-fail.log`. Гипотезы:
  воркер не выходит из деградации после подъёма ноды (пересоздание
  агентов/джобов при исчерпанном пуле) — разбирать по docker-логам
  `pgw-ew-pac4b` и `host.log` в артефактах.
- **`t03-docker-image-pull-timeout`** — таймаут HttpClient 7,5 с на
  `/images/create` под нагрузкой серии:
  `PgWorker.IntegrationTests.Docker.EngineSupersetTests.CreateContainer_MissingImage_PullsAndRetries`
  падал 2/2 прогонов полной серии (7 с каждый), 2026-10-23, ветка
  `feat-t24-walreceiver-tli-jump` (HEAD `4f80809e`, база main `91358f63`).
  Фильтр: `--filter "FullyQualifiedName~EngineSupersetTests"`; команда серии:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
  -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`.
  Ошибка: `Expected created.Error!.Message "The request was canceled
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
- **`t04-kfw-topiclifecycle-deleteticket-flake`** — del-заявка не удаляет
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
- **`t05-kfw-metrics-workerseries-flake`** — серия метрик без loop=reconcile:
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
- **`t06-valkey-metricsdomain-flake`** — живой кластер Valkey без словарей:
  `ValkeyWorker.IntegrationTests.Api.MetricsDomainTests.Metrics_
  LiveCluster_All15DictionaryNames` — FAIL 1 м 44 с (серия
  `FullyQualifiedName~IntegrationTests`, 2026-10-24, HEAD `7448fdd1`).
  Команда серии — та же (см. t04). Ожидаемое: 15 словарей в метриках живого
  кластера. Наблюдаемое:
  фейл ассерта словарей (текст — в логе серии); в следующей полной серии
  (d2-series) тест зелёный — флейк. Логи:
  `arch/roadmap/bugs/t06-valkey-metricsdomain-flake/` (выжимка). Гипотезы:
  тайминг-зависимый срез под нагрузкой серии.
- **`t07-kfw-reassignment-port-brokerhandle-flake`** — два эпизода одного
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
- **`t08-pgw-e2e-series-fails`** — пачка красных PgWorker.IntegrationTests
  E2E в полных сериях solutionx (2026-10-24, команда — та же, см. t04):
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
