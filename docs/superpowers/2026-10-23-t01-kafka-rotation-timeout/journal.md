# Журнал: t01-kafka-rotation-timeout

Диагностический журнал задачи (план, раздел «Соглашения»). Логи прогонов —
в `/tmp/pgw-kfw-t01-*.log` (в репозиторий не коммитятся).

## Диагноз

**Ветка матрицы (spec §5.4): D4 — невоспроизведённый.**

Критерии веток, проверенные по телеметрии обеих ступеней:

- **D1** (изолят зелёный; серия красная по AC10; describe-пробы падают
  таймаутом/перегрузкой) — НЕ подтверждён: AC10 зелёный и в изоляте,
  и в полной серии; красных исходов фаз H/K нет, describe-проба не
  вызывалась (сборщик телеметрии срабатывает только при падении).
- **D2** (красный изолят либо клиент-сторонние маркеры: describe-проба
  успешна при waiting-джобе, рост `createdClients`, гэп-отказы
  `[kfw-adminfactory]`) — НЕ подтверждён: изолят зелёный; маркеров
  `[kfw-adminfactory]` в логе серии 0 (`grep -c` = 0); AC10 зелёный.
- **D3** (средовые улики: осиротевшие сети/контейнеры, исказившие прогон) —
  НЕ подтверждён: перед обоими прогонами среда чистая
  (`docker ps -a | grep kfw-` и `docker network ls | grep kfw-net` — пусто);
  UpAsync прошёл, кластер AC10 поднялся и доигран.
- **D4** (баг не воспроизвёлся) — ПОДТВЕРЖДЁН: `BothTicketsPasswordAndCa_
  PlayThroughWithoutExpiry` зелёный в изоляте (1/1) и в полной серии
  (в составе KafkaWorker.IntegrationTests 73/74 — единственный красный
  класса — `TopicLifecycleTests.DeleteTicket_RemovesTopicAndKeys`,
  вне области t01).

Оговорка к букве критерия D4 («серия зелёная»): полная серия завершилась
красной, но ВСЕ красные — вне области t01 (17 PgWorker.IntegrationTests
E2E + 1 ValkeyWorker + 1 KafkaWorker TopicLifecycle); предмет задачи —
AC10 — зелёный в обеих ступенях. Ветки D1–D3 исключены критериями
(все требуют красного AC10 или его маркеров); невоспроизведение бага
зафиксировано. Падения вне области — по правилу AGENTS.md отдельные
задачи в `arch/roadmap/bugs.md` (см. «Закрытие»).

Улики:

- `/tmp/pgw-kfw-t01-isolate-1.log` — изолят AC10, `Passed! - Failed: 0,
  Passed: 1, Skipped: 0, Total: 1, Duration: 1 m 5 s`, TEST_EXIT:0.
- `/tmp/pgw-kfw-t01-series-1.log` — полная серия (58 м 58 с):
  SdGenerator 6/6, AdminPanel 184/184, S3Worker 54/54,
  ValkeyWorker 50/51 (фейл `Metrics_LiveCluster_All15DictionaryNames`),
  KafkaWorker 73/74 (фейл `DeleteTicket_RemovesTopicAndKeys`), PgWorker
  366/422 (17 фейлов, 39 skip), SERIES_EXIT:1; `grep -c BothTickets…`
  в блоке Failed — 0 (AC10 зелёный).
- `/tmp/pgw-kfw-t01-evidence-kfw-topiclifecycle-fail.log`,
  `/tmp/pgw-kfw-t01-evidence-valkey-fail.log` — выжимки чужих фейлов
  (тексты ассертов и стеки) из лога серии.
- `/tmp/pgw-kfw-t01-evidence-series-1-leftover-{ctrs,nets}.log` — снимок
  остатков серии ДО зачистки (~140 pgw-* контейнеров + сеть pgw-net-c2b9c2b7,
  оставлены упавшими E2E-сценариями по правилам телеметрии MarkFailed);
  после зачистки — 0.
- Артефакты упавших PgWorker E2E — в
  `$TMPDIR/pgw-e2e-artifacts-<guid>/` (docker-логи сняты teardown'ом;
  разбор — в roadmap-задаче, не здесь).
- `/tmp/pgw-kfw-t01-series-1-aborted-unprepared.log` — первый запуск серии,
  прерван (см. «Прогоны» п.3); улика причины — мгновенные фейлы
  PgWorker E2E «E2E statics are not prepared».

Условия невоспроизведения (для будущего повтора, если баг вернётся):

- база: worktree `fix-t01-kafka-rotation-timeout`, коммит `7448fdd1`
  (код `src/KafkaWorker.*` идентичен базе бага `91358f63`, проверено
  `git log 91358f63..HEAD` по путям — пусто);
- команды: изолят и серия — как в «Прогонах» п.2/п.5;
- телеметрия §5.1 влита в AC10 (счётчик тиков по journal-фазам +
  `RotationDiagAsync` в ассертах H/K) — следующий красный прогон
  AC10 несёт полный диагностический контекст в сообщении падения.

## Решения

- **A0-правка пробы `RotationDiagAsync` (отступление от буквы подпункта
  1.2, зафиксировано)**: snippet плана вызывал
  `admin.DescribeClusterAsync(options, cancellationToken)` — в проектном
  Confluent.Kafka 2.15 перегрузки с 2 аргументами нет (компиляция упала,
  CS1501); рабочий вызов — `DescribeClusterAsync(new DescribeClusterOptions
  { RequestTimeout = TimeSpan.FromSeconds(10) })` (прецедент —
  `src/AdminPanel.Probes/Kafka/ConfluentKafkaProbeClient.cs`). Канон пробы
  не изменён: RequestTimeout 10 с, текст ошибки в catch, число брокеров
  из `ClusterMetadata.Nodes`. Правка — только в RotationTimeoutTests.cs.
- **E2ePrepare перед серией (A2)**: первый запуск серии прерван —
  PgWorker E2E падали мгновенной пачкой «E2E statics are not prepared»
  (обязательная статическая подготовка по AGENTS.md не была выполнена).
  Прогон не эквивалентен серии бага (E2E-часть умерла до нагрузки).
  Выполнена подготовка одним процессом
  (`dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`, лог
  `/tmp/pgw-kfw-t01-e2e-prepare-a2.log`, «readiness stamp written», 25 с),
  серия перезапущена. Причина прерывания полностью известна и сохранена
  (`/tmp/pgw-kfw-t01-series-1-aborted-unprepared.log`) — это исправление
  подготовки окружения, не «перезапуск для выяснения что было».
- Механические решения шага A4 — см. ниже (дополняется).

## Прогоны

| # | Дата (2026-10-23/24) | Команда (кратко) | Лог | Исход |
|---|---|---|---|---|
| 1 | 24.10, ~22:10 | `dotnet build src/PgWorker.slnx -c Release --nologo` | `build-a0.log` | 0 errors / 0 warnings (после правки пробы; первый прогон — CS1501, исправлен) |
| 2 | 24.10, ~22:15 | изолят AC10 (filter `RotationTimeoutTests.BothTickets…`, PGW_TEST_DOCKER=1, --no-build) | `isolate-1.log` | Зелёный: 1/1, 1 м 5 с |
| 3 | 24.10, ~22:19 | серия solutionx (filter `IntegrationTests`) | `series-1-aborted-unprepared.log` | Прерван вручную: PgWorker E2E — мгновенные фейлы «E2E statics are not prepared» (не выполнен E2ePrepare); KafkaWorker-часть не успела |
| 4 | 24.10, ~22:26 | `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` | `e2e-prepare-a2.log` | Готово: образы e2e собраны, readiness stamp, 25 с |
| 5 | 24.10, 22:27–23:27 | серия solutionx (та же команда, что в баге) | `series-1.log` | AC10 ЗЕЛЁНЫЙ; серия красная по чужим (PgWorker 17, Valkey 1, KafkaWorker-TopicLifecycle 1); SERIES_EXIT:1 |
| 6 | 24.10, после серии | зачистка `pgw-*`/`kfw-*` + сеть | `cleanup-series-1.log`, `evidence-series-1-leftover-*.log` | 0 остатков, `kfw-net`/`pgw-net` отсутствуют |

## Закрытие

(дополняется в фазе A6)
