# Журнал: t01-kafka-rotation-timeout

Диагностический журнал задачи (план, раздел «Соглашения»). Логи прогонов —
в `/tmp/pgw-kfw-t01-*.log` (в репозиторий не коммитятся).

## Диагноз

**Ветка матрицы (spec §5.4): D4 — невоспроизведённый** (зафиксирована на
шаге A3 по телеметрии A1/A2; **ПЕРЕСМОТРЕНА на D2 красным приёмочной
серии — см. раздел «Красный приёмочной серии (п.13)» ниже**).

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

## Решения (пост-диагнозные, D2-ветка; spec §8 Р10–Р16)

- **Р11 — правило afterCommit-хвоста (arch/16 §5 H, arch-first)**: waiting-фазы
  журнала пишутся только при живой заявке; хвост (`rotated-commit`/`phase-c`,
  заявки нет) в точках ожидания журнал НЕ перезаписывает — тихая передержка
  тика. Обоснование: маркер мутации — единственный носитель возобновляемости
  фазы C; его затирание делало финал недостижимым (наблюдаемая телеметрия
  приёмочной серии: 195 тиков-чтений при одном `updated_unix`).
- **Р12 — форма фикса: тихий пропуск `Result<bool>.Success(false)`** в обеих
  waiting-точках хвоста (ca-window-гвард, слепой преф-чек describe), по
  образцу уже существующей тихой ветки слепого снапшота (строки 141–149):
  тик — Success, журнал не тронут, хвост доигрывается следующим тиком по
  journal-маркеру.
- **Р13 — без перезаписи last_error / обогащения журнала**: диагностическую
  ценность несёт тестовая телеметрия §5.1; прод-журнал не меняется (маркер
  фазы остаётся застывшим — наблюдаемость стагнации хвоста панельным
  алертом `kafka-rotation-stale`).
- **Р14 — форма юнит-кейсов**: существующий тест
  `Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry` ассертировал
  дефектное поведение — заменён на
  `Run_AfterCommitTailAndOpenCaWindow_TailKeptAndResumed` (хвост цел + доигрывание
  после закрытия окна K); добавлены
  `Run_AfterCommitTailDescribeTransient_TailKeptAndResumed` (корневой
  сценарий t01: describe-транзиент в хвосте) и контроль канона
  `Run_LiveTicketDescribeFail_WaitsWaitingClusterAsBefore` (живая заявка —
  waiting-cluster как раньше). Кейс «живая заявка + открытое окно K» покрыт
  существующим `Run_OpenCaWindow_WaitsCaWindow` — дубль не добавлен.
- Коммиты: arch `a929495a` (СТРОГО до кодового), код+юниты — следующий.

## Прогоны

| # | Дата (2026-10-23/24) | Команда (кратко) | Лог | Исход |
|---|---|---|---|---|
| 1 | 24.10, ~22:10 | `dotnet build src/PgWorker.slnx -c Release --nologo` | `build-a0.log` | 0 errors / 0 warnings (после правки пробы; первый прогон — CS1501, исправлен) |
| 2 | 24.10, ~22:15 | изолят AC10 (filter `RotationTimeoutTests.BothTickets…`, PGW_TEST_DOCKER=1, --no-build) | `isolate-1.log` | Зелёный: 1/1, 1 м 5 с |
| 3 | 24.10, ~22:19 | серия solutionx (filter `IntegrationTests`) | `series-1-aborted-unprepared.log` | Прерван вручную: PgWorker E2E — мгновенные фейлы «E2E statics are not prepared» (не выполнен E2ePrepare); KafkaWorker-часть не успела |
| 4 | 24.10, ~22:26 | `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` | `e2e-prepare-a2.log` | Готово: образы e2e собраны, readiness stamp, 25 с |
| 5 | 24.10, 22:27–23:27 | серия solutionx (та же команда, что в баге) | `series-1.log` | AC10 ЗЕЛЁНЫЙ; серия красная по чужим (PgWorker 17, Valkey 1, KafkaWorker-TopicLifecycle 1); SERIES_EXIT:1 |
| 6 | 24.10, после серии | зачистка `pgw-*`/`kfw-*` + сеть | `cleanup-series-1.log`, `evidence-series-1-leftover-*.log` | 0 остатков, `kfw-net`/`pgw-net` отсутствуют |
| 7 | 24.10, ~23:37 | `dotnet build` (A4) | `build-a4.log` | 0 / 0 |
| 8 | 24.10, ~23:40 | изолят AC10 (A4-механика) | `isolate-2.log` | Зелёный: 1/1, 56 с |
| 9 | 24.10, ~23:5x | свежая сборка приёмки | `accept-build.log` | 0 / 0 |
| 10 | 25.10, ~00:0x | юниты KafkaWorker.UnitTests | `accept-units.log` | Зелёные: 359/359 |
| 11 | 25.10, ~00:1x | изолят RotationTimeoutTests (3 кейса) | `accept-isolate.log` | Зелёный: 3/3, 1 м 30 с |
| 12 | 25.10, ~00:2x | полный KafkaWorker.IntegrationTests | `accept-full.log` | AC10 зелёный; 73/74 (фейл `Api.MetricsTests.Metrics_WorkerSeries_AfterFirstTick_SingleInstrumentation`, вне области — см. `evidence-metrics-fail.log`); маркеры «found 2»/«Broker handle destroyed»/SIGABRT = 0 |
| 13 | 25.10, ~00:3x–01:3x | приёмочная серия solutionx (D4) | `accept-series.log` | КРАСНАЯ — см. «Красный приёмочной серии (п.13)» |
| 14 | 25.10, ~07:5x | `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` (заново, Р16) | `d2-e2e-prepare.log` | Штамп записан, 20 с |
| 15 | 25.10, ~07:5x | `dotnet build` (отдельной командой) | `d2-accept-build.log` | 0 / 0 |
| 16 | 25.10, ~08:0x–08:1x | docker-E2E PgWorker `Scale_AddEmptyShard` | `d2-e2e-scale.log` | Зелёный: `Skipped!` + exit 0 — класс исполнен раннером успешно (штатная архитектура PgWorker E2E: зелёный исход факта = Assert.Skip, тело в раннере; runner.log в артефактах без фейлов, teardown полон) |
| 17 | 25.10, ~08:2x | юниты KafkaWorker.UnitTests | `d2-units.log` | Зелёные: 361/361 |
| 18 | 25.10, ~08:3x | изолят RotationTimeoutTests | `d2-isolate.log` | AC10 зелёный; 2/3 — AC12 `RebalanceTicket…` FAIL 17 с «Broker handle destroyed without termination» в UpAsync (вне области D2-фикса: дифф трогает только waiting-точки PasswordRotator; транзиент-класс фабрики, в соседнем прогоне d2-full зелёный; пункт `t09` bugs.md). Улика: `evidence-d2-isolate-ac12-broker-handle.log` |
| 19 | 25.10, ~08:4x | полный KafkaWorker.IntegrationTests | `d2-full.log` | **74/74 ЗЕЛЁНЫЙ** (AC10, AC12, TopicLifecycle, Metrics — все), 5 м 31 с; маркеры «found 2»/«Broker handle destroyed»/SIGABRT = 0 |
| 20 | 25.10, ~09:0x–10:2x | полная серия solutionx (Р15) | `d2-series.log` | **AC10 ЗЕЛЁНЫЙ — KafkaWorker 74/74, 7 м 41 с** (фикс подтверждён под полной нагрузкой); далее PgWorker E2E: 17 внеобластных фейлов (10–15 м), затем прогон ОСТАНОВЛЕН ВНЕШНИМ ЛИМИТОМ (MSB4166 «Child node exited prematurely» — дочерний процесс убит хостом, не тестовый фейл); KafkaWorker-часть завершена и зафиксирована до остановки; зачистка 159 контейнеров + 3 сети выполнена |

## Переприёмка (шаг 8, D2) — итог

Гейт t01: **AC10 зелёный в полной series solutionx на свежем Release со
свежим E2ePrepare-штампом** (п.20) + изолят (AC10 зелёный, п.18) + полный
прогон проекта 74/74 (п.19) + E2E-гейт PgWorker Scale_AddEmptyShard (п.16) +
юниты 361/361 (п.17) + сборка 0 warnings (п.15). Обрыв PgWorker-фрагмента
серии — внешний лимит хоста (MSB4166), не тестовый исход; внеобластные
PgWorker-фейлы заведены пунктом `t10` bugs.md. Повторов сверх Р15 нет.

## Красный приёмочной серии (п.13) — воспроизведение AC10 с телеметрией

Приёмочная серия запущена с ошибкой подготовки PgWorker-части: штамп
E2ePrepare (22:27:55) устарел относительно правок A4 (23:38:03) → 55
PgWorker E2E мгновенных fail-fast «sources are newer than the preparation»
(штатный гейт t09; контейнеры не создавались). KafkaWorker-часть от штампа
не зависит (свежая сборка) — её исходы валидны.

**AC10 — КРАСНЫЙ (4 м 53 с), телеметрия A0 сработала:**

```
phase H: backstop 260 s exhausted ...; work={"op":"rotate","phase":"waiting-cluster",
"instance":"135fc12e21b7","updated_unix":1792799826}; rotations=<none>;
ca_rotations={...}; outcome=<none>; snapshot: endpoints=True app_password=True
admin_password=True ca_pem=True; describe probe=ok: brokers=1; createdClients=5;
phase ticks: rotate:phase-a=1,rotate:waiting-cluster=195,rotate:rotated-commit=1
```

Разбор по коду (`src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs`):

1. Ротация H прошла A и B: `phase-a=1`, `rotated-commit=1` (маркер ставится
   В НАЧАЛЕ фазы C, строка 217), заявка удалена фазой B (`rotations=<none>`),
   исход ещё не записан (`outcome=<none>`).
2. В afterCommit-хвосте фазы C случился транзиент describe (брокер
   пересоздаётся rolling C) — джоб ушёл в преф-чек-точку (строки 159–162):
   `WaitOrExpireAsync(ticket: null, "waiting-cluster", mutationLive: true)`.
3. **ДЕФЕКТ**: `WaitOrExpireAsync` (строка 306) перезаписывает журнал на
   `waiting-cluster`, затирая `rotated-commit`. Детектор `afterCommit`
   (строки 117–119) признаёт ТОЛЬКО `rotated-commit`/`phase-c` → следующий
   тик: `ticket is null && !afterCommit` → `Success(false)` (строка 121–122)
   — вечный no-op. Фаза C НЕВОЗОБНОВИМА: финал (исход done, journal done)
   недостижим никогда.
4. Телеметрия сходится полностью: РОВНО ОДНА запись waiting-cluster
   (`updated_unix` не меняется — джоб больше не пишет журнал), 195 тиков —
   чтения тестом неизменного журнала, `describe probe=ok: brokers=1`
   (брокер жив; слепоты НЕТ — джоб в no-op вообще не доходит до describe),
   `createdClients=5` (один транзиент → пара Unhealthy-пересозданий, без
   шторма).

**Диагноз меняется: D4 → D2 (систематика, прод-дефект подтверждён кодом).**
Маркер матрицы §5.4-D2 присутствует («describe-проба тестом УСПЕШНА при
waiting-джобе»); корень сформулирован: затирание afterCommit-хвоста
waiting-фазой делает хвост C невосстановимым. Ожидаемое по канону §5 H:
«журнал роли показывает незавершённую мутацию (phase-a/rotated-commit/
phase-c …) → чистый waiting без экспирации» — журнал не должен терять
маркер мутации, по которому возобновляется C. Оба прошлых зелёных прогона
AC10 (изолят, серия A2) — прогоны, где транзиент describe в хвосте C не
выпадал (в изоляте нагрузка минимальна).

Сопутствующий красный той же серии: `ReassignmentTests.Drain_RemovesNonEmpty
Broker_ReducesRF` — «Bind for 0.0.0.0:21643 failed: port is already
allocated» (3 повторных старта брокера, docker 500) — порт-коллизия среды,
вне области t01.

Улики: `/tmp/pgw-kfw-t01-evidence-accept-series-ac10-fail.log` (блок падения
с телеметрией), `…-pgw-fail.log` (fail-fast штампа), `…-ctrs.log`/`…-nets.log`
(docker-улики после серии: контейнеров 0 — фикстуры демонтировали; 1 сеть
`pgw-net-c2eca3ca`), `/tmp/pgw-kfw-t01-accept-series.log` (полный лог).

## Закрытие

**t01 закрыт КАК ИСПРАВЛЕННЫЙ** (не «невоспроизведённый» — баг был
воспроизведён телеметрией в приёмочной серии и починен; Р8/Р10):

- Корень: прод-дефект PasswordRotator — afterCommit-хвост (заявка снята
  фазой B) в waiting-точках перезаписывал журнал (`rotated-commit`/`phase-c`
  → `waiting-cluster`), детектор afterCommit маркер терял, фаза C становилась
  невозобновляемой — ротация-«зомби» без финала и исхода.
- Фикс (spec §5.5): waiting-фазы пишутся ТОЛЬКО при живой заявке; хвост в
  точках ожидания — тихий пропуск `Success(false)` без записи журнала (обе
  точки: ca-window-гвард и слепой преф-чек describe; слепой снапшот уже был
  тихим). Arch-first: `arch/16-kafkaworker.md` §5 H дополнен правилом
  (коммит `a929495a` строго до кодового `71fff40f`).
- Приёмка: юниты 361/361 (включая три кейса нового контракта), изолят
  ротаций (AC10 зелёный), полный KafkaWorker.IntegrationTests 74/74
  (маркеры 0), docker-E2E PgWorker Scale_AddEmptyShard (зелёный, свежий
  штамп), полная серия solutionx — AC10 зелёный (KafkaWorker 74/74) на
  свежем Release.
- Пункт удалён из `arch/roadmap/bugs.md` этим же закрытием; подпапка
  `arch/roadmap/bugs/t01-kafka-rotation-timeout/` локально отсутствовала
  (утрачена до задачи — удалять нечего).
- Внеобластные красные прогонов задачи — пунктами `t06`–`t10` bugs.md
  (TopicLifecycle-flake, MetricsTests-flake, Valkey Metrics-flake,
  ReassignmentTests порт/BrokerHandle, PgWorker E2E пачка серий); не чинились
  в этой задаче по правилу «фейл вне области — отдельная задача».
