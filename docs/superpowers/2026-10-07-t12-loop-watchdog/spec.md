# t12-loop-watchdog — watchdog зависших циклов воркера

- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), трек сквозной надёжности, приоритет P2, целевая характеристика **R** (быстрая самовосстанавливаемость).
- **Формулировка**: «исключения циклов ловятся (TickSafelyAsync), но „завис без исключения“ виден только в `loops-alive` healthz; самоперезапуска по staleness нет».
- **Открытые решения**: вопросы пользователю заданы (2026-10-07, через AskUserQuestion) — ответов не получено; решения ниже приняты по best judgment и требуют подтверждения при ревью этой спеки:
  - **Q1 механизм** — выбран внутренний watchdog в воркере (альтернативы: внешний watchdog в панели; healthz 503 + docker HEALTHCHECK);
  - **Q2 охват** — все три воркера (PgWorker, KafkaWorker, ValkeyWorker); панель — НЕ-цель;
  - **Q3 действие при staleness** — graceful `StopApplication` (путь RestartHandler), не `Environment.FailFast`;
  - **Q4 порог** — конфигурируемый, дефолт = 2 × порог healthz loops-alive (60 с при дефолтных интервалах: 3×max(5,5)+15=30, ×2=60);
  - **Q5 анти-луп рестартов** — без программного лимита (docker restart-backoff + метрика/журнал дают видимость).

## 1. Контекст и цель

### 1.1. Проблема

Все фоновые циклы воркеров построены на `TickSafelyAsync`: исключение тика
превращается в ошибку тика (лог + `ErrorDelayMs`), цикл продолжает работать.
Но зависание «без исключения» — await внутри тика не возвращается (зависший
сетевой вызов без таймаута, deadlock, вечное ожидание внешней системы) —
этот контур защиты не ловит. Сегодня такое зависание:

1. **Видимо, но не лечится**: healthz `loops-alive` даёт `Degraded` — но это
   HTTP 200, docker HEALTHCHECK (`curl -sf /healthz`) считает успехом и
   контейнер не гасит; панельный алерт `worker-unhealthy` (warning) и
   Prometheus `WorkerLoopStalled` (critical, порог 60 с) — только сигналы
   оператору, нотификаций наружу нет (`t13`).
2. **Блокирует takeover**: keepalive-контур `ClaimStore` (продление lease,
   отдельный фоновый Task) продолжает жить при зависшем цикле — lease-клэймы
   и лидерство продлеваются бесконечно, второй инстанс (t07) клэймы НЕ
   забирает. Надзор (provisioning/rebuild/ротации/бэкапы) замерзает до
   ручного вмешательства оператора — нарушение характеристики R.
3. **Хуже одиночного инстанса**: при двух инстансах формально «всё живо»
   (оба lease горят, healthz у зависшего отвечает 200), но работающий цикл
   только у одного, а клэймы висят у зависшего.

### 1.2. Цель

Зависший без исключения цикл воркера самолечется за ограниченное время:
внутренний watchdog замечает staleness тиков цикла сверх порога, инициирует
graceful self-stop процесса (готовый путь `RestartHandler`:
`IHostApplicationLifetime.StopApplication`), docker-политика
`restart: unless-stopped` поднимает контейнер; lease гаснут ≤15 с, клэймы
  мигрируют второму инстансу или возвращаются после подъёма. RTO
  самовосстановления — порядка 1,5–2 минут (порог 60 с + shutdown ≤30 с +
  старт контейнера).

### 1.3. НЕ-цели (границы)

- Не решаем «завис весь процесс» (threadpool starvation, вечная GC-пауза):
  healthz тогда тоже не отвечает — это зона docker HEALTHCHECK, не watchdog.
- Не трогаем `TickSafelyAsync`-механику и обработку исключений тиков —
  watchdog закрывает ровно дополнительный класс «завис без исключения».
- Не строим внешний watchdog в панели и не меняем семантику healthz
  (Degraded остаётся HTTP 200; причины деградации не разделяем на
  «рестартуемые/нерестартуемые») — если такое понадобится, отдельная задача
  roadmap.
- Не добавляем лимит частоты рестартов / «уставшие» состояния —
  идемпотентность процессов + takeover делают рестарт безопасным, сдерживание
  ресторма — docker restart-backoff (см. §3.5).
- Не распространяем watchdog на панель (`AdminPanel.*`): её фоновые циклы —
  другой паттерн (не TickSafelyAsync/loops-alive), вне формулировки t12.
- Не меняем etcd-контракт (ключи, payload, lease-семантика) — watchdog
  ничего не пишет в etcd (симметрия RestartHandler: lease сами погаснут).

## 2. Что уже готово (доказано исследованием, не дублируем)

| Механизм | Где | Что переиспользуем |
|---|---|---|
| Тики живости циклов | `src/{PgWorker,KafkaWorker,ValkeyWorker}.App/HealthState.cs` — идентичные классы (`MarkReconcileTick`/`MarkKeepaliveTick`/`MarkSnapshotTick`), пишутся циклами | watchdog читает те же отметки; второй источник правды не заводим |
| Пороги staleness healthz | `src/*/App/HealthChecks/*WorkerHealth.cs` — формула `3 × max(ScanIntervalSec, KeepaliveSec) + 15` (быстрые циклы); snapshot — `3 × max(ScanIntervalSec, 60 × SnapshotIntervalMin) + 15` | формулу выносим в общий хелпер: healthz и watchdog не разъезжаются |
| Self-stop воркера | `src/*/App/Api/Operations/RestartHandler.cs` — `StopApplication` → контейнер поднимает `restart: unless-stopped` (docker перезапускает и exit 0) | watchdog использует тот же путь остановки |
| Keepalive-контур lease | `src/Shared.Etcd/Coordination/ClaimStore.cs` — собственный Task продления (TTL 15 с / тик 5 с) | обоснование вреда зависания (§1.1 п.2) и канал самоизлечения: стоп гасит lease |
| Метрики циклов | `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` — `LoopTick`/`LoopDuration`; серии `worker_loop_*` (arch/18 §2.2) | сюда добавляем counter рестартов watchdog |
| Алерты «цикл умер» | панель `WorkerUnhealthyRule` (warning); Prometheus `WorkerLoopStalled` (critical, 60 с, `dev-stand/adminpanel/metrics/prometheus/rules.yml`) | остаются как ранний сигнал; watchdog — лечение, не наблюдение |
| Shutdown-бюджет host | `HostOptions.ShutdownTimeout` — дефолт 30 с: зависший цикл не реагирует на cancellation → host всё равно завершится по таймауту | graceful stop ограничен по времени конструктивно |
| Опции циклов | `src/*/App/Options.cs` `LoopsOptions` (`ScanIntervalSec=5`, `KeepaliveSec=5`, `SnapshotIntervalMin=360`, `ErrorDelayMs=2000`) | сюда добавляем секцию watchdog |

**Реальные дыры, закрываемые задачей**:

- `BackupOrphanSweeperLoop` (`src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs`)
  не пишет тики в `HealthState` вообще («без health-обёртки») — невидим в
  `loops-alive` и выпадает из любого staleness-контроля;
- никакой компонент не преобразует staleness в действие.

## 3. Принципы

1. **Самодостаточность лечения**: воркер самовосстанавливается без внешних
   систем (панель/Prometheus могут быть мертвы) — канон multi-host
   надёжности (гарантии не строятся от соседних процессов).
2. **Единый источник живости**: `HealthState` — единственные отметки тиков;
   watchdog и healthz читают их и считают порог одной формулой (общий
   хелпер), различаются только множителем.
3. **Готовый путь остановки**: никаких новых механизмов завершения — тот же
   graceful `StopApplication`, что у `POST /api/restart`; идемпотентность
   процессов и takeover делают рестарт безопасным (арх-канон «ошибка тика →
   продолжение со следующего тика» распространяется и на рестарт всего
   воркера).
4. **Watchdog проще наблюдаемого**: компонент — тривиальный цикл чтения
   отметок и сравнения возрастов (без сетевых вызовов, без etcd, без docker);
   всё, что может зависнуть в его собственной проверке, — отсутствует.
5. **Без анти-лупа**: систематический баг «после рестарта снова виснет»
   даёт рестарт-цикл — его сдерживает docker restart-backoff, а видимость
   дают метрика/журнал (новый InstanceId, счётчик рестартов). Программный
   лимит («после N рестартов — лечь») означал бы отказ от самовосстановления
   ровно тогда, когда оно нужнее всего — противоречит R.
6. **arch-first**: канон watchdog сначала в `arch/` (§14/§16/§21 — надёжность
   и наблюдаемость, §18 — метрика), затем код.
7. **Каноны проекта**: воркеры всегда в докере; тесты — динамические порты,
   полная самоочистка, короткие бюджеты; язык документации — русский.

## 4. Структура и компоненты

### 4.1. arch/ — контракт watchdog (первая фаза, только документация)

etcd-контракт и интерфейсы API не меняются. Дополняем:

- **`arch/14-pgworker.md`** §6 «Надёжность»: пункт «Watchdog зависших
  циклов» — внутренний компонент следит за возрастом тиков всех фоновых
  циклов (reconcile/keepalive/snapshot/orphan-sweep) по `HealthState`;
  staleness сверх порога → журнал + метрика + graceful `StopApplication`
  (путь `POST /api/restart`); контейнер поднимает docker-политика; lease
  гаснут ≤15 с → takeover вторым инстансом. Отдельно: «завис весь процесс» —
  зона docker HEALTHCHECK, watchdog закрывает только «цикл завис, процесс
  жив». §7 «Наблюдаемость»: counter рестартов watchdog в словаре метрик.
  §8 «Конфигурация»: секция `Loops:Watchdog` (ниже).
- **`arch/16-kafkaworker.md`** §6/§7/§8 — симметричное дополнение
  (циклы reconcile/keepalive/snapshot).
- **`arch/21-valkeyworker.md`** §6/§7/§8 — симметричное дополнение.
- **`arch/18-metrics.md`** §2.2 — новая серия в словаре воркер-паттерна:
  `worker_watchdog_restarts_total` (counter, лейбл `loop`) — «инициированные
  watchdog-остановки по staleness»; источник — марк-метод
  `WorkerMetricsInstrumentation`.

### 4.2. LoopWatchdog — общий компонент (Shared.Core)

Новый `src/Shared.Core/Hosting/LoopWatchdog.cs` — `BackgroundService`:

- **Источник живости**: абстракция `ILoopsVitality` — снимок
  `IReadOnlyList<LoopHeartbeat>`, где `LoopHeartbeat = (string Name,
  DateTimeOffset? LastTickAt, TimeSpan StaleAfter)`. Каждый воркер
  реализует её поверх своего `HealthState` + `LoopsOptions` (перечень циклов
  и пороги — тем же общим хелпером формул, что в healthz, множитель
  watchdog-порога — §4.4). Существующий интерфейс
  `Shared.Core.HealthChecks.IHealthCheckService` (Inited/Working/StatusError)
  не расширяется.
- **Формулы порогов** (вынос из `*WorkerHealth` в общий хелпер
  `Shared.Core/HealthChecks/LoopStaleness.cs`; healthz переходит на хелпер
  без изменения поведения):
  - быстрые циклы (reconcile, keepalive, orphan-sweep):
    `3 × max(ScanIntervalSec, KeepaliveSec) + 15` — порог healthz;
    watchdog = `WatchdogMultiplier ×` этого порога;
  - snapshot-цикл: `3 × max(ScanIntervalSec, 60 × SnapshotIntervalMin) + 15`
    — порог healthz; watchdog = `WatchdogMultiplier ×` этого порога.
- **Свой цикл**: проверка каждые `Loops:Watchdog.CheckIntervalSec`
    (дефолт 15 с) — чтение снимка (lock-free, миллисекунды), сравнение
    возрастов. Никаких сетевых вызовов.
- **Grace старта**: отметки `null` («цикл ещё не тикал») не firing, пока
  возраст самого watchdog (от его запуска, `clock`) меньше
  `2 × StaleAfter` цикла; если тик так и не пришёл за grace — рестарт
  (цикл не стартовал или завис при старте).
- **Firing** (одного наблюдения превышения достаточно — порог 60 с сам по
  себе защита от ложных срабатываний на длинных тиках; гистерезис не вводим):
  1. `LogCritical` с именем цикла, возрастом и порогом;
  2. маркер метрики `worker_watchdog_restarts_total{loop}` (через колбэк,
     который app регистрирует при подключении — Shared.Core не зависит от
     Shared.Metrics);
  3. короткая пауза `StopDelaySec` (дефолт 1 с, чтобы лог/событие успели
     доехать в выхлоп контейнера);
  4. `IHostApplicationLifetime.StopApplication()`. Дальше — существующее:
     shutdown (зависший цикл не реагирует на токен → `ShutdownTimeout` 30 с
     закрывает ожидание) → exit → docker `restart: unless-stopped` →
     подъём → свежий InstanceId, lease старого погасли ≤15 с.
- **Не следит за собой** (принципиально: компонент без внешних вызовов;
  его собственное зависание означало бы деградацию всего процесса — зона
  docker HEALTHCHECK).

### 4.3. Дооснащение BackupOrphanSweeperLoop (PgWorker)

`BackupOrphanSweeperLoop` получает отметку тика в `HealthState`
(`MarkOrphanSweepTick`) и запись в `HealthSnapshot`/healthz `loops-alive`
симметично остальным циклам; порог — как у быстрых циклов. Без этого
зависание sweeper'а невидимо (сейчас «без health-обёртки») — watchdog
получает полный перечень циклов воркера. У KafkaWorker/ValkeyWorker
аналогичных «слепых» циклов нет (их циклы уже пишут тики).

### 4.4. Конфигурация (appsettings всех трёх воркеров)

Секция `LoopsOptions` дополняется:

```
Loops:Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 }
```

- `Enabled=false` — компонент не регистрируется (поведение как до задачи);
- `Multiplier` — множитель порога healthz (дефолт 2 → быстрые циклы 60 с
  при дефолтных интервалах: 3×max(5,5)+15=30, ×2=60; snapshot — свой порог
  той же формулой ×2); окно между Degraded healthz (порог ×1) и рестартом
  (порог ×2) — алертам оператора и легитимно-длинным тикам;
- явного `StaleAfterSec`-оверрайда НЕ вводим — формула от интервалов циклов
  единственная (меняем интервалы → пороги следуют автоматически).

### 4.5. Метрика и журнал

- `WorkerMetricsInstrumentation` (Shared.Metrics): маркер-метод
  `WatchdogRestart(string loop)` → counter `worker_watchdog_restarts_total`
  (arch/18 §2.2, §4.1). Подключается колбэком из каждого app при
  регистрации watchdog.
- Журнал: `LogCritical("watchdog: цикл {Loop} не тикал {Age} c (порог
  {Threshold} c) — инициирован self-restart", …)` — последняя запись
  контейнера перед остановкой; docker restart сохраняет логи контейнера,
  факт виден постфактум.
- healthz: в `data` добавляется секция `watchdog`
  (`armed; stale=<loop|нет>`), источник — состояние компонента; контракт
  статусов HTTP не меняется.

### 4.6. Подключение в воркерах

`Program.cs` каждого воркера (PgWorker, KafkaWorker, ValkeyWorker):
регистрация `ILoopsVitality` (поверх `HealthState` + `LoopsOptions`),
`AddHostedService<LoopWatchdog>` (после остальных hosted-сервисов) и
колбэк метрики. Состав наблюдаемых циклов:

| Воркер | Циклы под watchdog |
|---|---|
| PgWorker | reconcile, keepalive, snapshot, orphan-sweep (после §4.3) |
| KafkaWorker | reconcile, keepalive, snapshot |
| ValkeyWorker | reconcile, keepalive, snapshot |

### 4.7. Тесты

- **Юнит** (новый `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs`):
  формулы порогов хелпера (быстрые/snapshot, симметрия healthz); grace
  старта (null-отметки в grace-окне → не firing, после grace → firing);
  firing → `StopApplication` вызван ровно once (fake
  `IHostApplicationLifetime`); живые циклы → не firing; `Enabled=false` →
  компонент не регистрируется. Комментарии тестов — по нотации AAA.
- **Юнит per-app** (PgWorker.UnitTests): `BackupOrphanSweeperLoop` пишет
  тик в `HealthState`; healthz `loops-alive` включает orphan-sweep;
  `ILoopsVitality` отдаёт полный перечень с корректными порогами.
- **Интеграционный** (PgWorker.IntegrationTests, не docker): поднимается
  хост воркера в тестовом окружении с уменьшенными интервалами `Loops`
  (пороги порядка десятков секунд) и подменённым `ILoopsVitality`, чей
  снимок показывает устаревший тик → ассерт: хост инициировал остановку в
  бюджет (каждое отдельное ожидание поллинга ≤30 с на правило
  AGENTS.base.md §12). Симметричный негатив: свежие тики → хост живёт
  (отмена через штатный token).
- **Мерж-гейт E2E** (правило AGENTS.md — задача трогает `src/PgWorker.App`):
  кейс-маркер `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
  src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
  на свежем Release зелёный — доказывает, что watchdog не даёт ложных
  рестартов в живом docker-контуре (инстанс переживает полный provisioning
  без остановки).

### 4.8. Документация и мерж-гейт трека

- `docs/runbook.md`: раздел watchdog — как читать
  `worker_watchdog_restarts_total`, что значит строка журнала self-restart,
  когда легитимно выключить (`Loops:Watchdog:Enabled=false`), связь с
  чеком 35 (kill→takeover) и алертом `WorkerLoopStalled`.
- **Мерж-гейт трека** (правило `arch/roadmap/reliability.md`): закрытие
  задачи тем же мерж-коммитом (1) снимает пункт `t12-loop-watchdog` из
  `arch/roadmap/reliability.md` (включая `←`-зависимости — их нет) и
  (2) обновляет `arch/roadmap/reliability-report.md`: строка t12 уходит из
  «Осталось», в «Сделано» добавляется запись (тег — merge-коммит — влияние
  на R: «зависший без исключения цикл самолечется self-restart'ом за ~минуту,
  lease гаснут ≤15 с, takeover вторым инстансом не блокируется»), сводка
  раздела R — убрать «нет watchdog зависших циклов (t12)» из открытых
  разрывов.

## 5. Фазы

1. **arch-first** (только документация): arch/14 §6/§7/§8, arch/16, arch/21
   (симметрично), arch/18 §2.2 — канон watchdog, формулы, конфигурация,
   метрика (§4.1).
2. **Shared.Core + Shared.Metrics**: хелпер формул `LoopStaleness`,
   `LoopWatchdog` + `ILoopsVitality`, маркер `WatchdogRestart`
   (§4.2, §4.5); юнит-тесты §4.7.
3. **Воркеры**: дооснащение `BackupOrphanSweeperLoop` (§4.3), переход
   healthz на общий хелпер формул (поведение без изменений), секция
   `data["watchdog"]`, `Loops:Watchdog`-опции, подключение в `Program.cs`
   трёх воркеров (§4.4–4.6); юнит per-app.
4. **Интеграционный тест** §4.7.
5. **Мерж-гейт**: E2E-маркер `Scale_AddEmptyShard` зелёный; runbook;
   roadmap-гейт (§4.8).

Фазы 2→3 жёстко последовательны (компонент → подключение); фаза 4 после 3;
фаза 5 последней.

## 6. Ограничения

- `TreatWarningsAsErrors=true`, `Nullable=enable`, `LangVersion=latest` —
  новый код без предупреждений.
- Тесты: никаких хардкодов хост-портов; таймауты ожидания ≤30 с (каждое
  ожидание — не правило, а проверка с повтором); интеграционные бюджеты —
  короткие.
- E2E (маркер): свежий Release, самоочистка контура фикстурой — каноны
  `docs/e2e-isolation.md`/`docs/e2e-launch.md`; зачистка контейнеров/сетей
  после серии.
- Watchdog не делает сетевых вызовов и не пишет в etcd — нарушение в ревью
  блокирует.
- Изменение HTTP-статусов healthz / формул порогов healthz — вне scope
  (поведение healthz сохраняется бит-в-бит, кроме новой data-секции).
- Локально собираемые образы в registry `192.168.0.1:5000` не кладём.

## 7. Критерии приёмки

- **AC1 (юнит — механика)**: тесты §4.7 зелёные: формулы порогов
  (включая симметрию с healthz), grace старта, firing → ровно один
  `StopApplication`, свежие циклы → не firing, `Enabled=false` → компонент
  не поднят.
- **AC2 (интеграция — самолечение)**: хост воркера с устаревшим тиком в
  `ILoopsVitality` инициирует остановку в бюджет при уменьшенных тестовых
  интервалах `Loops` (пороги порядка десятков секунд; каждое отдельное
  ожидание поллинга ≤30 с — AGENTS.base.md §12); с живыми тиками хост
  работает без остановки.
- **AC3 (ложных срабатываний нет)**: E2E-маркер `Scale_AddEmptyShard`
  зелёный на свежем Release; за серию ни одного
  `worker_watchdog_restarts_total` и ни одной записи self-restart в журнале
  инстансов, участвующих в сценарии.
- **AC4 (наблюдаемость)**: `/metrics` инстанса содержит
  `worker_watchdog_restarts_total` (после триггера — с лейблом цикла);
  `/healthz` отдаёт секцию `watchdog`; журнал содержит критическое событие
  перед остановкой.
- **AC5 (полнота перечня)**: `loops-alive` healthz PgWorker включает
  orphan-sweep; зависание sweeper'а видно и лечится watchdog.
- **AC6 (симметрия)**: watchdog поднят у всех трёх воркеров с одним
  компонентом `Shared.Core` (без копий per-app), состав циклов — по §4.6.
- **AC7 (канон и гейт)**: arch/14/16/18/21 дополнены (§4.1); runbook
  описывает watchdog; мерж-коммит закрытия снимает пункт t12 из
  `arch/roadmap/reliability.md` и обновляет строку в
  `arch/roadmap/reliability-report.md` (перенос в «Сделано», правка сводки
  R) — рассинхрона «задача закрыта / отчёт нет» нет.
