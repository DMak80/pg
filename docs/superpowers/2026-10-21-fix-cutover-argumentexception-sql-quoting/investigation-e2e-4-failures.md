# Расследование 4 вне-диффовых docker-E2E фейлов (полный прогон PgWorker.slnx)

Прогон: `/tmp/pgw-plan-logs/task7-full-e2e-rerun.log` (worktree
`fix-cutover-argumentexception-sql-quoting`, Release, `--no-build`).
Хронология фейлов внутри прогона: S3 mesh (xUnit 00:04:25) → PgWorker
Acceptance (00:06:20) → Kafka takeover (00:06:42) → PgWorker Pool
(00:24:44). Дифф задачи (PgWorker.Moves + тесты) ни в один из четырёх
фейлов не входит.

Общий материал артефактов: `$TMPDIR = /var/folders/wv/0_cf0cks0zxfbkf3w76wd9rw0000gn/T`;
метки времени в логах docker/etcd — UTC (локальное = UTC+3).

---

## Фейл 1. `E2eScenarios.Acceptance_Scenario_Ac2_To_Ac7` — ac6-quarantine, «the shard return was not observed within 120 s»

### (a) Что наблюдалось

Артефакты:
`/var/folders/wv/0_cf0cks0zxfbkf3w76wd9rw0000gn/T/pgw-e2e-artifacts-17163734a252414ea4c0efa7462611b7/`
(runner.log, `ab81dd0655b248e7bead18328a5cea7a/` — phases.log, логи
воркеров и контейнеров). Фаза-бюджеты (phases.log):

```
13:45:21 ac6-evacuation: ok=True, elapsed=9.0s
13:47:23 ac6-quarantine: ok=False, elapsed=120.8s, budget=120s
```

Ассерт (`E2eScenarios.cs:450` AssertEvacuationAsync): журнал
`/pgworker/evacuations/shopab81dd06/shard2` на момент фейла —

```json
{"buckets":{"1":"shard1","3":"shard1","5":"shard1"},"reason":"shard-dead",
 "evacuated_unix":1792590321,"state":"DONE"}
```

— state=DONE, `returned_unix` отсутствует; контейнеры
shard2a/shard2b = exited.

Лог воркера p2 (`container-pgw-ew-p2-…log`, строки 12818–26926) — цикл
надзора `adopt → repair → evacuate/shard2` каждые ~4.5 c, и в нём:

```
13:45:21.816  evacuate/shard2: Done              ← эвакуация E4 (journal DONE)
13:45:47.621  evacuate/shard2 failed: The request was canceled due to the
              configured HttpClient.Timeout of 7.5 seconds elapsing.
13:46:00.568  evacuate/shard2 failed: … HttpClient.Timeout of 7.5 seconds …
13:46:11.869+ evacuate/shard2: Done              ← каждый следующий тик — ранний выход
```

Логи контейнеров шарда 2 (тест выполнил `docker start` возврата):

```
shard2a: 13:45:33 spilo entered RUNNING state
         13:45:40.840 WARN received SIGTERM …   ← PgWorker начал stop «призрака»
         13:45:43–47 waiting for spilo to die    ← умирал >7.5 c
shard2b: 13:45:53.290 WARN received SIGTERM
         13:46:02 … (окончательный exit)
```

То есть PgWorker ОБНАРУЖИЛ возврат и НАЧАЛ остановку обеих нод, но
каждый stop занял больше 7.5 c.

Код:

- `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs:230–252`
  (HandleReturnedShardAsync): при живой REST-пробе выполняется
  `driver.StopNodeAsync` по каждой ноде, и ТОЛЬКО ПОСЛЕ успешного стопа
  всех нод — запись журнала `state=QUARANTINED, returned_unix`.
  Любой сбой стопа → `return Failed` ДО записи журнала.
- `src/PgWorker.Docker/Drivers/ClusterDriver.cs:405–417`
  (StopNodeAsync) → `engine.StopContainerAsync(name, timeoutSec: 10)`.
- `src/Shared.Docker/Engine/DockerEngine.cs:149–160`: stop — это
  блокирующий `POST /containers/{id}/stop?t=10` через HttpClient.
- `src/Shared.Docker/Engine/DockerEngineFactory.cs:91`:
  `CallTimeout = 7.5 s` для ВСЕХ docker-вызовов (поллинг-инвариант
  arch/14 §6: пер-попыточный таймаут = половина окна watchdog).

Серийность: ac6-quarantine падал за этот день трижды — 13:06:41
(`…artifacts-5b1754117d524df28a0d9696ae46769d/…`), 13:30:10
(`…artifacts-6547f12aaaee4987881c42528510ece3/…`) и 13:47:22 (наш
каталог). Это не единичный флейк.

### (b) Корневая причина — уровень уверенности: ВЫСОКИЙ

Структурная гонка «клиентский таймаут < грейс стопа + журнал после
стопа»:

1. `docker stop?t=10` для упирающегося spilo (graceful shutdown PG)
   физически длится 7–10+ c; HttpClient обрывает запрос на 7.5 c →
   `StopNodeAsync` возвращает Failed, хотя демон продолжает и
   ЗАВЕРШАЕТ остановку (вплоть до SIGKILL — контейнеры в итоге exited).
2. `HandleReturnedShardAsync` падает ДО записи `QUARANTINED/returned_unix`.
3. Следующий тик: REST-пробы нод мертвы → ранний выход «ещё мёртв —
   держим карантин» → журнал НАВСЕГДА остаётся DONE. Самокоррекции нет.

Тест корректен (ждёт именно контрактное состояние журнала, arch/14 §5 E3:
возврат = QUARANTINED + returned_unix). Подтверждается каждой строкой
хронологии выше; опровергающих наблюдений нет. Параллельная docker-сессия
могла ЗАМЕДЛИТЬ остановку spilo, но баг воспроизводится и на спокойном
хосте (три фейла за день, spilo штатно умирает дольше 7.5 c).

### (c) Природа

Прод-баг (два слоя):

- Docker-слой: `StopContainerAsync(t=10)` несовместим с `CallTimeout=7.5 c`
  — любой контейнер, умирающий дольше 7.5 c, даёт клиентский Failed при
  фактически успешном исходе (идемпотентный повтор даёт 304, но до него
  дело не доходит — см. п.2).
- Слой эвакуатора: журнал QUARANTINED пишется ПОСЛЕ стопов вместо
  journal-before-manipulations (P7-принцип самого эвакуатора).

### (d) Предлагаемое решение

1. `Shared.Docker/Engine/DockerEngine.cs` — `StopContainerAsync` доводит
   исход до истины поллингом: одиночный HTTP-запрос ≤7.5 c (инвариант
   arch/14 §6 сохраняется), при клиентском таймауте — повтор POST /stop
   (идемпотентен; демон продолжает прежний) ИЛИ GET статуса, до
   `exited/running`-факта в пределах явного бюджета (timeoutSec + запас).
   Выход «exited или 304/404» = успех.
2. `PgWorker.Provisioning/Processes/BucketEvacuator.cs`
   (HandleReturnedShardAsync): зафиксировать возврат в журнале
   (`QUARANTINED` + `returned_unix`) В МОМЕНТ первого обнаружения живой
   ноды — ДО docker-стопов (P7: журнал до манипуляций); стопы —
   идемпотентные, сбой стопа не отменяет факт возврата, retry следующим
   тиком. Финальный тестовый ассерт exited-статусов остаётся валидным.
3. Arch-правка не требуется: поведение уже канонизировано (arch/14 §5 E3
   — карантин+returned_unix; §6 — идемпотентность/ретраи тиком);
   фиксируемая правка приводит код к канону. При изменении контракта
   стопа на уровне Shared.Docker — сначала отражение в arch/14 §6
   (поллинг-инвариант расширяется формулировкой «стоп — поллинг-итерации»).

ВЛИЯНИЕ Shared-правки: StopContainerAsync вызывается также из
NodeSupervisor.cs:645, ClusterDriver (4 места), аналогов
Kafka/S3/Valkey-драйверов — семантика «успех = контейнер остановлен»
для всех них корректна и безопасна (все вызыватели трактуют
Failed как аварию).

### (e) Docker-репро

Не обязателен (диагноз полный), но как verify фикса в execute-фазе:
соло-прогон `Acceptance_Scenario_Ac2_To_Ac7` (строго после проверки
отсутствия чужих pgw-*/kfw-*/s3w-* контуров). Критерий «фикс работает»:
ac6-quarantine ok=True; в логе воркера допустимы повторные stop-итерации,
но журнал достигает `QUARANTINED` с `returned_unix`. Критерий
«диагноз неверен»: ассерт снова падает с journal DONE при отсутствии
в логе воркера 7.5-секундных таймаутов стопа.

---

## Фейл 2. `PoolReadAnyNodeScenarios.Pool_ReadAnyNode_RestoreAfterNodeStop` — restore не достиг COMPLETED (RUNNING)

### (a) Что наблюдалось

Артефакты:
`/var/folders/wv/0_cf0cks0zxfbkf3w76wd9rw0000gn/T/pgw-e2e-artifacts-8675dba138d54f9bba8ec8c154849576/`
(runner.log + `686574f332184ecf952d00ccd94dae85/`). Ассерт
(`E2eBackupPoolScenarios.cs:328` WaitRestoreAsync, бюджет 420 c):

```
restore pac6686574f3/shard1/20261021135840Z-shard1 did not reach COMPLETED: RUNNING
```

Сценарий (код `E2eBackupPoolScenarios.cs:749–793`): пул S3 node1+node2,
`mc mirror` node1→node2, `docker stop` node1, restore-latest должен
дочитать с node2 (ReadAll) и завершиться COMPLETED.

Лог воркера `container-pgw-ew-pac6-…log` — начиная с 14:02:26 КАЖДЫЙ тик
restore-фазы падает одним и тем же исключением (стектрейс в логе):

```
fail: ReconcileLoop … System.TimeoutException → TaskCanceledException
   at PgWorker.Docker.Drivers.PlainClusterDriver.<RemoveNodeAsync>b__0(…)
   in ClusterDriver.cs:line 357
   … HttpClient 7.5 seconds elapsing
```

Повторы: 14:02:26, 14:03:26, 14:04:12, 14:04:59 — демонтаж шарда
restore-заявкой (`RestoreProcess.DemolishAsync → RemoveNodeAsync →
StopContainerAsync(t=10)`, ClusterDriver.cs:355–357) стабильно не
проходит: stop живого spilo > 7.5 c → клиентский Failed → тик
`backup-restore … failed` → следующий тик повторяет демонтаж. Restore-джоба
(контейнер `pgw-backup-restore-*`) НИ РАЗУ не была создана (в артефактах
отсутствует), статус заявки навсегда RUNNING до исчерпания 420 c.

Факты по контейнерам шарда 1 на teardown (docker inspect в артефактах):

```
shard1a: Created 14:05:03, running — patroni «waiting for leader to
         bootstrap» (PGDATA пуст; щит /service/…/initialize =
         "restore-in-progress" держит bootstrap)
shard1b: Created 14:04:17, exited 137 (14:05:01) — pg_control жив
         (Timeline 2, shut down 14:03:20)
```

Оба контейнера — НОВЫЕ create (не рестарты) между фейлами restore-тика.
Возвращающее пересоздание гарантирует бесконечность цикла: restore
стопает → кто-то в тике пересоздаёт → снова стоп >7.5 c → …

Вторичный симптом: `backup-wal …/shard2: control Active→Degraded; upload
silence 184…383 s (threshold 120)` — WAL-тишина shard2 с ~13:59
(следствие деградации тиков/файлового ввода-вывода контура; не блокирует
restore напрямую, отдельного фикса не требует).

### (b) Корневая причина — уровень уверенности

- Механика вечного цикла (7.5-секундный stop-таймаут в DemolishAsync +
  недостижение create restore-джобы): **ВЫСОКИЙ** — прямой стектрейс 4
  раза, отсутствие джобы, терминальный RUNNING.
- Тождественность пересоздателя нод: **СРЕДНИЙ**. Пересоздание — факт
  (новые Created 14:04:17/14:05:03, ensure-подобные create), но по
  доступным артефактам нельзя назвать конкретный код-путь: docker events
  не снимались, надзорные фазы в консоль воркера не логируются
  (пишутся только в etcd work-журнал). Аудит кода: NodeSupervisor
  гвардит restore-шарды (t05 §3.4, NodeSupervisor.cs:59–67/119–124/
  149–152); ProvisioningProcess для Active-кластера не вызывается
  (ReconcileLoop: work=Provision только для непровижененных); WAL
  пересоздаёт агентов, не ноды. Остаётся неаудированная комбинация
  ensure-путей (в т.ч. REBUILDING-ветки RecreateRestTlsNodes /
  ensure-declared при частично удалённой декларации) — точечная
  локализация требует репро со снятием docker events.

Env-конкуренция участвовала как усилитель (spilo умирал дольше обычного,
тики ReconcileLoop длились ~46 c), но и без неё демонтаж не проходил бы
с первого тика — тот же структурный дефект stop/7.5 c, что в фейле 1.

### (c) Природа

Прод-баг (слой docker-stop — общий с фейлом 1) + непроведённая до конца
изоляция демонтажа restore от ensure-путей надзора (breach или дыра
гварда t05 §3.4 — локализуется репро).

### (d) Предлагаемое решение

1. Общий docker-стоп-фикс из фейла 1 (DockerEngine.StopContainerAsync
   поллинг-итерации) — закрывает первичный триггер: демонтаж проходит
   за один тик (stop доводится до exited), RemoveNodeAsync доходит до
   remove/volume и create джобы.
2. Execute-фаза, шаг локализации (обязателен до правки надзора):
   соло-репро сценария с параллельным `docker events --filter
   container=pgw-pac6*…shard1*` (снимается в файл) + verbose-лог; по
   автору события create — назвать точный ensure-путь и закрыть его
   гвардом t05 §3.4 (если breach подтверждается).
3. Тест менять не нужно: бюджет 420 c достаточен при работающем
   демонтаже (штатный прогон укладывается в минуты).

### (e) Docker-репро

Нужен (совмещён с шагом локализации): соло-прогон
`Pool_ReadAnyNode_RestoreAfterNodeStop` (после проверки чистоты хоста),
параллельно `docker events`. Критерий подтверждения диагноза: в events
видны create pgw-…-shard1-shard1a/b ПОСЛЕ stop-событий демонтажа в окне
RUNNING-заявки, и автор create — процесс воркера. Критерий опровержения:
create отсутствует, а демонтаж всё равно циклится (тогда причина иная —
расширять диагностику логом DockerEngine).

---

## Фейл 3. `S3E2eMeshTests.PkiProvisioning_ReplicationOverLinks` — «[PHASE] replicated: budget of 150 s exhausted»

### (a) Что наблюдалось

Прогон: `/tmp/pgw-plan-logs/task7-full-e2e-rerun.log` (xUnit 00:04:25,
2 m 45 s). Контур `mesh66ace094` ПОЧЕМУ-ТО ЖИВ ДО СИХ ПОР (MarkFailed не
убил тома? — контейнеры `s3w-mesh66ace094-east1/east2` Up; это дало
возможность снять in-vivo диагностику).

Таймлайн по `docker logs -t` (все события — UTC):

```
east1: 13:43:03.475 POST /admin/replication/join (S3Worker, G-доводка)
                      → «join: сосед east2 добавлен в состав (auth=key)»
       13:43:03.802 PUT pgworker-backups/mesh/52bc….txt (тест) → 200
                      (PutObject, durationMs=63.6)
       13:43:04.366 «Репликация активна (1 соседей) — координатор запущен» ←
                      координатор стартовал ЧЕРЕЗ ~0.9 c ПОСЛЕ PUT
       13:43:04.375 «Новый сосед east2: рассылка с конца журнала (seqNo=1)»
                      ← seqNo=1 — это и есть запись PUT; «с конца» = запись
                        НЕ рассылается (канон owns3/07 §2/§8)
       13:43:04.41–44 Reconcile по соседу east2: «ключей обработано: 0»
east2: 13:43:03.705 координатор запущен
       13:43:03.73–87 Reconcile по соседу east1: «ключей обработано: 0»
                      ← листинг east1 снят ДО коммита PUT (.866) — гонка на мс
       далее: бесконечные GET mesh/52bc….txt → 404 (поллинг теста 150 c)
```

На east2 `operation=PutObject` встречается 0 раз за всю жизнь контейнера —
доставка не состоялась вовсе. Join/PKI при этом полностью корректны
(фазы joined/Assert1/Assert2 теста прошли; env контейнеров содержит
OWNS3_REPLICATION_CERT/TRUST_CA/ADVERTISED_ENDPOINT — проверено
`docker inspect`).

Env контейнеров (`docker inspect`, grep OWNS3): `OWNS3_REPLICATION_
RECONCILE_INTERVAL_SECONDS` ОТСУТСТВУЕТ — периодический reconcile
работает с дефолтом OwnS3 «3 часа».

Канон: `../OwnS3/arch/owns3/07-replication.md` §9, триггер 4:
«периодически — OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS (дефолт
3 ч …; частые проходы оправданы только тестовыми контурами
(E2E переопределяет env до 10 с)». Grep по pg: `OWNS3_REPLICATION_
RECONCILE_INTERVAL_SECONDS` НЕ встречается нигде в src — ни в
`S3Worker.Provisioning/InstanceEnvComposer.cs` (единственный
составитель env инстансов), ни в тестах. Обещанного каноном E2E-
переопределения НЕ СУЩЕСТВУЕТ.

### (b) Корневая причина — уровень уверенности: ВЫСОКИЙ

Дефект конфигурации E2E-контура (pg-сторона), вскрывший гонку стартового
окна OwnS3:

1. Тест кладёт объект сразу после фазы `joined` (сосед числится в
   /admin/replication/status) — но пайплайн востока-источника (координатор,
   notifier, исполнители) стартует на ~0.5–0.9 c ПОЗЖЕ join-записи.
2. По канону owns3/07 (§2/§8) новый сосед получает рассылку «с конца
   журнала»: запись PUT (seqNo=1) в рассылку не попадает — осознанно,
   дыры ДОЛЖЕН закрывать reconcile.
3. Оба стартовых reconcile проиграли гонку на миллисекундах (каждый
   снял листинг соседа до коммита PUT) — «ключей обработано: 0».
4. Периодический reconcile — единственный самовосстанавливающийся
   триггер — в контуре фактически ВЫКЛЮЧЕН (env не задан → 3 ч вместо
   10 с канонических для E2E). Итог: объект завис навсегда, бюджет
   150 с исчерпан.

OwnS3 ведёт себя строго по своему канону; дефект на стороне pg —
не передан env при создании инстансов.

Опровергающих наблюдений нет; конкуренция docker-сессий здесь
невиновна (все события — внутриконтурные, тайминги честные).

### (c) Природа

Тест-инфраструктурный дефект pg (env E2E-контура не соответствует
канону owns3/07 §9). Прод-код S3Worker корректен; OwnS3 корректен.

### (d) Предлагаемое решение

- `src/S3Worker.Provisioning/InstanceEnvComposer.cs`: прокинуть
  `OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS` в env инстансов из
  опции воркера (env `S3WORKER__…` / BackupsRuntimeOptions-подобная
  секция опций S3Worker; отсутствие опции = не ставить переменную,
  прод-дефолт OwnS3 3 ч не меняется).
- E2E-окружение тестов (`S3E2eEnvironment` / запуск воркера в тестах)
  выставляет опцию в 10 c — канон owns3/07 §9.
- arch-правка pg не требуется (arch/24 §5 G верен: «механика
  mesh-репликации — канон owns3/07»); при желании зафиксировать
  прокидывание env в arch/24 §5 A (список env инстансов) — на усмотрение
  ревью.

Дополнительная (опциональная) метрика устойчивости теста: в фазе
`replicated` при 404 дополнительно триггерить внеплановый
`POST /admin/replication/reconcile` (канон §9 триггер 5) — закрыл бы
стартовое окно даже без периодического интервала; НЕ вместо env, а как
дубль. Развилка — см. (f).

### (e) Docker-репро

Не нужен (диагноз полный, материал живого контура). Verify в execute-
фазе: соло-прогон `PkiProvisioning_ReplicationOverLinks` с фиксом env —
ожидание ok за ≤10–30 c (первый/второй периодический reconcile закрывает
дыру). Критерий опровержения диагноза: с env=10 c объект всё равно не
доставлен и в логах нет ни одного запуска ReconcileRunner после
стартового — тогда копать разбор env-префикса в OwnS3 (маппинг
`("OWNS3_…", "OwnS3:…")` OwnS3.App/Program.cs:55).

---

## Фейл 4. `KafkaE2eTakeoverTests.Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates` — survivor «The given key 'broker3' was not present in the dictionary»

### (a) Что наблюдалось

Прогон `/tmp/pgw-plan-logs/task7-full-e2e-rerun.log` (xUnit 00:06:42,
6 m 40 s):

```
Expected finished to be True because the survivor must finish provisioning
to done; work={"op":"provision","phase":"crashed","instance":"d39fabef8145",
"updated_unix":1792590469,"last_error":"The given key 'broker3' was not
present in the dictionary."}
```

Сценарий (`KafkaE2eTakeoverTests.cs:20–126`): два инстанса воркера, сид
broker1..3, якорь середины provisioning (контейнер broker1 создан,
config с state, клэйм жив) → docker kill держателя → выживший обязан
доиграть provisioning до done ≤360 c. Выживший при takeover входил в
фазу provision и падал с KeyNotFoundException (ключ словаря — 'broker3'),
до done не дошёл.

Код-кандидаты прямого доступа к словарю portalloc по имени брокера в
provision-пути (grep по KafkaWorker.Provisioning):

```
Processes/ProvisioningProcess.cs:275:  var addr = addresses[broker.Name];   ← K3 EnsureNodesAsync
Processes/AddBrokerProcess.cs:259:      … addresses[broker.Name] …
Processes/PortAllocHealer.cs:191:       address = merged[broker];
```

`ProvisioningProcess.EnsureNodesAsync` (строка 275) — единственный,
исполняемый в op=provision: обход `snap.Brokers`, для каждого не-RUNNING
брокера прямой `addresses[broker.Name]` без TryGetValue. Если etcd-ключ
`/kafkaworker/portalloc/<C>` на момент тика survivor'а не содержит
broker3 (частичная аллокация убитого держателя, либо survivor перечитал
портплан до её фиксации) — KeyNotFoundException, фаза crashed; следующий
тик повторяет то же самое → вечный crashed, тест ждёт 360 c.

Контур kfw-tkw* снесён teardown'ом, авторство частичного portalloc по
артефактам не восстановить.

### (b) Корневая причина — уровень уверенности: СРЕДНИЙ

Факт: survivor падал KeyNotFound('broker3') в фазе provision и не
восстанавливался — ВЫСОКИЙ (прямой last_error из work-журнала etcd).
Конкретная строка/механизм частичного portalloc — СРЕДНИЙ: строка 275 —
единственный неогвардированный доступ в этом op, но без снапшота
etcd-состояния на момент kill нельзя исключить SecurityMigrator.cs:96
(`containerEnvs[broker.Name]` — запись, не чтение) и иные словари;
подтверждается репро.

Связь с конкуренцией: вероятен вклад (kill держателя попал в окно
незавершённой аллокации), но окно существует и на спокойном хосте —
структурный дефект негигиеничного доступа.

### (c) Природа

Прод-баг KafkaWorker: необработанное отсутствие ключа портплана при
takeover-входе в середину provisioning (требует каноничную
transient-обработку: заявка ждёт полной аллокации тиками, а не роняет
тик исключением).

### (d) Предлагаемое решение

1. `KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` (K3
   EnsureNodesAsync): `addresses` — гигиенично: `TryGetValue` →
   отсутствие ключа = transient (journal-фаза
   `waiting-portalloc/<broker>`, InProgress — следующий тик повторит;
   портплан K1–K2 достроит). Аналогично аудировать
   AddBrokerProcess.cs:259 и PortAllocHealer.cs:191 (запись/чтение
   merged — на консистентность с планом).
2. Сверка с arch/16 (kafkaworker) §фаза K3: если контракт описывает
   «адрес обязателен к моменту K3» — фикс в парадигме «после K2-фиксации
   portalloc полный; K3 при частичном плане — ретрай», не меняя автомат.
   arch-правка не предполагается (поведение — частный случай
   идемпотентности/ретраев arch/16, как в arch/14 §6).
3. Сценарий не менять: он корректно поймал прод-дефект takeover.

### (e) Docker-репро

Нужен: соло-прогон `Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates`
(чистый хост; параллельно `docker events` и периодический дамп
`/kafkaworker/portalloc/<C>` + `/kafkaworker/work/<C>` из etcd контура).
Критерий подтверждения: репро воспроизводит crashed
KeyNotFound('brokerN') при дампе portalloc без brokerN на входе survivor;
после фикса — survivor доводит до done. Критерий опровержения: фейл
воспроизводится с ПОЛНЫМ portalloc в дампе → искать другой словарь
(расширять стектрейс-телеметрию: включить в last_error имя словаря/
строку — TypeNotFound-деталь).

---

## Гипотеза env-конкуренции (сквозная) — оценка

Подтверждается косвенно и НЕ является корнем ни одного из четырёх фейлов:

- Параллельная сессия существовала: `/tmp/e2e-full.log` — полный прогон
  ИЗ ГЛАВНОГО РЕПОЗИТОРИЯ `/Users/demakaev/ZCodeProject/pg` в то же окно
  (его Kafka-фейл — вообще `docker build kafkaworker:e2e did not finish
  within 120 s` — прямой след перегрузки docker-демона сборкой);
  десятки каталогов `pgw-e2e-artifacts-*` от обеих сессий в $TMPDIR.
- Однако у каждого фейла установлена собственная структурная причина
  (стек/логи/код выше), воспроизводимая без конкуренции; конкуренция
  лишь меняла вероятности/времена (spilo умирал дольше, тики тяжелей).
- Общий образ `pgw-e2e-runner:e2e`, пересобранный чужой сессией, влияет
  на ВОСПРОИЗВОДИМОСТЬ выводов прогона (часть серии — на чужом бинаре),
  но не на диагнозы: все четыре механизма подтверждены кодом и логами
  независимо от бинаря.

Вывод: харденинг изоляции docker-E2E сессий — отдельная работа
(образы per-worktree / глобальный лок запуска / детект чужих живых
pgw-*/kfw-*/s3w-* контуров перед стартом серии); в execute-фазе этой
задачи достаточно ОБЯЗАТЕЛЬНОГО пред-чека «хост чист от чужих контуров»
перед каждым репро (уже отражено в критериях (e) каждого фейла).

---

## (f) Закрытые развилки и вопросы пользователю

Закрытые (однозначно из диагнозов/канонов):

1. Фейл 1 фиксируется кодом, не тестом (тест ждёт контракт E3).
2. Порядок HandleReturnedShardAsync: журнал QUARANTINED ДО стопов —
   прямое применение P7 (journal-before-manipulations), развилки нет.
3. Фейл 3: тест правится НЕ увеличением бюджета 150 c, а env
   reconcile-интервала (канон owns3/07 §9 явно описывает E2E=10 c);
   прод-дефолт OwnS3 не трогаем.
4. Фейл 4: гвард-TryGetValue + transient, а не «достраивать portalloc
   из K3» (портплан — домен K1–K2/healer).
5. Docker-репро — только в execute-фазе, строго последовательно, по
   одному контуру, с пред-чеком чистоты хоста и `docker events`-съёмом.
6. Перезапуск упавших тестов до формулировки диагнозов был запрещён и
   не выполнялся; все диагнозы — по артефактам.

Требуют решения пользователя (сводный список):

- В1. Объём: чинить все 4 в ТЕКУЩЕЙ задаче fix-cutover или вынести
  S3/Kafka (и/или Shared.Docker-стоп) в отдельные задачи/roadmap?
- В2. Слой docker-стоп фикса: править `Shared.Docker/Engine/
  DockerEngine.StopContainerAsync` (поллинг-итерации до факта exited;
  затрагивает всех воркеров) — или локально только в PgWord-путях
  (обёртка LongCallPolling у StopNodeAsync)? Рекомендация: Shared
  (один дефект — одно место; arch/14 §6 дополнить формулировкой).
- В3. S3: прокидывать OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS как
  опцию S3Worker (прод-механизм с env-гайкой) или жёстко только в
  E2E-сборке env? Рекомендация: опция воркера (прод-нейтрально).
- В4. Дублирующая самодиагностика S3-теста (триггер внепланового
  reconcile при 404 в фазе replicated) — вводить или достаточно
  env-интервала?
- В5. Харденинг изоляции docker-E2E сессий (per-worktree образы /
  сериализация / детект чужих контуров) — отдельной задачей в
  arch/roadmap (stand) или в рамках этой?
- В6. Живой контур s3w-mesh66ace094-* (east1/east2, Up) — оставить до
  execute-фазы (материал для verify фикса фейла 3) или зачистить сейчас
  по README-правилам?
