# Спецификация: t02 — reassignment партиций Kafka (drain брокера + ребалансировка)

> Dev-flow, фаза спецификации. Worktree: `feat-t02-kafka-reassignment`. Дата: 2026-08-30.
> Задача — `arch/roadmap/kafkaworker.md` `t02-kafka-reassignment`: reassignment партиций
> (drain брокера, ребалансировка); разблокирует удаление непустого брокера (guard G
> «на брокере есть реплики» — arch/16 §5 G).
> Контракт `arch/` обновлён ДО этой спеки (arch-first): `arch/15-kafka-clusters.md` §4/§6,
> `arch/16-kafkaworker.md` (§0/§2.4/§3/§5 I/§6/§8/§9), `arch/adminpanel/02-etcd-contract.md`
> §10, `arch/adminpanel/03-panels.md` §7. Спека — исполняемая проекция контракта.

## 1. Цель

1. **KafkaWorker** получает девятый процесс — **PartitionReassigner (I)**: перенос реплик
   партиций между брокерами для двух сценариев:
   - **drain** — опустошение брокера с маркером `TO_REMOVE`, на котором есть реплики
     партиций (включая internal `__`-топики): после drain демонтаж процесса G
     продолжается сам (сегодня — вечное journal-ожидание `waiting-partitions`);
   - **balance** — ребалансировка размещения партиций по заявке панели (типовой
     сценарий: add broker → равномерно разложить реплики, восстановить RF до
     `config.replication_factor`).
2. **AdminPanel** получает мутации «Запросить ребалансировку» / «Отменить» и видимость
   прогресса (сколько партиций осталось) в UI + алерты.
3. Исполнение — через официальный CLI `kafka-reassign-partitions.sh` в контейнере
   брокера: в Confluent.Kafka 2.14.2 API reassignment отсутствует (факт зафиксирован
   референсом Puzzle `docs/01.16-kafka.md` §7.6 D8; подтверждено документацией
   Confluent — методы Java `alterPartitionReassignments`/`listPartitionReassignments`
   в .NET-клиенте не экспонированы).

Закрывается попутный дефект guard'а G: текущая проверка «на брокере нет реплик»
читает только не-`__` топики — брокер, несущий лишь реплики `__consumer_offsets`,
считается «пустым» и демонтируется с потерей этих реплик. После t02 guard и drain
работают по полным метаданным (включая internal).

## 2. Принципы

1. **Декларативная заявочная модель — как всегда**: панель пишет заявку
   (`/kafkaworker/rebalances/<C>`, протокол ротаций), воркер исполняет и пишет факт;
   панель никогда не вызывает Kafka-мутации напрямую.
2. **Сходимость от факта Kafka, а не от локального состояния**: план reassignment
   каждый раз вычисляется из свежих метаданных кластера; подача идемпотентна
   (повтор `kafka-reassign-partitions --execute` того же assignment безопасен,
   семантика KIP-455). Рестарт воркера, takeover, потеря прогресс-ключа не ломают
   процесс — следующий тик пересчитывает остаток. Состояние переживает сбои:
   in-flight reassignment живёт в самом Kafka, ход — в etcd-прогресс-ключе.
3. **Данные неприкосновенны**: reassignment только добавляет/переносит реплики;
   демонтаж (G) исполняется только по полностью синхронному пустому брокеру
   (нет реплик в assignment, нет USR затронутых топиков); слепая проба — никаких
   подач (собственная слепота воркера не повод трогать партиции); цели переезда —
   только `RUNNING`-брокеры.
4. **Минимальное вмешательство**: без новых docker-объектов (CLI — docker exec в
   существующий контейнер брокера, прецедент PgWorker `ExecNodeAsync`/t01
   pg_dump-транспорт), без новых пакетов (Confluent.Kafka уже есть; CLI уже в
   образе `apache/kafka:4.0.0`), без новых состояний брокеров.
5. **Ограничение нагрузки батчами**: за одну подачу — не более
   `ReassignBatchPartitions` партиций; следующий батч — после завершения
   предыдущего. Bandwidth-throttle (leader/follower.replication.throttled.rate) —
   roadmap (домашние объёмы, enterprise-усложнение).
6. **Язык и стиль**: документация/комментарии — русский; идентификаторы — английский;
   .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, CPM; тесты — AAA.

## 3. Рассмотренные подходы (зафиксированные решения)

### 3.1. Интеграция reassignment — выбран «CLI в контейнере брокера»

| Подход | Плюсы | Минусы | Вердикт |
|---|---|---|---|
| **A. docker exec `kafka-reassign-partitions.sh` в контейнер живого брокера** (bootstrap через INTERNAL `broker<k>:9092`) | прецедент `ExecNodeAsync` PgWorker (готовый паттерн драйвера plain+swarm); сеть `kfw-net` уже есть; не зависит от клиентского advertised-правила и host-портов; креды `app` уже валидны на INTERNAL (JAAS §2.2 arch/16); без новых docker-объектов | JVM CLI внутри контейнера брокера (митигация: `KAFKA_HEAP_OPTS=-Xmx256m`, одноразовый exec) | **выбран** |
| B. Одноразовый CLI-контейнер (docker run) | изоляция ресурсов | новая способность движка (create+start+wait+exit-code+logs); выбор хоста с сетью/портами; зависимость от `AdvertisedClientHost`/host-gateway | отвергнут: больше движущихся частей при равном результате |
| C. Wire-протокол `AlterPartitionReassignments` (API key 45) своими руками | без CLI/JVM | ручная реализация SASL-хендшейка и binary-протокола Kafka поверх TCP — хрупко и неоправданно для домашней системы | отвергнут |
| D. Ожидать API в Confluent.Kafka | нулевой код | не реализовано в librdkafka-обёртке на 2.14.2 (Puzzle D8 + docs Confluent); срокам не подчиняется | отвергнут |

CLI-вызов детерминирован и идемпотентен: файлы плана и SASL-конфиг передаются
через `sh -c 'printf … > /tmp/kfw-*.json && … kafka-reassign-partitions.sh …'`
(имена файлов с префиксом `kfw-`; содержимое планов не содержит апострофов —
топики `^[a-zA-Z0-9._-]+$`, креды `[A-Za-z0-9]`).

### 3.2. Отслеживание завершения — по метаданным, не по CLI `--verify`

`ListPartitionReassignments` в .NET-клиенте нет; парсинг текста `--verify` хрупок.
Критерий по факту `DescribeTopics` (метаданные): партиция «переехала» = drain-брокер
отсутствует в её `Replicas`; «батч завершён» = все партиции батча переехали И у
затронутых топиков нет under-replicated (ISR == assignment). Отсутствие ListReassignments
не влияет на корректность: переподача того же assignment безопасна, а «ранний» переход
к следующему батчу лишь расширяет параллелизм копирования в пределах уже поданных
партиций.

### 3.3. Снижение RF при drain — допускается автоматически (guard — minISR)

Демонтаж брокера при `B' = B-1 < RF` физически означает RF ≤ B'. Запрещать — тупик
для пользователя (desired-мутации RF нет и не планируется). Решение: план drain
добирает реплик до `min(len(старых), число целей)`; инвариант — не опускаться ниже
`min.insync.replicas` топика (иначе journal-отказ с человекочитаемой причиной:
«снизьте minISR или добавьте брокеров»). Факт RF в реестре обновит автосинк D
(`replication_factor` — факт метаданных). Для internal-топиков minISR — формулы
владения воркера (`min(2,B')`), снижает сам через AlterTopicConfigs при
необходимости.

### 3.4. Balance — converge к декларации

Целевой RF юзер-топиков = `min(config.replication_factor, число целей)`,
internal — формулы §2.1 arch/16 (`min(3,B)`/`min(2,B)`). Первая реплика (лидер)
сохраняется; добор — наименее загруженные живые брокеры (greedy по счётчику плана),
детерминизм сортировкой (topic, partition, brokerId): план стабилен между тиками,
осцилляций нет. Сходимость = факт == план → del заявки. Типовой сценарий
«add broker → rebalance» восстанавливает RF=3 после drain-снижения.

## 4. Контракт etcd (канон — обновлённый `arch/15` §4; здесь — сводка)

Новые ключи координации `/kafkaworker/`:

| Ключ | Кто пишет | Значение |
|---|---|---|
| `rebalances/<C>` | панель (клэйм-txn `version==0` + put — протокол ротаций §9.8 pg-02), воркер del по завершении, панель del — отмена | `{"requested_unix":T,"requested_by":"admin"}` |
| `reassignments/<C>` | только воркер (под живым клэймом): put при активной операции, del по завершении | `{"mode":"drain"\|"balance","drain_broker"?:"broker4","partitions_total":12,"partitions_remaining":5,"submitted_unix":T,"updated_unix":T,"instance":"id","last_error"?}` |

- Deprovisioning (X2) чистит оба ключа вместе с прочей координацией — вечные
  заявки/прогресс невозможны.
- Панель читает из `/kafkaworker/` — `rotations/`, `rebalances/`, `reassignments/`.
- Битый JSON — толерантность по общим правилам arch/15 §6 (parseError + алерт;
  воркер прогресс-ключ просто перезапишет, заявку/прогресс при разборе учитывает
  по факту Kafka).

## 5. KafkaWorker: процесс I (PartitionReassigner)

### 5.1. Место в конвейере

Active-ветка тика: надзор (C) → converge (E) → **reassign (I)** → remove (G) →
add (F) → ротация (H) → TopicSync (D). Drain стоит перед G — к моменту G дренируемый
брокер уже пуст (в пределах одного тика: I подаёт батчи, G демонтирует только
полностью пустых). Троттл процесса — `ReassignIntervalSec` (механика — как
`TopicSyncIntervalSec` в TopicSyncProcess: время последнего успешного прогона,
в памяти; провалившийся тик ретраится без штрафа).

### 5.2. Сценарий drain (приоритетный)

```
D1 claim-чек; describe-all (метаданные ВСЕХ топиков, вкл. __) —
   слепая проба → journal waiting-cluster, никаких подач
D2 drain-кандидаты = брокеры state=TO_REMOVE с репликами (по describe-all);
   нет кандидатов → переход к balance-сценарию
D3 guard-план per партиция: newReplicas = старые без drain + добор
   least-loaded из RUNNING-целей до min(len(старых), цели);
   newReplicas.Count < minISR(topic) → journal-отказ
   «minISR недостижим — снизьте minISR заявкой или добавьте брокеров»
   (internal: minISR снижаем сами до min(2,B'), AlterTopicConfigs — до плана)
D4 партиций без drain-брокера не осталось И затронутые топики без USR
   → drain завершён: del reassignments-ключ, journal done-phase
   (брокер остаётся TO_REMOVE — G демонтирует его сам)
D5 иначе подать батч: первые ≤ ReassignBatchPartitions непереехавших партиций
   (сортировка topic,partition) через CLI exec (§6); переподача того же батча
   не чаще ReassignRetrySubmitSec (дедуп по submitted_unix)
D6 put прогресс-ключ (partitions_total/remaining, mode=drain, drain_broker)
```

### 5.3. Сценарий balance (по заявке `rebalances/<C>`)

```
B1 заявка есть; живые drain-кандидаты → journal waiting-drain (сначала демонтаж)
B2 целевой план (§3.4): детерминированный greedy; факт == план по всем партициям
   → del заявки + del прогресс-ключа (сначала факт, потом del — повтор тика
   доиграет) + journal done-phase
B3 иначе подать батч разошедшихся партиций (§5.2 D5); прогресс-ключ mode=balance
```

Отмена (панель удалила заявку): воркер перестаёт находить заявку → новых батчей
нет, in-flight reassignment Kafka доигрывает сама, прогресс-ключ удаляется по
сходимости или по исчезновению заявки+факта.

### 5.4. Надёжность (памятка проекта: потеря данных недопустима)

- Рестарт воркера посреди drain/balance → takeover ≤ TTL 15 с + тик; процесс
  восстанавливает ход из факта Kafka (перепланирование идемпотентно).
- Смерть CLI-exec (брокер-контейнер умер) → Failed тика; следующий тик выберет
  другой живой контейнер и переподаст (подача идемпотентна).
- Молчание drain-брокера посреди drain: надзор C восстанавливает контейнер
  (том жив); данные не теряются; USR-критерий держит демонтаж до полной синхронизации.
- Двойной контроллер исключён клэймом; прогресс-ключ пишется только держателем.
- Крэш между подтверждением факта и del заявки/прогресса — повтор тика доигрывает.

### 5.5. Изменения в смежном коде воркера

| Точка | Изменение |
|---|---|
| `IKafkaAdminClient` | метаданные с параметром внутренних топиков: `DescribeTopicsAsync(bool includeInternal, CancellationToken)`; `KafkaTopicView` += `IsrPerPartition` (USR-критерий); используется и TopicSync (false), и I/G (true) |
| `IClusterDriver` (KafkaWorker.Docker) | += `ExecNodeAsync(cluster, nodeName, cmd, ct)` — порт PgWorker (plain: running-контейнер по имени `kfw-<C>-<b>`; swarm: running-таск → ContainerId) |
| `RemoveBrokerProcess` | `HasPartitionsAsync` — по describe-all (включая `__`); journal-текст ожидания: «drain идёт (процесс I)» |
| `KafkaClusterProcesses.ActiveAsync` | вставить reassigner между converge и remove |
| `ProvisioningOptions`/`KafkaWorkerOptions` | `Loops { ReassignIntervalSec=15, ReassignBatchPartitions=10 }`, `Thresholds { ReassignExecSec=180, ReassignRetrySubmitSec=120 }` |
| Новые файлы | `PartitionReassignerProcess.cs` (оркестрация D1–D6/B1–B3), `ReassignPlanner.cs` (чистые функции планов drain/balance — юнит-тесты без Kafka), `ReassignCli.cs` (сборка cmd/JSON/properties, exec через драйвер) |

Снапшоты P12 «до/после» — НЕ добавляются: etcd-изменения точечные и обратимые
(консистентно с add/remove брокеров, arch/16 §6).

## 6. CLI-вызов (канон — arch/16 §2.4)

Цель exec — контейнер drain-брокера (drain) или первого живого брокера (balance).
Bootstrap — INTERNAL-listener живых брокеров: `broker1:9092,broker2:9092,…`
(advertised INTERNAL = docker-DNS имена — резолвятся в `kfw-net`; от клиентского
advertised-правила и host-портов не зависит). Команда (одна строка `sh -c`):

```
printf %s '<admin.properties>' > /tmp/kfw-cmd.properties &&
printf %s '<reassignment.json>' > /tmp/kfw-reassign.json &&
KAFKA_HEAP_OPTS=-Xmx256m /opt/kafka/bin/kafka-reassign-partitions.sh
  --bootstrap-server <INTERNAL-bootstrap> --command-config /tmp/kfw-cmd.properties
  --execute --reassignment-json-file /tmp/kfw-reassign.json
```

`admin.properties`: `security.protocol=SASL_PLAINTEXT`, `sasl.mechanism=PLAIN`,
`sasl.jaas.config=…PlainLoginModule required username="app" password="<пароль из etcd>";`.
`reassignment.json`: `{"version":1,"partitions":[{"topic":T,"partition":P,
"replicas":[ids],"log_dirs":["any"×N]}]}`. Таймаут exec — `ReassignExecSec`
(linked CancellationToken); exit != 0 → Result.Failed (stderr в сообщении —
семантика ExecAsync движка).

## 7. AdminPanel

### 7.1. Чтение (Etcd/снапшот)

`KafkaSnapshotRefresher` дочитывает `/kafkaworker/rebalances/` и
`/kafkaworker/reassignments/` (рядом с rotations). Парсеры: `KafkaRebalanceTicket`,
`KafkaReassignmentProgress` — битые ключи → parseError (алерт `kafka-key-malformed`),
не падают. Модель: `KafkaClusterInfo` += rebalance-заявка + reassignment-прогресс.

### 7.2. Мутации (AdminPanel.Api/Operations/Kafka)

| Мутация | Протокол |
|---|---|
| `POST /api/kafka/clusters/{c}/rebalance` | 404 (нет/неканоническое имя, не Active — как ротация), 409 «уже запрошена» (живая заявка — ReadKey + клэйм-txn `version==0` + put), 201 `{cluster, requestedUnix, requestedBy}` |
| `DELETE /api/kafka/clusters/{c}/rebalance` | del заявки; 404 если нет; 204. Отмена безопасна: новые батчи не подаются, поданные Kafka доигрывает |

### 7.3. DTO/UI/алерты

- `KafkaClusterDto` += `rebalance{requestedUnix, requestedBy}?`,
  `reassignment{mode, drainBroker?, partitionsTotal, partitionsRemaining, updatedUnix}?`
  (null = операции нет); summary += `rebalancePending`.
- UI (`KafkaClusterDetailsPage`): бейдж в шапке «drain broker4: осталось 5/12
  партиций» / «ребалансировка: осталось 7/20»; кнопка «Перебалансировать» (модал
  с предупреждением о переносе данных; при живой заявке — «Отменить
  ребалансировку»); на вкладке Брокеры у TO_REMOVE с drain — подпись остатка.
- Алерты: `kafka-rebalance-pending` (info), `kafka-reassignment-stale`
  (warning; `partitions_remaining` не двигается дольше
  `AdminPanel:KafkaAlerts:ReassignStaleSec=900`).

## 8. Стенд и e2e

- `dev-stand/adminpanel/checks/55-kafka-e2e.sh`: шаг «удаление непустого брокера»
  (add broker4 → топик RF=3 → DELETE broker4 → прогресс-ключ появился → drain →
  демонтаж: контейнера и ключей нет, endpoints без адреса, RF топика восстановим
  rebalance-заявкой), шаг «rebalance» (заявка → RF=3 у всех партиций → заявка
  снята), негативы (повторная заявка 409; отмена 204/404).
- `50-kafka-api.sh`: мутации 9/10 на сиде (404/409/201/204 + ProblemDetails).
- Сид (`kafka-seed.sh`): + живая заявка rebalance и прогресс-ключ одного кластера
  (парсер/UI видны без живого воркера).

## 9. Волны реализации

- **Волна A — воркер + канон**: правки `arch/` (сделаны в этой фазе);
  `DescribeTopicsAsync(includeInternal)` + ISR; `ExecNodeAsync` в драйвере;
  `ReassignPlanner` + `PartitionReassignerProcess` + фикс guard'а G;
  конфигурация; unit (планы/guards/дедуп на fake) + integration (Testcontainers:
  drain непустого брокера 3→2, rebalance после add). Панель — НЕ входит.
- **Волна B — панель + e2e**: чтение rebalances/reassignments, мутации 9/10,
  DTO/UI/алерты, сид, чеки `50`/`55`.

## 10. Ограничения, допущения, выносы

Допущения (приняты без пользователя — принципы §2, домашняя система):

- CLI-интеграция exec'ом — единственный путь исполнения reassignment
  (API в .NET-клиенте нет; обоснование §3.1).
- Снижение RF при drain — автоматически, с minISR-guard (§3.3).
- Internal-топики drain'ятся вместе с юзер-топиками; их minISR воркер снижает
  сам (формулы владения); guard G — только по describe-all.
- Progress-ключ — перестраиваемый кэш хода (источник истины — Kafka); потеря
  ключа безопасна.
- Снапшоты P12 «до/после» — не добавляются (etcd-дельта точечная, консистентно
  с add/remove).
- CLI JVM ограничен `-Xmx256m` внутри контейнера брокера; подача — одна
  одновременно на кластер (клэйм).

Выносы (roadmap — в `arch/roadmap/kafkaworker.md` при мерже, по правилам
ведения; новые теги не заводятся в этой спеке):

- bandwidth-throttle reassignment (replication.throttled.rate через dynamic
  broker configs + уборка при сбоях) — enterprise-усложнение;
- preferred leader election после reassignment (`kafka-leader-election.sh`);
- ACL/TLS — уже в `t03-kafka-security`.

## 11. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный
   (unit — без Docker, integration — с Docker).
2. **Drain непустого брокера** (integration/e2e): 3 брокера RF=3, топик с
   данными → add broker4 → `DELETE broker4` (или `broker3`): воркер переносит
   реплики (прогресс-ключ жив, остаток убывает), демонтаж завершается
   (контейнер/том/ключи удалены, `endpoints` без адреса), ни одна партиция не
   потеряла реплики ниже RF-цели, данные топика читаются.
3. **Drain со снижением RF**: удаление брокера при B=3/RF=3 → партиции
   получают RF=2, автосинк обновляет `topics/<T>.replication_factor`,
   демонтаж завершён; повторный add + rebalance восстанавливает RF=3.
4. **Guard minISR**: B=2/RF=2/minISR=2, удаление одного → journal-отказ
   «minISR недостижим», брокер удерживается в TO_REMOVE, реплики не двигаются.
5. **Internal-топики**: после drain у демонтажируемого брокера нет реплик ни
   в одном `__`-топике (describe-all); guard G по непустому только-internal
   брокеру больше не демонтирует его мимо drain (регресс на fix §5.5).
6. **Balance**: заявка → размещение сходится к плану (RF = min(RF_целевого,
   брокеров), лидер-первая-реплика сохранён, счётчик реплик по брокерам
   выровнен в пределах ±1 для равного кластера), заявка снята, прогресс-ключ
   удалён; повторная заявка во время исполнения — 409; отмена — 204, новые
   батчи не подаются, поданные доигрываются.
7. **Надёжность**: kill инстанса посреди drain → takeover ≤ 15 с + тик, drain
   продолжается с факта, данные целы; слепая проба — 0 подач, прошлый
   прогресс-ключ не трогается.
8. **Панель**: мутации 9/10 отдают 201/204/404/409/503 (ProblemDetails),
   идемпотентны; прогресс виден в DTO и UI; битые ключи rebalances/reassignments
   не роняют парсер (parseError + алерт); алерты `kafka-rebalance-pending` /
   `kafka-reassignment-stale` работают.
9. **Deprovisioning**: TO_REMOVE кластера чистит и
   `/kafkaworker/{rebalances,reassignments}/<C>` (вечных заявок нет).
10. e2e-чеки (`50-kafka-api.sh`, `55-kafka-e2e.sh`) зелёные с чистого состояния.

## 12. Открытые вопросы

Нет: продуктовые развилки закрыты решениями §3 (CLI-exec, RF-снижение с
minISR-guard, converge-семантика balance) из канона arch/ и принципов
AGENTS.base («максимально надёжно, минимальное вмешательство, домашний запуск»);
enterprise-доработки (throttle, leader election) — roadmap.
