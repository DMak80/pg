# Спецификация: t06 — rolling-перегенерация брокеров Kafka (новые ресурсы + server-props)

Дата: 2026-09-02. Канон-контракты (обновлены этой задачей, arch-first):
[`arch/15-kafka-clusters.md`](../../../arch/15-kafka-clusters.md) §2/§4/§6,
[`arch/16-kafkaworker.md`](../../../arch/16-kafkaworker.md) (процесс J),
[`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md)
§10.1–10.3 (мутация №15). Roadmap-строка:
[`arch/roadmap/kafkaworker.md`](../../../arch/roadmap/kafkaworker.md)
`t06-kafka-node-regen` — «rolling-перегенерация существующих брокеров с новыми
ресурсами (лимиты cpu/mem) и новыми server-props»; исходное вынесение —
`docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8.

## 1. Цель

Дать оператору возможность менять ресурсы (лимиты cpu/mem контейнера)
существующих брокеров живого кластера и приводить брокеров к новой
декларации **rolling-пересозданием без потери данных**: воркер сам
обнаруживает расхождение лимитов контейнера с декларацией
`brokers/<b>/resources` и пересоздаёт контейнеры по одному (том сохраняется),
а панели показывает прогресс операции. Попутно пересоздание применяет
актуальные server-props: env брокера детерминирован от текущей декларации
(`NodeEnvBuilder`), поэтому изменения заявки кластера (config), не покрытые
dynamic-converge, доезжают рестартом.

Проблема (что сегодня не работает): `resources` применяется только при
создании контейнера — `EnsureNodeAsync` идемпотентен по имени, docker не
меняет лимиты живого контейнера; мутации изменения ресурсов существующего
брокера нет вовсе (в контракте 14 мутаций — создание/добавление только).
Декларация и факт расходятся навсегда.

Не-цели: квоты диска (disk — инфо-поле, roadmap), bandwidth-throttle,
изменение advertised/портов/ролей (детерминированы placement'ом и
фиксируются при создании), отмена идущей регенерации (операция сходится
сама; «отмена» = вернуть декларацию — следующий тик перегенерирует к ней).

## 2. Принципы

1. **arch-first**: контракт etcd обновлён до кода (см. §4); код — отражение
   контракта. `resources` остаётся декларацией панели; `/kafka/` и
   `/kafkaworker/` пишет только воркер (мутация — через его API).
2. **Декларативный автоконверге, без заявки-тикета** (решение пользователя):
   воркер сводит факт к декларации сам — как ClusterConfigConverger (E)
   сводит конфиги, NodeRegenerator (J) сводит лимиты. Рестарты — только по
   фактическому расхождению cpu/mem, вслепую не рестартуем (порт слепоты
   надзора C: собственная недоступность docker — не повод трогать брокеров).
3. **Данные неприкосновенны**: пересоздание контейнера всегда сохраняет том
   `kfw-<C>-<b>-data`; брокер возвращается со своими данными (RF>1 —
   self-healing репликацией; рестарт — штатный rolling-цикл Kafka).
4. **Один брокер за тик** (решение пользователя): темп — прецедент надзора C
   («одно пересоздание по молчанию за тик»); следующий брокер — только после
   возврата предыдущего в `RUNNING`. Кворум и ISR не роняются массово.
5. **Идемпотентность/takeover**: состояние операции = факт docker + декларация
   + прогресс-ключ (перестраиваем от факта); каждый шаг перепроверяет факт;
   смерть контроллера — takeover ≤ TTL 15 с + тик, продолжение с journal-фазы.
6. **Толерантность парсеров** (arch/15 §6): битый JSON прогресса — мусор,
   воркер перезаписывает; панель не падает.
7. Язык документации — русский, идентификаторы — английские; тесты — AAA,
   динамические порты docker, `BrokerBootSec` интеграционных фикстур ≤ 100 с.

## 3. Рассмотренные подходы (зафиксированные решения)

### 3.1. Триггер — автоматический converge (выбор пользователя)

Отвергнутые альтернативы: (а) заявка-тикет `/kafkaworker/regens/<C>` по
образцу ротаций — оператор явно инициирует rolling; (б) комбинированная
мутация «PUT resources + авто-заявка». Выбран автоконверге: симметрия с
converger'ом E (декларация — единственная правда), меньше сущностей
(нет заявки/отмены/409-очереди). Риск «неожиданные рестарты» снят п. 3.2 —
рестарт строго по расхождению, которое оператор создал сам, изменив
декларацию. Формат `/kafkaworker/regens/<C>` остаётся — но как
**live-прогресс** (пишет воркер на время операции, по образцу
`reassignments/`), а не заявка.

### 3.2. Предмет сверки — только лимиты cpu/mem (выбор пользователя)

Триггер регенерации — расхождение `inspect`-лимитов контейнера
(NanoCPUs/Memory) с `brokers/<b>/resources` (cpu/mem). `disk` не сверяется
(квот нет — инфо-поле). Env-дрейф триггером НЕ является: конфиг-мутации
(№3) применяются dynamic-converge'ером без рестартов — сверять env значило
бы рестартовать брокеров из-за уже применённых изменений. Env пересобирается
попутно тем же пересозданием (новые server-props доезжают рестартом);
отдельного канала server-props не вводим (YAGNI — расширяется управляемыми
полями config при потребности).

### 3.3. Темп — один брокер за тик (выбор пользователя)

Тик регенератора делает максимум ОДНО пересоздание: Remove(том жив) →
Ensure(лимиты из декларации) → `state=PROVISIONING`. Возврат в `RUNNING` —
штатной механикой AddBrokerProcess (F) следующих тиков (по факту
DescribeCluster; `endpoints`/portalloc не меняются — адрес стабилен).
Новый брокер регенерируется только когда в кластере нет недоведённых нод
(все `RUNNING`). Отвергнуто «все за один тик с ожиданием» — долгий тик
блокирует клэйм и остальные процессы кластера.

### 3.4. Прогресс — live-ключ `/kafkaworker/regens/<C>` (выбор пользователя)

По образцу `reassignments/`: ключ живёт только во время операции, панель
читает и показывает в деталях кластера. Отвергнуты «только state-бейджи»
(панель не видит масштаба) и «+ warning-алерт» (регенерация — штатная
операция, не инцидент).

### 3.5. Валидация мутации — любые значения в границах §10.3 (выбор пользователя)

Уменьшение cpu/mem разрешено (риск OOM — ответственность оператора,
UI-предупреждение; arch/16 R7). Не вводим force-флагов и асимметрии с
созданием кластера.

## 4. Контракт etcd (сводка; канон — обновлённые arch/15, arch/16, adminpanel/02)

### 4.1. Ключи

| Что | Изменение |
|---|---|
| `brokers/<b>/resources` (arch/15 §2) | формат неизменен (`{"cpu":"2","mem":"4Gi","disk":"40Gi"}`); примечание дополнено: изменение существующих — мутация №15 (через API воркера), применяет NodeRegenerator (J) rolling-пересозданием; `disk` — инфо, действий не вызывает |
| `/kafkaworker/regens/<C>` (arch/15 §4, новый) | обычный ключ; пишет ТОЛЬКО воркер; `{"brokers_total":3,"brokers_remaining":2,"current_broker"?:"broker2","updated_unix":1750000000,"instance":"…","last_error"?:"…"}`; put при старте первого пересоздания, del по сходимости; отсутствие ключа = операции нет |
| Читаемое панелью из `/kafkaworker/` | `rotations/`, `rebalances/`, `reassignments/` **+ `regens/`** |
| Обработка сбоев (arch/15 §6) | битый JSON прогресса regens — parseError + `kafka-key-malformed`; воркер перезаписывает |
| Очистка при TO_REMOVE (arch/16 §3.2, X2) | del `/kafkaworker/regens/<C>` вместе с claims/work/portalloc/rotations/rebalances/reassignments |

### 4.2. Мутация №15 (adminpanel/02 §10.2; панель → API воркера)

`PUT /api/kafka/clusters/{c}/brokers/{b}/resources`, тело
`{cpu?, memGi?, diskGi?}` (null = не менять; **хотя бы одно поле
обязательно** — иначе 400; порт семантики PUT config):

- Guard'ы (по прямым чтениям etcd, не по снапшоту): имя кластера/брокера
  каноническое (иначе 404); кластер существует и Active (409 иначе); брокер
  существует (404); `brokers/<b>/state` не `TO_REMOVE`/`REMOVING` (409 —
  декларация демонтажа); валидация границ §10.3: cpu 0.01..64, memGi/diskGi
  целые 1..65536 (400, массив `errors` по полям).
- Запись: put ключа `brokers/<b>/resources` каноническим JSON целиком
  (cpu — каноническая строка `KafkaClusterCreatePlan.Canonical`; mem/disk
  `"<n>Gi"`); RMW по mod_revision не нужен (ключ атомарно перезаписывается,
  формат плоский). Идемпотентен (повтор — та же запись, 200).
- Ответ 200: `{cluster, broker, cpu, memGi, diskGi}` (эффективные значения).
- Отказы: 400/404/409/503 (ProblemDetails; маппинг — порт существующих
  хендлеров). Новые исключения: `KafkaBrokerNotFoundException` (уже есть),
  `KafkaBrokerRemovalInProgressException` (409, new).
- Применение — автоматическое (никакой заявки): NodeRegenerator (§5.2).

### 4.3. Канонические примеры значений (критерий приёмки парсеров)

`/kafkaworker/regens/events` в середине операции:

```json
{"brokers_total":3,"brokers_remaining":2,"current_broker":"broker2",
 "updated_unix":1750000000,"instance":"kfw-1"}
```

`brokers_remaining` — брокеры, ещё не вернувшиеся в `RUNNING` с новыми
лимитами (включает текущего `current_broker`); после возврата последнего
воркер удаляет ключ.

## 5. KafkaWorker

### 5.1. Мутация API №15 (handler)

`src/KafkaWorker.App/Api/Operations/UpdateBrokerResourcesHandler.cs` (новый;
порт AddBrokerHandler/UpdateConfigHandler): guard'ы §4.2 → канонизация →
put. Регистрация в `ApiModule.MapWorkerApi`
(`PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources`) с
маппингом исключений (порт существующих веток: 400 — KafkaValidationException
с `errors`, 404, 409, 503). Валидатор — чистая функция
`KafkaResourcesUpdateValidator.Validate(request)` в `KafkaWriting`
(порт `KafkaCreateValidator`: границы KafkaLimits, «хотя бы одно поле»).

### 5.2. Процесс J — NodeRegenerator (новый; arch/16 §5 J)

`src/KafkaWorker.Provisioning/Processes/NodeRegenerator.cs`. Вызывается в
Active-ветке (`KafkaClusterProcesses.ActiveAsync`) **после ротации (H) и
перед TopicSync (D)**: к моменту J ротация и scale-проход уже разрулили
креды и состав; TopicSync последним сводит реестр к итогу. Только под
живым клэймом `<C>`; journal-before-manipulations.

Тик процесса:

```
J0  claim-чек; guard'ы передержки:
    — жива заявка ротации (/kafkaworker/rotations/<C>) ИЛИ journal
      op=rotate с фазой не done → journal waiting-rotation; no-op
    — жив /kafkaworker/reassignments/<C> → journal waiting-reassign; no-op
    (пересоздания не смешиваются с чужими rolling/переездами реплик)
J1  кандидаты: brokers state=RUNNING и resources != null (остальные —
    чужие процессы: TO_REMOVE/REMOVING — G, PROVISIONING/NOT_INITIALIZED —
    F, UNREACHABLE — надзор C)
J2  сверка лимитов: для каждого кандидата driver.NodeResourcesAsync
    (inspect; §5.3); ошибка инспекта → ошибка тика (никаких пересозданий
    вслепую); контейнера нет → пропуск (надзор восстановит)
    расхождение = NanoCPUs != (long)((double)cpu*1e9) ИЛИ Memory != MemGi*2^30 —
    арифметика 1:1 с ЗАПИСЬЮ DockerEngine (decimal → double → NanoCPUs; не
    (long)(cpu*1e9m): decimal-математика расходится с фактом инспекта для
    cpu, непредставимых точно в double — 0.01, 1.15 — и даёт вечный цикл
    регенерации)
    (resources нет у кандидата — не кандидат; ресурсы есть → лимиты
    обязаны стоять; сверка disk — никогда)
J3  операции нет (расхождений нет И прогресс-ключа нет) → no-op — чужие
    недоведённые ноды (F add-broker / надзор C) прогресс НЕ рисуют
    (§4.1: put при старте первого пересоздания, отсутствие ключа =
    операции нет); расхождений нет, ключ жив → сходимость: del ключа +
    journal done
J4  операция ЖИВА (расхождения есть ИЛИ хвост: ключ жив, а последний
    пересозданный брокер ещё не вернулся в RUNNING), но в кластере есть
    недоведённые ноды (не все brokers state=RUNNING) → обновить
    прогресс-ключ (updated_unix, brokers_remaining по факту) + journal
    waiting-return; новых пересозданий нет (доводит F следующими тиками;
    темп п. 3.3)
J5  регенерация первой (по имени, ordinal) расхождение-ноды:
      journal phase=regenerating:<broker>
      driver.RemoveNodeAsync(cluster, b, removeVolume: false)
      driver.EnsureNodeAsync(spec: лимиты из resources, env —
        BrokerEnvBuilder.Build(snap, b, addr, [appPassword], options);
        адрес/порт — portalloc, те же)
      put brokers/<b>/state = PROVISIONING
      put прогресс-ключ: brokers_total = число расхождений,
        brokers_remaining = total (текущий не вернулся),
        current_broker = b
```

Прогресс-ключ живёт ТОЛЬКО при живой операции (расхождения есть или хвост
недоведённого пересоздания) — фантомный «Регенерация N из N» без реальной
операции запрещён (ложный статус оператору); обновляется каждый рабочий тик
(updated_unix — панель видит живость); при возвратах нод
`brokers_remaining` уменьшается пересчётом от факта (J2). Сходимость = нет
расхождений и все ноды `RUNNING` → del ключа + journal done. PUT ресурсов
посреди операции (между J5 и возвратом) безопасен: нода, пересозданная по
старой декларации, снова попадёт в расхождения — converge к последней
декларации.

Note (толерантность): битый прогресс-ключ воркер просто перезаписывает
(оператор разбирается по факту inspect).

### 5.3. Docker-инспект лимитов

- `IDockerEngine.InspectContainerResourcesAsync(name, ct)` (новый): plain —
  `GET /containers/{name}/json` → `HostConfig.NanoCPUs`, `HostConfig.Memory`
  (те же поля, что пишет `CreateContainerAsync`, rework №5); 404 → null.
- `SwarmClusterDriver`: inspect сервиса `kfw-<C>-<b>` →
  `TaskTemplate.Resources.Limits.{NanoCPUs, MemoryBytes}`; нет сервиса → null
  (новый метод engine `InspectServiceResourcesAsync`; swarm-ветка —
  реализация по образцу ListServicesAsync).
- `IClusterDriver.NodeResourcesAsync(cluster, broker, ct)` →
  `Result<NodeLimits?>`, `record NodeLimits(long NanoCpus, long MemoryBytes)`
  (0 = без лимита; сверка §5.2 J2 — формулы идентичны формулам записи:
  `(long)(cores*1_000_000_000)` в DockerEngine — единая арифметика).

### 5.4. Надёжность

- Идемпотентность: повтор тика после сбоя между Remove и Ensure — надзор
  восстановит отсутствующий контейнер (том жив), J продолжит сверку; между
  Ensure и put PROVISIONING — следующий тик J2 увидит контейнер с новыми
  лимитами и состояние сойдётся (state переведёт F).
- Takeover: состояние = факт + декларация + прогресс-ключ; потеря ключа —
  пересчёт от факта (J2).
- Снапшоты P12: регенерация — БЕЗ снапшотов (воркер не меняет etcd-декларацию;
  как add/remove брокеров, arch/16 §6).
- Регенерация не трогает: `endpoints`, portalloc, role, креды — адресация
  стабильна (том + portalloc детерминированы).

### 5.5. Изменения в смежном коде воркера

- `KafkaClusterProcesses.ActiveAsync`: вставить `regenerator.RunAsync` между
  ротацией и TopicSync.
- `DeprovisioningProcess` X2: del `/kafkaworker/regens/<C>` (вместе с
  остальными координационными).
- `KafkaSnapshotParser`: ничего (regens — не префикс `/kafka/`); читает
  процесс напрямую (как rotations/reassignments).

## 6. AdminPanel

### 6.1. Чтение

- `KafkaSnapshot`: + `IReadOnlyList<KafkaRegenProgress> Regens`; record
  `KafkaRegenProgress(string Cluster, int BrokersTotal, int BrokersRemaining,
  string? CurrentBroker, long UpdatedUnix, string? LastError)` (порт
  KafkaRebalanceTicket/KafkaReassignmentProgress).
- Refresher kafka-снапшота: чтение `/kafkaworker/regens/` (префикс) рядом с
  rotations/rebalances/reassignments; битый JSON — parseError + алерт
  `kafka-key-malformed` (arch/15 §6).
- Inspection API: `KafkaRegenProgressDto` в DTO кластера (симметрично
  reassignment-полю).

### 6.2. Мутация (прокси)

`KafkaOperationsModule`: `PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources`
— прокси 1:1 в API воркера (порт остальных kafka-мутаций: живые
`/kafkaworker/api/` ключи, failover, маппинг ProblemDetails 1:1, 503 при
отсутствии живых инстансов).

### 6.3. UI

- `BrokersTab`: кнопка-иконка «Изменить ресурсы» на строке брокера (дизейбл:
  не-Active кластер, state TO_REMOVE/REMOVING/NOT_INITIALIZED) → модалка
  `EditBrokerResourcesModal` (поля cpu/memGi/diskGi, предзаполнено текущими;
  предупреждение при уменьшении mem/cpu: «уменьшение может привести к
  OOM/деградации — arch/16 R7»; подпись «применяется автоматически
  rolling-пересозданием брокеров, по одному»).
- Прогресс: в `KafkaClusterDetailsPage` (шапка/над вкладками) строка при
  живом regens-ключе: «Регенерация брокеров: 2 из 3, текущий broker2»
  (порт drain-подписи BrokersTab); в BrokersTab у `current_broker` —
  подпись «регенерация». Прогресс пропадает с исчезновением ключа (≤ 2
  тиков поллера).

## 7. Тесты

- **Unit (KafkaWorker.UnitTests)**:
  - сверка лимитов — чистая функция плана: (decl, inspect) → решение
    (совпадает/расходится; null-контейнер; формулы cpu*1e9, MemGi*2^30);
  - валидатор мутации: границы §10.3, «хотя бы одно поле», null-пропуски;
  - journal-фазы/порядок guard'ов J (моки driver/etcd — порт тестов
    процессов): waiting-rotation, waiting-reassign, waiting-return, один
    брокер за тик, del прогресса по сходимости;
  - прогресс-JSON: канонический roundtrip.
- **Integration — API (KafkaApiFactory, etcd-only)**:
  `UpdateBrokerResourcesApiTests`: 200 (канонический JSON в etcd, эффективные
  значения), 400 (границы/пустое тело), 404 (кластер/брокер), 409 (не Active,
  TO_REMOVE/REMOVING-брокер), идемпотентность повтора.
- **Integration — полный цикл (KafkaClusterFixture, docker; BrokerBootSec ≤
  100 с, порты динамические)**: 1-брокерный кластер (быстро; рестарт
  единственного брокера — тест-бюджет бут-времени), доведённый до Active
  циклом Provision-тиков (один тик кластер не поднимает — endpoints из K5
  нужны дискавери): PUT resources (cpu/mem вверх) → прогресс-ключ появился →
  контейнер пересоздан → state RUNNING → ключ исчез. Пересоздание
  доказывается сменой лимитов инспекта (docker меняет лимиты только
  пересозданием) и наблюдаемым PROVISIONING-циклом — отдельный container-Id
  NOT в driver API, осознанное упрощение; топик, созданный ДО, присутствует
  в метаданных ПОСЛЕ (том пережил пересоздание; produce/consume-цикл
  сообщения — не блокирует приёмку). Второй кейс: PUT тех же значений
  (совпадающих) → рестарта нет — прогресс-ключ не ставится, state остаётся
  RUNNING на серии Regen-тиков (любой рестарт обязан поставить PROVISIONING
  и живой ключ — наблюдаемо).
- **Стенд e2e**: `dev-stand/adminpanel/checks/59-kafka-regen.sh` (номер —
  следующий свободный): через API панели PUT ресурсов брокеру сида →
  поллинг снапшота: regens-ключ жив, брокер PROVISIONING → RUNNING → ключ
  исчез; inspect-проверка лимитов контейнера на docker-хосте стенда.

## 8. Волны реализации

- **Волна A — воркер**: контракт arch (уже обновлён этой задачей);
  DockerEngine inspect + NodeLimits + драйверы; NodeRegenerator + конвейер +
  X2; мутация №15 (валидатор, handler, ApiModule); unit + integration.
- **Волна B — панель и стенд**: чтение regens (refresher, DTO), прокси PUT,
  UI (модалка + прогресс), тесты панели; e2e-чек 59; dev-stand при
  необходимости (сид не меняется — ресурсы уже в seed-плане).

Каждая волна — зелёные `dotnet build` (0 warnings) + `dotnet test`.

## 9. Ограничения, допущения, выносы

Допущения (обоснование — §3):

- Автоконверге без заявки; без отмена-механики (обратная декларация = откат).
- Св только cpu/mem; env — попутно; disk — инфо.
- Один брокер за тик; прогресс — live-ключ; без warning-алерта.
- Уменьшение ресурсов разрешено (риск OOM — оператор; UI-предупреждение).
- Прогресс-ключ — операциональная оценка (PUT посреди операции меняет total
  следующим тиком); не аудит (в etcd нет requested_by — формат 1:1 с
  resources-ключом).

Выносы (roadmap `arch/roadmap/kafkaworker.md` — новым тегом при
потребности): квоты диска (docker volume size limits), расширенный список
управляемых server-props декларации, bandwidth-throttle регенерации
(несколько нод за тик с настраиваемым K), регенерация по env-дрейфу
(явный список рестарт-пропсов).

## 10. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный
   (unit без Docker; integration с Docker; фикстуры: динамические порты,
   `BrokerBootSec` ≤ 100 с; комментарии тестов — AAA).
2. **Мутация №15**: `PUT /api/kafka/clusters/{c}/brokers/{b}/resources`
   через API воркера и через прокси панели: 200/400/404/409/503 по §4.2;
   канонический JSON в etcd; повтор идемпотентен.
3. **Автоконверге**: PUT новых cpu/mem у RUNNING-брокера Active-кластера →
   воркер (без каких-либо заявок) пересоздаёт контейнер брокера: том
   сохранён (данные/топики живы), inspect-лимиты == декларации, state
   PROVISIONING → RUNNING; следующий брокер — только после RUNNING
   предыдущего; PUT совпадающих значений не вызывает рестарта.
4. **Прогресс**: `/kafkaworker/regens/<C>` появляется при первом
   пересоздании (brokers_total/remaining/current_broker корректны),
   обновляется на тиках, исчезает по сходимости; панель показывает прогресс
   в деталях кластера ≤ 2 тиков поллера.
5. **Guard'ы**: живая ротация/reassignment — регенерация ждёт (journal
   waiting-*); UNREACHABLE/TO_REMOVE/REMOVING/PROVISIONING-ноды не
   регенерируются; ошибка docker-inspect — никаких пересозданий (тик
   завершается ошибкой, следующий повторит); TO_REMOVE кластера удаляет
   regens-ключ (X2).
6. **Надёжность**: смерть инстанса посреди регенерации — takeover вторым ≤
   TTL 15 с + тик, продолжение по факту; PUT ресурсов посреди операции —
   сходимость к последней декларации.
7. **Панель**: модалка изменения ресурсов с предупреждением об уменьшении;
   битый regens-JSON не роняет парсер (parseError + `kafka-key-malformed`).
8. e2e-чек `59-kafka-regen.sh` зелёный с чистого состояния стенда.

## 11. Открытые вопросы

Нет — продуктовые решения зафиксированы ответами пользователя (§3.1–3.5:
автоконверге; только cpu/mem; один брокер за тик; прогресс-ключ без алерта;
уменьшение ресурсов разрешено), остальные — допущения §9.
