# Spec t01-reassign-exec-channel: reassign KafkaWorker — единый Kafka-протокол, docker-exec убирается

Дата: 2026-10-24. Пункт трека `arch/roadmap/kafkaworker.md` →
`t01-reassign-exec-channel` (отклонение arch/25 №24).

## 1. Цель

Переделать docker-взаимодействия reassignment партиций KafkaWorker на единый
механизм Kafka-протокола:

- сейчас `PartitionReassignerProcess` подаёт reassign-планы командой
  `kafka-reassign-partitions.sh --execute` через `ExecNodeAsync` внутрь
  kafka-контейнеров (docker exec) — отклонение от arch/25 §§1–2;
- целевое состояние: подача плана прямо из процесса воркера операцией
  Kafka-протокола `AlterPartitionReassignments` (KIP-455; идемпотентность
  подачи сохраняется), чтение метаданных — тем же каналом (уже AdminClient);
- docker-exec (`ExecNodeAsync` в kafka-контейнеры) из KafkaWorker исчезает
  полностью: метод уходит из `IClusterDriver` KafkaWorker вместе с
  реализациями;
- etcd-прогресс `/kafkaworker/reassignments/<C>` — формат, порядок записи и
  жизненный цикл НЕ меняются; `ReassignPlanner` (чистые функции планов) не
  меняется; AdminPanel-контракт не меняется.

## 2. Принципы

1. **arch-first** — контракт уже обновлён этим же изменением:
   - `arch/16-kafkaworker.md`: п.9 списка процессов, §2.3 (клиентские
     подключения воркера, роль admin), §2.4 (новый канон: протокольный
     миниклиент вместо CLI-exec), §5 I (подача батча), §8
     (`ReassignExecSec` → `ReassignSubmitSec=60`), §9 R5 (риск миниклиента
     вместо JVM-CLI);
   - `arch/25-component-communications.md`: канал 24 канонизирован
     («KafkaWorker → Kafka (reassignment партиций) — Kafka-протокол:
     Describe AdminClient + подача AlterReassignments/KIP-455, миниклиент
     arch/16 §2.4»), §5 — отклонения теперь №21–23 (пункт №24 удалён).
2. **Только сетевые протоколы** (arch/25 §1): никаких exec/logs/stdout в
   продуктовых каналах KafkaWorker.
3. **Seam-паттерн** (Puzzle §7): процессы не видят типов протокольного
   клиента; всё через `IKafkaAdminClient` + `Result`.
4. **Идемпотентность и наблюдаемость состояния** — критерий завершения по
   факту метаданных, повторная подача безопасна (KIP-455), состояние
   перестраиваемо от факта (arch/16 §5 I без изменений).
5. **Multi-host**: клиент воркера ходит по CLIENT endpoints из etcd —
   доступность не зависит от docker-хоста конкретного брокера.
6. .NET 10, `TreatWarningsAsErrors`, идентификаторы — английские, комментарии
   и документация — русские, тесты — AAA.

## 3. Контекст (факты, на которых стоит дизайн)

- Метаданные describe-all уже читаются AdminClient'ом
  (`IKafkaAdminClient.DescribeTopicsAsync(includeInternal: true)`) — exec
  используется ТОЛЬКО для подачи плана (`SubmitBatchAsync`: `ReassignCli`
  строит sh-команду, `driver.ExecNodeAsync` исполняет её в контейнере,
  fallback на первый RUNNING-контейнер при недоступности drain-контейнера).
- **Confluent.Kafka НЕ экспонирует AlterPartitionReassignments**: проверено
  по фактическим сборкам в локальном NuGet-кэше: `Confluent.Kafka.dll`
  2.15.1 (версия проекта, `src/Directory.Packages.props`) и 2.16.0
  (последняя на nuget.org, октябрь 2026) — среди admin-методов только
  `AlterConsumerGroupOffsetsAsync`/`AlterUserScramCredentialsAsync`/
  `IncrementalAlterConfigsAsync` и пр.; `*PartitionReassignments*` нет
  (ограничение librdkafka). Утверждение arch/16 «Confluent.Kafka операцию
  не экспонирует» — факт. Прецедент: Puzzle `docs/01.16-kafka.md` D8 — там
  мутация RF по этой же причине не реализована вовсе.
- Точные схемы протокола — из апстрим-схем apache/kafka 4.0.0
  (`clients/src/main/resources/common/message/*.json`, приведены в
  Приложении A): AlterPartitionReassignments существует ТОЛЬКО в версии 0 и
  является flexible; SaslHandshake v0–1 и SaslAuthenticate v0–1 — не
  flexible; ApiVersions v0–4 (flexible с v3).
- В прод-коде KafkaWorker `ExecNodeAsync` других потребителей нет (только
  `PartitionReassignerProcess`); тестовые потребители —
  `ReassignmentTests.cs:364` (подача идемпотентности) и
  `PortAllocHealingTests.cs:66` («контейнер жив» после healing-ветки).
- `ReassignExecSec` упоминается только в `appsettings.json`,
  `Options.cs`, `Program.cs`, `ProcessCommon.cs`, arch/16 §8.
- Прецедент собственного TCP+TLS миниклиента в монорепо —
  `ValkeyWorker.Core/Valkey/ValkeyConnection.cs` (одна команда = одно
  короткоживущее соединение, `SslStream` + per-cluster CA через
  `Shared.Tls.TlsChain.ValidateChain` + SAN-проверка хоста).

## 4. Структура/компоненты

### 4.1. `KafkaProtocolWriter` / `KafkaProtocolReader` (новое; internal)

Файл `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolCodec.cs`. Писатель и
читатель примитивов Kafka-протокола (без внешних зависимостей; всё
big-endian):

- `WriteInt16/32/64`, `ReadInt16/32/64`;
- unsigned varint (compact-примитивы: длины/счётчики — 7 бит на байт,
  старший бит — продолжение), `ReadUVarint`;
- строка old-style (`int16 длина` + UTF-8; −1 = null) — header/не-flexible
  тела; compact-строка (uvarint `длина+1`; 0 = null) — flexible тела;
- байты (`int32 длина` + данные) — SaslAuthenticate.AuthBytes;
- compact-массивы (uvarint `счётчик+1`; 0 = null) и old-style массивы
  (`int32 счётчик`);
- секция tagged fields (для наших сообщений всегда ПУСТАЯ: пишем uvarint 0;
  при чтении — скипания полей по тегам не требуется: собственные секции
  пусты, но ридер обязан уметь пропускать tagged-поля ответа брокера —
  unknown tags: `uvarint count`, затем для каждого `uvarint tag`,
  `uvarint size`, `size байт` — реализовать скип);
- кадр: `int32 длина` + payload (заголовок + тело) — и запрос, и ответ;
- RequestHeader v1 (не-flexible запросы): `int16 api_key`, `int16
  api_version`, `int32 correlation_id`, client_id old-style; RequestHeader
  v2 (flexible запросы): то же + пустая tagged-секция. ВАЖНО (апстрим
  RequestHeader.json): client_id ВСЕГДА сериализуется old-style (int16
  длина), даже в flexible-запросах;
- ResponseHeader v0 (`int32 correlation_id`) для не-flexible ответов, v1
  (+ пустая tagged-секция; при чтении — пропуск по факту) для flexible.

Значения: `ClientId = "kfw-reassign"`.

### 4.2. `KafkaProtocolClient` (новое; internal)

Файл `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolClient.cs`. Один
публичный метод:

```csharp
Task<Result> AlterReassignmentsAsync(
    string bootstrap, string user, string password, string? caPem,
    IReadOnlyList<ReassignMove> moves, TimeSpan connectAndCommandTimeout, CancellationToken ct)
```

Поведение (паттерн `ValkeyConnection.ExecuteAsync`):

1. Пустой `moves` → `Result.Failed` ("empty reassignment batch") без сети
   (защитный guard; процесс такие не подаёт).
2. `bootstrap` — строка `host:port[,host:port…]` (CLIENT endpoints из etcd);
   endpoints перебираются по очереди до первого полного успеха; все
   отказали → `Result.Failed` с перечнем причин.
3. Один endpoint: `TcpClient.ConnectAsync` → `SslStream`
   (`SslClientAuthenticationOptions.RemoteCertificateValidationCallback`,
   `TargetHost = host`): доверие — ВСЕ `CERTIFICATE`-блоки `caPem` в
   `CustomRootTrust` (окно ротации CA даёт bundle OLD+NEW — доверяем обоим;
   парсер всех блоков PEM — `internal static KafkaProtocolClient.
   ParseCertificates`, см. §4.3) + SAN хоста endpoint (DNS/IP — SAN-правило
   arch/16 §2.3 строит SAN по advertised-хосту). `caPem == null` — без
   TLS-валидации (тесты/fake, семантика фабрики адаптеров).
4. Диалог по одному соединению (correlation id — локальный счётчик 1..N,
   сверяется в каждом ответе, несовпадение → `Result.Failed`):
   - **ApiVersions v0** (api key 18, header v1, ПУСТОЕ тело; до
     аутентификации — брокер отвечает на SASL-листенере pre-auth).
     Ответ v0 (header v0): `int16 error_code` (≠0 → fail endpoint), массив
     old-style `int32 count` × {`int16 api_key`, `int16 min`, `int16 max`}.
     Требование: ключ 45 присутствует (min ≤ 0 ≤ max) — иначе
     `Result.Failed("broker does not support AlterPartitionReassignments")`
     — fail-fast с внятной диагностикой.
   - **SaslHandshake v1** (api key 17, header v1): тело — old-style строка
     `"PLAIN"`. Ответ v1: `int16 error_code` (≠0 → fail), old-style массив
     механизмов (читаем, не сверяем жёстко — механизм задан конфигом
     кластера).
   - **SaslAuthenticate v0** (api key 36, header v1): тело — байты
     `\0user\0password` (UTF-8, SASL PLAIN: пустой authzid + NUL + authcid +
     NUL + passwd). Ответ v0: `int16 error_code`, old-style nullable строка
     `error_message`, байты `auth_bytes` (читаем и игнорируем); error_code ≠ 0
     → `Result.Failed` с `error_message`.
   - **AlterPartitionReassignments v0** (api key 45, header v2 — flexible):
     тело: `int32 timeout_ms = 60000` (апстрим-дефолт); compact-массив
     топиков: для каждого — compact-строка имени, compact-массив партиций,
     затем tagged-секция топика (у нас пустая; tagged-секция структуры —
     одна и всегда после ВСЕХ её полей); партиция: `int32 partition_index`,
     компакт-массив `int32`-реплик (НЕ null — отмену переезда не используем),
     затем tagged-секция партиции (пустая); закрывающая tagged-секция тела
     (пустая).
     Ответ v0 (flexible, header v1): `int32 throttle_time_ms`,
     `int16 error_code`, compact nullable `error_message`, compact-массив
     (nullable: 0 = null при top-level ошибке) `responses[] { name,
     partitions[] { partition_index, error_code, error_message compact
     nullable, tagged }, tagged }` — tagged-секция каждой структуры в конце,
     после всех полей; закрывающая tagged-секция тела присутствует ВСЕГДА,
     в т.ч. при top-level ошибке с responses=null. Top-level error_code ≠ 0
     → `Result.Failed` с error_message; иначе любой per-partition
     error_code ≠ 0 → `Result.Failed` со сводкой вида `topic[p]: <code>
     (<message>)` по всем ненулевым; все нули → `Result.Success` (план
     принят).
5. Бюджет: linked CTS `connectAndCommandTimeout` на весь endpoint-попытка
   (connect + TLS + SASL + round-trip); любые исключения транспорта/протокола
   → `Result.Failed(new ApplicationException($"kfw-reassign {host}:{port}
   <этап>: <причина>", inner))` — паники нет.
6. Соединение закрывается после ответа (per-call; `using`).

Идемпотентность: повторная подача того же assignment — штатная семантика
KIP-455 (замещает/подтверждает текущий reassignment), дедуп уровня процесса
(`ReassignRetrySubmitSec`) не меняется.

### 4.3. `Shared.Tls.TlsChain` — перегрузка для CA-bundle

`src/Shared.Tls/TlsChain.cs`: добавить
`ValidateChain(X509Certificate?, IReadOnlyList<X509Certificate2> anchors)` —
`CustomRootTrust` со ВСЕМИ якорями (существующая односертовая перегрузка
делегирует новой с одним элементом; существующие вызовы ValkeyWorker не
меняются). Парсер всех CERTIFICATE-блоков PEM — `internal static
KafkaProtocolClient.ParseCertificates(string pem) → IReadOnlyList<
X509Certificate2>` (тот же файл, близость использования; ValkeyPki не
трогаем): visible-internal через `InternalsVisibleTo
"KafkaWorker.UnitTests"` (уже объявлен в
`KafkaWorker.Provisioning.csproj`) ради прямого юнит-теста bundle-расщепления
(§4.8) — поведенчески эквивалентно приватному, тестируемость лучше.

### 4.4. Seam: `IKafkaAdminClient.AlterReassignmentsAsync`

`src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs` — новый метод:

```csharp
// Подача reassignment-батча (KIP-455, arch/16 §2.4): план принят либо
// Result.Failed (top-level/per-partition ошибки Kafka). Идемпотентна.
Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct);
```

Реализация в `KafkaAdminClient` (адаптер, `KafkaAdminClient.cs`): тот же
каркас `RunAsync` (счётчик операций кэша, unhealthy-пометка при ошибке,
отмена host'а не фейлит), действие — `new KafkaProtocolClient().
AlterReassignmentsAsync(bootstrap, user, password, caPem, moves,
NetworkSubmitTimeout, ct)`, где `NetworkSubmitTimeout` — internal static
readonly `TimeSpan NetworkSubmitTimeout = TimeSpan.FromSeconds(30)`
(per-endpoint бюджет connect+TLS+SASL+round-trip;
подача регистрируется контроллером и отвечает быстро — это не ожидание
физического переноса данных). Клиент протокола создаётся per-call: соединение
короткоживущее, кэшировать нечего (нативного клиента нет — churn-проблемы t05
не применимы).

### 4.5. Процесс `PartitionReassignerProcess`

`src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`:

1. Из конструктора УДАЛЯЕТСЯ параметр `IClusterDriver driver` (после ухода
   exec процесс не работает с docker вовсе). Все 6 call-site'ов конструктора
   правятся: `Program.cs`; тесты — `PartitionReassignerProcessTests` (два
   места), `ReassignmentTests`, `RotationTimeoutTests`, `KafkaActiveGateTests`
   (`KafkaClusterFixture` конструкторов процесса не содержит и не правится).
2. `RunAsync` создаёт адаптер как сейчас (`await using var admin =
   adminFactory.Create(...)`); адаптер передаётся параметром в
   `RunDrainAsync` / `RunBalanceAsync` / `SubmitBatchAsync`.
3. `SubmitBatchAsync`: блок exec (bootstrap `liveBrokerNames`, выбор
   `execNode`, `execCts`, `ReassignCli.BuildExecCommand`,
   `driver.ExecNodeAsync`, fallback на первый RUNNING-контейнер) ЗАМЕНЯЕТСЯ
   на:

```csharp
using var submitCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
submitCts.CancelAfter(TimeSpan.FromSeconds(options.SubmitSec));
var submit = await admin.AlterReassignmentsAsync(batch, submitCts.Token);
if (!submit.IsSuccess)
    return Fail(cluster, submit.Error!, "submitting-batch"); // следующий тик переподает
```

   Внешний бюджет `options.SubmitSec` (бывший `ExecSec`) покрывает все
   endpoint-попытки клиента (per-endpoint — внутренние 30 с). Fallback-ветки
   «exec в drain-контейнер недоступен → первый RUNNING» исчезают вместе с
   exec: подача идёт по сети к CLIENT endpoints, контейнер-цель не нужна;
   идемпотентность KIP-455 сохраняет безопасность переподач тем же тиком.
4. XML-док класса: формулировки про «docker-exec» заменить на «подача
   AlterReassignments (KIP-455) протокольным миниклиентом arch/16 §2.4».
5. ВСЁ остальное (drain/balance-сценарии, minISR-guard, USR-критерий,
   прогресс-ключ, дедуп `RetrySubmitSec`, экспирация заявок t10, waiting-
   фазы, cancelled-ветка) — без изменений.

### 4.6. Опции: `ReassignExecSec` → `ReassignSubmitSec`

- `ProcessCommon.cs`: `ReassignOptions(IntervalSec, BatchPartitions,
  SubmitSec, RetrySubmitSec, RotationTicketTimeoutSec = 3600)`;
  `Default = new(15, 10, 60, 120, 3600)`.
- `Options.cs` `ThresholdsOptions`: свойство `ReassignExecSec` →
  `ReassignSubmitSec`, default 60 (бюджет подачи: connect+TLS+SASL+
  AlterReassignments round-trip; бывший exec-JVM-бюджет 180 не нужен —
  подача отвечает за секунды, а не ждёт переноса данных).
- `appsettings.json`: `"ReassignSubmitSec": 60`.
- `Program.cs`: маппинг на новый параметр.
- Комментарии — «бюджет reassign-подачи батча (§2.4)».

### 4.7. Удаления

1. `src/KafkaWorker.Provisioning/Processes/ReassignCli.cs` — весь файл
   (BuildExecCommand/Bootstrap/BuildAdminProperties/BuildAssignmentJson
   мертвы вместе с exec).
2. `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs` — весь
   файл.
3. `IClusterDriver.ExecNodeAsync` (KafkaWorker,
   `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs:73`) и обе реализации
   (plain `:364`, swarm `:510`) — метод и приватные exec-хелперы, которые
   после этого остаются без потребителей. `Shared.Docker` (общий клиент
   Engine API с exec-семантикой) НЕ трогаем — им пользуется PgWorker
   (отклонение №23 — отдельная задача `t03`).
4. `FakeKafkaDriver.ExecNodeAsync` / `Execs` / `ExecHandler`
   (юнит-фейк `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`) —
   удалить.
5. `PortAllocHealingTests.cs:66`: проверка «контейнер жив» через
   `ExecNodeAsync(cluster, "broker1", ["true"])` — строку удалить; живость
   контейнера уже покрыта соседним ассертом `ListNodeObjectsAsync` (объект
   существует, пересоздания не было) и `admin2.DescribeClusterAsync`
   (кластер отвечает).

### 4.8. Тесты

Новые юнит-тесты `src/tests/KafkaWorker.UnitTests/Kafka/KafkaProtocolCodecTests.cs`
(golden bytes, по Приложению A; каждый — AAA):

- кодирование ApiVersions v0 (header v1 + пустое тело) — точные байты;
- кодирование SaslHandshake v1 (`"PLAIN"`) и SaslAuthenticate v0
  (`\0admin\0pw`) — точные байты;
- кодирование AlterPartitionReassignments v0 (header v2 + тело с двумя
  топиками, три партиции, реплики `[1,2,3]`/`[2]`) — точные байты,
  включая uvarint-длин и пустые tagged-секции;
- чтение ответа AlterPartitionReassignments v0: успех (все коды 0),
  top-level error, per-partition error (сводка в сообщение), nullable
  compact-строки (0x00 = null);
- ResponseHeader v1 — пропуск tagged-полей ответа (unknown tag со Size —
  скип не падает);
- old-style против compact-строк/массивов (int16 против uvarint) —
  изоляция примитивов;
- прямой юнит-тест парсера CA-bundle: `ParseCertificates_Splits_Bundle` —
  PEM из двух CERTIFICATE-блоков (bundle OLD+NEW окна ротации) → ровно два
  сертификата (метод internal — виден через InternalsVisibleTo, §4.3).

Юнит-тесты процесса (`PartitionReassignerProcessTests`):

- риг: конструктор без driver; `FakeKafkaAdminClient` расширяется полем
  `SubmittedMoves` (List<ReassignMove>, add range на каждый вызов) и
  опциональным `FailSubmits` (bool → Result.Failed);
- `Drain_Submits_Batch_And_Writes_Progress`: вместо ассерта exec — ассерт
  `SubmittedMoves` (первый батч ≤ BatchPartitions, ожидаемые реплики);
- тест fallback-exec (недоступность drain-контейнера → второй контейнер) —
  УДАЛИТЬ (механизма больше нет);
- тест дедупа переподачи — сохранить, наблюдение через `SubmittedMoves`;
- новый кейс: `FailSubmits=true` → тик Failed с фазой `submitting-batch`,
  прогресс не пишется, следующий тик переподаст.

Интеграционные (`src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs`,
docker, KafkaClusterFixture — свой контур per-class, зачистка уже есть):

- `Drain_RemovesNonEmptyBroker_ReducesRF`, `Balance_RestoresRF_AfterRepeatedAdd`
  — контент без изменений (процесс уже гоняет новый канал); риг —
  `ReassignOptions(..., SubmitSec: 60, ...)` и конструктор без driver;
- `Reassign_ResubmissionIsSafe`: Act 2 (CLI-exec подача того же assignment
  дважды) заменить на двойной вызов seam-метода:

```csharp
await using var admin = fixture.AdminFactory.Create(creds.Bootstrap, adminUser!, adminPassword!, creds.CaPem);
for (var i = 0; i < 2; i++)
{
    var submit = await admin.AlterReassignmentsAsync(moves, ct);
    submit.IsSuccess.Should().BeTrue($"resubmission #{i + 1} must not fail: {submit.Error?.Message}");
}
```

  Assert (describe до/после идентичен) — без изменений.

Новых интеграционных сценариев не заводим: существующие три покрывают
drain/balance/идемпотентность через новый канал.

### 4.9. Что НЕ меняется

- `ReassignPlanner` (чистые планы drain/balance, USR-критерии);
- etcd-ключи и форматы: `/kafkaworker/reassignments/<C>`,
  `/kafkaworker/rebalances/<C>`, ticket_outcomes — буква roadmap;
- AdminPanel (читает прогресс-ключ), сид (пишет ключ того же формата);
- Remove/AddBroker, TopicSync, коллекторы метрик;
- `KafkaAdminClientFactory` (кэш, unhealthy, дрейн) — адаптер лишь получает
  новый метод;
- PgWorker exec-механики (arch/25 №23 → `t03`), `Shared.Docker`.

## 5. Фазы реализации

1. **Кодек + клиент**: `KafkaProtocolCodec.cs`, `KafkaProtocolClient.cs`,
   перегрузка `TlsChain.ValidateChain`, юнит-тесты кодека (golden bytes).
2. **Seam + процесс**: `IKafkaAdminClient.AlterReassignmentsAsync`,
   реализация адаптера, `ReassignOptions`/`Options`/`appsettings`/
   `Program.cs` rename, `PartitionReassignerProcess` на новый submit (убрать
   driver), правка юнит-тестов процесса и фейка.
3. **Чистка exec**: удалить `ReassignCli`+тесты, `ExecNodeAsync` из
   KafkaWorker-драйвера+фейка, правка `PortAllocHealingTests`.
4. **Прогоны**: сборка `src/PgWorker.slnx -c Release` (две отдельные
   команды сборка → тесты, `--no-build`); юниты KafkaWorker; docker-интеграции
   `ReassignmentTests` (+ smoke соседних Kafka-серий фикстуры при правках
   рига); зачистка docker-контуров после каждой серии (AGENTS.md).

## 6. Ограничения

- Никаких новых NuGet-пакетов (клиент — собственный, примитивы протокола —
  stdlib).
- Таймауты: интеграционные фикстуры `BrokerBootSec ≤ 100 с`; бюджеты подач —
  из опций (60/30 с), в тестах — динамические порты фикстуры (уже так).
- `TreatWarningsAsErrors` — ноль предупреждений.
- Язык: комментарии/XML-доки — русские; идентификаторы — английские.
- Исторические пассажи в документах не оставляем; roadmap-пункт
  `t01-reassign-exec-channel` удаляется из `arch/roadmap/kafkaworker.md`
  мерж-гейтом задачи (тем же коммитом слива).

## 7. Принятые решения (закрытые развилки)

1. **Механизм подачи — собственный протокольный миниклиент.** Roadmap
   требует «AdminClient — операцию AlterReassignments (KIP-455)», но
   Confluent.Kafka (2.15.1 проекта; последняя 2.16.0) операцию не экспонирует
   — ограничение librdkafka, проверено по сборкам. Обновление пакета и
   сторонние .NET-библиотеки решения не дают (мейнстрим-альтернатив с
   AlterPartitionReassignments нет; Puzzle D8 тот же вывод). Единственный
   путь «прямо из воркера Kafka-протоколом» — мини-клиент поверх
   TcpClient/SslStream. «AdminClient» в формулировке roadmap понимаем как
   административный Kafka-протокол — фактическая точка входа для процесса
   остаётся seam `IKafkaAdminClient` (метод адаптера), что сохраняет букву
   «подавать прямо из KafkaWorker через AdminClient» на уровне архитектуры.
2. **Точка входа процесса — seam-метод `IKafkaAdminClient`.** Альтернативы
   (отдельный интерфейс/фабрика) отклонены: seam уже существует, процессы не
   видят Confluent/протокольных типов, фейки юнит-тестов уже построены,
   diff процесса минимален.
3. **Канал подключения — CLIENT endpoints + креды admin + ca_pem** (arch/16
   §2.3): INTERNAL-listener из процесса воркера недостижим (docker-DNS сети
   `kfw-net-<C>`); AdminClient уже ходит по CLIENT — единый канал описан в
   обновлённом arch/16 §2.3.
4. **Соединение per-call, без пула/кэша**: подачи редки (тик 15 с, батч за
   раз), паттерн ValkeyConnection; коннект-кост пренебрежима, состояния
   соединения нет — надёжность выше (нет half-open соединений).
5. **Протокольные версии фиксированы**: ApiVersions v0 (pre-auth, header
   v1), SaslHandshake v1 (header v1), SaslAuthenticate v0 (header v1),
   AlterPartitionReassignments v0 (единственная, flexible, header v2).
   Образ брокера pinned `apache/kafka:4.0.0` (R1); все выбранные версии
   существуют с Kafka 2.4 — переговоры версий не нужны, ApiVersions читаем
   только для fail-fast проверки apiKey 45.
6. **Failover по списку endpoints внутри одного вызова** (как PutAsync у
   процесса): первый живой endpoint выигрывает; AlterPartitionReassignments
   — форвардируемый брокером на контроллер API, любой брокер годится.
7. **Опция переименована `ReassignExecSec` → `ReassignSubmitSec`, default
   60** (не «оставить имя» и не «удалить опцию»): exec-семантика исчезла —
   имя не должно врать; бюджет остаётся конфигурируемым по паттерну
   Thresholds. Упоминания — только appsettings/Options/Program/арх, внешних
   потребителей нет.
8. **Двухуровневые таймауты**: per-endpoint `NetworkSubmitTimeout` 30 с
   (internal const адаптера; connect+TLS+SASL+round-trip) и внешний бюджет
   процесса `SubmitSec` через linked CTS (бывший `execCts`-паттерн;
   покрывает все endpoint-попытки). Подача не ждёт физический перенос
   данных — реквестный `timeout_ms=60000` это таймаут обработки заявки, не
   переезда.
9. **ReassignCli.cs и его тесты удаляются целиком** — мёртвый код
   запрещённого канала;golden-функции формата reassignment.json не
   переносятся (новый формат — протокольные структуры кодека, свои тесты).
10. **`ExecNodeAsync` удаляется из KafkaWorker `IClusterDriver`** (обе
    реализации) и фейка: потребителей нет; arch/25 §2 запрещает exec в
    продукте — неиспользуемый метод запрещённой операции не оставляем.
    `Shared.Docker` и PgWorker — не трогаем (№23 — отдельная задача).
    Тестовое использование в `PortAllocHealingTests` покрыто соседними
    ассертами (см. §4.7.5).
11. **Fallback «недоступный drain-контейнер → первый RUNNING» удаляется**:
    exec-цели больше нет; недоступность endpoint'а закрывается failover по
    списку CLIENT endpoints (клиент) + идемпотентной переподачей тика
    (процесс). Семантика «следующий тик переподаст» сохранена.
12. **Валидация TLS — все CERTIFICATE-блоки `ca_pem`** (bundle OLD+NEW окна
    ротации CaRotator) + SAN хоста endpoint; перегрузка `TlsChain` в
    Shared.Tls (общая, с сохранением старой сигнатуры); парсер блоков —
    `internal static KafkaProtocolClient.ParseCertificates` (не private):
    прямой юнит-тест `ParseCertificates_Splits_Bundle` через уже
    объявленный `InternalsVisibleTo "KafkaWorker.UnitTests"`
    (KafkaWorker.Provisioning.csproj); поведенчески эквивалентно приватному,
    тестируемость лучше. ValkeyPki не трогаем — вне области задачи.
13. **Ответ AlterPartitionReassignments**: top-level error ≠ 0 ИЛИ любой
    per-partition error ≠ 0 → единый `Result.Failed` со сводкой; процесс
    трактует как «переподать следующим тиком» (прежняя семантика exec-фейла).
    Отмена reassignment (replicas null) — не используется, не реализуем.
14. **Прогресс-ключ и контракт etcd не меняются** — буква roadmap; меняется
    только транспорт подачи.
15. **`ClientId = "kfw-reassign"`** — старый-style string в header (обязателен
    по схеме; идентифицирует клиента в логах брокера).
16. **Новых метрик не вводим** — телеметрия подач уже наблюдаема журналом
    (фаза `submitting-batch` при фейле) и прогресс-ключом.
17. **arch/25 канал 24 канонизирован отдельной строкой реестра** (не слит с
    каналом 14): реестр описывает способ в терминах кода; отдельная строка
    сохраняет трассируемость операции reassignment. Отклонения §5 — теперь
    №21–23.

## 8. Критерии приёмки

1. `grep -rn "ExecNodeAsync" src/KafkaWorker.*` — пусто; `grep -rn "ReassignCli\|kafka-reassign-partitions" src/KafkaWorker.* src/tests/KafkaWorker.*` — пусто.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — без ошибок и предупреждений (TreatWarningsAsErrors).
3. Юнит-серия KafkaWorker зелёная, включая новые `KafkaProtocolCodecTests` (golden bytes, `ParseCertificates_Splits_Bundle`) и обновлённые `PartitionReassignerProcessTests`.
4. Docker-интеграции `ReassignmentTests` (все 3 теста) зелёные на свежем Release (`--no-build` после отдельной сборки), включая идемпотентность двойной seam-подачи; фикстура зачищает свой контур (существующая механика), сети/контейнеры после серии не остаются.
5. Поведенческие инварианты (арх-контракт): drain непустого брокера завершается демонтажем со снижением RF и сохранностью данных (интеграционный тест T7.1 без изменений ассертов); баланс восстанавливает RF с сохранностью лидеров; повторная подача того же assignment не меняет placement; слепая проба — без подач; прогресс-ключ пишется/удаляется в прежние моменты.
6. arch/16 и arch/25 отражают новое состояние (правки из §2 уже в ветке); roadmap-пункт удалён мерж-гейтом.

## Приложение A. Точные схемы протокола (apache/kafka 4.0.0, message/*.json)

Общие: все int — big-endian. uvarint — unsigned varint (7 бит/байт, MSB
continuation). Запрос/ответ = `int32 длина` + header + body. Correlation id
клиент инкрементирует и сверяет.

### A.1. ApiVersions (api key 18; v0–4, flexible 3+; используем v0)

- Request v0 (header v1, non-flexible): тело ПУСТОЕ.
- Response v0 (header v0): `int16 error_code`; old-style массив:
  `int32 count`, элементы { `int16 api_key`, `int16 min_version`,
  `int16 max_version` }. (ThrottleTimeMs — с v1; в v0 отсутствует.)

### A.2. SaslHandshake (api key 17; v0–1, flexible none; используем v1)

- Request v1 (header v1): old-style строка `mechanism` = `"PLAIN"`
  (int16 длина + UTF-8).
- Response v1 (header v0): `int16 error_code`; old-style массив строк
  `mechanisms` (`int32 count`, элементы — old-style строки).

### A.3. SaslAuthenticate (api key 36; v0–2, flexible 2+; используем v0)

- Request v0 (header v1): `bytes auth_bytes` = `int32 длина` + данные;
  содержимое для PLAIN: `"" NUL username NUL password` (UTF-8, authzid
  пустой).
- Response v0 (header v0): `int16 error_code`; old-style nullable строка
  `error_message` (int16 длина; −1 = null); `bytes auth_bytes` (int32 длина,
  читаем/игнорируем). SessionLifetimeMs — только v1+, в v0 отсутствует.

### A.4. AlterPartitionReassignments (api key 45; ТОЛЬКО v0; flexible 0+)

Tagged-секция структурного объекта — ОДНА и всегда В КОНЦЕ объекта, после
ВСЕХ его полей (правило flexible-версий KIP-482).

- Request v0 (header v2 — flexible: после client_id пустая tagged-секция
  `uvarint 0`; client_id — ВСЕГДА old-style int16-строка):

```
int32  timeout_ms = 60000
uvarint topics_count_1 = N+1                    // compact-массив топиков; 0 = null
повторять N раз:
    compact-строка name                          // uvarint len+1 + UTF-8
    uvarint partitions_count_1 = M+1             // compact-массив партиций
    повторять M раз:
        int32 partition_index
        uvarint replicas_count_1 = K+1           // 0 = null (отмена; не используем)
        K × int32 replica_id
        uvarint 0                                // tagged-секция партиции (пустая)
    uvarint 0                                    // tagged-секция топика (пустая)
uvarint 0                                        // tagged-секция тела (пустая)
```

- Response v0 (header v1 — flexible: correlation_id + пропуск tagged):

```
int32   throttle_time_ms
int16   error_code
compact-строка error_message                     // nullable: uvarint 0 = null
uvarint responses_count_1 = N+1                  // compact-массив; 0 = null (top-level ошибка)
повторять N раз:
    compact-строка name
    uvarint partitions_count_1 = M+1
    повторять M раз:
        int32 partition_index
        int16 error_code
        compact-строка error_message             // nullable
        uvarint 0                                // tagged-секция партиции (пустая)
    uvarint 0                                    // tagged-секция топика (пустая)
uvarint 0                                        // tagged-секция тела — ВСЕГДА (и при responses=null)
```

### A.5. Заголовки

- RequestHeader v1 (non-flexible): `int16 api_key`, `int16 api_version`,
  `int32 correlation_id`, old-style nullable строка `client_id`.
- RequestHeader v2 (flexible): v1 + `uvarint 0` (пустая tagged-секция).
- ResponseHeader v0: `int32 correlation_id`. ResponseHeader v1 (flexible):
  + tagged-секция (при чтении — скип: `uvarint count`, для каждого
  `uvarint tag`, `uvarint size`, `size байт`).

## Приложение B. Затрагиваемые файлы

Новые:
- `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolCodec.cs`
- `src/KafkaWorker.Provisioning/Kafka/KafkaProtocolClient.cs`
- `src/tests/KafkaWorker.UnitTests/Kafka/KafkaProtocolCodecTests.cs`

Правки:
- `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs` (+метод seam)
- `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (реализация)
- `src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`
- `src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs` (ReassignOptions)
- `src/Shared.Tls/TlsChain.cs` (перегрузка anchors)
- `src/KafkaWorker.App/Options.cs`, `src/KafkaWorker.App/Program.cs`,
  `src/KafkaWorker.App/appsettings.json`
- `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (−ExecNodeAsync)
- `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`,
  `…/PartitionReassignerProcessTests.cs`
- `src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs`,
  `…/RotationTimeoutTests.cs`, `…/KafkaActiveGateTests.cs` (call-site'ы
  конструктора процесса: риги без driver)
- `src/tests/KafkaWorker.IntegrationTests/Kafka/PortAllocHealingTests.cs`

Удаления:
- `src/KafkaWorker.Provisioning/Processes/ReassignCli.cs`
- `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs`

Arch (уже в ветке):
- `arch/16-kafkaworker.md`, `arch/25-component-communications.md`
