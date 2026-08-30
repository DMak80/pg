# Kafka Admin + Worker — план реализации (Волны A/B/C)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Третий домен панели (kafka) + сервис KafkaWorker: управление Kafka-кластерами декларативно через etcd (provision/deprovision/надзор/автосинк топиков/converge конфигов), дискавери клиентов только через etcd.

**Architecture:** Панель заявляет (ключи `/kafka/clusters/<C>/`) — KafkaWorker исполняет (docker plain/swarm, KRaft `apache/kafka:4.0.0`, SASL_PLAINTEXT c per-cluster `app_user`/`app_password`) и пишет факт; панель читает отдельный `KafkaSnapshot` + AdminClient-пробы. Канон: `arch/15-kafka-clusters.md` (контракт etcd), `arch/16-kafkaworker.md` (воркер), расширение `arch/adminpanel/02`+`03`.

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), CPM (`Confluent.Kafka` 2.14.2 — единственный новый пакет), xunit.v3 + FluentAssertions + Testcontainers, React+Vite+TS+Mantine, docker compose.

**Spec:** `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` (исполнитель читает spec + этот план; arch-файлы, создаваемые задачами A1–A2, — детальный канон для последующих задач).

## Global Constraints

- Сборка без ворнингов: `dotnet build src/PgWorker.slnx` → 0 warnings (`TreatWarningsAsErrors=true`); все тесты: `dotnet test src/PgWorker.slnx`.
- CPM: новый пакет один — `Confluent.Kafka` 2.14.2 в `src/Directory.Packages.props`.
- KafkaWorker запускается ТОЛЬКО в докере (`deploy/docker-compose.yml`); AdminPanel — хост-процесс (`dotnet run --project src/AdminPanel.Api`).
- Язык: комментарии/документация русские, идентификаторы английские; тесты с AAA-комментариями (`// Arrange / Act / Assert`).
- Имена объектов: контейнеры/тома `kfw-<C>-<b>[-data]`, сеть `kfw-net`, порты 16000–16999; координация `/kafkaworker/`.
- Не ломать pg-часть: существующие тесты PgWorker/AdminPanel остаются зелёными на каждой границе волны.
- Коммит — в feature-ветке после каждой задачи (`feat(kafka): …` / `docs(kafka): …`), без пуша; финальный мерж — отдельным решением пользователя.
- TDD: в каждой кодовой задаче тест пишется раньше реализации; образцы структур и форматов — в arch-файлах задач A1–A2 (создаются первыми).

---

## Карта файлов (итоговая структура)

```
arch/15-kafka-clusters.md                              # A1: контракт etcd /kafka/ + дискавери
arch/16-kafkaworker.md                                 # A2: канон воркера (снапшоты P12, advertised-правило)
arch/README.md                                         # A1: указатель 15; A2: указатель 16
arch/roadmap/kafkaworker.md, arch/roadmap/README.md    # A4: трек t01–t06
arch/adminpanel/02-etcd-contract.md, 03-panels.md      # B1: раздел kafka
src/Directory.Packages.props                           # A8: +Confluent.Kafka
src/KafkaWorker.Core/    (Model/ KafkaDomain.cs, KafkaPasswordGenerator.cs;
                          Planning/ PortAllocator.cs, PlacementPlanner.cs;
                          Templates/ NodeEnvBuilder.cs; Result.cs, DI/, Retry/)
src/KafkaWorker.Etcd/    (Client/ IEtcdGateway+EtcdGateway+Kv;
                          Coordination/ ClaimStore.cs, WorkJournal.cs;
                          Parsing/ KafkaSnapshotParser.cs; SnapshotJob.cs)
src/KafkaWorker.Docker/  (Engine/ IDockerEngine, DockerEngine; Drivers/ IClusterDriver,
                          PlainClusterDriver, SwarmClusterDriver)
src/KafkaWorker.Provisioning/ (Kafka/ IKafkaAdminClient.cs, KafkaAdminClient.cs;
                          Processes/ ProvisioningProcess.cs, DeprovisioningProcess.cs,
                          NodeSupervisor.cs, ClusterConfigConverger.cs, AddBrokerProcess.cs,
                          RemoveBrokerProcess.cs, AppPasswordRotator.cs, TopicSyncProcess.cs,
                          AppSecretEnsurer.cs)
src/KafkaWorker.App/     (Program.cs, Options.cs, Loops/ ReconcileLoop, KeepaliveLoop,
                          SnapshotLoop, KafkaClusterClassifier; HealthChecks/)
src/tests/KafkaWorker.UnitTests/  (Etcd/, Model/, Planning/, Templates/, Provisioning/, App/)
src/tests/KafkaWorker.IntegrationTests/ (Etcd/, Kafka/{KafkaClusterFixture,ProvisioningTests,
                          TopicSyncTests}.cs, E2e/)
src/AdminPanel.Core/     (Kafka/ KafkaSnapshot.cs, KafkaAlerting/ KafkaAlertEngine.cs)
src/AdminPanel.Etcd/     (Parsing/ KafkaParser.cs; KafkaSnapshotRefresher.cs,
                          KafkaSnapshotStore.cs; Writing/ KafkaWriting.cs)
src/AdminPanel.Probes/   (KafkaProbe.cs)
src/AdminPanel.Api/      (Inspection/ KafkaQuery.cs; Operations/ KafkaCommands.cs,
                          KafkaOperationsModule.cs)
frontend/src/api/        (dto.ts, queries.ts: kafka-секции)
frontend/src/pages/      (KafkaClustersPage.tsx; kafka-cluster/ …)
deploy/docker-compose.yml, docker/KafkaWorker.Dockerfile   # A13: extra_hosts + AdvertisedClientHost
dev-stand/adminpanel/    (kafka-seed.sh — compose-сервис в профиле seed; профиль kafkaworker;
                          checks/50-kafka-api.sh (сид-профиль), checks/55-kafka-e2e.sh (чистое /kafka/))
```

Порядок: Волна A (A1–A15) → граница A → Волна B (B1–B9) → граница B → Волна C (C1–C6) → граница C.

---

# ВОЛНА A — контракт и воркер (spec §7 A)

## Задача A1. arch/15-kafka-clusters.md — контракт etcd

**Spec:** §3 (весь), §3.1–3.3, §3.5.
**Вход:** spec одобрен; arch/14 (образец канона), `arch/adminpanel/02-etcd-contract.md` (образец стиля).
**Файлы:** Create `arch/15-kafka-clusters.md`; Modify `arch/README.md` (добавить строку в список).

**Действие (шаги):**
- [x] 1. Написать `arch/15-kafka-clusters.md` по структуре arch/adminpanel/02 (транспорт §1, читаемые ключи таблицей §2, формат `topics/<T>` §3 с протоколом автосинка/desired/missing — дословно из spec §3.2, координация `/kafkaworker/` §4, клиентский дискавери §5 — дословно из spec §3.5, обработка сбоев §6: битый JSON → parseError-запись; неизвестные ключи → unknownKeys; Active без `endpoints`).
- [x] 2. Включить канонические примеры значений (критерий приёмки парсеров): `config` NOT_INITIALIZED/Active/TO_REMOVE; `topics/orders` с desired и с `missing:true`; `endpoints "host.docker.internal:16001,…"`.
- [x] 3. В `arch/README.md` добавить указатель «15. Kafka-кластера (контракт etcd)».

**Выход:** канон etcd-контракта, на который ссылаются все кодовые задачи A5+.
**Проверка:** файл существует; grep `15-kafka` в `arch/README.md` находит строку; текст покрывает все ключи из spec §3.1 таблицы (сверить построчно).
**Spec-связь:** §3.1–3.3, §3.5 (контракт — источник истины для кода).

## Задача A2. arch/16-kafkaworker.md — канон воркера

**Spec:** §4.1–4.3.
**Вход:** A1; arch/14 (образец).
**Файлы:** Create `arch/16-kafkaworker.md`; Modify `arch/README.md` (добавить строку «16. KafkaWorker: оркестратор Kafka-кластеров»).

**Действие (шаги):**
- [x] 1. Перенести из spec §4 содержание: роль/разделение ответственности (панель-декларатор/воркер-исполнитель), модель размещения (образ `apache/kafka:4.0.0`, KRaft-роли/кворум `min(3,B)`, listeners CONTROLLER/INTERNAL/CLIENT, SASL/PLAIN JAAS, volume `kfw-…-data`, сеть `kfw-net`, порт-диапазон, лимиты ресурсов), контракт etcd (читаемые/пишемые — таблицы по образцу arch/14 §3.1–3.2; в очистке deprovisioning — `/kafka/clusters/<C>/` + `/kafkaworker/{claims,work,portalloc,rotations}/<C>*`), процессы A–H (K0–K6, X0–X3, надзор C, TopicSync D, Converger E, Add F, Remove G, ротация A/B/C), надёжность (идемпотентность/takeover/txn; **снапшоты P12-порта: лидер регулярно раз в 6 ч + „до/после“ в точках изменений — provisioning/deprovisioning/ротация**), конфигурация (блок appsettings из spec §4.3), риски (мин. набор: R1 образ сторонний — пин версии; R2 порт-коллизии — portalloc; R3 SASL-JAAS регенерация при рестарте; R4 RF=1 потеря тома).
- [x] 2. **Зафиксировать advertised-правило CLIENT-listener (в модель размещения):** advertised-хост ноды = `KafkaWorker:AdvertisedClientHost`, если задан; иначе — имя docker-хоста размещения (Name из `Hosts[]`/swarm-нод). Требование: значение должно резолвиться КЛИЕНТАМИ (оно попадает в `endpoints` → bootstrap клиентов). Паттерн локального docker-хоста: имя `local` в контейнере воркера нерезолвимо — лечится `extra_hosts: "local:host-gateway"` (прецедент pgworker, `deploy/docker-compose.yml`), а для клиентов на хосте/в контейнерах стендов рекомендуем `KafkaWorker__AdvertisedClientHost=host.docker.internal` (клиенты-контейнеры резолвят нативно; клиенты на хосте — через `HostMap` панели `host.docker.internal:<port>` → `localhost:<port>`; `docker run`-инструменты — `--add-host host.docker.internal:host-gateway` на Linux).
- [x] 3. Добавить канонический env-набор брокера (таблица `KAFKA_*` из §«Действие» задачи A7 — согласовать с A7 после его реализации: сначала в arch, потом код).
- [x] 4. В `arch/README.md` добавить указатель «16. KafkaWorker».

**Выход:** канон воркера для задач A5–A14 (включая снапшоты «до/после», полную etcd-очистку deprovisioning и advertised-правило).
**Проверка:** файл существует; секции «Процессы» содержат все 8 процессов (A–H) и фазы ротации A/B/C; grep `снапшот` — находит «до/после» provisioning/deprovisioning/ротации; grep `AdvertisedClientHost` — находит правило и extra_hosts-паттерн; grep `16-kafkaworker` в `arch/README.md` — находит строку.
**Spec-связь:** §4.1–4.3 (в т.ч. §4.1 advertised, §4.2 «Снапшоты», §4.2 B — очистка rotations).

## Задача A3. Слияние arch-канона с финальным кодом (зеркало)

**Spec:** arch-first правило (AGENTS.base).
**Вход:** задачи A1–A14 выполнены; выполняется ПОСЛЕ A14 и ДО закрытия границы A15 (предпоследний шаг волны A).
**Файлы:** Modify `arch/15-kafka-clusters.md`, `arch/16-kafkaworker.md`.

**Действие (шаги):**
- [ ] 1. Пройтись по реализованным процессам/ключам и синхронизировать arch-документы с фактом (расхождения — правкой arch, не кода, если код верен по spec).
- [ ] 2. Проверить: примеры значений в arch = фикстуры тестов задачи A6; advertised-правило A2 = реализация A9/A7.

**Выход:** arch = код (обязательство dev-flow).
**Проверка:** ревью-чтение; расхождений нет.
**Spec-связь:** arch-first.

## Задача A4. Roadmap-трек kafkaworker

**Spec:** §8 (выносы).
**Вход:** —.
**Файлы:** Create `arch/roadmap/kafkaworker.md`; Modify `arch/roadmap/README.md` (таблица «Треки» + строка в шапке).

**Действие (шаги):**
- [x] 1. Создать трек-файл по шаблону `arch/roadmap/pgworker.md` (заголовок, «Задачи») с шестью пунктами t01–t06 (тексты — из spec §8, сокращённо: суть + контекст-ссылки на arch/15/16).
- [x] 2. В `arch/roadmap/README.md` добавить строку трека в таблицу: `kafkaworker.md | сервис KafkaWorker: топики, ребалансировка, безопасность, метрики, дискавери-библиотека`.

**Выход:** отложенные задачи зафиксированы (merge-гейт по правилам roadmap).
**Проверка:** `grep -c "t0[1-6]-" arch/roadmap/kafkaworker.md` → 6.
**Spec-связь:** §8.

## Задача A5. Каркас проектов KafkaWorker (все 5 csproj + копии из PgWorker)

**Spec:** §4.3 (структура), §2.6 (копирование).
**Вход:** A1/A2 (канон).
**Файлы:**
- Create csproj (все пять, net10.0, без внешних пакетов; ссылки: Etcd→Core, Docker→Core, Provisioning→{Core,Etcd,Docker}, App→{Core,Etcd,Docker,Provisioning}): `src/KafkaWorker.Core/KafkaWorker.Core.csproj`, `src/KafkaWorker.Etcd/KafkaWorker.Etcd.csproj`, `src/KafkaWorker.Docker/KafkaWorker.Docker.csproj`, `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj`, `src/KafkaWorker.App/KafkaWorker.App.csproj`;
- Create код Core: `Result.cs`, `DI/` (7 файлов), `Retry/` (2 файла), `Planning/{PortAllocator,PlacementPlanner}.cs`, `Model/KafkaPasswordGenerator.cs`;
- Create код Etcd: `Client/{IEtcdGateway,EtcdGateway,Kv}.cs`, `Coordination/{ClaimStore,WorkJournal}.cs`;
- Create код Docker: `Engine/{IDockerEngine,DockerEngine}.cs`, `Drivers/{IClusterDriver,PlainClusterDriver,SwarmClusterDriver}.cs`;
- Modify `src/PgWorker.slnx` (новая `<Folder Name="/kafka/">` с пятью проектами; тест-проекты в `/tests/` — см. ниже);
- Create тест-проекты: `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj` + `ResultTests.cs` (копия-проверка каркаса), `src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`.

**Действие (шаги):**
- [ ] 1. Скопировать из `src/PgWorker.*` с заменой namespace `PgWorker.*`→`KafkaWorker.*` и удалением pg-специфики: Core → Result/DI/Retry как есть; `PortAllocator` — упростить до одного порта на ноду (диапазон из опций, без «тройки» pg); `PlacementPlanner` — как есть (анти-аффинити); `KafkaPasswordGenerator` — по образцу `AppSecretGenerator` (32 симв `[A-Za-z0-9]`).
- [ ] 2. `EtcdGateway`/`ClaimStore`/`WorkJournal` — копии (префикс `/pgworker/` → `/kafkaworker/` внутри констант координации).
- [ ] 3. `DockerEngine`/драйверы — копии с заменой `pgw-`→`kfw-` и удалением doorman/haproxy-специфики (env-генерация уходит в A7 `NodeEnvBuilder`).
- [ ] 4. Пять csproj + оба тест-проекта в `src/PgWorker.slnx` (папки `/kafka/` и `/tests/`). Проекты Provisioning/App пока пустые (кроме csproj) — наполняются с A8/A12.
- [ ] 5. Скопировать `ResultTests.cs` как smoke теста каркаса (AAA).

**Интерфейсы (Produces):** `Result`/`Result<T>`; `IEtcdGateway` (RangeAsync/TxnAsync/PutAsync/DeletePrefixAsync — как у PgWorker); `ClaimStore`(`AcquireAsync(claim, instanceId, ttl)`/`ReleaseAsync`); `WorkJournal`(`WriteAsync(cluster, op, phase, err)`); `IClusterDriver`(`EnsureNodeAsync(name, env, resources, volumes)`/`RemoveNodeAsync(name, removeVolume)`/`ListAsync(prefix)`); `KafkaPasswordGenerator.Generate()`.
**Выход:** собирающийся каркас воркера в решении (все 5 проектов в slnx).
**Проверка:** `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test src/tests/KafkaWorker.UnitTests` — зелёный.
**Spec-связь:** §4.3, §2.6.

## Задача A6. Домен + парсер `/kafka/`

**Spec:** §3.1–3.2 (ключи), arch/15.
**Вход:** A5.
**Файлы:** Create `src/KafkaWorker.Core/Model/KafkaDomain.cs`, `src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs`; Test `src/tests/KafkaWorker.UnitTests/Etcd/KafkaSnapshotParserTests.cs`, `src/tests/KafkaWorker.UnitTests/EtcdFixtures/Kafka/*.json`.

**Действие (шаги):**
- [ ] 1. Тесты-фикстуры FIRST: `.json`-файлы со строками `Kv(Key,Value)` из примеров arch/15 (Active-кластер, NOT_INITIALIZED, TO_REMOVE, topic с desired, topic missing, битый config-JSON, неизвестный ключ).
- [ ] 2. Тесты (AAA): parse полного префикса `/kafka/clusters/` → 2 кластера с конфигом/брокерами/topics; `state` отсутствует → `Active`; битый JSON → parseError-запись без исключения; unknown key → счётчик; topic desired/null/missing; brokers state-значения строкой (толерантно к новым).
- [ ] 3. Реализация:

```csharp
// KafkaWorker.Core/Model/KafkaDomain.cs — immutable records
public sealed record KafkaClusterConfig(int Brokers, int ReplicationFactor, int MinInSyncReplicas,
    int DefaultPartitions, long DefaultRetentionMs, long? CreatedUnix, string? State);
public sealed record KafkaBrokerDecl(string Name, string? State, string? Role, BrokerResources? Resources);
public sealed record BrokerResources(decimal Cpu, int MemGi, int DiskGi);   // "2", "4Gi", "40Gi"
public sealed record TopicDesired(int? Partitions, IReadOnlyDictionary<string,string>? Configs);
public sealed record KafkaTopicReg(string Topic, int Partitions, short? ReplicationFactor,
    IReadOnlyDictionary<string,string>? Configs, TopicDesired? Desired,
    long? DesiredUnix, string? DesiredBy, long? SyncedUnix, bool Missing);
public sealed record KafkaClusterSnapshot(string Cluster, KafkaClusterConfig Config,
    IReadOnlyList<KafkaBrokerDecl> Brokers, IReadOnlyList<KafkaTopicReg> Topics,
    IReadOnlyList<string> ParseErrors);
// KafkaSnapshotParser: static Result<IReadOnlyList<KafkaClusterSnapshot>> Parse(IReadOnlyList<Kv> kvs)
```

**Выход:** парсер контроль-плейна kafka (потребляют все процессы).
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter KafkaSnapshotParser` — зелёный.
**Spec-связь:** §3.1–3.2.

## Задача A7. NodeEnvBuilder — генератор env брокера

**Spec:** §4.1 (KRaft/listeners/SASL; advertised-правило arch/16).
**Вход:** A2 (канон env-таблицы + advertised-правило).
**Файлы:** Create `src/KafkaWorker.Core/Templates/NodeEnvBuilder.cs`; Test `src/tests/KafkaWorker.UnitTests/Templates/NodeEnvBuilderTests.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA): 3-ноды-кворум (роли/quorum voters), broker-only нода (voters не включает её), 1-брокерный кластер (offsets-RF=1), JAAS содержит `user_app=<pwd>` (и двухпользовательский вариант `user_app=<old>`+`user_app2=<new>` для ротации), default-конфиги из заявки попадают в env; CLIENT advertised = `<AdvertisedClientHost || имя хоста placement>:<клиентский порт>` (arch/16 A2-шаг 2).
- [ ] 2. Реализация:

```csharp
public static IReadOnlyDictionary<string,string> Build(NodeEnvSpec spec);
public sealed record NodeEnvSpec(
    int NodeId, string AdvertisedClient, bool IsController, IReadOnlyList<string> QuorumVoters,
    string AppUser, IReadOnlyList<string> AppPasswords, /* 1 или 2 для ротации */
    KafkaClusterConfig Config, int BrokerCount, string DataDir);
// фиксированные ключи: KAFKA_PROCESS_ROLES, KAFKA_NODE_ID, KAFKA_CONTROLLER_QUORUM_VOTERS,
// KAFKA_LISTENERS=CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094,
// KAFKA_ADVERTISED_LISTENERS (CLIENT — AdvertisedClient из spec: AdvertisedClientHost ?? docker-хост),
// KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
//   =CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT,
// KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER, KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL,
// KAFKA_SASL_ENABLED_MECHANISMS=PLAIN, KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG,
// KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG,
// KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=min(3,B), KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=min(3,B),
// KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=min(2,B), KAFKA_DEFAULT_REPLICATION_FACTOR=R,
// KAFKA_MIN_INSYNC_REPLICAS=M, KAFKA_NUM_PARTITIONS=P, KAFKA_LOG_RETENTION_MS=X,
// KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
```

**Выход:** детерминированная генерация env (вход для IClusterDriver.EnsureNodeAsync); advertised вычислен по канону arch/16.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter NodeEnvBuilder` — зелёный.
**Spec-связь:** §4.1 (+advertised-правило A2).

## Задача A8. Админ-клиент Kafka (seam + Confluent.Kafka)

**Spec:** §4.2 (D/E используют), §2.7 (Puzzle-паттерн seam).
**Вход:** A5 (csproj Provisioning существует).
**Файлы:** Modify `src/Directory.Packages.props` (+`<PackageVersion Include="Confluent.Kafka" Version="2.14.2" />`); Modify `src/KafkaWorker.Provisioning/KafkaWorker.Provisioning.csproj` (+PackageReference Confluent.Kafka); Create `src/KafkaWorker.Provisioning/Kafka/{IKafkaAdminClient.cs,KafkaAdminClient.cs,KafkaAdminClientFactory.cs}`; Test `src/tests/KafkaWorker.UnitTests/Provisioning/FakeKafkaAdminClient.cs` (hand-written fake).

**Действие (шаги):**
- [ ] 1. Seam-интерфейс (без Confluent-типов):

```csharp
public interface IKafkaAdminClient : IAsyncDisposable
{
    Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct); // brokers(ids,host), controllerId
    Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(CancellationToken ct); // волнам B (guard) и C (TopicSync)
    Task<Result<IReadOnlyDictionary<string, string>>> DescribeBrokerConfigsAsync(int brokerId, CancellationToken ct);
    Task<Result> AlterBrokerConfigsAsync(int brokerId, IReadOnlyDictionary<string, string> configs, CancellationToken ct);
}
public sealed record KafkaClusterView(IReadOnlyList<KafkaBrokerView> Brokers, int? ControllerId);
public sealed record KafkaBrokerView(int Id, string Host);
public sealed record KafkaTopicView(string Topic, int Partitions,
    IReadOnlyList<IReadOnlyList<int>> ReplicasPerPartition); // для guard'а RemoveBroker «на брокере нет реплик»
// Factory: IKafkaAdminClient Create(string bootstrap, string user, string password)
```

- [ ] 2. `KafkaAdminClient` — адаптер над `Confluent.Kafka.IAdminClient` (SecurityProtocol.SaslPlaintext, SaslMechanism.Plain; RequestTimeout из опций; исключения → `Result.Failed`). Волна C добавит сюда же AlterTopicConfigs/CreatePartitions (не сейчас — YAGNI).
- [ ] 3. Fake для юнит-тестов процессов (сценарии: cluster not ready → failure; brokers list; topics+replicas; configs diff).

**Выход:** изолированный Kafka-доступ (все процессы через seam), включая данные для guard'ов волны B.
**Проверка:** `dotnet build src/PgWorker.slnx` — 0 warnings (пакет подтянулся); fake компилируется в тестах.
**Spec-связь:** §4.2, §2.7.

## Задача A9. AppSecretEnsurer + ProvisioningProcess (K0–K6, снапшоты «до/после»)

**Spec:** §4.2 A, arch/16 (снапшоты P12; advertised-правило).
**Вход:** A5–A8.
**Файлы:** Create `src/KafkaWorker.Provisioning/Processes/{AppSecretEnsurer.cs,ProvisioningProcess.cs}`; Test `src/tests/KafkaWorker.UnitTests/Provisioning/{AppSecretEnsurerTests,ProvisioningProcessTests}.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA): ensure — оба ключа absent → txn put-if-absent обоих; txn проигран → re-read и использование существующих; генерация 32 симв `[A-Za-z0-9]`. Provisioning: (а) полный прогон на fake gateway/driver/adminclient — контейнеры созданы с env из NodeEnvBuilder, `endpoints` записан (**адреса — по advertised-правилу arch/16: `AdvertisedClientHost ?? docker-хост` : клиентский порт**), config перезаписан без `state` (txn по mod_revision), states PROVISIONING→RUNNING; (б) **снапшот-делегат вызван дважды — «до» (после claim) и «после» (перед journal done)**; (в) кластер не собирается (DescribeCluster failure) → journal last_error, без config-переписывания; (г) re-run при существующих контейнерах — сверка, пропуск; (д) TO_REMOVE появился посреди → процесс прекращается до config-фазы.
- [ ] 2. Реализация `ProvisioningProcess` — машина состояний одного вызова `RunAsync(KafkaClusterSnapshot, CancellationToken)` по фазам K0–K6 (arch/16): claim+journal → **снапшот P12 «до» (SnapshotJob-делегат, как в PgWorker)** → план (placement/порты через PortAllocator+PlacementPlanner; roles: `broker1..m` controller, `m=min(3,B)`, фиксация `brokers/<k>/role`) → AppSecretEnsurer → по нодам: `IClusterDriver.EnsureNodeAsync(name, env, resources, volume)` + `state=PROVISIONING` → цикл готовности `IKafkaAdminClient.DescribeClusterAsync` (бюджет `BrokerBootSec`; контроллер избран; брокеров = B) → `state=RUNNING` × B → ClusterConfigConverger-вызов (пока no-op — задача A11) → put `endpoints` (advertised host:clientPort через запятую) → txn config без `state` → **снапшот P12 «после»** → journal done.
- [ ] 3. `AppSecretEnsurer` — порт `AppSecretEnsurer` PgWorker (ключи `app_user`=`"app"`, `app_password`).

**Интерфейсы (Produces):** `Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)`; `IAppSecretEnsurer.EnsureAsync(cluster) → Result<KafkaSecrets(string User, string Password)>`; конструктор принимает `Func<CancellationToken, Task<Result>> snapshotDelegate` (порт P12).
**Выход:** кластер KRaft поднимается по заявке, снапшоты «до/после» пишутся, endpoints резолвимы клиентами.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter Provisioning` — зелёный.
**Spec-связь:** §4.2 A + §4.2 «Снапшоты»; §4.1 advertised.

## Задача A10. DeprovisioningProcess (X0–X3, снапшоты «до/после», очистка rotations)

**Spec:** §4.2 B (вкл. «+ /kafkaworker/rotations/<C>»), arch/16.
**Вход:** A9 (паттерн процесса).
**Файлы:** Create `src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs`; Test `src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA): удаление контейнеров+томов `kfw-<C>-*` (включая сироты из ListAsync); **etcd-очистка полным набором: `del --prefix /kafka/clusters/<C>/` + del `/kafkaworker/{claims,work,portalloc}/<C>*` + del `/kafkaworker/rotations/<C>` (остаточная заявка ротации не переживает удаление кластера — иначе вечный алерт kafka-rotation-pending)**; порядок docker→etcd; 404 от docker = ок; повтор — идемпотентен; снапшот-делегат вызван «до» (старт) и «после» (финал).
- [ ] 2. Реализация X0–X3 (порт DeprovisioningProcess PgWorker: claim+journal → снапшот «до» → docker-удаление → etcd-очистка (включая `/kafkaworker/rotations/<C>`) → снапшот «после» → клэйм снят явно).

**Выход:** полный демонтаж кластера со снапшотами P12 и без остаточных заявок ротации.
**Проверка:** юнит-фильтр Deprovisioning — зелёный (в т.ч. кейс «заявка ротации существовала → ключ удалён»).
**Spec-связь:** §4.2 B + §4.2 «Снапшоты»; §9.3 «+ координационные ключи».

## Задача A11. NodeSupervisor + ClusterConfigConverger

**Spec:** §4.2 C, E.
**Вход:** A9.
**Файлы:** Create `src/KafkaWorker.Provisioning/Processes/{NodeSupervisor,ClusterConfigConverger}.cs`; Test `src/tests/KafkaWorker.UnitTests/Provisioning/{NodeSupervisorTests,ClusterConfigConvergerTests}.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA) supervisor: снесённый контейнер → EnsureNodeAsync (тот же volume/env); брокер молчит > NodeDeadSec → state=UNREACHABLE + recreate; том отсутствует и RF>1 → чистый том; ноды TO_REMOVE/REMOVING/PROVISIONING не трогаются.
- [ ] 2. Тесты (AAA) converger: фактические dynamic-конфиги ≠ config.default_* → AlterBrokerConfigsAsync на всех брокерах (маппинг `default_retention_ms`→`log.retention.ms`, `default_partitions`→`num.partitions`, `replication_factor`→`default.replication.factor`, `min_insync_replicas`→`min.insync.replicas`); совпадают → no-op. Живую (Testcontainers) верификацию converge даёт B9-шаг e2e.
- [ ] 3. Реализация: `NodeSupervisor.RunAsync(snap, ct)` — сверка декларации/факта docker + AdminClient-проба; `ClusterConfigConverger.ApplyAsync(snap, ct)` — describe→decide→act (порт паттерна describe→decide→act из Puzzle §7.2).

**Выход:** самовосстановление нод и converge конфигов без рестартов.
**Проверка:** юнит-фильтры NodeSupervisor|ClusterConfigConverger — зелёные.
**Spec-связь:** §4.2 C, E.

## Задача A12. App: Options, Loops, Program, healthz

**Spec:** §4.3 (конфигурация/структура).
**Вход:** A5–A11 (csproj App существует с A5).
**Файлы:** Create `src/KafkaWorker.App/{Program.cs,Options.cs}`, `src/KafkaWorker.App/Loops/{ReconcileLoop,KeepaliveLoop,SnapshotLoop,KafkaClusterClassifier}.cs`, `src/KafkaWorker.App/HealthChecks/{KafkaWorkerHealth,ServiceProbes}.cs` (порты PgWorker.App), `src/KafkaWorker.App/appsettings.json`; Test `src/tests/KafkaWorker.UnitTests/App/KafkaClusterClassifierTests.cs`.

**Действие (шаги):**
- [ ] 1. `KafkaWorkerOptions` — дерево секций из spec §4.3 (Etcd/Docker/Loops/Thresholds/Parallelism/Snapshots/AdvertisedClientHost).
- [ ] 2. `KafkaClusterClassifier`-тесты (AAA): NOT_INITIALIZED→Provision, TO_REMOVE→Deprovision, иначе→Active(+кандидаты add/remove по стейтам брокеров — для волн B). Реализация — порт `ClusterClassifier`.
- [ ] 3. `ReconcileLoop` — тик ScanIntervalSec: range `/kafka/clusters/` → parse → классификация → процессы под клэймом (параллелизм MaxClusters; Active-ветка: supervisor → converger; add/remove/ротация/TopicSync — заглушки-расширения волн B/C). `KeepaliveLoop`/`SnapshotLoop` — порты PgWorker (leader-снапшоты 6 ч + retention; SnapshotJob собирается из `KafkaWorker.Etcd/SnapshotJob.cs` — копия PgWorker с префиксом `/kafka/`).
- [ ] 4. `Program.cs` — композиция по образцу PgWorker.App (без SecretsFromEnv — секретов per-install нет; fail-fast на пустые Etcd:Endpoints/Hosts); `appsettings.json` с дефолтами §4.3 (`AdvertisedClientHost=null` — правило arch/16).
- [ ] 5. Health: `/healthz` (etcd-reachable, docker-hosts, loops-alive, claims) — порт `PgWorkerHealth`.

**Выход:** runnable-хост воркера (`dotnet run` для локальной отладки; прод — docker).
**Проверка:** `dotnet build src/PgWorker.slnx` — 0 warnings; юнит App-тесты зелёные.
**Spec-связь:** §4.2–4.3.

## Задача A13. Dockerfile + deploy/docker-compose.yml (extra_hosts + AdvertisedClientHost)

**Spec:** §4.1 (докер-поставка, advertised), §6.
**Вход:** A12; A2-канон (advertised-правило).
**Файлы:** Create `docker/KafkaWorker.Dockerfile`; Modify `deploy/docker-compose.yml` (+service `kafkaworker`); проверить `.gitignore` (данных/томов в git попадать не должно — named volumes, ничего добавлять обычно не нужно).

**Действие (шаги):**
- [ ] 1. `docker/KafkaWorker.Dockerfile` — копия `docker/PgWorker.Dockerfile` с заменой проекта на `KafkaWorker.App` (multi-stage sdk→aspnet, curl+HEALTHCHECK `/healthz`).
- [ ] 2. В `deploy/docker-compose.yml` добавить (портативный паттерн pgworker: `extra_hosts` резолвит имя локального docker-хоста `local` в шлюз хоста; `AdvertisedClientHost=host.docker.internal` делает endpoints etcd резолвимыми для клиентов-контейнеров, клиенты на хосте — через HostMap панели):

```yaml
  kafkaworker:
    build: { context: .., dockerfile: docker/KafkaWorker.Dockerfile }
    image: kafkaworker:dev
    restart: unless-stopped
    ports: [ "8081:8080" ]          # /healthz (8081 — не конфликтует с pgworker)
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - kfw-snapshots:/snapshots
    extra_hosts:
      # docker-хост "local" (appsettings Docker:Hosts) — из контейнера резолвим его в
      # шлюз docker-хоста (прецедент pgworker, deploy/docker-compose.yml)
      - "local:host-gateway"
    environment:
      KafkaWorker__Etcd__Endpoints__0: ${KFW_ETCD_ENDPOINT:-http://localhost:2379}
      # Advertised-хост CLIENT-listener → endpoints в etcd резолвимы клиентами
      # (arch/16 advertised-правило; null допустим только когда имя docker-хоста
      # резолвимо клиентами само по себе)
      KafkaWorker__AdvertisedClientHost: ${KFW_ADVERTISED_CLIENT_HOST:-host.docker.internal}
volumes: + kfw-snapshots
```

**Выход:** воркер поставляется контейнером по правилу проекта; endpoints в etcd резолвимы.
**Проверка:** `docker build -f docker/KafkaWorker.Dockerfile .` (из корня worktree) — успешно; `grep -A2 extra_hosts deploy/docker-compose.yml` — находит `local:host-gateway` у kafkaworker; `grep AdvertisedClientHost deploy/docker-compose.yml` — находит env.
**Spec-связь:** §4.1, §6 (+arch/16 advertised-правило).

## Задача A14. Интеграционные тесты воркера (Testcontainers)

**Spec:** §4.3 (тесты), §9.2/9.3.
**Вход:** A9–A12.
**Файлы:** Create `src/tests/KafkaWorker.IntegrationTests/{Kafka/KafkaClusterFixture.cs, Kafka/ProvisioningTests.cs, Etcd/ClaimStoreTests.cs}` (по образцу `src/tests/PgWorker.IntegrationTests/Etcd/`).

**Действие (шаги):**
- [ ] 1. Fixture: Testcontainers etcd `quay.io/coreos/etcd:v3.5.21` + docker-host (локальный сокет) — воркер в тесте хост-процессом управляет docker-хостом (как PgWorker.IntegrationTests). AdvertisedClientHost fixture = `host.docker.internal` (endpoints резолвимы из тест-процесса и контейнеров). Fixture переиспользуется задачей C1.
- [ ] 2. Тест (AAA, Docker required): сеять заявку 1-брокерного кластера (config NOT_INITIALIZED + broker1/state+resources) → запустить Reconcile-проход → дождаться: контейнер `kfw-<C>-broker1` Running, ключи `endpoints`/`app_password`/state=RUNNING, config без `state` → **дискавери-проверка**: `Confluent.Kafka AdminClient` с bootstrap из `endpoints`-ключа и SASL из `app_*`-ключей успешно DescribeCluster → **положить заявку ротации `/kafkaworker/rotations/<C>` → сеять TO_REMOVE → проход** → контейнер/том удалены, префикс `/kafka/clusters/<C>/` пуст, `/kafkaworker/rotations/<C>` отсутствует (A10-очистка).
- [ ] 3. Таймауты: готовность брокера ≤ 120 с (политика ретраев в тесте).

**Выход:** e2e-доказательство волны A на реальном kafka+etcd+docker, включая очистку координационных ключей.
**Проверка:** `dotnet test src/tests/KafkaWorker.IntegrationTests` — зелёный (Docker запущен).
**Spec-связь:** §7 (граница A), §9.2–9.3.

## Задача A15. ГРАНИЦА ВОЛНЫ A

**Вход:** A1–A14 (A3 уже выполнен как предпоследний шаг волны).
**Действие:**
- [ ] 1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test src/PgWorker.slnx` — всё зелёное (включая pg-тесты — регрессов нет).
- [ ] 2. Коммит волны: `feat(kafka): волна A — контракт etcd + KafkaWorker (provision/deprovision/надзор/converge)`.

**Выход:** волна A закрыта (spec §7 A: всё, кроме add/remove/ротации/TopicSync); arch-зеркало (A3) синхронизировано до закрытия границы.
**Проверка:** команды выше; в git — коммит волны.
**Spec-связь:** §7.

---

# ВОЛНА B — панель: кластеры и мутации (spec §7 B)

## Задача B1. arch/adminpanel/02 + 03 — контракт панели kafka

**Spec:** §3.4, §5.2–5.4.
**Вход:** волна A слита.
**Файлы:** Modify `arch/adminpanel/02-etcd-contract.md` (+глава «Kafka»: читаемые ключи — ссылка на arch/15 + панельные мутации §3.4 дословно с протоколами), `arch/adminpanel/03-panels.md` (+секция kafka: таблица эндпоинтов, DTO, панели UI, алерты — из spec §5.2–5.4).

**Действие (шаги):**
- [ ] 1. 02: глава «Kafka (чтение + записи панели)»: чтение `/kafka/clusters/` + `/kafkaworker/rotations/`; 8 мутаций таблицей (метод/путь/протокол/отказы — из spec §3.4); интеракция desired/missing (ссылка arch/15 §3).
- [ ] 2. 03: эндпоинты `GET /api/kafka/clusters[...]`, мутации §3.4, DTO (`KafkaClusterDto`, `KafkaBrokerDto`, `KafkaTopicDto`, `KafkaGroupDto`, `CreateKafkaClusterRequestDto` и ответы), панели UI (список/детали/вкладки), каталог алертов §5.4.
- [ ] 3. README adminpanel-канона — без правок (структура файлов не меняется).

**Выход:** контракт панели заканонизирован до кода.
**Проверка:** grep `api/kafka` в `arch/adminpanel/03-panels.md` — находит все эндпоинты; grep `desired` в 02 — находит интеракцию.
**Spec-связь:** §3.4, §5.2–5.4 (arch-first).

## Задача B2. AdminPanel.Core: KafkaSnapshot + KafkaAlertEngine (кластерные алерты)

**Spec:** §5.1, §5.4 (кластерные kinds).
**Вход:** B1.
**Файлы:** Create `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs`, `src/AdminPanel.Core/Kafka/KafkaAlerting/{KafkaAlertEngine.cs,KafkaAlertsOptions.cs}`; Test `src/tests/AdminPanel.UnitTests/{KafkaModelTests.cs,KafkaAlertRulesTests.cs}` (по образцу `CoreModelTests`/`ShardingAlertRulesTests`).

**Действие (шаги):**
- [ ] 1. Модель (AAA-тесты: value-equality, state-маппинг):

```csharp
public sealed record KafkaSnapshot(DateTimeOffset BuiltAtUtc, bool EtcdReachable, int ConsecutiveFailures,
    IReadOnlyList<KafkaClusterInfo> Clusters, IReadOnlyList<KafkaRotationTicket> Rotations,
    IReadOnlyList<ProbeResult> Probes, IReadOnlyList<Alert> Alerts, int UnknownKeyCount);
public sealed record KafkaClusterInfo(string Name, KafkaClusterState State, // Active|NotInitialized|ToRemove
    int Brokers, int ReplicationFactor, int MinInSyncReplicas, int DefaultPartitions,
    long DefaultRetentionMs, long? CreatedUnix, string? Endpoints,
    IReadOnlyList<KafkaBrokerInfo> BrokersList, IReadOnlyList<KafkaTopicInfo> Topics);
public sealed record KafkaBrokerInfo(string Name, string? State, string? Role, decimal? Cpu, int? MemGi, int? DiskGi);
public sealed record KafkaTopicInfo(string Name, int Partitions, short? ReplicationFactor,
    long? RetentionMs, short? MinInSyncReplicas, TopicDesiredDto? Desired, bool Missing, long? SyncedUnix);
public sealed record KafkaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);
```

- [ ] 2. `KafkaAlertEngine.Evaluate(KafkaSnapshot prev, KafkaSnapshot next)` — кластерные kinds волны B: `kafka-cluster-not-initialized` (info), `kafka-cluster-to-remove` (info), `kafka-broker-not-running` (critical; fresh-PROVISIONING < 60 с — не алерт), `kafka-endpoints-missing` (critical), `kafka-rotation-pending` (info), `kafka-key-malformed` (warning). Тесты (AAA): каждый kind + `sinceUnix` по стабильному `id = kind:target` (ротационный алерт живёт только у живого кластера — A10 гарантирует очистку заявки при удалении кластера).

**Выход:** домен снапшота kafka + алерты.
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter Kafka` — зелёный.
**Spec-связь:** §5.1, §5.4.

## Задача B3. AdminPanel.Etcd: парсер + refresher + store

**Spec:** §5.1.
**Вход:** B2.
**Файлы:** Create `src/AdminPanel.Etcd/Parsing/KafkaParser.cs`, `src/AdminPanel.Etcd/{KafkaSnapshotRefresher,KafkaSnapshotStore}.cs`, Modify `src/AdminPanel.Etcd/ModuleExtensions.cs` (`AddKafka()` — HttpClient `kafka-etcd`, hosted-service, store); Test `src/tests/AdminPanel.UnitTests/{KafkaParserTests.cs,KafkaRefresherTests.cs,KafkaSnapshotStoreTests.cs}` + `src/tests/AdminPanel.UnitTests/EtcdFixtures/Kafka/*.json`.

**Действие (шаги):**
- [ ] 1. Тесты-фикстуры (значения — из arch/15 примеров): полный префикс, битые JSON, unknown-ключи, rotations.
- [ ] 2. `KafkaParser` — толерантный разбор (порт стиля `ClustersParser`; errors → ParseError-записи).
- [ ] 3. `KafkaSnapshotRefresher` — BackgroundService: тик `AdminPanel:Kafka:RefreshInterval` (3 c): range `/kafka/clusters/` + `/kafkaworker/rotations/`; failover/sticky endpoints общий через `EtcdOptions`; отказ → прежний снапшот + `EtcdReachable=false`/`ConsecutiveFailures++`. Тесты на fake `IEtcdGateway` (AAA): тик-сборка, транспортный провал роняет тик (неполный снапшот не публикуется).
- [ ] 4. `KafkaSnapshotStore` — volatile-ссылка + `Get()` (порт `SnapshotStore`).

**Выход:** снапшот kafka обновляется в фоне.
**Проверка:** юнит-фильтры Kafka — зелёные; `dotnet build` — 0 warnings.
**Spec-связь:** §5.1.

## Задача B4. Воркер: AddBroker (F) / RemoveBroker (G) / AppPasswordRotator (H, снапшоты «до/после»)

**Spec:** §4.2 F/G/H, arch/16 (снапшоты P12: ротация).
**Вход:** волна A.
**Файлы:** Create `src/KafkaWorker.Provisioning/Processes/{AddBrokerProcess,RemoveBrokerProcess,AppPasswordRotator}.cs`; Modify `src/KafkaWorker.App/Loops/ReconcileLoop.cs` (Active-ветка: scale-проход remove→add + ротация после converger); Test `src/tests/KafkaWorker.UnitTests/Provisioning/{AddBrokerProcessTests,RemoveBrokerProcessTests,AppPasswordRotatorTests}.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA) AddBroker: декларация `NOT_INITIALIZED` → порт/план → EnsureNodeAsync (role=broker; QUORUM_VOTERS не меняется) → DescribeCluster видит нового → RMW `endpoints` (добавлен адрес) → state=RUNNING; уже RUNNING → no-op.
- [ ] 2. Тесты (AAA) RemoveBroker: guards (controller → отказ; последний → отказ; партиции на брокере — через `DescribeTopicsAsync.ReplicasPerPartition` из A8 → journal-ожидание); удаление контейнера+тома → del `brokers/<b>/` prefix → RMW `endpoints` → portalloc-фильтрация; идемпотентность повтора.
- [ ] 3. Тесты (AAA) Rotator: фаза A — rolling EnsureNodeAsync с JAAS(old+new) по всем брокерам; фаза B — txn `[compare value(app_password)==OLD][put NEW; del заявку]`; фаза C — rolling c JAAS(new); отказ между фазами → journal-фаза, повтор продолжает (fake gateway); **снапшот-делегат «до» (старт ротации) и «после» (финал)**; нет заявки → no-op.
- [ ] 4. Реализация трёх процессов по канону arch/16 F/G/H; ReconcileLoop: remove-кандидаты (`TO_REMOVE`) → add-кандидаты (`NOT_INITIALIZED`) → ротация (по одному за тик, порт порядка PgWorker ClusterProcesses). Ротатор получает `snapshotDelegate` (порт P12); add/remove брокеров — без снапшотов (arch/16: точки изменений — provisioning/deprovisioning/ротация).

**Выход:** полный жизненный цикл брокеров + ротация со снапшотами P12.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter "AddBroker|RemoveBroker|AppPasswordRotator"` — зелёный.
**Spec-связь:** §4.2 F/G/H + §4.2 «Снапшоты».

## Задача B5. AdminPanel.Api: инспекция + мутации kafka

**Spec:** §3.4, §5.2.
**Вход:** B2–B4.
**Файлы:** Create `src/AdminPanel.Api/Inspection/KafkaQuery.cs` (GET `/api/kafka/clusters`, `/api/kafka/clusters/{cluster}`), `src/AdminPanel.Api/Operations/Kafka/{KafkaCommands.cs,KafkaOperationsModule.cs}` (8 команд: Create/Delete/UpdateConfig/AddBroker/RemoveBroker/RotatePassword — волна B; UpsertTopicDesired/CancelTopicDesired — объявляются, реализуются в C2), Modify `src/AdminPanel.Api/ModuleExtensions.cs` (`MapKafkaInspectionApi`/`MapKafkaOperationsApi` — вызвать в Program), `src/AdminPanel.Api/Inspection/OverviewQuery.cs` (+kafka-сводка: clustersTotal, clustersCritical), `src/AdminPanel.Api/Inspection/AlertsQuery.cs` (merge kafka-алертов); Create `src/AdminPanel.Etcd/Writing/KafkaWriting.cs` (все etcd-протоколы мутаций: claim-txn/RMW-txn/компенсации — по arch/adminpanel/02); Test `src/tests/AdminPanel.UnitTests/Kafka/{CreateKafkaClusterCommandTests,DeleteKafkaClusterCommandTests,UpdateKafkaConfigCommandTests,AddKafkaBrokerCommandTests,RemoveKafkaBrokerCommandTests,RotateKafkaPasswordCommandTests,KafkaWritingPlanTests}.cs` (по образцам `CreateClusterCommandHandlerTests`/`ClusterCreatePlanTests`).

**Действие (шаги):**
- [ ] 1. `KafkaWriting`: `BuildCreatePlan(CreateKafkaClusterRequest)` (валидация §3.4-границ: name `^[a-z][a-z0-9_]{0,62}$`, brokers 1..9 def 3, RF 1..9 ≤ brokers def 3, minISR 1..RF def 2, partitions 1..1000 def 12, retention 1..2147483647 def 604800000, cpu 0.01..64 / mem 1..65536 / disk 1..65536 Gi def 2/2/20 — тесты таблицей, AAA) + ключи-набор; RMW-протоколы (state=TO_REMOVE; config-update compare mod_revision; claim-txn broker; rotations claim-txn).
- [ ] 2. Команды (CQRS `ICommand`/`ICommandHandler`): каждая — валидация → чтение config напрямую (не из снапшота) → guard'ы 404/409 → запись через `KafkaWriting`; ответы 201/204 + ProblemDetails (порт текстов отказов из pg-команд).
- [ ] 3. `KafkaQuery` — маппинг `KafkaSnapshot` → DTO (02/03-контракт); ротации в детали кластера.
- [ ] 4. Overview/Alerts — merge (kafka-алерты в общий список, kind уже `kafka-*`).

**Выход:** API панели kafka полностью (кроме topic-desired — волна C).
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter Kafka` — зелёный; `dotnet build` — 0 warnings.
**Spec-связь:** §3.4, §5.2.

## Задача B6. Проба Kafka (DescribeCluster) в AdminPanel.Probes

**Spec:** §5.1 (проба).
**Вход:** B3; Confluent.Kafka уже в CPM (A8).
**Файлы:** Modify `src/AdminPanel.Probes/AdminPanel.Probes.csproj` (+PackageReference), `src/AdminPanel.Probes/ModuleExtensions.cs`; Create `src/AdminPanel.Probes/Kafka/{KafkaProbe.cs,KafkaProbeOptions.cs}` (+ wired: отдельный тик 15 c, пишет в KafkaSnapshotStore); Test `src/tests/AdminPanel.UnitTests/ProbesKafka/{KafkaProbeTests.cs}` (fake AdminClient seam — вынести `IKafkaProbeClient` по образцу проб pg).

**Действие (шаги):**
- [ ] 1. `KafkaProbeOptions`: `Enabled=true`, `Interval=15 c`, `Timeout=3 c`; HostMap-резолюция endpoints (переиспользовать `HostMapResolver`; на стенде `host.docker.internal:<port>` → `localhost:<port>` — симметрия advertised-паттерна A2/A13).
- [ ] 2. Проба per-кластер: bootstrap из `endpoints`, SASL из `app_user`/`app_password` (панель читает из etcd — добавить чтение в `KafkaSnapshotRefresher`: поля в `KafkaClusterInfo` НЕ попадают, отдельный internal-словарь стора); результат: brokers(id/host/controller/live) + error → `ProbeResult` (kind `kafka`).
- [ ] 3. Тесты (AAA): маппинг адресов HostMap; ошибка → ProbeResult.Error, etcd-часть жива; пароль в ProbeResult отсутствует.

**Выход:** live-данные брокеров в панели.
**Проверка:** юнит — зелёный; сборка 0 warnings.
**Spec-связь:** §5.1.

## Задача B7. Frontend: раздел Kafka (кластеры)

**Spec:** §5.3.
**Вход:** B5 (API жив).
**Файлы:** Modify `frontend/src/api/dto.ts` (+Kafka*Dto), `frontend/src/api/queries.ts` (+`kafkaClusters`, `kafkaCluster(name)`, мутации), `frontend/src/App.tsx` (маршруты `/kafka`, `/kafka/:cluster`), `frontend/src/layout/AppLayout.tsx` (nav «Kafka»), `frontend/src/pages/OverviewPage.tsx` (карточка kafka); Create `frontend/src/pages/KafkaClustersPage.tsx`, `frontend/src/pages/kafka-cluster/{KafkaClusterDetailsPage.tsx,BrokersTab.tsx,CreateKafkaClusterModal.tsx,AddBrokerModal.tsx,EditClusterConfigModal.tsx,RemoveBrokerButton.tsx,RotatePasswordButton.tsx,DeleteKafkaClusterButton.tsx}`.

**Действие (шаги):**
- [ ] 1. dto/queries по arch/adminpanel/03 (camelCase-зеркало DTO B5).
- [ ] 2. Список кластеров (Mantine Table: name, state-бейдж, brokers running/total, topics count, endpoints; кнопка «Создать кластер» → модал полей §3.4 с дефолтами 3/3/2/12/7д — клиентская валидация-зеркало).
- [ ] 3. Детали: шапка (бейджи TO_REMOVE/NOT_INITIALIZED, кнопки «Изменить параметры»/«Сменить app-пароль»/«Удалить кластер» — подтверждения с предупреждением ротации) + вкладка Брокеры (name/state/role/resources/host + «Убрать брокера» с guard-дизейблами controller/последний/непустой + «Добавить брокера»).
- [ ] 4. Общие компоненты переиспользовать (PollingToggle, StaleBadge, ProblemDetails-обработка client.ts); 401→login как везде.

**Выход:** UI раздела kafka (без вкладок Топики/Группы — заглушки «волна C»).
**Проверка:** `cd frontend && npm run build` — без ошибок; ручной смоук против стенда B8.
**Spec-связь:** §5.3.

## Задача B8. Стенд: kafka-сид (профиль seed) + профиль kafkaworker + чек API

**Spec:** §6 («ключи 1–2 кластеров»); изоляция сида от живого воркера.
**Вход:** B5–B7.
**Файлы:** Create `dev-stand/adminpanel/kafka-seed.sh`, `dev-stand/adminpanel/checks/50-kafka-api.sh`; Modify `dev-stand/adminpanel/docker-compose.yml` (+сервис `kafka-seed` **с `profiles: ["seed"]`**; +профиль `kafka`: сервис `kafkaworker` build `../../docker/KafkaWorker.Dockerfile`, docker.sock, `extra_hosts: ["local:host-gateway"]`, env `KafkaWorker__Etcd__Endpoints__0=http://etcd:2379` + `KafkaWorker__AdvertisedClientHost=host.docker.internal`), `dev-stand/adminpanel/README.md` (профили/чеки/предупреждение).

**Действие (шаги):**
- [ ] 1. `kafka-seed.sh` — РОВНО 2 кластера (идемпотентный, по образцу seed.sh; guard «config существует → пропуск»):
  - `events` — Active: 3 брокера RUNNING (roles: broker1..3 controller), `endpoints`, `resources`; topics: `orders` (без desired), `payments` (с desired partitions↑), `ghost` (`missing:true` + desired); ротационная заявка `/kafkaworker/rotations/events` (чистится только исполнением/удалением кластера — A10; events в чеке не удаляется);
  - `pending` — NOT_INITIALIZED: config с state + 3 брокера `NOT_INITIALIZED` + resources.
  - Состояние TO_REMOVE сид НЕ сеет: его создаёт сам чек шагом DELETE (пост-проверка бейджа) — так 2 кластера покрывают все три состояния.
- [ ] 2. Compose-развязка сида и воркера (quick-сервисы стенда поднимаются во всех профилях — сид обязан быть опциональным):
  - сервис `kafka-seed`: `profiles: ["seed"]`, depends_on etcd — НЕ поднимается дефолтным `docker compose up -d` и НЕ поднимается в профиле `kafka`;
  - сервис `kafkaworker`: `profiles: ["kafka"]` — поднимается только явно;
  - README: «сид — `docker compose --profile seed up -d` (или `run --rm kafka-seed`); живой воркер — `--profile kafka`; НЕ смешивать `--profile seed` с `--profile kafka`: сид выглядит для воркера как заявки (pending → provisioning, events-RUNNING без контейнеров → supervisor-пересоздания, заявка ротации → journal-fail); e2e-гейты (B9/C5) идут на чистом `/kafka/`».
- [ ] 3. `50-kafka-api.sh` (сид — часть чека, панель — хост-процесс): первый шаг сам активирует сид `docker compose --profile seed run --rm kafka-seed` (идемпотентен) → login → GET /api/kafka/clusters (**2 кластера**: events Active, pending NOT_INITIALIZED) → GET детали events (3 брокера/topics/ротация) → POST создать `events` → 409 → PUT config events (retention) → 204 → POST brokers events (resources) → 201 (сгенерировано имя broker4) → DELETE brokers/broker4 → 204 (только что заявленный пустой) → DELETE brokers/broker1 → 409 (controller-пред-проверка) → DELETE cluster pending → 204 → GET: pending с бейджем TO_REMOVE. ProblemDetails-коды фиксируются. Сид контейнеров не поднимает и воркер в чеке не запущен. Guard «на брокере есть партиции»: серверная пред-проверка панели его не делает (фактических реплик в etcd нет — только live-данные знают); guard реализован в воркере (B4, юнит-тесты через `DescribeTopicsAsync.ReplicasPerPartition` → journal-ожидание занятого брокера); живая негативная e2e-проверка сознательно НЕ ставится — размещение реплик недетерминировано (flaky-чек); демонтаж непустого брокера останется заблокирован guard'ом до roadmap `t02-kafka-reassignment`.

**Выход:** стенд панели покрывает kafka-домен в границах spec §6 («1–2 кластеров»); сид изолирован от живого воркера (профиль seed) — 50-чек исполняем, B9/C5 не зашумлены.
**Проверка:** `cd dev-stand/adminpanel && docker compose up -d && docker compose --profile seed run --rm kafka-seed && ./checks/50-kafka-api.sh` — зелёный (панель запущена); `docker compose config --profiles` содержит `seed` и `kafka`; `docker compose up -d` (без профилей) НЕ создаёт kafka-seed/kafkaworker.
**Spec-связь:** §6, §7 B; изоляция — требование стабильности e2e B9/C5.

## Задача B9. ГРАНИЦА ВОЛНЫ B (+e2e: брокеры/ротация/converge-верификация на чистом /kafka/)

**Вход:** B1–B8.
**Действие:**
- [ ] 1. Подготовка стенда (чистый префикс, без сида): `./checks/90-down.sh -v` → `docker compose up -d --profile kafka` (etcd чистый; kafka-seed НЕ поднимается — профиль seed не активен; pg-сид стенда воркеру не мешает: он читает только `/kafka/`; контроль чистоты: `etcdctl get /kafka/ --prefix --keys-only` пусто).
- [ ] 2. e2e (живой воркер, креды/адреса — только из etcd): поднять панель хост-процессом → POST /api/kafka/clusters (1 брокер — скорость) → дождаться RUNNING+endpoints → **converge-верификация (spec §9.3): зафиксировать `docker inspect kfw-<C>-broker1` (Id, StartedAt) → PUT /api/kafka/clusters/{c}/config (изменить retention) → дождаться применения (поллинг `docker run --rm --add-host host.docker.internal:host-gateway apache/kafka:4.0.0 kafka-configs --bootstrap-server <endpoints из etcd> --command-config <sasl-props из app_* из etcd> --entity-type brokers --entity-name 1 --describe --all | grep log.retention.ms` = новое значение; таймаут 60 с) → снова `docker inspect`: Id и StartedAt НЕИЗМЕННЫ — рестартов/пересозданий не было** → POST brokers (add broker2, broker-only) → DELETE brokers/broker2 (remove пустого) → rotate (фазы A/B/C — проверка: старый пароль отвергнут после C, новый работает) → **снова поставить заявку ротации** → DELETE cluster → префикс `/kafka/clusters/<C>/` пуст И `/kafkaworker/*/<C>*`-ключи отсутствуют (вкл. `/kafkaworker/rotations/<C>` — A10-очистка живьём).
- [ ] 3. `dotnet build`/`dotnet test` — зелёные; npm build — ок.
- [ ] 4. Коммит: `feat(kafka): волна B — панель (кластеры/брокеры/ротация/converge-e2e) + стенд`.

**Выход:** волна B закрыта (spec §7 B); критерий §9.3 «конфиг-мутация применяется без рестартов» верифицирован e2e на чистых данных.
**Проверка:** e2e-сценарий (вкл. converge-шаг с проверкой Id/StartedAt и очистку rotations при удалении) прошёл; тесты зелёные.
**Spec-связь:** §7, §9.3.

---

# ВОЛНА C — топики, группы, лаги (spec §7 C)

## Задача C1. TopicSyncProcess (автосинк + desired-converge) + интеграционный тест

**Spec:** §3.2, §4.2 D, §4.3 («TopicSync против реального Kafka» в интеграционных).
**Вход:** волна B; A8 (DescribeTopicsAsync).
**Файлы:** Modify `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs` (+`AlterTopicConfigsAsync(topic, configs)`, `CreatePartitionsAsync(topic, totalPartitions)`; fake — дополнить); Create `src/KafkaWorker.Provisioning/Processes/TopicSyncProcess.cs` (+ decision-функции `TopicSyncDecision` отдельным файлом); Modify `ReconcileLoop` (Active-ветка: TopicSync по `TopicSyncIntervalSec`); Test `src/tests/KafkaWorker.UnitTests/Provisioning/{TopicSyncDecisionTests,TopicSyncProcessTests}.cs`; Test (интеграционный) `src/tests/KafkaWorker.IntegrationTests/Kafka/TopicSyncTests.cs` (fixture A14).

**Действие (шаги):**
- [ ] 1. Чистые decision-функции (AAA-таблицей): `(факт-набор, etcd-набор) → план` — новый факт-топик → put; исчез без desired → del; исчез с desired → put(missing=true); desired отличается (retention/minISR/partitions↑) → apply+снять; desired partitions ≤ факт → перманентный отказ журнала (панель отсекала, etcd-мусор обходится); `__`-топики — пропуск.
- [ ] 2. Тесты процесса (fake gateway/adminclient): txn RMW по mod_revision (панель переписала desired между read/write → re-read, без порчи); apply-порядок (конфиги до partitions); идемпотентность повтора.
- [ ] 3. Реализация: тик только под клэймом `<C>`; describe→decide→act; Polly jitter поверх оркестрации (порт Puzzle §7.4).
- [ ] 4. Интеграционный тест (AAA, Docker required; fixture A14 — etcd + поднятый 1-брокерный кластер): создать топик AdminClient'ом → прогон TopicSync → ключ `topics/<t>` в etcd с фактом; положить desired (retention) → прогон → `kafka-configs --describe`/DescribeTopicConfigs = новое значение, desired снят; положить desired partitions↑ → прогон → partitions вырос; удалить топик: без desired → ключ удалён; с desired → `missing=true`; после отмены desired (etcd) → ключ удалён.

**Выход:** реестр topics = факт; desired применяется и снимается (верифицировано на реальном Kafka).
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSync` и `dotnet test src/tests/KafkaWorker.IntegrationTests --filter TopicSync` — зелёные.
**Spec-связь:** §3.2, §4.2 D, §4.3.

## Задача C2. Панель: desired-мутации API

**Spec:** §3.4 (последние две мутации).
**Вход:** C1 (воркер понимает desired), B5 (каркас команд).
**Файлы:** Modify `src/AdminPanel.Api/Operations/Kafka/{KafkaCommands.cs,KafkaOperationsModule.cs}` (+`UpsertTopicDesiredCommand`: PUT `/api/kafka/clusters/{c}/topics/{t}` — тело `{partitions?, retentionMs?, minInSyncReplicas?}`, 404 кластер/топик/missing, partitions ≤ факт → 400, RMW-txn; +`CancelTopicDesiredCommand`: DELETE `.../topics/{t}/desired` — desired=null, 404 если заявки нет); Modify `src/AdminPanel.Etcd/Writing/KafkaWriting.cs` (+topic RMW); Test `src/tests/AdminPanel.UnitTests/Kafka/{UpsertTopicDesiredCommandTests,CancelTopicDesiredCommandTests}.cs`.

**Действие (шаги):**
- [ ] 1. Тесты (AAA): валидация полей (хотя бы одно; partitions > фактического; имя топика Kafka-паттерн `^[a-zA-Z0-9._-]{1,249}$` без `__`-префикса → 400/404-грань); RMW-txn проигрыш → 503/retry-семантика; отмена отсутствующей заявки → 404.
- [ ] 2. Реализация команд + эндпоинты (в модуль B5 — добавить две записи).

**Выход:** конфиг-заявки топиков из панели.
**Проверка:** юнит-фильтры TopicDesired — зелёные.
**Spec-связь:** §3.2, §3.4.

## Задача C3. Проба: топики + группы + лаги

**Spec:** §5.1 (проба C-уровня), §5.4 (probe-алерты).
**Вход:** B6.
**Файлы:** Modify `src/AdminPanel.Probes/Kafka/{KafkaProbe.cs,+KafkaGroupLag.cs}` (+`IKafkaProbeClient`: ListGroups/DescribeGroups/ListOffsets/endOffsets → totalLag); Test `src/tests/AdminPanel.UnitTests/ProbesKafka/{KafkaGroupLagTests.cs,KafkaProbeTopicsTests.cs}`.

**Действие (шаги):**
- [ ] 1. `KafkaGroupLag` — чистая функция (AAA): `(endOffsets, committed) → totalLag` (сумма по партициям, отсутствие коммита = весь лаг).
- [ ] 2. Проба: describeTopics (partitions/RF/under-replicated по ISR), группы (state/members/totalLag); enrichment в store (topics/groups в `KafkaClusterInfo` runtime-поля — `KafkaTopicRuntime`, `KafkaGroupInfo(Group, State, Members, TotalLag)`).
- [ ] 3. AlertEngine-дополнение (тесты AAA): `kafka-topic-under-replicated` (warning), `kafka-group-lag-high` (warning, порог `AdminPanel:KafkaAlerts:GroupLagMessages=100000`), `kafka-topic-missing-desired`, `kafka-desired-stale` (600 c) — последние два по etcd-данным (модель B2 уже несёт поля).

**Выход:** лаги и живое состояние топиков в панели.
**Проверка:** юнит-фильтры — зелёные.
**Spec-связь:** §5.1, §5.4.

## Задача C4. Frontend: вкладки Топики и Группы

**Spec:** §5.3 (вкладки).
**Вход:** C2–C3.
**Файлы:** Create `frontend/src/pages/kafka-cluster/{TopicsTab.tsx,GroupsTab.tsx,TopicDesiredModal.tsx}`; Modify `KafkaClusterDetailsPage.tsx` (включить вкладки), `dto.ts`/`queries.ts` (+desired-мутации, groups).

**Действие (шаги):**
- [ ] 1. Топики: таблица (name/partitions/RF/retention/minISR/desired-бейдж с возрастом/missing-подсветка); per-row «Изменить конфиги» (модал partitions↑/retention/minISR) и «Отменить заявку»; подпись «состав топиков управляется на стороне Kafka (CLI/клиенты) — панель синхронизирует реестр из etcd».
- [ ] 2. Группы: таблица (group/state/members/totalLag, сортировка по лагу); fallback «проба отключена/недоступна».

**Выход:** UI полный по spec §5.3.
**Проверка:** `npm run build` — ок; ручной смоук на стенде.
**Spec-связь:** §5.3.

## Задача C5. Чек 55-kafka-e2e.sh (полный цикл на чистом /kafka/)

**Spec:** §6, §9.5 (в т.ч. missing-ветка).
**Вход:** C1–C4, B8-стенд (профили seed/kafka разведены).
**Файлы:** Create `dev-stand/adminpanel/checks/55-kafka-e2e.sh`; Modify `dev-stand/adminpanel/README.md` (e2e-порядок).

**Действие (шаги):**
- [ ] 1. Скрипт (чистое состояние, БЕЗ сида). Цепочка шагов (каждый дожидается устойчивого состояния перед следующим):
  1. `./checks/90-down.sh -v` → `docker compose up -d --profile kafka` (kafka-seed не поднимается — профиль seed не активен; контроль: `etcdctl get /kafka/ --prefix --keys-only` пусто);
  2. создать кластер (3 брокера) через API → ждать RUNNING+endpoints;
  3. создать топик `docker run --rm --add-host host.docker.internal:host-gateway apache/kafka:4.0.0 kafka-topics --create` (адреса/креды — ТОЛЬКО чтением ключей etcd через etcdctl) → ждать автосинка: ключ `topics/e2e` в etcd ≤ 2 тиков;
  4. **desired-применение**: PUT desired (partitions↑+retention) → ждать: describe показывает новые значения, desired снят (ключ без desired);
  5. **негатив**: PUT desired на уменьшение partitions → 400 (заявка НЕ пишется — desired в ключе отсутствует);
  6. **группа+лаг** (ДО missing-ветки — консьюмер читает живой топик e2e): произвести несколько сообщений в e2e → kafka-console-consumer --group lag-test (few messages; адреса/креды по ключам etcd) → GET детали кластера: группа lag-test с totalLag>0 видна;
  7. **missing-ветка** (desired обязан СТОЯТЬ на момент удаления топика — иначе автосинк просто удалит ключ): PUT валидной заявки (retentionMs) → 204 → убедиться: desired виден в значении ключа `topics/e2e` (etcdctl) → CLI-удаление топика e2e → ждать: `missing=true` + алерт `kafka-topic-missing-desired` (GET /api/alerts) → отмена: DELETE `…/topics/e2e/desired` → 204 → ждать: автосинк удалил ключ (топика нет, desired снят — etcdctl подтверждает отсутствие `topics/e2e`);
  8. **демонтаж брокера**: при B=3 все broker1..3 — controller (m=min(3,3)) — удалять их нельзя; сценарий: POST /brokers → broker4 (broker-only) → ждать RUNNING → DELETE brokers/broker1 → 409 (controller-guard, негативная проверка) → DELETE brokers/broker4 → 204 (пустой broker-only);
  9. TO_REMOVE кластера → префикс `/kafka/clusters/<C>/` пуст.
  (Converge-шаг «без рестартов» уже верифицирован в B9 — здесь не дублируется.)
- [ ] 2. README: строка e2e-прогона (55-й требует чистого состояния — как 30/40 pg-чеков; сид-профиль не поднимать).

**Выход:** автоматизированный критерий §9.5, включая достижимую missing-ветку (валидная заявка перед CLI-удалением) и согласование с KRaft-ролями (controller-ноды не демонтируются).
**Проверка:** `./checks/55-kafka-e2e.sh` с чистого состояния — зелёный (все 9 подшагов).
**Spec-связь:** §6, §9.5; §3.2 (missing-протокол), §4.1 (роли), §4.2 G (guards).

## Задача C6. ГРАНИЦА ВОЛНЫ C (финал)

**Вход:** C1–C5 (+A3 arch-зеркало актуально).
**Действие:**
- [ ] 1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test src/PgWorker.slnx` — всё зелёное; `cd frontend && npm run build` — ок.
- [ ] 2. Полный прогон стенда: quick-чеки pg (10/20) + 50-kafka (с сид-профилем) + 55-kafka (чистое состояние, профиль kafka) — зелёные.
- [ ] 3. Пройтись по критериям приёмки spec §9.1–9.8 построчно (чек-лист в PR-описании).
- [ ] 4. Коммит: `feat(kafka): волна C — топики/группы/лаги + e2e`.

**Выход:** spec реализована полностью.
**Проверка:** все команды зелёные; чек-лист §9 закрыт.
**Spec-связь:** §7, §9.

---

## Self-review плана

1. **Покрытие spec:** §3.1–3.3 → A1/A6; §3.2-протокол → C1/C2 (+интеграционный C1-шаг 4 — требование §4.3 «TopicSync против реального Kafka»); §3.4 → B5/C2 (все 8 мутаций); §3.5 → A1+A14 (дискавери-тест); §4.1 → A7/A13 (+роли controller/broker-only уважены в C5-сценарии демонтажа; advertised-правило — A2-канон, A7/A9-реализация, A13/B8-поставка с `extra_hosts`+`AdvertisedClientHost`); §4.2 A–H → A9/A10/A11/B4/C1 (+снапшоты P12 «до/после» provisioning/deprovisioning/ротации — A2-канон, A9/A10/B4; deprovisioning-очистка вкл. `/kafkaworker/rotations/<C>` — A2/A10, верифицировано A14-шагом 2 и B9-финалом); §4.3 → A5/A12/A8; §5.1 → B2/B3/B6/C3; §5.2 → B5; §5.3 → B7/C4; §5.4 → B2/C3; §6 → B8 (сид ровно 2 кластера; изолирован профилем `seed` от дефолтного набора и профиля `kafka`)/C5/A13; §7-границы → A15/B9/C6; §8 → A4; §9 → распределено по проверкам задач; §9.3-конфиг-мутация «без рестартов» → e2e-шаг B9 (kafka-configs --describe + Id/StartedAt неизменны); **§9.5-missing-ветка** → C5-подшаг 7: валидная заявка ставится ПЕРЕД CLI-удалением (после негативного 400 desired в ключе нет — без перевалидной заявки ветка недостижима) → missing=true + алерт + отмена + исчезновение ключа; группа+лаг (подшаг 6) — до missing-ветки, по живому топику e2e.
2. **Placeholder-скан:** TBD/TODO/«реализовать потом» — нет; каждая кодовая задача имеет файлы, интерфейсы и тест-критерии; тела методов не приводятся там, где источник — копия PgWorker-файла или arch-канон (указан точно).
3. **Консистентность типов и сценариев:** `KafkaClusterSnapshot`(воркер, A6) ≠ `KafkaSnapshot`(панель, B2) — осознанно; `TopicDesired`(воркер) и `TopicDesiredDto`(панель) — разделены; `IKafkaAdminClient` (A8, с DescribeTopicsAsync сразу) расширяется только в C1 — одна точка; `KafkaWriting` — единый писатель etcd панели; csproj-набор A5 (5 проектов) согласован с A8/A12; arch/README правится A1 (строка 15) и A2 (строка 16) — карта отражает оба; демонтаж брокеров в B8/B9/C5 идёт только через добавленного broker-only; guard «на брокере есть партиции» покрыт юнит-тестами B4 (воркер; серверно в B5 панель его не проверяет — нет фактических реплик в etcd), живая негативная e2e — roadmap `t02-kafka-reassignment` (недетерминированное размещение реплик → flaky); **профили стенда разведены**: kafka-seed — `profiles:["seed"]` (50-чек активирует его сам первым шагом — чек исполняем), kafkaworker — `profiles:["kafka"]` с `extra_hosts local:host-gateway` + `AdvertisedClientHost=host.docker.internal`; B9/C5 идут по рецепту `90-down -v → up -d --profile kafka` с контролем «`/kafka/` пуст»; advertised-цепочка согласована сквозно: A2 (правило) → A7/A9 (env/endpoints) → A13/B8 (compose env) → B6 (HostMap панели) → B9/C5 (`--add-host` в docker run-инструментах); missing-ветка C5 согласована с C1-интеграционным тестом (там desired кладётся в etcd напрямую — ветка достижима по построению) и с B8-сидом (`ghost` сеет missing статически).
