# Спецификация: t10 — интеграция Kafka-клиента Puzzle с библиотекой дискавери HA.Kafka

> Dev-flow, фаза спецификации. Worktree:
> `feat-t10-kafka-discovery-integration`. Дата: 2026-09-02. Задача roadmap:
> [`arch/roadmap/kafkaworker.md`](../../../arch/roadmap/kafkaworker.md)
> `t10-kafka-discovery-integration`.

## 1. Цель

Подключить Confluent-клиент приложения (`PuzzleServer.Infrastructure.App.Kafka`)
к библиотеке дискавери `PuzzleServer.Infrastructure.App.HA.Kafka` (t05, уже в
main) — в HA-режиме параметры подключения приходят из etcd-снапшота, а не из
`ConnectionStrings:Kafka`:

1. **Источник параметров** — `bootstrap.servers` + SASL/PLAIN-креды из
   снапшота дискавери (контракт
   [`arch/15-kafka-clusters.md`](../../../arch/15-kafka-clusters.md) §5:
   `endpoints`, `app_user`/`app_password`), вместо `ConnectionStrings:Kafka`.
   Сегодня `Infrastructure.App.Kafka` SASL не поддерживает вовсе — поддержка
   добавляется этой задачей.
2. **Реакция на изменения** — событие `Updated` стора дискавери (смена
   endpoints/кредов, вкл. ротацию `app_password` — сценарий
   [`arch/16-kafkaworker.md`](../../../arch/16-kafkaworker.md) §5 H, фаза B)
   переподключает продюсеров и консюмеров без рестарта процесса и без потери
   сообщений (at-least-once).
3. **Aspire-ветка без etcd** — локальная разработка: источник параметров —
   конфигурация (`ConnectionStrings:Kafka`), как сегодня. Переключатель —
   существующий `Database:Source` (см. решения §2).

Код — в репозитории **Puzzle** (feature-ветка `feat-t10-kafka-discovery-integration`
от main, по образцу t05); spec/plan/roadmap-артефакты — в этом worktree pg.
**Контракт etcd (arch/15 §5–§6) не меняется**: клиент — читатель, всё
необходимое уже в контракте (подтверждено user-review t05); каталог `arch/`
репозитория pg этой задачей не правится. Мерж-гейт (правила
[`arch/roadmap/README.md`](../../../arch/roadmap/README.md)): пункт
`t10-kafka-discovery-integration` удаляется из
`arch/roadmap/kafkaworker.md` тем же мерж-коммитом в `main` pg.

## 2. Принципы (решения user-review выделены)

1. **Один переключатель на домены БД и Kafka (решение user-review).**
   Существующая секция `Database:Source` (`Aspire` | `HaDb`,
   `DatabaseSourceReader` в `PuzzleServer.Infrastructure.App/DB/`) управляет и
   HA.Db, и источником Kafka-параметров. Отдельной секции `Kafka:Source` нет:
   `Aspire` — оба домена из конфигурации; `HaDb` — оба из etcd. Нераспознанное
   значение — уже существующий fail-fast ридера.
2. **Шов-провайдер в App.Kafka (решение user-review).** В
   `Infrastructure.App.Kafka` вводится интерфейс `IKafkaConnectionProvider`
   (текущие соединительные параметры + канал `OnChange`). Две реализации:
   конфигурационная (адаптер над `IOptionsMonitor<KafkaOptions>`, Aspire) и
   дискавери (над `IKafkaDiscoveryStore`, HaDb). Существующие механики
   hot-reload переиспользуются: продюсер — инвалидация кэша, консюмер —
   self-restart через `IKafkaConfigChangeSource`. Публичный API
   `Infrastructure.App.Kafka` (`IKafkaProducerBuilder`/`IKafkaConsumerBuilder`/
   `IKafkaTopicAdmin`/`IKafkaProducer`/`IKafkaConsumer`) не меняется.
3. **Направление зависимости: интеграция → библиотека.** Реализация
   дискавери-провайдера живёт в `Infrastructure.App.Kafka`; проект получает
   ProjectReference на `HA.Kafka`. Сама `HA.Kafka` остаётся чистой (без
   Confluent.Kafka, без knowledge о клиенте) — принцип t05 не нарушается.
4. **Fail-open ожидание (решение user-review).** Отсутствие валидного
   клиентского конфига (снапшот не собран, кластер ещё `NOT_INITIALIZED`, нет
   `endpoints`/кредов, etcd недоступен) не роняет старт приложения: продюсер
   возвращает `Result.Failed` на `SendAsync`, консюмер откладывает построение
   Confluent-клиента до появления параметров. Доступность восстанавливается
   сама, когда воркер допишет ключи (событие `Updated`).
5. **Событие фильтруется по соединительным параметрам.** `OnChange`
   провайдера стреляет только при фактическом изменении вычисленных параметров
   (bootstrap/креды/протокол; value-equality). Изменение только реестра
   топиков или `synced_unix` не роняет клиентские соединения. Это же
   гарантирует, что клиентские соединения не «моргают» от шума фонового
   автосинка реестра (arch/15 §3).
6. **Имя кластера — секция `Kafka:Cluster` (решение user-review).**
   HaDb-режим: `AddKafka` читает `Kafka:Cluster` (например `"events"` — имя
   кластера dev-стенда pg) и регистрирует
   `AddHaKafka(...).AddKafkaCluster(<имя>)`. Пустое значение — fail-fast при
   старте. Формат имени валидирует `AddKafkaCluster` (t05).
7. **Aspire-режим — полная не-регрессия.** Поведение, конфигурация и
   публичный API в Aspire-ветке эквивалентны сегодняшним: `BootstrapServers`
   из `ConnectionStrings:Kafka` (кладёт Aspire `AddKafka`), PLAINTEXT без
   кредов, hot-reload по `IOptionsMonitor`-нотификациям. SASL-поля в
   `KafkaOptions` НЕ добавляются (Aspire-брокер без SASL; YAGNI — TLS/SCRAM
   появятся с t03-kafka-security в pg).
8. **Секреты не светятся.** `SaslPassword` редацируется в `ToString()`
   моделей и не попадает в логи (паттерн `KafkaAppSecret`/`KafkaClientConfig`
   из t05; фиксируется тестами).
9. **Стиль Puzzle.** .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`
   (0 warnings — критерий); `Result`-монада, никаких `throw` через границы
   модуля; attribute-driven DI; file-scoped namespaces; идентификаторы
   по-английски, комментарии/документация по-русски; версии пакетов —
   централизованно в `src/Directory.Packages.props` (новых внешних пакетов
   нет); тесты — AAA-комментарии.

## 3. Что меняется (структура/компоненты)

### 3.1. Шов `IKafkaConnectionProvider` (новое в `Infrastructure.App.Kafka`)

```csharp
// Соединительные параметры Kafka-клиента (всё, что нужно Confluent-конфигам).
// SASL-поля опциональны: null → в Confluent-конфиг не задаются (дефолт
// PLAINTEXT — Aspire-ветка); полный набор — HaDb-ветка из контракта §5.
public sealed record KafkaConnectionParams(
    string BootstrapServers,
    string? SecurityProtocol,   // "SASL_PLAINTEXT" (контракт §5 п.2)
    string? SaslMechanism,      // "PLAIN"
    string? SaslUsername,
    string? SaslPassword)
{
    public override string ToString();  // SaslPassword = *** (редакция)
}

public interface IKafkaConnectionProvider
{
    // Текущие параметры; null = валидного конфига пока нет (fail-open,
    // см. принцип 4). Читается на каждый Build() — без кеширования у потребителя.
    KafkaConnectionParams? Current { get; }

    // Стреляет ТОЛЬКО при фактическом изменении Current (value-equality),
    // включая переходы null→params и params→null. Потокобезопасно;
    // исключения подписчика гасятся логом (паттерн Updated у t05).
    IDisposable OnChange(Action handler);
}
```

Маппинг параметров в Confluent-конфиги — чистые функции (unit-тестируемые без
брокера): `BootstrapServers` всегда; при наличии `SaslUsername`/`SaslPassword`
— `SecurityProtocol=SaslPlaintext`, `SaslMechanism=Plain`, креды (в
`ProducerConfig`, `ConsumerConfig` и `AdminClientConfig` одинаково).

### 3.2. Реализация `ConfigurationKafkaConnectionProvider` (Aspire)

Адаптер над `IOptionsMonitor<KafkaOptions>`:

- `Current` = `BootstrapServers` непуст → параметры без SASL-полей; пустой →
  `null` (мягче сегодняшнего поведения «Confluent бросает при Build» —
  единая fail-open-семантика обеих веток; consciously accepted).
- `OnChange` = `kafkaOptions.OnChange` — стреляет по любому изменению секции
  `Kafka` (эквивалент сегодняшней подписки builder'ов).

### 3.3. Реализация `DiscoveryKafkaConnectionProvider` (HaDb)

В `Infrastructure.App.Kafka` (ProjectReference на `HA.Kafka`), конструктор
принимает `IKafkaDiscoveryStore` + имя кластера:

- `Current`: `store.Get(cluster)` → Failed (не заявлен/снапшот не собран) →
  `null`; успех → `snapshot.GetClientConfig()` (t05: null при отсутствии
  `endpoints` ИЛИ неполном наборе кредов) → `null`; иначе маппинг
  `KafkaClientConfig` → `KafkaConnectionParams` (1:1, включая константные
  `SASL_PLAINTEXT`/`PLAIN`).
- `OnChange`: подписка на `store.Updated` (только своего кластера) →
  перечитать `Current` → сравнить с последним отданным значением → при
  отличии — обновить и стрелять. Значение вычисляется из снапшота
  детерминированно; `State` кластера НЕ интерпретируется (клиенту достаточно
  наличия точек дискавери: `NOT_INITIALIZED`-кластер без `endpoints` уже даёт
  `null`; `TO_REMOVE` — клиент работает, пока живы брокеры и ключи).
- Hosted-сервис не добавляется: актуализацией снапшота владеет `HA.Kafka`
  (`KafkaDiscoveryRefresher`, t05); провайдер — чистый потребитель кэша.

### 3.4. Перевод потребителей на шов (`Infrastructure.App.Kafka`, внутренние детали)

- **`KafkaProducerBuilder`**: `Build()` читает `provider.Current`:
  параметры есть → Confluent-producer с SASL-маппингом; `null` →
  producer-обёртка, чья `SendAsync` возвращает `Result.Failed` («kafka-конфиг
  недоступен», лог с троттлингом). Подписки на изменения — `TConfig.OnChange`
  и `provider.OnChange` → `InvalidateCache()` (существующая механика
  замещения кэша; orphaned-producer'ы дорабатывают и диспозятся в
  `DisposeAsync` builder'а). Подписка на `IOptionsMonitor<KafkaOptions>`
  устраняется — провайдер становится единственным путём соединительных
  параметров.
- **`KafkaConsumerBuilder`/`KafkaConsumer`**: `Build()` снимает параметры из
  `provider.Current`; при `null` построение Confluent-клиента откладывается
  до появления параметров (ожидание отменяемо через токен consumer'а,
  пробуждение по `provider.OnChange`; лог «ждём kafka-конфиг»). `StartAsync`
  не завершается ошибкой, пока конфига нет — rebuild-цикл владельца
  (например `KafkaBusConsumerHostedService`) не крутится вхолостую.
- **`KafkaConfigChangeSource<TConfig>`**: подписки `TConfig.OnChange` +
  `provider.OnChange` (вместо `TConfig` + `KafkaOptions`). В Aspire-ветке
  эквивалентно сегодняшнему поведению (см. §3.2), в HaDb-ветке добавляет
  реакцию на `Updated` → `TriggerRestart` consumer'а с новыми параметрами.
- **Фабрика `IKafkaTopicClient`** (AdminClient): параметры читаются из
  `provider.Current` в момент создания adapter'а; при `null` — adapter с
  пустым bootstrap (admin-операции возвращают `Result.Failed` через
  существующий jitter-ретрай). Обновление уже созданного `IAdminClient` при
  смене параметров НЕ выполняется (см. ограничения §6).

### 3.5. Ветвление `ModuleExtensions.AddKafka` (по образцу `AddHaDbAppDatabases`)

```
AddKafka(services, configuration):
  если DatabaseSourceReader.Read(configuration) == Aspire:
      регистрация ConfigurationKafkaConnectionProvider (адаптер IOptionsMonitor<KafkaOptions>);
      всё остальное — как сегодня (KafkaOptions из ConnectionStrings:Kafka + секция Kafka).
  иначе (HaDb):
      cluster = configuration["Kafka:Cluster"];
      пусто → InvalidOperationException("Kafka:Cluster не задан (Database:Source=HaDb)") — fail-fast;
      AddHaKafka(configuration).AddKafkaCluster(cluster)   // HA.Kafka: стор+refresher+watch+health
      регистрация DiscoveryKafkaConnectionProvider(store, cluster).
  в обеих ветках: KafkaOptions (GroupId-дефолт), admin-фабрика, AutoRegistration сборки.
```

`Program.cs` НЕ меняется: `.AddKafka(builder.Configuration)` сам выбирает
ветку (паттерн `AddHaDbAppDatabases`). В Aspire-окружении HA.Kafka-типы не
регистрируются вовсе (hosted-сервису нужен etcd — ронял бы старт Aspire-стенда;
причина, по которой t05 не подключала модуль).

### 3.6. Конфигурация (appsettings)

- `src/PuzzleServer.Api/appsettings.json` (дефолт = HaDb-режим, ридер: пустая
  секция → HaDb): добавить `"Kafka": {"Cluster": "events"}` и
  `"HaKafka": {"EtcdEndpoints": ["http://localhost:2379"]}` (секция HaKafka
  самостоятельная — модуль независим от HaDb; дублирование endpoints в
  конфиге осознанное, по одному etcd на стенд).
- `appsettings.Development.json` (`Database:Source=Aspire`): без правок —
  кластер и HaKafka-секция не нужны.
- Dev-стенд pg (`00-up.sh`): приложение-кандидат на подключение к стенду —
  вне скоупа; конфигурационные значения уже согласуются с сидом
  (`events`/`pending`).

### 3.7. Документация Puzzle

- `docs/01.16-kafka.md` — новый раздел «Источник соединительных параметров»:
  `Database:Source`, `IKafkaConnectionProvider`, SASL-маппинг, fail-open,
  `Kafka:Cluster`; правка существующих разделов про `KafkaOptions`
  (BootstrapServers — только Aspire-ветка).
- `docs/01.19-ha-kafka.md` — короткий раздел «Интеграция с
  `Infrastructure.App.Kafka`» (ссылка на 01.16).

## 4. Ротация app_password (сценарий arch/16 §5 H — что видит клиент)

Фазы воркера: A) rolling-рестарт брокеров с JAAS из двух пользователей
(OLD+NEW) — клиенты продолжают работать с OLD; B) txn etcd: `app_password` =
NEW — клиенты перечитывают etcd и переподключаются; C) rolling-рестарт с JAAS
только NEW. От клиента требуется: быстрая реакция на смену кредов (watch —
секунды, t05) и корректное переподключение. Механика t10: фаза B →
`store.Updated` → провайдер `OnChange` (креды изменились) → producer-кэш
инвалидирован (новые Build — с NEW), consumer сам останавливает loop и
пересоздаётся с NEW (существующий self-restart). Формально между фазами B и C
нет синхронизации с клиентами — короткое окно auth-fail у неуспевшего
переподключиться клиента возможно по построению контракта; клиентские
механизмы смягчают: idempotent-producer ретраи, at-least-once consumer,
fail-open ожидание. Интеграционный тест (§7) фиксирует исход: поток сообщений
не теряется.

## 5. Фазы реализации

Каждая фаза — самостоятельный коммит; тесты вместе с кодом (TDD, AAA).
Репозитории: код — `/Users/demakaev/ZCodeProject/Puzzle` (ветка
`feat-t10-kafka-discovery-integration`); артефакты фаз — этот worktree pg.

1. **Ф1 — шов + Aspire-ветка на шве.** `KafkaConnectionParams` (редакция
   ToString), `IKafkaConnectionProvider`,
   `ConfigurationKafkaConnectionProvider`, чистые функции SASL-маппинга в
   Confluent-конфиги; перевод `KafkaProducerBuilder`/`KafkaConsumerBuilder`/
   `KafkaConfigChangeSource`/admin-фабрики на провайдер; producer-обёртка
   «нет конфига» (`SendAsync` → Failed). Unit-тесты; не-регрессия Aspire
   (существующие тесты Bus/Kafka зелёные).
2. **Ф2 — Discovery-провайдер + fail-open консюмера.** ProjectReference
   App.Kafka → HA.Kafka; `DiscoveryKafkaConnectionProvider` (Get/Updated,
   фильтр по параметрам, null-семантика); отложенное построение Confluent
   consumer'а до валидных параметров (отменяемо). Unit-тесты на fake-сторе.
3. **Ф3 — ветвление AddKafka + конфигурация.** `DatabaseSourceReader`-ветка,
   fail-fast `Kafka:Cluster`, регистрация `AddHaKafka().AddKafkaCluster(...)`,
   appsettings.json (`Kafka:Cluster`, `HaKafka:EtcdEndpoints`). Unit-тесты
   регистраций (обе ветки, fail-fast).
4. **Ф4 — интеграционные тесты полного контура.** Testcontainers: etcd
   (`quay.io/coreos/etcd:v3.5.21`) + Kafka (`apache/kafka:4.0.0`, единственный
   combined-брокер KRaft, CLIENT-listener SASL_PLAINTEXT на динамическом
   host-порту — env по arch/16 §2.2, advertised `localhost` для хост-процесса
   теста, JAAS с пользователем `app`; паттерн — фикстура
   `KafkaClusterFixture` pg). Сценарии §7.
5. **Ф5 — документация.** `docs/01.16-kafka.md`, `docs/01.19-ha-kafka.md`.

## 6. Ограничения и принятые решения

- **arch/ pg не правится** (контракт §5–§6 достаточен, решение унаследовано от
  t05); правка roadmap pg — только мерж-гейт (удаление пункта t10).
- **`State` кластера не интерпретируется** клиентом (принцип §3.3);
  клиентская семантика = «есть точки дискавери или нет».
- **`IAdminClient` не перестраивается** при смене параметров: adapter создаётся
  от среза на момент первого resolve и живёт до диспоза владельца. Admin-операции
  редки, обёрнуты jitter-ретраями и `Result.Failed` — документированное
  ограничение; перестройка — если понадобится (roadmap-повод).
- **Реестр топиков снапшока** (`Topics`, `FindTopic`) клиентами
  `Infrastructure.App.Kafka` не используется — доступен потребителям напрямую
  через `IKafkaDiscoveryStore` (out of scope).
- **`KafkaOptions.BootstrapServers` в HaDb-режиме игнорируется** (провайдер —
  единственный источник); пережиток в конфиге не ошибка (не читается).
- **SASL-поля в `KafkaOptions` не добавляются** (Aspire = PLAINTEXT; TLS/SCRAM
  — вместе с t03-kafka-security в pg, тогда же модель расширится).
- **Ровно один kafka-кластер на процесс** (симметрия сегодняшнему одиночному
  bootstrap): `Kafka:Cluster` — скаляр. Multi-cluster — roadmap-повод.
- **Out of scope:** e2e-проверка полного цикла ротации воркером (три фазы
  rolling-рестарта) — интеграционный тест эмулирует фазу B (смена пароля в
  etcd) против брокера с окном двух кредов; AdminClient-операции поверх
  дискавери; метрики Prometheus (t04); TLS (t03); изменения pg-стенда.
- **Двухрепозиторная организация** — как t05: код в Puzzle (коммиты по его
  AGENTS.md: feature-ветки — свободно), spec/plan/roadmap в pg-worktree;
  ветка кода `feat-t10-kafka-discovery-integration`.

## 7. Тестирование

- **Unit** (`PuzzleServer.UnitTests/Kafka/`, без Docker):
  - `KafkaConnectionParams`: редакция пароля в `ToString`;
  - SASL-маппинг: полный набор → `SaslPlaintext`/`Plain`/креды в трёх видах
    Confluent-конфигов; null-поля → не заданы (PLAINTEXT-дефолт);
  - `ConfigurationKafkaConnectionProvider`: Current из options (пустой
    bootstrap → null); OnChange по options-нотификации;
  - `DiscoveryKafkaConnectionProvider` (fake `IKafkaDiscoveryStore`): Current
    из `GetClientConfig()` (null-семантика: Failed/нет endpoints/неполные
    креды); OnChange при изменении bootstrap и при ротации пароля; OnChange
    НЕ стреляет при изменении только топиков/`FetchedAtUtc`; переход
    null→params стреляет;
  - `KafkaProducerBuilder`: Build при null-Current → SendAsync Failed (без
    исключений); после OnChange с параметрами → новый producer;
  - `KafkaConsumer`: при null-Current построение Confluent-клиента отложено
    (`StartAsync` жив, клиент не создаётся), появление параметров → построение
    и подписка (fake-провайдер, без брокера — проверяется отсутствие
    Confluent-клиента до параметров);
  - `KafkaConfigChangeSource`: срабатывание от `TConfig` и провайдера;
  - `AddKafka`-ветвление: Aspire → в контейнере нет `HaKafkaClusterRegistry`,
    провайдер Configuration; HaDb → есть реестр с заявкой `Kafka:Cluster`;
    пустой `Kafka:Cluster` → fail-fast; нераспознанный `Database:Source` —
    fail-fast ридера.
- **Integration** (`PuzzleServer.IntegrationTests/Kafka/`, Docker): фикстура
  etcd+Kafka-SASL (§5 Ф4; динамические порты — `assignRandomHostPort`, без
  хардкодов и без пересечения с зоной dev-станда 16xxx). Сценарии:
  1. **Fail-open**: ключей в etcd нет → провайдер Current=null, producer
     SendAsync → Failed, приложение/хост живы; засев `endpoints`+`app_user`+
     `app_password` → (watch) параметры появляются без рестарта;
  2. **Сквозной контур**: producer пишет, consumer читает реальный
     SASL-брокер; креды/endpoints — только из etcd-ключей;
  3. **Ротация пароля**: брокер с JAAS-окном двух пользователей
     (OLD+NEW); put `app_password` = NEW → переподключение, поток сообщений
     продолжается без потерь (at-least-once: дубли допустимы, потери нет);
  4. **Шум не роняет**: put `topics/<T>` (реестр) с теми же endpoints/кредами
     → событие стора стреляет, провайдер OnChange — нет, соединения живы;
  5. **Смерть etcd**: снапшот в кэше → клиент работает; возврат etcd →
     обновления продолжаются (совместно с fail-open t05).
- Сборка/тесты: `dotnet build src/PuzzleServer.Api.slnx` (0 warnings);
  `dotnet test src/PuzzleServer.Api.slnx`. Существующие тесты HA/Kafka (t05),
  Bus, HA/Db — зелёные (не-регрессия).

## 8. Критерии приёмки

1. Сборка решения без warnings; все тесты зелёные (unit — без Docker,
   integration — с Docker).
2. Aspire-режим (`Database:Source=Aspire`): поведение `AddKafka` и клиентов
   эквивалентно прежнему — `ConnectionStrings:Kafka`, PLAINTEXT, hot-reload;
   существующие тесты зелёные (не-регрессия).
3. HaDb-режим: `AddKafka` регистрирует `AddHaKafka(...).AddKafkaCluster(...)`
   из `Kafka:Cluster`; отсутствие/пустое значение → fail-fast при старте;
   HA.Kafka-стек (watch, health `HaKafkaCheck`) поднимается в том же процессе.
4. Параметры клиентов в HaDb-режиме: `bootstrap.servers` + SASL_PLAINTEXT/PLAIN
   + креды — только из снапшота (интеграционный критерий: удаление ключей из
   конфигурации не влияет); `GetClientConfig() == null` → провайдер `null`.
5. `OnChange` провайдера: стреляет при изменении endpoints и при ротации
   `app_password` (unit+integration); НЕ стреляет при изменении только
   реестра топиков (unit); producer-кэш инвалидирован, consumer перезапущен с
   новыми параметрами без рестарта процесса.
6. Ротация пароля (integration, реальный брокер): смена `app_password` в etcd
   → клиент переподключается с NEW; сообщения не теряются.
7. Fail-open (integration): нет ключей / etcd лежит → старт жив, SendAsync →
   `Result.Failed`, consumer ждёт; появление ключей / возврат etcd →
   подключение/обновление без рестарта.
8. Секреты: `SaslPassword` редацирован в `ToString()` параметров; пароль не
   появляется в логах провайдера/builder'ов (тесты).
9. Публичный API `Infrastructure.App.Kafka` не изменился (сборки-потребители
   — Bus и др. — без правок); `HA.Kafka` не получила зависимость от
   Confluent.Kafka и не изменилась.
10. Документация: `docs/01.16-kafka.md` (источник параметров, провайдер,
    SASL, fail-open, `Kafka:Cluster`) и `docs/01.19-ha-kafka.md` (раздел
    интеграции) обновлены.
11. Трафик в HaDb-режиме — только чтение `/kafka/clusters/<C>/` через
    HA.Kafka (фиксировано тестами t05; новых путей не добавляется).

## 9. Открытые вопросы

Нет — ключевые решения приняты user-review: единый `Database:Source` на БД и
Kafka; fail-open ожидание при отсутствии конфига; имя кластера — секция
`Kafka:Cluster`; полный интеграционный контур с реальным Kafka (SASL);
архитектура — шов `IKafkaConnectionProvider` в `Infrastructure.App.Kafka` со
ссылкой на `HA.Kafka`.
