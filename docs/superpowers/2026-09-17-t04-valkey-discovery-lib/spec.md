# Spec: t04-valkey-discovery-lib — клиентская дискавери-библиотека HA.Valkey

> Дата: 2026-09-17. Тopic: `t04-valkey-discovery-lib`.
> Roadmap-пункт: `arch/roadmap/valkey.md` (track valkey).
> Канон контракта: `arch/20-valkey-clusters.md` §4 (клиентский дискавери), §5
> (толерантность читателей) — зафиксирован задачей t01, настоящей задачей НЕ
> меняется. Образец реализации: HA.Kafka в репозитории Puzzle
> (`docs/01.19-ha-kafka.md`, `src/PuzzleServer.Infrastructure.App.HA.Kafka`).

## 1. Цель

Клиентская дискавери-библиотека Valkey-кластеров для приложений Puzzle —
проект **`PuzzleServer.Infrastructure.App.HA.Valkey`** в репозитории
**Puzzle** (`/Users/demakaev/ZCodeProject/Puzzle`). Библиотека — **только
читатель** etcd-префикса `/valkey/clusters/<C>/`: держит иммутабельный
`ValkeyClusterSnapshot` в памяти (endpoints, ACL-креды app, raw-state),
актуализирует его в фоне (короткоживущие watch-стримы или poll), переживает
отказ etcd по последнему снапшоту (fail-open) и отдаёт параметры подключения
plain-полями через `GetClientConfig()` — без зависимости от
StackExchange.Redis. Пишет ключи Валkey-домена исключительно ValkeyWorker
(pg, t02); библиотека не делает ни одной etcd-мутации.

Решения пользователя (зафиксированы на фазе brainstorming):

1. **Git-организация**: работа зеркально в двух репозиториях — spec/plan и
   roadmap-правки в feature-ветке `t04-valkey-discovery-lib` worktree pg;
   код и канон-документ библиотеки — в feature-ветке
   `t04-valkey-discovery-lib` репозитория Puzzle (коммиты свободно, мерж в
   main — только по явной просьбе).
2. **Объём**: только библиотека HA.Valkey. Интеграция в клиентский модуль
   потребителя (по образцу связки HA.Kafka → `Infrastructure.App.Kafka`,
   docs/01.16 §1a) выделена в отдельную задачу roadmap
   `t08-valkey-client-integration` (см. Фазу 0): клиентского модуля
   Valkey/Redis со StackExchange.Redis в Puzzle сейчас не существует, у
   kafka интеграция тоже была отдельной задачей (t10).

## 2. Принципы

1. **Только чтение.** Весь etcd-трафик библиотеки — `POST /v3/kv/range`,
   `POST /v3/watch`, `POST /v3/cluster/member/list` (последний — только в
   members-режимах). Put/txn/lease запрещены; фиксируется
   интеграционным тестом по журналу трафика (декоратор typed-клиента).
2. **Образец HA.Kafka — 1:1.** Структура проекта, режимы актуализации
   (WatchLongPoll по умолчанию / Poll), семантика bootstrap, коалесценция
   сигналов, health-состояния, fail-open, `SameContent`-сравнение и событие
   `Updated` только при фактическом изменении — копии HA.Kafka
   (docs/01.19), отличия только в доменной модели (нет topics/brokers) и
   наборе читаемых ключей.
3. **Контракт — arch/20, толерантность — §5.** Битый JSON → parseError и
   пропуск ключа (без исключения); неизвестные ключи → лог + счётчик
   `unknownKeys`; известные ключи вне клиентского подмножества
   (`admin_*`, `nodes/*`) → молча; неполный набор кредов → `App = null` →
   `GetClientConfig() = null`; незнакомое `config.state` → raw-строкой
   (трактовка — дело потребителя); пустой/пробельный `endpoints` → как
   отсутствующий.
4. **Без внешних пакетов.** Проект не ссылается на StackExchange.Redis (и
   вообще ни на какие новые пакеты): параметры клиента — plain-поля.
   Транспорт — общий слой `PuzzleServer.Infrastructure.App.HA.Etcd`
   (`IEtcdClient`/`EtcdHttpClient`/`EtcdEndpointRotation`/
   `EtcdMembersMonitor`, docs/01.18).
5. **Arch-first.** Контракт дискавери уже описан в arch/20 §4 (задача t01)
   и не меняется. Правке подлежит только roadmap pg (декомпозиция t04 →
   t04 + t08) — docs-коммитом в feature-ветку ДО кода (Фаза 0). Канон
   библиотеки в Puzzle — новый `docs/01.21-ha-valkey.md` — пишется до/вместе
   с кодом, детали — в Фазе 1.
6. **Язык.** Документация и комментарии — русские; идентификаторы —
   английские. Тесты — с комментариями по нотации AAA.

## 3. Структура и компоненты

### 3.1. Проект в Puzzle

Новый проект `src/PuzzleServer.Infrastructure.App.HA.Valkey` (namespace
`PuzzleServer.Infrastructure.App.HA.Valkey` + `.Model`, `.Parsing`,
`.Refresh`), net10.0, подключается в `src/PuzzleServer.Api.slnx`; внешних
пакетов нет (CPM `Directory.Packages.props` не трогается). Файлы (зеркало
HA.Kafka):

| Файл | Назначение |
|---|---|
| `ModuleExtensions.cs` | `AddHaValkey(configuration)` + `AddValkeyCluster(name)`; typed etcd-клиент, ротация, сигнальщик по `Mode`, health-check `HaValkeyCheck`, members-монитор, AutoRegistration сборки |
| `HaValkeyOptions.cs` | опции секции `HaValkey` + `HaValkeyRefreshMode` (`WatchLongPoll`/`Poll`) + `HaValkeyMembersMode` (`Poll`/`OnFailure`/`Off`) |
| `HaValkeyClusterRegistry.cs` | реестр заявок кластеров (флуент-паттерн) |
| `HaValkeyException.cs` | ошибки доступа (незаявлен/снапшот не готов) |
| `ValkeyDiscoveryStore.cs` | `IValkeyDiscoveryStore` + кэш снапшотов: `Get`, `RefreshAsync`, событие `Updated` |
| `ValkeyDiscoveryRefresher.cs` | `BackgroundService` + `IHealthCheckService`: bootstrap, шина сигналов с коалесценцией |
| `Refresh/IHaValkeyRefreshSignaler.cs` | контракт сигнальщика |
| `Refresh/HaValkeyWatchLongPollSignaler.cs` | watch-стримы `/v3/watch` по префиксу кластера |
| `Refresh/HaValkeyPollRefreshSignaler.cs` | `PeriodicTimer` |
| `Parsing/ValkeyClusterParser.cs` | чистые функции разбора range-ответа |
| `Model/ValkeyClusterSnapshot.cs` | иммубельный снапшот + `GetClientConfig()` |
| `Model/ValkeyAppSecret.cs` | креды app (User+Password, редакция в `ToString`) |
| `Model/ValkeyClientConfig.cs` | plain-параметры для StackExchange.Redis |
| `HaValkeyHealthCheck.cs` | health по состояниям refresher'а |

### 3.2. Подключение и опции

```csharp
services.AddHaValkey(configuration)
        .AddValkeyCluster("cache");
```

- Первый вызов `AddHaValkey(configuration)` регистрирует модуль: опции
  (секция `HaValkey` + валидация при старте), typed etcd-клиент (таймаут из
  `HaValkey:RequestTimeoutMs`, имя клиента `typeof(IEtcdClient).FullName`
  — для декораторов в тестах), `EtcdEndpointRotation`, сигнальщик по
  `Mode`, health-check `HaValkeyCheck`, members-монитор по `MembersMode`
  (`Off` → монитора нет в DI), AutoRegistration сборки (store/refresher —
  `[InjectAsSingleton]`).
- Каждый `AddValkeyCluster("name")` добавляет заявку; кластеры секцией
  конфига НЕ задаются (PostConfigure наполняет `Clusters` из реестра).
- Fail-fast валидация: повторный `AddHaValkey`; `AddValkeyCluster` без
  модуля; пустое имя; формат не `^[a-z][a-z0-9_]{0,62}$` (arch/20 §1 — без
  дефиса); дубликат; старт без единой заявки; пустые `EtcdEndpoints`;
  неположительные интервалы.
- Опции (дефолты — как у HA.Kafka): `Mode=WatchLongPoll`;
  `EtcdEndpoints=[]`; `RequestTimeoutMs=2000`; `WatchWindowMs=1000`,
  `WatchReopenDelayMs=100`, `WatchErrorDelayMs=1000`; `PollIntervalMs=1000`;
  `MembersMode=Poll`, `MembersPollIntervalMs=30000`,
  `MembersMinIntervalMs=1000`; `BootstrapTimeoutSec=15`.

### 3.3. Модель снапшота

`ValkeyClusterSnapshot`:

- `Cluster` — имя;
- `State` — raw-строка `config.state` (null = Active; absence-of-field и
  незнакомое значение — трактовка потребителем);
- `Endpoints` — строка `endpoints` как есть (`"h1:p1,h2:p2"`; при nodes=1 —
  один адрес; null = ключа нет/пустой-пробельный);
- `App` — `ValkeyAppSecret(Username, Password)` только полным набором
  ОБЕИХ ключей `app_user`+`app_password` (неполный → null); `HasAppSecret`;
- `FetchedAtUtc`, `Revision` (max mod_revision ответа; 0 при пустом ответе;
  start_revision следующего watch-окна).

Вычислитель `GetClientConfig()` → `ValkeyClientConfig?` — **null при
отсутствии `Endpoints` ИЛИ секрета** (потребитель обязан проверить); иначе
plain-поля по arch/20 §4: `Endpoints`, `Username`, `Password`,
`Ssl = false` (v1 без TLS; константа `SslValue=false` по образцу
kafka-констант `SecurityProtocolValue`/`SaslMechanismValue`; `ssl=true`+CA —
после t06). Редакция пароля: `ValkeyAppSecret.ToString()` и
`ValkeyClientConfig.ToString()` дают `Password = ***` — фиксируется тестами.

Семантика равенства — `SameContent` в сторе: структурное сравнение
`Cluster`/`State`/`Endpoints`/`App` (record со строковыми полями —
структурно) без `FetchedAtUtc`/`Revision`; коллекционных полей нет (topics в
домене отсутствуют — arch/20 §2 «отличия от kafka»), поэтому сравнение
проще kafka-образца, паттерн тот же. Событие `Updated` стреляет только при
изменении содержимого (put тем же значением, включая рост ревизии, события
НЕ даёт); смена `app_password` (ротация arch/21 §5 E) = событие `Updated` →
новый пароль в `GetClientConfig()`.

### 3.4. Парсер `/valkey/clusters/<C>/`

Чистые функции разбора range-ответа (сегменты `key.Split('/')`; факт-ключи
кластера — 5 сегментов `/valkey/clusters/<C>/<leaf>`):

| Ключ | Поведение |
|---|---|
| `config` | JSON → только `state` raw-строкой (отсутствие поля/не-строка → null = Active). Битый JSON → parseError (укороченное редактированное значение) + state=null — Active-ветка, кластер в снапшоте жив (arch/20 §5) |
| `endpoints` | plain-строка в `Endpoints`; null/пустая/пробельная → null (arch/20 §5) |
| `app_user` / `app_password` | креды; в снапшот — только полным набором обоих |
| `admin_user` / `admin_password` | известные ключи вне клиентского подмножества §4 — молча (аналог kafka `brokers/`) |
| `nodes/<k>/state`, `nodes/<k>/resources` | известные ключи вне клиентского подмножества — молча |
| прочее | `unknownKeys` (лог + счётчик; arch/20 §5) — в т.ч. будущий `ca_pem` после t06 (обратная совместимость читателя: новый ключ не роняет парсер) |

Разбор — `JsonDocument` + ручные читатели, без `JsonSerializerOptions`
(паттерн kafka-парсера: предупреждение компилятора о неиспользуемом поле
под `TreatWarningsAsErrors=true`).

### 3.5. Хранилище и актуализация

- `IValkeyDiscoveryStore`: `Get(cluster)` — мгновенно из кэша (без сети;
  незаявленный кластер/снапшот не собран → `Result.Failed` c
  `HaValkeyException`); `RefreshAsync(cluster, ct)` — форс-рефетч (один
  range по префиксу `/valkey/clusters/<C>/` на активном endpoint —
  консистентность ревизии; отказ → `rotation.ReportFailure`, успех →
  `ReportSuccess`); событие `Updated`.
- `ValkeyDiscoveryRefresher` (`BackgroundService`): bootstrap при старте —
  рефетч всех кластеров с бюджетом `BootstrapTimeoutSec` (провал НЕ роняет
  старт приложения); далее сигналы → проходы с коалесценцией (пачка
  сигналов за время прохода = один дополнительный проход). Health:
  `Inited` (хотя бы один успешный рефетч), `Working` (успех за последние 3
  интервала режима), `StatusError` (Failed при ≥2 рефетч-сбоях подряд).
- `HaValkeyWatchLongPollSignaler`: короткоживущие watch-стримы `/v3/watch`
  по одному на префикс кластера; `start_revision` = `Revision` снапшота
  своего кластера (пропусков между окнами нет); первое
  событие/`Compacted` → сигнал → полный рефетч; compact → форс-рефетч и
  сброс ревизии; сбой окна → сигнал + ротация endpoint.
- `HaValkeyPollRefreshSignaler`: `PeriodicTimer` → полный рефетч.
- Members: per-module `EtcdMembersMonitor` (docs/01.18; объединение с
  мониторами HA.Db/HA.Kafka — YAGNI), seed-пул = `EtcdEndpoints`.
- **Fail-open**: недоступность всех endpoints etcd → `Get` отдаёт последний
  снапшот без ограничения времени; `RefreshAsync` → Failed (кэш не
  трогаем); health деградирует; возврат etcd — восстановление первым
  окном/тиком.

### 3.6. Тесты (Puzzle)

- **Unit** (`src/PuzzleServer.UnitTests/HA/Valkey/`): парсер (канонические
  примеры arch/20 §2.1; битые JSON; пустой endpoints; admin/nodes — молча;
  unknownKeys), модель (`GetClientConfig()` null-кейсы; редакция пароля),
  стор (SameContent/Updated; fail-open), сигнальщики
  (`FakeEtcdClient`-паттерн), регистрация модуля (fail-fast; формат имени).
- **Integration** (`src/PuzzleServer.IntegrationTests/HA/Valkey/`):
  testcontainers-etcd по образцу `KafkaEtcdFixture` (generic
  `quay.io/coreos/etcd:v3.5.21`, готовность — POST-ретрай; host-порт —
  параметр фикстуры, НЕ пересекающийся с занятыми 32490/32495). Полный цикл
  дискавери в обоих режимах: заявка кластера → put ключей префикса (конечный
  etcd-клиент теста — прямой HTTP, НЕ через библиотеку) → снапшот →
  `GetClientConfig()`; смена `app_password`/`endpoints` → `Updated`;
  удаление ключа → деградация конфига; стоп etcd → fail-open (последний
  снапшот); рестарт на том же endpoint → восстановление; битые значения →
  толерантность. Фиксация read-only трафика: декоратор `RecordingHandler` на
  typed-клиенте — только range/watch/member-list.
- Тесты пишутся с AAA-комментариями; после каждой docker-серии — зачистка
  контейнеров (ryuk + контроль).

## 4. Фазы

| # | Фаза | Что делается | Где |
|---|---|---|---|
| 0 | Roadmap-декомпозиция (arch-first, docs-коммит ДО кода) | `arch/roadmap/valkey.md`: уточнить пункт t04 (объём — только библиотека, без интеграции) и дописать новый пункт `t08-valkey-client-integration` (интеграция HA.Valkey в клиентский модуль Valkey со StackExchange.Redis по образцу docs/01.16 §1a — когда модуль появится; `← t04-valkey-discovery-lib`) | pg-worktree, feature-ветка |
| 1 | Канон Puzzle (docs-коммит) | Новый `docs/01.21-ha-valkey.md` по структуре 01.19 (назначение/подключение/режимы/модель/читаемые ключи/грабли); строка в индексе `docs/01-infrastructure.md` | Puzzle, feature-ветка |
| 2 | Каркас + модель + парсер (TDD) | Проект csproj, slnx, опции, реестр, исключение; `Model/*`, `Parsing/*`; unit-тесты парсера и модели | Puzzle |
| 3 | Стор + актуализация + DI | `ValkeyDiscoveryStore`, `ValkeyDiscoveryRefresher`, оба сигнальщика, `HaValkeyHealthCheck`, `ModuleExtensions` (AddHaValkey/AddValkeyCluster, members-монитор); unit-тесты (FakeEtcdClient, регистрация) | Puzzle |
| 4 | Интеграционные тесты | Фикстура etcd (порт-параметр), полный цикл обоих режимов, fail-open, read-only-фиксация | Puzzle |
| 5 | Синхронизация и гейты | Сверка docs/01.21 с фактом; сборка — 0 новых warnings (прод-код HA.Valkey — 0, см. §6 п.1); прогоны серий (юниты → интеграция) с зачисткой после каждой; code-review; мерж-гейт pg — снятие тега t04 из roadmap (пункт + `←`-зависимость t08) тем же мерж-коммитом | оба репо |

Мерж в main любого репозитория и пуши — ТОЛЬКО по явной просьбе
пользователя.

## 5. Ограничения

1. Библиотека НЕ пишет в etcd (единственная «активность» —
   короткоживущие watch-стримы).
2. Контракт arch/20 не меняется: библиотека реализует §4–5 как есть;
   любые обнаруженные нестыковки канона — вопрос пользователю, не
   самовольная правка.
3. TLS не поддерживается: `Ssl=false` — v1; `ssl=true`+CA — t06 (после него
   читатель обязан остаться совместимым: `ca_pem` → unknownKeys).
4. Никаких внешних пакетов (StackExchange.Redis — только потребитель,
   задача t08); Discovery-подмножество ключей — §4: `config`/`endpoints`/
   `app_user`/`app_password`.
5. Один etcd-контур на стенд (панель/воркеры/HA-модули); members-режим —
   per-module монитор (дублирование с HA.Db/HA.Kafka — осознанный паттерн
   docs/01.18).
6. Не реализуется: интеграция в клиентский модуль (t08), метрики домена
   (t05), TLS (t06), панель (t03), sentinel/replica-топологии (вне канона).
7. Тестовые порты etcd-фикстур Puzzle — фиксированные параметры без
   коллизий с существующими (32490, 32495) и с портами dev-стенда;
   таймауты ожидания контейнера — короткие.

## 6. Критерии приёмки

1. `dotnet build src/PuzzleServer.Api.slnx` — 0 новых предупреждений
   (эрратум мерж-гейта: в Puzzle `TreatWarningsAsErrors` не включён и есть
   baseline-предупреждения решения; требование 0 warnings относится к
   прод-коду HA.Valkey); новых пакетов нет.
2. Юнит-тесты HA.Valkey зелёные: парсер по каноническим примерам arch/20
   §2.1 (заявочный config с state / Active-конфиг без state / TO_REMOVE),
   неполные креды → `App=null` → `GetClientConfig()=null`, пустой
   `endpoints` → null, битый JSON → parseError без исключения, admin-креды
   и `nodes/*` — молча, неизвестный ключ → unknownKeys, редакция пароля в
   `ToString()`, fail-fast валидации подключения.
3. Интеграционные тесты против реального etcd зелёные в ОБОИХ режимах
   (WatchLongPoll, Poll): полный цикл заявка → снапшот → обновление кредов
   (ротация) через `Updated`; fail-open при стопе etcd (последний снапшот
   доступен неограниченно); восстановление после рестарта etcd; журнал
   трафика содержит ТОЛЬКО range/watch/member-list.
4. Событие `Updated` не стреляет на put тем же содержимым (включая рост
   ревизии).
5. Доки синхронны: `docs/01.21-ha-valkey.md` описывает реализованное,
   индекс `docs/01-infrastructure.md` ссылается на него; roadmap pg:
   t04 уточнён + t08 добавлен docs-коммитом ДО кода (Фаза 0); на мерж-гейте
   тег t04 снят из `arch/roadmap/valkey.md` тем же мерж-коммитом (пункт и
   `←`-зависимость t08).
6. После каждой docker-серии — зачистка: не осталось контейнеров/сетей
   фикстуры; тесты портят изоляцию друг друга — недопустимо.
7. Code-review пройден, мерж-гейты соблюдены (мерж/пуш — только по явной
   просьбе пользователя).

## 7. Риски и mitigation

| Риск | Mitigation |
|---|---|
| Неучтённые отличия домена от kafka (нет topics — упрощение; admin-креды в том же префиксе) | Парсер тестируется по каноническим примерам arch/20 §2.1; admin/nodes — явные кейсы «молча» |
| Коллизия портов etcd-фикстур с существующими тестами Puzzle | Порт — параметр фикстуры, выбирается вне 32490/32495; проверка занятости при старте |
| Будущий t06 (TLS) ломает читателя | `ca_pem` попадает в unknownKeys (не падает) — кейс в юнит-тестах парсера уже сейчас |
| Двойное чтение одного etcd несколькими HA-модулями | Пер-модульные мониторы — дёшево (member/list раз в 30 с, docs/01.18); объединение — YAGNI |

## 8. Open questions

Нет — обе развилки (git-организация, объём) закрыты решениями пользователя
на фазе brainstorming (см. §1).
