# t91-kafka-seed-api-flaky — план (фикс флейка SeedDemo_AlreadySeeded_NoOp)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** устранить флейк интеграционного теста `KafkaWorker.IntegrationTests.Api.KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp` — самодостаточный Arrange жертвы (вариант A) + own-only teardown классов-загрязнителей (вариант B); prod-код НЕ меняется.

**Архитектура:** три точечные правки тестовой инфраструктуры в `src/tests/KafkaWorker.IntegrationTests/Api/`: (A) чистка 5 префиксов сида в Arrange жертвы перед первым `POST /api/seed/demo` (копия прецедента `SeedDemo_EmptyEtcd_SeedsCanonicalKeySet`); (B) `IAsyncLifetime.DisposeAsync` у `ClusterMutationsApiTests` и `TopicMutationsApiTests` с чисткой ровно своих кластеров/заявок — per-test (после каждого кейса). Инвариант коллекции «общий etcd» не меняется; `SeedDemoHandler` не трогается (идемпотентность по живому `config` — канон arch/16 §1.1.1).

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit v3 (`IAsyncLifetime` с `ValueTask`-сигнатурами; экземпляр тест-класса на каждый тест-метод → `DisposeAsync` per-test), Testcontainers-etcd.

**Spec:** `docs/superpowers/2026-09-14-fix-kafka-seed-api-flaky/spec.md` (исполнитель читает spec и план вместе).

## Глобальные ограничения

- Меняются ТОЛЬКО три файла: `src/tests/KafkaWorker.IntegrationTests/Api/KafkaSeedApiTests.cs`, `.../ClusterMutationsApiTests.cs`, `.../TopicMutationsApiTests.cs` (spec §6). Плюс отдельный коммит снятия roadmap-тега t91 в feature-ветке в рамках мерж-гейта (Task 4; в `main` попадает тем же мержем задачи, прецедент t11 — spec §6, §7.7).
- Прод-код (`src/KafkaWorker.*`) и `arch/` — БЕЗ изменений (spec §2.4, §6).
- `KafkaSeedApiTests` НЕ получает teardown; `UpdateBrokerResourcesApiTests`, `MtlsApiTests`, `MetricsTests`, `RestartApiTests`, `WorkerApiCertStartupTests`, `KafkaApiTestSeed`, `KafkaApiFactory` — НЕ трогаются (spec §4.3).
- Существующие предочистки внутри кейсов `PostTopic`/`DeleteTopic`/`CancelLifecycle` НЕ убираются (spec §4.2).
- Никаких sleep/ретраев/таймингов; порты не хардкодятся (spec §3, §6).
- Комментарии в коде — по-русски; идентификаторы — на английском; тесты — по AAA (правки A/B — Arrange/teardown, тела Act/Assert жертвы не меняются).
- Сборка с `TreatWarningsAsErrors=true` — любой ворнинг ломает build (это часть проверок серий).
- Все bash-команды — из корня worktree `/Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky` (cwd между вызовами не сохраняется — использовать абсолютные пути).
- После КАЖДОЙ тестовой серии — зачистка docker (команды в Task 3); канонический список стендовых исключений (spec §5 Ф3, сверен по compose-файлам стенда): префиксы `as-` (dev-stand/adminpanel/docker-compose.yml), `pgw-stand-` (dev-stand/compose.yaml), `deploy-` (deploy/docker-compose.yml — имена даёт compose-проект по каталогу `deploy`: deploy-pgworker-*, deploy-kafkaworker-*). Мерж в `main` и пуш — ТОЛЬКО по явной просьбе пользователя.
- Таймаут ожидания bash-команды — не более 600 с на серию; упавший прогон анализируется по логам, перезапуск «для выяснения» запрещён (spec §7.3).

---

### Task 1: Ф1 — вариант A: самодостаточный Arrange `SeedDemo_AlreadySeeded_NoOp`

**Files:**
- Modify: `src/tests/KafkaWorker.IntegrationTests/Api/KafkaSeedApiTests.cs:92-98` (Arrange кейса; Act/Assert строки 100–111 без изменений)

**Interfaces:**
- Consumes: `Etcd.Gateway.DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)` — сигнатура `IEtcdGateway` (PgWorker.Etcd/Client/IEtcdGateway.cs:20); прецедент вызова — тот же файл, строки 43–45.
- Produces: ничего для других задач (точечная правка одного Arrange).

- **Вход (предусловие):** ветка `fix-kafka-seed-api-flaky` чистая (spec-коммит или untracked spec.md — норма); правки Task 2/3 ещё не делались.
- **Действие:** вставка чистки 5 префиксов сида в Arrange перед первым POST (код в Шаге 1.1).
- **Выход:** первый POST кейса гарантированно идёт по ветке наливки (`seeded:true`) независимо от порядка классов/методов коллекции; Act/Assert кейса не изменены.
- **Проверка:** изолированный прогон класса — 3/3 зелёные (Шаг 1.2).
- **Спека:** §4.1, принцип §3.1; критерии приёмки №1 и №4.

- [ ] **Шаг 1.1: Внести правку Arrange**

В файле `src/tests/KafkaWorker.IntegrationTests/Api/KafkaSeedApiTests.cs` заменить блок (строки 94–96 текущего файла):

```csharp
        // Arrange — наливаем (если ещё не налито предыдущим кейсом) и фиксируем значения
        var ct = TestContext.Current.CancellationToken;
        await Client.PostAsync("/api/seed/demo", null, ct);
```

на:

```csharp
        // Arrange — самодостаточный старт: порядок классов/методов коллекции не
        // гарантирован, чужой /kafka/clusters/events/config (наследие
        // SeedActiveClusterAsync соседних классов) уводил первый POST в no-op
        // без записи rotations и ронял Assert на null — чистим все префиксы
        // сида (как EmptyEtcd) и наливаем канонический набор сами
        var ct = TestContext.Current.CancellationToken;
        foreach (var prefix in new[] { "/kafka/clusters/events/", "/kafka/clusters/pending/",
            "/kafkaworker/rotations/", "/kafkaworker/rebalances/", "/kafkaworker/reassignments/" })
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, prefix, prefix: true, ct);
        await Client.PostAsync("/api/seed/demo", null, ct);
```

Строки ниже (`var config = ...`, `var rotation = ...`, Act, Assert) — НЕ трогать.

- [ ] **Шаг 1.2: Прогон класса изолированно**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
dotnet test src/tests/KafkaWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName~KafkaSeedApiTests"
```

Ожидание: сборка без ошибок (ворнинг = ошибка, `TreatWarningsAsErrors`); `Passed! - Failed: 0, Passed: 3` (кейсы `SeedDemo_EmptyEtcd_SeedsCanonicalKeySet`, `SeedDemo_AlreadySeeded_NoOp`, `SeedDemo_DisabledFlag_404`).

- [ ] **Шаг 1.3: Зачистка docker после серии**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
for id in $(docker ps -aq); do name=$(docker inspect --format '{{.Name}}' "$id"); \
  case "$name" in /as-*|/pgw-stand-*|/deploy-*) ;; *) docker rm -f "$id" ;; esac; done && \
docker network prune -f
```

Исключения — канонический список стенда (spec §5 Ф3): `as-` (dev-stand/adminpanel/docker-compose.yml), `pgw-stand-` (dev-stand/compose.yaml), `deploy-` (deploy/docker-compose.yml). Ожидание: команда завершилась успешно; контроль: `docker ps -a --format '{{.Names}}' | grep -vE '^(as-|pgw-stand-|deploy-)' | wc -l` → `0`.

- [ ] **Шаг 1.4: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
git add src/tests/KafkaWorker.IntegrationTests/Api/KafkaSeedApiTests.cs && \
git commit -m "test(kafka-api): SeedDemo_AlreadySeeded_NoOp — самодостаточный Arrange (t91): чистка 5 префиксов сида перед первым POST по прецеденту EmptyEtcd; порядок классов/методов коллекции не гарантирован, чужой events/config уводил первый POST в no-op без записи rotations — NRE в Assert; Act/Assert не изменены"
```

---

### Task 2: Ф2 — вариант B: own-only teardown классов-загрязнителей

**Files:**
- Modify: `src/tests/KafkaWorker.IntegrationTests/Api/ClusterMutationsApiTests.cs:16` (сигнатура класса) + вставка teardown-блока после свойств `Client`/`Etcd` (строка 20)
- Modify: `src/tests/KafkaWorker.IntegrationTests/Api/TopicMutationsApiTests.cs:16` (сигнатура класса) + вставка teardown-блока после свойства `Etcd` (строка 20)

**Interfaces:**
- Consumes: `Xunit.IAsyncLifetime` (v3, `ValueTask`-сигнатуры). Фреймворковый прецедент — `KafkaApiFixture` (collection fixture, `KafkaApiFactory.cs:46-64`); `RestartApiTests.RestartHost` — НЕ фреймворковый прецедент (там `IAsyncLifetime` вызывается вручную через `await using`, `RestartApiTests.cs:46` — на него не ссылаться). Плюс `Etcd.Gateway.DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)` (`IEtcdGateway.cs:20`).
- Produces: ничего для других задач (обе правки локальны в своих классах).

- **Вход (предусловие):** Task 1 закоммичен; классы пока без `IAsyncLifetime`.
- **Действие:** оба класса получают `: IAsyncLifetime` + `DisposeAsync` с own-only чисткой (код в Шагах 2.1/2.2).
- **Выход:** оба класса чистят за собой ровно свои кластеры/заявки при ЛЮБОМ исходе КАЖДОГО кейса. Семантика per-test: xUnit создаёт новый экземпляр тест-класса на каждый тест-метод, поэтому `DisposeAsync` выполняется после КАЖДОГО кейса, а не однократно после всех кейсов класса — чистка только усиливается, подписка на класс (class-fixture) не требуется. Это безопасно: кейсы этих классов самодостаточны (каждый наливает свой сид в Arrange — чистка после кейса не отнимает нужное у следующего), `DeleteAsync` по несуществующим ключам/префиксам — no-op (кейс мог не создавать часть ключей своего списка), тест-методы одного класса не паралеллятся (параллелизм xUnit — на уровне коллекций; внутри коллекции классы и их кейсы идут последовательно).
- **Проверка:** совместный прогон трёх классов (загрязнители + жертва) — все зелёные (Шаг 2.3).
- **Спека:** §4.2 (списки префиксов/ключей — дословно; per-test семантика), принцип §3.2; критерий приёмки №2.

- [ ] **Шаг 2.1: `ClusterMutationsApiTests` — реализовать `IAsyncLifetime`**

В файле `src/tests/KafkaWorker.IntegrationTests/Api/ClusterMutationsApiTests.cs`:

а) сигнатуру класса (строка 16):

```csharp
public class ClusterMutationsApiTests(KafkaApiFixture fixture)
```

заменить на:

```csharp
public class ClusterMutationsApiTests(KafkaApiFixture fixture) : IAsyncLifetime
```

б) сразу после свойства `private EtcdFixture Etcd => fixture.Etcd;` (строка 20) вставить:

```csharp
    // own-only: чистим только созданное этим классом (после каждого кейса —
    // per-test teardown IAsyncLifetime: xUnit создаёт экземпляр класса на
    // каждый тест, DisposeAsync выполняется после каждого кейса, а не
    // однократно после всех — чистка только усиливается, подписка на класс
    // не требуется) при любом исходе (правило полной самоочистки AGENTS.md).
    // Кластерные префиксы — целиком (lifecycle-ключи живут под ними); заявки
    // — точечными ключами, общие префиксы /kafkaworker/... не выключаем
    // (чужая территория коллекции; bad/nosuch не создаются — невалидное
    // тело/404). Безопасно: кейсы самодостаточны, DeleteAsync по
    // несуществующим ключам — no-op, методы класса не паралеллятся.
    // CancellationToken.None — токен теста к teardown уже неактуален,
    // удаления быстрые и идемпотентные.
    private static readonly string[] OwnClusterPrefixes =
    [
        "/kafka/clusters/smoke/", "/kafka/clusters/dup/", "/kafka/clusters/race/",
        "/kafka/clusters/gone/", "/kafka/clusters/events/", "/kafka/clusters/events2/",
        "/kafka/clusters/rotme/", "/kafka/clusters/rotme2/", "/kafka/clusters/reb/",
    ];

    private static readonly string[] OwnTicketKeys =
    [
        "/kafkaworker/rotations/events", "/kafkaworker/rotations/rotme",
        "/kafkaworker/rotations/rotme2", "/kafkaworker/rebalances/events",
        "/kafkaworker/rebalances/reb", "/kafkaworker/admin_rotations/events",
    ];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var prefix in OwnClusterPrefixes)
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, prefix, prefix: true, CancellationToken.None);
        foreach (var key in OwnTicketKeys)
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, key, prefix: false, CancellationToken.None);
    }
```

`using Xunit;` в файле уже есть (IAsyncLifetime доступен); `CancellationToken` — из неявных `System.Threading` (ImplicitUsings). Тела кейсов НЕ менять.

- [ ] **Шаг 2.2: `TopicMutationsApiTests` — реализовать `IAsyncLifetime`**

В файле `src/tests/KafkaWorker.IntegrationTests/Api/TopicMutationsApiTests.cs`:

а) сигнатуру класса (строка 16):

```csharp
public class TopicMutationsApiTests(KafkaApiFixture fixture)
```

заменить на:

```csharp
public class TopicMutationsApiTests(KafkaApiFixture fixture) : IAsyncLifetime
```

б) сразу после свойства `private Etcd.EtcdFixture Etcd => fixture.Etcd;` (строка 20) вставить:

```csharp
    // own-only: чистим только кластерные префиксы, созданные этим классом
    // (после каждого кейса — per-test teardown IAsyncLifetime: xUnit создаёт
    // экземпляр класса на каждый тест, DisposeAsync выполняется после
    // каждого кейса, а не однократно после всех — чистка только усиливается,
    // подписка на класс не требуется) при любом исходе (правило полной
    // самоочистки AGENTS.md); lifecycle-заявки topics/<t>/desired.* живут
    // под префиксом кластера и уходят вместе с ним. Безопасно: кейсы
    // самодостаточны, DeleteAsync по несуществующим ключам — no-op, методы
    // класса не паралеллятся. CancellationToken.None — токен теста к
    // teardown уже неактуален, удаления быстрые и идемпотентные.
    private static readonly string[] OwnClusterPrefixes =
    [
        "/kafka/clusters/events/", "/kafka/clusters/events2/",
        "/kafka/clusters/dx/", "/kafka/clusters/cx/",
    ];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var prefix in OwnClusterPrefixes)
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, prefix, prefix: true, CancellationToken.None);
    }
```

`using Xunit;` уже есть. Существующие предочистки внутри `PostTopic`/`DeleteTopic`/`CancelLifecycle` НЕ убирать (защита от остатков `KafkaSeedApiTests`, spec §4.2). Тела кейсов НЕ менять.

- [ ] **Шаг 2.3: Совместный прогон трёх классов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
dotnet test src/tests/KafkaWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName~ClusterMutationsApiTests|FullyQualifiedName~TopicMutationsApiTests|FullyQualifiedName~KafkaSeedApiTests"
```

Ожидание: сборка без ошибок; `Failed: 0, Passed: 24` (14 + 7 + 3 кейса). Если красно — анализ логов по правилам телеметрии (без перезапуска «для выяснения»).

- [ ] **Шаг 2.4: Зачистка docker после серии**

Те же команды, что в Шаге 1.3 (контроль `wc -l` → `0`; исключения стенда — `as-`/`pgw-stand-`/`deploy-`).

- [ ] **Шаг 2.5: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
git add src/tests/KafkaWorker.IntegrationTests/Api/ClusterMutationsApiTests.cs \
        src/tests/KafkaWorker.IntegrationTests/Api/TopicMutationsApiTests.cs && \
git commit -m "test(kafka-api): own-only teardown ClusterMutationsApiTests/TopicMutationsApiTests (t91): IAsyncLifetime.DisposeAsync (per-test — после каждого кейса, xUnit создаёт экземпляр класса на каждый тест) чистит только свои кластерные префиксы и точечные заявки при любом исходе (правило полной самоочистки AGENTS.md); общие префиксы /kafkaworker/... не выключаются, предочистки кейсов сохранены"
```

---

### Task 3: Ф3 — мерж-гейт-серия ×2 + юнит-серия + зачистка docker

**Files:** ничего не создаёт/не меняет — только прогоны и зачистка.

**Interfaces:**
- Consumes: правки Task 1 и Task 2 (закоммичены).
- Produces: доказательство нефлейковости (два зелёных прогона подряд одного фильтра) — основание для мерж-гейта.

- **Вход (предусловие):** Task 1 и Task 2 закоммичены; docker чист от посторонних контейнеров/сетей (кроме стендовых исключений).
- **Действие:** прогоны фильтра `Api|Etcd` дважды + юнит-серия, между сериями и после — зачистка docker.
- **Выход:** фильтр `Api|Etcd` (тот же, что вскрывал флейк 2026-09-14, 50/51) зелёный ДВАЖДЫ подряд; юнит-серия KafkaWorker зелёная; docker зачищен.
- **Проверка:** два прогона с `Failed: 0` + финальный контроль чистоты.
- **Спека:** §5 Ф3, принцип §3.5; критерии приёмки №3, №5, №6.

- [ ] **Шаг 3.1: Страховочная зачистка перед серией 1**

Команды Шага 1.3 + контроль: осиротевших тестовых сетей нет —

```bash
docker network ls --format '{{.Name}}' | grep -cE 'kfw-net|chainsruntime|testcontainers' || true
```

Ожидание: `0` (стендовые сети не матчатся паттерном; `|| true` — grep с нулем матчей возвращает код 1, это норма).

- [ ] **Шаг 3.2: Серия 1 — фильтр `Api|Etcd`**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
dotnet test src/tests/KafkaWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName~Api|FullyQualifiedName~Etcd"
```

Ожидание: `Failed: 0`; passed — все тесты фильтра (на момент постановки задачи в ветке их 51 — фиксация числа не цель, цель `Failed: 0`). Дождаться финальной строки прогона. Если красно — стоп: анализ логов/`last_error` по правилам телеметрии AGENTS.md (перезапуск без анализа запрещён, spec §7.3).

- [ ] **Шаг 3.3: Зачистка после серии 1**

Команды Шага 1.3 + контроль `wc -l` → `0`.

- [ ] **Шаг 3.4: Серия 2 — тот же фильтр**

Повторить команду Шага 3.2. Ожидание: `Failed: 0` — второй зелёный прогон и есть доказательство нефлейковости (spec §7.3).

- [ ] **Шаг 3.5: Зачистка после серии 2**

Команды Шага 1.3 + контроль `wc -l` → `0`.

- [ ] **Шаг 3.6: Юнит-серия KafkaWorker**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
dotnet test src/tests/KafkaWorker.UnitTests -c Release
```

Ожидание: `Failed: 0` (сборка без ворнингов — `TreatWarningsAsErrors`; юниты docker не требуют, зачистка после не нужна).

- [ ] **Шаг 3.7: Финальный контроль чистоты docker**

```bash
docker ps -a --format '{{.Names}}' | grep -vE '^(as-|pgw-stand-|deploy-)' | wc -l
docker network prune -f && docker network ls --format '{{.Name}}' | grep -cE 'kfw-net|chainsruntime|testcontainers' || true
```

Ожидание: `0` и `0` — 0 контейнеров помимо стендовых исключений (`as-`, `pgw-stand-`, `deploy-` — канонический список spec §5 Ф3/§7.6), осиротевших тестовых сетей нет, dev-стенд жив (критерий приёмки №6).

- [ ] **Шаг 3.8: Коммит не требуется**

Task 3 не меняет файлы (если план/отчёт задачи не правились). Если в `docs/superpowers/2026-09-14-fix-kafka-seed-api-flaky/` остались незакоммиченные spec/plan — закоммитить их отдельным коммитом `docs(t91): spec/plan флейка SeedDemo_AlreadySeeded_NoOp`.

---

### Task 4: Снятие roadmap-тега t91 (мерж-гейт)

**Files:**
- Modify: `arch/roadmap/kafkaworker.md:14-38` (пункт `- **`t91-kafka-seed-api-flaky`** — ...` — единственное вхождение тега в roadmap; после него файл заканчивается)

**Interfaces:**
- Consumes: Task 3 завершён (обе серии зелёные) — снятие тега возможно только у готовой к мержу задачи.
- Produces: чистый roadmap. Механика по обновлённому spec §6/§7.7: тег снимается **отдельным коммитом в feature-ветке в рамках мерж-гейта** и попадает в `main` **тем же мержем задачи** (прецедент t11: коммит `roadmap: t11 снят (мерж-гейт)` в ветке перед merge-коммитом).

- **Вход (предусловие):** Task 3 пройден (две зелёные серии `Api|Etcd` + юниты).
- **Действие:** удалить пункт t91 из `arch/roadmap/kafkaworker.md`, проверить отсутствие ссылок, закоммитить.
- **Выход:** пункт t91 удалён из `arch/roadmap/kafkaworker.md`; других упоминаний t91 в `arch/roadmap/` нет.
- **Проверка:** `grep -rn "t91" arch/roadmap/` → пусто.
- **Спека:** §6, §7.7 (roadmap-тег: отдельный коммит в feature-ветке в рамках мерж-гейта, в `main` — тем же мержем задачи, прецедент t11).

- [ ] **Шаг 4.1: Удалить пункт t91**

В `arch/roadmap/kafkaworker.md` удалить строки 14–38 — блок от `- **`t91-kafka-seed-api-flaky`** — флейм` до конца файла (конец блока — строка `  инфраструктура.`). После удаления файл заканчивается пунктом `t07-kafka-ca-rotation`.

- [ ] **Шаг 4.2: Проверить отсутствие ссылок на t91**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && grep -rn "t91" arch/roadmap/ ; echo "exit=$?"
```

Ожидание: пустой вывод, `exit=1` (нет матчей). Ссылок `←`-зависимостей на t91 нет (проверено при составлении плана).

- [ ] **Шаг 4.3: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-kafka-seed-api-flaky && \
git add arch/roadmap/kafkaworker.md && \
git commit -m "roadmap: t91-kafka-seed-api-flaky снят (мерж-гейт)"
```

- [ ] **Шаг 4.4: Далее — по dev-flow**

Мерж ветки в `main` (вместе с коммитом снятия тега) и пуш — ТОЛЬКО по явной просьбе пользователя (AGENTS.base §6). E2E Release PgWorker не требуется — prod-код воркеров не тронут (spec §5 Ф3).

---

## Самопроверка плана (выполнена при составлении и после правок по ревью)

- **Покрытие spec:** §4.1 → Task 1; §4.2 (списки префиксов/ключей дословно + per-test семантика) → Task 2; §4.3 → Global Constraints (запреты трогать соседей); §5 Ф1/Ф2/Ф3 → Task 1/2/3 (зачистка — с каноническим списком исключений `as-`/`pgw-stand-`/`deploy-`); §6 → Global Constraints + Task 4 (отдельный коммит в feature-ветке, в `main` тем же мержем, прецедент t11); §7 критерии 1–7 → проверки Шагов 1.2, 2.3, 3.2/3.4, 1.2, 3.6, 3.7, 4.2 соответственно.
- **Механика xUnit (по ревью):** Task 2 и комментарии вставок описывают per-test семантику (`DisposeAsync` после каждого кейса, экземпляр класса на каждый тест; чистка только усиливается; кейсы самодостаточны, no-op удаления, методы не паралеллятся); ссылка-прецедент — только `KafkaApiFixture` (`RestartApiTests.RestartHost` помечен как ручной `await using`, не прецедент).
- **Плейсхолдеры:** отсутствуют — все правки даны полным кодом, все команды точные.
- **Консистентность типов:** `IAsyncLifetime.InitializeAsync/DisposeAsync` — `ValueTask` (прецедент `KafkaApiFixture`); `DeleteAsync(endpoint, keyOrPrefix, prefix, ct)` — сигнатура `IEtcdGateway.cs:20`; имена `OwnClusterPrefixes`/`OwnTicketKeys` едины в обеих вставках одного класса.
