# Plan: Kafka PortAllocHealer, ветка 2 — RMW-merge вместо put-if-absent (ревью проекта #5)

> **Для агента-исполнителя:** план исполняет субагент **coder** (сильная модель,
> executor="coder" в `state.json`): он делает ВСЮ работу сам, включая подпункты.
> Подпункты написаны как самодостаточные задания «для слабой модели» (без знания
> проекта) — coder выполняет их по этой спецификации как есть. Шаги
> отслеживаются чекбоксами (`- [ ]`).

**Цель:** ветка 2 (`ReconstructAsync`) лестницы E9 закрепляет восстановленный из
docker inspect адрес в etcd при любом состоянии ключа `/kafkaworker/portalloc/<C>`
(пустой / частичный / конкурентный) — RMW по `mod_revision` с merge записей
соседей вместо `NotExists`-put на весь ключ.

**Архитектура:** замена put-if-absent на RMW-merge — паттерн уже реализован в
этой же лестнице, ветка 3 (`ReallocateAsync`): чтение с ревизией
(`ReadPortAllocWithRevisionAsync`) → merge → txn `CompareKeyUnchanged`
(ревизия 0 → `NotExists`) → проигрыш → reread (S5: первый записавший — истина;
нашей записи нет → `Failed` «retry on next tick»). Контракт меняется СНАЧАЛА в
`arch/16-kafkaworker.md` §5 C (arch-first), затем код, затем юнит-тест частичной
утеры.

**Спека:** `docs/superpowers/2026-10-21-fix-kafka-portalloc-partial-loss-unhealed/spec.md`
(план аргументирует от spec; исполнитель читает оба файла).

**Все относительные пути в командах — от корня worktree**
`/Users/demakaev/ZCodeProject/worktrees/fix-kafka-portalloc-partial-loss-unhealed`.

## Глобальные ограничения

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`
  (новый код не должен порождать warnings — сборка обязана быть чистой).
- Комментарии и документация — по-русски; идентификаторы — на английском;
  попадать в тон окружающего текста.
- ТОЛЬКО пункт ревью #5 (ветка 2 `ReconstructAsync`). Пункты #3, #6–#22 вне
  scope; атрибуции `(tNN)` в комментариях НЕ трогаются (пункт #7).
- Ветки 1/3 `PortAllocHealer`, `NodeSupervisor`, формат ключа portalloc
  (arch/15 §4), журнальные фазы, метрики — без изменений (`git diff` их не
  затрагивает).
- `arch/17` (E9) деталей txn ветки 2 не содержит — НЕ трогается.
- В `arch/16-kafkaworker.md` правится ТОЛЬКО формулировка лестницы E9 в §5 C:
  из десяти вхождений `put-if-absent` в файле удаляется ЕДИНСТВЕННОЕ вхождение
  §5 C (строка 537); остальные девять (разделы txn-кредов/CA: строки 267, 282,
  383–385, 398, 472, 738, 803) — вне scope и остаются как есть.
- Интеграционный сценарий частичной утери НЕ добавляется (решение spec #5);
  ветка 2 в полном стеке уже покрыта существующим
  `LostPortAlloc_Heals_ClusterStaysAlive` (Act 1).
- Коммиты — в feature-ветке worktree (свободно); мерж в main — только по
  явному приказу (вне этого плана).
- Длительные команды (build/test/docker-серии) запускать в фоне, полные логи —
  в файл, анализ — чтением файла (AGENTS.base §4, §13, §15); после КАЖДОЙ
  docker-серии — зачистка контейнеров/сетей (AGENTS.md).

## Review Focus

Пути входа, которые spec требует от ветки 2 (проверяются шагами плана; тесты
назначены по решениям spec §3.3/§6 — один новый юнит, интеграционный НЕ
добавляется):

1. Ключ жив, записи нашего брокера нет, конкуренции нет → RMW выигрывает,
   merge дописывает нашего, соседи не затёрты — новый юнит
   `Resolve_ContainerAlive_PartialLossMergesIntoLiveKey` (Шаг 3).
2. Ключа нет → ревизия 0 → `NotExists` → txn выигрывает — существующий
   `Resolve_ContainerAlive_ReconstructsPortAlloc` (без правок, Шаг 2).
3. Сосед записал НАШЕГО брокера между read и txn → compare проигрывает →
   reread-истина соседа (S5) — существующий
   `Resolve_ContainerAlive_TxnLostTakesForeignTruth` (без правок, Шаг 2).
4. Compare проиграл, нашего брокера в reread НЕТ → `Failed` «retry on next
   tick» — тест НЕ добавляется (решение spec #5); корректность пути
   верифицируется ревью кода в Шаге 2 (точный код зафиксирован в подпункте
   2.1, ветка `else` после `TryGetValue`).
5. Записи соседей переносятся в put как есть (merge, не перезапись ключа) —
   assert `pinned["broker2"]` нового юнита (Шаг 3).

---

## Шаг 1. arch/16 §5 C: контракт ветки 2 — RMW-merge (arch-first)

**Вход:** worktree на feature-ветке `fix-kafka-portalloc-partial-loss-unhealed`,
spec одобрен, файл `arch/16-kafkaworker.md` не изменён.

**Действие (кодер):** правка одного пассажа в `arch/16-kafkaworker.md`
(маркированный список «Backoff недоступного кластера», §5 C, строки 534–540):
фраза лестницы E9 о реконструкции меняется с put-if-absent на RMW-merge.
Механическая замена — подпункт 1.1. После правки кодер ревьюит результат по
критерию приёмки 1: формулировка put-if-absent удалена ИЗ §5 C (в §5 C больше
нет put-if-absent-семантики закрепления), семантика RMW-merge с merge соседей /
mod_revision / ревизия 0 = NotExists / проигрыш → re-read S5 / retry / соседи
не затираются — передана. Остальные девять вхождений `put-if-absent` в
`arch/16-kafkaworker.md` (разделы txn-кредов/CA, вне §5 C) не тронуты. Коммит
arch-first — сам кодер:

```
git add arch/16-kafkaworker.md
git commit -m "docs(arch): E9 ветка 2 — закрепление реконструкции portalloc как RMW-merge по mod_revision (ревью #5)"
```

**Выход:** arch/16 §5 C описывает закрепление ветки 2 как RMW-merge;
правка закоммичена РАНЬШЕ кода.

**Проверка (по состоянию файла ПОСЛЕ коммита, не по diff — diff обязан
содержать удалённую строку со старой формулировкой):**
1. `git show --stat HEAD` — единственный файл `arch/16-kafkaworker.md`.
2. `grep -c 'put-if-absent' arch/16-kafkaworker.md` → `9` (до правки
   вхождений 10; удалено ровно одно — строка 537 §5 C; оставшиеся 9 — разделы
   txn-кредов/CA, строки 267, 282, 383–385, 398, 472, 738, 803 — на месте).
3. `grep -c 'RMW `/kafkaworker/portalloc/<C>` под `locks/portalloc`' arch/16-kafkaworker.md`
   → `1` (новая формулировка §5 C на месте).
4. В добавленных строках diff (`git show HEAD -- arch/16-kafkaworker.md`)
   присутствуют `RMW`, `mod_revision`, `merge`, `S5`.

**Связь со spec:** §3.1 (правка arch/16), §4 фаза 1, решение #3 (arch-first),
критерий приёмки 1.

### Подпункт 1.1 (слабая модель): замена фразы в arch/16-kafkaworker.md

Файл: `arch/16-kafkaworker.md` (от корня worktree).

В блоке, начинающемся строкой «backoff ничего не пишет. **Лестница E9**
(arch/17): безадресные», замени ОДИН фрагмент.

Было (6 строк, целиком):

```
  Supervisable-брокеры — до любых деструктивных действий — portalloc →
  реконструкция из inspect живого контейнера (published-порт CLIENT + host,
  put-if-absent под `locks/portalloc`, проигрыш txn → re-read) → новая
  аллокация (S7-свидетельство смерти) + RMW endpoints; тупик «не закреплён
  в portalloc» устранён; клэйм занят → journal waiting-portalloc-lock
  (InProgress, следующий тик).
```

Стало (целиком):

```
  Supervisable-брокеры — до любых деструктивных действий — portalloc →
  реконструкция из inspect живого контейнера (published-порт CLIENT + host),
  закрепление — RMW `/kafkaworker/portalloc/<C>` под `locks/portalloc`:
  merge восстановленной записи с живыми записями соседей, txn по
  `mod_revision` (отсутствие ключа — ревизия 0 = NotExists); проигрыш txn →
  re-read (первый записавший — истина, S5; нашей записи нет — retry
  следующим тиком); записи соседей не затираются → новая аллокация
  (S7-свидетельство смерти) + RMW endpoints; тупик «не закреплён
  в portalloc» устранён; клэйм занят → journal waiting-portalloc-lock
  (InProgress, следующий тик).
```

Больше в файле ничего не менять. Внимание: в этом же файле есть ДРУГИЕ
вхождения текста «put-if-absent» (про txn-креды и CA, вне приведённого
фрагмента) — их НЕ трогать: заменяется только фрагмент выше. Текст — с
сохранением двухпробельного отступа маркированного списка и ширины строк
окружения.

---

## Шаг 2. Код: `ReconstructAsync` — RMW-merge вместо put-if-absent

**Вход:** Шаг 1 закоммичен (arch-first соблюдён).

**Действие (кодер):** три правки в
`src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs`, ТОЛЬКО внутри
ветки 2 (`ReconstructAsync`) и её комментариев:
- замена тела секции try (чтение → txn → reread) — подпункт 2.1;
- комментарий шапки класса, пункт 2 лестницы — подпункт 2.2;
- комментарий над `ReconstructAsync` — подпункт 2.3.

После правок кодер ревьюит дифф: ветки 1/3, `NodeSupervisor`, журнальные фазы,
метрики не затронуты; ветка `else` после `TryGetValue` (Failed «retry on next
tick») присутствует дословно — без неё успех без записи в etcd возвращался бы
при проигрыше без нашей записи (это и есть дефект). Коммит:

```
git add src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs
git commit -m "fix(kafkaworker): PortAllocHealer ветка 2 — RMW-merge portalloc вместо put-if-absent на весь ключ (ревью #5)"
```

**Выход:** `ReconstructAsync` закрепляет адрес merge-put по `mod_revision`
(ревизия 0 → NotExists); при проигрыше без нашей записи в reread — `Failed`
«retry on next tick»; записи соседей переносятся как есть.

**Проверка:**
1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   (в фоне, лог в файл) — успех, 0 warnings (TreatWarningsAsErrors).
2. Существующие тесты healer'а зелёные БЕЗ правок:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter FullyQualifiedName~PortAllocHealerTests`
   — все PASS (в т.ч. `ReconstructsPortAlloc`: ключа нет → ревизия 0 →
   NotExists → txn выигрывает; `TxnLostTakesForeignTruth`: NotExists
   проигрывает → reread 21050).
3. `git diff HEAD~1 -- src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs`
   — изменения только в `ReconstructAsync` и двух комментариях; строки
   `ReallocateAsync`, `ResolveAsync`, `UpdateEndpointsAsync`, `NodeId` и пр. не
   затронуты.

**Связь со spec:** §3.2, §4 фаза 2, решения #1/#2/#4/#8, критерии приёмки 2, 3, 4.

### Подпункт 2.1 (слабая модель): замена RMW-секции в ReconstructAsync

Файл: `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs` (от корня
worktree). Метод `ReconstructAsync` (после `if (!acquired.Value) ... return ...;`
идёт блок `try { ... } finally { await portLock.ReleaseAsync(); }`).

Внутри этого блока замени фрагмент.

Было (целиком, от `var key = ...` до закрывающей `}` ветвления `if (!txn.Value.Succeeded)`):

```csharp
            var key = PortAllocKey(cluster);
            var read = await ReadPortAllocAsync(cluster, ct);
            if (!read.IsSuccess)
                return Result<HealedAddress>.Failed(read.Error!);
            var merged = new Dictionary<string, NodeAddress>(read.Value);
            merged[broker] = address;
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<HealedAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5) и дальше по ней
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<HealedAddress>.Failed(reread.Error!);
                if (reread.Value.TryGetValue(broker, out var foreign))
                    address = foreign;
            }
```

Стало (целиком):

```csharp
            var key = PortAllocKey(cluster);
            var read = await ReadPortAllocWithRevisionAsync(cluster, ct);
            if (!read.IsSuccess)
                return Result<HealedAddress>.Failed(read.Error!);
            var (revision, pinnedAddresses) = read.Value;
            var merged = new Dictionary<string, NodeAddress>(pinnedAddresses);
            merged[broker] = address;
            var txn = await TxnAsync(
                TxnRequest.Of([CompareKeyUnchanged(key, revision)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<HealedAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5) и дальше по ней
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<HealedAddress>.Failed(reread.Error!);
                if (reread.Value.TryGetValue(broker, out var foreign))
                    address = foreign;
                else
                    return Result<HealedAddress>.Failed(new ApplicationException(
                        $"portalloc {cluster} changed under the claim — retry on next tick"));
            }
```

Используемые члены класса уже существуют в этом же файле: приватные методы
`ReadPortAllocWithRevisionAsync(string, CancellationToken)` (возвращает
`Result<(long Revision, IReadOnlyDictionary<string, NodeAddress> Addresses)>`),
`CompareKeyUnchanged(string, long)`, `PortAllocKey(string)`,
`SerializePortAlloc(...)`, `ReadPortAllocAsync(...)`, `TxnAsync(...)`.
Новые методы/поля НЕ вводить; остальной файл не трогать. Блок
`finally { await portLock.ReleaseAsync(); }` и код ПОСЛЕ него
(AdvertisedClient-контроль, фаза `reconstructed`, `return ... Success(...)`)
оставить без изменений.

### Подпункт 2.2 (слабая модель): комментарий шапки класса, пункт 2 лестницы

Файл: `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs` (от корня
worktree). Комментарий над `public sealed record HealedAddress...` (строки 12–19
файла).

Было (строки 14–16 этого комментария, целиком):

```
// 2) журнала нет, контейнер есть (положительная инспекция) → реконструкция
//    из docker inspect (published-порт + host) клэйм-txn version==0;
//    контейнер не трогаем — данные неприкосновенны;
```

Стало (целиком):

```
// 2) журнала нет, контейнер есть (положительная инспекция) → реконструкция
//    из docker inspect (published-порт + host) RMW-merge по mod_revision
//    (merge с живыми записями соседей); контейнер не трогаем — данные
//    неприкосновенны;
```

Остальные строки комментария (пункты 1 и 3, строка с `// Лестница источников...`)
не трогать.

### Подпункт 2.3 (слабая модель): комментарий над методом ReconstructAsync

Файл: `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs` (от корня
worktree). Комментарий из 4 строк непосредственно над
`private async Task<Result<HealedAddress>> ReconstructAsync(`.

Было (целиком):

```csharp
    // Ветка 2: запись восстановленного закрепления put-if-absent (version==0)
    // под глобальным клэймом; проигрыш txn → re-read (первый записавший —
    // истина, S5). Контейнер НЕ трогаем. Контроль: клиентский порт advertised
    // env == published — расхождение journal-warning (канон — PortBindings).
```

Стало (целиком):

```csharp
    // Ветка 2: запись восстановленного закрепления — RMW-merge по mod_revision
    // под глобальным клэймом (merge с живыми записями соседей; отсутствие
    // ключа — ревизия 0 = NotExists); проигрыш txn → re-read (первый
    // записавший — истина, S5; нашей записи нет — retry следующим тиком).
    // Контейнер НЕ трогаем. Контроль: клиентский порт advertised
    // env == published — расхождение journal-warning (канон — PortBindings).
```

---

## Шаг 3. Юнит-тест: частичная утеря — merge в живой ключ

**Вход:** Шаг 2 закоммичен; серия `PortAllocHealerTests` зелёная.

**Действие (кодер):** добавить в
`src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs` один
новый тест — подпункт 3.1 (точный код, место вставки задано). Существующие
тесты и риг НЕ менять. После вставки кодер ревьюит: тест ловит дефект — при
откате к NotExists-ветке (старый код) txn проиграет живому ключу, reread не
содержит `broker1`, успех вернётся без записи → `pinned["broker1"]` падает
KeyNotFoundException (мутационный прогон НЕ выполнять; решение spec #5 —
один тест). Коммит:

```
git add src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs
git commit -m "test(kafkaworker): юнит частичной утери portalloc — RMW-merge в живой ключ (ревью #5)"
```

**Выход:** тест `Resolve_ContainerAlive_PartialLossMergesIntoLiveKey` в
`PortAllocHealerTests`; вся серия `KafkaWorker.UnitTests` зелёная.

**Проверка:**
1. Точечно новый тест:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter FullyQualifiedName~PartialLossMergesIntoLiveKey`
   — PASS.
2. Полная серия юнитов:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release`
   — все PASS (в фоне, лог в файл, дождаться финальной строки).

**Связь со spec:** §3.3, §4 фаза 3, решения #5/#7, критерий приёмки 5.

### Подпункт 3.1 (слабая модель): добавить юнит-тест в PortAllocHealerTests

Файл: `src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs`
(от корня worktree). Вставь НОВЫЙ тестовый метод НЕПОСРЕДСТВЕННО ПОСЛЕ
закрывающей `}` метода `Resolve_ContainerAlive_TxnLostTakesForeignTruth`
(после его последней строки `rig.Driver.Ensured.Should().BeEmpty();` и
закрывающей фигурной скобки метода; следующий за местом вставки метод —
`Resolve_PortLockBusy_NoMutations` с комментарием «AAA: клэйм занят…»).

Вставляемый код (целиком, с пустой строкой-разделителем до и после):

```csharp
    // AAA: ветка 2, частичная утеря — ключ portalloc жив с записью соседа
    // broker2, записи нашего broker1 нет: RMW-merge дописывает broker1
    // адресом инспекции, запись соседа не затёрта, контейнер не тронут.
    [Fact]
    public async Task Resolve_ContainerAlive_PartialLossMergesIntoLiveKey()
    {
        // Arrange: ключ жив с чужой записью broker2 (нашей broker1 нет);
        // инспекция broker1 засеяна (живой контейнер).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker2":{"host":"h2","client":21020}}""");
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection(
            "h1", 21037, "CLIENT://host.docker.internal:21037");

        // Act: лестница для broker1.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: merge — ключ содержит и broker1 (адрес инспекции), и
        // соседа broker2 (запись не затёрта); контейнер не тронут.
        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21037));
        resolved.Value.Recreated.Should().BeFalse("a live container is left untouched");
        var pinned = await ReadPortAllocAsync(rig.Etcd);
        pinned["broker1"].Should().Be(new NodeAddress("h1", 21037),
            "reconstructed address is pinned in etcd");
        pinned["broker2"].Should().Be(new NodeAddress("h2", 21020),
            "neighbor records survive the merge");
        rig.Driver.Ensured.Should().BeEmpty("a live container is left untouched");
        rig.Driver.Removed.Should().BeEmpty("a live container is left untouched");
    }
```

Используемые члены уже существуют в этом тестовом классе: приватный статический
хелпер `ReadPortAllocAsync(Fakes.FakeEtcd etcd)` (возвращает
`Task<IReadOnlyDictionary<string, NodeAddress>>`, парсит JSON ключа
`/kafkaworker/portalloc/events`), фабрика `NewRig()` (порт-окно 21000–21100,
кластер `events`, брокер `broker1`), поля рига `Etcd` (`Fakes.FakeEtcd` с
методом `Seed(string key, string value)`), `Driver` (`Fakes.FakeKafkaDriver`,
словарь `Endpoints`, списки `Ensured`/`Removed`), `Healer`, `Snapshot`,
`Addresses`. Новых членов НЕ вводить; существующие тесты и риг не менять.

---

## Шаг 4. Прогоны: интеграция KafkaWorker (docker) + E2E-маркер PgWorker

**Вход:** Шаги 1–3 закоммичены; полная серия `KafkaWorker.UnitTests` зелёная.

**Действие (кодер):** последовательные прогоны (каждый — в фоне, полный лог в
файл; между docker-сериями — зачистка по AGENTS.md; тесты идут дольше 5 минут —
онлайн-анализ логов по правилам AGENTS.base §13):

1. Образы внешних зависимостей: `bash dev-stand/images/pull-images.sh`
   (подготовка docker-тестов по AGENTS.md).
2. Сборка Release ОТДЕЛЬНОЙ командой (свежесть бинаря — часть гейта):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`.
3. Полная серия интеграции KafkaWorker (docker):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~KafkaWorker.IntegrationTests`.
4. Зачистка после серии: удалить остаточные контейнеры префиксов `kfw-`/`pgw-`
   (`docker ps -a --format '{{.Names}}' | grep -E '^(kfw-|pgw-)'` →
   `docker rm -f ...`, при пустом списке — ничего), затем
   `docker network prune -f` (осиротевшие `kfw-net-*`; сети живого dev-станда
   используются и не удаляются). Диагностика до/после:
   `docker network ls | grep -c kfw-net`.
5. E2E-подготовка PgWorker ОДНИМ процессом ДО тестов:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`.
6. E2E-маркер (свежий Release, `--no-build`):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`.
7. Зачистка после E2E-серии — та же, что п. 4.

**Выход:** интеграционная серия `KafkaWorker.IntegrationTests` зелёная;
E2E-маркер `Scale_AddEmptyShard` зелёный на свежем Release; остаточных
контейнеров/сетей тестовых серий нет.

**Проверка:** финальные строки логов обоих прогонов — «Passed!» / 0 failed
(логи читать из файлов полностью); после зачисток
`docker ps -a --format '{{.Names}}' | grep -E '^(kfw-|pgw-)'` — пусто,
`docker network ls | grep -c kfw-net` — 0.

**Связь со spec:** §4 фаза 4, решения #6, критерии приёмки 6, 7.

Подпунктов нет (запуск/наблюдение/зачистка — работа сильной модели: наблюдение
за прогонами, разбор логов, реакция на зависания).

---

## Шаг 5. Гигиена ревью: удалить пункт #5 из `.my/review_project.md`

**Вход:** Шаги 1–4 завершены (дефект закрыт и проверен прогонами).

**Действие (кодер):** удалить закрытый пункт #5 из файла
`/Users/demakaev/ZCodeProject/pg/.my/review_project.md` — ВНИМАНИЕ: файл
лежит в ОСНОВНОМ дереве `/Users/demakaev/ZCodeProject/pg/` (untracked, в git
не входит, в worktree отсутствует) — править именно его, коммита нет:
1. в сводной таблице «Что найдено» удалить строку
   `| [5](#p5) | 🟡 Minor | Л1 | Kafka PortAllocHealer, ветка 2: put-if-absent на весь ключ — частичная утеря записи portalloc не чинится никогда |`;
2. в разделе «Детально» удалить блок детального описания #5 целиком: от строки
   `<a id="p5"></a>` до строки `**Фикс:** RMW по mod_revision с merge (как в `ReallocateAsync` стр. 176–178) вместо NotExists-put.`
   включительно. Разделитель `---` перед `<a id="p6"></a>` — ОДИН, не задвоить.
3. Остальные пункты НЕ перенумеровывать (AGENTS.base §16).

**Выход:** в `.my/review_project.md` нет пункта #5 (ни в сводке, ни в детально);
остальные пункты с исходными номерами.

**Проверка:** `grep -n 'p5\|#5' /Users/demakaev/ZCodeProject/pg/.my/review_project.md`
— нет вхождений пункта #5 (кроме, возможно, несвязанных); `grep -c 'id="p' …`
уменьшился на 1.

**Связь со spec:** критерий приёмки 8, AGENTS.base §16.

Подпунктов нет (правка вне репозитория, требует понимания границ блока).

---

## Итоговая самопроверка плана (исполнена составителем)

- Покрытие spec: §3.1 → Шаг 1; §3.2 → Шаг 2; §3.3 → Шаг 3; §4 фаза 4 и §7
  п. 6–7 → Шаг 4; §7 п. 8 → Шаг 5. Вне scope шагов ничего нет.
- Все имена/сигнатуры — из реального кода:
  `ReadPortAllocWithRevisionAsync`, `CompareKeyUnchanged`, `TxnRequest.Of`,
  `TxnOp.Put`, `SerializePortAlloc`, `PortAllocKey`, `ReadPortAllocAsync`,
  `TxnAsync`; тестовые `NewRig`, `rig.Etcd.Seed`, `rig.Driver.Endpoints`,
  `NodeEndpointInspection`, `ReadPortAllocAsync(rig.Etcd)`.
- Проверки шагов выполнимы и однозначны: Шаг 1 проверяет СОСТОЯНИЕ файла
  после коммита (`put-if-absent` → ровно 9 из исходных 10: удалено
  единственное вхождение §5 C (строка 537), девять вхождений разделов
  txn-кредов/CA не тронуты), а не diff (diff обязан содержать удалённую
  строку).
- Развилок нет: механика ветки 2, тексты arch/комментариев, код теста, команды
  и порядок прогонов, границы удаления в ревью — зафиксированы дословно.
- Двухуровневость: подпункты 1.1, 2.1, 2.2, 2.3, 3.1 — самодостаточные
  механические задания; ревью/прогоны/зачистка — в тексте шагов (сильная
  модель).
