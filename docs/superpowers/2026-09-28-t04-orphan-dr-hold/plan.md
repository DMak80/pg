# t04-orphan-dr-hold — план реализации (защита DR-источника от сиротского TTL)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ суб-скилл: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять план задача за задачей.
> Шаги помечены чекбоксами (`- [ ]`) для трекинга.

**Цель:** исключить потерю единственного DR-источника автоматикой сиротского TTL:
сирота с валидным полным (`full/<id>/backup_manifest`) автоматикой не удаляется
никогда; оператор управляет защитой (hold/unhold) и осознанным удалением
(confirm-заявка) через API воркера и панель.

**Архитектура:** чистые функции `OrphanRegistry` (детект полных, merge-перенос
`has_valid_full`, TTL-отбор с hold-множеством) → глобальный лидер-проход
`BackupOrphanSweeper` (санитар чужих hold/заявок, приоритет кандидатов
«доводка DELETING → заявка → TTL», del заявки в доводке) → API воркера
(put/del per-префиксных ключей `orphan-holds`/`orphan-deletes`, гварды
400/404/409/503) → панель (снапшот-парсинг, DTO/бейджи/алерт, команды-прокси,
UI-кнопки) → runbook + docker-E2E. Single-writer сохранён: реестр пишет только
лидер-проход, API пишет только свои per-префиксные ключи атомарными put/del.

**Стек:** .NET 10 (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), xUnit + FluentAssertions (AAA-комментарии в
тестах обязательны), React/Mantine-фронтенд панели (`frontend/`), docker-E2E
(testcontainers, динамические порты).

**Спека:** [`spec.md`](spec.md) (рядом; канон контракта —
[`arch/19-backups.md`](../../../arch/19-backups.md) §4/§10 и
[`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md)
§2.3.1/§2.5/§9.10 — уже обновлены коммитом спеки; код зеркалит их).

## Глобальные ограничения

- Все правки — ТОЛЬКО в worktree `feat-t04-orphan-dr-hold` (ветка
  `feat-t04-orphan-dr-hold`); коммит в feature-ветке свободен, мерж в `main` —
  только по явной просьбе пользователя.
- Реестр `/pgworker/backups/orphans` пишет ТОЛЬКО глобальный лидер-проход;
  API воркера пишет только `/pgworker/backups/orphan-holds/<C>/<X>` и
  `/pgworker/backups/orphan-deletes/<C>/<X>` атомарными put/del одного ключа;
  панель в etcd НЕ пишет (прокси в API воркера).
- Новых опций конфигурации нет; `Options.cs` не меняется; автоправило
  «валидный полный держит» не отключается; `OrphanTtlSec=0` — только доводка
  DELETING и заявки (алерт-семантика прежняя).
- Форматы ключей — дословно из arch/19 §4: hold
  `{"set_unix":T,"set_by":"operator|panel"}`, заявка
  `{"requested_unix":T,"requested_by":"operator|panel"}`, запись реестра
  получает `"has_valid_full":bool`.
- Критерий валидного полного: объект `full/<id>/backup_manifest` в префиксе
  (ключ `<C>/<X>/full/<id>/backup_manifest`: 5 сегментов, 3-й `full`,
  последний `backup_manifest`) — из уже снимаемого list bucket, ноль лишних
  запросов к S3. Механику restore/DR-выбора не трогаем.
- Один удаляемый префикс за проход sweeper'а — прежний темп.
- Комментарии и документация — по-русски; идентификаторы — по-английски; тесты
  — с AAA-комментариями (Arrange/Act/Assert).
- E2E/интеграции: своё окружение с guid-именами, динамические порты, полный
  teardown при любом исходе, ассерт чистоты; после КАЖДОЙ docker-серии —
  зачистка (контейнеры/сети, `docker network prune -f` при осиротевших
  `kfw-net-*`/`pgw-*-net`); перезапуск упавших тестов «посмотреть что было»
  запрещён — разбор по снятым логам.
- Все прогоны `dotnet test` — с `DOTNET_CLI_UI_LANGUAGE=en`.

---

### Task 1: Чистые функции `OrphanRegistry` — `HasValidFull`, детект полных, merge-перенос, TTL-отбор с hold-множеством

**Файлы:**
- Modify: `src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`
- Modify: `src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` (только адаптация трёх вызовов — поведение прохода не меняется в этом таске)
- Test: `src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs`

**Интерфейсы (produces):**
- `OrphanEntry(string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, OrphanState State, bool HasValidFull = false)`
- `record PrefixGrouping(IReadOnlyDictionary<string, long> Sizes, IReadOnlySet<string> FullPrefixes)`
- `static PrefixGrouping GroupPrefixes(IReadOnlyList<S3ObjectInfo> objects)` — ЗАМЕНЯЕТ `GroupShardPrefixes` (старая удаляется)
- `static Registry Merge(Registry? current, IReadOnlyDictionary<string, long> observedSizes, IReadOnlySet<string> observedFulls, IReadOnlySet<(string Cluster, string Shard)> liveShards, IReadOnlySet<string> liveClusters, long nowUnix)` — новый параметр `observedFulls` третий по счёту
- `static string? SelectTtlCandidate(Registry registry, long ttlSec, long nowUnix, IReadOnlySet<string> heldPrefixes)` — новый обязательный последний параметр
- `const string HoldsPrefix = "/pgworker/backups/orphan-holds/"`, `const string DeletesPrefix = "/pgworker/backups/orphan-deletes/"`
- `static string HoldKey(string prefix)`, `static string DeleteKey(string prefix)`
- `static string HoldToJson(long setUnix, string setBy)`, `static string DeleteToJson(long requestedUnix, string requestedBy)`
- `SameOrphans` сравнивает и `HasValidFull`
- Consumes: существующие `S3ObjectInfo`, `OrphanState`, `Registry`.

- [ ] **Шаг 1: Написать падающие юнит-тесты** (добавить в
  `OrphanRegistryTests.cs`; существующие тесты обновить под новые сигнатуры:
  `GroupShardPrefixes_только_нашей_формы` → `GroupPrefixes_только_нашей_формы_и_детект_полных`,
  вызовы `Merge`/`SelectTtlCandidate` дополнить новыми аргументами)

```csharp
// AAA (AC1/AC9): детект валидных полных из list-ключей — только
// full/<id>/backup_manifest (5 сегментов, 3-й full, последний backup_manifest)
[Fact]
public void GroupPrefixes_детектирует_полные_по_manifest()
{
    // Arrange — в префиксе c1/s1 есть manifest, в c2/s2 — только base.tar
    var objects = new List<S3ObjectInfo>
    {
        Obj("c1/s1/full/20260911110000Z/backup_manifest"),
        Obj("c1/s1/full/20260911110000Z/base.tar", 30),
        Obj("c2/s2/full/20260911110000Z/base.tar", 20),
        Obj("c3/s3/full/20260911110000Z/backup_manifest.extra"), // не manifest
        Obj("c3/s3/fullx/20260911110000Z/backup_manifest"),      // 3-й сегмент не full
        Obj("root/full/20260911110000Z/backup_manifest"),        // имя не нашей формы
    };

    // Act
    var grouped = OrphanRegistry.GroupPrefixes(objects);

    // Assert — полные только в c1/s1; размеры по всем префиксам нашей формы
    grouped.FullPrefixes.Should().BeEquivalentTo(["c1/s1"]);
    grouped.Sizes["c1/s1"].Should().Be(40);
    grouped.Sizes.Should().ContainKey("c2/s2");
}

// AAA (AC1): merge — has_valid_full из свежего наблюдения; ненаблюдаемый
// сирота сохраняет прежнее значение (transient list не роняет защиту)
[Fact]
public void Merge_HasValidFull_свежее_наблюдение_и_перенос()
{
    // Arrange — current: две записи, обе с has_valid_full=true
    var current = new OrphanRegistry.Registry(
    [
        new OrphanEntry("c1/s1", "cluster", 10, Now - 86400, OrphanState.Observed, HasValidFull: true),
        new OrphanEntry("c2/s2", "cluster", 20, Now - 86400, OrphanState.Observed, HasValidFull: true),
    ], Now);
    // Act — c1/s1 наблюдается БЕЗ полных (полный удалён), c2/s2 не наблюдается
    var merged = OrphanRegistry.Merge(
        current,
        new Dictionary<string, long> { ["c1/s1"] = 10 },
        new HashSet<string>(), // observedFulls пуст
        Shards(), Clusters(), Now);

    // Assert — наблюдаемый пересчитан в false, ненаблюдаемый перенёс true
    merged.Orphans.Single(e => e.Prefix == "c1/s1").HasValidFull.Should().BeFalse();
    merged.Orphans.Single(e => e.Prefix == "c2/s2").HasValidFull.Should().BeTrue("защита переживает transient list");
}

// AAA (AC1/AC2/AC3): TTL-отбор исключает защищённых; ttl=0 — только доводка
[Fact]
public void SelectTtlCandidate_защищённые_не_кандидаты()
{
    // Arrange — три просроченных OBSERVED: с полным, с hold, чистый
    var registry = new OrphanRegistry.Registry(
    [
        new OrphanEntry("c1/s1", "cluster", 10, Now - 8 * 86400, OrphanState.Observed, HasValidFull: true),
        new OrphanEntry("c2/s2", "cluster", 20, Now - 8 * 86400, OrphanState.Observed),
        new OrphanEntry("c3/s3", "cluster", 30, Now - 8 * 86400, OrphanState.Observed),
    ], Now);
    var held = new HashSet<string> { "c2/s2" };

    // Act / Assert — кандидат только незащищённый c3/s3
    OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now, held).Should().Be("c3/s3");
    // все защищены → кандидата нет
    OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now,
        new HashSet<string> { "c2/s2", "c3/s3" }).Should().BeNull();
    // ttl=0 → TTL-кандидатов нет (только доводка DELETING — прежняя семантика)
    OrphanRegistry.SelectTtlCandidate(registry, 0, Now, held).Should().BeNull();
}

// AAA (AC9): старый JSON без has_valid_full парсится (поле → false); roundtrip
// с полем симметричен; SameOrphans чувствителен к изменению has_valid_full
[Fact]
public void Parse_без_поля_HasValidFull_false_и_SameOrphans_чувствителен()
{
    // Arrange — реестр, записанный старым кодом (без has_valid_full)
    var legacy = """{"orphans":[{"prefix":"a/b","kind":"shard","size_bytes":1,"first_seen_unix":1,"state":"OBSERVED"}],"updated_unix":1}""";

    // Act / Assert — парсится, поле false
    var parsed = OrphanRegistry.Parse(legacy);
    parsed.Should().NotBeNull();
    parsed!.Orphans[0].HasValidFull.Should().BeFalse("старый ключ — ближайший проход пересчитает");

    // roundtrip нового формата: has_valid_full сериализуется и читается
    var registry = new OrphanRegistry.Registry(
        [new OrphanEntry("a/b", "shard", 1, 1, OrphanState.Observed, HasValidFull: true)], 2);
    var json = OrphanRegistry.ToJson(registry);
    json.Should().Contain("\"has_valid_full\":true");
    OrphanRegistry.SameOrphans(registry, OrphanRegistry.Parse(json)!).Should().BeTrue();
    OrphanRegistry.SameOrphans(registry, new OrphanRegistry.Registry(
        [new OrphanEntry("a/b", "shard", 1, 1, OrphanState.Observed)], 2)).Should()
        .BeFalse("изменение has_valid_full — повод для put");
}

// AAA (AC6): ключи/JSON hold-заявки канона arch/19 §4
[Fact]
public void HoldDelete_ключи_и_JSON_канона()
{
    // Act
    var holdKey = OrphanRegistry.HoldKey("c1/s1");
    var deleteKey = OrphanRegistry.DeleteKey("c2/s2");

    // Assert — формат канона
    holdKey.Should().Be("/pgworker/backups/orphan-holds/c1/s1");
    deleteKey.Should().Be("/pgworker/backups/orphan-deletes/c2/s2");
    OrphanRegistry.HoldToJson(1757764800, "operator").Should()
        .Be("""{"set_unix":1757764800,"set_by":"operator"}""");
    OrphanRegistry.DeleteToJson(1757764800, "panel").Should()
        .Be("""{"requested_unix":1757764800,"requested_by":"panel"}""");
}
```

- [ ] **Шаг 2: Прогнать тесты — убедиться в падении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.UnitTests.Backups.OrphanRegistryTests"`
Ожидание: FAIL — «`OrphanRegistry` не содержит `GroupPrefixes`/`HoldKey`...», CS1061/CS1501 на новых вызовах. Существующие вызовы `Merge`/`SelectTtlCandidate` в sweeper тоже сломаются — это ожидаемо, правим в шаге 3.

- [ ] **Шаг 3: Реализовать изменения в `OrphanRegistry.cs`**

1. `OrphanEntry` — добавить хвостовой параметр `bool HasValidFull = false`.
2. `OrphanPayload` — добавить
   `[property: JsonPropertyName("has_valid_full")] bool? HasValidFull`; в
   `Parse` — `HasValidFull = o.HasValidFull ?? false` (толерантность к
   отсутствию — AC9); в `ToJson` — писать `e.HasValidFull` как bool всегда.
3. Добавить `PrefixGrouping` и `GroupPrefixes` (один проход по объектам:
   существующая логика размеров + `parts.Length == 5 && parts[2] == "full" &&
   parts[4] == "backup_manifest"` → `fulls.Add(prefix)`); УДАЛИТЬ
   `GroupShardPrefixes`.
4. `Merge` — новый параметр `IReadOnlySet<string> observedFulls`:
   наблюдаемая запись → `HasValidFull = observedFulls.Contains(e.Prefix)`
   (в ветке `e with { ... }`); ненаблюдаемая → перенос как есть; новая
   запись → `HasValidFull = observedFulls.Contains(prefix)`.
5. `SelectTtlCandidate` — новый последний параметр
   `IReadOnlySet<string> heldPrefixes`; условие TTL-кандидата:
   `e.State == OrphanState.Observed && nowUnix - e.FirstSeenUnix > ttlSec
   && !e.HasValidFull && !heldPrefixes.Contains(e.Prefix)`.
6. `SameOrphans` — добавить `&& p.x.HasValidFull == p.y.HasValidFull`.
7. Добавить константы `HoldsPrefix`/`DeletesPrefix`, функции `HoldKey`/
   `DeleteKey`/`HoldToJson`/`DeleteToJson` (payload-record + общий
   `JsonOptions`, как у реестра; сериализация — строго
   `{"set_unix":…,"set_by":"…"}` / `{"requested_unix":…,"requested_by":"…"}`).

Синхронно адаптировать три вызова в `BackupOrphanSweeper.SweepAsync`
(поведение не меняется — полных в этих тестах нет, hold-множество пусто):

```csharp
var grouped = OrphanRegistry.GroupPrefixes(listed.Value);
var observed = grouped.Sizes;
// ...
var merged = OrphanRegistry.Merge(
    current, observed, grouped.FullPrefixes, liveShards, liveClusters, nowUnix);
// ...
var candidate = OrphanRegistry.SelectTtlCandidate(
    merged, options.SupervisorOrphanTtlSec, nowUnix, heldPrefixes: []);
```

- [ ] **Шаг 4: Прогнать тесты — убедиться в прохождении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.UnitTests.Backups.OrphanRegistryTests"`
Ожидание: PASS (все, включая обновлённые старые).
Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupOrphanSweeperTests"`
Ожидание: PASS (реальный etcd + FakeS3; поведение прохода не изменилось).

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.Backups/Supervisor/OrphanRegistry.cs src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs
git commit -m "t04: OrphanRegistry — has_valid_full, детект полных, TTL-отбор с hold-множеством (чистые функции)"
```

---

### Task 2: Sweeper — санитар hold/заявок, приоритет кандидатов, del заявки в доводке

**Файлы:**
- Modify: `src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`

**Интерфейсы (produces):** поведение `SweepAsync` (шаги 3.5/4/5–7 из spec §3.3):
после merge — санитар гасит hold/заявки с префиксом вне merged-реестра (журнал
`orphan-key-cleaned/<prefix>`); отбор: первый DELETING → первая заявка
`orphan-deletes` (по ordinal ключа; журнал `orphan-delete-requested/<prefix>`
до перехода в DELETING) → `SelectTtlCandidate` с hold-множеством; в финале
доводки по заявке — del ключа заявки.
**Consumes:** `OrphanRegistry.HoldsPrefix/DeletesPrefix/HoldKey/DeleteKey` (Task 1).

- [ ] **Шаг 1: Написать падающие интеграционные тесты** (добавить в
  `BackupOrphanSweeperTests.cs`; сид-расширение — посев hold/заявок через
  `fixture.Gateway.PutAsync`). Каждый тест — AAA-комментарий.

```csharp
// Хелпер сида: put hold-ключа/заявки канона (AAA-Act общий).
private async Task PutHoldAsync(string prefix, long unix = 1757764800)
    => await fixture.Gateway.PutAsync(fixture.Endpoint, OrphanRegistry.HoldKey(prefix),
        OrphanRegistry.HoldToJson(unix, "operator"), null, TestContext.Current.CancellationToken);

private async Task PutDeleteRequestAsync(string prefix, long unix = 1757764800)
    => await fixture.Gateway.PutAsync(fixture.Endpoint, OrphanRegistry.DeleteKey(prefix),
        OrphanRegistry.DeleteToJson(unix, "operator"), null, TestContext.Current.CancellationToken);

private async Task<bool> KeyExistsAsync(string key)
    => (await fixture.Gateway.GetAsync(
        fixture.Endpoint, key, TestContext.Current.CancellationToken)).Value is not null;
```

Кейсы (полные тела по образцу существующих тестов файла —
`SeedAsync`/`BuildSweeper`/`MutableClock`, сжатый `Options(ttl: 60)`):

1. `Проход_полный_в_префиксе_держит_сироту_после_TTL` (AC1) — сид:
   `s3.PrefixObjects` = `ghost/s1/full/20260911110000Z/base.tar` +
   `ghost/s1/full/20260911110000Z/backup_manifest`; первый проход — запись
   OBSERVED с `HasValidFull=true`; `clock.Now += 2 мин`; второй проход —
   объекты живы (`s3.DeletedKeys` пуст), запись осталась OBSERVED с
   `HasValidFull=true`.
2. `Проход_hold_держит_незащищённую_сироту` (AC2) — сид без полных; после
   первого прохода `PutHoldAsync("ghost/s1")`; `clock.Now += 2 мин`; проход —
   объекты живы, запись жива.
3. `Проход_unhold_возвращает_под_TTL` (AC3) — как кейс 2, но затем del
   hold-ключа (`DeleteAsync`) и ещё проход (время не двигаем) — объекты
   удалены, запись погашена.
4. `Проход_заявка_удаляет_защищённую_и_гасится` (AC4) — сирота с полным
   (manifest) И hold-ключом; `PutDeleteRequestAsync("ghost/s1")`; `clock.Now`
   вперёд; проход — объекты удалены, записи нет, ключа заявки нет
   (`KeyExistsAsync(OrphanRegistry.DeleteKey(...))` — false).
5. `Проход_воскресший_владелец_гасит_hold_и_заявку` (AC5) — сирота + hold +
   заявка; воскресить владельца (put `/clusters/ghost/config` +
   `/clusters/ghost/shards/s1/replicas` — по образцу существующего теста
   `Проход_владелец_воскрес_запись_гаснет`); проход — записи нет, hold-ключ
   удалён, ключ заявки удалён, объекты живы.
6. `Проход_санитар_чистит_чужие_ключи` (AC5/спека §2 п.6) — hold и заявка на
   префиксы, которых нет в реестре (`ghost/s1` пуст в S3; ключи
   `.../other/s9`); один проход — оба ключа удалены.
7. `Проход_ненаблюдаемый_сирота_сохраняет_has_valid_full` (AC1, transient
   list) — первый проход с manifest-объектом; затем
   `s3.PrefixObjects.Clear()` (list пуст — transient); второй проход — запись
   жива, `HasValidFull` остался true, объекты «не удалялись» (кандидата нет).

- [ ] **Шаг 2: Прогнать — убедиться в падении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupOrphanSweeperTests"`
Ожидание: FAIL новых кейсов (сирота с полным удаляется по TTL, санитар не
чистит, заявка не удаляет). После прогона — зачистить docker-остатки серии
(`docker ps -a --format '{{.Names}}' | grep -c pgw-` → при ненулевых
остатках разобрать; `docker network ls | grep pgw` — осиротевшие сети
`docker network prune -f`).

- [ ] **Шаг 3: Реализовать изменения `SweepAsync`** (нумерация комментариев
  продолжает существующие; новый код — между merge-put и TTL-отбором)

```csharp
// (3.5) Санитар: hold/заявки, чей префикс вне merged-реестра (воскрес/
// исчез/уже удалён), гасятся — «висящих» флагов не копится (spec §3.3).
var holdsRange = await RangeKeysAsync(OrphanRegistry.HoldsPrefix, ct);
if (!holdsRange.IsSuccess)
    return holdsRange;
var deletesRange = await RangeKeysAsync(OrphanRegistry.DeletesPrefix, ct);
if (!deletesRange.IsSuccess)
    return deletesRange;
var registryPrefixes = merged.Orphans
    .Select(e => e.Prefix).ToHashSet(StringComparer.Ordinal);
var heldPrefixes = new HashSet<string>(StringComparer.Ordinal);
foreach (var kv in holdsRange.Value)
{
    var prefix = PrefixOfKey(kv.Key); // «<C>/<X>» — 2 последних сегмента ключа
    if (registryPrefixes.Contains(prefix))
        heldPrefixes.Add(prefix);
    else if (await DeleteKeyAsync(kv.Key, ct) is { IsSuccess: false } delHold)
        return delHold; // transient — следующий проход повторит
    else
        await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
            $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
}
string? requested = null; // префикс первой заявки (по ordinal ключа)
foreach (var kv in deletesRange.Value.OrderBy(k => k.Key, StringComparer.Ordinal))
{
    var prefix = PrefixOfKey(kv.Key);
    if (registryPrefixes.Contains(prefix))
    {
        requested ??= prefix;
        continue;
    }
    if (await DeleteKeyAsync(kv.Key, ct) is { IsSuccess: false } delReq)
        return delReq;
    await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
        $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
}

// (4) Отбор (спека §3.1): (1) доводка первого DELETING безусловно →
// (2) первая заявка orphan-deletes (обходит hold и автоправило — осознанная
// команда) → (3) TTL-кандидат без полных и без hold. Один префикс за проход.
var candidate = merged.Orphans
    .FirstOrDefault(e => e.State == OrphanState.Deleting)?.Prefix;
var byRequest = false;
if (candidate is null && requested is not null)
{
    candidate = requested;
    byRequest = true;
}
if (candidate is null)
    candidate = OrphanRegistry.SelectTtlCandidate(
        merged, options.SupervisorOrphanTtlSec, nowUnix, heldPrefixes);
if (candidate is null)
    return Result.Success();
```

Далее — существующий блок доводки, с двумя правками:
- перед put DELETING, при `byRequest` — журнал заявки:

```csharp
if (byRequest)
    await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
        $"orphan-delete-requested/{candidate}", claims.InstanceId, null, ct);
```

- в финале (после `orphan-deleted`-журнала) — del ключа заявки:

```csharp
// Заявка исполнена — гасим её ключ (санитар следующего прохода тоже бы гасил,
// но чистим сразу: заявка и запись гасятся вместе — AC4).
if (byRequest && await DeleteKeyAsync(OrphanRegistry.DeleteKey(candidate), ct)
    is { IsSuccess: false } delDone)
    return delDone;
```

Приватные хелперы (failover по endpoints — по образцу `ReadRegistryAsync`):
`RangeKeysAsync(prefix)` — `etcd.RangeAsync(endpoint, prefix, ct)`; 
`DeleteKeyAsync(key)` — `etcd.DeleteAsync(endpoint, key, prefix: false, ct)`;
`static string PrefixOfKey(string key)` — `string.Join('/', key.Split('/')[^2..])`.

Примечание: `IEtcdGateway.DeleteAsync` с `prefix: false` — одиночный del
(образец — `SeedAsync` тестов). Обновить XML-комментарий класса sweeper
(упомянуть санитар/заявки — зеркалит arch/19 §4).

- [ ] **Шаг 4: Прогнать — убедиться в прохождении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~BackupOrphanSweeperTests"`
Ожидание: PASS всех (старые + 7 новых). Зачистка docker-остатков серии — как
в шаге 2.

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs
git commit -m "t04: sweeper — санитар hold/заявок, приоритет DELETING→заявка→TTL, del заявки в доводке"
```

---

### Task 3: API воркера — `POST/DELETE /api/backups/orphans/{C}/{X}/hold`, `POST .../delete`

**Файлы:**
- Create: `src/PgWorker.App/Api/Operations/OrphansHandlers.cs`
- Modify: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (новые исключения)
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (3 эндпоинта)
- Modify: `src/PgWorker.App/Program.cs` (DI-регистрации)
- Test: Create `src/tests/PgWorker.IntegrationTests/Api/OrphansApiTests.cs`

**Интерфейсы (produces):**
- `OrphanHoldHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time, Func<BackupsRuntimeOptions?> runtime)`:
  - `Task<Result> SetAsync(string cluster, string shard, string requestedBy, CancellationToken ct)` — put hold-ключа
  - `Task<Result> RemoveAsync(string cluster, string shard, CancellationToken ct)` — del hold-ключа, идемпотентен
- `OrphanDeleteHandler(...те же зависимости...)`:
  - `Task<Result<OrphanDeleteRequestedDto>> RequestAsync(string cluster, string shard, OrphanDeleteRequest? body, string requestedBy, CancellationToken ct)`
- `record OrphanDeleteRequest(string? Confirm)`; `record OrphanDeleteRequestedDto(string Prefix, long RequestedUnix, string RequestedBy)` — без `JsonPropertyName`, Minimal API отдаст camelCase `{"prefix","requestedUnix","requestedBy"}` (spec §3.4).
- Исключения: `OrphanNotFoundException`, `OrphanDeletingException`, `OrphanConfirmMismatchException`, `BackupsDisabledException` (все в `WorkerApiExceptions.cs`).
**Consumes:** `OrphanRegistry.Key/Parse/HoldKey/DeleteKey/HoldToJson/DeleteToJson` (Task 1); образец гвардов — `RestoreShardHandler`.

- [ ] **Шаг 1: Написать падающие API-интеграции** (`OrphansApiTests.cs`;
  `[Collection(PgApiCollection.Name)]`, сид реестра — прямой put
  `OrphanRegistry.ToJson(...)` в etcd; фабрика-наследник с
  `PgWorker:Backups:Enabled=true` — default false в `BackupsOptions`)

```csharp
// WAF-оверрайд: подсистема бэкапов включена (гвард 503 снят для основных
// кейсов; выключенное состояние проверяет базовая фабрика с default false).
public sealed class OrphansApiFactory(Etcd.EtcdFixture etcd) : PgWorkerApiFactory(etcd)
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
            new Dictionary<string, string?> { ["PgWorker:Backups:Enabled"] = "true" }));
    }
}
```

Сид реестра (общий хелпер тест-класса):

```csharp
// Сид реестра сирот: одна запись ghost/s1 в заданном state (AAA-Arrange).
private async Task SeedRegistryAsync(string prefix, string state = "OBSERVED",
    bool hasValidFull = false)
{
    var registry = new OrphanRegistry.Registry(
        [new OrphanEntry(prefix, "cluster", 100, 1757760000,
            state == "DELETING" ? OrphanState.Deleting : OrphanState.Observed,
            hasValidFull)],
        1757764800);
    await Etcd.Gateway.PutAsync(Etcd.Endpoint, OrphanRegistry.Key,
        OrphanRegistry.ToJson(registry), null, TestContext.Current.CancellationToken);
}
```

Кейсы (тела — по образцу `RestoreApiTests`; каждый с AAA-комментарием):
1. `Hold_204_ключ_в_etcd` (AC6) — `POST /api/backups/orphans/ghost/s1/hold`
   с заголовком `X-Requested-By: panel` → 204; в etcd ключ
   `/pgworker/backups/orphan-holds/ghost/s1` со значением, содержащим
   `"set_by":"panel"` и `"set_unix":`.
2. `Hold_повторный_204_перезаписывает` (AC6, идемпотентность) — второй POST →
   204, значение перезаписано (put поверх).
3. `Hold_404_не_сирота` (AC6) — реестр пуст → 404.
4. `Hold_404_мусорные_имена` (AC6) — `POST /api/backups/orphans/BAD!/x/hold`
   → 404 (имена не-regex, БЕЗ похода в etcd — реестр остался нетронутым).
5. `Hold_409_DELETING` (AC6) — сид записи state=DELETING → 409, hold-ключа нет.
6. `Unhold_204_идемпотентен` (AC6) — `DELETE .../hold` при живом ключе → 204 и
   ключ удалён; повторный DELETE (ключа нет) → 204.
7. `Delete_202_ключ_заявки` (AC6) — `POST .../delete` c
   `{"confirm":"ghost/s1"}` → 202; тело ответа содержит
   `"prefix":"ghost/s1"`; в etcd ключ
   `/pgworker/backups/orphan-deletes/ghost/s1` с `"requested_by"`.
8. `Delete_повторный_202_put_поверх` (AC6) — вторая заявка → 202, не 409.
9. `Delete_400_confirm_мисматч` (AC6) — `{"confirm":"other/s1"}` → 400; ключа
   заявки нет.
10. `Delete_400_нет_тела` (AC6) — POST без тела → 400.
11. `Delete_404_не_сирота` и `Delete_409_DELETING` (AC6).
12. `Hold_503_подсистема_выключена` (AC6) — клиент БАЗОВОЙ фабрики
    (`PgWorkerApiFactory`, Enabled=false по умолчанию) → 503.

- [ ] **Шаг 2: Прогнать — убедиться в падении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~OrphansApiTests"`
Ожидание: FAIL — 404 на всех маршрутах (эндпоинтов нет).

- [ ] **Шаг 3: Реализовать**

1. `WorkerApiExceptions.cs` — добавить:

```csharp
// ── Сироты бэкапов (reliability t04, arch/19 §4) ──

// Префикса нет в реестре сирот / имена неканонические — 404.
public sealed class OrphanNotFoundException(string prefix)
    : Exception($"сирота {prefix} не найдена в реестре /pgworker/backups/orphans");

// Запись в DELETING — доводку начатого удаления не спасти — 409.
public sealed class OrphanDeletingException(string prefix)
    : Exception($"сирота {prefix} уже в DELETING — удаление идёт, доводку не остановить");

// confirm != "<C>/<X>" — 400 (осознанная потеря DR-источника).
public sealed class OrphanConfirmMismatchException(string expected, string got)
    : Exception($"confirm обязан совпадать с префиксом сироты: ожидался '{expected}', получен '{got}'");

// Подсистема бэкапов выключена — 503 (заявки копятся? нет — API не принимает).
public sealed class BackupsDisabledException()
    : Exception("подсистема бэкапов выключена (PgWorker:Backups:Enabled=false) — операции сирот недоступны");
```

2. `OrphansHandlers.cs` — общий гвард + два хендлера (имена — regex как у
   `RestoreShardHandler`: `GeneratedRegex`-паттерны; чтение реестра —
   failover по endpoints):

```csharp
// Гварды сирот: имена канонические (иначе 404 без похода в etcd), префикс в
// реестре OBSERVED (нет → 404; DELETING → 409); подсистема включена (503).
internal static class OrphanGuards
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardPattern();

    // Чтение записи реестра для префикса: имена → 404 без etcd; выключено →
    // 503; сбой всех endpoints → 503 (EtcdWriteUnavailableException);
    // префикса нет → 404; DELETING → 409.
    public static async Task<Result<OrphanEntry>> FindObservedAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string shard,
        Func<BackupsRuntimeOptions?> runtime, CancellationToken ct)
    {
        if (!ClusterPattern().IsMatch(cluster) || !ShardPattern().IsMatch(shard))
            return Result<OrphanEntry>.Failed(
                new OrphanNotFoundException($"{cluster}/{shard}"));
        if (runtime() is null)
            return Result<OrphanEntry>.Failed(new BackupsDisabledException());

        var prefix = $"{cluster}/{shard}";
        Result<Kv?>? last = null;
        Kv? kv = null;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, OrphanRegistry.Key, ct);
            if (got.IsSuccess)
            {
                kv = got.Value;
                last = null;
                break;
            }
            last = got;
        }
        if (last is not null)
            return Result<OrphanEntry>.Failed(new EtcdWriteUnavailableException());

        var registry = kv is { } some ? OrphanRegistry.Parse(some.Value) : null;
        var entry = registry?.Orphans.FirstOrDefault(e => e.Prefix == prefix);
        if (entry is null)
            return Result<OrphanEntry>.Failed(new OrphanNotFoundException(prefix));
        if (entry.State == OrphanState.Deleting)
            return Result<OrphanEntry>.Failed(new OrphanDeletingException(prefix));
        return Result<OrphanEntry>.Success(entry);
    }
}
```

(Сигнатуру `GetAsync`/тип `Kv` сверить с `ReadRegistryAsync` sweeper'а при
исполнении — там цикл по endpoints с `result.Value is { } kv`.)

`OrphanHoldHandler.SetAsync`: гвард → put `HoldKey(prefix)` =
`HoldToJson(time.GetUtcNow().ToUnixTimeSeconds(), requestedBy)` (failover).
`RemoveAsync`: только гвард `runtime()` (503) и del `HoldKey` (ключа нет —
тоже успех, идемпотентность; реестр НЕ читаем — spec §3.4: у unhold только
503). `OrphanDeleteHandler.RequestAsync`: гвард → confirm
`body?.Confirm == $"{cluster}/{shard}"`, иначе
`OrphanConfirmMismatchException` → put `DeleteKey(prefix)` =
`DeleteToJson(now, requestedBy)` → `Result.Success(new
OrphanDeleteRequestedDto(prefix, now, requestedBy))`.

3. `ApiModule.cs` — три эндпоинта (маппинг ошибок — по образцу restore;
   `X-Requested-By` c fallback `"operator"` — spec §3.4):

```csharp
// POST /api/backups/orphans/{cluster}/{shard}/hold — hold-флаг сироты
// (reliability t04, arch/19 §4): put per-префиксного ключа orphan-holds;
// 204; 404 не-сирота/мусорные имена; 409 DELETING; 503 etcd/выключено.
endpoints.MapPost("/api/backups/orphans/{cluster}/{shard}/hold", async (
    string cluster, string shard, HttpRequest http, OrphanHoldHandler handler, CancellationToken ct) =>
{
    var requestedBy = http.Headers.TryGetValue("X-Requested-By", out var by)
        && !string.IsNullOrWhiteSpace(by) ? by.ToString() : "operator";
    var result = await handler.SetAsync(cluster, shard, requestedBy, ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return result.Error switch
    {
        OrphanNotFoundException => Results.Problem(statusCode: 404,
            title: "Not found", detail: result.Error.Message),
        OrphanDeletingException => Results.Problem(statusCode: 409,
            title: "Orphan hold rejected", detail: result.Error.Message),
        BackupsDisabledException or EtcdWriteUnavailableException => Results.Problem(statusCode: 503,
            title: "Backups unavailable", detail: result.Error.Message),
        _ => Results.Problem(statusCode: 503,
            title: "Etcd write failed", detail: result.Error!.Message),
    };
});
// DELETE /api/backups/orphans/{cluster}/{shard}/hold — 204 идемпотентен; 503.
// POST /api/backups/orphans/{cluster}/{shard}/delete — body {"confirm":"<C>/<X>"};
//   202 {prefix,requestedUnix,requestedBy}; 400/404/409/503 (маппинг тот же +
//   OrphanConfirmMismatchException → 400 "Orphan delete rejected"; нет тела → 400
//   "Invalid body", как у restore).
```

4. `Program.cs` — регистрации (по образцу `BackupsPolicyHandler`, рядом;
   `runtime`-lambda — как у sweeper, строки 574–584):

```csharp
// Сироты бэкапов: hold/unhold/delete-заявки (reliability t04, arch/19 §4) —
// пер-префиксные ключи orphan-holds/orphan-deletes, пишет только API.
builder.Services.AddSingleton(sp => new OrphanHoldHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime()
        : null));
builder.Services.AddSingleton(sp => new OrphanDeleteHandler(/* те же аргументы */));
```

- [ ] **Шаг 4: Прогнать — убедиться в прохождении**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~OrphansApiTests"`
Ожидание: PASS (все кейсы). Затем регресс: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.IntegrationTests.Api"` — PASS.

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.App/Api/Operations/OrphansHandlers.cs src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs src/PgWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/OrphansApiTests.cs
git commit -m "t04: API воркера — hold/unhold/delete сирот (гварды 400/404/409/503, идемпотентность)"
```

---

### Task 4: Панель — модель снапшота и парсер (`HasValidFull`, `Holds`, `DeleteRequests`)

**Файлы:**
- Modify: `src/AdminPanel.Core/BackupsInfo.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs`

**Интерфейсы (produces):**
- `BackupOrphanInfo(... , bool HasValidFull = false)`
- `record OrphanHoldInfo(string Prefix, long SetUnix, string SetBy)`; `record OrphanDeleteRequestInfo(string Prefix, long RequestedUnix, string RequestedBy)`
- `BackupOrphansInfo(IReadOnlyList<BackupOrphanInfo> Orphans, long UpdatedUnix, IReadOnlyDictionary<string, OrphanHoldInfo>? Holds = null, IReadOnlyDictionary<string, OrphanDeleteRequestInfo>? DeleteRequests = null)`
**Consumes:** формат ключей arch/19 §4; снапшот уже читает range `/pgworker/backups/` целиком (`SnapshotRefresher.cs:126`) — новые ключи попадают в парсер автоматически.

- [ ] **Шаг 1: Написать падающие юнит-тесты** (добавить в
  `BackupsParserTests.cs` панельный; существующие конструкторы `BackupOrphansInfo`
  не ломаются — хвостовые default-поля):

```csharp
// AAA (AC7/AC9): реестр с has_valid_full + hold/заявки парсятся в снапшот;
// отсутствие has_valid_full → false (толерантный читатель); битые hold/заявки —
// KeyParseError + пропуск, реестр не теряется
[Fact]
public void Parse_держит_полных_и_заявки_толерантно()
{
    // Arrange — orphans c has_valid_full, hold-ключ, заявка, битый hold
    var kvs = new List<Kv>
    {
        new(OrphansKey, """{"orphans":[{"prefix":"g/s1","kind":"cluster","size_bytes":1,"first_seen_unix":2,"state":"OBSERVED","has_valid_full":true},{"prefix":"g/s2","kind":"cluster","size_bytes":1,"first_seen_unix":2,"state":"OBSERVED"}],"updated_unix":3}"""),
        new("/pgworker/backups/orphan-holds/g/s1", """{"set_unix":5,"set_by":"panel"}"""),
        new("/pgworker/backups/orphan-deletes/g/s2", """{"requested_unix":6,"requested_by":"operator"}"""),
        new("/pgworker/backups/orphan-holds/g/s3", "{не json"),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert
    var orphans = result.Orphans!;
    orphans.Orphans.Single(o => o.Prefix == "g/s1").HasValidFull.Should().BeTrue();
    orphans.Orphans.Single(o => o.Prefix == "g/s2").HasValidFull.Should().BeFalse("поля нет — старый формат");
    orphans.Holds.Should().ContainKey("g/s1");
    orphans.Holds!["g/s1"].SetBy.Should().Be("panel");
    orphans.DeleteRequests.Should().ContainKey("g/s2");
    result.Errors.Should().ContainSingle(e => e.Key == "/pgworker/backups/orphan-holds/g/s3");
}
```

- [ ] **Шаг 2: Прогнать — падение**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.BackupsParserTests"`
Ожидание: FAIL — `HasValidFull`/`Holds` нет.

- [ ] **Шаг 3: Реализовать**

1. `BackupsInfo.cs` — расширить записи (сигнатуры выше; XML-комментарии
   по-русски, ссылка на arch/19 §4 / reliability t04).
2. `BackupsParser.cs`:
   - в orphans-ветке (существующий разбор записей) — читать
     `has_valid_full` как `Bool(item, "has_valid_full") ?? false` (отсутствие
     — валидно, НЕ malformed; новый хелпер `Bool` по образцу `Long`);
   - две новые ветки ДО гварда `segments.Length < 5 ... continue` (у новых
     ключей 6 сегментов):
     `segments.Length == 6 && segments[3] == "orphan-holds"` →
     `Long(root,"set_unix")` + `String(root,"set_by")` обязательны, иначе
     `KeyParseError` + пропуск; префикс `$"{segments[4]}/{segments[5]}"`;
     симметрично `orphan-deletes` (`requested_unix`/`requested_by`);
   - собираются в локальные словари → в `BackupOrphansInfo` (orphans-ключ
     отсутствует/битый → `orphans` остаётся null — Holds/DeleteRequests тогда
     тоже не собираются: они без реестра не информативны для модели, но
     парсить их БЕЗ реестра допустимо — решение: собираем всегда, в модель
     кладём только при валидном `orphans`, иначе — только errors).

- [ ] **Шаг 4: Прогнать — прохождение**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.BackupsParserTests"`
Ожидание: PASS (старые тесты не задеты — default-поля; алерт-тесты не меняются в этом таске).

- [ ] **Шаг 5: Коммит**

```bash
git add src/AdminPanel.Core/BackupsInfo.cs src/AdminPanel.Etcd/Parsing/BackupsParser.cs src/tests/AdminPanel.UnitTests/BackupsParserTests.cs
git commit -m "t04: панель — снапшот сирот: has_valid_full + Holds/DeleteRequests (парсер)"
```

---

### Task 5: Панель — DTO грани, `MergeOrphans`/`TtlLeftSec`, алерт `backup-orphan`

**Файлы:**
- Modify: `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs`
- Modify: `src/AdminPanel.Core/Alerting/Rules/BackupOrphanRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupStorageQueryTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupOrphanRuleTests.cs`

**Интерфейсы (produces):**
- `BackupOrphanDto(string Prefix, string Kind, long SizeBytes, bool InWorkerRegistry, string? RegistryState, long? FirstSeenUnix, long? TtlLeftSec, bool HasValidFull = false, bool Held = false, long? HeldUnix = null, string? HeldBy = null, bool DeleteRequested = false, long? DeleteRequestedUnix = null, string? DeleteRequestedBy = null)`
- Правило `TtlLeftSec`: null при `HasValidFull || Held || DeleteRequested` (защищённым/заявленным TTL-строка не нужна — spec §3.5/AC7).
- Алерт `backup-orphan` — приоритет fate: DELETING («идёт удаление», включая по заявке) → hold («защищён hold-флагом (<by>) — удаление только явной командой», Remedy OperatorRunbook) → автозащита («защищён автоправилом (есть валидный полный) — удаление только явной командой», Remedy OperatorRunbook) → заявка («к удалению заявкой оператора — ближайший проход воркера», Remedy WorkerAuto) → прежний остаток TTL / `OrphanTtlSec=0`. Severity — warning для всех.
**Consumes:** модель Task 4 (`BackupOrphansInfo.Holds/DeleteRequests/HasValidFull`), `AlertsOptions.Backups.OrphanTtlSec`.

- [ ] **Шаг 1: Падающие юнит-тесты**

В `BackupStorageQueryTests.cs` (по образцу существующих тестов
`MergeOrphans`/слитых сирот):

```csharp
// AAA (AC7): джойн реестр×hold×заявка — бейджи защиты в DTO; TtlLeftSec
// только у незащищённых
[Fact]
public void MergeOrphans_защита_и_заявки_в_DTO()
{
    // Arrange — реестр: g/s1 с полным, g/s2 без; hold на g/s2; заявка на g/s1
    var registry = new BackupOrphansInfo(
    [
        new BackupOrphanInfo("g/s1", "cluster", 10, NowUnix - 86400, "OBSERVED", HasValidFull: true),
        new BackupOrphanInfo("g/s2", "cluster", 20, NowUnix - TtlSec - 10, "OBSERVED"),
    ], NowUnix,
    new Dictionary<string, OrphanHoldInfo> { ["g/s2"] = new("g/s2", NowUnix - 60, "panel") },
    new Dictionary<string, OrphanDeleteRequestInfo> { ["g/s1"] = new("g/s1", NowUnix - 30, "operator") });

    // Act — MapStorage с деревом, где оба префикса (по образцу SnapshotWithTree)
    var dto = BackupStorageMappers.MapStorage(SnapshotWithTree(registry), TtlSec, NowUnix);

    // Assert
    var s1 = dto.Orphans.Single(o => o.Prefix == "g/s1");
    s1.HasValidFull.Should().BeTrue();
    s1.DeleteRequested.Should().BeTrue();
    s1.DeleteRequestedBy.Should().Be("operator");
    s1.TtlLeftSec.Should().BeNull("защищён полным + заявка");
    var s2 = dto.Orphans.Single(o => o.Prefix == "g/s2");
    s2.Held.Should().BeTrue();
    s2.HeldBy.Should().Be("panel");
    s2.TtlLeftSec.Should().BeNull("под hold");
    // незащищённая просроченная без заявки — TTL-остаток есть (отрицательный —
    // «следующий проход»; проверяется отдельным кейсом g/s3 без защит)
}
```

(Хелпер `SnapshotWithTree` в тест-классе уже существует — расширить
перегрузкой с реестром; добавить и кейс `g/s3` без защит → `TtlLeftSec` не
null.)

В `BackupOrphanRuleTests.cs` — кейсы четырёх+одного состояний (AC7):
`hold_фраза_защищён_и_рунбук`, `полный_фраза_автоправило`,
`незащищённый_остаток_TTL` (существующий тест остаётся),
`DELETING_идёт_удаление` (существующий), `заявка_к_удалению` (новый:
OBSERVED + DeleteRequests содержит префикс → текст содержит
«к удалению»; Remedy OperatorRunbook для защищённых).

- [ ] **Шаг 2: Прогнать — падение**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.BackupStorageQueryTests|FullyQualifiedName~AdminPanel.UnitTests.BackupOrphanRuleTests"`
Ожидание: FAIL новых кейсов.

- [ ] **Шаг 3: Реализовать**

1. `BackupStorageQuery.cs`:
   - расширить `BackupOrphanDto` (поля выше, хвостовые default — старые
     позиционные конструкции не ломаются);
   - `MergeOrphans(panelOrphans, registry, orphanTtlSec, nowUnix)` (уже
     принимает `BackupOrphansInfo?`) — при сборке DTO джойнить
     `registry?.Holds?.TryGetValue(prefix)` и
     `registry?.DeleteRequests?.TryGetValue(prefix)`; `TtlLeftSec` —
     вычислять только для незащищённых:

```csharp
private static long? TtlLeftSec(
    long? firstSeenUnix, long ttlSec, long nowUnix, bool protectedOrRequested)
    => !protectedOrRequested && firstSeenUnix is { } seen && ttlSec > 0
        ? seen + ttlSec - nowUnix
        : null;
// protectedOrRequested = HasValidFull || Held || DeleteRequested
```

   - для строк «реестр без S3-факта» (второй `AddRange`-цикок) — тот же джойн.
2. `BackupOrphanRule.Evaluate` — ветвление fate по приоритету выше; тексты
   hold/автозащиты включают `SetBy`/`by` держателя; Remedy
   `AlertRemedy.OperatorRunbook` для защищённых (в existing-стиле), manual
   строка: «удали заявкой delete (POST /api/backups/orphans/<C>/<X>/delete с
   confirm), если данные не нужны»; атрибуты алерта дополнить
   `["hasValidFull"]`/`["heldBy"]`/`["deleteRequested"]`.

- [ ] **Шаг 4: Прогнать — прохождение + регресс панели**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"`
Ожидание: PASS. Затем интеграцию грани (spec §1 п.7): в
`src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs` добавить
кейс `GET /api/backups/storage` — ответ `orphans` несёт `hasValidFull/held/
deleteRequested` (сид снапшота с hold/заявкой — по образцу существующих
кейсов файла); прогнать
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.BackupsStorageApiTests"`
— PASS.

- [ ] **Шаг 5: Коммит**

```bash
git add src/AdminPanel.Api/Inspection/BackupStorageQuery.cs src/AdminPanel.Core/Alerting/Rules/BackupOrphanRule.cs src/tests/AdminPanel.UnitTests/BackupStorageQueryTests.cs src/tests/AdminPanel.UnitTests/BackupOrphanRuleTests.cs src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs
git commit -m "t04: панель — DTO сирот (защита/hold/заявка), TtlLeftSec только незащищённым, алерт 4+1 состояний"
```

---

### Task 6: Панель — команды-прокси и эндпоинты `OperationsModule`

**Файлы:**
- Create: `src/AdminPanel.Api/Operations/OrphansCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs`
- Test: Create `src/tests/AdminPanel.UnitTests/Operations/OrphansCommandsTests.cs`

**Интерфейсы (produces):**
- `record HoldOrphanCommand(string Cluster, string Shard, string RequestedBy) : ICommand<OrphanMutatedDto>`
- `record UnholdOrphanCommand(string Cluster, string Shard) : ICommand<OrphanMutatedDto>`
- `record DeleteOrphanCommand(string Cluster, string Shard, string Confirm, string RequestedBy) : ICommand<OrphanDeleteAcceptedDto>`
- `record OrphanMutatedDto(string Prefix)`; `record OrphanDeleteAcceptedDto(string Prefix, long RequestedUnix, string RequestedBy)`
- Хендлеры `[InjectAsScoped]` через `WorkerProxy.SendAsync` (204-ответы — тело пустое, DTO default — модуль его не читает).
- Эндпоинты панели: `POST /api/backups/orphans/{cluster}/{shard}/hold` → 204; `DELETE .../hold` → 204; `POST .../delete` → 202 + DTO воркера.
**Consumes:** `IWorkerApiGateway`/`WorkerProxy`/`WorkerProblemDetails` (существующие); образец — `DeleteClusterCommand.cs`, `MoveOpsCommands.cs`.

- [ ] **Шаг 1: Падающие юнит-тесты команд** (`OrphansCommandsTests.cs`, по
  образцу `MoveOpsProxyCommandTests.cs` — фейковый `IWorkerApiGateway`,
  проверка метода/пути/тела/requestedBy; кейсы: hold → POST
  `/api/backups/orphans/c1/s1/hold`, requestedBy передан; unhold → DELETE;
  delete → POST `.../delete` с телом `{"confirm":"c1/s1"}`; ответ воркера
  409 ProblemDetails → `WorkerProblemDetails` с телом как есть; 202 → DTO
  десериализуется camelCase)

- [ ] **Шаг 2: Прогнать — падение** (типов не существует — CS0103)

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~OrphansCommandsTests"`

- [ ] **Шаг 3: Реализовать**

`OrphansCommands.cs` (полный файл — образец `DeleteClusterCommand`):

```csharp
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Сироты бэкапов: hold/unhold/delete-заявка (reliability t04, arch/02 §9.10) —
// прокси в API PgWorker; панель в etcd не пишет.
public sealed record HoldOrphanCommand(string Cluster, string Shard, string RequestedBy)
    : ICommand<OrphanMutatedDto>;

public sealed record UnholdOrphanCommand(string Cluster, string Shard)
    : ICommand<OrphanMutatedDto>;

public sealed record DeleteOrphanCommand(string Cluster, string Shard, string Confirm, string RequestedBy)
    : ICommand<OrphanDeleteAcceptedDto>;

public sealed record OrphanMutatedDto(string Prefix);

public sealed record OrphanDeleteAcceptedDto(string Prefix, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class HoldOrphanCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<HoldOrphanCommand, OrphanMutatedDto>
{
    public async ValueTask<Result<OrphanMutatedDto>> Handle(HoldOrphanCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<OrphanMutatedDto>(
            api, "pgworker", HttpMethod.Post,
            $"/api/backups/orphans/{command.Cluster}/{command.Shard}/hold",
            body: null, requestedBy: command.RequestedBy, ct);
}

// UnholdOrphanCommandHandler — HttpMethod.Delete, requestedBy: null.
// DeleteOrphanCommandHandler — HttpMethod.Post, ".../delete",
//   body: new { confirm = $"{command.Cluster}/{command.Shard}" } — confirm
//   команды (сервер воркера перепроверит), requestedBy: command.RequestedBy.
```

`OperationsModule.cs` — три эндпоинта рядом с существующими (образец
`secrets/rotate`; `requestedBy = user.Identity?.Name ?? "adminpanel"`):

```csharp
// POST /api/backups/orphans/{cluster}/{shard}/hold — hold-флаг сироты
// (t04, 02 §9.10): прокси в API PgWorker; 204.
endpoints.MapPost("/api/backups/orphans/{cluster}/{shard}/hold", async (
    string cluster, string shard, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<HoldOrphanCommand, OrphanMutatedDto>(
        new HoldOrphanCommand(cluster, shard, user.Identity?.Name ?? "adminpanel"), ct);
    return result.IsSuccess ? Results.NoContent() : Error(result);
});
// DELETE .../hold — 204 идемпотентен; POST .../delete — 202 + DTO (Accepted).
```

- [ ] **Шаг 4: Прогнать — прохождение + регресс**

Выполнить: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.Operations"`
Ожидание: PASS.

- [ ] **Шаг 5: Коммит**

```bash
git add src/AdminPanel.Api/Operations/OrphansCommands.cs src/AdminPanel.Api/Operations/OperationsModule.cs src/tests/AdminPanel.UnitTests/Operations/OrphansCommandsTests.cs
git commit -m "t04: панель — команды-прокси hold/unhold/delete сирот + эндпоинты"
```

---

### Task 7: Frontend — бейджи защиты, кнопки Hold/Unhold/Delete, confirm-модал

**Файлы:**
- Modify: `frontend/src/api/dto.ts` (расширение `BackupOrphanDto`)
- Modify: `frontend/src/api/queries.ts` (3 мутации)
- Modify: `frontend/src/pages/BackupsStoragePage.tsx` (карточка `OrphansCard`)
- Create: `frontend/src/pages/backups-storage/DeleteOrphanModal.tsx`

**Интерфейсы (produces):**
- `BackupOrphanDto` + `hasValidFull: boolean; held: boolean; heldUnix: number | null; heldBy: string | null; deleteRequested: boolean; deleteRequestedUnix: number | null; deleteRequestedBy: string | null`
- `holdOrphan(cluster, shard): Promise<void>`, `unholdOrphan(cluster, shard): Promise<void>`, `deleteOrphan(cluster, shard, confirm): Promise<OrphanDeleteAcceptedDto>`
**Consumes:** `apiFetch` (204 → undefined), образец confirm-модала — `frontend/src/pages/kafka-cluster/DeleteTopicModal.tsx` (ввод имени, кнопка заблокирована до совпадения), образец кнопок-мутаций со спиннером — существующие страницы.

- [ ] **Шаг 1: `dto.ts` + `queries.ts`**

```typescript
// dto.ts — расширить BackupOrphanDto (camelCase от DTO сервера):
//   hasValidFull, held, heldUnix, heldBy, deleteRequested,
//   deleteRequestedUnix, deleteRequestedBy (nullable — number | null / string | null)

// queries.ts — три мутации (образец removeShard/deleteCluster):
export function holdOrphan(cluster: string, shard: string): Promise<void> {
  return apiFetch<void>(
    `/api/backups/orphans/${encodeURIComponent(cluster)}/${encodeURIComponent(shard)}/hold`,
    { method: 'POST' });
}

export function unholdOrphan(cluster: string, shard: string): Promise<void> {
  return apiFetch<void>(
    `/api/backups/orphans/${encodeURIComponent(cluster)}/${encodeURIComponent(shard)}/hold`,
    { method: 'DELETE' });
}

export function deleteOrphan(cluster: string, shard: string, confirm: string): Promise<OrphanDeleteAcceptedDto> {
  return apiFetch<OrphanDeleteAcceptedDto>(
    `/api/backups/orphans/${encodeURIComponent(cluster)}/${encodeURIComponent(shard)}/delete`,
    { method: 'POST', body: { confirm } });
}
```

- [ ] **Шаг 2: `DeleteOrphanModal.tsx`** — по образцу `DeleteTopicModal.tsx`:
  props `{ prefix: string; cluster: string; shard: string; onClose: () => void; onDone: () => void }`;
  заголовок «Удалить сироту {prefix}», предупреждение о НЕОБРАТИМОСТИ
  (данные DR-источника будут потеряны), `TextInput` с плейсхолдером
  `<C>/<X>`, кнопка Delete (`color="red"`) заблокирована пока ввод !==
  prefix; после 202 — `notifications.show` успех и `onDone()`; ошибка —
  ApiError detail (образец остальных модалей).

- [ ] **Шаг 3: `OrphansCard` в `BackupsStoragePage.tsx`** — колонка «Защита»
  и «Действия»:
  - бейджи: `o.hasValidFull` → `<Badge color="teal" variant="light">полный: защита</Badge>`;
    `o.held` → `<Badge color="indigo" variant="light">hold{o.heldBy ? ` (${o.heldBy})` : ''}</Badge>`;
    `o.deleteRequested` → `<Badge color="red" variant="light">к удалению</Badge>`;
    ничего — `—` (dimmed);
  - TTL-строка — только при `o.ttlLeftSec !== null` (сервер уже отдаёт null
    защищённым; условие в тексте уже есть — оставить как есть);
  - кнопки: `Unhold` если `o.held`, иначе `Hold` (без модала, спиннер до 204;
    после — invalidate/refetch запроса хранилища — тот же механизм, что у
    других мутаций страницы); `Delete` (только при `o.inWorkerRegistry`) —
    открывает `DeleteOrphanModal`;
  - состояние строки DELETING — кнопки скрыты.

- [ ] **Шаг 4: Проверка типов и сборки**

Выполнить: `cd frontend && npm run typecheck && npm run build`
Ожидание: 0 ошибок TS, сборка успешна.

- [ ] **Шаг 5: Коммит**

```bash
git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/BackupsStoragePage.tsx frontend/src/pages/backups-storage/DeleteOrphanModal.tsx
git commit -m "t04: фронт — бейджи защиты сирот, кнопки Hold/Unhold/Delete с confirm-модалом"
```

---

### Task 8: Runbook — `docs/backup-restore.md` §2/§5/§5.1

**Файлы:**
- Modify: `docs/backup-restore.md`

**Consumes:** контракты Task 1–3 (ключи, curl-команды, коды).

- [ ] **Шаг 1: Переписать §5.1** («Судьба S3-объектов удалённого кластера») —
  новая судьба: DR-источник с валидным полным (`full/<id>/backup_manifest`)
  автоматикой НЕ удаляется никогда (автоправило последнего полного);
  TTL `OrphanTtlSec` чистит только незащищённых; hold — защита «до разбора»
  (WAL-огрызки и т.п.); осознанное удаление — только заявкой delete с
  confirm; накопление защищённых наблюдаемо (панель, ключ
  `/pgworker/backups/storage`, квота-алерты).

- [ ] **Шаг 2: Дополнить §2** — curl-команды и etcdctl-форматы:

```markdown
### Сироты бэкапов: hold / unhold / явное удаление (reliability t04)

# hold — защитить сироту до разбора (идемпотентен)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/hold -X POST \
  -H 'X-Requested-By: operator'

# unhold — снять hold (идемпотентен: нет ключа — тоже 204)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/hold -X DELETE

# заявка явного удаления — единственный путь удалить защищённую сироту
# (минует hold и автоправило; исполнит sweeper ближайшим проходом)
curl -sk https://<worker>/api/backups/orphans/<C>/<X>/delete -X POST \
  -H 'Content-Type: application/json' -d '{"confirm":"<C>/<X>"}'

Коды: 204/202 — принято; 400 — confirm-мисматч/нет тела; 404 — префикс не в
реестре / имена неканонические; 409 — запись в DELETING; 503 — etcd-сбой или
`PgWorker:Backups:Enabled=false`.

# Ручной путь без API (формат канона arch/19 §4):
etcdctl put /pgworker/backups/orphan-holds/<C>/<X> \
  '{"set_unix":1760000000,"set_by":"operator"}'
etcdctl del /pgworker/backups/orphan-holds/<C>/<X>
etcdctl put /pgworker/backups/orphan-deletes/<C>/<X> \
  '{"requested_unix":1760000000,"requested_by":"operator"}'
etcdctl get --prefix /pgworker/backups/orphan-holds/   # кто под защитой
```

- [ ] **Шаг 3: Правка §5** (сценарий DR) — заменить фразу «успеть до
  истечения TTL либо выключить авто-удаление» на «источник с валидным полным
  удерживается автоправилом бессрочно (§5.1); удаление возможно только явной
  заявкой». Проверить весь текст на остаточные упоминания старой семантики
  (`grep -n "успеть\|TTL" docs/backup-restore.md`) — противоречий быть не
  должно (AC8).

- [ ] **Шаг 4: Коммит**

```bash
git add docs/backup-restore.md
git commit -m "t04: runbook backup-restore — новая судьба DR-источника, hold/delete-команды"
```

---

### Task 9: Docker-E2E кейс + мерж-гейт + снятие тега roadmap

**Файлы:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eSupervisorScenarios.cs`
- Modify (мерж-гейтом): `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md`

**Consumes:** всё предыдущее; `E2eEnvironment` (withMinio), хелперы
`RunScenarioAsync`/`WaitPhaseAsync`/`McCpAsync`/`McLsAsync`/
`StartOrphanHostAsync` (существуют в файле); телеметрия по `docs/e2e-launch.md`
уже в `RunScenarioAsync`.

- [ ] **Шаг 1: Новый E2E-кейс** (добавить `[Fact]` рядом с
  `Backup_OrphanRegistry_TtlDelete`; Slug окружения `bk-drh`, кластер
  `bkdrh<тег>`, сирота `ghost<тег>`)

```csharp
// AAA (AC1/AC4/AC10): DR-источник выжил после истёкшего TTL; заявка чистит.
// Сирота с объектом full/<id>/backup_manifest → реестр has_valid_full=true →
// TTL 120 c истёк → объекты ЖИВЫ (автоправило) → заявка delete (etcd-put
// канона) → sweeper исполнил: объекты удалены, запись и заявка погашены.
[Fact]
public async Task Backup_OrphanDrHold_ПолныйПереживаетTtl_ЗаявкаЧистит()
{
    // Arrange 1 — окружение bk-drh (withMinio), воркер с OrphanTtlSec=120,
    //   Supervisor IntervalSec=60 (StartOrphanHostAsync — сжатое время)
    DockerTrait.SkipIfUnavailable();
    var ct = TestContext.Current.CancellationToken;
    await RunScenarioAsync("bk-drh", async fx =>
    {
        Fx = fx;
        var cluster = $"bkdrh{Fx.ClusterTag}";
        var ghost = $"ghost{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartOrphanHostAsync(cluster, ct);

        // Arrange 2 — provisioning DONE; посев СИРОТЫ С ВАЛИДНЫМ ПОЛНЫМ:
        //   manifest — критерий DR-выбора (arch/19 §3.5/§5)
        var provisioned = await WaitPhaseAsync("provisioning",
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning обязан дойти до DONE");
        await McCpAsync($"{ghost}/shard1/full/20260911110000Z/base.tar", "dr-data");
        await McCpAsync($"{ghost}/shard1/full/20260911110000Z/backup_manifest", "manifest");

        // Act 1 — реестр: запись с has_valid_full=true (WaitFor)
        var observed = await WaitPhaseAsync("orphan-has-full", async () =>
        {
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            return raw?.Value.Contains($"{ghost}/shard1") == true
                   && raw.Value.Contains("\"has_valid_full\":true");
        }, TimeSpan.FromSeconds(180), ct);
        observed.Should().BeTrue(
            $"сирота обязана попасть в реестр с has_valid_full=true: [{(await GetOrNullAsync("/pgworker/backups/orphans"))?.Value}]");

        // Act 2 — окно выживания: WaitFor, пока с момента first_seen записи
        // прошло ≥ TTL (120 c) + один проход (60 c) — к этому моменту sweeper
        // УЖЕ обязан был удалить незащищённую (поллинг по часам хоста; сна
        // >30 c в тесте нет — условие опрашивается тиками WaitFor)
        long firstSeen = 0;
        var window = await WaitPhaseAsync("dr-ttl-window", async () =>
        {
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            if (raw is null || !raw.Value.Contains($"{ghost}/shard1"))
                return false;
            using var doc = JsonDocument.Parse(raw.Value);
            firstSeen = doc.RootElement.GetProperty("orphans").EnumerateArray()
                .Single(o => o.GetProperty("prefix").GetString() == $"{ghost}/shard1")
                .GetProperty("first_seen_unix").GetInt64();
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() >= firstSeen + 120 + 60;
        }, TimeSpan.FromSeconds(360), ct);
        window.Should().BeTrue("окно истёкшего TTL + один проход обязано наступить");

        // Assert 1 — объекты ЖИВЫ, запись OBSERVED с has_valid_full (AC1)
        (await McLsAsync($"{ghost}/shard1/")).Should().HaveCountGreaterThanOrEqualTo(2,
            "DR-источник с валидным полным не удаляется автоматикой НИКОГДА");
        var registryAfter = (await GetOrNullAsync("/pgworker/backups/orphans"))!.Value;
        registryAfter.Should().Contain($"{ghost}/shard1")
            .And.Contain("OBSERVED").And.Contain("\"has_valid_full\":true",
            "запись жива под автоправилом");

        // Act 3 — заявка явного удаления (ручной путь канона, runbook §2);
        //   after-окно: TTL уже истёк, заявка приоритетнее — ближайший проход
        await G.PutAsync(Endpoint,
            $"/pgworker/backups/orphan-deletes/{ghost}/shard1",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"operator"}""",
            null, ct);

        // Assert — sweeper исполнил: объекты удалены, запись и заявка погашены
        var swept = await WaitPhaseAsync("orphan-delete-by-request", async () =>
        {
            var listed = await McLsAsync($"{ghost}/shard1/");
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            var req = await GetOrNullAsync($"/pgworker/backups/orphan-deletes/{ghost}/shard1");
            return listed.Count == 0
                   && (raw is null || !raw.Value.Contains($"{ghost}/shard1"))
                   && req is null;
        }, TimeSpan.FromSeconds(300), ct);
        swept.Should().BeTrue("заявка удаляет защищённую сироту; запись и заявка гасятся вместе");
    }, ct);
}
```

Примечание: 240-секундное окно «survived» проверяет НЕПОЯВЛЕНИЕ удаления —
ждём дважды TTL после first_seen; фазы >60 c автоматически снимают логи
(`WaitPhaseAsync` — уже сделано).

- [ ] **Шаг 2: Прогон E2E-серии**

Зачистка перед серией: `docker ps -a --format '{{.Names}}' | grep pgw-` (0
остатков), `docker network ls | grep -E 'pgw|kfw'` — осиротевшие сети убрать
`docker network prune -f`. Выполнить:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~E2eSupervisorScenarios"
```

Ожидание: PASS (3 сценария, включая новый). После — зачистка серии
(контейнеры/сети/`pgw-e2e-artifacts-*` при упавших — разбор по логам БЕЗ
перезапуска, `README-cleanup.txt`).

- [ ] **Шаг 3: Мерж-гейт — полный прогон на свежем Release**

1. Юниты+интеграции: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release`
   — PASS (после интеграционных серий — зачистка docker между проектами).
2. Кейс-маркер AGENTS.md: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
   — PASS (E2eFixture пересоберёт Release сам; `PGW_TEST_E2E_NOBUILD` не
   использовать). Зачистка после серии.
3. Критерий AC10: всё зелёное на свежем Release.

- [ ] **Шаг 4: Коммит E2E + roadmap (если задача идёт в мерж — по решению
  пользователя)**

Снятие тега `t04-orphan-dr-hold` из `arch/roadmap/reliability.md` (строка 41
и все `←`-упоминания) + перенос строки в «Сделано»
`arch/roadmap/reliability-report.md` с правкой сводки характеристики D —
ТЕМ ЖЕ мерж-коммитом (правило мерж-гейта трека):

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eSupervisorScenarios.cs
git commit -m "t04: E2E — DR-источник выживает истёкший TTL, заявка delete чистит"
# мерж-коммит (при явной просьбе пользователя о мерже в main):
#   снять тег из reliability.md + обновить reliability-report.md тем же коммитом
```

---

## Соответствие задач критериям приёмки (spec §6)

| AC | Закрывается |
|---|---|
| AC1 автоправило | Task 1 (функции), Task 2 (интеграция), Task 9 (E2E) |
| AC2 hold держит | Task 2 |
| AC3 unhold → удаление | Task 2 |
| AC4 заявка удаляет любую + journal-фазы | Task 2 (`orphan-delete-requested`/`orphan-deleting`/`orphan-deleted`), Task 9 |
| AC5 воскресение гасит запись/hold/заявку | Task 2 |
| AC6 гварды API 400/404/409/503 + идемпотентность | Task 3 |
| AC7 панель DTO/бейджи/кнопки/алерт | Task 4–7 |
| AC8 runbook | Task 8 |
| AC9 backcompat парсинга | Task 1 (юнит), Task 4 (панель) |
| AC10 мерж-гейт + roadmap | Task 9 |

## Правила исполнения

- Каждый таск выполняется отдельным субагентом (subagent-driven) с чтением
  ТОЛЬКО своего таска + заголовка плана; соседние таски связываются блоками
  «Интерфейсы».
- Коммит после каждого таска (шаг «Коммит»); в feature-ветке — свободно.
- Любая серия docker-тестов (интеграции с etcd-фикстурами, E2E) — с зачисткой
  контейнеров/сетей после финальной строки прогона.
- Упавший тест НЕ перезапускается «посмотреть» — разбор по логам/артефактам
  (`/tmp/pgw-e2e-artifacts-<guid>/`, `host.log`), формулировка причин, потом
  исправление.
