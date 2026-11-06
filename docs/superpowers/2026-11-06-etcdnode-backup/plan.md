# EtcdNode (перенос DR-бэкапа etcd в узлы контура) — план реализации

> **Для агентных исполнителей:** шаги отмечаются чекбоксами (`- [ ]`).
> План двухуровневый: ШАГ исполняет сильная модель (по умолчанию субагент
> coder); ПОДПУНКТЫ — самодостаточные механические задания для слабой
> модели (получает только текст подпункта). Всё, что не выделено в
> подпункты, делает сильная модель.

**Цель:** функция DR-бэкапа etcd (слепки + выгрузка в S3 + обслуживание)
переезжает из EtcdWorker в агента EtcdNode внутри каждого managed-узла;
внеочередной слепок триггерят только важные изменения (пульсы не триггерят).

**Архитектура:** новый компонент — агент (.NET) в кастомном образе узла
`pgworker/etcdnode:dev` (etcd + supervisord + агент, один контейнер);
лидер среди агентов — lease-клэйм `/etcdnode/leader`; слепок — со своего
`localhost:2379`; конвейер выгрузки переносится 1:1; статус —
`/etcdnode/snapshots`; фильтр важности — канон-список пульсов + дифф полной
карты `key→mod_revision` с эталоном последней выгрузки.

**Стек:** .NET 10 (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), центральные версии пакетов
(`Directory.Packages.props`), docker (testcontainers в тестах), AWSSDK.S3.

**Spec:** `docs/superpowers/2026-11-06-etcdnode-backup/spec.md` (исполнитель
читает spec и план вместе). Канон: `arch/etcdnode/` (обновлён), в конфликте
правится канон.

**Репозиторий:** работать в worktree
`/Users/demakaev/ZCodeProject/worktrees/feat-etcdnode-backup` (ветка
`feat-etcdnode-backup`); все пути ниже — от его корня.

## Глобальные ограничения

- P/Invoke запрещён (`DllImport`/`LibraryImport` — никаких).
- Бюджеты НЕ меняются: `IntervalMin=360`, `MinIntervalSec=300`,
  `RetryIntervalSec=300`, `TimeoutSec=30`, `RetentionFiles=10`,
  `RetentionObjects=28`, `MaintenanceIntervalMin=60`, lease TTL 15 с.
- Формат объектов S3 и JSON статус-ключа НЕ меняются (меняется только путь
  статус-ключа: `/etcdworker/snapshots` → `/etcdnode/snapshots`);
  `.meta.json.instance` — имя узла.
- Идентификаторы кода — английские; комментарии/докстринги — русские, в тон
  окружающего; тесты — с AAA-комментариями.
- Сборка солюшена после каждого шага:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
  — 0 warnings (warnings-as-errors), успех.
- Коммиты — свободно (feature-ветка): один шаг = один коммит.
- Базовые порты тестов — только динамические
  (`assignRandomHostPort: true` + `GetMappedPublicPort`), никаких
  литералов-портов; `BrokerBootSec`-подобные бюджеты ≤ 100 с; полный
  teardown окружения при любом исходе.

## Review Focus (поведение, которое легко сломать — проверяется тестами)

1. Пульс-перезапись (`/service/x/leader`, `/clusters/c/shards/s/master`)
   растит revision, но НЕ порождает слепков — юниты `PulseKeysTests` +
   интеграционный кейс (шаги 9, 10).
2. Новый незнакомый ключ или смена значения мастер-ключа — триггерят
   слепок (не только появление ключей) — юниты `ChangeMapDiffTests` +
   интеграционный кейс (шаги 9, 10).
3. Нет эталонной карты (первый тик после старта/смены лидера) — слепок
   снимается немедленно — юнит `SnapshotScheduleTests` (шаг 9),
   интеграционный старт агента (шаг 10).
4. Отказ собственного etcd у лидера — НЕ писать статус и НЕ релизить
   лидерство: lease гаснет по TTL ≤ 15 с (координация localhost-only),
   статус пишет новый лидер — проверяется ревью кода AgentLoop (шаг 3: в
   ветке TakeAsync-fail нет записи статуса и нет релиза) против канона
   arch/etcdnode/01-agent.md §2/§4.
5. Смена писателя статус-ключа не должна оставить панель на старом ключе —
   константа в `SnapshotRefresher` + юнит парсера формата (шаг 11).

---

### Шаг 1. Каркас EtcdNode.Core: новые чистые типы фильтра

**Вход:** worktree чист (ветка `feat-etcdnode-backup`), spec прочитан.

**Действие (сильная модель):** создать проекты `src/EtcdNode.Core` и
`src/tests/EtcdNode.UnitTests` (csproj в подпунктах), добавить оба в
`src/PgWorker.slnx` (в папку `/etcdw/` рядом с EtcdWorker.* строками
57–61 — проекты EtcdNode ставятся ПЕРЕД строками EtcdWorker: строки с
`EtcdNode.Core/EtcdNode.Core.csproj` и тесты в `/tests/` рядом со строками
93–94). Написать три типа (код в подпунктах): `PulseKeys`, `ChangeMapDiff`,
`SnapshotSchedule` (новая семантика spec §3.4) и тесты к ним.

**Выход:** новые проекты собираются, новые юниты зелёные; солюшен
собирается.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ успех; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release --no-build --nologo`
→ Passed, Failed=0.

**Spec:** §3.1 (EtcdNode.Core), §3.4 (логика фильтра), решение №3.

**Подпункты (слабая модель):**

1.1. Создай файл `src/EtcdNode.Core/EtcdNode.Core.csproj` с содержимым:

```xml
<Project Sdk="Microsoft.NET.Sdk">
    <ItemGroup>
        <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
        <ProjectReference Include="..\Shared.Etcd\Shared.Etcd.csproj"/>
        <!-- Переносимые EtcdSnapshotS3/S3CaHttpClientFactory используют
             Shared.Tls.TlsMaterial.LoadPem (как EtcdWorker.Core сегодня) —
             без ссылки шаг 2 упадёт CS0234. -->
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
    </ItemGroup>
    <ItemGroup>
        <PackageReference Include="AWSSDK.S3"/>
    </ItemGroup>
</Project>
```

1.2. Создай файл `src/tests/EtcdNode.UnitTests/EtcdNode.UnitTests.csproj`
с содержимым (копия структуры `src/tests/EtcdWorker.UnitTests/EtcdWorker.UnitTests.csproj`,
ProjectReference заменена на EtcdNode.Core):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <Using Include="Xunit"/>
        <Using Include="FluentAssertions"/>
    </ItemGroup>

    <ItemGroup>
        <PackageReference Include="coverlet.collector">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="FluentAssertions"/>
        <PackageReference Include="Microsoft.NET.Test.Sdk"/>
        <PackageReference Include="xunit.runner.visualstudio">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; build-transitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="xunit.v3"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\..\EtcdNode.Core\EtcdNode.Core.csproj"/>
    </ItemGroup>

    <ItemGroup>
        <Using Include="Shared.Core"/>
    </ItemGroup>

</Project>
```

Исправление: в блоке `xunit.runner.visualstudio` строка
`<IncludeAssets>runtime; build; native; contentfiles; analyzers; build-transitive</IncludeAssets>`
должна быть `buildtransitive` (без дефиса) — пиши ровно как в файле
`src/tests/EtcdWorker.UnitTests/EtcdWorker.UnitTests.csproj`.

1.3. Добавь в `src/PgWorker.slnx`: строку
`<Project Path="EtcdNode.Core/EtcdNode.Core.csproj" />` ПЕРВОЙ внутри папки
`<Folder Name="/etcdw/">` (сейчас первая строка в этой папке —
`<Project Path="EtcdWorker.Core/EtcdWorker.Core.csproj" />`), и строку
`<Project Path="tests/EtcdNode.UnitTests/EtcdNode.UnitTests.csproj" />` в
папку тестов ПЕРЕД строкой
`<Project Path="tests/EtcdWorker.UnitTests/EtcdWorker.UnitTests.csproj" />`.

1.4. Создай файл `src/EtcdNode.Core/PulseKeys.cs`:

```csharp
namespace EtcdNode.Core;

/// <summary>Канонический список пульсирующих ключей etcd
/// (arch/etcdnode/02-snapshots.md §3.1): временные метки живости сервисов,
/// перезаписываемые каждые 1–2 с; изменения ТОЛЬКО в этих ключах не
/// триггерят внеочередной DR-слепок. Пополнение списка — правка канона и
/// этого файла.</summary>
public static class PulseKeys
{
    /// <summary>Ключ — пульс: префикс /service/ (Patroni-DCS: leader,
    /// members, sync, initialize, config) ЛИБО мастер-ключ HA-контура
    /// /clusters/&lt;C&gt;/shards/&lt;X&gt;/master.</summary>
    public static bool IsPulse(string key)
        => key.StartsWith("/service/", StringComparison.Ordinal)
           || IsHaMasterKey(key);

    // /clusters/<C>/shards/<X>/master: split по '/' даёт 6 элементов с
    // ведущим пустым (["", "clusters", C, "shards", X, "master"]).
    private static bool IsHaMasterKey(string key)
    {
        var parts = key.Split('/');
        return parts.Length == 6
               && parts[1] == "clusters"
               && parts[3] == "shards"
               && parts[5] == "master";
    }
}
```

1.5. Создай файл `src/EtcdNode.Core/ChangeMapDiff.cs`:

```csharp
namespace EtcdNode.Core;

/// <summary>Дифф полной карты ключей (arch/etcdnode/02-snapshots.md §3):
/// сравнение текущей карты key→mod_revision с эталонной картой последней
/// успешной выгрузки. Изменение (появление/исчезновение/рост mod_revision)
/// ХОТЯ БЫ одного ключа ВНЕ списка пульсов — важное.</summary>
public static class ChangeMapDiff
{
    /// <summary>true — есть важные изменения (вне PulseKeys).</summary>
    public static bool IsImportant(
        IReadOnlyDictionary<string, long> baseline,
        IReadOnlyDictionary<string, long> current)
    {
        foreach (var (key, revision) in current)
        {
            if (PulseKeys.IsPulse(key))
                continue;
            if (!baseline.TryGetValue(key, out var was) || was != revision)
                return true; // новый ключ вне пульсов или изменился
        }

        foreach (var key in baseline.Keys)
        {
            if (!PulseKeys.IsPulse(key) && !current.ContainsKey(key))
                return true; // ключ вне пульсов исчез
        }

        return false;
    }
}
```

1.6. Создай файл `src/EtcdNode.Core/SnapshotSchedule.cs`:

```csharp
namespace EtcdNode.Core;

/// <summary>Чистое решение тика агента EtcdNode (arch/etcdnode/02-snapshots.md
/// §3, arch/etcdnode/processes/c-snapshot-export.md C2): плановый слепок раз в
/// IntervalMin; внеочередной — ТОЛЬКО важное изменение (пульсы не триггерят):
/// нет эталона — важно (снимаем сразу, RPO-консервативно); revision не вырос
/// относительно эталона — не важно; вырос — решает дифф карты (diffImportant,
/// вычисляется циклом ТОЛЬКО при росте revision).</summary>
public static class SnapshotSchedule
{
    public static bool ShouldTake(
        long nowUnix,
        long? lastTakeUnix,
        long? lastUploadedUnix,
        long currentRevision,
        long? baselineRevision,
        bool diffImportant,
        int intervalMin,
        int minIntervalSec)
    {
        // Плановый: никогда не снимали либо прошло ≥ IntervalMin.
        if (lastTakeUnix is not { } taken || nowUnix - taken >= intervalMin * 60L)
            return true;

        // Важность: нет эталона — важно; revision не вырос — не важно;
        // вырос — решает дифф карты.
        var important = baselineRevision is not { } baseline
            ? true
            : currentRevision > baseline && diffImportant;
        if (!important)
            return false;

        // Внеочередной: важно И выгрузка отстала ≥ MinIntervalSec
        // (никогда не выгружали — снимаем).
        return lastUploadedUnix is not { } uploaded
            ? true
            : nowUnix - uploaded >= minIntervalSec;
    }
}
```

1.7. Создай `src/tests/EtcdNode.UnitTests/PulseKeysTests.cs`: xunit-класс
`PulseKeysTests` (namespace `EtcdNode.UnitTests`), FluentAssertions, методы
с AAA-комментариями: `ServicePrefix_IsPulse` (`"/service/shop-1/leader"` →
true), `HaMasterKey_IsPulse` (`"/clusters/shop/shards/s1/master"` → true),
 `OtherKeys_NotPulse` (`"/clusters/shop/shards/s1/routing"`,
`"/service"`, `"/clusters/shop/shards/s1"`,
`"/clusters/shop/shards/s1/master/x"`, `"/pgworker/work"` → false).

1.8. Создай `src/tests/EtcdNode.UnitTests/ChangeMapDiffTests.cs`: класс
`ChangeMapDiffTests` (namespace `EtcdNode.UnitTests`), кейсы (AAA):
`PulseRewrite_NotImportant` (baseline
`{["/service/x/leader"]=1, ["/clusters/c/shards/s/master"]=1}`, current —
те же ключи с revision 2 и 5 → false);
`NewKeyOutsidePulses_Important` (current добавляет `"/pgworker/work"` →
true); `ChangedValueOutsidePulses_Important` (baseline
`{["/clusters/c/shards/s/routing"]=1}` current revision=2 → true);
`KeyOutsidePulsesDisappeared_Important` (baseline имеет `"/etcd/config"`,
current нет → true); `MasterKeyChangedValue_NotImportant` (только
`"/clusters/c/shards/s/master"` revision 1→2 → false); `EmptyMaps_NotImportant`
(оба пустые → false).

1.9. Создай `src/tests/EtcdNode.UnitTests/SnapshotScheduleTests.cs`: класс
`SnapshotScheduleTests` (namespace `EtcdNode.UnitTests`), кейсы (AAA):
`NeverTaken_Planned` (lastTakeUnix=null → true);
`IntervalElapsed_Planned` (now-taken=21600, intervalMin=360 → true);
`Quiet_NoBaselineGrowth_False` (taken недавно, baselineRevision=100,
currentRevision=100, diffImportant=false → false);
`NoBaseline_Important_Take` (baselineRevision=null, uploaded давно → true);
`RevisionGrown_DiffImportant_Take` (baseline=100, current=105,
diffImportant=true, uploaded 400 с назад, minIntervalSec=300 → true);
`RevisionGrown_DiffNotImportant_False` (diffImportant=false → false);
`Important_TooSoonAfterUpload_False` (uploaded 100 с назад,
minIntervalSec=300 → false);
`Important_NeverUploaded_Take` (lastUploadedUnix=null → true).

1.10. Скомпилируй и прогони (из корня worktree):
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ успех без warnings; затем
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release --no-build --nologo`
→ все тесты Passed. Ошибки компиляции — исправь и добейся зелёного.

- [ ] **Шаг 1 закрыт** (сильная модель ревьюит новые типы против spec §3.4:
  семантика «нет эталона — важно», «revision не вырос — дифф не нужен»,
  пульс-паттерны совпадают с arch/etcdnode/02-snapshots.md §3.1; коммит
  `feat(etcdnode): core filter types — pulse keys, change map diff, schedule`).**

---

### Шаг 2. Ядерный перенос механики + демонтаж EtcdWorker (атом сборки)

**Вход:** шаг 1 закрыт; солюшен собирается.

**Действие (сильная модель):** ОДНИМ шагом (солюшен собирается только
целиком): (а) перенести файлы конвейера из `EtcdWorker.*` в `EtcdNode.Core`
(namespace `EtcdNode.Core`, правки статус-ключа и CA-источника по
подпунктам); (б) доработать `ClaimStore` (Shared.Etcd): ТОЛЬКО необязательный
параметр конструктора `instanceId` (по канону `/etcdnode/instances/<k>` —
ключ живости с именем узла; F-релиз лидерства НЕ вводится — координация
агента localhost-only, при мёртвом собственном etcd клэйм гаснет по TTL
≤ 15 с, arch/etcdnode/01-agent.md §2); (в) демонтировать функцию в
воркере: удалить SnapshotLoop и
DI-регистрации, секцию healthz, витальные хартбиты, переименовать секции
конфига; (г) перенести юниты перенесённых типов в EtcdNode.UnitTests; (д)
правка затронутых тестов воркера (StartupChecks/Vitality/EffectiveConfig);
(е) compose: убрать том `etcdw-snapshots`. После правок — ревью диффа:
в `src/EtcdWorker.*` не осталось ссылок на перенесённые типы
(grep `SnapshotExporter|EtcdBackupTarget|EtcdSnapshotS3|EtcdExportRetention|EtcdSnapshotStatus|EtcdSnapshotMeta|SnapshotSchedule|S3CaHttpClientFactory|EtcdS3Object|BackupCaPem`
по `src/EtcdWorker.*` — пусто; `BackupCaPem` переносится тоже — воркер
зовёт его из `EtcdNode.Core` по ProjectReference, см. 2.3).

**Выход:** конвейер живёт в `EtcdNode.Core` (статус-ключ
`/etcdnode/snapshots`); воркер не снимает/не выгружает слепки и не пишет
статус; сборка и все юниты зелёные.

**Проверка:** сборка солюшена 0 warnings (после временной меры 2.17 —
удаления `EtcdWorker.IntegrationTests/Snapshots/`) →
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release --no-build --nologo`
зелёный →
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --nologo`
зелёный → grep выше пуст.

**Spec:** §3.1 (перенос 1:1), §3.3 (демонтаж), решения №4, №5, №7.

**Подпункты (слабая модель):**

2.1. Перенеси (git mv, затем правка) файлы, в каждом замени namespace
`EtcdWorker.Etcd`/`EtcdWorker.Core`/`EtcdWorker.Provisioning` на
`EtcdNode.Core` (включая doc-комментарии: ссылки на канон
`arch/etcdworker/04-snapshots.md` заменить на `arch/etcdnode/02-snapshots.md`,
`/etcdworker/snapshots` на `/etcdnode/snapshots`):
- `src/EtcdWorker.Etcd/EtcdExportRetention.cs` → `src/EtcdNode.Core/EtcdExportRetention.cs` (в файле using `EtcdWorker.Core` удалить — тип `EtcdS3Object` переедет в тот же namespace);
- `src/EtcdWorker.Etcd/EtcdSnapshotMeta.cs` → `src/EtcdNode.Core/EtcdSnapshotMeta.cs`;
- `src/EtcdWorker.Etcd/EtcdSnapshotStatus.cs` → `src/EtcdNode.Core/EtcdSnapshotStatus.cs`; в нём: константу `EtcdSnapshotStatusJson.Key` замени `"/etcdworker/snapshots"` → `"/etcdnode/snapshots"`;
- `src/EtcdWorker.Core/EtcdS3Object.cs` → `src/EtcdNode.Core/EtcdS3Object.cs`;
- `src/EtcdWorker.Core/EtcdSnapshotS3.cs` → `src/EtcdNode.Core/EtcdSnapshotS3.cs`;
- `src/EtcdWorker.Core/S3CaHttpClientFactory.cs` → `src/EtcdNode.Core/S3CaHttpClientFactory.cs`;
- `src/EtcdWorker.Provisioning/EtcdBackupTargetResolver.cs` → `src/EtcdNode.Core/EtcdBackupTargetResolver.cs`;
- `src/EtcdWorker.Provisioning/SnapshotExporter.cs` → `src/EtcdNode.Core/SnapshotExporter.cs`.

После переносов удали старую реализацию расписания (её преемник с новой
семантикой фильтра создан в шаге 1.6; пока файл жив — конфликт типов
`SnapshotSchedule` в солюшене не возникает, но grep-проверка шага и
требование spec §3.3 «SnapshotSchedule перенесён» требуют удаления):
`git rm src/EtcdWorker.Core/SnapshotSchedule.cs`.

2.3. Перенеси `git mv src/EtcdWorker.Core/BackupCaPem.cs
src/EtcdNode.Core/BackupCaPem.cs`, namespace `EtcdWorker.Core` →
`EtcdNode.Core`; в двух сообщениях исключений замени
`EtcdWorker:Snapshots:S3:CaBundlePath=` на
`EtcdWorker:NodeSnapshot:S3:CaBundlePath=` (два вхождения); докстринг
файла поправь: читается файлом хоста ВОРКЕРА, содержимое уходит в env
узла `ETCDNODE_S3_CA_PEM` (arch/etcdnode/01-agent.md §3). В
`src/EtcdWorker.App/EtcdWorker.App.csproj` добавь ProjectReference:

```xml
        <ProjectReference Include="..\EtcdNode.Core\EtcdNode.Core.csproj"/>
```

2.4. В `src/Shared.Etcd/Coordination/ClaimStore.cs` (строка 13)
конструктор
`public sealed class ClaimStore(string keyPrefix, string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null, string? certThumbprint = null)`
дополни последним параметром `string? instanceId = null`, а свойство
`public string InstanceId { get; } = Guid.NewGuid().ToString("N")[..12];`
замени на
`public string InstanceId { get; } = instanceId ?? Guid.NewGuid().ToString("N")[..12];`
(агент EtcdNode передаёт имя своего узла — ключ живости
`/etcdnode/instances/etcdw-node-<k>`, arch/etcdnode/00-etcdnode-contract.md
§1; все существующие call-сайты компилируются без правок — параметр
необязательный). Методы класса НЕ трогать: F-релиз лидерства не вводится
(координация агента localhost-only — клэйм при мёртвом localhost гаснет
по TTL сам, arch/etcdnode/01-agent.md §2).

2.5. Удали файл `src/EtcdWorker.App/Loops/SnapshotLoop.cs`
(`git rm src/EtcdWorker.App/Loops/SnapshotLoop.cs`).

2.6. В `src/EtcdWorker.App/Options.cs`:
- класс `SnapshotOptions` переименуй в `NodeSnapshotOptions`, свойство
  `Dir` (строка `public string Dir { get; set; } = "/snapshots";`) УДАЛИ,
  докстринг класса замени на: «Параметры агентов EtcdNode управляемых узлов
  (arch/etcdnode/01-agent.md §3): доставляются env контейнера узла при
  создании; креды/endpoint'ы/bucket НЕ здесь — агент резолвит цель из
  /s3/clusters/»;
- в `EtcdWorkerOptions` свойство `public SnapshotOptions Snapshots` замени
  на `public NodeSnapshotOptions NodeSnapshot { get; set; } = new();`
  (добавь using не нужен — тип в том же файле);
- в `DockerImagesOptions` свойство `public string Etcd` переименуй в
  `public string EtcdNode` со значением по умолчанию
  `"pgworker/etcdnode:dev"`; докстринг замени на
  «Образ managed-узла: etcd + агент EtcdNode, собирается локально
  (arch/etcdnode/01-agent.md §1); в registry не кладётся.».

2.7. В `src/EtcdWorker.App/EtcdStartupChecks.cs` метод
`public static bool Snapshots(EtcdWorkerOptions o)` переименуй в
`NodeSnapshot`, внутри замени все `o.Snapshots.` на `o.NodeSnapshot.`
(7 полей-условий без изменений).

2.8. В `src/EtcdWorker.App/Program.cs`:
- в блоке `.Validate(...)` (строки 58–71) замените вызов и текст:
  `EtcdStartupChecks.Snapshots(o)` → `EtcdStartupChecks.NodeSnapshot(o)`,
  текст сообщения `"EtcdWorker:Snapshots: intervals must be positive, retentions ≥ 1"`
  → `"EtcdWorker:NodeSnapshot: intervals must be positive, retentions ≥ 1"`;
- удали блок регистраций слепков ЦЕЛИКОМ (строки 130–160: комментарии
  «Циклы: keepalive…» сохранить для KeepaliveLoop): registrations
  `Shared.Etcd.Maintenance.SnapshotJob`, `EtcdBackupTargetResolver`,
  `SnapshotExporter`, `SnapshotLoop` (AddSingleton+AddHostedService) —
  оставить только `builder.Services.AddSingleton<KeepaliveLoop>();` и
  `builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());`;
- в регистрации `HealthCheckAbstract<SnapshotLoop>` (строки 219–220) и
  `.AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop")` (строка
  226) — удали обе строки;
- в построении `EtcdWorker.ProvisioningOptions` (строка 179) замени
  `o.Docker.Images.Etcd` на `o.Docker.Images.EtcdNode` (поля NodeSnapshot
  добавит шаг 4 — сейчас компилируется без них).

2.9. В `src/EtcdWorker.App/HealthState.cs`: удали методы
`MarkSnapshotTick`, `MarkSnapshotActivity`, `MarkSnapshotTaken` (три
public-метода с докстрингами про snapshot) и приватные поля
`_lastSnapshotTick`, `_lastSnapshotActivity`, `_lastSnapshotTaken`. В
immutable-снимке (вложенный record `Snapshot`, если поля там есть — найди
по имени `LastSnapshotTick`/`LastSnapshotActivity`/`LastSnapshotTaken`)
удали соответствующие свойства и их присваивания.

2.10. В `src/EtcdWorker.App/EtcdWorkerLoopsVitality.cs`: удали хартбит
`new LoopHeartbeat("snapshot", …)` из списка в `Snapshot()` (запятую
выровняй); удали ставший мёртвым private-хелпер `Latest` (его единственный
вызов был в удаляемом хартбите — строки ~31, 37–38); докстринг класса
«перечень — reconcile/keepalive/snapshot» замени на «перечень —
reconcile/keepalive».

2.11. В `src/EtcdWorker.App/HealthChecks/EtcdWorkerHealth.cs`:
- из конструктора удали параметр `EtcdWorker.Provisioning.SnapshotExporter exporter`;
- удали блок `// snapshot-export …` (строки ~78–92: чтение `exporter.ReadStatusAsync`
  и записи `data["snapshot-export"]`);
- докстринг класса «секции etcd-reachable, docker-hosts, loops-alive
  (keepalive), snapshot-export» замени на «секции etcd-reachable,
  docker-hosts, loops-alive (keepalive)».

2.12. В `src/EtcdWorker.App/Api/Operations/EffectiveConfig.cs`:
- группу `snapshots = new { … o.Snapshots.… }` переименуй в `nodeSnapshot`
  и все `o.Snapshots.` замени на `o.NodeSnapshot.` (7 полей те же);
- `images = new { etcd = o.Docker.Images.Etcd }` замени на
  `images = new { etcdNode = o.Docker.Images.EtcdNode }`;
- докстринг файла: «Snapshots.Dir наружу не выходят» замени на
  «S3 CA-бандл наружу не выходит».

2.13. В `src/EtcdWorker.App/appsettings.json` секцию `"Snapshots"` (строка
20) переименуй в `"NodeSnapshot"`, ВНУТРИ неё удали пару
`"Dir": "/snapshots",` (если есть; остальные ключи без изменений). Рядом в
секции Docker замени образ узла: ключ `"Etcd": "…quay.io/coreos/etcd:v3.5.21"`
(строка ~17) на `"EtcdNode": "pgworker/etcdnode:dev"` (мёртвый ключ `Etcd`
молча уйдёт в дефолт — обязательно проверь grep `"Etcd"` после правки:
остаться должен только `EtcdNode`). Формат JSON сохрани (отступы как в
файле).

2.14. В `deploy/docker-compose.yml`: удали строку `- etcdw-snapshots:/snapshots`
из volumes блока `x-etcdworker-base` (строка 262); удали объявление тома
`etcdw-snapshots:` в нижнем блоке `volumes:` (строка 510); в комментарии
второго инстанса (строка 377) фразу «; том etcdw-snapshots — общий» удали.
Строку `EtcdWorker__Docker__Images__Etcd: ${ETCDW_ETCD_IMAGE:-192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21}`
заменей на
`EtcdWorker__Docker__Images__EtcdNode: ${ETCDW_ETCD_NODE_IMAGE:-pgworker/etcdnode:dev}`
и комментарий над ней — на «Образ managed-узла — локально собираемый
(etcd + агент EtcdNode, arch/etcdnode/01-agent.md §1); в registry не кладётся.».

2.15. Перенеси юниты (git mv + правка): из
`src/tests/EtcdWorker.UnitTests/Etcd/EtcdExportRetentionTests.cs` →
`src/tests/EtcdNode.UnitTests/EtcdExportRetentionTests.cs`,
`src/tests/EtcdWorker.UnitTests/Etcd/EtcdExportStatusJsonTests.cs` →
`src/tests/EtcdNode.UnitTests/EtcdExportStatusJsonTests.cs`,
`src/tests/EtcdWorker.UnitTests/Provisioning/EtcdBackupTargetResolverTests.cs`
→ `src/tests/EtcdNode.UnitTests/EtcdBackupTargetResolverTests.cs`. В каждом:
namespace `EtcdWorker.UnitTests.Etcd`/`EtcdWorker.UnitTests.Provisioning`
→ `EtcdNode.UnitTests`, using `EtcdWorker.Core`/`EtcdWorker.Etcd`/
`EtcdWorker.Provisioning` → `EtcdNode.Core`. Файл
`src/tests/EtcdWorker.UnitTests/Fakes/FakeEtcdGateway.cs` — КОПИРУЙ в
`src/tests/EtcdNode.UnitTests/Fakes/FakeEtcdGateway.cs` (namespace
`EtcdNode.UnitTests.Fakes`; оригинал остаётся — им пользуются
`EtcdPortAllocStoreTests`); в перенесённых тестах using фейка поправь.
Файл `src/tests/EtcdWorker.UnitTests/App/SnapshotScheduleTests.cs` —
УДАЛИ (`git rm`): его покрытие заменяют новые тесты шага 1.

2.16. В `src/tests/EtcdWorker.UnitTests/App/StartupChecksTests.cs` замени
все `Snapshots`/`o.Snapshots`/`new SnapshotOptions` на
`NodeSnapshot`/`o.NodeSnapshot`/`new NodeSnapshotOptions` и удали присваивания
`Dir` (если есть); ТАКЖЕ замени присваивание `o.Docker.Images.Etcd = "etcd:dev"`
(строка ~23) на `o.Docker.Images.EtcdNode = "pgworker/etcdnode:dev"`.
В `src/tests/EtcdWorker.UnitTests/App/EffectiveConfigTests.cs`: (1) в
C#-иниализации образца `Sample()` (строки ~29–39) замени свойство
`Snapshots = new SnapshotOptions { … }` на `NodeSnapshot = new
NodeSnapshotOptions { … }` и УДАЛИ строку `Dir = "/secret-snapshots"`
внутри инициализатора (поиск по lowercase-подстрокам эти CamelCase-строки
НЕ находит — правь по именам свойств); (2) замени утверждения по
JSON-полям: `"snapshots"` → `"nodeSnapshot"` и `images.etcd` →
`images.etcdNode`; (3) замени инициализацию
`new DockerImagesOptions { Etcd = "etcd-img" }` (строка ~46) на
`new DockerImagesOptions { EtcdNode = "etcd-img" }`.
В `src/tests/EtcdWorker.UnitTests/App/EtcdWorkerLoopsVitalityTests.cs` удали
кейсы/утверждения про хартбит `snapshot` (найди по подстроке `"snapshot"`).

2.17. Скомпилируй:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ успех без warnings; затем прогони оба юнит-проекта:
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release --no-build --nologo`
и
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --nologo`
→ все Passed. Интеграционные проекты (`EtcdWorker.IntegrationTests`) на этом
шаге МОГУТ не собираться (используют перенесённые типы) — их правит шаг 8;
поэтому собирай точечно:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/EtcdWorker.IntegrationTests/EtcdWorker.IntegrationTests.csproj -c Release --nologo`
НЕ выполнять; сборку солюшена в этом шаге выполни как
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
и если она упадёт на `EtcdWorker.IntegrationTests` — это ожидаемо ТОЛЬКО из-за
ссылок на `SnapshotExporter`/`EtcdBackupTargetResolver`/`EtcdSnapshotS3` в
`src/tests/EtcdWorker.IntegrationTests/Snapshots/SnapshotExportTests.cs`;
временная мера ДО шага 8: удали каталог `src/tests/EtcdWorker.IntegrationTests/Snapshots/`
целиком (`git rm -r`) — тесты экспортёра переписываются в
EtcdNode.IntegrationTests на ШАГЕ 10; источник для шага 10 — git-история
(`git show <commit-шага-2>^:src/tests/EtcdWorker.IntegrationTests/Snapshots/SnapshotExportTests.cs`,
прецедент — «паттерн SnapshotLoop из git-истории» в шаге 3).

- [ ] **Шаг 2 закрыт** (сильная модель: ревью диффа — перенос 1:1 без
  изменения форматов; статус-ключ и instance-семантика;
  коммит `feat(etcdnode)!: move snapshot pipeline to EtcdNode.Core, dismantle EtcdWorker export`).**

---

### Шаг 3. EtcdNode.App — агент (Program, Options, AgentLoop)

**Вход:** шаги 1–2 закрыты.

**Действие (сильная модель):** создать `src/EtcdNode.App` (Worker-проект,
csproj в подпункте), добавить в slnx; `Options.cs` — ручное чтение env
(подпункт); `Program.cs` — хост минимальный (без Kestrel/портов!) + метрики
Meter `EtcdNode`; `Loops/AgentLoop.cs` — ЕДИНСТВЕННЫЙ BackgroundService,
исполняющий автомат C0–C5 (архитектурный код — пишет сильная модель по
описанию ниже; механические части — подпункты). Добавить
`<InternalsVisibleTo Include="EtcdNode.IntegrationTests"/>`.

**AgentLoop — контракт для сильной модели** (канон
`arch/etcdnode/processes/c-snapshot-export.md`, `arch/etcdnode/01-agent.md`
§2/§4):
- ctor: `AgentLoop(EtcdNodeOptions options, ClaimStore claims, IEtcdGateway etcd, TimeProvider clock, ILogger<AgentLoop> logger, Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics)`
  — опции НЕ через IOptionsMonitor: они собираются вручную из env
  (`EtcdNodeEnv.Load`, ключи с одиночными подчёркиваниями конфиг-билдером
  не биндятся) и регистрируются в DI singleton-ЭКЗЕМПЛЯРОМ
  (`builder.Services.AddSingleton(options)` в Program, 3.4) — стандартный
  IOptionsMonitor создал бы свой объект с дефолтами (IntervalMin=360,
  MinIntervalSec=300) и интеграционные кейсы шага 10 на ускоренных
  интервалах молча сломались бы (Finding 2). ClaimStore и gateway —
  синглтоны DI; exporter/SnapshotJob цикл создаёт per-тик сам — список
  endpoints обновляется каждый тик.
- Список endpoints тика: `http://localhost:2379` + client-грани членов из
  `MemberListAsync("http://localhost:2379")` (имена `EtcdMember.Name`
  непустые → `http://<name>:2379`, distinct, localhost первым; отказ
  member-list → только localhost).
- Состояние цикла (память процесса): `Dictionary<string,long>? _baseline`
  (эталонная карта) и `long? _baselineRevision`.
- Тик лидера: C1 CatchUp (`exporter.CatchUpAsync("/snapshots")`) → C2
  решение: `lastTakeUnix` из `SnapshotExporter.LatestLocalFile("/snapshots")`
  + `TakenUnixFromName`, `lastUploadedUnix` из `ReadStatusAsync`,
  `currentRevision` из `StatusAsync(localhost)`; если
  `_baseline is null` → `diffImportant=true`, иначе при
  `currentRevision > _baselineRevision` → `RangeAsync(localhost, "/")` в
  словарь `key→(long)ModRevision` → `ChangeMapDiff.IsImportant(_baseline,
  current)` (и сохранить current в локальную переменную — станет новым
  эталоном), иначе `diffImportant=false`; → `SnapshotSchedule.ShouldTake(...)`.
  C3: `SnapshotJob(etcd, ["http://localhost:2379"], "/snapshots",
  RetentionFiles, MaintenanceIntervalMin).TakeAsync` — ОТКАЗ localhost →
  warning-лог `shot.Error`, статус НЕ пишется и лидерство НЕ релизится
  (координация localhost-only, arch/etcdnode/01-agent.md §2: lease гаснет
  по TTL ≤ 15 с, работу и статус пишет новый лидер — его экспортёр несёт
  полный список endpoints тика) и возврат behind=true; успех → байты
  файла → `exporter.ExportAsync(name, data, revision)`; при успехе —
  `_baseline = currentMap` (карта этого тика; если дифф не выполнялся —
  взять range заново после выгрузки), `_baselineRevision = currentRevision`.
  C4: обслуживание — ОТДЕЛЬНЫЙ вызов
  `new SnapshotJob(etcd, tickEndpoints, "/snapshots", RetentionFiles,
  MaintenanceIntervalMin).MaintainAsync(ct)`, где `tickEndpoints` — полный
  список тика (localhost первым + члены из member list; SnapshotJob снятия
  остаётся localhost-only — compact+defrag ПО ВСЕМ членам, канон
  arch/etcdnode/02-snapshots.md §5, решение №9); отказ → warning-лог.
  C5: повторный `ReadStatusAsync`+`LatestLocalFile` → `IsBehind` → сон
  `RetryIntervalSec` иначе константа скана 5 с (пульсирующий сон не нужен —
  watchdog у агента нет).
- Старт/остановка ClaimStore (обязательно, ClaimStore.cs:215-241): в начале
  `ExecuteAsync` — `await claims.StartAsync(stoppingToken)` (постановка
  instance-ключа живости + ВНУТРЕННИЙ фоновый keepalive-цикл 5 с — без
  него lease лидерства не продлевается и ключи живости не ставятся);
  в `finally` — `await claims.DisposeAsync()` (ревок lease при штатной
  остановке). Без этого лидер теряет клэйм ≤ TTL и интеграционный кейс
  шага 10 `LeaderKey_Single_Instance_Keys_Both` падает.
- Не-лидер: `TryBecomeLeaderAsync` + сон 5 с. Потеря `claims.LeaderToken`
  — отмена тика (паттерн SnapshotLoop из git-истории, S5).
- Constructor ClaimStore в DI: `new ClaimStore("/etcdnode",
  ["http://localhost:2379"], gateway, TimeProvider.System,
  instanceId: options.NodeName)` — ключ живости
  `/etcdnode/instances/etcdw-node-<k>` точно по канону
  (`arch/etcdnode/00-etcdnode-contract.md` §1; параметр instanceId — шаг 2.4).

**Выход:** проект агента собирается, агент запускается (без etcd — стартует
и крутит попытки), slnx зелёный.

**Проверка:** сборка солюшена 0 warnings;
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/EtcdNode.App -c Release --nologo`
→ успех.

**Spec:** §3.1 (App), §3.4, решения №5, №6, №10, №12, №13.

**Подпункты (слабая модель):**

3.1. Создай `src/EtcdNode.App/EtcdNode.App.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Worker">
    <ItemGroup>
        <InternalsVisibleTo Include="EtcdNode.IntegrationTests"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\EtcdNode.Core\EtcdNode.Core.csproj"/>
        <ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
        <Using Include="Shared.Etcd.Client"/>
        <Using Include="Shared.Etcd.Maintenance"/>
        <Using Include="Shared.Etcd.Coordination"/>
    </ItemGroup>
</Project>
```

3.2. Добавь в `src/PgWorker.slnx` строку
`<Project Path="EtcdNode.App/EtcdNode.App.csproj" />` в папку
`<Folder Name="/etcdw/">` ПОСЛЕ строки EtcdNode.Core и ПЕРЕД строкой
EtcdWorker.Core.

3.3. Создай `src/EtcdNode.App/Options.cs`:

```csharp
namespace EtcdNode.App;

// Конфигурация агента EtcdNode (arch/etcdnode/01-agent.md §3): доставляется
// env контейнера узла воркером EtcdNodeSpecBuilder. Чтение env — ручное
// (ключи с одиночными подчёркиваниями не биндятся конфиг-билдером).
// Отсутствующая переменная — дефолт; заданная и нечитаемая — fail-fast.

/// <summary>Корневые настройки агента.</summary>
public sealed class EtcdNodeOptions
{
    /// <summary>Имя своего узла etcdw-node-&lt;k&gt; (env ETCDNODE_NODE_NAME);
    /// уходит в .meta.json.instance и логи.</summary>
    public string NodeName { get; set; } = "etcdw-node-?";

    /// <summary>Путь к материализованному CA-PEM (env ETCDNODE_S3_CA_PEM
    /// записывается в temp-файл на старте Program; пусто — null, http-цель).</summary>
    public string? S3CaPemPath { get; set; }

    public SnapshotOptions Snapshots { get; set; } = new();
}

/// <summary>Параметры DR-слепков (arch/etcdnode/02-snapshots.md): значения
/// прежние (бюджеты не меняются).</summary>
public sealed class SnapshotOptions
{
    public int IntervalMin { get; set; } = 360;
    public int MinIntervalSec { get; set; } = 300;
    public int RetryIntervalSec { get; set; } = 300;
    public int TimeoutSec { get; set; } = 30;
    public int RetentionFiles { get; set; } = 10;
    public int RetentionObjects { get; set; } = 28;
    public int MaintenanceIntervalMin { get; set; } = 60;
}

/// <summary>Ручное чтение env ETCDNODE_*: Int(...)-хелпер с дефолтом,
/// битое значение — InvalidOperationException (fail-fast старта).</summary>
public static class EtcdNodeEnv
{
    public static EtcdNodeOptions Load()
    {
        var o = new EtcdNodeOptions
        {
            NodeName = Environment.GetEnvironmentVariable("ETCDNODE_NODE_NAME") is { Length: > 0 } name
                ? name
                : o.NodeName,
        };
        // (присваивание выше — через локальную; реализуй без рекурсии:
        //  сначала прочитай имя в локальную переменную, потом собери объект)
        return o;
    }

    public static int Int(string key, int fallback)
        => Environment.GetEnvironmentVariable(key) is { } raw && int.TryParse(raw, out var v)
            ? v
            : fallback;
}
```

Реализуй `Load()` начисто (фрагмент выше — набросок): собрать
`EtcdNodeOptions`, где `NodeName` из `ETCDNODE_NODE_NAME` (дефолт
`"etcdw-node-?"`), `S3CaPemPath` — результат материализации PEM (см.
Program, 3.4; сюда передай уже готовый путь параметром `string? s3CaPemPath`),
`Snapshots` — 7 полей через `Int("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", 360)`,
`Int("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", 300)`,
`Int("ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC", 300)`,
`Int("ETCDNODE_SNAPSHOTS__TIMEOUT_SEC", 30)`,
`Int("ETCDNODE_SNAPSHOTS__RETENTION_FILES", 10)`,
`Int("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", 28)`,
`Int("ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN", 60)`; после сборки —
валидация fail-fast: все 7 значений > 0 и ретенции ≥ 1, иначе
`InvalidOperationException($"ETCDNODE_SNAPSHOTS__*: invalid values (…)")`
(в сообщении перечисли фактические значения).

3.4. Создай `src/EtcdNode.App/Program.cs` (top-level statements):

```csharp
using EtcdNode.App;
using Shared.Metrics;

// Точка входа агента EtcdNode (arch/etcdnode/README.md, 01-agent.md):
// единственный цикл AgentLoop; БЕЗ сетевых портов (наблюдаемость — ключи
// /etcdnode/ + логи). CA-PEM из env материализуется в temp-файл (AWSSDK
// путь принимает, содержимое — нет).

var s3CaPem = Environment.GetEnvironmentVariable("ETCDNODE_S3_CA_PEM");
string? s3CaPemPath = null;
if (!string.IsNullOrWhiteSpace(s3CaPem))
{
    s3CaPemPath = Path.Combine(Path.GetTempPath(), "etcdnode-s3-ca.pem");
    File.WriteAllText(s3CaPemPath, s3CaPem);
}

var options = EtcdNodeEnv.Load(s3CaPemPath); // fail-fast при битых значениях

var builder = Host.CreateApplicationBuilder(args);
// Опции — singleton-ЭКЗЕМПЛЯР (ручная сборка из env; IOptionsMonitor дал бы
// дефолты — см. контракт AgentLoop, Finding 2).
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddAppMetrics("EtcdNode", builder.Configuration.GetSection("EtcdNode:Metrics"));
builder.Services.AddSingleton(sp => new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5));
builder.Services.AddSingleton<Shared.Etcd.Client.IEtcdGateway>(sp =>
    new Shared.Etcd.Client.EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new Shared.Etcd.Coordination.ClaimStore(
    "/etcdnode",
    ["http://localhost:2379"],
    sp.GetRequiredService<Shared.Etcd.Client.IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    instanceId: options.NodeName));
builder.Services.AddHostedService<Loops.AgentLoop>();

await builder.Build().RunAsync();
```

Если `Host.CreateApplicationBuilder` или `AddAppMetrics` потребуют using-ов,
добавь (`Microsoft.Extensions.Hosting`). Сигнатуру
`EtcdNodeEnv.Load(string? s3CaPemPath)` реализуй в 3.3 именно с этим
параметром.

3.5. Скомпилируй:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ успех (файл `Loops/AgentLoop.cs` на этом подпункте ещё отсутствует —
строку `builder.Services.AddHostedService<Loops.AgentLoop>();` временно
ЗАКОММЕНТИРУЙ и собери; следующий подпункт агента вернёт её).

- [ ] **Шаг 3 закрыт** (сильная модель пишет `Loops/AgentLoop.cs` по
  контракту выше — это ядро шага, НЕ подпункт; затем ревью против автомата
  C0–C5; коммит `feat(etcdnode): agent app — leader loop C0–C5`).**

---

### Шаг 4. Managed-узел из образа агента: spec-builder, provisioning-опции, второй volume

**Вход:** шаги 1–3 закрыты.

**Действие (сильная модель):** расширить `EtcdProvisioningOptions` полями
снапшотов (канал доставки NodeSnapshot → env узла через Core-тип —
уточнение зафиксировано в spec §3.3: MembershipProcess/NodeSupervisor
получают `EtcdProvisioningOptions`, App-тип `EtcdWorkerOptions` в
Core-процессах недоступен; семантика сохранена — значения из секции
`EtcdWorker:NodeSnapshot`); переделать `EtcdNodeSpecBuilder.Build`
(Cmd→env `ETCD_ARGS` + env агента, включая `ETCDNODE_NODE_NAME`); Program —
прокинуть NodeSnapshot/CA-PEM в `EtcdProvisioningOptions`; правка
call-sites (MembershipProcess.cs:186/226, NodeSupervisor.cs:141) и тестов. САМА (не подпункт) — volume-механика
`Shared.Docker`: посмотреть, как `ContainerSpec.VolumeName/VolumeDest`
доходят до Binds/TaskTemplate в engine-реализациях (grep `VolumeName` в
`src/Shared.Docker/Engine/`), добавить в `ContainerSpec`
(`src/Shared.Docker/Engine/IDockerEngine.cs`) необязательную пару
`string? SnapshotsVolumeName = null, string? SnapshotsVolumeDest = null`
и её проброс в создании контейнера и swarm-сервиса (по образцу первой
пары; отсутствует — не монтировать); затем `EtcdNodeDriver` (Plain+Swarm):
второй volume `etcdw-node-<k>-snapshots` → `/snapshots` — EnsureVolume при
создании узла, DeleteVolume при removeVolume, листинг сирот (`ListNodeObjectsAsync`
Plain и Swarm) — дополнить отбор `.Where(v => v.EndsWith("-data", …))`
условием `|| v.EndsWith("-snapshots", StringComparison.Ordinal)`; Swarm
`EnsureNodeAsync` — второй EnsureVolume + передача в шаблон. Хелпер имени:
`internal static string SnapshotsVolumeName(int k) =>
$"etcdw-node-{k}-snapshots";` в `PlainEtcdNodeDriver`.

**Выход:** воркер создаёт узлы образом `pgworker/etcdnode:dev` с env
`ETCD_ARGS`/`ETCDNODE_*` и двумя volume; юниты spec-builder'а зелёные.

**Проверка:** сборка 0 warnings;
`dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build` зелёный
(включая переписанные `EtcdNodeSpecBuilderTests`).

**Spec:** §3.2 (образ/env), §3.3 (spec-builder, driver), решение №8.

**Подпункты (слабая модель):**

4.1. В `src/EtcdWorker.Core/EtcdProvisioningOptions.cs` расширь record
добавив ПОСЛЕ `int HealthIntervalSec` параметры:
`int SnapshotIntervalMin, int SnapshotMinIntervalSec, int SnapshotRetryIntervalSec,
int SnapshotTimeoutSec, int SnapshotRetentionFiles, int SnapshotRetentionObjects,
int SnapshotMaintenanceIntervalMin, string? SnapshotS3CaPem`
(позиционный record — все параметры через запятую в объявлении).

4.2. Полностью перепиши `src/EtcdWorker.Core/EtcdNodeSpecBuilder.cs`:

```csharp
namespace EtcdWorker.Core;

/// <summary>Спецификация managed-узла etcd (arch/etcdworker/01-placement.md §2,
/// arch/etcdnode/01-agent.md §1): контейнер — образ EtcdNode (etcd +
/// supervisord + агент), CMD образа — supervisord (Cmd НЕ передаётся);
/// флаги etcd — env ETCD_ARGS одной строкой; конфигурация агента —
/// env ETCDNODE_* (arch/etcdnode/01-agent.md §3). Флаги без
/// shell-метасимволов — экранирование не требуется.</summary>
public static class EtcdNodeSpecBuilder
{
    public static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, EtcdProvisioningOptions options, int clientHostPort, string? initialCluster = null)
    {
        var clientAdvertise = options.AdvertisedHost is { Length: > 0 } host
            ? $"http://etcdw-node-{k}:2379,http://{host}:{clientHostPort}"
            : $"http://etcdw-node-{k}:2379";
        var args = new List<string>
        {
            "--name=etcdw-node-{k}",
            "--data-dir=/etcd-data",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--initial-advertise-peer-urls=http://etcdw-node-{k}:2380",
            $"--advertise-client-urls={clientAdvertise}",
            "--initial-cluster-state=existing",
        };
        if (initialCluster is { Length: > 0 })
            args.Add($"--initial-cluster={initialCluster}");
        args.AddRange([
            "--heartbeat-interval=250",
            "--election-timeout=2000",
            "--auto-compaction-retention=1",
            "--quota-backend-bytes=8589934592", // 8 GiB (arch/etcdworker/01-placement.md)
        ]);

        var env = new Dictionary<string, string>
        {
            ["ETCD_ARGS"] = string.Join(' ', args),
            ["ETCDNODE_NODE_NAME"] = $"etcdw-node-{k}",
            ["ETCDNODE_SNAPSHOTS__INTERVAL_MIN"] = options.SnapshotIntervalMin.ToString(),
            ["ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC"] = options.SnapshotMinIntervalSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC"] = options.SnapshotRetryIntervalSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__TIMEOUT_SEC"] = options.SnapshotTimeoutSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETENTION_FILES"] = options.SnapshotRetentionFiles.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"] = options.SnapshotRetentionObjects.ToString(),
            ["ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN"] = options.SnapshotMaintenanceIntervalMin.ToString(),
        };
        if (options.SnapshotS3CaPem is { } pem)
            env["ETCDNODE_S3_CA_PEM"] = pem;

        return ([], env); // Cmd пуст: контейнер стартует CMD образа (supervisord)
    }

    /// <summary>Полный initial-cluster для join узла k: started-члены состава
    /// (имя = первое peer-URL-имя; unstarted-learner без имени не представим)
    /// + сам новый узел.</summary>
    public static string InitialCluster(
        IReadOnlyList<Shared.Etcd.Client.EtcdMember> members, int k)
    {
        var entries = members
            .Where(m => m.Name is { Length: > 0 })
            .Select(m => $"{m.Name}={m.PeerUrls.FirstOrDefault()}")
            .Where(e => !e.EndsWith("=", StringComparison.Ordinal))
            .Append($"etcdw-node-{k}=http://etcdw-node-{k}:2380");
        return string.Join(",", entries.Distinct(StringComparer.Ordinal));
    }
}
```

Исправление к фрагменту: строки `"--name=etcdw-node-{k}"` и
`"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380"` —
интерполируемые, пиши `$"--name=etcdw-node-{k}"` и
`$"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380"`.

4.3. В `src/EtcdWorker.Provisioning/Processes/MembershipProcess.cs` замени
обе конструкции вида
`var (cmd, env) = EtcdNodeSpecBuilder.Build(k, options.AdvertisedHost, port.Client, initialCluster);`
(строки ~186 и ~226; вторая с `member.K`) на
`var (cmd, env) = EtcdNodeSpecBuilder.Build(k, options, port.Client, initialCluster);`
(и с `member.K` соответственно). Деконструкция `var (cmd, env)` ОСТАЁТСЯ:
`cmd` передаётся дальше позиционным аргументом в
`new EtcdNodeSpec(k, host, port.Client, options.Image, cmd, env, options.Network)`
(Build возвращает пустой массив — контейнер стартует CMD образа; менять
call-сайт EtcdNodeSpec не нужно). То же в
`src/EtcdWorker.Provisioning/Processes/NodeSupervisor.cs` (строка ~141).

4.4. В `src/EtcdWorker.App/Program.cs` в построении
`EtcdWorker.Provisioning.EtcdProvisioningOptions` (блок `builder.Services.AddSingleton(sp => { var o = …; return new …EtcdProvisioningOptions(…) })`)
добавь после `o.Thresholds.HealthIntervalSec` аргументы:
`o.NodeSnapshot.IntervalMin, o.NodeSnapshot.MinIntervalSec,
o.NodeSnapshot.RetryIntervalSec, o.NodeSnapshot.TimeoutSec,
o.NodeSnapshot.RetentionFiles, o.NodeSnapshot.RetentionObjects,
o.NodeSnapshot.MaintenanceIntervalMin,
EtcdNode.Core.BackupCaPem.ReadIfConfigured(o.NodeSnapshot.S3.CaBundlePath)`.
(BackupCaPem перенесён в EtcdNode.Core — шаг 2.3; ProjectReference
EtcdWorker.App → EtcdNode.Core уже добавлен там.)

4.5. В `src/EtcdWorker.Docker/Drivers/EtcdNodeDriver.cs` (механику
ContainerSpec/engine подготовила сильная модель — см. «Действие»):
- в `PlainEtcdNodeDriver.EnsureNodeAsync` сразу ПОСЛЕ успешного
  `EnsureVolumeAsync(volumeName, …)` добавь второй том:
  `var snapshots = await engine.EnsureVolumeAsync(SnapshotsVolumeName(spec.K), LabelKey, LabelValue, ct);`
  `if (!snapshots.IsSuccess) throw snapshots.Error!;`
  и в конструктор `ContainerSpec` добавь именованные аргументы
  `SnapshotsVolumeName: PlainEtcdNodeDriver.SnapshotsVolumeName(spec.K),
  SnapshotsVolumeDest: "/snapshots"`.
- добавь в класс `PlainEtcdNodeDriver` статический метод:
  `internal static string SnapshotsVolumeName(int k) => $"etcdw-node-{k}-snapshots";`
- в `PlainEtcdNodeDriver.RemoveNodeAsync` в ЦИКЛЕ удаления томов
  (второй цикл метода — первый удаляет контейнеры) рядом с
  `engine.DeleteVolumeAsync(VolumeName(k), ct)` добавь строку
  `engine.DeleteVolumeAsync(SnapshotsVolumeName(k), ct)` с той же
  обработкой результата.
- в `SwarmEtcdNodeDriver.EnsureNodeAsync` рядом с `EnsureVolumeAsync(
  PlainEtcdNodeDriver.VolumeName(spec.K), …)` добавь второй вызов
  `EnsureVolumeAsync(PlainEtcdNodeDriver.SnapshotsVolumeName(spec.K), …)`
  с той же проверкой, а в `template` (ContainerSpec) — те же именованные
  аргументы SnapshotsVolumeName/SnapshotsVolumeDest.
- в `SwarmEtcdNodeDriver.RemoveNodeAsync` в цикле
  `foreach (var engine in _nodeEngines.Values.Append(_engine))` рядом с
  `DeleteVolumeAsync(PlainEtcdNodeDriver.VolumeName(k), ct)` добавь
  `DeleteVolumeAsync(PlainEtcdNodeDriver.SnapshotsVolumeName(k), ct)`.
- в `ListNodeObjectsAsync` (Plain и Swarm): в отборе томов рядом с
  `.Where(v => v.EndsWith("-data", StringComparison.Ordinal))` добавь
  `|| v.EndsWith("-snapshots", StringComparison.Ordinal)`.

4.6. Перепиши `src/tests/EtcdWorker.UnitTests/Core/EtcdNodeSpecBuilderTests.cs`
под новую сигнатуру: хелпер
`Build(int k, string? advertisedHost, int clientHostPort)` создаёт
`new EtcdProvisioningOptions("Plain", "etcdw-net", "img", advertisedHost,
18100, 18199, "local", 100, 300, 15, 360, 300, 300, 30, 10, 28, 60, null)`
и зовёт `EtcdNodeSpecBuilder.Build(k, opts, clientHostPort)`. Кейсы (AAA):
`EnvCarriesEtcdArgs` — `env["ETCD_ARGS"]` содержит
`--name=etcdw-node-1`, `--initial-cluster-state=existing`,
`--advertise-client-urls=http://etcdw-node-1:2379` (без host);
`WithAdvertisedHost_ClientUrlsInArgs` — содержит
`--advertise-client-urls=http://etcdw-node-1:2379,http://10.0.0.5:23791`;
`InitialClusterPassed` — содержит `--initial-cluster=seed=http://seed:2380`;
`CmdEmpty_ImageCmdUsed` — `cmd` пустой (Length == 0);
`AgentEnvPresent` — `env["ETCDNODE_NODE_NAME"]=="etcdw-node-1"`,
`env["ETCDNODE_SNAPSHOTS__INTERVAL_MIN"]=="360"`,
`env["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"]=="28"`,
`env` НЕ содержит `ETCDNODE_S3_CA_PEM`;
`AgentEnvCarriesCaPem` — при последнем аргументе `"PEM"` —
`env["ETCDNODE_S3_CA_PEM"]=="PEM"`.
Существующие кейсы старого файла (client-urls порядок и т.п.) сохрани,
адаптировав под env-строку, КРОМЕ кейса `Env_IsEmpty` (строки ~67–75,
`env.Should().BeEmpty()`) — его УДАЛИ: env теперь всегда непуст
(`ETCD_ARGS`, `ETCDNODE_*`) и старое утверждение неверно.

4.7. В `src/tests/EtcdWorker.IntegrationTests/Membership/MembershipProcessTests.cs`
в `NewRig` (конструктор `EtcdProvisioningOptions`, образ
`"quay.io/coreos/etcd:v3.5.21"`) замени образ на `"pgworker/etcdnode:dev"`
и допиши 8 новых хвостовых аргументов: `360, 300, 300, 30, 10, 28, 60, null`.

4.8. Скомпилируй
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ успех; прогони
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --nologo`
→ Passed (интеграционные правятся на шаге 8; их сборку проверит шаг 8).

- [ ] **Шаг 4 закрыт** (сильная модель: ревью ContainerSpec-расширения на
  идемпотентность; коммит `feat(etcdworker): managed node from EtcdNode image — ETCD_ARGS env, snapshots volume`).**

---

### Шаг 5. Docker-набор образа pgworker/etcdnode:dev

**Вход:** шаг 3 закрыт (publish EtcdNode.App возможен).

**Действие (сильная модель):** создать `docker/etcdnode/{Dockerfile,supervisord.conf,entrypoint.sh}`
по прецеденту `docker/node/` (publish на хосте → COPY артефактов; supervisord
с двумя программами). Проверить сборку вручную:
`dotnet publish src/EtcdNode.App -c Release -r <rid> --self-contained -o artifacts/etcdnode`
затем
`docker build -f docker/etcdnode/Dockerfile --build-arg ETCDNODE_PUBLISH_DIR=artifacts/etcdnode -t pgworker/etcdnode:dev .`
из корня worktree; быстрый smoke: контейнер с env
`ETCD_ARGS="--name=t1 --data-dir=/etcd-data --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://localhost:2379 --listen-peer-urls=http://0.0.0.0:2380 --initial-advertise-peer-urls=http://t1:2380 --initial-cluster=t1=http://t1:2380 --initial-cluster-state=new"`
поднимается, `etcdctl`/range по client-порту жив, агент в логах стартовал
(ошибки подключения к localhost:2379 до старта etcd — норма, supervisord
перезапускает).

**Выход:** образ собирается двумя шагами, узел-контейнер поднимается, etcd
здоров, агент жив.

**Проверка:** smoke выше (range по `/v3/kv/range` отвечает; `docker logs`
содержит строки агента; после остановки etcd-процесса supervisord его
перезапускает — опционально). Контейнер и тома зачистить.

**Spec:** §3.2 (образ), решение №1.

**Подпункты (слабая модель):**

5.1. Создай `docker/etcdnode/Dockerfile`:

```dockerfile
# Образ managed-узла etcd-контура (arch/etcdnode/01-agent.md §1): внешний
# etcd + supervisord + self-contained агент EtcdNode. Сборка НА ХОСТЕ, в
# образ — ТОЛЬКО publish-вывод (канон поставки узлов, прецедент
# docker/node/Dockerfile):
#   dotnet publish src/EtcdNode.App -c Release -r <rid> --self-contained -o artifacts/etcdnode
#   docker build -f docker/etcdnode/Dockerfile \
#     --build-arg ETCDNODE_PUBLISH_DIR=artifacts/etcdnode -t pgworker/etcdnode:dev .
# В registry НЕ класть (локальный образ).

FROM 192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21

RUN apt-get update \
    && apt-get install -y --no-install-recommends supervisor \
    && rm -rf /var/lib/apt/lists/*

ARG ETCDNODE_PUBLISH_DIR=artifacts/etcdnode
COPY ${ETCDNODE_PUBLISH_DIR} /opt/etcdnode/
RUN chmod +x /opt/etcdnode/EtcdNode.App

COPY docker/etcdnode/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/etcdnode/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Клиентская грань публикуется воркером (PortRange); новых портов нет.
EXPOSE 2379 2380

VOLUME ["/etcd-data", "/snapshots"]

CMD ["/entrypoint.sh"]
```

Если базовый образ etcd не Debian-based и apt недоступен — остановись и
верни ошибку исполнителю (не заменяй базу самостоятельно).

5.2. Создай `docker/etcdnode/supervisord.conf`:

```ini
; Супервизор узла EtcdNode (arch/etcdnode/01-agent.md §1): etcd и агент —
; равноправные программы; смерть любого — autorestart. Флаги etcd — env
; ETCD_ARGS (их формирует EtcdNodeSpecBuilder воркера).

[supervisord]
nodaemon=true
logfile=/dev/null
logfile_maxbytes=0
pidfile=/run/supervisord.pid

[unix_http_server]
file=/run/supervisor.sock
chmod=0700

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///run/supervisor.sock

[program:etcd]
command=/bin/sh -c "etcd $(ETCD_ARGS)"
directory=/etcd-data
autorestart=true
priority=10
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
redirect_stderr=true

[program:etcdnode]
command=/opt/etcdnode/EtcdNode.App
autorestart=true
priority=20
environment=DOTNET_gcHeapHardLimit="40000000"
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
redirect_stderr=true
```

Строку `environment=DOTNET_gcHeapHardLimit="40000000"` можно опустить —
она необязательна; сохрани её только если соседние образы проекта так
делают (docker/node), иначе удали.

5.3. Создай `docker/etcdnode/entrypoint.sh`:

```sh
#!/bin/sh
# Entrypoint узла EtcdNode (arch/etcdnode/01-agent.md §1): гвард обязательных
# env + supervisord (nodaemon, PID 1).
set -eu
: "${ETCD_ARGS:?ETCD_ARGS is required (flags of etcd, from EtcdWorker EtcdNodeSpecBuilder)}"
mkdir -p /etcd-data /snapshots
exec /usr/bin/supervisord -c /etc/supervisor/supervisord.conf
```

- [ ] **Шаг 5 закрыт** (сильная модель: smoke-проверка образа и зачистка
  контейнера/томов; коммит `build(etcdnode): node image — etcd + supervisord + agent`).**

---

### Шаг 6. Панель: статус-ключ /etcdnode/snapshots

**Вход:** шаг 2 закрыт.

**Действие (сильная модель):** переключить панель на новый ключ, поправить
комментарии; проверить юниты панели на старый ключ (grep
`etcdworker/snapshots` по `src/AdminPanel.*` и `src/tests/AdminPanel.*`).

**Выход:** панель читает `/etcdnode/snapshots`; тесты панели зелёные.

**Проверка:** сборка 0 warnings;
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build --nologo`
зелёный (если есть тесты парсера/рефрешера по ключу).

**Spec:** §3.5, решение №4.

**Подпункты (слабая модель):**

6.1. В `src/AdminPanel.Etcd/SnapshotRefresher.cs` (строка ~470):
константу
`public const string EtcdSnapshots = "/etcdworker/snapshots"; // статус DR-слепков EtcdWorker (arch/etcdworker/00-etcd-contract.md)`
замени на
`public const string EtcdSnapshots = "/etcdnode/snapshots"; // статус DR-слепков EtcdNode (arch/etcdnode/00-etcdnode-contract.md)`.
Комментарий на строке ~249 «ровно один ключ /etcdworker/snapshots» замени
на «ровно один ключ /etcdnode/snapshots».

6.2. В `src/AdminPanel.Etcd/SnapshotBuilder.cs` (строка ~64) комментарий
`// /etcdworker/snapshots (adminpanel/02 §2.3.5)` замени на
`// /etcdnode/snapshots (adminpanel/02 §2.3.5)`. В
`src/AdminPanel.Etcd/Parsing/EtcdSnapshotsParser.cs` докстринг
«ключа /etcdworker/snapshots (arch/etcdworker/00-etcd-contract.md)» замени
на «ключа /etcdnode/snapshots (arch/etcdnode/00-etcdnode-contract.md)».

6.3. Прогони grep по `src/` и `src/tests/`:
`grep -rn "etcdworker/snapshots" src/ --include="*.cs" | grep -v obj` —
должно быть ПУСТО; оставшиеся вхождения поправь по образцу 6.1.

- [ ] **Шаг 6 закрыт** (коммит `feat(adminpanel): read DR-snapshot status from /etcdnode/snapshots`).**

---

### Шаг 7. Подготовка статики: сборка образа в PgWorker.E2ePrepare

**Вход:** шаги 3–5 закрыты.

**Действие (сильная модель):** расширить `EnsureStaticAsync` в
`src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` блоком сборки
`pgworker/etcdnode:dev` (по образцу блока `NodeImage`: publish
self-contained → `artifacts/e2e/etcdnode` → docker build c build-arg,
контекст — корень); метки `[PHASE]` через `E2eFixture.StaticPhase`, логи
`E2eFixture.StaticProcessLogPath("etcdnode-publish"/"etcdnode-image")`,
`E2eFixture.RunProcessAsync`, `E2eFixture.NoMsBuildReuseEnv` на publish,
таймаут 10 минут на шаг. Блок ставить ПОСЛЕ сборки `NodeImage` (строка
после `"_staticReady = true;"` — ВСТРО, т.е. до присваивания `_staticReady = true`).

**Выход:** `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
собирает и pgworker-образы, и `pgworker/etcdnode:dev`.

**Проверка:** запуск утилиты — `[PHASE]`-строки etcdnode, штамп записан;
`docker image inspect pgworker/etcdnode:dev` → есть.

**Spec:** §3.6 (подготовка), решение №11.

**Подпункты (слабая модель):**

7.1. В `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` найди блок
сбора `NodeImage` в `EnsureStaticAsync` (финал блока — строка
`E2eFixture.StaticPhase($"e2e-image {NodeImage}: ready in …`)`) и сразу
ПОСЛЕ неё, ДО строки `_staticReady = true;`, вставь (паттерн соседних
блоков: при `noBuild` (PGW_TEST_E2E_NOBUILD=1, бисект-режим) publish
заменяется проверкой наличия каталога `E2eFixture.EnsurePublishedDir`,
иначе — publish; docker build выполняется всегда из готового каталога):

```csharp
            // Образ managed-узла etcd-контура (arch/etcdnode/01-agent.md):
            // publish агента self-contained НА ХОСТЕ → docker build (в образ
            // только артефакты); фиксированный тег pgworker/etcdnode:dev —
            // его ждут интеграционные фикстуры EtcdWorker/EtcdNode.
            var etcdNodeDir = Path.Combine(_root, "artifacts", "e2e", "etcdnode");
            sw = E2eFixture.StaticSw();
            if (noBuild)
                E2eFixture.EnsurePublishedDir(etcdNodeDir,
                    "required: dotnet publish src/EtcdNode.App -c Release -r <rid> --self-contained -o artifacts/e2e/etcdnode");
            else
            {
                E2eFixture.StaticPhase("etcdnode-image pgworker/etcdnode:dev: dotnet publish EtcdNode.App self-contained (host)…");
                await E2eFixture.RunProcessAsync("dotnet",
                [
                    "publish", $"{_root}/src/EtcdNode.App/EtcdNode.App.csproj",
                    "-c", "Release", "-r", nodeRid, "--self-contained", "-o", etcdNodeDir, "--nologo",
                ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                    logFile: E2eFixture.StaticProcessLogPath("etcdnode-publish"));
            }

            E2eFixture.StaticPhase("etcdnode-image pgworker/etcdnode:dev: docker build (context root)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/etcdnode/Dockerfile",
                "--build-arg", "ETCDNODE_PUBLISH_DIR=artifacts/e2e/etcdnode",
                "-t", "pgworker/etcdnode:dev", _root,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: E2eFixture.StaticProcessLogPath("etcdnode-image"));
            E2eFixture.StaticPhase($"etcdnode-image pgworker/etcdnode:dev: ready in {sw.Elapsed.TotalSeconds:F0} s");
```

Обрати внимание: переменная `sw` уже объявлена выше в методе (повторное
присваивание без типа); `_root`, `nodeRid` и `noBuild` уже определены выше
в методе — используй их без повторных объявлений.

- [ ] **Шаг 7 закрыт** (сильная модель: запуск утилиты целиком, проверка
  образа; коммит `build(e2e-prepare): build pgworker/etcdnode image`).**

---

### Шаг 8. EtcdWorker.IntegrationTests: гейт образа

**Вход:** шаги 4, 7 закрыты; образ `pgworker/etcdnode:dev` собран утилитой.

**Действие (сильная модель):** фикстура `EtcdWorkerEnvironment` получает
гейт наличия образа (docker CLI `image inspect` через
`DotNet.Process`? — нет: через testcontainers нельзя; запуск
`docker image inspect pgworker/etcdnode:dev` процесом из фикстуры,
аналогичных механик в репо нет — использовать
`System.Diagnostics.Process` с перенаправлением вывода; fail-fast в
`InitializeAsync` ДО старта контейнеров: `ApplicationException`
«pgworker/etcdnode:dev image is missing — run
DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release
(статика тестов не подготовлена)»). Гейт ставить в `InitializeAsync` до
`_network.CreateAsync` — managed-контейнеры создают membership-тесты, но
гейт единый в окружении (простота). Snapshots-подкаталог уже удалён (шаг 2).

**Выход:** интеграционные тесты EtcdWorker собираются; без образа падают
быстрым fail-fast с подсказкой; с образом — зелёные (membership создаёт
узлы из образа агента).

**Проверка:** сборка; без образа — первый тест падает с подсказкой;
после `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`:
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.IntegrationTests -c Release --no-build --nologo`
зелёный; зачистка контейнеров/сетей прогона после серии.

**Spec:** §3.6 (фикстура EtcdWorker), решения №1, №11.

**Подпункты (слабая модель):**

8.1. В `src/tests/EtcdWorker.IntegrationTests/Docker/EtcdWorkerEnvironment.cs`
в `InitializeAsync` ПЕРВОЙ строкой (до `await _network.CreateAsync(ct);`)
добавь вызов `await EnsureEtcdNodeImageAsync(ct);` и добавь в класс метод:

```csharp
    // Гейт статики (arch/etcdnode/01-agent.md): membership-тесты создают
    // managed-узлы из образа агента — без него серия падает пачкой
    // непонятных ошибок; fail-fast с рецептом.
    private static async Task EnsureEtcdNodeImageAsync(CancellationToken ct)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker",
            "image inspect pgworker/etcdnode:dev")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = System.Diagnostics.Process.Start(psi)!;
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new ApplicationException(
                "pgworker/etcdnode:dev image is missing — static assets are not prepared. "
                + "Run: DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release");
    }
```

using `System.Diagnostics` в файл не добавляй, если конфликтует с
существующими using — используй полные имена (как во фрагменте).

- [ ] **Шаг 8 закрыт** (сильная модель: прогон серии, зачистка; коммит
  `test(etcdworker): fail-fast gate on etcdnode image`).**

---

### Шаг 9. EtcdNode.UnitTests: покрытие перенесённого конвейера

**Вход:** шаги 1–2 закрыты.

**Действие (сильная модель):** убедиться, что перенесённые юниты
(EtcdExportRetentionTests/EtcdExportStatusJsonTests/EtcdBackupTargetResolverTests)
зелёные и покрывают формат; добавить недостающие точечные кейсы:
`EtcdExportStatusJsonTests` — ключ равен `/etcdnode/snapshots` (assert на
`EtcdSnapshotStatusJson.Key`); `SnapshotSchedule`-тесты уже написаны
(шаг 1). AAA-комментарии.

**Выход:** EtcdNode.UnitTests зелёные, покрытие статуса/ретенции/резолвера
перенесено.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release --no-build --nologo`
→ зелёный.

**Spec:** §3.6 (юниты), решение №4.

**Подпункты: механической работы сверх описанного нет — тесты пишет
сильная модель (требует знания фейков и форматов).**

- [ ] **Шаг 9 закрыт** (коммит `test(etcdnode): unit coverage for moved pipeline + status key`).**

---

### Шаг 10. EtcdNode.IntegrationTests: окружение и поведенческие кейсы

**Вход:** шаги 3–5, 7–9 закрыты; образ собран.

**Действие (сильная модель):** новый проект
`src/tests/EtcdNode.IntegrationTests` (csproj по образцу
`EtcdWorker.IntegrationTests.csproj`, ProjectReference только на
`EtcdNode.Core` + `EtcdNode.App`; packages как в образце) + slnx. Окружение
`EtcdNodeEnvironment` (по паттерну `EtcdWorkerEnvironment`): testcontainers-сеть
`etcdn-<guid>-net`; ДВА контейнера из образа `pgworker/etcdnode:dev`
(aliases `node1`, `node2`; оба `--initial-cluster-state=new` в общем
`ETCD_ARGS` `node1=http://node1:2380,node2=http://node2:2380`; host-порт
2379 только у node1, динамический; env агента с БЫСТРЫМИ интервалами:
`ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC=1`,
`ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC=1`,
`ETCDNODE_SNAPSHOTS__TIMEOUT_SEC=10`, остальные дефолты env скрипт
передаёт явно: INTERVAL_MIN=360, RETENTION_FILES=10, RETENTION_OBJECTS=28,
MAINTENANCE_INTERVAL_MIN=60; `ETCDNODE_NODE_NAME=node1`/`node2`);
ленивая ownS3-нода + `SeedEtcdBackupClusterAsync` (перенос механики из
`EtcdWorkerEnvironment`: bucket, /s3/clusters/eb/*); teardown полный
(ключи тестового префикса → контейнеры → сеть) при любом исходе; гейт
образа — тот же `EnsureEtcdNodeImageAsync` (скопировать).

Тест-классы (AAA-комментарии; все ожидания — поллинг с бюджетом ≤ 30 с):
1. `SnapshotAgentTests` (коллекция на окружении):
   - `Leader_Elected_StatusWritten_ObjectsInS3`: посев цели → ждём ключ
     `/etcdnode/snapshots` state=OK → в bucket префикса `etcd/` есть пара
     `.db`+`.meta.json`; meta.instance — `node1` или `node2` (имя узла).
   - `Pulse_DoesNotTrigger_Snapshot`: снять текущий список объектов; далее
     15 с писать в цикле `PutAsync` `/service/x/leader` и
     `/clusters/shop/shards/s1/master` (каждую секунду); списки объектов
     НЕ растут сверх уже снятых (число пар не увеличилось; допускается
     плановый — нет, интервал 360 мин); assertion: количество `.db`-объектов
     не изменилось.
   - `ImportantChange_Triggers_Snapshot`: написать
     `/clusters/shop/shards/s1/routing` `{"to":"s2"}` → в бюджете ~30 с
     число `.db`-объектов выросло ≥ 1.
   - `LeaderKey_Single_Instance_Keys_Both`: `/etcdnode/leader` существует
     (один), ключи живости `/etcdnode/instances/node1` и
     `/etcdnode/instances/node2` существуют (instanceId ClaimStore — имена
     узлов, шаг 2.4).
2. `SnapshotExporterTests` (перенос механики из старого
   `EtcdWorker.IntegrationTests/Snapshots/SnapshotExportTests.cs`,
   переписанный на gateway окружения node1 и `EtcdNode.Core`-типы):
   Planned_Export_UploadsPair_StatusOk; BrokenEndpoints→FAILED маркер
   `etcd-backup-missing` → CatchUp; ретенция пар; статус-ключ
   `/etcdnode/snapshots`.
   ВНИМАНИЕ: эти кейсы мутируют общий с живым агентом статус-ключ/том —
   кейсы выполняются ДО старта второго контейнера? Нет: изоляция —
   экспортёр-кейсы используют СВОЙ каталог tempdir и ключ один; помехи
   агента допустимы (CatchUp агента идемпотентен); порядок кейсов не
   фиксировать, assertions толерантны к лишним объектам (≥, не ровно).

**Выход:** новый интеграционный проект зелёный на docker-хосте.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.IntegrationTests -c Release --no-build --nologo`
→ зелёный; teardown-ассерты чистоты прошли; после серии — зачистка
(docker ps/networks — пусто от префиксов прогона).

**Spec:** §3.6 (интеграции: фильтр, лидерство, экспортёр), решения №1–№5,
№13; критерии приёмки 3–4.

**Подпункты (слабая модель):**

10.1. Создай `src/tests/EtcdNode.IntegrationTests/EtcdNode.IntegrationTests.csproj`
— копия `src/tests/EtcdWorker.IntegrationTests/EtcdWorker.IntegrationTests.csproj`,
в которой ProjectReference-блок заменён на:

```xml
    <ItemGroup>
        <ProjectReference Include="..\..\Shared.Etcd\Shared.Etcd.csproj"/>
        <ProjectReference Include="..\..\EtcdNode.Core\EtcdNode.Core.csproj"/>
        <ProjectReference Include="..\..\EtcdNode.App\EtcdNode.App.csproj"/>
    </ItemGroup>
```

Пакет `Microsoft.AspNetCore.Mvc.Testing` из образца УДАЛИ (web-fixture нет).

10.2. Добавь в `src/PgWorker.slnx` строку
`<Project Path="tests/EtcdNode.IntegrationTests/EtcdNode.IntegrationTests.csproj" />`
в папку тестов ПЕРЕД строкой EtcdWorker.UnitTests.

- [ ] **Шаг 10 закрыт** (сильная модель: окружение + тесты + прогон +
  зачистка; коммит `test(etcdnode): integration — live agent, filter, leadership, exporter`).**

---

### Шаг 11. Финальный прогон и сверка приёмки

**Вход:** шаги 1–10 закрыты.

**Действие (сильная модель):** полный прогон одной командой
`scripts/run-all-tests.sh` (ранбук `arch/operations/05-run-all-tests.md`;
перед прогоном — dev-стенд не поднят, образы подготовлены утилитой из
фазы скрипта). Следить по логам (правила AGENTS.base.md §13: старт ≤ 15 с,
проверка каждые 2 мин, зависание > 5 мин — онлайн-разбор). После прогона —
зачистка остаточных контейнеров/сетей по канону; сверка критериев приёмки
spec §7 по пунктам 1–8 (grep-доказательства: п.2 —
`grep -rn "SnapshotLoop\|SnapshotExporter\|EtcdBackupTargetResolver\|EtcdSnapshotS3" src/EtcdWorker.*`
пусто; п.8 — `grep -rn "04-snapshots\|c-snapshot-export" arch/` — только
`arch/etcdnode/`-файлы). Любые фейлы ВНЕ области задачи — не чинить:
фиксировать в `arch/roadmap/bugs.md` по правилам и продолжать.

**Выход:** полный прогон зелёный; критерии приёмки подтверждены.

**Проверка:** итоговая строка прогона (все серии exit=0); отчёт скрипта
без leftover-критики; чек-лист приёмки отмечен.

**Spec:** §4 Фаза D, §7 (все критерии).

**Подпункты: нет (прогон и анализ — сильная модель).**

- [ ] **Шаг 11 закрыт** (коммит финальных правок, если требуются; сводка
  приёмки — в отчёт исполнителя).**

---

## Сводка соответствия spec → шаги

| Spec | Шаги |
|---|---|
| §3.1 EtcdNode.Core (перенос 1:1 + фильтр) | 1, 2 |
| §3.1 EtcdNode.App (агент) | 3 |
| §3.2 Образ узла | 5 (+7 сборка статики) |
| §3.3 Демонтаж EtcdWorker | 2, 4 |
| §3.4 Фильтр важности | 1 (типы), 3 (цикл), 10 (интеграции) |
| §3.5 Панель | 6 |
| §3.6 Тесты и статика | 1, 9, 10, 7, 8 |
| §4 Фазы A–D | 1–5 (A), 2/4/6 (B), 7–10 (C), 11 (D) |
| §7 Критерии 1–8 | 11 (+пошагово: 1,2,4,6,8,10) |

Канал NodeSnapshot→env узла — через `EtcdProvisioningOptions`: уточнение
зафиксировано в spec §3.3 (Finding 10 ревью) и каноне
`arch/etcdnode/01-agent.md` §3; шаг 4 следует ему буквально.
