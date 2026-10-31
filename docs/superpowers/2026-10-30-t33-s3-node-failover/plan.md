# t33-s3-node-failover — план реализации (Фаза 3)

> **Исполнителям:** план исполняет СИЛЬНАЯ модель (по умолчанию — субагент coder,
> делает всё сама, включая подпункты; при executor="main" — main-агент, отдающий
> механические подпункты субагенту-слабой модели без контекста проекта).
> Каждый шаг = Вход / Действие / Выход / Проверка / Связь со spec. Подпункты
> (где есть) — самодостаточные задания для СЛАБОЙ модели: передаётся ТОЛЬКО
> текст подпункта; всё нужное — внутри него.

**Цель:** выгрузка слепков etcd (`EtcdSnapshotSink`) получает перебор целей:
настройка пары становится запятая-списком `PGW_SNAPSHOTS_S3_ENDPOINTS`,
носитель перебора — существующий `PoolBackupS3` (канон arch/19 §5), бюджеты и
статус-семантика sink — без изменений.

**Архитектура:** arch-first (arch/14 §6/§8, arch/19 §7(в) — первой фазой),
затем код зеркалит: `SnapshotExportS3Options.Endpoints` (запятая-список URI) →
`EtcdSnapshotSinkFactory.Create` возвращает `PoolBackupS3` (write=read=список,
per-цель `BackupS3(S3ClientOptions(uri, bucket, access, secret, Region,
CaBundlePath))`, дефолтные таймаут 10 с и breaker 45 с). Прочие S3-клиенты
системы — без изменений (инвентаризация spec §3: пробел один, строка 8).

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
AWSSDK.S3 (как есть), xUnit + FluentAssertions, docker-E2E (ownS3/etcd).

**Spec:** `docs/superpowers/2026-10-30-t33-s3-node-failover/spec.md` (рядом;
план аргументирует от spec — spec читается вместе с планом).

**Рабочая директория:** worktree `/Users/demakaev/ZCodeProject/worktrees/t33-s3-node-failover`
(ветка `t33-s3-node-failover`). Все пути ниже — от корня worktree.

## Глобальные ограничения (из spec §2/§7/§8 + правила проекта)

- Бюджеты/значения НЕ меняются: попытка `TimeoutSec=30`, пер-попытка 10 с
  (дефолт `BackupS3`), breaker 45 с (`PoolBackupS3.DefaultBreakerWindow`).
- Статус-семантика sink неизменна: неудача → `/pgworker/etcd-snapshots`
  `state=FAILED` + `error`, догоняет `CatchUpAsync`; новых маркеров/алертов/ключей НЕ вводится.
- P/Invoke запрещён; только управляемый .NET API (AWSSDK — как есть).
- Скипов в тестах нет; каждый тест чистит за собой (полный teardown при любом
  исходе); порты только динамические; `[PHASE]`-метки для фаз длиннее 60 с.
- Язык: документация/комментарии — русские, идентификаторы — английские.
- Область — ТОЛЬКО etcd-export sink (spec §3/§8-1): панель, S3Worker,
  WalReceiver-операции, sweeper — НЕ трогать.
- Исторических пометок (атрибуций слитых задач) в правимых документах не оставлять.
- Документация — «текущее и планируемое состояние», без истории.
- Grep-проверки «старого имени больше нет» НЕ включают `docs/`:
  `docs/superpowers/` — архив истории задач (исключение из правила «без
  исторических пометок»), старое имя `PGW_SNAPSHOTS_S3_ENDPOINT` остаётся там
  навечно (spec/планы слитых задач) — это канон, не дефект.
- Коммиты — в feature-ветке свободно (сообщения в стиле репозитория:
  `arch:`/`feat(backups):`/`test(...):` + развёрнутое тело по-русски).

## Review Focus (классы входов, специфичные для этой задачи)

1. Пустой список целей (пустая строка/только разделители/пробелы) при
   `Enabled=true` → fail-fast старта, не молчаливый экспорт «в никуда» →
   тесты `SnapshotExportOptionsTests` (шаг 3).
2. Одиночный URI (бывший `Endpoint`) — частный случай списка, работает →
   `ParseEndpoints`-кейсы (шаг 3) + существующие E2E классов шага 6.
3. Отказ ПЕРВОЙ цели при живой второй — выкладка продолжается на вторую →
   интеграция `FirstTargetDown…` (шаг 4) + E2E `E2eEtcdSnapshotFailoverScenarios` (шаг 5).
4. Отказ ВСЕХ целей — `FAILED` + `error` с маркером `s3-pool-exhausted`, поля
   последнего успеха сохранены, доводка `CatchUpAsync` → интеграция
   `BothTargetsDown…` (шаг 4).
5. Деплой со старым env-именем `PGW_SNAPSHOTS_S3_ENDPOINT` (список пуст,
   остальные поля пары заполнены — их имена не менялись) — fail-fast старта,
   не тихий отказ → `IsValid`-кейсы (шаг 3, в изоляции) + существующий
   fail-fast E2E `PoolSnapshotsOwnPairScenarios` (шаг 6).

---

## Шаг 1. arch: контракт списка целей (ПЕРВОЙ фазой)

**Вход:** worktree t33, spec одобрен; код ещё не тронут.

**Действие** (сильная модель; arch — канон, правки в тоне окружающего текста,
без исторических пометок):

1. `arch/14-pgworker.md`, §6 «Надёжность», буллит «**Экспорт слепков в S3**»
   (начинается словами «каждый снятый слепок (плановый и внеочередный) уезжает
   в bucket ОТДЕЛЬНОЙ per-install env-пары…»). В буллите:
   - после «…env-пары `PgWorker:Snapshots:Export:S3` (env `PGW_SNAPSHOTS_S3_*` — не
     креды/bucket пулов бэкапов: …; обязательна при `Export:Enabled=true` — fail-fast
     старта)» дополнить, что цель выгрузки — СПИСОК endpoints env-пары
     (`env PGW_SNAPSHOTS_S3_ENDPOINTS`, запятая; одиночный адрес — частный
     случай), перебор целей до первого доступного — по канону arch/19 §5
     (per-попыточный таймаут, breaker-окно, «быстрый отказ/успех»);
   - добавить: bucket, креды, Region, CA — одни на все цели пары; отказ всех
     целей — transient (FAILED статуса + доводка CatchUp тем же тиком лидера
     `RetryIntervalSec`, как сегодня для любого S3-отказа).
2. `arch/14-pgworker.md`, §8 «Конфигурация», блок
   `Export { Enabled=false, … S3 { Endpoint, Bucket, AccessKey, SecretKey,
   Region?, CaBundlePath? } } }` — поле переименовать: `S3 { Endpoints, Bucket,
   AccessKey, SecretKey, Region?, CaBundlePath? } }`; соседний комментарий
   блока дополнить: `Endpoints` — запятая-список URI (перебор до первого
   доступного, arch/19 §5); fail-fast-фразу «Endpoint/Bucket/AccessKey/
   SecretKey пусты» заменить на «Endpoints (после сплита пуст)/Bucket/
   AccessKey/SecretKey пусты».
3. `arch/19-backups.md`, §7 «Секреты», подпункт (в) (строка ~1166: «(env
   `PGW_SNAPSHOTS_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY`)»):
   - `PGW_SNAPSHOTS_S3_ENDPOINT` → `PGW_SNAPSHOTS_S3_ENDPOINTS`;
   - в том же подпункте после «обязна при `Snapshots:Export:Enabled=true`
     (fail-fast старта, [14-pgworker.md](14-pgworker.md) §8)» добавить фразу:
     цели — запятая-список URI, перебор до первого доступного (§5).

Больше в arch/ ничего не править: §9 arch/19 упоминает пару только маской
`PGW_SNAPSHOTS_S3_*` (имя в единственном числе там не перечислено — сверено
по коду на момент плана); §5 arch/19 (канон перебора) уже описан и не меняется.

**Выход:** arch/14 §6+§8 и arch/19 §7(в) описывают список целей и перебор;
единственного числа `PGW_SNAPSHOTS_S3_ENDPOINT` в правимых местах нет.

**Проверка:**
```
grep -rnE "SNAPSHOTS_S3_ENDPOINT([^A-Za-z]|$)" arch/   # пусто (единственного числа нет)
grep -n "Endpoints" arch/14-pgworker.md      # конфиг-блок §8 несёт Endpoints
grep -n "SNAPSHOTS_S3_ENDPOINTS" arch/19-backups.md  # §7(в) — новое имя
```
Плюс вычитка правок сильной моделью (соответствие букве spec §4.1).

**Связь со spec:** §4.1, §5 фаза 1, §9.2.

**Подпункты:** нет (правка канона — требует понимания arch, сильная модель).

**Commit:** `arch: t33 — слепочная пара PgWorker:Snapshots:Export:S3 — список целей PGW_SNAPSHOTS_S3_ENDPOINTS (запятая) + перебор до первого доступного по канону arch/19 §5 (arch/14 §6/§8, arch/19 §7(в)); bucket/креды/Region/CA — одни на все цели, отказ всех целей — transient (FAILED + CatchUp)`.

---

## Шаг 2. Код: Options + фабрика + Program + deploy-шаблоны

**Вход:** шаг 1 закоммичен (arch-first).

**Действие** (сильная модель): применить подпункты 2.1–2.5, затем ревью
результатов (соответствие arch из шага 1; рефакторинги сверх подпунктов — не делать).

**Выход:** код зеркалит контракт: поле `Endpoints`, разбор-сплит, фабрика
возвращает `PoolBackupS3`, Program собирает sink через фабрику; deploy несёт
новое имя.

**Проверка** (немедленная, на этом шаге):
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.App/PgWorker.App.csproj -c Release --nologo
# Build succeeded, 0 Warning(s) (TreatWarningsAsErrors), 0 Error(s)
grep -rnE "S3__Endpoint([^A-Za-z]|$)" deploy/ src/PgWorker.App dev-stand/   # пусто
grep -rnE "PGW_SNAPSHOTS_S3_ENDPOINT([^A-Za-z]|$)" deploy/ src/PgWorker.App dev-stand/   # пусто
grep -rn "S3__Endpoints" deploy/docker-compose.yml   # новое имя на месте
```
Оговорки области проверки (не дефекты, а порядок шагов):

- Полная сборка решения (`dotnet build src/PgWorker.slnx`) на этом шаге ЕЩЁ не
  проходит: юнит-тесты (шаг 3) пока используют старое поле — полная сборка
  без warnings есть проверка шага 3.
- Область grep'ов «старого имени нет» — только прод-код и deploy
  (`src/PgWorker.App`, `deploy/`, `dev-stand/`). Строчные env-ключи в
  E2E-тестах (`src/tests/PgWorker.E2eTests/E2e/E2eEtcdSnapshotExportScenarios.cs:42`,
  `src/tests/PgWorker.E2eTests/E2e/E2eBackupPoolScenarios.cs:921`) переименовываются
  лишь в шаге 5 (5.1/5.2) — полная grep-проверка по всему `src/` выполняется
  в шаге 5.
- `docs/` в grep-проверки не включать (глобальное ограничение: архив
  `docs/superpowers/` законно хранит старое имя).

**Связь со spec:** §4.2, §4.3, §9.2, §9.6.

### Подпункт 2.1 (слабая модель): поле Endpoints + разбор + IsValid в Options.cs

Файл: `src/PgWorker.App/Options.cs` (в worktree
`/Users/demakaev/ZCodeProject/worktrees/t33-s3-node-failover`). Три правки.

Правка A. В классе `SnapshotExportOptions` найди doc-комментарий свойства S3:

```csharp
    /// <summary>Собственная пара слепков (arch/14 §6/§8; env
    /// PGW_SNAPSHOTS_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY).</summary>
    public SnapshotExportS3Options S3 { get; set; } = new();
```

замени на:

```csharp
    /// <summary>Собственная пара слепков (arch/14 §6/§8; env
    /// PGW_SNAPSHOTS_S3_ENDPOINTS/BUCKET/ACCESS_KEY/SECRET_KEY).</summary>
    public SnapshotExportS3Options S3 { get; set; } = new();
```

Правка B. В том же классе `SnapshotExportOptions` замени метод `IsValid`
(вместе с doc-комментарием) целиком. БЫЛО:

```csharp
    /// <summary>Fail-fast старта (образец BackupsOptions.IsValid): Enabled=true
    /// требует ПОЛНУЮ собственную пару S3; диапазоны — всегда.</summary>
    public bool IsValid()
        => (!Enabled
            || (!string.IsNullOrWhiteSpace(S3.Endpoint)
                && !string.IsNullOrWhiteSpace(S3.Bucket)
                && !string.IsNullOrWhiteSpace(S3.AccessKey)
                && !string.IsNullOrWhiteSpace(S3.SecretKey)))
           && RetentionObjects >= 1
           && RetryIntervalSec > 0
           && TimeoutSec > 0;
```

СТАЛО:

```csharp
    /// <summary>Fail-fast старта (образец BackupsOptions.IsValid): Enabled=true
    /// требует ПОЛНУЮ собственную пару S3 (Endpoints — ≥1 непустого URI после
    /// сплита); диапазоны — всегда.</summary>
    public bool IsValid()
        => (!Enabled
            || (SnapshotExportS3Options.ParseEndpoints(S3.Endpoints).Length > 0
                && !string.IsNullOrWhiteSpace(S3.Bucket)
                && !string.IsNullOrWhiteSpace(S3.AccessKey)
                && !string.IsNullOrWhiteSpace(S3.SecretKey)))
           && RetentionObjects >= 1
           && RetryIntervalSec > 0
           && TimeoutSec > 0;
```

Правка C. В классе `SnapshotExportS3Options` замени первое свойство и добавь
статический метод в конец класса (перед закрывающей фигурной скобкой класса).
БЫЛО:

```csharp
    public string Endpoint { get; set; } = "";
```

СТАЛО:

```csharp
    /// <summary>Цели выгрузки — запятая-список ПОЛНЫХ URI
    /// (http://h1:9000,http://h2:9000; схема обязательна — значение идёт
    /// в ServiceURL, как сегодня); перебор целей до первого доступного —
    /// arch/19 §5. Одиночный URI — частный случай.</summary>
    public string Endpoints { get; set; } = "";
```

В конец класса `SnapshotExportS3Options` (после свойства `CaBundlePath`)
добавь:

```csharp
    /// <summary>Разбор списка целей (чистая функция для тестов; прецедент —
    /// FetchWalCommand.ParseEnv): сплит ',' + RemoveEmptyEntries/TrimEntries;
    /// пустые/пробельные элементы отбрасываются.</summary>
    internal static string[] ParseEndpoints(string raw)
        => (raw ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
```

Другие свойства класса (`Bucket`, `AccessKey`, `SecretKey`, `Region`,
`CaBundlePath`) и doc-комментарий самого класса — не трогать.

### Подпункт 2.2 (слабая модель): EtcdSnapshotSinkFactory возвращает PoolBackupS3

Файл: `src/PgWorker.App/EtcdSnapshotSinkFactory.cs`. Замени содержимое файла
целиком на:

```csharp
using PgWorker.App;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Фабрика цели слепков etcd (arch/14 §6/§8): пул-клиент из
/// ОТДЕЛЬНОЙ пары PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_* —
/// запятая-список URI) — не креды пулов бэкапов. Перебор целей до первого
/// доступного — канон arch/19 §5 (PoolBackupS3: пер-попыточный таймаут —
/// дефолт BackupS3 10 с, breaker-окно DefaultBreakerWindow 45 с);
/// write=read=разобранный список пары. Тестируемая точка сборки
/// Program-синка.</summary>
public static class EtcdSnapshotSinkFactory
{
    /// <summary>Per-цельные опции пары (чистая функция для тестов): разбор
    /// запятая-списка → S3ClientOptions на каждую цель; bucket/креды/Region/
    /// CA — одни на все цели пары (arch/14 §6/§8).</summary>
    internal static S3ClientOptions[] TargetOptions(SnapshotExportS3Options s3)
        => [.. SnapshotExportS3Options.ParseEndpoints(s3.Endpoints).Select(uri => new S3ClientOptions(
            uri, s3.Bucket, s3.AccessKey, s3.SecretKey, s3.Region, s3.CaBundlePath))];

    public static PoolBackupS3 Create(PgWorkerOptions opts)
    {
        var s3 = opts.Snapshots.Export.S3;
        var perTarget = TargetOptions(s3);
        var byUri = perTarget.ToDictionary(t => t.Endpoint);
        var uris = perTarget.Select(t => t.Endpoint).ToArray();
        // write=read: цель слепочной пары записываема и читаема (arch/19 §5);
        // targetFactory — per-цель пары со СВОИМИ Region/CA (дефолтная фабрика
        // пула их не знает); requestTimeout не задаём — дефолт BackupS3 10 с.
        return new PoolBackupS3(
            uris, uris,
            s3.AccessKey, s3.SecretKey, s3.Bucket, s3.CaBundlePath,
            targetFactory: uri => new BackupS3(byUri[uri]));
    }
}
```

Типы `S3ClientOptions`, `PoolBackupS3`, `BackupS3` — из namespace
`PgWorker.Backups`; файл уже в namespace `PgWorker.Backups.EtcdExport`, внутри
того же проекта — отдельный `using PgWorker.Backups;` не нужен (соседние
namespace резолвятся). Если компилятор потребует — добавь `using PgWorker.Backups;`
после `using PgWorker.App;`.

### Подпункт 2.3 (слабая модель): Program.cs — сборка sink через фабрику

Файл: `src/PgWorker.App/Program.cs`. Найди (около строки 377) фрагмент:

```csharp
            new BackupS3(EtcdSnapshotSinkFactory.Create(opts)),
```

замени на:

```csharp
            EtcdSnapshotSinkFactory.Create(opts),
```

И комментарий двумя строками выше. БЫЛО:

```csharp
            // цель слепков — ОТДЕЛЬНАЯ пара PgWorker:Snapshots:Export:S3
            // (env PGW_SNAPSHOTS_S3_*; arch/14 §6/§8 — не креды пулов бэкапов).
```

СТАЛО:

```csharp
            // цель слепков — ОТДЕЛЬНАЯ пара PgWorker:Snapshots:Export:S3
            // (env PGW_SNAPSHOTS_S3_*; arch/14 §6/§8 — не креды пулов бэкапов):
            // перебор целей до первого доступного — PoolBackupS3 (arch/19 §5),
            // синглтон на процесс — breaker-стейт переживает попытки выгрузки.
```

Больше ничего в файле не менять.

### Подпункт 2.4 (слабая модель): deploy/docker-compose.yml — новое env-имя

Файл: `deploy/docker-compose.yml`. Найди строку (около строки 57):

```yaml
  PgWorker__Snapshots__Export__S3__Endpoint: ${PGW_SNAPSHOTS_S3_ENDPOINT:-}
```

замени на:

```yaml
  PgWorker__Snapshots__Export__S3__Endpoints: ${PGW_SNAPSHOTS_S3_ENDPOINTS:-}
```

Комментарий над блоком (строки с «Экспорт etcd-снапшотов в S3 (t36, arch/14
§6/§8)») приведи к виду без исторической пометки и с упоминанием списка:

```yaml
  # Экспорт etcd-снапшотов в S3 (arch/14 §6/§8): ОТДЕЛЬНАЯ env-пара
  # PgWorker:Snapshots:Export:S3 (свой bucket, префикс etcd/, свои креды —
  # PGW_SNAPSHOTS_S3_*; Endpoints — запятая-список URI, перебор до первого
  # доступного — arch/19 §5); обязательна при Enabled=true (fail-fast старта).
  # false → sink не подключается (поведение прежнее).
```

Остальные строки блока (Enabled/Bucket/AccessKey/SecretKey/Region/CaBundlePath)
не трогать.

### Подпункт 2.5 (слабая модель): deploy/.env.example — пример списка

Файл: `deploy/.env.example`. Найди строки (около строк 100–103):

```
# Слепки etcd — ОТДЕЛЬНАЯ env-пара (t36, arch/14 §6/§8): адрес существующего
# owns3-сервиса стенда, СВОЙ bucket (префикс etcd/), свои креды; обязательна
# при PGW_SNAPSHOTS_EXPORT_ENABLED=true (fail-fast старта), PGW_BACKUP_S3_*
# не переиспользует.
#PGW_SNAPSHOTS_S3_ENDPOINT=http://owns3:9000
```

замени на:

```
# Слепки etcd — ОТДЕЛЬНАЯ env-пара (arch/14 §6/§8): ЗАПЯТАЯ-СПИСОК адресов
# существующих owns3-нод (перебор до первого живого — отказ ноды не
# останавливает выкладку слепков; одиночный адрес — частный случай), СВОЙ
# bucket (префикс etcd/), свои креды (одни на все цели); обязательна при
# PGW_SNAPSHOTS_EXPORT_ENABLED=true (fail-fast старта), PGW_BACKUP_S3_*
# не переиспользует.
#PGW_SNAPSHOTS_S3_ENDPOINTS=http://owns3:9000,http://owns3-2:9000
```

Строки ниже (`#PGW_SNAPSHOTS_S3_BUCKET=…` и соседние) не трогать.

---

## Шаг 3. Юнит-тесты (PgWorker.UnitTests) + сборка решения

**Вход:** подпункты 2.1–2.5 применены и отревьюены.

**Действие** (сильная модель): применить подпункты 3.1–3.3, затем:

1. полная сборка решения: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   (теперь с тестами) — 0 warnings/errors;
2. прогон обновлённых юнит-классов (команды ниже);
3. ревью: AAA-комментарии по канону, никаких скипов.

**Выход:** юнит-покрытие §6.1 зелёное; решение собирается без warnings.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~SnapshotSinkFactoryTests
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~SnapshotExportOptionsTests
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~BackupsOptionsValidationTests
# ожидание: Passed! во всех трёх; Failed/Skipped — 0
```

**Связь со spec:** §6.1, §9.2, §9.7.

### Подпункт 3.1 (слабая модель): переписать SnapshotSinkFactoryTests

Файл: `src/tests/PgWorker.UnitTests/App/SnapshotSinkFactoryTests.cs`. Замени
содержимое файла целиком на:

```csharp
using PgWorker.App;
using PgWorker.Backups.EtcdExport;

// FluentAssertions и PgWorker.Backups — в GlobalUsings.cs проекта.

namespace PgWorker.UnitTests.App;

// Фабрика цели слепков (arch/14 §6/§8): пул-клиент собирается из ОТДЕЛЬНОЙ
// пары Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_ENDPOINTS — запятая-список
// URI; перебор целей до первого доступного — arch/19 §5) — не из Backups:S3
// (источник правды о DR-слепках не живёт в восстанавливаемом etcd).
public class SnapshotSinkFactoryTests
{
    // Тестовая пара S3-ключей слепков (литералы — только в константах).
    private const string TestAccessKey = "snap-ak";
    private const string TestSecretKey = "snap-sk";

    // Полная тестовая пара с настраиваемым списком целей.
    private static SnapshotExportS3Options Pair(string endpoints) => new()
    {
        Endpoints = endpoints,
        Bucket = "pgworker-etcd-snapshots",
        AccessKey = TestAccessKey,
        SecretKey = TestSecretKey,
        Region = "us-east-1",
        CaBundlePath = "/tls/ca.pem",
    };

    // AAA: разбор списка «a,b» — две цели в порядке перечисления (порядок = приоритет перебора).
    [Fact]
    public void ParseEndpoints_TwoUris_TwoTargetsInOrder()
    {
        // Act
        var targets = SnapshotExportS3Options.ParseEndpoints("http://h1:9000,http://h2:9000");

        // Assert
        targets.Should().Equal("http://h1:9000", "http://h2:9000");
    }

    // AAA: одиночный URI — частный случай; пробелы/пустые элементы отбрасываются.
    [Theory]
    [InlineData("http://only:9000", "http://only:9000")]
    [InlineData(" http://h1:9000 , , http://h2:9000 ,", "http://h1:9000|http://h2:9000")]
    [InlineData(" , ", "")]
    public void ParseEndpoints_SingleAndJunk_CleanedList(string raw, string expected)
    {
        // Act
        var targets = SnapshotExportS3Options.ParseEndpoints(raw);

        // Assert
        targets.Should().Equal(expected.Split('|', StringSplitOptions.RemoveEmptyEntries));
    }

    // AAA: per-цельные опции — bucket/креды/Region/CA ОДНИ на все цели пары,
    // Endpoint — каждой цели свой.
    [Fact]
    public void TargetOptions_PairFieldsAppliedToEveryTarget()
    {
        // Act
        var perTarget = EtcdSnapshotSinkFactory.TargetOptions(Pair("http://h1:9000,http://h2:9000"));

        // Assert
        perTarget.Should().HaveCount(2);
        perTarget.Select(t => t.Endpoint).Should().Equal("http://h1:9000", "http://h2:9000");
        foreach (var target in perTarget)
        {
            target.Bucket.Should().Be("pgworker-etcd-snapshots");
            target.AccessKey.Should().Be(TestAccessKey);
            target.SecretKey.Should().Be(TestSecretKey);
            target.Region.Should().Be("us-east-1");
            target.CaBundlePath.Should().Be("/tls/ca.pem");
        }
    }

    // AAA: Create — пул-клиент PoolBackupS3 (носитель перебора arch/19 §5;
    // write=read=список пары — по построению фабрики).
    [Fact]
    public void Create_ReturnsPoolBackupS3()
    {
        // Arrange
        var opts = new PgWorkerOptions();
        opts.Snapshots.Export.S3 = Pair("http://h1:9000,http://h2:9000");
        opts.Backups.S3 = new BackupsS3Options { Bucket = "pgworker-backups" };

        // Act
        var target = EtcdSnapshotSinkFactory.Create(opts);

        // Assert
        target.Should().BeOfType<PoolBackupS3>();
    }
}
```

### Подпункт 3.2 (слабая модель): обновить SnapshotExportOptionsTests

Файл: `src/tests/PgWorker.UnitTests/App/SnapshotExportOptionsTests.cs`. Три правки.

Правка A. Замени приватную фабрику S3 (вместе с комментариями). БЫЛО:

```csharp
    private static SnapshotExportS3Options S3(bool full = true) => new()
    {
        Endpoint = full ? "http://owns3:9000" : "",
        Bucket = full ? "pgworker-etcd-snapshots" : "",
        AccessKey = full ? "k" : "",
        SecretKey = full ? "s" : "",
    };
```

СТАЛО:

```csharp
    private static SnapshotExportS3Options S3(
        string endpoints = "http://owns3:9000",
        string bucket = "pgworker-etcd-snapshots",
        string accessKey = "k",
        string secretKey = "s") => new()
    {
        Endpoints = endpoints,
        Bucket = bucket,
        AccessKey = accessKey,
        SecretKey = secretKey,
    };
```

Правка B. В тесте `Enabled_WithoutPair_Invalid` замени вызов `S3(full: false)`
на `S3(endpoints: "", bucket: "", accessKey: "", secretKey: "")`.

Правка C. В конец класса (после теста `Ranges_AlwaysInvalid`, перед закрывающей
фигурной скобкой класса) добавь два теста:

```csharp
    // AAA: Endpoints — запятая-список: валиден список из ≥1 непустого URI.
    [Fact]
    public void Enabled_EndpointsList_Valid()
    {
        // Arrange
        var options = new SnapshotExportOptions
        {
            Enabled = true,
            S3 = S3(endpoints: "http://a:9000,http://b:9000"),
        };

        // Act/Assert
        options.IsValid().Should().BeTrue();
    }

    // AAA: Enabled=true при списке, пустом после сплита (пустая строка или
    // одни разделители/пробелы), — невалиден: fail-fast, не молчаливый
    // экспорт «в никуда» (реалистичный случай — деплой со старым env-именем:
    // Endpoints пуст, остальные поля пары заполнены — их имена не менялись).
    [Theory]
    [InlineData("")]
    [InlineData(" , ")]
    [InlineData(",")]
    public void Enabled_EndpointsEmptyList_Invalid(string endpoints)
    {
        // Arrange — пара полная, кроме списка целей (изоляция проверки).
        var options = new SnapshotExportOptions
        {
            Enabled = true,
            S3 = S3(endpoints: endpoints),
        };

        // Act/Assert
        options.IsValid().Should().BeFalse();
    }
```

Существующие тесты `DisabledByDefault_ValidWithoutS3`, `Enabled_WithFullPair_Valid`,
`Ranges_AlwaysInvalid` не менять (они компилируются с новой фабрикой как есть).

### Подпункт 3.3 (слабая модель): обновить BackupsOptionsValidationTests

Файл: `src/tests/PgWorker.UnitTests/App/BackupsOptionsValidationTests.cs`.
В тесте `Snapshots_EnabledWithoutPair_Invalid` (около строки 43):

- комментарий `// Arrange — Enabled=true, секция S3 дефолтная (Endpoint пуст).`
  замени на `// Arrange — Enabled=true, секция S3 дефолтная (Endpoints пуст).`
- в инициализаторе `new SnapshotExportS3Options` строку
  `Endpoint = "http://owns3:9000",` замени на `Endpoints = "http://owns3:9000",`.

Больше ничего в файле не менять.

**Commit (шаги 2+3 одним коммитом после зелёных проверок):**
`feat(backups): t33 — слепочная пара становится списком целей PGW_SNAPSHOTS_S3_ENDPOINTS (SnapshotExportS3Options.Endpoints, сплит ',' RemoveEmptyEntries/TrimEntries, fail-fast ≥1 URI); EtcdSnapshotSinkFactory возвращает PoolBackupS3 (write=read=список, per-цель BackupS3 c Region/CA пары, дефолтные 10 с/45 с breaker); Program — sink через фабрику (синглтон, breaker переживает попытки); deploy compose/.env.example — новое имя с примером списка; юниты: ParseEndpoints/TargetOptions/IsValid`

---

## Шаг 4. Интеграционные тесты (PgWorker.IntegrationTests, OwnEtcd+OwnS3 per-Fact)

**Вход:** шаг 3 зелёный (сборка решения без warnings).

**Действие** (сильная модель — docker-окружение требует понимания проекта;
образец — существующие факты `EtcdSnapshotSinkTests`). В файл
`src/tests/PgWorker.IntegrationTests/Backups/EtcdSnapshotSinkTests.cs`:

1. Добавить `using PgWorker.App;` к существующим using (для `PgWorkerOptions`,
   `SnapshotExportS3Options`, `EtcdSnapshotSinkFactory` — фабрика в
   `PgWorker.Backups.EtcdExport`, using уже есть).
2. Добавить приватный хелпер сборки sink'а ЧЕРЕЗ ФАБРИКУ (правда прод-сборки):

```csharp
    // Пул целей слепочной пары через ПРОД-фабрику (EtcdSnapshotSinkFactory):
    // правда сборки Program.cs — перебор целей пары (arch/19 §5).
    private static EtcdSnapshotSink FactorySink(OwnEtcd etcd, string endpoints, string instance,
        int retention = 28)
    {
        var opts = new PgWorkerOptions();
        opts.Snapshots.Export.S3 = new SnapshotExportS3Options
        {
            Endpoints = endpoints,
            Bucket = OwnS3.Bucket,
            AccessKey = OwnS3.AccessKey,
            SecretKey = OwnS3.SecretKey,
        };
        return new EtcdSnapshotSink(
            EtcdSnapshotSinkFactory.Create(opts), etcd.Gateway, [etcd.Endpoint], retention, 5, instance, 360);
    }
```

3. Добавить Fact 1 — failover (первая цель закрытый порт, вторая живая):

```csharp
    // AAA (t33): перебор целей пары — первая цель закрытый порт (прецедент
    // BadS3Runtime + E2eFixture.FreePort()), вторая — живая ownS3: выгрузка
    // доезжает до второй цели; пара db+meta реально лежит на живой ноде
    // (креды/bucket пары применились к НЕ-первой цели), статус-ключ OK.
    [Fact]
    public async Task FirstTargetDownSecondAliveUploadsToAlive()
    {
        // Arrange — своё etcd + ownS3; цели [закрытый порт, живая нода]
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink-fo1", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink-fo1", ct);
        var dir = Directory.CreateTempSubdirectory("sink-failover-").FullName;
        var bad = s3NodeFx.S3() with { Endpoint = $"http://localhost:{E2eFixture.FreePort()}" };
        var sink = FactorySink(etcdFx, $"{bad.Endpoint},{s3NodeFx.HostEndpoint}", "inst-A");
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие и выгрузка OK: пара на ЖИВОЙ (второй) цели, статус OK
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(2)
            .And.Contain(k => k.Key.EndsWith(".db"))
            .And.Contain(k => k.Key.EndsWith(".meta.json"));
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(kv.Value)!.State.Should().Be("OK");
    }
```

4. Добавить Fact 2 — исчерпание → FAILED (поля успеха сохранены) → доводка
   (расширение прецедента «транзиент → FAILED → доводка» вторым узлом):

```csharp
    // AAA (t33): обе цели закрытые порты → снятие живо, выгрузка FAILED с
    // маркером s3-pool-exhausted в error, поля последнего успеха сохранены;
    // доводка живым пулом (CatchUpAsync) — статус OK (семантика arch/19 §5:
    // исчерпание — transient, догоняет тик лидера).
    [Fact]
    public async Task BothTargetsDown_StatusFailedFieldsKept_CatchUpOk()
    {
        // Arrange — успех на живой ноде (база полей последнего успеха)
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink-fo2", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink-fo2", ct);
        var dir = Directory.CreateTempSubdirectory("sink-exhaust-").FullName;
        var liveJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60,
            FactorySink(etcdFx, s3NodeFx.HostEndpoint, "inst-A"));
        (await liveJob.TakeAsync(ct)).IsSuccess.Should().BeTrue();
        var okKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var okStatus = EtcdSnapshotStatusJson.Parse(okKv.Value)!;

        // Act — обе цели закрытые порты: снятие живо, выгрузка FAILED
        var bad1 = $"http://localhost:{E2eFixture.FreePort()}";
        var bad2 = $"http://localhost:{E2eFixture.FreePort()}";
        var brokenJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60,
            FactorySink(etcdFx, $"{bad1},{bad2}", "inst-A"));
        var shot = await brokenJob.TakeAsync(ct);

        // Assert — снятие OK; статус FAILED + маркер; поля успеха сохранены
        shot.IsSuccess.Should().BeTrue("an upload failure is transient, the snapshot does not fail");
        var failedKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var failed = EtcdSnapshotStatusJson.Parse(failedKv.Value)!;
        failed.State.Should().Be("FAILED");
        failed.Error.Should().Contain("s3-pool-exhausted");
        failed.LastObject.Should().Be(okStatus.LastObject, "the fields of the last success are kept");
        failed.LastUploadedUnix.Should().Be(okStatus.LastUploadedUnix);

        // Act/Assert — доводка живым пулом: новейший локальный доезжает, статус OK
        var catchUp = await FactorySink(etcdFx, s3NodeFx.HostEndpoint, "inst-A").CatchUpAsync(dir, ct);
        catchUp.IsSuccess.Should().BeTrue();
        var finalKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(finalKv.Value)!.State.Should().Be("OK");
    }
```

Существующие факты файла — НЕ менять (они проверяют `EtcdSnapshotSink` на
одиночном `IBackupS3` — валидно и после задачи).

**Выход:** §6.2 покрыт: failover на реальном ownS3 + исчерпание→FAILED→доводка.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~EtcdSnapshotSinkTests
# ожидание: все факты Passed (включая два новых), Failed/Skipped — 0
```
Тест требует docker-хоста и образ `owns3:dev` (внешний, `../OwnS3`); после
серии — дождаться финальной строки и зачистить остатки контейнеров/сетей серии
(канон AGENTS.md; тест самоочищается — страховочный гейт: `docker ps -a | grep pgw-eo-` пуст).

**Связь со spec:** §6.2, §4.4, §9.3.

**Commit:** `test(backups): t33 — интеграции слепочного пула: failover (первая цель закрытый порт → пара на живой ноде, статус OK) и исчерпание обеих целей (FAILED + s3-pool-exhausted, поля успеха сохранены, CatchUp → OK); sink собирается через EtcdSnapshotSinkFactory (правда прод-сборки)`

---

## Шаг 5. E2E (PgWorker.E2eTests): env-имена + новый сценарий отказа цели

**Вход:** шаг 4 зелёный.

**Действие** (сильная модель): применить подпункты 5.1–5.2 (механика), затем
написать новый сценарный класс (ТЗ 5.3) — по прецедентам существующего файла.

### Подпункт 5.1 (слабая модель): env-имя в E2eEtcdSnapshotExportScenarios.cs

Файл: `src/tests/PgWorker.E2eTests/E2e/E2eEtcdSnapshotExportScenarios.cs`.
В методе `StartExportHostAsync` (около строки 42) найди строку:

```csharp
            ["PgWorker__Snapshots__Export__S3__Endpoint"] = s3Override ?? Fx.S3EndpointForContainers, // env воркера — alias
```

замени на:

```csharp
            ["PgWorker__Snapshots__Export__S3__Endpoints"] = s3Override ?? Fx.S3EndpointForContainers, // env воркера — alias(ы)
```

Больше ничего в файле не менять (новый класс добавляет сильная модель, п. 5.3).

### Подпункт 5.2 (слабая модель): env-имя в E2eBackupPoolScenarios.cs

Файл: `src/tests/PgWorker.E2eTests/E2e/E2eBackupPoolScenarios.cs`. В классе
`PoolSnapshotsOwnPairScenarios` (около строки 921) найди строку:

```csharp
                ["PgWorker__Snapshots__Export__S3__Endpoint"] = s3Endpoint,
```

замени на:

```csharp
                ["PgWorker__Snapshots__Export__S3__Endpoints"] = s3Endpoint,
```

Больше ничего в файле не менять.

### 5.3. Новый сценарный класс (сильная модель)

В файл `src/tests/PgWorker.E2eTests/E2e/E2eEtcdSnapshotExportScenarios.cs`
(после существующей фикстуры `E2eEtcdSnapshotExportScenariosFx`) добавить:

- класс `E2eEtcdSnapshotFailoverScenarios` + фикстуру
  `E2eEtcdSnapshotFailoverScenariosFx : E2eContourFixture` с
  `protected override string Slug => "snap-fail";` и
  `protected override bool WithS3Node => true;` (прецедент —
  `E2eEtcdSnapshotExportScenariosFx`);
- один Fact `FirstTargetDown_NextSnapshotOkOnSecondNode` (имя — в фильтр гейта):

Сценарий Fact'а (все элементы обязательны, канон e2e-isolation/e2e-launch):

1. Свой 3-минутный CTS (прецедент: `cts.CancelAfter(TimeSpan.FromMinutes(3))`),
   `try/catch { Fx.MarkFailed(); throw; }`.
2. Arrange: `await Fx.StartExtraS3NodeAsync("e2e-owns3-2", ct)` — вторая
   ownS3-нода контура (прецедент AC4/AC6 `E2eBackupPoolScenarios`); через
   `Fx.RunMcAsync` (инструментальный pgw-em контейнера, прецедент
   `E2eEnvironment.EnsureOwnS3BucketAsync`) создать bucket слепков
   `pgworker-etcd-snapshots` на ОБОИХ нодах (`mc mb --ignore-existing` по
   alias `e2e-owns3:9000` и `e2e-owns3-2:9000`, креды `E2eEnvironment.S3RootUser/S3RootPassword`).
3. Старт хост-воркера `Fx.StartHostAsync("snapfail", snapshotIntervalMin: 1,
   extraEnv: …)` с набором (прецедент `StartExportHostAsync`, но СВОЙ — класс
   не наследует существующий): `PgWorker__Snapshots__Export__Enabled=true`,
   `RetryIntervalSec=5`,
   `PgWorker__Snapshots__Export__S3__Endpoints=http://e2e-owns3:9000,http://e2e-owns3-2:9000`,
   `S3__Bucket=pgworker-etcd-snapshots`, `S3__AccessKey=S3RootUser`,
   `S3__SecretKey=S3RootPassword`; владение — `await using`.
4. Wait (бюджет 90 с, `E2eFixture.WaitForAsync`): статус-ключ
   `/pgworker/etcd-snapshots` содержит `"state":"OK"`; зафиксировать
   `last_object` первого успеха (parse `EtcdSnapshotStatusJson`).
5. Act: `await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct)` —
   стоп ПЕРВОЙ ноды (контейнер `pgw-eo-{runId}`, alias `e2e-owns3`).
6. Wait (бюджет 90 с): статус `"state":"OK"` И `LastObject != firstObject`
   (следующий слепок минуты/доводка RetryIntervalSec=5 уехали на вторую цель;
   возможный промежуточный FAILED — не помеха поллингу).
7. Assert: свежая пара реально на ВТОРОЙ ноде — `mc ls --recursive` по alias
   `e2e-owns3-2:9000` (через `Fx.RunMcAsync`) содержит db-ключ нового
   `last_object` (относительно префикса `etcd/`) и его `.meta.json`
   (`dbKey[..^".db".Length] + ".meta.json"` — прецедент `McKeys`).
8. Никаких скипов; teardown — фикстура (own-only чистка `pgw-*` подберёт и
   остановленную первую ноду, и extra-ноду `pgw-eo-e2e-owns3-2-{runId}`);
   долгие фазы (старт extra-ноды, ожидания) обернуть `E2eFixture.TimedAsync`
   с говорящими метками (`[TIMING]` → тайминговый журнал).

**Выход:** существующие E2E несут новое env-имя; новый сценарий доказывает
отказоустойчивость выкладки слепков end-to-end; старого имени нет во всём
рабочем коде.

**Проверка:** компиляция тестового проекта + полная grep-проверка старого
имени по всему рабочему коду (E2E-файлы переименованы в 5.1/5.2 — теперь
матчингов быть не должно):
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.E2eTests/PgWorker.E2eTests.csproj -c Release --nologo
# Build succeeded, 0 Warning(s), 0 Error(s)
grep -rnE "S3__Endpoint([^A-Za-z]|$)" src/           # пусто
grep -rnE "PGW_SNAPSHOTS_S3_ENDPOINT([^A-Za-z]|$)" src/ deploy/ dev-stand/   # пусто
```
(`docs/` сознательно вне проверки — архив `docs/superpowers/` законно хранит
старое имя; глобальное ограничение в начале плана.)

**Связь со spec:** §6.3 (кроме гейта — он в шаге 6), §8-11, §9.4.

**Commit:** `test(e2e): t33 — отказ первой цели слепочной пары: E2eEtcdSnapshotFailoverScenarios (две ownS3-ноды, Endpoints-список, docker stop первой → следующий слепок OK, свежая пара на второй ноде по mc); существующие сценарии экспорта — новое env-имя Endpoints`

---

## Шаг 6. Мерж-гейт: docker-E2E на свежем Release + зачистка серий

**Вход:** шаги 1–5 закоммичены.

**Действие** (сильная модель; канон AGENTS.md «E2E на свежем Release»):
 ВСЕГДА отдельными командами и в этом порядке; лог каждой команды — целиком в
файл `/tmp/t33-gate-<n>.log`, читать из файла; между сериями — дожидаться
финальной строки и зачищать контейнеры/сети (страховочный гейт:
`docker ps -a --format '{{.Names}}' | grep -c '^pgw-'` → 0;
`docker network ls | grep -c kfw-net` → 0, иначе `docker network prune -f`).

1. Статическая подготовка ОДНИМ процессом ДО тестов:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
2. Сборка (отдельно от тестов):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
3. Кейс-маркер (минимум гейта):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
4. Обновлённые + новый классы слепков (двумя запусками, между ними — зачистка):
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~E2eEtcdSnapshotExportScenarios|FullyQualifiedName~E2eEtcdSnapshotFailoverScenarios"`
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~PoolSnapshotsOwnPairScenarios`
5. После КАЖДОЙ серии: финальная строка прогона + зачистка остатков серии
   (правило AGENTS.md «Зачистка после каждой тестовой серии»).

**Выход:** гейт зелёный: E2ePrepare OK, сборка без warnings, Scale_AddEmptyShard
Passed, классы экспорта слепков (существующий + новый) и PoolSnapshotsOwnPairScenarios
Passed; скипов нет; зачистка подтверждена.

**Проверка:** перечисленные команды — все Passed/Build succeeded; в выводах
прогонов нет `Failed!`/`Skipped` (кроме допустимого `_prepareSkip` TLI, которого
в этих фильтрах нет); телеметрия по канону (логи в `/tmp/pgw-e2e-artifacts-*`
при падении — разбор БЕЗ перезапуска тестов до формулировки причин).

**Связь со spec:** §5 фаза 4, §6.3 (гейт), §9.4, §7.

**Commit (при правках по итогам гейта):** по факту правки отдельным коммитом
с указанием причины.

---

## Порядок исполнения и зависимость шагов

1 → 2 → 3 → 4 → 5 → 6 (строго; шаг 1 — arch-first, шаг 6 — только после 5).
Подпункты внутри шага применяются подряд (2.1→2.5, 3.1→3.3, 5.1→5.2), затем
общая проверка шага.
