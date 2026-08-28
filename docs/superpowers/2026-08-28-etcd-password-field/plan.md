# etcd password field — per-cluster app-секрет: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PgWorker генерирует и хранит per-cluster логин+пароль роли `app` в etcd (`/clusters/<C>/app_user`, `/clusters/<C>/app_password`); приложение читает креды только из etcd; `PGW_APP_ROLE_PASSWORD` уходит из env; механизм bucket_admin не трогается.

**Architecture:** Новые ключи пишет только держатель клэйма `<C>` через txn `put-if-absent` (новый `AppSecretEnsurer`, шаг P1.5 в Provisioning/AddShard). Роль `app` создаётся/выравнивается в БД из этих ключей (`CREATE ROLE` guard + идемпотентный `ALTER ROLE … PASSWORD`). AdminPanel только осознанно скипает новые ключи. Механизм bucket_admin (config-поля + `password=` в DSN + чтение панелью, коммиты 6edc80b/4c98338) сохраняется без изменений.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions, etcd HTTP JSON gateway (`/v3/*`), Npgsql.

**Spec:** `docs/superpowers/2026-08-28-etcd-password-field/spec.md` (ревизия 3 — план аргументируется от spec; исполнители читают оба документа).

## Global Constraints

- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-etcd-password-field`.
- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — код обязан компилироваться без ворнингов.
- Пароль app: 32 символа, алфавит `[A-Za-z0-9]` (без спецсимволов — безопасно для SQL-литералов/DSN/env/JSON).
- Механизм bucket_admin НЕ менять: config-поля `bucket_admin_user`/`bucket_admin_password`, `password=` внутри DSN-ключей, `DsnParser.Password`/`ShardInfo.Password`/`SqlProbe`-fallback панели остаются как есть (spec §1, §2.6, критерий 5).
- Значение app-пароля не появляется в: env контейнеров нод (`SpiloEnvBuilder`), DSN-ключах, config JSON, модели/UI/API AdminPanel, журнале `/pgworker/work/`, текстах исключений (spec §2.4, критерии 1/6/7/10; инвариант фиксируется тестом — Task 5).
- Все записи в etcd — только держателем клэйма; новые ключи — только через `TxnCompare.NotExists` (spec §3.1).
- Все обращения к etcd (чтение и txn) — с failover по endpoints до первого живого (единый паттерн `ReadPortAllocAsync`); повтор txn на другом endpoint безопасен для put-if-absent: проигрыш compare корректно разрешается re-read.
- Ensure-шаг процессов — единая нумерация **P1.5** (spec §3.3, ревизия 3); add-shard — процесс **G** по arch/14.
- Язык комментариев/доков — русский; идентификаторы — английские.
- Тесты — нотация AAA (комментарии Arrange/Act/Assert), как в существующих файлах.
- Коммиты — в ветке `feat-etcd-password-field` (worktree), каждый таск отдельным коммитом; формат `feat:/test:/docs:` + русское пояснение.

---

### Task 1: Канон arch/ — фиксация контракта app-секрета

**Files:**
- Modify: `arch/11-bucket-sharding.md` (§2 список ключей ~строки 100–113; §4 строка таблицы про app-роль ~строка 238; §5 строки ~573–575)
- Modify: `arch/14-pgworker.md` (§3.1 таблица `/clusters/` ~строки 180–233; §4 «Секреты» строки 264–271; §5 процесс A ~строки 305–329; §5 процесс G/add-shard)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.1 таблица строки 39–52; §6.2 строки ~229–235)
- Modify: `arch/roadmap/pgworker.md` (строки 9–11, пункт `t02-per-cluster-secrets`)

**Interfaces:**
- Consumes: формулировки spec §3.2–3.5 (переносятся в канон).
- Produces: канон, на который ссылаются задачи 2–9.

- [ ] **Step 1: arch/11-bucket-sharding.md**

1. В список «Ключи одного кластера» (после строки `/clusters/<C>/config`) добавить:

```
/clusters/<C>/app_user             → "app"          # per-cluster креды приложения (логин):
                                                     # пишет PgWorker при provisioning (генерация),
                                                     # читают PgWorker (роли/гранты) и приложение
/clusters/<C>/app_password         → "<32 симв [A-Za-z0-9]>"  # per-cluster креды приложения (пароль);
                                                     # кладётся txn put-if-absent — оба ключа атомарно
```

2. Пояснение к `shards/X/dsn` (сейчас «**Без пароля**: пароли в etcd не хранятся (P12/P17) — секреты в env/секрет-хранилище приложений и mover'а») заменить на:

```
- **shards/X/dsn** — статическая multi-host строка подключения к write-эндпоинту
  шарда (HAProxy любой ноды ведёт на текущего мастера, P2): для подписок
  переездов, mover'а и админки. У кластеров под PgWorker несёт per-cluster
  креды bucket_admin (`password=`, из config кластера). App-секрет в DSN
  не попадает никогда. В etcd хранятся: per-cluster app-пара
  (app_user/app_password — генерирует PgWorker) и per-cluster креды
  bucket_admin (config + DSN); superuser/standby/bucket_mover — per-install
  env PgWorker.
```

3. §4, строка таблицы «App-роль приложения…» — в колонку «Мера» добавить: «логин+пароль — per-cluster ключи `/clusters/<C>/app_user|app_password` (генерирует PgWorker), приложение берёт адрес из `shards/X/master` (doorman :6432)».

4. §5, фразу «Пароли шардов — там же (`SHARD_<X>_PASSWORD`, `MOVER_PASSWORD_<X>`); в etcd паролей нет.» заменить на: «Пароли шардов — там же (`SHARD_<X>_PASSWORD`, `MOVER_PASSWORD_<X>`); в etcd паролей ручных скриптовых кластеров нет. У кластеров под управлением PgWorker в etcd хранятся app-пара и bucket_admin-креды (канон §2).»

- [ ] **Step 2: arch/14-pgworker.md**

1. §3.1, в таблицу ключей `/clusters/` добавить две строки:

```
| `/clusters/<C>/app_user` | строка `"app"` | per-cluster логин приложения; пишет ТОЛЬКО PgWorker (P1.5 ensure, txn put-if-absent); читают PgWorker (роли) и приложение |
| `/clusters/<C>/app_password` | строка, 32 симв `[A-Za-z0-9]` | per-cluster пароль приложения; те же писатель/читатели; удаляется с префиксом кластера (D2) |
```

У строки `dsn` зафиксировать фактический формат: `host=… user=<bucket_admin> password=<per-cluster bucket_admin>` (как в коде, 6edc80b).

2. §4 «Секреты» — заменить раздел целиком на:

```
## 4. Секреты

Три группы:

1. **per-cluster, в etcd, генерирует PgWorker**: `app_user`/`app_password`
   (provisioning P1.5: txn put-if-absent, 32 симв `[A-Za-z0-9]`; роль app
   в БД выравнивается идемпотентным `ALTER ROLE … PASSWORD` на каждом шарде).
2. **per-cluster, в etcd, задаётся снаружи** (config JSON кластера, fallback
   env): `bucket_admin_user`/`bucket_admin_password` — попадают в dsn-ключ
   шарда и env контейнера ноды.
3. **per-install, из env PgWorker** (не в git, не в etcd — P12/P17):
   `PGW_PG_SUPERUSER_PASSWORD`, `PGW_PG_STANDBY_PASSWORD`,
   `PGW_BUCKET_ADMIN_PASSWORD` (fallback группы 2), `PGW_BUCKET_MOVER_PASSWORD`.
   `PGW_APP_ROLE_PASSWORD` исключён (app-секрет — только группа 1).
```

3. §5, процесс A: после строки `P1 план: …` вставить строку:

```
P1.5 ensure app-секрета: прочитать /clusters/<C>/{app_user,app_password};
    отсутствующие — сгенерировать (32 симв [A-Za-z0-9]) и положить ОДНОЙ txn
    (compare NotExists на отсутствующие + put); txn проигран (гонка/re-run) —
    re-read и использовать существующие; роль app на каждом шарде создаётся
    с этим паролем и выравнивается ALTER ROLE (идемпотентно)
```

Строку `P2.5 записать shards/X/dsn (multi-host, без пароля)` привести к факту: `P2.5 записать shards/X/dsn (multi-host, с per-cluster bucket_admin user+password)`.

4. §5, процесс G (add-shard): тем же текстом добавить ensure-шаг перед созданием ролей (образец P1.5).

- [ ] **Step 3: arch/adminpanel/02-etcd-contract.md**

1. §2.1, строка таблицы `dsn`, колонку «Примечания» привести к факту: «dsn PgWorker-кластеров несёт `password=` (per-cluster bucket_admin); панель разбирает его в `ShardInfo.Password`, SQL-проба использует `shard.Password ?? AdminPanel:Probes:Password`».
2. §2.1, после таблицы добавить абзац:

```
Ожидаемые игнорируемые ключи: `/clusters/<C>/app_user` и
`/clusters/<C>/app_password` — per-cluster креды приложения (генерирует
PgWorker). Панель их НЕ читает и НЕ отображает: парсер пропускает без
`unknownKeys`-счётчика, значение не попадает в модель/UI/API.
```

3. §6.2: формулировку «DSN шарда из etcd **+ Password из настроек панели**» заменить на «DSN шарда из etcd (пароль из DSN при наличии, иначе `AdminPanel:Probes:Password`; в DSN app-секрета не бывает никогда)».

- [ ] **Step 4: arch/roadmap/pgworker.md**

Пункт `t02-per-cluster-secrets` заменить на:

```
- **`t02-per-cluster-secrets`** — ротация секретов per-cluster (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Генерация per-cluster app-секрета в etcd сделана
  (2026-08-28, feat-etcd-password-field).
```

- [ ] **Step 5: Проверка канона**

Run: `grep -n "app_password" arch/11-bucket-sharding.md arch/14-pgworker.md arch/adminpanel/02-etcd-contract.md && grep -c "PGW_APP_ROLE_PASSWORD" arch/14-pgworker.md`
Expected: вхождения `app_password` во всех трёх файлах; счёт `PGW_APP_ROLE_PASSWORD` в arch/14 == 0 (из §4 исключён; если упоминается исторически вне §4 — оставить, но §4 без него).

- [ ] **Step 6: Commit**

```bash
git add arch/11-bucket-sharding.md arch/14-pgworker.md arch/adminpanel/02-etcd-contract.md arch/roadmap/pgworker.md
git commit -m "docs(arch): контракт per-cluster app-секрета в etcd (app_user/app_password), фиксация фактического bucket_admin-поведения"
```

---

### Task 2: Модель `AppCredentials` + генератор `AppSecretGenerator`

**Files:**
- Modify: `src/PgWorker.Core/Model/Domain.cs` (после record `ClusterConfig` строки ~38–41; record `ClusterSnapshot` строки ~58–60)
- Create: `src/PgWorker.Core/Model/AppSecretGenerator.cs`
- Create: `src/tests/PgWorker.UnitTests/Model/AppSecretGeneratorTests.cs`

**Interfaces:**
- Consumes: ничего (базовый таск).
- Produces:
  - `namespace PgWorker.Core.Model`: `sealed record AppCredentials(string User, string Password)`
  - `ClusterSnapshot(Config, Shards, Routing, AppCredentials? App = null)` — новый опциональный 4-й параметр (существующие вызовы `new ClusterSnapshot(...)` не ломаются)
  - `static class AppSecretGenerator { const int Length = 32; static string Generate(); }`

- [ ] **Step 1: Написать падающие тесты генератора**

`src/tests/PgWorker.UnitTests/Model/AppSecretGeneratorTests.cs`:

```csharp
using System.Text.RegularExpressions;
using PgWorker.Core.Model;
using Xunit;

namespace PgWorker.UnitTests.Model;

// Генератор per-cluster app-пароля (spec §4.1): 32 символа [A-Za-z0-9].
public partial class AppSecretGeneratorTests
{
    [GeneratedRegex("^[A-Za-z0-9]{32}$")]
    private static partial Regex PasswordPattern();

    [Fact]
    public void Generate_LengthAndAlphabet()
    {
        // Act
        var password = AppSecretGenerator.Generate();

        // Assert — 32 символа, только буквы/цифры (без спецсимволов: безопасно
        // для SQL-литералов, DSN, env, JSON — spec §4.1)
        PasswordPattern().IsMatch(password).Should().BeTrue();
    }

    [Fact]
    public void Generate_UniqueAcrossRuns()
    {
        // Arrange / Act — 100 генераций
        var generated = Enumerable.Range(0, 100)
            .Select(_ => AppSecretGenerator.Generate())
            .ToHashSet();

        // Assert — все различны (криптостойкий источник, не константа)
        generated.Should().HaveCount(100);
    }
}
```

(FluentAssertions доступен через GlobalUsings проекта, как в соседних тестах.)

- [ ] **Step 2: Прогнать тесты — убедиться, что падают**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AppSecretGeneratorTests"`
Expected: FAIL — `AppSecretGenerator` не существует (ошибка компиляции).

- [ ] **Step 3: Реализовать генератор и модель**

`src/PgWorker.Core/Model/AppSecretGenerator.cs`:

```csharp
using System.Security.Cryptography;

namespace PgWorker.Core.Model;

/// <summary>
/// Генератор per-cluster пароля приложения (spec §4.1): криптостойкий
/// источник, 32 символа, алфавит [A-Za-z0-9] — без спецсимволов, чтобы
/// пароль был безопасен для SQL-литералов, libpq/Npgsql-строк, env и JSON
/// без экранирования.
/// </summary>
public static class AppSecretGenerator
{
    public const int Length = 32;

    private const string Alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static string Generate()
    {
        Span<char> chars = stackalloc char[Length];
        for (var i = 0; i < Length; i++)
            chars[i] = Alphabet[RandomNumberGenerator.GetInt32(Alphabet.Length)];
        return new string(chars);
    }
}
```

`src/PgWorker.Core/Model/Domain.cs` — после record `ClusterConfig` (строка 41) добавить:

```csharp
/// <summary>Per-cluster креды приложения: /clusters/&lt;C&gt;/app_user + app_password.</summary>
public sealed record AppCredentials(string User, string Password);
```

Record `ClusterSnapshot` (строки 58–60) заменить на:

```csharp
/// <summary>Полный снапшот кластера: config + шарды + все N маршрутов бакетов
/// + per-cluster app-креды (null до первого ensure — spec §4.1).</summary>
public sealed record ClusterSnapshot(ClusterConfig Config, IReadOnlyList<ShardSpec> Shards,
    IReadOnlyList<BucketRoute> Routing, AppCredentials? App = null);
```

- [ ] **Step 4: Прогнать тесты — убедиться, что проходят**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`
Expected: PASS (2 новых + все прежние — record-расширение обратно совместимо).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Core/Model/Domain.cs src/PgWorker.Core/Model/AppSecretGenerator.cs src/tests/PgWorker.UnitTests/Model/AppSecretGeneratorTests.cs
git commit -m "feat: модель AppCredentials + генератор app-пароля (32 симв [A-Za-z0-9])"
```

---

### Task 3: Парсер `app_user`/`app_password` в ClusterSnapshotParser

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (класс `ClusterAcc` строки ~26–33; switch строки ~54–126; `BuildCluster` строки ~174–183)
- Create: `src/tests/PgWorker.UnitTests/EtcdFixtures/clusters-app-secret.json`
- Test: `src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs`

**Interfaces:**
- Consumes: `AppCredentials` (Task 2).
- Produces: `ClusterSnapshotParser.ParseClusters` заполняет `ClusterSnapshot.App`: оба ключа есть → `AppCredentials`; хотя бы одного нет/пустой → `null` (толерантно, ensure допишет).

- [ ] **Step 1: Фикстура**

`src/tests/PgWorker.UnitTests/EtcdFixtures/clusters-app-secret.json` — по формату соседних фикстур (`EtcdFixtures.LoadKv` читает JSON-массив `{key, value, mod_revision}`; свериться с `clusters-full.json`):

```json
[
  { "key": "/clusters/shop/config", "value": "{\"buckets\":4,\"dbname\":\"shop\"}", "mod_revision": 10 },
  { "key": "/clusters/shop/app_user", "value": "app", "mod_revision": 11 },
  { "key": "/clusters/shop/app_password", "value": "Kj9mP2qR7sT3vW5xYz1aBc4dEf6Gh8Jk", "mod_revision": 12 },
  { "key": "/clusters/shop/shards/shard1/replicas", "value": "2", "mod_revision": 13 },
  { "key": "/clusters/shop/shards/shard1/dsn", "value": "host=n1,n2 port=5432,5433 dbname=shop user=bucket_admin password=adm", "mod_revision": 14 },
  { "key": "/clusters/shop/buckets/routing/bucket_0", "value": "shard1", "mod_revision": 15 }
]
```

- [ ] **Step 2: Падающие тесты парсера**

Добавить в `ClusterSnapshotParserTests.cs`:

```csharp
[Fact]
public void ParseClusters_AppSecretKeys_FilledIntoSnapshot()
{
    // Arrange — оба ключа app_user/app_password (spec §3.1)
    var kvs = EtcdFixtures.LoadKv("clusters-app-secret.json");

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

    // Assert
    result.IsSuccess.Should().BeTrue();
    errors.Should().BeEmpty();
    var snap = result.Value.Should().ContainSingle().Subject;
    snap.App.Should().NotBeNull();
    snap.App!.User.Should().Be("app");
    snap.App.Password.Should().Be("Kj9mP2qR7sT3vW5xYz1aBc4dEf6Gh8Jk");
    // bucket_admin-поля config не задеты (механизм сохраняется)
    snap.Config.BucketAdminUser.Should().BeNull();
    snap.Config.BucketAdminPassword.Should().BeNull();
}

[Fact]
public void ParseClusters_NoAppKeys_AppIsNull()
{
    // Arrange — кластер без app-ключей (до первого ensure)
    var kvs = EtcdFixtures.LoadKv("clusters-provisioning.json");

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert
    result.Value.Single().App.Should().BeNull();
}

[Fact]
public void ParseClusters_PartialAppKeys_AppIsNull()
{
    // Arrange — только app_user без пароля (битое состояние): толерантно null
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", "{\"buckets\":1,\"dbname\":\"shop\"}", 1),
        new("/clusters/shop/app_user", "app", 2),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

    // Assert — не ошибка парсинга: ensure допишет недостающий ключ
    errors.Should().BeEmpty();
    result.Value.Single().App.Should().BeNull();
}
```

- [ ] **Step 3: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~ClusterSnapshotParserTests"`
Expected: FAIL — `App` не заполняется (новые 3 теста красные).

- [ ] **Step 4: Реализация парсера**

`ClusterSnapshotParser.cs`:

1. В `ClusterAcc` (строки 26–33) добавить поля:

```csharp
public string? AppUser;
public string? AppPassword;
```

2. В switch (перед `default:` строки ~123) добавить cases:

```csharp
case "app_user" when segments.Length == 4:
    acc.AppUser = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
    break;

case "app_password" when segments.Length == 4:
    acc.AppPassword = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
    break;
```

3. В `BuildCluster` (строки 174–183) — constructing снапшота:

```csharp
AppCredentials? app = acc.AppUser is { Length: > 0 } u && acc.AppPassword is { Length: > 0 } p
    ? new AppCredentials(u, p)
    : null;
return new ClusterSnapshot(config, shards, routing, app);
```

- [ ] **Step 5: Прогнать весь проект**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`
Expected: PASS (новые 3 + все прежние).

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs src/tests/PgWorker.UnitTests/EtcdFixtures/clusters-app-secret.json
git commit -m "feat: парсер /clusters/<C>/{app_user,app_password} → ClusterSnapshot.App"
```

---

### Task 4: `AppSecretEnsurer` — ensure шаг (txn put-if-absent, failover по endpoints)

**Files:**
- Create: `src/PgWorker.Provisioning/Processes/AppSecretEnsurer.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AppSecretEnsurerTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway` (`GetAsync`, `TxnAsync`), `TxnCompare.NotExists`, `TxnRequest.Of`, `TxnOp.Put` (`PgWorker.Etcd.Client`); `AppCredentials`, `AppSecretGenerator` (Task 2).
- Produces: `IAppSecretEnsurer.EnsureAsync(string cluster, CancellationToken ct) → Task<Result<AppCredentials>>` (namespace `PgWorker.Provisioning.Processes`). И чтение, и txn — с failover по endpoints до первого живого (единый паттерн `ReadPortAllocAsync`; повтор txn на другом endpoint безопасен: проигрыш compare разрешается re-read).

- [ ] **Step 1: Падающие тесты ensurer**

`src/tests/PgWorker.UnitTests/Provisioning/AppSecretEnsurerTests.cs` (использует `Fakes.FakeEtcd` — честный txn):

```csharp
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-cluster app-секрета (spec §3.1/§4.1): put-if-absent, идемпотентность,
// частичные состояния, failover txn по endpoints.
public class AppSecretEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    private static AppSecretEnsurer Sut(Fakes.FakeEtcd etcd) => new(etcd, [Ep]);

    [Fact]
    public async Task Ensure_NoKeys_GeneratesBoth()
    {
        // Arrange — пустой etcd
        var etcd = new Fakes.FakeEtcd();

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — оба ключа созданы, креды возвращены
        result.IsSuccess.Should().BeTrue();
        result.Value.User.Should().Be("app");
        result.Value.Password.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    [Fact]
    public async Task Ensure_ExistingKeys_ReturnsAndDoesNotRegenerate()
    {
        // Arrange — ключи уже есть (повторный тик/re-run)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword0000000000000000000A");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — значение не перегенерировано (идемпотентность, spec §2.5)
        result.Value.Password.Should().Be("OldPassword0000000000000000000A");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be("OldPassword0000000000000000000A");
        etcd.Txns.Should().BeEmpty("существующие ключи не переписываются txn");
    }

    [Fact]
    public async Task Ensure_PartialKeys_PutsOnlyMissing()
    {
        // Arrange — только app_user (битое состояние)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — дописан только пароль; user не тронут
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    [Fact]
    public async Task Ensure_TxnFailsOnFirstEndpoint_FailoverToNext()
    {
        // Arrange — txn падает на первом endpoint (транспортный сбой), второй жив;
        // чтение (GetAsync) живо на обоих
        var etcd = new Fakes.FakeEtcd();
        var flaky = new FailFirstEndpointTxn(etcd);
        var sut = new AppSecretEnsurer(flaky, ["http://e1:2379", "http://e2:2379"]);

        // Act
        var result = await sut.EnsureAsync("shop", CancellationToken.None);

        // Assert — txn повторён на втором endpoint, ключи созданы
        // (failover-паттерн ReadAsync: ошибочный endpoint → следующий)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    // Декоратор шлюза: TxnAsync возвращает Failed на первом endpoint,
    // остальное делегирует внутреннему FakeEtcd.
    private sealed class FailFirstEndpointTxn(Fakes.FakeEtcd inner) : IEtcdGateway
    {
        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => endpoint == "http://e1:2379"
                ? Task.FromResult(Result<TxnResult>.Failed(new ApplicationException("endpoint down")))
                : inner.TxnAsync(endpoint, req, ct);

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => inner.RangeAsync(endpoint, prefix, ct);

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => inner.GetAsync(endpoint, key, ct);

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => inner.PutAsync(endpoint, key, value, lease, ct);

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => inner.LeaseGrantAsync(endpoint, ttlSec, ct);

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseRevokeAsync(endpoint, lease, ct);

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseKeepaliveAsync(endpoint, lease, ct);

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => inner.SnapshotSaveAsync(endpoint, ct);

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
            => inner.StatusAsync(endpoint, ct);

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => inner.CompactAsync(endpoint, revision, ct);

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => inner.DefragmentAsync(endpoint, ct);
    }
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AppSecretEnsurerTests"`
Expected: FAIL — тип `AppSecretEnsurer` не существует.

- [ ] **Step 3: Реализация**

`src/PgWorker.Provisioning/Processes/AppSecretEnsurer.cs`:

```csharp
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ensure per-cluster app-секрета (spec §3.1/§4.1, arch/14 §5 P1.5): чтение
/// /clusters/&lt;C&gt;/{app_user,app_password}; отсутствующие ключи генерируются
/// и кладутся ОДНОЙ txn put-if-absent (compare NotExists только на отсутствующие).
/// И чтение, и txn — с failover по endpoints до первого живого (паттерн
/// ReadPortAllocAsync); повтор txn на другом endpoint безопасен для
/// put-if-absent: проигрыш compare корректно разрешается re-read.
/// Вызывается только держателем клэйма &lt;C&gt; (инвариант мутаций /clusters/).
/// </summary>
public interface IAppSecretEnsurer
{
    Task<Result<AppCredentials>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class AppSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IAppSecretEnsurer
{
    private const string DefaultAppUser = "app";

    public async Task<Result<AppCredentials>> EnsureAsync(string cluster, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<AppCredentials>.Failed(read.Error!);

        var (user, password) = read.Value;
        if (user is { Length: > 0 } && password is { Length: > 0 })
            return Result<AppCredentials>.Success(new AppCredentials(user, password));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run — spec §2.5).
        var newUser = user ?? DefaultAppUser;
        var newPassword = password ?? AppSecretGenerator.Generate();
        var compare = new List<TxnCompare>();
        var put = new List<TxnOp>();
        if (user is null)
        {
            compare.Add(TxnCompare.NotExists(UserKey(cluster)));
            put.Add(new TxnOp.Put(UserKey(cluster), newUser, null));
        }

        if (password is null)
        {
            compare.Add(TxnCompare.NotExists(PasswordKey(cluster)));
            put.Add(new TxnOp.Put(PasswordKey(cluster), newPassword, null));
        }

        // Txn с failover по endpoints (образец ReadAsync ниже): упавший
        // endpoint → следующий; ни один не ответил — Failed(lastError).
        // Замечание: txn.IsSuccess=false — транспортный сбой вызова; проигрыш
        // compare (txn.Value.Succeeded=false) — НЕ сбой: законный исход
        // put-if-absent, обрабатывается re-read ниже.
        Result<TxnResult>? lastTxnError = null;
        var txnDone = false;
        foreach (var endpoint in endpoints)
        {
            var txn = await etcd.TxnAsync(endpoint, TxnRequest.Of(compare, put), ct);
            if (!txn.IsSuccess)
            {
                lastTxnError = txn;
                continue;
            }

            txnDone = true;
            break;
        }

        if (!txnDone)
            return Result<AppCredentials>.Failed(lastTxnError!.Error!);

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<AppCredentials>.Failed(final.Error!);

        var (finalUser, finalPassword) = final.Value;
        if (finalUser is { Length: > 0 } && finalPassword is { Length: > 0 })
            return Result<AppCredentials>.Success(new AppCredentials(finalUser, finalPassword));

        return Result<AppCredentials>.Failed(new ApplicationException(
            $"ensure app-секрета {cluster}: после txn ключи неполны " +
            $"(app_user присутствует: {finalUser is not null}, app_password присутствует: {finalPassword is not null})"));
    }

    private static string UserKey(string cluster) => $"/clusters/{cluster}/app_user";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    // Чтение обоих ключей с failover по endpoints (паттерн ReadPortAllocAsync).
    private async Task<Result<(string?, string?)>> ReadAsync(string cluster, CancellationToken ct)
    {
        Result<Kv?>? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var user = await etcd.GetAsync(endpoint, UserKey(cluster), ct);
            if (!user.IsSuccess)
            {
                lastError = user;
                continue;
            }

            var password = await etcd.GetAsync(endpoint, PasswordKey(cluster), ct);
            if (!password.IsSuccess)
            {
                lastError = password;
                continue;
            }

            return Result<(string?, string?)>.Success((
                TrimOrNull(user.Value?.Value),
                TrimOrNull(password.Value?.Value)));
        }

        return Result<(string?, string?)>.Failed(lastError!.Error!);
    }

    private static string? TrimOrNull(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
}
```

- [ ] **Step 4: Прогнать — пройти**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AppSecretEnsurerTests"`
Expected: PASS (4 теста, включая failover-кейс `Ensure_TxnFailsOnFirstEndpoint_FailoverToNext`).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/AppSecretEnsurer.cs src/tests/PgWorker.UnitTests/Provisioning/AppSecretEnsurerTests.cs
git commit -m "feat: AppSecretEnsurer — put-if-absent app_user/app_password c failover (чтение и txn) и re-read"
```

---

### Task 5: Процессы + SQL-слой: ensure P1.5, роль app из кредов, ALTER ROLE, DI

> Сигнатура `BuildRoleGuardsSql` меняется вместе с обоими вызывающими процессами и их DI — отдельные промежуточные коммиты не собирались бы, поэтому это один таск.

**Files:**
- Modify: `src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs` (`BuildRoleGuardsSql` строки 56–60; `BuildSchemasSql` строки 87–110; новый метод рядом с `PgMonitorGrant` ~строка 68)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (конструктор строки ~27–42; ensure после строк 78–84; вызов строки 132; `ProvisionShardSqlAsync` строки 361–419)
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (конструктор строки ~27–40; ensure после строк 100–104; вызов `ProvisionShardSqlAsync`; метод строки 324–368)
- Modify: `src/PgWorker.App/Program.cs` (DI: новая регистрация + аргументы в фабриках строк 92–106 и 149–161)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DatabaseProvisionerTests.cs`, `ProvisioningProcessTests.cs`, `AddShardProcessTests.cs`

**Interfaces:**
- Consumes: `IAppSecretEnsurer.EnsureAsync` (Task 4), `AppCredentials` (Task 2).
- Produces:
  - `DatabaseProvisioner.BuildRoleGuardsSql(InstallSecrets s, AppCredentials app, string? bucketAdminUser = null, string? bucketAdminPassword = null)`
  - `DatabaseProvisioner.BuildAlterAppPasswordSql(AppCredentials app) → string` — `"ALTER ROLE \"app\" PASSWORD '<пароль>';"` (одинарное экранирование — исполняется напрямую, не gexec)
  - `DatabaseProvisioner.BuildSchemasSql(string dbname, IEnumerable<int> bucketIds, string bucketAdminUser = "bucket_admin", string appUser = "app")`
  - `ProvisioningProcess`/`AddShardProcess`: новый параметр конструктора `IAppSecretEnsurer appSecret` (после `InstallSecrets secrets`, перед `EtcdEndpoints etcdEndpoints`).

Примечание: `BucketEvacuator.cs:118` вызывает `BuildSchemasSql(dbname, ids)` с дефолтами — не правится: `appUser="app"` корректен (PgWorker всегда пишет в `app_user` имя `"app"`).

- [ ] **Step 1: Падающие тесты SQL-текстостроителей**

Добавить в `DatabaseProvisionerTests.cs` (существующий тест строки 36 `BuildRoleGuardsSql(Secrets)` обновить: `BuildRoleGuardsSql(Secrets, new AppCredentials("app", "app-pw"))`, ожидания app-роли — на `app-pw`):

```csharp
[Fact]
public void BuildRoleGuardsSql_AppRole_FromAppCredentials()
{
    // Arrange — креды из etcd-ключей (после ensure)
    var app = new AppCredentials("app", "AppPw1234567890AppPw1234567890");

    // Act
    var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(Secrets, app));

    // Assert — app-роль из кредов (env AppPassword больше не источник)
    sql.Should().Contain("CREATE ROLE \"app\" LOGIN PASSWORD ''AppPw1234567890AppPw1234567890'''");
    sql.Should().Contain("bucket_admin");
    sql.Should().Contain("bucket_mover");
}

[Fact]
public void BuildAlterAppPasswordSql_EscapesAndTargetsApp()
{
    // Arrange
    var app = new AppCredentials("app", "pw'with'quotes");

    // Act
    var sql = DatabaseProvisioner.BuildAlterAppPasswordSql(app);

    // Assert — прямой литерал (одинарный Escape), идемпотентный текст
    sql.Should().Be("ALTER ROLE \"app\" PASSWORD 'pw''with''quotes';");
}

[Fact]
public void BuildSchemasSql_GrantsParameterizedAppUser()
{
    // Act — кастомное имя app-роли
    var sql = DatabaseProvisioner.BuildSchemasSql("shop", [1], "bucket_admin", "appsvc");

    // Assert — гранты параметризованы app-именем (без хардкода "app")
    sql.Should().Contain("TO \"appsvc\", \"bucket_admin\", \"bucket_mover\"");
    sql.Should().Contain("TO \"appsvc\", \"bucket_admin\"");
    sql.Should().NotContain(" \"app\",");
}
```

- [ ] **Step 2: Падающие тесты ProvisioningProcess (в т.ч. секрет не попадает в ошибки)**

В `ProvisioningProcessTests.cs`:

1. `NewRig` (строки 84–101) — конструктор процесса получает ensurer поверх того же FakeEtcd:

```csharp
var appSecret = new AppSecretEnsurer(etcd, [Ep]);
var process = new ProvisioningProcess(
    etcd, [Ep], driver, sql, Probe(patroniResponse, trace), claims, journal, Opts, Secrets,
    appSecret, EtcdEndp, snapshot: null);
```

(порядок аргументов: `..., InstallSecrets secrets, IAppSecretEnsurer appSecret, EtcdEndpoints etcdEndpoints, ...`)

2. Новый тест (механика полного прохода — по образцу `Tick_PatroniAlive_DoesEverythingToDone`; если второй шард ждёт мастера — второй тик, как в соседних тестах):

```csharp
[Fact]
public async Task Tick_CreatesAppSecretKeysAndAlignsRole()
{
    // Arrange — Patroni жив (проход до SQL-фазы)
    var rig = await NewRig(_ => Patroni("shard1a"));

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert — P1.5: оба ключа созданы и валидны (spec §7.1)
    rig.Etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
    rig.Etcd.Store["/clusters/shop/app_password"].Value.Should().MatchRegex("^[A-Za-z0-9]{32}$");
    // Роль app создаётся из кредов + выравнивается ALTER (SQL через фейк)
    var password = rig.Etcd.Store["/clusters/shop/app_password"].Value;
    rig.Sql.Executed.Should().Contain(s => s.Sql.Contains("ALTER ROLE \"app\" PASSWORD"));
    rig.Sql.Scalars.Should().Contain(s =>
        s.Sql.Contains($"CREATE ROLE \"app\" LOGIN PASSWORD ''{password}'''"));
}
```

3. Новый тест инварианта «пароль не в ошибках/журнале» (spec §4.1, критерий 10 — закрытие finding 5 ревью):

```csharp
[Fact]
public async Task Tick_SqlFailure_ErrorAndJournalHaveNoAppPassword()
{
    // Arrange — Patroni жив; SQL-исполнение падает на ALTER/схемах
    var rig = await NewRig(_ => Patroni("shard1a"));
    rig.Sql.ExecuteResult = () => Result.Failed(new ApplicationException("connection refused"));

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert — сбой не выносит app-пароль ни в текст ошибки процесса,
    // ни в last_error журнала /pgworker/work/<C> (SQL-тексты с паролем
    // в сообщения исключений не включаются — spec §4.1)
    outcome.IsSuccess.Should().BeFalse();
    var password = rig.Etcd.Store["/clusters/shop/app_password"].Value;
    outcome.Error!.ToString().Should().NotContain(password);
    var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    work.Value!.LastError.Should().NotContain(password);
}
```

(`LastError` — поле записи журнала, `JsonPropertyName("last_error")`, `WorkJournal.cs:17`.)

- [ ] **Step 3: Падающий тест AddShardProcess**

В `AddShardProcessTests.cs` — конструктор Rig'а дополнить `new AppSecretEnsurer(etcd, [Ep])` (позиция как в ProvisioningProcess); тест:

```csharp
[Fact]
public async Task Tick_AddShard_UsesClusterAppSecret_NoRegenerate()
{
    // Arrange — кластер Active с app-ключами (созданы provisioning'ом раньше)
    // + декларацию нового шарда (сид — по образцу существующих тестов AddShard)
    etcd.Seed("/clusters/shop/app_user", "app");
    etcd.Seed("/clusters/shop/app_password", "ClusterPw0000000000000000000000A");
    // ... Act: тик AddShard до SQL-фазы (как соседние тесты)

    // Assert (spec §7.3) — пароль кластера использован, НЕ перегенерирован
    rig.Sql.Scalars.Should().Contain(s =>
        s.Sql.Contains("CREATE ROLE \"app\" LOGIN PASSWORD ''ClusterPw0000000000000000000000A'''"));
    rig.Etcd.Store["/clusters/shop/app_password"].Value.Should().Be("ClusterPw0000000000000000000000A");
}
```

- [ ] **Step 4: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~DatabaseProvisionerTests|FullyQualifiedName~ProvisioningProcessTests|FullyQualifiedName~AddShardProcessTests"`
Expected: FAIL (компиляция: новые сигнатуры/конструкторы отсутствуют).

- [ ] **Step 5: Реализация DatabaseProvisioner**

1. `BuildRoleGuardsSql` (строки 56–60) заменить:

```csharp
// Guard-SELECT'ы ролей бакетного слоя (§4 доки 11): app (write-доступ
// клиентов; per-cluster креды из etcd-ключей — spec §4.1), bucket_admin
// (DSN-точка входа, per-cluster из config с env-fallback), bucket_mover
// (REPLICATION — подписки переездов P2/P3). Паттерн \gexec: скаляр
// ВОЗВРАЩАЕТ текст CREATE ROLE, если её нет.
public static IReadOnlyList<string> BuildRoleGuardsSql(InstallSecrets s,
    AppCredentials app, string? bucketAdminUser = null, string? bucketAdminPassword = null)
    => [Role(app.User, app.Password),
        Role(bucketAdminUser ?? "bucket_admin", bucketAdminPassword ?? s.BucketAdminPassword),
        Role("bucket_mover", s.MoverPassword, replication: true)];
```

2. Добавить метод (рядом с `PgMonitorGrant`):

```csharp
// Идемпотентное выравнивание пароля app-роли значению etcd-ключа (spec §4.1):
// исполняется напрямую (ExecuteAsync, не gexec) — одинарное экранирование.
// Гарантирует «роль ↔ ключ» на любом шарде, включая кластеры, созданные
// до появления app-секрета (миграция) и rebuild нод.
public static string BuildAlterAppPasswordSql(AppCredentials app)
{
    ValidateIdentifier(app.User);
    return $"ALTER ROLE \"{app.User}\" PASSWORD '{Escape(app.Password)}';";
}
```

3. `BuildSchemasSql` (строки 87–110): сигнатура `+ string appUser = "app"`; `ValidateIdentifier(appUser);` и заменить гранты:

```csharp
sb.AppendLine($"GRANT USAGE ON SCHEMA bucket_{id} TO \"{appUser}\", \"{bucketAdminUser}\", \"bucket_mover\";");
sb.AppendLine($"GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_{id} TO \"{appUser}\", \"{bucketAdminUser}\";");
sb.AppendLine($"GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_{id} TO \"{appUser}\", \"{bucketAdminUser}\";");
```

- [ ] **Step 6: Реализация ProvisioningProcess**

1. Primary-конструктор: после `InstallSecrets secrets,` добавить `IAppSecretEnsurer appSecret,`.
2. После блока `clusterSecrets` (строки 78–84) вставить P1.5:

```csharp
// P1.5 (spec §3.3): ensure per-cluster app-секрета — до любых контейнеров/ролей:
// приложение получает креды в etcd раньше, чем поднимутся ноды.
var appCreds = await appSecret.EnsureAsync(cluster, ct);
if (!appCreds.IsSuccess)
    return await FailAsync(cluster, appCreds.Error!, "ensure-app-secret", ct);
```

3. Вызов `ProvisionShardSqlAsync(snap, shard, topology, master, ct)` (строка 132) → `ProvisionShardSqlAsync(snap, shard, topology, master, appCreds.Value, ct)`.
4. `ProvisionShardSqlAsync` (строки 361–419): сигнатура `+ AppCredentials app`; guard-цикл (377):

```csharp
foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(secrets, app, bucketAdminUser, bucketAdminPassword))
```

после цикла `BuildRoleExecSql` (строки 391–396) добавить:

```csharp
// Выравнивание app-роли паролю из etcd-ключа (идемпотентно; spec §4.1):
// кластеры, созданные до app-секрета, и rebuild нод получают актуальный пароль.
var alterApp = await db.ExecuteAsync(dbDsn, DatabaseProvisioner.BuildAlterAppPasswordSql(app), ct);
if (!alterApp.IsSuccess)
    return alterApp;
```

`BuildSchemasSql` (строка 404): `DatabaseProvisioner.BuildSchemasSql(dbname, bucketIds, bucketAdminUser, app.User)`.
DSN (строка 414) и bucket_admin-пути — БЕЗ ИЗМЕНЕНИЙ.

- [ ] **Step 7: Реализация AddShardProcess (зеркально)**

1. Конструктор: `+ IAppSecretEnsurer appSecret` (после `secrets`).
2. После `clusterSecrets` (строки ~100–104):

```csharp
// Ensure app-секрета кластера (spec §3.3, образец P1.5): у живого кластера
// ключи уже есть — читаем; отсутствующие (кластер до app-секрета) — создаём.
var appCreds = await appSecret.EnsureAsync(cluster, ct);
if (!appCreds.IsSuccess)
    return await FailAsync(cluster, appCreds.Error!, "ensure-app-secret", ct);
```

3. Вызов `ProvisionShardSqlAsync` — передать `appCreds.Value`.
4. `ProvisionShardSqlAsync` (строки 324–368): `+ AppCredentials app`; `BuildRoleGuardsSql(secrets, app, bucketAdminUser, bucketAdminPassword)`; после `BuildRoleExecSql`-цикла — `ALTER ROLE` (тот же код, что в ProvisioningProcess Step 6.4). DSN (364) — БЕЗ ИЗМЕНЕНИЙ.

- [ ] **Step 8: DI в Program.cs**

После регистрации `ShardEndpoints` (строки ~132–135) добавить:

```csharp
// Ensure per-cluster app-секрета (spec §4.1): чтение/txn put-if-absent
// /clusters/<C>/{app_user,app_password} — общий для Provisioning/AddShard.
builder.Services.AddSingleton<IAppSecretEnsurer>(sp => new AppSecretEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
```

В фабриках `ProvisioningProcess` (строки 97–106) и `AddShardProcess` (152–160) добавить аргумент `sp.GetRequiredService<IAppSecretEnsurer>(),` после `sp.GetRequiredService<InstallSecrets>(),`.

- [ ] **Step 9: Прогнать всё**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj && dotnet build src/PgWorker.slnx`
Expected: PASS (новые тесты, включая `Tick_SqlFailure_ErrorAndJournalHaveNoAppPassword`, + все прежние; решение собирается без ворнингов).

- [ ] **Step 10: Commit**

```bash
git add src/PgWorker.Provisioning src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests
git commit -m "feat: P1.5 ensure app-секрета в Provisioning/AddShard, роль app из etcd-кредов + ALTER ROLE"
```

---

### Task 6: `InstallSecrets` без `AppPassword` — env-секрет `PGW_APP_ROLE_PASSWORD` уходит + миграция e2e-потребителей секрета

**Files:**
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (record строки 15–17; `SpiloEnvBuilder` строки 52–59)
- Modify: `src/PgWorker.App/Program.cs` (`SecretsFromEnv` строки 228–241)
- Modify: `deploy/docker-compose.yml` (строка 25)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (хелпер чтения секрета; удаление константы `AppPassword` строки 22 и env-строки 137)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eMoveScenarios.cs` (строка 409 — `AppDsn`)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs` (строка 417 — inline DSN)
- Test (каскад удалений третьего аргумента — полный список ниже в Step 3.5):
  - `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs` (конструкция строки 21–22; ожидание строки 70)
  - `src/tests/PgWorker.UnitTests/Moves/ShardEndpointsTests.cs` (строки 44, 58, 73, 93, 106, 120, 132)
  - `src/tests/PgWorker.UnitTests/Moves/FakesMove.cs` (строка 73)
  - `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (строка 139)
  - `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (строка 19)
  - `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs` (строка 19)
  - `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` (строка 20)
  - `src/tests/PgWorker.UnitTests/Provisioning/BucketEvacuatorTests.cs` (строка 22)
  - `src/tests/PgWorker.UnitTests/Provisioning/DatabaseProvisionerTests.cs` (строка 11, многострочная конструкция)
  - `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs` (строка 31)

**Interfaces:**
- Consumes: Task 5 (процессы больше не читают `secrets.AppPassword`).
- Produces:
  - `InstallSecrets(string SuPassword, string StandbyPassword, string BucketAdminPassword, string MoverPassword, string BucketAdminUser = "bucket_admin")`
  - `E2eFixture.GetAppPasswordAsync(string cluster, CancellationToken ct = default) → Task<string>` — чтение `/clusters/<C>/app_password` через `Gateway` фикстуры (e2e становится потребителем секрета тем же путём, что и приложение — spec §4.3).

- [ ] **Step 1: Тест «app-пароль не течёт в env контейнера» (упадёт до правки)**

В `NodeConfigBuildersTests.cs`:

```csharp
[Fact]
public void SpiloEnv_NoAppPasswordLeak()
{
    // Arrange
    var topology = new ShardTopology("shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress>
        {
            ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
        });
    var secrets = new InstallSecrets("su", "sb", "adm", "mov");

    // Act
    var env = SpiloEnvBuilder.Build(topology, new EtcdEndpoints(["http://etcd:2379"]), secrets);

    // Assert — app-пароль в env контейнера не попадает (spec §2.4, критерий 6);
    // bucket_admin-механизм env не тронут
    env.Keys.Should().NotContain("PGW_APP_PASSWORD");
    env.Keys.Should().Contain("PGW_BUCKET_ADMIN_PASSWORD");
    env.Keys.Should().Contain("PGW_BUCKET_ADMIN_USER");
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~NodeConfigBuildersTests"`
Expected: FAIL — `InstallSecrets` ещё 5-аргументный / `PGW_APP_PASSWORD` ещё пишется.

- [ ] **Step 3: Реализация (record, env, compose, каскад тестов)**

1. `NodeConfigBuilders.cs`, record (строки 15–17):

```csharp
public sealed record InstallSecrets(string SuPassword, string StandbyPassword,
    string BucketAdminPassword, string MoverPassword,
    string BucketAdminUser = "bucket_admin");
```

2. `SpiloEnvBuilder.Build` (строки 52–59): удалить строку `["PGW_APP_PASSWORD"] = secrets.AppPassword,`; комментарий блока заменить на: «Пароли ролей бакетного слоя (создаёт DatabaseProvisioner; здесь — доступность внутри контейнера для админ-скриптов). App-пароль в env НЕ прокидывается — per-cluster в etcd (spec §4.1). bucket_admin — per-cluster credentials из config (переопределены в ProvisioningProcess).»
3. `Program.cs` `SecretsFromEnv` (строки 235–240): убрать аргумент `Required("PGW_APP_ROLE_PASSWORD"),`.
4. `deploy/docker-compose.yml`: удалить строку 25 `PGW_APP_ROLE_PASSWORD: ${PGW_APP_ROLE_PASSWORD:?задайте секрет}`.
5. **Каскад тестов — ⚠️ править НЕ «по ошибкам компиляции» (их не будет!), а по списку.** После смены сигнатуры `(Su, Standby, AppPassword, BucketAdmin, Mover, BucketAdminUser="bucket_admin")` → `(Su, Standby, BucketAdmin, Mover, BucketAdminUser="bucket_admin")` существующие 5-позиционные вызовы `new("su", "sb", "app-pw", "adm-pw", "mov-pw")` ПРОДОЛЖАТ компилироваться: пятый аргумент тихо ляжет в `BucketAdminUser`, а семантика сдвинется на позицию (`BucketAdminPassword="app-pw"`, `MoverPassword="adm-pw"`, `BucketAdminUser="mov-pw"`) — компилятор это не поймает, поломка вскроется только красными тестами. Рецепт: в каждой конструкции **удалить третий позиционный аргумент (AppPassword)**. Полный список (проверен grep'ом `new InstallSecrets(` + target-typed `InstallSecrets … = new(`):

| Файл | Строка | Конструкция |
|---|---|---|
| `src/tests/PgWorker.UnitTests/Moves/ShardEndpointsTests.cs` | 44, 58, 73, 93, 106, 120, 132 | `new InstallSecrets("su", "sb", "app", "adm", "moverpw")` и вариации → убрать 3-й аргумент |
| `src/tests/PgWorker.UnitTests/Moves/FakesMove.cs` | 73 | `Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw")` |
| `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` | 139 | `Secrets = new("su", "sb", "app", "ba", "mv")` |
| `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` | 19 | `Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw")` |
| `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs` | 19 | то же |
| `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` | 20 | то же |
| `src/tests/PgWorker.UnitTests/Provisioning/BucketEvacuatorTests.cs` | 22 | то же |
| `src/tests/PgWorker.UnitTests/Provisioning/DatabaseProvisionerTests.cs` | 11–12 | многострочная: убрать `"app-pw",` (3-я строка аргументов) |
| `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs` | 21–22 | многострочная: убрать `"app-secret",` |
| `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs` | 31 | `Secrets = new("su", "sb", "app", "ba", "mv")` |

   Плюс в `NodeConfigBuildersTests.cs:70` — правка ожидания: массив
   `env.Values.Should().Contain(new[] { "su-secret", "standby-secret", "app-secret", "admin-secret", "mover-secret" })`
   сократить до `new[] { "su-secret", "standby-secret", "admin-secret", "mover-secret" }` (значение app-секрета в env больше не пишется).

   Примечание: `Fakes.cs:211` и `StubScaleDriver.cs:26` — параметры методов с типом `InstallSecrets` (не конструкции), правка не нужна: тип остаётся.

- [ ] **Step 4: Миграция e2e-потребителей `E2eFixture.AppPassword` (finding 2 ревью итерации 1)**

После удаления константы `E2eFixture.AppPassword` (строка 22) и env-строки (строка 137) сценарии `E2eMoveScenarios.cs:409` и `E2eScaleScenarios.cs:417` (`Username=app;Password={E2eFixture.AppPassword}`) перестают компилироваться — пароль им теперь известен только из etcd. Миграция:

1. Новый хелпер в `E2eFixture.cs` (рядом со `StartHostAsync`; использует `Gateway` и `EtcdEndpoint` фикстуры — `Gateway.GetAsync(endpoint, key, ct) → Result<Kv?>`):

```csharp
/// <summary>
/// Пароль app-роли кластера из etcd (spec §3.1): e2e-сценарии читают секрет
/// тем же путём, что и приложение — /clusters/&lt;C&gt;/app_password.
/// </summary>
public async Task<string> GetAppPasswordAsync(string cluster, CancellationToken ct = default)
{
    var result = await Gateway.GetAsync(EtcdEndpoint, $"/clusters/{cluster}/app_password", ct);
    result.IsSuccess.Should().BeTrue("app-секрет обязан появиться после provisioning");
    return result.Value!.Value;
}
```

2. `E2eMoveScenarios.cs:408–409` — статический билдер сделать асинхронным (класс сценария имеет поле `fixture`; вызвать `await AppDsnAsync(port, ct)` по местам использования):

```csharp
private async Task<string> AppDsnAsync(int port, CancellationToken ct)
    => $"Host=localhost;Port={port};Database={Cluster};Username=app;" +
       $"Password={await fixture.GetAppPasswordAsync(Cluster, ct)};SSL Mode=Require;Trust Server Certificate=true";
```

3. `E2eScaleScenarios.cs:417` — inline-строку заменить на чтение через хелпер:

```csharp
$"Host=localhost;Port={master.Port};Database={cluster};Username=app;" +
$"Password={await fixture.GetAppPasswordAsync(cluster, ct)};Timeout=10;SSL Mode=Require;Trust Server Certificate=true"
```

(окружающий метод уже async — контекст по месту; имена полей `fixture`/`Cluster` сверить с классом сценария.)

- [ ] **Step 5: Прогнать всё + контроль каскада**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`
Expected: сборка (включая IntegrationTests) и unit-тесты зелёные.

Контроль, что сдвинутых 5-позиционных конструкций не осталось (finding 2 итерации 2 — старые app-значения не должны встречаться нигде):

```bash
grep -rnE "\"(app-pw|app-secret|app)\", ?\"(adm-pw|admin-secret|adm|ba)\"" src/tests --include="*.cs"
grep -rn "E2eFixture.AppPassword" src/tests
```
Expected: оба grep — 0 вхождений (первый ловит пары «app-значение, следующее за ним» в конструкциях; если остались — каскад Step 3.5 не завершён).

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Core/Templates/NodeConfigBuilders.cs src/PgWorker.App/Program.cs deploy/docker-compose.yml src/tests
git commit -m "feat: убрать PGW_APP_ROLE_PASSWORD — app-пароль только per-cluster в etcd; e2e читает секрет из etcd"
```

---

### Task 7: AdminPanel — expected-skip ключей `app_user`/`app_password`

**Files:**
- Modify: `src/AdminPanel.Etcd/Parsing/ClustersParser.cs` (switch строки 54–126)
- Test: `src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`

**Interfaces:**
- Consumes: `Kv` из `AdminPanel.Etcd.Client` (как в соседних тестах).
- Produces: `app_user`/`app_password` не увеличивают `UnknownKeyCount`, значение не попадает в модель. DsnParser/ShardInfo.Password/SqlProbe — без изменений.

- [ ] **Step 1: Падающий тест**

Добавить в `ClustersParserTests.cs`:

```csharp
[Fact]
public void Parse_AppSecretKeys_SkippedWithoutUnknown()
{
    // Arrange — app-ключи в префиксе /clusters/ (spec §3.4: панель их не читает)
    var kvs = new List<Kv>
    {
        new("/clusters/demo/config", "{\"buckets\":1,\"dbname\":\"demo\"}", 1),
        new("/clusters/demo/app_user", "app", 2),
        new("/clusters/demo/app_password", "Kj9mP2qR7sT3vW5xYz1aBc4dEf6Gh8Jk", 3),
    };

    // Act
    var result = ClustersParser.Parse(kvs);

    // Assert — expected-skip: не unknown, не в модели
    result.UnknownKeyCount.Should().Be(0);
    result.Errors.Should().BeEmpty();
    result.Clusters.Should().ContainSingle();
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~ClustersParserTests.Parse_AppSecretKeys"`
Expected: FAIL — `UnknownKeyCount` == 2 (ключи считаются unknown).

- [ ] **Step 3: Реализация**

В `ClustersParser.Parse`, switch (перед `default:`) добавить:

```csharp
// Креды приложения (генерирует PgWorker, spec §3.4): expected-skip — панель
// их не читает и не отображает; значение не попадает в модель/UI/API.
case "app_user" or "app_password" when segments.Length == 4:
    break;
```

- [ ] **Step 4: Прогнать всё (панель)**

Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/AdminPanel.Etcd/Parsing/ClustersParser.cs src/tests/AdminPanel.UnitTests/ClustersParserTests.cs
git commit -m "feat(adminpanel): expected-skip ключей app_user/app_password (не читаются панелью)"
```

---

### Task 8: E2e — app-секрет в сценарии provisioning

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eAppSecretScenarios.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` — только если после Task 6 остались использования `AppPassword` (константа и env-строка удалены в Task 6 Step 3/4)

**Interfaces:**
- Consumes: `E2eFixture` (`StartHostAsync`, `Gateway`, `WaitForAsync`, новый `GetAppPasswordAsync`), паттерн подключения по DSN из `E2eScenarios.cs:176–205` (regex `host=`/`port=`, пофрагментный Npgsql-проб), сид config из `E2eScenarios.cs:80`.
- Produces: e2e-доказательство критериев 1–3 spec §7.

**Отклонение от канона деплоя (фиксируется явно, finding 1 ревью итерации 1):** e2e-стенд работает без doorman — `E2eFixture.cs:153` задаёт `PgWorker__Docker__EnableDoorman=false`, и ВСЕ существующие сценарии (включая bucket_admin-пробы `E2eScenarios.cs:195–205`) подключаются к прямым pg-портам нод, извлечённым из dsn-ключа. Канон потребления для приложения — doorman `:6432` (spec §3.1) — в e2e отдельно не проверяется (spec §7.2, ревизия 3): проверяется сам факт «креды `user=app` + пароль из etcd работают против БД шарда». Это то же отклонение, что и у существующих сценариев, новой степени свободы оно не добавляет.

- [ ] **Step 1: Сценарий**

`src/tests/PgWorker.IntegrationTests/E2e/E2eAppSecretScenarios.cs` — класс `E2eAppSecretScenarios : IClassFixture<E2eFixture>`; структура (Arrange/Act/Assert; полный код исполнитель пишет по образцу `E2eScenarios.AssertProvisioningResultAsync`, строки 157–205):

```csharp
// E2e per-cluster app-секрета (spec §7.1–7.3).
// Arrange: сид кластера shop (образец E2eScenarios: config NOT_INITIALIZED
//          с bucket_admin-кредами фикстуры) + StartHostAsync("appsecret").
// Act:     WaitForAsync до появления /clusters/shop/shards/shard1/dsn.
// Assert 1 (критерий 1): /clusters/shop/app_user == "app";
//          /clusters/shop/app_password ~ ^[A-Za-z0-9]{32}$;
//          dsn-ключ содержит "user=bucket_admin" (как было), но НЕ содержит
//          "user=app" и НЕ содержит значение app_password (app-пароль в DSN
//          не светится — spec §2.4).
// Assert 2 (критерий 2): Npgsql к host:port из dsn-ключа (пофрагментно,
//          образец E2eScenarios:195–205; ПРЯМОЙ pg-порт ноды — стенд без
//          doorman, см. отклонение выше):
//          Host=<host>;Port=<port>;Database=shop;Username=<app_user из etcd>;
//          Password=<app_password из etcd>;SSL Mode=Require;
//          Trust Server Certificate=true;Timeout=5 → SELECT 1 == 1.
// Assert 3 (критерий 3): пауза 5 с (≥2 тика scan 1 c), перечитать
//          app_password — значение не изменилось (идемпотентность тиков).
```

- [ ] **Step 2: Прогон (требует docker; при недоступности — сборка + отложить на Task 9)**

Run: `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj --filter "FullyQualifiedName~E2eAppSecretScenarios"`
Expected: PASS. Минимум при недоступном docker: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` — без ошибок.

- [ ] **Step 3: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e
git commit -m "test(e2e): app-секрет — ключи etcd, подключение user=app (прямой порт стенда), стабильность пароля"
```

---

### Task 9: Deprovision-очистка app-ключей (unit) + финальная верификация

**Files:**
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`

**Interfaces:**
- Consumes: `Fakes.FakeEtcd`; изменений кода не требуется — D2 удаляет `del --prefix /clusters/<C>/`, app-ключи уходят с префиксом (тест фиксирует контракт spec §3.1/§7.4).

- [ ] **Step 1: Тест deprovision (по образцу `Tick_FullRemoval_RemovesNodesKeysAndScope`, строки 62–83)**

```csharp
[Fact]
public async Task Tick_FullRemoval_DeletesAppSecretKeys()
{
    // Arrange — сид кластера из соседнего теста + app-ключи
    // ... тот же сид, что в Tick_FullRemoval_RemovesNodesKeysAndScope ...
    etcd.Seed("/clusters/shop/app_user", "app");
    etcd.Seed("/clusters/shop/app_password", "Pw0000000000000000000000000000A");

    // Act — полный проход deprovision (как соседний тест)

    // Assert (spec §7.4): ключи удалены вместе с префиксом кластера
    etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/clusters/shop/", StringComparison.Ordinal));
}
```

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~DeprovisioningProcessTests"`
Expected: PASS.

- [ ] **Step 2: Полная верификация тестами (unit + integration, включая все e2e — finding 3 ревью итерации 1)**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj
dotnet build src/PgWorker.slnx
dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj
```
Expected: всё зелёное, 0 ворнингов (TreatWarningsAsErrors). Integration-прогон включает ВСЕ e2e-сценарии — в том числе существующие `E2eScenarios` (bucket_admin-механизм не сломан — критерий 5), мигрированные `E2eMoveScenarios`/`E2eScaleScenarios` (app-пароль из etcd — Task 6) и новый `E2eAppSecretScenarios` (Task 8). Требует docker; при недоступности docker в окружении исполнения — зафиксировать отклонение с обоснованием в финальном отчёте execute (какие классы не прогнаны) и прогнать хотя бы `dotnet build` IntegrationTests.

- [ ] **Step 3: E2e на dev-станде (фаза 5 spec; с оговоркой — finding 3 ревью итерации 1)**

Smoke по AGENTS.md-флоу стенда (docker доступен):

```bash
# 1. etcd стенда
docker compose -f dev-stand/compose.yaml up -d
# 2. сборка образа PgWorker + подъём (env-секреты; PGW_APP_ROLE_PASSWORD больше НЕ нужен)
docker compose -f deploy/docker-compose.yml up -d --build
# 3. сид кластера (app-ключи НЕ сеём — генерирует PgWorker)
dev-stand/seed.sh http://localhost:2379 shop
# 4. ждать provisioning (dsn-ключи) и проверить app-секрет одним range.
#    Диапазон — ПОЛУинтервал [key, range_end) в байтовом порядке; поскольку
#    "app_password" < "app_user" ('p' < 'u'), начало — app_password, конец —
#    app_user0 (охватывает ОБА ключа: "app_password" ≤ k < "app_user0").
curl -s http://localhost:2379/v3/kv/range -X POST -H 'Content-Type: application/json' \
  -d '{"key":"L2NsdXN0ZXJzL3Nob3AvYXBwX3Bhc3N3b3Jk","range_end":"L2NsdXN0ZXJzL3Nob3AvYXBwX3VzZXIw"}'
```
(base64: key=`/clusters/shop/app_password`, range_end=`/clusters/shop/app_user0`; ожидаем ровно 2 kv — `app_user="app"` и `app_password` из 32 симв `[A-Za-z0-9]`. Альтернатива — два точечных range по каждому ключу.)
Expected: ключи `app_user`/`app_password` появились после provisioning; в env контейнеров нод нет `PGW_APP_PASSWORD` (`docker exec <node> env | grep PGW_APP` — пусто).
Если dev-стенд недоступен в окружении исполнения (нет docker/образа): НЕ блокировать выполнение — зафиксировать отклонение с обоснованием в финальном отчёте execute; интеграционные e2e (Step 2) уже покрывают критерии 1–3, dev-stand smoke остаётся ручным шагом ревью/мержа.

- [ ] **Step 4: Grep-критерии «пароль не светится» (spec §7.1/7.6/7.7)**

```bash
grep -rn "PGW_APP_ROLE_PASSWORD" src deploy --include="*.cs" --include="*.yml"
grep -n "PGW_APP_PASSWORD" src/PgWorker.Core/Templates/NodeConfigBuilders.cs
grep -rn "app_password" src/AdminPanel.Core src/AdminPanel.Api
grep -rn "E2eFixture.AppPassword" src/tests
git diff main -- src/AdminPanel.Etcd/Parsing/DsnParser.cs src/AdminPanel.Core/ClusterInfo.cs src/AdminPanel.Probes/SqlProbe.cs
```
Expected: 0 вхождений `PGW_APP_ROLE_PASSWORD`; 0 вхождений `PGW_APP_PASSWORD` в SpiloEnvBuilder; 0 упоминаний `app_password` в Core/Api панели; 0 вхождений `E2eFixture.AppPassword` (миграция Task 6 завершена); diff по DsnParser/ShardInfo/SqlProbe — пуст (bucket_admin-механизм не тронут, spec-критерий 5).

- [ ] **Step 5: Сверка критериев spec §7 (чек-лист исполнителя)**

| Критерий spec §7 | Закрыто таском |
|---|---|
| 1. provisioning создаёт app_user/app_password; app-пароля нет в DSN/env/config | Task 5 (unit) + Task 8 (e2e) |
| 2. e2e: подключение `user=app` паролем из etcd (прямой pg-порт стенда; doorman `:6432` — канон деплоя, spec §7.2) | Task 8 |
| 3. идемпотентность (re-run/add-shard не меняют пароль) | Task 4 (unit) + Task 5 (`Tick_AddShard_UsesClusterAppSecret_NoRegenerate`) + Task 8 (e2e-пауза) |
| 4. deprovision удаляет ключи | Task 9 |
| 5. bucket_admin-поведение не изменилось | Task 9 Step 2 (integration-прогон всех существующих e2e) + пустой diff Step 4 |
| 6. `PGW_APP_ROLE_PASSWORD` отсутствует везде | Task 6 + grep Step 4 |
| 7. панель не отдаёт app-креды, unknownKeys не растёт | Task 7 + grep Step 4 |
| 8. канон соответствует коду | Task 1 |
| 9. roadmap t02 переформулирован | Task 1 |
| 10. тесты зелёные (unit+integration), пароль не в логах/журнале | Task 9 Steps 2–3 + Task 5 (`Tick_SqlFailure_ErrorAndJournalHaveNoAppPassword`) |

- [ ] **Step 6: Commit**

```bash
git add src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs
git commit -m "test: deprovision удаляет app_user/app_password с префиксом кластера"
```

---

## Порядок исполнения и зависимости

```
Task 1 (arch) → Task 2 (модель/генератор) → Task 3 (парсер) → Task 4 (ensurer, failover чтение+txn)
  → Task 5 (DatabaseProvisioner + Provisioning/AddShard + DI + тест «пароль не в ошибках»)
  → Task 6 (InstallSecrets без AppPassword + каскад по списку + миграция e2e-потребителей секрета)
  → Task 7 (панель expected-skip) → Task 8 (e2e, отклонение doorman задокументировано)
  → Task 9 (deprovision + полная верификация unit/integration + dev-stand smoke)
```

Таски строго последовательны (Task 5 объединяет SQL-слой и процессы из-за компиляционной целостности сигнатуры `BuildRoleGuardsSql`; Task 6 меняет record `InstallSecrets` каскадом по явному списку конструкций — «по ошибкам компиляции» править НЕЛЬЗЯ, см. предупреждение в Task 6 Step 3.5 — и мигрирует e2e-сценарии на чтение секрета из etcd). Каждый таск завершается зелёной сборкой/тестами и отдельным коммитом. Отклонения окружения (docker/dev-стенд недоступны) не блокируют выполнение — фиксируются с обоснованием в финальном отчёте execute.
