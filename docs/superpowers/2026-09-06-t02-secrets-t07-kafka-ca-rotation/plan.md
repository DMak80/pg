# t02-per-cluster-secrets + t07-kafka-ca-rotation — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** per-cluster секреты PgWorker (app, bucket_admin, bucket_mover) генерируются в etcd и ротируются одной заявкой без остановки записи; per-cluster CA/серты Kafka-кластера ротируются через окно двойного доверия и rolling-пересоздание брокеров.

**Architecture:** образцы уже в коде: `AppSecretEnsurer`/`AppPasswordRotator` (PgWorker, txn put-if-absent + заявка `/pgworker/rotations/<C>`) и `PasswordRotator` (KafkaWorker, фазы A/B/C rolling). T02 расширяет ensure до тройки кредов и ротатор до трёх ролей + перезапись dsn; T07 — новый `CaRotator` по тому же протоколу (staging `ca_next_*` → bundle `ca_pem` → rolling с NEW-подписью → атомарный коммит).

**Tech Stack:** .NET 10 / C# (Nullable, TreatWarningsAsErrors), etcd txn, Docker/Testcontainers, React+Mantine (панель).

**Spec:** [spec.md](spec.md) — план аргументируется от спеки; исполнитель читает оба.

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; версии пакетов — только через `Directory.Packages.props`.
- Язык: доки/комментарии — русский, идентификаторы — английский.
- Тесты: порты docker — динамические (`assignRandomHostPort`/`GetMappedPublicPort`), никаких литералов `:16000`; `BrokerBootSec` ≤ 100 с; после КАЖДОЙ серии — `docker rm -f $(docker ps -aq)` (кроме `as-*`/`adminpanel` стенда) + `docker network prune -f`.
- E2E на свежем Release обязателен (трогаются `PgWorker.App`/`Provisioning`): полный `E2eFixture` (8/8), `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test … -c Release`.
- etcd-кластер один (`as-etcd` стенда); панель и воркеры — только в docker.
- env-секреты `PGW_*` не удаляются (fallback/bootstrap); `PGW_APP_ROLE_PASSWORD`-подобных новых env не вводить.
- Панель секреты (bucket_admin/mover) не читает и не пишет — только прокси в API воркеров.

---

### Task 1: Канон arch/ — фиксация контракта (arch-first)

**Files:**
- Modify: `arch/14-pgworker.md`
- Modify: `arch/15-kafka-clusters.md`
- Modify: `arch/16-kafkaworker.md`
- Modify: `arch/adminpanel/02-etcd-contract.md`

**Interfaces:**
- Produces: канон ключей `/clusters/<C>/{mover_password,bucket_admin_user,bucket_admin_password}`, семантика заявки `/pgworker/rotations/<C>` (все per-cluster креды), эндпоинты `POST /api/clusters/{c}/secrets/rotate` и `POST /api/kafka/clusters/{c}/ca/rotate`, ключи `/kafka/clusters/<C>/{ca_next_key,ca_next_pem}`, заявка `/kafkaworker/ca_rotations/<C>`, bundle-семантика `ca_pem`. Все последующие задачи ссылаются на эти имена.

- [ ] **Step 1: arch/14 — ключи, ротатор, эндпоинт**

В §3.3 (таблица ключей) добавить строки: `/clusters/<C>/mover_password` (per-cluster пароль роли `bucket_mover`, строка 32 симв `[A-Za-z0-9]`, ensure + ротация R3), `/clusters/<C>/bucket_admin_user` и `/clusters/<C>/bucket_admin_password` (per-cluster креды DSN-точки входа; канон после этой задачи, config/env — fallback). В §4 «Секреты» группы 2–3 переписать: mover/bucket_admin — per-cluster, env `PGW_BUCKET_ADMIN_*`/`PGW_BUCKET_MOVER_PASSWORD` — переходный fallback до ensure. В §5 I переименовать процесс `AppPasswordRotator` → `ClusterSecretRotator`: R1 — ensure тройки (app, mover, bucket_admin), R2 — `ALTER ROLE` трёх ролей на мастере каждого шарда с dsn, R3 — одна txn: put `app_password`+`mover_password`+`bucket_admin_password`+перезапись dsn-ключей всех шардов (новый bucket_admin-пароль)+del заявки, compare `value==OLD` по всем четырём типам ключей. В §3.3 таблицу HTTP API и §8 (панельные команды) обновить: `POST /api/clusters/{c}/app-password/rotate` → `POST /api/clusters/{c}/secrets/rotate` (семантика — «ротация per-cluster секретов»).

- [ ] **Step 2: arch/15 — ключи CA-ротации и bundle-дискавери**

В §4 добавить: `/kafka/clusters/<C>/{ca_next_key,ca_next_pem}` (staging новой CA, живут только в окне ротации, ensure их не создаёт), `/kafkaworker/ca_rotations/<C>` (заявка §9.8, исполняет CaRotator). В §5 (дискавери) зафиксировать: `ca_pem` в окне ротации — bundle (конкатенация PEM OLD+NEW); приложения и панель строят truststore из `ca_pem` как есть — PEM-конкатенация стандартна для truststore (librdkafka `ssl.ca.location`, Java/.NET).

- [ ] **Step 3: arch/16 — канон ротации CA**

В §2.3 дописать канон ротации: порядок фаз P→D→R→C (staging put-if-absent → bundle `ca_pem` ДО замены сертов → rolling-пересоздание брокеров с сертами от NEW CA и truststore=bundle → атомарный коммит `ca_pem`/`ca_key` ← NEW + del staging + del заявки); OLD `ca_key` уничтожается перезаписью; truststore брокеров остаётся bundle до следующего пересоздания (вреда нет, env выравнивается надзором). R8/R10 — добавить ссылку «закрывается ротацией CaRotator (t07)». В §5 добавить процесс `CaRotator` (фазы, journal `op=rotate-ca`), в §1.1 — эндпоинт `POST /api/kafka/clusters/{c}/ca/rotate`.

- [ ] **Step 4: arch/adminpanel/02 — панельный контракт**

§9.8: семантика pg-заявки — «ротация per-cluster секретов (app + bucket_admin + mover)», команда-прокси `RotateClusterSecretsCommand` → `/api/clusters/{c}/secrets/rotate`. Таблица kafka-команд: новая мутация №17 `POST /api/kafka/clusters/{c}/ca/rotate` (клэйм-txn `/kafkaworker/ca_rotations/<C>` `version==0` + put `{"requested_unix","requested_by"}`; UI-модалка предупреждает о rolling-пересоздании брокеров и окне двойного доверия). Отметить: пробы панели читают `ca_pem`-bundle как строку без правок.

- [ ] **Step 5: Проверка канона**

Run: `grep -n "secrets/rotate\|ca_rotations\|ca_next_\|mover_password\|bucket_admin_password" arch/14-pgworker.md arch/15-kafka-clusters.md arch/16-kafkaworker.md arch/adminpanel/02-etcd-contract.md | wc -l`
Expected: ≥ 12 (все имена присутствуют в соответствующих файлах).

- [ ] **Step 6: Commit**

```bash
git add arch/14-pgworker.md arch/15-kafka-clusters.md arch/16-kafkaworker.md arch/adminpanel/02-etcd-contract.md
git commit -m "docs(arch): контракт t02 (per-cluster mover/bucket_admin + ClusterSecretRotator) и t07 (CaRotator: ca_next_*/bundle/окно двойного доверия)"
```

---

### Task 2: ClusterSnapshot — per-cluster креды в модели и парсере

**Files:**
- Modify: `src/PgWorker.Core/Model/Domain.cs:67-68` (record `ClusterSnapshot`)
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs` (или файл, где парсер тестируется — найти по `ParseClusters`)

**Interfaces:**
- Produces: `ClusterSnapshot(Config, Shards, Routing, App = null, MoverPassword = null, BucketAdminUser = null, BucketAdminPassword = null)`; новые case-ветки парсера для leaf-ключей `mover_password`, `bucket_admin_user`, `bucket_admin_password`.

- [ ] **Step 1: Тест-первый — парсер читает новые ключи**

В тест парсера добавить (AAA-комментарии):

```csharp
[Fact]
public void ParseClusters_ReadsPerClusterSecretKeys()
{
    // Arrange: kvs префикса /clusters/ с mover_password/bucket_admin_user/bucket_admin_password
    var kvs = new[]
    {
        new Kv("/clusters/c1/mover_password", "moverpass32charsaaaaaaaaaaaa"),
        new Kv("/clusters/c1/bucket_admin_user", "ba_user"),
        new Kv("/clusters/c1/bucket_admin_password", "bapass32charsaaaaaaaaaaaaa"),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert: значения попали в снапшот
    var snap = result.Value.Single(c => c.Config.Cluster == "c1");
    Assert.Equal("moverpass32charsaaaaaaaaaaaa", snap.MoverPassword);
    Assert.Equal("ba_user", snap.BucketAdminUser);
    Assert.Equal("bapass32charsaaaaaaaaaaaaa", snap.BucketAdminPassword);
}
```

- [ ] **Step 2: Run — тест падает**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~ParseClusters_ReadsPerClusterSecretKeys`
Expected: FAIL (компиляция: нет полей в record).

- [ ] **Step 3: Реализация — record + парсер**

`Domain.cs`:

```csharp
public sealed record ClusterSnapshot(ClusterConfig Config, IReadOnlyList<ShardSpec> Shards,
    IReadOnlyList<BucketRoute> Routing, AppCredentials? App = null,
    string? MoverPassword = null, string? BucketAdminUser = null, string? BucketAdminPassword = null);
```

`ClusterSnapshotParser.cs`: в `ClusterAcc` добавить `string? MoverPassword; string? BucketAdminUser; string? BucketAdminPassword;`; в switch добавить case-ветки по образцу `app_password` (leaf при `segments.Length == 4`, `Trim`, пустое → null); в `BuildCluster` передать в конструктор `ClusterSnapshot`.

- [ ] **Step 4: Run — зелёный**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release`
Expected: PASS (весь проект юнитов).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Core/Model/Domain.cs src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs src/tests/PgWorker.UnitTests
git commit -m "feat(pg): per-cluster mover/bucket_admin ключи в ClusterSnapshot (t02, arch/14 §3.3)"
```

---

### Task 3: ClusterSecretEnsurer — ensure тройки кредов

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AppSecretEnsurer.cs` (переименовать файл в `ClusterSecretEnsurer.cs`, интерфейс `IAppSecretEnsurer` → `IClusterSecretEnsurer`, класс `AppSecretEnsurer` → `ClusterSecretEnsurer`)
- Modify: `src/PgWorker.App/Program.cs` (DI: регистрации `IAppSecretEnsurer`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AppSecretEnsurerTests.cs` (переименовать в `ClusterSecretEnsurerTests.cs`, расширить)

**Interfaces:**
- Produces:
```csharp
public sealed record ClusterCredentials(AppCredentials App, string MoverPassword, AppCredentials BucketAdmin);
public interface IClusterSecretEnsurer
{
    Task<Result<ClusterCredentials>> EnsureAsync(string cluster, ClusterConfig config, CancellationToken ct);
}
```
Ключи: `/clusters/<C>/{app_user,app_password,mover_password,bucket_admin_user,bucket_admin_password}`. Mover-user фиксирован (`bucket_mover`) — ключа mover_user нет. bucket_admin: если `config.BucketAdminUser/Password` заданы — put-if-absent ИЗ config; иначе генерация (`"bucket_admin"` + `AppSecretGenerator.Generate()`).

- [ ] **Step 1: Тест-первый — ensure тройки**

Расширить переименованный тест: (1) пустой etcd → одна txn создаёт ВСЕ пять ключей (app_user/app_password/mover_password/bucket_admin_user/bucket_admin_password), `EnsureAsync` возвращает тройку; (2) `config.BucketAdminPassword="from-config"` → ключ `bucket_admin_password` = "from-config" (не сгенерирован); (3) существующие ключи не перезаписываются (put-if-absent, гонка → re-read возвращает существующие); (4) mover_password отсутствует при живом app → добирается только mover (compare только на отсутствующие).

- [ ] **Step 2: Run — FAIL**

Run: `dotnet build src/PgWorker.slnx -c Release` → ошибки компиляции по старому интерфейсу (ожидаемо на шаге переименования); тест ensure — FAIL.

- [ ] **Step 3: Реализация**

Класс по образцу текущего `AppSecretEnsurer` (failover-чтения по endpoints, ОДНА txn put-if-absent только отсутствующих, re-read после txn). Чтение расширить до пяти ключей (пара Reads по образцу `ReadAsync`); ensure-логика: для каждой из пар app/mover/bucket_admin — отсутствующий user/password генерируется (mover-user не генерируется — фикс `bucket_mover`; bucket_admin-user из config или `"bucket_admin"`); puts/compares только на отсутствующие ключи. Провал после txn (ключи неполные) — `ApplicationException` с перечислением присутствия каждого из пяти ключей.

- [ ] **Step 4: DI и компиляция**

`Program.cs` (строки ~223/273/288/322): переименовать типы; сигнатура `EnsureAsync` требует `ClusterConfig` — все вызовы передают `snap.Config`. Сборка всего решения до зелёного (ошибки вызовов — по компилятору): `dotnet build src/PgWorker.slnx -c Release`.

- [ ] **Step 5: Run — юниты зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "feat(pg): ClusterSecretEnsurer — ensure пер-cluster тройки app/mover/bucket_admin (t02 §3.1)"
```

---

### Task 4: Потребители кредов — гварды ролей, dsn-запись, mover-DSN из снапшота

**Files:**
- Modify: `src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs` (обобщить ALTER: `BuildAlterRolePasswordSql(string role, string password)`; `BuildAlterAppPasswordSql` оставить как тонкую обёртку)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs:106-107,606-616,659` (ensure → гварды/dsn из обеспеченных кредов)
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs:111-112,370-371`
- Modify: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs:353-354,441-478`
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` (`MoverConninfo`, `MoverNpgsqlDsn` — параметр mover-пароля)
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs:1033-1034` (mover-DSN из снапшота)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DatabaseProvisionerTests.cs`, `src/tests/PgWorker.UnitTests/Moves/*` (обновить фейки/вызовы)

**Interfaces:**
- Produces: `ShardEndpoints.MoverConninfo(string shardDsn, string moverPassword, string? advertisedHost = null)`, `ShardEndpoints.MoverNpgsqlDsn(string shardDsn, string moverPassword)` — env-секрет больше не читается внутри; резолвинг `snap.MoverPassword ?? secrets.MoverPassword` — на вызывающей стороне.

- [ ] **Step 1: Тест-первый — mover-DSN из переданного пароля**

В `MoveSqlPart2Tests`/`ShardEndpointsTests` добавить: `MoverConninfo(dsn, "newpass")` подставляет `password=newpass` и `user=bucket_mover`; `MoverNpgsqlDsn(dsn, "newpass")` — `Password=newpass`. Обновить существующие вызовы тестов на новую сигнатуру.

- [ ] **Step 2: Run — FAIL**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — сигнатуры изменились (компилятор перечислит все call-site: `MoveProcess`, фикстуры).

- [ ] **Step 3: Реализация резолвинга**

- `ShardEndpoints`: параметры `moverPassword` вместо `InstallSecrets` внутри двух методов (тело без изменений, кроме источника пароля).
- `MoveProcess`: в местах построения mover-DSN — `var moverPassword = snap.MoverPassword ?? secrets.MoverPassword;` и передача в `MoverConninfo`/`MoverNpgsqlDsn`/строку 1033.
- `ProvisioningProcess` P1.5: после `ensure.EnsureAsync(cluster, snap.Config, ct)` — гварды ролей `BuildRoleGuardsSql(secrets, creds.App, creds.BucketAdmin.User, creds.BucketAdmin.Password)` (env-fallback остаётся ВНУТРИ гвардов только как аргумент по умолчанию — не используется при обеспеченных кредах); P2.5 dsn-строка (строка 659) — из `creds.BucketAdmin`. Аналогично `AddShardProcess` (строки 111-112/370-371) и `AdoptionProcess` (353-354/441-478): ensure ПЕРЕД использованием, креды — из результата ensure.
- `DatabaseProvisioner.BuildAlterRolePasswordSql`:

```csharp
public static string BuildAlterRolePasswordSql(string role, string password)
    => $"ALTER ROLE \"{role}\" PASSWORD '{Escape(password)}';";
public static string BuildAlterAppPasswordSql(AppCredentials app)
    => BuildAlterRolePasswordSql(app.User, app.Password);
```

- [ ] **Step 4: Run — юниты зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release`
Expected: PASS (включая Moves-тесты — они в составе PgWorker.UnitTests).

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat(pg): mover/bucket_admin креды из per-cluster снапшота; ALTER ROLE обобщён (t02 §4.1)"
```

---

### Task 5: ClusterSecretRotator — ротация трёх ролей + перезапись dsn

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AppPasswordRotator.cs` → переименовать в `ClusterSecretRotator.cs`, класс `AppPasswordRotator` → `ClusterSecretRotator`
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` (regex замены password в dsn)
- Modify: `src/PgWorker.App/Program.cs` (DI-регистрация ротатора)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AppPasswordRotatorTests.cs` → `ClusterSecretRotatorTests.cs`

**Interfaces:**
- Consumes: `IClusterSecretEnsurer.EnsureAsync(cluster, snap.Config, ct)` → `ClusterCredentials`.
- Produces: journal `op=rotate-app-password` сохраняет имя op (совместимость журналов — образец `RotationRole.Phase` в PasswordRotator); txn R3.

- [ ] **Step 1: Тест-первый — фазы ротации тройки**

Расширить тесты ротатора (фейки уже есть в файле):
1. Заявка → на мастере каждого шарда с dsn выполнены ТРИ ALTER (`app`, `bucket_admin`, `bucket_mover`) — SQL-лог фиксируется фейком `ISqlExecutor`.
2. R3 txn: put `app_password`+`mover_password`+`bucket_admin_password` + put НОВОГО dsn на каждый шард (пароль bucket_admin заменён regex-ом, остальное dsn байт-в-байт) + del заявки; compare `value==OLD` на все dsn и три ключа.
3. Внешнее изменение dsn между чтением и txn → txn проиграна → Failed, заявка жива (ретрай тиком).
4. Нет заявки → `ProcessOutcome.Done`, ноль SQL.
5. Malformed-заявка → удаляется с journal-записью.

- [ ] **Step 2: Run — FAIL**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~ClusterSecretRotator`
Expected: FAIL.

- [ ] **Step 3: Реализация ротатора**

По образцу текущего `AppPasswordRotator` с изменениями:
- R1: `var creds = await appSecret.EnsureAsync(cluster, snap.Config, ct);` → OLD: `creds.App.Password`, `creds.MoverPassword`, `creds.BucketAdmin.Password` (все ключи существуют после ensure).
- R2: на каждый шард с dsn — три вызова `db.ExecuteAsync(dsn, BuildAlterRolePasswordSql(role, new…))` для `app`/`bucket_admin`/`bucket_mover`; NEW-пароли: `AppSecretGenerator.Generate()` ×3.
- R3 (одна txn):

```csharp
var compares = new List<TxnCompare>
{
    TxnCompare.ValueEqual(PasswordKey(cluster), creds.App.Password),
    TxnCompare.ValueEqual(MoverKey(cluster), creds.MoverPassword),
    TxnCompare.ValueEqual(BucketAdminPasswordKey(cluster), creds.BucketAdmin.Password),
};
var ops = new List<TxnOp>
{
    new TxnOp.Put(PasswordKey(cluster), newPassword, null),
    new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
    new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
    new TxnOp.Delete(TicketKey(cluster), Prefix: false),
};
foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
{
    compares.Add(TxnCompare.ValueEqual(DsnKey(cluster, shard.Name), shard.Dsn!));
    ops.Add(new TxnOp.Put(DsnKey(cluster, shard.Name),
        PasswordRegex().Replace(shard.Dsn!, m =>
            (m.Value.StartsWith(' ') ? " " : "") + "password=" + newBucketAdminPassword), null));
}
```

`PasswordRegex` в `ShardEndpoints`: `[GeneratedRegex(@"(^| )password=[^ ]*")]`. DSN-ключ: `$"/clusters/{cluster}/shards/{name}/dsn"`. Проигрыш compare (`txn.Value.Succeeded == false`) → `FailAsync` «ключ изменился с момента чтения — ретрай тиком».
- Mover-нюанс: active move при ротации — фазы создают новые подключения; DSN строится из свежего снапшота на тик (Task 4) — упавшая фаза возобновляется с journal-фазы. В доккомментарий класса добавить эту заметку.

- [ ] **Step 4: Run — зелёный**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Release`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat(pg): ClusterSecretRotator — заявка ротирует app+bucket_admin+mover с перезаписью dsn (t02 §5 I)"
```

---

### Task 6: API воркера — POST /api/clusters/{c}/secrets/rotate

**Files:**
- Modify: `src/PgWorker.App/Api/Operations/RotateAppPasswordHandler.cs` → `RotateClusterSecretsHandler.cs` (класс, DTO `AppPasswordRotatedDto` → `ClusterSecretsRotatedDto`)
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (route `app-password/rotate` → `secrets/rotate`)
- Test: `src/tests/PgWorker.IntegrationTests/Api/` (тест маршрута по образцу соседних) + юниты handler (если есть)

**Interfaces:**
- Produces: `POST /api/clusters/{c}/secrets/rotate` → 201 `{cluster, requested_unix, requested_by}`; 404/409/503 — семантика без изменений.

- [ ] **Step 1: Переименование + route**

Файл/класс/DTO переименовать; в `ApiModule.cs` route и комментарии обновить; тело handler (config-чтение, claim-txn NotExists, 409) — без изменений. Панель-прокси перевода в Task 7.

- [ ] **Step 2: Тест — 409 при живой заявке**

По образцу существующих тестов Api-папки: POST при живом `/pgworker/rotations/<C>` → 409; POST на неизвестном кластере → 404; повторный POST после снятия — 201.

- [ ] **Step 3: Run + интеграция pg**

Run: `dotnet test src/tests/PgWorker.IntegrationTests -c Release` (после серии — зачистка docker).
Затем docker-интеграционный тест ротации с writer-нагрузкой (новый файл `src/tests/PgWorker.IntegrationTests/Docker/ClusterSecretRotationTests.cs` по образцу соседних docker-тестов): поднять кластер (testcontainers, динамические порты), writer-цикл INSERT в фоне, заявка `/pgworker/rotations/<C>`, дождаться journal `done` (≤ 100 с), assert: app/bucket_admin/mover ключи изменились, dsn содержит новый bucket_admin-пароль, writer завершился без фатальных ошибок (допустимы ретраи реконнекта).
Expected: PASS.

- [ ] **Step 4: Зачистка docker**

Run: `docker rm -f $(docker ps -aq) ; docker network prune -f ; docker ps -aq | wc -l`
Expected: `0` (без учёта стенда).

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat(pg): API /secrets/rotate + docker-интеграция ротации тройки с writer-нагрузкой (t02)"
```

---

### Task 7: AdminPanel — pg-прокси и UI (семантика «пер-cluster секретов»)

**Files:**
- Modify: `src/AdminPanel.Api/Operations/RotateAppPasswordCommand.cs` → `RotateClusterSecretsCommand.cs` (record+handler; путь воркера)
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs:139-141` (route `secrets/rotate`)
- Modify: `frontend/src/api/queries.ts:188-192` (`rotateAppPassword` → `rotateClusterSecrets`, путь)
- Modify: `frontend/src/pages/cluster-details/RotateAppPasswordButton.tsx` → `RotateClusterSecretsButton.tsx` (тексты модалки: три роли, окно реконнекта)
- Modify: `frontend/src/pages/cluster-details/ClusterDetailsPage.tsx` (импорт кнопки)
- Test: `src/tests/AdminPanel.UnitTests/` (по образцу тестов прокси-команд)

**Interfaces:**
- Consumes: `POST /api/clusters/{c}/secrets/rotate` (Task 6).
- Produces: панельный маршрут `POST /api/clusters/{cluster}/rotate-secrets` (или текущий путь панели — сохранить форму существующего маршрута модуля, поменяв только upstream-путь).

- [ ] **Step 1: Прокси-команда**

Record `RotateClusterSecretsCommand(Cluster, RequestedBy)`; handler → `WorkerProxy.SendAsync<ClusterSecretsRotatedDto>(api, "pgworker", Post, $"/api/clusters/{command.Cluster}/secrets/rotate", null, command.RequestedBy, ct)` (образец текущего файла).

- [ ] **Step 2: Модуль + фронт**

`OperationsModule` — маршрут/комментарии; `queries.ts` — функция и URL; кнопка — переименовать файл/компонент, тексты модалки: «PgWorker сменит пароли ролей app, bucket_admin и bucket_mover… подключения со старым паролем отвергаются до перечитывания etcd».

- [ ] **Step 3: Сборка панели + тесты**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Release` и сборка SPA (`cd frontend && npm run build` или как в `docker/AdminPanel.Dockerfile`).
Expected: PASS / build OK.

- [ ] **Step 4: Commit**

```bash
git add -A src/AdminPanel.Api frontend/src
git commit -m "feat(panel): pg-ротация per-cluster секретов — прокси и UI (t02)"
```

---

### Task 8: KafkaWorker — BrokerEnvBuilder: Signing-CA и Trust-CA раздельно

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs` (Build: опциональные `signingCaPem/signingCaKey/trustCaPem`)
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/` (тест env-билдера по образцу соседних)

**Interfaces:**
- Produces:
```csharp
internal static IReadOnlyDictionary<string, string> Build(
    KafkaClusterSnapshot snap, string broker, NodeAddress addr,
    IReadOnlyList<string> appPasswords, IReadOnlyList<string> adminPasswords,
    ProvisioningOptions options, BrokerCertificateCache certificates,
    string? signingCaPem = null, string? signingCaKey = null, string? trustCaPem = null)
```
Семантика: подпись серта — `signingCaPem ?? snap.CaPem` / `signingCaKey ?? snap.CaKey` (кеш `GetOrCreate` по хешу CA-ключа даёт новый серт при NEW); truststore (`NodeEnvSpec.CaPem`) — `trustCaPem ?? snap.CaPem`.

- [ ] **Step 1: Тест-первый**

Тест: при `signingCaPem/signingCaKey` = NEW-материал и `trustCaPem` = bundle — env содержит `KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN` от NEW (серт ≠ серту от OLD при том же broker) и `KAFKA_SSL_TRUSTSTORE_CERTIFICATES` == bundle. Проверка «серт от NEW» — разбор PEM через `ClusterPki`-хелперы или сравнение с `certificates.GetOrCreate(cluster, broker, newCaPem, newCaKey, host)`.

- [ ] **Step 2: Run — FAIL → реализация → PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter FullyQualifiedName~BrokerEnvBuilder`
Expected: FAIL → после правки `Build` (guard: NEW-материал при ротации обязан быть полным — иначе `ApplicationException` по образцу премиграционного guard) → PASS весь проект юнитов.

- [ ] **Step 3: Commit**

```bash
git add -A src
git commit -m "feat(kfw): BrokerEnvBuilder — раздельные Signing/Trust CA для окна двойного доверия (t07)"
```

---

### Task 9: KafkaWorker — CaRotator (фазы P/D/R/C/F)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/CaRotator.cs`
- Modify: `src/KafkaWorker.App/Program.cs:298` (регистрация в цикле процессов рядом с `PasswordRotator`)
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/CaRotatorTests.cs` (фейки — `Fakes.cs`, `FakeKafkaAdminClient.cs`)

**Interfaces:**
- Consumes: `ClusterPki.GenerateCa(cluster)` → `(CaPem, CaKeyPem)`; `BrokerEnvBuilder.Build(..., signingCaPem, signingCaKey, trustCaPem)` (Task 8); `IClusterDriver.RemoveNodeAsync(cluster, broker, removeVolume: false, ct)` + `EnsureNodeAsync(spec, ct)` (образец `PasswordRotator.RollingRecreateAsync`); `IKafkaAdminClientFactory.Create(endpoints, adminUser, adminPassword, caPem)` для `WaitForBrokersAsync`.
- Produces: заявка `/kafkaworker/ca_rotations/<C>`; journal `op=rotate-ca`, фазы `phase-p`, `phase-d`, `phase-r/<broker>`, `committed`, `done`.

- [ ] **Step 1: Тест-первый — фазы на фейках**

Сценарии (фейковый etcd+driver, AAA):
1. P: заявка → txn put-if-absent `ca_next_key/ca_next_pem` (значение стабильно между тиками).
2. D: `ca_pem` == OLD + "\\n" + nextPem (bundle); повторный тик не перезаписывает (bundle уже содержит nextPem).
3. R: каждый брокер пересоздаётся (RemoveNode volume=false + EnsureNode с env: keystore от NEW, truststore == bundle), порядок по имени, по одному за тик; health-check между брокерами; трек `_rolled` не повторяет пересозданное.
4. C: после всех брокеров — одна txn `[compare value(ca_next_key)==staging][put ca_pem=NEW; put ca_key=NEW; del ca_next_pem; del ca_next_key; del заявку]`; заявка снята.
5. Re-entry: сбой фейка на любой фазе → следующий тик продолжает с journal-фазы без повторной генерации CA.
6. Клэйм не наш → Failed, ноль мутаций.

- [ ] **Step 2: Run — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter FullyQualifiedName~CaRotator`
Expected: FAIL (класса нет).

- [ ] **Step 3: Реализация CaRotator**

Каркас по образцу `PasswordRotator` (failover `GetAsync`/`TxnAsync`, claim-guard, journal, `ConcurrentDictionary` треки). Ключевые отличия:

```csharp
// Фаза P: staging НОВОЙ CA — одна на жизнь ротации (в etcd, а не в памяти —
// переживает рестарт воркера, в отличие от _newPasswords PasswordRotator).
var next = await GetAsync(NextKeyKey(cluster), ct);
if (next.Value is null)
{
    var (caPem, caKey) = ClusterPki.GenerateCa(cluster);
    var txn = await TxnAsync(TxnRequest.Of(
        [TxnCompare.NotExists(NextKeyKey(cluster)), TxnCompare.NotExists(NextPemKey(cluster))],
        [new TxnOp.Put(NextKeyKey(cluster), caKey, null), new TxnOp.Put(NextPemKey(cluster), caPem, null)]), ct);
    // проигрыш (гонка) — re-read; заявка жива
}

// Фаза D: bundle ДО замены сертов — клиенты с OLD-кэшем доверяют NEW.
var bundle = snap.CaPem!.Contains(nextPem) ? snap.CaPem : snap.CaPem + "\n" + nextPem;
// txn put ca_pem=bundle (idempotent: contains-check выше)

// Фаза R: rolling по одному брокеру (порядок Ordinal по имени, как PasswordRotator):
var env = BrokerEnvBuilder.Build(snap, broker.Name, addr, [snap.AppPassword!], [snap.AdminPassword!],
    options, certificates, signingCaPem: nextPem, signingCaKey: nextKey, trustCaPem: bundle);

// Фаза C: одна txn — коммит + cleanup staging + снятие заявки.
```

`WaitForBrokersAsync` — приватная копия из `PasswordRotator` (доверие — `bundle`, после D всегда bundle). Guard премиграционного кластера: `snap.CaPem/CaKey/AdminPassword` null → journal `waiting-cluster`, брокеров не трогаем. Док-комментарий класса: ссылка на arch/16 §2.3 и фазы.

- [ ] **Step 4: DI-регистрация**

`Program.cs`: `AddSingleton(sp => new CaRotator(etcd, endpoints, driver, claims, journal, adminFactory, options, certificates, snapshot))` рядом с `PasswordRotator` (строка 298) и включить в тик-цикл процессов там же, где ротатор.

- [ ] **Step 5: Run — юниты зелёные**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Release`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "feat(kfw): CaRotator — ротация per-cluster CA (staging/bundle/rolling/commit, t07 arch/16 §2.3)"
```

---

### Task 10: KafkaWorker API + интеграция TLS-ротации

**Files:**
- Create: `src/KafkaWorker.App/Api/Operations/RotateCaHandler.cs` (образец `RotateAppPasswordHandler.cs` воркера)
- Modify: `src/KafkaWorker.App/Api/ApiModule.cs:210-240` (маршрут рядом с двумя существующими)
- Test: `src/tests/KafkaWorker.IntegrationTests/Kafka/CaRotationTests.cs` (по образцу `TlsClusterTests.cs`), юнит API по образцу соседних

**Interfaces:**
- Produces: `POST /api/kafka/clusters/{c}/ca/rotate` → 201 `{cluster, requested_unix, requested_by}`; 404 (нет config) / 409 (не Active или заявка жива) / 503.

- [ ] **Step 1: Handler + маршрут**

Копия kafka-`RotateAppPasswordHandler` с изменениями: ключ заявки `/kafkaworker/ca_rotations/{cluster}`; DTO `CaRotatedDto`; в `ApiModule.cs` — `MapPost("/api/kafka/clusters/{cluster}/ca/rotate", …)`.

- [ ] **Step 2: Юнит-API**

Тесты: 409 при живой заявке; 404 на неизвестном кластере; claim-txn проигрыш → 409.

- [ ] **Step 3: Интеграционный тест окна двойного доверия (docker)**

`CaRotationTests` по образцу `TlsClusterTests` (TLS-кластер testcontainers, динамические порты):
1. Поднять кластер, зафиксировать OLD `ca_pem/ca_key`.
2. Клиент (admin, доверие OLD) пишет/читает — базовая зелёная точка.
3. Поставить заявку `/kafkaworker/ca_rotations/<C>` (etcd напрямую, формат §9.8), тикать воркер до journal `done` (≤ 100 с).
4. Assert: `ca_pem` == NEW (только), `ca_next_*` удалены, все брокеры работают; в момент окна (пока фаза R) клиент с bundle и клиент с OLD подключаются; после коммита клиент с OLD-CA-кэшем против NEW-сертов — ожидаемо отклонён, с NEW — подключён.
Expected: PASS.

- [ ] **Step 4: Зачистка docker**

Run: `docker rm -f $(docker ps -aq) ; docker network prune -f ; docker ps -aq | wc -l`
Expected: `0`.

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat(kfw): API /ca/rotate + docker-интеграция окна двойного доверия (t07)"
```

---

### Task 11: AdminPanel — kafka-прокси и UI «Ротация CA»

**Files:**
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs` (+`RotateCaCommand`)
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs:92-94` (+маршрут)
- Modify: `frontend/src/api/queries.ts` (+`rotateCa`)
- Create: `frontend/src/pages/kafka-cluster/RotateCaButton.tsx` (образец `RotateAdminPasswordButton.tsx`)
- Modify: страница kafka-кластера (импорт кнопки)
- Test: `src/tests/AdminPanel.UnitTests/` (прокси по образцу)

**Interfaces:**
- Consumes: `POST /api/kafka/clusters/{c}/ca/rotate` (Task 10).

- [ ] **Step 1: Прокси + модуль + фронт**

`RotateCaCommand(Cluster, RequestedBy)` → `WorkerProxy.SendAsync<CaRotatedDto>(api, "kafkaworker", Post, $"/api/kafka/clusters/{command.Cluster}/ca/rotate", …)`. Маршрут модуля, `queries.ts`, кнопка с модалкой: «Будет сгенерирована новая per-cluster CA; брокеры пересоздаются по одному (rolling), окно двойного доверия; приложения перечитывают ca_pem из etcd».

- [ ] **Step 2: Тесты + сборка SPA**

Тест пробы на bundle (спека §4.2): юнит `AdminPanel.Probes` — `KafkaProbe`/клиент с `ca_pem`-строкой OLD+NEW (конкатенация) успешно строит конфиг подключения (передача значения без правок кода пробы; тест фиксирует совместимость). Прокси-тесты модуля.

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Release && cd frontend && npm run build`
Expected: PASS / build OK.

- [ ] **Step 3: Commit**

```bash
git add -A src/AdminPanel.Api frontend/src
git commit -m "feat(panel): kafka-ротация CA — прокси и UI-модалка (t07)"
```

---

### Task 12: Стенд/e2e-документация + полный E2E Release

**Files:**
- Modify: `deploy/.env.example` (комментарии: PGW_BUCKET_ADMIN/MOVER — fallback до ensure; etcd-ключи — канон)
- Modify: `README.md` (секция секретов/ротации: pg `/secrets/rotate`, kafka `/ca/rotate`, etcd-ключи)
- Test: полный E2E

- [ ] **Step 1: Доки deploy/README**

Обновить комментарии `.env.example` (fallback-семантика) и README (как запустить ротацию: панель → кнопки; что происходит в etcd; окно реконнекта/двойного доверия).

- [ ] **Step 2: Юниты → интеграция → E2E последовательно, зачистка после каждой серии**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests src/tests/KafkaWorker.UnitTests src/tests/AdminPanel.UnitTests -c Release
docker rm -f $(docker ps -aq); docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests src/tests/KafkaWorker.IntegrationTests -c Release
docker rm -f $(docker ps -aq); docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eFixture
docker rm -f $(docker ps -aq); docker network prune -f
```
Expected: все зелёные; E2E — на свежем Release (без `PGW_TEST_E2E_NOBUILD`); счётчик контейнеров после зачистки — 0.

- [ ] **Step 3: Финальный коммит**

```bash
git add -A
git commit -m "docs(t02+t07): deploy/README — ротация per-cluster секретов и CA; прогон юниты→интеграция→E2E Release зелёный"
```

---

## Порядок исполнения и зависимости

1 → 2 → 3 → 4 → 5 → 6 → 7 (pg-трек) → 8 → 9 → 10 → 11 (kafka-трек) → 12 (финал).
Task 8 зависит только от Task 1 (может идти параллельно pg-треку при ручном исполнении; при последовательном — после 7).

**Мерж-гейт (Фаза 8, отдельным коммитом вместе с мержем):** удалить `t02-per-cluster-secrets` из `arch/roadmap/pgworker.md` и `t07-kafka-ca-rotation` из `arch/roadmap/kafkaworker.md`; добавить в `arch/roadmap/pgworker.md` отложенную задачу «интеграция с внешним secret-manager» (очередной свободный NN, краткое описание: публикация/чтение per-install/per-cluster секретов внешним SM — решение пользователя: etcd остаётся единственным хранилищем до такой интеграции).
