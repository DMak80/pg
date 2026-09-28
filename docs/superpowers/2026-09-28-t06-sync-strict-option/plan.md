# t06-sync-strict-option — план реализации (per-cluster `synchronous_mode_strict`)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ САБ-СКИЛЛ: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять план задача-за-задачей.
> Шаги отмечены чекбоксами (`- [ ]`) для трекинга.

**Цель:** сделать «долговечность vs доступность» при потере синхронных реплик
per-cluster настройкой `synchronous_mode_strict` в `/clusters/<C>/config`
(etcd-контракт + API воркера + панель + bootstrap Patroni + конвергенция DCS),
с валидацией `strict → replicas ≥ 2` и эскалацией алерта `sync-standby-missing`
до critical у strict-кластеров.

**Архитектура:** arch-first — сначала канон `arch/14` + `arch/adminpanel/02/03`,
затем код, зеркалящий канон. Единый источник опции — etcd-ключ
`/clusters/<C>/config` (поле `synchronous_mode_strict`, отсутствие = `true`):
bootstrap читает при (пере)создании нод, конвергенция DCS приводит живой Patroni
к ожиданию тиком надзора (PATCH /config, без рестартов), мутация — новый
`PUT /api/clusters/{c}/config` (RMW-txn по `mod_revision`, образец KafkaWorker
`PUT /api/kafka/clusters/{c}/config`), панель — прокси + UI.

**Стек:** .NET 10 (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), Minimal API, xUnit + FluentAssertions (AAA),
React/TypeScript/Mantine 9.5 (панель). Новых пакетов НЕ вводится
(`Directory.Packages.props` не меняется).

**Спека:** `docs/superpowers/2026-09-28-t06-sync-strict-option/spec.md`
(исполнитель читает спеку и план вместе; спека — источник решений, план —
разворачка в шаги).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t06-sync-strict-option`
(ветка `feat-t06-sync-strict-option`); все команды ниже — из корня worktree.

**Wire-имя опции (сквозное, критично):** REST/DTO-контракт —
`synchronousModeStrict` (спека §2 п.7). Именованный camelCase-биндинг Minimal
API даёт `SynchronousModeStrict → "synchronousModeStrict"` автоматически, а
короткое `SyncStrict → "syncStrict"` — НЕ совпадает: воркерский рекорд создания
`CreateClusterRequest` обязан нести
`[property: JsonPropertyName("synchronousModeStrict")]` (Задача 5); панельные
копии рекордов со свойством `SynchronousModeStrict` атрибута не требуют.

**Семантика чтения поля (сквозная, критично):** отсутствие поля ИЛИ не-bool
значение = `true` (решение пользователя №3). Везде тернарная форма
`TryGet && isBool ? GetBoolean() : true` — свёртка `&& ... && GetBoolean()`
инвертирует семантику (false при отсутствии/не-bool) и НЕ допускается.

## Глобальные ограничения (каждая задача неявно включает этот раздел)

- Код правится только после правки arch/ (arch-first): Задачи 1–3 — до Задачи 4.
- Воркер НИКОГДА не инициирует рестарт PG; strict — динамический параметр
  Patroni, применяется только PATCH /config.
- Усыновлённые (`object`) шарды и внешние Patroni-контуры не затрагиваются.
- Поле в etcd — только bool; отсутствие/битое не-bool = `true`; воркер поле не
  пишет (только API создания/мутации).
- Мутация — только Active-кластерам (409 иначе); меняется ровно одно поле.
- Включение strict требует `replicas ≥ 2` на всех шардах: создание, add-shard,
  включение — иначе 400; выключение разрешено всегда.
- Дефолт новых кластеров — `true` (durability-first).
- Комментарии/документация — по-русски; идентификаторы — английские; тесты — с
  AAA-комментариями; config-JSON — snake_case (`synchronous_mode_strict`),
  REST/DTO — camelCase (`synchronousModeStrict`).
- Docker-порты в тестах — только динамические (никаких литералов хост-портов);
  таймауты фикстур `BrokerBootSec ≤ 100 с`.
- E2E: изоляция guid-именами, полный teardown при любом исходе + ассерт чистоты,
  телеметрия в `/tmp/pgw-e2e-artifacts-<guid>/` (docs/e2e-isolation.md,
  docs/e2e-launch.md); между тестовыми сериями — зачистка docker-контейнеров и
  сетей.
- KafkaWorker/ValkeyWorker не затрагиваются (кроме как образец кода); локально
  собираемые образы в registry не кладутся.

**Порядок исполнения:** Задачи 1–3 (arch) → 4–9 (воркер) → 10–15 (панель и
фронтенд) → 16 (интеграция) → 17 (E2E) → 18 (мерж-гейт + roadmap). Каждая
задача заканчивается зелёной сборкой и коммитом.

---

### Задача 1: Канон воркера — `arch/14-pgworker.md`

**Файлы:**
- Modify: `arch/14-pgworker.md` (§1.1 таблица эндпоинтов; §2.1 канон Patroni;
  §3 контракт etcd; §5 A ProvisioningProcess; §5 C NodeSupervisor; §5 G AddShardProcess)

**Интерфейсы:**
- Потребляет: перечень правок из спеки §5.1 (обязательный объём).
- Производит: канонические формулировки, на которые ссылаются Задачи 4–9 и 17.

**Вход (предусловие):** worktree чист, спека прочитана.

- [ ] **Шаг 1: §1.1 — добавить строку в таблицу эндпоинтов** (после строки
  `POST /api/clusters/{c}/secrets/rotate`):

```markdown
| `PUT /api/clusters/{c}/config` | мутация per-cluster опции `synchronous_mode_strict` | 02 §9.10: RMW-txn по mod_revision; гварды — кластер Active |
```

- [ ] **Шаг 2: §2.1 — strict перестаёт быть константой.** Найти в §2.1 абзац
  про канон таймингов Patroni (строка ~229: `retry_timeout=3` (плюс
  `synchronous_mode=true`, strict=false)...) и заменить упоминание strict:
  канон таймингов остаётся глобальным, но `synchronous_mode_strict` —
  per-cluster опция из `/clusters/<C>/config` (отсутствие поля = `true`,
  дефолт durability-first): bootstrap ноды подставляет значение кластера в
  `bootstrap.dcs` SPILO_CONFIGURATION, живые кластеры приводит конвергенция
  DCS (§5 C) — динамический параметр, без рестартов PG.

- [ ] **Шаг 3: §3 — контракт etcd.** В таблице/описании ключа
  `/clusters/<C>/config` расширить формат JSON полем:

```markdown
`"synchronous_mode_strict"?: true|false` — per-cluster HA-режим (t06):
отсутствие поля или не-bool значение = `true` (durability-first: strict-кластер
при отсутствии sync-standby блокирует запись). Пишут ТОЛЬКО API создания
(POST /api/clusters) и мутации (PUT /api/clusters/{c}/config, 02 §9.10);
воркер поле читает (bootstrap + конвергенция), но никогда не пишет.
```

- [ ] **Шаг 4: §5 A (ProvisioningProcess)** — к описанию сборки env ноды
  добавить: `SpiloEnvBuilder` подставляет `synchronous_mode_strict` из
  config кластера в `bootstrap.dcs` SPILO_CONFIGURATION.

- [ ] **Шаг 5: §5 C (NodeSupervisor, конвергенция)** — к описанию конвергенции
  DCS добавить: в сверку входит `synchronous_mode_strict` с per-cluster
  ожиданием из config кластера (не константа); расхождение попадает в тот же
  единственный PATCH /config тика (порядок ключей патча: тайминги → strict →
  параметры).

- [ ] **Шаг 6: §5 G (AddShardProcess)** — добавить гвард: strict-кластер
  (`config.synchronous_mode_strict` = true, отсутствие поля = true) отклоняет
  add-shard с `replicas < 2` (400): без sync-standby новый шард блокировал бы
  запись.

- [ ] **Шаг 7: Проверка**

```bash
grep -n "synchronous_mode_strict" arch/14-pgworker.md | wc -l   # >= 5 вхождений (разделы §2.1/§3/§5 A/C/G)
grep -n "PUT /api/clusters/{c}/config" arch/14-pgworker.md       # 1 строка таблицы §1.1
```

- [ ] **Шаг 8: Коммит**

```bash
git add arch/14-pgworker.md
git commit -m "arch(14): per-cluster synchronous_mode_strict — эндпоинт PUT config, контракт etcd, bootstrap/конвергенция/add-shard (t06)"
```

**Выход:** канон воркера обновлён. **Проверка:** grep-критерий шага 7.
**Закрывает:** спека §5.1 строки arch/14.

---

### Задача 2: Канон панели — `arch/adminpanel/02-etcd-contract.md`

**Файлы:**
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.1 строка config; §3 модель
  снапшота; §9.1 набор ключей создания; §9.3 валидация; новый §9.10)

**Интерфейсы:**
- Производит: §9.10 — протокол мутации config (RMW-txn), на который ссылается
  Задача 8 (воркер) и Задача 12 (панель).

**Вход:** Задача 1 закоммичена.

- [ ] **Шаг 1: §2.1 — строка таблицы `/clusters/<C>/config`** расширить формат
  значения: `JSON {"buckets":N,"dbname":"<C>","created_unix":…,"state"?:"…","synchronous_mode_strict"?:true|false}`; в
  «В модель» добавить `ClusterInfo.SynchronousModeStrict`; в примечания —
  семантику «отсутствие/не-bool = true; пишут API создания/мутации (§9.10),
  воркер читает».

- [ ] **Шаг 2: §3 — модель снапшота:** в описание `ClusterInfo` добавить поле
  `SynchronousModeStrict` (bool; дефолт `true`).

- [ ] **Шаг 3: §9.1 — набор ключей создания:** config-JSON создаваемого
  кластера получает `synchronous_mode_strict` (значение из запроса создания;
  отсутствие поля запроса = `true`).

- [ ] **Шаг 4: §9.3 — валидация:** добавить правило: `synchronous_mode_strict
  != false` (вкл. отсутствие = true) при `replicas < 2` → ошибка по полю
  `syncStrict` («strict-режим требует replicas ≥ 2 на каждом шарде (без
  sync-standby запись блокируется)»); при явном `false` ограничений нет.

- [ ] **Шаг 5: новый §9.10 «Мутация config (synchronous_mode_strict)»** —
  вставить после §9.9 (управление серверным сертом API). Содержимое (по образцу
  §10.2 KafkaWorker):

```markdown
### 9.10. Мутация config: `synchronous_mode_strict` (t06)

Исполнитель — воркер (`PUT /api/clusters/{c}/config`, arch/14 §1.1); панель —
зеркальный эндпоинт-прокси. Тело: `{"synchronousModeStrict": bool}` — поле
обязательное (отсутствие → 400); меняется ровно это поле, прочие поля config
переносятся без изменений.

Протокол (RMW-txn по образцу §10.2):
1. Чтение `/clusters/<C>/config` напрямую у etcd (НЕ из снапшота) вместе с
   `mod_revision`. Ключа нет → 404; битый JSON → 503.
2. Гвард: `state` не Active → 409 (NOT_INITIALIZED «дождитесь инициализации» /
   TO_REMOVE «кластер удаляется»).
3. Валидация включения: `synchronousModeStrict=true` требует `replicas ≥ 2` на
   ВСЕХ шардах (чтение `/clusters/<C>/shards/*/replicas`) → иначе 400 по полю
   `syncStrict`. Выключение (false) разрешено всегда.
4. Идемпотентность: текущее значение уже совпадает → 204 без записи.
5. txn: `compare mod_revision(config) == прочитанной` + `put` обновлённого
   config-JSON. Проигрыш compare → 503 (retry клиентом).

Коды: 204 / 400 (валидация, битое тело) / 404 / 409 / 503 (etcd, гонка).
Применение к живому Patroni — конвергенция DCS воркера (arch/14 §5 C):
панель после 204 показывает значение опции; наблюдение применения — HA-страница
`/service/<scope>/config` (raw-JSON) и журнал `dcs-converge`.
```

- [ ] **Шаг 6: Проверка**

```bash
grep -n "### 9.10" arch/adminpanel/02-etcd-contract.md   # 1 заголовок
grep -n "SynchronousModeStrict" arch/adminpanel/02-etcd-contract.md  # >= 2 (§2.1, §3)
```

- [ ] **Шаг 7: Коммит**

```bash
git add arch/adminpanel/02-etcd-contract.md
git commit -m "arch(adminpanel/02): поле synchronous_mode_strict в config-контракте + §9.10 протокол мутации (t06)"
```

**Выход:** панельный etcd-контракт обновлён. **Закрывает:** спека §5.1 строки adminpanel/02.

---

### Задача 3: Канон UI/API панели — `arch/adminpanel/03-panels.md`

**Файлы:**
- Modify: `arch/adminpanel/03-panels.md` (§1 таблица эндпоинтов; §1.1 тело
  создания; §2 DTO; §3.1 форма создания; §3.2 форма add-shard; §3 Cluster
  details; §4 каталог алертов)

**Вход:** Задача 2 закоммичена.

- [ ] **Шаг 1: §1 — таблица эндпоинтов:** строка
  `PUT /api/clusters/{cluster}/config — мутация synchronous_mode_strict
  (204; прокси в API воркера — 02 §9.10; 400/404/409/503 как у воркера)`.

- [ ] **Шаг 2: §1.1 — тело создания:** добавить поле
  `synchronousModeStrict?: boolean` (отсутствие = true; false допускает
  replicas=1).

- [ ] **Шаг 3: §2 — DTO:** `CreateClusterRequestDto` +
  `synchronousModeStrict: boolean?`; `ClusterInfoDto`/`ClusterDto` +
  `synchronousModeStrict: boolean` (снапшот-поле).

- [ ] **Шаг 4: §3.1 — форма создания:** чекбокс «Синхронный strict-режим
  (блокировать запись при потере sync-реплики)», дефолт включён;
  клиентская валидация: strict + replicas=1 → ошибка у поля реплик (зеркало
  серверной).

- [ ] **Шаг 5: §3.2 — форма add-shard:** при strict-кластере поле «реплики» —
  минимум 2 (клиентская валидация; серверная — arch/14 §5 G).

- [ ] **Шаг 6: §3 Cluster details:** шапка — бейдж «strict»/«availability» +
  переключатель (мутация только для Active; не-Active — контрол заблокирован с
  подсказкой); подтверждение — модальный диалог с предупреждением о
  последствиях (включение: запись блокируется при потере sync-standby;
  выключение: возможна потеря «хвоста» при failover). Список кластеров —
  отображение режима.

- [ ] **Шаг 7: §4 — каталог алертов:** строка `sync-standby-missing` меняет
  severity на «strict ? critical : warning» (strict: запись блокирована —
  инцидент); в Hint/Remedy: для strict ремеди «запись блокирована —
  восстановите реплику (rebuild воркера) или выключите strict (PUT config)».

- [ ] **Шаг 8: Проверка**

```bash
grep -n "synchronousModeStrict" arch/adminpanel/03-panels.md  # >= 4 (§1.1, §2 ×2, §3)
grep -n "strict ? critical : warning" arch/adminpanel/03-panels.md  # 1 (§4)
```

- [ ] **Шаг 9: Коммит**

```bash
git add arch/adminpanel/03-panels.md
git commit -m "arch(adminpanel/03): PUT config эндпоинт, DTO/формы strict, эскалация sync-standby-missing (t06)"
```

**Выход:** UI/API-канон панели обновлён. **Закрывает:** спека §5.1 строки adminpanel/03; фаза 0 спеки завершена.

---

### Задача 4: Воркер — модель `ClusterConfig.SyncStrict` + парсер

**Файлы:**
- Modify: `src/PgWorker.Core/Model/Domain.cs:39-41` (ClusterConfig)
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs:229-258` (ParseConfig)
- Test: `src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs`

**Интерфейсы:**
- Производит: `ClusterConfig.SyncStrict` (bool, именованный параметр,
  дефолт `true`) — потребляется Задачами 6, 7 (воркер) и алертом панели
  (аналогичное поле, Задача 10).

**Вход:** arch/ канон (Задачи 1–3) закоммичен.

- [ ] **Шаг 1: Написать падающий тест** (добавить в
  `ClusterSnapshotParserTests.cs`; стиль файла — AAA-комментарии):

```csharp
// AAA (t06, spec §6.1): config-поле synchronous_mode_strict — true/false
// читается; отсутствие поля и не-bool значение = true (durability-first).
[Theory]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":true}""", true)]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":false}""", false)]
[InlineData("""{"buckets":2,"dbname":"shop"}""", true)]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":"yes"}""", true)]
public void ParseConfig_SyncStrict_FieldAbsentOrNonBool_IsTrue(string raw, bool expected)
{
    // Arrange — сид одного config-ключа кластера.
    // (Seed-механика фикстуры теста — по соседним кейсам файла; ключ
    //  /clusters/<C>/config со значением raw)
    var snapshot = ParseSingleClusterConfig(raw);

    // Act / Assert
    snapshot.Config.SyncStrict.Should().Be(expected);
}
```

Привязать `ParseSingleClusterConfig` к существующей фикстуре файла (как
соседние кейсы парсят `/clusters/<C>/…`; если файл парсит через kvs-список —
использовать его хелпер).

- [ ] **Шаг 2: Прогнать — убедиться, что падает**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~ClusterSnapshotParser"
```
Ожидание: FAIL — `ClusterConfig` не содержит `SyncStrict` (ошибка компиляции).

- [ ] **Шаг 3: Реализация.** `Domain.cs`, запись `ClusterConfig` — добавить
  последний позиционный параметр с дефолтом (существующие вызовы не меняются;
  дефолт = семантика «отсутствие = true»):

```csharp
/// <summary>/clusters/&lt;C&gt;/config: константы создания + state + per-cluster credentials.</summary>
public sealed record ClusterConfig(string Cluster, int Buckets, string DbName,
    long? CreatedUnix, ClusterState State,
    string? BucketAdminUser = null, string? BucketAdminPassword = null,
    bool SyncStrict = true);
```

`ClusterSnapshotParser.ParseConfig` — в удачном JSON-пути добавить аргумент
(после `ReadString(root, "bucket_admin_password")`) тернарной формой
(`&&`-свёртка даст false при отсутствии — запрещено):

```csharp
// t06: отсутствие поля или не-bool = true (durability-first, arch/14 §3).
SyncStrict: root.TryGetProperty("synchronous_mode_strict", out var strict)
    && strict.ValueKind is JsonValueKind.True or JsonValueKind.False
        ? strict.GetBoolean()
        : true),
```

Оба fallback-возврата (`raw is null`, `JsonException`) уже вернут `true` через
дефолт параметра — проверить, что в них не передан `SyncStrict: false`.

- [ ] **Шаг 4: Прогнать — зелёно** (команда шага 2; также весь юнит-проект):

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
```

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.Core/Model/Domain.cs src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs
git commit -m "feat(pgworker): ClusterConfig.SyncStrict + парсер config-поля (отсутствие/не-bool = true) (t06)"
```

**Выход:** снапшот воркера несёт strict-режим кластера. **Закрывает:** спека §3.1, §6.1 (парсер).

---

### Задача 5: Воркер — создание кластера: запрос, валидатор, план, сид

**Файлы:**
- Modify: `src/PgWorker.Core/Writing/CreateClusterRequest.cs` (запрос + Normalize + Validator)
- Modify: `src/PgWorker.Core/Writing/ClusterCreatePlan.cs:82-86` (ConfigJson)
- Modify: `src/PgWorker.Core/Seed/PostgresDemoSeedPlan.cs:20-22` (config сида)
- Test: `src/tests/PgWorker.UnitTests/Writing/CreateClusterStrictTests.cs` (новый)

**Интерфейсы:**
- Производит: `CreateClusterRequest.SyncStrict` (bool?, null → Normalize →
  true; wire-имя JSON — `synchronousModeStrict` через `JsonPropertyName` —
  camelCase от `SyncStrict` дал бы `syncStrict` и молча потерял поле);
  config-JSON создания содержит `synchronous_mode_strict`.

**Вход:** Задача 4 закоммичена.

- [ ] **Шаг 1: Падающие тесты** (новый файл
  `src/tests/PgWorker.UnitTests/Writing/CreateClusterStrictTests.cs`):

```csharp
using System.Text.Json;
using PgWorker.Core.Writing;

namespace PgWorker.UnitTests.Writing;

// t06 (spec §3.2/§6.2): опция strict в создании — нормализация null→true,
// валидация strict→replicas≥2, wire-имя synchronousModeStrict, ConfigJson.
public class CreateClusterStrictTests
{
    // Minimal API биндит camelCase (JsonSerializerDefaults.Web) — сериализуем
    // теми же опциями: wire-имя обязано быть synchronousModeStrict (spec §2.7).
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web);

    private static CreateClusterRequest Valid(bool? syncStrict = null) => new(
        "shop", 4, 2, 2, 2m, 8, 100, Sharded: true, SyncStrict: syncStrict);

    // AAA: Normalize — отсутствующая опция становится true (дефолт strict).
    [Fact]
    public void Normalize_NullSyncStrict_BecomesTrue()
    {
        // Arrange
        var request = Valid();

        // Act
        var normalized = request.Normalize();

        // Assert
        normalized.SyncStrict.Should().BeTrue();
    }

    // AAA: wire-имя поля — synchronousModeStrict (не syncStrict): биндинг
    // Minimal API и прокси-панель проходят только при точном имени.
    [Fact]
    public void Serialize_WireName_IsSynchronousModeStrict()
    {
        // Arrange
        var request = Valid(syncStrict: false);

        // Act
        var json = JsonSerializer.Serialize(request, Wire);

        // Assert
        json.Should().Contain("\"synchronousModeStrict\":false")
            .And.NotContain("\"syncStrict\"");
    }

    // AAA: strict (true или null) + replicas=1 — ошибка по полю syncStrict.
    [Theory]
    [InlineData(null)]
    [InlineData(true)]
    public void Validate_StrictWithSingleReplica_Fails(bool? syncStrict)
    {
        // Arrange
        var request = Valid(syncStrict) with { Replicas = 1 };

        // Act
        var errors = CreateClusterValidator.Validate(request.Normalize());

        // Assert
        errors.Should().ContainSingle(e => e.Field == "syncStrict")
            .Which.Message.Should().Contain("replicas ≥ 2");
    }

    // AAA: strict=false + replicas=1 — валидно (ограничений нет).
    [Fact]
    public void Validate_StrictOffWithSingleReplica_Passes()
    {
        // Arrange / Act
        var errors = CreateClusterValidator.Validate(
            Valid(syncStrict: false) with { Replicas = 1 }.Normalize());

        // Assert
        errors.Should().BeEmpty();
    }

    // AAA: план пишет synchronous_mode_strict в ConfigJson.
    [Theory]
    [InlineData(null, "\"synchronous_mode_strict\":true")]
    [InlineData(false, "\"synchronous_mode_strict\":false")]
    public void Build_ConfigJsonCarriesStrict(bool? syncStrict, string expected)
    {
        // Arrange / Act
        var plan = ClusterCreatePlan.Build(Valid(syncStrict).Normalize(), 1_700_000_000);

        // Assert
        plan.ConfigValue.Should().Contain(expected);
    }
}
```

- [ ] **Шаг 2: Прогнать — упадёт** (нет свойства `SyncStrict`):

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~CreateClusterStrict"
```

- [ ] **Шаг 3: Реализация.**

`CreateClusterRequest.cs` (добавить
`using System.Text.Json.Serialization;` в шапку файла):

```csharp
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk,
    bool? Sharded = null,
    [property: JsonPropertyName("synchronousModeStrict")] bool? SyncStrict = null)
{
    // Нормализация (arch/02 §9.3 + t06): sharded=false → buckets/shards в 1/1;
    // отсутствующий syncStrict трактуется как true (durability-first).
    // Вызывается ДО Validate. Идемпотентна.
    public CreateClusterRequest Normalize()
    {
        var sharded = Sharded ?? true;
        var syncStrict = SyncStrict ?? true;
        return sharded
            ? this with { Sharded = sharded, SyncStrict = syncStrict }
            : this with { Sharded = sharded, Buckets = 1, Shards = 1, SyncStrict = syncStrict };
    }
}
```

Wire-имя фиксировано атрибутом: camelCase от `SyncStrict` дал бы `syncStrict`,
а контракт (спека §2 п.7, §1.1 arch/03) — `synchronousModeStrict`.

В `CreateClusterValidator.Validate`, после блока `request.Replicas ...`
(границы реплик не меняются), добавить:

```csharp
// t06: strict-режим требует sync-standby → replicas ≥ 2; отсутствие опции = true.
if ((request.SyncStrict ?? true) && request.Replicas < 2)
    errors.Add(new("syncStrict", "strict-режим требует replicas ≥ 2 на каждом шарде (без sync-standby запись блокируется)"));
```

`ClusterCreatePlan.cs` — `ConfigJson` получает поле и передачу из запроса:

```csharp
private sealed record ConfigJson(
    [property: JsonPropertyName("buckets")] int Buckets,
    [property: JsonPropertyName("dbname")] string DbName,
    [property: JsonPropertyName("created_unix")] long CreatedUnix,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("synchronous_mode_strict")] bool SyncStrict);
```

и в `Build`: `var config = new ConfigJson(request.Buckets, request.Name, nowUnix,
NotInitialized, request.SyncStrict ?? true);` (`Build` вызывается после
Normalize; `?? true` — страховка от прямых вызовов).

`PostgresDemoSeedPlan.cs:20-22` — config сида (спека §3.2: «сид пишет поле
явленно (true)»):

```csharp
new PlanPut("/clusters/demo/config",
    $"{{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":{now},\"synchronous_mode_strict\":true}}"),
```

- [ ] **Шаг 4: Прогнать — зелёно** (шаг 2 + весь юнит-проект; упавшие
  существующие кейсы, если точечно сравнивали ConfigJson, — обновить ожидание
  на присутствие поля, НЕ на точную строку).

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.Core/Writing/CreateClusterRequest.cs src/PgWorker.Core/Writing/ClusterCreatePlan.cs src/PgWorker.Core/Seed/PostgresDemoSeedPlan.cs src/tests/PgWorker.UnitTests/Writing/CreateClusterStrictTests.cs
git commit -m "feat(pgworker): опция SyncStrict в создании кластера (wire synchronousModeStrict, null→true, валидация replicas≥2, ConfigJson, сид) (t06)"
```

**Выход:** создание пишет strict в config; противоречие не создаётся; wire-имя
сериализации/биндинга — `synchronousModeStrict`. **Закрывает:** спека §3.2, §6.2.

---

### Задача 6: Воркер — bootstrap: `SpiloEnvBuilder` + драйверы + EnsureNode-пути

**Файлы:**
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs:39-106` (SpiloEnvBuilder.Build)
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс `IClusterDriver.EnsureNodeAsync:36-37`, Plain `EnsureNodeAsync:188`, `BuildSpec:582`, Swarm `EnsureNodeAsync:767-786`)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (EnsureNodesAsync ≈445-465 и вызов ≈147)
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (EnsureNodesAsync ≈253-275)
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (EnsureDeclaredNodesAsync ≈277, RecreateMarkedNodes ≈501, rebuild ≈654)
- Modify: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs:416`
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs` (два вызова `EnsureNodeAsync`: восстановленная нода ≈563 и реплики ≈592 — метод `RejoinAsync`, `snap` доступен с ≈508)
- Modify (тестовая фикстура): `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs:297`
- Test: `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (сигнатурные вызовы)

**Интерфейсы:**
- Производит: `SpiloEnvBuilder.Build(ShardTopology, EtcdEndpoints,
  InstallSecrets, bool syncStrict, PgTuneResult? tuning = null,
  IReadOnlySet<string>? excludeParams = null)` — strict без дефолта, каждый
  вызов обязан передать значение явно;
  `IClusterDriver.EnsureNodeAsync(..., PgTuneResult? tuning, bool syncStrict,
  CancellationToken ct)`.

**Вход:** Задача 4 (SyncStrict в снапшоте) закоммичена.

- [ ] **Шаг 1: Падающий тест** (в `NodeConfigBuildersTests.cs`):

```csharp
// AAA (t06, spec §3.4/§6.3): strict из параметра попадает в bootstrap.dcs
// SPILO_CONFIGURATION рядом с synchronous_mode; false сохраняет прежний текст.
[Theory]
[InlineData(true, "synchronous_mode_strict: true")]
[InlineData(false, "synchronous_mode_strict: false")]
public void Build_SyncStrict_GoesToBootstrapDcs(bool syncStrict, string expected)
{
    // Arrange — топология/секреты фикстуры файла.
    // Act
    var spilo = SpiloEnvBuilder.Build(Topology, Etcd, Secrets, syncStrict)["SPILO_CONFIGURATION"];
    // Assert
    spilo.Should().Contain(expected).And.Contain("synchronous_mode: true");
}
```

- [ ] **Шаг 2: Прогнать — упадёт** (нет параметра `syncStrict`):

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~NodeConfigBuildersTests"
```

- [ ] **Шаг 3: Реализация SpiloEnvBuilder.** Сигнатура и YAML (значение —
  из config кластера; тестовые/изолированные пути передают `false` —
  байт-в-байт инвариант старых тестов сохраняется только у путей с false):

```csharp
public static IReadOnlyDictionary<string, string> Build(
    ShardTopology topology, EtcdEndpoints etcd, InstallSecrets secrets,
    bool syncStrict, PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)
```

в SPILO_CONFIGURATION (строки 92-93 текущего файла):

```
                    synchronous_mode: true
                    synchronous_mode_strict: {(syncStrict ? "true" : "false")}
```

Обновить doc-комментарий билдера: strict — per-cluster из config (t06).

- [ ] **Шаг 4: Прокинуть через драйвер.** `IClusterDriver.EnsureNodeAsync` —
  добавить `bool syncStrict` после `tuning` (без дефолта):

```csharp
Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
    InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
    PgTuneResult? tuning, bool syncStrict, CancellationToken ct);
```

`BuildSpec` (Plain, internal): + `bool syncStrict` (после `tuning`), вызов
`SpiloEnvBuilder.Build(topology, etcd, secrets, syncStrict, tuning, pgtuneExclude)`.
Plain `EnsureNodeAsync`: `BuildSpec(topology, nodeName, addr, secrets, etcd, resources, tuning, syncStrict)`.
Swarm `EnsureNodeAsync`: `plain.BuildSpec(..., tuning, syncStrict)`.
`Fakes.FakeDriver.EnsureNodeAsync` — тот же параметр (фикстура; значение
игнорирует, но можно сохранить `EnsuredSyncStrict` для ассертов).

- [ ] **Шаг 5: Обновить ВСЕ реализации и вызовы `EnsureNodeAsync`.**
  Прод-пути (значение — из снапшота кластера; спека: явность в каждом пути):
  - `ProvisioningProcess`: `EnsureNodesAsync(...)` получает параметр `bool
    syncStrict`, вызов из тика передаёт `snap.Config.SyncStrict`.
  - `AddShardProcess`: аналогично (`snap.Config.SyncStrict`).
  - `NodeSupervisor`: `EnsureDeclaredNodesAsync` (вызов ≈277) — из `snap`;
    `RecreateMarkedNodesAsync` (≈501) и rebuild-путь (≈654) — из `snap`.
  - `AdoptionProcess` (≈416, репарация контейнеров) — из `snap.Config.SyncStrict`
    (TickAsync уже принимает ClusterSnapshot).
  - `RestoreProcess` (прод, PgWorker.Backups): ДВА вызова в `RejoinAsync` —
    подъём восстановленной ноды (≈563, `firstEnsure`) и подъём реплик
    (≈592, `ensured`) — оба передают `snap.Config.SyncStrict` (bootstrap
    восстановленной ноды обязан нести strict кластера; `ClusterSnapshot snap` —
    параметр `RejoinAsync`).

  Тестовые call-сайты (`ClusterDriverTests`, `DockerDriverTests`) —
  `syncStrict: false` (изолированные пути драйвера).

  Тестовые РЕАЛИЗАЦИИ `IClusterDriver` (расширение интерфейса ломает их
  компиляцию — правка обязательна):
  - `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs:239` —
    заглушка: добавить параметр `bool syncStrict` в сигнатуру (тело не меняется).
  - `src/tests/PgWorker.IntegrationTests/Backups/BackupSelfHealTests.cs:203` —
    делегирующая обёртка: добавить параметр и прокинуть в `inner.EnsureNodeAsync`.
  - `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs:175` —
    заглушка: добавить параметр в сигнатуру (тело не меняется).
  - `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs:402` —
    делегирующая обёртка: параметр + прокидка в `inner`.
  - `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs:90` —
    заглушка: добавить параметр в сигнатуру (тело не меняется).
  - `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs:297` — `FakeDriver`
    (шаг 4).

  Вызовы `SpiloEnvBuilder.Build` в тестах (`PatroniTimingsTests:23`,
  `DcsConfigConvergenceTests:266`, `NodeConfigBuildersTests` все) — добавить
  `syncStrict: false` (кроме новых кейсов шага 1) — прежний YAML-инвариант.

- [ ] **Шаг 6: Прогнать** (сборка всего решения ловит оставшиеся
  компиляционные разрывы реализаций/вызовов; юниты):

```bash
dotnet build src/PgWorker.slnx -c Release
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
```

- [ ] **Шаг 7: Коммит**

```bash
git add -A src/PgWorker.Core/Templates/NodeConfigBuilders.cs src/PgWorker.Docker/Drivers/ClusterDriver.cs src/PgWorker.Provisioning/Processes/ src/PgWorker.Backups/Process/RestoreProcess.cs src/tests/
git commit -m "feat(pgworker): SpiloEnvBuilder syncStrict из config кластера; сигнатуры EnsureNode-путей, вкл. RestoreProcess (t06)"
```

**Выход:** bootstrap свежеподнятых нод (вкл. восстановленных из бэкапа) несёт
strict кластера. **Закрывает:** спека §3.4 (bootstrap), §6.3, риск «расширение
сигнатуры» §7.

---

### Задача 7: Воркер — конвергенция DCS: `DcsConfigConvergence` + `NodeSupervisor`

**Файлы:**
- Modify: `src/PgWorker.Core/Templates/DcsConfigConvergence.cs:44-67` (Analyze/DivergencePatch)
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs:182-196,306-361` (ConvergeDcsConfigAsync)
- Test: `src/tests/PgWorker.UnitTests/Templates/DcsConfigConvergenceTests.cs` (новые кейсы + обновление существующих)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` (новые кейсы + обновление существующих)

**Интерфейсы:**
- Производит: `DcsConfigConvergence.Analyze(string? configJson, bool syncStrict,
  IReadOnlyList<(string Name, string RawValue)>? desiredParameters)` и
  `DivergencePatch(string? configJson, bool syncStrict, ...)` — strict после
  `synchronous_mode` в блоке таймингов патча (порядок: тайминги → strict →
  параметры).

**Вход:** Задачи 4, 6 закоммичены.

- [ ] **Шаг 1: Падающие тесты конвергенции** (в `DcsConfigConvergenceTests.cs`):

```csharp
// AAA (t06, spec §6.4): strict расходится/отсутствует/битый → в патче;
// конвергентно → null. Порядок: strict после synchronous_mode.
[Fact]
public void Divergence_StrictMismatch_PatchCarriesStrict()
{
    // Arrange — живой конфиг с strict=false, ожидание true.
    const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"synchronous_mode_strict":false}""";

    // Act
    var patch = DcsConfigConvergence.DivergencePatch(config, syncStrict: true, null);

    // Assert
    patch.Should().Be("""{"synchronous_mode_strict":true}""");
}

[Fact]
public void Divergence_StrictAbsent_AddedToPatch()
{
    // Arrange — поля нет (легаси-конфиг).
    const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

    // Act
    var patch = DcsConfigConvergence.DivergencePatch(config, syncStrict: true, null);

    // Assert
    patch.Should().Be("""{"synchronous_mode_strict":true}""");
}

[Fact]
public void Divergence_StrictConvergent_NoPatch()
{
    // Arrange / Act
    var patch = DcsConfigConvergence.DivergencePatch(
        """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"synchronous_mode_strict":true}""",
        syncStrict: true, null);

    // Assert
    patch.Should().BeNull();
}

// AAA: порядок ключей — тайминги → strict → параметры (детерминизм).
[Fact]
public void Divergence_BothTimingAndStrictDiverge_StrictAfterTimings()
{
    // Arrange / Act
    var patch = DcsConfigConvergence.DivergencePatch("{}", syncStrict: true, null);

    // Assert
    patch.Should().Be(
        """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"synchronous_mode_strict":true}""");
}
```

- [ ] **Шаг 2: Прогнать — упадёт** (сигнатура без syncStrict):

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~DcsConfigConvergenceTests"
```

- [ ] **Шаг 3: Реализация `DcsConfigConvergence`:**

```csharp
public static string? DivergencePatch(
    string? configJson, bool syncStrict,
    IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
    => Analyze(configJson, syncStrict, desiredParameters).Patch;

public static ConvergenceDivergence Analyze(
    string? configJson, bool syncStrict,
    IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
```

внутри после `AddIfDivergent(timingPatch, root, "synchronous_mode", ...)`:

```csharp
// t06: strict — per-cluster ожидание из config кластера (арх/14 §5 C).
AddIfDivergent(timingPatch, root, "synchronous_mode_strict", syncStrict);
```

(перегрузка `AddIfDivergent(..., bool expected)` уже существует — строки
159-169.)

- [ ] **Шаг 4: `NodeSupervisor`:** `ConvergeDcsConfigAsync` получает параметр
  `bool syncStrict` (после `shard`), вызов в тике (строка 188) передаёт
  `snap.Config.SyncStrict`; внутри — `DcsConfigConvergence.Analyze(config.Value,
  syncStrict, desired)`. Doc-комментарий дополнить: strict — per-cluster из
  config, динамический параметр, рестартов не требует.

- [ ] **Шаг 5: Обновить существующие тесты** (все вызовы Analyze/DivergencePatch
  получают второй аргумент):
  - `DcsConfigConvergenceTests`: канонические конфиги фикстур дополняются
    `"synchronous_mode_strict":<ожидание>`; в кейсах с точным
    `patch.Should().Be(...)` (например
    `Regression_T09_Divergence_PatroniAdjustedConfig_MinimalPatch` — ожидание
    становится `{"loop_wait":1,"synchronous_mode_strict":true}` либо в
    living-конфиг добавляется конвергентный strict — сохранить смысл кейса:
    минимальный патч только по расходящемуся);
    self-check `Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource`
    (строки ≈250-290): Build вызывается с `syncStrict: false`, собираемый JSON
    дополняется `"synchronous_mode_strict":false`, Analyze — с
    `syncStrict: false` (инвариант null-патча сохраняется);
    аналогично `PatroniTimingsTests` (Build + self-check).
  - `NodeSupervisorTests`: сид `/clusters/shop/config` (строка 48) остаётся БЕЗ
    поля → SyncStrict=true → все living-конфиги GET /config фикстур,
    претендующие на конвергентность, дополняются `"synchronous_mode_strict":true`
    (`Regression_T09_DcsConfigConvergence_CanonicalConfig_NoPatch`,
    `Tick_DcsConfigConvergence_ParametersDiverged_...` и прочие с собранным
    canonical-конфигом — строки ≈550, ≈598); дивергентные кейсы получают
    Contain-ассерт `"synchronous_mode_strict":true` в патче.

- [ ] **Шаг 6: Новый кейс per-cluster в NodeSupervisorTests** (spec §6.4:
  NodeSupervisor передаёт значение кластера):

```csharp
// AAA (t06, spec §6.4): strict=false в config кластера — конвергенция ведёт
// DCS к false (per-cluster ожидание, не константа).
[Fact]
public async Task Tick_DcsConvergence_StrictFalseCluster_PatchesStrictFalse()
{
    // Arrange — сид с явным strict=false; GET /config с strict=true.
    var patches = new List<string>();
    var rig = await NewRig(_ => Ok(), respondRaw: r =>
    {
        if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
        {
            patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
        if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"synchronous_mode_strict":true}""",
                    Encoding.UTF8, "application/json"),
            };
        return Ok();
    }, configOverride: """{"buckets":2,"dbname":"shop","created_unix":1755900000,"synchronous_mode_strict":false}""");

    // Act
    var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert
    outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
    patches.Should().ContainSingle().Which.Should().Contain("\"synchronous_mode_strict\":false");
}
```

(`configOverride` — если хелпер сида `NewRig`/`Seed` не принимает переопределение
config, добавить параметр с дефолтом `null` → сеет стандартный сид, иначе
подменяет строку `/clusters/shop/config`; механика — по устройству `NewRig`
в файле.)

- [ ] **Шаг 7: Прогнать — зелёно:**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
```

- [ ] **Шаг 8: Коммит**

```bash
git add src/PgWorker.Core/Templates/DcsConfigConvergence.cs src/PgWorker.Provisioning/Processes/NodeSupervisor.cs src/tests/PgWorker.UnitTests/
git commit -m "feat(pgworker): конвергенция DCS сверяет synchronous_mode_strict per-cluster (t06)"
```

**Выход:** живые кластеры приводятся к strict-режиму config тиком надзора.
**Закрывает:** спека §3.4 (конвергенция), §6.4, риск «формат патча» §7.

---

### Задача 8: Воркер — API `PUT /api/clusters/{c}/config`

**Файлы:**
- Create: `src/PgWorker.App/Api/Operations/UpdateClusterConfigHandler.cs`
- Modify: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (2 новых исключения)
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (MapPut)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs:109-130` (FakeEtcd: хук `OnTxnBeforeCompare` — для кейса гонки шага 3б)
- Test: `src/tests/PgWorker.UnitTests/Api/UpdateClusterConfigHandlerTests.cs` (новый — юнит `RewriteStrict` + гонка RMW)

**Интерфейсы:**
- Потребляет: `IEtcdGateway`, `EtcdFailover`, `TxnCompare.ModRevisionEqual`
  (Shared.Etcd; образец `KafkaApiHelpers.ReadConfigAsync` +
  `UpdateConfigHandler` KafkaWorker; `Kv.ModRevision` — ulong, а
  `ModRevisionEqual(string, long)` — обязательный каст).
- Производит: эндпоинт `PUT /api/clusters/{c}/config`, тело
  `{"synchronousModeStrict": bool}` (camelCase от `SynchronousModeStrict` —
  атрибут не нужен); коды 204/400/404/409/503.

**Вход:** Задачи 4–7 закоммичены.

- [ ] **Шаг 1: Исключения** (`WorkerApiExceptions.cs`):

```csharp
// Валидация мутации config не прошла: 400 с errors по полям (t06, 02 §9.10).
public sealed class UpdateClusterConfigValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры мутации config некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-txn проигран: config изменился между чтением и записью — 503, retry клиентом (t06).
public sealed class ClusterConcurrentWriteException(string key)
    : Exception($"конкурентная запись изменила {key} между чтением и записью — повторите запрос");
```

- [ ] **Шаг 2: Handler** (`UpdateClusterConfigHandler.cs`; образец —
  KafkaWorker `UpdateConfigHandler` + `KafkaApiHelpers.ReadConfigAsync`).
  Внимание на каст `(long)kv.ModRevision` — `Kv.ModRevision` ulong,
  `ModRevisionEqual(string, long)`, неявного ulong→long нет:

```csharp
using System.Text.Json;
using PgWorker.Core;
using Shared.Etcd.Client;

using PgWorker.Core.Writing;
namespace PgWorker.App.Api.Operations;

// Тело PUT /api/clusters/{c}/config (t06, 02 §9.10): поле обязательное —
// отсутствие/не-bool → 400 (выключение strict «по умолчанию» недопустимо).
// camelCase-биндинг Minimal API: SynchronousModeStrict → "synchronousModeStrict".
public sealed record UpdateClusterConfigRequest(bool? SynchronousModeStrict);

// Мутация synchronous_mode_strict через API воркера (t06, arch/14 §1.1):
// RMW-txn по mod_revision прочитанного config (образец — KafkaWorker §10.2).
// Гварды: кластер Active; включение strict требует replicas ≥ 2 на всех шардах.
// Успех — 204 (без тела); применение к Patroni — конвергенция DCS воркера.
public sealed class UpdateClusterConfigHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result> HandleAsync(string cluster, UpdateClusterConfigRequest request, CancellationToken ct)
    {
        // 1) Тело: поле обязательно.
        if (request.SynchronousModeStrict is not { } strict)
            return Result.Failed(new UpdateClusterConfigValidationException(
                [new("synchronousModeStrict", "поле обязательно: boolean (true|false)")]));

        // 2) Имя каноническое + чтение config с mod_revision (напрямую у etcd).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result.Failed(new ClusterNotFoundException(cluster));
        var read = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/config", ct));
        if (!read.IsSuccess)
            return Result.Failed(read.Error!); // 503
        var kv = read.Value.FirstOrDefault(k => k.Key == $"/clusters/{cluster}/config");
        if (kv is null)
            return Result.Failed(new ClusterNotFoundException(cluster)); // 404
        string? rawState;
        bool current;
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            rawState = root.TryGetProperty("state", out var state)
                && state.ValueKind == JsonValueKind.String ? state.GetString() : null;
            current = root.TryGetProperty("synchronous_mode_strict", out var strictField)
                && strictField.ValueKind is JsonValueKind.True or JsonValueKind.False
                    ? strictField.GetBoolean() : true; // отсутствие/не-bool = true
        }
        catch (JsonException)
        {
            return Result.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        // 3) Гвард: только Active (409).
        if (rawState is not null)
            return Result.Failed(new ClusterNotActiveException(cluster, rawState));

        // 4) Валидация включения: strict=true → replicas ≥ 2 на всех шардах.
        //    Сбой чтения шардов — 503 (НЕ «валидация не прошла»).
        if (strict)
        {
            var check = await HasUndersizedShardAsync(cluster, ct);
            if (!check.IsSuccess)
                return Result.Failed(check.Error!); // 503
            if (check.Value is { } undersized)
                return Result.Failed(new UpdateClusterConfigValidationException(
                    [new("syncStrict",
                        $"включение strict требует replicas ≥ 2 на всех шардах (шард {undersized} с меньшим числом реплик; без sync-standby запись блокируется)")]));
        }

        // 5) Идемпотентность: значение совпадает → 204 без записи.
        if (current == strict)
            return Result.Success();

        // 6) RMW-txn: compare mod_revision + put пересобранного config
        //    (поле обновлено, прочие поля перенесены без изменений).
        //    Kv.ModRevision — ulong, ModRevisionEqual(long) — каст обязателен.
        var updated = RewriteStrict(kv.Value, strict);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(kv.Key, (long)kv.ModRevision)],
                [new TxnOp.Put(kv.Key, updated, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result.Failed(new ClusterConcurrentWriteException(kv.Key)); // 503, retry
        return Result.Success();
    }

    // Шард с replicas < 2 (имя, напр. "shard3"); null — все ≥ 2; Failed — 503.
    private async Task<Result<string?>> HasUndersizedShardAsync(string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/shards/", ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        foreach (var k in range.Value)
        {
            var segments = k.Key.Split('/');
            if (segments.Length == 6 && segments[3] == "shards" && segments[5] == "replicas"
                && int.TryParse(k.Value.Trim(), out var replicas) && replicas < 2)
                return Result<string?>.Success(segments[4]);
        }
        return Result<string?>.Success(null);
    }

    // Пересборка config-JSON: synchronous_mode_strict заменён/добавлен, прочие
    // свойства перенесены как есть (сырые JsonElement — формат значений 1:1).
    internal static string RewriteStrict(string raw, bool strict)
    {
        using var doc = JsonDocument.Parse(raw);
        var properties = doc.RootElement.EnumerateObject()
            .Where(p => p.Name != "synchronous_mode_strict")
            .Select(p => JsonSerializer.Serialize(p.Name) + ":" + p.Value.GetRawText())
            .ToList();
        properties.Add($"\"synchronous_mode_strict\":{(strict ? "true" : "false")}");
        return "{" + string.Join(",", properties) + "}";
    }
}
```

- [ ] **Шаг 3а: Юнит чистой функции** (новый файл
  `src/tests/PgWorker.UnitTests/Api/UpdateClusterConfigHandlerTests.cs`).
`RewriteStrict` живёт в handler'е как `internal static`; юнит-проект
ссылается на `PgWorker.App` (csproj:33) и `PgWorker.App.csproj` уже объявляет
`<InternalsVisibleTo Include="PgWorker.UnitTests"/>` (csproj:4) — тест
доступен без правок видимости. Код теста:

```csharp
// AAA (t06): пересборка config — поле заменено/добавлено, прочие сохранены.
[Theory]
[InlineData("""{"buckets":10,"dbname":"demo","created_unix":123,"synchronous_mode_strict":true}""", false)]
[InlineData("""{"buckets":10,"dbname":"demo","created_unix":123}""", true)]
public void RewriteStrict_ReplacesOrAddsField_OthersUntouched(string raw, bool strict)
{
    // Arrange / Act
    var result = UpdateClusterConfigHandler.RewriteStrict(raw, strict);

    // Assert
    using var doc = JsonDocument.Parse(result);
    doc.RootElement.GetProperty("buckets").GetInt32().Should().Be(10);
    doc.RootElement.GetProperty("dbname").GetString().Should().Be("demo");
    doc.RootElement.GetProperty("created_unix").GetInt64().Should().Be(123);
    doc.RootElement.GetProperty("synchronous_mode_strict").GetBoolean().Should().Be(strict);
}
```

- [ ] **Шаг 3б: Юнит гонки RMW (проигрыш compare → 503-исключение).**
  Детерминированно воспроизвести гонку между read и txn ОДНОГО вызова на
  реальном etcd нельзя (нужен прод-хук) — поэтому кейс гонки верифицируется
  юнитом через `Fakes.FakeEtcd` (реализует `IEtcdGateway`; хендлер принимает
  интерфейс — реальный код-путь read→txn проходит целиком), конкурентная запись
  инжектируется хуком `OnTxnBeforeCompare` по образцу KafkaWorker/ValkeyWorker
  (`KafkaWorker.UnitTests/Provisioning/Fakes.cs:44,117`;
  прецеденты-кейсы: `PortAllocHealerTests.cs:155` «гонка S5»).

  Сначала расширить `FakeEtcd` (`src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`,
  рядом с `TxnFault` ≈50; вызов — первой строкой `TxnAsync` ≈109-116, ДО
  вычисления compare):

```csharp
// Гонка RMW (t06, образец KafkaWorker Fakes): конкурентная запись ДО compare.
public Action<TxnRequest>? OnTxnBeforeCompare { get; set; }
```

```csharp
public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
{
    if (TxnFault?.Invoke(req) is { } failed)
        return Task.FromResult(failed);
    OnTxnBeforeCompare?.Invoke(req); // t06: инжекция гонки до compare
    ... // далее без изменений
}
```

  Тест (в `UpdateClusterConfigHandlerTests.cs`; FakeEtcd — internal того же
  проекта; `EtcdFailover.CallAsync` перебирает endpoints — передать один любой
  литерал, например `["http://fake"]`):

```csharp
// AAA (t06, spec §6.5 — форма: юнит, см. примечание в Задаче 15 шаг 2):
// конкурентная запись между read и txn — compare проигран, хендлер возвращает
// ClusterConcurrentWriteException (маппинг ApiModule — 503, retry клиентом).
[Fact]
public async Task HandleAsync_ConcurrentConfigWrite_LosesCompare()
{
    // Arrange — FakeEtcd: config strict=true, шард без ограничений; хук пишет
    // конкурирующий config ДО compare хендлерского txn (mod_revision растёт).
    var etcd = new Fakes.FakeEtcd();
    etcd.Seed("/clusters/shop/config", """{"buckets":2,"dbname":"shop","created_unix":1755900000}""");
    etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
    etcd.OnTxnBeforeCompare = _ =>
        etcd.PutAsync("http://fake", "/clusters/shop/config",
            """{"buckets":2,"dbname":"shop","created_unix":1755900000,"synchronous_mode_strict":true}""",
            null, CancellationToken.None).GetAwaiter().GetResult();
    var handler = new UpdateClusterConfigHandler(etcd, ["http://fake"]);

    // Act
    var result = await handler.HandleAsync("shop",
        new UpdateClusterConfigRequest(SynchronousModeStrict: false), CancellationToken.None);

    // Assert — txn проигран: ошибка гонки, значение НЕ перезаписано хендлером.
    result.IsSuccess.Should().BeFalse();
    result.Error.Should().BeOfType<ClusterConcurrentWriteException>();
    etcd.Store["/clusters/shop/config"].Value
        .Should().Contain("\"synchronous_mode_strict\":true", "конкурентная запись победила");
}
```

(механику `Fakes.FakeEtcd.Seed`/`Store` — по соседним кейсам
NodeSupervisorTests: сид пишет в `Store` напрямую; при отсутствии хелпера
`Seed(string,string)` в FakeEtcd — писать `etcd.Store[key] = ...` по образцу
соседних тестов; при необходимости хук инжекции конкурентной записи можно
упростить до прямой правки `Store` с инкрементом ModRevision — главное:
compare хендлера видит изменившийся revision).

- [ ] **Шаг 4: Маппинг эндпоинта** (`ApiModule.cs`, рядом с DELETE кластера):

```csharp
// PUT /api/clusters/{cluster}/config — мутация synchronous_mode_strict (t06,
// 02 §9.10): 204; 400 валидация/битое тело; 404; 409 не-Active; 503 etcd/гонка.
endpoints.MapPut("/api/clusters/{cluster}/config", async (
    string cluster, UpdateClusterConfigRequest request, UpdateClusterConfigHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleAsync(cluster, request, ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return result.Error switch
    {
        UpdateClusterConfigValidationException validation => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Validation failed",
            detail: result.Error.Message,
            extensions: new Dictionary<string, object?>
            {
                ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
            }),
        ClusterNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
            detail: result.Error.Message),
        ClusterNotActiveException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
            detail: result.Error.Message),
        ClusterConcurrentWriteException => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Concurrent write",
            detail: result.Error.Message),
        EtcdWriteUnavailableException or InvalidClusterConfigException => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd unavailable",
            detail: result.Error.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
            detail: result.Error!.Message),
    };
});
```

И DI: проверить регистрацию хендлеров (если `UpdateClusterConfigHandler`
регается автоматически сборкой — по образцу `CreateClusterHandler` в
`ModuleExtensions`/`Program` воркера; добавить строку по образцу).

- [ ] **Шаг 5: Сборка зелёная + юниты (вкл. кейс гонки):**

```bash
dotnet build src/PgWorker.slnx -c Release
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~UpdateClusterConfigHandler"
```

- [ ] **Шаг 6: Коммит**

```bash
git add src/PgWorker.App/Api/Operations/UpdateClusterConfigHandler.cs src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs src/PgWorker.App/Api/ApiModule.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs src/tests/PgWorker.UnitTests/Api/UpdateClusterConfigHandlerTests.cs
git commit -m "feat(pgworker): PUT /api/clusters/{c}/config — RMW-мутация synchronous_mode_strict + юнит гонки compare (t06)"
```

**Выход:** мутация strict живого кластера доступна через API воркера; проигрыш
compare покрыт детерминированным тестом. **Закрывает:** спека §3.3 (API воркера
+ протокол), §6.5 (503-ветка гонки — юнит-форма, см. Задачу 15 шаг 2).

---

### Задача 9: Воркер — AddShard: strict-гвард

**Файлы:**
- Modify: `src/PgWorker.App/Api/Operations/AddShardHandler.cs` (гвард после чтения config + хелпер)
- Test: `src/tests/PgWorker.UnitTests/Api/AddShardStrictTests.cs` (новый — юнит хелпера)
- Test: `src/tests/PgWorker.IntegrationTests/Api/ShardsApiTests.cs` (кейс 400; детально — Задача 15)

**Интерфейсы:**
- Производит: `AddShardHandler.ReadStrictField` (internal static) — семантика
  «отсутствие/не-bool = true» тернарной формой; 400 `AddShardValidationException`
  с ошибкой поля `replicas` при strict-кластере и `replicas < 2`.

**Вход:** Задача 8 закоммичена.

- [ ] **Шаг 1: Падающий юнит хелпера** (новый файл
  `src/tests/PgWorker.UnitTests/Api/AddShardStrictTests.cs`;
  `InternalsVisibleTo("PgWorker.UnitTests")` у PgWorker.App уже есть):

```csharp
using PgWorker.App.Api.Operations;

namespace PgWorker.UnitTests.Api;

// t06 (spec §3.6/§6.6, юнит-часть AC): чтение strict из config-JSON —
// отсутствие/не-bool = true (легаси-кластеры strict по умолчанию, решение №3).
public class AddShardStrictTests
{
    // AAA: семантика поля — тернарная форма, свёртка && запрещена (даёт false).
    [Theory]
    [InlineData("""{"buckets":4,"dbname":"shop"}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":"yes"}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":true}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":false}""", false)]
    public void ReadStrictField_AbsentOrNonBool_IsTrue(string raw, bool expected)
    {
        // Arrange / Act
        var strict = AddShardHandler.ReadStrictField(raw);

        // Assert
        strict.Should().Be(expected);
    }
}
```

- [ ] **Шаг 2: Прогнать — упадёт** (нет `ReadStrictField`):

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~AddShardStrict"
```

- [ ] **Шаг 3: Реализация.** В `AddShardHandler.HandleAsync` после гварда
  `rawState is not null → 409` (строки ≈55-56) добавить:

```csharp
// t06 (arch/14 §5 G): strict-кластер требует replicas ≥ 2 и на новом шарде —
// однорепликный шард в strict-кластере навсегда блокирует запись.
var clusterStrict = ReadStrictField(config.Value);
if (clusterStrict && request.Replicas < 2)
    return Result<ShardAddedDto>.Failed(new AddShardValidationException(
        [new("replicas", "strict-кластер: replicas ≥ 2 на каждом шарде (без sync-standby запись блокируется); выключите strict (PUT config) для однорепликных шардов")]));
```

и приватный→internal-хелпер рядом с `ReadBucketsField` (тернарная форма —
свёртка `&& GetBoolean()` даёт false при отсутствии/не-bool и НЕ допускается):

```csharp
// strict из config-JSON; отсутствие/не-bool = true (t06, arch/14 §3).
internal static bool ReadStrictField(string raw)
{
    using var doc = JsonDocument.Parse(raw);
    return doc.RootElement.TryGetProperty("synchronous_mode_strict", out var strict)
        && strict.ValueKind is JsonValueKind.True or JsonValueKind.False
            ? strict.GetBoolean()
            : true;
}
```

(битый JSON уже отсечён выше `JsonException → InvalidClusterConfigException`).

- [ ] **Шаг 4: Сборка + существующие тесты:**

```bash
dotnet build src/PgWorker.slnx -c Release
dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~ShardsApiTests"
```

Существующие кейсы add-shard через `ApiTestSeed.SeedActiveClusterAsync`
сеют config БЕЗ поля → strict=true → кейсы с POST `replicas=1`
(`AddShard_ActiveCluster_201ShardMaxPlusOne`, ShardsApiTests ≈20-35; прочие
используют гварды NOT_INITIALIZED/orphan — не задеты) честно упадут на 400.
Обновление (смысл кейса — механика max+1, не strict): расширить
`ApiTestSeed.SeedActiveClusterAsync` (`src/tests/PgWorker.IntegrationTests/Api/ApiTestSeed.cs`)
параметром `bool syncStrict = true`, пишущим `"synchronous_mode_strict":<v>` в
config; кейс 201 вызывает с `syncStrict: false`; остальные вызовы не меняются
(дефолт true, Contain-ассерты соседних тестов от поля не зависят).

- [ ] **Шаг 5: Коммит**

```bash
git add src/PgWorker.App/Api/Operations/AddShardHandler.cs src/tests/PgWorker.IntegrationTests/Api/ApiTestSeed.cs src/tests/PgWorker.IntegrationTests/Api/ShardsApiTests.cs src/tests/PgWorker.UnitTests/Api/AddShardStrictTests.cs
git commit -m "feat(pgworker): add-shard strict-гвард replicas≥2 (отсутствие поля = strict) + юнит (t06)"
```

**Выход:** строгий кластер (вкл. легаси без поля) не может получить
однорепликный шард. **Закрывает:** спека §3.6, §6.6 (юнит + интеграция).

---

### Задача 10: Панель — модель `ClusterInfo.SynchronousModeStrict` + парсер

**Файлы:**
- Modify: `src/AdminPanel.Core/ClusterInfo.cs:4-16` (ClusterInfo)
- Modify: `src/AdminPanel.Etcd/Parsing/ClustersParser.cs:161-202` (BuildCluster/ParseConfig)
- Test: `src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`

**Интерфейсы:**
- Производит: `ClusterInfo.SynchronousModeStrict` (bool, дефолт `true`) —
  потребляется Задачами 11, 12, 14.

**Вход:** arch/ канон (Задача 2) закоммичен.

- [ ] **Шаг 1: Падающий тест** (в `ClustersParserTests.cs`, по механике
  соседних кейсов):

```csharp
// AAA (t06, spec §6.1/§6.9): config-поле читается в снапшот панели;
// отсутствие/не-bool/битый JSON = true (единая семантика с воркером).
[Theory]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":false}""", false)]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":true}""", true)]
[InlineData("""{"buckets":2,"dbname":"shop"}""", true)]
[InlineData("""{"buckets":2,"dbname":"shop","synchronous_mode_strict":"yes"}""", true)]
public void Parse_StrictField_SemanticsMatchesWorker(string configRaw, bool expected)
{
    // Arrange — kvs с config-ключом (хелпер файла).
    // Act
    var cluster = ParseSingleCluster(configRaw);
    // Assert
    cluster.SynchronousModeStrict.Should().Be(expected);
}
```

- [ ] **Шаг 2: Прогнать — упадёт** (нет поля):

```bash
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter "FullyQualifiedName~ClustersParserTests"
```

- [ ] **Шаг 3: Реализация.** `ClusterInfo` — последний параметр с дефолтом:

```csharp
public sealed record ClusterInfo(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    ClusterState State,
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,
    IReadOnlyList<HealRecord> Heals,
    bool SynchronousModeStrict = true)
```

`ClustersParser.ParseConfig` — кортеж расширяется `bool SyncStrict`; чтение —
СТРОГО тернарной формой (свёртка `&& GetBoolean()` даёт false при
отсутствии/не-bool — противоречит тесту шага 1 и спеке §3.1):

```csharp
private static (string? DbName, int BucketsCount, long? CreatedUnix, ClusterState State, bool SyncStrict) ParseConfig(
    string cluster, string? raw, List<KeyParseError> errors)
{
    if (raw is null)
        return (null, 0, null, ClusterState.Active, true); // incomplete, не ошибка
    try
    {
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        ...
        // t06: отсутствие/не-bool = true (arch/02 §2.1) — тернарник, не &&-свёртка.
        var syncStrict = root.TryGetProperty("synchronous_mode_strict", out var strict)
            && strict.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? strict.GetBoolean()
                : true;
        return (..., syncStrict);
    }
    catch (JsonException)
    {
        errors.Add(...);
        return (null, 0, null, ClusterState.Active, true);
    }
}
```

`BuildCluster`: `new ClusterInfo(acc.Name, dbName, bucketsCount, createdUnix,
state, shards, buckets, acc.Heals, syncStrict)`.

- [ ] **Шаг 4: Прогнать — зелёно** (команда шага 2 + весь юнит-проект панели).

- [ ] **Шаг 5: Коммит**

```bash
git add src/AdminPanel.Core/ClusterInfo.cs src/AdminPanel.Etcd/Parsing/ClustersParser.cs src/tests/AdminPanel.UnitTests/ClustersParserTests.cs
git commit -m "feat(adminpanel): ClusterInfo.SynchronousModeStrict + парсер config (t06)"
```

**Выход:** снапшот панели несёт strict. **Закрывает:** спека §3.1 (панельный парсер), §6.1.

---

### Задача 11: Панель — алерт `sync-standby-missing`: severity от strict

**Файлы:**
- Modify: `src/AdminPanel.Core/Alerting/Rules/SyncStandbyMissingRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` (или
  `ShardingAlertRulesTests.cs` — где живут кейсы этого правила; проверить grep'ом
  `sync-standby-missing` в тестах)

**Интерфейсы:**
- Производит: алерт strict-кластера — `AlertSeverity.Critical`, не-strict —
  `Warning` (как сейчас); Message/Hint/Details без изменений; RemedyText для
  strict дополнен.

**Вход:** Задача 10 закоммичена.

- [ ] **Шаг 1: Падающий тест** (в файл кейсов правила):

```csharp
// AAA (t06, spec §3.5/§6.9): strict-кластер без sync-standby — critical
// (запись блокирована); не-strict — warning (как раньше).
[Fact]
public void SyncStandbyMissing_SeverityDependsOnStrict()
{
    // Arrange — снапшот: кластер с мастером без sync-standby (фикстура файла);
    // вариант strict и вариант strict=false.
    var strictSnapshot = SnapshotWithCluster(synchronousModeStrict: true);
    var looseSnapshot = SnapshotWithCluster(synchronousModeStrict: false);

    // Act
    var strictAlerts = new SyncStandbyMissingRule().Evaluate(strictSnapshot, Context());
    var looseAlerts = new SyncStandbyMissingRule().Evaluate(looseSnapshot, Context());

    // Assert
    strictAlerts.Single().Severity.Should().Be(AlertSeverity.Critical);
    looseAlerts.Single().Severity.Should().Be(AlertSeverity.Warning);
    strictAlerts.Single().RemedyText.Should().Contain("выключите strict");
}
```

(`SnapshotWithCluster` — по механике существующих кейсов правила: мастер с
`IsInRecovery=false`, `Error=null`, standbies без sync/quorum.)

- [ ] **Шаг 2: Прогнать — упадёт** (severity всегда Warning):

```bash
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter "FullyQualifiedName~SyncStandbyMissing"
```

- [ ] **Шаг 3: Реализация** (`SyncStandbyMissingRule.Evaluate`):

```csharp
foreach (var shard in cluster.Shards)
{
    ... // существующие фильтры без изменений
    if (runtime.Standbies.Any(s => s.SyncState is "sync" or "quorum"))
        continue;

    // t06: strict-кластер без sync-standby БЛОКИРУЕТ запись — critical
    // (инцидент); не-strict — прежний warning (доступность переездов).
    var strict = cluster.SynchronousModeStrict;
    yield return new Alert(
        $"{KindName}:{cluster.Name}/{shard.Name}",
        strict ? AlertSeverity.Critical : AlertSeverity.Warning,
        KindName,
        $"{cluster.Name}/{shard.Name}",
        $"у мастера шарда {cluster.Name}/{shard.Name} нет sync-standby (sync_state sync/quorum) — предусловие переездов не выполнено (P8)",
        new Dictionary<string, string> { ["standbiesTotal"] = runtime.Standbies.Count.ToString() },
        null,
        "у мастера нет синхронного standby (sync/quorum): синхронная репликация — предусловие бесшовных переездов (cutover требует sync-подтверждения); мастер обязан держать sync-standby",
        AlertRemedy.WorkerAuto,
        strict
            ? "запись блокирована (strict) — восстановите реплику (rebuild воркера) или выключите strict (PUT config)"
            : "надзор воркера восстановит реплику (rebuild); висит — проверьте /service/<scope>/members и recreate отстающей ноды");
}
```

- [ ] **Шаг 4: Прогнать — зелёно** (весь юнит-проект панели; кейсы
  `AlertHintRemedyTests` не должны пострадать — Hint/Remedy непустые).

- [ ] **Шаг 5: Коммит**

```bash
git add src/AdminPanel.Core/Alerting/Rules/SyncStandbyMissingRule.cs src/tests/AdminPanel.UnitTests/
git commit -m "feat(adminpanel): sync-standby-missing — critical у strict-кластеров (t06)"
```

**Выход:** эскалация алерта. **Закрывает:** спека §3.5 (алерт), §6.9, риск §7 п.1.

---

### Задача 12: Панель — эндпоинт-прокси `PUT /api/clusters/{cluster}/config` + DTO + тело создания

**Файлы:**
- Create: `src/AdminPanel.Api/Operations/UpdateClusterConfigCommand.cs`
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs` (MapPut)
- Modify: `src/AdminPanel.Api/Operations/CreateClusterCommand.cs:11-19` (панельная копия `CreateClusterRequest` — без неё поле теряется на прокси-хопе)
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs` (ClusterDto + маппер)
- Modify: `src/AdminPanel.Api/Inspection/ClustersQuery.cs` (ClusterSummaryDto + маппер)
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs` (поле в DTO)

**Интерфейсы:**
- Потребляет: `WorkerProxy.SendAsync`, `IWorkerApiGateway` (механика прокси).
- Производит: панельный `PUT /api/clusters/{cluster}/config` (204; коды
  воркера проксируются как есть); `ClusterDto.SynchronousModeStrict`,
  `ClusterSummaryDto.SynchronousModeStrict` (camelCase в JSON);
  сквозное прохождение `synchronousModeStrict` через панель в создании.

**Вход:** Задача 8 (эндпоинт воркера) и 10 закоммичены.

- [ ] **Шаг 1: Команда-прокси** (`UpdateClusterConfigCommand.cs`; образец —
  `RotateClusterSecretsCommand`/`DeleteClusterCommand`):

```csharp
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Тело PUT /api/clusters/{cluster}/config (t06, 02 §9.10): уходит в API
// PgWorker как есть; панель не валидирует — источник истины воркер.
public sealed record UpdateClusterConfigRequest(bool? SynchronousModeStrict);

public sealed record UpdateClusterConfigCommand(string Cluster, UpdateClusterConfigRequest Request)
    : ICommand<ClusterConfigUpdatedDto>;

// Панель отвечает 204 — DTO-заглушка (паттерн 204-мутаций DELETE; тело пустое).
public sealed record ClusterConfigUpdatedDto(string Cluster);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// применение к Patroni — конвергенция DCS воркера («применит воркер», 02 §9.10).
[InjectAsScoped]
public sealed class UpdateClusterConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateClusterConfigCommand, ClusterConfigUpdatedDto>
{
    public async ValueTask<Result<ClusterConfigUpdatedDto>> Handle(
        UpdateClusterConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ClusterConfigUpdatedDto>(
            api, "pgworker", HttpMethod.Put, $"/api/clusters/{command.Cluster}/config",
            command.Request, requestedBy: null, ct);
}
```

(`SynchronousModeStrict` → camelCase `synchronousModeStrict` автоматически —
`WorkerProxy.Json` = `JsonSerializerDefaults.Web`; атрибут не нужен.)

- [ ] **Шаг 2: Маппинг эндпоинта** (`OperationsModule.cs`, после DELETE
  кластера):

```csharp
// PUT /api/clusters/{cluster}/config — мутация synchronous_mode_strict (t06,
// 02 §9.10): 204; ошибки воркера — ProblemDetails как есть.
endpoints.MapPut("/api/clusters/{cluster}/config", async (
    string cluster, UpdateClusterConfigRequest request, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<UpdateClusterConfigCommand, ClusterConfigUpdatedDto>(
        new UpdateClusterConfigCommand(cluster, request), ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return Error(result);
});
```

- [ ] **Шаг 3: Тело создания — панельная копия рекорда**
  (`CreateClusterCommand.cs:11-19`). Панель биндит СВОЮ копию
  `CreateClusterRequest` и ре-сериализует её в воркер (`WorkerProxy`);
  System.Text.Json отбрасывает неизвестные JSON-поля — без поля в панельной
  копии `synchronousModeStrict` из UI не дошёл бы до воркера:

```csharp
// Тело POST /api/clusters (arch/03 §1.1): биндится Minimal API как JSON и
// уходит в API PgWorker как есть (панель не валидирует — источник истины
// воркер, spec §3.4). Sharded: отсутствует/null = true — совместимость
// старых клиентов (arch/02 §9.3; нормализует воркер). SynchronousModeStrict:
// camelCase-биндинг/сериализация Web даёт wire-имя synchronousModeStrict
// 1:1 с воркером (t06, arch/03 §1.1).
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk,
    bool? Sharded = null,
    bool? SynchronousModeStrict = null);
```

(атрибут не нужен: camelCase от `SynchronousModeStrict` — ровно
`synchronousModeStrict`; воркерский рекорд с коротким `SyncStrict` несёт
`JsonPropertyName` — Задача 5.)

- [ ] **Шаг 4: DTO снапшота.** `ClusterDto` — добавить `bool
  SynchronousModeStrict` (после `Sharded`); `ClusterDetailsMapper.Map` —
  передать `cluster.SynchronousModeStrict`. `ClusterSummaryDto` — добавить
  `bool SynchronousModeStrict` (после `Sharded`); `ClustersMapper.Map` —
  передать `c.SynchronousModeStrict`.

- [ ] **Шаг 5: Юнит мапперов** (`ClustersMappersTests.cs` / маппер-тесты
  деталей — по соседним кейсам):

```csharp
// AAA (t06): поле strict проходит в сводку и детали кластера.
[Fact]
public void Map_ClusterCarriesSynchronousModeStrict()
{
    // Arrange — ClusterInfo со strict=false (конструктор файла-фикстуры).
    // Act — ClustersMapper.Map / ClusterDetailsMapper.Map.
    // Assert — DTO.SynchronousModeStrict == false (сводка и детали).
}
```

- [ ] **Шаг 6: Прогнать:**

```bash
dotnet build src/PgWorker.slnx -c Release
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release
```

- [ ] **Шаг 7: Коммит**

```bash
git add src/AdminPanel.Api/Operations/ src/AdminPanel.Api/Inspection/ src/tests/AdminPanel.UnitTests/
git commit -m "feat(adminpanel): PUT config прокси + synchronousModeStrict в DTO и теле создания (t06)"
```

**Выход:** панельный API зеркалит воркер (вкл. сквозное поле создания); фронт
получает поле. **Закрывает:** спека §3.3 (панель), §3.5 (снапшот/DTO).

---

### Задача 13: Панель — интеграционный тест эндпоинта

**Файлы:**
- Test: `src/tests/AdminPanel.IntegrationTests/UpdateClusterConfigApiTests.cs` (новый)

**Вход:** Задача 12 закоммичена; фабрика — по образцу
`RotateClusterSecretsApiTests.cs` (его фикстура мока/поднимает API-гейтвей
воркера — механика 1:1).

- [ ] **Шаг 1: Тест** (по механике `RotateClusterSecretsApiTests` — WAF панели
  + фейк/реальный API-гейтвей воркера; если там фейк воркера — отвечать
  204/400/404/409/503 и ассертить проксирование статуса и тела запроса):

```csharp
// AAA (t06, 02 §9.10): панельный PUT — 204 при успехе; проксирует коды
// воркера (400 валидация / 404 / 409 / 503) без изменений.
[Fact]
public async Task PutClusterConfig_ProxiesWorkerCodes()
{
    // Arrange — WAF панели, кластер "shop" в снапшоте (EtcdSeed), API-гейтвей
    // по образцу соседних Api-тестов.
    // Act — PUT /api/clusters/shop/config {"synchronousModeStrict": false}.
    // Assert — 204; в фейк-гейтвее зафиксирован путь
    // "/api/clusters/shop/config", метод PUT, тело с полем synchronousModeStrict.
}
```

Конкретику фикстуры взять из `RotateClusterSecretsApiTests.cs` (тот же
паттерн мутации-прокси; при Real-etcd варианте — проверка фактической записи
через воркера НЕ делается: это панельный прокси-тест, воркер покрыт Задачей 15).

- [ ] **Шаг 2: Прогнать:**

```bash
dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~UpdateClusterConfig"
```
После серии — зачистка docker-контейнеров/сетей фикстур (если поднимались:
`EtcdContainerFixture`), по правилам AGENTS.md.

- [ ] **Шаг 3: Коммит**

```bash
git add src/tests/AdminPanel.IntegrationTests/UpdateClusterConfigApiTests.cs
git commit -m "test(adminpanel): интеграция PUT config прокси (t06)"
```

**Выход:** панельный эндпоинт покрыт. **Закрывает:** спека §4 фаза 2 (интеграция эндпоинта).

---

### Задача 14: Фронтенд — формы и переключатель

**Файлы:**
- Modify: `frontend/src/api/dto.ts` (CreateClusterRequestDto, ClusterDto/кластерная сводка — добавить `synchronousModeStrict`)
- Modify: `frontend/src/api/queries.ts` (+ `updateClusterConfig`)
- Modify: `frontend/src/pages/clusters/ClusterCreateModal.tsx`
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx` (+ новый компонент
  `frontend/src/pages/cluster-details/StrictModeSwitch.tsx`)
- Modify: `frontend/src/pages/cluster-details/AddShardModal.tsx`
- Modify: `frontend/src/pages/ClustersPage.tsx` (отображение режима в списке)

**Вход:** Задача 12 закоммичена (API-поле доступно).

- [ ] **Шаг 1: DTO и запрос.** `dto.ts`: в `CreateClusterRequestDto` добавить
  `synchronousModeStrict?: boolean;`; в DTO кластера (сводка и детали) —
  `synchronousModeStrict: boolean;`. `queries.ts` — единственный HTTP-хелпер
  фронта `apiFetch` (`frontend/src/api/client.ts:46`): тело передаётся
  ОБЪЕКТОМ (хелпер сам делает `JSON.stringify` и ставит Content-Type; 204 →
  `undefined`). Мутация (по образцу соседних в файле):

```typescript
// t06: мутация strict-режима кластера (02 §9.10; 204, ошибки — ProblemDetails).
export async function updateClusterConfig(
  cluster: string,
  body: { synchronousModeStrict: boolean },
): Promise<void> {
  await apiFetch<void>(`/api/clusters/${cluster}/config`, {
    method: 'PUT',
    body,
  });
}
```

- [ ] **Шаг 2: Форма создания** (`ClusterCreateModal.tsx`): в `FormState` —
  `syncStrict: boolean`; `EMPTY` — `syncStrict: true`; контрол (Mantine
  `Switch`/`Checkbox` после поля «Реплики»):

```tsx
<Switch
  label="Синхронный strict-режим (блокировать запись при потере sync-реплики)"
  description="strict: без синхронной реплики запись блокируется (нет потерь); требует реплик ≥ 2"
  checked={form.syncStrict}
  onChange={(e) => set('syncStrict', e.currentTarget.checked)}
/>
```

клиентская валидация в `validate()` (зеркало серверной):

```typescript
if (form.syncStrict && form.replicas < 2)
  errors.replicas = 'strict-режим требует реплик ≥ 2 (выключите strict для 1)';
```

тело запроса всегда несёт `synchronousModeStrict: form.syncStrict` (обе ветки
sharded/single).

- [ ] **Шаг 3: Cluster details** — новый `StrictModeSwitch.tsx` (Switch в шапке
  страницы рядом с `RotateClusterSecretsButton`; разблокирован только при
  `state === 'ACTIVE'`, иначе disabled с подсказкой «после инициализации»);
  подтверждение — `Modal` с предупреждением (включение: «запись будет
  блокироваться при потере sync-реплики (durability)»; выключение: «при
  failover возможна потеря неподтверждённых транзакций (availability)») и
  кнопками «Отмена»/«Переключить»; мутация `updateClusterConfig` c
  последующей инвалидацией `queryKeys.cluster(name)`; бейдж в шапке:

```tsx
{data.synchronousModeStrict
  ? <Badge color="red" variant="light">strict</Badge>
  : <Badge color="teal" variant="light">availability</Badge>}
```

- [ ] **Шаг 4: Add-shard форма** (`AddShardModal.tsx`): компонент принимает
  `strict: boolean` (props из `ClusterDetailsPage`: `data.synchronousModeStrict`);
  при strict: `min={2}` у NumberInput «Реплики» и валидация
  `if (strict && form.replicas < 2) errors.replicas = 'strict-кластер: минимум 2 реплики';`
  + подпись «кластер в strict-режиме».

- [ ] **Шаг 5: Список кластеров** (`ClustersPage.tsx`): колонка/бейдж режима —
  компактный бейдж `strict`/`avail.` по `synchronousModeStrict` (по стилю
  соседних колонок таблицы).

- [ ] **Шаг 6: Проверка:**

```bash
cd frontend && npm run typecheck && cd ..
```

- [ ] **Шаг 7: Коммит**

```bash
git add frontend/src/
git commit -m "feat(ui): strict-режим — форма создания, переключатель Cluster details, минимум реплик add-shard (t06)"
```

**Выход:** полный UI панели. **Закрывает:** спека §3.5 (UI), решения пользователя №5.
Ручная проверка на dev-стенде (docker) — в мерж-гейте (Задача 17).

---

### Задача 15: Интеграционные тесты API воркера (реальный etcd)

**Файлы:**
- Create: `src/tests/PgWorker.IntegrationTests/Api/UpdateClusterConfigApiTests.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/Api/CreateClusterApiTests.cs` (кейсы strict создания)
- Modify: `src/tests/PgWorker.IntegrationTests/Api/ShardsApiTests.cs` (кейсы add-shard 400, вкл. легаси без поля)
- Modify: `src/tests/PgWorker.IntegrationTests/Api/SeedApiTests.cs` (поле в config сида)

**Вход:** Задачи 5, 8, 9 закоммичены; коллекция `PgApiCollection` (WAF +
реальный etcd) существует.

- [ ] **Шаг 1: Тесты создания** (`CreateClusterApiTests.cs`); тела POST несут
  wire-имя `synchronousModeStrict` (сквозная проверка биндинга — критерий
  спеки §6.2):

```csharp
// AAA (t06, spec §6.2): без поля → strict-кластер; strict+replicas=1 → 400;
// strict=false+replicas=1 → 201. Wire-имя поля — synchronousModeStrict.
[Fact]
public async Task PostCluster_StrictSemantics()
{
    // Arrange / Act — три POST (camelCase-тела):
    //  1) {"name":"s1",..., "replicas":2}                      — без поля
    //  2) {"name":"s2",..., "replicas":1,"synchronousModeStrict":true}
    //  3) {"name":"s3",..., "replicas":1,"synchronousModeStrict":false}
    // Assert — 201/400/201: config в etcd несёт "synchronous_mode_strict":true
    // (кейс 1), 400-тело несёт errors.syncStrict (кейс 2), 201 +
    // "synchronous_mode_strict":false (кейс 3).
}
```

- [ ] **Шаг 2: Тесты мутации** (`UpdateClusterConfigApiTests.cs`; WAF-клиент +
  `Etcd.Gateway`; сид кластера — прямой записью ключей etcd по образцу
  соседних тестов):

```csharp
// AAA (t06, spec §6.5): PUT — 204 и поле в etcd; идемпотентен (повтор 204,
// mod_revision не растёт); включение при replicas=1 → 400; не-Active → 409;
// нет кластера → 404; битое тело (без поля) → 400.
[Fact]
public async Task PutClusterConfig_GuardsAndCodes() { /* кейсы выше, по одному Fact на группу */ }
```

**Примечание к AC §6.5 (гонка mod_revision → 503):** спека требует
«интеграционный тест с реальным etcd», однако гонка между read и txn ВНУТРИ
одного вызова хендлера на реальном etcd детерминированно невоспроизводима
(инъекция конкурентной записи потребовала бы прод-хука в хендлере). Форма
верификации этой ветки — детерминированный юнит через `Fakes.FakeEtcd`
(хендлер принимает `IEtcdGateway`, реальный код-путь read→txn→проигрыш
compare проходит целиком): Задача 8, шаг 3б, кейс
`HandleAsync_ConcurrentConfigWrite_LosesCompare` → `ClusterConcurrentWriteException`
→ 503 маппингом ApiModule (Задача 8, шаг 4). Интеграция на реальном etcd
покрывает остальное: коды 204/400/404/409, идемпотентность, фактическое
изменение поля, неизменность прочих полей config (buckets/dbname/created_unix/
state). Отступление от буквы AC (юнит вместо интеграции для единственной
ветки гонки) — осознанное, суть AC (проигрыш compare → 503) сохранена.

- [ ] **Шаг 3: Тесты add-shard** (`ShardsApiTests.cs`) — AC §6.6 «юнит +
  интеграция»: юнит — Задача 9 шаг 1 (`ReadStrictField`); интеграция здесь,
  ОБЯЗАТЕЛЬНО включая легаси-кейс (гвард обязан срабатывать для config БЕЗ
  поля — решение пользователя №3):

```csharp
// AAA (t06, spec §6.6): strict-кластер + replicas=1 → 400 errors.replicas;
// strict=false → 201. Отдельный кейс: config БЕЗ поля (легаси) + replicas=1
// → 400 — гвард работает по умолчанию, а не только при явном true.
[Fact]
public async Task AddShard_StrictCluster_RejectsSingleReplica() { /* сид: config с "synchronous_mode_strict":true → 400 */ }

[Fact]
public async Task AddShard_LegacyConfigWithoutStrictField_AlsoRejectsSingleReplica()
{ /* сид: config без поля (легаси) → POST replicas=1 → 400 errors.replicas */ }

[Fact]
public async Task AddShard_StrictOffCluster_AcceptsSingleReplica()
{ /* сид: "synchronous_mode_strict":false → POST replicas=1 → 201 */ }
```

- [ ] **Шаг 4: Сид** (`SeedApiTests.cs`): ассерт config демо-сида содержит
  `"synchronous_mode_strict":true`.

- [ ] **Шаг 5: Прогнать серию + зачистка:**

```bash
dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Api"
# после финальной строки — зачистка контейнеров/сетей фикстур (ryuk + страховка):
docker ps -a --format '{{.Names}}' | grep -E 'pgw-|etcd' || true
docker network ls --format '{{.Name}}' | grep -E 'pgw-|kfw-' || true
```

- [ ] **Шаг 6: Коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/Api/
git commit -m "test(pgworker): интеграция strict — создание (wire), PUT config, add-shard вкл. легаси (t06)"
```

**Выход:** контракт API подтверждён на реальном etcd (вкл. точное wire-имя и
легаси-семантика add-shard). **Закрывает:** спека §6.2/6.5/6.6 (гонка — юнит-форма
с примечанием шага 2), §4 фаза 3.

---

### Задача 16: Docker-E2E — строгий кейс на свежем Release

**Файлы:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eStrictScenarios.cs`

**Вход:** Задачи 4–9 закоммичены; все юниты/интеграции зелёные; docker-демон
доступен; образы зеркалированы (`dev-stand/images/pull-images.sh` при
необходимости).

- [ ] **Шаг 1: Сценарий** (по механике `E2eScaleScenarios`/
  `E2eRotateScenarios`: `E2eEnvironment.StartAsync("strict-sync")`, свой
  guid-тег, own-only teardown + ассерт чистоты уже в E2eEnvironment; сид — по
  образцу `E2eRotateScenarios.SeedClusterAsync(string cluster)`
  (`E2eRotateScenarios.cs:161`; пишет оба шарда с `replicas "2"` и нодами
  a/b) с добавлением в config-JSON `"synchronous_mode_strict":true`; подъём
  хоста `StartHostAsync`):
  1. **Bootstrap-ассерт**: после provisioning — `docker inspect` env контейнера
     `pgw-<C>-shard1-shard1a` (хелпер `ContainerEnvAsync` из E2ePgtuneScenarios)
     — `SPILO_CONFIGURATION` содержит `synchronous_mode_strict: true`.
  2. **DCS-ассерт**: GET `http://<host>:<patroniPort>/config` первой ноды
     (PatroniHttp-хелпер по образцу E2eScenarios:523; порт — из portalloc,
     хелпер `PortallocAsync`) — JSON содержит `"synchronous_mode_strict":true`.
  3. **Мутация**: PUT к API воркера — URL из дискавери-ключа `/pgworker/api/<id>`
     (ожидание появления — по образцу `E2eWorkerCertScenarios:217-264`),
     mTLS-клиент: клиентский серт от install CA —
     `E2eTestPki.Issue(E2eEnvironment.InstallCaPem, E2eEnvironment.InstallCaKeyPem,
     "e2e-strict", ["localhost", "127.0.0.1"], ip: null)` (5-й параметр `ip`
     без дефолта — обязан быть передан; `ip: null` при "127.0.0.1" в dnsNames —
     паттерн серверного серта E2eEnvironment:765-766), доверие серверу —
     install CA (серт подписан ею), TLS 1.2 (паттерн
     `E2eWorkerCertScenarios.TlsClient:268-282`). Тело
     `{"synchronousModeStrict": false}` → 204.
  4. **Конвергенция**: ожидание (`E2eFixture.WaitForAsync`, бюджет ≤ 360 с как у
     provisioning-фаз соседних сценариев) GET /config →
     `"synchronous_mode_strict":false` — без рестартов нод (ассерт: набор
     контейнеров кластера тот же, `docker inspect` StartedAt нод старее PUT;
     журнал `/pgworker/work/<C>` несёт фазу `dcs-converge` с
     `"synchronous_mode_strict":false` в diff-патче).
  5. **Валидация 400**: PUT `{"synchronousModeStrict": true}` на кластере,
     где сид создал однорепликный шард (отдельный sub-кейс: сид с shard2
     replicas=1 и strict=false → попытка включения → 400 с errors.syncStrict).

Каждый метод — отдельный `[Fact]` со своим окружением (`StartAsync` на Fact),
AAA-комментарии, полный teardown при любом исходе (механика E2eEnvironment),
телеметрия в `/tmp/pgw-e2e-artifacts-<guid>/` (уже в E2eEnvironment;
`MarkFailed()` — контейнеры стоят, не удаляются).

- [ ] **Шаг 2: Прогон E2E-кейса (свежий Release; автосборка E2eFixture):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2eStrictScenarios"
```

Упавший прогон — разбор по логам артефактов (перезапуск без анализа запрещён);
фазы >60 с — строка `[PHASE]` (уже в E2eEnvironment).

- [ ] **Шаг 3: Зачистка после серии** (обязательная, между сериями):

```bash
docker ps -a --format '{{.Names}}' | grep 'pgw-' | xargs -r docker rm -f
docker network ls --format '{{.Name}}' | grep -E 'pgw-|kfw-net' | xargs -r docker network rm || docker network prune -f
docker volume ls --format '{{.Name}}' | grep 'pgw-' | xargs -r docker volume rm || true
```

- [ ] **Шаг 4: Коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eStrictScenarios.cs
git commit -m "test(e2e): strict-режим — bootstrap, мутация PUT config, конвергенция, валидация 400 (t06)"
```

**Выход:** E2E-доказательство применения strict. **Закрывает:** спека §6.7, §5 п.7 (E2E-каноны).

---

### Задача 17: Полный мерж-гейт — прогоны

**Вход:** Задачи 1–16 закоммичены.

- [ ] **Шаг 1: Полная сборка решения (Release, warnings-as-errors):**

```bash
dotnet build src/PgWorker.slnx -c Release
```

- [ ] **Шаг 2: Юниты всех проектов (серия):**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release
```
После серии — зачистка docker (страховка, команда Задачи 16 шаг 3).

- [ ] **Шаг 3: Интеграции (серия; между проектами — зачистка):**

```bash
dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Api|FullyQualifiedName~Etcd"
docker ps -a --format '{{.Names}}' | grep 'pgw-' | xargs -r docker rm -f; docker network prune -f
dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release
docker ps -a --format '{{.Names}}' | grep -E 'pgw-|etcd' | xargs -r docker rm -f; docker network prune -f
```

- [ ] **Шаг 4: E2E — кейс-маркер мерж-гейта + новый кейс + полный прогон серии
  PgWorker E2E (задача трогает код воркеров — правило AGENTS.md):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Scale_AddEmptyShard"
# зачистка (Задача 16 шаг 3)
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2eStrictScenarios"
# зачистка
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2e"
# финальная зачистка + проверка чистоты:
docker ps -a --format '{{.Names}}' | grep -c 'pgw-'   # 0
docker network ls --format '{{.Name}}' | grep -cE 'pgw-|kfw-net'  # 0
```

- [ ] **Шаг 5: Фронтенд:**

```bash
cd frontend && npm run typecheck && cd ..
```

- [ ] **Шаг 6: Ручная проверка UI на dev-стенде** (панель всегда в докере;
  подъём — `dev-stand/adminpanel/checks/00-up.sh`; галочка создания, бейдж и
  переключатель Cluster details, критический алерт у strict-кластера без
  sync-standby; после проверки — зачистка стенда по его чекам). Шаг
  выполняется при участи пользователя.

**Выход:** все прогоны зелёные; чистота docker подтверждена. **Закрывает:** спека §6.8/6.9, правило AGENTS.md (E2E в мерж-гейте).

---

### Задача 18: Roadmap-гейт (тем же мерж-коммитом)

**Файлы:**
- Modify: `arch/roadmap/reliability.md:50-55` (удалить пункт `t06-sync-strict-option` и его упоминания в `←`-зависимостях других пунктов, если есть)
- Modify: `arch/roadmap/reliability-report.md:93` (строка реестра «Осталось» — перенести/обновить по правилам реестра)

**Вход:** Задача 17 завершена; мерж в `main` одобрен пользователем (без
одобрения — задачу не исполнять; эти правки входят в мерж-коммит).

- [ ] **Шаг 1:** удалить пункт тега из `reliability.md` (строки 50-55,
  блок `- **`t06-sync-strict-option`** — ...`); grep-проверка:

```bash
grep -rn "t06-sync-strict-option" arch/roadmap/ | grep -v reliability-report || echo "чисто"
```

- [ ] **Шаг 2:** обновить строку 93 `reliability-report.md` (строка реестра
  `| t06-sync-strict-option | per-cluster synchronous_mode_strict | P1 | D |`)
  — по формату реестра: перенос из «Осталось» в исполненные тем же мерж-коммитом
  (формат строк исполненных — по соседним исполненным трекам этого файла).

- [ ] **Шаг 3:** итоговый коммит ветки + мерж в `main` (только по явной команде
  пользователя; мерж-коммит несёт и roadmap-правки — спека §4 фаза 4, §6.10).

**Выход:** тег снят, реестр обновлён. **Закрывает:** спека §6.10.

---

## Самопроверка плана (выполнена; правки ревью Фазы 4 раундов 1–2 учтены)

1. **Покрытие спеки:** §5.1 arch-таблица → Задачи 1–3; §3.1 → 4, 10; §3.2 → 5;
   §3.3 → 8, 12; §3.4 → 6, 7; §3.5 → 11, 14; §3.6 → 9; §6.1 → 4, 10; §6.2 → 5,
   15; §6.3 → 6; §6.4 → 7; §6.5 → 8 (шаг 3б — гонка юнит-формой с примечанием
   в Задаче 15 шаг 2), 15; §6.6 → 9 (юнит) + 15 (интеграция, вкл. легаси);
   §6.7 → 16; §6.8 → 17; §6.9 → 11, 14, 17; §6.10 → 18. Решения пользователя
   №1–6 покрыты (№1 — Задача 5; №2 — 8/16; №3 — 4/9/10 + легаси-кейс 15;
   №4 — 11; №5 — 14; №6 — 5/9/15).
2. **Placeholder-скан:** шаги несут конкретный код/команды; места, где механика
   берётся «по образцу соседнего файла», привязаны к точным файлам и строкам.
3. **Консистентность типов и wire-имён:** `ClusterConfig.SyncStrict` (воркер)
   vs `ClusterInfo.SynchronousModeStrict` (панель) — разные модели проектов,
   каждая внутренне консистентна; etcd/YAML — `synchronous_mode_strict`;
   REST/DTO — `synchronousModeStrict` везде: воркерский рекорд создания —
   `JsonPropertyName` (camelCase от `SyncStrict` не совпадает), панельные
   рекорды и `UpdateClusterConfigRequest` — автоматический camelCase;
   фронт работает через `apiFetch` (body-объект, без ручной сериализации).
   Сигнатуры `SpiloEnvBuilder.Build`/`EnsureNodeAsync`/`Analyze` едины во всех
   задачах; список всех реализаций `IClusterDriver` (прод: Plain/Swarm/
   FakeDriver; тесты: BackupProcessTests, BackupSelfHealTests,
   BackupVerifyProcessTests, RestoreProcessTests, StubScaleDriver) перечислен в
   Задаче 6; прод-вызовы `EnsureNodeAsync` покрыты все, включая
   `RestoreProcess` (восстановленная нода + реплики).
4. **Семантика чтения и типы:** чтение поля везде тернарной формой
   `TryGet && isBool ? GetBoolean() : true` (Задачи 4, 8, 9, 10) — свёртка
   `&& GetBoolean()` инвертирует «отсутствие/не-bool = true» и запрещена;
   юнит-кейсы на семантику есть у воркерского парсера (4), add-shard-хелпера
   (9), панельного парсера (10); легаси-кейс add-shard (config без поля) —
   Задача 15 шаг 3. `Kv.ModRevision` (ulong) кастуется в `(long)` для
   `ModRevisionEqual`; `E2eTestPki.Issue` вызывается с 5-м аргументом
   `ip: null`.
