# EtcdNode: ретенция 3 в S3 + пауза внеочередных 5 с + лидерные инварианты — план реализации

> **Для агентов-исполнителей:** шаги (Шаг N) исполняет СИЛЬНАЯ модель (субагент coder с контекстом проекта); подпункты (НП N.m) — самодостаточные задания, которые сильная модель может делегировать СЛАБОЙ модели (субагент coder-flash БЕЗ контекста проекта: получает только текст подпункта). Всё, что не выделено в подпункты, делает сильная модель. Шаги отмечаются чекбоксами.

**Цель:** протянуть пару канонических значений DR-слепков etcd (RetentionObjects 28→3, MinIntervalSec 300→5) по всей цепочке канон→код→конфиги→тесты и закрыть интеграционными ассертами инварианты: снимает только лидер, смена лидера → свежий бэкап, бэкапы полные/самодостаточные (плюс сопутствующая переработка счётного ассерта `ImportantChange` под ретенцию 3).

**Архитектура:** изменения точечные; механика (расписание, дифф, ретенция, экспортёр, AgentLoop) НЕ трогается. Конфигурация течёт appsettings воркера → EtcdProvisioningOptions → env узла → дефолты EtcdNodeEnv; все звенья меняются согласованно. Для двух ассертов добавляется минимальная тестовая инфраструктура (байтовое чтение S3-объекта, exec в контейнер).

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit, testcontainers, docker.

**Spec:** `docs/superpowers/2026-11-07-etcdnode-snapshot-tuning/spec.md` (в worktree; план аргументируется от spec — исполнитель читает оба).

**Рабочий каталог всех команд и путей:** `/Users/demakaev/ZCodeProject/worktrees/feat-etcdnode-snapshot-tuning` (далее — корень worktree). Ветка `feat-etcdnode-snapshot-tuning`.

## Глобальные ограничения (из spec §4; действуют на каждый шаг)

- НЕ меняются значения: `IntervalMin=360`, `RetryIntervalSec=300`, `TimeoutSec=30`, `RetentionFiles=10`, `MaintenanceIntervalMin=60`.
- НЕ меняются механики: `SnapshotSchedule`, `ChangeMapDiff`, `PulseKeys`, логика `EtcdExportRetention.Select`, логика `AgentLoop`, `EtcdNodeSpecBuilder`, форматы объектов/меты/статус-ключа.
- Fail-fast валидация env не меняется.
- Миграционных шагов нет.
- Комментарии по-русски, идентификаторы по-английски; тесты — с AAA-комментариями (Arrange/Act/Assert).
- Никакого P/Invoke; сборка `TreatWarningsAsErrors=true` — предупреждений быть не должно.
- Бюджеты ожиданий в существующих тестах не менять (правило AGENTS.md о бюджетах).

## Review Focus (главные риски; каждый закрыт проверкой своего шага)

1. Старые значения (300/28) и старые RPO-пассажи («≈5 мин») остаются в каком-то звене цепочки/канона → Шаг 6: grep-критерий spec §6.1–6.2.
2. Интеграции гоняются против НЕпересобранного образа `pgworker/etcdnode:dev` (старый бинар, ложный результат) → Шаг 5 обязан precede Шага 7; проверка `docker image inspect`.
3. Ассерт «полные бэкапы» сверяет мету не той пары (старой/чужой) → привязка к свежей паре через отсечку `knownKeys` (Шаг 4, кейс 3).
4. Takeover-ассерт принимает старую пару за свежую → `taken_unix` строго больше времени kill (Шаг 4, кейс 2).
5. Кейс «не-лидеры не снимают» ложно падает из-за негарантированного порядка кейсов xUnit v3 (бывший лидер имеет старые файлы в томе) → проверка ВНУТРИ `LeaderDown...` до kill: единственный источник смены лидера в коллекции — kill этого же кейса, до него тома не-лидеров пусты при любом порядке (Шаг 4, кейс 1).
6. Счётный ассерт `ImportantChange` ложно падает при `RetentionObjects=3` (общий bucket коллекции: ретенция агента сносит старейшую пару при новой выгрузке — счёт не растёт) → детекция нового `.db`-ключа вне отсечки `knownKeys` вместо роста счётчика (Шаг 4, кейс 4).

---

## Шаг 1. Канон: arch-first (сильная модель, без подпунктов — документация)

- **Вход:** worktree чист, spec прочитан.
- **Действие:** правки в 4 файлах `arch/` (точные тексты ниже; тон окружающего текста, кириллические формулировки, разметка как в файлах).
  1. `arch/etcdnode/02-snapshots.md` §2, первый абзац (строки ~34–40):
     - Точной парой внутри абзаца: «(эталонной карты нет — первый тик нового лидера снимает слепок немедленно, RPO-консервативно)» → «(эталонной карты нет — первый тик нового лидера снимает слепок немедленно: гейт — только `MinIntervalSec` от последней выгрузки; RPO-консервативно)».
     - Отдельным абзацем ПОСЛЕ конца этого абзаца (сразу после «…RPO-консервативно).», до заголовка «## 3.») вставить:
       «Каждый слепок — ПОЛНЫЙ автономный дамп всего пространства ключей (`/v3/snapshot/save` иных не делает): каждый хранимый бэкап самодостаточен — восстановление из любого одного слепка, без цепочек; инкрементальных форм нет.»
  2. `arch/etcdnode/02-snapshots.md` §3: «(если выгрузка старше `MinIntervalSec=300`)» → «(если выгрузка старше `MinIntervalSec=5`)».
  3. `arch/etcdnode/02-snapshots.md` §4: «ретенция N=28 пар (guard ≥ 1)» → «ретенция N=3 пар (guard ≥ 1)».
  4. `arch/etcdnode/01-agent.md` §3 (env-листинг): `ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC=300` → `ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC=5`; `ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS=28` → `ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS=3`.
  5. `arch/etcdnode/03-restore.md` п.6: «детекция важных изменений закрывает ≈5 мин при активности» → «детекция важных изменений закрывает ≈5 с при активности».
  6. `arch/etcdworker/06-operability.md` §3 («Надёжность», строка ~50, второй RPO-пассаж): «RPO = интервал\n  выгрузки: 360 мин планово / ≈5 мин при важных изменениях» → заменить «≈5 мин при важных изменениях» на «≈5 с при важных изменениях» (перенос строки в файле — сохранить как есть, меняется только фрагмент «≈5 мин» → «≈5 с»).
  7. `arch/etcdworker/06-operability.md` §5 (листинг NodeSnapshot): `MinIntervalSec=300` → `MinIntervalSec=5`; `RetentionObjects=28` → `RetentionObjects=3`.
- **Выход:** канон несёт новые значения (5/3) и два явных инварианта (полные автономные бэкапы; немедленный слепок нового лидера с гейтом MinIntervalSec); оба RPO-пассажа говорят «≈5 с».
- **Проверка:** `grep -rn "MinIntervalSec=300\|MIN_INTERVAL_SEC=300\|RetentionObjects=28\|RETENTION_OBJECTS=28\|N=28\|≈5 мин" arch/` — вывод пуст (паттерн «≈5 мин» матчится только правимыми `03-restore.md:43` и `06-operability.md:50`; «−5 мин»/«~25 мин» в других документах arch/ не матчатся).
- **Spec:** §3.1 (все 6 строк таблицы), §6.1.

## Шаг 2. Код и конфиги: дефолты значений (сильная + подпункты)

- **Вход:** Шаг 1 принят (канон обновлён).
- **Действие (сильная):** делегирует подпункты НП 2.1–2.3 слабой модели, затем ревьюит diff (значения 5/3 в четырёх местах EtcdNode.App/Options.cs; RetentionFiles=10 и IntervalMin=360 нетронуты) и собирает решение.
- **Подпункты:**

**НП 2.1.** Файл `<корень worktree>/src/EtcdNode.App/Options.cs`. Пять замен (каждая уникальна в файле):
1. Заменить XML-комментарий класса (строки ~23–24):
   - старое: `/// <summary>Параметры DR-слепков (arch/etcdnode/02-snapshots.md): значения`
     `/// прежние (бюджеты не меняются).</summary>`
   - новое: `/// <summary>Параметры DR-слепков (arch/etcdnode/02-snapshots.md).</summary>`
2. `    public int MinIntervalSec { get; set; } = 300;` → `    public int MinIntervalSec { get; set; } = 5;`
3. `    public int RetentionObjects { get; set; } = 28;` → `    public int RetentionObjects { get; set; } = 3;`
4. `                MinIntervalSec = Int("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", 300),` → `                MinIntervalSec = Int("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", 5),`
5. `                RetentionObjects = Int("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", 28),` → `                RetentionObjects = Int("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", 3),`
Больше в файле ничего не менять.

**НП 2.2.** Файл `<корень worktree>/src/EtcdWorker.App/Options.cs`. Две замены в классе `NodeSnapshotOptions`:
1. `    public int MinIntervalSec { get; set; } = 300;` → `    public int MinIntervalSec { get; set; } = 5;`
2. `    public int RetentionObjects { get; set; } = 28;` → `    public int RetentionObjects { get; set; } = 3;`
Больше в файле ничего не менять.

**НП 2.3.** Файл `<корень worktree>/src/EtcdWorker.App/appsettings.json`. Две замены в секции `"NodeSnapshot"`:
1. `      "MinIntervalSec": 300,` → `      "MinIntervalSec": 5,`
2. `      "RetentionObjects": 28,` → `      "RetentionObjects": 3,`
Больше в файле ничего не менять (в т.ч. `"RetentionFiles": 10`, `"IntervalMin": 360`, `"RetryIntervalSec": 300`).

- **Выход:** цепочка кода несёт 5/3.
- **Проверка (сильная):** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-plan-build.log 2>&1` (из корня worktree; фон, затем читать лог) — 0 Error, 0 Warning; `grep -n "MinIntervalSec\|RetentionObjects" src/EtcdNode.App/Options.cs src/EtcdWorker.App/Options.cs src/EtcdWorker.App/appsettings.json` — только 5 и 3 (кроме упоминаний в fail-fast сообщениях).
- **Spec:** §3.2, §6.2.

## Шаг 3. Юнит-тесты: синхронизация чисел (сильная + подпункты)

- **Вход:** Шаг 2 собран.
- **Действие (сильная):** делегирует НП 3.1–3.3, ревьюит diff, прогоняет юниты (см. Проверку), при падении разбирает (падение вне правок — не чинить, фиксировать).
- **Подпункты:**

**НП 3.1.** Файл `<корень worktree>/src/tests/EtcdNode.UnitTests/EtcdNodeEnvTests.cs`, метод `Load_NoEnv_Defaults`. Две замены в блоке Assert:
1. `            o.Snapshots.MinIntervalSec.Should().Be(300);` → `            o.Snapshots.MinIntervalSec.Should().Be(5);`
2. `            o.Snapshots.RetentionObjects.Should().Be(28);` → `            o.Snapshots.RetentionObjects.Should().Be(3);`
Больше в файле ничего не менять.

**НП 3.2.** Файл `<корень worktree>/src/tests/EtcdWorker.UnitTests/Core/EtcdNodeSpecBuilderTests.cs`. Три замены, каждая уникальна по тексту в файле (кортеж позиционный: `IntervalMin, MinIntervalSec, RetryIntervalSec, TimeoutSec, RetentionFiles, RetentionObjects, MaintenanceIntervalMin, S3CaPem` — меняются ТОЛЬКО 2-я и 6-я позиции):
1. В хелпере `Build`: `360, 300, 300, 30, 10, 28, 60, s3CaPem);` → `360, 5, 300, 30, 10, 3, 60, s3CaPem);`
2. В методе `InitialClusterPassed`: `360, 300, 300, 30, 10, 28, 60, null);` → `360, 5, 300, 30, 10, 3, 60, null);`
3. В методе `AgentEnvPresent`: `        env["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"].Should().Be("28");` → `        env["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"].Should().Be("3");`
Строку с `ETCDNODE_SNAPSHOTS__INTERVAL_MIN` = `"360"` не трогать. Больше в файле ничего не менять.

**НП 3.3.** Файл `<корень worktree>/src/tests/EtcdWorker.IntegrationTests/Membership/MembershipProcessTests.cs`. Две замены в кортеже `EtcdProvisioningOptions` (именованные аргументы):
1. `            SnapshotIntervalMin: 360, SnapshotMinIntervalSec: 300, SnapshotRetryIntervalSec: 300,` → `            SnapshotIntervalMin: 360, SnapshotMinIntervalSec: 5, SnapshotRetryIntervalSec: 300,`
2. `            SnapshotTimeoutSec: 30, SnapshotRetentionFiles: 10, SnapshotRetentionObjects: 28,` → `            SnapshotTimeoutSec: 30, SnapshotRetentionFiles: 10, SnapshotRetentionObjects: 3,`
Больше в файле ничего не менять.

- **Выход:** юнит-ассерты дефолтов и явные кортежи синхронизированы.
- **Проверка (сильная):** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release > /tmp/pgw-plan-u1.log 2>&1` и `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release > /tmp/pgw-plan-u2.log 2>&1` (фон; по правилам наблюдения AGENTS.base §13: старт ≤15 с, контроль каждые 2 мин по лог-файлу) — оба Passed! Failed: 0.
- **Spec:** §3.3 (строки 1–2 и 4-я — кортеж MembershipProcessTests), §6.3.

## Шаг 4. Интеграционные ассерты: инфраструктура + 3 кейса (сильная; НП — только механические вставки)

- **Вход:** Шаги 1–3 зелёные.
- **Действие (сильная):**
  1. Делегирует подпункты НП 4.1–4.3 слабой модели (механические вставки готового кода).
  2. Сам пишет ассерты в `src/tests/EtcdNode.IntegrationTests/SnapshotAgentTests.cs` (живой контур — НЕ подпункт; стиль файла: AAA-комментарии, поллинг с бюджетом ≤ 30 с, `TestContext.Current.CancellationToken`):
     - **Кейс 1 (лидерное снятие, spec §3.4.1).** Отдельный кейс НЕ заводится: порядок кейсов xUnit v3 не фиксирован, отдельный кейс ложно падал бы при исполнении после смены лидера (том бывшего лидера содержит старые `.db`). Проверка вставляется ВНУТРЬ `LeaderDown_StatusFrozenUntilTtl_ThenTakeover` как Assert-0: после определения `leaderNode`/`aliveNode` (Arrange-фаза: посев цели, `WaitStatusOkAsync`, `AwaitQuietPhaseAsync` уже выполнены) и ДО `KillNodeAsync` — определить двух не-лидеров (имена `node1..3`, не равные `leaderNode`; `aliveNode` — любой из них) и для каждого вызвать `env.SnapshotFilesOfAsync(name)` → Assert: вывод не содержит `snapshot-` (пустая строка или пустой том). Обоснование надёжности: единственный источник смены лидера в коллекции — kill внутри этого же кейса, поэтому на момент его Arrange лидер неизменен с сотворения контура и тома не-лидеров пусты при ЛЮБОМ порядке исполнения кейсов сборки.
     - **Кейс 2 (смена лидера → свежий бэкап, spec §3.4.2).** Дополнить тот же `LeaderDown_StatusFrozenUntilTtl_ThenTakeover` ПОСЛЕ существующего Assert-2 (takeover): перед `KillNodeAsync` зафиксировать `killUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds()` и `knownDb` — множество ключей `.db` из `ListDbObjectsAsync`; после подтверждения takeover поллингом ≤ 30 с найти `.db`-ключ вне `knownDb`; скачать его мету (`GetObjectAsync` по ключу `<id>.meta.json`, парсинг JsonDocument): Assert `taken_unix` (GetInt64) > killUnix; прочитать `/etcdnode/leader` заново: Assert `instance` меты — имя узла из значения leader-ключа (сравнение `Contains` по образцу существующего кода кейса). Существующие Assert-1/Assert-2 не менять; `finally` с `StartNodeAsync` остаётся.
     - **Кейс 3 (полные/самодостаточные бэкапы, spec §3.4.3).** Дополнить `Leader_Elected_StatusWritten_ObjectsInS3` после существующего instance-ассерта: скачать свежий `.db` — `await s3.GetObjectBytesAsync(freshDb.Key, ct)`; распарсить уже прочитанную мету `metaJson` полностью: Assert (а) `size_bytes` == `db.Value.Length`; (б) `sha256` меты == `Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(db.Value)).ToLowerInvariant()` (канон hex-lower — формула `SnapshotExporter`); (в) `revision` существует (ValueKind Number) и > 0. Добавить `using System.Security.Cryptography;` в usings файла, если отсутствует.
     - **Кейс 4 (сопутствующая правка, spec §3.4.4 — следствие `RetentionObjects=3` в общем bucket коллекции).** Переработать `ImportantChange_Triggers_Snapshot`: заменить счётный ассерт (`before`/`after` через `DbCount`, «after > before») на детекцию НОВОГО `.db`-ключа вне отсечки: перед Act снять `knownKeys` (множество ключей `.db` из `ListDbObjectsAsync` — паттерн кейса `Leader_Elected_StatusWritten_ObjectsInS3`), после `PutAssertedAsync` важного ключа поллингом ≤ 30 с ждать `.db`-ключ вне `knownKeys`. Причина: bucket один на коллекцию, экспортёр-кейсы (вкл. `Retention_RemovesOldestPair`) оставляют ≥3 пар, ретенция агента (N=3) сносит старейшую при новой выгрузке — рост счётчика не детерминирован. Хелпер `DbCount` НЕ удалять (используется `AwaitQuietPhaseAsync`; в тишине лидер не снимает — счёт там стабилен и при N=3).
  3. Ревьюнит вставки НП и свои кейсы; сборка тестового проекта.
- **Подпункты:**

**НП 4.1.** Файл `<корень worktree>/src/EtcdNode.Core/EtcdSnapshotS3.cs`. Две вставки готового кода (больше ничего не менять):
1. В интерфейс `IEtcdSnapshotS3`, сразу ПОСЛЕ объявления метода `GetObjectAsync` (перед `DeleteKeysAsync`), вставить:
```csharp
    /// <summary>Точечное байтовое чтение объекта (интеграционные сверки
    /// содержимого .db с метой); отсутствие ключа — Failed.</summary>
    Task<Result<byte[]>> GetObjectBytesAsync(string key, CancellationToken ct);
```
2. В класс `EtcdSnapshotS3`, сразу ПОСЛЕ закрывающей скобки метода `public async Task<Result<string>> GetObjectAsync(...)`, вставить:
```csharp
    public async Task<Result<byte[]>> GetObjectBytesAsync(string key, CancellationToken ct)
    {
        try
        {
            var request = new GetObjectRequest { BucketName = _bucket, Key = key };
            using var response = await _client.GetObjectAsync(request, ct);
            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms, ct);
            return Result<byte[]>.Success(ms.ToArray());
        }
        catch (Exception e)
        {
            return Result<byte[]>.Failed(new ApplicationException($"S3 get {key}: {e.Message}", e));
        }
    }
```

**НП 4.2.** Файл `<корень worktree>/src/tests/EtcdNode.IntegrationTests/EtcdNodeEnvironment.cs`. Одна вставка готового кода: сразу ПОСЛЕ метода `StartNodeAsync` (перед `private IContainer Node(...)`), вставить:
```csharp
    // Список файлов в томе /snapshots узла (docker exec ls; пустой том —
    // пустая строка): ассерт «слепки снимает только лидер».
    public async Task<string> SnapshotFilesOfAsync(string name)
        => await DockerAsync(
            $"exec {Node(name).Name.TrimStart('/')} ls /snapshots", okWhenMissing: false);
```
Больше в файле ничего не менять.

**НП 4.3.** Файл `<корень worktree>/src/tests/EtcdNode.IntegrationTests/EtcdNodeEnvironment.cs`. Две замены в методе `BuildNode`:
1. `            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "28")` → `            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "3")`
2. Непосредственно ПЕРЕД строкой `            .WithEnvironment("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", "1")` вставить строку комментария:
   `            // MinIntervalSec: канон-дефолт 5; поведенческим кейсам нужно ускорение 1`
Больше в файле ничего не менять (в т.ч. `MIN_INTERVAL_SEC "1"` остаётся).

- **Выход:** тестовая инфраструктура готова, ассерты инвариантов и сопутствующая правка написаны (до прогона — образ ещё старый).
- **Проверка (сильная):** сборка ОБОИХ интеграционных проектов: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/EtcdNode.IntegrationTests -c Release --nologo > /tmp/pgw-plan-itbuild.log 2>&1` и `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/EtcdWorker.IntegrationTests -c Release --nologo > /tmp/pgw-plan-itbuild2.log 2>&1` (вторая обязательна: НП 3.3 правит `MembershipProcessTests` — компилируется только там, до Шага 7 п.4) — оба 0 Error/0 Warning; код-ревью своих кейсов против Review Focus 3–6.
- **Spec:** §3.3 (строки 3–4), §3.4 (все четыре), §6.8.

## Шаг 5. Пересборка образа узла pgworker/etcdnode:dev (сильная; ОБЯЗАТЕЛЬНО до интеграционных серий)

- **Вход:** Шаги 1–4 приняты; все правки кода EtcdNode завершены (после пересборки образа код больше не менять — иначе повторить шаг).
- **Действие (сильная):** из корня worktree, обе команды в фоне с логом:
  1. Определить rid: `docker version --format '{{.Server.Arch}}'` → вывод `arm64` → rid=`linux-arm64`; вывод `amd64`/`x86_64` → rid=`linux-x64`.
  2. `DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/EtcdNode.App -c Release -r <rid> --self-contained -o artifacts/etcdnode > /tmp/pgw-plan-publish.log 2>&1`
  3. `docker build -f docker/etcdnode/Dockerfile --build-arg ETCDNODE_PUBLISH_DIR=artifacts/etcdnode -t pgworker/etcdnode:dev . > /tmp/pgw-plan-imgbuild.log 2>&1`
- **Выход:** образ `pgworker/etcdnode:dev` содержит свежий бинар агента с дефолтами 5/3.
- **Проверка:** оба лога без ошибок; `docker image inspect pgworker/etcdnode:dev --format '{{.Id}} {{.Created}}'` — Created позже времени правки `Options.cs`.
- **Spec:** §6.6.

## Шаг 6. Контроль полноты значений по репозиторию (сильная)

- **Вход:** Шаги 1–5 выполнены.
- **Действие (сильная):** grep-критерии spec §6.1–6.2 по корню worktree:
  - `grep -rn "MinIntervalSec=300\|MIN_INTERVAL_SEC=300\|RetentionObjects=28\|RETENTION_OBJECTS=28\|N=28" arch/` — пусто.
  - `grep -rn '"MinIntervalSec": 300\|"RetentionObjects": 28\|MinIntervalSec = Int("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", 300)\|RetentionObjects = Int("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", 28)' src/` — пусто.
  - `grep -rn "SnapshotMinIntervalSec: 300\|SnapshotRetentionObjects: 28\|Should().Be(\"28\")" src/tests/` — пусто.
- **Выход:** ни одного старого значения в актуальных контекстах.
- **Проверка:** все три grep пусты (попадания в `docs/superpowers/` — не править: история задач).
- **Spec:** §6.1, §6.2, §6.8.

## Шаг 7. Прогоны серий с зачисткой (сильная)

- **Вход:** образ пересобран (Шаг 5), сборка зелёная.
- **Действие (сильная):** серии строго по порядку, каждая — в фоне с полным логом в файл (`/tmp/pgw-plan-<серия>.log`), наблюдение по AGENTS.base §13 (старт ≤ 15 с, контроль зависаний каждые 2 мин, глубже 5 мин — онлайн-анализ лога); после КАЖДОЙ серии — дождаться финальной строки и зачистить остатки (own-only: контейнеры/тома/сети её тега; страховочно `docker ps -a | grep etcdn-`, `docker network ls | grep etcdn-`; осиротевшие сети — `docker network prune -f`):
  1. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.UnitTests -c Release` → зелёная.
  2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release` → зелёная.
  3. Зачистка → `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdNode.IntegrationTests -c Release` → зелёная (вкл. дополненные `LeaderDown_StatusFrozenUntilTtl_ThenTakeover` (Assert-0 не-лидеров + свежая пара после takeover), `Leader_Elected_StatusWritten_ObjectsInS3` (согласованность меты) и переработанный `ImportantChange_Triggers_Snapshot`). Точечный прогон перед полным: `--filter FullyQualifiedName~SnapshotAgentTests`.
  4. Зачистка → `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.IntegrationTests -c Release` → зелёная (вкл. `MembershipProcessTests` с обновлённым кортежем).
  5. Финальная зачистка: остатков нет (`docker ps -a | grep etcdn-` пусто, `docker network ls | grep etcdn-` пусто).
- **Выход:** все четыре серии зелёные; падение вне области правок — НЕ чинить: зафиксировать (фильтр/команда/коммит/текст ошибки/пути логов) и заводить пункт в `arch/roadmap/bugs.md` (AGENTS.md), работу продолжить.
- **Проверка:** в каждом логе финальная строка Passed!, Failed: 0; ассерты инвариантов прошли (Review Focus 3–5 подтверждены фактически).
- **Spec:** §6.3–6.5, §6.7.

## Шаг 8. Коммит и финальный чек-лист (сильная)

- **Вход:** Шаг 7 зелёный.
- **Действие (сильная):** прогнать чек-лист spec §6 (пп. 1–5, 7–8) по факту; закоммитить ВСЁ в feature-ветке (коммиты в feature-ветке свободны, AGENTS.base §7):
  ```
  git add arch/etcdnode/ arch/etcdworker/06-operability.md src/EtcdNode.App/Options.cs src/EtcdNode.Core/EtcdSnapshotS3.cs src/EtcdWorker.App/Options.cs src/EtcdWorker.App/appsettings.json src/tests/EtcdNode.UnitTests/ src/tests/EtcdWorker.UnitTests/Core/EtcdNodeSpecBuilderTests.cs src/tests/EtcdNode.IntegrationTests/ src/tests/EtcdWorker.IntegrationTests/Membership/MembershipProcessTests.cs
  git commit -m "etcdnode: ретенция S3 3 пары, пауза внеочередных 5 с (канон+код+тесты), ассерты лидерных инвариантов"
  ```
  Мерж в main / пуш — ТОЛЬКО по явной просьбе пользователя.
- **Выход:** работа закоммичена; `artifacts/` и логи в коммит не попадают.
- **Проверка:** `git status` чист (кроме неотслеживаемых артефактов сборки — проверить, что `artifacts/` в .gitignore; если нет — не добавлять в коммит); чек-лист spec §6 закрыт полностью.
- **Spec:** весь §6.

---

## Соответствие шагов и spec (самопроверка покрытия)

| Spec | Шаг |
|---|---|
| §3.1 канон (7 правок, вкл. второй RPO-пассаж 06-operability.md §3) | Шаг 1 |
| §3.2 код/конфиги (3 файла) | Шаг 2 (НП 2.1–2.3) |
| §3.3 синхронизация тестов (4 файла) | Шаг 3 (НП 3.1–3.3) + Шаг 4 (НП 4.3 — EtcdNodeEnvironment) |
| §3.4 ассерты инвариантов (3 шт.) + сопутствующая правка ImportantChange | Шаг 4 (сильная) + НП 4.1–4.2 (инфраструктура) |
| §6.6 пересборка образа | Шаг 5 |
| §6.1–6.2, 6.8 grep-критерии | Шаг 6 |
| §6.3–6.5, 6.7 прогоны+зачистка | Шаг 7 |
| §6 весь | Шаг 8 |

Развилок у исполнителя нет: все решения зафиксированы в spec §5, технические следствия (GetObjectBytesAsync, SnapshotFilesOfAsync, правило rid, проверка не-лидеров внутри takeover-кейса до kill, переработка ImportantChange на отсечку knownKeys) закреплены в шагах 4–5 с обоснованием.
