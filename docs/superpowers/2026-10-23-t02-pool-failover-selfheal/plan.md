# t02-pool-failover-selfheal — план реализации

> **Для исполнителя (dev-flow, Фаза 6):** план исполняет субагент coder —
> сильная модель, работает один и делает ВСЮ работу сам, включая подпункты
> (для него подпункты — обычные инструкции). Подпункты оформлены как
> самодостаточные задания для слабой модели (без контекста проекта) — формат
> сохраняется на случай смены режима исполнения. Шаги отмечаются чекбоксами
> (`- [ ]`).

**Цель:** E2E-сценарий `Pool_FailoverList_And_Exhaustion_SelfHeal` стабильно
сходится: после исчерпания write-списка пула (DEGRADED + маркер
`s3-pool-exhausted`) и подъёма ноды контроль возвращает wal-статус в ACTIVE в
бюджете гейта; устранены вечный цикл пересъёмов полных (checkpoint=fast) и
медленная реакция контроля на исцеление (контроль каждый тик при
exhaustion-DEGRADED).

**Архитектура:** три независимых фикса одного контура. (1) Позиционный
детектор `IsPoolExhaustionActive` — маркер исчерпания актуален, пока последняя
`pool_exhausted`-строка лога мастер-агента новее последней `heartbeat`-строки
(heartbeat — единственный знак успешной доставки); сменяет подстрочный
`Contains` в обоих t36-блоках `WalStreamProcess.ControlDueAsync`, чтение лога
консолидируется в один docker-вызов за проход. (2) Расписание контроля:
exhaustion-DEGRADED (DEGRADED с маркером в `error`) — контроль каждый тик,
симметрично BROKEN. (3) `docker/backup/entrypoint.sh`: `--checkpoint=spread` →
`--checkpoint=fast`. Arch-first: канон `arch/19-backups.md` правится ДО кода.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
xUnit + FluentAssertions, интеграционные тесты на реальном etcd (fake
docker/SQL/S3-драйверы), docker-E2E.

**Spec:** `docs/superpowers/2026-10-23-t02-pool-failover-selfheal/spec.md`
(план аргументирует от spec — исполнитель читает оба).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t02-pool-failover-selfheal`
(вся работа — только внутри; ветка `feat-t02-pool-failover-selfheal`).

## Глобальные ограничения (spec §6 — действуют на каждый шаг)

- НЕ менять: формат stdout-маркеров приёмника (`WalReceiverMarkers`), контракт
  wal-ключа etcd (поле `error` как было), механику breaker/перебора
  `PoolBackupS3`, инвариант подтверждения-по-доставке, тестовый сценарий AC4.
- `AgentLogTail = 200` не трогать (окно видимости tli_jump-маркера).
- Никаких таймеров/порогов свежести маркера — только позиционное сравнение
  строк (решение 2 spec).
- Юнит-тесты — комментарии AAA; документация/комментарии — по-русски,
  идентификаторы — на английском.
- Оба Dockerfile (`PgWorker.Backup.Dockerfile`, `PgWorker.Backup.E2E.Dockerfile`)
  используют один `docker/backup/entrypoint.sh` — правка только в нём.
- Сборка и запуск тестов — двумя ОТДЕЛЬНЫМИ командами; тесты — только с
  `--no-build` и env-префиксом при самой команде; перед docker-E2E —
  `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`; зачистка
  контейнеров/сетей после КАЖДОЙ серии.
- Коммиты — в feature-ветке свободно (в main — только по явной команде
  пользователя).

## Review Focus

Классы входов/отказов, которые spec подразумевает, но основные тесты не
проверяют; каждый закрыт тестом в шаге, владеющем кодом:

1. Docker-чтение лога агента падает (transient: агент пересоздаётся) — контроль
   не должен падать тиком и деградировать фантомно → тест
   `ExhaustionMarker_AgentLogReadFailure_NoPhantomDegradation` (шаг 3).
2. Маркер НЕ снимается сам по себе, пока доставки нет (удержание между тиками
   при не поднятой ноде) → средняя фаза теста
   `ExhaustionMarker_ThenHeartbeatHealsActive` (шаг 3).
3. Обычные DEGRADED (lag/тишина, без маркера) остаются на общем расписании —
   правило «каждый тик» не должно их зацепить → существующие тесты
   (`VerifyLagAboveThresholdDegradedAc5a` и др.) как регресс шага 3.
4. Матч детектора — ТОЛЬКО stdout-маркер приёмника `pool_exhausted`
   (подчёркивание, `{"pool_exhausted":true}`, `WalReceiverMarkers.PoolExhausted`);
   дефисная форма `s3-pool-exhausted` (`S3PoolMarkers.Exhausted`) в
   result-JSON и текстах ошибок детектором НЕ матчится — посторонние строки не
   матчатся (spec п. 4.3.1). Закреплено тестом
   `PoolExhaustionDetector_ForeignLinesIgnored`, включающим result-JSON с
   дефисной формой в `error` (шаг 2).
5. tli_jump-механика (t24) не затронута консолидацией чтения — чтение t36-блоков
   отдельное от tli_jump-чтения → существующие тесты `TliJump*` как регресс
   шага 3.

---

### Шаг 1. Канон arch/19: актуальность маркера исчерпания + checkpoint=fast (arch-first)

**Вход:** worktree `feat-t02-pool-failover-selfheal`, spec прочитан; код ещё не
тронут.

**Действие** (сильная модель — документация, подпунктов нет): две точечные
правки `arch/19-backups.md`:

1. §3 «Online-WAL», абзац «Правила непрерывности» — фрагмент, заканчивающийся
   словами «(§3 — алерт `backup-pool-unreachable`; «lag/тишина» без маркера —
   обычное отставание).» (сразу после него, перед «Слот при BROKEN:»),
   вставить два предложения (дословно по spec п. 4.1):

```
Маркер исчерпания актуален, пока в хвосте лога мастер-агента последняя строка
`pool_exhausted` НОВЕЕ последней строки `heartbeat` (heartbeat — единственный
знак успешной доставки: подтверждение слота после успешного put); успешная
доставка после исчерпания (heartbeat позже последнего маркера) снимает
маркер — статус пересчитывается общим порядком (цепочка/lag/тишина), `error`
очищается. Exhaustion-DEGRADED (DEGRADED с маркером в `error`) — контроль
КАЖДЫЙ тик, как BROKEN (скорость заживления; union-листинг — один класс
вызовов).
```

2. §2 «Механика полных бэкапов», строка «Инструмент» — заменить
   `--checkpoint=spread` на `--checkpoint=fast` и дополнить обоснованием
   (дословно по spec п. 4.2). Итоговый вид строки:

```
- **Инструмент**: `pg_basebackup` (plain-формат, `-X stream`,
  `--checkpoint=fast`, `--manifest-checksums=SHA256`). `fast`, а не `spread`:
  spread берёт давний checkpoint/restartpoint источника — `wal_start_segment`
  из `backup_label` оказывается ниже первого сегмента набора `-X stream`,
  сегменты [wal_start .. набор) не дублируются в `wal/` → verify-полнота от
  wal_start — вечный FAILED → бесконечные пересъёмы без валидного полного;
  fast делает checkpoint на момент старта — wal_start-сегмент входит в набор
  и дублируется, verify-цепь сходится.
```

**Выход:** канон описывает актуальность маркера, контроль каждый тик для
exhaustion-DEGRADED и checkpoint=fast; история ветки фиксирует канон ДО кода.

**Проверка:** `git diff arch/19-backups.md` — ровно две правки (§2 и §3);
`grep -n "checkpoint=fast" arch/19-backups.md` находит строку «Инструмент»;
`grep -c "checkpoint=spread" arch/19-backups.md` → 0.

**Связь со spec:** п. 4.1, 4.2; фаза 1; решения 5, 6; критерий 8.5 (arch-first
в истории ветки).

**Коммит:** `docs(arch): arch/19 — актуальность маркера s3-pool-exhausted (снятие heartbeat'ом, контроль каждый тик) и pg_basebackup --checkpoint=fast`.

---

### Шаг 2. Детектор `IsPoolExhaustionActive` + юнит-тесты детектора (TDD)

**Вход:** шаг 1 закоммичен.

**Действие** (сильная модель): тесты детектора пишутся ДО/ВМЕСТЕ с реализацией
(TDD, spec фаза 2). Механическая часть — подпункты 2.1–2.4; coder исполняет их
сам и ревьюит результат. Реализация — чистая функция рядом с
`ParseTliJumpMarker` (`src/PgWorker.Backups/WalStreamProcess.cs`, после метода
`ParseTliJumpMarker`, до `TliOfSegment`), сигнатура из spec п. 4.3.1:
`internal static bool IsPoolExhaustionActive(string logTail)`. Матч —
подстрочный по строке: «строка `pool_exhausted`» — строка, содержащая
`pool_exhausted` (подчёркивание); единственный реальный источник подстроки в
логе агента — stdout-маркер приёмника `{"pool_exhausted":true}`
(`WalReceiverMarkers.PoolExhausted`). Дефисная форма `s3-pool-exhausted`
(`S3PoolMarkers.Exhausted` — тексты ошибок, result-JSON) детектором НЕ
матчится — посторонние строки не матчатся (spec п. 4.3.1). `heartbeat`-строка —
содержащая `heartbeat` (`{"heartbeat":"X/Y"}`). Никакого JSON-парсинга и
таймеров.

**Выход:** детектор реализован, покрыт юнит-тестами; сборка без warnings
(`TreatWarningsAsErrors=true`).

**Проверка:**
1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — успешно;
2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcessTests"` — все тесты
   зелёные, включая новые детекторные (5 шт.).

**Связь со spec:** п. 4.3.1, 4.5 (детектор), фаза 2; решения 2, 3, 4, 13.

**Подпункты (слабой модели; coder исполняет сам):**

- [ ] **2.1. Добавить `InternalsVisibleTo` в `src/PgWorker.Backups/PgWorker.Backups.csproj`.**
  В файле `src/PgWorker.Backups/PgWorker.Backups.csproj` найди блок (ближе к
  концу файла):

```xml
    <ItemGroup>
        <!-- Юниты ворк-фактов agents (t27): BuildAgentFacts — internal-функция. -->
        <InternalsVisibleTo Include="PgWorker.UnitTests"/>
    </ItemGroup>
```

  Замени его целиком на:

```xml
    <ItemGroup>
        <!-- Юниты ворк-фактов agents (t27) и детектора исчерпания (t02):
             internal-функции. -->
        <InternalsVisibleTo Include="PgWorker.UnitTests"/>
        <InternalsVisibleTo Include="PgWorker.IntegrationTests"/>
    </ItemGroup>
```

  Больше в файле ничего не менять.

- [ ] **2.2. Добавить юнит-тесты детектора в
  `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs`.**
  В самом конце класса `WalStreamProcessTests` (сразу после закрывающей
  скобки метода `TliJumpMarker_CoveredByFull_NotSet`, перед закрывающей `}`
  класса, строка ~1860) добавь блок:

```csharp
    // ---- t02 (arch/19 §3): детектор актуальности исчерпания write-списка ----

    // Строки маркеров приёмника (формат WalReceiverMarkers): heartbeat —
    // единственный знак успешной доставки (подтверждение слота после put).
    private const string HeartbeatLine = "{\"heartbeat\":\"0/8000000\"}";
    private const string PoolExhaustedLine = "{\"pool_exhausted\":true}";

    // AAA: маркер есть, heartbeat после него НЕТ — исчерпание актуально.
    [Fact]
    public void PoolExhaustionDetector_MarkerWithoutHeartbeat_IsActive()
    {
        // Arrange — хвост: старый heartbeat, затем маркер исчерпания
        var tail = HeartbeatLine + "\n" + PoolExhaustedLine;

        // Act / Assert — маркер новее последнего heartbeat → активен
        WalStreamProcess.IsPoolExhaustionActive(tail).Should().BeTrue();
    }

    // AAA: heartbeat ПОСЛЕ последнего маркера — доставка прошла, маркер снят.
    [Fact]
    public void PoolExhaustionDetector_HeartbeatAfterMarker_Lifted()
    {
        // Arrange — хвост: маркер, затем свежий heartbeat (успешная доставка)
        var tail = PoolExhaustedLine + "\n" + HeartbeatLine;

        // Act / Assert — heartbeat новее маркера → исчерпание снято
        WalStreamProcess.IsPoolExhaustionActive(tail).Should().BeFalse();
    }

    // AAA: маркера нет вовсе — не активен (посторонние строки и пустой хвост).
    [Fact]
    public void PoolExhaustionDetector_NoMarker_Inactive()
    {
        // Arrange — только посторонние строки (фаза старта, heartbeat,
        // успешный result-JSON) и пустой хвост
        var tail = "{\"phase\":\"starting\"}\n" + HeartbeatLine
            + "\n{\"ok\":true,\"wal_start_segment\":\"000000010000000000000002\",\"size_bytes\":1024}";

        // Act / Assert
        WalStreamProcess.IsPoolExhaustionActive(tail).Should().BeFalse();
        WalStreamProcess.IsPoolExhaustionActive("").Should().BeFalse();
    }

    // AAA: delivering успехом НЕ считается (heartbeat — единственное
    // подтверждение): маркер + delivering без heartbeat — исчерпание держится;
    // heartbeat после — снимает (delivering между ними не мешает снятию).
    [Fact]
    public void PoolExhaustionDetector_DeliveringDoesNotLift()
    {
        // Arrange — delivering между маркером и heartbeat
        var delivering = "{\"phase\":\"delivering\",\"segment\":\"000000010000000000000008\"}";

        // Act / Assert — delivering сам по себе не снимает маркер
        WalStreamProcess.IsPoolExhaustionActive(PoolExhaustedLine + "\n" + delivering)
            .Should().BeTrue("heartbeat is the only delivery confirmation");
        // heartbeat после маркера снимает — delivering не препятствие
        WalStreamProcess.IsPoolExhaustionActive(
                PoolExhaustedLine + "\n" + delivering + "\n" + HeartbeatLine)
            .Should().BeFalse();
    }

    // AAA: посторонние строки не матчатся: фазы starting/streaming, stderr,
    // result-JSON — ВКЛЮЧАЯ дефисную форму s3-pool-exhausted в error (детектор
    // ищет pool_exhausted с подчёркиванием — только stdout-маркер приёмника;
    // дефисный текст ошибки маркером не является).
    [Fact]
    public void PoolExhaustionDetector_ForeignLinesIgnored()
    {
        // Arrange — только посторонние строки
        var tail = "{\"phase\":\"starting\"}\n"
            + "{\"phase\":\"streaming\",\"wal_start\":\"0/8000000\"}\n"
            + "stderr: connection refused\n"
            + "{\"ok\":false,\"error\":\"connection refused\"}\n"
            + "{\"ok\":false,\"error\":\"s3-pool-exhausted: upload full failed on all pool endpoints\"}\n"
            + HeartbeatLine;

        // Act / Assert
        WalStreamProcess.IsPoolExhaustionActive(tail).Should().BeFalse();
    }
```

  Используй существующие using файла — новых не нужно. Тесты ссылаются на
  метод `WalStreamProcess.IsPoolExhaustionActive` — он появится в подпункте
  2.3 (пока не компилируется — это ожидаемо).

- [ ] **2.3. Реализовать детектор в `src/PgWorker.Backups/WalStreamProcess.cs`.**
  В файле `src/PgWorker.Backups/WalStreamProcess.cs` найди метод
  `ParseTliJumpMarker` (заканчивается строкой `return null; // чужая/битая строка — не маркер` и закрывающей `}`), и сразу ПОСЛЕ его закрывающей `}`
  (до метода `TliOfSegment`) добавь:

```csharp
    // t02 (arch/19 §3): актуальность исчерпания write-списка мастер-агента —
    // позиционный детектор по хвосту лога: маркер активен ⇔ найдена строка
    // pool_exhausted и ПОСЛЕДНЯЯ такая строка стоит ПОСЛЕ последней строки
    // heartbeat (heartbeat — единственный знак успешной доставки: подтверждение
    // слота после успешного put приёмника; успешная доставка после исчерпания
    // снимает маркер). Подстрочный матч по строке: единственный источник
    // подстроки pool_exhausted — stdout-маркер приёмника
    // {"pool_exhausted":true}; дефисная форма s3-pool-exhausted (тексты
    // ошибок, result-JSON) не матчится — посторонние строки; фазы
    // starting/streaming/delivering успехом НЕ считаются. Без таймеров и
    // порогов свежести — только позиции строк.
    internal static bool IsPoolExhaustionActive(string logTail)
    {
        var lastMarker = -1;
        var lastHeartbeat = -1;
        var lines = logTail.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].Contains("pool_exhausted", StringComparison.Ordinal))
                lastMarker = i;
            else if (lines[i].Contains("heartbeat", StringComparison.Ordinal))
                lastHeartbeat = i;
        }

        return lastMarker >= 0 && lastMarker > lastHeartbeat;
    }
```

  Больше в файле ничего не менять.

- [ ] **2.4. Скомпилировать и прогнать тесты детектора.**
  Из корня репозитория выполни по очереди:
  1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
     — ожидаемый результат: сборка успешна, 0 ошибок, 0 warnings;
  2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcessTests" > /tmp/t02-step2-unit.log 2>&1; echo "exit=$?"`
     — ожидаемый результат: `exit=0`, в конце `/tmp/t02-step2-unit.log` строка
     «Passed! - Failed: 0» и число Passed выросло на 5 (новые
     `PoolExhaustionDetector_*`); ни один существующий тест не стал красным.

**Коммит шага 2:** `feat(backups): WalStreamProcess — позиционный детектор актуальности pool_exhausted (снятие heartbeat'ом) + юнит-тесты`.

---

### Шаг 3. Контроль: детектор в t36-блоках, одно чтение лога, расписание exhaustion-DEGRADED (TDD)

**Вход:** шаг 2 закоммичен (детектор существует и зелёный).

**Действие** (сильная модель): три правки `WalStreamProcess.ControlDueAsync`
(spec п. 4.3.2, 4.3.3) + тесты контроля/расписания. Механика — подпункты
3.1–3.6; coder исполняет сам, ревьюит и сверяет с каноном arch/19 (шаг 1).

Ключевые инварианты правки (все решены spec, реализация предопределена):
- Чтение лога агентa — ОДИН docker-вызов `BackupAgentLogsAsync` за контрольный
  проход на оба t36-блока (решение 8): локальная переменная `poolLogTail`
  читается при первой необходимости (объединённое условие «state == Active»
  ИЛИ «переход в Degraded»), переиспользуется вторым блоком. Чтение
  tli_jump-блока (выше по коду) — ОТДЕЛЬНОЕ, его не трогаем (spec требует
  консолидацию только двух t36-блоков).
- Структура и тексты error обоих блоков НЕ меняются (spec: заменить только
  детект); в т.ч. сохраняется существующее поведение двойного префикса
  `s3-pool-exhausted: s3-pool-exhausted: …` при установке через оба блока
  (Active-ключ → блок 1 → блок 2 префиксует) — тесты ассертят `Contains`, не
  точную строку.
- Расписание: условие пропуска «не время» не действует для BROKEN (как было)
  и для exhaustion-DEGRADED — DEGRADED-ключ, чей `error` содержит
  `S3PoolMarkers.Exhausted` (п. 4.3.3). Обычные DEGRADED (lag/тишина) — по
  расписанию, как было (решение 7).
- Docker-фейл чтения лога (`poolLogTail == null`) — блоки не срабатывают:
  контроль не падает и не деградирует фантомно (семантика прежнего
  `IsSuccess`-гейта).

**Выход:** оба t36-блока работают на детекторе актуальности; лог читается
один раз за проход; exhaustion-DEGRADED контролируется каждым тиком; тесты
контроля, расписания и лог-фейла зелёные; существующие тесты
`WalStreamProcessTests` (вкл. `TliJump*`, `VerifyAtBrokenEveryTickHealsInOneTick`,
`VerifyLagAboveThresholdDegradedAc5a`) — зелёные (регресс).

**Проверка:**
1) сборка (отдельная команда): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — успешно;
2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcessTests" > /tmp/t02-step3-unit.log 2>&1; echo "exit=$?"` —
   `exit=0`, все зелёные (новые +3, существующие без регресса).

**Связь со spec:** п. 4.3.2, 4.3.3, 4.5 (контроль + расписание), фаза 3;
решения 1, 7, 8; дефекты A и C (§2.1, §2.3 spec).

**Подпункты (слабой модели; coder исполняет сам):**

- [ ] **3.1. Добавить флаг фейла чтения лога в
  `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`.**
  Найди в файле строки (метод `BackupAgentLogsAsync` и поле над ним):

```csharp
    public string BackupAgentLogTail = "";

    public Task<Result<string>> BackupAgentLogsAsync(string cluster, string containerName, int tailLines, CancellationToken ct)
        => Task.FromResult(Result<string>.Success(BackupAgentLogTail));
```

  Замени на:

```csharp
    public string BackupAgentLogTail = "";

    // t02: транспорт-фейл docker-чтения лога агента (transient — контроль не
    // должен падать тиком и деградировать фантомно).
    public bool FailBackupAgentLogs;

    public Task<Result<string>> BackupAgentLogsAsync(string cluster, string containerName, int tailLines, CancellationToken ct)
        => FailBackupAgentLogs
            ? Task.FromResult(Result<string>.Failed(new ApplicationException("agent logs unavailable")))
            : Task.FromResult(Result<string>.Success(BackupAgentLogTail));
```

- [ ] **3.2. Добавить юнит-тесты контроля/расписания в
  `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs`.**
  Сразу после тестов детектора из подпункта 2.2 (перед закрывающей `}` класса)
  добавь:

```csharp
    // ---- t02 (arch/19 §3): контроль — исчерпание write-списка и self-heal ----

    // AAA (контроль): фейковый лог мастер-агента с АКТИВНЫМ маркером
    // исчерпания → wal DEGRADED с маркером s3-pool-exhausted; пока доставки
    // нет — DEGRADED удерживается; heartbeat в логе ТОГО ЖЕ агента → контроль
    // возвращает ACTIVE с очищенным error (self-heal одним контрольным
    // проходом). Между тиками ключ ПЕРЕЧИТЫВАЕТСЯ из etcd (контроль берёт wal
    // из backups-аргумента тика — паттерн BrokenSurvivesProcessRestart);
    // VerifyIntervalSec=3600 дискриминирует расписание: без правила
    // «exhaustion-DEGRADED каждый тик» тики 2-3 были бы пропущены.
    [Fact]
    public async Task ExhaustionMarker_ThenHeartbeatHealsActive()
    {
        // Arrange — полный wal_start=..01, цепочка 1..3, ключ ACTIVE, слот
        // жив; лог агента: старый heartbeat, затем маркер (актуален)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "pe1";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver
        {
            BackupAgentLogTail = HeartbeatLine + "\n" + PoolExhaustedLine,
        };
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed),
            });

        // Свежий backups из ПЕРЕЧИТАННОГО ключа (как ReconcileLoop тика)
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        async Task<ClusterBackups> FreshBackupsAsync()
        {
            var reread = await reader.ReadAsync(cluster, "shard1", ct);
            return new ClusterBackups(cluster, null,
                new Dictionary<string, ShardBackups>
                {
                    ["shard1"] = new(FullShard("000000010000000000000001").Full, reread.Value),
                });
        }

        // Act — тик 1 (первый контроль): маркер актуален → DEGRADED с маркером
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var degraded = await ReadWal(cluster);
        degraded!.State.Should().Be(WalStreamStatus.Degraded);
        degraded.Error.Should().Contain("s3-pool-exhausted");

        // Act — тик 2 (ключ перечитан — exhaustion-DEGRADED, расписание 3600 с
        // НЕ наступило, но контроль выполняется): лог не изменился (доставки
        // нет) — DEGRADED удерживается
        (await process.TickAsync(BuildSnap(cluster), await FreshBackupsAsync(), ct)).IsSuccess.Should().BeTrue();
        var held = await ReadWal(cluster);
        held!.State.Should().Be(WalStreamStatus.Degraded);
        held.Error.Should().Contain("s3-pool-exhausted");

        // Act — тик 3: в логе ТОГО ЖЕ агента появился heartbeat ПОСЛЕ маркера
        // (успешная доставка — единственный знак снятия)
        driver.BackupAgentLogTail = PoolExhaustedLine + "\n" + HeartbeatLine;
        (await process.TickAsync(BuildSnap(cluster), await FreshBackupsAsync(), ct)).IsSuccess.Should().BeTrue();

        // Assert — self-heal одним контрольным проходом: ACTIVE, error очищен
        var healed = await ReadWal(cluster);
        healed!.State.Should().Be(WalStreamStatus.Active);
        healed.Error.Should().BeNull();
    }

    // AAA (расписание, образец VerifyAtBrokenEveryTickHealsInOneTick): ключ
    // exhaustion-DEGRADED (DEGRADED с маркером в error) + VerifyIntervalSec
    // =3600 — контроль выполняется ТЕМ ЖЕ тиком (heartbeat в логе уже снимает
    // маркер): заживление ACTIVE без ожидания интервала.
    [Fact]
    public async Task ExhaustionDegradedEveryTickHealsInOneTick()
    {
        // Arrange — сид: ключ DEGRADED с маркером; полный wal_start=..01;
        // цепочка 1..3; слот жив; лог агента уже исцелён (heartbeat после
        // маркера)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "pe2";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver
        {
            BackupAgentLogTail = PoolExhaustedLine + "\n" + HeartbeatLine,
        };
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var seed = LiveWal(cluster, state: WalStreamStatus.Degraded) with
        {
            Error = "s3-pool-exhausted: write list exhausted by master agent (receiver retry loop)",
        };
        await writer.WriteIfChangedAsync(cluster, "shard1", seed, ct);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, seed),
            });

        // Act — ОДИН тик (расписание 3600 с НЕ наступило — exhaustion-DEGRADED
        // контролирует каждый тик)
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE, error очищен
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().BeNull();
    }

    // AAA (Review Focus): docker-чтение лога мастер-агента упало (transient:
    // агент пересоздаётся) — контроль НЕ падает тиком и НЕ деградирует
    // фантомно: статус по общим правилам (цепочка/lag/тишина).
    [Fact]
    public async Task ExhaustionMarker_AgentLogReadFailure_NoPhantomDegradation()
    {
        // Arrange — полный wal_start=..01, цепочка 1..3, ключ ACTIVE, слот
        // жив; docker-чтение лога агента — транспорт-фейл
        var ct = TestContext.Current.CancellationToken;
        var cluster = "pe3";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver { FailBackupAgentLogs = true };
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed),
            });

        // Act — тик контроля (лог агента недоступен)
        var result = await process.TickAsync(BuildSnap(cluster), backups, ct);

        // Assert — тик успешен; ключ ACTIVE без маркера (лог-фейл — не
        // деградация, transient повторит следующий контрольный проход)
        result.IsSuccess.Should().BeTrue();
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().BeNull();
    }
```

  Используемые хелперы (`SeedAsync`, `SeedSlot`, `LiveWal`, `FullShard`,
  `BuildSnap`, `BuildProcess`, `ReadWal`, `Options`, константы
  `HeartbeatLine`/`PoolExhaustedLine`) уже есть в этом файле выше — новых
  using не нужно.

- [ ] **3.3. Правка расписания в `src/PgWorker.Backups/WalStreamProcess.cs`
  (метод `ControlDueAsync`).**
  Найди в `ControlDueAsync` начало метода:

```csharp
        var key = $"{cluster}/{shard}";
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        // Расписание — пропуск только для НЕ-BROKEN (t07: BROKEN контролирует
        // каждый тик — заживление одним тиком после пересъёма полного).
        if (wal is not { State: WalStreamStatus.Broken }
            && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal);
```

  Замени на:

```csharp
        var key = $"{cluster}/{shard}";
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        // Расписание — пропуск только для НЕ-BROKEN (t07) и НЕ-exhaustion-DEGRADED
        // (t02: заживление подъёмом ноды пула — контроль каждый тик, симметрично
        // BROKEN; union-листинг — один класс вызовов).
        if (wal is not { State: WalStreamStatus.Broken }
            && !IsExhaustionDegraded(wal)
            && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal);
```

  И в том же файле найди комментарий над полем `_lastVerifyUnix`:

```csharp
    // Расписание контроля (list S3 — не каждый тик): ключ cluster/shard → unix последнего прохода.
    // При BROKEN-ключе шарда расписание НЕ действует — контроль каждый тик (t07).
```

  Замени вторую строку комментария на (первую не трогай):

```csharp
    // При BROKEN-ключе шарда или exhaustion-DEGRADED расписание НЕ действует —
    // контроль каждый тик (t07/t02).
```

- [ ] **3.4. Замена обоих t36-блоков на детектор с одним чтением лога в
  `src/PgWorker.Backups/WalStreamProcess.cs` (метод `ControlDueAsync`).**
  Найди в `ControlDueAsync` ДВА подряд идущих блока после веток деградаций
  (первый начинается комментарием «t36 (arch/19 §3): исчерпание
  write-списка мастер-агента — контроль КАЖДЫЙ тик верификации», второй —
  комментарием «t36 (arch/19 §3): переход в DEGRADED — хвост лога
  мастер-агента»):

```csharp
        // t36 (arch/19 §3): исчерпание write-списка мастер-агента — контроль
        // КАЖДЫЙ тик верификации, НЕЗАВИСИМО от lag/тишины: «тишина» считается
        // по последнему объекту пула (ReadAll) и маскируется прогрессом
        // sync-источника (write_replica на живой ноде) — разбор AC4: мастер-агент
        // в ретрай-цикле исчерпания, wal-статус оставался Active. Хвост лога
        // мастер-агента содержит маркер pool_exhausted приёмника → DEGRADED с
        // s3-pool-exhausted (алерт backup-pool-unreachable); docker-вызов — по
        // интервалу верификации, не ежетиковый.
        if (state == WalStreamStatus.Active)
        {
            var agentLogs = await driver.BackupAgentLogsAsync(cluster,
                BackupAgentNames.Container(cluster, shard, masterRef), AgentLogTail, ct);
            if (agentLogs.IsSuccess && agentLogs.Value.Contains("pool_exhausted", StringComparison.Ordinal))
            {
                state = WalStreamStatus.Degraded;
                error = $"{S3PoolMarkers.Exhausted}: write list exhausted by master agent (receiver retry loop)";
            }
        }

        // t36 (arch/19 §3): переход в DEGRADED — хвост лога мастер-агента: маркер
        // pool_exhausted приёмника (ретрай-цикл исчерпания списка пула) → error
        // несёт s3-pool-exhausted (алерт backup-pool-unreachable). Только на
        // переходе (не каждый тик — docker-вызов дёшев, но не ежетиковый).
        if (state == WalStreamStatus.Degraded && wal is not { State: WalStreamStatus.Degraded })
        {
            var logs = await driver.BackupAgentLogsAsync(cluster,
                BackupAgentNames.Container(cluster, shard, masterRef), AgentLogTail, ct);
            if (logs.IsSuccess && logs.Value.Contains("pool_exhausted", StringComparison.Ordinal))
                error = $"{S3PoolMarkers.Exhausted}: {error}";
        }
```

  Замени ОБА блока (от первого комментария до закрывающей `}` второго блока
  включительно) на:

```csharp
        // t36/t02 (arch/19 §3): хвост лога мастер-агента — ОДИН docker-вызов за
        // контрольный проход: читается при первой необходимости (блоки ниже),
        // переиспользуется вторым блоком (чтение tli_jump-маркера выше —
        // отдельное, не консолидируется). Чтение упало — tail остаётся null:
        // блоки пропускаются (transient, не деградация).
        string? poolLogTail = null;
        if (state == WalStreamStatus.Active
            || (state == WalStreamStatus.Degraded && wal is not { State: WalStreamStatus.Degraded }))
        {
            var agentLogs = await driver.BackupAgentLogsAsync(cluster,
                BackupAgentNames.Container(cluster, shard, masterRef), AgentLogTail, ct);
            if (agentLogs.IsSuccess)
                poolLogTail = agentLogs.Value;
        }

        // t36/t02 (arch/19 §3): исчерпание write-списка мастер-агента — контроль
        // КАЖДЫЙ тик верификации, НЕЗАВИСИМО от lag/тишины: «тишина» считается
        // по последнему объекту пула (ReadAll) и маскируется прогрессом
        // sync-источника (write_replica на живой ноде). Детект t02 —
        // АКТУАЛЬНОСТЬ маркера: старые строки pool_exhausted в хвосте не
        // деградируют восстановившийся поток — heartbeat после маркера снимает
        // (self-heal одним проходом); алерт backup-pool-unreachable.
        if (state == WalStreamStatus.Active && poolLogTail is not null
            && IsPoolExhaustionActive(poolLogTail))
        {
            state = WalStreamStatus.Degraded;
            error = $"{S3PoolMarkers.Exhausted}: write list exhausted by master agent (receiver retry loop)";
        }

        // t36/t02 (arch/19 §3): переход в DEGRADED — АКТУАЛЬНЫЙ маркер
        // исчерпания → error несёт префикс s3-pool-exhausted (алерт
        // backup-pool-unreachable). Только на переходе (не каждый тик —
        // docker-вызов дёшев, но не ежетиковый).
        if (state == WalStreamStatus.Degraded && wal is not { State: WalStreamStatus.Degraded }
            && poolLogTail is not null && IsPoolExhaustionActive(poolLogTail))
        {
            error = $"{S3PoolMarkers.Exhausted}: {error}";
        }
```

  ВНИМАНИЕ: блок чтения лога для tli_jump-маркера выше по методу (внутри
  `if (gap is null) { var agentLogTail = ... }`) НЕ трогать — он остаётся как
  есть, отдельным чтением.

- [ ] **3.5. Добавить helper `IsExhaustionDegraded` в
  `src/PgWorker.Backups/WalStreamProcess.cs`.**
  Сразу ПОСЛЕ метода `IsPoolExhaustionActive` (добавленного в подпункте 2.3,
  до метода `TliOfSegment`) добавь:

```csharp
    // t02 (arch/19 §3): exhaustion-DEGRADED — DEGRADED-ключ с маркером
    // s3-pool-exhausted в error (исчерпание write-списка приёмника) — контроль
    // КАЖДЫЙ тик, симметрично BROKEN (скорость заживления; union-листинг —
    // один класс вызовов).
    private static bool IsExhaustionDegraded(WalStreamState? wal)
        => wal is { State: WalStreamStatus.Degraded }
            && wal.Error?.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal) == true;
```

- [ ] **3.6. Скомпилировать и прогнать тесты.**
  Из корня репозитория по очереди:
  1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
     — ожидаемый результат: сборка успешна, 0 ошибок, 0 warnings;
  2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcessTests" > /tmp/t02-step3-unit.log 2>&1; echo "exit=$?"`
     — ожидаемый результат: `exit=0`, «Passed! - Failed: 0»; новые
     `ExhaustionMarker_ThenHeartbeatHealsActive`,
     `ExhaustionDegradedEveryTickHealsInOneTick`,
     `ExhaustionMarker_AgentLogReadFailure_NoPhantomDegradation` зелёные;
     все существующие тесты `WalStreamProcessTests` зелёные (регресса нет).

**Коммит шага 3:** `fix(backups): WalStreamProcess — детектор актуальности pool_exhausted в t36-блоках, одно чтение лога за проход, контроль каждый тик при exhaustion-DEGRADED`.

---

### Шаг 4. Entrypoint: `--checkpoint=fast` в `docker/backup/entrypoint.sh`

**Вход:** шаг 3 закоммичен.

**Действие** (механика — подпункт 4.1; coder проверяет, что больше нигде в
проекте `checkpoint=spread` не встречается). Правка — одна строка скрипта
(строка 24); оба образа (`PgWorker.Backup.Dockerfile`,
`PgWorker.Backup.E2E.Dockerfile`) собирают этот один скрипт — больше нигде не
менять (spec п. 4.4, решение 14). Скрипт не компилируется — юнит-покрытия нет
(осознанно, spec п. 4.5: «checkpoint-флаг entrypoint — не юнитится (скрипт);
покрытие — E2E»).

**Выход:** entrypoint снимает полные с `--checkpoint=fast`: wal_start-сегмент
входит в набор `-X stream` и дублируется в `wal/` — verify-цепь сходится
(дефект B закрыт; механизм — решение 6 spec).

**Проверка:** `grep -rn "checkpoint=spread" docker/ src/` → пусто;
`grep -n "checkpoint=fast" docker/backup/entrypoint.sh` → строка 24.

**Связь со spec:** п. 4.4, фаза 4; решения 6, 14; дефект B (§2.2).

**Подпункты (слабой модели; coder исполняет сам):**

- [ ] **4.1. Заменить флаг checkpoint в `docker/backup/entrypoint.sh`.**
  В файле `docker/backup/entrypoint.sh` найди строку 24:

```sh
pg_basebackup -d "$PGW_BK_DSN" -D "$STAGING/full" -X stream --checkpoint=spread --manifest-checksums=SHA256 \
```

  Замени в ней `--checkpoint=spread` на `--checkpoint=fast`. Итоговая строка:

```sh
pg_basebackup -d "$PGW_BK_DSN" -D "$STAGING/full" -X stream --checkpoint=fast --manifest-checksums=SHA256 \
```

  Больше в файле ничего не менять.

**Коммит шага 4:** `fix(backup): pg_basebackup --checkpoint=fast — wal_start входит в набор -X stream, verify-цепь сходится (нет вечных пересъёмов)`.

---

### Шаг 5. Серии: сборка Release, юниты, E2ePrepare, docker-E2E с зачисткой

**Вход:** шаги 1–4 закоммичены.

**Действие** (сильная модель — наблюдение за прогонами, разбор, зачистка;
подпунктов нет: серии требуют мониторинга и суждения по логам). Все команды —
из корня worktree, каждой серии предшествует СВОЯ сборка/подготовка по
правилам; вывод каждой команды пишется ПОЛНОСТЬЮ в файл лога; запуск — в фоне
с фиксацией task_id; мониторинг: старт в течение 15 с по логу, далее проверка
каждые 2 минуты; тесты с фильтром E2e* требуют `PGW_TEST_DOCKER=1`; после
КАЖДОЙ серии — зачистка: `docker ps -a --filter name=pgw-` пусто; осиротевшие
сети (`docker network ls | grep -E 'kfw-net|pgw-.*-net'` при нуле
контейнеров) — `docker network prune -f`.

Порядок серий (между сериями — дожидаться финальной строки прогона, затем
зачистка; НИКОГДА не запускать следующую серию поверх незачищенной):

1. Сборка (отдельная команда, свежесть бинаря видна её выводом):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t02-s5-build.log 2>&1; echo "exit=$?"` — `exit=0`.
2. Юниты (без docker):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcessTests" > /tmp/t02-s5-unit.log 2>&1; echo "exit=$?"` — `exit=0`, все зелёные.
3. Статическая подготовка E2E ОДНИМ процессом ДО тестов:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/t02-s5-prepare.log 2>&1; echo "exit=$?"` — `exit=0`, штамп готовности записан. Зачистка сетей перед docker-сериями: `docker network prune -f`.
4. AC4 — ДВА повторных прогона (критерий 8.2), между ними зачистка:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Pool_FailoverList_And_Exhaustion_SelfHeal > /tmp/t02-s5-ac4-run1.log 2>&1; echo "exit=$?"`
   затем ТА ЖЕ команда с логом `run2`. Ожидание: оба `exit=0`.
5. Пул-регресс AC1–AC8 (критерий 8.3):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2e.Pool" > /tmp/t02-s5-pool.log 2>&1; echo "exit=$?"` — `exit=0` (фильтр покрывает классы PoolNotSelected/PoolMasterReplicaSplit/PoolSingleNode/PoolFailoverExhaustion/PoolRoleSwitch/PoolReadAnyNode/PoolSnapshotsOwnPair/PoolPolicyValidation — все E2e.Pool*).
6. WAL-стрим серии:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2eWalStream" > /tmp/t02-s5-walstream.log 2>&1; echo "exit=$?"` — `exit=0`.
7. Кейс-маркер:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/t02-s5-scale.log 2>&1; echo "exit=$?"` — `exit=0`.
8. Разбор артефактов зелёного AC4-прогона (критерий 8.4, БЕЗ перезапуска
   тестов): в артефактах teardown (`$TMPDIR/pgw-e2e-artifacts-*`) по
   host.log/docker-логам воркера подтвердить: контрольный переход
   `Degraded→Active` шарда после подъёма ноды; полный бэкап достиг
   `verify.state=OK`; новых пересъёмов полных в пределах `full_max_age_sec`
   нет. Каждая фаза прогона дольше 60 с — строка `[PHASE]` в журнале
   (телеметрия E2E — правила AGENTS.md).

Упавший тест: teardown помечает контур `MarkFailed()` — контейнер
останавливается, но не удаляется; разбор по `README-cleanup.txt` из
артефактов, ручная зачистка ПОСЛЕ разбора. Перезапуск упавших тестов без
полного анализа логов ЗАПРЕЩЁН. Фейл серии вне области задачи — пункт в
`arch/roadmap/bugs.md` (файл трека — правила ведения), работа над задачей
продолжается.

**Выход:** все серии зелёные; артефакты AC4 подтверждают self-heal и
отсутствие пересъёмов.

**Проверка:** `exit=0` каждой серии; п. 8 выполнен и зафиксирован в журнале
задачи (артефакты + вывод grep по логам воркера).

**Связь со spec:** фаза 5; критерии 8.1–8.4; §6 правила E2E.

**Коммит шага 5:** правок кода нет (серии только верифицируют); при
необходимости точечных правок после разбора — отдельные коммиты с повтором
затронутых серий.

---

### Шаг 6. Мерж-гейт: удаление пункта t02 из роадмапа (тем же коммитом, что и мерж в main)

**Вход:** шаг 5 завершён, все серии зелёные; пользователь ЯВНО потребовал
мерж в main (без этого шаг не исполняется — AGENTS.base п. 7).

**Действие** (сильная модель):
1. В `arch/roadmap/bugs.md` удалить пункт `t02-pool-failover-selfheal`
   ЦЕЛИКОМ (текущие строки 48–66: от строки `- **\`t02-pool-failover-selfheal\`** — E2E-самолечение пула после исчерпания не`
   до строки `воркер не выходит из деградации после подъёма ноды … host.log в артефактах).` включительно).
   Из списков/`←`-зависимостей других пунктов тег `t02-pool-failover-selfheal`
   удалить, если где-то остался (проверка: `grep -rn "t02-pool-failover-selfheal" arch/roadmap/`).
2. Удалить локальную подпапку логов
   `arch/roadmap/bugs/t02-pool-failover-selfheal/`, ЕСЛИ она существует
   локально (правило трека bugs: пункт удаляется вместе со своей подпапкой).
   Подпапка ВНЕ git (путь упомянут в тексте пункта, но в worktree её нет —
   артефакты прогона лежат в `$TMPDIR`, spec п. 4.6); отсутствие подпапки —
   не ошибка, шаг просто пропускается.
3. Правки arch/19 (шаг 1) уже в ветке — входят в мерж-коммит; история ветки
   подтверждает канон до кода (критерий 8.5).

**Выход:** пункт t02 удалён из роадмапа тем же коммитом, что и мерж в main;
локальная подпапка логов (если была) удалена.

**Проверка:** в мерж-коммите diff `arch/roadmap/bugs.md` содержит только
удаление пункта t02; `grep -c "t02-pool-failover-selfheal" arch/roadmap/bugs.md` → 0;
`ls arch/roadmap/bugs/t02-pool-failover-selfheal/ 2>/dev/null` — пусто (подпапки нет).

**Связь со spec:** п. 4.6; критерий 8.5.

---

## Самопроверка плана (исполнена составителем; правки ревью Фазы 4 учтены)

- **Покрытие spec:** 4.1→шаг 1; 4.2→шаг 1; 4.3.1→шаг 2; 4.3.2→шаг 3
  (подп. 3.4); 4.3.3→шаг 3 (подп. 3.3/3.5); 4.4→шаг 4; 4.5→шаги 2–3
  (детектор/контроль/расписание) + шаг 5 (checkpoint — E2E-покрытие,
  сценарий AC4 не меняется); 4.6→шаг 6; фазы 1–5→шаги 1–5; решения 1–14
  отражены (2/3/4→детектор; 7→расписание; 8→одно чтение; 6/14→entrypoint;
  13→файл тестов; 9→AC4 без правок, гейт `healed`; 10/11/12→вне области,
  шагов нет). Шагов вне scope spec нет.
- **Развилки:** имена/сигнатуры зафиксированы (`IsPoolExhaustionActive` — из
  spec; `IsExhaustionDegraded(WalStreamState?)`, `poolLogTail`,
  `FailBackupAgentLogs`, имена тестов — в подпунктах); матч детектора —
  подстрочный по строке `pool_exhausted` (подчёркивание; единственный
  источник — stdout-маркер приёмника `{"pool_exhausted":true}`; дефисная
  форма `s3-pool-exhausted` в result-JSON/текстах ошибок НЕ матчится —
  посторонние строки, spec п. 4.3.1); тексты error и структура условий
  t36-блоков — без изменений (двойной префикс при установке через оба блока —
  существующее поведение); InternalsVisibleTo — установленный паттерн проекта
  (`PgWorker.Docker` → `PgWorker.IntegrationTests`), без него тесты детектора
  в требуемом spec (решение 13) файле невозможны; место вставки в arch/19 §3 —
  абзац «Правила непрерывности» (единственный абзац семантики контроля
  DEGRADED/маркера). Точек выбора при реализации не остаётся.
- **Типы/сигнатуры:** сверены с кодом: `ControlDueAsync` (WalStreamProcess.cs
  строки 682–908), `BackupAgentLogsAsync(cluster, containerName, tailLines,
  ct)` → `Result<string>`, `BackupAgentNames.Container(cluster, shard, node)`,
  `S3PoolMarkers.Exhausted` = "s3-pool-exhausted" (дефис), `TryClaimClusterAsync`
  → `Task<Result<bool>>` (ассерт — `.Value.Should().BeTrue(...)`, паттерн
  WalStreamProcessTests.cs), `WalStreamState.Error` — `string?`,
  `WalStreamStatus.{Active,Degraded,Broken}`, фейковые хелперы
  (`SeedAsync`, `SeedSlot`, `LiveWal`, `FullShard`, `Options(verify:)`,
  `BuildSnap`, `ReadWal`) — существуют в WalStreamProcessTests.cs;
  `StubScaleDriver.BackupAgentLogTail` — мутабельное поле.
- **E2E-гигиена:** сборка отдельной командой; `--no-build` + env-префикс при
  команде; E2ePrepare до серий; зачистка после каждой серии; перезапуск
  упавших — только после разбора логов.
