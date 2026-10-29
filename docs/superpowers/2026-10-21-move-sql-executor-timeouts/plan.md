# План: NpgsqlMoveSqlExecutor — SQL-таймауты полокна watchdog

> **Для исполняющих агентов:** план исполняет СИЛЬНАЯ модель (по умолчанию
> main-агент) по шагам ниже; механическая часть (подпункты) отдаётся субагенту
> coder-flash — слабой модели БЕЗ контекста проекта. Каждый подпункт
> самодостаточен: субагенту передаётся ТОЛЬКО его текст. Шаги отмечаются
> чекбоксами (`- [ ]`).

**Цель:** привести `NpgsqlMoveSqlExecutor` к инварианту поллинга arch/14 §6 —
пер-попыточные SQL-таймауты (connect и command = 7 с при дефолтах) во ВСЕХ
четырёх методах класса, симметрично `DatabaseProvisioner` и
`NpgsqlWalSqlExecutor`.

**Архитектура:** правится один файл — добавляются константа `SqlTimeoutSec = 7`
и два хелпера (`OpenConnection`/`Command`) по образцу соседей; три Polly-метода
переводятся на хелперы; транзакция заморозки получает connect-таймаут 7 с и
command-бюджет `Math.Max(SqlTimeoutSec, lockTimeoutSec + 2)`. Интерфейс,
Polly-параметры, `WrapError`/`Redact`, вызывающий код и arch/ — без изменений.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
Npgsql, Polly (`SqlRetryPolicies.SqlRetry`).

**Spec:** `docs/superpowers/2026-10-21-move-sql-executor-timeouts/spec.md`
(ветка `fix-move-sql-executor-timeouts`; план аргументирует от spec —
исполнитель читает оба).

## Глобальные ограничения (из spec, действуют на все шаги)

- Код правится ТОЛЬКО в worktree
  `/Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts`;
  единственный файл кода — `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`
  (spec §5, §7.6).
- `arch/` НЕ трогается (spec §6.4): инвариант уже канонизирован в arch/14 §6.
- НЕ меняются: `IMoveSqlExecutor`, Polly-параметры (`RetryCount = 3`,
  `FirstRetryDelay = 1 s`), `WrapError`/`Redact`/`PasswordRegex`, тест
  `MoveSqlExecutorTests.WrapError_MasksPassword`, вызывающий код
  (`CutoverSequence`, `MoveProcess`) — spec §3.4, §6.5.
- Новых тестов НЕ добавляется (spec §6.7); приёмка — код-ревью диффа + гейт.
- Комментарии — по-русски, идентификаторы — по-английски, в тон окружающего
  текста файла (spec §5).
- Все команды гейта — отдельными запусками из корня worktree; ПОЛНЫЙ вывод
  каждой команды пишется в файл (`> <лог> 2>&1`), анализ — чтением из файла
  (базовое правило 15). Логи этого плана: `/tmp/pgw-plan-*.log`.
- Долгие команды (сборка, тесты, E2E) — только в фоне (`run_in_background`),
  с контролем старта в течение 15 с и проверкой отсутствия зависания каждые
  2 минуты; sleep/поллинг агента — не более 30 с (базовые правила 4, 12, 13).

## Фокус ревью

Spec запрещает новые тесты (§6.7), поэтому классы отказов ниже закрыты
ревью-чеками шага 4 и гейтом шагов 2–3, а не юнит-тестами:

1. **Молчание connect** (зависший docker-proxy: TCP принимает, ответа нет) —
   закрыто `Timeout = 7` в CSB у ВСЕХ четырёх методов, включая
   `ExecuteTransactionalAsync` (шаг 1, ревью-чек 4.2).
2. **Молчание command** (медленный/зависший SQL) — закрыто `CommandTimeout = 7`
   в трёх Polly-методах (ревью-чек 4.3).
3. **Ломка семантики заморозки при `FreezeLockTimeoutSec > 5`** (command-timeout
   срезал бы LOCK-барьер раньше lock_timeout) — закрыто формулой
   `Math.Max(SqlTimeoutSec, lockTimeoutSec + 2)` (ревью-чек 4.4; решение
   spec §6.3).
4. **Регрессии SQL-пути moves на реальных переездах** — закрыто полным E2E-прогоном
   `E2eFixture` (шаг 3; решение spec §6.8).
5. **Выход за скоуп** (случайная правка соседних файлов/арх) — закрыто
   `git diff --stat` ровно один файл (ревью-чек 4.1; spec §7.6).

## Роли исполнения

- Шаги 1–5 исполняет сильная модель; всё, что не выделено в подпункты
  (ревью диффа, прогоны тестов/E2E, мониторинг, зачистка, документация), —
  сама.
- Подпункты (шаги 1а–1д, 2а) — субагент coder-flash, каждому передаётся
  ТОЛЬКО текст подпункта; субагент не получает ни spec, ни план, ни контекст.
- По явной просьбе пользователя — субагент coder исполняет всё сам.

---

## Шаг 1. Правка `NpgsqlMoveSqlExecutor.cs`: константа, хелперы, таймауты в 4 методах

- [ ] **Шаг 1** (подпункты 1а–1д — coder-flash; ревью диффа — сильная модель)

**Вход:** worktree
`/Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts` на
ветке `fix-move-sql-executor-timeouts`, рабочее дерево чистое (кроме каталога
`docs/superpowers/`); файл `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`
в исходном виде: в трёх Polly-методах (строки 21–91) нет
`Timeout`/`CommandTimeout`; единственные упоминания — параметр
`lockTimeoutSec` и `SET LOCAL lock_timeout` транзакции.

**Действие (сильная модель):** передать субагенту coder-flash подпункты 1а,
1б, 1в, 1г, 1д ОДНИМ заданием, пункты подряд и в этом порядке (правки не
пересекаются; 1а строго первым: 1б–1д используют его хелперы). По завершении
субагента — принять работу и сделать ревью диффа по чек-листу:

- 1.1. `git -C <worktree> diff src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`:
  изменения ТОЛЬКО перечисленные ниже (см. ревью-чеки шага 4).
- 1.2. Комментарии на месте и по-русски; канон-комментарий arch/14 §6 у
  константы — дословно как в подпункте 1а.
- 1.3. Если coder-flash отклонился от текста подпункта — сильная модель
  правит сама (исправления после ревью — её работа).

**Выход:** файл `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs` соответствует
spec §3.1–3.3: константа + 2 хелпера; `ScalarAsync`/`ListAsync`/`ExecuteAsync`
на `OpenConnection`/`Command`; `ExecuteTransactionalAsync` с
`bodyTimeoutSec = Math.Max(SqlTimeoutSec, lockTimeoutSec + 2)`.

**Проверка:** `git -C <worktree> diff --stat` — изменён ровно один файл;
визуальная сверка диффа с §3 spec (чек-лист 1.2; компиляция — шаг 2).

**Связь со spec:** §3.1 (константа/хелперы), §3.2 (три Polly-метода), §3.3
(транзакция), §6.1 (значение 7), §6.2 (охват — все 4 метода, подтверждено
пользователем на user-review 2026-10-21), §6.3 (формула bodyTimeoutSec).

### Подпункты для слабой модели (coder-flash)

**1а. Добавить константу и два хелпера в файл
`/Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts/src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`.**

Что сделать: добавить (не заменяя существующее) приведённый ниже блок. Место:
СРАЗУ ПОСЛЕ строки

```csharp
    private static readonly TimeSpan FirstRetryDelay = TimeSpan.FromSeconds(1);
```

и одной пустой строки после неё, ПЕРЕД комментарием
`// Скалярный запрос (префлайт/пробы/счётчики; DBNull → null).` Вставляемый
блок (с пустой строкой после него перед этим комментарием):

```csharp
    // Поллинг-инвариант (arch/14 §6): одиночная SQL-операция не молчит дольше
    // окна проверки watchdog — таймауты подключения и команды = половина окна
    // (7 c при дефолтах 15 c); зависшая операция → исключение → transient-фейл,
    // а не молчание до дефолтных Npgsql 15/30 c.
    private const int SqlTimeoutSec = 7;

    private static NpgsqlConnection OpenConnection(string dsn)
    {
        var csb = new NpgsqlConnectionStringBuilder(dsn) { Timeout = SqlTimeoutSec };
        return new NpgsqlConnection(csb.ConnectionString);
    }

    private static NpgsqlCommand Command(string sql, NpgsqlConnection conn, int commandTimeoutSec)
        => new(sql, conn) { CommandTimeout = commandTimeoutSec };

```

Ничего больше в файле не менять. Имена `SqlTimeoutSec`, `OpenConnection`,
`Command` — ровно такие, как в блоке.

**1б. Замена в методе `ScalarAsync` того же файла.**

Что сделать: внутри метода `ScalarAsync` (первый метод класса; ищется по
строке `return await cmd.ExecuteScalarAsync(token);` — она встречается в файле
ровно один раз) заменить блок

```csharp
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                return await cmd.ExecuteScalarAsync(token);
```

на блок

```csharp
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
                return await cmd.ExecuteScalarAsync(token);
```

Остальное тела метода (Polly-обёртка, catch, маппинг DBNull) не трогать.

**1в. Замена в методе `ListAsync` того же файла.**

Что сделать: внутри метода `ListAsync` (ищется по строке
`await using var reader = await cmd.ExecuteReaderAsync(token);` — встречается
в файле ровно один раз) заменить блок

```csharp
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await using var reader = await cmd.ExecuteReaderAsync(token);
```

на блок

```csharp
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
                await using var reader = await cmd.ExecuteReaderAsync(token);
```

Остальное тела метода не трогать.

**1г. Замена в методе `ExecuteAsync` того же файла.**

Что сделать: внутри метода `ExecuteAsync` (метод БЕЗ параметра-скаляра и БЕЗ
reader; ищется по строке `await cmd.ExecuteNonQueryAsync(token);` — переменная
`cmd` с токеном `token`; в других методах имена `setup`/`body` и токен `ct`)
заменить блок

```csharp
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync(token);
```

на блок

```csharp
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
                await cmd.ExecuteNonQueryAsync(token);
```

Остальное тела метода не трогать.

**1д. Замена метода `ExecuteTransactionalAsync` целиком в том же файле.**

Что сделать: заменить ВЕСЬ метод `ExecuteTransactionalAsync` (от его
комментария-дока `// Транзакция заморозки: SET LOCAL lock_timeout и тело батча — в ОДНОЙ`
до закрывающей скобки блока `catch` включительно) на приведённый ниже код.
Комментарий-док метода сохраняется дословно; меняются: создание подключения,
появление переменной `bodyTimeoutSec` с комментарием, создание команд `setup`
и `body` через хелпер `Command`. Обе команды получают ОДИН И ТОТ ЖЕ таймаут
`bodyTimeoutSec`:

```csharp
    // Транзакция заморозки: SET LOCAL lock_timeout и тело батча — в ОДНОЙ
    // транзакции (барьер LOCK обязан видеть те же REVOKE, что и читатели);
    // без Polly: LOCK сам ждёт lock_timeout, внешние ретраи — задача процесса
    // (FreezeLockTries, пауза PollIntervalSec).
    public async Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct)
    {
        try
        {
            await using var conn = OpenConnection(dsn);
            await conn.OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            // Command-бюджет обязан ПОКРЫВАТЬ lock_timeout: LOCK-барьер сам
            // ждёт lock_timeout (ретраи — задача процесса, не SQL-слоя); +2 —
            // запас тела батча (SET LOCAL + REVOKE×3); нижняя граница 7 —
            // поллинг-инвариант arch/14 §6 (дефолтный FreezeLockTimeoutSec = 5
            // → ровно 7 с, симметрия с остальными методами).
            var bodyTimeoutSec = Math.Max(SqlTimeoutSec, lockTimeoutSec + 2);
            await using (var setup = Command($"SET LOCAL lock_timeout = '{lockTimeoutSec}s'", conn, bodyTimeoutSec))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            await using (var body = Command(sql, conn, bodyTimeoutSec))
            {
                await body.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return WrapError(dsn, e);
        }
    }
```

Другие методы файла не трогать.

---

## Шаг 2. Сборка Release + юнит-тесты + коммит

- [ ] **Шаг 2** (подпункт 2а — coder-flash; юниты и коммит — сильная модель)

**Вход:** шаг 1 принят (дифф прошёл ревью сильной модели).

**Действие:**
- Подпункт 2а (сборка) — субагенту coder-flash; при результате, отличном от
  «0 error, 0 warning», сборку НЕ чинить самому субагенту — вернуть вывод
  сильной модели (разбор ошибок — её работа).
- Юнит-тесты — сильная модель сама (это не механика для слабой модели):
  запустить в фоне из корня worktree:

  ```
  DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~PgWorker.UnitTests > /tmp/pgw-plan-unit.log 2>&1
  ```

  В течение 15 с убедиться по логу, что прогон стартовал; далее контролировать
  каждые 2 минуты; падение — разбор по логу, исправление — сильной моделью
  (правки только в рамках шага 1/spec).
- Коммит (сильная модель, feature-ветка — коммит свободен):

  ```
  git -C /Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts add src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs
  git -C /Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts commit -m "fix(moves): пер-попыточные SQL-таймауты NpgsqlMoveSqlExecutor — полокна окна watchdog (arch/14 §6): Timeout/CommandTimeout = 7 с во всех 4 методах; транзакция заморозки — command-бюджет Math.Max(7, lockTimeoutSec+2) покрывает lock_timeout (дефолт FreezeLockTimeoutSec=5 → ровно 7 с); симметрия DatabaseProvisioner/NpgsqlWalSqlExecutor"
  ```

**Выход:** Release-сборка зелёная без варнингов; все юниты
`PgWorker.UnitTests` зелёные (включая `MoveSqlExecutorTests.WrapError_MasksPassword`
— без изменений, `CutoverSequenceTests`/`MoveProcessCutoverTests`/`MoveDdlTests`
— на фейках, дифф их не задевает); изменения закоммичены.

**Проверка:** `/tmp/pgw-plan-build.log` — `Build succeeded`, `0 Error(s)`,
`0 Warning(s)`; `/tmp/pgw-plan-unit.log` — `Passed!`, 0 failed;
`git log -1 --stat` — один файл в коммите.

**Связь со spec:** §4 фаза 2, критерии приёмки 1–2.

### Подпункты для слабой модели (coder-flash)

**2а. Запустить сборку решения в фоне и зафиксировать результат.**

Что сделать: из каталога
`/Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts`
выполнить команду (целиком, одной строкой) СТРОГО В ФОНОВОМ РЕЖИМЕ — запуск
и возврат управления сразу, БЕЗ блокирующего ожидания завершения:

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-plan-build.log 2>&1
```

Сразу зафиксировать идентификатор фоновой задачи. Контроль завершения —
циклом коротких проверок: посмотреть статус задачи по идентификатору и хвост
`/tmp/pgw-plan-build.log`; задача ещё выполняется — подождать не более 30
секунд и проверить снова (каждое одиночное ожидание — не более 30 секунд);
задача завершена — перейти к чтению лога. Полный вывод команды уже пишется в
`/tmp/pgw-plan-build.log`; читать и анализировать — ТОЛЬКО из этого файла,
никаких `tail -5` поверх и никакого блокирующего ожидания «минутами».

Ожидаемый результат: задача завершена с exit code 0, в
`/tmp/pgw-plan-build.log` есть `Build succeeded`, строки счётчиков —
`0 Error(s)` и `0 Warning(s)`. Прочитать последние 40 строк лога из файла и
сообщить их как результат. Если exit code не 0 или в логе есть
`error`/`warning` — НЕ исправлять код, сообщить те строки лога, где
встречаются `error` или `warning`.

---

## Шаг 3. E2E-гейт: статподготовка, smoke-маркер, полный прогон E2eFixture, зачистка

- [ ] **Шаг 3** (исполняет сильная модель; подпунктов нет — наблюдение за
  тестами и зачистка требуют понимания контекста)

**Вход:** шаг 2 зелёный (свежий Release-бинарь собран ОТДЕЛЬНОЙ командой
сборки, юниты зелёные, коммит создан).

**Действие:** из корня worktree, в фоне, ПО ОЧЕРЕДИ (каждая команда — отдельно;
следующая только после дожидания финальной строки прогона предыдущей и её
разбора; лог каждой — в свой файл):

1. Статическая подготовка E2E одним процессом ДО тестов (без неё ВСЕ E2E-классы
   PgWorker падают пачкой «статика E2E не подготовлена»):

   ```
   DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/pgw-plan-e2eprepare.log 2>&1
   ```

2. Smoke-маркер (минимальный обязательный кейс для задач, трогающих код
   воркеров):

   ```
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/pgw-plan-smoke.log 2>&1
   ```

3. Полный E2E-прогон (решение spec §6.8, подтверждено пользователем на
   user-review 2026-10-21: правка накрывает SQL-путь moves, включая
   `E2eMoveScenarios`):

   ```
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~PgWorker.IntegrationTests.E2e > /tmp/pgw-plan-e2e-full.log 2>&1
   ```

Мониторинг каждой тестовой команды (канон телеметрии E2E, базовое правило 13):
старт-чек по логу в течение 15 с; далее проверка отсутствия зависания каждые
2 минуты; фаза дольше 60 с — сверка `[PHASE]`-строк в логе; тест идёт дольше
5 минут — онлайн-анализ логов БЕЗ остановки прогона. Упавший сценарий —
разбор по `/tmp/pgw-e2e-artifacts-*` (контейнеры остановлены, не удалены);
ПЕРЕЗАПУСК упавших тестов для выяснения «что было» — запрещён (без согласия
пользователя).

Зачистка после КАЖДОЙ серии (smoke — серия, полный прогон — серия; страховочный
гейт после дожидания финальной строки прогона): убедиться, что контейнеры/тома
`pgw-*` ушли (тесты чистят сами), осиротевшие сети —
`docker network ls | grep -c kfw-net` при нуле контейнеров; при находке —
`docker network prune -f`. Не запускать следующую серию поверх незачищенной
предыдущей.

**Выход:** все три команды зелёные; серии зачищены (нет контейнеров/томов/сетей
`pgw-*`/`kfw-net-*` от этих прогонов).

**Проверка:** `/tmp/pgw-plan-e2eprepare.log` — штамп готовности записан;
`/tmp/pgw-plan-smoke.log` — `Passed!`; `/tmp/pgw-plan-e2e-full.log` — `Passed!`,
0 failed (все сценарии E2eFixture, включая E2eMoveScenarios); вывод
`docker ps -a --filter name=pgw-` пуст, `docker network ls | grep -E 'kfw-net|pgw-.*-net'`
пуст.

**Связь со spec:** §4 фаза 3, §6.8 (полный прогон), критерии приёмки 3–5.

---

## Шаг 4. Код-ревью диффа: соответствие §3 и границы скоупа

- [ ] **Шаг 4** (исполняет сильная модель; подпунктов нет — ревью требует
  понимания проекта)

**Вход:** шаги 1–3 зелёные, изменения закоммичены.

**Действие:** сверка диффа
`git -C <worktree> diff main...HEAD -- src/` (плюс `git show HEAD`) по чек-листу
критерия 6 spec:

- 4.1. Изменён РОВНО один файл кода — `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`;
  `arch/`, тесты, соседние исполнители — нетронуты.
- 4.2. `Timeout = SqlTimeoutSec` действует в подключении ВСЕХ четырёх методов
  (ScalarAsync, ListAsync, ExecuteAsync, ExecuteTransactionalAsync — все через
  `OpenConnection`).
- 4.3. Три Polly-метода создают команды через `Command(sql, conn, SqlTimeoutSec)`;
  Polly-параметры не тронуты (`RetryCount = 3`, `FirstRetryDelay = 1 s`).
- 4.4. В `ExecuteTransactionalAsync`: `var bodyTimeoutSec = Math.Max(SqlTimeoutSec, lockTimeoutSec + 2);`,
  ОБЕ команды (`setup` и `body`) с `bodyTimeoutSec`, комментарий-обоснование
  на месте; транзакция/SET LOCAL/commit/Polly-отсутствие — не тронуты.
- 4.5. Канон-комментарий arch/14 §6 у константы `SqlTimeoutSec` на месте
  (дословно из подпункта 1а); прочие комментарии по-русски, в тон файла.
- 4.6. `IMoveSqlExecutor`, `WrapError`/`Redact`/`PasswordRegex` — без изменений.

Замечание — сильная модель исправляет сама и перезапускает гейт с шага 2
(сборка), затем шаг 3 заново (полный E2E на свежем бинаре).

**Выход:** дифф соответствует spec §3 полностью; мерж-гейт (критерии 1–6)
собран.

**Проверка:** каждый пункт 4.1–4.6 подтверждён по диффу (фактическая команда
просмотра — на усмотрение сильной модели: `git diff`/`git show`).

**Связь со spec:** критерий приёмки 6; принципы §2 (минимальный дифф,
максимальная надёжность).

---

## Шаг 5. Пост-мерж: удалить пункт #2 из `.my/review_project.md` (основной чекаут)

- [ ] **Шаг 5** (исполняет сильная модель; подпунктов нет — границы блока и
  правило ненумерации требуют понимания; исполняется ПОСЛЕ мержа ветки в
  `main`, вне worktree)

**Вход:** ветка `fix-move-sql-executor-timeouts` смержена в `main` (мерж — по
отдельному гейту dev-flow, НЕ часть этого плана).

**Действие:** в файле `/Users/demakaev/ZCodeProject/pg/.my/review_project.md`
(основной чекаут; документ git'ом не отслеживается) удалить ДВЕ строки,
относящиеся к пункту #2:

1. Строку пункта в таблице «Что найдено» (секция `<a id="summary"></a>`) —
   строку, начинающуюся с `| [2](#p2) |` (полный текст строки):

   ```
   | [2](#p2) | 🟠 Major | Л-perf+Л-arch | `NpgsqlMoveSqlExecutor` без таймаутов: молчание до ~135 с против порога сноса 30 с — рестарт воркера посреди переезда |
   ```

   Остальные строки таблицы не трогать.
2. Блок пункта #2 ЦЕЛИКОМ: от строки `<a id="p2"></a>` до разделителя `---`
   включительно, который стоит непосредственно перед строкой
   `<a id="p3"></a>`. Разделитель `---` ПЕРЕД `<a id="p2"></a>`
   (принадлежащий пункту #1) — остаётся.

Номера и якоря остальных пунктов НЕ перенумеровываются и не
переименовываются (базовое правило 16); в оглавлении/таблице не остаётся
битых ссылок на удалённый якорь `#p2`. Никаких пометок
«закрыта/реализована» не оставлять.

**Выход:** пункт #2 удалён ЦЕЛИКОМ — и блок (заголовок «#2.
`NpgsqlMoveSqlExecutor` без таймаутов…», механизм, сценарий отказа,
проверка, «как чинится»), и его строка в таблице «Что найдено»; ссылка
`[2](#p2)` в таблице отсутствует; якорная последовательность
`p1 → p3 → …` сохранена без перенумерации.

**Проверка:** `grep -nE 'id="p2"|NpgsqlMoveSqlExecutor' /Users/demakaev/ZCodeProject/pg/.my/review_project.md`
— пусто (точный якорь пункта и уникальный содержательный маркер; голую
подстроку `p2` в grep не подставлять — она матчит якоря `p20`/`p21`/…);
`grep -nE '\[2\]\(#p2\)' /Users/demakaev/ZCodeProject/pg/.my/review_project.md`
— пусто (битой ссылки в таблице нет); `grep -c 'id="p'` — на единицу меньше
исходного.

**Связь со spec:** критерий приёмки 7.

---

## Самопроверка плана (выполнена при составлении)

- Покрытие spec: §3.1–3.3 → шаг 1; §4 фазы 2–4 → шаги 2–4; критерии 1–2 →
  шаг 2; 3–5 → шаг 3; 6 → шаг 4; 7 → шаг 5; §3.4/§6.4/§6.5/§6.7 (что не
  меняется) — глобальные ограничения; §6.1–6.3 → шаг 1 и ревью-чеки 4.2–4.4;
  §6.8 → шаг 3. Вне scope шагов нет.
- Развилки: значения таймаутов, формула, охват 4 методов, единый
  `bodyTimeoutSec` для setup/body, текст комментария (arch/14 §6 — как у
  обоих соседей-эталонов, соответствует текущей редакции spec §3.3),
  сообщение коммита, пути логов — всё предопределено выше; решений,
  отсутствующих в spec, план не принимает.
- Имена/сигнатуры — из фактического кода: `SqlRetryPolicies.SqlRetry(RetryCount, FirstRetryDelay)`,
  `Result<object?>`/`Result<IReadOnlyList<string>>`/`Result`, `WrapError`,
  `FreezeLockTimeoutSec` (дефолт 5, `src/PgWorker.App/Options.cs:352`),
  тест-классы и namespace `PgWorker.IntegrationTests.E2e` — проверены по файлам.
