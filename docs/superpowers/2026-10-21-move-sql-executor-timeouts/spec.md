# Spec: NpgsqlMoveSqlExecutor — SQL-таймауты полокна watchdog

Дата: 2026-10-21. Ветка: `fix-move-sql-executor-timeouts`.
Источник: пункт #2 ревью `.my/review_project.md` (основной чекаут, строки
80–100), Severity Major (Л-perf + Л-arch).

## 1. Контекст и цель

`NpgsqlMoveSqlExecutor` (`src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`)
— единственный SQL-исполнитель процессов переездов бакетов (M0–M6, cutover,
rollback, finalize, abort, repair; вызывается из reconcile-тика
`ReconcileLoop.RunClusterOpAsync(cluster, "moves", …)`). Ни один из его
четырёх методов не задаёт ни `NpgsqlConnectionStringBuilder.Timeout`, ни
`CommandTimeout` — действуют дефолты Npgsql (connect 15 с, command 30 с), а
Polly `SqlRetry(3, 1 с)` в трёх методах умножает молчание на 4 попытки
(худший случай ~135 с на один вызов, ревью: 15/30 с × 3 ретрая).

Это прямое нарушение инварианта поллинга arch/14 §6: одиночный блокирующий
вызов внутри фазы reconcile не имеет права молчать дольше порога сноса
watchdog (30 с при дефолтах 2 × 15 с). Два соседних исполнителя соблюдают
инвариант явно:

- `DatabaseProvisioner` (`src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs`,
  строки 37–50): `SqlTimeoutSec = 7`, `Timeout = 7` в CSB,
  `CommandTimeout = 7`, комментарий-канон «Поллинг-инвариант (arch/14 §6)…
  таймауты подключения и команды = половина окна (7 c при дефолтах 15 c);
  зависшая операция → исключение → transient-фейл, а не молчание до
  дефолтных Npgsql 15/30 c»;
- `NpgsqlWalSqlExecutor` (`src/PgWorker.Backups/Sql/NpgsqlWalSqlExecutor.cs`,
  строки 10–22): тот же `SqlTimeoutSec = 7` с прямой ссылкой на arch/14 §6.

Сценарий отказа (ревью): мастер шарда жив, TCP принимает, ответа нет
(зависший docker-proxy/файрвол) → SQL-проба молчит 15–30 с × 3 ретрая →
watchdog гасит воркер (`StopApplication`) посреди переезда, прерывая
обработку ВСЕХ кластеров; при устойчивой деградации — шторм рестартов.

**Цель**: привести `NpgsqlMoveSqlExecutor` к инварианту arch/14 §6 —
пер-попыточные SQL-таймауты полокна окна проверки watchdog (7 с при
дефолтах), симметрично двум соседним исполнителям.

## 2. Принципы

1. **arch-first — без правки arch/**. Инвариант поллинга УЖЕ канонизирован в
   arch/14 §6 («Инвариант поллинга долгих фаз», «симметрия таймаутов
   etcd/patroni/SQL»); per-исполнительных деталей уровня `SqlTimeoutSec = 7`
   в `arch/` нет (проверено grep'ом — соседние исполнители несут константу в
   коде с комментарием-ссылкой). Контракт не меняется: задача — приведение
   кода к существующему канону. `arch/` не трогаем.
2. **Консистентность с соседями**: паттерн реализации (const + хелперы +
   канон-комментарий) копирует `DatabaseProvisioner`/`NpgsqlWalSqlExecutor`
   дословно по форме.
3. **Минимальный дифф**: правится один файл
   (`src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`); интерфейс
   `IMoveSqlExecutor`, Polly-параметры, `WrapError`/`Redact` — без изменений.
4. **Максимальная надёжность**: таймауты закрываются во ВСЕХ четырёх методах
   класса (включая `ExecuteTransactionalAsync`), а не только в трёх,
   названных ревью, — класс дефекта «молчание SQL» закрывается целиком.

## 3. Структура/компоненты (что именно меняется)

Единственный файл: `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`.

### 3.1. Константа и хелперы (новые, по образцу соседей)

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

### 3.2. Три Polly-метода: `ScalarAsync` / `ListAsync` / `ExecuteAsync`

Внутри Polly-делегата вместо `new NpgsqlConnection(dsn)` +
`conn.CreateCommand()` — `OpenConnection(dsn)` + `Command(sql, conn,
SqlTimeoutSec)`. Прочая логика (Polly-обёртка, маппинг DBNull, WrapError) —
без изменений.

### 3.3. `ExecuteTransactionalAsync` (транзакция заморозки, без Polly)

- Подключение: `OpenConnection(dsn)` (connect-таймаут 7 с — сейчас молчание
  15 с при зависшем connect).
- CommandTimeout для `setup` (`SET LOCAL lock_timeout`) и `body`
  (REVOKE×3 + барьер `LOCK TABLE`):

```csharp
var bodyTimeoutSec = Math.Max(SqlTimeoutSec, lockTimeoutSec + 2);
```

  с комментарием-обоснованием: command-бюджет обязан ПОКРЫВАТЬ
  `lock_timeout` — «LOCK сам ждёт lock_timeout — ретраи ставит процесс»
  (канон: arch/14 §6; фраза дословно из док-комментария
  `IMoveSqlExecutor`); `+2` — запас тела батча (SET LOCAL + REVOKE×3);
  нижняя граница 7 — поллинг-инвариант arch/14 §6 при дефолтном
  `FreezeLockTimeoutSec = 5` (итог ровно 7 с, симметрия с остальными
  методами). При `FreezeLockTimeoutSec > 5` бюджет растёт вместе с локом и
  остаётся меньше порога сноса при любых разумных значениях опции.
- Транзакция, `SET LOCAL`, commit, отсутствие Polly — без изменений.

### 3.4. Что НЕ меняется

- `IMoveSqlExecutor` (контракт метода уже принимает `lockTimeoutSec`).
- Polly-параметры: `RetryCount = 3`, `FirstRetryDelay = 1 s` — как у
  `DatabaseProvisioner`.
- `WrapError`/`Redact`/`PasswordRegex` и существующий тест
  `MoveSqlExecutorTests.WrapError_MasksPassword`.
- Вызывающий код (`CutoverSequence`, `MoveProcess`) — поллинг-разметка
  `progress?.Mark()` уже стоит на месте (строки 113, 141 CutoverSequence).

## 4. Фазы

1. **Правка кода** — `NpgsqlMoveSqlExecutor.cs` по §3 (единственный файл).
2. **Сборка + юниты** — Release-сборка без варнингов
   (`TreatWarningsAsErrors=true`), юнит-тесты PgWorker зелёные
   (`MoveSqlExecutorTests`, `CutoverSequenceTests`,
   `MoveProcessCutoverTests`, `MoveDdlTests` работают на фейках
   `IMoveSqlExecutor` — дифф исполнителя их не задевает).
3. **E2E-гейт** (код воркера PgWorker; изменён SQL-путь moves) — по §7.
4. **Закрытие пункта ревью** — см. критерии приёмки (пост-мерж, основной
   чекаут).

## 5. Ограничения

- Правка кода ТОЛЬКО в worktree
  `/Users/demakaev/ZCodeProject/worktrees/fix-move-sql-executor-timeouts`.
- Новых юнит-тестов на таймауты НЕ добавляется: сетевую константу юнитом без
  живого PG не проверить, хрупкие сетевые тесты каноном не поощряются,
  соседи-эталоны (`DatabaseProvisioner`, `NpgsqlWalSqlExecutor`) тестов на
  таймауты не имеют. Приёмка — код-ревью диффа + гейт.
- Комментарии — по-русски, идентификаторы — по-английски; попадать в тон
  окружающего текста.

## 6. Принятые решения (закрытые развилки)

1. **Значение таймаута: `Timeout = 7` + `CommandTimeout = 7`.** Однозначно
   вытекает: предложение ревью #2, канон двух соседей, arch/14 §6
   («полокна окна проверки» = 7 с при `CheckIntervalSec = 15`).
2. **Охват: все 4 метода, не только 3 из ревью.** Ревью называет
   `ScalarAsync`/`ListAsync`/`ExecuteAsync` (строки 21–91), но
   `ExecuteTransactionalAsync` (строки 97–123) — тот же класс дефекта
   (connect 15 с + command 30 с = худший случай ~45 с молчания против
   порога 30 с, без Polly и без Mark внутри). Инвариант arch/14 §6 не
   делает исключений для транзакции. Решение подтверждено пользователем на
   гейте user-review 2026-10-21: охват — все 4 метода (максимальный по
   надёжности вариант).
3. **CommandTimeout транзакции согласован с `lockTimeoutSec`:
   `Math.Max(SqlTimeoutSec, lockTimeoutSec + 2)`.** Жёсткие 7 с в
   `ExecuteTransactionalAsync` конфликтуют с конфигурируемым
   `FreezeLockTimeoutSec` (дефолт 5, `MovesRuntimeOptions`/App-Options):
   при значении > 5 LOCK-барьер умирал бы по command-timeout раньше
   lock_timeout, ломая семантику заморозки. Формула покрывает лок + тело
   батча и при дефолтах даёт ровно 7 (симметрия с остальными методами).
4. **arch/14 не правится.** Инвариант уже канонизирован (§6), per-исполн
   ительных деталей (7 с) в arch нет — это уровень кода с
   комментариями-ссылками (так сделано в обоих соседях). Контракт не
   затронут — arch-first триггер «сначала arch» не срабатывает.
5. **Polly-параметры не меняются (3 / 1 с).** Residual: худший случай
   полной Polly-цепочки одного вызова ≈ 4 × 7 + ~3 с джиттер-делеев ≈ 31 с
   — на грани порога 30 с. Это ИДЕНТИЧНО принятому канону
   `DatabaseProvisioner` (та же структура RetryCount/Delay/Timeout);
   ложный снос требует 4 идеальных таймаутов подряд = глубокая деградация,
   при которой снос + takeover ≤ 15 с (lease) — штатный исход. Смягчать
   (уменьшать RetryCount/Timeout) не будем — расходится с соседями без
   запроса ревью.
6. **Тяжёлые SQL (сверка `count(*)`, шаг 6 cutover) при таймауте 7 с —
   transient, не permanent.** Проверено по коду: SQL-фейл `RowCount` →
   `FailAsync(VerifyFailed)` с ОБЫЧНЫМ исключением → `MoveProcess` (строки
   495–505) обрабатывает как transient (заявка жива, ретраи тиками);
   permanent (`CutoverPermanentException` → `RejectAsync`) — только
   row-count-mismatch и flip-conflict. Residual: на таблицах, чей `count(*)`
   стабильно > 7 с, verify будет бесконечно ретраиться (видимо в статусе,
   не молчание). Масштаб стенда/дома (базовое правило 9) — вне риска;
   вынос «тяжёлого verify» в поллинг-итерации — отдельная задача, НЕ этого
   spec'а (в roadmap не оформляется: сценарий не наблюдаем на текущих
   объёмах).
7. **Новых тестов нет** — симметрия соседям (см. §5).
8. **E2E-гейт: полный прогон E2eFixture, не только smoke-маркер.** Буква
   AGENTS.md: минимальный маркер `Scale_AddEmptyShard` обязателен для задач,
   трогающих код воркеров; полный прогон — «при изменении
   provisioning/portalloc/moves-процессов». Правка накрывает SQL-путь
   moves (включая `E2eMoveScenarios`), поэтому трактовка — полный прогон:
   он дороже smoke-маркера, но ловит регрессию таймаутов на реальных
   сценариях переездов. Решение подтверждено пользователем на гейте
   user-review 2026-10-21.

## 7. Критерии приёмки (мерж-гейт)

Порядок ОБЯЗАТЕЛЕН; каждая команда — отдельным запуском из корня worktree;
лог каждой команды пишется в файл и читается из файла:

1. Сборка (свежесть бинаря видна этой командой):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   — 0 error, 0 warning.
2. Юниты:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~PgWorker.UnitTests`
   — все зелёные.
3. Статическая подготовка E2E (одним процессом, ДО тестов):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
   — штамп готовности записан.
4. Smoke-маркер:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
   — зелёный.
5. Полный E2E-прогон (все сценарии E2eFixture, включая E2eMoveScenarios):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~PgWorker.IntegrationTests.E2e`
   — зелёный; после серии — зачистка контейнеров/сетей/томов pgw-* и
   осиротевших сетей (`docker network prune -f` при необходимости) до
   следующей серии.
6. Код-ревью диффа: ровно один файл кода изменён
   (`NpgsqlMoveSqlExecutor.cs`); таймауты во всех четырёх методах; формулы
   и комментарии соответствуют §3; канон-комментарий arch/14 §6 на месте.
7. Пост-мерж (основной чекаут, вне worktree): пункт #2 УДАЛЯЕТСЯ из
   `.my/review_project.md` (правило 16 базовых правил; документ не
   отслеживается git); номера остальных пунктов НЕ перенумеровываются.

## 8. Риски и остаточные эффекты

- Худший случай Polly-цепочки ≈ 31 с на грани порога watchdog — принят
  каноном соседей (решение 5).
- `FreezeLockTimeoutSec > 5`: транзакция заморозки получает CommandTimeout
  `lockTimeoutSec + 2` > 7 — осознанно (LOCK-семантика важнее симметрии
  константы); при гигантских значениях опции (> ~28 с) молчание одной
  попытки приближается к порогу сноса — операторская конфигурация вне
  дефолтов, `Mark()` после каждой попытки в CutoverSequence (строка 113)
  ограничивает последствия рамками одной попытки.
- Таймауты НЕ добавляются в `NpgsqlSlotPositionReader` (Timeout=10 уже
  задан) и `AdminPanel.Probes.SqlProbe` (панель, вне тика reconcile) —
  вне скоупа пункта #2.
