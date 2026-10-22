# t91-e2e-container-stderr — план реализации (Фаза 3 dev-flow)

> **Для исполнителя:** план двухуровневый. ШАГ исполняет сильная модель (по
> умолчанию main-агент; при executor="coder" — субагент coder). ПОДПУНКТЫ
> внутри шага — самодостаточные задания для слабой модели (субагент
> coder-flash, БЕЗ контекста проекта): ей передаётся только текст подпункта.
> Всё, что не выделено в подпункты (ревью, отладка, прогоны, документация), —
> сильная модель делает сама. Шаги отмечаются чекбоксами `- [ ]`.

**Цель:** телеметрия E2E всех четырёх контуров (PgWorker/Kafka/S3/Valkey)
снимает `docker logs` объединённо (stdout-блок, затем stderr-блок), не меняя
контракт `RunProcessAsync` (stdout-only при exit=0) и сигнатуры существующих
методов.

**Архитектура:** в каждом контуре вводятся НОВЫЕ точки входа
(`RunProcessMergedAsync` / `RunDockerLogsAsync`; в S3/Valkey — перепись
`ShellToFileAsync` с параллельным чтением обоих потоков и бюджетом 60 с);
существующие методы становятся тонкими обёртками закрытого core с параметром
`mergeStderrOnSuccess=false`. Все вызовы `docker logs` переводятся на
объединённый съём; `docker inspect` не переводится.

**Спека:** `docs/superpowers/2026-10-22-t91-e2e-container-stderr/spec.md`
(план аргументирует от спеки; исполнитель читает обе).

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), xUnit v3, FluentAssertions. Один солюшен
`src/PgWorker.slnx` (включает все четыре тестовых проекта).

## Глобальные ограничения (из спеки §5, действуют во всех шагах)

- Прод-код (`src/PgWorker.*`, `src/KafkaWorker.*`, `src/S3Worker.*`,
  `src/ValkeyWorker.*`, `src/Shared.*` — не-тестовые проекты) не трогается.
- Существующие сигнатуры `RunProcessAsync`/`RunDockerAsync`/`ShellToFileAsync`
  всех контуров не меняются; поведение не-logs вызовов не меняется.
- Имена/пути артефактов телеметрии не меняются (`container-*.log`,
  `runner.log`, `wal-diag-*.txt`, `pgw-diag-{cluster}.txt`, `*-{mark}.log`;
  путь `/tmp/pgw-diag-{cluster}.txt` — дословно).
- Формат объединения (§3.1): stdout-блок, затем stderr-блок; между двумя
  непустыми блоками ровно один `\n`; общий `Trim()` по краям; пустой stderr
  ничего не добавляет; интерливинг по времени НЕ восстанавливается.
- `RunDockerLogsAsync` всегда подставляет подкоманду `"logs"` САМ
  (`["logs", .. logsArgs]`, §3.2/§3.3) — вызовы передают только аргументы
  после `logs`.
- Бюджет `docker logs` в новых хелперах — 60 с по умолчанию (решение №7);
  ветки ненулевого exit и таймаута существующих методов не меняются вовсе.
- Телеметрия — «лучшими усилиями»: ошибки сбора не роняют teardown.
- Комментарии в коде — по-русски, в стиле окружающего кода, с пояснением
  «почему» (docker logs разделяет потоки контейнера по потокам CLI;
  WalReceiver пишет диагностику в stderr). Тесты — с AAA-комментариями.
- Таймауты ожиданий агента — не более 30 с; команды с длинным выводом —
  лог целиком в файл, анализ из файла.

## Ревью-фокус (риски, не покрытые тестами отдельных шагов — проверяются в шаге 9)

1. Дедлок полного stderr-пайпа в старом `ShellToFileAsync` (S3/Valkey) —
   закрыт параллельным чтением (шаги 7–8, факты на оба маркера).
2. Поломка контракта stdout-only для не-logs вызовов (парсеры
   `inspect --format`) — регресс-факт шага 4 + неизменность веток core.
3. Таймаут-ветка merge-методов обязана сохранить хвост в исключение —
   код core в шагах 2/5/6 идентичен текущему, меняется только возврат при
   exit=0 (ревью сильной моделью).
4. `runner.log` и парсинг `Total: 0` (`E2eContour.cs:206`) — объединение не
   рвёт строки; smoke-критерий шага 9.
5. Остаточные stdout-only `docker logs`-вызовы — grep-контроль шага 9
   (заранее известные легальные исключения: S3 `ContainerLogsAsync` — уже
   читает оба потока; Kafka `CaRotationTests.cs:172` — `2>&1` в shell;
   упоминания `docker logs` в комментариях `*E2ePhase.cs`).

---

## Шаг 1. Фаза 0 — канон `docs/e2e-launch.md` §1 (ДО кода)

- **Вход:** спека одобрена; worktree-ветка задачи активна.
- **Действие** (сильная модель, документация): три точечные правки
  `/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/docs/e2e-launch.md`
  — только текущее состояние, без исторических пассажей:
  1. Первый bullet §1 (строки 41–45, «docker-логи и inspect всех
     контейнеров…»): после `e2e[slug]: телеметрия … → …`); дополнить
     предложением: «docker-логи снимаются **объединённо**: stdout-блок,
     затем stderr-блок (docker logs разделяет потоки контейнера по потокам
     CLI);».
  2. Bullet про `runner.log` (строка 47): «(вывод `docker logs` раннера);» →
     «(вывод `docker logs` раннера, объединённо stdout+stderr);».
  3. Таблица «Где что лежит» (строка 161): `container-*.log` →
     `container-*.log` (stdout+stderr) — внутри той же ячейки, остальные
     колонки не трогать.
- **Выход:** канон §1 описывает объединённый съём docker-логов.
- **Проверка:** `grep -n "объединённо" docs/e2e-launch.md` — ровно 2 строки
  (bullet §1 и runner.log; пометка таблицы — по букве спеки «(stdout+stderr)»
  без этого слова); `grep -n "stdout+stderr" docs/e2e-launch.md` — 2 строки
  (runner.log и таблица); имена артефактов в таблице не изменились (кроме
  пометки stdout+stderr).
- **Связь со spec:** Фаза 0, принцип 1 («канон прежде кода»), решение №11.
- Подпунктов нет (документация).

## Шаг 2. Фаза 1.1 — PgWorker: core + `RunProcessMergedAsync` + `RunDockerLogsAsync` в `E2eFixture`

- **Вход:** шаг 1 завершён (канон обновлён).
- **Действие** (сильная модель: ревью результатов подпунктов, точечные
  исправления после ревью): правка
  `/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs`
  по подпунктам 2.1–2.2. Существующие перегрузки `RunProcessAsync` и
  `RunDockerAsync` остаются публичными точками с прежними сигнатурами и
  поведением stdout-only (тонкие обёртки core). Ветви ненулевого exit и
  таймаута core не меняются.
- **Выход:** в `E2eFixture` есть закрытый core с `mergeStderrOnSuccess`,
  новые `RunProcessMergedAsync`/`RunDockerLogsAsync` (сигнатуры — §3.2 спеки;
  `RunDockerLogsAsync` сам подставляет `"logs"`), приватный `MergeOutput`
  (формат §3.1).
- **Проверка:** подпункт 2.3 (сборка) без ошибок и warnings.
- **Связь со spec:** §3.2, §4 Фаза 1 п.1, решения №5/№6/№7.

Подпункты (для слабой модели):

**2.1.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs`.
Изменить метод `RunProcessAsync` с полной сигнатурой (сейчас строки ~251–309,
начинается с `internal static async Task<string> RunProcessAsync(` и
параметрами `string file, string[] args, CancellationToken ct, TimeSpan? timeout,
IReadOnlyDictionary<string, string>? env = null, string? logFile = null`):
1) переименовать его в `RunProcessCoreAsync`, сделать `private` вместо
   `internal`, добавить последний параметр `bool mergeStderrOnSuccess = false`;
   doc-комментарий над методом и тело оставить прежними, кроме строки
   `return output.Trim();` — заменить её на
   `return mergeStderrOnSuccess ? MergeOutput(output, error) : output.Trim();`
2) на место прежнего метода (сразу после `RunProcessCoreAsync`) вставить
   тонкую обёртку с прежней сигнатурой и приведённым ниже doc-комментарием:

```csharp
    /// <summary>Процесс с бюджетом (t27): docker-CLI изредка виснет — по
    /// истечении дерево процесса убивается, наружу ApplicationException.
    /// Возврат при exit=0 — только stdout (контракт вызовов телеметрии);
    /// объединённый съём — RunProcessMergedAsync.</summary>
    internal static Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null)
        => RunProcessCoreAsync(file, args, ct, timeout, env, logFile);
```

Малую перегрузку `RunProcessAsync(string file, string[] args, CancellationToken ct = default)`
и `RunDockerAsync` НЕ трогать.

**2.2.** В тот же файл, перед методом `DrainAsync` (приватный
`private static async Task<string> DrainAsync(Task<string> outTask, Task<string> errTask)`),
добавить три метода:

```csharp
    /// <summary>Объединение потоков (канон docs/e2e-launch.md §1):
    /// stdout-блок, затем stderr-блок; между двумя непустыми блоками ровно
    /// один перевод строки, края тримятся. docker-CLI разводит потоки
    /// контейнера по СВОИМ потокам (docker logs: stdout контейнера → stdout
    /// CLI, stderr → stderr CLI) — stdout-only теряет диагностику агентов
    /// (WalReceiver пишет open_failed/stream_broken в stderr). Интерливинг
    /// по времени не восстанавливается — порядок внутри потока сохранён.</summary>
    private static string MergeOutput(string output, string error)
    {
        var o = output.Trim();
        var e = error.Trim();
        return e.Length == 0 ? o : o.Length == 0 ? e : o + "\n" + e;
    }

    /// <summary>Процесс с объединённым возвратом (stdout+stderr) при exit=0;
    /// исключения и таймаут-семантика — идентичны RunProcessAsync. Отдельная
    /// точка входа для телеметрии — контракт RunProcessAsync не меняется.</summary>
    internal static Task<string> RunProcessMergedAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null)
        => RunProcessCoreAsync(file, args, ct, timeout, env, logFile,
            mergeStderrOnSuccess: true);

    /// <summary>docker logs — всегда объединённо (stdout+stderr контейнера):
    /// ["logs", .. logsArgs] — подкоманду logs подставляет сам хелпер.
    /// Бюджет по умолчанию 60 с — как у logs-вызовов телеметрии;
    /// переопределяется параметром timeout.</summary>
    internal static Task<string> RunDockerLogsAsync(
        IReadOnlyList<string> logsArgs, CancellationToken ct = default,
        TimeSpan? timeout = null)
        => RunProcessMergedAsync("docker", ["logs", .. logsArgs], ct,
            timeout ?? TimeSpan.FromSeconds(60));
```

**2.3.** Компиляция: из корня
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr`
выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo`
(лог целиком в файл `/tmp/t91-step2-build.log`, затем прочитать его).
Ожидание: `Build succeeded`, 0 warnings (`TreatWarningsAsErrors`).

## Шаг 3. Фаза 1.2 — PgWorker: перевод всех 11 вызовов `docker logs` (§3.5)

- **Вход:** шаг 2 завершён (`E2eFixture.RunDockerLogsAsync` существует).
- **Действие** (сильная модель: ревью результатов подпунктов): замены
  «было → стало» по подпунктам 3.1–3.7. Флаги (`--timestamps`, `--tail N`),
  целевые файлы, try/catch-обвязки и обработка результата — сохраняются
  дословно. `E2eEnvironment.RunDockerAsync` (строка 1037) не трогается
  (остаётся для не-logs вызовов). Бюджеты: у всех переводимых вызовов
  становится 60 с по умолчанию (решение №7), кроме `EnsureWorkerAsync` —
  там явные 30 с сохраняются.
- **Выход:** в PgWorker.IntegrationTests/E2e не осталось вызовов `docker logs`
  через stdout-only путь.
- **Проверка:** подпункт 3.8; плюс
  `grep -rn '"logs"' src/tests/PgWorker.IntegrationTests` — ровно ОДНО
  вхождение: `["logs", .. logsArgs]` внутри определения
  `E2eFixture.RunDockerLogsAsync`.
- **Связь со spec:** §3.5 (таблица PgWorker-строк), §4 Фаза 1 п.2, решения №2/№7.

Подпункты (для слабой модели; путь корня репозитория во всех —
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr`):

**3.1.** Файл `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`,
две замены.
Первая (метод `EnsureWorkerAsync`, ~строка 992). Было:

```csharp
                logs = await E2eFixture.RunProcessAsync("docker", ["logs", containerName],
                    CancellationToken.None, TimeSpan.FromSeconds(30));
```

Стало:

```csharp
                logs = await E2eFixture.RunDockerLogsAsync([containerName],
                    CancellationToken.None, TimeSpan.FromSeconds(30));
```

Вторая (метод `CollectDiagnosticsAsync`, ~строка 1404). Было:

```csharp
                    var logs = await E2eFixture.RunProcessAsync("docker", ["logs", "--timestamps", id],
                        CancellationToken.None, TimeSpan.FromSeconds(60));
```

Стало:

```csharp
                    var logs = await E2eFixture.RunDockerLogsAsync(["--timestamps", id]);
```

Окружающий код (try/catch, запись файла) не менять.

**3.2.** Файл `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs`,
две замены в классе `HostInstance`.
Первая (метод `ReadLogTailAsync`, ~строка 477). Было:

```csharp
        => await E2eFixture.RunDockerAsync(["logs", "--tail", lines.ToString(), ContainerName], ct);
```

Стало:

```csharp
        => await E2eFixture.RunDockerLogsAsync(["--tail", lines.ToString(), ContainerName], ct);
```

Вторая (метод `DisposeAsync`, ~строки 517–518). Было:

```csharp
            var logs = await E2eFixture.RunDockerAsync(
                ["logs", "--timestamps", ContainerName], CancellationToken.None);
```

Стало:

```csharp
            var logs = await E2eFixture.RunDockerLogsAsync(
                ["--timestamps", ContainerName], CancellationToken.None);
```

**3.3.** Файл `src/tests/PgWorker.IntegrationTests/E2e/E2eContour.cs`, одна
замена (~строка 182). Было:

```csharp
            runnerLog = await E2eFixture.RunDockerAsync(["logs", runnerName], CancellationToken.None);
```

Стало:

```csharp
            runnerLog = await E2eFixture.RunDockerLogsAsync([runnerName], CancellationToken.None);
```

**3.4.** Файл
`src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs`,
одна замена (~строка 241). Было:

```csharp
                        await E2eFixture.RunDockerAsync(["logs", "--timestamps", c], CancellationToken.None),
```

Стало:

```csharp
                        await E2eFixture.RunDockerLogsAsync(["--timestamps", c], CancellationToken.None),
```

**3.5.** Файл `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamScenarios.cs`,
три замены (все — локальная переменная `logs`).
~строка 102. Было:

```csharp
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "12", name], ct);
```

Стало:

```csharp
                var logs = await E2eFixture.RunDockerLogsAsync(["--tail", "12", name], ct);
```

~строка 233. Было:

```csharp
                    var logs = await Fx.RunDockerAsync(["logs", "--tail", "25", name], ct);
```

Стало:

```csharp
                    var logs = await E2eFixture.RunDockerLogsAsync(["--tail", "25", name], ct);
```

~строки 807–808. Было:

```csharp
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "60", name],
                    TestContext.Current.CancellationToken);
```

Стало:

```csharp
                var logs = await E2eFixture.RunDockerLogsAsync(["--tail", "60", name],
                    TestContext.Current.CancellationToken);
```

**3.6.** Файл
`src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamMasterDownScenarios.cs`,
одна замена (~строки 330–331). Было:

```csharp
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "60", name],
                    TestContext.Current.CancellationToken);
```

Стало:

```csharp
                var logs = await E2eFixture.RunDockerLogsAsync(["--tail", "60", name],
                    TestContext.Current.CancellationToken);
```

**3.7.** Файл
`src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamPromoteScenarios.cs`,
одна замена (~строки 260–261), идентичная подпункту 3.6 (тот же текст
«было/стало»).

**3.8.** Компиляция: из корня worktree выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step3-build.log`, затем прочитать его). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 4. Фаза 1.3 — PgWorker: факты merge и регресс-охраны контракта

- **Вход:** шаги 2–3 завершены.
- **Действие** (сильная модель: ревью результата подпункта): создание файла
  тестов по подпункту 4.1.
- **Выход:** два зелёных `[Fact]` в `NonE2eCollection`.
- **Проверка** (запускает сильная модель; фикстуры коллекции
  `NonE2eCollection` поднимутся — это штатно, сами факты их состояния не
  используют):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2eProcessMergeTests" --nologo`
  (лог целиком в `/tmp/t91-step4-test.log`, анализ из файла) — 2 passed.
- **Связь со spec:** §4 Фаза 1 п.3, критерий приёмки №1, решение №9.

Подпункты:

**4.1.** Создать новый файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/PgWorker.IntegrationTests/E2e/E2eProcessMergeTests.cs`
с содержимым:

```csharp
using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// Механика объединённого съёма stdout+stderr (канон docs/e2e-launch.md §1):
// docker-CLI разводит потоки контейнера по своим потокам — телеметрия обязана
// видеть оба; контракт RunProcessAsync (только stdout при exit=0) не меняется.
// Не-E2E факт: локальный /bin/sh, без docker.
[Collection(NonE2eCollection.Name)]
public sealed class E2eProcessMergeTests
{
    [Fact]
    public async Task RunProcessMergedAsync_ReturnsStdoutThenStderr()
    {
        // Arrange: процесс пишет маркер в stdout и другой — в stderr.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: объединённый запуск.
        var merged = await E2eFixture.RunProcessMergedAsync(
            "/bin/sh", args, CancellationToken.None, TimeSpan.FromSeconds(30));

        // Assert: ОБА маркера; stdout-блок раньше stderr-блока (§3.1).
        merged.Should().Contain("stdout-marker");
        merged.Should().Contain("stderr-marker");
        merged.IndexOf("stdout-marker").Should().BeLessThan(
            merged.IndexOf("stderr-marker"));
    }

    [Fact]
    public async Task RunProcessAsync_ReturnsOnlyStdout()
    {
        // Arrange: тот же двупоточный процесс.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: обычный (не-merge) запуск.
        var output = await E2eFixture.RunProcessAsync(
            "/bin/sh", args, CancellationToken.None);

        // Assert: регресс-охрана контракта — в возврате ТОЛЬКО stdout.
        output.Should().Contain("stdout-marker");
        output.Should().NotContain("stderr-marker");
    }
}
```

После создания файла выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step4-build.log`, затем прочитать его). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 5. Фаза 2 (часть 1) — KafkaWorker: core + новые методы + перевод точек + факт

- **Вход:** шаг 4 завершён (PgWorker-контур готов).
- **Действие** (сильная модель: ревью результатов подпунктов): правки
  `src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eEnvironment.cs`
  (подпункты 5.1–5.3) и новый файл факта (5.4). Существующий
  `RunDockerAsync` и `ShellToFileAsync` (для `inspect`) не меняются.
- **Выход:** `KafkaE2eEnvironment.RunDockerLogsAsync` существует и используется
  в `CollectDiagnosticsAsync`; лёгкий факт зелёный.
- **Проверка** (сильная модель):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --filter "FullyQualifiedName~KafkaProcessMergeTests" --nologo`
  (лог целиком в `/tmp/t91-step5-test.log`) — 1 passed.
- **Связь со spec:** §3.3, §3.5 (KafkaWorker-строки), Фаза 2, решения №5/№7/№9.

Подпункты:

**5.1.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eEnvironment.cs`,
метод `private static async Task<string> RunProcessAsync(string file, string[] args, CancellationToken ct, TimeSpan timeout, string? workingDirectory = null, string? logFile = null)`
(~строки 261–320): добавить последний параметр
`bool mergeStderrOnSuccess = false`; строку `return output.Trim();` заменить
на `return mergeStderrOnSuccess ? MergeOutput(output, error) : output.Trim();`.
Остальное тело (включая запись `logFile` и таймаут-ветку) не менять.

**5.2.** В тот же файл, сразу после закрывающей скобки метода
`RunProcessAsync` (приватный метод с параметрами
`string file, string[] args, CancellationToken ct, TimeSpan timeout,
string? workingDirectory = null, string? logFile = null`; до секции
«── Хелперы etcd ──»), добавить три метода:

```csharp
    /// <summary>Объединение потоков (канон docs/e2e-launch.md §1):
    /// stdout-блок, затем stderr-блок; между двумя непустыми блоками ровно
    /// один перевод строки, края тримятся. docker-CLI разводит потоки
    /// контейнера по своим потокам — stdout-only теряет stderr-диагностику.
    /// Интерливинг по времени не восстанавливается.</summary>
    private static string MergeOutput(string output, string error)
    {
        var o = output.Trim();
        var e = error.Trim();
        return e.Length == 0 ? o : o.Length == 0 ? e : o + "\n" + e;
    }

    /// <summary>Процесс с объединённым возвратом (stdout+stderr) при exit=0 —
    /// телеметрия docker logs; контракт RunProcessAsync/RunDockerAsync
    /// (только stdout) не меняется.</summary>
    internal static Task<string> RunProcessMergedAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout,
        string? workingDirectory = null, string? logFile = null)
        => RunProcessAsync(file, args, ct, timeout, workingDirectory, logFile,
            mergeStderrOnSuccess: true);

    /// <summary>docker logs — всегда объединённо (stdout+stderr контейнера):
    /// ["logs", .. logsArgs] — подкоманду logs подставляет сам хелпер;
    /// бюджет 60 с (канон docs/e2e-launch.md §1).</summary>
    public Task<string> RunDockerLogsAsync(IReadOnlyList<string> logsArgs, CancellationToken ct)
        => RunProcessMergedAsync("docker", ["logs", .. logsArgs], ct,
            TimeSpan.FromSeconds(60));
```

**5.3.** В том же файле, метод `CollectDiagnosticsAsync` (~строки 339–360),
две замены.
Первая — в цикле по `new[] { Worker1Name, Worker2Name, $"kfw-ee-{_runId}" }`
(~строка 343). Было:

```csharp
            await ShellToFileAsync($"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.log"));
```

Стало:

```csharp
            // docker logs — объединённо (stdout+stderr); телеметрия —
            // «лучшими усилиями», не роняет сбор.
            try
            {
                await File.WriteAllTextAsync(
                    Path.Combine(ArtifactsDir, $"container-{name}-{mark}.log"),
                    await RunDockerLogsAsync(["--tail", "2000", name], CancellationToken.None));
            }
            catch
            {
                // телеметрия — лучшее усилие
            }
```

Следующая за ней строка `await ShellToFileAsync($"inspect {name}", ...)` НЕ
меняется. Вторая — в цикле по `names` (~строка 358). Было:

```csharp
        foreach (var name in names)
            await ShellToFileAsync($"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"node-{name}-{mark}.log"));
```

Стало:

```csharp
        foreach (var name in names)
        {
            // docker logs — объединённо (stdout+stderr); телеметрия —
            // «лучшими усилиями», не роняет сбор.
            try
            {
                await File.WriteAllTextAsync(
                    Path.Combine(ArtifactsDir, $"node-{name}-{mark}.log"),
                    await RunDockerLogsAsync(["--tail", "2000", name], CancellationToken.None));
            }
            catch
            {
                // телеметрия — лучшее усилие
            }
        }
```

**5.4.** Создать новый файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/KafkaWorker.IntegrationTests/E2e/KafkaProcessMergeTests.cs`
с содержимым:

```csharp
using FluentAssertions;
using Xunit;

namespace KafkaWorker.IntegrationTests.E2e;

// Механика объединённого съёма stdout+stderr (канон docs/e2e-launch.md §1):
// лёгкий факт без docker — /bin/sh пишет в оба потока, merge-хелпер обязан
// вернуть оба блока (stdout раньше stderr).
public sealed class KafkaProcessMergeTests
{
    [Fact]
    public async Task RunProcessMergedAsync_ReturnsStdoutThenStderr()
    {
        // Arrange: процесс пишет маркер в stdout и другой — в stderr.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: объединённый запуск.
        var merged = await KafkaE2eEnvironment.RunProcessMergedAsync(
            "/bin/sh", args, CancellationToken.None, TimeSpan.FromSeconds(30));

        // Assert: оба маркера; stdout-блок раньше stderr-блока.
        merged.Should().Contain("stdout-marker");
        merged.Should().Contain("stderr-marker");
        merged.IndexOf("stdout-marker").Should().BeLessThan(
            merged.IndexOf("stderr-marker"));
    }
}
```

После создания выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/KafkaWorker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step5-build.log`, затем прочитать). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 6. Фаза 2 (часть 2) — ValkeyWorker: core + новые методы + перевод точки + факт

- **Вход:** шаг 5 завершён.
- **Действие** (сильная модель: ревью результатов подпунктов): правки
  `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs`
  (подпункты 6.1–6.2), `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eTakeoverTests.cs`
  (6.3), новый файл факта (6.4). Существующий `RunDockerAsync` не меняется.
  Добавляемый здесь `MergeOutput` — ЕДИНСТВЕННОЕ определение этого метода в
  классе (шаг 8 переиспользует его, не дублируя).
- **Выход:** `ValkeyE2eEnvironment.RunDockerLogsAsync` существует, точка
  TakeoverTests переведена; факт зелёный.
- **Проверка** (сильная модель):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --filter "FullyQualifiedName~ValkeyProcessMergeTests" --nologo`
  (лог целиком в `/tmp/t91-step6-test.log`) — 1 passed.
- **Связь со spec:** §3.3, §3.5 (ValkeyWorker-строки: 512/528 закрываются
  переписью `ShellToFileAsync` шага 8, точка 285 — здесь), Фаза 2.

Подпункты:

**6.1.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs`,
метод `private static async Task<string> RunProcessAsync(string file, string[] args, CancellationToken ct, TimeSpan timeout)`
(~строки 367–419): добавить последний параметр
`bool mergeStderrOnSuccess = false`; строку `return output.Trim();` заменить
на `return mergeStderrOnSuccess ? MergeOutput(output, error) : output.Trim();`.
Остальное тело не менять.

**6.2.** В тот же файл, сразу после закрывающей скобки метода
`RunProcessAsync` (приватный метод с параметрами
`string file, string[] args, CancellationToken ct, TimeSpan timeout`),
добавить три метода:

```csharp
    /// <summary>Объединение потоков (канон docs/e2e-launch.md §1):
    /// stdout-блок, затем stderr-блок; между двумя непустыми блоками ровно
    /// один перевод строки, края тримятся. docker-CLI разводит потоки
    /// контейнера по своим потокам — stdout-only теряет stderr-диагностику.
    /// Интерливинг по времени не восстанавливается.</summary>
    private static string MergeOutput(string output, string error)
    {
        var o = output.Trim();
        var e = error.Trim();
        return e.Length == 0 ? o : o.Length == 0 ? e : o + "\n" + e;
    }

    /// <summary>Процесс с объединённым возвратом (stdout+stderr) при exit=0 —
    /// телеметрия docker logs; контракт RunProcessAsync/RunDockerAsync
    /// (только stdout) не меняется.</summary>
    internal static Task<string> RunProcessMergedAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout)
        => RunProcessAsync(file, args, ct, timeout, mergeStderrOnSuccess: true);

    /// <summary>docker logs — всегда объединённо (stdout+stderr контейнера):
    /// ["logs", .. logsArgs] — подкоманду logs подставляет сам хелпер;
    /// бюджет 60 с (канон docs/e2e-launch.md §1).</summary>
    public Task<string> RunDockerLogsAsync(IReadOnlyList<string> logsArgs, CancellationToken ct)
        => RunProcessMergedAsync("docker", ["logs", .. logsArgs], ct,
            TimeSpan.FromSeconds(60));
```

**6.3.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eTakeoverTests.cs`,
одна замена (~строка 285). Было:

```csharp
            var survivorLog = await fx.RunDockerAsync(["logs", survivorName], ct);
```

Стало:

```csharp
            var survivorLog = await fx.RunDockerLogsAsync([survivorName], ct);
```

**6.4.** Создать новый файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyProcessMergeTests.cs`
с содержимым:

```csharp
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// Механика объединённого съёма stdout+stderr (канон docs/e2e-launch.md §1):
// лёгкий факт без docker — /bin/sh пишет в оба потока, merge-хелпер обязан
// вернуть оба блока (stdout раньше stderr).
public sealed class ValkeyProcessMergeTests
{
    [Fact]
    public async Task RunProcessMergedAsync_ReturnsStdoutThenStderr()
    {
        // Arrange: процесс пишет маркер в stdout и другой — в stderr.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: объединённый запуск.
        var merged = await ValkeyE2eEnvironment.RunProcessMergedAsync(
            "/bin/sh", args, CancellationToken.None, TimeSpan.FromSeconds(30));

        // Assert: оба маркера; stdout-блок раньше stderr-блока.
        merged.Should().Contain("stdout-marker");
        merged.Should().Contain("stderr-marker");
        merged.IndexOf("stdout-marker").Should().BeLessThan(
            merged.IndexOf("stderr-marker"));
    }
}
```

После создания выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/ValkeyWorker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step6-build.log`, затем прочитать). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 7. Фаза 3 (часть 1) — S3Worker: перепись `ShellToFileAsync` + факт

- **Вход:** шаг 6 завершён.
- **Действие** (сильная модель: ревью результатов подпунктов): перепись
  `ShellToFileAsync` в `src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`
  по подпункту 7.1 и новый файл факта (7.2). Сигнатура сохраняется, метод
  становится `internal static` (инстанс не создаётся фабрикой вне E2E;
  состояние экземпляра метод не использует — тесту той же сборки нужен
  статический доступ, это условие Фазы 3 спеки). Точки вызова в
  `CollectDiagnosticsAsync` (635/656: `logs --tail 2000`, `inspect`, `ps`,
  `volume ls`) НЕ меняются — объединение даёт сам метод (§3.4).
- **Выход:** `ShellToFileAsync` читает оба потока параллельно, бюджет 60 с,
  kill дерева + дренаж, пишет объединённый вывод; факт зелёный.
- **Проверка** (сильная модель):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~S3ShellToFileTests" --nologo`
  (лог целиком в `/tmp/t91-step7-test.log`) — 1 passed.
- **Связь со spec:** §3.4, §3.5 (S3Worker-строки), Фаза 3, решения №8/№9.

Подпункты:

**7.1.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`.
Заменить ЦЕЛИКОМ метод (сейчас ~строки 815–830)

```csharp
    private async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(startInfo)!;
            var output = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }
```

на

```csharp
    // Телеметрия процесса в файл ОБЪЕДИНЁННО (stdout-блок, затем stderr-блок —
    // docker-CLI разводит потоки контейнера по своим потокам, канон
    // docs/e2e-launch.md §1). Оба потока читаются ПАРАЛЛЕЛЬНО до ожидания
    // выхода — полный stderr-пайп при непараллельном чтении дедлокит процесс;
    // бюджет 60 с: по истечении дерево убивается, доступный вывод дренажится
    // и пишется. Телеметрия — «лучшими усилиями»: исключения наружу не
    // пробрасываются. Статический доступ — для факта той же сборки
    // (инстанс окружения создаётся только фабрикой E2E).
    internal static async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            using var process = Process.Start(startInfo)
                ?? throw new ApplicationException($"failed to start {program}");
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            // Оба потока — параллельно и БЕЗ токена бюджета: при kill пайпы
            // закрываются, накопленный вывод доступен (канон E2eFixture).
            var outTask = process.StandardOutput.ReadToEndAsync();
            var errTask = process.StandardError.ReadToEndAsync();
            try
            {
                await process.WaitForExitAsync(timeoutCts.Token);
                var output = await outTask;
                var error = await errTask;
                await File.WriteAllTextAsync(path, MergeOutput(output, error));
            }
            catch (OperationCanceledException)
            {
                // Бюджет исчерпан: убить дерево, дренировать, писать доступное.
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch
                {
                    // процесс мог уже выйти
                }

                var drained = await DrainAsync(outTask, errTask);
                await File.WriteAllTextAsync(path, drained);
            }
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    // Объединение потоков (stdout-блок, затем stderr-блок; между двумя
    // непустыми блоками ровно один перевод строки, края тримятся).
    private static string MergeOutput(string output, string error)
    {
        var o = output.Trim();
        var e = error.Trim();
        return e.Length == 0 ? o : o.Length == 0 ? e : o + "\n" + e;
    }

    // Дренаж вывода убитого процесса: короткое страховочное окно (5 с) —
    // диагностика уже убитого процесса, НЕ бюджет выполняемой команды.
    private static async Task<string> DrainAsync(Task<string> outTask, Task<string> errTask)
    {
        try
        {
            var drained = await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5));
            return drained[0] + drained[1];
        }
        catch (TimeoutException)
        {
            return "";
        }
    }
```

Существующие вызовы `ShellToFileAsync(...)` внутри класса не менять
(компилятор резолвит статический вызов без правок).

**7.2.** Создать новый файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/S3Worker.IntegrationTests/E2e/S3ShellToFileTests.cs`
с содержимым:

```csharp
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// ShellToFileAsync телеметрии обязан писать ОБА потока процесса (stdout+
// stderr, канон docs/e2e-launch.md §1): лёгкий факт без docker — /bin/sh
// пишет в оба потока, файл содержит оба блока.
public sealed class S3ShellToFileTests
{
    [Fact]
    public async Task ShellToFileAsync_WritesBothStreams()
    {
        // Arrange: temp-файл артефакта; процесс пишет в оба потока.
        var path = Path.Combine(Path.GetTempPath(), $"s3w-shelltofile-{Guid.NewGuid():N}.txt");

        try
        {
            // Act: съём вывода в файл.
            await S3E2eEnvironment.ShellToFileAsync(
                "/bin/sh", "-c \"echo stdout-marker; echo stderr-marker >&2\"", path);
            var content = await File.ReadAllTextAsync(path);

            // Assert: файл содержит оба маркера.
            content.Should().Contain("stdout-marker");
            content.Should().Contain("stderr-marker");
        }
        finally
        {
            File.Delete(path);
        }
    }
}
```

После создания выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/S3Worker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step7-build.log`, затем прочитать). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 8. Фаза 3 (часть 2) — ValkeyWorker: перепись `ShellToFileAsync` + факт

- **Вход:** шаг 7 завершён; в `ValkeyE2eEnvironment` уже существует
  `private static string MergeOutput(string output, string error)`
  (добавлен шагом 6.2) — шаг 8 НЕ определяет его повторно.
- **Действие** (сильная модель: ревью результатов подпунктов): перепись
  `ShellToFileAsync` в `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs`
  (подпункт 8.1) и новый файл факта (8.2). Точки `CollectDiagnosticsAsync`
  (512/528) НЕ меняются — объединение даёт сам метод (§3.4).
- **Выход:** Valkey-`ShellToFileAsync` объединённый с бюджетом; факт зелёный.
- **Проверка** (сильная модель):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --filter "FullyQualifiedName~ValkeyShellToFileTests" --nologo`
  (лог целиком в `/tmp/t91-step8-test.log`) — 1 passed.
- **Связь со spec:** §3.4, §3.5 (ValkeyWorker 512/528), Фаза 3, решения №8/№9.

Подпункты:

**8.1.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs`.
Заменить ЦЕЛИКОМ метод (сейчас ~строки 634–649)

```csharp
    private async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(startInfo)!;
            var output = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }
```

на блок из двух методов (важно: метод
`private static string MergeOutput(string output, string error)` В ЭТОМ ФАЙЛЕ
УЖЕ ЕСТЬ — НЕ добавлять его повторно, дубликат определения не скомпилируется;
новый код ниже вызывает существующий):

```csharp
    // Телеметрия процесса в файл ОБЪЕДИНЁННО (stdout-блок, затем stderr-блок —
    // docker-CLI разводит потоки контейнера по своим потокам, канон
    // docs/e2e-launch.md §1). Оба потока читаются ПАРАЛЛЕЛЬНО до ожидания
    // выхода — полный stderr-пайп при непараллельном чтении дедлокит процесс;
    // бюджет 60 с: по истечении дерево убивается, доступный вывод дренажится
    // и пишется. Телеметрия — «лучшими усилиями»: исключения наружу не
    // пробрасываются. Статический доступ — для факта той же сборки
    // (инстанс окружения создаётся только фабрикой E2E).
    internal static async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            using var process = Process.Start(startInfo)
                ?? throw new ApplicationException($"failed to start {program}");
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            // Оба потока — параллельно и БЕЗ токена бюджета: при kill пайпы
            // закрываются, накопленный вывод доступен (канон E2eFixture).
            var outTask = process.StandardOutput.ReadToEndAsync();
            var errTask = process.StandardError.ReadToEndAsync();
            try
            {
                await process.WaitForExitAsync(timeoutCts.Token);
                var output = await outTask;
                var error = await errTask;
                await File.WriteAllTextAsync(path, MergeOutput(output, error));
            }
            catch (OperationCanceledException)
            {
                // Бюджет исчерпан: убить дерево, дренировать, писать доступное.
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch
                {
                    // процесс мог уже выйти
                }

                var drained = await DrainAsync(outTask, errTask);
                await File.WriteAllTextAsync(path, drained);
            }
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    // Дренаж вывода убитого процесса: короткое страховочное окно (5 с) —
    // диагностика уже убитого процесса, НЕ бюджет выполняемой команды.
    private static async Task<string> DrainAsync(Task<string> outTask, Task<string> errTask)
    {
        try
        {
            var drained = await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5));
            return drained[0] + drained[1];
        }
        catch (TimeoutException)
        {
            return "";
        }
    }
```

Вызовы `ShellToFileAsync(...)` внутри класса не менять.

**8.2.** Создать новый файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t91-e2e-container-stderr/src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyShellToFileTests.cs`
с содержимым:

```csharp
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// ShellToFileAsync телеметрии обязан писать ОБА потока процесса (stdout+
// stderr, канон docs/e2e-launch.md §1): лёгкий факт без docker — /bin/sh
// пишет в оба потока, файл содержит оба блока.
public sealed class ValkeyShellToFileTests
{
    [Fact]
    public async Task ShellToFileAsync_WritesBothStreams()
    {
        // Arrange: temp-файл артефакта; процесс пишет в оба потока.
        var path = Path.Combine(Path.GetTempPath(), $"vwk-shelltofile-{Guid.NewGuid():N}.txt");

        try
        {
            // Act: съём вывода в файл.
            await ValkeyE2eEnvironment.ShellToFileAsync(
                "/bin/sh", "-c \"echo stdout-marker; echo stderr-marker >&2\"", path);
            var content = await File.ReadAllTextAsync(path);

            // Assert: файл содержит оба маркера.
            content.Should().Contain("stdout-marker");
            content.Should().Contain("stderr-marker");
        }
        finally
        {
            File.Delete(path);
        }
    }
}
```

После создания выполнить
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/ValkeyWorker.IntegrationTests -c Release --nologo`
(лог целиком в `/tmp/t91-step8-build.log`, затем прочитать). Ожидание:
`Build succeeded`, 0 warnings.

## Шаг 9. Фаза 4 — мерж-гейт: сборка, юниты, grep-контроль, smoke docker-E2E, артефакты

- **Вход:** шаги 1–8 завершены.
- **Действие** (сильная модель; все логи — целиком в `/tmp/t91-gate-*.log`,
  анализ из файлов; ожидания — не более 30 с за проверку, тесты — по правилам
  мониторинга AGENTS.base §13):
  1. Сборка всего солюшена (один slnx включает все четыре тестовых проекта):
     `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
     → `Build succeeded`, 0 warnings.
  2. Юниты новых фактов (5 команд с фильтрами из проверок шагов 4–8,
     прогонять по очереди): все зелёные — `E2eProcessMergeTests` (2 passed),
     `KafkaProcessMergeTests` (1), `ValkeyProcessMergeTests` (1),
     `S3ShellToFileTests` (1), `ValkeyShellToFileTests` (1); регресс-факт
     `RunProcessAsync_ReturnsOnlyStdout` зелёный.
  3. Grep-контроль полноты перевода (критерий приёмки №2):
     - `grep -rn '"logs"' src/tests/PgWorker.IntegrationTests src/tests/KafkaWorker.IntegrationTests src/tests/S3Worker.IntegrationTests src/tests/ValkeyWorker.IntegrationTests`
       → PgWorker/Kafka/Valkey: ровно по ОДНОМУ вхождению — `["logs", .. logsArgs]`
       внутри определений `RunDockerLogsAsync`; S3: пусто.
     - `grep -rn 'logs --tail\|logs --timestamps' src/tests`
                       → только: `S3E2eEnvironment.cs` 635/656 и
       `ValkeyE2eEnvironment.cs` 512/528 (вызовы переписанного
       `ShellToFileAsync` — объединение даёт сам метод);
       `KafkaWorker.IntegrationTests/Kafka/CaRotationTests.cs:172` (`2>&1` —
       уже объединён shell'ом); комментарии `*E2ePhase.cs` и
       `S3E2eEnvironment.ContainerLogsAsync` (уже читает оба потока) —
       легальны и не меняются.
  4. Статическая подготовка PgWorker docker-E2E:
     `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
     → штамп готовности записан.
  5. Smoke docker-E2E (сборка и тесты — ОТДЕЛЬНЫМИ командами; тесты с
     `--no-build` и env-префиксом при команде):
     `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
     → Passed (метод сценария зелёный; оркестратор — Passed/Skipped по
     канону e2e-launch §5).
  6. Сверка smoke-артефактов: `ls ${TMPDIR} | grep pgw-e2e-artifacts` (и
     контрольный `ls /tmp | grep pgw-e2e-artifacts` — искать в ОБОИХ местах);
     в каталоге последнего прогона: у каждого `container-pgw-ee*.log` (etcd)
     размер > 0 (`wc -c`) — etcd документированно логирует в stderr, до фикса
     файл был пуст; `runner.log` непуст и содержит итоговые строки раннера
     (в т.ч. отсутствие ложного срабатывания `Total: 0`). Если файл
     unexpectedly пуст — прямая демультипликация живого etcd-контейнера
     `docker logs <id> >out.txt 2>err.txt` для установления факта (контейнеры
     зелёного прогона удалены — поднять etcd-контейнер вручную:
     `docker run -d --name t91-stderr-check quay.io/coreos/etcd:v3.5.21` —
     тот же образ, что использует E2E-контур, `EtcdImage` в
     `E2eEnvironment.cs:43`), фактический stderr-писатель используется как
     маркер; находка фиксируется в журнале задачи. Ручной контейнер удалить
     сразу после проверки (`docker rm -f t91-stderr-check`).
  7. Зачистка после серии: зелёный прогон чистит себя сам; страховочный гейт —
     `docker ps -a --format '{{.Names}}' | grep -c 'pgw-'` = 0 и
     `docker network ls --format '{{.Name}}' | grep -c 'pgw-en-\|kfw-net'` = 0
     (класс-сети серии PgWorker-E2E именуются `pgw-en-{runId}` —
     `E2eEnvironment.cs:411`; `kfw-net` — страховка от осиротевших сетей
     по канону AGENTS.md; dev-стенд сетей с этими префиксами не создаёт;
     сети `pgw-it-net-*` NonE2e-фикстуры к серии не относятся и этим
     паттерном не проверяются). При остатках — разбор по канону, затем
     зачистка (осиротевшие сети — `docker network prune -f`).
- **Выход:** мерж-гейт пройден: сборка без warnings, факты зелёные,
  stdout-only logs-вызовов нет, smoke-артефакты содержат stderr etcd.
- **Проверка:** пункты 1–7 данного шага (каждый — своим критерием выше).
- **Связь со spec:** §4 Фаза 4 п.1–5, критерии приёмки №1–№4, решения №4/№10.
- Подпунктов нет (запуски, анализ и диагностика — сильная модель).

## Шаг 10. Фаза 5 — roadmap-гейт и коммит ветки

- **Вход:** шаг 9 пройден.
- **Действие** (сильная модель, документация):
  1. `arch/roadmap/reliability.md`: удалить пункт `t91-e2e-container-stderr`
     целиком (строки 128–135, хвост файла, начиная с
     `- **`t91-e2e-container-stderr`** —` до конца включительно); удалить в
     пункте t29 две строки «Из этого пункта — только инструментальная
     часть: телеметрия / теряла stderr агентов и показывала цикл
     «молчащим» — `t91-e2e-container-stderr`.» (сейчас строки 69–70).
     Никаких пометок «закрыта/реализована» не оставлять.
  2. `grep -n "t91" arch/roadmap/reliability-report.md` — пусто (при
     появлении — обновить сводку отчёта по правилу мерж-гейта трека).
  3. Коммит ветки (feature-ветка worktree; сообщения — по стилю истории
     `git log --oneline -5`): все изменения шагов 1–10 одним коммитом.
- **Выход:** roadmap чист, изменения закоммичены в ветке задачи.
- **Проверка:** `grep -rn "t91-e2e-container-stderr" arch/` — пусто;
  `git status` — чистый рабочий каталог.
- **Связь со spec:** §4 Фаза 5, критерий приёмки №6, правило roadmap-гейта
  AGENTS.md.
- Подпунктов нет (документация и git — сильная модель).

---

## Самопроверка плана (выполнена составителем)

- Покрытие спеки: §3.1 → шаги 2/5/6/7/8 (`MergeOutput`); §3.2 → шаг 2;
  §3.3 → шаги 5–6; §3.4 → шаги 7–8; §3.5 → шаги 3 (11 точек PgWorker),
  5 (Kafka 343/358), 6 (Valkey 285), 7–8 (S3 635/656, Valkey 512/528 через
  перепись `ShellToFileAsync`); §4 Фазы 0–5 → шаги 1–10; §5 ограничения →
  глобальные; §6 решения №1–№12 вшиты в шаги (№5 — шаги 2/5/6, №6 —
  `MergeOutput`, №7 — 60 с, №8 — шаги 7–8, №9 — шаги 4–8, №10 — шаг 9 п.6,
  №11 — шаг 1, №12 — шаг 3 п.3.5); §7 критерии №1–№7 → шаги 4/9/10.
- Шагов вне scope нет: прод-код не трогается, имена артефактов не меняются.
- `RunDockerLogsAsync` во всех трёх контурах (шаги 2.2/5.2/6.2) идентично
  подставляет подкоманду: `["logs", .. logsArgs]` — вызовы шагов 3.1–3.7,
  5.3, 6.3 передают только аргументы после `logs`; grep-ожидания шагов 3 и
  9.3 согласованы с этим (ровно одно `"logs"`-вхождение на контур — в
  определении хелпера).
- `MergeOutput` в каждом классе определён РОВНО ОДИН раз: `E2eFixture` — 2.2;
  `KafkaE2eEnvironment` — 5.2; `ValkeyE2eEnvironment` — 6.2 (шаг 8.1 его
  переиспользует, не дублирует — оговорено в шаге 8 и подпункте 8.1);
  `S3E2eEnvironment` — 7.1. Дублей CS0111 нет.
- Гейт зачистки (шаг 9 п.7) матчит фактические имена серии: класс-сети
  `pgw-en-{runId}` (`E2eEnvironment.cs:411`) + страховка `kfw-net`
  (осиротевшие, канон AGENTS.md); юнит-гейт (п.2) — 5 команд по числу
  тест-классов шагов 4–8.
- Развилок нет: все имена/сигнатуры/тексты фиксированы; единственное место,
  где спека требовала доступ «из теста той же сборки» при приватном
  конструкторе окружения (S3/Valkey `ShellToFileAsync`), закрыто
  `internal static` (состояние экземпляра метод не использует) — это прямое
  следствие Фазы 3 спеки, зафиксировано в шагах 7–8.
