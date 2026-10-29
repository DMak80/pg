# t91-e2e-container-stderr — телеметрия E2E снимает stderr контейнеров

- Дорожка: `arch/roadmap/reliability.md`, пункт `t91-e2e-container-stderr`.
- Канон телеметрии: `docs/e2e-launch.md` §1 (обновляется этой задачей, см. Фаза 0).
- Тип: правка тестовой инфраструктуры E2E (`src/tests/**`), прод-код воркеров не затрагивается.

## 1. Цель

При разборе фейлов E2E-прогонов по артефактам телеметрии (`container-*.log`,
`runner.log`, `wal-diag-*.txt`) видна диагностика, которую контейнеры пишут в
stderr, а не только в stdout. Сегодня она теряется, и «разбор слепеет»:
агент WalReceiver пишет `open_failed`/`stream_broken` в `Console.Error`
(`src/PgWorker.WalReceiver/WalReceiverCore.cs:67,109,191`), но `docker logs`
кладёт stdout контейнера в stdout docker-CLI, а stderr контейнера — в stderr
docker-CLI, и хелпер запуска процессов при exit=0 возвращает только stdout.

Механика потери по контурам:

- **PgWorker**: `E2eFixture.RunProcessAsync` (`src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs:228,251`) при exit=0 возвращает `output.Trim()` — только stdout; stderr уходит лишь в `logFile` (если задан) и в текст исключения при ненулевом exit.
- **KafkaWorker**: тот же паттерн в `KafkaE2eEnvironment.RunProcessAsync` (`src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eEnvironment.cs:261`).
- **S3Worker / ValkeyWorker**: `ShellToFileAsync` (`S3E2eEnvironment.cs:815`, `ValkeyE2eEnvironment.cs:634`) читает только `StandardOutput`; stderr не читается вовсе — при полном stderr-пайпе процесс может дедлокиться (RedirectStandardError=true без дренажа).

## 2. Принципы

1. **Канон прежде кода**: формулировка «объединённый съём» фиксируется в
   `docs/e2e-launch.md` §1 до правки кода (арх-правило; для телеметрии E2E
   источник истины — docs/e2e-launch.md, `arch/` контракт не затронут).
2. **Контракт `RunProcessAsync` не меняется**: существующие сигнатуры и
   поведение всех прочих вызовов (docker ps/inspect/rm/build/…) остаются
   байт-в-байт прежними — только stdout при exit=0. Объединённый съём —
   НОВЫЕ точки входа.
3. **Телеметрия «лучшими усилиями»**: ошибки сбора не роняют teardown —
   существующая семантика сохраняется.
4. **Логи максимально полные** (канон §1): никакого нового обрезания,
   форматы артефактов (имена файлов, пути) не меняются.

## 3. Дизайн

### 3.1. Объединённый формат

Объединение — конкатенация блоков: stdout-блок, затем stderr-блок. Между
двумя непустыми блоками гарантируется ровно один перевод строки; общий
результат тримится по краям (как текущий возврат `output.Trim()`). Пустой
stderr не добавляет ничего. Интерливинг потоков по времени НЕ
восстанавливается (осознанный компромисс; порядок внутри каждого потока
сохранён, для кейса «агент пишет только в stderr» цепочка видна целиком).
Формат совпадает с уже принятым в `RunProcessAsync` для `logFile`
(`WriteLogAsync(logFile, output + error)`).

### 3.2. PgWorker — новые методы в `E2eFixture`

Вынести тело текущей внутренней перегрузки `RunProcessAsync` (бюджет, kill
по таймауту, параллельное чтение обоих потоков, `logFile`, хвост в
исключение) в закрытый core-метод с параметром `bool mergeStderrOnSuccess`
(дефолт `false`). Существующие два `internal RunProcessAsync` и
`RunDockerAsync` становятся тонкими обёртками core со значением `false` —
их сигнатуры и поведение не меняются.

Новые точки входа:

```csharp
// Объединённый вывод (stdout+stderr) при exit=0; исключения и
// таймаут-семантика — идентичны RunProcessAsync.
internal static Task<string> RunProcessMergedAsync(
    string file, string[] args, CancellationToken ct,
    TimeSpan? timeout, IReadOnlyDictionary<string, string>? env = null,
    string? logFile = null)

// docker logs — всегда объединённо: ["logs", .. logsArgs].
// Бюджет по умолчанию 60 с (как у текущих logs-вызовов телеметрии).
internal static Task<string> RunDockerLogsAsync(
    IReadOnlyList<string> logsArgs, CancellationToken ct = default,
    TimeSpan? timeout = null)
```

При `mergeStderrOnSuccess=true` и exit=0 возвращается конкатенация §3.1;
ветки ненулевого exit и таймаута не меняются вовсе.

### 3.3. KafkaWorker и ValkeyWorker — симметрично

В `KafkaE2eEnvironment` и `ValkeyE2eEnvironment`: тот же приём — core с
`mergeStderrOnSuccess` внутри их private `RunProcessAsync`, плюс новые
`internal RunProcessMergedAsync(...)` и `public Task<string>
RunDockerLogsAsync(IReadOnlyList<string> logsArgs, CancellationToken ct)`
(бюджет 60 с по умолчанию). Их существующие `RunDockerAsync` не меняются.

### 3.4. S3Worker и ValkeyWorker — `ShellToFileAsync`

`ShellToFileAsync` в `S3E2eEnvironment` и `ValkeyE2eEnvironment`
переписывается (сигнатура сохраняется):

- оба потока читаются ПАРАЛЛЕЛЬНО (`ReadToEndAsync` на stdout и stderr до
  `WaitForExitAsync`) — устраняется риск дедлока полного пайпа;
- бюджет 60 с: `WaitForExitAsync(ct.Token)` + `CancelAfter`; по истечении —
  `Kill(entireProcessTree: true)` и дренаж с коротким страховочным окном
  (как `DrainAsync` в `E2eFixture`);
- при exit=0 в файл пишется объединённый вывод (§3.1), при любом исходе —
  «лучшими усилиями», без проброса исключений наружу (как сейчас).

Точки вызова `logs --tail 2000 …` остаются вызовами `ShellToFileAsync` —
объединение даёт сам метод. `inspect`/`ps`/`volume ls` идут через тот же
метод и не страдают (их stderr при успехе пуст).

### 3.5. Перевод всех вызовов `docker logs` (полный перечень)

Все вызовы, снимающие `docker logs`, переводятся на объединённый съём;
флаги (`--timestamps`, `--tail N`) и целевые файлы сохраняются дословно.

**PgWorker.IntegrationTests/E2e:**

| Файл : строка | Было | Имя файла-артефакта |
|---|---|---|
| `E2eEnvironment.cs:1404` (`CollectDiagnosticsAsync`) | `RunProcessAsync("docker", ["logs","--timestamps",id])` | `container-{name}.log` |
| `E2eEnvironment.cs:992` (`EnsureWorkerAsync`, readiness-фейл; хвост также в исключение) | `RunProcessAsync("docker", ["logs", containerName])` | `container-{containerName}.log` |
| `E2eFixture.cs:477` (`HostInstance.ReadLogTailAsync`) | `RunDockerAsync(["logs","--tail",N,ContainerName])` | — (возврат в сценарии) |
| `E2eFixture.cs:517` (`HostInstance.DisposeAsync`, логи до rm) | `RunDockerAsync(["logs","--timestamps",ContainerName])` | `container-{ContainerName}.log` |
| `E2eContour.cs:182` (журнал раннера) | `RunDockerAsync(["logs", runnerName])` | `runner.log` |
| `E2ePatroniFileSdScenarios.cs:241` (prom/sdgen) | `RunDockerAsync(["logs","--timestamps",c])` | `container-{c}.log` |
| `E2eWalStreamScenarios.cs:102` (tail 12, в исключение) | `RunDockerAsync` | — |
| `E2eWalStreamScenarios.cs:233` (tail 25, в исключение) | `RunDockerAsync` | — |
| `E2eWalStreamScenarios.cs:807` (tail 60, дамп `/tmp/pgw-diag-*.txt` — путь не менять) | `RunDockerAsync` | `pgw-diag-{cluster}.txt` |
| `E2eWalStreamMasterDownScenarios.cs:330` (tail 60, wal-diag) | `RunDockerAsync` | `wal-diag-{cluster}.txt` |
| `E2eWalStreamPromoteScenarios.cs:260` (tail 60, wal-diag) | `RunDockerAsync` | `wal-diag-{cluster}.txt` |

**KafkaWorker.IntegrationTests/E2e:** `KafkaE2eEnvironment.cs:343,358`
(`CollectDiagnosticsAsync`: `logs --tail 2000` → `container-{name}-{mark}.log`,
`node-{name}-{mark}.log`).

**S3Worker.IntegrationTests/E2e:** `S3E2eEnvironment.cs:635,656`
(`logs --tail 2000` → `container-…-{mark}.log`, `node-…-{mark}.log`).

**ValkeyWorker.IntegrationTests/E2e:** `ValkeyE2eEnvironment.cs:512,528`
(`logs --tail 2000` → `container-…-{mark}.log`, `node-…-{mark}.log`);
`ValkeyE2eTakeoverTests.cs:285` (`logs {survivorName}` → merge-хелпер).

`docker inspect` НИГДЕ на объединённый съём не переводится (JSON в stdout,
stderr при успехе пуст; парсеры `--format`-вызовов не должны видеть мусора).

Отдельное следствие для `E2eContour.cs:206`: парсинг `runnerLog.Contains("Total: 0")`
должен продолжать работать — объединение не режет строки (строки целы),
критерий приёмки это фиксирует.

## 4. Фазы

### Фаза 0 — канон (ДО кода)

`docs/e2e-launch.md` §1: в первом bullet (docker-логи/inspect всех
контейнеров) явно указать объединённый съём: stdout-блок, затем stderr-блок
(docker logs разделяет потоки контейнера по потокам CLI). В bullet про
`runner.log` уточнить «объединённо (stdout+stderr)». В таблице «Где что
лежит» пометить `container-*.log` как «(stdout+stderr)». Исторических
пассажей не добавлять — только текущее состояние.

### Фаза 1 — PgWorker

1. Core + `RunProcessMergedAsync` + `RunDockerLogsAsync` в `E2eFixture`
   (§3.2). Комментарии — по-русски, в стиле окружающего кода, с пояснением
   «почему» (docker logs разделяет потоки; WalReceiver пишет в stderr).
2. Перевод всех вызовов из таблицы §3.5 (PgWorker-строки).
3. Тесты (AAA-комментарии):
   - `[Fact]` в `NonE2eCollection`: `RunProcessMergedAsync("/bin/sh",
     ["-c","echo stdout-marker; echo stderr-marker >&2"], …)` возвращает
     ОБА маркера, stdout-маркер в тексте раньше stderr-маркера;
   - регресс-охрана контракта `[Fact]`: тот же процесс через обычный
     `RunProcessAsync` возвращает ТОЛЬКО `stdout-marker` (stderr в возврат
     не попадает).

### Фаза 2 — KafkaWorker, ValkeyWorker

Симметричные core + `RunProcessMergedAsync` + `RunDockerLogsAsync`
(§3.3), перевод точек §3.5, `[Fact]` на merge-механику в каждой сборке
(лёгкий, без docker; в Kafka/Valkey-сборках нет NonE2e-коллекции — факт
добавляется обычным, выполняется мгновенно и параллельно E2E не мешает).

### Фаза 3 — S3Worker, ValkeyWorker `ShellToFileAsync`

Переписать оба `ShellToFileAsync` (§3.4), `[Fact]` в каждой сборке:
запуск `/bin/sh -c 'echo stdout-marker; echo stderr-marker >&2'` в
temp-файл → файл содержит оба маркера (метод сделать `internal` для
доступа из теста той же сборки).

### Фаза 4 — мерж-гейт

1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   (и сборки Kafka/S3/Valkey — по solution-структуре проекта; все четыре
   тестовых проекта обязаны собираться с `TreatWarningsAsErrors`).
2. Юниты: новые `[Fact]` всех четырёх контуров зелёные; `RunProcessAsync`
   -регресс-факт зелёный.
3. Smoke docker-E2E (PgWorker): статическая подготовка
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`,
   затем тесты отдельной командой с `--no-build`:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`.
4. Сверка smoke-артефактов: в `/tmp/pgw-e2e-artifacts-<guid>/` (или
   `$TMPDIR`-аналог) у `container-pgw-ee*.log` (etcd) присутствует
   содержимое — etcd документированно логирует в stderr, до фикса этот файл
   пуст. Если unexpectedly пуст — прямая демультипликация живого контейнера
   `docker logs <id> >out.txt 2>err.txt` для установления факта, и
   фактический stderr-писатель контура используется как маркер; находка
   фиксируется в журнале задачи.
5. Зачистка контейнеров/сетей после серии (канон AGENTS.md).

### Фаза 5 — roadmap-гейт (тем же мерж-коммитом)

- Удалить пункт `t91-e2e-container-stderr` из `arch/roadmap/reliability.md`
  (строки 128–135).
- Убрать упоминание тега `t91-e2e-container-stderr` в тексте пункта t29
  (`reliability.md:70`) — фраза «телеметрия теряла stderr агентов…»
  описывает уже несуществующее состояние.
- `reliability-report.md`: тег t91 в отчёте не упоминается (проверено);
  при мерже перепроверить grep'ом — если появился, обновить сводку.

## 5. Ограничения

- Прод-код (`src/PgWorker.*`, `src/KafkaWorker.*`, `src/S3Worker.*`,
  `src/ValkeyWorker.*`, `src/Shared.*` — не-тестовые проекты) не
  трогается.
- Существующие сигнатуры `RunProcessAsync`/`RunDockerAsync` всех контуров
  не меняются; поведение прочих (не-logs) вызовов не меняется.
- Имена/пути артефактов телеметрии не меняются (`container-*.log`,
  `runner.log`, `wal-diag-*.txt`, `pgw-diag-*.txt`, `*-{mark}.log`).
- Интерливинг stdout/stderr по времени не восстанавливается (merge по
  timestamp отвергнут — развилка закрыта, см. §6).
- Полная E2E-серия PgWorker в гейте не требуется (правка — тестовая
  инфраструктура; достаточно smoke-класса).

## 6. Принятые решения (закрытые развилки)

| # | Развилка | Решение |
|---|---|---|
| 1 | Граница контуров: только PgWorker (канон WalReceiver) или все 4 E2E-контура | **Все 4 контура** (PgWorker/Kafka/S3/Valkey) — выбрано пользователем: потеря stderr воспроизведена везде, механика правки одна, в S3/Valkey заодно устраняется риск дедлока пайпа. |
| 2 | Какие вызовы переводить: только артефакты телеметрии или все `docker logs` | **Все вызовы `docker logs`** — выбрано пользователем: единая семантика канона §1 («логи всегда полные»), хвосты в исключениях ассертов при stdout-only слепнут так же; `docker inspect` не переводится. |
| 3 | Формат объединения: конкатенация блоков или merge по `--timestamps` | **Конкатенация блоков** (stdout, затем stderr) — выбрано пользователем: отказоустойчивость (нет парсинга), работает для вызовов без `--timestamps` (runner.log, tail-N), не ломает парсинг `Total: 0` и человекочитаемость. |
| 4 | Доказательство в мерж-гейте | **[Fact] + smoke E2E** (`Scale_AddEmptyShard`) — выбрано пользователем: юнит-доказательство механики + end-to-end сверка артефактов без тяжёлой серии. |
| 5 | Как не сломать контракт `RunProcessAsync` | Новые точки входа (`RunProcessMergedAsync`/`RunDockerLogsAsync`) + закрытый core с параметром; сигнатуры и поведение существующих методов не меняются вообще — буквальное исполнение требования задачи. Однозначно из формулировки задачи. |
| 6 | Разделитель между блоками | Между двумя непустыми блоками — один `\n`; общий `Trim()` по краям. Следует из единообразия с `WriteLogAsync(logFile, output + error)` и детерминированности парсинга. |
| 7 | Бюджет `docker logs` в новых хелперах | 60 с по умолчанию — как у текущих logs-вызовов телеметрии PgWorker (§ `CollectDiagnosticsAsync`); переопределяется параметром. |
| 8 | Бюджет в переписанном `ShellToFileAsync` (S3/Valkey) | 60 с + kill дерева + дренаж: канон §1 «вывод внешних процессов переживает таймаут» распространяется на телеметрию этих контуров; сегодня процесса-бюджета там нет вовсе. |
| 9 | Место `[Fact]`-тестов | PgWorker — `NonE2eCollection` (канон §5 e2e-launch: не-E2E последовательны); Kafka/S3/Valkey — обычные лёгкие факты (коллекции в сборках отсутствуют, факт ~100 мс параллелизму не мешает). |
| 10 | Smoke-маркер присутствия stderr | etcd-узел контура (`pgw-ee*`): документированно логирует в stderr → до фикса `container-pgw-ee*.log` пуст, после — содержит etcd-строки; fallback — прямая демультипликация `docker logs <id>` живого контейнера. |
| 11 | Правка канона | Обязательна (Фаза 0): §1 уже обещает «полный stdout/stderr» для runner.log — код не выполнял; формулировка распространяется явно на все docker-логи. |
| 12 | Старый дамп `/tmp/pgw-diag-{cluster}.txt` (`E2eWalStreamScenarios:807-816`) | Переводится на объединённый съём, путь и имя сохраняются — смена пути вне границы задачи. |

## 7. Критерии приёмки

1. `RunProcessMergedAsync` при exit=0 возвращает оба потока (юнит-факт
   зелёный); обычный `RunProcessAsync` по-прежнему возвращает только stdout
   (регресс-факт зелёный).
2. В PgWorker/Kafka/S3/Valkey не осталось НИ ОДНОГО вызова `docker logs`,
   идущего через stdout-only путь (grep-контроль: `"logs"` только в
   merge-хелперах и `ShellToFileAsync` с объединённой записью).
3. Smoke-прогон `Scale_AddEmptyShard`: в артефактах `container-pgw-ee*.log`
   (etcd) присутствует stderr-содержимое (критерий №10 §6), `runner.log`
   по-прежнему содержит итоговые строки раннера (включая корректное
   отсутствие ложного срабатывания `Total: 0`).
4. Все четыре тестовых проекта собираются в Release без warnings
   (`TreatWarningsAsErrors`), новые факты и smoke зелёные.
5. Канон `docs/e2e-launch.md` §1 описывает объединённый съём; имена и пути
   артефактов не изменились.
6. Roadmap-гейт: тег `t91-e2e-container-stderr` удалён из
   `arch/roadmap/reliability.md` (пункт и упоминание в t29) тем же
   мерж-коммитом; `reliability-report.md` перепроверен.
7. Никакие изменения не задевают прод-код воркеров и поведение не-logs
   вызовов процессов (регресс-факт №1 и сборка всех прочих тестов).
