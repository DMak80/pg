# План: t01-kafka-rotation-timeout — стабильность AC10 + прод-фикс PasswordRotator (D2)

> **Для исполнителя:** план исполняется по шагам (checkbox-синтаксис). Шаги
> следуют фазам spec §6: A0 → A1 → A2 → A3 → A4 → A5 → **A4-D2 → A5-D2** →
> A6 — порядок НЕ меняется. executor = coder: план исполняет сильная
> модель-ОДИНОЧКА; «подпункты для слабой модели» — обычные инструкции: делай
> точно по их тексту (они самодостаточны и не требуют иного контекста).
> **Шаги 1–6 УЖЕ ИСПОЛНЕНЫ** (статусы в заголовках) — НЕ исполнять заново;
> исполнение продолжается со ШАГА 7 (A4-D2).

**Цель:** кейс AC10 (`BothTicketsPasswordAndCa_PlayThroughWithoutExpiry`)
стабилен в полной параллельной серии `IntegrationTests` solutionx:
телеметрия и механика ожидания влиты (исполнено), диагноз установлен —
D2, прод-дефект PasswordRotator (afterCommit-хвост фазы C затирается
waiting-записью → вечный no-op «зомби»); остаётся прод-фикс по контракту
§5.5 (arch-first), юнит-кейсы, полная переприёмка (A4-D2/A5-D2) и закрытие
пункта роадмапа как исправленного (A6).

**Архитектура:** телеметрия §5.1 и механика §5.2 — влиты до прогонов (A0/A4,
исполнено); приёмочная серия воспроизвела баг с полной телеметрией → диагноз
D4→D2 (§3.1 spec). Дальше: arch-first правка `arch/16` §5 H → прод-фикс
`PasswordRotator.cs` (тихий пропуск `Success(false)` в двух waiting-точках
хвоста) → юнит-кейсы → переприёмка (E2ePrepare заново, docker-E2E-маркер,
юниты, изолят, полный проект, полная серия) → закрытие. Прод-код вне объёма
§5.5 не трогается.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
xunit.v3 (maxParallelThreads=5 из `xunit.runner.json`), Testcontainers,
Confluent.Kafka 2.15.1, FluentAssertions.

**Spec:** `docs/superpowers/2026-10-23-t01-kafka-rotation-timeout/spec.md`
(редакция после Фазы 6, одобрена user-review; план аргументирует от spec —
исполнитель читает ОБА). Журнал исполнения: `journal.md` там же.

**Кодовая база (worktree):** `/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout`,
ветка `fix-t01-kafka-rotation-timeout`. Исполненные коммиты: `7448fdd1`
(A0), `c9d4a1a1` (A3), `86953df0` (A4), `d1b9a8be` (разбор красного, D4→D2).
Рабочие команды выполняются от корня worktree.

## Глобальные ограничения (spec §7; действуют на ВСЕ шаги)

- Правки тестов — ТОЛЬКО `src/tests/KafkaWorker.IntegrationTests` (уже влито,
  не откатывается); прод-правка — ТОЛЬКО в объёме контракта §5.5 и ТОЛЬКО
  arch-first (канон `arch/16` §5 H первой правкой): `PasswordRotator.cs` +
  юниты `PasswordRotatorTests.cs`. Экспирации/гварды t10 при живой заявке,
  механика K (CaRotator), фабрика AdminClient — НЕ трогаются (§5.3).
- Ассерты AC10 не ослабляются (построчная сверка §2 на ревью); кейсы
  AC11/AC12 — бука в букву; `UpAsync`/`TeardownAsync`/`NewRigAsync`
  интеграционной фикстуры, per-class параллельность, `xunit.runner.json` —
  без изменений; `requestTimeout` фабрики фикстуры остаётся 10 с (Р5: D1 не
  подтверждён).
- `BrokerBootSec` фикстуры остаётся ≤ 100 с; литералов хост-портов не
  добавлять; таймауты/бюджеты «вслепую» не увеличивать (каждое число — из
  механики §5.2 или улик §3.1, фиксируется в journal.md).
- **E2ePrepare перевыполняется ЗАНОВО после КАЖДОЙ правки кода** — ДО
  PgWorker-E2E и любых серий solutionx: штамп готовности инвалидируется
  правками исходников → мгновенные fail-fast «sources are newer than the
  preparation» (урок приёмочной серии; Р16). Это штатный гейт свежести, не
  дефект.
- Все команды — только фоново (`run_in_background`), с фиксацией task_id и
  мониторингом (старт-чек ≤15 с; контроль каждые 2 мин; тест >5 мин —
  онлайн-анализ лога без остановки; любое ожидание ≤30 с). Полные выводы —
  в файлы `/tmp/pgw-kfw-t01-*.log` (никаких `tail`/обрезаний).
- Перезапуск упавших тестов «для выяснения что было» ЗАПРЕЩЁН; повтор
  приёмочной серии (A5-D2) — ЕДИНСТВЕННЫЙ повтор задачи, исполняется ПОСЛЕ
  прод-фикса по согласию пользователя (Р15), не для «выяснения».
- После КАЖДОЙ серии/прогона — зачистка `kfw-*`/`pgw-*` контейнеров/томов/сетей
  тестовых прогонов (dev-стендовые объекты НЕ трогать); проверка
  `docker network ls | grep 'kfw-net\|pgw-net'` — пусто.
- Сборка всегда зелёная и без warnings (`TreatWarningsAsErrors=true`).
  Комментарии/документация — по-русски; идентификаторы — на английском.
- Коммиты — свободно в feature-ветке; мерж в `main`/пуш — только по явному
  требованию пользователя (в плане НЕ выполняется).

## Соглашения

- **Журнал задачи**: `docs/superpowers/2026-10-23-t01-kafka-rotation-timeout/journal.md`
  (создан в A3, ведётся до A6). Разделы: Диагноз (D2 с уликами), Решения
  (каждое числовое изменение с обоснованием; пост-диагнозные Р10–Р16),
  Прогоны (команда, дата, лог-файл, исход), Закрытие.
- **Улики** (при красном): полные снимки — в `/tmp/pgw-kfw-t01-evidence-<n>-<имя>.log`;
  выжимки и выводы — в journal.md. Логи в репозиторий не коммитятся.
- **Имена логов**: A0-сборка `…-build-a0.log`; A1 `…-isolate-1.log`; A2
  `…-series-1.log`; A4 `…-build-a4.log`, `…-isolate-2.log`; первая приёмка
  (A5) — `…-accept-*.log`/`…-e2e-*.log` (см. journal.md); A4-D2 `…-d2-build.log`,
  `…-d2-units.log`; A5-D2 `…-d2-e2e-prepare.log`, `…-d2-accept-build.log`,
  `…-d2-e2e-scale.log`, `…-d2-units.log`, `…-d2-isolate.log`,
  `…-d2-full.log`, `…-d2-series.log`.

## Review Focus (классы отказов, которые план обязан держать закрытыми)

1. **Затирание afterCommit-маркера waiting-фазой → «зомби»-ротация** (корень
   D2) → фикс §5.5: обе waiting-точки хвоста (ca-window, преф-чек describe)
   — тихий пропуск `Success(false)` без записи журнала; юнит-кейсы пинят
   обе точки + контроли живой заявки (шаг 7; spec §5.5, Р11–Р13).
2. **Ложное красное при медленной, но живой ротации** → backstop вычисляемый
   и скользящий: пол 260 с = 2×BrokerBootSec(100)+60, с замерами темпа —
   `2 × avgTick × 1.5 + 60`, кап 1800 с (влито, шаг 5; spec §5.2).
3. **Транзиентный Failed-тик (etcd/exec/describe) валит тест** → тики фаз H/K
   через `TickAsync` (3 подряд Failed → красный с диагностикой; одиночный
   транзиент переживается) (влито, шаг 5; spec §5.2).
4. **Спам дорогих тиков (JVM-подачи) по дешёвому гейту** → подача не чаще
   `TickIntervalMs=1000`, опрос `PollMs=250` (влито, шаг 5; spec §5.2).
5. **Пропуск фазы `expired` между опросами / ослабление ассертов AC10** →
   NotContain("expired") на каждом чтении журнала + финальный ассерт по
   journalTexts; ассерты после циклов — дословно (шаги 1/5; spec §2).
6. **Диагностика маскирует причину падения** → сборщик `RotationDiagAsync`
   не бросает: ошибки etcd-чтений дают `<none>`, describe-проба — в
   try/catch с текстом ошибки, endpoints=null — явный «skip» (влито, шаг 1;
   spec §5.1).
7. **Устаревший штамп E2ePrepare роняет PgWorker-часть серии** → E2ePrepare
   заново после правок кода, ДО серий (шаг 8; Р16).

---

### Шаг 1 (фаза A0): диагностическая телеметрия фаз H/K — ДО любых прогонов

- [x] Шаг 1 выполнен — **СТАТУС: ИСПОЛНЕН** (коммит `7448fdd1`; телеметрия
  влита до прогонов — дала корень D2 на приёмочной серии).

**Вход (предусловие):** worktree чист (кроме каталога задачи), код
`src/KafkaWorker.*` идентичен базе бага; spec прочитан.

**Действие (кодер):** применить подпункты 1.1–1.4 (правки только
`RotationTimeoutTests.cs`), затем собрать (фоново, лог
`/tmp/pgw-kfw-t01-build-a0.log`):
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
и закоммитить: `test(kfw): t01 A0 — диагностическая телеметрия фаз H/K AC10`.
Существующие циклы H/K и литеральные бюджеты 240/300 с в этом шаге НЕ
меняются — телеметрия встраивается в текущие точки падения, чтобы красные
прогоны A1/A2 уже несли причину.

**Выход:** в `RotationTimeoutTests.cs` есть `ParseJournalOpPhase`,
`RotationDiagAsync`, счётчик тиков по journal-фазам в циклах H/K кейса AC10,
сообщения ассертов падения H/K дополнены полным диагностическим контекстом.

**Проверка:** сборка Release — 0 errors/0 warnings (лог
`/tmp/pgw-kfw-t01-build-a0.log`); в файле больше нет обращений к методам,
которых не существует; AC11/AC12 не изменены (`git diff --stat` — только
`RotationTimeoutTests.cs`).

**Связь со spec:** §5.1 (диагностический контекст), §4.1 («сначала
телеметрия»), §6 A0.

**Подпункты для слабой модели** (файл всех подпунктов:
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs`):

**1.1. Добавь новый приватный статический метод `ParseJournalOpPhase`**
в класс `RotationTimeoutTests` — сразу ПОСЛЕ закрывающей скобки метода
`TryReadFinishedUnix` (метод, читающий `finished_unix` из JSON; заканчивается
строками `finishedUnix = 0; return false;` и `}`) и ПЕРЕД комментарием,
начинающимся словами «Вычисляемый backstop финала». Вставь точно этот код:

```csharp
    // op/phase journal-записи work/<C> (null — битый JSON или полей нет):
    // ключ счётчика тиков по journal-фазам фаз H/K.
    private static (string Op, string Phase)? ParseJournalOpPhase(string workJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(workJson);
            if (doc.RootElement.TryGetProperty("op", out var op) && op.ValueKind == JsonValueKind.String
                && doc.RootElement.TryGetProperty("phase", out var phase) && phase.ValueKind == JsonValueKind.String)
                return (op.GetString()!, phase.GetString()!);
        }
        catch (JsonException)
        {
            // битый JSON журнала — в счётчик фаз не попадает
        }

        return null;
    }
```

**1.2. Добавь новый приватный асинхронный метод `RotationDiagAsync`** в класс
`RotationTimeoutTests` — сразу ПОСЛЕ закрывающей скобки метода
`DiagContextAsync` (метод с сигнатурой `private async Task<string>
DiagContextAsync(string cluster)`). Вставь точно этот код:

```csharp
    // Полный диагностический контекст фаз H/K AC10: журнал work/<C> + хвост
    // journalTexts, счётчик тиков по journal-фазам, живость заявок и исход,
    // наличие ключей снапшота (различает «слепой снапшот» от «слепого
    // преф-чека»), прямая describe-проба тестом (фактический текст ошибки
    // брокера/клиента при явном RequestTimeout 10 с) и CreatedClients фабрики
    // (маркер Unhealthy-цикла пересозданий). Сборщик не бросает: ошибки
    // чтений и пробы попадают в текст, не маскируя причину падения.
    private async Task<string> RotationDiagAsync(
        string cluster, IReadOnlyList<string> journalTexts, IReadOnlyDictionary<string, int> phaseTicks)
    {
        var work = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
        var rotTicket = await fixture.GetAsync($"/kafkaworker/rotations/{cluster}");
        var caTicket = await fixture.GetAsync($"/kafkaworker/ca_rotations/{cluster}");
        var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
        var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        var appPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
        var caPem = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");

        string probe;
        if (endpoints is null)
        {
            probe = "skip: endpoints key is null (blind snapshot branch)";
        }
        else
        {
            try
            {
                using var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build();
                var view = await admin.DescribeClusterAsync(
                    new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(10) },
                    TestContext.Current.CancellationToken);
                probe = $"ok: brokers={view.Nodes.Count}";
            }
            catch (Exception e)
            {
                probe = $"fail: {e.Message}";
            }
        }

        var createdClients = ((KafkaAdminClientFactory)fixture.AdminFactory).CreatedClients;
        var phases = string.Join(",", phaseTicks.Select(kv => $"{kv.Key}={kv.Value}"));
        return $"work={work ?? "<none>"}; rotations={rotTicket ?? "<none>"}; "
            + $"ca_rotations={caTicket ?? "<none>"}; outcome={outcome ?? "<none>"}; "
            + $"snapshot: endpoints={endpoints is not null} app_password={appPassword is not null} "
            + $"admin_password={adminPassword is not null} ca_pem={caPem is not null}; "
            + $"describe probe={probe}; createdClients={createdClients}; "
            + $"phase ticks: {phases}; journal tail: {string.Join(" | ", journalTexts.TakeLast(6))}";
    }
```

Примечание: `fixture` — поле класса (параметр первичного конструктора);
`KafkaAdminClientFactory` доступен через существующий `using
KafkaWorker.Provisioning.Kafka;`, `DescribeClusterOptions` — через
существующий `using Confluent.Kafka.Admin;`, `JsonDocument` — через
существующий `using System.Text.Json;`. Ничего дополнительно не импортируй.

**1.3. В методе `BothTicketsPasswordAndCa_PlayThroughWithoutExpiry`** (кейс
AC10) СРАЗУ ПОСЛЕ строки `var journalTexts = new List<string>();` вставь
строку:

```csharp
            var phaseTicks = new Dictionary<string, int>(); // "op:phase" → сколько тиков вернули фазу
```

Затем в цикле фазы H (цикл с `var hDeadline = DateTimeOffset.UtcNow.AddSeconds(240);`)
внутри блока `if (workNow is not null) { ... }` СРАЗУ ПОСЛЕ строки
`journalTexts.Add(workNow);` вставь:

```csharp
                    if (ParseJournalOpPhase(workNow) is { } opPhase)
                        phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                            phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
```

И замени ассерт после цикла H — старый блок:

```csharp
            passwordSeenDone.Should().BeTrue(
                "H must have completed within the phase budget (with a LIVE ca ticket — deadlock broken); "
                + "journal (last phases): " + string.Join(" | ", journalTexts.TakeLast(6)));
```

на новый:

```csharp
            passwordSeenDone.Should().BeTrue(
                "H must have completed within the phase budget (with a LIVE ca ticket — deadlock broken); "
                + await RotationDiagAsync(cluster, journalTexts, phaseTicks));
```

**1.4. В том же методе, в цикле фазы K** (цикл с
`var kDeadline = DateTimeOffset.UtcNow.AddSeconds(300);`) внутри блока
`if (workNow is not null) { ... }` СРАЗУ ПОСЛЕ строки
`journalTexts.Add(workNow);` вставь тот же инкремент:

```csharp
                    if (ParseJournalOpPhase(workNow) is { } opPhase)
                        phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                            phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
```

И замени ассерт после цикла K — старый блок:

```csharp
            caDone.Should().BeTrue(
                "K must have completed within the phase budget; journal: "
                + string.Join(" | ", journalTexts.TakeLast(6)));
```

на новый:

```csharp
            caDone.Should().BeTrue(
                "K must have completed within the phase budget; "
                + await RotationDiagAsync(cluster, journalTexts, phaseTicks));
```

Больше ничего в файле не меняй.

---

### Шаг 2 (фаза A1): изолированный прогон кейса AC10 на main-базе

- [x] Шаг 2 выполнен — **СТАТУС: ИСПОЛНЕН, изолят ЗЕЛЁНЫЙ** (1/1, 1 м 5 с;
  лог `/tmp/pgw-kfw-t01-isolate-1.log`).

**Вход (предусловие):** шаг 1 закоммичен; сборка Release зелёная.

**Действие (кодер):**
1. Фоново собрать (если после коммита шага 1 не собрано):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   → лог `/tmp/pgw-kfw-t01-build-a0.log` (дописать).
2. Фоново запустить ИЗОЛЯТ (только проект KafkaWorker.IntegrationTests, без
   соседних сборок — Р9):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~RotationTimeoutTests.BothTicketsPasswordAndCa_PlayThroughWithoutExpiry"`
   → ПОЛНЫЙ лог `/tmp/pgw-kfw-t01-isolate-1.log`. Мониторинг по базовым
   правилам §13: старт-чек ≤15 с, контроль каждые 2 мин (по task_id), дольше
   5 мин — онлайн-анализ лога (тест не останавливать).
3. Дождаться финальной строки прогона, зафиксировать исход.
4. Зачистка после прогона (страховочный гейт): убедиться, что остаточных
   контейнеров/сетей тестового прогона нет
   (`docker ps -a --format '{{.Names}}' | grep '^kfw-'`; `docker network ls --format '{{.Name}}' | grep '^kfw-net'`);
   остатки СВОЕГО прогона (имена с RunTag из лога) удалить
   (`docker rm -f <имена>` / `docker network rm <сети>`); dev-стендовые
   объекты НЕ трогать.

**Выход:** зафиксированный исход изолята + полный лог; среда чистая.

**Проверка:** лог `/tmp/pgw-kfw-t01-isolate-1.log` содержит итоговую строку
`Passed!`/`Failed!` с числом тестов; `docker network ls | grep kfw-net` —
пусто (после зачистки).

**Связь со spec:** §6 A1, Р1, Р6, Р9; изолят эквивалентен main-базе бага.

**Ветвление (закрыто spec, НЕ развилка):**
- Зелёный изолят → шаг 3 (A2).
- Красный изолят → шаг 2 не повторять; СРАЗУ снять улики ДО зачистки:
  `docker ps -a` (фильтр `kfw-`), `docker network ls`, `docker logs <контейнер
  брокера кластера rotto<RunTag>>` — каждый в свой файл
  `/tmp/pgw-kfw-t01-evidence-*.log`; из лога прогона выписать строки
  `[kfw-adminfactory]` и блок диагностики `RotationDiagAsync` из сообщения
  падения. Потом зачистка → шаг 4 (A3, разбор по D2-критериям).

---

### Шаг 3 (фаза A2): полная серия solutionx — ТОЛЬКО при зелёном A1

- [x] Шаг 3 выполнен — **СТАТУС: ИСПОЛНЕН** (AC10 в серии ЗЕЛЁНЫЙ; красные —
  вне области t01: 17 PgWorker E2E, 1 Valkey, 1 KafkaWorker TopicLifecycle;
  первый запуск серии прерван из-за невыполненного E2ePrepare — подготовка
  выполнена, серия перезапущена как исправление подготовки окружения; всё —
  в journal.md).

**Вход (предусловие):** шаг 2 = зелёный изолят.

**Действие (кодер):**
1. Фоново запустить серию (та же команда, что ловила баг):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`
   → ПОЛНЫЙ лог `/tmp/pgw-kfw-t01-series-1.log`. Мониторинг §13 (старт-чек
   ≤15 с; каждые 2 мин; серии >5 мин — онлайн-анализ по фазам лога; никаких
   перезапусков).
2. Дождаться финальной строки, зафиксировать исход серии.
3. Зелёная серия → зачистка (как в шаге 2, п.4) → шаг 4 (A3, ветка D4).
4. Красный AC10 → БЕЗ перезапуска: улики ДО зачистки —
   `docker ps -a --format '{{.Names}}' | grep '^kfw-'` > файл; `docker network
   ls --format '{{.Name}}' | grep '^kfw-net'` > файл; `docker logs` контейнера
   брокера кластера `rotto<RunTag>-broker1` > файл; строки `[kfw-adminfactory]`
   из лога серии > файл (все — `/tmp/pgw-kfw-t01-evidence-*.log`); блок
   `RotationDiagAsync` из сообщения падения AC10 выписать в journal.md. Затем
   зачистка → шаг 4 (A3, разбор по матрице).

**Выход:** зафиксированный исход полной серии + улики при красном; среда
чистая.

**Проверка:** `/tmp/pgw-kfw-t01-series-1.log` содержит финальную строку
сводки; после зачистки `docker network ls | grep kfw-net` — пусто.

**Связь со spec:** §6 A2, Р1; нагрузочная гипотеза D1 проверяется именно
серей.

---

### Шаг 4 (фаза A3): разбор по матрице «диагноз → действие» (§5.4)

- [x] Шаг 4 выполнен — **СТАТУС: ИСПОЛНЕН** (коммит `c9d4a1a1`: по критериям
  ступеней зафиксирован D4; ОПОРОВЕРГНУТ приёмочной серией — диагноз
  пересмотрен D4→D2, разбор с телеметрией — коммит `d1b9a8be` и spec §3.1/§3.2;
  ветка D2 исполняется в этом же проходе по решению пользователя Р10).

**Вход (предусловие):** выполнены A1 и (при зелёном A1) A2; логи и улики
сохранены; среда чистая.

**Действие (кодер):** создать
`docs/superpowers/2026-10-23-t01-kafka-rotation-timeout/journal.md` и
зафиксировать диагноз, выбрав РОВНО ОДНУ ветку матрицы по критериям ниже
(критерии определены spec §5.4 — выбор по телеметрии, НЕ развилка):

| Ветка | Критерии (по телеметрии §5.1 и уликам A1/A2) |
|---|---|
| **D1 — нагрузка серии** | изолят A1 зелёный И серия A2 красная по AC10 И телеметрия красного: describe-пробы падают таймаутом/перегрузкой при живом контейнере брокера. Доп-условие для повышения `requestTimeout` (шаг 5, п. D1): упор ровно в границу 10 с — пробы отменяются по `RequestTimeout` |
| **D2 — систематика (клиент-сторона воркера)** | красный И изолят (обе ступени), ЛИБО телеметрия: describe-проба тестом УСПЕШНА при waiting-джобе, ИЛИ `createdClients` растёт от тика к тику, ИЛИ в логе раннера гэп-отказы «клиент в dispose» (маркеры `[kfw-adminfactory]`) |
| **D3 — среда** | докер-улики показывают осиротевшие сети/контейнеры прошлых серий, исказившие прогон (UpAsync при этом прошёл — деградация после подъёма) |
| **D4 — невоспроизведённый** | изолят зелёный И серия зелёная |

В journal.md записать: ветку, улики (со ссылкой на файлы
`/tmp/pgw-kfw-t01-evidence-*.log` и строки логов), команд/даты/исходы обеих
ступеней. Повторные прогоны — ТОЛЬКО в объёме, разрешённом веткой (см. шаг 5),
после полного разбора.

**Выход:** journal.md существует; ветка матрицы зафиксирована с уликами.

**Проверка:** раздел «Диагноз» journal.md заполнен и ссылается на
существующие файлы улик/логов; выбрана ровно одна ветка D1–D4.

**Связь со spec:** §6 A3, §5.4; закрытие «неразличимости пост-фактум» (§3.3).

---

### Шаг 5 (фаза A4): реализация — механика §5.2 (всегда) + ветка матрицы

- [x] Шаг 5 выполнен — **СТАТУС: ИСПОЛНЕН** (коммит `86953df0`: backstop,
  PollMs/TickInterval, транзиент-толерантные тики; изолят зелёный; ветка D4
  «допправок нет» — по тогдашнему диагнозу; прод-ветка D2 исполняется
  шагом 7 по Р10).

**Вход (предусловие):** шаг 4 завершён, ветка матрицы зафиксирована.

**Действие (кодер):** ВСЕГДА применить подпункты 5.1–5.6 (механика §5.2 —
страховка в любой ветке, Р2/Р8). Затем — блок выбранной ветки:

- **D1:** применить подпункт 5.D1 (повышение `requestTimeout` фабрики
  фикстуры до 30 с) — ТОЛЬКО если телеметрия доказала упор ровно в 10-с
  границу; иначе (пробы падают не по RequestTimeout) 5.D1 НЕ применяется, а
  решение «не повышать» фиксируется в journal.md. Прод-код не меняется.
- **D2:** прод-ветка — в этой редакции плана вынесена в ОТДЕЛЬНЫЙ шаг 7
  (фаза A4-D2, решение пользователя Р10); здесь не исполняется повторно.
- **D3:** чистка среды по рунбуку (`docker network prune -f` + разбор
  остатков) — зафиксировать в journal.md; затем ЕДИНСТВЕННЫЙ разрешённый
  повтор полной серии (команда шага 3, лог `/tmp/pgw-kfw-t01-series-2.log`)
  после фиксации решения о повторе в journal.md; далее — по исходу повтора
  (зелёная → диагноз «среда» подтверждён; красная → разбор телеметрии
  повторного красного и переход к D1/D2-критериям). Тест-код не виноват:
  §5.1/§5.2 вливаются как страховка.
- **D4:** дополнительных правок нет (страховка §5.1/§5.2 уже влита
  подпунктами 5.1–5.6); пункт закрывается как невоспроизведённый (шаг 9).

После применения всех правок — фоново собрать:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ `/tmp/pgw-kfw-t01-build-a4.log`; фоново прогнать изолят AC10 (команда и
правила шага 2, п.2) → `/tmp/pgw-kfw-t01-isolate-2.log`; зачистка. Коммит:
`test(kfw): t01 A4 — механика ожидания фаз H/K: вычисляемый backstop, PollMs/TickInterval, транзиент-толерантные тики`.

**Выход:** механика §5.2 влита (литеральные бюджеты 240/300 с и паузы 3000 мс
в циклах H/K отсутствуют); ветка матрицы исполнена; сборка зелёная; изолят
AC10 зелёный; все числовые изменения обоснованы в journal.md.

**Проверка:** `grep -n "AddSeconds(240)\|AddSeconds(300)\|Task.Delay(3000"
src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs` —
внутри метода `BothTicketsPasswordAndCa_PlayThroughWithoutExpiry` пусто
(3000-паузы AC11/AC12/UpAsync НЕ тронуты — сверить по контексту строк);
сборка 0 warnings; `/tmp/pgw-kfw-t01-isolate-2.log` — `Passed!`; финальные
ассерты AC10 (после циклов: заявки null, `workFinal` содержит `rotate-ca` +
`done`, `journalTexts` без `expired`, исход `kind=ca`/`done`, пароль и CA
изменены, порядок H→K) сохранены дословно.

**Связь со spec:** §5.2 (механика), §5.4 (ветки), §6 A4, Р2, Р3 (счётчик
стагнации H/K НЕ вводится — в п.5.1 отражено комментарием), Р5 (только D1
при доказанном упоре).

**Подпункты для слабой модели** (файл всех подпунктов:
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs`;
применять строго по порядку 5.1 → 5.6):

**5.1. Добавь константы и статический метод `RotationBackstopSec`** в класс
`RotationTimeoutTests` — сразу ПОСЛЕ закрывающей скобки метода
`ConvergeBackstopSec` (метод с сигнатурой `private static int
ConvergeBackstopSec(IReadOnlyList<double> tickSeconds, int? remainingFirst)`,
заканчивается `return Math.Clamp((int)(batches * avgTickSec * 1.5 + 60), 300, 1800);`).
Вставь точно этот код:

```csharp
    // Вычисляемый backstop фаз H/K AC10: пол = 2 rolling-фазы ×
    // BrokerBootSec(100) + гейт-хвост 60 с = 260 с (литеральные 240/300 с
    // ниже механики самого теста — удалены); при наличии замеров темпа
    // (длительность успешных тиков) — batches(2) × средний тик × запас 1.5
    // + 60 с, кап 1800 с (по образцу ConvergeBackstopSec). Счётчик стагнации
    // по тикам для H/K НЕ вводится: передержка гейта waiting-cluster
    // канонична и не ограничена по сроку — красит только этот backstop.
    private const int RotationBackstopFloorSec = 260;
    private const int RotationBackstopCapSec = 1800;

    private static int RotationBackstopSec(IReadOnlyList<double> tickSeconds)
    {
        if (tickSeconds.Count == 0)
            return RotationBackstopFloorSec;
        var avgTickSec = tickSeconds.Average();
        return Math.Clamp(
            (int)(2 * avgTickSec * 1.5 + 60), RotationBackstopFloorSec, RotationBackstopCapSec);
    }
```

**5.2. Добавь методы `HPhaseDoneAsync` и `KPhaseDone`** в класс
`RotationTimeoutTests` — сразу ПОСЛЕ метода `ParseJournalOpPhase` (добавлен
ранее; заканчивается `return null;` и `}`). Вставь точно этот код:

```csharp
    // Признак «фаза H доиграна»: journal op=rotate + phase=done, ЛИБО заявка
    // удалена фазой B + исход password-app/done (исход пишется до journal
    // done). Проверка «не expired» — на каждом чтении журнала.
    private async Task<bool> HPhaseDoneAsync(string cluster, string? workNow)
    {
        if (workNow is not null)
        {
            workNow.Should().NotContain(
                "expired", "started rotations are never expired (triple guard)");
            if (workNow.Contains("\"op\":\"rotate\"", StringComparison.Ordinal)
                && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal))
                return true;
        }

        if (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}") is null)
        {
            var outcomeH = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            if (outcomeH is not null
                && outcomeH.Contains("\"kind\":\"password-app\"", StringComparison.Ordinal)
                && outcomeH.Contains("\"outcome\":\"done\"", StringComparison.Ordinal))
                return true; // исход H done записан (до journal done)
        }

        return false;
    }

    // Признак «фаза K доиграна»: journal op=rotate-ca + phase=done;
    // «не expired» — на каждом чтении журнала.
    private static bool KPhaseDone(string? workNow)
    {
        if (workNow is null)
            return false;
        workNow.Should().NotContain("expired", "K window completes without expiration");
        return workNow.Contains("\"op\":\"rotate-ca\"", StringComparison.Ordinal)
            && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal);
    }
```

**5.3. Измени сигнатуру и точку красного метода `TickAsync`.** Найди метод с
сигнатурой `private async Task<TimeSpan> TickAsync(string label, string
cluster, Func<Task<Result>> tick, Queue<string> diag)`. Замени ТОЛЬКО КОД
МЕТОДА — от строки сигнатуры (начинается с `private async Task<TimeSpan>
TickAsync(`) до закрывающей скобки метода `}` включительно. Комментарий над
методом (строки, начинающиеся с `//`, между концом предыдущего метода и
сигнатурой `TickAsync`) НЕ трогай и новых строк-комментариев НЕ вставляй.
Блок замены (точно этот код, без комментариев):

```csharp
    private async Task<TimeSpan> TickAsync(
        string label, string cluster, Func<Task<Result>> tick, Queue<string> diag,
        Func<Task<string>>? extraDiag = null)
    {
        for (var attempt = 1; ; attempt++)
        {
            var started = DateTimeOffset.UtcNow;
            var result = await tick();
            if (result.IsSuccess)
                return DateTimeOffset.UtcNow - started;

            EnqueueDiag(diag, $"{label}: tick #{attempt} failed — {result.Error!.Message}");
            if (attempt >= FailedTickLimit)
            {
                var context = await DiagContextAsync(cluster);
                var extra = extraDiag is null ? "" : "; " + await extraDiag();
                throw new Xunit.Sdk.XunitException(
                    $"{label}: stagnation — {FailedTickLimit} consecutive failed ticks; {context}{extra}; "
                    + $"tick tail: {string.Join(" | ", diag)}");
            }

            await Task.Delay(TickIntervalMs, TestContext.Current.CancellationToken);
        }
    }
```

**5.4. В методе `BothTicketsPasswordAndCa_PlayThroughWithoutExpiry` замени
цикл фазы H ЦЕЛИКОМ.** Найди блок, начинающийся строкой
`var passwordSeenDone = false;` и заканчивающийся ассертом
`passwordSeenDone.Should().BeTrue(...)` включительно (в блок входят:
объявления `passwordSeenDone`/`journalTexts`/`phaseTicks`, комментарий
«Фаза H (бюджет 240 c)…», цикл `while (DateTimeOffset.UtcNow < hDeadline && !passwordSeenDone)`,
ассерт `passwordSeenDone.Should().BeTrue(...)`). Удали весь этот блок и на
его место вставь точно этот код:

```csharp
            var passwordSeenDone = false;
            var journalTexts = new List<string>();
            var phaseTicks = new Dictionary<string, int>(); // "op:phase" → сколько тиков вернули фазу
            var diag = new Queue<string>();
            var hTickSeconds = new List<double>();

            // Фаза H: тики — не чаще TickIntervalMs, опрос journal/заявки/исхода —
            // немедленно после тика и каждые PollMs; тики транзиент-толерантны
            // (TickAsync, 3 подряд Failed → красный с диагностикой); красный —
            // по вычисляемому backstop (RotationBackstopSec) с полным
            // диагностическим контекстом, не по литеральным часам. Признак
            // «H доиграна» — HPhaseDoneAsync; порядок тиков H→K —
            // последовательный (не перемежающийся).
            var hStarted = DateTimeOffset.UtcNow;
            var hNextTickAt = DateTimeOffset.UtcNow; // первый тик — немедленно
            while (!passwordSeenDone)
            {
                if (DateTimeOffset.UtcNow >= hNextTickAt)
                {
                    var took = await TickAsync("phase-h", cluster,
                        async () => await passwordRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag,
                        extraDiag: () => RotationDiagAsync(cluster, journalTexts, phaseTicks));
                    hTickSeconds.Add(took.TotalSeconds);
                    hNextTickAt = DateTimeOffset.UtcNow.AddMilliseconds(TickIntervalMs);

                    var workAfterTick = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                    if (workAfterTick is not null)
                    {
                        journalTexts.Add(workAfterTick);
                        if (ParseJournalOpPhase(workAfterTick) is { } opPhase)
                            phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                                phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
                    }

                    passwordSeenDone = await HPhaseDoneAsync(cluster, workAfterTick);
                    if (passwordSeenDone)
                        break;
                }

                var hBackstopSec = RotationBackstopSec(hTickSeconds);
                if (DateTimeOffset.UtcNow - hStarted >= TimeSpan.FromSeconds(hBackstopSec))
                    throw new Xunit.Sdk.XunitException(
                        $"phase H: backstop {hBackstopSec} s exhausted without the rotation completing "
                        + $"(live ca ticket — deadlock broken by priority; the budget follows the rotation "
                        + $"mechanics, not literal clocks); "
                        + $"{await RotationDiagAsync(cluster, journalTexts, phaseTicks)}; "
                        + $"ticks: {string.Join(" | ", diag)}");

                await Task.Delay(PollMs, ct);
                var workPeek = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workPeek is not null)
                    journalTexts.Add(workPeek);
                passwordSeenDone = await HPhaseDoneAsync(cluster, workPeek);
            }
```

**5.5. В том же методе замени цикл фазы K и удали ассерт после него.**
Сначала найди блок, начинающийся комментарием `// Фаза K (бюджет 300 c): тики
ca-ротатора до финала K4.` и заканчивающийся закрывающей `}` цикла
`while (DateTimeOffset.UtcNow < kDeadline && !caDone) { ... }`. Удали весь
блок и на его место вставь точно этот код:

```csharp
            // Фаза K: та же механика, что H (PollMs-опрос, TickInterval-подача,
            // транзиент-толерантные тики, вычисляемый backstop с полной
            // диагностикой); тики H→K последовательны — общий журнал work/<C>.
            var caDone = false;
            var kTickSeconds = new List<double>();
            var kStarted = DateTimeOffset.UtcNow;
            var kNextTickAt = DateTimeOffset.UtcNow; // первый тик — немедленно
            while (!caDone)
            {
                if (DateTimeOffset.UtcNow >= kNextTickAt)
                {
                    var took = await TickAsync("phase-k", cluster,
                        async () => await caRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag,
                        extraDiag: () => RotationDiagAsync(cluster, journalTexts, phaseTicks));
                    kTickSeconds.Add(took.TotalSeconds);
                    kNextTickAt = DateTimeOffset.UtcNow.AddMilliseconds(TickIntervalMs);

                    var workAfterTick = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                    if (workAfterTick is not null)
                    {
                        journalTexts.Add(workAfterTick);
                        if (ParseJournalOpPhase(workAfterTick) is { } opPhase)
                            phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                                phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
                    }

                    caDone = KPhaseDone(workAfterTick);
                    if (caDone)
                        break;
                }

                var kBackstopSec = RotationBackstopSec(kTickSeconds);
                if (DateTimeOffset.UtcNow - kStarted >= TimeSpan.FromSeconds(kBackstopSec))
                    throw new Xunit.Sdk.XunitException(
                        $"phase K: backstop {kBackstopSec} s exhausted without the CA rotation completing; "
                        + $"{await RotationDiagAsync(cluster, journalTexts, phaseTicks)}; "
                        + $"ticks: {string.Join(" | ", diag)}");

                await Task.Delay(PollMs, ct);
                var workPeek = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workPeek is not null)
                    journalTexts.Add(workPeek);
                caDone = KPhaseDone(workPeek);
            }
```

Затем сразу ПОСЛЕ вставленного цикла, в блоке финальных ассертов, удали
ассерт (он теперь лишний — красный по незавершению даёт backstop цикла):

```csharp
            caDone.Should().BeTrue(
                "K must have completed within the phase budget; "
                + await RotationDiagAsync(cluster, journalTexts, phaseTicks));
```

Все прочие финальные ассерты после циклов (проверки
`/kafkaworker/rotations/...`, `/kafkaworker/ca_rotations/...`,
`workFinal`, `journalTexts.Should().NotContain(t => t.Contains("expired"))`,
`outcome` с `"kind":"ca"`/`"outcome":"done"`, `app_password`, `ca_pem`) —
НЕ трогай.

**5.6. Обнови устаревший комментарий над константами опроса.** Найди
комментарий, начинающийся словами `// Периоды опроса и подачи (spec §5.6):` и
заканчивающийся строкой `// вне области правки.` (перед строкой
`private const int PollMs = 250;`). Замени его на:

```csharp
    // Периоды опроса и подачи (spec §5.6; фазы H/K AC10 — вычисляемый
    // backstop + PollMs-опрос + TickInterval-подача): завершённая работа
    // обнаруживается мгновенно — опрос состояния дешёв (etcd-GET,
    // миллисекунды), тик с exec дорог (JVM-старт CLI, секунды); интервал
    // тика страхует от спама дешёвых waiting-тиков и ретраев хелпера.
    // Поллинг AC11 и UpAsync (3000 мс) — вне области правки.
```

**5.D1 (ТОЛЬКО ветка D1 с доказанным упором в 10 с).** В файле
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClusterFixture.cs`
найди инициализацию поля `_adminFactory`:

```csharp
    private readonly KafkaAdminClientFactory _adminFactory = new(
        TimeSpan.FromSeconds(10), logger: AdminFactoryConsoleLogger.Instance);
```

и замени `TimeSpan.FromSeconds(10)` на `TimeSpan.FromSeconds(30)`. Рядом (в
комментарии над полем, если он есть, или новой строкой над инициализацией)
добавь строку комментария:
`// requestTimeout 30 с (t01/D1): пробы DescribeCluster под пиковой нагрузкой полной серии упираются в 10-с границу — транзит и DrainGrace пересчитываются автоматически; прод-профиль фабрики не трогается.`
Больше в файле ничего не меняй.

---

### Шаг 6 (фаза A5): приёмка

- [x] Шаг 6 выполнен — **СТАТУС: ИСПОЛНЕН ЧАСТИЧНО, диагноз пересмотрен**:
  сборка 0 warnings; юниты KafkaWorker 359/359; изолят RotationTimeoutTests
  3/3; полный KafkaWorker.IntegrationTests — AC10 ЗЕЛЁНЫЙ (73/74, MetricsTests
  вне области); приёмочная серия solutionx — КРАСНАЯ по AC10 (4 м 53 с) с
  полной телеметрией → диагноз D4→D2 (корень §3.1 spec); сопутствующее —
  55 PgWorker E2E мгновенных fail-fast устаревшего штампа E2ePrepare (Р16),
  KafkaWorker-исходы серии валидны. Продолжение — шаги 7–8 (Р10/Р15).

**Вход (предусловие):** шаг 5 закоммичен; сборка зелёная; изолят AC10 зелёный.

**Действие (кодер):** ВСЕ прогоны — фоново, с мониторингом §13 и ПОЛНЫМИ
логами; после КАЖДОГО прогона — зачистка `kfw-*` (шаг 2, п.4). Последовательность:

1. Свежая сборка (отдельной командой): `DOTNET_CLI_UI_LANGUAGE=en dotnet
   build src/PgWorker.slnx -c Release --nologo` → лог
   `/tmp/pgw-kfw-t01-accept-build.log` — 0 warnings.
2. Юниты KafkaWorker: `DOTNET_CLI_UI_LANGUAGE=en dotnet test
   src/tests/KafkaWorker.UnitTests -c Release --no-build` → лог
   `/tmp/pgw-kfw-t01-accept-units.log` — зелёные (включая
   `KafkaAdminClientFactoryTests`; при D2 — юниты области фикса).
3. Изолят всех трёх кейсов: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1
   dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build
   --filter "FullyQualifiedName~RotationTimeoutTests"` → лог
   `/tmp/pgw-kfw-t01-accept-isolate.log` — зелёный.
4. Полный параллельный прогон проекта: `DOTNET_CLI_UI_LANGUAGE=en
   PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c
   Release --no-build` → лог `/tmp/pgw-kfw-t01-accept-full.log` — зелёный
   100%; маркеры из лога: `grep -c "found 2"` = 0,
   `grep -c "Broker handle destroyed"` = 0, `grep -c "SIGABRT"` = 0.
5. **Только при D2** (прод-правка) — docker-E2E PgWorker на свежем Release:
   перенесено в шаг 8 (фаза A5-D2); здесь не исполняется.
6. **Только при D4** — зелёная ПОЛНАЯ серия solutionx (та же команда, что в
   шаге 3) → лог `/tmp/pgw-kfw-t01-accept-series.log` — зелёная (приёмочный
   прогон после вливания страховки, НЕ перезапуск упавшего).
7. Все исходы + использованные команды — в journal.md (раздел «Прогоны»).

**Выход:** все приёмочные прогоны зелёные, логи сохранены, среда чистая,
journal.md дополнен.

**Проверка:** каждый лог содержит финальную строку `Passed!` (или сводку без
`Failed!`); маркеры п.4 = 0; после последнего прогона
`docker network ls | grep kfw-net` — пусто.

**Связь со spec:** §6 A5, §9 п.1–3 и п.7 (D2), Р7.

---

### Шаг 7 (фаза A4-D2): прод-фикс PasswordRotator — arch-first + юнит-кейсы

- [x] Шаг 7 выполнен — **СТАТУС: ИСПОЛНЕН** (arch-коммит `a929495a` строго до
  кодового `71fff40f`: правило afterCommit-хвоста в arch/16 §5 H; тихий пропуск
  `Success(false)` в двух waiting-точках PasswordRotator; юниты контракта —
  361/361 зелёные)

**Вход (предусловие):** диагноз D2 зафиксирован в journal.md (шаги 4/6);
worktree чист; существующие юниты зелёные (`PasswordRotatorTests` — на
последнем прогоне 359/359).

**Действие (кодер):** строго по порядку (arch-first — нарушение порядка =
сломанный контракт):
1. Применить подпункт 7.1 (правка `arch/16-kafkaworker.md` §5 H — правило
   afterCommit-хвоста). Закоммитить СРАЗУ, ДО правок кода:
   `docs(arch): t01 — правило afterCommit-хвоста §5 H: waiting-фазы только при живой заявке`.
2. Применить подпункты 7.2–7.3 (прод-фикс `PasswordRotator.cs`: тихий пропуск
   `Success(false)` в двух waiting-точках хвоста — ca-window и слепой преф-чек
   describe; Р11/Р12/Р13). Прод-дифф — ТОЛЬКО эти две точки; всё прочее
   (`WaitOrExpireAsync`, гварды t10 при живой заявке, слепой снапшот
   `:141–149` — уже тихий, образец; механика K; фабрика) — нетронуто (§5.3).
3. Применить подпункты 7.4–7.6 (юнит-кейсы §5.5: замена теста ca-window-хвоста
   на новый контракт + два новых кейса; кейс 4 §5.5 «живая заявка + открытое
   ca-окно → waiting-ca-window как раньше» уже покрыт существующим
   `Run_OpenCaWindow_WaitsCaWindow` — остаётся как есть, дубль не добавляется).
4. Фоново собрать → лог `/tmp/pgw-kfw-t01-d2-build.log` (0 warnings); фоново
   прогнать юниты KafkaWorker целиком:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --no-build`
   → лог `/tmp/pgw-kfw-t01-d2-units.log` — зелёные, включая новые кейсы.
   Зачистка не требуется (юниты без docker).
5. Зафиксировать решения Р11–Р14 (правило, форма `Success(false)`, отказ от
   last_error-переписи, форма юнит-тестов) с обоснованиями в journal.md.
   Закоммитить: `fix(kfw): t01 — afterCommit-хвост не перезаписывает журнал waiting-фазами (тихий пропуск Success(false))`.

**Выход:** канон §5 H правлен ПЕРВОЙ правкой и соответствует коду; прод-фикс
в объёме §5.5 влит; юнит-кейсы зелёные; решения в journal.md.

**Проверка:** `git log --oneline` — arch-коммит СТРАГО ДО кодового;
`git diff HEAD~2 -- src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs`
— ровно две изменённые точки (ca-window, преф-чек), каждая добавляет
`if (ticket.Value is null) return Result<bool>.Success(false);` с комментарием;
`grep -n "rotated-commit\|waiting-ca-window\|waiting-cluster" src/KafkaWorker.Core src/KafkaWorker.Provisioning`
без новых изменений вне PasswordRotator; юнит-лог `…-d2-units.log` —
`Passed!` (все, включая `Run_AfterCommitTailDescribeTransient_TailKeptAndResumed`,
`Run_AfterCommitTailAndOpenCaWindow_TailKeptAndResumed`,
`Run_LiveTicketDescribeFail_WaitsWaitingClusterAsBefore`,
`Run_OpenCaWindow_WaitsCaWindow`, `Run_AfterCommitTailWithoutEndpoints_NoOpWithoutExpiry`).

**Связь со spec:** §5.5 (контракт фикса: правило, обе точки, форма
`Success(false)`, юнит-кейсы), §6 A4-D2, §4 п.5–6, Р10–Р14; §5.3 (что не
трогается).

**Подпункты для слабой модели:**

**7.1. Правка документации: вставь правило afterCommit-хвоста в файл**
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/arch/16-kafkaworker.md`.
Найди в разделе «### H. PasswordRotator» абзац, начинающийся словом
«Guard'ы:» (первая строка абзаца: `Guard'ы: кластер не поднят (нет
endpoints/кредов) или не отвечает`). Абзац заканчивается предложением:
`стагнация начатой ротации — зона панельного алерта
`kafka-rotation-stale`.` (после него — пустая строка и абзац, начинающийся
словами «Отказ между фазами безопасен»). СРАЗУ ПОСЛЕ этого предложения, в
конце того же абзаца (перед пустой строкой и абзацем «Отказ между фазами
безопасен»), добавь новый текст (с новой строки, переносы строк — как ниже):

```markdown
Waiting-фазы журнала пишутся только при живой заявке: afterCommit-хвост
(заявка снята фазой B, журнал `rotated-commit`/`phase-c`, финал не
достигнут) в точках ожидания (окно CA, слепой снапшот, слепой преф-чек)
журнал не перезаписывает — тихая передержка тика; маркер мутации —
единственный носитель возобновляемости фазы C, его затирание waiting-фазой
делало бы финал недостижимым (ротация-«зомби»). Стагнация хвоста наблюдаема
по застывшей фазе (`updated_unix` не растёт) — зона панельного алерта
`kafka-rotation-stale`.
```

Больше ничего в файле не меняй.

**7.2. Правка кода: ca-window-гвард — тихий пропуск для afterCommit-хвоста.**
Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs`,
метод `RunRoleAsync`. Найди блок (внутри комментария «Guard: открытое окно
CA-ротации K»):

```csharp
        if (nextKey.Value is not null || nextPem.Value is not null)
            return await WaitOrExpireAsync(
                snap, role, ticket.Value, "waiting-ca-window", mutationLive: true, ct);
```

Замени его на:

```csharp
        if (nextKey.Value is not null || nextPem.Value is not null)
        {
            // afterCommit-хвост (заявка снята фазой B) журнал не перезаписывает:
            // waiting-фазы пишутся только при живой заявке — затирание маркера
            // rotated-commit/phase-c делало бы финал недостижимым
            // (ротация-«зомби»); тихая передержка тика (arch/16 §5 H).
            if (ticket.Value is null)
                return Result<bool>.Success(false);
            return await WaitOrExpireAsync(
                snap, role, ticket.Value, "waiting-ca-window", mutationLive: true, ct);
        }
```

**7.3. Правка кода: слепой преф-чек describe — тихий пропуск для
afterCommit-хвоста.** Тот же файл и метод. Найди блок (сразу после
комментария «Преф-чек: кластер должен отвечать DescribeCluster ДО rolling»):

```csharp
        var alive = await WaitForBrokersAsync(snap, 1, ct);
        if (!alive.Value)
            return await WaitOrExpireAsync(snap, role, ticket.Value, "waiting-cluster",
                RoleMutationLive(journalState.Value, role), ct);
```

Замени его на:

```csharp
        var alive = await WaitForBrokersAsync(snap, 1, ct);
        if (!alive.Value)
        {
            // afterCommit-хвост журнал не перезаписывает (arch/16 §5 H):
            // waiting-фазы — только при живой заявке; хвост доигрывается
            // следующим тиком по journal-маркеру.
            if (ticket.Value is null)
                return Result<bool>.Success(false);
            return await WaitOrExpireAsync(snap, role, ticket.Value, "waiting-cluster",
                RoleMutationLive(journalState.Value, role), ct);
        }
```

**7.4. Замени юнит-тест ca-window-хвоста на новый контракт.** Файл
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/src/tests/KafkaWorker.UnitTests/Provisioning/PasswordRotatorTests.cs`.
Найди метод (целиком, с атрибутом `[Fact]` и комментарием внутри):

```csharp
    [Fact]
    public async Task Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry()
    {
        // Arrange: afterCommit-хвост (заявки нет) + живой staging ca_next_*.
        var rig = await NewRig();
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "phase-c", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_key", "next-key");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_pem", "next-pem");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: journal-фаза waiting-ca-window (Kv?-ветка — экспирации нет);
        // ticket_outcomes не создаётся.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-ca-window");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }
```

(если текст внутри немного отличается — ищи метод по ИМЕНИ
`Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry` и заменяй его
целиком, от строки `[Fact]` перед ним до закрывающей `}` метода включительно).
Удали его и на то же место вставь точно этот код:

```csharp
    [Fact]
    public async Task Run_AfterCommitTailAndOpenCaWindow_TailKeptAndResumed()
    {
        // Arrange: afterCommit-хвост — заявки НЕТ (del фазой B), журнал
        // rotate/rotated-commit (сидирован), пароль в etcd уже NEW; окно K
        // открыто (живой staging ca_next_*).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/app_password", "NewPassword0123456789abcdef");
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "rotated-commit", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_key", "next-key");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_pem", "next-pem");
        ReadyCluster(rig.Admin, 2);

        // Act: тик при открытом окне K.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Success; журнал НЕ перезаписан waiting-ca-window — маркер
        // хвоста цел (waiting-фазы — только при живой заявке); исхода нет.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("rotated-commit");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));

        // Act-2: окно K закрылось (staging снят) — хвост доигрывается.
        rig.Etcd.Store.Remove("/kafka/clusters/events/ca_next_key");
        rig.Etcd.Store.Remove("/kafka/clusters/events/ca_next_pem");
        var resumed = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert-2: фаза C доведена, journal done, исход password-app/done.
        resumed.IsSuccess.Should().BeTrue();
        var done = await rig.Journal.ReadAsync("events", CancellationToken.None);
        done.Value!.Phase.Should().Be("done");
        var outcome = rig.Etcd.Store[TicketOutcomes.Key("/kafkaworker", "events")].Value;
        outcome.Should().Contain("\"kind\":\"password-app\"").And.Contain("\"outcome\":\"done\"");
    }
```

**7.5. Добавь новый юнит-тест: хвост + describe-транзиент.** В тот же файл,
СРАЗУ ПОСЛЕ закрывающей `}` метода `Run_AfterCommitTailAndOpenCaWindow_TailKeptAndResumed`
(из подпункта 7.4) и ПЕРЕД методом `Run_ClusterDownFreshTicket_WaitingCluster`,
вставь точно этот код:

```csharp
    [Fact]
    public async Task Run_AfterCommitTailDescribeTransient_TailKeptAndResumed()
    {
        // Arrange: afterCommit-хвост — заявки НЕТ (del фазой B), журнал
        // rotate/rotated-commit (сидирован), пароль в etcd уже NEW; endpoints
        // жив, DescribeCluster слеп (одиночный транзиент преф-чека).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/app_password", "NewPassword0123456789abcdef");
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "rotated-commit", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Admin.ClusterError = new ApplicationException("describe transient");

        // Act: тик в слепом преф-чеке.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Success; журнал НЕ перезаписан waiting-cluster — маркер
        // хвоста цел (корень t01: затирание делало ротацию вечным no-op).
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("rotated-commit");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));

        // Act-2: describe ожил — хвост доигрывается до финала.
        rig.Admin.ClusterError = null;
        ReadyCluster(rig.Admin, 2);
        var resumed = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert-2: фаза C доведена, journal done, исход password-app/done.
        resumed.IsSuccess.Should().BeTrue();
        var done = await rig.Journal.ReadAsync("events", CancellationToken.None);
        done.Value!.Phase.Should().Be("done");
        var outcome = rig.Etcd.Store[TicketOutcomes.Key("/kafkaworker", "events")].Value;
        outcome.Should().Contain("\"kind\":\"password-app\"").And.Contain("\"outcome\":\"done\"");
    }
```

**7.6. Добавь новый юнит-тест: контроль канона живой заявки.** В тот же файл,
СРАЗУ ПОСЛЕ закрывающей `}` метода `Run_AfterCommitTailDescribeTransient_TailKeptAndResumed`
(из подпункта 7.5) и ПЕРЕД методом `Run_ClusterDownFreshTicket_WaitingCluster`,
вставь точно этот код:

```csharp
    [Fact]
    public async Task Run_LiveTicketDescribeFail_WaitsWaitingClusterAsBefore()
    {
        // Arrange: живая СВЕЖАЯ заявка (возраст меньше порога экспирации),
        // endpoints жив, DescribeCluster слеп (отказ преф-чека).
        var rig = await NewRig();
        var freshUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10;
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{freshUnix}},"requested_by":"it"}""");
        rig.Admin.ClusterError = new ApplicationException("describe fail");

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контроль канона — при ЖИВОЙ заявке waiting-cluster пишется
        // как раньше (ожидания с заявкой фиксом не тронуты); заявка жива,
        // исхода нет.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-cluster");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }
```

Больше ничего в файле не меняй (существующие тесты
`Run_AfterCommitTailWithoutEndpoints_NoOpWithoutExpiry` и
`Run_OpenCaWindow_WaitsCaWindow` остаются как есть).

**7.7. Компиляция/сборка.** Выполни в корне
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout`
команду (вывод целиком пиши в файл `/tmp/pgw-kfw-t01-d2-build.log`):

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo
```

Ожидаемый результат: сборка завершается без ошибок и без warnings
(`Warning(s): 0`, `Error(s): 0`). Любое предупреждение — ошибка исполнения
(проект собирается с TreatWarningsAsErrors=true и падает на warning'ах).

---

### Шаг 8 (фаза A5-D2): переприёмка после прод-фикса

- [x] Шаг 8 выполнен — **СТАТУС: ИСПОЛНЕН** (E2ePrepare заново по Р16; сборка
  0 warnings; docker-E2E Scale_AddEmptyShard зелёный; юниты 361/361; полный
  KafkaWorker.IntegrationTests 74/74, маркеры деградации = 0; полная серия
  solutionx — AC10 ЗЕЛЁНЫЙ; PgWorker-фрагмент серии оборван внешним лимитом
  хоста (MSB4166, не тестовый исход) после фиксации ключевого результата;
  зачистка — 0 остатков kfw-*/pgw-*)

**Вход (предусловие):** шаг 7 закоммичен (arch-коммит строго до кодового);
сборка и юниты зелёные.

**Действие (кодер):** ВСЕ прогоны — фоново, с мониторингом §13, полные логи в
файлы; зачистка `kfw-*`/`pgw-*` ПОСЛЕ КАЖДОЙ серии (шаг 2, п.4 + осиротевшие
`pgw-*-net`). Строго по порядку (повтор серии — единственный повтор задачи,
согласован пользователем Р15; это проверка фикса, НЕ «выяснение что было»):

1. **E2ePrepare ЗАНОВО** (код изменился — штамп инвалидируется, Р16; ДО
   любых PgWorker-E2E и серий):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
   → лог `/tmp/pgw-kfw-t01-d2-e2e-prepare.log` (штамп готовности записан).
2. Сборка Release отдельной командой:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   → лог `/tmp/pgw-kfw-t01-d2-accept-build.log` — 0 warnings (свежесть бинаря
   видна выводом этой команды — часть гейта).
3. Docker-E2E PgWorker минимум кейс-маркер (отдельной командой, только
   `--no-build`):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
   → лог `/tmp/pgw-kfw-t01-d2-e2e-scale.log` — зелёный.
4. Юниты KafkaWorker целиком:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --no-build`
   → лог `/tmp/pgw-kfw-t01-d2-units.log` — зелёные.
5. Изолят ротаций:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~RotationTimeoutTests"`
   → лог `/tmp/pgw-kfw-t01-d2-isolate.log` — зелёный.
6. Полный параллельный `KafkaWorker.IntegrationTests`:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build`
   → лог `/tmp/pgw-kfw-t01-d2-full.log`; маркеры: `grep -c "found 2"` = 0,
   `grep -c "Broker handle destroyed"` = 0, `grep -c "SIGABRT"` = 0.
7. Полная серия solutionx (та же команда, что в баге):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~IntegrationTests"`
   → лог `/tmp/pgw-kfw-t01-d2-series.log` — **AC10 зелёный**; внеобластные
   красные (известные: MetricsTests, TopicLifecycle, Valkey Metrics,
   ReassignmentTests-порт, PgWorker E2E артефакты — см. §6 A6) — НЕ чинить,
   зафиксировать исходы; телеметрия при любом красном ротаций — уже в выводе
   прогона (§5.1).
8. Зачистка после КАЖДОЙ серии: остаточных `kfw-<RunTag>-*`/`pgw-*`
   контейнеров/томов нет; `docker network ls | grep 'kfw-net\|pgw-net'` —
   пусто (dev-стендовые объекты не трогать).
9. Все команды/исходы — в journal.md (раздел «Прогоны»); при красном — стоп,
   улики до зачистки, разбор; повторов сверх этого шага НЕТ.

**Выход:** вся переприёмка проведена; AC10 зелёный в полной серии solutionx
на свежем Release со свежим E2ePrepare-штампом; среда чистая; journal.md
дополнен.

**Проверка:** логи пп.1–7 существуют и содержат финальные строки; п.3 —
`Passed!`; пп.4–6 — зелёные (маркеры п.6 = 0); п.7 — AC10 зелёный (grep по
имени `BothTicketsPasswordAndCa_PlayThroughWithoutExpiry` — Passed); после
зачистки сети чисты.

**Связь со spec:** §6 A5-D2 (порядок ступеней дословно), §9 п.1–8, Р7, Р15,
Р16.

---

### Шаг 9 (фаза A6): закрытие задачи

- [x] Шаг 9 выполнен — **СТАТУС: ИСПОЛНЕН** (коммит `49a04eca`: journal.md
  финализирован; t01 удалён из bugs.md как исправленный; внеобластные красные —
  пунктами t04–t08; упоминания закрытого тега t01 вычищены; worktree чист)

**Вход (предусловие):** переприёмка шага 8 зелёная по всем пунктам (AC10
зелёный в полной серии).

**Действие (кодер):**
1. Финализировать journal.md: диагноз D2 с уликами (§3.1), пост-диагнозные
   решения Р10–Р16, прогоны (команды/логи/исходы, включая переприёмку),
   закрытие «как исправленный» (НЕ «невоспроизведённый» — баг воспроизведён
   и починен; Р8).
2. Применить подпункт 9.1 (удаление пункта из `arch/roadmap/bugs.md`).
   Проверить наличие локальной подпапки
   `arch/roadmap/bugs/t01-kafka-rotation-timeout/` — при наличии удалить её
   целиком (правило закрытия багов: пункт удаляется вместе с подпапкой
   логов; по факту подпапка утрачена ранее — тогда удалять нечего, это
   зафиксировать в journal.md).
3. Внеобластные красные прогонов — отдельные пункты `arch/roadmap/bugs.md`
   (правило AGENTS.md «фейл вне области — отдельная задача»), НЕ чинить в
   этой задаче; перечень известных: MetricsTests (KafkaWorker), TopicLifecycle,
   Valkey Metrics, ReassignmentTests-порт, PgWorker E2E артефакты — для
   КАЖДОГО, ещё не заведённого пунктом: имя теста/фильтр, точная команда
   запуска, коммит/ветка, текст ошибки, пути к логам/артефактам, ожидаемое
   vs фактическое. Уже заведённые — не дублировать.
4. Проверить область диффа: `git status --short` + `git diff --stat <база>..HEAD`
   — изменения ТОЛЬКО в допустимых путях: тестовая сборка
   `src/tests/KafkaWorker.IntegrationTests`,
   `src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs`,
   `src/tests/KafkaWorker.UnitTests/Provisioning/PasswordRotatorTests.cs`,
   `arch/16-kafkaworker.md`, `arch/roadmap/bugs.md`,
   `docs/superpowers/2026-10-23-t01-kafka-rotation-timeout/`. Всё лишнее —
   вернуть/починить до коммита.
5. Коммит: `chore(roadmap): t01-kafka-rotation-timeout закрыт как исправленный — пункт удалён из bugs.md`
   (включает journal.md и правку bugs.md).
6. Мерж в `main`/пуш — НЕ выполнять (только по явному требованию пользователя).

**Выход:** пункт `t01-kafka-rotation-timeout` отсутствует в
`arch/roadmap/bugs.md` (закрыт как исправленный); внеобластные красные —
пунктами роадмапа; дифф строго в области; журнал полный; ветка закоммичена.

**Проверка:** `grep -n "t01-kafka-rotation-timeout" arch/roadmap/bugs.md` —
пусто; `git log --oneline -5` показывает коммиты шагов 1/5/7 (два: arch +
код) /9; `git status --short` — чист.

**Связь со spec:** §6 A6, §9 п.8/11, Р8 (закрытие как исправленный), Р10;
правило закрытия багов (bugs.md) и «фейл вне области — отдельная задача»
(AGENTS.md).

**Подпункты для слабой модели:**

**9.1. Удали пункт t01 из файла**
`/Users/demakaev/ZCodeProject/worktrees/fix-t01-kafka-rotation-timeout/arch/roadmap/bugs.md`.
Найди блок списка задач, начинающийся строкой:

```markdown
- **`t01-kafka-rotation-timeout`** — джоб rotate KafkaWorker не выходит из фазы
```

и заканчивающийся строкой (последняя строка пункта t01):

```markdown
  (`waiting-cluster`) и тайкетов rotate.
```

Удали все строки от начала пункта t01 до этой строки ВКЛЮЧИТЕЛЬНО. Строку,
начинающую следующий пункт:

```markdown
- **`t02-pool-failover-selfheal`** — E2E-самолечение пула после исчерпания не
```

— НЕ удаляй; заголовки и правила ведения файла, пункты t02/t03 — не трогай.
После удаления проверь: между заголовком `## Задачи` и пунктом t02 осталась
ровно одна пустая строка-разделитель (без дублей пустых строк).

---

## Самопроверка плана (исполнена автором плана)

- **Покрытие spec (редакция после Фазы 6):** §5.1/§5.2 — влито (шаги 1/5,
  исполнены); §5.3 «не меняется» — глобальные ограничения + проверка шага 7
  (прод-дифф ровно две точки; юниты не трогают существующие контроли);
  §5.4 — исходы зафиксированы в статусах шагов 3/4/6 (D1/D3 не подтверждены,
  D4 опровергнут → D2); §5.5 — шаг 7 (правило arch-first п.7.1 → фикс пп.7.2–7.3
  → юниты пп.7.4–7.6 + кейс 4 покрыт существующим `Run_OpenCaWindow_WaitsCaWindow`);
  §6 A4-D2 → шаг 7; §6 A5-D2 → шаг 8 (8 ступеней дословно, включая E2ePrepare
  заново и порядок сборка/E2E двумя командами); §6 A6 → шаг 9 (закрытие как
  исправленный + внеобластные пункты); §7 — глобальные ограничения (Р15/Р16
  добавлены); §8 Р1–Р9 — в шагах 1–6; Р10 (D2 в этом же проходе) — сама
  структура шагов 7–8; Р11–Р13 (правило + форма `Success(false)` + отказ от
  last_error) — код подпунктов 7.2–7.3 и текст шага 7; Р14 (форма юнитов) —
  подпункты 7.4–7.6; Р15 — шаг 8; Р16 — глобальные ограничения + шаг 8 п.1;
  §9 п.1–12 — шаги 7/8/9 и проверки.
- **Имена/сигнатуры** — из реального кода: `PasswordRotator.RunRoleAsync`
  (блоки ca-window и преф-чека — дословно из текущего файла),
  `WaitOrExpireAsync`, `Result<bool>.Success(false)` (образец — ветка
  слепого снапшота), риг `NewRig`/`SeedRotation`/`ReadyCluster`/
  `rig.Admin.ClusterError`/`rig.Etcd.{Seed,Store.Remove}`/
  `rig.Journal.WritePhaseAsync`/`TicketOutcomes.Key` (юнит-файл),
  `TicketOutcomes.WriteDoneAsync` null-толерантен к аудиту (проверено),
  `Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry` — существующий
  тест, ассертящий дефектное поведение, заменяется по новому контракту.
- **Подпункты самодостаточны**: каждый содержит файл, точное место (якорь
  по имени метода/комментарию), полный код вставки/замены, ожидаемый
  результат сборки; внешних ссылок внутри кода нет.
- **Развилок нет**: порядок фаз фиксирован (A4-D2 до A5-D2 до A6); arch-коммит
  строго до кодового; форма фикса предопределена (Р11–Р13: `Success(false)`,
  без last_error-переписи, обе точки); юнит-постановка предопределена (Р14:
  сидирование `rotated-commit` + пароль NEW; замена одного существующего
  теста — его ассерт кодифицирует дефект); кейс 4 покрыт существующим тестом
  (дубль не добавляется); порядок переприёмки и повторы — Р15/Р16; закрытие —
  как исправленный (Р8).
