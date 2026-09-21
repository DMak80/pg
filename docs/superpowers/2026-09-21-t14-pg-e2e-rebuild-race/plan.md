# t14-pg-e2e-rebuild-race — план правки ac5-rebuild (ModRevision-гейт)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Устранить детерминированный FAIL pg-E2E `Acceptance_Scenario_Ac2_To_Ac7` в фазе `ac5-rebuild`: фаза обязана ждать факт перехода ноды `REBUILDING→RUNNING` (AC5), а не мгновенно-истинное условие на до-стоповом (stale) значении etcd-ключа ноды.

**Architecture:** В `AssertFailoverRebuildAsync` перед `docker stop` фиксируется ревизия ключа state ноды (`revAtStop`); условие фазы `ac5-rebuild` получает гейт `state == "RUNNING" && kv.ModRevision > revAtStop` — stale-`RUNNING` (ревизия ≤ `revAtStop`) отсекается безусловно, новая запись `RUNNING` с ревизией выше возможна только после реального цикла восстановления (порядок записей супервизора строго `UNREACHABLE`→`REBUILDING`→`RUNNING`). Диагностика при `!rebuilt` — `Assert.Fail` с дампом (state+ModRevision, `revAtStop`, статус контейнера) по образцу `ac6-quarantine`. Код воркера/движка не меняется.

**Tech Stack:** .NET 10 / C# (LangVersion=latest, Nullable=enable, TreatWarningsAsErrors=true), xUnit, FluentAssertions, etcd (`Shared.Etcd`), docker (контур E2eFixture).

**Spec:** `docs/superpowers/2026-09-21-t14-pg-e2e-rebuild-race/spec.md` (подход §5 — ModRevision-гейт — подтверждён пользователем 2026-09-21; формулировку spec §5.3 «остался без ответа» считать устаревшей).

Все пути в плане — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/fix-t14-pg-e2e-rebuild-race`. Все команды выполнять из корня worktree.

## Global Constraints

- Правка ТОЛЬКО в `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs`, метод `AssertFailoverRebuildAsync`. Не трогать: код `src/PgWorker.*` и `src/Shared.*`, `E2eFixture`, `E2eEnvironment`, фазы `ac5-failover`/`ac5-replica-catchup`, AC6/AC7, бюджеты фаз (300 с / 10 с / 120 с остаются как есть).
- Запрещено «лечение таймаутом» — никакие бюджеты не увеличиваются (AGENTS.base.md, spec §4.4).
- `dotnet build src/PgWorker.slnx -c Release` — 0 warnings (`TreatWarningsAsErrors=true`).
- Комментарии в коде — на русском, в стиле окружающего файла; идентификаторы — на английском.
- Новых хелперов не вводится: достаточно существующих `GetOrNullAsync` (возвращает `Kv?`, где `Kv(string Key, string Value, ulong ModRevision)` — `src/Shared.Etcd/Client/Kv.cs`), `WaitPhaseAsync`, `ContainerStatusAsync` (spec §6).
- E2E-прогон (Task 2) — на свежем Release, `PGW_TEST_E2E_NOBUILD` НЕ использовать (E2eFixture собирает Release сам). После финальной строки серии — зачистка контейнеров/сетей (`docker network prune -f` как страховочный гейт против осиротевших `pgw-*-net`).
- Упавший E2E-прогон НЕ перезапускать: сначала полный разбор логов/артефактов по `docs/e2e-launch.md` §4; повторный запуск — только после сформулированных причин (и по правилам AGENTS.md — с согласия пользователя).
- Портов тест не хардкодит (правка новых портов не вводит — сверить, что литералов вида `:NNNNN` в правке нет).
- Коммит в feature-ветку `fix-t14-pg-e2e-rebuild-race` — свободно. Мерж в `main`, пуш и снятие тега `t14-pg-e2e-rebuild-race` из `arch/roadmap/pgworker.md` — мерж-коммитом в `main`, ВНЕ этого плана (spec §8: гигиена roadmap — обязанность мерж-коммита, не кодовой фазы).
- Ожидания агента при наблюдении за прогоном — порциями ≤30 с (AGENTS.base.md); сам тест длинный — ждать его финальной строки в фоне; тест идёт дольше 5 минут — НЕМЕДЛЕННО онлайн-анализ docker-логов контейнеров, тест не останавливать.

---

### Task 1: ModRevision-гейт фазы ac5-rebuild в `AssertFailoverRebuildAsync`

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs` — метод `AssertFailoverRebuildAsync`, строки ~278–321 (три правки: вставка до `docker stop`; замена условия `ac5-rebuild`; замена ассерта `rebuilt`).

**Interfaces:**
- Consumes: `private async Task<Kv?> GetOrNullAsync(string key)` (E2eScenarios.cs:156) — возвращает `Kv?`; `public sealed record Kv(string Key, string Value, ulong ModRevision)` (`src/Shared.Etcd/Client/Kv.cs:4`) — поле `ModRevision` уже наполняется шлюзом (`src/Shared.Etcd/Client/EtcdGateway.cs:33,44`), тесту ничего нового не требуется; `WaitPhaseAsync(string phase, Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct)` (E2eScenarios.cs:102); `ContainerStatusAsync(string name, CancellationToken ct)` (E2eScenarios.cs:486); `Assert.Fail` (xUnit; образец использования — E2eScenarios.cs:408).
- Produces: наружу ничего нового (новых типов/хелперов/сигнатур нет); изменяется поведение приватного метода — фаза `ac5-rebuild` не может пройти на до-стоповом значении ключа.

Контекст для исполнителя (TDD-оговорка spec §7.1): юнит-покрытие условия НЕ выделяется — рейс воспроизводится только на живом docker-контуре, условие встроено в E2E-сценарий, изолировать его в юнит бессмысленно (проверяется интеграция etcd-таймингов с docker). Красного теста не будет: валидация — компиляция (Step 4), юнит-серия (Step 5) и живой E2E (Task 2).

- [ ] **Step 1: Фиксация `revAtStop` до `docker stop` + ассерт предусловия**

  **Вход:** `AssertFailoverRebuildAsync` в исходном виде; `masterBefore`/`container` определены, ассерт статуса лидера `running` пройден; хелпер `GetOrNullAsync` доступен.

  **Действие:** между ассертом статуса контейнера и запуском `Stopwatch` — вставка. Было (E2eScenarios.cs:280–285):

```csharp
        var masterBefore = await WaitForMasterAsync(cluster, shard, ct);
        var container = $"pgw-{cluster}-{shard}-{masterBefore.Node}";
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "лидер до failover работает");

        var sw = Stopwatch.StartNew();
        await Fx.RunDockerAsync(["stop", container], ct);
```

  Стало:

```csharp
        var masterBefore = await WaitForMasterAsync(cluster, shard, ct);
        var container = $"pgw-{cluster}-{shard}-{masterBefore.Node}";
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "лидер до failover работает");

        // t14 (гейт против stale-RUNNING): фиксируем ревизию ключа state ноды
        // ДО stop. После docker stop супервизор пишет ключ строго в порядке
        // UNREACHABLE → REBUILDING → RUNNING (NodeSupervisor.SuperviseShardAsync,
        // arch/14 §5 C): каждая запись — PutAsync и растит ModRevision, а
        // RUNNING живого пути пишется только нодам, живым пробой. До-стоповое
        // RUNNING (с provisioning-времени) имеет ревизию <= revAtStop — гейт
        // фазы ac5-rebuild ниже отсекает его безусловно, не полагаясь на
        // тайминги опроса. Гипотетический флап между фиксацией и stop лишь
        // увеличит ревизию — гейт остаётся корректным (строже).
        var stateBefore = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{masterBefore.Node}/state");
        (stateBefore?.Value).Should().Be("RUNNING", "лидер до failover: ключ state существует и равен RUNNING (предусловие гейта t14)");
        var revAtStop = stateBefore!.ModRevision;

        var sw = Stopwatch.StartNew();
        await Fx.RunDockerAsync(["stop", container], ct);
```

  Форма ассерта — именно со скобками `(stateBefore?.Value).Should()…`: вызов `.Should().Be(...)` БЕЗУСЛОВЕН, вне null-условной цепочки. Ловушка `stateBefore?.Value.Should()…` (как в образце E2eScenarios.cs:385) здесь недопустима: `?.` при `stateBefore == null` пропускает весь хвост цепочки вместе с ассертом, и тогда `stateBefore!.ModRevision` на следующей строке бросает NRE вместо сообщения ассерта (в образце :385 ловушка не стреляет лишь потому, что за ним нет `!`-разыменования). Со скобками: при null-ключе FluentAssertions честно репортит found `<null>`, при не-RUNNING — фактическое значение; одно сообщение покрывает оба требования предусловия «ключ есть и значение RUNNING» (spec §5.1 п.1). `stateBefore!.ModRevision` после ПРОЙДЕННОГО ассерта безопасен: `Be("RUNNING")` истинно ⇒ `stateBefore` не null.

  **Выход:** в методе зафиксирована `revAtStop` (ulong) с проверенным предусловием; переменная доступна для гейта Step 2 и дампа Step 3.

  **Проверка:** фрагмент метода между ассертом статуса и `var sw = ...` дословно соответствует блоку «Стало»; в строке ассерта есть внешние скобки `(stateBefore?.Value)` и `!`-разыменование стоит ПОСЛЕ ассерта.

  **Связь со spec:** §5.1 п.1 (фиксация ревизии + ассерт предусловия), §4.1/§4.2 (детерминизм без вероятностных окон — гейт ложен на до-стоповом значении по построению), §6 таблица правка №1.

- [ ] **Step 2: Условие фазы ac5-rebuild — гейт по ModRevision**

  **Вход:** Step 1 выполнен — `revAtStop` определена и валидна.

  **Действие:** замена тела условия `WaitPhaseAsync("ac5-rebuild", …)`. Было (E2eScenarios.cs:314–319):

```csharp
        // Rebuild: остановленный контейнер пересоздаётся, нода RUNNING.
        var rebuilt = await WaitPhaseAsync("ac5-rebuild", async () =>
        {
            var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{masterBefore.Node}/state");
            return state?.Value == "RUNNING";
        }, TimeSpan.FromSeconds(300), ct);
```

  Стало:

```csharp
        // Rebuild: остановленный контейнер пересоздаётся, нода проходит
        // REBUILDING → RUNNING (AC5, spec задачи 26 §11.5). Гейт по ревизии:
        // RUNNING && ModRevision > revAtStop — условие ложно на до-стоповом
        // значении ключа (в установившемся режиме гварды NodeSupervisor не
        // перезаписывают RUNNING), а новая запись RUNNING с ревизией выше
        // возможна только после реального цикла восстановления: порядок
        // записей строго UNREACHABLE → REBUILDING → RUNNING, RUNNING пишется
        // только живым пробой нодам (NodeSupervisor, arch/14 §5 C). Без гейта
        // фаза мгновенно истинна на stale-ключе (рейс t07/t14) и одиночный
        // inspect ниже падает на исходном остановленном контейнере.
        var rebuilt = await WaitPhaseAsync("ac5-rebuild", async () =>
        {
            var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{masterBefore.Node}/state");
            return state?.Value == "RUNNING" && state.ModRevision > revAtStop;
        }, TimeSpan.FromSeconds(300), ct);
```

  Здесь `state?.Value == "RUNNING" && …` безопасно без скобок: при `state == null` первый операнд `false`, короткое замыкание `&&` не допускает вычисления `state.ModRevision`.

  **Выход:** фаза `ac5-rebuild` конструктивно не может пройти на до-стоповом (stale) значении ключа; бюджет 300 с не изменён.

  **Проверка:** в новом теле лямбды условие — конъюнкция `state?.Value == "RUNNING"` и `state.ModRevision > revAtStop`; таймаут остался `TimeSpan.FromSeconds(300)`.

  **Связь со spec:** §5.1 п.2 (условие фазы), §5.2 (обоснование корректности: монотонность ModRevision, гварды, порядок записей), §4.4 (бюджеты не меняются), §6 правка №2.

- [ ] **Step 3: Диагностика `Assert.Fail` при `!rebuilt` + комментарий к одиночному inspect**

  **Вход:** Step 2 выполнен — `rebuilt` вычисляется гейтом; `revAtStop` в области видимости.

  **Действие:** замена ассерта после фазы. Было (E2eScenarios.cs:320–321):

```csharp
        rebuilt.Should().BeTrue("остановленная нода должна быть пересоздана и вернуться в RUNNING");
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "контейнер ноды пересоздан");
```

  Стало (образец дампа — `ac6-quarantine`, E2eScenarios.cs:405–412):

```csharp
        if (!rebuilt)
        {
            var current = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{masterBefore.Node}/state");
            Assert.Fail($"восстановление ноды не зафиксировано за 300с; state: {current?.Value ?? "нет"} " +
                        $"(ModRevision {current?.ModRevision.ToString() ?? "-"}, revAtStop {revAtStop}); " +
                        $"статус контейнера: {await ContainerStatusAsync(container, ct)}");
        }
        // Одиночный inspect корректен: RUNNING с ревизией > revAtStop пишется
        // только когда пересозданная нода жива пробой → контейнер уже running;
        // повторная смерть сразу после — реальный сбой, тест обязан упасть.
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "контейнер ноды пересоздан");
```

  Интерполяция `{current?.ModRevision.ToString() ?? "-"}` — null-условная цепочка на `current` (типа `Kv?`): при `current == null` пропускается вся цепочка `.ModRevision.ToString()`, выражение даёт `null` → `?? "-"`. `await` внутри интерполяции — в `async`-методе, как в образце ac6.

  **Выход:** при `!rebuilt` тест падает с полным дампом (state + ModRevision текущей записи, `revAtStop`, статус контейнера) вместо голого `Should().BeTrue`; одиночный `ContainerStatusAsync` после `rebuilt` сохранён.

  **Проверка:** ветка `if (!rebuilt)` присутствует и содержит все три факта дампа; ретраи вокруг финального inspect не введены.

  **Связь со spec:** §5.1 п.3 (Assert.Fail с дампом), §5.1 заключительный абзац (одиночный inspect, альтернатива 2 отклонена — семантика слабее AC5), §4.3 (телеметрия: падение с контекстом), §6 правки №3–4.

- [ ] **Step 4: Компиляционная валидация — 0 warnings**

  **Вход:** Steps 1–3 внесены в файл.

  **Действие:** сборка всего solution.

  **Выход:** подтверждена компилируемость правки (в т.ч. `(stateBefore?.Value).Should()` и интерполяция дампа) под `Nullable=enable`/`TreatWarningsAsErrors=true`.

  **Проверка:** Run: `dotnet build src/PgWorker.slnx -c Release` — Expected: `Build succeeded`, `0 Warning(s)`, `0 Error(s)`.

  **Связь со spec:** §7.2, §9.5 (0 warnings).

- [ ] **Step 5: Быстрая серия без docker (страховка сборки)**

  **Вход:** Step 4 зелёный.

  **Действие:** прогон юнит-серии PgWorker.

  **Выход:** подтверждено, что тестовый контур решения в целом жив.

  **Проверка:** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release` — Expected: PASS (правка юниты не покрывает; серия — «быстрые серии без docker при их наличии», spec §7.2).

  **Связь со spec:** §7.2.

- [ ] **Step 6: Commit**

  **Вход:** Steps 1–5 зелёные.

  **Действие:**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs
git commit -m "test(t14): ac5-rebuild — ModRevision-гейт против stale-RUNNING: фиксация revAtStop ключа ноды до docker stop (+ассерт предусловия RUNNING), условие фазы RUNNING && ModRevision > revAtStop (порядок записей супервизора UNREACHABLE→REBUILDING→RUNNING), Assert.Fail-диагностика при !rebuilt (state+ModRevision, revAtStop, статус контейнера — по образцу ac6-quarantine); бюджеты фаз и остальной сценарий без изменений"
```

  **Выход:** правка Task 1 зафиксирована в feature-ветке `fix-t14-pg-e2e-rebuild-race`.

  **Проверка:** `git log --oneline -1` показывает новый коммит; `git status --short` чист по `src/`.

  **Связь со spec:** §7.1 (правка теста завершена).

---

### Task 2: Мерж-гейт — полный E2E-прогон на свежем Release + зачистка

**Files:**
- Изменений кода нет; на живом docker-контуре проверяется результат Task 1 (требование roadmap-записи t14, spec §7.3).

**Interfaces:**
- Consumes: результат Task 1 (изменённый `E2eScenarios.cs`, закоммичен).
- Produces: зелёный прогон `Acceptance_Scenario_Ac2_To_Ac7` — входной факт для ревью/мержа; зачищенный хост.

- [ ] **Step 1: Прогон E2E на свежем Release**

  **Вход:** Task 1 закоммичен; на хосте нет остатков предыдущих серий (контейнеры `pgw-*`/сети зачищены); docker доступен (`DockerTrait` не скипнет тест).

  **Действие:** прогон (E2eFixture соберёт Release сам — инкрементальный no-op после Task 1; `PGW_TEST_E2E_NOBUILD` НЕ использовать):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7
```

  Запуск — в фоне, до финальной строки прогона (таймаут инструмента 600000 мс; если vstest не уложился — прочитать вывод, где остановился, НЕ перезапускать). Тест идёт дольше 5 минут — НЕМЕДЛЕННО начать онлайн-анализ docker-логов (`docker ps`, `docker logs` контуров `pgw-*` этого прогона), тест НЕ останавливать (AGENTS.base.md, диагностика зависаний).

  **Выход:** финальная строка прогона с вердиктом по единственному тесту.

  **Проверка:** Expected: `Passed!` — `Acceptance_Scenario_Ac2_To_Ac7`.

  **Связь со spec:** §7.3, §9.4.

- [ ] **Step 2: Проверка журнала прогона по телеметрии (docs/e2e-launch.md §1/§2)**

  **Вход:** Step 1 завершился (любым исходом; при FAIL — сначала Step 4, затем этот разбор).

  **Действие:** чтение строк `[PHASE]`/`AC5` в выводе теста.

  **Выход:** подтверждено фактическое (не мгновенно-истинное) прохождение фазы и неизменность соседних фаз.

  **Проверка:** в журнале обязаны быть (критерии приёмки spec §9.2/§9.4):
  - `[PHASE] ac5-rebuild: ok=True, elapsed=<N> ms`, где N ~ 15000–120000 (фактическое время восстановления: `UNREACHABLE` ≤2 с + `NodeDeadSec=6` + пересоздание/basebackup) — КРИТИЧНО: не ~300–500 мс (мгновенно-истинное значение = гейт не работает);
  - `AC5 .../shard1: leader failover took <M> ms` с M ≤ 5000 (фаза ac5-failover не менялась);
  - `[PHASE] ac5-replica-catchup: ok=True` (поведение после правки не изменилось);
  - `ac6-*`/`ac7-*` фазы — зелёные (окружение/teardown не задеты).

  Если elapsed ac5-rebuild ~ сотни мс — стоп, гейт не сработал: разбираться по коду/ключам, прогон НЕ засчитывать.

  **Связь со spec:** §9.2, §9.4.

- [ ] **Step 3: Зачистка хоста после серии (правила AGENTS.md)**

  **Вход:** финальная строка прогона получена (не действовать поверх живой серии).

  **Действие:**

```bash
docker network prune -f
docker ps -a --format '{{.Names}}' | grep 'pgw-' ; echo "exit=$?"
docker network ls --format '{{.Name}}' | grep -c 'pgw' ; echo "exit=$?"
```

  **Выход:** хост свободен от артефактов серии; следующая серия не столкнётся с осиротевшими сетями/портами.

  **Проверка:** `docker network prune -f` отчитался удалением (или «0 networks»); grep по контейнерам `pgw-` — пусто (exit=1); счётчик сетей `pgw` = 0. Ассерт чистоты самого teardown'а уже внутри фикстуры; перечисленное — страховочный гейт против осиротевших `pgw-*-net` (создаются движком, ryuk их не подбирает).

  **Связь со spec:** §7.3 (зачистка после серии), §9.6.

- [ ] **Step 4 (условный, только при FAIL): разбор без перезапуска**

  **Вход:** Step 1 завершился FAIL.

  **Действие:** НЕ перезапускать (запрещено без согласия пользователя — AGENTS.md/телеметрия). Упавший сценарий уже пометил себя `MarkFailed()` → teardown ОСТАНОВИЛ контейнеры, но не удалил их: артефакты в `/tmp/pgw-e2e-artifacts-<guid>/` (docker-логи/inspect, host.log воркеров). Порядок: (1) прочитать сообщение падения — при `!rebuilt` это собственный `Assert.Fail` из Task 1 Step 3 с полным дампом (state, ModRevision, revAtStop, статус контейнера); (2) сопоставить дамп с порядком записей `NodeSupervisor`; (3) сформулировать все причины по `docs/e2e-launch.md` §4; (4) зачистить хост вручную по `README-cleanup.txt` из артефактов ПОСЛЕ разбора; (5) вернуть результат координатору (при неоднозначности — `NEEDS_CONTEXT` с вопросом).

  **Выход:** сформулированные причины падения; повторный прогон — только после полного анализа и согласия пользователя.

  **Проверка:** отчёт содержит вердикт «что произошло» по логам/артефактам без единого перезапуска теста.

  **Связь со spec:** §7.3, §9.4, §9.6; docs/e2e-launch.md §4.

---

## Покрытие spec (сверка self-review)

| Требование spec | Закрывает |
|---|---|
| §5.1 п.1 фиксация `revAtStop` + предусловие | Task 1 Step 1 |
| §5.1 п.2 условие `RUNNING && ModRevision > revAtStop` | Task 1 Step 2 |
| §5.1 п.3 `Assert.Fail`-диагностика с дампом | Task 1 Step 3 |
| §5.1 п.4 комментарии-обоснования (рус.) | Task 1 Steps 1–3 (комментарии в коде) |
| §5.1 одиночный inspect без ретраев | Task 1 Step 3 (комментарий-обоснование) |
| §6 таблица правок 1–4 | Task 1 Steps 1–3 |
| §7.1 правка теста (TDD ограничен) | Task 1 (оговорка перед шагами) |
| §7.2 build 0 warnings + быстрые серии | Task 1 Steps 4–5 |
| §7.3 мерж-гейт E2E + зачистка | Task 2 Steps 1–3 |
| §9.1 конструктивная невозможность stale-прохода | Task 1 Step 2 (гейт) + комментарии |
| §9.2 elapsed ~15–120 с, контейнер running, replica-catchup | Task 2 Step 2 |
| §9.3 падение с контекстом | Task 1 Step 3 |
| §9.4 зелёный полный прогон | Task 2 Steps 1–2 |
| §9.5 0 warnings; AAA-оговорка (правится тело сценария) | Task 1 Step 4; AAA не вводится (spec §9.5) |
| §9.6 зачистка/повторные прогоны | Task 2 Steps 3–4 |
| §8 запреты (src-код, бюджеты, тег roadmap — мержем) | Global Constraints |

Миниатюра итогового метода после Task 1 (сверка исполнителем): `masterBefore`/`container`/ассерт статуса → фиксация `stateBefore`+`revAtStop` (ассерт `(stateBefore?.Value).Should().Be("RUNNING", …)`) → `sw`+`docker stop` → фаза `ac5-failover` (без изменений) → фаза `ac5-rebuild` с гейтом → `if (!rebuilt) Assert.Fail(дамп)` → одиночный inspect `running` → `ac5-replica-catchup` (без изменений).
