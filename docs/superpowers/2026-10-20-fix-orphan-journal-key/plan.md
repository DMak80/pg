# План: фазы BackupOrphanSweeper — выделенный ключ /pgworker/backups/orphan-journal

> **Исполнение (dev-flow, Фаза 6):** план исполняет СИЛЬНАЯ модель (main-агент).
> Механическая часть — подпункты «coder-flash» в конце шагов: субагент-слабая
> модель получает ТОЛЬКО текст подпункта + путь к файлу (ни spec, ни плана, ни
> контекста проекта). После каждого подпункта сильная модель РЕВЬЮИТ результат
> (diff) и правит сама при расхождении. Шаги без блока подпунктов исполняются
> сильной моделью целиком. Отслеживание шагов — чекбоксами `- [ ]`.

**Цель:** orphan-фазы свипера перестают писаться в per-cluster work-ключ
`/pgworker/work/<C>` (RMW-гонка с держателем клэйма, фантомные ключи удалённых
кластеров, вечные фазовые метрики) и пишутся plain put'ом в новый глобальный
ключ `/pgworker/backups/orphan-journal` — единственный писатель глобальный
лидер, «последняя фаза побеждает», без чистки.

**Архитектура:** без нового класса-store — константа ключа + сериализатор в
`OrphanRegistry` (образец `HoldToJson`/`DeleteToJson`), приватный
failover-put `WriteJournalAsync` в `BackupOrphanSweeper` (образец
`PutRegistryAsync`); зависимость `WorkJournal` удаляется из свипера,
`Program.cs` и всех четырёх тестовых конструкторов. Метрики:
`backup-orphan-sweeper` пополняет `WorkerMetricsInstrumentation.SuppressedOps`.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
etcd HTTP JSON gateway, xUnit (AAA-комментарии в тестах обязательны).

**Spec:** `docs/superpowers/2026-10-20-fix-orphan-journal-key/spec.md` (в этом
же каталоге; план аргументируется от spec — исполнитель читает оба).

**Рабочий каталог всех команд:** корень worktree
`/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key`.

## Глобальные ограничения

- arch-first: контракт меняется СНАЧАЛА в `arch/19-backups.md` §4 и
  `arch/18-metrics.md` §2.2, ПОТОМ код (spec §2, §5 этап 0; arch-коммит —
  до кодового, критерий приёмки 5).
- Сборка и тесты — ДВУМЯ отдельными командами: `dotnet build` отдельно,
  `dotnet test` — ТОЛЬКО с `--no-build` и env-префиксом при самой команде
  (AGENTS.md).
- `TreatWarningsAsErrors=true` — ноль warnings в сборке Release.
- Все команды с длинным логом — с выводом в файл (`> /tmp/<имя>.log 2>&1`),
  чтение/анализ — из файла; запуск в фоне, наблюдение по канону base-rules
  №12/13: старт виден в логе ≤15 с, контроль каждые 2 мин, зависание >5 мин —
  онлайн-анализ логов без остановки.
- После КАЖДОЙ тестовой серии — зачистка docker-окружения (контейнеры/сети
  прогона; страховочный гейт — критерий: `docker ps -a` и `docker network ls`
  не содержат артефактов серии).
- Русский язык комментариев/документации; идентификаторы — английские.
- Тесты — с AAA-комментариями (Arrange/Act/Assert).
- Правила `AGENTS.base.md` и `AGENTS.md` worktree — обязательны для
  исполнителя и субагентов (ссылка на
  `/Users/demakaev/ZCodeProject/AGENTS.base.md` передаётся coder-flash).
- Изменяемые файлы — ТОЛЬКО перечисленные в шагах; иные потребители
  `WorkJournal` не трогаются (spec §6 п.1, критерий приёмки 7).

## Review Focus

Классы отказов, подразумеваемые spec, и чем закрыты (новые задачи вне §5
этапа 2 spec НЕ вводятся — объём тестов закрыт решением Р7):

1. Put orphan-journal при недоступных ВСЕХ endpoints — проход свипера НЕ
   должен падать (журнал — наблюдаемость; Д1). Закрыто конструкцией
   `WriteJournalAsync` (шаг 3, подпункт 3.3): возврат после failover-цикла +
   `LogWarning`, исключение не бросается; отдельного теста нет — решение Р7.
2. Не-лидер пишет orphan-journal — запрещено (Р1). Закрыт ассертом в
   `SweepWithoutLeadershipWritesNothing` (шаг 5, подпункт 5.3).
3. Фантомный work-ключ кластера сироты — не создаётся (цель фикса, §1 п.2).
   Закрыт ассертом отсутствия `/pgworker/work/ghost` в новом сценарии (шаг 5,
   подпункт 5.2).
4. Orphan-фазы дают вечные фазовые метрики — подавлены (§1 п.3, Д3). Закрыто
   третьей строкой Act в `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries`
   (шаг 6) + `SuppressedOps` в коде (шаг 4).
5. `updated_unix` не от системных часов, а от инжектированного `time`
   (тестируемость, Д2). Закрыт ассертом `updated_unix ==
   clock.Now.ToUnixTimeSeconds()` в новом сценарии (шаг 5, подпункт 5.2).
6. Изоляция интеграционных сценариев по новому ключу — чистка в `SeedAsync`
   (шаг 5, подпункт 5.1).

---

### Шаг 1. arch-first: правки arch/19 §4 и arch/18 §2.2 (этап 0)

Сильная модель (нужен контекст канона). Подпунктов нет.

**Вход:** worktree чист (изменений нет); spec прочитан; строки arch сверены с
фактическим файлом.

**Действие:**

1. `arch/19-backups.md`, таблица §4 (строки 591-601):
   - ВСТАВИТЬ новую строку ПОСЛЕ строки 599
     (`| /pgworker/backups/orphan-deletes/<C>/<X> | …`), ПЕРЕД строкой 600
     (`| /pgworker/backups/<C>/<X>/restore/<id> | …`) — дословный текст:

     ```markdown
     | `/pgworker/backups/orphan-journal` | журнал фаз лидер-прохода свипера сирот: `{"op":"backup-orphan-sweeper","phase":"orphan-…/<C>/<X>","instance":"<id>","updated_unix":T,"last_error"?}` (фазы: orphan-key-cleaned/orphan-cancelled/orphan-delete-requested/orphan-deleting/orphan-deleted — каждая с «/<C>/<X>»); ключ ГЛОБАЛЬНЫЙ (вне per-cluster префиксов, D2-чистки не касается); пишет ТОЛЬКО глобальный лидер /pgworker/leader (единственный писатель — гонок нет; plain put без read-modify-write); «последняя фаза побеждает» — ключ перезаписывается каждым проходом, чистка не нужна; панель ключ НЕ читает (состояние сирот наблюдаемо через /pgworker/backups/orphans + orphan-holds + orphan-deletes) |
     ```

   - Строка 597 (`/pgworker/backups/orphans`): заменить фрагмент
     «кандидат → DELETING (journal) → batch-delete» на
     «кандидат → DELETING (фаза orphan-journal) → batch-delete».
   - Строка 599 (`/pgworker/backups/orphan-deletes/<C>/<X>`): заменить фрагмент
     «исполняет sweeper ближайшим проходом: DELETING (journal) → batch-delete»
     на «исполняет sweeper ближайшим проходом: DELETING (фаза orphan-journal)
     → batch-delete».
   - ПРОЧИЕ «journal»-упоминания arch/19 (строки 327, 342, 552-562, 605, 644,
     867) — записи владельцев клэйма в `/pgworker/work/<C>`, корректные: НЕ
     трогать (spec §5 этап 0 п.1).
2. `arch/18-metrics.md` §2.2:
   - Строка 92 (таблица `worker_operation_total`): заменить фрагмент
     «подавленные ops — supervise/evacuate — не считаются, см. ниже» на
     «подавленные ops — supervise/evacuate/backup-orphan-sweeper — не
     считаются, см. ниже».
   - Абзац строк 108-112 («Стационарные ops без терминальной фазы — …»)
     заменить ЦЕЛИКОМ на дословный текст (добавка третьего op):

     ```markdown
     Стационарные ops без терминальной фазы — `supervise` (пишется и через
     `WriteSupervisionAsync`, мимо фазового события) и `evacuate` (только
     `waiting-*`/`blocked-moving`), а также `backup-orphan-sweeper`
     (журнал домена сирот — свой ключ `/pgworker/backups/orphan-journal`
     (arch/19 §4), в work-фазовые метрики не входит) — фазовых серий НЕ
     эмитят: их живость закрывает `worker_loop_last_success_timestamp_seconds`
     (алерт WorkerLoopStalled), вечные серии не копятся.
     ```

   - Список `process` у PgWorker (строки 96-99) НЕ пополняется (решение Р4).
3. `arch/adminpanel/02-etcd-contract.md` — БЕЗ правок (панель ключ не читает;
  решение Р4/spec §5 этап 0 п.3).
4. Коммит (arch-правки — отдельным коммитом ДО кода):

   ```bash
   git add arch/19-backups.md arch/18-metrics.md
   git commit -m "arch(19,18): orphan-фазы свипера — выделенный глобальный ключ /pgworker/backups/orphan-journal"
   ```

**Выход:** arch/19 §4 описывает ключ `/pgworker/backups/orphan-journal`
(строка таблицы + «(фаза orphan-journal)» в orphans/orphan-deletes); arch/18
§2.2 содержит `backup-orphan-sweeper` в подавленных; arch-коммит создан.

**Проверка:**
- `grep -n "orphan-journal" arch/19-backups.md` — 3 совпадения (новая строка
  таблицы + строки 597 и 599 с «(фаза orphan-journal)»).
- `grep -n "backup-orphan-sweeper" arch/18-metrics.md` — 2 совпадения
  (строка 92 и абзац 108-112).
- `grep -n "DELETING (journal)" arch/19-backups.md` — пусто.
- `git log --oneline -1` — arch-коммит; `git show --stat HEAD` — только два
  arch-файла.

**Связь со spec:** §5 этап 0 (пп.1-3), §7 Р4; критерий приёмки 5.

---

### Шаг 2. OrphanRegistry: JournalKey + JournalPhaseToJson + JournalPayload (этап 1.1)

**Вход:** шаг 1 завершён (arch-коммит есть); файл
`src/PgWorker.Backups/Supervisor/OrphanRegistry.cs` не изменён.

**Действие:** сильная модель выдаёт субагенту coder-flash подпункты 2.1-2.2
(по одному запуску), затем ревьюит diff (соответствие образцам
`HoldToJson`/`DeleteToJson` и стилю doc-комментариев) и правит сама при
расхождениях.

**Выход:** `OrphanRegistry` содержит публичную константу `JournalKey` и
публичный статический `JournalPhaseToJson`; решение собирается (проверка на
шаге 3).

**Проверка:** визуальный ревью diff; сборка проекта (быстрая):
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.Backups/PgWorker.Backups.csproj -c Release --nologo > /tmp/pgw-plan-p2-build.log 2>&1`
— в логе `Build succeeded`, 0 Warning(s) (полная сборка solution — шаг 3).

**Связь со spec:** §4.3 (реализация: константа + сериализатор + payload), §4.2
(формат значения), §7 Р2, Р3.

**Подпункты (coder-flash):**

- [ ] **Подпункт 2.1.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`.
  В классе `public static class OrphanRegistry` найди строку:

  ```csharp
      public const string DeletesPrefix = "/pgworker/backups/orphan-deletes/";
  ```

  (это третья константа подряд: `Key`, `HoldsPrefix`, `DeletesPrefix`) и
  ВСТАВЬ СРАЗУ ПОСЛЕ неё (перед doc-комментарием `/// <summary>Реестр
  осиротевших S3-префиксов установки…`) точно такой блок:

  ```csharp
      /// <summary>Глобальный ключ журнала фаз лидер-прохода свипера сирот
      /// (arch/19 §4): {"op":"backup-orphan-sweeper","phase":"orphan-…/&lt;C&gt;/&lt;X&gt;",
      /// "instance":"…","updated_unix":T,"last_error"?}; вне per-cluster префиксов
      /// (D2-чистки deprovisioning не касается); пишет ТОЛЬКО глобальный лидер
      /// /pgworker/leader plain put'ом без read-modify-write (единственный
      /// писатель — гонок нет); «последняя фаза побеждает» — ключ перезаписывается
      /// каждым проходом, чистка не нужна.</summary>
      public const string JournalKey = "/pgworker/backups/orphan-journal";
  ```

  Больше в файле ничего не менять.

- [ ] **Подпункт 2.2.** Файл: тот же
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`.
  Две вставки.

  Первая: найди приватный record (строки в районе 91-93):

  ```csharp
      private sealed record DeletePayload(
          [property: JsonPropertyName("requested_unix")] long RequestedUnix,
          [property: JsonPropertyName("requested_by")] string RequestedBy);
  ```

  и ВСТАВЬ СРАЗУ ПОСЛЕ него точно такой record:

  ```csharp
      private sealed record JournalPayload(
          [property: JsonPropertyName("op")] string Op,
          [property: JsonPropertyName("phase")] string Phase,
          [property: JsonPropertyName("instance")] string Instance,
          [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
          [property: JsonPropertyName("last_error")] string? LastError);
  ```

  Вторая: найди метод (в середине класса):

  ```csharp
      public static string DeleteToJson(long requestedUnix, string requestedBy)
          => JsonSerializer.Serialize(new DeletePayload(requestedUnix, requestedBy), Json);
  ```

  и ВСТАВЬ СРАЗУ ПОСЛЕ него точно такой метод:

  ```csharp
      /// <summary>Значение ключа журнала фаз свипера (arch/19 §4, строго):
      /// {"op":"backup-orphan-sweeper","phase":"…","instance":"…",
      /// "updated_unix":T,"last_error"?} — null-поля опускаются сериализацией
      /// (WhenWritingNull); парсер в прод-коде не нужен (читатель — оператор).</summary>
      public static string JournalPhaseToJson(string phase, string instance, long updatedUnix, string? lastError)
          => JsonSerializer.Serialize(
              new JournalPayload("backup-orphan-sweeper", phase, instance, updatedUnix, lastError), Json);
  ```

  Больше в файле ничего не менять. Оба вставленных члена — внутри класса
  `OrphanRegistry` (до его закрывающей фигурной скобки), с тем же отступом в
  4 пробела, что у соседних членов.

---

### Шаг 3. Миграция свипера на orphan-journal + удаление зависимости WorkJournal (этапы 1.2, 1.3, 1.5)

**Вход:** шаг 2 завершён (константа и сериализатор в `OrphanRegistry` есть).

**Действие:** сильная модель выдаёт субагенту coder-flash подпункты 3.1-3.6
(можно по 2-3 за запуск в пределах одного файла), после каждого файла ревьюит
diff: параметры конструктора, все 6 замен, тело `WriteJournalAsync`,
корректность удалённых строк. Затем — сборка solution целиком (ДО шага 3
решение собирается частично: удаление параметра ломает вызовы до правок
3.4-3.6 — это ожидаемое промежуточное состояние).

**Выход:** свипер пишет фазы в `/pgworker/backups/orphan-journal` через
`WriteJournalAsync`; параметр `WorkJournal journal` удалён из конструктора
`BackupOrphanSweeper`; аргумент удалён из фабрики `Program.cs` и всех 4
тестовых конструкторов; решение собирается в Release без warnings.

**Проверка:**
- `grep -n "WritePhaseAsync" src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` — пусто (критерий приёмки 1).
- `grep -n "WorkJournal" src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` — пусто.
- `grep -n "WriteJournalAsync" src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` — 7 совпадений (1 определение + 6 вызовов).
- Сборка solution (лог в файл):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-plan-p3-build.log 2>&1`
  — в логе `Build succeeded`, `0 Warning(s)` (TreatWarningsAsErrors).

**Связь со spec:** §4.3 (свипер: удаление зависимости, 6 точек замены,
`WriteJournalAsync` failover-put; Program.cs:774; тестовые конструкторы
§3.3 п.1), §7 Р3, Д1, Д2; критерий приёмки 1.

**Подпункты (coder-flash):**

- [ ] **Подпункт 3.1.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs`.
  В списке параметров primary-конструктора класса `BackupOrphanSweeper`
  (начинается `public sealed class BackupOrphanSweeper(`) найди строку:

  ```csharp
      WorkJournal journal,
  ```

  и УДАЛИ её целиком (одна строка, параметр больше не нужен). Остальные
  параметры и порядок НЕ менять. Больше ничего в этой правке не делать.

- [ ] **Подпункт 3.2.** Файл: тот же `BackupOrphanSweeper.cs`. В методе
  `SweepAsync` ЗАМЕНИ ровно 6 двухстрочных вызовов на однострочные — каждый
  вызов идёт `await journal.WritePhaseAsync(...)` с телом на 2 строках;
  старый фрагмент → новый фрагмент (отступы сохранить как в файле):

  1-й (внутри `else` санитара hold-ключей, ~строки 126-127; отступ 16
  пробелов):

  ```csharp
                await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
                    $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
                await WriteJournalAsync($"orphan-key-cleaned/{prefix}", ct);
  ```

  2-й (санитар заявок, ~строки 140-141; отступ 12 пробелов):

  ```csharp
            await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
                $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
            await WriteJournalAsync($"orphan-key-cleaned/{prefix}", ct);
  ```

  3-й (ветка воскресшего владельца, ~строки 197-198; отступ 12 пробелов):

  ```csharp
            await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
                $"orphan-cancelled/{candidate}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
            await WriteJournalAsync($"orphan-cancelled/{candidate}", ct);
  ```

  4-й (журнал заявки до DELETING, внутри `if (byRequest)`, ~строки 215-216;
  отступ 12 пробелов):

  ```csharp
            await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
                $"orphan-delete-requested/{candidate}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
            await WriteJournalAsync($"orphan-delete-requested/{candidate}", ct);
  ```

  5-й (после put реестра с DELETING, ~строки 226-227; отступ 8 пробелов):

  ```csharp
        await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
            $"orphan-deleting/{candidate}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
        await WriteJournalAsync($"orphan-deleting/{candidate}", ct);
  ```

  6-й (после del записи реестра, ~строки 262-263; отступ 8 пробелов):

  ```csharp
        await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
            $"orphan-deleted/{candidate}", claims.InstanceId, null, ct);
  ```

  →

  ```csharp
        await WriteJournalAsync($"orphan-deleted/{candidate}", ct);
  ```

  Строки фаз (`orphan-key-cleaned/…`, `orphan-cancelled/…`,
  `orphan-delete-requested/…`, `orphan-deleting/…`, `orphan-deleted/…`)
  менять НЕЛЬЗЯ — только обёртка вызова. Другой код метода не трогать.

- [ ] **Подпункт 3.3.** Файл: тот же `BackupOrphanSweeper.cs`. В КОНЦЕ класса
  `BackupOrphanSweeper` найди последний приватный метод `PutRegistryAsync`
  (заканчивается строками):

  ```csharp
          return Result.Failed(last.Error ?? new ApplicationException(
              $"put {OrphanRegistry.Key}: no live endpoints"));
      }
  ```

  и ВСТАВЬ СРАЗУ ПОСЛЕ закрывающей фигурной скобки этого метода (перед
  закрывающей скобкой класса) точно такой метод:

  ```csharp
      // Фаза свипера — глобальный ключ /pgworker/backups/orphan-journal
      // (arch/19 §4): failover-put по endpoints (образец PutRegistryAsync),
      // plain put без read-modify-write. Журнал — наблюдаемость: неудача
      // записи НЕ роняет проход (все endpoints упали — предупреждение).
      private async Task WriteJournalAsync(string phase, CancellationToken ct)
      {
          foreach (var endpoint in endpoints)
          {
              var put = await etcd.PutAsync(
                  endpoint, OrphanRegistry.JournalKey,
                  OrphanRegistry.JournalPhaseToJson(
                      phase, claims.InstanceId, time.GetUtcNow().ToUnixTimeSeconds(), null),
                  null, ct);
              if (put.IsSuccess)
                  return;
          }

          logger?.LogWarning("{Op}: orphan journal write failed for phase {Phase}", Op, phase);
      }
  ```

  Имена `etcd`, `endpoints`, `claims`, `time`, `logger` — параметры
  primary-конструктора класса, `Op` — приватная константа класса; всё уже
  существует, ничего дополнительно объявлять не нужно. Отступ члена класса —
  4 пробела.

- [ ] **Подпункт 3.4.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/PgWorker.App/Program.cs`.
  Найди фабричную регистрацию (комментарий над ней «// Глобальный проход сирот
  S3 (t07, arch/19 §4)…», код начинается
  `builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupOrphanSweeper(`).
  Внутри списка аргументов найди РОВНО ОДНУ строку:

  ```csharp
      sp.GetRequiredService<WorkJournal>(),
  ```

  — это аргумент фабрики BackupOrphanSweeper — и УДАЛИ её целиком. Другие
  регистрации `WorkJournal` в файле НЕ трогать (строка
  `builder.Services.AddSingleton(sp => new WorkJournal(` ~164 и аргумент
  фабрики BackupSupervisorProcess ~758 остаются). Комментарий над фабрикой не
  менять.

- [ ] **Подпункт 3.5.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`.
  Два удаления.

  Первое: в методе `BuildSweeper` (вернулся `=> new(gateway ?? fixture.Gateway,
  [fixture.Endpoint], FakePoolFactory.Single(s3), …)`) найди строку:

  ```csharp
            new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
  ```

  и УДАЛИ её целиком (аргумент `claims,` до неё и лямбда `() => options ??
  Options(), …` после остаются на своих местах).

  Второе: в методе теста `SweepKeyInTwoPoolsCountedTwice` найди строку:

  ```csharp
            new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
  ```

  (внутри `new BackupOrphanSweeper(fixture.Gateway, [fixture.Endpoint], …)`) и
  УДАЛИ её целиком. Больше в файле ничего не менять; using-блок не трогать.

- [ ] **Подпункт 3.6.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`.
  Два удаления, оба — строка вида:

  ```csharp
            new WorkJournal("/pgworker", etcd, options.CurrentValue.Etcd.Endpoints),
  ```

  Первое — в статическом методе `Create` (внутри `new BackupOrphanSweeper(
  etcd, options.CurrentValue.Etcd.Endpoints, …)`); второе — в методе теста
  `ExecuteAsync_LeaderHangingSweep_PulsesActivityBeyondTick` (внутри другого
  `new BackupOrphanSweeper(…)`). Каждую строку УДАЛИ целиком. Больше в файле
  ничего не менять; using-блок не трогать.

---

### Шаг 4. WorkerMetricsInstrumentation.SuppressedOps: + backup-orphan-sweeper (этап 1.4)

**Вход:** шаг 3 завершён (свипер больше не эмитит `PhaseWritten`; событие
`Program.cs:107` остаётся — подписка не меняется).

**Действие:** подпункт 4.1 (две правки в одном файле) → coder-flash; ревью
diff сильной моделью (оба комментария синхронны с множеством); коммит кода
шагов 2-4.

**Выход:** `SuppressedOps` содержит `"backup-orphan-sweeper"`; комментарий у
множества и перечень ops в комментарии над `OnJournalPhase` (строка 543)
зеркалят arch/18 §2.2.

**Проверка:** `grep -n "backup-orphan-sweeper" src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` — 3 совпадения (комментарий у множества,
строка HashSet, перечень в комментарии над `OnJournalPhase`); сборка
Shared.Metrics:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/Shared.Metrics/Shared.Metrics.csproj -c Release --nologo > /tmp/pgw-plan-p4-build.log 2>&1`
— `Build succeeded`, 0 warnings. Коммит:

```bash
git add src/PgWorker.Backups/Supervisor/OrphanRegistry.cs src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs src/PgWorker.App/Program.cs src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs
git commit -m "feat(backups,metrics): orphan-фазы свипера — глобальный ключ orphan-journal; зависимость WorkJournal удалена; suppression backup-orphan-sweeper"
```

**Связь со spec:** §4.3 (метрики), §3.3 (дополнение: SuppressedOps зеркалится в
коде), §7 Д3; критерий приёмки 4. Пополнение комментария над `OnJournalPhase`
— замечание ревью плана (Фаза 4): перечень в код-комментарии обязан
соответствовать множеству.

**Подпункты (coder-flash):**

- [ ] **Подпункт 4.1.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`.
  Две правки.

  Первая: найди блок (рядом с началом класса `WorkerMetricsInstrumentation`):

  ```csharp
      // Ops без терминальной фазы: supervise (стационарные записи, часть через
      // WriteSupervisionAsync мимо события) и evacuate (только waiting-*/blocked-moving).
      public static readonly IReadOnlySet<string> SuppressedOps =
          new HashSet<string> { "supervise", "evacuate" };
  ```

  и ЗАМЕНИ его целиком на:

  ```csharp
      // Ops без терминальной фазы: supervise (стационарные записи, часть через
      // WriteSupervisionAsync мимо события), evacuate (только waiting-*/blocked-moving)
      // и backup-orphan-sweeper (журнал домена сирот — свой ключ
      // /pgworker/backups/orphan-journal, arch/19 §4, в work-фазовые метрики
      // не входит).
      public static readonly IReadOnlySet<string> SuppressedOps =
          new HashSet<string> { "supervise", "evacuate", "backup-orphan-sweeper" };
  ```

  Вторая: найди строку комментария над методом `OnJournalPhase` (~строка 543,
  комментарий начинается строкой «// Журнальное событие фазы (подписка
  WorkJournal.PhaseWritten/WriteAsync):»):

  ```csharp
      //  - ops без терминальной фазы (SuppressedOps: supervise, evacuate) — ИГНОР:
  ```

  и ЗАМЕНИ её на две строки (перечень дополнен третьим op):

  ```csharp
      //  - ops без терминальной фазы (SuppressedOps: supervise, evacuate,
      //    backup-orphan-sweeper) — ИГНОР:
  ```

  Строки этого комментария до/после заменяемой (про
  `WriteSupervisionAsync`, `WorkerLoopStalled`, терминальные фазы и т.д.) НЕ
  трогать. Больше в файле ничего не менять.

---

### Шаг 5. Интеграционные тесты BackupOrphanSweeperTests (этап 2.1)

**Вход:** шаги 3-4 завершены (коммит кода есть); решение собирается.

**Действие:** подпункты 5.1-5.3 → coder-flash; ревью diff сильной моделью
(AAA-структура, точность ассертов, использование хелперов `KeyExistsAsync`);
существующие сценарии НЕ переписываются (spec §5 этап 2 п.3).

**Выход:** `SeedAsync` чистит `OrphanRegistry.JournalKey`; добавлен новый
сценарий последней фазы + отсутствия work-ключа;
`SweepWithoutLeadershipWritesNothing` проверяет отсутствие журнала у
не-лидера.

**Проверка:** `grep -n "JournalKey" src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs` — 3 совпадения (SeedAsync, новый сценарий, не-лидер); компиляция тестового проекта войдёт в сборку шага 7; сам прогон — шаг 8.

**Связь со spec:** §5 этап 2 п.1, §7 Р7; критерии приёмки 2, 3.

**Подпункты (coder-flash):**

- [ ] **Подпункт 5.1.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`.
  В методе `SeedAsync` найди строку:

  ```csharp
          await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.Key, prefix: false, ct);
  ```

  и ВСТАВЬ СРАЗУ ПОСЛЕ неё точно такую строку (чистка нового ключа от
  предыдущих тестов — изоляция сценариев):

  ```csharp
          await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.JournalKey, prefix: false, ct);
  ```

  Больше в методе ничего не менять.

- [ ] **Подпункт 5.2.** Файл: тот же `BackupOrphanSweeperTests.cs`. Найди
  закрывающую фигурную скобку теста `SweepTtlExpiredRemovesPrefixAndRecord`
  (тест заканчивается строками):

  ```csharp
          var registry = await ReadRegistryAsync();
          registry!.Orphans.Should().BeEmpty("the removal is confirmed — the record is retired");
      }
  ```

  и ВСТАВЬ СРАЗУ ПОСЛЕ этой скобки (перед комментарием `// AAA (AC7):
  OrphanTtlSec=0 — реестр живёт, удаления нет`) точно такой новый тест-метод:

  ```csharp
      // AAA: проход с TTL-удалением сироты пишет последнюю фазу в ГЛОБАЛЬНЫЙ
      // ключ /pgworker/backups/orphan-journal; work-ключ кластера сироты
      // НЕ создаётся (фантомных журналов несуществующего кластера нет)
      [Fact]
      public async Task SweepTtlExpiredWritesOrphanJournalPhase()
      {
          // Arrange — сирота в реестре с first_seen 2 минуты назад (ttl 60 c)
          var ct = TestContext.Current.CancellationToken;
          var s3 = new FakeBackupS3();
          var clock = await SeedAsync(s3);
          var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
          (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
          var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
          (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue(); // сирота в реестре

          // Act — время вперёд за TTL; проход с удалением
          clock.Now = clock.Now.AddMinutes(2);
          (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

          // Assert — ключ orphan-journal содержит ПОСЛЕДНЮЮ фазу прохода:
          // op/instance/updated_unix соответствуют payload свипера
          var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, OrphanRegistry.JournalKey, ct);
          journal.Value.Should().NotBeNull("a pass with removal writes its last phase");
          using var doc = System.Text.Json.JsonDocument.Parse(journal.Value!.Value);
          var root = doc.RootElement;
          root.GetProperty("op").GetString().Should().Be("backup-orphan-sweeper");
          root.GetProperty("phase").GetString().Should().Be("orphan-deleted/ghost/s1");
          root.GetProperty("instance").GetString().Should().Be(claims.InstanceId);
          root.GetProperty("updated_unix").GetInt64().Should().Be(clock.Now.ToUnixTimeSeconds());
          // work-ключ кластера сироты НЕ создан — прямой ассерт цели фикса
          (await KeyExistsAsync("/pgworker/work/ghost")).Should()
              .BeFalse("the sweeper does not write into per-cluster work keys");
      }
  ```

  Имена `SeedAsync`, `BuildSweeper`, `Options`, `KeyExistsAsync` — существующие
  члены класса теста; `claims.InstanceId` — публичное свойство `ClaimStore`;
  `clock.Now` — публичное свойство приватного класса `MutableClock` того же
  файла. Больше ничего не менять.

- [ ] **Подпункт 5.3.** Файл: тот же `BackupOrphanSweeperTests.cs`. В методе
  `SweepWithoutLeadershipWritesNothing` найди строки Assert:

  ```csharp
          var kv = await fixture.Gateway.GetAsync(
              fixture.Endpoint, OrphanRegistry.Key, ct);
          kv.Value.Should().BeNull("only the global leader writes the registry");
          s3.DeletedKeys.Should().BeEmpty();
  ```

  и ЗАМЕНИ их на (добавлен ассерт отсутствия orphan-journal у не-лидера):

  ```csharp
          var kv = await fixture.Gateway.GetAsync(
              fixture.Endpoint, OrphanRegistry.Key, ct);
          kv.Value.Should().BeNull("only the global leader writes the registry");
          (await KeyExistsAsync(OrphanRegistry.JournalKey)).Should()
              .BeFalse("non-leader does not write the orphan journal either");
          s3.DeletedKeys.Should().BeEmpty();
  ```

  Больше в методе ничего не менять.

---

### Шаг 6. Юнит-тест подавленных ops: третья строка Act (этап 2.2)

**Вход:** шаг 4 завершён (`SuppressedOps` содержит `backup-orphan-sweeper`).

**Действие:** подпункт 6.1 → coder-flash; ревью diff; коммит тестов.

**Выход:** `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries` гоняет все три
подавленных op; Assert без изменений (ни серий, ни операций).

**Проверка:** `grep -n "backup-orphan-sweeper" src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs` — 2 совпадения (комментарий
+ Act). Прогон — шаг 7. Коммит:

```bash
git add src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs
git commit -m "test(backups,metrics): orphan-journal — последняя фаза прохода, не-лидер молчит, suppression backup-orphan-sweeper"
```

**Связь со spec:** §5 этап 2 п.2, §3.3 п.3, §7 Р7; критерий приёмки 4.

**Подпункты (coder-flash):**

- [ ] **Подпункт 6.1.** Файл:
  `/Users/demakaev/ZCodeProject/worktrees/fix-orphan-journal-key/src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`.
  Найди тест-метод `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries` (блок):

  ```csharp
      public void OnJournalPhase_SuppressedOps_EmitNoPhaseSeries()
      {
          // Arrange: ревью Ф4-2 — supervise (стационарные записи, часть через
          // WriteSupervisionAsync мимо события) и evacuate (только waiting-*) не имеют
          // терминальной фазы: фазовые серии для них НЕ эмитим — иначе вечно горящий
          // ProcessPhaseStuck; живость надзора закрывает WorkerLoopStalled.
          using var meter = new Meter("TestWorker");
          using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

          // Act
          sut.OnJournalPhase("demo", "supervise", "dcs-converge");
          sut.OnJournalPhase("demo", "evacuate", "waiting-alive");

          // Assert: подавлены полностью — ни серий, ни операций
          sut.DebugSnapshot().Phases.Should().BeEmpty();
          sut.DebugSnapshot().Operations.Should().BeEmpty();
      }
  ```

  и ЗАМЕНИ его целиком на:

  ```csharp
      public void OnJournalPhase_SuppressedOps_EmitNoPhaseSeries()
      {
          // Arrange: ревью Ф4-2 — supervise (стационарные записи, часть через
          // WriteSupervisionAsync мимо события) и evacuate (только waiting-*) не имеют
          // терминальной фазы: фазовые серии для них НЕ эмитим — иначе вечно горящий
          // ProcessPhaseStuck; живость надзора закрывает WorkerLoopStalled.
          // backup-orphan-sweeper — журнал домена сирот (свой ключ
          // /pgworker/backups/orphan-journal): его фазы подавлены тоже.
          using var meter = new Meter("TestWorker");
          using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

          // Act
          sut.OnJournalPhase("demo", "supervise", "dcs-converge");
          sut.OnJournalPhase("demo", "evacuate", "waiting-alive");
          sut.OnJournalPhase("demo", "backup-orphan-sweeper", "orphan-deleting/ghost/s1");

          // Assert: подавлены полностью — ни серий, ни операций
          sut.DebugSnapshot().Phases.Should().BeEmpty();
          sut.DebugSnapshot().Operations.Should().BeEmpty();
      }
  ```

  Больше в файле ничего не менять.

---

### Шаг 7. Верификация: сборка Release + юниты (этапы 3.1-3.2)

Сильная модель. Подпунктов нет (запуски с наблюдением — base-rules №12/13/15).

**Вход:** шаги 1-6 завершены, коммиты arch/код/тесты созданы.

**Действие:**
1. Полная сборка (лог в файл, старт и завершение видны в логе):

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-plan-verify-build.log 2>&1
   ```

2. Юниты (двумя отдельными командами: сборка уже сделана в п.1; тесты — с
   `--no-build` и env-префиксом при команде):

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~BackupOrphanSweeperLoopTests|FullyQualifiedName~WorkerMetricsInstrumentationTests" > /tmp/pgw-plan-verify-unit.log 2>&1
   ```

   Запуск в фоне; в течение 15 с убедиться по логу, что тесты СТАРТОВАЛИ;
   каждые 2 мин контролировать отсутствие зависания (прогон юнитов — минуты).

3. Контроль scope-диффа: `git diff --stat main...HEAD` (или
   `git show --stat` по коммитам ветки) — изменённые файлы ТОЛЬКО из списка
   шагов 1-6; файлов иных потребителей `WorkJournal` нет (критерий
   приёмки 7).

**Выход:** сборка `Build succeeded`, 0 warnings; оба юнит-класса зелёные;
дифф ветки — только ожидаемые файлы.

**Проверка:** в `/tmp/pgw-plan-verify-build.log` — `Build succeeded` и
`0 Warning(s)`; в `/tmp/pgw-plan-verify-unit.log` — `Passed!` с числом
тестов ≥ 3 (LoopTests) + ≥ 1 (SuppressedOps) в своих классах, `Failed: 0`.

**Связь со spec:** §5 этап 3 пп.1-2; критерий приёмки 6 (сборка, юниты), 7.

---

### Шаг 8. Верификация: интегросерия BackupOrphanSweeperTests + зачистка (этап 3.3)

Сильная модель. Подпунктов нет.

**Вход:** шаг 7 зелёный; docker-демон доступен (фикстура OwnedEtcdFixture
поднимает свой etcd-контейнер).

**Действие:**
1. Прогон (отдельной командой после сборки шага 7, `--no-build`):

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~BackupOrphanSweeperTests > /tmp/pgw-plan-verify-int.log 2>&1
   ```

   Запуск в фоне; по логу ≤15 с — старт; каждые 2 мин — контроль; серия
   длиннее 5 мин — онлайн-анализ лога без остановки. Новый сценарий
   `SweepTtlExpiredWritesOrphanJournalPhase` обязан пройти.

2. ПОСЛЕ финальной строки прогона — зачистка docker-окружения серии:
   контрольные `docker ps -a` / `docker network ls` — артефактов серии
   (контейнеры/сети фикстуры) быть не должно; остатки — удалить по канону
   AGENTS.md (страховочный гейт; фикстура чистит сама — это проверка, что
   почистила).

**Выход:** вся серия `BackupOrphanSweeperTests` зелёная (включая новый
сценарий и расширенный не-лидерский); docker чист.

**Проверка:** в `/tmp/pgw-plan-verify-int.log` — `Passed!`, `Failed: 0`;
`docker ps -a` и `docker network ls` не содержат артефактов серии.

**Связь со spec:** §5 этап 3 п.3; критерии приёмки 2, 3, 6.

---

### Шаг 9. Мерж-гейт: docker-E2E минимум-маркер на свежем Release + зачистка (этап 3.4)

Сильная модель. Подпунктов нет.

**Вход:** шаг 8 зелёный; код воркера менялся → E2E-гейт обязателен (AGENTS.md).

**Действие:**
1. СВЕЖИЙ Release (сборка отдельной командой — свежесть бинаря видна её
   выводом; сборку шага 7 НЕ переиспользовать, если после неё были правки —
   при правках между шагами пересобрать здесь же):

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/pgw-plan-verify-e2e-build.log 2>&1
   ```

2. Статическая подготовка E2E ОДНИМ процессом ДО тестов:

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/pgw-plan-verify-e2eprepare.log 2>&1
   ```

3. Docker-E2E минимум-маркер (только `--no-build`, env-префикс при команде):

   ```bash
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/pgw-plan-verify-e2e.log 2>&1
   ```

   Полный E2eFixture НЕ требуется (provisioning/portalloc/moves не меняются —
   решение Д5). Наблюдение — канон base-rules №13 + телеметрия E2E
   (`[PHASE]`-фазы, артефакты `/tmp/pgw-e2e-artifacts-*` при падении; без
   перезапуска упавших — разбор по логам).

4. После серии — зачистка docker-окружения (контейнеры/сети/тома прогона;
   контроль `docker ps -a`, `docker network ls`; при осиротевших сетях —
   `docker network prune -f`).

**Выход:** E2E-маркер `Scale_AddEmptyShard` зелёный на свежем Release; docker
чист; ветка готова к ревью dev-flow.

**Проверка:** `/tmp/pgw-plan-verify-e2e-build.log` — `Build succeeded`,
0 warnings; `/tmp/pgw-plan-verify-e2eprepare.log` — штамп готовности;
`/tmp/pgw-plan-verify-e2e.log` — `Passed!`, `Failed: 0`; docker чист.

**Связь со spec:** §5 этап 3 п.4, §7 Д5; критерий приёмки 6.

---

## Решения вне spec (делегировано; синхронизировать spec)

Все поведенческие развилки закрыты в spec §7 (Р1-Р8, Д1-Д5). Ниже —
локальные уточнения, заданные планом (на поведение системы не влияют):

1. Имя нового интеграционного сценария — `SweepTtlExpiredWritesOrphanJournalPhase`
   (шаг 5, подпункт 5.2); spec описывал сценарий без имени.
2. Формулировки doc-комментариев новых членов `OrphanRegistry`
   (`JournalKey`, `JournalPhaseToJson`) и комментария `WriteJournalAsync` —
   по образцу соседей (spec задавал сигнатуры и стиль, не дословный текст).
3. Сообщение `LogWarning` в `WriteJournalAsync`:
   `"{Op}: orphan journal write failed for phase {Phase}"` (spec задавал
   семантику — предупреждение, не исключение).
4. Чтение JSON в новом сценарии — полностью квалифицированным
   `System.Text.Json.JsonDocument.Parse` без добавления using в тестовый
   файл (минимум правок).
5. Точная позиция чистки `JournalKey` в `SeedAsync` — сразу после чистки
   `OrphanRegistry.Key` (spec: «рядом с существующими чистками»).
6. Тексты коммит-сообщений (arch / код / тесты) — три коммита, arch первым
   (реализация требования критерия приёмки 5 «arch закоммичен ДО кодовых»).
7. Комментарии кода в местах замен (строки 212-213 «journal-before-
   manipulations…» и над фабрикой Program.cs) оставлены без изменений:
   упоминаемый «журнал» после фикса — orphan-journal, смысл комментария
   сохраняется; spec их правку не требовал.
8. Пополнение перечня ops в комментарии над `OnJournalPhase` (~строка 543
   `WorkerMetricsInstrumentation.cs`) — по замечанию ревью плана (Фаза 4):
   код-комментарий обязан перечислять все элементы множества; spec эту строку
   не упоминал.

## Самопроверка покрытия spec

- §5 этап 0 → шаг 1; этап 1.1 → шаг 2; 1.2-1.3 → шаг 3 (подпункты 3.1-3.4);
  1.4 → шаг 4; 1.5 → шаг 3 (подпункты 3.5-3.6); этап 2.1 → шаг 5; 2.2 →
  шаг 6; этап 3 п.1 → шаг 7, п.2 → шаг 7, п.3 → шаг 8, п.4 → шаг 9.
- Критерии приёмки: 1 → шаг 3 (проверка), 2 → шаг 5.2 + шаг 8, 3 → шаг 5.3 +
  шаг 8, 4 → шаги 4/6 + шаг 7, 5 → шаг 1 (коммиты), 6 → шаги 7-9, 7 → шаг 7
  (контроль диффа).
- §6 (ограничения): ни один шаг не выходит за пределы перечисленных файлов;
  остальные потребители `WorkJournal` не трогаются.
