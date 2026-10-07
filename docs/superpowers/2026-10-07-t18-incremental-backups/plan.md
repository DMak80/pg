# t18 (переопределена): целевая схема бэкапов — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** система бэкапов работает по целевой схеме пользователя из коробки: дефолт ретенции 1/1/1 (три GFS-позиции: последний + недельная + месячная), WAL-cutoff = `wal_start` новейшего verify-OK полного (гвард «без успешного verify — не срезать»), стартовая точка контроля цепочки (`chain_start`) — та же точка, что cutoff.

**Архитектура:** arch-first — сначала правки канона `arch/19-backups.md`, затем минимальный код: дефолты конфигурации, одна чистая функция выбора точки (`RetentionPlanner.LatestVerifiedWalStart`), новый ratchet-гвард (`WalChain.RatchetedStart` с одиночным кандидатом), две точки применения (шаг 4 `RetentionProcess`, `ControlDueAsync` `WalStreamProcess`). Механика GFS-отбора уточняется под буквальную формулировку канона §4 п.1 («ПРЕДЫДУЩИХ недель/месяцев»). DELETING-доводка, guard последнего COMPLETED, приёмник WAL, verify-джоб, restore, панель — без изменений.

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit v3 + FluentAssertions; юниты `src/tests/PgWorker.UnitTests`, интеграции (etcd-testcontainers + FakeBackupS3, non-E2E) `src/tests/PgWorker.IntegrationTests`.

**Spec:** `docs/superpowers/2026-10-07-t18-incremental-backups/spec.md` (там же каталог плана). Worktree: `feat-t18-incremental-backups`.

## Global Constraints

- Сборка Release — 0 warnings / 0 errors (`TreatWarningsAsErrors=true`), команда гейта: `dotnet build src/PgWorker.slnx -c Release`.
- Комментарии и документация — по-русски; идентификаторы — на английском; тесты — с AAA-комментариями (Arrange/Act/Assert).
- Arch-first: задача 1 (канон) выполняется и коммитится ДО любой правки кода (AGENTS.base.md §1).
- Коммитить в feature-ветке — свободно (стиль `feat(t18): …` / `test(t18): …` / `docs(t18): …`); мерж в `main` и roadmap-гейт (задача 9) — ТОЛЬКО по явной команде пользователя, тем же мерж-коммитом.
- Любые запуски команд тестов/сборки — в фоне (`run_in_background`), ожидания ≤ 30 с; после КАЖДОЙ тестовой серии — контроль зачистки docker (правила `pg/AGENTS.md`): `docker ps -a --format '{{.Names}}' | grep -c '^pgw-'` → 0 новых; осиротевшие сети `docker network ls | grep -c 'pgw-.*-net\|kfw-net'` не растут (поднятый dev-стенд не трогаем — сверяем со списком контейнеров ДО прогона).
- Упавший тест НЕ перезапускается без анализа логов (правила телеметрии E2E/интеграций `pg/AGENTS.md`).
- docker-E2E в задаче НЕ обязателен (spec §6 фаза 3: provisioning/portalloc/moves не затронуты, `PgWorker.Backups` вне перечня обязательного E2E-гейта).
- Решение по GFS-отбору (вопрос пользователю от 2026-10-07 остался без ответа; принято по канону — arch-first, AGENTS.base.md §1): недельные/месячные кандидаты GFS — строго из ПРЕДЫДУЩИХ календарных периодов, как сформулировано в `arch/19-backups.md` §4 п.1. Без этого AC1 недостижим (см. задачу 3). Если пользователь решит иначе — вернуться к задаче 3.

---

### Задача 1: Канон arch/19-backups.md — дефолт 1/1/1, cutoff/гвард verify, chain_start от cutoff-точки

**Вход (предусловие):** worktree `feat-t18-incremental-backups` чист (`git status`); код ещё не тронут (arch-first).

**Действие (файлы):** Modify `arch/19-backups.md` — восемь точечных правок ниже (цитаты «было → стало»).

**Выход:** канон описывает целевую схему; код после задач 2–7 зеркалит его.

**Проверка:** grep-гейты (шаг 1.9) + ревью arch-диффа; зелёных тестов не требует (документ).

**Связь со spec:** §5.1 (пп. 1–5), AC6; фаза 1 (§6).

- [ ] **Шаг 1.1: §9 — дефолт ретенции**

Было (строка ~800, «Конфигурация»):
```
`Policy { Retention { Days=7, Weeks=4, Months=6 }, FullMaxAgeSec=86400,
```
Стало:
```
`Policy { Retention { Days=1, Weeks=1, Months=1 }, FullMaxAgeSec=86400,
```

- [ ] **Шаг 1.2: §4 — policy-пример в таблице ключей**

Было (строка ~529, ключ `/pgworker/backups/<C>/policy`): `"retention":{"days":7,"weeks":4,"months":6}`.
Стало: `"retention":{"days":1,"weeks":1,"months":1}`.

- [ ] **Шаг 1.3: §4 п.1 — уточнение «предыдущих периодов»**

После предложения «Счётчики — policy-ключ кластера, дефолт — `PgWorker:Backups:Policy`.» добавить:
```
Полные ТЕКУЩЕЙ недели/месяца вне дневного окна недельными/месячными
точками НЕ удерживаются: счётчики считают строго ПРЕДЫДУЩИЕ календарные
недели/месяцы; текущий период удерживает только дневная гранула.
```

- [ ] **Шаг 1.4: §4 п.5 «Чистка WAL» — новая формулировка cutoff + гварда**

Было (строки ~587–595):
```
5. **Чистка WAL**: сегменты префикса `wal/` СТРОГО ниже стартовой точки
   (`wal_start_segment`) старейшего ОСТАВЛЯЕМОГО COMPLETED-полного
   удаляются — сравнение по позиции (`log·256+seg`) при равном TLI и
   весь TLI ниже стартового, включая `.history` тех TLI (§5). Сам
   стартовый сегмент и всё выше/новее — живут. Полных нет → WAL не
   чистится (нет точки привязки). `chain_start` t03 пересчитывается от
   оставшихся полных автоматически при следующем контроле (list S3 —
   истина): list-префикс укорачивается, старые дыры ниже cutoff
   исчезают.
```
Стало:
```
5. **Чистка WAL**: cutoff = `wal_start_segment` новейшего (по
   `started_unix`) COMPLETED-полного с `verify.state=OK` — срез только
   после создания полного И его успешного verify. Сегменты префикса
   `wal/` СТРОГО ниже cutoff удаляются — сравнение по позиции
   (`log·256+seg`) при равном TLI и весь TLI ниже стартового, включая
   `.history` тех TLI (§5). Сам cutoff-сегмент и всё выше/новее — живут.
   ГВАРД verify: прунинг WAL ниже точки полного, НЕ прошедшего verify
   (PENDING/FAILED — в т.ч. при перепроверке OK→FAILED), запрещён —
   cutoff держится на предыдущем verify-OK полном; verify-OK-полного
   нет вовсе → WAL не чистится (нет безопасной точки привязки).
   Недельная/месячная GFS-точки на cutoff НЕ влияют. `chain_start` t03
   пересчитывается автоматически при следующем контроле (list S3 —
   истина; старт контроля — та же точка, что cutoff, §3): list-префикс
   укорачивается, старые дыры ниже cutoff исчезают.
```

- [ ] **Шаг 1.5: §3 «Правила непрерывности» — стартовая точка контроля**

Было (строки ~257–262, фрагмент):
```
Цепочка проверяется от `chain_start_segment`: стартовая
точка = `wal_start_segment` старейшего COMPLETED полного бэкапа; полных
нет → первый сегмент потока агентов (закрепляется в статусе при первом
upload). Ретенция (t06) чистит сегменты ниже стартовой точки старейшего
ОСТАВЛЯЕМОГО полного (§4) — `chain_start` поднимается автоматически при
следующем контроле (list S3 — истина), list-префикс укорачивается.
```
Стало:
```
Цепочка проверяется от `chain_start_segment`: стартовая
точка = `wal_start_segment` новейшего (по `started_unix`) COMPLETED-
полного с `verify.state=OK` — ТА ЖЕ точка, что cutoff WAL-чистки
ретенции (§4 п.5; расщепление точек даёт ложный BROKEN: контроль,
стартующий от срезанных сегментов, видит дыру). verify-OK-полного нет →
первый сегмент потока агентов (закрепляется в статусе при первом
upload). Ретенция (t06) чистит сегменты ниже этой точки — `chain_start`
поднимается автоматически при следующем контроле (list S3 — истина),
list-префикс укорачивается.
```
И в этом же §3, абзац про ratchet (строки ~284–286), было:
```
При контроле цепочки стартовая точка = min(wal_start_segment COMPLETED-полных с wal_start ≥
записанного chain_start) — полные со стартом ниже границы разрыва
игнорируются для контроля (их цепь до их точки может быть цела; дыра
выше).
```
Стало:
```
При контроле цепочки стартовая точка = wal_start_segment новейшего (по
started_unix) COMPLETED-полного с `verify.state=OK` и wal_start ≥
записанного chain_start (ratchet не понижается) — кандидаты ниже границы
разрыва игнорируются для контроля (их цепь до их точки может быть цела;
дыра выше).
```

- [ ] **Шаг 1.6: §5 «Ретенционная чистка» — синхронно точке cutoff**

Было (строки ~679–686, фрагмент): «чистка WAL = delete сегментов `wal/<segment>` со позицией (`log·256+seg`) строго ниже стартовой точки старейшего оставляемого полного при равном TLI, и всех объектов TLI ниже стартового».
Стало: «чистка WAL = delete сегментов `wal/<segment>` со позицией (`log·256+seg`) строго ниже cutoff — `wal_start` новейшего verify-OK COMPLETED-полного (§4 п.5; нет OK-полного — чистка не выполняется) — при равном TLI, и всех объектов TLI ниже стартового».

- [ ] **Шаг 1.7: §8 (карта задач, строка t06) — согласование формулировки**

Было (строка ~793): «…чистка WAL ниже стартовой точки оставляемых…».
Стало: «…чистка WAL ниже cutoff последнего verify-OK полного…».

- [ ] **Шаг 1.8: §10 (риски) — осознанное следствие схемы + синхронизация строки риска**

Новая строка риск-таблицы (после строки «Ошибка ретенции удаляет нужный бэкап…»):
```
| Накат WAL поверх недельной/месячной GFS-точки невозможен (WAL ниже последнего verify-OK полного срезан) | осознанное следствие целевой схемы (retention 1/1/1 + cutoff по последнему verify-OK): старые GFS-точки восстановимы до собственной точки съёма — WAL их `-X stream`-набора лежит при них в `full/<id>/pg_wal/`; PITR с накатом — только от последнего валидного полного; restore-заявка от старого полного с целью позже его точки честно падает на валидации цепочки (permanent-отказ с границами) |
```
И в этой же таблице — хвост строки «Ошибка ретенции удаляет нужный бэкап…» (строка ~853) остаётся со старой семантикой cutoff, синхронизировать. Было: «…; WAL-чистка — строго ниже стартовой точки оставляемых (§4/§5, t06)». Стало: «…; WAL-чистка — строго ниже cutoff последнего verify-OK полного (§4/§5)».

- [ ] **Шаг 1.9: Проверка канона grep-гейтами и commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t18-incremental-backups
grep -n "Days=7\|days\":7" arch/19-backups.md            # ожидание: пусто
grep -n "старейшего ОСТАВЛЯЕМОГО" arch/19-backups.md      # ожидание: пусто
grep -c "verify-OK" arch/19-backups.md                    # ожидание: >= 6 (строки с «verify-OK» в §3/§4 п.5/§5/§8/§10, вкл. обе строки рисков; маркер однострочный — длинные фразы cutoff в каноне переносятся строками, grep по ним ненадёжен)
grep -n "Days=1, Weeks=1, Months=1" arch/19-backups.md    # ожидание: 1 (§9)
```
Commit: `git add arch/19-backups.md && git commit -m "docs(t18): канон arch/19 — ретенция 1/1/1, WAL-cutoff по новейшему verify-OK полному (гвард verify), chain_start от той же точки, недельные/месячные GFS-точки строго предыдущих периодов (spec §5.1)"`

---

### Задача 2: Дефолты ретенции 1/1/1 в коде конфигурации и API-хендлера policy

**Вход:** задача 1 закоммичена (канон — источник правды). Второй дефолт ретенции 7/4/6 — `BackupsPolicyHandler` (приём policy через API): отсутствующие retention-поля тела PUT замещаются 7/4/6 и пишутся в policy-ключ; после правки канона хендлер обязан зеркалить его, иначе первый partial-policy PUT вернёт кластеру 7/4/6 (до ~17 полных) — ломает «целевую схему из коробки» (spec §3/§8). Это дефолт ретенции из минимальной поверхности spec, НЕ расширение scope.

**Действие (файлы):**
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (тест `Defaults_PolicyStagingAgent_CanonValues`, строки ~114–116).
- Test: `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs` (тест `Опущенные_поля_дефолты`, строки ~59–79).
- Modify: `src/PgWorker.App/Options.cs` (`BackupsRetentionOptions`, строки ~595–602), `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`, строки ~33–35).
- Modify: `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs` (дефолты отсутствующих retention-полей тела PUT, строки ~73–75).

**Выход:** дефолт конфигурации `Days=1, Weeks=1, Months=1` на обоих уровнях (bind-опции и runtime) И в API-хендлере приёма policy (partial-policy PUT больше не возвращает кластеру 7/4/6); per-cluster policy-ключ по-прежнему перекрывает.

**Проверка:** юнит `BackupsOptionsTests` зелёный; интеграционный `BackupsPolicyApiTests.Опущенные_поля_дефолты` зелёный (etcd-testcontainers, non-E2E).

**Связь со spec:** §5.2 п. 1, AC1 («дефолт конфигурации Days=1, Weeks=1, Months=1»), §3/§8 (целевая схема «из коробки», без ручных policy-ключей; дефолт — выбор спеки, механизм policy сохранён).

- [ ] **Шаг 2.1: Править тест дефолтов (RED)**

В `BackupsOptionsTests.Defaults_PolicyStagingAgent_CanonValues` заменить ассерты и комментарий:
```csharp
// Assert — GFS 1/1/1 — целевая схема (t18: последний + недельная +
// месячная позиции), суточное окно, verify при создании; staging-каталог;
// квота и лимиты джобов — null (без лимита, образец request_* нод
// arch/14 §2.4 п.4).
options.Policy.Retention.Days.Should().Be(1);
options.Policy.Retention.Weeks.Should().Be(1);
options.Policy.Retention.Months.Should().Be(1);
```

- [ ] **Шаг 2.2: Прогнать тест — убедиться в падении**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~BackupsOptionsTests.Defaults_PolicyStagingAgent_CanonValues"
```
Ожидание: FAIL (`Days` = 7, ожидалось 1). Прогон — в фоне, результат забрать по task_id.

- [ ] **Шаг 2.3: Править дефолты (GREEN)**

`src/PgWorker.App/Options.cs`, класс `BackupsRetentionOptions`:
```csharp
/// <summary>GFS-ретенция полных бэкапов (дни/недели/месяцы, t06; t18 —
/// дефолт целевой схемы: последний + недельная + месячная позиции).</summary>
public sealed class BackupsRetentionOptions
{
    public int Days { get; set; } = 1;

    public int Weeks { get; set; } = 1;

    public int Months { get; set; } = 1;
}
```
`src/PgWorker.Backups/Options.cs`, параметры `BackupsRuntimeOptions` (строки ~33–35) + комментарий над ними:
```csharp
    // t06 (arch/19 §9): ретенция и квота; t18 — дефолт-политика GFS для кластеров
    // без policy-ключа: 1/1/1 — целевая схема (последний + недельная + месячная).
    int PolicyRetentionDays = 1,
    int PolicyRetentionWeeks = 1,
    int PolicyRetentionMonths = 1,
```

- [ ] **Шаг 2.4: Прогнать тест — зелёный + commit**

Команда та же, ожидание PASS. Затем:
```bash
git add src/PgWorker.App/Options.cs src/PgWorker.Backups/Options.cs src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs
git commit -m "feat(t18): дефолт ретенции полных 1/1/1 — целевая схема (последний + недельная + месячная позиции) (spec §5.2 п.1, AC1)"
```

- [ ] **Шаг 2.5: Править тест API-дефолтов (RED)**

В `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs`, тест `Опущенные_поля_дефолты` — заменить комментарий ассерта и retention-ассерты:
```csharp
        // Assert — 200, ключ с дефолтами 1/1/1/86400/true (t18: целевая схема,
        // хендлер зеркалит дефолт канона arch/19 §9)
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd/policy", ct);
        stored.Value!.Value.Should().Contain("\"days\":1")
            .And.Contain("\"weeks\":1")
            .And.Contain("\"months\":1")
            .And.Contain("\"full_max_age_sec\":86400")
            .And.Contain("\"on_create\":true");
```

- [ ] **Шаг 2.6: Прогнать тест — убедиться в падении**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~BackupsPolicyApiTests.Опущенные_поля_дефолты"
```
Ожидание: FAIL (ключ содержит `"days":7`, ожидалось `"days":1`). Прогон — в фоне, результат забрать по task_id; после — контроль зачистки docker (etcd-контейнер testcontainers подбирает ryuk).

- [ ] **Шаг 2.7: Править дефолты хендлера (GREEN), прогон, commit**

`src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs` (строки ~73–75) — дефолты отсутствующих retention-полей тела → 1/1/1:
```csharp
        //    t18: отсутствующие retention-поля → дефолт 1/1/1 (целевая схема,
        //    arch/19 §9) — partial-policy PUT не возвращает кластер к 7/4/6.
        var days = body.Retention?.Days ?? 1;
        var weeks = body.Retention?.Weeks ?? 1;
        var months = body.Retention?.Months ?? 1;
```
Прогон — команда шага 2.6, ожидание PASS; контроль зачистки docker. Затем:
```bash
git add src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs
git commit -m "feat(t18): дефолт ретенции API-хендлера policy 1/1/1 — partial-policy PUT не возвращает кластер к 7/4/6 (spec §3/§8, AC1)"
```

---

### Задача 3: GFS-отбор — недельные/месячные точки строго ПРЕДЫДУЩИХ периодов

**Вход:** задачи 1–2 закоммичены. Обоснование (решение по неотвеченному вопросу, см. Global Constraints): текущий `AddCalendarPoint` («N свежайших групп вне дневного окна») при days=1 тратит недельный слот на ТЕКУЩУЮ ISO-неделю, месячный — на ТЕКУЩИЙ месяц (при 7/4/6 это маскировалось 7-дневным окном): Keep вырождается в один «последний», недельная/месячная позиции удаляются — AC1 недостижим. Канон §4 п.1 формулирует буквально «ПРЕДЫДУЩИХ календарных недель/месяцев» — код подводится под канон.

**Действие (файлы):**
- Test: `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs` (два новых теста + актуализация комментария теста `Годовая_граница_ISO_недели`).
- Modify: `src/PgWorker.Backups/Retention/RetentionPlanner.cs` (`AddCalendarPoint`, строки ~167–188; вызовы в `SelectKeep`, строки ~61–64).

**Выход:** при 1/1/1 на суточном ряду Keep = {последний, последний полный предыдущей ISO-недели, последний полный предыдущего месяца}; при days≥7 поведение недельной гранулы не меняется, месячная — строго по канону.

**Проверка:** `RetentionPlannerTests` целиком зелёный (все существующие 16 тестов SelectKeep перепроверены на новую семантику — проходят без правок ассертов, вкл. `Пустой_набор` и `Политика_0_0_0`).

**Связь со spec:** AC1 (ядро: «ровно три позиции»), §1 п. 2 («недельная точка = последний полный предыдущей календарной ISO-недели, месячная = предыдущего календарного месяца»); §5.3 первый буллет.

- [ ] **Шаг 3.1: Два падающих теста (RED)**

Добавить в `RetentionPlannerTests` (использует существующие хелперы `Now` = ср 2026-09-09 12:00 UTC, `Full`, `Select`):
```csharp
// ---- t18: точность «предыдущих периодов» GFS (AC1) ----

// AC1: дефолт 1/1/1 на суточном ряду — ровно ТРИ позиции: последний +
// последний полный предыдущей ISO-недели + последний полный предыдущего
// календарного месяца; промежуточные суточные — кандидаты удаления.
// now = ср 2026-09-09 (ISO-неделя 37: пн 07.09); предыдущая неделя W36 =
// 31.08–06.09; предыдущий месяц — август.
[Fact]
public void Дефолт_1_1_1_ровно_три_позиции()
{
    // Arrange — суточный ряд: 08.09 (последний, W37), 07.09 (W37),
    // 06.09 (вс — последний W36), 04.09 (пт W36), 01.09 (вт W36),
    // 31.08 (пн W36 — последний августа), 20.08, 15.07 (старше месячного окна)
    var policy = new BackupPolicy(1, 1, 1, 86400, false);
    FullBackupState[] fulls =
    [
        Full("last", new DateTimeOffset(2026, 9, 8, 10, 0, 0, TimeSpan.Zero)),
        Full("w37-mid", new DateTimeOffset(2026, 9, 7, 10, 0, 0, TimeSpan.Zero)),
        Full("w36-last", new DateTimeOffset(2026, 9, 6, 10, 0, 0, TimeSpan.Zero)),
        Full("w36-fri", new DateTimeOffset(2026, 9, 4, 10, 0, 0, TimeSpan.Zero)),
        Full("w36-tue", new DateTimeOffset(2026, 9, 1, 10, 0, 0, TimeSpan.Zero)),
        Full("aug-last", new DateTimeOffset(2026, 8, 31, 10, 0, 0, TimeSpan.Zero)),
        Full("aug-mid", new DateTimeOffset(2026, 8, 20, 10, 0, 0, TimeSpan.Zero)),
        Full("jul", new DateTimeOffset(2026, 7, 15, 10, 0, 0, TimeSpan.Zero)),
    ];

    // Act — отбор
    var (keep, delete) = Select(policy, fulls);

    // Assert — три позиции; всё промежуточное (вкл. текущую неделю вне
    // дневного окна и середину августа) — Delete
    keep.Should().BeEquivalentTo(["last", "w36-last", "aug-last"]);
    delete.Should().BeEquivalentTo(["w37-mid", "w36-fri", "w36-tue", "aug-mid", "jul"]);
}

// Дискриминация бага «слот на текущем периоде»: недельный слот НЕ уходит на
// текущую ISO-неделю — вчерашний полный (W37) удерживается только guard
// последнего, недельная точка = последний W36 (канон §4 п.1 «предыдущих»).
[Fact]
public void Недельный_слот_не_уходит_на_текущую_неделю()
{
    // Arrange — days=1/weeks=1/months=0; вчера 08.09 (W37) и 06.09 (W36)
    var policy = new BackupPolicy(1, 1, 0, 86400, false);
    FullBackupState[] fulls =
    [
        Full("yesterday", new DateTimeOffset(2026, 9, 8, 10, 0, 0, TimeSpan.Zero)),
        Full("w36-last", new DateTimeOffset(2026, 9, 6, 10, 0, 0, TimeSpan.Zero)),
    ];

    // Act — отбор
    var (keep, delete) = Select(policy, fulls);

    // Assert — недельная точка W36 удержана (багованный отбор брал W37 и
    // ставил w36-last в Delete)
    keep.Should().BeEquivalentTo(["yesterday", "w36-last"]);
    delete.Should().BeEmpty();
}

// Дискриминация бага «слот на текущем периоде»: месячный слот НЕ уходит на
// текущий месяц — точка = последний ПРЕДЫДУЩЕГО месяца.
[Fact]
public void Месячный_слот_не_уходит_на_текущий_месяц()
{
    // Arrange — days=1/weeks=0/months=1; 01.09 (текущий месяц, самый свежий)
    // и 31.08 (последний августа)
    var policy = new BackupPolicy(1, 0, 1, 86400, false);
    FullBackupState[] fulls =
    [
        Full("sep01", new DateTimeOffset(2026, 9, 1, 10, 0, 0, TimeSpan.Zero)),
        Full("aug31", new DateTimeOffset(2026, 8, 31, 10, 0, 0, TimeSpan.Zero)),
    ];

    // Act — отбор
    var (keep, delete) = Select(policy, fulls);

    // Assert — месячная точка августа удержана; 01.09 жив только guard-ом
    // последнего (багованный отбор тратил слот на сентябрь → aug31 удалялся)
    keep.Should().BeEquivalentTo(["sep01", "aug31"]);
    delete.Should().BeEmpty();
}
```
Актуализировать комментарий теста `Годовая_граница_ISO_недели` (ассерт не меняется): «одна группа → ОДИН слот» → «обе даты — ОДНА ISO-неделя 1 ISO-2025 (предыдущая от now=W2): одна группа → один слот weeks; представитель 05.01 удержан, Delete = [2024-12-30]».

- [ ] **Шаг 3.2: Прогон — RED**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RetentionPlannerTests"
```
Ожидание: 3 новых FAIL; существующие — PASS.

- [ ] **Шаг 3.3: Правка AddCalendarPoint (GREEN)**

`src/PgWorker.Backups/Retention/RetentionPlanner.cs` — вызовы в `SelectKeep` (строки ~61–64):
```csharp
// Недельные: полные ВНЕ ТЕКУЩЕЙ ISO-недели группируются по ISO-неделе UTC;
// из каждой из retention.weeks предыдущих свежайших групп — последний.
AddCalendarPoint(completed, keep, policy.RetentionWeeks, ISOWeekYearOf, now);

// Месячные: аналогично по календарному месяцу UTC.
AddCalendarPoint(completed, keep, policy.RetentionMonths, MonthOf, now);
```
(оба вызова передают `now` — 5-й параметр сигнатуры `DateTime`; группа текущего периода вычисляется ВНУТРИ метода от синтетической записи — передавать вычисленную группу-кортеж вместо `now` нельзя: CS1503.) Сам метод (заменяет строки ~163–188):
```csharp
// Универсальная гранула (недели/месяцы): кандидаты — полные ПЕРИОДА
// СТРОГО РАНЬШЕ текущего (t18, arch/19 §4 п.1: счётчики считают предыдущие
// календарные периоды; полные текущей недели/месяца вне дневного окна
// точками не удерживаются — текущий период держит только дневная гранула)
// → группы по календарному периоду UTC → N свежайших групп (сравнение
// (Year, Num) лексикографически) → из каждой последний COMPLETED
// (max started_unix). counter = 0 → гранула исключена.
private static void AddCalendarPoint<TGroup>(
    List<FullBackupState> completed, HashSet<string> keep, int counter,
    Func<FullBackupState, TGroup> groupOf, DateTime now)
    where TGroup : IComparable<TGroup>
{
    if (counter <= 0)
        return;

    var current = groupOf(new FullBackupState(
        "now", FullBackupStatus.Completed, "n1", BackupSourceRole.Replica,
        new DateTimeOffset(now).ToUnixTimeSeconds(), null, null, null, null, null));
    var previous = completed
        .Where(f => groupOf(f).CompareTo(current) < 0)
        .GroupBy(groupOf)
        .OrderByDescending(g => g.Key)
        .Take(counter);
    foreach (var group in previous)
    {
        var last = group.MaxBy(f => f.StartedUnix); // ПОСЛЕДНИЙ периода (AC1)
        if (last is not null)
            keep.Add(last.Id);
    }
}
```
ПРИМЕЧАНИЕ для исполнителя: `groupOf` принимает `FullBackupState` — для вычисления группы текущего момента строится синтетическая запись от `now` (альтернатива — перегрузка `Func<DateTime, TGroup>`; синтетическая запись короче и не меняет две существующие групповые функции `ISOWeekYearOf`/`MonthOf`). Параметр `dailyWindow` из сигнатуры удаляется — фильтр «вне дневного окна» заменён более сильным фильтром «период раньше текущего» (дневное окно остаётся первой гранулой отбора без изменений).

- [ ] **Шаг 3.4: Прогон — GREEN (весь класс, без фильтра метода)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RetentionPlannerTests"
```
Ожидание: все PASS (новые 3 + существующие, включая `Високосность`, `Годовая_граница_ISO_недели(53)`, `Guard_самый_свежий_всегда_в_Keep`).

- [ ] **Шаг 3.5: Commit**

```bash
git add src/PgWorker.Backups/Retention/RetentionPlanner.cs src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs
git commit -m "feat(t18): GFS недельные/месячные точки строго предыдущих периодов (канон §4 п.1) — при 1/1/1 держат ровно три позиции, слоты не утекают на текущую неделю/месяц (spec AC1)"
```

---

### Задача 4: Чистая функция точки отсчёта — RetentionPlanner.LatestVerifiedWalStart

**Вход:** задачи 1–3 закоммичены.

**Действие (файлы):**
- Test: `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs` (хелпер `Full` расширяется параметром `walStart`; 4 новых теста).
- Modify: `src/PgWorker.Backups/Retention/RetentionPlanner.cs` (новый публичный метод).

**Выход:** общая юнит-тестируемая точка «wal_start новейшего (по started_unix) COMPLETED-полного с verify.state=OK»; потребители — задачи 5 и 6.

**Интерфейс (Produces):**
```csharp
public static WalFileName? LatestVerifiedWalStart(IReadOnlyList<FullBackupState> fulls)
```
— `null`, когда verify-OK-полного нет (прунинг запрещён).

**Проверка:** `RetentionPlannerTests` зелёный.

**Связь со spec:** §5.2 п. 4 («чистый хелпер выбора "новейшего verify-OK полного", общий для RetentionProcess и WalStreamProcess»), §4 принципа «Verify как ключ ретенции WAL», AC2/AC3 (функциональное ядро).

- [ ] **Шаг 4.1: Расширить хелпер Full и добавить 4 теста (RED)**

Хелпер тестов (заменить существующий):
```csharp
// COMPLETED-полный с заданным стартом (остальные поля — нейтральные);
// walStart — позиция wal_start_segment, verify — статус проверки.
private static FullBackupState Full(
        string id, DateTimeOffset started,
        string walStart = "000000010000000000000001", BackupVerify? verify = null)
    => new(id, FullBackupStatus.Completed, "n1", BackupSourceRole.Replica,
        Unix(started), Unix(started) + 60, walStart, 1024, null, verify);
```
Новые тесты:
```csharp
// ---- t18: LatestVerifiedWalStart — точка отсчёта cutoff/chain_start (AC2/AC3) ----

// AC2: точка = wal_start новейшего (по started_unix) OK-полного — недельная/
// месячная (старые OK) на неё НЕ влияют.
[Fact]
public void LatestVerifiedWalStart_новейший_OK_полный()
{
    // Arrange — месячный OK ..01, недельный OK ..02, последний OK ..08
    var fulls = new[]
    {
        Full("m", Now.AddMonths(-1), "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
        Full("w", Now.AddDays(-8), "000000010000000000000002",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
        Full("last", Now.AddDays(-1), "000000010000000000000008",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
    };

    // Act — выбор точки
    var start = RetentionPlanner.LatestVerifiedWalStart(fulls);

    // Assert — wal_start последнего OK-полного
    start!.Value.Name.Should().Be("000000010000000000000008");
}

// AC3: verify не OK (PENDING/FAILED/отсутствует) — полные не кандидаты;
// ни одного OK → null (прунинг no-op).
[Fact]
public void LatestVerifiedWalStart_без_OK_полных_null()
{
    // Arrange — последний PENDING, старший FAILED, третий без verify вовсе
    var fulls = new[]
    {
        Full("pending", Now.AddDays(-1), "000000010000000000000008",
            new BackupVerify(BackupVerifyStatus.Pending, null)),
        Full("failed", Now.AddDays(-2), "000000010000000000000005",
            new BackupVerify(BackupVerifyStatus.Failed, 1)),
        Full("noverify", Now.AddDays(-3), "000000010000000000000003"),
    };

    // Act — выбор точки
    var start = RetentionPlanner.LatestVerifiedWalStart(fulls);

    // Assert — безопасной точки нет
    start.Should().BeNull();
}

// AC3-гвард: новейший по времени полный не прошёл verify (PENDING/FAILED,
// в т.ч. перепроверка OK→FAILED) — точка держится на предыдущем OK.
[Fact]
public void LatestVerifiedWalStart_непроверенный_новый_держит_предыдущий_OK()
{
    // Arrange — OK недельной давности ..03 и вчерашний FAILED ..08
    var fulls = new[]
    {
        Full("ok-old", Now.AddDays(-8), "000000010000000000000003",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
        Full("failed-new", Now.AddDays(-1), "000000010000000000000008",
            new BackupVerify(BackupVerifyStatus.Failed, 1)),
    };

    // Act — выбор точки
    var start = RetentionPlanner.LatestVerifiedWalStart(fulls);

    // Assert — cutoff на предыдущем OK (WAL ниже ..03 уже срезан ранее —
    // понижение точки ничего не восстановит и запрещено гвардом)
    start!.Value.Name.Should().Be("000000010000000000000003");
}

// Дефективный wal_start у новейшего OK (ручная правка ключа) — пропускается,
// берётся следующий OK ниже ( defensiveness парсера, без исключений).
[Fact]
public void LatestVerifiedWalStart_битый_wal_start_пропущен()
{
    // Arrange — новейший OK с мусорным wal_start, старший OK валиден
    var fulls = new[]
    {
        Full("broken", Now.AddDays(-1), "not-a-wal-name",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
        Full("ok", Now.AddDays(-2), "000000010000000000000005",
            new BackupVerify(BackupVerifyStatus.Ok, 1)),
    };

    // Act — выбор точки
    var start = RetentionPlanner.LatestVerifiedWalStart(fulls);

    // Assert — взят валидный OK ниже
    start!.Value.Name.Should().Be("000000010000000000000005");
}
```

- [ ] **Шаг 4.2: Прогон — RED**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RetentionPlannerTests.LatestVerifiedWalStart"
```
Ожидание: 4 FAIL (`RetentionPlanner` не содержит `LatestVerifiedWalStart`).

- [ ] **Шаг 4.3: Реализация (GREEN)**

В `src/PgWorker.Backups/Retention/RetentionPlanner.cs`, после `SelectKeep`:
```csharp
/// <summary>Точка отсчёта WAL-ретенции и контроля цепочки (t18, arch/19
/// §4 п.5/§3): wal_start новейшего (по started_unix) COMPLETED-полного с
/// verify.state=OK — ОДНА точка для cutoff-чистки WAL и chain_start.
/// verify-OK-полного нет (PENDING/FAILED/verify отсутствует) → null:
/// безопасной точки привязки нет, прунинг WAL запрещён. Чистая функция.
/// Дефективный wal_start кандидата (ранний FAILED-хвост/ручная правка)
/// пропускается — берётся следующий OK ниже.</summary>
public static WalFileName? LatestVerifiedWalStart(IReadOnlyList<FullBackupState> fulls)
    => fulls
        .Where(f => f.State == FullBackupStatus.Completed
                    && f.Verify is { State: BackupVerifyStatus.Ok })
        .Select(f => (f.StartedUnix, Start: WalFileName.TryParse(f.WalStartSegment ?? "")))
        .Where(x => x.Start is not null)
        .OrderByDescending(x => x.StartedUnix)
        .Select(x => x.Start)
        .Cast<WalFileName?>()
        .FirstOrDefault();
```

- [ ] **Шаг 4.4: Прогон всего класса — GREEN, commit**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RetentionPlannerTests"
```
Ожидание: PASS. Commit:
```bash
git add src/PgWorker.Backups/Retention/RetentionPlanner.cs src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs
git commit -m "feat(t18): LatestVerifiedWalStart — общая точка отсчёта cutoff/chain_start: wal_start новейшего verify-OK полного, гвард «нет OK → null» (spec §5.2 п.4)"
```

---

### Задача 5: WalChain.RatchetedStart (кандидат-макс) + chain_start в WalStreamProcess

**Вход:** задача 4 закоммичена (`LatestVerifiedWalStart` доступен).

**Действие (файлы):**
- Test: `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs` (раздел `RatchetedStart`, строки ~422–462: заменить 3 теста, добавить 4-й).
- Modify: `src/PgWorker.Backups/Model/WalChain.cs` (`RatchetedStart`, строки ~17–31), `src/PgWorker.Backups/WalStreamProcess.cs` (`ControlDueAsync`, строки ~510–526).

**Выход:** контроль цепочки стартует от wal_start новейшего verify-OK полного с ratchet-гвардом «не понижается»; полных/OK-полных нет — от записанной точки либо min-объекта (как сейчас).

**Интерфейс (Produces):** `public static WalFileName? RatchetedStart(WalFileName? recorded, WalFileName? candidate)` — сигнатура меняется (был список строк-стартов); единственный вызов вне тестов — `WalStreamProcess` (задача 5 правит его же).

**Проверка:** `WalChainTests` зелёный; интеграционные `WalStreamProcessTests` временно НЕ прогоняются (они актуализируются в задаче 7) — после правки WalStreamProcess обязательна сборка `dotnet build src/PgWorker.slnx -c Release` (0/0), чтобы поймать прочие вызовы, если найдутся.

**Связь со spec:** §5.2 п. 3, §5.1 п. 3 (канон §3), AC4 («контроль стартует от wal_start новейшего verify-OK полного; ratchet не понижается»).

- [ ] **Шаг 5.1: Новые тесты RatchetedStart (RED)**

Заменить в `WalChainTests` три существующих теста (`RatchetedStart_ПолныеНижеГраницы_Игнорируются`, `RatchetedStart_НетКандидатов_ВозвращаетЗаписанную`, `RatchetedStart_БезЗаписи_MinПолных`) на:
```csharp
// ---- t18: RatchetedStart — одиночный кандидат (новейший verify-OK полный) ----

// AAA (t18): кандидат ≥ записанной границы — контроль от кандидата
// (новый OK-полный закрывает разрыв/поднимает точку).
[Fact]
public void RatchetedStart_кандидат_выше_границы_принят()
{
    // Arrange — записанная граница ..05; кандидат (новейший OK-полный) ..09
    var recorded = WalFileName.TryParse("000000010000000000000005");
    var candidate = WalFileName.TryParse("000000010000000000000009");

    // Act
    var start = WalChain.RatchetedStart(recorded, candidate);

    // Assert — контроль от ..09
    start!.Value.Name.Should().Be("000000010000000000000009");
}

// AAA (t18): ratchet — кандидат НИЖЕ границы не понижает точку: verify-
// перепроверка старшего OK→FAILED откатила кандидата — контроль остаётся
// на прежней границе (полные ниже границы разрыва игнорируются).
[Fact]
public void RatchetedStart_кандидат_ниже_границы_не_понижает()
{
    // Arrange — записанная граница ..05; кандидат ..03
    var recorded = WalFileName.TryParse("000000010000000000000005");
    var candidate = WalFileName.TryParse("000000010000000000000003");

    // Act
    var start = WalChain.RatchetedStart(recorded, candidate);

    // Assert — точка не понижена
    start.Should().Be(recorded);
}

// AAA (t18): записи нет — контроль от кандидата (первый OK-полный задаёт
// стартовую точку; полных нет — остаётся min-объект потока у вызова).
[Fact]
public void RatchetedStart_без_записи_кандидат()
{
    // Arrange — записи нет; кандидат ..03
    var candidate = WalFileName.TryParse("000000010000000000000003");

    // Act
    var start = WalChain.RatchetedStart(null, candidate);

    // Assert — старт от кандидата
    start!.Value.Name.Should().Be("000000010000000000000003");
}

// AAA (t18): кандидата нет (verify-OK-полных нет) — записанная точка
// держится; нет и записи — null (контролю остаётся min-объект).
[Fact]
public void RatchetedStart_нет_кандидата_записанная()
{
    // Arrange — записанная ..05; кандидата нет
    var recorded = WalFileName.TryParse("000000010000000000000005");

    // Act
    var onlyRecorded = WalChain.RatchetedStart(recorded, null);
    var neither = WalChain.RatchetedStart(null, null);

    // Assert — записанная точка / null
    onlyRecorded.Should().Be(recorded);
    neither.Should().BeNull();
}
```

- [ ] **Шаг 5.2: Прогон — RED**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~WalChainTests"
```
Ожидание: 4 новых FAIL (сигнатура не совпадает / поведение min).

- [ ] **Шаг 5.3: Реализация RatchetedStart + правка WalStreamProcess (GREEN)**

`src/PgWorker.Backups/Model/WalChain.cs` — заменить метод (строки ~17–31):
```csharp
/// <summary>Стартовая точка контроля цепочки с ratchet (t18, arch/19 §3):
/// кандидат — wal_start новейшего verify-OK полного
/// (RetentionPlanner.LatestVerifiedWalStart); ratchet — точка НИКОГДА не
/// понижается ниже записанной границы разрыва (кандидаты ниже игнорируются:
/// их цепь может быть цела — дыра выше). Кандидата нет → записанная точка.
/// Чистая функция.</summary>
public static WalFileName? RatchetedStart(WalFileName? recorded, WalFileName? candidate)
    => candidate is { } start
       && (recorded is null
           || string.CompareOrdinal(start.Name, recorded.Value.Name) >= 0)
        ? start
        : recorded;
```
`src/PgWorker.Backups/WalStreamProcess.cs` — заменить строки ~510–518:
```csharp
        // chain_start (t18, arch/19 §3): wal_start новейшего (по started_unix)
        // COMPLETED-полного с verify.state=OK — ОДНА точка с cutoff-ретенции
        // (§4 п.5; расщепление точек давало бы ложный BROKEN после среза WAL);
        // ratchet — не понижается ниже записанной границы; нет OK-полного →
        // записанная ?? min-объект потока (как раньше при полных нет).
        var ratchet = wal is { ChainStartSegment.Length: > 0 }
            ? WalFileName.TryParse(wal.ChainStartSegment) : null;
        WalFileName? fromFull = WalChain.RatchetedStart(ratchet,
            RetentionPlanner.LatestVerifiedWalStart(shardBackups?.Full ?? []));
```
(`minObject` ниже по коду и `var chainStart = fromFull ?? minObject;` — БЕЗ изменений.)

- [ ] **Шаг 5.4: Сборка 0/0 + прогон WalChainTests — GREEN, commit**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~WalChainTests"
```
Ожидание: build 0 warn/0 err; тесты PASS. Commit:
```bash
git add src/PgWorker.Backups/Model/WalChain.cs src/PgWorker.Backups/WalStreamProcess.cs src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs
git commit -m "feat(t18): chain_start контроля = wal_start новейшего verify-OK полного (одна точка с cutoff, ratchet не понижается) — ложный BROKEN после среза WAL исключён (spec §5.2 п.3, AC4)"
```
ВНИМАНИЕ: интеграционные `WalStreamProcessTests`/`BackupSelfHealTests` в этом состоянии могут быть красными (полные в фикстурах без verify) — это ОЖИДАЕМО, актуализация в задаче 7; юнит-гейт задачи — зелёный.

---

### Задача 6: RetentionProcess шаг 4 — cutoff по последнему verify-OK (гвард прунинга)

**Вход:** задача 4 закоммичена; сборка зелёная.

**Действие (файлы):**
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RetentionProcessTests.cs` (актуализация 2 тестов + 4 новых; хелпер `Options` — дефолт параметров 7/4/6 → 1/1/1).
- Modify: `src/PgWorker.Backups/Retention/RetentionProcess.cs` (шаг (4), строки ~128–156).

**Выход:** ретенционный проход чистит WAL строго ниже wal_start новейшего verify-OK полного; OK-полного нет — шаг no-op. GFS-отбор и DELETING-доводка не тронуты.

**Проверка:** `RetentionProcessTests` целиком зелёный (реальный etcd + FakeBackupS3).

**Связь со spec:** §5.2 п. 2, §5.1 п. 2/п. 4, AC2, AC3.

- [ ] **Шаг 6.1: Актуализировать хелпер Options и тесты WAL-чистки под verify=OK (RED)**

В `RetentionProcessTests`: хелпер `Options(...)` — дефолты параметров `policyDays = 7, policyWeeks = 4, policyMonths = 6` заменить на `policyDays = 1, policyWeeks = 1, policyMonths = 1` (фикстура отражает новый прод-дефолт; тесты с явными значениями не трогать). Комментарий теста `Policy_из_ключа_кластера_действует`: «дефолт конфига 7/4/6» → «дефолт конфига 1/1/1» (сам тест остаётся валидным: policy 1/0/0 замещает дефолт, полный удаляется и при 1/1/1 — месячная точка не удерживает 40-дневный полный, он вне «предыдущего месяца»).

В тестах `WAL_чистка_ниже_cutoff` (кластер rt5) и `Чистка_WAL_поднимает_chain_start_контроля_t03` (кластер rt6) — сид полного дополнить verify OK (иначе после правки cutoff-точки нет и чистка no-op):
```csharp
// rt5: полный с verify OK — cutoff определён
var full = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000005",
    verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now)));
```
(в rt6 — та же правка для `full`; ассерты не меняются: cutoff ..05 остаётся точкой чистки/контроля.)

- [ ] **Шаг 6.2: Добавить 4 новых теста гварда (ещё RED)**

```csharp
// ---- t18: cutoff по последнему verify-OK (AC2/AC3) ----

// AC3: последний полный PENDING — прунинг WAL ниже его wal_start запрещён:
// cutoff держится на предыдущем OK (удаляется только строго ниже OK-точки).
[Fact]
public async Task WAL_чистка_гвард_последний_PENDING_держит_предыдущий_OK()
{
    // Arrange — old OK wal_start=..03 (8 дней назад), новый PENDING wal_start=..06
    // (вчера); объекты ..01.. ..08
    var ct = TestContext.Current.CancellationToken;
    const string cluster = "rt16";
    await SeedAsync(cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var okOld = Full("20260901000000Z", Now.AddDays(-8), walStart: "000000010000000000000003",
        verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now)));
    var pendingNew = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000006",
        verify: new BackupVerify(BackupVerifyStatus.Pending, null));
    await SeedFullAsync(cluster, okOld);
    await SeedFullAsync(cluster, pendingNew);
    var s3 = new FakeBackupS3();
    for (var i = 1; i <= 8; i++)
        s3.PrefixObjects.Add(($"{cluster}/shard1/wal/0000000100000000000000{i:x2}", 16L));
    var process = BuildProcess(Options(), s3);

    // Act — тик ретенции
    (await process.TickAsync(BuildSnap(cluster), backups(cluster, okOld, pendingNew), ct))
        .IsSuccess.Should().BeTrue();

    // Assert — срез строго ниже ..03 (точка предыдущего OK); WAL между ..03 и
    // ..06 (ниже непроверенного полного) жив
    s3.DeletedKeys.Should().BeEquivalentTo(
    [
        $"{cluster}/shard1/wal/000000010000000000000001",
        $"{cluster}/shard1/wal/000000010000000000000002",
    ]);
}

// AC3: последний полный verify=FAILED — гвард симметричен PENDING.
[Fact]
public async Task WAL_чистка_гвард_последний_FAILED_держит_предыдущий_OK()
{
    // Arrange — как предыдущий тест, но новый полный verify=FAILED
    var ct = TestContext.Current.CancellationToken;
    const string cluster = "rt17";
    await SeedAsync(cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var okOld = Full("20260901000000Z", Now.AddDays(-8), walStart: "000000010000000000000003",
        verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now)));
    var failedNew = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000006",
        verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now)));
    await SeedFullAsync(cluster, okOld);
    await SeedFullAsync(cluster, failedNew);
    var s3 = new FakeBackupS3();
    for (var i = 1; i <= 8; i++)
        s3.PrefixObjects.Add(($"{cluster}/shard1/wal/0000000100000000000000{i:x2}", 16L));
    var process = BuildProcess(Options(), s3);

    // Act — тик ретенции
    (await process.TickAsync(BuildSnap(cluster), backups(cluster, okOld, failedNew), ct))
        .IsSuccess.Should().BeTrue();

    // Assert — cutoff на ..03, WAL ..04+ (вкл. ниже FAILED-полного) жив
    s3.DeletedKeys.Should().BeEquivalentTo(
    [
        $"{cluster}/shard1/wal/000000010000000000000001",
        $"{cluster}/shard1/wal/000000010000000000000002",
    ]);
}

// AC3-хвост: ни одного verify-OK полного — WAL-чистка no-op (полный есть,
// но непроверенный: точки привязки нет).
[Fact]
public async Task WAL_чистка_нет_OK_полных_no_op()
{
    // Arrange — единственный COMPLETED без verify вовсе; объекты есть
    var ct = TestContext.Current.CancellationToken;
    const string cluster = "rt18";
    await SeedAsync(cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var pending = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000005",
        verify: new BackupVerify(BackupVerifyStatus.Pending, null));
    await SeedFullAsync(cluster, pending);
    var s3 = new FakeBackupS3();
    for (var i = 1; i <= 4; i++)
        s3.PrefixObjects.Add(($"{cluster}/shard1/wal/0000000100000000000000{i:x2}", 16L));
    var process = BuildProcess(Options(), s3);

    // Act — тик ретенции
    (await process.TickAsync(BuildSnap(cluster), backups(cluster, pending), ct))
        .IsSuccess.Should().BeTrue();

    // Assert — ничего не удалено (гвард: прунинг без успешного verify запрещён)
    s3.DeletedKeys.Should().BeEmpty();
}

// AC2-хвост: недельная/месячная точки на cutoff НЕ влияют — cutoff = wal_start
// последнего OK-полного; срез идёт ниже НЕГО (вкл. сегменты выше старых точек).
[Fact]
public async Task WAL_чистка_недельная_месячная_точки_не_влияют_на_cutoff()
{
    // Arrange — GFS-точка августа OK wal_start=..03 (31.08 — последний W36 и
    // последний августа: удерживается и недельной, и месячной гранулой 1/1/1)
    // + последний OK wal_start=..08 (вчера); объекты ..01.. ..0A
    var ct = TestContext.Current.CancellationToken;
    const string cluster = "rt19";
    await SeedAsync(cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var monthly = Full("20260831000000Z", new DateTimeOffset(2026, 8, 31, 10, 0, 0, TimeSpan.Zero),
        walStart: "000000010000000000000003",
        verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now)));
    var latest = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000008",
        verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now)));
    await SeedFullAsync(cluster, monthly);
    await SeedFullAsync(cluster, latest);
    var s3 = new FakeBackupS3();
    for (var i = 1; i <= 10; i++)
        s3.PrefixObjects.Add(($"{cluster}/shard1/wal/0000000100000000000000{i:x2}", 16L));
    var process = BuildProcess(Options(), s3);

    // Act — тик ретенции
    (await process.TickAsync(BuildSnap(cluster), backups(cluster, monthly, latest), ct))
        .IsSuccess.Should().BeTrue();

    // Assert — удалено строго ниже ..08 (вкл. ..03.. ..07 — «выше» месячной
    // точки, но ниже последнего полного); ..08+ живы
    s3.DeletedKeys.Should().BeEquivalentTo(
        Enumerable.Range(1, 7)
            .Select(i => $"{cluster}/shard1/wal/0000000100000000000000{i:x2}"));
    s3.PrefixObjects.Select(o => o.Key).Should().BeEquivalentTo(
    [
        $"{cluster}/shard1/wal/000000010000000000000008",
        $"{cluster}/shard1/wal/000000010000000000000009",
        $"{cluster}/shard1/wal/00000001000000000000000A",
    ]);
}
```
(числа кластеров rt16–rt19 — продолжение нумерации файла.)

- [ ] **Шаг 6.3: Прогон — RED**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RetentionProcessTests" 
```
Ожидание: 2 из 4 новых (rt18, rt19) — FAIL; rt16/rt17 и rt5/rt6 — ЗЕЛЁНЫЕ и на старом коде (cutoff-точка `..03` совпадает: min оставляемых = единственный verify-OK; в rt16/rt17 старый код даёт min(..03, ..06)=..03 — удаления [..01, ..02] совпадают с ассертом). Дискриминаторы старой семантики — rt18 (старый код с cutoff `..05` удалит ..01..04 против ожидания «пусто») и rt19 (старый min(..03, ..08)=..03 даст 2 удаления против ожидаемых 7). rt16/rt17 выполняют роль регрессионных гвардов AC3, шаг 6.4 обязателен независимо от их зелёного цвета. После прогона — контроль зачистки docker (etcd-контейнеры testcontainers подбирает ryuk: `docker ps -a --format '{{.Names}}' | grep -c '^pgw-'` → 0).

- [ ] **Шаг 6.4: Реализация шага (4) (GREEN)**

`src/PgWorker.Backups/Retention/RetentionProcess.cs` — заменить строки ~128–156:
```csharp
        // (4) Чистка WAL (t18, arch/19 §4 п.5): cutoff = wal_start новейшего
        // (по started_unix) verify-OK COMPLETED-полного из оставляемых — ОДНА
        // точка с chain_start контроля (§3). ГВАРД verify: OK-полного нет
        // (PENDING/FAILED/отсутствует) → шаг no-op — прунинг без успешного
        // verify нового полного запрещён; недельная/месячная точки на cutoff
        // не влияют.
        var remaining = fulls.Where(f =>
            f.State == FullBackupStatus.Completed
            && !(selection.Delete.Count > 0 && f.Id == selection.Delete[0])).ToList();
        if (RetentionPlanner.LatestVerifiedWalStart(remaining) is { } cutoff)
        {
            var listed = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/", ct: ct);
            if (!listed.IsSuccess)
                throw new ApplicationException($"list wal: {listed.Error!.Message}");
            var doomed = RetentionPlanner.SelectWalForDeletion(
                listed.Value.Select(o => o.Key.Split('/')[^1]).ToList(), cutoff);
            if (doomed.Count > 0)
            {
                var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
                var deleted = await s3.DeleteKeysAsync(keys, ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete wal: {deleted.Error!.Message}");
                await journal.WritePhaseAsync(cluster, Op, $"wal-trimmed/{shard}/{doomed.Count}",
                    claims.InstanceId, null, ct);
            }
        }
```

- [ ] **Шаг 6.5: Прогон — GREEN, commit**

Команда шага 6.3; ожидание: все `RetentionProcessTests` PASS (вкл. rt7 «WAL_без_полных_не_чистится» — остаётся валидным). Контроль зачистки docker. Commit:
```bash
git add src/PgWorker.Backups/Retention/RetentionProcess.cs src/tests/PgWorker.IntegrationTests/Backups/RetentionProcessTests.cs
git commit -m "feat(t18): WAL-cutoff ретенции = wal_start новейшего verify-OK полного, гвард «без успешного verify — не срезать» (no-op без OK-полных); недельная/месячная точки на cutoff не влияют (spec §5.2 п.2, AC2/AC3)"
```

---

### Задача 7: Актуализация WalStream/SelfHeal фикстур + интеграционные AC4-тесты контроля

**Вход:** задачи 5–6 закоммичены (известно, какие интеграционные тесты красные — полные в фикстурах без verify больше не якорят chain_start).

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (хелпер `FullShard`, строки ~85–89; 2 новых теста), `src/tests/PgWorker.IntegrationTests/Backups/BackupSelfHealTests.cs` (хелпер `FullShard`, строки ~91–94; запись `recompleted`, строки ~165–167).

**Выход:** контроль цепочки сквозно (ретенция срезает → контроль от cutoff-точки — ACTIVE, без ложного BROKEN); ratchet не понижается; сквозной self-heal тест зелёный на новой семантике.

**Проверка:** `WalStreamProcessTests` + `BackupSelfHealTests` зелёные.

**Связь со spec:** §5.3 («после среза WAL ниже последнего verify-OK полного контроль стартует от него — ложный BROKEN отсутствует; ratchet не понижается»), AC4.

- [ ] **Шаг 7.1: Хелперы FullShard — verify OK по умолчанию**

`WalStreamProcessTests.FullShard` (строки ~85–89) заменить на:
```csharp
    // Полный COMPLETED с wal_start_segment (для chain_start от полного);
    // t18: якорь chain_start — только verify-OK полные, поэтому по умолчанию
    // полный валиден (Ok), не-OK задаётся явно параметром.
    private static ShardBackups FullShard(
            string walStart, WalStreamState? wal = null, BackupVerify? verify = null) => new(
        [new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, walStart, 1024, null,
            verify ?? new BackupVerify(BackupVerifyStatus.Ok, 1757500400))],
        wal);
```
`BackupSelfHealTests.FullShard` (строки ~91–94) — та же правка. В `BackupSelfHealTests` «Дыра_BROKEN_пересъём_и_заживление_сквозным_циклом»: `recompleted` (строки ~165–167) дополнить verify OK (пересъём заживляет цепочку только после успешного verify — новое поведение):
```csharp
        var recompleted = new FullBackupState(newId, FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757501000, 1757501300, "000000010000000000000005",
            2048, null, new BackupVerify(BackupVerifyStatus.Ok, 1757501400));
```

- [ ] **Шаг 7.2: Два новых теста AC4 в WalStreamProcessTests**

```csharp
    // ---- t18 (AC4): контроль от wal_start новейшего verify-OK полного ----

    // AC4: после среза WAL ниже cutoff контроль стартует от wal_start
    // новейшего OK-полного — ложный BROKEN отсутствует. Дискриминация
    // старой min-семантики: от старого OK (..03) при объектах {5,6} контроль
    // дал бы дыру «ожидался ..04» и BROKEN.
    [Fact]
    public async Task Контроль_стартует_от_новейшего_OK_полного_без_ложного_BROKEN()
    {
        // Arrange — два OK-полных: старый wal_start=..03 (сегменты ..03/..04
        // срезаны ретенцией), новый wal_start=..05; S3: 5,6; ключа wal нет
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cv1");
        (await _claims.TryClaimClusterAsync("cv1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/6000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cv1", 5, 6);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var oldOk = FullShard("000000010000000000000003").Full[0];
        var newOk = oldOk with
        {
            Id = "20260911120000Z", StartedUnix = 1757600000, FinishedUnix = 1757600300,
            WalStartSegment = "000000010000000000000005",
        };
        var backups = new ClusterBackups("cv1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new([oldOk, newOk], null) });

        // Act — контроль due
        (await process.TickAsync(BuildSnap("cv1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — chain_start от ..05 (новейший OK), ACTIVE — не BROKEN
        var wal = await ReadWal("cv1");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
    }

    // AC4: ratchet — новый OK-полный со стартом НИЖЕ записанной границы
    // (verify-перепроверка старшего OK→FAILED откатила точку) не понижает
    // chain_start.
    [Fact]
    public async Task Ratchet_chain_start_не_понижается_младшим_OK_полным()
    {
        // Arrange — ключ ACTIVE с chain_start=..05; OK-полный wal_start=..03;
        // S3: 5,6,7 (непрерывны от ..05)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cv2");
        (await _claims.TryClaimClusterAsync("cv2", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/7000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cv2", 5, 7);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("cv2", "shard1", new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cv2_shard1", "shard1a",
            "000000010000000000000005", "000000010000000000000007",
            "000000010000000000000007", 1757500000, 0, null), ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var walKey = (await writer.ReadAsync("cv2", "shard1", ct)).Value;
        var backups = new ClusterBackups("cv2", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000003").Full, walKey),
            });

        // Act — контроль due
        (await process.TickAsync(BuildSnap("cv2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — chain_start остался ..05 (ratchet не понижается), ACTIVE
        var wal = await ReadWal("cv2");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
    }
```
ПРИМЕЧАНИЕ: точные сигнатуры `ReadWal`/`SeedSegments`/`BuildProcess` — из того же файла; при расхождении имён свериться с соседними тестами (`Контроль_сплошная_цепочка...`, `Контроль_новый_полный_выше_дыры...`).

- [ ] **Шаг 7.3: Прогон обеих серий — GREEN (актуализация по месту), зачистка, commit**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~WalStreamProcessTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~BackupSelfHealTests"
```
Ожидание: все PASS. Если отдельные тесты падают из-за семантики «полный без verify не якорит» (напр. тесты, где полный неявно считался валидным) — привести их фикстуры к явному verify (OK — валидный якорь, null/PENDING — проверяется min-объект/BROKEN-путь), НЕ меняя проверяемые инварианты; список затронутых тестов зафиксировать в сообщении коммита. После каждой серии — контроль зачистки docker. Commit:
```bash
git add src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/BackupSelfHealTests.cs
git commit -m "test(t18): фикстуры WalStream/SelfHeal — явный verify OK у якорных полных; AC4-тесты: контроль от новейшего OK-полного без ложного BROKEN, ratchet не понижается (spec §5.3, AC4)"
```

---

### Задача 8: Итоговый гейт — сборка 0/0, полные прогоны, зачистка серий

**Вход:** задачи 1–7 закоммичены; рабочее дерево чистое.

**Действие:** контрольные прогоны (без новых правок кода — только фиксация результата; упавшее НЕ перезапускать без разбора логов).

**Выход:** зелёная не-E2E серия проекта на Release; чистые docker-хосты (0 остатков).

**Проверка:** команды ниже, все зелёные; сборка 0 warnings / 0 errors.

**Связь со spec:** §6 фаза 3 («юниты по §5.3; полный прогон не-E2E серии»), AC1–AC5 сквозным прогоном.

- [ ] **Шаг 8.1: Сборка Release 0/0**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```
Ожидание: `0 Warning(s)`, `0 Error(s)`.

- [ ] **Шаг 8.2: Юниты проекта целиком**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.UnitTests"
```
Ожидание: все PASS. Зачистка после серии: `docker ps -a --format '{{.Names}}' | grep -c '^pgw-'` → 0 новых.

- [ ] **Шаг 8.3: Интеграционная серия Backups (non-E2E, etcd-testcontainers)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.IntegrationTests.Backups"
```
Ожидание: все PASS (вкл. RestoreProcessTests/RestoreDrillProcessTests без правок механики — AC5). Зачистка серии (контроль docker-остатков/сетей).

- [ ] **Шаг 8.4: Полный не-E2E прогон проекта**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName!~E2e"
```
Ожидание: все PASS (docker-E2E классы не входят в фильтр; PGW_TEST_DOCKER не выставлен — docker-зависимые скипнутся сами; интеграционные серии API, вкл. `BackupsPolicyApiTests`, идут в этом прогоне). Дождаться финальной строки прогона; зачистка серии: контейнеры/сети/тома pgw-* → 0 остатков (правило `pg/AGENTS.md`).

- [ ] **Шаг 8.5: Фиксация итога гейта (без коммита, если не просят)**

Сводка «сборка 0/0; юниты N/N; Backups M/M; не-E2E K/K; зачистки 0 остатков» — в сообщение координатору (журнал задачи). Дополнительно только по требованию ревью (spec §6 фаза 3): кейс-маркер E2E по фильтру ретенции — `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` — не обязателен (PgWorker.Backups вне обязательного E2E-гейта AGENTS.md).

---

### Задача 9: Roadmap-гейт — снятие пункта t18 (ТОЛЬКО мерж-коммитом в main)

**Вход:** работа слита в `main` по ЯВНОЙ команде пользователя («мержи в main»); выполняется ТЕМ ЖЕ мерж-коммитом (правило `arch/roadmap/README.md` и `pg/AGENTS.md`; в feature-ветке НЕ выполнять).

**Действие (файлы):**
- Modify: `arch/roadmap/reliability.md` (пункт t18, строки ~62–64; диапазон в шапке, строка ~18), `arch/roadmap/reliability-report.md` (строка ~62 текст «Открытые разрывы», строка ~126 таблица).

**Выход:** упоминаний t18 в roadmap нет; история — в git и `docs/superpowers/2026-10-07-t18-incremental-backups/`.

**Проверка:** `grep -rn "t18" arch/roadmap/` → пусто; `grep -rn "t18" arch/ docs/ --include="*.md" | grep -v docs/superpowers` → пусто (упоминания слитой задачи вычищены из живых документов её мерж-коммитом, правило `pg/AGENTS.md`).

**Связь со spec:** §6 фаза 4, §7 (судьба roadmap-пункта), AC6 (вторая половина).

- [ ] **Шаг 9.1: reliability.md — удалить пункт t18 и поправить диапазон P4**

Удалить строки пункта:
```
- **`t18-incremental-backups`** — инкрементальные полные (PG17+
  `pg_incremental_backup`): суточные полные на больших базах — длинные
  окна, двойной расход staging, риск ENOSPC (arch/19 §10).
```
Строку шапки `P3 (t13–t17) — наблюдаемость, P4 (t18–t22) — долговременные улучшения.` заменить на `P3 (t13–t17) — наблюдаемость, P4 (t19–t22) — долговременные улучшения.`

- [ ] **Шаг 9.2: reliability-report.md — вычистить оба упоминания**

Строку ~62 «Открытые разрывы: бэкапы не шифрованы и в единственном хранилище (`t01`, `t03`); сокращение окна пересъёма — инкрементальные полные (`t18`) и автозакрытие потерянных слотов (`t19`).» заменить на «Открытые разрывы: бэкапы не шифрованы и в единственном хранилище (`t01`, `t03`); автозакрытие потерянных слотов (`t19`).». Из таблицы (~строка 126) удалить строку `| t18-incremental-backups | инкрементальные полные (PG17+) | P4 | D |`.

- [ ] **Шаг 9.3: grep-гейт + мерж-коммит**

```bash
grep -rn "t18" arch/roadmap/   # ожидание: пусто
```
Оба файла включить в мерж-коммит в `main` (текст сообщения — по ситуации мержа, с упоминанием «мерж-гейт трека reliability: пункт t18 снят из roadmap тем же коммитом»).

---

## Соответствие план ↔ spec (self-review)

| Spec | План |
|---|---|
| §5.1 пп. 1–5 (канон §3/§4/§5/§9 + следствие) | Задача 1 (шаги 1.1–1.8) |
| §5.2 п. 1 (дефолты Options) + дефолты API-хендлера policy («из коробки», §3/§8) | Задача 2 (шаги 2.1–2.4 — Options, шаги 2.5–2.7 — хендлер) |
| §5.2 п. 2 (cutoff+гвард RetentionProcess) | Задача 6 |
| §5.2 п. 3 (chain_start WalStreamProcess) | Задача 5 |
| §5.2 п. 4 (хелпер; SelectWalForDeletion без изменений) | Задача 4 (SelectWalForDeletion не трогаем) |
| §5.2 п. 5 (панель — правок нет) | Правок панели нет (проверено: панельных дефолтов ретенции нет, policy читается nullable) |
| §5.3 (юниты ретенции 1/1/1, cutoff-гварды, контроль цепочки, актуализация fixtures) | Задачи 3, 4, 6, 7 |
| §6 фазы 1–4 | Задачи 1 → (2–7) → 8 → 9 |
| §7 (roadmap-гейт) | Задача 9 |
| AC1 (1/1/1, три позиции) | Задачи 2, 3 |
| AC2 (cutoff по последнему verify-OK) | Задачи 4, 6 (rt19) |
| AC3 (гварды PENDING/FAILED/нет-OK) | Задачи 4, 6 (rt16–rt18) |
| AC4 (контроль от cutoff-точки, ratchet) | Задачи 5, 7 (cv1/cv2, rt6) |
| AC5 (restore-регресс без правок) | Задача 8 (шаг 8.3) |
| AC6 (канон + roadmap) | Задачи 1, 9 |

Открытое решение (помечено в Global Constraints): правка GFS-отбора под буквальный канон «ПРЕДЫДУЩИХ периодов» (задача 3) — вопрос пользователю задан, ответа нет; без правки AC1 недостижим, канон §4 п.1 формулирует именно так. При ином решении пользователя — вернуться к задаче 3 и канон-шагу 1.3.
