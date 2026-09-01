# Спецификация: ошибки проб и подключений кластеров Pg — настоящими алертами

Ветка: `fix-pg-probe-alerts` · Дата: 2026-09-01 · Фаза dev-flow: spec.
Канон контрактов уже обновлён этой задачей (arch-first, см. §7 «Изменения
arch/»): `arch/adminpanel/03-panels.md` §4 (каталог алертов — строка
`probe-failed`), `arch/adminpanel/01-architecture.md` §8 (сводка отказов).

## 0. Контекст (что сейчас и почему меняется)

Live-пробы панели (`src/AdminPanel.Probes`): Patroni REST `:8008` по каждому
члену matched HA-скопа и SQL через Npgsql по DSN шарда (`:5432`, один коннект
на шард). Ошибки проб **не теряются** — они попадают в `ProbeState` →
`snapshot.Probes`, отображаются в деталях (HA details — `probeError` tooltip,
Cluster details → Шарды — красная строка `runtime.error`). Но как **алерт**
ошибка сегодня почти невидима:

- `ProbeFailedRule` (`src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs`)
  создаёт алерт `probe-failed` с severity **info** на каждую упавшую пробу.
- Info-алерты **не попадают** ни в навигационные счётчики «Алерты»
  (`frontend/src/layout/AlertsNavCounters.tsx` — считает только
  critical/warning), ни в ленту Overview (`frontend/src/pages/OverviewPage.tsx`
  — фильтр `severity !== 'info'`). На странице Alerts они внизу таблицы.
- Итог: **лежащий Pg-кластер не виден как авария** — максимум тихая
  info-заметка (если зайти на страницу Alerts) плюс красные строки глубоко
  в деталях. Смежный `shard-no-master` (critical) срабатывает только по
  протухшему master-ключу в etcd: если master-lease жив (etcd-часть цела),
  а PG/Patroni недоступны — critical-алерта нет вовсе.

В каноне был конфликт: `arch/01` §8 обещал «алерт warning „probe failed“»,
каталог `arch/03` §4 говорил `info`; код следовал каталогу (комментарий в
`ProbeFailedRule`). Пользователь требует: любая ошибка пробы/подключения
кластера Pg — настоящий алерт; **неработающий кластер = critical**.

Шум, который нельзя игнорировать: SQL-проба ходит по **всем** кластерам
снапшота независимо от state (DSN пишется в декларацию при создании), поэтому
кластеры в `NOT_INITIALIZED` (ещё поднимаются) и `TO_REMOVE` (демонтируются)
дают постоянные `probe-failed` — чек стенда
`dev-stand/adminpanel/checks/40-live-probes.sh` п.3 прямо терпит их как info.
Простое повышение severity без подавления lifecycle-состояний превратит
каждое создание кластера в лавину critical.

Решения пользователя (зафиксированы, вопросы заданы по одному):
SQL-проба упала → **critical** сразу (без дебаунса); Patroni-проба одного
члена → **warning**, все члены скопа молчат → **critical**; все состояния —
единый kind `probe-failed` (без разделения на новые kind'ы).

## 1. Цель

1. **Ошибка SQL-пробы шарда = critical-алерт** `probe-failed:sql:<C>/<X>`:
   панель не смогла подключиться к Pg-шарду (все хосты DSN отказали или
   writable-мастер не найден) → кластер не работает → красный алерт,
   счётчик critical в навигации, верх ленты Overview.
2. **Ошибка Patroni-пробы одного члена = warning** `probe-failed:patroni:<scope>/<member>`
   (одна нода ≠ весь кластер).
3. **Весь matched-скоп молчит = один critical** `probe-failed:patroni-scope:<scope>`
   (Patroni-пробы всех членов скопа упали; per-member warning этого скопа
   при этом не эмитятся — один факт, один алерт).
4. **Без lifecycle-шума**: цели кластеров/шардов в `NOT_INITIALIZED` и
   `TO_REMOVE` не алертятся (прецедент — подавление `shard-no-leader`);
   пробы по ним продолжают ходить, runtime-ошибки остаются в деталях UI.
5. **Фронт не меняется**: severity-цвета, счётчики и лента уже существуют —
   алерты «проявляются» в них автоматически.

Не-цели: дебаунс/эскалация по числу последовательных фейлов (единичный сбой
уже алерт; возраст виден через `sinceUnix`), новые kind'ы алертов, kafka-пробы
(отдельный домен, свой движок), изменения самих проб (`ProbeOrchestrator`,
`SqlProbe`, `PatroniRestProbe`, HostMap), история алертов.

## 2. Принципы

- **arch-first**: каталог `arch/03` §4 и сводка `arch/01` §8 обновлены до
  кода (конфликт «info vs warning» снят в пользу новой семантики); код —
  отражение каталога.
- **Недоступность кластера = critical**: единственный факт «панель не может
  подключиться к шард/скопу» детектируется только пробами (etcd-часть может
  быть живой) — поэтому severity проб reflects доступность, а не «заметку».
- **Цели, а не результаты**: правило идёт от целей текущего снапшота
  (кластеры/шарды/скопы), результаты проб только lookup — исчезнувшая цель
  не алертится (симметрия `ProbeEnricher`: лишние ключи игнорируются).
- **Один факт — один алерт**: при полностью молчащем скопе не плодить
  N warning + 1 critical на одно и то же.
- **Lifecycle-подавление, а не отключение проб**: подавление только в
  правиле алертинга; пробы продолжают собирать данные для UI.
- **Стабильность id**: `id = kind:target` не меняется для существующих
  целей (`probe-failed:sql:<C>/<X>`, `probe-failed:patroni:<scope>/<member>`);
  новый id только у скопового critical (`probe-failed:patroni-scope:<scope>`)
  — механика `sinceUnix` движка не трогается.
- Пробы выключены (`PatroniEnabled`/`SqlEnabled`=false) → `probe-failed`
  не вычисляется (результатов нет) — как сегодня; «нет данных» ≠ «ошибка».
- .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`;
  панель — только docker; язык сообщений алертов — русский.

## 3. Структура и компоненты

### 3.1. Правило `ProbeFailedRule` (переписать)

`src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs` — единственный
меняемый компонент алертинга. Новая логика `Evaluate(snapshot, ctx)`:

1. **SQL-цели**: для каждого кластера `State == Active` → каждого шарда с
   `DsnHosts.Count > 0` и `State == Active` — поиск в `snapshot.Probes`
   результата `Kind == "sql"`, `Target == "<C>/<X>"`; найден и `!Ok` →
   алерт:
   - id `probe-failed:sql:<C>/<X>`, severity **Critical**;
   - message: `SQL-проба шарда <C>/<X> не удалась: <error>` (текст ошибки —
     из `ProbeResult.Error`);
   - details: `kind=sql`, `target`, `error`, `dsnHosts` (список хостов DSN —
     оператору сразу видно, куда панель не достучалась);
   - Hint: панель не смогла подключиться ни к одному хосту DSN шарда либо
     writable-мастер не найден — шард недоступен целиком: либо кластер
     лежит, либо недостижим из сети панели; SQL-живость — предусловие
     live-данных (слоты/лаги/инвентарь);
   - Remedy: `OperatorRunbook` («проверьте контейнеры нод шарда и Patroni
     скопа <C>-<X>, достижимость хостов из сети панели; панель ретраит
     пробу каждым тиком»).
2. **Patroni-цели**: для каждого `HaScope` с `Matched` и связанным кластером
   `State == Active` — сбор результатов `Kind == "patroni"` членов скопа
   (`Target == "<scope>/<member>"`):
   - результатов нет (проба выключена/тиков не было) → ничего;
   - есть хотя бы один и **все** `!Ok` → один алерт **Critical**
     id `probe-failed:patroni-scope:<scope>`, target `patroni-scope:<scope>`,
     message «Patroni-скоп <scope> недоступен целиком: <N>/<N> проб упали
     (<первая ошибка>)», details: `scope`, `failed`, `total`, `error`
     (первая ошибка), `cluster`/`shard` из матчинга скопа; Hint: ни один
     член скопа не ответил на REST :8008 — HA-кластер патрони невидим для
     панели (недоступен целиком или сетевая изоляция от панели);
     Remedy `OperatorRunbook`;
   - иначе — для каждого члена с упавшей пробой алерт **Warning**
     id `probe-failed:patroni:<scope>/<member>` (как сегодня по форме:
     message `проба patroni по <scope>/<member> не удалась: <error>`,
     details kind/target/error; Hint/Remedy — текущие).
3. Скопы, сматченные к кластерам `NOT_INITIALIZED`/`TO_REMOVE` (и шард-цели
   этих состояний), пропускаются — подавление по lifecycle (§1.4).

Каталог severity сводно: sql → critical; patroni одиночный → warning;
patroni весь скоп → critical (без per-member warning).

### 3.2. Компоненты, которые НЕ меняются

- `ProbeOrchestrator` / `SqlProbe` / `PatroniRestProbe` / `ProbeResultsStore`
  / `ProbeEnricher` — сбор проб и обогащение снапшота без изменений
  (в т.ч. пробы по NOT_INITIALIZED-целям продолжают ходить).
- `AlertEngine` / `AlertsOptions` — без изменений: новых порогов нет
  (состояния кластера — из снапшота, не из конфига); `sinceUnix`-механика
  id-стабильна.
- DTO `/api/alerts`, фронт (`AlertsPage`, `AlertsNavCounters`,
  `OverviewPage`, `AlertSeverityBadge`) — без изменений: critical/warning
  уже считаются и отображаются.
- Остальные правила (`shard-no-master`, `shard-no-leader`,
  `ha-member-not-streaming` и пр.) — без изменений; зона ответственности
  `probe-failed` прежняя («ошибка попытки пробы»), только severity
  становится честным.

### 3.3. Тесты

- **Unit** (`src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs`):
  переписать `ProbeFailed_EachFailedResult_Info` и смежные сценарии по AAA:
  - sql упала (Active-кластер) → один **critical** `probe-failed:sql:<C>/<X>`,
    details содержат `error` и `dsnHosts`;
  - patroni один член упал (второй ок) → один **warning**, id прежний;
  - patroni все члены скопа упали → один **critical**
    `probe-failed:patroni-scope:<scope>` и **ноль** per-member warning;
  - кластер `NOT_INITIALIZED` / `TO_REMOVE`, шард `TO_REMOVE` → алертов нет
    (при упавших пробах);
  - шард без DSN / скоп без членов / пробы выключены (Probes пуст) → ничего;
  - цель исчезла из снапшота, в Probes остался result → не алертится
    (правило идёт от целей).
  `AlertHintRemedyTests` — kind `probe-failed` остаётся в списке (Hint/Remedy
  не пустые).
- **Integration** (`src/tests/AdminPanel.IntegrationTests/InspectionProbeApiTests.cs`):
  `LiveEtcd_FailedProbe_ProducesProbeFailedAlert` — ожидание severity
  **warning** (одиночная patroni); добавить сценарий упавшей sql-пробы
  Active-шарда → severity **critical** в `/api/alerts`.
- **Стенд** (`dev-stand/adminpanel/checks/40-live-probes.sh` п.3): условие
  усиливается — `probe-failed` отсутствует **полностью** (неподнятые
  кластеры чека 15 теперь подавлены, живые цели — без ошибок проб);
  формулировку комментария обновить. Чек остановки кластера → критичный
  алерт — по желанию в живой верификации плана (основа — п.3 + ручная
  остановка контейнера).

### 3.4. Практики

`docs/adminpanel/03-probes-alerts.md`: таблица «AlertEngine — 25 правил»
(строка «пробы (1)») и грабли «`probe-failed` ≠ пустые данные» — дополнить
новой семантикой severity и lifecycle-подавлением.

## 4. Фазы

1. **arch** (сделано в spec-фазе): каталог arch/03 §4 + сводка arch/01 §8.
2. **Правило**: переписать `ProbeFailedRule` (§3.1).
3. **Тесты**: unit-сценарии §3.3 (сначала красные, потом зелёные — TDD),
   интеграционные правки.
4. **Стенд/практики**: чек 40-live-probes, docs/adminpanel/03.
5. **Верификация**: `dotnet build` + весь unit/integration-набор; живой
   прогон чеков стенда (20-alerts, 40-live-probes) — по решению плана.

## 5. Ограничения

- Контракт `/api/alerts` обратно совместим: kind, поля, id-формат
  существующих целей не меняются; меняется только severity и добавляется
  один новый id-шаблон (`probe-failed:patroni-scope:<scope>`).
- Никаких новых порогов/конфигов (`AlertsOptions` не трогается) — решение
  «Active или lifecycle» принимает правило по состоянию снапшота.
- Единичный упавший тик пробы уже алертит (без дебаунса) — осознанно:
   возраст алерта виден в `sinceUnix`, флапы — редкость на тике 15 c.
- Фронтенд не модифицируется (изменение поведения — только за счёт
  существующей severity-логики отображения).

## 6. Критерии приёмки

1. Active-кластер, SQL-проба шарда падает (PG остановлен/недоступен) →
   в `/api/alerts` появляется `probe-failed:sql:<C>/<X>` с
   `severity=critical`; счётчик critical у пункта «Алерты» в навигации и
   карточка/лента Overview отражают его (≤2 KV-тиков после падения пробы).
2. Остановлен один patroni-эмулятор члена скопа (второй жив) →
   `probe-failed:patroni:<scope>/<member>` с `severity=warning`; critical
   от скопа нет.
3. Остановлены все patroni-эмуляторы скопа Active-кластера → ровно один
   `probe-failed:patroni-scope:<scope>` с `severity=critical`, per-member
   warning этого скопа отсутствуют.
4. Кластер в `NOT_INITIALIZED` (поднятие) или `TO_REMOVE` (демонтаж), шард
   в `TO_REMOVE` — упавшие пробы этих целей **не** создают алертов; при
   этом `runtime.error`/`probeError` в деталях UI остаются (пробы ходят).
5. Пробы выключены (`PatroniEnabled=SqlEnabled=false`) → `probe-failed`
   не вычисляется.
6. Юнит-сценарии §3.3 зелёные; `HaAlertRulesTests.ProbeFailed_*` не
   содержит ожиданий info для Active-целей; интеграционный сценарий
   sql-critical зелёный.
7. Чек `40-live-probes.sh` п.3 в новой формулировке (probe-failed нет
   вовсе) зелёный на полном стенде; `20-alerts.sh` без регрессий.
8. `arch/03` §4, `arch/01` §8, `docs/adminpanel/03` синхронны коду
   (сделано arch-first; противоречий «info» больше нигде не осталось —
   включая комментарий в `ProbeFailedRule`).

## 7. Изменения arch/ (сделано в spec-фазе, arch-first)

- `arch/adminpanel/03-panels.md` §4: строка каталога `probe-failed` —
  новая семантика (sql→critical, patroni→warning, весь скоп→critical без
  per-member) + абзац о вычислении по целям Active-состояний и подавлении
  `NOT_INITIALIZED`/`TO_REMOVE`.
- `arch/adminpanel/01-architecture.md` §8: строки «Patroni REST недоступен»
  / «SQL-проба недоступна» — честные severity, ссылка на каталог 03 §4
  (конфликт «warning vs info» снят).
