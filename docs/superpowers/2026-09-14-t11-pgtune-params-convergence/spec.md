# Spec: t11 — конвергенция pg-параметров работающих нод (PGTune без пересоздания)

Дата: 2026-09-14
Основание: roadmap `arch/roadmap/pgworker.md` (тег `t11-pgtune-params-convergence`); предыдущая фаза — `docs/superpowers/2026-09-12-pgtune-provision/spec.md` (§6: конвергенция — осознанный out of scope, дрейф конфига между нодами).
Канон: `arch/14-pgworker.md` (§2.1 PGTune/bootstrap, §5 C конвергенция DCS), `arch/12-bucket-pitfalls.md` (P15).

Решения пользователя (зафиксированы вопросами):
- **2026-09-14, рестарт-политика — вариант 1: только PATCH /config + pending_restart,
  БЕЗ авто-рестарта.** Воркер патчит DCS-конфиг; динамические параметры
  применяются Patroni сразу (reload), postmaster-параметры (`max_connections`,
  `shared_buffers`, …) Patroni помечает `pending_restart` — фактическое
  применение при ближайшем рестарте ноды (rebuild/эвакуация/оператор). Воркер
  НИКОГДА не инициирует рестарт PG и даунтайм (`POST /restart` воркером не
  используется). Doorman остаётся согласованным: фактический рестарт идёт через
  пересоздание контейнера, которое обновляет doorman-env (P15).
- **2026-09-14, неполная заявка — SKIP + warning, не фейл тика**: skip pg-части
  конвергенции при ЛЮБОЙ неполной заявке (ресурсов нет вовсе ИЛИ любой из
  `MemoryBytes`/`CpuCores` null); тайминги конвергируются как обычно.

## 1. Цель

Живой конфиг PostgreSQL нод работающих шардов автоматически выравнивается с
рассчитанным PGTune-набором (merge(PGTune ∪ канон) от АКТУАЛЬНЫХ заявок
`/service/<scope>/request_{cpu,mem}` и опций `PgWorker:Pgtune`) — расширением
существующей конвергенции DCS-конфига (t09, `NodeSupervisor.ConvergeDcsConfigAsync`)
с таймингов Patroni на `postgresql.parameters`. Без пересоздания нод.

Сегодня (после 2026-09-12-pgtune-provision) PGTune-параметры применяются только
при bootstrap контейнера (`SPILO_CONFIGURATION` → `bootstrap.dcs` — действует при
ПЕРВОЙ инициализации DCS-кластера scope). Смена заявок/опций у живого шарда
подхватывается лишь новым scope; ноды живого шарда продолжают работать на
прежнем конфиге — осознанный дрейф (спецификация 2026-09-12 §6, arch/14 §2.1).
t11 устраняет дрейф: желаемый набор становится единым источником и для
bootstrap (SPILO_CONFIGURATION), и для живого конфига (DCS `postgresql.parameters`
через PATCH /config Patroni-REST).

Инварианты (переносятся из 2026-09-12 без изменений):
- **P3** (`arch/12`): `wal_level=logical`, `max_wal_senders=10`,
  `max_replication_slots=10`, `sync_replication_slots`, `max_slot_wal_keep_size`,
  `wal_keep_size` — канон поверх PGTune всегда;
- **P15** (`arch/12`): `max_connections` — вход PGTune (`PgWorker:Pgtune:Connections`),
  doorman-бюджет = `max(10, max_connections − 5)` — обновляется при создании
  контейнера (env `DOORMAN_CONFIG`), воркер живые контейнеры не трогает.

## 2. Принципы

1. **Расширение существующего контура, не новый процесс.** Конвергенция — уже
   работающий шаг тика надзора (`NodeSupervisor.ConvergeDcsConfigAsync`, arch/14
   §5 C): GET /config первого канонического Patroni-узла шарда → патч при
   расхождении. t11 распространяет ту же схему на `postgresql.parameters`;
   новый процесс/цикл/etcd-ключи НЕ вводятся (etcd-контракт не меняется).
2. **Единый источник желаемого набора.** Merge(PGTune ∪ канон) в одном
   компоненте Core: и `SpiloEnvBuilder` (bootstrap YAML), и конвергенция
   (JSON-патч) строят набор из одной функции. Bootstrap и конвергенция не могут
   разойтись по определению.
3. **Конвергентно — мутаций нет** («не второй регулярный писатель», t09): патч
   строится ТОЛЬКО при расхождении живого `/config` с желаемым набором;
   стабильный кластер не патчится вовсе.
4. **Полная конвергенция DCS-параметров**: расхождение значения → обновление;
   параметр в DCS отсутствует → добавление; параметр в DCS есть, а в желаемом
   наборе нет (исчез из PGTune-вывода при смене заявок, добавлен в
   `ExcludeParams`) → удаление null-значением в патче (Patroni-семантика PATCH
   /config: `null` удаляет ключ). «Старое не живёт параллельно канону» (arch/14 §5 C).
5. **Postmaster — pending_restart, применение не наш акт** (решение пользователя
   2026-09-14): динамические параметры Patroni применяет сам (reload в пределах
   `loop_wait`); postmaster-параметры Patroni помечает `pending_restart=true`
   (GET /patroni) — применяются при ближайшем рестарте ноды. Воркер НЕ читает
   `pending_restart` и НЕ принимает на его основе решений; воркер НЕ вызывает
   `POST /restart`, `POST /switchover` в контексте конвергенции и никогда не
   инициирует рестарт PG/даунтайм.
6. **Один PATCH на тик на шард**: тайминги и параметры — в одном документе
   (`{"ttl":20,…,"postgresql":{"parameters":{…}}}`), как сегодня (минимальные
   мутации).
7. **Транзиент-толерантность как в t09**: недоступность GET/PATCH (рестарт ноды,
   мёртвый шард) — пропуск конвергенции этого тика без фейла; патч повторится
   следующим тиком.
8. **Отсутствие или неполнота заявки — skip конвергенции pg-параметров, не фейл
   тика** (решение пользователя 2026-09-14). Заявки `request_{cpu,mem}` обязательны
   по канону (arch/14 §2.1 п.4, панель пишет при создании шарда); их отсутствие
   или неполнота у шарда с dsn — аномалия данных. Skip срабатывает при ЛЮБОЙ
   неполной заявке: ресурсов нет вовсе (`resources is null`) ИЛИ любой из них
   null (`MemoryBytes`/`CpuCores` — `NodeResourcesParser.Parse("2", null)`
   возвращает частичный `NodeResources(CpuCores:2, MemoryBytes:null)`, НЕ null).
   В EnsureNode-путях неполная заявка фейлит тик исключением фабрики (там
   мутация обязательна: нельзя создать ноду без размера); в конвергенции
   мутация опциональна (пропуск = конфиг остаётся прежним, безопасно; выдумывать
   размер ноды по остаточному ресурсу нельзя) — pg-часть патча пропускается с
   warning-логом воркера, тайминги конвергируются как раньше. Аномалия не
   блокирует пробы/rebuild остального кластера.

## 3. Изменение канона (arch/) — до кода

- `arch/14-pgworker.md` §5 C, пункт «Конвергенция DCS-конфига (t09)»:
  дополнить — конвергенция охватывает не только тайминги, но и
  `postgresql.parameters`: желаемый набор = merge(PGTune ∪ канон) от актуальных
  заявок (пересчёт на каждый тик конвергенции, БЕЗ фиксации в etcd); патч
  обновляет расходящиеся, добавляет отсутствующие и удаляет лишние
  (null-патч); postmaster-параметры Patroni помечает `pending_restart` —
  применяются при ближайшем рестарте ноды (решение 2026-09-14: воркер никогда
  не инициирует рестарт PG); заявка отсутствует/неполна — skip pg-части с
  журналом.
- `arch/14-pgworker.md` §2.1, абзац «Параметры PG в bootstrap.dcs — PGTune»:
  заменить фразу «Конвергенция pg-параметров работающих нод — out of scope (…)
  конвергенция DCS на pg-параметры не распространяется» на описание t11:
  конвергенция РАСПРОСТРАНЕНА на pg-параметры (механизм §5 C); дрейф конфига
  между нодами шарда при изменении заявок устранён на уровне DCS
  (динамика — сразу, postmaster — pending_restart до ближайшего рестарта);
  doorman-бюджет обновляется при создании контейнера (P15 согласован:
  фактическое применение postmaster идёт через rebuild).
- `arch/12-bucket-pitfalls.md` (P15): без изменений — бюджет doorman остаётся
  вычисляемым от `max_connections` при создании контейнера.
- **etcd-контракт не меняется вообще** (новых ключей нет; панель не затрагивается).

## 4. Структура/компоненты

### 4.1. Единый желаемый набор — `src/PgWorker.Core/Tuning/PgParametersCanon.cs` (новое)

```
static class PgParametersCanon
{
    /// Merge(PGTune ∪ канон): вывод PgTune.Calculate в порядке §5.2 спецификации
    /// алгоритма (минус excludeParams — параметр отсутствует вовсе), затем канон
    /// P3/лог-блок поверх с перезаписью по имени без дубликатов (позиция первого
    /// вхождения сохраняется, новые ключи — в конец). Значения — СЫРЫЕ строки
    /// ("60", "2047MB", "on", "logical") без YAML/JSON-обвязки.
    static IReadOnlyList<(string Name, string RawValue)> Desired(
        PgTuneResult tuning, IReadOnlySet<string>? excludeParams);
}
```

- Массив `CanonParameters` (P3: `wal_level=logical`, `hot_standby`,
  `sync_replication_slots`, `max_slot_wal_keep_size`, `max_wal_senders`,
  `max_replication_slots`, `wal_keep_size`, `checkpoint_timeout` + лог-блок)
  переезжает сюда из `SpiloEnvBuilder` — со ЗНАЧЕНИЯМИ БЕЗ кавычек (сегодня
  кавычки вшиты в строки значений).
- `SpiloEnvBuilder.TunedParametersBlock` переключается на
  `PgParametersCanon.Desired`; YAML-цитирование — деталь сериализатора
  `SpiloEnvBuilder`: все значения в кавычках, КРОМЕ `wal_level` (исторический
  стиль raw-string). **Инвариант: генерируемый SPILO_CONFIGURATION
  байт-в-байт не меняется** — существующие тесты `NodeConfigBuildersTests`
  зелёные без правки expects.

### 4.2. Патч-билдер конвергенции — `src/PgWorker.Core/Templates/DcsConfigConvergence.cs` (новое)

```
static class DcsConfigConvergence
{
    /// Расхождение фактического динамического конфига (GET /config, JSON) с
    /// каноном → минимальный патч-документ для PATCH /config: тайминги Patroni
    /// (ttl/loop_wait/retry_timeout/synchronous_mode из PatroniTimings) +
    /// postgresql.parameters от desiredParameters. desiredParameters == null —
    /// патч только таймингов (нет/неполна заявка). Конвергентно → null.
    static string? DivergencePatch(
        string? configJson,
        IReadOnlyList<(string Name, string RawValue)>? desiredParameters);
}
```

- **Поглощает** логику `PatroniTimings.DivergencePatch` (t09): значения таймингов
  по-прежнему из `PatroniTimings` (единый канон), но построение патча — здесь.
  `PatroniTimings.DivergencePatch` удаляется; тесты `PatroniTimingsTests`
  мигрируют на новый API (смысл/имена кейсов Regression_T09_* сохраняются).
- Сравнение параметров (живой `postgresql.parameters` vs желаемый набор):
  - живое значение нормализуется к строке: JSON-строка → как есть, число →
    инвариантная `GetRawText()` (`60` → `"60"`), bool → `"true"`/`"false"`,
    null/объект/массив — «расхождение» (в патч);
  - сравнение строковое (Ordinal); расхождение или отсутствие ключа в живом
    конфиге → `"name":"value"` в патч (значение — JSON-строкой ВСЕГДА, включая
    `wal_level`);
  - ключ в живом конфиге есть, в желаемом наборе нет → `"name":null` (удаление);
  - нормализация НЕ семантическая: `"2048MB"` ≠ `"2GB"` — наши источники пишут
    один формат (bootstrap из того же набора), самонормализация не нужна.
- Битый/чужой/null JSON → полный патч (все тайминги + весь желаемый набор) —
  безопасный исход t09: непонятный конфиг приводится к канону.
- Порядок ключей в патче: тайминги, затем параметры в порядке желаемого набора,
  удаления — в конце (стабильный, детерминированный документ; тестируемо).

### 4.3. Проводка — `NodeSupervisor.ConvergeDcsConfigAsync` (расширение)

Шаг тика надзора (конец тика, шаг 4; позиция и границы не меняются: шарды с
dsn, не в restore, гвард клэйма уже пройден) расширяется:

1. `probeNode` — как сегодня: первый канонический Patroni-узел шарда
   (`Ports.Patroni != 0 && Object is null`); нет — не наш домен (adoption-шарды
   без канонических Patroni-нод конвергенции не подлежат — граница t09
   сохраняется).
2. `GET /config` — как сегодня; транспорт/5xx → транзиент-skip тика.
3. Желаемый набор: `ReadShardResourcesAsync(cluster, shard)` → **ресурсы
   отсутствуют ИЛИ неполны** (`resources is null`, либо `MemoryBytes is null`,
   либо `CpuCores is null` — `NodeResourcesParser.Parse("2", null)` возвращает
   частичный объект, НЕ null) → **skip pg-части конвергенции** (warning-лог
   воркера «pgtune-конвергенция пропущена: заявка request_{cpu,mem}
   отсутствует/неполна»; патч таймингов — как раньше). **Полная заявка** →
   `pgtune.Create(resources)` → `PgParametersCanon.Desired(tuning,
   settings.ExcludeParams)`. `ExcludeParams` — из `PgtuneSettings` (единый
   источник с bootstrap; воркер получает settings конструкторно рядом с
   `PgtuneInputsFactory`). Прочие исключения `pgtune.Create` (не неполнота
   заявки — она отсечена проверкой выше) = фейл фазы тика, транзиент-ретрай —
   как в EnsureNode-путях.
4. `DcsConfigConvergence.DivergencePatch(config, desired)` → null — конец
   (конвергентно); иначе `PATCH /config` ОДНИМ документом (тайминги + параметры);
   не-2xx/транспорт → транзиент-skip тика.
5. Журнал: существующая фазовая запись `dcs-converge` расширяется — счётчики
   `updated/added/removed` параметров и пометка «postmaster-параметры →
   pending_restart (применение при ближайшем рестарте ноды)», когда патч
   затрагивает хотя бы одно postmaster-имя (список — статическая константа
   компонента, имена `pg_settings.context=postmaster`, реально встречающиеся в
   PGTune-выводе/каноне: `max_connections`, `shared_buffers`, `huge_pages`,
   `wal_buffers`, `max_worker_processes`, `max_parallel_workers`,
   `autovacuum_max_workers`, `autovacuum_work_mem`, `wal_level`,
   `max_wal_senders`, `max_replication_slots` — только для ТЕКСТА журнала;
   решения на этом списке не строятся).
6. `pending_restart` воркером НЕ читается; рестарт PG НЕ инициируется (решение
   пользователя). `ShardProbe` НЕ расширяется (GET/PATCH /config уже есть).

### 4.4. Тесты

- **Юнит `PgParametersCanonTests`** (`src/tests/PgWorker.UnitTests/Tuning/`, AAA):
  merge-набор = порядок §5.2 + канон поверх; перезапись канона (имя из
  PGTune-вывода получает каноническое значение); exclude вырезает; значения
  сырые (без кавычек); детерминизм.
- **Юнит `DcsConfigConvergenceTests`** (`src/tests/PgWorker.UnitTests/Templates/`):
  миграция кейсов t09 (дефолтный конфиг — полный патч таймингов; минимальный
  патч; канонический — null; битый/чужой — полный) + новые: расходящееся
  значение параметра → обновление; параметр отсутствует в живом → добавление;
  лишний в живом (нет в desired) → `"name":null`; число в живом (`"shared_buffers":2048`)
  vs строка `"2048"` в desired → конвергентно (нормализация); тайминги+параметры
  в одном документе; `desired == null` → патч только таймингов; self-check
  SingleCanonicalSource — JSON, собранный из SPILO_CONFIGURATION текущего
  билдера, конвергентен с desired (null-патч): bootstrap и конвергенция из
  одного источника, молодой кластер не патчится.
- **Юнит `NodeSupervisorTests`** (расширение, FakeEtcd + FakeHandler по портам —
  существующий паттерн): живой /config расходится → ровно один PATCH /config за
  тик (перехват запроса: метод/тело); конвергентный /config → PATCH нет; заявка
  НЕПОЛНАЯ (request_mem удалён при живом request_cpu —
  `NodeResources(CpuCores:2, MemoryBytes:null)`, НЕ null) → патч только
  таймингов + warning; обе заявки отсутствуют (`resources is null`) — тот же
  исход; /config недоступен → мутаций нет; фазовая запись dcs-converge при патче.
- **Интеграционные docker** (`src/tests/PgWorker.IntegrationTests/Docker/`,
  изоляция/teardown по `docs/e2e-isolation.md` — guid-теги во всех именах,
  динамические порты, own-only чистка, ассерт чистоты): поднять кластер →
  перезаписать `request_mem` в etcd (напр. 8Gi → 16Gi) → тик надзора →
  (а) GET /config несёт пересчитанные параметры (динамика + postmaster);
  (б) динамический параметр фактически применён живым PG
  (`SHOW effective_cache_size` / `current_setting` — reload в пределах
  loop_wait; ожидание ≤ BrokerBootSec с поллингом);
  (в) нода с изменённым postmaster-параметром — `pending_restart=true`
  (GET /patroni);
  (г) второй тик — мутаций нет (конвергентно);
  (д) возврат заявки 16Gi → 8Gi — патч вниз отрабатывает (уменьшение значений).
- **Docker-E2E на Release обязателен** (меняется `PgWorker.Provisioning`):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`.

## 5. Фазы

1. **Канон**: правки `arch/14-pgworker.md` §5 C + §2.1 (тем же коммитом, что код;
   по arch-first — пишутся первыми, коммитятся вместе).
2. **Желаемый набор**: `PgParametersCanon.Desired` + рефакторинг `SpiloEnvBuilder`
   на него (инвариант YAML) — TDD: сначала тесты слияния/инварианта.
3. **Патч-билдер**: `DcsConfigConvergence.DivergencePatch`, поглощение
   `PatroniTimings.DivergencePatch`, миграция `PatroniTimingsTests` — TDD.
4. **Проводка**: расширение `NodeSupervisor.ConvergeDcsConfigAsync` (расчёт,
   skip по неполной заявке, один PATCH, журнал) + юниты надзора.
5. **Интеграционные docker-сценарии** конвергенции (изменение заявки вверх/вниз,
  pending_restart, идемпотентность).
6. **E2E на Release** `Scale_AddEmptyShard`; зачистка контейнеров/сетей после
   каждой серии (`docker rm -f`, `docker network prune -f`).

## 6. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; новые компоненты
  Core — только BCL, без NuGet.
- **Без авто-рестарта (решение пользователя 2026-09-14)**: postmaster-параметры
  фактически применяются только при рестарте ноды (rebuild/эвакуация/оператор);
  до того кластер несёт `pending_restart=true` — это штатное состояние, не
  алерт воркера. Воркер не использует POST /restart и не инициирует switchover
  в контексте конвергенции.
- **Doorman живых нод не синхронизируется** (env контейнера иммутабен) — и не
  рассинхронизируется воркером: пока postmaster-параметры не применены, PG
  работает на прежнем `max_connections`, соответствующем текущему doorman-env;
  фактическое применение идёт через rebuild — пересоздание контейнера пишет и
  новый `DOORMAN_CONFIG` (P15 согласован). Единственный остаточный случай —
  операторский `docker restart` контейнера (env прежний) после уменьшения
  `max_connections`: doorman-бюджет ноды может превысить серверный лимит;
  известный дрейф, лечится пересозданием ноды (TO_RECREATE).
- **`postgresql.parameters` DCS — целиком домен воркера**: конвергенция удаляет
  любые ключи, отсутствующие в желаемом наборе (включая вручную записанные
  оператором в DCS — прямой принцип «старое не живёт параллельно канону»);
  канал переопределения параметров один — опции `PgWorker:Pgtune` +
  `ExcludeParams` (решение 2026-09-12 сохраняется).
- Усыновлённые шарды: конвергенция только при наличии канонического
  Patroni-узла (probeNode-граница t09); шарды в restore — skip (t05, без изменений).
- `ExcludeParams` действует симметрично: параметр в exclude не пишется в
  bootstrap и УДАЛЯЕТСЯ из живого DCS-конфига (null-патч).
- Валидация значений/имён — на стороне Patroni: если Patroni отвергает
  значение, патч будет повторяться каждым тиком с фазовой записью —
  диагностируемо (журнал), не блокирует остальные функции надзора.
- Сравнение значений нормализованное, но не семантическое (`2048MB` ≠ `2GB`,
  `on` ≠ `true`): источники нашего формата едины (bootstrap и патч из одного
  набора), посторонние форматы конвергируются первым патчем и далее стабильны.
- Изменение `PgtuneOptions` (Connections/DbType/…) подхватывается следующим
  тиком конвергенции у живых шардов (в отличие от 2026-09-12, где — только
  новым созданием контейнера).

## 7. Критерии приёмки

1. Изменение заявки `request_mem` живого шарда → следующий тик надзора патчит
   DCS-конфиг: `GET /config` несёт пересчитанные параметры; повторный тик
   мутаций не делает (идемпотентность, «не второй регулярный писатель»).
2. Динамические параметры фактически применены живым PG без рестарта
   (интеграционная проверка `current_setting`); postmaster-параметры помечены
   `pending_restart=true` (GET /patroni) и НЕ применены — рестарт воркером не
   инициируется.
3. Параметры, исчезнувшие из PGTune-вывода (смена заявок) или добавленные в
   `ExcludeParams`, удаляются из живого DCS-конфига null-патчем.
4. SPILO_CONFIGURATION байт-в-байт не изменился (существующие
   `NodeConfigBuildersTests` зелёные без правки expects); bootstrap и конвергенция
   строят параметры из одного источника (`PgParametersCanon.Desired`,
   self-check-тест).
5. Отсутствие ИЛИ неполнота заявки (любой из `request_{cpu,mem}` отсутствует/
   нечитаем при живом втором — `NodeResources(CpuCores, MemoryBytes:null)`, и
   полное отсутствие обеих) у шарда: pg-часть конвергенции пропущена
   (warning-лог), тайминги конвергируются, тик надзора НЕ фейлится.
6. Юнит/интеграционные зелёные; docker-E2E `Scale_AddEmptyShard` на свежем
   Release зелёный; teardown сценариев чист полностью (ассерт чистоты).
7. `arch/14-pgworker.md` (§2.1, §5 C) синхронизирован с кодом тем же коммитом.
