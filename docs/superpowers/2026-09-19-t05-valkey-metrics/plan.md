# t05-valkey-metrics — план реализации (Фаза 3 dev-flow)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ САБ-СКИЛЛ: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять по задаче за раз. Шаги —
> чекбоксы (`- [ ]`) для трекинга. Перед исполнением прочитать правила:
> [`/Users/demakaev/ZCodeProject/pg/AGENTS.md`](../../../../pg/AGENTS.md) и
> [`/Users/demakaev/ZCodeProject/AGENTS.base.md`](../../../../AGENTS.base.md).

**Цель:** телеметрия Valkey-домена единым каркасом `Shared.Metrics`: коллектор доменных
метрик INFO (`ValkeyMetricsState` + `ValkeyMetricsCollector`, зеркало Kafka), словарь 15
серий в arch/18 §2.6, стенд (scrape-джоба, 3 алерта, дашборд `valkey.json`, чек 65).

**Архитектура:** структурное зеркало Kafka-коллектора без рефакторинга Shared.Metrics.
RESP-миниклиент воркера получает `InfoAllAsync` (команда `INFO all` одной пробой на ноду
за тик); hosted `BackgroundService` тикает `ValkeyWorker:Metrics:CollectIntervalSec`,
читает etcd read-only (`/valkey/clusters/` + `/valkeyworker/portalloc/`), пишет в
ObservableGauge-стейт. Все доменные серии — gauge (persistence off сбрасывает кумулятивы).

**Стек:** .NET 10, `System.Diagnostics.Metrics` + `OpenTelemetry.Exporter.Prometheus.AspNetCore`
(версии пин в `Directory.Packages.props`, новых пакетов НЕТ), xUnit v3 + FluentAssertions,
Testcontainers (etcd), docker (valkey/valkey:9.1.2 из локального registry).

**Спека:** `docs/superpowers/2026-09-19-t05-valkey-metrics/spec.md` (в этом же каталоге;
план аргументируется от спеки — исполнители читают обе).

## Глобальные ограничения (из spec + правил репо)

- Работа ТОЛЬКО в worktree `feat-t05-valkey-metrics` (текущая ветка); коммиты в ветке
  свободны, мерж в `main` — по гейту dev-flow (AGENTS.base §6).
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true` — код обязан компилироваться
  без предупреждений; централизованное версионирование (CPM) — пакеты НЕ добавляются.
- Идентификаторы — английские, комментарии/документация — русские (AGENTS.base §7);
  тесты — с комментариями по AAA (правило AGENTS.md пользователя).
- arch-first: задача 1 (канон) — ПЕРВЫМ коммитом, до кода (spec §6/§7 Ф1).
- Тесты: docker-порты только динамические (`FreePortWindow`/`assignRandomHostPort`),
  никаких хост-портовых литералов; бюджеты ожиданий ≤ 100 с; полный teardown при любом
  исходе + ассерт чистоты; зачистка docker-остатков (`vwk-*`, сети) после КАЖДОЙ серии
  прогонов (AGENTS.md).
- Код воркера меняется ТОЛЬКО новыми файлами `ValkeyMetrics*` + точечные вставки
  DI/опций в `Program.cs`/`Options.cs`/`appsettings.json` (spec §10.6).
- Поведение процессов A–E не меняется; сбор — read-only вне клэймов.
- Все команды ниже выполняются из корня worktree:
  `cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-valkey-metrics`.

## Договорённости плана (уточнения spec на уровне кода)

- **Roadmap-тег `t05-valkey-metrics` снимается мерж-коммитом в main** (правило
  AGENTS.md/AGENTS.base и spec §6.3/Ф6), НЕ задачей 1: в Ф1 roadmap не трогается
  (упоминание roadmap в spec §7 Ф1 покрывается шагом мерж-гейта, задача 8).
- Источник адресов нод — второй инжектируемый делегат `portAllocSnapshot`
  (Range `/valkeyworker/portalloc/` → `cluster → node → NodeAddress`), симметрично
  делегату `clustersSnapshot` Kafka-коллектора; оба собираются в `Program.cs`.
- `ValkeyNodeSample` — все числовые поля `long?` (единый тип измерений `Measurement<long>`):
  `null` = поле INFO отсутствует/нечислово → серия не эмитится (консервативно, spec §3).
- Фильтр кластеров: `Config is null || Config.State is not null || AdminUser is null ||
  AdminPassword is null → skip` (битый config-ключ консервативно пропускаем — не Active).
- Название.series в коде — dot-нотация Meter (`valkey.memory.used_bytes`), финальные
  имена (`valkey_memory_used_bytes`) фиксирует интеграционный тест (риск M3, задача 6).

---

### Задача 1. Канон arch/ (Ф1, arch-only коммит)

**Вход:** ветка `feat-t05-valkey-metrics` чистая, spec прочитан.
**Выход:** arch/18 §2.2/§2.6/§4/§5.2/§5.3/§6/§8 + arch/21 в ДВУХ местах (§7 — ссылка
на arch/18; блок «Границы» преамбулы — без пункта про коллектор) обновлены;
arch-only коммит.
**Проверка:** grep-ассерты шага 6 + `git diff --stat` только по двум файлам arch/.
**Связь со spec:** §6 п.1–2 (§6.2 — два места arch/21), §7 Ф1, §10.1.

**Files:**
- Modify: `arch/18-metrics.md` (§2.2 — строка процесс-словаря; новая §2.6 после §2.5;
  §4 — переименование + valkey-подраздел; §5.2 — строка таблицы джоб; §5.3 — дашборд;
  §6 — строки тестов; §8 — строка конфига)
- Modify: `arch/21-valkeyworker.md` (ДВА места: §7 «Наблюдаемость» — замена фразы;
  блок «Границы (что НЕ входит)» преамбулы, строки 30–35, — удаление пункта про
  коллектор. «Урок инцидента t05» в §2 (kfw-net, строка 125) — НЕ ТРОГАТЬ)

- [x] **Шаг 1. arch/18 §2.2 — процесс-словарь valkey одной строкой**

В `arch/18-metrics.md`, §2.2, абзац «`process` — фактическое `op` журнала работы…»,
после перечня KafkaWorker:

```
`regen`, `topicsync`. …
```

заменить фрагмент «у KafkaWorker — `provision`, `deprovision`, `add-broker`,
`remove-broker`, `reassign`, `rotate`, `regen`, `topicsync`.» на «у KafkaWorker —
`provision`, `deprovision`, `add-broker`, `remove-broker`, `reassign`, `rotate`,
`regen`, `topicsync`; у ValkeyWorker — `provision`, `deprovision`, `rotate`,
`supervise` (подавляемый), `healing-portalloc` (вспомогательный).» (остальной текст
абзаца не трогать; полный фактический словарь из spec §3/§6.1 — пять значений).

- [x] **Шаг 2. arch/18 §2.6 — новая подсекция словаря valkey-домена**

После §2.5 (перед `## 3.`) вставить:

```markdown
### 2.6. Valkey-домен (коллектор ValkeyWorker §4)

Имена — финальные Prometheus-формата; в коде — dot-нотация Meter. Лейблы конечны:
`cluster` (доменное имя), `node` (`node<k>`), `role` ∈ {master,slave} (только у
`valkey_role`); `job`/`instance` назначает Prometheus по scrape-джобе (§5.2).

| Имя | Тип | Лейблы | Источник INFO | Смысл |
|---|---|---|---|---|
| `valkey_memory_used_bytes` | gauge | cluster, node | Memory.used_memory | потребление памяти нодой, байты |
| `valkey_memory_max_bytes` | gauge | cluster, node | Memory.maxmemory | предел памяти ноды (maxmemory), байты |
| `valkey_connected_clients` | gauge | cluster, node | Clients.connected_clients | подключённые клиенты |
| `valkey_blocked_clients` | gauge | cluster, node | Clients.blocked_clients | заблокированные клиенты (BLPOP и т.п.) |
| `valkey_evicted_keys` | gauge | cluster, node | Stats.evicted_keys | кумулятив выселенных ключей (сброс при рестарте — gauge) |
| `valkey_expired_keys` | gauge | cluster, node | Stats.expired_keys | кумулятив истёкших ключей |
| `valkey_keyspace_hits` | gauge | cluster, node | Stats.keyspace_hits | кумулятив попаданий (hit-rate = hits/(hits+misses)) |
| `valkey_keyspace_misses` | gauge | cluster, node | Stats.keyspace_misses | кумулятив промахов |
| `valkey_instantaneous_ops_per_sec` | gauge | cluster, node | Stats.instantaneous_ops_per_sec | операции/сек (готовая скорость, без rate()) |
| `valkey_total_connections_received` | gauge | cluster, node | Stats.total_connections_received | кумулятив принятых соединений |
| `valkey_rejected_connections` | gauge | cluster, node | Stats.rejected_connections | кумулятив отклонённых соединений (maxclients) |
| `valkey_total_commands_processed` | gauge | cluster, node | Stats.total_commands_processed | кумулятив обработанных команд |
| `valkey_role` | gauge | cluster, node, role | Replication.role | 1 в серии с `role="master\|slave"` (enum-паттерн) |
| `valkey_connected_slaves` | gauge | cluster, node | Replication.connected_slaves | число реплик (v1 standalone — всегда 0; заготовка под будущие топологии) |
| `valkey_collector_last_success_timestamp_seconds` | gauge | — | самонаблюдение | unix-время последнего успешного тика коллектора |

Семантика gauge-кумулятивов: persistence off и пересоздания контейнера сбрасывают
счётчики Valkey в 0 — counter-тип ломал бы `increase()`/`rate()`; динамика в PromQL —
`rate()`/`increase()` по gauge-сериям (каноническое решение доменного словаря).
Поле INFO отсутствует/нечислово в ответе ноды → конкретная серия не эмитится
(консервативно, без нулей-фантомов); сам сбор кластера при этом успешен.
```

- [x] **Шаг 3. arch/18 §4 — переименование раздела + valkey-подраздел**

Заголовок `## 4. Коллектор Kafka-метрик` → `## 4. Коллекторы доменных метрик (Kafka, Valkey)`;
существующий текст раздела оформить подразделом: сразу после заголовка вставить строку
`### 4.1. Kafka` (текст без изменений). После него добавить:

```markdown
### 4.2. Valkey

Фоновый hosted-сервис `ValkeyWorker.App` (тик `ValkeyWorker:Metrics:
CollectIntervalSec`, default 30; `<=0` → 30 + warning-лог): по Active-кластерам
(снапшот etcd `/valkey/clusters/`, парсер ValkeySnapshotParser; `Config.State == null`
— невыполненные заявки не трогаем) с полными дискавери-кредами (`admin_user`/
`admin_password`). Адреса нод — отдельный read-only Range `/valkeyworker/portalloc/`
(канон `NodeAddress`); нода кластера без portalloc-записи — пропуск (лестница E9 —
забота надзора C, коллектор только читает). Хост пробы —
`AdvertisedClientHost ?? portalloc.host` (правило advertised arch/21 §2).

Сбор ноды — ОДНА RESP-проба `INFO all` за тик (аналог «одного AdminClient-коннекта
за тик» §4.1): все секции одним bulk-кадром, короткоживущий TCP с таймаутом клиента
(5 с), admin-кред. Ошибка ноды → тик кластера неуспешен (warning-лог), остальные
кластеры тика собираются. `valkey_collector_last_success_timestamp_seconds`
обновляется ТОЛЬКО при полном успехе всех (cluster, node)-проб тика (консервативно;
пустой список кластеров = успех). Сбор — вне клэймов (read-only, безопасен
параллельно любым процессам).

Без backoff-механизма (отличие от Kafka): RESP-миниклиент — лёгкая короткоживущая
TCP-проба (не тяжёлый AdminClient); лежачая нода стоит один connect-таймаут за тик,
отдельный backoff-стейт не нужен (YAGNI). Результаты пишутся в ObservableGauge-стейт
§2.6 (`ValkeyMetricsState`, материализация измерений под lock — паттерн §4.1);
`UpdateCluster` затирает предыдущие записи кластера — ушедшие ноды не копятся.
```

- [x] **Шаг 4. arch/18 §5.2/§5.3/§6/§8 — точечные строки**

- §5.2, таблица Job: после строки `kafkaworker` добавить

  ```
  | `valkeyworker` | имя сети стенда `valkeyworker:8080` (профиль `valkey`; хост-публикации нет — чеки ходят изнутри контейнера) | §2.1–2.2, §2.6 |
  ```

- §5.3, перечень дашбордов: после `dashboards/kafka.json` добавить «,
  `dashboards/valkey.json` (память/hit-rate/эвикции/ops/клиенты/подключения/
  slaves/коллектор)».
- §6, после Unit-строки добавить строку:

  ```
  - **Unit**: valkey-коллектор — парсер INFO, затирание ушедших нод
    UpdateCluster, консервативный LastSuccess, пропуски не-Active/без кредов/без
    portalloc, дефолт интервала `<=0` → 30 (фейк `IValkeyConnection`).
  ```

  и расширить Integration-строку: после «содержит
  канонические имена §2 (фиксирует фактические экспортированные имена против
  словаря)» дополнить «; valkey — все 15 имён §2.6 при живом демо-кластере (live-WAF
  с реальным docker), живая/остановленная нода — LastSuccess стоит, тик не падает».
- §8, после строки `KafkaWorker:Metrics { CollectIntervalSec=30 }` добавить:

  ```
  ValkeyWorker:Metrics { CollectIntervalSec=30 }          # коллектор §4.2
  ```

- [x] **Шаг 5. arch/21 — ДВА места упоминания t05-коллектора (spec §6.2)**

**(а) §7 «Наблюдаемость»:** заменить «коллектор доменных
метрик INFO и дашборд — t05 (вне t01).» на «коллектор доменных метрик INFO и
дашборд — [18-metrics.md](18-metrics.md) §4.2/§2.6.».

**(б) Преамбула, блок «Границы (что НЕ входит)» (строки 30–35):** удалить пункт
«коллектор доменных метрик INFO и
дашборд (t05, arch/18 §2.2/§4-паттерн)» — фраза spans строки 32–33. Было:

```
Границы (что НЕ входит): TLS клиентских подключений (`t06-valkey-tls` —
per-cluster CA, tls-port, ключ `ca_pem`); реплики/sentinel/cluster-топологии
(кеш восполним, шардирование не нужно); коллектор доменных метрик INFO и
дашборд (t05, arch/18 §2.2/§4-паттерн); панель valkey-домена (t03);
клиентская библиотека Puzzle (t04); persistence RDB/AOF — off по канону
(кеш восполним); квоты томов — томов нет.
```

стало (части сшиты; пункты про панель t03 и библиотеку Puzzle t04 — внешние
артефакты, остаются):

```
Границы (что НЕ входит): TLS клиентских подключений (`t06-valkey-tls` —
per-cluster CA, tls-port, ключ `ca_pem`); реплики/sentinel/cluster-топологии
(кеш восполним, шардирование не нужно); панель valkey-домена (t03);
клиентская библиотека Puzzle (t04); persistence RDB/AOF — off по канону
(кеш восполним); квоты томов — томов нет.
```

Обоснование (spec §6.2): после t05 коллектор — внутренний hosted-сервис
ValkeyWorker, утверждение «не входит» становится ложным, а канон не вправе
содержать ложь после задачи (arch/ — источник истины; прецедент t08); наблюдаемость
воркера уже описана в §7, дашборд — артефакт стека мониторинга arch/18 §5.

**НЕ ТРОГАТЬ:** упоминание «урок инцидента t05» в §2 (строка 125, kfw-net,
исчерпание подсетей docker) — это ДРУГОЙ t05 (нумерация инцидента мерж-гейта
2026-09-05), к тегу задачи `t05-valkey-metrics` отношения не имеет.

- [x] **Шаг 6. Проверка канона**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-valkey-metrics
# arch/18: словарь, коллектор, стенд, конфиг (два независимых ассерта — регистр!)
grep -c "2.6. Valkey-домен" arch/18-metrics.md            # → 1
grep -c "Коллекторы доменных метрик (Kafka, Valkey)" arch/18-metrics.md  # → 1
grep -c '\`valkeyworker\`' arch/18-metrics.md              # → 1 (§5.2, строка таблицы джоб)
grep -c "ValkeyWorker:Metrics { CollectIntervalSec=30 }" arch/18-metrics.md  # → 1 (§8)
grep -c "у ValkeyWorker — \`provision\`" arch/18-metrics.md # → 1
# arch/21: ДВА места чисты, инцидент kfw-net цел (§-скоуп, не глобальный grep):
grep -c "t05, arch/18" arch/21-valkeyworker.md             # → 0 (пункт «Границ» удалён)
grep -c "вне t01" arch/21-valkeyworker.md                  # → 0 (старая §7-фраза ушла)
grep -c "§4.2/§2.6" arch/21-valkeyworker.md                # → 1 (§7 ссылается на arch/18)
grep -c "урок инцидента t05" arch/21-valkeyworker.md       # → 1 (kfw-net §2 НЕ тронут)
grep -c "t05" arch/21-valkeyworker.md                      # → 1 (ровно одно — инцидент kfw-net)
git diff --stat                                             # только arch/18-metrics.md и arch/21-valkeyworker.md
```

- [x] **Шаг 7. Коммит**

```bash
git add arch/18-metrics.md arch/21-valkeyworker.md
git commit -m "docs(metrics): канон valkey-телеметрии t05 — arch/18 §2.6 словарь 15 серий + §4.2 коллектор + §5/§6/§8 стенд; arch/21 — §7 ссылка на arch/18 §4.2/§2.6, пункт коллектора снят из «Границ» (коллектор стал внутренним)"
```

---

### Задача 2. RESP: `InfoAllAsync` + парсер INFO (Ф2)

**Вход:** задача 1 закоммичена.
**Выход:** `IValkeyConnection.InfoAllAsync` + `ValkeyConnection.ParseInfo` (internal,
чистая функция) + дополненный `FakeValkeyConnection`; юнит-тесты зелёные.
**Проверка:** `dotnet test … ValkeyWorker.UnitTests` зелёный.
**Связь со spec:** §4.1, §7 Ф2, §9 (unit: парсер INFO).

**Files:**
- Modify: `src/ValkeyWorker.Core/Valkey/IValkeyConnection.cs` (новый метод интерфейса)
- Modify: `src/ValkeyWorker.Core/Valkey/ValkeyConnection.cs` (`InfoAllAsync`,
  `ProjectInfo`, `ParseInfo`)
- Modify: `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` (`FakeValkeyConnection`
  — реализация нового метода, компиляция фейка)
- Test: `src/tests/ValkeyWorker.UnitTests/Core/ValkeyConnectionTests.cs` (добавить тесты)

**Interfaces:**
- Produces: `Task<Result<IReadOnlyDictionary<string, string>>> IValkeyConnection.InfoAllAsync(ValkeyEndpoint ep, CancellationToken ct)`;
  `internal static IReadOnlyDictionary<string, string> ValkeyConnection.ParseInfo(string bulk)`.
  На эти имена опираются задачи 3–6.

- [x] **Шаг 1. Пишем падающие тесты парсера и команды (в конец `ValkeyConnectionTests.cs`)**

```csharp
// ── INFO all: парсер + команда (t05, arch/18 §4.2) ──

// AAA: заголовки секций «# Name», пустые строки и \r\n — парсер даёт плоский словарь.
[Fact]
public void ParseInfo_СекцииПустыеСтроки_ПлоскийСловарь()
{
    // Arrange: типовой фрагмент INFO all с тремя секциями.
    var bulk = "# Server\r\nredis_version:9.1.2\r\nredis_mode:standalone\r\n\r\n" +
               "# Memory\r\nused_memory:1048576\r\nmaxmemory:536870912\r\n\r\n" +
               "# Keyspace\r\n";

    // Act
    var info = ValkeyConnection.ParseInfo(bulk);

    // Assert: k:v-пары собраны; заголовки/пустые строки не попали.
    info["redis_version"].Should().Be("9.1.2");
    info["used_memory"].Should().Be("1048576");
    info["maxmemory"].Should().Be("536870912");
    info.Should().HaveCount(4);
    info.Should().NotContainKey("# Server");
}

// AAA: нечисловые значения хранятся строками (db0 из Keyspace) — выбор чисел за коллектором.
[Fact]
public void ParseInfo_НечисловыеЗначения_ХранятсяСтроками()
{
    // Arrange
    var bulk = "# Keyspace\r\ndb0:keys=3,expires=0,avg_ttl=0\r\nrole:master\r\n";

    // Act
    var info = ValkeyConnection.ParseInfo(bulk);

    // Assert
    info["db0"].Should().Be("keys=3,expires=0,avg_ttl=0");
    info["role"].Should().Be("master");
}

// AAA: битый ввод — строки без «ключа», «:значение», пустой bulk — мусор пропускается,
// пустой ввод → пустой словарь (не исключение).
[Fact]
public void ParseInfo_МусорныеСтроки_Пропущены()
{
    // Arrange
    var bulk = "без-разделителя\r\n:значение-без-ключа\r\nused_memory:1\r\n";

    // Act
    var info = ValkeyConnection.ParseInfo(bulk);

    // Assert: валидная пара сохранена, мусор пропущен.
    info.Should().HaveCount(1);
    info["used_memory"].Should().Be("1");
    ValkeyConnection.ParseInfo("").Should().BeEmpty();
}

// AAA: INFO all по проводу — bulk-кадр; фрейминг команды «INFO all» после AUTH.
[Fact]
public async Task InfoAll_BulkОтвет_СловарьПолей()
{
    // Arrange: полный INFO-ответ (bulk), включающий Replication.
    var body = "# Memory\r\nused_memory:1048576\r\nmaxmemory:536870912\r\n" +
               "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n";
    var bulk = $"${Encoding.UTF8.GetByteCount(body)}\r\n{body}\r\n";
    await using var stub = RespStub.Start("+OK\r\n", bulk);
    var conn = new ValkeyConnection(FastTimeout);

    // Act
    var result = await conn.InfoAllAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

    // Assert: словарь полей; фрейминг — RESP-массив INFO all.
    result.IsSuccess.Should().BeTrue();
    result.Value!["used_memory"].Should().Be("1048576");
    result.Value!["role"].Should().Be("master");
    var text = AssertAuthFrame(stub.Received);
    text.Should().Contain("$4\r\nINFO\r\n$3\r\nall\r\n");
}

// AAA: ошибка сервера на INFO → Result.Failed (S7: проба — не исключение).
[Fact]
public async Task InfoAll_ОшибкаСервера_Failed()
{
    // Arrange
    await using var stub = RespStub.Start("+OK\r\n", "-ERR unknown command\r\n");
    var conn = new ValkeyConnection(FastTimeout);

    // Act
    var result = await conn.InfoAllAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

    // Assert
    result.IsSuccess.Should().BeFalse();
    result.Error!.Message.Should().Contain("ERR");
}
```

- [x] **Шаг 2. Прогон — убедиться, что падают (нет метода)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.UnitTests.Core.ValkeyConnectionTests"
```
Ожидание: ошибка компиляции (`'IValkeyConnection' does not contain 'InfoAllAsync'`,
`ParseInfo` не найден) — это и есть «падение» TDD-цикла.

- [x] **Шаг 3. Реализация — `IValkeyConnection.cs`**

После `AclSetUserAsync` добавить:

```csharp
    // INFO all → плоский словарь ключ → значение (коллектор метрик t05, arch/18 §4.2).
    Task<Result<IReadOnlyDictionary<string, string>>> InfoAllAsync(ValkeyEndpoint ep, CancellationToken ct);
```

- [x] **Шаг 4. Реализация — `ValkeyConnection.cs`**

После `AclSetUserAsync` добавить:

```csharp
    public async Task<Result<IReadOnlyDictionary<string, string>>> InfoAllAsync(
        ValkeyEndpoint ep, CancellationToken ct)
        => await ExecuteAsync(ep, ["INFO", "all"], ProjectInfo, ct);
```

Рядом с `ProjectConfigGet` добавить проекцию и парсер:

```csharp
    // INFO all (bulk-строка) → словарь; не-bulk ответ — пустой словарь (не-error
    // кадр: сбор успешен, серии не эмитятся — консервативно, arch/18 §2.6).
    private static IReadOnlyDictionary<string, string> ProjectInfo(object? reply)
        => reply is string bulk ? ParseInfo(bulk) : [];

    // INFO-текст → плоский словарь: строки «ключ:значение» (\r\n-разделители);
    // заголовки секций «# Name» и пустые строки пропускаются; значения — строки
    // (числа выбирает коллектор). Чистая функция, internal — юнит-тесты
    // (паттерн Resp-парсера).
    internal static IReadOnlyDictionary<string, string> ParseInfo(string bulk)
    {
        var result = new Dictionary<string, string>();
        foreach (var rawLine in bulk.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r');
            if (line.Length == 0 || line[0] == '#')
                continue; // пустая строка / заголовок секции
            var separator = line.IndexOf(':');
            if (separator <= 0)
                continue; // мусорная строка без «ключ:» — консервативный skip
            result[line[..separator]] = line[(separator + 1)..];
        }

        return result;
    }
```

- [x] **Шаг 5. `FakeValkeyConnection` в `Fakes.cs` — реализация нового метода**

В `FakeValkeyConnection` (после `ConfigGetAsync`) добавить поле и метод:

```csharp
        // INFO-словарь ноды (коллектор t05): пустой — поля «отсутствуют».
        public readonly Dictionary<string, string> Info = [];

        public Task<Result<IReadOnlyDictionary<string, string>>> InfoAllAsync(
            ValkeyEndpoint ep, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Blind<IReadOnlyDictionary<string, string>>());
            if (Silent)
                return Task.FromResult(SilentBlind<IReadOnlyDictionary<string, string>>());
            if (!AuthOk(ep))
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Failed(
                    new ApplicationException("AUTH failed")));
            return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                (IReadOnlyDictionary<string, string>)Info));
        }
```

- [x] **Шаг 6. Прогон — зелёный + весь юнит-проект**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.UnitTests"
```
Ожидание: PASS (все, включая старые).

- [x] **Шаг 7. Коммит**

```bash
git add src/ValkeyWorker.Core/Valkey/IValkeyConnection.cs src/ValkeyWorker.Core/Valkey/ValkeyConnection.cs \
        src/tests/ValkeyWorker.UnitTests/Core/ValkeyConnectionTests.cs src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs
git commit -m "feat(valkey): InfoAllAsync + парсер INFO в RESP-миниклиенте — команда INFO all одной пробой (t05, arch/18 §4.2)"
```

---

### Задача 3. `ValkeyMetricsState` — стейт и 15 серий (Ф3)

**Вход:** задача 2 закоммичена (`InfoAllAsync` доступен).
**Выход:** `ValkeyMetricsState` + `ValkeyNodeSample`; юнит-тесты стейта зелёные.
**Проверка:** `dotnet test … ValkeyWorker.UnitTests.App.ValkeyMetricsStateTests` зелёный.
**Связь со spec:** §3 (словарь), §4.2, §7 Ф3, §9 (unit: стейт).

**Files:**
- Create: `src/ValkeyWorker.App/ValkeyMetricsState.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/App/ValkeyMetricsStateTests.cs`

**Interfaces:**
- Produces: `record ValkeyNodeSample(string Node, long? UsedMemoryBytes, long? MaxMemoryBytes,
  long? ConnectedClients, long? BlockedClients, long? EvictedKeys, long? ExpiredKeys,
  long? KeyspaceHits, long? KeyspaceMisses, long? InstantaneousOpsPerSec,
  long? TotalConnectionsReceived, long? RejectedConnections, long? TotalCommandsProcessed,
  string? Role, long? ConnectedSlaves)`;
  `ValkeyMetricsState(Meter meter)`; `void UpdateCluster(string cluster,
  IReadOnlyCollection<ValkeyNodeSample> samples)`; `void MarkSuccess(DateTimeOffset at)`;
  `internal DebugSnapshotRecord DebugSnapshot()` с `Nodes` и `LastSuccess`
  (используют задачи 4, 6).

- [x] **Шаг 1. Пишем падающие тесты `ValkeyMetricsStateTests.cs`**

```csharp
using System.Diagnostics.Metrics;
using FluentAssertions;
using ValkeyWorker.App;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Юнит-тесты ValkeyMetricsState (t05, arch/18 §2.6): затирание ушедших нод
// UpdateCluster, перезапись значений, консервативный LastSuccess, DebugSnapshot.
public sealed class ValkeyMetricsStateTests
{
    private static ValkeyNodeSample Sample(string node, long used = 1048576,
        string? role = "master", long? max = 536870912)
        => new(node, used, max, 1, 0, 0, 0, 5, 2, 7, 10, 0, 42, role, 0);

    // AAA: UpdateCluster затирает предыдущие записи кластера — ушедшие ноды не копятся.
    [Fact]
    public void UpdateCluster_ЗатираетУшедшиеНоды()
    {
        // Arrange: кластер c1 с node1; новый тик приносит только node2.
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        state.UpdateCluster("c1", [Sample("node1")]);

        // Act
        state.UpdateCluster("c1", [Sample("node2")]);

        // Assert: node1 исчез, node2 на месте; другой кластер не тронут.
        var nodes = state.DebugSnapshot().Nodes;
        nodes.Keys.Should().NotContain(("c1", "node1"));
        nodes.Keys.Should().Contain(("c1", "node2"));
    }

    // AAA: повторный сбор той же ноды перезаписывает значения (не дублирует).
    [Fact]
    public void UpdateCluster_ПерезаписываетЗначения()
    {
        // Arrange
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        state.UpdateCluster("c1", [Sample("node1", used: 100)]);

        // Act
        state.UpdateCluster("c1", [Sample("node1", used: 200)]);

        // Assert
        state.DebugSnapshot().Nodes[("c1", "node1")].UsedMemoryBytes.Should().Be(200);
        state.DebugSnapshot().Nodes.Should().HaveCount(1);
    }

    // AAA: MarkSuccess фиксирует время только явно (консервативный LastSuccess §4.2).
    [Fact]
    public void MarkSuccess_ФиксируетВремя()
    {
        // Arrange
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        var at = DateTimeOffset.UnixEpoch.AddHours(3);

        // Act
        state.MarkSuccess(at);

        // Assert
        state.DebugSnapshot().LastSuccess.Should().Be(at);
    }

    // AAA: поля сэмпла (вкл. role enum-паттерна и null-поля) видны в снапшоте.
    [Fact]
    public void DebugSnapshot_ПоляСэмплаВключаяRoleИNull()
    {
        // Arrange: сэмпл с отсутствующим maxmemory (null) и ролью slave.
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));

        // Act
        state.UpdateCluster("c1", [new ValkeyNodeSample(
            "node1", 1048576, null, 1, 0, 0, 0, 5, 2, 7, 10, 0, 42, "slave", null)]);

        // Assert: null-поля сохранены как null (серия не эмитится).
        var node = state.DebugSnapshot().Nodes[("c1", "node1")];
        node.MaxMemoryBytes.Should().BeNull();
        node.ConnectedSlaves.Should().BeNull();
        node.Role.Should().Be("slave");
    }
}
```

- [x] **Шаг 2. Прогон — падение (тип не существует)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.UnitTests.App.ValkeyMetricsStateTests"
```
Ожидание: ошибка компиляции `ValkeyMetricsState`/`ValkeyNodeSample` не найдены.

- [x] **Шаг 3. Реализация `src/ValkeyWorker.App/ValkeyMetricsState.cs`**

```csharp
using System.Collections.Frozen;
using System.Diagnostics.Metrics;

namespace ValkeyWorker.App;

// Срез доменных метрик ноды из INFO (arch/18 §2.6): null — поле INFO
// отсутствует/нечислово → серия НЕ эмитится (консервативно, без нулей-фантомов).
public sealed record ValkeyNodeSample(
    string Node,
    long? UsedMemoryBytes,
    long? MaxMemoryBytes,
    long? ConnectedClients,
    long? BlockedClients,
    long? EvictedKeys,
    long? ExpiredKeys,
    long? KeyspaceHits,
    long? KeyspaceMisses,
    long? InstantaneousOpsPerSec,
    long? TotalConnectionsReceived,
    long? RejectedConnections,
    long? TotalCommandsProcessed,
    string? Role,
    long? ConnectedSlaves);

// Стейт + ObservableGauge-серии valkey-коллектора (arch/18 §2.6/§4.2; зеркало
// KafkaMetricsState). Пассивный наблюдатель: чтение стейта под lock, серии пустые
// до первого тика; все доменные серии — gauge (persistence off сбрасывает
// кумулятивы ноды — counter ломал бы rate()/increase()).
public sealed class ValkeyMetricsState
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Cluster, string Node), ValkeyNodeSample> _nodes = [];
    private DateTimeOffset? _lastSuccess;

    public ValkeyMetricsState(Meter meter)
    {
        Gauge(meter, "valkey.memory.used_bytes", s => s.UsedMemoryBytes,
            "Потребление памяти нодой, байты (INFO used_memory)");
        Gauge(meter, "valkey.memory.max_bytes", s => s.MaxMemoryBytes,
            "Предел памяти ноды maxmemory, байты (INFO maxmemory)");
        Gauge(meter, "valkey.connected_clients", s => s.ConnectedClients,
            "Подключённые клиенты (INFO connected_clients)");
        Gauge(meter, "valkey.blocked_clients", s => s.BlockedClients,
            "Заблокированные клиенты BLPOP и т.п. (INFO blocked_clients)");
        Gauge(meter, "valkey.evicted_keys", s => s.EvictedKeys,
            "Кумулятив выселенных ключей (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.expired_keys", s => s.ExpiredKeys,
            "Кумулятив истёкших ключей (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.keyspace_hits", s => s.KeyspaceHits,
            "Кумулятив попаданий (hit-rate = hits/(hits+misses))");
        Gauge(meter, "valkey.keyspace_misses", s => s.KeyspaceMisses,
            "Кумулятив промахов ключей");
        Gauge(meter, "valkey.instantaneous_ops_per_sec", s => s.InstantaneousOpsPerSec,
            "Операции/сек — готовая скорость INFO (без rate())");
        Gauge(meter, "valkey.total_connections_received", s => s.TotalConnectionsReceived,
            "Кумулятив принятых соединений (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.rejected_connections", s => s.RejectedConnections,
            "Кумулятив отклонённых соединений (maxclients)");
        Gauge(meter, "valkey.total_commands_processed", s => s.TotalCommandsProcessed,
            "Кумулятив обработанных команд (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.connected_slaves", s => s.ConnectedSlaves,
            "Число реплик (v1 standalone — всегда 0; заготовка под топологии)");

        // role: enum-паттерн — 1 в серии с лейблом фактической роли (master|slave);
        // прочие значения INFO серию не эмитят (консервативно).
        meter.CreateObservableGauge(
            "valkey.role",
            () => Measure(_nodes
                .Where(kv => kv.Value.Role is "master" or "slave")
                .Select(kv => new Measurement<long>(1,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("node", kv.Key.Node),
                    new KeyValuePair<string, object?>("role", kv.Value.Role)))),
            description: "Роль ноды: 1 в серии с role=\"master|slave\" (INFO Replication.role)");

        meter.CreateObservableGauge(
            "valkey.collector.last_success_timestamp_seconds",
            () => Measure(new[] { ReadLastSuccess() }.OfType<Measurement<long>>()),
            unit: "s", description: "Unix-время последнего успешного тика коллектора");
    }

    // Обновление стейта кластера тиком: предыдущие записи кластера затираются —
    // ушедшие ноды не копятся; LastSuccess — только через MarkSuccess.
    public void UpdateCluster(string cluster, IReadOnlyCollection<ValkeyNodeSample> samples)
    {
        lock (_lock)
        {
            foreach (var key in _nodes.Keys.Where(k => k.Cluster == cluster).ToList())
                _nodes.Remove(key);
            foreach (var sample in samples)
                _nodes[(cluster, sample.Node)] = sample;
        }
    }

    // LastSuccess обновляется ТОЛЬКО при полном успехе всех проб тика (консервативно;
    // пустой домен = успех — алерт ValkeyCollectorStalled §5.2-правил).
    public void MarkSuccess(DateTimeOffset at)
    {
        lock (_lock)
            _lastSuccess = at;
    }

    // Чтение ТОЛЬКО под lock (зеркало KafkaMetricsState): конкурентный
    // UpdateCluster мутирует _lastSuccess.
    private Measurement<long>? ReadLastSuccess()
    {
        lock (_lock)
            return _lastSuccess is { } at ? new Measurement<long>(at.ToUnixTimeSeconds()) : null;
    }

    // Материализация (.ToArray) ОБЯЗАТЕЛЬНА под lock: OTel перечисляет результат
    // вне колбэка — ленивый Select по словарю даст InvalidOperationException при
    // конкурентном UpdateCluster (урок Ф7-4).
    private IEnumerable<Measurement<T>> Measure<T>(IEnumerable<Measurement<T>> read) where T : struct
    {
        lock (_lock)
            return read.ToArray();
    }

    // Регистрация числовой серии: поле сэмпла null → точка НЕ эмитится.
    private void Gauge(Meter meter, string name, Func<ValkeyNodeSample, long?> field, string description)
        => meter.CreateObservableGauge(
            name,
            () => Measure(_nodes
                .Select(kv => (kv.Key, Value: field(kv.Value)))
                .Where(t => t.Value is not null)
                .Select(t => new Measurement<long>(t.Value!.Value,
                    new KeyValuePair<string, object?>("cluster", t.Key.Cluster),
                    new KeyValuePair<string, object?>("node", t.Key.Node)))),
            description: description);

    /// <summary>Internal-снимок стейта для юнит-проверок (InternalsVisibleTo).</summary>
    internal DebugSnapshotRecord DebugSnapshot()
    {
        lock (_lock)
            return new DebugSnapshotRecord(_nodes.ToFrozenDictionary(), _lastSuccess);
    }

    internal sealed record DebugSnapshotRecord(
        IReadOnlyDictionary<(string Cluster, string Node), ValkeyNodeSample> Nodes,
        DateTimeOffset? LastSuccess);
}
```

- [x] **Шаг 4. Прогон — зелёный**

Команда из шага 2. Ожидание: 4 PASS.

- [x] **Шаг 5. Коммит**

```bash
git add src/ValkeyWorker.App/ValkeyMetricsState.cs src/tests/ValkeyWorker.UnitTests/App/ValkeyMetricsStateTests.cs
git commit -m "feat(valkey): ValkeyMetricsState — 15 ObservableGauge-серий словаря arch/18 §2.6, зеркало KafkaMetricsState (t05)"
```

---

### Задача 4. `ValkeyMetricsCollector` + опции + DI (Ф3)

**Вход:** задачи 2–3 закоммичены.
**Выход:** коллектор (hosted), `ValkeyWorkerMetricsOptions`, `appsettings.json`,
DI в `Program.cs` с делегатами снапшотов; юнит-тесты коллектора зелёные.
**Проверка:** юнит-тесты + `dotnet build` ValkeyWorker.App зелёные.
**Связь со spec:** §4.2–§4.4, §7 Ф3, §9 (unit: коллектор), §10.6.

**Files:**
- Create: `src/ValkeyWorker.App/ValkeyMetricsCollector.cs`
- Modify: `src/ValkeyWorker.App/Options.cs` (Metrics → subclass)
- Modify: `src/ValkeyWorker.App/appsettings.json` (CollectIntervalSec)
- Modify: `src/ValkeyWorker.App/Program.cs` (DI + делегаты + правка комментария)
- Test: `src/tests/ValkeyWorker.UnitTests/App/ValkeyMetricsCollectorTests.cs`

**Interfaces:**
- Consumes: `IValkeyConnection.InfoAllAsync` (задача 2); `ValkeyMetricsState` (задача 3);
  `ValkeySnapshotParser.Parse`, `ProcessCommon.ParsePortAlloc`,
  `ValkeyClusterSnapshot`, `NodeAddress` (существующие).
- Produces: `ValkeyMetricsCollector(int collectIntervalSec,
  Func<CancellationToken, Task<Result<IReadOnlyList<ValkeyClusterSnapshot>>>> clustersSnapshot,
  Func<CancellationToken, Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>>> portAllocSnapshot,
  IValkeyConnection valkey, string? advertisedClientHost, ValkeyMetricsState state,
  TimeProvider clock, ILogger<ValkeyMetricsCollector> logger) : BackgroundService`;
  `Task CollectOnceAsync(CancellationToken ct)`; `internal static int EffectiveIntervalSec(int)`;
  `internal static ValkeyNodeSample ToSample(string node, IReadOnlyDictionary<string, string> info)`;
  `class ValkeyWorkerMetricsOptions : Shared.Metrics.MetricsOptions { int CollectIntervalSec = 30 }`.

- [x] **Шаг 1. Пишем падающие тесты `ValkeyMetricsCollectorTests.cs`**

```csharp
using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core;
using ValkeyWorker.App;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Юнит-тесты ValkeyMetricsCollector (t05, arch/18 §4.2) на inline-фейке
// IValkeyConnection: успешный сбор обновляет стейт, ошибка ноды — тик жив и
// LastSuccess не двигается, пустой домен — успех, пропуски не-Active/без кредов/
// без portalloc, advertised-приоритет хоста, консервативные null-поля, дефолт <=0.
public sealed class ValkeyMetricsCollectorTests
{
    // Фейк RESP-клиента: INFO-ответы по (host, port) + журнал вызовов.
    private sealed class FakeValkey : IValkeyConnection
    {
        public Dictionary<(string Host, int Port), IReadOnlyDictionary<string, string>> Info = [];
        public Exception? FailAll; // сетевой отказ всех проб (нода лежит)
        public List<ValkeyEndpoint> Calls = [];

        public Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(ValkeyEndpoint ep, string parameter, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success([]));
        public Task<Result> ConfigSetAsync(ValkeyEndpoint ep, string parameter, string value, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyDictionary<string, string>>> InfoAllAsync(ValkeyEndpoint ep, CancellationToken ct)
        {
            Calls.Add(ep);
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Failed(
                    new ApplicationException(FailAll.Message)));
            return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                Info.GetValueOrDefault((ep.Host, ep.Port), [])));
        }
    }

    // Снапшот Active-кластера c1/node1 (RUNNING) с admin-кредами.
    private static ValkeyClusterSnapshot Snap(string cluster = "c1", string? state = null,
        string? adminUser = "admin", string? adminPassword = "secret")
        => new(cluster,
            new ValkeyClusterConfig(1, 536870912, "allkeys-lru", 1756500000, state),
            new Dictionary<string, ValkeyNodeSnapshot> { ["node1"] = new("node1", "RUNNING", null) },
            Endpoints: "dockhost:17001", AppUser: "app", AppPassword: "app-secret",
            AdminUser: adminUser, AdminPassword: adminPassword,
            UnknownKeys: [], ParseErrors: []);

    private static IReadOnlyDictionary<string, NodeAddress> Alloc(params (string Node, int Port)[] nodes)
        => nodes.ToDictionary(n => n.Node, n => new NodeAddress("dockhost", n.Port));

    private static Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>> AllocsOf(
        string cluster, IReadOnlyDictionary<string, NodeAddress> nodes)
        => Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>.Success(
            new Dictionary<string, IReadOnlyDictionary<string, NodeAddress>> { [cluster] = nodes });

    private static readonly IReadOnlyDictionary<string, string> FullInfo =
        new Dictionary<string, string>
        {
            ["used_memory"] = "1048576",
            ["maxmemory"] = "536870912",
            ["connected_clients"] = "3",
            ["blocked_clients"] = "0",
            ["evicted_keys"] = "0",
            ["expired_keys"] = "12",
            ["keyspace_hits"] = "5",
            ["keyspace_misses"] = "2",
            ["instantaneous_ops_per_sec"] = "7",
            ["total_connections_received"] = "10",
            ["rejected_connections"] = "0",
            ["total_commands_processed"] = "42",
            ["role"] = "master",
            ["connected_slaves"] = "0",
        };

    private static ValkeyMetricsCollector Collector(FakeValkey valkey, ValkeyMetricsState state,
        Func<CancellationToken, Task<Result<IReadOnlyList<ValkeyClusterSnapshot>>>>? clusters = null,
        Func<CancellationToken, Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>>>? allocs = null,
        string? advertised = "localhost", TimeProvider? clock = null)
        => new(30,
            clusters ?? (ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success([Snap()]))),
            allocs ?? (ct => Task.FromResult(AllocsOf("c1", Alloc(("node1", 17001))))),
            valkey, advertised, state, clock ?? TimeProvider.System,
            NullLogger<ValkeyMetricsCollector>.Instance);

    // AAA: успешный сбор — стейт обновлён, проба по advertised-хосту, LastSuccess двигается.
    [Fact]
    public async Task Collect_InfoOk_СерииИLastSuccess()
    {
        // Arrange: INFO-ответ на (localhost, 17001) — advertised приоритетнее portalloc.host.
        var valkey = new FakeValkey { Info = { [("localhost", 17001)] = FullInfo } };
        var clock = new FakeClock(DateTimeOffset.UnixEpoch.AddHours(5));
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state, clock: clock);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        var node = state.DebugSnapshot().Nodes[("c1", "node1")];
        node.UsedMemoryBytes.Should().Be(1048576);
        node.MaxMemoryBytes.Should().Be(536870912);
        node.Role.Should().Be("master");
        node.ConnectedSlaves.Should().Be(0);
        valkey.Calls.Should().ContainSingle().Which.Host.Should().Be("localhost");
        state.DebugSnapshot().LastSuccess.Should().Be(DateTimeOffset.UnixEpoch.AddHours(5));
    }

    // AAA: advertised == null → хост пробы из portalloc-записи (правило §2 arch/21).
    [Fact]
    public async Task Collect_БезAdvertised_ХостИзPortalloc()
    {
        // Arrange
        var valkey = new FakeValkey { Info = { [("dockhost", 17001)] = FullInfo } };
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state, advertised: null);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().ContainSingle().Which.Host.Should().Be("dockhost");
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: ошибка ноды — тик жив (не бросает), LastSuccess НЕ обновляется (зеркало Kafka).
    [Fact]
    public async Task Collect_ОшибкаНоды_ТикЖив_LastSuccessМёртв()
    {
        // Arrange: все INFO-провалы (нода лежит).
        var valkey = new FakeValkey { FailAll = new ApplicationException("connection refused") };
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state);

        // Act
        var act = () => collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        await act.Should().NotThrowAsync();
        state.DebugSnapshot().Nodes.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().BeNull();
    }

    // AAA: пустой домен — консервативный успех (LastSuccess двигается; алерт не горит).
    [Fact]
    public async Task Collect_ПустойДомен_Успех()
    {
        // Arrange: кластеров нет; portalloc пуст.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success([])),
            allocs: ct => Task.FromResult(AllocsOf("c1", Alloc())));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: не-Active (Config.State задан) — проб нет; тик успешен (skip ≠ fail).
    [Fact]
    public async Task Collect_НеActive_ПропускБезПробы()
    {
        // Arrange: заявка PROVISIONING.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success(
                [Snap(state: "PROVISIONING")])));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: неполные дискавери-креды (AdminUser/AdminPassword null) — пропуск без пробы.
    [Fact]
    public async Task Collect_БезAdminКредов_ПропускБезПробы()
    {
        // Arrange
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success(
                [Snap(adminUser: null, adminPassword: null)])));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: нода без portalloc-записи — пропуск (E9 — забота надзора C), тик успешен.
    [Fact]
    public async Task Collect_НодаБезPortalloc_Пропуск()
    {
        // Arrange: portalloc-ключ кластера пуст.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            allocs: ct => Task.FromResult(AllocsOf("c1", Alloc())));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().Nodes.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: ошибка чтения снапшота кластеров — тик пропущен, LastSuccess не двигается.
    [Fact]
    public async Task Collect_СнапшотНедоступен_ТикПропущен()
    {
        // Arrange: etcd-отказ (обёртка Failed).
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Failed(
                new ApplicationException("etcd down"))));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().BeNull();
    }

    // AAA: отсутствующие/нечисловые поля INFO → null-поля сэмпла (серии не эмитятся).
    [Fact]
    public void ToSample_ОтсутствующиеПоля_КонсервативныйNull()
    {
        // Arrange: только used_memory и role.
        IReadOnlyDictionary<string, string> partial = new Dictionary<string, string>
        {
            ["used_memory"] = "1",
            ["role"] = "master",
            ["instantaneous_ops_per_sec"] = "not-a-number",
        };

        // Act
        var sample = ValkeyMetricsCollector.ToSample("node1", partial);

        // Assert
        sample.UsedMemoryBytes.Should().Be(1);
        sample.Role.Should().Be("master");
        sample.MaxMemoryBytes.Should().BeNull();
        sample.InstantaneousOpsPerSec.Should().BeNull("нечисловое поле — серия не эмитится");
        sample.ConnectedSlaves.Should().BeNull();
    }

    // AAA: <=0-интервал → дефолт 30 (зеркало Kafka-паттерна SnapshotRefresher).
    [Theory]
    [InlineData(0, 30)]
    [InlineData(-5, 30)]
    [InlineData(45, 45)]
    public void EffectiveIntervalSec_НеПоложительный_Дефолт30(int configured, int expected)
    {
        // Arrange/Act/Assert
        ValkeyMetricsCollector.EffectiveIntervalSec(configured).Should().Be(expected);
    }

    private sealed class FakeClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
```

- [x] **Шаг 2. Прогон — падение**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.UnitTests.App.ValkeyMetricsCollectorTests"
```
Ожидание: ошибка компиляции `ValkeyMetricsCollector` не найден.

- [x] **Шаг 3. Реализация `src/ValkeyWorker.App/ValkeyMetricsCollector.cs`**

```csharp
using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.App;

// Коллектор valkey-метрик (t05, arch/18 §4.2; зеркало KafkaMetricsCollector):
// hosted-сервис с тиком ValkeyWorker:Metrics:CollectIntervalSec; по Active-кластерам
// (снапшот /valkey/clusters/) и адресам из /valkeyworker/portalloc/ — ОДНА проба
// INFO all на ноду за тик. Сбор read-only вне клэймов, без backoff (лёгкая
// TCP-проба); ошибка ноды не валит тик — LastSuccess обновляется только при
// полном успехе всех (cluster, node)-проб тика (консервативно, алерт §5.2).
public sealed class ValkeyMetricsCollector(
    int collectIntervalSec,
    Func<CancellationToken, Task<Result<IReadOnlyList<ValkeyClusterSnapshot>>>> clustersSnapshot,
    Func<CancellationToken, Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>>> portAllocSnapshot,
    IValkeyConnection valkey,
    string? advertisedClientHost,
    ValkeyMetricsState state,
    TimeProvider clock,
    ILogger<ValkeyMetricsCollector> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // <=0 → 30 с лог-предупреждением (паттерн SnapshotRefresher/Kafka-коллектора).
        var interval = EffectiveIntervalSec(collectIntervalSec);
        if (collectIntervalSec <= 0)
            logger.LogWarning(
                "ValkeyWorker:Metrics:CollectIntervalSec={Value} <= 0 — используется дефолт 30 с",
                collectIntervalSec);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CollectOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка host'а
            }
            catch (Exception ex)
            {
                // Исключение тика — лог warning, тик жив (метрики не роняют воркер).
                logger.LogWarning(ex, "тик ValkeyMetricsCollector упал: {Message}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
        }
    }

    // Ядро тика — публично для unit-тестов без хоста (паттерн Kafka-коллектора).
    public async Task CollectOnceAsync(CancellationToken ct)
    {
        var snapshots = await clustersSnapshot(ct);
        if (!snapshots.IsSuccess)
        {
            logger.LogWarning("снапшот кластеров недоступен: {Message}", snapshots.Error!.Message);
            return; // пропуск тика — LastSuccess не двигается (Stalled сработает)
        }

        var allocs = await portAllocSnapshot(ct);
        if (!allocs.IsSuccess)
        {
            logger.LogWarning("снапшот portalloc недоступен: {Message}", allocs.Error!.Message);
            return;
        }

        var allOk = true;
        foreach (var snap in snapshots.Value)
        {
            // Только Active (Config.State == null — невыполненные заявки не трогаем,
            // зеркало ревью Ф4-6; битый Config — консервативный skip) и полные
            // дискавери-креды (иначе проба невозможна — паттерн NodeSupervisor).
            if (snap.Config is null || snap.Config.State is not null
                || snap.AdminUser is null || snap.AdminPassword is null)
                continue;

            var addresses = allocs.Value.GetValueOrDefault(snap.Cluster) ?? EmptyAddresses;
            if (!await TryCollectClusterAsync(snap, addresses, ct))
                allOk = false;
        }

        if (allOk)
            state.MarkSuccess(clock.GetUtcNow());
    }

    private static readonly IReadOnlyDictionary<string, NodeAddress> EmptyAddresses =
        new Dictionary<string, NodeAddress>();

    // Сбор кластера: одна INFO all на ноду, без ретраев; false — ошибка сбора
    // (тик жив, LastSuccess не обновляется; остальные кластеры тика собираются).
    private async Task<bool> TryCollectClusterAsync(
        ValkeyClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        try
        {
            var samples = new List<ValkeyNodeSample>();
            foreach (var node in snap.Nodes.Keys.Order())
            {
                // Нода без portalloc-записи — пропуск (лестница E9 — забота надзора C,
                // коллектор только читает); хост пробы — advertised ?? portalloc.host.
                if (!addresses.TryGetValue(node, out var address))
                    continue;

                var info = await valkey.InfoAllAsync(
                    new ValkeyEndpoint(
                        advertisedClientHost ?? address.Host, address.ClientPort,
                        snap.AdminUser!, snap.AdminPassword!),
                    ct);
                if (!info.IsSuccess)
                {
                    logger.LogWarning("кластер {Cluster}: INFO {Node} не удался: {Message}",
                        cluster, node, info.Error!.Message);
                    return false;
                }

                samples.Add(ToSample(node, info.Value));
            }

            state.UpdateCluster(cluster, samples);
            return true;
        }
        catch (Exception ex)
        {
            // Пассивный наблюдатель: исключение сбора — не роняет тик.
            logger.LogWarning(ex, "кластер {Cluster}: сбор метрик упал: {Message}", cluster, ex.Message);
            return false;
        }
    }

    // INFO-словарь → срез серий §2.6: только поля словаря; отсутствующие/нечисловые
    // → null (серия не эмитится — консервативно); role — только master|slave.
    internal static ValkeyNodeSample ToSample(string node, IReadOnlyDictionary<string, string> info) => new(
        node,
        Long(info, "used_memory"),
        Long(info, "maxmemory"),
        Long(info, "connected_clients"),
        Long(info, "blocked_clients"),
        Long(info, "evicted_keys"),
        Long(info, "expired_keys"),
        Long(info, "keyspace_hits"),
        Long(info, "keyspace_misses"),
        Long(info, "instantaneous_ops_per_sec"),
        Long(info, "total_connections_received"),
        Long(info, "rejected_connections"),
        Long(info, "total_commands_processed"),
        info.GetValueOrDefault("role") is "master" or "slave" ? info["role"] : null,
        Long(info, "connected_slaves"));

    private static long? Long(IReadOnlyDictionary<string, string> info, string key)
        => long.TryParse(info.GetValueOrDefault(key), NumberStyles.Integer,
               CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    // <=0 → 30 с (internal — юнит-тест дефолта).
    internal static int EffectiveIntervalSec(int configured) => configured <= 0 ? 30 : configured;
}
```

- [x] **Шаг 4. Опции `Options.cs`**

Заменить блок:

```csharp
    /// <summary>Метрики воркера (arch/18 §2.2): базовый набор Shared.Metrics
    /// (коллектор доменных метрик — t05, в t02 не входит).</summary>
    public Shared.Metrics.MetricsOptions Metrics { get; set; } = new();
```

на:

```csharp
    /// <summary>Метрики воркера (arch/18 §2.2/§2.6/§4.2): экспозиция + тик
    /// коллектора INFO.</summary>
    public ValkeyWorkerMetricsOptions Metrics { get; set; } = new();
```

и сразу после класса `ValkeyWorkerOptions` добавить:

```csharp
/// <summary>Метрики ValkeyWorker (arch/18 §2.6/§4.2): базовая экспозиция +
/// интервал тика коллектора INFO.</summary>
public sealed class ValkeyWorkerMetricsOptions : Shared.Metrics.MetricsOptions
{
    /// <summary>Тик коллектора INFO, сек (default 30; arch/18 §4.2).</summary>
    public int CollectIntervalSec { get; set; } = 30;
}
```

- [x] **Шаг 5. `appsettings.json`**

Строку `"Metrics": { "Enabled": true, "Path": "/metrics" },` заменить на
`"Metrics": { "Enabled": true, "Path": "/metrics", "CollectIntervalSec": 30 },`.

- [x] **Шаг 6. `Program.cs` — DI коллектора и делегаты**

1) Обновить комментарий блока метрик (строки над `AddAppMetrics`): «Коллектор
   доменных метрик INFO — t05 (в t02 не входит).» → «Коллектор доменных метрик
   INFO — arch/18 §4.2 (ValkeyMetricsCollector ниже).»
2) После блока циклов (после `builder.Services.AddHostedService(sp =>
   sp.GetRequiredService<ReconcileLoop>());`) и ДО блока «Наблюдаемость» вставить:

```csharp
// Коллектор доменных метрик INFO (arch/18 §4.2, t05): read-only сбор вне клэймов;
// источники — снапшот /valkey/clusters/ (парсер ValkeySnapshotParser) и Range
// /valkeyworker/portalloc/; только Active + полные дискавери-креды (зеркало
// Kafka-коллектора: сразу после циклов).
builder.Services.AddSingleton(sp => new ValkeyMetricsState(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddHostedService(sp => new ValkeyMetricsCollector(
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Metrics.CollectIntervalSec,
    ct => SnapshotClustersAsync(sp, ct),
    ct => SnapshotPortAllocAsync(sp, ct),
    sp.GetRequiredService<IValkeyConnection>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.AdvertisedClientHost,
    sp.GetRequiredService<ValkeyMetricsState>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<ValkeyMetricsCollector>>()));
```

3) Внизу `Program.cs`, рядом с kafka-аналогом (после `ToProvisioningOptions`),
   добавить два static-делегата (в using файла — `System.Text.Json`, если его нет):

```csharp
// Источник кластеров для коллектора метрик (arch/18 §4.2): Range /valkey/clusters/
// c failover по endpoints → ValkeySnapshotParser (паттерн Kafka-коллектора).
static async Task<Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>> SnapshotClustersAsync(
    IServiceProvider sp, CancellationToken ct)
{
    var gateway = sp.GetRequiredService<IEtcdGateway>();
    var endpoints = sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints;
    Result<IReadOnlyList<Kv>>? last = null;
    foreach (var endpoint in endpoints)
    {
        var range = await gateway.RangeAsync(endpoint, "/valkey/clusters/", ct);
        if (!range.IsSuccess)
        {
            last = range;
            continue;
        }

        var parsed = ValkeyWorker.Etcd.Parsing.ValkeySnapshotParser.Parse(range.Value);
        return parsed.IsSuccess
            ? Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Success(parsed.Value.Clusters)
            : Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(parsed.Error!);
    }

    return Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(last!.Error!);
}

// Источник адресов нод коллектора: Range /valkeyworker/portalloc/ →
// cluster → node → NodeAddress; битые ключи — warning + skip (паттерн PortAllocIndex),
// тик не роняют.
static async Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>> SnapshotPortAllocAsync(
    IServiceProvider sp, CancellationToken ct)
{
    var gateway = sp.GetRequiredService<IEtcdGateway>();
    var logger = sp.GetRequiredService<ILogger<ValkeyMetricsCollector>>();
    var endpoints = sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints;
    Result<IReadOnlyList<Kv>>? last = null;
    foreach (var endpoint in endpoints)
    {
        var range = await gateway.RangeAsync(endpoint, "/valkeyworker/portalloc/", ct);
        if (!range.IsSuccess)
        {
            last = range;
            continue;
        }

        var allocs = new Dictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>();
        foreach (var kv in range.Value)
        {
            var cluster = kv.Key.Split('/')[^1];
            try
            {
                allocs[cluster] = ValkeyWorker.Provisioning.Processes.ProcessCommon.ParsePortAlloc(kv.Value);
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                logger.LogWarning("коллектор метрик: битый portalloc-ключ {Cluster}: {Error}",
                    cluster, ex.Message);
            }
        }

        return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>.Success(allocs);
    }

    return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>.Failed(last!.Error!);
}
```

Примечание: если в файле есть соответствующие `using` (`ValkeyWorker.Core.Model`,
`ValkeyWorker.Etcd.Parsing`, `ValkeyWorker.Provisioning.Processes`) — уточнённые
имена можно сократить; главное — компиляция без предупреждений.

- [x] **Шаг 7. Прогон — зелёный (юнит-тесты + сборка)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.UnitTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
```
Ожидание: все тесты PASS, сборка без предупреждений (TreatWarningsAsErrors).

- [x] **Шаг 8. Коммит**

```bash
git add src/ValkeyWorker.App/ValkeyMetricsCollector.cs src/ValkeyWorker.App/Options.cs \
        src/ValkeyWorker.App/appsettings.json src/ValkeyWorker.App/Program.cs \
        src/tests/ValkeyWorker.UnitTests/App/ValkeyMetricsCollectorTests.cs
git commit -m "feat(valkey): ValkeyMetricsCollector + ValkeyWorkerMetricsOptions + DI — тиковый INFO-сбор Active-кластеров (t05, arch/18 §4.2, зеркало Kafka)"
```

---

### Задача 5. Интеграционные тесты `/metrics` (Ф4): last_success на пустом домене + live-фикстура 15 имён

**Вход:** задача 4 закоммичена (коллектор в DI).
**Выход:** (а) быстрый тест `valkey_collector_last_success_timestamp_seconds` на пустом
etcd в существующей коллекции; (б) live-фикстура (свой etcd + реальный docker-сокет +
WAF с живыми циклами): сид заявки → нода поднята → все 15 фактических имён в `/metrics`.
**Проверка:** прогон обоих тестов (docker доступен) зелёный; teardown-чистота.
**Связь со spec:** §9 (Integration: имена словаря, otel_scope_name), §7 Ф4, §10.2.

**Files:**
- Modify: `src/tests/ValkeyWorker.IntegrationTests/Api/MetricsApiTests.cs` (+1 тест)
- Create: `src/tests/ValkeyWorker.IntegrationTests/Api/MetricsDomainTests.cs`
  (фикстура + фабрика + тест; отдельный файл — не мешает быстрой коллекции)

**Interfaces:**
- Consumes: DI-хост `Program` (коллектор зарегистрирован задачей 4);
  `FreePortWindow.Find()` и паттерны `ValkeyClusterFixture` (etcd-контейнер,
  PlainClusterDriver, сид) — только public-составляющие, саму фикстуру НЕ меняем.

- [x] **Шаг 1. Быстрый тест в `MetricsApiTests.cs` (пустой домен = успех)**

Добавить в класс `MetricsApiTests`:

```csharp
    // AAA (t05): пустой домен — консервативный успех тика: серия самонаблюдения
    // коллектора обязана появиться в экспорте при живом воркере (без docker-нод).
    [Fact]
    public async Task Metrics_ValkeyCollectorLastSuccess_ПустойДоменУспех()
    {
        // Arrange: WAF-фабрика с живыми циклами на пустом etcd; первый тик
        // коллектора — сразу при старте (до Task.Delay), scrape 500 мс.
        using var client = fx.Factory.CreateClient();

        // Act: retry-цикл до 15 с.
        string body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("valkey_collector_last_success_timestamp_seconds"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серия самонаблюдения в scope ValkeyWorker.
        body.Should().Contain("valkey_collector_last_success_timestamp_seconds");
        body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Should().Contain(l => l.StartsWith("valkey_collector_last_success_timestamp_seconds")
                && l.Contains("""otel_scope_name="ValkeyWorker""""));
    }
```

- [x] **Шаг 2. Live-фикстура и тест — `MetricsDomainTests.cs`**

```csharp
using System.Net;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using ValkeyWorker.Docker.Drivers;
using ValkeyWorker.Docker.Engine;
using Shared.Core.Planning;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// Live-фикстура фактических имён словаря arch/18 §2.6 (риск M3, spec §9): WAF-хост
// ValkeyWorker с ЖИВЫМИ hosted-циклами, реальным docker-сокетом и собственным etcd.
// Сид заявки → ReconcileLoop поднимает ноду → коллектор собирает INFO → /metrics
// содержит все 15 канонических имён. Полный teardown при любом исходе + ассерт
// чистоты (AGENTS.base §11): ни контейнера vwk-<тега>, ни ключей префикса.
public sealed class ValkeyMetricsDomainFixture : IAsyncLifetime
{
    private readonly IContainer _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
        .WithCommand(
            "etcd",
            "--name=test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379")
        .WithPortBinding(2379, assignRandomHostPort: true)
        .Build();

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // Динамическое окно хост-портов публикации нод — вне стендовой зоны 17000–17999.
    private static readonly (int From, int To) HostPorts =
        Valkey.FreePortWindow.Find();

    public string Cluster { get; } = $"mtr{Guid.NewGuid():N}"[..12];

    public MetricsLiveFactory Factory { get; private set; } = null!;

    private PlainClusterDriver? Driver { get; set; }

    private string Endpoint { get; set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";

        // Проба готовности etcd (паттерн ValkeyClusterFixture).
        using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var resp = await probe.PostAsync(Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"), ct);
                if (resp.IsSuccessStatusCode)
                    break;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается
            }

            await Task.Delay(1000, ct);
        }

        // Сид заявки (формат SeedClusterAsync) — до старта WAF: циклы возьмут сразу.
        var gateway = new Shared.Etcd.Client.EtcdGateway(_http);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/nodes/node1/state",
            "NOT_INITIALIZED", lease: null, ct);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/nodes/node1/resources",
            """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", lease: null, ct);

        Factory = new MetricsLiveFactory(Endpoint, HostPorts.From, HostPorts.To);
        Driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory());
    }

    public async ValueTask DisposeAsync()
    {
        // Демонтаж при ЛЮБОМ исходе: WAF (останавливает циклы), контейнеры ноды, etcd.
        if (Factory is not null)
            await Factory.DisposeAsync();

        var ct = CancellationToken.None;
        if (Driver is not null)
        {
            try
            {
                var objects = await Driver.ListNodeObjectsAsync(Cluster, ct);
                if (objects.IsSuccess)
                    foreach (var name in objects.Value)
                        await Driver.RemoveNodeAsync(Cluster, name[$"vwk-{Cluster}-".Length..], ct);
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают
            }
        }

        _http.Dispose();
        await _etcd.DisposeAsync();

        // Ассерт чистоты: ни контейнера vwk-<C>-* этого прогона.
        if (Driver is not null)
            (await Driver.ListNodeObjectsAsync(Cluster, ct)).Value.Should().BeEmpty(
                "после teardown не осталось контейнеров прогона");
    }
}

// WAF-хост с реальным docker-сокетом и быстрыми тиками: циклы поднимают ноду,
// коллектор собирает, /metrics — in-memory (AllowInsecureHttp — только WAF-транспорт).
public sealed class MetricsLiveFactory(string etcdEndpoint, int portFrom, int portTo)
    : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ValkeyWorker:Etcd:Endpoints:0"] = etcdEndpoint,
            ["ValkeyWorker:Docker:Hosts:0:Name"] = "local",
            ["ValkeyWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/docker.sock",
            ["ValkeyWorker:Docker:PortRange:From"] = portFrom.ToString(),
            ["ValkeyWorker:Docker:PortRange:To"] = portTo.ToString(),
            ["ValkeyWorker:AdvertisedClientHost"] = "localhost",
            ["ValkeyWorker:Api:AdvertiseUrl"] = "https://localhost:9997",
            ["ValkeyWorker:Api:EnableSeedEndpoint"] = "false",
            ["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "true",
            ["ValkeyWorker:Loops:ScanIntervalSec"] = "2",
            ["ValkeyWorker:Loops:KeepaliveSec"] = "2",
            ["ValkeyWorker:Thresholds:NodeBootSec"] = "100",
            ["ValkeyWorker:Metrics:CollectIntervalSec"] = "2",
        }));
    }
}

public sealed class MetricsDomainTests(ValkeyMetricsDomainFixture fx) : IClassFixture<ValkeyMetricsDomainFixture>
{
    // Все 15 канонических имён §2.6 — факт экспорта против словаря (риск M3).
    private static readonly string[] CanonicalNames =
    [
        "valkey_memory_used_bytes", "valkey_memory_max_bytes",
        "valkey_connected_clients", "valkey_blocked_clients",
        "valkey_evicted_keys", "valkey_expired_keys",
        "valkey_keyspace_hits", "valkey_keyspace_misses",
        "valkey_instantaneous_ops_per_sec",
        "valkey_total_connections_received", "valkey_rejected_connections",
        "valkey_total_commands_processed",
        "valkey_role", "valkey_connected_slaves",
        "valkey_collector_last_success_timestamp_seconds",
    ];

    // AAA: живой кластер (hosted-циклы подняли ноду из сида) → все 15 имён словаря
    // экспортированы фактически, scope — ValkeyWorker, role — master (standalone).
    [Fact]
    public async Task Metrics_ЖивойКластер_Все15ИмёнСловаря()
    {
        // Arrange: фикстура засеяла заявку; WAF с циклами (NodeBootSec ≤ 100).
        using var client = fx.Factory.CreateClient();

        // Act: поллинг до первого INFO-сбора (бюджет ≤ 100 с: цикл 2 с + boot + сбор 2 с).
        string body = "";
        for (var i = 0; i < 100; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("valkey_memory_used_bytes"))
                break;
            await Task.Delay(1000, TestContext.Current.CancellationToken);
        }

        // Assert: фактические имена против словаря §2.6.
        foreach (var name in CanonicalNames)
            body.Should().Contain(name, "серия {0} обязана экспортироваться при живой ноде", name);
        body.Should().Contain("""role="master"""", "standalone-нода — master");
        body.Should().Contain("""otel_scope_name="ValkeyWorker"""");
    }
}
```

Если `FreePortWindow`/`EtcdGateway`/`PlainClusterDriver` не видны по namespaces в
новом файле — добавить соответствующие `using` (`ValkeyWorker.IntegrationTests.Valkey`,
`Shared.Etcd.Client`; все типы public в своих сборках/проекте).

- [x] **Шаг 3. Прогон (docker доступен)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests.Api.MetricsDomainTests | FullyQualifiedName~ValkeyWorker.IntegrationTests.Api.MetricsApiTests"
```
Ожидание: PASS (live-тест — до ~2 мин: boot ноды + первый сбор).

- [x] **Шаг 4. Зачистка после серии (обязательный гейт AGENTS.md)**

```bash
docker ps -a --filter "name=vwk-" -q | xargs -r docker rm -f
docker network ls --filter "type=custom" -q | xargs -r docker network rm 2>/dev/null || true
```
(вторую команду — только если сети остались от прогона; valkey-домен per-cluster сетей не создаёт).

- [x] **Шаг 5. Коммит**

```bash
git add src/tests/ValkeyWorker.IntegrationTests/Api/MetricsApiTests.cs \
        src/tests/ValkeyWorker.IntegrationTests/Api/MetricsDomainTests.cs
git commit -m "test(valkey): интеграция телеметрии t05 — last_success на пустом домене + live-WAF фиксация всех 15 фактических имён §2.6 (риск M3)"
```

---

### Задача 6. Живой коллектор против docker-ноды (Ф4): сбор + остановленная нода

**Вход:** задача 4 закоммичена; docker доступен.
**Выход:** интеграционный тест в коллекции `valkey-cluster`: provisioning → INFO-сбор
серий со значениями → `docker stop` ноды → тик жив, LastSuccess стоит.
**Проверка:** прогон теста зелёный; фикстура чистит (rm -f работает на остановленных).
**Связь со spec:** §9 (Integration: живая/остановленная нода), §7 Ф4.

**Files:**
- Test: `src/tests/ValkeyWorker.IntegrationTests/Valkey/MetricsCollectorTests.cs`

**Interfaces:**
- Consumes: `ValkeyClusterFixture` (etcd+docker, `NewProvisioning`, `RequireSnapshotAsync`),
  `ValkeyMetricsCollector`/`ValkeyMetricsState` (задачи 3–4), `ValkeyConnection`.

- [x] **Шаг 1. Пишем тест `MetricsCollectorTests.cs`**

```csharp
using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.App;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Живой коллектор против docker-ноды (t05, spec §9): provisioning кластера →
// CollectOnceAsync → серии словаря со значениями (роль master, maxmemory из
// декларации); docker stop ноды → тик не падает, LastSuccess стоит, стейт не
// перезаписан. Отдельный от live-WAF (задача 5) тест: прямой контроль тиков.
[Collection(ValkeyClusterCollection.Name)]
public sealed class MetricsCollectorTests(ValkeyClusterFixture fx)
{
    // Settable-часы: между тиками двигаем время — «LastSuccess стоит» проверяемо.
    private sealed class SettableClock(DateTimeOffset utc) : TimeProvider
    {
        private DateTimeOffset _utc = utc;
        public override DateTimeOffset GetUtcNow() => _utc;
        public void Advance(TimeSpan span) => _utc += span;
    }

    private static async Task<Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>> ClustersAsync(
        ValkeyClusterFixture fx, CancellationToken ct)
    {
        var range = await fx.Gateway.RangeAsync(fx.Endpoint, "/valkey/clusters/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(range.Error!);
        var parsed = ValkeySnapshotParser.Parse(range.Value);
        return parsed.IsSuccess
            ? Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Success(parsed.Value.Clusters)
            : Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(parsed.Error!);
    }

    private static async Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>> AllocsAsync(
        ValkeyClusterFixture fx, CancellationToken ct)
    {
        var range = await fx.Gateway.RangeAsync(fx.Endpoint, "/valkeyworker/portalloc/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>.Failed(range.Error!);
        var allocs = new Dictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>();
        foreach (var kv in range.Value)
            allocs[kv.Key.Split('/')[^1]] = ProcessCommon.ParsePortAlloc(kv.Value);
        return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<ValkeyWorker.Core.Model.NodeAddress>>>.Success(allocs);
    }

    private static void DockerStop(string container)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker", $"stop {container}")
        {
            RedirectStandardOutput = true,
        };
        using var proc = System.Diagnostics.Process.Start(psi)!;
        proc.WaitForExit();
    }

    // AAA: живая нода — серии словаря со значениями; остановленная — тик жив,
    // LastSuccess стоит (консервативно), стейт не перезаписан.
    [Fact]
    public async Task Collect_ЖиваяНодаИОстановленная_СерииИLastSuccess()
    {
        // Arrange: provisioning кластера (паттерн ProvisioningTests).
        var ct = TestContext.Current.CancellationToken;
        var cluster = fx.Cluster("mtr");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(),
            fx.NewSecretEnsurer());
        var snap = await fx.RequireSnapshotAsync(cluster);
        (await process.TickAsync(snap, ct)).IsSuccess.Should().BeTrue("нода поднята provisioning-тиком");

        var state = new ValkeyMetricsState(new Meter("TestValkeyLiveCollector"));
        var clock = new SettableClock(DateTimeOffset.UnixEpoch.AddHours(9));
        var collector = new ValkeyMetricsCollector(30,
            ct2 => ClustersAsync(fx, ct2),
            ct2 => AllocsAsync(fx, ct2),
            new ValkeyConnection(TimeSpan.FromSeconds(2)),
            ValkeyClusterFixture.AdvertisedClientHost,
            state, clock, NullLogger<ValkeyMetricsCollector>.Instance);

        // Act 1: сбор живой ноды.
        await collector.CollectOnceAsync(ct);

        // Assert 1: поля INFO с фактическими значениями; LastSuccess = T0.
        var node = state.DebugSnapshot().Nodes[(cluster, "node1")];
        node.UsedMemoryBytes.Should().BeGreaterThan(0);
        node.MaxMemoryBytes.Should().Be(536870912, "maxmemory задан декларацией сида");
        node.Role.Should().Be("master");
        node.ConnectedSlaves.Should().Be(0);
        node.TotalCommandsProcessed.Should().BeGreaterThanOrEqualTo(0);
        state.DebugSnapshot().LastSuccess.Should().Be(DateTimeOffset.UnixEpoch.AddHours(9));

        // Arrange 2: останавливаем ноду (порт закрылся; portalloc-ключ жив).
        DockerStop($"vwk-{cluster}-node1");
        clock.Advance(TimeSpan.FromSeconds(60));
        var before = state.DebugSnapshot().LastSuccess;

        // Act 2: тик на лежачей ноде.
        var act = () => collector.CollectOnceAsync(ct);

        // Assert 2: тик не бросает; LastSuccess НЕ двигался; стейт прежний.
        await act.Should().NotThrowAsync();
        state.DebugSnapshot().LastSuccess.Should().Be(before, "LastSuccess стоит при неуспехе тика");
        state.DebugSnapshot().Nodes[(cluster, "node1")].UsedMemoryBytes
            .Should().Be(node.UsedMemoryBytes, "стейт не перезаписан провальным тиком");
    }
}
```

- [x] **Шаг 2. Прогон (docker)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests.Valkey.MetricsCollectorTests"
```
Ожидание: PASS.

- [x] **Шаг 3. Зачистка серии + коммит**

```bash
docker ps -a --filter "name=vwk-" -q | xargs -r docker rm -f
git add src/tests/ValkeyWorker.IntegrationTests/Valkey/MetricsCollectorTests.cs
git commit -m "test(valkey): живой коллектор t05 — INFO-сбор docker-ноды и консервативный LastSuccess на остановленной"
```

---

### Задача 7. Стенд мониторинга (Ф5): scrape, alerts, dashboard, чек 65

**Вход:** задачи 1–6 закоммичены (код словаря/коллектора соответствует канону).
**Выход:** `prometheus.yml` + `rules.yml` + `dashboards/valkey.json` + правки
`checks/65-metrics.sh`; полный прогон стенда — задача 8.
**Проверка:** синтаксис-гейты шага 5 (bash -n / jq / compose config).
**Связь со spec:** §5.1–§5.4, §7 Ф5.

**Files:**
- Modify: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` (новая джоба)
- Modify: `dev-stand/adminpanel/metrics/prometheus/rules.yml` (группа valkey)
- Create: `dev-stand/adminpanel/metrics/grafana/dashboards/valkey.json`
- Modify: `dev-stand/adminpanel/checks/65-metrics.sh` (4 правки)

- [x] **Шаг 1. `prometheus.yml` — джоба valkeyworker (зеркало kafkaworker)**

После блока `kafkaworker` (перед `adminpanel`) вставить:

```yaml
  - job_name: valkeyworker         # сеть стенда (профиль valkey; mTLS — общий tls_config, t05)
    scheme: https
    tls_config:
      ca_file: /tls/ca.pem
      cert_file: /tls/prometheus.crt
      key_file: /tls/prometheus.key
    static_configs: [{targets: ["valkeyworker:8080"]}]
```

- [x] **Шаг 2. `rules.yml` — группа valkey (3 алерта, spec §5.2)**

После группы `kafka` (перед `pg`) вставить:

```yaml
  - name: valkey
    rules:
      - alert: ValkeyCollectorStalled
        expr: time() - valkey_collector_last_success_timestamp_seconds > 300
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "коллектор valkey-метрик не собирает >5мин"
          description: "фиксированный порог ≥3×CollectIntervalSec(30с); runbook — arch/18 §4.2"
      - alert: ValkeyEvictionsGrowing
        expr: rate(valkey_evicted_keys[5m]) > 0
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.node }}: выселения ключей растут"
          description: "rate по gauge-кумулятиву (сбросы при рестарте — arch/18 §2.6); runbook — arch/18 §2.6"
      - alert: ValkeyMemoryNearMax
        expr: (valkey_memory_used_bytes / valkey_memory_max_bytes > 0.9) and (valkey_memory_max_bytes > 0)
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.node }}: память >90% maxmemory"
          description: "and valkey_memory_max_bytes > 0 отсекает безлимитный maxmemory=0; runbook — arch/18 §2.6"
```

- [x] **Шаг 3. `dashboards/valkey.json` — 9 панелей spec §5.3**

Создать файл (стиль `kafka.json`: uid `valkey`, schemaVersion 41, refresh 30s, now-1h):

```json
{
  "uid": "valkey", "title": "Valkey (memory / hit-rate / evictions / clients / collector)", "schemaVersion": 41,
  "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {"type": "timeseries", "title": "Memory used vs max", "gridPos": {"x":0,"y":0,"w":16,"h":9},
     "targets": [{"expr": "valkey_memory_used_bytes", "legendFormat": "{{cluster}}/{{node}} used", "refId": "A"},
                 {"expr": "valkey_memory_max_bytes", "legendFormat": "{{cluster}}/{{node}} max", "refId": "B"}]},
    {"type": "stat", "title": "Memory pressure", "gridPos": {"x":16,"y":0,"w":8,"h":4},
     "targets": [{"expr": "valkey_memory_used_bytes / valkey_memory_max_bytes and valkey_memory_max_bytes > 0", "refId": "A"}]},
    {"type": "stat", "title": "Collector staleness, s (alert > 300)", "gridPos": {"x":16,"y":4,"w":8,"h":5},
     "targets": [{"expr": "time() - valkey_collector_last_success_timestamp_seconds", "refId": "A"}]},
    {"type": "stat", "title": "Keyspace hit rate", "gridPos": {"x":0,"y":9,"w":8,"h":5},
     "targets": [{"expr": "valkey_keyspace_hits / clamp_min(valkey_keyspace_hits + valkey_keyspace_misses, 1)", "refId": "A"}]},
    {"type": "timeseries", "title": "Evictions (rate 5m)", "gridPos": {"x":8,"y":9,"w":8,"h":5},
     "targets": [{"expr": "rate(valkey_evicted_keys[5m])", "legendFormat": "{{cluster}}/{{node}}", "refId": "A"}]},
    {"type": "timeseries", "title": "ops/sec", "gridPos": {"x":16,"y":9,"w":8,"h":5},
     "targets": [{"expr": "valkey_instantaneous_ops_per_sec", "legendFormat": "{{cluster}}/{{node}}", "refId": "A"}]},
    {"type": "timeseries", "title": "Clients", "gridPos": {"x":0,"y":14,"w":12,"h":8},
     "targets": [{"expr": "valkey_connected_clients", "legendFormat": "{{cluster}}/{{node}} connected", "refId": "A"},
                 {"expr": "valkey_blocked_clients", "legendFormat": "{{cluster}}/{{node}} blocked", "refId": "B"}]},
    {"type": "timeseries", "title": "Connections (rate)", "gridPos": {"x":12,"y":14,"w":12,"h":8},
     "targets": [{"expr": "rate(valkey_total_connections_received[5m])", "legendFormat": "{{cluster}}/{{node}} received", "refId": "A"},
                 {"expr": "rate(valkey_rejected_connections[5m])", "legendFormat": "{{cluster}}/{{node}} rejected", "refId": "B"}]},
    {"type": "stat", "title": "Connected slaves", "gridPos": {"x":0,"y":22,"w":24,"h":4},
     "targets": [{"expr": "valkey_connected_slaves", "refId": "A"}]}
  ]
}
```

- [x] **Шаг 4. `checks/65-metrics.sh` — 4 правки по spec §5.4**

1) Шаг 0 (после блока живости pgworker) — гарантия живости valkeyworker (порт на
   хост НЕ публикуется — healthz изнутри контейнера, паттерн `05-seed.sh`):

```bash
# 0.1) гарантия живости valkeyworker (t05): up -d создаёт/стартует (частичная
#      предыстория), healthz — mTLS-парой healthcheck ИЗНУТРИ контейнера.
docker compose --profile valkey up -d valkeyworker >/dev/null 2>&1 || true
for i in $(seq 1 60); do
  docker compose exec -T valkeyworker curl -fsS -m 3 \
    --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1
done
docker compose exec -T valkeyworker curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ valkeyworker не ожил за 60 c (:8080/healthz mTLS изнутри; docker compose logs valkeyworker)"; exit 1; }
```

и дополнить результирующую строку шага 0: `echo "  воркеры живы (pgworker :8080, kafkaworker :8082, valkeyworker in-compose)"`.

2) Шаг 3 (посредине kafka-условия, после `for s in worker_loop_ticks_total …`) —
   серия самонаблюдения valkey (retry, живой воркер):

```bash
# valkey-серия (t05): пустой домен = консервативный успех тика — серия обязана
# быть в TSDB при живом воркере (отличие от условной kafka-серии).
valkey_found=""
for i in $(seq 1 30); do
  curl -fsS --data-urlencode "query=valkey_collector_last_success_timestamp_seconds" "$PROM/api/v1/query" \
    | jq -e '.data.result | length > 0' >/dev/null 2>&1 && { valkey_found=1; break; }; sleep 2
done
[ -n "$valkey_found" ] \
  || { echo "❌ серия valkey_collector_last_success_timestamp_seconds не найдена в TSDB (жив valkeyworker? шаг 0)"; exit 1; }
```

3) Шаг 4: `[ "$rules" -ge 8 ]` → `[ "$rules" -ge 11 ]`; тексты `"< 8"` → `"< 11"`
   (8 существующих + 3 valkey).
4) Шаг 5: `[ "$ds" -ge 3 ]` → `[ "$ds" -ge 4 ]`; тексты `"< 3"` → `"< 4"`.
   Scrape-джоба `valkeyworker` попадает в all-up-гейт шага 2 автоматически (джоба
   в prometheus.yml) — шаг 2 не менять.

- [x] **Шаг 5. Синтаксис-гейты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-valkey-metrics
bash -n dev-stand/adminpanel/checks/65-metrics.sh
jq -e . dev-stand/adminpanel/metrics/grafana/dashboards/valkey.json >/dev/null && echo "json ok"
python3 -c "import yaml,sys; yaml.safe_load(open('dev-stand/adminpanel/metrics/prometheus/prometheus.yml')); yaml.safe_load(open('dev-stand/adminpanel/metrics/prometheus/rules.yml')); print('yaml ok')"
```
Ожидание: `json ok`, `yaml ok`, bash -n молчит. (Если python3/yaml недоступны —
пропустить yaml-гейт с пометкой, полная проверка на стенде в задаче 8.)

- [x] **Шаг 6. Коммит**

```bash
git add dev-stand/adminpanel/metrics/prometheus/prometheus.yml \
        dev-stand/adminpanel/metrics/prometheus/rules.yml \
        dev-stand/adminpanel/metrics/grafana/dashboards/valkey.json \
        dev-stand/adminpanel/checks/65-metrics.sh
git commit -m "feat(stand): мониторинг valkey t05 — scrape-джоба valkeyworker, 3 алерта, дашборд valkey.json, чек 65 (11 алертов / 4 дашборда / серия last_success)"
```

---

### Задача 8. Мерж-гейт (Ф6): прогоны, E2E Release, стенд, roadmap-тег

**Вход:** задачи 1–7 закоммичены.
**Выход:** зелёные прогоны (юниты, интеграции, docker-E2E Release, чек 65 на полном
стенде), тег `t05-valkey-metrics` снят из roadmap мерж-коммитом; ветка готова к
ревью и мержу (по гейтам dev-flow — мерж в main только по явной команде).
**Проверка:** команды шагов; все — зелёные.
**Связь со spec:** §7 Ф6, §9, §10 (все критерии).

⚠️ Правила прогона (AGENTS.md): после КАЖДОЙ серии — финальная строка прогона и
зачистка docker-остатков (`vwk-*`, сети); серии не накладываются; перезапуск
упавших тестов «для выяснения» запрещён — только анализ логов.

- [x] **Шаг 1. Полный прогон юнитов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-valkey-metrics
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~UnitTests"
```
Ожидание: все зелёные (ValkeyWorker.UnitTests + KafkaWorker/PgWorker/Shared не сломаны).
Зачистка: юниты docker не поднимают (RespStub/etcd-in-memory) — проверить
`docker ps -a --filter name=vwk- -q` пуст.

- [x] **Шаг 2. Интеграции ValkeyWorker (docker) + зачистка**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug \
  --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"
docker ps -a --filter "name=vwk-" -q | xargs -r docker rm -f
docker network prune -f >/dev/null
```
Ожидание: зелёные (включая Api/MetricsApiTests, Api/MetricsDomainTests,
Valkey/MetricsCollectorTests, E2e — skip без PGW_TEST_DOCKER).

- [x] **Шаг 3. Docker-E2E ValkeyWorker на свежем Release (мерж-гейт AGENTS.md)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"
docker ps -a --filter "name=vwk-" -q | xargs -r docker rm -f
docker network prune -f >/dev/null
```
Ожидание: PASS (Release собирается тестом сам, инкрементально; `PGW_TEST_E2E_NOBUILD`
не ставить — бинарь обязан быть свежим, урок t09).

- [x] **Шаг 4. Полный стенд + чек 65**

```bash
dev-stand/images/pull-images.sh
dev-stand/adminpanel/checks/00-up.sh
dev-stand/adminpanel/checks/65-metrics.sh
```
Ожидание: чек 65 зелёный — все scrape-джобы up (вкл. `valkeyworker`), серия
`valkey_collector_last_success_timestamp_seconds` в TSDB, rules ≥ 11, дашборды ≥ 4,
алерт-симуляция ServiceDown жива. Ручные проверки (критерии §10.2–10.3, по
необходимости — сообщить пользователю результат):
- `GET /metrics` живого воркера стенда содержит все 15 имён при живом демо-кластере:
  `docker compose -f dev-stand/adminpanel/docker-compose.yml exec -T valkeyworker curl -fsS --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key https://localhost:8080/metrics | grep -c '^valkey_'` → ≥ 15;
- Grafana (`http://localhost:3000`, admin/admin) — дашборд valkey с данными;
- симуляция `ValkeyCollectorStalled`: `docker stop vwk-demo-node1` → алерт в
  Prometheus → `docker start vwk-demo-node1` (восстановление).

- [ ] **Шаг 5. Roadmap: снять тег t05 (правило мерж-коммита)**

В `arch/roadmap/valkey.md`: удалить пункт `t05-valkey-metrics` целиком; в строке
«Порядок: канон → воркер → панель → библиотека Puzzle → метрики» убрать «→
метрики» (порядок остаётся про остальные задачи). Коммит этой правки — мерж-коммит
в main (правило AGENTS.md roadmap / AGENTS.base; если механизм мержа не даёт —
последним коммитом ветки, согласовав на гейте ревью):

```bash
git add arch/roadmap/valkey.md
git commit -m "docs(roadmap): тег t05-valkey-metrics снят — задача слита (мерж t05-valkey-metrics)"
```

- [x] **Шаг 6. Self-review диффа против критериев §10**

```bash
git log --oneline main..HEAD
git diff main...HEAD --stat
git diff main...HEAD -- src/ValkeyWorker.App/Program.cs src/ValkeyWorker.App/Options.cs
```
Чек-лист: (1) arch/18 §2.6/§4.2/§5/§6/§8 + arch/21 чист от устаревших упоминаний
t05-коллектора — §7 ссылается на arch/18 §4, блок «Границы» без пункта про
коллектор, «урок инцидента t05» (kfw-net, §2) на месте (spec §10.1); (2) `/metrics`
15 имён; (3) стенд: джоба up, 4 дашборда, 11 алертов; (4) прогоны зелёные; (5)
чек 65 после полной И частичной предыстории; (6) дифф кода воркера — только новые
`ValkeyMetrics*` + точечные вставки Program.cs/Options.cs/appsettings.json;
etcd-контракт (arch/20) и AdminPanel не тронуты (`git diff main...HEAD -- src/AdminPanel.* src/Shared.Metrics` пуст).

---

## Резюме соответствия план ↔ spec

| Spec | Задача |
|---|---|
| §6 п.1–2 (arch-first; §6.2 — arch/21 в ДВУХ местах: §7 + блок «Границы», kfw-net не трогать) | 1 |
| §4.1 InfoAllAsync + парсер | 2 |
| §4.2 ValkeyMetricsState; §3 словарь 15 серий | 3 |
| §4.3–§4.5 коллектор, опции, DI, фейки | 4 |
| §9 Integration: фиксация фактических имён; пустой домен | 5 |
| §9 Integration: живая/остановленная нода | 6 |
| §5.1–§5.4 стенд (scrape/rules/dashboard/чек) | 7 |
| §7 Ф6, §10: прогоны, E2E Release, стенд, roadmap-тег | 8 |

Незакрываемые планом намеренно (вне скоупа spec §8): etcd-контракт arch/20,
AdminPanel, рефакторинг Shared.Metrics, TLS к нодам (t06), управление репликами.
