# Plan: AdminPanel — закрытие etcdworker-части по отчёту 06 (полная ревизия + хвосты)

> **Исполнителям:** план двухуровневый. ШАГ выполняет сильная модель (субагент coder;
> при явной просьбе пользователя — main-агент). ПОДПУНКТ (если есть в шаге) —
> самодостаточное задание для слабой модели (субагент coder-flash БЕЗ контекста
> проекта): получает только текст подпункта и путь к файлу. Всё, что не вынесено
> в подпункты (ревью результатов подпунктов, документация, отладка, прогоны),
> делает сильная модель. Шаги отмечаются чекбоксами `- [ ]`.

**Цель:** закрыть хвосты etcdworker-части панели: `GET /api/config` в EtcdWorker,
классификация managed/external по peer-URL, события etcd в ленте `/events`, тесты
пробелов (D1–D3), чек стенда `44-etcdworker.sh`, актуализация канона arch/ и
практик docs/adminpanel, отчёт 06 — по его же чек-листу.

**Спец:** `docs/superpowers/2026-11-01-adminpanel-etcdworker/spec.md` (в этом же
каталоге; все решения развилок — spec §4, применяются ниже буквально).

**Стек:** .NET 10 (`TreatWarningsAsErrors=true`, `Nullable=enable`), xUnit +
FluentAssertions, React/Mantine + TanStack Query + vitest, bash+jq-чеки стенда.

## Глобальные ограничения (из spec §2/§6 — обязательны для каждого шага)

- arch-first: правка контракта — сначала `arch/`, затем код, тем же коммитом.
- Не трогать работающую реализацию spec §3 (чтение/алерты/поллер/UI-грань/прокси);
  бюджеты и таймауты любых подсистем не менять; P/Invoke запрещён.
- Панель в etcd-домен не пишет; `GET /api/config` — read-only, без секретов.
- События etcd — только на данных уже читаемого `/api/etcd/domain`; новых
  etcd-чтений для ленты НЕ вводить. Чек 44 — read-only, без `PUT /api/etcd/config`.
- Доки — по-русски, идентификаторы — по-английски; тесты — с AAA-комментариями.
- В docs/arch — только текущее состояние, без исторических пассажей («после t32»
  в отчёте 06 остаётся только как указатель на канон arch/25).
- Все команды с длинным выводом — лог целиком в файл (`cmd > /tmp/... 2>&1`),
  чтение из файла. Тесты: после КАЖДОЙ серии — зачистка контейнеров/сетей.

## Порядок исполнения

Шаги идут в порядке номеров (фазы spec: A=1–2, B=3–4, C=5–7, D=8–10, E=11–12,
F=13, G=14). Внутри шага: сперва подпункты (если есть), затем ревью и проверка
шага сильной моделью, затем коммит.

---

### ШАГ 1. Канон arch/25 §1.1: эндпоинт `GET /api/config`

**Вход:** worktree чист; spec §5-A1 (состав групп — решение развилки §4-7).

**Действие (сильная модель, без подпунктов — правка канона):**
в `arch/25-etcdworker.md` §1.1, в таблицу «Метод+путь» (после строки
`| POST /api/restart | graceful self-stop инстанса (паттерн arch/14 §1.1) |`)
добавить строку:

```markdown
| `GET /api/config` | эффективный конфиг установки (read-only; on-demand прокси панели — adminpanel/02 §2.3.5; та же mTLS-грань `/api`): тело-объект `source:"env"` + группы camelCase (фактические поля POCO Options; применяется рестартом воркера): `thresholds{nodeBootSec,nodeDeadSec,healthIntervalSec}`, `loops{scanIntervalSec,keepaliveSec,watchdog{enabled,multiplier,checkIntervalSec}}`, `snapshots{intervalMin,minIntervalSec,maintenanceIntervalMin,retentionFiles,retentionObjects,retryIntervalSec,timeoutSec}`, `docker{mode,network,advertisedHost,portRange{from,to},images{etcd}}`. Наружу НЕ выходят: Etcd-endpoints, секция Api/TLS, Docker Tls/Ssh, Snapshots.Dir → 200 `application/json` |
```

Состав групп — дословно из spec §4-7/A1; в группы НЕ входят `Loops.ErrorDelayMs`,
`Watchdog.StopDelaySec`, `Docker.Hosts/SwarmManager/Tls/Ssh`, `Snapshots.Dir/S3`
(это зафиксировано формулировкой списка полей выше).

**Выход:** arch/25 §1.1 описывает `GET /api/config` с точным составом тела.

**Проверка:** `grep -c 'GET /api/config' arch/25-etcdworker.md` → `1`; в строке
присутствуют все 4 группы (`thresholds`, `loops`, `snapshots`, `docker`) и
запретительная фраза про Etcd-endpoints/Api/TLS/Tls/Ssh/Dir.

**Связь со spec:** §5-A1, §4-развилка 2/7; критерий приёмки 1.

**Коммит:** `git add arch/25-etcdworker.md && git commit -m "arch/25: GET /api/config effective config endpoint"`.

---

### ШАГ 2. Канон arch/adminpanel: 02 (§2.3.5), 03 (§2, §3, §3.7), 04 (стенд)

**Вход:** ШАГ 1 закоммичен; spec §5-A2–A7.

**Действие (сильная модель, без подпунктов — правки канона; все тексты заданы):**

2.1. `arch/adminpanel/02-etcd-contract.md`, §2.3.5 «On-demand обращения…»:
заменить «(контракт тела — arch/14/16/21/24 §1.1:» на
«(контракт тела — arch/14/16/21/24/25 §1.1:» (добавлен arch/25).

2.2. Там же, §2.3.5 «`/etcd/` + `/etcdworker/` — домен EtcdWorker»: ссылки на
мутацию состава ведут на §9.14 (фактический заголовок «9.14. Мутация состава
etcd-контура»; §9.13 — restore-заявка). Три точечные замены:
- «воркера (§9.13); в etcd домена не пишет ничего.» → «воркера (§9.14); в etcd домена не пишет ничего.»
- «мутация — API воркера (§9.13), панель читает» → «мутация — API воркера (§9.14), панель читает»
- «источник URL для мутации состава §9.13;» → «источник URL для мутации состава §9.14;»

(устранение внутреннего расхождения канона — рамка spec §1 «устранение любых
расхождений»; остальные «§9.13» в файле — про restore, корректны, НЕ трогать.)

2.3. `arch/adminpanel/03-panels.md` §2, блок `OverviewDto` → `backups{…}`:
фактический текст канона — строки «…usedBytes, quotaBytes?, etcdSnapshots{enabled,»
и следующая « state}?} (из S3StorageInfo…», где завершающий `}` закрывает
`backups{`. Заменить эти ДВЕ строки (целиком) на ОДНУ:

```text
 usedBytes, quotaBytes?, etcdSnapshots{state}?} (из S3StorageInfo + BackupStorageInfo + EtcdSnapshots;
```

(продолжение строки после `EtcdSnapshots;` в каноне не менять). Поле `enabled`
убрано — arch/25 §5: выгрузка безусловна; код уже без него (spec §4-6).
ВНИМАНИЕ: закрывающая скобка `}` после `?` ОБЯЗАНА сохраниться — она закрывает
`backups{`; замена только внутренностей `etcdSnapshots{…}` разбалансирует DTO.

2.4. `arch/adminpanel/03-panels.md` §3.7: заменить «Страница `/workers` — карточки
PgWorker, KafkaWorker, ValkeyWorker и\nS3Worker (02 §9.9). В каждой:» на
«Страница `/workers` — карточки PgWorker, KafkaWorker, ValkeyWorker,\nS3Worker и
EtcdWorker (02 §9.9). В каждой:»; в пункт «**Координация** (read-only):» после
«— из `coordination` ответа `/api/workers`.» дополнить: « Блок — домены
pg/kafka/valkey/s3; у EtcdWorker блока нет: координационные ключи `/etcdworker/*`
вне панельного контракта (02 §2.3.5) — блок скрыт.»

2.5. `arch/adminpanel/03-panels.md` §3, строка таблицы «**etcd**»: заменить
«members (+лидер; пометки managed/external — managed по префиксу имени `etcdw`,
arch/25 §2)» на «members (+лидер; пометки managed/external — managed по
peer-URL-паттерну `http://etcdw-node-<k>:2380`, arch/25 §3.1; unstarted-learner
без имени классифицируется по peer-URL)».

2.6. `arch/adminpanel/03-panels.md` §3, строка «**События**»: в перечне
«(диффы prev→next ответов polling'а: overview/clusters/cluster-details/ha/
kafka/valkey/s3/backups-storage)» дополнить `/etcd-domain`; после «(заявка:
появление = «поставлена», исчезновение = «исполнена или отвергнута»);» вставить:

```markdown
события etcd-домена: state-change/appeared/gone managed-узла (`etcdw-node-<k>`), state-change статуса DR-слепков (OK↔FAILED), state-change декларации «поддерживать N» (null↔N, N→N', null отображается «—»); эмитятся, пока опрашивается запрос etcdDomain (страница /etcd открыта) — общая модель ленты «живёт в открытой вкладке»;
```

2.7. `arch/adminpanel/04-local-stand.md`:
- §1, фраза «воркеры (стендовые `kafkaworker`/`valkeyworker` профилями +
  deploy-контур `pgworker`/`s3worker` ×2)» → «… deploy-контур
  `pgworker`/`s3worker`/`etcdworker` ×2)».
- Таблица «Контейнер | Внутри | На хосте» §1: после строки `vwk-<C>-node<k>` добавить:

```markdown
| `deploy-etcdworker-1/-2` (deploy-контур ×2) | 8080 (API mTLS) | 8087/8088 (env `ETCDW_API_HOST_PORT/2`) |
```

- §2, после подраздела 2.4 добавить подраздел:

```markdown
### 2.5. `etcdworker` (deploy-контур ×2)

Сервис deploy-compose `etcdworker`/`etcdworker-2` (образ publish-COPY
`etcdworker:dev`, собирает `00-up.sh` на хосте; env — якорь `x-etcdworker-env`):
оркестратор etcd-контура (arch/25). Стендовый контур `as-etcd-1/2/3` для него
ВНЕШНИЙ: ключа `/etcd/config` нет — режим «только наблюдение/слепки»
(arch/25 §3; `/api/etcd/domain` отдаёт `declaredNodes:null`). HTTP API — mTLS
(порты 8087/8088, `ETCDW_API_HOST_PORT/2`), дискавери — lease-ключи
`/etcdworker/api/`; стенд поднимает 2 инстанса.
```

- §3, таблица чеков: после строки `43-etcd-ha.sh` добавить:

```markdown
| `44-etcdworker.sh` | read-only чек etcdworker-грани: `/api/workers` — ряд etcdworker ≥1 инстанса, health healthy; `/api/etcd/domain` — 200, стендовый инвариант внешнего контура (declaredNodes null, узлов нет, snapshots отсутствует или OK); `/api/etcd/status` — members ≥ 3, все external | без мутаций; чистка — сессионный JAR |
```

- §3, фраза «Полный e2e-прогон — последовательность `00 → 10 → 20 → 30 → 40`» →
  «`00 → 10 → 20 → 30 → 40 → 43 → 44`» (синхронно с docs/05 — ШАГ 12).

**Выход:** канон adminpanel соответствует фактической/целевой реализации
(spec §5-A2–A7).

**Проверка:** `grep -n '§9.13' arch/adminpanel/02-etcd-contract.md` — в §2.3.5-домене
ссылок не осталось (остались только про restore: строка шапки «restore-заявка… (§9.13)»);
`grep -c 'etcdSnapshots{enabled' arch/adminpanel/03-panels.md` → `0` (легитимные
`enabled` других секций — например `watchdog{enabled,…}` — не считаются);
`grep -c 'etcdSnapshots{state}?}' arch/adminpanel/03-panels.md` → `1` (скобка
`backups{` закрыта);
`grep -n '44-etcdworker' arch/adminpanel/04-local-stand.md` → 1 строка (таблица) + упоминание порядка.

**Связь со spec:** §5-A2–A7, §4-5/6/9/10; критерий приёмки 1.

**Коммит:** `git add arch/adminpanel/ && git commit -m "arch/adminpanel: etcdworker domain contract, panels, stand canon"`.

---

### ШАГ 3. Код EtcdWorker: маппер + эндпоинт `GET /api/config`

**Вход:** ШАГ 1 (канон) закоммичен; spec §5-B1/B2.

**Действие (сильная модель):** ревью результатов подпунктов 3.1–3.2; сборка.
Подпункты:

#### Подпункт 3.1 (слабая модель): создать файл `src/EtcdWorker.App/Api/Operations/EffectiveConfig.cs`

Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/EtcdWorker.App/Api/Operations/EffectiveConfig.cs`
с РОВНО таким содержимым (файл новый, других изменений не делать):

```csharp
namespace EtcdWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/25 §1.1 GET /api/config): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd-endpoints, секция Api/TLS, Docker Tls/Ssh,
// Snapshots.Dir наружу не выходят никогда); поле source=env — значения
// применяются рестартом воркера. Панель отдаёт это как есть через on-demand
// прокси GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class EtcdWorkerEffectiveConfig
{
    public static object From(EtcdWorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            nodeBootSec = o.Thresholds.NodeBootSec,
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            healthIntervalSec = o.Thresholds.HealthIntervalSec,
        },
        loops = new
        {
            scanIntervalSec = o.Loops.ScanIntervalSec,
            keepaliveSec = o.Loops.KeepaliveSec,
            watchdog = new
            {
                enabled = o.Loops.Watchdog.Enabled,
                multiplier = o.Loops.Watchdog.Multiplier,
                checkIntervalSec = o.Loops.Watchdog.CheckIntervalSec,
            },
        },
        snapshots = new
        {
            intervalMin = o.Snapshots.IntervalMin,
            minIntervalSec = o.Snapshots.MinIntervalSec,
            maintenanceIntervalMin = o.Snapshots.MaintenanceIntervalMin,
            retentionFiles = o.Snapshots.RetentionFiles,
            retentionObjects = o.Snapshots.RetentionObjects,
            retryIntervalSec = o.Snapshots.RetryIntervalSec,
            timeoutSec = o.Snapshots.TimeoutSec,
        },
        docker = new
        {
            mode = o.Docker.Mode,
            network = o.Docker.Network,
            advertisedHost = o.Docker.AdvertisedHost,
            portRange = new
            {
                from = o.Docker.PortRange.From,
                to = o.Docker.PortRange.To,
            },
            images = new
            {
                etcd = o.Docker.Images.Etcd,
            },
        },
    };
}
```

Класс `EtcdWorkerOptions` уже существует в namespace `EtcdWorker.App` (файл
`src/EtcdWorker.App/Options.cs`) — дополнительных using не нужно.

#### Подпункт 3.2 (слабая модель): добавить эндпоинт в `src/EtcdWorker.App/Api/ApiModule.cs`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/EtcdWorker.App/Api/ApiModule.cs`.
Две правки:

1) В списке using-директив сверху файла (строки `using Microsoft.AspNetCore.Builder;` …
`using EtcdWorker.App.Api.Operations;`) добавь строку:

```csharp
using Microsoft.Extensions.Options;
```

2) Найди комментарий `// POST /api/restart — 202 graceful self-stop (arch/14 §1.1).`
и ВСТАВЬ ПЕРЕД ним (между закрывающей `});` эндпоинта PUT /api/etcd/config и этим
комментарием) блок:

```csharp
        // GET /api/config — эффективный конфиг установки (arch/25 §1.1):
        // read-only, mTLS-зона /api, без etcd-чтений и секретов.
        endpoints.MapGet("/api/config", (IOptions<EtcdWorkerOptions> options)
            => Results.Ok(EtcdWorkerEffectiveConfig.From(options.Value)));

```

Других изменений в файле не делать.

**Выход:** `EtcdWorkerEffectiveConfig.From` + `GET /api/config` в API-модуле;
сборка чистая.

**Проверка (сильная модель):** из корня worktree
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/EtcdWorker.App/EtcdWorker.App.csproj -c Release --nologo > /tmp/etcdw-build.log 2>&1; tail -5 /tmp/etcdw-build.log`
— 0 errors/warnings. Ревью: состав групп шага 3.1 совпадает с arch/25 §1.1 (ШАГ 1)
пословно; никаких секций сверх списка.

**Связь со spec:** §5-B1/B2, §4-7; критерий приёмки 2.

**Коммит:** `git add src/EtcdWorker.App/Api/ && git commit -m "etcdworker: GET /api/config effective config (arch/25 s1.1)"`.

---

### ШАГ 4. Юнит-тест EffectiveConfig (B3)

**Вход:** ШАГ 3 закоммичен; spec §5-B3.

**Действие (сильная модель):** ревью подпункта 4.1, прогон тестов. Подпункт:

#### Подпункт 4.1 (слабая модель): создать файл `src/tests/EtcdWorker.UnitTests/App/EffectiveConfigTests.cs`

Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/tests/EtcdWorker.UnitTests/App/EffectiveConfigTests.cs`
с РОВНО таким содержимым:

```csharp
using System.Text.Json;
using EtcdWorker.App;
using EtcdWorker.App.Api.Operations;
using FluentAssertions;
using Xunit;

namespace EtcdWorker.UnitTests.App;

// Эффективный конфиг (arch/25 §1.1 GET /api/config): состав групп ровно по
// канону, значения — из EtcdWorkerOptions; секретные секции (Etcd-endpoints,
// Api/TLS, Docker Tls/Ssh, Snapshots.Dir/Hosts) в тело НЕ попадают.
public class EffectiveConfigTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    // Не-дефолтные значения: перепутанные поля ловятся сравнением.
    private static EtcdWorkerOptions Sample() => new()
    {
        Etcd = new EtcdOptions { Endpoints = ["http://secret-etcd:2379"] },
        Api = new ApiOptions { AdvertiseUrl = "https://secret-api:8080" },
        Thresholds = new ThresholdsOptions { NodeBootSec = 55, NodeDeadSec = 666, HealthIntervalSec = 7 },
        Loops = new LoopsOptions
        {
            ScanIntervalSec = 3,
            KeepaliveSec = 4,
            Watchdog = new Shared.Core.Hosting.WatchdogOptions
                { Enabled = true, Multiplier = 5, CheckIntervalSec = 6 },
        },
        Snapshots = new SnapshotOptions
        {
            Dir = "/secret-snapshots",
            IntervalMin = 11,
            MinIntervalSec = 22,
            MaintenanceIntervalMin = 33,
            RetentionFiles = 44,
            RetentionObjects = 55,
            RetryIntervalSec = 66,
            TimeoutSec = 77,
        },
        Docker = new DockerOptions
        {
            Mode = "Plain",
            Network = "etcdw-net",
            AdvertisedHost = "host.docker.internal",
            PortRange = new PortRangeOptions { From = 18001, To = 18002 },
            Images = new DockerImagesOptions { Etcd = "etcd-img" },
            Hosts = [new DockerHostOptions { Name = "local", Endpoint = "unix:///var/run/docker.sock" }],
        },
    };

    [Fact]
    public void From_CanonicalOptions_MapsAllGroups()
    {
        // Arrange
        var o = Sample();

        // Act
        using var doc = JsonDocument.Parse(
            JsonSerializer.Serialize(EtcdWorkerEffectiveConfig.From(o), Json));

        // Assert — каждое поле равно значению Options (не-дефолты ловят путаницу полей)
        var root = doc.RootElement;
        root.GetProperty("source").GetString().Should().Be("env");

        var thresholds = root.GetProperty("thresholds");
        thresholds.GetProperty("nodeBootSec").GetInt32().Should().Be(55);
        thresholds.GetProperty("nodeDeadSec").GetInt32().Should().Be(666);
        thresholds.GetProperty("healthIntervalSec").GetInt32().Should().Be(7);

        var loops = root.GetProperty("loops");
        loops.GetProperty("scanIntervalSec").GetInt32().Should().Be(3);
        loops.GetProperty("keepaliveSec").GetInt32().Should().Be(4);
        var watchdog = loops.GetProperty("watchdog");
        watchdog.GetProperty("enabled").GetBoolean().Should().Be(true);
        watchdog.GetProperty("multiplier").GetInt32().Should().Be(5);
        watchdog.GetProperty("checkIntervalSec").GetInt32().Should().Be(6);

        var snapshots = root.GetProperty("snapshots");
        snapshots.GetProperty("intervalMin").GetInt32().Should().Be(11);
        snapshots.GetProperty("minIntervalSec").GetInt32().Should().Be(22);
        snapshots.GetProperty("maintenanceIntervalMin").GetInt32().Should().Be(33);
        snapshots.GetProperty("retentionFiles").GetInt32().Should().Be(44);
        snapshots.GetProperty("retentionObjects").GetInt32().Should().Be(55);
        snapshots.GetProperty("retryIntervalSec").GetInt32().Should().Be(66);
        snapshots.GetProperty("timeoutSec").GetInt32().Should().Be(77);

        var docker = root.GetProperty("docker");
        docker.GetProperty("mode").GetString().Should().Be("Plain");
        docker.GetProperty("network").GetString().Should().Be("etcdw-net");
        docker.GetProperty("advertisedHost").GetString().Should().Be("host.docker.internal");
        var portRange = docker.GetProperty("portRange");
        portRange.GetProperty("from").GetInt32().Should().Be(18001);
        portRange.GetProperty("to").GetInt32().Should().Be(18002);
        docker.GetProperty("images").GetProperty("etcd").GetString().Should().Be("etcd-img");
    }

    [Fact]
    public void From_SerializedBody_HasNoSecretKeys()
    {
        // Arrange
        var o = Sample();

        // Act
        var text = JsonSerializer.Serialize(EtcdWorkerEffectiveConfig.From(o), Json);

        // Assert — секретные секции и эндпоинты наружу не выходят (по JSON-тексту)
        text.Should().NotContain("\"tls\"");
        text.Should().NotContain("\"ssh\"");
        text.Should().NotContain("\"endpoints\"");
        text.Should().NotContain("\"apiKey\"");
        text.Should().NotContain("\"dir\"");
        text.Should().NotContain("\"hosts\"");
        text.Should().NotContain("secret");
    }
}
```

#### Действие сильной модели после подпункта

Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.UnitTests -c Release --filter "FullyQualifiedName~EffectiveConfigTests" --nologo > /tmp/etcdw-unit.log 2>&1; tail -5 /tmp/etcdw-unit.log`
— Passed 2. Ревью: тест не тянет новых зависимостей в csproj (xunit/FluentAssertions уже подключены).

**Выход:** юнит-покрытие `GET /api/config` (состав + отсутствие секретов).

**Проверка:** фильтр выше зелёный (2 Passed).

**Связь со spec:** §5-B3; критерий приёмки 2.

**Коммит:** `git add src/tests/EtcdWorker.UnitTests/App/EffectiveConfigTests.cs && git commit -m "etcdworker unit tests: effective config mapping and secrets guard"`.

---

### ШАГ 5. Панель: managed/external по peer-URL + ссылки §9.14

**Вход:** ШАГ 2 закоммичен (arch/03 §3 «etcd» уже описывает peer-URL-паттерн);
spec §5-C1/C2.

**Действие (сильная модель):** ревью подпунктов; typecheck. Подпункты:

#### Подпункт 5.1 (слабая модель): заменить isManagedMember в `frontend/src/pages/EtcdPage.tsx`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/pages/EtcdPage.tsx`.

1) Замени функцию и её комментарий (сейчас строки 28–31: комментарий
`// Managed-члены контура — префикс имени etcdw (arch/25 §2; arch/03 §2).` и
функция `function isManagedMember(name: string | null): boolean { return name !== null && name.startsWith('etcdw'); }`)
на:

```tsx
// Managed-члены контура — peer-URL-паттерн http://etcdw-node-<k>:2380
// (arch/25 §3.1: адресация ТОЛЬКО по именам узлов); имя члена пусто у
// unstarted-learner'а — классификация по peerUrls, не по имени.
function isManagedMember(member: { peerUrls: string[] }): boolean {
  return member.peerUrls.some((u) => /^http:\/\/etcdw-node-\d+:2380$/.test(u));
}
```

2) В функции `MembersCard` замени комментарий (сейчас строки 272–273)
`// Члены кластера etcd: id, имена, URL; лидер — меткой; managed/external —\n// по префиксу имени etcdw (arch/25 §2, arch/03 §2).`
на:

```tsx
// Члены кластера etcd: id, имена, URL; лидер — меткой; managed/external —
// по peer-URL-паттерну http://etcdw-node-<k>:2380 (arch/25 §3.1; бейдж
// показывается всем членам, включая unstarted-learner'а без имени).
```

3) В `MembersCard` в колонке Role (сейчас строки 300–308) замени блок

```tsx
                      {m.name !== null ? (
                        <Badge color={isManagedMember(m.name) ? 'blue' : 'gray'} variant="light">
                          {isManagedMember(m.name) ? 'managed' : 'external'}
                        </Badge>
                      ) : null}
```

на (бейдж — ВСЕМ членам; новый isManagedMember принимает члена целиком):

```tsx
                      <Badge color={isManagedMember(m) ? 'blue' : 'gray'} variant="light">
                        {isManagedMember(m) ? 'managed' : 'external'}
                      </Badge>
```

Других изменений в файле не делать.

#### Подпункт 5.2 (слабая модель): ссылки «02 §9.13» → «02 §9.14»

В файле `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/pages/EtcdPage.tsx`
замени во ВСЁМ файле текст `02 §9.13` на `02 §9.14` (3 вхождения: комментарий в
шапке файла, комментарий над `function ContourCard`, комментарий над
`function MaintainNodesModal`).

В файле `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/api/queries.ts`
замени в комментарии над функцией `putEtcdConfig` текст `02 §9.13` на `02 §9.14`
(1 вхождение; другие `§9.13` в этом файле — про restore-заявку, НЕ трогать).

#### Действие сильной модели после подпунктов

`cd frontend && npm run typecheck` — без ошибок. Ревью: сигнатура
`isManagedMember(m)` нигде больше не вызывается со строкой (в файле вызовы только
в MembersCard); unstarted-learner (name null, peerUrls `http://etcdw-node-<k>:2380`)
теперь получает бейдж managed.

**Выход:** классификация managed/external по peer-URL (arch/25 §3.1);
комментарии кода ссылаются на §9.14.

**Проверка:** `cd /Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend && npm run typecheck` — exit 0;
`grep -c '§9.13' src/pages/EtcdPage.tsx` → 0.

**Связь со spec:** §5-C1/C2 (решение развилки §4-5); критерий приёмки 3.

**Коммит:** `git add frontend/src/pages/EtcdPage.tsx frontend/src/api/queries.ts && git commit -m "etcd page: managed classification by peer-url, contract link s9.14"`.

---

### ШАГ 6. Панель: события etcd-домена в ленте /events

**Вход:** ШАГ 2 (arch/03 §3 «События») и ШАГ 5 закоммичены; spec §5-C3 (решение
развилки §4-3/9: эмитятся, пока опрашивается `etcdDomain` — страница /etcd открыта).

**Действие (сильная модель):** ревью подпунктов, typecheck. Подпункты:

#### Подпункт 6.1 (слабая модель): расширить типы в `frontend/src/events/eventsStore.ts`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/events/eventsStore.ts`.
Определение типа (строки 6–9)

```ts
export type EventEntityKind =
  | 'cluster' | 'shard' | 'node' | 'broker' | 'bucket'
  | 'topic' | 'ticket' | 'heal' | 'evacuation' | 'rotation' | 'instance'
  | 'restore' | 'drill' | 'orphan';
```

замени на:

```ts
export type EventEntityKind =
  | 'cluster' | 'shard' | 'node' | 'broker' | 'bucket'
  | 'topic' | 'ticket' | 'heal' | 'evacuation' | 'rotation' | 'instance'
  | 'restore' | 'drill' | 'orphan' | 'snapshot' | 'config';
```

Других изменений не делать. (`EventDomain` уже содержит `'etcd'` — не трогать.)

#### Подпункт 6.2 (слабая модель): добавить diffEtcdDomain в `frontend/src/events/eventsDiff.ts`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/events/eventsDiff.ts`.

1) В import-блоке типов вверху файла (строки `import type { … } from '../api/dto';`)
добавь строку `  EtcdDomainDto,` между `EtcdDomainDto`-соседями по алфавиту —
после строки `  ClusterSummaryDto,` и перед строкой `  KafkaClusterDto,`
(алфавитный порядок: BackupStorageDto, BucketDto, ClusterDto, ClusterSummaryDto,
EtcdDomainDto, KafkaClusterDto, …).

2) Сразу ПОСЛЕ закрывающей скобки функции `diffS3` (перед комментарием
`// Исход заявки (ticket_outcomes kafka/valkey): любое ИЗМЕНЕНИЕ содержимого`)
вставь:

```ts
// ——— etcd: домен EtcdWorker (02 §2.3.5) — диффы /api/etcd/domain: узлы
// state-change/appeared/gone (entity etcdw-node-<k>), статус DR-слепков и
// декларация «поддерживать N» — state-change (null отображается «—»).
// События эмитятся, пока опрашивается запрос etcdDomain (страница /etcd
// открыта) — общая модель ленты «живёт в открытой вкладке» (arch/03 §3). ———

export function diffEtcdDomain(prev: EtcdDomainDto | undefined, next: EtcdDomainDto): void {
  if (prev === undefined) return;

  // Управляемые узлы /etcd/nodes/<k>: появление/исчезновение/смена state.
  const prevNodes = new Map(prev.nodes.map((n) => [n.k, n]));
  for (const node of next.nodes) {
    const before = prevNodes.get(node.k);
    if (before === undefined) {
      emit({ tsUnix: now(), domain: 'etcd', entityKind: 'node', entity: `etcdw-node-${node.k}`, type: 'appeared' });
    } else if ((before.state ?? '—') !== (node.state ?? '—')) {
      emit({
        tsUnix: now(), domain: 'etcd', entityKind: 'node', entity: `etcdw-node-${node.k}`,
        type: 'state-change', from: before.state ?? '—', to: node.state ?? '—',
      });
    }
  }
  const nextKs = new Set(next.nodes.map((n) => n.k));
  for (const node of prev.nodes) {
    if (!nextKs.has(node.k)) {
      emit({ tsUnix: now(), domain: 'etcd', entityKind: 'node', entity: `etcdw-node-${node.k}`, type: 'gone' });
    }
  }

  // Статус DR-выгрузки слепков (/etcdworker/snapshots): OK↔FAILED↔нет ключа.
  const prevSnap = prev.snapshots?.state ?? null;
  const nextSnap = next.snapshots?.state ?? null;
  if (prevSnap !== nextSnap) {
    emit({
      tsUnix: now(), domain: 'etcd', entityKind: 'snapshot', entity: 'etcd',
      type: 'state-change', from: prevSnap ?? '—', to: nextSnap ?? '—',
    });
  }

  // Декларация «поддерживать N»: null↔N и N→N' (null — «только наблюдение»).
  if (prev.declaredNodes !== next.declaredNodes) {
    emit({
      tsUnix: now(), domain: 'etcd', entityKind: 'config', entity: 'etcd',
      type: 'state-change',
      from: String(prev.declaredNodes ?? '—'), to: String(next.declaredNodes ?? '—'),
    });
  }
}
```

Других изменений не делать.

#### Подпункт 6.3 (слабая модель): ветка etcd-domain в `frontend/src/events/EventsProvider.tsx`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/events/EventsProvider.tsx`.

1) В import-блоке из './eventsDiff' (строки `import { diffBackupsStorage, … };`)
добавь `diffEtcdDomain,` — после `diffClusters,` (итог:
`diffBackupsStorage, diffClusterDetails, diffClusterExtras, diffClusters, diffEtcdDomain, diffHa, diffKafka, diffOverview, diffS3, diffValkey,`).

2) В колбэке подписки найди ветку

```ts
        } else if (id === keyId(['overview'])) {
```

и ВСТАВЬ ПЕРЕД ней новую ветку:

```ts
        } else if (id === keyId(['etcd-domain'])) {
          // etcd-домен (02 §2.3.5): состав контура/DR-слепки/декларация —
          // события, пока запрос etcdDomain опрашивается (страница /etcd).
          diffEtcdDomain(prev as never, next as never);
```

(закрывающая скобка `}` уже есть у следующей ветки). Других изменений не делать.

#### Подпункт 6.4 (слабая модель): фильтры и deep-link в `frontend/src/pages/EventsPage.tsx`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/pages/EventsPage.tsx`.

1) Комментарий над `DOMAIN_OPTIONS` (строки 17–18)

```ts
// Домены ленты = домены, которые диффы реально эмитят (pg/kafka/valkey/s3;
// etcd-домен диффы не эмитят — ключи etcd-контура в ленту не наблюдаются).
```

замени на:

```ts
// Домены ленты = домены, которые диффы реально эмитят (pg/kafka/valkey/s3/etcd —
// etcd-домен из ответа /api/etcd/domain, пока опрашивается страница /etcd).
```

2) В `DOMAIN_OPTIONS` после строки `  { value: 's3', label: 's3' },` добавь:

```ts
  { value: 'etcd', label: 'etcd' },
```

3) В `ENTITY_KIND_OPTIONS` после строки `  { value: 'orphan', label: 'orphan' },`
добавь две строки:

```ts
  { value: 'snapshot', label: 'snapshot' },
  { value: 'config', label: 'config' },
```

4) В `EventRow` в цепочке `fakeKind` найди строки

```ts
            : e.domain === 's3' ? 's3-rotation-pending'
                : 'cluster-incomplete';
```

и замени на

```ts
            : e.domain === 's3' ? 's3-rotation-pending'
              // etcd-домен: все etcd-* kind'ы алерт-маршрутов ведут на /etcd
              // (иначе fallback cluster-incomplete ушёл бы на /clusters/… — 404)
              : e.domain === 'etcd' ? 'etcd-alarm'
                : 'cluster-incomplete';
```

Других изменений не делать.

#### Действие сильной модели после подпунктов

`npm run typecheck` — exit 0. Ревью: `alertRoute('etcd-alarm', …)` → `/etcd`
(проверено по `frontend/src/utils/alertRoutes.ts`); новых etcd-чтений не появилось
(дифф живёт на данных `['etcd-domain']`-запроса, который уже опрашивает EtcdPage).

**Выход:** лента /events эмитит события etcd (node/snapshot/config) с фильтром
домена etcd и deep-link на /etcd.

**Проверка:** `npm run typecheck` — exit 0 (после шага 7 — ещё и vitest).

**Связь со spec:** §5-C3, §4-3/9; критерии приёмки 1 (arch/03) и 3.

**Коммит:** `git add frontend/src/events/ frontend/src/pages/EventsPage.tsx && git commit -m "events feed: etcd domain diffs (nodes, dr snapshots, declaration)"`.

---

### ШАГ 7. Тесты событий (C4)

**Вход:** ШАГ 6 закоммичен; spec §5-C4.

**Действие (сильная модель):** ревью подпункта, прогон vitest. Подпункт:

#### Подпункт 7.1 (слабая модель): добавить кейсы в `frontend/src/events/eventsDiff.test.ts`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/frontend/src/events/eventsDiff.test.ts`.

1) В import-функций из '../events/eventsDiff' добавь `diffEtcdDomain,` (в
алфавитном порядке списка: после `diffClusters,`).

2) В import типов из '../api/dto' добавь `EtcdDomainDto,` (в алфавитном порядке:
после `ClusterSummaryDto,`).

3) В КОНЕЦ файла (после последнего `});`) допиши:

```ts
// ——— etcd: домен EtcdWorker ———

describe('diffEtcdDomain', () => {
  const domain = (overrides?: Partial<EtcdDomainDto>): EtcdDomainDto =>
    ({
      declaredNodes: 2,
      createdUnix: 1,
      nodes: [
        { k: 1, state: 'PROVISIONING', placedOn: 'local' },
        { k: 2, state: 'RUNNING', placedOn: null },
      ],
      snapshots: null,
      ...overrides,
    }) as EtcdDomainDto;

  it('state-change узла PROVISIONING→RUNNING', () => {
    // Arrange
    const prev = domain();
    const next = domain({
      nodes: [
        { k: 1, state: 'RUNNING', placedOn: 'local' },
        { k: 2, state: 'RUNNING', placedOn: null },
      ],
    });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'node', entity: 'etcdw-node-1',
      type: 'state-change', from: 'PROVISIONING', to: 'RUNNING',
    });
  });

  it('appeared/gone управляемого узла', () => {
    // Arrange — узел 3 появился, узел 2 исчез
    const prev = domain();
    const next = domain({
      nodes: [
        { k: 1, state: 'PROVISIONING', placedOn: 'local' },
        { k: 3, state: 'NOT_INITIALIZED', placedOn: null },
      ],
    });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.entityKind}:${e.entity}:${e.type}`)).toEqual([
      'node:etcdw-node-3:appeared',
      'node:etcdw-node-2:gone',
    ]);
  });

  it('статус DR-слепков OK→FAILED', () => {
    // Arrange
    const prev = domain({ snapshots: { state: 'OK', lastUploadedUnix: null, lastObject: null, lastSha256: null, sizeBytes: null, intervalMin: null, error: null } });
    const next = domain({ snapshots: { state: 'FAILED', lastUploadedUnix: null, lastObject: null, lastSha256: null, sizeBytes: null, intervalMin: null, error: 'boom' } });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'snapshot', entity: 'etcd',
      type: 'state-change', from: 'OK', to: 'FAILED',
    });
  });

  it('декларация 0→2 (kind config)', () => {
    // Arrange
    const prev = domain({ declaredNodes: 0 });
    const next = domain({ declaredNodes: 2 });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'config', entity: 'etcd',
      type: 'state-change', from: '0', to: '2',
    });
  });

  it('первый тик (prev нет) и без изменений — молчит', () => {
    // Arrange/Act/Assert — prev undefined → без событий
    let mark = getEvents().length;
    diffEtcdDomain(undefined, domain());
    expect(eventsSince(mark)).toEqual([]);
    // повторный тик без изменений → без событий
    mark = getEvents().length;
    diffEtcdDomain(domain(), domain());
    expect(eventsSince(mark)).toEqual([]);
  });
});
```

Других изменений не делать.

#### Действие сильной модели после подпункта

`cd frontend && npm test > /tmp/fe-test.log 2>&1; tail -10 /tmp/fe-test.log` —
все зелёные (в т.ч. 5 новых кейсов). Ревью: тип `EtcdSnapshotsDto` в кейсах
заполнен всеми полями (null-поля обязательны для TypeScript).

**Выход:** AAA-кейсы diffEtcdDomain: state-change/appeared/gone узла, OK→FAILED
слепков, 0→2 декларации, первый тик молчит.

**Проверка:** vitest зелёный; `npm run typecheck` — exit 0.

**Связь со spec:** §5-C4; критерий приёмки 3.

**Коммит:** `git add frontend/src/events/eventsDiff.test.ts && git commit -m "events tests: etcd domain diff cases"`.

---

### ШАГ 8. D1: юниты PUT /api/etcd/config (прокси)

**Вход:** реализация `PutEtcdConfigCommandHandler` уже в `main`-состоянии worktree
(`src/AdminPanel.Api/Operations/Etcd/EtcdConfigCommands.cs`) — НЕ переделывать;
spec §5-D1.

**Действие (сильная модель):** ревью подпункта, прогон. Подпункт:

#### Подпункт 8.1 (слабая модель): добавить тесты в `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs`.

1) В using-блоке вверху после `using AdminPanel.Api.Operations.Kafka;` добавь:

```csharp
using AdminPanel.Api.Operations.Etcd;
```

2) ПЕРЕД финальной закрывающей фигурной скобкой класса (после последнего теста
`EffectiveConfig_NoLiveInstances_503Exception`) добавь три теста:

```csharp
    // Мутация состава etcd-контура (02 §9.14): прокси PUT /api/etcd/config в
    // API EtcdWorker; оператора нет (декларация, не заявка) — requestedBy null.
    [Fact]
    public async Task PutEtcdConfig_200WithBody_ReturnsDtoAndProxies()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(200, """{"nodes":3}"""),
        };
        var handler = new PutEtcdConfigCommandHandler(api);

        // Act
        var result = await handler.Handle(new PutEtcdConfigCommand(3), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Nodes.Should().Be(3);
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "etcdworker" && c.Method == HttpMethod.Put && c.Path == "/api/etcd/config"
            && c.RequestedBy == null);
        JsonSerializer.Serialize(api.Calls[0].Body, Json).Should().Be("""{"nodes":3}""");
    }

    [Fact]
    public async Task PutEtcdConfig_409ProblemDetails_FailedWithStatus()
    {
        // Arrange — кворум-гвард воркера проксируется 1:1
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Unsafe membership","status":409,"detail":"состав ниже безопасного"}"""),
        };
        var handler = new PutEtcdConfigCommandHandler(api);

        // Act
        var result = await handler.Handle(new PutEtcdConfigCommand(1), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerProblemDetails>()
            .Which.StatusCode.Should().Be(409);
    }

    [Fact]
    public async Task PutEtcdConfig_GatewayUnavailable_FailedWithUnavailableException()
    {
        // Arrange — живых ключей/URL нет → модуль панели ответит 503
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("etcdworker") };
        var handler = new PutEtcdConfigCommandHandler(api);

        // Act
        var result = await handler.Handle(new PutEtcdConfigCommand(3), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }
```

Других изменений не делать.

#### Действие сильной модели после подпункта

`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --filter "FullyQualifiedName~WorkerProxyCommandTests" --nologo > /tmp/ap-unit.log 2>&1; tail -5 /tmp/ap-unit.log`
— все Passed (существующие + 3 новых).

**Выход:** прокси `PUT /api/etcd/config` покрыт юнитами (200/409/503-семантика).

**Проверка:** фильтр выше зелёный.

**Связь со spec:** §5-D1; критерий приёмки 3.

**Коммит:** `git add src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs && git commit -m "panel unit tests: PUT /api/etcd/config proxy handler"`.

---

### ШАГ 9. D2: интеграционный API-тест GET /api/etcd/domain

**Вход:** панельный `EtcdDomainQuery/EtcdDomainQueryHandler` уже реализованы —
НЕ переделывать; spec §5-D2 (паттерн InspectionApiTests, БЕЗ реального etcd).

**Действие (сильная модель):** ревью подпункта, прогон, зачистка. Подпункт:

#### Подпункт 9.1 (слабая модель): создать файл `src/tests/AdminPanel.IntegrationTests/EtcdDomainApiTests.cs`

Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/tests/AdminPanel.IntegrationTests/EtcdDomainApiTests.cs`
с РОВНО таким содержимым:

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// GET /api/etcd/domain (adminpanel/02 §2.3.5): домен EtcdWorker из снапшота —
// паттерн InspectionApiTests (TestSnapshotStore общего хоста, без etcd).
[Collection("api")]
public class EtcdDomainApiTests
{
    private readonly AuthWebFactory _factory;

    public EtcdDomainApiTests(AuthWebFactory factory) => _factory = factory;

    private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task Domain_WithSnapshot_ReturnsDto()
    {
        // Arrange — логин до установки снапшота (окно rate-limiter'а), затем
        // снапшот с доменом: декларация 2, узлы 1 RUNNING/local и
        // 2 PROVISIONING/—, статус DR-слепков OK.
        using var client = await LoginAsync();
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with
        {
            EtcdDomain = new EtcdDomainInfo(2, 1756000000L,
            [
                new ManagedNodeView(1, "RUNNING", "local"),
                new ManagedNodeView(2, "PROVISIONING", null),
            ]),
            EtcdSnapshots = new EtcdSnapshotExportInfo(
                "OK", 1759330000L, "etcd/snapshot-x.db", "abc123", 2048L, 360, null),
        };

        // Act
        using var response = await client.GetAsync("/api/etcd/domain", TestContext.Current.CancellationToken);

        // Assert — 200; декларация/узлы/статус слепков в camelCase-DTO
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("declaredNodes").GetInt32().Should().Be(2);
        dto.GetProperty("createdUnix").GetInt64().Should().Be(1756000000L);
        var nodes = dto.GetProperty("nodes").EnumerateArray().ToList();
        nodes.Should().HaveCount(2);
        nodes[0].GetProperty("k").GetInt32().Should().Be(1);
        nodes[0].GetProperty("state").GetString().Should().Be("RUNNING");
        nodes[0].GetProperty("placedOn").GetString().Should().Be("local");
        nodes[1].GetProperty("k").GetInt32().Should().Be(2);
        nodes[1].GetProperty("state").GetString().Should().Be("PROVISIONING");
        nodes[1].GetProperty("placedOn").ValueKind.Should().Be(JsonValueKind.Null);
        dto.GetProperty("snapshots").GetProperty("state").GetString().Should().Be("OK");
    }
}
```

#### Действие сильной модели после подпункта

Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Release --filter "FullyQualifiedName~EtcdDomainApiTests" --nologo > /tmp/ap-it-domain.log 2>&1; tail -5 /tmp/ap-it-domain.log`
— Passed 1. Зачистка после серии (docker-контейнеры/сети теста — по правилам
AGENTS.md; тест D2 контейнеров не поднимает, проверить отсутствие остатков от
соседних серий; вывод — ЦЕЛИКОМ в файл, без пайпов `| head`/`| grep`):
`docker ps -a > /tmp/docker-ps-check.log 2>&1` и
`docker network ls > /tmp/docker-net-check.log 2>&1` — прочитать файлы и
убедиться в отсутствии тестовых контейнеров/осиротевших сетей).

**Выход:** API-контракт `/api/etcd/domain` покрыт интеграционным тестом.

**Проверка:** фильтр выше зелёный.

**Связь со spec:** §5-D2 (решение §4-8: TestSnapshotStore, не реальный etcd);
критерий приёмки 3.

**Коммит:** `git add src/tests/AdminPanel.IntegrationTests/EtcdDomainApiTests.cs && git commit -m "panel integration test: GET /api/etcd/domain dto"`.

---

### ШАГ 10. D3: интеграционный refresher-кейс домена /etcd/

**Вход:** EtcdDomainParser/SnapshotRefresher уже реализованы — НЕ переделывать;
spec §5-D3 (точечные `EtcdSeed.PutAsync`, `EtcdSeed.Demo` НЕ расширять — §4-8).

**Действие (сильная модель):** ревью подпункта, прогон, зачистка. Подпункт:

#### Подпункт 10.1 (слабая модель): добавить класс в конец `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`

Файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`.
В КОНЕЦ файла (после последней закрывающей фигурной скобки последнего класса)
допиши:

```csharp

// Домен /etcd/ (adminpanel/02 §2.3.5): точечные ключи PutAsync поверх demo-сида
// (Demo НЕ расширяется) — свой класс со СВОИМ контейнером по образцу
// EtcdRoutingMutationTests (order-independent).
public class EtcdDomainRefresherTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Refresher_PicksUpEtcdDomainKeys()
    {
        // Arrange — декларация 2 + узлы 1 (RUNNING/local) и 2 (PROVISIONING);
        // demo-сид уже налит фикстурой контейнера.
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await EtcdSeed.PutAsync(fixture.Endpoint, "/etcd/config",
            """{"nodes":2,"created_unix":1756000000}""", CancellationToken.None);
        await EtcdSeed.PutAsync(fixture.Endpoint, "/etcd/nodes/1/state", "RUNNING", CancellationToken.None);
        await EtcdSeed.PutAsync(fixture.Endpoint, "/etcd/nodes/1/placed_on", "local", CancellationToken.None);
        await EtcdSeed.PutAsync(fixture.Endpoint, "/etcd/nodes/2/state", "PROVISIONING", CancellationToken.None);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — тик успешен; домен в снапшоте: declared=2, два узла,
        // placedOn узла 1 == "local".
        result.IsSuccess.Should().BeTrue();
        var domain = store.Current!.EtcdDomain.Should().NotBeNull().Subject;
        domain.DeclaredNodes.Should().Be(2);
        domain.CreatedUnix.Should().Be(1756000000L);
        domain.Nodes.Should().HaveCount(2);
        var node1 = domain.Nodes.Single(n => n.K == 1);
        node1.State.Should().Be("RUNNING");
        node1.PlacedOn.Should().Be("local");
        domain.Nodes.Single(n => n.K == 2).State.Should().Be("PROVISIONING");
    }
}
```

Других изменений не делать (using-директив файла достаточно: AdminPanel.Core,
AdminPanel.Etcd, Xunit, FluentAssertions уже есть).

#### Действие сильной модели после подпункта

Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Release --filter "FullyQualifiedName~EtcdDomainRefresherTests" --nologo > /tmp/ap-it-dom.log 2>&1; tail -5 /tmp/ap-it-dom.log`
— Passed 1. Контейнер фикстуры поднимется сам; после серии — зачистка по правилам
AGENTS.md: `docker ps -a > /tmp/docker-ps-dom.log 2>&1` и
`docker network ls > /tmp/docker-net-dom.log 2>&1`, прочитать файлы — остатков
быть не должно (ryuk подберёт testcontainers, сети проверить при нуле контейнеров).

**Выход:** refresher-путь домена `/etcd/` покрыт живым etcd-кейсом.

**Проверка:** фильтр выше зелёный.

**Связь со spec:** §5-D3 (решение §4-8); критерий приёмки 3.

**Коммит:** `git add src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs && git commit -m "panel integration test: etcd domain refresher keys"`.

---

### ШАГ 11. E1: чек стенда `checks/44-etcdworker.sh`

**Вход:** ШАГ 2 (arch/04 §3 — строка чека) закоммичен; стенд поднимает
etcdworker ×2 (`00-up.sh`), `/api/workers` отдаёт объект `{workers:[…]}` c
карточкой `worker:"etcdworker"`, `instances[].health == "healthy"`;
`/api/etcd/domain` — camelCase DTO; `/api/etcd/status` — `members[].peerUrls`;
spec §5-E1 (read-only, чистка — только JAR).

**Действие (сильная модель):** ревью подпункта, запуск на стенде (ШАГ 14) —
сам скрипт пишется подпунктом:

#### Подпункт 11.1 (слабая модель): создать файл `dev-stand/adminpanel/checks/44-etcdworker.sh`

Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/dev-stand/adminpanel/checks/44-etcdworker.sh`
с РОВНО таким содержимым:

```bash
#!/usr/bin/env bash
# 44-etcdworker.sh: read-only чек etcdworker-грани панели на стенде
# (arch/adminpanel/04 §3): 1) /api/workers — ряд etcdworker ≥1 инстанса,
# health инстансов healthy (стенд поднимает 2); 2) /api/etcd/domain — 200 и
# стендовый инвариант внешнего контура as-etcd: declaredNodes null («только
# наблюдение»), managed-узлов нет, snapshots отсутствует или OK (отсутствие —
# не fail, значение выводим); 3) /api/etcd/status — members ≥ 3, все external
# (peer-URL без etcdw-node-паттерна). Без мутаций; чистка — только сессионный
# JAR (trap).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"
cleanup() { rm -f "$JAR"; }
trap cleanup EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ adminpanel login (is the stand up? 00-up.sh)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) Воркеры: ряд etcdworker с ≥1 инстансом, health каждого — healthy.
row="$(api /api/workers | jq -e '.workers[] | select(.worker == "etcdworker")')" \
  || { echo "❌ /api/workers: no etcdworker row (discovery keys /etcdworker/api/)"; exit 1; }
echo "$row" | jq -e '(.instances | length) >= 1' >/dev/null \
  || { echo "❌ etcdworker row: no instances"; exit 1; }
echo "$row" | jq -e 'all(.instances[]; .health == "healthy")' >/dev/null \
  || { echo "❌ etcdworker instances not healthy (docker logs deploy-etcdworker-1/-2)"; exit 1; }
echo "  etcdworker: $(echo "$row" | jq '.instances | length') instance(s), all healthy"

# 2) Домен: внешний контур — «только наблюдение»; слепки отсутствуют или OK.
dom="$(api /api/etcd/domain)"
echo "$dom" | jq -e '.declaredNodes == null and (.nodes | length) == 0' >/dev/null \
  || { echo "❌ /api/etcd/domain: stand invariant failed (declaredNodes/nodes must be null/empty)"; exit 1; }
snap="$(echo "$dom" | jq -r '.snapshots.state // "absent"')"
[ "$snap" = "absent" ] || [ "$snap" = "OK" ] \
  || { echo "❌ /api/etcd/domain: snapshots.state=$snap (expected absent or OK)"; exit 1; }
echo "  etcd domain: observation only (declaredNodes null, no managed nodes), snapshots: $snap"

# 3) Контур: members ≥ 3, все external (никаких etcdw-node-<k> peer-URL).
api /api/etcd/status | jq -e '(.members | length) >= 3
  and all(.members[]; all(.peerUrls[]?; (startswith("http://etcdw-node-") | not)))' >/dev/null \
  || { echo "❌ /api/etcd/status: members < 3 or managed-looking member present"; exit 1; }
echo "✓ etcdworker: workers row healthy, domain = observation only, contour members external"
```

После создания файла выполни команду
`chmod +x /Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-etcdworker/dev-stand/adminpanel/checks/44-etcdworker.sh`.

#### Действие сильной модели после подпункта

Ревью: jq-пути соответствуют фактическим DTO (`workers[]`, `instances[].health`,
`declaredNodes`, `nodes`, `snapshots.state`, `members[].peerUrls`); скрипт не
мутирует (только GET) и чистит только JAR. Синтаксис: `bash -n dev-stand/adminpanel/checks/44-etcdworker.sh` — exit 0.

**Выход:** чек 44 существует, исполняемый, read-only.

**Проверка:** `bash -n` exit 0; `ls -l` показывает бит x. Полный прогон — ШАГ 14.

**Связь со spec:** §5-E1 (решения §4-4/10); критерии приёмки 1 (arch/04) и 6.

**Коммит:** `git add dev-stand/adminpanel/checks/44-etcdworker.sh && git commit -m "stand check 44: read-only etcdworker panel facet"`.

---

### ШАГ 12. E2–E4: практики docs/adminpanel

**Вход:** ШАГ 2 (канон arch/04) закоммичен; spec §5-E2/E3/E4.

**Действие (сильная модель, без подпунктов — правки документации; тексты заданы):**

12.1. `docs/adminpanel/05-dev-stand.md` (E2):
- Абзац «Стенд — ОДИН контур…» (строки 7–11): «…kafkaworker и valkeyworker,
  PgWorker + ownS3 (deploy-контур подключается external; `owns3:9000` внутри
  контура, `:9010` — хост-публикация наружу), мониторинг.» → «…kafkaworker и
  valkeyworker, PgWorker + S3Worker + EtcdWorker ×2 + ownS3 (deploy-контур
  подключается external; `owns3:9000` внутри контура, `:9010` — хост-публикация
  наружу), мониторинг.»
- Блок E2E-прогона: цепочку `checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh && checks/60-move-ops.sh \
  && checks/65-metrics.sh` дополнить после `checks/40-live-probes.sh` двумя
  звеньями `&& checks/43-etcd-ha.sh && checks/44-etcdworker.sh` (итог: 00 → 10 →
  20 → 30 → 40 → 43 → 44 → 60 → 65; согласовано с arch/04 §3 — ШАГ 2).

12.2. `docs/adminpanel/02-etcd-snapshot.md` (E3):
- Абзац «Кратко:» после «member/list + alarm → парсеры → `ProbeEnricher` →
  `AlertEngine` → `store.Replace`.» дополнить предложением:
  «Etcdworker-часть тика: пять range-чтений — `/etcdworker/snapshots`
  (точечный), `/etcd/config` (точечный), `/etcd/nodes/`, `/etcdworker/api/`,
  `/workers/api_tls/etcdworker` (точечный); транспортный провал ЛЮБОГО
  KV-чтения роняет весь тик (прежний снапшот сохраняется — значения домена
  переносятся из прежнего). Парсеры — `EtcdDomainParser` (config+nodes) и
  `EtcdSnapshotsParser` (формат arch/25 §5, без `enabled`); health-поллер
  инстансов EtcdWorker — стор `EtcdWorkerHealthStore` (02 §2.3.5).»
- Чек-лист «добавить ключ/поле снапшота», п.3 после «(`Parsing/*`, толерантный
  `JsonValues`).» дополнить: « Пример домена — ключи `/etcd/`
  (config/nodes → `EtcdDomainParser`).»

12.3. `docs/adminpanel/03-probes-alerts.md` (E4):
- Заголовок «## AlertEngine — 47 правил» → «## AlertEngine — 48 правил».
- Таблица групп, строка «etcd-здоровье (7)» → «etcd-здоровье (8)» и в перечень
  добавить `etcd-backup-cluster-missing` (итог: `etcd-unreachable`,
  `etcd-endpoint-down`, `etcd-no-quorum`, `etcd-alarm`,
  `etcd-snapshot-export-failed`, `etcd-snapshot-export-stale`, `snapshot-stale`,
  `etcd-backup-cluster-missing` — сверить фактический набор по коду группы
  правил: etcd-* и SnapshotStale).
- Сверка числа: `ls src/AdminPanel.Core/Alerting/Rules/ | grep -c 'Rule.cs$'`
  должно равняться числу в заголовке (на момент правки — 48; если к моменту
  правки число иное — вписать фактическое и то же число в 03-probes-alerts,
  INDEX (этот пункт) и README (п.12.4)).
- `docs/adminpanel/INDEX.md`: строка таблицы 03 — «`AlertEngine` — 25 правил.» →
  «`AlertEngine` — 48 правил.» (то же фактическое число, что и в 12.3).

12.4. `docs/adminpanel/README.md` (рассинхрон счётчика правил — та же рамка spec
§1 «полная ревизия / устранение расхождений доков», решение согласовано на
ревью): два точечных вхождения «25 правил» → фактическое число из 12.3 (48):
- строка 12: «Patroni REST/SQL (тик 15 c), 25 правил алертов.» → «…, 48 правил
  алертов.»;
- строка 21: «| `src/AdminPanel.Core` | Домен снапшота + `AlertEngine` (25 правил) |»
  → «… `AlertEngine` (48 правил) |».

**Выход:** практики 05/02/03/INDEX/README соответствуют фактической реализации.

**Проверка:** `grep -n 'etcd-backup-cluster-missing' docs/adminpanel/03-probes-alerts.md` → 1 строка;
числа правил в 03-probes-alerts.md, INDEX.md и README.md равны выводу
`ls src/AdminPanel.Core/Alerting/Rules/ | grep -c 'Rule.cs$'`;
`grep -c '25 правил' docs/adminpanel/README.md docs/adminpanel/INDEX.md` → 0 в обоих;
в 05-dev-stand.md цепочка содержит `43-etcd-ha.sh && checks/44-etcdworker.sh`.

**Связь со spec:** §5-E2/E3/E4 + рамка §1 (актуализация практик README — решение
ревью Фазы 4); критерий приёмки 5.

**Коммит:** `git add docs/adminpanel/ && git commit -m "docs/adminpanel: stand composition with etcdworker, tick readings, alert catalogue count"`.

---

### ШАГ 13. F1–F7: отчёт 06-ui-analysis.md по его чек-листу

**Вход:** ШАГИ 1–12 закоммичены (фактическое состояние соответствует); spec §5-F.
Чек-лист отчёта: «Меняешь страницы/навигацию — обнови дерево §4.2 и таблицу §5»,
«Реализована задача — её пункт удаляется из §6».

**Действие (сильная модель, без подпунктов — правки аналитики; тексты заданы):**

13.1. **F1** §1.1, строка «| **etcdworker** (план, t32) | Будущий оркестратор
etcd-контура: состав узлов/кворум, бэкапы etcd (перенос `Snapshots:Export` из
PgWorker), per-install ключ бэкапа etcd. Не реализован | [arch/roadmap/reliability
t32](…) |» → заменить на:

```markdown
| **EtcdWorker** | Оркестратор etcd-контура: поддержание состава (managed-узлы, кворум-гварды, rolling-доводка), DR-слепки etcd → S3 (безусловная выгрузка, ключ `/etcdworker/snapshots`). Единственный писатель `/etcd/`, `/etcdworker/` | [arch/25](../../arch/25-etcdworker.md) |
```

13.2. **F2** §2.3: удалить п.6 («Экспорт etcd-слепков (`PGW_SNAPSHOTS_*`) —
статус виден, управления нет.» — выгрузка безусловна, управления не требуется)
и п.12 («Состав etcd-контура (member add/remove) — runbook; панель показывает
health.» — мутация через панель). Список перенумеровать 1–10.

13.3. **F3** §4.2, блок «ИНФРАСТРУКТУРА», строки etcd-контура:
«etcd-контур /etcd               endpoints/members/alarms; слепки
                                  (выгрузка, статус); после t32 — состав
                                  узлов/кворум (etcdworker)» → заменить на:

```text
  etcd-контур /etcd               endpoints/members/alarms; состав контура
                                  (декларация N, managed-узлы, форма
                                  «Поддерживать N узлов»); DR-слепки
                                  (статус выгрузки)
```

13.4. **F4** §4.3, строка «| etcd-контур | endpoints/members/alarms/raft,
слепки | — (runbook; после t32 — заявки etcdworker) |» → «| etcd-контур |
endpoints/members/alarms/raft, состав контура (декларация, managed-узлы),
DR-слепки | мутация состава «поддерживать N узлов» (02 §9.14; прокси в API
EtcdWorker) |».

13.5. **F5** §5: последнее предложение «Известное остаточное — секция
etcd-контура под выделенный etcdworker (после `t32-etcdworker` —
arch/roadmap/reliability.md).» заменить на: «Секция etcd-контура реализована:
endpoints, members с managed/external-пометками, блок состава контура
(декларация «поддерживать N узлов», managed-узлы), карточка DR-слепков,
alarms; мутация состава — через API EtcdWorker (02 §9.14).»

13.6. **F6** §6: удалить блок «**Будущее (после roadmap t32-etcdworker)**» с
его пунктом; оставить в §6 строку «Открытых задач нет.» (правило отчёта:
реализованный пункт удаляется мерж-коммитом; пустой §6 сохраняется).

13.7. **F7** §3.1: в строке «**бэкапы**» концовку «выгрузка etcd-слепков
`OK/FAILED` — [arch/19](../../arch/19-backups.md)» заменить на «выгрузка
etcd-слепков `OK/FAILED` — EtcdWorker (ключ `/etcdworker/snapshots`,
[arch/25 §5](../../arch/25-etcdworker.md))». §3.2/§3.4 прочитать и убедиться,
что PgWorker как источник статуса слепков больше нигде не назван (сегодня таких
формулировок там нет — менять нечего).

**Выход:** отчёт 06 отражает текущее состояние; «план/не реализован/остаточное» убраны.

**Проверка:** `grep -n 'план, t32\|Не реализован\|после t32\|после roadmap\|остаточное' docs/adminpanel/06-ui-analysis.md` — пусто;
`grep -n 'PGW_SNAPSHOTS' docs/adminpanel/06-ui-analysis.md` — пусто.

**Связь со spec:** §5-F1–F7; критерий приёмки 4.

**Коммит:** `git add docs/adminpanel/06-ui-analysis.md && git commit -m "docs/adminpanel/06: etcdworker section actual state"`.

---

### ШАГ 14. G: прогоны, стенд, зачистка

**Вход:** ШАГИ 1–13 закоммичены.

**Действие (сильная модель, без подпунктов):** все прогоны с логом в файл и
чтением из файла; каждый запуск — наблюдение по правилам AGENTS.base.md §13
(старт ≤15 с, контроль зависаний; тест > 5 мин — онлайн-анализ логов, НЕ
увеличивать таймауты молча).

1. Сборка решения: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/build-release.log 2>&1; tail -5 /tmp/build-release.log` — 0 errors/warnings.
2. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build --nologo > /tmp/unit-ap.log 2>&1; tail -5 /tmp/unit-ap.log`;
   `… src/tests/EtcdWorker.UnitTests … > /tmp/unit-etcdw.log 2>&1` — зелёные.
3. Интеграции (по ОДНОЙ серии, после каждой — зачистка контейнеров/сетей
   `docker ps -a` / `docker network ls`, orphan-сети — `docker network prune -f`
   только как страховка): `dotnet test src/tests/AdminPanel.IntegrationTests -c Release --no-build --nologo > /tmp/it-ap.log 2>&1`;
   затем `… src/tests/EtcdWorker.IntegrationTests … > /tmp/it-etcdw.log 2>&1` —
   зелёные. docker-E2E PgWorker НЕ требуется (код PgWorker не трогаем).
4. Фронт: `cd frontend && npm run build > /tmp/fe-build.log 2>&1` и
   `npm test > /tmp/fe-test.log 2>&1` — зелёные.
5. Стенд: полный прогон по docs/adminpanel/05-dev-stand.md:
   `checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh && checks/43-etcd-ha.sh && checks/44-etcdworker.sh && checks/60-move-ops.sh && checks/65-metrics.sh`
   (лог в файл); вывод чека 44 обязан содержать фактические значения (число
   инстансов, snapshots-статус). После — `checks/90-down.sh -v`.
6. Упавшие тесты ВНЕ области задачи — НЕ чинить: завести пункт в
   `arch/roadmap/bugs.md` с полным составом сведений (фильтр, команда, коммит,
   текст ошибки, пути логов) и продолжить.

**Выход:** все серии зелёные (или падения вне области оформлены в bugs.md);
стенд разобран; зачистка после каждой серии выполнена.

**Проверка:** финальные строки каждого лога Passed/завершены успехом;
после 90-down — `docker ps -a > /tmp/docker-ps-final.log 2>&1`, затем
`grep -cE 'as-|pgw-|s3w-|etcdw-|adminpanel' /tmp/docker-ps-final.log` → 0.

**Связь со spec:** §5-G; критерии приёмки 6–7.

**Коммит:** при правках, вытекающих из прогонов (если нужны), — отдельными
коммитами по фазам; финального «пустого» коммита не делать.

---

## Ревью-фокус (отказо-режимы, не покрытые явно тестами шагов)

1. Unstarted-learner (member без name, peerUrls `http://etcdw-node-7:2380`) —
   обязан получать бейдж `managed`: шаг 5 (код), Г-стенд не проверяет (внешний
   контур) — ручная сверка ревью шага 5 по коду.
2. Первый тик `diffEtcdDomain` (prev undefined) — молчит: покрыт кейсом шага 7.
3. `GET /api/config` при отсутствующих секциях Options (дефолты) — группа
   нулей/дефолтов, не 500: шаг 4 покрывает полный набор полей (все свойства
   обязательны в Options-инициализации, null-секций нет).
4. Ответ `/api/workers` без ряда etcdworker (ключей дискавери нет) — чек 44
   падает с внятной ошибкой (jq-пусто → `❌ no etcdworker row`): шаг 11.
5. `snapshots.state` = FAILED на стенде — чек 44 обязан ПАДАТЬ (не «absent/OK»):
   шаг 11 (`[ "$snap" = "absent" ] || [ "$snap" = "OK" ]`).

## Чего в плане НЕТ (вне scope spec §6 — не делать)

- Никаких правок работающей реализации spec §3 (SnapshotRefresher/алерты/
  поллер/прокси-модуль/DTO-мапперы панели и EtcdWorker).
- Никаких новых etcd-чтений для ленты; никаких мутаций в чеке 44.
- docker-E2E PgWorker; изменений бюджетов/таймаутов; расширения `EtcdSeed.Demo`.
