# t15-prometheus-file-sd — план реализации (дельта 3: единая сеть мониторинга)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/superpowers/2026-10-08-t15-prometheus-file-sd/spec.md` (ревизия 3, ключевой раздел «Дельта 3 (единая сеть)»; план не спорит со spec — executors читают оба).

**Статус ветки:** ревизия 1 исполнена и зелёная; этот план — дельта 3 поверх неё, в той же ветке/Worktree.

## Ревизия 1 — исполнено, зелёный гейт (не исполнять повторно)

Задачи 1–12 ниже ПОЛНОСТЬЮ реализованы, все гейты зелёные (коммиты
`0e2ea09…fa22da74` + `8c2dfb98` + `0627cb92`); их тексты удалены из плана,
канон — git-история и `docs/superpowers/…/` архива задач. Сводка сделанного
(точки, на которые опирается дельта 3):

- [x] Task 1 — arch/18 §2.5/§5.2/§5.4/§6/§8 (словарь `patroni_*` 23 серии,
      джобы `patroni-nodes`/`sd-generator`, паттерн file_sd).
- [x] Task 2 — каркас `src/Metrics.SdGenerator` (опции, Kestrel `/metrics`).
- [x] Task 3 — `TargetMapping` (маппинг `host:patroni`, чистая функция).
- [x] Task 4 — `SdFileWriter` (атомарная запись при diff).
- [x] Task 5 — `SdGeneratorLoop` + failover + самонаблюдение + wiring.
- [x] Task 6 — `docker/Metrics.SdGenerator.Dockerfile` (publish на хосте).
- [x] Task 7 — стенд: сервис `sd-generator` (профиль metrics) + 00-up.sh.
- [x] Task 8 — prometheus.yml (2 джобы) + rules.yml (3 алерта, 21 рул) + чек 65.
- [x] Task 9 — дашборд `pg.json` (4 панели real).
- [x] Task 10 — integration-тесты генератора на живом etcd.
- [x] Task 11 — docker-E2E `E2ePatroniFileSdScenarios` (host-форвардинг-контур
      + `WithExtraHost` — именно это перерабатывает дельта 3).
- [x] Task 12 — мерж-гейт-прогоны ревизии 1 (roadmap-гейт самого трека — Д6).

## Goal (дельта 3)

Метрики реальных Patroni-нод собираются Prometheus'ом по сетевым адресам
ЕДИНОЙ docker-сети контура — и в E2E, и в стенде/поставке: воркер подключает
создаваемые ноды к сети контура (`PgWorker:Docker:ScrapeNetwork`) и дописывает
в portalloc опциональные поля `alias`/`net`; генератор строит таргет
`alias:8008` (запись без `alias` — деградационная advertised-ветка `host:patroni`);
deploy-компоуз объявляет сеть `pgw-metrics` с воркерами (aliases
`pgworker`/`pgworker-2`); стендовый as-prometheus аттачит её как external;
extra_hosts из as-prometheus удаляются полностью; `pgworker-targets.json`
переходит на сетевые адреса; E2E-класс живёт в одной сети окружения без
`host.docker.internal` вообще. Результат: скрейп не зависит от
host-форвардинга ни в одном контуре (spec §1).

## Architecture (дельта 3)

- **Per-node сетевая идентичность в portalloc** (spec «Дельта 3», §3.2):
  конфиг-ключ `PgWorker:Docker:ScrapeNetwork` — имя сети контура (НЕ
  переключатель режима). Задан → движок при создании/ensure канонической ноды
  Ensure-attach'ит её к этой сети поверх основной `pgw-net-<C>` (инвариант
  arch/14 §2.1 не трогается, wal-агенты не подключаются; усыновлённые `object`
  — обходятся, R9), а точки записи portalloc дописывают в запись ноды
  nullable-поля `alias` (полное docker-имя `pgw-<C>-<X>-<n>`, резолвится
  user-defined DNS) и `net` (информационное, для инспекции etcd). Сеть
  контура движком НЕ создаётся и НЕ удаляется; отсутствует — fail-fast
  провижининга (diagnose в ошибке). Пустой ключ — поведение воркера бинарно
  идентично ревизии 1: ни attach, ни полей.
- **Генератор** (spec §3.3): per-node правило в `TargetMapping` — `alias` есть
  → `<alias>:8008` (контейнерный порт Patroni REST, именованная константа);
  нет → `host:patroni` (деградационная ветка: легаси-записи, усыновлённые).
  Фильтры/детерминизм — без изменений; конфигурация генератора не меняется
  (сеть ему не нужна: etcd по compose-DNS, таргеты резолвит Prometheus).
- **Стенд/поставка** (spec §3.4): `deploy/docker-compose.yml` объявляет сеть
  `pgw-metrics` (`name: ${PGW_METRICS_NETWORK:-pgw-metrics}`), pgworker/
  pgworker-2 — в ней с aliases; env `PgWorker__Docker__ScrapeNetwork:
  ${PGW_SCRAPE_NETWORK:-pgw-metrics}`. Стендовый as-prometheus — external-attach
  второй сетью, extra_hosts (`host.docker.internal` и `local`) удаляются
  полностью. 00-up.sh поднимает deploy-контур ДО стендового компоуза (сеть
  создаётся до external-attach), 90-down — в обратном порядке; осиротевшей
  сети не остаётся. SAN серта `pgserver` расширяется `DNS:pgworker-2`
  (Prometheus сверяет имя сетевого таргета с SAN — прецедент t07 в gen.sh).
- **E2E** (spec §3.5): `E2ePatroniFileSdScenarios` — одна сеть окружения
  класса: sd-generator читает etcd по alias `e2e-etcdN:2379`, prometheus
  скрейпит ноды по `alias:8008`, `WithExtraHost`-ов нет (регрессия
  host-форвардинга ловится конструктивно), воркер получает
  `PgWorker__Docker__ScrapeNetwork = <имя сети окружения>`; ассерты — сетевые
  `scrapeUrl` без `host.docker.internal`.

Приёмка: unit → integration (живой etcd) → docker-E2E (сетевой контур +
полный E2e-контур на дефолте поставки — provisioning/portalloc меняются,
канон AGENTS.md) → стенд 00-up/65/90-down. arch-first: правки arch/14 + arch/18
до кода (Д0).

**Tech Stack:** без изменений ревизии 1 (.NET 10, `Nullable=enable`,
`TreatWarningsAsErrors=true`, CPM без новых пакетов, xUnit v3 + FluentAssertions
+ Testcontainers; Prometheus file_sd).

## Global Constraints

- Работа ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t15-prometheus-file-sd` (ветка `feat-t15-prometheus-file-sd`); команды — из корня worktree (пути относительные).
- Базовые правила — [`../../AGENTS.base.md`](../../../AGENTS.base.md) (прочитать до исполнения) и [`../../AGENTS.md`](../../../AGENTS.md): буква приказа, язык русский, AAA-тесты, зачистка серий, телеметрия E2E.
- .NET 10, `TreatWarningsAsErrors=true` — сборка 0 warnings; идентификаторы английские, комментарии/доки русские.
- **Дельта 3 ТРОГАЕТ код воркера** (отличие от ревизии 1): `PgWorker.Core/Model`, `PgWorker.Docker/Drivers/ClusterDriver.cs`, `PgWorker.App/Options.cs`+`Program.cs`, `src/PgWorker.Provisioning/Processes/*` — точечно, по ключу `ScrapeNetwork`; БЕЗ ключа поведение бинарно идентично базе (ни attach, ни полей) — это отдельные юнит-ассерты.
- Контракт etcd расширяется ТОЛЬКО значением существующего ключа `/pgworker/portalloc/<C>` (опциональные поля, пишет воркер; spec §2): ноль новых ключей, генератор по-прежнему только `RangeAsync`.
- НОВЫХ внешних docker-образов и записей в `images.txt` нет; локально собираемые образы в registry `192.168.0.1:5000` НЕ класть.
- Порты в тестах — ТОЛЬКО динамические (`assignRandomHostPort: true` + `GetMappedPublicPort`, окно `E2eEnvironment`); контейнерный порт `8008` — константа контракта ноды (именованная в коде), не host-порт.
- Каждый интеграционный/E2E тест — полный teardown при любом исходе + ассерт чистоты; `[PHASE]`-метки фаз; `MarkFailed()` для упавших; docker-логи до удалений; перезапуск упавших тестов «для выяснения» запрещён.
- Между тестовыми СЕРИЯМИ — зачистка (контейнеры/тома/сети/`docker network prune -f` при осиротевших `pgw-net-*`): серии не накладываются.
- Коммиты: `feat(t15): …` / `test(t15): …` / `docs(t15): …` (описание на русском).
- Ограничения spec §5 действуют целиком (advertised-адреса не меняются, alias не попадает в dsn/endpoints/пробы; `patroni <= 0` не скрейпится; Patroni REST без tls_config; джоба pgworker остаётся mTLS).

## Карта файлов (дельта 3)

| Файл | Ответственность |
|---|---|
| `arch/14-pgworker.md` | Д0: §2.1 — оговорка scrape-сети к per-cluster-инварианту; §2.4 п.2 — опц. поля `alias`/`net` записи ноды; §3.3 таблица — формат ключа portalloc |
| `arch/18-metrics.md` | Д0: §2.5 — семантика таргетов (alias→8008 штатно, advertised — деградационно); §5.2 — сетевые таргеты джобы pgworker, extra_hosts удалён; §5.4 — единая сеть `pgw-metrics`; §6 — приёмка дельты |
| `src/PgWorker.App/Options.cs` | Д1: `DockerOptions.ScrapeNetwork` (string, дефолт пусто) |
| `src/PgWorker.Core/Model/Domain.cs` | Д1: `NodeAddress` + nullable `ScrapeAlias`/`ScrapeNetwork` |
| `src/PgWorker.Core/Model/Portalloc.cs` | Д1: `PortallocEntry` поля `alias`/`net` (симметричная сериализация) + `PortallocIdentity.Decorate` |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | Д1: `PlainClusterDriver` ctor + Ensure-attach в `EnsureNodeAsync` |
| `src/PgWorker.App/Program.cs` | Д1: валидация `ScrapeNetwork` (Swarm — fail-fast), прокидывание в драйвер и процессы |
| `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` | Д1: decorate в `SerializePortAlloc`; перенос scrape-полей в `AdoptRunningContainersAsync` |
| `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` | Д1: decorate на put portalloc |
| `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` | Д1: decorate на merge/repair put |
| `src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs` | Д1: БЕЗ правок кода — preserve через симметрию Parse/Serialize (юнит-доказательство) |
| `src/tests/PgWorker.UnitTests/Model/PortallocTests.cs` + новый `PortallocIdentityTests.cs` | Д1: юниты сериализации/RMW/decorate |
| `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` | Д1: юниты Ensure-attach (FakeEngine) |
| `src/Metrics.SdGenerator/TargetMapping.cs` | Д2: alias-ветка + константа 8008 |
| `src/tests/Metrics.SdGenerator.UnitTests/TargetMappingTests.cs` | Д2: обе ветки, смешанный контур |
| `src/tests/Metrics.SdGenerator.IntegrationTests/SdGeneratorIntegrationTests.cs` | Д2: put с alias → сетевой таргет |
| `deploy/docker-compose.yml` | Д3: сеть `pgw-metrics` + aliases воркеров + env-ключ |
| `deploy/.env.example` | Д3: `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK` |
| `deploy/tls/gen.sh` | Д3: SAN `DNS:pgworker-2` в pgserver-серте (+перегенерация по отсутствию) |
| `dev-stand/adminpanel/docker-compose.yml` | Д3: as-prometheus external-attach, удаление extra_hosts |
| `dev-stand/adminpanel/checks/00-up.sh` | Д3: сетевые pgworker-targets, порядок deploy→стенд, sync env |
| `dev-stand/adminpanel/checks/90-down.sh` | Д3: обратный порядок (стенд → deploy) |
| `dev-stand/adminpanel/checks/65-metrics.sh` | Д3: сетевой ассерт шага 2.1 (логика без изменений) |
| `src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs` | Д4: единая сеть класса, сетевые ассерты |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | Д6: мерж-гейт трека (НЕ в execute) |

## Контракт типов (дельта 3; задачи повторяют сигнатуры)

```csharp
// src/PgWorker.App/Options.cs — DockerOptions
/// <summary>Имя docker-сети контура мониторинга поставки (ревизия 3 t15):
/// задано → движок Ensure-attach'ит канонические ноды к ней (поверх
/// pgw-net-<C>) и точки записи portalloc дописывают alias/net. Пусто —
/// поведение идентично базе (ни attach, ни полей). Не поддерживается в
/// Mode=Swarm (fail-fast старта).</summary>
public string ScrapeNetwork { get; set; } = "";

// src/PgWorker.Core/Model/Domain.cs
public sealed record NodeAddress(
    string Host, NodePorts Ports, string? Object = null,
    string? ScrapeAlias = null, string? ScrapeNetwork = null);

// src/PgWorker.Core/Model/Portalloc.cs
private sealed record PortallocEntry( // JSON: host,pg,patroni,doorman,object?,alias?,net?
    string Host, int Pg, int Patroni, int Doorman,
    string? Object = null,
    [property: JsonPropertyName("alias")] string? Alias = null,
    [property: JsonPropertyName("net")] string? Net = null);

/// <summary>Сетевая идентичность записей portalloc (ревизия 3): при заданном
/// ключе ScrapeNetwork каноническим записям (без object) дописываются
/// alias = pgw-<C>-<X>-<n> (из ключа "<X>/<n>") и net = имя сети. Чистая,
/// идемпотентная; null/пустой ключ — словарь без изменений.</summary>
public static class PortallocIdentity
{
    public static IReadOnlyDictionary<string, NodeAddress> Decorate(
        IReadOnlyDictionary<string, NodeAddress> addresses,
        string cluster, string? scrapeNetwork);
}

// src/PgWorker.Docker/Drivers/ClusterDriver.cs — PlainClusterDriver
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts, DockerEngineFactory factory,
    bool enableDoorman, string nodeImage = "pgworker-node:dev",
    string? advertisedHost = null, IReadOnlySet<string>? pgtuneExclude = null,
    string? scrapeNetwork = null) : IClusterDriver;
// EnsureNodeAsync: заданный ключ → идемпотентный attach контейнера ноды к
// scrapeNetwork (inspect.Networks → NetworkConnectAsync; сети нет — Failed,
// fail-fast); усыновлённые (object) — без attach; NetworkConnectAsync docker
// принимает повторно как no-op — идемпотентность API (DockerEngine.cs:254).

// src/Metrics.SdGenerator/TargetMapping.cs
private sealed record PortallocEntry(
    string Host, int Pg, int Patroni, int Doorman,
    [property: JsonPropertyName("alias")] string? Alias = null);
public static class TargetMapping
{
    /// <summary>Контейнерный порт Patroni REST — константа контракта ноды
    /// (arch/14 §2.1), не host-порт.</summary>
    public const int PatroniRestPort = 8008;
    // Правило per-node: Alias не пуст → $"{Alias}:{PatroniRestPort}";
    // иначе → $"{Host}:{Patroni}" (деградационная ветка). Остальное без изменений.
}
```

---

### Д0: arch-first — контракт единой сети (arch/14 + arch/18, до кода)

**Вход (предусловие):** spec ревизии 3 одобрен; worktree чист (кроме
`docs/superpowers/…t15…/spec.md` + этого plan.md); ревизия 1 зелёная.

**Действие (Files):**

1. Modify `arch/14-pgworker.md`:
   - **§2.1**, абзац «Ноды кластера и wal-агенты подключаются к per-cluster
     docker-сети `pgw-net-<C>`…» — дополнить в конец абзаца оговорку (тон
     окружающего текста):

     ```markdown
     Дополнительно, при заданном `PgWorker:Docker:ScrapeNetwork` (ревизия 3
     t15 — имя docker-сети контура мониторинга поставки) канонические ноды
     Ensure-attach'ятся движком к этой сети (поверх `pgw-net-<C>`, идемпотентно
     по inspect; wal-агенты НЕ подключаются — им scrape-сеть не нужна). Сеть
     контура движком НЕ создаётся и НЕ удаляется (владелец — поставка/сценарий);
     отсутствует при включённом ключе — fail-fast провижининга с diagnose.
     Усыновлённые контейнеры (`object`) не подключаются (R9 — чужой контейнер
     движок не трогает). Назначение сети — скрейп Prometheus по сетевым
     адресам (arch/18 §5.4); клиентская адресация (dsn/endpoints/пробы)
     сетью НЕ меняется (advertised-правило §2.4 п.5).
     ```

   - **§2.4 п.2**, фразу «Запись ноды: `{"host","pg","patroni","doorman"}` +
     опциональное `"object"` (§5 J): …» дополнить (сразу после описания
     `object`):

     ```markdown
     + опциональные `"alias"`/`"net"` (ревизия 3 t15): сетевая идентичность
     ноды для скрейпа — пишутся ТОЛЬКО каноническим нодам при заданном
     `PgWorker:Docker:ScrapeNetwork` (`alias` = полное docker-имя
     `pgw-<C>-<X>-<n>`, глобально уникально, резолвится user-defined DNS сети
     контура; `net` = имя сети контура — информационное, для инспекции etcd).
     Read-modify-write portalloc (remove-shard, adoption-merge, repair) поля
     сохраняет, а не стирает (симметричная сериализация). `alias` НЕ входит в
     dsn/endpoints/панельные пробы (потребитель один — file_sd-генератор
     arch/18 §5.4); записи без полей — легаси/деградационный контур.
     ```

   - **§3.3**, строка таблицы `/pgworker/portalloc/<C>` — формат значения
     дополнить опц. полями: `(+опц. "object" для усыновлённых, §5 J;
     +опц. "alias"/"net" сетевой идентичности при
     PgWorker:Docker:ScrapeNetwork, §2.4 п.2)`.

2. Modify `arch/18-metrics.md`:
   - **§2.5**, последний абзац секции («Таргеты реальных нод — file_sd из
     portalloc… узлы PgWorker скрейпятся по host-публикациям портов Patroni,
     per-cluster сети для этого не нужны») заменить на:

     ```markdown
     Таргеты реальных нод — file_sd из portalloc (§5.2 `patroni-nodes`, §5.4).
     Семантика per-node: запись с `"alias"` → сетевой таргет
     `<alias>:8008` (контейнерный порт Patroni REST; штатная ветка единой
     сети контура — arch/14 §2.4); запись без `"alias"` (легаси-переходный
     контур, усыновлённая нода) → advertised `host:patroni` по host-публикации
     — деградационная ветка: адрес честен из portalloc, тихая потеря наблюдения
     живой ноды хуже запасной ветки той же чистой функции. Фильтр
     `patroni <= 0` (усыптлённые) действует в обеих ветках.
     ```

   - **§5.2**, строку джобы `pgworker` заменить: таргеты — сетевые
     `pgworker:8080`/`pgworker-2:8080` (file_sd `pgworker-targets.json`,
     пишет 00-up.sh; mTLS без изменений — сетевой адрес не меняет транспорт;
     SAN серверного серта покрывает compose-DNS-имена, deploy/tls/gen.sh).
     После таблицы/абзаца про tls_config дополнить одной фразой: extra_hosts
     (`host.docker.internal`, `local`) из as-prometheus удалены — все таргеты
     прометея сетевые (сеть стенда + external `pgw-metrics`, §5.4).
   - **§5.4**, первый абзац заменить на семантику единой сети:

     ```markdown
     Единая сеть контура `pgw-metrics`: объявляет deploy-компоуз поставки
     (`name: ${PGW_METRICS_NETWORK:-pgw-metrics}`) — в ней deploy-воркеры
     PgWorker (aliases `pgworker`/`pgworker-2`), Prometheus (external-attach
     из стендового компоуза) и подключаемые движком кластерные ноды
     (PgWorker:Docker:ScrapeNetwork, arch/14 §2.1/§2.4): скрейп
     patroni-nodes — по `alias:8008` из portalloc, джоба pgworker — по
     сетевым адресам воркеров; host-форвардинг и extra_hosts из контура
     скрейпа удалены. Advertised-ветка (запись без alias) — деградационная:
     легаси-записи/усыновлённые/чужой контур без сети — адрес честен,
     достижимость — зона сетевой политики той поставки (сетевая семантика —
     single-хост-контур «дома»). Граница: Kafka/Valkey-ноды без HTTP
     metrics-эндпоинта наблюдаются доменными сериями коллекторов воркеров
     (§2.3/§2.6); расширение словаря их нод — отдельные задачи.
     ```

   - **§6**, пункт «docker-E2E (file_sd-генератор §5.4)» дополнить: контур
     единой сети окружения класса (воркер с `ScrapeNetwork`, ноды attached —
     inspect подтверждает membership, таргеты up по `alias:8008`, в scrapeUrl
     нет `host.docker.internal`); пункт «E2E-чек стенда» дополнить: таргеты
     pgworker — сетевые, extra_hosts отсутствуют, 00-up/90-down не оставляют
     осиротевшей сети; добавить пункт-строку: дефолт без ключа
     `ScrapeNetwork` не ломает кластерные пути (маркер `Scale_AddEmptyShard`
     на свежем Release).

- [ ] Step 1: Внести правки (вычитка тона окружающего текста; текущее
      состояние без истории).
- [ ] Step 2: Коммит: `git add arch/14-pgworker.md arch/18-metrics.md && git commit -m "docs(t15): дельта 3 — единая сеть мониторинга (arch/14 §2.1/§2.4/§3, arch/18 §2.5/§5.2/§5.4/§6)"`.

**Выход:** контракт дельты в истории до кода; точка отсчёта Д1–Д5.

**Проверка:** `git show --stat HEAD` — один коммит; `grep -c "ScrapeNetwork" arch/14-pgworker.md` ≥ 3; `grep -c "pgw-metrics" arch/18-metrics.md` ≥ 2; в arch/18 §5.2 нет «host-публикация deploy-compose» в строке джобы pgworker.

**Spec:** §3.1 (таблица правок), §2 (arch-first), «Дельта 3».

---

### Д1: Воркер — ключ ScrapeNetwork, поля alias/net, Ensure-attach, decorate точек записи

**Вход:** Д0 закоммичен.

**Действие (Files):**

1. `src/PgWorker.App/Options.cs` — `DockerOptions.ScrapeNetwork` (контракт
   типов; XML-doc по-русски, дефолт `""`).
2. `src/PgWorker.Core/Model/Domain.cs` — `NodeAddress` + nullable
   `ScrapeAlias`/`ScrapeNetwork` в конце параметров (позиционные вызовы
   `new NodeAddress(host, ports, obj)` не ломаются).
3. `src/PgWorker.Core/Model/Portalloc.cs`:
   - `PortallocEntry` — поля `Alias`/`Net` (JSON `alias`/`net`, контракт
     типов); `ToAddress`/`From` переносят оба поля; сериализация уже
     `WhenWritingNull` — записи без полей бинарно те же, Parse симметричен
     (RMW сохраняет).
   - `PortallocIdentity.Decorate` (контракт типов): для каждой записи без
     `Object` при непустом `scrapeNetwork` —
     `ScrapeAlias = $"pgw-{cluster}-{key.Replace('/', '-')}"`,
     `ScrapeNetwork = scrapeNetwork`; записи с `Object` — как есть
     (усыптлённые без alias — деградационная ветка генератора);
     идемпотентно (повторный Decorate не меняет JSON).
4. `src/PgWorker.Docker/Drivers/ClusterDriver.cs` — `PlainClusterDriver`:
   - ctor-параметр `string? scrapeNetwork = null` (контракт типов);
   - `EnsureNodeAsync`, ветка «существующий контейнер, порты совпали» —
     ПОСЛЕ ensure-инварианта кластер-сети и ПЕРЕД `return`: при заданном
     ключе и `!inspect.Value.Networks.Contains(scrapeNetwork)` →
     `engine.NetworkConnectAsync(scrapeNetwork, name, ct)` (ошибка → throw —
     fail-fast; усыновлённый `object` возвращается раньше — attach не
     выполняется);
   - ветка «новый контейнер» — после `StartContainerAsync` при заданном
     ключе: `NetworkConnectAsync(scrapeNetwork, name, ct)` (docker принимает
     повторный connect как no-op — идемпотентность; сети нет — 404 →
     `Result.Failed`, провижининг несёт diagnose в journal/`last_error`).
     Сеть контура движком НЕ создаётся и НЕ удаляется (`EnsureNetworkAsync`
     только для `pgw-net-<C>`; демонтаж гасит membership сам — контейнер
     удаляется).
5. `src/PgWorker.App/Program.cs`:
   - валидация старта рядом с AdvertisedHost: непустой
     `docker.ScrapeNetwork` + `Mode=Swarm` → `ApplicationException`
     («PgWorker:Docker:ScrapeNetwork не поддерживается в Mode=Swarm —
     сетевой attach нод реализован для plain-контейнеров»); обоснование:
     swarm-Ensure — сервисы, второй сети у сервиса нет в MVP, молчаливое
     игнорирование ключа недопустимо (fail-fast по прецеденту AdvertisedHost);
   - `new PlainClusterDriver(..., docker.ScrapeNetwork)` (строка ~254).
6. Процессы — параметр `string? scrapeNetwork = null` в primary-ctor
   (создаются в Program.cs — прокинуть `docker.ScrapeNetwork`):
   - `ProvisioningProcess`: `SerializePortAlloc` →
     `Portalloc.Serialize(PortallocIdentity.Decorate(addresses, cluster, scrapeNetwork))`
     (cluster передать параметром; покрывает обе точки CommitPortAllocAsync);
     в `AdoptRunningContainersAsync` при перезаписи `existing[key] = fact`
     переносить scrape-поля текущей записи: `fact = fact with {
     ScrapeAlias = current?.ScrapeAlias, ScrapeNetwork = current?.ScrapeNetwork }`
     — ОБЯЗАТЕЛЬНО: без переноса record-Equals(current с alias, fact без
     alias) = false на каждом тике → вечная перезапись portalloc;
   - `AddShardProcess` (put на ~242): `Portalloc.Serialize(existing)` →
     Decorate;
   - `AdoptionProcess` (put на ~142 merge и ~328 repair): аналогично
     (142 — фактически object-записи, Decorate их пропускает; 328 — после
     переаллокации detached-нод новые канонические записи ПОЛУЧАЮТ alias);
   - `RemoveShardProcess` — БЕЗ правок кода: preserve достигается
     симметрией Parse→Serialize (юнит Д1 доказывает); меньше касаний — надёжнее.
7. Юниты (AAA, русские комментарии):
   - `src/tests/PgWorker.UnitTests/Model/PortallocTests.cs` — дополнить:
     `Serialize_WithScrapeFields_WritesAliasNet` (JSON содержит
     `"alias":"pgw-c1-s1-s1a"`, `"net":"pgw-metrics"`); `RoundTrip_PreservesScrapeFields`
     (RMW-цикл parse→serialize без Decorate — поля на месте: это и есть
     preserve remove-shard/чужих RMW); `Serialize_WithoutScrapeFields_NoFields`
     (бинарно базе: `DoesNotContain("\"alias\"")`, `DoesNotContain("\"net\"")`);
     `Parse_LegacyJsonWithAlias_StillWorks` (толерантность).
   - Create `src/tests/PgWorker.UnitTests/Model/PortallocIdentityTests.cs`:
     `Decorate_CanonicalNode_AddsAliasNet` (ключ `"s1/s1a"` кластера `c1` →
     alias `pgw-c1-s1-s1a`, net переданный); `Decorate_AdoptedObject_Skipped`
     (object-запись без полей); `Decorate_Idempotent` (двойной Decorate —
     равный JSON); `Decorate_EmptyKey_ReturnsAsIs` (null/"" — словарь не
     тронут, записи без полей).
   - `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` — дополнить
     (FakeEngine пишет вызовы в `Calls`, строки 56–61): 
     `EnsureNode_ScrapeNetwork_ExistingContainer_Attaches` (контейнер есть,
     порты совпали, сетей нет → `Calls` содержит
     `("network-connect", "pgw-metrics:<имя>")`); 
     `EnsureNode_ScrapeNetwork_AlreadyAttached_NoConnect` (inspect.Networks
     содержит сеть → connect НЕ вызван); 
     `EnsureNode_ScrapeNetwork_NewContainer_AfterStart` (контейнера нет →
     create+start+connect); 
     `EnsureNode_NoScrapeNetwork_NoConnect` (ключ null → ни одного
     network-connect сверх кластер-инварианта); 
     `EnsureNode_ScrapeNetworkMissing_FailsFast` (FakeEngine
     `NetworkConnectAsync` → `Result.Failed(new ApplicationException("network pgw-metrics not found"))`
     → `EnsureNodeAsync` `IsSuccess == false`, ошибка содержит имя сети);
     `EnsureNode_AdoptedObject_NoScrapeAttach` (object-контейнер → connect
     не вызван). `NewPlainDriver` — перегрузка с `scrapeNetwork`.

- [ ] Step 1: Юниты (7+4 файлов) → `dotnet test src/tests/PgWorker.UnitTests -c Release --filter 'FullyQualifiedName~Portalloc|FullyQualifiedName~ClusterDriver'` FAIL.
- [ ] Step 2: Реализация (пп. 1–6) → тесты PASS; `dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 3: Дефолт-регрессия: весь `src/tests/PgWorker.UnitTests` зелёный (ключ не задан — поведение базы не сломано).
- [ ] Step 4: Коммит `feat(t15): сетевая идентичность нод — ScrapeNetwork, alias/net в portalloc, Ensure-attach движка`.

**Выход:** воркер подключает ноды к сети контура и несёт alias/net в portalloc; без ключа — бинарно база.

**Проверка:** шаги 1–3 зелёные; `grep -n "ScrapeNetwork" src/PgWorker.App/Options.cs src/PgWorker.Core/Model/Portalloc.cs src/PgWorker.Docker/Drivers/ClusterDriver.cs` — определения на месте; в `RemoveShardProcess.cs` diff пуст.

**Spec:** «Дельта 3» (portalloc-поля, attach, fail-fast), §3.2, §5 (ограничение опциональности), §6.1.

---

### Д2: Генератор — сетевой таргет alias:8008 + advertised-fallback

**Вход:** Д1 закоммичен.

**Действие (Files):**

1. `src/Metrics.SdGenerator/TargetMapping.cs`:
   - DTO `PortallocEntry` + `Alias` (JSON `alias`, контракт типов;
     PropertyNameCaseInsensitive уже включён);
   - константа `PatroniRestPort = 8008` (контракт типов);
   - в `MapKey`: после фильтра `patroni <= 0` — per-node правило:
     `Target = !string.IsNullOrWhiteSpace(entry.Alias)
       ? $"{entry.Alias}:{PatroniRestPort}"
       : $"{entry.Host}:{entry.Patroni}"`;
     фильтры (битый JSON, без shard/node-разделителя, пустой host),
     детерминизм (Cluster, Shard, Node) — без изменений. Пустой `alias`
     (`""`/whitespace) трактуется как отсутствие — advertised-ветка.
2. `src/tests/Metrics.SdGenerator.UnitTests/TargetMappingTests.cs` — дополнить:
   `Map_WithAlias_NetworkTarget` (запись с `"alias":"pgw-c1-s1-s1a"` →
   Target `pgw-c1-s1-s1a:8008`, порты записи host-публикации НЕ участвуют);
   `Map_WithoutAlias_AdvertisedTarget` (та же запись без alias → `h1:8008`
   по host:patroni); `Map_MixedContour_PerNode` (в одном ключе записи с alias
   и без → покомпонентно: alias-запись сетевая, без-alias — advertised);
   `Map_AliasSuspended_Skipped` (`patroni:0` + alias → группы нет, warn не
   вызван); `Map_AliasEmpty_FallsBack` (`alias:""` → advertised);
   `Serialize_NetworkTarget_Format` (JSON-формат группы не изменился).
3. `src/tests/Metrics.SdGenerator.IntegrationTests/SdGeneratorIntegrationTests.cs`
   — дополнить тест `Put_PortallocWithAlias_NetworkTarget`: put
   `/pgworker/portalloc/c2` значения с `"alias":"pgw-c2-s1-s1a"` (host/pg/
   patroni/doorman — произвольные фактические) → `LoopAsync` → true; файл
   содержит `"pgw-c2-s1-s1a:8008"` и НЕ содержит `host:patroni` этой записи;
   лейблы cluster/shard/node на месте. (put/del/свежесть — покрыты базой,
   не дублировать.)

- [ ] Step 1: Юнит-кейсы → FAIL. Step 2: реализация → PASS; `dotnet build src/PgWorker.slnx -c Release` 0 warnings.
- [ ] Step 3: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/Metrics.SdGenerator.IntegrationTests -c Release` — зелёный; зачистка серии (`docker ps -a --filter name=/testcontainers/ -q | wc -l` → 0).
- [ ] Step 4: Коммит `feat(t15): сетевые таргеты генератора — alias:8008, advertised — деградационная ветка`.

**Выход:** генератор штатно строит сетевые таргеты; смешанный контур маппится покомпонентно.

**Проверка:** шаги 1–3 зелёные; `grep -n "PatroniRestPort" src/Metrics.SdGenerator/TargetMapping.cs` — константа именованная (никаких голых `8008` в логике таргета).

**Spec:** §3.3, §6.1 (unit), §6.2 (integration).

---

### Д3: Стенд/поставка — сеть pgw-metrics, external-attach, демонтаж extra_hosts

**Вход:** Д1–Д2 закоммичены (ключ/env осмысленны); docker жив.

**Действие (Files):**

1. `deploy/docker-compose.yml`:
   - корневая секция:

     ```yaml
     # Единая сеть контура мониторинга (t15 ревизия 3, arch/18 §5.4):
     # deploy-проект ВЛАДЕЕТ сетью — в ней deploy-воркеры PgWorker (aliases),
     # Prometheus (external-attach стендового компоуза) и кластерные ноды
     # (attach движком, PgWorker:Docker:ScrapeNetwork). Предсказуемое имя —
     # для внешних потребителей (prometheus external-сеть).
     networks:
       pgw-metrics:
         name: ${PGW_METRICS_NETWORK:-pgw-metrics}
     ```

   - `x-pgworker-env` (общий анкор, получают оба инстанса):

     ```yaml
       # Сеть контура скрейпа (t15 ревизия 3): имя сети, к которой движок
       # Ensure-attach'ит создаваемые ноды; portalloc несёт alias/net,
       # sd-generator строит таргеты <alias>:8008 (arch/14 §2.4, arch/18 §5.4).
       PgWorker__Docker__ScrapeNetwork: ${PGW_SCRAPE_NETWORK:-pgw-metrics}
     ```

     (имя env — `PGW_SCRAPE_NETWORK`; вариант `PGW_SCRAKE_NETWORK` из
     §3.4 spec — опечатка, канон — `PGW_SCRAPE_NETWORK` из приказа
     пользователя и §3.6 spec).
   - сервисы `pgworker`/`pgworker-2` — добавить сети (aliases различаются,
     default сохраняется явно — compose не подключает default при указанном
     `networks:`):

     ```yaml
     networks:
       default: {}
       pgw-metrics:
         aliases: [pgworker]     # у pgworker-2: [pgworker-2]
     ```

     Host-публикации 8080/8083, mTLS-серт, extra_hosts `local`/`host.docker.internal`
     (etcd/advertised — НЕ скрейп) — без изменений.
2. `deploy/.env.example` — блок «Единая сеть мониторинга (t15 ревизия 3)»:

   ```bash
   # Единая сеть контура мониторинга: объявляет deploy-compose (воркеры),
   # external-attach — стендовый Prometheus; ноды подключает PgWorker
   # (ScrapeNetwork). Переименование — синхронно в стендовом METRICS_NETWORK.
   PGW_METRICS_NETWORK=pgw-metrics
   PGW_SCRAPE_NETWORK=pgw-metrics
   ```

3. `deploy/tls/gen.sh` — SAN pgserver-серта дополнить `DNS:pgworker-2` и
   перегенерацией по отсутствию (паттерн t07 того же файла, строки 36–41):
   `if [ ! -f pgserver.crt ] || ! openssl x509 -in pgserver.crt -noout -text 2>/dev/null | grep -q 'DNS:pgworker-2'; then issue pgserver pgworker serverAuth "DNS:pgworker,DNS:pgworker-2,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"; fi`
   — ОБЯЗАТЕЛЬНО: сетевой таргет `pgworker-2:8080` верифицируется прометеем
   против SAN (сегодня серт покрывает только `DNS:pgworker` — чек 65 упадёт
   на шаге 2).
4. `dev-stand/adminpanel/docker-compose.yml`:
   - сервис `prometheus`: УДАЛИТЬ `extra_hosts` ЦЕЛИКОМ (обе записи —
     `host.docker.internal:host-gateway` из ревизии 1 и `local:host-gateway`
     из 0627cb92); добавить сети:

     ```yaml
     networks:
       default: {}
       pgw-metrics: {}   # внешняя сеть контура (deploy-compose) — таргеты
                         # pgworker/pgworker-2/patroni-nodes (t15 ревизия 3)
     ```

   - корневые сети: `pgw-metrics: { name: ${METRICS_NETWORK:-pgw-metrics}, external: true }`.
     sd-generator — БЕЗ изменений (сеть стенда: etcd по compose-DNS, файл в
     volume; таргеты резолвит Prometheus).
5. `dev-stand/adminpanel/checks/00-up.sh`:
   - строки 14–19: `pgworker-targets.json` — сетевые адреса:

     ```bash
     printf '[{"targets": ["pgworker:8080", "pgworker-2:8080"]}]\n' \
       > metrics/prometheus/pgworker-targets.json
     ```

     (host-порты `PGW_API_HOST_PORT*` остаются — advertise/чеки/панель, не скрейп);
   - перенос ПОДЪЁМА deploy-контура ВЫШЕ стендового `compose up` (строка 50):
     блок «deploy/.env sync + `docker compose --env-file deploy/.env up -d
     --build --force-recreate pgworker pgworker-2` (с ретраями порта)»
     переносится из §1b к позиции сразу после publish SdGenerator; причина —
     external-сеть `pgw-metrics` стендового компоуза обязана существовать ДО
     `compose up` (иначе «network declared as external, but could not be
     found»). Проверки healthz×2 и `/pgworker/api/`-ключей ОСТАЮТСЯ в §1b
     (после etcd-кворума — они требуют живой etcd, поднятый стендовым
     компоузом); меняется только момент старта контейнеров deploy;
   - `.env`-sync дополнить ключами `PGW_METRICS_NETWORK`/`PGW_SCRAPE_NETWORK`
     (по образцу `PGW_API_HOST_PORT`, строки 110–120: существующий
     `deploy/.env` не содержит новых ключей после апгрейда — sed/append).
6. `dev-stand/adminpanel/checks/90-down.sh` — после стендового down добавить
   deploy-разбор (обратный порядок: стенд → deploy сносит `pgw-metrics`):

   ```bash
   # t15 ревизия 3: deploy-контур разбирается ПОСЛЕ стендового — compose down
   # deploy-проекта сносит сеть pgw-metrics (её владелец), осиротевшей сети не
   # остаётся. Воркеры deploy больше НЕ переживают 90-down — подъём 00-up.sh.
   if [ -f "$ROOT/deploy/.env" ]; then
     (cd "$ROOT/deploy" && docker compose --env-file .env down ${1:+-v} --remove-orphans) \
       || echo "⚠ deploy down не удался (docker network ls | grep pgw-metrics)"
   fi
   ```

   (`$1 = -v` синхронно сносит и deploy-тома — семантика «стереть данные»).
7. `dev-stand/adminpanel/checks/65-metrics.sh` — ТОЛЬКО усиление ассерта шага
   2.1 (логика чека не меняется): при `pn_total > 0` все таргеты patroni-nodes
   сетевые — `scrapeUrl` без `host.docker.internal` и с портом 8008:

   ```bash
   if [ "$pn_total" -gt 0 ]; then
     bad_net=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[]
       | select(.labels.job=="patroni-nodes")
       | select(.scrapeUrl | contains("host.docker.internal") or (test(":8008/metrics$") | not))] | length')
     [ "$bad_net" -eq 0 ] || { echo "  ❌ patroni-nodes: $bad_net таргетов не сетевые (host-форвардинг/не :8008)"; exit 1; }
   fi
   ```

- [ ] Step 1: Правки семи файлов.
- [ ] Step 2: Статическая валидация: `cd dev-stand/adminpanel && docker compose --profile metrics config -q`; `cd deploy && docker compose --env-file .env.example config -q` (обязательные секреты из example валидны); `bash -n dev-stand/adminpanel/checks/{00-up,90-down,65-metrics}.sh`.
- [ ] Step 3: SAN-гейт: `bash deploy/tls/gen.sh && openssl x509 -in deploy/tls/pgserver.crt -noout -text | grep -q 'DNS:pgworker-2'`.
- [ ] Step 4: Коммит `feat(t15): единая сеть pgw-metrics — deploy-компоуз, external-attach прометея, сетевые pgworker-targets, демонтаж extra_hosts`.

**Выход:** поставка и стенд живут в единой сети контура; host-форвардинг из скрейпа удалён.

**Проверка:** шаги 2–3 зелёные; `grep -c "extra_hosts" dev-stand/adminpanel/docker-compose.yml` — упоминания только у воркеров (as-prometheus без); в `00-up.sh` deploy-up идёт раньше стендового `compose up`; `grep -n "pgworker:8080" dev-stand/adminpanel/checks/00-up.sh` есть.

**Spec:** §3.4, §3.6, «Дельта 3» (порядок подъёма/разбора), §6.5.

---

### Д4: E2E — E2ePatroniFileSd на единой сети окружения класса

**Вход:** Д1–Д3 закоммичены; Release собирается; docker жив.

**Действие (Files):** Modify
`src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs`
(правки ТОЛЬКО сценария; `E2eEnvironment` уже даёт `Net`/`NetName`/
`StartHostAsync(extraEnv)`/`EnsureSdImageAsync`):

1. **Контур мониторинга — единая сеть, БЕЗ host-форвардинга:**
   - у `sdGen` и `prom` УДАЛИТЬ `.WithExtraHost("host.docker.internal", "host-gateway")`
     (регрессия host-форвардинга ловится конструктивно: резолва нет → скрейп
     умер → тест красный);
   - `sdGen` env: вместо `http://host.docker.internal:<etcdPort>` — список по
     числу узлов окружения:
     `for (var i = 0; i < Fx.EtcdEndpoints.Count; i++) … SdGenerator__Etcd__Endpoints__{i} = $"http://e2e-etcd{i + 1}:2379"`
     (алиасы сети окружения — `WithNetworkAliases($"e2e-etcd{i+1}")`,
     E2eEnvironment.cs:352); вычисление `etcdPort` удалить.
2. **Воркер:** `await Fx.StartHostAsync("s1", ct: ct, extraEnv: new()
   { ["PgWorker__Docker__ScrapeNetwork"] = Fx.NetName })` — движок подключает
   ноды к сети окружения, portalloc несёт alias/net.
3. **Ассерты (заменяют host-портовую логику):**
   - `PatroniPortsAsync` → `PatroniAliasesAsync(cluster)`: DTO записи
     дополнить `Alias` (JSON `alias`); ассерт: все 4 записи имеют непустой
     `alias` вида `pgw-<cluster>-…` и `net == Fx.NetName` (доказательство
     Д1-контура в E2E);
   - `PatroniTargetsUpAsync`: `health == "up"`; `scrapeUrl` —
     `http://<alias>:8008/metrics`: `Uri.Host` ∈ aliases, `Uri.Port == 8008`
     (именованная константа сценария, синхрон с `TargetMapping.PatroniRestPort`);
     и НАПРЯМУЮЙ ассерт: ни один `scrapeUrl` (обе джобы: patroni-nodes и
     sd-generator) не содержит `host.docker.internal` — канон ревизии 3;
   - факультативно (cheap, канон чистоты): после провижининга — один
     `docker inspect <имя-ноды>` на подтверждение membership в `Fx.NetName`
     (`NetworkSettings.Networks` содержит) — spec §6.4 «inspect подтверждает
     membership».
4. Канон-минимум серий, демонтаж, teardown, телеметрия — БЕЗ изменений
   (сетевые адреса не меняют ни словарь, ни lifecycle).

- [ ] Step 1: Переработка сценария; `dotnet build src/PgWorker.slnx -c Release` 0 warnings.
- [ ] Step 2: Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2ePatroniFileSd` — зелёный (E2eFixture соберёт Release; фазы — с `[PHASE]`).
- [ ] Step 3: Зачистка серии: `docker ps -a --filter name=pgw- -q | wc -l` → 0; `docker volume ls -q | grep -c pgw-sd` → 0; `docker network ls | grep pgw` → пусто (или только чужие живые прогоны); артефакты `/tmp/pgw-e2e-artifacts-<guid>/` содержат логи prom/sdgen и `patroni-series-fact.txt`.
- [ ] Step 4: Коммит `test(t15): E2E file_sd в единой сети окружения — alias-таргеты, ноль host-форвардинга`.

**Выход:** сквозная приёмка сетевого контура на реальном кластере.

**Проверка:** шаг 2 зелёный; в логе прогона `scrapeUrl`-ассерты прошли на сетевых адресах; в git-diff сценария нет `WithExtraHost`.

**Spec:** §3.5, §6.4.

---

### Д5: Перегон — юниты → интеграции → docker-E2E → стенд

**Вход:** Д0–Д4 закоммичены.

**Действие:** последовательный перегон с зачисткой ПОСЛЕ КАЖДОЙ серии
(дождаться финальной строки прогона; между сериями — контроль
`docker ps`/`networks`, `docker network prune -f` при осиротевших
`pgw-net-*`; фазы > 60 с — `[PHASE]`-отчёт «почему долго»).

- [ ] Step 1: `dotnet build src/PgWorker.slnx -c Release` — 0 errors, 0 warnings.
- [ ] Step 2: Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter 'FullyQualifiedName~UnitTests'` — зелёные. Зачистка-контроль.
- [ ] Step 3: Интеграции генератора: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Metrics.SdGenerator.IntegrationTests` — зелёные; зачистка (testcontainers/ryuk, сети).
- [ ] Step 4: docker-E2E PgWorker на свежем Release — ПОЛНЫЙ контур
  (дельта трогает provisioning/portalloc — канон AGENTS.md):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~PgWorker.IntegrationTests.E2e`
  — зелёный; внутрь входят кейс-маркер `Scale_AddEmptyShard` (дефолт
  E2eEnvironment: ключ ScrapeNetwork НЕ задан → attach нет → деградационный
  дефолт поставки не ломает кластерные пути, spec §4 Д5/§6.6) и сетевой
  `E2ePatroniFileSd`. Зачистка после серии. При ресурсных проблемах хоста —
  разбить на серии (E2ePatroniFileSd отдельно от остальных) с зачисткой
  между ними.
- [ ] Step 5: Стенд: `bash dev-stand/adminpanel/checks/00-up.sh` — полный
  подъём зелёный; затем `bash dev-stand/adminpanel/checks/65-metrics.sh` —
  зелёный, при этом:
  - таргеты джобы pgworker — сетевые `pgworker:8080`/`pgworker-2:8080`, оба up:
    `curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="pgworker") | .labels.instance + ":" + .health]'`
    → `["pgworker:8080:up","pgworker-2:8080:up"]`;
  - `docker inspect as-prometheus --format '{{json .NetworkSettings.Networks}}' | jq 'keys'` содержит сеть pgw-metrics; extra_hosts пуст;
  - шаг 2.1 — patroni-nodes: на стенде кластеров PgWorker нет → file_sd
    пуст (корректно), сетевой ассерт не срабатывает.
- [ ] Step 6: `bash dev-stand/adminpanel/checks/90-down.sh` — зелёный;
  ассерт чистоты сети: `docker network ls --format '{{.Name}}' | grep -x pgw-metrics`
  → пусто (deploy-down снёс); `docker ps --filter name=deploy- -q` → пусто.
  Повторный 00-up → 65 (идемпотентность цикла, сеть пересоздалась).
- [ ] Step 7: `git status` чист; все серии зелёные — ветка готова к Фазе 8 (мерж — только по явной просьбе пользователя).

**Выход:** дельта 3 доказана на всех контурах: юниты, живой etcd, docker-E2E (сетевой + дефолт поставки), стенд (подъём/чек/разбор без сирот).

**Проверка:** шаги 1–6 зелёные; после Step 6 `docker network ls | grep -c pgw-metrics` → 0.

**Spec:** §4 (фазы Д5), §6.3–§6.6.

---

### Д6: Мерж-гейт трека reliability — в execute НЕ выполнять

**Вход:** Д5 зелёный; пользователь одобрил мерж (Фаза 8 dev-flow).

**Действие (Files):** roadmap-гейт ОДНИМ мерж-коммитом с кодом дельты
(правки готовятся на этапе мержа, НЕ в execute этого плана):

- `arch/roadmap/reliability.md` — удалить пункт `t15-prometheus-file-sd`
  (проверить `grep -n t15`: `←`-ссылок на t15 нет);
- `arch/roadmap/reliability-report.md` — строку таблицы «Осталось»
  (`t15-prometheus-file-sd | file_sd для скрейпа реальных нод | P3 | N`)
  перенести в «Сделано в рамках трека»: независимый file_sd-канал наблюдения
  реальных Patroni-нод (характеристика N) — sd-generator читает
  `/pgworker/portalloc/` и пишет таргеты `alias:8008` единой сети контура
  `pgw-metrics` (воркеры+ноды+Prometheus, ScrapeNetwork-attach движком),
  host-форвардинг и extra_hosts из скрейпа удалены во всех контурах
  (стенд/поставка/E2E); advertised-ветка — деградационная для легаси/
  усыновлённых; граница — Kafka/Valkey-ноды на доменных сериях коллекторов;
- сводка N: убрать «метрики реальных нод не собираются», дописать сетевой
  канал; «Открытые разрывы»: убрать «прод-ноды недоскрейпимы (`t15`)»;
- правки arch/14 + arch/18 (Д0) — в этом же мерж-коммите (spec §6.7).

**Выход:** roadmap синхронен мерж-коммиту; исторических пометок нет.

**Проверка (на этапе мержа):** `grep -rn "t15" arch/roadmap/reliability.md` → пусто; в reliability-report.md t15 только в «Сделано».

**Spec:** §6.7.

---

## Self-Review (выполнен при написании)

- **Покрытие спеки:** «Дельта 3»+§3.1→Д0; §3.2→Д1 (ключ, поля, attach,
  decorate, fail-fast, усыновлённые); §3.3→Д2; §3.4/§3.6→Д3 (включая
  продиктованный кодом SAN `DNS:pgworker-2` — без него mTLS-джоба на
  `pgworker-2:8080` падает на верификации серта, прецедент t07 gen.sh);
  §3.5→Д4; §4-фазы→Д0–Д6; §6.1→Д1; §6.2→Д2; §6.3→Д2(интеграция)/Д4;
  §6.4→Д4; §6.5→Д3/Д5; §6.6→Д5 (маркер в полном E2E-контуре — дельта трогает
  provisioning/portalloc, полный прогон каноничен AGENTS.md); §6.7→Д6.
- **Разрешённые детали (обоснования в текстах задач):** имя env
  `PGW_SCRAPE_NETWORK` (опечатка спеки §3.4 опровергнута приказом и §3.6);
  `RemoveShardProcess` без правок (preserve = симметрия Parse/Serialize,
  юнит-доказательство); перенос scrape-полей в `AdoptRunningContainersAsync`
  (иначе record-Equals даёт вечную перезапись portalloc при заданном ключе);
  fail-fast `ScrapeNetwork`+Swarm в валидации старта (прецедент AdvertisedHost);
  90-down разбирает deploy-контур (прямое следствие spec — сеть не сиротеть);
  decorate на adoption-merge 142 — единообразие точек записи при фактическом
  no-op (object-записи).
- **Placeholder-скан:** TBD/TODO нет; все сигнатуры — в контракте типов.
- **Консистентность:** константа 8008 именована (`TargetMapping.PatroniRestPort`,
  дубль в сценарии E2E — DTO генератора независим от Core); `WhenWritingNull`
  уже в `Portalloc.Json` — «бинарно те же» доказуемо; FakeEngine уже пишет
  network-connect в `Calls`; `EtcdEndpoints.Count`/`NetName`/`extraEnv` уже
  публичны в `E2eEnvironment` — Д4 не требует его правок; внешние образы и
  `images.txt` не тронуты; тесты — динамические порты, teardown, `[PHASE]`,
  AAA.
