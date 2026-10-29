# t25: WalReceiver внутри PG-контейнера ноды — план

> **Для исполняющего агента:** план двухуровневый. ШАГИ исполняет сильная модель
> (субагент coder: вся работа сам, включая контроль подпунктов). ПОДПУНКТЫ
> (помечены «ПОДПУНКТ … (слабой модели)») — самодостаточные задания для слабой
> модели без контекста проекта: передаются текстом подпункта + путь к файлу.
> Внутри подпункта НЕТ ссылок на spec/план/проект — всё нужное повторено в нём.

**Цель:** приёмник WAL исполняется процессом ВНУТРИ контейнера PG-ноды
(`supervisord`, self-contained `walreceiver`), работает с etcd напрямую, несёт
собственные метрики; упраздняются отдельный wal-контейнер `pgw-backup-wal-*`,
образы `pgworker-wal`, конфиг `Wal:AgentImage`, чтение docker-логов wal-агентов
воркером.

**Архитектура:** приёмник = WebApplication (Kestrel `0.0.0.0:8080`, эндпоинт
`/metrics`) + hosted-цикл (гейты → REST-роль → резолв etcd → стрим-сессия через
переиспользуемое ядро `WalReceiverCore`) + lease-статус
`/pgworker/backups/<C>/<X>/agent/<N>`. Воркер: env ноды `PGW_BACKUP_*` в
`SpiloEnvBuilder.Build`/`BuildSpec`; `WalStreamProcess` берёт agents-факты и
факты `tli_jump`/`pool_exhausted` из статус-ключей приёмников (Range), docker-логи
и exec-доклад истории удаляются; санитарный снос legacy `pgw-backup-wal-*` —
`RemoveBackupAgentsAsync` в начале тика.

**Тех-стек:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`),
ASP.NET Core, Npgsql, AWSSDK.S3, etcd v3-gateway (JSON API), supervisord,
docker (образ `pgworker-node` на базе `ghcr.io/zalando/spilo-18:4.1-p2`).

**Spec:** `docs/superpowers/2026-10-24-t25-wal-receiver-in-pgnode/spec.md`
(утверждён user-review; arch-правки уже в worktree — `arch/` в плане НЕ
трогается, кроме roadmap-правок мерж-гейта).

**БАЗОВЫЕ ПРАВИЛА:** перед исполнением прочитать
`/Users/demakaev/ZCodeProject/AGENTS.base.md` — исполнять безукоризненно
(команды/субагенты только в фоне с фиксацией task_id; запрет sleep-поллинга
> 30 c; вывод длинных команд — в файл; вопросы — только NEEDS_CONTEXT).

## Глобальные ограничения

- **КОММИТОВ НЕТ.** Git-гейт Mimosa блокирует любой `git commit`: в плане нет
  шагов «закоммитить»; все изменения — правки файлов + прогоны проверок.
  Единственный коммит — Фаза 8 (мерж-гейт), только по явному приказу
  пользователя (Шаг 13).
- **Арх-first уже выполнен** (`arch/19`, `arch/14`, `arch/18`, `arch/25`,
  `arch/adminpanel/04`, `arch/README.md` — правки в worktree, spec §8). Код
  зеркалит arch; расхождение с arch/19 §3/§4 — блокирующее.
- **Язык:** комментарии/документация — по-русски; идентификаторы — на
  английском.
- Сборка всего: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
  (в корне worktree; ожидаемо 0 Error 0 Warning — `TreatWarningsAsErrors`).
- Тесты — ТОЛЬКО `--no-build` после отдельной сборки; env-префикс при самой
  команде (не `export`).
- Порты docker-контейнеров в тестах — только динамические
  (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`);
  никаких литералов вида `:16000`.
- Каждый docker-тест полностью чистит за собой (teardown при любом исходе +
  ассерт чистоты); после КАЖДОЙ тестовой серии — зачистка контейнеров и сетей
  (`docker network prune -f` при осиротевших `pgw-net-*`).
- E2E: перед docker-E2E — `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
  src/tests/PgWorker.E2ePrepare -c Release` (одним процессом, ДО тестов).
- Перезапуск упавших тестов «для выяснения» запрещён — сначала полный разбор
  логов из артефактов (`/tmp/pgw-e2e-artifacts-*`).
- Канон изоляции/телеметрии E2E: `docs/e2e-isolation.md`, `docs/e2e-launch.md`.

## Связка spec → шаги

| spec | шаги |
|---|---|
| §3.1 приёмник (Program/цикл/статус/гейты/роль/резолв/метрики) | 1, 2, 3 |
| §3.2 образ ноды (Dockerfile/supervisord/sanity) | 4 |
| §3.3 воркер: env ноды, санитарный снос, удаление агентных API драйвера | 5, 6 |
| §3.4 воркер: WalStreamProcess (agents/gap/exhaustion/стоп-семантика) | 7 |
| §3.5 конфигурация (удаление AgentImage) | 8 |
| §3.6 образы/стенд/E2E-инфраструктура (метрики — словарь уже в шаге 2; sd-generator/prometheus/дашборд; deploy/00-up/E2eEnvironment/runbook) | 9, 10, 11 |
| §4 фаза 6 E2E + §6 критерии | 12 |
| §6 п.6 мерж-гейт roadmap | 13 |

---

### Шаг 1. Приёмник: контракты идентичности, статуса, гейтов, роли, резолва

**Вход:** шагов до этого нет; код собирается (`dotnet build` зелёный).

**Действие (сильная модель):** в `src/PgWorker.WalReceiver` создать классы
контрактов нового приёмника (подпункты 1.1–1.5 ниже — механическое добавление
новых файлов; сильная модель ревьюит их после исполнения, правит замечания
самостоятельно):

- `WalReceiverIdentity.cs` — env-контракт приёмника (ПОДПУНКТ 1.1);
- `WalReceiverAgentStatus.cs` — модель+JSON статус-ключа `agent/<N>`
  (ПОДПУНКТ 1.2);
- `WalReceiverStatusSession.cs` — lease-сессия статус-ключа (TTL 15 c,
  keepalive 5 c): grant → put → периодический keepalive + put статуса при
  изменении/не реже 5 c; отказ put статуса — diagnostic-log, keepalive
  продолжается; SIGTERM-путь — revoke. Пишет через `IEtcdGateway`
  (`Shared.Etcd.Client`): `LeaseGrantAsync(endpoint, ttlSec, ct)`,
  `PutAsync(endpoint, key, value, lease, ct)`,
  `LeaseKeepaliveAsync(endpoint, lease, ct)`, `LeaseRevokeAsync(endpoint,
  lease, ct)`; failover-перебор endpoints (образец `WalStatusWriter.ReadAsync`);
- `WalReceiverGates.cs` — стоп-гейты: GET `/pgworker/backups-disabled` (ключ
  есть → disabled), GET `/pgworker/backups/<C>/<X>/wal` (значение содержит
  `"state":"STOPPED"`/`"state":"BROKEN"` → paused); чтение обоих — failover;
  транспортный отказ etcd → `Unknown` (живой цикл трактует как «работать по
  последним известным гейтам», повторив чтение через 5 c);
- `PatroniRoleProbe.cs` — REST-проба роли (ПОДПУНКТ 1.4);
- `WalReceiverTargets.cs` — резолв «куда бэкапиться» из etcd на каждом цикле:
  GET `/clusters/<C>/backup_password` (нет/пусто →
  `WalReceiverTargetsResult.WaitingPassword`); чтение policy-ключа
  `/pgworker/backups/<C>/policy` и резолв пула через `BackupPoolResolver`
  (`PgWorker.Backups`) + `S3PoolStore` (`PgWorker.Etcd.S3`) + `BackupPolicyReader`
  (`PgWorker.Backups`); `BackupPolicy?.S3 == null` или Failed-резолв →
  `WaitingPolicy` (текст ошибки — в `Error`); успех — контекст
  `BackupPoolContext` (write-список по роли `WriteMaster`/`WriteReplica`,
  `ReadAll`, креды) + `Slot = BackupNames.Slot(cluster, shard)`.

Каркас типа результата резолва (сильная модель держит сигнатуры stable — их
потребует Шаг 2):

```csharp
public sealed record WalReceiverTargetsResult(
    string? Password, BackupPoolContext? Pool, string? Slot, string? Error)
{
    public bool WaitingPassword => Password is null;
    public bool WaitingPolicy => Pool is null;
    public static WalReceiverTargetsResult PasswordMissing() => new(null, null, null, null);
    public static WalReceiverTargetsResult PolicyMissing(string? error) => new("", null, null, error);
    public static WalReceiverTargetsResult Ok(string password, BackupPoolContext pool, string slot)
        => new(password, pool, slot, null);
}
```

**Выход:** новые файлы контрактов компилируются; старый код не тронут.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Error 0 Warning.

**Связь со spec:** §3.1 п.1–5, п.7; решения §7 №3/№4/№6/№10.

#### ПОДПУНКТ 1.1 (слабой модели)
Файл: `src/PgWorker.WalReceiver/WalReceiverIdentity.cs` (новый). Создать
статический класс `WalReceiverIdentity` в namespace `PgWorker.WalReceiver`:

```csharp
namespace PgWorker.WalReceiver;

/// <summary>Идентичность приёмника WAL (env контейнера ноды, arch/19 §3):
/// env ставит EnsureNode воркера; невалидный набор — старт-отказ процесса
/// (выход с ненулевым кодом, supervisord с exitcodes=0 его не перезапускает).</summary>
public sealed record WalReceiverIdentity(
    string Cluster,
    string Shard,
    string Node,
    IReadOnlyList<string> EtcdEndpoints,
    string? S3Bucket,
    string? S3CaPem)
{
    /// <summary>null — обязательные env отсутствуют/пусты; errors — имена
    /// отсутствующих ключей (значения env в тексты ошибок не включаются).</summary>
    public static WalReceiverIdentity? Parse(Func<string, string?> env, out List<string> errors)
    {
        var failures = new List<string>();
        string Require(string key)
        {
            var value = env(key);
            if (string.IsNullOrWhiteSpace(value))
            {
                failures.Add($"required env {key} is missing or empty");
                return "";
            }
            return value;
        }

        var cluster = Require("PGW_BACKUP_CLUSTER");
        var shard = Require("PGW_BACKUP_SHARD");
        var node = Require("PGW_BACKUP_NODE");
        var etcd = Require("PGW_ETCD");
        // bucket/CA per-install (arch/19 §7): необязательны — приёмник гейтится
        // etcd-ключами, их отсутствие не старт-отказ.
        var bucket = env("PGW_BACKUP_S3_BUCKET");
        var caPem = env("PGW_BACKUP_S3_CA_PEM");

        errors = failures;
        if (failures.Count > 0)
            return null;

        var endpoints = etcd.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (endpoints.Count == 0)
        {
            errors = ["env PGW_ETCD must be a non-empty comma-separated list of urls"];
            return null;
        }

        return new WalReceiverIdentity(
            cluster, shard, node, endpoints,
            string.IsNullOrWhiteSpace(bucket) ? null : bucket,
            string.IsNullOrWhiteSpace(caPem) ? null : caPem);
    }
}
```

Комментарии писать ровно приведённые (русский). Файл сохранить; компиляцию
отдельно не запускать.

#### ПОДПУНКТ 1.2 (слабой модели)
Файл: `src/PgWorker.WalReceiver/WalReceiverAgentStatus.cs` (новый). Создать
в namespace `PgWorker.WalReceiver` перечисления, record и статический класс
JSON-сериализации (формат ключа `/pgworker/backups/<C>/<X>/agent/<N>`,
arch/19 §4; поле `node` пишется по факту значения identity):

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.WalReceiver;

/// <summary>Роль источника приёмника (REST-проба своей ноды, arch/19 §3).</summary>
public enum WalReceiverRole { Master, Sync, None }

/// <summary>Состояние приёмника (arch/19 §4): waiting_* — ожидание условий,
/// paused — стоп-семантика, failed_permanent — несъёмное условие (причина в Error).</summary>
public enum WalReceiverState
{
    Starting, WaitingRole, WaitingPassword, WaitingPolicy, WaitingSlot,
    WaitingDisabled, Streaming, Paused, FailedPermanent,
}

/// <summary>Факт tli-прыжка приёмника (58P01): сегменты (From .. To] не будут
/// доставлены; живёт в статус-ключе до следующего прыжка.</summary>
public sealed record WalReceiverTliJump(string From, string To, uint Tli);

public sealed record WalReceiverAgentStatus(
    WalReceiverRole Role,
    WalReceiverState State,
    string? LastDeliveredSegment,
    long? LastDeliveredUnix,
    bool PoolExhausted,
    WalReceiverTliJump? TliJump,
    string? Error,
    long UpdatedUnix);

public static class WalReceiverAgentStatusJson
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Значение статус-ключа: {"node","role","state",...} — arch/19 §4.</summary>
    public static string ToJson(string node, WalReceiverAgentStatus status) => JsonSerializer.Serialize(new
    {
        node,
        role = RoleName(status.Role),
        state = StateName(status.State),
        last_delivered_segment = status.LastDeliveredSegment,
        last_delivered_unix = status.LastDeliveredUnix,
        pool_exhausted = status.PoolExhausted,
        tli_jump = status.TliJump is { } jump
            ? new { from = jump.From, to = jump.To, tli = jump.Tli }
            : null,
        error = status.Error,
        updated_unix = status.UpdatedUnix,
    }, Options);

    /// <summary>null — значение не парсится (чужое/битое).</summary>
    public static (string Node, WalReceiverAgentStatus Status)? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json, Options);
            if (dto is null || dto.Node is null || dto.State is null)
                return null;
            return (dto.Node, new WalReceiverAgentStatus(
                RoleOf(dto.Role), StateOf(dto.State),
                dto.LastDeliveredSegment, dto.LastDeliveredUnix,
                dto.PoolExhausted ?? false,
                dto.TliJump is { From: { Length: > 0 }, To: { Length: > 0 }, Tli: not null } t
                    ? new WalReceiverTliJump(t.From, t.To, t.Tli.Value) : null,
                dto.Error, dto.UpdatedUnix ?? 0));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string RoleName(WalReceiverRole role) => role switch
    {
        WalReceiverRole.Master => "master",
        WalReceiverRole.Sync => "sync",
        _ => "none",
    };

    private static WalReceiverRole RoleOf(string? name) => name switch
    {
        "master" => WalReceiverRole.Master,
        "sync" => WalReceiverRole.Sync,
        _ => WalReceiverRole.None,
    };

    private static string StateName(WalReceiverState state) => state switch
    {
        WalReceiverState.Starting => "starting",
        WalReceiverState.WaitingRole => "waiting_role",
        WalReceiverState.WaitingPassword => "waiting_password",
        WalReceiverState.WaitingPolicy => "waiting_policy",
        WalReceiverState.WaitingSlot => "waiting_slot",
        WalReceiverState.WaitingDisabled => "waiting_disabled",
        WalReceiverState.Streaming => "streaming",
        WalReceiverState.Paused => "paused",
        _ => "failed_permanent",
    };

    private static WalReceiverState StateOf(string name) => name switch
    {
        "starting" => WalReceiverState.Starting,
        "waiting_role" => WalReceiverState.WaitingRole,
        "waiting_password" => WalReceiverState.WaitingPassword,
        "waiting_policy" => WalReceiverState.WaitingPolicy,
        "waiting_slot" => WalReceiverState.WaitingSlot,
        "waiting_disabled" => WalReceiverState.WaitingDisabled,
        "streaming" => WalReceiverState.Streaming,
        "paused" => WalReceiverState.Paused,
        _ => WalReceiverState.FailedPermanent,
    };

    private sealed record Dto(
        [property: JsonPropertyName("node")] string? Node,
        [property: JsonPropertyName("role")] string? Role,
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("last_delivered_segment")] string? LastDeliveredSegment,
        [property: JsonPropertyName("last_delivered_unix")] long? LastDeliveredUnix,
        [property: JsonPropertyName("pool_exhausted")] bool? PoolExhausted,
        [property: JsonPropertyName("tli_jump")] TliJumpDto? TliJump,
        [property: JsonPropertyName("error")] string? Error,
        [property: JsonPropertyName("updated_unix")] long? UpdatedUnix);

    private sealed record TliJumpDto(
        [property: JsonPropertyName("from")] string? From,
        [property: JsonPropertyName("to")] string? To,
        [property: JsonPropertyName("tli")] uint? Tli);
}
```

Файл сохранить; компиляцию отдельно не запускать.

#### ПОДПУНКТ 1.4 (слабой модели)
Файл: `src/PgWorker.WalReceiver/PatroniRoleProbe.cs` (новый). Создать в
namespace `PgWorker.WalReceiver`:

```csharp
namespace PgWorker.WalReceiver;

/// <summary>Роль источника по REST-пробе Patroni loopback (arch/19 §3):
/// GET https://127.0.0.1:8008/primary → 200 ⇒ master; /sync → 200 ⇒ sync;
/// любой не-200/ошибка ⇒ none (переходное окно/async). Доверие — CA REST-Api
/// ноды (env SSL_RESTAPI_CA, PEM материализуется во временный файл и читается
/// из него — прецедент master-lease.py; 127.0.0.1 в SAN серта —
/// arch/14 §2.4 п.6).</summary>
public sealed class PatroniRoleProbe
{
    private const int RestPort = 8008;

    /// <summary>Проба: caPem (env SSL_RESTAPI_CA) → временный файл →
    /// X509Certificate2-CA → валидация цепочки сервера (TlsChain). Любая
    /// ошибка транспорта/ФС — none (переходное окно, ожидание).</summary>
    public static async Task<WalReceiverRole> ProbeAsync(string caPem, CancellationToken ct)
    {
        System.Security.Cryptography.X509Certificates.X509Certificate2 ca;
        try
        {
            var caPath = Path.Combine(Path.GetTempPath(), $"pgw-rest-ca-{Guid.NewGuid():N}.pem");
            await File.WriteAllTextAsync(caPath, caPem + "\n", ct);
            ca = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPemFile(caPath);
        }
        catch (Exception)
        {
            return WalReceiverRole.None; // ФС/PEM недоступны — роли нет (ожидание)
        }

        var handler = new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    if (certificate is null) return false;
                    var cert = certificate is System.Security.Cryptography.X509Certificates.X509Certificate2 c2
                        ? c2
                        : new System.Security.Cryptography.X509Certificates.X509Certificate2(certificate);
                    try
                    {
                        return Shared.Tls.TlsChain.ValidateChain(cert, ca);
                    }
                    catch
                    {
                        return false;
                    }
                },
            },
        };
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(3) };
        if (await OkAsync(client, "primary", ct))
            return WalReceiverRole.Master;
        if (await OkAsync(client, "sync", ct))
            return WalReceiverRole.Sync;
        return WalReceiverRole.None;
    }

    private static async Task<bool> OkAsync(HttpClient client, string path, CancellationToken ct)
    {
        try
        {
            using var response = await client.GetAsync($"https://127.0.0.1:{RestPort}/{path}", ct);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false; // REST недоступен — переходное окно (ожидание)
        }
    }
}
```

Комментарии — ровно приведённые. Файл сохранить; компиляцию отдельно не
запускать.

---

### Шаг 2. Приёмник: переработка ядра в сессии, цикл, Program, метрики

**Вход:** Шаг 1 выполнен (контракты на месте, сборка зелёная).

**Действие (сильная модель):**

1. **Переработать `WalReceiverCore.cs`** (это ревью-чувствительный перенос —
   НЕ подпункт):
   - `RunAsync` с вечным циклом и exit-кодами (3/4/5/6) УПРАЗДНЯЕТСЯ. Механика
     ядра (OpenAsync + сверка `wal_segment_size`; резолв хвоста S3 через
     `S3TailResolver`; `START_REPLICATION`; `SegmentAssembler`; put с
     перебором write-списка + backpressure; `SetConfirmedAsync` ТОЛЬКО после
     успешного put; tli-процедура 58P01 `TliProcedureAsync`) сохраняется 1:1,
     но оформляется как управляемая сессия для нового цикла:
     `Task<WalSessionOutcome> RunSessionAsync(WalSessionContext ctx,
     CancellationToken ct)`, где одна сессия = «открытие → хвост → старт →
     приём до обрыва/смены роли», а исход — различимое событие
     (`WalSessionOutcome`: `Reconnect` (обрыв/transient), `WaitingSlot`
     (слота нет — теперь transient-ожидание, НЕ permanent), `FailedPermanent`
     (segment-size mismatch ИЛИ разрыв LSN — с текстом ошибки),
     `RoleChanged` (цикл оборвал сессию сам)). Инвариант RPO не меняется:
     подтверждение слота ТОЛЬКО после успешного put (arch/19 §3).
   - `TliProcedureAsync` сохраняется; прыжок-fallback теперь не пишет
     stdout-маркер, а возвращает факт `WalReceiverTliJump` наружу (цикл кладёт
     его в статус-ключ, живёт до следующего прыжка).
   - `.history`: при недоступности `ReadTimelineHistoryAsync` (runtime не
     экспонирует) — чтение файла
     `/home/postgres/pgdata/pgroot/data/pg_wal/<tli:x8>.history` ФС ноды
     (байт-контент → тот же put); нет и файла — факт `HistoryMissing` в
     исходе сессии (диагностика в статус, воркер ничего не докладывает).
   - `PutWithRetryAsync`-механика (пер-целевой таймаут `PutAttemptTimeout`,
     бюджет вызова, backpressure) сохраняется; исчерпание write-списка — не
     маркер в stdout, а флаг `PoolExhausted=true` в исходе (снимается первым
     успешным put); каждая неудачная попытка/ретрай/успех — колбэки метрик
     (счётчики приёмника).
2. **Удалить упраздняемое:** `WalReceiverOptions.cs` — парсер `WalReceiverEnv`
   удалить, record `WalReceiverOptions` сохранить как параметры СЕССИИ
   (PgHost/PgPort/PgUser/PgPassword/PgDbname/Slot/Cluster/Shard/S3-списки/
   креды/bucket/CaPem — сборкой управляет цикл: host `127.0.0.1:5432`, user
   `backup_exec`, dbname `postgres` — фиксирует цикл приёмника, не env);
   `WalReceiverMarkers.cs` удалить полностью (stdout-протокол джобов упразднён
   — spec §3.1 хвост); `Program.cs` переписать (подпункт 2.1).
3. **Новый `WalReceiverLoop.cs`** (hosted-сервис, логика §3.1 — сильная
   модель): главный цикл:
   - старт: `WalReceiverIdentity.Parse` (env) — невалидный → лог в stdout +
     `Environment.Exit(1)` (supervisord `exitcodes=0` не перезапустит);
   - каждый цикл (~5 c пауза при ожидании): гейты (disabled →
     `waiting_disabled`, STOPPED/BROKEN → `paused`; etcd недоступен → работать
     по последним известным, перепроверка через 5 c) → роль REST-пробой
     (`none` → `waiting_role`) → резолв кредов/целей (`WalReceiverTargets`;
     missing → `waiting_password`; policy-нет/Failed → `waiting_policy` c
     ошибкой в `error`) → CA S3: `S3CaPem` identity → материализация в temp
     (как в старом Program) → сборка `PoolBackupS3` (write-список по роли,
     read `ReadAll`) → статус-ключ пишется начиная со `starting` и при каждой
     смене состояния/фактов, не реже 5 c вместе с keepalive (put вне hot-path:
     после закрытия сегмента/по таймеру — `WalReceiverStatusSession`);
   - в стриме: периодическая (5 c) REST-проба роли; смена роли (sync → none
     и т.п.) — чистое завершение сессии → `waiting_role`; события ядра
     (`WaitingSlot` → `waiting_slot`, `FailedPermanent` → `failed_permanent`
     с ошибкой, стрим стоит, процесс жив, retry-полл гейтов продолжается),
     `tli_jump`/`pool_exhausted`/`history_missing` — факты статус-ключа;
   - слот: имя `BackupNames.Slot(<C>,<X>)`; слота нет/lost → `waiting_slot`
     (ensure делает воркер; lost-лечение — recreate воркером);
   - SIGTERM (supervisord stop/docker stop): graceful — отмена токена хоста,
     закрытие стрим-сессии, `LeaseRevokeAsync`, exit 0;
   - метрики (см. ниже) пишутся из цикла/ядра марк-методами.
4. **Новый `WalReceiverMetrics.cs`** (сильная модель; паттерн
   `Shared.Metrics.Worker.WorkerMetricsInstrumentation` — инструменты
   создаются из `Meter`, марк-методы не бросают): Meter
   `PgWorker.WalReceiver` (имя константа `MeterName`), словарь arch/18 §2.8:
   gauges `walreceiver_role` (лейблы cluster/shard/node/role —
   ObservableGauge с активной ролью=1), `walreceiver_streaming`,
   `walreceiver_backlog_bytes` (serverWalEnd потока − подтверждённый хвост),
   `walreceiver_last_success_timestamp_seconds`; counters
   `walreceiver_segments_delivered_total`, `walreceiver_bytes_delivered_total`,
   `walreceiver_put_retries_total`, `walreceiver_put_errors_total`,
   `walreceiver_reconnects_total`.
5. **Юнит-тесты** (`src/tests/PgWorker.UnitTests/Backups/`, сильная модель;
   фейковые транспорты уже есть в `WalReceiverCoreTests` —
   `IXLogReplicationSource`-фейки, `FakeS3`):
   - `WalReceiverIdentityTests`: полный/неполный env, PGW_ETCD без элементов;
   - `WalReceiverAgentStatusJsonTests`: ToJson/Parse round-trip всех полей,
     толерантность Parse к чужому JSON;
   - цикл/машина состояний (фейки etcd-гейтов/REST/S3, AAA-комментарии):
     disabled → `waiting_disabled`; wal-ключ STOPPED/BROKEN → `paused`;
     роль none → `waiting_role`; нет пароля → `waiting_password`; policy без
     s3 → `waiting_policy`; слота нет → `waiting_slot`; segment-size mismatch
     → `failed_permanent` БЕЗ выхода процесса; разрыв LSN →
     `failed_permanent`; исчерпание write-списка → `pool_exhausted=true`,
     первый успешный put снимает; tli-прыжок → факт `tli_jump` в статусе;
     etcd-моргание при живом стриме — стрим НЕ рвётся;
   - существующие `WalReceiverCoreTests` — адаптировать под новый контракт
     сессии (инвариант SetConfirmed-после-put сохраняется в ассертах);
     `WalReceiverMarkersTests`, `WalReceiverOptionsTests` — удалить
     (маркеры/env-парсер упразднены).
6. **Ре-доклад инвентаря `.history` (ДОПОЛНЕНИЕ по правке spec от
   2026-10-24, SPEC_DEVIATION E2eTliSwitchScenarios). Пункты 1–5 выше УЖЕ
   ИСПОЛНЕНЫ — исполнитель делает ТОЛЬКО этот пункт и ПОДПУНКТ 2.2, затем
   Проверку Шага 2 заново (сборка + юнит-фильтр).** Spec §3.1 п.6 / решение
   §7 №9 (дополнено): при (ре)старте стрима и раз за цикл подключения
   приёмник перечисляет `.history`-файлы `pg_wal` СВОЕЙ ноды и кладёт в S3
   `wal/` отсутствующие — страховка TLI-переходов ВНЕ окна стрима (промоут,
   пока приёмник не стримил; после промоута `000000NN.history` лежит в
   `pg_wal` нового мастера, после restore — в pgdata восстановленной ноды,
   arch/19 §3). Реализация:
   - `WalReceiverCore.RedeliverHistoryInventoryAsync(...)` — новый статический
     метод (ПОДПУНКТ 2.2): каталог `pg_wal` (прод — `DefaultPgWalDir`, путь
     уже известен по ФС-fallback `.history`) → файлы, чьё имя = 8 hex-цифр +
     `.history`; пусто — выход; `s3.ListWalAsync(cluster, shard)` (перебор
     целей до успешного — read-цели включены) — отказ list → diagnostic-log и
     выход (следующий цикл подключения повторит); для каждого локального
     имени, отсутствующего в list, — ОДИНОЧНЫЙ `s3.PutObjectAsync` с SHA256
     (как put `.history` в сессии); отказ put — diagnostic-log, переход к
     следующему файлу. ВАЖНО (закрытая развилка): put — именно одиночный
     `PutObjectAsync`, НЕ `PutWithRetryAsync` (его бесконечный ретрай —
     hot-path backpressure стрима; ре-доклад — вне hot-path: отказ =
     diagnostic-log, доставит следующий цикл, spec §3.1 п.6). Идемпотентно:
     put только отсутствующих по list, дублей нет. Метрик и фактов
     статус-ключа ре-доклад НЕ пишет.
   - `WalReceiverLoop`: internal-конструктор получает опциональный параметр
     `string? pgWalDir = null` (поле `_pgWalDir` нормализуется в конструкторе:
     `pgWalDir ?? WalReceiverCore.DefaultPgWalDir`; ФС-подмена для юнитов —
     образец `HistoryFilePath` в тестах ядра; прод-конструктор не меняется);
     вызов — в блоке 4.5, НЕПОСРЕДСТВЕННО ПЕРЕД `WalReceiverCore.RunSessionAsync`
     (каждая итерация цикла, дошедшая до сессии, = «(ре)старт стрима и раз за
     цикл подключения»):
     `await WalReceiverCore.RedeliverHistoryInventoryAsync(s3,
     identity.Cluster, identity.Shard, _pgWalDir, msg => Console.Error.WriteLine(msg), ct)`;
     сам вызов исключений не бросает (метод глотает внутренне) и отказ
     ре-доклада старту сессии НЕ мешает.
   - юниты (сильная модель, AAA; фейки `FakeS3`/`FakeEtcd` в
     `WalReceiverLoopTests`; фейку добавить флаг устойчивого отказа list —
     по образцу `FailKeys`):
     1. файл есть в pg_wal (tmpdir `00000002.history`), в S3 отсутствует →
        появился ключ `<C>/<X>/wal/00000002.history`;
     2. объект уже существует в S3 → дубль-put нет (счётчик put-вызовов
        фейка = 0);
     3. отказ list → ни одного put, цикл приёмника жив; после «починки»
        листинга следующий цикл доносит файл;
     4. интеграция цикла: `WalReceiverLoop` на фейках (роль master, слот,
        пустой поток) с tmpdir-каталогом → объект в S3 без наблюдаемого
        перехода в сессии.

**Выход:** `src/PgWorker.WalReceiver` — WebApplication-приёмник с циклом,
статус-ключом, метриками; юниты зелёные; `WalReceiverMarkers`/`WalReceiverEnv`
удалены; ре-доклад инвентаря `.history` — метод в ядре + вызов в цикле
(put отсутствующих вне hot-path, идемпотентно), юнит-кейсы ре-доклада
зелёные.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Error 0 Warning.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalReceiver"` — зелёные (юниты приёмника, включая кейсы ре-доклада инвентаря `.history`).

**Связь со spec:** §3.1 (весь, вкл. п.6 — ре-доклад инвентаря history), §2 (инвариант RPO, двойная архивация), §7 №1/№5/№8/№9/№17; arch/18 §2.8, arch/19 §3 («.history при TLI-переходе»).

#### ПОДПУНКТ 2.1 (слабой модели)
Файл: `src/PgWorker.WalReceiver/Program.cs` (ПЕРЕЗАПИСАТЬ целиком). Точка
входа приёмника — ASP.NET-хост: Kestrel `0.0.0.0:8080`, единственный эндпоинт
`/metrics` (Shared.Metrics), фоновый цикл — hosted-сервис `WalReceiverLoop`
(класс уже существует в проекте; подключить по имени). Точное содержимое:

```csharp
using PgWorker.WalReceiver;
using Shared.Metrics;

// Точка входа приёмника WAL (t25, arch/19 §3): WebApplication — база образа
// требует aspnet-рантайм; Kestrel 0.0.0.0:8080 несёт ТОЛЬКО /metrics
// (Shared.Metrics.AddAppMetrics/MapAppMetrics, Meter PgWorker.WalReceiver,
// словарь arch/18 §2.8); стрим — hosted-сервис WalReceiverLoop. Идентичность —
// env контейнера ноды (WalReceiverIdentity): невалидный набор — старт-отказ
// (лог в stdout контейнера + exit 1; supervisord exitcodes=0 не перезапускает).
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:8080");
builder.Services.AddAppMetrics(WalReceiverMetrics.MeterName, builder.Configuration.GetSection("Metrics"));
builder.Services.AddHostedService<WalReceiverLoop>();

var app = builder.Build();
app.MapAppMetrics();
app.Run();
```

Если `WalReceiverLoop`/`WalReceiverMetrics` ещё не существуют в проекте — файл
НЕ сохранять, вернуть ошибку «зависимости шага не готовы». Иначе сохранить;
компиляцию отдельно не запускать.

#### ПОДПУНКТ 2.2 (слабой модели)
Файл: `src/PgWorker.WalReceiver/WalReceiverCore.cs` (существующий). Добавить
метод в КОНЕЦ класса `WalReceiverCore` — сразу ПОСЛЕ закрывающей фигурной
скобки метода `PutWithRetryAsync` и ПЕРЕД закрывающей фигурной скобкой самого
класса (последняя `}` файла). Других правок в файле НЕТ. Точный код:

```csharp
    /// <summary>Ре-доклад инвентаря .history (t25, arch/19 §3): перечисляет
    /// .history-файлы pg_wal своей ноды и кладёт в S3 wal/ отсутствующие —
    /// страховка TLI-переходов вне окна стрима (промоут, пока приёмник не
    /// стримил: после промоута файл лежит в pg_wal нового мастера, после
    /// restore — в pgdata восстановленной ноды). Вне hot-path: один list
    /// (перебор целей до успешного) + точечные одиночные put с SHA256;
    /// идемпотентно (put только отсутствующих по list); любой отказ —
    /// diagnostic-log, доставит следующий цикл подключения; фактов в
    /// статус-ключ не пишет.</summary>
    public static async Task RedeliverHistoryInventoryAsync(
        IBackupS3 s3, string cluster, string shard, string pgWalDir,
        Action<string>? diagnostic, CancellationToken ct)
    {
        List<string> local;
        try
        {
            // PG пишет .history как "<tli:x8>.history" (8 hex-цифр).
            local = Directory.EnumerateFiles(pgWalDir, "*.history")
                .Select(Path.GetFileName)
                .OfType<string>()
                .Where(n => n.Length == 16
                    && uint.TryParse(n.AsSpan(0, 8),
                        System.Globalization.NumberStyles.HexNumber, null, out _))
                .ToList();
        }
        catch (Exception e)
        {
            // pg_wal недоступен — инвентаря нет, не ошибка приёмника.
            diagnostic?.Invoke(
                $"history inventory: enumerate {pgWalDir} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            return;
        }

        if (local.Count == 0)
            return;

        var listing = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listing.IsSuccess)
        {
            // list не прошёл — ре-доклад отложен до следующего цикла.
            diagnostic?.Invoke(
                $"history inventory: list wal/ failed: {listing.Error!.Message.Split('\n')[0]}");
            return;
        }

        var remote = listing.Value.Select(o => o.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var name in local)
        {
            if (remote.Contains(name))
                continue; // уже в архиве — дубль-put не нужен (идемпотентность)

            try
            {
                var content = await File.ReadAllAsync(Path.Combine(pgWalDir, name), ct);
                var put = await s3.PutObjectAsync(
                    $"{cluster}/{shard}/wal/{name}", content,
                    Convert.ToHexString(SHA256.HashData(content)), ct);
                if (!put.IsSuccess)
                    diagnostic?.Invoke(
                        $"history inventory: put {name} failed: {put.Error!.Message.Split('\n')[0]}");
            }
            catch (Exception e)
            {
                diagnostic?.Invoke(
                    $"history inventory: put {name} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            }
        }
    }
```

Комментарии — ровно приведённые (русский). Файл сохранить; компиляцию отдельно
не запускать.

---

### Шаг 3. Приёмник: имя бинаря и self-contained publish

**Вход:** Шаг 2 выполнен, сборка зелёная.

**Действие:** в `src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj`
добавить `<AssemblyName>walreceiver</AssemblyName>` в `<PropertyGroup>` —
self-contained publish обязан дать бинарь `walreceiver` (путь
`/opt/pgw-wal/walreceiver` в supervisord-конфиге образа ноды, spec §3.2).
Проверить publish: на хосте сборки
`DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -r linux-arm64 --self-contained -o artifacts/node-wal --nologo`
(RID по arch хоста: `linux-arm64` на Apple Silicon, `linux-x64` на x86_64;
БЕЗ trimming — рефлексия Npgsql/AWSSDK/OTel). Если хост x86_64 — в команде
подставить `-r linux-x64`.

**Выход:** publish-вывод содержит исполняемый `walreceiver` (self-contained,
без sdk-зависимостей).

**Проверка:** `ls artifacts/node-wal/walreceiver` существует; повторная сборка
решения зелёная.

**Связь со spec:** §3.2, §7 №2.

---

### Шаг 4. Образ ноды: Dockerfile + supervisord + sanity-прогон

**Вход:** Шаг 3 выполнен (`artifacts/node-wal/walreceiver` собирается).

**Действие:** правки образа ноды (подпункты 4.1–4.2 — точные тексты); затем
сильная модель: сборка образа
`docker build -f docker/node/Dockerfile --build-arg WAL_PUBLISH_DIR=artifacts/node-wal -t pgworker-node:dev .`
(контекст — корень worktree) и sanity по критерию §6 п.2 — интеграционный
docker-тест `src/tests/PgWorker.IntegrationTests/Backups/NodeWalReceiverTests.cs`
(сильная модель; паттерн `EtcdFixture` + testcontainers, порты только
динамические, teardown при любом исходе):
- контейнер `pgworker-node:dev` с env идентичности (`PGW_BACKUP_CLUSTER/SHARD/NODE`,
  `PGW_BACKUP_S3_BUCKET`, `PGW_ETCD=http://<alias>:2379`) в сети с etcd-контейнером
  теста: приёмник жив (supervisorctl status walreceiver RUNNING), `/metrics`
  внутри контейнера (`docker exec … curl -fsS http://127.0.0.1:8080/metrics`)
  отдаёт серии словаря §2.8 (хотя бы `walreceiver_streaming`), в etcd появляется
  ключ `/pgworker/backups/<C>/<X>/agent/<N>` с `"state":"waiting_role"` (REST
  ноды ещё не поднят — роль none);
- lease TTL: `docker exec … pkill -f walreceiver` → ключ гаснет в пределах
  ~15 c (TTL), процесс перезапущен supervisord (падение ≠ exit 0) → ключ
  возвращается;
- без env идентичности (контейнер без `PGW_BACKUP_CLUSTER`): процесс выходит,
  supervisord НЕ лупит (статус EXITED, счётчик рестартов 0).

**Выход:** `pgworker-node:dev` содержит `/opt/pgw-wal/walreceiver` под
supervisord; sanity-тест зелёный.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~NodeWalReceiver"` — зелёный; зачистка контейнеров/сетей теста после прогона.

**Связь со spec:** §3.2, §6 п.2, §7 №1.

#### ПОДПУНКТ 4.1 (слабой модели)
Файл: `docker/node/supervisord.conf` (ДОБАВИТЬ секцию в конец файла, после
секции `[program:pg_doorman]`, с пустой строкой-разделителем):

```ini

[program:walreceiver]
; Приёмник WAL бэкапов (t25, arch/19 §3): self-contained бинарь /opt/pgw-wal
; (сборка образа — двухшаговая: publish на хосте, см. docker/node/Dockerfile).
; Условность запуска обеспечивает сам бинарь: отсутствие PGW_BACKUP_* —
; старт-отказ с ненулевым exit (окно диагностики в /dev/stdout), supervisord
; его НЕ перезапускает (autorestart=unexpected + exitcodes=0 — рестарты только
; падений). user=postgres — доступ к pg_wal PGDATA (fallback .history, §3).
command=/opt/pgw-wal/walreceiver
priority=40
user=postgres
autorestart=unexpected
exitcodes=0
startsecs=5
stopasgroup=true
killasgroup=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
redirect_stderr=true
```

Комментарии — ровно приведённые. Файл сохранить.

#### ПОДПУНКТ 4.2 (слабой модели)
Файл: `docker/node/Dockerfile` (ПРАВИТЬ существующий). Вставить ПЕРЕД строкой
`COPY docker/node/master-lease.py /home/postgres/master-lease.py` следующие
строки (и только их):

```dockerfile
# Приёмник WAL (t25, arch/19 §3/§14 §2.1): self-contained publish НА ХОСТЕ
# (RID хоста сборки, без trimming — рефлексия Npgsql/AWSSDK/OTel), в образ —
# ТОЛЬКО publish-вывод (канон «publish на хосте, в контейнер — артефакты»):
#   dotnet publish src/PgWorker.WalReceiver -c Release -r <rid> \
#     --self-contained -o artifacts/node-wal
#   docker build -f docker/node/Dockerfile \
#     --build-arg WAL_PUBLISH_DIR=artifacts/node-wal -t pgworker-node:dev .
ARG WAL_PUBLISH_DIR=artifacts/node-wal
COPY ${WAL_PUBLISH_DIR} /opt/pgw-wal/
RUN chmod +x /opt/pgw-wal/walreceiver
```

Ничего больше в файле не менять (ENTRYPOINT/EXPOSE/CMD прежние). Файл
сохранить.

---

### Шаг 5. Воркер: env ноды `PGW_BACKUP_*` (SpiloEnvBuilder / драйверы / DI)

**Вход:** Шаги 1–4 выполнены; сборка зелёная.

**Действие (сильная модель):**
1. `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`,
   `SpiloEnvBuilder.Build`: всегда добавлять в словарь env
   `["PGW_BACKUP_CLUSTER"] = topology.Cluster`,
   `["PGW_BACKUP_SHARD"] = topology.Shard`,
   `["PGW_BACKUP_NODE"] = nodeName` (независимо от `Backups:Enabled` — приёмник
   гейтится etcd-ключами; spec §3.3, решение §7 №16). Подпункт 5.1.
2. Новый record `NodeBackupEnv(string? Bucket, string? CaPem)` в
   `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (рядом с
   `SpiloEnvBuilder`): env бэкап-целей ноды (bucket пуст ⇒ null ⇒ env не
   ставится — приёмник старт-фейлится, подсистема выключена валидацией старта).
3. `ClusterDriver.cs`: конструкторы `PlainClusterDriver` и
   `SwarmClusterDriver` получают опциональный параметр
   `NodeBackupEnv? nodeBackupEnv = null`; `PlainClusterDriver.BuildSpec`
   добавляет в env `PGW_BACKUP_S3_BUCKET`/`PGW_BACKUP_S3_CA_PEM`, если
   значения не null/не пусты (Swarm использует внутренний Plain.BuildSpec —
   параметр проксировать в его конструктор).
4. `src/PgWorker.App/Program.cs` (DI-фабрика `IClusterDriver`, строки ~271–326):
   собирать `nodeBackupEnv` из опций:
   `new NodeBackupEnv(bucket: string.IsNullOrWhiteSpace(opts.Backups.S3.Bucket) ? null : opts.Backups.S3.Bucket, caPem: BackupCaPem.ReadIfConfigured(opts.Backups.S3.CaBundlePath))`
   (`PgWorker.Backups.BackupCaPem` уже есть; нечитаемый CA-файл при заданном
   пути — исключение при старте воркера = fail-fast, прецедент TLS-материалов)
   и передавать обоим драйверам.
5. Юнит-тесты: `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`
   — ассерты трёх новых env; `ClusterDriverTests` — ассерт BuildSpec env
   (`PGW_BACKUP_S3_BUCKET`/`CA_PEM` ставится при заданном `NodeBackupEnv`, не
   ставится при null).

**Выход:** env ноды несёт идентичность и бэкап-цели приёмника; тесты зелёные.

**Проверка:** сборка решения; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~NodeConfigBuilders|FullyQualifiedName~ClusterDriverTests"` — зелёные.

**Связь со spec:** §3.3, §7 №3/№10/№16; arch/14 §2.1.

#### ПОДПУНКТ 5.1 (слабой модели)
Файл: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`. НАЙТИ в методе
`public static IReadOnlyDictionary<string, string> Build(` класса
`SpiloEnvBuilder` строку

```
            // ENV lease-скрипта мастер-ключа (callback on_role_change, P11).
```

и ВСТАВИТЬ ПЕРЕД ней:

```csharp
            // Идентичность приёмника WAL (t25, arch/19 §3/§14 §2.1): СТРОГО
            // всегда, независимо от Backups:Enabled — приёмник гейтится
            // etcd-стоп-ключами; включение подсистемы не требует пересоздания нод.
            ["PGW_BACKUP_CLUSTER"] = topology.Cluster,
            ["PGW_BACKUP_SHARD"] = topology.Shard,
            ["PGW_BACKUP_NODE"] = nodeName,

```

Затем в ТОМ ЖЕ файле, ПЕРЕД объявлением `public static class SpiloEnvBuilder`
(после закрывающей фигурной скобки record `NodeRestTls`) вставить:

```csharp
/// <summary>Env бэкап-целей ноды для приёмника WAL (t25, arch/19 §7):
/// bucket/CA https S3 — per-install константы (не секреты, не дрейфуют);
/// ставит BuildSpec драйвера из Backups-опций воркера. Bucket null/пуст ⇒
/// env не ставится — приёмник старт-фейлится (подсистема выключена
/// валидацией старта: Enabled=true ⇒ Bucket непустой).</summary>
public sealed record NodeBackupEnv(string? Bucket, string? CaPem);

```

Ничего больше в файле не менять. Комментарии — ровно приведённые. Файл
сохранить; компиляцию отдельно не запускать.

---

### Шаг 6. Воркер: драйвер — удаление агентных API, санитарный снос legacy

**Вход:** Шаг 5 выполнен; сборка зелёная.

**Действие (сильная модель):**
1. `src/PgWorker.Docker/Drivers/ClusterDriver.cs`:
   - из интерфейса `IClusterDriver` УДАЛИТЬ методы `EnsureBackupAgentAsync`,
     `ListBackupAgentsAsync`, `BackupAgentLogsAsync` и файл
     `src/PgWorker.Docker/Drivers/BackupAgentNames.cs` удалить (тип
     упразднён; префикс `pgw-backup-wal-<C>-` драйверы формируют приватной
     константой внутри `RemoveBackupAgentsAsync`);
   - `PlainClusterDriver.RemoveBackupAgentsAsync` — ОСТАЁТСЯ (миграционно-
     санитарный снос legacy-контейнеров `pgw-backup-wal-<C>-*` + их
     `-staging`-томов; текущая реализация уже это делает — не менять
     поведение, только источник префикса);
   - `SwarmClusterDriver`: удалить заглушки `EnsureBackupAgentAsync`/
     `ListBackupAgentsAsync`/`BackupAgentLogsAsync`; `RemoveBackupAgentsAsync`
     остаётся (префикс строкой);
   - `PlainClusterDriver.RemoveNodeAsync`: убрать блок сверки
     `ListBackupAgentsAsync` перед демонтажем сети (агентов в сети больше не
     бывает по построению; сеть сносится, когда объектов нод нет; санитарный
     снос legacy уже прошёл тиком `WalStreamProcess`);
   - комментарий в `GetHostsAsync` про «pgw-backup-wal-* попадают в счёттер
     СОЗНАТЕЛЬНО» — обновить (легаси-контейнеры сносит санитарный вызов).
2. Правки ВСЕХ реализаций/тестов `IClusterDriver` (сборка обязана стать
   зелёной, `TreatWarningsAsErrors`):
   `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` (удалить
   стабы ensure/list/logs, счётчики `EnsuredBackupAgents`/`EnsuredAgentSpecs`/
   `BackupAgentLogTail`/`FailBackupAgentLogs`/`AgentEnsureDelay`/
   `SlowAgentEnsures`; `RemovedBackupAgents`/`BackupAgentObjects` — оставить,
   добавить счётчик `RemoveBackupAgentsCalls`), обёртки в
   `Backups/RestoreProcessTests.cs`, `Backups/BackupVerifyProcessTests.cs`,
   `Backups/RestoreDrillProcessTests.cs`, `Backups/BackupSelfHealTests.cs`,
   `PgWorker.UnitTests/Provisioning/Fakes.cs`,
   `PgWorker.UnitTests/Backups/BackupProcessTests.cs` — удалить делегаты
   упразднённых методов;
   `PgWorker.UnitTests/Docker/BackupAgentNamesTests.cs` — удалить файл;
   `PgWorker.UnitTests/Docker/ClusterDriverTests.cs` — удалить тесты
   ensure/list/logs агентов, СОХРАНИТЬ/добавить тест санитарного сноса
   (`RemoveBackupAgentsAsync` сносит контейнеры `pgw-backup-wal-<C>-<X>-<N>`
   и `-staging`-тома, no-op при пустом листинге).
3. `PgWorker.Backups/WalStreamProcess.cs` в этом шаге ЕЩЁ не правится —
   компиляция сломается его вызовами `EnsureBackupAgentAsync`/
   `ListBackupAgentsAsync`/`BackupAgentLogsAsync`; их удаление — Шаг 7.
   Поэтому порядок внутри шага 6–7 единый: сначала механические правки
   драйвера/стабов (подпункт 6.1), затем Шаг 7, затем общая сборка. Проверка
   сборки — в конце Шага 7.

**Выход:** интерфейс драйвера без агентных API; санитарный снос на месте.

**Связь со spec:** §3.3, §7 №11; §5 (санитарный снос — stop/remove).

#### ПОДПУНКТ 6.1 (слабой модели)
Файл: `src/PgWorker.Docker/Drivers/ClusterDriver.cs`. Три точечные правки:

1. В теле `public interface IClusterDriver` УДАЛИТЬ ПОЛНОСТЬЮ три объявления
   (вместе с их doc-комментариями `///` и комментариями `//` над ними):
   `Task<Result> EnsureBackupAgentAsync(...);`,
   `Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(...);`,
   `Task<Result<string>> BackupAgentLogsAsync(...);`. Метод
   `Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct);`
   и его комментарий — ОСТАВИТЬ, заменив текст doc-комментария на:

   ```csharp
   // Санитарный снос legacy wal-контейнеров pgw-backup-wal-<C>-* (+ их
   // -staging-тома) — t25: приёмник живёт в контейнере ноды, отдельных
   // контейнеров больше нет; вызов идемпотентен, 404 = успех.
   ```

2. В `public sealed class PlainClusterDriver` УДАЛИТЬ ПОЛНОСТЬЮ реализации
   методов `EnsureBackupAgentAsync` и `BackupAgentLogsAsync` и
   `ListBackupAgentsAsync`. Реализацию `RemoveBackupAgentsAsync` оставить.
   Все обращения к `BackupAgentNames` внутри оставшегося кода класса заменить:
   - `ListAgentsOfEngineAsync`: `BackupAgentNames.Prefix(cluster)` → приватную
     строку-выражение `$"pgw-backup-wal-{cluster}-"`, введя в начале класса
     локальную переменную/параметр (допускается вычислить префикс дважды —
     одинаковым выражением).
3. В `public sealed class SwarmClusterDriver` УДАЛИТЬ ПОЛНОСТЬЮ реализации
   `EnsureBackupAgentAsync`, `ListBackupAgentsAsync`, `BackupAgentLogsAsync`.
   В `RemoveBackupAgentsAsync` заменить `BackupAgentNames.Prefix(cluster)` на
   `$"pgw-backup-wal-{cluster}-"` (в теле метода и в `AgentShardOfSwarm`,
     передавая префикс параметром или повторив выражение).

Также удалить файл `src/PgWorker.Docker/Drivers/BackupAgentNames.cs` целиком.
Проект после правок НЕ собирать (зависимые правки сделает другой исполнитель).

---

### Шаг 7. Воркер: WalStreamProcess по §3.4 + стоп-семантика backups-disabled

**Вход:** Шаг 6 (правки драйвера внесены; решение ещё не собрано).

**Действие (сильная модель):**
1. `src/PgWorker.Backups/WalReceiverStatusReader.cs` (новый): Range
   `/pgworker/backups/<C>/<X>/agent/` (failover-перебор endpoints, образец
   `WalStatusWriter.ReadAsync`) → `IReadOnlyList<WalReceiverAgentFacts>`
   (record `WalReceiverAgentFacts(string Node, WalReceiverRole Role,
   WalReceiverState State, bool PoolExhausted, WalReceiverTliJump? TliJump,
   string? Error)`; парсинг значений — `WalReceiverAgentStatusJson.Parse` из
   PgWorker.WalReceiver НЕ переиспользуется (Backups не ссылается на
   WalReceiver): продублировать парсинг DTO в Backups толерантно — незнакомые
   поля/битый JSON → пропуск записи). Роль/статус — строковые значения
   (`"master"/"sync"/"none"`, `"streaming"` и т.д.).
2. `src/PgWorker.Backups/BackupsDisabledGate.cs` (новый): ключ
   `/pgworker/backups-disabled`;
   `Task<Result> PutDisabledAsync(string[] endpoints, long unix, CancellationToken ct)`
   — plain put `{"updated_unix":T}` (failover); `Task<Result>
   DeleteAsync(string[] endpoints, CancellationToken ct)` — del (404-семантика
   движка = успех). Используется лидерным циклом (ниже).
3. `WalStreamProcess.cs` — переработка по §3.4 (сильная модель, по пунктам):
   - санитарный снос: в начале `TickAsync`, сразу после `claims.IsMine(cluster)`
     — `driver.RemoveBackupAgentsAsync(cluster, shard: null, ct)` (один list на
     тик кластера; найдены — снос; ошибка — исключение тика, идемпотентный
     ретрай следующим тиком);
   - `EnsureAgentsAsync`, `AgentEnv`, `BuildAgentFacts`,
     `ShardOfListedName`, `ParseTliJumpMarker`, `IsPoolExhaustionActive`,
     `DeliverMissingHistoriesAsync` — УДАЛИТЬ (вместе с константой
     `AgentLogTail`, `AgentCreateBudget`); `TliJumpLogMarker` — удалить;
   - agents-факты: вместо шага (4–5) — `WalReceiverStatusReader.Range`:
     ключ жив (есть в Range) → `WalAgentPresence.Running`, нет → `Absent`;
     `exited` новыми записями не пишется (legacy-значение парсера
     `WalStatusWriter.AgentOf` остаётся); факты пишутся в `agents` wal-ключа
     по нодам portalloc шарда (как раньше `BuildAgentFacts`), только при
     живом wal-ключе, `WriteIfChangedAsync`;
   - «источник без живого приёмника» (мастер или sync без running-ключа при
     не-BROKEN wal-ключе) → `DEGRADED` c `error="wal receiver not running on <N>"`
     (transient-семантика, приоритет после gap, до lag/тишины);
   - gap (tli_jump): источник — `TliJump`-факт статус-ключа МАСТЕР-приёмника
     (`WalReceiverStatusReader`); гейты установки/снятия прежние (покрытие
     COMPLETED-полным / исчезновение профиля из листинга); текст ошибки
     прежний (`tli-gap: ...` через `TliOfSegment`);
   - pool-exhaustion: маркер `S3PoolMarkers.Exhausted` в `error` wal-ключа —
     по флагу `PoolExhausted` статус-ключа мастер-приёмника (контроль каждый
     тик при exhaustion-DEGRADED — `IsExhaustionDegraded` остаётся);
   - `ApplyPoolResultAsync`: ветка «пул не выбран» — БЕЗ docker-вызова
     (journal `waiting-backup-pool` + `Observe(null,null)`; приёмники сами
     стоят в `waiting_policy`);
   - `StopAllAsync` (`Enabled=false`): БЕЗ `RemoveBackupAgentsAsync` — только
     финальные STOPPED-ключи живых шардов; `StopShardAsync` (QUARANTINED) —
     STOPPED-ключ, docker-вызовов нет; `BreakAsync` — BROKEN-ключ БЕЗ сноса
     агентов (слот пересоздаётся как сегодня);
   - `IsExhaustionDegraded`, `TliOfSegment`, контроль цепочки/лаг-зонд/
     слоты/гварды — БЕЗ изменений семантики.
4. Стоп-ключ `/pgworker/backups-disabled` (arch/19 §4: «пишет/стирает PgWorker
   (любой живой инстанс: при старте с `Backups:Enabled=true` — del; при
   `Enabled=false` — периодический put)»):
   - `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs` (единственный
     глобальный лидерный цикл бэкапов, `claims.IsLeader`/`LeaderToken` уже
     есть): в начале `ExecuteAsync` — при `options.CurrentValue.Backups.Enabled`
     вызвать `BackupsDisabledGate.DeleteAsync` (каждый инстанс при старте);
     в ветке `claims.IsLeader` при `!Backups.Enabled` — на каждом тике лидера
     `BackupsDisabledGate.PutDisabledAsync` (ключ plain — TTL нет, период
     тика достаточен);
   - воркер больше ничего с ключом не делает (приёмник читает сам).
5. Тесты (`WalStreamProcessTests` — сильная модель, AAA):
   - agents-факты: сид etcd-ключей `agent/<N>` (через `fixture.Gateway.PutAsync`)
     → тик → `agents` wal-ключа running/absent; ключа нет → absent; `exited`
     новыми записями не пишется;
   - DEGRADED not-running: мастер без agent-ключа при живом wal-ключе →
     DEGRADED `wal receiver not running on <N>`; с ключом → ACTIVE;
   - gap из `tli_jump`-факта: сид `agent/<master>` с `tli_jump` → wal-ключ
     `gap` + error `tli-gap:`; снятие покрытием (COMPLETED-полный с
     `wal_start >= to`);
   - exhaustion-маркер: сид `agent/<master>` с `pool_exhausted=true` → error с
     `s3-pool-exhausted`; флаг снят → маркер снят, контроль по общему порядку;
   - стоп-семантика: `Enabled=false` тик → wal-ключ STOPPED, НОЛЬ вызовов
     `RemoveBackupAgentsAsync` (счётчик fake-драйвера); лидер `BackupsDisabledGate`
     put (юнит через реальный etcd fixture: put/del ключа);
   - санитарный снос: сид `BackupAgentObjects` (legacy-имена) → тик →
     `RemovedBackupAgents` заполнен, повторный тик — no-op (счётчик не растёт);
   - «docker-вызовы агентов отсутствуют»: интерфейс больше не содержит
     ensure/logs — ассерт вырожден в компиляцию; сохранить ассерт нуля
     `ExecNodeCalls` в контрольном тике (history-доклад удалён);
   - `WalAgentFactsTests` — переписать под факты из etcd-ключей (или удалить,
     покрыв новыми тестами WalStreamProcessTests);
   - `BackupSelfHealTests`/`RestoreProcessTests`/`BackupVerifyProcessTests`/
     `RestoreDrillProcessTests` — починить под новый интерфейс драйвера
     (`RestoreProcess` продолжает звать `RemoveBackupAgentsAsync` — санитарно,
     идемпотентно; не менять).

**Выход:** WalStreamProcess без docker-агентов; стоп-семантика через etcd;
сборка решения зелёная.

**Проверка:**
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Error 0 Warning.
2. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~WalStreamProcess|FullyQualifiedName~WalAgentFacts|FullyQualifiedName~BackupSelfHeal|FullyQualifiedName~RestoreProcess|FullyQualifiedName~BackupVerify|FullyQualifiedName~RestoreDrill"` — зелёные.

**Связь со spec:** §3.4 п.1–6, §6 п.1; arch/19 §3 «Стоп-семантика»/§4.

---

### Шаг 8. Воркер: конфигурация — удаление Wal:AgentImage

**Вход:** Шаг 7 выполнен, сборка зелёная.

**Действие (сильная модель):**
1. `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`): удалить поле
   `WalAgentImage` (и упоминание в doc-комментарии).
2. `src/PgWorker.App/Options.cs`: `BackupsWalOptions` — удалить свойство
   `AgentImage`; `BackupsOptions.ToRuntime` — убрать именованный аргумент
   `WalAgentImage`; `BackupsOptions.IsValid` — убрать условие
   `!string.IsNullOrWhiteSpace(Wal.AgentImage)` (остаётся `Enabled →
   непустой S3.Bucket и Job.Image`); секция `BackupsAgentOptions`
   (`Agent.Cpu/Mem`) — оставить как есть (spec велит удалять только
   `Wal.AgentImage`).
3. `src/PgWorker.App/appsettings.json` — секция `Backups:Wal` уже не содержит
   `AgentImage` (проверить grep'ом; если встречается где-либо в
   `deploy/.env.example`/`appsettings*.json` — удалить строку).
4. Тесты: юниты валидации старта (`IsValid`) — обновить (без AgentImage);
   все env `PgWorker__Backups__Wal__AgentImage` в E2E-сценариях удаляются
   Шагом 12 (здесь только не-E2E упоминания).
5. `WalStreamProcess`-конструктор и вызовы `options.WalAgentImage` уже ушли в
   Шаге 7 — проверить grep'ом `WalAgentImage|AgentImage` по `src/` (кроме
   `Job.Image`/`JobImage`) — ноль вхождений.

**Выход:** конфиг без AgentImage; новых обязательных ключей нет.

**Проверка:** сборка решения зелёная; `grep -rn "WalAgentImage" src/` — пусто.

**Связь со spec:** §3.5.

---

### Шаг 9. SdGenerator: второй file_sd `walreceiver-nodes.json`

**Вход:** сборка зелёная (шаги 1–8).

**Действие (сильная модель):** `src/Metrics.SdGenerator`:
- `SdGeneratorOptions`: добавить свойство
  `public string WalOutputPath { get; set; } = "/sd/walreceiver-nodes.json";`
  (подпункт 9.1);
- `TargetMapping`: обобщить сериализацию —
  `Serialize(IReadOnlyList<SdTargetGroup> groups)` остаётся (порт 8008,
  Patroni), добавить `SerializeWal(IReadOnlyList<SdTargetGroup> groups)` —
  те же записи с таргетом `<alias>:8080` (http; const
  `WalReceiverMetricsPort = 8080`; записи без alias — advertised
  `<host>:8080` — скрейп приёмника без ScrapeNetwork недоступен, записи всё
  равно пишутся единообразно, arch/18 §5.4 «те же записи per-node, порт 8080»);
- `Program.cs`: второй `SdFileWriter` на `WalOutputPath` → в `SdGeneratorLoop`
  (конструктор получает второго writer) — тик пишет ОБА файла через
  `WriteIfChanged` (общий diff-кетчинг `SdFileWriter`);
- `appsettings.json` — ключ `"WalOutputPath": "/sd/walreceiver-nodes.json"`;
- юниты (`Metrics.SdGenerator.UnitTests`): второй файл — те же группы с
  портом 8080; атомарность/diff — прежними тестами на оба writer'а;
- integration-тесты `Metrics.SdGenerator.IntegrationTests` — при наличии
  ассертов файла добавить симметричные для `walreceiver-nodes.json`.

**Выход:** генератор пишет оба file_sd-файла.

**Проверка:** сборка решения; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~SdGenerator"` — зелёные.

**Связь со spec:** §3.6 (sd-generator + второй file_sd); arch/18 §5.4/§8.

#### ПОДПУНКТ 9.1 (слабой модели)
Файл: `src/Metrics.SdGenerator/SdGeneratorOptions.cs`. НАЙТИ строку

```csharp
    /// <summary>Путь file_sd-файла (volume Prometheus, маунт /sd).</summary>
    public string OutputPath { get; set; } = "/sd/patroni-nodes.json";
```

и СРАЗУ ПОСЛЕ неё вставить:

```csharp

    /// <summary>Путь второго file_sd-файла — приёмники WAL в нодах (t25,
    /// arch/18 §5.4/§8): те же записи per-node, порт 8080 (http).</summary>
    public string WalOutputPath { get; set; } = "/sd/walreceiver-nodes.json";
```

Файл `src/Metrics.SdGenerator/appsettings.json`: в объекте `"SdGenerator"`
после строки `    "OutputPath": "/sd/patroni-nodes.json",` вставить строку

```json
    "WalOutputPath": "/sd/walreceiver-nodes.json",
```

Оба файла сохранить; компиляцию отдельно не запускать.

---

### Шаг 10. Prometheus и дашборд стенда

**Вход:** Шаг 9 выполнен.

**Действие:** `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` —
добавить джобу (подпункт 10.1); `dev-stand/adminpanel/metrics/grafana/dashboards/backups.json`
— добавить панель «WAL receivers (per node)»: grid-панель с выражениями
`walreceiver_role`, `walreceiver_streaming`,
`rate(walreceiver_segments_delivered_total[5m])`, `walreceiver_backlog_bytes`,
`rate(walreceiver_put_errors_total[5m])` (лейблы cluster/shard/node; формат
существующих панелей файла — datasource/uid/gridPos по образцу соседних).
Новых alert-правил НЕТ (решение §7 №13).

**Выход:** джоба `walreceiver-nodes` в конфиге Prometheus; панель в дашборде.

**Проверка:** `python3 -c "import json;json.load(open('dev-stand/adminpanel/metrics/grafana/dashboards/backups.json'))"` — OK; `docker run --rm -v .../prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=... check` не требуется — валидация YAML глазами + прогон Шага 12.

**Связь со spec:** §3.6, §6 п.4, §7 №12/№13; arch/18 §5.2.

#### ПОДПУНКТ 10.1 (слабой модели)
Файл: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`. НАЙТИ блок

```yaml
  - job_name: sd-generator        # самонаблюдение генератора (arch/18 §5.4)
    static_configs: [{targets: ["sd-generator:8080"]}]
```

и ДОБАВИТЬ ПЕРЕД ним:

```yaml
  - job_name: walreceiver-nodes   # приёмники WAL в нодах PgWorker (t25, arch/18 §2.8/§5.2)
    scheme: http
    file_sd_configs:
      - files: ["/etc/prometheus/sd/walreceiver-nodes.json"]
        refresh_interval: 30s
```

Ничего больше не менять. Файл сохранить.

---

### Шаг 11. Инфраструктура: deploy-compose, 00-up.sh, E2eEnvironment, runbook

**Вход:** шаги 1–10 выполнены, сборка зелёная.

**Действие (сильная модель):**
1. Удалить файлы `docker/PgWorker.Wal.Dockerfile`,
   `docker/PgWorker.Wal.E2E.Dockerfile`; в `deploy/docker-compose.yml`
   удалить сервис `pgworker-wal` (build-профиль) целиком (подпункт 11.1).
2. `dev-stand/adminpanel/checks/00-up.sh`: заменить фазу «WAL-агент»
   (publish + `docker build -f docker/PgWorker.Wal.Dockerfile -t
   pgworker-wal:dev`) на двухшаговую сборку образа ноды (подпункт 11.2):
   publish self-contained (RID по `uname -m`) → `artifacts/node-wal` →
   `docker build -f docker/node/Dockerfile --build-arg
   WAL_PUBLISH_DIR=artifacts/node-wal -t pgworker-node:dev "$ROOT"`; все
   `[PHASE]`-метки и тайминги сохранить; образ `pgworker-wal:dev` больше не
   собирается.
3. `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`:
   - удалить `WalImage`-свойство и метод `EnsureWalImageAsync` (и вызов в
     `PrepareStaticAssetsAsync`, поле `_walImageReady`);
   - `EnsureStaticAsync`: publish WalReceiver self-contained (RID
     `linux-arm64` при `RuntimeInformation.ProcessArchitecture == Arm64`,
     иначе `linux-x64`) → `artifacts/e2e/node-wal` (с `NoMsBuildReuseEnv` и
     бюджетом по канону `EnsureWalImageAsync`), затем сборка образа ноды
     становится двухшаговой: существующий `docker build -f docker/node/
     Dockerfile -t pgworker-node:e2e-<suffix> <root>` ДОПОЛНИТЬ
     `--build-arg WAL_PUBLISH_DIR=artifacts/e2e/node-wal` (контекст — корень,
     как сегодня; лог — `[PHASE]` + тайминг);
   - `VerifyStaticReadyOrThrowAsync` — проверка образов остаётся
     (`WorkerImage`, `RunnerImage`, `NodeImage`).
4. `docs/runbook.md`: минимальная заметка (подпункт 11.3): двухшаговая сборка
   `pgworker-node` (publish приёмника на хосте + build с
   `WAL_PUBLISH_DIR`), обновление живых нод (recreate-API / `docker rm -f
   pgw-…` → надзор поднимет с новым образом; данные в volume сохраняются),
   сервис `pgworker-wal` снесён; из таблицы «Каталог зеркалируемых образов»
   строку про `minio/mc` (WAL-агенты) не трогать (mc остался для джобов).
5. Проверить отсутствие упоминаний `pgworker-wal` в репо (кроме roadmap/
   истории): `grep -rn "pgworker-wal" --include="*.cs" --include="*.yml" \
   --include="*.sh" --include="*.json" src/ deploy/ dev-stand/ docker/` —
   пусто.

**Выход:** образы wal упразднены; стенд/E2E собирают ноду с приёмником.

**Проверка:** сборка решения зелёная; `bash -n dev-stand/adminpanel/checks/00-up.sh` — синтаксис OK; grep-проверка выше — пусто.

**Связь со spec:** §3.6, §3.7, §7 №19.

#### ПОДПУНКТ 11.1 (слабой модели)
Файл: `deploy/docker-compose.yml`. НАЙТИ и УДАЛИТЬ ПОЛНОСТЬЮ блок (включая
комментарии над ним и одну пустую строку до следующего раздела):

```yaml
  # Образ wal-агента (t27/t41, arch/19 §3): publish НА ХОСТЕ (00-up.sh,
  # [PHASE]) → compose build пакует только вывод (канон E2E-образов).
  pgworker-wal:
    build:
      context: ..
      dockerfile: docker/PgWorker.Wal.Dockerfile
      args:
        WAL_PUBLISH_DIR: artifacts/wal/publish
    image: pgworker-wal:dev
    profiles: ["build"]
    entrypoint: ["true"]
```

Также удалить файлы `docker/PgWorker.Wal.Dockerfile` и
`docker/PgWorker.Wal.E2E.Dockerfile` (полностью).

#### ПОДПУНКТ 11.2 (слабой модели)
Файл: `dev-stand/adminpanel/checks/00-up.sh`. НАЙТИ блок (строки с комментарием
«# WAL-агент (t41): publish НА ХОСТЕ...» по строку `echo "  image
pgworker-wal:dev ready"`) и ЗАМЕНИТЬ его целиком на:

```bash
# Образ PG-ноды с приёмником WAL (t25, arch/19 §3/§14 §2.1): publish НА ХОСТЕ
# self-contained (RID хоста сборки, без trimming) → docker build пакует вывод
# в /opt/pgw-wal (канон «publish на хосте, в контейнер — артефакты»; сборка не
# «тихая» — [PHASE] и тайминг; отдельного образа pgworker-wal больше нет).
case "$(uname -m)" in
  arm64|aarch64) WAL_RID=linux-arm64 ;;
  *)             WAL_RID=linux-x64 ;;
esac
echo ">>> [PHASE] publish PgWorker.WalReceiver self-contained $WAL_RID ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" \
  -c Release -r "$WAL_RID" --self-contained -o "$ROOT/artifacts/node-wal" --nologo \
  || { echo "❌ WalReceiver publish failed"; exit 1; }
echo ">>> [PHASE] pgworker-node:dev build (WAL_PUBLISH_DIR=artifacts/node-wal) ($(date +%H:%M:%S))"
docker build -q -f "$ROOT/docker/node/Dockerfile" \
  --build-arg "WAL_PUBLISH_DIR=artifacts/node-wal" -t pgworker-node:dev "$ROOT" \
  || { echo "❌ image pgworker-node:dev build failed"; exit 1; }
echo "  image pgworker-node:dev ready (walreceiver inside)"
```

Больше ничего в файле не менять. Файл сохранить.

#### ПОДПУНКТ 11.3 (слабой модели)
Файл: `docs/runbook.md`. Вставить в раздел про сборку образов (после абзаца
«⚠️ Локально собираемые образы в registry НЕ класть — никогда») следующий
подраздел:

```markdown
### Образ PG-ноды с приёмником WAL (двухшаговая сборка)

Приёмник WAL бэкапов исполняется процессом ВНУТРИ контейнера PG-ноды
(supervisord-программа `walreceiver`, `/opt/pgw-wal`). Отдельного образа
`pgworker-wal` и контейнеров `pgw-backup-wal-*` больше нет; сборка ноды —
двухшаговая (publish на хосте, в контейнер — только артефакты):

    dotnet publish src/PgWorker.WalReceiver -c Release -r <rid> \
      --self-contained -o artifacts/node-wal        # rid: linux-arm64 | linux-x64
    docker build -f docker/node/Dockerfile \
      --build-arg WAL_PUBLISH_DIR=artifacts/node-wal -t pgworker-node:dev .

Полный стенд собирает образ скриптом `dev-stand/adminpanel/checks/00-up.sh`
(RID определяется по `uname -m` автоматически).

Обновление живых кластеров на новый образ ноды: надзор НЕ пересоздаёт живые
ноды — приёмники появятся после пересоздания нод штатным механизмом (recreate
через панель/API либо `docker rm -f pgw-…` на docker-хосте — надзор поднимет
контейнер с новым образом; данные в volume сохраняются). До пересоздания
контроль воркера показывает DEGRADED «источник без живого приёмника» с именем
ноды. Остатки старых wal-контейнеров `pgw-backup-wal-*` сносятся воркером
автоматически (санитарный вызов в тике WalStreamProcess), руками чистить не
нужно.
```

Файл сохранить.

---

### Шаг 12. E2E: адаптация сценариев + прогоны

**Вход:** шаги 1–11 выполнены; сборка зелёная.

**Действие (сильная модель):**
1. Удалить env `["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage`
   во ВСЕХ сценариях (18 файлов: `E2eWalStreamScenarios`,
   `E2eWalStreamTliScenarios`, `E2eWalStreamPromoteScenarios`,
   `E2eWalStreamMasterDownScenarios`, `E2eTliSwitchScenarios`,
   `E2eBackupPoolScenarios`, `E2eBackupJobsScenarios`,
   `E2eBackupVerifyScenarios`, `E2eBackupFailsOnBadS3Scenarios`,
   `E2eDrillScenarios`, `E2eRestoreScenarios`, `E2eRestoreNewClusterScenarios`,
   `E2eRetentionScenarios`, `E2eSupervisorSelfHealScenarios`,
   `E2eSupervisorOrphanRegistryScenarios`, `E2eSupervisorOrphanDrHoldScenarios`
   — точный список даёт `grep -rn "Wal__AgentImage" src/tests/`).
2. Ассерты wal-потока перевести с `docker ps pgw-backup-wal-*` на
   agent-ключи etcd и объекты S3 (spec §6 п.3): хелпер чтения
   `/pgworker/backups/<C>/<X>/agent/` через `Gateway.RangeAsync` +
   `WalReceiverAgentStatusJson`-совместимый парсинг (сценарии в
   IntegrationTests могут ссылаться на PgWorker.WalReceiver напрямую);
   проверки «обе ноды-источники стримят» → agent-ключи обеих с
   `state=streaming` + сегменты в S3; «async-реплика не стримит» → ключ с
   `state=waiting_role`.
3. Специфика t25 (новые/адаптированные кейсы, `E2eWalStreamScenarios`/`E2eBackupPoolScenarios`):
   - санитарный снос: перед стартом кластера поднять контейнер-имитацию
     `docker run -d --name pgw-backup-wal-<C>-<X>-<N> quay.io/coreos/etcd:v3.5.21 sleep infinity`
     (образ из локального зеркала; имя — легаси-формат) → после первого тика
     воркера контейнер снесён (`docker ps -a` пуст по фильтру), новый приёмник
     стримит (слот свободен);
   - исчерпание write-списка: stop нод пула кроме read-цели → флаг
     `pool_exhausted=true` в agent-ключе → DEGRADED wal-ключа с
     `s3-pool-exhausted`; подъём ноды — self-heal (флаг снят, ACTIVE);
   - `Enabled=false` → ключ `/pgworker/backups-disabled` в etcd жив,
     приёмники `waiting_disabled`/`paused`; enable → ключ снят, стрим
     возобновился;
   - tli-сценарии (`E2eWalStreamTliScenarios`, `E2eTliSwitchScenarios`):
     `tli_jump`-факт в agent-ключе (не лог), gap в wal-ключе, восстановление
     покрытием; `.history`-объекты появляются от приёмника (без docker-exec).
     Ожидаемый механизм доставки `.history` в `E2eTliSwitchScenarios` (PITR
     target_time по старому полному через двойной промоут TLI1→TLI2→TLI3):
     `.history` перехода, случившегося ВНЕ окна стрима приёмника (промоут,
     который стримящая сессия не наблюдала), доставляется РЕ-ДОКЛАДОМ
     инвентаря приёмника ноды, физически содержащей файл (после промоута
     файл лежит в `pg_wal` нового мастера; ре-доклад — при следующем
     (ре)старте/цикле подключения, spec §3.1 п.6, решение §7 №9). Ассерты
     сценария НЕ меняются: гейт `wal/{tli:x8}.history` в list-S3 и
     `WalChain.Check` закрываются любым из двух путей (наблюдаемый переход в
     сессии / ре-доклад инвентаря); при разборе различать источник по
     diagnostic-логу walreceiver (`history inventory: put …`).
4. Прогоны (каждый — ОТДЕЛЬНОЙ командой; перед docker-E2E — подготовка):
   - сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`;
   - подготовка: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`;
   - интеграции docker: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~NodeWalReceiver"` — зелёный;
   - wal/tli/pool-серии: тот же шаблон с
     `--filter "FullyQualifiedName~E2eWalStream|FullyQualifiedName~E2eBackupPool"` — зелёные;
   - расширенный набор по §6 п.3: `E2eBackupJobs|E2eBackupVerify|
     E2eBackupFailsOnBadS3|E2eDrill` — зелёные (skip'ы оркестратора — штатный исход);
   - кейс-маркер гейта: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` — зелёный.
   После КАЖДОЙ серии: дождаться финальной строки, зачистить контейнеры/сети
   (`docker ps -a | grep pgw-`, `docker network prune -f` при осиротевших
   `pgw-net-*`); упавший тест — `MarkFailed`-телеметрия в
   `/tmp/pgw-e2e-artifacts-<guid>/`, разбор по логам БЕЗ перезапуска.

**Выход:** E2E-серии адаптированы и зелёные на свежем Release.

**Проверка:** все команды выше зелёные (вторая фаза `--no-build` после
отдельной сборки).

**Связь со spec:** §4 фаза 6, §6 п.1–5; AGENTS.md (E2E-каноны).

---

### Шаг 13. Мерж-гейт (вне автономного исполнения)

**Вход:** шаги 1–12 выполнены; сборка/тесты/E2E зелёные; код-ревью Фазы 7 и
mr-ревью пройдены.

**Действие:** ИСПОЛНЯЕТСЯ ТОЛЬКО ПО ЯВНОМУ ПРИКАЗУ ПОЛЬЗОВАТЕЛЯ (Фаза 8).
Автономный исполнитель этот шаг НЕ выполняет — git-гейт Mimosa блокирует
коммиты. Содержимое единственного коммита (код + arch-правки spec §8 +
spec.md/plan.md + roadmap-правки):

1. `arch/roadmap/backup.md`:
   - удалить пункт `t25-wal-receiver-in-pgnode` целиком (вместе с
     упоминанием arch/02 — снимается вместе с пунктом, решение §7 №15);
   - пункт `t24-job-status-channel` — убрать АГЕНТНУЮ часть: из текста
     удаляются «и stdout-маркеры wal-агента (фазы/`tli_jump` —
     `WalStreamProcess`, `BackupAgentLogsAsync`)», «Джобы и агент пишут...» →
     «Джобы пишут...», «(№21–22» → «(№22»; остаётся ТОЛЬКО джобовая часть
     (фазы/result-JSON backup-джобов full/verify/drill/restore через
     `BackupProcess`/`BackupVerifyProcess`/`RestoreProcess`/
     `RestoreDrillProcess`, отклонение №22);
   - зависимость `t26 ← t25` — СОХРАНЯЕТСЯ как есть.
2. Весь код/инфраструктура шагов 1–12; arch-правки, уже лежащие в worktree
   (spec §8); документы задачи
   (`docs/superpowers/2026-10-24-t25-wal-receiver-in-pgnode/spec.md`,
   `plan.md`).
3. Перед коммитом — контроль: `grep -rn "t25" arch/roadmap/*.md` — только
   зависимость `t26 ← t25`; `grep -rn "pgworker-wal\|BackupAgentNames\|WalAgentImage" src/ deploy/ dev-stand/ docker/` — пусто.

**Выход:** единый мерж-коммит (по приказу пользователя).

**Проверка:** мерж-гейт Фазы 8 (пользовательское ревью).

**Связь со spec:** §6 п.6, §7 №15.

---

## Контроль отсутствия развилок (самопроверка плана)

Все решения реализации ниже предопределены spec §7 и выписаны в шаги:

- надзор внутри ноды — supervisord `autorestart=unexpected`/`exitcodes=0` (§7 №1) → Шаг 4;
- self-contained publish без trimming, RID хоста (§7 №2) → Шаги 3/11/12;
- идентичность — env `PGW_BACKUP_*` от EnsureNode/SpiloEnvBuilder, всегда (§7 №3/№16) → Шаг 5;
- роль — REST-пробы `/primary`,`/sync` loopback c REST-CA ноды (§7 №4) → Шаг 1;
- стримят мастер+sync, приёмник во всех нодах (§7 №5) → Шаги 1/2/7;
- статус — lease TTL 15 c/keepalive 5 c, `exited` новыми записями не пишется (§7 №6) → Шаги 1/2/7;
- `backups-disabled` — plain-ключ: put лидером `BackupOrphanSweeperLoop`, del каждым инстансом при старте `Enabled=true` (§7 №7 + arch/19 §4 дословно) → Шаг 7;
- `pool_exhausted`/`tli_jump` — факты статус-ключа (§7 №8) → Шаги 2/7;
- `.history`-fallback — чтение `pg_wal` приёмником; docker-exec-доклад
  удаляется; ре-доклад инвентаря `.history` (переходы вне окна стрима):
  перечисление `pg_wal` + list `wal/` + одиночный put отсутствующих вне
  hot-path, отказ — diagnostic-log (§7 №9 дополнено) → Шаг 2 (метод/вызов/
  юниты), Шаг 12 (E2E-механизм tli-кейса);
- креды/цели — резолв из etcd на каждом цикле; bucket/CA — единственные env (§7 №10) → Шаги 1/5;
- санитарный снос legacy — тик `WalStreamProcess`, stop/remove (§7 №11) → Шаги 6/7;
- метрики — Kestrel `:8080` внутри контейнера, второй file_sd, скрейп по ScrapeNetwork; новых алертов нет (§7 №12/№13) → Шаги 2/9/10;
- усыновлённые ноды — вне подсистемы (§7 №14) — код не трогает;
- `WalReceiverCore` переиспользуется, exit-коды 3/4/5/6 и stdout-маркеры упраздняются (§7 №17) → Шаг 2;
- панель не меняется (§7 №18); runbook — минимальная правка (§7 №19) → Шаг 11.

Незакрытых развилок нет; NEEDS_CONTEXT отсутствует.
