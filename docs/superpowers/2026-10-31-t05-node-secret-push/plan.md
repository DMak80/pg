# t05-node-secret-push — план реализации (Фаза 3 dev-flow, рев. 5)

> **Для исполнителя (субагент coder):** работать по шагам, следом за заголовком
> «Глобальные ограничения». Каждый шаг: Вход → Действие → Выход → Проверка →
> Связь со spec. Подпункты в конце шага — самодостаточные спецификации
> механической работы (по умолчанию исполняет сам кодер; по явной просьбе
> пользователя они передаются слабой модели coder-flash без остального
> контекста — поэтому подпункт самодостаточен и не ссылается на другие шаги).

**Цель:** контейнер PG-ноды создаётся без секретов в docker-env; секреты
доставляет агент PgNode через хендшейк по статусной машине
(`not_initialized → initializing → ready | start_failed`, статусный ключ etcd
с lease): per-cluster/пер-нодовое — bootstrap-ключ etcd, per-install пароли PG
— прямой mTLS-push воркера; ротация `rest_password` — командой применения без
пересоздания контейнеров; `start_failed` — пересоздание ноды с новым диском;
компонент walreceiver переименован в PgNode.

**Архитектура:** воркер пишет per-node bootstrap-ключ
`/clusters/<C>/shards/<X>/nodes/<n>/bootstrap` (JSON: `spilo_configuration`,
`cert_pem`, `key_pem`) и мониторит статусный ключ агента
`/pgworker/nodes/<C>/<X>/<N>/status` (lease TTL 15 c, пишет только агент).
По `not_initialized` воркер шлёт `POST /api/bootstrap` (mTLS `:8081`, порт
внутри docker-сети) с SuPassword/StandbyPassword; агент валидирует аргументы,
атомарно (txn compare) переводит машину в `initializing`, рендерит tmpfs
`/run/pgw`, стартует компоненты (≤3 попытки/компонент) и ждёт `SELECT 1`
без бюджета → `ready` (`applied_revision` = `mod_revision` ключа). Ротации —
`POST /api/bootstrap/apply` только ready-нодам; `start_failed` — пересоздание
ноды kill+remove+remove volume+ensure заново.

**Стек:** .NET 10 (C# latest, Nullable, TreatWarningsAsErrors), Kestrel,
System.Text.Json, Npgsql, docker Engine API (Shared.Docker), etcd HTTP JSON
gateway (lease/txn). P/Invoke запрещён.

**Spec:** `docs/superpowers/2026-10-31-t05-node-secret-push/spec.md` (рядом,
ревизия со статусной машиной и хендшейком). Канон — arch/14 §2.1/§3.2/§3.3/§4/
§5 C/§5 I, arch/13 §2/§4/§5, arch/25 №23, arch/18 §2.8/§5.4, arch/19
(arch-first правки уже в worktree — код зеркалит их).

Все пути ниже — от корня worktree
`/Users/demakaev/ZCodeProject/worktrees/feat-t05-node-secret-push`.
Коммитить в feature-ветку свободно, по шагам. Комментарии в коде — по-русски.

## Глобальные ограничения

- P/Invoke (`DllImport`/`LibraryImport`/нативные вызовы) — запрещён; только
  управляемый .NET API.
- Таймауты/бюджеты существующих фаз НЕ менять: `BrokerBootSec` ≤ 100 с; тик
  ~5 с; клиент канала — HttpClient 10 с (существующий уровень); интервалы
  проб/поллинга агента — 5 с (кратные тику, паттерн `IdlePause`/
  `DefaultKeepalivePeriod`); Npgsql-пробы `SELECT 1` — Connection Timeout=5
  (кратный интервалу опроса — единственные новые «числа», разрешённые spec
  §8.17). Фаза `initializing`-проб (б) — БЕЗ бюджета (spec §3.6-3б).
- E2E/интеграции: полное самоочищение (teardown при любом исходе, ассерт
  чистоты), только динамические порты (зонд/`GetMappedPublicPort`), guid в
  именах; зачистка контейнеров И сетей после каждой серии.
- Образы для E2E: publish на хосте, в контейнер только артефакты (без
  sdk-стадий/COPY исходников); сборка и тесты — ОТДЕЛЬНЫМИ командами.
- Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
  Release --nologo` — 0 warnings/0 errors.
- Команда `fetch-wal`, env `PGW_WALRECEIVER_NO_STREAM`, метрики-серии
  `walreceiver_*`, классы `WalReceiverLoop`/`WalReceiverCore`/
  `WalReceiverStatusSession`, ключи `/pgworker/backups/.../agent/<N>`,
  legacy-снос `pgw-backup-wal-<C>-*`, env `PGW_BACKUP_*` — НЕ переименовываются
  (функция приёма WAL; spec §3.8).
- Решение пользователя: tmpfs-файл env-паролей ноды — `/run/pgw/spilo-env`.
- Канал несёт ТОЛЬКО команды (`POST /api/bootstrap`, `POST
  /api/bootstrap/apply`); статус воркер читает из etcd — GET-эндпоинта канала
  НЕТ (spec §3.4, §8.20).

## Review Focus (риски, не покрываемые напрямую тестами шагов — проверить в коде)

1. **Утечка секретов в `docker inspect`** — любое новое место сборки env ноды
   обязано проходить через `SpiloEnvBuilder.Build` (шаг 3); негативный ассерт —
   E2E шага 13.
2. **Дребезг `mod_revision`** (пересериализация JSON меняет байты → вечные
   команды) — сериализация симметрична (один builder, детерминированные
   опции), put только при изменении содержимого; юнит «повтор без изменений —
   нет put/команд» (шаг 9).
3. **Ок-пинок / повторная инициализация**: команда доставки при состоянии ≠
   `not_initialized` обязана быть no-op (атомарный txn compare; проигрыш —
   `200 ok`); юниты гонки (шаг 6).
4. **Окно ротации: ноды разных поколений** (dual-auth у Patroni нет, 401
   между нодами) — гварды окна (skip PATCH `/config`, RecreateMarked-soft)
   сохранены; порядок реплики-вперёд/лидер-последним, ≤1 нода/тик (шаг 11).
5. **`start_failed` → потеря данных ноды** (новый диск) — реакция строго по
   границам: `object`-ноды, restore/TO_REMOVE-шарды, QUARANTINED/REMOVING —
   вне; E2E-сценарий инжекции сбоя (шаг 13).
6. **API-тесты Program агента** (`PGW_WALRECEIVER_NO_STREAM=1`) не должны
   сломаться вторым листенером `:8081` — под TestServer
   `ConfigureKestrel`-листенеры не bind'ятся; прогон существующих API-тестов —
   проверка шага 7.

---

## Шаг 1. Переименование компонента WalReceiver → PgNode (только имена)

**Вход:** worktree чист, arch-first правки внесены; сборка зелёная на старте.

**Действие (кодер):** чистое переименование по границе spec §3.8 — проект,
namespace, бинарь, программа supervisord, идентичность/статус-классы, Meter,
джоба/file_sd, опция генератора; ПОВЕДЕНИЕ системы не меняется (autostart/
entrypoint-логику трогаем только именами — поведение меняет шаг 12). Каталоги
publish-артефактов (`artifacts/node-wal`, `artifacts/e2e/node-wal`) НЕ
переименовываются — они вне перечня §3.8 (внутренние пути сборки, не имена
компонента). Комментарии в коде/скриптах, называющие компонент «walreceiver»,
обновляются по месту («агент PgNode», «бинарь pgnode»); комментарии про функцию
приёма WAL не трогать.

**Выход:** проект `src/PgWorker.PgNode`, namespace `PgWorker.PgNode`, бинарь
`pgnode`, сборка Release 0w/0e, тесты переименованы и зелёные.

**Проверка:**
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
→ 0w/0e; затем
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build`
→ зелёные.

**Связь со spec:** §3.8 (таблица переименований + «остаются как есть»), §4.2.

**Подпункты (механика для слабой модели):**

- П1.1. Переименовать проект: `git mv src/PgWorker.WalReceiver src/PgWorker.PgNode`;
  в `src/PgWorker.PgNode/PgWorker.PgNode.csproj` (файл переименовать из
  `PgWorker.WalReceiver.csproj`): `<AssemblyName>walreceiver</AssemblyName>` →
  `<AssemblyName>pgnode</AssemblyName>`; XML-комментарий сверху: «supervisord-программа
  walreceiver, self-contained publish — см. docker/node/Dockerfile» заменить на
  «supervisord-программа pgnode, self-contained publish — см.
  docker/node/Dockerfile» и «Приёмник WAL (t25, arch/19 §3)» → «Агент PgNode
  (t25/t05, arch/19 §3, arch/14 §2.1)». Остальное содержимое csproj не менять.
- П1.2. Во ВСЕХ файлах каталога `src/PgWorker.PgNode/` (включая
  `Operations/`): заменить `namespace PgWorker.WalReceiver` → `namespace
  PgWorker.PgNode` и `using PgWorker.WalReceiver` → `using PgWorker.PgNode`.
  Больше никаких замен имён в этих файлах не делать.
- П1.3. В файле `src/PgWorker.slnx`: заменить каждое вхождение
  `PgWorker.WalReceiver` на `PgWorker.PgNode` (путь проекта).
- П1.4. В трёх тест-проектах (`src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`,
  `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj`,
  `src/tests/PgWorker.E2eTests/PgWorker.E2eTests.csproj`): заменить
  ProjectReference `PgWorker.WalReceiver\PgWorker.WalReceiver.csproj` →
  `PgWorker.PgNode\PgWorker.PgNode.csproj` (в существующем относительном пути
  заменить только имя проекта).
- П1.5. Класс `WalReceiverIdentity` → `PgNodeIdentity`: в файле
  `src/PgWorker.PgNode/WalReceiverIdentity.cs` (переименовать файл в
  `PgNodeIdentity.cs`) заменить `public sealed record WalReceiverIdentity(` →
  `public sealed record PgNodeIdentity(` и `public static WalReceiverIdentity?
  Parse` → `public static PgNodeIdentity? Parse`. Затем по всему `src/` и
  `src/tests/` заменить тип `WalReceiverIdentity` → `PgNodeIdentity` (файлы:
  `src/PgWorker.PgNode/Program.cs`, `src/PgWorker.PgNode/WalReceiverLoop.cs`,
  `src/PgWorker.PgNode/Operations/FullBackupOperationFactory.cs` и все
  прочие вхождения по grep `WalReceiverIdentity`). Тест-файл
  `src/tests/PgWorker.UnitTests/Backups/WalReceiverIdentityTests.cs`
  переименовать в `PgNodeIdentityTests.cs`, класс внутри — также.
- П1.6. Класс `WalReceiverAgentStatus` → `PgNodeAgentStatus`: в файле
  `src/PgWorker.PgNode/WalReceiverAgentStatus.cs` (переименовать файл в
  `PgNodeAgentStatus.cs`) заменить `public sealed record
  WalReceiverAgentStatus(` → `public sealed record PgNodeAgentStatus(`;
  статический класс `WalReceiverAgentStatusJson` → `PgNodeAgentStatusJson`
  (вместе с упоминаниями типа `WalReceiverAgentStatus` внутри него). Enum'ы
  `WalReceiverRole`, `WalReceiverState`, `WalReceiverTliJump` в этом файле НЕ
  трогать. По всему `src/` заменить тип `WalReceiverAgentStatus` →
  `PgNodeAgentStatus` и `WalReceiverAgentStatusJson` → `PgNodeAgentStatusJson`
  (включая `src/PgWorker.Backups/WalReceiverStatusReader.cs` и тесты).
  Тест-файл `src/tests/PgWorker.UnitTests/Backups/WalReceiverAgentStatusJsonTests.cs`
  переименовать в `PgNodeAgentStatusJsonTests.cs` (класс внутри — также).
- П1.7. В `src/PgWorker.PgNode/WalReceiverMetrics.cs`: заменить
  `public const string MeterName = "PgWorker.WalReceiver";` → `public const
  string MeterName = "PgWorker.PgNode";`. Имена серий `walreceiver_*` и класс
  `WalReceiverMetrics` НЕ менять.
- П1.8. `docker/node/supervisord.conf`: секцию `[program:walreceiver]` →
  `[program:pgnode]`; строку `command=/opt/pgw-wal/walreceiver` →
  `command=/opt/pgw-node/pgnode`; комментарий «Приёмник WAL бэкапов (t25,
  arch/19 §3): self-contained бинарь /opt/pgw-wal» → «Агент PgNode (t25/t05,
  arch/19 §3, arch/14 §2.1): self-contained бинарь /opt/pgw-node». Остальные
  директивы секции (priority=40, user=postgres, autorestart=unexpected,
  exitcodes=0, startsecs=5, startretries=0, stopasgroup=true, killasgroup=true,
  логи) оставить без изменений.
- П1.9. `docker/node/Dockerfile`: `ARG WAL_PUBLISH_DIR=artifacts/node-wal` →
  `ARG PGNODE_PUBLISH_DIR=artifacts/node-wal` (значение пути не менять);
  `COPY ${WAL_PUBLISH_DIR} /opt/pgw-wal/` → `COPY ${PGNODE_PUBLISH_DIR}
  /opt/pgw-node/`; `RUN chmod +x /opt/pgw-wal/walreceiver` → `RUN chmod +x
  /opt/pgw-node/pgnode`; в комментарии блока: `dotnet publish
  src/PgWorker.WalReceiver -c Release -r <rid> \ --self-contained -o
  artifacts/node-wal` → `dotnet publish src/PgWorker.PgNode -c Release -r
  <rid> \ --self-contained -o artifacts/node-wal`, `--build-arg
  WAL_PUBLISH_DIR=artifacts/node-wal` → `--build-arg
  PGNODE_PUBLISH_DIR=artifacts/node-wal`; заголовок комментария «Приёмник WAL
  (t25, arch/19 §3 + arch/14 §2.1)» → «Агент PgNode (t25/t05, arch/19 §3 +
  arch/14 §2.1)».
- П1.10. `docker/node/docker-entrypoint.sh`: строку `exec
  /opt/pgw-wal/walreceiver` → `exec /opt/pgw-node/pgnode`; комментарий
  «WalReceiver — главный процесс» → «агент PgNode — главный процесс».
- П1.11. `src/PgWorker.PgNode/Operations/RestoreOperation.cs`, строка
  `"restore_command = '/opt/pgw-wal/walreceiver fetch-wal %f %p',"` →
  `"restore_command = '/opt/pgw-node/pgnode fetch-wal %f %p',"`. Тест
  `src/tests/PgWorker.UnitTests/WalReceiver/RestoreOperationTests.cs:235`:
  ассерт `"restore_command = '/opt/pgw-wal/walreceiver fetch-wal %f %p'"` →
  `"restore_command = '/opt/pgw-node/pgnode fetch-wal %f %p'"`.
- П1.12. `src/tests/PgWorker.IntegrationTests/Backups/NodeWalReceiverTests.cs`:
  переименовать файл в `NodePgNodeTests.cs`, класс `NodeWalReceiverTests` →
  `NodePgNodeTests`; строку `["pkill", "-9", "-f", "/opt/pgw-wal/walreceiver"]`
  → `["pkill", "-9", "-f", "/opt/pgw-node/pgnode"]`.
- П1.13. `src/Metrics.SdGenerator`: в `SdGeneratorOptions.cs` свойство
  `public string WalOutputPath { get; set; } = "/sd/walreceiver-nodes.json";`
  → `public string PgNodeOutputPath { get; set; } =
  "/sd/pgnode-nodes.json";` (XML-комментарий обновить: «file_sd агенты PgNode
  в нодах»); в `Program.cs` строку `new SdFileWriter(options.WalOutputPath),`
  → `new SdFileWriter(options.PgNodeOutputPath),`; в `appsettings.json`
  `"WalOutputPath": "/sd/walreceiver-nodes.json",` → `"PgNodeOutputPath":
  "/sd/pgnode-nodes.json",`; комментарии в `SdGeneratorLoop.cs`/`Program.cs`,
  называющие файл `walreceiver-nodes`, — переименовать в `pgnode-nodes`.
  Тесты: `src/tests/Metrics.SdGenerator.UnitTests/SdGeneratorLoopTests.cs` и
  `src/tests/Metrics.SdGenerator.IntegrationTests/SdGeneratorIntegrationTests.cs`
  — заменить `"walreceiver-nodes.json"` → `"pgnode-nodes.json"` и обращения к
  опции `WalOutputPath` → `PgNodeOutputPath` (включая appsettings-конфиг
  тестов, если задают).
- П1.14. `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`: `job_name:
  walreceiver-nodes` → `job_name: pgnode-nodes`; `- files:
  ["/etc/prometheus/sd/walreceiver-nodes.json"]` → `- files:
  ["/etc/prometheus/sd/pgnode-nodes.json"]` (комментарий строки обновить:
  «агенты PgNode в нодах»).
- П1.15. `dev-stand/adminpanel/checks/00-up.sh`: `dotnet publish
  "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj"` → `dotnet
  publish "$ROOT/src/PgWorker.PgNode/PgWorker.PgNode.csproj"`; `--build-arg
  "WAL_PUBLISH_DIR=artifacts/node-wal"` → `--build-arg
  "PGNODE_PUBLISH_DIR=artifacts/node-wal"`; строки [PHASE]/fail-сообщений с
  `WalReceiver`/`walreceiver` → `PgNode`/`pgnode`; финальную строку `image
  pgworker-node:dev ready (walreceiver + pgwnodetool inside)` → `…(pgnode +
  pgwnodetool inside)`.
- П1.16. `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` (метод
  `EnsureStaticAsync`, строки ~1912–1943): путь publish
  `$"{_root}/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj"` →
  `$"{_root}/src/PgWorker.PgNode/PgWorker.PgNode.csproj"`; build-arg
  `"WAL_PUBLISH_DIR=artifacts/e2e/node-wal"` → `"PGNODE_PUBLISH_DIR=artifacts/e2e/node-wal"`;
  каталог `artifacts/e2e/node-wal` и лог-имя `node-wal-publish` НЕ менять;
  [PHASE]-строки с `WalReceiver` → `PgNode`; строку `(walreceiver and
  pgwnodetool inside)` → `(pgnode and pgwnodetool inside)`.
- П1.17. Каталог `src/tests/PgWorker.UnitTests/WalReceiver` переименовать в
  `src/tests/PgWorker.UnitTests/PgNode` (`git mv`); внутри пяти его файлов
  заменить `using PgWorker.WalReceiver` → `using PgWorker.PgNode` и
  `namespace PgWorker.UnitTests.WalReceiver` → `namespace
  PgWorker.UnitTests.PgNode` (если такой namespace используется). В файлах
  `src/tests/PgWorker.UnitTests/Backups/*.cs` и
  `src/tests/PgWorker.IntegrationTests/Backups/BackupApiOperationTests.cs`
  заменить `using PgWorker.WalReceiver` → `using PgWorker.PgNode`.
- П1.18. `docs/runbook.md`: строки ~192–196 — «supervisord-программа
  `walreceiver`, `/opt/pgw-wal`» → «supervisord-программа `pgnode`,
  `/opt/pgw-node`»; `dotnet publish src/PgWorker.WalReceiver -c Release -r
  <rid> --self-contained -o artifacts/node-wal` → `dotnet publish
  src/PgWorker.PgNode -c Release -r <rid> --self-contained -o
  artifacts/node-wal`; build-arg `WAL_PUBLISH_DIR` → `PGNODE_PUBLISH_DIR`
  (строки ~200–204); строку ~243 `NodeWalReceiverTests.cs` →
  `NodePgNodeTests.cs`.
- П1.19. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
  Release --nologo` — ожидаемый результат: 0 warnings, 0 errors. Если
  компилятор указывает на оставшиеся вхождения `PgWorker.WalReceiver` /
  `WalReceiverIdentity` / `WalReceiverAgentStatus` — заменить их по тем же
  правилам (namespace → `PgWorker.PgNode`, классы → `PgNodeIdentity` /
  `PgNodeAgentStatus`) в указанных компилятором файлах.

## Шаг 2. Клиентский серт воркера для mTLS-канала (RestPki + DI)

**Вход:** шаг 1 завершён, сборка зелёная.

**Действие (кодер):** метод `RestPki.IssueWorkerClientCertificate` (EKU
ClientAuth, CN по роли — литерал `pgworker`, прочтение spec §3.4 «CN по роли»:
роль-компонент инициатора канала) + DI-синглтон сертификата в
`src/PgWorker.App/Program.cs`. Выпуск при старте воркера из `RestTlsMaterial`
(пара `PGW_REST_TLS_{CA,CA_KEY}` уже обязательна валидацией — новых env-секретов
не появляется, spec §3.7.5). Юнит-тест: серт парсится, EKU содержит только
ClientAuth (1.3.6.1.5.5.7.3.2), издан тем же CA.

**Выход:** метод выпуска + зарегистрированный сертификат; тест зелёный.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
FullyQualifiedName~RestPki` → зелёные (старые + новый).

**Связь со spec:** §3.4 (клиентский серт воркера), §3.7.5, §8.6.

**Подпункты:**

- П2.1. В файле `src/PgWorker.Core/Templates/RestPki.cs` после метода
  `IssueNodeCertificate` добавить метод (полный код):
```csharp
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    /// <summary>Выпуск клиентского серта воркера для mTLS-канала доставки
    /// (arch/14 §4, arch/25 №23): CN по роли инициатора канала (pgworker),
    /// EKU — только ClientAuth; RSA-2048/SHA256, срок и границы — как у
    /// серверного серта ноды (зажим в NotAfter CA).</summary>
    public static (string CertPem, string KeyPem) IssueWorkerClientCertificate(
        string caCertPem, string caKeyPem, string commonName)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ClientAuthOid], critical: false));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        if (notBefore < caWithKey.NotBefore)
            notBefore = caWithKey.NotBefore;
        var notAfter = DateTimeOffset.UtcNow.AddYears(10);
        if (notAfter > caWithKey.NotAfter)
            notAfter = caWithKey.NotAfter;
        using var certificate = request.Create(
            caWithKey, notBefore, notAfter, RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }
```
- П2.2. Новый тест в `src/tests/PgWorker.UnitTests/Templates/RestPkiTests.cs`
  (файл существует — добавить метод в класс): имя
  `WorkerClientCertificate_ClientAuthEkuAndIssuer`; AAA-комментарии по канону;
  логика: взять тестовый CA (как в существующих тестах файла), вызвать
  `RestPki.IssueWorkerClientCertificate(caPem, caKeyPem, "pgworker")`;
  ассерты: `X509Certificate2.CreateFromPemFile(certPem, keyPem)` не бросает;
  EKU-расширение содержит ровно OID `1.3.6.1.5.5.7.3.2`; `cert.Issuer`
  совпадает с `ca.Subject`; SAN-расширения нет либо не содержит ServerAuth.
- П2.3. В `src/PgWorker.App/Program.cs` рядом с регистрацией
  `RestCertificateCache` (строки ~259–264) добавить DI-синглтон (полный код):
```csharp
// Клиентский серт mTLS-канала доставки (arch/14 §4): выпуск из той же пары
// PGW_REST_TLS_{CA,CA_KEY} при старте, кеш процесса; новых env-секретов нет.
builder.Services.AddSingleton(sp =>
{
    var material = sp.GetRequiredService<RestTlsMaterial>();
    var (certPem, keyPem) = RestPki.IssueWorkerClientCertificate(
        material.CaPem, material.CaKeyPem, "pgworker");
    return X509Certificate2.CreateFromPemFile(certPem, keyPem);
});
```

## Шаг 3. Core: env ноды без секретов + bootstrap-JSON + doorman-шаблон

**Вход:** шаг 2 завершён.

**Действие (кодер):**
1. `SpiloEnvBuilder.Build` (файл `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`)
   — новая сигнатура `Build(ShardTopology topology, EtcdEndpoints etcd, bool
   syncStrict, string nodeName, string caPem, PgTuneResult? tuning = null,
   IReadOnlySet<string>? excludeParams = null)`: из словаря УДАЛЯЮТСЯ ключи
   `SSL_RESTAPI_CERTIFICATE`, `SSL_RESTAPI_PRIVATE_KEY`,
   `PGW_REST_PASSWORD_HASH` (константа `RestRotation.EnvPasswordHash`),
   `PGUSER_SUPERUSER`, `PGPASSWORD_SUPERUSER`, `PGUSER_STANDBY`,
   `PGPASSWORD_STANDBY`, `PGW_BUCKET_ADMIN_PASSWORD`,
   `PGW_BUCKET_ADMIN_USER`, `PGW_BUCKET_MOVER_PASSWORD`,
   `SPILO_CONFIGURATION`; ОСТАЮТСЯ `SCOPE`, `ETCD3_HOSTS`, `SSL_RESTAPI_CA`
   (теперь значение — параметр `caPem`), `PGW_BACKUP_CLUSTER`,
   `PGW_BACKUP_SHARD`, `PGW_BACKUP_NODE`, `PGW_ETCD`, `PGW_MASTER_KEY`,
   `USE_DATA_DIR_FOR_WAL`. YAML `SPILO_CONFIGURATION` строится прежней логикой
   (вынести в приватный метод `SpiloConfiguration(...)` с прежними входами)
   — теперь его единственный потребитель bootstrap-билдер.
2. Новый `src/PgWorker.Core/Templates/NodeBootstrapBuilder.cs`: статический
   `string Build(ShardTopology topology, EtcdEndpoints etcd, bool syncStrict,
   string nodeName, string certPem, string keyPem, string restPassword,
   PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)`
   — детерминированный JSON `{"spilo_configuration":…,"cert_pem":…,
   "key_pem":…}` (System.Text.Json, без отступов; SuPassword/StandbyPassword
   в JSON НЕ входят — Д7). Сериализация симметрична: одинаковый вход —
   байт-в-байт одинаковый вывод (spec §3.7.2).
3. `DoormanConfigBuilder.Build(string dbname, int serverConnections)` —
   параметр `suPassword` убрать; в YAML `password: "{suPassword}"` → `password:
   "${PGW_SU_PASSWORD}"` (литеральный плейсхолдер). Комментарий класса
   обновить: конфиг — неконфиденциальный шаблон docker-env
   `DOORMAN_CONFIG_TEMPLATE`, пароль подставляет агент PgNode (arch/14 §4);
   `InstallSecretsGuard` остаётся гвардом алфавита для подстановки в
   YAML-строку.
4. Правка тестов `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`
   под новые контракты (детали — подпункты) + новые тесты bootstrap-JSON.

**Выход:** env-билдер без секретов; bootstrap-билдер; doorman-шаблон; тесты.

**Проверка:** сборка UnitTests-зависимостей; `DOTNET_CLI_UI_LANGUAGE=en
dotnet test src/tests/PgWorker.UnitTests -c Release --no-build --filter
FullyQualifiedName~NodeConfigBuilders` → зелёные (сборка всего решения на этом
шаге частично упадёт — драйвер согласуется в шаге 4; проверка только по фильтру).

**Связь со spec:** §3.1 (таблица env), §3.3 (bootstrap-ключ), §3.5 (таблица
файлов, `DOORMAN_CONFIG_TEMPLATE`), §3.7.1.

**Подпункты:**

- П3.1. В `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` заменить метод
  `SpiloEnvBuilder.Build` целиком на (полный код):
```csharp
    /// <summary>ENV контейнера ноды — ТОЛЬКО неконфиденциальное (t05,
    /// arch/14 §4): идентификация Patroni-DCS, lease-скрипт, идентичность
    /// приёмника, публичные CA. Секреты (SPILO_CONFIGURATION с rest_password,
    /// серт/ключ ноды, пароли PG) доставляет агент PgNode: bootstrap-ключ etcd
    /// (NodeBootstrapBuilder) + mTLS-push per-install паролей.</summary>
    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, bool syncStrict,
        string nodeName, string caPem,
        PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)
    {
        var etcdHosts = string.Join(",", etcd.Http.Select(StripScheme));
        var etcdUrls = string.Join(",", etcd.Http);
        var masterKey = $"/clusters/{topology.Cluster}/shards/{topology.Shard}/master";
        return new Dictionary<string, string>
        {
            ["SCOPE"] = topology.Scope,
            ["ETCD3_HOSTS"] = etcdHosts,
            // Публичный CA REST-слоя (не секретен: arch/13 §4).
            ["SSL_RESTAPI_CA"] = caPem,
            ["PGW_BACKUP_CLUSTER"] = topology.Cluster,
            ["PGW_BACKUP_SHARD"] = topology.Shard,
            ["PGW_BACKUP_NODE"] = nodeName,
            ["PGW_ETCD"] = etcdUrls,
            ["PGW_MASTER_KEY"] = masterKey,
            ["USE_DATA_DIR_FOR_WAL"] = "true",
        };
    }

    /// <summary>Полный YAML SPILO_CONFIGURATION (rest_password внутри) —
    /// содержимое bootstrap-ключа etcd (arch/14 §3.2/§4); в docker-env НЕ
    /// попадает. Тело — прежний YAML без изменений (байт-в-байт).</summary>
    internal static string SpiloConfiguration(
        ShardTopology topology, EtcdEndpoints etcd, bool syncStrict,
        string nodeName, string restPassword,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams)
    {
        // СЮДА: прежнее тело Build от строки `var etcdHosts = …` до
        // формирования словаря, БЕЗ строки ["SPILO_CONFIGURATION"] = …,
        // возвращающее только значение YAML (строка $$""" … """).
    }
```
  Перенос YAML: значение прежнего `["SPILO_CONFIGURATION"] = $$"""…"""` (со
  всеми комментариями и интерполяциями `{{nodeFullName}}`,
  `{{RestRotation.RestUsername}}`, `{{RestPasswordKey}}`,
  `{{restPassword}}`, `{{PatroniTimings.*}}`,
  `{{(syncStrict ? "true" : "false")}}`, `{{ParametersBlock(tuning,
  excludeParams)}}`) сделать возвращаемым значением `SpiloConfiguration`
  (интерполяция `rest.RestPassword` → `restPassword`). Вспомогательные методы
  `ParametersBlock`, `CanonicalParametersBlock`, `TunedParametersBlock`,
  `StripScheme` не менять.
- П3.2. Новый файл `src/PgWorker.Core/Templates/NodeBootstrapBuilder.cs`
  (полный код):
```csharp
using System.Text.Json;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;

namespace PgWorker.Core.Templates;

/// <summary>Bootstrap-ключ ноды (arch/14 §3.2/§4, t05): JSON
/// {spilo_configuration, cert_pem, key_pem} — полный SPILO_CONFIGURATION
/// (rest_password внутри) и серверный серт ноды (per-install REST-CA, SAN —
/// arch/14 §2.4 п.6; один серт на REST :8008 / doorman :6432 / mTLS-канал
/// :8081). Пишет ТОЛЬКО воркер; читает агент PgNode; per-install пароли PG
/// сюда НЕ входят (прямая mTLS-передача). Сериализация симметрична —
/// детерминированный вывод без дребезга mod_revision (spec §3.7.2).</summary>
public static class NodeBootstrapBuilder
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = false,
    };

    private sealed record BootstrapData(
        [property: JsonPropertyName("spilo_configuration")] string SpiloConfiguration,
        [property: JsonPropertyName("cert_pem")] string CertPem,
        [property: JsonPropertyName("key_pem")] string KeyPem);

    public static string Build(
        ShardTopology topology, EtcdEndpoints etcd,
        bool syncStrict, string nodeName, string certPem, string keyPem,
        string restPassword, PgTuneResult? tuning = null,
        IReadOnlySet<string>? excludeParams = null) =>
        JsonSerializer.Serialize(new BootstrapData(
            SpiloEnvBuilder.SpiloConfiguration(
                topology, etcd, syncStrict, nodeName, restPassword, tuning, excludeParams),
            certPem, keyPem), Options);
}
```
- П3.3. В `NodeConfigBuilders.cs` метод `DoormanConfigBuilder.Build` заменить
  на (полный код; отличия от прежнего YAML: строка password → плейсхолдер,
  пути TLS-файлов → tmpfs-пути рендера агента `/run/pgw/doorman/tls.*` —
  spec §3.5, файлы пишет BootstrapRenderer (П5.3), стартует пулер с ними
  П12.1; `/etc/pg_doorman/*` больше не существует — П12.2):
```csharp
    /// <summary>Шаблон конфига pg_doorman (env DOORMAN_CONFIG_TEMPLATE —
    /// неконфиденциальный): литеральный плейсхолдер ${PGW_SU_PASSWORD}
    /// подставляет агент PgNode при рендере в tmpfs (arch/14 §4); TLS-файлы —
    /// tmpfs-пути /run/pgw/doorman/tls.* (рендерит агент, spec §3.5). Конфиг
    /// детерминирован между reconcile'ами (пароля в нём нет — перезаписей
    /// контейнера по дрейфу не возникает); InstallSecretsGuard — гвард
    /// алфавита подстановки в YAML-строку.</summary>
    public static string Build(string dbname, int serverConnections) =>
        $"""
        # pg_doorman: single pool {dbname} per node (P13/P14/P15/P17).
        # Clients connect on :6432 with sslmode=require (P17); admission — auth_query.
        general:
          host: "0.0.0.0"
          port: 6432
          admin_username: "pgwd_admin"
          admin_password: "pgwd_admin_password"
          tls_mode: "require"
          tls_certificate: "/run/pgw/doorman/tls.crt"
          tls_private_key: "/run/pgw/doorman/tls.key"
        pools:
          "{dbname}":
            server_host: "127.0.0.1"
            server_port: 5432
            server_database: "{dbname}"
            pool_mode: "transaction"
            max_db_connections: {serverConnections}
            auth_query:
              query: "SELECT passwd FROM pg_shadow WHERE usename = $1"
              user: "postgres"
              password: "${{PGW_SU_PASSWORD}}"
        """;
```
  ВАЖНО: в raw-string `$"""` плейсхолдер записывается как
  `${{PGW_SU_PASSWORD}}` (экранирование фигурной скобки интерполяции) — в
  выводе получается буквальный `${PGW_SU_PASSWORD}`.
- П3.4. Правки теста `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`:
  а) все вызовы `SpiloEnvBuilder.Build(topology, etcd, secrets, syncStrict,
  nodeName, new NodeRestTls(...), tuning, exclude)` заменить на
  `SpiloEnvBuilder.Build(topology, etcd, syncStrict, nodeName, caPem,
  tuning, exclude)` (caPem — строка теста, как прежде передавалась в
  NodeRestTls); тесты `SpiloEnv_RestTlsPemCarriedByteInByte` и
  `SpiloEnv_RestPasswordHashIsSha256Hex` УДАЛИТЬ (env больше не несёт
  серт/хеш); б) тесты `SpiloEnv_ContainsScopeAndEtcdHosts`,
  `SpiloEnv_CarriesReceiverIdentityEnv`,
  `SpiloEnv_SpiloConfigurationContainsRestapiSection` (переносится на
  NodeBootstrapBuilder, см. П3.5), `SpiloEnv_NoAppPasswordLeak`,
  `SpiloEnv_Tuning_*`, `Build_SyncStrict_*` — обновить вызовы и утверждения
  под новый набор env-ключей; в) тест `Doorman_ConfigCarriesSuPassword_OtherSecretsNeverLeak`
  переименовать в `Doorman_TemplateCarriesPlaceholder_NoRealPassword`: ассерты
  `Contain("password: \"su\"")` → `Contain("password: \"${PGW_SU_PASSWORD}\"")`
  + `NotContain("\"su\"")`; если этот или соседние Doorman-тесты ассертят
  TLS-пути — заменить `/etc/pg_doorman/tls.crt|tls.key` на
  `/run/pgw/doorman/tls.crt|tls.key` (шаблон П3.3) и добавить ассерты
  `Contain("tls_certificate: \"/run/pgw/doorman/tls.crt\"")`,
  `Contain("tls_private_key: \"/run/pgw/doorman/tls.key\"")`; г) новый тест
  `SpiloEnv_SecretsNeverLeak` (AAA): собрать env через новый Build и
  ассертнуть `NotContainKey` для каждого из: `PGUSER_SUPERUSER`,
  `PGPASSWORD_SUPERUSER`, `PGUSER_STANDBY`, `PGPASSWORD_STANDBY`,
  `PGW_BUCKET_ADMIN_USER`, `PGW_BUCKET_ADMIN_PASSWORD`,
  `PGW_BUCKET_MOVER_PASSWORD`, `SSL_RESTAPI_CERTIFICATE`,
  `SSL_RESTAPI_PRIVATE_KEY`, `PGW_REST_PASSWORD_HASH`, `SPILO_CONFIGURATION`,
  `DOORMAN_CONFIG`; и `ContainKey("SSL_RESTAPI_CA")`.
- П3.5. Новый тест в `NodeConfigBuildersTests`: `Bootstrap_ContainsSpiloConfigurationCertAndKey`
  (AAA): вызвать `NodeBootstrapBuilder.Build(topology, etcd,
  syncStrict: false, "shard1a", "CERTPEM", "KEYPEM", "restpw", null, null)`;
  ассерты: результат парсится `JsonDocument.Parse`; корень содержит свойства
  `spilo_configuration` (строка, содержит `restapi:` и `password: "restpw"`),
  `cert_pem` == "CERTPEM", `key_pem` == "KEYPEM"; JSON не содержит значений
  тестовых SuPassword/StandbyPassword (подстрочный ассерт); повторный вызов с
  теми же входами даёт идентичную строку (симметрия).

## Шаг 4. Драйвер: BuildSpec без секретов, DOORMAN_CONFIG_TEMPLATE, tmpfs

**Вход:** шаг 3 завершён.

**Действие (кодер):** `PlainClusterDriver.BuildSpec` и сигнатура
`IClusterDriver.EnsureNodeAsync` (обе реализации): параметры `InstallSecrets
secrets` и `string restPassword` УДАЛЯЮТСЯ (секреты больше не проходят через
драйвер; их несёт NodeBootstrapDelivery шага 9). `BuildSpec`: env =
`SpiloEnvBuilder.Build(topology, etcd, syncStrict, nodeName,
restCertificates.CaPem, tuning, pgtuneExclude)` + прежние дополнения
(`PGW_NODE_HOST`, `PGW_NODE_NAME`, backup-env, `PGW_DOORMAN_PORT`) + новые:
`env["DOORMAN_CONFIG_TEMPLATE"] = DoormanConfigBuilder.Build(topology.Cluster,
serverConnections)` и `env["PGW_AGENT_CLIENT_CA"] = restCertificates.CaPem`;
`ContainerSpec` получает `Tmpfs: new Dictionary<string, string> {
["/run/pgw"] = "rw,noexec,nosuid,size=16m" }` (mount уже поддержан движком —
`Shared.Docker/Engine/DockerEngine.cs:971`). Record `NodeRestTls` удалить
(после правок потребителей нет — проверить grep). Вызовы `EnsureNodeAsync`
(ProvisioningProcess, AddShardProcess, AdoptionProcess, NodeSupervisor×3,
фейки/тесты) — убрать два аргумента (delivery добавит шаг 9). Swarm — то же
(внутренний plain BuildSpec общий).

**Выход:** контейнер ноды создаётся без секретов в env, с tmpfs `/run/pgw`,
шаблоном doorman и CA канала; сборка и юниты драйвера зелёные.

**Проверка:** сборка Release 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
FullyQualifiedName~ClusterDriver` → зелёные.

**Связь со spec:** §3.1, §3.2 (tmpfs), §3.5 (шаблон), §3.7.1, §3.7.2.

**Подпункты:**

- П4.1. В `src/PgWorker.Docker/Drivers/ClusterDriver.cs` сигнатура
  `Task<Result> EnsureNodeAsync(...)` интерфейса `IClusterDriver`: удалить
  параметры `InstallSecrets secrets,` и `string restPassword,` (итоговая
  сигнатура: `ShardTopology topology, string nodeName, NodeAddress addr,
  EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning, bool
  syncStrict, CancellationToken ct`). Те же параметры удалить в
  `PlainClusterDriver.EnsureNodeAsync` и `SwarmClusterDriver.EnsureNodeAsync`;
  XML-комментарии параметров secrets/restPassword удалить.
- П4.2. В `PlainClusterDriver.BuildSpec`: сигнатуру привести к
  `internal ContainerSpec BuildSpec(ShardTopology topology, string nodeName,
  NodeAddress addr, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult?
  tuning, bool syncStrict)`; блок сборки env заменить на:
```csharp
        var (certPem, keyPem) = restCertificates.GetOrCreate(topology.Cluster, topology.Shard, nodeName);
        var env = new Dictionary<string, string>(
            SpiloEnvBuilder.Build(topology, etcd, syncStrict, nodeName,
                restCertificates.CaPem, tuning, pgtuneExclude))
        {
            ["PGW_NODE_HOST"] = addr.Host,
            ["PGW_NODE_NAME"] = nodeName,
        };
```
  (блок `nodeBackupEnv` оставить без изменений); после блока `nodeBackupEnv`:
```csharp
        var serverConnections = tuning is null ? 55 : DoormanConfigBuilder.ServerConnections(tuning);
        env["DOORMAN_CONFIG_TEMPLATE"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections);
        env["PGW_DOORMAN_PORT"] = addr.Ports.Doorman.ToString();
        // Публичный CA верификации клиентских сертов воркера на mTLS-канале
        // агента :8081 (arch/13 §2, arch/25 №23) — тот же per-install REST-CA.
        env["PGW_AGENT_CLIENT_CA"] = restCertificates.CaPem;
```
  и в возвращаемый `new ContainerSpec(` добавить именованный аргумент
  `Tmpfs: new Dictionary<string, string> { ["/run/pgw"] =
  "rw,noexec,nosuid,size=16m" },` (перед `CpuCores:`).
- П4.3. Удалить record `NodeRestTls` из `NodeConfigBuilders.cs` (перед
  удалением grep `NodeRestTls` по `src/` — после правок потребителей нет;
  если тесты ещё создают — заменить вызовы согласно П3.4).
- П4.4. Обновить вызовы `EnsureNodeAsync` (удалить аргументы `clusterSecrets|secrets`
  и `restPassword|creds.RestPassword` — фактический идентификатор виден по
  месту): `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (метод
  EnsureNodesAsync), `AddShardProcess.cs` (EnsureNodesAsync),
  `AdoptionProcess.cs` (цикл ensured), `NodeSupervisor.cs` (EnsureDeclaredNodesAsync,
  RecreateMarkedNodesAsync, rebuild-ветка SuperviseShardAsync), фейки
  `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`,
  `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` и прямые
  вызовы в `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs`.
- П4.5. Правки `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`:
  а) все `driver.BuildSpec(Topology(Addr), "shard1a", Addr, Secrets, Etcd,
  null, null, false, RestTestPassword)` → `driver.BuildSpec(Topology(Addr),
  "shard1a", Addr, Etcd, null, null, false)`; б) ассерты `DOORMAN_CONFIG` (три
  блока, строки ~607–656) заменить: ключ `DOORMAN_CONFIG` →
  `DOORMAN_CONFIG_TEMPLATE`; `Contain("password: \"su\"")` →
  `Contain("password: \"${PGW_SU_PASSWORD}\"")` (строки admin_username/
  admin_password/auth_query/user/max_db_connections — без изменений);
  ДОБАВИТЬ ассерты TLS-путей tmpfs-рендера агента (П3.3):
  `Contain("tls_certificate: \"/run/pgw/doorman/tls.crt\"")` и
  `Contain("tls_private_key: \"/run/pgw/doorman/tls.key\"")`; встречающиеся
  ассерты `/etc/pg_doorman/tls.*` — заменить на эти же tmpfs-пути; в)
  добавить тест `BuildSpec_NoSecretsInEnvAndTmpfsMounted` (AAA): собрать
  spec, ассерты: `spec.Env.Should().NotContainKey(...)` для списка из
  `SpiloEnv_SecretsNeverLeak` (П3.4) + `DOORMAN_CONFIG`; `spec.Env["PGW_AGENT_CLIENT_CA"]`
  равен `CaPem` тестового кеша; `spec.Tmpfs!["/run/pgw"]` ==
  `"rw,noexec,nosuid,size=16m"`; г) тесты `BuildSpec_RestTlsCertificateIssuedFromCache`
  и `BuildSpec_SameNodeStableCertificate` — серт теперь не в env: заменить
  ассерты env на стабильность env между двумя вызовами (два BuildSpec —
  идентичные словари Env); выпуск серта покрыт `RestCertificateCacheTests`
  (не трогать).

## Шаг 5. Агент: базовые блоки bootstrap-модуля (данные, рендер, supervisorctl, lease-сессия, пробы)

**Вход:** шаги 1–4 завершены; проект `src/PgWorker.PgNode`.

**Действие (кодер):** новые файлы в `src/PgWorker.PgNode/`:
- `EnvironmentFile.cs` — `static string GenerateEnvironmentVariable(string
  key, string value)`: значение из алфавита `^[A-Za-z0-9._-]*$` → `KEY=value`;
  иначе — sh-одинарные кавычки с заменой `'` → `'\''` (per-install пароли
  гарантированно DSN-safe — `InstallSecretsGuard`; функция — защитный слой,
  spec §3.5).
- `NodeBootstrapData.cs` — record `(string SpiloConfiguration, string CertPem,
  string KeyPem)` + `static NodeBootstrapData? Parse(string json)` + метод
  `bool IsValidForStart()` (серт+ключ парсятся `X509Certificate2.CreateFromPemFile`,
  YAML непуст — проверки аргументов хендшейка, spec §3.4). Контракт дублирован
  между воркером (PgWorker.Core) и агентом — осознанно, прецедент
  `ApiContracts.cs` (запрет цикла ссылок).
- `BootstrapRenderer.cs` — статический рендер всех файлов tmpfs (таблица
  spec §3.5; doorman-конфиг = env `DOORMAN_CONFIG_TEMPLATE` с заменой
  литеральной подстроки `${PGW_SU_PASSWORD}` → SuPassword); права 0600
  (`File.SetUnixFileMode`), каталоги `Directory.CreateDirectory`.
- `SupervisorCommands.cs` — `ISupervisorCommands.StartAsync(program, ct)` /
  `RestartAsync(program, ct)` через `IProcessRunner` (`supervisorctl start|restart`);
  вывод содержит `already started` → успех; прочий ненулевой код — исключение.
  Семантика supervisord: `start` блокирует до `startsecs` и рапортует
  провал старта ненулевым кодом — попытка старта = один вызов `StartAsync`.
- `PgNodeStatus.cs` — агентский writer-контракт статусного ключа: record
  `(string State, long? AppliedRevision, IReadOnlyDictionary<string,string>
  Components, long UpdatedUnix, string? Error)`; состояния `not_initialized`/
  `initializing`/`ready`/`start_failed`; компоненты `pg`/`doorman` со
  значениями `starting|ready|failed|stopped` (+ `prepared` — tool-путь);
  `ToJson()` — сериализация контракта arch/14 §3.3.
- `NodeStatusSession.cs` — lease-сессия статусного ключа
  `/pgworker/nodes/<C>/<X>/<N>/status`: КОПИЯ-адаптация
  `WalReceiverStatusSession` (тот же файл проекта) с заменой типа значения на
  `PgNodeStatus` и JSON-сериализатора на `PgNodeStatus.ToJson()`; TTL 15 c /
  keepalive+put 5 c; плюс метод `Task<bool> TryTransitionAsync(string
  newValueJson, string expectedValueJson, CancellationToken ct)` — атомарный
  txn `[TxnCompare.ValueEqual(key, expectedValueJson)] [TxnOp.Put(key,
  newValueJson, lease сессии)]` (spec §3.6-2, §8.16, arch/14 §3.3: compare
  по VALUE «state==not_initialized» — устойчив к keepalive-put'ам сессии,
  которые не меняют значение, но меняют mod_revision; прецедент
  `TxnCompare.ValueEqual` — конкурентный flip routing arch/11 §5, паттерн
  `Shared.Etcd/Coordination/PortAllocLock.cs`).
- `ComponentProbes.cs` — пробы `SELECT 1` фазы (б) (spec §3.6-3б):
  `ProbePgAsync(string suPassword, CancellationToken)` — Npgsql
  `Host=127.0.0.1;Port=5432;Username=postgres;Password=<su>;Database=postgres;Timeout=5`;
  `ProbeDoormanAsync(string appUser, string appPassword, string dbName,
  CancellationToken)` — `Host=127.0.0.1;Port=6432;Username=<appUser>;Password=
  <appPassword>;Database=<dbName>;SslMode=Require;Timeout=5` (проба заодно
  валидирует цепочку auth_query). Обе — true/false, исключения глотаются.
Юнит-тесты: рендер, экранирование, JSON-контракт статуса, пробы (fake
Npgsql-подключение не вынести — тестировать строку подключения через
вынесенный фабричный метод/`NpgsqlDataSource`-seam по образцу
`IXLogReplicationSource`).

**Выход:** блоки агента с юнит-покрытием; машина/листенер — шаги 6–7.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
"FullyQualifiedName~PgNode"` → зелёные.

**Связь со spec:** §3.3 (статусный ключ), §3.5, §3.6-3 (пробы), §8.15–8.17.

**Подпункты:**

- П5.1. Новый файл `src/PgWorker.PgNode/EnvironmentFile.cs` (полный код):
```csharp
using System.Text.RegularExpressions;

namespace PgWorker.PgNode;

/// <summary>KEY=VALUE-строка для source-able env-файла tmpfs (spec §3.5):
/// значение алфавита [A-Za-z0-9._-] пишется как есть; прочее — sh-экранирование
/// одинарными кавычками (апостроф → '\''). Per-install пароли гарантированно
/// DSN-safe (InstallSecretsGuard воркера) — экранирование защитный слой.</summary>
public static class EnvironmentFile
{
    private static readonly Regex ShSafe = new("^[A-Za-z0-9._-]*$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static string GenerateEnvironmentVariable(string key, string value)
        => ShSafe.IsMatch(value)
            ? $"{key}={value}"
            : $"{key}='{value.Replace("'", "'\\''")}'";
}
```
- П5.2. Новый файл `src/PgWorker.PgNode/NodeBootstrapData.cs` (полный код):
```csharp
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.PgNode;

/// <summary>Значение bootstrap-ключа etcd /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/bootstrap
/// (arch/14 §3.2/§4): полный SPILO_CONFIGURATION (rest_password внутри) и
/// серверный серт ноды. Контракт — дубль PgWorker.Core NodeBootstrapBuilder
/// (запрет цикла ссылок, прецедент ApiContracts).</summary>
public sealed record NodeBootstrapData(
    string SpiloConfiguration,
    string CertPem,
    string KeyPem)
{
    /// <summary>null — JSON не парсится (чужой/битый ключ).</summary>
    public static NodeBootstrapData? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json);
            return dto is { SpiloConfiguration: { Length: > 0 },
                    CertPem: { Length: > 0 }, KeyPem: { Length: > 0 } }
                ? new NodeBootstrapData(dto.SpiloConfiguration, dto.CertPem, dto.KeyPem)
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>Валидность для старта (spec §3.4): сертификат+ключ парсятся в
    /// пару, YAML непуст. Значения в текст ошибок не включаются.</summary>
    public bool IsValidForStart()
    {
        try
        {
            using var _ = X509Certificate2.CreateFromPemFile(CertPem, KeyPem);
            return SpiloConfiguration.Trim().Length > 0;
        }
        catch (Exception e) when (
            e is ArgumentException or System.Security.Cryptography.CryptographicException)
        {
            return false;
        }
    }

    private sealed record Dto(
        [property: JsonPropertyName("spilo_configuration")] string? SpiloConfiguration,
        [property: JsonPropertyName("cert_pem")] string? CertPem,
        [property: JsonPropertyName("key_pem")] string? KeyPem);
}
```
- П5.3. Новый файл `src/PgWorker.PgNode/BootstrapRenderer.cs` (полный код):
```csharp
using System.IO;

namespace PgWorker.PgNode;

/// <summary>Рендер tmpfs /run/pgw (arch/14 §4, spec §3.5): все файлы 0600,
/// процесс агента работает под postgres (владелец верен по построению).
/// Кэша на диске нет — tmpfs память контейнера; повторный рендер перезаписывает
/// файлы идемпотентно.</summary>
public static class BootstrapRenderer
{
    public const string Root = "/run/pgw";
    public const string SpiloEnvPath = Root + "/spilo-env";
    public const string SpiloConfigurationPath = Root + "/spilo-configuration.yml";
    public const string RestCertPath = Root + "/rest-cert.pem";
    public const string RestKeyPath = Root + "/rest-key.pem";
    public const string DoormanConfigPath = Root + "/doorman/pg_doorman.yaml";
    public const string DoormanCertPath = Root + "/doorman/tls.crt";
    public const string DoormanKeyPath = Root + "/doorman/tls.key";

    // Плейсхолдер шаблона doorman (spec §3.5/§3.8-11).
    public const string SuPasswordPlaceholder = "${PGW_SU_PASSWORD}";

    public static void Render(
        NodeBootstrapData bootstrap, string suPassword, string standbyPassword,
        string doormanTemplate, string? rootDir = null)
    {
        var root = rootDir ?? Root;
        Directory.CreateDirectory(root + "/doorman");
        var mode = UnixFileMode.UserRead | UnixFileMode.UserWrite; // 0600
        WriteAll($"{root}/spilo-env", string.Join("\n",
            EnvironmentFile.GenerateEnvironmentVariable("PGUSER_SUPERUSER", "postgres"),
            EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_SUPERUSER", suPassword),
            EnvironmentFile.GenerateEnvironmentVariable("PGUSER_STANDBY", "standby"),
            EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_STANDBY", standbyPassword),
            string.Empty), mode);
        WriteAll($"{root}/spilo-configuration.yml", bootstrap.SpiloConfiguration, mode);
        WriteAll($"{root}/rest-cert.pem", bootstrap.CertPem, mode);
        WriteAll($"{root}/rest-key.pem", bootstrap.KeyPem, mode);
        WriteAll($"{root}/doorman/pg_doorman.yaml",
            doormanTemplate.Replace(SuPasswordPlaceholder, suPassword, StringComparison.Ordinal), mode);
        WriteAll($"{root}/doorman/tls.crt", bootstrap.CertPem, mode);
        WriteAll($"{root}/doorman/tls.key", bootstrap.KeyPem, mode);
    }

    private static void WriteAll(string path, string content, UnixFileMode mode)
    {
        File.WriteAllText(path, content);
        File.SetUnixFileMode(path, mode);
    }
}
```
- П5.4. Новый файл `src/PgWorker.PgNode/SupervisorCommands.cs` (полный код;
  контракт `IProcessRunner.RunAsync` —
  `Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(ProcessStartInfo
  psi, CancellationToken ct)`, паттерн вызова `RestoreOperation.StopPatroniAsync`):
```csharp
using System.Diagnostics;
using PgWorker.PgNode.Operations;

namespace PgWorker.PgNode;

/// <summary>Управление сервисами ноды через supervisorctl (arch/14 §2.1/§4;
/// прецедент RestoreOperation): start блокирует до startsecs и рапортует
/// провал старта ненулевым кодом — попытка старта = один вызов; «already
/// started» — успех (идемпотентность). restart безопасен на остановленной
/// программе.</summary>
public interface ISupervisorCommands
{
    Task StartAsync(string program, CancellationToken ct);
    Task RestartAsync(string program, CancellationToken ct);
}

public sealed class SupervisorCommands(IProcessRunner runner) : ISupervisorCommands
{
    public async Task StartAsync(string program, CancellationToken ct)
        => await RunAsync("start", program, ct);

    public async Task RestartAsync(string program, CancellationToken ct)
        => await RunAsync("restart", program, ct);

    private static async Task RunAsync(string verb, string program, CancellationToken ct)
    {
        var psi = new ProcessStartInfo { FileName = "supervisorctl" };
        psi.ArgumentList.Add(verb);
        psi.ArgumentList.Add(program);
        var (exit, output, err) = await runner.RunAsync(psi, ct);
        if (exit != 0
            && !(verb == "start"
                 && (output + err).Contains("already started", StringComparison.Ordinal)))
            throw new ApplicationException(
                $"supervisorctl {verb} {program}: exit {exit}: " +
                ((output.Length > 0 ? output : err).Split('\n')[0]));
    }
}
```
- П5.5. Новый файл `src/PgWorker.PgNode/PgNodeStatus.cs` (полный код):
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.PgNode;

/// <summary>Значение статусного ключа /pgworker/nodes/&lt;C&gt;/&lt;X&gt;/&lt;N&gt;/status
/// (arch/14 §3.3, spec §3.6): машина агента — not_initialized → initializing
/// → ready | start_failed; applied_revision — mod_revision применённого
/// bootstrap-ключа (null до применения); components pg/doorman:
/// starting|ready|failed|stopped (+prepared — tool-путь). Пишет ТОЛЬКО агент
/// (lease TTL 15 c); панелью не разбирается.</summary>
public sealed record PgNodeStatus(
    string State,
    long? AppliedRevision,
    IReadOnlyDictionary<string, string> Components,
    long UpdatedUnix,
    string? Error = null)
{
    public const string NotInitialized = "not_initialized";
    public const string Initializing = "initializing";
    public const string Ready = "ready";
    public const string StartFailed = "start_failed";

    public static readonly IReadOnlyList<string> ComponentStates =
        ["starting", "ready", "failed", "stopped", "prepared"];

    public string ToJson() => JsonSerializer.Serialize(new Dto(
        State, AppliedRevision, Components, UpdatedUnix, Error), Options);

    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record Dto(
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision,
        [property: JsonPropertyName("components")] IReadOnlyDictionary<string, string> Components,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("error")] string? Error);
}
```
- П5.6. Новый файл `src/PgWorker.PgNode/NodeStatusSession.cs`: скопировать
  файл `src/PgWorker.PgNode/WalReceiverStatusSession.cs` целиком и в копии
  заменить: имя класса `WalReceiverStatusSession` → `NodeStatusSession`;
  тип поля `_current` и параметра `StartAsync`/`Update` —
  `WalReceiverAgentStatus` → `PgNodeStatus`; вызовы
  `WalReceiverAgentStatusJson.ToJson(…)` → `value.ToJson()` (передавать
  значение как есть, без выделения суффикса ключа: node в статусном ключе не
  пишется — контракт §3.3). Семантика lease/keepalive/revoke/_gate/_ops — без
  изменений (TTL 15 c / период 5 c — те же константы). Добавить метод:
```csharp
    /// <summary>Атомарный переход машины txn'ом (spec §3.6-2, §8.16,
    /// arch/14 §3.3): compare по VALUE — прочитанное значение статуса (где
    /// state была not_initialized) обязано быть неизменным; put нового
    /// значения на lease сессии. Compare по значению, а не по mod_revision —
    /// keepalive-put'ы сессии ТОГО ЖЕ значения (каждые 5 c, value не меняется,
    /// mod_revision — да) переход не срывают. false — compare проигран
    /// (параллельный писатель ИЗМЕНИЛ значение) или etcd недоступен.</summary>
    public async Task<bool> TryTransitionAsync(string newValueJson, string expectedValueJson, CancellationToken ct)
    {
        await _ops.WaitAsync(ct);
        try
        {
            var leaseSnapshot = await ReadLeaseAsync(ct);
            if (leaseSnapshot is not { } lease)
                return false;
            var txn = TxnRequest.Of(
                [TxnCompare.ValueEqual(_key, expectedValueJson)],
                [new TxnOp.Put(_key, newValueJson, lease)]);
            // Txn возвращает Result<TxnResult>: не-generic FailoverAsync
            // копируемой сессии (Func<string, Task<Result>>) сюда не годится —
            // прямой generic-вызов (перегрузка EtcdEndpointFailover.CallAsync<T>;
            // прецеденты KafkaWorker NodeSupervisor.cs:428-429,
            // PortAllocHealer.cs:361-362).
            var result = await EtcdEndpointFailover.CallAsync(
                _endpoints, (endpoint, token) => _etcd.TxnAsync(endpoint, txn, token), ct);
            return result.IsSuccess && result.Value.Succeeded;
        }
        finally
        {
            _ops.Release();
        }
    }
```
  (`ReadLeaseAsync` — приватный снапшот `_lease` под `_gate`, по образцу
  `PutCurrentCoreAsync`; сигнатуры сверены:
  `TxnCompare.ValueEqual(string key, string expected)` и
  `TxnOp.Put(string, string, long?)` — `src/Shared.Etcd/Client/IEtcdGateway.cs`;
  generic-перегрузка `EtcdEndpointFailover.CallAsync<T>(IReadOnlyList<string>,
  Func<string, CancellationToken, Task<Result<T>>>, CancellationToken)` —
  `src/Shared.Etcd/Client/EtcdEndpointFailover.cs` — вместо не-generic
  `FailoverAsync` сессии, который принимает только не-generic `Result`.)
- П5.7. Новый файл `src/PgWorker.PgNode/ComponentProbes.cs` (полный код):
```csharp
using Npgsql;

namespace PgWorker.PgNode;

/// <summary>Пробы готовности фазы (б) initializing (spec §3.6-3б): SELECT 1
/// со стороны ноды. PG — 127.0.0.1:5432 под postgres/SuPassword (доставлен);
/// doorman — клиентский вход :6432 (sslmode=require) под app-ролью с
/// app_password per-cluster etcd (проба заодно валидирует цепочку auth_query).
/// Бюджет — Connection Timeout=5 (кратный интервалу опроса, spec §8.17);
/// ожидание готовности — БЕЗ бюджета (живой процесс может инициализироваться
/// долго).</summary>
public static class ComponentProbes
{
    public const int TimeoutSec = 5;

    public static string PgConnectionString(string suPassword)
        => $"Host=127.0.0.1;Port=5432;Username=postgres;Password={suPassword};" +
           $"Database=postgres;Timeout={TimeoutSec}";

    public static string DoormanConnectionString(string appUser, string appPassword, string dbName)
        => $"Host=127.0.0.1;Port=6432;Username={appUser};Password={appPassword};" +
           $"Database={dbName};SslMode=Require;Timeout={TimeoutSec}";

    public static async Task<bool> ProbeAsync(string connectionString, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand("SELECT 1", connection);
            var result = await command.ExecuteScalarAsync(ct);
            return result is 1;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return false; // ещё инициализируется/недоступен — следующая проба
        }
    }
}
```
- П5.8. Юнит-тесты (новые файлы `src/tests/PgWorker.UnitTests/PgNode/`):
  `EnvironmentFileTests` (AAA: safe-значение без кавычек; значение с `;`/`=`/
  пробелом — в одинарных кавычках; апостроф экранирован `'\''`),
  `BootstrapRendererTests` (AAA: во временном каталоге через `rootDir` — все
  7 файлов; spilo-env содержит ровно 4 строки KEY=VALUE с пуш-паролями;
  pg_doorman.yaml содержит пароль вместо `${PGW_SU_PASSWORD}` и не содержит
  плейсхолдер; pg_doorman.yaml ссылается на TLS-файлы tmpfs-рендера —
  `tls_certificate: "/run/pgw/doorman/tls.crt"` и
  `tls_private_key: "/run/pgw/doorman/tls.key"` (шаблон П3.3, spec §3.5);
  режим файлов UserRead|UserWrite без групп/остальных),
  `NodeBootstrapDataTests` (Parse валидного JSON; битый JSON → null; пустые
  поля → null; IsValidForStart — валидная пара PEM true, мусорный серт false),
  `PgNodeStatusTests` (AAA: ToJson содержит state/applied_revision/components/
  updated_unix; null-поля (error, applied_revision) не сериализуются;
  компонентный словарь сериализуется как {"pg":…,"doorman":…}),
  `ComponentProbesTests` (AAA: строки подключения содержат Timeout=5;
  doorman-строка — SslMode=Require и порт 6432; pg-строка — порт 5432 и
  переданный пароль).

## Шаг 6. Агент: статусная машина PgNodeMachine (хендшейк, фазы, применение)

**Вход:** шаг 5 завершён.

**Действие (кодер):** новый `src/PgWorker.PgNode/PgNodeMachine.cs` — машина
агента поверх блоков шага 5; чтение bootstrap-ключа (ретрай-цикл) живёт в
самой машине:
- Зависимости: `PgNodeIdentity`, `IEtcdGateway`, `string[] endpoints`,
  `ISupervisorCommands`, `string doormanTemplate` (env
  `DOORMAN_CONFIG_TEMPLATE`), `TimeProvider`; ключи:
  bootstrapKey=`/clusters/<C>/shards/<X>/nodes/<n>/bootstrap`,
  statusKey=`/pgworker/nodes/<C>/<X>/<N>/status` (N — `identity.Node`).
- `Task RunAsync(CancellationToken ct)` — фоновый цикл процесса: читать
  bootstrap-ключ (EtcdEndpointFailover; нет/битый → пауза 5 с, повтор —
  «внутренний ретрай-цикл чтения», spec §3.4); по первому успеху: валидировать
  (`IsValidForStart`), закешировать данные+mod_revision, построить
  `ServerCertificate` (X509Certificate2.CreateFromPemFile, кеш по
  mod_revision — потребляет листенер шага 7), открыть `NodeStatusSession` и
  опубликовать `not_initialized` (put со своим lease затирает протухший
  статус прошлого процесса, spec §3.6-5); далее — держать сессию (keepalive
  внутри сессии). Машина НЕ продолжает фазы сама: `initializing` запускается
  только командой хендшейка.
- `Task<HandshakeResult> HandshakeAsync(string suPassword, string
  standbyPassword, bool isToolStart, CancellationToken ct)` — команда
  доставки (spec §3.4/§3.6-2): 1) валидация аргументов: оба пароля непусты +
  закешированный bootstrap-ключ валиден → иначе `Invalid` (эндпоинт → 400,
  статус не меняется); 2) прочитать текущий статус-ключ (нет сессии/ключа —
  etcd-сбой → `EtcdError` → 503); значение не `not_initialized` → `Nop`
  (200 ok «пинок состоялся»); 3) `NodeStatusSession.TryTransitionAsync(
  initializingJson, kv.Value)` — атомарный compare по ЗНАЧЕНИЮ
  («state==not_initialized», §8.16): true → сохранить пароли в памяти,
  запустить фазы фоном (`_ = RunInitializingAsync(...)`), вернуть `Started`;
  false → `Nop` (значение изменил параллельный писатель; keepalive-put ТОГО
  ЖЕ значения сессии переход не срывает — compare по value, не по
  mod_revision).
- `RunInitializingAsync` — фазы (spec §3.6-3), фон, под фабрикой токенов
  хоста:
  - tool-путь (`isToolStart=true`): рендер tmpfs (ошибка → `start_failed`
    +error; компоненты не запускаются) → публикация `ready` c
    `components={pg:prepared, doorman:prepared}` и
    `applied_revision=mod_revision`; фаза (б) НЕ выполняется (spec §8.22).
  - штатный путь: рендер (ошибка → `start_failed` +error) → публикация
    components={pg:starting, doorman:starting} → старт компонентов
    ПАРАЛЛЕЛЬНО, надзор старта ≤3 попытки/компонент (`SupervisorCommands.
    StartAsync`; исключение/провал — попытка №+1; исчерпаны → компонент
    `failed`) — сбой одного НЕ блокирует другой; первый `failed` → публикация
    `start_failed` (+error, компоненты с фактом), отмена второго ожидания не
    требуется (машина терминальна; spec §8.18) → фаза (б): цикл каждые 5 с:
    `ComponentProbes.ProbeAsync(PgConnectionString(su))` и
    `ProbeAsync(DoormanConnectionString(appUser, appPassword, dbName))`;
    app-креды/dbname — ленивое чтение per-cluster ключей etcd
    `/clusters/<C>/app_user`, `/clusters/<C>/app_password`, `/clusters/<C>/config`
    (JSON-поле `dbname`; чтение — пассивное, канон приёмника); нет ключей →
    doorman-проба не выполняется (компонент остаётся `starting`), повтор в
    следующем цикле. Готовы оба (`SELECT 1` ok) → публикация `ready` с
    `components={pg:ready, doorman:ready}`, `applied_revision=mod_revision`.
    Ожидание — БЕЗ бюджета.
- `Task<ApplyResult> ApplyAsync(CancellationToken ct)` — команда применения
  (spec §3.4/§3.6-4): только из `ready` (иначе `NotReady` → эндпоинт 409);
  перечитать bootstrap-ключ (ошибка etcd → `EtcdError` → 503); ревизия равна
  применённой → `Noop` (200, applied_revision прежний); изменилась → рендер +
  точечное применение: spilo-материал (`SpiloConfiguration` или `CertPem`/
  `KeyPem`) изменился → `restart spilo`; doorman-материал (отрендеренный
  конфиг или серт) изменился → `restart pg_doorman`; оба → сначала
  `restart pg_doorman`, затем `restart spilo`; машина ОСТАЁТСЯ `ready`,
  публикация с новым `applied_revision=mod_revision` (спека §3.6-4; смена
  серта триггерит оба рестарта — серт входит в оба материала, §6.4).
- `X509Certificate2? ServerCertificate` — volatile-публикация для листенера.
- Пароли/применённый материал — только в памяти; в тексты ошибок не входят.
Юнит-тесты (фейки ISupervisorCommands/IEtcdGateway): полный хендшейк →
ready; ok-пинок при ready; 400 при пустом пароле/битом ключе; ≤3 попытки →
start_failed; один компонент failed при живом втором; tool-путь → prepared;
apply: no-op/spilo-restart/doorman-restart/оба; apply не из ready → NotReady;
рестарт контейнера (потеря памяти) — модельруется новым экземпляром машины.

**Выход:** машина агента с юнит-покрытием всех переходов.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
FullyQualifiedName~PgNodeMachine` → зелёные.

**Связь со spec:** §3.6 (вся статусная машина и хендшейк), §3.4, §8.14–8.22.

**Подпункты:**

- П6.1. Новый файл `src/PgWorker.PgNode/PgNodeMachine.cs`. Публичный контракт
  (реализация по логике шага):
```csharp
/// <summary>Результат команды доставки (эндпоинт маппит: Started/Nop → 200,
/// Invalid → 400, EtcdError → 503).</summary>
public enum HandshakeResult { Started, Nop, Invalid, EtcdError }

/// <summary>Результат команды применения (200 c applied_revision / NotReady →
/// 409 / EtcdError → 503).</summary>
public sealed record ApplyResult(bool Ok, long? AppliedRevision, bool NotReady, bool EtcdError);

/// <summary>Статусная машина агента PgNode (arch/14 §3.3/§4, spec §3.6):
/// публикация not_initialized после чтения bootstrap-ключа; хендшейк —
/// валидация аргументов + атомарный txn-переход в initializing; фазы — старт
/// компонентов ≤3 попытки (параллельно) и ожидание SELECT 1 без бюджета
/// (is_tool_start — только рендер, components=prepared); применение на ready —
/// перерендер + точечные рестарты. Секреты — только в памяти процесса.</summary>
public sealed class PgNodeMachine : IDisposable
{
    public PgNodeMachine(PgNodeIdentity identity, IEtcdGateway etcd, string[] endpoints,
        ISupervisorCommands supervisor, string doormanTemplate, TimeProvider? clock = null);

    public X509Certificate2? ServerCertificate { get; }
    public long? AppliedRevision { get; }

    /// <summary>Фоновый цикл: чтение bootstrap-ключа (ретрай 5 c) → валидация →
    /// серт листенера → публикация not_initialized (lease-сессия живёт до конца
    /// процесса). Повторные чтения — освежение серта/данных для применения.</summary>
    Task RunAsync(CancellationToken ct);

    /// <summary>Команда доставки (POST /api/bootstrap): валидация аргументов
    /// (400) → статус not_initialized? → атомарный txn в initializing; не
    /// not_initialized — Nop («пинок состоялся», 200 без действий).</summary>
    Task<HandshakeResult> HandshakeAsync(
        string suPassword, string standbyPassword, bool isToolStart, CancellationToken ct);

    /// <summary>Команда применения (POST /api/bootstrap/apply): только ready;
    /// перечитать ключ, ревизия изменилась — перерендер + точечные рестарты,
    /// applied_revision обновляется, машина остаётся ready.</summary>
    Task<ApplyResult> ApplyAsync(CancellationToken ct);
}
```
  Реализационные инварианты: `_status` (NodeStatusSession) создаётся при
  первом успешном чтении ключа; `_appliedSpiloMaterial`/`_appliedDoormanConfig`
  — строки для diff (п. Действие); `HandshakeAsync` и `ApplyAsync` — под
  общим `SemaphoreSlim(1,1)` с `WaitAsync(0)`: занят → Handshake возвращает
  `Nop` (повторная команда — ок-пинок), Apply возвращает `ApplyResult(Ok:
  false, NotReady: true, …)` (эндпоинт ответит 409); чтение per-cluster
  ключей app_user/app_password/config — лениво в цикле фазы (б), кеш на
  процесс.
- П6.2. Юнит-тест `src/tests/PgWorker.UnitTests/PgNode/PgNodeMachineTests.cs`
  (AAA; фейки: ISupervisorCommands с журналом вызовов и настраиваемым
  провалом start по компоненту; IEtcdGateway-фейк со словарём ключей и
  счётчиками put/txn — по образцу существующих фейков тестов; статусный ключ
  эмулируется словарём; сессия lease не гоняется — тестировать машину через
  введённый internal-конструктор с фейком сессии либо публиковать статусные
  переходы через internal-seam `internal Task RunOnceAsync(...)` для
  детерминизма; выбор за исполнителем — контракт переходов выше обязателен).
  Обязательные кейсы: (1) хендшейк при not_initialized → переход
  initializing, рендер, start pg_doorman + start spilo (параллельно — оба в
  журнале), пробы (фейк ComponentProbes через internal-seam или
  делегат-фабрику строк → succеss) → ready с applied_revision==mod_revision;
  (2) повторный хендшейк при ready → Nop, никаких новых start; (2а) гонка
  keepalive: между чтением статуса и txn фейк-сессия делает put ТОГО ЖЕ
  значения not_initialized (мод-ревизия выросла) → txn по ValueEqual
  успешен, переход НЕ ломается (ложных Nop нет); (2б) значение изменил
  параллельный писатель (уже initializing) → txn проигран → Nop; (3) пустой
  su_password → Invalid, статус не меняется; (4) битый bootstrap-ключ →
  Invalid; (5) start pg_doorman падает 3 раза → start_failed c error,
  components.doorman=failed; components.pg при этом достигает ready (второй
  компонент не блокируется); (6) is_tool_start=true → рендер без start,
  ready, components=prepared, select-пробы не вызывались; (7) ошибка рендера
  (недоступный rootDir рендера через internal-seam) → start_failed; (8) apply
  при готовой ready-ноде с тем же ключом → no-op (рестартов нет); (9) apply с
  изменённой spilo_configuration → restart spilo; (10) apply с изменённым
  только SuPassword → только restart pg_doorman; (11) apply с изменённым
  только cert_pem/key_pem → restart pg_doorman и restart spilo; (12) apply не
  из ready → NotReady; (13) apply при etcd-фейле чтения ключа → EtcdError.

## Шаг 7. Агент: mTLS-листенер :8081, команды канала, интеграция Program

**Вход:** шаг 6 завершён.

**Действие (кодер):** в `src/PgWorker.PgNode/Program.cs`:
1. После парсинга identity создать `PgNodeMachine` и запустить `RunAsync`
   фоном (`_ = machine.RunAsync(app.Lifetime.ApplicationStopping)` после
   `builder.Build()`); стрим/ops/metrics — без изменений (spec §3.6-6).
2. `builder.WebHost.UseUrls("http://0.0.0.0:8080")` заменить на
   `ConfigureKestrel`: `ListenAnyIP(8080)` + `ListenAnyIP(8081, lo =>
   lo.UseHttps(...))` — `ClientCertificateMode.RequireCertificate`,
   `ServerCertificateSelector = (_, _) => machine.ServerCertificate` (null до
   чтения ключа — handshake отклоняется: канала нет, эквивалент «листенер
   поднимется по появлении ключа», spec §3.4), клиент-валидатор: цепочка к CA
   из env `PGW_AGENT_CLIENT_CA` (`Shared.Tls.TlsChain.ValidateChain`; добавить
   ProjectReference `Shared.Tls` в csproj агента).
3. Маршруты (после `MapNodeRoutes`): `POST /api/bootstrap` тело
   `{"su_password","standby_password","is_tool_start"?}` (is_tool_start
   опционален, умолчание false) → `HandshakeAsync`: Started/Nop → 200
   `{"ok":true}`; Invalid → 400 `{"error":"..."}`; EtcdError → 503.
   `POST /api/bootstrap/apply` (тело пустое) → `ApplyAsync`: Ok → 200
   `{"applied_revision":N}`; NotReady → 409 `{"error":"node is not ready"}`;
   EtcdError → 503. GET-эндпоинта канала НЕТ (статус — etcd, spec §3.4).
   Секретные значения в тексты ошибок не включаются.
Проверить, что существующие API-тесты Program
(`PGW_WALRECEIVER_NO_STREAM=1`, WebApplicationFactory) живы — под TestServer
листенеры не bind'ятся, фоновый etcd-цикл не блокирует (Review Focus 6).

**Выход:** канал `:8081` с командами доставки/применения; API-тесты живы.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.IntegrationTests -c Release --no-build --filter
"FullyQualifiedName~BackupApiOperation|FullyQualifiedName~NodePgNodeTests"`
→ зелёные (существующие Program-API-тесты не сломаны).

**Связь со spec:** §3.4 (листенер/верификация/эндпоинты), §3.6-1, §5.

**Подпункты:**

- П7.1. В `src/PgWorker.PgNode/PgWorker.PgNode.csproj` добавить
```xml
        <!-- Валидация цепочки клиентских сертов mTLS-канала :8081 (arch/13 §2). -->
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
```
- П7.2. В `src/PgWorker.PgNode/Program.cs` в `Main` после блока валидации
  identity (после `var endpoints = identity.EtcdEndpoints.ToArray();`)
  добавить:
```csharp
        // Статусная машина агента (arch/14 §3.3/§4, spec §3.6): mTLS-канал
        // :8081 и хендшейк живут этим же процессом; стрим/операции бэкапов от
        // машины не зависят (spec §3.6-6).
        var machine = new PgNodeMachine(
            identity, etcd, endpoints,
            new SupervisorCommands(new ProcessRunner()),
            Environment.GetEnvironmentVariable("DOORMAN_CONFIG_TEMPLATE") ?? "");
```
  и заменить `builder.WebHost.UseUrls("http://0.0.0.0:8080");` на:
```csharp
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.ListenAnyIP(8080); // metrics + HTTP API операций (arch/25 №22)
            o.ListenAnyIP(8081, lo => lo.UseHttps(https =>
            {
                // Канал доставки секретов (arch/13 §2, arch/25 №23): серверный
                // серт — из bootstrap-ключа (null до чтения → handshake
                // отклоняется: канала нет, воркер повторит тиком); клиент
                // обязан предъявить валидный серт воркера (публичный CA из env).
                https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                https.ServerCertificateSelector = (_, _) => machine.ServerCertificate;
                https.OnHandshakeServerOptions = options =>
                    options.ClientCertificateValidationCallback = (certificate, _, _) =>
                        certificate is { } client
                        && NodeClientCa() is { } ca
                        && Shared.Tls.TlsChain.ValidateChain(client, ca);
            });
        });
```
  где `NodeClientCa()` — приватный static-хелпер Program: загрузка
  `X509Certificate2` из env `PGW_AGENT_CLIENT_CA` (через temp-файл по образцу
  `PatroniRoleProbe.GetCaAsync`, кеш по содержимому PEM). Сверить точный API
  `OnHandshakeServerOptions`/`ClientCertificateValidationCallback` по
  используемой версии TargetFramework — при отличии имени использовать
  фактический колбэк-хук `SslServerAuthenticationOptions`.
- П7.3. В `src/PgWorker.PgNode/Program.cs` после `var app = builder.Build();`
  добавить:
```csharp
        _ = machine.RunAsync(app.Lifetime.ApplicationStopping);
```
  и метод (рядом с MapNodeRoutes; контракты — вложенные private records с
  JsonPropertyName):
```csharp
    // Команды канала доставки (spec §3.4): статус воркер читает из etcd —
    // здесь только команды. POST /api/bootstrap — хендшейк старта (валидация
    // аргументов 400 / атомарный txn-переход / ок-пинок 200); POST
    // /api/bootstrap/apply — применение на ready-ноде (ротации/замены).
    // Секреты в логи/ошибки не попадают.
    private static void MapBootstrapRoutes(WebApplication app, PgNodeMachine machine)
    {
        app.MapPost("/api/bootstrap", async (WorkerPushRequest body, CancellationToken ct) =>
        {
            var result = await machine.HandshakeAsync(
                body.SuPassword ?? "", body.StandbyPassword ?? "", body.IsToolStart ?? false, ct);
            return result switch
            {
                HandshakeResult.Invalid => Results.Json(
                    new ErrorResponse("invalid arguments or bootstrap key"), JsonOpts, statusCode: 400),
                HandshakeResult.EtcdError => Results.Json(
                    new ErrorResponse("etcd is unavailable"), JsonOpts, statusCode: 503),
                _ => Results.Json(new OkResponse(), JsonOpts), // Started | Nop («пинок состоялся»)
            };
        });

        app.MapPost("/api/bootstrap/apply", async (CancellationToken ct) =>
        {
            var result = await machine.ApplyAsync(ct);
            if (result.EtcdError)
                return Results.Json(new ErrorResponse("etcd is unavailable"), JsonOpts, statusCode: 503);
            if (!result.Ok)
                return Results.Json(new ErrorResponse("node is not ready"), JsonOpts, statusCode: 409);
            return Results.Json(new AppliedResponse(result.AppliedRevision), JsonOpts);
        });
    }

    private sealed record OkResponse([property: JsonPropertyName("ok")] bool Ok = true);
    private sealed record AppliedResponse([property: JsonPropertyName("applied_revision")] long? AppliedRevision);
    private sealed record WorkerPushRequest(
        [property: JsonPropertyName("su_password")] string? SuPassword,
        [property: JsonPropertyName("standby_password")] string? StandbyPassword,
        [property: JsonPropertyName("is_tool_start")] bool? IsToolStart);
```
  Вызов `MapBootstrapRoutes(app, machine);` добавить после `MapNodeRoutes(app);`.
- П7.4. Новый тест-файл `src/tests/PgWorker.IntegrationTests/PgNode/BootstrapApiTests.cs`
  (WebApplicationFactory по образцу существующих Program-API-тестов
  `BackupApiOperationTests`, env `PGW_WALRECEIVER_NO_STREAM=1` + валидный
  identity-env теста): тесты (AAA): `Bootstrap_Post_EmptyPassword_Returns400`
  — POST без паролей → 400; `Bootstrap_Post_NoEtcd_Returns503OrInvalid` —
  POST с паролями (etcd фикстуры/ключа нет) → 400 либо 503 (по факту
  контура: ключа нет → Invalid); `Apply_Post_Returns409Or503` — POST
  `/api/bootstrap/apply` без ready-машины → 409 либо 503; GET `/api/bootstrap`
  → 405 (маршрута нет — статус только в etcd). Валидный identity-env —
  заимствовать из существующих Program-API-тестов.

## Шаг 8. Воркер: читатель статусов + клиент канала NodeChannelClient (mTLS)

**Вход:** шаги 2, 6–7 завершены.

**Действие (кодер):**
1. `src/PgWorker.Core/Templates/PgNodeStatusReader.cs` — воркерская копия
   контракта статусного ключа (дублирование осознанное, прецедент): record
   `PgNodeStatusFact(string State, long? AppliedRevision,
   IReadOnlyDictionary<string,string>? Components, long UpdatedUnix, string?
   Error)` + `static PgNodeStatusFact? Parse(string json)`; статический
   `string StatusKey(string cluster, string shard, string node)` →
   `/pgworker/nodes/<C>/<X>/<N>/status`.
2. Новый `src/PgWorker.Provisioning/Probes/NodeChannelClient.cs`:
   - `NodeChannelAddress.ForNode(cluster, shard, node)` →
     `https://pgw-<C>-<X>-<n>:8081` (полное docker-имя — alias в сети
     кластера; SAN полного имени; прецедент `WalReceiverAddress.ForNode`).
   - `INodeChannelClient`/`NodeChannelClient(HttpClient)`:
     `Task<ChannelReply> HandshakeAsync(string baseUrl, string suPassword,
     string standbyPassword, bool isToolStart, CancellationToken ct)` — POST
     `/api/bootstrap` с телом `{"su_password","standby_password",
     "is_tool_start"}`; `Task<ChannelReply> ApplyAsync(string baseUrl,
     CancellationToken ct)` — POST `/api/bootstrap/apply` без тела.
     `ChannelReply(int StatusCode, bool Ok, long? AppliedRevision, string?
     Error)`: `IsSuccess` = 2xx; transport/400/409/5xx → `IsTransient`
     (воркер ретраит тиком, spec §3.4). Пароли в URL/лог не попадают.
3. DI в `src/PgWorker.App/Program.cs`: `AddHttpClient("pgnode-channel", c =>
   c.Timeout = TimeSpan.FromSeconds(10))` с `SocketsHttpHandler`: сертификат
   из шага 2 в `SslOptions.ClientCertificates`; `RemoteCertificateValidationCallback`
   — цепочка к per-install CA + SAN-пиннинг полного имени ноды (через
   `SslStream.TargetHostName` sender'а; DER-разбор SAN по образцу
   `PatroniRoleProbe.SanContainsLoopback`, тег `0x82` dNSName).
4. Юнит-тесты контрактов клиента (fake HttpMessageHandler): тела/ответы
   команд; transient-коды.

**Выход:** читатель статусов и клиент канала с DI; тесты зелёные.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
"FullyQualifiedName~NodeChannelClient|FullyQualifiedName~PgNodeStatusReader"`
→ зелёные.

**Связь со spec:** §3.3 (чтение статуса воркером), §3.4 (клиент, верификация
CA+SAN, команды), §2.7 (таймауты), §8.20.

**Подпункты:**

- П8.1. Новый файл `src/PgWorker.Core/Templates/PgNodeStatusReader.cs`
  (полный код):
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Core.Templates;

/// <summary>Читатель статусного ключа агента PgNode (arch/14 §3.3; дубль
/// контракта агента — запрет цикла ссылок, прецедент ApiContracts):
/// воркер мониторит poll-тиком, панелью не разбирается.</summary>
public sealed record PgNodeStatusFact(
    string State,
    long? AppliedRevision,
    IReadOnlyDictionary<string, string>? Components,
    long UpdatedUnix,
    string? Error = null)
{
    public static string StatusKey(string cluster, string shard, string node)
        => $"/pgworker/nodes/{cluster}/{shard}/{node}/status";

    /// <summary>null — значение не парсится (чужой/битый ключ = статуса нет).</summary>
    public static PgNodeStatusFact? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json);
            return dto is { State: { Length: > 0 } }
                ? new PgNodeStatusFact(dto.State, dto.AppliedRevision,
                    dto.Components, dto.UpdatedUnix ?? 0, dto.Error)
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private sealed record Dto(
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision,
        [property: JsonPropertyName("components")] IReadOnlyDictionary<string, string>? Components,
        [property: JsonPropertyName("updated_unix")] long? UpdatedUnix,
        [property: JsonPropertyName("error")] string? Error);
}
```
- П8.2. Новый файл `src/PgWorker.Provisioning/Probes/NodeChannelClient.cs`
  (полный код):
```csharp
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Provisioning.Probes;

/// <summary>Адрес mTLS-канала агента ноды (arch/25 №23): порт :8081 ВНУТРИ
/// docker-сети кластера, на хост не публикуется; имя — полное docker-имя ноды
/// (SAN серта, прецедент WalReceiverAddress).</summary>
public static class NodeChannelAddress
{
    public static string ForNode(string cluster, string shard, string node)
        => $"https://pgw-{cluster}-{shard}-{node}:8081";
}

/// <summary>Исход команды канала: StatusCode 0 — транспорт (включая отказ
/// TLS до чтения ключа агентом); 2xx — успех (Ok / applied_revision);
/// 400/409/5xx — transient для тиков воркера (ретрай тиком, spec §3.4).
/// Секретные значения в Error не попадают.</summary>
public sealed record ChannelReply(int StatusCode, bool Ok, long? AppliedRevision, string? Error)
{
    public bool IsSuccess => StatusCode is >= 200 and < 300;
    public bool IsTransient => StatusCode is 0 or >= 400;
}

/// <summary>mTLS-клиент канала доставки (spec §3.4): HandshakeAsync — команда
/// доставки (хендшейк старта, тело с per-install паролями PG + is_tool_start);
/// ApplyAsync — команда применения (ротации/замены, тело пустое, только
/// ready-нодам; воркер различает состояния по статусному ключу etcd).</summary>
public interface INodeChannelClient
{
    Task<ChannelReply> HandshakeAsync(
        string baseUrl, string suPassword, string standbyPassword,
        bool isToolStart, CancellationToken ct);
    Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct);
}

public sealed class NodeChannelClient(HttpClient http) : INodeChannelClient
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task<ChannelReply> HandshakeAsync(
        string baseUrl, string suPassword, string standbyPassword,
        bool isToolStart, CancellationToken ct)
    {
        try
        {
            using var resp = await http.PostAsJsonAsync($"{baseUrl}/api/bootstrap",
                new WorkerPushBody(suPassword, standbyPassword, isToolStart), JsonOpts, ct);
            return await ReadAsync(resp, ct);
        }
        catch (Exception e)
        {
            return new ChannelReply(0, false, null, e.Message.Split('\n')[0]);
        }
    }

    public async Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct)
    {
        try
        {
            using var resp = await http.PostAsync($"{baseUrl}/api/bootstrap/apply", content: null, ct);
            return await ReadAsync(resp, ct);
        }
        catch (Exception e)
        {
            return new ChannelReply(0, false, null, e.Message.Split('\n')[0]);
        }
    }

    private static async Task<ChannelReply> ReadAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        if (!resp.IsSuccessStatusCode)
            return new ChannelReply((int)resp.StatusCode, false, null, null);
        try
        {
            var ok = await resp.Content.ReadFromJsonAsync<CommandBody>(JsonOpts, ct);
            return new ChannelReply((int)resp.StatusCode, true, ok?.AppliedRevision, null);
        }
        catch (JsonException)
        {
            return new ChannelReply((int)resp.StatusCode, false, null, "bad body");
        }
    }

    private sealed record WorkerPushBody(
        [property: JsonPropertyName("su_password")] string SuPassword,
        [property: JsonPropertyName("standby_password")] string StandbyPassword,
        [property: JsonPropertyName("is_tool_start")] bool IsToolStart);

    private sealed record CommandBody(
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision);
}
```
- П8.3. В `src/PgWorker.App/Program.cs` после регистрации `IWalReceiverApiClient`
  (строки ~134–140) добавить:
```csharp
// mTLS-клиент канала доставки секретов (arch/14 §4, arch/25 №23): клиентский
// серт воркера + верификация серверного серта агента по per-install CA и SAN
// полного имени ноды. Таймаут — существующий HttpClient-уровень 10 с.
builder.Services.AddHttpClient("pgnode-channel", c => c.Timeout = TimeSpan.FromSeconds(10))
    .ConfigurePrimaryHttpMessageHandler(sp =>
    {
        var material = sp.GetRequiredService<RestTlsMaterial>();
        var clientCert = sp.GetRequiredService<X509Certificate2>();
        var ca = material.Ca;
        return new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                ClientCertificates = new X509CertificateCollection { clientCert },
                RemoteCertificateValidationCallback = (sender, certificate, _, _) =>
                    NodeChannelSsl.Validate(sender, certificate, ca),
            },
        };
    });
builder.Services.AddSingleton<PgWorker.Provisioning.Probes.INodeChannelClient>(sp =>
    new PgWorker.Provisioning.Probes.NodeChannelClient(
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("pgnode-channel")));
```
  И новый файл `src/PgWorker.Provisioning/Probes/NodeChannelSsl.cs`: static
  `bool Validate(object? sender, X509Certificate? certificate, X509Certificate2 ca)`
  — цепочка `Shared.Tls.TlsChain.ValidateChain` И SAN-пиннинг: `sender is
  System.Net.Security.SslStream ssl` и среди DNS-имён сертификата (тег 0x82
  DER-разбора SAN, образец `PatroniRoleProbe.SanContainsLoopback` в
  `src/PgWorker.PgNode/PatroniRoleProbe.cs`) есть строка, равная
  `ssl.TargetHostName` (TargetHostName = host URL = полное имя ноды).
  ВАЖНО: `X509Certificate2 clientCert`-синглтон из П2.3 резолвится как
  `sp.GetRequiredService<X509Certificate2>()`.
- П8.4. Юнит-тесты (AAA; fake HttpMessageHandler по образцу тестов
  `WalReceiverApiClient`): `NodeChannelClientTests` — Handshake отправляет
  JSON-тело с полями `su_password`/`standby_password`/`is_tool_start`
  (захват тела фейком, ассерт значений) и парсит 200 `{"ok":true}` → Ok,
  AppliedRevision==null; Apply парсит 200 `{"applied_revision":42}` →
  AppliedRevision==42; 409/503/400 → IsTransient и !IsSuccess;
  транспорт-исключение → StatusCode==0. `PgNodeStatusReaderTests` — Parse
  полного JSON; битый → null; StatusKey формат
  `/pgworker/nodes/<C>/<X>/<N>/status`.

## Шаг 9. Воркер: NodeBootstrapDelivery (ключ + статусы + команды) + Ensure-хвосты

**Вход:** шаги 3, 4, 8 завершены.

**Действие (кодер):** новый
`src/PgWorker.Provisioning/Processes/NodeBootstrapDelivery.cs`:
```csharp
public sealed record DeliveryOutcome(bool Delivered, bool Wait, bool Transient, string? Detail);
public interface INodeBootstrapDelivery
{
    Task<DeliveryOutcome> EnsureAsync(ShardTopology topology, string nodeName,
        NodeAddress addr, InstallSecrets secrets, PgTuneResult? tuning, bool syncStrict,
        string restPassword, CancellationToken ct);
}
```
Логика `EnsureAsync` (spec §3.7.2 — «put ключа, затем хендшейк по статусу»):
1) построить JSON (`NodeBootstrapBuilder.Build`) → GET ключа
   (`EtcdEndpointFailover`): значение равно JSON → put не делаем; иначе put →
   повторный GET (нужен `Kv.ModRevision`); etcd-отказ → Transient(текст).
2) прочитать статусный ключ (`PgNodeStatusReader.StatusKey`): нет ключа →
   `Wait` (агент стартует, следующий тик увидит `not_initialized` — arch/14
   §5 C); `not_initialized` → `HandshakeAsync(NodeChannelAddress.ForNode(...),
   secrets.SuPassword, secrets.StandbyPassword, isToolStart: false, ct)`:
   IsSuccess → `Delivered`; иначе Transient(адрес+код); `initializing` /
   `start_failed` → `Wait` (машина конечна; реакция на start_failed —
   надзор, шаг 10); `ready` с `AppliedRevision == modRevision` →
   `Delivered`; `ready` с расхождением → `ApplyAsync(...)`: IsSuccess →
   `Delivered`, иначе Transient(адрес+код).
3) немутирующая проба `IsOnRevisionAsync(topology, nodeName, addr, tuning,
   syncStrict, restPassword, ct)` → `Task<bool>` (для ротатора, шаг 11):
   БЕЗ put и БЕЗ команд канала (не нарушает ≤1 нода/тик): построить
   expected-JSON (`NodeBootstrapBuilder.Build` с переданным `restPassword`) →
   GET bootstrap-ключа: `kv.Value != expected` → false (ключ ещё не
   перерендерен под эту пару); GET статусного ключа: `state == "ready"` И
   `AppliedRevision == (long)kv.ModRevision` → true; прочее → false; отказ
   etcd → false (безопасно: следующая `EnsureAsync` честно разрулит
   transient и вернёт исход для окна ротации).
Вызовы после каждого `driver.EnsureNodeAsync` (контейнер создан/жив —
идемпотентная ветка тоже): ProvisioningProcess.EnsureNodesAsync,
AddShardProcess.EnsureNodesAsync, AdoptionProcess (цикл ensured),
NodeSupervisor: EnsureDeclaredNodesAsync, RecreateMarkedNodesAsync,
rebuild-ветка SuperviseShardAsync. Отказ/Wait — НЕ провал EnsureNode-пути
(спека §3.7.2); Transient — journal-факт (phase=`bootstrap-delivery/<X>/<n>`,
detail=адрес/код); фазы WaitPatroni падают по своим бюджетам при недоставке —
ретрай тиком. `restPassword` — эффективная пара вызывающего
(`RestRotation.EffectivePassword`, окно ротации). object-ноды
(addr.Object != null) — доставку не зовём (spec §3.3). DI: процессы получают
`INodeBootstrapDelivery` параметром (Program.cs). Удаление ключей: отдельной
работы нет — демонтажи уже удаляют префиксы (D2 `/clusters/<C>/`, S3
`shards/<X>/`), bootstrap-ключи внутри них (spec §3.3).

**Выход:** доставка встроена во все Ensure-пути; юнит-тесты зелёные.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
FullyQualifiedName~NodeBootstrapDelivery` → зелёные.

**Связь со spec:** §3.7.2, §2.6 (идемпотентность), §3.6-2 (хендшейк).

**Подпункты:**

- П9.1. Новый файл `src/PgWorker.Provisioning/Processes/NodeBootstrapDelivery.cs`
  (скелет-контракт; реализация по логике шага):
```csharp
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Provisioning.Probes;
using Shared.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>Итог доставки: Delivered — команда принята/агент на ревизии;
/// Wait — машина в ходу (нет статуса / initializing / start_failed — reacts
/// надзор); Transient — канал/etcd недоступны (journal-факт с Detail, НЕ
/// провал EnsureNode-пути — spec §3.7.2).</summary>
public sealed record DeliveryOutcome(bool Delivered, bool Wait, bool Transient, string? Detail);

/// <summary>Доставка bootstrap-данных ноде (arch/14 §4, spec §3.7.2): put
/// ключа /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/bootstrap только
/// при изменении (симметричная сериализация — без дребезга mod_revision),
/// затем хендшейк по статусному ключу: not_initialized → команда доставки с
/// per-install паролями PG; ready c расхождением applied_revision → команда
/// применения. Инициатор всегда воркер; идемпотентен на каждом шаге.</summary>
public interface INodeBootstrapDelivery
{
    Task<DeliveryOutcome> EnsureAsync(
        ShardTopology topology, string nodeName, NodeAddress addr, InstallSecrets secrets,
        PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct);

    /// <summary>Немутирующая проба «нода применила bootstrap с ЭТОЙ парой
    /// restPassword» (ротация, шаг 11): без put и без команд канала.
    /// true ⇔ content хранимого ключа == NodeBootstrapBuilder.Build(…,
    /// restPassword, …) И статус ready И applied_revision == mod_revision
    /// ключа. Отказ etcd → false (EnsureAsync разрулит transient).</summary>
    Task<bool> IsOnRevisionAsync(
        ShardTopology topology, string nodeName, NodeAddress addr,
        PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct);
}

public sealed class NodeBootstrapDelivery(
    IEtcdGateway etcd,
    string[] endpoints,
    INodeChannelClient channel,
    RestCertificateCache restCertificates) : INodeBootstrapDelivery
{
    public static string BootstrapKey(string cluster, string shard, string node)
        => $"/clusters/{cluster}/shards/{shard}/nodes/{node}/bootstrap";

    // EnsureAsync: 1) put ключа при изменении + чтение mod_revision;
    // 2) статусный ключ → реакции (Wait/Handshake/Apply) — по логике шага.
    // IsOnRevisionAsync: expected-JSON → content-сверка ключа → статус
    // ready + applied == mod_revision. Общий построитель JSON с EnsureAsync
    // (NodeBootstrapBuilder) — проба и доставка не расходятся.
    // etcd-обращения — EtcdEndpointFailover.CallAsync(endpoints, …).
}
```
- П9.2. Вызовы (каждое место — сразу ПОСЛЕ успешного `driver.EnsureNodeAsync`/
  `LongCallPolling.EnsureAsync`; объектная ветка `addr.Object is not null`
  уже исключена выше по коду в Adoption/NodeSupervisor): файлы
  `ProvisioningProcess.cs` (EnsureNodesAsync), `AddShardProcess.cs`
  (EnsureNodesAsync), `AdoptionProcess.cs` (цикл после `ensured`),
  `NodeSupervisor.cs` (EnsureDeclaredNodesAsync; RecreateMarkedNodesAsync;
  rebuild-ветка SuperviseShardAsync). Шаблон вызова:
```csharp
            var delivery = await bootstrap.EnsureAsync(
                topology, node.Name, addr, secrets, tuning, syncStrict, restPassword, ct);
            if (delivery.Transient)
                await journal.WritePhaseAsync(cluster, op, $"bootstrap-delivery/{shard.Name}/{node.Name}",
                    claims.InstanceId, delivery.Detail, ct);
```
  (имена локальных переменных — по месту; `restPassword` — эффективная пара
  тика, уже вычислена вызывающим). Конструкторы четырёх процессов получают
  новый параметр `INodeBootstrapDelivery bootstrap` — синхронно обновить
  регистрации в `src/PgWorker.App/Program.cs`.
- П9.3. Юнит-тест `src/tests/PgWorker.UnitTests/Provisioning/NodeBootstrapDeliveryTests.cs`
  (AAA; фейки IEtcdGateway/INodeChannelClient по образцу Fakes.cs; тестовый
  CA — `E2eTestPki.GenerateCa`, как в RestTlsDockerRig): кейсы: ключа нет →
  put сделан, статус not_initialized → Handshake 200 → Delivered; ключ равен
  построенному JSON → put НЕ вызывался (счётчик фейка); статус ready,
  applied==modRevision → Delivered БЕЗ команд канала; статус ready,
  applied!=modRevision → Apply 200 → Delivered; статус initializing → Wait
  без команд; статусного ключа нет → Wait; Handshake 400 → Transient;
  Handshake транспорт (0) → Transient с адресом в Detail; etcd-put отказ →
  Transient. Проба `IsOnRevisionAsync` (счётчики фейка — БЕЗ put и вызовов
  канала): ключ содержит рендер с переданной парой + статус ready +
  applied==modRevision → true (счётчики put/канала нулевые); ключ ещё со
  СТАРОЙ парой → false; ключ с новой парой, но статус
  not_initialized/initializing или applied != modRevision → false;
  статусного ключа нет → false; etcd-отказ → false.

## Шаг 10. NodeSupervisor: шаг хендшейка/конвергенции по статусам; start_failed → новый диск

**Вход:** шаг 9 завершён.

**Действие (кодер):**
1. Удалить `RecreateRestTlsNodesAsync` (метод + вызов на месте шага 1.7
   тика) и `HasRunningDcsWitness` (потребителей после удаления нет — grep).
   На то же место тика — новый шаг `EnsureBootstrapConvergenceAsync`
   (arch/14 §5 C): один range-чтение префикса `/pgworker/nodes/<C>/` (все
   статусные ключи кластера) + по каждому шарду с dsn, по каждой канонической
   ноде (запись portalloc есть, `addr.Object is null`, state не
   Quarantined/Removing; restore/TO_REMOVE — гварды ниже): ключа нет —
   ничего (агент стартует, следующий тик увидит `not_initialized`);
   `not_initialized` → `NodeBootstrapDelivery.EnsureAsync(...)` (ресурсы+
   tuning — `ReadShardResourcesAsync` + `pgtune.Create`, как
   EnsureDeclaredNodesAsync); `initializing` — ждать; `ready` с
   `AppliedRevision != mod_revision` bootstrap-ключа — `EnsureAsync`
   (выполнит apply); `start_failed` — пересоздание ноды С НОВЫМ ДИСКОМ
   (spec §3.7.3): `driver.RemoveNodeAsync(cluster, shard, node)` (kill+remove
   контейнера + remove volume) → `driver.EnsureNodeAsync(...)` (новый volume)
   → delivery-хвост; journal-фаза `node-startfailed-recreate/<X>/<n>`.
   ≤1 мутация (доставка/apply/пересоздание) на тик кластера — паттерн прежнего
   шага. Гварды шага: restore-шарды (свежая сверка `ReadRestoringShardsAsync`,
   как EnsureDeclaredNodesAsync) и TO_REMOVE — пропускаются ЦЕЛИКОМ (реакция
   «не касается», spec §3.7.3/§8.19).
2. Удалить `IClusterDriver.InspectNodeEnvAsync` + реализации Plain/Swarm +
   движковый `InspectContainerEnvAsync` (если потребителей не осталось —
   grep) + фейки `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs:413`,
   `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs:111`.
3. `RestRotation` (`src/PgWorker.Core/Templates/RestRotation.cs`): удалить
   `EnvCert`, `EnvPasswordHash`, `PasswordHash` (потребители — снесённые
   шаги; проверить grep). `RestUsername`/`EffectivePassword` остаются.
4. Удалить интеграционный тест
   `src/tests/PgWorker.IntegrationTests/Docker/RestTlsConvergenceTests.cs`
   (механика упразднена); риг `RestTlsDockerRig` из этого файла переносится в
   новый файл `src/tests/PgWorker.IntegrationTests/Docker/NodeSecretRig.cs`
   (доработка — шаг 11).
5. `DeprovisioningProcess.cs`: в D2-очистку (блок `DeleteAsync(prefix:true)`
   строк ~150–196) добавить `var delNodeStatus = await
   DeleteAsync($"/pgworker/nodes/{cluster}/", prefix: true, ct);` (arch/14
   §3.3 — D2 чистит префикс статусных ключей).
6. Проверка экспозиции (spec §7): панельные читатели `src/AdminPanel.Etcd`
   НЕ разбирают `/pgworker/nodes/…` (панелью статусный ключ не
   разбирается — arch/14 §3.3; префикс `/pgworker/` панель читает
   избирательно) и `…/nodes/<n>/bootstrap` — проверить grep'ом парсеров;
   если какой-то парсер зацепит — явный exclusion (без UI), юнит-ассерт.
7. Новый юнит-тест шага (фейк-канал/фейк-driver): реакции по состояниям
   статуса; ≤1 мутация/тик; гварды restore/TO_REMOVE/object; start_failed →
   RemoveNode+EnsureNode (новый диск) вызовы.

**Выход:** надзор хендшейка/конвергенции/пересоздания; env-инспекция удалена.

**Проверка:** сборка 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.UnitTests -c Release --no-build --filter
"FullyQualifiedName~NodeSupervisor"` → зелёные (существующие тесты надзора
правятся по месту: заглушки InspectNodeEnv удалены).

**Связь со spec:** §3.7.3, §3.6-5, §6.2b, §8.16–8.19, arch/14 §5 C/§3.3.

**Подпункты:**

- П10.1. В `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`: удалить
  метод `RecreateRestTlsNodesAsync` (строки ~557–701) и `HasRunningDcsWitness`
  (~703–722); вызов (строки ~134–147, комментарий «1.7) Общий шаг
  пересоздания…») заменить на:
```csharp
        // 1.7) Шаг хендшейка и конвергенции доставки секретов (arch/14 §5 C,
        // spec §3.7.3): реакции по статусному ключу /pgworker/nodes/<C>/… —
        // not_initialized → доставка; initializing → ждать; ready с
        // расхождением applied_revision → применение; start_failed →
        // пересоздание ноды С НОВЫМ ДИСКОМ. Недоступность канала — transient
        // тика (journal с адресом), статус ноды не портится. Пересоздание по
        // СЕКРЕТАМ не выполняется — замены применяются доставкой.
        var converged = await EnsureBootstrapConvergenceAsync(
            cluster, snap, addresses.Value, restoring, restPassword, ct);
        if (!converged.IsSuccess)
            return Fail(converged.Error!);
```
- П10.2. Новый метод в NodeSupervisor (рядом с EnsureDeclaredNodesAsync);
  контракт (тело — по логике шага):
```csharp
    // Шаг хендшейка/конвергенции (arch/14 §5 C): один range статусных ключей
    // /pgworker/nodes/<C>/ + реакции. Гварды: restore (свежая сверка)/
    // TO_REMOVE-шарды и object-ноды пропускаются; QUARANTINED/REMOVING — вне.
    // ≤1 мутация на тик кластера. start_failed → RemoveNodeAsync (kill+rm
    // контейнера+volume) + EnsureNodeAsync (новый volume) + доставка.
    private async Task<Result> EnsureBootstrapConvergenceAsync(
        string cluster, ClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        HashSet<string> restoring, string restPassword, CancellationToken ct)
    {
        // range /pgworker/nodes/<C>/ → словарь <X>/<N> → PgNodeStatusFact;
        // per shard с dsn (!ToRemove, свежий restore-гвард — образец
        // EnsureDeclaredNodesAsync): per node (state не Quarantined/Removing,
        // addr есть, addr.Object null):
        //   нет статуса / initializing → continue;
        //   not_initialized → delivery.EnsureAsync → journal → return (1 мутация);
        //   ready+расхождение applied!=mod(bootstrap-ключа) → delivery.EnsureAsync
        //     → journal → return (1 мутация);
        //   start_failed → ClaimAliveAsync("node-startfailed/<X>/<N>") →
        //     driver.RemoveNodeAsync → driver.EnsureNodeAsync → delivery →
        //     journal node-startfailed-recreate/<X>/<N> → return (1 мутация).
        // Мод-ревизия bootstrap-ключа — GetAsync ключа (без put — put делает
        // delivery при изменении содержимого).
    }
```
  Переменные/хелперы (`claims`, `journal`, `pgtune`, `driver`, `secrets`,
  `restoring`) — поля/параметры класса, как в EnsureDeclaredNodesAsync.
- П10.3. В `src/PgWorker.Docker/Drivers/ClusterDriver.cs`: из интерфейса
  `IClusterDriver` удалить метод `InspectNodeEnvAsync` (строки ~69–74) и его
  XML-комментарий; удалить реализации `PlainClusterDriver.InspectNodeEnvAsync`
  (~625–640) и `SwarmClusterDriver.InspectNodeEnvAsync` (~770–775). Grep
  `InspectNodeEnvAsync` и `InspectContainerEnvAsync` по `src/`: оставшиеся
  реализации (Fakes.cs, StubScaleDriver.cs) удалить; движковый
  `IDockerEngine.InspectContainerEnvAsync`/реализацию в
  `Shared.Docker/Engine/DockerEngine.cs` удалить ТОЛЬКО если потребителей не
  осталось (иначе — оставить).
- П10.4. В `src/PgWorker.Core/Templates/RestRotation.cs`: удалить члены
  `EnvCert`, `EnvPasswordHash`, `PasswordHash(string)`; XML класса обновить
  (хеш env-инспекции упразднён t05; остались RestUsername и
  EffectivePassword). Grep `EnvPasswordHash|PasswordHash|EnvCert` по `src/` —
  править тесты/код по месту (ассерты env-хеша удалить вместе с
  RestTlsConvergenceTests/старым RestRotationTests-механизмом — шаг 11).
- П10.5. Удалить файл `src/tests/PgWorker.IntegrationTests/Docker/RestTlsConvergenceTests.cs`
  ЦЕЛИКОМ, вынеся `internal static class RestTlsDockerRig` в новый файл
  `src/tests/PgWorker.IntegrationTests/Docker/NodeSecretRig.cs` (namespace
  `PgWorker.IntegrationTests.Docker`, содержимое без изменений на этом шаге;
  доработка — П11.2).
- П10.6. В `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs`
  после строки `var delBackups = await DeleteAsync($"/pgworker/backups/{cluster}/",
  prefix: true, ct);` (строка ~191) добавить:
```csharp
        // Статусные ключи агентов PgNode (arch/14 §3.3: D2 чистит префикс).
        var delNodeStatus = await DeleteAsync($"/pgworker/nodes/{cluster}/", prefix: true, ct);
        if (!delNodeStatus.IsSuccess)
            return delNodeStatus;
```
- П10.7. Юнит-тест `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorBootstrapStepTests.cs`
  (AAA; фейки driver/etcd/канала/delivery по образцу существующих тестов
  надзора): кейсы: статус not_initialized одной ноды → delivery.EnsureAsync
  вызвана ровно один раз, тик успешен; статусы всех ready+applied==mod → нет
  вызовов; ready+расхождение → delivery вызвана; start_failed →
  driver.RemoveNodeAsync + EnsureNodeAsync вызваны (новый диск), delivery
  после; restore-шард/TO_REMOVE-шард/object-нода → никаких вызовов даже при
  start_failed; два кандидата за тик → обработан один.

## Шаг 11. ClusterSecretRotator: R2-rest через bootstrap-перерендер + apply

**Вход:** шаг 10 завершён.

**Действие (кодер):** в `ClusterSecretRotator` заменить блок R2-rest
rolling-гейта (строки ~170–205: `InspectNodeEnvAsync` + hash-сверка) на
проход применения NEW (arch/14 §5 I, spec §3.7.4):
- Если `rest_pending` есть: для dsn-шардов не-restore/не-TO_REMOVE,
  канонических нод (не object, не Quarantined/Removing) — порядок: реплики
  вперёд, лидер последним (лидер — `RangeAsync(/service/<scope>/)` +
  `ClusterSnapshotParser.ParseService`, как в надзоре): первая нода, НЕ
  прошедшая немутирующую пробу `INodeBootstrapDelivery.IsOnRevisionAsync(…,
  pending, ct)` (true ⇔ content хранимого bootstrap-ключа == рендер под NEW +
  статус `ready` + `applied_revision == mod_revision` — сверка именно по NEW,
  иначе нода со стационарным applied==mod(OLD) ошибочно считалась бы
  применившей) → `NodeBootstrapDelivery.EnsureAsync` (restPassword = pending:
  put ключа NEW + apply по готовности) → исход доставки ЗАВЕРШАЕТ тик В
  ЛЮБОМ случае (Delivered и нет): Finish-фаза `rotate-rest-rolling/<X>/<n>`,
  Done — окно живёт, следующая нода следующим тиком; безусловный выход после
  первой доставки обязателен: синхронный apply иначе позволил бы одному тику
  применить NEW сразу нескольким нодам (нарушение ≤1 нода/тик и порядка).
  Ключ перезаписывается
  ТОЛЬКО у обрабатываемой ноды (иначе конвергенция-шаг надзора применила бы
  NEW всем без порядка, нарушая ≤1 нода/тик). Все не-пропущенные ноды прошли
  пробу по NEW → R3. Skip-нутые гейтом (restore/TO_REMOVE/object) в
  блокировку R3 не входят (arch/14 §5 I: их ноды создадутся с NEW после R3).
- Конструктор ротатора получает `INodeBootstrapDelivery bootstrap` +
  `PgtuneInputsFactory pgtune` (тюнинг для рендера ключа; ресурсы —
  `ReadShardResourcesAsync` по образцу надзора; заявки обязательны —
  отсутствие/неполнота → фейл фазы тика, как в EnsureNode-путях).
- R3/R4/`rest_pending`-семантика/гварды окна (skip PATCH `/config`,
  RecreateMarked-soft до R3) — БЕЗ изменений. Гвард
  `driver.SupportsRunningInspection` НЕ переносить (канал
  режимно-симметричен, spec §5).
- Переписать интеграционный `RestRotationTests` на живом etcd + фейк-канале
  (docker-стаб не нужен), прогресс по статусным ключам.

**Выход:** ротация rest_password без пересоздания контейнеров; тесты.

**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test
src/tests/PgWorker.IntegrationTests -c Release --no-build --filter
FullyQualifiedName~RestRotation` → зелёный (переписанный).

**Связь со spec:** §3.7.4, §6.3, §8.8/§8.20, arch/14 §5 I.

**Подпункты:**

- П11.1. В `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs`:
  конструктору добавить параметры `INodeBootstrapDelivery bootstrap,
  PgtuneInputsFactory pgtune` (тип — как у NodeSupervisor). Подготовка блока
  (две механические правки ДО вставки нового кода): (а) объявление
  `var restoring = await ReadRestoringShardsAsync(cluster, ct);` с проверкой
  `if (!restoring.IsSuccess) return await FailAsync(cluster,
  restoring.Error!, "backups-read", ct);` (строки фактического файла ~181–183,
  попадают в заменяемый диапазон) — ПЕРЕНЕСТИ выше, оставив в методе ДО
  места вставки (новый код использует `restoring.Value.Contains(...)`); (б) в
  класс ротатора добавить приватную копию чтения заявок ресурсов (в самом
  ротаторе метода нет — есть в NodeSupervisor/Provisioning/AddShard/
  Adoption/Restore; прецедент упрощённой копии — `RestoreProcess.cs:1058`),
  дословно по `NodeSupervisor.cs:1084`:
```csharp
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }
```
  Затем блок строк ~170–205 (от комментария «R2-rest rolling-гейт» до
  закрывающей скобки `if (driver.SupportsRunningInspection) {…}`) заменить
  на (перенесённое объявление `restoring` из пункта (а) стоит НЕПОСРЕДСТВЕННО
  ПЕРЕД этим блоком):
```csharp
        // R2-rest (arch/14 §5 I): NEW-пара применяется доставкой каналом БЕЗ
        // пересоздания: перерендер bootstrap-ключа ОДНОЙ ноды с pending-парой
        // + команда применения (≤1 нода/тик; реплики вперёд, лидер последним;
        // restore/TO_REMOVE-шарды и усыновлённые ноды гейт пропускает — их
        // ноды создадутся с NEW после R3). Прогресс — немутирующая проба
        // IsOnRevisionAsync(pending): content хранимого ключа == рендер под
        // NEW + ready + applied_revision == mod_revision (сверка по NEW, а не
        // по текущему/OLD ключу — нода со стационарным applied==mod(OLD)
        // пробу НЕ проходит и получает доставку NEW).
        if (work.Value?.RestPending is not null)
        {
            foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
            {
                if (restoring.Value.Contains(shard.Name))
                    continue; // демонтажем владеет restore-процесс
                var scopeKvs = await RangeAsync($"/service/{cluster}-{shard.Name}/", ct);
                if (!scopeKvs.IsSuccess)
                    return await FailAsync(cluster, scopeKvs.Error!, $"scope/{shard.Name}", ct);
                var leader = ClusterSnapshotParser.ParseService(scopeKvs.Value)
                    .FirstOrDefault()?.LeaderName;
                var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
                var tuning = pgtune.Create(resources);
                var ordered = shard.Nodes
                    .Where(n => n.State is not (NodeState.Quarantined or NodeState.Removing))
                    .OrderBy(n => n.Name == leader ? 1 : 0); // реплики вперёд, лидер последним
                foreach (var node in ordered)
                {
                    if (!addresses.Value.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                        || addr.Object is not null)
                        continue; // записи нет / усыновлённая (R9) — гейт пропускает
                    var topology = new ShardTopology(cluster, shard.Name, $"{cluster}-{shard.Name}",
                        addresses.Value.Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
                            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));
                    if (await bootstrap.IsOnRevisionAsync(
                            topology, node.Name, addr, tuning,
                            snap.Config.SyncStrict, newRestPassword, ct))
                        continue; // применила NEW — следующая нода

                    // ОДНА доставка на тик (spec §3.7.4: ≤1 нода/тик, реплики
                    // вперёд, лидер последним): доставка завершает тик В
                    // ЛЮБОМ случае — синхронный apply не даёт этому же тику
                    // уйти дальше по нодам; следующая нода — следующим тиком.
                    // Finish фиксирует фазу rotate-rest-rolling/<X>/<n> в обоих
                    // исходах; исход диагностируется следующим тиком (проба
                    // IsOnRevisionAsync: применила — идём дальше, нет — повтор).
                    await bootstrap.EnsureAsync(
                        topology, node.Name, addr, secrets, tuning,
                        snap.Config.SyncStrict, newRestPassword, ct);
                    return await Finish(cluster,
                        $"rotate-rest-rolling/{shard.Name}/{node.Name}", ProcessOutcome.Done, ct);
                }
            }
        }
```
  Переменные `restoring`/`newRestPassword`/`secrets`/`addresses` вычислены
  выше по методу; блок стоит ПОСЛЕ их чтения (замена прежнего места).
  Прямые чтения статуса/ключа из rotator'а УДАЛЕНЫ (весь детект «применила
  NEW» — внутри `IsOnRevisionAsync`, П9.1).
- П11.2. Переписать `src/tests/PgWorker.IntegrationTests/Docker/RestRotationTests.cs`
  на живом etcd + фейк-канале (риг из `NodeSecretRig.cs` упростить: etcd-сид
  контроль-плейна (config/replicas/dsn/nodes/state/master/request_*/portalloc/
  claim) БЕЗ docker-контейнеров и стаб-образа — env-инспекции больше нет;
  фейк `INodeChannelClient`: Handshake/Apply по адресу → читает bootstrap-ключ
  из etcd и запоминает applied_revision в словаре адресов; статусы —
  fake-агент пишет статусные ключи в etcd по факту «применения»). Тест
  `Rotation_RestAppliedWithoutRecreate_CommitsAndIdempotent` (AAA): Arrange —
  ensure rest_password=OLD, тик надзора доставил (статусы ready, applied==mod);
  Act — заявка + цикл тиков Rotator до исчезновения заявки; Assert —
  rest_pending зафиксирован и равен итоговому ключу; bootstrap-ключи обеих
  нод содержат pending-пароль внутри spilo_configuration (подстрочный ассерт
  GET ключа); ПЕРВЫЙ тик после заявки обработал РОВНО ОДНУ ноду и это реплика
  (журнал вызовов фейка); регрессия content-пробы: до первой доставки NEW
  обе ноды имеют OLD-ключ со стационарным ready+applied==mod(OLD) — тик
  ОБЯЗАН начать доставку (не закрыть окно преждевременно); порядок — лидер
  применил последним (индекс применения лидера > индекса реплики); заявка
  снята, pending сброшен; повторная заявка — новый проход (идемпотентность).
  Teardown: etcd-префиксы кластера удалить (по образцу DisposeAsync рига).

## Шаг 12. Образ ноды: supervisord autostart, entrypoint без openssl, обёртка spilo

**Вход:** шаги 1–11 завершены; образ собирается.

**Действие (кодер):** поведение образа — по spec §3.2:
1. `docker/node/supervisord.conf`: `[program:pgnode]` — единственный с
   автостартом (не указывать autostart=false); `[program:spilo]`,
   `[program:pg_doorman]` — `autostart=false`; конфиг pg_doorman —
   позиционный аргумент `/run/pgw/doorman/pg_doorman.yaml`; `user=postgres` у
   pg_doorman уже есть. Комментарии секций обновить (старт — командой агента).
2. `docker/node/docker-entrypoint.sh`: проверка `DOORMAN_CONFIG` →
   `DOORMAN_CONFIG_TEMPLATE` (пустой — старт-отказ); блоки openssl-генерации
   серта пулера и chown/chmod doorman-файлов — УДАЛИТЬ (файлы рендерит агент
   в tmpfs под postgres); генерация haproxy/pgw-node.env/CA-файла/pgroot —
   без изменений.
3. `docker/node/spilo-supervised.sh`: перед `/launch.sh &` — fail-fast
   проверка tmpfs-файлов и экспорт (код в подпункте); файлов нет → диагностика
   в stderr и exit 1 (supervisord старт-отказ не лупит).
4. Тест `src/tests/PgWorker.IntegrationTests/Docker/NodeEntrypointRestCaTests.cs`
   — обновить под DOORMAN_CONFIG_TEMPLATE/отсутствие openssl; проверить
   сборку образа локально.

**Выход:** образ: автостарт только pgnode; entrypoint без секретов/openssl;
обёртка ждёт доставку.

**Проверка:** сборка образа успешна (команда П12.5);
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c
Release --no-build --filter FullyQualifiedName~NodeEntrypoint` → зелёный.

**Связь со spec:** §3.2 (образ), §7 (риск «обёртка ждёт tmpfs»).

**Подпункты:**

- П12.1. `docker/node/supervisord.conf`: в `[program:spilo]` после
  `command=/spilo-supervised.sh` добавить строку `autostart=false` и
  обновить комментарий секции: «старт — командой агента pgnode после доставки
  секретов (arch/14 §2.1/§4)». В `[program:pg_doorman]`: строку
  `command=/usr/local/bin/pg_doorman /etc/pg_doorman/pg_doorman.yaml` →
  `command=/usr/local/bin/pg_doorman /run/pgw/doorman/pg_doorman.yaml`;
  добавить `autostart=false`; комментарий: «конфиг — позиционный аргумент
  tmpfs (рендерит агент PgNode); TLS-файлы — /run/pgw/doorman/tls.*»;
  устаревший фрагмент комментария про «файл пишет главный entrypoint из env
  DOORMAN_CONFIG» удалить. Секцию `[program:pgnode]` не менять (шаг 1).
- П12.2. `docker/node/docker-entrypoint.sh`: блок
```sh
if [ -n "$DOORMAN_CONFIG" ]; then
    printf '%s\n' "$DOORMAN_CONFIG" > /etc/pg_doorman/pg_doorman.yaml
else
    echo "pg_doorman: DOORMAN_CONFIG env is empty - cannot start the mandatory pooler"
    exit 1
fi
```
  заменить на:
```sh
# Шаблон конфига пулера (неконфиденциальный): пароль подставляет агент PgNode
# при рендере в tmpfs (arch/14 §4); пустой шаблон — старт-отказ контейнера.
if [ -z "$DOORMAN_CONFIG_TEMPLATE" ]; then
    echo "pg_doorman: DOORMAN_CONFIG_TEMPLATE env is empty - cannot start the mandatory pooler"
    exit 1
fi
```
  и удалить блоки: openssl-генерации (`openssl req -x509 …`), chown/chmod
  doorman-файлов (`chown postgres:postgres /etc/pg_doorman/…`, `chmod 600 …`)
  и их комментарии (серт доставляет агент). Строку `mkdir -p /etc/haproxy
  /etc/pg_doorman` → `mkdir -p /etc/haproxy`; блок записи haproxy.cfg — без
  изменений.
- П12.3. `docker/node/spilo-supervised.sh`: после строки `#!/bin/sh` и
  комментария, ПЕРЕД `/launch.sh &` добавить:
```sh
# Доставка секретов (arch/14 §4): секретные env ПРОЦЕССА — из tmpfs-файлов
# агента PgNode; файлов нет (доставка не выполнена) — немедленный отказ
# старта: supervisord старт-отказы не лупит, воркер повторит доставкой.
for f in /run/pgw/spilo-env /run/pgw/spilo-configuration.yml /run/pgw/rest-cert.pem /run/pgw/rest-key.pem; do
    if [ ! -s "$f" ]; then
        echo "spilo: $f is missing - waiting for secret delivery from the worker (pgnode bootstrap)" >&2
        exit 1
    fi
done
set -a
. /run/pgw/spilo-env
set +a
export SPILO_CONFIGURATION="$(cat /run/pgw/spilo-configuration.yml)"
export SSL_RESTAPI_CERTIFICATE="$(cat /run/pgw/rest-cert.pem)"
export SSL_RESTAPI_PRIVATE_KEY="$(cat /run/pgw/rest-key.pem)"
```
- П12.4. `src/tests/PgWorker.IntegrationTests/Docker/NodeEntrypointRestCaTests.cs`
  (существующий тест entrypoint-контейнера): env-переменную контейнера
  `DOORMAN_CONFIG` (если ставится) → `DOORMAN_CONFIG_TEMPLATE` со значением
  `pooler template ${PGW_SU_PASSWORD}` (неконфиденциальный шаблон); ассерты
  про /etc/pg_doorman/tls.* (если есть) удалить; ассерт CA-файла
  (`pgw-node-ca.pem`, 600, postgres) — оставить (механизм не изменился).
  Если тест стартует supervisord-программы — pg_doorman/spilo теперь
  autostart=false: ассерты их старта убрать (ожидать только работу
  entrypoint-фазы). Тестовые env — только неконфиденциальные.
- П12.5. Сборка образа (проверка шага):
```sh
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-node-secret-push
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/PgWorker.PgNode -c Release -r linux-x64 --self-contained -o artifacts/node-wal --nologo
DOTNET_CLI_UI_LANGUAGE=en dotnet publish src/PgWorker.NodeTools -c Release -r linux-x64 --self-contained -o artifacts/node-tools --nologo
docker build -q -f docker/node/Dockerfile --build-arg PGNODE_PUBLISH_DIR=artifacts/node-wal --build-arg NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools --build-arg DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0 -t pgworker-node:t05check .
```
  Ожидаемый результат: обе публикации 0w/0e, образ собран (выведен sha).
  RID — по архитектуре хоста (`linux-arm64` на Apple Silicon), как в
  `dev-stand/adminpanel/checks/00-up.sh`.

## Шаг 13. E2E t05: негативный inspect, хендшейк, рестарт, сбой старта, ротация

**Вход:** шаги 1–12 завершены; статическая подготовка — шаг 14.

**Действие (кодер):** новый
`src/tests/PgWorker.E2eTests/E2e/E2eNodeSecretsScenarios.cs` (каркас — по
образцу `E2eScaleScenarios`: guid-кластер, `Fx.StartHostAsync`,
`SeedClusterAsync`, budgets через `E2eFixture.WaitForAsync`, полный teardown
фикстуры, динамические порты, свои etcd-префиксы — канон docs/e2e-isolation.md;
телеметрия — docs/e2e-launch.md):
1. `NodeSecrets_DockerInspectHasNoSecrets` (spec §6.5/§9.5): provision до
   Active → docker inspect всех `pgw-<C>-*`: env без запрещённых ключей
   (список §3.1) и без фактических значений прогона (SuPassword/
   StandbyPassword/BucketAdminPassword/MoverPassword/rest_password из etcd);
   содержит `DOORMAN_CONFIG_TEMPLATE` с `${PGW_SU_PASSWORD}` и
   `PGW_AGENT_CLIENT_CA`.
2. `NodeSecrets_Handshake_ReachesReady` (spec §6.2a/§9.6): provision →
   дождаться (etcd) статусных ключей всех нод `state=ready` с
   `applied_revision != null`; компоненты pg/doorman = ready; SELECT через
   опубликованный `:6432` (`sslmode=require`) работает (ac2-паттерн).
3. `NodeSecrets_NodeRestart_ConvergedBySupervisorTick` (spec §6.2): SELECT
   через doorman жив → `docker restart` ноды → lease-ключ статуса исчез
   (проверить etcd-отсутствие в течение ~20 c) → сервисы стоят (SELECT-проба
   падает) → `WaitForAsync` (бюджет 90 c): статус снова `ready`, SELECT через
   doorman жив — конвергенция тика доставила (хендшейк заново).
4. `NodeSecrets_StartFailed_RecreatesNodeWithNewDisk` (spec §6.2b/§9.6):
   provision → готова; инжекция сбоя старта: `docker exec <нода> mv
   /usr/local/bin/pg_doorman /usr/local/bin/pg_doorman.bak` (rw-слой
   контейнера переживает restart) + `docker restart <нода>` → хендшейк заново
   → старт pg_doorman падает ×3 → `WaitForAsync` статус `start_failed` с
   error и components.doorman=failed → надзор пересоздаёт: `WaitForAsync`
   (бюджет 120 c): container id изменился, StartedAt изменился, статус
   `ready`, SELECT через doorman жив; диск НОВЫЙ: перед инжекцией записать
   маркер `docker exec … touch /home/postgres/pgdata/MARKER` — после
   восстановления маркера нет (volume пересоздан).
5. Расширить `src/tests/PgWorker.E2eTests/E2e/E2eRotateScenarios.cs`
   (`Rotate_TicketRotatesPerClusterSecretsOnAllShards`): до заявки — снять
   `StartedAt` всех контейнеров нод; после завершения ротации — ассерт:
   StartedAt каждого контейнера НЕ изменился (ротация без пересоздания,
   spec §6.3/§9.6).

**Выход:** пять E2E-проверок t05 в общем E2eFixture.

**Проверка:** шаг 14 (полная серия). Локально после E2ePrepare:
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release
--no-build --filter "FullyQualifiedName~E2eNodeSecretsScenarios"` → зелёный.

**Связь со spec:** §6.2/§6.2a/§6.2b/§6.3/§6.5, §9.5/§9.6.

**Подпункты:**

- П13.1. Каркас нового файла `src/tests/PgWorker.E2eTests/E2e/E2eNodeSecretsScenarios.cs`
  — копия паттерна `E2eScaleScenarios.cs`: тот же класс-фикстура `E2eFixture Fx`
  (конструктор), `using`-набор, `CancellationTokenSource` с
  `CancelAfter(TimeSpan.FromMinutes(6))`, guid-кластер
  `$"nsec{Fx.ClusterTag}"`. Тест 1 (скелет):
```csharp
    [Fact]
    public async Task NodeSecrets_DockerInspectHasNoSecrets()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(6));
        var ct = cts.Token;
        var cluster = $"nsec{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var p1 = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach Active via the secret handshake");

        // Запрещённые env-ключи (spec §3.1) и фактические секретные значения
        // прогона — подстрочный ассерт docker inspect всех нод кластера.
        var restPassword = await GetOrNullAsync($"/clusters/{cluster}/rest_password", ct);
        var containers = (await E2eFixture.RunDockerAsync(
            ["ps", "-aq", "--filter", $"name=pgw-{cluster}-"], ct))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
        containers.Should().NotBeEmpty("the cluster nodes are up");
        foreach (var container in containers)
        {
            var envJson = await E2eFixture.RunDockerAsync(
                ["inspect", container.Trim(), "--format", "{{json .Config.Env}}"], ct);
            string[] forbiddenKeys = [ "PGUSER_SUPERUSER", "PGPASSWORD_SUPERUSER",
                "PGUSER_STANDBY", "PGPASSWORD_STANDBY", "PGW_BUCKET_ADMIN_USER",
                "PGW_BUCKET_ADMIN_PASSWORD", "PGW_BUCKET_MOVER_PASSWORD",
                "SSL_RESTAPI_CERTIFICATE", "SSL_RESTAPI_PRIVATE_KEY",
                "PGW_REST_PASSWORD_HASH", "SPILO_CONFIGURATION", "DOORMAN_CONFIG" ];
            foreach (var key in forbiddenKeys)
                envJson.Should().NotContain($"{key}=", "node env carries no secrets (spec §3.1)");
            envJson.Should().NotContain(E2eFixture.SuPassword);
            envJson.Should().NotContain(E2eFixture.StandbyPassword);
            envJson.Should().NotContain(E2eFixture.BucketAdminPassword);
            envJson.Should().NotContain(E2eFixture.MoverPassword);
            if (restPassword is { Length: > 0 } rest)
                envJson.Should().NotContain(rest, "rest_password lives only in the etcd bootstrap key");
            envJson.Should().Contain("DOORMAN_CONFIG_TEMPLATE=");
            envJson.Should().Contain("${PGW_SU_PASSWORD}");
            envJson.Should().Contain("PGW_AGENT_CLIENT_CA=");
        }
    }
```
  (вспомогательные `SeedClusterAsync/ProvisionedAsync/GetOrNullAsync` —
  переиспользовать из E2eScaleScenarios, как соседние сценарий-классы: если
  они private — вынести в общий partial/статик по образцу переиспользования в
  E2eScenarios; `E2eFixture.SuPassword/StandbyPassword/…` — фактические
  секреты контура). Сверить фактические сигнатуры фикстуры.
- П13.2. Тесты 2–4 (скелеты, тот же файл):
```csharp
    [Fact]
    public async Task NodeSecrets_Handshake_ReachesReady()
    {
        // Arrange/Assert (spec §6.2a): provision → WaitForAsync по etcd:
        // все статусные ключи /pgworker/nodes/<C>/<X>/<N>/status имеют
        // state=="ready", applied_revision != null, components pg/doorman ==
        // "ready" (бюджет 120 c); SELECT через опубликованный :6432
        // sslmode=require (паттерн TryInsertAppAsync) работает.
    }

    [Fact]
    public async Task NodeSecrets_NodeRestart_ConvergedBySupervisorTick()
    {
        // Arrange: provision + SELECT через doorman жив.
        // Act: docker restart контейнера ноды1 → WaitForAsync(статусного
        // ключа НЕТ, 30 c) — lease истёк; сразу SELECT-проба падает (сервисы
        // стоят: supervisord autostart только pgnode).
        // Assert (spec §6.2): WaitForAsync(статус state=="ready" И SELECT
        // через doorman жив, 90 c) — тик конвергенции доставил (хендшейк
        // заново); статус-ключ агента бэкапов жив (стрим поднялся, §6.7).
    }

    [Fact]
    public async Task NodeSecrets_StartFailed_RecreatesNodeWithNewDisk()
    {
        // Arrange: provision + SELECT жив; маркер на диск:
        //   docker exec <node> touch /home/postgres/pgdata/MARKER
        // Act (spec §6.2b): docker exec <node> mv /usr/local/bin/pg_doorman
        //   /usr/local/bin/pg_doorman.bak; docker restart <node>;
        //   WaitForAsync(статус state=="start_failed" И components.doorman
        //   =="failed" И error непуст, 120 c).
        // Assert: WaitForAsync(контейнер пересоздан: container id/StartedAt
        //   изменились; статус state=="ready"; SELECT через doorman жив,
        //   150 c) — надзор пересоздал с новым диском; маркер исчез:
        //   docker exec <new container> test -f /home/postgres/pgdata/MARKER
        //   → ненулевой код (volume новый).
    }
```
- П13.3. В `E2eRotateScenarios.cs` (`Rotate_TicketRotatesPerClusterSecretsOnAllShards`):
  перед подачей заявки собрать словарь `startedAt` — по каждому контейнеру
  нод кластера `docker inspect <c> --format {{.State.StartedAt}}`; после
  успешного завершения ротации — повторно снять и ассертнуть поэлементное
  равенство (`spec §6.3: ротация без пересоздания контейнеров`).

## Шаг 14. Мерж-гейт (приёмка spec §9)

**Вход:** шаги 1–13 завершены.

**Действие (кодер):** прогон полного гейта двумя ОТДЕЛЬНЫМИ командами на
каждую серию; зачистка контейнеров/сетей между сериями (`docker network
prune -f` при осиротевших `pgw-net-*`); упавшие тесты НЕ перезапускать без
разбора логов (AGENTS.md).

1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
   Release --nologo` → 0 warnings / 0 errors.
2. Юниты + интеграции: `DOTNET_CLI_UI_LANGUAGE=en dotnet test
   src/tests/PgWorker.UnitTests -c Release --no-build`; затем `…
   src/tests/PgWorker.IntegrationTests -c Release --no-build` → зелёные.
3. Статическая подготовка: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
   src/tests/PgWorker.E2ePrepare -c Release` → штамп готовности.
4. docker-E2E (после зачистки): минимум
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c
   Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`;
   затем полный прогон E2eFixture (затронуты provisioning/надзор/ротация):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c
   Release --no-build` → зелёные, включая ac2-doorman-sql, WalStream-серии,
   E2eNodeSecretsScenarios (все 4), Rotate (с StartedAt-ассертом).
5. Grep-контроль: в `arch/` нет «walreceiver» как имени компонента
   (`grep -rn "walreceiver" arch/` — функциональные упоминания приёма WAL
   допустимы; имя компонента/программы — нет); `docker inspect` нод — без
   секретов (закрыто E2E-ассертом).
6. Роадмап: удалить тег `t05-node-secret-push` из
   `arch/roadmap/pgworker.md` мерж-коммитом (включая `←`-зависимости) — при
   мерже в main (не в feature-ветке).

**Выход:** все критерии §9 закрыты; ветка готова к ревью/мержу.

**Проверка:** все команды гейта зелёные; логи прогонов сохранены в
`/tmp/pgw-e2e-artifacts-*` (телеметрия — docs/e2e-launch.md).

**Связь со spec:** §9 целиком, §5 (ограничения).

**Подпункты:** нет (исполнение и разбор логов — сильная модель).

---

## Самопроверка плана (выполнена plan-writer'ом)

- Покрытие spec: §3.1 (шаги 3–4, 13), §3.2 (1, 4, 12), §3.3 (3, 5, 8–11,
  10.5-D2), §3.4 (2, 6–9), §3.5 (3, 5, 12 — spilo-env канон), §3.6 (5–7:
  машина/фазы/txn/≤3/SELECT-1/is_tool_start/apply), §3.7 (3, 4, 9, 10, 11,
  2), §3.8 (1, 14.5), §4 (порядок шагов), §5 (глобальные ограничения), §6.1/
  6.2/6.2a/6.2b/6.3/6.4(юнит П6.2-11)/6.5/6.6/6.7 (13), §7 (вкл. панель —
  10.6; start_failed-риск — 10/13; долгое initializing — 6), §8.1–8.22, §9
  (14 + проверки шагов).
- Имена/сигнатуры — из реального кода worktree (IProcessRunner,
  EnsureNodeAsync/BuildSpec, WalReceiverStatusSession-паттерн,
  TxnCompare.ValueEqual, ContainerSpec.Tmpfs, DeprovisioningProcess-чистки,
  ReadShardResourcesAsync-образец NodeSupervisor.cs:1084, риги, скрипты);
  сквозная согласованность сигнатур PgNodeMachine/
  INodeChannelClient/INodeBootstrapDelivery/PgNodeStatusReader между
  шагами 5–11 проверена.
- Подпункты самодостаточны (полные коды/точные замены; П6.2/П10.2 — полные
  контракты с перечнем обязательных кейсов).
- Развилок нет: spilo-env — решение пользователя; CN=pgworker — прочтение
  «CN по роли»; каталоги publish-артефактов — вне перечня §3.8; численные
  интервалы проб (5 с / Timeout=5) — единственное, что spec §8.17 делегирует
  плану, зафиксированы в глобальных ограничениях; txn-переход машины —
  compare по VALUE прочитанного статуса (`TxnCompare.ValueEqual`,
  §3.4/§8.16, arch/14 §3.3 — устойчив к keepalive-put'ам неизменного
  значения); ok-пинок (Started|Nop → 200), 400/409/503-семантика, порядок
  рестартов при смене серта, гварды реакций надзора — выписаны из spec
  дословно.
- Рев. 3 (фиксы ревью plan↔spec, verdict CHANGES_REQUESTED): (1) TLS-пути
  шаблона doorman — `/run/pgw/doorman/tls.crt|tls.key` (П3.3; spec §3.5)
  + синхронные ассерты П3.4/П4.5(б)/П5.8 — устранён сценарий
  «tls_mode:require без серта → start_failed → вечное пересоздание»;
  (2) детект «применила NEW» в R2-rest — немутирующая проба
  `INodeBootstrapDelivery.IsOnRevisionAsync` (П9.1/П9.3: content-совпадение
  хранимого ключа с рендером под NEW + ready + applied==mod_revision) вместо
  сравнения applied с ревизией ещё-OLD ключа в П11.1; rotator лишён прямых
  чтений ключа/статуса; регрессионный ассерт в П11.2.
- Рев. 4 (повторное ревью plan↔spec, verdict CHANGES_REQUESTED; блокеры
  рев. 3 подтверждены закрытыми): (1) blocker — R2-rest: исход доставки
  первой ноды завершает тик БЕЗУСЛОВНО (`return await Finish(
  rotate-rest-rolling/<X>/<n>, Done)` в обеих ветвях; фаза фиксируется в
  обоих исходах) — синхронный apply более не позволяет одному тику применить
  NEW нескольким нодам (§3.7.4 ≤1 нода/тик, порядок реплики→лидер);
  (2) minor — П11.1: объявление `restoring` (строки ротатора ~181–183,
  внутри заменяемого диапазона) переносится выше блока; в ротатор добавлена
  приватная копия `ReadShardResourcesAsync` (дословно NodeSupervisor.cs:1084,
  прецедент RestoreProcess.cs:1058) — новый код компилируется; (3) minor —
  txn-переход машины: `TryTransitionAsync(newValue, expectedValue)` на
  `TxnCompare.ValueEqual` прочитанного значения (П5.6/П6; §8.16, arch/14
  §3.3 — прямое соответствие spec, устойчивость к keepalive-put'ам того же
  значения, прецедент PortAllocLock); юнит-кейсы П6.2 дополнены (2а)/(2б).
- Рев. 5 (третье ревью plan↔spec, verdict CHANGES_REQUESTED; все три фикса
  рев. 4 подтверждены закрытыми): (1) minor — П5.6 `TryTransitionAsync`:
  вызов txn через generic `EtcdEndpointFailover.CallAsync<T>` (не-generic
  `FailoverAsync` копируемой сессии принимает только не-generic `Result`, а
  `TxnAsync` возвращает `Result<TxnResult>` — CS1662/CS1061; перегрузка и
  прецеденты сверены по worktree: KafkaWorker NodeSupervisor.cs:428-429,
  PortAllocHealer.cs:361-362), строка «сигнатуры сверены» дополнена;
  (2) nit — П11.1: неиспользуемая переменная `delivered` убрана (исход
  доставки диагностируется следующим тиком через `IsOnRevisionAsync`;
  ассерты П11.2 строятся на журнале фейк-канала и не зависят от переменной).
