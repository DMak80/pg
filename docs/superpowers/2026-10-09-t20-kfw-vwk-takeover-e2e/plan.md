# t20-kfw-vwk-takeover-e2e — план реализации, ревизия 3 (env-TLS у ValkeyWorker + host-kill E2E)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/superpowers/2026-10-09-t20-kfw-vwk-takeover-e2e/spec.md` (ревизия 3, одобрена; план следует ей буква-в-букву — executors читают оба). Спека — источник истины по механике; в плане — точные файлы/методы/бюджеты/команды.

**Задача ДВУХСОСТАВНАЯ** (spec, шапка):

1. **Прод-изменение**: TLS-доставка сертов нод ValkeyWorker — PEM в env ноды
   (`VALKEY_TLS_{CERT,KEY,CA}`) + cmd-обёртка старта (модель KafkaWorker);
   демонтаж helper-механики (`Put/GetVolumeArchiveAsync`, `CreateHelperAsync`,
   `TarArchive`) из `Shared.Docker`; миграция живых кластеров надзором;
   финальный демонтаж Valkey ≤15 с (класс «409 volume-in-use» исчезает).
2. **Тестовая часть**: docker-E2E host-kill takeover для KafkaWorker /
   ValkeyWorker (инварианты I1–I7, spec §2).

**Текущее состояние ветки (учтено):** тестовая часть прошлой итерации УЖЕ
написана на СТАРОЙ volume-модели и зелёная (рабочая копия, коммитов нет):
`ValkeyE2ePhase.cs`, `ValkeyE2eEnvironment.cs` (`StartTwoAsync`),
`ValkeyE2eTakeoverTests.cs`, `KafkaE2eEnvironment.cs`, `KafkaE2ePhase.cs`,
`KafkaE2eTakeoverTests.cs` — все в `src/tests/{Valkey,Kafka}Worker.IntegrationTests/E2e/`.
Прод-код НЕ тронут. После прод-части Kafka-файлы адаптируются точечно (Э5);
Valkey-кейс переписывается под жертву-демонтаж (Э6, ротация отменена —
спека §8/§15-Р2), остальная Valkey-инфраструктура переиспользуется.

## Goal

- **Часть A (прод)**: нода Valkey получает TLS-материал через env контейнера;
  cmd-обёртка раскатывает PEM в `/tls` (каталог в ФС контейнера, НЕ том) и
  exec'ает `valkey-server` с прежним каноническим набором args. TLS-том
  `vwk-<C>-tls` и helper-механика выпадают из модели и демонтируются. Живые
  кластеры старой модели мигрируются надзорной сверкой (пересоздание с env +
  легаси-чистка осиротевшего тома).
- **Часть B (тесты)**: доказать docker-E2E, что при внезапной смерти ОДНОГО
  инстанса воркера посреди длительной надзорной операции второй бесшовно
  забирает надзор и доигрывает операцию — без дублей, с целым контроль-плейном
  и живым датаплейном; kill доказательно попадает в ДЕРЖАТЕЛЯ клэйма (резолв
  по etcd-фактам перед kill).

## Порядок этапов (маппинг на spec §12 (а)–(ж))

Порядок фиксирован спекой (зависимости: arch-first → движок → демонтаж →
адаптация; тесты E2E — после прод-части):

| Этап | Таск spec §12 | Содержимое | Коммит-единица |
|---|---|---|---|
| Э1 | (а) | arch-first правка `arch/21` (+проверка `arch/20`) | `arch(t20)` |
| Э2 | (б) | Р7-верификация entrypoint → движок env-TLS (BuildCmd/BuildNodeTlsEnv/IsValidNodeEnv, драйверы Env/NodeEnvAsync/демонтаж TLS-volume API, процессы V3/CaRotator-R/NodeSupervisor+легаси-чистка/X1/TlsMigrator) + юниты TDD | `feat(t20)` |
| Э3 | (в) | демонтаж helper-механики Shared.Docker + TarArchive (grep-гейт §4.3) + фейки (Valkey ×3, PgWorker ×6 — по фактическим grep-фактам, см. ниже, Shared.Docker.UnitTests) | `refactor(t20)` |
| Э4 | (г) | адаптация интеграций Valkey + 3 E2E lifecycle кейса + чек 51 + runbook | `test(t20)` + `docs(t20)` |
| Э5 | (д) | E2E Kafka: вычистка helper-комментария + контрольный прогон | `test(t20)` |
| Э6 | (е) | E2E Valkey takeover: перепись готового кейса под жертву-демонтаж (env-модель; I2 по построению + лог-факт; I1-бюджет ≤20 с; wait-clean ≤15 с) + прогон | `test(t20)` |
| Э7 | (ж) | мерж-гейт (spec §14) + roadmap-правка — ТОЛЬКО по приказу пользователя на мерже | мерж-коммит |

Коммиты делает координатор по готовности этапов; план фиксирует логические
коммит-единицы. Незакоммиченная тестовая база прошлой итерации входит в
коммит-единицы Э5/Э6 (файл + адаптация = одна единица).

## Global Constraints

- Работа ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t20-kfw-vwk-takeover-e2e-2` (ветка `feat-t20-kfw-vwk-takeover-e2e-2`); команды — из корня worktree.
- Базовые правила — `../../AGENTS.base.md` (прочитать до исполнения) и `../../AGENTS.md`: буква приказа, язык русский, AAA-тесты, зачистка серий, телеметрия E2E, dev-flow. Команды и субагенты — только в фоне (`run_in_background: true`), task_id фиксировать.
- **Границы spec §5 действуют целиком**: прод-правки только `src/ValkeyWorker.*` (TLS-механика §4) и `src/Shared.Docker` (демонтаж §4.3); `src/KafkaWorker.*`/`src/PgWorker.*`/`src/AdminPanel.*`/`src/Shared.{Core,Etcd,Tls,Metrics}` — только чтение (PgWorker-фейки — единственное исключение, синхронное удаление демонтируемых методов); продовые Dockerfile не трогаем; панель не трогаем; операции-жертвы E2E не меняем; никаких общих контуров E2E; никаких хардкод-портов; никаких sleep-поллингов; фильтр `~Takeover` НЕ использовать (цепляет 7 чужих тестов); бюджеты демонтажа не двигаем (15 с достигается устранением тома).
- .NET 10, `TreatWarningsAsErrors=true` → `dotnet build src/PgWorker.slnx -c Release` — 0 warnings; идентификаторы английские, комментарии/доки/логи русские; FluentAssertions; тесты — AAA-комментарии.
- E2E-каноны: guid-уникальность; полный teardown при любом исходе + ассерт чистоты; `MarkFailed()` → stop-без-удаления + `README-cleanup.txt`; docker-логи/inspect ДО удалений; `[PHASE]`/`[PHASE-TICK]` (тик 5 с, полл 500 мс); slow-phase >60 с — автосбор логов и отчёт; docker-CLI в тиках прогресса запрещён; перезапуск упавших тестов — ТОЛЬКО после полного анализа артефактов (`/tmp/pgw-e2e-artifacts-{runId}/`, docs/e2e-launch §4).
- Порты: только `FreePort()`-зонды / `FreePortWindow` (21000–31000, вне зоны стенда); никаких литералов.
- Любое ожидание агента/команд ≤30 с (AGENTS.base §12); фоновые прогоны тестов — по task_id, не блокирующе.
- TDD: к прод-части (б)–(г) — юниты `BuildCmd`/`BuildNodeTlsEnv`/`IsValidNodeEnv`/сверок пишутся ПЕРВЫМИ (spec §12 Ф3); к самим E2E TDD не применяется.

## Карта файлов

| Файл | Статус | Этап |
|---|---|---|
| `arch/21-valkeyworker.md` | Modify (§2 TLS-абзац, §5 A/B/C/T/K, §6, «Границы», §2-строки про helper-транспорт) | Э1 |
| `arch/20-valkey-clusters.md` | Read-only проверка (grep «volume» при доставке сертов — на момент плана пуст) | Э1 |
| `src/ValkeyWorker.Provisioning/Processes/NodeArgsBuilder.cs` | Modify (+`BuildCmd`) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/NodeTlsProvisioner.cs` | Modify (перепись на env: `BuildNodeTlsEnv`/`IsValidNodeEnv`; `EnsureNodeTlsAsync`/`IsValidTar` удаляются) | Э2 |
| `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs` | Modify (`ValkeyNodeSpec.TlsVolume`→`Env`; +`NodeEnvAsync`; удаление `EnsureTlsVolumeAsync`/`Put/GetTlsArchiveAsync`/`RemoveTlsVolumeAsync`/`TlsVolumeName`; +утилита легаси-чистки тома) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs` | Modify (V3: env-сверка, EnsureNode с env+cmd) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs` | Modify (R: факт-детект по env, пересоздание с env от `ca_next_*`) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/NodeSupervisor.cs` | Modify (env-сверка supervisable; RecreateAsync env+cmd; легаси-чистка `vwk-<C>-tls`) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/TlsMigrator.cs` | Modify (T2: EnsureNode с env; детект `--tls-port` по вхождению — ОБЕ точки: `NeedsMigration` :55 и T2-предчек `RunAsync` :118–121) | Э2 |
| `src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs` | Modify (X1: шаг remove-tls-volume удаляется) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeArgsBuilderTests.cs` | Modify (+`BuildCmd` TDD) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeTlsProvisionerTests.cs` | Modify (перепись на `BuildNodeTlsEnv`/`IsValidNodeEnv`) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs` | Modify (ассерт env от `ca_next_*` вместо `spec.TlsVolume`) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Provisioning/{ProvisioningProcess,NodeSupervisor,TlsMigrator,DeprovisioningProcess}Tests.cs` | Modify (cmd-обёртка в сверках, env-факты, легаси-чистка, X1 без тома) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` | Modify (`FakeDriver`: `TlsVolumes` → env-хранилище + `NodeEnvAsync`) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Docker/SwarmClusterDriverTlsTests.cs` | **Delete** (тестирует демонтируемую volume-механику) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverLimitsTests.cs` | Modify (фейк чистится от демонтируемых методов) | Э2 |
| `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverEnvTests.cs` | **Create** (env в spec Plain+Swarm, `NodeEnvAsync`-роутинг) | Э2 |
| `src/Shared.Docker/Engine/IDockerEngine.cs`, `DockerEngine.cs` | Modify (удаление `Put/GetVolumeArchiveAsync`, `CreateHelperAsync`/`ExecInHelperAsync`/`HelperName`) | Э3 |
| `src/Shared.Docker/Engine/TarArchive.cs` | **Delete** (по гейту §4.3) | Э3 |
| `src/tests/Shared.Docker.UnitTests/Engine/TarArchiveTests.cs` | **Delete** | Э3 |
| PgWorker-фейки ×6: `src/tests/PgWorker.UnitTests/Docker/{BackupJobsCleanerTests,ClusterDriverTests}.cs`, `src/tests/PgWorker.IntegrationTests/Backups/{FakeBackupEngine,BackupVerifyProcessTests,RestoreDrillProcessTests,BackupProcessTests}.cs` | Modify (удаление фейк-реализаций `Put/GetVolumeArchiveAsync`; поведение тестов не меняется) | Э3 |
| `src/tests/ValkeyWorker.IntegrationTests/Valkey/*.cs` (13 файлов интеграций + `ValkeyClusterFixture.cs`) | Modify (адаптация к env-модели по §9) | Э4 |
| `src/tests/ValkeyWorker.IntegrationTests/Api/MetricsDomainTests.cs` | Modify (teardown без `RemoveTlsVolumeAsync`) | Э4 |
| `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eLifecycleTests.cs` | Modify (args-ассерты → обёртка; ассерты отсутствия тома остаются) | Э4 |
| `dev-stand/adminpanel/checks/51-valkey-api.sh` | Modify (строки 159–170: убрать volume-ассерты) | Э4 |
| `docs/runbook.md` | Modify (§TLS-подключения, §Ротация CA; +абзац осиротевших томов) | Э4 |
| `src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eTakeoverTests.cs` | Modify (вычистка комментария про `vwk-tls-writer`, бюджет A6 60 с, ассерт-сообщение) | Э5 |
| `src/tests/KafkaWorker.IntegrationTests/E2e/{KafkaE2eEnvironment,KafkaE2ePhase}.cs` | Add (незакоммиченная база прошлой итерации; входят в коммит-единицу Э5) | Э5 |
| `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eTakeoverTests.cs` | Modify (жертва — демонтаж: Fact `Kill_ClaimHolderMidDeprovision_SurvivorFinishesNoDuplicates`; env-модель; пре-факт A3 «kill до X0»; I2 конструктивно + лог-факт `deprovision {C}`; I1-бюджет ≤20 с; wait-clean ≤15 с; без финального A6) | Э6 |
| `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs` | Modify (`StartTwoAsync`-база уже в рабочей копии; синхронизация volume-комментариев :427–428/:522–523; входит в коммит-единицу Э6) | Э6 |
| `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2ePhase.cs` | Add (незакоммиченная база; входит в коммит-единицу Э6) | Э6 |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | Modify — ТОЛЬКО мерж-коммит, по приказу (Э7) | Э7 |

Diff за пределами карты — пуст (проверка Э7). `deploy/docker-compose.yml` без
изменений (`vw-snapshots`/`vw-api-tls` — per-install тома API воркера, не
per-cluster TLS-тома).

## Контракт изменений (сигнатуры — задача повторяет)

```csharp
// NodeArgsBuilder (src/ValkeyWorker.Provisioning/Processes/NodeArgsBuilder.cs)
public static IReadOnlyList<string> Build(...)              // прежний канон args (вкл. "valkey-server") — НЕ меняется
public static IReadOnlyList<string> BuildCmd(IReadOnlyList<string> args)
    // ["sh","-c", раскатка + "; exec " + shell-escape(args)]
    // раскатка (spec §4.1, дословно):
    //   umask 077; mkdir -p /tls;
    //   printf %s "$VALKEY_TLS_CERT" > /tls/node.crt;
    //   printf %s "$VALKEY_TLS_KEY"  > /tls/node.key;
    //   printf %s "$VALKEY_TLS_CA"   > /tls/ca.pem;
    // экранирование: каждый arg → '…' с заменой внутренних ' на '\'' (пустой
    // arg --save '' → '', пароли [A-Za-z0-9] — без спецсимволов)

// NodeTlsProvisioner — полная замена тела (EnsureNodeTlsAsync/IsValidTar удаляются)
public static IReadOnlyDictionary<string, string> BuildNodeTlsEnv(
    string caPem, string caKeyPem, string node, string advertisedHost)
    // { VALKEY_TLS_CERT, VALKEY_TLS_KEY, VALKEY_TLS_CA } — свежий
    // ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, node, advertisedHost);
    // многострочный PEM как есть (переносы — в значении env)
internal static bool IsValidNodeEnv(
    IReadOnlyDictionary<string, string>? env, string advertisedHost, string caPem, TimeProvider clock)
    // эквивалент IsValidTar по тем же критериям (CA==ожидаемому Trim-сравнением,
    // цепочка валидна, key↔cert, NotAfter жив, SAN покрывает advertised
    // DNS|IP); env==null/нет ключа — false

// ValkeyNodeSpec (src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs)
public sealed record ValkeyNodeSpec(
    ..., IReadOnlyList<string> Args, decimal? CpuCores, long? MemoryBytes,
    IReadOnlyDictionary<string, string>? Env = null);   // TlsVolume УДАЛЁН
// Процессы передают Args = NodeArgsBuilder.BuildCmd(NodeArgsBuilder.Build(...))

// IClusterDriver — удаляются: EnsureTlsVolumeAsync, PutTlsArchiveAsync,
// GetTlsArchiveAsync, RemoveTlsVolumeAsync; TlsVolumeName удаляется.
// Добавляются:
Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
    string cluster, string nodeName, CancellationToken ct);
    // Plain: перебор хостов InspectContainerEnvAsync(vwk-<C>-node<k>) — первый найденный;
    // Swarm: InspectServiceEnvAsync; null = объекта нет
Task<Result> CleanupLegacyVolumeAsync(string cluster, CancellationToken ct);
    // легаси-чистка надзора: DeleteVolumeAsync($"vwk-{cluster}-tls") на ВСЕХ
    // engines (Plain — таблица хостов; Swarm — ноды + manager); 404 = успех;
    // 409 (in use) — НЕ фейлит тик надзора и НЕ пишется в warnings: попытка
    // повторяется следующим тиком безусловно (вызов — каждым тиком надзора,
    // пока том не уйдёт). Это НЕ X1-шаг демонтажа (тома в модели нет), а
    // утилита миграции живых кластеров старой модели.

// DockerEngine/IDockerEngine — удаляются: PutVolumeArchiveAsync,
// GetVolumeArchiveAsync, CreateHelperAsync, ExecInHelperAsync, HelperName.
// Остаются: EnsureVolumeAsync/DeleteVolumeAsync/VolumeExistsAsync/RemoveVolumeAsync
// (утилиты движка; VolumeExistsAsync используют E2E-окружения).
```

## Контракт env (spec §4.1 — канон после правки arch/21 §2)

| env | Значение |
|---|---|
| `VALKEY_TLS_CERT` | PEM серверного серта ноды (CN=`node<k>`, SAN advertised-хоста, подпись `ca_key`) |
| `VALKEY_TLS_KEY` | PEM приватного ключа ноды (PKCS#8, RSA-2048) |
| `VALKEY_TLS_CA` | PEM per-cluster CA (`ca_pem`; в окне ротации фазы R — NEW из staging) |

Файлы в контейнере: `/tls/node.crt`, `/tls/node.key`, `/tls/ca.pem` — ТЕ ЖЕ
имена/пути (канонические TLS-args не меняются). `/tls` — каталог в ФС
контейнера, не том. Uid: с обёрткой `$1 == sh` → gosu-ветка образного
entrypoint не срабатывает, valkey-server стартует от root; серты 0600 root
(umask 077); изоляция секрета — периметром контейнера (env и так виден из
`docker inspect`).

Идемпотентность по факту: свежий серт случаен → побайтовая сверка env
НЕВОЗМОЖНА; решение «пересоздать/пропустить» = `IsValidNodeEnv(...)` И
`Cmd == BuildCmd(канонический args)` И порт/лимиты совпадают → пропуск; иначе
RemoveNode + EnsureNode со свежим env (spec §4.1, риск Р9).

---

### Э1: arch-first — правка канона `arch/21` (+проверка `arch/20`) (таск (а) spec §12)

**Вход:** спека ревизии 3 одобрена; worktree чист (кроме
`docs/superpowers/…t20…/` и незакоммиченной тестовой базы прошлой итерации).

**Шаги:**

- [ ] Step 1: `arch/21-valkeyworker.md` — правки по spec §4.5 (описывать
  модель «как устроено», БЕЗ исторических пассажей «раньше было volume»):
  - §2 TLS-абзац (сейчас строки ~126–154): доставка сертов = PEM в env ноды
    (`VALKEY_TLS_{CERT,KEY,CA}`) + cmd-обёртка старта (раскатка в `/tls`,
    exec valkey-server с прежними args); volume/helper исключаются; «объекты
    домена: кластер = контейнер(ы) `vwk-<C>-node<k>`» (без volume); uid-примечание
    (root, 0600, периметр контейнера); ротация CA — фаза R: ca.pem в env = NEW.
  - §2 хвост (сейчас ~строки 184–189, «Клиент Engine API…»): убрать строки про
    TLS-volume-транспорт (helper-контейнер, tar-архив,
    `EnsureVolumeAsync`/`Put/GetVolumeArchiveAsync`/`DeleteVolumeAsync`-методы
    как TLS-механику); дополнить: env-инспекция
    (`InspectContainerEnvAsync`/`InspectServiceEnvAsync`), cmd-обёртка.
  - §5 A (V3): серт ноды — env в spec создания; сверка re-run: env валиден +
    Cmd каноничен + порт + лимиты → пропуск.
  - §5 B (X1): без remove-tls-volume — только rm контейнеров `vwk-<C>-*`.
  - §5 C (надзор): сверка env живой ноды против `ca_pem`; пересоздание при
    отсутствии/невалидности env; легаси-чистка осиротевшего `vwk-<C>-tls`
    старой модели (после успешной обработки нод, все ноды на env-модели,
    404/409 = успех/ретрай тиком).
  - §5 T (миграция): T2 — пересоздание с env (тома не касается; осиротевший
    легаси-том уберёт легаси-чистка).
  - §5 K (R): факт-детект «R завершён» — по env контейнера
    (`IsValidNodeEnv` против `ca_next_pem`); пересоздание — env от
    `ca_next_key`/`ca_next_pem`.
  - §6 (идемпотентность): «серт env валиден против текущего CA (в окне
    ротации — против staging `ca_next_pem`)» вместо «серт volume валиден».
  - «Границы» (сейчас строка 37): убрать «TLS-volume `vwk-<C>-tls` — секреты,
    не данные» (томов в модели нет).
- [ ] Step 2: `arch/20-valkey-clusters.md` — grep-проверка: `grep -n -i
  "volume\|helper\|tar" arch/20-valkey-clusters.md`. На момент написания плана
  пусто — правка НЕ требуется; если найдётся строка о доставке сертов через
  volume — править тем же arch-first коммитом (spec §4.5).
- [ ] Step 3: Сверка полноты: `grep -n -i "vwk-.*-tls\|volume\|helper\|vwk-tls-writer\|PutTlsArchive\|GetTlsArchive\|EnsureTlsVolume\|RemoveTlsVolume\|TarArchive" arch/21-valkeyworker.md`
  — не осталось строк, описывающих volume/helper как действующую модель
  (допустимо единственное упоминание легаси-тома в §5 C — легаси-чистка
  миграции). Исторических пассажей нет.
- [ ] Step 4: Коммит-единица `arch(t20): env-TLS модель доставки сертов нод Valkey — правка канона arch/21` (делает координатор).

**Выход:** канон arch/21 описывает env-модель; arch/20 чист.

**Проверка (критерий перехода на Э2):** шаг 3 чист; diff Э1 — только
`arch/21-valkeyworker.md` (+`arch/20` если потребовалось).

**Spec:** §4.1, §4.5, §12(а).

---

### Э2: Движок ValkeyWorker env-TLS + юниты (таск (б) spec §12)

**Вход:** Э1 закоммичен; docker жив; образ `192.168.0.1:5000/valkey/valkey:9.1.2`
доступен локально (нет — `dev-stand/images/pull-images.sh`, ранбук).

**Шаги:**

- [ ] Step 1 — **Р7-верификация entrypoint ПЕРВЫМ, до вписывания в процессы**
  (spec §4.1 «Точка верификации», §15 Р7). Ручной прогон обёртки на пине
  образа: серты openssl → `docker run` с env+cmd-обёрткой → PING по TLS.
  Проверяемые факты: (а) entrypoint при `$1 == sh` делает `exec "$@"` как
  есть — обёртка срабатывает ДО valkey-server; (б) раскатка создаёт читаемые
  root-серты; (в) valkey-server стартует от root с каноническими TLS-args;
  (г) RESP PING по TLS отвечает PONG. Команды (в фоне, свободный порт зоной):

  ```bash
  D=$(mktemp -d /tmp/t20-entrypoint-check-XXXX)
  # CA + серт ноды (SAN localhost), PKCS#8-ключ
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.pem" -days 2 -subj "/CN=t20-check-ca"
  openssl req -newkey rsa:2048 -nodes -keyout "$D/node.key" -out "$D/node.csr" -subj "/CN=node1"
  openssl x509 -req -in "$D/node.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key" -CAcreateserial -days 2 \
    -out "$D/node.crt" -extfile <(printf 'subjectAltName=DNS:localhost')
  P=<свободный порт — зонд, вне зоны стенда>
  docker run -d --name t20-entrypoint-check -p $P:6379 \
    -e VALKEY_TLS_CERT="$(cat $D/node.crt)" -e VALKEY_TLS_KEY="$(cat $D/node.key)" -e VALKEY_TLS_CA="$(cat $D/ca.pem)" \
    192.168.0.1:5000/valkey/valkey:9.1.2 \
    sh -c 'umask 077; mkdir -p /tls; printf %s "$VALKEY_TLS_CERT" > /tls/node.crt; printf %s "$VALKEY_TLS_KEY" > /tls/node.key; printf %s "$VALKEY_TLS_CA" > /tls/ca.pem; exec valkey-server --tls-port 6379 --port 0 --tls-cert-file /tls/node.crt --tls-key-file /tls/node.key --tls-ca-cert-file /tls/ca.pem --tls-auth-clients no --tls-replication no --user default on ~* +@all'
  docker exec t20-entrypoint-check valkey-cli --tls --cacert /tls/ca.pem -h 127.0.0.1 -p 6379 PING   # → PONG
  docker exec t20-entrypoint-check ls -l /tls        # 0600 root (umask 077)
  docker inspect t20-entrypoint-check --format '{{.Path}} {{.Args}}'   # фиксация фактического entrypoint-поведения
  ```

  Зачистка: `docker rm -f t20-entrypoint-check; rm -rf "$D"`.
  **Расхождение с §4.1 (обёртка не работает / entrypoint ведёт себя иначе) —
  СТОП и пересогласование обёртки, в процессы НЕ вписывать** (СТОП-условие 3).
  Результат (факт entrypoint, тайминги) — в журнал этапа.

- [ ] Step 2 — TDD юниты (сначала тесты, красные):
  - `NodeArgsBuilderTests`: `BuildCmd` — канонический args → `["sh","-c",
    "<раскатка>; exec valkey-server …"]`; детерминизм (одинаковый args →
    побитово равная строка); экранирование: пустой аргумент `--save ''`
    сохраняется как `--save ''`; пароль без спецсимволов не искажается;
    обратный парсинг: строка обёртки содержит все флаги канона
    (`--tls-cert-file /tls/node.crt` и т.д.) в исходном порядке.
  - `NodeTlsProvisionerTests` (перепись): `BuildNodeTlsEnv` — три ключа
    `VALKEY_TLS_{CERT,KEY,CA}`; `ca` == входному; серт валиден против CA,
    SAN покрывает advertised (DNS и IP), key↔cert; `IsValidNodeEnv`:
    валидный env → true; `null`/нет ключа → false; чужой CA → false;
    истёкший NotAfter → false; key≠cert → false; SAN-drift → false.
- [ ] Step 3 — `NodeArgsBuilder.BuildCmd` (контракт выше). `Build` не меняется.
- [ ] Step 4 — `NodeTlsProvisioner` (перепись): `EnsureNodeTlsAsync`/
  `IsValidTar`/TarArchive-код удаляются; `BuildNodeTlsEnv` (свежий
  `ValkeyPki.IssueNodeCertificate`) + `IsValidNodeEnv` (порт критериев
  `IsValidTar`, источник — env; нормализация `\n`→переносы на границе
  сверки, Trim-сравнение CA).
- [ ] Step 5 — драйверы `ClusterDriver.cs`:
  - `ValkeyNodeSpec`: `TlsVolume` → `Env` (контракт выше).
  - `PlainClusterDriver.EnsureNodeAsync`: `ContainerSpec` получает
    `Env: spec.Env`, `Cmd: spec.Args` (теперь = обёртка), `Binds` — удалить
    (TLS-биндов больше нет). `SwarmClusterDriver.EnsureNodeAsync` — то же в
    `ServiceSpec.Template`.
  - `NodeEnvAsync`: Plain — перебор хостов `InspectContainerEnvAsync`
    (первый найденный); Swarm — `InspectServiceEnvAsync`; null = объекта нет.
  - `CleanupLegacyVolumeAsync`: перебор engines (Plain — все; Swarm — ноды +
    manager), `DeleteVolumeAsync($"vwk-{cluster}-tls")`, 404 = успех, 409 →
    Failed (ретрай тиком надзора).
  - Удалить: `EnsureTlsVolumeAsync`, `PutTlsArchiveAsync`,
    `GetTlsArchiveAsync`, `RemoveTlsVolumeAsync`, `TlsVolumeName` (AC1).
- [ ] Step 6 — процессы:
  - `ProvisioningProcess` (V3, `EnsureContainersAsync`): убрать
    `EnsureNodeTlsAsync`-вызов; env = `NodeTlsProvisioner.BuildNodeTlsEnv(
    creds.CaPem, creds.CaKey, node, advertised)`; cmd =
    `NodeArgsBuilder.BuildCmd(args)`; сверка re-run: существующий объект —
    `existingCmd.SequenceEqual(cmd)` И `IsValidNodeEnv(await
    driver.NodeEnvAsync(...), advertised, creds.CaPem, _clock)` И порт И
    лимиты → пропуск; иначе/нет — Remove+Ensure c `Env` (проверка env — после
    дешёвых сверок Cmd/порта/лимитов, только при совпадении прочих).
  - `CaRotator` (R, `ReplayNodeCommitAsync`): R.1 факт-детект —
    `driver.NodeEnvAsync(cluster, "node1")` → `IsValidNodeEnv(env,
    advertised, nextPem, _clock)` → CommitAsync; R.3 пересоздание: env =
    `BuildNodeTlsEnv(nextPem, nextKey, "node1", advertised)` (ca.pem в env =
    NEW, НЕ bundle), cmd = `BuildCmd(args)`, `RemoveNode` → `EnsureNode` с
    `Env`. Фазы P/D/C не меняются.
  - `NodeSupervisor` (`TickAsync`/`RecreateAsync`): для supervisable-ноды с
    живым docker-объектом — сверка `IsValidNodeEnv(await NodeEnvAsync(...),
    advertised, snap.CaPem, _clock)`; отсутствие/невалидность → пересоздание
    с env+cmd (в рамках «одно пересоздание за тик», warning). `RecreateAsync`:
    env = `BuildNodeTlsEnv(snap.CaPem!, snap.CaKey!, node, advertised)`,
    cmd = `BuildCmd(args)`. Легаси-чистка: после успешной обработки нд, если
    ВСЕ ноды кластера на env-модели — `CleanupLegacyVolumeAsync(cluster, ct)`
    (404 = успех; 409 — НЕ фейлит тик и НЕ пишется в warnings: безусловный
    ретрай следующим тиком).
  - `TlsMigrator` (T2, `RecreateNodeAsync`): `EnsureNodeTlsAsync`-вызов
    удалить; env+cmd при Ensure. **Детект `--tls-port` — ОБЕ точки точного
    сравнения** меняются на вхождение подстроки в элементы args (args живого
    контейнера — теперь обёртка `["sh","-c","…--tls-port…"]`, точное
    равенство элемента всегда false → бесконечное пересоздание):
    (а) `NeedsMigration` (`TlsMigrator.cs:55`); (б) T2-предчек в `RunAsync`
    (`TlsMigrator.cs:118–121`, `!args.Contains("--tls-port")`).
  - `DeprovisioningProcess` (X1): шаг `RemoveTlsVolumeAsync` удалить; X1 =
    только rm контейнеров `vwk-<C>-*` (перечисление `ListNodeObjectsAsync` +
    `RemoveNodeAsync`); остальное (X2/X3) без изменений.
- [ ] Step 7 — юниты процессов и фейки (продолжение TDD, к зелёному):
  - `Fakes.cs` `FakeDriver`: `TlsVolumes`-хранилище → env-хранилище
    (запись env при EnsureNodeAsync из `spec.Env`; `NodeEnvAsync` отдаёт env
    живого контейнера, null — нет объекта; `CleanupLegacyVolumeAsync` на
    in-memory множестве «легаси-томов» — для юнита AC2). Драйверные
    TLS-volume-методы фейка удалить.
  - `ProvisioningProcessTests`/`CaRotatorTests`/`NodeSupervisorTests`/
    `TlsMigratorTests`/`DeprovisioningProcessTests`: ассерты `spec.TlsVolume`
    → `spec.Env` (`CaRotatorTests`:429 — env от `ca_next_*`: `VALKEY_TLS_CA`
    == `ca_next_pem`); сверка Cmd — на обёртку; кейс «валидный env +
    канонический Cmd → пересоздания НЕТ» (Р9); NodeSupervisor: кейсы
    «env отсутствует/невалиден → пересоздание с env» и «легаси-том
    `vwk-<C>-tls` удалён легаси-чисткой после перевода всех нод на env»
    (AC2); Deprovisioning: X1 без remove-tls-volume, чистота домена.
  - `SwarmClusterDriverTlsTests.cs` — DELETE. `ClusterDriverLimitsTests.cs` —
    фейк чистится. `ClusterDriverEnvTests.cs` — CREATE: env в `ContainerSpec`
    (Plain: CreateContainerAsync получает Env; Swarm: ServiceSpec.Template) +
    `NodeEnvAsync`-роутинг (фейк движка запоминает env контейнера/сервиса).
  - Минимальные компиляционные правки интеграций Valkey (полная адаптация —
    Э4): `ValkeyClusterFixture.NewTlsProvisioner` → фабрика env-набора;
    вызовы `RemoveTlsVolumeAsync`/`EnsureNodeTlsAsync` в интеграциях —
    убрать/заменить стабами компиляции; в `ProvisioningTests.cs` (:94–96) и
    `TlsMigrationTests.cs` (:73–75) убрать вызовы
    `fx.Driver.GetTlsArchiveAsync(...)` + `TarArchive.Read(...)` — без этого
    удаление TarArchive в Э3 ломает сборку (ассерты этих мест переходят на
    env-форму в Э4). Интеграционные ассерты НЕ переботываются (этап Э4).
- [ ] Step 8: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 9: юниты (без docker):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.UnitTests"`
  — зелёный (в фоне; task_id; ждать финальной строки).
- [ ] Step 10: Зачистки docker не требует (юниты без контейнеров); остаток
  Р7-шага уже зачищен (шаг 1). Артефактов серий нет.
- [ ] Step 11: Коммит-единица `feat(t20): env-TLS ValkeyWorker — BuildCmd/BuildNodeTlsEnv/IsValidNodeEnv, драйверы Env+NodeEnvAsync, процессы V3/R/C/T/X1, юниты`.

**Выход:** прод-движок на env-модели; TLS-volume API драйвера отсутствуют
(кроме утилиты легаси-чистки); юниты зелёные; факт entrypoint зафиксирован.

**Проверка (критерий перехода на Э3):** шаги 8–9 зелёные; `grep -rn
"EnsureTlsVolumeAsync\|PutTlsArchiveAsync\|GetTlsArchiveAsync\|RemoveTlsVolumeAsync\|TlsVolumeName" src/ValkeyWorker.* src/tests/ValkeyWorker.* --include="*.cs" | grep -v obj/` — пусто; сверка V3/надзора идёт по env+Cmd.

**Spec:** §4.1, §4.2, §12(б), §15 (Р7–Р9), §13 AC1/AC2 (юнит-часть).

---

### Э3: Демонтаж helper-механики Shared.Docker + фейки (таск (в) spec §12)

**Вход:** Э2 закоммичен (драйверы Valkey больше не зовут helper-механику).

**Шаги:**

- [ ] Step 1 — **grep-гейт §4.3 (обязателен перед удалением)**:
  `grep -rn "PutVolumeArchiveAsync\|GetVolumeArchiveAsync" src --include="*.cs" | grep -v obj/`
  Прод-потребители вне `src/Shared.Docker/Engine` — ДОЛЖНЫ ОТСУТСТВОВАТЬ
  (после Э2 `ValkeyWorker.Docker/Drivers/ClusterDriver.cs` их не содержит;
  остаются только тестовые фейки). Найден скрытый прод-потребитель — **СТОП и
  анализ с пользователем, демонтаж НЕ делать** (СТОП-условие 2, Р10).
  Дополнительно TarArchive-гейт: `grep -rn "TarArchive" src --include="*.cs" | grep -v obj/`
  — потребители после Э2: только `Shared.Docker/Engine/{DockerEngine,TarArchive}.cs`
  и `src/tests/Shared.Docker.UnitTests/Engine/TarArchiveTests.cs`; вызовы
  `TarArchive.Read` в интеграциях Valkey (`ProvisioningTests`/`TlsMigrationTests`)
  убираются Э2 Step 7, юниты Valkey чисты после Э2, PgWorker чист. Иной
  прод-потребитель — TarArchive остаётся, зафиксировать в плане/резюме (§17).
- [ ] Step 2: `src/Shared.Docker/Engine/IDockerEngine.cs` — удалить
  `PutVolumeArchiveAsync`, `GetVolumeArchiveAsync`.
- [ ] Step 3: `src/Shared.Docker/Engine/DockerEngine.cs` — удалить
  `PutVolumeArchiveAsync` (строки ~274–311), `GetVolumeArchiveAsync`
  (~317–351), приватные `CreateHelperAsync`/`ExecInHelperAsync`/`HelperName`
  (~1127–1179) и комментарии helper-секции. Volume-утилиты
  (`EnsureVolumeAsync`/`DeleteVolumeAsync`/`VolumeExistsAsync`/
  `RemoveVolumeAsync`) остаются.
- [ ] Step 4: `src/Shared.Docker/Engine/TarArchive.cs` — DELETE (гейт шага 1
  пройден). `src/tests/Shared.Docker.UnitTests/Engine/TarArchiveTests.cs` —
  DELETE.
- [ ] Step 5: чистка фейк-реализаций демонтируемых методов (поведение тестов
  не меняется):
  - PgWorker ×6: `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs`,
    `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`,
    `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs`,
    `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`,
    `src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs`,
    `src/tests/PgWorker.IntegrationTests/Backups/BackupProcessTests.cs`.
  - Valkey: фейки `IDockerEngine` в `SwarmClusterDriverTlsTests` уже удалены
    (Э2), `ClusterDriverLimitsTests`/новый `ClusterDriverEnvTests` — без
    демонтируемых методов.
- [ ] Step 6: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 7: юниты (без docker), две серии:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.UnitTests"`
  затем `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~PgWorker.UnitTests"`
  — обе зелёные (в фоне, по одной, финальная строка каждой).
- [ ] Step 8: Зачистки docker не требует. Контрольная: `docker ps -a --filter name=vwk-tls-writer --format '{{.Names}}'` → пусто (helper'ов не осталось; осиротевшие от старых прогонов — удалить руками, они вне модели).
- [ ] Step 9: Коммит-единица `refactor(t20): демонтаж helper-механики Shared.Docker — Put/GetVolumeArchive, CreateHelper, TarArchive; чистка фейков`.

**Выход:** в `IDockerEngine`/`DockerEngine` нет helper-механики и TarArchive;
AC1 (grep-часть) выполнен; юниты обоих проектов зелёные.

**Проверка (критерий перехода на Э4):** шаги 6–7 зелёные; `grep -rn
"PutVolumeArchiveAsync\|GetVolumeArchiveAsync\|CreateHelperAsync\|TarArchive" src --include="*.cs" | grep -v obj/` — пусто.

**Spec:** §4.3, §12(в), §15 (Р10), §13 AC1.

---

### Э4: Адаптация интеграций Valkey + lifecycle E2E + чек 51 + runbook (таск (г) spec §12)

**Вход:** Э3 закоммичен; docker жив.

**Шаги:**

- [ ] Step 1: интеграции `src/tests/ValkeyWorker.IntegrationTests/Valkey/` —
  поведенческая адаптация к env-модели (spec §9), все обязаны быть зелёными:
  - `ValkeyClusterFixture`: teardown — убрать `RemoveTlsVolumeAsync`-шаг;
    `NewTlsProvisioner()` → фабрика env-набора (`NodeTlsProvisioner`
    статического использования или локальный хелпер); комментарий
    «TLS-volume» переписать под env.
  - `ProvisioningTests`: args-ассерт `args[0]=="valkey-server"` → обёртка
    (`args[0]=="sh"`, `args[1]=="-c"`, строка содержит `exec valkey-server` +
    канонические флаги); доп-ассерт env контейнера `VALKEY_TLS_{CERT,KEY,CA}`
    (через `docker inspect --format` или драйверный `NodeEnvAsync`).
  - `DeprovisioningTests`: убрать сверку «TLS-volume удалён»; доп-ассерт
    «нет env/объектов, чистота домена»; шаг remove-tls-volume исчез.
  - `TlsMigrationTests`: премиграционный контейнер — старый канон args БЕЗ
    TLS-хвоста и БЕЗ env; после миграции — env + обёртка.
  - `CaRotationTests`: R-факт по env; NEW `VALKEY_TLS_CA` в env ноды.
  - `SupervisionTests`: env-сверка надзора; пересоздание «env
    отсутствует/невалиден»; легаси-чистка тома (юнит-покрытие уже в Э2 —
    здесь интеграционная проверка при наличии).
  - `ConvergeTests`, `AclMatrixTests`, `ResourcesAutorecreateTests`,
    `RotationTests`, `RotationTimeoutTests`, `MetricsCollectorTests` —
    точечные правки (фикстура/teardown без `RemoveTlsVolumeAsync`).
- [ ] Step 2: `src/tests/ValkeyWorker.IntegrationTests/Api/MetricsDomainTests.cs`
  — teardown без `RemoveTlsVolumeAsync` (строка ~163).
- [ ] Step 3: E2E lifecycle `ValkeyE2eLifecycleTests.cs` (3 кейса): args =
  обёртка с прежними путями `/tls/node.crt` и флагами `--tls-*` — ассерты
  `Contain("--tls-cert-file")` остаются валидны как вхождения в строку
  обёртки; кавычечные ассерты вида `"\"6379\""` — заменить на форму обёртки;
  ассерты «ни тома `vwk-<C>-tls`» остаются валидны (том не создаётся);
  комментарии — env-модель, без helper-упоминаний.
- [ ] Step 4: `dev-stand/adminpanel/checks/51-valkey-api.sh` (строки 159–170):
  убрать `docker volume inspect vwk-$TAG-tls` из wait-цикла чистоты и ассерт
  «TLS-volume не удалён»; комментарий «(t06-ревью)» про TLS-volume —
  переписать (ассерт чистоты — контейнер/ключи удалены; проверка отсутствия
  тома не нужна). Остальные чеки том не упоминают.
- [ ] Step 5: `docs/runbook.md`: §«TLS-подключения к Valkey» (строка ~298:
  «volume `vwk-<C>-tls`» → «сертификаты доставляются env ноды, перевыпуск —
  пересозданием контейнера») и §«Ротация CA» (строка ~310: «(R) пересоздание
  node1 с сертом от NEW (env ноды)»). Добавить абзац: осиротевшие
  `vwk-*-tls` старой модели (демонтаж до миграции) — удаляются вручную
  `docker volume rm`; живые кластеры чистит воркер сам (легаси-чистка надзора).
- [ ] Step 6: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 7: docker-серия (в фоне; ждать финальной строки):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests&FullyQualifiedName!~Takeover"`
  — интеграции + 3 lifecycle кейса зелёные (takeover-кейс исключён: его
  адаптация — Э6; полный фильтр — мерж-гейт Э7).
- [ ] Step 8: Зачистка серии (после финальной строки): тег серии `{tag}` (из
  лога прогона/`README-cleanup.txt`; входит во ВСЕ имена серии — кластеры,
  контейнеры, etcd-контейнеры фикстур). Own-only — голые префиксы `vwk-` НЕ
  использовать (жив dev-стенд со своими `vwk-<C>-node1`):
  `docker ps -a --filter name={tag} --format '{{.Names}}'` → пусто;
  `docker volume ls -q | grep -c {tag}` → 0; per-cluster сетей у Valkey нет.
  Артефакты `/tmp/pgw-e2e-artifacts-<guid>/` — фазы >60 с объяснить отчётом
  по логам.
- [ ] Step 9: Коммит-единицы `test(t20): адаптация интеграций Valkey и lifecycle E2E к env-модели` + `docs(t20): чек 51 и runbook — env-модель TLS, ручная чистка осиротевших томов`.

**Выход:** интеграции/lifecycle зелёные на env-модели; чек и runbook
синхронны модели.

**Проверка (критерий перехода на Э5):** шаги 6–7 зелёные; в diff нет
`RemoveTlsVolumeAsync`-вызовов и helper-упоминаний в `src/tests/ValkeyWorker.*`.

**Spec:** §9, §10, §12(г).

---

### Э5: E2E Kafka — вычистка комментария + контрольный прогон (таск (д) spec §12)

**Вход:** Э4 закоммичен; docker жив; тёплые образы (`kafkaworker:e2e`,
`apache/kafka` из локального registry; недоступен —
`dev-stand/images/pull-images.sh`).

Тестовая база Kafka УЖЕ написана и зелёная (рабочая копия); прод-часть Kafka
не менялась — правки только в фазе A6 (комментарий + бюджет, шаг 1).

**Шаги:**

- [ ] Step 1: `src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eTakeoverTests.cs`
  — две правки фазы A6:
  1) вычистить упоминание `vwk-tls-writer` из комментария (строки ~203–206:
     «движковый helper `vwk-tls-writer-*` … ретраит volume-rm тиками…»):
     домен Kafka helper-механику не использует (spec §7: «хвостов», держащих
     объекты после kill воркера, модель не порождает; серты — через env);
     комментарий — про демонтаж выжившим (контейнеры/portalloc/сеть);
  2) бюджет фазы A6 демонтажа 180 с → **60 с** — буква спеки §7 A6; 180 с
     обосновывались helper-хвостом, которого у KafkaWorker нет;
  3) синхронизировать текст ассерт-сообщения фазы A6 (строка ~231: «за 180 с»
     → «за 60 с») с новым бюджетом.
  Больше в Kafka-части ничего не трогать.
- [ ] Step 2: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 3: контрольный прогон (в фоне; ждать финальной строки):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~KafkaE2eTakeoverTests"`
  — зелёный (~3–5 мин тёплое, spec §16); убедиться, что фактический демонтаж
  A6 укладывается в ≤60 с **с запасом** (факт прошлого зелёного прогона —
  17.3 с; близко к 60 с — разбираться по логам, не ослаблять бюджет).
- [ ] Step 4: Зачистка серии: own-only по тегу `{tag}` (воркеры/etcd/сеть —
  через runId, брокеры и `kfw-net-{C}` — через имя кластера `tkw{tag}`):
  `docker ps -a --filter name={tag} --format '{{.Names}}'` → пусто;
  `docker network ls | grep kfw-net-tkw` — остатки анализируются ТОЛЬКО при
  нулевом числе живых контуров серии; при осиротевших —
  `docker network prune -f` (страховочный гейт уровня хоста).
- [ ] Step 5: Коммит-единица `test(t20): Kafka takeover E2E — kill держателя клэйма посреди provisioning (I1–I7); A6 без helper-хвоста, бюджет 60 с`.

**Выход:** зелёный кейс K (AC3) с бюджетом A6 60 с, сети `kfw-net-tkw*` не копятся.

**Проверка (критерий перехода на Э6):** шаг 3 зелёный; демонтаж A6 ≤60 с с запасом; доказательность kill
(резолв + дамп ассерта «клэйм существует») в журнале прогона; после зачистки
ни контейнеров тега, ни сетей `kfw-net-tkw*`.

**Spec:** §6, §7, §12(д), §13 AC3.

---

### Э6: E2E Valkey takeover — кейс «kill держателя посреди демонтажа» (таск (е) spec §12)

**Вход:** Э5 закоммичен; docker жив; тёплый образ `valkeyworker:e2e` (сборка
из кода Э2–Э4 — образ пересоберётся автоматически, `[PHASE]`-тайминг
контролировать).

**Контекст:** готовый кейс прошлой итерации написан на жертве-ротации —
ротация ОТМЕНЕНА решением пользователя (спека §8/§15-Р2: в env-модели ~0,2 с
— окно kill исчезло; диагностический прогон `~/pgw-diag-t20/2026-10-09-182646`).
Кейс переписывается под жертву-демонтаж (X0–X3); инфраструктура
(`StartTwoAsync`, `ValkeyE2ePhase`, резолв держателя A4, хелперы) —
переиспользуется как есть. Сверенные факты X-фаз (спека §8): DELETE → 202
ставит `config.state=TO_REMOVE` синхронно; X0 `work/{C}` c
`op=deprovision`; X1 rm контейнера; X2 del домена + координации; journal
`done` у демонтажа НЕ существует — терминальные факты: пустой домен + пустая
координация + нет контейнеров. **I2 — по построению + документальный лог**
(решение пользователя по Р2, 2026-10-09): survivor после re-захвата клэйма
проходит X0→X2 одним тиком <0.5 с (доказано прогоном `e055e1e0`) —
journal-полл (полл 500 мс) неприменим; якорь гарантирует kill ДО X0 жертвы,
инвариант доказывается конструктивной цепью + подстрокой `deprovision {C}`
в логах survivor.

**Шаги (правки в `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eTakeoverTests.cs`):**

- [ ] Step 1 — Fact переименовать в
  `Kill_ClaimHolderMidDeprovision_SurvivorFinishesNoDuplicates`; комментарий
  кейса — env-модель («сертификаты нод — env, TLS-тома нет»; жертва —
  демонтаж); упоминаний helper/vwk-tls-writer — нет (§6.3).
- [ ] Step 2 — A1 (без изменений): `StartTwoAsync` → готовность ОБОИХ
  (`Range("/valkeyworker/api/")` = 2 И `instances/` = 2), бюджет ≤100 с;
  тик `api={n}, instances={n}`.
- [ ] Step 3 — A2 Arrange-кластер [wait-worker ≤30 c; wait-provision ≤100 c]:
  mTLS-клиент ew1 → `POST /api/valkey/clusters` (name=`tvw{fx.ClusterTag}`,
  nodes=1, defaults) → 201 → `nodes/node1/state=RUNNING` + контейнер
  `vwk-{C}-node1` жив. Доп-проверка env-модели: env контейнера содержит
  `VALKEY_TLS_{CERT,KEY,CA}` (`docker inspect`, вне тиков). Датаплейн-проверка
  (I5-часть): RESP `PING` admin-кредом по TLS (endpoints + `ca_pem` + кред
  из etcd) → `PONG`.
- [ ] Step 4 — A3 Act-заявка + якорь «посередине» [DELETE сразу; якорь ≤30 c]:
  1) `DELETE /api/valkey/clusters/{C}` → 202 — mTLS-клиент ЛЮБОГО из ew1/ew2
     (демонтаж ведёт держатель клэйма — адресат заявки не важен);
  2) якорь — ВСЕ условия одновременно (kill гарантированно внутри демонтажа;
     «мимо» — демонтаж уже завершён — исключено условием а):
     а) config `/valkey/clusters/{C}/config` существует и содержит
        `"state":"TO_REMOVE"` (живёт от DELETE-202 до X2 — почти весь
        демонтаж);
     б) клэйм `/valkeyworker/claims/{C}` существует;
     в) демонтаж в полёте: контейнер `vwk-{C}-node1` существует (`docker ps` —
        в теле условия, docker-CLI в тиках запрещён) ИЛИ `work/{C}` существует
        с `op=deprovision` (ставится в X0, живёт до X2);
     г) **пре-факт для I2-инварианта**: `work/{C}` отсутствует ИЛИ его
        `op` != `deprovision` — X0 жертвы НЕ начат (жертва убита до своей
        journal-записи; ассерт на момент kill — провал с дампом
        claims/work/config).
     Покрытие окна непрерывно: до X0 — config+клэйм+контейнер; X0..X1 — +work;
     X1..X2 — config+клэйм+work. Тик: config.state + claims + наличие work
     (etcd-срез).
- [ ] Step 5 — A4 резолв держателя и kill (прежний; префикс `/valkeyworker`,
  контейнеры `vwk-ew{1,2}-{runId}`): ассерт «клэйм существует» (перечитать) →
  `claims/{C}`.instance → `/valkeyworker/api/{instance}`.url → порт → ровно
  одно совпадение с `fx.Api1Port`/`fx.Api2Port` (иначе FAIL с дампом, Р3) →
  `docker kill` держателя (без рестарта).
- [ ] Step 6 — A5 Assert-takeover (I2 — по построению + документальный лог;
  journal-полл «дождаться work op=deprovision instance=survivor» НЕ
  применяется: survivor проходит X0→X2 одним тиком <0.5 с < полла 500 мс —
  доказано прогоном `e055e1e0`, решение пользователя 2026-10-09):
  - **I1-фаза** [≤20 с; тик: число api/instances-ключей]:
    `Range("/valkeyworker/api/")` = 1 И `instances/` = 1 — ассерт С БЮДЖЕТОМ
    ожидания **≤20 с** (дискавери мертвеца гаснет с его lease ≤15 с —
    мгновенный ассерт неверен, §2 I1); единственный выживший ключ — survivor
    (по факту резолва A4). К концу фазы клэйм пере-захвачен survivor'ом
    (TTL ≤15 с истёк в этом же окне);
  - **терминал** [wait-clean ≤15 с; тик: наличие config/work, домен-ключи]:
    контейнеров `vwk-{C}-node1` ровно 0 (вкл. stopped), префикс
    `/valkey/clusters/{C}/` пуст, координация пуста — `work`, `portalloc`,
    `rotations`, `ca_rotations`, `ticket_outcomes`, `claims` по `<C>` сняты;
    survivor после re-захвата проходит X0→X2 одним тиком — превышение 15 с
    без объяснения по логам — фейл фазы, разбор по телеметрии;
  - **документальный лог-факт** (I2-след; ОДНОКРАТНО после терминала,
    docker-CLI вне тиков): `docker logs <survivorName>` содержит подстроку
    `deprovision {C}` — имя кластера guid-уникально, жертва убита до X0
    (пре-факт A3-г) → строка могла прийти только от survivor;
  - **I3**: `node1` ровно 0 (в терминале); helper-контейнеров и TLS-томов
    префикса нет (env-модель; docker-проверки — вне тиков);
  - **I4**: `portalloc/{C}` снят (входит в терминальную чистоту).
  Отдельного финального демонтажа (бывший A6) НЕТ — жертва и есть демонтаж:
  домен чист средствами survivor'а (§8); из тела кейса убрать прежний
  финальный DELETE/бюджет 180 с/helper-комментарии (строки ~208–235 старого
  кейса).
- [ ] Step 7 — A7 Teardown: `DisposeAsync` (оба воркера + etcd + сеть +
  ассерт чистоты по тегу).
- [ ] Step 8: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 9: прогон (в фоне, таймаут 600000 мс; ждать финальной строки) —
  **TMPDIR вне /tmp** (факт: внешняя чистка /tmp съедает артефакты;
  `ArtifactsDir` окружения строится от `Path.GetTempPath()` → попадает в
  TMPDIR):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 TMPDIR=$HOME/pgw-diag-t20/ dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eTakeoverTests"`
  — зелёный (~2–3,5 мин тёплое, spec §16). Нестабильность якоря A3 (окно не
  поймано / пре-факт нарушен) — фейл с дампом, разбор по артефактам и
  перезапуск ТОЛЬКО после анализа (Р2-блокер снят решением пользователя
  2026-10-09: окно X-фаз < полла — journal-полл заменён конструктивным
  доказательством I2).
- [ ] Step 10: Зачистка серии: own-only по тегу `{tag}` (воркеры/etcd —
  runId, кластер — `tvw{tag}`): `docker ps -a --filter name={tag} --format
  '{{.Names}}'` → пусто; `docker volume ls -q | grep -c {tag}` → 0;
  per-cluster сетей нет. Артефакты серии — `~/pgw-diag-t20/pgw-e2e-artifacts-{runId}/`
  (фазы >60 с — отчёт «почему долго»).
- [ ] Step 11: Коммит-единица `test(t20): Valkey takeover E2E — kill держателя клэйма посреди демонтажа, survivor доигрывает (I1–I7, wait-clean ≤15 с)`.
  Дополнительно (тем же коммитом) `ValkeyE2eEnvironment.cs`: синхронизировать
  устаревшие volume-комментарии (строки ~427–428 «чистота X1 (t06)»,
  ~522–523 «тома `vwk-<C>-tls` — X1 демонтажа») с env-моделью; хелпер
  `VolumeExistsAsync` ОСТАЕТСЯ (используется как проверка отсутствия тома).

**Выход:** зелёный кейс V (AC4) на жертве-демонтаже: I2 доказан конструктивной
цепью + лог-фактом `deprovision {C}` в логах survivor; терминальная чистота
домена/координации; wait-clean ≤15 с; I1 с бюджетом ≤20 с.

**Проверка (критерий перехода на Э7):** шаг 9 зелёный; доказательность kill
(резолв + дамп ассерта «клэйм существует» + пре-факт A3-г «X0 жертвы не
начат»); **I2-инвариант** = конструктивная цепь (kill-до-X0 + kill без
рестарта + единственность survivor по I1 + терминальная чистота) И
лог-факт `deprovision {C}` в логах survivor; wait-clean ≤15 с в
`[PHASE]`-строке; артефакты в `~/pgw-diag-t20/pgw-e2e-artifacts-{runId}/`.

**Spec:** §8 (сценарий V — демонтаж, сверенные факты X-фаз), §2 (I1-бюджет
≤20 с, I2/I3/I5/I6 V-форма), §6.3 (имя кейса), §12(е), §13 AC4, §15 (Р2),
§16 (оценка 2–3,5 мин).

---

### Э7: Мерж-гейт (spec §14) + roadmap-правка (таск (ж); roadmap — ТОЛЬКО по приказу пользователя на мерже)

**Вход:** Э1–Э6 закоммичены; ветка содержит только правки из карты файлов.

**Шаги (execute — прогон мерж-гейта):**

- [ ] Step 1: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 errors, 0 warnings.
- [ ] Step 2 — юниты (без docker; раздельные серии, каждая до финальной
  строки):
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.UnitTests"`
  `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~PgWorker.UnitTests"`
- [ ] Step 3 — интеграции Valkey ПОЛНЫЙ фильтр (docker; вкл. lifecycle и
  takeover): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"`
  → финальная строка → зачистка серии (Э4 Step 8).
- [ ] Step 4 — обе takeover-серии точными именами (раздельно, между ними —
  зачистка):
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~KafkaE2eTakeoverTests"`
  → финальная строка → зачистка + контроль `docker network ls | grep kfw-net-tkw` (остатки — только при нулевом числе живых контуров серии) →
  `docker network prune -f` при осиротевших.
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eTakeoverTests"`
  → финальная строка → зачистка серии.
- [ ] Step 5 — Shared.Docker тронут → кейс-маркер PgWorker docker-E2E на
  свежем Release: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
  → финальная строка → зачистка серии.
- [ ] Step 6 — сверка AC по протоколам прогонов (spec §13):
  - AC1: grep `src` не находит прод-потребителей демонтированного
    (`EnsureTlsVolumeAsync|Put/GetTlsArchiveAsync|RemoveTlsVolumeAsync|Put/GetVolumeArchiveAsync|CreateHelperAsync|TlsVolumeName`);
    `arch/21` описывает env-модель.
  - AC2: юнит/интеграция миграции живого кластера (пересоздание с env ≤2
    тиков + легаси-чистка тома) — покрыто Э2/Э4.
  - AC3/AC4: шаги 4 зелёные, kill доказателен, I1–I7, демонтаж Valkey ≤15 с.
  - AC5: шаги 1–3 зелёные.
  - AC6: после зелёных прогонов на хосте нет ни контейнеров, ни сетей
    runId/тегов серий; упавший прогон (если был) — stop-без-удаления +
    `README-cleanup.txt`.
  - AC7: `[PHASE]`/`[PHASE-TICK]` в журналах и `phases.log`; логи в
    `/tmp/pgw-e2e-artifacts-{runId}/`; фазы >60 с объяснены.
  - AC8: `git diff --stat main...HEAD` — только файлы карты (прод
    KafkaWorker/PgWorker/AdminPanel/Shared.Core,Etcd,Tls,Metrics — пусто).
- [ ] Step 7: `git status` чист; все серии зелёные — ветка готова к Ф4
  (ревью и мерж — ТОЛЬКО по явному приказу пользователя).

**Шаги (Ф4 roadmap — ТОЛЬКО по приказу пользователя на мерже; мерж-коммит
один, канон AGENTS.md):**

- [ ] Step M1: `arch/roadmap/reliability.md` — удалить пункт
  `t20-kfw-vwk-takeover-e2e` (строки ~45–48); легенда «P4 (t20–t22)» →
  «P4 (t21–t22)» (строка ~18). Проверка: `grep -n "t20" arch/roadmap/reliability.md` → пусто (вкл. `←`-ссылки).
- [ ] Step M2: `arch/roadmap/reliability-report.md` — строка t20 из «Осталось»
  (строка ~120) удалена; сводка R (строка ~75: «(`t20`, `t21`)») → без t20.
  Проверка: `grep -n "t20" arch/roadmap/reliability-report.md` → пусто.
- [ ] Step M3: исторических пометок не оставлять (AGENTS.md: только текущее
  состояние).

**Выход:** мерж-гейт зелёный (AC1–AC8), roadmap синхронен мерж-коммиту.

**Spec:** §13 (AC), §14 (мерж-гейт; прогоны/фильтры/зачистка — дословно), §12(ж).

---

## Риски и СТОП-условия (spec §15 — действуют на всех этапах)

| # | Риск | Митигация (этап плана) |
|---|---|---|
| Р1 | Сборка `kafkaworker:e2e` из продового Dockerfile при холодном кэше дольше 120 с CLI-бюджета | Э5: готовая инфраструктура уже содержит `[PHASE]`-метки ДО/ПОСЛЕ + полный вывод в `process-build-*.log` + бюджет CLI 120 с; тёплый кэш — секунды; устойчивое переполнение — СТОП и пересогласование (свой Dockerfile НЕ изобретать) |
| Р2 | Окно kill у Valkey: жертва-ротация ОТМЕНЕНА (env-модель ~0,2 с — окно исчезло, решение пользователя 2026-10-09); жертва = демонтаж X0–X3, окно по коду ~1–3 с; остаток: окно X-фаз < полла 500 мс (survivor проходит X0→X2 одним тиком <0.5 с, прогон `e055e1e0`) — journal-полл неприменим | Э6: якорь §8 A3 (config TO_REMOVE + клэйм + контейнер ИЛИ work op=deprovision) + пре-факт «X0 жертвы не начат»; journal-полл заменён конструктивным доказательством I2 (kill-до-X0 + kill без рестарта + единственность survivor + терминальная чистота + лог-факт `deprovision {C}`) — решение пользователя 2026-10-09; **блокер снят** |
| Р3 | Держатель клэйма резолвится, но url-порт не совпадает ни с одним api-портом | Готовые кейсы: ассерт-fail с полным дампом (claims/api-ключи/порты); чинить тест, не ослаблять |
| Р4 | Осиротевшие `kfw-net-tkw*` исчерпают подсети | Э5/Э7: own-only teardown + демонтаж движком (A6) + ассерт чистоты + контроль между сериями + `network prune` |
| Р5 | Нагрузка docker-хоста растягивает K4/бут → бюджеты фаз | Бюджеты 120–360 с с тиками 5 с; slow-phase >60 с — автосбор логов и отчёт; перезапуск упавших — только после анализа |
| Р6 | Коллизия PortRange с dev-стендом/чужими сериями | Окна `FreePortWindow` (21000–31000, вне зоны стенда); api/etcd-порты — эфемерная зона; литералов нет |
| Р7 | Entrypoint пина `valkey/valkey` ведёт себя иначе ожидания — обёртка не раскатывает серты | Э2 Step 1: ручной прогон обёртки на пине ДО вписывания в процессы; расхождение с §4.1 — СТОП и пересогласование обёртки |
| Р8 | Cmd-обёртка/shell-экранирование собрана неверно → нода стартует с битыми args | Э2: TDD `BuildCmd` (канон → строка → обратный парсинг, пустой `--save ''`); Э4: интеграционный ProvisioningTests ловит фактический boot |
| Р9 | Случайная генерация серта в env ломает идемпотентность | Э2: сверка только по `IsValidNodeEnv` + детерминированная обёртка; юнит «валидный env не триггерит пересоздание» |
| Р10 | Скрытый прод-потребитель helper-механики/TarArchive | Э3 Step 1: grep-гейт перед демонтажом; найден — СТОП, демонтаж только после анализа с пользователем |
| Р11 | Демонтаж до миграции оставляет осиротевший `vwk-<C>-tls` | Живые чистит легаси-чистка надзора (AC2); прочие — runbook-инструкция ручной чистки (Э4 Step 5) |
| Р12 | Нужна правка в НЕ-граничных местах (панель читает том, KafkaWorker-прод, etcd-контракт) | Спекой проверено: панель — только `ca_pem` из etcd; опровергнет фаза кода — СТОП и пересогласование границ |

**СТОП-условия (прекратить и доложить; spec §15):** (1) нужна правка за
пределами границ §5 (прод KafkaWorker/PgWorker/AdminPanel, контракт etcd
arch/20, поведение вне TLS-механики); (2) скрытый прод-потребитель
helper-механики — демонтаж без анализа; (3) entrypoint/обёртка на реальном
образе не работает как §4.1 (Р7); (4) окно kill у Valkey стабильно
недостаточно — пересогласование жертвы; (5) каноны E2E
(изоляция/порты/телеметрия) невозможно соблюсти без компромисса; (6) резолв
держателя недетерминирован и не чинится правкой теста.

**Перезапуск упавших тестов** — ТОЛЬКО после полного анализа артефактов
(`/tmp/pgw-e2e-artifacts-{runId}/`, docker-логи, `[PHASE]`-журналы) и
формулировки причин (docs/e2e-launch §4; AGENTS.md телеметрия).

## Зачистка серий (сводка; после КАЖДОЙ серии — по финальной строке прогона)

- **Юнит-серии** (Э2/Э3): docker-контейнеров не порождают; зачистка не нужна.
- **Серии интеграций/lifecycle Valkey** (Э4, Э7 Step 3): own-only по тегу
  серии `{tag}`; `docker ps -a --filter name={tag}` → пусто;
  `docker volume ls -q | grep -c {tag}` → 0; per-cluster сетей нет
  (standalone); артефакты `/tmp/pgw-e2e-artifacts-<guid>/`.
- **Kafka-серии** (Э5, Э7 Step 4): own-only по тегу; контейнеры/тома тега → 0;
  контроль `docker network ls | grep kfw-net-tkw` (остатки — только при
  нулевом числе живых контуров серии) + `docker network prune -f` при
  осиротевших — уровень хоста, не теста.
- **Valkey takeover-серия** (Э6, Э7 Step 4): own-only по тегу (воркеры/etcd —
  runId, кластер — `tvw{tag}`); контейнеры/тома тега → 0; прогон Э6 — с
  `TMPDIR=$HOME/pgw-diag-t20/` (артефакты серии —
  `~/pgw-diag-t20/pgw-e2e-artifacts-{runId}/`, вне чистимого /tmp).
- **PgWorker Scale_AddEmptyShard** (Э7 Step 5): самоочистка сценария +
  страховочный контроль после финальной строки.
- Упавший прогон: teardown уже остановил контейнеры без удаления — ручная
  зачистка по `README-cleanup.txt` ПОСЛЕ разбора; повторный прогон — только
  после анализа.

## Self-Review (выполнен при написании)

- **Покрытие спеки:** §1→Goal; §2 (I1–I7)→Э5/Э6 (кейсы готовы/адаптация);
  §3→контракты (проверено по коду); §4.1→контракт env/обёртки + Э2 Step 1
  (Р7); §4.2→Э2; §4.3→Э3; §4.4→миграция (Э2 надзор + Э4 runbook);
  §4.5→Э1; §5→Global Constraints; §6→Э5/Э6 (инфраструктура готова);
  §7→Э5; §8 (сценарий V — демонтаж X0–X3; I2 по построению + документальный
  лог — решение пользователя по Р2: journal-полл неприменим, прогон `e055e1e0`)
  →Э6; §9→Э2 (юниты)/Э4 (интеграции/lifecycle);
  §10→Э4 Steps 4–5; §11→Global Constraints + зачистка серий; §12→таблица
  этапов (а)–(ж) = Э1–Э7; §13→Э7 Step 6; §14→Э7 Steps 2–5 (дословные
  команды); §15→таблица рисков/СТОП; §16→бюджеты в шагах прогонов; §17→Э3
  Step 1 (TarArchive-гейт).
- **Сверено по коду (фактические пути/строки):** `NodeArgsBuilder.cs` —
  фактически `src/ValkeyWorker.Provisioning/Processes/` (НЕ
  `ValkeyWorker.Core/Templates/`, как в вводной); `NodeTlsProvisioner.cs` —
  там же в `Processes/` (НЕ корень `Provisioning/`); `ClusterDriver.cs` —
  `EnsureTlsVolumeAsync`/`Put/GetTlsArchiveAsync`/`RemoveTlsVolumeAsync`/
  `TlsVolumeName` на местах (Plain ~:298–336, Swarm ~:456–486), `Binds` в
  обоих `EnsureNodeAsync`; `ProvisioningProcess` V3 — вызов
  `EnsureNodeTlsAsync` ~:283, сверка `liveArgs.SequenceEqual(args)` ~:299;
  `CaRotator` R — `GetTlsArchiveAsync`+`IsValidTar` ~:179–184, R.3
  `EnsureNodeTlsAsync`+`TlsVolumeName` ~:196–216; `NodeSupervisor` —
  `EnsureNodeTlsAsync` в `RecreateAsync` ~:286, `TlsVolumeName` ~:299;
  `TlsMigrator` — ДВЕ точки точного сравнения `args.Contains("--tls-port")`:
  `NeedsMigration` :55 и T2-предчек `RunAsync` :118–121 (при обёртке обе
  сломаются — в плане заменены на вхождение); `DeprovisioningProcess`
  — `RemoveTlsVolumeAsync` ~:64–67; `DockerEngine` — helper-блок ~:265–351 и
  ~:1127–1179; `ContainerSpec.Env` существует; `InspectContainerEnvAsync`/
  `InspectServiceEnvAsync` в `IDockerEngine` существуют; пин образа
  `valkey/valkey:9.1.2` (`ProcessCommon.cs:34`, appsettings); фейки:
  `Fakes.cs` `FakeDriver.TlsVolumes` ~:378–430; grep-потребители
  `Put/GetVolumeArchiveAsync`: прод — только `ClusterDriver.cs` (4 места);
  фейки — Valkey ×2 файла + PgWorker ×6 файлов; TarArchive-потребители вне
  Shared.Docker — только интеграционные `ProvisioningTests`/`TlsMigrationTests`
  (`TarArchive.Read`) и юниты Valkey — чистятся в Э2 (Step 7);
  `arch/20` — grep
  volume/helper/tar пуст (правка не требуется); чек 51 — volume-ассерты в
  строках ~159–170; runbook — «volume `vwk-<C>-tls`» ~:298, «(R) … volume»
  ~:310; готовые E2E-кейсы — Volume-ассерты: готовый `ValkeyE2eTakeoverTests` (на
  жертве-ротации, volume-ассерты :197/:210–235 — переписывается под
  демонтаж целиком в Э6), `KafkaE2eTakeoverTests` :203–206
  (vwk-tls-writer-комментарий), `ValkeyE2eLifecycleTests` :113/:233/:381
  (VolumeExistsAsync в wait-clean — остаются, том не создаётся);
  `ValkeyE2eEnvironment.ArtifactsDir` = `Path.GetTempPath()` (:130/:226 —
  уважает TMPDIR → прогон Э6 с `TMPDIR=$HOME/pgw-diag-t20/` уводит артефакты
  из чистимого /tmp); roadmap-блоки t20: reliability.md :18/:45–48,
  reliability-report.md :75/:120.
- **Детали, выведенные из кода (не споря со спекой, доложено):** (1)
  `CleanupLegacyVolumeAsync` — имя новой утилиты легаси-чистки на
  `IClusterDriver` (спека §4.2 требует удаления `RemoveTlsVolumeAsync`, но
  легаси-чистке нужен перебор engines — имя спекой не задано; семантика 409 —
  по §4.2: не фейлит тик и не пишется в warnings, безусловный ретрай тиком);
  (2) `ValkeyNodeSpec.Args` при env-модели несёт cmd-обёртку
  (`BuildCmd(Build(...))`) — ContainerSpec.Cmd без изменений семантики;
  (3) детект `--tls-port` в `TlsMigrator` — по вхождению в ОБЕИХ точках
  точного сравнения (`NeedsMigration` :55, T2-предчек `RunAsync` :118–121);
  (4) фильтр серии Э4 исключает Takeover
  (`!~Takeover`) — до адаптации Э6 полный фильтр не зелёный; полный фильтр —
  Э7 Step 3; (5) бюджет A6 Kafka-кейса возвращён к 60 с — буква спеки §7 A6
  (решение координатора: 180 с обосновывались helper-хвостом, которого у
  KafkaWorker нет — серты через env; факт прошлого зелёного прогона — 17.3 с);
  (6) путь `NodeArgsBuilder.cs`/`NodeTlsProvisioner.cs` — фактические, не из
  вводной.
- **Placeholder-скан:** TBD/TODO нет; все сигнатуры — в контракте изменений;
  бюджеты всех фаз указаны.
- **Регресс-обязательства:** AC3/AC4/AC5 — Э5/Э6/Э4+Э7; AC8 — карта файлов +
  Э7 Step 6; фильтр `~Takeover` нигде не используется; бюджеты демонтажа не
  двигаются (15 с — устранением тома).
