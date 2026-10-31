# План: переделка UI AdminPanel по целевой модели 06-ui-analysis (u01–u19 + restore)

Исполнитель плана — субагент coder (сильная модель, работает один, включая подпункты).
Порядок шагов фиксирован (spec §12): arch → воркеры → панель (бэкенд) → SPA → финальные чеки.
E2E — НЕ шаг исполнения: правки воркеров включают docker-E2E серии в МЕРЖ-ГЕЙТ (spec §10 п.5), в Фазе 6 не прогоняются.

Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-ui-rework`.
Команды сборки/тестов — канон проекта: `DOTNET_CLI_UI_LANGUAGE=en dotnet build/test ...`, `npm run typecheck/build` в `frontend/`.
Перед каждой сборкой/серией тестов — зачистка остатков предыдущих серий (AGENTS.md проекта); вывод команд пишется в файл-лог полностью.

Принятые в плане решения на неопределённостях spec (список «assumptions», передан main-агенту):
- A1 (шаг 1, u16): границы валидации `garbage_min_age_sec` — long ≥ 600 (единообразно с `full_max_age_sec`; дефолт канона arch/19 §4 — 600 с); фиксируется в arch/02 §9.12.
- A2 (шаги 1, 4, u07): живые ротации pg требуют чтения range `/pgworker/rotations/` в pg-тик + поле `EtcdSnapshot.PgRotations` (в spec §8 прямо не перечислено, но u07 «живая ротация /pgworker/rotations/<C>» задаёт источник; добавляется строкой в arch/02 §2.3.1 и §3).
- A3 (шаг 3, u17): признак «ротация начата» для гварда отмены — work-журнал кластера `/kafkaworker/work/<C>` (или `/valkeyworker/work/<C>`) с op ротации (`rotate`/`rotate-ca`) и фазой исполнения; множества фаз берутся из констант PasswordRotator/CaRotator соответствующего воркера (терминальные `done`/aborted-фазы и waiting-паузы — «не начата»).
- A4 (шаг 2, u15): состав тела `GET /api/config` — выписан в шаге 2 по фактическим POCO `Options.cs` каждого воркера; исключаются все TLS/секретные поля, etcd/docker endpoints.
- A5 (шаг 9, u09): механика ленты — подписка `queryClient.getQueryCache().subscribe(...)` в провайдере `EventsProvider`, смонтированном внутри `AppLayout`; список наблюдаемых ключей — в шаге 9.

---

## Шаг 1. Arch-first: контракты (arch/adminpanel/02, 03; arch/14/16/21/24; docs/adminpanel/03, 04)

**Вход**: worktree чист, spec прочитана, код воркеров/панели изучен (имена ниже — из фактического кода).

**Действие** (сильная модель; все правки — только документация, код не трогаем):

1. `arch/adminpanel/02-etcd-contract.md`:
   - Шапка (после строки 1-заголовка, абзац «Мутации»): дополнить состав мутаций: отмена живых заявок ротаций kafka/valkey (§10.2-18/19/20, §11.2-7/8), restore-заявка (новый §9.13), эффективный конфиг (on-demand прокси `GET /api/workers/{worker}/effective-config`).
   - §2.3.1 (таблица читаемых ключей `/pgworker/…`): добавить строки `/pgworker/backups-disabled` (значение — канон arch/19: `{"updated_unix","updated_by"?}`), `/pgworker/evacuations/<C>/<X>` (формат — arch/14 §3.3), `/pgworker/backups/<C>/<X>/agent/<N>` (формат — arch/19 §4), `/pgworker/rotations/<C>` (живая заявка ротации per-cluster секретов; формат — arch/14 §3.3), `/pgworker/leader`, `/pgworker/claims/`, `/pgworker/instances/` (форматы — arch/14 §3.3).
   - §2.3.2/§2.3.3/§2.3.4: к читаемым ключам каждого воркера добавить `work/`, `leader`, `claims/`, `instances` соответствующего префикса (`/kafkaworker/…`, `/valkeyworker/…`, `/s3worker/…`).
   - §3 (модель снапшота): новые записи — `BackupsDisabledInfo { UpdatedUnix, UpdatedBy? } | null`, `WalAgentInfo { Cluster, Shard, Node, Role, State, LastDeliveredSegment?, LastDeliveredUnix?, PoolExhausted, TliJump?, HistoryMissingTli?, Error?, UpdatedUnix }`, `EvacuationInfo { Cluster, Shard, EvacuatedUnix, Reason, Buckets: словарь старый→новый, State, ReturnedUnix? }`, `PgRotationTicket { Cluster, RequestedUnix, RequestedBy? }`, `WorkerCoordination { LeaderInstance?, Claims: [{ Cluster, Instance, SinceUnix? }], Instances: [{ InstanceId, SinceUnix? }] }`, work-журналы доменов (`WorkJournalInfo` для kafka/valkey/s3 — тем же типом, что pg).
   - §9.12: тело policy + `garbage_min_age_sec` (long, ≥ 600; отсутствие поля → секция не кладётся, дефолт конфига 600 с — решение A1).
   - Новый §9.13 «Restore-заявка (прокси)»: `POST /api/clusters/{c}/shards/{shard}/restore`, тело `{ "confirm": "<shard>", "targetTime"?: "<RFC3339>" }`, панель — прокси в `POST /api/clusters/{c}/shards/{shard}/restore` API PgWorker (arch/19 §3.5), коды 202/400/404/409/503, `requested_by` — username сессии (заголовок X-Requested-By).
   - §10.1 (читаемые ключи kafka): + `work/`, `leader`, `claims/`, `instances`; §10.2 (таблица мутаций): + №18 `DELETE /api/kafka/clusters/{c}/app-password/rotate` (снятие живой заявки `/kafkaworker/rotations/<C>`), №19 `DELETE .../admin-password/rotate` (`/kafkaworker/admin_rotations/<C>`), №20 `DELETE .../ca/rotate` (`/kafkaworker/ca_rotations/<C>`) — без `requested_by` (del без тела, аудит — host-лог воркера; инвариант аудита spec §7: username сессии — на мутациях, СОЗДАЮЩИХ заявку; существующая отмена rebalance — тот же паттерн); семантика: удаление ключа выполняет ТОЛЬКО воркер, гвард «ротация не начата» (фазы A/B/C или P/D/R/C) → 409 «ротация уже исполняется»; заявки нет → 404; успех → 204; недоступность etcd/API → 503.
   - §11.1/§11.2 (valkey): чтение work/leader/claims/instances; мутации №7 `DELETE /api/valkey/clusters/{c}/password/rotate` (ключ `/valkeyworker/rotations/<C>` — один на обе роли), №8 `DELETE /api/valkey/clusters/{c}/ca/rotate` (`/valkeyworker/ca_rotations/<C>`) — без `requested_by` (del без тела, аудит — host-лог воркера); семантика та же.
   - §12.1/§12.2 (s3): чтение `work/`, `leader`, `claims/`, `instances` s3worker.
   - В §2.3.x (описание дискавери) добавить: on-demand прокси эффективного конфига `GET /api/workers/{worker}/effective-config` → `GET /api/config` воркера (без снапшота, без etcd; живых нет → 503); контракт `GET /api/config` — arch/14/16/21/24 §1.1.
2. `arch/adminpanel/03-panels.md`:
   - §1 (таблица эндпоинтов): + `DELETE /api/kafka/clusters/{c}/app-password/rotate`, `.../admin-password/rotate`, `.../ca/rotate`; `DELETE /api/valkey/clusters/{c}/password/rotate`, `.../ca/rotate`; `POST /api/clusters/{c}/shards/{shard}/restore`; `GET /api/workers/{worker}/effective-config`.
   - §1.6: заголовок и маршрут `POST /api/clusters/{cluster}/app-password/rotate` → `POST /api/clusters/{cluster}/secrets/rotate` (u19: arch под фактический код — `OperationsModule.cs`/`ApiModule.cs`; семантика: ротация трёх ролей app/bucket_admin/bucket_mover + rest, DTO `ClusterSecretsRotatedDto`; ключ `/pgworker/rotations/<C>` без изменений). Аналогично пройтись по §2 и §3 (шапка кластера) — все вхождения `app-password/rotate` заменить на `secrets/rotate`.
   - §2 (DTO): `OverviewDto` + `ha { scopesTotal, withoutLeader, unmatched }`, `backups { configured, health { apiOk, liveOk, clusterOk } | null, storageState (OK|WARN|CRIT|null), usedBytes, quotaBytes, etcdSnapshots { enabled, state } | null }`, `reliability { worstRpoPotentialSec | null, fullOnlyCount, offCount, lastFailover { durationSec, atUnix } | null }`, `operations[] { domain (pg|kafka|valkey|s3), kind, cluster, entity | null, detail, updatedUnix | null }`, `freshness { etcd { builtAtUtc }, pg { builtAtUtc }, kafka { builtAtUtc }, valkey { builtAtUtc }, s3 { builtAtUtc } }`; `BackupShardStorageDto` + `agents[]`; `ClusterDto` + `evacuations[]`; `KafkaClusterDto`/`ValkeyClusterDto`/`S3ClusterDto` + `work`; `WorkerViewDto` + `coordination`; policy-тело + `garbageMinAgeSec`.
   - §3 (дерево панелей): навигация 5 групп (Обзор / Домены / Данные и защита / Наблюдение / Инфраструктура — состав по spec §3.1), вкладки доменных страниц (pg: Шарды|Бакеты|Переезды|Бэкапы (срез)|Секреты|Журнал; kafka: Брокеры|Топики|Группы|Журнал; valkey: Нода|Журнал; s3: Ноды|Репликация|Связность и синхронность|Журнал), новая страница шарда `/clusters/:c/shards/:x`, `/events`, «Репликация» `/ha` (u01/u03).
   - §3.7 (Воркеры): + координация (лидер, claims «кто держит кластер»), + «Эффективный конфиг» read-only (пометки «env, применяется рестартом воркера»).
   - §3.8: форма «Policy бэкапов кластера» — единый модал (retention + full_max_age_sec + verify + drill + garbage_min_age_sec + секция s3), кнопки: per-кластер на `/backups-storage` и на вкладке «Бэкапы (срез)».
   - §4 (каталог алертов AlertEngine): актуализировать полным фактическим перечнем kind — снять список с кода `src/AdminPanel.Core/Alerting/Rules/*.cs` (46 файлов: backup-chain-broken, backup-deleting-stuck, backup-drill-failed, backup-drill-stale, backup-full-stale, backup-orphan, backup-pool-missing, backup-pool-unreachable, backup-s3-unreachable, backup-storage-quota, backup-verify-failed, backup-wal-tli-gap, bucket-lost, bucket-no-routing, bucket-out-of-range, cluster-incomplete, etcd-alarm, etcd-endpoint-down, etcd-no-quorum, etcd-snapshot-export-failed, etcd-snapshot-export-stale, etcd-unreachable, ha-member-not-streaming, inventory-mismatch, key-malformed, move-aborting, move-flipped-status-stuck, move-frozen-long, move-stale, probe-failed, provision-stuck, replica-lag-high, restore-failed, shard-no-leader, shard-no-master, slot-invalidation-risk, slot-lag-high, slot-wal-lost, snapshot-stale, sync-standby-missing, wal-chain-broken, wal-stream-lag, wal-stream-stopped, worker-api-unreachable, worker-unhealthy + правило cluster-not-initialized, если оно эмитит kind); критерий — каждый `kind` кода присутствует в каталоге; + новый `backups-disabled` (warning, Remedy `OperatorRunbook`, RemedyText «запуск — снять ключ /pgworker/backups-disabled или включить Backups:Enabled»). Критерий полноты — сверка по фактическому перечню из кода правил (kind-строки берутся из файлов правил, не из этого списка).
   - §7.1/§8.1: + DELETE-эндпоинты отмены ротаций (kafka — 3, valkey — 2).
   - §7.4/§8.4/§12.4: каталоги доменных алертов — сверка с кодом `KafkaAlertEngine`/`ValkeyAlertEngine`/`S3AlertEngine` (добавить отсутствующие kind по коду).
3. `arch/14-pgworker.md` §1.1 (контракт HTTP API): + `GET /api/config` (эффективный конфиг установки, read-only, mTLS-зона /api; состав — группы Thresholds/Loops/Moves/Parallelism/Backups.Enabled/Snapshots.Export(enabled, interval)/Pgtune-входы; БЕЗ секретов и кредов); + `POST /api/clusters/{c}/shards/{x}/backups/policy` принимает `garbage_min_age_sec` (long ≥ 600, отсутствие → поле в policy не кладётся).
4. `arch/16-kafkaworker.md` §1.1: + `GET /api/config` (Thresholds/Loops/Parallelism + флаги подсистем, без секретов); + DELETE-отмена ротаций app/admin/CA (гвард «не начата» по work-журналу; 204/404/409/503).
5. `arch/21-valkeyworker.md` §1.1: + `GET /api/config`; + DELETE-отмена ротаций пароля/CA (204/404/409/503).
6. `arch/24-s3worker.md` §1.1: + `GET /api/config`.
7. `docs/adminpanel/03-probes-alerts.md`: «25 правил» → фактический перечень (снимается с кода правил; тот же список, что для arch/03 §4).
8. `docs/adminpanel/04-frontend.md` (раздел «Слои»): + две строки — `components/actions/` (общие компоненты действий), `events/` (клиентский событийный слой ленты).

`docs/adminpanel/06-ui-analysis.md` в этом шаге НЕ правится (его чек-лист-обновление — шаг 11).

**Подпункты для слабой модели**: нет (документация требует понимания контрактов — сильная модель).

**Выход**: arch/adminpanel/02, 03 и arch/14/16/21/24 отражают новые контракты; docs/adminpanel/03, 04 дополнены.

**Проверка**: ручная сверка по чек-листу раздела 8 spec (каждый пункт раздела 8 имеет встречную правку в файле); `grep -n "app-password/rotate" arch/adminpanel/03-panels.md` — 0 вхождений в контексте pg-маршрута ротации секретов (u19).

**Связь со spec**: §2 п.1 (arch-first), §8 (полный список), u19, D16.

---

## Шаг 2. Воркеры: `GET /api/config` ×4 (u15)

**Вход**: шаг 1 закоммичен (контракты arch/14/16/21/24 §1.1 зафиксированы).

**Действие** (сильная модель):

В каждом из четырёх воркеров добавить read-only эндпоинт `GET /api/config` (mTLS-зона `/api` — она уже защищена существующим TLS-матчингом, отдельно ничего не нужно). Единый механизм: минимальный хендлер-маппер «POCO Options → плоский DTO без секретов», отвечающий 200 `application/json`. Состав тела (решение A4, по фактическим POCO `Options.cs` каждого воркера; всё небезопасное НЕ-секретное; секреты/TLS-поля/endpoints — НЕ отдаются):

- PgWorker (`src/PgWorker.App/Options.cs`, `PgWorkerOptions`): `thresholds` (NodeDeadSec, ShardDeadSec, PatroniBootSec, CutoverTimeoutSec, ConnFailBudgetSec, ProvisionRetryBaseSec, ProvisionRetryMaxSec), `loops` (ScanIntervalSec, KeepaliveSec, SnapshotIntervalMin, ErrorDelayMs), `moves` (PollIntervalSec, FreezeWaitSec, FreezeLockTimeoutSec, FreezeLockTries, AbortMinAgeSec), `parallelism` (MaxClusters — поле ParallelismOptions), `backups` (только `enabled: bool` из BackupsOptions), `snapshots` → `export` (enabled: bool, intervalMin — фактические имена полей SnapshotExportOptions, проверить по коду), `pgtune` (DbVersion, DbType, HdType, DbSize, Connections — входы профиля). Каждая группа с пометкой `"source": "env"` — применение рестартом воркера.
- KafkaWorker (`src/KafkaWorker.App/Options.cs`, `KafkaWorkerOptions`): `thresholds` (BrokerBootSec, NodeDeadSec, ReassignSubmitSec, ReassignRetrySubmitSec, RotationTicketTimeoutSec), `loops` (ScanIntervalSec, KeepaliveSec, TopicSyncIntervalSec, ReassignIntervalSec, ReassignBatchPartitions, SnapshotIntervalMin, ErrorDelayMs), `parallelism` (MaxClusters).
- ValkeyWorker (`src/ValkeyWorker.App/Options.cs`): `thresholds`, `loops`, `parallelism` — фактические поля POCO (сверить по файлу; состав — все числовые интервалы/бюджеты этих секций).
- S3Worker (`src/S3Worker.App/Options.cs`): `thresholds`, `loops`, `parallelism` — фактические поля POCO.

Исключаются всегда: `Api` (AdvertiseUrl, Tls, EnableSeedEndpoint), `Etcd` (Endpoints), `Docker` (Hosts, SwarmManager, Tls, Ssh, RestTls), любые PEM/пути/пароли, `Metrics` (не входит в u15).

Реализация: в `src/PgWorker.App/Api/ApiModule.cs`, `src/KafkaWorker.App/Api/ApiModule.cs`, `src/ValkeyWorker.App/Api/ApiModule.cs`, `src/S3Worker.App/Api/ApiModule.cs` добавить `endpoints.MapGet("/api/config", (IOptions<*WorkerOptions> options) => Results.Ok(<Worker>EffectiveConfig.From(options.Value)))` — minimal API резолвит `IOptions` напрямую, отдельный DI не нужен; маппер — статический класс `<Worker>EffectiveConfig` в `Api/Operations` соответствующего воркера (код — в подпунктах 2.1–2.4). Сериализация — camelCase по умолчанию.

**Подпункты для слабой модели** (механическая часть — 4 идентичных маппера):

2.1. В файле `src/PgWorker.App/Api/ApiModule.cs` (worktree `/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-ui-rework`) в метод `MapWorkerApi` перед строкой `endpoints.MapPost("/api/seed/demo"` добавить регистрацию GET-эндпоинта эффективного конфига. Точно следующим кодом (новый класс — в конце того же файла, вне класса ApiModule):

```csharp
// Внутри MapWorkerApi, перед MapPost("/api/seed/demo"):
endpoints.MapGet("/api/config", (IOptions<PgWorkerOptions> options)
    => Results.Ok(PgWorkerEffectiveConfig.From(options.Value)));

// Новый файл src/PgWorker.App/Api/Operations/EffectiveConfig.cs:
// Эффективный конфиг установки (arch/14 §1.1 GET /api/config): read-only,
// БЕЗ секретов/TLS/endpoints; поле source=env — применяется рестартом воркера.
public static class PgWorkerEffectiveConfig
{
    public static object From(PgWorkerOptions o) => new
    {
        source = "env",
        thresholds = new { nodeDeadSec = o.Thresholds.NodeDeadSec, shardDeadSec = o.Thresholds.ShardDeadSec, patroniBootSec = o.Thresholds.PatroniBootSec, cutoverTimeoutSec = o.Thresholds.CutoverTimeoutSec, connFailBudgetSec = o.Thresholds.ConnFailBudgetSec, provisionRetryBaseSec = o.Thresholds.ProvisionRetryBaseSec, provisionRetryMaxSec = o.Thresholds.ProvisionRetryMaxSec },
        loops = new { scanIntervalSec = o.Loops.ScanIntervalSec, keepaliveSec = o.Loops.KeepaliveSec, snapshotIntervalMin = o.Loops.SnapshotIntervalMin, errorDelayMs = o.Loops.ErrorDelayMs },
        moves = new { pollIntervalSec = o.Moves.PollIntervalSec, freezeWaitSec = o.Moves.FreezeWaitSec, freezeLockTimeoutSec = o.Moves.FreezeLockTimeoutSec, freezeLockTries = o.Moves.FreezeLockTries, abortMinAgeSec = o.Moves.AbortMinAgeSec },
        parallelism = new { maxClusters = o.Parallelism.MaxClusters },
        backups = new { enabled = o.Backups.Enabled },
        snapshots = new { export = new { enabled = o.Snapshots.Export.Enabled, intervalMin = o.Snapshots.Export.IntervalMin } },
        pgtune = new { dbVersion = o.Pgtune.DbVersion, dbType = o.Pgtune.DbType, hdType = o.Pgtune.HdType, dbSize = o.Pgtune.DbSize, connections = o.Pgtune.Connections },
    };
}
```
Перед записью сверить фактические имена полей в `src/PgWorker.App/Options.cs` (ParallelismOptions, BackupsOptions.Enabled, SnapshotExportOptions.Enabled/IntervalMin) — если имя поля отличается, использовать фактическое; структуру ответа не менять. using: `Microsoft.AspNetCore.Builder; Microsoft.AspNetCore.Http; Microsoft.AspNetCore.Routing; Microsoft.Extensions.Options;` (какие уже есть в файле — не дублировать).

2.2. Аналогично для KafkaWorker: файл `src/KafkaWorker.App/Api/ApiModule.cs`, перед `endpoints.MapPost("/api/seed/demo"` добавить `endpoints.MapGet("/api/config", (IOptions<KafkaWorkerOptions> options) => Results.Ok(KafkaWorkerEffectiveConfig.From(options.Value)));`; новый файл `src/KafkaWorker.App/Api/Operations/EffectiveConfig.cs` — класс `KafkaWorkerEffectiveConfig` с `public static object From(KafkaWorkerOptions o) => new { source = "env", thresholds = new { brokerBootSec = o.Thresholds.BrokerBootSec, nodeDeadSec = o.Thresholds.NodeDeadSec, reassignSubmitSec = o.Thresholds.ReassignSubmitSec, reassignRetrySubmitSec = o.Thresholds.ReassignRetrySubmitSec, rotationTicketTimeoutSec = o.Thresholds.RotationTicketTimeoutSec }, loops = new { scanIntervalSec = o.Loops.ScanIntervalSec, keepaliveSec = o.Loops.KeepaliveSec, topicSyncIntervalSec = o.Loops.TopicSyncIntervalSec, reassignIntervalSec = o.Loops.ReassignIntervalSec, reassignBatchPartitions = o.Loops.ReassignBatchPartitions, snapshotIntervalMin = o.Loops.SnapshotIntervalMin, errorDelayMs = o.Loops.ErrorDelayMs }, parallelism = new { maxClusters = o.Parallelism.MaxClusters } };` (сверить имена полей по `src/KafkaWorker.App/Options.cs`).

2.3. Аналогично для ValkeyWorker: `src/ValkeyWorker.App/Api/ApiModule.cs` + новый `src/ValkeyWorker.App/Api/Operations/EffectiveConfig.cs` (класс `ValkeyWorkerEffectiveConfig.From(ValkeyWorkerOptions o)` — группы thresholds/loops/parallelism со всеми числовыми полями этих секций из `src/ValkeyWorker.App/Options.cs`, формат как в 2.2).

2.4. Аналогично для S3Worker: `src/S3Worker.App/Api/ApiModule.cs` + новый `src/S3Worker.App/Api/Operations/EffectiveConfig.cs` (класс `S3WorkerEffectiveConfig.From(S3WorkerOptions o)` — группы thresholds/loops/parallelism из `src/S3Worker.App/Options.cs`, формат как в 2.2).

**Выход**: 4 новых GET-эндпоинта; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 warnings / 0 errors.

**Проверка**: сборка выше зелёная; в каждом из четырёх ApiModule файлов `grep -n 'api/config'` находит MapGet.

**Связь со spec**: §5 u15, D12, D19; правки `src/{PgWorker,KafkaWorker,ValkeyWorker,S3Worker}.App` (→ мерж-гейт E2E, spec §10 п.5).

---

## Шаг 3. Воркеры: DELETE-отмена ротаций kafka/valkey (u17) + policy-поле pg (u16, воркерская часть)

**Вход**: шаг 2 собран; контракты arch/16 §1.1, arch/21 §1.1, arch/02 §10.2-18/19/20, §11.2-7/8, §9.12 зафиксированы (шаг 1).

**Действие** (сильная модель):

1. KafkaWorker — 3 новых DELETE-хендлера в `src/KafkaWorker.App/Api/Operations/` (по образцу `RotateAppPasswordHandler.cs`: паттерн `KafkaApiHelpers.ReadConfigAsync`/`ReadKeyAsync` + `EtcdFailover.CallAsync`):
   - `CancelAppRotationHandler` (ключ `/kafkaworker/rotations/<C>`), `CancelAdminRotationHandler` (`/kafkaworker/admin_rotations/<C>`), `CancelCaRotationHandler` (`/kafkaworker/ca_rotations/<C>`).
   - Общая логика каждого (решение A3): (а) имя каноническое (`KafkaLimits.ClusterPattern()`), иначе 404-исключение; (б) читать work-журнал `/kafkaworker/work/<C>` (тем же `KafkaApiHelpers.ReadKeyAsync`): если JSON-объект с `op` == op ротации (`rotate` — для паролей, `rotate-ca` — для CA) и `phase` ∈ множества переходных фаз исполнения (для `rotate`: `phase-a`, `rotated-commit`, `phase-c`, `admin:phase-a`, `admin:rotated-commit`, `admin:phase-c` — сверить с константами `PasswordRotator`; для `rotate-ca`: `phase-p`, `phase-d`, `phase-r`, `committed`, `phase-c` — сверить с `CaRotator`) → отказ `KafkaRotationInProgressException` (новое исключение в `KafkaExceptions.cs`, сообщение «rotation is already executing — cancellation window closed»); терминальные (`done` и производные aborted) и waiting-фазы, отсутствие журнала/чужой op — «не начата», отмена разрешена; (в) ключ заявки отсутствует → `KafkaRotationNotFoundException` (новое исключение, 404; идемпотентность повтора отсутствует осознанно — образец §9.7.5); (г) del ключа заявки через `EtcdFailover.CallAsync` + `TxnRequest.Of([TxnCompare.Value(key, "==", прочитанное значение)], [TxnOp.Delete(key)])` — compare по значению, чтобы гонку с параллельным клэймом воркера поймать отказом txn → тогда вернуть `KafkaRotationNotFoundException`/409 по факту (если ключ исчез — 404; если значение изменилось — повторить чтение п.(б) один раз и решить заново); (д) успех → `Result<RotationCancelledDto>.Success(new(cluster))`, где `record RotationCancelledDto(string Cluster)` — по образцу воркерских DTO-дублей (204 без тела, панель тело не читает).
   - `src/KafkaWorker.App/Api/ApiModule.cs`: три `MapDelete` — `/api/kafka/clusters/{cluster}/app-password/rotate`, `.../admin-password/rotate`, `.../ca/rotate`; маппинг исключений (порт существующих веток): `KafkaClusterNotFoundException or KafkaRotationNotFoundException` → 404 «Not found»; `KafkaRotationInProgressException` → 409 «Rotation rejected»; `EtcdWriteUnavailableException or InvalidKafkaConfigException` → 503; дефолт → 503. Успех → `Results.NoContent()` (204).
2. ValkeyWorker — 2 хендлера в `src/ValkeyWorker.App/Api/Operations/` (по образцу `RotatePasswordHandler.cs`; `ValkeyApiHelpers`, `ValkeyEtcdFailover`, исключения `ValkeyExceptions.cs`):
   - `CancelPasswordRotationHandler` (ключ `/valkeyworker/rotations/<C>`; гвард: work-журнал `/valkeyworker/work/<C>` op == `rotate` и фаза переходная (фазы E1/E2 — сверить константы `PasswordRotator` valkey) → `ValkeyRotationInProgressException`; иначе отмена), `CancelCaRotationHandler` (`/valkeyworker/ca_rotations/<C>`; op == `rotate-ca`, фазы P/D/R/C — сверить `CaRotator` valkey).
   - Новые исключения: `ValkeyRotationNotFoundException`, `ValkeyRotationInProgressException` (в `ValkeyExceptions.cs`).
   - `src/ValkeyWorker.App/Api/ApiModule.cs`: `MapDelete("/api/valkey/clusters/{cluster}/password/rotate")` и `MapDelete("/api/valkey/clusters/{cluster}/ca/rotate")`; маппинг: 404/409/503 — по образцу существующих веток; успех → 204.
3. PgWorker, policy-поле (u16): `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs` — в приватный record `PolicyBody` добавить `[property: JsonPropertyName("garbage_min_age_sec")] long? GarbageMinAgeSec`; валидация: значение < 600 → `errors.Add(new("garbage_min_age_sec", "minimum 600 sec"))` (граница — решение A1); в сборку `policyBody` — если `GarbageMinAgeSec is { } g` → `policyBody["garbage_min_age_sec"] = g` (отсутствие → поле не кладётся, дефолт конфига 600 с).
4. Юнит-тесты:
   - Новый `src/tests/KafkaWorker.UnitTests/App/CancelRotationHandlerTests.cs` (AAA-комментарии; фикстура `Fakes.FakeEtcd` — образец `src/tests/KafkaWorker.UnitTests/App/…`/`Provisioning/Fakes.cs`; образец стиля — `src/tests/ValkeyWorker.UnitTests/Api/RotateCaHandlerTests.cs`): кейсы — неканоническое имя → 404-исключение; заявки нет → NotFound; заявка есть, журнала нет → успех (ключ удалён); журнал op=rotate фаза phase-a → InProgress; журнал op=rotate фаза done → успех; (для CA-хендлера — зеркально с rotate-ca/phase-p).
   - Новый `src/tests/ValkeyWorker.UnitTests/Api/CancelRotationHandlerTests.cs` (образец `RotateCaHandlerTests.cs`): те же 5 кейсов для password (op rotate) и ca (op rotate-ca).
   - `src/tests/PgWorker.UnitTests/Api/` — новый `BackupsPolicyHandlerGarbageTests.cs` (образец мока — `FakeEtcdGateway.cs` в той же папке; в Arrange засеять в фейк ключ `/clusters/<C>/config`): тело с `garbage_min_age_sec: 600` → policy-put содержит поле (проверить записанный payload); `< 600` → `BackupsPolicyValidationException` с полем `garbage_min_age_sec`; без поля → поле в payload отсутствует.

**Подпункты для слабой модели**: да — механические заготовки 3.1–3.3 (полные спецификации хендлеров с кодом), тесты 3.4–3.6. Сильная модель пишет их сама ВНУТРИ этого шага как подпункты coder-flash (образцы выше самодостаточны) ЛИБО сама — по своему выбору; ответственность за результат и сверку фаз с константами Rotator'ов — на сильной модели. Точные тексты подпунктов формулирует сильная модель при исполнении по образцам из действия (состав подпунктов: 3.1 CancelAppRotationHandler+ApiModule-ветка kafka; 3.2 CancelAdminRotationHandler; 3.3 CancelCaRotationHandler; 3.4 CancelPasswordRotationHandler valkey + ветка; 3.5 CancelCaRotationHandler valkey + ветка; 3.6 — правка BackupsPolicyHandler тремя точечными вставками, приведёнными в п.3 действия).

**Выход**: DELETE-ротации kafka/valkey работают (204/404/409/503); policy принимает garbage_min_age_sec; тесты зелёные.

**Проверка**: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0w/0e; затем `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --no-build` и `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Release --no-build` и `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build --filter FullyQualifiedName~BackupsPolicyHandlerGarbage` — Passed.

**Связь со spec**: §5 u17 (D13), §5 u16 (воркерская часть), §10 п.4; arch/02 §10.2-18/19/20, §11.2-7/8, §9.12.

---

## Шаг 4. Панель бэкенд: снапшот — новые чтения u10–u14 + u07-источники (модели, парсеры, refresher'ы, DTO)

**Вход**: шаг 1 закоммичен; шаги 2–3 собраны (снапшот панели не зависит от них, но порядок канона — воркеры раньше панели).

**Действие** (сильная модель):

1. Новые модели в `src/AdminPanel.Core/`:
   - `EtcdSnapshot.cs` — дополнить record (новыми optional-параметрами В КОНЕЦ, значения по умолчанию `= null`/`= []`): `BackupsDisabledInfo? BackupsDisabled = null`, `IReadOnlyList<WalAgentInfo> WalAgents = []`, `IReadOnlyList<EvacuationInfo> Evacuations = []`, `IReadOnlyList<PgRotationTicket> PgRotations = []`, `WorkerCoordination? Coordination = null`. Сами типы — новыми записями в этом же файле (или соседних): `BackupsDisabledInfo(long UpdatedUnix, string? UpdatedBy)`, `WalAgentInfo(string Cluster, string Shard, string Node, string? Role, string State, long? LastDeliveredSegment, long? LastDeliveredUnix, bool PoolExhausted, long? TliJump, long? HistoryMissingTli, string? Error, long UpdatedUnix)`, `EvacuationInfo(string Cluster, string Shard, long EvacuatedUnix, string Reason, IReadOnlyDictionary<string, string> Buckets, string State, long? ReturnedUnix)`, `PgRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy)`, `WorkerCoordination(string? LeaderInstance, IReadOnlyList<WorkerClaim> Claims, IReadOnlyList<WorkerInstanceInfo> Instances)` + `WorkerClaim(string Cluster, string Instance, long? SinceUnix)` + `WorkerInstanceInfo(string InstanceId, long? SinceUnix)`.
   - `KafkaSnapshot.cs` — + `IReadOnlyList<WorkJournalInfo>? WorkJournals = null`, `WorkerCoordination? Coordination = null` (тип `WorkJournalInfo` — существующий `src/AdminPanel.Core/WorkJournalInfo.cs`; для kafka/valkey/s3 факт-поля last_failover/last_rebuild отсутствуют — парсер уже толерантен, ок).
   - `ValkeySnapshot.cs`, `S3/S3Snapshot.cs` — аналогично (+WorkJournals, +Coordination).
2. Новые парсеры в `src/AdminPanel.Etcd/Parsing/`:
   - `CoordinationParser.cs` — статический `Parse(prefixRoot: string, leaderKv: Kv?, claimsKv: IReadOnlyList<Kv>, instancesKv: IReadOnlyList<Kv>)` → `WorkerCoordination?`: leader — JSON `{"instance","since_unix"?}` (ключа нет → LeaderInstance=null); claims `<root>/claims/<C>` → `WorkerClaim(C, instance, since_unix?)`; instances `<root>/instances/<id>` → `WorkerInstanceInfo(id, since_unix?)`; отсутствие всех ключей → пустая координация (`new(null? LeaderInstance из leaderKv, [], [])`), битый JSON → `KeyParseError`, тик не роняет (толерантность). Значение instances-ключа может быть пустым или JSON — парсить только при непустом теле, иначе SinceUnix=null.
   - `EvacuationsParser.cs` — `Parse(IReadOnlyList<Kv>)` → `(IReadOnlyList<EvacuationInfo> Items, IReadOnlyList<KeyParseError> Errors)`; ключ `/pgworker/evacuations/<C>/<X>`; значение — канон arch/14 §3.3: `{"evacuated_unix","reason","buckets":{...},"state":"DONE|QUARANTINED","returned_unix"?}`; `buckets` — объект старый→новый (словарь строк); толерантно: битый JSON/не объект → parseError.
   - `BackupsParser.cs` (существующий) — расширить разбором agent-ключей `/pgworker/backups/<C>/<X>/agent/<N>` (сейчас они, вероятно, уходят в unknownKeys — убрать из unknown и парсить): значение — канон arch/19 §4: `{"node","role":"master|sync|none","state":"starting|waiting_role|waiting_password|waiting_policy|waiting_slot|waiting_disabled|streaming|paused|failed_permanent","last_delivered_segment"?,"last_delivered_unix"?,"pool_exhausted":bool,"tli_jump"?,"history_missing_tli"?,"error"?,"updated_unix"}`; результат — `IReadOnlyList<WalAgentInfo>` в `BackupsParseResult` (новое поле `Agents`); битый JSON → parseError. Плюс точечный ключ `/pgworker/backups-disabled` — сюда НЕ входит (отдельное чтение, см. п.3: ключ НЕ входит в range `/pgworker/backups/` — байтовое сравнение `-` < `/`, решение D9).
   - `BackupsDisabledParser.cs` — точечный разбор одного Kv `/pgworker/backups-disabled`: значение `{"updated_unix","updated_by"?}` → `BackupsDisabledInfo`; битый JSON → `KeyParseError`.
   - `RotationsParser.cs` — разбор `/pgworker/rotations/<C>`: `{"requested_unix","requested_by"?}` → `PgRotationTicket(C, requested_unix, requested_by)`; толерантный.
   - Work-журналы kafka/valkey/s3 — переиспользовать существующий `WorkJournalParser.Parse(kvs)` (он generic: Cluster = лист ключа) — отдельных парсеров не нужно.
3. `src/AdminPanel.Etcd/SnapshotRefresher.cs` (pg-тик): в `Prefixes` добавить `BackupsDisabled = "/pgworker/backups-disabled"`, `Evacuations = "/pgworker/evacuations/"`, `PgRotations = "/pgworker/rotations/"`, `Leader = "/pgworker/leader"`, `Claims = "/pgworker/claims/"`, `Instances = "/pgworker/instances/"`; добавить параллельные `WithFailoverAsync`-задачи (образец существующих строк 86–96), включить их результаты в условие отказа тика (строки 117–120 — консервативный отказ при транспортном провале ЛЮБОГО нового KV-чтения, spec §12), распарсить и передать в `SnapshotBuilder.Build` (сигнатура расширить новыми optional-параметрами); в `FailTick` (строки 231–250) — перенос прежних значений новых полей (по образцу `previous?.Backups ?? []`); `BackupsDisabled` — точечный range-запрос того же вида (`gateway.RangeAsync(ep, Prefixes.BackupsDisabled, t)` — range по точечному ключу, образец `WorkerApiCert`).
4. `KafkaSnapshotRefresher.cs` / `ValkeySnapshotRefresher.cs` / `S3SnapshotRefresher.cs`: в соответствующих приватных `Prefixes` добавить `Work = "/<worker>/work/"`, `Leader = "/<worker>/leader"`, `Claims = "/<worker>/claims/"`, `Instances = "/<worker>/instances/"`; добавить чтения в набор тика (провал → отказ тика по образцу существующих), `WorkJournalParser.Parse` → `WorkJournals`, `CoordinationParser.Parse` → `Coordination`; отказный путь тика переносит прежние значения новых полей (по образцу существующих optional-полей, напр. `CaRotations`).
5. `src/AdminPanel.Core/Alerting/`: новый файл `Rules/BackupsDisabledRule.cs` по образцу существующих правил (посмотреть любой, напр. `BackupFullStaleRule.cs`): если `snapshot.BackupsDisabled is not null` → алерт kind `backups-disabled`, severity Warning, target `global`, Hint — «глобальный стоп бэкапов: ключ /pgworker/backups-disabled», Remedy `OperatorRunbook`, RemedyText «запуск — снять ключ /pgworker/backups-disabled или включить Backups:Enabled»; зарегистрировать правило в `AlertEngine` (по образцу регистрации существующих правил — найти в `AlertEngine.cs`).
6. DTO инспекции:
   - `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`: `ClusterDto` + `IReadOnlyList<EvacuationDto> Evacuations` (новый `EvacuationDto(string Shard, long EvacuatedUnix, string Reason, IReadOnlyDictionary<string, string> Buckets, string State, long? ReturnedUnix)`) и `bool SecretsRotationPending` (живая заявка `/pgworker/rotations/<C>`: `snapshot.PgRotations.Any(r => r.Cluster == cluster)` — источник для бейджа шапки и вкладки «Секреты»); маппинг в существующем маппере `ClusterDetails` (снапшот: `snapshot.Evacuations.Where(e => e.Cluster == cluster)`).
   - `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs`: `BackupShardStorageDto` + `IReadOnlyList<WalAgentDto> Agents` (`WalAgentDto(string Node, string? Role, string State, long? LastDeliveredSegment, long? LastDeliveredUnix, bool PoolExhausted, long? TliJump, string? Error, long UpdatedUnix)`); маппинг `MapShard` — из `snapshot.WalAgents.Where(a => a.Cluster == cluster && a.Shard == shard)`.
   - `src/AdminPanel.Api/Inspection/KafkaQuery.cs` / `ValkeyQuery.cs` / `S3Query.cs`: `KafkaClusterDto`/`ValkeyClusterDto`/`S3ClusterDto` + `WorkDto? Work` (тип `WorkDto` — существующий в ClusterDetailsQuery.cs); маппинг — джойн `WorkJournals` по имени кластера (отсутствие журнала → null). Дополнительно `KafkaClusterDto` + `KafkaRotationTicketDto? AdminRotation = null` (джойн `AdminRotations` по кластеру — по образцу существующего `CaRotation`; бейдж живой admin-ротации для шапки kafka-страницы и u07-операций). Существующие `Rotation`/`CaRotation`/`Rebalance`/`Reassignment`/`Regen` у KafkaClusterDto, `Rotation`/`CaRotation` у ValkeyClusterDto, `Rotation` у S3ClusterDto — уже есть, не трогать.
   - `src/AdminPanel.Api/Operations/WorkersModule.cs` (`GetWorkersQueryHandler`): `WorkerViewDto` (record определён в этом файле или рядом — найти фактическое объявление) + `WorkerCoordinationDto? Coordination` (`WorkerCoordinationDto(string? LeaderInstance, IReadOnlyList<WorkerClaimDto> Claims, IReadOnlyList<string> Instances)`, `WorkerClaimDto(string Cluster, string Instance, long? SinceUnix)`); маппинг — из `p?.Coordination`, `k?.Coordination`, `v?.Coordination`, `s?.Coordination` соответственно.
7. Юнит-тесты (критерий §10 п.3 — «реальные фрагменты etcd-значений по канону 02 §8, вкл. вырожденные»):
   - `src/tests/AdminPanel.UnitTests/BackupsDisabledParserTests.cs` — новое: валидный JSON; битый JSON → parseError; пустое значение → parseError.
   - `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` — расширить: agent-ключ валидный (state streaming / waiting_slot / failed_permanent + error + tli_jump); битый JSON agent-ключа → parseError; agent-ключ НЕ попадает в unknownKeys.
   - `src/tests/AdminPanel.UnitTests/EvacuationsParserTests.cs` — новое: валидный (DONE + QUARANTINED + returned_unix + buckets-словарь); битый.
   - `src/tests/AdminPanel.UnitTests/CoordinationParserTests.cs` — новое: все ключи есть; пусто (нет ключей → пустая координация, не ошибка); битый leader; claims/instances с since_unix и без.
   - `src/tests/AdminPanel.UnitTests/WorkJournalParserTests.cs` — расширить кейсом без факт-полей (формат kafka/valkey/s3: только op/phase/instance/updated_unix/last_error).
   - `src/tests/AdminPanel.UnitTests/BackupsDisabledRuleTests.cs` — новое: ключ есть → алерт warning/remedy OperatorRunbook; ключа нет → нет алерта.
   - `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` / `KafkaRefresherTests.cs` / `ValkeyRefresherTests.cs` / `S3RefresherTests.cs` — расширить по одному кейсу: новые ключи в ответах gateway → новые поля снапшота; транспортный провал нового range → отказ тика (данные прежние).

**Подпункты для слабой модели**: сильная модель выделяет при исполнении (кандидаты: CoordinationParser.cs целиком, EvacuationsParser.cs целиком, BackupsDisabledParser.cs целиком, WalAgentDto/EvacuationDto-маппинги) — полные спецификации с кодом сильная модель пишет сама по образцам существующих парсеров (WorkJournalParser.cs — эталон толерантности); в плане код не захардкожен, т.к. требуется сверка с фактическим `Kv`-типом и структурой существующего `BackupsParseResult`.

**Выход**: снапшоты панели несут BackupsDisabled/WalAgents/Evacuations/PgRotations/WorkJournals×3/Coordination×4; DTO отдают agents/evacuations/work/coordination; правило backups-disabled в движке; юнит-тесты зелёные.

**Проверка**: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` — Passed.

**Связь со spec**: §4 u10–u14 (+u07-источники), D9–D11; §7 таблица (строки снапшота и DTO), §12 «инварианты сохраняются».

---

## Шаг 5. Панель бэкенд: OverviewDto — ha/backups/reliability (u06), operations (u07), freshness (u08)

**Вход**: шаг 4 собран (в снапшотах есть PgRotations, WorkJournals, restore/drill уже в `ClusterBackupsInfo`, coordination — для полноты не нужен здесь).

**Действие** (сильная модель; файл `src/AdminPanel.Api/Inspection/OverviewQuery.cs`):

1. Расширить `OverviewDto` (новые positional-параметры в конец, со значениями по умолчанию, чтобы не ломать существующие вызовы):
   - `OverviewHaDto? Ha = null` → `record OverviewHaDto(int ScopesTotal, int WithoutLeader, int Unmatched)`;
   - `OverviewBackupsDto? Backups = null` → `record OverviewBackupsDto(bool Configured, OverviewBackupsHealthDto? Health, string? StorageState, long UsedBytes, long? QuotaBytes, OverviewEtcdSnapshotsDto? EtcdSnapshots)` + `record OverviewBackupsHealthDto(bool ApiOk, bool LiveOk, bool? ClusterOk)` + `record OverviewEtcdSnapshotsDto(bool Enabled, string? State)`;
   - `OverviewReliabilityDto? Reliability = null` → `record OverviewReliabilityDto(long? WorstRpoPotentialSec, int FullOnlyCount, int OffCount, OverviewLastFailoverDto? LastFailover)` + `record OverviewLastFailoverDto(long DurationSec, long AtUnix)`;
   - `IReadOnlyList<OverviewOperationDto> Operations = []` → `record OverviewOperationDto(string Domain, string Kind, string Cluster, string? Entity, string Detail, long? UpdatedUnix)` (domain: `pg|kafka|valkey|s3`; entity — бакет/топик/нода или null);
   - `OverviewFreshnessDto? Freshness = null` → `record OverviewFreshnessDto(OverviewFreshnessItemDto? Etcd, OverviewFreshnessItemDto? Pg, OverviewFreshnessItemDto? Kafka, OverviewFreshnessItemDto? Valkey, OverviewFreshnessItemDto? S3)` + `record OverviewFreshnessItemDto(long BuiltAtUnix)` (unix-секунды; null — снапшот ещё не тикал).
2. `OverviewMapper` — новые чистые функции (перенос TS-логики 1:1, D6; источники — `frontend/src/pages/OverviewPage.tsx` строки ReliabilityCard/BackupsCard/HaCard):
   - `MapHa(EtcdSnapshot)` — `scopesTotal = HaScopes.Count`, `withoutLeader = HaScopes.Count(s => s.Matched && s.LeaderName is null)` (только matched — как TS), `unmatched = HaScopes.Count(s => !s.Matched)`.
   - `MapBackups(EtcdSnapshot)` — из `snapshot.S3Storage` (S3StorageInfo — фактические поля ApiOk/LiveOk/ClusterOk/…) + `snapshot.BackupStorage` (BackupStorageInfo: UsedBytes/QuotaBytes/State) + `snapshot.EtcdSnapshots` (Enabled/State): configured = `S3Storage is not null`; health = null при отсутствии фактов; storageState — строка воркера или null; usedBytes/quotaBytes из BackupStorage; etcdSnapshots из EtcdSnapshots. Сверить фактические имена полей по `src/AdminPanel.Core/S3StorageInfo.cs` и `BackupsInfo.cs`.
   - `MapReliability(EtcdSnapshot snapshot)` — перенос `ReliabilityCalculator`-агрегации TS (фронт фильтрует Active-кластеры и шарды; те же входы — `ReliabilityCalculator` уже вызывается для `/api/reliability`, смотреть `src/AdminPanel.Api/Inspection/ReliabilityQuery.cs` и `src/AdminPanel.Core/ReliabilityCalculator.cs`): active-only worst RPO (`rpo.mode != off && rpoPotentialSec != null` → max), fullOnlyCount (`mode == full`), offCount (`mode == off`), lastFailover — самый недавний закрытый (max по `resolvedUnix ?? detectedUnix`, только с `durationSec != null`). Логика — точно как TS-строки 142–160 OverviewPage.tsx.
   - `MapOperations(EtcdSnapshot pg, KafkaSnapshot? k, ValkeySnapshot? v, S3Snapshot? s)` — источники: pg: не-ACTIVE бакеты (SYNCING/FROZEN/ABORTING — kind `move`, entity = bucket id, detail = `owner → target`), `MoveTickets` (kind `move-ticket`, detail = op), `PgRotations` (kind `rotation`, detail = requestedBy), restore из `Backups` c `state=RUNNING|REJOINING` (kind `restore`, entity = shard) и drill `RUNNING` (kind `drill`, entity = shard), `PgWorkerWork` с переходной фазой op (kind `work`, detail = `op/phase` — только не-терминальные фазы: не done); kafka: `Rotations`/`AdminRotations` (kind `rotation`), `CaRotations` (kind `ca-rotation`), `Rebalances` (kind `rebalance`), `Reassignments` (kind `reassignment`, detail = remaining/total), `Regens` (kind `regen`), `LifecycleTickets` (kind `lifecycle`, entity = topic, detail = op); valkey: `Rotations` (kind `rotation`, detail = role), `CaRotations` (kind `ca-rotation`); s3: `Rotations` (kind `rotation`). updatedUnix — из тикета/прогресса где есть. Имена kind — фиксированный словарь этого шага (выше), UI их не парсит.
   - `MapFreshness(EtcdSnapshot pg, KafkaSnapshot? k, ValkeySnapshot? v, S3Snapshot? s)` — `Etcd = Item(pg.Etcd.LastRefreshUnix …)` — фактическое поле свежести etcd в `EtcdStatus` (`LastRefreshUtc`-аналог; сверить `src/AdminPanel.Core/EtcdStatus.cs`); Pg/Kafka/Valkey/S3 — `BuiltAtUtc.ToUnixTimeSeconds()` (null, если снапшот null).
   - `Map(...)` — дополнить вызовами новых функций (все входы уже есть в хендлере).
3. `OverviewQueryHandler.Handle` — передать доменные снапшоты в `OverviewMapper.Map` (расширить сигнатуру Map: + kafka/valkey/s3 параметры) и заполнить новые поля через `with`.
4. Юнит-тесты — новый `src/tests/AdminPanel.UnitTests/OverviewMapperTests.cs` (AAA; входы — снапшоты-заготовки по образцу `TestSnapshots.cs`): ha (matched-без-лидера/безmatched/unmatched), backups (не настроено/настроено+storage+etcdSnapshots), reliability (worst по active-only, fullOnly/off, lastFailover = max(resolved??detected) среди закрытых), operations (переезд/заявка/ротация pg/kafka/valkey/s3 — по одному на домен), freshness (null-домены → null; BuiltAtUtc → unix). «Те же входы → те же числа»: для reliability продублировать кейс, который сейчас задаёт TS (числовые ассерты совпадают с ручным пересчётом).

**Подпункты для слабой модели**: кандидаты — объявления record'ов DTO (механические); сильная модель выделяет по образцу существующих record'ов OverviewQuery.cs. Логика мапперов — сильной модели (перенос TS 1:1 требует понимания).

**Выход**: `GET /api/overview` отдаёт ha/backups/reliability/operations/freshness; клиентские доборы с дашборда можно удалять (шаг 9).

**Проверка**: сборка 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build --filter FullyQualifiedName~OverviewMapper` — Passed.

**Связь со spec**: §3.6 u06–u08, D6–D7; §10 п.3 (обязательные тесты OverviewMapper).

---

## Шаг 6. Панель бэкенд: прокси u15/u16/u17 + restore + баннер-данные u10 (+ юнит-тесты прокси)

**Вход**: шаги 2–5 собраны.

**Действие** (сильная модель):

1. u15-прокси — `src/AdminPanel.Api/Operations/WorkersModule.cs`: новый эндпоинт `GET /api/workers/{worker}/effective-config`: команда `GetWorkerEffectiveConfigCommand(string Worker) : ICommand<JsonElement>` + хендлер `[InjectAsScoped] GetWorkerEffectiveConfigCommandHandler(IWorkerApiGateway api)` → `WorkerProxy.SendAsync<JsonElement>(api, worker, HttpMethod.Get, "/api/config", body: null, requestedBy: null, ct)` (паттерн `UpdateBackupsPolicyCommandHandler`; `SendAsync<T>` — `src/AdminPanel.Api/Operations/WorkerProblemDetails.cs`, десериализация Web-camelCase); модуль: успех → `Results.Ok(result.Value)` (JSON воркера как есть); ошибки — по существующей ветке `Error(result)` модуля (`WorkerApiUnavailableException` → 503, `WorkerNotFoundException` → 404, прочее → 503).
2. u16 — `src/AdminPanel.Api/Operations/BackupsCommands.cs`: `UpdateBackupsPolicyRequest` + `long? GarbageMinAgeSec`; `UpdateBackupsPolicyCommand` + `long? GarbageMinAgeSec`; `BuildBody`: `if (c.GarbageMinAgeSec is { } g) body["garbage_min_age_sec"] = g;`; валидация в `OperationsModule.cs` (эндпоинт PUT policy, строки ~197–216): добавить `|| (request.GarbageMinAgeSec is < 600)` + текст детекта «garbageMinAgeSec ≥ 600» (граница A1/A6).
3. u17 — `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs`: три `MapDelete` (`/api/kafka/clusters/{cluster}/app-password/rotate`, `.../admin-password/rotate`, `.../ca/rotate`) — команды-прокси по образцу `CancelKafkaRebalanceCommand` (`src/AdminPanel.Api/Operations/Kafka/RebalanceCommands.cs`): `CancelKafkaRotationCommand(string Cluster, string Path)` : `ICommand<KafkaRotationCancelledDto>` + `public sealed record KafkaRotationCancelledDto(string Cluster);` + хендлер → `WorkerProxy.SendAsync<KafkaRotationCancelledDto>(api, "kafkaworker", HttpMethod.Delete, $"/api/kafka/clusters/{command.Cluster}/{command.Path}", body: null, requestedBy: null, ct)` (204 без тела → dto default — так работает образец); модуль: успех → `Results.NoContent()`; ошибки — существующая ветка `Error(result)` модуля (503/ProblemDetails как есть). Аналогично `src/AdminPanel.Api/Operations/Valkey/ValkeyOperationsModule.cs`: `CancelValkeyRotationCommand` + `ValkeyRotationCancelledDto` для `.../password/rotate` и `.../ca/rotate` → `WorkerProxy.SendAsync<ValkeyRotationCancelledDto>(api, "valkeyworker", …)`; успех → 204. `requestedBy: null` на всех пяти DELETE — осознанно, закреплено в spec §7: DELETE-отмена заявку не создаёт (del без тела, воркер при del в etcd ничего не пишет), аудит самой отмены — host-лог воркера; ClaimsPrincipal в эти ветки не подключается.
4. Restore-прокси — `src/AdminPanel.Api/Operations/OperationsModule.cs`: `MapPost("/api/clusters/{cluster}/shards/{shard}/restore")` (тело `RestoreShardRequestDto(string Confirm, string? TargetTime)`; новый файл команды `RestoreShardCommand.cs` по образцу `OrphansCommands.cs`: `RestoreShardCommand(string Cluster, string Shard, string Confirm, string? TargetTime, string RequestedBy) : ICommand<JsonElement>`): хендлер → `WorkerProxy.SendAsync<JsonElement>(api, "pgworker", HttpMethod.Post, $"/api/clusters/{c}/shards/{x}/restore", body: new { confirm = c.Confirm, target_time = c.TargetTime }, requestedBy: c.RequestedBy, ct)`; успех → `Results.Accepted((string?)null, result.Value)` (тело DTO воркера как есть); ошибки — `Error(result)` (ProblemDetails 400/404/409/503 как есть).
5. u10-данные для баннера: в `OverviewDto` (шаг 5) добавить поле `BackupsDisabledDto? BackupsDisabled = null` (`record BackupsDisabledDto(long UpdatedUnix, string? UpdatedBy)`) + `OverviewMapper.Map` заполняет его из `snapshot.BackupsDisabled`; также `BackupStorageDto` (`BackupStorageQuery.cs`) + `BackupsDisabledDto? BackupsDisabled` в `MapStorage` (для баннера на `/backups-storage`).
6. Юнит-тесты:
   - `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs` (существующий) — расширить: effective-config прокси (мок gateway — образец в файле), 503 при отсутствии живых.
   - Новый `src/tests/AdminPanel.UnitTests/Operations/BackupsPolicyGarbageTests.cs`: `BuildBody` с GarbageMinAgeSec=600 → ключ есть; null → ключа нет; эндпоинт-валидация < 600 → 400 (если тестируется на уровне модуля — по образцу существующих тестов валидации; иначе тест BuildBody достаточен, эндпоинт-гвард — как в существующем покрытии policy).
   - Новый `src/tests/AdminPanel.UnitTests/Operations/CancelRotationProxyTests.cs` + `RestoreProxyTests.cs`: мок `IWorkerApiGateway` (образец `KafkaWorkerProxyTests.cs`) — успех → 204/202, недоступность → 503, ProblemDetails воркера — телом как есть.

**Подпункты для слабой модели**: 6.1–6.4 — команды+хендлеры+MapDelete/MapPost-ветки (механические, образцы в самом шаге: `UpdateBackupsPolicyCommandHandler` и `RotateKafkaPasswordCommand`-ветки); сильная модель готовит тексты подпунктов при исполнении.

**Выход**: все новые панельные эндпоинты живут; тесты зелёные.

**Проверка**: сборка 0w/0e; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` — Passed.

**Связь со spec**: §5 u15 (прокси-часть, D12), u16 (панельная часть), u17 (панельные DELETE), restore (D18); §7 таблица.

---

## Шаг 7. SPA: навигация 5 групп + DomainPageLayout + вкладки + страница шарда (u01, u03)

**Вход**: шаги 4–6 собраны (DTO с agents/evacuations/work готовы).

**Действие** (сильная модель, `frontend/`):

1. `frontend/src/layout/AppLayout.tsx`: `NAV_ITEMS` → структура групп: `[{ section: 'ОБЗОР', items: [{to:'/',label:'Дашборд'}] }, { section: 'ДОМЕНЫ', items: [{to:'/clusters',label:'PostgreSQL'},{to:'/kafka',label:'Kafka'},{to:'/valkey',label:'Valkey'},{to:'/s3',label:'S3-хранилища'}] }, { section: 'ДАННЫЕ И ЗАЩИТА', items: [{to:'/backups-storage',label:'Бэкапы'},{to:'/reliability',label:'Надёжность'}] }, { section: 'НАБЛЮДЕНИЕ', items: [{to:'/alerts',label:'Алерты'},{to:'/events',label:'События'},{to:'/ha',label:'Репликация'}] }, { section: 'ИНФРАСТРУКТУРА', items: [{to:'/workers',label:'Воркеры'},{to:'/etcd',label:'etcd-контур'}] }]`; рендер — `Stack`-секции: заголовок `Text size="xs" c="dimmed" tt="uppercase"` + `NavLink`-пункты (существующие props: active-логика и `AlertsNavCounters` у `/alerts` — сохранить). Заголовок страницы/кнопки — русские (D2): «Sign out» → «Выйти», «Panel is unavailable» → «Панель недоступна», «Retry» → «Повторить».
2. Новый `frontend/src/components/DomainPageLayout.tsx`: props `{ title: string; statusBadges?: ReactNode; modeBadges?: ReactNode; endpoints?: string | null; inFlightBadges?: ReactNode; actions?: ReactNode; tabs: { value: string; label: string; content: ReactNode }[]; hideTabs?: string[]; journal?: ReactNode }`; рендер: `Stack` → шапка (`Title order={2}` + бейджи + endpoints + actions), `Tabs` (value/label/panel), нижний блок `journal` (для доменов, где журнал — не вкладка). State-бейджи: NOT_INITIALIZED/TO_REMOVE — `Badge color="gray"` (штатная тишина серым), ACTIVE — teal.
3. `frontend/src/pages/ClusterDetailsPage.tsx` — перестроить на `DomainPageLayout`: вкладки `Шарды | Бакеты | Переезды | Бэкапы (срез) | Секреты | Журнал` (Бакеты скрыта при `!data.sharded` — как сейчас):
   - Шарды — существующий `ShardsTab` + строка шарда ссылкой на `/clusters/:c/shards/:x` (`Anchor component={Link}`);
   - Бакеты/Переезды — существующие `BucketsTab`/`MovesTab`; клик по не-ACTIVE бакету — переход на вкладку «Переезды» с подсветкой строки (u04: локальный state активной вкладки + `highlightBucket`, проброс в MovesTab, подсветка `style={{ background: 'var(--mantine-color-yellow-1)' }}`);
   - Бэкапы (срез) — новый `BackupsSliceTab` (`frontend/src/pages/cluster-details/BackupsSliceTab.tsx`): существующий query `backupsQueryKeys.storage`, фильтр по кластеру; RPO/WAL/полные (поля `BackupStorageDto.clusters[].shards[]`), policy (`BackupPolicyDto`) + ссылки на `/backups-storage` и `/reliability`; кнопка «Policy бэкапов» — модал шага 10;
   - Секреты — новый `SecretsTab` (`frontend/src/pages/cluster-details/SecretsTab.tsx`): кнопка «Сменить app-пароль» (перенос `RotateClusterSecretsButton` сюда, через `RotateSecretButton` шага 8), состав ротируемых ролей (текст: app / bucket_admin / bucket_mover), предупреждение о разрыве подключений, живая заявка — бейдж «ротация…» из `data.secretsRotationPending` (поле `ClusterDto.SecretsRotationPending`, добавлено шагом 4; источник — `/pgworker/rotations/<C>`); бейдж «ротация…» live-заявки дублируется и в шапке страницы;
   - Журнал — новый `JournalTab`: work (`WorkDto`) + heals (перенос `HealsTab` содержимого) + эвакуации (`Evacuations` — блок «Эвакуации»: шард/причина/план/State-бейдж DONE=gray/QUARANTINED=yellow);
   - «Стендовая топология» (`StandNodesBlock`) — остаётся блоком ПОД вкладками (статус-кво, D5); strict-переключатель (`StrictModeSwitch`) — в шапке (D4); «Добавить шард»/«Убрать шард» — на вкладке Шарды как сейчас; заявки в полёте — бейджи шапки («в очереди: N заявок» из `pendingMoves.length`).
4. `frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx` — на `DomainPageLayout`: вкладки `Брокеры | Топики | Группы | Журнал` (три существующих + новый `WorkJournalBlock`); бейджи шапки: RF/minISR, живые ротации из `data.rotation` (app), `data.adminRotation` (поле шага 4), `data.caRotation`, плюс `rebalance…`/`reassignment…`/`regen…` — каждое бейджем; кнопка «Отменить» у бейджа ротации — шаг 10 (u17).
5. `frontend/src/pages/valkey-cluster/ValkeyClusterDetailsPage.tsx` — вкладки `Нода | Журнал`; бейджи шапки: maxmemory/policy, живые ротации из `data.rotation` (пароль app|admin — role в бейдже) и `data.caRotation` (кнопка «Отменить» — шаг 10).
6. `frontend/src/pages/s3-cluster/S3ClusterDetailsPage.tsx` — вкладки `Ноды | Репликация | Связность и синхронность | Журнал`; бейдж etcdBackup.
7. Общий `frontend/src/components/WorkJournalBlock.tsx`: таблица op/phase/instance/updated/last_error + fail-серия (`failCount`/`failFirstUnix`/`retryNotBeforeUnix` — при наличии полей в DTO; pg — существующие heals-поля остаются в JournalTab).
8. Страница шарда (u03) — новый роут в `frontend/src/App.tsx`: `{ path: 'clusters/:cluster/shards/:shard', element: <ShardDetailsPage /> }`; новый `frontend/src/pages/shard/ShardDetailsPage.tsx`: данные `fetchClusterDetails(cluster)` (фильтр шарда по имени) + `fetchHaScope(\`${cluster}-${shard}\`)` (404/ошибка → HA-блок скрыт с пометкой «HA-scope не найден»); блоки: детали нод (существующие поля `ShardDto`), заявка ресурсов (`NodeRequestsDto`), runtime SQL (`ShardRuntimeDto` — при включённых пробах), HA-блок: leader (`leaderName`), optime (`optimeLeader`), members (`HaMemberDto`: роль/состояние/лаг/timeline), raw config свёрнуто (`Code`/`pre` в `Collapse`), кнопка «Пересоздать ноду» soft/hard (перенос UI с `HaScopeDetailsPage` — mutation `recreateNode(scope, node, mode)`), заявленные ресурсы scope (`Requests`).
9. `frontend/src/pages/HaScopeDetailsPage.tsx`: в начале компонента — если `scope.matched && scope.cluster && scope.shard` → `return <Navigate to={`/clusters/${scope.cluster}/shards/${scope.shard}`} replace />`; остальная страница — unmatched-детали (как сейчас).
10. `frontend/src/pages/HaPage.tsx`: заголовок «Репликация»; строка matched-скопа — ссылка на страницу шарда (вместо `/ha/:scope`), unmatched — на `/ha/:scope`.
11. `frontend/src/api/dto.ts`: синхронно новые поля (`ClusterDto.evacuations`, `BackupShardStorageDto.agents`, `KafkaClusterDto.work` и т.д. — по фактическим C#-DTO шагов 4–6); `frontend/src/api/queries.ts` — новые queryKeys: `shardHa: (cluster, shard) => ['ha-scopes', cluster, shard]`.

**Подпункты для слабой модели**: 7.1 (`DomainPageLayout.tsx` целиком — самодостаточный компонент с точными props выше), 7.2 (`WorkJournalBlock.tsx`), 7.3 (навигационные группы в AppLayout — точная структура выше). Сильная модель готовит тексты при исполнении; интеграция вкладок/страниц — сильной модели.

**Выход**: 5 групп навигации; доменные страницы на едином шаблоне с журналами; `/clusters/:c/shards/:x` работает; `/ha/:scope` matched → редирект.

**Проверка**: `cd frontend && npm run typecheck && npm run build` — зелёные; ручная смока-проверка не выполняется (Фаза 6 — код).

**Связь со spec**: §3.1 (D1, D2), §3.2 (D4, D5), §3.3 (u03, D3), §3.5 (бакет→переезд), §10 п.7–9.

---

## Шаг 8. SPA: общие компоненты действий (u02) + `utils/validation.ts`

**Вход**: шаг 7 собран.

**Действие** (сильная модель):

1. Новый `frontend/src/utils/validation.ts` — единственный источник границ (значения дословно из arch/adminpanel/02 §9.3/§10.3/§11.3/§12.3):
   ```ts
   export const CLUSTER_NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;
   export const LIMITS = {
     cpu: { min: 0.01, max: 64 },
     memGi: { min: 1, max: 65536 },
     diskGi: { min: 1, max: 65536 },
     replicas: { min: 1, max: 26 },
     buckets: { min: 1, max: 8192 },
     shards: { min: 1, max: 128 },
     kafkaBrokers: { min: 1, max: 9 },
     kafkaRf: { min: 1, max: 9 },
     kafkaPartitions: { min: 1, max: 1000 },
     kafkaRetentionMs: { min: 1, max: 2147483647 },
     valkeyMaxmemoryBytes: { min: 1 },
     s3Instances: { min: 0, max: 9 },
   } as const;
   export const VALKEY_POLICIES = ['noeviction','allkeys-lru','allkeys-lfu','allkeys-random','volatile-lru','volatile-lfu','volatile-random','volatile-ttl'] as const;
   export function validateCpu(v: number): string | null { ... }
   ```
   (+ аналогичные функции `validateMemGi/validateDiskGi/validateReplicas/validateBuckets/validateShards/validateKafkaRf/validateMinIsr(rf, minIsr)/validatePartitions/validateRetentionMs/validateValkeyMaxmemory(bytes, memGi)/validateS3Instances`; каждая возвращает строку ошибки или null; инвариант R3 `maxmemory < memGi` — в `validateValkeyMaxmemory`). Сверить фактические 8 значений policy по arch/02 §11.3 (список выше — канон Valkey; при расхождении — arch).
2. Новые общие компоненты `frontend/src/components/actions/`:
   - `DeleteEntityButton.tsx` — props `{ title: string; confirmText: string; expectedInput: string; canMutate: boolean; onConfirm: () => Promise<void> }`: кнопка red + `ConfirmModal` (ввод ожидаемой строки `expectedInput`).
   - `RotateSecretButton.tsx` — props `{ title: string; warningText: string; liveTicketText?: string | null; confirmText?: string; onRotate: () => Promise<void>; onCancel?: () => Promise<void> }`: кнопка + confirm; при `liveTicketText` — бейдж «ротация…» + опциональная кнопка «Отменить» (вызывает `onCancel`; ошибки 409/404 — текстом ProblemDetails через `ApiError`).
   - `ResourcesForm.tsx` (модал) — props `{ title: string; initial: { cpu?: number; memGi?: number; diskGi?: number }; applyNote: string; onSubmit: (v) => Promise<void> }`: поля с валидацией из `validation.ts` (import LIMITS), подпись способа применения `applyNote` (rolling/пересоздание).
   - `ConfirmModal.tsx` — props `{ opened, onClose, title, text, expectedInput?: string, confirmLabel, busy, onConfirm }`: если `expectedInput` задан — поле ввода, кнопка подтверждения заблокирована до совпадения.
3. Замена дубликатов (все существующие формы сохраняют эндпоинты/guards/тексты предупреждений — arch/03 §3.x–§12.3):
   - `DeleteClusterButton`/`DeleteKafkaClusterButton`/`DeleteValkeyClusterButton`/`DeleteS3ClusterButton` → `DeleteEntityButton` (удалить старые файлы, вызовы переключить);
   - `RotateClusterSecretsButton`, kafka `RotatePasswordButton`/`RotateAdminPasswordButton`/`RotateCaButton`, valkey `RotatePasswordButton`/`RotateCaButton`, s3 `RotateSecretsButton` → `RotateSecretButton`;
   - kafka `EditBrokerResourcesModal`, valkey `EditNodeResourcesModal`, s3 `EditNodeResourcesModal` → `ResourcesForm`;
   - `DeleteOrphanModal` (backups-storage) — на `ConfirmModal`;
   - модали создания (`ClusterCreateModal`, `CreateKafkaClusterModal`, `CreateValkeyClusterModal`, `CreateS3ClusterModal`, `AddShardModal`, `AddBrokerModal`, `TopicCreateModal`, `AddNodeModal`, `MoveBucketsModal`) — заменить локальные константы границ на импорты `validation.ts` (числа в компонентах удалить).
4. Русские подписи (D2): заголовки/кнопки/пустые состояния всех затронутых компонентов («Удалить», «Отменить», «Сменить пароль», «Ротация CA», «Подтвердите», «Загрузка…», «Нет данных» и т.п.); kind/state/домены — английские моноширинно.

**Подпункты для слабой модели**: 8.1 (`validation.ts` — точный код фрагментом выше + функции по спецификации), 8.2 (`ConfirmModal.tsx` — полный компонент по props), 8.3 (`DeleteEntityButton.tsx`), 8.4 (`ResourcesForm.tsx`), 8.5 (`RotateSecretButton.tsx`). Сильная модель пишет тексты подпунктов при исполнении (props выше самодостаточны); замена вызовов в страницах — сильной модели (контекст страниц).

**Выход**: единые компоненты действий + один модуль границ; дубли 4×Delete/6×Rotate/3×Resources удалены.

**Проверка**: `cd frontend && npm run typecheck && npm run build` — зелёные; `grep -rn "DeleteKafkaClusterButton\|RotateAdminPasswordButton" frontend/src` — только новые обёртки/0 вхождений старых имён файлов.

**Связь со spec**: §3.4 (u02, D17), §10 п.8–12.

---

## Шаг 9. SPA: дашборд u05–u08 + `/events` u09 + `/workers` u14/u15-UI

**Вход**: шаги 5–8 собраны (`/api/overview` несёт все агрегаты; компоненты готовы).

**Действие** (сильная модель):

1. `frontend/src/api/dto.ts`: `OverviewDto` + `ha/backups/reliability/operations/freshness/backupsDisabled` (зеркало шага 5/6); `WorkerViewDto` + `coordination`.
2. `frontend/src/pages/OverviewPage.tsx`:
   - удалить useQuery `haScopes`/`backups`/`reliability` (D6) и их доборы; `BackupsCard`/`ReliabilityCard`/`HaCard` — переключить на `data.backups`/`data.reliability`/`data.ha` (рендер сохранить, числа — из сервера);
   - новая `ValkeyCard` (порт `KafkaCard`: `data.valkey`, null → «Снапшот valkey ещё не собран»);
   - `MovesSection` → `OperationsSection` (таблица `data.operations`: Домен | Тип | Кластер | Сущность | Детали | Обновлено; пусто → «Нет активных операций»); старую таблицу activeMoves удалить (переезды — подмножество операций);
   - карточки доменов: строка свежести (`data.freshness.pg…` — возраст, жёлтый при старше 3×interval — вычисление по `usePollingIntervalMs()`); при `data.backupsDisabled` — жёлтый `Alert`-баннер «Бэкапы остановлены (глобальный стоп)» над карточками (u10);
   - секция «Требуют оператора» рядом с лентой алертов: `alerts.filter(a => a.remedy === 'OperatorApi' || a.remedy === 'OperatorRunbook')` (u18-дашборд);
   - заголовок «Обзор», карточки — русские заголовки («Надёжность», «Бэкапы», «Алерты», «Кластеры»).
3. `frontend/src/layout/AppLayout.tsx`: `StaleBadge` → новый `frontend/src/layout/FreshnessBadges.tsx`: 5 мини-бейджей (etcd/pg/kafka/valkey/s3) из `queryKeys.overview`-ответа `data.freshness`: `Badge size="xs"` серый «pg: 3с» / жёлтый «pg: stale»; null-домен — серый «pg: —». Файл `StaleBadge.tsx` удалить, импорт заменить (общий `Stale`-бейдж удаляется — u08).
4. `/events` (u09, механика A5): новый модуль `frontend/src/events/`:
   - `frontend/src/events/eventsStore.ts`: in-memory store — `useSyncExternalStore`-хранилище `EventItem[]` (cap 500, новые сверху): `EventItem = { id: string; tsUnix: number; domain: 'pg'|'kafka'|'valkey'|'s3'|'etcd'; entityKind: 'cluster'|'shard'|'node'|'broker'|'bucket'|'topic'|'ticket'|'heal'|'evacuation'|'rotation'|'instance'; entity: string; type: 'state-change'|'appeared'|'gone'|'outcome'; from?: string; to?: string; detail?: string }`.
   - `frontend/src/events/eventsDiff.ts`: чистые функции сравнения prev→next снимков (типы DTO): `diffClusters(ClusterSummaryDto[], ClusterSummaryDto[])`, `diffClusterDetails` (шарды/бакеты/ноды — state-переходы, появление/исчезновение), `diffHa`, `diffKafka` (кластеры/брокеры/топики + тикеты: появление = «поставлена», исчезновение = «исполнена или отвергнута», detail по `work.lastError` при наличии; `ticketOutcomes` — исход напрямую), `diffValkey`, `diffS3`; heals — новые записи по ts; эвакуации — появление/изменение/исчезновение; freshness НЕ эмитим.
   - `frontend/src/events/EventsProvider.tsx`: `useEffect(() => queryClient.getQueryCache().subscribe(event => { if (event.type === 'updated' && event.action.type === 'success') { …снять prev из event.action.data.previous… } })` — по ключам `['overview'] | ['clusters'] | ['clusters', name] | ['ha-scopes'] | ['kafka-clusters'] | ['valkey-clusters'] | ['s3-clusters'] | ['backups-storage']` дифф prev→next и `emit(...)`. Смонтировать `<EventsProvider>` внутри `AppLayout` (после guard'а сессии, чтобы жил только в открытой вкладке; перезагрузка = пустая лента — persistence нет).
   - `frontend/src/pages/EventsPage.tsx`: роут `{ path: 'events', element: <EventsPage /> }` в App.tsx; таблица-лента (Время | Домен | Тип сущности | Сущность | Событие from→to | Детали), фильтры: `Select` домена (Все/pg/kafka/valkey/s3/etcd) + `Select` entityKind; клик по событию — переход по таблице маршрутов `alertRoutes.ts` (шаг 10, единая таблица); подпись «Лента живёт в открытой вкладке; после перезагрузки пуста».
5. `/workers` (u14+u15-UI), `frontend/src/pages/WorkersPage.tsx`:
   - `WORKER_TITLES` + `valkeyworker: 'ValkeyWorker (Valkey)'` (u05, D2);
   - в `WorkerCard`: блок «Координация»: строка «Лидер: `<instance>`» (или «Лидер: —») + таблица claims «Кто держит кластер» (Кластер → Инстанс, SinceUnix); из `view.coordination`;
   - раскрывающийся блок «Эффективный конфиг»: `Button` «Показать конфиг» → useQuery `workerQueryKeys.effectiveConfig(worker)` → `fetchWorkerEffectiveConfig(worker)` (новая функция в queries.ts: `GET /api/workers/{worker}/effective-config`); рендер — дерево групп ключ-значение (`Table` по группам: thresholds/loops/…) + пометка «env, применяется рестартом воркера» (поле `source === 'env'`); read-only, форм нет; 503 → «Живые инстансы не найдены».

**Подпункты для слабой модели**: 9.1 (`eventsStore.ts` по точной спецификации типа выше), 9.2 (`FreshnessBadges.tsx`), 9.3 (`ValkeyCard` — порт KafkaCard с русскими подписями). Диффы и EventsProvider — сильной модели (требуют знания DTO).

**Выход**: дашборд на серверных агрегатах (один запрос `/api/overview`), 5 бейджей свежести, лента `/events`, координация и конфиг на `/workers`.

**Проверка**: `cd frontend && npm run typecheck && npm run build` — зелёные.

**Связь со spec**: §3.6 u05–u08 (D6–D8), §3.7 u09, §4 u14-UI/u15-UI, §10 п.14–16.

---

## Шаг 10. SPA: алерты u18 + deep-links u04 + restore/policy/cancel-UI (u16/u17/restore) + u10-баннер

**Вход**: шаги 6–9 собраны.

**Действие** (сильная модель):

1. Единая таблица маршрутов — новый `frontend/src/utils/alertRoutes.ts`: `export function alertRoute(kind: string, target: string): string | null` — маппинг kind-префикса → маршрут: `kafka-*` → `/kafka/:c`; `valkey-*` → `/valkey/:c`; `s3-*` → `/s3/:c`; pg-кластерные (`shard-*`, `move-*`, `bucket-*`, `cluster-*`, `provision-stuck`, `cluster-incomplete`, `replica-*`, `slot-*`, `sync-standby-missing`, `ha-*`, `wal-*` без per-shard target-формата, `backup-*` с кластерным target) → `/clusters/:c`; `backup-*` per-shard (target вида `<C>/<X>` или `<C>/shard<X>` — сверить фактический формат target по коду правил `src/AdminPanel.Core/Alerting/Rules/Backup*Rule.cs`) → `/backups-storage/:c/:x`; probe-алерты (`probe-failed`) → `/ha`; `etcd-*`/`snapshot-stale`/`snapshot-export-*` → `/etcd`; `worker-*` → `/workers`; `backups-disabled` → `/backups-storage`; непокрытые → null (без ссылки). Точный разбор target — по коду правил (исполнитель снимает формат target каждого движка: pg `AlertEngine`/`KafkaAlertEngine`/`ValkeyAlertEngine`/`S3AlertEngine`); `EventsPage` использует ту же функцию.
2. `frontend/src/pages/AlertsPage.tsx` (u18): группировка — секции по `remedy` («Автоматика воркера» WorkerAuto / «Через API» OperatorApi / «Runbook» OperatorRunbook; null-remedy → «Прочее»); фильтр по домену (клиентский, по kind-префиксу — D15: `kafka-*`/`valkey-*`/`s3-*`/остальное «pg и инфраструктура»); переключатель «Требуют оператора» = remedy ∈ {OperatorApi, OperatorRunbook}; строки кликабельны (`Anchor`/`UnstyledButton` → `alertRoute(kind, target)`); severity/kind-фильтры сохранить. Заголовок «Алерты».
3. `frontend/src/pages/BackupsStoragePage.tsx`:
   - при `data.backupsDisabled` — жёлтый баннер «Бэкапы остановлены (глобальный стоп)» (u10);
   - кнопка per-кластер «Policy бэкапов» → единый модал (замена `BackupsPoolModal`): новый `frontend/src/pages/backups-storage/PolicyBackupsModal.tsx` — поля: retentionDays/Weeks/Months, fullMaxAgeSec, verifyOnCreate, drillIntervalDays, garbageMinAgeSec (u16; подпись «Минимальный возраст мусора (сек)», дефолт 600 при null в policy), секция s3 (pool/writeMaster/writeReplica — существующая логика); submit — `updateBackupsPolicy` ПОЛНЫМ телом (канон замещения целиком; текущие значения из policy-ключа/дефолтов); файл `BackupsPoolModal.tsx` удалить;
   - «Policy бэкапов» — вторая кнопка на вкладке «Бэкапы (срез)» pg-кластера (шаг 7.3, тот же модал).
4. Restore-UI — `frontend/src/pages/backups-storage/BackupsShardDetailsPage.tsx`: кнопка «Восстановить из бэкапа» → новый `RestoreShardModal.tsx` (та же папка): поле `targetTime` (опционально, RFC3339, placeholder `2026-10-31T12:00:00Z`), подтверждение вводом имени шарда (по образцу delete-сироты), предупреждение «операция разрушающая и долгая; шард восстанавливается из бэкапа»; submit — новая `restoreShard(cluster, shard, confirm, targetTime?)` в queries.ts (`POST /api/clusters/{c}/shards/{shard}/restore`); успех 202 — баннер «Заявка restore поставлена»; блок «Приёмники WAL» (u11): таблица `agents[]` (Нода | Роль | State-бейдж: streaming=green, waiting_*=yellow, failed_permanent=red | Ошибка | Штамп).
5. Cancel-UI (u17): у бейджа «ротация…» (app/admin/ca) на kafka-странице и (пароль/CA) на valkey-странице — кнопка «Отменить» через `RotateSecretButton.onCancel`; confirm «Начатая ротация доедет; отменяется только не начатая заявка»; 409/404 — текстом ProblemDetails (`ApiError`); новые функции `cancelKafkaRotation(cluster, kind: 'app-password'|'admin-password'|'ca')`, `cancelValkeyRotation(cluster, kind: 'password'|'ca')` в queries.ts (DELETE).
6. Лента дашборда алертов: строки кликабельны (`alertRoute`).

**Подпункты для слабой модели**: 10.1 (`alertRoutes.ts` — после того как сильная модель сняла форматы target с кода правил и выписала таблицу целиком), 10.2 (`RestoreShardModal.tsx` по точным props), 10.3 (`PolicyBackupsModal.tsx`). Сильная модель формирует тексты при исполнении.

**Выход**: алерты сгруппированы/фильтруемы/кликабельны; restore/policy/cancel-формы на месте; баннер стоп-бэкапов на двух гранях.

**Проверка**: `cd frontend && npm run typecheck && npm run build` — зелёные.

**Связь со spec**: §3.5 (u04), §3.8 (u18, D15), §5 u16-UI (D14), u17-UI, restore-UI (D18), §4 u10-UI/u11-UI; §10 п.10–13, 16–17.

---

## Шаг 11. Финальные чеки + чек-лист docs/06 + фиксация мерж-гейта

**Вход**: шаги 1–10 выполнены.

**Действие** (сильная модель):

1. `docs/adminpanel/06-ui-analysis.md`: §4.2 (дерево) и §5 (таблица сопоставления) — под фактическую структуру после переделки (5 групп, страница шарда, /events, вкладки-журналы, «Репликация»); §3.4 — снять пункты, закрытые u10–u14 (глобальный стоп, приёмники WAL, эвакуации, work-журналы kafka/valkey/s3, координация — как наблюдаемость из панели); §6 — удалить пункты u01–u19 (u20 остаётся с пометкой «после t32»). Исторические пассажи не вносить (AGENTS.md: только текущее состояние).
2. Полный прогон (каждая серия — после финальной строки предыдущей; лог каждой команды полностью в файл):
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 warnings / 0 errors (TreatWarningsAsErrors);
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` — Passed;
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release --no-build` — Passed;
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Release --no-build` — Passed;
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build` — Passed;
   - `cd frontend && npm ci && npm run typecheck && npm run build` — зелёные; бандл ложится в `src/AdminPanel.Api/wwwroot`.
3. Сверка покрытия по чек-листу spec §10 п.1–4 (сборка/статика/юниты) — все зелёные; п.5 (docker-E2E) и п.6 (dev-стенд) — МЕРЖ-ГЕЙТ, НЕ шаг Фазы 6: зафиксировать в итоговом отчёте задачи, что мерж-гейт обязан прогнать docker-E2E серии (PgWorker: build → `dotnet run --project src/tests/PgWorker.E2ePrepare` → `dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`; Kafka/Valkey — их E2E-серии по канону AGENTS) и зачистку контейнеров/сетей после каждой серии.
4. Фейлы тестов вне области задачи — НЕ чинить: пункт в `arch/roadmap/bugs.md` (состав сведений — по правилам трека), работа продолжается.

**Подпункты для слабой модели**: нет.

**Выход**: все чеки зелёные; docs/06 актуализирован; отчёт задачи фиксирует мерж-гейт.

**Проверка**: сами команды п.2 — их вывод и есть проверка.

**Связь со spec**: §10 (критерии приёмки), §8 (06-ui-analysis), §12 (порядок работ, E2E — гейт).

---

## Матрица покрытия

| spec | шаг |
|---|---|
| §8 arch/docs правки | 1 (arch), 11 (docs/06) |
| u01 навигация/шаблон | 7 |
| u02 компоненты+validation | 8 |
| u03 шард/HA-слияние | 7 |
| u04 deep-links | 7 (бакет→переезд), 10 (alertRoutes/лента) |
| u05 карточка Valkey/WORKER_TITLES | 9 |
| u06 агрегация Overview | 5 |
| u07 активные операции | 5 (сервер) |
| u08 свежесть | 5 (сервер), 9 (UI) |
| u09 /events | 9 |
| u10 стоп бэкапов | 4 (чтение/алерт), 6 (DTO-поля), 9/10 (баннеры) |
| u11 приёмники WAL | 4 (парсер/DTO), 10 (UI) |
| u12 эвакуации | 4 (парсер/DTO), 7 (JournalTab) |
| u13 work-журналы | 4 (тик/DTO), 7 (вкладки) |
| u14 координация | 4 (снапшот/DTO), 9 (UI) |
| u15 конфиг | 2 (воркеры), 6 (прокси), 9 (UI) |
| u16 policy | 3 (воркер), 6 (панель), 10 (форма) |
| u17 отмена ротаций | 3 (воркеры), 6 (прокси), 10 (кнопки) |
| u18 алерты | 9 (дашборд-секция), 10 (группировка) |
| u19 контракт | 1 |
| restore | 6 (прокси), 10 (модал) |
| E2E | 11 п.3 — мерж-гейт, не Фаза 6 |
