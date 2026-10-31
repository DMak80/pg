# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.

- **`t03-doorman-mandatory`** — doorman в коде: пулер обязателен, режим
  «нода без пулера» снят (контракт уже изменён arch-first: arch/14 §2.1/§2.4
  п.5/§8/§9 R1, arch/17 S4). Код PgWorker: удалить `EnableDoorman`
  (Options/appsettings/Program), ветвления драйвера — env
  DOORMAN_CONFIG/PGW_DOORMAN_PORT и публикация 6432 ВСЕГДА
  (`ClusterDriver`: параметр enableDoorman, PortsMatchPlan), ветку
  `EnableDoorman=false` в `MasterKeyReconciler.IsCurrent`, zero-ignore
  doorman-порта в `PortPlanConvergence.MatchesFact`, EnableDoorman-мотивацию
  в комментарии `ClusterSecretRotator.ResolveMasterAsync`. Образ ноды:
  бинарник pg_doorman — обязательный build-arg `DOORMAN_URL` (пин релиза;
  проект github.com/ozontech/pg_doorman, есть контейнер-образ
  ghcr.io/ozontech/pg_doorman): `docker/node/Dockerfile` — fail-fast при
  пустом DOORMAN_URL (сейчас условный), `docker-entrypoint-doorman.sh` —
  вместо молчаливого «not starting» отказ старта контейнера, комментарий
  supervisord. Пин в сборках pgworker-node:dev|e2e: стенд
  `dev-stand/adminpanel/checks/00-up.sh`, E2E EnsureNodeImage
  (`E2eEnvironment`), ранбук `docs/runbook.md`. Тесты: снять
  env-переопределения `PgWorker__Docker__EnableDoorman` (E2eEnvironment и
  E2eSecondInstanceScenarios — false, E2ePgtuneScenarios — true),
  переписать комментарии «master-ключ host:0 при EnableDoorman=false» в
  сценарных файлах E2e (по grep EnableDoorman: Drill/Move/Scale/Scenarios/
  WalStream*/TliSwitch/Pgtune/AppSecret), юниты: ClusterDriverTests (ветка
  без пулера), ClusterSecretRotatorTests (деградировавший кейс host:0),
  PortPlanConvergenceTests (запись без пулера — теперь расхождение).
  Мерж-гейт: docker-E2E PgWorker на свежем Release.

- **`t04-api-client-cert-eku`** — EKU clientAuth в валидации клиентских
  сертов /api (сейчас `TlsChain.ValidateChain` EKU не проверяет: серверные
  серты той же CA годятся как клиентские для /api). `TlsChain.ValidateChain`
  — общий валидатор (15 вызывателей, валидирует и серверные серты:
  DockerEngineFactory, PatroniRoleProbe, PgWorker Program.cs, AdminPanel
  WorkerTlsHandler, RespProbe×2, ValkeyConnection×2, KafkaProtocolClient,
  MasterLeaseCommand, E2eEnvironment, PatroniRestProbe, FakeBackupDeps) —
  НЕ трогать; `ClientCertAuthMiddleware` — единственный клиентский путь,
  нужен ОТДЕЛЬНЫЙ клиентский валидатор (в клиентской ветке добавлять
  `chain.ChainPolicy.ApplicationPolicy.Add("1.3.6.1.5.5.7.3.2")`). Факты
  PKI: gen.sh panel/seed — EKU clientAuth уже; тест-хелпер Pg —
  [serverAuth, clientAuth] (MtlsApiTests.cs:58–59); Kafka/Valkey/S3
  тест-хелперы — БЕЗ EKU (править в этой задаче); E2E-серты —
  serverAuth-only (E2eWorkerCertScenarios.cs:212) — правка тянет E2E-PKI
  (граница §3.4 задачи t04-open-metrics-healthz-facet, потому не сделано
  в ней).

- **`t05-stand-tls-hardening`** — хардненинг стенда: (1) пофайловые маунты
  TLS вместо bind всего каталога `deploy/tls` (внутри `ca.key` — приватный
  ключ per-install CA): `dev-stand/adminpanel/docker-compose.yml:182,204,238,271,301,341`
  (`../../deploy/tls:/tls(-workers):ro`) — панели/прометеусу/воркерам
  класть только нужные файлы (`ca.pem`, `server.*`, `panel.*`/`seed.*`)
  без избыточных приватных ключей; (2) опциональный bind `127.0.0.1`
  хост-публикаций API воркеров отдельным env-флагом
  (`deploy/docker-compose.yml:245,264,278,288,296,306,317,327` — сейчас на
  `0.0.0.0`): сужение границы доверия открытой грани (`/healthz`, `/metrics`)
  до периметра docker-хоста (arch/18 §5.2); ломает
  `host.docker.internal`-потребителей (панель) — по умолчанию выключен,
  включается осознанно.

