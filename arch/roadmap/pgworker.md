# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../pgworker/README.md](../pgworker/README.md)).

## Задачи

- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.

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
  [serverAuth, clientAuth] (MtlsApiTests.cs:58–59); Valkey/S3
  тест-хелперы — БЕЗ EKU (править в этой задаче); Pg TestPki и Kafka
  ClusterPki.IssueBrokerCertificate — уже с clientAuth; E2E-серты —
  serverAuth-only (E2eWorkerCertScenarios.cs:212) — правка тянет E2E-PKI.

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

- **`t06-rotate-backup-creds`** — ротация per-cluster backup-кредов
  (`/clusters/<C>/backup_password`, роль `backup_exec`): в общий тикет
  ротации (ETCD-FIRST, arch/14 §5 I — app/bucket_admin/bucket_mover/
  rest + dsn) backup-креды t05 НЕ включил — пароль статичен с ensure
  (arch/19 §7). Канон той же модели: NEW в etcd в начале окна →
  потребители (стрим-сессии PgNode, джобы полного — креды читают из
  etcd на сессию/операцию, рестарты нод не нужны) подхватывают NEW →
  финальный ALTER ROLE backup_exec на мастерах каждого шарда («etcd —
  источник, PG следует», окно/флаг/гварды — по образцу §5 I; pg_hba не
  меняется — роль та же). Развилки на spec: перенос журнального флага
  окна (отдельный от rest_window_open или общий «окно ротации»),
  порядок потребители→ALTER (потребители могут работать на OLD/NEW
  параллельно — одна роль, два пароля в окне невозможны: определить
  порядок без разрыва стримов). ← t05-node-secret-push.
