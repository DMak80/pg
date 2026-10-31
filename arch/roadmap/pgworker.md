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

- **`t05-node-secret-push`** — нода без секретов в env: доставка пушем
  от воркера через загрузочного агента (дизайн пользователя, arch-first
  внесён: arch/14 §4 «Планируемое», arch/13 §4). Контейнер ноды
  создаётся БЕЗ секретов в env (только неконфиденциальное:
  DOORMAN_CONFIG-шаблон, адреса etcd); supervisord поднимает ТОЛЬКО
  walreceiver, тот поднимает mTLS-сервер (сёрт ноды — материалы
  per-install REST-CA, кандидат); PgWorker — инициатор канала: он создал
  контейнер, знает хост/порты, контролит поднятие (adopt/supervise) и
  ИНИЦИИРУЕТ на walreceiver проверку и замену (триггер по mTLS-каналу:
  порт внутри docker-сети кластера, на хост не публикуется); нода про
  pgWorker ничего не знает и ничего не инициирует. Данные — из etcd,
  самим walreceiver (доступ к etcd у него уже есть): воркер пишет в
  etcd серверный серт doorman (серт+ключ; per-cluster секреты —
  кандидат на тот же путь), walreceiver забирает сам, пишет файлами
  0600, рендерит конфиги (Patroni-bootstrap, DOORMAN_CONFIG), стартует
  spilo/pg_doorman; walreceiver — менеджер doorman: ведёт его конфиг и
  серты, по триггеру проверки/замены перечитывает etcd и выполняет
  релод/рестарт пулера. Целевая схема серта заменяет
  entrypoint-генерацию self-signed (текущий канон — до мержа t05).
  Доступность: restart контейнера — конфиги/серты из writable-слоя
  (воркер не нужен; перечитывание etcd — по триггеру); recreate делает
  только воркер — он же тут же инициирует. Развилки на spec: роль
  агента (walreceiver vs отдельный бинарник), bootstrap PG файлами без
  PGPASSWORD_SUPERUSER (отход от upstream Spilo), доставка per-install
  SuPassword при модели «etcd-only» (Д7: per-install в etcd не живут —
  исключение-ключ / env / данные по триггеру), источник серта
  (per-install CA c SAN — кандидат) и верификация клиентами, watch
  etcd-ключа вместо/вместе с триггером, ротация rest_password без
  rolling-recreate, защита кэша (0600/шифрование).
  Затронуто: docker/node (entrypoint/supervisord), PgWorker.WalReceiver
  (агент), ClusterDriver (env без секретов + push-клиент), arch/13 §2
  (firewall-строка канала). Не зависит от t02-external-secret-manager
  (то — хранение per-cluster секретов, это — доставка в ноду).
  Мерж-гейт: docker-E2E PgWorker на свежем Release + негативная
  проверка «в docker inspect ноды нет секретов».


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
