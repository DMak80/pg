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

- **`t04-open-metrics-healthz-facet`** — открытая грань `/healthz` +
  `/metrics` воркеров в коде и конфигах (контракт уже изменён arch-first:
  arch/14 §1.1, arch/16 §1.1, arch/21 §1.1, arch/24 §1.1, arch/13 §2,
  arch/18 §3/§5.2/§6, adminpanel/02 §2.3.1/§2.3.2/§9.9): грань — один
  HTTPS-порт `:8080`, клиентский сертификат (mTLS) обязателен только на
  `/api`. Код воркеров (паттерн TLS-грани `Shared.Tls`, Program.cs
  PgWorker/KafkaWorker/ValkeyWorker/S3Worker): клиентский сертификат на
  TLS-хендшейке — опциональный; требование валидного клиентского серта —
  middleware/авторизационная политика ТОЛЬКО на префиксе `/api` (без
  серта — 401); `/healthz` и `/metrics` — отвечают без клиентского серта.
  Панель без изменений: `IWorkerApiGateway` шлёт `panel.crt` на `/api`
  (mTLS-проксирование), healthz-тики — те же URL. Стенд/deploy:
  `deploy/tls/gen.sh` — прекратить выпуск клиентских `prometheus.crt/key`
  и `healthcheck.crt/key`;
  `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` — джобы
  воркеров: `tls_config` только `ca_file`, без `cert_file`/`key_file`;
  docker-HEALTHCHECK воркеров (deploy-компоуз/Dockerfile) — без
  клиентского серта; чеки стенда с прямыми пробами `/healthz`//`/metrics`
  (35-worker-second-instance, 57-kafka-worker-health, 65-metrics) —
  `curl --cacert ca.pem` без `--cert/--key` на этих пробах (пробы `/api` —
  с клиентским сертом, как сейчас); `dev-stand/adminpanel/README.md` —
  снять переходную оговорку про t04. Тесты: интеграционные/WAF — `/api`
  без клиентского серта = 401, `/metrics` и `/healthz` без серта = 200;
  Shared.Metrics-интеграция (§6 arch/18) — без авторизации на `/metrics`.
  Мерж-гейт: docker-E2E PgWorker на свежем Release + чеки стенда
  65-metrics.sh и 43-etcd-ha.sh.

- **`t05-node-secret-push`** — нода без секретов в env: доставка пушем
  от воркера через загрузочного агента (дизайн пользователя, arch-first
  внесён: arch/14 §4 «Планируемое», arch/13 §4). Контейнер ноды
  создаётся БЕЗ секретов в env (только неконфиденциальное:
  DOORMAN_CONFIG-шаблон, адреса etcd); supervisord поднимает ТОЛЬКО
  walreceiver, тот поднимает mTLS-сервер (сёрт ноды — материалы
  per-install REST-CA, кандидат); PgWorker — инициатор канала: он создал
  контейнер, знает хост/порты, контролит поднятие (adopt/supervise) и
  ПУШИТ бандл паролей (состав — SuPassword/standby/bucket_admin/mover +
  per-cluster секреты кластера + серверный серт doorman (серт+ключ) —
  на spec); нода про pgWorker ничего не знает и ничего не инициирует.
  Агент пишет секреты файлами 0600, рендерит конфиги
  (Patroni-bootstrap, DOORMAN_CONFIG) и стартует spilo/pg_doorman;
  walreceiver — менеджер doorman: ведёт его конфиг и серты, при замене
  серта/конфига выполняет релод/рестарт пулера. Целевая схема серта
  заменяет entrypoint-генерацию self-signed (текущий канон — до мержа
  t05). Доступность: restart контейнера — бандл из
  writable-слоя (воркер не нужен); recreate делает только воркер — он
  же тут же пушит; канал — порт внутри docker-сети кластера, на хост
  не публикуется. Развилки на spec: роль агента (walreceiver vs
  отдельный бинарник), bootstrap PG файлами без PGPASSWORD_SUPERUSER
  (отход от upstream Spilo), источник серта doorman (per-install CA
  c SAN — кандидат, vs self-signed от воркера) и верификация его
  клиентами, ротация rest_password пушем вместо rolling-recreate,
  защита кэша (0600/шифрование).
  Затронуто: docker/node (entrypoint/supervisord), PgWorker.WalReceiver
  (агент), ClusterDriver (env без секретов + push-клиент), arch/13 §2
  (firewall-строка канала). Не зависит от t02-external-secret-manager
  (то — хранение per-cluster секретов, это — доставка в ноду).
  Мерж-гейт: docker-E2E PgWorker на свежем Release + негативная
  проверка «в docker inspect ноды нет секретов».

