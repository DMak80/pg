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
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t07-unify-docker-engine`** — унификация docker-движков: три копии
  DockerEngine/ClusterDriver (PgWorker.Docker — SSH-туннели/TLS-docker;
  KafkaWorker.Docker; ValkeyWorker.Docker — копия kfw, t02) разошлись
  (diff kfw/pg ~492 строк). Вынос в общую Shared-сборку с сохранением
  Pg-специфики (SSH/TLS) как опций. Мерж-гейт: полный docker-E2E
  Pg+Kfw+Valkey (все три домена).
- **`t14-pg-e2e-rebuild-race`** — тест-рейс «stale-RUNNING» в pg-E2E
  `Acceptance_Scenario_Ac2_To_Ac7` (AssertFailoverRebuildAsync,
  tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs, маркер
  `ac5-rebuild`). Причина (разбор мерж-гейта t07, 2026-09-20,
  детерминированно 2/2): ассерт трактирует etcd state==RUNNING как
  «нода пересоздана», но ключ ещё «RUNNING» с provisioning — супервизор
  (тик ≤ 5 с) не успевает записать UNREACHABLE в окно быстрого
  failover'а (сценарий сам делает `docker stop` лидера :285, реплика
  поднимается за ~1.2 с), `WaitPhaseAsync("ac5-rebuild", …)` возвращается
  мгновенно (~0.3 с) на до-failover значении, после чего одиночный
  docker-inspect видит остановленный сценарием исходный контейнер
  «exited» → FAIL (сценарий падает за ~2 мин, не простояв 300 с
  бюджета). Не регрессия t07: движок в упавшей цепочке не участвует
  (stop/inspect — docker CLI теста, записи state — супервизор домена,
  промоушен — patroni образа); построчный диф Shared.Docker/DockerEngine
  против pg-оригинала — только добавления (kfw/vwk + суперсет §7);
  соседние серии на том же движке зелёные (kfw 69/69, vwk E2E 2/2);
  подтверждено код-ревью t07. Артефакты: teardown-дамп
  pgw-e2e-artifacts-c471a894eb7b41b18dd85973fdd75ef5 (docker-логи/inspect
  — телеметрия E2eScenarios, c49d662), лог прогона /tmp/t07-pg-e2e.log.
  Фикс-подход: ждать факт перехода ноды (UNREACHABLE/REBUILDING →
  RUNNING), а не факт RUNNING; либо ретраи docker-инспекции по образцу
  стабильной проверки ac5-replica-catchup; после фикса — полный pg-E2E
  прогон на свежем Release.
