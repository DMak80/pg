# MR-ревью `feat-t25-wal-receiver-in-pgnode` — «t25: WalReceiver внутри PG-контейнера ноды»

**Ветка:** `feat-t25-wal-receiver-in-pgnode` · **База:** `main` (merge-base `b79d46f8`, main влит) · **Задача dev-flow:** t25-wal-receiver-in-pgnode

> **Вердикт: можно мержить.** Все пункты ревью (4 Major, 14 Minor, Nit-группы) исправлены коммитом в ветке (fix(t25) поверх bd5d379d); пункты удалены из документа по правилу закрытия .my-ревью. Верификация: build 0/0; юниты PgWorker 1231 + NodeTools 15 (новый проект PgWorker.NodeTools.UnitTests); Kafka/Valkey/S3/AdminPanel/SdGenerator/Shared — зелёные; docker: WalStreamProcessTests 28/28, MasterLease (Failover+Https) 4/4, NodeWalReceiverTests 3/3 (свежий образ per-worktree тега).

## Закрытые пункты (исправлены и удалены)

1. Статус-ключ: гонка lease-перевыпуска + свежесть 5 c — keepalive-цикл сериализует put/re-grant (`_ops`), put значения каждый тик; OCE в `StartAsync` → revoke.
2. `agents=absent` при etcd-сбое → прежние факты сохраняются.
3. NodeWalReceiverTests: inspect-fallback устранён (всегда сборка на прогон), тег per-worktree `pgworker-node:sanity-<suffix>`.
4. hasync: per-server try/continue, `state.Clear()` при недоступности runtime, валидация backend/servers/port.
5–6. Слот: типизированный `WalSlotMissing` (SQLSTATE 42704) → `waiting_slot` (в т.ч. lost-слот при непустом хвосте); SQL-сбой — transient-ретрай, не маскируется.
7. Публикации фактов сериализованы (`_publishGate`).
9. Диагностика «CA задан, но пул http» (одноразово).
10. SAN-пиннинг — точно 127.0.0.1 (REST-проба и master-lease); S3-фабрика — hostname-чек не вводится осознанно (SAN ownS3-leaf не покрывает алиасы нод — задокументировано в коде).
11. Мёртвое `S3CaPem` удалено из `WalReceiverOptions`.
12. Runbook дополнен publish NodeTools + build-arg.
13. Лог CA-деградации пробы роли — дедуп по переходу.
14–15. Комментарий `WalAgentPresence` переписан; failover-петли приёмника переведены на `EtcdEndpointFailover`.
16–18. Компактные тесты контрактов WalStreamProcess (факт-не-now, lag-DEGRADED, ratchet-chain_start); проект NodeTools.UnitTests (контракты CLI/guard'ов); флейк RoleFlap — инжектируемый период пробы.
19–21. `put_errors` не растёт от confirm-фейлов; master-lease: lock-файл callback'ов + идентификация процесса перед kill + атомарный PID; EnvFileSource — setdefault (код↔комментарий согласованы); CA-temp 0600; rolecheck — пароль из `--pg-password-file`; комментарии/опечатки (Dockerfile, WalStreamProcess, MasterLeaseFailover, NodeWalReceiverTests, musl-лог).

## Осталось на merge-гейте (не пункт ревью)

- `arch/roadmap/backup.md:25-27` — агентная часть пункта t24 удаляется мерж-коммитом (spec §6.6), вместе с тегом t25 из списков роадмапа.
