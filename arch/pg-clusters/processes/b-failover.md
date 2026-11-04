# B. Failover (неплановая смена лидера)

Уровень 4. Автомат смены лидера при отказе: обнаружение, выборы, promote,
возврат старого лидера, защита от split-brain. Модель —
[01-ha-architecture.md](../01-ha-architecture.md) §4; DCS —
[04-etcd-dcs.md](../04-etcd-dcs.md) §3; ускорение — PgWorker
([pgworker/processes/c-node-supervisor.md](../../pgworker/processes/c-node-supervisor.md) §3).

## 1. Участники

- Patroni каждой ноды (продление lease, участие в выборах, promote);
- etcd-DCS (`/service/<scope>/leader`, `optime/leader`, `members/*`);
- PgWorker (надзор: положительное свидетельство смерти лидера, ускорение,
  фиксация HA-факта `last_failover`);
- клиенты (роутер: мастер-ключ; приложения: ретрай ошибок записи).

## 2. Состояния

| Состояние | Что означает | Условие выхода |
|---|---|---|
| `LEADER_OK` | lease лидера продлевается, primary принимает запись | смерть/изоляция лидера → `LEADER_LOST` |
| `LEADER_LOST` | лидер перестал продлевать lease; ключ истекает по TTL | истечение TTL → `ELECTION` |
| `ACCELERATED` | PgWorker получил положительное свидетельство смерти (контейнер отсутствует/не running) и удалил leader-ключ + выставил `/failover` с кандидатом | выборы → `PROMOTION` |
| `ELECTION` | ноды конкурируют за lease; побеждает самая свежая по WAL | lease выдан → `PROMOTION` |
| `PROMOTION` | победитель делает `pg_ctl promote`, публикует себя лидером | PG writable → `NEW_LEADER` |
| `NEW_LEADER` | новый primary пишет `optime/leader`, callback обновляет мастер-ключ | нормальная работа → `LEADER_OK` |
| `REJOIN` | старый лидер вернулся/демотирован; догоняет WAL или `pg_rewind` | streaming → `LEADER_OK` (реплика) |
| `BLOCKED` | выборы невозможны: нет кворума etcd или нет живой свежей реплики | восстановление причины → `ELECTION` |

Переходы: `LEADER_OK → LEADER_LOST → ELECTION → PROMOTION → NEW_LEADER →
LEADER_OK`; ускоренная ветка `LEADER_LOST → ACCELERATED → PROMOTION`;
возврат старого лидера — `REJOIN`. `BLOCKED` — диагностическая ветка.

## 3. Ход процесса

1. **Обнаружение.** Patroni-лидер продлевает lease каждые `loop_wait`;
   остальные ноды замечают отсутствие лидера после истечения TTL
   (пол Patroni 4.x `ttl≥20`; бюджеты —
   [pgworker/01-placement.md](../../pgworker/01-placement.md) §2).
2. **Ускорение (PgWorker).** При положительном свидетельстве смерти
   лидера (docker-объект отсутствует ИЛИ контейнер есть, но не running —
   только для драйверов с честной running-инспекцией) надзор удаляет
   leader-ключ и выставляет `/service/<scope>/failover` с кандидатом
   (sync-standby); промоушен — в пределах `loop_wait`. Недоступность REST
   при живом контейнере — не свидетельство смерти: ускорение не
   применяется, ждём lease.
3. **Выборы.** Побеждает нода с самой свежей позицией WAL
   (`optime/leader`); при синхронной репликации — гарантированно
   sync-реплика (подтверждённые коммиты не теряются).
4. **Promote.** Победитель делает `pg_ctl promote`, становится writable,
   публикует членов и `optime/leader`.
5. **Мастер-ключ.** Callback `on_role_change` пишет
   `/clusters/<C>/shards/<X>/master` (`host:<doorman-port>`, lease TTL 5 с);
   при отказе callback ключ корректирует MasterKeyReconciler PgWorker
   ([07-identify-master.md](../07-identify-master.md) §3).
6. **Возврат старого лидера.** Он обнаруживает новую timeline, догоняет по
   WAL или делает `pg_rewind` и становится репликой
   ([d-node-recovery.md](d-node-recovery.md)).

## 4. Бюджеты

| Канал | Бюджет |
|---|---|
| graceful (SIGTERM/stop) | Patroni демотирует лидера и повышает sync-реплику в пределах `loop_wait` — единицы секунд |
| жёсткая смерть + ускорение PgWorker | тик надзора + проба + `loop_wait` (e2e-профиль ≤5 с) |
| жёсткая смерть без ускорения (Swarm-заглушка инспекции, нет кандидата) | истечение lease — 20+ с |

Время события (`detected_unix`→`resolved_unix`, `duration_sec`) фиксируется
в `work.last_failover` с `cause=accelerated|elections`; открытое событие
переживает takeover контроллера.

## 5. Защита от split-brain

- Пока жив кворум etcd, лидер один: lease-ключ один, без продления нода
  обязана уйти в read-only/остановить PG.
- Сеть: меньшинство теряет DCS и демотируется; большинство выбирает нового
  лидера после истечения lease. Окно, в котором старый лидер ещё принимает
  запись до истечения lease, перекрывается синхронной репликацией:
  подтверждённые коммиты есть на новой реплике.
- Ручной `pg_ctl promote`/запуск PG мимо Patroni — единственный источник
  двух primary; разбор — [09-troubleshooting.md](../09-troubleshooting.md) §4.

## 6. Отказы

- **Нет кворума etcd** — `BLOCKED`: выборы не проводятся; запись стоит до
  восстановления etcd ([04-etcd-dcs.md](../04-etcd-dcs.md) §5).
- **Нет живой свежей реплики** — Patroni не повышает отстающую при
  `synchronous_mode`; принудительный `failover --candidate <самая свежая>` —
  последнее средство, затем разбор.
- **REST `:8008` недоступен между нодами** — выборы могут не состояться
  (firewall/TLS); проверка — [09](../09-troubleshooting.md) §3.
- **Старый лидер продолжает продлевать lease, не зная о демоушене** —
  MasterKeyReconciler сверяет ключ с фактом `/primary` и корректирует;
  канарейка/алерты — внешний мониторинг.
