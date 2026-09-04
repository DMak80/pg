# Отказоустойчивый PostgreSQL-кластер на 3 ноды (Docker)

Цель этой документации — **воспроизводимый рецепт** развёртывания надёжного HA-кластера
PostgreSQL в Docker на 3 физических (или виртуальных) хостах, у каждого своё локальное
хранилище. Документ написан так, чтобы по нему мог настроить кластер кто угодно —
включая меня самого в будущем.

> **Базовая конфигурация — 3 ноды.** Нужен вариант на **4 ноды** (переживает двойной отказ,
> 2 синхронные реплики)? См. [10-four-nodes.md](10-four-nodes.md). Главное правило там:
> PostgreSQL-нод может быть сколько угодно, а **etcd всегда нечётный (3 или 5)**.

> Нужно **несколько таких кластеров-шардов** с «бакетами» (схемами) поверх них и
> онлайн-переездом бакета между шардами? См. [11-bucket-sharding.md](11-bucket-sharding.md).
> Шардированный кластер создаётся командой `init-cluster.sh` (число бакетов N и имя
> БД — константы), шарды добавляются/удаляются командами; один etcd обслуживает
> несколько независимых кластеров (префиксы `/clusters/<C>/`).

> Стек проверен на Patroni 4.x + Spilo 3.3 + PostgreSQL 16 + etcd 3.5 + HAProxy 2.8.
> Образы — официальные, из публичных реестров.
> Бакетный слой ([11-bucket-sharding.md](11-bucket-sharding.md)) — скрипты проверены
> на **PostgreSQL 18.4** (последний стабильный релиз).

---

## Что внутри (TL;DR)

```
┌──────────────────────────────────────────────────────────────────────┐
│  3 ноды (хоста), у каждой свой диск. Docker на каждой ноде.           │
│                                                                       │
│  etcd-кластер (3 узла) ── DCS: кто сейчас лидер, конфиг кластера     │
│       ▲▲▲                                                             │
│       |||                                                             │
│  Patroni (3 экз.) ─── управляет локальным PostgreSQL, держит лидера  │
│       ▲                                                              │
│       │                                                              │
│  PostgreSQL x3 ─── 1 master (read+write) + 2 replicas (read-only)    │
│       ▲                                                              │
│       │                                                              │
│  HAProxy ── единая точка входа: порт 5432 → master, 5433 → replicas │
└──────────────────────────────────────────────────────────────────────┘
```

- **Patroni** ([github.com/patroni/patroni](https://github.com/patroni/patroni)) — менеджер
  репликации и автоматического failover.
- **Spilo** ([github.com/zalando/spilo](https://github.com/zalando/spilo)) — готовый Docker-образ
  от Zalando = PostgreSQL + Patroni в одном контейнере. Берём его, чтобы не собирать своё.
- **etcd** ([etcd.io](https://etcd.io/)) — распределённое KV-хранилище (DCS). Patroni
  хранит в нём состояние кластера и проводит выборы лидера.
- **HAProxy** ([haproxy.com](https://www.haproxy.com/)) — балансировщик и «маршрутизатор
  на мастера»: всегда направляет write-трафик на текущего лидера.

---

## Быстрый старт (если хочется «прямо сейчас»)

Все команды — с привилегиями (sudo/ssh на ноды). Полное пояснение — в соответствующих разделах.

```bash
# 0. На каждой из 3 нод: ставим docker + docker compose plugin (см. 03-prerequisites.md)

# 1. Разворачиваем etcd-кластер (по одной ноде etcd на хост)
#    См. 04-deploy-etcd.md → docker compose -f etcd/docker-compose.yml up -d

# 2. Разворачиваем PostgreSQL/Patroni (по одному Spilo-контейнеру на хост)
#    См. 05-deploy-postgres.md → docker compose -f postgres/docker-compose.yml up -d

# 3. Разворачиваем HAProxy (можно на отдельной ноде или вместе с одной из БД-нод)
#    См. 06-deploy-haproxy.md → docker compose -f haproxy/docker-compose.yml up -d

# 4. Проверяем, кто лидер
./scripts/find-leader.sh            # или: ./scripts/get-role.sh pg1
```

Готовые файлы лежат в `configs/`, рабочие скрипты — в `scripts/`.

---

## Структура репозитория

```
arch/
├── README.md                  ← вы здесь
├── 01-architecture.md         ← как это работает (компоненты, failover)
├── 02-topology.md             ← адреса нод, диски, порты, firewall
├── 03-prerequisites.md        ← что поставить на хосты до старта
├── 04-deploy-etcd.md          ← кластер etcd
├── 05-deploy-postgres.md      ← кластер PostgreSQL/Patroni (Spilo)
├── 06-deploy-haproxy.md       ← балансировщик HAProxy
├── 07-identify-master.md      ← ★ как узнать, кто сейчас мастер
├── 08-operations.md           ← switchover, failover, backup, добавление ноды
├── 09-troubleshooting.md      ← диагностика и типовые проблемы
├── 10-four-nodes.md           ← ★ опция: 4-нодовая схема (3 узла etcd + 4 БД-ноды)
├── 11-bucket-sharding.md      ← ★ опция: бакеты (схемы) поверх нескольких кластеров,
│                                  онлайн-переезд бакета между шардами
├── 12-bucket-pitfalls.md      ← ★ реестр рисков топологии бакетов (etcd + pg_doorman)
├── 13-network-security.md     ← ★ сетевая модель бакетов: firewall-матрица, TLS, аутентификация (P17)
├── 14-pgworker.md             ← ★ оркестратор PgWorker: provisioning/deprovisioning/надзор/эвакуация
│                                  (docker-управление кластерами, префикс /pgworker/, roadmap/)
├── 15-kafka-clusters.md       ← ★ Kafka-кластера: контракт etcd /kafka/ + координация /kafkaworker/
│                                  + клиентский дискавери (endpoints/app-креды/реестр топиков)
├── 16-kafkaworker.md          ← ★ KafkaWorker: оркестратор Kafka-кластеров
│                                  (provisioning/deprovisioning/надзор/автосинк топиков/converge)
├── 17-synchronization-principles.md ← ★ принципы синхронизации распределённого состояния
│                                  (единый etcd, конвергенция DCS, детерминизм, реестр ошибок)
├── configs/
│   ├── etcd/
│   │   ├── docker-compose.yml
│   │   └── etcd.env
│   ├── postgres/
│   │   ├── docker-compose.yml          (по ноде на хост: pg1/pg2/pg3, для pg4 — hostname:pg4)
│   │   ├── pg.env                      (базовый, для 3 нод)
│   │   └── pg4.env.example             (★ для 4 нод: + 2 синхронные реплики)
│   ├── patroni/
│   │   └── patroni.yml                 (пример YAML, если Spilo не подходит)
│   └── haproxy/
│       ├── docker-compose.yml
│       ├── haproxy.cfg                 (базовый, 3 ноды)
│       └── haproxy-4nodes.cfg          (★ для 4 нод: + server pg4 в обоих backend'ах)
│   └── buckets/
│       └── buckets.env.example         (★ конфиг бакетных скриптов: кластер, etcd, пароли шардов)
└── scripts/
    ├── find-leader.sh         ← найти текущего лидера кластера
    ├── get-role.sh            ← узнать роль конкретной ноды (master/replica)
    ├── cluster-state.sh       ← полное состояние кластера из DCS
    ├── switchover.sh          ← плановая смена мастера
    ├── rebuild-node.sh        ← пересоздать ноду с пустого/повреждённого диска
    ├── health.sh              ← быстрая проверка здоровья всех нод
    ├── patronictl.sh          ← обёртка над patronictl в контейнере
    ├── buckets-common.sh      ← ★ общие функции бакетных скриптов (11-bucket-sharding.md)
    ├── init-cluster.sh        ← ★ создать шардированный кластер: константы (N, dbname),
    │                              все N бакетов поровну по шардам, регистрация в etcd
    ├── add-shard.sh           ← ★ подключить ПУСТОЙ шард (бакеты мигрируются позже)
    ├── remove-shard.sh        ← ★ снять пустой шард (guard: без бакетов и переездов, P23)
    ├── create-bucket.sh       ← ★ аварийное восстановление/регистрация одного бакета
    ├── move-bucket.sh         ← ★ онлайн-переезд бакета: move/status/rollback/finalize
    ├── abort-move.sh          ← ★ etcd: отмена незавершённого переезда + уборка артефактов (P7)
    ├── restore-cluster.sh     ← ★ P12: снапшоты etcd + verify/heal/restore контрол-плейна
    ├── restore-system.sh      ← ★ P22: восстановление системы в правильном порядке
    │                              (etcd → шарды → verify/heal → приложение)
    # все скрипты читают ALL_NODES из env → автоматически работают и с 3, и с 4 нодами
```

---

## Ключевые решения и почему именно так

| Решение | Почему |
|---|---|
| **Spilo-образ**, а не самописный Dockerfile | Это проверенный в проде образ Zalando: PostgreSQL + Patroni + всё нужное внутри. Не надо собирать/поддерживать свой. |
| **etcd**, а не Consul/ZooKeeper | Лёгкий, родной для Patroni, простой в Docker. 3 узла дают кворум и переживают потерю 1. |
| **HAProxy**, а не pgBouncer/Pgpool-II | HAProxy делает ровно то, что нужно — по health-check Patroni направляет writes на лидера, reads — на реплики. Прозрачно и без магии. |
| **По одному контейнеру БД на хост** (а не 3 в одном compose) | Хранилище на каждой ноде своё; отказ хоста = отказ ровно одной ноды БД. Это и есть «fail-safe на 3 ноды». |
| **Локальные диски**, без общего NFS/ SAN | Требование задачи. Репликация стриминговая (physical streaming replication), общего диска не нужно. |

---

## Отказоустойчивость — что переживёт кластер

- **Потеря 1 из 3 нод БД** → оставшиеся 2 выбирают нового лидера, etcd (2 из 3) сохраняет
  кворум. Доступность сохраняется. ✅
- **Потеря 1 из 3 узлов etcd** → кворум etcd (2/3) цел, Patroni продолжает работать. ✅
- **Сетевой split** между нодами → в меньшинстве Patroni уводит свой PostgreSQL в read-only/
  останавливает, лидер остаётся один. Защита от split-brain. ✅
- **Потеря 2 нод одновременно** → кворума нет, кластер останавливает запись. Это правильно:
  лучше недоступность записи, чем два мастера (data loss). Восстановление — в `09-troubleshooting.md`.

---

## Дальше

1. [01-architecture.md](01-architecture.md) — понять, как это работает.
2. [02-topology.md](02-topology.md) — зафиксировать адреса и диски под свой стенд.
3. [03-prerequisites.md](03-prerequisites.md) — подготовить хосты.
4. [04-deploy-etcd.md](04-deploy-etcd.md) → [05-deploy-postgres.md](05-deploy-postgres.md) →
   [06-deploy-haproxy.md](06-deploy-haproxy.md) — пошаговый деплой.
5. [07-identify-master.md](07-identify-master.md) — главная операционка: «кто мастер?».
6. [08-operations.md](08-operations.md) — рутина (switchover, бэкапы, rebuild ноды).
7. [09-troubleshooting.md](09-troubleshooting.md) — когда что-то сломалось.
8. [10-four-nodes.md](10-four-nodes.md) — **опционально**: расширение до 4 нод.
9. [11-bucket-sharding.md](11-bucket-sharding.md) — **опционально**: виртуальные
   шарды-бакеты поверх нескольких кластеров.
10. [12-bucket-pitfalls.md](12-bucket-pitfalls.md) — реестр рисков топологии
    (константа N, etcd-контрол-плейн, pg_doorman).
11. [13-network-security.md](13-network-security.md) — **опционально**: сетевая
    модель бакетов (firewall-матрица, TLS, аутентификация).
12. [14-pgworker.md](14-pgworker.md) — оркестратор PgWorker: декларативный
    provisioning/deprovisioning кластеров, контроль нод, эвакуация бакетов
    (отложенные задачи — [roadmap/](roadmap/)).
13. [15-kafka-clusters.md](15-kafka-clusters.md) — Kafka-кластера: контракт
    etcd (контроль-плейн `/kafka/`, координация `/kafkaworker/`) и клиентский
    дискавери.
14. [16-kafkaworker.md](16-kafkaworker.md) — оркестратор KafkaWorker:
    декларативный жизненный цикл Kafka-кластеров (KRaft, docker plain/swarm),
    автосинк топиков, converge конфигов.
15. [17-synchronization-principles.md](17-synchronization-principles.md) —
    принципы синхронизации распределённого состояния (единый etcd-контур,
    конвергенция динамического DCS-конфига, инварианты внешних систем,
    недискриминантные ключи, детерминизм мутаций, флап≠смерть, реестр
    ошибок использования).
