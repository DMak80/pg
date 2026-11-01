# 13. Сетевая модель: firewall-матрица, TLS, аутентификация ★

Сетевая модель топологии бакетного шардирования ([11](11-bucket-sharding.md);
референс топологии — [12](12-bucket-pitfalls.md)). Источник — закрытие риска
**P17** ([12](12-bucket-pitfalls.md)): в базовой топологии доков 01–10 наружу
торчала только HAProxy-нода (`pg-lb:5432/5433`), в топологии бакетов наружу
выходят порты самих нод шардов. Здесь зафиксировано, кто куда ходит, что
шифруется и как аутентифицируется. Базовые кластеры PgWorker без бакетных
переездов — те же правила, минус межшардовые подписки.

Инварианты (P17):

- **etcd — внешний инфраструктурный слой**: на нодах кластера `:2379` не
 слушается вообще; ссылка на etcd — настройка при старте кластера
 ([11](11-bucket-sharding.md) §2);
- наружу от нод кластера — только **`:6432`** (приложениям) и **`:5432`**
 (межшард-подписки и админка); всё остальное — внутри зоны кластера или
 на отдельном infra-слое.

---

## 1. Зоны

| Зона | Кто в ней |
|---|---|
| **app** | инстансы приложения с роутером (etcd watch: бакет → шард → мастер) |
| **shard** | ноды кластеров-шардов: PG :5432, Patroni :8008, pg_doorman :6432 |
| **infra** | внешний etcd-слой :2379 ([04](04-deploy-etcd.md)) — не роль кластера |
| **admin** | opsbox/mover, админ-хосты дежурных, мониторинг |

---

## 2. Firewall-матрица «кто → куда»

| Откуда | Куда | Порт | Протокол / аутентификация | Зачем |
|---|---|---|---|---|
| app | etcd-слой (infra) | 2379 | etcd client, TLS ([04](04-deploy-etcd.md)) | bootstrap + watch: `config`, `routing/N`, `shards/X/master`; чтение своего префикса `/clusters/<C>/` |
| app | мастер-нода каждого шарда | 6432 | PostgreSQL wire, **TLS require** + SCRAM (§4) | клиентский data path через pg_doorman; адрес — из `.../shards/X/master` |
| ноды шарда-приёмника | ноды шарда-источника (pg-порты portalloc) | 5432 | replication-протокол; SCRAM роль `bucket_mover`, `sslmode=require`; conninfo — multi-host по всем нодам источника, `target_session_attrs=read-write` | подписки переездов (прямая + обратная, tablesync); conninfo из dsn-ключа ([11](11-bucket-sharding.md) §4) |
| реплика → мастер | свой шард | 5432 | streaming replication, physical slot | физрепликация внутри шарда |
| Patroni | etcd-слой (infra) | 2379 | etcd client, TLS | DCS `/service/<scope>/` |
| PgWorker (сверяющий демон P11) | Patroni REST :8008 + etcd :2379 | 8008, 2379 | HTTPS, basic-auth per-cluster (мутации) + etcd | сверка `.../master` ↔ Patroni REST, автокоррекция ключа |
| Patroni-нода | Patroni-ноды своего кластера | 8008 | HTTPS (межнодовые, basic-auth кластера) | fetch_node_status, switchover |
| lease-скрипт ноды (P11) | `127.0.0.1:8008` той же ноды | 8008 | HTTPS (ca-файл ноды) | определение роли ноды в `on_start` |
| mover / opsbox (admin) | ноды шардов (pg-порты), etcd | 5432, 2379 | SQL (mover-роль, SCRAM) + etcdctl | переезды бакетов, снапшоты P12 |
| админка (admin) | Patroni, etcd, ноды шардов | 8008, 2379, 5432 | patronictl, etcdctl, psql | эксплуатация; pg-порты нод — напрямую (SCRAM) |
| мониторинг (doorman/etcd metrics) | doorman metrics, etcd metrics | 6432, 2379 | HTTP scrape | метрики, алерты (P21) |
| pg_doorman | `127.0.0.1` той же ноды | 5432 | PostgreSQL, **SCRAM** (не trust, §5) | loopback — вне firewall, но под pg_hba |
| PgWorker | агент ноды (PgNode) | 8081 | HTTPS **mTLS** (серверный серт ноды per-install REST-CA с SAN; клиентский серт воркера из того же CA, верифицирует агент по публичному CA из docker-env + **EKU ClientAuth + CN=pgworker** — серверные серты нод того же CA клиентом канала НЕ являются) | команды хендшейка/ротации (arch/14 §4): доставка per-install паролей PG телом по `not_initialized`, команда применения на `ready`; статусы воркер читает из etcd, не по каналу; порт ВНУТРИ docker-сети кластера, на хост не публикуется; инициатор — всегда воркер |
| PgWorker | docker-хосты (Engine API) | 2376 | HTTPS **mTLS** (клиентский серт per-install docker-CA; `--tlsverify` на демоне) или SSH-key-туннель :22 → daemon :2376 loopback | provisioning/надзор/rebuild (arch/14 §2.2.1); `:2375` plaintext — запрещён |
| панель / Prometheus | PgWorker, KafkaWorker | 8080 | HTTPS (серверный серт per-install API-CA); mTLS — только `/api` (клиентские серты `panel.crt`, `seed.crt`); `/healthz`/`/metrics` — без клиентского серта | мутации (mTLS)/healthz/скрейп (arch/14 §1.1, arch/16 §1.1, arch/18 §5.2) |
| панель | Patroni REST :8008 | 8008 | HTTPS (GET /cluster, ca.pem) | live-пробы кластера (arch/adminpanel/02 §6.1) |
| Prometheus | Patroni :8008 | 8008 | HTTPS scrape (ca.pem), сеть pgw-metrics | метрики Patroni-нод, алерты (arch/18 §5.2) |

Нет в матрице — и не должно появляться:

- **app → :5432 / :8008** — приложениям напрямую к PG и Patroni хода нет;
- **app-ноды как источники межшард-трафика** — межшардовое только
 нода↔нода: подписки переездов ([11](11-bucket-sharding.md) §4);
- **`:2379` на нодах кластера** — etcd внешний (P17);
- **`:6432` между нодами** — doorman всегда локален своей ноде
 (бэкенд строго `127.0.0.1:5432`).

---

## 3. Принципы

1. **Default deny**: разрешено только перечисленное в матрице; правила
 группируются по подсетям зон (app-net, shard-net, infra-net, admin-net).
2. **Приложению — два адреса**: `2379` (infra etcd) и `6432` (ноды шардов).
 Всё остальное для app-зоны закрыто.
3. **Межшард — только нода↔нода, подписки переездов**: conninfo —
 multi-host по всем нодам источника с `target_session_attrs=read-write`
 (писателя находит libpq — эквивалент выделенной точки входа без отдельного
 компонента); аутентификация — SCRAM `bucket_mover`, `sslmode=require`
 ([11](11-bucket-sharding.md) §4).
4. **`:8008` не покидает зону shard/admin** — его клиенты:
 PgWorker (сверяющий демон), админка, панель, Prometheus,
 межнодовые вызовы Patroni и loopback lease-скрипт ноды (P11) — всё по TLS.
5. **`:5432` PG напрямую** — loopback, реплики своего шарда, подписки
 переездов и админ-зона; приложениям `:5432` хода нет (клиентский
 data path — только `:6432`).
6. **`:2379`** слушается только на infra-слое; клиенты — app (watch),
 Patroni, mover, админка, мониторинг ([04](04-deploy-etcd.md) — TLS etcd).

---

## 4. TLS (решение P17)

| Канал | Режим | Параметры |
|---|---|---|
| app → doorman `:6432` | **`sslmode=require`** | `tls_certificate` + `tls_private_key` в конфиге doorman; минимум TLS 1.2; `hostssl`-правила в `general.pg_hba` doorman; клиентский DSN роутера — `sslmode=require` ([11](11-bucket-sharding.md) §3) |
| doorman → PG (loopback) | без TLS | `server_tls_sslmode=disable` — трафик не покидает ноду; включить — одна строка, если потребует security-политика |
| app → etcd `:2379` | TLS | сертификаты etcd-слоя ([04](04-deploy-etcd.md)) |
| подписки межшард `:5432` | `sslmode=require` | libpq-параметр в `CONNECTION` подписки (канон conninfo — [11](11-bucket-sharding.md) §4) |
| Patroni `:8008` | **TLS** | серверные серты per-install CA (SAN — arch/14 §2.4); unsafe-эндпоинты — basic-auth per-cluster; верификация клиентами — цепочка к CA без hostname-проверки (канон P17) |

Принято для `:6432`: **require, не verify-full** — шифрование трафика и
кредов обязательно, верификация серверного сертификата клиентом не требуется:
адрес ноды приложение получает из etcd-контрол-плейна, т.е. доверие адресу
обеспечивает аутентификация самого etcd-канала (TLS + префикс кластера).

Схема серверного сертификата doorman: выпускает PgWorker из per-install
REST-CA (SAN — тот же канон, что у REST-серта ноды: arch/14 §2.4 п.6),
доставляет загрузочный агент (bootstrap-ключ etcd → tmpfs-файлы,
arch/14 §4); один и тот же серт ноды обслуживает REST `:8008`, doorman
`:6432` и mTLS-канал агента `:8081`. Клиенты по-прежнему
`sslmode=require` — верификация цепочки не требуется (канон P17 выше),
но серт теперь несёт SAN и управляемую замену: обновление = перезапись
bootstrap-ключа воркером + сигнал агенту → агент заменяет файлы и
рестартует пулер (entrypoint-генерация self-signed per-start упразднена).
Остальные серверные серты контура — Patroni `:8008` и doorman (общая
схема выше), etcd-слой ([04](04-deploy-etcd.md)).

---

## 5. Аутентификация (решение P17)

**Клиент → doorman (`:6432`).** Логин/пароль единственной app-роли,
SCRAM-SHA-256. **Per-pool auth_query** — единственный работающий механизм
pg_doorman v3.11.0 (режим без объявленных пользователей отклоняет каждого
клиента): doorman проверяет креды клиента **динамически** —
запрос `SELECT passwd FROM pg_shadow WHERE usename = $1` к бэкенду от
superuser `postgres` — и верифицирует клиентское SCRAM-доказательство по
полученному verifier'у. Верификатор в конфиге doorman не хранится;
plaintext-пароль superuser'а (per-install SuPassword) доставляется
агенту PgNode по mTLS-каналу (arch/14 §4: прямая передача — в etcd и
docker-env контейнера не попадает) и живёт только в tmpfs `/run/pgw`
(рендеренный конфиг doorman, env процессов Spilo — память контейнера);
в файлы volume'ов и etcd не попадает.

**Per-bucket роли — отклонены .** Пул doorman — пара
`(database, user)`: каждая роль = отдельный пул со своим `pool_size` и
`max_db_connections`, общего капа «на ноду» нет ([12](12-bucket-pitfalls.md)
P14/P15) — 256 пулов ломают бюджет 55/ноду. Радиус компрометации кредов
app-роли = все бакеты кластера — принято вместе с инвариантом единой БД.
Права роли минимальны по построению: app ≠ owner, только DML +
`USAGE,UPDATE` на sequences, `CREATE` срезается мораторием P5.

**doorman → PG (loopback).** Серверное соединение — от имени клиентского
юзера (`app`), бэкенд-аутентификация — переиспользованием клиентского
SCRAM-доказательства. pg_hba PG:

```
host <C> app 127.0.0.1/32 scram-sha-256
```

**Не trust**: trust на loopback открывал бы PG любому процессу на
скомпрометированной ноде без пароля, мимо doorman.

**Прочие входы PG (`:5432`):** `bucket_mover` — SCRAM, подписки переездов
с нод приёмника (multi-host conninfo, [11](11-bucket-sharding.md) §4);
физрепликация — `replication`-строки
реплик кластера; миграционная роль-владелец — не через doorman, напрямую
с admin-зоны.

**Admin-консоль doorman** (БД `pgdoorman` на `:6432`): `admin_username` /
`admin_password` из конфига; в `general.pg_hba` doorman — только с ноды:
`host pgdoorman admin 127.0.0.1/32 <метод>`.

---

## 6. Что получает компрометация (модель угроз)

| Компрометация | Доступ | Не получает |
|---|---|---|
| нода приложения | креды app-роли (запись во все бакеты кластера); чтение ключей своего префикса etcd | доступ к PG-нодам напрямую (firewall), запись в etcd |
| нода кластера-шарда | PG своей ноды (loopback + scram), конфиг doorman (plaintext SuPassword — tmpfs/память контейнера, mTLS-канал доставки — arch/14 §4), роль `bucket_mover` (своё участие в переездах) | etcd-слой (`:2379` там не слушается), чужие шарды напрямую (мимо подписок переездов), другие ноды мимо firewall-матрицы |
| admin-хост | всё: Patroni, etcd, SQL, mover — максимальный радиус, держать минимальным по составу хостов | — |
| перехват сети | зашифровано: `:6432` (TLS require), `:2379` (TLS etcd), `:8008` (HTTPS; мутационная грань — basic-auth); межшард `:5432` — опционально `sslmode=require` у подписок | — |

---

## Дальше

→ [11-bucket-sharding.md](11-bucket-sharding.md) — механика переездов,
[12-bucket-pitfalls.md](12-bucket-pitfalls.md) — реестр рисков (P17).
