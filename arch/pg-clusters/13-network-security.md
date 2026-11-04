# Сетевая модель и безопасность

Уровень 3. Кто куда ходит, что шифруется и как аутентифицируется: зоны,
firewall-матрица, TLS-каналы, аутентификация, модель угроз. Порты и
публикации — [02-topology.md](02-topology.md) §2; пулер и его auth —
[06-client-access.md](06-client-access.md) §4–§5; принципы и решения —
[12-bucket-pitfalls.md](12-bucket-pitfalls.md) P17.

## 1. Зоны

| Зона | Кто в ней |
|---|---|
| **app** | инстансы приложения с роутером (etcd watch: бакет → шард → мастер) |
| **shard** | ноды кластеров-шардов: PostgreSQL, Patroni, pg_doorman, PgNode |
| **infra** | etcd-контур установки (`:2379`/`:2380`) — не роль кластера |
| **admin** | opsbox/админ-хосты, миграционная роль, мониторинг |
| **worker** | PgWorker (контейнер) и его транспорт к Engine API |

Инварианты:

- **etcd — внешний инфраструктурный слой**: на нодах кластера `:2379` не
  слушается вообще; ссылка на etcd — настройка при старте кластера.
- Наружу от нод кластера — только опубликованные порты **portalloc**:
  `:6432` (приложения), `:5432` (подписки переездов, админка), `:8008`
  (пробы/метрики/операции Patroni). Порты не литералы — значения из
  `/pgworker/portalloc/<C>`.
- `:8080`/`:8081` PgNode на хост не публикуются: адресация — `alias:8080`
  и mTLS-канал `:8081` внутри docker-сети воркера и нод.

## 2. Firewall-матрица «кто → куда»

| Откуда | Куда | Порт | Протокол / аутентификация | Зачем |
|---|---|---|---|---|
| app | etcd-контур | 2379 | etcd client, TLS | bootstrap + watch: `config`, `routing/N`, `shards/X/master`; чтение своего префикса `/clusters/<C>/` |
| app | мастер-нода шарда | doorman (portalloc) | PostgreSQL wire, **TLS require** + SCRAM | клиентский data path через pg_doorman; адрес — из `shards/X/master` |
| ноды шарда-приёмника | ноды шарда-источника | pg (portalloc) | replication-протокол; SCRAM `bucket_mover`, `sslmode=require`; multi-host conninfo, `target_session_attrs=read-write` | подписки переездов (прямая и обратная, tablesync) |
| реплика | мастер своего шарда | pg (portalloc) | streaming replication, physical slot | физрепликация внутри шарда |
| Patroni | etcd-контур | 2379 | etcd client, TLS | DCS `/service/<scope>/` |
| Patroni-нода | Patroni-ноды своего шарда | patroni (portalloc) | HTTPS (серт ноды; мутации — basic-auth per-cluster) | `fetch_node_status`, switchover |
| lease-скрипт ноды | `127.0.0.1` той же ноды | 8008 | HTTPS (ca-файл ноды) | определение роли в `on_start` |
| PgWorker | Patroni REST | patroni (portalloc) | HTTPS; мутации — basic-auth per-cluster | пробы/сверка master-ключа/конвергенция конфига |
| PgWorker | PgNode (API операций) | 8080 | HTTP внутри общей сети воркера и нод (`alias:8080`), без публикации на хост | полный/verify/restore/drill, `pgdump`, `pgdata`, `hba-ensure` |
| PgWorker | PgNode (канал секретов) | 8081 | HTTPS **mTLS**: серверный серт ноды per-install REST-CA (SAN), клиентский серт воркера — EKU ClientAuth + CN=pgworker | доставка секретов/команды применения; инициатор — всегда воркер |
| PgWorker | docker-хосты | 2376 | HTTPS **mTLS** (docker-PKI, `--tlsverify`) или SSH-туннель :22 → daemon :2376 loopback | provisioning/надзор/rebuild; `:2375` plaintext запрещён |
| панель | PgWorker | 8080 | HTTPS (серверный серт per-install API-CA); mTLS — только `/api` | мутации; `/healthz`/`/metrics` — без клиентского серта |
| панель | Patroni REST | patroni (portalloc) | HTTPS (GET `/cluster`, ca.pem) | live-пробы кластера |
| Prometheus | Patroni REST | patroni (portalloc) | HTTPS scrape (ca.pem), сеть скрейпа | метрики Patroni-нод |
| Prometheus | PgNode | 8080 | HTTP scrape внутри сети скрейпа | метрики приёмника WAL |
| админка/mover | ноды шардов (pg-порт), Patroni, etcd | portalloc, 2379 | SQL (mover/миграционная роль, SCRAM), patronictl, etcdctl | переезды, обслуживание, снапшоты |
| doorman | `127.0.0.1` той же ноды | 5432 | PostgreSQL, **SCRAM** (не trust) | loopback-бэкенд пулера |
| etcd-узел | etcd-узлы контура | 2380 | peer TLS | raft |

Нет в матрице — и не должно появляться:

- **app → pg-порт или Patroni** — приложениям напрямую к PG/Patroni хода
  нет, только `:6432` doorman;
- **app-ноды как источники межшард-трафика** — межшардовое только
  нода↔нода (подписки переездов);
- **`:2379` на нодах кластера** — etcd внешний;
- **`:6432` между нодами** — doorman всегда локален своей ноде (бэкенд
  строго `127.0.0.1:5432`);
- **`:2375` plaintext** к Engine API;
- **`:8081`/`:8080` ноды наружу хоста**.

## 3. Принципы

1. **Default deny**: разрешено только перечисленное; правила группируются
   по подсетям зон (app-net, shard-net, infra-net, admin-net).
2. **Приложению — два адреса**: etcd `:2379` (infra) и doorman
   (portalloc-порт мастер-ноды). Всё остальное для app-зоны закрыто.
3. **Межшард — только нода↔нода, подписки переездов**: conninfo —
   multi-host по всем нодам источника с `target_session_attrs=read-write`;
   аутентификация — SCRAM `bucket_mover`, `sslmode=require`.
4. **Patroni `:8008` не покидает зону shard/admin/worker**: его клиенты —
   PgWorker, админка, панель, Prometheus, межнодовые вызовы Patroni и
   loopback lease-скрипт — всё по TLS.
5. **pg-порт `:5432`** — loopback (doorman), реплики своего шарда,
   подписки переездов и админ-зона; приложениям закрыт.
6. **Порты нод — из portalloc**: publish-порты выделяются динамически;
   firewall строится на фактические значения (или на диапазон portalloc),
   не на литералы.

## 4. TLS

| Канал | Режим | Параметры |
|---|---|---|
| app → doorman | **`sslmode=require`** | `tls_certificate`/`tls_private_key` в конфиге doorman; минимум TLS 1.2; `hostssl`-правила `general.pg_hba`; клиентский DSN — `sslmode=require` |
| doorman → PG (loopback) | без TLS | `server_tls_sslmode=disable` — трафик не покидает ноду; включение — одна строка, если потребует политика |
| app → etcd | TLS | серты etcd-контура |
| подписки межшард | `sslmode=require` | libpq-параметр в `CONNECTION` подписки |
| Patroni REST | **TLS** | серверные серты per-install REST-CA (SAN: `<n>`, `pgw-<C>-<X>-<n>`, `127.0.0.1`); unsafe-эндпоинты — basic-auth per-cluster; клиенты верифицируют цепочку к CA без hostname-проверки |
| PgWorker → PgNode | **mTLS** | серверный серт ноды per-install REST-CA; клиентский серт воркера из того же CA + EKU ClientAuth + CN=pgworker; граница канала — порт `:8081` (`/api/bootstrap*` только там; по `:8080` — 404) |
| PgWorker → Engine API | **mTLS** / SSH | docker-PKI, `--tlsverify`; или SSH-туннель с pin host-key |

**Почему `require`, а не `verify-full` для `:6432`**: шифрование трафика и
кредов обязательно, адрес ноды приложение получает из etcd — доверие
адресу обеспечивает аутентификация etcd-канала (TLS + префикс кластера).
Серт doorman — тот же серт ноды из per-install REST-CA (SAN), который
обслуживает REST `:8008` и mTLS `:8081`; замена — через доставку агентом и
рестарт пулера, без self-signed на каждый старт.

## 5. Аутентификация

**Клиент → doorman (`:6432`).** Логин/пароль единственной app-роли,
SCRAM-SHA-256. **Per-pool auth_query** — единственный работающий механизм
pg_doorman: пулер проверяет креды клиента **динамически** запросом
`SELECT passwd FROM pg_shadow WHERE usename = $1` к бэкенду от superuser
`postgres` и верифицирует клиентское SCRAM-доказательство по полученному
верификатору. Верификатор в конфиге doorman не хранится; plaintext-пароль
superuser'а (per-install SuPassword) доставляется агенту PgNode по
mTLS-каналу и живёт только в tmpfs `/run/pgw` (рендеренный конфиг doorman,
env процессов Spilo — память контейнера); в файлы volume'ов и etcd не
попадает.

**Per-bucket роли отклонены**: пул — пара `(database, user)`; каждая роль =
отдельный пул со своим `pool_size`/`max_db_connections`, общего капа «на
ноду» нет — 256 пулов ломают бюджет 55/ноду. Права app-роли минимальны:
app ≠ owner, только DML + `USAGE, UPDATE` на sequences, `CREATE` срезается
мораторием P5.

**doorman → PG (loopback).** Серверное соединение — от имени клиентского
юзера; бэкенд-аутентификация — переиспользованием клиентского
SCRAM-доказательства. pg_hba PG:

```
host <C> app 127.0.0.1/32 scram-sha-256
```

**Не trust**: trust на loopback открывал бы PG любому процессу на
скомпрометированной ноде без пароля, мимо doorman.

**Прочие входы PG (`:5432`):** `bucket_mover` — SCRAM, подписки переездов
с нод приёмника (multi-host conninfo); физрепликация — `replication`-строки
реплик кластера; миграционная роль-владелец — напрямую из admin-зоны, не
через doorman.

**Админ-консоль doorman** (БД `pgdoorman` на `:6432`): `admin_username`/
`admin_password` из конфига; в `general.pg_hba` doorman — только с ноды:
`host pgdoorman admin 127.0.0.1/32 <метод>`.

## 6. Модель угроз

| Компрометация | Доступ | Не получает |
|---|---|---|
| нода приложения | креды app-роли (запись во все бакеты кластера); чтение ключей своего префикса etcd | доступ к PG-нодам напрямую (firewall), запись в etcd |
| нода кластера-шарда | PG своей ноды (loopback + SCRAM), конфиг doorman (plaintext SuPassword — tmpfs/память контейнера), роль `bucket_mover` (своё участие в переездах) | etcd-слой (`:2379` не слушается), чужие шарды напрямую (мимо подписок), другие ноды мимо firewall-матрицы |
| admin-хост | всё: Patroni, etcd, SQL, mover — максимальный радиус; держать минимальным по составу | — |
| перехват сети | зашифровано: `:6432` (TLS require), `:2379` (TLS etcd), `:8008` (HTTPS; мутационная грань — basic-auth), межшард `:5432` (`sslmode=require`), mTLS-каналы воркера | — |
