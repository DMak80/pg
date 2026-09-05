# 13. Сетевая модель: firewall-матрица, TLS, аутентификация ★

Сетевая модель топологии бакетного шардирования ([11](11-bucket-sharding.md);
референс топологии — [12](12-bucket-pitfalls.md)). Источник — закрытие риска
**P17** ([12](12-bucket-pitfalls.md)): в базовой топологии доков 01–10 наружу
торчала только HAProxy-нода (`pg-lb:5432/5433`), в топологии бакетов наружу
выходят порты самих нод шардов. Здесь зафиксировано, кто куда ходит, что
шифруется и как аутентифицируется. Базовые кластеры без бакетов — матрица
вырождается в «наружу только HAProxy» ([02](02-topology.md)).

Инварианты (P17, 2026-08-22):

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
| **shard** | ноды кластеров-шардов: PG :5432, Patroni :8008, pg_doorman :6432, HAProxy :5432 |
| **infra** | внешний etcd-слой :2379 ([04](04-deploy-etcd.md)) — не роль кластера |
| **admin** | opsbox/mover, админ-хосты дежурных, мониторинг |

---

## 2. Firewall-матрица «кто → куда»

| Откуда | Куда | Порт | Протокол / аутентификация | Зачем |
|---|---|---|---|---|
| app | etcd-слой (infra) | 2379 | etcd client, TLS ([04](04-deploy-etcd.md)) | bootstrap + watch: `config`, `routing/N`, `shards/X/master`; чтение своего префикса `/clusters/<C>/` |
| app | мастер-нода каждого шарда | 6432 | PostgreSQL wire, **TLS require** + SCRAM (§4) | клиентский data path через pg_doorman; адрес — из `.../shards/X/master` |
| ноды шарда-приёмника | HAProxy-ноды шарда-источника | 5432 | replication-протокол; pg_hba `bucket_mover` по IP HAProxy-нод **источника** | подписки переездов (прямая + обратная, tablesync); multi-host conninfo ([11](11-bucket-sharding.md) §4) |
| реплика → мастер | свой шард | 5432 | streaming replication, physical slot | физрепликация внутри шарда |
| HAProxy | Patroni REST всех нод кластера | 8008 | HTTP `GET /primary` (без аутентификации) | health-check «кто мастер» |
| Patroni | etcd-слой (infra) | 2379 | etcd client, TLS | DCS `/service/<scope>/` |
| сверяющий демон (P11) | Patroni :8008 + etcd :2379 | 8008, 2379 | HTTP + etcd | сверка `.../master` ↔ Patroni REST, автокоррекция ключа |
| HAProxy-синкеры (динамические адреса) | etcd + Patroni нод кластера | 2379, 8008 | etcd watch + `GET /whoami` (валидация идентичности) | топология из etcd → runtime API HAProxy ([11](11-bucket-sharding.md) §4) |
| mover / opsbox (admin) | HAProxy шардов, etcd | 5432, 2379 | SQL (mover-роль) + etcdctl | переезды бакетов, снапшоты P12 |
| админка (admin) | Patroni, etcd, HAProxy | 8008, 2379, 5432 (+7000 stats) | patronictl, etcdctl, psql | эксплуатация; PG `:5432` **напрямую** — только аварийно |
| мониторинг | Patroni :8008, doorman metrics, etcd metrics | 8008, 6432, 2379 | HTTP scrape | метрики, алерты (P21) |
| pg_doorman | `127.0.0.1` той же ноды | 5432 | PostgreSQL, **SCRAM** (не trust, §5) | loopback — вне firewall, но под pg_hba |
| PgWorker | docker-хосты (Engine API) | 2376 | HTTPS **mTLS** (клиентский серт per-install docker-CA; `--tlsverify` на демоне) или SSH-key-туннель :22 → daemon :2376 loopback | provisioning/надзор/rebuild (arch/14 §2.2.1, t03); `:2375` plaintext — запрещён |
| панель / Prometheus | PgWorker, KafkaWorker | 8080 | HTTPS **mTLS** (клиентские серты per-install API-CA: `panel.crt`, `seed.crt`, `prometheus.crt`) | мутации/healthz/скрейп (arch/14 §1.1, arch/16 §1.1, arch/18 §5.2) |

Нет в матрице — и не должно появляться:

- **app → :5432 / :8008** — приложениям напрямую к PG и Patroni хода нет;
- **app-ноды как источники межшард-трафика** — межшардовое только нода↔нода
  через HAProxy;
- **`:2379` на нодах кластера** — etcd внешний (P17);
- **`:6432` между нодами** — doorman всегда локален своей ноде
  (бэкенд строго `127.0.0.1:5432`).

---

## 3. Принципы

1. **Default deny**: разрешено только перечисленное в матрице; правила
   группируются по подсетям зон (app-net, shard-net, infra-net, admin-net).
2. **Приложению — два адреса**: `2379` (infra etcd) и `6432` (ноды шардов).
   Всё остальное для app-зоны закрыто.
3. **Межшард — только через HAProxy `:5432`**: подписки/tablesynс/mover не
   ходят в PG-ноды напрямую; источник видит IP HAProxy-нод (правило pg_hba
   `bucket_mover`, [11](11-bucket-sharding.md) §4).
4. **`:8008` не покидает зону shard/admin** — его клиенты: HAProxy,
   сверяющий демон, HAProxy-синкеры, админка.
5. **`:5432` PG напрямую** — loopback и реплики своего шарда; всё остальное
   проходит HAProxy (или аварийная админка).
6. **`:2379`** слушается только на infra-слое; клиенты — app (watch),
   Patroni, mover, админка, мониторинг ([04](04-deploy-etcd.md) — TLS etcd).

---

## 4. TLS (решение P17)

| Канал | Режим | Параметры |
|---|---|---|
| app → doorman `:6432` | **`sslmode=require`** | `tls_certificate` + `tls_private_key` в конфиге doorman; минимум TLS 1.2; `hostssl`-правила в `general.pg_hba` doorman; клиентский DSN роутера — `sslmode=require` ([11](11-bucket-sharding.md) §3) |
| doorman → PG (loopback) | без TLS | `server_tls_sslmode=disable` — трафик не покидает ноду; включить — одна строка, если потребует security-политика |
| app → etcd `:2379` | TLS | сертификаты etcd-слоя ([04](04-deploy-etcd.md)) |
| подписки межшард `:5432` | опционально `sslmode=require` | libpq-параметр в `CONNECTION` подписки (HAProxy прозрачен для TLS pass-through) |
| Patroni `:8008`, HAProxy `:7000` | без TLS | внутренняя зона кластера |

Принято для `:6432`: **require, не verify-full** — шифрование трафика и
кредов обязательно, верификация серверного сертификата клиентом не требуется:
адрес ноды приложение получает из etcd-контрол-плейна, т.е. доверие адресу
обеспечивает аутентификация самого etcd-канала (TLS + префикс кластера).

Нюансы эксплуатации: серверные сертификаты клиентской стороны doorman не
hot-reload'ятся (подхватываются новыми соединениями после рестарта/upgrade) —
заложить план ротации; сертификат с SAN на все ноды шардов или wildcard.

---

## 5. Аутентификация (решение P17)

**Клиент → doorman (`:6432`).** Логин/пароль единственной app-роли,
SCRAM-SHA-256. **Passthrough-режим pg_doorman** (дефолт, рекомендован
проектом): doorman проверяет креды клиента и **переиспользует его
криптографический proof** (SCRAM ClientKey) для аутентификации к PG —
plaintext-пароля в конфиге doorman нет, только хэш из `pg_authid`.

**Per-bucket роли — отклонены (2026-08-22).** Пул doorman — пара
`(database, user)`: каждая роль = отдельный пул со своим `pool_size` и
`max_db_connections`, общего капа «на ноду» нет ([12](12-bucket-pitfalls.md)
P14/P15) — 256 пулов ломают бюджет 55/ноду. Радиус компрометации кредов
app-роли = все бакеты кластера — принято вместе с инвариантом единой БД.
Права роли минимальны по построению: app ≠ owner, только DML +
`USAGE,UPDATE` на sequences, `CREATE` срезается мораторием P5.

**doorman → PG (loopback).** Passthrough: серверное соединение — от имени
клиентского юзера (`app`). pg_hba PG:

```
host    <C>    app    127.0.0.1/32    scram-sha-256
```

**Не trust**: trust на loopback открывал бы PG любому процессу на
скомпрометированной ноде без пароля, мимо doorman.

**Прочие входы PG (`:5432`):** `bucket_mover` — по IP HAProxy-нод своего
шарда ([11](11-bucket-sharding.md) §4); физрепликация — `replication`-строки
реплик кластера; миграционная роль-владелец — не через doorman, только с
admin-зоны через HAProxy.

**Admin-консоль doorman** (БД `pgdoorman` на `:6432`): `admin_username` /
`admin_password` из конфига; в `general.pg_hba` doorman — только с ноды:
`host pgdoorman admin 127.0.0.1/32 <метод>`.

---

## 6. Что получает компрометация (модель угроз)

| Компрометация | Доступ | Не получает |
|---|---|---|
| нода приложения | креды app-роли (запись во все бакеты кластера); чтение ключей своего префикса etcd | доступ к PG-нодам напрямую (firewall), запись в etcd |
| нода кластера-шарда | PG своей ноды (loopback + scram), конфиг/хэши doorman, HAProxy-трафик переездов | etcd-слой (`:2379` там не слушается), чужие шарды напрямую (pg_hba по IP HAProxy), другие ноды мимо firewall-матрицы |
| admin-хост | всё: Patroni, etcd, SQL, mover — максимальный радиус, держать минимальным по составу хостов | — |
| перехват сети | зашифровано: `:6432` (TLS require), `:2379` (TLS etcd); межшард `:5432` — опционально `sslmode=require` у подписок | — |

---

## Дальше

→ [11-bucket-sharding.md](11-bucket-sharding.md) — механика переездов,
[12-bucket-pitfalls.md](12-bucket-pitfalls.md) — реестр рисков (P17).
