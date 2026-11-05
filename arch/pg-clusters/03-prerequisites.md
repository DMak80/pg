# Подготовка хостов и образов

Уровень 3. Требования к docker-хостам, на которых PgWorker поднимает ноды
шардов, и к образам. Транспорт до Engine API и модель Plain/Swarm —
[pgworker/01-placement.md](../pgworker/01-placement.md) §3–§4; размещение
и диски — [02-topology.md](02-topology.md).

## 1. Хосты

- **Linux x86_64** — целевая платформа. arm64 — только платформа
  разработки (doorman-бинарник собирается под amd64; на arm64 допускается
  dev-контур).
- **Синхронизированное время (NTP)** — критично для etcd/raft и TTL
  Patroni: рассогласование часов ведёт к отказам записи etcd и ложным
  выборам лидера.
- **Docker Engine** (официальный, не `docker.io` из дистрибутива) и
  `docker compose` plugin v2 — для стенда и поставки. Версия Engine —
  актуальная стабильная; точный транспорт и режимы — ниже.
- **Транспорт Engine API** — только защищённый: `unix://` (локальный хост),
  `tcp://…:2376` с `--tlsverify` и per-install docker-PKI либо `ssh://`
  туннель с pin host-key. Plaintext `tcp://…:2375` — не канон (допустим
  только в dev/тестах, warning-лог воркера); в firewall-матрице
  отсутствует ([13-network-security.md](13-network-security.md) §2).
  Конфигурация — [pgworker/01-placement.md](../pgworker/01-placement.md) §4.
- **Диски**: отдельный локальный том под docker-volume нод; для узлов
  etcd — SSD/NVMe (etcd чувствителен к fsync); для PGDATA — XFS или ext4,
  `noatime`, размер с запасом под WAL (слоты переездов и failover slots
  держат WAL).
- **sysctl/limits** (host-level, для контейнеров — по необходимости):
  `vm.swappiness=1`, `vm.overcommit_memory=2`, `net.core.somaxconn=1024`,
  `tcp_max_syn_backlog=4096`; `nofile`/`nproc` для пользователя postgres
  (в контейнерах лимиты задаёт compose/движок). Swap под нагрузкой
  отключается.
- **Свободное место** под образы и docker-слои; данные бэкапов на диск не
  пишутся (полный — потоком в S3, verify — без распаковки), scratch дрилла
  живёт в data-volume ноды.

## 2. Образы

- **Внешние образы** (Spilo, etcd, pg_doorman и пр.) — из локального
  registry установки (`192.168.0.1:5000`); недоступен — Docker Hub/апстрим.
  Зеркалирование и список образов — `dev-stand/images/`, канон —
  [operations/02-registry-images.md](../operations/02-registry-images.md). Локально собираемые образы
  (`pgworker*:dev` и т.п.) в registry **не** кладутся.
- **Образ ноды `pgworker-node`** собирается из Spilo + `pg_doorman` +
  PgNode; мульти-арх, целевая платформа x64. Обязательный build-arg
  `DOORMAN_IMAGE` — пин контейнер-образа pg_doorman; сборка без него
  fail-fast ([pgworker/01-placement.md](../pgworker/01-placement.md) §1).
- **Образ PgWorker** — `pgworker:dev`/поставка; запускается только
  контейнером (никогда хост-процессом), с env-секретами и доступом к
  Engine API ([pgworker/01-placement.md](../pgworker/01-placement.md) §5).
- Отдельных образов/контейнеров для операций ноды (полный, verify,
  restore, drill) нет — их исполняет PgNode внутри контейнера ноды
  ([pgnode/01-placement.md](../pgnode/01-placement.md) §1).

## 3. etcd-контур

Узлы etcd — отдельный infra-слой, не на нодах кластера:
3 узла (нечётное число), кворум 2/3, SSD, NTP, TLS. Состав доводит
EtcdWorker; рецепт seed-бутстрапа и адресация потребителей —
[04-etcd-dcs.md](04-etcd-dcs.md) §2. Ноды кластера ходят в etcd по
адресам `PgWorker:Etcd:AdvertisedEndpoints` (формат `host:port` без
scheme); сами etcd-узлы на нодах не поднимаются.

## 4. Сеть и firewall

Доступ между зонами — по матрице [13-network-security.md](13-network-security.md)
§2: default deny, разрешены только перечисленные каналы (приложения →
doorman `:6432`, ноды → etcd `:2379`, подписки переездов → pg-порт ноды,
PgWorker → Engine API `:2376`/SSH, панель/Prometheus → Patroni `:8008`,
PgWorker → PgNode `:8081` внутри docker-сети). Порты нод наружу —
публикации portalloc, а не фиксированные литералы.

## 5. Чек-лист готовности хоста

```text
[ ] Linux x86_64 (arm64 — только dev), NTP синхронизирован
[ ] Docker Engine + docker compose plugin v2, hello-world запускается
[ ] Transport Engine API: unix / tcp:2376+mTLS / ssh с pin host-key
[ ] Отдельный локальный том под docker-volume нод, место под WAL
[ ] sysctl/limits применены, swap отключён под нагрузкой
[ ] Внешние образы доступны из локального registry
[ ] Образ pgworker-node собран с DOORMAN_IMAGE (fail-fast без него)
[ ] Firewall открывает только каналы матрицы 13
[ ] NTP и диск etcd-узлов соответствуют требованиям 04
```
