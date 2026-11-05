# PG-кластер: топология и размещение

Уровень 3. Модель размещения нод: контейнер, порты и публикации, тома и
диски, сети, адресация и portalloc. Алгоритм планирования и закрепления
портов — [pgworker/01-placement.md](../pgworker/01-placement.md) §5;
подготовка хостов — [03-prerequisites.md](03-prerequisites.md).

## 1. Нода = один контейнер

Нода шарда — контейнер (plain) или сервис с `replicas=1` (swarm) с именем
`pgw-<C>-<X>-<n>` из образа **`pgworker-node`**:

- Spilo (`ghcr.io/zalando/spilo-18:4.1-p2`) — PostgreSQL 18 + Patroni 4.x;
- `pg_doorman` — пулер `:6432` (обязателен; бинарник — пин
  `DOORMAN_IMAGE`, сборка без него fail-fast);
- `pgnode` — агент ноды (supervisord автостартует только его; `spilo` и
  `pg_doorman` поднимает агент после доставки секретов);
- `supervisord`, `pgwnodetool master-lease` — служебные программы ноды.

Внутри контейнера все компоненты общаются через localhost — это работает и
в plain, и в swarm (в swarm нет host-network и «подов»). Один контейнер на
хост — отказ хоста теряет ровно одну ноду шарда; при `hosts >= replicas`
ноды шарда разносятся по разным хостам (анти-аффинити), иначе — равномерно
с видимым оператору фактом совпадения.

## 2. Порты

| Порт | Где слушает | Публикация на хост |
|---|---|---|
| PostgreSQL `:5432` | контейнер | `pg` — выделенный порт portalloc (админка, подписки переездов, SQL-операции) |
| Patroni REST `:8008` | контейнер | `patroni` — выделенный порт portalloc (пробы, switchover, метрики) |
| pg_doorman `:6432` | контейнер | `doorman` — выделенный порт portalloc (клиентский вход приложений) |
| PgNode API/метрики `:8080` | контейнер | **не публикуется** (адресация `alias:8080` в общей сети воркера и нод) |
| PgNode mTLS-канал `:8081` | контейнер | **не публикуется** (только PgWorker, внутри docker-сети) |

Внутренние порты контейнера фиксированы (`5432`/`8008`/`6432`); наружу
публикуются три порта ноды, выделенные **portalloc** — последовательные
слоты из `PgWorker:Docker:PortRange`. Порт-публикация — не константа
схемы «base+i»: каждая публикация получает следующий свободный слот,
закрепляется за нодой и переиспользуется при rebuild. Механика выбора,
кросс-кластерная занятость и глобальный portalloc-клэйм —
[pgworker/01-placement.md](../pgworker/01-placement.md) §5.

## 3. Тома и диски

- Данные — том `pgw-<C>-<X>-<n>-data` → `/home/postgres/pgdata`
  (дефолтный PGROOT Spilo; переопределение PGROOT ломает bootstrap).
  PGDATA и WAL живут на локальном диске ноды; общего NFS/SAN нет —
  репликация физическая, общего диска не требуется.
- Staging-каталога полного/verify НЕТ (tar-поток пишется прямо в
  multipart-части — [pgnode/01-placement.md](../pgnode/01-placement.md) §4);
  scratch дрилла —
  `/home/postgres/pgdata/backup-drill/<id>/pgdata` в том же data-volume.
- Секреты и серты — только tmpfs `/run/pgw` (в памяти контейнера); в файлы
  volume'ов секреты не попадают ([pgnode/01-placement.md](../pgnode/01-placement.md) §4).
- Том удаляется только при rebuild/deprovisioning; локальный диск — не
  источник гарантий durability: при потере хоста данные восстанавливаются
  репликацией или бэкапом.

Рекомендации по диску хоста (отдельный том под docker-volume, XFS/ext4,
SSD для etcd-узлов) — [03-prerequisites.md](03-prerequisites.md) §4.

## 4. Сети

- **Per-cluster `pgw-net-<C>`** — сеть кластера (создаёт движок при
  provisioning, удаляет при полном демонтаже; 409 = уже есть). Алиас ноды
  в сети — `<n>`; полное docker-имя — `pgw-<C>-<X>-<n>`. Per-cluster
  сеть изолирует DNS-зону кластера от соседних кластеров и тестов.
- **Сеть контура** (`PgWorker:Docker:ContourNetwork`, env
  `PGW_CONTOUR_NETWORK`) — внешняя сеть поставки: задана → ноды создаются
  в ней, `pgw-net-<C>` не создаётся; в swarm не поддерживается (fail-fast).
- **Сеть скрейпа** (`PgWorker:Docker:ScrapeNetwork`) — ноды дополнительно
  подключаются к сети мониторинга; несёт скрейп `:8008` и `:8080`, на
  клиентскую адресацию не влияет.
- **Общая сеть воркера и нод** — инвариант поставки: контейнеры PgWorker и
  нод всегда в одной сети (контур или per-cluster); этим работает
  адресация API PgNode `alias:8080`
  ([communications/02-channel-registry.md](../communications/02-channel-registry.md) №22).

Внутри шарда Patroni-репликация и резолв мастера идут по внутренним
адресам сети; наружу — только опубликованные порты.

## 5. Именование и адресация

| Сущность | Формат | Уникальность |
|---|---|---|
| Кластер | `<C>` | в пределах etcd-контура; префикс `/clusters/<C>/` |
| Шард | `<X>`, `^[a-z][a-z0-9_]{0,30}$` | в пределах кластера |
| Нода | `<n>` | в пределах шарда |
| Patroni scope | `<C>-<X>` | **глобально** в общем etcd (`/service/<scope>/`) |
| Контейнер | `pgw-<C>-<X>-<n>` | глобально в docker |

Совпадение scope у двух шардов сливает их ноды в один виртуальный
Patroni-кластер (гонка за leader-ключ, репликация между чужими данными) —
поэтому scope строится конкатенацией и обязан быть уникальным глобально.

Адреса, которые домен публикует в etcd:

| Ключ | Значение |
|---|---|
| `/pgworker/portalloc/<C>` | `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` (+ опц. `object` — усыновлённая нода, `alias`/`net` — сетевая идентичность для скрейпа) |
| `/clusters/<C>/shards/<X>/dsn` | `host=n1,n2,n3 port=p1,p2,p3 dbname=<C> user=<bucket_admin> password=…` — multi-host вход для подписок и админки; hosts/ports — по возрастанию имён нод |
| `/clusters/<C>/shards/<X>/master` | `host:<doorman-port>` — динамический адрес мастера для роутера приложений (lease; пишет Patroni-callback, сверяет и корректирует PgWorker) |
| `/clusters/<C>/shards/<X>/nodes/<n>/app_params` | `keyword=value` — серверные параметры подключения к конкретной ноде (`sslmode=require` и т.п.); клиент конкатенирует к своему DSN |

**Advertised-имя хоста** (`PgWorker:Docker:AdvertisedHost`): адреса нод в
etcd обязаны быть резолвимы клиентами записей — панелью. На
single-host/tunnel-развёртываниях (стенд) наружу отдаётся advertised-имя
(`host.docker.internal`), а не внутреннее имя docker-хоста. Усыновлённые
ноды advertised не получают — их адресация операторская.

## 6. Ресурсы ноды

Ресурсы контейнера — `request_cpu`/`request_mem` из
`/service/<scope>/request_{cpu,mem}`: лимиты plain (`NanoCPUs`,
`Memory`) или swarm (`TaskTemplate.Resources.Limits`), они же — входы
PGTune для параметров PostgreSQL. Заявка обязательна: без неё воркер не
знает размер ноды и не выдумывает дефолтов. `request_disk` примитива
лимита в docker не имеет и игнорируется (квоты volume — roadmap).

Расчёт параметров PG/PGTune и бюджет соединений doorman —
[05-postgres-patroni.md](05-postgres-patroni.md) §3,
[pgworker/01-placement.md](../pgworker/01-placement.md) §2.
