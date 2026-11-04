# EtcdWorker: модель managed-узла

Уровень 3. Как устроен managed-узел контура: контейнер etcd и его флаги,
volume данных, имена, адресация по именам сети и публикации client-портов,
размещение в Plain/Swarm. Процессы, использующие модель, —
[доводка состава](02-membership.md), [надзор](03-node-supervision.md);
контракт ключей — [00-etcd-contract.md](00-etcd-contract.md).

## 1. Транспорт docker и конфигурация

- **Транспорт** — общий `Shared.Docker` (unix/tcp+TLS/SSH), env-секреты
  `ETCDW_DOCKER_TLS_*`, `ETCDW_DOCKER_SSH_*`.
- **Конфиг** ([06-operability.md](06-operability.md) §4):
  `EtcdWorker:Docker { Mode, Hosts[{Name,Endpoint}], SwarmManager,
  Network, PortRange{From=18100,To=18199}, Images{Etcd}, Tls, Ssh,
  AdvertisedHost }`.
- **Режимы**: `Plain` — контур на ОДНОМ docker-хосте (`Hosts[]` — ровно один
  хост, fail-fast старта при большем: «мультихост — Swarm»); `Swarm` —
  managed-узлы поднимаются сервисами с placement constraint на
  `SwarmManager`.

## 2. Узел = контейнер/сервис `etcdw-node-<k>`

- **Образ** — внешний `Images{Etcd}` (канон `quay.io/coreos/etcd:v3.5.21`,
  зеркалируется в локальный registry, ранбук `docs/runbook.md`).
- **Имена**: контейнер `etcdw-node-<k>`, volume `etcdw-node-<k>-data`
  (`--data-dir=/etcd-data`); `<k>` — сквозной номер: замена узла получает
  СЛЕДУЮЩИЙ номер, имена не переиспользуются (data-dir старого при возврате
  не конфликтует). Label `etcdworker` на контейнере — перечисление своих
  объектов идёт по label (имена произвольны после замен).
  Restart-политика `unless-stopped`.
- **Флаги узла** (генерирует `EtcdNodeSpecBuilder`; параметры контура —
  [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2):

  | Флаг | Значение |
  |---|---|
  | `--name` | `etcdw-node-<k>` |
  | `--data-dir` | `/etcd-data` (volume `etcdw-node-<k>-data`) |
  | `--listen-peer-urls` | `http://0.0.0.0:2380` |
  | `--initial-advertise-peer-urls` | `http://etcdw-node-<k>:2380` — только имя узла в сети контура |
  | `--listen-client-urls` | `http://0.0.0.0:2379` |
  | `--advertise-client-urls` | СПИСОК `http://etcdw-node-<k>:2379` + (при заданном `AdvertisedHost`) `http://<advertised-host>:<client-port>` |
  | `--initial-cluster` | полный состав started-членов + сам узел (без него etcd не может fetch cluster info при `existing`) |
  | `--initial-cluster-state` | `existing` — managed-узел всегда стартует присоединяясь (`new` — только seed бутстрапа у оператора) |
  | `--heartbeat-interval` | `250` |
  | `--election-timeout` | `2000` |
  | `--auto-compaction-retention` | `1` |
  | `--quota-backend-bytes` | `8589934592` (8 GiB) |

- **Лимиты** контейнера настройкой не задаются: ресурсы узла — переменная
  контура, а не декларация панели; заявка ресурсов у домена отсутствует.

## 3. Адресация: имена сети, а не хосты

- Узлы контура (managed + seed/внешние) живут в ОДНОЙ общей сети
  `Docker:Network` (дефолт `etcdw-net`): Plain — bridge-сеть (ensure-создание
  идемпотентно воркером), Swarm — overlay-сеть (создаёт воркер), стенд/E2E —
  имя сети контура поставки.
- **Peer-адресация — только по именам узлов** (`http://etcdw-node-<k>:2380`,
  [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2): имя docker-хоста и
  `AdvertisedHost` в peer-адресах НЕ фигурируют. Peer-порт 2380 на хост НЕ
  публикуется — межузловой трафик остаётся внутри сети.
- **Клиентский порт 2379 публикуется** на host-порт из `PortRange` —
  единственный канал потребителей вне сети; закрепление
  `/etcdworker/portalloc` переживает rebuild ([00-etcd-contract.md](00-etcd-contract.md) §1).
- `--advertise-client-urls` — список «имя сети + advertised-host:client-port»:
  потребители внутри сети ходят по имени, потребители на хосте — по
  публикации; в список попадают только резолвимые URL (правило NXDOMAIN
  [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2). Прецедент — `as-etcd`
  стенда.
- **Смена адресов публикаций** (`AdvertisedHost`, `PortRange`): peer-URL
  членов не меняются, переиздание client-публикаций переживает rebuild;
  `advertise-client-urls` существующего члена фиксируется в кластере —
  плановая смена клиентского адреса отдельного члена вне штатных операций
  (замена члена пишет адреса нового,
  [06-operability.md](06-operability.md) §3).

## 4. Размещение

- **Plain** — единственный хост: `Hosts[]` ровно один (fail-fast при
  большем). Мультихост — только Swarm.
- **Swarm** — managed-узлы разносятся по swarm-нодам: анти-аффинити кворума
  (по разным нодам при числе нод ≥ N), иначе least-loaded по числу занятых
  тасков/портов, tie-break по имени; факт выбора — в journal и панели.
- **`placed_on` фиксирован** (том данных привязан к машине): перепривязка на
  месте не допускается; перенос — замена члена
  ([02-membership.md](02-membership.md) §3).

## 5. Portalloc

- **Диапазон** `18100–18199`, **1 client-порт на узел**; закрепление —
  `/etcdworker/portalloc` (переживает rebuild,
  [00-etcd-contract.md](00-etcd-contract.md) §1).
- **Занятость** для довыделения = docker-публикации ∪ записи portalloc;
  довыделение и member-add выполняются в одной секции под глобальным
  клэймом `/etcdworker/locks/portalloc` (взаимоисключение «выбор порта →
  запись порта → member add»): не взял клэйм — transient-отказ тика, ретрай
  следующим; смерть держателя гасит lease ≤ 15 с.
- **Освобождение** — только вместе с remove члена; осиротевшая запись
  (члена и docker-объекта нет) чистится reconcile-доводкой
  ([02-membership.md](02-membership.md) §5).

## 6. Инстансы самого воркера

- EtcdWorker — контейнер с `docker.sock` (или swarm-manager), volume
  слепков и TLS-секретов API, поставка через `deploy/docker-compose.yml`.
- **Два инстанса по умолчанию**: уникальный хост-порт API и уникальный
  `Api:AdvertiseUrl` на инстанс, дискавери-ключ `/etcdworker/api/<id>`
  ([05-api.md](05-api.md) §3); координация — lease-клэймы, лидер — один
  ([06-operability.md](06-operability.md) §1).
