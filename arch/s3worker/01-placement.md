# S3Worker: модель размещения и инстансы кластера

Уровень 3. Как устроены инстансы кластера: контейнер ownS3 и volume данных,
S3-порт, placement-селекторы и привязка к хостам, advertised-адреса, сеть,
режимы Plain/Swarm, поставка и инстансы самого воркера. Процессы,
использующие модель, — [жизненный цикл](02-cluster-lifecycle.md),
[масштабирование](03-node-scaling.md); секреты и PKI —
[безопасность](04-security.md).

## 1. Транспорт docker и конфигурация

- **Транспорт** — общий клиент `Shared.Docker` (канон-суперсет, образец
  [pgworker/01-placement.md](../pgworker/01-placement.md) §4): plain/swarm, транспорт
  unix/tcp+TLS/SSH; env-секреты `S3W_DOCKER_TLS_{CA,CERT,KEY}`,
  `S3W_DOCKER_SSH_KEY[_PATH]`, `S3W_DOCKER_SSH_FINGERPRINT`.
- **Конфиг** ([08-operability.md](08-operability.md) §5):
  `S3Worker:Docker { Mode, Hosts[{Name,Endpoint}], SwarmManager,
  PortRange{From,To}, Images{OwnS3}, Tls, Ssh, ContourNetwork,
  AdvertisedHost }`.
- **Режимы Plain / Swarm** (порт режимов PgWorker,
  [pgworker/01-placement.md](../pgworker/01-placement.md) §3): plain — `Hosts[{Name,
  Endpoint}]`, per-host клиент Engine API, контейнеры `s3w-<C>-<I>`,
  restart-политика `unless-stopped`; swarm — `SwarmManager`, инстанс =
  сервис `replicas=1`, placement constraint, `publish mode=host`.
  Перечисление объектов кластера (сироты, занятость) — по префиксу
  `s3w-<C>-` + label `s3worker` (имена инстансов произвольные — перебор
  детерминированных имён невозможен).

## 2. Инстанс кластера = контейнер `s3w-<C>-<I>`

- **Образ** — `owns3:dev` (`Images{OwnS3}`): внешний, собирается
  репозиторием `../OwnS3` (`deploy/build-image.sh`), pg его не собирает и в
  registry не кладёт (правило — `../operations/02-registry-images.md`). Инстанс — ownS3
  single-drive: данные в одном каталоге `OWNS3_DATA_DIR=/data`.
- **Volume данных** `s3w-<C>-<I>-data` → `/data`: данные объектов и
  targets/cursors репликации переживают пересоздание контейнера.
  Платформа — единственная: `platform=owns3` ([s3-clusters/05-platform.md](../s3-clusters/05-platform.md)).
- **S3-порт**: контейнерный `9000` → host-порт из portalloc (§4);
  клиентская грань и межнодовые линки адресуются одним advertised
  `host:port`.
- **Лимиты** контейнера из `resources` (cpu/mem; `disk` — инфо-поле,
  enforcement-примитива нет, [s3-clusters/02-keys.md](../s3-clusters/02-keys.md)); расхождение
  живого контейнера с декларацией сводит надзор пересозданием
  ([02-cluster-lifecycle.md](02-cluster-lifecycle.md) §4).
- **Bucket'ы инстанса** — per-install bucket на каждом инстансе
  (и дополнительно фиксированный `etcd-backups` у etcd-backup кластера);
  создаёт воркер, идемпотентно ([02-cluster-lifecycle.md](02-cluster-lifecycle.md) §5).
- **Env** инстанса (креды клиентской грани, репликационный материал,
  параметры reconcile) генерирует воркер при создании — состав и правила
  доставки — [04-security.md](04-security.md) §3.
- **Имя инстанса** задаётся декларацией (`nodes/<I>`), паттерн и
  уникальность — [07-api.md](07-api.md) §3; имена docker-объектов
  детерминированы от него.

## 3. Placement-селекторы и привязка к хостам

Контракт привязки — [s3-clusters/04-placement.md](../s3-clusters/04-placement.md); поведение воркера:

- **Атрибуты хоста**: plain — имя из `Hosts[]` (атрибут `docker_host`);
  swarm — hostname нод и `node.labels.*` (атрибуты `swarm_node` /
  `swarm_labels`, `GET /nodes`).
- **Резолюция селектора** — фаза планирования provisioning/add-node:
  конъюнкция равенств → список кандидатов → детерминированный выбор
  (least-loaded по числу занятых портов/тасков, tie-break по имени) →
  ключ `nodes/<I>/placed_on`. Ноль кандидатов — transient-фейл фазы
  (ретрай тиком: хост может появиться в конфигурации).
- **Привязка фиксирована**: размещение не перепривязывается на месте —
  перенос инстанса выполняется выводом и добавлением
  ([03-node-scaling.md](03-node-scaling.md)); отказ docker-хоста оставляет
  инстансы на нём `UNREACHABLE` до возврата машины
  ([08-operability.md](08-operability.md) §3).
- **Анти-аффинити автоматикой не вводится**: раснос по площадкам —
  обязанность оператора (явные селекторы); канон фиксирует рекомендацию
  «ноды одного кластера — на разных хостах/площадках»
  ([s3-clusters/04-placement.md](../s3-clusters/04-placement.md)).

## 4. Portalloc и advertised-адреса

- **Диапазон** `18000–18999` (продолжение ряда pg / kafka / valkey);
  **1 S3-порт на инстанс**, закрепление — `/s3worker/portalloc/<C>`
  (переживает rebuild, [s3-clusters/08-coordination.md](../s3-clusters/08-coordination.md)).
- **Занятость** для довыделения = docker-публикации ∪ portalloc ВСЕХ
  кластеров; довыделение — только держателем глобального клэйма
  `/s3worker/locks/portalloc` (паттерн portalloc-клэйма:
  [pgworker/01-placement.md](../pgworker/01-placement.md) §5). Не взял клэйм — InProgress
  (следующий тик); смерть держателя гасит lease ≤ 15 с — takeover без
  оператора. Полностью закреплённый portalloc (rebuild, повторный проход)
  клэйма не требует.
- **Восстановление адреса** безадресного инстанса — лестница E9
  ([synchronization/README.md](../synchronization/README.md)):
  до любых деструктивных действий — portalloc → реконструкция из inspect
  живого контейнера (published-порт) → новая аллокация
  ([02-cluster-lifecycle.md](02-cluster-lifecycle.md) §4).
- **Advertised-адрес** — только от фактических публикаций: хост —
  `S3Worker:Docker:AdvertisedHost`, если задан, иначе имя docker-хоста
  размещения (single-host/стенд — [pgworker/01-placement.md](../pgworker/01-placement.md) §5
  п.5; multi-host — фактические имена хостов). Требование: значение обязано
  резолвиться клиентами — оно попадает в `endpoints` и в
  `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` ([04-security.md](04-security.md) §3).

## 5. Сеть

- **Per-cluster сеть НЕ создаётся**: межнодовая коммуникация mesh и клиенты
  идут по advertised `host:port` — единый namespace адресов; отказ от
  аналога `pgw-net-<C>` осознанный (внутреннего кластерного протокола вне
  S3-грани у нод нет). Домен не порождает `*-net-*` объектов и не расходует
  пул подсетей docker.
- **Опция `ContourNetwork`** (стенд/E2E) — инстансы создаются в сети
  контура поставки; адресация остаётся advertised-правилом §4, сеть
  воркером не создаётся и не удаляется (владелец — поставка/сценарий;
  образец — [pgworker/01-placement.md](../pgworker/01-placement.md) §1). Не поддерживается в
  `Mode=Swarm` — fail-fast старта: сеть overlay назначает сам Swarm.

## 6. Инстансы самого воркера

- S3Worker — контейнер с `docker.sock` (или swarm-manager), volume
  снапшотов и TLS-секретов API, поставка через `deploy/docker-compose.yml`.
- **Два инстанса по умолчанию** (образец pg —
  [pgworker/01-placement.md](../pgworker/01-placement.md) §5): уникальный хост-порт API и
  уникальный `Api:AdvertiseUrl` на инстанс, общие тома снапшотов и
  API-TLS; уникальность — случайный `InstanceId` без env-идентификации,
  дискавери-ключ `/s3worker/api/<id>` ([07-api.md](07-api.md) §4).
- **Где это используется**: создание кластера
  ([A. ProvisioningProcess](processes/a-provisioning.md)), добавление
  инстанса ([D. AddNodeProcess](processes/d-add-node.md)), пересоздание
  надзором ([C. NodeSupervisor](processes/c-node-supervisor.md)), ротация
  кредов ([H. SecretRotator](processes/h-credentials-rotator.md)).
