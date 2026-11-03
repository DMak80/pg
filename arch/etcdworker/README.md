# EtcdWorker — оркестратор etcd-контура установки

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`06`) и контракт домена (`00`),
уровень 4 — процессы и перечисления в `processes/`.

## 1. Назначение

**EtcdWorker** — фоновый сервис (.NET 10), хозяин etcd-контура
контроль-плейна установки: поддерживает состав etcd-узлов (декларативное
число **managed**-контейнеров, кворум) и выгружает DR-слепки etcd в S3
(etcd-backup кластер `/s3/clusters/`,
[s3-clusters/README.md](../s3-clusters/README.md)). **DR-бэкапы etcd в S3 делает
ТОЛЬКО EtcdWorker** — дублирующей механики нет.

**Граница бутстрапа**: воркер координируется через сам etcd (клэймы,
декларации) и НЕ поднимает контур с полного нуля. Первичный контур —
оператор: ЕДИНСТВЕННЫЙ seed-узел (рецепт `deploy/etcd` — compose одного узла
в общей сети контура, peer-URL по имени узла,
[04-deploy-etcd.md](../04-deploy-etcd.md) §8 п.5); воркер подключается к
ЖИВОМУ контуру по env endpoints и далее доводит состав до декларации:
**managed-узлы** (контейнеры/сервисы, созданные воркером:
детерминированные имена `etcdw-node-<k>`, peer-URL
`http://etcdw-node-<k>:2380`, label `etcdworker`) + **external-члены** (seed
и прочие поднятые оператором/компоузом — наблюдаются, не трогаются; после
доводки состава seed рекомендуется вывести —
[04-deploy-etcd.md](../04-deploy-etcd.md) §8 п.4). Дев-стенд живёт на
external-контуре (`as-etcd-1/2/3`) — воркер наблюдает и ведёт слепки,
managed-механика на стенде не используется.

## 2. Обязанности и границы

- **Единственный писатель** префиксов `/etcd/` и `/etcdworker/` (контракт —
  [00-etcd-contract.md](00-etcd-contract.md)); панель ходит только через его
  HTTP API ([05-api.md](05-api.md)).
- **Единственный**, кто меняет состав кластера etcd (member add/remove/
  promote), создаёт/удаляет managed-контейнеры и их volume, снимает слепки
  и выгружает их в S3.
- **Несколько инстансов работают одновременно**: координация — lease-клэймы
  в etcd (`/etcdworker/`), смерть контролирующего инстанса не роняет
  процессы (takeover ≤ TTL 15 с + тик); операции идемпотентны, значимое
  состояние — в etcd.
- **Не входит**: disaster-recovery при потере кворума (2/3 недоступны —
  runbook [09-troubleshooting.md](../09-troubleshooting.md) §4, автоматика
  без кворума невозможна); восстановление контура из слепка
  (`docs/runbook.md`); Patroni-ноды и прочие потребители контура (их
  адресация — env поставки); S3-кластеры и их секреты — зона S3Worker
  ([s3worker/README.md](../s3worker/README.md)).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Контракт домена | ключи `/etcd/` и `/etcdworker/`, их форматы, писатели и инварианты записи | [00-etcd-contract.md](00-etcd-contract.md) |
| Модель managed-узла | контейнер etcd и volume данных, имена, адресация по именам сети, публикации client-портов, размещение Plain/Swarm | [01-placement.md](01-placement.md) |
| Доводка состава и кворум | недобор (learner → promote), rolling-вывод, авто-замена мёртвого члена, external-члены | [02-membership.md](02-membership.md) |
| Надзор managed-узлов | health-пробы, пересоздание, конвергенция порта, снос осиротевших объектов | [03-node-supervision.md](03-node-supervision.md) |
| DR-слепки и выгрузка | расписание, снятие, экспорт в etcd-backup кластер, доводка, обслуживание контура | [04-snapshots.md](04-snapshots.md) |
| Внешний HTTP API | декларация состава, restart, `GET /api/config`, mTLS, дискавери | [05-api.md](05-api.md) |
| Эксплуатация | циклы и лидерство, надёжность, наблюдаемость, конфигурация, риски | [06-operability.md](06-operability.md) |

Точные автоматы — в процессах уровня 4, на которые ссылается каждая
функция; перечисления — [processes/enums.md](processes/enums.md).

## 4. Взаимодействие

- **AdminPanel → EtcdWorker**: только HTTP API (`/api`, mTLS) —
  [05-api.md](05-api.md); панельная проекция —
  [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.5,
  [adminpanel/03-panels.md](../adminpanel/03-panels.md).
- **EtcdWorker → etcd**: координация (клэймы, журнал, portalloc) и
  member-API (`/v3/cluster/*`) — [00-etcd-contract.md](00-etcd-contract.md).
- **EtcdWorker → docker**: managed-контейнеры и сеть контура —
  [01-placement.md](01-placement.md).
- **EtcdWorker → S3**: DR-слепки в etcd-backup кластер
  ([04-snapshots.md](04-snapshots.md)); креды/endpoints — из
  `/s3/clusters/<C>/secrets` (ключ `backups`), как у остальных клиентов пула.
- **Prometheus → EtcdWorker**: `/metrics` на порту Kestrel `:8080` —
  [18-metrics.md](../18-metrics.md).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага: повторная сверка факта (member в списке?
  контейнер есть? порт закреплён? слепок выгружен?) —
  [06-operability.md](06-operability.md) §2.
- Journal-before-manipulations, фиксированный порядок операций, клэймы и
  txn-сравнения — [17-synchronization-principles.md](../17-synchronization-principles.md)
  (S5–S7), [00-etcd-contract.md](00-etcd-contract.md) §2.
- Операции состава — только при здоровом кворуме контура; недобор и замена
  идут через learner (кворум не проседает ни на одном шаге) —
  [02-membership.md](02-membership.md).
- Все операции состава и надзора — под глобальным лидер-клэймом; слепки —
  singleton-цикл того же лидера ([06-operability.md](06-operability.md) §1).
- Локальный data-dir узла и том слепков — кэш доводки без гарантий
  durability; гарантии строятся от кворума и от выгруженных в S3 слепков
  ([06-operability.md](06-operability.md) §3).
