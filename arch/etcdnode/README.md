# EtcdNode — агент DR-бэкапа внутри узлов etcd-контура

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`03`) и контракт домена (`00`),
уровень 4 — процессы в `processes/`.

## 1. Назначение

**EtcdNode** — агент (.NET), живущий ВНУТРИ каждого managed-узла
etcd-контура: контейнер узла `etcdw-node-<k>` собран кастомным образом
(etcd + агент под supervisord, модель — [01-agent.md](01-agent.md)).
Агент снимает DR-слепки etcd со СВОЕГО узла и выгружает их в S3
(etcd-backup кластер `/s3/clusters/`,
[s3-clusters/README.md](../s3-clusters/README.md)). **DR-бэкапы etcd в S3
делают ТОЛЬКО агенты EtcdNode** — дублирующей механики нет (зона EtcdWorker
демонтирована; [etcdworker/README.md](../etcdworker/README.md) §2).

Бэкап живёт, пока жив контур: не зависит от доступности инстансов
воркеров. Исполняет работу один агент-лидер (lease-клэйм
`/etcdnode/leader`); слепок снимается с `localhost` СВОЕГО узла.

## 2. Обязанности и границы

- **Единственный писатель** домена `/etcdnode/` (контракт —
  [00-etcdnode-contract.md](00-etcdnode-contract.md)).
- Снятие DR-слепков (плановое и внеочередное — только по ВАЖНЫМ
  изменениям, пульсы сервисов не триггерят), выгрузка пар
  `snapshot-<id>.db` + `.meta.json` в bucket `etcd-backups`, ретенция,
  доводка отставания, обслуживание контура (compact + defrag).
- **Не входит**: управление составом контура (member add/remove/promote,
  надзор контейнеров — зона EtcdWorker,
  [etcdworker/02-membership.md](../etcdworker/02-membership.md));
  disaster-recovery при потере кворума (runbook
  [pg-clusters/09-troubleshooting.md](../pg-clusters/09-troubleshooting.md)
  §5); восстановление контура из слепка (runbook
  [03-restore.md](03-restore.md)); S3-кластеры и их секреты — зона
  S3Worker ([s3worker/README.md](../s3worker/README.md)).
- **Только managed-узлы**: external-контуры (дев-стенд `as-etcd-*`, seed)
  агента не содержат — DR-слепки на них НЕ снимаются
  ([etcdworker/README.md](../etcdworker/README.md) §1).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Контракт домена | ключи `/etcdnode/`, их форматы, писатели и инварианты записи | [00-etcdnode-contract.md](00-etcdnode-contract.md) |
| Модель агента | образ узла (etcd + агент, supervisord), env-конфигурация, тома, адресация | [01-agent.md](01-agent.md) |
| DR-слепки и выгрузка | расписание с фильтром важности изменений, снятие с localhost, экспорт, доводка, обслуживание контура | [02-snapshots.md](02-snapshots.md) |
| Восстановление из S3 | операторский runbook: диагностика, целостность, restore, контроль | [03-restore.md](03-restore.md) |

Точный автомат — [processes/c-snapshot-export.md](processes/c-snapshot-export.md)
(уровень 4).

## 4. Взаимодействие

- **EtcdWorker → EtcdNode**: создаёт контейнеры managed-узлов из образа
  агента, доставляет конфигурацию env — [01-agent.md](01-agent.md);
  больше воркер функцию бэкапа не касается.
- **EtcdNode → etcd**: слепок `/v3/snapshot/save` на localhost СВОЕГО
  узла; координация и статус — с failover по составу (localhost + члены
  из member list) — [00-etcdnode-contract.md](00-etcdnode-contract.md).
- **EtcdNode → S3**: DR-слепки в etcd-backup кластер
  ([02-snapshots.md](02-snapshots.md) §1); креды/endpoints — живой
  резолв из `/s3/clusters/` на каждую попытку.
- **AdminPanel → `/etcdnode/snapshots`**: статус DR-выгрузки — панельные
  алерты и карточка ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага (put поверх; повтор тика перепроверяет
  факт «слепок выгружен») — [02-snapshots.md](02-snapshots.md) §4.
- Лидерство lease-клэймом TTL 15 с, takeover ≤ TTL + тик скана —
  [00-etcdnode-contract.md](00-etcdnode-contract.md) §2.
- Локальный том слепков узла — кэш доводки без гарантий durability;
  источник истины для восстановления — выгруженные объекты S3
  ([02-snapshots.md](02-snapshots.md) §2).
- Смерть агента не роняет узел: supervisord перезапускает процесс,
  etcd-член жив независимо ([01-agent.md](01-agent.md) §1).
