# EtcdWorker: контракт домена `/etcd/` и координации `/etcdworker/`

Уровень 3. Ключи домена и координационного слоя, их форматы, писатели и
инварианты записи. Перечисления значений — [processes/enums.md](processes/enums.md);
пространства соседних доменов — [s3-clusters/README.md](../s3-clusters/README.md)
(читаемые воркером), [backups/06-s3-layout.md](../backups/06-s3-layout.md) (layout объектов
слепков).

## 1. Ключи

| Ключ | Формат | Пишет | Назначение |
|---|---|---|---|
| `/etcd/config` | JSON `{"nodes":N,"created_unix":T}` | API воркера (панель) | декларация числа managed-узлов; 0/отсутствие — только наблюдение (external-контур, стенд) |
| `/etcd/nodes/<k>/state` | строка `NOT_INITIALIZED\|PROVISIONING\|RUNNING\|UNREACHABLE\|REMOVING` | воркер | жизненный цикл managed-узла (семантика kafka/valkey/s3) |
| `/etcd/nodes/<k>/placed_on` | строка — имя docker-хоста (Plain — единственный; Swarm — swarm-нода) | воркер | фактическое размещение (после резолва размещения, [01-placement.md](01-placement.md) §4) |
| `/etcdworker/leader` | lease TTL 15 с | воркер | глобальный лидер singleton-цикла (слепки/обслуживание/доводка состава) |
| `/etcdworker/locks/portalloc` | lease TTL 15 с | воркер | глобальный portalloc-клэйм (взаимоисключение секции «выбор порта → member-add») |
| `/etcdworker/instances/<id>` | lease TTL 15 с | воркер | живость инстансов |
| `/etcdworker/api/<id>` | lease TTL 15 с, JSON `{"url","instance","since_unix","cert_thumbprint"?}` | воркер | дискавери API (панель — единственный читатель мутаций) |
| `/etcdworker/portalloc` | JSON `{"<k>":{"host":"h1","client":18100}}` | воркер | закрепление host-портов клиентской грани managed-узлов (peer — имена сети, без host-портов; переживает rebuild) |
| `/etcdworker/work` | JSON `{"op","phase","updated_unix","instance","last_error"?}` | воркер | журнал фаз состава и надзора (journal-before-manipulations) |
| `/etcdworker/snapshots` | JSON `{"state":"OK"\|"FAILED","last_uploaded_unix"?,"last_object"?,"last_sha256"?,"size_bytes"?,"interval_min"?,"error"?}` | воркер (инстанс-исполнитель выгрузки) | статус DR-выгрузки (семантика покрытия — §3) |

Панель читает `/etcd/config`, `/etcd/nodes/*`, `/etcdworker/{api,snapshots}`;
координационные ключи — вне панельного контракта. Unknown-ключи — не ошибка
(толерантные парсеры, как pg/kafka/valkey/s3).

## 2. Инварианты записи

- **Единственный писатель домена**: `/etcd/` и `/etcdworker/` пишет только
  EtcdWorker (панель — через API воркера); исключение — серверный серт API
  `/workers/api_tls/etcdworker` (пишет панель,
  [05-api.md](05-api.md) §3).
- **Декларация** — замещается целиком: RMW по `mod_revision` (ключа нет —
  txn `version==0`), `created_unix` исходной декларации сохраняется при
  каждой замене. Проигранная гонка — транзиент, клиент повторяет.
- **Журнал** — journal-before-manipulations: запись фазы предшествует
  docker/member-мутации; takeover продолжает фазу по фактам, а не по журналу
  вслепую. Ключ журнала — БЕЗ суффикса кластера (домен один).
- **portalloc** — закрепление переживает rebuild; довыделение — только под
  глобальным клэймом; освобождение — вместе с remove члена.
- **Состояния узла** — пишет только воркер; переходы описаны в
  [processes/enums.md](processes/enums.md).

## 3. Статус DR-выгрузки

- `state` — `OK` (последняя выгрузка успешна) либо `FAILED` (сообщение — в
  `error`, поля последнего успеха сохраняются).
- `last_uploaded_unix` — метка **покрываемого** слепка (время снятия, не
  время заливки): инвариант отставания сравнивает её с новейшим локальным
  слепком; фактическое время заливки живёт только в
  `.meta.json` (`uploaded`).
- `last_object` — объект последней выгрузки, реально лежащий в S3;
  `last_sha256` — наблюдаемость (в решении о записи не участвует).
- `interval_min` — эффективный плановый интервал для панельного
  stale-алерта (2× интервала).
- Поле `enabled` НЕ пишется: выгрузка безусловна, «не настроено» отражает
  `FAILED` с маркером `etcd-backup-missing`
  ([04-snapshots.md](04-snapshots.md) §1).

## 4. Читаемые ключи чужих доменов

Воркер читает (и никогда не пишет) `/s3/clusters/<C>/{config,endpoints,secrets}`
etcd-backup кластера: признак `etcd_backup=true`, адреса инстансов и креды
ключа `backups` — живой резолв цели на каждую попытку выгрузки
([04-snapshots.md](04-snapshots.md) §1). Ключ `backups` для etcd-backup
кластера содержит спец-ключ из env S3Worker
([s3worker/04-security.md](../s3worker/04-security.md) §1).
