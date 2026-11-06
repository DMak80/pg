# EtcdWorker: контракт домена `/etcd/` и координации `/etcdworker/`

Уровень 3. Ключи домена и координационного слоя, их форматы, писатели и
инварианты записи. Перечисления значений — [processes/enums.md](processes/enums.md);
соседний домен координации агентов слепков —
[etcdnode/00-etcdnode-contract.md](../etcdnode/00-etcdnode-contract.md)
(`/etcdnode/`, воркером не трогается).

## 1. Ключи

| Ключ | Формат | Пишет | Назначение |
|---|---|---|---|
| `/etcd/config` | JSON `{"nodes":N,"created_unix":T}` | API воркера (панель) | декларация числа managed-узлов; 0/отсутствие — только наблюдение (external-контур, стенд) |
| `/etcd/nodes/<k>/state` | строка `NOT_INITIALIZED\|PROVISIONING\|RUNNING\|UNREACHABLE\|REMOVING` | воркер | жизненный цикл managed-узла (семантика kafka/valkey/s3) |
| `/etcd/nodes/<k>/placed_on` | строка — имя docker-хоста (Plain — единственный; Swarm — swarm-нода) | воркер | фактическое размещение (после резолва размещения, [01-placement.md](01-placement.md) §4) |
| `/etcdworker/leader` | lease TTL 15 с | воркер | глобальный лидер singleton-цикла (доводка состава/надзор) |
| `/etcdworker/locks/portalloc` | lease TTL 15 с | воркер | глобальный portalloc-клэйм (взаимоисключение секции «выбор порта → member-add») |
| `/etcdworker/instances/<id>` | lease TTL 15 с | воркер | живость инстансов |
| `/etcdworker/api/<id>` | lease TTL 15 с, JSON `{"url","instance","since_unix","cert_thumbprint"?}` | воркер | дискавери API (панель — единственный читатель мутаций) |
| `/etcdworker/portalloc` | JSON `{"<k>":{"host":"h1","client":18100}}` | воркер | закрепление host-портов клиентской грани managed-узлов (peer — имена сети, без host-портов; переживает rebuild) |
| `/etcdworker/work` | JSON `{"op","phase","updated_unix","instance","last_error"?}` | воркер | журнал фаз состава и надзора (journal-before-manipulations) |

Панель читает `/etcd/config`, `/etcd/nodes/*`, `/etcdworker/api/*`;
статус DR-выгрузки слепков — `/etcdnode/snapshots` (домен EtcdNode).
Координационные ключи — вне панельного контракта. Unknown-ключи — не
ошибка (толерантные парсеры, как pg/kafka/valkey/s3).

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
