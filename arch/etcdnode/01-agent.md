# EtcdNode: модель агента в узле контура

Уровень 3. Как устроен managed-узел контура с агентом: кастомный образ,
supervisord, env-конфигурация, тома, адресация. Создание контейнера узла
и флаги etcd — зона EtcdWorker
([etcdworker/01-placement.md](../etcdworker/01-placement.md)); контракт
ключей — [00-etcdnode-contract.md](00-etcdnode-contract.md).

## 1. Образ и процессы

- **Один контейнер `etcdw-node-<k>`** — кастомный локально собираемый
  образ `pgworker/etcdnode:dev` (docker-файл `docker/etcdnode/`):
  база — внешний образ etcd (канон `quay.io/coreos/etcd:v3.5.21`,
  зеркалируется в локальный registry,
  [operations/02-registry-images.md](../operations/02-registry-images.md))
  + self-contained publish агента EtcdNode, скопированный в образ
  (канон поставки узлов: publish НА ХОСТЕ, в контейнер — ТОЛЬКО
  артефакты; прецедент `pgworker-node`,
  [pgnode/01-placement.md](../pgnode/01-placement.md)) + `supervisor`.
  В registry локальный образ НЕ кладётся.
- **Supervisord** — CMD контейнера: две программы, `autorestart=true`:
  - `etcd` — флаги из env `ETCD_ARGS` (их формирует EtcdWorker при
    создании контейнера — тот же набор флагов,
    [etcdworker/01-placement.md](../etcdworker/01-placement.md) §2);
  - `etcdnode` — агент (env `ETCDNODE_*`, §3).
  Смерть любого процесса — перезапуск supervisord; узел (etcd-член) живёт
  независимо от агента и наоборот. Restart-политика контейнера
  `unless-stopped`.
- **Тома**: `/etcd-data` (volume `etcdw-node-<k>-data` — данные etcd) и
  `/snapshots` (volume `etcdw-node-<k>-snapshots` — локальный кэш
  слепков; переживает пересоздание контейнера, сносится вместе с узлом).

## 2. Адресация

- **Слепок снимается СО СВОЕГО узла**: `/v3/snapshot/save` на
  `http://localhost:2379`. Свой узел недоступен — тик лидера не исполняет
  снятие; лидерство уходит живому узлу по TTL lease (§4).
- **Координация** (ключ живости, лидерство) — клиент ТОЛЬКО своего
  `http://localhost:2379`: агент и etcd живут в одном контейнере и умирают
  вместе (решение «один контейнер», README §1) — failover-координация
  бессмысленна при мёртвом собственном etcd. Отказ своего etcd = lease не
  продлевается и гаснет по TTL ≤ 15 с, лидерство автоматически уходит
  живому узлу; статус `FAILED` в этом окне умерший лидер НЕ пишет — его
  пишет новый лидер первым же тиком (статус/экспорт — полный список
  endpoints тика, см. ниже).
- **Статус и экспорт** — клиент с failover: `localhost:2379` первым +
  client-грани членов из member list (имена сети
  `http://etcdw-node-<j>:2379` — канон адресации контура,
  [etcdworker/01-placement.md](../etcdworker/01-placement.md) §3). Состав
  перечитывается с localhost на старте тика — список актуален без
  отдельной конфигурации.
- **Обслуживание контура** (compact + defrag) — по client-грани каждого
  члена из member list (полный список тика).
- Наружу узел публикует ТОЛЬКО client-порт 2379 (портпаблиш EtcdWorker);
  новых публикаций агент не вводит. Наблюдаемость агента — ключи домена
  `/etcdnode/` (живость, статус) и логи контейнера; портов /healthz и
  /metrics НЕТ.

## 3. Конфигурация (env контейнера, доставляет EtcdWorker при создании)

```
ETCD_ARGS=<(флаги etcd: --name, --data-dir, peer/client URLs, ...)>
ETCDNODE_NODE_NAME=etcdw-node-<k>   # имя СВОЕГО узла: ключ живости
                                    # /etcdnode/instances/<k> (instanceId
                                    # ClaimStore) и .meta.json.instance
ETCDNODE_SNAPSHOTS__INTERVAL_MIN=360
ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC=5
ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC=300
ETCDNODE_SNAPSHOTS__TIMEOUT_SEC=30
ETCDNODE_SNAPSHOTS__RETENTION_FILES=10
ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS=3
ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN=60
ETCDNODE_S3_CA_PEM=<?>          # содержимое PEM per-install CA https-цели
                                # (опц.; источник — CaBundlePath конфига
                                #  воркера, читается на его хосте)
```

Значения — из конфига EtcdWorker (секция `NodeSnapshot`,
[etcdworker/06-operability.md](../etcdworker/06-operability.md) §5);
env иммутабелен в жизни контейнера — смена значений подхватывается
пересозданием контейнера (repair/замена члена строит spec из ТЕКУЩЕГО
конфига воркера). Fail-fast старта агента: переменные интервалов
распарсены и валидны (> 0, ретенции ≥ 1); `localhost:2379` доступен
(агент стартует независимо — supervisord перезапустит при отказе).

## 4. Жизненный цикл агента

- На старте: ставит ключ живости `/etcdnode/instances/<k>` (lease TTL
  15 с, keepalive-цикл 5 с), затем пытается захватить лидерство
  `/etcdnode/leader` ([00-etcdnode-contract.md](00-etcdnode-contract.md) §2).
- Лидер исполняет цикл слепков
  ([processes/c-snapshot-export.md](processes/c-snapshot-export.md));
  не-лидер — сон тика скана и пере-захват.
- Лидер НЕ может снять слепок со своего localhost (свой etcd-процесс
  мёртв): тик работу не исполняет (warning-лог), статус `FAILED` НЕ
  пишется и лидерство НЕ релизится — координация localhost-only, lease не
  продлевается и гаснет по TTL ≤ 15 с; работу и статус пишет агент живого
  узла, забравший лидерство (экспортёр нового лидера несёт полный список
  endpoints тика — §2).
- Остановка контейнера (снос/замена узла) — lease истекают ≤ 15 с;
  лидерство и живость уходят без ручных шагов.
