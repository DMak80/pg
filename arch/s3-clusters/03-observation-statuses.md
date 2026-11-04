# S3-кластеры: статусы наблюдения (связность и синхронность)

Уровень 3. Ключи `connectivity` и `sync` — единственный честный источник
сведений о связности и синхронности пула. Пишет их зонд
([s3worker/06-sync-probes.md](../s3worker/06-sync-probes.md)); контракт
форматов фиксируется здесь.

## 1. Ключи

| Ключ | Формат | Кто пишет |
|---|---|---|
| `/s3/clusters/<C>/connectivity` | JSON `{"state":"OK"\|"DEGRADED"\|"DOWN","checked_unix":T,"links":{"node1":{"node2":"ok"\|"offline",...},...},"probe_error"?}` | зонд (периодически + по команде API): связность нод между собой — активные репликационные ping-пробы. Зонд опрашивает `POST /admin/replication/probe` ([owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md) §2/§6) каждой ноды — параллельный ping её соседей исходящим репликационным транспортом. Матрица `links` — только НАСТРОЕННЫЕ линки (targets каждой ноды; полнота меша и зеркальность состава НЕ контролируются — настройка состава — зона оркестратора, не зонда). Ping-ок = «канал жив» — отдельно от доставки (лаг/очередь статуса) и сверки данных (sync-половина): в `connectivity` ложится только линк. Линков не настроено — `OK` с пустой матрицей, отсутствие репликации отражает sync-агрегат (PUT-пробы связность ownS3 не детектируют: путь записи без синхронных взаимодействий с соседями — PUT всегда 0 с, [owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md) §1) |
| `/s3/clusters/<C>/sync` | JSON `{"state":"SYNCED"\|"LAGGING"\|"DIVERGED","checked_unix":T,"pairs":[{"from":"node1","to":"node2","missing":N,"extra":N,"lag_bytes"?,"lag_objects"?}...],"last_error"?}` | зонд: сверка содержимого bucket между нодами — пары по настроенным линкам (объединение targets, как у connectivity; `from`/`to` — упорядоченная пара по Ordinal); элемент сверки — `(key, modTimeUnixMs)`: `missing` — элементы from, отсутствующие у to (нет ключа или другая версия), `extra` — наоборот; листинги/хвосты (штатные метрики репликации лаг данных не отражают — факт 7 `../../../OwnS3/docs/s3-replication.md`; админ-грань ownS3 sync-report — R5 `../../../OwnS3/docs/owns3-replication-requirements.md`) |

## 2. Агрегаты

`state` — по настроенным линкам:

- **connectivity**: `DOWN` — нет живых среди настроенных линков при непустом
  их составе; `DEGRADED` — часть настроенных линков offline; пустой состав
  настроенных линков — `OK` с пустой матрицей.
- **sync**: `LAGGING` — догон известен и мал/растёт; `DIVERGED` — устойчивое
  расхождение/дыра. Алерты панели: расхождение, отставание, нода вне контура
  — по `nodes/*/state`.

## 3. Границы и толерантность

- **Форматы толерантны к расширению полей**: парсеры панели читают известные
  поля, новые не ломают чтение ([02-keys.md](02-keys.md) §3).
- **Состав/здоровье нод** наблюдаются `nodes/*/state` + `usage`
  ([02-keys.md](02-keys.md) §1); отдельного health-ключа НЕТ — состояние
  живости несёт `state`, детальная телеметрия — метрики
  [metrics/README.md](../metrics/README.md).
- **Зонд ничего не настраивает и не лечит**: состав mesh доводит оркестратор
  ([s3worker/05-mesh-governance.md](../s3worker/05-mesh-governance.md)),
  восстановление полноты — reconcile-механика ownS3.

`connectivity` OK-кластера из 3 нод (full-mesh: строка ноды — исходы её
probe по собственным targets; линки симметричны):

```json
{"state":"OK","checked_unix":1760156900,
 "links":{"node1":{"node2":"ok","node3":"ok"},
          "node2":{"node1":"ok","node3":"ok"},
          "node3":{"node1":"ok","node2":"ok"}}}
```

`sync` SYNCED (пары без расхождений):

```json
{"state":"SYNCED","checked_unix":1760156900,
 "pairs":[{"from":"node1","to":"node2","missing":0,"extra":0},
          {"from":"node1","to":"node3","missing":0,"extra":0},
          {"from":"node2","to":"node3","missing":0,"extra":0}]}
```
