# F. SyncProbeProcess (зонд связности и синхронности)

Уровень 4. Периодические и внеплановые пробы, дающие статусы наблюдения
кластера. Функция — [06-sync-probes.md](../06-sync-probes.md); форматы ключей
и агрегатов — [s3-clusters/03-observation-statuses.md](../../s3-clusters/03-observation-statuses.md) (единственный источник истины).

```
F0 гейт периодичности: `checked_unix` ключей connectivity/sync не старше
   `SyncIntervalSec` и заявки на внеплановый зонд нет — no-op (journal не
   пишется)
F1 journal(op=sync-probe) под клэймом <C>; адреса — portalloc (advertised
   host + S3-порт); работают только RUNNING-инстансы
F2 connectivity: POST /admin/replication/probe каждой ноде — ping её
   targets исходящим репликационным транспортом; только настроенные линки;
   матрица → ключ connectivity (агрегат OK/DEGRADED/DOWN — 23 §3)
F3 sync: сверка содержимого bucket по парам настроенных линков
   (объединение targets): постраничный sync-report → при расхождении
   страницы — diff листингов (missing/extra/lag_bytes); бюджет
   `SyncMaxPages` на сторону; → ключ sync (SYNCED/LAGGING/DIVERGED)
F4 запись обеих половин — только после успеха: заявка /s3worker/probes/<C>
   сносится ПОСЛЕ успешной записи обеих половин (иначе внеплановый зонд
   потеряется); journal фаза probed (или probe-failed с last_error) +
   счётчики sync_probe_*
```

Границы: зонд только наблюдает — состав mesh не настраивает (зона G,
[05-mesh-governance.md](../05-mesh-governance.md)), данные не копирует
(reconcile-механика ownS3). Ping-ок = «канал жив», отдельно от доставки и
сверки данных; PUT-пробы не используются. Ветка не эксклюзивна: отказ зонда
не блокирует надзор/D/E следующих тиков, заявка живёт до исполнения.
Ошибка транзиента по отдельной паре/линку не фейлит тик — попадает в
`last_error`/warnings соответствующего ключа.
