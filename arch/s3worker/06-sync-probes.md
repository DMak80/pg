# S3Worker: зонд связности и синхронности

Уровень 3. Функция «зонд»: периодические и внеплановые пробы, дающие статусы
наблюдения кластера. Форматы ключей и состав полей —
[s3-clusters/03-observation-statuses.md](../s3-clusters/03-observation-statuses.md) (единственный источник истины); полный автомат
— [F. SyncProbeProcess](processes/f-sync-probe.md); механика репликации —
[owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md).

## 1. Когда исполняется

- **Периодически** — гейт `SyncIntervalSec` по `checked_unix` ключей
  наблюдения ([08-operability.md](08-operability.md) §5).
- **По команде панели** — `POST /api/s3/clusters/{c}/sync-probe`
  ([07-api.md](07-api.md) §2): заявка `/s3worker/probes/<C>` put-if-absent
  (already → идемпотентно), исполняется ближайшим тиком любого держателя
  клэйма, заявка сносится по исполнении.
- Ветка **не эксклюзивна**: идёт последней в Active-ветке тика, под клэймом
  `<C>`, journal `op=sync-probe`.

## 2. Половина connectivity

Ping-матрица настроенных линков: `POST /admin/replication/probe` каждой
`RUNNING`-ноде — её targets и живость каждого линка параллельным ping
исходящим репликационным транспортом. Контролируются только настроенные
линки: полнота меша и зеркальность состава — зона G
([05-mesh-governance.md](05-mesh-governance.md) §4). Ping-ок = «канал жив»,
отдельно от доставки и сверки данных; PUT-пробы не используются (путь записи
ownS3 без синхронных взаимодействий с соседями). Агрегат — `connectivity`
([s3-clusters/03-observation-statuses.md](../s3-clusters/03-observation-statuses.md)).

## 3. Половина sync

Сверка содержимого bucket между нодами по настроенным парам (объединение
targets, как у connectivity): постраничная сверка sync-report
(порядко-независимая свёртка) → при расхождении страницы — количественный
diff листингов S3-грани (`missing`/`extra`/`lag_bytes`, элемент сверки —
`(key, modTime)`); лаг доставки — статус приёмника (fetch-очередь,
`lag_objects`). Бюджет страниц на сторону пары — `SyncMaxPages`
([08-operability.md](08-operability.md) §5); сторона не уложилась или
отказала — пара не сверена (`last_error`, диагностика в warnings). Агрегат —
`sync` ([s3-clusters/03-observation-statuses.md](../s3-clusters/03-observation-statuses.md)).

## 4. Источник истины и границы

Собственный зонд — единственный честный источник синхронности: штатные
метрики репликации лаг данных не отражают (факт 7
`../../../OwnS3/docs/s3-replication.md`). S3-операции зонда — per-попыточный
таймаут + пульс живости ([08-operability.md](08-operability.md) §3). Зонд
ничего не настраивает и не лечит: состав — G, восстановление полноты —
reconcile-механика ownS3.
