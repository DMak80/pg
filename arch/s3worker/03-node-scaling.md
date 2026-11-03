# S3Worker: масштабирование состава кластера

Уровень 3. Функция «масштабирование»: добавление инстанса, вывод инстанса и
применение изменённых ресурсов. Полные автоматы — процессы
[D](processes/d-add-node.md), [E](processes/e-remove-node.md); модель
инстанса — [01-placement.md](01-placement.md); данные и их копии —
[05-mesh-governance.md](05-mesh-governance.md).

## 1. Добавление инстанса (D)

`nodes/<I>/state=NOT_INITIALIZED` у Active-кластера: резолв placement и
portalloc, выпуск PKI-материала инстанса, volume и контейнер с env из
текущих секретов, health-ожидание по advertised-адресу, bucket'ы инстанса →
`RUNNING` → RMW `endpoints` и актуализация `config.nodes`. Включение в mesh
— доводка G по `links` ([05-mesh-governance.md](05-mesh-governance.md)):
join недостающих пар и backfill существующих объектов — механика
реконфигурации ownS3 при добавленном соседе
([owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md) §2/§9).

Гварды: имя уникально в кластере, состав 1..9 (предел — по фактическому
составу), один инстанс за тик; повторный проход идемпотентен по факту
(порт из portalloc, `EnsureNode`/health/bucket — no-op).

Точные шаги и ожидания — [D. AddNodeProcess](processes/d-add-node.md).

## 2. Вывод инстанса (E)

Маркер панели `nodes/<I>/state=TO_REMOVE`: гварды — не последний инстанс
кластера (пул не может быть пуст) и наличие живой `RUNNING`-ноды кроме
выводимой (вывод не рвёт последнюю точку записи пула) → `REMOVING`, снос
контейнера и volume, удаление ключей инстанса (включая `pki`), имени из
`denylist` кластера и записи portalloc, RMW `endpoints` и `config.nodes`.
Связи выводимого инстанса сносятся доводкой G (его имя исчезло из состава —
записи у соседей «лишние»).

Отказ данных осознан: volume инстанса сносится; копия — только на
инстансах, связанных с ним в `links` (без связей объекты теряются —
[08-operability.md](08-operability.md) §6 R1).

Точные гварды и шаги — [E. RemoveNodeProcess](processes/e-remove-node.md).

## 3. Применение ресурсов инстанса

`PUT …/nodes/{n}/resources` ([07-api.md](07-api.md) §2) меняет декларацию
`nodes/<I>/resources`; сходимость обеспечивает надзор C rolling-пересозданием
контейнера с лимитами декларации — volume, PKI и адреса сохраняются,
пересоздание одно за тик ([02-cluster-lifecycle.md](02-cluster-lifecycle.md) §4).
`disk` — инфо-поле: декларация и наблюдение (`usage`), действий не вызывает.
