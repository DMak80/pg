# KafkaWorker: масштабирование брокеров

Уровень 3. Функция «масштабирование»: добавление брокера, снятие брокера и
rolling-перегенерация ресурсов. Полные автоматы — процессы
[F](processes/f-add-broker.md), [G](processes/g-remove-broker.md),
[J](processes/j-node-regenerator.md); модель узла —
[01-placement.md](01-placement.md).

## 1. Добавление брокера (F)

`brokers/<b>/state=NOT_INITIALIZED` у Active-кластера: план host/порт и
`role=broker`, контейнер (кворум controller'ов уже зафиксирован), ожидание
появления в кластере, добавление адреса в `endpoints`, `RUNNING`. Добавляемые
брокеры всегда broker-only — контроллерный кворум не меняется. Уже RUNNING —
no-op.

Точные шаги и portalloc-ожидания — [F. AddBrokerProcess](processes/f-add-broker.md).

## 2. Снятие брокера (G)

Маркер `TO_REMOVE` снимается только при выполнении guards: кластер Active,
брокер не controller, не последний, на нём нет реплик партиций (включая
internal-топики), а затронутые топики без USR. Брокер с репликами сначала дренируется
процессом [I](processes/i-partition-reassigner.md); демонтаж — удаление
контейнера и тома, чистка `brokers/<b>/`, `endpoints` и portalloc. Удаления,
роняющие доступность, исключены.

Точные guards и шаги — [G. RemoveBrokerProcess](processes/g-remove-broker.md).

## 3. Rolling-перегенерация ресурсов (J)

Лимиты контейнера (cpu/mem) сводятся к декларации `brokers/<b>/resources`
пересозданием: docker не меняет лимиты живого контейнера. Триггер — только
расхождение cpu/mem; env-дрейф триггером не является. Один брокер за тик, том
сохраняется, прогресс — `/kafkaworker/regens/<C>`; чужие rolling/переезды
(ротации, reassignment) не смешиваются.

Точные условия и идемпотентность — [J. NodeRegenerator](processes/j-node-regenerator.md).
