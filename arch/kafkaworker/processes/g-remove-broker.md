# G. RemoveBrokerProcess

Уровень 4. Демонтаж брокера по маркеру `TO_REMOVE`. Функция —
[03-broker-scaling.md](../03-broker-scaling.md) §2; drain —
[I. PartitionReassigner](i-partition-reassigner.md); контракт ключей —
[kafka-clusters/02-keys.md](../../kafka-clusters/02-keys.md) §1.

Маркер `TO_REMOVE`: guards (кластер Active; не controller; не последний
брокер; на брокере нет реплик партиций **включая internal-топики** — по
describe-all; реплики есть → journal-ожидание: процесс
[I](i-partition-reassigner.md) дренирует брокер, демонтаж продолжится сам
следующими тиками) → `state=REMOVING` → удалить контейнер+volume → del
префикс `brokers/<b>/` + RMW `endpoints` (убрать адрес) + portalloc-фильтрация
→ journal done. Идемпотентен на повторе. Демонтаж исполняется только по
пустому брокеру без USR (партиции затронутых топиков полностью синхронны) —
удаления, роняющие доступность, исключены.
