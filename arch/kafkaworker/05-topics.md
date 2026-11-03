# KafkaWorker: топики

Уровень 3. Функция «топики»: поддержание реестра топиков в etcd, исполнение
desired-заявок и lifecycle-заявок создания/удаления. Контракт ключей, формат
JSON и протокол автосинка — [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md)
§2–§3 (единственный источник истины); полный автомат —
[D. TopicSyncProcess](processes/d-topic-sync.md).

## 1. Что делает воркер

- **Реестр = факт Kafka.** Воркер периодически (тик `TopicSyncIntervalSec`)
  сверяет топики кластера с ключами `topics/<T>`: новый топик → put ключа с
  фактом; исчез без desired → del ключа; исчез с desired → `missing=true`.
  `__`-топики в реестр не попадают.
- **Desired-заявка панели.** Управляемые поля — `partitions` (только
  увеличение), `retention.ms`, `min.insync.replicas`; применив, воркер снимает
  `desired` (факт = заявке). Уменьшение partitions — перманентный отказ
  (панель отсекает раньше).
- **Lifecycle-заявки** `desired.create` / `desired.delete` исполняются перед
  факт-синком (порядок: чистка create-коллизий → delete → create → sync);
  один топик — одно действие за тик; CreateTopics/DeleteTopics идемпотентны
  по факту (`AlreadyExists`/`TopicDoesNotExist` = исполнено).
- Обе lifecycle-заявки на один топик запрещены панелью; при etcd-мусоре
  delete авторитетен, create чистится первым.

Создавать отсутствующие топики по одной desired-заявке воркер не умеет —
`missing=true` и алерт панели (roadmap).

## 2. Границы

Топики создаются/удаляются только явно (панелью, CLI или клиентами; 
`auto.create.topics.enable=false`). RF, partitions и конфиги топиков воркер
меняет только по заявкам; данные топиков не копирует — это делает
ребалансировка ([06-rebalancing.md](06-rebalancing.md)).
