# Kafka-кластеры: клиентский дискавери (приложения)

Уровень 3. Что и в каком порядке читает приложение, как строится TLS- и
SASL-конфигурация. Ключи — [02-keys.md](02-keys.md) §1; сбойные случаи —
[06-fault-tolerance.md](06-fault-tolerance.md).

## 1. Что читает приложение

Приложение читает из etcd и только из него (полная аналогия pg
dsn/app_password/routing):

1. `/kafka/clusters/<C>/endpoints` → `bootstrap.servers`;
2. `/kafka/clusters/<C>/ca_pem` → доверие брокерам: TLS-транспорт
   (`security.protocol=SASL_SSL`, `sasl.mechanisms=PLAIN`; CA-серт(ы) —
   truststore). Truststore подаётся ФАЙЛОМ (`ssl.ca.location`): inline
   `ssl.ca.pem` librdkafka читает ОДИН серт — bundle окна ротации требует
   файла. В окне ротации CA ([kafkaworker/04-security.md](../kafkaworker/04-security.md) §4)
   значение — bundle OLD+NEW: читатель пишет его в файл как есть (конкатенация
   PEM валидна для truststore); клиент, перечитавший bundle до пересоздания
   брокеров, переживает замену сертов без реконфигурации;
3. `/kafka/clusters/<C>/app_user` + `app_password` → SASL/PLAIN креды
   (роль app — ACL на READ/WRITE/DESCRIBE топиков + группы,
   [kafkaworker/04-security.md](../kafkaworker/04-security.md) §2);
4. `/kafka/clusters/<C>/topics/` (префикс) → реестр топиков: имена +
   partitions + RF + конфиги (по нему выбирается топик и ожидаемая
   параллельность). Читатель реестра фильтрует leaf-ключи заявок
   `desired.{create,delete}` по числу сегментов (факт-ключи — 6 сегментов).

## 2. Библиотека дискавери

Библиотека дискавери (по образцу `Puzzle .../ha-db-etcd-clusters`,
watch-long-poll/poll) — в репозитории Puzzle; контракт etcd выше уже
содержит всё необходимое.
