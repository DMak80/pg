# KafkaWorker: ребалансировка партиций

Уровень 3. Функция «ребалансировка»: drain демонтируемых брокеров и balance
размещения по заявке панели. Полный автомат —
[I. PartitionReassigner](processes/i-partition-reassigner.md); контракт
ключей — [kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md).

## 1. Протокольный миниклиент AlterReassignments

Подача reassignment-плана — операция Kafka-протокола
`AlterPartitionReassignments` (KIP-455, apiKey 45, версия 0) из процесса
воркера: Confluent.Kafka/librdkafka операцию не экспонирует, docker exec с
`kafka-reassign-partitions.sh` не применяется
([25-component-communications.md](../25-component-communications.md) §1 —
только сетевые протоколы). Мини-клиент — внутренний компонент адаптера
AdminClient (seam `AlterReassignmentsAsync`):

- **Транспорт**: короткоживущее TCP-соединение (одна подача = одно
  соединение) к CLIENT endpoint из `endpoints`, попытка по очереди до первого
  успеха; TLS (`SslStream`) с доверием per-cluster CA `ca_pem` (все
  CERTIFICATE-блоки — bundle OLD+NEW в окне ротации
  [K](processes/k-ca-rotator.md)) и SAN-проверкой хоста; SASL/PLAIN
  (admin-креды) поверх TLS — тот же контракт дискавери
  [kafka-clusters/05-client-discovery.md](../kafka-clusters/05-client-discovery.md), что у AdminClient.
- **Диалог** (версии фиксированы, брокер `apache/kafka:4.0.0`): ApiVersions v0
  до аутентификации (наличие apiKey 45 — fail-fast) → SaslHandshake v1
  (PLAIN) → SaslAuthenticate v0 → AlterPartitionReassignments v0 (timeout_ms +
  список топик/партиция/реплики).
- **Ответ**: top-level error + per-partition коды; любой ненулевой код —
  `Result.Failed` (переподача следующим тиком), успех — план принят;
  идемпотентность повторной подачи — семантика KIP-455. Прогресс наблюдается
  describe-ом, а не ожиданием подачи.
- **Бюджет**: внешний — весь вызов со всеми endpoint-попытками —
  `Thresholds:ReassignSubmitSec`; per-endpoint — 30 с. Ошибки транспорта/
  протокола — `Result.Failed`, паники процессов нет.

## 2. Drain и balance

- **Drain** опустошает брокеров `TO_REMOVE`, разблокируя их снятие
  ([G](processes/g-remove-broker.md)); **balance** приводит размещение к
  декларации по заявке панели.
- План строится по факту метаданных (describe-all, включая `__`): сохраняет
  лидера, добирает наименее загруженных `RUNNING`-брокеров, держит
  `newReplicas ≥ min.insync.replicas`; для internal-топиков minISR снижает сам
  воркер.
- Цели переезда — только `RUNNING`; при живых drain-кандидатах balance ждёт
  демонтажа. Подача — батчами `ReassignBatchPartitions`; новый батч — только
  после завершения предыдущего; повторная подача безопасна; состояние — факт
  Kafka + прогресс-ключ.

Инварианты плана, критерии завершения, дедуп подач и обработка слепой пробы —
[I. PartitionReassigner](processes/i-partition-reassigner.md).

## 3. Экспирация

Заявка balance старше `RotationTicketTimeoutSec`, чьи батчи не подаются
(waiting-drain или слепая проба), снимается как не-начатая; идущая
ребалансировка таймаутом не снимается — стагнацию закрывает панельный алерт
`kafka-reassignment-stale`. Общая политика —
[08-operability.md](08-operability.md) §3.
