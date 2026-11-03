# I. PartitionReassigner (drain + ребалансировка)

Уровень 4. Drain демонтируемых брокеров и balance по заявке панели.
Функция — [06-rebalancing.md](../06-rebalancing.md); миниклиент подачи —
[06 §1](../06-rebalancing.md); контракт ключей —
[kafka-clusters/04-coordination.md](../../kafka-clusters/04-coordination.md).

Reassignment партиций: (а) **drain** — опустошение брокеров `TO_REMOVE` с
репликами (разблокирует [G](g-remove-broker.md)); (б) **balance** —
ребалансировка размещения по заявке панели `/kafkaworker/rebalances/<C>`.
Чтение метаданных и подача плана — единый Kafka-протокол по CLIENT/admin:
describe — AdminClient, подача батча — AlterReassignments (KIP-455,
[06 §1](../06-rebalancing.md)) протокольным миниклиентом (Confluent.Kafka
операцию не экспонирует). Тик `ReassignIntervalSec`, только под клэймом;
describe-all (метаданные всех топиков **включая `__`**) → decide (чистые
функции плана) → act (submit батча ≤ `ReassignBatchPartitions` партиций) →
прогресс-ключ `/kafkaworker/reassignments/<C>`
([kafka-clusters/04-coordination.md](../../kafka-clusters/04-coordination.md)).

- **Drain-план** (per партиция с репликой на drain-брокере):
  newReplicas = старые реплики без drain (порядок сохранён — лидер меняется
  только если он и есть drain) + добор наименее загруженных живых брокеров
  (greedy по счётчику плана) до `min(len(старых), число целей)` реплик.
  Инвариант плана: `newReplicas ≥ min.insync.replicas` топика — иначе
  journal-отказ с причиной (оператор снижает minISR заявкой или добавляет
  брокеров); для internal-топиков minISR владеет воркер — снижает сам до
  `min(2, B')` (формулы [01-placement.md](../01-placement.md) §1 от
  фактического числа брокеров B'). Снижение RF (целей меньше старого RF) —
  допустимое следствие демонтажа; факт в реестре обновит автосинк
  [D](d-topic-sync.md).
- **Balance-план** (цель = converge к декларации): юзер-топики — RF
  `min(config.replication_factor, число целей)`, internal — формулы
  [01-placement.md](../01-placement.md) §1; первая реплика (лидер)
  сохраняется, добор остальных — наименее загруженные живые брокеры,
  детерминизм сортировкой (topic, partition, brokerId). Сходимость = факт ==
  план по всем партициям → del заявки + put `ticket_outcomes/<C>` outcome=done
  (kind=rebalance).
- **Цели переезда** — только `RUNNING`-брокеры (не TO_REMOVE/REMOVING/
  PROVISIONING/UNREACHABLE). Заявка balance при живых drain-кандидатах ждёт
  (journal waiting-drain): сначала демонтаж, потом баланс. Возрастная
  самозачистка: заявка balance старше `RotationTicketTimeoutSec`, чьи батчи
  не подаются (waiting-drain или передержка слепой пробы — прогресс-ключ
  balance не жив), снимается как не-начатая
  ([08-operability.md](../08-operability.md) §3, kind=rebalance); ИДУЩАЯ
  ребалансировка (прогресс-ключ жив) таймаутом не снимается — стагнацию
  `partitions_remaining` закрывает панельный алерт `kafka-reassignment-stale`.
- **Завершение** (критерий по факту метаданных): drain — drain-брокер
  отсутствует в Replicas всех партиций и затронутые топики без USR
  (ISR == assignment); баланс — факт == план. Слепая проба — никаких
  действий (передержка, как надзор [C](c-node-supervisor.md)): подача
  вслепую запрещена.
- **Идемпотентность/отказоустойчивость**: повторная подача того же
  assignment безопасна (семантика KIP-455); состояние = факт Kafka +
  прогресс-ключ (перестраиваем: потеря ключа — следующий тик пересчитает
  план от факта); смерть контроллера — takeover, in-flight reassignment
  живёт в Kafka; сбой между фактом и del заявки — повтор тика доигрывает.
  Дедуп подач: тот же батч не переподаётся чаще раза в
  `ReassignRetrySubmitSec` (защита от потерянного submit без спама подач).
- Батчи партиций (`ReassignBatchPartitions`) — самоограничение нагрузки
  (bandwidth-throttle — roadmap); новый батч — только после завершения
  предыдущего (по критерию выше).
