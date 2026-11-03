# Kafka-кластеры: координация воркера `/kafkaworker/`

Уровень 3. Служебный слой KafkaWorker. Порт схемы `/pgworker/`
([14-pgworker.md](../14-pgworker.md) §3.3) один в один, свой префикс.

## 1. Ключи

| Ключ | Тип | Назначение |
|---|---|---|
| `/kafkaworker/leader` | lease TTL 15 с | лидер singleton-задач (регулярные снапшоты P12) |
| `/kafkaworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/kafkaworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` |
| `/kafkaworker/portalloc/<C>` | обычный | `{"broker<k>":{"host":"h","client":16001}}` — закрепление клиентских портов (переживает rebuild) |
| `/kafkaworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** ([kafkaworker/01-placement.md](../kafkaworker/01-placement.md) §1): взаимоисключение секции довыделения клиентских портов «чтение занятости → выбор портов → запись `/kafkaworker/portalloc/<C>`» (provision / add-broker) — пер-кластерные клэймы кросс-кластерную гонку не закрывают. Value: `{"instance":"<id>","since_unix":…}`. Захват txn `version==0` + put-with-lease; освобождение по завершении секции (del + revoke lease), смерть держателя — TTL. Не взял → InProgress (следующий тик). Без keepalive: секция короткая (единицы секунд ≪ TTL). |
| `/kafkaworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/kafkaworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** ([kafkaworker/07-api.md](../kafkaworker/07-api.md) §4): `{"url":"http://<host>:<port>","instance":"<id>","since_unix":…}` — ставит сам инстанс; ключ жив = инстанс жив и URL валиден. Читает панель; префикс `/kafka/` и этот координационный слой пишет только воркер (мутации панели — через его API) |
| `/kafkaworker/rotations/<C>` | обычный | заявка ротации app-пароля `{"requested_unix","requested_by"}` (панель, клэйм-txn; формат и протокол — pg 02 §9.8; del воркером по завершении, панелью — отмена или возрастным таймаутом не-начатой — [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4) |
| `/kafkaworker/admin_rotations/<C>` | обычный | заявка ротации admin-пароля `{"requested_unix","requested_by"}` (панель, клэйм-txn `version==0` — протокол ротаций; исполнение — фазы A/B/C с окном двух кредов `user_admin`/`user_admin2`, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4; del воркером по завершении, панелью — отмена или возрастным таймаутом не-начатой) |
| `/kafkaworker/ca_rotations/<C>` | обычный | заявка ротации per-cluster CA/сертов `{"requested_unix","requested_by"}` (формат/протокол — 02 §9.8 один в один; исполнение — [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4; del воркером в коммите фазы C или возрастным таймаутом не-начатой) |
| `/kafkaworker/rebalances/<C>` | обычный | заявка ребалансировки партиций `{"requested_unix","requested_by"}` (панель, клэйм-txn — протокол ротаций; del воркером по завершении, панелью — отмена или возрастным таймаутом не-начатой — [kafkaworker/06-rebalancing.md](../kafkaworker/06-rebalancing.md)) |
| `/kafkaworker/reassignments/<C>` | обычный | прогресс текущего reassignment — пишет только воркер: `{"mode":"drain"\|"balance","drain_broker"?,"partitions_total","partitions_remaining","submitted_unix","updated_unix","instance","last_error"?}`; ключ живёт только во время операции (put при старте, del по завершении — пусто = операции нет) |
| `/kafkaworker/regens/<C>` | обычный | прогресс rolling-регенерации брокеров ([kafkaworker/03-broker-scaling.md](../kafkaworker/03-broker-scaling.md) §3) — пишет только воркер: `{"brokers_total","brokers_remaining","current_broker"?,"updated_unix","instance","last_error"?}`; ключ живёт только во время операции (put при старте первого пересоздания, del по сходимости всех лимитов — пусто = операции нет) |
| `/kafkaworker/ticket_outcomes/<C>` | обычный | последний исход заявки — пишет только воркер: `{"kind":"password-app"\|"password-admin"\|"ca"\|"rebalance","outcome":"expired"\|"done","reason"?,"requested_unix","requested_by","finished_unix"}`; перезаписывается новым исходом, чистится демонтажом ([kafkaworker/processes/b-deprovisioning.md](../kafkaworker/processes/b-deprovisioning.md)); панель читает (алерт `kafka-ticket-expired`, 03 §7.4) |

## 2. Писатель и читатели

- **Пишет** весь префикс — только KafkaWorker; исключение вне префикса —
  серверный серт API `/workers/api_tls/kafkaworker` (пишет панель,
  [kafkaworker/07-api.md](../kafkaworker/07-api.md) §3).
- **Панель читает** только `rotations/`, `admin_rotations/`,
  `ca_rotations/`, `rebalances/`, `reassignments/`, `regens/`,
  `ticket_outcomes/` (очередь ротаций app/admin/CA и ребалансировок +
  прогресс reassignment/регенерации + исходы заявок в UI); остальные ключи
  не читает и не пишет.
