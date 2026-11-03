# J. NodeRegenerator (rolling-перегенерация брокеров)

Уровень 4. Приведение лимитов контейнеров к декларации `resources`.
Функция — [03-broker-scaling.md](../03-broker-scaling.md) §3; модель —
[01-placement.md](../01-placement.md); контракт ключей —
[kafka-clusters/04-coordination.md](../../kafka-clusters/04-coordination.md).

Автоконверге лимитов контейнера к декларации `brokers/<b>/resources`
(изменение — мутация №15 панели через API,
[adminpanel/02-etcd-contract.md](../../adminpanel/02-etcd-contract.md) §10.2).
Docker не меняет лимиты живого контейнера — сходимость только пересозданием;
триггер — **только расхождение cpu/mem** (сверка inspect контейнера:
NanoCpus/Memory vs декларация; `resources=null` или `disk` — не сверяются:
disk — инфо, квоты томов — roadmap). Env пересобирается из текущей
декларации тем же пересозданием (NodeEnvBuilder — детерминизм R3): новые
server-props применяются попутно, но **env-дрейф триггером НЕ является** —
иначе конфиг-мутации, уже применённые converger'ом
[E](e-config-converger.md) без рестартов, требовали бы лишних рестартов.

- **Один брокер за тик**: снять контейнер (том сохраняется всегда) →
  пересоздать (EnsureNode с лимитами из декларации) → `state=PROVISIONING`;
  в `RUNNING` доводит AddBrokerProcess [F](f-add-broker.md) следующими тиками
  по факту DescribeCluster; следующий брокер регенерируется только после
  `RUNNING` предыдущего. Прецедент темпа — надзор
  [C](c-node-supervisor.md) («одно пересоздание по молчанию за тик»).
- **Прогресс-ключ** `/kafkaworker/regens/<C>`
  ([kafka-clusters/04-coordination.md](../../kafka-clusters/04-coordination.md)): put при старте первого пересоздания
  (`brokers_total`/`brokers_remaining`/`current_broker`), обновление на каждом
  шаге, del по сходимости (все стабильные брокеры `RUNNING` и лимиты ==
  декларации). Ключ жив = операция идёт (панель показывает в деталях
  кластера).
- **Guard'ы**: только `RUNNING`-брокеры (TO_REMOVE/REMOVING/PROVISIONING/
  NOT_INITIALIZED/UNREACHABLE — чужие процессы, их владельцы доведут); живая
  заявка ротации (`rotations/<C>` или journal-фаза ротации) или живой
  reassignment (`reassignments/<C>`) — передержка (journal waiting, без
  действий: пересоздания не смешиваются с чужими rolling/переездами).
  Inspect недоступен (docker-хост молчит) — ошибка тика, никаких пересозданий
  вслепую (порт слепоты надзора [C](c-node-supervisor.md)).
- **Идемпотентность/takeover**: состояние = факт docker + декларация +
  прогресс-ключ (перестраиваем от факта; потеря ключа — следующий тик
  пересчитает расхождения). PUT ресурсов посреди регенерации безопасен: если
  новый контейнер собрался по старой декларации — следующий тик увидит
  расхождение снова и перегенерирует (сходимость к последней декларации).
- Регенерация — без снапшотов P12 (etcd-декларацию воркер не меняет; как
  add/remove брокеров, [08-operability.md](../08-operability.md) §4).
