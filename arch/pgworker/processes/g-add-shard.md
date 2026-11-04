# G. AddShardProcess (A0–A6)

Уровень 4. Подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере по
декларации панели. Функция — [03-shard-scaling.md](../03-shard-scaling.md)
§2; модель — [01-placement.md](../01-placement.md); секреты —
[04-security.md](../04-security.md) §1; перечисления — [enums.md](enums.md).

Декларация — [adminpanel/02-etcd-contract.md](../../adminpanel/02-etcd-contract.md)
§9.5: replicas + nodes/NOT_INITIALIZED + request_*. Машина состояний одного
тика, идемпотентна (механика ProvisioningProcess в scoped-to-shard виде:
EnsureNode, WaitPatroni, portalloc-merge, DatabaseProvisioner).

```
A0 claim + journal(op=provision)
A1 guard:
   - кластер Active;
   - полное объявление (replicas>0, nodes.Count==replicas, ноды
     NOT_INITIALIZED/PROVISIONING — иначе phase=waiting-keys);
   - dsn нет (есть → Done);
   - scope /service/<C>-<X>/initialize отсутствует — либо есть, но лидер
     совпадает с именем нод НАШЕГО шарда (наш же поднимающийся Patroni после
     A3 — идемпотентность повторных тиков); коллизия имён — initialize с
     чужим лидером (перманентная ошибка);
   - имя шарда ^[a-z][a-z0-9_]{0,30}$;
   - перечитывание config (R6) — NOT_INITIALIZED/TO_REMOVE → phase=aborted;
   - strict-гвард: strict-кластер (config.synchronous_mode_strict = true,
     отсутствие поля = true) отклоняет add-shard с replicas < 2 (400
     панели) — без sync-standby новый шард блокировал бы запись;
   - пересоздание шарда поверх непустого бэкап-контура: ключ
     /pgworker/backups/<C>/<X>/wal-life существует (счётчик жизни цепочки
     пережил remove-shard — тот ключи бэкапов не удаляет) → инкремент
     счётчика (причина shard-rebuilt, ДО старта нод — приёмники включатся
     уже со свежим номером; идемпотентность и причины —
     [../../backups/05-etcd-contract.md](../../backups/05-etcd-contract.md) §1);
     отсутствует — первое создание шарда, жизнь 0, инкремента нет
A2 ensure app-секрета кластера (образец P1.5: у живого кластера ключи уже
   есть — читаем; отсутствуют, кластер создан до app-секрета, — генерируем и
   кладём txn put-if-absent; роль app выравнивается ALTER ROLE … PASSWORD)
A3 тюнинг per-shard: PgtuneInputsFactory.Create от уже прочитанной заявки
   request_* шарда — ОДИН результат на все ноды прохода; doorman-бюджет P15
   — от рассчитанного max_connections. Для каждой ноды — volume +
   контейнер/сервис (EnsureNode: план портов, сеть, REST-TLS-материал),
   state=PROVISIONING; существующий контейнер (re-run) — сверка имени и
   портов, расхождение — пересоздать
A4 ждать: /service/<C>-<X>/initialize есть + leader есть + у каждой ноды
   Patroni REST отвечает И идентифицирует ноду (GET /patroni: scope=<C>-<X>,
   name=<n>); чужой ответ по коллизионному порту — НЕ успех; бюджет Patroni
   (транзиент-толерантно); state=RUNNING
A5 SQL на мастере шарда: БД <dbname> (если нет), роли app/bucket_admin/
   bucket_mover + GRANT'ы (P2.3) — ТОЛЬКО они; СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ
   (шард пустой, routing не указывает); Routing/status не пишутся ВООБЩЕ.
   Свежий re-read app-кредов (пока нода поднималась, ротация могла сменить
   app_password) + ensure app_params нод шарда (как P2.5') + put
   shards/<X>/dsn (multi-host, порты из portalloc, per-cluster
   bucket_admin user+password)
A6 journal phase=done
```

Финальное состояние: у шарда есть `dsn`, все ноды `RUNNING`, `nodes`-ключи
= декларации, routing/status/schema-мир кластера не изменён НИКАК.

Отказ на шаге — journal `last_error` + фаза; transient — ретрай следующим
тиком с фазы; перманентные отказы (коллизия scope, невалидное имя) — журнал
с подсказкой. Гонка «панель пишет TO_REMOVE/add-shard отменён посреди
процесса»: перечитывание config перед фазами — смена state безопасно
прекращает процесс (контейнеры подчистит deprovisioning/remove-shard).
