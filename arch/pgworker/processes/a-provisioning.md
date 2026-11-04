# A. ProvisioningProcess (P0–P5)

Уровень 4. Создание кластера от `NOT_INITIALIZED` до рабочего состояния.
Функция — [02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §2; модель
размещения — [01-placement.md](../01-placement.md); секреты —
[04-security.md](../04-security.md) §1; перечисления —
[enums.md](enums.md).

Все шаги идемпотентны — перепроверяют факт (guard'ы те же, что в скриптовом
пути — P23, [11-bucket-sharding.md](../../11-bucket-sharding.md) §4.5).
Guard входа: полный набор ключей панели (config, shards/*/replicas,
nodes/*, routing всех N) — иначе journal `phase=waiting-keys`, ожидание
доустойчивости ключей (полуфабрикат не provisioning'уем).

```
P0 claim + journal(/pgworker/work/<C>, op=provision)
P1 план: placement ([01-placement.md](../01-placement.md) §5) для всех
   шард/нод; порт-аллокация; journal phase=planned.
   Сначала — усыновление факта: P1 КАЖДЫЙ тик provision инспектирует живые
   контейнеры кластера (InspectNodesAsync, механика AD1 процесса J) —
   расхождение нельзя узнать без инспекции, а полный portalloc может быть
   расходящимся (потерян и выделен заново): живой канонический контейнер
   pgw-<C>-<X>-<n> (pg>0, patroni>0) = положительное свидетельство — его
   фактические public-порты становятся каноном записи "<X>/<n>" (факт над
   записью: контейнер уже жив, portalloc — лишь след плана; совпадение
   записи с фактом — не пишем; перезапись только записей без object;
   неканонический/неоднозначный контейнер — пропуск с journal-заметкой).
   Ноды без находок — обычная аллокация [01-placement.md](../01-placement.md) §5. Запись merge: ничего не
   изменилось — не пишем вовсе (идемпотентность); ключ существовал — put
   (read-modify-write под клэймом), не существовал — txn version==0.
   Перепланирование занятых (Д1, per-node): подтверждение записи — факт
   контейнера САМОЙ ноды (все порты записи публикует её контейнер — пулер
   обязателен, нулевых портов у канонической ноды нет); занятость = ВСЕ
   docker-публикации (чужие и контейнеры СВОЕГО кластера: дубликат порта
   внутри кластера — такой же конфликт) ∪ portalloc-записи соседей.
   Закрепление, не подтверждённое фактом своего контейнера и занятое,
   снимается — нода аллокируется заново на свободные порты, portalloc
   перезаписывается, EnsureNode (P2.1) создаёт контейнер в ТОМ ЖЕ тике;
   для Allocate-переиспользования факты подтверждённых записей вычитаются
   из занятости (ConfirmedFact — иначе allocator не переиспользовал бы
   валидные записи и EnsureNode пересоздавал бы живые контейнеры);
   наследие-коллизии portalloc двух кластеров самолечатся, вечный цикл
   «закреплено и переиспользуется → create fail» невозможен
P1.5 ensure app-секрета: прочитать /clusters/<C>/{app_user,app_password};
   отсутствующие — сгенерировать (32 симв [A-Za-z0-9]) и положить ОДНОЙ txn
   (compare NotExists на отсутствующие + put); txn проигран (гонка/re-run) —
   re-read и использовать существующие; роль app на каждом шарде создаётся
   с этим паролем и выравнивается ALTER ROLE (идемпотентно)
P2 на каждый шард X:
 P2.0 вычислить тюнинг шарда: PgtuneInputsFactory.Create(resources) от уже
   прочитанных заявок request_* (Pgtune.Calculate по нормативу
   docs/pgtune-calculation-spec.md) — ОДИН результат на все ноды прохода;
   сбой расчёта — фейл фазы тика (транзиент-ретрай)
 P2.1 для каждой ноды n: создать volume + контейнер/сервис с конфигом
   (поллинг-инвариант [08-operability.md](../08-operability.md) §3:
   create/start контейнера — долгая операция медленного daemon;
   выполняется идемпотентными поллинг-итерациями с таймаутом короче окна
   проверки watchdog, каждая итерация — отметка ILoopProgress + лог
   elapsed; одиночный блокирующий вызов без поллинга оставляет слепую
   зону watchdog)
   (Spilo env: SCOPE=<C>-<X>, ETCD3_HOSTS=host:port (etcd v3),
   канонические тайминги ttl=20/loop_wait=1/retry_timeout=3 в
   bootstrap.dcs ([01-placement.md](../01-placement.md) §2: полы Patroni
   4.x; гарантируются конвергенцией); SpiloEnvBuilder подставляет
   synchronous_mode_strict из config кластера (per-cluster опция) в
   bootstrap.dcs рядом с synchronous_mode, параметры PG — merge(PGTune ∪
   канон): PGTune-вывод P2.0 в порядке §5.2, канон P3/P4 поверх
   (wal_level=logical + sync_replication_slots + max_slot_wal_keep_size,
   walsenders/slots=10); doorman-бюджет P15 от рассчитанного
   max_connections — max(10, max_connections − 5); callback on_role_change
   → lease-скрипт мастер-ключа; doorman: пул <dbname>, TLS require),
   env-секреты ([04-security.md](../04-security.md) §2);
   дополнительно REST-TLS-материал: SSL_RESTAPI_* PEM + секция restapi
   SPILO_CONFIGURATION с connect_address/authentication из per-cluster
   rest_password;
   nodes/<n>/state=PROVISIONING; при существовании (re-run) — сверить
   имя И порты: фактические public-биндинги контейнера (inspect) обязаны
   совпадать с планом (5432→pg, 8008→patroni, 6432→doorman); расхождение →
   пересоздать контейнер (stop+rm, volume сохраняется — фаза PROVISIONING,
   данных нет; идемпотентность по имени одному оставляла контейнер навсегда
   на чужих портах: WaitPatroni бил в мёртвый порт); совпадение — пропустить
 P2.2 ждать: /service/<C>-<X>/initialize есть + leader есть + у каждой ноды
   Patroni REST отвечает И ИДЕНТИФИЦИРУЯ ноду (GET /patroni несёт
   scope=<C>-<X> и name=<n>; чужой ответ по коллизионному порту — НЕ успех:
   фальш-RUNNING/фальш-dsn на чужие данные исключены; бюджет 10 мин,
   транзиент-толерантно); nodes/<n>/state=RUNNING.
   При исчерпании бюджета (лидера нет) — проверка данных scope (Д3):
   проба данных GET /api/node/pgdata приёмнику каждой ноды (канал
   [25-component-communications.md](../../25-component-communications.md)
   №22; приёмник проверяет PG_VERSION файловой системой своего контейнера)
   → Present/Absent/Unknown (транспортный отказ); ВСЕ Absent (доказанная
   утрата) → чистка HA-scope (точечные initialize/leader/sync/failover +
   префиксы optime//members/; failover обязателен — остаточный маркер с
   scheduled_at в прошлом гонит Patroni новой жизни в accelerated failover
   на неинициализированного кандидата; request_* — декларации панели — НЕ
   трогаем) с journal phase=reset-scope → Patroni бутстрапится заново,
   бюджет ожидания начинается снова; хоть одна Present → НЕ лечить (чистка
   уничтожила бы данные): journal-фейл «данные есть, лидера нет — разбор
   оператора» (панель: provision-stuck/shard-no-leader); Unknown (транспорт
   docker) → ждать, не лечить
 P2.3 на мастере шарда (адрес из master-ключа/Patroni): создать БД <dbname>
   (если нет), роли app/bucket_admin/bucket_mover + GRANT'ы (канон —
   [11-bucket-sharding.md](../../11-bucket-sharding.md)), если нет
 P2.4 создать схемы bucket_<i> по routing (только шардовы бакеты;
   CREATE SCHEMA IF NOT EXISTS, GRANT USAGE) — идемпотентно
 P2.5 записать shards/X/dsn (multi-host, с per-cluster bucket_admin user+password)
P2.5' ensure app_params КАЖДОЙ ноды шарда: put-if-absent (txn NotExists)
   nodes/<n>/app_params = PgWorker:AppParams:Default ("sslmode=require");
   существующий ключ не трогаем (ручные правки оператора живы)
P3 снять ВСЕ status/bucket_<i> (txn-пакетами ≤128 ops) — бакеты ACTIVE
P4 config: txn (compare mod_revision) → put канонического JSON без state
P5 снапшот P12; journal phase=done; кластер переходит в обычный надзор
```

Отказ на шаге: journal `last_error` + фаза; ретрай следующим тиком с
**бэкоффом**: подряд идущие фейлы provision наращивают `fail_count` в
`/pgworker/work/<C>`, задержка ретрая —
`min(ProvisionRetryBaseSec·2^(n−1), ProvisionRetryMaxSec)` (5 с → 60 с);
до `retry_not_before_unix` тик процесса — skip без записи (самолечение не
блокируется: серия сбрасывается успехом (`Done`); фазы прогресса серию
переносят — [../00-etcd-contract.md](../00-etcd-contract.md) §4). Исчерпание
бюджета Patroni (P2.2) сбрасывает трекер бюджета: следующая попытка
получает новый бюджет (иначе каждый тик после первого таймаута фейлил
мгновенно — 234 одинаковых отказа за 10 минут на живом стенде). Гонка
«панель пишет TO_REMOVE посреди provisioning»: перед фазой процесс
перечитывает config — смена state безопасно прекращает provisioning
(контейнеры подчистит deprovisioning).

Состояния нод и кластера — [enums.md](enums.md). Точка P2.2 закрывается
только зрячей пробой: чужой ответ Patroni по коллизионному порту — не успех.
