# Стенд практической проверки P1–P8 (PostgreSQL 18.4)

Docker-стенд для проверки решений реестра рисков [12-bucket-pitfalls.md](../12-bucket-pitfalls.md)
практически — имитацией ситуаций, а не чтением документации. Результаты внесены
в [11-bucket-sharding.md](../11-bucket-sharding.md) и [05-deploy-postgres.md](../05-deploy-postgres.md).

## Топология

```
«шард1» (источник)                          «шард2» (приёмник — полноценный кластер, P8)
  s1a  postgres:18 мастер                      s2a  postgres:18 мастер
  s1b  postgres:18 реплика (phys. слот)        s2b  postgres:18 реплика (phys. слот)
  hc1a/hc1b  сайдкары: эмуляция Patroni REST /primary   hc2a/hc2b — симметрично
                                               (python+pg8000, netns нод — HAProxy
                                                видит их как порты 8008 нод)
  hap1  HAProxy :5432 → текущий мастер         hap2 — симметрично
  (option httpchk GET /primary)

etcd (v3.5.21)  контрол-плейн: /buckets/routing/*, /buckets/status/*
opsbox          psql+pg_dump+etcdctl+jq — машина запуска скриптов (профиль ops:
                docker compose run --rm opsbox ..., хостовые бинарики не нужны)
```

Патрони/pg_doorman нет — их роль в проверках играет HAProxy с health-check
по `/primary` (сайдкары отвечают 200 только на мастере). Параметры: `wal_level=logical`
на всех нодах, `sync_replication_slots=on` + `hot_standby_feedback=on` на репликах,
`max_slot_wal_keep_size=32MB` (занижен для скорости), аутентификация trust
(только стенд). `synchronous_standby_names` выставляет `00-up.sh` через ALTER SYSTEM
(не флагами `-c` — флаг сильнее ALTER SYSTEM) — предусловие P8: у приёмника обязан
быть живой sync-standby для `remote_apply` подписок переездов.

## Карта «проверка → риски → результат»

| Скрипт | Риски | Что делает | Результат |
|---|---|---|---|
| `checks/00-up.sh` | — | поднимает стенд (оба шарда + etcd), патчит pg_hba, ждёт реплики, проверяет hap1/hap2 → мастерам, включает sync-standby | ✓ |
| `checks/10-p1-p5-freeze.sh` | P1, P5 | app-роль ≠ owner; REVOKE CREATE (P5) и deny для app; голый REVOKE при живом писателе; заморозка REVOKE + `LOCK TABLE ACCESS EXCLUSIVE` с lock_timeout; призрак → permission denied; GRANT-разморозка | ✓ |
| `checks/20-move-subscription.sh` | P2, P3 | pg_dump schema-only → s2; подписка через hap1 с `failover=true`; initial copy 2/2; стриминг; слот synced на s1b без ошибок синхронизации | ✓ |
| `checks/30-failover-p2-p3.sh` | P2, P3 | `docker stop s1a` + promote s1b; hap1 сам → s1b; подписка переподключилась (conninfo не менялся); счётчики равны; duplicate-key 0 | ✓ |
| `checks/40-cutover-p6-p1.sh` | P1, P6 | заморозка на s1b; провал наивной эвристики sequences (поиск через pg_depend, тихий duplicate key); sequence→sequence + инвариант; DROP SUBSCRIPTION; запись на s2 | ✓ |
| `checks/50-p4-wal-lost.sh` | P4 | «зависший подписчик» (slowconsumer.pl) + пачки несжимаемого WAL → `wal_status='lost'`; шард жив; уборка срезает слот | ✓ |
| `checks/60-p7-abort.sh` | P7 | фаза 1: зависший FROZEN — list, отказы-защиты, журнал в etcd до манипуляций (s2 недоступен → phase=blocked, БД не тронута), resume после возврата s2, откат в ACTIVE у s1; фаза 2: routing==target (flip прошёл, статус завис) — отказ без --force, с --force доведение: sequences довинчены (P6), приёмник владелец, старый шард вычищен | ✓ |
| `checks/65-move-e2e.sh` | P1–P8 | e2e настоящими скриптами из ops-бокса: create-bucket → move (заморозка P1, инвентарь P5, sequence→sequence P6, сверка строк P8, атомарный etcd-flip) → призрак → rollback через обратную подписку → повторный move → finalize; негатив: move в шард без sync-standby отказывается (P8-предусловие) | ✓ |
| `checks/70-p8-receiver-failover.sh` | P8 | RED: подписка с дефолтным synchronous_commit=off — failover приёмника молча пропускает срез W1 при «здоровом» стриме (лаг 0), лечение — abort (P7); GREEN: move-bucket.sh с `remote_apply` — W2 висит в SyncRep (не подтверждается при replay-паузе), после failover переслан и применён; mover пережил обрывы, copy рестартовал на новом мастере, cutover со сверкой строк и атомарным flip; finalize добил осиротевшие sync-слоты | ✓ |
| `checks/90-down.sh` | — | разбор стенда | — |

Полный прогон по порядку номеров; логи — в `logs/`.

## Главные находки (изменили доку 11/12/05)

1. **REVOKE — НЕ барьер** (P1). Голый `REVOKE ... ON ALL TABLES IN SCHEMA` берёт
   лишь AccessShareLock: при живой пишущей транзакции проходит за 0.05с, и поздний
   писатель коммитится уже после «заморозки». Барьер — `LOCK TABLE ... IN ACCESS
   EXCLUSIVE MODE` в той же транзакции (упирается в lock_timeout, terminate + ретрай).
2. **nextval даёт И USAGE, И UPDATE** (P1): `REVOKE UPDATE ON ALL SEQUENCES` не
   закрывает `nextval` — отнимать обе привилегии, выдавать app-роли тоже обе.
3. **Рецепт failover slots** (P3) — обязательны все три: подписка сразу с
   `failover=true` (потом — только вне транзакции: DISABLE → SET → ENABLE);
   физический слот у реплики (`primary_slot_name`, `pg_basebackup -C -S`);
   `dbname` в `primary_conninfo` реплики. Иначе `sync_replication_slots` молча
   не работает.
4. **Окно досинка слота после initial copy** (P3): в логах реплики возможны
   `could not synchronize slot` (скачок catalog_xmin); promote в этом окне теряет
   слот → fallback re-copy. Перед cutover проверять `synced=true` на репликах.
5. **Неактивный слот не инвалидируется** (P4): при плавной записи checkpoint'ы
   подтягивают restart_lsn — лимит ловит активного молчащего потребителя
   (walsender жив, подтверждений нет) и всплески генерации. Для стенда написан
   `slowconsumer.pl` — `pg_recvlogical` не годится (отвечает на keepalive).
6. **Провал наивной эвристики sequences** (P6): поиск sequence через
   принадлежность колонке (`pg_depend`, deptype 'a') не видит standalone-sequence →
   продемонстрирован тихий будущий duplicate key. Рабочая схема —
   sequence→sequence (`last_value`/`is_called` → `setval`) поимённо для всех
   sequence схемы + инвариант «следующее на приёмнике > последнего выданного
   на источнике».
7. **`boolean||text` даёт 'true'/'false', а не 't'/'f'** (P6/P7): конкатенация
   `last_value||' '||is_called` возвращает `50 true` — короткая форма `t` это
   только дисплей psql. Сравнение `[ "$ic" = "t" ]` в bash всегда ложно: в
   checks/40 латентно ослабляло P6-инвариант на 1 (пропускало next==issued),
   в sync-функции abort-move.sh дало `setval(49)` при выданных 50 → duplicate key.
   Лечение: issued/next считать на стороне SQL (`CASE WHEN is_called ...`),
   boolean в bash не парсить.
8. **Failover приёмника молча пропускает срез изменений** (P8): feedback
   (`confirmed_flush_lsn`) убегает вперёд физической репликации приёмника — после
   promote standby срез [позиция standby; confirmed_flush) НЕ пересылается, при
   этом стрим «здоров» (лаг 0, новые строки доезжают) — лаг-метрики слепы.
   Защита — `synchronous_commit=remote_apply` у подписок переездов: apply-воркер
   висит в SyncRep до replay на standby, feedback удерживается; W-срез после
   failover пересылается и применяется. Без живого sync-standby у приёмника
   защита тихо вырождается — preflight move отказывает заранее.
9. **Промоушен без второй реплики обязан снимать synchronous_standby_names**:
   при непустых sync-именах и неподключённой реплике коммиты ВИСЯТ в SyncRep
   бесконечно (ловушка синхронной репликации, актуальна с P8). Поэтому стенд
   ставит sync-имена ALTER SYSTEM'ом, а не флагами `-c` (флаг сильнее — не снять).

## Нюансы воспроизведения

- PGDATA в postgres:18 — `/var/lib/postgresql/18/docker`; после `pg_basebackup`
  нужен `chmod 700` (и `rm -rf` при повторе попытки).
- `psql -c` с несколькими командами сворачивается в одну транзакцию —
  `ALTER SUBSCRIPTION ... SET` и подобные давать отдельными вызовами.
- Несжимаемый WAL для P4: `LATERAL string_agg(md5(random()::text),'')` —
  `repeat(md5(x),N)` TOAST-сжимается и WAL почти не создаёт.
- VirtioFS на macOS: initdb.d-скрипты с шебангом не исполняются (noexec) —
  pg_hba патчит `00-up.sh` после старта.
- `docker stop`/`start` ноды меняет её netns — сайдкар (network_mode: service:...)
  не переподключается сам: после старта ноды рестартуй и его (`docker restart hc2b`).
- Рестарт контейнера меняет его IP в сети — HAProxy резолвит бэкенды только при
  своём старте и ловит Layer4-timeout по устаревшему адресу. Поэтому PG-нодам
  в compose заданы фиксированные IP (172.28.0.11/.12/.21/.22); после пересоздания
  ноды `docker compose up -d` адрес стабилен.

## Запуск / разбор

```bash
cd arch/stand
checks/00-up.sh && checks/10-p1-p5-freeze.sh && checks/20-move-subscription.sh \
  && checks/30-failover-p2-p3.sh && checks/40-cutover-p6-p1.sh && checks/50-p4-wal-lost.sh \
  && checks/60-p7-abort.sh && checks/65-move-e2e.sh && checks/70-p8-receiver-failover.sh
# разбор:
checks/90-down.sh
```

Порядок важен: 30-й делает failover шарда 1 (мастер — s1b, s1a не поднимается),
70-й ломает и восстанавливает шард 2 (s2a пересоздаётся репликой s2b, потом
наоборот). Все скрипты (`create/move/abort-move.sh`) запускаются из ops-бокса
по внутренней сети стенда (конфиг `buckets.stand.env`); руками:
`docker compose run --rm -T opsbox bash /arch/scripts/move-bucket.sh status bucket_45`.
На хосте нужен только docker (+ jq для ассертов проверок).
