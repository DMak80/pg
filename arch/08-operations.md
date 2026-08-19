# 08. Операционка: switchover, бэкапы, добавление ноды

Повседневные операции. Все `patronictl` вызовы идут через `scripts/patronictl.sh`
(обёртка над `docker exec`).

---

## 1. Плановая смена лидера — switchover

**Switchover** = плановая смена мастера (например, для обслуживания pg1). Без даунтайма для
приложений: Patroni штатно передаёт роль, HAProxy автоматически перенаправит трафик.

```bash
# сделать pg2 новым лидером (pg1 станет репликой)
./scripts/patronictl.sh switchover --master pg1 --candidate pg2

# или интерактивно (спросит кандидата):
./scripts/patronictl.sh switchover
```

Что происходит под капотом:
1. Patroni проверяет, что кандидат-реплика догнала лидера (`lag=0`).
2. Лидер закрывает текущие сессии на запись, делает последнюю контрольную точку.
3. Кандидат `pg_ctl promote` → становится primary.
4. Бывший лидер подключается к новому как streaming-replica.
5. HAProxy на следующем health-check видит 200 на `/primary` уже у pg2 → пишет идут туда.

> Проверка: `./scripts/find-leader.sh` → должен показать нового лидера.

---

## 2. Неплановая потеря лидера — failover (автоматический)

В нормальном режиме **ничего делать не надо** — Patroni сам:
1. обнаруживает, что старый лидер не продлевает lease,
2. выбирает самую свежую реплику,
3. повышает её до primary,
4. HAProxy перенаправляет трафик.

Когда干预ать приходится:
- failover не происходит слишком долго (>2 минут) → смотри `09-troubleshooting.md`;
- нужно принудительно повысить конкретную реплику:
  ```bash
  ./scripts/patronictl.sh failover --candidate pg3
  ```

---

## 3. Вернуть упавшую ноду в кластер

Если нода (например pg1) была лидером, упала, кластер выбрал pg2. Когда pg1 вернётся —
Patroni автоматически:
1. обнаружит, что pg1 «отстал» (старый timeline),
2. сделает `pg_rewind` (если расхождение большое) или дотянется по WAL,
3. подключит её к новому лидеру как replica.

**Руками обычно трогать не нужно.** Если автоматика не справилась:
```bash
ssh pg1
cd /opt/postgres
docker compose down
# опционально: очистить PGDATA для чистого клонирования (ОПАСНО — удаляет данные этой ноды)
# rm -rf /data/pg/pgroot/pgdata/*
docker compose up -d
docker compose logs -f postgres     # ждём "started as replica"
```

> ⚠️ `rm -rf` содержимого PGDATA — только если уверен, что **эта** нода не нужна как
> источник. В кластере из 3 такие операции безопасны (данные есть на двух других).

### Альтернатива:一键 rebuild через скрипт
Для частого/регулярного случая (особенно с пустым/новым диском) есть готовый скрипт со
всеми проверками — см. следующий раздел «Пересоздание ноды с пустого/повреждённого диска».

---

## 4.1 Пересоздание ноды с пустого/повреждённого диска ★

**Сценарий**: диск ноды пересоздали/заменили (PGDATA пустой) или данные повредились и нода
не поднимается как реплика. Нужно пересоздать её «с нуля» — клонировать с текущего лидера.

### Главное: это умеет сам Patroni — ничего «включать» не надо
Patroni выбирает способ старта по состоянию PGDATA:

| Состояние PGDATA при старте | Действие Patroni |
|---|---|
| Пусто / не существует | **`pg_basebackup` с лидера** → нода становится replica. ← наш случай |
| Есть данные, небольшое расхождение | дотягивается по WAL |
| Есть данные, большое расхождение | `pg_rewind` (включён `use_pg_rewind: true`) → пересинхронизация |
| Данные повреждены/несогласованны | может застрять → тогда «принудительный» rebuild |

Ключ в DCS `/service/<scope>/initialize` **уже существует** (кластер инициализирован),
поэтому пустая нода **не** пытается сделать `initdb` и стать мастером — она клонируется
с лидера. Это встроено в Patroni.

### Ручной путь (для понимания)
```bash
# на целевой ноде (НЕ лидере!)
cd /opt/postgres
docker compose down
rm -rf /data/pg/* /data/pg/.[!.]*    # очистить PGDATA (точнее — PGROOT Spilo)
docker compose up -d                  # Patroni сделает basebackup и догонит
docker compose logs -f postgres       # ждём "streaming"/"replica"
```

### Безопасный путь: `scripts/rebuild-node.sh`
Делает то же, но с защитой:
- ✗ откажется, если целевая нода — текущий лидер (нельзя сносить мастера);
- ✗ откажется, если в кластере осталось < 2 живых нод (потеряем избыточность);
- ✗ откажется, если путь `PG_DATA_DIR` не похож на postgres-каталог;
- ✗ спросит подтверждение `YES` (или `--yes`).

```bash
# пересоздать pg3 (по ssh на pg3; контейнер postgres живёт там)
./scripts/rebuild-node.sh pg3

# если выполняешь локально на самой ноде (без ssh):
./scripts/rebuild-node.sh pg3 --local

# без подтверждения (для автоматизации/учений):
./scripts/rebuild-node.sh pg3 --yes
```

Пример вывода:
```
✓ Текущий лидер: pg1 (целевая 'pg3' — НЕ лидер).
✓ Живых нод: 3 (избыточность сохранится после пересоздания).

ВНИМАНИЕ: будут УДАЛЕНЫ все данные PostgreSQL на 'pg3' (/data/pg)
          и нода пересоздана с нуля (клонирование с лидера 'pg1').
Продолжить? [введите YES]: YES

>>> Останавливаю контейнер postgres на 'pg3' ...
>>> Очищаю PGDATA (/data/pg) ...
>>> Запускаю контейнер — Patroni сделает pg_basebackup с лидера ...
>>> Логи (Ctrl+C — выйти; ждём 'replica'/'streaming'):
...
```

### Что важно помнить
- **Перед rebuild** убедись, что лидер здоров и репликация с него работает
  (`pg_stat_replication` на лидере). Иначе нода будет клонироваться с «битого» мастера.
- При `synchronous_mode: true` удаление синхронной реплики может **временно заблокировать
  запись** на лидере (ему некому подтверждать коммиты). Если пересоздание затягивается —
  можно на время выключить: `./scripts/patronictl.sh edit-config` → `synchronous_mode: false`,
  rebuild, затем вернуть обратно.
- Время rebuild = размер данных / скорость сети с лидером. Для 100 ГБ по 1 Гбит ~ 15–20 мин.
  Для больших баз выгоднее восстановиться из `pgbackrest`/`wal-g` инкремента, а не basebackup
  (см. раздел 4 «Бэкапы»).

### Автоматизация «вернуть узел к жизни» (опционально, продвинутый уровень)
Если хочешь, чтобы **новый пустой хост** сам поднимал postgres-контейнер при загрузке
(например, восстановление после полной замены машины):

1. cloud-init / kickstart при создании машины: ставит docker, монтирует диск, кладёт
   `docker-compose.yml` + `pg.env` из конфиг-репозитория;
2. `systemd`-unit (или `docker compose up -d` из cloud-init) запускает контейнер;
3. Patroni видит пустой PGDATA → basebackup → replica.

Сам Patroni **по-прежнему не создаёт** машины/диски — это делает оркестратор инфра
(cloud ASG / K8s / terraform + user-data). Но **логика «пустой диск → догнать лидера»**
уже встроена в Patroni и **работает без единой дополнительной настройки**.

---

## 4. Бэкапы

База на отдельном диске на каждой ноде, но **репликация ≠ бэкап** (DROP TABLE реплицируется
тоже). Регулярные бэкапы обязательны.

### Вариант A — `pg_basebackup` с реплики (онлайн, простое)
```bash
docker exec -it postgres pg_basebackup \
  -h pg2 -U standby -D /tmp/backup -Ft -z -P
# создаёт tar-архив PGDATA. Положить на внешнее хранилище.
```

### Вариант B — `pgbackrest` или `wal-g` (продакшн-стандарт)
Spilo-образ идёт с **WAL-G/PGBACKREST** встроенными (смотри
[Spilo ENVIRONMENT](https://github.com/zalando/spilo/blob/master/ENVIRONMENT.rst), переменные
`WAL_*`, `PGBACKREST_*`). Конфигурируется под S3/GCS:
```bash
# примерный набор env для WAL-G → S3
USE_WALG_BACKUP=true
USE_WALG_RESTORE=true
WALG_S3_PREFIX=s3://my-bucket/pgcluster
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT=...
WALG_DISABLE_S3_SSL=false
```
Тогда:
```bash
docker exec -it postgres wal-g backup-push /home/postgres/pgroot/pgdata/pg16
docker exec -it postgres wal-g backup-list
```

### Вариант C — снапшот диска (быстрый full)
Если PGDATA на отдельном томе (как у нас) — снапшот этого тома (LVM/EC2 EBS snapshot/
cloud disk snapshot) = мгновенный full backup. Для согласованности ставь pg в
`pg_start_backup()` или используй `pgbackrest` для обеспечения консистентности.

> Минимально: **ежедневный basebackup + непрерывная архивация WAL**. Точка восстановления —
> любая в пределах retention (обычно 7–30 дней).

---

## 5. Добавление новой ноды (масштабирование 3 → 4)

> В рамках ТЗ кластер на 3 ноды, но процедура — для полноты.

1. Подготовь хост `pg4` по `03-prerequisites.md`.
2. Смонтируй `/data/pg`, `chown 999:999`.
3. Скопируй `/opt/postgres/{docker-compose.yml, pg.env}`, поставь `hostname: pg4`.
4. `docker compose up -d` — Patroni:
   - увидит существующий кластер в DCS,
   - сделает `pg_basebackup` с текущего лидера,
   - стартует как replica, начнёт стримить.
5. (опционально) Добавь pg4 в backend-списки HAProxy (`pg4:8008`), reload:
   ```bash
   docker exec -it haproxy kill -HUP 1
   ```
6. Проверь: `./scripts/patronictl.sh list` → pg4 как Replica.

> ⚠️ При добавлении узла в **etcd**-кластер (если хочешь etcd тоже из 3 → 4) — процедура
> другая (etcdctl member add). В базовой схеме etcd остаётся 3 узла.

---

## 6. Обновление PostgreSQL major-версии (pg16 → pg17)

Major upgrade без даунтайма = **через логическую репликацию** или **rolling с pg_upgrade**.
Это отдельная большая процедура — выходит за рамки этого гайда. Принцип:
1. Поднять **параллельный** кластер pg17.
2. Настроить logical replication pg16 → pg17.
3. Догнать, сделать switchover на уровне приложений.
4. Демонтировать pg16.

Minor-обновления (16.4 → 16.5): просто смена тега образа Spilo + rolling restart нод:
```bash
# pg1 → update image tag → docker compose up -d → ждём, пока станет replica после switchover
./scripts/patronictl.sh switchover --candidate pg2      # теперь pg2 лидер, pg1 можно обновлять
ssh pg1 'docker compose pull && docker compose up -d'
# повторить для pg2, pg3 по кругу
```

---

## 7. HAProxy HA (чтобы убрать SPOF точки входа)

Один HAProxy — точка отказа. Два + keepalived дают плавающий VIP:

```
              VIP 10.0.0.5 (плавающий)
                    │
        ┌───────────┴───────────┐
     haproxy-A (pg1)         haproxy-B (pg2)
     keepalived              keepalived
        └────────┬──────────────┘
                 ▼
   pg1/pg2/pg3 (PostgreSQL)
```

`keepalived` конфиг (на обеих HAProxy-нодах, разница в `priority`):
```
# /etc/keepalived/keepalived.conf (пример)
vrrp_script chk_haproxy { script "pidof haproxy"; interval 2 }
vrrp_instance VI_1 {
    state MASTER           # на втором — BACKUP
    interface eth0
    virtual_router_id 51
    priority 100           # на втором — 90
    advert_int 1
    authentication { auth_type PASS auth_pass <secret> }
    virtual_ipaddress { 10.0.0.5/24 dev eth0 }
    track_script { chk_haproxy }
}
```
Приложения подключаются к `10.0.0.5:5432`. При отказе pg1 — VIP перетекает на pg2.

> В облаке проще: AWS NLB / GCP TCP LB / Azure LB перед 2–3 HAProxy-инстансами.

---

## 8. Полезные patronictl команды (шпаргалка)

```bash
./scripts/patronictl.sh list                      # табличка состояния
./scripts/patronictl.sh list --format json        # для скриптов
./scripts/patronictl.sh topology                  # дерево master→replicas
./scripts/patronictl.sh show-config               # текущий DCS-конфиг
./scripts/patronictl.sh edit-config               # изменить параметры (hot)
./scripts/patronictl.sh switchover                # плановая смена
./scripts/patronictl.sh failover                  # аварийная смена (если авт. не сработало)
./scripts/patronictl.sh reinit pg1                # пересоздать ноду с нуля (basebackup)
./scripts/patronictl.sh restart pg1               # рестарт PostgreSQL на ноде
./scripts/patronictl.sh pause                     # поставить авто-failover на паузу (обслуживание)
./scripts/patronictl.sh resume                    # снять паузу
./scripts/patronictl.sh dsync pg1 --force         # (danger) разовая sync, если что-то застряло
```

> `pause`/`resume` — **важно** при плановых работах на etcd или машине: patroni перестанет
> реагировать на отказ лидера (что во время окна обслуживания может быть нежелательно).

---

## 9. Мониторинг (минимум)

Что стоит наблюдать:
- `patroni_cluster_unlocked` → 0 (кворум есть, лидер выбран).
- `patroni_master` → 1 (лидер существует).
- `pg_replication_lag` (на лидере, `pg_stat_replication`).
- `etcd_server_has_leader` → 1.
- `haproxy_backend_up` для `bk_pg_master` → ровно один сервер UP.

Готовые экспортёры: `patroni` сам отдаёт метрики на `:8008/metrics` (Prometheus-формат),
`etcd` — на `:2379/metrics`, HAProxy — через `haproxy_exporter`.

---

## Дальше
→ [09-troubleshooting.md](09-troubleshooting.md): что делать, когда что-то сломалось.
