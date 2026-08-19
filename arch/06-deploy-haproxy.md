# 06. Деплой HAProxy (точка входа)

Приложения подключаются **к HAProxy**, а не к PostgreSQL-нодам напрямую. HAProxy по
health-check Patroni сам определяет, кто лидер, и направляет трафик правильно.

> Конфиги: `configs/haproxy/docker-compose.yml` + `configs/haproxy/haproxy.cfg`.

---

## 1. Идея маршрутизации

| Приложение пишет в | Порт HAProxy | Куда идёт трафик | Как HAProxy выбирает |
|---|---|---|---|
| `pg-lb:5432` | 5432 | только на текущего **лидера** | опрашивает `http://<pg>:8008/primary` — 200 только у лидера |
| `pg-lb:5433` | 5433 | на лидера **или** реплики (read) | опрашивает `http://<pg>:8008/read-only` — 200 у всех живых |
| `pg-lb:7000` | 7000 | веб-морда статистики (админам) | — |

Т.о. **write-соединения** (INSERT/UPDATE/DDL) → порт **5432**,
**read-only соединения** (SELECT, аналитика) → порт **5433** для масштабирования чтения.

---

## 2. haproxy.cfg (ключевое)

Полная версия в `configs/haproxy/haproxy.cfg`. Ключевые блоки:

### Frontend для write (→ master)
```haproxy
frontend ft_pg_write
    bind *:5432
    mode tcp
    default_backend bk_pg_master
```

### Backend: выбираем мастера по health-check
```haproxy
backend bk_pg_master
    mode tcp
    balance roundrobin
    option httpchk
    http-check expect status 200                      # мастер = 200 на /primary
    default-server inter 3s fall 3 rise 2
    server pg1 pg1:8008 check port 8008
    server pg2 pg2:8008 check port 8008
    server pg3 pg3:8008 check port 8008
```

> ⚠️ **Важно**: health-check HAProxy делает по протоколу **HTTP**, но сам трафик
> PostgreSQL — это **TCP**. HAProxy умеет так: `mode tcp` + `option httpchk` = «в TCP-режиме
> периодически делать HTTP-запрос для проверки живости, по результатам добавлять/убирать
> сервер в пул». Чтобы запрос шёл именно на `/primary`, добавляем в `haproxy.cfg`:

```haproxy
# Глобально задаём, по какому пути проверять
# (через опцию на server-уровне http-check проще всего задать единый путь)
# Здесь указываем проверку именно /primary:
backend bk_pg_master
    ...
    option httpchk GET /primary
    http-check expect status 200
```

> Patroni возвращает:
> - на `/primary` **200** только если нода — лидер,
> - иначе **503**.
> Значит в `bk_pg_master` активным будет ровно один сервер — текущий лидер.

### Frontend/backend для read (→ master + replicas)
```haproxy
frontend ft_pg_read
    bind *:5433
    mode tcp
    default_backend bk_pg_read

backend bk_pg_read
    mode tcp
    balance leastconn
    option httpchk GET /read-only
    http-check expect status 200
    default-server inter 3s fall 3 rise 2
    server pg1 pg1:8008 check port 8008
    server pg2 pg2:8008 check port 8008
    server pg3 pg3:8008 check port 8008
```

> `/read-only` возвращает 200 и у лидера, и у реплик → запросы балансируются по всем живым.
> Если хочешь **только реплики** для read — поменяй путь на `/replica`.

> Про `init-addr libc,none` в `default-server`: позволяет HAProxy стартовать, даже если
> какой-то узел ещё не зарезолвился (DNS/`/etc/hosts`). Адрес подхватится позже. Без этой
> опции HAProxy упадёт при старте, если хоть одно имя не резолвится в момент запуска.
> Валидируется через `haproxy -c -f haproxy.cfg` (выведет `Configuration file is valid`).

### Stats UI
```haproxy
listen stats
    bind *:7000
    mode http
    stats enable
    stats uri /
    stats admin if TRUE
```
Открой в браузере `http://pg-lb:7000` — увидишь, какой `server` в каком backend-е UP.

---

## 3. docker-compose.yml (HAProxy)

```yaml
# /opt/haproxy/docker-compose.yml
services:
  haproxy:
    image: haproxy:2.8-alpine
    container_name: haproxy
    restart: unless-stopped
    network_mode: host              # чтобы pg1/pg2/pg3 резолвились через /etc/hosts хоста
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    # порты 5432, 5433, 7000 открываются через host-сеть
```

> Если HAProxy стоит **не** на той же ноде, что и БД — убедись, что из контейнера резолвятся
> имена `pg1`/`pg2`/`pg3` (через `/etc/hosts` хоста, который виден в `network_mode: host`).
> Иначе — замени имена на IP прямо в `haproxy.cfg`.

---

## 4. Запуск

```bash
sudo mkdir -p /opt/haproxy && cd /opt/haproxy
# скопировать configs/haproxy/{docker-compose.yml,haproxy.cfg}
docker compose up -d
docker compose logs -f haproxy | head -40
```

### Проверка
```bash
# веб-морда
curl -s http://pg-lb:7000/ | head

# порт 5432 должен пускать на мастера (psql):
PGPASSWORD='<PG_SU_PASSWORD>' psql -h pg-lb -p 5432 -U postgres -c "SELECT pg_is_in_recovery();"
# →  f  (false) → мы на primary

# порт 5433 — на read-only:
PGPASSWORD='<PG_SU_PASSWORD>' psql -h pg-lb -p 5433 -U postgres -c "SELECT pg_is_in_recovery();"
# →  t  (true) → мы на реплике (чаще всего); иногда может попасться мастер
```

### Поведение при failover — тест
```bash
# 1) Запиши данные через 5432:
PGPASSWORD='...' psql -h pg-lb -p 5432 -U postgres -c "CREATE TABLE h(x int); INSERT INTO h VALUES (1);"

# 2) Урони лидера (например docker compose down на pg1):
ssh pg1 'cd /opt/postgres && docker compose down'

# 3) Через 30–60с HAProxy должен автоматически переключить 5432 на нового лидера.
#    Проверь:
./scripts/find-leader.sh        # кто стал лидером (ожидаем pg2 или pg3)
PGPASSWORD='...' psql -h pg-lb -p 5432 -U postgres -c "SELECT * FROM h;"
# → x=1, без даунтайма для клиента (новое соединение пойдёт на нового лидера)

# 4) Верни pg1 — он станет репликой и догонит:
ssh pg1 'cd /opt/postgres && docker compose up -d'
```

---

## 5. HA для самой точки входа (продвинутый вариант)

Один HAProxy — это **SPOF**. Если упадёт нода pg-lb — приложения потеряют вход даже при
живом кластере. Решения, от простого к сложному:

1. **DNS round-robin**: 3 A-записи `db.example.com` → на 3 HAProxy (по одному на каждой
   БД-ноде). Клиент перебирает. Дёшево, но переключение зависит от TTL DNS.
2. **VIP + keepalived**: пара HAProxy + keepalived, между ними плавающий VIP. Классика.
3. **Cloud LB**: AWS NLB / GCP TCP LB / Azure LB перед HAProxy-инстансами.
4. **K8s LoadBalancer Service**: если кластер в K8s, metallb/Cloud LB.

В рамках этого гайда достаточно варианта 1 или 2. Шаблон keepalived — в
`08-operations.md` → «HAProxy HA».

---

## 6. Чек-лист

```text
[ ] /opt/haproxy/{docker-compose.yml, haproxy.cfg}
[ ] haproxy.cfg: 3 backend-сервера (pg1/pg2/pg3) с check port 8008
[ ] frontend 5432 → bk_pg_master (httpchk GET /primary)
[ ] frontend 5433 → bk_pg_read   (httpchk GET /read-only)
[ ] stats на 7000
[ ] docker compose up -d
[ ] psql -p 5432 → SELECT pg_is_in_recovery() = f
[ ] psql -p 5433 → SELECT pg_is_in_recovery() = t (обычно)
[ ] тестовый failover: запись переживает остановку лидера
```

Готово → [07-identify-master.md](07-identify-master.md): **как узнать, кто мастер** (главное по ТЗ).
