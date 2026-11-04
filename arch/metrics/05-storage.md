# Телеметрия: хранение и scrape-стек

Уровень 3. Стек мониторинга dev-стенда, scrape-таргеты, дашборды, file_sd.
Родитель — [README.md](README.md); соседние документы —
[01-framework.md](01-framework.md), [02-dictionary.md](02-dictionary.md),
[03-exposure.md](03-exposure.md), [04-collectors.md](04-collectors.md),
[06-testing.md](06-testing.md), [07-risks.md](07-risks.md).

## 1. Сервисы (профиль `metrics` в `dev-stand/adminpanel/docker-compose.yml`)

| Сервис | Образ | Порт (хост) | Назначение |
|---|---|---|---|
| `prometheus` | `prom/prometheus` (пин версии) | 9090 | TSDB + rules; конфиг `metrics/prometheus/prometheus.yml` |
| `grafana` | `grafana/grafana` (пин версии) | 3000 | дашборды; provisioning `metrics/grafana/` (datasource + JSON-дашборды из репо) |
| `alertmanager` | `prom/alertmanager` (пин версии) | 9093 | доставка алертов; `metrics/alertmanager/alertmanager.yml` |

Профиль входит в полный подъём `dev-stand/adminpanel/checks/00-up.sh`
(«стенд = полная система»); quick-профиль стенда мониторинга не поднимает.
Порты 9090/3000/9093 публикуются на хост; коллизия с локальными сервисами —
через env-override compose (значения по умолчанию в `.env`-шаблоне).

## 2. Scrape-таргеты (prometheus.yml)

| Job | Таргеты | Что снимает |
|---|---|---|
| `pgworker` | сетевые `pgworker:8080`, `pgworker-2:8080` (file_sd `pgworker-targets.json`, пишет 00-up.sh; deploy-воркеры с aliases в стендовой сети контура — прод `pgw-metrics`, §4) | [02-dictionary.md](02-dictionary.md) §1–2 |
| `kafkaworker` | имя сети стенда `kafkaworker:8080` (хост-публикация 8082 — только для чеков, Prometheus её не использует; fallback не предусмотрен) | [02-dictionary.md](02-dictionary.md) §1–3 |
| `valkeyworker` | имя сети стенда `valkeyworker:8080` (профиль `valkey`; хост-публикации нет — чеки ходят изнутри контейнера) | [02-dictionary.md](02-dictionary.md) §1–2, §6 |
| `adminpanel` | имя сети стенда `adminpanel:8080` (хост-публикация 5050 — только для браузера/чеков) | [02-dictionary.md](02-dictionary.md) §4 |
| `patroni-nodes` | file_sd `/etc/prometheus/sd/patroni-nodes.json` (источник файла — генератор §4 из `/pgworker/portalloc/`) | [02-dictionary.md](02-dictionary.md) §5 нативные `patroni_*` |
| `pgnode-nodes` | file_sd `/etc/prometheus/sd/pgnode-nodes.json` (источник — тот же генератор §4, те же записи portalloc, порт 8080; http — закрытая сеть скрейпа) | [02-dictionary.md](02-dictionary.md) §8 серии агента PgNode (приём WAL) |
| `sd-generator` | static: имя сети стенда `sd-generator:8080` (профиль `metrics`) | самонаблюдение генератора §4 |

Джоба `patroni-nodes` — `scheme: https` +
тот же `tls_config {ca_file: /tls/ca.pem}` (без клиентского серта —
`verify_client` у REST нет; серт нод выпускает воркер из per-install CA —
[pgworker/01-placement.md](../pgworker/01-placement.md) §1; REST-скрейп нод — по TLS одного per-install CA, §4).
Джобы воркеров — `scheme: https` + `tls_config {ca_file: /tls/ca.pem}`
(`/metrics` воркеров — открытая https-грань без клиентского серта,
[pgworker/07-api.md](../pgworker/07-api.md) §2,
[kafkaworker/07-api.md](../kafkaworker/07-api.md) §2,
[valkeyworker/05-api.md](../valkeyworker/05-api.md) §2,
[s3worker/07-api.md](../s3worker/07-api.md) §2; контейнер
прометеуса монтирует TLS-пакет стенда ro; сетевой адрес не меняет
транспорт). Открытая грань (`/healthz`, `/metrics`) доступна в доверенной
docker-сети И через хост-публикации портов API docker-хоста
(`localhost:{PORT}`); хост-публикации нужны панели (AdvertiseUrl
`host.docker.internal`) и хостовым потребителям (чеки, скрейп); граница
доверия грани — периметр docker-хоста, а не только docker-сеть.
Опциональное сужение (bind `127.0.0.1` отдельным env-флагом; ломает
`host.docker.internal`-потребителей) — роадмап, каноном не
предписывается. Джоба `adminpanel` — http (панель
без TLS на стенде). extra_hosts (`host.docker.internal`,
`local`) из as-prometheus удалены — все таргеты прометея сетевые (единая
стендовая сеть контура, §4); SAN серверного серта покрывает
compose-DNS-имена (deploy/tls/gen.sh).

## 3. Дашборды (Grafana provisioning, JSON в репо)

`dashboards/workers.json` (циклы/клэймы/фазы/операции/снапшоты обоих
воркеров), `dashboards/kafka.json` (USR, consumer-lag, коллектор),
`dashboards/valkey.json` (память/hit-rate/эвикции/ops/клиенты/подключения/
slaves/коллектор),
`dashboards/pg.json` (репликация Patroni-нод, health-грань систем),
`dashboards/backups.json` (бэкап-домен [02-dictionary.md](02-dictionary.md) §7: возраст полных vs per-cluster
порог, WAL-лаг/тишина загрузок, исходы verify/restore/drill).

## 4. Таргеты нод: file_sd из portalloc (реализация в стенде, прод — тот же паттерн)

Единая сеть контура `pgw-metrics`: объявляет deploy-компоуз поставки
(`name: ${PGW_METRICS_NETWORK:-pgw-metrics}`) — в ней deploy-воркеры
PgWorker (aliases `pgworker`/`pgworker-2`), Prometheus (external-attach
из стендового компоуза) и подключаемые движком кластерные ноды
(PgWorker:Docker:ScrapeNetwork, [pgworker/01-placement.md](../pgworker/01-placement.md) §1/§5): скрейп
patroni-nodes — по `alias:8008` из portalloc, джоба pgworker — по
сетевым адресам воркеров; host-форвардинг и extra_hosts из контура
скрейпа удалены. Advertised-ветка (запись без alias) — деградационная:
легаси-записи/усыновлённые/чужой контур без сети — адрес честен,
достижимость — зона сетевой политики той поставки (сетевая семантика —
single-хост-контур «дома»).

Механика file_sd — генератор `sd-generator` (профиль `metrics`,
`src/Metrics.SdGenerator`): тиком читает `/pgworker/portalloc/`
(read-only, `RangeAsync`, без клэймов) и пишет `sd/patroni-nodes.json` в
volume Prometheus; таргет ноды — per-node правило
[02-dictionary.md](02-dictionary.md) §5 (`alias:8008`
штатно, `host:patroni` деградационно). Тем же проходом пишется
`sd/pgnode-nodes.json` — те же записи per-node, порт `:8080`
(телеметрия приёмника WAL [02-dictionary.md](02-dictionary.md) §8; джоба http — без TLS-конфига).
Механика file_sd/единой сети —
без изменений; фиксируется: REST-скрейп нод — по TLS одного
per-install CA (§2). Сеть `pgw-metrics` генератору не
нужна: etcd читает по compose-DNS сети стенда, таргеты резолвит
Prometheus. Граница: Kafka/Valkey-ноды без HTTP metrics-эндпоинта
наблюдаются доменными сериями коллекторов воркеров
([02-dictionary.md](02-dictionary.md) §3/§6); расширение словаря их нод — отдельные задачи.
