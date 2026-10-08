# t15-prometheus-file-sd — file_sd из etcd-снапшота для скрейпа реальных нод

**Фаза 1 dev-flow (spec).** Roadmap: `arch/roadmap/reliability.md` (тег
`t15-prometheus-file-sd`, P3 — наблюдаемость, характеристика N; отчёт —
`arch/roadmap/reliability-report.md`, строка в «Осталось»).

Решения пользователя по развилкам (зафиксированы до дизайна):

1. **Скоуб — только Patroni-ноды** (spilo, нативный `/metrics` REST :8008).
   Kafka/Valkey-ноды остаются на коллекторах воркеров (arch/18 §2.3/§2.6 —
   доменные метрики уже собираются); сырые метрики этих нод — вне словаря,
   граница фиксируется в arch/18.
2. **Генератор — отдельный .NET mini-сервис** в стеке мониторинга (профиль
   `metrics`), независимый от панели и воркеров (устойчивость контура
   мониторинга, характеристика N), переиспользует `Shared.Etcd`.
3. **Словарь реальных нод — нативные `patroni_*`** (генерация нод PgWorker не
   трогается); дашборд `pg.json` и группа алертов `pg` дополняются под них,
   эмуляторный словарь (`pg_replica_lag_seconds`) не меняется.
4. **Приёмка — тесты + docker-E2E** (генератор + настоящий `prom/prometheus` +
   реальный кластер в E2E-контуре); чек 65 не утяжеляется созданием кластеров.

## 1. Цель

Метрики реальных Patroni-нод кластеров PgWorker собираются Prometheus'ом
стенда. Сейчас ноды, создаваемые PgWorker, живут в per-cluster сетях
(`pgw-net-<C>`), недостижимых для Prometheus стека мониторинга: таргеты
Prometheus — static/file_sd только для сервисов стенда и эмуляторов
(arch/18 §2.5/§5.2), поэтому Patroni-ноды прод-кластеров невидимы для
мониторинга — репликация/HA реальных кластеров наблюдаемы только панелью.

Замыкающие элементы:

- **file_sd-генератор** — сервис, который тиком читает etcd (read-only,
  закрепления портов `/pgworker/portalloc/<C>`) и пишет file_sd JSON в volume,
  смонтированный в Prometheus; таргет ноды = `advertised host` + `patroni`-порт
  (host-публикация) — Prometheus скрейпит по публикуемым портам, per-cluster
  сети не нужны.
- **Scrape-джоба `patroni-nodes`** с file_sd; словарь нативных серий Patroni
  (`patroni_*`), дашборд и алерты Prometheus на них.
- **Граница словаря** — arch/18 фиксирует: Kafka/Valkey-ноды без HTTP
  metrics-эндпоинта наблюдаются доменными сериями коллекторов воркеров;
  file_sd-механика расширяема на новые домены отдельными задачами.

Результат: независимый от панели канал наблюдения репликации/HA реальных
кластеров (характеристика N), симметрично каналу бэкапов t14.

## 2. Принципы

- **arch-first**: контракт наблюдаемости — `arch/18` (правки Ф0, ДО кода:
  §2.5 словарь `patroni_*`, §5.2 джоба `patroni-nodes`, §5.4 паттерн
  file_sd реализован, §6/§8 — тесты и конфигурация). **Контракт etcd НЕ
  меняется**: генератор — немой read-only читатель существующих
  `/pgworker/portalloc/<C>` (как панель — «немой читатель», ноль новых
  etcd-ключей, ноль записей).
- **Генератор независим от наблюдаемых** (характеристика N): падение
  панели/воркеров не ослепляет мониторинг нод; генератор — часть стека
  мониторинга (профиль `metrics`), а не воркеров. Прецедент независимости —
  канал бэкапов t14.
- **Таргеты — из реплицированной истины**: единственный источник — portalloc
  в etcd (закрепление портов переживает rebuild ноды). Фильтр по статусам
  кластеров НЕ вводится: portalloc-ключ сносится демонтажом (X2), создаётся
  провижинингом — список таргетов самосинхронизируется с реальностью без
  чтения `states`. Единственный фильтр — `patroni > 0` (усыптлённые ноды без
  Patroni-REST не скрейпимы, arch/14 §2.4).
- **Консервативная свежесть**: ошибка чтения etcd — файл таргетов НЕ
  трогается (прежние таргеты живут), `sd_generator_last_success_timestamp_
  seconds` не двигается, тик не падает (warning-лог) — паттерн коллекторов
  Kafka/Valkey (arch/18 §4). Пустой префикс (кластеров нет) — валидный `[]`,
  успех.
- **Лейблы конечны** (arch/18 §2, риск M1): у таргетов — только `cluster`,
  `shard`, `node`; самонаблюдение генератора — серия без лейблов.
- **MULTI-HOST-честность**: таргет = `host:port` из portalloc-записи (host —
  advertised-имя docker-хоста ноды, arch/14 §2.4 п.5) — на мульти-хосте
  адреса остаются корректными; достижимость скрейпа — зона сетевой политики
  прода (§5.4 паттерн документируется, таргеты честны).
- **Запись файла минимальна**: file_sd JSON перезаписывается только при
  изменении контента (byte-compare), атомарно (tmp + rename) — mtime не
  дёргается впустую, Prometheus перечитывает по факту изменения.

## 3. Структура/компоненты

### 3.1. Правки контракта arch/18 (Ф0, до кода)

| Секция | Правка |
|---|---|
| §2.5 | Словарь реальных Patroni-нод: нативные серии REST `/metrics` (spilo, Patroni 4.x) — канон-минимум, на который пишутся дашборд/алерты: `patroni_master`, `patroni_replica`, `patroni_sync_standby`, `patroni_timeline`, `patroni_xlog_replay_timestamp`, `patroni_version`, `patroni_postgres_running` (лейблы `scope`, `name` — сам Patroni). Эмуляторы остаются на `pg_replica_lag_seconds` — два словаря сосуществуют: эмуляторный (стенд без PgWorker-кластеров) и нативный (реальные ноды); фактический набор фиксирует docker-E2E (паттерн M3) |
| §5.2 | Новая строка таблицы джоб: `patroni-nodes` — file_sd `sd/patroni-nodes.json`, scheme http (Patroni REST без TLS — t22 вне скоуба), source: генератор из `/pgworker/portalloc/` |
| §5.4 | Паттерн реализован в стенде: file_sd-генератор `sd-generator` (профиль metrics) читает portalloc и пишет таргеты в volume; прод-мультихост — тот же паттерн рядом с Prometheus (адреса из portalloc честны для мульти-хоста); граница: Kafka/Valkey-ноды без HTTP metrics-эндпоинта — доменные метрики через коллекторы воркеров (§2.3/§2.6), расширение словаря их нод — отдельные задачи |
| §6 | Приёмка: unit-маппинг portalloc→таргеты, integration-цикл с etcd-фикстурой, docker-E2E «реальный кластер → таргет up → демонтаж → таргет исчез» |
| §8 | Секция конфигурации `SdGenerator:*` |

`arch/14`/`arch/15`/`arch/20`/`arch/21` НЕ меняются (контракт portalloc
существует; новый потребитель — читатель).

### 3.2. Генератор — `src/Metrics.SdGenerator`

.NET mini-сервис (console + hosted service; паттерн поставки воркеров):
`HostApplicationBuilder` + фоновый цикл + Kestrel только для самонаблюдения
`/metrics` (порт compose-сети, http — доверенная сеть, паттерн джобы
`adminpanel`).

- **Цикл** (`SdGeneratorLoop`, тик `SdGenerator:RefreshIntervalSec`, дефолт
  15, `<=0` → 15 + warning-лог — паттерн §4.2 valkey):
  `IEtcdGateway.RangeAsync("/pgworker/portalloc/")` c перебором endpoints
  (`EtcdFailover`-паттерн) → парсинг закреплений → сборка таргетов →
  атомарная запись при diff.
- **Маппинг** (чистая функция, тестируется unit'ами): ключ
  `/pgworker/portalloc/<C>` (JSON `{"<X>/<n>":{"host","pg","patroni","doorman",...}}`)
  → на каждую ноду с `patroni > 0` группу file_sd:
  `{"targets":["<host>:<patroni>"],"labels":{"cluster":"<C>","shard":"<X>","node":"<n>"}}`.
  Порядок групп детерминирован (cluster, shard, node). Битые/незнакомые
  JSON-записи — пропуск записи с warning-логом (толерантность панели,
  arch/adminpanel: «state-значения строкой — система развивается»).
- **Самонаблюдение** (`Shared.Metrics`, Meter `SdGenerator`):
  `sd_generator_last_success_timestamp_seconds` (gauge; обновляется только при
  успешном тике — консервативно, пустой префикс = успех).
- **Сам скрейп**: джоба `sd-generator` в prometheus.yml — static
  `sd-generator:8080`, http (сеть стенда).

### 3.3. Стенд: сервис и Prometheus (профиль `metrics`)

`dev-stand/adminpanel/docker-compose.yml`:

```yaml
  sd-generator:
    build: { context: ../.., dockerfile: docker/Metrics.SdGenerator.Dockerfile }
    image: sdgenerator:dev            # локально собираемый — в registry НЕ класть
    container_name: as-sd-generator
    profiles: ["metrics"]
    volumes:
      - prometheus-sd:/sd             # rw — пишет file_sd
    environment:
      SdGenerator__Etcd__Endpoints__0..2: http://etcd1..3:2379   # сеть стенда
      SdGenerator__OutputPath: /sd/patroni-nodes.json
  prometheus:
    volumes:
      - prometheus-sd:/etc/prometheus/sd:ro   # новый маунт к существующим
```

`metrics/prometheus/prometheus.yml` — новая джоба:

```yaml
  - job_name: patroni-nodes        # реальные ноды PgWorker (t15, arch/18 §2.5)
    file_sd_configs:
      - files: ["/etc/prometheus/sd/patroni-nodes.json"]
      - refresh_interval: 30s
```

`pgworker-targets.json` (перезапись 00-up.sh) не меняется — прецедент
file_sd сохраняется, механика расширяется.

### 3.4. Алерты и дашборд

`rules.yml` — три новых правила:

| Alert | Expr | Severity/for |
|---|---|---|
| `PatroniNodeDown` | `up{job="patroni-nodes"} == 0` | warning / 5m (нода в rebuild — штатный сценарий, порог терпит пересоздание ~90 с) |
| `PatroniReplicaLagHigh` | `time() - patroni_xlog_replay_timestamp > 30` | warning / 5m (серия есть только у реплик — зеркало `PgReplicaLagHigh` эмуляторов) |
| `SdGeneratorStalled` | `time() - sd_generator_last_success_timestamp_seconds > 300` | warning / 0m (фиксированный порог ≥ 3×RefreshIntervalSec по образцу коллекторов §4) |

`dashboards/pg.json` — панели: «Patroni nodes up (real)»
(`up{job="patroni-nodes"}` by cluster/shard/node), «Patroni role»
(`patroni_master`/`patroni_replica` by scope/name), «Replica replay lag, s»
(`time() - patroni_xlog_replay_timestamp`), «Timeline» (`patroni_timeline`).
Существующие эмуляторные панели остаются.

### 3.5. Образ и конфигурация

- `docker/Metrics.SdGenerator.Dockerfile` — runtime-слой с `COPY` publish-вывода
  (сборка на хосте — канон E2E-образов; база `mcr.microsoft.com/dotnet/aspnet:
  10.0` уже в images.txt, новых внешних образов нет).
- Конфигурация (`[Config]`-паттерн, env-оверрайды compose):

```
SdGenerator:Etcd:Endpoints[]          # узлы HA-контура (as-etcd-1/2/3)
SdGenerator:RefreshIntervalSec = 15   # <=0 → 15 + warning
SdGenerator:OutputPath = /sd/patroni-nodes.json
SdGenerator:Metrics { Enabled=true, Path="/metrics" }   # самонаблюдение
```

## 4. Фазы

| # | Фаза | Содержимое |
|---|---|---|
| Ф0 | arch-first | Правки `arch/18` §2.5/§5.2/§5.4/§6/§8 (§3.1) — контракт до кода |
| Ф1 | Генератор | `src/Metrics.SdGenerator` (цикл, маппинг, самонаблюдение), `docker/Metrics.SdGenerator.Dockerfile`, unit-тесты `src/tests/Metrics.SdGenerator.UnitTests` |
| Ф2 | Стенд и мониторинг | compose-сервис + volume, `prometheus.yml` (джобы `patroni-nodes`, `sd-generator`), `rules.yml` (+3 алерта), `pg.json`, счётчик алертов чека 65 → `>= 21` (18+3) |
| Ф3 | Integration | `src/tests/Metrics.SdGenerator.IntegrationTests`: etcd-фикстура (testcontainers, динамический порт) → цикл генератора → ассерты файла (появление/обновление/исчезновение таргетов, недоступный etcd не трогает файл, `last_success` стоит) |
| Ф4 | docker-E2E | `src/tests/PgWorker.IntegrationTests`, класс E2E `E2ePatroniFileSd`: контур E2eFixture (etcd + PgWorker + реальный кластер) + контейнер sd-generator + контейнер `prom/prometheus` с file_sd на общий volume → создание кластера (динамические порта, `Scale_AddEmptyShard`-подобная механика) → таргеты `patroni-nodes` up, серии `patroni_*` в TSDB → демонтаж кластера → таргеты исчезли. Полный teardown контура (канон E2E-изоляции) |
| Ф5 | Мерж-гейт | Roadmap-гейт трека (см. §6, п. 6) |

## 5. Ограничения

- Код PgWorker/KafkaWorker/ValkeyWorker/AdminPanel НЕ меняется (решение 3:
  нативные `patroni_*`, без custom-SQL в SPILO_CONFIGURATION): воркеры не
  узнают о скрейпе; обязательный docker-E2E воркеров не триггерится, E2E Ф4 —
  приёмка самой t15.
- Kafka/Valkey-ноды не скрейпятся (нет HTTP metrics-эндпоинта; JMX/RESP-
  exporter'ы — отдельные задачи при потребности); их доменные метрики уже
  живут у воркеров (§2.3/§2.6).
- Ноды с `patroni = 0` (усыптлённые, Patroni-REST сайдкаром/отсутствует) в
  таргеты не попадают — их живость закрывают панельные SQL-пробы (arch/14 §5 C).
- Демо-кластер стенда — усыновлённые postgres-контейнеры с hc-эмуляторами:
  если adopt-закрепление несёт `patroni ≠ 0`, таргеты эмуляторов появятся в
  `patroni-nodes` параллельно со static-джобой `patroni` (другой
  `instance`-адрес host-публикации) — дублирование серий между джобами
  допустимо (различаются `job`-лейблом).
- Patroni REST — открытый HTTP (arch/13, t22 вне скоуба): скрейп без
  tls_config, доверенная зона хост-публикаций.
- Образ `sdgenerator:dev` локально собираемый — в registry не попадает
  (канон AGENTS.md); новых строк в `images.txt` нет.
- Стенд single-host: advertised host нод `host.docker.internal` (контейнер
  Prometheus уже с `extra_hosts: host-gateway`); мульти-хост-достижимость —
  прод-паттерн §5.4, таргеты portalloc честны.

## 6. Критерии приёмки

1. **Unit**: маппинг `portalloc → file_sd` — лейблы cluster/shard/node,
   `targets = host:patroni`, пропуск `patroni ≤ 0`, пропуск битых записей,
   пустой префикс → `[]`, детерминизм порядка, дефолт интервала `<=0` → 15.
2. **Integration** (живой etcd): put portalloc → таргет в файле ≤ интервала;
   del → таргет исчез; недоступный etcd → файл и `last_success` не изменились;
   восстановление → догоняет.
3. **docker-E2E**: реальный кластер PgWorker в E2E-контуре — таргеты
   `patroni-nodes` up в настоящем Prometheus, канон-минимум `patroni_*`
   (§3.1) в TSDB (факт фиксирован тестом); демонтаж кластера — таргеты
   исчезли; контур полностью зачищен teardown'ом.
4. **Стенд**: полный подъём `00-up.sh` зелёный (профиль metrics поднимает
   `sd-generator`); чек 65 — алерт-рулов `>= 21`, таргеты `patroni-nodes`
   при их наличии up; сервис `sd-generator` самоскрейпится.
5. **Словарь канонизирован**: `sd_generator_last_success_timestamp_seconds`
   и `patroni_*`-минимум зафиксированы тестами против arch/18 (факт — M3).
6. **Мерж-гейт трека reliability** (одним мерж-коммитом): пункт `t15` снят
   из `arch/roadmap/reliability.md`; в `arch/roadmap/reliability-report.md`
   строка перенесена в «Сделано» (merge-коммит + что изменилось в N), из
   сводки N убрано «метрики реальных нод не собираются» и слабое место
   «прод-ноды недоскрейпимы» — заменено на file_sd-канал нод (оговаривается
   граница Kafka/Valkey-нод); `arch/18` правки Ф0 в этом же коммите.
