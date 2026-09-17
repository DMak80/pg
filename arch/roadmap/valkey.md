# Roadmap: Valkey-домен (канон, ValkeyWorker, панель, дискавери-библиотека)

Задачи добавления управления Valkey-кластерами по образцу Kafka-домена:
**доступ** (клиентский дискавери endpoints/кредов) и **контроль**
(декларативное управление) — через единый etcd-контур, как у PG и Kafka.
Канон-образцы — [../15-kafka-clusters.md](../15-kafka-clusters.md)
(контракт etcd `/kafka/` + клиентский дискавери),
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер),
[../17-synchronization-principles.md](../17-synchronization-principles.md).
Клиентская библиотека исполняется в репозитории Puzzle
(`PuzzleServer.Infrastructure.App.HA.Valkey` по образцу HA.Kafka —
`docs/01.19-ha-kafka.md` там). Канон домена —
[../20-valkey-clusters.md](../20-valkey-clusters.md) (контракт etcd) и
[../21-valkeyworker.md](../21-valkeyworker.md) (оркестратор).

Порядок: канон → воркер → панель → библиотека Puzzle → метрики.

## Задачи

- **`t03-valkey-panel`** — valkey-домен AdminPanel:
  etcd-инспекция `/valkey/` (AdminPanel.Etcd), API + React-панель —
  кластеры/ноды/состояния, live-пробы, алерты, операции (создание/удаление
  кластера, операции над нодами).
- **`t04-valkey-discovery-lib`** — клиентская
  дискавери-библиотека в Puzzle: `PuzzleServer.Infrastructure.App.HA.Valkey`
  — только читатель `/valkey/clusters/<C>/` (снапшот endpoints/креды/state,
  watch+poll актуализация, fail-open, `GetClientConfig()` с plain-полями для
  StackExchange.Redis). Объём — только библиотека; интеграция в клиентский
  модуль Valkey выделена в `t08-valkey-client-integration` (клиентского
  модуля Valkey/Redis в Puzzle пока нет — как у kafka интеграция была t10).
- **`t05-valkey-metrics`** — телеметрия Valkey-домена
  по образцу [../18-metrics.md](../18-metrics.md): ValkeyWorker на каркасе
  `Shared.Metrics` (воркер-паттерн §2.2: фазы/HealthState), коллектор метрик
  Valkey-нод (аналог коллектора Kafka §4: INFO/репликация через redis-пробу,
  самонаблюдение коллектора), доменный словарь §2, дашборд Grafana
  `dashboards/valkey.json`, конфиг `ValkeyWorker:Metrics`.
- **`t06-valkey-tls`** — TLS клиентских подключений
  Valkey-кластеров (образец — kafka t03, arch/16 §2.3). **Что нужно
  сделать**: per-cluster CA (`ca_pem`/`ca_key` в
  `/valkey/clusters/<C>/`, ensure воркером при provisioning, подпись
  серверного сертификата ноды CN=`node<k>` + SAN advertised-хоста);
  tls-port контейнера (порт из portalloc; plain-порт закрывается);
  дискавери-ключ `ca_pem` для внешнего читателя — доверие клиентов
  StackExchange.Redis (`GetClientConfig()` получает `ssl=true` + CA);
  advertised/SAN-правило по 16 §2.1; окно двойного доверия при ротации
  CA — по потребности (образец CaRotator 16 §5 K). **Зачем**:
  шифрование клиентского трафика и аутентификация сервера (сейчас —
  доверенная docker-сеть + ACL-креды: пароли ходят по сети открыто в
  пределах закрытого контура). **Почему отложено**: кеш восполним и не
  содержит данных дороже секрета доступа; v1 живёт в доверенной
  закрытой docker-сети домашней установки (enterprise-защиты не нужны
  — AGENTS базовые правила п.8); контракт кред/endpoints не меняется —
  добавятся только CA-ключи, внешняя библиотека t04 совместима без
  переделок (обратная совместимость дискавери arch/20 §4).
- **`t08-valkey-client-integration`** (`← t04-valkey-discovery-lib`) —
  интеграция HA.Valkey в клиентский модуль Valkey приложения
  (StackExchange.Redis) по образцу связки HA.Kafka →
  `Infrastructure.App.Kafka` (docs/01.16 §1a): клиентский модуль в HaDb-режиме
  регистрирует `AddHaValkey(...).AddValkeyCluster(<Valkey:Cluster>)`,
  соединительные параметры берёт из снапшота `GetClientConfig()`, ротация
  `app_password`/смена endpoints доставляется событием `Updated`, fail-open
  без параметров. Берётся в работу, когда клиентский модуль Valkey появится
  в Puzzle.
