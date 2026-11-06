# Runbook: внешние docker-образы и локальный registry

Уровень 3. Внешние docker-образы, локальный registry `192.168.0.1:5000`,
сборка образа PG-ноды, каталог зеркалируемых образов. Родитель —
[README.md](README.md); placement и образ ноды —
[pgworker/01-placement.md](../pgworker/01-placement.md) §1;
поставка образов — `dev-stand/images/`.

Все **внешние** образы (dev-стенд, тесты, деплой) берутся из приватного registry
локальной сети `https://192.168.0.1:5000`; если registry недоступен — из
апстрима (Docker Hub / quay.io / ghcr.io / mcr.microsoft.com). Исчерпывающая
инструкция по самому registry (TLS, auth, API, бэкап) — [`../../../docker-registry.md`](../../../docker-registry.md)
в каталоге над репозиторием.

**Правило приоритета: сначала registry, при недоступности — Docker Hub/апстрим.**
Перед подъёмом стенда и прогонами docker-тестов:

```bash
dev-stand/images/pull-images.sh
```

Скрипт ДОСТАВЛЯЕТ ТОЛЬКО ОТСУТСТВУЮЩЕЕ: образ, уже лежащий локально под
каноническим именем (`docker image inspect`), не тянется вовсе — скрипт печатает
`local:` и не лезет в сеть за имеющимся. Отсутствующий образ тянется из
`192.168.0.1:5000/<образ>` и перетегируется в каноническое имя (`postgres:18`,
`quay.io/coreos/etcd:v3.5.21`, …) — compose-файлы и тесты остаются на
канонических именах и не знают про registry; если registry недоступен — образ
тянется напрямую из апстрима (скрипт печатает `upstream:`). Обновление УЖЕ
имеющегося локально образа (плавающий тег догнал апстрим) — осознанная процедура,
пре-пуллом не делается: `docker rmi <образ>` + повтор пре-пулла, либо обновление
зеркала на хосте registry (`mirror-all.sh` / `mirror-image.sh`) с последующим
`docker rmi` + пре-пуллом на клиенте.

## 1. Разовая настройка клиента

CA-сертификат registry должен быть доверен Docker и выполнен логин (подробно —
в `../../../docker-registry.md`): CA положить в
`~/.docker/certs.d/192.168.0.1:5000/ca.crt` и перезапустить Docker Desktop,
затем `docker login 192.168.0.1:5000 -u dm` (пароль — в инструкции registry).

## 2. Имена в registry

`192.168.0.1:5000/<образ>`: для Docker Hub — краткое имя как есть
(`192.168.0.1:5000/postgres:18`, `192.168.0.1:5000/apache/kafka:4.0.0`), для
чужих реестров — с хостом (`192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`,
`192.168.0.1:5000/mcr.microsoft.com/dotnet/sdk:10.0`). Рядом с мульти-арх тегом
лежат per-arch теги `…-amd64` / `…-arm64`.

## 3. ⚠️ Локально собираемые образы в registry НЕ класть — никогда

`pgworker:dev`, `kafkaworker:dev`, `adminpanel:dev`,
`pgworker-node:dev|e2e`, сайдкары/opsbox стенда —
артефакты конкретной машины/ветки. В общий registry они **категорически
запрещены**: реестр — зеркало воспроизводимых внешних зависимостей, а не хранилище
сборок. `mirror-image.sh` для таких имён не запускать, в `images.txt` не вносить.

## 4. Образ PG-ноды с приёмником WAL (двухшаговая сборка)

Приёмник WAL бэкапов исполняется процессом ВНУТРИ контейнера PG-ноды
(supervisord-программа `pgnode`, `/opt/pgw-node`). Отдельного образа
`pgworker-wal` и контейнеров `pgw-backup-wal-*` больше нет; сборка ноды —
двухшаговая (publish на хосте, в контейнер — только артефакты):

    dotnet publish src/PgWorker.PgNode -c Release -r <rid> \
      --self-contained -o artifacts/node-wal        # rid: linux-arm64 | linux-x64
    dotnet publish src/PgWorker.NodeTools -c Release -r <rid> \
      --self-contained -o artifacts/node-tools
    docker build -f docker/node/Dockerfile \
      --build-arg PGNODE_PUBLISH_DIR=artifacts/node-wal \
      --build-arg NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools \
      --build-arg DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0 \
      -t pgworker-node:dev .

`DOORMAN_IMAGE` — обязательный build-arg; требования к образу ноды и пулеру
(мульти-арх, платформа сборки) — [pgworker/01-placement.md](../pgworker/01-placement.md) §1.
Сборка без build-arg падает fail-fast. Перед сборкой на офлайн-хосте —
`dev-stand/images/pull-images.sh` (ретаг канонического имени из локального
registry). Обновление версии пина — рутина ниже («Обновить пин pg_doorman»).


Полный стенд собирает образ скриптом `dev-stand/adminpanel/checks/00-up.sh`
(RID определяется по `uname -m` автоматически).

Обновление живых кластеров на новый образ ноды: надзор НЕ пересоздаёт живые
ноды — приёмники появятся после пересоздания нод штатным механизмом (recreate
через панель/API либо `docker rm -f pgw-…` на docker-хосте — надзор поднимет
контейнер с новым образом; данные в volume сохраняются). До пересоздания
контроль воркера показывает DEGRADED «источник без живого приёмника» с именем
ноды. Остатки старых wal-контейнеров `pgw-backup-wal-*` сносятся воркером
автоматически (санитарный вызов в тике WalStreamProcess), руками чистить не
нужно.

## 5. Добавить новый внешний образ / обновить версию

1. Добавить строку `<образ>` в [`dev-stand/images/images.txt`](../../dev-stand/images/images.txt)
   (единый источник списка).
2. Зеркалировать: `dev-stand/images/mirror-image.sh <образ>` (мульти-арх
   amd64+arm64, пер-арх pull по digest'ам → per-arch push → манифест-лист);
   всё разом — `mirror-all.sh`.
3. Проверить: `docker manifest inspect --insecure 192.168.0.1:5000/<образ>` —
   в списке `amd64` и `arm64`.

## 6. Обновить пин pg_doorman

1. Изменить пин `ghcr.io/ozontech/pg_doorman:v3.11.0` на новую версию
   РОВНО в четырёх рабочих точках: `dev-stand/adminpanel/checks/00-up.sh`,
   `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` (константа
   `DoormanImage`),
   `src/tests/PgWorker.IntegrationTests/Backups/NodePgNodeTests.cs`
   (константа `DoormanImage` — файл сам собирает образ ноды с
   `--build-arg DOORMAN_IMAGE`), этот раздел (команда сборки + строка
   каталога ниже).
2. Обновить строку в `dev-stand/images/images.txt`.
3. Перемirrorить: `dev-stand/images/mirror-image.sh ghcr.io/ozontech/pg_doorman:<новая версия>`,
   затем `dev-stand/images/pull-images.sh`.
4. Контроль полноты: `grep -rn "pg_doorman:v" dev-stand/ src/ docker/ arch/operations/02-registry-images.md`
   — вхождений старой версии не осталось (grep заодно ловит упоминания пина
   вне рабочих точек: комментарий в `docker/node/Dockerfile`, этот раздел).

## 7. Известная особенность: TLS-сертификат registry и Go-клиенты

Сертификат registry (самоподписанный, RSA-4096/SHA-256) отвергается новыми
Go-1.24-клиентами с ошибкой `x509: certificate is not standards compliant`
(docker-daemon при этом работает). Поэтому зеркалирование идёт через daemon
(pull/push) + `docker manifest --insecure`, а не через `docker buildx imagetools`.
Если сборка манифест-листа падает по TLS — это оно.

## 8. Каталог зеркалируемых образов

| Образ | Где используется |
|---|---|
| `alpine:3.20` | тесты PgWorker (Docker/Exec/SshTunnel/TlsEngine), наполнение TLS-volume в `00-up.sh` |
| `apache/kafka:4.0.0` | KafkaWorker: дефолт образа брокера (`Options`), интеграция-тесты, kafka-e2e чек стенда |
| `ghcr.io/ozontech/pg_doorman:v3.11.0` | бинарник пулера в образе PG-ноды (обязательный build-arg DOORMAN_IMAGE, COPY --from) |
| `ghcr.io/zalando/spilo-18:4.1-p2` | база образа PG-ноды `pgworker-node` (`docker/node/Dockerfile`) |
| `grafana/grafana:13.0.8` | dev-стенд (метрики) |
| `mcr.microsoft.com/dotnet/sdk:10.0` | build-стадии Dockerfile'ов PgWorker/KafkaWorker/AdminPanel |
| `mcr.microsoft.com/dotnet/aspnet:10.0` | runtime-стадии тех же Dockerfile'ов |
| `minio/mc:RELEASE.2025-08-13T08-35-41Z` | бакетные проверки E2E, `00-up.sh` (запинен — supply-chain; операции бэкапов — .NET S3-клиент приёмника, mc не потребляют) |
| `minio/minio:RELEASE.2025-09-07T16-13-09Z` | dev-стенд — переходный read-only-архив `as-minio` (снос — `../../../OwnS3/docs/owns3-migration.md` §6) |
| `nginx:alpine` | TlsEngineProxyTests |
| `node:22-alpine` | AdminPanel.Dockerfile (сборка SPA) |
| `postgres:18-alpine` | интеграционные тесты бэкапов (OwnPostgres, WalSqlTests) |
| `postgres:18` | dev-стенд (шарды), тесты AdminPanel, opsbox |
| `prom/alertmanager:v0.28.1` | dev-стенд (метрики) |
| `prom/prometheus:v3.14.0` | dev-стенд (метрики) |
| `python:3.12-alpine` | сайдкар эмулятора Patroni (dev-stand) |
| `quay.io/coreos/etcd:v3.5.21` | etcd-контур: стенд ×3 (HA, external), deploy/etcd ×1 (seed; состав доводит EtcdWorker), E2E/интеграция-фикстуры (×5), базовый слой локально собираемого образа `pgworker/etcdnode` (узлы контура с агентом EtcdNode — сам локальный образ в registry НЕ кладётся) |

При смене версии образа в коде/компоузе — обновить строку в `images.txt` и
перемазеркалировать (старую версию из registry можно удалить — см. «Удаление
образа» в инструкции registry).
