# Spec t39-owns3-e2e: приёмочная грань ownS3 (E2E реальными клиентами, dev-стенд-сервис, health/метрики; снятие If-Range и UploadPartCopy)

Дата: 2026-10-10. Постановка — `arch/roadmap/owns3.md` (тег `t39-owns3-e2e`).
Каноны: `arch/owns3/01…05` (протокол, операции, хранение, доступ/конфиг);
E2E-каноны проекта — `AGENTS.base.md` §13, `docs/e2e-isolation.md`,
`docs/e2e-launch.md`. Контекст реализованных слоёв (t36–t38):
`docs/superpowers/2026-10-10-t38-owns3-multipart/spec.md`.

Правки ревью спеки (решения пользователя):
- **Q1: поддержка `If-Range` снимается с ownS3 полностью** — канон и код
  зачищаются в составе t39 (AWSSDK.S3 3.7.511.8 не позволяет клиенту
  выставить `If-Range`; §3.3, §4.6, §9.1).
- **Q2: серверная операция `UploadPartCopy` снимается с ownS3 полностью**,
  симметрично Q1 — контракт, реализация t38 и тестовое покрытие изымаются
  (AWSSDK.S3 3.7.511.8 не содержит API `UploadPartCopy`; §3.4, §4.6, §9.2).
  Общий принцип пользователя: всё, что не поддерживает AWSSDK.S3, убирается
  из кода в рамках текущей задачи; код приводится согласно описанию
  AWSSDK.S3.
- **Trailing slash `/{bucket}/` — bucket-level** (внесено шагом 4.1a плана,
  решение пользователя по схеме Q1/Q2 — фикс дефекта Фазы 6): правка
  03 §6 — пустой ключ трактуется как его отсутствие (прецедент S3/MinIO;
  AWS SDK CreateBucket и mc probe шлют именно так).

## 1. Цель

Приёмочная грань ownS3: доказать, что реализованный S3-контур (21 операция,
SigV4, xl-хранение) работает с **реальными клиентами** — не с собственным
тестовым подписантом интеграционных тестов, а с AWS SDK .NET (клиент подсистемы
бэкапов) и `mc` (клиент оператора). Составляющие:

1. **Интеграционные/E2E тесты реальными клиентами**: docker-E2E серия
   `src/tests/OwnS3.IntegrationTests/E2e` — контейнер `owns3:e2e` в полностью
   своём изолированном окружении, клиенты: AWS SDK .NET (простые PUT/GET,
   multipart, conditional, Range; оба режима flexible checksums) и `mc`
   (админ-операции). Изоляция, телеметрия и зачистка — по канонам
   `docs/e2e-isolation.md` / `docs/e2e-launch.md`.
2. **Dev-стенд сервис в докере**: сервис `owns3` в `deploy/docker-compose.yml`
   (образ — по правилам E2E-образов: сборка .NET на хосте, publish-COPY;
   локально собираемый образ в registry не кладётся), env-секреты из
   `deploy/.env` (шаблон — секция `OWNS3_*` в `deploy/.env.example`),
   подъём в `00-up.sh`, smoke-чек `46-owns3.sh`.
3. **Health/метрики**: `/healthz` и `/metrics` в E2E и стенд-чеке (состав —
   канон 05 §5, уже реализован; приёмка подтверждает их в докер-контуре).
4. **Снятие граней, недостижимых для целевых клиентов** (расширение scope
   по решениям пользователя Q1/Q2 на ревью спеки): `If-Range` (заголовок
   чтения) и `UploadPartCopy` (серверная операция копирования части) —
   ни AWS SDK .NET, ни `mc` не могут их использовать; обе грани изымаются
   из канона (02/03) и из кода/тестов ownS3. Набор операций становится
   **21** (был 22). Поведение на остаточных запросах: заголовок
   `If-Range` игнорируется; `PUT ?partNumber&uploadId` с заголовком
   `x-amz-copy-source` обрабатывается как обычный `UploadPart` (заголовок
   игнорируется) — ровно новые формулировки канона 02 (§3.3–3.4, §4.6).

После t39 ownS3 готов к вытеснению MinIO (переключение клиентов
env-endpoint'ом — без изменений кода клиентов); сам перевод бэкапов — t41.

### Не-цели (границы)

- **Перевод подсистемы бэкапов на ownS3** — t41: стендовые бэкапы остаются
  на `as-minio` (`PGW_BACKUP_S3_*` не меняются); E2E «PgWorker-джоба бэкапа
  против ownS3» не делается.
- **Erasure coding (t40), TLS (t42), миграция данных (t41)** — отдельные
  задачи трека; ownS3 остаётся single-drive HTTP-only.
- **Права/матрица ролей, протокольные краевые кейсы** (битые XML, лимиты,
  версия подписи, вне-наборные грани) — уже покрыты интеграционными
  сценариями t36–t38 (`AuthScenarios`, `AccessScenarios`, `ErrorFormatScenarios`,
  `BodyIntegrityScenarios`, `MultipartScenarios`); E2E их не дублирует.
- **Персистентность тома через рестарт контейнера** (PUT → restart → GET):
  расширяет букву roadmap-пункта; при желании — отдельным roadmap-пунктом.
- **Прод-Dockerfile поставки ownS3** (multi-stage из исходников) — отдельный
  процесс (канон 01 §5); в t39 только E2E-стиль образа.
- Новые панели/дашборды Grafana под ownS3-метрики — не делаются; скрейп
  Prometheus-стендом — вопрос будущего (метрики доступны на `/metrics`).
- **Иные контрактовые изменения S3-грани, кроме снятия `If-Range` и
  `UploadPartCopy`** — вне t39: E2E — приёмочная серия, найденные дефекты
  обслуживаются отдельными пунктами плана с обоснованием против канона.

## 2. Принципы

1. **Arch-first**: модель запуска ownS3 в докере (стенд-сервис, env-шаблон,
   E2E-серия, портовой ряд) и изъятия `If-Range`/`UploadPartCopy`
   фиксированы канонами (05 §6; 02 §1/§5, 03 §2/§4, 01/22 — счёт 21)
   ДО кода (правки раздела 3 внесены этой спекой). Прочие контракты
   протокола/операций/хранения t39 только проверяет реальными клиентами.
2. **Изоляция по канонам E2E** (`AGENTS.base.md` §13, `docs/e2e-isolation.md`):
   единица изоляции — сценарный класс; каждое окружение целиком своё
   (сеть, контейнер ownS3, том), guid во всех именах; own-only чистка,
   teardown при любом исходе, ассерт чистоты; только динамические порты;
   телеметрия по `docs/e2e-launch.md` (логи до удалений, `MarkFailed` →
   stop-без-удаления, `README-cleanup.txt`, `[PHASE]`-метки, без
   перезапуска упавших).
3. **Реальные клиенты как есть**: AWS SDK .NET и `mc` в их фактических
   режимах (никаких собственных подписантов); клиентские возможности,
   недоступные целевым клиентам SDK (такие как `If-Range`,
   `UploadPartCopy`), из системы изымаются, а не тестируются обходными
   путями (принцип решения Q2). Исключение — управляемая настройка
   flexible checksums (канон 03 §3: `RequestChecksumCalculation` обязан
   учитываться приёмкой): оба поддерживаемых режима SDK проверяются
   явно — `WHEN_REQUIRED` (основные сценарии) и `WHEN_SUPPORTED`
   (чанковая подпись `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` —
   реальным клиентом).
4. **Образ по правилам E2E** (AGENTS.md, канон 05 §6): `dotnet publish` на
   хосте (инкрементально — секунды), в контейнер упаковывается только
   publish-вывод (runtime-слой, без `COPY src/` и sdk-стадий); сборка —
   с `[PHASE]`-меткой и таймингом; тег в registry `192.168.0.1:5000`
   не кладётся; новых внешних образов нет (aspnet:10.0, minio/mc — в
   `images.txt`).
5. **Свежий Release на каждом прогоне** (урок t09): E2E-окружение само
   делает `dotnet publish -c Release` (инкрементальный no-op — секунды);
   `PGW_TEST_E2E_NOBUILD=1` — только бисект/отладка конкретного бинаря.
6. **Гейт docker-серии**: `PGW_TEST_DOCKER=1` (прецедент PgWorker/
   KafkaWorker — единая env для docker-серий монорепо); без неё тесты
   Skip — CI без docker остаётся зелёным.
7. Язык: документация и комментарии — русские; идентификаторы — английские.

## 3. Arch-first правки канона (внесены в worktree этой спекой)

1. **`arch/owns3/05-access-config.md` §6 «Запуск в докере»** — модель,
   которую канон откладывал до t39 («детальная модель dev-стенд-сервиса —
   t39»), зафиксирована: Dockerfile `docker/OwnS3.Dockerfile` с
   `ARG OWNS3_PUBLISH_DIR` (прецедент `Metrics.SdGenerator.Dockerfile`),
   теги `owns3:dev`/`owns3:e2e`, стенд-сервис `owns3` в
   `deploy/docker-compose.yml` (env из `deploy/.env`, публикация
   `${OWNS3_HOST_PORT:-9010}:9000` — 9000/9001 заняты `as-minio`, том
   `owns3-data`), подъём `00-up.sh`, чек `46-owns3.sh`, E2E-серия
   `src/tests/OwnS3.IntegrationTests/E2e` с гейтом `PGW_TEST_DOCKER=1` и
   приёмкой checksum-режимов `WHEN_REQUIRED`/`WHEN_SUPPORTED`. Заодно
   удалена историческая атрибуция слитой задачи («не канон t35» в §5/§6).
2. **`arch/owns3/01-overview.md` §5** — плейсхолдеры «env-шаблон создаётся
   в t39» и «детальная модель — t39» заменены ссылкой на главу 05 §6.
3. **`arch/owns3/02-operations.md` §1 «Range-запросы»** — `If-Range` изъят
   из поддерживаемых семантик: пункт «`If-Range` не поддерживается:
   заголовок игнорируется — при его наличии ответ определяется только
   `Range`» (обоснование — целевые клиенты заголовок не отправляют: AWS
   SDK .NET не выставляет его вовсе; критерий — простота); в контракте
   `GetObject` упоминание `If-Range` в заголовках убрано. Решение
   пользователя (Q1, §9.1) — до зачистки кода.
4. **`UploadPartCopy` изъят из канона** (решение пользователя Q2, §9.2 —
   до зачистки кода):
   - `02-operations.md`: контракт операции `UploadPartCopy` (раздел 5)
     удалён целиком; счёт «22 операции» → «21 операция» (введение, раздел
     вне-наборных граней); ETag-таблица и лимит части — «`UploadPart`» без
     `UploadPartCopy`; `CopyObject` `EntityTooLarge` — серверная копия
     источника > 5 ГБ не поддерживается (клиент копирует большие объекты
     самостоятельно: download + multipart-загрузка); в контракт `UploadPart`
     (Заголовки) добавлено: `x-amz-copy-source` игнорируется — запрос с
     ним обрабатывается как обычная загрузка части;
   - `03-protocol.md`: `UploadPartCopy` убран из списка разрешённых
     presigned-операций (§2); XML-образец `CopyPartResult` удалён (§4);
     «не матчатщийся ни на одну из 22 операций» → 21 (§6);
   - `01-overview.md` (§1 диаграмма, §2 состав — multipart (6), §3
     таблица) и входная точка `22-owns3.md` (§границы, §главы) — счёт
     операций 21, `UploadPartCopy` исключён из перечней;
   - `05-access-config.md` §3 — матрица прав без `UploadPartCopy`;
   - `arch/roadmap/owns3.md` (шапка трека) — границы этапа: «21 операция»,
     `UploadPartCopy` исключён из перечня (снятие расхождения
     roadmap-пересказа с каноном).

Ссылки на живой тег t39 в `22-owns3.md` (входная точка, «после приёмки
t39»), `01` §1 и `03` §3 корректны до мержа; мерж-коммит t39 удаляет их
(правило чистки упоминаний слитой задачи).

## 4. Структура и компоненты

### 4.1. Docker-образ `docker/OwnS3.Dockerfile`

По прецеденту `docker/Metrics.SdGenerator.Dockerfile` (runtime-слой +
`ARG`-путь publish-вывода; два режима сборки одним файлом):

```dockerfile
# syntax=docker/dockerfile:1
# Образ ownS3 (arch/owns3/01 §5, 05 §6): dotnet publish НА ХОСТЕ, в контейнер —
# ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников; канон E2E-образов).
# CLI (узкий контекст — каталог артефактов):
#   dotnet publish src/OwnS3.App/OwnS3.App.csproj -c Release -o artifacts/e2e/owns3
#   docker build -f docker/OwnS3.Dockerfile -t owns3:e2e artifacts/e2e/owns3
# Compose (контекст — корень репо, ARG указывает путь вывода):
#   build: { context: .., dockerfile: docker/OwnS3.Dockerfile,
#            args: { OWNS3_PUBLISH_DIR: artifacts/owns3/publish } }
ARG OWNS3_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG OWNS3_PUBLISH_DIR
WORKDIR /app
COPY ${OWNS3_PUBLISH_DIR} ./
# Порт S3-грани — OwnS3:Server:Port (дефолт 9000; HTTP-only, path-style).
EXPOSE 9000
ENTRYPOINT ["dotnet", "OwnS3.App.dll"]
```

- `ASPNETCORE_HTTP_PORTS` не задаётся: Program сам слушает
  `OwnS3:Server:Port` (9000) через `app.Urls`.
- HEALTHCHECK в образе не добавляется: `/healthz` не требует подписи, но
  curl в aspnet-образе отсутствует; готовность в стендовых чеках и E2E —
  внешними пробами (wait-стратегии).
- Теги: `owns3:dev` — стенд (compose), `owns3:e2e` — E2E-серия. В registry
  не кладутся (локально собираемые).

### 4.2. Dev-стенд-сервис

**`deploy/docker-compose.yml`** — сервис рядом с воркерами (env-секреты
per-install из `deploy/.env` — тот же механизм, канон 01 §5):

```yaml
  # ownS3 (arch/owns3/01, 05 §6): целевое S3-хранилище бэкапов; после
  # перевода клиентов (t41) вытесняет as-minio. До того — живёт в контуре
  # стенда параллельно (бэкапы остаются на as-minio). standalone-сервис:
  # без etcd, без docker.sock; один процесс, один том.
  owns3:
    build:
      context: ..
      dockerfile: docker/OwnS3.Dockerfile
      args:
        OWNS3_PUBLISH_DIR: artifacts/owns3/publish
    image: owns3:dev
    restart: unless-stopped
    ports:
      # 9000/9001 хоста заняты as-minio полного стенда — ownS3 продолжает
      # портовой ряд (канон 05 §6); внутри контейнера канонический 9000.
      - "${OWNS3_HOST_PORT:-9010}:9000"
    environment:
      OWNS3_ROOT_USER: ${OWNS3_ROOT_USER:?задайте секрет}
      OWNS3_ROOT_PASSWORD: ${OWNS3_ROOT_PASSWORD:?задайте секрет}
    volumes:
      - owns3-data:/data
# volumes: + owns3-data
```

**`deploy/.env.example`** — секция ownS3 (dev-значения по образцу секций
воркеров; prod — per-install из `deploy/.env`, в git не попадает):

```
# ownS3 (arch/owns3/05): root-пара обязательна (fail-fast старта);
# статические ключи — опционально (пусто = только root).
OWNS3_ROOT_USER=owns3-local-root
OWNS3_ROOT_PASSWORD=owns3-local-root-pwd
#OWNS3_HOST_PORT=9010
#OWNS3_ACCESS_KEYS__0__ACCESSKEY=stands-writer
#OWNS3_ACCESS_KEYS__0__SECRETKEY=…
#OWNS3_ACCESS_KEYS__0__POLICY=read-write
```

**`dev-stand/adminpanel/checks/00-up.sh`** — перед подъёмом deploy-контура
(там, где publish SdGenerator) добавляется publish ownS3 с `[PHASE]`-метками
и таймингом:

```bash
echo ">>> [PHASE] publish OwnS3.App ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/OwnS3.App/OwnS3.App.csproj" \
  -c Release -o "$ROOT/artifacts/owns3/publish" --nologo \
  || { echo "❌ publish OwnS3 не удался"; exit 1; }
echo ">>> [PHASE] publish OwnS3 готов ($(date +%H:%M:%S))"
```

и `owns3` включается в `docker compose ... up -d` deploy-проекта (образ
строится compose-билдом из артефактов); готовность — healthz-проба (бюджет
60 с, полл 2 с): `curl -sf http://localhost:${OWNS3_HOST_PORT:-9010}/healthz`.

**`dev-stand/adminpanel/checks/46-owns3.sh`** — smoke-чек стендового ownS3
(порт из `deploy/.env` с дефолтом 9010):

1. `curl /healthz` → 200;
2. `curl /metrics` → содержит `ownS3_requests_total{` и
   `ownS3_disk_total_bytes`;
3. `mc`-smoke через хост-публикацию (элемент прецедента `00-up.sh` —
   ephemeral `docker run`): `alias set → mb os3check-<ts> → cp → ls → cat
   (сверка байтов) → stat → rm → rb` c root-кредами из `deploy/.env`;
   временное имя бакета уникально (ts-суффикс), чистка — в конце скрипта
   (`set -e` + trap: `mc rb --force` при любом исходе);
4. финальная строка-итог для журнала чека.

Стендовые бэкапы остаются на `as-minio`: чек не трогает `PGW_BACKUP_S3_*`
и бакет `pgworker-backups`.

### 4.3. E2E-окружение `OwnS3E2eEnvironment`

`src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eEnvironment.cs` — по образцу
`KafkaE2eEnvironment` (компактный standalone-случай: контейнер сервиса в
сети, без etcd/воркеров):

- **Идентификатор**: `runId = Guid.NewGuid().ToString("N")`; имена:
  сеть `os3-en-{runId}`, контейнер `os3-es-{runId}`, том `os3-vd-{runId}`
  (кодировка «префикс + guid» — читаемость в `docker ps` и точный
  опознавательный признак; `os3` — трёхсимвольный префикс по прецедентам
  `pgw`/`kfw`/`vwk`).
- **Артефакты телеметрии**: `/tmp/pgw-e2e-artifacts-{runId}/` (единый
  префикс проекта; канон `docs/e2e-launch.md`).
- **Статическая сборка образа** (раз на процесс, семафор; прецедент
  `E2eEnvironment`): `dotnet publish src/OwnS3.App -c Release -o
  artifacts/e2e/owns3` с `NoMsBuildReuseEnv`-env, `[PHASE]`-метками до/после,
  бюджетом 10 мин и полным логом в `process-build-*.log`; затем
  `docker build -f docker/OwnS3.Dockerfile -t owns3:e2e artifacts/e2e/owns3`
  (CLI-режим, узкий контекст; `[PHASE]`-метка, бюджет 120 с). Гейт
  `PGW_TEST_E2E_NOBUILD=1` пропускает сборку (бисект).
- **Подъём** (`StartAsync(slug)`; гейт `PGW_TEST_DOCKER=1` — иначе
  `Assert.Skip` в тестах, как `DockerTrait`): создание сети testcontainers →
  named-volume `os3-vd-{runId}` → контейнер `os3-es-{runId}`:
  `WithNetwork`, `WithPortBinding(зонд свободного порта, 9000)` (динамический
  хост-порт; зонд `TcpListener :0` — хелпер окружения, прецедент
  `E2eFixture.FreePort`), `WithVolume(volume, "/data")`, env:
  `OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD` (литералы e2e-секретов), три
  статических ключа (reader/writer/admin — литералы, для негативных
  клиентских кейсов), `OWNS3_HOST_ID=os3-e2e-{runId[..8]}`. Wait-стратегия:
  `UntilHttpRequestIsSucceeded` GET `/healthz` (внутренний 9000, бюджет
  ≤ 30 с — канон «поднятие инстанса ≤ 30 с», никаких sleep).
- **Retry старта** — 3 попытки на распознанные гонки (`network … not found`,
  портовый bind-фейл), подъём заново ЦЕЛИКОМ (полное удаление попытки).
- **Телеметрия teardown**: `docker logs`/`docker inspect` контейнера — ДО
  любых удалений (`CollectDiagnosticsAsync(reason)`; причины
  `failed`/`teardown`/`slow-phase-*`).
- **`MarkFailed()`** — teardown ОСТАНАВЛИВАЕТ контейнер, но не удаляет
  его/том/сеть; команды зачистки — в `README-cleanup.txt` (own-only по
  guid). Ассерт чистоты в stop-режиме не применяется. Обёртка
  `RunScenarioAsync` (try/catch → `MarkFailed` → rethrow) — как у PgWorker.
- **DisposeAsync** (при любом исходе): телеметрия → dispose контейнера →
  `docker volume rm -f os3-vd-{runId}` с ретраем (демон освобождает
  асинхронно; бюджет ~30 с) → удаление сети → **ассерт чистоты**: по
  фильтрам `name={runId}` в `docker ps -a` / `docker volume ls` /
  `docker network ls` не осталось ни одного объекта — иначе тест падает
  («teardown окружения неполный»). Чужие объекты не трогаются (own-only).
- **Публичный API окружения**: `S3BaseUrl` (`http://localhost:{порт}` —
  для SDK/HttpClient), `S3InternalUrl` (`http://os3-es-{runId}:9000` — для
  mc изнутри сети), `NetName` (docker run --network), креды литералов,
  `ArtifactsDir`, `[PHASE]`-хелперы (метки фаз в Console + `phases.log` в
  ArtifactsDir с UTC-метками; сбор docker-диагностики при фазе > 60 с).

### 4.4. Клиенты тестов

**AWS SDK .NET** (`AWSSDK.S3` 3.7.511.8 — версия уже централизована в
`Directory.Packages.props`; добавляется `PackageReference` в
`OwnS3.IntegrationTests.csproj`; там же `Testcontainers` 4.14.0):

- Фабрика `OwnS3E2eClients.CreateS3(env, роль)`: `AmazonS3Config`
  { `ServiceURL = env.S3BaseUrl`, `ForcePathStyle = true`,
  `AuthenticationRegion = "us-east-1"`, `Timeout = 10 s` } +
  `BasicAWSCredentials` (root/writer/reader/admin — литералы окружения).
- **Flexible checksums** (канон 03 §3): настройка per-client —
  `AmazonS3Config.RequestChecksumCalculation` (enum
  `Amazon.Runtime.RequestChecksumCalculation`: `WHEN_REQUIRED`/`WHEN_SUPPORTED`;
  глобальной статики `AWSConfigs.RequestChecksumCalculation` в AWSSDK.Core
  3.7.501.1 нет — проверено по пакету). Фабрика принимает режим: основные
  Facts создают клиента в `WHEN_REQUIRED` (режим, в который обязан быть
  настроен клиент бэкапов: без него свежий SDK шлёт неподдерживаемый
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER` → 400), отдельный Fact — в
  `WHEN_SUPPORTED` (SDK сам использует чанковый режим с подписанными
  трейлерами). E2E-классы последовательны одной коллекцией
  (консервативная мера; глобального состояния серия не имеет). In-memory
  `Api`-сценарии AWS SDK не используют.
- Сверка ETag: тест сам считает MD5 тела (простой PUT) и составной
  `<N>-<md5(concat hex-ETag'ов)>` (multipart) — по формулам канона 02 §1.

**`mc`** — ephemeral `docker run --rm --network {NetName}
minio/mc:RELEASE.2025-08-13T08-35-41Z` (тег уже в `images.txt` и запинен
в E2eEnvironment PgWorker) с `sh -c`-скриптом: `mc alias set t
{S3InternalUrl} <root> … && mc …` (прецедент `E2eBackupJobsScenarios`).
Вывод mc — в диагностику ассерта при падении; полезный stdout парсится
(ls/stat) с допуском на формат.

### 4.5. Сценарные классы (состав E2E-серии)

Все — `E2e/{Name}.cs`, `[Collection(OwnS3E2eCollection)]`, каждый Fact:
`await using var fx = await OwnS3E2eEnvironment.StartAsync(slug); try {
… } catch { await fx.MarkFailed(); throw; }` — через общую обёртку
`RunScenarioAsync` (канон MarkFailed). Бакеты имён `os3e2e{тег}{слоган}` —
уникальны per-окружение (правила имени 3–63, строчные). Данные —
детерминированные байты (seed), размеры малые (КиБ–МиБ) — серия быстрая.

**Класс 1: `AwsSdkSimpleScenarios`** (AWS SDK, простые операции — буква
«простые PUT/GET, conditional, Range»):

1. `Buckets_Lifecycle_ByAdmin`: CreateBucket → HeadBucket 200 →
   ListBuckets содержит → GetBucketLocation (`""` = us-east-1) → повторный
   CreateBucket → `BucketAlreadyOwnedByYou` 409 → DeleteBucket пустого →
   204; DeleteBucket несуществующего → `NoSuchBucket` 404.
2. `PutGet_Roundtrip_Metadata_And_Etag`: PUT (256 КиБ, ключ с UTF-8
   спецсимволами и слэшами `backups/os3-e2e/файл+1.dat`,
   `Content-Type`, 2×`x-amz-meta-*`) → 200; GET: байты равны, ETag =
   MD5, Content-Type/metadata возвращены; HEAD: Size/ETag/LastModified;
   ListObjectsV2 видит ключ.
3. `Conditional_Requests`: If-Match верный → 200; If-Match чужой → 412
   `PreconditionFailed`; If-None-Match верный → AmazonS3Exception 304;
   If-Modified-Since (будущее) → 304; If-Unmodified-Since (прошлое) → 412.
4. `Range_Requests`: bytes=10-20 → 206 + срез тела; открытый `bytes=N-`;
   суффикс `-5`; end за размером → 206 с обрезкой. (`If-Range`-кейсов
   нет: SDK заголовок не выставляет, поддержка снята — §3.3/§4.6.)
5. `CopyObject_Copy`: копия читается (байты/ETag наследуются), REPLACE
   меняет Content-Type/metadata; DeleteObject → 204; GET → `NoSuchKey`
   404; повторный DeleteObject → 204 (идемпотентность).
6. `DeleteObjects_Batch_And_Listings`: 12 ключей `list/prefix{i}/obj{j}`
   → DeleteObjects (3 ключа) → DeleteResult перечисляет; ListObjectsV2 с
   delimiter → `CommonPrefixes`; пагинация max-keys=5 до исчерпания
   (IsTruncated/NextContinuationToken); GetObjectAttributes →
   ETag/ObjectSize/StorageClass.
7. `ChecksumTrailer_PutGet_WhenSupported`: клиент фабрики в режиме
   `WHEN_SUPPORTED` → PUT с
   вычисляемыми SDK чексуммами (трейлерный чанковый режим) → 200; GET →
   байты равны; ETag = MD5 (трейлеры не меняют ETag-правило). Приёмка
   канона 03 §3 реальным клиентом.
8. `ReaderRole_Denied_On_Write`: reader-ключ: PutObject → 403
   `AccessDenied`; GetObject после writer-PUT → 200 (матрица 05 §3 —
   минимальная E2E-проверка, что статические ключи работают в докер-контуре).

**Класс 2: `AwsSdkMultipartScenarios`** (multipart — буква roadmap;
`UploadPartCopy`-кейсов нет — SDK API не содержит, операция снята
§3.4/§4.6):

1. `Multipart_FullCycle`: Initiate → UploadPart×3 (2×5 МиБ + 1 МиБ хвост —
   лимит 5 МиБ не-последних) → ListParts (3 записи, ETag/Size) → Complete
   (составной ETag `3-md5(concat)` — сверка формулой) → GET: байты =
   конкатенация частей; HEAD Size; GetObjectAttributes `ObjectParts` —
   реальные 3 части.
2. `Multipart_Range_Over_Part_Boundaries`: bytes через стык частей → 206,
   срез по границам (механика t38 — реальным клиентом).
3. `Multipart_Abort_Semantics`: Initiate+Part → Abort → 204; повторный
   Abort → `NoSuchUpload` 404; Complete после Abort → `NoSuchUpload`;
   ListParts после Abort → `NoSuchUpload`.
4. `Multipart_PartReupload_LastWriterWins`: повторная загрузка того же
   partNumber с другим телом → Complete собирает последнюю версию части.
5. `DeleteBucket_BlockedByLiveUpload`: живая загрузка → DeleteBucket →
   409 `BucketNotEmpty`; после Abort → 204 (гейт t38).

**Класс 3: `McScenarios`** (mc — второй реальный клиент, админ-операции):

1. `Mc_Bucket_Object_Lifecycle`: `mc alias set` (root) → `mc mb` →
   `mc cp` (локальный временный файл 128 КиБ внутрь контейнера mc:
   `head -c` /dev/urandom) → `mc ls` видит → `mc cat | sha256` == источник
   → `mc stat` (Size/ETag) → `mc cp` скачивание → `mc rm` → `mc ls` пуст →
   `mc rb`.
2. `Mc_Recursive_Listing`: `mc cp` набора ключей с префиксами →
   `mc ls --recursive` перечисляет все; `mc ls <bucket>/prefix` — фильтр
   по префиксу (путь листинга цепочек бэкапов).
3. `Mc_Alias_Region_Protocol`: `mc alias set` с `--api S3v4`; фиксация, что
   mc (SigV4-клиент с собственной канонизацией) проходит
   заголовочную подпись ownS3 (сквозная проверка подписи вторым
   независимым клиентом).

**Класс 4: `HealthMetricsScenarios`** (health/метрики — буква roadmap):

1. `Healthz_200_NoSignature`: plain HttpClient GET `/healthz` → 200
   (доступность без аутентификации; том жив).
2. `Metrics_Prometheus_Series_AfterTraffic`: SDK-серия запросов (PUT, GET,
   LIST) → GET `/metrics` (plain HttpClient): текст содержит
   `ownS3_requests_total{operation="PutObject",code="200"}` (и для
   GetObject/ListObjectsV2), `ownS3_request_duration_seconds_bucket`,
   `ownS3_disk_used_bytes` > 0 и `ownS3_disk_total_bytes` ≥ used
   (Prometheus-экспозиция канона 05 §5 в докер-контуре).

### 4.6. Зачистка `If-Range` и `UploadPartCopy` в коде (расширение scope — решения пользователя Q1/Q2)

Канон уже поправлен (§3.3–3.4): обе грани не поддерживаются; остаточные
запросы/заголовки игнорируются (`If-Range` — ответ только по `Range`;
`x-amz-copy-source` на part-endpoint — обработка как обычный `UploadPart`).
Отражение в коде — полное изъятие механики (не «оставить мёртвым»).

**If-Range**:

| Файл | Правка |
|---|---|
| `src/OwnS3.Storage/Domain/DomainTypes.cs` | `ObjectReadOptions` — убрать поле `IfRange` (запись `(Conditions, Range)`); все места конструирования |
| `src/OwnS3.Storage/RangeResolver.cs` | `Resolve(ByteRange?, string? ifRange, string etag, long size)` → `Resolve(ByteRange?, long size)`: удалить ветку ifRange и зависимость от `ConditionalEvaluator` внутри резолва; комментарий шапки — без If-Range |
| `src/OwnS3.Storage/XlObjectStore.Objects.cs` | два вызова `RangeResolver.Resolve` (Get/Head-пути, строки ~226/250) — новая сигнатура |
| `src/OwnS3.App/Handlers/ObjectHandlers.cs` | убрать чтение заголовка `If-Range` (`Headers["If-Range"]`, ~строка 336) и передачу в `ObjectReadOptions`; комментарий опций чтения — без If-Range |
| `src/tests/OwnS3.UnitTests/Storage/RangeResolverTests.cs` | удалить If-Range-кейсы (совпал/не совпал/дата-форма); диапазонные кейсы остаются |
| `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreObjectTests.cs` | удалить If-Range-кейсы простых чтений |
| `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` | удалить If-Range-кейсы чтений multipart-объектов |
| `src/tests/OwnS3.IntegrationTests/Api/ObjectStorageScenarios.cs` | удалить If-Range HTTP-сценарии (206-при-совпадении / 200-полный-при-несовпадении); Range-сценарии остаются |

**UploadPartCopy** (серверная операция t38 + покрытие):

| Файл | Правка |
|---|---|
| `src/OwnS3.App/Routing/S3Operation.cs` | убрать значение enum `UploadPartCopy` |
| `src/OwnS3.App/Routing/S3Router.cs` | убрать маршрут (дискриминатор `x-amz-copy-source` на `PUT ?partNumber&uploadId`): после снятия запрос матчится как `UploadPart`, заголовок игнорируется (канон 02) |
| `src/OwnS3.App/Handlers/MultipartHandlers.cs` | удалить `UploadPartCopyHandler` (+ `IOperationHandler`-обвязка, `CopyPartResult`-ответ) |
| `src/OwnS3.App/Handlers/OperationValidation.cs` | убрать валидацию заголовков операции (`x-amz-copy-source`/`-range`/copy-условия на part-endpoint) |
| `src/OwnS3.App/Access/OperationAccessMatrix.cs` | убрать ячейку `UploadPartCopy` (строка матрицы — `Create/UploadPart/Complete/Abort`) |
| `src/OwnS3.App/Program.cs` | убрать DI-регистрации `UploadPartCopyHandler` (обе) и `IOperationHandler`; комментарий «22 хендлера протокольного контура» → «21 хендлер» |
| `src/OwnS3.Protocol/Auth/S3PresignedOperations.cs` | убрать `UploadPartCopy` из presigned-разрешённых (набор — канон 03 §2) |
| `src/OwnS3.Protocol/Xml/S3XmlModels.cs` | удалить тип `CopyPartResult` (~строки 252–253; XML-схема изъята из канона 03 §4) |
| `src/OwnS3.Storage/IObjectStore.cs` | убрать `UploadPartCopyAsync` из контракта домена |
| `src/OwnS3.Storage/XlObjectStore.Multipart.cs` | удалить реализацию: срез источника по границам частей, copy-условия, `InvalidArgument`/`EntityTooLarge`/412-ветки |
| `src/OwnS3.Storage/MultipartJournals.cs` | убрать вспомогательную механику UploadPartCopy |
| `src/tests/OwnS3.UnitTests/Storage/XlObjectStoreMultipartTests.cs` | удалить UploadPartCopy-кейсы (весь объект/диапазон/`InvalidArgument`/`EntityTooLarge`/412/multipart-источник — фаза 5 t38) |
| `src/tests/OwnS3.UnitTests/S3RouterTests.cs` | удалить маршрутный кейс UploadPartCopy |
| `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` | удалить валидационный кейс операции |
| `src/tests/OwnS3.UnitTests/OperationAccessMatrixTests.cs` | актуализировать ожидаемую матрицу (без UploadPartCopy) |
| `src/tests/OwnS3.UnitTests/S3XmlTests.cs` | удалить `Serialize_CopyPartResult_MatchesSample` и кейсы типа |
| `src/tests/OwnS3.IntegrationTests/Api/MultipartScenarios.cs` | удалить HTTP-сценарии UploadPartCopy |
| `src/tests/OwnS3.IntegrationTests/Api/RoutingScenarios.cs` | удалить роутинг-кейс UploadPartCopy |

- Поведение при остаточных запросах: заголовки/запросы снятых граней не
  читаются вовсе — ответы идут по канонным правилам оставшихся операций;
  отдельные тесты «игнорирования» не нужны (механики нет — проверять
  нечего, отсутствие кода контролирует grep-гейт).
- **Grep-гейт**: после зачистки в `src/OwnS3.*` и `src/tests/OwnS3.*`
  не остаётся ни одного вхождения `IfRange` / `If-Range` /
  `UploadPartCopy` / `CopyPartResult` (регистронезависимо; исключение —
  этот spec/архив задач; в `arch/owns3/` легитимны только канонические
  фиксации снятия в 02).
- `ConditionalEvaluator` и conditional-семантики (If-Match/If-None-Match/
  If-Modified-Since/If-Unmodified-Since), `CopyObject` (его copy-условия
  `x-amz-copy-source-if-*` на объектном endpoint) и прочие multipart-
  операции — без изменений.
- Presigned-Theory `AuthScenarios` (`Api`) при падении от сужения
  разрешённого набора — актуализировать ожидаемый список по канону 03 §2
  (это следствие правки, не расширение объёма).

### 4.7. Изменения в проекте тестов

`src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj`: +
`PackageReference` `AWSSDK.S3`, `Testcontainers` (версии централизованы).
Существующие `Api`-сценарии не трогаются (кроме изъятий §4.6).
`GlobalUsings`/структура — по образцу PgWorker-серий. xunit-коллекция
`OwnS3E2eCollection` (`DisableParallelization=true`) — только для
E2E-классов.

## 5. Фазы реализации (для plan)

0. **Arch-фаза**: контрольная сверка спеки с канонами 01/02/03/05 +
   входной точкой `22-owns3.md` + roadmap-шапкой после правок §3
   (изменения уже внесены).
1. **Зачистка If-Range и UploadPartCopy** (§4.6): правки Protocol/Storage/
   App + тесты; grep-гейт чист; `dotnet build` Release 0 warnings; юниты +
   интеграция OwnS3 зелёные (до всякого docker-контура — быстрая обратная
   связь).
2. **Образ**: `docker/OwnS3.Dockerfile` (§4.1); ручная проверка сборки
   CLI-режимом (publish → build → запуск контейнера с env root-пары →
   healthz) — до интеграции в тесты.
3. **Стенд-сервис**: сервис compose (§4.2), секция `deploy/.env.example`,
   publish-блок в `00-up.sh`, чек `46-owns3.sh`; подъём стенда частично
   (deploy-контур) — чек зелёный; при отсутствии — быстрые правки.
4. **E2E-окружение**: `OwnS3E2eEnvironment` (§4.3) + кейс-маркер
   `Healthz_200_NoSignature` (первый живой тест серии); проверка
   teardown/ассерта чистоты на зелёном и принудительно упавшем прогоне
   (MarkFailed → stop-режим, объекты остаются).
5. **Клиентские классы**: `AwsSdkSimpleScenarios` (фабрика SDK, режимы
   checksum) → `AwsSdkMultipartScenarios` → `McScenarios` →
   `HealthMetricsScenarios` (§4.5; порядок — от простого к составному).
6. **Мерж-гейт**: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings
   (`TreatWarningsAsErrors`); `dotnet test` OwnS3.UnitTests +
   OwnS3.IntegrationTests (без docker) зелёные; grep-гейт §4.6
   (`IfRange|If-Range|UploadPartCopy|CopyPartResult`) чист; docker-E2E
   серия на свежем Release: `DOTNET_CLI_UI_LANGUAGE=en
   PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter
   FullyQualifiedName~OwnS3.IntegrationTests.E2e` — зелёная, после серии
   хост чист (`docker ps -a | grep os3-` пусто); стенд-чек `46-owns3.sh`
   зелёный на полном стенде. Roadmap-гейт: тег `t39` снят тем же
   мерж-коммитом — пункт удалён из `arch/roadmap/owns3.md`, зависимости
   `← t39-owns3-e2e` у t40/t41/t42 очищены; упоминания t39 в
   `arch/22-owns3.md` (входная точка), `arch/owns3/01` §1 и `03` §3
   переписаны в безадресную форму («после приёмочной грани», «при приёмке
   клиентами»).

## 6. Тестовая стратегия

- **Изоляция**: per-class окружение (сеть+контейнер+том, guid в именах);
  никаких общих контуров; E2E-прогоны параллельны с чем угодно — помех
  нет конструктивно. Сами E2E-классы последовательны (одна коллекция —
  последовательный прогон, консервативная мера против глобального состояния SDK; окружения при этом независимы).
- **Детерминированность**: тела фиксированы seed; имена бакетов содержат
  тег окружения; никаких снов — готовность wait-стратегиями, внутренние
  ожидания поллом 0.5 с с конечным бюджетом (подъём ≤ 30 с; весь Fact —
  минуты).
- **Телеметрия**: `[PHASE]` на publish/build/старт/teardown; логи
  контейнера до удалений; упавший прогон оставляет контейнер для разбора;
  перезапуск упавших — только после полного анализа (канон e2e-launch §4).
- **AAA-нотация** в комментариях тестов; ассерты FluentAssertions;
  диагностика в сообщениях ассертов — без обрезок (хвост stdout mc/SDK).
- Новые юниты/интеграция не пишутся (слои готовы; изъятия If-Range/
  UploadPartCopy — удаление кейсов, §4.6). Чек стенда — bash-скрипт
  с чисткой за собой.

## 7. Ограничения и риски

- **Код сервиса** (`src/OwnS3.{Protocol,Storage,App}`): правки ТОЛЬКО
  в части зачистки If-Range/UploadPartCopy (§4.6) — решения пользователя,
  расширившие scope; всё прочее считается готовым (t36–t38). Если E2E
  вскроет иной дефект сервиса — фикс отдельным пунктом плана с
  обоснованием против канона (не «подгонка теста»).
- **Серверная копия частей недоступна после снятия UploadPartCopy**:
  клиенту, которому нужна частичная серверная копия (составление объекта
  из диапазонов другого), остаётся client-side download + UploadPart;
  целевые клиенты (AWS SDK, mc) такой операции не имеют — сценария нет.
  Копия объекта > 5 ГБ (`CopyObject` `EntityTooLarge`) — аналогично
  client-side (канон 02, формулировка обновлена §3.4).
- **Flexible checksums per-client** (`AmazonS3Config.RequestChecksumCalculation`): фабрика задаёт режим на каждом клиенте; env `AWS_REQUEST_CHECKSUM_CALCULATION` (статический кеш SDK) серией не используется — влияние окружения исключено.
- **Формат вывода `mc`** (ls/stat) меняется между версиями — образ запинен
  тегом из `images.txt`; парсинг минимальный (наличие ключа в выводе),
  без хрупких колонок.
- **Порт 9010** — новый хост-порт стенда: коллизии с чужими сервисами
  хоста возможны как у любого стенда (`OWNS3_HOST_PORT` параметризован);
  в тестах порты всегда динамические.
- **Скорость серии**: 4 окружения × (publish no-op + build кэш-хит +
  старт ≤ 30 с + сценарии секунд) — полная серия порядка 1–2 минут
  (образ/бинарь — статические ассеты процесса, собираются раз).
- **`docker volume rm` ретраи**: демон освобождает том асинхронно после
  rm контейнера — бюджет ~30 с с повторами (канон e2e-isolation §3);
  превышение — громкий фейл ассерта чистоты, не тихое продолжение.
- **Игнорирование If-Range экзотическим клиентом** (после снятия):
  клиент, полагающийся на If-Range, получит 206 по Range независимо от
  ETag — поведение ровно как зафиксировано в каноне 02 §1; целевых
  клиентов (AWS SDK, mc) это не касается (заголовок не шлюют).

## 8. Критерии приёмки

1. Docker-E2E серия `OwnS3.IntegrationTests.E2e` (4 класса, §4.5) зелёная
   на свежем Release с `PGW_TEST_DOCKER=1`; автосборка publish+образ на
   каждом прогоне с `[PHASE]`-метками; `PGW_TEST_E2E_NOBUILD=1` работает.
2. AWS SDK .NET как реальный клиент: простые PUT/GET/HEAD/Copy/Delete
   (метаданные, ETag-MD5), conditional (412/304), Range (206/обрезка),
   листинги с пагинацией/CommonPrefixes, GetObjectAttributes,
   полный multipart-цикл (составной ETag по формуле, Range через стыки,
   Abort-семантика NoSuchUpload, перезагрузка части, DeleteBucket-гейт) —
   все сверки проходят против контейнера ownS3.
3. Flexible checksums: PUT/GET в `WHEN_REQUIRED` и в `WHEN_SUPPORTED`
   (трейлерный чанковый режим реальным SDK) — оба зелёные (канон 03 §3).
4. `mc` как второй клиент: alias/mb/cp/cat/stat/ls --recursive/rm/rb
   против ownS3 — зелёные; roundtrip байтов сходится (sha256).
5. `/healthz` 200 без подписи; `/metrics` несёт серии
   `ownS3_requests_total{operation,code}` (растут по факту запросов),
   `ownS3_request_duration_seconds{operation}`, `ownS3_disk_used_bytes`/
   `ownS3_disk_total_bytes` — в докер-контуре E2E.
6. Изоляция/зачистка: после зелёной серии — ни одного `os3-*`
   контейнера/тома/сети (ассерт чистоты в каждом teardown); упавший
   прогон оставляет контейнер остановленным (MarkFailed), команды чистки
   в `README-cleanup.txt`; телеметрия (docker-логи, phases.log) в
   `/tmp/pgw-e2e-artifacts-{guid}/`.
7. Dev-стенд: сервис `owns3` поднимается `00-up.sh` полного стенда
   (publish с меткой + compose build); хост-порт 9010 (`OWNS3_HOST_PORT`
   параметризован); стендовые бэкапы остались на as-minio; чек
   `46-owns3.sh` зелёный (healthz, metrics, mc-smoke с самоочисткой).
8. Образ `docker/OwnS3.Dockerfile`: publish-COPY без исходников и sdk;
   CLI- и compose-режимы сборки работают; в registry не кладётся; новых
   строк `images.txt` нет.
9. **If-Range и UploadPartCopy сняты полностью**: канон — 02 §1
   (If-Range игнорируется) и 02 §5 (контракт UploadPartCopy удалён,
   UploadPart игнорирует `x-amz-copy-source`), счёт операций 21 в
   01/02/03/22-входной точке/roadmap-шапке; presigned-набор (03 §2) и
   XML-схемы (03 §4, `CopyPartResult`) без UploadPartCopy; в `src/OwnS3.*`
   и `src/tests/OwnS3.*` ни одного вхождения `IfRange`/`If-Range`/
   `UploadPartCopy`/`CopyPartResult` (grep-гейт, регистронезависимо);
   `ConditionalEvaluator`/conditional-семантики, `CopyObject` и прочие
   multipart-операции не регрессируют — юниты и интеграция OwnS3 зелёные.
10. `deploy/.env.example` содержит секцию `OWNS3_*` (root-пара
    dev-значениями, закомментированные ключи/порт); `deploy/.env` — в
    gitignore как прежде.
11. Канон `arch/owns3/05` §6 соответствует реализованному (модель
    запуска); упоминания t39 в `arch/22-owns3.md`, `arch/owns3/01` §1,
    `03` §3 переписаны; roadmap-пункт t39 удалён, зависимости t40/t41/t42
    очищены (мерж-гейт).
12. Сборка Release чистая (0 warnings), юниты + интеграция OwnS3 зелёные
    (без docker), существующие сценарии t36–t38 не регрессируют
    (кроме изъятых If-Range/UploadPartCopy-кейсов §4.6).

## 9. Ключевые решения

1. **Q1 (решение пользователя на ревью спеки): снять `If-Range` с ownS3
   полностью.** Факт: AWSSDK.S3 3.7.511.8 не позволяет выставить заголовок
   `If-Range` — в `GetObjectRequest` такого свойства нет; `mc` заголовок
   тоже не отправляет. Поддержка недостижима ни одним целевым клиентом →
   мёртвый код изымается: канон 02 §1 (заголовок игнорируется, ответ —
   только по `Range`), механика Storage/App и все If-Range-кейсы тестов
   (§3.3, §4.6). Альтернатива «оставить поддержку, не проверяя E2E»
   отвергнута решением пользователя; отдельный тест «игнорирования» не
   заводится — механики нет, отсутствие контролирует grep-гейт.
2. **Q2 (решение пользователя на ревью спеки): снять серверную операцию
   `UploadPartCopy` с ownS3 полностью — принцип «всё, что не поддерживает
   AWSSDK.S3, убирать из кода в рамках текущей задачи; код — согласно
   описанию AWSSDK.S3».** Факт: AWSSDK.S3 3.7.511.8 не содержит API
   `UploadPartCopy` — типа `UploadPartCopyRequest` и метода
   `UploadPartCopyAsync` в пакете нет (компиляционная проба CS0246;
   XML-дока упоминает операцию только текстами ссылок); `mc` операцию не
   использует. Серверная реализация t38 и её тестовое покрытие изымаются
   целиком (§3.4, §4.6): контракт канона 02 §5, presigned-набор 03 §2,
   XML-тип `CopyPartResult`, маршрут/матрица/хендлер/домен
   (`IObjectStore.UploadPartCopyAsync`) и кейсы юнитов/интеграции.
   Набор операций — 21. Поведение на остаточный запрос (`PUT
   ?partNumber&uploadId` + `x-amz-copy-source`): маршрутизация как
   `UploadPart`, заголовок игнорируется (канон 02). Альтернатива
   «оставить операцию серверу без клиента» отвергнута решением
   пользователя (мёртвая грань — источник регрессий без потребителя).
3. **Оба режима flexible checksums в приёмке** (`WHEN_REQUIRED` +
   `WHEN_SUPPORTED`): канон 03 §3 явно возлагает на t39 учёт настройки
   клиентов; ownS3 поддерживает оба режима — приёмочная грань обязана
   доказать оба реальным SDK (иначе будущий клиент с дефолтом SDK
   (`WHEN_SUPPORTED`-ветка трейлеров) сломается необнаруженно). Дефолт SDK
   = `WHEN_SUPPORTED` — это же фиксирует факт для t41: `BackupS3`
   (PgWorker) при переводе на ownS3 потребует явной настройки.
4. **Стенд-сервис — в `deploy/docker-compose.yml`**, не в
   dev-stand-компоузе: ownS3 — сервис поставки (как воркеры), env-секреты
   из `deploy/.env` — по букве канона 01 §5 («env-файл по образцу
   deploy-воркеров»); dev-stand-компоуз — контур etcd/панели/MinIO.
5. **Хост-порт стенда 9010**: 9000/9001 заняты `as-minio` полного стенда
   (S3 API и консоль); ownS3 продолжает портовой ряд (дефолт
   `OWNS3_HOST_PORT`, параметризован против коллизий).
6. **Один Dockerfile, два режима сборки** (`ARG OWNS3_PUBLISH_DIR`) —
   прецедент `Metrics.SdGenerator.Dockerfile` (BuildKit-песочница Docker
   Desktop не читает dockerfile вне контекста — compose требует контекст
   корня; CLI берёт узкий контекст артефактов).
7. **Гейт `PGW_TEST_DOCKER=1`** — единая env docker-серий монорепо
   (прецеденты PgWorker/KafkaWorker), а не новая переменная.
8. **E2E-классы последовательны одной коллекцией** — единственное
   нефункциональное ограничение: настройка checksum — per-client
   (`AmazonS3Config.RequestChecksumCalculation`; глобальной статики в
   AWSSDK.Core 3.7.501.1 нет), последовательность — консервативная мера;
   изоляция окружений полная (per-class).
9. **Персистентность тома через рестарт и сценарий «PgWorker-бэкап против
   ownS3» — не-цели** (буква roadmap-пункта; второе — t41). E2E-прогон
   полной серии — это и есть мерж-гейт (серия маленькая; отдельный
   кейс-маркер не выделяется, маркер — имя каталога `E2e`).

## 10. Ссылки

- Roadmap: `arch/roadmap/owns3.md` (t39; последующие t40/t41/t42 ← t39).
- Каноны: `arch/22-owns3.md` (входная точка), `arch/owns3/01-overview.md`
  (§5 образ/env; счёт операций), `02-operations.md` (§1 Range — правка
  If-Range; §5 multipart — контракт UploadPartCopy удалён, правка
  UploadPart/CopyObject), `03-protocol.md` (§2 presigned-набор, §3 чанковая
  подпись/требование к клиентам, §4 XML-схемы — правки; §6), `04-storage.md`,
  `05-access-config.md` (§3 матрица — правка; §5 health/метрики; §6 запуск
  в докере — с правкой).
- E2E-каноны: `AGENTS.base.md` §13, `docs/e2e-isolation.md`,
  `docs/e2e-launch.md`.
- Прецеденты: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`
  (телеметрия/teardown/ассерт чистоты, статическая сборка),
  `src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eEnvironment.cs`
  (контейнер сервиса в сети — ближайшая модель ownS3),
  `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupJobsScenarios.cs`
  (mc через docker run в сети), `docker/Metrics.SdGenerator.Dockerfile`
  (ARG-образ), `dev-stand/adminpanel/checks/00-up.sh` (publish → compose).
- Контекст t36–t38: `docs/superpowers/2026-10-10-t38-owns3-multipart/spec.md`
  и одноимённый plan; интеграционные сценарии
  `src/tests/OwnS3.IntegrationTests/Api/`.
