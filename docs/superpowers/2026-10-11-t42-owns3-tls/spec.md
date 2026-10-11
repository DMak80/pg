# t42-owns3-tls: TLS-опция S3-грани ownS3

Спецификация roadmap-задачи `t42-owns3-tls`
([arch/roadmap/owns3.md](../../../arch/roadmap/owns3.md)): TLS-опция
S3-грани — per-install сертификат, режим HTTP-only для стенда, доверие CA
у клиентов (SDK бэкапов, `mc`).

Канон: [arch/owns3/03-protocol.md §6](../../../arch/owns3/03-protocol.md)
(транспорт), [arch/owns3/05-access-config.md](../../../arch/owns3/05-access-config.md)
(env/докер), [arch/19-backups.md §7](../../../arch/19-backups.md)
(доверие CA подсистемы бэкапов). Контракт в `arch/` обновлён этой задачей
до написания кода (arch-first); спека описывает отражение контракта в коде.

## 1. Цель

ownS3 умеет принимать S3-трафик по HTTPS с серверным сертификатом
per-install, а подсистема бэкапов (AWS SDK .NET воркера/WAL-приёмника,
`mc` в джобах) умеет доверять per-install CA при https-endpoint. Стенд
остаётся HTTP-only: TLS — опция прод-инсталляции, включаемая env, без
изменения поведения инсталляций без сертификата.

Не-цели (вне задачи):

- mTLS / клиентские сертификаты на S3-грани — аутентификация остаётся
  SigV4 (канон 03 §6).
- Virtual-host style — не поддерживается и в TLS-режиме (path-style only).
- Отдельный TLS-порт, одновременный HTTP+HTTPS, HTTP→HTTPS-редиректы —
  один порт, один режим.
- Стендовый TLS-сервис ownS3 (профиль compose) — не поднимается; стенд
  живёт в HTTP-режиме, TLS-грань проверяется E2E-серией.
- Переключение бэкапов стенда на ownS3 — задача `t41-owns3-minio-migration`.

## 2. Принципы

1. **Буква постановки**: TLS — опция; режим по умолчанию (и режим стенда) —
   HTTP. Инсталляция без `OWNS3_TLS_*` не меняет поведение ни на бит.
2. **Один порт, один режим**: `OWNS3_SERVER_PORT` слушает HTTP либо
   HTTPS-only — по наличию материала серта. Все эндпоинты (21 операция,
   `/healthz`, `/metrics`, админ-грань) — в обоих режимах на одном порту.
3. **Per-install сертификат**: серт `owns3` выпускается из единой CA
   установки `kfw-install-ca` скриптом `deploy/tls/gen.sh` — тот же пакет,
   что обслуживает TLS API воркеров (t03); отдельных CA/PKI не заводится.
4. **Fail-fast**: невалидный/неполный материал серта (задан один из пары,
   путь не читается, PEM не парсится) — отказ старта с диагностикой в лог,
   как у root-пары (канон 05 §1). Сервис не поднимается «молча на HTTP».
5. **Клиенты доверяют CA штатными механизмами**: AWS SDK .NET — env
   `AWS_CA_BUNDLE` (стандартный механизм SDK, поддержка в версии проекта
   3.7.511.8); `mc` (Go) — env `SSL_CERT_FILE`. Код `BackupS3`
   (S3-протокол) не меняется.
6. **Multi-host**: джобы/агенты бэкапов живут на любых docker-хостах —
   файл CA с хоста воркера в их контейнеры не прокидывается; CA передаётся
   значением PEM через env `S3_CA_PEM` и материализуется в файл внутри
   контейнера.
7. **Простота/YAGNI**: ротация CA — вручную существующим порядком пакета
   (rm ca.* + перезапуск gen.sh); автоматической ротации не заводим.

## 3. Структура и компоненты

### 3.1. ownS3-грань (src/OwnS3.App)

- **Конфиг**: секция `OwnS3:Tls` (`ServerCertPem`/`ServerKeyPem` —
  PEM-значения, `ServerCertPath`/`ServerKeyPath` — пути к файлам) — поля в
  `OwnS3Options`; env-транляция в Program.cs по образцу существующих
  (`OWNS3_TLS_CERT`, `OWNS3_TLS_KEY`, `OWNS3_TLS_CERT_PATH`,
  `OWNS3_TLS_KEY_PATH` — канон 05 §4).
- **Загрузка материала**: `Shared.Tls.TlsMaterial` (общая с воркерами
  монорепо): PEM-пара или чтение `_PATH`-файлов; результат —
  `X509Certificate2` для Kestrel. Задан только один из пары / файл не
  читается / невалидный PEM → исключение → Critical-лог + `Environment.Exit(1)`
  до старта прослушивания (место — блок fail-fast Program.cs рядом с
  валидацией режима/тома).
- **Kestrel**: при заданном материале — `https://*:{port}` вместо
  `http://*:{port}` (тот же `OwnS3:Server:Port`), `UseHttps(cert)`;
  `ClientCertificateMode` не трогаем (не запрашивается). HTTP/2 за TLS —
  штатный ALPN Kestrel, ничего не настраиваем.
- **Протокол не меняется**: SigV4 (canonical request не зависит от схемы),
  роутинг, ошибки, `/healthz`, `/metrics`, админ-грань — как в HTTP-режиме.

### 3.2. Per-install TLS-пакет (deploy/tls/gen.sh)

- Выпуск серверного серта `owns3.crt`/`owns3.key` (serverAuth) из
  `kfw-install-ca`, SAN: `DNS:owns3,DNS:owns3-ec,DNS:localhost,
  DNS:host.docker.internal,IP:127.0.0.1` — имена, по которым клиенты
  адресуют грань (compose-сервисы стенда/прода, хост-публикация, локальные
  клиенты).
- Идемпотентность по образцу существующих сертов: выпущен и SAN покрывает
  `DNS:owns3-ec` — не трогаем; отсутствует или SAN неполный (пакет старого
  выпуска) — перегенерация (CA жив).
- Финальный `echo` дополнить `owns3.*`.

### 3.3. Стенд/деплой (deploy/)

- `deploy/docker-compose.yml`: сервисы `owns3`/`owns3-ec` НЕ меняются
  (стенд HTTP-only); у сервиса `owns3` — комментарий-инструкция
  прод-включения TLS (ro-монтирование каталога TLS-пакета + env
  `OWNS3_TLS_CERT_PATH`/`_KEY_PATH`), по образцу TLS API воркеров.
- `deploy/.env.example`: закомментированные `OWNS3_TLS_CERT_PATH` /
  `OWNS3_TLS_KEY_PATH` (прод) и `PGW_BACKUP_S3_CA_BUNDLE_PATH` (прод;
  путь `ca.pem` в контейнере воркера) — с пояснениями.
- Стендовые чеки `46-owns3.sh`/`47-owns3-ec.sh` — без изменений (HTTP).

### 3.4. Клиентская сторона подсистемы бэкапов

- **Воркер** (`src/PgWorker.Backups` + Program воркера):
  - `BackupsRuntimeOptions`/`Options.cs`: поле `S3CaBundlePath` (конфиг
    `PgWorker:Backups:S3:CaBundlePath`, env
    `PGW_BACKUP_S3_CA_BUNDLE_PATH`); при заданном пути Program воркера
    выставляет процессу `AWS_CA_BUNDLE` до создания S3-клиентов.
  - `BackupS3` (SDK-клиент) — без изменений.
- **Агенты/джобы** (материал CA — PEM через env `S3_CA_PEM` в контейнеры,
  материализация внутри):
  - `WalStreamProcess.AgentEnv` + `WalReceiver`: при заданном
    `S3CaBundlePath` воркер читает PEM и передаёт `S3_CA_PEM`; WalReceiver
    пишет PEM во временный файл и выставляет себе `AWS_CA_BUNDLE`
    (SDK-клиент приёмника).
  - Спеки джобов `BackupJobSpec`/`VerifyJobSpec`/`RestoreJobSpec`/
    `DrillJobSpec`: аналогично передают `S3_CA_PEM`.
  - Джоб полного бэкапа (`docker/backup/entrypoint.sh`): `S3_CA_PEM` →
    `/tmp/s3-ca.pem` + `export SSL_CERT_FILE` (mc).
  - Verify/restore/drill-команды (запускают mc-подпроцессы с
    `MC_HOST_*`): материализуют PEM и выставляют `SSL_CERT_FILE`
    подпроцессу.
- **Операторский `mc`** (прод): доверие через `SSL_CERT_FILE=<ca.pem>` —
  фиксируется комментарием в `.env.example`/ранбуке.

### 3.5. Тесты

- **Юниты**:
  - OwnS3: env-биндинги `OWNS3_TLS_*` → конфиг; fail-fast (один из пары,
    битый PEM, отсутствующий файл); HTTP-режим по умолчанию (материал не
    задан — Urls `http://`, поведение прежнее).
  - PgWorker.Backups: `S3CaBundlePath` в опциях; спеки джобов/AgentEnv
    передают `S3_CA_PEM` при заданном пути и не передают без него;
    материализация PEM→файл→env (AWS_CA_BUNDLE/SSL_CERT_FILE) в командах.
- **E2E** (`src/tests/OwnS3.IntegrationTests/E2e`): TLS-сценарии —
  собственный контур по канонам изоляции (`docs/e2e-isolation.md`,
  `docs/e2e-launch.md`): тест генерирует свою CA + серверный серт
  (`CertificateRequest`, .NET, на хосте тестов; SAN localhost/127.0.0.1),
  поднимает контейнер `owns3:e2e` с `OWNS3_TLS_CERT_PATH`/`_KEY_PATH`
  (файлы — ro bind-mount во временный каталог; динамический хост-порт),
  клиенты:
  - AWS SDK .NET с CA-bundle (механизм `AWS_CA_BUNDLE`): put/get/head/
    list/multipart smoke + `/healthz` по https;
  - `mc` c `SSL_CERT_FILE`: alias → mb → cp → cat → rm → rb;
  - негативный кейс: клиент БЕЗ доверия CA получает TLS-отказ (грань
    действительно валидирует цепочку), обычный HTTP-запрос на HTTPS-порт —
    отказ (не тихий downgrade);
  - полный teardown + ассерт чистоты.

## 4. Фазы

1. **arch** (сделано этой спекой): owns3/01 §2/§4, owns3/03 §6,
   owns3/05 §4/§6, arch/22, arch/19 §7/§9.
2. **ownS3 TLS-режим**: `OwnS3Options.Tls` + env-транляция + загрузка
   материала (Shared.Tls) + Kestrel https + fail-fast; юниты.
3. **TLS-пакет**: `gen.sh` — серт `owns3` (SAN, идемпотентность);
   `.env.example` + compose-комментарии.
4. **Клиентская сторона бэкапов**: `S3CaBundlePath` (опции, AWS_CA_BUNDLE
   воркера), `S3_CA_PEM`-проброс в спеки джобов/агентов, материализация
   (entrypoint.sh, команды джобов, WalReceiver); юниты.
5. **E2E TLS + прогоны**: TLS-сценарии OwnS3 E2E; полные серии
   OwnS3.UnitTests/IntegrationTests; чек стендовых чеков 46/47 не меняется;
   мерж-гейт: задача трогает `src/PgWorker.Backups` → docker-E2E маркер
   PgWorker `Scale_AddEmptyShard` на свежем Release (правило AGENTS.md).

## 5. Ограничения

- .NET 10, `TreatWarningsAsErrors=true`, `Nullable=enable`; новые пакеты
  не добавляются (серты в gen.sh — `openssl` CLI, как в существующих;
  тестовые CA/серты — `CertificateRequest` из BCL).
- Docker-образ ownS3 не меняется (runtime-слой, артефакты publish;
  локальные образы в registry не кладутся).
- Тесты: динамические порты, полный teardown, `BrokerBootSec`-подобные
  таймауты ≤ 100 с; никаких хардкод-портов.
- `AWS_CA_BUNDLE` — механизм AWS SDK .NET (поддерживается линейкой 3.7.х
  проекта); если E2E вскроет иное поведение конкретной версии — fallback:
  программная валидация цепочки в фабрике HttpClient `BackupS3`
  (локализованная правка, контракт arch/19 §7 не меняется — CA тот же).
- `SSL_CERT_FILE` для `mc` — стандартный механизм Go crypto/x509
  (Unix); проверяется E2E-сценарием с реальным `mc`.

## 6. Критерии приёмки

1. ownS3 без `OWNS3_TLS_*`: стартует и работает как раньше (HTTP);
   существующие юнит/интеграционные/E2E серии OwnS3 зелёные без правок.
2. ownS3 с валидным материалом: HTTPS-only на порту сервера —
   SDK-клиент с CA-bundle и `mc` с `SSL_CERT_FILE` проходят полный smoke
   (put/get/list/multipart, `/healthz`); HTTP-запрос на HTTPS-порт
   отклоняется; клиент без доверия CA получает TLS-отказ.
3. Невалидный материал (один из пары / битый PEM / нет файла) — отказ
   старта с понятной диагностикой (Critical + exit ≠ 0).
4. `deploy/tls/gen.sh`: выпускает `owns3.crt/key` идемпотентно (повторный
   запуск — no-op), серт верифицируется против `ca.pem`, SAN покрывает
   owns3/owns3-ec/localhost/host.docker.internal/127.0.0.1.
5. Воркер с `PGW_BACKUP_S3_CA_BUNDLE_PATH`: процесс получает
   `AWS_CA_BUNDLE`; спеки джобов/WAL-агента несут `S3_CA_PEM` (юниты);
   без пути — env-набор джобов идентичен прежнему (юнит-диф).
6. Стенд: `00-up.sh`+чекы 46/47 работают без изменений (HTTP-only),
   `gen.sh` в подъёме выпускает серт `owns3` без действий оператора.
7. E2E TLS-сценарии зелёны; teardown-ассерт чистоты контура проходит.
8. Мерж-гейт задачи: свежий Release, docker-E2E маркер PgWorker
   (`Scale_AddEmptyShard`) зелёный (правило для задач, трогающих код
   воркеров).
