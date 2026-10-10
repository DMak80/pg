# 01. ownS3: обзор и структура решения

Входная точка канона — [22-owns3.md](../22-owns3.md); трек развития —
[roadmap/owns3.md](../roadmap/owns3.md). Эта глава — место ownS3 в
системе, границы начального этапа, состав проектов и ключевые решения.
Референс-исходники MinIO — [../minio](../../../minio) (Go): точечный
пример устройства, не образец для копирования (принцип — во входной
точке).

## 1. Место в системе

```
   подсистема бэкапов (arch/19)                    mc (админ-операции)
   PgWorker: воркер/джобы бэкапов (write):
     полные бэкапы (pg_basebackup),                mc ls / cp / mirror …
     WAL-цепочки (pg_receivewal-агент)
   restore / verify / drill (read)
        │                                                │
        │ AWS SDK .NET (S3-агностичный клиент),          │ SigV4,
        │ path-style, env-endpoint, статические ключи    │ статические ключи
        ▼                                                ▼
   ┌────────────────────────────────────────────────────────────┐
   │ ownS3 — standalone-сервис (докер-контейнер)                │
   │   src/OwnS3.App — Kestrel :9000: SigV4, роутинг 22 операций│
   │   src/OwnS3.Storage — xl-хранение                          │
   │                            │                               │
   │                            ▼                               │
   │   том данных (docker volume, OWNS3_DATA_DIR) —             │
   │   single-drive xl-раскладка (глава 04)                     │
   └────────────────────────────────────────────────────────────┘
```

- **Клиенты** — два: подсистема бэкапов ([19-backups.md](../19-backups.md))
  — write (воркер/джобы бэкапов: полные `pg_basebackup`, WAL-агент
  `pg_receivewal`) и read (restore/verify/drill); клиент остаётся
  S3-агностичным — arch/19 не знает, MinIO за endpoint'ом или ownS3.
  Оператор — `mc` (рутинные операции руками). Оба клиента path-style,
  настраиваются env-endpoint'ом и статическими ключами.
- **ownS3 — standalone-сервис, не воркер**: без etcd-контракта, без
  provisioning-проектов, без лидерства — один процесс, один том.
- **Отношение к MinIO**: ownS3 — **целевая замена MinIO** подсистемы
  бэкапов после приёмки t39: клиенты переключаются env-endpoint'ом,
  код клиентов не меняется. Трек
  [roadmap/s3.md](../roadmap/s3.md) (t31–t34, MinIO-кластера как
  объект управления системы) живёт параллельно на переходный период.

## 2. Границы начального этапа

- **22 операции** (детальные контракты — глава
  [02-operations.md](02-operations.md)):
  - объекты (7): `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`,
    `DeleteObjects`, `CopyObject`, `GetObjectAttributes`;
  - листинги (3): `ListObjects` (v1), `ListObjectsV2`,
    `ListObjectVersions`;
  - бакеты (5): `CreateBucket`, `DeleteBucket`, `HeadBucket`,
    `ListBuckets`, `GetBucketLocation`;
  - multipart (7): `CreateMultipartUpload`, `UploadPart`,
    `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`,
    `ListParts`, `ListMultipartUploads`.
- **Бакеты без versioning**: `ListObjectVersions` — unversioned-семантика
  (текущие объекты, без delete markers и `VersionId`-полей).
- **Single-drive**: без erasure coding и репликации (том один —
  глава [04-storage.md](04-storage.md)); EC — t40.
- **Вне-наборные грани** — сабресурсы объекта (ACL, tagging, retention,
  legal-hold), конфигурации бакета (versioning, lifecycle, replication,
  encryption, …), S3-Express, notification, select — обращение к ним
  завершается единым канонизированным отказом `NotImplemented`
  (глава [02-operations.md](02-operations.md), раздел общих семантик).
- **Path-style only, HTTP-only** (глава
  [03-protocol.md](03-protocol.md)).
- Последующие этапы (erasure coding, репликация, TLS) — трек
  [roadmap/owns3.md](../roadmap/owns3.md).

## 3. Состав решения

| Проект | Ответственность | Чего внутри НЕТ |
|---|---|---|
| `src/OwnS3.Protocol` | чистые типы протокола: канонизация/проверка SigV4 (заголовочная, presigned, чанковая потоковая), XML-схемы запросов/ответов, формат S3-ошибок, ETag-семантика | без ASP.NET, без диска |
| `src/OwnS3.Storage` | xl-модель хранения: раскладка каталогов, чтение/запись `xl.meta`, `.owns3.sys`, атомарный коммит, multipart на диске, чистки | без HTTP |
| `src/OwnS3.App` | хост Kestrel: роутинг 22 операций, хендлеры, конфиг/env (`OwnS3:*` / `OWNS3_*`), health/метрики (канон [18-metrics.md](../18-metrics.md), каркас `Shared.Metrics`), стыковка Protocol×Storage | — |
| `src/tests/OwnS3.UnitTests`, `src/tests/OwnS3.IntegrationTests` | юниты (тест-векторы подписи, XML) и контурные тесты | — |

Границы зависимостей: `Protocol` и `Storage` не зависят от `App` и друг
от друга; `App` — от обоих; общие библиотеки `Shared.*` — по мере
надобности.

## 4. Ключевые решения

| Решение | Почему | Критерий |
|---|---|---|
| path-style only | оба целевых клиента (AWS SDK .NET в режиме ForcePathStyle, `mc`) работают path-style; веток virtual-host в роутинге и подписи нет | простота (клиенты — стандарт S3) |
| HTTP-only на начальном этапе | внутренний контур; целостность запроса защищает SigV4; TLS — задача трека (t42) | простота |
| собственный формат `xl.meta` | побайтовая совместимость с msgp-форматом MinIO не требуется; версионирование и расширяемость (EC-поля t40) — свои | референс MinIO (устройство, не формат) |
| root из env + статические access keys | единый env-механизм с воркерами; полный IAM (пользователи/политики через API) — вне начального этапа | простота |
| xl-раскладка хранения по механике MinIO, имена служебного контура собственные | раскладка — единственное сознательное заимствование у MinIO; имена задаёт канон ownS3 (`.owns3.sys`, `volume.json`) | референс MinIO |
| три проекта решения (`Protocol`/`Storage`/`App`) | протокол и дисковая модель тестируемы без хоста; хост — тонкий | простота |

## 5. Запуск

- **ownS3 всегда запускается в докере** (никогда хост-процессом — как
  PgWorker); том данных — единственный persistent-volume.
- **Env-секреты** (root-пара, статические ключи) — из env-файла по
  образцу `deploy/.env.example` воркеров (env-шаблон ownS3 создаётся в
  t39; состав секрета фиксирует глава
  [05-access-config.md](05-access-config.md)).
- **Правила образа**: сборка .NET на хосте (`dotnet publish`), в образ
  `COPY`-ируется только publish-вывод (runtime-слой; правило
  E2E-образов); локально собираемый образ в registry `192.168.0.1:5000`
  не кладётся; базовый runtime-образ — из существующих `images.txt`
  (новых внешних образов нет). Прод-Dockerfile поставки — отдельный
  процесс.
- Детальная модель dev-стенд-сервиса — t39; здесь зафиксированы только
  правила образа.

## 6. Durability и отказы

Вопрос «что теряется при отключении машины X» (multi-host-правило
системы):

- **Single-drive том — единственная копия данных ownS3**: отказ
  диска/машины = потеря всех данных ownS3 на нём, гарантий durability
  нет. Локальный том — кэш/хранилище без репликации.
- Если ownS3 уже обслуживает бэкапы — они недоступны на время отказа
  (или навсегда при потере диска); источник восстановления — реплики
  PG-шардов и цепочки бэкапов вне отказавшей машины.
- Осознанное ограничение начального этапа; путь митигации — t40
  (erasure coding) и последующие задачи трека (репликация).
