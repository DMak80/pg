# Runbook: запуск docker-E2E серий

Уровень 3. Порядок сборки и запуска docker-E2E серий, подготовка окружения.
Родитель — [README.md](README.md); детальные правила телеметрии и поведения
при падении — [docs/e2e-launch.md](../../docs/e2e-launch.md); изоляция
окружений — [docs/e2e-isolation.md](../../docs/e2e-isolation.md).

Все тестовые проекты docker-обязательны: нет docker-демона/образов — тест
падает с диагнозом, НЕ скип. «Запустил проект — ноль скипов»: отчёт штатного
прогона любого тестового проекта — только из Passed/Failed (единственный
допускаемый скип — условное предусловие TLI-сценария `_prepareSkip`).
Сборка Release и запуск тестов — ВСЕГДА двумя ОТДЕЛЬНЫМИ командами: сначала
сборка решения, затем тесты с `--no-build` и env-префиксом при самой команде
(`DOTNET_CLI_UI_LANGUAGE=en dotnet test …`); сборка внутри команды тестов не
используется — свежесть бинаря видна выводом отдельной команды сборки.
Тесты разделены на интеграционные проекты (`<Имя>.IntegrationTests`) и
E2E-проекты (`<Имя>.E2eTests`); PgWorker-E2E — хостовая модель: класс-фикстура
сама поднимает и сама удаляет своё окружение (двухфазный раннер-контур
упразднён — вердикт серии читается прямо в сводке прогона). E2E-образы
PgWorker собирает подготовка `PgWorker.E2ePrepare` — обязательна ДО прогона
`PgWorker.E2eTests` (без неё — мгновенный пачка-фейл с подсказкой);
E2E-образы Kafka/Valkey/S3 их проекты собирают сами once-per-process;
`PGW_TEST_E2E_NOBUILD=1` — только для бисекта/отладки конкретного бинаря.

Полные серии (по одной, не поверх друг друга и не поверх поднятого dev-стенда;
правила телеметрии и поведения при падении — [docs/e2e-launch.md](../../docs/e2e-launch.md),
изоляция окружений — [docs/e2e-isolation.md](../../docs/e2e-isolation.md); внешние образы
зеркалируются — `dev-stand/images/pull-images.sh`):

```bash
# 0) Сборка Release решения — ОТДЕЛЬНОЙ командой ДО любой серии
#    (обязательный шаг; все команды тестов ниже — строго с --no-build)
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo

# PgWorker — статическая подготовка E2E ДО прогона PgWorker.E2eTests
# (per-branch образы e2e-{ветка}; конкурентные подготовки сериализуются локом)
DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release

# PgWorker — юниты (docker не нужен)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build

# PgWorker — интеграционные (docker; Skipped: 0)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build

# PgWorker — docker-профиль драйверов (ExecDriver/DockerDriver/RestTls/RestRotation/…)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests \
    -c Release --no-build --filter FullyQualifiedName~PgWorker.IntegrationTests.Docker

# PgWorker — полная E2E-серия (класс-фикстуры; параллельность:
# maxParallelThreads=5 + Guard ≤ 3 живых контуров, env PGW_E2E_MAX_CONTOURS)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build

# KafkaWorker / ValkeyWorker / S3Worker — E2E (образы e2e-{ветка} собирают сами)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.E2eTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.E2eTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.E2eTests -c Release --no-build

# KafkaWorker / ValkeyWorker / S3Worker — интеграционные (docker; Skipped: 0)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Release --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Release --no-build
```

Кейс-маркер быстрой проверки (мерж-гейт задач воркеров, см. AGENTS.md):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests \
    -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard
```

После жёстких срезов серий проверять осиротевшие сети движка
(`docker network ls | grep -E 'kfw-net|pgw-.*-net'` при нуле контейнеров):
`docker network prune -f` — страховочный гейт между сериями.
