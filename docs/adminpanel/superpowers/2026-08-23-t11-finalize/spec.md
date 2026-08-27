# Спецификация t11-finalize — финализация проекта AdminPanel

Дата: 2026-08-23. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/infra.md` (пункт `t11-finalize` — объём; все зависимости
t08/t09/t10 слиты), `arch/01-architecture.md` §7 (сборка и запуск, строка
о многостадийном Dockerfile — уточняется этой задачей, см. §3),
`arch/README.md` (что такое arch/ и что в него не входит),
`arch/04-local-stand.md` §3/§5 + `dev-stand/README.md` (e2e-прогон,
порядок чеков), `arch/roadmap/README.md` (правила ведения roadmap и
прецедент удаления опустевшего трека: stand.md после t10, коммит 47ac22b).
Референс стиля документации — `../Puzzle/docs/` (индекс
`01-infrastructure.md`: таблица документов + «чувствительные зоны»;
документы-образцы `01.01-di.md`, `01.03-cqrs.md`: шапка «Назад: индекс»,
«Кратко: как пользоваться», разделы-таблицы, финальные «Чек-лист» и
«Грабли» — стиль копируем, наполнение своё).
Фактическое состояние: t01–t10 в `main`; сборка зелёная с 0 warnings
(`TreatWarningsAsErrors=true`, проверено в worktree), 35 unit-тест-файлов
+ 13 integration (Testcontainers: etcd + postgres:18), фронт
React+Vite+TS7+Mantine собирается в `src/AdminPanel.Api/wwwroot`
(каталог в `.gitignore`), стенд `dev-stand/` с чеками 00/10/20/30/40/90.
Известный долг: `arch/roadmap/ha.md` — живой пункт
`t90-fix-probe-enrich-flaky` (флак `EtcdSnapshotIntegrationTests.
Refresher_EnrichesSnapshot_FromProbeState` на полном прогоне, занесён
при мерже t07).

## 1. Цель

Финализация репозитория: рабочий `README.md` корня, практическая
документация `docs/` в стиле Puzzle, многостадийный `Dockerfile` +
`.dockerignore`, зелёный полный прогон (build + все тесты + e2e стенда +
smoke контейнера), стабилизация известного флака t90 (тест-код) и
закрытие roadmap (удаление пунктов `t11-finalize` и `t90-…` по
мерж-гейту). Функциональный код панели (C#/TS) не меняется.

Состав поставки:

1. `arch/`-правки (минимальные, до кода — §3): уточнение поставки в
   `arch/01-architecture.md` §7; указатель на `docs/` в `arch/README.md`.
2. `README.md` корня — полная замена (текущий написан на этапе t01):
   карта репо, быстрый старт, сборка/тесты, контейнер, ссылки.
3. `docs/` — индекс (`docs/README.md`) + 5 документов подсистем с
   чек-листами и граблями (§5).
4. `Dockerfile` (корень) + `.dockerignore` (сейчас отсутствует) — §6.
5. Полный прогон (§7): `dotnet build` (Debug+Release) 0 warnings,
   `dotnet test` (unit+integration), `npm ci && npm run build`,
   e2e стенда `90 -v → 00 → 10 → 20 → 30 → 40`, `docker build` + smoke
   контейнера. Включая фикс t90-флака (правка тестов, §7.4).
6. Roadmap-деливерабл (§8): удаление пунктов `t11-finalize` (infra.md) и
   `t90-fix-probe-enrich-flaky` (ha.md) тем же мерж-коммитом; опустевшие
   треки infra.md/ha.md удаляются вместе со строками в таблице
   `arch/roadmap/README.md`.
7. Финальное ревью ветки (§10).

## 2. Принципы

- **arch — контракт, docs — практики.** `docs/` не дублирует `arch/`:
  каждый документ подсистемы отвечает на «как это устроено в коде и что
  сломается при изменении», а не «что должно быть» (это — arch). При
  расхождении правится arch; docs ссылается на arch-разделы, а не
  пересказывает их.
- **Грабли — только фактические.** Каждый пункт раздела «Грабли» —
  реальный урок t01–t10 (коммит/spec-ссылка), не домыслы. Чек-листы —
  проверяемые шаги.
- **Стиль Puzzle.** Структура документов (шапка → «Кратко» → разделы →
  «Чек-лист при изменениях» → «Грабли»), русский язык, таблицы;
  идентификаторы/команды — как в коде.
- **Функциональный код не трогаем.** Исключения ограничены: (а) правка,
  без которой Dockerfile не собирается — минимальная, с фиксацией в PR;
  (б) тестовый код t90-фикса. Новых фич нет.
- **Прод-поставка без секретов.** В Dockerfile и appsettings.json нет
  паролей; всё прод-конфигурирование — ENV поверх образа
  (fail-closed auth уже в коде: без Password/PasswordHash логин отключён
  с warning в лог — Program.cs t02).
- **Канон .NET-доставки**: один процесс, один порт 8080
  (`ASPNETCORE_HTTP_PORTS`), не-root пользователь, HEALTHCHECK на
  `/api/healthz` (liveness, HealthzWriter), многостадийность
  (node-сборка фронта → sdk publish → aspnet runtime).

## 3. arch-правки (внесены в этой же ветке до фиксации spec; прецедент t10 §15)

Минимальные — контракт поставки уже описан в arch/01 §7, задача его
уточняет и связывает слои документации (обе правки уже в ветке):

1. `arch/01-architecture.md` §7, пункт «Контейнер (поставка)» —
   заменить одну строку на уточнённый контракт:
   `Dockerfile` в корне репо, три стадии — `node:22-alpine`
   (`npm ci && npm run build`, бандл SPA), `sdk:10.0`
   (`dotnet publish -c Release`), `aspnet:10.0` (runtime); порт
   8080 (`ASPNETCORE_HTTP_PORTS`), `EXPOSE 8080`, не-root пользователь,
   `HEALTHCHECK` на `GET /api/healthz`; прод-настройки — только ENV
   (`AdminPanel__*`: etcd-endpoints, auth, probes); wwwroot в образ
   собирается из frontend-стадии (в git его нет).
2. `arch/README.md` — после таблицы «Документы» добавлен указатель на
   `docs/` («практические документы подсистем: как менять, чек-листы,
   грабли; arch — контракт, docs — практики: при расхождении правится
   arch») и на `docs/superpowers/` (история задач). Ссылка на
   `docs/README.md` станет валидной первым же коммитом исполняющей фазы
   (индекс — деливерабл §5.1, тот же PR).

Другие arch-файлы не трогаются: поведение панели не меняется.
Roadmap-правки — отдельно, мерж-гейтом (§8).

## 4. README.md корня

Полная замена текущего (он описывает состояние t01 «скелет»). Разделы:

1. **Заголовок и первый абзац** — что это: read-only панель
   администрирования шардированных HA-кластеров PostgreSQL из репозитория
   `../pg` (etcd-контроль-плейн, шардирование, HA, алерты); операций
   записи нет. Стек одной строкой: .NET 10 Minimal API + React/Vite/TS
   (Mantine), снапшот-модель из etcd, live-пробы Patroni/SQL.
2. **Карта репо** — таблица: `arch/` (контракт: 01-архитектура,
   02-etcd-контракт, 03-панели, 04-стенд, roadmap), `docs/` (практики
   подсистем), `src/` (проекты решения и роли — компактно из arch/01 §2),
   `frontend/` (SPA), `dev-stand/` (docker-стенд + e2e-чеки),
   `Dockerfile`/`.dockerignore`, `docs/superpowers/` (история задач).
3. **Быстрый старт (стенд)** — из arch/04 §5 / dev-stand/README:
   терминал 1 `dotnet run --project src/AdminPanel.Api`, терминал 2
   `cd dev-stand && checks/00-up.sh` (или `docker compose up -d` — quick),
   `open http://localhost:5000`, логин `admin/admin`
   (appsettings.Development.json). Вариант «только API без стенда» —
   healthz отвечает, данные пустые.
4. **Сборка и тесты** — `dotnet build src/AdminPanel.slnx` (0 warnings:
   warnings как ошибки), `dotnet test src/AdminPanel.slnx` (Docker нужен
   — Testcontainers); фронт: `cd frontend && npm ci && npm run build`
   (typecheck + бандл в wwwroot) / `npm run dev` (прокси на :5000).
5. **Контейнер** — `docker build -t adminpanel .`; пример `docker run`
   с обязательными ENV (`AdminPanel__Etcd__Endpoints__0`,
   `AdminPanel__Auth__Username`/`Password` или `PasswordHash`,
   `AdminPanel__Auth__AllowHttp` для http без TLS; опционально
   `AdminPanel__Probes__*`); порт `-p 8080:8080`; healthcheck встроен.
   Отметка: из контейнера стенд доступен через
   `host.docker.internal:2379` (`--add-host=host.docker.internal:host-gateway`
   на Linux).
6. **e2e стенда** — краткая команда последовательности со ссылкой на
   `dev-stand/README.md` (порядок важен, повтор — с `90-down.sh -v`).
7. **Дальше** — ссылки: `arch/README.md` (контракт), `docs/README.md`
   (практики), `dev-stand/README.md`, AGENTS.md (правила работы).

Тон и язык — русский, команды точные (проверяются прогоном §7).

## 5. docs/ — индекс + 5 документов

Стиль — `../Puzzle/docs/` (образцы 01-infrastructure.md как индекс,
01.01-di.md как документ). Русский; шапка каждого документа:
`> Назад: [docs/README.md]`; финал каждого документа — «Чек-лист при
изменениях» и «Грабли» (фактические, из истории t01–t10). docs не
дублирует arch: ссылается на его разделы. Названия файлов и охват:

1. **`docs/README.md`** — индекс: таблица «документ → подсистема →
   назначение» (5 строк), принцип «arch — контракт / docs — практики»,
   строка про docs/superpowers (история задач) и `../Puzzle` как
   референс каркаса.
2. **`docs/01-framework.md`** — каркас `AdminPanel.Infrastructure`:
   attribute-DI (`[InjectAs*]`, `[Config]`, `UseDiBehaviours`),
   авто-хостинг BackgroundService через `[InjectAsSingleton]`,
   CQRS-queries + `Result`-монада, модульная композиция Program.cs,
   health-checks (self/etcd, теги live). Чек-лист: добавить сервис /
   добавить [Config]-секцию / добавить query. Грабли: статический кеш
   `ServiceCollectionExtensions._assemblies` — второй DI-хост в том же
   процессе не получает регистраций (урок t02 §14 → t03 §15: unit-тесты
   — без attribute-DI, WAF — один на коллекцию «api»); порядок
   `UseDiBehaviours` до `AutoRegistration`; NU1903 при обновлении
   пакетов (OpenApi 10.0.9 → 10.0.11 — предупреждение об уязвимости как
   ошибка; CPM-обновление = проверка прогона).
3. **`docs/02-etcd-snapshot.md`** — etcd-слой: HTTP JSON gateway `/v3/*`
   (base64-ключи, int64 decimal-строками →
   `JsonNumberHandling.AllowReadingFromString`; lease-ID — десятичная
   строка), sticky+failover по endpoints, парсеры `/clusters/` и
   `/service/`, `SnapshotRefresher` (тик 3 c, единственный писатель) →
   `SnapshotStore` (volatile-swap), `EtcdHealthCheck`. Ссылки:
   arch/02 (контракт ключей/модели). Чек-лист: добавить ключ/поле в
   модель (arch/02 → парсер → DTO → фронту), изменить тик. Грабли:
   API не ходит в etcd на запрос (только снапшот — иначе латентность
   etcd ломает UI); «connection refused» на свободный порт мгновенный —
   тесты недоступности не флакают по таймауту (t03); пустые Endpoints =
   снапшот не строится, панель жива (это норма, не падение).
4. **`docs/03-probes-alerts.md`** — live-пробы и алерты:
   `ProbeOrchestrator` (тик 15 c, цели из снапшота, отключаемость
   PatroniEnabled/SqlEnabled), Patroni REST `:8008/cluster`, SQL-проба
   (Npgsql, каталог 03 §5 одним подключением, read-only),
   `HostMapResolver` (точное совпадение `host:port`; в appsettings ключ
   `host__port` — конфиг-байндер режет `:`; канонический формат в
   памяти приоритетен при наличии обоих — урок t10), `AlertEngine` —
   24 правила (состав по группам — ниже в этом же пункте). Чек-лист: добавить
   правило (rules-файл + unit-сценарий + при необходимости DTO/фронт),
   добавить поле пробы. Грабли: `TargetSessionAttributes=ReadWrite` в
   Npgsql 10 работает только на multi-host DSN, read-only-guard —
   сессионный SET (t06); DSN берётся из etcd — если хосты не резолвятся
   с панели, это лечится HostMap, не правкой кода; пробы кратно реже
   тика снапшота — чеки ждут до 40 c. Состав `AlertEngine`: 24 правила
   (по факту кода `Core/Alerting/Rules/`: etcd-здоровье 5, шардирование
   и переезды 11, HA/слоты 7, пробы 1) — перечислить в документе
   группами с kind-идентификаторами.
5. **`docs/04-frontend.md`** — фронт-каркас: сборка (`npm run build` =
   tsc-typecheck ×2 + vite build → `src/AdminPanel.Api/wwwroot`,
   `emptyOutDir: true` обязателен — outDir вне root), dev-режим
   (прокси `/api` → :5000, CORS не нужен), api-слой (`apiFetch` +
   ApiError + 401→/login, DTO camelCase — как в JSON), TanStack Query
   (polling-контекст 2/5/15/off + localStorage), guard через
   `/api/auth/me`, Mantine dark + один css-импорт. Чек-лист: добавить
   страницу/эндпоинт-клиент (dto.ts → queries.ts → страница), изменить
   polling. Грабли: TS7 (typescript 7.0.2) не понимает side-effect
   css-импорт без ambient-декларации — `vite-env.d.ts`
   (`declare module '*.css'`, урок t07 коммит f4edda4); `.npmrc` с
   публичным registry.npmjs.org обязателен для Docker/чистых окружений
   (default-registry машины может быть приватным — урок t07 c0c5ac9);
   wwwroot в `.gitignore` — бандла в клоне нет, «SPA не отдаётся» =
   просто не собран (warning в логе хоста); severity и enum'ы в DTO —
   строчные строки.
6. **`docs/05-dev-stand.md`** — стенд: профили quick/full, состав и
   порты (таблица HostMap из arch/04 §1), идемпотентный сид (значения =
   `EtcdSeed` интеграционных тестов), эмуляторы `hc*` (lease TTL 5 c,
   master-ключи под lease), e2e-чеки и их порядок. Чек-лист: изменить
   сид (seed.sh + EtcdSeed + фикстуры синхронно!), добавить чек,
   повторный прогон. Грабли: полный прогон только с чистого состояния
   (`90-down.sh -v`); после full переход в quick — только с `-v`
   (идемпотентный сид не восстанавливает протухшие lease-ключи);
   SyncRep-ловушка после promote (30-й чек снимает
   `synchronous_standby_names`); `container_name` с префиксом `as-` —
   сосуществование со стендом `../pg`; официальный etcd-образ
   distroless — seed-образ это alpine + скопированный etcdctl.

Объём каждого документа — ориентир 80–150 строк (как 01.12-docs Puzzle),
без воды. Все ссылки на файлы/разделы arch — точные.

## 6. Dockerfile + .dockerignore

### 6.1. Dockerfile (корень репо)

Три стадии (канон dotnet-docker samples, adaptation под SPA):

```dockerfile
# 1) фронт: бандл SPA (node >=22.12 по engines)
FROM node:22-alpine AS frontend
WORKDIR /src
COPY frontend/.npmrc frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build          # tsc --noEmit ×2 + vite build → бандл SPA (см. инвариант ниже)

# 2) бэкенд: publish
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/AdminPanel.Api/AdminPanel.Api.csproj \
    -c Release -o /app --nologo

# 3) runtime
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=publish /app ./
COPY --from=frontend <бандл> ./wwwroot   # точный путь стадии — план; инвариант ниже
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER app
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:8080/api/healthz || exit 1
ENTRYPOINT ["dotnet", "AdminPanel.Api.dll"]
```

Точные пути выходов frontend-стадии (vite `outDir` =
`../src/AdminPanel.Api/wwwroot` относительно `frontend/`) — деталь плана;
в spec фиксируется инвариант: **бандл попадает в runtime-образ, в git его
нет, ручная сборка wwwroot не требуется**. `curl` в aspnet-образе
отсутствует — ставится в runtime-стадии (`apt-get update && apt-get
install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*`)
единственно ради HEALTHCHECK; пользователь `app` (uid 1654) есть в
aspnet:8.0+, при отсутствии в 10.0 — создать `useradd` (проверить на
этапе плана, выбрать один вариант). Прод-secret'ы в Dockerfile и ENV
по умолчанию отсутствуют (fail-closed: логин отключён, etcd-список
пуст — панель стартует и отвечает healthz).

### 6.2. .dockerignore (новый файл, корень)

Исключить из контекста: `.git`, `.dev-flow`, `docs`, `arch`,
`dev-stand`, `**/bin`, `**/obj`, `**/node_modules`,
`src/AdminPanel.Api/wwwroot` (артефакт vite), `graphify-out`, `*.md`
(кроме тех, что не нужны образу — README в образ не копируется),
`.DS_Store`, `.vs`, `.idea`. Инвариант: контекст сборки — только
`frontend/` и `src/` (+ Dockerfile); ничего из стенда и документации в
образ не попадает.

## 7. Полный прогон (гейт задачи)

Все шаги — на worktree-ветке; каждый фиксируется выводом в отчёте
(verification-before-completion):

1. **Сборка**: `dotnet build src/AdminPanel.slnx` и
   `dotnet build -c Release` — 0 warnings (уже так; гейт против
   регрессий документации/фронта не должен влиять).
2. **Тесты**: `dotnet test src/AdminPanel.slnx` — unit + integration
   (Docker запущен; Testcontainers). Зелёные, включая стабилизированный
   t90-тест (§7.4), прогнать integration дважды (полный прогон —
   там, где флак воспроизводился).
3. **Фронт**: `cd frontend && npm ci && npm run build` — typecheck+build
   зелёные, wwwroot создан; `dotnet run` отдаёт SPA (ручная проверка
   `GET /` = index.html).
4. **Фикс t90-флака** (`t90-fix-probe-enrich-flaky`, тест-код):
   воспроизвести полный integration-прогон → причина по описанию пункта
   — порядок тестов в коллекции общего etcd-контейнера влияет на сид
   («лишний bucket_0» в inventory-mismatch) → изолировать сид
   (idempotent-seed/isolation в `EtcdContainerFixture`/`EtcdSeed`),
   не меняя продуктовый код. Если причина окажется в продуктовом коде —
   минимальная правка по правилу §2 (зафиксировать в PR отдельным
   коммитом с объяснением).
5. **e2e стенда** (панель на хосте): `dev-stand/checks/90-down.sh -v` →
   `00-up.sh` → `10-smoke-api.sh` → `20-alerts.sh` → `30-failover.sh` →
   `40-live-probes.sh` — все зелёные; финал — `90-down.sh -v`.
6. **Контейнер**: `docker build -t adminpanel .` (использует свежий
   .dockerignore) → smoke против quick-станда:
   `docker run -d -p 8080:8080 --add-host=host.docker.internal:host-gateway \
   -e AdminPanel__Etcd__Endpoints__0=http://host.docker.internal:2379 \
   -e AdminPanel__Auth__Username=admin -e AdminPanel__Auth__Password=admin \
   -e AdminPanel__Auth__AllowHttp=true adminpanel`
   → проверить: `docker inspect` health становится `healthy`;
   `curl http://localhost:8080/api/healthz` = 200; `GET /` отдаёт SPA; логин и
   `/api/overview` с данными сида. Пробы из контейнера отключить
   (`AdminPanel__Probes__PatroniEnabled=false`, `SqlEnabled=false`) —
   стендовые адреса из etcd из контейнера не резолвятся (ожидаемо,
   фиксируется в README §4.5 как ограничение smoke-режима).

## 8. Roadmap-деливерабл (мерж-гейт)

По правилам `arch/roadmap/README.md`, тем же мерж-коммитом:

- удалить пункт `t11-finalize` из `arch/roadmap/infra.md`;
- удалить пункт `t90-fix-probe-enrich-flaky` из `arch/roadmap/ha.md`
  (закрывается фиксом §7.4 — полный зелёный прогон невозможен с живым
  флаком);
- оба трека опустеют → удалить файлы `infra.md` и `ha.md` и их строки из
  таблицы треков `arch/roadmap/README.md` (прецедент: `stand.md` после
  t10, коммит 47ac22b). Остаются etcd/sharding/frontend (пустые
  заготовки — не трогаем).

## 9. Ограничения

- Функциональный код (C#, TS, compose, скрипты стенда) — без изменений,
  кроме двух явных исключений §2 (Dockerfile-блокер, t90-тест-фикс).
- Контракт API/etcd/алертов не меняется; arch-правки — только §3.
- Никаких новых зависимостей (npm, NuGet) — образ собирается из
  существующих lock-файлов (`package-lock.json`, CPM).
- Не делаем (осознанно, вне объёма): CI, multi-arch, docker-compose
  поставки, публикация образа в registry,versioning.

## 10. Финальное ревью (условия закрытия)

1. Все прогоны §7 зелёные, выводы зафиксированы (кратко в PR).
2. README/docs проверены на соответствие фактическому коду: команды
   запускаются, ссылки ведут, числа верны (24 правила, чеки, порты).
3. Docker-образ: собирается, healthy, отдаёт SPA и API.
4. Roadmap/арх-правки применены (§3, §8).
5. Ревью по superpowers:requesting-code-review: дифф ветки целиком;
   отдельное внимание — отсутствие изменений функционального кода
   (кроме зафиксированных исключений) и полнота «граблей» в docs.

## 11. Принятые решения (апрув выдан заранее)

1. **Dockerfile в корне, 3 стадии**; node:22-alpine (engines >=22.12),
   sdk/aspnet 10.0; порт 8080; `USER app`; HEALTHCHECK через curl
   (curl доставляется в runtime-стадию — в базовом образе его нет, а
   HEALTHCHECK на /api/healthz требуется roadmap-пунктом).
2. **docs/ — 5 документов + индекс** (а не 4 и не 6): каждый — ровно
   одна подсистема с собственной историей граблей; поставка описана в
   README корня и самом Dockerfile, отдельный документ не нужен.
3. **arch-правки минимальны**: только уточнение §7 arch/01 и указатель
   в arch/README — контракт уже описывал поставку, стиль документации
   контрактом не является.
4. **t90 закрывается в t11**: полный зелёный прогон — деливерабл t11,
   флак ему противоречит; правка только тестов. Треки infra/ha
   опустеют и удаляются (прецедент stand.md).
5. **README корня заменяется целиком** — текущий описывает t01-скелет,
   править по кускам дороже, чем переписать.
6. **Smoke контейнера — против quick-станда** с выключенными пробами:
   full-пробы из контейнера требуют HostMap на host-gateway — это
   опциональная ручная проверка, в гейт не входит.
7. **`.dockerignore` создаётся** (отсутствует): контекст ограничен
   frontend/+src/, образ не распухает bin/obj/node_modules.
8. **Никаких secrets в образе**: конфигурация только через ENV поверх
   fail-closed-поведения кода (без пароля логин отключён).
9. Язык всех новых документов — русский; идентификаторы, команды,
   пути — английские как в коде.

## 12. Риски

| Риск | Митигация |
|---|---|
| vite outDir в frontend-стадии пишет вне WORKDIR относительно `frontend/` | план фиксирует копирование бандла явным `COPY --from` по фактическому пути; инвариант §6.1 проверяется smoke-проверкой `GET /` |
| `USER app` отсутствует в aspnet:10.0 | проверить при реализации; фолбэк — `useradd -u 1654` (одна строка, канон dotnet-samples) |
| HEALTHCHECK без curl | curl ставится в runtime-стадии; альтернатива не выбрана сознательно (dash не умеет /dev/tcp, distroless-минимализм не требуется) |
| Флак t90 не воспроизводится на этой машине | прогон integration ×2 + полный прогон §7.5; фикс изоляции сида остаётся (безвреден), пункт t90 удаляется только при стабильном зелёном |
| Docker build тянет npm-пакеты из приватного default-registry | `.npmrc` копируется в стадию первым слоем (публичный registry.npmjs.org — урок t07) |
| NuGet restore в контейнере | `src/NuGet.Config` — публичный nuget.org + source mapping, креды не нужны |
| docs расползутся по объёму | ориентир 80–150 строк/документ; arch не пересказывается, ссылки вместо копирования |
