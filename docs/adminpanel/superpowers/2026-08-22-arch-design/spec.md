# Спецификация: AdminPanel — архитектура и разбивка на задачи

Дата: 2026-08-22. Фаза dev-flow: spec. Решения приняты исполнителем без
опроса пользователя (пользователь заранее одобрил: «не спрашивать, решать
сам») — каждое с обоснованием в §5.

## 1. Цель

Спроектировать с нуля и подготовить к исполнению **AdminPanel** — панель
администрирования (строго **read-only**) шардированных HA-кластеров
PostgreSQL из репозитория `../pg`:

1. **Инспекция etcd** — статус серверов (endpoints, кворум), члены кластера,
   лидер, alarms; особенности взаимодействия (lease-ключи, TTL).
2. **Инспекция шардированных БД** — кластеры, шарды, распределение бакетов,
   статусы переездов, журнал heals.
3. **HA-кластеры БД** — по каждому шарду: лидер, члены, роли, состояние
   реплик, лаги.
4. **Мониторинг/алертинг** — протухший lease мастера (P11), незавершённые
   переезды (P7), лаги репликации и слотов (P4/P8), потерянные бакеты
   (P23) и др. — реализация требования P21 (`../pg/arch/12-bucket-pitfalls.md`
   §7): «сводный дашборд бакеты × шарды × статусы × лаги + состояние
   lease-мастеров».

Инспектируемая система описана в `../pg/arch/11-bucket-sharding.md` (§2
контроль-плейн, §9 мониторинг); исследование —
[`research/pg-report.md`](research/pg-report.md), подходы реализации —
[`research/puzzle-report.md`](research/puzzle-report.md) (референс `../Puzzle`).

## 2. Принципы

- **Read-only навсегда**: ни одного эндпоинта/кнопки, меняющей etcd или PG.
  Единственная «запись» панели — собственная сессионная cookie.
- **Единственное обязательное подключение — etcd**: список endpoint'ов в
  настройках; вся топология (кластеры, шарды, реплики, бакеты) выводится из
  префиксов `/clusters/` и `/service/`. Live-пробы — опциональный слой.
- **arch/ — источник истины**: контракты (`arch/01–04`) опережают код;
  изменение поведения = правка контракта тем же коммитом.
- **Реюз референса**: каркас .NET берём из `../Puzzle` (копируем и обрезаем),
  не изобретаем: Minimal API, attribute-DI, CQRS+Result, модульная
  композиция, тест-стек, стиль документации.
- **Язык**: документация и комментарии — русский; идентификаторы — английские.

## 3. Компоненты (что строим)

Проект решения `src/AdminPanel.slnx` (формат .slnx):
`AdminPanel.Infrastructure` (каркас из Puzzle: Result, attribute-DI, CQRS
queries), `AdminPanel.Core` (модель снапшота + AlertEngine),
`AdminPanel.Etcd` (клиент gateway, парсеры, SnapshotRefresher/Store),
`AdminPanel.Probes` (Patroni REST + Npgsql SQL-пробы), `AdminPanel.Api`
(host: auth, REST, SPA из wwwroot); `frontend/` (React+Vite+TS);
`tests/AdminPanel.UnitTests` + `tests/AdminPanel.IntegrationTests`;
`dev-stand/` (собственный docker-стенд). Полная карта —
[`arch/01-architecture.md`](../../../arch/01-architecture.md).

Контракты (созданы этой задачей, источник истины):

- [`arch/README.md`](../../../arch/README.md) — индекс + ключевые решения;
- [`arch/01-architecture.md`](../../../arch/01-architecture.md) — слои,
  потоки данных, проекты, auth, фронтенд, конфигурация, отказы;
- [`arch/02-etcd-contract.md`](../../../arch/02-etcd-contract.md) — все
  читаемые ключи/префиксы etcd, форматы, модель снапшота, стратегия poll,
  обработка сбоев, live-пробы;
- [`arch/03-panels.md`](../../../arch/03-panels.md) — REST API (11
  эндпоинтов, DTO), панели UI, каталог из 24 алертов, SQL-каталог проб;
- [`arch/04-local-stand.md`](../../../arch/04-local-stand.md) — собственный
  dev-стенд (профили quick/full, сид, patroni-эмуляторы, e2e-проверки);
- [`arch/roadmap/`](../../../arch/roadmap/README.md) — 11 задач `t01…t11`
  в 6 треках, порядок = исполнение.

## 4. Ограничения (заданы пользователем)

- Бэкенд: .NET 10, C# (`LangVersion=latest`, `Nullable=enable`,
  `TreatWarningsAsErrors=true`), CPM в `Directory.Packages.props`, решение
  `.slnx`; подходы/код — из `../Puzzle`.
- Фронтенд: React + Vite + TypeScript, статикой из wwwroot ASP.NET Core;
  обновление — polling API (без WebSocket).
- Локальная разработка в docker: etcd + шардированная PG; паттерны из
  `../pg/arch/stand/`, но стенд **собственный, self-contained** в репо
  AdminPanel.
- Аутентификация: логин/пароль админа из настроек, без БД пользователей.
- Все функции — только просмотр.

## 5. Ключевые проектные решения (приняты самостоятельно, с обоснованием)

| # | Решение | Почему |
|---|---|---|
| 1 | **etcd-клиент = HTTP JSON gateway `/v3/*`** (HttpClient + base64), без gRPC и без сторонних .NET-клиентов etcd | Gateway — стабильный API etcd 3.5, включён по умолчанию при CLI-запуске (наш стенд и прод `../pg` так стартуют); тот же транспорт уже используют сайдкары инспектируемой системы; .NET gRPC-клиенты etcd заброшены/сырые — риск зависимости |
| 2 | **Снапшот в памяти + фоновый refresher (тик 3 с), API читает только память** | Панель не зависит от латентности etcd; отказ etcd не роняет UI (данные со штампом возраста); lease TTL 5 с виден с запасом; объём KV мал (сотни ключей) — полный range за тик |
| 3 | **Poll, а не etcd watch** | Read-only UI не нуждается в миллисекундах; watch-стрим = состояние/реванши/chunked-парсинг ради экономы одного range в 3 с; пересмотр только по доказанной потребности |
| 4 | **Live-пробы: Patroni REST `:8008/cluster` — да (on); SQL Npgsql — да (on), обе отключаемые** | Patroni REST даёт фактическое состояние реплик (running/streaming/лаг/timeline), которого в DCS нет; SQL даёт лаги слотов, sync-standby (P8) и inventory для сверки (P21) — без них каталог алертов неполон. DSN из etcd без пароля: пароль подставляется из настроек панели, соединение `default_transaction_read_only=on` + только SELECT к pg_catalog — двойная защита от записи |
| 5 | **Frontend: React + Vite + TS + Mantine + TanStack Query + React Router** (установка главного агента — React/Vite/TS; остальное — выбор) | Mantine — зрелый набор таблиц/бейджей/тёмной темы без emotion; TanStack Query — декларативный polling (`refetchInterval`) и кеш; роутер — стандарт. Сборка в `wwwroot` одним артефактом с бэком, dev — vite-proxy без CORS |
| 6 | **Polling UI 5 с (2/5/15/off)** | Данные и так производные от тика refresher 3 с; 5 с достаточно для дашборда и не грузит API; переключатель закрывает личные предпочтения |
| 7 | **Auth = ASP.NET Core Cookie Authentication, один админ, PBKDF2-hash или plain из `[Config]`** | Без БД пользователей по ТЗ; cookie — простейший серверный механизм без JWT-инфраструктуры; rate-limit 5/мин на login; static public + API guarded (секретов в бандле нет) |
| 8 | **CQRS только queries (IQuery), команд нет; Result-монада как в Puzzle** | Панель немая к данным; единый pipeline диспетчера (Activity/Result) переиспользуется из референса без мутационной ветки |
| 9 | **Стенд: 2 профиля — quick (etcd+сид) и full (+PG-шарды и patroni-эмуляторы с master-lease)**; без HAProxy/opsbox | Быстрый цикл бэкенд-разработки не требует PG; full воспроизводит failover (stop мастера → lease гаснет → алерт); HAProxy не нужен — панель не роутит клиентский трафик, SQL идёт по multi-host DSN с `TargetSessionAttributes` |
| 10 | **Тесты: unit (парсеры/AlertEngine на реальных фрагментах `../pg`) + integration (Testcontainers etcd/postgres + WebApplicationFactory) + e2e bash-скрипты стенда** | Три уровня: чистая логика алертов, реальный IO в контейнерах, сценарии UI-критичных переходов (failover). Фикстуры-фрагменты из скриптов `../pg` страхуют от расползания форматов |
| 11 | **Толерантность парсеров: неизвестный ключ — не ошибка (счётчик + warning)** | Инспектируемая система развивается; панель не должна падать или врать на новых ключах |
| 12 | **AGENTS.md: настоящие треки roadmap** (infra/etcd/sharding/ha/frontend/stand) вместо шаблонных Solana/EVM | Требование задачи; правила ведения — в `arch/roadmap/README.md` |

## 6. Критерии приёмки (задачи «Проектирование»)

1. Создан `arch/` с контрактами 01–04 + roadmap (README + 6 треков, 11 задач
   `tNN-slug` с `←`-зависимостями; порядок = исполнение).
2. Задачи roadmap покрывают: скелет (t01), auth (t02), etcd-клиент+снапшот
   (t03), API инспекции etcd (t04), API шардирования (t05), API HA+алерты
   (t06), фронтенд ×3 (t07–t09), dev-стенд+e2e (t10), финализация (t11).
3. Контракт 02 покрывает все читаемые ключи: `/clusters/` (config, shards/
   dsn|replicas|master, buckets/routing|status, heals), `/service/` (leader,
   members, config, optime, initialize), `/cluster/nodes/` (стенд),
   кластерные метаданные (status/member list/alarm).
4. Контракт 03 определяет REST API + DTO всех панелей и каталог алертов из
   P21 (протухший lease, не-ACTIVE статусы, лаги/safe_wal_size/wal_status,
   sync-standby) + P23 (routing в никуда).
5. AGENTS.md указывает на настоящие треки; шаблонные Solana/EVM убраны.
6. Документация на русском, идентификаторы английские; без TBD/TODO.

Критерии приёмки самой **панели** (инвариант для всех задач roadmap): `dotnet
build`/`dotnet test` зелёные (`TreatWarningsAsErrors=true`); e2e стенда
(04 §3) зелёный; в UI нет ни одного мутирующего действия; при остановленном
etcd панель остаётся доступной и честно показывает возраст данных.

## 7. Порядок исполнения

`arch/roadmap/`: t01 → t02 → t03 → t04 → t05 → t06 → t07 → t08 → t09 → t10
→ t11. Зависимости: t02,t03 ← t01; t04 ← t02,t03; t05 ← t04; t06 ← t04;
t07 ← t02,t04; t08 ← t05,t07; t09 ← t06,t07; t10 ← t06; t11 ← t08,t09,t10.
Каждая задача — отдельный проход dev-flow (spec → plan → код → ревью → мерж);
после мержа пункт удаляется из roadmap тем же коммитом.
