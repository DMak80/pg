# Spec: t02-per-cluster-secrets — закрытие хвоста мерж-гейта (аудит + roadmap-чистка)

Задача roadmap: [`t02-per-cluster-secrets`](../../../arch/roadmap/pgworker.md).
Итог Фазы 1: **задача уже реализована и слита в `main`** (merge `5bcd467`,
2026-09-06, spec/plan
[`2026-09-06-t02-secrets-t07-kafka-ca-rotation`](../2026-09-06-t02-secrets-t07-kafka-ca-rotation/spec.md));
не выполнен только хвост мерж-гейта — roadmap-чистка. Этот spec фиксирует
результат сверки (аудит) и описывает единственную работу: правку
`arch/roadmap/`. Код, arch-контракты и тесты не меняются.

## 1. Цель

1. Зафиксировать документально (сверкой по коду `main`), что все критерии
   приёмки Части A (t02) spec 2026-09-06 выполнены и слиты.
2. Выполнить невыполненный мерж-гейт merge-коммита `5bcd467`, который в своём
   сообщении заявляет: «теги t02-per-cluster-secrets и t07-kafka-ca-rotation
   удалены из arch/roadmap/{pgworker,kafkaworker}.md; добавлена
   t02-external-secret-manager» — но фактический diff `5bcd467^1..5bcd467`
   по `arch/roadmap/` пуст, теги живы до сих пор (`git log -S` подтверждает:
   удаляющего коммита не было). Долг перед правилом «Roadmap — только
   несделанные задачи» закрывается этой задачей.

## 2. Решения пользователя (зафиксированы вопросами 2026-09-14)

| Вопрос | Решение |
|---|---|
| Задача уже слита в main — что делать | Аудит + roadmap-чистка: сверка критериев приёмки со слитым кодом, закрытие хвоста мерж-гейта; новая функциональность не пишется |
| Объём roadmap-чистки | Оба тега одного мерж-гейта `5bcd467`: `t02-per-cluster-secrets` (pgworker.md) и `t07-kafka-ca-rotation` (kafkaworker.md) — реализация обоих в main, тесты зелёные |
| Имя отложенной задачи про внешний SM | `t02-external-secret-manager` — имя уже зафиксировано сообщением merge `5bcd467`; слот t02 освобождается тем же коммитом, коллизии нет (слаги различаются) |

## 3. Принципы

1. **Roadmap — только несделанные задачи** (`arch/roadmap/README.md`):
   слитое удаляется из списков тем же коммитом; никаких пометок
   «закрыта/реализована» — история в git и `docs/superpowers/`.
2. **arch-first без изменений контрактов**: канон секретов
   (arch/14 §3.1/§3.3/§4/§5 I) уже согласован со слитым кодом — правится
   только backlog (`arch/roadmap/`), сервисные каноны не трогаем.
3. **Ничего сверх**: код воркеров/панели, arch-контракты, тесты, стенд —
   без изменений; новая разработка (внешний SM) — отложенная задача, не эта.
4. Язык: документация — русский, идентификаторы — английские.

## 4. Аудит: критерии приёмки t02 ↔ факт в main

Источник критериев — spec 2026-09-06 §8, Часть A (t02). Сверка по коду
`main` (ветка `t02-per-cluster-secrets` = `main` на `b76a3f9`):

| # | Критерий приёмки | Артефакт в main | Статус |
|---|---|---|---|
| 1 | Заявка `/pgworker/rotations/<C>` ротирует app + bucket_admin + mover: новые значения в etcd, dsn-ключи всех шардов с новым bucket_admin-паролем, заявка снята, journal done, роли ALTER'нуты на мастерах всех шардов с dsn | `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs`: R1 ensure → R2 ALTER ROLE трёх ролей (мастер — цепочка master-ключ → HA-лидер → Patroni REST) → R3 одна txn [compare по кредам и всем dsn][put 4 кредов + перезапись dsn regex `password=` + del заявки] → R4 снапшот. Сверх spec 2026-09-06 — четвёртый секрет `backup_password`/`backup_exec` (arch/19 §7, коммит `7ab266c`) | Выполнено |
| 2 | Новый кластер получает per-cluster mover/bucket_admin ключи при provisioning/adopt/add-shard (ensure); env — только fallback до ensure | `ClusterSecretEnsurer` (`IClusterSecretEnsurer.EnsureAsync`) вызывается из `ProvisioningProcess` (P1.5), `AddShardProcess` (A-фаза), `AdoptionProcess` (AD3), `ClusterSecretRotator` (R1); ключи `/clusters/<C>/{mover_password, bucket_admin_user, bucket_admin_password}` — канон arch/14 §3.1 | Выполнено |
| 3 | `POST /api/clusters/{c}/secrets/rotate` работает; старый путь `app-password/rotate` у PgWorker удалён вместе с панельной командой | `src/PgWorker.App/Api/Operations/RotateClusterSecretsHandler.cs`; панельный прокси `src/AdminPanel.Api/Operations/RotateClusterSecretsCommand.cs` → `/api/clusters/{C}/secrets/rotate`; UI `frontend/src/pages/cluster-details/RotateClusterSecretsButton.tsx`; путь `app-password/rotate` в PgWorker-коде отсутствует (остался только в KafkaWorker — осознанный дубль по канону arch/16) | Выполнено |
| 4 | Интеграционный/E2E тест: writer-нагрузка переживает ротацию без остановки записи (допустимы ретраи реконнекта) | `src/tests/PgWorker.IntegrationTests/E2e/E2eRotateScenarios.cs`: writer через DSN-точку (bucket_admin) с перечитыванием etcd, assert «письменная нагрузка продолжается после ротации»; прогон merge `5bcd467`: docker-E2E 8/8 на свежем Release (вкл. E2eRotateScenarios — маркер t02) | Выполнено |
| 8 | arch/14 согласован с кодом (ключи, фазы, имена процессов/эндпоинтов) | arch/14 §3.1 (ключи тройки), §3.3 (`/pgworker/rotations/<C>` — все per-cluster креды), §4 (env — fallback), §5 I (ClusterSecretRotator R0–R4), §1.1 (эндпоинт `/secrets/rotate`) | Выполнено |
| 9 | Юниты → интеграция → E2E зелёные | По протоколу merge `5bcd467`: юниты 1353 (pg-часть: `ClusterSecretRotatorTests`, `ClusterSecretEnsurerTests`), интеграция 246, docker-E2E 8/8 | Выполнено |
| 10 | Мерж-коммит: t02/t07 удалены из arch/roadmap/, добавлен пункт про внешнюю SM-интеграцию | **НЕ выполнено** — diff `5bcd467^1..5bcd467` по `arch/roadmap/` пуст; теги живы, пункт не добавлен | **Хвост — эта задача** |

Номера 5–7 (Часть B, t07 KafkaWorker) в таблицу не включены — аудита Части
B эта задача не несёт: CaRotator слит тем же merge `5bcd467`
(`src/KafkaWorker.Core`/`App` — фазы P/D/R/C/F, `CaRotatorTests`,
`CaRotationTests`; детали — merge-сообщение и §5.2), что достаточно для
снятия тега по правилу «Roadmap — только несделанные задачи».

Заключение аудита: функциональная часть t02 полностью в `main`, пробелов
реализации нет; единственный незакрытый пункт — критерий 10 (roadmap-чистка).
«Интеграция с secret-manager» из исходной постановки t02 закрыта решением
пользователя (spec 2026-09-06 §1): etcd — единственное хранилище per-cluster
секретов; внешняя интеграция отложена (см. §5 — возвращается в roadmap как
`t02-external-secret-manager`).

## 5. Структура и компоненты (единственная работа — arch/roadmap/)

### 5.1. `arch/roadmap/pgworker.md`

- **Удалить** пункт `t02-per-cluster-secrets` (строки 9–12 текущего файла —
  блок от `- **`t02-per-cluster-secrets`** —` до «(2026-08-28,
  feat-etcd-password-field).» включительно).
- **Добавить** первым пунктом раздела «## Задачи» (слот t02 освобождается тем
  же коммитом; порядок номеров = порядок исполнения):

```markdown
- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.
```

### 5.2. `arch/roadmap/kafkaworker.md`

- **Удалить** пункт `t07-kafka-ca-rotation` (строки 10–13 — весь блок до конца
  файла; CaRotator слит тем же merge `5bcd467`: окна двойного доверия,
  rolling-пересоздание, API `/api/kafka/clusters/{C}/ca/rotate`, панель+UI;
  тесты `CaRotatorTests`, `CaRotationTests` зелёные).
- Файл трека остаётся с пустым разделом «## Задачи» — прецедент
  `arch/roadmap/backup.md` (все задачи сняты, файл живёт).

### 5.3. `arch/roadmap/README.md`

- В правиле ведения (строка 8) пример тега `t02-per-cluster-secrets` (после
  снятия — несуществующая задача) заменить на живой `t05-quarantine-merge`.
  Иллюстрация в правилах не должна ссылаться на снятый тег. Больше ничего в
  README не меняется.

### 5.4. Не меняется (границы)

- Код (`src/**`), тесты, arch-контракты сервисов (14/15/16/19,
  adminpanel/02), deploy/, стенд — без изменений.
- `←`-зависимости: на оба снимаемых тега никто не ссылается (проверено
  grep по `arch/roadmap/`), чистить нечего.

## 6. Фазы

1. **Spec** (этот документ) — фиксация аудита и решений.
2. **Roadmap-правка**: точно три правки из §5 (pgworker.md, kafkaworker.md,
   README.md) — docs-only коммит в ветке задачи.
3. **Верификация**: grep-гейты §8 + сборка не требуется (код не тронут);
   `git diff` содержит только три roadmap-файла.
4. **Возврат в main** — по обычному флоу dev-flow (ревью → мерж), после
   чего roadmap соответствует правилу «только несделанные задачи».

## 7. Ограничения

- Код, тесты, сервисные arch-контракты — не трогать (это docs-only задача).
- Docker-E2E мерж-гейта не требуется: правило «E2E обязателен для задач,
  трогающих код воркеров» не срабатывает — код не трогаем.
- Коммит/мерж в `main` — только по явному одобрению пользователя
  (AGENTS.base §6).
- Внешняя SM-интеграция — НЕ в этой задаче: только roadmap-пункт
  (решение пользователя, §2).

## 8. Критерии приёмки

1. `grep -rn "t02-per-cluster-secrets" arch/roadmap/` — пусто (включая
   README-пример: заменён на живой тег).
2. `grep -rn "t07-kafka-ca-rotation" arch/roadmap/` — пусто;
   `arch/roadmap/kafkaworker.md` существует с пустым разделом «## Задачи»
   (как backup.md).
3. `arch/roadmap/pgworker.md` содержит пункт `t02-external-secret-manager`
   с формулировкой §5.1; живые пункты t05/t08 не тронуты.
4. `git diff main...HEAD` — только три файла `arch/roadmap/` (docs-only).
5. Источник истины не расходится: arch/14 §4/§5 I уже описывают слитый
   канон (аудит §4, строка 8 таблицы) — после задачи расхождений
   spec↔arch↔roadmap нет.
