# MR-ревью `feat-t26-backup-jobs-in-receiver` — «t26-backup-jobs-in-receiver: backup-операции внутри WalReceiver»

**Ветка:** `feat-t26-backup-jobs-in-receiver` · **База:** `main` (merge-base `9f3eaa4a`) · **Задача dev-flow:** t26-backup-jobs-in-receiver
**Масштаб:** 41 коммит · 136 файлов · +7794/−3669

> **Вердикт: мержить с правками.** Функциональность задачи реализована и зелёная на целевых E2E (Р11/Р17 работают: ChainBroken refull 12–15 с); блокеров нет. Но три Major-находки — класс «данные/корректность при деградации»: частичный файл отдаётся recovery как валидный, S3-таймаут 15 с делает большие наборы непроходимыми, фантомный COMPLETED чужого снимка — их стоит закрыть до или сразу после мержа.

**Линзы ревью:** Л1 — корректность/конкурентность/отказоустойчивость · API-контракт (HTTP/etcd/S3/конфиг) · Л2 — чистота/совместимость/доки · Л3 — тесты. Обоснование: дифф крупный и трогает контракт подсистемы бэкапов целиком — все четыре релевантны, токены не урезались.

**Разделы:** [Что в ветке](#branch) · [Что найдено](#summary) · [Детально](#details) · [Что проверили и отклонили](#ruledout)

---

<a id="branch"></a>
## Что в ветке

- **arch-first**: arch/19 (548 строк — исполнитель-приёмник, tool-контейнеры, API, WAL_CHAIN_ERROR, §5 boot-window), arch/25 (№22 канон alias:8080, №13/t24 упразднены), arch/14 §2.1/§2.4 (инвариант общей сети, portalloc не тронут), arch/18 (:8080 = /metrics + API).
- **WalReceiver**: NEW `Operations/` — OperationRegistry (409-гвард), ApiContracts, FullBackupOperation (pg_basebackup HOME=/tmp, staging-чистка, upload 1:1 + wal-дубли), VerifyOperation/RestoreOperation (инцидентная механика 1:1, setpriv, fetch-wal), FetchWalCommand, ProcessRunner, фабрики; Program.cs (+241: режимы PGW_NODE_TOOL/fetch-wal, маршруты /api/backup/full* и /api/ops/*, гейт `Disabled || (Paused && !ChainError) || Unknown` ⇒ 503, ChainError ⇒ 202); WalReceiverGates (ChainError, парсинг WAL_CHAIN_ERROR).
- **PgWorker.Backups**: WalReceiverApiClient (транспорт ⇒ transient), WalReceiverAddress (ForNode alias:8080 / ForTool), FreePortProbe, ToolJobSpec, PoolBackupS3.ListPrefixUnionAsync; BackupProcess/BackupVerifyProcess/RestoreProcess/RestoreDrillProcess переведены на API; джоб-классы (BackupJobSpec/VerifyJobSpec/RestoreJob*/DrillJob*, логи, команды) и BackupNames.Container/Volume/JobContainer/JobVolume удалены; конфиг Job.Image/QuotaBytes/Staging.QuotaBytes упразднены, Staging.Dir канонический.
- **Provisioning**: PgtuneInputsFactory — кэш «вход→результат» (t094).
- **Панель**: парсеры, BackupChainBrokenRule/WalChainBrokenRule, ReliabilityCalculator — переименованы.
- **Упразднение pgworker-backup**: Dockerfile ×2, entrypoint.sh, deploy-compose, dev-stand (00-up.sh, images.txt), docs (runbook/backup-restore/e2e-isolation).
- **Тесты**: новые юниты операций/API/registry/клиента (≈1500 строк), процессы на фейке IWalReceiverApiClient (регресс вечного перезапуска), E2E на tool-контейнеры, джоб-тесты удалены вместе с классами.
- **Роадмап**: t26/t24/t23 удалены из backup.md; bugs.md — t093/t096–t100, t12 расширен.

---

<a id="summary"></a>
## Что найдено

Перенос реализован в соответствии со spec и работает на целевых E2E. Основной риск — не основная функциональность, а **поведение при деградации**: три находки класса «transient-условие трактуется как permanent/валидное» (частичный файл, таймаут на большие файлы, фантомный COMPLETED), плюс потеря покрытия E2E и кластер low-находок по гигиене.

**Происхождение:** 🆕 внесено этим диффом · ♻️ pre-existing · 🆕♻️ смешанное.

| # | Severity | Линза | Происх. | Проблема |
|---|----------|-------|:-------:|----------|
| [12](#p12) | ⚪ Info | все | — | Осознанность/семантика: cancel=FAILED не зафиксирован; wal-дубли верхнего регистра (♻️ 1:1); legacy-окно «BROKEN»; process-wide env в тестах |

---

<a id="details"></a>
# Детально

<a id="p12"></a>
## ⚪ #13. Info — осознанность/семантика
- `/api/ops/cancel`: операторская отмена verify в `verifying` → перманентный verify-FAILED («данные плохие»), restore в `recovering` → FAILED + снос data-volume; spec говорит лишь «best-effort». Зафиксировать в arch семантику или ввести отличимый маркер отмены.
- Wal-дубли полного — верхний регистр имён pg_wal, стрим пишет строчные: дубли никто не читает, контроль регистронечувствителен; bash-джоб вёл себя так же (♻️ 1:1).
- Legacy-окно «BROKEN»: старый wal-ключ парсится в Wal=null/не паузит стрим до первой перезаписи (~тик); самозаживляется, решение о переименовании санкционировано spec (Р17).
- `BackupApiOperationTests` мутирует process-wide env (восстановление в Dispose, сегодня безопасно).

<a id="ruledout"></a>
## Что проверили и отклонили (не-находки)

- **Р11/Р17 гейт-семантика**: формула, краевые случаи (STOPPED+ChainError, Unknown+ChainError, disabled), стрим-paused при WAL_CHAIN_ERROR — точно по spec/arch (проверено двумя линзами + тест-матрицей).
- **HTTP-коды и JSON-имена**: попарная сверка ApiContracts↔WalReceiverApiClient (id/target_time/endpoints/секреты/статусы) — расходится только документированный target_time (#10); 409-гвард и порядок IsGated→IsTransient корректны.
- **etcd-контракт**: пути не менялись; переименование проведено у всех писателей/читателей (WalStatusWriter, снапшотный и панельный парсеры, гейты, enum, алерт-правила, ReliabilityCalculator); fixture-«BROKEN» — легитимные толеранс-тесты других полей.
- **S3-совместимость**: старые полные читаются новым кодом (union + относительный ключ; задвоение исправлено в 43a79fc8); layout не менялся.
- **Конфиг**: Job.*/QuotaBytes/PGW_BACKUP_JOB_IMAGE — нулевые упоминания; Staging.Dir/NodeImage/PGW_BACKUP_STAGING_DIR — цепочка согласована; appsettings↔ToRuntime↔arch §9 сходятся.
- **Фиксы прошлых раундов**: union-листинг (дедуп/anySuccess), ProcessRunner (WhenAll+kill-дерево+OCE), RestoreProcess (freshlyLaunched/alreadyLaunched + 404-ветка), кэш pgtune (попадания реальные), HOME=/tmp, staging-чистка — верифицированы, регрессий нет.
- **E2E-канон** обновлённых сценариев: guid, own-only teardown + ассерт чистоты, MarkFailed+README, телеметрия, динамические порты, [PHASE] — нарушений нет.
- **Удаление джоб-тестов**: потери покрытия нет (классы удалены вместе с прод-кодом; замещающие ассерты глубже).
- **Терминальный снимок без Clear в проде** — корректно (супервизы опираются на персистентность).

---

*Линзы: 4 субагента (волнами по 2), находки перепроверены оркестратором по актуальному коду worktree (grep/чтение строк); все Medium+ подтверждены, Low/Info — с указанием статуса верификации. Трекed-файлы не менялись.*
