# MR-ревью `feat-t25-wal-receiver-in-pgnode` — «t25: WalReceiver внутри PG-контейнера ноды»

**Ветка:** `feat-t25-wal-receiver-in-pgnode` · **База:** `main` (merge-base `b6c9f4c3`; работа не закоммичена — дифф = рабочее дерево worktree) · **Задача dev-flow:** t25-wal-receiver-in-pgnode

> **Остаток ревью: п.9.4 (E2E-ассерт tli_jump).** Ассерт написан в
> `WalStream_TliJump_GapMarked_FullRetaken` (+ сценарий усилен wait-якорём
> TLI1-базы, без которого вход tli-процедуры недетерминирован — гонка
> «индукция раньше первой доставки»). Зелёный прогон серии блокирован
> дефектом вне области t25: переснятие полного после честной индукции не
> завершается (mc-передача джоба виснет под параллельной WAL-нагрузкой
> ownS3, воркер даёт ретрай-шторм полных) — заводка
> `arch/roadmap/bugs.md` → `t92-tlijump-full-retake-mc-stall`
> (логи/артефакты обоих прогонов в её подпапке). Прочие пункты ревью
> (#1–#8, #10, #11, #12 вкл. hot-path ArrayPool-пул) закрыты кодом и
> проверены: сборка 0/0; юниты WalReceiver/Assembler/Sweeper + AdminPanel
> зелёные; docker-интеграции (WalStreamProcess/ClusterDriver/
> NodeWalReceiver вкл. supervisorctl stop-кейс/WalReceiverIntegration)
> зелёные; docker-E2E E2eWalStream — 6/6 (вкл. новый 3-репликовый
> кейс), E2eTliSwitch — 2/2 (гейт `.history` цел).

---

<a id="p9"></a>
## 🟡 #9 (остаток). Тест-пробелы против spec §6 — 9.4 не закрыт прогоном

**Severity:** Minor (№9.1/№9.2 обязательны до мержа — закрыты; №9.4 — тест есть, зелёный прогон блокирован t92) · **Линза:** Л3

- **9.4** E2E tli-сценарий не ассертировал `tli_jump`-факт в agent-ключе.
  Ассерт добавлен в
  `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamTliScenarios.cs`
  (`WalStream_TliJump_GapMarked_FullRetaken`: from=old-TLI-хвост, to,
  tli ≥ 2, поллинг agent-ключа мастер-приёмника после сходимости) вместе с
  wait-якорем «wal-tail-anchored» (≥ 2 TLI1-сегмента в S3 до индукции —
  детерминизация входа tli-процедуры, образец `WalStream_TliBackfill`).
  Зелёный прогон серии ожидает закрытия
  `t92-tlijump-full-retake-mc-stall` (см. шапку).

Закрыты и удалены из отчёта: №9.1 (юниты лидерского put/del
backups-disabled), №9.2 (supervisorctl stop → ключ гаснет ≤ TTL → start →
вернулся), №9.3 (3-репликовый шард: async-реплика waiting_role), №9.5
(двухшаговая сборка `pgworker-node:dev` при отсутствии).

---

*Отчёт собран скиллом dev-flow-mr-review (5 линз, каждая находка перепроверена оркестратором по коду). Worktree не тронут; флоу остаётся на merge-gate.*
