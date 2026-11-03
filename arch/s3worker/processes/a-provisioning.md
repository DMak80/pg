# A. ProvisioningProcess (P0–P5)

Уровень 4. Создание кластера от `NOT_INITIALIZED` до рабочего состояния.
Функция — [02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §2; модель
размещения — [01-placement.md](../01-placement.md); секреты и PKI —
[04-security.md](../04-security.md).

```
P0 claim + journal(op=provision, phase=started) + снапшот P12 «до»;
   перечитка config: TO_REMOVE посреди provisioning — безопасная остановка
   (phase=aborted-state-changed, клэйм жив)
P1 план: резолв placement всех инстансов (селектор → кандидаты →
   детерминированный выбор → placed_on) + portalloc — под глобальным
   клэймом /s3worker/locks/portalloc ([01-placement.md](../01-placement.md) §3–§4);
   не взял клэйм → journal waiting-portalloc-lock (InProgress, следующий
   тик); для etcd-backup кластера — проверка полноты спец-ключа env
   ([04-security.md](../04-security.md) §1; гвард API создания уже отсёк
   неполную пару — защита вглубину, неполная → permanent-отказ фазы);
   0 инстансов — фазы P2(docker)–P4 пустые (нет docker-мутаций); journal
   phase=planned
P2 ensure per-cluster secrets (txn put-if-absent, [04-security.md](../04-security.md) §1;
   etcd-backup — подстановка спец-ключа env) — выполняется и для
   0-инстансовых кластеров (etcd-операция); отказ, включая неполноту
   спец-ключа env, → journal-отказ ensure-secrets;
   далее на каждый инстанс: ленивый выпуск PKI (txn put-if-absent:
   pki/ca + pki/orchestrator, затем nodes/<I>/pki), volume данных,
   контейнер ownS3 (env — [04-security.md](../04-security.md) §3, лимиты
   resources, S3-порт из portalloc) + state=PROVISIONING; существующий
   (re-run) — сверка и пропуск; journal-фаза на инстанс
P2.5 ждать health всех инстансов (бюджет NodeBootSec, транзиент-толерантный
   цикл; не готово — journal waiting-nodes, InProgress)
P3 bucket'ы на каждом инстансе — идемпотентно (exists → пропуск;
   CreateBucket root-кредом из secrets-ключа): per-install + для
   etcd-backup кластера фиксированный etcd-слепковый `etcd-backups`
   ([02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §5)
P4 доводка состава mesh по links ([05-mesh-governance.md](../05-mesh-governance.md)) —
   та же механика, что G: deny-доведение + join недостающих пар + снос
   лишних target-записей; links пуст/0 инстансов — no-op; отказ P4 —
   НЕ отказ provisioning'а: journal waiting-mesh, P5 продолжает (состав
   mesh доведёт Active-G)
P5 put endpoints (advertised host:S3-порт; 0 инстансов — ключ НЕ пишется:
   endpoints существует ⟺ ≥1 инстанс) + config: txn (compare mod_revision)
   → put канонического JSON без state; снапшот P12 «после»; journal done
```

Идемпотентность: каждый шаг перепроверяет факт (контейнер есть? health
отвечает? bucket существует?); отказ между фазами — повтор тика по
journal-фазе. Состояния — [enums.md](enums.md).
