# A. ProvisioningProcess (V0–V5)

Уровень 4. Создание кластера от `NOT_INITIALIZED` до рабочего состояния.
Функция — [02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §2; модель —
[01-placement.md](../01-placement.md); секреты и TLS —
[04-security.md](../04-security.md) §1/§3.

```
V0 claim + journal(op=provision); снапшот «до»
V1 план: placement + порт-аллокация — под глобальным portalloc-клэймом
   /valkeyworker/locks/portalloc: занято = docker-публикации ∪ portalloc
   чужих кластеров; не взял клэйм → journal waiting-portalloc-lock
   (следующий тик); journal phase=planned
V2 ensure секретов: admin + app + CA (`ca_pem`/`ca_key`) — txn
   put-if-absent по отсутствующим из шести ключей
   (проигрыш → re-read существующих)
V3 контейнер (аргументы ACL/maxmemory из декларации и кредов, TLS-args
   лимиты resources, клиентский host-порт; env ноды
   VALKEY_TLS_{CERT,KEY,CA} со свежим сертом — NodeTlsProvisioner.
   BuildNodeTlsEnv; Cmd — cmd-обёртка раскатки [01-placement.md](../01-placement.md) §2)
   + state=PROVISIONING; существующий (re-run) — сверка: Cmd == канонической
   обёртке + env валиден против ca_pem (IsValidNodeEnv) + порт + лимиты →
   пропуск; иначе — RemoveNode + EnsureNode со свежим env
V4 ждать готовности: PING с admin-кредом по TLS отвечает (бюджет
   NodeBootSec, транзиент-толерантно) → state=RUNNING
V5 put endpoints (advertised host:clientPort); config: txn
   (compare mod_revision) → put канонического JSON без state;
   снапшот «после»; journal done
```

Гонка «панель пишет TO_REMOVE посреди provisioning»: перечитывание config
перед фазами — смена state безопасно прекращает процесс.
