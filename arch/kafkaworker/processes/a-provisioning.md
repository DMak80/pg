# A. ProvisioningProcess (K0–K6)

Уровень 4. Создание кластера от `NOT_INITIALIZED` до рабочего состояния.
Функция — [02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §2; модель
размещения — [01-placement.md](../01-placement.md); секреты —
[04-security.md](../04-security.md) §3.

```
K0 claim + journal(op=provision); снапшот P12 «до»
K1 план: placement + порт-аллокация (закрепление portalloc) — под
 глобальным portalloc-клэймом /kafkaworker/locks/portalloc
 ([01-placement.md](../01-placement.md) §1):
 занят = docker ∪ portalloc чужих кластеров; не взял клэйм → journal
 waiting-portalloc-lock (InProgress, следующий тик);
 роли: broker1..m — controller (m=min(3,B)); фиксация brokers/<k>/role;
 journal phase=planned
K2 ensure секретов кластера: CA (ca_pem/ca_key) + креды admin + app —
 txn put-if-absent (проигрыш → re-read существующих)
K3 на каждую ноду: volume + контейнер (env [01-placement.md](../01-placement.md) §2,
 лимиты resources, сеть kfw-net-<C>, клиентский host-порт) +
 state=PROVISIONING; существующие (re-run) — сверка и пропуск;
 адрес ноды отсутствует в portalloc (частичный план после takeover) →
 journal `waiting-portalloc/<broker>` (InProgress, следующий тик; план
 достроит K1-фиксация/healer)
K4 ждать готовности: DescribeCluster отвечает, контроллер избран,
 брокеров = B (бюджет BrokerBootSec=600 с, транзиент-толерантно);
 state=RUNNING у всех
K5 применить dynamic broker configs из заявки (Converger
 [E](e-config-converger.md) — стартовый converge); put endpoints
 (advertised host:clientPort, запятая); config: txn (compare
 mod_revision) → put канонического JSON без state
K6 снапшот P12 «после»; journal done
```

Гонка «панель пишет TO_REMOVE посреди provisioning»: перечитывание config
перед фазами — смена state безопасно прекращает процесс. Состояния нод и
кластера — [enums.md](enums.md); критерий `RUNNING` — три факта, см.
[C. NodeSupervisor](c-node-supervisor.md).
