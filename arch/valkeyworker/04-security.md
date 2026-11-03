# ValkeyWorker: безопасность и ротации

Уровень 3. Функция «безопасность»: per-cluster CA и серты, TLS клиентского
порта, ACL-роли, секреты и ротации. Полные автоматы —
[E](processes/e-password-rotator.md), [T](processes/t-tls-migrator.md),
[K](processes/k-ca-rotator.md); сетевая модель и TLS установки —
[13-network-security.md](../13-network-security.md).

## 1. Per-cluster CA и серты

- `ca_key` — self-signed CA (PEM PKCS#8) — подпись серверных сертов нод;
  `ca_pem` — публичный CA — точка дискавери TLS-доверия клиентов (панель
  читает для live-проб). Ensure — той же txn put-if-absent, что и креды.
- Серт ноды: CN=`node<k>`, SAN только advertised-хоста (DNS|IP по правилу
  [01-placement.md](01-placement.md) §4), 10 лет, RSA-2048, EKU ServerAuth,
  NotAfter зажат в CA; генерируется `CertificateRequest` .NET. Доставка в
  контейнер — PEM в env, раскатка cmd-обёрткой
  ([01-placement.md](01-placement.md) §2). Перевыпуск — пересозданием
  контейнера со свежим env.
- В окне ротации CA (`ca_next_*`) серт ноды перевыпускается от НОВОЙ CA, а
  `VALKEY_TLS_CA` — NEW (не bundle: `--tls-auth-clients no`, нода клиентов не
  валидирует, CA — только материал issuer'а). Публичный `ca_pem` в окне —
  bundle OLD+NEW для клиентов.

## 2. ACL-роли

При старте контейнера ACL задаётся аргументами, на живой ноде — `ACL SETUSER`
(converge [D](processes/d-config-converger.md)). `default` отключён — вход
только по именованным пользователям.

| Пользователь | Роль | Права |
|---|---|---|
| `admin` | администратор | `~* +@all` — воркер (CONFIG/ACL/пробы), панель (read-only пробы) |
| `app` | приложения | `~* +@read +@write +ping +echo`; `+ping +echo` обязательны для StackExchange.Redis (handshake/keepalive) |
| `default` | — | `off`: анонимный вход запрещён |

## 3. Секреты

Per-cluster, в etcd, генерирует воркер (ensure txn put-if-absent):

- **app** — `app_user`=`"app"`, `app_password` (32 симв `[A-Za-z0-9]`);
  ротация — [E](processes/e-password-rotator.md).
- **admin** — `admin_user`=`"admin"`, `admin_password`; ротация —
  [E](processes/e-password-rotator.md).
- **CA** — `ca_key` + `ca_pem`; ротация — [K](processes/k-ca-rotator.md).

Компрометация etcd = зона доверия контроль-плейна (как pg/kafka); ротация —
единственный штатный путь смены кредов.

Env-секреты per-install — только TLS HTTP API воркера
(`VWK_API_TLS_{CERT,KEY,CLIENT_CA}`, [05-api.md](05-api.md) §2);
per-cluster-секреты живут в etcd; клиентский трафик нод шифруется TLS,
парольная ACL-защита дополняет его.

## 4. Ротация паролей (E)

Окно двух паролей без рестартов и без пересозданий: `ACL SETUSER <role>
>NEW` (OLD+NEW валидны) → атомарный коммит NEW в etcd (клиенты
переподключаются) → `ACL SETUSER <role> <OLD` (снятие OLD). Ротация admin не
трогает app-кред и наоборот. Отказ между фазами безопасен — оба пароля
валидны; повтор доигрывает по отдельному стейту `work/<C>/rotation` (фаза +
OLD/NEW), который переживает перезапись журнала надзором. E3 доигрывается
даже без заявки (краш между E2 и E3). Точный автомат —
[E. PasswordRotator](processes/e-password-rotator.md).

## 5. TLS-миграция (T)

Первый шаг Active-ветки: кластер без `ca_pem`/`ca_key` или контейнер без
`--tls-port` — пересоздаётся с каноническими TLS-аргументами и свежим
сертом; порт/адреса не меняются. nodes=1, persistence off — окно миграции =
одно пересоздание (секунды; кеш восполним). Отработавший миграцию кластер
неотличим от поднятого канонически. Точный автомат —
[T. TlsMigrator](processes/t-tls-migrator.md).

## 6. Ротация CA (K)

Второй шаг Active-ветки, окно эксклюзивно: пока оно открыто, надзор,
converge и ротация кредов в тике не идут (причина — env-сверка надзора
валидирует серт против одного issuer'а, а в окне D→R `ca_pem` — bundle).
Фазы P→D→R→C: staging новой CA → bundle в `ca_pem` (клиенты доверяют обоим
поколениям) → пересоздание ноды с сертом NEW CA (лечит любое состояние,
преф-чека живости нет) → атомарный коммит NEW и уничтожение OLD-ключа
перезаписью. Снятие заявки — атомарно коммиту. Точный автомат —
[K. CaRotator](processes/k-ca-rotator.md); классификация тика —
[06-operability.md](06-operability.md) §1.
