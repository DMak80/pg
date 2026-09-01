# Верификация фикса на живом стенде (после деплоя образа pgworker:dev по приказу)

Стенд НЕ перезапускается и руками не трогается: все проверки — чтение
(docker logs/ps, etcdctl get, UI панели :5050). Деплой фикса — отдельный шаг
(пересборка образа + up -d pgworker в deploy/), по приказу пользователя.

## 1. Самолечение canon10/smoke (тик воркера, ожидание — минуты)

- Журнал воркера: `docker logs -f <pgworker>` — фазы provision идут без
  мгновенных повторных фейлов «не поднялся за бюджет» (бэкофф: серия фейлов
  редеет до ≤1/60 c, если проблема остаётся).
- Portalloc = факт: `docker compose exec etcd etcdctl get /pgworker/portalloc/canon10`
  содержит порты ФАКТИЧЕСКИХ контейнеров (15004–15009; сверка с
  `docker ps --filter name=pgw-canon10 --format '{{.Ports}}'`).
- Итог: `/clusters/canon10/config` без поля state (ACTIVE), все
  nodes state=RUNNING, status-ключи бакетов сняты, dsn записаны. Аналогично smoke.
- Контейнеры НЕ пересоздавались: `docker ps --filter name=pgw-` — те же
  контейнеры (CreatedAt/uptime не сброшены).

## 2. Коллизия закрыта (по приказу, опционально)

Создать новый кластер через панель/UI (e2e чек 15 или вручную) при живом
canon10: новый portalloc не пересекается с /pgworker/portalloc/* соседей.

## 2a. Живое восстановление portalloc (фикс Ф7-adoption, по приказу)

При живых контейнерах кластера удалить etcd-ключ закреплений — тик воркера
должен восстановить записи ФАКТОМ контейнеров, не пересоздавая их:

1. Зафиксировать фактические порты и uptime:
   `docker ps --filter name=pgw-canon10 --format '{{.Names}} {{.Ports}} {{.Status}}'`.
2. Удалить ключ: `docker compose exec etcd etcdctl del /pgworker/portalloc/canon10`.
3. Ждать один-два тика воркера (секунды; смотреть `docker logs -f <pgworker>` —
   фаза `planned` без `adopt-skipped`).
4. Проверить: `docker compose exec etcd etcdctl get /pgworker/portalloc/canon10`
   — записи восстановлены фактическими портами из п.1; в docker-сети кластеров
   с одинаковыми именами нод (canon/canon10/smoke — все с hostname `shard1a`)
   находка — только свой контейнер (фильтр чужих pgw-<C'>-* до матчинга).
5. Контейнеры НЕ пересоздавались: uptime/CreatedAt из п.1 не сброшены
   (adoption переписал записи, EnsureNode-сверка сошлась — recreate не нужен).

## 3. Алерты панели (UI :5050)

- cluster-not-initialized: после 900 c зависания — Warning (если кластер
  ещё не поднялся); гаснет при ACTIVE.
- provision-stuck: при живом last_error provision + серия старше 300 c —
  Warning с текстом ошибки (проверить details: fail_count).
- worker-unhealthy: пока /healthz воркера 503 — Warning pgworker/<id>;
  после самолечения — гаснет (healthz 200).

## 4. Черепки pgw-solo-*

Не трогать. Диагностика/процедура — arch/09 §11 (только по приказу).

## 5. Автономный reconcile Д1–Д3 (после деплоя, read-only)

- Д1 (коллизия портов): пересекающиеся portalloc canon10/smoke сходятся — каждый
  кластер на своих свободных портах; в docker logs воркера исчезает цикл
  «port is already allocated» за ≤ 2 тика; etcdctl get /pgworker/portalloc/<C> —
  без чужих пересечений (сверка с docker ps).
- Д2 (фальш-Active): журнал /pgworker/work/<C> (docker logs) показывает фазы
  repaired-portalloc / repaired-dsn; unreachable-ноды Active уходят вместе с
  репарацией адресов; dsn в etcd указывает на фактические порты контейнеров.
- Д3 (мёртвый HA-scope): scope без лидера при пустых volume — фаза reset-scope
  в журнале, ключи /service/<scope>/{initialize,leader,sync} исчезают, request_*
  живы, Patroni бутстрапится (initialize появляется заново); при живых данных —
  last_error «разбор оператора» (панель: provision-stuck с текстом).
