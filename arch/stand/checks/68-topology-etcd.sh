#!/usr/bin/env bash
# Топология в etcd — источник правды адресов (стендовая инкарнация
# /shards/X/master из референса 12-bucket-pitfalls.md; в проде адрес пишет
# Patroni-Callback, здесь — сайдкары нод + синкеры hasync у HAProxy).
# IP контейнеров НЕ фиксированы: сайдкар регистрирует адрес ноды
# (/cluster/nodes/<node> → <ip>), hasync транслирует его в HAProxy runtime
# API (set server ... addr). Проверяется консистентность etcd↔реальность↔
# runtime и живая смена адреса реплики (пересоздание при занятом старом IP)
# БЕЗ рестарта HAProxy и обрыва write-эндпоинта.
# Предусловие: после 65-го (s2a мастер, s2b реплика/sync-standby).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

ip()  { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
h1()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h2()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
# чтение HAProxy runtime API из hasync-контейнера (сокет в общем volume)
haprt() { # <контейнер-hasync> <команда>
  docker exec -e CMD="$2" "$1" python -c '
import os, socket
with socket.socket(socket.AF_UNIX) as s:
    s.settimeout(5); s.connect(os.environ["HAPROXY_SOCKET"])
    s.sendall((os.environ["CMD"] + "\n").encode())
    print(s.recv(65536).decode(), end="")'
}
rt_addr() { haprt "$1" "show servers state" | awk -v srv="$2" '$4==srv {print $5}'; }

cleanup() { docker rm -f ipblocker >/dev/null 2>&1 || true; }
trap cleanup EXIT

# ═══ Arrange / Assert 0: предусловие — s2a мастер, s2b реплика ════════════════
[ "$(h2 -c 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ] \
  || { echo "❌ hap2 не ведёт на мастера s2a — запусти после 65-го"; exit 1; }
[ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()')" = "t" ] \
  || { echo "❌ s2b не реплика — запусти после 65-го"; exit 1; }

# ═══ Assert 1: etcd = фактическим адресам (регистрация сайдкаров) ════════════
# Проверяем только работающие ноды: после 30-го failover s1a лежит до конца
# прогона (docker inspect остановленной возвращает 'invalid IP')
echo ">>> Assert 1: /cluster/nodes/* = фактические IP нод"
for c in s1a s1b s2a s2b; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$c")" != "true" ]; then
    echo "  $c не работает (после 30-го failover) — проверяю живых"
    continue
  fi
  v="$(ect get /cluster/nodes/$c --print-value-only)"
  [ "$v" = "$(ip $c)" ] || { echo "❌ /cluster/nodes/$c = '${v:-нет ключа}', фактически $(ip $c)"; exit 1; }
  echo "  etcd: $c → $v"
done

# ═══ Assert 1.5: мёртвая нода исчезла из etcd (lease истёк) ══════════════════
# Анти-коллизия (находка прогона): docker переиспользует освободившиеся IP, и
# протухший ключ мёртвой ноды «накрыл» чужую ноду — HAProxy считал чужой
# сервер своим мастером. Ключ умирает с нодой (lease TTL), а hasync отводит
# бэкенд в 127.0.0.1 — health-check роняет его, чужие запросы не уходят.
for c in s1a s1b s2a s2b; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$c")" = "true" ]; then continue; fi
  v="$(ect get /cluster/nodes/$c --print-value-only)"
  [ -z "$v" ] || { echo "❌ /cluster/nodes/$c='$v' — ключ мёртвой ноды жив (lease?)"; exit 1; }
  echo "  etcd: $c мертва — ключ исчез (lease истёк)"
done
a="$(rt_addr hasync1 s1a)"
[ "$a" = "127.0.0.1" ] || { echo "❌ hap1/s1a addr='$a' (ожидался отвод в 127.0.0.1 после grace)"; exit 1; }
echo "  hap1 runtime: s1a → 127.0.0.1 (мёртвая нода вне ротации)"

# ═══ Assert 2: HAProxy runtime = etcd (синкеры применили адреса) ═════════════
echo ">>> Assert 2: HAProxy runtime-адреса бэкендов = etcd (hasync)"
for pair in "hasync1 s1a" "hasync1 s1b" "hasync2 s2a" "hasync2 s2b"; do
  set -- $pair
  [ "$(docker inspect -f '{{.State.Running}}' "$2")" = "true" ] || continue
  a="$(rt_addr "$1" "$2")"
  [ "$a" = "$(ect get /cluster/nodes/$2 --print-value-only)" ] \
    || { echo "❌ runtime $2 = '${a:-?}' != etcd"; exit 1; }
done
echo "  hap1/hap2 runtime: адреса бэкендов = etcd"

# ═══ Act: реплика s2b пересоздаётся с гарантированно ДРУГИМ IP ════════════════
# Старый адрес на время пересоздания занимает одноразовый контейнер ipblocker:
# без этого compose выделил бы s2b первый свободный = прежний адрес, и смена
# IP не была бы доказательной. HAProxy при этом НЕ рестартуем.
old="$(ect get /cluster/nodes/s2b --print-value-only)"
echo ">>> Act: пересоздание s2b; её старый адрес $old занимает ipblocker"

# Arrange: снять sync-имена на s2a — иначе после смерти s2b коммиты мастера
# (в т.ч. pg_drop_replication_slot ниже) зависнут в SyncRep (находка №9)
h2 -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null

# Act: убить реплику, срезать её physical-слот (basebackup создаст заново: -C -S)
docker compose rm -sf hc2b s2b >/dev/null 2>&1 || true
dropped=""
for i in $(seq 1 15); do
  if h2 -c "SELECT pg_drop_replication_slot('s2b_phys')" >/dev/null 2>&1; then dropped=1; break; fi
  sleep 1
done
[ -n "$dropped" ] || { echo "❌ не срезать слот s2b_phys на s2a"; exit 1; }

# Act: занять старый адрес, поднять s2b заново (клон от s2a)
docker run -d --name ipblocker --network pgstand_pgnet --ip "$old" python:3.12-alpine sleep 300 >/dev/null
docker compose up -d s2b hc2b >/dev/null 2>&1
until docker exec s2b pg_isready -U postgres -q 2>/dev/null; do sleep 2; done
until [ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ]; do
  sleep 1
done
new="$(ip s2b)"
echo "  s2b пересоздана: $old → $new (клон от s2a, слот s2b_phys заново)"
[ "$new" != "$old" ] || { echo "❌ адрес не сменился — проверка не доказательна"; exit 1; }

# ═══ Assert 3: цепочка подхвата без рестарта HAProxy ═════════════════════════
# сайдкар hc2b перерегистрировал адрес → hasync2 переприменил в runtime
v=""
for i in $(seq 1 30); do
  v="$(ect get /cluster/nodes/s2b --print-value-only)"; [ "$v" = "$new" ] && break; sleep 1
done
echo "  etcd: /cluster/nodes/s2b → ${v:-?}"
[ "$v" = "$new" ] || { echo "❌ etcd не узнал новый адрес s2b (сайдкар hc2b не перерегистрировал)"; exit 1; }

a=""
for i in $(seq 1 30); do
  a="$(rt_addr hasync2 s2b)"; [ "$a" = "$new" ] && break; sleep 1
done
echo "  hap2 runtime: s2b → ${a:-?}"
[ "$a" = "$new" ] || { echo "❌ HAProxy runtime не получил новый адрес (hasync2?)"; exit 1; }

# ═══ Assert 4: кластер снова здоров (sync-standby вернулся, hap2 пишет) ══════
h2 -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (s2b)'" -c "SELECT pg_reload_conf()" >/dev/null
st=""
for i in $(seq 1 60); do
  st="$(docker exec s2a psql -U postgres -tAc \
    "select sync_state from pg_stat_replication where application_name='s2b'" 2>/dev/null || true)"
  [ "$st" = "sync" ] && break; sleep 1
done
echo "  s2a: sync-standby s2b → ${st:-нет}"
[ "$st" = "sync" ] || { echo "❌ s2b не вернулась sync-standby (P8-предусловие шарда 2)"; exit 1; }
echo "  hap1:5432 → $(h1 -c 'select inet_server_addr()') = etcd s1b: $(ect get /cluster/nodes/s1b --print-value-only) (мёртвая s1a вне ротации)"
echo "  hap2:5432 → $(h2 -c 'select inet_server_addr()') = etcd-адрес мастера s2a: $(ect get /cluster/nodes/s2a --print-value-only)"
echo "✓ топология из etcd: смена IP подхвачена (регистрация → etcd → hasync → runtime) без рестарта HAProxy;"
echo "  мёртвые ноды исчезают из etcd (lease) и отводятся в 127.0.0.1 — переиспользованный IP чужой нодой не подменит мастер"
