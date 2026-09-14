#!/usr/bin/env bash
# Чек грани «Воркеры» (spec §3.3/§3.4): generate → pending restart →
# restart → applied; панель сохраняет доступ к API воркера после
# перезапуска на self-signed серте (thumbprint-доверие). Возвращает стенд
# в исходное состояние (DELETE ключа + финальный рестарт → env-серт).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель жива, login
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл (стенд поднят? 00-up.sh)"; exit 1; }

# 401 без cookie / GET /api/workers отдаёт обоих воркеров
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/workers")"
[ "$code" = 401 ] || { echo "❌ /api/workers без cookie = $code"; exit 1; }
curl -fsS -b "$JAR" "$BASE/api/workers" | jq -e '.workers | length == 2' >/dev/null \
  || { echo "❌ /api/workers: ожидались карточки pgworker+kafkaworker"; exit 1; }
echo "  /api/workers: без cookie 401, с cookie — оба воркера"

# Act 1: generate (409-идемпотентность допускаем: ключ мог остаться от прогона)
http="$(curl -s -o /tmp/pgw-wc-gen.json -w '%{http_code}' -b "$JAR" -X POST \
  "$BASE/api/workers/pgworker/api-cert/generate")"
[ "$http" = 201 ] || [ "$http" = 409 ] || { echo "❌ generate = $http"; cat /tmp/pgw-wc-gen.json; exit 1; }
thumb="$(jq -r '.thumbprint // empty' /tmp/pgw-wc-gen.json)"
[ -n "$thumb" ] || thumb="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .targetCert.thumbprint // empty')"
[ -n "$thumb" ] || { echo "❌ thumbprint целевого серта не найден"; exit 1; }
echo "  generate pgworker api-cert: 201/409, thumbprint ${thumb:0:16}…"

# pending restart до перезапуска (PECULIARITY: живые инстансы сообщают старый thumbprint)
status="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .instances[0].applyStatus // "none"')"
[ "$status" = "pending restart" ] || echo "  ⚠️ статус до рестарта: $status (не 'pending restart' — проверьте стенд)"

# Act 2: restart → 202; Assert: applied после подъёма (полл ≤60 с)
http="$(curl -s -o /tmp/pgw-wc-restart.json -w '%{http_code}' -b "$JAR" -X POST \
  "$BASE/api/workers/pgworker/restart")"
[ "$http" = 202 ] || { echo "❌ restart = $http"; cat /tmp/pgw-wc-restart.json; exit 1; }
st=""
for i in $(seq 1 60); do
  st="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .instances[0].applyStatus // "none"')"
  [ "$st" = "applied" ] && break; sleep 1
done
[ "${st:-}" = "applied" ] || { echo "❌ после рестарта статус ${st:-none}, ожидался applied"; exit 1; }
echo "  серт применён (applied), панель сохранила доступ к API воркера"

# Cleanup: откат на env (DELETE + рестарт) — стенд в исходном состоянии
curl -fsS -b "$JAR" -X DELETE "$BASE/api/workers/pgworker/api-cert" -o /dev/null \
  || { echo "⚠️ DELETE api-cert не прошёл (ключа уже нет?)"; }
curl -fsS -b "$JAR" -X POST "$BASE/api/workers/pgworker/restart" -o /dev/null
echo "✅ чек грани Воркеры пройден"
