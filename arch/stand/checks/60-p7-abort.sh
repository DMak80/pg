#!/usr/bin/env bash
# P7: артефакты незавершённых переездов. Скрипт abort-move.sh (etcd):
# находит остатки в etcd, пишет журнал уборки в etcd ДО любых манипуляций
# с БД, идемпотентно чистит БД, затем возвращает ACTIVE удалением статус-ключа.
#
# Фаза 1 — зависший FROZEN, переключение НЕ произошло: заморозка P1 на s1,
#   живая подписка и схема на s2, routing=s1 + протухший статус в etcd.
#   Проверяет: list, отказы-защиты, журнал до манипуляций (s2 недоступен →
#   phase=blocked, БД не тронута), resume после возврата s2, возврат ACTIVE.
# Фаза 2 — routing==target, переключение ПРОИЗОШЛО (flip отразился во владении,
#   статус-ключ завис — вырожденный случай неатомарного cutover): в БД то же,
#   но routing уже на s2. Проверяет: abort без --force отказывается, не трогая
#   ничего; с --force ДОВОДИТ перевод до конца — владелец s2 не трогается,
#   старый шард вычищается (finalize-семантика), etcd → ACTIVE на s2.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

# psql «с ops-бокса» по внутренней сети стенда (hap1 = write-эндпоинт шарда s1,
# выбирает текущего мастера — работает и после failover из 30-й проверки)
h1()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 \
          "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h1a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 \
          "host=hap1 port=5432 dbname=postgres user=app_role" "$@"; }
h2()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 \
          "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
h2a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 \
          "host=hap2 port=5432 dbname=postgres user=app_role" "$@"; }
abort() { docker compose run --rm -T opsbox bash /arch/scripts/abort-move.sh "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

docker compose --profile ops build opsbox >logs/60-opsbox-build.log 2>&1
echo "opsbox собран (psql+etcdctl+jq, профиль ops)"

# Arrange (общий для фаз): зависший переезд bucket_43 s1 → s2 — данные и
# публикация у s1, живая подписка (настоящий initial copy) и схема на s2,
# заморозка P1 (REVOKE) + мораторий P5 на s1. etcd-ключи выставляет фаза.
arrange_stuck_move() {
  # хвосты прошлого прогона/фазы — подчищаем
  ect del /clusters/legacy/buckets/status/bucket_43 >/dev/null
  ect del /clusters/legacy/buckets/routing/bucket_43 >/dev/null
  h2 -c "DROP SUBSCRIPTION IF EXISTS sub_bucket_43;" >/dev/null 2>&1 || true
  h2 -c "DROP SCHEMA IF EXISTS bucket_43 CASCADE;" >/dev/null
  h1 -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_43%';" >/dev/null 2>&1 || true
  h1 -c "DROP PUBLICATION IF EXISTS pub_bucket_43;" >/dev/null 2>&1 || true
  h1 -c "DROP SCHEMA IF EXISTS bucket_43 CASCADE;" >/dev/null
  # владелец: схема + данные + app-роль с правами (на ОБОИХ шардах — владельцем
  # в фазе 2 становится s2, unfreeze будет GRANT-ить там)
  h1 -c "CREATE SCHEMA bucket_43;
         CREATE TABLE bucket_43.orders(id serial PRIMARY KEY, note text);
         CREATE SEQUENCE bucket_43.seq_standalone;
         INSERT INTO bucket_43.orders(note) SELECT 'row'||g FROM generate_series(1,50) g;"
  if [ "$(h1 -c "SELECT count(*) FROM pg_roles WHERE rolname='app_role'")" = "0" ]; then
    h1 -c "CREATE ROLE app_role LOGIN;"
  fi
  if [ "$(h2 -c "SELECT count(*) FROM pg_roles WHERE rolname='app_role'")" = "0" ]; then
    h2 -c "CREATE ROLE app_role LOGIN;"
  fi
  h1 -c "GRANT USAGE ON SCHEMA bucket_43 TO app_role;
         GRANT INSERT,UPDATE,DELETE,TRUNCATE ON ALL TABLES IN SCHEMA bucket_43 TO app_role;
         GRANT USAGE,UPDATE ON ALL SEQUENCES IN SCHEMA bucket_43 TO app_role;
         GRANT CREATE ON SCHEMA bucket_43 TO app_role;"
  # артефакты переезда: публикация на s1, DDL + живая подписка на s2.
  # Базовые гранты app-роли — на ОБОИХ шардах (§4 доки 11): USAGE на схему
  # заморозка не отбирает, он должен быть у приёмника заранее.
  h1 -c "CREATE PUBLICATION pub_bucket_43 FOR TABLES IN SCHEMA bucket_43;"
  h2 -c "CREATE SCHEMA bucket_43;
         CREATE TABLE bucket_43.orders(id serial PRIMARY KEY, note text);
         CREATE SEQUENCE bucket_43.seq_standalone;"
  h2 -c "GRANT USAGE ON SCHEMA bucket_43 TO app_role;
         GRANT INSERT,UPDATE,DELETE,TRUNCATE ON ALL TABLES IN SCHEMA bucket_43 TO app_role;
         GRANT USAGE,UPDATE ON ALL SEQUENCES IN SCHEMA bucket_43 TO app_role;
         GRANT CREATE ON SCHEMA bucket_43 TO app_role;"
  h2 -c "CREATE SUBSCRIPTION sub_bucket_43
           CONNECTION 'host=hap1 port=5432 dbname=postgres user=postgres'
           PUBLICATION pub_bucket_43 WITH (copy_data=true, failover=true);"
  until [ "$(h2 -c "SELECT count(*) FROM pg_subscription_rel r
                    JOIN pg_subscription s ON s.oid=r.srsubid
                    WHERE s.subname='sub_bucket_43' AND r.srsubstate='r'")" = "1" ]; do
    sleep 1
  done
  # cutover сорвался на заморозке: REVOKE P1 (DML+sequences) и мораторий P5
  h1 -c "REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON ALL TABLES IN SCHEMA bucket_43 FROM app_role;
         REVOKE USAGE,UPDATE ON ALL SEQUENCES IN SCHEMA bucket_43 FROM app_role;
         REVOKE CREATE ON SCHEMA bucket_43 FROM app_role;"
  [ "$(h1 -c "SELECT count(*) FROM bucket_43.orders")" = "50" ]
}

# ═══ Фаза 1: зависший FROZEN, переключение НЕ произошло ═════════════════════

echo ">>> Фаза 1 / Arrange: зависший FROZEN bucket_43 (s1 → s2), routing=s1"
arrange_stuck_move
echo "  initial copy готов, подписка стримит, s1 заморожен"
# etcd: routing + зависший статус (обновлён час назад — mover точно мёртв)
ect put /clusters/legacy/buckets/routing/bucket_43 s1 >/dev/null
ect put /clusters/legacy/buckets/status/bucket_43 "{\"state\":\"FROZEN\",\"target\":\"s2\",\"updated_unix\":$(( $(date +%s) - 3600 ))}" >/dev/null

# ── Act/Assert: list видит зависший переезд ─────────────────────────────────
echo ">>> abort-move.sh list: bucket_43 = FROZEN"
abort list >logs/60-list.log 2>&1
grep -q "bucket_43.*FROZEN" logs/60-list.log \
  || { echo "❌ list не показал зависший переезд"; cat logs/60-list.log; exit 1; }

# ── Assert: отказы-защиты ────────────────────────────────────────────────────
echo ">>> отказы: ACTIVE-бакет (нет статуса) и свежий статус (mover жив?)"
ect put /clusters/legacy/buckets/routing/bucket_42 s1 >/dev/null
if abort abort bucket_42 --yes 2>logs/60-refuse1.log; then
  echo "❌ abort должен отказаться: бакет ACTIVE (нет статус-ключа)"; exit 1
fi
grep -q "статус-ключа" logs/60-refuse1.log || { echo "❌ неожиданный отказ:"; cat logs/60-refuse1.log; exit 1; }
ect del /clusters/legacy/buckets/routing/bucket_42 >/dev/null
ect put /clusters/legacy/buckets/routing/bucket_44 s1 >/dev/null
ect put /clusters/legacy/buckets/status/bucket_44 "{\"state\":\"SYNCING\",\"target\":\"s2\",\"updated_unix\":$(date +%s)}" >/dev/null
if abort abort bucket_44 --yes 2>logs/60-refuse2.log; then
  echo "❌ abort должен отказаться: статус свежий, mover может быть жив"; exit 1
fi
grep -q "ещё жив" logs/60-refuse2.log || { echo "❌ неожиданный отказ:"; cat logs/60-refuse2.log; exit 1; }
ect del /clusters/legacy/buckets/status/bucket_44 >/dev/null
ect del /clusters/legacy/buckets/routing/bucket_44 >/dev/null
echo "  ✓ оба отказа корректны"

# ── Act: крах уборки — приёмник недоступен ──────────────────────────────────
echo ">>> s2 недоступен → abort пишет журнал (phase=blocked) и НЕ трогает БД"
docker stop s2a > /dev/null
if abort abort bucket_43 --yes >logs/60-abort-blocked.log 2>&1; then
  echo "❌ abort должен был споткнуться о недоступный s2"; docker start s2a >/dev/null; exit 1
fi
grep -v "Container\|Creating\|Created" logs/60-abort-blocked.log | sed 's/^/  /'
# Assert: журнал в etcd УЖЕ есть, а в БД ещё НИЧЕГО не тронуто —
# журнал строго ДО манипуляций
j="$(ect get /clusters/legacy/buckets/status/bucket_43 --print-value-only)"
echo "  журнал: $j"
echo "$j" | jq -e '.state=="ABORTING" and .phase=="blocked" and (.unreachable_shards|index("s2"))!=null' >/dev/null \
  || { echo "❌ журнал blocked не записан/не так записан"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_publication WHERE pubname='pub_bucket_43'")" = "1" ] \
  || { echo "❌ публикация исчезла ДО разрешения убирать — манипуляции без журнала?!"; exit 1; }
grep -q "журнал записан" logs/60-abort-blocked.log \
  || { echo "❌ в выводе нет записи журнала"; exit 1; }
echo "  ✓ журнал в etcd есть, БД не тронута"

# ── Act: s2 вернулся → повторный abort продолжает (resume по журналу) ────────
echo ">>> s2 вернулся → повторный abort дочищает"
docker start s2a >/dev/null
# сайдкар не переподключается к новому netns перезапущенной ноды — рестартуем
docker restart hc2a >/dev/null 2>&1 || true
until docker exec s2a pg_isready -U postgres -q; do sleep 1; done
# abort ходит через hap2 — ждём, пока HAProxy снова видит живой бэкенд (health-check)
until [ "$(h2 -c 'SELECT 1' 2>/dev/null)" = "1" ]; do sleep 1; done
abort abort bucket_43 --yes 2>&1 | tee logs/60-abort-resume.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
grep -q "продолжаю" logs/60-abort-resume.log || { echo "❌ resume не увидел журнал ABORTING"; exit 1; }

# ── Assert: откат к старому владельцу, чисто ────────────────────────────────
echo ">>> Фаза 1 / Assert: артефактов нет, данные целы, заморозка снята, etcd = ACTIVE на s1"
[ "$(h1 -c "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pub_bucket_43%'")" = "0" ] || { echo "❌ публикация на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_43%'")" = "0" ] || { echo "❌ слот на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_43'")" = "1" ] || { echo "❌ схема владельца пропала с s1!"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_subscription WHERE subname LIKE 'sub_bucket_43%'")" = "0" ] || { echo "❌ подписка на s2"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_43'")" = "0" ] || { echo "❌ схема-копия осталась на s2"; exit 1; }
# заморозка P1 снята: app-роль снова пишет; мораторий P5 снят: CREATE работает
h1a -c "INSERT INTO bucket_43.orders(note) VALUES ('after-abort');" \
  || { echo "❌ app_role не пишет на s1 после abort"; exit 1; }
h1a -c "CREATE TABLE bucket_43.tmp_app_ddl(id int);" \
  || { echo "❌ app_role не делает DDL на s1 после abort (P5)"; exit 1; }
h1 -c "DROP TABLE bucket_43.tmp_app_ddl;"
[ "$(h1 -c "SELECT count(*) FROM bucket_43.orders")" = "51" ] || { echo "❌ данные владельца повреждены"; exit 1; }
# etcd: статус-ключ удалён (нет ключа = ACTIVE), routing не изменился
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_43 --print-value-only)" ] || { echo "❌ статус-ключ не удалён"; exit 1; }
[ "$(ect get /clusters/legacy/buckets/routing/bucket_43 --print-value-only)" = "s1" ] || { echo "❌ routing изменился"; exit 1; }
echo "  ✓ фаза 1 зелёная: откат к ACTIVE у s1"

# ═══ Фаза 2: routing==target — переключение ПРОИЗОШЛО, статус завис ═════════

echo ">>> Фаза 2 / Arrange: то же в БД, но routing УЖЕ на s2 (flip прошёл, статус остался)"
arrange_stuck_move
ect put /clusters/legacy/buckets/routing/bucket_43 s2 >/dev/null
ect put /clusters/legacy/buckets/status/bucket_43 "{\"state\":\"FROZEN\",\"target\":\"s2\",\"updated_unix\":$(( $(date +%s) - 3600 ))}" >/dev/null
[ "$(h2 -c "SELECT count(*) FROM bucket_43.orders")" = "50" ] \
  || { echo "❌ arrange: копия на s2 не догнала"; exit 1; }
echo "  etcd: routing=s2 (flip прошёл), status=FROZEN (завис)"

# ── Act/Assert: без --force — отказ, НЕ трогая ни БД, ни etcd ────────────────
echo ">>> abort БЕЗ --force: должен отказаться («flip прошёл») и ничего не тронуть"
if abort abort bucket_43 --yes >logs/60-refuse3.log 2>&1; then
  echo "❌ abort не отказался на routing==target"; exit 1
fi
grep -q "flip прошёл" logs/60-refuse3.log \
  || { echo "❌ неожиданный отказ:"; cat logs/60-refuse3.log; exit 1; }
ect get /clusters/legacy/buckets/status/bucket_43 --print-value-only | grep -q FROZEN \
  || { echo "❌ статус-ключ тронут при отказе"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_publication WHERE pubname='pub_bucket_43'")" = "1" ] \
  || { echo "❌ БД тронута при отказе"; exit 1; }
echo "  ✓ отказ без каких-либо действий"

# ── Act: с --force — ДОВЕСТИ перевод до конца ───────────────────────────────
echo ">>> abort --force: довести перевод (владелец s2 не трогается, s1 вычищается)"
abort abort bucket_43 --yes --force 2>&1 | tee logs/60-abort-force.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'

# ── Assert: перевод доведён, владелец — s2 ──────────────────────────────────
echo ">>> Фаза 2 / Assert: s2 — владелец с данными, s1 вычищен, etcd = ACTIVE на s2"
[ "$(h2 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_43'")" = "1" ] || { echo "❌ схема нового владельца пропала"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM bucket_43.orders")" = "50" ] || { echo "❌ данные нового владельца повреждены"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_subscription WHERE subname LIKE 'sub_bucket_43%'")" = "0" ] || { echo "❌ подписка на s2"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pub_bucket_43%'")" = "0" ] || { echo "❌ публикация на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_43%'")" = "0" ] || { echo "❌ слот на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_43'")" = "0" ] || { echo "❌ схема-копия осталась на s1"; exit 1; }
# unfreeze отработал на НОВОМ владельце: app_role пишет и делает DDL на s2
h2a -c "INSERT INTO bucket_43.orders(note) VALUES ('after-force-abort');" \
  || { echo "❌ app_role не пишет на s2 после abort --force"; exit 1; }
h2a -c "CREATE TABLE bucket_43.tmp_app_ddl(id int);" \
  || { echo "❌ app_role не делает DDL на s2 после abort --force (P5)"; exit 1; }
h2 -c "DROP TABLE bucket_43.tmp_app_ddl;"
[ "$(h2 -c "SELECT count(*) FROM bucket_43.orders")" = "51" ] || { echo "❌ данные нового владельца повреждены"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_43 --print-value-only)" ] || { echo "❌ статус-ключ не удалён"; exit 1; }
[ "$(ect get /clusters/legacy/buckets/routing/bucket_43 --print-value-only)" = "s2" ] || { echo "❌ routing изменился"; exit 1; }
abort list >logs/60-list-final.log 2>&1
if grep -q "bucket_43" logs/60-list-final.log; then
  echo "❌ list всё ещё видит bucket_43"; cat logs/60-list-final.log; exit 1
fi
echo "✓ P7 подтверждён: фаза 1 — журнал до манипуляций, resume, откат в ACTIVE у s1;"
echo "                 фаза 2 — routing==target: отказ без --force, --force доводит перевод (ACTIVE на s2)"
