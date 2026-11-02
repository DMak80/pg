#!/bin/sh
# pgworker-node: supervisord-обёртка Spilo (arch/14 §2.1, arch/19 §3.5).
# Апстримный /launch.sh ставит `runsvdir -P` — каждый runsv в СВОЕЙ группе
# процессов, групповой stop/kill supervisord'а (stopasgroup/killasgroup) их
# не достаёт: сироты держат supervise/lock, и Patroni следующего запуска не
# стартует никогда («runsv patroni: fatal: unable to lock supervise/lock» —
# restore-операция останавливает/возвращает Patroni ноды через supervisorctl).
# Обёртка гасит runit-дерево сама: TERM → штатный sv-stop (patroni — он
# останавливает свой постгрес; бюджет до окна supervisord'а), затем точечный
# добор runsv/runsvdir — лок освобождён, `start spilo` поднимает Patroni чисто.
# Доставка секретов (arch/14 §4): секретные env ПРОЦЕССА — из tmpfs-файлов
# агента PgNode; файлов нет (доставка не выполнена) — немедленный отказ
# старта: supervisord старт-отказы не лупит, воркер повторит доставкой.
for f in /run/pgw/spilo-env /run/pgw/spilo-configuration.yml /run/pgw/rest-cert.pem /run/pgw/rest-key.pem; do
    if [ ! -s "$f" ]; then
        echo "spilo: $f is missing - waiting for secret delivery from the worker (pgnode bootstrap)" >&2
        exit 1
    fi
done
set -a
. /run/pgw/spilo-env
set +a
export SPILO_CONFIGURATION="$(cat /run/pgw/spilo-configuration.yml)"
export SSL_RESTAPI_CERTIFICATE="$(cat /run/pgw/rest-cert.pem)"
export SSL_RESTAPI_PRIVATE_KEY="$(cat /run/pgw/rest-key.pem)"

/launch.sh &
MAIN=$!
on_term() {
    kill -TERM "$MAIN" 2>/dev/null
    sv -w 5 stop patroni 2>/dev/null
    sv -w 2 stop /etc/service/* 2>/dev/null
    pkill -x runsv 2>/dev/null
    pkill -x runsvdir 2>/dev/null
    exit 0
}
trap on_term TERM INT QUIT
wait "$MAIN"
