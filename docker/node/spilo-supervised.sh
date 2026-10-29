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
