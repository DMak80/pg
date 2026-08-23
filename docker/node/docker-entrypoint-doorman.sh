#!/bin/sh
# Условный запуск pg_doorman: бинарник ставится при сборке только с DOORMAN_URL
# (R1); без бинарника или конфига — тихий выход (supervisord не перезапускает).
if [ ! -x /usr/local/bin/pg_doorman ] || [ ! -s /etc/pg_doorman/pg_doorman.ini ]; then
    echo "pg_doorman: бинарник/конфиг отсутствуют — не запускаемся"
    exit 0
fi
exec /usr/local/bin/pg_doorman -c /etc/pg_doorman/pg_doorman.ini
