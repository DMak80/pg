#!/usr/bin/env python3
# lease-скрипт мастер-ключа (P11, arch/14 §2.1): Patroni callback on_role_change.
# Вызывается Patroni при смене роли ноды; аргументы включают новую роль:
#   role=master  → демон: lease TTL 5с + put PGW_MASTER_KEY=<host>:<doormanPort>,
#                  keepalive 1с (в 5 раз чаще TTL — переживает 2–3 потери);
#   role=replica/standby_leader → погасить демон (ключ исчезнет по TTL сам).
# Адаптация arch/stand/sidecar/rolecheck.py (etcd-примитивы идентичны).
import base64
import json
import os
import sys
import time
import urllib.request

# Patroni-callback стартует с обрезанным runit'ом env: параметры дублируются
# entrypoint'ом в файл (env-переменные имеют приоритет над файлом).
ENV_FILE = "/home/postgres/pgw-node.env"

# Patroni 3.x называет мастера по-разному в зависимости от контекста.
MASTER_ROLES = {"master", "leader", "primary"}
REPLICA_ROLES = {"replica", "standby_leader"}


def load_env_file():
    try:
        with open(ENV_FILE) as f:
            for line in f:
                name, sep, value = line.strip().partition("=")
                if sep and name:
                    os.environ.setdefault(name, value)
    except OSError:
        pass  # файла нет — ручной запуск, остаёмся на env


def local_role():
    """Фактическая роль этой ноды: GET /primary локального Patroni (для
    on_start Patroni не передаёт роль аргументами)."""
    try:
        req = urllib.request.Request("http://127.0.0.1:8008/primary")
        with urllib.request.urlopen(req, timeout=3) as r:
            return "master" if r.status == 200 else "replica"
    except Exception:
        return None  # Patroni недоступен — ничего не делаем


load_env_file()

ETCD = os.getenv("PGW_ETCD", "http://etcd:2379")
KEY = os.getenv("PGW_MASTER_KEY", "")
HOST = os.getenv("PGW_NODE_HOST", "")
DOORMAN_PORT = os.getenv("PGW_DOORMAN_PORT", "6432")
LEASE_TTL = 5      # ключ гаснет ≤5с после смерти/смены роли ноды (P11)
KEEPALIVE_SEC = 1  # продлеваем lease в 5 раз чаще TTL
PID_FILE = "/tmp/master-lease.pid"
MASTER_ROLES = {"master", "primary", "master}", "primary}"}


def etcd_post(path, payload):
    req = urllib.request.Request(
        ETCD + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def etcd_put_leased(key, value, lease_id):
    # gateway etcd 3.5 принимает lease как ДЕСЯТИЧНУЮ строку (не hex)
    etcd_post("/v3/kv/put", {
        "key": base64.b64encode(key.encode()).decode(),
        "value": base64.b64encode(value.encode()).decode(),
        "lease": str(lease_id),
    })


def lease_grant():
    return int(etcd_post("/v3/lease/grant", {"TTL": LEASE_TTL})["ID"])


def lease_keepalive(lease_id):
    etcd_post("/v3/lease/keepalive", {"ID": str(lease_id)})


def stop_running_daemon():
    """Гасим демон предыдущего мастера (PID-файл), если жив."""
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        os.kill(pid, 15)
        for _ in range(20):
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.1)
    except (OSError, ValueError):
        pass  # демона нет (первый запуск/уже погашен)


def lease_loop():
    """Демон мастера: put мастер-ключа под lease + keepalive."""
    lease_id = None
    first = True
    while True:
        try:
            if lease_id is None:
                lease_id = lease_grant()
            etcd_put_leased(KEY, f"{HOST}:{DOORMAN_PORT}", lease_id)
            if first:
                first = False
                print(f"master-lease: {KEY} = {HOST}:{DOORMAN_PORT} (ttl={LEASE_TTL}s)", flush=True)
        except Exception:
            lease_id = None  # lease истёк/etcd недоступен — пересоздать
        try:
            if lease_id is not None:
                lease_keepalive(lease_id)
        except Exception:
            lease_id = None
        time.sleep(KEEPALIVE_SEC)


def main():
    if not KEY or not HOST:
        print("master-lease: PGW_MASTER_KEY/PGW_NODE_HOST не заданы — выходим", flush=True)
        return

    # Новая роль приходит аргументами callback (master/leader/replica); для
    # on_start Patroni роль НЕ передаёт — узнаём фактическую сами.
    args = {a.strip().lower() for a in sys.argv[1:]}
    if args & REPLICA_ROLES:
        is_master = False
    elif args & MASTER_ROLES:
        is_master = True
    else:
        role = local_role()
        if role is None:
            print("master-lease: роль не передана и Patroni недоступен — выходим", flush=True)
            return
        is_master = role == "master"

    stop_running_daemon()

    if not is_master:
        # Реплика не держит мастер-ключ: ключ старого lease истечёт сам ≤TTL.
        print("master-lease: роль не мастер — демон погашен", flush=True)
        return

    pid = os.fork()
    if pid > 0:
        with open(PID_FILE, "w") as f:
            f.write(str(pid))
        return

    # Ребёнок: отвязываемся от Patroni-процесса и держим lease до смены роли.
    os.setsid()
    sys.stdout.flush()
    lease_loop()


if __name__ == "__main__":
    main()
