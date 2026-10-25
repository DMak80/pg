# hasync — синкер топологии: etcd (/cluster/nodes/*) → HAProxy runtime API.
#
# HAProxy резолвит бэкенды только при своём старте — поэтому в конфиге
# init-addr none, а ЕДИНСТВЕННЫЙ источник адресов вот здесь, из etcd:
#   ключ есть    → проверить GET http://ip:8008/whoami (сайдкар отдаёт имя
#                  ноды) и применить `set server ... addr` только при совпа-
#                  дении — docker переиспользует освободившиеся IP, и без
#                  проверки чужая нода стала бы «мастером» чужого шарда;
#   ключа нет    → регистрация умерла вместе с нодой (lease TTL): после
#                  MISSING_GRACE циклов подряд адрес уводится в 127.0.0.1 —
#                  health-check честно роняет сервер, ротация прекращается;
#   etcd недост. → последнее исключение: НИЧЕГО не менять (P9: отказ
#                  контрол-плейна не должен выламывать дата-плейн).
# Живость ноды решает health-check HAProxy (сайдкар /primary) — не etcd.
import base64
import json
import os
import ipaddress
import http.client
import socket
import time
from urllib.parse import urlsplit

ETCD = os.getenv("ETCD_ENDPOINTS", "http://etcd:2379").rstrip("/")
BACKEND = os.environ["BACKEND"]                 # напр. bk_master
SERVERS = os.environ["SERVERS"].split(",")      # отслеживаемые бэкенды
SOCK = os.getenv("HAPROXY_SOCKET", "/run/haproxy/admin.sock")
WHOAMI_PORT = int(os.getenv("WHOAMI_PORT", "8008"))
PORT = os.getenv("SERVER_PORT", "5432")
INTERVAL = float(os.getenv("INTERVAL", "2"))
MISSING_GRACE = int(os.getenv("MISSING_GRACE", "3"))  # циклов пусто → 127.0.0.1
RESYNC_CYCLES = int(os.getenv("RESYNC_CYCLES", "30"))  # периодический полный ре-сет

state = {}    # server → последний успешно применённый адрес
missing = {}  # server → сколько циклов подряд ключа не было


def b64(s):
    return base64.b64encode(s.encode()).decode()


def etcd_get(key):
    parsed = urlsplit(ETCD)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("invalid etcd endpoint scheme/host")
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parsed.hostname, parsed.port, timeout=5)
    try:
        conn.request("POST", parsed.path + "/v3/kv/range", body=json.dumps({"key": b64(key)}), headers={"Content-Type": "application/json"})
        with conn.getresponse() as r:
            kvs = json.load(r).get("kvs") or []
    finally:
        conn.close()
    return base64.b64decode(kvs[0]["value"]).decode() if kvs else None


def hap(cmd):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(5)
        s.connect(SOCK)
        s.sendall((cmd + "\n").encode())
        try:
            return s.recv(65536).decode(errors="replace")
        except socket.timeout:
            return ""


def whoami(ip):
    ipaddress.ip_address(ip)  # только IP-литерал из etcd
    conn = http.client.HTTPConnection(ip, WHOAMI_PORT, timeout=3)
    try:
        conn.request("GET", "/whoami")
        with conn.getresponse() as r:
            return r.read().decode().strip()
    finally:
        conn.close()


def apply_addr(srv, ip, why=""):
    out = hap(f"set server {BACKEND}/{srv} addr {ip} port {PORT}")
    # успех: пустой ответ, идемпотентный «no need to change...» или
    # «IP changed from ... to ...»; остальное — ошибка runtime-команды
    ok = (not out.strip()) or ("no need to change" in out) or out.lstrip().startswith("IP changed")
    if not ok:
        print(f"{srv}: ⚠️ runtime replied: {out.strip()}", flush=True)
        return False
    state[srv] = ip
    if not out.strip():
        print(f"{srv} → {ip}:{PORT}{why}", flush=True)
    return True


cycle = 0
while True:
    if cycle % RESYNC_CYCLES == 0:
        # HAProxy мог перезапуститься (runtime сбрасывается к конфигу) —
        # принудительно перевыставим все адреса
        state.clear()
    cycle += 1
    for srv in SERVERS:
        try:
            ip = etcd_get("/cluster/nodes/" + srv)
        except Exception as e:
            # etcd недоступен: держим последние применённые адреса (P9)
            print(f"{srv}: etcd unreachable ({e}) — keeping the last address", flush=True)
            missing[srv] = 0
            continue

        if ip is None:
            missing[srv] = missing.get(srv, 0) + 1
            # ключ исчез (lease истёк = нода мертва): после grace уводим адрес
            # в 127.0.0.1 — health-check роняет сервер, чужие запросы не идут
            if missing[srv] >= MISSING_GRACE and state.get(srv) != "127.0.0.1":
                if apply_addr(srv, "127.0.0.1", " (registration vanished — node is dead)"):
                    missing[srv] = MISSING_GRACE
            continue

        missing[srv] = 0
        if ip == state.get(srv):
            continue
        try:
            name = whoami(ip)
        except Exception as e:
            print(f"{srv}: {ip} does not answer /whoami ({e}) — not applying the address", flush=True)
            continue
        if name != srv:
            # анти-коллизия: по этому адресу живёт ДРУГАЯ нода (docker пере-
            # использовал освободившийся IP) — не подставляем её вместо srv
            print(f"{srv}: {ip} answers with name '{name}' — that is not {srv}, not applying the address", flush=True)
            continue
        try:
            apply_addr(srv, ip)
        except Exception as e:
            print(f"{srv}: HAProxy runtime unreachable ({e}) — retrying", flush=True)
            state.clear()
            break
    time.sleep(INTERVAL)
