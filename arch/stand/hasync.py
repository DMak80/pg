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
import socket
import time
import urllib.request

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
    req = urllib.request.Request(
        ETCD + "/v3/kv/range",
        data=json.dumps({"key": b64(key)}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        kvs = json.load(r).get("kvs") or []
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
    with urllib.request.urlopen(f"http://{ip}:{WHOAMI_PORT}/whoami", timeout=3) as r:
        return r.read().decode().strip()


def apply_addr(srv, ip, why=""):
    out = hap(f"set server {BACKEND}/{srv} addr {ip} port {PORT}")
    # успех: пустой ответ, идемпотентный «no need to change...» или
    # «IP changed from ... to ...»; остальное — ошибка runtime-команды
    ok = (not out.strip()) or ("no need to change" in out) or out.lstrip().startswith("IP changed")
    if not ok:
        print(f"{srv}: ⚠️ runtime ответил: {out.strip()}", flush=True)
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
            print(f"{srv}: etcd недоступен ({e}) — держу последний адрес", flush=True)
            missing[srv] = 0
            continue

        if ip is None:
            missing[srv] = missing.get(srv, 0) + 1
            # ключ исчез (lease истёк = нода мертва): после grace уводим адрес
            # в 127.0.0.1 — health-check роняет сервер, чужие запросы не идут
            if missing[srv] >= MISSING_GRACE and state.get(srv) != "127.0.0.1":
                if apply_addr(srv, "127.0.0.1", " (регистрация исчезла — нода мертва)"):
                    missing[srv] = MISSING_GRACE
            continue

        missing[srv] = 0
        if ip == state.get(srv):
            continue
        try:
            name = whoami(ip)
        except Exception as e:
            print(f"{srv}: {ip} не отвечает /whoami ({e}) — адрес не применяю", flush=True)
            continue
        if name != srv:
            # анти-коллизия: по этому адресу живёт ДРУГАЯ нода (docker пере-
            # использовал освободившийся IP) — не подставляем её вместо srv
            print(f"{srv}: {ip} отвечает именем '{name}' — это не {srv}, адрес не применяю", flush=True)
            continue
        try:
            apply_addr(srv, ip)
        except Exception as e:
            print(f"{srv}: HAProxy runtime недоступен ({e}) — повтор", flush=True)
            state.clear()
            break
    time.sleep(INTERVAL)
