# Сайдкар-эмуляция Patroni REST API для HAProxy (httpchk GET /primary).
# Живёт в netns PG-ноды, опрашивает её 127.0.0.1:5432 и отдаёт:
#   /         -> 200, если PG отвечает
#   /primary  -> 200 только если нода НЕ в recovery (мастер)
#   /replica  -> 200 только если нода в recovery
#   /read-only -> 200, если PG отвечает
#
# Дополнительно регистрирует адрес ноды в etcd-контрол-плейне (источник
# правды топологии стенда, стендовая инкарнация /shards/X/master из
# 12-bucket-pitfalls.md — в проде адрес пишет Patroni-Callback):
#   /cluster/nodes/<NODE_NAME> → <ip>   (lease TTL: ключ исчезает со смертью ноды)
# Регистрация с lease обязательна: docker переиспользует освободившиеся IP,
# и протухший ключ мёртвой ноды «накрыл» бы чужую ноду (hasync валидирует
# идентичность через GET /whoami — см. hasync.py).
import base64
import json
import os
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pg8000.native

PG = dict(
    host=os.getenv("PGHOST", "127.0.0.1"),
    port=int(os.getenv("PGPORT", "5432")),
    user=os.getenv("PGUSER", "postgres"),
    database=os.getenv("PGDATABASE", "postgres"),
)

ETCD = os.getenv("ETCD_ENDPOINTS", "http://etcd:2379").rstrip("/")
NODE_NAME = os.getenv("NODE_NAME", "")  # пусто = сайдкар без регистрации
LEASE_TTL = 15        # ключ живёт 15с после смерти ноды
KEEPALIVE_SEC = 5     # продлеваем lease втрое чаще TTL


def node_ip():
    # адрес ноды в сети стенда = локальный адрес этого (общего с нодой) netns
    u = urlparse(ETCD)
    s = socket.create_connection((u.hostname, u.port or 2379), timeout=5)
    try:
        return s.getsockname()[0]
    finally:
        s.close()


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


def register_loop():
    if not NODE_NAME:
        return
    key = "/cluster/nodes/" + NODE_NAME
    lease_id = None
    first = True
    while True:
        try:
            if lease_id is None:
                lease_id = lease_grant()
            etcd_put_leased(key, node_ip(), lease_id)
            if first:
                first = False
                print(f"registered {key} (lease ttl={LEASE_TTL}s)", flush=True)
        except Exception:
            lease_id = None  # lease мог истечь/etcd недоступен — пересоздать
        else:
            try:
                lease_keepalive(lease_id)
            except Exception:
                lease_id = None
        time.sleep(KEEPALIVE_SEC)


def is_in_recovery():
    con = pg8000.native.Connection(**PG)
    try:
        return bool(con.run("SELECT pg_is_in_recovery()")[0][0])
    finally:
        con.close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/whoami":
            # идентичность ноды: hasync применяет адрес из etcd только если
            # по этому адресу отвечает именем именно эта нода (анти-коллизия
            # переиспользованных docker'ом IP)
            self.send_response(200 if NODE_NAME else 404)
            self.end_headers()
            self.wfile.write(NODE_NAME.encode() + b"\n")
            return
        try:
            inrec = is_in_recovery()
        except Exception:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"pg unreachable\n")
            return
        if self.path in ("/", "/read-only"):
            ok = True
        elif self.path == "/primary":
            ok = not inrec
        elif self.path == "/replica":
            ok = bool(inrec)
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200 if ok else 503)
        self.end_headers()
        self.wfile.write(b"OK\n")

    def log_message(self, *args):
        pass  # не шумим в логи контейнера


threading.Thread(target=register_loop, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8008), Handler).serve_forever()
