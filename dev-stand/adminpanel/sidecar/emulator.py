# Patroni-эмулятор dev-стенда AdminPanel (spec t10 §5): REST :8008 + etcd-lease.
# Развитие ../pg/arch/stand/sidecar/rolecheck.py (HTTP-основа, gateway-паттерн,
# lease-механика); отличия: /cluster в формате Patroni по составу MEMBERS,
# master-lease шардового ключа + leader/optime, регистрация только при живой PG.
import base64
import json
import os
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pg8000.native

PG_PORT = 5432
PG_USER = "postgres"
PG_DB = "postgres"
ETCD = os.getenv("ETCD_ENDPOINTS", "http://etcd:2379").rstrip("/")
NODE = os.getenv("NODE_NAME", "")
CLUSTER = os.getenv("CLUSTER", "demo")
SHARD = os.getenv("SHARD", "s1")
MEMBERS = [m.strip() for m in os.getenv("MEMBERS", NODE).split(",") if m.strip()]
LEASE_TTL = 5  # ключи гаснут <=5 c после смерти ноды (как Patroni TTL; arch/02 §2.1)
STEP_SEC = 1   # цикл опроса/продления в 5 раз чаще TTL (паттерн rolecheck)
SCOPE = f"{CLUSTER}-{SHARD}"

# Снимок членов scope: name -> {alive, role, state, timeline, lag}
state = {}
state_lock = threading.Lock()
last_role = {m: "replica" for m in MEMBERS}  # последняя известная роль (для stopped)


# ---------- опрос PG (spec §5.1) ----------
def probe_node(host):
    con = pg8000.native.Connection(host=host, port=PG_PORT, user=PG_USER, database=PG_DB)
    try:
        try:
            # Поле контроль-точки в PG18 называется timeline_id (не timeline)
            inrec, timeline, lag = con.run(
                "select pg_is_in_recovery(), (pg_control_checkpoint()).timeline_id,"
                " coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0)"
            )[0]
        except Exception:
            # Фолбэк spec §5.1: сбой enrichment-полей не должен гасить ноду
            # (и её lease) — роль узнаём минимальным запросом, timeline/lag
            # дефолтны. Падение и его = PG реально недоступна -> исключение
            # наружу, poll_loop пометит ноду stopped.
            inrec = con.run("select pg_is_in_recovery()")[0][0]
            timeline, lag = 1, 0
        inrec = bool(inrec)
        return {
            "alive": True,
            "role": "replica" if inrec else "master",
            "state": "streaming" if inrec else "running",
            "timeline": int(timeline or 1),
            "lag": int(lag or 0),
        }
    finally:
        con.close()


def optime_lsn(host):
    # LSN мастера числом-строкой для optime/leader (формат как у EtcdSeed)
    con = pg8000.native.Connection(host=host, port=PG_PORT, user=PG_USER, database=PG_DB)
    try:
        return str(con.run("select (pg_current_wal_lsn() - '0/0')")[0][0])
    finally:
        con.close()


def node_ip(host):
    # IP своей PG-ноды в сети стенда: DNS-resolve (эмулятор — отдельный
    # контейнер, сокет-приём rolecheck дал бы адрес эмулятора; spec §5.3)
    return socket.gethostbyname(host)


# ---------- etcd gateway (паттерн rolecheck.py) ----------
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


# ---------- цикл: опрос членов + регистрация себя (spec §5.3) ----------
def poll_loop():
    lease_id = None
    prev_role = None
    while True:
        snap = {}
        for name in MEMBERS:
            try:
                snap[name] = probe_node(name)
                last_role[name] = snap[name]["role"]
            except Exception:
                snap[name] = {"alive": False, "role": last_role[name],
                              "state": "stopped", "timeline": None, "lag": None}
        with state_lock:
            state.clear()
            state.update(snap)

        own = snap.get(NODE)
        # Регистрация/продление — только пока своя PG отвечает: смерть ноды
        # убирает ключи через TTL <=5 c, как у Patroni (spec §5.3)
        if own is not None and own["alive"]:
            try:
                if lease_id is None:
                    lease_id = lease_grant()
                etcd_put_leased(f"/service/{SCOPE}/members/{NODE}", json.dumps({
                    "name": NODE, "conn_url": f"{NODE}:5432",
                    "role": own["role"], "state": own["state"],
                    "timeline": own["timeline"], "lag": own["lag"],
                }), lease_id)
                etcd_put_leased(f"/cluster/nodes/{NODE}", node_ip(NODE), lease_id)
                if own["role"] == "master":
                    etcd_put_leased(f"/clusters/{CLUSTER}/shards/{SHARD}/master",
                                    f"{NODE}:5432", lease_id)
                    etcd_put_leased(f"/service/{SCOPE}/leader",
                                    json.dumps({"name": NODE}), lease_id)
                    etcd_put_leased(f"/service/{SCOPE}/optime/leader",
                                    optime_lsn(NODE), lease_id)
                lease_keepalive(lease_id)
                if prev_role != own["role"]:
                    print(f"{NODE}: role {prev_role} -> {own['role']}", flush=True)
                    prev_role = own["role"]
            except Exception:
                lease_id = None  # lease истёк/etcd недоступен — пересоздать
        time.sleep(STEP_SEC)


# ---------- REST :8008 (spec §5.2) ----------
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/cluster":
            # Всегда 200, пока жив эмулятор: полный состав MEMBERS, мёртвые —
            # stopped (Patroni ведёт себя так; панели нужна запись по имени)
            with state_lock:
                members = [
                    {"name": n, "role": s["role"], "state": s["state"],
                     "timeline": s["timeline"], "lag": s["lag"],
                     "host": n, "port": PG_PORT}
                    for n, s in sorted(state.items())
                ]
            self._send(200, json.dumps({"members": members}).encode(),
                       content_type="application/json")
            return
        if self.path == "/metrics":
            # §2.5 + ревью Ф4-7: ТОЛЬКО своя нода (NODE_NAME) — экспорт всех членов
            # scope с каждого инстанса дублировал бы серии при scrape всех hc*.
            # Мастер: lag=0 (running); реплика: receive-replay diff (state c lock, S6).
            with state_lock:
                own = state.get(NODE)
            lines = [
                "# HELP pg_replica_lag_seconds replication lag of the node (emulator)",
                "# TYPE pg_replica_lag_seconds gauge",
            ]
            if own is not None and own["alive"]:
                lag = 0 if own["role"] == "master" else (own["lag"] or 0)
                lines.append(f'pg_replica_lag_seconds{{scope="{SCOPE}",node="{NODE}"}} {int(lag)}')
            self._send(200, ("\n".join(lines) + "\n").encode())
            return
        with state_lock:
            own = state.get(NODE)
        if own is None or not own["alive"]:
            self._send(503, b"pg unreachable\n")
            return
        if self.path in ("/", "/read-only"):
            ok = True
        elif self.path == "/primary":
            ok = own["role"] == "master"
        elif self.path == "/replica":
            ok = own["role"] == "replica"
        else:
            self._send(404, b"not found\n")
            return
        self._send(200 if ok else 503, b"OK\n")

    def _send(self, code, body, content_type="text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # не шумим в логи контейнера


print(f"{NODE}: эмулятор scope {SCOPE}, members={MEMBERS}", flush=True)
threading.Thread(target=poll_loop, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8008), Handler).serve_forever()
