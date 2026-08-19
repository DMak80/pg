# Сайдкар-эмуляция Patroni REST API для HAProxy (httpchk GET /primary).
# Живёт в netns PG-ноды, опрашивает её 127.0.0.1:5432 и отдаёт:
#   /         -> 200, если PG отвечает
#   /primary  -> 200 только если нода НЕ в recovery (мастер)
#   /replica  -> 200 только если нода в recovery
#   /read-only -> 200, если PG отвечает
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pg8000.native

PG = dict(
    host=os.getenv("PGHOST", "127.0.0.1"),
    port=int(os.getenv("PGPORT", "5432")),
    user=os.getenv("PGUSER", "postgres"),
    database=os.getenv("PGDATABASE", "postgres"),
)


def is_in_recovery():
    con = pg8000.native.Connection(**PG)
    try:
        return bool(con.run("SELECT pg_is_in_recovery()")[0][0])
    finally:
        con.close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
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


ThreadingHTTPServer(("0.0.0.0", 8008), Handler).serve_forever()
