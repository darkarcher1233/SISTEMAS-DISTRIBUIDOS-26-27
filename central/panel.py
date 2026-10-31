"""
panel.py: panel WEB de monitorización de CENTRAL (solo librería estándar)
Responsable: Vlad · Tarjeta Trello: "[Vlad] Panel de CENTRAL"
Lo usa: central/WM_Central.py -> iniciar_panel(estaciones, lock, log_buffer)

CÓMO FUNCIONA
  - Un servidor HTTP (http.server.ThreadingHTTPServer) en un hilo daemon, en 0.0.0.0:<puerto>
  - GET /        -> central/panel.html (la página: HTML + CSS + un poco de JS)
  - GET /estado  -> JSON con el estado actual; la página lo pide cada 1 s con fetch()
        {"estaciones": [{"ws_id", "ubicacion", "estado", "caudal", "volumen", "operario"}],
         "log": [últimas 8 líneas, de la más antigua a la más nueva],
         "hora": "HH:MM:SS"}
  - El dict estaciones de Central se lee SOLO con su lock cogido y se devuelve una copia

PUERTO: variable de entorno PANEL_PORT (por defecto 8080). Abrir http://localhost:8080
Prueba sin Central ni Kafka: python -m tests.panel_demo
"""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RUTA_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel.html")
LINEAS_LOG = 8


def foto_estado(estaciones, lock, log_buffer):
    """Copia del estado para /estado. El dict solo se lee con el lock de Central cogido."""
    with lock:
        lista = [
            {
                "ws_id": ws_id,
                "ubicacion": ws.get("ubicacion", ""),
                "estado": ws.get("estado", ""),
                "caudal": ws.get("caudal", 0.0),
                "volumen": ws.get("volumen", 0.0),
                "operario": ws.get("operario"),
            }
            for ws_id, ws in sorted(estaciones.items())
        ]
    # list() de un deque se hace de una vez (sin soltar el GIL): no hace falta el lock
    log = list(log_buffer)[-LINEAS_LOG:]
    return {"estaciones": lista, "log": log, "hora": time.strftime("%H:%M:%S")}


class _Manejador(BaseHTTPRequestHandler):
    """Atiende cada petición del navegador (un hilo por petición)."""

    def do_GET(self):
        ruta = self.path.split("?", 1)[0]
        if ruta in ("/", "/index.html"):
            try:
                with open(RUTA_HTML, "rb") as f:
                    self._responder(200, "text/html; charset=utf-8", f.read())
            except OSError:
                self._responder(500, "text/plain; charset=utf-8", b"No se encuentra panel.html")
        elif ruta == "/estado":
            srv = self.server
            datos = foto_estado(srv.estaciones, srv.lock, srv.log_buffer)
            cuerpo = json.dumps(datos, ensure_ascii=False).encode("utf-8")
            self._responder(200, "application/json; charset=utf-8", cuerpo)
        else:
            self._responder(404, "text/plain; charset=utf-8", b"No encontrado")

    def _responder(self, codigo, tipo, cuerpo):
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(cuerpo)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(cuerpo)

    def log_message(self, formato, *args):
        pass  # sin una línea por petición: no ensuciar la consola de Central


def iniciar_panel(estaciones, lock, log_buffer, puerto=None):
    """Arranca el panel web en un hilo daemon y devuelve el servidor (None si no se puede).

    estaciones = dict de Central {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario"}}
    lock       = el threading.Lock con el que Central protege estaciones
    log_buffer = collections.deque con las últimas líneas del log de Central
    puerto     = si no se da: variable de entorno PANEL_PORT, o 8080
    """
    if puerto is None:
        puerto = int(os.environ.get("PANEL_PORT", "8080"))
    try:
        servidor = ThreadingHTTPServer(("0.0.0.0", puerto), _Manejador)
    except OSError as e:
        print(f"[{time.strftime('%H:%M:%S')}] AVISO: panel web no disponible en el puerto {puerto}: {e}")
        return None
    servidor.daemon_threads = True
    servidor.estaciones = estaciones
    servidor.lock = lock
    servidor.log_buffer = log_buffer
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    print(f"[{time.strftime('%H:%M:%S')}] Panel web en http://localhost:{puerto}")
    return servidor
