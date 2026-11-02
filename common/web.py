"""
web.py: mini servidor web común para las pantallas de cada módulo (solo librería estándar)

Cada módulo (Central, Monitor, Engine, FO y el lanzador) arranca uno en un hilo daemon:

    iniciar_web("estacion.html", estado_fn, {"regar": fn, ...}, puerto_defecto=8200)

  - GET  /             -> ui/<pagina>
  - GET  /comun.css    -> ui/comun.css      (estilos compartidos)
  - GET  /comun.js     -> ui/comun.js       (funciones JS compartidas)
  - GET  /api/estado   -> JSON de estado_fn(); la página lo pide cada segundo
  - GET  /api/<x>?a=b  -> JSON de consultas[x]({"a": "b"})          (opcional)
  - POST /api/<x>      -> JSON de acciones[x](cuerpo_json)
        la acción devuelve un dict (por defecto {"ok": true});
        si lanza ValueError("motivo") se responde {"ok": false, "error": "motivo"}

PUERTO: variable de entorno WEB_PORT. Si no está, se prueba puerto_defecto y los siguientes
hasta encontrar uno libre (así varias WS en el mismo PC no chocan).

Registro: lista de eventos para el "Registro" de cada pantalla ({t, nivel, texto}).
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, urlsplit

RUTA_UI = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ui")
ESTATICOS = {"/comun.css": "text/css; charset=utf-8", "/comun.js": "text/javascript; charset=utf-8"}
NIVELES = ("INFO", "OK", "AVISO", "ALERTA")


class Registro:
    """Últimos eventos de un módulo, para pintarlos en su pantalla. Seguro entre hilos."""

    def __init__(self, maximo: int = 200):
        self._eventos = deque(maxlen=maximo)
        self._lock = threading.Lock()

    def add(self, texto: str, nivel: str = "INFO"):
        if nivel not in NIVELES:
            nivel = "INFO"
        with self._lock:
            self._eventos.append({"t": time.time(), "nivel": nivel, "texto": str(texto)})

    def lista(self, n: int = 80) -> list[dict]:
        """Los n más recientes, el más nuevo primero."""
        with self._lock:
            return list(self._eventos)[-n:][::-1]


class _Manejador(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        ruta = url.path
        srv = self.server
        if ruta in ("/", "/index.html"):
            self._fichero(srv.pagina, "text/html; charset=utf-8")
        elif ruta in ESTATICOS:
            self._fichero(ruta.lstrip("/"), ESTATICOS[ruta])
        elif ruta == "/api/estado":
            self._ejecutar(srv.estado_fn)
        elif ruta.startswith("/api/") and ruta[5:] in srv.consultas:
            parametros = dict(parse_qsl(url.query))
            self._ejecutar(lambda: srv.consultas[ruta[5:]](parametros))
        else:
            self._responder(404, "text/plain; charset=utf-8", b"No encontrado")

    def do_POST(self):
        ruta = urlsplit(self.path).path
        accion = self.server.acciones.get(ruta[5:]) if ruta.startswith("/api/") else None
        if accion is None:
            self._json(404, {"ok": False, "error": "Acción desconocida"})
            return
        try:
            largo = int(self.headers.get("Content-Length") or 0)
            cuerpo = json.loads(self.rfile.read(largo) or b"{}") if largo else {}
            if not isinstance(cuerpo, dict):
                raise ValueError("El cuerpo debe ser un objeto JSON")
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"ok": False, "error": f"Petición no válida: {exc}"})
            return
        self._ejecutar(lambda: accion(cuerpo))

    def _ejecutar(self, funcion):
        try:
            resultado = funcion()
        except ValueError as exc:   # error del usuario (datos mal, estado que no lo permite)
            self._json(400, {"ok": False, "error": str(exc)})
            return
        except Exception as exc:  # noqa: BLE001 - se muestra en la página en vez de tumbar el hilo
            self._json(500, {"ok": False, "error": f"Error interno: {exc}"})
            return
        if resultado is None:
            resultado = {"ok": True}
        self._json(200, resultado)

    def _fichero(self, nombre, tipo):
        try:
            with open(os.path.join(RUTA_UI, nombre), "rb") as f:
                self._responder(200, tipo, f.read())
        except OSError:
            self._responder(500, "text/plain; charset=utf-8", f"No se encuentra ui/{nombre}".encode())

    def _json(self, codigo, datos):
        cuerpo = json.dumps(datos, ensure_ascii=False, default=str).encode("utf-8")
        self._responder(codigo, "application/json; charset=utf-8", cuerpo)

    def _responder(self, codigo, tipo, cuerpo):
        try:
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(cuerpo)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass  # el navegador cerró la pestaña a mitad de respuesta

    def log_message(self, formato, *args):
        pass  # sin una línea por petición: no ensuciar la consola del módulo


def iniciar_web(pagina, estado_fn, acciones=None, consultas=None, puerto=None,
                puerto_defecto=8100, nombre="Interfaz web"):
    """Arranca el servidor en un hilo daemon. Devuelve el servidor (o None si no hay puerto).

    puerto: fijo (si está ocupado no se busca otro). Si es None se usa WEB_PORT y, si tampoco,
    el primer puerto libre a partir de puerto_defecto.
    """
    if puerto is None and os.environ.get("WEB_PORT"):
        puerto = int(os.environ["WEB_PORT"])
    candidatos = [puerto] if puerto is not None else range(puerto_defecto, puerto_defecto + 40)

    servidor = None
    for candidato in candidatos:
        try:
            servidor = ThreadingHTTPServer(("0.0.0.0", candidato), _Manejador)
            break
        except OSError as exc:
            error = exc
    if servidor is None:
        print(f"[{time.strftime('%H:%M:%S')}] AVISO: {nombre} no disponible ({error})", flush=True)
        return None

    servidor.daemon_threads = True
    servidor.pagina = pagina
    servidor.estado_fn = estado_fn
    servidor.acciones = acciones or {}
    servidor.consultas = consultas or {}
    servidor.puerto = servidor.server_address[1]
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    print(f"[{time.strftime('%H:%M:%S')}] {nombre} en http://localhost:{servidor.puerto}", flush=True)
    return servidor


def esperar_sin_teclado(quien: str):
    """Para menús con input(): si no hay teclado (lanzador, Docker sin -it) el proceso sigue vivo
    y se maneja desde su pantalla web. Termina con Ctrl+C / al matar el proceso."""
    print(f"[{quien}] sin teclado: se maneja desde su pantalla web", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
