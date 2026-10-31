"""
panel_demo.py: prueba del panel web de Central SIN Central ni Kafka (datos falsos).

USO (desde la raíz del repo):
    python -m tests.panel_demo          (PANEL_PORT=xxxx para otro puerto)
    abrir http://localhost:8080

Cinco WS, una en cada estado. WS2 está REGANDO: su volumen sube cada segundo y cada
segundo se añade una línea al log. Ctrl+C para salir.
"""
import threading
import time
from collections import deque

from central.panel import iniciar_panel

estaciones = {
    "WS1": {"ubicacion": "Parque Norte", "estado": "DISPONIBLE", "caudal": 0.0, "volumen": 0.0, "operario": None},
    "WS2": {"ubicacion": "Parque Sur", "estado": "REGANDO", "caudal": 12.5, "volumen": 0.0, "operario": "FO1"},
    "WS3": {"ubicacion": "Jardín Central", "estado": "FUGA", "caudal": 0.0, "volumen": 0.0, "operario": None},
    "WS4": {"ubicacion": "Rotonda Este", "estado": "FUERA_SERVICIO", "caudal": 0.0, "volumen": 0.0, "operario": None},
    "WS5": {"ubicacion": "Huerto Oeste", "estado": "DESCONECTADA", "caudal": 0.0, "volumen": 0.0, "operario": None},
}
lock = threading.Lock()
log_buffer = deque(maxlen=50)


def log(texto):
    linea = f"[{time.strftime('%H:%M:%S')}] {texto}"
    print(linea)
    log_buffer.append(linea)


log("REG: nueva WS WS1 (ubicación: Parque Norte)")
log("ESTADO: WS3 DISPONIBLE -> FUGA")
log("ESTADO: WS4 DISPONIBLE -> FUERA_SERVICIO")
log("CONEXION CERRADA 127.0.0.1:50123")

if iniciar_panel(estaciones, lock, log_buffer) is None:
    raise SystemExit(1)

try:
    while True:
        time.sleep(1)
        with lock:
            ws = estaciones["WS2"]
            ws["volumen"] += ws["caudal"] / 60  # L/min -> L en 1 s
            volumen = ws["volumen"]
        log(f"TELEMETRIA: WS2 caudal=12.50 L/min volumen={volumen:.2f} L")
except KeyboardInterrupt:
    print("panel_demo detenido")
