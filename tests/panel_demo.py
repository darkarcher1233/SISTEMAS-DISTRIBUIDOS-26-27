"""
panel_demo.py: prueba del panel web de Central SIN Central ni Kafka (datos falsos).

USO (desde la raíz del repo):
    python -m tests.panel_demo          (PANEL_PORT=xxxx para otro puerto)
    abrir http://localhost:8080

Cinco WS, una en cada estado. WS2 está REGANDO: su volumen sube cada segundo.
Los botones del panel funcionan sobre los datos falsos (regar / bloquear / activar). Ctrl+C para salir.
"""
import threading
import time

from WM_Central.panel import foto_estado, iniciar_panel
from common.web import Registro

ahora = time.time()


def ws(ubicacion, estado, **extra):
    datos = {"ubicacion": ubicacion, "estado": estado, "caudal": 0.0, "volumen": 0.0, "operario": None,
             "inicio_riego": None, "duracion": 0, "ultimo_riego": ahora - 600, "motivo_fuga": None}
    datos.update(extra)
    return datos


estaciones = {
    "WS1": ws("Parque Norte", "DISPONIBLE"),
    "WS2": ws("Parque Norte", "REGANDO", caudal=18.5, operario="FO1", inicio_riego=ahora, duracion=120),
    "WS3": ws("Jardín Central", "FUGA", motivo_fuga="Avería avisada por el monitor"),
    "WS4": ws("Rotonda Este", "FUERA_SERVICIO"),
    "WS5": ws("Huerto Oeste", "DESCONECTADA", ultimo_riego=None),
}
lock = threading.Lock()
eventos = Registro()
operarios = [("fo1", "Operario 1"), ("fo2", "Operario 2"), ("fo3", "Operario 3")]
eventos.add("REG: nueva WS WS1 (ubicación: Parque Norte)", "OK")
eventos.add("PETICION AUTORIZADA: FO1 -> WS2 durante 120s", "OK")
eventos.add("ESTADO: WS3 DISPONIBLE -> FUGA", "ALERTA")
eventos.add("ORDEN CENTRAL: WS4 DISPONIBLE -> FUERA_SERVICIO", "AVISO")


def orden(cuerpo):
    accion, ws_id = cuerpo.get("accion"), cuerpo.get("ws")
    with lock:
        ids = list(estaciones) if ws_id == "*" else [ws_id]
        for i in ids:
            e = estaciones[i]
            if accion == "regar" and e["estado"] == "DISPONIBLE":
                e.update(estado="REGANDO", caudal=20.0, volumen=0.0, operario="CENTRAL",
                         inicio_riego=time.time(), duracion=int(cuerpo.get("duracion") or 60))
            elif accion == "bloquear" and e["estado"] != "DESCONECTADA":
                e.update(estado="FUERA_SERVICIO", caudal=0.0)
            elif accion == "activar" and e["estado"] == "FUERA_SERVICIO":
                e.update(estado="DISPONIBLE")
    eventos.add(f"ORDEN (demo): {accion} {ws_id}", "INFO")
    return {"ok": True, "mensaje": f"{accion}: {ws_id}"}


def alta(cuerpo):
    operarios.append((cuerpo["id"].lower(), cuerpo["nombre"]))
    return {"ok": True, "mensaje": "Operario dado de alta"}


central = {"kafka": "conectado", "broker": "demo:9092", "monitores": 4, "puerto_sockets": 5050, "inicio": ahora}
if iniciar_panel(lambda: foto_estado(estaciones, lock, eventos, central, operarios, {"WS1": 152.4, "WS2": 37.0}),
                 {"orden": orden, "operarios": alta}) is None:
    raise SystemExit(1)

try:
    while True:
        time.sleep(1)
        with lock:
            for e in estaciones.values():
                if e["estado"] == "REGANDO":
                    e["volumen"] += e["caudal"] / 60  # L/min -> L en 1 s
                    if time.time() - e["inicio_riego"] >= e["duracion"]:
                        e.update(estado="DISPONIBLE", caudal=0.0, operario=None, ultimo_riego=time.time())
except KeyboardInterrupt:
    print("panel_demo detenido")
