"""
panel.py: panel WEB de CENTRAL (solo librería estándar)
Responsable: Vlad · Tarjeta Trello: "[Vlad] Panel de CENTRAL"
Lo usa: central/WM_Central.py -> iniciar_panel(estado_fn, acciones)

CÓMO FUNCIONA (servidor común en common/web.py, página en ui/panel.html)
  - GET  /             -> ui/panel.html
  - GET  /api/estado   -> JSON de foto_estado(); la página lo pide cada 1 s
  - POST /api/orden    -> {"accion": "regar"|"bloquear"|"activar", "ws": id o "*", "duracion": s}
  - POST /api/operarios-> {"id", "nombre"}: alta de un operario en la BD

  El dict estaciones de Central se lee SOLO con su lock cogido y se devuelve una copia.

PUERTO: variable de entorno PANEL_PORT (por defecto 8080). Abrir http://localhost:8080
Prueba sin Central ni Kafka: python -m tests.panel_demo
"""

import os
import time

from common.web import iniciar_web

ESTADOS = ("DISPONIBLE", "REGANDO", "FUGA", "FUERA_SERVICIO", "DESCONECTADA")


def foto_estado(estaciones, lock, eventos, central, operarios):
    """JSON de /api/estado.

    estaciones = dict de Central {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario",
                 "inicio_riego", "duracion", "ultimo_riego", "motivo_fuga"}}
    eventos    = common.web.Registro de Central
    central    = {"kafka", "broker", "monitores", "puerto_sockets", "inicio"}
    operarios  = [(id, nombre)] de la BD
    """
    ahora = time.time()
    with lock:
        lista = []
        for ws_id, ws in sorted(estaciones.items()):
            inicio = ws.get("inicio_riego")
            lista.append({
                "id": ws_id,
                "ubicacion": ws.get("ubicacion") or ws_id,
                "estado": ws.get("estado", "DESCONECTADA"),
                "caudal": ws.get("caudal", 0.0),
                "volumen": ws.get("volumen", 0.0),
                "operario": ws.get("operario"),
                "segundos": int(ahora - inicio) if inicio and ws.get("estado") == "REGANDO" else 0,
                "duracion": ws.get("duracion") or 0,
                "ultimo_riego": ws.get("ultimo_riego"),
                "motivo_fuga": ws.get("motivo_fuga"),
                "bloqueada": ws.get("estado") == "FUERA_SERVICIO",
            })
    recuento = {estado: sum(1 for ws in lista if ws["estado"] == estado) for estado in ESTADOS}
    return {
        "ahora": ahora,
        "central": central,
        "recuento": recuento,
        "estaciones": lista,
        "eventos": eventos.lista(),
        "operarios": [{"id": str(i).upper(), "nombre": nombre} for i, nombre in operarios],
    }


def iniciar_panel(estado_fn, acciones, puerto=None):
    """Arranca el panel en un hilo daemon y devuelve el servidor (None si el puerto está ocupado).

    estado_fn = función sin argumentos que devuelve foto_estado(...)
    acciones  = {"orden": fn(cuerpo), "operarios": fn(cuerpo)}
    puerto    = si no se da: variable de entorno PANEL_PORT, o 8080
    """
    if puerto is None:
        puerto = int(os.environ.get("PANEL_PORT", "8080"))
    return iniciar_web("panel.html", estado_fn, acciones, puerto=puerto, nombre="Panel web de Central")
