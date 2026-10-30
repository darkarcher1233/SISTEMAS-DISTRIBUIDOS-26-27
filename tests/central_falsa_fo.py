"""
central_falsa_fo.py: CENTRAL FALSA para probar WM_FO sin la Central real (solo Kafka).

USO (desde la raíz del repo, con Kafka en marcha):
    python -m tests.central_falsa_fo <ip_broker:puerto>
    python -m fo.WM_FO <ip_broker:puerto> FO1      (en otras terminales, FO1 y FO2)

Conoce WS1 y WS2. Responde a cada PETICION según docs/PROTOCOLO.md:
  - WS que no existe o no DISPONIBLE -> DENEGADO
  - si no -> AUTORIZADO + TELEMETRIA cada 1 s + RESUMEN (motivo TIEMPO)
  - truco para la demo: una petición de WS2 con 15 s acaba a los 3 s con motivo FUGA
Publica ESTADO_WS cada 2 s. NO envía REGAR (no hay Engine real detrás).
"""
import sys, threading, time
from common.kafka_utils import TOPIC_ORDENES, TOPIC_PETICIONES, consumidor, productor

broker = sys.argv[1]
prod = productor(broker)
lock = threading.Lock()
ws = {"WS1": "DISPONIBLE", "WS2": "DISPONIBLE"}
ubic = {"WS1": "Parque Norte", "WS2": "Parque Sur"}

def send(m):
    with lock:
        prod.send(TOPIC_ORDENES, m); prod.flush()

def estado():
    while True:
        with lock:
            lista = [{"ws_id": k, "ubicacion": ubic[k], "estado": v} for k, v in ws.items()]
        send({"tipo": "ESTADO_WS", "estaciones": lista}); time.sleep(2)

def riego(fo, w, dur):
    vol, motivo = 0.0, "TIEMPO"
    for s in range(dur):
        if w == "WS2" and dur == 15 and s == 3:
            motivo = "FUGA"; break
        vol += 0.33
        send({"tipo": "TELEMETRIA", "fo_id": fo, "ws_id": w, "caudal": 20.0, "volumen": round(vol, 2)})
        time.sleep(1)
    send({"tipo": "RESUMEN", "fo_id": fo, "ws_id": w, "volumen": round(vol, 2), "duracion": s, "motivo": motivo})
    ws[w] = "DISPONIBLE"

threading.Thread(target=estado, daemon=True).start()
cons = consumidor(broker, TOPIC_PETICIONES, group_id=None, auto_offset_reset="latest", enable_auto_commit=False)
print("fake central lista", flush=True)
for m in cons:
    d = m.value; fo, w, dur = d.get("fo_id"), d.get("ws_id"), int(d.get("duracion", 0))
    print("PETICION", d, flush=True)
    if w not in ws:
        send({"tipo": "DENEGADO", "fo_id": fo, "ws_id": w, "motivo": "La WS no existe"})
    elif ws[w] != "DISPONIBLE":
        send({"tipo": "DENEGADO", "fo_id": fo, "ws_id": w, "motivo": f"WS en estado {ws[w]}"})
    else:
        ws[w] = "REGANDO"
        send({"tipo": "AUTORIZADO", "fo_id": fo, "ws_id": w, "duracion": dur})
        threading.Thread(target=riego, args=(fo, w, dur), daemon=True).start()
