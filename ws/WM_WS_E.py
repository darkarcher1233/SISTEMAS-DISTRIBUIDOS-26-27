"""
WM_WS_E.py: ENGINE de la Watering Station (nombre obligatorio: WM_WS_E)
Responsable: Vlad · Semanas 1 (sockets) y 2 (riego + Kafka)

USO
    python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>

SEMANA 1 (hecho):
  - Conecta por socket al Monitor y recibe  ID#<ws_id>.
  - Hilo responder_health_check: responde a HC con OK o KO según 'averia'.

SEMANA 2 (nuevo):
  - Hilo consumidor del topic 'ordenes' (group_id=f"ws_{ws_id}", auto_offset_reset="latest")
    que procesa SOLO los mensajes dirigidos a su ws_id:
        REGAR    (duracion, operario)   -> inicia riego
        BLOQUEAR                        -> bloquea la estación y para el riego
        ACTIVAR                         -> desbloquea la estación
  - Riego = hilo que cada 1 s calcula caudal aleatorio (15-25 L/min),
    acumula volumen y publica TELEMETRIA en el topic 'telemetria'.
  - El riego termina por: tiempo agotado · tecla 's' · averia · orden BLOQUEAR
        -> publica FIN (volumen, duracion, motivo)
  - Menú local:
        f = averia True   r = averia False
        m = riego manual (operario "MANUAL", pide duración)
        s = parar el riego     q = salir

Mensajes: docs/PROTOCOLO.md
"""

from __future__ import annotations

import random
import socket
import sys
import threading
import time

import common.sockets_utils
from common.kafka_utils import (
    TOPIC_ORDENES,
    TOPIC_TELEMETRIA,
    consumidor,
    productor,
)


# ---------------------- Estado global protegido con Lock ----------------------
estado_lock = threading.Lock()
stop_event = threading.Event()   # señal para cortar el hilo de riego al instante

averia = False      # True -> HC responde KO; si estamos regando, el riego acaba como FUGA
bloqueada = False   # True -> la estación no acepta REGAR
regando = False     # True -> hay un hilo de riego vivo
motivo: str | None = None   # motivo con el que acabará el riego actual


# ---------------------- Hilo 1: health check por socket -----------------------
def responder_health_check(sock):
    """Responde cada HC del Monitor con OK o KO según la variable averia."""
    while True:
        msg = common.sockets_utils.recibir(sock)
        if not msg:
            print(f"[{ws_id}] Monitor desconectado")
            break
        if msg == "HC":
            with estado_lock:
                hay_averia = averia
            respuesta = "KO" if hay_averia else "OK"
            common.sockets_utils.enviar(sock, respuesta)


# ---------------------- Hilo 2: ciclo de riego --------------------------------
def regar(duracion: int, operario: str, broker: str):
    """Bucle de riego: publica TELEMETRIA cada 1 s y FIN cuando termina."""
    global regando, motivo

    prod = productor(broker)
    volumen = 0.0
    inicio = time.monotonic()
    print(f"[{ws_id}] riego iniciado (duracion={duracion}s operario={operario})")

    while True:
        transcurrido = time.monotonic() - inicio

        # Comprobar condiciones de fin que no se hayan señalado desde fuera
        with estado_lock:
            if motivo is None:
                if averia:
                    motivo = "FUGA"
                elif transcurrido >= duracion:
                    motivo = "TIEMPO"
            motivo_actual = motivo

        if motivo_actual is not None:
            break

        caudal = random.uniform(15, 25)
        volumen += caudal / 60.0
        telemetria = {
            "tipo": "TELEMETRIA",
            "ws_id": ws_id,
            "caudal": round(caudal, 2),
            "volumen": round(volumen, 3),
            "operario": operario,
        }
        prod.send(TOPIC_TELEMETRIA, telemetria)
        print(f"[{ws_id}] caudal={caudal:.2f} L/min volumen={volumen:.3f} L")

        # Espera interrumpible: si alguien hace stop_event.set() salimos al momento
        if stop_event.wait(1):
            # El motivo lo pone quien dispara el evento (menú / Kafka / HC)
            pass

    duracion_real = round(time.monotonic() - inicio, 1)
    fin = {
        "tipo": "FIN",
        "ws_id": ws_id,
        "volumen": round(volumen, 3),
        "duracion": duracion_real,
        "motivo": motivo_actual,
    }
    prod.send(TOPIC_TELEMETRIA, fin)
    prod.flush()
    print(f"[{ws_id}] FIN motivo={motivo_actual} volumen={volumen:.3f} L duracion={duracion_real}s")

    with estado_lock:
        regando = False
        motivo = None
    stop_event.clear()


def iniciar_riego(duracion: int, operario: str, broker: str) -> bool:
    """Lanza el hilo de riego si la estación está libre y no bloqueada."""
    global regando
    with estado_lock:
        if bloqueada:
            print(f"[{ws_id}] orden ignorada: estacion bloqueada")
            return False
        if regando:
            print(f"[{ws_id}] orden ignorada: ya estamos regando")
            return False
        regando = True
    stop_event.clear()
    threading.Thread(
        target=regar, args=(duracion, operario, broker), daemon=True
    ).start()
    return True


# ---------------------- Hilo 3: consumidor de 'ordenes' -----------------------
def consumir_ordenes(broker: str):
    """Consume el topic 'ordenes' y procesa las que vayan dirigidas a este ws_id."""
    global bloqueada, motivo
    cons = consumidor(
        broker,
        TOPIC_ORDENES,
        group_id=f"ws_{ws_id}",
        auto_offset_reset="latest",
    )
    for mensaje in cons:
        try:
            datos = mensaje.value
            if not isinstance(datos, dict):
                continue
            if datos.get("ws_id") != ws_id:
                continue

            tipo = datos.get("tipo")
            if tipo == "REGAR":
                duracion = int(datos.get("duracion", 0))
                operario = str(datos.get("operario", "DESCONOCIDO"))
                if duracion <= 0:
                    print(f"[{ws_id}] REGAR ignorada: duracion invalida")
                    continue
                iniciar_riego(duracion, operario, broker)

            elif tipo == "BLOQUEAR":
                with estado_lock:
                    bloqueada = True
                    if regando:
                        motivo = "BLOQUEO"
                        stop_event.set()
                print(f"[{ws_id}] BLOQUEAR recibido")

            elif tipo == "ACTIVAR":
                with estado_lock:
                    bloqueada = False
                print(f"[{ws_id}] ACTIVAR recibido")

        except Exception as exc:  # noqa: BLE001
            print(f"[{ws_id}] ERROR procesando orden: {exc}")


# ---------------------- Menú (hilo principal) --------------------------------
def menu(broker: str):
    """Menú de teclado en el hilo principal."""
    global averia, motivo
    ayuda = "f=averia  r=reparada  m=riego manual  s=parar  q=salir"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if opcion == "f":
            with estado_lock:
                averia = True
                if regando and motivo is None:
                    motivo = "FUGA"
                    stop_event.set()
            print(f"[{ws_id}] averia=True")

        elif opcion == "r":
            with estado_lock:
                averia = False
            print(f"[{ws_id}] averia=False")

        elif opcion == "m":
            texto = input("duracion (segundos): ").strip()
            try:
                duracion = int(texto)
                if duracion <= 0:
                    raise ValueError
            except ValueError:
                print("duracion invalida, debe ser un entero > 0")
                continue
            iniciar_riego(duracion, "MANUAL", broker)

        elif opcion == "s":
            with estado_lock:
                if regando and motivo is None:
                    motivo = "MANUAL"
                    stop_event.set()
                    print(f"[{ws_id}] parando riego...")
                else:
                    print(f"[{ws_id}] no hay riego en curso")

        elif opcion == "q":
            print(f"[{ws_id}] saliendo")
            break

        else:
            print(ayuda)


# ---------------------- Arranque ---------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("USO: python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>")
        sys.exit(1)

    broker = sys.argv[1]  # cadena completa host:puerto, la pasamos tal cual a Kafka
    ip_monitor, puerto_monitor = sys.argv[2].split(":")
    puerto_monitor = int(puerto_monitor)

    my_socket = socket.socket()
    my_socket.connect((ip_monitor, puerto_monitor))

    mensaje = common.sockets_utils.recibir(my_socket)
    ws_id = mensaje.split("#")[1]

    print(f"[{ws_id}] conectado al monitor, broker Kafka={broker}")

    threading.Thread(
        target=responder_health_check, args=(my_socket,), daemon=True
    ).start()
    threading.Thread(
        target=consumir_ordenes, args=(broker,), daemon=True
    ).start()

    menu(broker)
