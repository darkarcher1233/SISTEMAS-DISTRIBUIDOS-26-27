"""
WM_WS_E.py: ENGINE de la Watering Station (nombre obligatorio: WM_WS_E)
Responsable: Vlad · Semanas 1 (sockets) y 2 (riego + Kafka)

USO
    python -m WM_WS.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>
    Pantalla web: la URL sale por consola (otro puerto: variable de entorno WEB_PORT)

SOCKETS (semana 1)
  - Conecta por socket al Monitor y recibe  ID#<ws_id>[#<ubicacion>].
  - Hilo conexion_monitor: responde a HC con OK o KO según 'averia'. Si el Monitor se cae,
    se reintenta la conexión cada pocos segundos.

KAFKA (semana 2)
  - Hilo consumidor del topic 'ordenes' (group_id=f"ws_{ws_id}", auto_offset_reset="latest")
    que procesa SOLO los mensajes dirigidos a su ws_id:
        REGAR    (duracion, operario)   -> inicia riego
        BLOQUEAR                        -> bloquea la estación y para el riego
        ACTIVAR                         -> desbloquea la estación
  - Riego = hilo que cada 1 s calcula caudal aleatorio (15-25 L/min),
    acumula volumen y publica TELEMETRIA en el topic 'telemetria'.
  - El riego termina por: tiempo agotado · tecla 's' · averia · orden BLOQUEAR
        -> publica FIN (volumen, duracion, motivo)

MENÚ (teclado) y PANTALLA WEB (ui/estacion.html) hacen lo mismo:
        f = averia True   r = averia False
        m = pedir un riego a Central (pide operario y duración; Central valida y lo autoriza)
        s = parar el riego     q = salir
  La pantalla web además puede SOLICITAR un riego a Central (PETICION por Kafka, como un FO).

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
    TOPIC_PETICIONES,
    TOPIC_TELEMETRIA,
    consumidor,
    productor,
)
from common.web import Registro, esperar_sin_teclado, iniciar_web

REINTENTO = 3   # segundos entre reintentos (Monitor o Kafka caídos)

# ---------------------- Estado global protegido con Lock ----------------------
estado_lock = threading.Lock()
stop_event = threading.Event()   # señal para cortar el hilo de riego al instante

ws_id: str | None = None   # lo manda el Monitor (ID#...)
ubicacion = ""
averia = False      # True -> HC responde KO; si estamos regando, el riego acaba como FUGA
bloqueada = False   # True -> la estación no acepta REGAR
regando = False     # True -> hay un hilo de riego vivo
motivo: str | None = None   # motivo con el que acabará el riego actual
riego: dict | None = None   # datos del riego en curso para la pantalla
con_monitor = False
kafka_ok = False
monitor_dir = ""
eventos = Registro()
_prod = None


def log(texto, nivel="INFO"):
    print(f"[{ws_id or 'WS-?'}] {texto}", flush=True)
    eventos.add(texto, nivel)


def productor_engine(broker):
    global _prod
    if _prod is None:
        _prod = productor(broker, max_block_ms=5000)
    return _prod


# ---------------------- Hilo 1: conexión con el Monitor -----------------------
def responder_health_check(sock):
    """Responde cada HC del Monitor con OK o KO según la variable averia."""
    while True:
        msg = common.sockets_utils.recibir(sock)
        if not msg:
            return
        if msg == "HC":
            with estado_lock:
                hay_averia = averia
            common.sockets_utils.enviar(sock, "KO" if hay_averia else "OK")


def conexion_monitor(ip_monitor, puerto_monitor, broker):
    """Se conecta al Monitor (y se reconecta si se cae) y atiende su health check."""
    global ws_id, ubicacion, con_monitor
    aviso_dado = False
    while True:
        try:
            sock = socket.create_connection((ip_monitor, puerto_monitor), timeout=5)
            sock.settimeout(None)
        except OSError as exc:
            if not aviso_dado:
                log(f"Monitor {ip_monitor}:{puerto_monitor} no disponible ({exc}); reintentando", "AVISO")
                aviso_dado = True
            time.sleep(REINTENTO)
            continue
        aviso_dado = False
        try:
            mensaje = common.sockets_utils.recibir(sock)
            partes = (mensaje or "").split("#")
            if partes[0] != "ID" or len(partes) < 2:
                raise ValueError(f"se esperaba ID#<ws_id>, llegó {mensaje!r}")
            primera_vez = ws_id is None
            ws_id = partes[1]
            ubicacion = partes[2] if len(partes) > 2 else ""
            con_monitor = True
            log(f"conectado al monitor {ip_monitor}:{puerto_monitor}, broker Kafka={broker}", "OK")
            if primera_vez:
                threading.Thread(target=consumir_ordenes, args=(broker,), daemon=True).start()
            responder_health_check(sock)
        except (OSError, ValueError) as exc:
            log(f"error con el monitor: {exc}", "AVISO")
        finally:
            sock.close()
        if con_monitor:
            log("Monitor desconectado; reintentando", "AVISO")
        con_monitor = False
        time.sleep(REINTENTO)


# ---------------------- Hilo 2: ciclo de riego --------------------------------
def regar(duracion: int, operario: str, broker: str):
    """Bucle de riego: publica TELEMETRIA cada 1 s y FIN cuando termina."""
    global regando, motivo, riego

    prod = productor_engine(broker)
    volumen = 0.0
    inicio = time.monotonic()
    log(f"riego iniciado (duracion={duracion}s operario={operario})", "OK")
    with estado_lock:
        riego = {"caudal": 0.0, "volumen": 0.0, "fo_id": operario, "segundos": 0.0, "duracion": duracion}

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
        try:
            prod.send(TOPIC_TELEMETRIA, telemetria)
        except Exception as exc:  # noqa: BLE001 - sin Kafka se sigue regando (y se avisa)
            log(f"no se pudo enviar TELEMETRIA: {exc}", "AVISO")
        with estado_lock:
            riego.update(caudal=caudal, volumen=volumen, segundos=transcurrido)
        print(f"[{ws_id}] caudal={caudal:.2f} L/min volumen={volumen:.3f} L", flush=True)

        # Espera interrumpible: si alguien hace stop_event.set() salimos al momento
        stop_event.wait(1)

    duracion_real = round(time.monotonic() - inicio, 1)
    fin = {
        "tipo": "FIN",
        "ws_id": ws_id,
        "volumen": round(volumen, 3),
        "duracion": duracion_real,
        "motivo": motivo_actual,
    }
    try:
        prod.send(TOPIC_TELEMETRIA, fin)
        prod.flush(timeout=10)
    except Exception as exc:  # noqa: BLE001
        log(f"no se pudo enviar FIN: {exc}", "ALERTA")
    nivel = "ALERTA" if motivo_actual in ("FUGA", "BLOQUEO") else "OK"
    log(f"FIN motivo={motivo_actual} volumen={volumen:.3f} L duracion={duracion_real}s", nivel)

    with estado_lock:
        regando = False
        motivo = None
        riego = None
    stop_event.clear()


def iniciar_riego(duracion: int, operario: str, broker: str) -> bool:
    """Lanza el hilo de riego si la estación está libre y no bloqueada."""
    global regando
    with estado_lock:
        if bloqueada:
            log("orden de riego ignorada: estacion bloqueada", "AVISO")
            return False
        if regando:
            log("orden de riego ignorada: ya estamos regando", "AVISO")
            return False
        regando = True
    stop_event.clear()
    threading.Thread(
        target=regar, args=(duracion, operario, broker), daemon=True
    ).start()
    return True


def pedir_riego_a_central(broker: str, fo_id: str, duracion: int):
    """Riego pedido desde la propia estación (menú 'm' o pantalla web): se envía una PETICION a
    Central por Kafka, igual que haría un FO. Central comprueba que el operario existe y que la WS
    está disponible y contesta AUTORIZADO (+ REGAR) o DENEGADO. Lanza ValueError si no se puede."""
    if not ws_id:
        raise ValueError("La estación aún no tiene Id (falta el monitor)")
    if not fo_id or not 0 < duracion <= 3600:
        raise ValueError("Operario y duración (1-3600 s) obligatorios")
    try:
        productor_engine(broker).send(
            TOPIC_PETICIONES,
            {"tipo": "PETICION", "fo_id": fo_id, "ws_id": ws_id, "duracion": duracion},
        ).get(timeout=5)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Kafka no responde ({type(exc).__name__})") from exc
    log(f"PETICION enviada a Central: {fo_id} durante {duracion} s")


def parar_riego(nuevo_motivo="MANUAL") -> bool:
    global motivo
    with estado_lock:
        if regando and motivo is None:
            motivo = nuevo_motivo
            stop_event.set()
            return True
    return False


def poner_averia(valor: bool):
    global averia
    with estado_lock:
        averia = valor
    if valor:
        parar_riego("FUGA")
    log(f"averia={valor}", "ALERTA" if valor else "OK")


# ---------------------- Hilo 3: consumidor de 'ordenes' -----------------------
def procesar_orden(datos, broker):
    global bloqueada
    if not isinstance(datos, dict) or datos.get("ws_id") != ws_id:
        return
    tipo = datos.get("tipo")
    if tipo == "REGAR":
        duracion = int(datos.get("duracion", 0))
        operario = str(datos.get("operario", "DESCONOCIDO"))
        if duracion <= 0:
            log("REGAR ignorada: duracion invalida", "AVISO")
            return
        iniciar_riego(duracion, operario, broker)

    elif tipo == "BLOQUEAR":
        with estado_lock:
            bloqueada = True
        parar_riego("BLOQUEO")
        log("BLOQUEAR recibido: fuera de servicio", "AVISO")

    elif tipo == "ACTIVAR":
        with estado_lock:
            bloqueada = False
        log("ACTIVAR recibido", "OK")

    elif tipo == "AUTORIZADO":   # Central autoriza un riego en esta WS (el REGAR llega aparte)
        log(f"Central AUTORIZA el riego de {datos.get('fo_id')} ({datos.get('duracion')} s)", "OK")

    elif tipo == "DENEGADO":     # Central deniega un riego pedido para esta WS
        log(f"Central DENIEGA el riego de {datos.get('fo_id')}: {datos.get('motivo')}", "AVISO")


def consumir_ordenes(broker: str):
    """Consume el topic 'ordenes' y procesa las que vayan dirigidas a este ws_id."""
    global kafka_ok
    while True:
        cons = None
        try:
            cons = consumidor(
                broker,
                TOPIC_ORDENES,
                group_id=f"ws_{ws_id}",
                auto_offset_reset="latest",
                # si un Engine anterior murió sin despedirse, el grupo lo descarta en 6 s
                # (por defecto 45 s: tras reiniciar el Engine no le llegarían órdenes en ese rato)
                session_timeout_ms=6000,
                heartbeat_interval_ms=2000,
            )
            while True:
                lotes = cons.poll(timeout_ms=1000)
                if not kafka_ok and cons.assignment():
                    kafka_ok = True
                    log(f"Kafka conectado ({broker})", "OK")
                for mensajes in lotes.values():
                    for mensaje in mensajes:
                        try:
                            procesar_orden(mensaje.value, broker)
                        except Exception as exc:  # noqa: BLE001
                            log(f"ERROR procesando orden: {exc}", "ALERTA")
        except Exception as exc:  # noqa: BLE001
            if kafka_ok:
                log(f"ERROR Kafka ({broker}): {exc}; reintentando", "ALERTA")
            else:
                print(f"[{ws_id}] Kafka no responde ({exc}); reintentando", flush=True)
            kafka_ok = False
        finally:
            if cons is not None:
                try:
                    cons.close()
                except Exception:  # noqa: BLE001
                    pass
        time.sleep(REINTENTO)


# ---------------------- Pantalla web -----------------------------------------
def estado_web():
    with estado_lock:
        if averia:
            estado = "FUGA"
        elif bloqueada:
            estado = "FUERA_SERVICIO"
        elif regando:
            estado = "REGANDO"
        else:
            estado = "DISPONIBLE"
        r = dict(riego) if riego else None
        return {
            "ws_id": ws_id, "ubicacion": ubicacion, "monitor": monitor_dir, "con_monitor": con_monitor,
            "kafka": kafka_ok, "bloqueada": bloqueada, "averia": averia, "estado": estado,
            "riego": r, "eventos": eventos.lista(),
        }


def acciones_web(broker):
    def solicitar(cuerpo):
        try:
            duracion = int(cuerpo.get("duracion") or 0)
        except (TypeError, ValueError):
            duracion = 0
        pedir_riego_a_central(broker, str(cuerpo.get("fo_id") or "").strip(), duracion)
        return {"ok": True, "mensaje": "Solicitud enviada a CENTRAL"}

    def parar(_cuerpo):
        if not parar_riego("MANUAL"):
            raise ValueError("No hay riego en curso")
        log("parando riego (manual)")
        return {"ok": True, "mensaje": "Parando el riego"}

    def fuga(_cuerpo):
        with estado_lock:
            nuevo = not averia
        poner_averia(nuevo)
        return {"ok": True, "mensaje": "Avería simulada" if nuevo else "Avería resuelta"}

    return {"regar": solicitar, "parar": parar, "fuga": fuga}


# ---------------------- Menú (hilo principal) --------------------------------
def menu(broker: str):
    """Menú de teclado en el hilo principal."""
    ayuda = "f=averia  r=reparada  m=pedir riego (a CENTRAL)  s=parar  q=salir"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip().lower()
        except EOFError:
            esperar_sin_teclado(f"Engine {ws_id or ''}".strip())
            break
        except KeyboardInterrupt:
            print()
            break

        if opcion == "f":
            poner_averia(True)

        elif opcion == "r":
            poner_averia(False)

        elif opcion == "m":
            fo_id = input("operario (p. ej. FO1): ").strip()
            try:
                duracion = int(input("duracion (segundos): ").strip())
                pedir_riego_a_central(broker, fo_id, duracion)
            except ValueError as exc:
                print(f"No se puede pedir el riego: {exc or 'duración entera > 0'}")

        elif opcion == "s":
            if parar_riego("MANUAL"):
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
        print("USO: python -m WM_WS.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>")
        sys.exit(1)

    broker = sys.argv[1]  # cadena completa host:puerto, la pasamos tal cual a Kafka
    try:
        ip_monitor, puerto_texto = sys.argv[2].rsplit(":", 1)
        puerto_monitor = int(puerto_texto)
    except ValueError:
        print("Argumentos incorrectos. Ejemplo: python -m WM_WS.WM_WS_E 127.0.0.1:9092 127.0.0.1:6000")
        sys.exit(1)
    monitor_dir = f"{ip_monitor}:{puerto_monitor}"

    iniciar_web("estacion.html", estado_web, acciones_web(broker), puerto_defecto=8200,
                nombre="Pantalla de la estación")
    threading.Thread(
        target=conexion_monitor, args=(ip_monitor, puerto_monitor, broker), daemon=True
    ).start()

    menu(broker)
