"""
WM_Central.py: CENTRAL, el cerebro del sistema (nombre obligatorio: WM_Central)
Responsable: Abde · Semanas 1, 2 y 3

USO
    python -m central.WM_Central <puerto_escucha> <ip_broker:puerto>
    (ejecutar desde la raíz del repo para poder importar common/)

SEMANA 1: solo sockets (Tarjeta: "WM_Central: servidor de sockets + registro de WS")
  - Servidor concurrente: un hilo por cada WM_WS_M conectado. SIN límite de conexiones
    (quitar MAX_CONEXIONES del ejemplo de clase)
  - Estado en memoria: dict {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario"}}
    protegido con threading.Lock
  - REG#id#ubicacion -> alta o actualización -> DISPONIBLE, responder OK
  - FUGA#id -> FUGA   ·   RESUELTA#id -> DISPONIBLE
  - Si se cierra el socket de un Monitor -> DESCONECTADA
  - Imprimir la tabla de estados cada vez que cambie algo
  - El broker se recibe por argumento pero en la semana 1 NO se usa

SEMANA 2 (Tarjetas: "SQLite..." + "estados de riego + consumo de telemetría por Kafka")
  - Al arrancar: cargar las WS de la BD (central/db.py) -> todas DESCONECTADA
  - Cada cambio de estado -> también a la BD
  - Hilo consumidor de 'telemetria': caudal y volumen cada 1 s · FIN -> DISPONIBLE

SEMANA 3 (Tarjetas: "flujo de activación de riego" + "menú de órdenes")
  - Hilo consumidor de 'peticiones': validar operario y WS DISPONIBLE -> AUTORIZADO / DENEGADO
  - Reenviar al FO la telemetría de su riego y el RESUMEN final
  - Cada ~2 s publicar ESTADO_WS (lista de WS) en 'ordenes'
  - Menú (punto 11 del enunciado): iniciar riego / bloquear / activar, a una WS o a todas (ALL)
  - Lanzar el panel (central/panel.py de Vlad) en un hilo

ESTADOS: DISPONIBLE · REGANDO · FUGA · FUERA_SERVICIO · DESCONECTADA
MENSAJES: docs/PROTOCOLO.md
-------------------------------------------------------------------------------
Basado en ejemplos/servidor_concurrente.py: un hilo por cada Monitor conectado.
""" 

import socket
import sys
import threading
import time
from collections import deque
from datetime import datetime

from central.db import (
    crear_tablas,
    get_estaciones,
    guardar_estacion,
    guardar_riego,
    operario_existe,
    set_estado,
)
from central.panel import iniciar_panel
from common.kafka_utils import (
    TOPIC_ORDENES,
    TOPIC_PETICIONES,
    TOPIC_TELEMETRIA,
    consumidor,
    productor,
)
from common.sockets_utils import enviar, recibir

# Estados que se usan en la semana 1 (REGANDO y FUERA_SERVICIO llegan en las semanas 2 y 3)
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"

# Estado de todas las WS, en memoria (en la semana 2 también se guardará en SQLite):
#   {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario"}}
estaciones = {}

# Socket del Monitor por el que se ha registrado cada WS conectada: {ws_id: conn}
# Sirve para saber qué WS pasa a DESCONECTADA cuando se cierra un socket.
conexiones = {}

# Hay un hilo por Monitor y todos usan estaciones y conexiones:
# solo se leen o se modifican con este lock cogido.
lock = threading.Lock()

# Últimas líneas del log, para el panel web (central/panel.py)
log_buffer = deque(maxlen=50)


def log(texto):
    linea = f"[{time.strftime('%H:%M:%S')}] {texto}"
    print(linea)
    log_buffer.append(linea)


def imprimir_tabla():
    """Imprime la tabla de estados de todas las WS. Llamarla SIEMPRE con el lock cogido."""
    titulo = f"================ ESTACIONES DE RIEGO ({len(estaciones)}) ================"
    lineas = ["", titulo, f"{'ID':<10}{'UBICACION':<32}ESTADO"]
    for ws_id in sorted(estaciones):
        ws = estaciones[ws_id]
        lineas.append(f"{ws_id:<10}{ws['ubicacion'][:30]:<32}{ws['estado']}")
    if not estaciones:
        lineas.append("(todavía no hay ninguna WS registrada)")
    lineas.append("=" * len(titulo))
    print("\n".join(lineas))


def registrar_ws(conn, ws_id, ubicacion):
    """REG: da de alta la WS (o la actualiza si ya existía) y la deja DISPONIBLE."""
    with lock:
        ya_existia = ws_id in estaciones
        anterior = conexiones.get(ws_id)
        estaciones[ws_id] = {"ubicacion": ubicacion, "estado": DISPONIBLE,
                             "caudal": 0.0, "volumen": 0.0, "operario": None}
        conexiones[ws_id] = conn
        guardar_estacion(ws_id, ubicacion)
        set_estado(ws_id, DISPONIBLE)

        if ya_existia:
            log(f"REG: {ws_id} ya existía, se actualiza (ubicación: {ubicacion})")
        else:
            log(f"REG: nueva WS {ws_id} (ubicación: {ubicacion})")
        if anterior is not None and anterior is not conn:
            log(f"AVISO: {ws_id} ya tenía otra conexión abierta; a partir de ahora vale esta")
        imprimir_tabla()


def cargar_estaciones():
    """Restaura las estaciones persistidas y las marca desconectadas al iniciar."""
    with lock:
        for ws_id, ubicacion, _estado_anterior in get_estaciones():
            estaciones[ws_id] = {
                "ubicacion": ubicacion,
                "estado": DESCONECTADA,
                "caudal": 0.0,
                "volumen": 0.0,
                "operario": None,
            }
            set_estado(ws_id, DESCONECTADA)


def cambiar_estado(conn_o_ws_id, ws_id=None, nuevo_estado=None):
    """Actualiza el estado de una WS.

    Soporta la API antigua: cambiar_estado(conn, ws_id, nuevo_estado)
    y la versión más directa del enunciado: cambiar_estado(ws_id, nuevo_estado)
    """
    if nuevo_estado is None:
        ws_id, nuevo_estado = conn_o_ws_id, ws_id
        conn = None
    else:
        conn = conn_o_ws_id

    with lock:
        if ws_id not in estaciones:
            return False
        if conn is not None and conexiones.get(ws_id) is not conn:
            return False

        estado_anterior = estaciones[ws_id]["estado"]
        if nuevo_estado in (DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA):
            estaciones[ws_id]["estado"] = nuevo_estado
            if nuevo_estado == DISPONIBLE:
                estaciones[ws_id]["caudal"] = 0.0
                estaciones[ws_id]["volumen"] = 0.0
                estaciones[ws_id]["operario"] = None
            set_estado(ws_id, nuevo_estado)
            log(f"ESTADO: {ws_id} {estado_anterior} -> {nuevo_estado}")
            imprimir_tabla()
        return True


def desconectar_ws(conn, ws_id):
    """Se ha cerrado el socket de un Monitor: su WS pasa a DESCONECTADA."""
    with lock:
        # Nada que hacer si esta conexión no llegó a registrar ninguna WS (ws_id = None) o si
        # la WS ya se ha vuelto a registrar por otra conexión más nueva (esa es la que vale)
        if conexiones.get(ws_id) is not conn:
            return
        del conexiones[ws_id]
        estado_anterior = estaciones[ws_id]["estado"]
        estaciones[ws_id]["estado"] = DESCONECTADA
        set_estado(ws_id, DESCONECTADA)
        log(f"ESTADO: {ws_id} {estado_anterior} -> {DESCONECTADA}")
        imprimir_tabla()


def procesar_telemetria(mensaje, prod=None):
    """Actualiza el estado local y reenvía telemetría/resumen al FO correspondiente."""
    if not isinstance(mensaje, dict):
        return False

    ws_id = mensaje.get("ws_id")
    if ws_id is None:
        return False

    with lock:
        if ws_id not in estaciones:
            return False

        if mensaje.get("tipo") == "TELEMETRIA":
            caudal = float(mensaje.get("caudal", estaciones[ws_id].get("caudal", 0.0)))
            volumen = float(mensaje.get("volumen", estaciones[ws_id].get("volumen", 0.0)))
            operario = mensaje.get("operario", estaciones[ws_id].get("operario"))
            estaciones[ws_id]["caudal"] = caudal
            estaciones[ws_id]["volumen"] = volumen
            estaciones[ws_id]["operario"] = operario
            estaciones[ws_id]["estado"] = REGANDO
            set_estado(ws_id, REGANDO)
            log(f"TELEMETRIA: {ws_id} caudal={caudal:.2f} L/min volumen={volumen:.2f} L")
            imprimir_tabla()

            if prod is not None and operario:
                mensaje_fo = {
                    "tipo": "TELEMETRIA",
                    "fo_id": str(operario),
                    "ws_id": ws_id,
                    "caudal": round(caudal, 2),
                    "volumen": round(volumen, 3),
                }
                _emitir_orden(prod, mensaje_fo)
            return True

        if mensaje.get("tipo") == "FIN":
            volumen = float(mensaje.get("volumen", estaciones[ws_id].get("volumen", 0.0)))
            motivo = mensaje.get("motivo", "FIN")
            operario = estaciones[ws_id].get("operario")
            if operario is not None:
                inicio = mensaje.get("inicio") or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                fin = mensaje.get("fin") or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                guardar_riego(ws_id, operario, inicio, fin, volumen)
            estaciones[ws_id]["caudal"] = 0.0
            estaciones[ws_id]["volumen"] = volumen
            estaciones[ws_id]["operario"] = None
            estaciones[ws_id]["estado"] = DISPONIBLE
            set_estado(ws_id, DISPONIBLE)
            log(f"FIN: {ws_id} motivo={motivo} volumen={volumen:.2f} L")
            imprimir_tabla()

            if prod is not None and operario:
                resumen = {
                    "tipo": "RESUMEN",
                    "fo_id": str(operario),
                    "ws_id": ws_id,
                    "volumen": round(volumen, 3),
                    "duracion": mensaje.get("duracion", 0),
                    "motivo": motivo,
                }
                _emitir_orden(prod, resumen)
            return True

        return False


def _emitir_orden(prod, mensaje):
    """Envía un mensaje al topic 'ordenes' usando el productor indicado o uno nuevo."""
    if mensaje is None or not isinstance(mensaje, dict):
        return None
    if prod is None:
        return None
    prod.send(TOPIC_ORDENES, mensaje)
    prod.flush()
    return mensaje


def publicar_estado_ws(prod=None, broker: str | None = None):
    """Publica la instantánea del estado de todas las WS en el topic 'ordenes'."""
    if prod is None and broker is None:
        return None
    if prod is None:
        prod = productor(broker)
        cerrar = True
    else:
        cerrar = False
    try:
        with lock:
            estaciones_snapshot = [
                {
                    "ws_id": ws_id,
                    "ubicacion": ws.get("ubicacion", ""),
                    "estado": ws.get("estado", DISPONIBLE),
                    "caudal": ws.get("caudal", 0.0),
                    "volumen": ws.get("volumen", 0.0),
                    "operario": ws.get("operario"),
                }
                for ws_id, ws in sorted(estaciones.items())
            ]
        mensaje = {"tipo": "ESTADO_WS", "estaciones": estaciones_snapshot}
        _emitir_orden(prod, mensaje)
        return mensaje
    finally:
        if cerrar:
            prod.close()


def orden_ws(ws_id, tipo, prod=None, **extra):
    """Envía una orden concreta a una WS y opcionalmente actualiza el estado local."""
    mensaje = {"tipo": tipo, "ws_id": ws_id, **extra}
    _emitir_orden(prod, mensaje)
    return mensaje


def bloquear_estacion(ws_id, prod=None, broker: str | None = None):
    """Bloquea la estación: la marca FUERA_SERVICIO y envía orden BLOQUEAR."""
    if not ws_id:
        return False

    with lock:
        if ws_id not in estaciones:
            return False
        estaciones[ws_id]["estado"] = FUERA_SERVICIO
        estaciones[ws_id]["operario"] = None
        set_estado(ws_id, FUERA_SERVICIO)
        log(f"ORDEN CENTRAL: {ws_id} -> {FUERA_SERVICIO}")
        imprimir_tabla()

    if prod is None and broker is not None:
        prod = productor(broker)
        close_prod = True
    else:
        close_prod = False
    try:
        return orden_ws(ws_id, "BLOQUEAR", prod=prod) is not None
    finally:
        if close_prod:
            prod.close()


def activar_estacion(ws_id, prod=None, broker: str | None = None):
    """Activa la estación; la deja DISONIBLE si estaba bloqueada."""
    if not ws_id:
        return False

    with lock:
        if ws_id not in estaciones:
            return False
        estaciones[ws_id]["estado"] = DISPONIBLE
        estaciones[ws_id]["caudal"] = 0.0
        estaciones[ws_id]["volumen"] = 0.0
        estaciones[ws_id]["operario"] = None
        set_estado(ws_id, DISPONIBLE)
        log(f"ORDEN CENTRAL: {ws_id} -> {DISPONIBLE}")
        imprimir_tabla()

    if prod is None and broker is not None:
        prod = productor(broker)
        close_prod = True
    else:
        close_prod = False
    try:
        return orden_ws(ws_id, "ACTIVAR", prod=prod) is not None
    finally:
        if close_prod:
            prod.close()


def procesar_peticion(datos, prod=None, broker: str | None = None):
    """Procesa una PETICION de Kafka y decide si autoriza o deniega el riego."""
    if not isinstance(datos, dict):
        return False
    if datos.get("tipo") != "PETICION":
        return False

    fo_id = str(datos.get("fo_id", "")).strip()
    ws_id = str(datos.get("ws_id", "")).strip()
    try:
        duracion = int(datos.get("duracion", 0))
    except (TypeError, ValueError):
        duracion = 0

    if not fo_id or not ws_id or duracion <= 0:
        mensaje = {"tipo": "DENEGADO", "fo_id": fo_id, "ws_id": ws_id, "motivo": "Petición inválida"}
        if prod is None and broker is not None:
            prod = productor(broker)
            close_prod = True
        else:
            close_prod = False
        try:
            _emitir_orden(prod, mensaje)
        finally:
            if close_prod:
                prod.close()
        return False

    if not operario_existe(fo_id.lower()):
        mensaje = {"tipo": "DENEGADO", "fo_id": fo_id, "ws_id": ws_id, "motivo": "Operario no válido"}
        if prod is None and broker is not None:
            prod = productor(broker)
            close_prod = True
        else:
            close_prod = False
        try:
            _emitir_orden(prod, mensaje)
        finally:
            if close_prod:
                prod.close()
        return False

    with lock:
        if ws_id not in estaciones:
            motivo = "La WS no existe"
        elif estaciones[ws_id]["estado"] != DISPONIBLE:
            motivo = f"WS en estado {estaciones[ws_id]['estado']}"
        else:
            motivo = None

    if motivo is not None:
        mensaje = {"tipo": "DENEGADO", "fo_id": fo_id, "ws_id": ws_id, "motivo": motivo}
        if prod is None and broker is not None:
            prod = productor(broker)
            close_prod = True
        else:
            close_prod = False
        try:
            _emitir_orden(prod, mensaje)
        finally:
            if close_prod:
                prod.close()
        return False

    mensaje_regar = {"tipo": "REGAR", "ws_id": ws_id, "duracion": duracion, "operario": fo_id}
    mensaje_autorizado = {"tipo": "AUTORIZADO", "fo_id": fo_id, "ws_id": ws_id, "duracion": duracion}

    with lock:
        estaciones[ws_id]["estado"] = REGANDO
        estaciones[ws_id]["operario"] = fo_id
        set_estado(ws_id, REGANDO)
        log(f"PETICION AUTORIZADA: {fo_id} -> {ws_id} durante {duracion}s")
        imprimir_tabla()

    if prod is None and broker is not None:
        prod = productor(broker)
        close_prod = True
    else:
        close_prod = False
    try:
        _emitir_orden(prod, mensaje_regar)
        _emitir_orden(prod, mensaje_autorizado)
    finally:
        if close_prod:
            prod.close()
    return True


def consumir_telemetria(broker):
    """Hilo consumidor del topic 'telemetria' de Kafka."""
    prod = productor(broker)
    try:
        consumer = consumidor(broker, TOPIC_TELEMETRIA, group_id="central_telemetria")
        for mensaje in consumer:
            if not isinstance(mensaje.value, dict):
                continue
            procesar_telemetria(mensaje.value, prod=prod)
    except Exception as exc:  # noqa: BLE001 - se trata como error de sistema en la consola
        log(f"ERROR Kafka telemetria ({broker}): {exc}")
    finally:
        prod.close()


def consumir_peticiones(broker):
    """Hilo consumidor del topic 'peticiones' de Kafka: valida riegos y reenvía autorizaciones."""
    try:
        consumer = consumidor(broker, TOPIC_PETICIONES, group_id="central_peticiones")
        for mensaje in consumer:
            if not isinstance(mensaje.value, dict):
                continue
            try:
                procesar_peticion(mensaje.value, broker=broker)
            except Exception as exc:  # noqa: BLE001
                log(f"ERROR procesando petición: {exc}")
    except Exception as exc:  # noqa: BLE001
        log(f"ERROR Kafka peticiones ({broker}): {exc}")


def menu(broker):
    """Menú interactivo del operador de Central para gestión manual de las estaciones."""
    ayuda = "1 iniciar  2 bloquear  3 activar  4 estado  0 salir"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if opcion in ("1", "iniciar"): 
            ws_id = input("ws_id (o ALL): ").strip() or "ALL"
            try:
                duracion = int(input("duracion (segundos): ").strip())
                if duracion <= 0:
                    raise ValueError
            except ValueError:
                print("duracion invalida, debe ser un entero > 0")
                continue
            if ws_id == "ALL":
                with lock:
                    ids = list(sorted(estaciones))
                for ws in ids:
                    procesar_peticion({"tipo": "PETICION", "fo_id": "MANUAL", "ws_id": ws, "duracion": duracion}, broker=broker)
            else:
                procesar_peticion({"tipo": "PETICION", "fo_id": "MANUAL", "ws_id": ws_id, "duracion": duracion}, broker=broker)

        elif opcion in ("2", "bloquear"):
            ws_id = input("ws_id (o ALL): ").strip() or "ALL"
            with lock:
                ids = list(sorted(estaciones)) if ws_id == "ALL" else [ws_id]
            prod = productor(broker)
            try:
                for ws in ids:
                    bloquear_estacion(ws, prod=prod)
            finally:
                prod.close()

        elif opcion in ("3", "activar"):
            ws_id = input("ws_id (o ALL): ").strip() or "ALL"
            with lock:
                ids = list(sorted(estaciones)) if ws_id == "ALL" else [ws_id]
            prod = productor(broker)
            try:
                for ws in ids:
                    activar_estacion(ws, prod=prod)
            finally:
                prod.close()

        elif opcion in ("4", "estado", "e"):
            with lock:
                imprimir_tabla()

        elif opcion in ("0", "q", "salir"):
            print("[Central] saliendo del menú")
            break
        else:
            print(ayuda)


def publicar_estado_periodico(broker, pausa=2.0):
    """Publica ESTADO_WS cada 2 s para que los FO sepan qué estaciones están disponibles."""
    while True:
        try:
            publicar_estado_ws(broker=broker)
        except Exception as exc:  # noqa: BLE001
            log(f"ERROR publicando ESTADO_WS: {exc}")
        time.sleep(pausa)


def atender_monitor(conn, addr):
    """Hilo que atiende a un WM_WS_M mientras su socket siga abierto."""
    origen = f"{addr[0]}:{addr[1]}"
    log(f"NUEVA CONEXION desde {origen}")
    ws_id = None  # la WS de este Monitor: se sabe cuando llega su REG

    try:
        while True:
            msg = recibir(conn)
            if msg is None:  # el Monitor ha cerrado el socket
                break
            partes = msg.split("#")

            if partes[0] == "REG":
                # REG#<id_ws>#<ubicacion>  ->  se responde OK o ERROR#<motivo>
                if len(partes) != 3 or partes[1] == "" or partes[2] == "":
                    log(f"REG rechazado desde {origen}: formato incorrecto ('{msg}')")
                    enviar(conn, "ERROR#Formato incorrecto, se esperaba REG#<id_ws>#<ubicacion>")
                elif ws_id is not None and partes[1] != ws_id:
                    log(f"REG rechazado desde {origen}: esta conexión ya es la de {ws_id}")
                    enviar(conn, f"ERROR#Esta conexion ya esta registrada como {ws_id}")
                else:
                    ws_id = partes[1]
                    registrar_ws(conn, ws_id, partes[2])
                    enviar(conn, "OK")

            elif partes[0] in ("FUGA", "RESUELTA") and len(partes) == 2:
                # FUGA#<id_ws> -> FUGA  ·  RESUELTA#<id_ws> -> DISPONIBLE  (no se responde nada)
                nuevo_estado = FUGA if partes[0] == "FUGA" else DISPONIBLE
                if not cambiar_estado(conn, partes[1], nuevo_estado):
                    log(f"AVISO: '{msg}' desde {origen} ignorado: esa WS no se ha registrado por esta conexión")

            else:
                log(f"AVISO: mensaje no válido desde {origen}: '{msg}'")

    except (OSError, ValueError) as e:
        # OSError: la conexión se ha cortado de golpe · ValueError: la cabecera no es un número
        log(f"ERROR en la conexión {origen}: {e}")
    finally:
        conn.close()
        log(f"CONEXION CERRADA {origen}")
        desconectar_ws(conn, ws_id)


def main():
    if len(sys.argv) != 3:
        print("USO: python -m central.WM_Central <puerto_escucha> <ip_broker:puerto>")
        sys.exit(1)

    try:
        puerto = int(sys.argv[1])
        ip_broker, puerto_broker = sys.argv[2].rsplit(":", 1)
        if not 0 < puerto < 65536 or ip_broker == "" or not 0 < int(puerto_broker) < 65536:
            raise ValueError
    except ValueError:
        print("Argumentos incorrectos. Ejemplo: python -m central.WM_Central 5050 127.0.0.1:9092")
        sys.exit(1)
    broker = sys.argv[2]  # Kafka: se acepta ya, pero no se usa hasta la semana 2

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        servidor.bind(("", puerto))  # "" = todas las interfaces (hace falta en Docker/Railway)
    except OSError as e:
        print(f"No se puede escuchar en el puerto {puerto}: {e}")
        sys.exit(1)
    servidor.listen()
    # accept() se despierta cada segundo: en Windows, si se queda bloqueado, Ctrl+C no lo para
    servidor.settimeout(1)

    crear_tablas()
    cargar_estaciones()
    log(f"WM_Central escuchando en el puerto {puerto} (broker Kafka {broker})")
    with lock:
        imprimir_tabla()

    consumo_kafka = threading.Thread(
        target=consumir_telemetria,
        args=(broker,),
        daemon=True,
    )
    consumo_kafka.start()

    consumo_peticiones = threading.Thread(
        target=consumir_peticiones,
        args=(broker,),
        daemon=True,
    )
    consumo_peticiones.start()

    estado_periodico = threading.Thread(
        target=publicar_estado_periodico,
        args=(broker,),
        daemon=True,
    )
    estado_periodico.start()

    menu_thread = threading.Thread(
        target=menu,
        args=(broker,),
        daemon=True,
    )
    menu_thread.start()

    iniciar_panel(estaciones, lock, log_buffer)  # panel web: http://localhost:8080 (PANEL_PORT)

    try:
        while True:
            try:
                conn, addr = servidor.accept()
            except socket.timeout:
                continue  # nadie se ha conectado en este segundo: seguir esperando
            # Un hilo por Monitor y sin límite de conexiones.
            # daemon=True: los hilos no impiden que el programa termine con Ctrl+C
            threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        log("WM_Central detenida (Ctrl+C)")
    finally:
        servidor.close()


if __name__ == "__main__":
    main()
