"""
WM_Central.py: CENTRAL, el cerebro del sistema (nombre obligatorio: WM_Central)
Responsable: Abde · Semanas 1, 2 y 3

USO
    python -m central.WM_Central <puerto_escucha> <ip_broker:puerto>
    (ejecutar desde la raíz del repo para poder importar common/)
    Panel web: http://localhost:8080  (otro puerto: variable de entorno PANEL_PORT)

SEMANA 1: solo sockets (Tarjeta: "WM_Central: servidor de sockets + registro de WS")
  - Servidor concurrente: un hilo por cada WM_WS_M conectado. SIN límite de conexiones
  - Estado en memoria: dict {ws_id: {...}} protegido con threading.Lock
  - REG#id#ubicacion -> alta o actualización -> DISPONIBLE, responder OK
  - FUGA#id -> FUGA   ·   RESUELTA#id -> DISPONIBLE
  - Si se cierra el socket de un Monitor -> DESCONECTADA
  - Imprimir la tabla de estados cada vez que cambie algo

SEMANA 2 (Tarjetas: "SQLite..." + "estados de riego + consumo de telemetría por Kafka")
  - Al arrancar: cargar las WS de la BD (central/db.py) -> todas DESCONECTADA
  - Cada cambio de estado -> también a la BD
  - Hilo consumidor de 'telemetria': caudal y volumen cada 1 s · FIN -> DISPONIBLE

SEMANA 3 (Tarjetas: "flujo de activación de riego" + "menú de órdenes")
  - Hilo consumidor de 'peticiones': validar operario y WS DISPONIBLE -> AUTORIZADO / DENEGADO
  - Reenviar al FO la telemetría de su riego y el RESUMEN final
  - Cada ~2 s publicar ESTADO_WS (lista de WS) en 'ordenes'
  - Menú (punto 11 del enunciado): iniciar riego / bloquear / activar, a una WS o a todas (ALL)
    -> también desde el panel web (central/panel.py + ui/panel.html)

ESTADOS: DISPONIBLE · REGANDO · FUGA · FUERA_SERVICIO · DESCONECTADA
MENSAJES: docs/PROTOCOLO.md
-------------------------------------------------------------------------------
Basado en ejemplos/servidor_concurrente.py: un hilo por cada Monitor conectado.
"""

import socket
import sys
import threading
import time
from datetime import datetime

from central.db import (
    crear_tablas,
    get_estaciones,
    get_operarios,
    guardar_estacion,
    guardar_operario,
    guardar_riego,
    operario_existe,
    set_estado,
)
from central.panel import foto_estado, iniciar_panel
from common.kafka_utils import (
    TOPIC_ORDENES,
    TOPIC_PETICIONES,
    TOPIC_TELEMETRIA,
    consumidor,
    productor,
)
from common.sockets_utils import enviar, recibir
from common.web import Registro

DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"
ESTADOS = (DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA)

OPERARIO_CENTRAL = "CENTRAL"   # fo_id de los riegos ordenados desde el menú / panel de Central
REINTENTO_KAFKA = 3            # segundos entre reintentos si Kafka no responde

# Estado de todas las WS, en memoria (y en SQLite):
#   {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario",
#            "inicio_riego", "duracion", "ultimo_riego", "motivo_fuga"}}
estaciones = {}

# Socket del Monitor por el que se ha registrado cada WS conectada: {ws_id: conn}
# Sirve para saber qué WS pasa a DESCONECTADA cuando se cierra un socket.
conexiones = {}

# Hay un hilo por Monitor y todos usan estaciones y conexiones:
# solo se leen o se modifican con este lock cogido.
lock = threading.Lock()

# Eventos para el panel web (los más importantes; la telemetría de cada segundo no)
eventos = Registro()

# Datos de Central que enseña el panel
info = {"kafka": "conectando", "broker": "", "puerto_sockets": 0, "inicio": time.time()}


def log(texto, nivel=None, panel=True):
    linea = f"[{time.strftime('%H:%M:%S')}] {texto}"
    print(linea, flush=True)
    if panel:
        eventos.add(texto, nivel or _nivel(texto))


def _nivel(texto):
    """Color de la línea en el registro del panel."""
    if texto.startswith("ERROR") or "-> FUGA" in texto or "motivo=FUGA" in texto:
        return "ALERTA"
    if texto.startswith("AVISO") or "DENEGAD" in texto or "-> DESCONECTADA" in texto or "-> FUERA" in texto:
        return "AVISO"
    if texto.startswith("REG") or "AUTORIZADA" in texto or "-> DISPONIBLE" in texto:
        return "OK"
    return "INFO"


def _nueva_ws(ubicacion, estado):
    return {"ubicacion": ubicacion, "estado": estado, "caudal": 0.0, "volumen": 0.0,
            "operario": None, "inicio_riego": None, "duracion": 0, "ultimo_riego": None,
            "motivo_fuga": None}


def _limpiar_riego(ws):
    ws["caudal"] = 0.0
    ws["operario"] = None
    ws["inicio_riego"] = None
    ws["duracion"] = 0


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
    print("\n".join(lineas), flush=True)


def registrar_ws(conn, ws_id, ubicacion):
    """REG: da de alta la WS (o la actualiza si ya existía) y la deja DISPONIBLE."""
    with lock:
        ya_existia = ws_id in estaciones
        anterior = conexiones.get(ws_id)
        ws = estaciones.setdefault(ws_id, _nueva_ws(ubicacion, DISPONIBLE))
        ws["ubicacion"] = ubicacion
        ws["estado"] = DISPONIBLE
        ws["motivo_fuga"] = None
        ws["volumen"] = 0.0
        _limpiar_riego(ws)
        conexiones[ws_id] = conn
        guardar_estacion(ws_id, ubicacion)
        set_estado(ws_id, DISPONIBLE)

        if ya_existia:
            log(f"REG: {ws_id} autenticada (ubicación: {ubicacion})")
        else:
            log(f"REG: nueva WS {ws_id} (ubicación: {ubicacion})")
        if anterior is not None and anterior is not conn:
            log(f"AVISO: {ws_id} ya tenía otra conexión abierta; a partir de ahora vale esta")
        imprimir_tabla()


def cargar_estaciones():
    """Restaura las estaciones persistidas y las marca desconectadas al iniciar."""
    with lock:
        for ws_id, ubicacion, _estado_anterior in get_estaciones():
            estaciones[ws_id] = _nueva_ws(ubicacion, DESCONECTADA)
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

        ws = estaciones[ws_id]
        estado_anterior = ws["estado"]
        if nuevo_estado == estado_anterior == FUGA:
            return True   # ya se sabía (p. ej. por el FIN del riego): no se repite
        if nuevo_estado in ESTADOS:
            ws["estado"] = nuevo_estado
            if nuevo_estado == DISPONIBLE:
                ws["volumen"] = 0.0
                ws["motivo_fuga"] = None
                _limpiar_riego(ws)
            elif nuevo_estado == FUGA:
                ws["motivo_fuga"] = "Avería avisada por el monitor"
                ws["caudal"] = 0.0
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
        _limpiar_riego(estaciones[ws_id])
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
        ws = estaciones[ws_id]

        if mensaje.get("tipo") == "TELEMETRIA":
            caudal = float(mensaje.get("caudal", ws.get("caudal", 0.0)))
            volumen = float(mensaje.get("volumen", ws.get("volumen", 0.0)))
            operario = mensaje.get("operario", ws.get("operario"))
            ws["caudal"] = caudal
            ws["volumen"] = volumen
            ws["operario"] = operario
            # Una telemetría que llega tarde no saca a la WS de FUGA / FUERA_SERVICIO
            if ws["estado"] in (DISPONIBLE, REGANDO):
                if ws["estado"] != REGANDO:
                    set_estado(ws_id, REGANDO)
                ws["estado"] = REGANDO
                if not ws.get("inicio_riego"):   # riego lanzado desde el propio Engine
                    ws["inicio_riego"] = time.time()
            log(f"TELEMETRIA: {ws_id} caudal={caudal:.2f} L/min volumen={volumen:.2f} L", panel=False)

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
            volumen = float(mensaje.get("volumen", ws.get("volumen", 0.0)))
            motivo = mensaje.get("motivo", "FIN")
            operario = ws.get("operario")
            if operario is not None:
                inicio = mensaje.get("inicio") or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                fin = mensaje.get("fin") or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                guardar_riego(ws_id, operario, inicio, fin, volumen)
            _limpiar_riego(ws)
            ws["volumen"] = volumen
            ws["ultimo_riego"] = time.time()
            # Si el riego acabó por FUGA o BLOQUEO la WS se queda en ese estado. El FIN por FUGA
            # suele llegar (Kafka) antes que el aviso del Monitor (socket): se pasa ya a FUGA
            if ws["estado"] == REGANDO:
                ws["estado"] = FUGA if motivo == "FUGA" else DISPONIBLE
                if motivo == "FUGA":
                    ws["motivo_fuga"] = "Fuga detectada durante el riego"
                set_estado(ws_id, ws["estado"])
            log(f"FIN: {ws_id} motivo={motivo} volumen={volumen:.2f} L -> {ws['estado']}")
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
    """Envía un mensaje al topic 'ordenes' usando el productor indicado."""
    if mensaje is None or not isinstance(mensaje, dict):
        return None
    if prod is None:
        return None
    prod.send(TOPIC_ORDENES, mensaje)
    prod.flush()
    return mensaje


def _emitir_con(prod, broker, *mensajes):
    """Emite los mensajes con prod o, si no hay, con un productor temporal sobre broker."""
    cerrar = prod is None and broker is not None
    if cerrar:
        prod = productor(broker)
    try:
        for mensaje in mensajes:
            _emitir_orden(prod, mensaje)
    finally:
        if cerrar:
            prod.close()


def _mensaje_estado_ws():
    with lock:
        estaciones_snapshot = [
            {
                "ws_id": ws_id,
                "ubicacion": ws.get("ubicacion", ""),
                "estado": ws.get("estado", DISPONIBLE),
                "caudal": ws.get("caudal", 0.0),
                "volumen": ws.get("volumen", 0.0),
                "operario": ws.get("operario"),
                "motivo_fuga": ws.get("motivo_fuga"),
            }
            for ws_id, ws in sorted(estaciones.items())
        ]
    return {"tipo": "ESTADO_WS", "estaciones": estaciones_snapshot}


def publicar_estado_ws(prod=None, broker: str | None = None):
    """Publica la instantánea del estado de todas las WS en el topic 'ordenes'."""
    if prod is None and broker is None:
        return None
    mensaje = _mensaje_estado_ws()
    _emitir_con(prod, broker, mensaje)
    return mensaje


def orden_ws(ws_id, tipo, prod=None, **extra):
    """Envía una orden concreta a una WS."""
    mensaje = {"tipo": tipo, "ws_id": ws_id, **extra}
    _emitir_orden(prod, mensaje)
    return mensaje


def bloquear_estacion(ws_id, prod=None, broker: str | None = None):
    """Bloquea la estación: la marca FUERA_SERVICIO y envía orden BLOQUEAR."""
    if not ws_id:
        return False

    with lock:
        if ws_id not in estaciones or estaciones[ws_id]["estado"] == DESCONECTADA:
            return False
        anterior = estaciones[ws_id]["estado"]
        estaciones[ws_id]["estado"] = FUERA_SERVICIO
        estaciones[ws_id]["caudal"] = 0.0
        set_estado(ws_id, FUERA_SERVICIO)
        log(f"ORDEN CENTRAL: {ws_id} {anterior} -> {FUERA_SERVICIO}")
        imprimir_tabla()

    _emitir_con(prod, broker, {"tipo": "BLOQUEAR", "ws_id": ws_id})
    return True


def activar_estacion(ws_id, prod=None, broker: str | None = None):
    """Activa la estación: si estaba FUERA_SERVICIO vuelve a DISPONIBLE."""
    if not ws_id:
        return False

    with lock:
        if ws_id not in estaciones or estaciones[ws_id]["estado"] != FUERA_SERVICIO:
            return False
        ws = estaciones[ws_id]
        ws["estado"] = DISPONIBLE
        ws["volumen"] = 0.0
        _limpiar_riego(ws)
        set_estado(ws_id, DISPONIBLE)
        log(f"ORDEN CENTRAL: {ws_id} {FUERA_SERVICIO} -> {DISPONIBLE}")
        imprimir_tabla()

    _emitir_con(prod, broker, {"tipo": "ACTIVAR", "ws_id": ws_id})
    return True


def procesar_peticion(datos, prod=None, broker: str | None = None, desde_central=False):
    """Procesa una PETICION y decide si autoriza o deniega el riego.

    desde_central=True: la ordena el operador de Central (menú / panel), sin operario de la BD.
    Devuelve True si se autoriza.
    """
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
        motivo = "Petición inválida"
    elif not desde_central and not operario_existe(fo_id.lower()):
        motivo = "Operario no válido"
    else:
        with lock:
            if ws_id not in estaciones:
                motivo = "La WS no existe"
            elif estaciones[ws_id]["estado"] != DISPONIBLE:
                motivo = f"WS en estado {estaciones[ws_id]['estado']}"
            else:
                motivo = None
                ws = estaciones[ws_id]
                ws["estado"] = REGANDO
                ws["operario"] = fo_id
                ws["inicio_riego"] = time.time()
                ws["duracion"] = duracion
                ws["volumen"] = 0.0
                set_estado(ws_id, REGANDO)
                log(f"PETICION AUTORIZADA: {fo_id} -> {ws_id} durante {duracion}s")
                imprimir_tabla()

    if motivo is not None:
        log(f"PETICION DENEGADA: {fo_id or '?'} -> {ws_id or '?'}: {motivo}")
        _emitir_con(prod, broker, {"tipo": "DENEGADO", "fo_id": fo_id, "ws_id": ws_id, "motivo": motivo})
        return False

    _emitir_con(
        prod, broker,
        {"tipo": "REGAR", "ws_id": ws_id, "duracion": duracion, "operario": fo_id},
        {"tipo": "AUTORIZADO", "fo_id": fo_id, "ws_id": ws_id, "duracion": duracion},
    )
    return True


def ordenar(accion, ws_id, broker, duracion=0):
    """Orden del operador de Central (menú o panel) a una WS o a todas ('ALL' / '*').

    Devuelve (hechas, total): cuántas WS la han aceptado de cuántas se intentó.
    """
    with lock:
        ids = sorted(estaciones) if ws_id in ("ALL", "*") else [ws_id]
    if accion == "regar":
        hechas = sum(
            procesar_peticion({"tipo": "PETICION", "fo_id": OPERARIO_CENTRAL, "ws_id": ws,
                               "duracion": duracion}, broker=broker, desde_central=True)
            for ws in ids
        )
        return hechas, len(ids)
    funcion = {"bloquear": bloquear_estacion, "activar": activar_estacion}.get(accion)
    if funcion is None:
        raise ValueError(f"Orden desconocida: {accion}")
    prod = productor(broker)
    try:
        hechas = sum(funcion(ws, prod=prod) for ws in ids)
    finally:
        prod.close()
    return hechas, len(ids)


def _consumir(broker, topic, group_id, procesar):
    """Bucle de un consumidor de Kafka: si Kafka cae, se reintenta cada pocos segundos."""
    while True:
        consumer = None
        try:
            # session 6 s (por defecto 45): tras reiniciar Central, se recupera antes el grupo
            consumer = consumidor(broker, topic, group_id=group_id,
                                  session_timeout_ms=6000, heartbeat_interval_ms=2000)
            for mensaje in consumer:
                if isinstance(mensaje.value, dict):
                    try:
                        procesar(mensaje.value)
                    except Exception as exc:  # noqa: BLE001
                        log(f"ERROR procesando mensaje de '{topic}': {exc}")
        except Exception as exc:  # noqa: BLE001 - se trata como error de sistema en la consola
            log(f"ERROR Kafka '{topic}' ({broker}): {exc}. Reintentando en {REINTENTO_KAFKA} s")
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:  # noqa: BLE001
                    pass
        time.sleep(REINTENTO_KAFKA)


def consumir_telemetria(broker):
    """Hilo consumidor del topic 'telemetria' de Kafka."""
    prod = productor(broker)
    _consumir(broker, TOPIC_TELEMETRIA, "central_telemetria",
              lambda datos: procesar_telemetria(datos, prod=prod))


def consumir_peticiones(broker):
    """Hilo consumidor del topic 'peticiones' de Kafka: valida riegos y reenvía autorizaciones."""
    _consumir(broker, TOPIC_PETICIONES, "central_peticiones",
              lambda datos: procesar_peticion(datos, broker=broker))


def menu(broker):
    """Menú interactivo del operador de Central para gestión manual de las estaciones."""
    ayuda = "1 iniciar  2 bloquear  3 activar  4 estado  0 salir del menú  (también desde el panel web)"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("[Central] sin teclado: las órdenes se dan desde el panel web")
            break

        try:
            if opcion in ("1", "iniciar"):
                ws_id = input("ws_id (o ALL): ").strip() or "ALL"
                try:
                    duracion = int(input("duracion (segundos): ").strip())
                    if duracion <= 0:
                        raise ValueError
                except ValueError:
                    print("duracion invalida, debe ser un entero > 0")
                    continue
                hechas, total = ordenar("regar", ws_id, broker, duracion)
                print(f"riego autorizado en {hechas} de {total} WS")

            elif opcion in ("2", "bloquear", "3", "activar"):
                accion = "bloquear" if opcion in ("2", "bloquear") else "activar"
                ws_id = input("ws_id (o ALL): ").strip() or "ALL"
                hechas, total = ordenar(accion, ws_id, broker)
                print(f"{accion}: {hechas} de {total} WS")

            elif opcion in ("4", "estado", "e"):
                with lock:
                    imprimir_tabla()

            elif opcion in ("0", "q", "salir"):
                print("[Central] saliendo del menú")
                break
            else:
                print(ayuda)
        except Exception as exc:  # noqa: BLE001 - p. ej. Kafka caído
            print(f"No se pudo hacer: {exc}")


def publicar_estado_periodico(broker, pausa=2.0):
    """Publica ESTADO_WS cada 2 s para que los FO sepan qué estaciones están disponibles.
    De paso sirve de latido para saber si Kafka responde (lo enseña el panel)."""
    prod = None
    while True:
        try:
            if prod is None:
                prod = productor(broker, max_block_ms=5000)
            # .get(): espera la confirmación del broker (máx. 5 s) para saber si Kafka responde
            prod.send(TOPIC_ORDENES, _mensaje_estado_ws()).get(timeout=5)
            if info["kafka"] != "conectado":
                log(f"Kafka conectado ({broker})", "OK")
            info["kafka"] = "conectado"
        except Exception as exc:  # noqa: BLE001
            if info["kafka"] != "sin conexión":
                log(f"ERROR Kafka ({broker}): {exc}. Se reintenta solo")
            info["kafka"] = "sin conexión"
            if prod is not None:
                try:
                    prod.close()
                except Exception:  # noqa: BLE001
                    pass
            prod = None
        time.sleep(pausa)


# ---------------------- Panel web --------------------------------------------
def estado_panel():
    with lock:
        monitores = len(conexiones)
    central = {**info, "monitores": monitores}
    return foto_estado(estaciones, lock, eventos, central, get_operarios())


def accion_orden(cuerpo):
    """POST /api/orden desde el panel."""
    accion = cuerpo.get("accion")
    ws_id = str(cuerpo.get("ws") or "").strip()
    if not ws_id:
        raise ValueError("Falta la estación")
    duracion = 0
    if accion == "regar":
        try:
            duracion = int(cuerpo.get("duracion") or 0)
        except (TypeError, ValueError):
            duracion = 0
        if not 0 < duracion <= 3600:
            raise ValueError("La duración debe estar entre 1 y 3600 s")
    if info["kafka"] != "conectado":
        raise ValueError("Kafka no responde: las órdenes no llegarían a las estaciones")
    hechas, total = ordenar(accion, ws_id, info["broker"], duracion)
    if total == 0:
        raise ValueError("No hay estaciones")
    nombre = "todas las estaciones" if ws_id in ("*", "ALL") else ws_id
    verbo = {"regar": "Riego autorizado", "bloquear": "Bloqueada", "activar": "Activada"}[accion]
    if hechas == 0:
        raise ValueError(f"{nombre}: no está en un estado que lo permita")
    return {"ok": True, "mensaje": f"{verbo}: {nombre} ({hechas} de {total})" if total > 1 else f"{verbo}: {nombre}"}


def accion_operario(cuerpo):
    """POST /api/operarios: alta de un operario."""
    fo_id = str(cuerpo.get("id") or "").strip()
    nombre = str(cuerpo.get("nombre") or "").strip()
    if not fo_id or not nombre or "#" in fo_id or " " in fo_id:
        raise ValueError("Id (sin espacios) y nombre obligatorios")
    guardar_operario(fo_id, nombre)
    log(f"Operario {fo_id.upper()} dado de alta ({nombre})", "OK")
    return {"ok": True, "mensaje": f"Operario {fo_id.upper()} dado de alta"}


# ---------------------- Sockets con los Monitor -------------------------------
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
                    log(f"REG rechazado desde {origen}: formato incorrecto ('{msg}')", "AVISO")
                    enviar(conn, "ERROR#Formato incorrecto, se esperaba REG#<id_ws>#<ubicacion>")
                elif ws_id is not None and partes[1] != ws_id:
                    log(f"REG rechazado desde {origen}: esta conexión ya es la de {ws_id}", "AVISO")
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
        log(f"CONEXION CERRADA {origen}", panel=False)
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
    broker = sys.argv[2]
    info["broker"] = broker
    info["puerto_sockets"] = puerto

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
    log(f"WM_Central escuchando en el puerto {puerto} (broker Kafka {broker})", "OK")
    with lock:
        imprimir_tabla()

    for hilo in (consumir_telemetria, consumir_peticiones, publicar_estado_periodico, menu):
        threading.Thread(target=hilo, args=(broker,), daemon=True).start()

    # panel web: http://localhost:8080 (PANEL_PORT)
    iniciar_panel(estado_panel, {"orden": accion_orden, "operarios": accion_operario})

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
