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

from common.sockets_utils import enviar, recibir

# Estados que se usan en la semana 1 (REGANDO y FUERA_SERVICIO llegan en las semanas 2 y 3)
DISPONIBLE = "DISPONIBLE"
FUGA = "FUGA"
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


def log(texto):
    print(f"[{time.strftime('%H:%M:%S')}] {texto}")


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

        if ya_existia:
            log(f"REG: {ws_id} ya existía, se actualiza (ubicación: {ubicacion})")
        else:
            log(f"REG: nueva WS {ws_id} (ubicación: {ubicacion})")
        if anterior is not None and anterior is not conn:
            log(f"AVISO: {ws_id} ya tenía otra conexión abierta; a partir de ahora vale esta")
        imprimir_tabla()


def cambiar_estado(conn, ws_id, nuevo_estado):
    """FUGA / RESUELTA. Solo se aceptan de la conexión por la que se registró esa WS:
    devuelve False si no es así (WS sin registrar o que pertenece a otro Monitor)."""
    with lock:
        if conexiones.get(ws_id) is not conn:
            return False
        estado_anterior = estaciones[ws_id]["estado"]
        if estado_anterior != nuevo_estado:
            estaciones[ws_id]["estado"] = nuevo_estado
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
        log(f"ESTADO: {ws_id} {estado_anterior} -> {DESCONECTADA}")
        imprimir_tabla()


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

    log(f"WM_Central escuchando en el puerto {puerto} (broker Kafka {broker}: todavía no se usa)")
    with lock:
        imprimir_tabla()

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
