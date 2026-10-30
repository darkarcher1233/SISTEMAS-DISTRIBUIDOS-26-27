"""
WM_FO.py: aplicación del OPERARIO DE CAMPO (nombre obligatorio: WM_FO)
Responsable: Vlad · Semana 3 · Tarjetas: "WM_FO: modo manual" + "WM_FO: peticiones desde fichero"
Solo usa KAFKA (no tiene sockets).

USO
    python -m fo.WM_FO <ip_broker:puerto> <id_operario>

QUÉ HACE
  - Menú:  1 ver WS disponibles · 2 pedir riego (ws_id + duración) · 3 leer fichero · 0 salir
  - Pedir riego -> publica en 'peticiones'  {tipo: PETICION, fo_id, ws_id, duracion}
  - Hilo consumidor de 'ordenes', quedándose SOLO con los mensajes de su fo_id:
        AUTORIZADO · DENEGADO (+motivo) · TELEMETRIA de su WS · RESUMEN final
        (+ aviso claro si el riego acaba por FUGA o BLOQUEO)
  - Ver WS disponibles: guarda el último ESTADO_WS que publica Central y lo muestra
  - Modo fichero (fo/peticiones/*.txt, una línea "ws_id;duracion"):
        envía la primera -> espera a que termine (bien o mal) -> espera 4 s -> la siguiente

HECHO CUANDO: 2 FO piden riego a la vez y cada uno ve solo lo suyo
Mensajes: docs/PROTOCOLO.md
"""

from __future__ import annotations

import os
import sys
import threading
import time

from common.kafka_utils import (
    TOPIC_ORDENES,
    TOPIC_PETICIONES,
    consumidor,
    productor,
)

PAUSA_ENTRE_PETICIONES = 4   # segundos entre una petición del fichero y la siguiente
MARGEN_ESPERA = 30           # segundos extra (sobre la duración) antes de dar por perdida una petición
ALARMAS = ("FUGA", "BLOQUEO")

fo_id: str = ""

# ---------------------- Estado compartido con el hilo consumidor -------------------
estado_lock = threading.Lock()
ultimo_estado_ws: list[dict] | None = None    # última lista de WS recibida en ESTADO_WS
pendientes: dict[str, threading.Event] = {}  # ws_id -> se activa al llegar DENEGADO o RESUMEN
kafka_listo = threading.Event()              # el consumidor ya tiene particiones asignadas
kafka_caido = threading.Event()              # el consumidor ha fallado (sin Kafka no hay FO)


def mostrar(texto: str):
    """Print desde el hilo consumidor: salto de línea delante para no pisar el prompt."""
    print(f"\n{texto}", flush=True)


def alarma(texto: str):
    linea = "!" * 60
    mostrar(f"{linea}\n!!!  {texto}\n{linea}")


def normalizar_estaciones(estaciones) -> list[dict]:
    """ESTADO_WS puede traer una lista [{ws_id, ...}] o el dict de Central {ws_id: {...}}."""
    if isinstance(estaciones, dict):
        return [{"ws_id": ws_id, **datos} for ws_id, datos in estaciones.items()]
    if isinstance(estaciones, list):
        return [ws for ws in estaciones if isinstance(ws, dict)]
    return []


def terminar_peticion(ws_id):
    """La petición sobre ws_id ha acabado (denegada o con RESUMEN): despierta al modo fichero."""
    with estado_lock:
        evento = pendientes.pop(ws_id, None)
    if evento is not None:
        evento.set()


# ---------------------- Hilo consumidor de 'ordenes' ---------------------------------
def procesar_orden(datos):
    """Procesa un mensaje de 'ordenes'. Solo se queda con ESTADO_WS y con lo de su fo_id."""
    global ultimo_estado_ws
    if not isinstance(datos, dict):
        return

    tipo = datos.get("tipo")
    if tipo == "ESTADO_WS":
        with estado_lock:
            ultimo_estado_ws = normalizar_estaciones(datos.get("estaciones"))
        return

    if datos.get("fo_id") != fo_id:
        return   # es de otro operario (o una orden para una WS)

    ws_id = datos.get("ws_id", "?")
    if tipo == "AUTORIZADO":
        mostrar(f"[{fo_id}] AUTORIZADO: riego en {ws_id} durante {datos.get('duracion', '?')} s")

    elif tipo == "DENEGADO":
        mostrar(f"[{fo_id}] DENEGADO en {ws_id}: {datos.get('motivo', 'sin motivo')}")
        terminar_peticion(ws_id)

    elif tipo == "TELEMETRIA":
        mostrar(f"[{fo_id}] {ws_id} caudal={datos.get('caudal', 0)} L/min "
                f"volumen={datos.get('volumen', 0)} L")

    elif tipo == "RESUMEN":
        motivo = datos.get("motivo", "?")
        if motivo in ALARMAS:
            alarma(f"RIEGO EN {ws_id} INTERRUMPIDO POR {motivo}")
        mostrar(f"[{fo_id}] RESUMEN {ws_id}: volumen={datos.get('volumen', 0)} L "
                f"duracion={datos.get('duracion', 0)} s motivo={motivo}")
        terminar_peticion(ws_id)


def consumir_ordenes(broker: str):
    """Lee 'ordenes' sin grupo: cada FO recibe TODOS los mensajes nuevos y filtra los suyos."""
    try:
        # group_id=None: sin commits, cada arranque empieza en "latest" (no repite riegos viejos)
        cons = consumidor(broker, TOPIC_ORDENES, group_id=None,
                          auto_offset_reset="latest", enable_auto_commit=False)
        while True:
            lotes = cons.poll(timeout_ms=500)
            if not kafka_listo.is_set() and cons.assignment():
                kafka_listo.set()
            for mensajes in lotes.values():
                for mensaje in mensajes:
                    try:
                        procesar_orden(mensaje.value)
                    except Exception as exc:  # noqa: BLE001
                        mostrar(f"[{fo_id}] ERROR procesando orden: {exc}")
    except Exception as exc:  # noqa: BLE001
        mostrar(f"[{fo_id}] ERROR Kafka 'ordenes' ({broker}): {exc}")
        kafka_caido.set()
        kafka_listo.set()   # no dejar colgado al hilo principal


# ---------------------- Acciones del menú --------------------------------------------
def ver_estaciones():
    with estado_lock:
        estaciones = ultimo_estado_ws
    if estaciones is None:
        print("Todavía no ha llegado ningún ESTADO_WS de Central (lo publica cada ~2 s).")
        return
    if not estaciones:
        print("Central no tiene ninguna WS registrada.")
        return
    print(f"{'ID':<8}{'UBICACION':<26}ESTADO")
    for ws in sorted(estaciones, key=lambda w: str(w.get("ws_id"))):
        print(f"{str(ws.get('ws_id')):<8}{str(ws.get('ubicacion', ''))[:24]:<26}{ws.get('estado', '?')}")


def pedir_riego(prod, ws_id: str, duracion: int) -> threading.Event:
    """Publica la PETICION y devuelve el evento que se activa cuando termine."""
    evento = threading.Event()
    with estado_lock:
        pendientes[ws_id] = evento
    prod.send(TOPIC_PETICIONES, {"tipo": "PETICION", "fo_id": fo_id,
                                 "ws_id": ws_id, "duracion": duracion})
    prod.flush()
    print(f"[{fo_id}] PETICION enviada: {ws_id} durante {duracion} s")
    return evento


def parsear_linea(linea: str) -> tuple[str, int] | None:
    """'WS1;10' -> ('WS1', 10). Vacías, comentarios (#) o mal formadas -> None."""
    linea = linea.strip()
    if not linea or linea.startswith("#"):
        return None
    partes = linea.split(";")
    if len(partes) != 2 or not partes[0].strip():
        return None
    try:
        duracion = int(partes[1])
    except ValueError:
        return None
    if duracion <= 0:
        return None
    return partes[0].strip(), duracion


def leer_fichero(ruta: str) -> list[tuple[str, int]]:
    peticiones = []
    with open(ruta, encoding="utf-8") as fichero:
        for numero, linea in enumerate(fichero, start=1):
            peticion = parsear_linea(linea)
            if peticion is not None:
                peticiones.append(peticion)
            elif linea.strip() and not linea.strip().startswith("#"):
                print(f"Línea {numero} ignorada (formato 'ws_id;duracion'): {linea.strip()}")
    return peticiones


def modo_fichero(prod):
    por_defecto = os.path.join("fo", "peticiones", f"{fo_id.lower()}.txt")
    ruta = input(f"fichero [{por_defecto}]: ").strip() or por_defecto
    try:
        peticiones = leer_fichero(ruta)
    except OSError as exc:
        print(f"No se puede leer {ruta}: {exc}")
        return
    if not peticiones:
        print(f"{ruta} no tiene peticiones válidas.")
        return

    for i, (ws_id, duracion) in enumerate(peticiones, start=1):
        print(f"[{fo_id}] --- petición {i}/{len(peticiones)} del fichero ---")
        evento = pedir_riego(prod, ws_id, duracion)
        if not evento.wait(duracion + MARGEN_ESPERA):
            terminar_peticion(ws_id)
            print(f"[{fo_id}] {ws_id}: sin respuesta de Central, se pasa a la siguiente")
        if i < len(peticiones):
            time.sleep(PAUSA_ENTRE_PETICIONES)
    print(f"[{fo_id}] fichero {ruta} terminado")


def menu(prod):
    ayuda = "1 ver WS  2 pedir riego  3 leer fichero  0 salir"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if opcion == "1":
            ver_estaciones()
        elif opcion == "2":
            ws_id = input("ws_id: ").strip()
            try:
                duracion = int(input("duracion (segundos): ").strip())
                if not ws_id or duracion <= 0:
                    raise ValueError
            except ValueError:
                print("Datos inválidos: ws_id no vacío y duración entera > 0")
                continue
            pedir_riego(prod, ws_id, duracion)
        elif opcion == "3":
            modo_fichero(prod)
        elif opcion == "0":
            print(f"[{fo_id}] saliendo")
            break
        else:
            print(ayuda)


def main():
    global fo_id
    if len(sys.argv) != 3:
        print("USO: python -m fo.WM_FO <ip_broker:puerto> <id_operario>")
        sys.exit(1)
    broker, fo_id = sys.argv[1], sys.argv[2].strip()
    if ":" not in broker or not fo_id:
        print("Argumentos incorrectos. Ejemplo: python -m fo.WM_FO 127.0.0.1:9092 FO1")
        sys.exit(1)

    threading.Thread(target=consumir_ordenes, args=(broker,), daemon=True).start()
    print(f"[{fo_id}] conectando a Kafka {broker}...")
    if not kafka_listo.wait(20):
        print(f"[{fo_id}] AVISO: Kafka aún no responde; las respuestas pueden perderse")
    if kafka_caido.is_set():
        sys.exit(1)   # en Docker, restart: on-failure lo vuelve a intentar

    prod = productor(broker)
    try:
        menu(prod)
    finally:
        prod.close()


if __name__ == "__main__":
    main()
