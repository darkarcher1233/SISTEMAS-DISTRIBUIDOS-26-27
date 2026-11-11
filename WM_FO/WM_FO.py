"""
WM_FO.py: aplicación del OPERARIO DE CAMPO (nombre obligatorio: WM_FO)
Responsable: Vlad · Semana 3 · Tarjetas: "WM_FO: modo manual" + "WM_FO: peticiones desde fichero"
Solo usa KAFKA (no tiene sockets).

USO
    python -m WM_FO.WM_FO <ip_broker:puerto> <id_operario>
    Pantalla web: la URL sale por consola (otro puerto: variable de entorno WEB_PORT)

QUÉ HACE
  - Menú:  1 ver WS disponibles · 2 pedir riego (ws_id + duración) · 3 leer fichero · 0 salir
    (lo mismo desde la pantalla web: ui/operario.html)
  - Pedir riego -> publica en 'peticiones'  {tipo: PETICION, fo_id, ws_id, duracion}
  - Hilo consumidor de 'ordenes', quedándose SOLO con los mensajes de su fo_id:
        AUTORIZADO · DENEGADO (+motivo) · TELEMETRIA de su WS · RESUMEN final
        (+ aviso claro si el riego acaba por FUGA o BLOQUEO)
  - Ver WS disponibles: guarda el último ESTADO_WS que publica Central y lo muestra
  - Modo fichero (WM_FO/peticiones/*.txt, una línea "ws_id;duracion"):
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
from common.web import Registro, esperar_sin_teclado, iniciar_web

PAUSA_ENTRE_PETICIONES = 4   # segundos entre una petición del fichero y la siguiente
MARGEN_ESPERA = 30           # segundos extra (sobre la duración) antes de dar por perdida una petición
SIN_RESPUESTA = 10           # segundos sin AUTORIZADO/DENEGADO -> se marca "sin respuesta"
REINTENTO_KAFKA = 3
SIN_CENTRAL = 6              # s sin ESTADO_WS (Central lo publica cada 2 s) -> Central caída
ALARMAS = ("FUGA", "BLOQUEO", "MONITOR", "AVERIA")
MOTIVOS = {
    "TIEMPO": "Riego completado: se agotó el tiempo pedido",
    "MANUAL": "Parado a mano en la estación",
    "FUGA": "Interrumpido por una FUGA en la estación",
    "BLOQUEO": "Interrumpido: CENTRAL bloqueó la estación",
    "MONITOR": "Interrumpido: la estación perdió su monitor (averiada)",
    "AVERIA": "Interrumpido: el engine de la estación se cayó (volumen real recuperado al volver)",
}
NO_CENTRAL = "Imposible conectar con WM_Central"

fo_id: str = ""
broker: str = ""

# ---------------------- Estado compartido con el hilo consumidor -------------------
estado_lock = threading.Lock()
ultimo_estado_ws: list[dict] | None = None    # última lista de WS recibida en ESTADO_WS
instante_estado_ws: float | None = None
pendientes: dict[str, threading.Event] = {}  # ws_id -> se activa al llegar DENEGADO o RESUMEN
solicitudes: list[dict] = []                 # para la pantalla, la más nueva primero
fichero_en_curso: dict | None = None
kafka_listo = threading.Event()              # el consumidor ya tiene particiones asignadas
eventos = Registro()
_contador = 0
_prod = None


def mostrar(texto: str, nivel: str = "INFO"):
    """Print desde el hilo consumidor: salto de línea delante para no pisar el prompt."""
    print(f"\n{texto}", flush=True)
    eventos.add(texto.split("] ", 1)[-1], nivel)


def alarma(texto: str):
    linea = "!" * 60
    print(f"\n{linea}\n!!!  {texto}\n{linea}", flush=True)
    eventos.add(texto, "ALERTA")


def normalizar_estaciones(estaciones) -> list[dict]:
    """ESTADO_WS puede traer una lista [{ws_id, ...}] o el dict de Central {ws_id: {...}}."""
    if isinstance(estaciones, dict):
        return [{"ws_id": ws_id, **datos} for ws_id, datos in estaciones.items()]
    if isinstance(estaciones, list):
        return [ws for ws in estaciones if isinstance(ws, dict)]
    return []


def central_ok() -> bool:
    """Central publica ESTADO_WS cada 2 s: si hace más de SIN_CENTRAL s que no llega, está caída."""
    with estado_lock:
        visto = instante_estado_ws
    return visto is not None and time.time() - visto < SIN_CENTRAL


def vigilar_central():
    """Hilo: avisa (una vez) cuando Central deja de responder y cuando vuelve."""
    time.sleep(SIN_CENTRAL + 4)   # margen al arrancar para recibir el primer ESTADO_WS
    estaba_ok = None
    while True:
        ok = central_ok()
        if ok != estaba_ok:
            if ok and estaba_ok is not None:
                mostrar(f"[{fo_id}] WM_Central vuelve a responder", "OK")
            elif not ok:
                alarma(f"{NO_CENTRAL}: no se pueden pedir riegos hasta que vuelva")
            estaba_ok = ok
        time.sleep(1)


def terminar_peticion(ws_id):
    """La petición sobre ws_id ha acabado (denegada o con RESUMEN): despierta al modo fichero."""
    with estado_lock:
        evento = pendientes.pop(ws_id, None)
    if evento is not None:
        evento.set()


EN_CURSO = ("ENVIADA", "AUTORIZADA", "REGANDO")


def _solicitud_abierta(ws_id, estados=EN_CURSO + ("SIN_RESPUESTA",)):
    """La solicitud más reciente sobre ws_id que aún no ha terminado. Con estado_lock cogido.
    Por defecto también vale una "sin respuesta": si la respuesta llega tarde, se actualiza."""
    for s in solicitudes:
        if s["ws_id"] == ws_id:
            return s if s["estado"] in estados else None
    return None


# ---------------------- Hilo consumidor de 'ordenes' ---------------------------------
def procesar_orden(datos):
    """Procesa un mensaje de 'ordenes'. Solo se queda con ESTADO_WS y con lo de su fo_id."""
    global ultimo_estado_ws, instante_estado_ws
    if not isinstance(datos, dict):
        return

    tipo = datos.get("tipo")
    if tipo == "ESTADO_WS":
        with estado_lock:
            ultimo_estado_ws = normalizar_estaciones(datos.get("estaciones"))
            instante_estado_ws = time.time()
        return

    if datos.get("fo_id") != fo_id:
        return   # es de otro operario (o una orden para una WS)

    ws_id = datos.get("ws_id", "?")
    ahora = time.time()
    with estado_lock:
        s = _solicitud_abierta(ws_id)

    if tipo == "AUTORIZADO":
        mostrar(f"[{fo_id}] AUTORIZADO: riego en {ws_id} durante {datos.get('duracion', '?')} s", "OK")
        if s:
            with estado_lock:
                s.update(estado="AUTORIZADA", autorizada=ahora)

    elif tipo == "DENEGADO":
        mostrar(f"[{fo_id}] DENEGADO en {ws_id}: {datos.get('motivo', 'sin motivo')}", "AVISO")
        if s:
            with estado_lock:
                s.update(estado="DENEGADA", motivo=datos.get("motivo", "sin motivo"))
        terminar_peticion(ws_id)

    elif tipo == "TELEMETRIA":
        print(f"\n[{fo_id}] {ws_id} caudal={datos.get('caudal', 0)} L/min "
              f"volumen={datos.get('volumen', 0)} L", flush=True)
        if s:
            with estado_lock:
                inicio = s.get("autorizada") or s["creada"]
                s["estado"] = "REGANDO"
                s["telemetria"] = {"caudal": datos.get("caudal", 0), "volumen": datos.get("volumen", 0),
                                   "segundos": ahora - inicio, "duracion": s["duracion"]}

    elif tipo == "AVISO":   # Central: la WS de mi riego se ha averiado o desconectado
        alarma(f"{ws_id}: {datos.get('motivo', 'incidencia en la estación')}")
        if s:
            with estado_lock:
                s["alerta"] = datos.get("motivo", "incidencia en la estación")

    elif tipo == "RESUMEN":
        motivo = datos.get("motivo", "?")
        if not s:
            # Resultado de un riego que pedí antes de cerrarse esta aplicación: Kafka me lo
            # guarda (grupo fo_<id>) y me lo entrega al volver
            mostrar(f"[{fo_id}] Resultado de un riego anterior en {ws_id}:", "AVISO")
            with estado_lock:
                s = {"req_id": "anterior", "ws_id": ws_id, "duracion": 0, "estado": "REGANDO", "creada": ahora}
                solicitudes.insert(0, s)
        if motivo in ALARMAS:
            alarma(f"RIEGO EN {ws_id} INTERRUMPIDO POR {motivo}")
        mostrar(f"[{fo_id}] RESUMEN {ws_id}: volumen={datos.get('volumen', 0)} L "
                f"duracion={datos.get('duracion', 0)} s motivo={motivo}",
                "ALERTA" if motivo in ALARMAS else "OK")
        if s:
            segundos = float(datos.get("duracion") or 0)
            volumen = float(datos.get("volumen") or 0)
            with estado_lock:
                s.update(estado="FINALIZADA", resumen={
                    "volumen": volumen, "segundos": segundos, "duracion_pedida": s["duracion"] or "?",
                    "caudal_medio": volumen / (segundos / 60) if segundos else 0,
                    "motivo": MOTIVOS.get(motivo, f"Motivo: {motivo}"),
                })
                if motivo in ALARMAS:
                    s["alerta"] = MOTIVOS[motivo]
        terminar_peticion(ws_id)


def consumir_ordenes():
    """Lee 'ordenes' y se queda con ESTADO_WS y con lo de su fo_id. Si Kafka cae, reintenta.

    Grupo de Kafka propio por operario (fo_<id>) y commit DESPUÉS de procesar: Kafka recuerda
    por dónde iba este operario. Si el FO se cierra durante un riego, al volver recibe lo que
    se perdió (incluido el RESUMEN). Cada FO tiene su grupo, así que todos reciben todo.
    La primera vez (grupo nuevo) empieza en "latest": no repite riegos de otros días."""
    while True:
        cons = None
        try:
            cons = consumidor(broker, TOPIC_ORDENES, group_id=f"fo_{fo_id}",
                              auto_offset_reset="latest", enable_auto_commit=False,
                              session_timeout_ms=6000, heartbeat_interval_ms=2000)
            while True:
                lotes = cons.poll(timeout_ms=500)
                if not kafka_listo.is_set() and cons.assignment():
                    kafka_listo.set()
                    eventos.add(f"Conectado a Kafka ({broker})", "OK")
                for mensajes in lotes.values():
                    for mensaje in mensajes:
                        try:
                            procesar_orden(mensaje.value)
                        except Exception as exc:  # noqa: BLE001
                            mostrar(f"[{fo_id}] ERROR procesando orden: {exc}", "ALERTA")
                if lotes:
                    cons.commit()   # procesados: si el FO cae ahora, no se repiten ni se pierden
        except Exception as exc:  # noqa: BLE001
            if kafka_listo.is_set():
                mostrar(f"[{fo_id}] ERROR Kafka ({broker}): {type(exc).__name__}; reintentando", "ALERTA")
            else:
                print(f"[{fo_id}] Kafka no responde ({type(exc).__name__}); reintentando", flush=True)
            kafka_listo.clear()
        finally:
            if cons is not None:
                try:
                    cons.close()
                except Exception:  # noqa: BLE001
                    pass
        time.sleep(REINTENTO_KAFKA)


# ---------------------- Acciones ------------------------------------------------------
def prod_fo():
    global _prod
    if _prod is None:
        _prod = productor(broker, max_block_ms=5000)
    return _prod


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
    global _contador
    evento = threading.Event()
    with estado_lock:
        pendientes[ws_id] = evento
        _contador += 1
        solicitud = {"req_id": f"#{_contador}", "ws_id": ws_id, "duracion": duracion,
                     "estado": "ENVIADA", "creada": time.time()}
        solicitudes.insert(0, solicitud)
        del solicitudes[30:]
    evento.solicitud = solicitud   # para que el modo fichero sepa cómo acabó
    if not central_ok():
        with estado_lock:
            solicitud.update(estado="NO_ENVIADA", motivo=NO_CENTRAL)
        mostrar(f"[{fo_id}] {NO_CENTRAL}: la petición de {ws_id} no se envía", "ALERTA")
        terminar_peticion(ws_id)
        return evento
    try:
        prod.send(TOPIC_PETICIONES, {"tipo": "PETICION", "fo_id": fo_id,
                                     "ws_id": ws_id, "duracion": duracion}).get(timeout=5)
    except Exception as exc:  # noqa: BLE001
        with estado_lock:
            solicitud.update(estado="NO_ENVIADA", motivo=f"Kafka no responde ({type(exc).__name__})")
        mostrar(f"[{fo_id}] No se pudo enviar la PETICION: Kafka no responde", "ALERTA")
        terminar_peticion(ws_id)
        return evento
    mostrar(f"[{fo_id}] PETICION enviada: {ws_id} durante {duracion} s")
    return evento


def parsear_linea(linea: str) -> tuple[str, int] | None:
    """'WS1;10' (o 'WS1 10') -> ('WS1', 10). Vacías, comentarios (#) o mal formadas -> None."""
    linea = linea.strip()
    if not linea or linea.startswith("#"):
        return None
    partes = linea.split(";") if ";" in linea else linea.split()
    if len(partes) != 2 or not partes[0].strip():
        return None
    try:
        duracion = int(partes[1])
    except ValueError:
        return None
    if duracion <= 0:
        return None
    return partes[0].strip(), duracion


def leer_lineas(lineas) -> list[tuple[str, int]]:
    peticiones = []
    for numero, linea in enumerate(lineas, start=1):
        peticion = parsear_linea(linea)
        if peticion is not None:
            peticiones.append(peticion)
        elif linea.strip() and not linea.strip().startswith("#"):
            print(f"Línea {numero} ignorada (formato 'ws_id;duracion'): {linea.strip()}")
    return peticiones


def leer_fichero(ruta: str) -> list[tuple[str, int]]:
    with open(ruta, encoding="utf-8") as fichero:
        return leer_lineas(fichero)


def ejecutar_peticiones(prod, nombre: str, peticiones: list[tuple[str, int]]):
    """Envía las peticiones de una en una: espera a que termine cada una + 4 s."""
    global fichero_en_curso
    progreso = {"nombre": nombre, "activo": True, "actual": 0, "total": len(peticiones), "resultados": []}
    fichero_en_curso = progreso
    eventos.add(f"Fichero {nombre}: {len(peticiones)} peticiones")
    for i, (ws_id, duracion) in enumerate(peticiones, start=1):
        progreso["actual"] = i
        print(f"[{fo_id}] --- petición {i}/{len(peticiones)} del fichero ---")
        evento = pedir_riego(prod, ws_id, duracion)
        solicitud = evento.solicitud
        if not evento.wait(duracion + MARGEN_ESPERA):
            terminar_peticion(ws_id)
            with estado_lock:
                solicitud["estado"] = "SIN_RESPUESTA"
            print(f"[{fo_id}] {ws_id}: sin respuesta de Central, se pasa a la siguiente")
        progreso["resultados"].append({"ws_id": ws_id, "duracion": duracion, "resultado": solicitud["estado"]})
        if i < len(peticiones):
            time.sleep(PAUSA_ENTRE_PETICIONES)
    progreso["activo"] = False
    mostrar(f"[{fo_id}] fichero {nombre} terminado", "OK")


def modo_fichero(prod):
    por_defecto = os.path.join("WM_FO", "peticiones", f"{fo_id.lower()}.txt")
    ruta = input(f"fichero [{por_defecto}]: ").strip() or por_defecto
    try:
        peticiones = leer_fichero(ruta)
    except OSError as exc:
        print(f"No se puede leer {ruta}: {exc}")
        return
    if not peticiones:
        print(f"{ruta} no tiene peticiones válidas.")
        return
    ejecutar_peticiones(prod, ruta, peticiones)


# ---------------------- Pantalla web --------------------------------------------------
def estado_web():
    ahora = time.time()
    with estado_lock:
        for s in solicitudes:
            if s["estado"] == "ENVIADA" and ahora - s["creada"] > SIN_RESPUESTA:
                s["estado"] = "SIN_RESPUESTA"
            if s["estado"] == "REGANDO" and s.get("telemetria"):
                s["telemetria"]["segundos"] = ahora - (s.get("autorizada") or s["creada"])
        estaciones = [
            {"id": ws.get("ws_id"), "ubicacion": ws.get("ubicacion") or "", "estado": ws.get("estado", "DESCONECTADA"),
             "caudal": ws.get("caudal") or 0, "operario": ws.get("operario"), "motivo_fuga": ws.get("motivo_fuga")}
            for ws in sorted(ultimo_estado_ws or [], key=lambda w: str(w.get("ws_id")))
        ]
        lista = [dict(s) for s in solicitudes]
        fichero = None
        if fichero_en_curso:
            fichero = {**fichero_en_curso, "resultados": list(fichero_en_curso["resultados"])}
    return {"ahora": ahora, "fo_id": fo_id, "broker": broker, "kafka": kafka_listo.is_set(),
            "central": central_ok(),
            "instantanea": instante_estado_ws, "estaciones": estaciones, "solicitudes": lista,
            "fichero": fichero, "eventos": eventos.lista()}


def web_solicitar(cuerpo):
    ws_id = str(cuerpo.get("ws") or "").strip()
    try:
        duracion = int(cuerpo.get("duracion") or 0)
    except (TypeError, ValueError):
        duracion = 0
    if not ws_id or not 0 < duracion <= 3600:
        raise ValueError("Estación y duración (1-3600 s) obligatorias")
    if not central_ok():
        raise ValueError(NO_CENTRAL)
    with estado_lock:
        if _solicitud_abierta(ws_id, EN_CURSO):
            raise ValueError(f"Ya tienes una solicitud en curso en {ws_id}")
    threading.Thread(target=pedir_riego, args=(prod_fo(), ws_id, duracion), daemon=True).start()
    return {"ok": True, "mensaje": f"Solicitud enviada: {ws_id} durante {duracion} s"}


def web_fichero(cuerpo):
    if fichero_en_curso and fichero_en_curso["activo"]:
        raise ValueError("Ya hay un fichero en curso")
    nombre = str(cuerpo.get("nombre") or "fichero.txt")
    peticiones = leer_lineas(str(cuerpo.get("contenido") or "").splitlines())
    if not peticiones:
        raise ValueError("El fichero no tiene peticiones válidas (una por línea: WS1;30)")
    threading.Thread(target=ejecutar_peticiones, args=(prod_fo(), nombre, peticiones), daemon=True).start()
    return {"ok": True, "mensaje": f"{len(peticiones)} peticiones en marcha"}


# ---------------------- Menú ----------------------------------------------------------
def menu(prod):
    ayuda = "1 ver WS  2 pedir riego  3 leer fichero  0 salir"
    print(ayuda)
    while True:
        try:
            opcion = input("> ").strip()
        except EOFError:
            esperar_sin_teclado(f"Operario {fo_id}")
            break
        except KeyboardInterrupt:
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
    global fo_id, broker
    if len(sys.argv) != 3:
        print("USO: python -m WM_FO.WM_FO <ip_broker:puerto> <id_operario>")
        sys.exit(1)
    broker, fo_id = sys.argv[1], sys.argv[2].strip()
    if ":" not in broker or not fo_id:
        print("Argumentos incorrectos. Ejemplo: python -m WM_FO.WM_FO 127.0.0.1:9092 FO1")
        sys.exit(1)

    iniciar_web("operario.html", estado_web, {"solicitar": web_solicitar, "fichero": web_fichero},
                puerto_defecto=8300, nombre=f"Pantalla del operario {fo_id}")
    threading.Thread(target=consumir_ordenes, daemon=True).start()
    threading.Thread(target=vigilar_central, daemon=True).start()
    print(f"[{fo_id}] conectando a Kafka {broker}...")
    if not kafka_listo.wait(20):
        print(f"[{fo_id}] AVISO: Kafka aún no responde; se sigue reintentando")

    try:
        menu(prod_fo())
    except KeyboardInterrupt:
        pass
    finally:
        if _prod is not None:
            try:
                _prod.close()   # envía lo que quede pendiente
            except Exception:  # noqa: BLE001 - al salir, sin trazas
                pass
        print(f"[{fo_id}] aplicación del operario cerrada")


if __name__ == "__main__":
    main()
