"""
WM_WS_M.py: MONITOR de la Watering Station, programa principal (nombre obligatorio: WM_WS_M)
Responsable: Abde (este fichero y monitor_central.py) · Vlad (monitor_engine.py)
Semana 1 · Tarjeta Trello: "WM_WS_M, parte Central"

USO
    python -m WM_WS.WM_WS_M <puerto_para_engine> <ip_central:puerto> <id_ws> [ubicacion]
    Pantalla web: la URL sale por consola (otro puerto: variable de entorno WEB_PORT)

QUÉ HACE (solo coordinar: la lógica está en los otros dos ficheros)
  1. Leer y validar los argumentos
  2. monitor_engine.iniciar_servidor_engine(...)  -> en un hilo: espera al Engine y
                                                     hace el health check (Vlad)
  3. monitor_central.mantener_conexion(...)       -> en un hilo: se registra en Central y
                                                     se vuelve a registrar si se cae (Abde)
  4. Pantalla web (ui/monitor.html) y mantenerse vivo

POR QUÉ ESTÁ DIVIDIDO: cada uno trabaja en su fichero y no hay conflictos en Git.
"""

import os
import sys
import threading
import time

from common.web import Registro, iniciar_web
from WM_WS import monitor_central, monitor_engine

eventos = Registro()


def avisar(texto):
    print(f"[{time.strftime('%H:%M:%S')}] {texto}", flush=True)
    if "KO" in texto or "FUGA" in texto:
        nivel = "ALERTA"
    elif "perdida" in texto or "No se pudo" in texto or "rechaz" in texto or "desconectado" in texto:
        nivel = "AVISO"
    elif "registrada" in texto or "OK" in texto or "RESUELTA" in texto or "conectado desde" in texto:
        nivel = "OK"
    else:
        nivel = "INFO"
    eventos.add(texto, nivel)


def main():
    if len(sys.argv) not in (4, 5):
        print("USO: python -m WM_WS.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicacion]")
        sys.exit(1)

    try:
        puerto_engine = int(sys.argv[1])
        ip_central, puerto_central_texto = sys.argv[2].rsplit(":", 1)
        puerto_central = int(puerto_central_texto)
        if not 0 < puerto_engine < 65536 or not ip_central or not 0 < puerto_central < 65536:
            raise ValueError
    except ValueError:
        print("Argumentos incorrectos. Ejemplo: python -m WM_WS.WM_WS_M 6000 127.0.0.1:5050 WS1 Parque")
        sys.exit(1)

    ws_id = sys.argv[3]
    ubicacion = sys.argv[4] if len(sys.argv) == 5 else ws_id
    if not ws_id or not ubicacion or "#" in ws_id + ubicacion:
        print("El id de la WS y la ubicación no pueden estar vacíos ni contener '#'.")
        sys.exit(1)

    monitor_central.avisar = avisar
    # Tras (re)registrarse: si el Engine sigue averiado, Central tiene que volver a saberlo
    monitor_central.al_registrar = lambda: None if monitor_engine.estado["salud_ok"] else monitor_central.notificar_fuga()

    def estado_web():
        return {
            "ws_id": ws_id,
            "ubicacion": ubicacion,
            "puerto_engine": puerto_engine,
            "central": f"{ip_central}:{puerto_central}",
            "con_central": monitor_central.estado["conectada"],
            "autenticado": monitor_central.estado["autenticado"],
            "error_central": monitor_central.estado["ultimo_error"],
            "engine": monitor_engine.estado["engine"],
            "salud_ok": monitor_engine.estado["salud_ok"],
            "motivo": monitor_engine.estado["motivo"],
            "eventos": eventos.lista(),
        }

    iniciar_web("monitor.html", estado_web, puerto_defecto=8100, nombre=f"Pantalla del monitor {ws_id}")

    def servidor_engine():
        try:
            monitor_engine.iniciar_servidor_engine(puerto_engine, ws_id, ubicacion)
        except OSError as exc:   # p. ej. el puerto ya lo usa otro Monitor
            avisar(f"No se puede escuchar al Engine en el puerto {puerto_engine}: {exc}")
            os._exit(1)

    threading.Thread(target=servidor_engine, daemon=True).start()
    threading.Thread(
        target=monitor_central.mantener_conexion,
        args=(ip_central, puerto_central, ws_id, ubicacion),
        daemon=True,
    ).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Cerrando conexión con Central.")
    finally:
        monitor_central.cerrar_conexion()


if __name__ == "__main__":
    main()
