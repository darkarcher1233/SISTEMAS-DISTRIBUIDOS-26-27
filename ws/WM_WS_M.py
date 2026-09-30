"""
WM_WS_M.py: programa principal del Monitor de una Watering Station.

Uso: python -m ws.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicacion]
"""

import sys
import threading
import time

from ws import monitor_central, monitor_engine


def main():
    if len(sys.argv) not in (4, 5):
        print("USO: python -m ws.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicacion]")
        sys.exit(1)

    try:
        puerto_engine = int(sys.argv[1])
        ip_central, puerto_central_texto = sys.argv[2].rsplit(":", 1)
        puerto_central = int(puerto_central_texto)
        if not 0 < puerto_engine < 65536 or not ip_central or not 0 < puerto_central < 65536:
            raise ValueError
    except ValueError:
        print("Argumentos incorrectos. Ejemplo: python -m ws.WM_WS_M 6000 127.0.0.1:5050 WS1 Parque")
        sys.exit(1)

    ws_id = sys.argv[3]
    ubicacion = sys.argv[4] if len(sys.argv) == 5 else ws_id
    if not ws_id or not ubicacion:
        print("El id de la WS y la ubicación no pueden estar vacíos.")
        sys.exit(1)

    if not monitor_central.registrar(ip_central, puerto_central, ws_id, ubicacion):
        print("No se pudo registrar la WS en Central.")
        sys.exit(1)

    print(f"Monitor {ws_id} registrado en Central; esperando al Engine en el puerto {puerto_engine}.")
    threading.Thread(
        target=monitor_engine.iniciar_servidor_engine,
        args=(puerto_engine, ws_id),
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