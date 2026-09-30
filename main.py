"""Prueba local del hito 1: Central, Monitor y Engine comunicándose por sockets."""

import argparse
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time


ROOT = Path(__file__).resolve().parent
BROKER_DE_PRUEBA = "127.0.0.1:9092"


def puerto_libre():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def esperar_servidor(proceso, puerto, nombre, timeout=10):
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if proceso.poll() is not None:
            raise RuntimeError(f"{nombre} terminó durante el arranque (código {proceso.returncode}).")
        try:
            with socket.create_connection(("127.0.0.1", puerto), timeout=0.2):
                return
        except OSError:
            threading.Event().wait(0.05)
    raise RuntimeError(f"{nombre} no abrió el puerto {puerto} a tiempo.")


def iniciar_engine(puerto, ws_id, monitor):
    limite = time.monotonic() + 10
    comando = [
        sys.executable, "-u", "-m", "ws.WM_WS_E",
        BROKER_DE_PRUEBA, f"127.0.0.1:{puerto}",
    ]
    while time.monotonic() < limite:
        if monitor.poll() is not None:
            raise RuntimeError("El proceso WM_WS_M terminó antes de aceptar el Engine.")
        engine = subprocess.Popen(comando, cwd=ROOT, stderr=subprocess.DEVNULL)
        margen = time.monotonic() + 0.4
        while time.monotonic() < margen and engine.poll() is None:
            threading.Event().wait(0.05)
        if engine.poll() is None:
            return engine
        threading.Event().wait(0.1)
    raise RuntimeError(f"WM_WS_E no pudo conectarse al Monitor de {ws_id}.")


def detener(proceso):
    if proceso is None or proceso.poll() is not None:
        return
    proceso.terminate()
    try:
        proceso.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proceso.kill()
        proceso.wait(timeout=3)


def main():
    parser = argparse.ArgumentParser(description="Prueba local de sockets de WaterManagement (hito 1).")
    parser.add_argument(
        "--segundos",
        type=float,
        default=8,
        help="duración de la prueba antes de cerrar los procesos (por defecto: 8)",
    )
    args = parser.parse_args()
    if args.segundos <= 0:
        parser.error("--segundos debe ser mayor que cero")

    central_port = puerto_libre()
    engine_port = puerto_libre()
    while engine_port == central_port:
        engine_port = puerto_libre()

    entorno = os.environ.copy()
    entorno["PYTHONUNBUFFERED"] = "1"
    procesos = {"central": None, "monitor": None, "engine": None}

    try:
        print("Iniciando WM_Central...")
        procesos["central"] = subprocess.Popen(
            [sys.executable, "-u", "-m", "central.WM_Central", str(central_port), BROKER_DE_PRUEBA],
            cwd=ROOT,
            env=entorno,
        )
        esperar_servidor(procesos["central"], central_port, "WM_Central")

        print("Iniciando WM_WS_M...")
        procesos["monitor"] = subprocess.Popen(
            [
                sys.executable, "-u", "-m", "ws.WM_WS_M", str(engine_port),
                f"127.0.0.1:{central_port}", "WS-DEMO", "Parque de Prueba",
            ],
            cwd=ROOT,
            env=entorno,
        )
        procesos["engine"] = iniciar_engine(engine_port, "WS-DEMO", procesos["monitor"])

        print(f"Prueba activa durante {args.segundos:g} s. Usa Ctrl+C para terminar antes.")
        limite = time.monotonic() + args.segundos
        while time.monotonic() < limite:
            terminados = [nombre for nombre, proceso in procesos.items() if proceso.poll() is not None]
            if terminados:
                raise RuntimeError(f"Proceso(s) terminaron inesperadamente: {', '.join(terminados)}.")
            threading.Event().wait(min(0.2, limite - time.monotonic()))
        print("Prueba completada: Central, Monitor y Engine siguen conectados.")
    except KeyboardInterrupt:
        print("Prueba interrumpida por el usuario.")
    finally:
        detener(procesos["engine"])
        detener(procesos["monitor"])
        threading.Event().wait(0.2)
        detener(procesos["central"])


if __name__ == "__main__":
    main()
