"""
lanzador.py: arranca y vigila desde el navegador todos los módulos de ESTE PC

USO (desde la raíz del repo):
    python lanzador.py                (o python -m lanzador)
    abrir http://localhost:8000       (otro puerto: variable de entorno LANZADOR_PORT)

Qué hace
  - Cada módulo es un proceso hijo con su comando de siempre (python -m WM_Central.WM_Central ...),
    sin teclado: se maneja desde su pantalla web, que el lanzador enseña dentro de la suya.
  - CENTRAL local (sockets 5050, panel 8080) o remota (solo se usan sus direcciones).
  - Estaciones: Monitor + Engine de cada una.  Operarios: un WM_FO por operario.
  - Guarda la lista de estaciones / operarios y los ajustes en lanzador.json (no se sube).
  - Simular una fuga en una estación (lo mismo que el botón de su Engine o la tecla f).
  - Comprueba si Kafka responde y, si no, puede arrancarlo con docker compose (docker-compose.yml).
  - Al cerrarlo (Ctrl+C) para todos los procesos.

Puertos en este PC (estación n = 0, 1, 2...; operario n = 0, 1, 2...)
    CENTRAL           sockets 5050 · panel 8080
    Monitor  WS n     escucha al Engine en 6000+n · web 8100+n
    Engine   WS n     web 8200+n
    Operario n        web 8300+n
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque

from common.web import iniciar_web

ROOT = os.path.dirname(os.path.abspath(__file__))
RUTA_AJUSTES = os.path.join(ROOT, "lanzador.json")
PUERTO_SOCKETS, PUERTO_PANEL = 5050, 8080
BASE_ENGINE, BASE_WEB_MONITOR, BASE_WEB_ENGINE, BASE_WEB_FO = 6000, 8100, 8200, 8300

lock = threading.RLock()


def ajustes_iniciales():
    return {
        "broker": os.environ.get("KAFKA_BROKER") or "localhost:9092",
        "modo": "local",
        "sockets": "",
        "panel": "",
        "estaciones": [{"id": "WS1", "ubicacion": "Parque Norte", "n": 0},
                       {"id": "WS2", "ubicacion": "Parque Sur", "n": 1}],
        "operarios": [{"id": "FO1", "n": 0}, {"id": "FO2", "n": 1}],
    }


def cargar_ajustes():
    datos = ajustes_iniciales()
    try:
        with open(RUTA_AJUSTES, encoding="utf-8") as f:
            datos.update(json.load(f))
    except (OSError, ValueError):
        pass
    return datos


def guardar_ajustes():
    with open(RUTA_AJUSTES, "w", encoding="utf-8") as f:
        json.dump(ajustes, f, ensure_ascii=False, indent=2)


ajustes = cargar_ajustes()


# ---------------------- Procesos hijos -----------------------------------------
class Proceso:
    def __init__(self, clave, nombre, web, comando_fn, env_extra=None):
        self.clave, self.nombre, self.web = clave, nombre, web
        self.comando_fn = comando_fn          # se calcula al arrancar: así valen los ajustes nuevos
        self.env_extra = env_extra or {}
        self.popen = None
        self.inicio = 0.0
        self.parado_a_mano = False
        self.lineas = deque(maxlen=500)

    def vivo(self):
        return self.popen is not None and self.popen.poll() is None

    def estado(self):
        if self.vivo():
            return "en_marcha"
        if self.popen is not None and self.popen.returncode not in (0, None) and not self.parado_a_mano:
            return "error"
        return "parado"

    def info(self):
        return {"clave": self.clave, "nombre": self.nombre, "estado": self.estado(),
                "inicio": self.inicio, "web": self.web}

    def arrancar(self):
        if self.vivo():
            return
        comando = [sys.executable, "-u", *self.comando_fn()]
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8", **self.env_extra}
        self.lineas.clear()
        self.lineas.append("$ python " + " ".join(f'"{c}"' if " " in c else c for c in comando[2:]))
        self.parado_a_mano = False
        self.inicio = time.time()
        self.popen = subprocess.Popen(
            comando, cwd=ROOT, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1,
        )
        threading.Thread(target=self._leer, args=(self.popen,), daemon=True).start()

    def _leer(self, popen):
        for linea in popen.stdout:
            self.lineas.append(linea.rstrip("\n"))
        codigo = popen.wait()
        self.lineas.append(f"--- proceso terminado (código {codigo}) ---")

    def parar(self):
        if not self.vivo():
            return
        self.parado_a_mano = True
        self.popen.terminate()
        try:
            self.popen.wait(timeout=4)
        except subprocess.TimeoutExpired:
            self.popen.kill()


procesos: dict[str, Proceso] = {}


def direccion_central():
    if ajustes["modo"] == "remota":
        return ajustes.get("sockets") or "127.0.0.1:5050"
    return f"127.0.0.1:{PUERTO_SOCKETS}"


def crear_procesos():
    """(Re)crea los objetos Proceso que falten según los ajustes. Con lock."""
    if "central" not in procesos:
        procesos["central"] = Proceso(
            "central", "CENTRAL", PUERTO_PANEL,
            lambda: ["-m", "WM_Central.WM_Central", str(PUERTO_SOCKETS), ajustes["broker"]],
            {"PANEL_PORT": str(PUERTO_PANEL)})
    for e in ajustes["estaciones"]:
        ws_id, n = e["id"], e["n"]
        if f"ws:{ws_id}:monitor" not in procesos:
            procesos[f"ws:{ws_id}:monitor"] = Proceso(
                f"ws:{ws_id}:monitor", f"Monitor {ws_id}", BASE_WEB_MONITOR + n,
                lambda e=e: ["-m", "WM_WS.WM_WS_M", str(BASE_ENGINE + e["n"]), direccion_central(), e["id"], e["ubicacion"]],
                {"WEB_PORT": str(BASE_WEB_MONITOR + n)})
            procesos[f"ws:{ws_id}:engine"] = Proceso(
                f"ws:{ws_id}:engine", f"Engine {ws_id}", BASE_WEB_ENGINE + n,
                lambda e=e: ["-m", "WM_WS.WM_WS_E", ajustes["broker"], f"127.0.0.1:{BASE_ENGINE + e['n']}"],
                {"WEB_PORT": str(BASE_WEB_ENGINE + n)})
    for o in ajustes["operarios"]:
        if f"fo:{o['id']}" not in procesos:
            procesos[f"fo:{o['id']}"] = Proceso(
                f"fo:{o['id']}", f"Operario {o['id']}", BASE_WEB_FO + o["n"],
                lambda o=o: ["-m", "WM_FO.WM_FO", ajustes["broker"], o["id"]],
                {"WEB_PORT": str(BASE_WEB_FO + o["n"])})


def procesos_de(grupo):
    """'central', 'ws:WS1', 'fo:FO1' o la clave de un solo proceso ('ws:WS1:engine')."""
    if grupo in procesos:
        return [procesos[grupo]]
    if grupo.startswith("ws:"):
        claves = [f"{grupo}:monitor", f"{grupo}:engine"]
        if all(c in procesos for c in claves):
            return [procesos[c] for c in claves]
    raise ValueError(f"No existe {grupo}")


def todos_los_grupos():
    grupos = ["central"] if ajustes["modo"] == "local" else []
    grupos += [f"ws:{e['id']}" for e in ajustes["estaciones"]]
    grupos += [f"fo:{o['id']}" for o in ajustes["operarios"]]
    return grupos


def hacer(grupo, accion):
    if grupo.startswith("central") and ajustes["modo"] == "remota" and accion != "parar":
        raise ValueError("CENTRAL está en modo remoto: no se arranca en este PC")
    lista = procesos_de(grupo)
    if accion in ("parar", "reiniciar"):
        for p in reversed(lista):
            p.parar()
    if accion in ("arrancar", "reiniciar"):
        for p in lista:
            p.arrancar()
    if accion not in ("arrancar", "parar", "reiniciar"):
        raise ValueError(f"Acción desconocida: {accion}")


# ---------------------- Kafka -----------------------------------------------------
kafka = {"ok": False}
docker = {"ocupado": False, "salida": deque(maxlen=40)}


def vigilar_kafka():
    while True:
        try:
            host, puerto = ajustes["broker"].split(",")[0].strip().rsplit(":", 1)
            with socket.create_connection((host, int(puerto)), timeout=1.5):
                kafka["ok"] = True
        except (OSError, ValueError):
            kafka["ok"] = False
        time.sleep(2)


def arrancar_kafka_docker():
    docker["salida"].clear()
    docker["salida"].append("$ docker compose up -d")
    try:
        p = subprocess.Popen(["docker", "compose", "up", "-d"], cwd=ROOT, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
        for linea in p.stdout:
            if linea.strip():
                docker["salida"].append(linea.rstrip())
        codigo = p.wait()
        docker["salida"].append("Hecho. Kafka tarda unos segundos en aceptar conexiones." if codigo == 0
                                else f"docker terminó con código {codigo}. ¿Está Docker Desktop abierto?")
    except OSError as exc:
        docker["salida"].append(f"No se puede ejecutar docker: {exc}")
    finally:
        docker["ocupado"] = False


# ---------------------- Avería de los Engine --------------------------------------
averias: dict[str, bool] = {}   # ws_id -> el Engine tiene la avería simulada (lo pregunta un hilo)


def _engine(ws_id, ruta, cuerpo=None):
    """Llama a la API web del Engine de ws_id (POST si hay cuerpo)."""
    e = next((x for x in ajustes["estaciones"] if x["id"] == ws_id), None)
    if e is None:
        raise ValueError(f"No existe {ws_id}")
    datos = None if cuerpo is None else json.dumps(cuerpo).encode()
    peticion = urllib.request.Request(f"http://127.0.0.1:{BASE_WEB_ENGINE + e['n']}/api/{ruta}", datos,
                                      {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(peticion, timeout=2) as r:
            return json.load(r)
    except urllib.error.HTTPError as exc:
        return json.load(exc)


def vigilar_averias():
    while True:
        for e in list(ajustes["estaciones"]):
            p = procesos.get(f"ws:{e['id']}:engine")
            try:
                averias[e["id"]] = bool(p and p.vivo() and _engine(e["id"], "estado").get("averia"))
            except (OSError, ValueError):
                averias[e["id"]] = False
        time.sleep(1)


def api_fuga(cuerpo):
    """Simula (o resuelve) una fuga en el Engine de la estación: igual que su botón o la tecla f."""
    grupo = str(cuerpo.get("grupo") or "")
    ws_id = grupo.split(":")[1] if grupo.startswith("ws:") else ""
    p = procesos.get(f"ws:{ws_id}:engine")
    if not p or not p.vivo():
        raise ValueError("El Engine de esa estación no está en marcha")
    try:
        respuesta = _engine(ws_id, "fuga", {})
    except OSError as exc:
        raise ValueError(f"El Engine no responde todavía: {exc}") from exc
    if respuesta.get("ok"):
        averias[ws_id] = respuesta.get("mensaje") == "Avería simulada"
    return respuesta


# ---------------------- API web -------------------------------------------------------
def _id_valido(texto, que):
    texto = str(texto or "").strip()
    if not texto or any(c in texto for c in " #:/;"):
        raise ValueError(f"{que}: obligatorio y sin espacios ni # : / ;")
    return texto


def _hueco(lista):
    usados = {x["n"] for x in lista}
    return next(n for n in range(len(lista) + 1) if n not in usados)


def api_estado():
    with lock:
        crear_procesos()
        return {
            "ahora": time.time(),
            "broker": ajustes["broker"],
            "kafka": kafka["ok"],
            "docker": {"ocupado": docker["ocupado"], "salida": list(docker["salida"])},
            "ajustes": {k: ajustes.get(k) for k in ("broker", "modo", "sockets", "panel")},
            "central": {"modo": ajustes["modo"], "sockets": direccion_central(), "panel": ajustes.get("panel"),
                        "proceso": procesos["central"].info()},
            "estaciones": [{"id": e["id"], "ubicacion": e["ubicacion"],
                            "averia": averias.get(e["id"], False),
                            "monitor": procesos[f"ws:{e['id']}:monitor"].info(),
                            "engine": procesos[f"ws:{e['id']}:engine"].info()} for e in ajustes["estaciones"]],
            "operarios": [{"id": o["id"], "proceso": procesos[f"fo:{o['id']}"].info()} for o in ajustes["operarios"]],
        }


def api_registro(parametros):
    with lock:
        p = procesos.get(parametros.get("clave", ""))
        return {"lineas": list(p.lineas) if p else []}


def api_grupo(cuerpo):
    with lock:
        hacer(str(cuerpo.get("grupo") or ""), cuerpo.get("accion"))


def api_todo(cuerpo):
    accion = cuerpo.get("accion")
    with lock:
        if accion == "parar":
            for p in reversed(list(procesos.values())):
                p.parar()
            return {"ok": True, "mensaje": "Todos los procesos parados"}
        for grupo in todos_los_grupos():
            hacer(grupo, "arrancar")
    return {"ok": True, "mensaje": "Arrancando todo"}


def alta_en_central(ws_id, ubicacion):
    """Da de alta la WS en Central por su panel web (local o remoto). Devuelve un texto para el aviso."""
    base = f"http://127.0.0.1:{PUERTO_PANEL}" if ajustes["modo"] == "local" else (ajustes.get("panel") or "").rstrip("/")
    if not base:
        return "Dala de alta en el panel de CENTRAL (falta su dirección en Ajustes)"
    peticion = urllib.request.Request(f"{base}/api/estaciones", json.dumps({"id": ws_id, "ubicacion": ubicacion}).encode(),
                                      {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(peticion, timeout=3) as r:
            return json.load(r).get("mensaje", "dada de alta en CENTRAL")
    except urllib.error.HTTPError as exc:
        return json.load(exc).get("error", "CENTRAL no la ha dado de alta")   # p. ej. "ya está dada de alta"
    except OSError:
        return "CENTRAL no responde: dala de alta en su panel cuando esté en marcha"


def api_estacion(cuerpo):
    ws_id = _id_valido(cuerpo.get("id"), "Id de la estación")
    ubicacion = str(cuerpo.get("ubicacion") or "").strip()
    if not ubicacion or "#" in ubicacion:
        raise ValueError("Ubicación obligatoria y sin #")
    with lock:
        if any(e["id"] == ws_id for e in ajustes["estaciones"]):
            raise ValueError(f"{ws_id} ya está en el lanzador")
        ajustes["estaciones"].append({"id": ws_id, "ubicacion": ubicacion, "n": _hueco(ajustes["estaciones"])})
        guardar_ajustes()
        crear_procesos()
    alta = alta_en_central(ws_id, ubicacion)   # sin alta, Central denegaría su REG ("WS no registrada")
    with lock:
        hacer(f"ws:{ws_id}", "arrancar")
    return {"ok": True, "mensaje": f"Estación {ws_id} arrancando. {alta}", "vista": f"ws/{ws_id}"}


def api_operario(cuerpo):
    fo_id = _id_valido(cuerpo.get("id"), "Id del operario")
    with lock:
        if any(o["id"] == fo_id for o in ajustes["operarios"]):
            raise ValueError(f"{fo_id} ya está en el lanzador")
        ajustes["operarios"].append({"id": fo_id, "n": _hueco(ajustes["operarios"])})
        guardar_ajustes()
        crear_procesos()
        hacer(f"fo:{fo_id}", "arrancar")
    return {"ok": True, "mensaje": f"Operario {fo_id} arrancando", "vista": f"fo/{fo_id}"}


def api_quitar(cuerpo):
    grupo = str(cuerpo.get("grupo") or "")
    if not grupo.startswith(("ws:", "fo:")):
        raise ValueError("Solo se pueden quitar estaciones y operarios")
    with lock:
        for p in procesos_de(grupo):
            p.parar()
            del procesos[p.clave]
        tipo, ident = grupo.split(":", 1)
        clave = "estaciones" if tipo == "ws" else "operarios"
        ajustes[clave] = [x for x in ajustes[clave] if x["id"] != ident]
        guardar_ajustes()
    return {"ok": True, "mensaje": f"{ident} quitado"}


def api_ajustes(cuerpo):
    broker = str(cuerpo.get("broker") or "").strip()
    if ":" not in broker:
        raise ValueError("Kafka: escribe IP:puerto")
    modo = cuerpo.get("modo") if cuerpo.get("modo") in ("local", "remota") else "local"
    sockets = str(cuerpo.get("sockets") or "").strip()
    if modo == "remota" and ":" not in sockets:
        raise ValueError("CENTRAL remota: falta la dirección de sockets (host:puerto)")
    with lock:
        if modo == "remota":
            procesos["central"].parar()
        ajustes.update(broker=broker, modo=modo, sockets=sockets, panel=str(cuerpo.get("panel") or "").strip())
        guardar_ajustes()
    return {"ok": True, "mensaje": "Ajustes guardados: se aplican al arrancar o reiniciar cada proceso"}


def api_kafka(_cuerpo):
    if docker["ocupado"]:
        raise ValueError("Ya se está arrancando")
    docker["ocupado"] = True
    threading.Thread(target=arrancar_kafka_docker, daemon=True).start()
    return {"ok": True, "mensaje": "Arrancando Kafka con Docker…"}


def main():
    puerto = int(os.environ.get("LANZADOR_PORT", "8000"))
    with lock:
        crear_procesos()
    servidor = iniciar_web(
        "lanzador.html", api_estado,
        {"grupo": api_grupo, "todo": api_todo, "estacion": api_estacion, "operario": api_operario,
         "quitar": api_quitar, "ajustes": api_ajustes, "kafka": api_kafka, "fuga": api_fuga},
        consultas={"registro": api_registro}, puerto=puerto, nombre="Lanzador",
    )
    if servidor is None:
        sys.exit(1)
    threading.Thread(target=vigilar_kafka, daemon=True).start()
    threading.Thread(target=vigilar_averias, daemon=True).start()
    print(f"Abre http://localhost:{puerto} en el navegador. Ctrl+C para parar todo.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Parando todos los procesos…")
    finally:
        with lock:
            for p in reversed(list(procesos.values())):
                p.parar()


if __name__ == "__main__":
    main()
