"""Tests de los casos de resiliencia de la guía de corrección (fase 4).

Los que necesitan matar procesos de verdad están en docs/PRUEBAS_RESILIENCIA.md (pruebas manuales).
"""
import json
import os
import socket
import threading
import time

import pytest

from WM_Central import WM_Central as C
from WM_Central import db
from WM_FO import WM_FO as F
from WM_WS import WM_WS_E as E


class ProductorFalso:
    """Guarda lo que se 'envía' a Kafka en vez de enviarlo."""

    def __init__(self):
        self.mensajes = []

    def send(self, topic, value=None, *args, **kwargs):
        self.mensajes.append((topic, value))
        return self   # para .get(timeout=...)

    def get(self, timeout=None):
        return None

    def flush(self, timeout=None):
        pass

    def tipos(self):
        return [m.get("tipo") for _, m in self.mensajes]


def ws_regando(ws_id, operario="FO1"):
    C.estaciones[ws_id] = {"ubicacion": "Parque", "estado": C.REGANDO, "caudal": 20.0, "volumen": 1.0,
                           "operario": operario, "inicio_riego": time.time(), "duracion": 30,
                           "ultimo_riego": None, "motivo_fuga": None}


@pytest.fixture(autouse=True)
def limpiar():
    C.estaciones.clear()
    C.conexiones.clear()
    yield
    C.estaciones.clear()
    C.conexiones.clear()


# ---------------- a) cae un Monitor ----------------------------------------------------
def test_monitor_caido_durante_riego_desconecta_y_avisa_al_fo(monkeypatch):
    avisos = []
    monkeypatch.setattr(C, "_avisar_fo", lambda fo, ws, motivo: avisos.append((fo, ws, motivo)))
    ws_regando("WSA")
    conn = object()
    C.conexiones["WSA"] = conn

    C.desconectar_ws(conn, "WSA")

    assert C.estaciones["WSA"]["estado"] == C.DESCONECTADA
    assert avisos and avisos[0][:2] == ("FO1", "WSA")
    assert C.estaciones["WSA"]["operario"] == "FO1"   # se guarda para el FIN que llegará


def test_engine_sin_monitor_no_admite_riegos(monkeypatch):
    monkeypatch.setattr(E, "con_monitor", False)
    monkeypatch.setattr(E, "bloqueada", False)
    monkeypatch.setattr(E, "regando", False)
    monkeypatch.setattr(E, "ws_id", "WSA")

    assert E.iniciar_riego(10, "FO1", "x:1") is False
    with pytest.raises(ValueError, match="averiada"):
        E.pedir_riego_a_central("x:1", "FO1", 10)


def test_fin_tras_desconexion_llega_al_fo_con_su_operario():
    C.estaciones["WSB"] = {"ubicacion": "Parque", "estado": C.DESCONECTADA, "caudal": 0.0, "volumen": 0.0,
                           "operario": None, "inicio_riego": None, "duracion": 0,
                           "ultimo_riego": None, "motivo_fuga": None}
    prod = ProductorFalso()

    C.procesar_telemetria({"tipo": "FIN", "ws_id": "WSB", "operario": "FO2", "volumen": 3.5,
                           "duracion": 12.0, "motivo": "MONITOR"}, prod)

    resumen = next(m for _, m in prod.mensajes if m["tipo"] == "RESUMEN")
    assert (resumen["fo_id"], resumen["volumen"], resumen["motivo"]) == ("FO2", 3.5, "MONITOR")
    assert C.estaciones["WSB"]["estado"] == C.DESCONECTADA
    assert db.volumen_por_ws()["WSB"] == 3.5   # el riego queda guardado en SQLite


def test_sin_mensajes_de_vida_central_marca_desconectada(monkeypatch):
    """Monitor que se queda callado sin cerrar el socket (red cortada): a los TIMEOUT_VIDA s, DESCONECTADA."""
    from common.sockets_utils import enviar, recibir
    monkeypatch.setattr(C, "TIMEOUT_VIDA", 1)
    C.alta_ws("WSV", "Parque")
    lado_central, lado_monitor = socket.socketpair()
    hilo = threading.Thread(target=C.atender_monitor, args=(lado_central, ("prueba", 1)), daemon=True)
    hilo.start()

    enviar(lado_monitor, "REG#WSV#Parque")
    assert recibir(lado_monitor) == "OK"
    enviar(lado_monitor, "VIVO#WSV")          # un latido...
    time.sleep(0.5)
    assert C.estaciones["WSV"]["estado"] == C.DISPONIBLE
    hilo.join(timeout=3)                       # ...y luego silencio

    assert C.estaciones["WSV"]["estado"] == C.DESCONECTADA
    lado_monitor.close()


# ---------------- b) cae un Engine ----------------------------------------------------
def test_fuga_durante_riego_avisa_al_fo(monkeypatch):
    avisos = []
    monkeypatch.setattr(C, "_avisar_fo", lambda fo, ws, motivo: avisos.append((fo, ws)))
    ws_regando("WSC", "FO3")

    C.cambiar_estado("WSC", C.FUGA)

    assert C.estaciones["WSC"]["estado"] == C.FUGA
    assert avisos == [("FO3", "WSC")]


def test_engine_recupera_lo_regado_y_envia_el_fin(tmp_path, monkeypatch):
    prod = ProductorFalso()
    datos = tmp_path / "datos"
    datos.mkdir()
    monkeypatch.setattr(E, "DATOS_DIR", str(datos))
    monkeypatch.setattr(E, "ws_id", "WSD")
    monkeypatch.setattr(E, "productor_engine", lambda broker: prod)

    class HiloSincrono:   # ejecuta el hilo en el momento, para poder comprobar el resultado
        def __init__(self, target, args=(), daemon=None):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(E.threading, "Thread", HiloSincrono)
    # Lo que habría dejado en disco un Engine que se cayó a los 20 s de riego
    (datos / "riego_WSD.json").write_text(json.dumps(
        {"ws_id": "WSD", "operario": "FO1", "duracion": 60, "volumen": 6.789, "segundos": 20.0}))

    E.recuperar_riego_interrumpido("x:1")

    assert prod.mensajes == [("telemetria", {"tipo": "FIN", "ws_id": "WSD", "operario": "FO1",
                                             "volumen": 6.789, "duracion": 20.0, "motivo": "AVERIA"})]
    assert os.listdir(datos) == []   # ni riego en curso ni FIN pendiente


# ---------------- c) cae un FO ----------------------------------------------------------
def test_fo_recibe_el_resumen_de_un_riego_anterior(monkeypatch):
    monkeypatch.setattr(F, "fo_id", "FO1")
    monkeypatch.setattr(F, "solicitudes", [])

    F.procesar_orden({"tipo": "RESUMEN", "fo_id": "FO1", "ws_id": "WS1", "volumen": 4.2,
                      "duracion": 13.0, "motivo": "TIEMPO"})

    assert F.solicitudes[0]["estado"] == "FINALIZADA"
    assert F.solicitudes[0]["resumen"]["volumen"] == 4.2


# ---------------- d) cae Central ------------------------------------------------------
def test_fo_no_envia_peticiones_con_central_caida(monkeypatch):
    prod = ProductorFalso()
    monkeypatch.setattr(F, "fo_id", "FO1")
    monkeypatch.setattr(F, "solicitudes", [])
    monkeypatch.setattr(F, "instante_estado_ws", time.time() - 60)   # último ESTADO_WS hace 1 min

    evento = F.pedir_riego(prod, "WS1", 10)

    assert prod.mensajes == []
    assert evento.is_set()   # el modo fichero no se queda esperando
    assert evento.solicitud["estado"] == "NO_ENVIADA"
    assert evento.solicitud["motivo"] == "Imposible conectar con WM_Central"
    with pytest.raises(ValueError, match="Imposible conectar con WM_Central"):
        F.web_solicitar({"ws": "WS1", "duracion": 10})


def test_engine_no_pide_riegos_con_central_caida(monkeypatch):
    monkeypatch.setattr(E, "con_monitor", True)
    monkeypatch.setattr(E, "ws_id", "WSE")
    monkeypatch.setattr(E, "ultimo_estado_central", 0.0)

    with pytest.raises(ValueError, match="Imposible conectar con WM_Central"):
        E.pedir_riego_a_central("x:1", "FO1", 10)


# ---------------- f) ninguna transacción perdida -----------------------------------------
def test_peticion_vieja_se_contesta_como_caducada():
    C.estaciones["WSF"] = {"ubicacion": "Parque", "estado": C.DISPONIBLE, "caudal": 0.0, "volumen": 0.0,
                           "operario": None, "inicio_riego": None, "duracion": 0,
                           "ultimo_riego": None, "motivo_fuga": None}
    prod = ProductorFalso()
    hace_un_minuto = (time.time() - 60) * 1000

    ok = C.procesar_peticion_kafka({"tipo": "PETICION", "fo_id": "FO1", "ws_id": "WSF", "duracion": 10},
                                   hace_un_minuto, prod=prod)

    assert ok is False
    assert prod.tipos() == ["DENEGADO"]
    assert "caducada" in prod.mensajes[0][1]["motivo"]
    assert C.estaciones["WSF"]["estado"] == C.DISPONIBLE   # no se lanza un riego viejo


def test_peticion_reciente_se_procesa_normal():
    C.estaciones["WSG"] = {"ubicacion": "Parque", "estado": C.DISPONIBLE, "caudal": 0.0, "volumen": 0.0,
                           "operario": None, "inicio_riego": None, "duracion": 0,
                           "ultimo_riego": None, "motivo_fuga": None}
    prod = ProductorFalso()

    ok = C.procesar_peticion_kafka({"tipo": "PETICION", "fo_id": "FO1", "ws_id": "WSG", "duracion": 10},
                                   time.time() * 1000, prod=prod)

    assert ok is True
    assert prod.tipos() == ["REGAR", "AUTORIZADO"]
