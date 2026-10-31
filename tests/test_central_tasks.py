from central import db
from central.WM_Central import (
    REGANDO,
    DISPONIBLE,
    DESCONECTADA,
    FUERA_SERVICIO,
    estaciones,
    cambiar_estado,
    cargar_estaciones,
    bloquear_estacion,
    procesar_peticion,
    procesar_telemetria,
    publicar_estado_ws,
    registrar_ws,
)


def test_cambiar_estado_updates_memory_state():
    estaciones.clear()
    estaciones["ws-1"] = {
        "ubicacion": "A1",
        "estado": DISPONIBLE,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": None,
    }

    cambiar_estado("ws-1", REGANDO)

    assert estaciones["ws-1"]["estado"] == REGANDO


def test_cambiar_estado_can_restore_to_disponible():
    estaciones.clear()
    estaciones["ws-2"] = {
        "ubicacion": "B2",
        "estado": REGANDO,
        "caudal": 12.5,
        "volumen": 34.2,
        "operario": "fo-1",
    }

    cambiar_estado("ws-2", DISPONIBLE)

    assert estaciones["ws-2"]["estado"] == DISPONIBLE
    assert estaciones["ws-2"]["operario"] is None


def test_cargar_estaciones_restores_them_as_desconectadas(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "central.db"))
    db.crear_tablas()
    db.guardar_estacion("ws-db", "Invernadero")
    db.set_estado("ws-db", REGANDO)
    estaciones.clear()

    cargar_estaciones()

    assert estaciones["ws-db"] == {
        "ubicacion": "Invernadero",
        "estado": DESCONECTADA,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": None,
    }
    assert db.get_estaciones() == [("ws-db", "Invernadero", DESCONECTADA)]


def test_registrar_ws_persists_station(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "central.db"))
    db.crear_tablas()
    estaciones.clear()

    registrar_ws(None, "ws-new", "Parque")

    assert db.get_estaciones() == [("ws-new", "Parque", DISPONIBLE)]


def test_procesar_peticion_autoriza_riego():
    estaciones.clear()
    estaciones["ws-riego"] = {
        "ubicacion": "Huerto",
        "estado": DISPONIBLE,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": None,
    }

    class DummyProducer:
        def __init__(self):
            self.messages = []

        def send(self, topic, value=None, *args, **kwargs):
            self.messages.append((topic, value))
            return None

        def flush(self):
            pass

    prod = DummyProducer()

    ok = procesar_peticion({"tipo": "PETICION", "fo_id": "FO1", "ws_id": "ws-riego", "duracion": 30}, prod)

    assert ok is True
    assert prod.messages[0][0] == "ordenes"
    assert prod.messages[0][1]["tipo"] == "REGAR"
    assert prod.messages[0][1]["ws_id"] == "ws-riego"
    assert prod.messages[1][1]["tipo"] == "AUTORIZADO"


def test_bloquear_estacion_marks_station_out_of_service():
    estaciones.clear()
    estaciones["ws-block"] = {
        "ubicacion": "Naranjos",
        "estado": DISPONIBLE,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": None,
    }

    bloquear_estacion("ws-block")

    assert estaciones["ws-block"]["estado"] == FUERA_SERVICIO


def test_procesar_peticion_rejects_unknown_operator():
    estaciones.clear()
    estaciones["ws-fo"] = {
        "ubicacion": "Patio",
        "estado": DISPONIBLE,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": None,
    }

    class DummyProducer:
        def __init__(self):
            self.messages = []

        def send(self, topic, value=None, *args, **kwargs):
            self.messages.append((topic, value))
            return None

        def flush(self):
            pass

    prod = DummyProducer()

    ok = procesar_peticion({"tipo": "PETICION", "fo_id": "FO999", "ws_id": "ws-fo", "duracion": 15}, prod)

    assert ok is False
    assert prod.messages[0][1]["tipo"] == "DENEGADO"
    assert prod.messages[0][1]["motivo"] == "Operario no válido"


def test_procesar_telemetria_reenvia_a_fo_and_summary():
    estaciones.clear()
    estaciones["ws-tele"] = {
        "ubicacion": "Huerto",
        "estado": DISPONIBLE,
        "caudal": 0.0,
        "volumen": 0.0,
        "operario": "FO1",
    }

    class DummyProducer:
        def __init__(self):
            self.messages = []

        def send(self, topic, value=None, *args, **kwargs):
            self.messages.append((topic, value))
            return None

        def flush(self):
            pass

    prod = DummyProducer()

    procesar_telemetria({"tipo": "TELEMETRIA", "ws_id": "ws-tele", "caudal": 20.0, "volumen": 4.0, "operario": "FO1"}, prod)
    procesar_telemetria({"tipo": "FIN", "ws_id": "ws-tele", "volumen": 7.5, "duracion": 20.0, "motivo": "TIEMPO"}, prod)

    telemetry = next(m for _, m in prod.messages if isinstance(m, dict) and m.get("tipo") == "TELEMETRIA")
    resumen = next(m for _, m in prod.messages if isinstance(m, dict) and m.get("tipo") == "RESUMEN")

    assert telemetry["fo_id"] == "FO1"
    assert telemetry["ws_id"] == "ws-tele"
    assert resumen["fo_id"] == "FO1"
    assert resumen["motivo"] == "TIEMPO"
