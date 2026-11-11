from WM_Central import db
from WM_Central.WM_Central import (
    alta_ws,
    autenticar_ws,
    conexiones,
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

    ws = estaciones["ws-db"]
    assert ws["ubicacion"] == "Invernadero"
    assert ws["estado"] == DESCONECTADA
    assert (ws["caudal"], ws["volumen"], ws["operario"]) == (0.0, 0.0, None)
    assert ("ws-db", "Invernadero", DESCONECTADA) in db.get_estaciones()


def test_registrar_ws_persists_station(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "central.db"))
    db.crear_tablas()
    estaciones.clear()

    registrar_ws(None, "ws-new", "Parque")

    assert ("ws-new", "Parque", DISPONIBLE) in db.get_estaciones()


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


def test_fin_por_bloqueo_no_devuelve_la_ws_a_disponible():
    estaciones.clear()
    estaciones["ws-b"] = {"ubicacion": "Rotonda", "estado": DISPONIBLE, "caudal": 0.0,
                          "volumen": 0.0, "operario": None}
    procesar_telemetria({"tipo": "TELEMETRIA", "ws_id": "ws-b", "caudal": 20.0, "volumen": 1.0, "operario": "FO1"})
    bloquear_estacion("ws-b")
    procesar_telemetria({"tipo": "TELEMETRIA", "ws_id": "ws-b", "caudal": 20.0, "volumen": 1.3, "operario": "FO1"})
    procesar_telemetria({"tipo": "FIN", "ws_id": "ws-b", "volumen": 1.3, "duracion": 4.0, "motivo": "BLOQUEO"})

    assert estaciones["ws-b"]["estado"] == FUERA_SERVICIO


def test_riego_ordenado_desde_central_no_necesita_operario_de_la_bd():
    estaciones.clear()
    estaciones["ws-c"] = {"ubicacion": "Plaza", "estado": DISPONIBLE, "caudal": 0.0,
                          "volumen": 0.0, "operario": None}

    class DummyProducer:
        def __init__(self):
            self.messages = []

        def send(self, topic, value=None, *args, **kwargs):
            self.messages.append((topic, value))

        def flush(self):
            pass

    prod = DummyProducer()
    ok = procesar_peticion({"tipo": "PETICION", "fo_id": "CENTRAL", "ws_id": "ws-c", "duracion": 5},
                           prod, desde_central=True)

    assert ok is True
    assert estaciones["ws-c"]["estado"] == REGANDO
    assert prod.messages[0][1]["tipo"] == "REGAR"


def test_fin_por_fuga_deja_la_ws_en_fuga_aunque_el_monitor_avise_despues():
    estaciones.clear()
    estaciones["ws-f"] = {"ubicacion": "Huerto", "estado": DISPONIBLE, "caudal": 0.0,
                          "volumen": 0.0, "operario": None}
    procesar_telemetria({"tipo": "TELEMETRIA", "ws_id": "ws-f", "caudal": 20.0, "volumen": 1.0, "operario": "FO1"})
    procesar_telemetria({"tipo": "FIN", "ws_id": "ws-f", "volumen": 1.2, "duracion": 4.0, "motivo": "FUGA"})
    assert estaciones["ws-f"]["estado"] == "FUGA"

    cambiar_estado("ws-f", "FUGA")      # aviso del Monitor que llega después
    assert estaciones["ws-f"]["estado"] == "FUGA"


def test_bd_nueva_trae_ws1_y_ws2_desconectadas():
    nombres = {ws_id: (ubic, estado) for ws_id, ubic, estado in db.get_estaciones()}
    assert nombres["WS1"] == ("Parque Norte", DESCONECTADA)
    assert nombres["WS2"] == ("Parque Sur", DESCONECTADA)


def test_autenticar_deniega_ws_no_registrada():
    estaciones.clear()
    conexiones.clear()

    assert autenticar_ws(object(), "WS99", "Parque X") == "WS no registrada"
    assert "WS99" not in estaciones


def test_alta_y_despues_autenticacion_aceptada():
    estaciones.clear()
    conexiones.clear()

    assert alta_ws("WS3", "Jardín de prueba") is None
    assert estaciones["WS3"]["estado"] == DESCONECTADA
    assert alta_ws("WS3", "Otro") == "WS3 ya está dada de alta"

    assert autenticar_ws(object(), "WS3", "Jardín de prueba") is None
    assert estaciones["WS3"]["estado"] == DISPONIBLE


def test_autenticar_deniega_ws_ya_conectada_desde_otro_monitor():
    estaciones.clear()
    conexiones.clear()
    alta_ws("WS4", "Rotonda")
    primero, segundo = object(), object()

    assert autenticar_ws(primero, "WS4", "Rotonda") is None
    assert autenticar_ws(segundo, "WS4", "Rotonda") == "WS ya conectada desde otro Monitor"
    assert conexiones["WS4"] is primero


def test_volumen_por_ws_suma_los_riegos_terminados():
    db.guardar_riego("WS1", "fo1", "a", "b", 2.5)
    db.guardar_riego("WS1", "fo2", "c", "d", 1.5)

    assert db.volumen_por_ws()["WS1"] == 4.0
