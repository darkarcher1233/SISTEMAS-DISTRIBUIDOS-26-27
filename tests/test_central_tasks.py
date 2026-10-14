from central import db
from central.WM_Central import (
    REGANDO,
    DISPONIBLE,
    DESCONECTADA,
    estaciones,
    cambiar_estado,
    cargar_estaciones,
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
