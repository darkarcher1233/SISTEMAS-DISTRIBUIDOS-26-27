from central.WM_Central import REGANDO, DISPONIBLE, estaciones, cambiar_estado


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
