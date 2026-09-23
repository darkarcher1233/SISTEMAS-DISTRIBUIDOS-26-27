"""
panel.py: panel de monitorización de CENTRAL en CONSOLA (librería colorama)
Responsable: Vlad · Semana 3 · Tarjeta Trello: "Panel de CENTRAL en consola con colores"
Lo usa: central/WM_Central.py (lo lanza en un hilo que redibuja cada 1 s)

QUÉ TIENE QUE TENER
  - dibujar_panel(estados: dict, log: list[str]) -> None
        estados = el dict de Central {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario"}}
        (acordar el formato exacto con Abde)
        log     = últimos mensajes del sistema (se muestran las 5-8 últimas líneas)
  - Limpia la pantalla (cls / clear) y pinta una fila por WS:
        ID | Ubicación | Estado | Caudal (L/min) | Volumen (L) | Operario
  - Colores de fondo según el estado (enunciado, página 4):
        DISPONIBLE      -> VERDE
        REGANDO         -> VERDE PARPADEANDO (alternar dos verdes cada segundo) + caudal, volumen, operario
        FUGA            -> ROJO
        FUERA_SERVICIO  -> NARANJA (colorama no tiene naranja: usar amarillo) + texto "Fuera de Servicio"
        DESCONECTADA    -> GRIS
  - Debajo de la tabla: el log

HECHO CUANDO: se ven los 5 estados correctamente con varias WS a la vez
"""
