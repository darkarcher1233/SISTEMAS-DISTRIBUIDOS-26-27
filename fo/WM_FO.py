"""
WM_FO.py: aplicación del OPERARIO DE CAMPO (nombre obligatorio: WM_FO)
Responsable: Vlad · Semana 3 · Tarjetas: "WM_FO: modo manual" + "WM_FO: peticiones desde fichero"
Solo usa KAFKA (no tiene sockets).

USO
    python -m fo.WM_FO <ip_broker:puerto> <id_operario>

QUÉ TIENE QUE HACER
  - Menú:  1 ver WS disponibles · 2 pedir riego (ws_id + duración) · 3 leer fichero · 0 salir
  - Pedir riego -> publicar en 'peticiones'  {tipo: PETICION, fo_id, ws_id, duracion}
  - Hilo consumidor de 'ordenes', quedándose SOLO con los mensajes de su fo_id:
        AUTORIZADO · DENEGADO (+motivo) · TELEMETRIA de su WS · RESUMEN final
        (+ aviso claro si hay FUGA o BLOQUEO durante el riego)
  - Ver WS disponibles: guardar el último ESTADO_WS que publica Central y mostrarlo
  - Modo fichero (fo/peticiones/*.txt, una línea "ws_id;duracion"):
        enviar la primera -> esperar a que termine (bien o mal) -> esperar 4 s -> la siguiente
        Preparar fo1.txt y fo2.txt para la defensa (incluir alguna WS en fuga o bloqueada
        para enseñar denegaciones)

HECHO CUANDO: 2 FO piden riego a la vez y cada uno ve solo lo suyo
Mensajes: docs/PROTOCOLO.md
"""
