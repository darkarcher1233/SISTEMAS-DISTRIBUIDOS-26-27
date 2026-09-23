"""
sockets_utils.py: funciones comunes para enviar y recibir mensajes por SOCKETS
Responsable: Vlad · Semana 1 · Tarjeta Trello: "Estructura del repo + common/sockets_utils.py"
Lo usan: WM_Central, ws/monitor_central.py, ws/monitor_engine.py, WM_WS_E

QUÉ TIENE QUE TENER
  - Constantes: HEADER = 64, FORMAT = "utf-8"
  - enviar(sock, msg: str) -> None
        Envía primero la longitud del mensaje, rellenada con espacios hasta HEADER bytes,
        y después el mensaje. Es lo mismo que la función send() de ejemplos/cliente_*.py.
  - recibir(sock) -> str | None
        Lee los HEADER bytes de cabecera, convierte a int y lee ese número de bytes.
        Devuelve None si el otro extremo ha cerrado el socket (así quien llama sabe
        que la conexión se ha perdido: DESCONECTADA / FUGA).

FORMATO DE LOS MENSAJES: campos separados por '#'  ->  "REG#WS1#Parque El Palmeral"
Lista completa de mensajes: docs/PROTOCOLO.md

OPCIONAL (solo si al final sobra tiempo): versión con <STX><DATA><ETX><LRC> y ACK/NACK.
El enunciado lo recomienda, pero no es obligatorio.

HECHO CUANDO: Abde puede importarlo en WM_Central y en monitor_central.py
"""
