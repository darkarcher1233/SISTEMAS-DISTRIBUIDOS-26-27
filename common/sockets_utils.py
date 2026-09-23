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



#### CONSTANTES
HEADER = 64
FORMAT = "utf-8"
#### CONSTANTES

def enviar(sock, msg: str):
      mensaje = msg.encode(FORMAT) #str to bites
      msg_length = len(mensaje)
      send_length = str(msg_length).encode(FORMAT) 
      send_length += b" " * (HEADER - len(send_length)) 
      
      sock.send(send_length)
      sock.send(mensaje)  
      
      
       
      
def recibir(sock):
      try:
            header = sock.recv(HEADER)
      except ConnectionResetError:
            return None
      if not header:             
            return None
      
      msg_length = int(header.decode(FORMAT))
      mensaje = sock.recv(msg_length).decode(FORMAT)
      return mensaje
