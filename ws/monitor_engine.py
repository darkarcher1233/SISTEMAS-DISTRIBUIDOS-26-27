"""
monitor_engine.py: mitad del MONITOR que habla con el ENGINE (sockets + health check)
Responsable: Vlad · Semana 1 · Tarjeta Trello: "WM_WS_M, parte Engine"
Lo usa: ws/WM_WS_M.py (lo lanza en un hilo)

QUÉ TIENE QUE TENER
  - iniciar_servidor_engine(puerto, ws_id) -> None
        1. Abre un socket servidor en <puerto> y espera a que se conecte WM_WS_E.
        2. Nada más conectarse, le envía  ID#<ws_id>  (el Engine recibe su ID del Monitor,
           tal como pide el enunciado).
        3. Bucle cada 1 s: envía  HC  y espera la respuesta con sock.settimeout(2)
             - Timeout, socket cerrado o respuesta KO  -> notificar_fuga()
             - Vuelve a responder OK                     -> notificar_resuelta()
           IMPORTANTE: avisar solo al CAMBIAR de estado (OK->KO o KO->OK), no cada segundo.
        4. Imprimir por pantalla:  Engine: OK  /  Engine: KO

DEPENDENCIA: from ws.monitor_central import notificar_fuga, notificar_resuelta   (de Abde)

Usa common/sockets_utils.py. Mensajes: docs/PROTOCOLO.md

HECHO CUANDO: con el Engine en marcha, 'f' provoca un FUGA (una sola vez) y 'r' un RESUELTA
"""


import time
import socket
import common.sockets_utils
from ws.monitor_central import notificar_fuga, notificar_resuelta
HOST = ""


def iniciar_servidor_engine(puerto, ws_id):
   my_socket = socket.socket()
   my_socket.bind((HOST, puerto)) #aceptar todo para el puerto predeterminado
   my_socket.listen()
   
   print("Servidor de monitor ", ws_id," escucha en ", HOST, " ", puerto)
   conn, addr = my_socket.accept()
   conn.settimeout(2) # si el Engine no contesta al HC en 2 s, cuenta como KO
   
   common.sockets_utils.enviar(conn, f"ID#{ws_id}")
   
   
   estado_anterior = "OK"
   while True:
      try:
         common.sockets_utils.enviar(conn, "HC")
         respuesta = common.sockets_utils.recibir(conn)
      except (OSError, ValueError): # timeout, conexión cortada o cabecera rota
         respuesta = None

      if respuesta == None:
         # Engine caído: es una avería -> FUGA (si no se había avisado ya) y se deja de vigilar
         print("Engine: KO (no responde)")
         if estado_anterior == "OK":
            notificar_fuga()
         conn.close()
         break

      if respuesta == "KO" and estado_anterior == "OK":
         print("Engine: KO")
         notificar_fuga()
         estado_anterior = "KO"
         
      if respuesta == "OK" and estado_anterior == "KO":
         print("Engine: OK")
         notificar_resuelta()
         estado_anterior = "OK"
         
      time.sleep(1)
   
   
if __name__ == "__main__":
   iniciar_servidor_engine(6000, "WS1")
