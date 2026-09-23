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
Mientras no existan, probar con funciones falsas que solo hagan print("FUGA").

Usa common/sockets_utils.py. Mensajes: docs/PROTOCOLO.md

HECHO CUANDO: con el Engine en marcha, 'f' provoca un FUGA (una sola vez) y 'r' un RESUELTA
"""


import time
import socket 
import common.sockets_utils
HOST = ""


def notificar_fuga():
   print("FUGA detectada") 
   
def notificar_resuelta():
   print("Resuelta")
   
def iniciar_servidor_engine(puerto, ws_id):
   my_socket = socket.socket()
   my_socket.bind((HOST, puerto)) #aceptar todo para el puerto predeterminado
   my_socket.listen()
   
   print("Servidor de monitor ", ws_id," escucha en ", HOST, " ", puerto)
   conn, addr = my_socket.accept()
   
   common.sockets_utils.enviar(conn, f"ID#{ws_id}")
   
   
   estado_anterior = "OK"
   while True:
      common.sockets_utils.enviar(conn, "HC")
      respuesta = common.sockets_utils.recibir(conn)
      if respuesta == None:
         conn.close()
         print("no hay respuesta apagar engine")
         break
      if respuesta == "KO" and estado_anterior == "OK":
         notificar_fuga()
         estado_anterior = "KO"
         
      if respuesta == "OK" and estado_anterior == "KO":
         notificar_resuelta()
         estado_anterior = "OK"
         
      time.sleep(1)
   
   
if __name__ == "__main__":
   iniciar_servidor_engine(6000, "WS1")
