"""
monitor_engine.py: mitad del MONITOR que habla con el ENGINE (sockets + health check)
Responsable: Vlad · Semana 1 · Tarjeta Trello: "WM_WS_M, parte Engine"
Lo usa: WM_WS/WM_WS_M.py (lo lanza en un hilo)

QUÉ HACE
  - iniciar_servidor_engine(puerto, ws_id, ubicacion) -> None
        1. Abre un socket servidor en <puerto> y espera a que se conecte WM_WS_E.
        2. Nada más conectarse, le envía  ID#<ws_id>#<ubicacion>  (el Engine recibe su ID
           del Monitor, tal como pide el enunciado; la ubicación es para su pantalla).
        3. Bucle cada 1 s: envía  HC  y espera la respuesta con sock.settimeout(2)
             - Respuesta KO                              -> notificar_fuga()
             - Timeout o socket cerrado (Engine caído)   -> notificar_fuga() y se vuelve a
               esperar a que el Engine se conecte otra vez
             - Vuelve a responder OK                     -> notificar_resuelta()
           IMPORTANTE: se avisa solo al CAMBIAR de estado (OK->KO o KO->OK), no cada segundo.
        4. Imprime por pantalla:  Engine: OK  /  Engine: KO
  - estado: dict para la pantalla web del Monitor (salud, motivo, Engine conectado)

DEPENDENCIA: from WM_WS.monitor_central import notificar_fuga, notificar_resuelta   (de Abde)
Usa common/sockets_utils.py. Mensajes: docs/PROTOCOLO.md
"""

import socket
import time

import common.sockets_utils
from WM_WS import monitor_central
from WM_WS.monitor_central import notificar_fuga, notificar_resuelta

HOST = ""

# salud_ok=False -> se ha avisado (o se avisará) de FUGA a Central
estado = {"engine": None, "salud_ok": True, "motivo": "", "puerto": None}


def _avisar(texto):
   monitor_central.avisar(texto)


def _pasar_a_ko(motivo):
   if estado["salud_ok"]:
      estado["salud_ok"] = False
      estado["motivo"] = motivo
      _avisar(f"Engine: KO ({motivo})")
      notificar_fuga()


def _pasar_a_ok():
   if not estado["salud_ok"]:
      estado["salud_ok"] = True
      estado["motivo"] = ""
      _avisar("Engine: OK")
      notificar_resuelta()


def _vigilar(conn):
   """Health check cada segundo mientras el Engine siga conectado."""
   while True:
      try:
         common.sockets_utils.enviar(conn, "HC")
         respuesta = common.sockets_utils.recibir(conn)
      except (OSError, ValueError):  # timeout, conexión cortada o cabecera rota
         respuesta = None

      if respuesta is None:
         _pasar_a_ko("El engine no responde")
         return
      if respuesta == "KO":
         _pasar_a_ko("El sensor del engine detecta una avería")
      elif respuesta == "OK":
         _pasar_a_ok()
      time.sleep(1)


def iniciar_servidor_engine(puerto, ws_id, ubicacion=""):
   my_socket = socket.socket()
   my_socket.bind((HOST, puerto))  # aceptar todo para el puerto predeterminado
   my_socket.listen()
   estado["puerto"] = puerto
   _avisar(f"Monitor {ws_id} esperando al Engine en el puerto {puerto}")

   while True:
      conn, addr = my_socket.accept()
      conn.settimeout(2)  # si el Engine no contesta al HC en 2 s, cuenta como KO
      estado["engine"] = f"{addr[0]}:{addr[1]}"
      _avisar(f"Engine conectado desde {addr[0]}:{addr[1]}")
      try:
         common.sockets_utils.enviar(conn, f"ID#{ws_id}#{ubicacion}")
         _vigilar(conn)
      except OSError:
         _pasar_a_ko("El engine no responde")
      finally:
         conn.close()
         estado["engine"] = None
      _avisar("Engine desconectado; esperando a que vuelva a conectarse")


if __name__ == "__main__":
   iniciar_servidor_engine(6000, "WS1")
