"""
WM_WS_E.py: ENGINE de la Watering Station (nombre obligatorio: WM_WS_E)
Responsable: Vlad · Semanas 1 (sockets) y 2 (riego + Kafka)

USO
    python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>

SEMANA 1: solo sockets (Tarjeta: "WM_WS_E: socket con el Monitor + responder HC + tecla KO")
  - Conectar por socket al Monitor y recibir  ID#<ws_id>  (así sabe qué WS es)
  - Hilo que responde a cada  HC  con  OK  o  KO  según la variable  averia
  - Hilo de teclado:  f -> averia = True (simula FUGA)  ·  r -> averia = False (reparada)
  - Mostrar el estado de la WS por pantalla
  - El broker se recibe por argumento pero en la semana 1 NO se usa

SEMANA 2: riego + Kafka (Tarjeta: "WM_WS_E: simulación de riego + telemetría por Kafka")
  - Consumir el topic 'ordenes' y quedarse solo con los mensajes de su ws_id:
        REGAR (duracion, operario) · BLOQUEAR · ACTIVAR
  - Riego = hilo que cada 1 s calcula el caudal (aleatorio, p. ej. 15-25 L/min),
    va sumando el volumen y publica TELEMETRIA en 'telemetria'
  - El riego termina por: tiempo agotado · tecla 's' · averia · orden BLOQUEAR
        -> publicar FIN (volumen, duracion, motivo)
  - Menú local:  m = riego manual (pide duración)  ·  s = parar el riego

NO HACE FALTA: reconexión automática (si se cae, se reinicia a mano)
Mensajes: docs/PROTOCOLO.md
"""

averia = False

import socket
import sys
import threading
import common.sockets_utils  
import time

def responder_health_check(sock):
  while True:
    msg = common.sockets_utils.recibir(sock)
    if not msg:
      print("Monitor desconectado")
      break
    if msg == "HC":
      if averia == False:
        common.sockets_utils.enviar(sock, "OK")
      else:
        common.sockets_utils.enviar(sock, "KO")
    
    
    


if len(sys.argv) != 3:
  print("USO: python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>")
  exit()
  
  
ip_broker, puerto_broker = sys.argv[1].split(":")
ip_monitor, puerto_monitor = sys.argv[2].split(":")
puerto_broker = int(puerto_broker)
puerto_monitor = int(puerto_monitor)

my_socket = socket.socket()
my_socket.connect((ip_monitor,puerto_monitor))

mensaje = common.sockets_utils.recibir(my_socket)
ws_id = mensaje.split("#")[1]

print(f"[{ws_id}] conectado al monitor")
threading.Thread(target=responder_health_check, args=(my_socket,), daemon=True).start()

while True:          # Блок 7 (временно), держит программу живой
  time.sleep(1)
