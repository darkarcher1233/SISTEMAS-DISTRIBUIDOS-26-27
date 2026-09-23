"""
WM_WS_M.py: MONITOR de la Watering Station, programa principal (nombre obligatorio: WM_WS_M)
Responsable: Abde (este fichero y monitor_central.py) · Vlad (monitor_engine.py)
Semana 1 · Tarjeta Trello: "WM_WS_M, parte Central"

USO
    python -m ws.WM_WS_M <puerto_para_engine> <ip_central:puerto> <id_ws> [ubicacion]

QUÉ TIENE QUE HACER (solo coordinar: la lógica está en los otros dos ficheros)
  1. Leer y validar los argumentos
  2. monitor_central.registrar(...)                   -> se registra en Central (Abde)
  3. monitor_engine.iniciar_servidor_engine(...)      -> en un hilo: espera al Engine y
                                                          hace el health check (Vlad)
  4. Mantenerse vivo mostrando el estado de la WS

POR QUÉ ESTÁ DIVIDIDO: cada uno trabaja en su fichero y no hay conflictos en Git.
-------------------------------------------------------------------------------
Debajo está la versión inicial de Abde. Ahora mismo es un cliente simple: hay que
adaptarla a lo de arriba y mover la lógica de conexión a monitor_central.py.
"""

#cliente

import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
########## MAIN ##########


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2020/2021 SOCKET CLIENT ****")

if  (len(sys.argv) == 4):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    msg=sys.argv[3]
    
    print("Envio al servidor: ", msg)
    send(msg)
    print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
    client.close()
        

    print ("SE ACABO LO QUE SE DABA")
    print("Envio al servidor: ", FIN)
    #send(FIN)
    #client.close()
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Texto Bienvenida>")
