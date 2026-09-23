"""
WM_Central.py: CENTRAL, el cerebro del sistema (nombre obligatorio: WM_Central)
Responsable: Abde · Semanas 1, 2 y 3

USO
    python -m central.WM_Central <puerto_escucha> <ip_broker:puerto>
    (ejecutar desde la raíz del repo para poder importar common/)

SEMANA 1: solo sockets (Tarjeta: "WM_Central: servidor de sockets + registro de WS")
  - Servidor concurrente: un hilo por cada WM_WS_M conectado. SIN límite de conexiones
    (quitar MAX_CONEXIONES del ejemplo de clase)
  - Estado en memoria: dict {ws_id: {"ubicacion", "estado", "caudal", "volumen", "operario"}}
    protegido con threading.Lock
  - REG#id#ubicacion -> alta o actualización -> DISPONIBLE, responder OK
  - FUGA#id -> FUGA   ·   RESUELTA#id -> DISPONIBLE
  - Si se cierra el socket de un Monitor -> DESCONECTADA
  - Imprimir la tabla de estados cada vez que cambie algo
  - El broker se recibe por argumento pero en la semana 1 NO se usa

SEMANA 2 (Tarjetas: "SQLite..." + "estados de riego + consumo de telemetría por Kafka")
  - Al arrancar: cargar las WS de la BD (central/db.py) -> todas DESCONECTADA
  - Cada cambio de estado -> también a la BD
  - Hilo consumidor de 'telemetria': caudal y volumen cada 1 s · FIN -> DISPONIBLE

SEMANA 3 (Tarjetas: "flujo de activación de riego" + "menú de órdenes")
  - Hilo consumidor de 'peticiones': validar operario y WS DISPONIBLE -> AUTORIZADO / DENEGADO
  - Reenviar al FO la telemetría de su riego y el RESUMEN final
  - Cada ~2 s publicar ESTADO_WS (lista de WS) en 'ordenes'
  - Menú (punto 11 del enunciado): iniciar riego / bloquear / activar, a una WS o a todas (ALL)
  - Lanzar el panel (central/panel.py de Vlad) en un hilo

ESTADOS: DISPONIBLE · REGANDO · FUGA · FUERA_SERVICIO · DESCONECTADA
MENSAJES: docs/PROTOCOLO.md
-------------------------------------------------------------------------------
Debajo está la versión inicial de Abde (basada en ejemplos/servidor_concurrente.py).
"""

#server


import socket 
import threading


HEADER = 64
PORT = 5050
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            
            # string ya esta comprabada esta comprobada que tiene 3 palabras 1 es numero  operacion 3 otro numero
            partes = msg.split("#")
            if(len(partes) != 3):
                resultado = "Formato incorrecto, formato es <Registro>#<id_Estacion>#<Ubicacion>"
                conn.send(resultado.encode(FORMAT))
            if(len(partes) == 3):
                registro,id_Estacion, ubicacion  = partes
                print(f"He recibido del cliente [{addr}] los datos: {registro} {id_Estacion} {ubicacion}")
            
            conn.send(f"STATUS#OK#Estacion registrada correctamente".encode(FORMAT))
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
        

def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

######################### MAIN ##########################


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")

start()
