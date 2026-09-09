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
            partes = msg.split()
            if(len(partes) != 3):
                resultado = "Formato incorrecto, formato es numero|operacion|numero"
                conn.send(resultado.encode(FORMAT))
            if(len(partes) == 3):
                num1, op, num2 = partes

            try:
                num1 = float(num1)
                num2 = float(num2)
                
                match op:
                    case "+":
                        resultado = f"Resultado de la operacion: {num1 + num2}"
                    case "-":
                        resultado = f"Resultado de la operacion: {num1 - num2}"
                    case "*":
                        resultado = f"Resultado de la operacion: {num1 * num2}"
                    case "/":
                        if num2 == 0:
                            resultado = "Error: Division por cero"
                        else:
                            resultado = f"Resultado de la operacion: {num1 / num2}"
                    case _:
                        resultado = "Formato incorrecto, formato es numero|operacion|numero"
                    
            except ValueError:
                resultado = "Formato incorrecto, formato es numero|operacion|numero"
            
      
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
            conn.send(f"HOLA CLIENTE: He recibido tu operacion: {msg}, {resultado} ".encode(FORMAT))
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
