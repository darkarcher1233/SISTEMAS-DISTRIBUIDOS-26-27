#SERVIDOR
import socket
import threading

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
MAX_CONEXIONES = 10


class WM_Central:
    """Servidor central que acepta registros de estaciones (concurrencia con hilos)."""
    def __init__(self, host=None, port=PORT, max_conex=MAX_CONEXIONES):
        self.SERVER = host if host else socket.gethostbyname(socket.gethostname())
        self.PORT = int(port)
        self.ADDR = (self.SERVER, self.PORT)
        self.HEADER = HEADER
        self.FORMAT = FORMAT
        self.MAX_CONEXIONES = max_conex

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.ADDR)

    def handle_client(self, conn, addr):
        print(f"[NUEVA CONEXION] {addr} connected.")
        connected = True
        while connected:
            try:
                msg_length = conn.recv(self.HEADER).decode(self.FORMAT)
                if not msg_length:
                    break
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(self.FORMAT)
                if not msg:
                    break
                if msg == "FIN":
                    connected = False
                    break

                partes = msg.split('#')
                if len(partes) != 3 or partes[0] != 'REGISTRO':
                    resultado = "STATUS#ERROR#Formato incorrecto. Use REGISTRO#<ID>#<UBICACION>"
                    conn.send(resultado.encode(self.FORMAT))
                else:
                    _, station_id, location = partes
                    if not station_id.strip() or not location.strip():
                        resultado = "STATUS#ERROR#ID o UBICACION vacíos"
                        conn.send(resultado.encode(self.FORMAT))
                    else:
                        print(f"[REGISTRO] Estacion ID={station_id} - Ubicacion={location} - desde {addr}")
                        resultado = "STATUS#OK#Estacion registrada correctamente"
                        conn.send(resultado.encode(self.FORMAT))

            except Exception as e:
                print(f"[ERROR] {e}")
                break

        conn.close()
        print(f"[DESCONECTADO] {addr} cerrado.")

    def start(self):
        self.server.listen()
        print(f"[LISTENING] WM_Central a la escucha en {self.SERVER}:{self.PORT}")
        while True:
            conn, addr = self.server.accept()
            CONEX_ACTIVAS = threading.active_count()
            if CONEX_ACTIVAS <= self.MAX_CONEXIONES:
                thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {threading.active_count()-1}")
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. Rechazando nueva conexión")
                conn.send("STATUS#ERROR#Servidor ocupado, inténtalo más tarde".encode(self.FORMAT))
                conn.close()


if __name__ == '__main__':
    server = WM_Central()
    server.start()
