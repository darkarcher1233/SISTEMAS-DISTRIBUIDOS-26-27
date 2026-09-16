import socket
import sys

HEADER = 64
FORMAT = 'utf-8'


class WM_WS_M:
	"""Cliente/Monitor que envía una trama de registro al servidor WM_Central."""
	def __init__(self, server, port, station_id, location):
		self.SERVER = server
		self.PORT = int(port)
		self.ADDR = (self.SERVER, self.PORT)
		self.station_id = station_id
		self.location = location
		self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	def send(self, msg: str):
		message = msg.encode(FORMAT)
		msg_length = len(message)
		send_length = str(msg_length).encode(FORMAT)
		send_length += b' ' * (HEADER - len(send_length))
		self.client.send(send_length)
		self.client.send(message)

	def run(self):
		self.client.connect(self.ADDR)
		print(f"Conectado a {self.ADDR}")
		trama = f"REGISTRO#{self.station_id}#{self.location}"
		print("Enviando:", trama)
		self.send(trama)

		respuesta = self.client.recv(2048).decode(FORMAT)
		print("Respuesta del servidor:", respuesta)

		print("Cerrando conexión")
		self.client.close()


if __name__ == "__main__":
	if len(sys.argv) >= 4:
		server = sys.argv[1]
		port = sys.argv[2]
		station_id = sys.argv[3]
		location = " ".join(sys.argv[4:]) if len(sys.argv) > 4 else input("Ubicación: ")
		cliente = WM_WS_M(server, port, station_id, location)
		cliente.run()
	else:
		print("Uso: python WM_WS_M.py <ServerIP> <Puerto> <ID_ESTACION> <UBICACION (opcional)>")
