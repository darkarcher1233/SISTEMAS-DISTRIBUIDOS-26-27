"""Comunicación persistente entre WM_WS_M y WM_Central mediante sockets."""

import socket
import threading

from common.sockets_utils import enviar, recibir


_lock = threading.Lock()
_conexion_central = None
_ws_id = None


def registrar(ip_central, puerto, ws_id, ubicacion):
  """Registra la WS y conserva abierta la conexión mientras siga activa."""
  global _conexion_central, _ws_id

  conexion = None
  try:
    conexion = socket.create_connection((ip_central, puerto), timeout=5)
    enviar(conexion, f"REG#{ws_id}#{ubicacion}")
    respuesta = recibir(conexion)
    if respuesta != "OK":
      print(f"Central rechazó el registro de {ws_id}: {respuesta}")
      conexion.close()
      return False
    conexion.settimeout(None)
  except (OSError, ValueError) as error:
    if conexion is not None:
      conexion.close()
    print(f"No se pudo registrar {ws_id} en Central: {error}")
    return False

  with _lock:
    conexion_anterior = _conexion_central
    _conexion_central = conexion
    _ws_id = ws_id
  if conexion_anterior is not None:
    conexion_anterior.close()

  print(f"WS {ws_id} registrada en Central ({ip_central}:{puerto}).")
  return True


def _notificar(mensaje):
  global _conexion_central, _ws_id

  with _lock:
    if _conexion_central is None:
      print("No hay una conexión activa con Central.")
      return False
    try:
      enviar(_conexion_central, mensaje)
    except OSError as error:
      print(f"Error enviando '{mensaje}' a Central: {error}")
      _conexion_central.close()
      _conexion_central = None
      _ws_id = None
      return False

  print(f"Notificación enviada a Central: {mensaje}")
  return True


def notificar_fuga():
  """Notifica a Central que el Engine ha detectado una fuga o avería."""
  if _ws_id is None:
    print("No se puede notificar una fuga: la WS no está registrada.")
    return False
  return _notificar(f"FUGA#{_ws_id}")


def notificar_resuelta():
  """Notifica a Central que la avería de la WS se ha resuelto."""
  if _ws_id is None:
    print("No se puede notificar la resolución: la WS no está registrada.")
    return False
  return _notificar(f"RESUELTA#{_ws_id}")


def cerrar_conexion():
  """Cierra limpiamente el socket para que Central marque la WS desconectada."""
  global _conexion_central, _ws_id

  with _lock:
    conexion = _conexion_central
    _conexion_central = None
    _ws_id = None
  if conexion is not None:
    conexion.close()
