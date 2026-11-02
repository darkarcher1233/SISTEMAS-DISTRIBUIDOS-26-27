"""
monitor_central.py: mitad del MONITOR que habla con CENTRAL (sockets)
Responsable: Abde · Semana 1 · Tarjeta Trello: "WM_WS_M, parte Central"
Lo usan: WM_WS/WM_WS_M.py (programa principal) y WM_WS/monitor_engine.py (de Vlad)

QUÉ TIENE
  - registrar(ip_central, puerto_central, ws_id, ubicacion) -> bool
        Conecta con WM_Central, envía  REG#<ws_id>#<ubicacion>  y espera  OK.
        Deja el socket ABIERTO: si se cierra, Central marca la WS como DESCONECTADA.
  - mantener_conexion(...)  -> hilo: registra y, si se pierde la conexión con Central
        (Central parada, red caída), vuelve a registrarse cada REINTENTO segundos.
  - notificar_fuga()      -> envía  FUGA#<ws_id>
  - notificar_resuelta()  -> envía  RESUELTA#<ws_id>
  - al_registrar: función opcional que se llama tras cada registro correcto
        (WM_WS_M la usa para volver a avisar de una FUGA que siga activa)
  - estado: dict para la pantalla web del Monitor

CONTRATO CON VLAD: monitor_engine.py importa notificar_fuga() y notificar_resuelta()
y las llama cuando el Engine pasa a KO o vuelve a OK. Si cambias sus nombres, avisa.

Usa common/sockets_utils.py (enviar / recibir). Mensajes: docs/PROTOCOLO.md
"""

import socket
import threading

from common.sockets_utils import enviar, recibir

REINTENTO = 3   # segundos entre intentos de registro

_lock = threading.Lock()
_conexion_central = None
_ws_id = None
_parar = threading.Event()

al_registrar = None   # callback sin argumentos tras cada REG aceptado
avisar = print        # WM_WS_M la cambia para que los avisos salgan también en su pantalla

# Para la pantalla web: conectada = hay socket abierto y aceptado; autenticado = alguna vez OK
estado = {"conectada": False, "autenticado": False, "central": "", "ultimo_error": ""}


def registrar(ip_central, puerto, ws_id, ubicacion, silencioso=False):
  """Registra la WS y conserva abierta la conexión mientras siga activa.
  silencioso=True: si falla, no se avisa (para no repetir el mismo aviso en cada reintento)."""
  global _conexion_central, _ws_id

  estado["central"] = f"{ip_central}:{puerto}"
  conexion = None
  try:
    conexion = socket.create_connection((ip_central, puerto), timeout=5)
    enviar(conexion, f"REG#{ws_id}#{ubicacion}")
    respuesta = recibir(conexion)
    if respuesta != "OK":
      estado["ultimo_error"] = f"Central rechazó el registro: {respuesta}"
      avisar(f"Central rechazó el registro de {ws_id}: {respuesta}")
      conexion.close()
      return False
    conexion.settimeout(None)
  except (OSError, ValueError) as error:
    if conexion is not None:
      conexion.close()
    estado["ultimo_error"] = str(error)
    if not silencioso:
      avisar(f"No se pudo registrar {ws_id} en Central ({ip_central}:{puerto}): {error}")
    return False

  with _lock:
    conexion_anterior = _conexion_central
    _conexion_central = conexion
    _ws_id = ws_id
  if conexion_anterior is not None:
    conexion_anterior.close()

  estado.update(conectada=True, autenticado=True, ultimo_error="")
  avisar(f"WS {ws_id} registrada en Central ({ip_central}:{puerto}).")
  if al_registrar is not None:
    al_registrar()
  return True


def _esperar_cierre(conexion):
  """Bloquea hasta que Central cierre el socket (Central no envía nada después del OK)."""
  try:
    while recibir(conexion) is not None:
      pass
  except (OSError, ValueError):
    pass


def mantener_conexion(ip_central, puerto, ws_id, ubicacion):
  """Hilo: se registra en Central y se vuelve a registrar cada vez que se pierde la conexión."""
  global _conexion_central
  fallos = 0
  while not _parar.is_set():
    if not registrar(ip_central, puerto, ws_id, ubicacion, silencioso=fallos > 0):
      if fallos == 0:
        avisar(f"Se reintenta el registro cada {REINTENTO} s")
      fallos += 1
      _parar.wait(REINTENTO)
      continue
    fallos = 0
    with _lock:
      conexion = _conexion_central
    _esperar_cierre(conexion)
    if _parar.is_set():
      break
    with _lock:
      if _conexion_central is conexion:
        _conexion_central = None
    estado["conectada"] = False
    estado["ultimo_error"] = "Conexión con Central perdida"
    avisar(f"Conexión con Central perdida; reintentando cada {REINTENTO} s")
    _parar.wait(REINTENTO)


def _notificar(mensaje):
  global _conexion_central

  with _lock:
    if _conexion_central is None:
      avisar(f"No hay conexión con Central: '{mensaje}' se enviará al reconectar.")
      return False
    try:
      enviar(_conexion_central, mensaje)
    except OSError as error:
      avisar(f"Error enviando '{mensaje}' a Central: {error}")
      _conexion_central.close()
      _conexion_central = None
      estado["conectada"] = False
      return False

  avisar(f"Notificación enviada a Central: {mensaje}")
  return True


def notificar_fuga():
  """Notifica a Central que el Engine ha detectado una fuga o avería."""
  if _ws_id is None:
    avisar("No se puede notificar una fuga todavía: la WS no está registrada.")
    return False
  return _notificar(f"FUGA#{_ws_id}")


def notificar_resuelta():
  """Notifica a Central que la avería de la WS se ha resuelto."""
  if _ws_id is None:
    avisar("No se puede notificar la resolución todavía: la WS no está registrada.")
    return False
  return _notificar(f"RESUELTA#{_ws_id}")


def cerrar_conexion():
  """Cierra limpiamente el socket para que Central marque la WS desconectada."""
  global _conexion_central, _ws_id

  _parar.set()
  with _lock:
    conexion = _conexion_central
    _conexion_central = None
    _ws_id = None
  if conexion is not None:
    conexion.close()
