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
  """Se autentica en Central (REG) y conserva abierta la conexión mientras siga activa.
  Si falla deja el motivo en estado["ultimo_error"] y devuelve False.
  silencioso=True: no avisa del fallo (lo hace mantener_conexion, solo cuando cambia)."""
  global _conexion_central, _ws_id

  estado["central"] = f"{ip_central}:{puerto}"
  conexion = None
  try:
    conexion = socket.create_connection((ip_central, puerto), timeout=5)
    enviar(conexion, f"REG#{ws_id}#{ubicacion}")
    respuesta = recibir(conexion)
    if respuesta != "OK":
      motivo = (respuesta or "sin respuesta").split("#", 1)[-1]   # "ERROR#WS no registrada" -> motivo
      estado["ultimo_error"] = f"WM_Central ha denegado el registro de {ws_id}: {motivo}"
      conexion.close()
      if not silencioso:
        avisar(estado["ultimo_error"])
      return False
    conexion.settimeout(None)
  except (OSError, ValueError):
    if conexion is not None:
      conexion.close()
    estado["ultimo_error"] = f"Imposible conectar con WM_Central ({ip_central}:{puerto})"
    if not silencioso:
      avisar(estado["ultimo_error"])
    return False

  with _lock:
    conexion_anterior = _conexion_central
    _conexion_central = conexion
    _ws_id = ws_id
  if conexion_anterior is not None:
    conexion_anterior.close()

  estado.update(conectada=True, autenticado=True, ultimo_error="")
  avisar(f"WS {ws_id} autenticada en WM_Central ({ip_central}:{puerto}).")
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
  """Hilo: se autentica en Central y lo vuelve a intentar cada REINTENTO segundos si no puede
  o si se pierde la conexión. Cada aviso sale una sola vez (hasta que cambie el motivo)."""
  global _conexion_central
  ultimo_aviso = ""
  while not _parar.is_set():
    if not registrar(ip_central, puerto, ws_id, ubicacion, silencioso=True):
      if estado["ultimo_error"] != ultimo_aviso:
        ultimo_aviso = estado["ultimo_error"]
        avisar(f"{ultimo_aviso}. Se reintenta cada {REINTENTO} s")
      _parar.wait(REINTENTO)
      continue
    ultimo_aviso = ""
    with _lock:
      conexion = _conexion_central
    _esperar_cierre(conexion)
    if _parar.is_set():
      break
    with _lock:
      if _conexion_central is conexion:
        _conexion_central = None
    estado["conectada"] = False
    estado["ultimo_error"] = "Conexión con WM_Central perdida"
    avisar(f"Conexión con WM_Central perdida. Se reintenta cada {REINTENTO} s")
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
