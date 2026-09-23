"""
monitor_central.py: mitad del MONITOR que habla con CENTRAL (sockets)
Responsable: Abde · Semana 1 · Tarjeta Trello: "WM_WS_M, parte Central"
Lo usan: ws/WM_WS_M.py (programa principal) y ws/monitor_engine.py (de Vlad)

QUÉ TIENE QUE TENER
  - registrar(ip_central, puerto_central, ws_id, ubicacion) -> bool
        Conecta con WM_Central, envía  REG#<ws_id>#<ubicacion>  y espera  OK.
        Deja el socket ABIERTO: si se cierra, Central marca la WS como DESCONECTADA.
  - notificar_fuga()      -> envía  FUGA#<ws_id>
  - notificar_resuelta()  -> envía  RESUELTA#<ws_id>

CONTRATO CON VLAD: monitor_engine.py importa notificar_fuga() y notificar_resuelta()
y las llama cuando el Engine pasa a KO o vuelve a OK. Si cambias sus nombres, avisa.

Usa common/sockets_utils.py (enviar / recibir). Mensajes: docs/PROTOCOLO.md

HECHO CUANDO: llamando a notificar_fuga() a mano, Central pone la WS en FUGA
"""
