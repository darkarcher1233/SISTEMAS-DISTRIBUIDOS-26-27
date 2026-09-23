"""
monitor_engine.py: mitad del MONITOR que habla con el ENGINE (sockets + health check)
Responsable: Vlad · Semana 1 · Tarjeta Trello: "WM_WS_M, parte Engine"
Lo usa: ws/WM_WS_M.py (lo lanza en un hilo)

QUÉ TIENE QUE TENER
  - iniciar_servidor_engine(puerto, ws_id) -> None
        1. Abre un socket servidor en <puerto> y espera a que se conecte WM_WS_E.
        2. Nada más conectarse, le envía  ID#<ws_id>  (el Engine recibe su ID del Monitor,
           tal como pide el enunciado).
        3. Bucle cada 1 s: envía  HC  y espera la respuesta con sock.settimeout(2)
             - Timeout, socket cerrado o respuesta KO  -> notificar_fuga()
             - Vuelve a responder OK                     -> notificar_resuelta()
           IMPORTANTE: avisar solo al CAMBIAR de estado (OK->KO o KO->OK), no cada segundo.
        4. Imprimir por pantalla:  Engine: OK  /  Engine: KO

DEPENDENCIA: from ws.monitor_central import notificar_fuga, notificar_resuelta   (de Abde)
Mientras no existan, probar con funciones falsas que solo hagan print("FUGA").

Usa common/sockets_utils.py. Mensajes: docs/PROTOCOLO.md

HECHO CUANDO: con el Engine en marcha, 'f' provoca un FUGA (una sola vez) y 'r' un RESUELTA
"""
