"""
kafka_utils.py: funciones comunes para producir y consumir mensajes de KAFKA
Responsable: Vlad · Semana 2 · Tarjeta Trello: "Kafka en local (docker-compose) + kafka_utils + topics"
Lo usan: WM_Central, WM_WS_E, WM_FO   (librería: kafka-python)

QUÉ TIENE QUE TENER
  - Nombres de los topics como constantes (nadie escribe el string a mano):
        TOPIC_PETICIONES = "peticiones"   # WM_FO      -> WM_Central
        TOPIC_ORDENES    = "ordenes"      # WM_Central -> WM_WS_E y WM_FO
        TOPIC_TELEMETRIA = "telemetria"   # WM_WS_E    -> WM_Central
  - productor(broker: str)  -> KafkaProducer que serializa dict -> JSON
  - consumidor(broker: str, topic: str, group_id: str | None = None) -> KafkaConsumer que
        deserializa JSON -> dict
        Ojo: cada WS y cada FO lee TODOS los mensajes de 'ordenes' y se queda solo con
        los que llevan su ws_id / fo_id. Por eso cada proceso usa un group_id distinto (o ninguno).
  - Todos los mensajes llevan un campo "tipo" (REGAR, BLOQUEAR, TELEMETRIA, FIN...).
    Lista completa: docs/PROTOCOLO.md

HECHO CUANDO: un script de prueba envía un dict y otro lo recibe (primero en local
y después contra el Kafka de Railway)
"""
