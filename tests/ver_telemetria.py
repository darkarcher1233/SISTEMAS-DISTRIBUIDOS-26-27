"""Pequeño consumidor manual del topic 'telemetria' para pruebas del Engine.

No empieza por 'test_' aposta: pytest no debe recogerlo (bloquearía esperando a Kafka).

USO:
    python -m tests.ver_telemetria              # localhost:9092 por defecto
    python -m tests.ver_telemetria broker:9092
"""

from __future__ import annotations

import sys

from common.kafka_utils import TOPIC_TELEMETRIA, consumidor


def main():
    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    print(f"escuchando '{TOPIC_TELEMETRIA}' en {broker} (Ctrl+C para salir)")
    cons = consumidor(
        broker,
        TOPIC_TELEMETRIA,
        group_id=None,
        auto_offset_reset="latest",
    )
    for mensaje in cons:
        print(mensaje.value)


if __name__ == "__main__":
    main()
