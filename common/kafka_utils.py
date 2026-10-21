"""Utilidades comunes para producir y consumir mensajes de Kafka.

Este módulo centraliza los topics del proyecto y ofrece un adaptador simple para
serializar mensajes Python en JSON y deserializarlos al leerlos desde Kafka.
"""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

TOPIC_PETICIONES = "peticiones"   # WM_FO      -> WM_Central
TOPIC_ORDENES = "ordenes"         # WM_Central -> WM_WS_E y WM_FO
TOPIC_TELEMETRIA = "telemetria"    # WM_WS_E    -> WM_Central


class _LazyKafkaProducer:
    """Wrapper que evita conectar a Kafka hasta el primer uso real."""

    def __init__(self, broker: str, **kwargs):
        self._broker = broker
        self._kwargs = kwargs
        self._producer: KafkaProducer | None = None

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._broker,
                value_serializer=_serializar_json,
                api_version=(2, 8, 0),
                **self._kwargs,
            )
        return self._producer

    def __getattr__(self, name: str):
        return getattr(self._get_producer(), name)

    def send(self, topic: str, value: Any = None, *args, **kwargs):
        return self._get_producer().send(topic, value=value, *args, **kwargs)

    def flush(self):
        if self._producer is not None:
            self._producer.flush()

    def close(self):
        if self._producer is not None:
            self._producer.close()


class _LazyKafkaConsumer:
    """Wrapper que retrasa la conexión a Kafka hasta que se necesite consumir."""

    def __init__(self, broker: str, topic: str, group_id: str | None = None, **kwargs):
        self._broker = broker
        self._topic = topic
        self._group_id = group_id
        self._kwargs = kwargs
        self._consumer: KafkaConsumer | None = None

    def _get_consumer(self) -> KafkaConsumer:
        if self._consumer is None:
            opciones = {
                "auto_offset_reset": "earliest",
                "enable_auto_commit": True,
            }
            opciones.update(self._kwargs)
            self._consumer = KafkaConsumer(
                self._topic,
                bootstrap_servers=self._broker,
                group_id=self._group_id,
                value_deserializer=_deserializar_json,
                **opciones,
            )
        return self._consumer

    def __getattr__(self, name: str):
        return getattr(self._get_consumer(), name)

    def __iter__(self):
        return iter(self._get_consumer())

    def poll(self, *args, **kwargs):
        return self._get_consumer().poll(*args, **kwargs)

    def subscribe(self, *args, **kwargs):
        return self._get_consumer().subscribe(*args, **kwargs)

    def close(self):
        if self._consumer is not None:
            self._consumer.close()


def _serializar_json(mensaje: dict[str, Any] | None) -> bytes:
    """Serializa un diccionario a JSON para enviarlo por Kafka."""
    if mensaje is None:
        return b""
    if not isinstance(mensaje, dict):
        raise TypeError("El mensaje debe ser un dict")
    return json.dumps(mensaje, ensure_ascii=False).encode("utf-8")


def _deserializar_json(mensaje: bytes | bytearray | str | None) -> dict[str, Any]:
    """Deserializa un JSON recibido por Kafka en un diccionario."""
    if mensaje is None:
        return {}
    if isinstance(mensaje, (bytes, bytearray)):
        mensaje = bytes(mensaje).decode("utf-8")
    if isinstance(mensaje, str):
        if not mensaje.strip():
            return {}
        return json.loads(mensaje)
    return mensaje


def productor(broker: str, **kwargs) -> _LazyKafkaProducer:
    """Crea un productor Kafka que serializa dict -> JSON."""
    return _LazyKafkaProducer(broker, **kwargs)


def consumidor(broker: str, topic: str, group_id: str | None = None, **kwargs) -> _LazyKafkaConsumer:
    """Crea un consumidor Kafka que deserializa JSON -> dict.

    Cada proceso puede usar un group_id distinto para leer el mismo topic
    'ordenes' sin perder mensajes relevantes para su ws_id o fo_id.
    """
    return _LazyKafkaConsumer(broker, topic, group_id=group_id, **kwargs)


__all__ = [
    "TOPIC_PETICIONES",
    "TOPIC_ORDENES",
    "TOPIC_TELEMETRIA",
    "productor",
    "consumidor",
]

