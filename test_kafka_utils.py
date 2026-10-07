import json

from common.kafka_utils import (
    TOPIC_ORDENES,
    TOPIC_PETICIONES,
    TOPIC_TELEMETRIA,
    consumidor,
    productor,
)


def test_topic_constants_are_defined():
    assert TOPIC_PETICIONES == "peticiones"
    assert TOPIC_ORDENES == "ordenes"
    assert TOPIC_TELEMETRIA == "telemetria"


def test_kafka_helpers_return_lazy_objects_when_broker_is_given():
    prod = productor("localhost:9092")
    cons = consumidor("localhost:9092", TOPIC_ORDENES, group_id="test-group")

    assert prod is not None
    assert cons is not None
    assert hasattr(prod, "send")
    assert hasattr(cons, "poll")
    assert hasattr(cons, "subscribe")


def test_json_roundtrip_for_message_payload():
    payload = {"tipo": "TELEMETRIA", "ws_id": 1, "valor": 22.5}
    raw = json.dumps(payload).encode("utf-8")
    restored = json.loads(raw.decode("utf-8"))
    assert restored == payload
