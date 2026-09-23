"""
Cliente Kafka para Redpanda. Envuelve al productor confluent-kafka
con manejo de errores y timeout.
"""
import json
import logging
from confluent_kafka import Producer, KafkaException

log = logging.getLogger(__name__)


class CloudClient:
    def __init__(self, bootstrap_servers: str = "localhost:19092",
                 topic: str = "iot-events",
                 timeout: float = 2.0):
        self.topic = topic
        self.timeout = timeout
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "edge-gateway",
            "message.timeout.ms": int(timeout * 1000),
            "socket.timeout.ms": int(timeout * 1000),
            "request.timeout.ms": int(timeout * 1000),
            "enable.idempotence": False,
        })

    def send(self, event: dict) -> bool:
        """
        Envia un evento a Redpanda.
        Devuelve True si se encolo correctamente, False si fallo.
        """
        try:
            payload = json.dumps(event).encode("utf-8")
            self.producer.produce(
                self.topic,
                key=event["event_id"].encode("utf-8"),
                value=payload,
            )
            self.producer.poll(0)
            remaining = self.producer.flush(self.timeout)
            if remaining > 0:
                log.warning(f"Timeout enviando {event['event_id']} (quedan {remaining})")
                return False
            return True
        except KafkaException as exc:
            log.error(f"Kafka error: {exc}")
            return False
        except Exception as exc:
            log.error(f"Error inesperado: {exc}")
            return False

    def close(self):
        self.producer.flush(5)