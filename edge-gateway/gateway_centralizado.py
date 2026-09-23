"""
Gateway CENTRALIZADO (baseline).
Reenvía cada evento directo a Redpanda. Si la red cae, el evento se pierde.
NO usa almacenamiento local ni idempotencia.
"""
import argparse
import json
import logging
import signal
import sys
import time
import uuid

import paho.mqtt.client as mqtt

from cloud_client import CloudClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("gateway-centralizado")


# ---------- Estadisticas ----------
class Stats:
    def __init__(self):
        self.received = 0
        self.sent = 0
        self.failed = 0
        self.start = time.time()

    def summary(self):
        elapsed = time.time() - self.start
        return {
            "received": self.received,
            "sent": self.sent,
            "failed": self.failed,
            "lost": self.received - self.sent,
            "elapsed_s": round(elapsed, 2),
            "rate_msg_s": round(self.received / elapsed, 2) if elapsed > 0 else 0,
        }


# ---------- MQTT callbacks ----------
def make_on_message(stats, cloud):
    def on_message(client, userdata, msg):
        stats.received += 1
        try:
            event = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            log.warning(f"Payload invalido en {msg.topic}")
            return

        if "event_id" not in event:
            event["event_id"] = str(uuid.uuid4())

        ok = cloud.send(event)
        if ok:
            stats.sent += 1
        else:
            stats.failed += 1
            # AQUI esta la diferencia: el evento se pierde
            log.warning(f"PERDIDO {event['event_id']}")
    return on_message


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Conectado a Mosquitto")
    else:
        log.error(f"Fallo de conexion MQTT: {reason_code}")


def run(args):
    cloud = CloudClient(bootstrap_servers=args.kafka, topic=args.topic)
    stats = Stats()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"gw-central-{uuid.uuid4().hex[:8]}",
    )
    client.on_connect = on_connect
    client.on_message = make_on_message(stats, cloud)

    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.subscribe(args.topic_prefix + "/#", qos=0)
    client.loop_start()

    stop = {"flag": False}

    def handle_sigint(signum, frame):
        stop["flag"] = True
        log.info("Deteniendo gateway...")

    signal.signal(signal.SIGINT, handle_sigint)

    log.info(f"Gateway CENTRALIZADO escuchando {args.topic_prefix}/#")
    log.info(f"Publicando a Redpanda en {args.kafka} (topic {args.topic})")

    try:
        while not stop["flag"]:
            time.sleep(1)
    finally:
        client.loop_stop()
        client.disconnect()
        cloud.close()
        log.info(f"RESUMEN: {json.dumps(stats.summary())}")


def parse_args():
    p = argparse.ArgumentParser(description="Gateway centralizado (baseline)")
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--topic-prefix", default="iot/sensor")
    p.add_argument("--kafka", default="localhost:19092")
    p.add_argument("--topic", default="iot-events")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())