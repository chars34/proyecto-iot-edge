"""
Gateway STORE-AND-FORWARD SIMPLE.
Si Redpanda falla, guarda el evento en SQLite local.
Al reconectar, drena el backlog en segundo plano.
NO usa deduplicacion: puede duplicar si un ack se pierde.
"""
import argparse
import json
import logging
import signal
import threading
import time
import uuid

import paho.mqtt.client as mqtt

from cloud_client import CloudClient
from storage import LocalStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("gateway-sf-simple")


class Stats:
    def __init__(self):
        self.received = 0
        self.sent_direct = 0
        self.queued = 0
        self.drained = 0
        self.failed = 0
        self.start = time.time()

    def summary(self):
        elapsed = time.time() - self.start
        return {
            "received": self.received,
            "sent_direct": self.sent_direct,
            "queued": self.queued,
            "drained": self.drained,
            "failed": self.failed,
            "pending_at_end": None,  # se rellena al cerrar
            "elapsed_s": round(elapsed, 2),
            "rate_msg_s": round(self.received / elapsed, 2) if elapsed > 0 else 0,
        }


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Conectado a Mosquitto")
    else:
        log.error(f"Fallo de conexion MQTT: {reason_code}")


def drain_worker(stats, cloud, storage, stop):
    """Hilo que drena el backlog cada 2 segundos."""
    while not stop["flag"]:
        n = storage.pending_count()
        if n > 0:
            batch = storage.dequeue_batch(limit=100)
            for event in batch:
                if stop["flag"]:
                    break
                if cloud.send(event):
                    storage.ack(event["event_id"])
                    stats.drained += 1
                else:
                    # No se pudo enviar, paramos hasta el proximo intento
                    break
        time.sleep(2)


def run(args):
    cloud = CloudClient(bootstrap_servers=args.kafka, topic=args.topic)
    storage = LocalStorage(db_path=args.db, enable_dedup=False)
    stats = Stats()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"gw-sfs-{uuid.uuid4().hex[:8]}",
    )
    client.on_connect = on_connect

    def on_message(c, userdata, msg):
        stats.received += 1
        try:
            event = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            log.warning(f"Payload invalido en {msg.topic}")
            return

        if "event_id" not in event:
            event["event_id"] = str(uuid.uuid4())

        if cloud.send(event):
            stats.sent_direct += 1
        else:
            storage.enqueue(event)
            stats.queued += 1
            log.warning(
                f"ENCOLADO {event['event_id']} (pendientes: {storage.pending_count()})"
            )

    client.on_message = on_message
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.subscribe(args.topic_prefix + "/#", qos=0)
    client.loop_start()

    stop = {"flag": False}

    def handle_sigint(signum, frame):
        stop["flag"] = True
        log.info("Deteniendo gateway...")

    signal.signal(signal.SIGINT, handle_sigint)

    drain_thread = threading.Thread(
        target=drain_worker, args=(stats, cloud, storage, stop), daemon=True
    )
    drain_thread.start()

    log.info(f"Gateway SF-SIMPLE escuchando {args.topic_prefix}/#")
    log.info(f"DB local: {args.db} (dedup OFF)")

    try:
        while not stop["flag"]:
            time.sleep(1)
    finally:
        client.loop_stop()
        client.disconnect()
        drain_thread.join(timeout=5)
        summary = stats.summary()
        summary["pending_at_end"] = storage.pending_count()
        storage.close()
        cloud.close()
        log.info(f"RESUMEN: {json.dumps(summary)}")


def parse_args():
    p = argparse.ArgumentParser(description="Gateway store-and-forward simple")
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--topic-prefix", default="iot/sensor")
    p.add_argument("--kafka", default="localhost:19092")
    p.add_argument("--topic", default="iot-events")
    p.add_argument("--db", default="gateway_sfs.db")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())

if __name__ == "__main__":
    run(parse_args())