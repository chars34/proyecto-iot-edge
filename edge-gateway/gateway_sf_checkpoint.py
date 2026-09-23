"""
Gateway STORE-AND-FORWARD CON CHECKPOINTS (propuesta).
- Guarda en SQLite si la red cae.
- Deduplica por event_id (idempotencia).
- Reenvia el backlog con acks antes de purgar.
- Si un evento ya fue procesado, lo salta.
"""
import argparse
import json
import logging
import signal
import sys
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
log = logging.getLogger("gateway-sf-checkpoint")


class Stats:
    def __init__(self):
        self.received = 0
        self.sent_direct = 0
        self.queued = 0
        self.drained = 0
        self.dedup_skipped = 0
        self.failed = 0
        self.start = time.time()

    def summary(self):
        elapsed = time.time() - self.start
        return {
            "received": self.received,
            "sent_direct": self.sent_direct,
            "queued": self.queued,
            "drained": self.drained,
            "dedup_skipped": self.dedup_skipped,
            "failed": self.failed,
            "elapsed_s": round(elapsed, 2),
        }


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Conectado a Mosquitto")
    else:
        log.error(f"Fallo MQTT: {reason_code}")


def process_event(event, cloud, storage, stats, is_retry=False):
    """
    Logica unificada de envio con idempotencia.
    Devuelve True si el evento se considera manejado (enviado o ya procesado).
    """
    eid = event["event_id"]

    # 1. Deduplicacion: si ya lo procesamos, no lo reenviamos
    if storage.is_duplicate(eid):
        stats.dedup_skipped += 1
        return True

    # 2. Intento de envio al cloud
    if cloud.send(event):
        storage.mark_processed(eid)
        return True

    return False


def drain_worker(stats, cloud, storage, stop):
    while not stop["flag"]:
        n = storage.pending_count()
        if n > 0:
            batch = storage.dequeue_batch(limit=100)
            for event in batch:
                if stop["flag"]:
                    break
                if process_event(event, cloud, storage, stats, is_retry=True):
                    # solo purgamos si ya fue marcado como procesado
                    storage.ack(event["event_id"])
                    stats.drained += 1
                else:
                    break
            if n > 0:
                log.info(f"Pendientes: {storage.pending_count()}")
        time.sleep(2)


def run(args):
    cloud = CloudClient(bootstrap_servers=args.kafka, topic=args.topic)
    storage = LocalStorage(db_path=args.db, enable_dedup=True)
    stats = Stats()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"gw-sfc-{uuid.uuid4().hex[:8]}",
    )
    client.on_connect = on_connect

    def on_message(c, userdata, msg):
        stats.received += 1
        try:
            event = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            return
        if "event_id" not in event:
            event["event_id"] = str(uuid.uuid4())

        # Deduplicacion tambien en el camino directo
        if storage.is_duplicate(event["event_id"]):
            stats.dedup_skipped += 1
            return

        if cloud.send(event):
            storage.mark_processed(event["event_id"])
            stats.sent_direct += 1
        else:
            storage.enqueue(event)
            stats.queued += 1
            log.warning(f"ENCOLADO {event['event_id']} (pendientes: {storage.pending_count()})")

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

    log.info(f"Gateway SF-CHECKPOINT escuchando {args.topic_prefix}/#")
    log.info(f"DB local: {args.db} (dedup ON)")

    try:
        while not stop["flag"]:
            time.sleep(1)
    finally:
        client.loop_stop()
        client.disconnect()
        drain_thread.join(timeout=5)
        storage.close()
        cloud.close()
        log.info(f"RESUMEN: {json.dumps(stats.summary())}")


def parse_args():
    p = argparse.ArgumentParser(description="Gateway store-and-forward con checkpoints")
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--topic-prefix", default="iot/sensor")
    p.add_argument("--kafka", default="localhost:19092")
    p.add_argument("--topic", default="iot-events")
    p.add_argument("--db", default="gateway_sfc.db")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())