"""
Simulador IoT para el proyecto proyecto-iot-edge.

Publica datos aleatorios de humedad, temperatura y estado del suelo
al broker MQTT (Mosquitto) en el topic iot/sensor/<id>.
"""
import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


# ---------- Configuracion ----------
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 1883
DEFAULT_TOPIC_PREFIX = "iot/sensor"
DEFAULT_RATE = 1.0          # segundos entre mensajes por sensor
DEFAULT_SENSORS = 10
DEFAULT_DURATION = 0        # 0 = infinito


# ---------- Generacion de datos ----------
def generate_reading(sensor_id: str) -> dict:
    """Genera una lectura sintetica para un sensor."""
    return {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "humidity": round(random.uniform(20.0, 80.0), 2),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "soil_state": random.choice(["dry", "moist", "wet", "saturated"]),
    }


# ---------- Callbacks MQTT ----------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"[OK] Conectado al broker {userdata['host']}:{userdata['port']}")
    else:
        print(f"[ERROR] Fallo de conexion: {reason_code}", file=sys.stderr)


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    userdata["published"] = userdata.get("published", 0) + 1


# ---------- Bucle principal ----------
def run(args):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"iot-sim-{uuid.uuid4().hex[:8]}",
    )
    client.user_data_set({"host": args.host, "port": args.port, "published": 0})
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        client.connect(args.host, args.port, keepalive=60)
    except Exception as exc:
        print(f"[FATAL] No se pudo conectar: {exc}", file=sys.stderr)
        sys.exit(1)

    client.loop_start()

    sensor_ids = [f"s{i:04d}" for i in range(1, args.sensors + 1)]
    print(f"[INFO] Sensores: {args.sensors}, tasa: {args.rate}s, duracion: {args.duration or 'infinito'}s")
    print(f"[INFO] Topico: {args.topic_prefix}/<sensor_id>")
    print("[INFO] Ctrl+C para detener.\n")

    # Manejo limpio de Ctrl+C
    stop = {"flag": False}

    def handle_sigint(signum, frame):
        stop["flag"] = True
        print("\n[INFO] Deteniendo simulador...")

    signal.signal(signal.SIGINT, handle_sigint)

    start = time.time()
    count = 0

    try:
        while not stop["flag"]:
            for sid in sensor_ids:
                if stop["flag"]:
                    break
                reading = generate_reading(sid)
                topic = f"{args.topic_prefix}/{sid}"
                payload = json.dumps(reading)
                client.publish(topic, payload, qos=0)
                count += 1

                if count % 50 == 0:
                    elapsed = time.time() - start
                    print(f"[STATS] Publicados {count} mensajes en {elapsed:.1f}s ({count/elapsed:.1f} msg/s)")

                time.sleep(args.rate)

            if args.duration and (time.time() - start) >= args.duration:
                print(f"\n[INFO] Duracion alcanzada ({args.duration}s). Deteniendo.")
                break

    finally:
        client.loop_stop()
        client.disconnect()
        elapsed = time.time() - start
        print(f"\n[RESUMEN] Total: {count} mensajes en {elapsed:.1f}s")
        if elapsed > 0:
            print(f"[RESUMEN] Tasa efectiva: {count/elapsed:.1f} msg/s")


# ---------- CLI ----------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulador IoT para Mosquitto",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host del broker MQTT")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Puerto del broker MQTT")
    parser.add_argument("--sensors", type=int, default=DEFAULT_SENSORS, help="Numero de sensores")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Segundos entre lecturas por sensor")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION, help="Duracion en segundos (0 = infinito)")
    parser.add_argument("--topic-prefix", default=DEFAULT_TOPIC_PREFIX, help="Prefijo del topico MQTT")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())