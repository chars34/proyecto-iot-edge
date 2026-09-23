#!/usr/bin/env bash
set -euo pipefail

echo "==> 1. Verificando contenedores..."
docker compose ps

echo "==> 2. Verificando Mosquitto (MQTT)..."
docker exec iot-mosquitto mosquitto_sub -h localhost -t '$SYS/broker/version' -C 1 -W 5

echo "==> 3. Verificando Redpanda (Kafka API)..."
docker exec iot-redpanda rpk cluster health

echo "==> 4. Verificando TimescaleDB..."
docker exec iot-timescaledb psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\dx'

echo "==> 5. Publicando mensaje MQTT de prueba..."
docker exec iot-mosquitto mosquitto_pub -h localhost -t 'test/smoke' -m '{"hello":"world"}'

echo "==> 6. Suscribiendose y capturando el mensaje..."
docker exec iot-mosquitto mosquitto_sub -h localhost -t 'test/smoke' -C 1 -W 5

echo "Smoke test completado."
