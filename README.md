# proyecto-iot-edge

Arquitectura distribuida tolerante a fallos para IoT rural.

## Descripción

Gateway edge con mecanismo **Store-and-Forward** para escenarios con conectividad intermitente.
Compara tres arquitecturas: centralizado puro, store-and-forward simple y pipeline con checkpoints.

## Stack

- Mosquitto (MQTT broker en el edge)
- Redpanda (streaming en el cloud)
- TimescaleDB (persistencia de series temporales)
- Docker Compose (orquestación)

## Estructura


## Licencia

MIT
