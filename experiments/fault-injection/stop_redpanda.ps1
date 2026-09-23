# Detiene el contenedor de Redpanda durante N segundos.
# Simula una caida de red al cloud.
param(
    [int]$DurationSeconds = 30
)

Write-Host "Deteniendo Redpanda por $DurationSeconds segundos..."
docker stop iot-redpanda | Out-Null

Write-Host "Redpanda detenido. Esperando..."
Start-Sleep -Seconds $DurationSeconds

Write-Host "Reiniciando Redpanda..."
docker start iot-redpanda | Out-Null
Start-Sleep -Seconds 10  # dar tiempo a que arranque

Write-Host "Redpanda reiniciado."