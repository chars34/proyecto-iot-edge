# Orquesta una prueba de fallo completa:
# 1. Arranca el simulador en background
# 2. Espera 10 segundos
# 3. Detiene Redpanda por 30 segundos
# 4. Espera a que el simulador termine
param(
    [int]$Sensors = 5,
    [double]$Rate = 0.2,
    [int]$Duration = 60,
    [int]$BlockAfter = 10,
    [int]$BlockDuration = 30
)

Write-Host "=== Prueba de fallo ===" -ForegroundColor Cyan
Write-Host "Simulador: $Sensors sensores, $Rate s/msg, ${Duration}s"
Write-Host "Bloqueo: a los ${BlockAfter}s por ${BlockDuration}s"
Write-Host ""

# 1. Arrancar simulador en background
$simPath = "$HOME\Documents\proyecto-iot-edge\iot-simulator"
Write-Host "Arrancando simulador..." -ForegroundColor Yellow
$sim = Start-Process -FilePath "python" `
    -ArgumentList "simulator.py","--sensors","$Sensors","--rate","$Rate","--duration","$Duration" `
    -WorkingDirectory $simPath -PassThru -NoNewWindow

# 2. Esperar antes del bloqueo
Write-Host "Esperando ${BlockAfter}s antes del bloqueo..." -ForegroundColor Yellow
Start-Sleep -Seconds $BlockAfter

# 3. Bloquear Redpanda
Write-Host "Deteniendo Redpanda..." -ForegroundColor Red
docker stop iot-redpanda | Out-Null
Write-Host "Redpanda detenido. Esperando ${BlockDuration}s..." -ForegroundColor Red
Start-Sleep -Seconds $BlockDuration

# 4. Restaurar Redpanda
Write-Host "Reiniciando Redpanda..." -ForegroundColor Green
docker start iot-redpanda | Out-Null
Start-Sleep -Seconds 10

Write-Host "Redpanda reiniciado. Esperando que el simulador termine..." -ForegroundColor Green

# 5. Esperar al simulador
if (-not $sim.HasExited) {
    $sim.WaitForExit()
}

Write-Host "=== Prueba terminada ===" -ForegroundColor Cyan