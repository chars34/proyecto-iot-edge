# Bloquea el puerto 19092 (Redpanda) durante N segundos.
param(
    [int]$DurationSeconds = 30
)

Write-Host "Bloqueando puerto 19092 (Redpanda) por $DurationSeconds segundos..."

# Regla de firewall de salida para bloquear el trafico
New-NetFirewallRule `
    -DisplayName "BlockRedpandaTemp" `
    -Direction Outbound `
    -RemotePort 19092 `
    -Protocol TCP `
    -Action Block `
    -Profile Any | Out-Null

Write-Host "Puerto bloqueado. Esperando..."
Start-Sleep -Seconds $DurationSeconds

Remove-NetFirewallRule -DisplayName "BlockRedpandaTemp"
Write-Host "Puerto desbloqueado. Red restaurada."