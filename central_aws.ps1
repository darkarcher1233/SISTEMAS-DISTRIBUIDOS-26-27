# central_aws.ps1: maneja la parte cloud (Kafka + WM_Central en la EC2) desde TU PC, sin abrir
# la consola de AWS ni escribir comandos en la EC2. Por debajo usa ssh (viene con Windows 10/11).
#
# Uso (PowerShell, en la raíz del repo):
#   .\central_aws.ps1                 # = deploy: git pull + docker compose up -d --build en la EC2
#   .\central_aws.ps1 estado          # docker compose ps
#   .\central_aws.ps1 logs            # logs de Central en directo (Ctrl+C para salir)
#   .\central_aws.ps1 menu            # menú de Central (docker attach). Salir SIN pararla: Ctrl+P, Ctrl+Q
#   .\central_aws.ps1 parar           # docker compose down (los datos se quedan en los volúmenes)
#   .\central_aws.ps1 ssh             # abrir una terminal normal en la EC2
#   .\central_aws.ps1 -Ip 1.2.3.4     # cambiar la IP guardada (Learner Lab: cambia en cada arranque)
#
# La primera vez pregunta IP, usuario y ruta del .pem y lo guarda en deploy\ec2.local (no se sube).
# Si PowerShell no deja ejecutar scripts:  powershell -ExecutionPolicy Bypass -File .\central_aws.ps1
param(
    [ValidateSet('deploy', 'estado', 'logs', 'menu', 'parar', 'ssh')]
    [string]$Accion = 'deploy',
    [string]$Ip,
    [string]$Usuario,
    [string]$Clave,
    [string]$Carpeta
)
Set-Location $PSScriptRoot
$ficheroCfg = Join-Path $PSScriptRoot 'deploy\ec2.local'

# ---- Configuración guardada (deploy\ec2.local, formato CLAVE=valor) ----
$cfg = @{ EC2_USER = 'ubuntu'; EC2_DIR = '~/SISTEMAS-DISTRIBUIDOS-26-27' }
if (Test-Path $ficheroCfg) {
    Get-Content $ficheroCfg | Where-Object { $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$' } |
        ForEach-Object { $cfg[$Matches[1]] = $Matches[2].Trim() }
}
if ($Ip)      { $cfg.EC2_HOST = $Ip }
if ($Usuario) { $cfg.EC2_USER = $Usuario }
if ($Clave)   { $cfg.EC2_KEY  = $Clave }
if ($Carpeta) { $cfg.EC2_DIR  = $Carpeta }
if (-not $cfg.EC2_HOST) { $cfg.EC2_HOST = Read-Host 'IP pública de la EC2' }
if (-not $cfg.EC2_KEY)  { $cfg.EC2_KEY  = Read-Host 'Ruta del .pem (p.ej. C:\Users\yo\Downloads\labsuser.pem)' }
$cfg.EC2_KEY = $cfg.EC2_KEY.Trim('"')
if (-not (Test-Path $cfg.EC2_KEY)) { throw "No existe la clave: $($cfg.EC2_KEY)" }

New-Item -ItemType Directory -Force (Split-Path $ficheroCfg) | Out-Null
@("EC2_HOST=$($cfg.EC2_HOST)", "EC2_USER=$($cfg.EC2_USER)", "EC2_KEY=$($cfg.EC2_KEY)", "EC2_DIR=$($cfg.EC2_DIR)") |
    Set-Content -Encoding ascii $ficheroCfg

# ---- Permisos del .pem: el ssh de Windows rechaza claves que pueden leer otros usuarios ----
icacls $cfg.EC2_KEY /inheritance:r /grant:r "$($env:USERNAME):(R)" | Out-Null

$destino = "$($cfg.EC2_USER)@$($cfg.EC2_HOST)"
$opts = @('-i', $cfg.EC2_KEY, '-o', 'StrictHostKeyChecking=accept-new', '-o', 'ConnectTimeout=10')
$compose = 'docker compose -f docker-compose.cloud.yml'
$cd = "cd $($cfg.EC2_DIR)"   # sin comillas: así ~ se expande en la EC2

switch ($Accion) {
    'deploy' { ssh @opts $destino "$cd && bash deploy/deploy_ec2.sh" }
    'estado' { ssh @opts $destino "$cd && $compose ps" }
    'logs'   { ssh -t @opts $destino "$cd && $compose logs -f --tail 100 wm_central" }
    'menu'   { ssh -t @opts $destino 'docker attach wm_central' }
    'parar'  { ssh @opts $destino "$cd && $compose down" }
    'ssh'    { ssh @opts $destino }
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nssh falló (código $LASTEXITCODE). Revisa: lab arrancado, IP actual (-Ip ...), puerto 22 abierto en el security group." -ForegroundColor Red
    exit $LASTEXITCODE
}
