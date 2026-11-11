# nueva_ws.ps1: lanza en caliente una Watering Station más (Monitor + Engine) con Docker
#
# Uso (PowerShell, en la raíz del repo, con el .env del laboratorio):
#   .\nueva_ws.ps1 WS3 "Jardín Nuevo"
#   .\nueva_ws.ps1 WS3 "Jardín Nuevo" -SoloMostrar     # solo enseña los comandos docker
#   .\nueva_ws.ps1 WS3 -Quitar                          # para y borra sus contenedores
#
# ANTES: dar de alta la WS en Central (panel web > "Dar de alta una estación", o menú de
# Central opción 5). Si no, Central la deniega ("WS no registrada") y el Monitor lo reintenta
# cada 3 s hasta que se dé de alta.
#
# Puertos en este PC (N = número del Id: WS3 -> 3):
#   pantalla del Monitor  http://localhost:(8099+N)      WS3 -> 8102
#   pantalla del Engine   http://localhost:(8199+N)      WS3 -> 8202
# El Engine encuentra a su Monitor por el nombre del contenedor (wsN_m) en la red "wm-lab".
# Si PowerShell no deja ejecutar scripts:  powershell -ExecutionPolicy Bypass -File .\nueva_ws.ps1 WS3 "Jardín Nuevo"
param(
    [Parameter(Mandatory = $true)][string]$Id,
    [string]$Ubicacion,
    [int]$N = 0,
    [switch]$SoloMostrar,
    [switch]$Quitar
)
# Sin "Stop": en PowerShell 5.1 convertiría en excepción cualquier aviso de docker por stderr.
# Los errores se comprueban con $LASTEXITCODE y throw.
Set-Location $PSScriptRoot

$nombre = $Id.ToLower()
if ($Quitar) {
    docker rm -f "${nombre}_m" "${nombre}_e"
    Write-Host "Contenedores de $Id borrados (el volumen ${nombre}_datos se conserva)."
    exit 0
}
if (-not $Ubicacion) { throw "Falta la ubicación. Ejemplo: .\nueva_ws.ps1 WS3 `"Jardín Nuevo`"" }
if ($N -le 0) {
    if ($Id -match '(\d+)$') { $N = [int]$Matches[1] } else { throw "El Id no acaba en número: pasa -N <número> para elegir los puertos" }
}

# KAFKA_BROKER y CENTRAL del .env (los mismos que usan los docker-compose)
if (-not (Test-Path .env)) { throw "No hay .env: cópialo de .env.example y pon la IP de Central/Kafka" }
$cfg = @{}
Get-Content .env | Where-Object { $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$' } | ForEach-Object { $cfg[$Matches[1]] = $Matches[2].Trim() }
foreach ($clave in "KAFKA_BROKER", "CENTRAL") {
    if (-not $cfg[$clave] -or $cfg[$clave] -match '^:') { throw "$clave falta o está mal en el .env (debe ser IP:puerto): '$($cfg[$clave])'" }
}

$webM = 8099 + $N
$webE = 8199 + $N
$comandos = @(
    "docker network create wm-lab",
    "docker run -d --name ${nombre}_m --network wm-lab -p ${webM}:8100 --restart on-failure sd-app python -m WM_WS.WM_WS_M 6000 $($cfg.CENTRAL) $Id `"$Ubicacion`"",
    "docker run -d -it --name ${nombre}_e --network wm-lab -p ${webE}:8200 -v ${nombre}_datos:/app/datos --restart on-failure sd-app python -m WM_WS.WM_WS_E $($cfg.KAFKA_BROKER) ${nombre}_m:6000"
)
if ($SoloMostrar) { $comandos; exit 0 }

# La imagen sd-app la crean los docker-compose; si no existe, se construye
docker image inspect sd-app *> $null
if ($LASTEXITCODE -ne 0) { docker build -t sd-app . ; if ($LASTEXITCODE -ne 0) { throw "No se pudo construir la imagen" } }
docker network inspect wm-lab *> $null
if ($LASTEXITCODE -ne 0) { docker network create wm-lab | Out-Null }

docker run -d --name "${nombre}_m" --network wm-lab -p "${webM}:8100" --restart on-failure `
    sd-app python -m WM_WS.WM_WS_M 6000 $cfg.CENTRAL $Id $Ubicacion
if ($LASTEXITCODE -ne 0) { throw "No se pudo arrancar el Monitor (¿ya existe ${nombre}_m? -> .\nueva_ws.ps1 $Id -Quitar)" }
docker run -d -it --name "${nombre}_e" --network wm-lab -p "${webE}:8200" -v "${nombre}_datos:/app/datos" --restart on-failure `
    sd-app python -m WM_WS.WM_WS_E $cfg.KAFKA_BROKER "${nombre}_m:6000"
if ($LASTEXITCODE -ne 0) { throw "No se pudo arrancar el Engine" }

Write-Host ""
Write-Host "$Id en marcha:  Monitor http://localhost:$webM   Engine http://localhost:$webE"
Write-Host "Consola: docker logs -f ${nombre}_m   |   menú del Engine: docker attach ${nombre}_e (salir: Ctrl+P, Ctrl+Q)"
