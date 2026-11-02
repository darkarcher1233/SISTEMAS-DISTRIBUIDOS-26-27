# nuevo_fo.ps1: lanza en caliente un operario (WM_FO) más con Docker
#
# Uso (PowerShell, en la raíz del repo, con el .env del laboratorio):
#   .\nuevo_fo.ps1 FO3
#   .\nuevo_fo.ps1 FO3 -SoloMostrar      # solo enseña el comando docker
#   .\nuevo_fo.ps1 FO3 -Quitar           # para y borra su contenedor
#
# El operario tiene que existir en Central (FO1, FO2 y FO3 vienen de serie; otros: panel web >
# "Operarios dados de alta"). Si no, Central deniega sus riegos con "Operario no válido".
# Pantalla: http://localhost:(8299+N)  (FO3 -> 8302).  Menú: docker attach fo3 (salir: Ctrl+P, Ctrl+Q)
# Si PowerShell no deja ejecutar scripts:  powershell -ExecutionPolicy Bypass -File .\nuevo_fo.ps1 FO3
param(
    [Parameter(Mandatory = $true)][string]$Id,
    [int]$N = 0,
    [switch]$SoloMostrar,
    [switch]$Quitar
)
# Sin "Stop": en PowerShell 5.1 convertiría en excepción cualquier aviso de docker por stderr.
# Los errores se comprueban con $LASTEXITCODE y throw.
Set-Location $PSScriptRoot

$nombre = $Id.ToLower()
if ($Quitar) { docker rm -f $nombre; Write-Host "Contenedor $nombre borrado."; exit 0 }
if ($N -le 0) {
    if ($Id -match '(\d+)$') { $N = [int]$Matches[1] } else { throw "El Id no acaba en número: pasa -N <número> para elegir el puerto" }
}

if (-not (Test-Path .env)) { throw "No hay .env: cópialo de .env.example y pon la IP de Kafka" }
$cfg = @{}
Get-Content .env | Where-Object { $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$' } | ForEach-Object { $cfg[$Matches[1]] = $Matches[2].Trim() }
if (-not $cfg.KAFKA_BROKER -or $cfg.KAFKA_BROKER -match '^:') { throw "KAFKA_BROKER falta o está mal en el .env (debe ser IP:9092): '$($cfg.KAFKA_BROKER)'" }

$web = 8299 + $N
$comando = "docker run -d -it --name $nombre -p ${web}:8300 --restart on-failure sd-app python -m WM_FO.WM_FO $($cfg.KAFKA_BROKER) $Id"
if ($SoloMostrar) { $comando; exit 0 }

docker image inspect sd-app *> $null
if ($LASTEXITCODE -ne 0) { docker build -t sd-app . ; if ($LASTEXITCODE -ne 0) { throw "No se pudo construir la imagen" } }

docker run -d -it --name $nombre -p "${web}:8300" --restart on-failure sd-app python -m WM_FO.WM_FO $cfg.KAFKA_BROKER $Id
if ($LASTEXITCODE -ne 0) { throw "No se pudo arrancar (¿ya existe $nombre? -> .\nuevo_fo.ps1 $Id -Quitar)" }

Write-Host ""
Write-Host "$Id en marcha:  pantalla http://localhost:$web   menú: docker attach $nombre (salir: Ctrl+P, Ctrl+Q)"
