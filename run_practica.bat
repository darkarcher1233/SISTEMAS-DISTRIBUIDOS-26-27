@echo off
setlocal
cd /d "%~dp0"

REM Solicita permisos de administrador si hace falta
>nul 2>&1 "%SYSTEMROOT%\system32\cacls.exe" "%SYSTEMROOT%\system32\config\system"
if "%errorlevel%" NEQ "0" (
    echo Solicitando permisos de Administrador...
    powershell -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)
    
cls
color 0B

echo =====================================================
echo  Arrancando entorno completo de la practica
echo =====================================================
echo.
echo Este archivo se ha abierto con permisos de Administrador.
echo.

REM Kafka local
start "Kafka" cmd /k "docker compose up"

REM Espera a que Kafka acepte conexiones antes de arrancar la aplicacion
call :wait_listener 9092 Kafka
if errorlevel 1 goto :error

REM Central
start "Central" cmd /k "python -m WM_Central.WM_Central 5050 127.0.0.1:9092"
call :wait_listener 5050 Central
if errorlevel 1 goto :error

REM Cada Monitor debe registrarse y abrir su puerto antes de iniciar su Engine
start "WS1 Monitor" cmd /k python -m WM_WS.WM_WS_M 8001 127.0.0.1:5050 WS1 "Parque Norte"
call :wait_listener 8001 "Monitor WS1"
if errorlevel 1 goto :error
start "WS1 Engine" cmd /k "python -m WM_WS.WM_WS_E 127.0.0.1:9092 127.0.0.1:8001"

start "WS2 Monitor" cmd /k python -m WM_WS.WM_WS_M 8002 127.0.0.1:5050 WS2 "Parque Sur"
call :wait_listener 8002 "Monitor WS2"
if errorlevel 1 goto :error
start "WS2 Engine" cmd /k "python -m WM_WS.WM_WS_E 127.0.0.1:9092 127.0.0.1:8002"

REM Los FO arrancan cuando Kafka y Central ya estan disponibles
start "FO1" cmd /k "python -m WM_FO.WM_FO 127.0.0.1:9092 FO1"
start "FO2" cmd /k "python -m WM_FO.WM_FO 127.0.0.1:9092 FO2"

echo.
echo Todas las terminales se han abierto.
echo.
echo Acciones disponibles:
echo   - Kafka: docker compose up
echo   - Central: python -m WM_Central.WM_Central 5050 127.0.0.1:9092
echo   - WS1: python -m WM_WS.WM_WS_M 8001 127.0.0.1:5050 WS1 "Parque Norte"
echo   - WS2: python -m WM_WS.WM_WS_M 8002 127.0.0.1:5050 WS2 "Parque Sur"
echo   - FO1/FO2: python -m WM_FO.WM_FO 127.0.0.1:9092 FO1 / FO2
echo.

pause
exit /b 0

:wait_listener
echo Esperando a %~2 en el puerto %~1...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(120); do { if (Get-NetTCPConnection -State Listen -LocalPort %~1 -ErrorAction SilentlyContinue) { exit 0 }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1" >nul 2>&1
if errorlevel 1 exit /b 1
echo %~2 esta disponible.
exit /b 0

:error
echo.
echo ERROR: No se pudo iniciar el entorno. Comprueba Docker y los puertos 9092 y 5050.
pause
exit /b 1
