# WaterManagement · Sistemas Distribuidos 26/27 · Release 1

Sistema distribuido que gestiona una red de **estaciones de riego** (Watering Stations, WS) de los
parques y jardines de una ciudad. Una **Central** registra y supervisa las estaciones y autoriza los
riegos que piden los **operarios de campo** (FO); cada estación simula el riego y manda su
telemetría (caudal y volumen) cada segundo.

Grupo: ver `Nombres.txt` · Tareas: tablero de Trello «SD».

## Arquitectura

```
                 ┌────────────── Cloud (AWS EC2) ──────────────┐
                 │  WM_Central ── SQLite (volumen)   Kafka      │
                 │   :5050 sockets  :8080 panel      :9092      │
                 └─────▲───────────────────────────────▲────────┘
          sockets      │ REG / VIVO / FUGA / RESUELTA  │ Kafka: peticiones · ordenes · telemetria
     ┌─────────────────┴──────────┐             ┌──────┴──────────────┐
     │ PC2: WM_WS (n estaciones)  │             │ PC1: WM_FO (n)      │
     │  WM_WS_M ── sockets (HC) ── WM_WS_E ─────┤  (solo Kafka)       │
     └────────────────────────────┘   Kafka     └─────────────────────┘
```

| Módulo | Programa | Habla con | Para qué |
|---|---|---|---|
| **WM_Central** | `python -m WM_Central.WM_Central <puerto> <broker>` | Monitor (sockets), todos (Kafka) | Registra y autentica WS, autoriza riegos, panel, menú de órdenes, SQLite |
| **WM_WS_M** (Monitor) | `python -m WM_WS.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicación]` | Central y Engine (sockets) | Se autentica en Central, health check del Engine cada 1 s, avisa de fugas |
| **WM_WS_E** (Engine) | `python -m WM_WS.WM_WS_E <broker> <ip_monitor:puerto>` | Monitor (sockets), Kafka | Riega: caudal, volumen, electroválvula; menú de la estación |
| **WM_FO** | `python -m WM_FO.WM_FO <broker> <id_operario>` | Kafka | Pide riegos (a mano o desde fichero) y ve el resultado |

- **Sockets** donde hace falta respuesta inmediata o detectar caídas (autenticación, salud).
- **Kafka** donde varios productores y consumidores intercambian eventos sin conocerse.
- Protocolo completo (mensajes y JSON): [`docs/PROTOCOLO.md`](docs/PROTOCOLO.md).

## Despliegue

**El paso a paso del día de la corrección está en [`docs/GUIA_DESPLIEGUE.md`](docs/GUIA_DESPLIEGUE.md).**
Resumen:

| Dónde | Fichero | Comando |
|---|---|---|
| EC2 (cloud) | `docker-compose.cloud.yml` | `bash deploy/deploy_ec2.sh` (preparación de AWS: [`docs/DESPLIEGUE_AWS.md`](docs/DESPLIEGUE_AWS.md)) |
| PC2 (WS) | `docker-compose.ws.yml` | `docker compose -f docker-compose.ws.yml up -d --build` |
| PC1 (FO) | `docker-compose.fo.yml` | `docker compose -f docker-compose.fo.yml up -d --build` |
| Más WS / FO | `nueva_ws.ps1`, `nuevo_fo.ps1` | `.\nueva_ws.ps1 WS3 "Jardín Nuevo"` · `.\nuevo_fo.ps1 FO3` |

En los PCs del laboratorio, `.env` (copia de `.env.example`) con la IP de la EC2:
`KAFKA_BROKER=<IP_EC2>:9092` y `CENTRAL=<IP_EC2>:5050`.

Una sola imagen Docker (`sd-app`, del `Dockerfile`) para todos los módulos; cada contenedor solo
cambia su comando. **CI/CD** GitHub → AWS: `.github/workflows/deploy.yml` ejecuta
`deploy/deploy_ec2.sh` por SSH en cada push a `main`.

## Parámetros (nada fijo en el código)

| Qué | Cómo |
|---|---|
| Puertos, IPs, broker, Id de WS/operario | Argumentos de cada programa (tabla de arriba) |
| Puerto del panel de Central | `PANEL_PORT` (por defecto 8080) |
| Puerto de la pantalla web de Monitor / Engine / FO | `WEB_PORT` (si no, el primero libre desde 8100 / 8200 / 8300) |
| Fichero SQLite de Central | `WATER_DB` (por defecto `water.db`; en la nube `/data/water.db` en un volumen) |
| Carpeta del riego en curso del Engine | `DATOS_DIR` (por defecto `datos`; en Docker un volumen) |
| IP pública y puertos de la nube | `.env` de la EC2: `PUBLIC_IP`, `KAFKA_PORT`, `CENTRAL_PORT`, `PANEL_PORT` |

## Pantallas

Cada módulo tiene su consola **y** su pantalla web (solo librería estándar: `common/web.py` + `ui/`).

| Módulo | Pantalla | Qué se hace |
|---|---|---|
| Central | `http://<IP_EC2>:8080` | WS por parque con su estado; regar / bloquear / activar una o todas; alta de WS y de operarios; registro de eventos |
| Monitor | `:8100`, `:8101`… | Salud del Engine y conexión con Central |
| Engine | `:8200`, `:8201`… | Caudal y volumen en vivo; pedir riego a Central; parar; simular fuga |
| FO | `:8300`, `:8301`… | WS disponibles; pedir riego; fichero de peticiones; telemetría y resumen |

| Color | Estado | Significado |
|---|---|---|
| Verde | `DISPONIBLE` (Available) | Conectada y lista para regar |
| Verde **parpadeando** | `REGANDO` (Watering) | Caudal (L/min), volumen (L) e Id del operario |
| Rojo | `FUGA` (Leak) | El Monitor avisó de una avería (Engine KO o caído) |
| Naranja | `FUERA_SERVICIO` («Out of Service») | Bloqueada por Central |
| Gris | `DESCONECTADA` (Disconnected) | Central no recibe mensajes de vida de su Monitor |

## Funcionamiento

1. Central arranca, lee SQLite y muestra las WS ya registradas como **Desconectadas**.
2. Cada Monitor se **autentica** (`REG#id#ubicación`): Central lo acepta si la WS está dada de alta
   (WS1 y WS2 vienen de serie; otras: panel o menú opción 5) y lo deniega si no
   («WS no registrada») o si ya está conectada desde otro Monitor.
3. Un riego se pide desde el **FO**, desde el **menú de la WS** (`m` en el Engine) o desde
   **Central**. Central comprueba operario y disponibilidad → `AUTORIZADO` + `REGAR` o `DENEGADO`.
4. El Engine riega y manda `TELEMETRIA` cada segundo; Central la muestra y la reenvía solo al FO
   de ese riego. Al acabar (tiempo, parada, fuga, bloqueo) manda `FIN` → Central → `RESUMEN` al FO.
5. Health check cada segundo Monitor → Engine. KO o sin respuesta → `FUGA` (rojo); si estaba
   regando, el riego acaba al instante y se informa a todos.

## Resiliencia

| Cae… | Qué pasa | Detalle |
|---|---|---|
| Monitor | WS Desconectada; el riego acaba; la WS no admite riegos («Averiada») | Latido `VIVO` cada 2 s, 6 s sin él → Desconectada |
| Engine | WS en Fuga; Central avisa al FO; lo regado se guarda en disco y se envía al volver | `datos/riego_<ws>.json` |
| FO | El riego sigue; al volver ve su resultado | Grupo Kafka `fo_<id>` con commit |
| Central | Las WS terminan su riego; no se admiten riegos nuevos; «Imposible conectar con WM_Central» | Al volver procesa lo pendiente (commit tras procesar) |

Pruebas paso a paso: [`docs/PRUEBAS_RESILIENCIA.md`](docs/PRUEBAS_RESILIENCIA.md).

## Estructura

```
WM_Central/                 CORE SYSTEM (en la nube: AWS)
  WM_Central.py             Central: sockets, estados, riegos, menú, resiliencia      (Abde + Vlad)
  db.py                     SQLite: estaciones, operarios, riegos                     (Abde)
  panel.py                  Panel web de Central                                      (Vlad)
WM_WS/                      WATERING STATION (laboratorio, PC 2)
  WM_WS_M.py                Monitor: programa principal                               (Abde)
  monitor_central.py        Monitor ⇄ Central (sockets, latido)                        (Abde)
  monitor_engine.py         Monitor ⇄ Engine (sockets + health check)                  (Vlad)
  WM_WS_E.py                Engine: riego, telemetría, menú, riego en disco           (Vlad)
WM_FO/                      FIELD OPERATOR (laboratorio, PC 1)
  WM_FO.py                  Aplicación del operario                                   (Vlad)
  peticiones/               Ficheros de peticiones (modo fichero)
common/                     sockets_utils.py · kafka_utils.py · web.py
ui/                         Pantallas web (HTML + comun.css / comun.js)
prompts/                    Prompts de IA usados (obligatorio)
docs/                       PROTOCOLO · GUIA_DESPLIEGUE · DESPLIEGUE_AWS · PRUEBAS_RESILIENCIA ·
                            CHECKLIST_CORRECCION
tests/                      Tests (pytest) y herramientas de prueba
deploy/deploy_ec2.sh        Despliegue en la EC2
docker-compose.cloud.yml    Cloud: Kafka + WM_Central + SQLite
docker-compose.ws.yml       PC 2: WS1 y WS2       ·  nueva_ws.ps1  para más
docker-compose.fo.yml       PC 1: FO1 y FO2       ·  nuevo_fo.ps1  para más
docker-compose.yml          Solo desarrollo: Kafka en localhost:9092
lanzador.py                 Solo desarrollo: arranca todo un PC desde el navegador (:8000)
Readme.md · Nombres.txt
```

## Desarrollo en local (sin Docker para los módulos)

```powershell
pip install -r requirements.txt
docker compose up -d                                   # Kafka de desarrollo en localhost:9092
python -m WM_Central.WM_Central 5050 localhost:9092
python -m WM_WS.WM_WS_M 6000 127.0.0.1:5050 WS1 "Parque Norte"
python -m WM_WS.WM_WS_E localhost:9092 127.0.0.1:6000
python -m WM_FO.WM_FO localhost:9092 FO1
```

O todo desde el navegador: `python lanzador.py` → `http://localhost:8000`.

Tests: `python -m pytest` (30 tests, sin Kafka: usan productores falsos y una BD temporal).
Panel sin Central ni Kafka: `python -m tests.panel_demo`.
