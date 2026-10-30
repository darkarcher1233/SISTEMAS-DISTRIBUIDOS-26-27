# WaterManagement: Sistemas Distribuidos 26/27

Práctica no guiada (Release 1): red de estaciones de riego con **sockets**, **Kafka**, **SQLite**,
**Docker** y despliegue en **Railway**.
Grupo: Vlad + Abde · Tareas y fechas: tablero de Trello **SD**.

## Estructura

```
central/                    CORE SYSTEM (en Railway)
  WM_Central.py             Central: sockets, estados, flujo de riego, menú      (Abde)
  db.py                     Acceso a SQLite                                      (Abde)
  panel.py                  Panel de monitorización en consola                   (Vlad)
ws/                         WATERING STATION (laboratorio, PC 2)
  WM_WS_M.py                Monitor: programa principal                          (Abde)
  monitor_central.py        Monitor ⇄ Central (sockets)                          (Abde)
  monitor_engine.py         Monitor ⇄ Engine (sockets + health check)            (Vlad)
  WM_WS_E.py                Engine: health check, riego, telemetría              (Vlad)
fo/                         FIELD OPERATOR (laboratorio, PC 1)
  WM_FO.py                  Aplicación del operario                              (Vlad)
  peticiones/               Ficheros de peticiones para las pruebas
common/                     Código compartido
  sockets_utils.py          enviar / recibir por sockets                         (Vlad)
  kafka_utils.py            productor / consumidor de Kafka + nombres de topics  (Vlad)
docs/
  PROTOCOLO.md              Mensajes de sockets y Kafka  <- LEER PRIMERO
  capturas/                 Capturas para la memoria
  enunciado/                PDF y tutoriales (no se sube a GitHub)
prompts_ia/                 Prompts de IA usados (obligatorio)
ejemplos/                   Ejemplos de sockets de clase (no se entregan)
Dockerfile                  Imagen única para todos los módulos
docker-compose.yml          Kafka en local (desarrollo)
docker-compose.ws.yml       Laboratorio PC 2: 2 WS
docker-compose.fo.yml       Laboratorio PC 1: 2 FO
```

## Cómo ejecutar (desde la raíz del repo)

```
python -m central.WM_Central <puerto> <ip_broker:puerto>
python -m ws.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicacion]
python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>
python -m fo.WM_FO <ip_broker:puerto> <id_operario>
```

Se usa `python -m ...` desde la raíz para que los imports de `common/` funcionen.

## Laboratorio con Docker

Una sola imagen (`sd-app`, del `Dockerfile`) para todos los módulos; cada contenedor solo cambia
el comando de arranque.

| Fichero | Para qué |
|---|---|
| `Dockerfile` | Imagen común: Python 3.12 + `requirements.txt` + el código |
| `docker-compose.yml` | Solo **desarrollo local**: Kafka en `localhost:9092`. No sirve para los contenedores del lab (anuncia `localhost`, que dentro de un contenedor es el propio contenedor) |
| `docker-compose.ws.yml` | Laboratorio **PC 2**: `ws1_m`, `ws1_e`, `ws2_m`, `ws2_e` |
| `docker-compose.fo.yml` | Laboratorio **PC 1**: `fo1`, `fo2` |
| `.env.example` → `.env` | `KAFKA_BROKER` (Kafka en AWS) y `CENTRAL` (host:puerto de WM_Central). El `.env` no se sube |

### Preparación (una vez por PC)

```
cp .env.example .env        # y revisar KAFKA_BROKER y CENTRAL
```

`CENTRAL` es la dirección de Central **vista desde el contenedor**: `host.docker.internal:5050`
si Central corre en el mismo PC, `<IP_del_PC>:5050` si está en otro PC del laboratorio, o la IP
pública de AWS cuando se despliegue allí.

### PC 2: Watering Stations

```
python -m central.WM_Central 5050 51.20.108.192:9092           # antes, donde esté Central
docker compose -f docker-compose.ws.yml up -d --build          # construir y arrancar
docker compose -f docker-compose.ws.yml ps                     # estado de los contenedores
docker compose -f docker-compose.ws.yml logs -f ws1_m          # salida del Monitor WS1
docker attach ws1_e                                            # menú del Engine WS1
docker compose -f docker-compose.ws.yml down                   # parar y borrar todo
```

### PC 1: Field Operators

```
docker compose -f docker-compose.fo.yml up -d --build
docker compose -f docker-compose.fo.yml ps
docker compose -f docker-compose.fo.yml logs -f fo1
docker attach fo1
docker compose -f docker-compose.fo.yml down
```

### Menús (`docker attach`)

- `docker attach <contenedor>` conecta el teclado al `input()` del programa. Puede que no se vea
  el prompt: pulsar Enter y sale la ayuda del menú.
- Para **salir sin parar** el contenedor: **Ctrl+P y luego Ctrl+Q**.
- **Ctrl+C** dentro de `attach` mata el programa (y el contenedor).

### Errores típicos

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `ps` muestra `Restarting` en un Monitor | No llega a Central (no arrancada, IP/puerto mal en `CENTRAL`, firewall de Windows) | `logs ws1_m`; arrancar Central; revisar `CENTRAL` en `.env`; permitir Python en el firewall |
| `ps` muestra `Restarting` en un Engine | Su Monitor aún no escucha (normal unos segundos) o el Monitor falla | Esperar; si sigue, mirar `logs` del Monitor |
| El Monitor arranca pero la WS **no aparece en Central** | `CENTRAL` apunta a otro sitio (p. ej. `localhost`) | Usar `host.docker.internal:5050` o la IP real del PC de Central; luego `up -d` de nuevo |
| El Engine no conecta a Kafka / no llegan órdenes | `KAFKA_BROKER` mal, o Kafka anuncia `localhost` | Revisar `.env`; el broker de AWS debe anunciar su IP pública |
| `logs` vacíos | Imagen antigua sin `PYTHONUNBUFFERED=1`, o el programa espera en `input()` | `up -d --build`; hacer `attach` y pulsar Enter |
| El Engine se reinició y el Monitor no lo vuelve a aceptar | El Monitor solo acepta un Engine por ejecución | `docker compose -f docker-compose.ws.yml restart ws1_m ws1_e` |
| `fo1`/`fo2` en `Exited (0)` | `WM_FO` aún no tiene menú (termina al instante) | Normal hasta terminar WM_FO |

## Plan

| Semana | Qué |
|---|---|
| 1 (hasta 4 oct) | Solo sockets: Central ⇄ Monitor (Abde) · Monitor ⇄ Engine (Vlad) |
| 2 (hasta 11 oct) | Kafka local y en Railway, riego y telemetría, SQLite |
| 3 (hasta 18 oct) | FO, flujo completo de activación, menú de Central, panel |
| 4 (hasta 25 oct) | Docker, despliegue en Railway, pruebas, memoria |
