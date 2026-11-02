# WaterManagement: Sistemas Distribuidos 26/27

Práctica no guiada (Release 1): red de estaciones de riego con **sockets**, **Kafka**, **SQLite**,
**Docker** y despliegue en **Railway**.
Grupo: Vlad + Abde · Tareas y fechas: tablero de Trello **SD**.

## Estructura

```
WM_Central/                 CORE SYSTEM (en la nube: AWS)
  WM_Central.py             Central: sockets, estados, flujo de riego, menú      (Abde)
  db.py                     Acceso a SQLite                                      (Abde)
  panel.py                  Panel web de Central (http://...:8080)               (Vlad)
WM_WS/                      WATERING STATION (laboratorio, PC 2)
  WM_WS_M.py                Monitor: programa principal                          (Abde)
  monitor_central.py        Monitor ⇄ Central (sockets)                          (Abde)
  monitor_engine.py         Monitor ⇄ Engine (sockets + health check)            (Vlad)
  WM_WS_E.py                Engine: health check, riego, telemetría              (Vlad)
WM_FO/                      FIELD OPERATOR (laboratorio, PC 1)
  WM_FO.py                  Aplicación del operario                              (Vlad)
  peticiones/               Ficheros de peticiones para las pruebas
ui/                         Páginas web de todos los módulos (HTML + comun.css/js)
lanzador.py                 Lanzador web: arranca y vigila los módulos de un PC
common/                     Código compartido
  sockets_utils.py          enviar / recibir por sockets                         (Vlad)
  kafka_utils.py            productor / consumidor de Kafka + nombres de topics  (Vlad)
  web.py                    mini servidor web común de las pantallas
docs/
  PROTOCOLO.md              Mensajes de sockets y Kafka  <- LEER PRIMERO
  capturas/                 Capturas para la memoria
  CHECKLIST_CORRECCION.md   Cada punto de la guía de corrección y su estado
  enunciado/                PDF y tutoriales (no se sube a GitHub)
prompts/                    Prompts de IA usados (obligatorio)
Nombres.txt                 Nombres de los integrantes del grupo
ejemplos/                   Ejemplos de sockets de clase (no se entregan)
Dockerfile                  Imagen única para todos los módulos
docker-compose.yml          Kafka en local (desarrollo)
docker-compose.ws.yml       Laboratorio PC 2: 2 WS
docker-compose.fo.yml       Laboratorio PC 1: 2 FO
```

## Cómo ejecutar (desde la raíz del repo)

```
python -m WM_Central.WM_Central <puerto> <ip_broker:puerto>
python -m WM_WS.WM_WS_M <puerto_engine> <ip_central:puerto> <id_ws> [ubicacion]
python -m WM_WS.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>
python -m WM_FO.WM_FO <ip_broker:puerto> <id_operario>
```

Se usa `python -m ...` desde la raíz para que los imports de `common/` funcionen.

## Interfaz web (todas las pantallas)

Cada módulo levanta su propia página web (solo librería estándar: `common/web.py` + `ui/*.html`).
Los menús de teclado siguen funcionando igual; sin teclado (lanzador, Docker sin `-it`) se usa la web.

| Módulo | Página | Puerto por defecto | Qué se hace desde ahí |
|---|---|---|---|
| WM_Central | `ui/panel.html` | **8080** (`PANEL_PORT`) | Ver todas las WS por parque, regar / bloquear / activar una o todas, alta de operarios |
| WM_WS_M | `ui/monitor.html` | 8100, 8101… | Salud del Engine (HC) y conexión con Central |
| WM_WS_E | `ui/estacion.html` | 8200, 8201… | Caudal y volumen en vivo, pedir riego a Central, parar, simular fuga |
| WM_FO | `ui/operario.html` | 8300, 8301… | Ver WS, pedir riego, subir un fichero de peticiones, ver telemetría y resumen |

- Puerto concreto: variable de entorno `WEB_PORT`. Si no se da, cada proceso coge el primer puerto
  libre desde el de la tabla (la URL sale por consola).
- Probar el panel sin Central ni Kafka: `python -m tests.panel_demo`.
- **AWS**: abrir en el *Security Group* el **8080** (panel) igual que el 5050 de los Monitor.

| Color | Estado | Qué significa |
|---|---|---|
| Verde | `DISPONIBLE` | Conectada y lista para regar |
| Verde **parpadeando** | `REGANDO` | Regando: caudal (L/min), volumen (L), operario y tiempo |
| Rojo | `FUGA` | El Monitor ha avisado de una fuga (el Engine responde KO o no responde) |
| Naranja | `FUERA_SERVICIO` | Bloqueada desde Central |
| Gris | `DESCONECTADA` | Se ha cerrado el socket de su Monitor (o aún no se ha conectado) |

### Lanzador (todo un PC desde el navegador)

```
python lanzador.py          # y abrir http://localhost:8000
```

Arranca, para y reinicia como procesos hijos CENTRAL, cada WS (Monitor + Engine) y cada FO de este
PC, y enseña la pantalla y la consola de cada uno. Se pueden añadir estaciones y operarios, parar
solo el Engine o solo el Monitor de una WS (para probar las caídas), poner CENTRAL en modo
**remota** (los Monitor se conectan a otra máquina) y arrancar el Kafka local con Docker si no
responde. La lista y los ajustes se guardan en `lanzador.json` (no se sube).

Puertos: CENTRAL 5050 (sockets) y 8080 (panel) · WS n: Engine↔Monitor 6000+n, webs 8100+n y
8200+n · FO n: web 8300+n.

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
python -m WM_Central.WM_Central 5050 51.20.108.192:9092           # antes, donde esté Central
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
| El Engine dice «Monitor no disponible; reintentando» | Su Monitor aún no escucha (normal unos segundos) o el Monitor falla | Esperar; si sigue, mirar `logs` del Monitor |
| El Monitor arranca pero la WS **no aparece en Central** | `CENTRAL` apunta a otro sitio (p. ej. `localhost`) | Usar `host.docker.internal:5050` o la IP real del PC de Central; luego `up -d` de nuevo |
| El Engine no conecta a Kafka / no llegan órdenes | `KAFKA_BROKER` mal, o Kafka anuncia `localhost` | Revisar `.env`; el broker de AWS debe anunciar su IP pública |
| `logs` vacíos | Imagen antigua sin `PYTHONUNBUFFERED=1`, o el programa espera en `input()` | `up -d --build`; hacer `attach` y pulsar Enter |
| El Engine se reinició y la WS sigue en FUGA | El Monitor espera a que el Engine vuelva a conectarse (reintenta cada 3 s) y entonces avisa RESUELTA | Esperar unos segundos; si no, `logs` del Engine |
| `fo1`/`fo2` en `Restarting` | El FO no llega a Kafka (`ERROR Kafka 'ordenes'` en `logs`) | Revisar `KAFKA_BROKER` en `.env` |
| El FO envía la PETICION pero no recibe nada | Central aún no procesa `peticiones` o no está arrancada | Mirar la consola de Central; para probar solo el FO: `tests/central_falsa_fo.py` |

## Plan

| Semana | Qué |
|---|---|
| 1 (hasta 4 oct) | Solo sockets: Central ⇄ Monitor (Abde) · Monitor ⇄ Engine (Vlad) |
| 2 (hasta 11 oct) | Kafka local y en Railway, riego y telemetría, SQLite |
| 3 (hasta 18 oct) | FO, flujo completo de activación, menú de Central, panel |
| 4 (hasta 25 oct) | Docker, despliegue en Railway, pruebas, memoria |
