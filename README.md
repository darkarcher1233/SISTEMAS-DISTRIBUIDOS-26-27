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

## Plan

| Semana | Qué |
|---|---|
| 1 (hasta 4 oct) | Solo sockets: Central ⇄ Monitor (Abde) · Monitor ⇄ Engine (Vlad) |
| 2 (hasta 11 oct) | Kafka local y en Railway, riego y telemetría, SQLite |
| 3 (hasta 18 oct) | FO, flujo completo de activación, menú de Central, panel |
| 4 (hasta 25 oct) | Docker, despliegue en Railway, pruebas, memoria |
