# Protocolo de mensajes: WaterManagement (Release 1)

> Documento **a completar por los dos** (Semana 1, tarjeta "Definir mensajes de SOCKETS + reparto de WM_WS_M").
> Lo de abajo es una **propuesta**: cambiad lo que queráis, pero que el código y este fichero digan lo mismo.

## 1. Sockets (Semana 1)

Protocolo de clase: cabecera de longitud (`HEADER = 64` bytes, UTF-8) + mensaje con campos separados por `#`.
Implementación: `common/sockets_utils.py`.

### Central ⇄ Monitor (Abde)

| Sentido | Mensaje | Respuesta | Significado |
|---|---|---|---|
| Monitor → Central | `REG#<ws_id>#<ubicacion>` | `OK` / `ERROR#<motivo>` | Alta o autenticación de la WS → DISPONIBLE |
| Monitor → Central | `FUGA#<ws_id>` | – | El Engine no responde o responde KO → FUGA |
| Monitor → Central | `RESUELTA#<ws_id>` | – | El Engine vuelve a OK → DISPONIBLE |
| (socket cerrado) | – | – | Central marca la WS como DESCONECTADA |

### Monitor ⇄ Engine (Vlad)

| Sentido | Mensaje | Respuesta | Significado |
|---|---|---|---|
| Monitor → Engine | `ID#<ws_id>#<ubicacion>` | – | Nada más conectarse: el Engine recibe su ID (la ubicación solo es para su pantalla; un Engine antiguo lee solo el ID) |
| Monitor → Engine | `HC` (cada 1 s) | `OK` / `KO` | Health check. Timeout de 2 s = KO |

### Funciones compartidas dentro de WM_WS_M

| Función | Fichero | Quién la escribe | Quién la llama |
|---|---|---|---|
| `registrar(ip, puerto, ws_id, ubicacion)` | `ws/monitor_central.py` | Abde | `WM_WS_M.py` |
| `notificar_fuga()` | `ws/monitor_central.py` | Abde | `monitor_engine.py` |
| `notificar_resuelta()` | `ws/monitor_central.py` | Abde | `monitor_engine.py` |
| `iniciar_servidor_engine(puerto, ws_id, ubicacion)` | `ws/monitor_engine.py` | Vlad | `WM_WS_M.py` |
| `mantener_conexion(ip, puerto, ws_id, ubicacion)` | `ws/monitor_central.py` | Abde | `WM_WS_M.py` (se re-registra si Central se cae) |

## 2. Kafka (Semana 2: completar entonces)

Mensajes en JSON; todos llevan `"tipo"`. Implementación: `common/kafka_utils.py`.

| Topic | Quién escribe → quién lee | Tipos de mensaje |
|---|---|---|
| `peticiones` | FO → Central | `PETICION` |
| `ordenes` | Central → WS y FO | `REGAR`, `BLOQUEAR`, `ACTIVAR`, `AUTORIZADO`, `DENEGADO`, `TELEMETRIA`, `RESUMEN`, `ESTADO_WS` |
| `telemetria` | WS → Central | `TELEMETRIA`, `FIN` |

Cada WS y cada FO lee todo `ordenes` y se queda solo con lo que lleva su `ws_id` / `fo_id`.

### Ejemplos de JSON (semana 2)

Topic `ordenes` (Central -> WS):

```json
{"tipo": "REGAR",    "ws_id": "WS1", "duracion": 30, "operario": "OP1"}
{"tipo": "BLOQUEAR", "ws_id": "WS1"}
{"tipo": "ACTIVAR",  "ws_id": "WS1"}
```

Topic `telemetria` (WS -> Central):

```json
{"tipo": "TELEMETRIA", "ws_id": "WS1", "caudal": 18.42, "volumen": 0.307, "operario": "OP1"}
{"tipo": "FIN", "ws_id": "WS1", "volumen": 9.21, "duracion": 30.0, "motivo": "TIEMPO"}
```

Topic `peticiones` (FO -> Central):

```json
{"tipo": "PETICION", "fo_id": "FO1", "ws_id": "WS1", "duracion": 10}
```

Topic `ordenes` (Central -> FO). Todos llevan `fo_id` (el FO ignora los de otro operario),
salvo `ESTADO_WS`, que es para todos:

```json
{"tipo": "AUTORIZADO", "fo_id": "FO1", "ws_id": "WS1", "duracion": 10}
{"tipo": "DENEGADO",   "fo_id": "FO1", "ws_id": "WS1", "motivo": "WS en estado FUGA"}
{"tipo": "TELEMETRIA", "fo_id": "FO1", "ws_id": "WS1", "caudal": 18.42, "volumen": 0.307}
{"tipo": "RESUMEN",    "fo_id": "FO1", "ws_id": "WS1", "volumen": 9.21, "duracion": 30.0, "motivo": "TIEMPO"}
{"tipo": "ESTADO_WS",  "estaciones": [{"ws_id": "WS1", "ubicacion": "Parque Norte", "estado": "DISPONIBLE"}]}
```

- `RESUMEN.motivo` = el `motivo` del FIN del Engine; con `FUGA` o `BLOQUEO` el FO muestra un aviso grande.
- `DENEGADO` y `RESUMEN` cierran la petición: en modo fichero el FO pasa a la siguiente (tras 4 s).
- `ESTADO_WS.estaciones` también se acepta como el dict de Central `{ws_id: {"ubicacion", "estado", ...}}`.
- Prueba sin Central real: `tests/central_falsa_fo.py`.
- Los riegos ordenados desde el menú o el panel de Central van con `operario` / `fo_id` = `CENTRAL` (no se valida en la BD).

`motivo` del FIN:

| Valor      | Cuándo lo pone el Engine |
|------------|--------------------------|
| `TIEMPO`   | Se agotó la duración solicitada |
| `MANUAL`   | El operario pulsó `s` en el menú del Engine |
| `FUGA`     | Se activó `averia = True` durante el riego |
| `BLOQUEO`  | Llegó una orden `BLOQUEAR` mientras regaba |

## 3. Flujo de un riego (resumen)

1. FO → `peticiones`: PETICION (fo_id, ws_id, duracion)
2. Central comprueba que el operario existe y que la WS está DISPONIBLE
3. Si no → `ordenes`: DENEGADO (fo_id, motivo). Fin.
4. Si sí → `ordenes`: REGAR (ws_id, duracion, operario) + AUTORIZADO (fo_id) → estado REGANDO
5. WS → `telemetria`: TELEMETRIA cada 1 s → Central la muestra y la reenvía al FO
6. WS → `telemetria`: FIN (tiempo · parada manual · fuga · bloqueo) → Central → FO: RESUMEN → DISPONIBLE
