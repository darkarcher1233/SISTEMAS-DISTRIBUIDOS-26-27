# Protocolo de mensajes: WaterManagement (Release 1)

> Documento **a completar por los dos** (Semana 1, tarjeta "Definir mensajes de SOCKETS + reparto de WM_WS_M").
> Lo de abajo es una **propuesta**: cambiad lo que queráis, pero que el código y este fichero digan lo mismo.

## 1. Sockets (Semana 1)

Protocolo de clase: cabecera de longitud (`HEADER = 64` bytes, UTF-8) + mensaje con campos separados por `#`.
Implementación: `common/sockets_utils.py`.

### Central ⇄ Monitor (Abde)

| Sentido | Mensaje | Respuesta | Significado |
|---|---|---|---|
| Monitor → Central | `REG#<ws_id>#<ubicacion>` | `OK` / `ERROR#<motivo>` | Autenticación de la WS → DISPONIBLE. Motivos de `ERROR`: `WS no registrada` (no está dada de alta en Central), `WS ya conectada desde otro Monitor`, formato incorrecto. El Monitor reintenta cada 3 s |
| Monitor → Central | `FUGA#<ws_id>` | – | El Engine no responde o responde KO → FUGA |
| Monitor → Central | `RESUELTA#<ws_id>` | – | El Engine vuelve a OK → DISPONIBLE |
| Monitor → Central | `VIVO#<ws_id>` (cada 2 s) | – | Mensaje de vida |
| (socket cerrado o 6 s sin ningún mensaje) | – | – | Central marca la WS como DESCONECTADA |

### Monitor ⇄ Engine (Vlad)

| Sentido | Mensaje | Respuesta | Significado |
|---|---|---|---|
| Monitor → Engine | `ID#<ws_id>#<ubicacion>` | – | Nada más conectarse: el Engine recibe su ID (la ubicación solo es para su pantalla; un Engine antiguo lee solo el ID) |
| Monitor → Engine | `HC` (cada 1 s) | `OK` / `KO` | Health check. Timeout de 2 s = KO. Si el Engine pasa 3 s sin recibir HC, da el Monitor por perdido: corta el riego (FIN `MONITOR`) y no admite riegos |

### Funciones compartidas dentro de WM_WS_M

| Función | Fichero | Quién la escribe | Quién la llama |
|---|---|---|---|
| `registrar(ip, puerto, ws_id, ubicacion)` | `WM_WS/monitor_central.py` | Abde | `WM_WS_M.py` |
| `notificar_fuga()` | `WM_WS/monitor_central.py` | Abde | `monitor_engine.py` |
| `notificar_resuelta()` | `WM_WS/monitor_central.py` | Abde | `monitor_engine.py` |
| `iniciar_servidor_engine(puerto, ws_id, ubicacion)` | `WM_WS/monitor_engine.py` | Vlad | `WM_WS_M.py` |
| `mantener_conexion(ip, puerto, ws_id, ubicacion)` | `WM_WS/monitor_central.py` | Abde | `WM_WS_M.py` (se re-registra si Central se cae) |

## 2. Kafka (Semana 2: completar entonces)

Mensajes en JSON; todos llevan `"tipo"`. Implementación: `common/kafka_utils.py`.

| Topic | Quién escribe → quién lee | Tipos de mensaje |
|---|---|---|
| `peticiones` | FO → Central | `PETICION` |
| `ordenes` | Central → WS y FO | `REGAR`, `BLOQUEAR`, `ACTIVAR`, `AUTORIZADO`, `DENEGADO`, `TELEMETRIA`, `RESUMEN`, `ESTADO_WS` |
| `telemetria` | WS → Central | `TELEMETRIA`, `FIN` |

Cada WS y cada FO lee todo `ordenes` y se queda solo con lo que lleva su `ws_id` / `fo_id`.
El Engine también enseña los `AUTORIZADO` / `DENEGADO` de su `ws_id` (así se ve en la estación la
respuesta a un riego pedido desde su menú `m` o su pantalla, que envían una `PETICION` como un FO).

**Alta de WS**: solo se autentican las WS dadas de alta en Central (menú opción 5 o panel web,
`POST /api/estaciones`). WS1 y WS2 (escenario mínimo) vienen dadas de alta en una BD nueva.

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
{"tipo": "FIN", "ws_id": "WS1", "operario": "FO1", "volumen": 9.21, "duracion": 30.0, "motivo": "TIEMPO"}
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
{"tipo": "AVISO",      "fo_id": "FO1", "ws_id": "WS1", "motivo": "Avería en la estación: el riego se ha interrumpido"}
{"tipo": "ESTADO_WS",  "estaciones": [{"ws_id": "WS1", "ubicacion": "Parque Norte", "estado": "DISPONIBLE"}]}
```

- `RESUMEN.motivo` = el `motivo` del FIN del Engine; con `FUGA` o `BLOQUEO` el FO muestra un aviso grande.
- `DENEGADO` y `RESUMEN` cierran la petición: en modo fichero el FO pasa a la siguiente (tras 4 s).
- `AVISO`: Central avisa al FO de que la WS de su riego ha pasado a FUGA o DESCONECTADA. El RESUMEN
  con el volumen real llega cuando la WS manda su FIN (aunque sea al recuperarse).
- `FIN.operario`: el Engine lo incluye para que Central sepa a qué FO mandar el RESUMEN aunque se
  haya reiniciado entre medias.
- Central caída: FO y Engine lo detectan porque dejan de llegar `ESTADO_WS` (6 s). Una `PETICION`
  con más de 30 s cuando Central la lee se contesta `DENEGADO` «Petición caducada».
- Grupos de consumidores: Central `central_peticiones` / `central_telemetria`, Engine `ws_<id>`,
  FO `fo_<id>`. Central y FO confirman (commit) cada mensaje después de procesarlo.
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
| `MONITOR`  | El Engine perdió su Monitor mientras regaba (la WS queda averiada) |
| `AVERIA`   | El Engine se cayó regando; al volver envía lo regado (guardado en disco) |

## 3. Flujo de un riego (resumen)

1. FO → `peticiones`: PETICION (fo_id, ws_id, duracion)
2. Central comprueba que el operario existe y que la WS está DISPONIBLE
3. Si no → `ordenes`: DENEGADO (fo_id, motivo). Fin.
4. Si sí → `ordenes`: REGAR (ws_id, duracion, operario) + AUTORIZADO (fo_id) → estado REGANDO
5. WS → `telemetria`: TELEMETRIA cada 1 s → Central la muestra y la reenvía al FO
6. WS → `telemetria`: FIN (tiempo · parada manual · fuga · bloqueo) → Central → FO: RESUMEN → DISPONIBLE
