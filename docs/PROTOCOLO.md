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
| Monitor → Engine | `ID#<ws_id>` | – | Nada más conectarse: el Engine recibe su ID |
| Monitor → Engine | `HC` (cada 1 s) | `OK` / `KO` | Health check. Timeout de 2 s = KO |

### Funciones compartidas dentro de WM_WS_M

| Función | Fichero | Quién la escribe | Quién la llama |
|---|---|---|---|
| `registrar(ip, puerto, ws_id, ubicacion)` | `ws/monitor_central.py` | Abde | `WM_WS_M.py` |
| `notificar_fuga()` | `ws/monitor_central.py` | Abde | `monitor_engine.py` |
| `notificar_resuelta()` | `ws/monitor_central.py` | Abde | `monitor_engine.py` |
| `iniciar_servidor_engine(puerto, ws_id)` | `ws/monitor_engine.py` | Vlad | `WM_WS_M.py` |

## 2. Kafka (Semana 2: completar entonces)

Mensajes en JSON; todos llevan `"tipo"`. Implementación: `common/kafka_utils.py`.

| Topic | Quién escribe → quién lee | Tipos de mensaje |
|---|---|---|
| `peticiones` | FO → Central | `PETICION` |
| `ordenes` | Central → WS y FO | `REGAR`, `BLOQUEAR`, `ACTIVAR`, `AUTORIZADO`, `DENEGADO`, `TELEMETRIA`, `RESUMEN`, `ESTADO_WS` |
| `telemetria` | WS → Central | `TELEMETRIA`, `FIN` |

Cada WS y cada FO lee todo `ordenes` y se queda solo con lo que lleva su `ws_id` / `fo_id`.

_TODO semana 2: un ejemplo de JSON de cada tipo._

## 3. Flujo de un riego (resumen)

1. FO → `peticiones`: PETICION (fo_id, ws_id, duracion)
2. Central comprueba que el operario existe y que la WS está DISPONIBLE
3. Si no → `ordenes`: DENEGADO (fo_id, motivo). Fin.
4. Si sí → `ordenes`: REGAR (ws_id, duracion, operario) + AUTORIZADO (fo_id) → estado REGANDO
5. WS → `telemetria`: TELEMETRIA cada 1 s → Central la muestra y la reenvía al FO
6. WS → `telemetria`: FIN (tiempo · parada manual · fuga · bloqueo) → Central → FO: RESUMEN → DISPONIBLE
