# Checklist de la guía de corrección (Release 1)

Un punto por cada fila de `Practica_SD_WaterManagement_2026_2027_GuiaCorreccion.pdf`, en su orden.
Estado: **Cumple** · **Parcial** · **No cumple**. «Fase» = en qué fase del plan se arregla.
Rutas de la rama `release1-final` ANTES de la fase 1 (después: `central/` → `WM_Central/`, `ws/` → `WM_WS/`, `fo/` → `WM_FO/`).

> Frase que manda: **«LA PRÁCTICA NO ES EVALUABLE si no se logra desplegar el escenario mínimo»**
> (parte Cloud en AWS + al menos dos PCs con WM_FO y WM_WS, todo conectado). Prioridad 1 = fase 2.

## 1. Despliegue, modularidad y escalabilidad (2 puntos)

| # | Punto de la guía | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 1.1 | Parte Cloud (WM_Central + Kafka + SQLite) ya desplegada en AWS; FO y WS en el laboratorio; todo sin IDE | **No cumple** | En la EC2 solo hay Kafka (51.20.108.192:9092). Central solo se ha ejecutado en local (`python -m central.WM_Central`) | `docker-compose.cloud.yml` (Kafka + Central + volumen SQLite), reglas del security group 5050/8080, pasos en la EC2. CI/CD GitHub → AWS (lo pide el enunciado, pág. 13) | 2 |
| 1.2 | Repo en GitHub con estructura común: carpetas `WM_Central`, `WM_WS`, `WM_FO`, `prompts`, y `Readme.md` + `Nombres.txt`; acceso al profesor | **No cumple** | Carpetas `central/`, `ws/`, `fo/`, `prompts_ia/`; fichero `README.md`. `prompts_ia/` **solo tiene un README, no hay prompts** | Renombrar con `git mv`, crear `Nombres.txt`, `Readme.md`. **Copiar los prompts reales** (vuestra tarea). Dar acceso al profesor en GitHub (vuestra tarea) | 1 |
| 1.3 | Tantas instancias de cada módulo como pida el profesor, en la misma o en distintas máquinas; parar una de golpe ("crash") | **Parcial** | Los compose del lab tienen 2 WS y 2 FO fijos (`docker-compose.ws.yml`, `docker-compose.fo.yml`). Los módulos aguantan varias instancias (puertos web libres automáticos, `common/web.py: iniciar_web`) | Forma documentada y probada de lanzar la WS nº N o el FO nº N con Docker en caliente (comandos `docker run` parametrizados o script) | 2 y 5 |

## 2. Funcionamiento base (3 puntos)

| # | Punto de la guía | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 2.1 | Todo parametrizado (puertos de servidor, IPs/puertos a los que conectar); sin recompilar | **Cumple** (revisar) | Argumentos obligatorios en los 4 `main()`; `PANEL_PORT`, `WEB_PORT`, `WATER_DB` por variable de entorno | Revisar que no quede ninguna IP fija (p. ej. compose y `.env.example` con la IP de la EC2 en variables) | 2, 3 |
| 2.2 | No falla durante una ejecución normal; Central atiende a cuantas WS/FO lleguen | **Cumple** | Un hilo por Monitor (`atender_monitor`), consumidores Kafka en hilos, panel `ThreadingHTTPServer` | Probarlo en el escenario real (AWS + 2 PCs) | 5 |
| 2.3 | Todo arranca sin interacción y se ve en las terminales de Central, FO y WS lo que pasa | **Cumple** | Logs por consola en todos los módulos + pantallas web | Comprobar en Docker (`docker compose logs -f`) | 5 |
| 2.4 | Al arrancar/reiniciar, Central lee SQLite y muestra las WS registradas (con ubicación) como Desconectadas | **Cumple** | `cargar_estaciones()` (WM_Central.py:161) + `get_estaciones()` (db.py:72). Captura 22 | En la nube la BD debe vivir en un volumen (`WATER_DB=/data/water.db`) | 2 |
| 2.5 | Panel: ID, ubicación, agua suministrada, estados con colores; Watering parpadeante con L/min, L y operario; naranja con la leyenda **"Out of Service"** | **Parcial** | `ui/panel.html`, `ui/comun.css`, `central/panel.py: foto_estado` | La leyenda dice «Fuera de servicio»: la guía pide literalmente **"Out of Service"** (`ui/comun.js` ETIQUETA). El panel debe escuchar en 0.0.0.0 (ya lo hace) y verse desde fuera de la EC2 | 3 |
| 2.6 | Registro y **autenticación** de WS con aceptación o **denegación** (p. ej. «WS no registrada») | **Parcial** | `atender_monitor()` (WM_Central.py:626): `REG` siempre da de alta; solo deniega formato incorrecto o una conexión que ya es de otra WS | Distinguir alta (`REG`) y autenticación (`AUTH`) y denegar «WS no registrada» y «WS ya conectada desde otro Monitor». Ver decisión D1 del plan | 3 |
| 2.7 | Validar disponibilidad y aceptar/denegar riegos de un FO | **Cumple** | `procesar_peticion()` (WM_Central.py:395): operario en BD + WS DISPONIBLE → AUTORIZADO/DENEGADO | — | — |
| 2.8 | Efectos visibles en pantalla en todas las apps; **no se pierde ninguna transacción** | **Parcial** | Pantallas web + consola en todos | Consumidores de Central con auto-commit (`kafka_utils.py:70`) → un mensaje se puede perder si Central cae a mitad. FO con `group_id=None` (WM_FO.py:182) → pierde lo que llega mientras está caído | 4 |
| 2.9 | Riegos controlables desde WM_FO, desde un menú de la WS y desde Central | **Parcial** | FO: `pedir_riego`. Central: `ordenar("regar")`. WS: tecla `m` del Engine (`menu`, WM_WS_E.py:365) | La `m` del Engine riega **sin pasar por Central** (no valida, no hay AUTORIZADO, Central se entera por la telemetría). Debe enviar una PETICION como la pantalla web | 3 |
| 2.10 | Central: iniciar / bloquear (corta el riego → naranja) / activar, a una o a todas | **Cumple** | `ordenar()`, `bloquear_estacion()`, `activar_estacion()`; menú y panel. Engine para con motivo BLOQUEO (`procesar_orden`) | — | — |
| 2.11 | Monitor autenticado + HC cada segundo; KO o sin respuesta → fuga (rojo); fuga en riego → acaba e informa a todos; resuelta → disponible; visible en Central y en el Monitor | **Cumple** | `monitor_engine._vigilar` (HC 1 s, timeout 2 s), `notificar_fuga/resuelta`, FIN motivo FUGA → RESUMEN al FO | — | — |
| 2.12 | Los FO solo reciben los mensajes de SU riego, pero ven todas las WS disponibles | **Cumple** | `procesar_orden` del FO filtra por `fo_id` (WM_FO.py:123); `ESTADO_WS` para todos | — | — |
| 2.13 | Todos los servidores concurrentes y siempre disponibles | **Cumple** | Central: hilo por Monitor. Monitor: hilo de Central + hilo de Engine. Webs: `ThreadingHTTPServer` | — | — |

## 3. Resiliencia (3 puntos, solo si 2 está bien)

| # | Caso | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 3.1 | Un fallo solo invalida el servicio de ese componente | **Parcial** | Reintentos de Kafka y de sockets en todos los módulos | Lo que falta está en 3.2–3.8 | 4 |
| 3.2 | **Cae un Monitor**: Central → Desconectada; el riego en curso termina (salvo Engine caído); la WS no admite más riegos ni manuales y muestra «Averiada» | **Parcial** | Central marca DESCONECTADA al cerrarse el socket (`desconectar_ws`) | El Engine **sigue regando y acepta riegos** sin Monitor (`conexion_monitor` solo cambia `con_monitor`). Además la guía define Desconectada como «Central no recibe ningún mensaje de vida»: hoy no hay latido, solo el cierre del socket | 4 |
| 3.3 | **Cae un Engine**: Monitor → avería a Central (rojo) → Central avisa al FO; no se pierde lo regado; al volver envía el estado final | **Parcial** | Monitor avisa FUGA al caer el Engine (`_vigilar`) | Central no avisa al FO cuando la WS pasa a FUGA por socket. Lo regado se pierde (vive en memoria). Falta fichero local del riego en curso + FIN al volver | 4 |
| 3.4 | **Cae un FO** durante su riego: el riego sigue; al volver ve el resultado | **Parcial** | El riego sigue (no depende del FO) | `group_id=None` + `latest` → al volver no recibe el RESUMEN | 4 |
| 3.5 | **Cae Central**: las WS acaban el riego en curso y luego paran; no se admiten peticiones nuevas | **Parcial** | El Engine sigue regando (Kafka es independiente). El Monitor reintenta (`mantener_conexion`) | El Engine no sabe que Central ha caído (sigue aceptando riegos manuales); el FO puede enviar PETICION que Central procesaría tarde | 4 |
| 3.6 | Recuperación reiniciando el mínimo de módulos; nuevas instancias sin fallos | **Cumple** (probar) | Monitor se re-registra solo; Engine se reconecta al Monitor; session timeout 6 s en Kafka | Prueba escrita en `docs/PRUEBAS_RESILIENCIA.md` | 4 |
| 3.7 | Errores capturados y mostrados de forma controlada: «Imposible conectar con WM_Central» en FO y WS | **Parcial** | Monitor: «Conexión con Central perdida; reintentando» | Mensaje exacto en FO, Engine y Monitor (consola + web); revisar que Ctrl+C no saque trazas de Python | 4 |
| 3.8 | (Nota de la guía) Tabla de estados Monitor/Engine → Disponible / Averiado / Desconectado | **Parcial** | Monitor OK + Engine KO → FUGA (rojo) ✔; Monitor caído → DESCONECTADA ✔ | Que la WS muestre «Averiada» cuando el Monitor falta (3.2) | 4 |

## 4. General (2 puntos)

| # | Punto | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 4.1 | Memoria (desarrollo, despliegue, resultados) | **Parcial** | `MEMORIA SD p1.docx` (sin subir), `capturas/` | Rehacer capturas con la parte cloud; guía de despliegue = la del día de la corrección | 5 |
| 4.2 | Diseño, confort, interfaces, acabado | **Cumple** | Pantallas web de todos los módulos (`ui/`), lanzador | — | — |
| 4.3 | Otros: dos centrales/brokers, tramas `<STX>D<ETX><LRC>` | **No cumple** | Sockets con cabecera de 64 bytes (`common/sockets_utils.py`) | Opcional, preguntar antes | Opcional |

## Fuera de la guía pero obligatorio por el enunciado / uso de IA

| Punto | Estado | Qué falta |
|---|---|---|
| Prompts de IA en el repo (`prompts/`) | **No cumple** | `prompts_ia/` no tiene prompts: hay que copiarlos (incluidos los de esta sesión) |
| CI/CD: VS Code → GitHub → AWS | **No cumple** | Script `deploy/deploy_ec2.sh` + (opcional) GitHub Actions por SSH |
| Saber explicar el código | — | Resumen en español sencillo al final de cada fase |
