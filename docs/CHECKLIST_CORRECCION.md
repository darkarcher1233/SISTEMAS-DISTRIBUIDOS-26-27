# Checklist de la guía de corrección (Release 1)

Un punto por cada fila de `Practica_SD_WaterManagement_2026_2027_GuiaCorreccion.pdf`, en su orden.
Estado: **Cumple** · **Parcial** · **No cumple**. «Fase» = en qué fase del plan se arregló.
**Estado final (fase 5):** todo el código cumple; lo que queda está en «Pendiente de vosotros».
Desde la fase 1 las carpetas son `WM_Central/`, `WM_WS/` y `WM_FO/` (antes `central/`, `ws/`, `fo/`).

> Frase que manda: **«LA PRÁCTICA NO ES EVALUABLE si no se logra desplegar el escenario mínimo»**
> (parte Cloud en AWS + al menos dos PCs con WM_FO y WM_WS, todo conectado). Prioridad 1 = fase 2.

## 1. Despliegue, modularidad y escalabilidad (2 puntos)

| # | Punto de la guía | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 1.1 | Parte Cloud (WM_Central + Kafka + SQLite) ya desplegada en AWS; FO y WS en el laboratorio; todo sin IDE | **Parcial** | Fase 2: `docker-compose.cloud.yml` (Kafka KRaft con listeners INTERNAL/EXTERNAL + Central + volumen SQLite), `deploy/deploy_ec2.sh`, `.github/workflows/deploy.yml`, `docs/DESPLIEGUE_AWS.md`. Probado en local con la IP de red del PC como `PUBLIC_IP` | **Vosotros:** seguir `docs/DESPLIEGUE_AWS.md` (security group 5050/8080, swap, desplegar) y comprobar desde el laboratorio | 2 ✔ (falta AWS) |
| 1.2 | Repo en GitHub con estructura común: carpetas `WM_Central`, `WM_WS`, `WM_FO`, `prompts`, y `Readme.md` + `Nombres.txt`; acceso al profesor | **Parcial** | Hecho en la fase 1: `WM_Central/`, `WM_WS/`, `WM_FO/`, `prompts/`, `Readme.md`, `Nombres.txt` | **Vosotros:** copiar los prompts reales a `prompts/`, poner los apellidos en `Nombres.txt` y dar acceso al profesor en GitHub | 1 ✔ |
| 1.3 | Tantas instancias de cada módulo como pida el profesor, en la misma o en distintas máquinas; parar una de golpe («crash») | **Cumple** | `nueva_ws.ps1 WS3 "…"` y `nuevo_fo.ps1 FO3` (Docker, en caliente; probados), alta de WS en el panel; caídas con `docker kill` / Ctrl+C (`docs/GUIA_DESPLIEGUE.md` §5–6) | — | 5 ✔ |

## 2. Funcionamiento base (3 puntos)

| # | Punto de la guía | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 2.1 | Todo parametrizado (puertos de servidor, IPs/puertos a los que conectar); sin recompilar | **Cumple** | Argumentos obligatorios en los 4 `main()`; `PANEL_PORT`, `WEB_PORT`, `WATER_DB` por variable de entorno; compose con `${KAFKA_BROKER}`, `${CENTRAL}`, `${PUBLIC_IP}` y puertos en variables. Revisado: ninguna IP fija en los módulos | — | 3 ✔ |
| 2.2 | No falla durante una ejecución normal; Central atiende a cuantas WS/FO lleguen | **Cumple** | Un hilo por Monitor (`atender_monitor`), consumidores Kafka en hilos, panel `ThreadingHTTPServer`. Probado en el laboratorio con 2 PCs (02/11) | Repetirlo con la EC2 | 5 ✔ |
| 2.3 | Todo arranca sin interacción y se ve en las terminales de Central, FO y WS lo que pasa | **Cumple** | Consola de cada contenedor (`docker logs -f`, `docker attach`) + pantallas web. Guía: `docs/GUIA_DESPLIEGUE.md` §4 | — | 5 ✔ |
| 2.4 | Al arrancar/reiniciar, Central lee SQLite y muestra las WS registradas (con ubicación) como Desconectadas | **Cumple** | `cargar_estaciones()` + `get_estaciones()`. En la nube la BD está en el volumen `central_datos` (`WATER_DB=/data/water.db`): probado `down` + `up` → la WS sigue, en gris | — | 2 ✔ |
| 2.5 | Panel: ID, ubicación, agua suministrada, estados con colores; Watering parpadeante con L/min, L y operario; naranja con la leyenda **"Out of Service"** | **Cumple** | `ui/panel.html`, `ui/comun.js` (ETIQUETA `Out of Service`), `WM_Central/panel.py: foto_estado` (campo `total` = litros de los riegos terminados en la BD + el riego en curso, `db.volumen_por_ws`) | — | 3 ✔ |
| 2.6 | Registro y **autenticación** de WS con aceptación o **denegación** (p. ej. «WS no registrada») | **Cumple** | `autenticar_ws()` y `alta_ws()` (WM_Central.py); alta desde el menú (opción 5) y el panel (`/api/estaciones`); WS1 y WS2 de serie (`db.crear_tablas`). Denegaciones: «WS no registrada», «WS ya conectada desde otro Monitor». El Monitor lo muestra y reintenta (`monitor_central.mantener_conexion`). Tests `test_autenticar_*` | — | 3 ✔ |
| 2.7 | Validar disponibilidad y aceptar/denegar riegos de un FO | **Cumple** | `procesar_peticion()` (WM_Central.py:395): operario en BD + WS DISPONIBLE → AUTORIZADO/DENEGADO | — | — |
| 2.8 | Efectos visibles en pantalla en todas las apps; **no se pierde ninguna transacción** | **Cumple** | Pantallas web + consola. Central: commit después de procesar (`_consumir`), PETICION vieja → DENEGADO «caducada» (`procesar_peticion_kafka`). FO: grupo `fo_<id>` con commit. Engine: FIN guardado en disco si no sale (`enviar_fines_pendientes`) | — | 4 ✔ |
| 2.9 | Riegos controlables desde WM_FO, desde un menú de la WS y desde Central | **Cumple** | FO: `pedir_riego`. WS: tecla `m` del Engine y su pantalla → `pedir_riego_a_central()` (PETICION por Kafka, Central valida); el Engine muestra el AUTORIZADO/DENEGADO de su WS. Central: `ordenar("regar")` | — | 3 ✔ |
| 2.10 | Central: iniciar / bloquear (corta el riego → naranja) / activar, a una o a todas | **Cumple** | `ordenar()`, `bloquear_estacion()`, `activar_estacion()`; menú y panel. Engine para con motivo BLOQUEO (`procesar_orden`) | — | — |
| 2.11 | Monitor autenticado + HC cada segundo; KO o sin respuesta → fuga (rojo); fuga en riego → acaba e informa a todos; resuelta → disponible; visible en Central y en el Monitor | **Cumple** | `monitor_engine._vigilar` (HC 1 s, timeout 2 s), `notificar_fuga/resuelta`, FIN motivo FUGA → RESUMEN al FO | — | — |
| 2.12 | Los FO solo reciben los mensajes de SU riego, pero ven todas las WS disponibles | **Cumple** | `procesar_orden` del FO filtra por `fo_id` (WM_FO.py:123); `ESTADO_WS` para todos | — | — |
| 2.13 | Todos los servidores concurrentes y siempre disponibles | **Cumple** | Central: hilo por Monitor. Monitor: hilo de Central + hilo de Engine. Webs: `ThreadingHTTPServer` | — | — |

## 3. Resiliencia (3 puntos, solo si 2 está bien)

| # | Caso | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 3.1 | Un fallo solo invalida el servicio de ese componente | **Cumple** | Cada módulo reintenta solo (sockets y Kafka cada 3 s) | — | 4 ✔ |
| 3.2 | **Cae un Monitor**: Central → Desconectada; el riego en curso termina (salvo Engine caído); la WS no admite más riegos ni manuales y muestra «Averiada» | **Cumple** | Latido `VIVO` cada 2 s + `conn.settimeout(TIMEOUT_VIDA)` en `atender_monitor`; Engine: `conexion_monitor` corta el riego (FIN `MONITOR`), `iniciar_riego`/`pedir_riego_a_central` lo rechazan, pantalla «Averiada»; `desconectar_ws` avisa al FO. Probado matando el proceso + tests | — | 4 ✔ |
| 3.3 | **Cae un Engine**: Monitor → avería a Central (rojo) → Central avisa al FO; no se pierde lo regado; al volver envía el estado final | **Cumple** | `cambiar_estado` (FUGA en riego → `_avisar_fo`); Engine guarda `datos/riego_<ws>.json` cada segundo y `recuperar_riego_interrumpido` manda el FIN `AVERIA` al volver (volumen `wsN_datos` en Docker). Probado + test | — | 4 ✔ |
| 3.4 | **Cae un FO** durante su riego: el riego sigue; al volver ve el resultado | **Cumple** | FO con grupo `fo_<id>` y commit tras procesar (`consumir_ordenes`); un RESUMEN sin solicitud se muestra como «riego anterior». Probado + test | — | 4 ✔ |
| 3.5 | **Cae Central**: las WS acaban el riego en curso y luego paran; no se admiten peticiones nuevas | **Cumple** | FO y Engine detectan Central caída por la falta de `ESTADO_WS` (6 s): no envían peticiones; el riego en curso sigue; los FIN se procesan al volver. Probado con `docker kill wm_central` + tests | — | 4 ✔ |
| 3.6 | Recuperación reiniciando el mínimo de módulos; nuevas instancias sin fallos | **Cumple** | Solo se reinicia el módulo caído; los demás se reconectan solos. WS nueva: alta + arrancar. `docs/PRUEBAS_RESILIENCIA.md` (e) | — | 4 ✔ |
| 3.7 | Errores capturados y mostrados de forma controlada: «Imposible conectar con WM_Central» en FO y WS | **Cumple** | Monitor, Engine y FO (consola + pantalla). Ctrl+C (`docker kill -s INT`) en los 4 módulos: salida 0 y ninguna traza | — | 4 ✔ |
| 3.8 | (Nota de la guía) Tabla de estados Monitor/Engine → Disponible / Averiado / Desconectado | **Cumple** | Monitor OK + Engine KO → FUGA (rojo); Monitor caído (con o sin Engine) → DESCONECTADA; la WS sin Monitor se muestra «Averiada» | — | 4 ✔ |

## 4. General (2 puntos)

| # | Punto | Estado | Dónde está | Qué falta | Fase |
|---|---|---|---|---|---|
| 4.1 | Memoria (desarrollo, despliegue, resultados) | **Parcial** | `MEMORIA SD p1.docx` (sin subir), `capturas/` (locales), guía de despliegue en `docs/GUIA_DESPLIEGUE.md` | **Vosotros:** rehacer las capturas con la parte cloud en AWS (lista abajo) y copiar a la memoria la guía de despliegue | 5 |
| 4.2 | Diseño, confort, interfaces, acabado | **Cumple** | Pantallas web de todos los módulos (`ui/`), lanzador | — | — |
| 4.3 | Otros: dos centrales/brokers, tramas `<STX>D<ETX><LRC>` | **No cumple** | Sockets con cabecera de 64 bytes (`common/sockets_utils.py`) | Opcional, preguntar antes | Opcional |

## Fuera de la guía pero obligatorio por el enunciado / uso de IA

| Punto | Estado | Qué falta |
|---|---|---|
| Prompts de IA en el repo (`prompts/`) | **No cumple** | `prompts_ia/` no tiene prompts: hay que copiarlos (incluidos los de esta sesión) |
| CI/CD: VS Code → GitHub → AWS | **No cumple** | Script `deploy/deploy_ec2.sh` + (opcional) GitHub Actions por SSH |
| Saber explicar el código | — | Resumen en español sencillo al final de cada fase |

## Pendiente de vosotros (no se puede hacer desde el código)

| Qué | Dónde |
|---|---|
| Desplegar la parte cloud en la EC2 y probarla desde el laboratorio | `docs/DESPLIEGUE_AWS.md` |
| Copiar los prompts de IA reales a `prompts/` | Obligatorio: sin ellos se puede suspender |
| Apellidos y DNI en `Nombres.txt` y en la memoria | `Nombres.txt` |
| Dar acceso al profesor al repo de GitHub | GitHub > Settings > Collaborators |
| Rehacer las capturas con AWS (lista abajo) y terminar la memoria | `MEMORIA SD p1.docx` |
| Imprimir la guía de corrección a doble cara con la columna «E.» rellenada | — |
| (Opcional) Tramas `<STX><DATA><ETX><LRC>` en sockets: suma en «otros aspectos técnicos» | Preguntar antes |

## Capturas para la memoria

Las de `capturas/` se hicieron en local (Kafka en `localhost`). Las que deben salir con la parte
cloud en AWS, en un navegador o terminal **real** donde se vea la IP o el comando:

| Captura de la memoria | Qué capturar ahora |
|---|---|
| 0 (portada), 6, 13, 14, 15, 20, 21, 22 | Panel en `http://<IP_EC2>:8080` con la barra de direcciones visible (mismos estados que las de `capturas/`) |
| 2 | En la EC2: `docker logs wm_central` con `REG: WS1/WS2 autenticada` y «Kafka conectado (kafka:29092)» |
| 3, 11 | PC2: `docker compose -f docker-compose.ws.yml ps` (4 contenedores Up) y `docker logs ws1_m` («autenticada en WM_Central (<IP_EC2>:5050)») |
| 8 | Consola de AWS: la instancia «En ejecución» con su IP pública |
| 9 | Security group: reglas 22, 9092, **5050**, **8080** |
| 10 | En la EC2: `docker compose -f docker-compose.cloud.yml ps` (kafka y wm_central Up) y el final de `bash deploy/deploy_ec2.sh` |
| 12 | PC2: `docker attach ws1_e` con el menú del Engine |
| 16 | En la EC2: `docker logs wm_central` con `FIN: WS… motivo=FUGA … -> FUGA` |
| 17 | `python -m tests.ver_telemetria <IP_EC2>:9092` (TELEMETRIA y FIN) |
| 18 | PC1: `docker attach fo1` y `docker attach fo2` en modo fichero |
| 19 | PC1: `docker compose -f docker-compose.fo.yml ps` |
| 4, 5a, 5b, 7 (código) | Ya rehechas con el código final (`capturas/`) |
| Nuevas (recomendadas) | Monitor con «WS no registrada» y la WS entrando tras el alta · un caso de resiliencia (p. ej. Engine caído → FIN AVERIA con el volumen) · FO/Engine con «Imposible conectar con WM_Central» · `Test-NetConnection` a 9092/5050/8080 · ejecución de GitHub Actions (si se usa) |
