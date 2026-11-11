# Pruebas de resiliencia (guía de corrección, apartado «Resiliencia»)

Cada caso: **cómo provocarlo**, **qué debe verse** y **cómo se recupera**. Los marcados con 🧪
tienen además un test automático en `tests/test_resiliencia.py` (`python -m pytest`).

## Preparación (escenario mínimo)

- **EC2**: parte cloud desplegada (`docs/DESPLIEGUE_AWS.md`). Panel: `http://<IP_EC2>:8080`.
- **PC 2 (WS)**: `docker compose -f docker-compose.ws.yml up -d --build`
  Pantallas: Monitor WS1 `http://localhost:8100`, Engine WS1 `:8200`, Monitor WS2 `:8101`, Engine WS2 `:8201`.
- **PC 1 (FO)**: `docker compose -f docker-compose.fo.yml up -d --build`
  Pantallas: FO1 `http://localhost:8300`, FO2 `:8301`.
- Consolas (lo que pide la guía: «observar las terminales»): `docker compose -f <fichero> logs -f <servicio>`
  o `docker attach <contenedor>` (salir sin pararlo: **Ctrl+P, Ctrl+Q**).

Formas de "matar de golpe" un módulo (equivalen a cerrar su ventana):
- `docker kill <contenedor>` (no se reinicia solo: Docker no reinicia lo que paras tú), o
- `docker attach <contenedor>` y **Ctrl+C** (sale limpio, sin trazas de Python).

Volver a arrancarlo: `docker start <contenedor>` (o `docker compose -f <fichero> up -d`).

---

## a) Cae un Monitor (WM_WS_M) 🧪

1. FO1 pide un riego largo en WS1 (pantalla del FO1 o `docker attach fo1` → opción 2: `WS1`, `60`).
2. Mientras riega, en el PC 2: `docker kill ws1_m`.

Debe verse:
- **Central** (panel y `docker logs -f wm_central` en la EC2): WS1 en **gris, Desconectada**, al
  momento (socket cerrado). Si lo que se corta es la red (el socket no se cierra), a los **6 s**
  sin mensajes de vida (`VIVO#WS1` cada 2 s) → «sin mensajes de vida del Monitor de WS1».
- **Engine WS1** (`docker logs -f ws1_e` o `:8200`): «Monitor perdido durante un riego: el riego
  termina», `FIN motivo=MONITOR`, y **«WS AVERIADA: sin monitor no se admiten riegos (ni manuales)»**.
  La pantalla muestra **Averiada**. Probar `m` en `docker attach ws1_e`: lo rechaza.
- **FO1**: aviso «La estación ha perdido su monitor: el riego termina» y el RESUMEN con lo regado
  (motivo MONITOR).

Recuperación: `docker start ws1_m` → se autentica solo (`REG: WS1 autenticada`), el Engine se
reconecta solo y WS1 vuelve a **verde**. No se reinicia nada más.

Monitor **y** Engine caídos a la vez (`docker kill ws1_m ws1_e`): Central → Desconectada. Lo regado
queda en el volumen del Engine y se envía al volver (caso b).

## b) Cae un Engine (WM_WS_E) 🧪

1. FO1 pide un riego largo en WS2.
2. Mientras riega: `docker kill ws2_e`.

Debe verse:
- **Monitor WS2** (`:8101`): «Engine: KO (El engine no responde)» y `FUGA#WS2` enviado a Central.
- **Central**: WS2 en **rojo (Fuga)** — «Avería avisada por el monitor».
- **FO1**: alerta «WS2: Avería en la estación: el riego se ha interrumpido».
- Lo regado NO se pierde: el Engine lo guardaba cada segundo en `/app/datos/riego_WS2.json`
  (volumen `ws2_datos`). Comprobar: `docker run --rm -v sd-ws_ws2_datos:/d alpine cat /d/riego_WS2.json`

Recuperación: `docker start ws2_e` → «Riego interrumpido recuperado del disco: X L de FO1»,
«FIN pendiente enviado (motivo=AVERIA, X L)». El Monitor ve OK → `RESUELTA#WS2` → WS2 **verde**.
FO1 recibe el **RESUMEN con el volumen real** («el engine de la estación se cayó (volumen real
recuperado al volver)»).

## c) Se cierra un FO durante su riego 🧪

1. FO2 pide `WS1`, `20` s.
2. A los pocos segundos: `docker kill fo2`.

Debe verse: el riego **sigue** en Central (verde parpadeando) y en el Engine hasta acabar.

Recuperación: `docker start fo2` (y `docker attach fo2`, o su pantalla `:8301`) → «Resultado de un
riego anterior en WS1» con su RESUMEN. Funciona porque cada FO tiene su grupo de Kafka (`fo_FO2`)
y confirma (commit) cada mensaje solo después de procesarlo: Kafka le guarda lo que llegó mientras
estaba cerrado.

## d) Cae WM_Central

1. FO1 pide un riego de `30` s en WS2.
2. En la EC2: `docker kill wm_central`.

Debe verse (sin trazas de Python en ninguna consola):
- **WS2** sigue regando hasta acabar (Kafka es independiente de Central) y luego se para.
- **Engine** (consola y pantalla): «Imposible conectar con WM_Central: el riego en curso termina y
  no se aceptan riegos nuevos hasta que vuelva». Pedir riego (`m`) → «Imposible conectar con WM_Central».
- **Monitor**: «Imposible conectar con WM_Central (IP:5050). Se reintenta cada 3 s».
- **FO**: banner «Imposible conectar con WM_Central: no se pueden pedir riegos hasta que vuelva»;
  pedir un riego (menú o pantalla) → no se envía. En modo fichero cada línea queda «no enviada».

Recuperación: `docker start wm_central` (o `docker compose -f docker-compose.cloud.yml up -d`):
- Lee SQLite: todas las WS en gris hasta que sus Monitor se autentican **solos** (sin reiniciar las WS).
- Procesa lo que llegó a Kafka mientras estaba caída (caso f): el FIN del riego de WS2 → guarda el
  riego y manda a FO1 su RESUMEN.
- FO y Engine: «WM_Central vuelve a responder».

## e) Recuperación con el mínimo de reinicios y nuevas instancias

- En a)–d) solo se vuelve a arrancar el módulo caído: el resto se reconecta solo
  (Monitor → Central cada 3 s; Engine → Monitor cada 3 s; todos → Kafka cada 3 s).
- **Nueva WS en caliente** (p. ej. WS3, sin parar nada):
  1. Panel de Central → «Dar de alta una estación»: `WS3`, `Jardín Nuevo` (o menú de Central, opción 5).
  2. En el PC 2 (ver `docs/GUIA_DESPLIEGUE.md` para el comando completo): Monitor y Engine de WS3.
  Sin el paso 1, Central la **deniega** («WS no registrada») y el Monitor lo reintenta solo: en
  cuanto se da de alta, entra.
- **Nuevo FO en caliente**: alta del operario en el panel («Operarios dados de alta») y arrancar su
  contenedor (ver la guía de despliegue).

## f) No se pierde ninguna transacción 🧪

- Los consumidores de Central (`peticiones`, `telemetria`) confirman a Kafka cada mensaje **después**
  de procesarlo (`enable_auto_commit=False` + `commit()` en `WM_Central._consumir`). Si Central cae,
  al volver Kafka le entrega todo lo pendiente (visto en d: el FIN).
- Una PETICION enviada con Central caída (p. ej. por un FO que no lo detectó) se **contesta** al
  volver: si tiene más de 30 s, DENEGADO «Petición caducada: WM_Central no estaba disponible cuando
  se envió» (no se lanza un riego que el operario ya no espera).
- Un FIN que el Engine no pudo enviar (Kafka caído) se guarda en `datos/fin_pendiente_*.json` y se
  reintenta hasta que sale.
- El FO recibe lo suyo aunque esté cerrado (caso c).

---

## Prueba hecha (02/11/2026, en local con la parte cloud en Docker)

Cloud con `docker-compose.cloud.yml` (Kafka por la IP de red del PC, como la EC2), WS y FO como
procesos matados con `taskkill /F`:

| Caso | Resultado |
|---|---|
| a | WS1 Desconectada al instante; Engine: FIN MONITOR, «WS AVERIADA», riego manual rechazado; FO1: aviso + RESUMEN. Monitor de vuelta → verde |
| b | WS2 Fuga; FO1 alerta; `riego_WS2.json` en disco. Engine de vuelta → FIN AVERIA 2,178 L → FO1 RESUMEN con ese volumen; WS2 verde |
| c | FO2 cerrado; el riego terminó; FO2 de vuelta → «riego anterior» FINALIZADA 2,491 L |
| d | `docker kill wm_central`: el riego de WS2 terminó; Engine/Monitor/FO «Imposible conectar con WM_Central»; FO no deja pedir. `docker start` → WS re-autenticadas solas, FIN procesado (RESUMEN a FO1), PETICION antigua DENEGADA «caducada» |
| Ctrl+C | `docker kill -s INT` a Central, Monitor, Engine y FO: salen con código 0 y 0 trazas |
