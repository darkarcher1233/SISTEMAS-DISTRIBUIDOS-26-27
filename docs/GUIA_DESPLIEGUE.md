# Guía de despliegue: el día de la corrección

Escenario mínimo de la guía (si no se despliega, la práctica **no es evaluable**):

| Dónde | Qué | Cómo |
|---|---|---|
| **Cloud (AWS EC2)** | Kafka + WM_Central + SQLite | `docker-compose.cloud.yml` (llega YA desplegado) |
| **PC1 del laboratorio** | Varios WM_FO | `docker-compose.fo.yml` (+ `nuevo_fo.ps1`) |
| **PC2 del laboratorio** | Varias WM_WS (Monitor + Engine) | `docker-compose.ws.yml` (+ `nueva_ws.ps1`) |

Todo con Docker, sin IDE. `<IP_EC2>` = IP pública de la EC2. Todos los comandos de los PCs son de
**PowerShell**, en la carpeta del repo, con **Docker Desktop abierto**.

---

## 0. Antes del día (con tiempo)

- [ ] Parte cloud desplegada y probada siguiendo `docs/DESPLIEGUE_AWS.md` (security group con
      9092, 5050 y 8080; swap; `bash deploy/deploy_ec2.sh`). Mejor con **Elastic IP** (la IP no cambia).
- [ ] Todo subido a GitHub (`main`) y el profesor con acceso al repo.
- [ ] `Nombres.txt` con los apellidos y los prompts de IA en `prompts/`.
- [ ] Guía de corrección impresa a doble cara con la columna «E.» rellenada.

## 1. Cloud: arrancar y comprobar (EC2)

En el Learner Lab: **Start Lab** (esperar al punto verde) → EC2 → la instancia en «En ejecución».
Si no hay Elastic IP, apuntar la **IP pública nueva**.

```bash
ssh -i labsuser.pem ubuntu@<IP_EC2>            # o ec2-user@ en Amazon Linux
cd ~/SISTEMAS-DISTRIBUIDOS-26-27
bash deploy/deploy_ec2.sh                      # git pull + IP pública + docker compose up -d --build
docker compose -f docker-compose.cloud.yml ps  # kafka y wm_central "Up"
docker logs -f wm_central                      # "Kafka conectado (kafka:29092)"  (Ctrl+C solo sale del log)
```

Panel: `http://<IP_EC2>:8080` → WS1 y WS2 en **gris** (Desconectadas, leídas de SQLite: punto de la guía).

Dejar abierta esta terminal con `docker logs -f wm_central` (o `docker attach wm_central` para el
menú de Central; salir sin pararla: **Ctrl+P, Ctrl+Q**).

## 2. PC2: estaciones (WS)

```powershell
cd $HOME
git clone https://github.com/darkarcher1233/SISTEMAS-DISTRIBUIDOS-26-27.git   # o: cd ...; git pull
cd SISTEMAS-DISTRIBUIDOS-26-27
```

`.env` **escribiendo la IP a mano** (si se usa una variable `$IP` que no existe en esa ventana,
queda `KAFKA_BROKER=:9092` y nada conecta):

```powershell
"KAFKA_BROKER=<IP_EC2>:9092`nCENTRAL=<IP_EC2>:5050" | Set-Content -Encoding ascii .env
Get-Content .env              # deben verse las dos líneas CON la IP
```

Comprobar que se llega a la nube (las tres `TcpTestSucceeded : True`):

```powershell
Test-NetConnection <IP_EC2> -Port 9092
Test-NetConnection <IP_EC2> -Port 5050
Test-NetConnection <IP_EC2> -Port 8080
```

Arrancar:

```powershell
docker compose -f docker-compose.ws.yml up -d --build
docker compose -f docker-compose.ws.yml ps      # ws1_m, ws1_e, ws2_m, ws2_e "Up"
```

→ En el panel WS1 y WS2 pasan a **verde**.

Ver lo que pasa (una ventana de PowerShell para cada uno):

```powershell
docker logs -f ws1_m          # Monitor WS1: autenticación, health check, FUGA/RESUELTA
docker attach ws1_e           # Engine WS1 con su menú: f avería · r reparada · m pedir riego · s parar
```

Pantallas: Monitor WS1 `http://localhost:8100`, Engine WS1 `:8200`, Monitor WS2 `:8101`, Engine WS2 `:8201`.

## 3. PC1: operarios (FO)

```powershell
cd $HOME
git clone https://github.com/darkarcher1233/SISTEMAS-DISTRIBUIDOS-26-27.git   # o: cd ...; git pull
cd SISTEMAS-DISTRIBUIDOS-26-27
"KAFKA_BROKER=<IP_EC2>:9092`nCENTRAL=<IP_EC2>:5050" | Set-Content -Encoding ascii .env
Get-Content .env
docker compose -f docker-compose.fo.yml up -d --build
docker logs --tail 5 fo1      # debe salir "Conectado a Kafka (<IP_EC2>:9092)"
docker attach fo1             # menú: 1 ver WS · 2 pedir riego · 3 fichero · 0 salir  (salir sin pararlo: Ctrl+P, Ctrl+Q)
```

Pantallas: FO1 `http://localhost:8300`, FO2 `:8301` (arriba: «WM_Central: conectada»).

## 4. Demostración (sin interacción: «el sistema se inicia y se observa»)

| Paso | Dónde | Qué se ve |
|---|---|---|
| FO1 → opción 2: `WS1`, `30` | PC1 | Panel: WS1 verde parpadeando con L/min, L y FO1. FO1: AUTORIZADO, telemetría, RESUMEN |
| FO2 → opción 2: `WS1` mientras riega | PC1 | DENEGADO «WS en estado REGANDO» |
| FO1 → opción 3, Enter (`WM_FO/peticiones/fo1.txt`) | PC1 | Modo fichero: una tras otra, 4 s entre ellas; `WS9` → DENEGADO «La WS no existe» |
| `docker attach ws2_e` → `f`, luego `r` | PC2 | Rojo (Fuga) en Central y en el Monitor; un riego en curso acaba e informa al FO; `r` → verde |
| Engine → `m`: `FO1`, `20` | PC2 | Riego pedido desde la propia WS (pasa por Central) |
| Panel (o `docker attach wm_central`): bloquear / activar una o todas | EC2 | Naranja «Out of Service» (corta el riego) / verde |
| Panel: iniciar riego en una o todas | EC2 | Igual que si lo pidiera un operario (operario `CENTRAL`) |

## 5. Más instancias cuando lo pida el profesor (en caliente, sin parar nada)

**Nueva WS** (p. ej. WS3):
1. Darla de alta en Central: panel → «Dar de alta una estación» → `WS3`, `Jardín Nuevo`
   (o `docker attach wm_central` → opción `5`). Sin esto Central la **deniega** («WS no registrada»).
2. En el PC2 (u otro PC con el `.env`):
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\nueva_ws.ps1 WS3 "Jardín Nuevo"
   ```
   Pantallas: Monitor `http://localhost:8102`, Engine `:8202`. Menú: `docker attach ws3_e`.
   Para verlo sin ejecutarlo: añadir `-SoloMostrar`. Para quitarla: `.\nueva_ws.ps1 WS3 -Quitar`.

**Nuevo FO** (p. ej. FO3, que ya viene dado de alta; otros: panel → «Operarios dados de alta»):
```powershell
powershell -ExecutionPolicy Bypass -File .\nuevo_fo.ps1 FO3
docker attach fo3
```
Pantalla `http://localhost:8302`. Quitar: `.\nuevo_fo.ps1 FO3 -Quitar`.

Puertos de las pantallas: Monitor `8099+N`, Engine `8199+N`, FO `8299+N` (N = número del Id).

## 6. Caídas («crash») que puede pedir el profesor

Detalle y qué debe verse en cada caso: `docs/PRUEBAS_RESILIENCIA.md`. Resumen:

| Matar | Comando | Volver |
|---|---|---|
| Monitor | `docker kill ws1_m` | `docker start ws1_m` |
| Engine | `docker kill ws2_e` | `docker start ws2_e` |
| FO | `docker kill fo2` | `docker start fo2` |
| Central (EC2) | `docker kill wm_central` | `docker start wm_central` |

(O `docker attach <contenedor>` y **Ctrl+C**: sale limpio, sin trazas.) Solo se vuelve a arrancar
lo que se ha matado: lo demás se reconecta solo.

## 7. Si algo falla

| Síntoma | Causa | Solución |
|---|---|---|
| `Test-NetConnection` False | Security group de la EC2 sin 9092/5050/8080, o IP cambiada | Revisar reglas e IP (`docs/DESPLIEGUE_AWS.md`, A) |
| FO/WS: «Kafka no responde» o «Imposible conectar con WM_Central» y Kafka sin conectar | `.env` sin IP (`KAFKA_BROKER=:9092`) | `Get-Content .env`; rehacerlo con la IP escrita; `docker compose -f <fichero> up -d` |
| Todo conecta desde el PC pero Kafka no (sí el panel 8080) | Kafka anuncia otra IP (`PUBLIC_IP` de la EC2 mal) | En la EC2: `bash deploy/deploy_ec2.sh` de nuevo |
| Monitor: «WS no registrada» | Falta el alta | Panel → «Dar de alta una estación» (entra sola en ≤ 3 s) |
| `port is already allocated` | El puerto lo usa otro contenedor o programa (p. ej. una Kafka local en el 9092) | `docker ps`, `docker stop <nombre>` |
| Compose: «Falta KAFKA_BROKER en el .env» | No hay `.env` o está mal escrito | Crearlo (pasos 2/3) |
| `.\nueva_ws.ps1` no se puede ejecutar | Política de scripts de Windows | `powershell -ExecutionPolicy Bypass -File .\nueva_ws.ps1 ...` |
| Riego de un FO: «Operario no válido» | El operario no existe en Central | Panel → «Operarios dados de alta» |

## 8. Al terminar

```powershell
docker compose -f docker-compose.fo.yml down        # PC1  (+ .\nuevo_fo.ps1 FO3 -Quitar si se lanzó)
docker compose -f docker-compose.ws.yml down        # PC2  (+ .\nueva_ws.ps1 WS3 -Quitar)
```
La EC2 puede quedarse encendida (o `docker compose -f docker-compose.cloud.yml down`: la BD se conserva).

---

## Prueba sin AWS (dos PCs del laboratorio, para ensayar)

La parte cloud en el **PC2** en vez de en la EC2 (`<IP_PC2>` = `ipconfig` → Dirección IPv4):

```powershell
docker stop broker            # si hay una Kafka de desarrollo (docker-compose.yml) ocupando el 9092
"PUBLIC_IP=<IP_PC2>`nKAFKA_BROKER=<IP_PC2>:9092`nCENTRAL=<IP_PC2>:5050" | Set-Content -Encoding ascii .env
docker compose -f docker-compose.cloud.yml up -d --build
```

Firewall del PC2 (PowerShell **como administrador**, en UNA línea):
```powershell
New-NetFirewallRule -DisplayName "WaterManagement" -Direction Inbound -Protocol TCP -LocalPort 9092,5050,8080 -Action Allow
```

Después, pasos 2 y 3 con `<IP_PC2>` en lugar de `<IP_EC2>`. **Solo para ensayar**: el día de la
corrección Central tiene que estar en la nube.
