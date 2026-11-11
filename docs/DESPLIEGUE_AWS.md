# Despliegue de la parte cloud en AWS (EC2): Kafka + WM_Central + SQLite

Todo lo de aquí lo hacéis vosotros (consola de AWS y terminal de la EC2). Orden:
**A** security group → **B** preparar la EC2 (una vez) → **C** desplegar → **D** comprobar desde el
laboratorio → **E** (opcional) CI/CD con GitHub Actions.

`<IP_EC2>` = IP pública de la instancia (EC2 > Instancias > «Dirección IPv4 pública»).

> **Learner Lab:** la sesión se apaga a las 4 h y, al volver a arrancarla, **la IP pública cambia**.
> Opciones: (1) asignar una **Elastic IP** a la instancia (EC2 > Direcciones IP elásticas >
> Asignar > Asociar a la instancia): la IP ya no cambia, recomendado; o (2) volver a ejecutar
> `bash deploy/deploy_ec2.sh` tras cada arranque (lee la IP nueva solo) y cambiar `.env` en los
> PCs del laboratorio. **El día de la corrección: arrancar el lab con tiempo** y comprobar D.

---

## A. Reglas de entrada del security group

EC2 > Instancias > (vuestra instancia) > pestaña Seguridad > clic en el security group >
**Editar reglas de entrada** > Agregar regla (una por fila):

| Tipo | Protocolo | Puerto | Origen | Para qué |
|---|---|---|---|---|
| SSH | TCP | 22 | Mi IP (o `0.0.0.0/0` si usáis GitHub Actions, sección E) | Entrar a la EC2 |
| TCP personalizado | TCP | 9092 | `0.0.0.0/0` | Kafka para los WS y FO del laboratorio (**ya la tenéis**) |
| TCP personalizado | TCP | **5050** | `0.0.0.0/0` | Sockets de WM_Central (los Monitor) |
| TCP personalizado | TCP | **8080** | `0.0.0.0/0` | Panel web de WM_Central |

Guardar reglas. (`0.0.0.0/0` = desde cualquier IP: los PCs del laboratorio salen a Internet con
IPs que no conocemos de antemano.)

---

## B. Preparar la EC2 (solo la primera vez)

Entrar por SSH (desde la carpeta donde está la clave; en el Learner Lab: «AWS Details» > Download PEM):

```bash
ssh -i labsuser.pem ubuntu@<IP_EC2>        # AMI Ubuntu
ssh -i labsuser.pem ec2-user@<IP_EC2>      # AMI Amazon Linux
```

Si no sabéis cuál es: `cat /etc/os-release`.

### B1. Docker + Docker Compose + git

Comprobar primero (si las tres responden, saltar a B2):

```bash
docker --version && docker compose version && git --version
```

**Ubuntu:**
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
exit        # salir y volver a entrar por SSH para que el grupo docker tenga efecto
```

**Amazon Linux 2023:**
```bash
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
     -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
exit        # volver a entrar por SSH
```

### B2. Memoria de intercambio (swap) de 2 GB

La t3.micro tiene 1 GB de RAM. Medido en local: Kafka ≈ 350 MB (con `-Xmx512m` puede llegar a
~700 MB) + Central ≈ 30 MB + Docker. Cabe, pero justo: el swap evita que Linux mate a Kafka si
un momento se pasa.

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
free -h      # debe salir una línea "Swap: 2.0Gi"
```

Si aun así Kafka se reinicia solo (`docker ps` muestra «Restarting» o `docker logs kafka` dice
`OutOfMemory`): parar la instancia y cambiar el tipo a **t3.small** (2 GB)
(EC2 > Instancia > Estado: Detener > Acciones > Configuración de la instancia > Cambiar tipo).

### B3. Quitar el Kafka antiguo

El Kafka que ya tenéis en la EC2 se llama `kafka` y ocupa el puerto 9092: el nuevo
`docker-compose.cloud.yml` lo sustituye (con los dos listeners).

```bash
docker ps -a                  # ver cómo se llama el contenedor de Kafka actual
docker rm -f kafka            # (cambiar "kafka" por su nombre si es otro)
```

### B4. Clonar el repositorio

Si el repo es **público**:
```bash
cd ~
git clone https://github.com/darkarcher1233/SISTEMAS-DISTRIBUIDOS-26-27.git
```

Si es **privado**, GitHub pide usuario y **token** (no la contraseña): GitHub > Settings >
Developer settings > Personal access tokens > Fine-grained tokens > Generate new token >
Repository access: solo este repo > Permissions: Contents = Read-only. Después:
```bash
cd ~
git clone https://github.com/darkarcher1233/SISTEMAS-DISTRIBUIDOS-26-27.git
# Username: vuestro usuario de GitHub      Password: el token
git -C SISTEMAS-DISTRIBUIDOS-26-27 config credential.helper store   # para que git pull no lo vuelva a pedir
```

Mientras el trabajo esté en la rama `release1-final` (antes de pasarla a `main`):
```bash
git -C ~/SISTEMAS-DISTRIBUIDOS-26-27 checkout release1-final
```

### B5. Crear el .env de la EC2

```bash
cd ~/SISTEMAS-DISTRIBUIDOS-26-27
cp .env.example .env
```

No hace falta editarlo: `deploy/deploy_ec2.sh` escribe `PUBLIC_IP` solo (las líneas
`KAFKA_BROKER` y `CENTRAL` son para los PCs del laboratorio y aquí no se usan).

---

## C. Desplegar (y cada vez que haya cambios en GitHub)

```bash
cd ~/SISTEMAS-DISTRIBUIDOS-26-27
bash deploy/deploy_ec2.sh
```

Hace: `git pull` → `PUBLIC_IP` en `.env` → `docker compose -f docker-compose.cloud.yml up -d --build`
→ muestra el estado. La primera vez tarda unos minutos (descarga Kafka y construye la imagen).

Comprobar en la EC2:
```bash
docker compose -f docker-compose.cloud.yml ps        # kafka y wm_central "Up"
docker logs wm_central | grep -E "Kafka|Topics"      # "Topics creados..." y "Kafka conectado (kafka:29092)"
docker exec kafka kafka-topics.sh --bootstrap-server localhost:29092 --list   # ordenes, peticiones, telemetria
docker attach wm_central                             # menú de Central (Enter muestra la ayuda)
                                                     #   salir SIN pararla: Ctrl+P y luego Ctrl+Q
```

Otros comandos útiles:
```bash
docker compose -f docker-compose.cloud.yml logs -f wm_central   # ver Central en directo (Ctrl+C sale)
docker compose -f docker-compose.cloud.yml restart wm_central   # reiniciar solo Central
docker compose -f docker-compose.cloud.yml down                 # parar todo (la BD y Kafka se conservan)
docker compose -f docker-compose.cloud.yml down -v              # parar Y BORRAR BD y datos de Kafka
```

La base de datos está en el volumen `wm-cloud_central_datos` (`/data/water.db` dentro del
contenedor): sobrevive a reinicios, a `down` y a `--build`. Solo `down -v` la borra.

---

## D. Comprobar desde un PC del laboratorio

PowerShell (las tres deben decir `TcpTestSucceeded : True`):
```powershell
Test-NetConnection <IP_EC2> -Port 9092
Test-NetConnection <IP_EC2> -Port 5050
Test-NetConnection <IP_EC2> -Port 8080
```

- Navegador: `http://<IP_EC2>:8080` → panel de Central con «Kafka: conectado».
- Kafka de verdad (no solo el puerto), desde la raíz del repo:
  `python -m tests.ver_telemetria <IP_EC2>:9092` (se queda escuchando; Ctrl+C para salir).
  Si da error de conexión a `kafka:29092` o a `localhost`, Kafka está anunciando la dirección
  equivocada: revisar `PUBLIC_IP` en el `.env` de la EC2 y volver a ejecutar C.

En cada PC del laboratorio, el `.env` (copiado de `.env.example`) debe tener:
```
KAFKA_BROKER=<IP_EC2>:9092
CENTRAL=<IP_EC2>:5050
```

---

## E. (Opcional) CI/CD: GitHub → AWS con GitHub Actions

`.github/workflows/deploy.yml`: en cada `push` a `main` (o a mano: pestaña **Actions** >
«Desplegar en AWS» > Run workflow), GitHub entra por SSH en la EC2 y ejecuta
`bash deploy/deploy_ec2.sh`. Es el ciclo VS Code → GitHub → AWS que pide el enunciado.

Secretos (GitHub > repo > Settings > Secrets and variables > Actions > New repository secret):

| Secreto | Valor |
|---|---|
| `EC2_HOST` | `<IP_EC2>` (si no hay Elastic IP, actualizarlo cada vez que cambie) |
| `EC2_USER` | `ubuntu` o `ec2-user` |
| `EC2_SSH_KEY` | el contenido completo de `labsuser.pem` (abrirlo con un editor y copiar TODO, con las líneas `-----BEGIN` y `-----END`) |
| `EC2_REPO_DIR` | (opcional) si el repo no está en `~/SISTEMAS-DISTRIBUIDOS-26-27` |

Qué hace falta en AWS: el puerto **22 abierto a `0.0.0.0/0`** (los servidores de GitHub no tienen
IP fija) y el repo ya clonado en la EC2 con `git pull` funcionando sin pedir contraseña (B4).

Seguridad: la clave `.pem` va **solo** como secreto de GitHub (cifrado). **Nunca** en el repo:
el `.gitignore` ya ignora `*.pem` y `.env`.
