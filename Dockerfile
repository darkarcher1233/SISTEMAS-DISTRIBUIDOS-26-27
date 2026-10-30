# Dockerfile ÚNICO para todos los módulos (todos son Python)
# Responsable: Vlad · Semana 4 · Tarjeta: "Un Dockerfile para todo + docker-compose del lab"
#
# Cada módulo NO tiene su propio Dockerfile: la misma imagen (sd-app) sirve para todos y
# solo cambia el comando, que se pone en el docker-compose, p. ej.
#   python -m ws.WM_WS_M 6000 host.docker.internal:5050 WS1 "Parque Norte"
# Prueba rápida:  docker build -t sd-app .  &&  docker run --rm sd-app

FROM python:3.12-slim

# Sin buffer: los print() salen al momento en "docker logs"
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Primero solo requirements.txt: si no cambia, Docker reutiliza la capa del pip install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Después el código (lo que no copiar está en .dockerignore)
COPY . .

# Comando por defecto; el docker-compose lo sustituye por el módulo concreto
CMD ["python", "--version"]
