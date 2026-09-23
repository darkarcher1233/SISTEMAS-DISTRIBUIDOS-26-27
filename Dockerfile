# Dockerfile ÚNICO para todos los módulos (todos son Python)
# Responsable: Vlad · Semana 4 · Tarjeta: "Un Dockerfile para todo + docker-compose del lab"
#
# Qué debe hacer:
#   - Partir de una imagen python:3.x-slim
#   - Instalar requirements.txt
#   - Copiar el repo
# Cada módulo NO tiene su propio Dockerfile: el comando cambia en el docker-compose
# (o en Railway, en "Start Command"), p. ej.  python -m central.WM_Central 5050 kafka:9092
