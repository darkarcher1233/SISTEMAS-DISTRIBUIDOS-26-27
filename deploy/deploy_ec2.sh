#!/usr/bin/env bash
# deploy_ec2.sh: despliega (o actualiza) la parte cloud en la EC2: Kafka + WM_Central + SQLite
#
# Uso en la EC2, dentro del repo:   bash deploy/deploy_ec2.sh
# Lo mismo lo ejecuta GitHub Actions por SSH (.github/workflows/deploy.yml): CI/CD GitHub -> AWS.
#
# Pasos: 1) git pull   2) PUBLIC_IP en .env   3) docker compose up -d --build   4) estado
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker-compose.cloud.yml"

echo "== 1/4 Código: git pull"
git pull --ff-only

echo "== 2/4 IP pública de esta EC2"
# Servicio de metadatos de AWS (IMDSv2): solo responde desde dentro de la propia EC2.
# En el Learner Lab la IP pública CAMBIA cada vez que se arranca el laboratorio: por eso se lee
# aquí en cada despliegue en vez de dejarla escrita a mano.
TOKEN=$(curl -s -m 3 -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
IP=$(curl -s -m 3 -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/public-ipv4" || true)
touch .env
if [[ "$IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  if grep -q '^PUBLIC_IP=' .env; then
    sed -i "s/^PUBLIC_IP=.*/PUBLIC_IP=$IP/" .env
  else
    echo "PUBLIC_IP=$IP" >> .env
  fi
  echo "PUBLIC_IP=$IP"
else
  echo "No se pudo leer la IP de los metadatos; se usa la del .env:"
  grep '^PUBLIC_IP=' .env || { echo "ERROR: falta PUBLIC_IP en .env"; exit 1; }
fi

echo "== 3/4 docker compose up -d --build"
$COMPOSE up -d --build

echo "== 4/4 Estado"
sleep 5
$COMPOSE ps
IP_FINAL=$(grep '^PUBLIC_IP=' .env | cut -d= -f2 || true)
PUERTO_PANEL=$(grep '^PANEL_PORT=' .env | cut -d= -f2 || true)
echo
echo "Panel de Central:   http://$IP_FINAL:${PUERTO_PANEL:-8080}"
echo "Para los PCs del laboratorio (.env):  KAFKA_BROKER=$IP_FINAL:9092   CENTRAL=$IP_FINAL:5050"
