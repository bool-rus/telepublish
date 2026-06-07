#!/usr/bin/env bash
set -euo pipefail

echo "==> Остановка MinIO..."
if [ -f /var/run/minio.pid ]; then
  kill "$(cat /var/run/minio.pid)" 2>/dev/null || true
  sudo rm -f /var/run/minio.pid
fi
pkill -x minio 2>/dev/null || true

echo "==> Удаление MinIO..."
sudo rm -f /usr/local/bin/minio /usr/local/bin/mc

echo "==> Удаление данных..."
sudo rm -rf /data/minio

echo "==> Очистка nginx..."
sudo rm -f /etc/nginx/sites-available/telepublish
sudo rm -f /etc/nginx/sites-enabled/telepublish
sudo rm -rf /var/www/telepublish
sudo nginx -t && sudo systemctl reload nginx || true

echo "Готово. Всё чисто."
