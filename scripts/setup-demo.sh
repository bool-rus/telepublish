#!/usr/bin/env bash
set -euo pipefail

MINIO_DIR=/usr/local/bin
DATA_DIR=/data/minio

echo "==> Скачивание MinIO server..."
sudo mkdir -p "$DATA_DIR"
sudo wget -q -O "$MINIO_DIR/minio" https://dl.min.io/server/minio/release/linux-amd64/minio
sudo chmod +x "$MINIO_DIR/minio"

echo "==> Запуск MinIO..."
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
nohup "$MINIO_DIR/minio" server "$DATA_DIR" --console-address ":9001" > /tmp/minio.log 2>&1 &
MINIO_PID=$!
echo "$MINIO_PID" | sudo tee /var/run/minio.pid > /dev/null
echo "MinIO PID: $MINIO_PID"

echo "==> Ожидание MinIO..."
for i in $(seq 1 30); do
  if curl -s http://127.0.0.1:9000/minio/health/live > /dev/null 2>&1; then
    echo "MinIO готов"
    break
  fi
  sleep 1
done

echo "==> Скачивание mc..."
sudo wget -q -O /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
sudo chmod +x /usr/local/bin/mc

echo "==> Настройка mc alias..."
mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "==> Создание bucket 'bulletins'..."
mc mb local/bulletins --ignore-existing

echo "==> Настройка nginx..."
sudo mkdir -p /var/www/telepublish
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
sudo cp "$SCRIPT_DIR/../html/index.html" /var/www/telepublish/
sudo cp "$SCRIPT_DIR/../nginx/telepublish.conf" /etc/nginx/sites-available/telepublish
sudo ln -sf /etc/nginx/sites-available/telepublish /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx || true

echo ""
echo "Готово. MinIO на http://127.0.0.1:9000 (console: :9001)"
echo "Bucket: bulletins"
echo ""
echo "Запусти сервис:"
echo "  DB_URL=sqlite:./demo.db \\"
echo "  S3_ENDPOINT=http://127.0.0.1:9000 \\"
echo "  S3_BUCKET=bulletins \\"
echo "  S3_ACCESS_KEY=$MINIO_ROOT_USER \\"
echo "  S3_SECRET_KEY=$MINIO_ROOT_PASSWORD \\"
echo "  cargo run"
echo ""
echo "Открой браузер: http://localhost"
