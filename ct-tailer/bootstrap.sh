#!/usr/bin/env bash
# bootstrap.sh — one-shot installer for Oracle Cloud Free ARM VM (Ubuntu 22.04/24.04).
#
# Usage on a fresh VM:
#   sudo bash bootstrap.sh
#
# Then edit /etc/ct-tailer.env with your R2 credentials and:
#   sudo systemctl restart ct-tailer && journalctl -u ct-tailer -f

set -euo pipefail

echo "[1/6] apt update + node.js 20"
apt-get update -qq
apt-get install -y curl git build-essential ca-certificates
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs
node -v && npm -v

echo "[2/6] create ct user + dirs"
id ct &>/dev/null || useradd -r -s /usr/sbin/nologin -d /opt/ct-tailer ct
mkdir -p /opt/ct-tailer /opt/ct-tailer/state /var/log
touch /var/log/ct-tailer.log && chown ct:ct /var/log/ct-tailer.log

echo "[3/6] pull ct-tailer code"
cd /opt/ct-tailer
if [ ! -d .git ]; then
  git clone --depth 1 https://github.com/seanblatter/lettuce-discovery-ingest.git repo
  cp -r repo/ct-tailer/* .
  rm -rf repo
fi
chown -R ct:ct /opt/ct-tailer

echo "[4/6] npm install"
sudo -u ct npm install --omit=dev --prefer-offline --no-audit

echo "[5/6] install env file (edit before starting)"
if [ ! -f /etc/ct-tailer.env ]; then
  cat > /etc/ct-tailer.env <<'ENV'
# --- fill these in ---
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET=lettuce-discovery
R2_PREFIX=discovery/
# --- tuning ---
CT_STATE_DIR=/opt/ct-tailer/state
CT_BATCH_SIZE=100000
CT_MAX_CONC=6
NODE_OPTIONS=--max-old-space-size=3072
ENV
  chmod 600 /etc/ct-tailer.env
  echo "  → edit /etc/ct-tailer.env before starting"
fi

echo "[6/6] systemd unit"
cp ct-tailer.service /etc/systemd/system/ct-tailer.service
systemctl daemon-reload
systemctl enable ct-tailer

# Log rotation
cat > /etc/logrotate.d/ct-tailer <<'LOGR'
/var/log/ct-tailer.log {
  daily
  rotate 7
  compress
  missingok
  notifempty
  copytruncate
}
LOGR

echo
echo "✅ install complete."
echo "Next steps:"
echo "  1) sudo nano /etc/ct-tailer.env    (fill R2 creds)"
echo "  2) sudo systemctl start ct-tailer"
echo "  3) sudo journalctl -u ct-tailer -f"
