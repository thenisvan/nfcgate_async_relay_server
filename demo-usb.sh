#!/usr/bin/env bash
# Lowest-latency demo path: NATIVE relay (no Docker proxy) + adb reverse over USB.
#
# Each phone's localhost:5566 is tunnelled over the USB cable to the host relay,
# so NFCGate traffic never touches WiFi -> near-wired latency (best shot at a
# contactless tap succeeding instead of "use chip").
#
# Usage (phones connected via USB, debugging on, rooted for the prefs step):
#     ./demo-usb.sh          # switch to native + USB
#     ./demo-usb.sh wifi      # switch back: stop native, restart Docker (WiFi)
#
# Re-runnable. Then start Relay in NFCGate on both phones (session 1).
set -u
cd "$(dirname "$0")"
PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"
PORT="${NFCGATE_PORT:-5566}"
LOG_PORT="${NFCGATE_LOG_PORT:-8090}"
PKG="de.tu_darmstadt.seemoo.nfcgate"

listening(){ "$PY" - "$1" <<'P' 2>/dev/null
import socket,sys
try: socket.create_connection(("127.0.0.1",int(sys.argv[1])),1).close()
except Exception: sys.exit(1)
P
}

# ---- wifi mode: revert to the Docker container ----
if [ "${1:-}" = "wifi" ]; then
  echo "reverting to WiFi/Docker..."
  pkill -f "server.py" 2>/dev/null
  if command -v adb >/dev/null 2>&1; then
    for s in $(adb devices | awk 'NR>1&&$2=="device"{print $1}'); do adb -s "$s" reverse --remove-all >/dev/null 2>&1; done
  fi
  docker compose up -d >/dev/null 2>&1 && echo "docker relay späť. NFCGate host nastav späť na LAN IP (compose/preflight)."
  exit 0
fi

# ---- 1) free the port: stop the Docker container ----
if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^nfcgate-relay$'; then
  echo "stopping docker container (frees :$PORT)"; docker compose down >/dev/null 2>&1 || docker stop nfcgate-relay >/dev/null 2>&1
fi

# ---- 2) start the native relay ----
if listening "$PORT"; then
  echo "relay už počúva na :$PORT"
else
  echo "spúšťam natívny relay ($PY server.py)..."
  nohup "$PY" server.py >/tmp/nfcgate_native.log 2>&1 &
  for i in 1 2 3 4 5; do listening "$PORT" && break; sleep 1; done
  listening "$PORT" && echo "  relay beží (log: /tmp/nfcgate_native.log)" || { echo "  !! relay sa nespustil, pozri /tmp/nfcgate_native.log"; exit 1; }
fi

# ---- 3) per phone: adb reverse + point NFCGate at 127.0.0.1 ----
cat > /tmp/usbseed.sh <<EOF
PKG=$PKG
DD=/data/data/\$PKG; PREF=\$DD/shared_prefs/\${PKG}_preferences.xml
mkdir -p \$DD/shared_prefs
cat > \$PREF <<XML
<?xml version='1.0' encoding='utf-8' standalone='yes' ?>
<map>
    <boolean name="network" value="true" />
    <string name="port">$PORT</string>
    <string name="host">127.0.0.1</string>
    <string name="session">1</string>
    <boolean name="tls" value="false" />
    <string name="mode">index</string>
    <string name="presence_interval">750</string>
</map>
XML
UID=\$(stat -c %u \$DD); chown \$UID:\$UID \$DD/shared_prefs \$PREF 2>/dev/null; chmod 660 \$PREF 2>/dev/null
restorecon -R \$DD/shared_prefs 2>/dev/null
am force-stop \$PKG
EOF

if command -v adb >/dev/null 2>&1; then
  devs=$(adb devices | awk 'NR>1&&$2=="device"{print $1}')
  [ -z "$devs" ] && echo "žiadny telefón cez adb — pripoj USB a spusti znova"
  for s in $devs; do
    adb -s "$s" reverse tcp:$PORT tcp:$PORT >/dev/null 2>&1 && echo "[$s] adb reverse :$PORT -> host OK"
    adb -s "$s" push /tmp/usbseed.sh /data/local/tmp/usbseed.sh >/dev/null 2>&1
    if adb -s "$s" shell su -c 'sh /data/local/tmp/usbseed.sh' >/dev/null 2>&1; then
      echo "[$s] NFCGate host=127.0.0.1:$PORT session=1 nastavené"
    else
      echo "[$s] prefs sa nepodarilo nastaviť (root/su?) — nastav v NFCGate ručne host 127.0.0.1"
    fi
  done
else
  echo "adb nenájdené"
fi

echo
echo "Hotovo. V NFCGate na oboch spusti Relay (J3=Reader, A23=Tag), session 1."
echo "Web log: http://127.0.0.1:$LOG_PORT/   (na projektor http://<LAN-IP>:$LOG_PORT/)"
echo "Späť na WiFi/Docker:  ./demo-usb.sh wifi"
