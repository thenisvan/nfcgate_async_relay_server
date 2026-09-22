#!/usr/bin/env bash
# Pre-flight check for the Ghost Tap demo. Verifies the whole chain in ~30-60s
# before going on stage. Read-only; starts nothing except the smoke test client.
#
#   ./preflight.sh
#
set -u
cd "$(dirname "$0")"

PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"
RELAY_PORT="${NFCGATE_PORT:-5566}"
LOG_PORT="${NFCGATE_LOG_PORT:-8090}"
METRICS_PORT="${NFCGATE_METRICS_PORT:-8000}"
HEALTH_HOST_PORT="8081"   # docker-compose maps container 8080 -> host 8081

pass=0; fail=0; warn=0
ok(){   printf '  \033[32m✓\033[0m %s\n' "$1"; pass=$((pass+1)); }
no(){   printf '  \033[31m✗\033[0m %s\n' "$1"; fail=$((fail+1)); }
wn(){   printf '  \033[33m!\033[0m %s\n' "$1"; warn=$((warn+1)); }

# quick TCP check via python (with timeout), no external nc dependency
tcp(){ "$PY" - "$1" "$2" <<'PY' 2>/dev/null
import socket,sys
try:
    socket.create_connection((sys.argv[1],int(sys.argv[2])),1.5).close();sys.exit(0)
except Exception:
    sys.exit(1)
PY
}

echo "== Ghost Tap demo pre-flight =="

# --- network ---
LAN_IP=""
for i in en0 en1 en2; do ip=$(ipconfig getifaddr $i 2>/dev/null); [ -n "$ip" ] && LAN_IP=$ip && break; done
if [ -n "$LAN_IP" ]; then ok "LAN IP: $LAN_IP  (telefóny: $LAN_IP:$RELAY_PORT, log: http://$LAN_IP:$LOG_PORT/)"
else wn "LAN IP sa nezistila (ipconfig getifaddr)"; fi

# --- docker relay ---
if command -v docker >/dev/null 2>&1; then
  st=$(docker inspect --format '{{.State.Health.Status}}' nfcgate-relay 2>/dev/null)
  if [ "$st" = "healthy" ]; then ok "Docker kontajner nfcgate-relay: healthy"
  elif [ -n "$st" ]; then wn "Docker kontajner beží, health=$st"
  else no "Docker kontajner nfcgate-relay nebeží (docker compose up -d --build)"; fi
else wn "docker CLI nenájdené"; fi

# --- relay / web / metrics ---
tcp 127.0.0.1 "$RELAY_PORT" && ok "Relay TCP $RELAY_PORT počúva" || no "Relay TCP $RELAY_PORT neodpovedá"
code=$(curl -s --max-time 3 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$LOG_PORT/" 2>/dev/null)
[ "$code" = "200" ] && ok "Web log :$LOG_PORT (HTTP 200)" || no "Web log :$LOG_PORT nedostupný (HTTP ${code:-?})"
ac=$(curl -s --max-time 3 "http://127.0.0.1:$METRICS_PORT/" 2>/dev/null | awk '/^active_connections/{print $2}')
if [ -n "$ac" ]; then
  ok "Metriky :$METRICS_PORT (active_connections=$ac)"
  case "$ac" in -*) wn "active_connections je záporné ($ac) — pozri stray proces";; esac
else wn "Metriky :$METRICS_PORT nedostupné"; fi

# --- smoke test (relay round-trip) ---
if [ -f test.py ]; then
  if "$PY" test.py 127.0.0.1 "$RELAY_PORT" >/tmp/preflight_smoke.log 2>&1; then ok "Smoke test relayu prešiel (test.py)"
  else no "Smoke test zlyhal — pozri /tmp/preflight_smoke.log"; fi
fi

# --- phones (adb) ---
if command -v adb >/dev/null 2>&1; then
  devs=$(adb devices 2>/dev/null | awk 'NR>1 && $2=="device"{print $1}')
  n=$(printf '%s\n' "$devs" | grep -c . )
  if [ "$n" -ge 2 ]; then ok "Pripojené telefóny: $n"
  elif [ "$n" -eq 1 ]; then wn "Pripojený len 1 telefón (relay potrebuje 2)"
  else no "Žiadny telefón cez adb"; fi
  for s in $devs; do
    mdl=$(adb -s "$s" shell getprop ro.product.model 2>/dev/null | tr -d '\r')
    rel=$(adb -s "$s" shell getprop ro.build.version.release 2>/dev/null | tr -d '\r')
    nfc=$(adb -s "$s" shell dumpsys nfc 2>/dev/null | grep -oE 'mState=[a-z]+' | head -1 | tr -d '\r')
    has=$(adb -s "$s" shell pm list packages 2>/dev/null | grep -c nfcgate | tr -d '\r')
    tag="$mdl (Android $rel): $nfc, NFCGate=$([ "$has" -ge 1 ] && echo áno || echo NIE)"
    if [ "$nfc" = "mState=on" ] && [ "$has" -ge 1 ]; then ok "$tag"; else wn "$tag"; fi
  done
else wn "adb nenájdené — telefóny neoverené"; fi

echo "== hotovo: ${pass} OK, ${warn} upozornení, ${fail} chýb =="
[ "$fail" -eq 0 ]
