import argparse
import asyncio
import ssl
import struct
import datetime
import logging
import sys, json, collections, time, ipaddress, socket
import uvloop
import signal
from prometheus_client import start_http_server, Gauge

import os

# Optional protobuf decoding of relayed frames (for the web log view).
# The relay itself never depends on this — decoding is best-effort.
try:
    from plugins import c2c_pb2, c2s_pb2
    _PROTO_OK = True
except Exception:  # pragma: no cover - decoding is optional
    _PROTO_OK = False


def _env_int(name, default):
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logging.warning(f"Invalid int in env {name}={val!r}, using default {default}")
        return default


# Defaults (overridable via env or CLI). Set a *_PORT to 0 to disable that listener.
DEFAULT_HOST = os.environ.get("NFCGATE_HOST", "0.0.0.0")
DEFAULT_PORT = _env_int("NFCGATE_PORT", 5566)
DEFAULT_HEALTH_PORT = _env_int("NFCGATE_HEALTH_PORT", 8080)
DEFAULT_METRICS_PORT = _env_int("NFCGATE_METRICS_PORT", 8000)
DEFAULT_LOG_PORT = _env_int("NFCGATE_LOG_PORT", 8090)
DEFAULT_DELAY_MS = _env_int("NFCGATE_DELAY_MS", 0)

# Set uvloop as the event loop policy for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Metrics
connection_count = Gauge('active_connections', 'Number of active connections')


def _now():
    return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]


class LogHub:
    """In-memory ring buffer + pub/sub feeding the HTTP log service.

    Events are plain dicts. Two kinds:
      {"t": "frame", ...}  one relayed APDU (decoded when possible)
      {"t": "log", ...}    a system log line
    """
    def __init__(self, maxlen=1000):
        self.buffer = collections.deque(maxlen=maxlen)
        self.subscribers = set()

    def publish(self, event):
        self.buffer.append(event)
        for q in list(self.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def subscribe(self):
        q = asyncio.Queue(maxsize=2000)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q):
        self.subscribers.discard(q)


log_hub = LogHub()


class _HubHandler(logging.Handler):
    """Feeds every log record into the LogHub as a {"t":"log"} event."""
    def emit(self, record):
        try:
            log_hub.publish({
                "t": "log",
                "ts": _now(),
                "level": record.levelname,
                "msg": record.getMessage(),
            })
        except Exception:
            pass


def decode_frame(payload):
    """Best-effort decode of a relayed ServerData/NFCData frame.
    Returns a dict with any of {op, src, hex}; empty dict on failure."""
    out = {}
    if not _PROTO_OK or not payload:
        return out
    try:
        sd = c2s_pb2.ServerData()
        sd.ParseFromString(payload)
        try:
            out["op"] = c2s_pb2.ServerData.Opcode.Name(sd.opcode)
        except Exception:
            out["op"] = str(sd.opcode)
        if sd.data:
            nd = c2c_pb2.NFCData()
            nd.ParseFromString(sd.data)
            out["src"] = "CARD" if nd.data_source == c2c_pb2.NFCData.CARD else "READER"
            out["hex"] = bytes(nd.data).hex()
            out["dtp"] = "INITIAL" if nd.data_type == c2c_pb2.NFCData.INITIAL else "CONT"
    except Exception:
        pass
    return out


# The web log page (served at GET / on the log port) lives in weblog.html
_PAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weblog.html")
def _load_page():
    try:
        with open(_PAGE_PATH, encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logging.warning(f"weblog.html not found ({e}); serving minimal page")
        return "<!doctype html><meta charset=utf-8><title>NFCGate log</title><body style='background:#141414;color:#eee;font-family:sans-serif'>weblog.html missing on server</body>"
LOG_PAGE = _load_page()


def _list_captures():
    try:
        d = "captures"
        return sorted(f for f in os.listdir(d) if f.endswith(".jsonl")) if os.path.isdir(d) else []
    except Exception:
        return []


# Configure structured logging
def setup_logging(level="INFO"):
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    hub_handler = _HubHandler()
    logger.addHandler(hub_handler)


def log(message, origin="server", tag="server", level="INFO"):
    log_message = {
        "timestamp": str(datetime.datetime.now()),
        "tag": tag,
        "origin": str(origin),
        "message": message
    }
    log_function = getattr(logging, level.lower(), logging.info)
    log_function(json.dumps(log_message))


# Plugins known to the web portal (loaded at start; toggle/config at runtime).
KNOWN_PLUGINS = ["modify", "log"]

class PluginHandler:
    def __init__(self, plugins):
        self.plugins = {}     # name -> module (ordered: filter runs in this order)
        self.enabled = {}     # name -> bool
        for modname in list(dict.fromkeys(list(KNOWN_PLUGINS) + list(plugins))):
            try:
                mod = __import__(f"plugins.mod_{modname}", fromlist=["plugins"])
                self.plugins[modname] = mod
                self.enabled[modname] = modname in plugins   # on only if requested on CLI
                logging.info(f"Loaded mod_{modname} (enabled={self.enabled[modname]})")
            except ImportError as e:
                logging.error(f"Failed to load mod_{modname}: {e}")

    async def filter(self, client, data):
        for name, mod in self.plugins.items():
            if not self.enabled.get(name):
                continue
            try:
                data = await mod.handle_data(lambda *x, level="INFO": client.log(*x, tag=name, level=level), data, client.state)
            except Exception as e:
                client.log(f"Error in plugin {name}: {e}", tag="plugin", level="ERROR")
        return data

    def list(self):
        out = []
        for name, mod in self.plugins.items():
            info = {"name": name, "enabled": bool(self.enabled.get(name))}
            if hasattr(mod, "describe"):
                try:
                    info.update(mod.describe() or {})
                except Exception:
                    pass
            out.append(info)
        return out

    def toggle(self, name, on):
        if name in self.plugins:
            self.enabled[name] = bool(on)
            return True
        return False

    def configure(self, name, data):
        mod = self.plugins.get(name)
        if mod is not None and hasattr(mod, "configure"):
            try:
                mod.configure(data or {})
                return True
            except Exception as e:
                logging.warning(f"configure({name}) failed: {e}")
        return False


class Client:
    def __init__(self, reader, writer, address, server, timeout=300):
        self.reader = reader
        self.writer = writer
        self.address = address
        self.state = {}
        self.server = server
        self.timeout = timeout
        # Serialize writes to this client so concurrent relays (in sessions with
        # more than 2 peers) cannot interleave frames. Async port of upstream
        # nfcgate/server commit eaaf5e7 (threading.Lock -> asyncio.Lock).
        self.write_lock = asyncio.Lock()

    def log(self, *args, tag="server", level="INFO"):
        log(" ".join(map(str, args)), origin=self.address, tag=tag, level=level)

    async def receive_data(self):
        try:
            while True:
                msg_len_data = await asyncio.wait_for(self.reader.readexactly(5), timeout=self.timeout)
                msg_len, session_id = struct.unpack("!IB", msg_len_data)
                data = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=self.timeout)
                # raw bytes only at DEBUG (structured frame goes to the web log instead)
                self.log("data:", data, level="DEBUG")
                await self.server.process_data(self, session_id, data)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            self.log("disconnected due to inactivity or read error")
        except Exception as e:
            self.log(f"Error: {e}", level="ERROR")
        finally:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass


class NFCGateServer:
    def __init__(self, host, port, plugins, tls_options=None, max_clients=100, health_port=DEFAULT_HEALTH_PORT, log_port=DEFAULT_LOG_PORT, max_peers=2, allow_nets=None, delay_ms=0, record_path=None, replay_path=None, replay_loop=False):
        self.host = host
        self.port = port
        self.health_port = health_port
        self.log_port = log_port
        self.max_peers = max_peers            # max clients per session (0 = unlimited)
        self.allow_nets = allow_nets or []    # list of ip_network; empty = allow all
        self.delay_ms = delay_ms              # artificial relay latency (ms), for demoing timing defenses
        self.replay_path = replay_path
        self.replay_loop = replay_loop
        self._rec = open(record_path, "a", buffering=1) if record_path else None
        self.plugins = PluginHandler(plugins)
        self.clients = {}
        self._last_frame_ts = {}              # session -> monotonic ts of last frame
        self.tls_options = tls_options
        self.running = True
        self.semaphore = asyncio.Semaphore(max_clients)
        connection_count.set(0)

    def _allowed(self, addr):
        if not self.allow_nets:
            return True
        try:
            ip = ipaddress.ip_address(addr[0])
        except Exception:
            return False
        return any(ip in net for net in self.allow_nets)

    async def handle_client(self, reader, writer):
        async with self.semaphore:
            client_address = writer.get_extra_info('peername')
            if not self._allowed(client_address):
                log(f"rejected {client_address}: not in allowlist", tag="acl", level="WARNING")
                writer.close()
                await writer.wait_closed()
                return
            # Disable Nagle: relayed APDU frames are tiny, so Nagle + delayed-ACK
            # can add up to ~40 ms per round-trip. TCP_NODELAY sends them immediately.
            _sock = writer.get_extra_info("socket")
            if _sock is not None:
                try:
                    _sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
            client = Client(reader, writer, client_address, self)
            # gauge tracks live TCP connections (one inc per socket, one dec on close)
            connection_count.inc()
            client.log("connected")
            try:
                await client.receive_data()
            finally:
                self.remove_client(client)
                connection_count.dec()

    def _publish_frame(self, session_id, payload):
        now = time.monotonic()
        prev = self._last_frame_ts.get(session_id)
        self._last_frame_ts[session_id] = now
        ev = {"t": "frame", "ts": _now(), "ms": int(time.time() * 1000), "session": session_id, "n": len(payload)}
        if prev is not None:
            ev["dt"] = round((now - prev) * 1000, 1)   # ms since previous frame in this session
        if self.delay_ms:
            ev["delay"] = self.delay_ms
        ev.update(decode_frame(payload))
        log_hub.publish(ev)
        if self._rec:
            try:
                self._rec.write(json.dumps(ev) + "\n")
            except Exception:
                pass

    async def replay_frames(self, path, loop=False):
        try:
            with open(path) as f:
                events = [json.loads(l) for l in f if l.strip()]
        except (OSError, ValueError) as e:
            log(f"Replay: cannot read {path}: {e}", tag="replay", level="ERROR")
            return
        events = [e for e in events if e.get("t") == "frame"]
        if not events:
            log(f"Replay: no frames in {path}", tag="replay", level="WARNING")
            return
        log(f"Replay: {len(events)} frames from {path}" + (" (loop)" if loop else ""), tag="replay")
        while True:
            prev_ms = None
            for ev in events:
                ms = ev.get("ms")
                if prev_ms is not None and ms is not None:
                    await asyncio.sleep(min(max((ms - prev_ms) / 1000.0, 0), 2.0))
                prev_ms = ms
                out = dict(ev)
                out["ts"] = _now()
                out["replay"] = True
                log_hub.publish(out)
            if not loop:
                break
            await asyncio.sleep(1.5)
        log("Replay: finished", tag="replay")

    async def process_data(self, client, session_id, data):
        # Filter data through plugins
        filtered_data = await self.plugins.filter(client, data)

        # Session management + peer limit (default 2: reader + card only)
        members = self.clients.get(session_id)
        if client not in (members or ()):
            if self.max_peers and members and len(members) >= self.max_peers:
                client.log(f"session {session_id} full ({self.max_peers} peers), rejecting join", tag="session", level="WARNING")
                return  # do not add, do not relay this outsider's frames
            self.add_client(client, session_id)

        # Structured event for the web log view (decoded best-effort)
        self._publish_frame(session_id, filtered_data)

        # Broadcast data to other clients in the same session
        await self.send_to_clients(session_id, filtered_data, client)

    def add_client(self, client, session):
        if session not in self.clients:
            self.clients[session] = set()
        self.clients[session].add(client)
        client.log(f"joined session {session}")

    def remove_client(self, client):
        for session, clients in self.clients.items():
            if client in clients:
                clients.discard(client)
                client.log(f"left session {session}")
                if not clients:
                    del self.clients[session]
                    self._last_frame_ts.pop(session, None)
                break

    async def send_to_clients(self, session, data, origin):
        if session not in self.clients:
            return
        if self.delay_ms:
            await asyncio.sleep(self.delay_ms / 1000.0)   # simulate relay/WAN latency
        for client in list(self.clients[session]):
            if client is origin:
                continue
            try:
                async with client.write_lock:
                    client.writer.write(struct.pack("!I", len(data)) + data)
                    await client.writer.drain()
            except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError, OSError):
                self.remove_client(client)
        log(f"Publish reached {len(self.clients.get(session, []))} clients", tag="broadcast")

    async def shutdown(self, server):
        if self.running:
            log("Server is shutting down...")
            self.running = False
            server.close()
            await server.wait_closed()
            for session, clients in self.clients.items():
                for client in list(clients):
                    try:
                        client.writer.close()
                        await client.writer.wait_closed()
                    except Exception as e:
                        log(f"Error closing client connection: {e}", tag="shutdown")
            log("All connections closed, server shut down.")

    async def health_check_server(self):
        async def handle_health_check(reader, writer):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nOK")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        try:
            health_server = await asyncio.start_server(handle_health_check, host=self.host, port=self.health_port)
        except OSError as e:
            log(f"Health check disabled: cannot bind port {self.health_port}: {e}", tag="health", level="WARNING")
            return
        addr = health_server.sockets[0].getsockname()
        log(f"Health check listening on {addr}", tag="health")
        async with health_server:
            await health_server.serve_forever()

    def _control(self, path, payload):
        parts = path.strip("/").split("/")   # e.g. api/plugins/modify/toggle
        if len(parts) == 4 and parts[1] == "plugins":
            name, action = parts[2], parts[3]
            if action == "toggle":
                ok = self.plugins.toggle(name, payload.get("enabled", True))
                log(f"plugin {name} enabled={payload.get('enabled', True)} via portal", tag="control")
                return {"ok": ok, "plugins": self.plugins.list()}
            if action == "config":
                ok = self.plugins.configure(name, payload)
                log(f"plugin {name} reconfigured via portal", tag="control")
                return {"ok": ok, "plugins": self.plugins.list()}
        if len(parts) == 3 and parts[1] == "control":
            if parts[2] == "delay":
                try:
                    self.delay_ms = max(0, int(payload.get("ms", 0)))
                except (TypeError, ValueError):
                    pass
                log(f"artificial delay set to {self.delay_ms} ms via portal", tag="control")
                return {"ok": True, "delay_ms": self.delay_ms}
            if parts[2] == "replay":
                fp = os.path.join("captures", os.path.basename(payload.get("file", "")))
                if payload.get("file") and os.path.exists(fp):
                    asyncio.create_task(self.replay_frames(fp, bool(payload.get("loop"))))
                    return {"ok": True, "replaying": fp}
                return {"ok": False, "error": "capture not found"}
        return {"ok": False, "error": "unknown endpoint"}

    async def log_http_server(self):
        async def handle(reader, writer):
            try:
                request_line = await reader.readline()
                clen = 0
                while True:
                    hdr = await reader.readline()
                    if hdr in (b"\r\n", b"\n", b""):
                        break
                    if hdr.lower().startswith(b"content-length:"):
                        try:
                            clen = int(hdr.split(b":", 1)[1].strip())
                        except ValueError:
                            clen = 0
                parts = request_line.split()
                method = parts[0].decode("latin-1") if parts else "GET"
                path = parts[1].decode("latin-1") if len(parts) >= 2 else "/"
                body_raw = await reader.readexactly(clen) if clen > 0 else b""
                peer = writer.get_extra_info("peername")
                loopback = bool(peer) and str(peer[0]) in ("127.0.0.1", "::1")

                def send_json(obj, code="200 OK"):
                    b = json.dumps(obj).encode("utf-8")
                    writer.write(("HTTP/1.1 %s\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" % code).encode() + b)

                if path.startswith("/stream"):
                    await self._log_stream(writer)
                    return
                if path == "/logs":
                    send_json(list(log_hub.buffer))
                elif path == "/api/plugins" and method == "GET":
                    send_json({"plugins": self.plugins.list()})
                elif path == "/api/control" and method == "GET":
                    send_json({"delay_ms": self.delay_ms, "captures": _list_captures()})
                elif path.startswith("/api/") and method == "POST":
                    if not loopback:
                        send_json({"error": "control allowed only from localhost"}, "403 Forbidden")
                    else:
                        try:
                            payload = json.loads(body_raw or b"{}")
                        except Exception:
                            payload = {}
                        send_json(self._control(path, payload))
                else:
                    body = _load_page().encode("utf-8")   # re-read so weblog.html edits show on refresh
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n" + body)
                await writer.drain()
            except Exception as e:
                log(f"log http error: {e}", tag="logsvc", level="ERROR")
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        try:
            srv = await asyncio.start_server(handle, host=self.host, port=self.log_port)
        except OSError as e:
            log(f"Log service disabled: cannot bind port {self.log_port}: {e}", tag="logsvc", level="WARNING")
            return
        addr = srv.sockets[0].getsockname()
        log(f"Log service (web) on http://{addr[0]}:{addr[1]}/", tag="logsvc")
        async with srv:
            await srv.serve_forever()

    async def _log_stream(self, writer):
        q = log_hub.subscribe()

        def sse(ev):
            return b"data: " + json.dumps(ev).encode("utf-8", "replace") + b"\n\n"

        try:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\nConnection: keep-alive\r\n\r\n")
            for ev in list(log_hub.buffer):
                writer.write(sse(ev))
            await writer.drain()
            while True:
                ev = await q.get()
                writer.write(sse(ev))
                await writer.drain()
        except Exception:
            # a log viewer disconnecting is normal — never surface it as an error
            pass
        finally:
            log_hub.unsubscribe(q)
            try:
                writer.close()
            except Exception:
                pass

    async def start(self):
        ssl_context = None
        if self.tls_options:
            ssl_context = self.setup_ssl_context()

        server = await asyncio.start_server(self.handle_client, self.host, self.port, ssl=ssl_context)
        addr = server.sockets[0].getsockname()
        log(f"Server running on {addr}")

        # Start health check server as a background task (health_port=0 disables it)
        if self.health_port:
            asyncio.create_task(self.health_check_server())

        # Start web log service (log_port=0 disables it)
        if self.log_port:
            asyncio.create_task(self.log_http_server())

        # Replay a recorded capture into the web log (no hardware needed)
        if self.replay_path:
            asyncio.create_task(self.replay_frames(self.replay_path, self.replay_loop))

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown(server)))

        async with server:
            await server.serve_forever()

    def setup_ssl_context(self):
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=self.tls_options['cert_file'], keyfile=self.tls_options['key_file'])
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        # Client-certificate auth only when a CA is supplied; otherwise server-side TLS only.
        ca_file = self.tls_options.get('ca_file')
        if ca_file:
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=ca_file)
        else:
            context.verify_mode = ssl.CERT_NONE
        return context


def parse_args():
    parser = argparse.ArgumentParser(prog="NFCGate server")
    parser.add_argument("plugins", type=str, nargs="*", help="List of plugin modules to load.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Bind address (default {DEFAULT_HOST}, env NFCGATE_HOST).")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help=f"Relay TCP port (default {DEFAULT_PORT}, env NFCGATE_PORT).")
    parser.add_argument("--health-port", type=int, default=DEFAULT_HEALTH_PORT, help=f"Health-check HTTP port, 0 disables (default {DEFAULT_HEALTH_PORT}, env NFCGATE_HEALTH_PORT).")
    parser.add_argument("--metrics-port", type=int, default=DEFAULT_METRICS_PORT, help=f"Prometheus metrics port, 0 disables (default {DEFAULT_METRICS_PORT}, env NFCGATE_METRICS_PORT).")
    parser.add_argument("--log-port", type=int, default=DEFAULT_LOG_PORT, help=f"Web log service port (live APDU stream), 0 disables (default {DEFAULT_LOG_PORT}, env NFCGATE_LOG_PORT).")
    parser.add_argument("--max-peers", type=int, default=_env_int("NFCGATE_MAX_PEERS", 2), help="Max clients per session, 0 = unlimited (default 2 = reader+card, env NFCGATE_MAX_PEERS).")
    parser.add_argument("--allow", default=os.environ.get("NFCGATE_ALLOW", ""), help="Comma-separated IP/CIDR allowlist for incoming connections (default: allow all, env NFCGATE_ALLOW).")
    parser.add_argument("--delay-ms", type=int, default=DEFAULT_DELAY_MS, help="Artificial relay latency in ms added to every relayed frame, to demo timing-based defenses (default 0, env NFCGATE_DELAY_MS).")
    parser.add_argument("--record", default=os.environ.get("NFCGATE_RECORD", ""), help="Append decoded frames to this JSONL file for later replay.")
    parser.add_argument("--replay", default="", help="Replay a JSONL capture into the web log on startup (no hardware needed).")
    parser.add_argument("--replay-loop", action="store_true", help="Loop the --replay capture continuously.")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging, includes raw relayed frames on the console.")
    parser.add_argument("-s", "--tls", help="Enable TLS. You must specify certificate and key.", default=False, action="store_true")
    parser.add_argument("--tls_cert", help="TLS certificate file in PEM format.", action="store")
    parser.add_argument("--tls_key", help="TLS key file in PEM format.", action="store")
    parser.add_argument("--tls_ca", help="CA file (PEM) to require & verify client certificates (mutual TLS). Omit for server-side TLS only.", action="store")

    args = parser.parse_args()
    tls_options = None

    if args.tls:
        if args.tls_cert is None or args.tls_key is None:
            print("You must specify tls_cert and tls_key!")
            sys.exit(1)

        tls_options = {
            "cert_file": args.tls_cert,
            "key_file": args.tls_key,
            "ca_file": args.tls_ca,
        }

    return args, tls_options


def main():
    args, tls_options = parse_args()
    setup_logging("DEBUG" if args.verbose else "INFO")
    allow_nets = []
    for item in (args.allow or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            allow_nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            logging.warning(f"Ignoring invalid --allow entry: {item!r}")
    server = NFCGateServer(args.host, args.port, args.plugins, tls_options,
                           health_port=args.health_port, log_port=args.log_port,
                           max_peers=args.max_peers, allow_nets=allow_nets,
                           delay_ms=args.delay_ms, record_path=(args.record or None),
                           replay_path=(args.replay or None), replay_loop=args.replay_loop)
    if allow_nets:
        logging.info(f"Connection allowlist: {[str(n) for n in allow_nets]}")
    logging.info(f"Max peers per session: {args.max_peers or 'unlimited'}")
    if args.delay_ms:
        logging.info(f"Artificial relay latency: {args.delay_ms} ms")
    if args.record:
        logging.info(f"Recording frames to: {args.record}")
    if args.replay:
        logging.info(f"Replaying capture: {args.replay}" + (" (loop)" if args.replay_loop else ""))

    # Start the Prometheus metrics server (metrics_port=0 disables it)
    if args.metrics_port:
        try:
            start_http_server(args.metrics_port)
            logging.info(f"Prometheus metrics on :{args.metrics_port}")
        except OSError as e:
            logging.warning(f"Metrics disabled: cannot bind port {args.metrics_port}: {e}")

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logging.info("Server shutting down")


if __name__ == "__main__":
    main()
