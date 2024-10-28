import argparse
import asyncio
import ssl
import struct
import datetime
import logging
import sys, json
import uvloop
import signal
from prometheus_client import start_http_server, Gauge

HOST = "0.0.0.0"
PORT = 5566

# Set uvloop as the event loop policy for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Metrics
connection_count = Gauge('active_connections', 'Number of active connections')

# Configure structured logging
def setup_logging(level="INFO"):
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log(message, origin="server", tag="server", level="INFO"):
    log_message = {
        "timestamp": str(datetime.datetime.now()),
        "tag": tag,
        "origin": str(origin),
        "message": message
    }
    log_function = getattr(logging, level.lower(), logging.info)
    log_function(json.dumps(log_message))

class PluginHandler:
    def __init__(self, plugins):
        self.plugin_list = []
        for modname in plugins:
            try:
                plugin_module = __import__(f"plugins.mod_{modname}", fromlist=["plugins"])
                self.plugin_list.append((modname, plugin_module))
                logging.info(f"Loaded mod_{modname}")
            except ImportError as e:
                logging.error(f"Failed to load mod_{modname}: {e}")

    async def filter(self, client, data):
        for modname, plugin in self.plugin_list:
            try:
                data = await plugin.handle_data(lambda *x, level="INFO": client.log(*x, tag=modname, level=level), data, client.state)
            except Exception as e:
                client.log(f"Error in plugin {modname}: {e}", tag="plugin", level="ERROR")
        return data

class Client:
    def __init__(self, reader, writer, address, server, timeout=300):
        self.reader = reader
        self.writer = writer
        self.address = address
        self.state = {}
        self.server = server
        self.timeout = timeout

    def log(self, *args, tag="server", level="INFO"):
        log(" ".join(map(str, args)), origin=self.address, tag=tag, level=level)

    async def receive_data(self):
        try:
            while True:
                msg_len_data = await asyncio.wait_for(self.reader.readexactly(5), timeout=self.timeout)
                msg_len, session_id = struct.unpack("!IB", msg_len_data)
                data = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=self.timeout)
                self.log("data:", data)
                await self.server.process_data(self, session_id, data)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            self.log("disconnected due to inactivity or read error")
        except Exception as e:
            self.log(f"Error: {e}", level="ERROR")
        finally:
            self.writer.close()
            await self.writer.wait_closed()

class NFCGateServer:
    def __init__(self, host, port, plugins, tls_options=None, max_clients=100):
        self.host = host
        self.port = port
        self.plugins = PluginHandler(plugins)
        self.clients = {}
        self.tls_options = tls_options
        self.running = True
        self.semaphore = asyncio.Semaphore(max_clients)
        connection_count.set(0)

    async def handle_client(self, reader, writer):
        async with self.semaphore:
            client_address = writer.get_extra_info('peername')
            client = Client(reader, writer, client_address, self)
            client.log("connected")
            try:
                await client.receive_data()
            finally:
                self.remove_client(client)

    async def process_data(self, client, session_id, data):
        # Filter data through plugins
        filtered_data = await self.plugins.filter(client, data)

        # Session management
        if client not in self.clients.get(session_id, set()):
            self.add_client(client, session_id)

        # Broadcast data to other clients in the same session
        await self.send_to_clients(session_id, filtered_data, client)

    def add_client(self, client, session):
        connection_count.inc()
        if session not in self.clients:
            self.clients[session] = set()
        self.clients[session].add(client)
        client.log(f"joined session {session}")

    def remove_client(self, client):
        connection_count.dec()
        for session, clients in self.clients.items():
            if client in clients:
                clients.discard(client)
                client.log(f"left session {session}")
                if not clients:
                    del self.clients[session]
                break

    async def send_to_clients(self, session, data, origin):
        if session not in self.clients:
            return
        for client in self.clients[session]:
            if client is origin:
                continue
            try:
                client.writer.write(struct.pack("!I", len(data)) + data)
                await client.writer.drain()
            except (ConnectionResetError, asyncio.IncompleteReadError):
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

        health_server = await asyncio.start_server(handle_health_check, host="0.0.0.0", port=8080)
        async with health_server:
            await health_server.serve_forever()

    async def start(self):
        ssl_context = None
        if self.tls_options:
            ssl_context = self.setup_ssl_context()

        server = await asyncio.start_server(self.handle_client, self.host, self.port, ssl=ssl_context)
        addr = server.sockets[0].getsockname()
        log(f"Server running on {addr}")

        # Start health check server as a background task
        asyncio.create_task(self.health_check_server())

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown(server)))

        async with server:
            await server.serve_forever()

    def setup_ssl_context(self):
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=self.tls_options['cert_file'], keyfile=self.tls_options['key_file'])
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.set_ciphers('ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384')
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile="path/to/ca-certificates.pem")
        return context

def parse_args():
    parser = argparse.ArgumentParser(prog="NFCGate server")
    parser.add_argument("plugins", type=str, nargs="*", help="List of plugin modules to load.")
    parser.add_argument("-s", "--tls", help="Enable TLS. You must specify certificate and key.", default=False, action="store_true")
    parser.add_argument("--tls_cert", help="TLS certificate file in PEM format.", action="store")
    parser.add_argument("--tls_key", help="TLS key file in PEM format.", action="store")

    args = parser.parse_args()
    tls_options = None

    if args.tls:
        if args.tls_cert is None or args.tls_key is None:
            print("You must specify tls_cert and tls_key!")
            sys.exit(1)

        tls_options = {
            "cert_file": args.tls_cert,
            "key_file": args.tls_key
        }

    return args.plugins, tls_options

def main():
    plugins, tls_options = parse_args()
    setup_logging("DEBUG")
    server = NFCGateServer(HOST, PORT, plugins, tls_options)

    # Start the Prometheus metrics server
    start_http_server(8000)

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logging.info("Server shutting down")

if __name__ == "__main__":
    main()
