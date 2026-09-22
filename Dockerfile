# NFCGate async relay server
FROM python:3.12-slim

WORKDIR /app

# deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app code
COPY server.py .
COPY weblog.html .
COPY plugins/ ./plugins/

# 5566 relay · 8080 health · 8000 metrics (override via NFCGATE_*_PORT / CLI)
EXPOSE 5566 8080 8000 8090

# container-internal health probe (no curl in slim image)
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD python -c "import socket,sys; s=socket.create_connection(('127.0.0.1',8080),2); s.sendall(b'GET / HTTP/1.0\r\n\r\n'); sys.exit(0 if b'200' in s.recv(64) else 1)" || exit 1

ENTRYPOINT ["python", "server.py"]
CMD []
