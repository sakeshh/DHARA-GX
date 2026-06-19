import socket
import sys

host = "datasogetiatrgacc.blob.core.windows.net"
port = 443
timeout = 3

print(f"Testing DNS resolution for {host}...")
try:
    ip = socket.gethostbyname(host)
    print(f"DNS Resolution: Success! IP is {ip}")
    
    print(f"Testing connection to {host}:{port} with timeout={timeout}s...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((ip, port))
    print("TCP connection successful!")
    s.close()
except Exception as e:
    print(f"Error: {e}")
