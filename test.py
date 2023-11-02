import socket
import msgpack

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8080  # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    payload = b"ready"

    s.sendall(len(payload).to_bytes(4, byteorder="big"))
    s.sendall(payload)

    inc_payload_size = int.from_bytes(s.recv(4), byteorder="big")

    inc_payload = msgpack.unpackb(s.recv(inc_payload_size), raw=False)
    print(inc_payload)

