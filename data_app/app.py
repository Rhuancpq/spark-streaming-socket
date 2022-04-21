from essential_generators import DocumentGenerator
import socket
import time

s = socket.socket()
s.bind(("", 9999))
s.listen(5)

print("Server is running..." + str(time.time()))

while True:
    conn, addr = s.accept()
    print("Got connection from", addr)
    gen = DocumentGenerator()
    try:
        while True:
            data = gen.sentence() + "\n"
            conn.send(data.encode())
            time.sleep(0.5)
    except:
        conn.close()
