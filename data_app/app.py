from essential_generators import DocumentGenerator
import socket
import time
import sys
from threading import Thread


def on_client(client_socket, addr):
    gen = DocumentGenerator()
    try:
        while True:
            data = gen.sentence() + "\n"
            client_socket.send(data.encode())
            time.sleep(0.5)
    except:
        conn.close()


if __name__ == "__main__":

    s = socket.socket()
    s.bind(("", 9999))
    s.listen(5)

    print("Server is running..." + str(time.time()))

    while True:
        try:
            conn, addr = s.accept()
            print("Got connection from", addr)
            t = Thread(target=on_client, args=(conn, addr))
            t.start()

        except KeyboardInterrupt:
            s.close()

        except:  # catch *all* exceptions
            e = sys.exc_info()[0]
            print("Error: %s" % e)
