import socket
import threading
from .redisParser import RedisParser,INFO
import sys


class thread(threading.Thread):
    def __init__(self,thread_conn):
        threading.Thread.__init__(self)
        self.thread_conn = thread_conn

    def run(self):
        while self.thread_conn:
                recv = self.thread_conn.recv(1024).decode()
                # print(recv)
                res = RedisParser.decode.decodeArrays(recv)
                # print(res.encode())
                self.thread_conn.send(res.encode())

def main():
    
    args = sys.argv[1:]

    port = 6379

    if "--port" in args or "-p" in args:
        port = args[args.index("--port"or"-p")+1]
        port = int(port)

    if "--replicaof" in args:
         INFO.update({"role":"slave"})

    server_socket = socket.create_server(("localhost", port), reuse_port=True)

     # wait for client

    while True:
        conn,adrr = server_socket.accept()
        thread(conn).start()
        
        


if __name__ == "__main__":
    main()
