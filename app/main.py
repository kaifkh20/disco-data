import socket
import threading
from .redisParser import RedisParser,INFO
import sys
import struct


class thread(threading.Thread):
    def __init__(self,thread_conn):
        threading.Thread.__init__(self)
        self.thread_conn = thread_conn

    def run(self):
        while self.thread_conn:
                recv = self.thread_conn.recv(1024).decode()
                # print(recv)
                res = RedisParser.decode.decodeArrays(recv)
                self.thread_conn.sendall(res.encode())


def handshake(masterhost,masterport):
     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
     client_socket.connect((masterhost,int(masterport)))
     client_socket.send(RedisParser.encode.encode_array(['PING']).encode())


def main():
    
    args = sys.argv[1:]

    port = 6379

    if "--port" in args or "-p" in args:
        port = args[args.index("--port"or"-p")+1]
        port = int(port)

    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    if "--replicaof" in args:
         INFO.update({"role":"slave"})
         masterhost = args[args.index("--replicaof")+1]
         masterport = args[args.index("--replicaof")+2]
         handshake(masterhost,masterport)


    

     # wait for client

    while True:
        conn,adrr = server_socket.accept()
        thread(conn).start()
        
        


if __name__ == "__main__":
    main()
