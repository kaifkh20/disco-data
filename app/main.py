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
                if type(res) is list:
                     for x in res:
                          if type(x) is list:
                            resSend = x[0].encode()+x[1]
                            self.thread_conn.sendall(resSend)
                          else :self.thread_conn.sendall(x.encode())
                else:self.thread_conn.sendall(res.encode())


def handshake(masterhost,masterport,listening_port):
     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
     client_socket.connect((masterhost,int(masterport)))
     client_socket.send(RedisParser.encode.encode_array(['PING']).encode())
     client_socket.recv(1024).decode()
     client_socket.send(RedisParser.encode.encode_array(['REPLCONF','listening-port',str(listening_port)]).encode())
     client_socket.recv(1024)
     client_socket.send(RedisParser.encode.encode_array(['REPLCONF','capa','psync2']).encode())
     client_socket.recv(1024)
    #  print(RedisParser.decode.decodeSimpleString(res))
     client_socket.send(RedisParser.encode.encode_array(['PSYNC','?','-1']).encode())
    #  client_socket.recv(1024)
    #  client_socket.send(RedisParser.encode.encode_rdb().encode())

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
         handshake(masterhost,masterport,port)


    

     # wait for client

    while True:
        conn,adrr = server_socket.accept()
        thread(conn).start()
        
        


if __name__ == "__main__":
    main()
