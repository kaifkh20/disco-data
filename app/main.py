import socket
import sys
import threading

from .redisParser import INFO, RedisParser

replicas_addr = []
replica_true = False
buffered_commands_to_replicate = []


class thread(threading.Thread):
    def __init__(self, thread_conn):
        threading.Thread.__init__(self)
        self.thread_conn = thread_conn

    def run(self):
        global replica_true
        global replicas_addr
        global buffered_commands_to_replicate
        is_connection_replicate = False
        while self.thread_conn:
            recv = self.thread_conn.recv(1024).decode()
            # print(recv)
            cmnd = RedisParser.decode.decodeOnlyCommand(recv)
            res = RedisParser.decode.decodeArrays(recv)
            # if replica_true==True and cmnd=='SET':
            #     # while replica_true==True and cmnd=='SET':
            #     buffered_commands_to_replicate.append(recv)
            #     # self.thread_conn.sendall(res.encode())
            if type(res) is list:
                for x in res:
                    if type(x) is list:
                        resSend = x[0].encode() + x[1]
                        self.thread_conn.sendall(resSend)
                        replicas_addr.append(self.thread_conn)
                        replica_true = True
                        is_connection_replicate = True
                    else:
                        self.thread_conn.sendall(x.encode())
            else:
                if not is_connection_replicate and cmnd=='SET' or cmnd=='GET':
                    for conn in replicas_addr:
                        response = RedisParser.decode.decodeArrays(recv, True)
                        print("sent response")
                        if cmnd=='GET':
                            print(response)
                        conn.sendall(response.encode())
                self.thread_conn.sendall(res.encode())
                # self.thread_conn.close()




def handshake(masterhost, masterport, listening_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((masterhost, int(masterport)))
    print("connection established")
    client_socket.send(RedisParser.encode.encode_array(["PING"]).encode())
    client_socket.recv(1024)
    # print(recv)
    client_socket.send(
        RedisParser.encode.encode_array(
            ["REPLCONF", "listening-port", str(listening_port)]
        ).encode()
    )
    client_socket.recv(1024)
    client_socket.send(
        RedisParser.encode.encode_array(["REPLCONF", "capa", "psync2"]).encode()
    )
    client_socket.recv(1024)
    #  print(RedisParser.decode.decodeSimpleString(res))
    client_socket.send(RedisParser.encode.encode_array(["PSYNC", "?", "-1"]).encode())
    
    client_socket.recv(1024)
    client_socket.recv(1024)

    while client_socket:
        recv = client_socket.recv(1024).decode()
        res = RedisParser.decode.decodeArrays(recv)
        print(recv)
        print(res)
        client_socket.sendall(res.encode())
#  client_socket.recv(1024)
#  client_socket.send(RedisParser.encode.encode_rdb().encode())


def main():

    args = sys.argv[1:]

    port = 6379

    if "--port" in args or "-p" in args:
        port = args[args.index("--port" or "-p") + 1]
        port = int(port)

    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    replica_of = False

    if "--replicaof" in args:
        INFO.update({"role": "slave"})
        string = args[args.index("--replicaof") + 1]
        #  masterhost[args.index("--replicaof")+1]
        #  masterport = args[args.index("--replicaof")+2]
        string = string.split(" ")
        masterhost = string[0]
        masterport = string[1]
        handshake(masterhost, masterport, port)

    # wait for client

    while True and not replica_of:
        conn, adrr = server_socket.accept()
        thread(conn).start()


if __name__ == "__main__":
    main()
