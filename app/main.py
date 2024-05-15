import socket
import sys
import threading
import  concurrent.futures
import select

from .redisParser import INFO, RedisParser,BYTES_RECIEVED

replicas_addr = []
replica_true = False
buffered_commands_to_replicate = []

BYTES_SENT = 0

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
                if not is_connection_replicate and cmnd=='SET':
                    for conn in replicas_addr:
                        response = RedisParser.decode.decodeArrays(recv, True)
                        print("sent response")
                        conn.sendall(response.encode())
                self.thread_conn.sendall(res.encode())
                # self.thread_conn.close()


def handlePropogation(client_socket,recv):
    global BYTES_SENT 
    bytes_recv = 0
    # wrecv = client_socket.recv(1024).decode()
    #print(recv,'This is recieving')
    splitted_recv = recv.split("*")
    print(splitted_recv)
    
    for cmnd in splitted_recv:
        bytes_recv = 0
        print('reaching in for loop')
        if cmnd=='\r\n' or cmnd=='':continue
        #bytes_recv+=len(cmnd)+1
        if not 'REPLCONF' in cmnd:
            bytes_recv+=(len(cmnd)+1)
            BYTES_SENT += bytes_recv
        res = RedisParser.decode.decodeArrays(cmnd,False,BYTES_SENT)
        # print(bytes_recv,"bytes_recv")
        if 'REPLCONF' in res:
            client_socket.sendall(res.encode())
            bytes_recv+=37
            BYTES_SENT += bytes_recv
        print('total bytes now after propogation',BYTES_SENT)
def handleHandshake(client_socket,server_socket):
    global BYTES_SENT
    #bytes_recv = 0
    recv = ''
    while True:
        recv = client_socket.recv(1024)
            
        str_recv = str(recv)
        print(str_recv)
        if "*" in str_recv:
            #print(str_recv)
            break
    res = recv.decode('cp1252').encode('utf-8').decode().split("*")
    del res[0]
    print(res)
    for cmnd in res:
        bytes_recv =0
        if cmnd=='\r\n':
            continue
        with concurrent.futures.ThreadPoolExecutor() as executor:
            #bytes_recv+= len(cmnd)+1
            
            #print(cmnd)
            if not 'REPLCONF' in cmnd:
                bytes_recv+=(len(cmnd)+1)
            # print(bytes_recv,"bytes recv")
                BYTES_SENT += bytes_recv
            future = executor.submit(RedisParser.decode.decodeArrays,cmnd,False,BYTES_SENT)
            response = future.result()
            print(response)
            if 'REPLCONF' in response:
                client_socket.sendall(response.encode())
                bytes_recv+=37
                BYTES_SENT += bytes_recv #adding replconf size harcoding
            print('total bytes now after handshake',BYTES_SENT)
    print('handshake completed')
    #conn,addr = server_socket.accept()
    recv = client_socket.recv(1024).decode()
    while recv!="":
        t = threading.Thread(target=handlePropogation,args=(client_socket,recv,))
        t.start()
        t.join()
        recv = client_socket.recv(1024).decode()
    #thread(conn).start()
def handshake(masterhost, masterport, listening_port,server_socket):
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

    server_socket.setblocking(False)
    while True:
        readable,_,_ = select.select([server_socket],[],[],1)
        if(not readable):    
            t = threading.Thread(target=handleHandshake,args=(client_socket,server_socket,))
            t.start()
            break
        if(readable):
            conn,addr = server_socket.accept()
            t = threading.Thread(target=handleHandshake,args=(client_socket,server_socket,))
            t.start()
            thread(conn).start()
            break
    #conn,addr = server_socket.accept()

    # conn,addr = server_socket.accept()
    #thread(conn).start()
    # while True:
    #     conn,addr = server_socket.accept()
    #     thr = thread(conn)
    #     thr.start()
    #     thr.join()
    #client_socket.close()
     # thread(conn).start()
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
        replica_of = True
        print("replica statrted")
        t = threading.Thread(target=handshake,args=(masterhost,masterport,port,server_socket))
        t.start() 
        t.join()
        #print("handshake complete")
        
    # wit for client

    while True:
            if not replica_of:
                conn, adrr = server_socket.accept()
                thread(conn).start()
                print('connection established')


if __name__ == "__main__":
    main()
