import socket
import threading
import redisParser



class thread(threading.Thread):
    def __init__(self,thread_conn):
        threading.Thread.__init__(self)
        self.thread_conn = thread_conn

    def run(self):
        with self.thread_conn:
                recv = self.thread_conn.recv(1024).decode()
                # print(recv)
                res = redisParser.RedisParser.decode.decodeArrays(recv)
                # print(res.encode())
                self.thread_conn.send(res.encode())

def main():
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

     # wait for client

    while True:
        conn,adrr = server_socket.accept()
        thread(conn).start()
        
        


if __name__ == "__main__":
    main()
