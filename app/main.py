import socket


def main():
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn,adrr = server_socket.accept() # wait for client

    pong = "+PONG\r\n"

    with conn:
        conn.recv(1024)
        conn.send(pong.encode())
        


if __name__ == "__main__":
    main()
