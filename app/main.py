import socket


def main():
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn,adrr = server_socket.accept() # wait for client

    if conn:
        conn.sendall("PONG")
        


if __name__ == "__main__":
    main()
