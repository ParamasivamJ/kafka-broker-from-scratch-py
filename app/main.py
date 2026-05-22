import socket  # noqa: F401


def main():
    
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()

    print(f"Connection from {addr}")

    # Receive request
    request = conn.recv(1024)

    # Extract correlation_id (bytes 8-11)
    correlation_id = request[8:12]

    # Build response
    message_size = (0).to_bytes(4, byteorder="big")

    response = message_size + correlation_id

    # Send response
    conn.sendall(response)



if __name__ == "__main__":
    main()
