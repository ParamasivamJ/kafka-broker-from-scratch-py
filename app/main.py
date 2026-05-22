import socket  # noqa: F401


def main():
    
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()

    print(f"Connection from {addr}")

    # Receive request (not used yet)
    conn.recv(1024)

    # Build response

    message_size = (0).to_bytes(4, byteorder="big")
    correlation_id = (7).to_bytes(4, byteorder="big")

    response = message_size + correlation_id

    # Send response
    conn.sendall(response)


if __name__ == "__main__":
    main()
