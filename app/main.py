import socket  # noqa: F401

def main():
    
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()

    print(f"Connection from {addr}")

    # Receive request
    request = conn.recv(1024)

    # Extract fields
    api_version = int.from_bytes(
        request[6:8],
        byteorder="big"
    )
    correlation_id = request[8:12]

    # Validate API version
    if 0 <= api_version <= 4:
        error_code = 0
    else:
        error_code = 35

    
    # Build response
    message_size = (0).to_bytes(4, byteorder="big")

    error_code_bytes = error_code.to_bytes(
        2,
        byteorder="big"
    )

    response = (
        message_size +
        correlation_id +
        error_code_bytes
    )

    # Send response
    conn.sendall(response)



if __name__ == "__main__":
    main()
