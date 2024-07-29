import json
import asyncio
from datetime import datetime
from functions import db_write, file_write, parse_wialon_packet

# -> глобальные переменные
FILENAME = "data.txt"
PORT = 25565


# -> обработка запроса клиента
async def handle_client(reader, writer):
    data = await reader.read(1024)
    addr = writer.get_extra_info("peername")
    message = ""
    while data:
        message = message + data.decode()
        data = await reader.read(1024)

    print(f"At {datetime.now()} received message from {addr!r}")
    try:
        parsed_data = parse_wialon_packet(message)
    except ValueError as e:
        print(f"Error parsing data: {e}")

    # await file_write(parsed_data, FILENAME)
    await db_write(parsed_data)

    # -> closing writer
    writer.close()
    await writer.wait_closed()


# -> тело функции сервера
async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}")
    async with server:
        await server.serve_forever()


asyncio.run(main())
