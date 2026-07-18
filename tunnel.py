import asyncio
import struct
from fastapi import WebSocket, WebSocketDisconnect

MSG_OPEN = 1
MSG_DATA = 2
MSG_CLOSE = 3
MSG_OPEN_OK = 4
MSG_OPEN_FAIL = 5

phone_ws: WebSocket | None = None
streams: dict[int, asyncio.Queue] = {}
open_waiters: dict[int, asyncio.Future] = {}
next_stream_id = 0


def encode(msg_type: int, stream_id: int, payload: bytes = b"") -> bytes:
    return struct.pack(">BI", msg_type, stream_id) + payload


def decode(data: bytes):
    msg_type, stream_id = struct.unpack(">BI", data[:5])
    return msg_type, stream_id, data[5:]


async def tunnel_websocket_endpoint(ws: WebSocket):
    global phone_ws
    await ws.accept()
    phone_ws = ws
    try:
        while True:
            data = await ws.receive_bytes()
            msg_type, stream_id, payload = decode(data)

            if msg_type == MSG_OPEN_OK:
                fut = open_waiters.pop(stream_id, None)
                if fut and not fut.done():
                    fut.set_result(True)

            elif msg_type == MSG_OPEN_FAIL:
                fut = open_waiters.pop(stream_id, None)
                if fut and not fut.done():
                    fut.set_result(False)

            elif msg_type == MSG_DATA:
                q = streams.get(stream_id)
                if q:
                    await q.put(payload)

            elif msg_type == MSG_CLOSE:
                q = streams.get(stream_id)
                if q:
                    await q.put(None)
    except WebSocketDisconnect:
        pass
    finally:
        if phone_ws is ws:
            phone_ws = None
        for q in streams.values():
            await q.put(None)


async def open_stream(host: str, port: int) -> int | None:
    global next_stream_id
    if phone_ws is None:
        return None

    stream_id = next_stream_id
    next_stream_id += 1

    fut = asyncio.get_event_loop().create_future()
    open_waiters[stream_id] = fut
    streams[stream_id] = asyncio.Queue()

    target = f"{host}:{port}".encode()
    await phone_ws.send_bytes(encode(MSG_OPEN, stream_id, target))

    try:
        ok = await asyncio.wait_for(fut, timeout=10)
    except asyncio.TimeoutError:
        ok = False

    if not ok:
        streams.pop(stream_id, None)
        return None
    return stream_id


async def send_data(stream_id: int, data: bytes):
    if phone_ws is not None:
        await phone_ws.send_bytes(encode(MSG_DATA, stream_id, data))


async def close_stream(stream_id: int):
    if phone_ws is not None:
        await phone_ws.send_bytes(encode(MSG_CLOSE, stream_id))
    streams.pop(stream_id, None)


async def recv_data(stream_id: int) -> bytes | None:
    q = streams.get(stream_id)
    if q is None:
        return None
    return await q.get()


async def run_socks5_server(host="127.0.0.1", port=1080):
    server = await asyncio.start_server(handle_socks5_client, host, port)
    async with server:
        await server.serve_forever()


async def handle_socks5_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        version, nmethods = struct.unpack(">BB", await reader.readexactly(2))
        await reader.readexactly(nmethods)
        writer.write(struct.pack(">BB", 0x05, 0x00))
        await writer.drain()

        header = await reader.readexactly(4)
        version, cmd, _, addr_type = struct.unpack(">BBBB", header)

        if addr_type == 0x01:
            addr_bytes = await reader.readexactly(4)
            host = ".".join(str(b) for b in addr_bytes)
        elif addr_type == 0x03:
            length = (await reader.readexactly(1))[0]
            host = (await reader.readexactly(length)).decode()
        else:
            writer.write(struct.pack(">BBBBIH", 0x05, 0x08, 0x00, 0x01, 0, 0))
            await writer.drain()
            writer.close()
            return

        port_bytes = await reader.readexactly(2)
        port = struct.unpack(">H", port_bytes)[0]

        stream_id = await open_stream(host, port)
        if stream_id is None:
            writer.write(struct.pack(">BBBBIH", 0x05, 0x01, 0x00, 0x01, 0, 0))
            await writer.drain()
            writer.close()
            return

        writer.write(struct.pack(">BBBBIH", 0x05, 0x00, 0x00, 0x01, 0, 0))
        await writer.drain()

        async def upstream():
            try:
                while True:
                    chunk = await reader.read(8192)
                    if not chunk:
                        break
                    await send_data(stream_id, chunk)
            except Exception:
                pass
            finally:
                await close_stream(stream_id)

        async def downstream():
            try:
                while True:
                    chunk = await recv_data(stream_id)
                    if chunk is None:
                        break
                    writer.write(chunk)
                    await writer.drain()
            except Exception:
                pass
            finally:
                writer.close()

        await asyncio.gather(upstream(), downstream())

    except Exception:
        try:
            writer.close()
        except Exception:
            pass
