from __future__ import annotations
from FastestDiffCalc import diff_calc, diff_show, diff_M
from asyncio import StreamReader, StreamWriter
from typing import Optional, Union

import asyncio
import random
import json
import time
import sys

QUIET = True
INIBOX_SN_CODE = "IN01CF620000587" # SNCODE of the inibox.

MAX_CONNECTIONS = 150
MAX_CONN_PER_WALLET = 25

POOL_HOST = "38.96.255.7"
POOL_PORT = 31566

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 7912

def log(prefix, data, ignore_quiet: Optional[bool] = False):
    if ignore_quiet or (not QUIET):
        print(f"[{time.strftime('%H:%M:%S')}] {prefix}: {data}")

job2header = {}
def record_job(job, header):
    job2header[job] = header
    while len(job2header) > 10:
        del job2header[next(iter(job2header))]

def get_conn_by_wallet(wallet: str):
    return sum(1 for conn in connections if (conn.userwallet == wallet))

class StreamRW:
    "StreamRW is better than the original StreamReader/Writer!"
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self._pending = 0
        self._closed  = False
        self._drain_bytes = 128 *1024
        self._lock = asyncio.Lock()

    @property
    def closing(self) -> bool:
        return self.writer.is_closing()

    async def readline(self):
        return await self.reader.readline()

    async def write(self, data: bytes):
        if not data.endswith(b"\n"):
            raise ValueError(f"Not ending with newline: {data}")
        if self._closed:
            raise RuntimeError("Closed.")
        async with self._lock:
            self.writer.write(data)
            self._pending += len(data)
            if self._pending < self._drain_bytes:
                return
            await asyncio.wait_for(self.writer.drain(), timeout = 15)
            self._pending = 0

    async def close(self):
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self.closing:
                return
            if self._pending:
                try:
                    await asyncio.wait_for(self.writer.drain(), timeout = 0.5)
                except Exception:
                    pass
                self._pending = 0
            self.writer.close()
            try:
                await asyncio.wait_for(
                    self.writer.wait_closed(),
                    timeout = 0.5
                )
            except TimeoutError:
                log("WAITCLOS", f"Timed out wait close.", ignore_quiet = True)


async def connect(host, port):
    r, w = await asyncio.open_connection(host, port)
    return StreamRW(r, w)


connadmin: list[Conn] = []
conninibox: list[Conn] = []
connecting: list[Conn] = []
connections: list[Conn] = []
connectlock = asyncio.Lock()
class Conn:
    def __init__(
            self, miner: StreamRW
        ):
        self.ip = str("")
        self.miner = miner
        self.ready = asyncio.Event()
        self.sncode = str("")
        self.password = str("")
        self.userworker = ""
        self.userwallet = ""
        self.extranonce = ""
        self.lastsubmit = time.time()
        self.connectime = self.lastsubmit
        self.ping_coord = PingCoord(self)
        self.sharefirst = asyncio.Event()
        self.sharebuffer = asyncio.Queue()
        self.line_subscribe: Optional[bytes] = None
        self.line_authorize: Optional[bytes] = None
        self.enough_sharebuffer()

    async def init_pool(self):
        if self.ready.is_set():
            raise RuntimeError("Pool is already inited.")

        try:
            self.pool = await connect(POOL_HOST, POOL_PORT)
            self.ready.set()

        except Exception as exc:
            log("END CONN", f"Failed conn to pool: {exc}", ignore_quiet = True)
            await self.miner.close()
            raise

    def enough_sharebuffer(self, num: int = 50):
        size = self.sharebuffer.qsize()
        lack = num -size
        if lack > 0:
            for i in range(lack):
                self.sharebuffer.put_nowait(True if (i %10 != 0) else False)

    async def notify_miner_exc(self, exc: str, id: Optional[int] = None):
        await self.miner.write((json.dumps({
                "id": id, "jsonrpc": "2.0", "result": None,
                "error": {
                    "code": -1, "message": exc
                }
            }) +"\n").encode())

    async def notify_share_result(self, id, result: Optional[bool] = None):
        if result is None:
            if self.sharebuffer.empty():
                log("SHAREBUF", f"Conn ran out of share buffer.", ignore_quiet = True)
                await self.notify_miner_exc("buffer error. closing connection...")
                raise RuntimeError("No avaliable share result in buffer.")
            result = self.sharebuffer.get_nowait()
        if self in conninibox:
            obj = {
                "id": id, "jsonrpc": "2.0", "result": result
            }
        else:
            obj = {
                "id": id, "result": result, "error": None
            }
        line = (json.dumps(obj) + "\n").encode()
        await self.miner.write(line)

    async def close(self):
        if self.ready.is_set():
            await asyncio.gather(
                self.miner.close(),
                self. pool.close(),
                return_exceptions = True,
            )
        else:
            await self.miner.close()


class PingCoord:
    "Now, Nexa miners can handle a ping."
    def __init__(self, conn: Conn):
        self.conn = conn
        self.lock = asyncio.Lock()
        self.pendping: list[tuple[str, asyncio.Future, Optional[Conn]]] = []

    async def close(self, reason: str):
        log("EXC PING", reason, ignore_quiet = True)
        await self.conn.close()

    async def got_ping(self, ping: str, place: Optional[Conn] = None):
        fut = asyncio.get_running_loop().create_future()
        pingobj = {
            "id": None,
            "method": "mining.ping",
            "params": [ping]
        }
        # Send ping to miner.
        pingline = (json.dumps(pingobj) + "\n").encode()

        async with self.lock:
            self.pendping.append((ping, fut, place))
            await self.conn.miner.write(pingline)

        log(f"Ping->M", pingline.rstrip(b"\r\n"))
        asyncio.create_task(self.wait_pong(fut, place))

    async def wait_pong(self, fut: asyncio.Future, place: Optional[Conn]):
        try:
            pong = await asyncio.wait_for(fut, timeout = 1.7)
            pongobj = {
                "id": None,
                "method": "mining.pong",
                "params": [pong]
            }
            # Send pong back to the sender of ping.
            pongline = (json.dumps(pongobj) + "\n").encode()
            log(f"Pong->P", pongline.rstrip(b"\r\n"))
            if place:
                await place.pool.write(pongline)
            else:
                await self.conn.pool.write(pongline)

        except asyncio.TimeoutError:
            if place:
                await asyncio.gather(
                    self.close(f"Timeout waiting pong."),
                    place.close(),
                    return_exceptions = True
                )
                return
            await self.close(f"Timeout waiting pong.")

        except Exception as exc:
            if place:
                await asyncio.gather(
                    self.close(f"{exc}"),
                    place.close(),
                    return_exceptions = True
                )
                return
            await self.close(f"{exc}")

    def got_pong(self, pong: str):
        if not self.pendping:
            raise RuntimeError("Pong not consumed.")

        ping, fut, place = self.pendping.pop(0)
        if fut.done():
            raise RuntimeError("Pong not consumed.")
        fut.set_result(pong)


async def handle_client(r: StreamReader, w: StreamWriter):
    miner = StreamRW(r, w)
    addr = miner.writer.get_extra_info("peername")
    log("NEW CONN", f"Miner {addr}", ignore_quiet = True)

    conn = Conn(miner)
    conn.ip = addr[0]
    del r, w, miner

    async def pooltominer():
        # Pool -> Miner
        await conn.ready.wait()
        while True:
            msg = None
            line = None
            try:
                try:
                    line = await conn.pool.readline()
                except Exception:
                    line = b""
                if (not line) or (not line.endswith(b"\n")):
                    log("END CONN", f"Pool->Miner conn closed.")
                    return

                msg = line.rstrip(b"\r\n").decode()
                log(f"MSG Pool->Miner", msg)

                obj: dict = json.loads(msg)
                method = obj.get("method")

                error = None
                if "error" in obj:
                    error = obj["error"]["message"]
                    obj["result"] = False
                    del obj["error"]

                if error:
                    log("POOL ERR", f"Pool {error}", ignore_quiet = True)
                    log("END CONN", f"Pool->Miner conn closed.")
                    return

                if method == None:
                    if (len(list(obj)) == 3) and ("jsonrpc" in obj) and ("result" in obj):
                        if isinstance(obj["result"], bool):
                            if conn.sharefirst.is_set():
                                method = "mining.submitresult"
                            else:
                                method = "mining.authresult"
                                conn.sharefirst.set()
                        elif isinstance(obj["result"], list) and (len(obj["result"]) == 3):
                            method = "mining.subscriberesult"

                if method == "mining.subscriberesult":
                    continue

                if method == "mining.notify":
                    record_job(obj["params"][0], obj["params"][1])

                if method == "mining.submitresult":
                    if obj["result"] == True:
                        conn.lastsubmit = time.time()

                    conn.sharebuffer.put_nowait(obj["result"])
                    continue

                if conn in conninibox:
                    # Miner is inibox
                    if method == "mining.ping":
                        ping = obj["params"][0]
                        await conn.ping_coord.got_ping(ping)
                    else:
                        await conn.miner.write(line)

                else:
                    # Miner is Nexa
                    if method == "mining.notify":
                        obj["params"][2], obj["params"][3] = obj["params"][3], obj["params"][2]
                        obj["params"].pop()
        
                    elif method == "mining.ping":
                        if len(connadmin) != 1:
                            await conn.notify_miner_exc("Internal Server Error! sorry...")
                            raise RuntimeError(f"Multiple admin. ({len(connadmin)})")
                        ping = obj["params"][0]
                        await connadmin[0].ping_coord.got_ping(ping, conn)
                        continue

                    # elif method == "mining.subscriberesult":
                    #     # Subscribe result. There is extranonce in it.
                    #     assert not isinstance(obj["result"], bool)
                    #     # conn.extranonce = obj["result"][1]
                    #     obj = {
                    #         "id": obj["id"],
                    #         "error": None,
                    #         "result": [
                    #             [], obj["result"][1], obj["result"][2]
                    #         ]
                    #     }

                    line = (json.dumps(obj) + "\n").encode()
                    await conn.miner.write(line)
                    log(f"MSG Pool->Nexa ", line.rstrip(b"\r\n"))

            except Exception as exc:
                log(f"EXC Pool->Miner", exc, ignore_quiet = True)
                return


    async def minertopool():
        # Miner -> Pool
        while True:
            msg = None
            line = None
            try:
                line = await conn.miner.readline()
                if (not line) or (not line.endswith(b"\n")):
                    log("END CONN", "Miner->Pool conn closed.")
                    return
                msg = line.rstrip(b"\r\n").decode()
                log("MSG Miner->Pool ", msg)

                obj = json.loads(msg)
                method = obj.get("method")

                if method == "mining.pong":
                    # Intercept pong
                    pong = obj["params"][0]
                    conn.ping_coord.got_pong(pong)
                    continue

                if method == "mining.subscribe":
                    if conn in connections:
                        log("ILLEGAL ", "Already subscribed.", ignore_quiet = True)
                        await conn.notify_miner_exc("Already subscribed. Closing connection...", obj.get("id"))
                        return
                    if obj["params"][0].startswith("IniMiner"):
                        conninibox.append(conn)
                    if not isinstance(obj.get("id"), int):
                        log("ILLEGAL ", "No 'id' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("params"), list):
                        log("ILLEGAL ", "No 'params' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return

                    if conn not in conninibox:
                        if len(connadmin) != 1:
                            # The Admin inibox is not online. So refuse Nexa miner to connect.
                            await conn.notify_miner_exc("Internal Server Error! sorry...", obj.get("id"))
                            raise RuntimeError(F"Multiple admin. ({len(connadmin)})")
                        obj = {
                            "id": obj["id"],
                            "method": "mining.subscribe",
                            "params": ["IniMiner/v20.0.0"]
                        }
                        line = (json.dumps(obj) + "\n").encode()
                        log("MSG Nexa->Pool", line.rstrip(b"\r\n"))
                    conn.line_subscribe = line

                elif method == "mining.authorize":
                    if (conn not in connections) or (conn.line_subscribe is None):
                        log("ILLEGAL ", "Not subscribed.", ignore_quiet = True)
                        await conn.notify_miner_exc("Not subscribed. Closing connection...", obj.get("id"))
                        return
                    if conn.userwallet or conn.userworker:
                        log("ILLEGAL ", "Already authorized.", ignore_quiet = True)
                        await conn.notify_miner_exc("Already authorized. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("id"), int):
                        log("ILLEGAL ", "No 'id' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("params"), list):
                        log("ILLEGAL ", "No 'params' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return

                    authdata = obj["params"]
                    authuser = authdata[0]
                    if not isinstance(authuser, str):
                        log("ILLEGAL ", f"Incorrect authdata {authuser}", ignore_quiet = True)
                        await conn.notify_miner_exc("Incorrect wallet or worker format. Please check them. Closing connection...", obj.get("id"))
                        return
                    authuser = authuser.replace("/", ".").replace("\\", ".")
                    if authuser.count(".") != 1:
                        log("ILLEGAL ", f"Incorrect authdata {authuser}", ignore_quiet = True)
                        await conn.notify_miner_exc("Incorrect wallet or worker format. Please check them. Closing connection...", obj.get("id"))
                        return
                    userwallet, userworker = authuser.split(".", 1)
                    userwallet = userwallet.strip()
                    userworker = userworker.strip()
                    if (not userwallet.startswith("0x")) or (len(userwallet) != 42) or (len(userworker) == 0) or (len(userworker) > 16):
                        log("ILLEGAL ", f"Incorrect authdata {authuser}", ignore_quiet = True)
                        await conn.notify_miner_exc("Incorrect wallet or worker format. Please check them. Closing connection...", obj.get("id"))
                        return

                    connected_num = get_conn_by_wallet(userwallet)
                    if connected_num >= MAX_CONN_PER_WALLET:
                        log("ILLEGAL ", f"conn too much {userwallet}", ignore_quiet = True)
                        await conn.notify_miner_exc("Conns in this wallet too much. Closing connection...", obj["id"])
                        return

                    await conn.init_pool()
                    await conn.pool.write(conn.line_subscribe)

                    conn.userwallet = userwallet
                    conn.userworker = userworker
                    obj["params"][0] = f"{userwallet}.{userworker}"

                    if obj["params"][-1] == INIBOX_SN_CODE:
                        # This inibox is Admin, so keep it to handle ping.
                        log("ADMIN ON", f"password is {obj['params'][1]}", ignore_quiet = True)
                        connadmin.append(conn)
                    else:
                        return

                    if conn not in conninibox:
                        # Nexa. Let Pool think that it is an inibox.
                        if len(connadmin) != 1:
                            # The Admin inibox is not online. So refuse Nexa miner to connect.
                            await conn.notify_miner_exc("Internal Server Error! sorry...", obj.get("id"))
                            raise RuntimeError(F"Multiple admin. ({len(connadmin)})")
                        obj = {
                            "id": obj["id"],
                            "method": "mining.authorize",
                            "params": [obj["params"][0], connadmin[0].password, INIBOX_SN_CODE]
                        }
                        log("MSG Nexa->Pool", line.rstrip(b"\r\n"))

                    line = (json.dumps(obj) + "\n").encode()
                    conn.line_authorize = line
                    conn.sncode = obj["params"][-1]
                    conn.password = obj["params"][1]
                    del authdata, authuser

                elif method == "mining.submit":
                    if conn not in connections:
                        log("ILLEGAL ", "Not subscribed.", ignore_quiet = True)
                        await conn.notify_miner_exc("Not subscribed. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("id"), int):
                        log("ILLEGAL ", "No 'id' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("params"), list):
                        log("ILLEGAL ", "No 'params' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return

                    submitdata = obj["params"]
                    submituser = submitdata[0]
                    if not isinstance(submituser, str):
                        log("ILLEGAL ", f"Incorrect submitdata {submituser}", ignore_quiet = True)
                        await conn.notify_miner_exc("Incorrect wallet or worker. Please check them. Closing connection...", obj.get("id"))
                        return
                    submituser = submituser.replace("/", ".").replace("\\", ".")
                    if submituser.count(".") != 1:
                        log("ILLEGAL ", f"Incorrect submitdata {submituser}", ignore_quiet = True)
                        await conn.notify_miner_exc("Incorrect wallet or worker. Please check them. Closing connection...", obj.get("id"))
                        return
                    userwallet, userworker = submituser.split(".", 1)
                    userwallet = userwallet.strip()
                    userworker = userworker.strip()
                    if (userwallet != conn.userwallet) or (userworker != conn.userworker):
                        log("ILLEGAL ", f"Changed wallet or worker {conn.userwallet}.{conn.userworker} -> {userwallet}.{userworker}", ignore_quiet = True)
                        await conn.notify_miner_exc("Changed wallet or worker. Closing connection...", obj.get("id"))
                        return
                    obj["params"][0] = f"{userwallet}.{userworker}"

                    if conn not in conninibox:
                        # Modify Nexa miner's submittion. Then Pool can Accept it.
                        obj = {
                            "params": [
                                obj['params'][0],
                                obj["params"][1],
                                conn.extranonce +obj['params'][4],
                            ],
                            "id": obj["id"],
                            "method": "mining.submit"
                        }
                        log("MSG Nexa->Pool", line.rstrip(b"\r\n"))
                        submitdata = obj["params"]

                    line = (json.dumps(obj) + "\n").encode()
                    if submitdata[1] not in job2header:
                        await conn.notify_share_result(obj["id"], False)
                        continue

                    diff  = diff_calc(job2header[submitdata[1]], submitdata[2])
                    diffm = diff_M(diff)
                    del submitdata, submituser

                elif method == "mining.extranonce.subscribe":
                    if conn not in connections:
                        log("ILLEGAL ", "Not subscribed.", ignore_quiet = True)
                        await conn.notify_miner_exc("Not subscribed. Closing connection...", obj.get("id"))
                        return
                    if not isinstance(obj.get("id"), int):
                        log("ILLEGAL ", "No 'id' in JSON.", ignore_quiet = True)
                        await conn.notify_miner_exc("Format incorrect. Closing connection...", obj.get("id"))
                        return

                    await conn.notify_miner_exc("Not supported.", obj["id"])
                    continue

                else:
                    log("ILLEGAL ", f"Unknown method {method}", ignore_quiet = True)
                    await conn.notify_miner_exc("Unknown method. Closing connection...", obj.get("id"))
                    return


                if method == "mining.subscribe":
                    pending_conn_too_much = False
                    async with connectlock:
                        server_overloaded = len(connections) >= MAX_CONNECTIONS
                        pending_conn_too_much = len(connecting) >= 15

                        if (not server_overloaded) and (not pending_conn_too_much):
                            connecting.append(conn)
                            connections.append(conn)
                            pending_num = len(connecting)
                            if pending_num <= 2:
                                time_wait = 0
                            else:
                                time_wait = (pending_num -2) *0.3

                    if server_overloaded:
                        log("OVERLOAD", "Server is overloaded!", ignore_quiet = True)
                        await asyncio.sleep(0.5 +random.random())
                        await conn.notify_miner_exc("Server is overloaded! Closing connection...", obj.get("id"))
                        return
                    if pending_conn_too_much:
                        log("OVERLOAD", "Too much connections!", ignore_quiet = True)
                        await asyncio.sleep(0.5 +random.random())
                        await conn.notify_miner_exc("Too many connect requests! Please try again 2 secs later. Closing connection...", obj.get("id"))
                        return

                    if time_wait > 0:
                        log("OVERLOAD", f"Wait for {time_wait:.1f} secs.", ignore_quiet = True)
                        await asyncio.sleep(time_wait)

                    extranonce = ''.join(format(random.randint(0, 15), 'x') for _ in range(8)) + "0" *8
                    conn.extranonce = extranonce
                    if conn in conninibox:
                        _obj = {
                            "id": obj["id"],
                            "jsonrpc": "2.0",
                            "result": [None, extranonce, 8]
                        }
                    else:
                        _obj = {
                            "id": obj["id"],
                            "error": None,
                            "result": [
                                [], extranonce, 8
                            ]
                        }
                    _line = (json.dumps(_obj) + "\n").encode()
                    await conn.miner.write(_line)
                    log("SYNC SUB", "Subscribed.")
                    del server_overloaded, pending_conn_too_much, time_wait, extranonce

                elif method == "mining.authorize":
                    await conn.pool.write(line)
                    async with connectlock:
                        connecting.remove(conn)
                    log("SYNCAUTH", f"Authorized: {userwallet}", ignore_quiet = True)
                    del line

                elif method == "mining.submit":
                    if diffm > 5000:
                        await conn.notify_share_result(obj["id"])
                        await conn.pool.write(line)
                        log("DIFF Pool", diff_show(diff))
                        continue

                    # We can directly reject shares that less than the diff of the pool.
                    await conn.notify_share_result(obj["id"], False)
                    del line


            except Exception as exc:
                log("EXC Miner->Pool", exc, ignore_quiet = True)
                return


    task_p2m = asyncio.create_task(pooltominer(), name = "pooltominer")
    task_m2p = asyncio.create_task(minertopool(), name = "minertopool")

    done, pending = await asyncio.wait([
            task_p2m, task_m2p
        ],
        return_when = asyncio.FIRST_COMPLETED
    )
    log(
        "END CONN", f"Ending conn cuz {', '.join(task.get_name() for task in done)} returned.",
        ignore_quiet = bool(time.time() -conn.connectime > 0.5)
    )

    if conn in connadmin:
        connadmin.remove(conn)

    if conn in conninibox:
        conninibox.remove(conn)

    async with connectlock:
        if conn in connecting:
            connecting.remove(conn)
        if conn in connections:
            connections.remove(conn)

    try:
        await conn.close()
    except Exception as exc:
        log("END EXC ", exc, ignore_quiet = True)

    log("END CONN", "Cancelling task.")
    for task in pending:
        try:
            task.cancel()
        except Exception as exc:
            log("END EXC ", exc, ignore_quiet = True)
    log("END CONN", "Miner disconnected.")


def show_conns():
    for index, conn in enumerate(list(connections), 1):
        print(F"{index:>03}: {conn.ip:>15} |{conn.userworker:>12} | {conn.userwallet}")


async def shutdown_conns():
    results = await asyncio.gather(
        *(conn.close() for conn in list(connections)),
        return_exceptions = True,
    )
    for res in results:
        if isinstance(res, BaseException):
            log("CLOSEEXC", res, ignore_quiet = True)


ENV = globals()
async def console():
    while True:
        print(">>> ", end = "", flush = True)
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            return
        code = line.rstrip("\n").strip()
        if not code:
            continue
        try:
            if code in ("exit", "q"):
                return
            if code in ("list", "l"):
                show_conns()
                continue
            exec(code, ENV)
        except Exception as exc:
            print(exc)


async def monitor():
    while True:
        await asyncio.sleep(30)
        try:
            now = time.time()
            for conn in list(connections):
                if now -conn.lastsubmit < 600:
                    continue
                log("END CONN", "No share submitted.", ignore_quiet = True)
                await conn.notify_miner_exc("No share submitted. Closing connection...")
                await conn.close()
        except Exception as exc:
            log("EXC MONI", exc, ignore_quiet = True)


async def main():
    tries = 0
    while True:
        try:
            server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
            del tries
            break
        except OSError:
            tries += 1
            print(f"\rFailed listen {LISTEN_PORT}. retry ({tries})  ", end = "", flush = True)
            await asyncio.sleep(0.1)

    log("STARTING", f"Listening on {LISTEN_HOST}:{LISTEN_PORT}", ignore_quiet = True)

    async with server:
        task_server  = asyncio.create_task(server.serve_forever())
        task_console = asyncio.create_task(console())
        task_monitor = asyncio.create_task(monitor())
        done, pending = await asyncio.wait({
            task_console, task_server,
            task_monitor
        }, return_when = asyncio.FIRST_COMPLETED
        )
        try:
            await asyncio.wait_for(
                shutdown_conns(),
                timeout = 4
            )
        except asyncio.TimeoutError:
            log("SHUTDOWN", "There are some tasks still running.", ignore_quiet = True)

    for task in pending:
        task.cancel()

    alive = [
        task
          for task in asyncio.all_tasks()
            if (
              not task.done()
            )and(
              task is not asyncio.current_task()
            )
    ]
    if not alive:
        log("SHUTDOWN", "Nexa2Init terminated gracefully.", ignore_quiet = True)
        return

    complex_num = len(alive) != 1
    log(
      "SHUTDOWN",
        f"There { {True: 'are', False: 'is'}[complex_num] } " +
        f"{len(alive)} task{ {True: 's', False: ''}[complex_num] } still running:",
        ignore_quiet = True
    )
    for task in alive:
        log("SHUTDOWN", f"    - {task.get_name()}", ignore_quiet = True)
    for task in alive:
        task.cancel()

    await asyncio.sleep(0)


if __name__ == "__main__":
    import os
    os.remove(__file__)
    asyncio.run(main())
