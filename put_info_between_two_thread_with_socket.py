import asyncio
import threading

from aiohttp import ClientSession, TCPConnector
import os
import pandas as pd
import socket
import json
import sys
import time

# sys.stdout = open("out.log", mode="w")


SERVER_START_LOCK = threading.Lock()


STOP_WORKER_SIGNAL = False
SERVER_START_SIGNAL = False

SPLIT_CHAR = "###***###"


def create_client():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ("127.0.0.1", 8000)
    sock.connect(server_address)
    return sock


class ParseResponse:
    def __init__(self, pipe: socket.socket, size_recv: int = 1024):
        self._pipe = pipe
        self._size_recv = size_recv
        self._pre_response = ""

    def get_post_list_and_request_serial(self):
        # print("Start parse response")
        data_raw = self._pre_response

        while SPLIT_CHAR not in data_raw and not STOP_WORKER_SIGNAL:
            # print("Waiting data")
            data_raw += self._pipe.recv(self._size_recv).decode()
            # print(f"Have data: {data_raw}")

        index_of_split_char = data_raw.find(SPLIT_CHAR)

        post_list_and_request_serial_str = data_raw[:index_of_split_char]
        post_list, request_serial = json.loads(post_list_and_request_serial_str)

        self._pre_response = data_raw[index_of_split_char + len(SPLIT_CHAR):]
        # print(f"End parse response: \n\tdata_raw = {data_raw} \n\t_pre_response = {self._pre_response} \n\tpost_list = {post_list} \n\trequest_serial = {request_serial}")
        return post_list, request_serial


def worker_save_post_list_to_excel(path_to_file_excel: str, size_of_receive_data: int = 1024):
    print("Go in worker")
    with SERVER_START_LOCK:
        print("Start worker")
        pipe = create_client()
        parse_response = ParseResponse(pipe, size_of_receive_data)

        if not os.path.exists(path_to_file_excel):
            print("First save")
            post_list, request_serial = parse_response.get_post_list_and_request_serial()
            print(f"First save receive post list of {request_serial}: {post_list}")
            df = pd.DataFrame(data=post_list)
            df.to_excel(path_to_file_excel, index=False)
            print(f"Done first save of request {request_serial}")

        with pd.ExcelWriter(path_to_file_excel, if_sheet_exists="overlay", mode="a") as writer:
            while True:
                try:
                    post_list, request_serial = parse_response.get_post_list_and_request_serial()
                except Exception as er:
                    print(f"Have error when worker receive data: {er}")
                    break
                print(f"Worker receive post list of request {request_serial}")
                df = pd.DataFrame(data=post_list)
                df.to_excel(
                    writer,
                    startrow=writer.sheets["Sheet1"].max_row,
                    index=False
                )
                print(f"Worker done save post list of request {request_serial} to excel")
    print("Finish worker")


async def get_post(session: ClientSession, url: str, serial: int, delay: int = 0):
    print(f"Start request {serial}")
    if delay:
        await asyncio.sleep(delay)
    async with session.get(url) as result:
        json_res = await result.json()
        print(f"Request {serial} has response")
        return json_res, serial


async def create_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setblocking(False)
    server_address = ("127.0.0.1", 8000)
    server_socket.bind(server_address)
    server_socket.listen()

    print("Finished create server")
    SERVER_START_LOCK.release()

    loop = asyncio.get_running_loop()
    connection, address = await loop.sock_accept(server_socket)
    connection.setblocking(False)
    return connection


async def fetch_posts(post_url):
    pipe = await create_server()
    loop = asyncio.get_running_loop()

    async with ClientSession(
            connector=TCPConnector(limit=10000)
    ) as session:
        post_url_list = [post_url for _ in range(50)]
        fetchers = [
            get_post(session, post_url_list[i], i+1) for i in range(len(post_url_list))
        ]
        fetchers += [
            get_post(session, post_url_list[i], i + 51, 4) for i in range(len(post_url_list))
        ]

        for finished_request in asyncio.as_completed(fetchers):
            post_list, request_serial = await finished_request
            print(f"Have post list of request {request_serial}")
            try:
                data_dumps = json.dumps((post_list, request_serial))
                data_dumps += SPLIT_CHAR
                await loop.sock_sendall(pipe, data_dumps.encode())
                print(f"Done put post list to socket of request {request_serial}")
            except Exception as er:
                print(f"Have error of put post list to socket of request {request_serial}: {str(er)}")
    pipe.close()


def fetch_post_in_event_loop(post_url):
    asyncio.get_event_loop().run_until_complete(fetch_posts(post_url))
    global STOP_WORKER_SIGNAL
    STOP_WORKER_SIGNAL = True
    print("Done fetch")


if __name__ == '__main__':
    start_time = time.time()
    # path_to_file_excel = "post_data.xlsx"
    path_to_file_excel = "users_data.xlsx"
    if os.path.exists(path_to_file_excel):
        os.remove(path_to_file_excel)
        print(f"Remove file {path_to_file_excel}")

    SERVER_START_LOCK.acquire()

    worker = threading.Thread(
        target=worker_save_post_list_to_excel,
        kwargs={
            "path_to_file_excel": path_to_file_excel,
            "size_of_receive_data": 1024 * 2 * 5,
        }
    )
    worker.start()

    fetch_post_in_event_loop(
        # post_url="https://jsonplaceholder.typicode.com/posts"
        post_url="https://jsonplaceholder.typicode.com/users"
    )
    end_time = time.time()
    print(f"time execute: {end_time - start_time}")
