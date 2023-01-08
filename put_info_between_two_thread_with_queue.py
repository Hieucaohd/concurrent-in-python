import asyncio
import queue
import threading

from aiohttp import ClientSession, TCPConnector
import os
import pandas as pd


q = queue.Queue(maxsize=10000000)


stop_worker = False


class get_item_from_queue:
    def __init__(self, queue_input: queue.Queue, timeout: int = 0):
        self._queue = queue_input
        self._timeout = timeout

    def __enter__(self):
        if self._timeout != 0:
            return self._queue.get(timeout=self._timeout)
        return self._queue.get()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._queue.task_done()


def worker_save_post_list_to_excel(path_to_file_excel: str):
    if not os.path.exists(path_to_file_excel):
        print("First save")
        with get_item_from_queue(q) as (post_list, request_serial):
            print(f"First save receive post list of {request_serial}")
            df = pd.DataFrame(data=post_list)
            df.to_excel(path_to_file_excel, index=False)
            print(f"Done first save of request {request_serial}")

    with pd.ExcelWriter(path_to_file_excel, if_sheet_exists="overlay", mode="a") as writer:
        while True:
            try:
                with get_item_from_queue(q, 5) as (post_list, request_serial):
                    print(f"Worker receive post list of request {request_serial}")
                    df = pd.DataFrame(data=post_list)
                    df.to_excel(
                        writer,
                        startrow=writer.sheets["Sheet1"].max_row,
                        index=False
                    )
                print(f"Worker done save post list of request {request_serial} to excel")
            except queue.Empty:
                if stop_worker:
                    print("stop worker")
                    break
                else:
                    print("continue loop")
                    continue

    print("Finish worker")


async def get_post(session: ClientSession, url: str, serial: int, delay: int = 0):
    print(f"Start request {serial}")
    if delay:
        await asyncio.sleep(delay)
    async with session.get(url) as result:
        json_res = await result.json()
        print(f"Request {serial} has response")
        return json_res, serial


async def fetch_posts(post_url):
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

        all_posts = []
        for finished_request in asyncio.as_completed(fetchers):
            post_list, request_serial = await finished_request
            print(f"Have post list of request {request_serial}")
            try:
                q.put_nowait((post_list, request_serial))
                print(f"Done put post list to queue of request {request_serial}")
                if len(all_posts) != 0:
                    try:
                        q.put_nowait((all_posts, "all_posts"))
                        print("Put all_posts to queue")
                        all_posts = []
                    except queue.Full:
                        pass
            except queue.Full:
                all_posts += post_list
                print("Queue full, add to all_posts")

        if all_posts:
            q.put((all_posts, "all_posts"))
            print("Finish put all post list")


def fetch_post_in_event_loop(post_url):
    asyncio.get_event_loop().run_until_complete(fetch_posts(post_url))
    global stop_worker
    stop_worker = True
    print("Done fetch")


if __name__ == '__main__':
    # path_to_file_excel = "post_data.xlsx"
    path_to_file_excel = "users_data.xlsx"
    if os.path.exists(path_to_file_excel):
        os.remove(path_to_file_excel)
        print(f"Remove file {path_to_file_excel}")

    worker = threading.Thread(
        target=worker_save_post_list_to_excel,
        kwargs={
            "path_to_file_excel": path_to_file_excel
        }
    )
    worker.start()

    fetch_post_in_event_loop(
        # post_url="https://jsonplaceholder.typicode.com/posts"
        post_url="https://jsonplaceholder.typicode.com/users"
    )

    q.join()
