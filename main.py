import concurrent.futures
import queue
import sqlite3
import threading
import time

import requests

# API possui limite máximo de 32000 porém se utilizar este valor há perda de dados
LIMIT = 31000
DB_PATH = "./db/emprendimentos.sqlite3"
API_ENDPOINT = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=b1bd71e7-d0ad-4214-9053-cbd58e9564a7"


def fetch_data(page, limit=LIMIT, api_endpoint=API_ENDPOINT):
    params = {"offset": page * limit, "limit": limit}
    response = requests.get(api_endpoint, params=params)
    if response.status_code == 200:
        data = response.json()["result"]
        return data["records"], data["total"]
    return [], 0


def get_total_pages(total_records, limit=LIMIT):
    return (total_records + LIMIT - 1) // LIMIT


def get_sql_query(data):
    columns = ", ".join(data[0].keys())
    placeholders = ", ".join("?" * len(data[0]))
    sql = "INSERT INTO emprendimento_geracao_distribuida({}) VALUES({})".format(
        columns, placeholders
    )

    return sql


def fetch_and_enqueue_page(page, limit, data_queue, progress_queue):
    data, _ = fetch_data(page, limit)
    if data:
        data_queue.put(data)
    progress_queue.put(1)


def insert_data(conn, data, sql):
    c = conn.cursor()
    c.executemany(sql, [tuple(item.values()) for item in data])
    conn.commit()


def write_to_db(data_queue, sql, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    while True:
        data = data_queue.get()
        if data is None:
            break
        insert_data(conn, data, sql)
        data_queue.task_done()
    conn.close()


def display_progress(total_pages, progress_queue):
    processed_pages = 0
    while processed_pages < total_pages:
        processed_pages += progress_queue.get()
        print(f"{processed_pages} de {total_pages} páginas processadas", end="\r")


def main():
    data_queue = queue.Queue()
    progress_queue = queue.Queue()

    initial_data, total_records = fetch_data(0, 1)
    total_pages = get_total_pages(total_records)
    sql = get_sql_query(initial_data)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        write_thread = threading.Thread(
            target=write_to_db,
            args=(data_queue, sql),
        )
        write_thread.start()

        futures = [
            executor.submit(
                fetch_and_enqueue_page, page, LIMIT, data_queue, progress_queue
            )
            for page in range(total_pages)
        ]

        display_progress(total_pages, progress_queue)

        concurrent.futures.wait(futures)

    data_queue.put(None)
    write_thread.join()


if __name__ == "__main__":
    main()
