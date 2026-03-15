# init function

import time
import pandas as pd
import sqlite3
from pipeline import run_pipeline
from config import PAGE_SLEEP_SECONDS, DB_PATH


# this function will run the pipeline until it has processed the required amount of posts.

def rerun_pipeline(amount_posts: int = 1000, db_path: str = DB_PATH) -> int:

    total_processed = 0
    while total_processed < amount_posts:
        df = run_pipeline(db_path=DB_PATH)
        batch_size = len(df)
        total_processed += batch_size
        if batch_size == 0:
            break
        if total_processed < amount_posts:
            time.sleep(PAGE_SLEEP_SECONDS)
    return total_processed


if __name__ == "__main__":
    x = rerun_pipeline(1000)
    print(f"Processed {x}.")
    conn = sqlite3.connect(DB_PATH)
    preview = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} LIMIT 20", conn)
    conn.close()
    display_output(preview)
