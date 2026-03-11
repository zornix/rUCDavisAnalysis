"""
This file contains the load stage of our pipeline

We could keep the data in a df or a csv file, but I think we should use a sqlite db.

This file should create the table if it doesn't exist in the db, and then load the data.
"""

import sqlite3
import pandas as pd

from config import DB_PATH, TABLE_NAME

# Temp table for staging rows before upsert; column order must match TABLE_NAME.
POSTS_COLUMNS = [
    "id", "timestamp", "time_category", "day_posted", "title", "title_length",
    "title_words", "selftext", "selftext_length", "selftext_words", "media",
    "attachment", "flair", "flair_text", "question", "upvotes", "upvote_ratio",
    "num_comments", "num_keywords", "score",
]
TEMP_TABLE = "posts_load_temp"


# This function creates a table in the database if it does not already exists in db.
def create_table(conn: sqlite3.Connection) -> None:
    cur = conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                    id varchar(255) PRIMARY KEY,
                    timestamp float,
                    time_category varchar(255),
                    day_posted varchar(255),
                    title varchar(255),
                    title_length int,
                    title_words int,
                    selftext TEXT,
                    selftext_length int,
                    selftext_words int,
                    media int,
                    attachment int,
                    flair int,
                    flair_text varchar(255),
                    question int,
                    upvotes int,
                    upvote_ratio float,
                    num_comments int,
                    num_keywords int,
                    score int)""")

    conn.commit()


# This function upserts rows from the transformed DataFrame to the posts table in the database.
# Posts that already exist in the db are replaced, and new rows are inserted.
def add_rows(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    if df.empty:
        print("There are no more rows")
        return

    # df that has columns that exist both in POSTS_COLUMNS and df, in the order of the schema.
    df_ordered = df[[c for c in POSTS_COLUMNS if c in df.columns]]

    df_ordered.to_sql(TEMP_TABLE, conn, if_exists="replace", index=False)


    # copy all the rows from the temp table into the main posts table. replace if there is an ID conflict.
    conn.execute(
        f"INSERT OR REPLACE INTO {TABLE_NAME} ({', '.join(POSTS_COLUMNS)}) "
        f"SELECT {', '.join(POSTS_COLUMNS)} FROM {TEMP_TABLE}"
    )

    conn.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")
    conn.commit()

# This function takes DataFrame and loads it into the database.
def load(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    if df.empty:
        print("Dataframe is empty.")
        return
    conn = sqlite3.connect(db_path)
    create_table(conn) # Create the table if it doesn't exist
    add_rows(conn, df) # Adds the rows from the df to the table
    cur = conn.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5")
    rows = cur.fetchall()
    #for row in rows:
    #    print(row)
    conn.close()
    pass
