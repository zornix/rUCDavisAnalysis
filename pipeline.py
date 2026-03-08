"""
This file runs our full pipeline.

It should run extract -> transform -> load and then return the data for us to look at.
"""

import pandas as pd

from extract import extract
from transform import transform
from load import load
from config import DB_PATH, DEFAULT_BATCH_SIZE


# This function is the top-level orchestrator for the full pipeline.
def run_pipeline(subreddit: str = "UCDavis",
                 sort: str = "new",
                 batch_size: int = DEFAULT_BATCH_SIZE,
                 resume: bool = True,
                 db_path: str = DB_PATH) -> pd.DataFrame:
    raw_posts = extract() # Gets the raw posts
    transformed_df = transform(raw_posts) # Clean and engineer features
    load(transformed_df, DB_PATH) # Write sqlite
    return transformed_df # Return transformer DataFrame




"""
this function will display a preview of the transformed data.

what this function should do:
    print a summary header
    print a readable preview for each post
    print a footer at the end
    handle empty DataFrames gracefully
"""
def display_output(df: pd.DataFrame) -> None:
    if df.empty:
        print("No posts to display.")
        return

    print("\n========== POSTS PREVIEW ==========\n")

    for row in df.iterrows():
        print(f"Title: {row['title']}")
        print(f"Timestamp: {row['hours']}") # used to be created_utc
        print(f"Time Category: {row['time_category']}")
        print(f"Day Posted: {row['day_posted']}")
        print(f"Title Length: {row['title_length']}")
        print(f"Title Words: {row['title_words']}")
        print(f"Text: {row['selftext']}")
        print(f"Text Length: {row['selftext_length']}")
        print(f"Text Words: {row['selftext_words']}")
        print(f"Media: {row['media']}")
        print(f"Attachment: {row['attachment']}")
        print(f"Flair: {row['flair']}")
        print(f"Flair Text: {row['flair_text']}")
        print(f"Question: {row['has_question']}")
        print(f"Upvotes: {row['upvotes']}")
        print(f"Number of Keywords: {row['num_keywords']}")
        print(f"Upvote Ratio: {row['upvote_ratio']}")
        print(f"Engagement Ratio: {row['engagement_ratio']}")
        print("-" * 100)

    print("\n========== END ==========\n")
    pass

