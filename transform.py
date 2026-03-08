"""
This file contains the transform stage of our pipeline.

We will clean raw reddit post data, detect useful flags, and add engineered features.

The output should be a pandas DataFrame that matches the db schema.
"""

from pkgutil import get_data
import re

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from config import EMOJI_PATTERN

KEYWORDS = ["cheeto", "housing", "taps", "silo", "unitrans", "professor", "curve", "ship", "waitlist"]

# CLEANING HELPERS

# This function applies clean_text across an entire Series using vectorized pandas ops.
def clean_text_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str)
    cleaned = cleaned.apply(lambda x: EMOJI_PATTERN.sub("", x))
    cleaned = cleaned.str.split().str.join(" ")  # collapse whitespace
    return cleaned


# DETECTION HELPERS

# This function will detect if a post has an image / video and return True or False.
def has_media(post_data: dict) -> int:
    result = {"image": 0, "video": 0}

    try:
        # checks for image
        if post_data.get("post_hint") == "image":
            result["image"] = 1
        else:
            preview = post_data.get("preview")
            if preview and preview.get("images"):
                result["image"] = 1

        # Check for video
        if post_data.get("is_video"):
            result["video"] = 1

    except:
        print("Media error")
        return 0

    return int((result["image"] + result["video"]) > 0)

# TOP-LEVEL TRANSFORM FUNCTIONS

def transform(posts_data: list[dict]) -> pd.DataFrame:
    if not posts_data:
        return pd.DataFrame()

    df = pd.DataFrame(posts_data)
    print(df.columns.tolist())
    #variables related to the title
    df["clean_title"] = clean_text_series(df["title"])
    df["title_length"] = df["clean_title"].str.len().astype(int)
    df["title_words"] = df["clean_title"].str.split().str.len().astype(int)
    df["question"] = df["clean_title"].str.contains(r"\?", na=False).astype(int)

    # variables related to the body of text

    #A self-post is a post that has no link to a website, image, or video, but instead only contains text.
    df["clean_selftext"] = clean_text_series(df.get("selftext", pd.Series(dtype=str)))

    # variables related to the time posted
    dt_series = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    hours = dt_series.dt.hour
    df["timestamp"] = hours
    df["day_posted"] = dt_series.dt.day_name()

    times_of_day = [
        (hours >= 5) & (hours < 11),
        (hours >= 11) & (hours < 17),
        (hours >= 17) & (hours < 24),
    ]
    names = ["morning", "afternoon", "night"]
    df["time_category"] = np.select(times_of_day, names, default="late_night")

    df["selftext_length"] = df["clean_selftext"].str.len().astype(int)
    df["selftext_words"] = df["clean_selftext"].str.split().str.len().astype(int)

    raw_series = pd.Series(posts_data)
    df["media"] = raw_series.apply(has_media)

    # has_attachment vectorized: a post with is_self=True has no attachment
    df["attachment"] = (~df["is_self"]).astype(int)

    # has_flair vectorized
    df["flair"] = df["link_flair_text"].notna().astype(int)
    df["flair_text"] = df["link_flair_text"]

    # --- Engagement features (vectorized, mirrors compute_engagement_ratio) ---
    df["upvotes"] = df["ups"]

    # --- Keyword count (vectorized, mirrors count_keywords) ---
    title_lower = df["clean_title"].str.lower()
    df["num_keywords"] = sum(title_lower.str.contains(kw, na=False).astype(int) for kw in KEYWORDS)

    # Select and rename final columns to match schema
    result = df[[
        "id", "timestamp", "time_category", "day_posted",
        "clean_title", "title_length", "title_words",
        "clean_selftext", "selftext_length", "selftext_words",
        "media", "attachment", "flair", "flair_text", "question",
        "upvotes", "upvote_ratio", "num_comments", "num_keywords", "score",
    ]].rename(columns={"clean_title": "title", "clean_selftext": "selftext"})

    return result