Explore the response that we get from the Reddit API -> file page.json


It will give you a good idea of how the json is structured.
You can access all the different parts of the data using .get() or using dict logic (the json file is structured like a big dict).


We get the data using Reddit's json API
https://www.reddit.com/r/{subreddit}/{sort}.json?limit={n}

Reddit uses cursor pagination (more on that in extract.py), we can add `&after=<cursor>` after the url above to get the next page.

The db schema:
        id                TEXT PRIMARY KEY
        title             TEXT NOT NULL
        selftext          TEXT
        author            TEXT
        timestamp         TEXT
        upvotes           INTEGER
        upvote_ratio      REAL
        num_comments      INTEGER
        spoiler           INTEGER
        flair             TEXT
        has_image         INTEGER

        + any engineered features