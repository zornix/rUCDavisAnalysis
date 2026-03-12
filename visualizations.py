import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import sqlite3
conn = sqlite3.connect("reddit_posts.db")
reddit_posts = pd.read_sql_query("SELECT * FROM posts", conn)

# Summary Statistics
cat_columns = ['time_category', 'day_posted', 'media', 'attachment', 'flair', 'question']
num_columns = ['timestamp', 'title_length', 'title_words', 'selftext_length', 'selftext_words', 
               'upvotes', 'upvote_ratio', 'num_comments', 'num_keywords', 'score']

def numerical_summary(column):
    sns.displot(reddit_posts, x=column, kde=True)
    plt.title(f'Distribution of {column}')
    plt.show()

    numerical_stats = reddit_posts[column].describe()
    print(numerical_stats)
def categorical(column):
    sns.countplot(x=column, data=reddit_posts)
    plt.title(f'Distribution of {column}')
    plt.show()

    categorical_stats = reddit_posts[column].describe()
    print(categorical_stats)

for col in cat_columns:
    categorical(col)
for col in num_columns:
    numerical_summary(col)
    
# Correlation heatmat between variables and our response
def heatmap(table):
    newTable = table.select_dtypes(include='number') #get rid of strings
    corr = newTable.corr() # gets correlation of only numeric value in originall table
    plt.figure(figsize=(11,7))
    sns.heatmap(corr, annot=True)
    plt.savefig("../output/heatmap.png")
    plt.show()

# Distribution of upvotes per posts: summary statistics

# Scatter plot of upvotes vs. comments sorted by the day of the week
def daysort_upvotes_vs_comments():
    markers = {
        "Monday": "o",
        "Tuesday": "s",
        "Wednesday": "^",
        "Thursday": "D",
        "Friday": "P",
        "Saturday": "*",
        "Sunday": "X"
    }

    plt.figure(figsize=(10,7))

    for day, marker in markers.items():
        day_reddit_posts = reddit_posts[reddit_posts["day_posted"] == day]
        plt.scatter(
            plot_df = reddit_posts[reddit_posts["upvotes"].between(50, 1000)]
            plot_df["num_comments"],
            plot_df["upvotes"],
            marker=marker,
            alpha=0.7,
            s=70,
            label=day
        )

    plt.title("Upvotes vs Comments by Day Posted")
    plt.xlabel("Number of Comments")
    plt.ylabel("Number of Upvotes")

    plt.legend(title="Day Posted", bbox_to_anchor=(1.05,1))
    plt.grid(alpha=0.3)

    plt.show()

# effect of title length on upvotes removing outliers and removing dense numbers
def titlelength_upvotes():
    plt.figure(figsize=(8,6))
    # plot_df["log_upvotes"] = np.log2(df["upvotes"]+1) for log
    plot_df = reddit_posts[reddit_posts["upvotes"].between(50, 1000)]
    
    plt.scatter(
        plot_df["title_length"],
        plot_df["upvotes"],
        marker="*",
        alpha=0.7,
        s=80
    )

    plt.title("Title Length Effect on Upvotes")
    plt.xlabel("Title Length")
    plt.ylabel("Upvotes")

    plt.grid(True, linestyle="--", alpha=0.3)

    plt.show()