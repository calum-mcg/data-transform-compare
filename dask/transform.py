import dask.dataframe as dd
from dotenv import load_dotenv
import os
import sys
import pandas as pd
from dask.delayed import delayed
import logging
from datetime import datetime
import numpy as np
from dask.distributed import Client
from dask_yarn import YarnCluster

# Setup logger
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(message)s")
logging.info("Starting...")

# Load variables from file
logging.info("Loading .env files...")
load_dotenv()
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")

logging.info("Setting up cluster...")
cluster = YarnCluster()
cluster.scale(7)
client = Client(cluster)
cluster.adapt() # Dynamically scale Dask resources

# Specify dtypes
meta = {
    "users" : {
        "user_id":int,
        "displayname":str,
        "reputation":int,
        "aboutme":str,
        "websiteurl":str,
        "location":str,
        "profileimageurl":str,
        "views":int,
        "upvotes":int,
        "downvotes":int
    },
    "posts" : {
        "post_id":int,
        "title":str,
        "body_text":str,
        "score":int,
        "view_count":int,
        "comment_count":int,
        "user_poster_id":int
    }
}

dfs = {}
for table in meta.keys():
    logging.info(f"Loading compressed file '{table}.csv.gz' into Dask...")
    delayed_df = delayed(pd.read_csv(f"gs://{STORAGE_BUCKET_NAME}/{table}.csv.gz", compression="gzip"))
    dfs[table] = dd.from_delayed(delayed_df, meta=meta[table]).repartition(npartitions=10)
    logging.info("Loaded..")

def calculate_columns(x) -> pd.Series:
    series_dict = {}
    comment_count = x["comment_count"].sum()
    number_unique_posts = x["post_id"].nunique()
    series_dict["post_to_comment_ratio"] = 0 if comment_count == 0 else (number_unique_posts / comment_count).round(4)
    series_dict["average_post_score"] = x["score"].mean().round(4)
    series_dict["last_updated"] = datetime.now().isoformat()
    return pd.Series(series_dict, index=["post_to_comment_ratio", "average_post_score", "last_updated"])

logging.info("Creating new columns...")
aggregated_posts_by_user = dfs["posts"].groupby("user_poster_id").apply(calculate_columns, meta={"post_to_comment_ratio":float, "average_post_score":float, "last_updated":str}).reset_index().rename(columns={"index":"user_poster_id"})

logging.info("Joining with users table...")
# Join aggregated table with users, selecting only columns of interest
aggregated_posts_by_user_with_info = aggregated_posts_by_user.merge(
    dfs["users"][["user_id", "displayname", "location"]], how="left", left_on="user_poster_id", right_on="user_id").drop("user_id", axis=1)

logging.info("Creating ISO 3166 country code column...")
# Create cleaned country column used for join
aggregated_posts_by_user_with_info["country"] = aggregated_posts_by_user_with_info["location"].str.extract("([^,]+$)", expand=False).str.lower().str.strip()
# Load country code CSV into dataframe
country_codes = pd.read_csv(f"gs://{STORAGE_BUCKET_NAME}/country_codes.csv")
# Clean country names
country_codes["name"] = country_codes["name"].str.lower().str.strip()
# Join
aggregated_posts_by_user_with_country = aggregated_posts_by_user_with_info.merge(
    country_codes, how="inner", left_on="country", right_on="name")

logging.info("Finalising output table...")
# Rename columns
aggregated_posts_by_user_with_country = aggregated_posts_by_user_with_country.rename(columns={"user_poster_id": "user_id"})
# Reorder columns
aggregated_posts_by_user_with_country = aggregated_posts_by_user_with_country[["user_id", "displayname", "post_to_comment_ratio", "average_post_score", "location", "code", "last_updated"]]
# Sort by post to comment ratio desc
aggregated_posts_by_user_with_country = aggregated_posts_by_user_with_country.sort_values("post_to_comment_ratio", ascending=False)
# Drop columns
aggregated_posts_by_user_with_country = aggregated_posts_by_user_with_country.drop(["temp_index", "country", "name"], axis=1)

logging.info("Exporting final table to GCS (compressed)...")
aggregated_posts_by_user_with_country.to_csv(f"gs://{STORAGE_BUCKET_NAME}/dask/agg_users.csv.gz", compression="gzip", index=False, single_file=True)

logging.info("Done...")
