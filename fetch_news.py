import pandas as pd
import requests
import datetime
import uuid
import os
import logging
from google.cloud import storage


def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    """Upload local file to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logging.info(f"✅ File {source_file_name} uploaded to {destination_blob_name}.")


def fetch_news_data():
    today = datetime.date.today()
    api_key = "9fbbe9b2ae9c43a9b49a3611f07cecca"

    # Dynamic date range: yesterday → today
    start_date_value = str(today - datetime.timedelta(days=1))
    end_date_value = str(today)

    # Build API URL properly
    url = (
        "https://newsapi.org/v2/everything?"
        f"q=apple&from={start_date_value}&to={end_date_value}"
        f"&sortBy=popularity&apiKey={api_key}"
    )

    df = pd.DataFrame(columns=[
        "newsTitle", "timestamp", "url_source", "content",
        "source", "author", "urlToImage"
    ])

    response = requests.get(url)
    d = response.json()

    # Check for valid API response
    if "articles" not in d:
        raise ValueError(f"❌ Unexpected API response: {d}")

    for i in d["articles"]:
        newsTitle = i.get("title")
        timestamp = i.get("publishedAt")
        url_source = i.get("url")
        source = i.get("source", {}).get("name")
        author = i.get("author")
        urlToImage = i.get("urlToImage")
        partial_content = i.get("content") or ""

        # Trim content for consistency
        if len(partial_content) >= 200:
            trimmed_part = partial_content[:199]
        elif "." in partial_content:
            trimmed_part = partial_content[:partial_content.rindex(".")]
        else:
            trimmed_part = partial_content

        new_row = pd.DataFrame({
            "newsTitle": [newsTitle],
            "timestamp": [timestamp],
            "url_source": [url_source],
            "content": [trimmed_part],
            "source": [source],
            "author": [author],
            "urlToImage": [urlToImage],
        })

        df = pd.concat([df, new_row], ignore_index=True)

    # Save DataFrame as Parquet with timestamp
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"run_{current_time}.parquet"

    logging.info("✅ DataFrame created successfully.")
    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info(f"Saving file: {filename}")

    df.to_parquet(filename)

    # Upload to GCS
    bucket_name = "snowflake_project_test007"
    destination_blob_name = f"news_data_analysis/parquet_files/{filename}"
    upload_to_gcs(bucket_name, destination_blob_name, filename)

    # Clean up local file
    os.remove(filename)
    logging.info(f"🧹 Removed local file {filename}")

    logging.info("🎉 fetch_news_data task completed successfully.")
