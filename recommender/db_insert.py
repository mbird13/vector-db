## This script is used to insert data into the database
import os
import json
import glob
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert

load_dotenv()

CONNECTION = os.getenv('TIGERDATA_CONNECTION') 

embedding_records = []

# TODO: Read the embedding files
jsonl_files = glob.glob(os.path.join("embedding", "*.jsonl"))
for file_path in jsonl_files:
    with open(file_path, "r") as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get("error") is not None:
                    continue
                response_body = data["response"]["body"]
                embedding_data = response_body["data"][0]

                record = {
                    "id": data.get("custom_id"),
                    "embedding": embedding_data.get("embedding")
                }

                embedding_records.append(record)
            except (json.JSONDecodeError, KeyError, IndexError):
                    continue

# TODO: Read documents files
documents_records = []
jsonl_files = glob.glob(os.path.join("documents", "*.jsonl"))
for file_path in jsonl_files:
    with open(file_path, "r") as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get("error") is not None:
                    continue

                body = data.get("body", {})
                metadata = body.get("metadata", {})

                record = {
                    "id": data.get("custom_id"),
                    "start_time": metadata.get("start_time"),
                    "end_time": metadata.get("stop_time"),
                    "content": body.get("input"),
                    "podcast_id": metadata.get("podcast_id"),
                }

                documents_records.append(record)
            except (json.JSONDecodeError, KeyError, IndexError):
                    continue

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")
train_ds = ds["train"]
train_subset = train_ds.select_columns(["id", "title"])
podcast_data = train_subset.to_pandas()

embedding_data = pd.DataFrame(embedding_records)

document_data = pd.DataFrame(documents_records)
merged_df_inner = pd.merge(embedding_data, document_data, on='id')

# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time
fast_pg_insert(podcast_data, CONNECTION, "podcast", ['id', 'title'])
fast_pg_insert(merged_df_inner, CONNECTION, "podcast_segment", ['id', 'embedding', 'start_time', 'end_time', 'content', 'podcast_id'])
