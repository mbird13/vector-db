## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv('TIGERDATA_CONNECTION') 

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
CREATE TABLE podcast (
id VARCHAR(30) PRIMARY KEY,
title VARCHAR(200) NOT NULL
);
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
CREATE TABLE podcast_segment (
id VARCHAR(30) PRIMARY KEY,
start_time DECIMAL(10, 2) NOT NULL,
end_time DECIMAL(10, 2) NOT NULL,
content TEXT,
embedding vector(128) NOT NULL,
podcast_id VARCHAR(30) NOT NULL,
FOREIGN KEY (podcast_id) REFERENCES podcast(id)
);
"""

conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()
#cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)
conn.commit()
conn.close()
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)


