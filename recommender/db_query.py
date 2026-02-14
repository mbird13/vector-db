## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv('TIGERDATA_CONNECTION') 


question1 = """
Q1) What are the five most similar segments to segment "267:476"

Input: "that if we were to meet alien life at some point"
For each result return the podcast name, the segment id, segment raw text,  the start time, stop time, raw text and embedding distance
"""

query1 = """
WITH target AS (SELECT embedding FROM podcast_segment WHERE id = '267:476')
SELECT p.title, s.id, s.content, s.start_time, s.end_time, s.embedding <-> target.embedding AS embedding_distance
FROM podcast_segment s
JOIN podcast p 
    ON p.id = s.podcast_id,
target
WHERE s.id != '267:476'
ORDER BY embedding_distance
LIMIT 5;"""

def print_results(question, results):
    print (question)
    for i, (title, seg_id, content, start, end, distance) in enumerate(results, 1):
        print("-" * 80)
        print(f"Result #{i}")
        print(f"Podcast: {title}")
        print(f"Segment: {seg_id}")
        print(f"Time: {float(start)}s to {float(end)}s")
        print(f"Embedding Distance: {distance}")
        print(f"Text: {content.strip()}")

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query1)
    results = cursor.fetchall()
    print_results(question1, results)

question2 = """
Q2) What are the five most dissimilar segments to segment "267:476"

Input: "that if we were to meet alien life at some point"
For each result return the podcast name, the segment id, segment raw text,  the start time, stop time, raw text and embedding distance

"""

query2 = """
WITH target AS (SELECT embedding FROM podcast_segment WHERE id = '267:476')
SELECT p.title, s.id, s.content, s.start_time, s.end_time, s.embedding <-> target.embedding AS embedding_distance
FROM podcast_segment s
JOIN podcast p 
    ON p.id = s.podcast_id,
target
WHERE s.id != '267:476'
ORDER BY embedding_distance DESC
LIMIT 5;"""

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query2)
    results = cursor.fetchall()
    print_results(question2, results)

question3 = """
Q3) What are the five most similar segments to segment '48:511'

Input: "Is it is there something especially interesting and profound to you in terms of our current deep learning neural network, artificial neural network approaches and the whatever we do understand about the biological neural network."
For each result return the podcast name, the segment id, segment raw text,  the start time, stop time, raw text and embedding distance
"""

query3 = """
WITH target AS (SELECT embedding FROM podcast_segment WHERE id = '48:511')
SELECT p.title, s.id, s.content, s.start_time, s.end_time, s.embedding <-> target.embedding AS embedding_distance
FROM podcast_segment s
JOIN podcast p 
    ON p.id = s.podcast_id,
target
WHERE s.id != '48:511'
ORDER BY embedding_distance
LIMIT 5;"""

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query3)
    results = cursor.fetchall()
    print_results(question3, results)

question4 = """
Q4) What are the five most similar segments to segment '51:56'

Input: "But what about like the fundamental physics of dark energy? Is there any understanding of what the heck it is?"
For each result return the podcast name, the segment id, segment raw text,  the start time, stop time, raw text and embedding distance
"""

query4 = """
WITH target AS (SELECT embedding FROM podcast_segment WHERE id = '51:56')
SELECT p.title, s.id, s.content, s.start_time, s.end_time, s.embedding <-> target.embedding AS embedding_distance
FROM podcast_segment s
JOIN podcast p 
    ON p.id = s.podcast_id,
target
WHERE s.id != '51:56'
ORDER BY embedding_distance
LIMIT 5;"""

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query4)
    results = cursor.fetchall()
    print_results(question4, results)

question5 = """
Q5) For each of the following podcast segments, find the five most similar podcast episodes. Hint: You can do this by averaging over the embedding vectors within a podcast episode.

    a) Segment "267:476"

    b) Segment '48:511'

    c) Segment '51:56'

For each result return the Podcast title and the embedding distance
"""

def query5(id):
    return """
WITH target AS (SELECT embedding, podcast_id FROM podcast_segment WHERE id = '""" + id + """')
SELECT p.title, AVG(s.embedding <-> target.embedding) AS avg_embedding_distance
FROM podcast_segment s
JOIN podcast p ON p.id = s.podcast_id,
target
WHERE s.podcast_id != target.podcast_id
GROUP BY p.id
ORDER BY avg_embedding_distance
LIMIT 5;"""

def print_results_5(results):
    for i, (title, avg) in enumerate(results, 1):
        print("-" * 80)
        print(f"Result #{i}")
        print(f"Podcast: {title}")
        print(f"Average Embedding Distance: {avg}")

print (question5)
with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query5('267:476'))
    results = cursor.fetchall()
    print("\nResults a)")
    print_results_5(results)

    cursor.execute(query5('48:511'))
    results = cursor.fetchall()
    print("\nResults b)")
    print_results_5(results)

    cursor.execute(query5('51:56'))
    results = cursor.fetchall()
    print("\nResults c)")
    print_results_5(results)
    



question6 = """
Q6) For podcast episode id = VeH7qKZr0WI, find the five most similar podcast episodes. Hint: you can do a similar averaging procedure as Q5

Input Episode: "Balaji Srinivasan: How to Fix Government, Twitter, Science, and the FDA | Lex Fridman Podcast #331"
For each result return the Podcast title and the embedding distance"""


query6 = """
WITH target AS (
    SELECT AVG(embedding) AS embedding
    FROM podcast_segment
    WHERE podcast_id = 'VeH7qKZr0WI'
)
SELECT 
    p.title, 
    AVG(s.embedding <-> target.embedding) AS avg_embedding_distance
FROM podcast_segment s
JOIN podcast p ON p.id = s.podcast_id,
 target
WHERE s.podcast_id != 'VeH7qKZr0WI'
GROUP BY p.id, p.title
ORDER BY avg_embedding_distance
LIMIT 5;
"""

print(question6)
with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(query6)
    results = cursor.fetchall()
    print("\nResults")
    print_results_5(results)