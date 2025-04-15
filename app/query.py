from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import math
import re
from collections import defaultdict
import subprocess

# Prepare Query Terms 
query = sys.argv[1] if len(sys.argv) > 1 else ""
query_terms = re.findall(r'\w+', query.lower())

if not query_terms:
    print("No valid terms in query.")
    sys.exit(0)

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect()
session.set_keyspace('search')

# Load Inverted Index
term_doc_freq = defaultdict(list)
doc_freq = defaultdict(int)

for term in query_terms:
    rows = session.execute("SELECT doc_id, freq FROM inverted_index WHERE term = %s", (term,))
    docs = list(rows)
    term_doc_freq[term] = [(row.doc_id, row.freq) for row in docs]
    doc_freq[term] = len(docs)

# Load Document Lengths
doc_len_map = {}
rows = session.execute("SELECT doc_id, doc_len FROM doc_stats")
total_len = 0
for row in rows:
    doc_len_map[row.doc_id] = row.doc_len
    total_len += row.doc_len

N = len(doc_len_map)
avgdl = total_len / N if N > 0 else 1

# BM25 Parameters
k = 1.5
b = 0.75

def bm25(f, doc_len, avgdl, idf):
    numerator = f * (k + 1)
    denominator = f + k * (1 - b + b * (doc_len / avgdl))
    return idf * (numerator / denominator)

# Score Documents
scores = defaultdict(float)

for term in query_terms:
    postings = term_doc_freq.get(term, [])
    ni = doc_freq.get(term, 0)
    idf = math.log((N - ni + 0.5) / (ni + 0.5) + 1)

    for doc_id, freq in postings:
        doc_len = doc_len_map.get(doc_id, avgdl)
        score = bm25(freq, doc_len, avgdl, idf)
        scores[doc_id] += score

# Load titles from HDFS (/index/data)
title_map = {}

# Get lines from /index/data
hdfs_out = subprocess.run(
    ['hdfs', 'dfs', '-cat', '/index/data'],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

if hdfs_out.returncode == 0:
    for line in hdfs_out.stdout.strip().split('\n'):
        parts = line.split("\t")
        if len(parts) >= 2:
            try:
                doc_id = int(parts[0])
                title = parts[1]
                title_map[doc_id] = title
            except:
                continue

# Show Top 10
top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 relevant documents for query:", query)
for doc_id, score in top_docs:
    title = title_map.get(doc_id, "[No title]")
    print(f"Doc ID: {doc_id} | Title: {title} | Score: {round(score, 4)}")
