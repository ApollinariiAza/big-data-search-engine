from cassandra.cluster import Cluster
import subprocess

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect()
session.set_keyspace("search")

# Read MapReduce output from HDFS
print("Reading MapReduce output from /tmp/index...")

# Run HDFS command to get the output lines
hdfs_cat = subprocess.run(
    ['hdfs', 'dfs', '-cat', '/tmp/index/part-*'],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

# Check if HDFS read was successful
if hdfs_cat.returncode != 0:
    print("Error reading from HDFS:")
    print(hdfs_cat.stderr)
    exit(1)

# Split output into lines
lines = hdfs_cat.stdout.strip().split('\n')

# Process each line and insert into Cassandra
for line in lines:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        print(f"Skipped malformed line: '{line}' (fields: {len(parts)})")
        continue

    term, doc_id, freq = parts
    try:
        doc_id = int(doc_id)
        freq = int(freq)

        if term == "__doc_len__":
            session.execute(
                "INSERT INTO doc_stats (doc_id, doc_len) VALUES (%s, %s)",
                (doc_id, freq)
            )
        else:
            session.execute(
                "INSERT INTO inverted_index (term, doc_id, freq) VALUES (%s, %s, %s)",
                (term, doc_id, freq)
            )
    except Exception as e:
        print(f"Insert error: {e}")
        print("Offending line:", line)
