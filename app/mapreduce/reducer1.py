import sys
from collections import defaultdict

current_term = None
doc_freqs = defaultdict(int)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 3:
        continue

    term, doc_id, count = parts
    count = int(count)

    if term == current_term:
        doc_freqs[doc_id] += count
    else:
        if current_term:
            for d_id, freq in doc_freqs.items():
                print(f"{current_term}\t{d_id}\t{freq}")
        current_term = term
        doc_freqs = defaultdict(int)
        doc_freqs[doc_id] = count

if current_term:
    for d_id, freq in doc_freqs.items():
        print(f"{current_term}\t{d_id}\t{freq}")
