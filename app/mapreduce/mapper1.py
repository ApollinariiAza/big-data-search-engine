#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    words = re.findall(r"\w+", text.lower())
    for word in words:
        print(f"{word}\t{doc_id}\t1")

    print(f"__doc_len__\t{doc_id}\t{len(words)}")
