#!/usr/bin/env python3
import sys
from collections import defaultdict

# dictionaries for storing from Map-Reducer 1
doc_lengths = {}
texts = {}
term_frequencies = defaultdict(dict)

# process input from mapper1
for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split('\t')
        
        if len(parts) < 3:
            continue
            
        entry_type = parts[0]
        if entry_type == "DOC_LENGTH":
            doc_id = parts[1]
            doc_length = int(parts[2])
            doc_lengths[doc_id] = doc_length
        elif entry_type == "TERM_FREQUENCY":
            term = parts[1]
            doc_id = parts[2]
            freq = int(parts[3])
            term_frequencies[term][doc_id] = freq
        elif entry_type == "DOC_TEXT":
            doc_id = parts[1]
            text = parts[2]
            texts[doc_id] = text
            
    except Exception as e:
        sys.stderr.write(f"Error processing line {line}: {str(e)}\n")

# output to reducer2
for doc_id, length in doc_lengths.items():
    print(f"DOC_LENGTH\t{doc_id}\t{length}")
for term, doc_freqs in term_frequencies.items():
    for doc_id, freq in doc_freqs.items():
        print(f"TERM_FREQUENCY\t{term}\t{doc_id}\t{freq}")
for term, doc_freqs in term_frequencies.items():
    df = len(doc_freqs)
    print(f"DOC_FREQUENCY\t{term}\t{df}")
for term in term_frequencies.keys():
    print(f"VOCABULARY\t{term}")
for doc_id, text in texts.items():
    print(f"DOC_TEXT\t{doc_id}\t{text}")

