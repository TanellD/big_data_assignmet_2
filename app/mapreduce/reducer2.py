#!/usr/bin/env python3
import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

def connect_to_cassandra(max_retries=5, retry_interval=5):
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect()
        return cluster, session
    except Exception as e:
        sys.stderr.write(f"Cassandra connection attempt failed: {str(e)}\n")
        raise

try:
    # connect to Cassandra
    cluster, session = connect_to_cassandra()
    
    # use the keyspace
    session.execute("USE document_index")
    
    # prepare insert statements
    insert_vocabulary = session.prepare("INSERT INTO vocabulary (term) VALUES (?)")
    insert_doc_length = session.prepare("INSERT INTO document_lengths (doc_id, doc_length) VALUES (?, ?)")
    insert_term_freq = session.prepare("INSERT INTO term_frequencies (term, doc_id, frequency) VALUES (?, ?, ?)")
    insert_doc_freq = session.prepare("INSERT INTO document_frequencies (term, doc_frequency) VALUES (?, ?)")
    insert_text = session.prepare("INSERT INTO document_texts (doc_id, content) VALUES (?, ?)")
    # i will use batch insertions for efficiency
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch_size = 0
    max_batch_size = 1
    
    # process input from mapper2
    for line in sys.stdin:
        try:
            line = line.strip()
            parts = line.split('\t')
            
            if len(parts) < 2:
                continue
                
            entry_type = parts[0]
            
            if entry_type == "DOC_LENGTH":
                doc_id = parts[1]
                doc_length = int(parts[2])
                batch.add(insert_doc_length, (doc_id, doc_length))
                batch_size += 1
            elif entry_type == "TERM_FREQUENCY":
                term = parts[1]
                doc_id = parts[2]
                freq = int(parts[3])
                batch.add(insert_term_freq, (term, doc_id, freq))
                batch_size += 1
            elif entry_type == "DOC_FREQUENCY":
                term = parts[1]
                df = int(parts[2])
                batch.add(insert_doc_freq, (term, df))
                batch_size += 1
            elif entry_type == "VOCABULARY":
                term = parts[1]
                batch.add(insert_vocabulary, (term,))
                batch_size += 1
            elif entry_type == "DOC_TEXT":
                doc_id = parts[1]
                text = parts[2]
                batch.add(insert_text, (doc_id, text))
                batch_size += 1
            if batch_size >= max_batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_size = 0
                
        except Exception as e:
            sys.stderr.write(f"Error processing line {line}: {str(e)}\n")
    
    # execute remaining items in batch
    if batch_size > 0:
        session.execute(batch)
    
except Exception as e:
    sys.stderr.write(f"Fatal error in reducer2: {str(e)}\n")
    sys.exit(1)
    
finally:
    # Close the Cassandra connection
    if 'cluster' in locals():
        cluster.shutdown()