#!/usr/bin/env python3

import sys
import math
from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark import SparkConf
from cassandra.cluster import Cluster
from operator import add
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re

nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)
stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()
# BM25 parameters
k1 = 1.2
b = 0.75

def connect_to_cassandra():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('document_index')
    return cluster, session

def fetch_cassandra_table(session, table_name):
    rows = session.execute(f"SELECT * FROM {table_name}")
    return list(rows)

def parse_query(query_text):
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', query_text.lower())
    try:
        tokens = word_tokenize(text)
    except Exception:
        # Fallback to simple whitespace tokenization
        tokens = text.split()
    
    # Remove stopwords and stem
    processed_tokens = [stemmer.stem(token) for token in tokens if token.isalnum() and token not in stop_words]
    
    return processed_tokens

def calculate_bm25(query_terms, doc_id, doc_len, avg_doc_len, term_freqs, doc_freqs, total_docs):
    """Calculate BM25 score for a document"""
    score = 0.0
    for term in query_terms:
        if term in term_freqs and doc_id in term_freqs[term]:
            tf = term_freqs[term][doc_id]
            df = doc_freqs.get(term, 0)
            if df > 0:
                idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)
                score += idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len)))
    return score

def get_document_title(doc_id, sc):
    """Placeholder function to get document title"""
    return f"Document {doc_id}"

def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py 'your search query'")
        return
        
    query_text = " ".join(sys.argv[1:])
    
    spark = SparkSession.builder \
        .appName("BM25 Document Search") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()
    sc = spark.sparkContext

    query_terms = parse_query(query_text)
    
    if not query_terms:
        print("Empty query. Please provide search terms.")
        return

    cluster, session = connect_to_cassandra()
    
    try:
        doc_lengths = {row.doc_id: row.doc_length for row in fetch_cassandra_table(session, 'document_lengths')}
        doc_freqs = {row.term: row.doc_frequency for row in fetch_cassandra_table(session, 'document_frequencies')}
        doc_texts = {row.doc_id: row.content for row in fetch_cassandra_table(session, 'document_texts')}
        term_freqs = {}
        for term in query_terms:
            rows = session.execute(f"SELECT doc_id, frequency FROM term_frequencies WHERE term = '{term}'")
            term_freqs[term] = {row.doc_id: row.frequency for row in rows}
        
        total_docs = len(doc_lengths)
        avg_doc_len = sum(doc_lengths.values()) / total_docs if total_docs > 0 else 0
        
        doc_ids_rdd = sc.parallelize(list(doc_lengths.keys()))
        
        doc_scores = doc_ids_rdd.map(
            lambda doc_id: (
                doc_id, 
                calculate_bm25(
                    query_terms, 
                    doc_id, 
                    doc_lengths.get(doc_id, 0), 
                    avg_doc_len, 
                    term_freqs, 
                    doc_freqs, 
                    total_docs
                )
            )
        )
        
        top_docs = doc_scores.sortBy(lambda x: -x[1]).take(10)
        
        # Print results
        print(f"\nTop 10 results for query: '{query_text}'")
        print("-" * 50)
        
        if not top_docs:
            print("No matching documents found.")
        else:
            for i, (doc_id, score) in enumerate(top_docs, 1):
                title = get_document_title(doc_id, sc)
                print(f"{i}. Document ID: {doc_id}")
                print(f"   Title: {title}")
                print(f"   Score: {score:.4f}")
                print(f"    Text: {doc_texts[doc_id][:50]}...")
                print()
        
    finally:
        # Close connections
        cluster.shutdown()
        spark.stop()

if __name__ == "__main__":
    main()