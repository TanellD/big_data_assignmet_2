#!/usr/bin/env python3
import sys
import os
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

try:
    nltk.download('stopwords', quiet=True)
    nltk.download('punkt', quiet=True)
    stop_words = set(stopwords.words('english'))
except Exception as e:
    sys.stderr.write(f"Warning: Could not download NLTK data: {str(e)}\n")
    # Fallback to a small set of common English stopwords
    stop_words = set(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 
                     'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 
                     'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 
                     'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 
                     'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 
                     'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 
                     'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 
                     'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 
                     'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 
                     'through', 'during', 'before', 'after', 'above', 'below', 'to', 
                     'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 
                     'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 
                     'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
                     'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 
                     'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 
                     'should', 'now'])

stemmer = PorterStemmer()

def preprocess_text(text):
    """Preprocess text: lowercase, remove non-alphanumeric, tokenize, remove stopwords, stem."""
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text.lower())
    try:
        tokens = word_tokenize(text)
    except Exception:
        # Fallback to simple whitespace tokenization
        tokens = text.split()
    
    # Remove stopwords and stem
    processed_tokens = [stemmer.stem(token) for token in tokens if token.isalnum() and token not in stop_words]
    
    return processed_tokens

# Main mapper function
for line_num, line in enumerate(sys.stdin):
    try:
        parts = line.strip().split("\t")
        doc_id = parts[0]       # Original 'id' column
        text = parts[2]         # Original 'text' column
        title = parts[1]     # New 'filename' column
        content = text.strip()
        doc_id = (str(doc_id) + "_" + title).replace(" ", "_")
        
        print(f"DOC_TEXT\t{doc_id}\t{content}")
        tokens = preprocess_text(content)
        doc_length = len(tokens)
        print(f"DOC_LENGTH\t{doc_id}\t{doc_length}")
        term_freq = {}
        for token in tokens:
            term_freq[token] = term_freq.get(token, 0) + 1
        for term, freq in term_freq.items():
            print(f"TERM_FREQUENCY\t{term}\t{doc_id}\t{freq}")
            
    except Exception as e:
        sys.stderr.write(f"Error processing line {line_num}: {str(e)}\n")