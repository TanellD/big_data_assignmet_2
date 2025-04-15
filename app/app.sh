#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz

echo "Waiting for Cassandra to be ready..."
until cqlsh cassandra-server -e "describe keyspaces" > /dev/null 2>&1; do
  echo "Cassandra is unavailable - sleeping"
  sleep 5
done
echo "Cassandra is up - continuing"

# Initialize Cassandra schema
cat > /tmp/init_cassandra.cql << EOF
CREATE KEYSPACE IF NOT EXISTS document_index
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE document_index;

CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS document_lengths (
    doc_id text PRIMARY KEY,
    doc_length int
);

CREATE TABLE IF NOT EXISTS term_frequencies (
    term text,
    doc_id text,
    frequency int,
    PRIMARY KEY (term, doc_id)
);

CREATE TABLE IF NOT EXISTS document_frequencies (
    term text PRIMARY KEY,
    doc_frequency int
);

CREATE TABLE IF NOT EXISTS document_texts (
    doc_id text PRIMARY KEY,
    content text
);
EOF

cqlsh cassandra-server -f /tmp/init_cassandra.cql

# Collect data
bash prepare_data.sh

# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "Famous chemistry scientists"