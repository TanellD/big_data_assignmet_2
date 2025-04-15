import os
import sys
import csv
import re
from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

def main():
    # Check if a folder path is provided as argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <folder_path> [output_csv_path]")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    
    # Set default output CSV path or use provided argument
    output_csv = "/index/data" if len(sys.argv) < 3 else sys.argv[2]
    
    print(f"Processing files from: {input_folder}")
    print(f"Output will be saved to: {output_csv}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName('data preparation') \
        .master("local") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    
    # Create a list to hold all file data
    file_data = []
    
    # Read all files in the given folder
    print("Reading files...")
    for filename in tqdm(os.listdir(input_folder)):
        file_path = os.path.join(input_folder, filename)
        
        # Skip directories
        if os.path.isdir(file_path):
            continue
        
        # Extract id and title from filename
        # Assuming format: id_title.txt
        match = re.match(r'(\d+)_(.+)\.txt$', filename)
        
        if match:
            doc_id = match.group(1)
            title = match.group(2).replace("_", " ")
        else:
            # If filename doesn't match pattern, use filename as title and generate an id
            doc_id = str(len(file_data) + 1)
            title = os.path.splitext(filename)[0]
        
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
            # Add to our data list
            file_data.append((doc_id, title, text))
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
    
    # Create DataFrame from collected data
    print(f"Creating DataFrame with {len(file_data)} documents...")
    df = spark.createDataFrame(file_data, ["id", "title", "text"])
    
    # Save the processed text files as individual files
    print("Saving individual text files...")
    df.foreach(create_doc)
    
    # Save as CSV
    print(f"Saving data to CSV at {output_csv}...")
    df.write.csv(output_csv, sep="\t", mode='overwrite', header=True)
    
    print("Processing complete!")
    spark.stop()

def create_doc(row):
    # Create a directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Create sanitized filename
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    
    # Write content to file
    with open(filename, "w", encoding='utf-8') as f:
        f.write(row['text'])

if __name__ == "__main__":
    main()