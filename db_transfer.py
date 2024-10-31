from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()


def process_in_batches(source_collection, destination_collection, batch_size=1000):
    """
    Process documents in batches using cursor to manage memory
    """
    total_processed = 0
    cursor = source_collection.find({})

    while True:
        # Get batch_size documents
        batch_documents = []
        for _ in range(batch_size):
            try:
                doc = next(cursor)
                # Create a new document without the _id field
                new_doc = {k: v for k, v in doc.items() if k != '_id'}
                new_doc['images_scraped'] = False
                new_doc['retry_count'] = 0
                new_doc['processing_status'] = 'pending'
                batch_documents.append(new_doc)
            except StopIteration:
                break

        # If no documents left, break the loop
        if not batch_documents:
            break

        # Insert the batch
        destination_collection.insert_many(batch_documents)
        total_processed += len(batch_documents)
        print(f"Processed {total_processed} documents")

    return total_processed


def main():
    client = MongoClient(os.getenv('MONGO_URI'))
    try:
        # Connect to MongoDB
        db = client["santosh-companies"]

        # Source and destination collections
        source_collection = db["queue"]
        destination_collection = db["queue3"]

        # Get total count for progress tracking
        total_documents = source_collection.count_documents({})
        print(f"Total documents to process: {total_documents}")

        # Process documents in batches
        total_processed = process_in_batches(
            source_collection,
            destination_collection,
            batch_size=10000
        )

        print(f"\nTransfer completed!")
        print(f"Successfully transferred {total_processed} documents to images_queue collection")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the MongoDB connection
        client.close()


if __name__ == "__main__":
    main()
