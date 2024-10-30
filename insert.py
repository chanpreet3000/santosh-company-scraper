import asyncio
import pandas as pd
from dotenv import load_dotenv
from Logger import Logger
from db import Database

load_dotenv()


async def import_csv_to_mongodb():
    db = Database()
    await db.connect()

    try:
        df = pd.read_csv('input.csv')
        df.columns = df.columns.str.lower()

        df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype(int)

        # Convert inserted_date to datetime and handle empty values
        df['inserted_date'] = pd.to_datetime(df['inserted_date'], errors='coerce')
        df['inserted_date'] = df['inserted_date'].fillna(pd.Timestamp.utcnow())

        # Add additional fields
        df['retry'] = 0
        df['processed'] = 'pending'
        df['scraped'] = False

        # Convert DataFrame to list of dictionaries
        documents = df.to_dict('records')

        # Insert documents in chunks for better performance
        chunk_size = 1000
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            try:
                result = await db.queue_collection.insert_many(chunk)
                Logger.info(f"Inserted {len(result.inserted_ids)} documents. "
                            f"Batch {i // chunk_size + 1} of {(len(documents) + chunk_size - 1) // chunk_size}")
            except Exception as e:
                Logger.error(f"Failed to insert batch starting at index {i}", e)

        Logger.info("CSV import completed")
    except Exception as e:
        Logger.error(f"Error processing CSV file: {str(e)}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(import_csv_to_mongodb())
