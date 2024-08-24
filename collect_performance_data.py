
import sqlite3
import time
import psutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_segments(db_name):
    """Reads segments from the pi_segments table and measures the time taken to display each row.

    Args:
        db_name: The name of the database to read from.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pi_segments")

        rows = cursor.fetchall()
        total_time = 0

        for row in rows:
            start_time = time.time()
            print(row)
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
            logging.info(f"Time to display row: {elapsed_time:.6f} seconds")

            # Record performance metrics
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_info = psutil.virtual_memory()
            logging.info(f"CPU Usage: {cpu_usage}%")
            logging.info(f"Memory Usage: {memory_info.percent}%")

        conn.close()
        logging.info(f"Total time to display all rows: {total_time:.6f} seconds")
    except Exception as e:
        logging.error(f"Error reading segments: {e}")

def main():
    db_name = "pi_database_tier2.db"
    read_segments(db_name)

if __name__ == "__main__":
    main()
