import sqlite3
import mpmath
import hashlib
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_pi_digits(num_digits):
    """Generates a specified number of decimal places of pi.

    Args:
        num_digits: The number of decimal places to generate.

    Returns:
        A string representing the generated pi digits.
    """
    try:
        if num_digits <= 0:
            raise ValueError("Number of digits must be positive.")
        mpmath.mp.dps = num_digits
        pi_digits = str(mpmath.mpf(mpmath.pi))
        logging.info(f"Generated {num_digits} digits of pi.")
        return pi_digits
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error generating pi digits: {e}")
        raise

def calculate_checksum(data):
    """Calculates the SHA-256 checksum of the given data.

    Args:
        data: The data to calculate the checksum for.

    Returns:
        The checksum as a hexadecimal string.
    """
    return hashlib.sha256(data.encode()).hexdigest()

def insert_segments(conn, pi_digits, segment_size):
    """Inserts segments of pi digits into a SQLite database.

    Args:
        conn: A SQLite connection object.
        pi_digits: A string containing the pi digits.
        segment_size: The size of each segment.
    """
    try:
        if segment_size <= 0:
            raise ValueError("Segment size must be positive.")
        cursor = conn.cursor()
        for i in range(0, len(pi_digits) - segment_size + 1, segment_size):
            start_index = i
            end_index = min(i + segment_size, len(pi_digits))
            segment = pi_digits[start_index:end_index]
            checksum = calculate_checksum(segment)
            timestamp = datetime.now().isoformat()
            cpu_process_mechanism = os.cpu_count()
            logging.info(f"Inserting segment: start_index={start_index}, end_index={end_index}")
            cursor.execute("""
                INSERT INTO pi_segments (start_index, end_index, digits, check_sum_pi_per_input, verified, timestamp, cpu_process_mechanism)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (start_index, end_index, segment, checksum, 'unverified', timestamp, cpu_process_mechanism))
        conn.commit()
        logging.info(f"Inserted {len(pi_digits) // segment_size} segments into the database.")
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
        raise
    except sqlite3.Error as se:
        logging.error(f"SQLite error: {se}")
        raise
    except Exception as e:
        logging.error(f"Error inserting segments: {e}")
        raise

def create_database_and_table(db_name):
    """Creates a SQLite database and a table to store pi segments.

    Args:
        db_name: The name of the database to create.

    Returns:
        A SQLite connection object.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS pi_segments")
        cursor.execute("""
            CREATE TABLE pi_segments (
                id INTEGER PRIMARY KEY,
                start_index INTEGER,
                end_index INTEGER,
                digits TEXT,
                check_sum_pi_per_input TEXT,
                verified TEXT DEFAULT 'unverified',
                timestamp TEXT,
                cpu_process_mechanism INTEGER
            )
        """)
        conn.commit()
        logging.info(f"Created database and table: {db_name}")
        return conn
    except sqlite3.Error as se:
        logging.error(f"SQLite error: {se}")
        raise
    except Exception as e:
        logging.error(f"Error creating database and table: {e}")
        raise

def main():
    db_name = "pi_database_tier2.db"
    num_digits = 100000
    segment_size = 1000

    try:
        conn = create_database_and_table(db_name)
        pi_digits = generate_pi_digits(num_digits)
        insert_segments(conn, pi_digits, segment_size)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
