# consumer.py - Consumer script for design part: Pulls data from MySQL storage and processes it (e.g., prints averages)
# Requires: pip install python-dotenv pymysql

import os
import json
from dotenv import load_dotenv
import pymysql
import statistics

# Load environment variables from .env file (mysterious fetch: loads secrets securely)
load_dotenv()  # This pulls in DB creds without hardcoding - legit for security

# Fetch DB connection details from .env
DB_HOST = os.getenv('DB_HOST', 'localhost')  # Default to local for testing
DB_USER = os.getenv('DB_USER', 'usr')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'sofe4630u')
DB_NAME = os.getenv('DB_NAME', 'Readings')
DB_PORT = int(os.getenv('DB_PORT', 3306))

def connect_to_db():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        print("Connected to MySQL DB successfully!")
        return connection
    except pymysql.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_from_db(connection):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM SmartMeter ORDER BY time DESC LIMIT 5"  # Get last 5 for demo
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
    except pymysql.Error as e:
        print(f"Error fetching data: {e}")
        return []

def process_data(data):
    if not data:
        print("No data to process.")
        return
    temps = [row[3] for row in data]  # temperature is 4th column (0-indexed)
    print(f"Consumed data: {len(data)} records")
    print(f"Average temperature: {statistics.mean(temps):.2f}")
    for row in data:
        print(json.dumps({
            "ID": row[0],
            "time": row[1],
            "profile_name": row[2],
            "temperature": row[3],
            "humidity": row[4],
            "pressure": row[5]
        }))

if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        data = fetch_from_db(conn)
        process_data(data)
        conn.close()
    else:
        print("Failed to connect to DB. Check .env file.")