# producer.py - Producer script for design part: Generates fake smart meter data and stores it in MySQL (simulating middleware storage via sink)
# Requires: pip install python-dotenv pymysql

import os
import time
import random
import json
from dotenv import load_dotenv
import pymysql

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

def generate_fake_data():
    return {
        "ID": random.randint(1000, 9999),  # Unique ID to avoid duplicates
        "time": int(time.time()),
        "profile_name": "fake_smart_meter",
        "temperature": round(random.uniform(15, 35), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "pressure": round(random.uniform(900, 1100), 2)
    }

def store_in_db(connection, data):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO SmartMeter (ID, time, profile_name, temperature, humidity, pressure)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (data['ID'], data['time'], data['profile_name'], data['temperature'], data['humidity'], data['pressure']))
        connection.commit()
        print(f"Stored data in DB: {json.dumps(data)}")
    except pymysql.Error as e:
        print(f"Error storing data: {e}")

if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        for _ in range(5):  # Produce 5 fake readings
            data = generate_fake_data()
            print(f"Producing: {json.dumps(data)}")
            store_in_db(conn, data)
            time.sleep(2)  # Simulate delay
        conn.close()
    else:
        print("Failed to connect to DB. Check .env file.")