import time
import psutil
import psycopg2
import socket
from datetime import datetime

# --- CONFIGURATION ---
DB_HOST = "10.0.0.41"
DB_NAME = "air_quality_db"
DB_USER = "postgres"
DB_PASS = "Paris/muj@890"

# Get current hostname (e.g., "master", "worker1")
HOSTNAME = socket.gethostname()

def collect_and_push():
    try:
        # 1. Connect to DB
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()

        while True:
            # 2. Get Metrics
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory()

            # 3. Insert into DB
            query = """
                INSERT INTO cluster_health (timestamp, hostname, cpu_usage, ram_usage, ram_total)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(query, (datetime.now(), HOSTNAME, cpu, ram.percent, ram.total / (1024**3)))
            conn.commit()

            print(f"[{HOSTNAME}] CPU: {cpu}% | RAM: {ram.percent}% - Sent to DB")
            time.sleep(4) # Wait 4 seconds

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print(f"--- Starting Monitoring Agent on {HOSTNAME} ---")
    collect_and_push()