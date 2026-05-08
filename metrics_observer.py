import requests
import time
import csv
import subprocess
from datetime import datetime

RABBITMQ_URL = "http://guest:guest@localhost:15672/api/queues"

def get_container_count(service_name):
    try:
        output = subprocess.check_output(f"docker ps -f name={service_name} --format '{{{{.Names}}}}'", shell=True)
        return len(output.strip().split(b'\n')) if output.strip() else 0
    except:
        return 0

def get_queue_messages():
    try:
        resp = requests.get(RABBITMQ_URL).json()
        return sum(q.get('messages', 0) for q in resp)
    except:
        return 0

with open('metrics.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Time', 'Total_Messages', 'Stock_Workers', 'Order_Workers', 'Notif_Workers', 'DB_Workers'])

print("[INFO] Metrics Observer Started! Recording data to CSV...")

try:
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        msg_count = get_queue_messages()
        
        stock_c = get_container_count("stock_worker")
        order_c = get_container_count("order_worker")
        notif_c = get_container_count("notif_worker")
        db_c = get_container_count("db_worker")

        with open('metrics.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([now, msg_count, stock_c, order_c, notif_c, db_c])
        
        time.sleep(2)
except KeyboardInterrupt:
    print("\n[INFO] Data collection completed successfully.")