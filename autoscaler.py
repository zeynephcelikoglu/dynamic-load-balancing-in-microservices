import docker
import requests
import time
import math
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

RABBITMQ_BASE_URL = "http://localhost:15672/api/queues/%2f/"

SCALING_MAP = {
    "stock_queue": "stock_worker",
    "order_queue": "order_worker",
    "notif_queue": "notif_worker",
    "order_db_write_queue": "db_worker" 
}

POLLING_INTERVAL = 5      
MAX_WORKERS = 10           
MIN_WORKERS = 1           
MESSAGES_PER_WORKER = 15  
client = docker.from_env()

def get_queue_depth(queue_name):
    try:
        url = f"{RABBITMQ_BASE_URL}{queue_name}"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS), timeout=3)

        if response.status_code == 404:
            return -1
            
        response.raise_for_status()
        return response.json().get('messages_ready', 0)
    except Exception as err: 
        return -1

def scale_service(service_name, target_count):
    current_containers = [c for c in client.containers.list() if service_name in c.name]
    current_count = len(current_containers)
    
    if current_count == target_count:
        return
    
    print(f"\n[SCALING] {service_name}: {current_count} -> {target_count}")
    os.system(f"docker-compose up -d --scale {service_name}={target_count} --no-recreate")

def main():
    print("Autoscaler Başlatıldı...")
    
    while True:
        for q_name, s_name in SCALING_MAP.items():
            depth = get_queue_depth(q_name)
            if depth == -1: continue
            
            target = math.ceil(depth / MESSAGES_PER_WORKER) if depth > 0 else MIN_WORKERS
            target = max(MIN_WORKERS, min(MAX_WORKERS, target))
            
            scale_service(s_name, target)
        
        time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    main()