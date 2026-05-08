import pika
import json
import time
import random

RABBITMQ_HOST = 'localhost'
QUEUES = ['order_queue', 'stock_queue', 'notif_queue', 'order_db_write_queue']
MESSAGE_COUNT_PER_QUEUE = 150 

def run_stress_test():
    print("[INFO] Initializing Stress Test...")
    time.sleep(2)
    
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
    except Exception as e:
        print(f"[ERROR] Failed to connect to RabbitMQ. Is Docker running? Error: {e}")
        return

    for i in range(1, MESSAGE_COUNT_PER_QUEUE + 1):
        db_payload = {
            "user_id": random.randint(1000, 9999),
            "subtotal": 250.0,
            "total": 250.0,
            "items": [{"pid": 1, "price": 250.0, "qty": 1}]
        }
        
        standard_payload = {
            "order_id": f"ORD-STRESS-{i}",
            "status": "processing",
            "timestamp": time.time()
        }

        for q in QUEUES:
            payload = db_payload if q == 'order_db_write_queue' else standard_payload
            channel.basic_publish(exchange='', routing_key=q, body=json.dumps(payload))
        
        print(f"[PUBLISH] Wave {i}: Load injected to all four microservices!")
        time.sleep(0.01)  

    connection.close()
    print("\n[SUCCESS] Stress Test COMPLETED!")
    print(f"Total {MESSAGE_COUNT_PER_QUEUE * 4} messages pumped into the message broker.")

if __name__ == '__main__':
    run_stress_test()