import pika, time, requests, json, psutil, sys

def get_topology(worker_type):
    try:
        res = requests.post("http://host.docker.internal:5000/api/v1/register", json={"worker_type": worker_type})
        res.raise_for_status()
        return res.json()['topology']
    except Exception as e:
        print(f"[ERROR] Unreachable Controller (Flask): {e}")
        sys.exit(1)

def get_health_score():
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    return round((cpu * 0.5) + (ram * 0.5), 2) 

def callback(ch, method, properties, body):
    score = get_health_score()

    data = json.loads(body)
    print(f"\n[TOKYO REGION - 120ms Latency] Notification Process Triggered. Health Score: {score}")
    time.sleep(1)
    print(f"   [OK] Email sent to user.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    topology = get_topology("notif")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=callback)
    
    print(f" [*] Notification Service Listening on Queue: {topology['queue']}...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()